/*
 * Copyright (C) 2026 Huawei Technologies Co.,Ltd.
 *
 * dstore is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * dstore is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. if not, see <https://www.gnu.org/licenses/>.
 *
 * -------------------------------------------------------------------------
 *
 * dstore_tac_snapshot.cpp
 *
 * IDENTIFICATION
 *      src/transaction/dstore_tac_snapshot.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "transaction/dstore_tac_snapshot.h"
#include "framework/dstore_instance.h"
#include "framework/dstore_thread.h"
#include "transaction/dstore_csn_mgr.h"


namespace DSTORE {

static const int HEAP_INITIAL_CAPACITY = 8;

struct TacOrphanTrxEntry {
    CommitSeqNo csn;
    TimestampTz expiryTimestamp;
};

static int TacOrphanTrxEntryComparator(Datum a, Datum b, void* arg)
{
    (void)arg; /* keep compiler happy */
    TacOrphanTrxEntry* entryA = reinterpret_cast<TacOrphanTrxEntry*>(a);
    TacOrphanTrxEntry* entryB = reinterpret_cast<TacOrphanTrxEntry*>(b);
 
    /*
     * We want a min-heap, so return 1 when a < b.
     * First compare csn, then compare expiryTimestamp (lexicographic order on (csn, expiryTimestamp)).
     */
    if (entryA->csn < entryB->csn) {
        return 1;
    } else if (entryA->csn == entryB->csn) {
        /*
         * if csn are the same, let the entry with smaller expiryTimestamp become the smaller one, so that the
         * recycling works better as it puts eariler entires on top
         */
        if (entryA->expiryTimestamp < entryB->expiryTimestamp) {
            return 1;
        } else if (entryA->expiryTimestamp == entryB->expiryTimestamp) {
            return 0;
        } else {
            return -1;
        }
    } else {
        return -1;
    }
}
 
void TacOrphanTrxTracker::Init()
{
    m_allocator = DstoreAllocSetContextCreate(
        g_storageInstance->GetMemoryMgr()->GetRoot(),
        "TacOrphanTrxTracker",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
        MemoryContextType::SHARED_CONTEXT);

    AutoMemCxtSwitch autoMemCxtSwitch(m_allocator);
    m_csnHeap = BinaryheapAllocate(HEAP_INITIAL_CAPACITY, TacOrphanTrxEntryComparator, nullptr);
    LWLockInitialize(&m_heapLock, LWLOCK_GROUP_TAC_HEAP);
}

void TacOrphanTrxTracker::Destroy()
{
    if (m_csnHeap != nullptr) {
        BinaryheapFree(m_csnHeap);
        m_csnHeap = nullptr;
    }

    if (m_allocator != nullptr) {
        DstoreMemoryContextDelete(m_allocator);
        m_allocator = nullptr;
    }
}

/**
 * Add a orphan csn into the tracker. The expiry time is calculated according to current time and tacGracePeriod.
 * @param csn      The new orphan csn.
 */
void TacOrphanTrxTracker::AddTacOrphanTrx(CommitSeqNo csn)
{
    if (likely(g_storageInstance->GetGuc()->tacGracePeriod == 0 || csn == INVALID_CSN)) {
        return;
    }

    /* We use timestamp in seconds and tacGracePeriod is also in seconds */
    DstoreLWLockAcquire(&m_heapLock, LWLockMode::LW_EXCLUSIVE);
    TimestampTz timeIncrement = static_cast<TimestampTz>(g_storageInstance->GetGuc()->tacGracePeriod);
    TimestampTz expireTime = GetCurrentTimestampInSecond() + timeIncrement;
    /* No need to add an entry if top entry covers the new entry (have earlier csn and expires even later) */
    if (!binaryheap_empty(m_csnHeap)) {
        TacOrphanTrxEntry* topEntry = reinterpret_cast<TacOrphanTrxEntry*>(DatumGetPointer(BinaryheapFirst(m_csnHeap)));
        if (topEntry->csn <= csn && topEntry->expiryTimestamp >= expireTime) {
            LWLockRelease(&m_heapLock);
            return;
        }
    }

    AutoMemCxtSwitch autoMemCxtSwitch(m_allocator);

    /* double the capacity of heap if heap is full */
    if (unlikely(m_csnHeap->bh_size == m_csnHeap->bh_space)) {
        unsigned int oldCapacity = static_cast<unsigned int>(m_csnHeap->bh_space);
        unsigned int newCapacity = oldCapacity * 2;
        unsigned long oldHeapBytes = offsetof(binaryheap, bh_nodes) + sizeof(Datum) * oldCapacity;
        unsigned long newHeapBytes = offsetof(binaryheap, bh_nodes) + sizeof(Datum) * newCapacity;
 
        binaryheap* newHeap = (binaryheap*)DstorePalloc(newHeapBytes);
        int rc = memcpy_s(newHeap, newHeapBytes, m_csnHeap, oldHeapBytes);
        storage_securec_check(rc, "\0", "\0");
        newHeap->bh_space = static_cast<int>(newCapacity); /* all other fields remains the same except bh_space */

        DstorePfreeExt(m_csnHeap);
        m_csnHeap = newHeap;
    }
 
    /* add new entry */
    TacOrphanTrxEntry *entry = (TacOrphanTrxEntry *)DstorePalloc0(sizeof(TacOrphanTrxEntry));
    if (unlikely(entry == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION, ErrMsg("Alloc tac rrphan trx entry failed."));
        return;
    }
    entry->csn = csn;
    entry->expiryTimestamp = expireTime;
    BinaryheapAdd(m_csnHeap, PointerGetDatum(entry));
 
    LWLockRelease(&m_heapLock);
}
 
/**
 * Get the smallest orphan csn that hasn't expired yet from the tracker. If there are none, return INVALID_CSN.
 */
CommitSeqNo TacOrphanTrxTracker::GetSmallestOrphanCsn()
{
    if (likely(g_storageInstance->GetGuc()->tacGracePeriod == 0 || unlikely(m_csnHeap == nullptr))) {
        return INVALID_CSN;
    }
    DstoreLWLockAcquire(&m_heapLock, LWLockMode::LW_SHARED);
    /*
     * Even if u_sess->attr.attr_storage.enable_tac is false, we still need to check the heap, because enable_tac is a
     * per-session setting, but the heap is per-pdb, and may contains orphan csn from other sessions.
     */
    if (likely(binaryheap_empty(m_csnHeap))) {
        LWLockRelease(&m_heapLock);
        return INVALID_CSN;
    }
    TimestampTz currentTime = GetCurrentTimestampInSecond();
    while (!binaryheap_empty(m_csnHeap)) {
        TacOrphanTrxEntry* entry = reinterpret_cast<TacOrphanTrxEntry*>(DatumGetPointer(BinaryheapFirst(m_csnHeap)));
        if (entry->expiryTimestamp < currentTime) {
            /* try to remove the expired entry */
            LWLockRelease(&m_heapLock);
            if (!TryRemoveExpiredEntry()) {
                return entry->csn;
            }
            return GetSmallestOrphanCsn();
        }
        /* entry has not expire yet */
        LWLockRelease(&m_heapLock);
        return entry->csn;
    }
    LWLockRelease(&m_heapLock);
    return INVALID_CSN;
}

bool TacOrphanTrxTracker::TryRemoveExpiredEntry()
{
    TimestampTz currentTime = GetCurrentTimestampInSecond();
    /*
    * Skip lock that need to wait on conflicting locks on other threads.
    * If Thread1 holds the heap lock to remove expired entries and Thread2 is
    * waiting for it, Thread2 will wait until Thread1 to finish and it will
    * eventually find out all expired entries has been removed, which is
    * not necessary and degrades the performance, what we will do here is
    * to just return the expired entry, which might not be the latest csn but
    * we gain performance in return.
    */
    if (!DstoreLWLockConditionalAcquire(&m_heapLock, LWLockMode::LW_EXCLUSIVE)) {
        return false;
    }
    while (!binaryheap_empty(m_csnHeap)) {
        TacOrphanTrxEntry* entry = reinterpret_cast<TacOrphanTrxEntry*>(DatumGetPointer(BinaryheapFirst(m_csnHeap)));
        if (entry->expiryTimestamp < currentTime) {
            /* remove the expired entry */
            BinaryheapRemoveFirst(m_csnHeap);
            continue;
        }
        /* entry has not expire yet */
        break;
    }
    LWLockRelease(&m_heapLock);
    /* All expired entries has been successfully removed */
    return true;
}

void TacOrphanTrxTracker::RefreshOrphanTrxExpiryTime()
{
    if (likely(g_storageInstance->GetGuc()->tacGracePeriod == 0)) {
        return;
    }

    DstoreLWLockAcquire(&m_heapLock, LWLockMode::LW_EXCLUSIVE);
    /* No need to extend an entry if the heap is empty */
    if (binaryheap_empty(m_csnHeap)) {
        LWLockRelease(&m_heapLock);
        return;
    }

    /* Replace the topmost entry after setting a new expiry time */
    TacOrphanTrxEntry* oldEntry = reinterpret_cast<TacOrphanTrxEntry*>(DatumGetPointer(BinaryheapFirst(m_csnHeap)));
    TimestampTz timeIncrement = static_cast<TimestampTz>(g_storageInstance->GetGuc()->tacGracePeriod);
    oldEntry->expiryTimestamp = GetCurrentTimestampInSecond() + timeIncrement;
    BinaryheapReplaceFirst(m_csnHeap, PointerGetDatum(oldEntry));

    LWLockRelease(&m_heapLock);
    return;
}

} /* namespace DSTORE */