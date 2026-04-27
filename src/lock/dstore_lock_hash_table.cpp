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
 */

#include "lock/dstore_lock_hash_table.h"

namespace DSTORE {

LockTagCache::LockTagCache(const LockTag *tag)
    : lockTag(tag), hashCode(tag_hash(static_cast<const void *>(tag), sizeof(LockTag)))
{}

LockHashTable::LockHashTable()
    : m_lockTable(nullptr),
      m_partitionLocks(nullptr),
      m_partitionNum(0),
      m_freeLists(),
      m_ctx(nullptr)
{}

LockHashTable::~LockHashTable()
{
    m_lockTable = nullptr;
    m_partitionLocks = nullptr;
    m_ctx = nullptr;
}

RetStatus LockHashTable::Initialize(uint32 hashTableSize, uint32 partitionNum, DstoreMemoryContext ctx)
{
    HASHCTL info;
    info.keysize = sizeof(LockTag);
    info.entrysize = sizeof(LockEntry);
    info.hash = tag_hash;
    info.num_partitions = static_cast<long>(partitionNum);
    info.hcxt = ctx;
#ifndef DSTORE_USE_ASSERT_CHECKING
    info.alloc = malloc;
    info.dealloc = free;
    int hashFlags = (HASH_ELEM | HASH_FUNCTION | HASH_PARTITION | HASH_SHRCTX | HASH_ALLOC | HASH_DEALLOC);
#else
    int hashFlags = (HASH_ELEM | HASH_FUNCTION | HASH_PARTITION | HASH_SHRCTX);
#endif

    m_lockTable = hash_create("lock hash", hashTableSize, &info, hashFlags);
    if (m_lockTable == nullptr) {
        storage_set_error(LOCK_ERROR_OUT_OF_MEMORY);
        return DSTORE_FAIL;
    }

    m_partitionLocks = static_cast<LWLockPadded *>(DstoreMemoryContextAlloc(ctx, sizeof(LWLockPadded) * partitionNum));
    if (m_partitionLocks == nullptr) {
        Destroy();
        storage_set_error(LOCK_ERROR_OUT_OF_MEMORY);
        return DSTORE_FAIL;
    }

    constexpr uint32 maxFreelistLength = 1024;
    RetStatus ret = m_freeLists.BuildFreeLists(partitionNum, maxFreelistLength, ctx);
    if (STORAGE_FUNC_FAIL(ret)) {
        Destroy();
        return DSTORE_FAIL;
    }

    m_partitionNum = partitionNum;
    for (uint32 i = 0; i < partitionNum; i++) {
        LWLockInitialize(&(m_partitionLocks[i].lock), LWLOCK_GROUP_HASH_TABLE_PARTITION);
    }

    m_ctx = ctx;
    return DSTORE_SUCC;
}

void LockHashTable::Destroy()
{
    if (m_lockTable != nullptr) {
        hash_destroy(m_lockTable);
        m_lockTable = nullptr;
    }

    if (m_partitionLocks != nullptr) {
        DstorePfree(m_partitionLocks);
        m_partitionLocks = nullptr;
    }

    m_freeLists.DestroyFreeLists();
    m_partitionNum = 0;
    m_ctx = nullptr;
}

RetStatus LockHashTable::LockRequestEnqueue(const LockTagCache &tag, LockRequestInterface *request,
    LockErrorInfo *info)
{
    RetStatus ret = DSTORE_FAIL;

    StorageAssert(tag.hashCode == get_hash_value(m_lockTable, static_cast<const void *>(tag.lockTag)));
    uint32 partitionId = tag.hashCode % m_partitionNum;
    LockEntry *lockEntry = nullptr;
    bool found = false;

    DstoreLWLockAcquire(&(m_partitionLocks[partitionId].lock), LW_EXCLUSIVE);

    FAULT_INJECTION_CALL_REPLACE(DstoreLockMgrFI::LOCK_HASH_OOM_POINT, nullptr)
    lockEntry = static_cast<LockEntry *>(hash_search_with_hash_value(m_lockTable,
        static_cast<const void *>(tag.lockTag), tag.hashCode, HASH_ENTER, &found));
    FAULT_INJECTION_CALL_REPLACE_END;

    if (STORAGE_VAR_NULL(lockEntry)) {
        storage_set_error(LOCK_ERROR_OUT_OF_MEMORY);
        LWLockRelease(&(m_partitionLocks[partitionId].lock));
        return ret;
    }

    if (!found) {
        lockEntry->Initialize(tag.lockTag);
    }

    ret = lockEntry->EnqueueLockRequest(request, m_freeLists.GetFreeList(partitionId), info);
    if (lockEntry->IsNoHolderAndNoWaiter()) {
        (void)hash_search_with_hash_value(m_lockTable, static_cast<const void *>(tag.lockTag),
                                          tag.hashCode, HASH_REMOVE, nullptr);
    }
    LWLockRelease(&(m_partitionLocks[partitionId].lock));

    return ret;
}

bool LockHashTable::LockRequestDequeue(const LockTagCache &tag, LockRequestInterface *request)
{
    StorageAssert(tag.hashCode == get_hash_value(m_lockTable, static_cast<const void *>(tag.lockTag)));
    uint32 partitionId = tag.hashCode % m_partitionNum;
    LockEntry *lockEntry = nullptr;
    bool isFound = false;

    DstoreLWLockAcquire(&(m_partitionLocks[partitionId].lock), LW_EXCLUSIVE);
    lockEntry = static_cast<LockEntry *>(hash_search_with_hash_value(m_lockTable,
        static_cast<const void *>(tag.lockTag), tag.hashCode, HASH_FIND, nullptr));
    if (lockEntry != nullptr) {
        isFound = lockEntry->DequeueLockRequest(request, m_freeLists.GetFreeList(partitionId));
        if (lockEntry->IsNoHolderAndNoWaiter()) {
            (void)hash_search_with_hash_value(m_lockTable, static_cast<const void *>(tag.lockTag),
                                              tag.hashCode, HASH_REMOVE, nullptr);
        }
    }
    LWLockRelease(&(m_partitionLocks[partitionId].lock));
    return isFound;
}

void LockHashTable::UpdateAllWaitersAndHolders()
{
    HASH_SEQ_STATUS status;
    LockEntry *lock = nullptr;

    AcquireAllPartitionLocks(LW_EXCLUSIVE);

    hash_seq_init(&status, m_lockTable);
    while ((lock = static_cast<LockEntry *>(hash_seq_search(&status))) != nullptr) {
        const void* key = static_cast<const void *>(lock->GetLockTag());
        uint32 partitionId = get_hash_value(m_lockTable, key) % m_partitionNum;

        lock->AdvanceWaitingQueue(m_freeLists.GetFreeList(partitionId));
        if (lock->IsNoHolderAndNoWaiter()) {
            (void)hash_search(m_lockTable, key, HASH_REMOVE, nullptr);
        }
    }

    ReleaseAllPartitionLocks();
}

void LockHashTable::AcquireAllPartitionLocks(LWLockMode mode)
{
    if (unlikely(mode == LW_EXCLUSIVE)) {
        for (uint32 i = 0; i < m_partitionNum; i++) {
            DstoreLWLockAcquire(&(m_partitionLocks[i].lock), LW_EXCLUSIVE);
        }
    } else {
        for (uint32 i = 0; i < m_partitionNum; i++) {
            DstoreLWLockAcquire(&(m_partitionLocks[i].lock), LW_SHARED);
        }
    }
}

void LockHashTable::ReleaseAllPartitionLocks()
{
    for (uint32 i = 0; i < m_partitionNum; i++) {
        LWLockRelease(&(m_partitionLocks[i].lock));
    }
}

/*
 * Determine whether lock entry is already held by the same requester in the same mode.
 */
bool LockHashTable::IsHeldByRequester(const LockTag *tag, LockRequestInterface *request)
{
    StorageAssert(tag != nullptr);
    StorageAssert(request != nullptr);
    bool result = false;
    uint32 hashcode = get_hash_value(m_lockTable,  static_cast<const void *>(tag));
    uint32 partitionId = hashcode % m_partitionNum;
    LockEntry *lock = nullptr;

    DstoreLWLockAcquire(&(m_partitionLocks[partitionId].lock), LW_SHARED);
    bool found = false;
    lock = static_cast<LockEntry *>(hash_search(m_lockTable, static_cast<const void *>(tag), HASH_FIND, &found));
    if (found) {
        if (lock != nullptr && lock->IsHeldByRequester(request)) {
            /* Assert that tag found is being held in the expected mode. */
            StorageAssert((lock->lockEntryCore).m_grantedCnt[request->GetLockMode()] != 0);
            result = true;
        }
    }
    LWLockRelease(&(m_partitionLocks[partitionId].lock));
    return result;
}

#ifdef UT
/* Get lock entry based on lock tag for diagnose and UT. */
LockEntry *LockHashTable::UTGetLockEntry(const LockTag *tag)
{
    StorageAssert(tag != nullptr);
    LockEntry *lock = nullptr;
    bool found = false;
    lock = static_cast<LockEntry *>(hash_search(m_lockTable, static_cast<const void *>(tag), HASH_FIND, &found));
    return lock;
}
#endif

/*
 * Transfer the holder of a lock to the current thread and return information about the previous holder.
 */
RetStatus LockHashTable::TransferSingleLockHolder(const LockTagCache &tagCache, uint32 *oldThrdCoreIdx)
{
    StorageAssert(oldThrdCoreIdx != nullptr);
    RetStatus ret = DSTORE_FAIL;
    uint32 partitionId = tagCache.GetHashCode() % m_partitionNum;
    LockEntry *lock = nullptr;
    DstoreLWLockAcquire(&(m_partitionLocks[partitionId].lock), LW_EXCLUSIVE);
    bool found = false;
    lock = static_cast<LockEntry *>(
        hash_search(m_lockTable, static_cast<const void *>(tagCache.GetLockTag()), HASH_FIND, &found));
    StorageAssert(!found || lock != nullptr);
    if (found && lock != nullptr) {
        LockRequestSkipList::ListIterator iter(&lock->grantedQueue, 0);
        /* We need to cast the request to at least LockRequest so we can get the old owner. */
        LockRequest *holder = dynamic_cast<LockRequest *>(iter.GetNextRequest());
        StorageAssert(holder != nullptr);
        /* Return who the old owner was. */
        *oldThrdCoreIdx = holder->threadCoreIdx;
        /*
         * Update the entry with the new owner. Since there was only 1 entry in the granted queue, we do not need
         * to update the skip list order.
         */
        holder->threadId = thrd->GetThreadId();
        holder->threadCoreIdx = thrd->GetCore()->selfIdx;
        ret = DSTORE_SUCC;
        StorageAssert(iter.GetNextRequest() == nullptr);
    } else {
        storage_set_error(XACTLOCK_ERROR_TRANSFER_LOCK_NOT_HELD);
    }
    LWLockRelease(&(m_partitionLocks[partitionId].lock));
    return ret;
}

RetStatus LockHashTable::DumpByLockTag(const LockTagCache &tagCache, StringInfo str)
{
    LockEntry *lock = nullptr;
    RetStatus ret = DSTORE_SUCC;
    uint32 partitionId = tagCache.GetHashCode() % m_partitionNum;
    DstoreLWLockAcquire(&(m_partitionLocks[partitionId].lock), LW_SHARED);

    lock = static_cast<LockEntry *>(
        hash_search(m_lockTable, static_cast<const void *>(tagCache.GetLockTag()), HASH_FIND, nullptr));
    if (STORAGE_VAR_NULL(lock)) {
        if (STORAGE_FUNC_FAIL(str->append("Lock doesn't exist in lock table for lock tag: ")) ||
            STORAGE_FUNC_FAIL(tagCache.lockTag->DescribeLockTag(str)) || STORAGE_FUNC_FAIL(str->append("\n"))) {
            storage_set_error(LOCK_ERROR_OUT_OF_MEMORY);
            ret = DSTORE_FAIL;
        }
    } else {
        ret = lock->DumpLockEntry(str);
    }

    LWLockRelease(&(m_partitionLocks[partitionId].lock));
    return ret;
}

RetStatus LockHashTable::DescribeState(bool dumpAllLocks, StringInfo str)
{
    HASH_SEQ_STATUS status;
    LockEntry *lock = nullptr;
    RetStatus ret = DSTORE_SUCC;

    AcquireAllPartitionLocks(LW_SHARED);
    ret = str->append("Main lock table configuration: partition number %u.\n", m_partitionNum);
    if (STORAGE_FUNC_FAIL(ret)) {
        ReleaseAllPartitionLocks();
        storage_set_error(LOCK_ERROR_OUT_OF_MEMORY);
        return DSTORE_FAIL;
    }

    uint32 *lockCnts = static_cast<uint32 *>(DstorePalloc0(sizeof(uint32) * m_partitionNum));
    if (STORAGE_VAR_NULL(lockCnts)) {
        ReleaseAllPartitionLocks();
        storage_set_error(LOCK_ERROR_OUT_OF_MEMORY);
        return DSTORE_FAIL;
    }

    hash_seq_init(&status, m_lockTable);
    while ((lock = static_cast<LockEntry *>(hash_seq_search(&status))) != nullptr) {
        const void* key = static_cast<const void *>(lock->GetLockTag());
        uint32 partitionId = get_hash_value(m_lockTable, key) % m_partitionNum;
        lockCnts[partitionId]++;
    }

    for (uint32 i = 0; i < m_partitionNum; i++) {
        ret = str->append("Hash bucket[%u] state: Total locks: %u. Lock request freelist length: %u, max length: %u.\n",
                          i, lockCnts[i], m_freeLists.GetFreeList(i)->GetCurLength(),
                          m_freeLists.GetFreeList(i)->GetMaxLength());
        if (STORAGE_FUNC_FAIL(ret)) {
            ReleaseAllPartitionLocks();
            DstorePfree(lockCnts);
            storage_set_error(LOCK_ERROR_OUT_OF_MEMORY);
            return DSTORE_FAIL;
        }
    }

    if (dumpAllLocks) {
        hash_seq_init(&status, m_lockTable);
        while ((lock = static_cast<LockEntry *>(hash_seq_search(&status))) != nullptr) {
            ret = lock->DumpLockEntry(str);
            if (STORAGE_FUNC_FAIL(ret)) {
                storage_set_error(LOCK_ERROR_OUT_OF_MEMORY);
                break;
            }
        }
    }
    ReleaseAllPartitionLocks();
    DstorePfree(lockCnts);

    return ret;
}

} /* namespace DSTORE */
