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

#include "lock/dstore_lock_thrd_local.h"
#include "lock/dstore_lock_mgr.h"
#include "lock/dstore_table_lock_mgr.h"
#include "lock/dstore_xact_lock_mgr.h"
#include "framework/dstore_thread.h"
#include "framework/dstore_instance.h"
#include "framework/dstore_pdb.h"
#include "common/log/dstore_log.h"
#include "framework/dstore_instance.h"
#include "lock/dstore_table_lock_mgr.h"
#include "lock/dstore_xact_lock_mgr.h"
#include "lock/dstore_lock_interface.h"
#include "transaction/dstore_transaction_mgr.h"
#include "lock/dstore_deadlock_detector.h"
#include "lock/dstore_lock_struct.h"

namespace DSTORE {

LocalLock::LocalLock()
    : m_holdLocks(nullptr),
      m_fastLockEntryMapUseCnt(0),
      m_hashMapUseCnt(0)
{
    LWLockInitialize(&m_mutex.lock, LWLOCK_GROUP_LOCAL_LOCK);
    errno_t rc = memset_s(m_fastLockEntryMap, sizeof(m_fastLockEntryMap), 0, sizeof(m_fastLockEntryMap));
    storage_securec_check(rc, "\0", "\0");
}

LocalLock::~LocalLock()
{
    m_holdLocks = nullptr;
}

RetStatus LocalLock::Initialize(DstoreMemoryContext ctx)
{
    HASHCTL info;
    info.keysize = sizeof(LockTag);
    info.entrysize = sizeof(LocalLockEntry);
    info.hash = tag_hash;
    info.hcxt = ctx;
    uint32 flags = (static_cast<uint32>(HASH_ELEM) | static_cast<uint32>(HASH_FUNCTION));
    if (ctx->type == MemoryContextType::SHARED_CONTEXT) {
        flags |= HASH_SHRCTX;
    } else {
        flags |= HASH_CONTEXT;
    }

    /* Setup hash table for hold locks. */
    const long nelem = 256;
    m_holdLocks = hash_create("thread-hold locks hash", nelem, &info, static_cast<int>(flags));
    if (m_holdLocks == nullptr) {
        return DSTORE_FAIL;
    }

    for (uint32 i = 0; i < FAST_LOCK_ENTRY_MAP_MAX_SLOT; i++) {
        m_fastLockEntryMap[i].tag.SetInvalid();
    }
    m_fastLockEntryMapUseCnt = 0;
    m_hashMapUseCnt = 0;

    return DSTORE_SUCC;
}

void LocalLock::Destroy() noexcept
{
    if (m_holdLocks != nullptr) {
        hash_destroy(m_holdLocks);
        m_holdLocks = nullptr;
    }
}

LocalLock::LocalLockEntry *LocalLock::CreateLock(const LockTagCache &tag, LockMgrType mgrType)
{
    bool found = false;
    LocalLockEntry *localLock = nullptr;

    FAULT_INJECTION_CALL_REPLACE(DstoreLockMgrFI::LOCAL_LOCK_OOM_POINT, nullptr)
    uint32 fastHash = tag.hashCode % FAST_LOCK_ENTRY_MAP_MAX_SLOT;
    if (m_fastLockEntryMap[fastHash].tag.IsInvalid() && (m_hashMapUseCnt == 0)) {
        m_fastLockEntryMap[fastHash].Initialize(tag.lockTag, mgrType);
        m_fastLockEntryMapUseCnt++;
        return &m_fastLockEntryMap[fastHash];
    } else if (LockTag::HashCompareFunc(&m_fastLockEntryMap[fastHash].tag, tag.lockTag, sizeof(LockTag)) == 0) {
        return &m_fastLockEntryMap[fastHash];
    }

    localLock = static_cast<LocalLockEntry *>(hash_search_with_hash_value(m_holdLocks,
        static_cast<const void *>(tag.lockTag), tag.hashCode, HASH_ENTER, &found));
    FAULT_INJECTION_CALL_REPLACE_END;
    if (unlikely(localLock == nullptr)) {
        storage_set_error(LOCK_ERROR_OUT_OF_MEMORY);
        return nullptr;
    }

    if (found) {
        return localLock;
    }

    localLock->Initialize(tag.lockTag, mgrType);
    m_hashMapUseCnt++;
    StorageAssert(m_hashMapUseCnt == static_cast<uint32>(hash_get_num_entries(m_holdLocks)));
    return localLock;
}

void LocalLock::GrantLock(LocalLock::LocalLockEntry *entry, LockMode mode) const
{
    /* Record the granted lock. */
    entry->granted[mode]++;
    entry->grantedTotal++;
}

void LocalLock::UnGrantLock(LocalLock::LocalLockEntry *entry, LockMode mode) const
{
    entry->granted[mode]--;
    entry->grantedTotal--;
}

bool LocalLock::IsWaitingLock(LockTag *tag, LockMode *mode)
{
    bool ret = false;
    DstoreLWLockAcquire(&m_mutex.lock, LW_EXCLUSIVE);
    LocalLockEntry *entry = nullptr;
    GetWaitingEntryForLock(&entry, mode);
    if (entry != nullptr) {
        *tag = entry->tag;
        ret = true;
    }
    LWLockRelease(&m_mutex.lock);
    return ret;
}

bool LocalLock::IsHoldingLock(const LockTagCache &tag)
{
    DstoreLWLockAcquire(&m_mutex.lock, LW_EXCLUSIVE);
    bool found = (FindLock(tag) != nullptr);
    LWLockRelease(&m_mutex.lock);
    return found;
}

LocalLock::LocalLockEntry *LocalLock::FindLock(const LockTagCache &tag)
{
    uint32 fastHash = tag.hashCode % FAST_LOCK_ENTRY_MAP_MAX_SLOT;
    if (LockTag::HashCompareFunc(&m_fastLockEntryMap[fastHash].tag, tag.lockTag, sizeof(LockTag)) == 0) {
        return &m_fastLockEntryMap[fastHash];
    }

    bool found = false;
    return static_cast<LocalLockEntry *>(hash_search_with_hash_value(m_holdLocks,
        static_cast<const void *>(tag.lockTag), tag.hashCode, HASH_FIND, &found));
}

void LocalLock::ClearLock(LocalLock::LocalLockEntry *entry, uint32 tagHashCode)
{
    if (!entry->IsEmpty() || !IsClearable(entry)) {
        return;
    }

    uint32 fastHash = tagHashCode % FAST_LOCK_ENTRY_MAP_MAX_SLOT;
    if (LockTag::HashCompareFunc(&m_fastLockEntryMap[fastHash].tag, &(entry->tag), sizeof(LockTag)) == 0) {
        m_fastLockEntryMap[fastHash].tag.SetInvalid();
        m_fastLockEntryMapUseCnt--;
        return;
    }

    void *temp = nullptr;
    temp = hash_search_with_hash_value(m_holdLocks, static_cast<void *>(&(entry->tag)),
                                       tagHashCode, HASH_REMOVE, nullptr);
    StorageReleasePanic((temp == nullptr), MODULE_LOCK, ErrMsg("local lock is corrupted"));
    m_hashMapUseCnt--;
    StorageAssert(m_hashMapUseCnt == static_cast<uint32>(hash_get_num_entries(m_holdLocks)));
}

RetStatus LocalLock::GrantIfAlreadyHold(const LockTagCache &tag, LockMode mode, LockMgrType mgrType)
{
    DstoreLWLockAcquire(&m_mutex.lock, LW_EXCLUSIVE);
    SetLockScannedForRecovery(false);

    LocalLock::LocalLockEntry *localLock = CreateLock(tag, mgrType);
    if (localLock == nullptr) {
        LWLockRelease(&m_mutex.lock);
        return DSTORE_FAIL;
    }

    /* If we already hold the lock, we can just increase the count locally and return. */
    if (localLock->granted[mode] > 0) {
        GrantLock(localLock, mode);
        LWLockRelease(&m_mutex.lock);
        return DSTORE_SUCC;
    }

    /* Or else, record lock as "waiting" and wait for global lock acquiring. */
    SetWaitingEntryForLock(localLock, mode);
    LWLockRelease(&m_mutex.lock);
    /*
     * StorageSetErrorCodeOnly() is used to avoid performance penalty
     * of vsnprintf in storage_set_error().
     */
    StorageSetErrorCodeOnly(LOCK_INFO_NOT_AVAIL);
    return DSTORE_FAIL;
}

RetStatus LocalLock::RecordLockResult(const LockTagCache &tag, UNUSE_PARAM LockMode mode, RetStatus status)
{
    LocalLockEntry *lockEntry = nullptr;
    LockMode lockMode = DSTORE_NO_LOCK;
    GetWaitingEntryForLock(&lockEntry, &lockMode);
    StorageAssert(lockEntry != nullptr);
    StorageAssert(lockMode == mode);
    StorageAssert(tag.GetHashCode() == tag_hash(static_cast<const void *>(tag.GetLockTag()), sizeof(LockTag)));
    DstoreLWLockAcquire(&m_mutex.lock, LW_EXCLUSIVE);

    if (STORAGE_FUNC_SUCC(status)) {
        if (GetLockScannedByRecovery()) {
            SetLockScannedForRecovery(false);
            LWLockRelease(&m_mutex.lock);
            storage_set_error(LOCK_ERROR_SCAN_PASSED);
            ErrLog(DSTORE_LOG, MODULE_LOCK, ErrMsg("Lock tag %s could not be recorded. %s",
                tag.lockTag->ToString().CString(), StorageGetMessage()));
            return DSTORE_FAIL;
        } else {
            GrantLock(lockEntry, lockMode);
        }
    } else {
        ClearLock(lockEntry, tag.hashCode);
    }

    /* Clear wait status. */
    SetWaitingEntryForLock(nullptr, DSTORE_NO_LOCK);

    LWLockRelease(&m_mutex.lock);
    return DSTORE_SUCC;
}

RetStatus LocalLock::UngrantIfGrantedMultipleTimes(const LockTagCache &tag, LockMode mode)
{
    DstoreLWLockAcquire(&m_mutex.lock, LW_EXCLUSIVE);
    SetLockScannedForRecovery(false);
    LocalLockEntry *localLockEntry = FindLock(tag);
    /* If we don't own the lock, return. */
    if (unlikely((localLockEntry == nullptr) || (localLockEntry->granted[mode] == 0))) {
        LWLockRelease(&m_mutex.lock);
        /* Assert on debug build will ensure this "logical" error is discovered early. */
        ErrLog(DSTORE_PANIC, MODULE_LOCK,
            ErrMsg("Lock is not owned by the current thread: lock %s %s.",
                tag.lockTag->ToString().CString(), GetLockModeString(mode)));
        return DSTORE_SUCC;

    /* If we own the lock more than once, decrease local counts and return. */
    } else if (localLockEntry->granted[mode] > 1) {
        UnGrantLock(localLockEntry, mode);
        LWLockRelease(&m_mutex.lock);
        return DSTORE_SUCC;
    }
    SetWaitingEntryForUnlock(localLockEntry);
    LWLockRelease(&m_mutex.lock);
    /*
     * StorageSetErrorCodeOnly() is called instead of storage_set_error()
     * due to its performance impact.
     */
    StorageSetErrorCodeOnly(LOCK_INFO_NOT_AVAIL);
    return DSTORE_FAIL;
}

RetStatus LocalLock::BatchDecreaseGrantedCount(const LockTagCache &tag, LockMode mode,
                                               uint32 decreaseCnt, bool &isCntZero)
{
    DstoreLWLockAcquire(&m_mutex.lock, LW_EXCLUSIVE);
    LocalLockEntry *localLockEntry = FindLock(tag);
    if (unlikely(localLockEntry == nullptr)) {
        LWLockRelease(&m_mutex.lock);
        /* Assert on debug build will ensure this "logical" error is discovered early. */
        ErrLog(DSTORE_PANIC, MODULE_LOCK,
            ErrMsg("Lock is not owned by the current thread: lock %s.",
                tag.lockTag->ToString().CString()));
        return DSTORE_FAIL;
    }

    if (unlikely(localLockEntry->granted[mode] < decreaseCnt)) {
        ErrLog(DSTORE_PANIC, MODULE_LOCK,
            ErrMsg("Lock owns %u times < %u times by the current thread: lock %s %s.",
                localLockEntry->granted[mode], decreaseCnt, tag.lockTag->ToString().CString(),
                GetLockModeString(mode)));
        LWLockRelease(&m_mutex.lock);
        return DSTORE_FAIL;
    }

    localLockEntry->granted[mode] -= decreaseCnt;
    localLockEntry->grantedTotal -= decreaseCnt;
    if (localLockEntry->granted[mode] == 0) {
        isCntZero = true;
    }

    ClearLock(localLockEntry, tag.hashCode);

    LWLockRelease(&m_mutex.lock);
    return DSTORE_SUCC;
}

RetStatus LocalLock::RemoveLockRecord(const LockTagCache &tag, LockMode mode)
{
    DstoreLWLockAcquire(&m_mutex.lock, LW_EXCLUSIVE);
    LocalLockEntry *lockEntry = nullptr;
    GetWaitingEntryForUnlock(&lockEntry);
    /* We can safely remove the local lock record if recovery has not scanned this thread. */
    if (GetLockScannedByRecovery()) {
        SetLockScannedForRecovery(false);
        LWLockRelease(&m_mutex.lock);
        storage_set_error(LOCK_ERROR_SCAN_PASSED);
        ErrLog(DSTORE_WARNING, MODULE_LOCK, ErrMsg("Lock tag %s could not be removed. %s",
            tag.lockTag->ToString().CString(), StorageGetMessage()));
        return DSTORE_FAIL;
    }
    StorageAssert(lockEntry != nullptr);
    StorageAssert(lockEntry->tag == *(tag.lockTag));

    /* Delete from local locks. */
    UnGrantLock(lockEntry, mode);
    ClearLock(lockEntry, tag.hashCode);

    SetWaitingEntryForUnlock(nullptr);
    LWLockRelease(&m_mutex.lock);
    return DSTORE_SUCC;
}

bool LocalLock::IsClearable(UNUSE_PARAM const LocalLockEntry *entry)
{
    return true;
}

bool LocalLock::IsEmpty() const
{
    return ((m_fastLockEntryMapUseCnt == 0) && (m_hashMapUseCnt == 0));
}

uint32 LocalLock::GetNumEntries()
{
    DstoreLWLockAcquire(&m_mutex.lock, LW_EXCLUSIVE);
    StorageAssert(m_hashMapUseCnt == static_cast<uint32>(hash_get_num_entries(m_holdLocks)));
    uint32 numEntries = m_hashMapUseCnt + m_fastLockEntryMapUseCnt;
    LWLockRelease(&m_mutex.lock);
    return numEntries;
}

void LocalLock::GetHoldLockCnt(const LockTag *tag, uint32 *granted, uint32 grantedLen)
{
    StorageAssert(grantedLen == static_cast<uint32>(DSTORE_LOCK_MODE_MAX));
    DstoreLWLockAcquire(&m_mutex.lock, LW_EXCLUSIVE);
    LocalLockEntry *localLock = FindLock(LockTagCache(tag));

    for (uint32 i = 0; i < grantedLen; i++) {
        granted[i] = (localLock == nullptr) ? 0 : localLock->granted[i];
    }
    LWLockRelease(&m_mutex.lock);
}

void LocalLock::LocalLockEntry::Initialize(const LockTag *lockTag, LockMgrType mgrType)
{
    tag = *lockTag;
    type = mgrType;
    grantedByFastPath = 0;
    grantedTotal = 0;
    errno_t rc = memset_s(granted, sizeof(uint32) * static_cast<size_t>(DSTORE_LOCK_MODE_MAX), 0,
        sizeof(uint32) * static_cast<size_t>(DSTORE_LOCK_MODE_MAX));
    storage_securec_check(rc, "\0");
}

LockMask LocalLock::LocalLockEntry::GetMask() const
{
    LockMask mask = 0;
    for (int i = static_cast<int>(DSTORE_ACCESS_SHARE_LOCK); i < static_cast<int>(DSTORE_LOCK_MODE_MAX); i++) {
        mask |= (granted[i] > 0) ? (1U << i) : 0U;
    }
    return mask;
}

LocalLock::WaitingLockInfo *ThreadLocalLock::GetWaitingEntry()
{
    return &m_waitingInfo;
}

LocalLock::LocalLockEntry *ThreadLocalLock::GetActiveEntry()
{
    if (m_backupWaitLock != nullptr) {
        return m_backupWaitLock;
    }
    return m_waitingInfo.GetActiveEntry();
}

void LocalLock::SetLockScannedForRecovery(bool scanned)
{
    GetWaitingEntry()->lockScanned = scanned;
}

bool LocalLock::GetLockScannedByRecovery()
{
    return GetWaitingEntry()->lockScanned;
}

void LocalLock::SetWaitingEntryForLock(LocalLockEntry *entry, LockMode mode)
{
    GetWaitingEntry()->lockEntry = entry;
    GetWaitingEntry()->lockMode = mode;
}

void LocalLock::GetWaitingEntryForLock(LocalLockEntry **entry, LockMode *mode)
{
    *entry = GetWaitingEntry()->lockEntry;
    if (GetWaitingEntry()->lockEntry != nullptr && mode != nullptr) {
        *mode = GetWaitingEntry()->lockMode;
    }
}

void LocalLock::SetWaitingEntryForUnlock(LocalLockEntry *entry)
{
    GetWaitingEntry()->unlockEntry = entry;
}

void LocalLock::GetWaitingEntryForUnlock(LocalLockEntry **entry)
{
    *entry = GetWaitingEntry()->unlockEntry;
}

LocalLock::LocalLockEntry *LocalLock::WaitingLockInfo::GetActiveEntry()
{
    StorageAssert(lockEntry == nullptr || unlockEntry == nullptr);
    if (lockEntry != nullptr) {
        return lockEntry;
    }
    return unlockEntry;
}

FastPathStrongLockData::FastPathStrongLockData()
{
    mutex.Init();
    errno_t rc = memset_s(counts, sizeof(uint32) * static_cast<size_t>(FAST_PATH_STRONG_LOCK_HASH_PARTITIONS),
        0, sizeof(uint32) * static_cast<size_t>(FAST_PATH_STRONG_LOCK_HASH_PARTITIONS));
    storage_securec_check(rc, "\0");
}

RetStatus FastPathStrongLockData::DescribeStatus(StringInfo str)
{
    /*
     * Note: there could be race condition when we read the value while some other thread is writing it,
     * but we're just reading it for debugging purposes so we don't have to bother acquiring the mutex,
     * as it might block the ddl lock for a while.
     */
    constexpr uint32 cntsPerLine = 16;
    RetStatus ret = DSTORE_SUCC;
    ret = str->append("Fast path strong lock count by hash partition:\n");
    if (STORAGE_FUNC_FAIL(ret)) {
        storage_set_error(LOCK_ERROR_OUT_OF_MEMORY);
        return DSTORE_FAIL;
    }
    for (uint32 i = 0; i < FAST_PATH_STRONG_LOCK_HASH_PARTITIONS; i++) {
        if (((i + 1) % cntsPerLine) == 0) {
            ret = str->append("%u:%u\n", i, counts[i]);
        } else {
            ret = str->append("%u:%u\t", i, counts[i]);
        }
        if (STORAGE_FUNC_FAIL(ret)) {
            storage_set_error(LOCK_ERROR_OUT_OF_MEMORY);
            return DSTORE_FAIL;
        }
    }
    ret = str->append("\n");
    if (STORAGE_FUNC_FAIL(ret)) {
        storage_set_error(LOCK_ERROR_OUT_OF_MEMORY);
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

FastPathStrongLockData ThreadLocalLock::m_strongLocks;

ThreadLocalLock::ThreadLocalLock()
    : LocalLock(),
      m_waitingInfo(),
      m_waitLocksLen(0),
      m_waitLocks{0},
      m_backupWaitLock(nullptr),
      m_backupWaitMode(DSTORE_NO_LOCK),
      m_backupWaitLocksLen(0),
      m_backupWaitLocks{0}
{
    const uint32 arrayBytes = sizeof(LocalLockEntry *) * m_waitLockMaxCount;
    errno_t rc = memset_s(m_waitLocks, arrayBytes, 0, arrayBytes);
    storage_securec_check(rc, "\0", "\0");
    rc = memset_s(m_backupWaitLocks, arrayBytes, 0, arrayBytes);
    storage_securec_check(rc, "\0", "\0");
}

ThreadLocalLock::~ThreadLocalLock()
{
    SetWaitingEntryForLock(nullptr, DSTORE_NO_LOCK);
    SetWaitingEntryForUnlock(nullptr);
    m_backupWaitLock = nullptr;
}

RetStatus ThreadLocalLock::Initialize(DstoreMemoryContext ctx)
{
    SetWaitingEntryForLock(nullptr, DSTORE_NO_LOCK);
    SetWaitingEntryForUnlock(nullptr);
    return LocalLock::Initialize(ctx);
}

void ThreadLocalLock::CheckForLockLeakage() noexcept
{
    const LocalLockEntry *entry = nullptr;
    HoldLockIterator holdLockIter(this);
    while ((entry = holdLockIter.GetNextLock()) != nullptr) {
        StringInfoData string;
        bool reportSucc = false;
        if (likely(string.init())) {
            RetStatus ret = entry->tag.DescribeLockTag(&string);
            if (STORAGE_FUNC_SUCC(ret)) {
                ErrLog(DSTORE_ERROR, MODULE_LOCK, ErrMsg("Lock(%s) is not released.", string.data));
                reportSucc = true;
            }
            DstorePfreeExt(string.data);
        }
        if (unlikely(!reportSucc)) {
            ErrLog(DSTORE_ERROR, MODULE_LOCK, ErrMsg("Lock(%s, %u, %u, %u, %u, %u) is not released.",
                entry->tag.GetTypeName(), entry->tag.field1, entry->tag.field2, entry->tag.field3,
                entry->tag.field4, entry->tag.field5));
        }
    }
    StorageReleasePanic(!IsEmpty(), MODULE_LOCK, ErrMsg("Found lock leakage before thread exit."));
}

void ThreadLocalLock::Destroy() noexcept
{
#ifndef UT
    CheckForLockLeakage();
#endif
    SetWaitingEntryForLock(nullptr, DSTORE_NO_LOCK);
    SetWaitingEntryForUnlock(nullptr);
    LocalLock::Destroy();
}

RetStatus ThreadLocalLock::TryGrantByFastPath(const LockTagCache &tag, LockMode mode,
    LockMgrType mgrType, bool &isAlreadyHeld)
{
    StorageAssert(isAlreadyHeld == false);
    StorageAssert(!(tag.lockTag->IsInvalid()));
    StorageAssert((mode != DSTORE_NO_LOCK) && (mode < DSTORE_SHARE_UPDATE_EXCLUSIVE_LOCK));
    DstoreLWLockAcquire(&m_mutex.lock, LW_EXCLUSIVE);
    SetLockScannedForRecovery(false);

    LocalLock::LocalLockEntry *localLock = CreateLock(tag, mgrType);
    if (localLock == nullptr) {
        LWLockRelease(&m_mutex.lock);
        return DSTORE_FAIL;
    }

    /* If we already hold the lock, we can just increase the count locally and return. */
    if (localLock->granted[mode] > 0) {
        GrantLock(localLock, mode);
        LWLockRelease(&m_mutex.lock);
        isAlreadyHeld = true;
        return DSTORE_SUCC;
    }

    /*
     * If there is no potential strong lock conflict, we can grant the lock directly from fastpath.
     * Since m_spinLock.Acquire() acts as a memory sequencing point, it's safe to assume that any strong
     * locker whose increment to m_strongLocks->counts becomes visible after we test it has yet
     * to begin to transfer fast-path locks.
     */
    RetStatus ret;
    uint32 fastHashcode = m_strongLocks.GetStrongLockHashPartition(tag.hashCode);
    TsAnnotateBenignRaceSized(&m_strongLocks.counts[fastHashcode], sizeof(uint32));
    if (m_strongLocks.counts[fastHashcode] != 0) {
        /* Potential strong lock conflict exist, set waiting state and return fail. */
        SetWaitingEntryForLock(localLock, mode);
        StorageSetErrorCodeOnly(LOCK_INFO_NOT_AVAIL);
        ret = DSTORE_FAIL;
    } else {
        GrantLock(localLock, mode);
        localLock->SetGrantedByFastPath(mode);
        ret = DSTORE_SUCC;
    }

    LWLockRelease(&m_mutex.lock);
    return ret;
}

RetStatus ThreadLocalLock::TryReleaseByFastPath(const LockTagCache &tag, LockMode mode)
{
    DstoreLWLockAcquire(&m_mutex.lock, LW_EXCLUSIVE);
    SetLockScannedForRecovery(false);
    LocalLockEntry *localLockEntry = FindLock(tag);
    /* If we don't own the lock, return. */
    if (unlikely((localLockEntry == nullptr) || (localLockEntry->granted[mode] == 0))) {
        LWLockRelease(&m_mutex.lock);
        /* Panic will ensure that this "logic" error is discovered earlier. */
        ErrLog(DSTORE_PANIC, MODULE_LOCK,
            ErrMsg("Lock is not owned by the current thread: lock %s %s.",
                tag.lockTag->ToString().CString(), GetLockModeString(mode)));
        return DSTORE_SUCC;

    /* If we own the lock more than once, decrease local counts and return. */
    } else if (localLockEntry->granted[mode] > 1) {
        UnGrantLock(localLockEntry, mode);
        LWLockRelease(&m_mutex.lock);
        return DSTORE_SUCC;

    /* If we own the lock by fastpath, release it directly. */
    } else if (localLockEntry->IsGrantedByFastPath(mode)) {
        localLockEntry->ClearGrantedByFastPath(mode);
        UnGrantLock(localLockEntry, mode);
        ClearLock(localLockEntry, tag.hashCode);
        LWLockRelease(&m_mutex.lock);
        return DSTORE_SUCC;
    }

    SetWaitingEntryForUnlock(localLockEntry);
    LWLockRelease(&m_mutex.lock);
    /*
     * StorageSetErrorCodeOnly() is called instead of storage_set_error()
     * due to its performance impact.
     */
    StorageSetErrorCodeOnly(LOCK_INFO_NOT_AVAIL);
    return DSTORE_FAIL;
}

RetStatus ThreadLocalLock::BatchReleaseByFastPath(const LockTagCache &tag, LockMode mode,
    uint32 decreaseCnt, bool &unlockFinished)
{
    StorageAssert(unlockFinished == false);
    DstoreLWLockAcquire(&m_mutex.lock, LW_EXCLUSIVE);
    LocalLockEntry *localLockEntry = FindLock(tag);
    if (unlikely(localLockEntry == nullptr)) {
        LWLockRelease(&m_mutex.lock);
        /* Panic will ensure that this "logic" error is discovered earlier. */
        ErrLog(DSTORE_PANIC, MODULE_LOCK,
            ErrMsg("Lock is not owned by the current thread: lock %s.",
                tag.lockTag->ToString().CString()));
        return DSTORE_FAIL;
    }

    if (unlikely(localLockEntry->granted[mode] < decreaseCnt)) {
        ErrLog(DSTORE_PANIC, MODULE_LOCK,
            ErrMsg("Lock %s mode %s is owned %u times by the current thread, less than %u times.",
                tag.lockTag->ToString().CString(), GetLockModeString(mode),
                localLockEntry->granted[mode], decreaseCnt));
        LWLockRelease(&m_mutex.lock);
        return DSTORE_FAIL;
    }

    localLockEntry->granted[mode] -= decreaseCnt;
    localLockEntry->grantedTotal -= decreaseCnt;
    if (localLockEntry->granted[mode] == 0) {
        if (localLockEntry->IsGrantedByFastPath(mode)) {
            localLockEntry->ClearGrantedByFastPath(mode);
            unlockFinished = true;
        }
        ClearLock(localLockEntry, tag.hashCode);
    } else {
        unlockFinished = true;
    }

    LWLockRelease(&m_mutex.lock);
    return DSTORE_SUCC;
}

void ThreadLocalLock::MarkStrongLockInFastPath(uint32 tagHashCode)
{
    uint32 fastHashcode = m_strongLocks.GetStrongLockHashPartition(tagHashCode);

    m_strongLocks.mutex.Acquire();
    m_strongLocks.counts[fastHashcode]++;
    m_strongLocks.mutex.Release();
}

void ThreadLocalLock::UnmarkStrongLockInFastPath(uint32 tagHashCode)
{
    uint32 fastHashcode = m_strongLocks.GetStrongLockHashPartition(tagHashCode);

    m_strongLocks.mutex.Acquire();
    StorageAssert(m_strongLocks.counts[fastHashcode] > 0);
    m_strongLocks.counts[fastHashcode]--;
    m_strongLocks.mutex.Release();
}

void ThreadLocalLock::CheckStrongLocksInFastPathLeak()
{
    m_strongLocks.mutex.Acquire();
    for (uint32 i = 0; i < FAST_PATH_STRONG_LOCK_HASH_PARTITIONS; i++) {
        StorageReleasePanic(m_strongLocks.counts[i] != 0, MODULE_LOCK,
            ErrMsg("Found strong lock leakage in fast path."));
    }
    m_strongLocks.mutex.Release();
}

uint32 ThreadLocalLock::GetStrongLockCntInPartition(uint32 hashcode)
{
    uint32 cnt = 0;
    uint32 fastHashcode = m_strongLocks.GetStrongLockHashPartition(hashcode);

    m_strongLocks.mutex.Acquire();
    cnt = m_strongLocks.counts[fastHashcode];
    m_strongLocks.mutex.Release();

    return cnt;
}

ThreadLocalLock::LocalLockEntry *ThreadLocalLock::LockAndGetLocalLockEntry(const LockTagCache &tag)
{
    DstoreLWLockAcquire(&m_mutex.lock, LW_EXCLUSIVE);
    return FindLock(tag);
}

void ThreadLocalLock::UnlockLocalLock()
{
    LWLockRelease(&m_mutex.lock);
}

RetStatus ThreadLocalLock::DescribeStrongLocksInFastPath(StringInfo str)
{
    return m_strongLocks.DescribeStatus(str);
}

bool ThreadLocalLock::GetWaitingLocks(LockTag *waitTags, LockMode *waitMode, UNUSE_PARAM uint32 arrayLen,
    uint32 *waitLocksLen)
{
    StorageAssert(waitTags != nullptr && waitMode != nullptr && waitLocksLen != nullptr);
    if (!IsWaitingForMultipleLocks()) {
        bool ret = IsWaitingLock(waitTags, waitMode);
        *waitLocksLen = ret ? 1 : 0;
        return ret;
    }
    DstoreLWLockAcquire(&m_mutex.lock, LW_EXCLUSIVE);
    StorageAssert(m_waitLocksLen <= arrayLen && m_waitLocksLen <= m_waitLockMaxCount);
    *waitLocksLen = m_waitLocksLen;
    for (uint32 i = 0; i < m_waitLocksLen; i++) {
        waitTags[i] = m_waitLocks[i]->tag;
    }
    *waitMode = LOCK_XACT_SHARED_WAIT_LOCK;
    LWLockRelease(&m_mutex.lock);
    return true;
}

RetStatus ThreadLocalLock::RecordWaitingForMultiLocks(const LockTagCache *waitTagCaches, uint32 count,
    LockMode waitMode, LockMgrType mgrType)
{
    DstoreLWLockAcquire(&m_mutex.lock, LW_EXCLUSIVE);

    m_waitLocksLen = count;
    LocalLockEntry *localLockEntry = nullptr;
    uint32 lockedIndex;
    for (lockedIndex = 0; lockedIndex < count; lockedIndex++) {
        localLockEntry = CreateLock(waitTagCaches[lockedIndex], mgrType);
        if (unlikely(localLockEntry == nullptr)) {
            LWLockRelease(&m_mutex.lock);
            RecordLockResultForMultiLocks(waitTagCaches, lockedIndex, DSTORE_FAIL);
            return DSTORE_FAIL;
        }

        /* We should not be already holding the lock. */
        if (localLockEntry->granted[waitMode] > 0) {
            ErrLog(DSTORE_PANIC, MODULE_LOCK, ErrMsg("Current thread should not wait for its own transaction."));
            LWLockRelease(&m_mutex.lock);
            storage_set_error(LOCK_ERROR_NOT_SUPPORTED);
            return DSTORE_FAIL;
        }

        /* Record lock as "waiting" and wait for global lock acquiring. */
        m_waitLocks[lockedIndex] = localLockEntry;
    }

    LWLockRelease(&m_mutex.lock);
    return DSTORE_SUCC;
}

void ThreadLocalLock::RecordLockResultForMultiLocks(const LockTagCache *waitTagCaches, uint32 count,
    UNUSE_PARAM RetStatus ret)
{
    DstoreLWLockAcquire(&m_mutex.lock, LW_EXCLUSIVE);
    StorageAssert(m_waitingInfo.lockEntry == nullptr && m_waitingInfo.lockMode == DSTORE_NO_LOCK);
    StorageAssert(STORAGE_FUNC_FAIL(ret));

    /* Clear wait status. */
    for (uint32 lockedIndex = 0; lockedIndex < count; lockedIndex++) {
        ClearLock(m_waitLocks[lockedIndex], waitTagCaches[lockedIndex].hashCode);
        m_waitLocks[lockedIndex] = nullptr;
    }
    m_waitLocksLen = 0;

    LWLockRelease(&m_mutex.lock);
}

/**
 * When lazy lock is enabled, a thread may acquire a lock while waiting for another.
 * Therefore, it need to back up the waiting information before execution and restore it after finished.
 */
void ThreadLocalLock::BackupWaitingLock()
{
    DstoreLWLockAcquire(&m_mutex.lock, LW_EXCLUSIVE);
    if (m_waitLocksLen == 0) {
        GetWaitingEntryForLock(&m_backupWaitLock, &m_backupWaitMode);
        SetWaitingEntryForLock(nullptr, DSTORE_NO_LOCK);
        LWLockRelease(&m_mutex.lock);
        return;
    }

    const uint32 arrayBytes = sizeof(LocalLockEntry *) * m_waitLockMaxCount;
    errno_t ret = memcpy_s(m_backupWaitLocks, arrayBytes, m_waitLocks, arrayBytes);
    storage_securec_check(ret, "\0", "\0");
    m_backupWaitLocksLen = m_waitLocksLen;
    errno_t rc = memset_s(m_waitLocks, arrayBytes, 0, arrayBytes);
    storage_securec_check(rc, "\0", "\0");
    m_waitLocksLen = 0;
    LWLockRelease(&m_mutex.lock);
}

void ThreadLocalLock::RestoreWaitingLock()
{
    DstoreLWLockAcquire(&m_mutex.lock, LW_EXCLUSIVE);
    if (m_backupWaitLocksLen == 0) {
        SetWaitingEntryForLock(m_backupWaitLock, m_backupWaitMode);
        m_backupWaitLock = nullptr;
        m_backupWaitMode = DSTORE_NO_LOCK;
        LWLockRelease(&m_mutex.lock);
        return;
    }

    const uint32 arrayBytes = sizeof(LocalLockEntry *) * m_waitLockMaxCount;
    errno_t ret = memcpy_s(m_waitLocks, arrayBytes, m_backupWaitLocks, arrayBytes);
    storage_securec_check(ret, "\0", "\0");
    m_waitLocksLen = m_backupWaitLocksLen;
    errno_t rc = memset_s(m_backupWaitLocks, arrayBytes, 0, arrayBytes);
    storage_securec_check(rc, "\0", "\0");
    m_backupWaitLocksLen = 0;
    LWLockRelease(&m_mutex.lock);
}

/*
 * If the thread is waiting for the lock, then it should not be cleared.
 * This happens when lazy lock is enabled, and the thread tries to acquire
 * and release the same lock it was originally waiting for.
 */
bool ThreadLocalLock::IsClearable(const LocalLockEntry *entry)
{
    return (m_backupWaitLock != entry);
}

LocalLock::HoldLockIterator::HoldLockIterator(LocalLock *localLock)
    : m_localLock(localLock),
      m_fastLockEntryIndex(0)
{
    DstoreLWLockAcquire(&(m_localLock->m_mutex.lock), LW_EXCLUSIVE);
    hash_seq_init(&m_status, m_localLock->m_holdLocks);
}

LocalLock::HoldLockIterator::~HoldLockIterator() noexcept
{
    LWLockRelease(&(m_localLock->m_mutex.lock));
    m_localLock = nullptr;
}

const LocalLock::LocalLockEntry *LocalLock::HoldLockIterator::GetNextLock()
{
    if (m_fastLockEntryIndex != FAST_LOCK_ENTRY_MAP_MAX_SLOT) {
        while ((m_fastLockEntryIndex < FAST_LOCK_ENTRY_MAP_MAX_SLOT) &&
               (m_localLock->m_fastLockEntryMap[m_fastLockEntryIndex].tag.IsInvalid())) {
            m_fastLockEntryIndex++;
        }

        if (m_fastLockEntryIndex != FAST_LOCK_ENTRY_MAP_MAX_SLOT) {
            const LocalLockEntry *entry = &m_localLock->m_fastLockEntryMap[m_fastLockEntryIndex];
            m_fastLockEntryIndex++;
            return entry;
        }
    }

    return (const LocalLockEntry *)hash_seq_search(&m_status);
}

LockThreadContext::~LockThreadContext()
{
    m_deadlockState = nullptr;
}

void LockThreadContext::InitializeBasic()
{
    m_tableLockContext.Initialize();
    m_lockErrorInfo.Initialize();
    InitializeLazyLockHint();
}

RetStatus LockThreadContext::Initialize()
{
    if (STORAGE_FUNC_FAIL(m_localLock.Initialize(thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LOCK)))) {
        return DSTORE_FAIL;
    }

    DstoreMemoryContext ctx = g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LOCK);
    m_deadlockState = DstoreNew(ctx) DeadlockThrdState();
    if (unlikely(m_deadlockState == nullptr)) {
        storage_set_error(LOCK_ERROR_OUT_OF_MEMORY);
        m_localLock.Destroy();
        return DSTORE_FAIL;
    }
    m_deadlockState->Initialize();
    m_tableLockContext.Initialize();
    InitializeLazyLockHint();
    isWaiting = false;
    m_lockErrorInfo.Initialize();
    return DSTORE_SUCC;
}

void LockThreadContext::InitializeLazyLockHint()
{
    for (uint32 partId = 0; partId < LazyLockHint::LAZY_LOCK_HINT_PART_CNT; partId++) {
        m_lazyLockHint[partId].Initialize(partId);
    }
}

void LockThreadContext::Destroy()
{
    m_localLock.Destroy();
    m_tableLockContext.Destroy();
    if (m_deadlockState != nullptr) {
        m_deadlockState->Destroy();
        delete m_deadlockState;
        m_deadlockState = nullptr;
    }
}

LockWaitScheduler::LockWaitScheduler(DeadlockDetector *detector, bool mustWaitFinish)
    : m_deadlockState(thrd->GetLockCtx()->GetDeadlockState()),
      m_deadlockDetector(detector),
      m_waitTag(nullptr),
      m_waitMode(DSTORE_NO_LOCK),
      m_startWaitTime(0),
      m_mustWaitFinish(mustWaitFinish)
{
    static constexpr uint64 MICROSEC_PER_MILLISEC = 1000;
    m_deadlockCheckInterval =
        static_cast<uint64>(g_storageInstance->GetGuc()->deadlockTimeInterval) * MICROSEC_PER_MILLISEC;

    if (mustWaitFinish) {
        m_waitTimeout = 0;
    } else {
        m_waitTimeout = (g_storageInstance->m_lockWaitTimeoutCallBack == nullptr) ? 0 :
            (static_cast<uint64>(g_storageInstance->m_lockWaitTimeoutCallBack()) * MICROSEC_PER_MILLISEC);
    }
}

LockWaitScheduler::~LockWaitScheduler()
{
    m_deadlockState = nullptr;
    m_deadlockDetector = nullptr;
}

void LockWaitScheduler::StartWaiting()
{
    m_startWaitTime = GetSystemTimeInMicrosecond();
    m_deadlockState->StartWaiting();
}

void LockWaitScheduler::FinishWaiting()
{
    m_deadlockState->EndWaiting();
}

void LockWaitScheduler::GetLockWaitErrorInfo()
{
    char *sqlStat = nullptr;
    bool getStat = false;
    if (STORAGE_VAR_NULL(thrd) || STORAGE_VAR_NULL(thrd->GetLockCtx())) {
        return;
    }
    LockErrorInfo *errInfo = thrd->GetLockCtx()->GetLockErrorInfo();
    if (!STORAGE_VAR_NULL(errInfo)) {
        if ((errInfo->threadId != (ThreadId)0) &&
            DeadlockDetector::GetSQLStatementForThread(errInfo->threadId, &sqlStat) == DSTORE_SUCC &&
            sqlStat != nullptr) {
            getStat = true;
        }
        ErrLog(DSTORE_ERROR, MODULE_LOCK,
            ErrMsg("Lock wait timeout: blocked by%sthread %lu, statement <%s>,%slockmode %s.",
            errInfo->isHolder ? " hold lock " : " lock requested waiter ", errInfo->threadId,
            getStat ? sqlStat : "abnormal", errInfo->isHolder ? " hold " : " requested ",
            GetLockModeString(errInfo->lockMode)));
    }
    DstorePfreeExt(sqlStat);
}

RetStatus LockWaitScheduler::GetLockWaitReportString(StringInfo lockTags, LockMode &lockMode)
{
    RetStatus ret = DSTORE_SUCC;
    ThreadLocalLock *localLock = thrd->GetLockCtx()->GetLocalLock();
    LockTag waitTags[ThreadLocalLock::m_waitLockMaxCount];
    uint32 waitLocksLen = 0;
    bool hasWaitLock = localLock->GetWaitingLocks(waitTags, &lockMode, ThreadLocalLock::m_waitLockMaxCount,
        &waitLocksLen);
    StorageReleasePanic(!hasWaitLock, MODULE_LOCK, ErrMsg("lock is not waiting when error report."));
    for (uint32 i = 0; i < waitLocksLen; i++) {
        if (STORAGE_FUNC_FAIL(lockTags->append(" ")) ||
            STORAGE_FUNC_FAIL(waitTags[i].DescribeLockTag(lockTags))) {
            ret = DSTORE_FAIL;
            break;
        }
    }
    return ret;
}

void LockWaitScheduler::ReportLockWaitTimeout()
{
    AutoMemCxtSwitch autoSwitch{thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LOCK)};
    LockMode waitMode = DSTORE_NO_LOCK;
    StringInfoData lockTags;
    if (!lockTags.init()) {
        ErrLog(DSTORE_ERROR, MODULE_LOCK,
            ErrMsg("Out of memory when report lock wait timeout."));
        return;
    }
    if (STORAGE_FUNC_SUCC(GetLockWaitReportString(&lockTags, waitMode))) {
        ErrLog(DSTORE_ERROR, MODULE_LOCK,
            ErrMsg("Wait lock(s):%s in %s mode timeout after %lu us.", lockTags.data,
                GetLockModeString(waitMode), m_waitTimeout));
    } else {
        ErrLog(DSTORE_ERROR, MODULE_LOCK,
            ErrMsg("Out of memory when report lock wait timeout."));
    }
    DstorePfreeExt(lockTags.data);
    GetLockWaitErrorInfo();
}

void LockWaitScheduler::ReportLockWaitCanceled()
{
    AutoMemCxtSwitch autoSwitch{thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LOCK)};
    LockMode waitMode = DSTORE_NO_LOCK;
    StringInfoData lockTags;
    if (!lockTags.init()) {
        ErrLog(DSTORE_ERROR, MODULE_LOCK,
            ErrMsg("Out of memory when report lock wait canceled."));
        return;
    }
    if (STORAGE_FUNC_SUCC(GetLockWaitReportString(&lockTags, waitMode))) {
        ErrLog(DSTORE_ERROR, MODULE_LOCK,
            ErrMsg("Lock wait canceled when waiting for lock(s):%s in %s mode.", lockTags.data,
                GetLockModeString(waitMode)));
    } else {
        ErrLog(DSTORE_ERROR, MODULE_LOCK,
            ErrMsg("Out of memory when report lock wait canceled."));
    }
    DstorePfreeExt(lockTags.data);
}

uint64 LockWaitScheduler::GetNextWaitTime(bool &almostTimeout)
{
    almostTimeout = false;
    uint64 nextWaitTime;
    if (m_waitTimeout != 0) {
        uint64 currentTime = GetSystemTimeInMicrosecond();
        if ((m_startWaitTime + m_waitTimeout) > (currentTime + m_deadlockCheckInterval)) {
            /* It's not going to timeout before the next deadlock detect. */
            nextWaitTime = m_deadlockCheckInterval;
        } else if ((m_startWaitTime + m_waitTimeout) <= currentTime) {
            /* We somehow wait longer than expected. */
            nextWaitTime = 0;
        } else {
            /* We only need to wait a short time before reporting timeout. */
            nextWaitTime = m_startWaitTime + m_waitTimeout - currentTime;
            almostTimeout = true;
        }
    } else {
        nextWaitTime = m_deadlockCheckInterval;
    }
    return nextWaitTime;
}

LockWaitScheduler::WakeupReason LockWaitScheduler::WaitForNextCycle()
{
    bool almostTimeout = false;
    uint64 nextWaitTime = GetNextWaitTime(almostTimeout);
    if (unlikely(nextWaitTime == 0)) {
        StorageAssert(m_waitTimeout != 0);
        ReportLockWaitTimeout();
        return WakeupReason::WAIT_TIMEOUT;
    }

    /* Sleep before next check. */
    constexpr __time_t secondInMs = 1000 * 1000;
    constexpr __time_t msInNs = 1000;
    struct timespec nextWaitTimeInterval;
    nextWaitTimeInterval.tv_sec = static_cast<__time_t>(nextWaitTime / secondInMs);
    nextWaitTimeInterval.tv_nsec = static_cast<long>((nextWaitTime % secondInMs) * msInNs);
    thrd->Sleep(&nextWaitTimeInterval);

    /* Recheck lock's waiting state to avoid other threads modifying is_wating while this thread is sleeping. */
    if (!(thrd->GetLockCtx()->isWaiting)) {
        return WakeupReason::NORMAL_WAKEUP;
    }

    /* Check for lock timeout. */
    if (unlikely(almostTimeout)) {
        StorageAssert(m_waitTimeout != 0);
        uint64 currentTime = GetSystemTimeInMicrosecond();
        if (currentTime >= (m_startWaitTime + m_waitTimeout)) {
            ReportLockWaitTimeout();
            return WakeupReason::WAIT_TIMEOUT;
        }
    }

    /* Check for client-triggered abort. */
    if (!m_mustWaitFinish) {
        if (STORAGE_FUNC_FAIL(thrd->CheckforInterrupts())) {
            ErrLog(DSTORE_ERROR, MODULE_LOCK, ErrMsg("Lock wait is canceled."));
            return WakeupReason::WAIT_CANCELED;
        }
    }

    /* Check for deadlock. */
    if (m_deadlockState->IsDeadlock()) {
        ErrLog(DSTORE_ERROR, MODULE_LOCK, ErrMsg("Deadlock detected."));
        return WakeupReason::DEADLOCK_DETECTED;
    }

    /* Perform deadlock detection and check again. */
    if ((m_deadlockDetector != nullptr) &&
        STORAGE_FUNC_SUCC(m_deadlockDetector->RunDeadlockDetect()) && m_deadlockState->IsDeadlock()) {
        ErrLog(DSTORE_ERROR, MODULE_LOCK, ErrMsg("Deadlock detected."));
        return WakeupReason::DEADLOCK_DETECTED;
    }

    /* Nothing happened. */
    return WakeupReason::NORMAL_WAKEUP;
}

}
