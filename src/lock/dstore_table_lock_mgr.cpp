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
 *     TableLockMgr is a type of lock manager that benefit from "fast path".
 *
 *                           Lock()
 *                             |
 *     --------------    --------------    --------------
 *     | local lock |    | local lock |    | local lock |
 *     | fast path  |    | fast path  |    | fast path  |
 *     --------------    --------------    --------------
 *            |                |                  |
 *            -------------------------------------
 *                             |
 *                   ---------------------
 *                   |  main lock table  |
 *                   ---------------------
 *
 *     Fast path contains 2 parts:
 *     1. Per-thread data structure to store weak locks. We allow "weak" relation
 * locks (AccesShareLock, RowShareLock, RowExclusiveLock) to be recorded in the
 * per-thread structure rather than the main lock table. This helps accelerating
 * acquisition and release of locks that rarely conflict.
 *     2. Shared hash array to store strong lock counter. To make the fast-path
 * lock mechanism work, we must have some way of preventing the use of the
 * fast-path when a conflicting lock might be present. We partition the locktag
 * space into FAST_PATH_STRONG_LOCK_HASH_PARTITIONS partitions(check
 * FastPathStrongLockData for detail), and maintain an integer count of
 * the number of "strong" lockers in each partition. When any "strong" lockers
 * are present (which is hopefully not very often), the fast-path mechanism
 * can't be used, and we must fall back to the slower method of pushing matching
 * locks directly into the main lock tables.
 *
 *    The deadlock detector does not know anything about the fast path mechanism,
 * so any locks that might be involved in a deadlock must be transferred from
 * the fast-path queues to the main lock table before the conflicting strong lock
 * start to wait.
 *
 * ---------------------------------------------------------------------------------------
 */

#include "lock/dstore_table_lock_mgr.h"
#include <unistd.h>
#include "securec.h"
#include "framework/dstore_instance.h"
#include "lock/dstore_lock_entry.h"
#include "lock/dstore_lock_hash_table.h"
#include "lock/dstore_lock_thrd_local.h"
#include "transaction/dstore_resowner.h"

namespace DSTORE {

RetStatus TableLockMgr::Initialize(uint32 hashTableSize, uint32 partitionNum)
{
    if (STORAGE_FUNC_FAIL(LockMgr::Initialize(hashTableSize, partitionNum))) {
        Destroy();
        return DSTORE_FAIL;
    }
    (&m_tableLockStats)->Initialize();
    return DSTORE_SUCC;
}

void TableLockMgr::Destroy()
{
    LockMgr::Destroy();
}

RetStatus TableLockMgr::TryMarkStrongLockByFastPath(const LockTagCache &tag)
{
    ThreadLocalLock::MarkStrongLockInFastPath(tag.hashCode);

    if (STORAGE_FUNC_FAIL(TransferWeakLocks(tag))) {
        ThreadLocalLock::UnmarkStrongLockInFastPath(tag.hashCode);
        return DSTORE_FAIL;
    }

    return DSTORE_SUCC;
}

RetStatus TableLockMgr::TransferOneWeakLock(const LockTag *tag, LockMode mode, ThreadId id, uint32 threadCoreIdx)
{
    LockRequest request(mode, id, threadCoreIdx, LockEnqueueMethod::HOLD_BUT_DONT_WAIT);
    LockErrorInfo info = {0};
    RetStatus ret = m_lockTable->LockRequestEnqueue(LockTagCache(tag), &request, &info);
    return ret;
}

/*
 * Transfer all weak lock mode locks matching the given lock tag from per-backend local lock
 * fast-path bits to the main lock table.
 */
RetStatus TableLockMgr::TransferAllModesForWeakLock(const ThreadCore *threadCore, const LockTag *tag,
    ThreadLocalLock::LocalLockEntry *lockEntry)
{
    /* Traverse every weak lock mode to transfer to main lock table if the thread is holding one. */
    for (uint32 lockMode = DSTORE_ACCESS_SHARE_LOCK; lockMode < DSTORE_SHARE_UPDATE_EXCLUSIVE_LOCK; lockMode++) {
        if (!lockEntry->IsGrantedByFastPath(static_cast<LockMode>(lockMode))) {
            continue;
        }

        RetStatus ret = TransferOneWeakLock(tag, static_cast<LockMode>(lockMode), threadCore->pid,
            threadCore->selfIdx);
        if (STORAGE_FUNC_FAIL(ret)) {
            ErrLog(DSTORE_ERROR, MODULE_LOCK,
                   ErrMsg("Failed to transfer weak lock %s in mode %s for thread %lu with core index %u,"
                          "error code: 0x%llx, error message: %s.",
                          tag->ToString().CString(), GetLockModeString(static_cast<LockMode>(lockMode)),
                          threadCore->pid, threadCore->selfIdx, StorageGetErrorCode(), StorageGetMessage()));
            return ret;
        }

        lockEntry->ClearGrantedByFastPath(static_cast<LockMode>(lockMode));
    }
    return DSTORE_SUCC;
}

/*
 * TransferWeakLocks
 *		Transfer locks matching the given lock tag from per-backend fast-path
 *		arrays to the main lock table.
 *
 * Returns true if successful, false if ran out of shared memory.
 */
RetStatus TableLockMgr::TransferWeakLocks(const LockTagCache &tag)
{
    ThreadLocalLock *localLock = nullptr;

    /* Traverse every thread that can potentially hold a fast-path lock. */
    ThreadCoreMgr::ThreadIterator iter(g_storageInstance->GetThreadCoreMgr());
    ThreadCore *threadCore = nullptr;
    while ((threadCore = iter.GetNextThreadCore()) != nullptr) {
        localLock = threadCore->regularLockCtx->GetLocalLock();
        ThreadLocalLock::LocalLockEntry *lockEntry = localLock->LockAndGetLocalLockEntry(tag);
        /* The thread is not holding this lock. */
        if (lockEntry == nullptr) {
            localLock->UnlockLocalLock();
            continue;
        }

        /* Traverse every weak lock mode to transfer to main lock table if the thread is holding one. */
        if (STORAGE_FUNC_FAIL(TransferAllModesForWeakLock(threadCore, tag.lockTag, lockEntry))) {
            localLock->UnlockLocalLock();
            return DSTORE_FAIL;
        }

        if (unlikely(static_cast<uint64>(g_traceSwitch) & TABLELOCK_STATS_TRACE_SWITCH)) {
            TableLockStats *tableLockStats = &m_tableLockStats;
            static_cast<void>(++(tableLockStats->numWeakLockTransfers));
        }

        localLock->UnlockLocalLock();
    }

    return DSTORE_SUCC;
}

RetStatus TableLockMgr::Lock(const LockTag *tag, LockMode mode, bool dontWait, LockErrorInfo *info)
{
    /*
     * LockTable provides additional functionality required by LockInterface:
     * detect whether table lock is already held.
     * Lock() is currently not utilizing isAlreadyHeld.
     */
    bool isAlreadyHeld = false;
    LockAcquireContext lockContext = {.tagCache = LockTagCache(tag), mode, dontWait, info};
    return LockTable(lockContext, isAlreadyHeld);
}

RetStatus TableLockMgr::WaitLazyLockGoneOnAllThreads(const LockTagCache &tagCache, LockMode mode)
{
    LockRequest request(mode, thrd);
    thrd->GetLockCtx()->isWaiting = true;

    RetStatus ret = LockResource::AsyncDisableLazyLockOnAllThreads(tagCache, &request);
    if ((ret == DSTORE_SUCC) || (StorageGetErrorCode() != LOCK_INFO_WAITING)) {
        return ret;
    }
    StorageStat *stat = g_storageInstance->GetStat();
    stat->m_reportWaitEvent(
        static_cast<uint32_t>(tagCache.lockTag->lockTagType | OPTUTIL_GSSTAT_WAIT_LOCK));
    DeadlockDetector detector;
    LockWaitScheduler waitScheduler(&detector, false);
    waitScheduler.StartWaiting();
    for (;;) {
        LockWaitScheduler::WakeupReason reason = waitScheduler.WaitForNextCycle();
        switch (reason) {
            case LockWaitScheduler::DEADLOCK_DETECTED: {
                waitScheduler.FinishWaiting();
                stat->ReportDeadLockTag(mode, tagCache.lockTag);
                stat->m_reportWaitEventFailed(
                    static_cast<uint32_t>(tagCache.lockTag->lockTagType | OPTUTIL_GSSTAT_WAIT_LOCK));
                stat->m_reportWaitEvent(OPTUTIL_GSSTAT_WAIT_EVENT_END);
                storage_set_error(LOCK_ERROR_DEADLOCK);
                return DSTORE_FAIL;
            }
            case LockWaitScheduler::WAIT_TIMEOUT: {
                waitScheduler.FinishWaiting();
                stat->m_reportWaitEventFailed(
                    static_cast<uint32_t>(tagCache.lockTag->lockTagType | OPTUTIL_GSSTAT_WAIT_LOCK));
                stat->m_reportWaitEvent(OPTUTIL_GSSTAT_WAIT_EVENT_END);
                storage_set_error(LOCK_ERROR_WAIT_TIMEOUT);
                return DSTORE_FAIL;
            }
            case LockWaitScheduler::WAIT_CANCELED: {
                waitScheduler.FinishWaiting();
                stat->m_reportWaitEventFailed(
                    static_cast<uint32_t>(tagCache.lockTag->lockTagType | OPTUTIL_GSSTAT_WAIT_LOCK));
                stat->m_reportWaitEvent(OPTUTIL_GSSTAT_WAIT_EVENT_END);
                storage_set_error(LOCK_ERROR_WAIT_CANCELED);
                return DSTORE_FAIL;
            }
            case LockWaitScheduler::NORMAL_WAKEUP: {
                break;
            }
            default: {
                StorageReleasePanic(false, MODULE_LOCK, ErrMsg("lock wakeup reason unexpected."));
            }
        }

        if (!(thrd->GetLockCtx()->isWaiting)) {
            break;
        }
    }

    waitScheduler.FinishWaiting();
    stat->m_reportWaitEvent(OPTUTIL_GSSTAT_WAIT_EVENT_END);
    return DSTORE_SUCC;
}

void TableLockMgr::EnableLazyLockOnAllThreads(const LockTagCache &tagCache, LockMode mode)
{
    LockRequest request(mode, thrd);
    LockResource::EnableLazyLockOnAllThreads(tagCache, &request);
}

void TableLockMgr::TableLockStats::Initialize()
{
    /* Initialize counters. */
    numWeakLockTransfers = 0;
    numFastPathSuccesses = 0;
    numStrongLocksAcquired = 0;
    /* Collect timestamp when initializes table lock statistics. */
    SetTimeStamp();
    LWLockInitialize(&mutex, LWLOCK_GROUP_TABLE_LOCK_STAT_MUTEX);
}

/*
 * Function resets table lock statistics on weak lock and strong lock.
 */
void TableLockMgr::TableLockStats::Reset()
{
    DstoreLWLockAcquire(&mutex, LW_EXCLUSIVE);
    /* Reset weak lock counters. */
    numWeakLockTransfers = 0;
    numFastPathSuccesses = 0;
    /* Reset strong lock counters. */
    numStrongLocksAcquired = 0;
    /* Collect timestamp when resets table lock statistics. */
    SetTimeStamp();
    LWLockRelease(&mutex);
}

/*
 * Collect current timestamp and update TableLockStats with newest timestamp.
 */
void TableLockMgr::TableLockStats::SetTimeStamp()
{
    struct timespec curTime;
    const int thousands = 1000;
    static const int bufferMaxSize = 80;
    static_cast<void>(clock_gettime(CLOCK_REALTIME, &curTime));
    int usesc = static_cast<int>(curTime.tv_nsec) / thousands;
    char buffer[bufferMaxSize] = {0};
    struct tm nowTime;
    static_cast<void *>(localtime_r(&curTime.tv_sec, &nowTime));
    UNUSE_PARAM size_t numBytes = strftime(buffer, sizeof(buffer), "%F %T", &nowTime);
    StorageAssert(numBytes != 0);

    int rc = snprintf_s(resetTimeStamp, sizeof(resetTimeStamp), sizeof(resetTimeStamp) - 1, "%s.%03d", buffer, usesc);
    storage_securec_check_ss(rc);
}

RetStatus TableLockMgr::WeakLockAcquire(LockAcquireContext lockContext, bool &isAlreadyHeld)
{
    StorageAssert(thrd != nullptr);
    StorageAssert(thrd->GetLockCtx() != nullptr);
    ThreadLocalLock *localLock = thrd->GetLockCtx()->GetLocalLock();
    StorageAssert(localLock != nullptr);

    RetStatus ret = localLock->TryGrantByFastPath(lockContext.tagCache, lockContext.mode, GetType(), isAlreadyHeld);
    if (unlikely(static_cast<uint64>(g_traceSwitch) & TABLELOCK_STATS_TRACE_SWITCH)) {
        TableLockStats *tableLockStats = &m_tableLockStats;
        if (STORAGE_FUNC_SUCC(ret) && !isAlreadyHeld) {
            static_cast<void>(++(tableLockStats->numFastPathSuccesses));
        }
    }
    if (STORAGE_FUNC_SUCC(ret)) {
        return ret;
    } else if (StorageGetErrorCode() != LOCK_INFO_NOT_AVAIL) {
        /* unexpected failure. */
        ErrLog(DSTORE_ERROR, MODULE_LOCK,
            ErrMsg("Failed to try grant weak lock %s in mode %s by fast-path,"
                   " error code: 0x%llx, error message: %s.", lockContext.tagCache.lockTag->ToString().CString(),
                   GetLockModeString(lockContext.mode), StorageGetErrorCode(),
                   StorageGetMessage()));
        return ret;
    }

    ret = LockInMainLockTable(lockContext);

    localLock->RecordLockResult(lockContext.tagCache, lockContext.mode, ret);
    return ret;
}

RetStatus TableLockMgr::StrongLockAcquire(LockAcquireContext lockContext, bool &isAlreadyHeld)
{
    StorageAssert(thrd != nullptr);
    StorageAssert(thrd->GetLockCtx() != nullptr);
    ThreadLocalLock *localLock = thrd->GetLockCtx()->GetLocalLock();
    StorageAssert(localLock != nullptr);

    RetStatus ret = localLock->GrantIfAlreadyHold(lockContext.tagCache, lockContext.mode, GetType());
    if (STORAGE_FUNC_SUCC(ret)) {
        isAlreadyHeld = true;
        return ret;
    } else if (StorageGetErrorCode() != LOCK_INFO_NOT_AVAIL) {
        /* unexpected GrantIfAlreadyHold() failure, e.g. out-of-memory. */
        ErrLog(DSTORE_ERROR, MODULE_LOCK,
            ErrMsg("Failed to create local lock %s in mode %s, error code: 0x%llx, error message: %s.",
                lockContext.tagCache.lockTag->ToString().CString(), GetLockModeString(lockContext.mode),
                StorageGetErrorCode(), StorageGetMessage()));
        return ret;
    }

    if (g_storageInstance->GetGuc()->enableLazyLock) {
        if (STORAGE_FUNC_FAIL(WaitLazyLockGoneOnAllThreads(lockContext.tagCache, lockContext.mode))) {
            ErrLog(DSTORE_ERROR, MODULE_LOCK,
                   ErrMsg("Failed to wait lazy lock gone on all threads, error message: %s.", StorageGetMessage()));
            ret = DSTORE_FAIL;
            goto Finish;
        }
    }

    /*
     * ShareUpdateExclusiveLock is self-conflicting, it can't use the fast-path mechanism;
     * but it also does not conflict with any of the locks that do, so we can ignore it completely.
     */
    if (lockContext.mode > DSTORE_SHARE_UPDATE_EXCLUSIVE_LOCK) {
        if (STORAGE_FUNC_FAIL(TryMarkStrongLockByFastPath(lockContext.tagCache))) {
            ret = DSTORE_FAIL;
            goto Finish;
        }
    }

    ret = LockInMainLockTable(lockContext);
    if ((lockContext.mode > DSTORE_SHARE_UPDATE_EXCLUSIVE_LOCK) && STORAGE_FUNC_FAIL(ret)) {
        ThreadLocalLock::UnmarkStrongLockInFastPath(lockContext.tagCache.hashCode);
    }

    if (unlikely(static_cast<uint64>(g_traceSwitch) & TABLELOCK_STATS_TRACE_SWITCH)) {
        if ((lockContext.mode > DSTORE_SHARE_UPDATE_EXCLUSIVE_LOCK) && STORAGE_FUNC_SUCC(ret)) {
            TableLockStats *tableLockStats = &m_tableLockStats;
            static_cast<void>(++(tableLockStats->numStrongLocksAcquired));
        }
    }

Finish:
    if (g_storageInstance->GetGuc()->enableLazyLock && STORAGE_FUNC_FAIL(ret)) {
        EnableLazyLockOnAllThreads(lockContext.tagCache, lockContext.mode);
    }
    localLock->RecordLockResult(lockContext.tagCache, lockContext.mode, ret);
    return ret;
}

/*
 * Main function to acquire table lock.
 *
 * Extends the regular Lock() functionality by making caller
 * to be informed if lock is held already.
 */
RetStatus TableLockMgr::LockTable(LockAcquireContext lockContext, bool &isAlreadyHeld)
{
    if (lockContext.mode < DSTORE_SHARE_UPDATE_EXCLUSIVE_LOCK) {
        return WeakLockAcquire(lockContext, isAlreadyHeld);
    } else {
        return StrongLockAcquire(lockContext, isAlreadyHeld);
    }
}

void TableLockMgr::WeakLockRelease(const LockTag *tag, LockMode mode)
{
    StorageAssert(thrd != nullptr);
    StorageAssert(thrd->GetLockCtx() != nullptr);
    ThreadLocalLock *localLock = thrd->GetLockCtx()->GetLocalLock();
    StorageAssert(localLock != nullptr);
    LockTagCache tagCache(tag);
    if (STORAGE_FUNC_SUCC(localLock->TryReleaseByFastPath(tagCache, mode))) {
        return;
    }

    UnlockInMainLockTable(tagCache, mode);

    (void)localLock->RemoveLockRecord(tagCache, mode);
}

void TableLockMgr::StrongLockRelease(const LockTag *tag, LockMode mode)
{
    StorageAssert(thrd != nullptr);
    StorageAssert(thrd->GetLockCtx() != nullptr);
    ThreadLocalLock *localLock = thrd->GetLockCtx()->GetLocalLock();
    StorageAssert(localLock != nullptr);
    LockTagCache tagCache(tag);
    if (STORAGE_FUNC_SUCC(localLock->UngrantIfGrantedMultipleTimes(tagCache, mode))) {
        return;
    }

    if (g_storageInstance->GetGuc()->enableLazyLock) {
        EnableLazyLockOnAllThreads(tagCache, mode);
    }

    UnlockInMainLockTable(tagCache, mode);

    if (mode > DSTORE_SHARE_UPDATE_EXCLUSIVE_LOCK) {
        ThreadLocalLock::UnmarkStrongLockInFastPath(tagCache.hashCode);
    }

    (void)localLock->RemoveLockRecord(tagCache, mode);
}

void TableLockMgr::Unlock(const LockTag *tag, LockMode mode)
{
    if (mode < DSTORE_SHARE_UPDATE_EXCLUSIVE_LOCK) {
        WeakLockRelease(tag, mode);
    } else {
        StrongLockRelease(tag, mode);
    }
}

void TableLockMgr::WeakLockBatchRelease(const LockTag *tag, LockMode mode, uint32 unlockTimes)
{
    StorageAssert(thrd != nullptr);
    StorageAssert(thrd->GetLockCtx() != nullptr);
    ThreadLocalLock *localLock = thrd->GetLockCtx()->GetLocalLock();
    LockTagCache tagCache(tag);

    bool unlockFinished = false;
    RetStatus ret = localLock->BatchReleaseByFastPath(tagCache, mode, unlockTimes, unlockFinished);
    if (STORAGE_FUNC_FAIL(ret) || unlockFinished) {
        return;
    }

    UnlockInMainLockTable(tagCache, mode);
}

void TableLockMgr::StrongLockBatchRelease(const LockTag *tag, LockMode mode, uint32 unlockTimes)
{
    StorageAssert(thrd != nullptr);
    StorageAssert(thrd->GetLockCtx() != nullptr);
    ThreadLocalLock *localLock = thrd->GetLockCtx()->GetLocalLock();
    LockTagCache tagCache(tag);

    bool needFurtherUnlock = false;
    RetStatus ret = localLock->BatchDecreaseGrantedCount(tagCache, mode, unlockTimes, needFurtherUnlock);
    if (STORAGE_FUNC_FAIL(ret)) {
        return;
    }

    if (!needFurtherUnlock) {
        return;
    }

    if (g_storageInstance->GetGuc()->enableLazyLock) {
        EnableLazyLockOnAllThreads(tagCache, mode);
    }

    UnlockInMainLockTable(tagCache, mode);

    if (mode > DSTORE_SHARE_UPDATE_EXCLUSIVE_LOCK) {
        ThreadLocalLock::UnmarkStrongLockInFastPath(tagCache.hashCode);
    }
}

void TableLockMgr::BatchUnlock(const LockTag *tag, LockMode mode, uint32 unlockTimes)
{
    StorageAssert((mode != DSTORE_LOCK_MODE_MAX) && (mode != DSTORE_NO_LOCK));
    StorageAssert(unlockTimes > 0);

    if (mode < DSTORE_SHARE_UPDATE_EXCLUSIVE_LOCK) {
        WeakLockBatchRelease(tag, mode, unlockTimes);
    } else {
        StrongLockBatchRelease(tag, mode, unlockTimes);
    }
    g_storageInstance->GetStat()->ReportLockStat(DSTORE::StmtDetailType::LOCK_RELEASE, mode, tag);
}

RetStatus TableLockMgr::DumpFastpathSlotModes(ThreadLocalLock::LocalLockEntry *lockEntry, ThreadCore *threadCore,
    StringInfo str, bool& found)
{
    bool hasFastPathLock = false;
    for (uint32 lockMode = DSTORE_ACCESS_SHARE_LOCK; lockMode < DSTORE_SHARE_UPDATE_EXCLUSIVE_LOCK; lockMode++) {
        if (lockEntry->IsGrantedByFastPath(static_cast<LockMode>(lockMode))) {
            hasFastPathLock = true;
            break;
        }
    }

    if (!hasFastPathLock) {
        found = hasFastPathLock;
        return DSTORE_SUCC;
    }
    RetStatus ret = DSTORE_SUCC;
    ret = str->append("  Thread %lu [core index %u] holds lock by fastpath in mode:", threadCore->pid,
                      threadCore->selfIdx);
    if (STORAGE_FUNC_FAIL(ret)) {
        storage_set_error(LOCK_ERROR_OUT_OF_MEMORY);
        return DSTORE_FAIL;
    }
    for (uint32 lockMode = DSTORE_ACCESS_SHARE_LOCK; lockMode < DSTORE_SHARE_UPDATE_EXCLUSIVE_LOCK; lockMode++) {
        if (lockEntry->IsGrantedByFastPath(static_cast<LockMode>(lockMode))) {
            ret = str->append(" %s", GetLockModeString(static_cast<LockMode>(lockMode)));
            if (STORAGE_FUNC_FAIL(ret)) {
                storage_set_error(LOCK_ERROR_OUT_OF_MEMORY);
                return DSTORE_FAIL;
            }
        }
    }
    ret = str->append(".\n");
    if (STORAGE_FUNC_FAIL(ret)) {
        storage_set_error(LOCK_ERROR_OUT_OF_MEMORY);
        return DSTORE_FAIL;
    }
    found = true;
    return DSTORE_SUCC;
}

RetStatus TableLockMgr::DumpFastpathByLockTag(const LockTagCache &tagCache, StringInfo str)
{
    bool found = false;
    RetStatus ret = DSTORE_SUCC;
    ret = str->append("Fast path summary:\n");
    if (STORAGE_FUNC_FAIL(ret)) {
        storage_set_error(LOCK_ERROR_OUT_OF_MEMORY);
        return DSTORE_FAIL;
    }

    /* Traverse every thread that can potentially hold the fast-path lock. */
    ThreadCoreMgr::ThreadIterator iter(g_storageInstance->GetThreadCoreMgr());
    ThreadCore *threadCore = nullptr;
    ThreadLocalLock *localLock = nullptr;
    while ((threadCore = iter.GetNextThreadCore()) != nullptr) {
        localLock = threadCore->regularLockCtx->GetLocalLock();
        ThreadLocalLock::LocalLockEntry *lockEntry = localLock->LockAndGetLocalLockEntry(tagCache);
        /* The thread is not holding this lock. */
        if (lockEntry == nullptr) {
            localLock->UnlockLocalLock();
            continue;
        }

        bool tmpFound = true;
        ret = DumpFastpathSlotModes(lockEntry, threadCore, str, tmpFound);
        found = tmpFound ? true : found;
        localLock->UnlockLocalLock();
        if (STORAGE_FUNC_FAIL(ret)) {
            return DSTORE_FAIL;
        }
    }

    if (!found) {
        ret = str->append("    None.\n");
        if (STORAGE_FUNC_FAIL(ret)) {
            storage_set_error(LOCK_ERROR_OUT_OF_MEMORY);
            return DSTORE_FAIL;
        }
    }
    return DSTORE_SUCC;
}

RetStatus TableLockMgr::DumpByLockTag(const LockTag *tag, StringInfo str)
{
    RetStatus ret = DSTORE_SUCC;
    LockTagCache tagCache(tag);

    if (g_storageInstance->GetGuc()->enableLazyLock) {
        ret = LockResource::DumpLazyLockCntsByLockTag(tagCache, str);
        if (STORAGE_FUNC_FAIL(ret)) {
            return DSTORE_FAIL;
        }
    }

    ret = DumpFastpathByLockTag(tagCache, str);
    if (STORAGE_FUNC_FAIL(ret)) {
        return DSTORE_FAIL;
    }
    ret = LockMgr::DumpByLockTag(tag, str);
    return ret;
}

RetStatus TableLockMgr::DescribeStatus(StringInfo str)
{
    return (STORAGE_FUNC_SUCC(ThreadLocalLock::DescribeStrongLocksInFastPath(str)) &&
            STORAGE_FUNC_SUCC(LockMgr::DescribeStatus(str)))
               ? DSTORE_SUCC
               : DSTORE_FAIL;
}

LockMgrType TableLockMgr::GetType() const
{
    return TABLE_LOCK_MGR;
}

}  // namespace DSTORE
