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
 *     LockMgr provides a low-level lock infrastructure, consists of
 * local lock and main lock table.
 *
 *                           Lock()
 *                             |
 *     --------------    --------------    --------------
 *     | local lock |    | local lock |    | local lock |
 *     --------------    --------------    --------------
 *            |                |                  |
 *            -------------------------------------
 *                             |
 *                   ---------------------
 *                   |  main lock table  |
 *                   ---------------------
 *
 * Local lock:
 *     Per-thread collection of holding locks.
 * Main lock table:
 *     Shared lock table to calculate lock conflicts.
 *
 * ---------------------------------------------------------------------------------------
 */
#include "lock/dstore_lock_mgr.h"
#include <unistd.h>
#include "securec.h"
#include "framework/dstore_instance.h"
#include "lock/dstore_lock_hash_table.h"
#include "lock/dstore_lock_thrd_local.h"
#include "common/log/dstore_log.h"

namespace DSTORE {

RetStatus LockMgr::Initialize(uint32 hashTableSize, uint32 partitionNum)
{
    DstoreMemoryContext ctx = g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LOCK);

    /* Init hash table to store lock entry. */
    LockHashTable *lockTable = DstoreNew(ctx) LockHashTable();
    if (lockTable == nullptr) {
        return DSTORE_FAIL;
    }

    if (STORAGE_FUNC_FAIL(lockTable->Initialize(hashTableSize, partitionNum, ctx))) {
        delete lockTable;
        return DSTORE_FAIL;
    }

    m_lockTable = lockTable;
    return DSTORE_SUCC;
}

void LockMgr::Destroy()
{
    if (m_lockTable != nullptr) {
        m_lockTable->Destroy();
        delete m_lockTable;
        m_lockTable = nullptr;
    }
}

RetStatus LockMgr::Lock(const LockTag *tag, LockMode mode, bool dontWait, LockErrorInfo *info)
{
    StorageAssert(thrd != nullptr);
    StorageAssert(thrd->GetLockCtx() != nullptr);
    g_storageInstance->GetStat()->ReportLockStat(DSTORE::StmtDetailType::LOCK_START, mode, tag);
#ifdef LOCK_DEBUG
    LogStartLockAcquire(tag, mode);
#endif
    LockAcquireContext lockContext = {.tagCache = LockTagCache(tag), mode, dontWait, info};

    ThreadLocalLock *localLock = thrd->GetLockCtx()->GetLocalLock();
    StorageAssert(localLock != nullptr);
    RetStatus ret = localLock->GrantIfAlreadyHold(lockContext.tagCache, mode, GetType());
    if (STORAGE_FUNC_SUCC(ret) || StorageGetErrorCode() != LOCK_INFO_NOT_AVAIL) {
        goto Finish;
    }

    ret = LockInMainLockTable(lockContext);

    localLock->RecordLockResult(lockContext.tagCache, mode, ret);

Finish:
#ifdef LOCK_DEBUG
    LogEndLockAcquire(tag, mode, ret);
#endif
    g_storageInstance->GetStat()->ReportLockStat(DSTORE::StmtDetailType::LOCK_END,
        STORAGE_FUNC_FAIL(ret) ? DSTORE_NO_LOCK : mode);
    return ret;
}

void LockMgr::Unlock(const LockTag *tag, LockMode mode)
{
    StorageAssert(thrd != nullptr);
    StorageAssert(thrd->GetLockCtx() != nullptr);
    ThreadLocalLock *localLock = thrd->GetLockCtx()->GetLocalLock();
    StorageAssert(localLock != nullptr);
    LockTagCache tagCache(tag);
    if (STORAGE_FUNC_SUCC(localLock->UngrantIfGrantedMultipleTimes(tagCache, mode))) {
        goto Finish;
    }

    UnlockInMainLockTable(tagCache, mode);

    localLock->RemoveLockRecord(tagCache, mode);

Finish:
#ifdef LOCK_DEBUG
    LogLockRelease(tag, mode);
#endif
    g_storageInstance->GetStat()->ReportLockStat(DSTORE::StmtDetailType::LOCK_RELEASE, mode, tag);
    return;
}

RetStatus LockMgr::LockInMainLockTable(const LockAcquireContext &lockContext)
{
    if (!lockContext.dontWait) {
        thrd->GetLockCtx()->isWaiting = true;
    }
    LockRequest request(lockContext.mode, thrd, lockContext.dontWait ?
                        LockEnqueueMethod::HOLD_BUT_DONT_WAIT : LockEnqueueMethod::HOLD_OR_WAIT);

    RetStatus ret = m_lockTable->LockRequestEnqueue(lockContext.tagCache, &request, lockContext.info);
    if ((ret == DSTORE_SUCC) || (StorageGetErrorCode() != LOCK_INFO_WAITING)) {
        /*
         * We can return here because of
         * 1. Lock was granted.
         * 2. Early deadlock was found.
         * 3. Other errors were found and lock request shouldn't go to the waiting queue.
         */
        return ret;
    }

    if (!STORAGE_VAR_NULL(lockContext.info)) {
        thrd->GetLockCtx()->SetLockErrorInfo(lockContext.info);
    }
    StorageStat *stat = g_storageInstance->GetStat();
    stat->ReportLockStat(DSTORE::StmtDetailType::LOCK_WAIT_START, lockContext.mode,
        lockContext.tagCache.lockTag);
    stat->m_reportWaitEvent(
        static_cast<uint32_t>(lockContext.tagCache.lockTag->lockTagType | OPTUTIL_GSSTAT_WAIT_LOCK));

    DeadlockDetector detector;
    LockWaitScheduler waitScheduler(&detector, GetType() == LockMgrType::LOCK_MGR);
    waitScheduler.StartWaiting();
    for (;;) {
        LockWaitScheduler::WakeupReason reason = waitScheduler.WaitForNextCycle();
        switch (reason) {
            case LockWaitScheduler::DEADLOCK_DETECTED: {
                (void)m_lockTable->LockRequestDequeue(lockContext.tagCache, &request);
                waitScheduler.FinishWaiting();
                stat->ReportLockStat(DSTORE::StmtDetailType::LOCK_WAIT_END);
                stat->ReportDeadLockTag(lockContext.mode, lockContext.tagCache.lockTag);
                stat->m_reportWaitEventFailed(
                    static_cast<uint32_t>(lockContext.tagCache.lockTag->lockTagType | OPTUTIL_GSSTAT_WAIT_LOCK));
                stat->m_reportWaitEvent(OPTUTIL_GSSTAT_WAIT_EVENT_END);
                storage_set_error(LOCK_ERROR_DEADLOCK);
                return DSTORE_FAIL;
            }
            case LockWaitScheduler::WAIT_TIMEOUT: {
                (void)m_lockTable->LockRequestDequeue(lockContext.tagCache, &request);
                waitScheduler.FinishWaiting();
                stat->ReportLockStat(DSTORE::StmtDetailType::LOCK_WAIT_END);
                stat->m_reportWaitEventFailed(
                    static_cast<uint32_t>(lockContext.tagCache.lockTag->lockTagType | OPTUTIL_GSSTAT_WAIT_LOCK));
                stat->m_reportWaitEvent(OPTUTIL_GSSTAT_WAIT_EVENT_END);
                storage_set_error(LOCK_ERROR_WAIT_TIMEOUT);
                return DSTORE_FAIL;
            }
            case LockWaitScheduler::WAIT_CANCELED: {
                (void)m_lockTable->LockRequestDequeue(lockContext.tagCache, &request);
                waitScheduler.FinishWaiting();
                stat->ReportLockStat(DSTORE::StmtDetailType::LOCK_WAIT_END);
                stat->m_reportWaitEventFailed(
                    static_cast<uint32_t>(lockContext.tagCache.lockTag->lockTagType | OPTUTIL_GSSTAT_WAIT_LOCK));
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
    stat->ReportLockStat(DSTORE::StmtDetailType::LOCK_WAIT_END);
    stat->m_reportWaitEvent(OPTUTIL_GSSTAT_WAIT_EVENT_END);
    return DSTORE_SUCC;
}

void LockMgr::UnlockInMainLockTable(const LockTagCache &tag, LockMode mode)
{
    LockRequest request(mode, thrd);
    (void)m_lockTable->LockRequestDequeue(tag, &request);
}

void LockMgr::LogStartLockAcquire(const LockTag *tag, LockMode mode) const
{
    /* ErrLog that we started acquiring lock and will wait if needed. */
    AutoMemCxtSwitch autoSwitch{thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LOCK)};
    StringInfoData lockTagBuf;
    if (unlikely(!lockTagBuf.init())) {
        ErrLog(DSTORE_DEBUG1, MODULE_LOCK,
            ErrMsg("Out of memory when log start lock acquire."));
        return;
    }
    if (STORAGE_FUNC_FAIL(tag->DescribeLockTag(&lockTagBuf))) {
        ErrLog(DSTORE_DEBUG1, MODULE_LOCK,
            ErrMsg("Out of memory when log start lock acquire."));
    } else {
        ErrLog(DSTORE_DEBUG1, MODULE_LOCK,
           ErrMsg("Lock acquiring: lock \"%s\", mode %s.", lockTagBuf.data, GetLockModeString(mode)));
    }
    DstorePfree(lockTagBuf.data);
}

void LockMgr::LogEndLockAcquire(const LockTag *tag, LockMode mode, RetStatus ret) const
{
    /* ErrLog of lock acquisition completion. */
    AutoMemCxtSwitch autoSwitch{thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LOCK)};
    StringInfoData lockTagBuf;
    if (unlikely(!lockTagBuf.init())) {
        ErrLog(DSTORE_DEBUG1, MODULE_LOCK,
            ErrMsg("Out of memory when log end lock acquire."));
        return;
    }
    if (STORAGE_FUNC_FAIL(tag->DescribeLockTag(&lockTagBuf))) {
        ErrLog(DSTORE_DEBUG1, MODULE_LOCK,
            ErrMsg("Out of memory when log end lock acquire."));
    } else {
        ErrLog(DSTORE_DEBUG1, MODULE_LOCK,
            ErrMsg("Lock acquire result%d: lock \"%s\", mode %s.", static_cast<int>(ret), lockTagBuf.data,
                    GetLockModeString(mode)));
    }
    DstorePfree(lockTagBuf.data);
}

void LockMgr::LogLockRelease(const LockTag *tag, LockMode mode) const
{
    /* ErrLog that we've released the lock. */
    AutoMemCxtSwitch autoSwitch{thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LOCK)};
    StringInfoData lockTagBuf;
    if (unlikely(!lockTagBuf.init())) {
        ErrLog(DSTORE_DEBUG1, MODULE_LOCK,
            ErrMsg("Out of memory when log lock release."));
        return;
    }
    if (STORAGE_FUNC_FAIL(tag->DescribeLockTag(&lockTagBuf))) {
        ErrLog(DSTORE_DEBUG1, MODULE_LOCK,
            ErrMsg("Out of memory when log lock release."));
    } else {
        ErrLog(DSTORE_DEBUG1, MODULE_LOCK,
           ErrMsg("Lock released: lock \"%s\", mode %s.", lockTagBuf.data, GetLockModeString(mode)));
    }
    DstorePfree(lockTagBuf.data);
}

LockMgrType LockMgr::GetType() const
{
    return LOCK_MGR;
}

RetStatus LockMgr::DumpByLockTag(const LockTag *tag, StringInfo str)
{
    LockTagCache tagCache(tag);
    return m_lockTable->DumpByLockTag(tagCache, str);
}

RetStatus LockMgr::DescribeStatus(StringInfo str)
{
    return m_lockTable->DescribeState(false, str);
}

}
