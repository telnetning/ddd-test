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
 * Implement the interface of transaction lock
 * 1. lock xid : When transaction start, we need call this interface
 * 2. unlock xid : When transaction commit or abort, we need call this interface
 * 3. wait xid : When lock tuple which is being modified by other transaction, we need call this interface
 * ---------------------------------------------------------------------------------------
 */

#include "lock/dstore_xact_lock_mgr.h"
#include "lock/dstore_lock_entry.h"
#include "lock/dstore_lock_mgr.h"
#include "framework/dstore_instance.h"
#include "lock/dstore_lock_hash_table.h"
#include "common/log/dstore_log.h"
#include "lock/dstore_lock_thrd_local.h"

namespace DSTORE {

/*
 * Lock is only accessible in exclusive mode to the outside caller.
 * For a shared lock, waiting for a transaction to release an
 * exclusive lock, the Wait(xid) function should be used.
 */
RetStatus XactLockMgr::Lock(PdbId pdbId, Xid xid)
{
    LockTag lockTag;
    lockTag.SetTransactionLockTag(pdbId, xid);
    LockErrorInfo info = {0};
    RetStatus ret = LockMgr::Lock(&lockTag, DSTORE_EXCLUSIVE_LOCK, LOCK_WAIT, &info);
    return ret;
}

/*
 * Unlock function which only unlocks under exclusive mode.
 * For shared mode, we use the Wait(xid) function.
 */
void XactLockMgr::Unlock(PdbId pdbId, Xid xid)
{
    LockTag lockTag;
    lockTag.SetTransactionLockTag(pdbId, xid);
    LockMgr::Unlock(&lockTag, DSTORE_EXCLUSIVE_LOCK);
}

/*
 * This function allows for a caller to wait on a transaction
 * that is holding an exclusive lock.
 */
RetStatus XactLockMgr::Wait(PdbId pdbId, Xid xid)
{
    LockTag transactionLockTag;
    transactionLockTag.SetTransactionLockTag(pdbId, xid);
    LockErrorInfo info = {0};
    /* Lock will be obtained only after the Xid transaction ends (unlocks xact lock). */
    if (STORAGE_FUNC_SUCC(LockMgr::Lock(&transactionLockTag, LOCK_XACT_SHARED_WAIT_LOCK, LOCK_WAIT, &info))) {
        LockMgr::Unlock(&transactionLockTag, LOCK_XACT_SHARED_WAIT_LOCK);
        return DSTORE_SUCC;
    }
    return DSTORE_FAIL;
}

void XactLockMgr::LogStartLockAcquires(LockTag *tags, uint32 modeCount) const
{
    for (uint32 i = 0; i < modeCount; i++) {
        LockMgr::LogStartLockAcquire(&tags[i], LOCK_XACT_SHARED_WAIT_LOCK);
    }
}

void XactLockMgr::LogEndLockAcquires(LockTag *tags, uint32 modeCount, RetStatus ret) const
{
    for (uint32 i = 0; i < modeCount; i++) {
        LockMgr::LogEndLockAcquire(&tags[i], LOCK_XACT_SHARED_WAIT_LOCK, ret);
    }
}

void XactLockMgr::UnlockAll(const LockTagCache *tagCaches, uint32 dequeueLen) const
{
    /* Unlock all. */
    for (uint32 lockedIndex = 0; lockedIndex < dequeueLen; lockedIndex++) {
        LockMode mode = LOCK_XACT_SHARED_WAIT_LOCK;
        LockRequest request(mode, thrd);
        (void)m_lockTable->LockRequestDequeue(tagCaches[lockedIndex], &request);
    }
}

RetStatus XactLockMgr::WaitForAnyTransactionEndForMultiWaitLock(const LockTagCache *tagCaches, uint32 arrayLen,
    LockRequest *request)
{
    StorageStat *stat = g_storageInstance->GetStat();
    ThreadLocalLock *threadLocalLock = thrd->GetLockCtx()->GetLocalLock();
    bool needWait = true;
    uint32 dequeueLen = arrayLen;
    RetStatus ret = threadLocalLock->RecordWaitingForMultiLocks(tagCaches, arrayLen,
                                                                LOCK_XACT_SHARED_WAIT_LOCK, GetType());
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_LOCK,
            ErrMsg("Record wait for multi locks failed with error code: 0x%llx, error message: %s.",
            static_cast<unsigned long long>(StorageGetErrorCode()), StorageGetMessage()));
        return DSTORE_FAIL;
    }
    ErrorCode errorCode = 0;
    thrd->GetLockCtx()->isWaiting = true;
    LockErrorInfo info = {0};
    for (uint32 lockedIndex = 0; lockedIndex < arrayLen; lockedIndex++) {
        ret = m_lockTable->LockRequestEnqueue(tagCaches[lockedIndex], request, &info);
        if (ret == DSTORE_SUCC) {
            needWait = false;
        } else if (StorageGetErrorCode() != LOCK_INFO_WAITING) {
            ErrLog(DSTORE_ERROR, MODULE_LOCK,
                ErrMsg("Lock enqueue failed with unexpected error code: 0x%llx, error message: %s.",
                static_cast<unsigned long long>(StorageGetErrorCode()), StorageGetMessage()));
            dequeueLen = lockedIndex;
            ret = DSTORE_FAIL;
            goto EXIT;
        } else {
            thrd->GetLockCtx()->SetLockErrorInfo(&info);
        }
    }
    if (needWait) {
        ret = DSTORE_SUCC;
        stat->ReportLockStat(DSTORE::StmtDetailType::LOCK_WAIT_START,
            LOCK_XACT_SHARED_WAIT_LOCK, tagCaches->lockTag);
        stat->m_reportWaitEvent(
            static_cast<uint32_t>(tagCaches->lockTag->lockTagType | OPTUTIL_GSSTAT_WAIT_LOCK));

        DeadlockDetector detector;
        LockWaitScheduler waitScheduler(&detector, false);
        waitScheduler.StartWaiting();
        for (;;) {
            LockWaitScheduler::WakeupReason reason = waitScheduler.WaitForNextCycle();
            switch (reason) {
                case LockWaitScheduler::DEADLOCK_DETECTED: {
                    waitScheduler.FinishWaiting();
                    errorCode = LOCK_ERROR_DEADLOCK;
                    ret = DSTORE_FAIL;
                    stat->ReportDeadLockTag(LOCK_XACT_SHARED_WAIT_LOCK, tagCaches->lockTag);
                    stat->ReportLockStat(DSTORE::StmtDetailType::LOCK_WAIT_END);
                    stat->m_reportWaitEventFailed(
                        static_cast<uint32_t>(tagCaches->lockTag->lockTagType | OPTUTIL_GSSTAT_WAIT_LOCK));
                    stat->m_reportWaitEvent(OPTUTIL_GSSTAT_WAIT_EVENT_END);
                    goto EXIT;
                }
                case LockWaitScheduler::WAIT_TIMEOUT: {
                    waitScheduler.FinishWaiting();
                    errorCode = LOCK_ERROR_WAIT_TIMEOUT;
                    ret = DSTORE_FAIL;
                    stat->ReportLockStat(DSTORE::StmtDetailType::LOCK_WAIT_END);
                    stat->m_reportWaitEventFailed(
                        static_cast<uint32_t>(tagCaches->lockTag->lockTagType | OPTUTIL_GSSTAT_WAIT_LOCK));
                    stat->m_reportWaitEvent(OPTUTIL_GSSTAT_WAIT_EVENT_END);
                    goto EXIT;
                }
                case LockWaitScheduler::WAIT_CANCELED: {
                    waitScheduler.FinishWaiting();
                    errorCode = LOCK_ERROR_WAIT_CANCELED;
                    ret = DSTORE_FAIL;
                    stat->ReportLockStat(DSTORE::StmtDetailType::LOCK_WAIT_END);
                    stat->m_reportWaitEventFailed(
                        static_cast<uint32_t>(tagCaches->lockTag->lockTagType | OPTUTIL_GSSTAT_WAIT_LOCK));
                    g_storageInstance->GetStat()->m_reportWaitEvent(OPTUTIL_GSSTAT_WAIT_EVENT_END);
                    goto EXIT;
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
    }

EXIT:
    UnlockAll(tagCaches, dequeueLen);
    /* Remove waiting information from local lock. */
    threadLocalLock->RecordLockResultForMultiLocks(tagCaches, arrayLen, DSTORE_FAIL);
    if (!needWait) {
        return DSTORE_SUCC;
    }
    if ((STORAGE_FUNC_FAIL(ret)) && (errorCode != 0)) {
        storage_set_error(errorCode);
    }
    return ret;
}

RetStatus XactLockMgr::WaitForAnyTransactionEnd(PdbId *pdbIds, const Xid *xids, uint32 arrayLen)
{
    LockTag tags[ThreadLocalLock::m_waitLockMaxCount];
    for (uint32 i = 0; i < arrayLen; i++) {
        tags[i].SetTransactionLockTag(pdbIds[i], xids[i]);
    }
#ifdef LOCK_DEBUG
    LogStartLockAcquires(tags, arrayLen);
#endif
    LockTagCache tagCaches[ThreadLocalLock::m_waitLockMaxCount];
    for (uint32 i = 0; i < arrayLen; i++) {
        tagCaches[i] = LockTagCache(&tags[i]);
    }
    LockRequest request(LOCK_XACT_SHARED_WAIT_LOCK, thrd);
    RetStatus ret = WaitForAnyTransactionEndForMultiWaitLock(tagCaches, arrayLen, &request);
#ifdef LOCK_DEBUG
    LogEndLockAcquires(tags, arrayLen, ret);
#endif
    return ret;
}

/*
 * Transfer the holder of a xact lock from one thread to another thread.
 * This function is used when client re-connect to the same node but needs a different to lock
 * the same transaction.
 */
RetStatus XactLockMgr::TransferXactLockHolder(PdbId pdbId, Xid xid)
{
    LockTag tag;
    tag.SetTransactionLockTag(pdbId, xid);
    LockTagCache tagCache(&tag);
    ThreadLocalLock *localLock = thrd->GetLockCtx()->GetLocalLock();
    StorageAssert(localLock != nullptr);
    RetStatus ret = localLock->GrantIfAlreadyHold(tagCache, DSTORE_EXCLUSIVE_LOCK, GetType());
    if (STORAGE_FUNC_SUCC(ret)) {
        /* We're already holding the lock. Undo the duplicate grant. */
        ret = localLock->UngrantIfGrantedMultipleTimes(tagCache, DSTORE_EXCLUSIVE_LOCK);
        StorageAssert(STORAGE_FUNC_SUCC(ret));
        ErrLog(DSTORE_WARNING, MODULE_LOCK,
            ErrMsg("XactLockMgr Transfer Lock: Lock already held (pdbId = %hhu, zoneId = %lu, slotId = %lu).",
                pdbId, (uint64_t)xid.m_zoneId, xid.m_logicSlotId));
        return ret;
    } else if (StorageGetErrorCode() != LOCK_INFO_NOT_AVAIL) {
        ErrLog(DSTORE_ERROR, MODULE_LOCK,
            ErrMsg("XactLockMgr Transfer Lock: Failed GrantIfAlreadyHold for (pdbId = %hhu, zoneId = %lu, "
            "slotId = %lu) with errCode = 0x%llx. %s.", pdbId, (uint64_t)xid.m_zoneId, xid.m_logicSlotId,
            StorageGetErrorCode(), StorageGetMessage()));
        return ret;
    }

    uint32 oldThrdCoreIdx;
    ret = m_lockTable->TransferSingleLockHolder(tagCache, &oldThrdCoreIdx);
    if (STORAGE_FUNC_FAIL(ret)) {
        localLock->RecordLockResult(tagCache, DSTORE_EXCLUSIVE_LOCK, ret);
        ErrLog(DSTORE_ERROR, MODULE_LOCK,
            ErrMsg("XactLockMgr Transfer Lock: Failed TransferSingleLockHolder for (pdbId = %hhu, zoneId = %lu, "
                "slotId = %lu) with errCode = 0x%llx. %s.", pdbId, (uint64_t)xid.m_zoneId, xid.m_logicSlotId,
                StorageGetErrorCode(), StorageGetMessage()));
        return ret;
    }
    /*
     * If there is still a previous holder, TAC will guarantee that the old holder's thread was gracefully
     * destroyed and the thread's local lock was not cleaned up. For this reason, we may have to remove
     * the old lock record from the previous holder.
     */
    /*
     * todo: Add back this code once TAC has delivered their guarantee above
     *
     * ThreadCore *core = g_storageInstance->GetThreadCoreMgr()->GetSpecifiedCore(oldThrdCoreIdx);
     * if (core != nullptr && core->regularLockCtx->GetLocalLock()->IsHoldingLock(tagCache)) {
     *     while (STORAGE_FUNC_SUCC(core->regularLockCtx->GetLocalLock()->
     *         UngrantIfGrantedMultipleTimes(tagCache, DSTORE_EXCLUSIVE_LOCK))) {}
     *     core->regularLockCtx->GetLocalLock()->RemoveLockRecord(tagCache, DSTORE_EXCLUSIVE_LOCK);
     * }
    */

    /* Update local lock. */
    localLock->RecordLockResult(tagCache, DSTORE_EXCLUSIVE_LOCK, ret);
    ErrLog(DSTORE_WARNING, MODULE_LOCK,
        ErrMsg("XactLockMgr Transfer Lock: Lock successfully transferred (pdbId = %hhu, zoneId = %lu, slotId = %lu).",
            pdbId, (uint64_t)xid.m_zoneId, xid.m_logicSlotId));
    return ret;
}

RetStatus XactLockMgr::DumpTrxLockInfo(PdbId pdbId, Xid xid, StringInfo str)
{
    LockTag tag;
    tag.SetTransactionLockTag(pdbId, xid);
    LockTagCache tagCache(&tag);
    return m_lockTable->DumpByLockTag(tagCache, str);
}

} /* namespace DSTORE */
