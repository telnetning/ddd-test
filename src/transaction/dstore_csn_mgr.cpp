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
 * ---------------------------------------------------------------------------------------
 *
 * dstore_csn.cpp
 *
 *
 * IDENTIFICATION
 *        storage/src/transaction/dstore_csn.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "transaction/dstore_csn_mgr.h"
#include "common/dstore_datatype.h"
#include "framework/dstore_instance.h"
#include "common/instrument/trace/dstore_transaction_trace.h"
#include "common/log/dstore_log.h"
#include "diagnose/dstore_csn_mgr_diagnose.h"

namespace DSTORE {

CsnMgr::CsnMgr(ThreadCoreMgr *threadCoreMgr, ControlFile *controlFile, CommitSeqNo csnAssignmentIncrement)
    : m_localCsnMin(COMMITSEQNO_FIRST_NORMAL),
      m_flashbackCsnMin(INVALID_CSN),
      m_backupRestoreCsnMin(INVALID_CSN),
      m_localBarrierCsnMin(INVALID_CSN),
      m_threadCoreMgr(threadCoreMgr),
      m_controlFile(controlFile),
      m_csnAssignmentIncrement(csnAssignmentIncrement),
      m_sendBarrierCsn(nullptr),
      m_nextCsn(COMMITSEQNO_FIRST_NORMAL),
      m_maxReservedCsn(COMMITSEQNO_FIRST_NORMAL),
      m_tacOrphanTrxTracker(nullptr)
{
    LWLockInitialize(&m_maxReservedCsnLock, LWLOCK_GROUP_MAX_RESERVED_CSN);
    LWLockInitialize(&m_updateLocalCsnMinLock, LWLOCK_GROUP_UPDATE_LOCAL_CSN_MIN);
}

RetStatus CsnMgr::Initialize()
{
    storage_trace_entry(TRACE_ID_CsnMgr__Initialize);
    CommitSeqNo oldMaxReservedCsn;
    RetStatus status = DSTORE_SUCC;
    /* Note that CSN is persisted in control file. */
    if (STORAGE_VAR_NULL(m_controlFile)) {
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION, ErrMsg("Control file is null when init csnmgr."));
        goto ErrorExit;
    }

    if (STORAGE_FUNC_FAIL(m_controlFile->GetMaxReservedCSN(oldMaxReservedCsn))) {
        goto ErrorExit;
    }
    /*
     * It's ok here to set nextCsn to oldMaxReservedCsn as the oldMaxReservedCsn would have accommodated
     * the range for the CSN + 1 done by the last commit.
     */
    StorageAssert(m_nextCsn == COMMITSEQNO_FIRST_NORMAL);
    GsAtomicWriteU64(&m_nextCsn, oldMaxReservedCsn + 1);
    GsAtomicWriteU64(&m_maxReservedCsn, oldMaxReservedCsn);
    SetLocalCsnMin(m_nextCsn);
    ErrLog(DSTORE_LOG, MODULE_TRANSACTION, ErrMsg("Initialize m_nextCsn:%lu", m_nextCsn));

    /* For tac feature */
    if (g_storageInstance->GetGuc()->tacGracePeriod > 0) {
        m_tacOrphanTrxTracker = DstoreNew(g_dstoreCurrentMemoryContext) TacOrphanTrxTracker();
        if (STORAGE_VAR_NULL(m_tacOrphanTrxTracker)) {
            ErrLog(DSTORE_ERROR, MODULE_TRANSACTION, ErrMsg("Alloc m_tacOrphanTrxTracker failed."));
            goto ErrorExit;
        }
        m_tacOrphanTrxTracker->Init();
    }

Finish:
    storage_trace_exit(TRACE_ID_CsnMgr__Initialize);
    return status;

ErrorExit:
    status = DSTORE_FAIL;
    goto Finish;
}
 
RetStatus CsnMgr::PromoteInitialize()
{
    /* Note that CSN is persisted in control file. */
    if (STORAGE_VAR_NULL(m_controlFile)) {
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION, ErrMsg("Control file is null when promote initialize."));
        return DSTORE_FAIL;
    }
    DstoreLWLockAcquire(&m_updateLocalCsnMinLock, LW_EXCLUSIVE);
    CommitSeqNo oldMaxReservedCsn;
    if (STORAGE_FUNC_FAIL(m_controlFile->GetMaxReservedCSN(oldMaxReservedCsn))) {
        LWLockRelease(&m_updateLocalCsnMinLock);
        return DSTORE_FAIL;
    }
    /*
     * It's ok here to set nextCsn to oldMaxReservedCsn as the oldMaxReservedCsn would have accommodated
     * the range for the CSN + 1 done by the last commit.
     */
    GsAtomicWriteU64(&m_nextCsn, oldMaxReservedCsn + 1);
    GsAtomicWriteU64(&m_maxReservedCsn, oldMaxReservedCsn);
    SetLocalCsnMin(m_nextCsn);
    ErrLog(DSTORE_LOG, MODULE_TRANSACTION, ErrMsg("Promote initialize m_nextCsn:%lu", m_nextCsn));
    LWLockRelease(&m_updateLocalCsnMinLock);
    return DSTORE_SUCC;
}

void CsnMgr::Destroy()
{
    if (m_tacOrphanTrxTracker != nullptr) {
        m_tacOrphanTrxTracker->Destroy();
        delete m_tacOrphanTrxTracker;
        m_tacOrphanTrxTracker = nullptr;
    }
}

/*
 * Get current csn and push csn++ (if advance=true) when transaction committing.
 */
RetStatus CsnMgr::GetNextCsn(CommitSeqNo& outNextCsn, bool advance)
{
    storage_trace_entry(TRACE_ID_CsnMgr__GetNextCsn);
    RetStatus status = DSTORE_SUCC;
    CommitSeqNo curCsn = INVALID_CSN;
    if (!advance) {
        curCsn = GsAtomicReadU64(&m_nextCsn);
        goto Finish;
    }

    curCsn = GsAtomicFetchAddU64(&m_nextCsn, 1);
    /* use the condition to avoid lock contention */
    if (curCsn % m_csnAssignmentIncrement == COMMITSEQNO_FIRST_NORMAL) {
        if (STORAGE_FUNC_FAIL(UpdateMaxReservedCsnIfNecessary(curCsn))) {
            goto ErrorExit;
        }
    }

    /*
     * For any csn higher than current max reserved csn,
     * wait here until finish updating new max reserved csn.
     */
    while (unlikely(curCsn > GetMaxReservedCsn())) {
        (void)usleep(waitCsnUpdateTimeUs);
    }

Finish:
    outNextCsn = curCsn;
    storage_trace_exit(TRACE_ID_CsnMgr__GetNextCsn);
    return status;

ErrorExit:
    status = DSTORE_FAIL;
    curCsn = INVALID_CSN;
    goto Finish;
}

/*
 * Calculate global min csn from all nodes.
 */
void CsnMgr::UpdateLocalCsnMin()
{
    /* The lock can avoid update local csn min concurrently. */
    DstoreLWLockAcquire(&m_updateLocalCsnMinLock, LW_EXCLUSIVE);
    CommitSeqNo nextCsn = GsAtomicReadU64(&m_nextCsn); /* Get NextCsn before we get local min csn */
    CommitSeqNo newLocalCsnMin = m_threadCoreMgr->GetLocalCsnMin();
    CommitSeqNo oldLocalCsnMin = m_localCsnMin.load();
    newLocalCsnMin = (newLocalCsnMin < nextCsn) ? newLocalCsnMin : nextCsn;

    /*
     * For TAC in single mode, we will add firstStatementCsn into tracker when session is disconnected.
     * So when computing localCsnMin we should consider the csn of tracker to prevent advancing recycleCsnMin.
     */
    CommitSeqNo tacCsnMin = newLocalCsnMin;
    if (m_tacOrphanTrxTracker != nullptr) {
        tacCsnMin = m_tacOrphanTrxTracker->GetSmallestOrphanCsn();
        if (unlikely(DstoreCsnIsValid(tacCsnMin) && tacCsnMin < newLocalCsnMin)) {
            newLocalCsnMin = tacCsnMin;
        }
    }

    StorageReleasePanic(newLocalCsnMin < oldLocalCsnMin, MODULE_TRANSACTION,
        ErrMsg("Invalid new localCsnMin:%lu < old LocalCsnMin:%lu, nextCsn:%lu",
            newLocalCsnMin, oldLocalCsnMin, nextCsn));

    CommitSeqNo tmpLocalCsnMin = m_localCsnMin.load(std::memory_order_acquire);
    while (tmpLocalCsnMin < newLocalCsnMin) {
        /* Advance local csn min */
        if (m_localCsnMin.compare_exchange_weak(tmpLocalCsnMin, newLocalCsnMin, std::memory_order_acq_rel)) {
            break;
        }
    }
    LWLockRelease(&m_updateLocalCsnMinLock);
}

CommitSeqNo CsnMgr::GetBarrierMinCsn(PdbId pdbId)
{
    if (pdbId < FIRST_USER_PDB_ID) {
        return MAX_COMMITSEQNO;
    }
    CommitSeqNo barrierCsn = GetSendBarrierCsn(pdbId);
    return barrierCsn;
}

CommitSeqNo CsnMgr::GetRecycleCsnMin(PdbId pdbId)
{
    StorageReleasePanic(g_storageInstance->GetType() != StorageInstanceType::SINGLE, MODULE_FRAMEWORK, ErrMsg(
        "[PdbRecycleCsnMin] pdb %u single GetRecycleCsnMin in no single type(%hhu).",
        pdbId, static_cast<uint8_t>(g_storageInstance->GetType())));
    CommitSeqNo recycleMinCsn = GetLocalCsnMin();
    CommitSeqNo flashbackCsnMin = GetFlashbackCsnMin();
    if (DstoreCsnIsValid(flashbackCsnMin) && flashbackCsnMin < recycleMinCsn) {
        recycleMinCsn = flashbackCsnMin;
    }
    CommitSeqNo backupRestoreCsnMin = GetBackupRestoreCsnMin();
    if (unlikely(DstoreCsnIsValid(backupRestoreCsnMin) && backupRestoreCsnMin < recycleMinCsn)) {
        recycleMinCsn = backupRestoreCsnMin;
    }
    return recycleMinCsn;
}

RetStatus CsnMgr::UpdateMaxReservedCsn(CommitSeqNo newMaxReservedCsn)
{
    storage_trace_entry(TRACE_ID_CsnMgr__UpdateMaxReservedCsn);
    RetStatus status = DSTORE_SUCC;
    /* Check to avoid redundent update */
    if (likely(newMaxReservedCsn > GetMaxReservedCsn())) {
        DstoreLWLockAcquire(&m_maxReservedCsnLock, LW_EXCLUSIVE);
        /* Check again to make sure no one already updated it while I am getting the lock */
        if (likely(newMaxReservedCsn > GetMaxReservedCsn())) {
            ErrLog(DSTORE_LOG, MODULE_TRANSACTION,
                ErrMsg("Push maxReservedCsn to %lu in control file, old value in memory is %lu",
                    newMaxReservedCsn, GetMaxReservedCsn()));
            if (STORAGE_FUNC_FAIL(m_controlFile->SetMaxReservedCSN(newMaxReservedCsn, newMaxReservedCsn))) {
                ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
                    ErrMsg("Set max reserved CSN to control file fail. Error Code: %s", StorageGetErrorName()));
                LWLockRelease(&m_maxReservedCsnLock);
                goto ErrorExit;
            }
            /* Update max reserved csn recorded in memory */
            GsAtomicWriteU64(&m_maxReservedCsn, newMaxReservedCsn);
        }
        LWLockRelease(&m_maxReservedCsnLock);
    }

Finish:
    storage_trace_exit(TRACE_ID_CsnMgr__UpdateMaxReservedCsn);
    return status;

ErrorExit:
    status = DSTORE_FAIL;
    goto Finish;
}

RetStatus CsnMgr::UpdateMaxReservedCsnIfNecessary(CommitSeqNo curCsn)
{
    storage_trace_entry(TRACE_ID_CsnMgr__UpdateMaxReservedCsnIfNecessary);
    RetStatus status = DSTORE_SUCC;

    /*
     * Increment maxReservedCsn to the next multiple of csn assignment increment.
     * Any case we guarantee maxReservedCsn is larger than m_nextCsn at least one
     * m_csnAssignmentIncrement after maxReservedCsn is incremented, this can avoid
     * wait of work threads.
     */
    CommitSeqNo oldMaxReservedCsn = GetMaxReservedCsn();
    CommitSeqNo newMaxReservedCsn;
    if (oldMaxReservedCsn <= curCsn) {
        newMaxReservedCsn = (curCsn / m_csnAssignmentIncrement + 1) * m_csnAssignmentIncrement;
        newMaxReservedCsn += m_csnAssignmentIncrement;
    } else if (oldMaxReservedCsn - curCsn <= m_csnAssignmentIncrement) {
        newMaxReservedCsn = oldMaxReservedCsn + m_csnAssignmentIncrement;
    } else {
        goto Finish;
    }
    if (STORAGE_FUNC_FAIL(UpdateMaxReservedCsn(newMaxReservedCsn))) {
        goto ErrorExit;
    }

Finish:
    storage_trace_exit(TRACE_ID_CsnMgr__UpdateMaxReservedCsnIfNecessary);
    return status;

ErrorExit:
    status = DSTORE_FAIL;
    goto Finish;
}

char *CsnMgr::DumpInfo()
{
    StringInfoData dumpInfo;
    if (unlikely(!dumpInfo.init())) {
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
               ErrMsg("Malloc csn dump info failed!"));
        return nullptr;
    }

    dumpInfo.AppendString("type:single\n");
    CommitSeqNo csnAssignmentIncrement = m_csnAssignmentIncrement;
    CommitSeqNo localCsnMin = m_localCsnMin;
    CommitSeqNo nextCsn = GsAtomicReadU64(&m_nextCsn);
    DstoreLWLockAcquire(&m_maxReservedCsnLock, LW_SHARED);
    CommitSeqNo maxReservedCsn = GetMaxReservedCsn();
    LWLockRelease(&m_maxReservedCsnLock);

    dumpInfo.append("next csn                :%lu\n", nextCsn);
    dumpInfo.append("localCsnMin             :%lu\n", localCsnMin);
    dumpInfo.append("csnAssignmentIncrement  :%lu\n", csnAssignmentIncrement);
    dumpInfo.append("maxReservedCsn          :%lu\n", maxReservedCsn);
    return dumpInfo.data;
}

char *CsnMgrDiagnose::DumpCsnMgr()
{
    CsnMgr *csnMgr = g_storageInstance->GetCsnMgr();
    return csnMgr->DumpInfo();
}

}
