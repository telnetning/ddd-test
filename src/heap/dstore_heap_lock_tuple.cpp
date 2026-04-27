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
 * dstore_heap_lock_tup.cpp
 *
 *
 *
 * IDENTIFICATION
 *        storage/src/heap/dstore_heap_lock_tup.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "common/log/dstore_log.h"
#include "common/dstore_datatype.h"
#include "errorcode/dstore_heap_error_code.h"
#include "errorcode/dstore_transaction_error_code.h"
#include "errorcode/dstore_lock_error_code.h"
#include "transaction/dstore_transaction_mgr.h"
#include "wal/dstore_wal_write_context.h"
#include "heap/dstore_heap_undo_struct.h"
#include "heap/dstore_heap_struct.h"
#include "heap/dstore_heap_prune.h"
#include "heap/dstore_heap_wal_struct.h"
#include "heap/dstore_heap_scan.h"
#include "heap/dstore_heap_perf_unit.h"
#include "heap/dstore_heap_lock_tuple.h"
#include "page/dstore_heap_page.h"
#include "common/fault_injection/dstore_heap_fault_injection.h"
#include "fault_injection/fault_injection.h"

namespace DSTORE {

HeapLockTupHandler::HeapLockTupHandler(StorageInstance *instance, ThreadContext *thread, StorageRelation heapRel,
                                       bool isLobOperation)
    : HeapHandler(instance, thread, heapRel, isLobOperation)
{}

RetStatus HeapLockTupHandler::LockUnchangedTuple(HeapLockTupleContext *lockContext)
{
    if (m_isLob) {
        ErrLog(DSTORE_LOG, MODULE_HEAP, ErrMsg("try lock lob tuple({%hu, %u}, %hu).",
            lockContext->ctid.GetFileId(), lockContext->ctid.GetBlockNum(), lockContext->ctid.GetOffset()));
    }
    LatencyStat::Timer timer(&HeapPerfUnit::GetInstance().m_lockUnchangedTupleLatency);

    RetStatus result = DSTORE_FAIL;
    StorageReleasePanic(lockContext->retTup, MODULE_HEAP, ErrMsg("Tuple in lock tuple context should be null."));
    AutoMemCxtSwitch autoMemCxtSwitch(m_thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_STACK));

    StorageAssert(m_thrd->GetActiveTransaction() != nullptr);
    if (STORAGE_FUNC_FAIL(m_thrd->GetActiveTransaction()->AllocTransactionSlot())) {
        lockContext->failureInfo.reason = HeapHandlerFailureReason::ALLOC_TRANS_SLOT_FAILED;
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Failed to prepare undo when locking unchanged tuple({%hu, %u}, %hu).",
            lockContext->ctid.GetFileId(), lockContext->ctid.GetBlockNum(), lockContext->ctid.GetOffset()));
        return DSTORE_FAIL;
    }

LOCKTUPLESTART:
    if (STORAGE_FUNC_FAIL(InitBufferDesc(lockContext->ctid))) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Failed to init buffer desc when locking unchanged"
            " tuple({%hu, %u}, %hu).",
            lockContext->ctid.GetFileId(), lockContext->ctid.GetBlockNum(), lockContext->ctid.GetOffset()));
        lockContext->failureInfo.reason = HeapHandlerFailureReason::READ_BUFFER_FAILED;
        return DSTORE_FAIL;
    }

    do {
        /* Step 1: Heap begin lock tuple */
        if (STORAGE_FUNC_FAIL(BeginLock(lockContext))) {
            ErrLog(DSTORE_DEBUG1, MODULE_HEAP, ErrMsg("Begin lock failed, ctid({%hu, %u}, %hu)",
                lockContext->ctid.GetFileId(), lockContext->ctid.GetBlockNum(), lockContext->ctid.GetOffset()));
            break;
        }
        /* Step 2: lock tuple */
        TdId tdId = INVALID_TD_SLOT;
        if (STORAGE_FUNC_FAIL(DoLock(lockContext, tdId))) {
            lockContext->failureInfo.reason = HeapHandlerFailureReason::LOCK_TUP_FAILED;
            ErrLog(DSTORE_WARNING, MODULE_HEAP, ErrMsg("Lock failed, ctid({%hu, %u}, %hu)",
                lockContext->ctid.GetFileId(), lockContext->ctid.GetBlockNum(), lockContext->ctid.GetOffset()));
            break;
        }
        result = DSTORE_SUCC;
    } while (false);

    if (STORAGE_FUNC_FAIL(result) && CanRetry(lockContext->failureInfo.reason)) {
        ErrLog(DSTORE_WARNING, MODULE_HEAP, ErrMsg("Lock unchanged failed, retry alloctd, ctid({%hu, %u}, %hu)",
            lockContext->ctid.GetFileId(), lockContext->ctid.GetBlockNum(), lockContext->ctid.GetOffset()));
        goto LOCKTUPLESTART;
    }

    TryToUnlockReleaseBufferDesc();
#ifdef DSTORE_USE_ASSERT_CHECKING
    ErrLog(DSTORE_DEBUG1, MODULE_HEAP, ErrMsg("Lock unchanged tuple, result: %d, ctid({%hu, %u}, %hu)",
        static_cast<int>(result), lockContext->ctid.GetFileId(), lockContext->ctid.GetBlockNum(),
        lockContext->ctid.GetOffset()));
#endif
    return result;
}

/*
 * The caller must hold page lock
 */
RetStatus HeapLockTupHandler::CheckTupleChanged(ItemPointerData ctid, bool &isChanged, CommandId &tupleCid,
                                                Snapshot snapshot, bool executedEpq)
{
    LatencyStat::Timer timer(&HeapPerfUnit::GetInstance().m_checkTupleChangedLatency);

    StorageAssert(ctid != INVALID_ITEM_POINTER);
    Transaction *txn = m_thrd->GetActiveTransaction();
    StorageAssert(txn != nullptr);
    HeapPage *page = static_cast<HeapPage *>(m_bufferDesc->GetPage());
    OffsetNumber offset = ctid.GetOffset();
    isChanged = false;

    /*
     * Step 1: Wait for the transaction to end, if necessary.
     *     case 1: the tuple has been locked by others.
     *     case 2: the tuple is being modified by others.
     */
    if (STORAGE_FUNC_FAIL(WaitTupleIfNeed(ctid))) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("CheckTupleChanged failed, ctid({%hu, %u}, %hu)",
            ctid.GetFileId(), ctid.GetBlockNum(), ctid.GetOffset()));
        return DSTORE_FAIL;
    }

    /* Check if the transaction status is pending_commit or In_progress, if so, panic using storage_relasse_panic. */
    uint32 tdStatus = 0;
    ItemId *itemId = page->GetItemIdPtr(offset);
    if (unlikely(itemId->IsNoStorage())) {
        tdStatus = itemId->redirect.m_tdStatus;
    } else {
        HeapDiskTuple *diskTup = static_cast<HeapDiskTuple *>(page->GetRowData(itemId));
        tdStatus = diskTup->m_info.val.m_tdStatus;
    }

    /* if tuple is lob, we do not check tuple changed, since we have locked the main tuple */
    if (unlikely(m_isLob))
        return DSTORE_SUCC;

    /*
     * Step 2: if the tuple has been locked by us, it must be unchanged.
     *         if the tuple is locked by others, the lock must be expired, because we have waited txn in Step 1.
     */
    TdId tdId = page->GetTupleTdId(offset);
    TdId lockerTdId = page->GetTupleLockerTdId(offset);
    if (lockerTdId != INVALID_TD_SLOT) {
        Xid lockerXid = page->GetTd(lockerTdId)->GetLockerXid();
        if (txn->IsCurrent(lockerXid)) {
            /*
            * case 1: if Epq executed, we do not need to check tuple changed.
            *         if tuple xid = lockerxid, the tuple must be unchanged.
            */
            if (executedEpq || page->GetDiskTuple(offset)->GetXid() == lockerXid) {
                return DSTORE_SUCC;
            }

            /* case 2: if the tuple detached from td, it must be unchanged. */
            if (page->TestTupleTdStatus(static_cast<TupleTdStatus>(tdStatus), DETACH_TD)) {
                return DSTORE_SUCC;
            }

            if (GetTableSmgr()->IsGlobalTempTable()) {
                return DSTORE_SUCC;
            }

            /*
            * case 3: reference step 5.
            */
            TD *td = page->GetTd(tdId);
            XidStatus xstatus(td->GetXid(), txn, td);
            if (page->TestTupleTdStatus(static_cast<TupleTdStatus>(tdStatus), ATTACH_TD_AS_HISTORY_OWNER) ||
                xstatus.IsCommitted()) {
                bool isDirty;
                bool tupCommitBefore =
                    page->JudgeTupCommitBeforeSpecCsn<true>(m_heapRel->m_pdbId, offset, snapshot->GetCsn(), isDirty);
                if (isDirty) {
                    (void)m_bufMgr->MarkDirty(m_bufferDesc, false);
                }
                if (likely(tupCommitBefore)) {
                    return DSTORE_SUCC;
                }
            }

            /* case 4：the tuple has been locked by us, so it must be commited or frozen */
#ifdef DSTORE_USE_ASSERT_CHECKING
            StorageAssert(xstatus.IsCommitted() || xstatus.IsFrozen());
#endif
            if (xstatus.IsFrozen()) {
                return DSTORE_SUCC;
            }
            isChanged = true;
            return DSTORE_SUCC;
        }

        /* The tuple lock has expired, so we rollback it here. */
        HeapDiskTuple *diskTup = page->GetDiskTuple(offset);
        diskTup->SetLockerTdId(INVALID_TD_SLOT);
        (void)m_bufMgr->MarkDirty(m_bufferDesc, false);
    }

    /* Step 3: if the tuple detached from td, it must be unchanged. */
    if (page->TestTupleTdStatus(static_cast<TupleTdStatus>(tdStatus), DETACH_TD)) {
        return DSTORE_SUCC;
    }

    /* Step 4: the tuple xid is current, compare cid if necessary. */
    TD *td = page->GetTd(tdId);
    if (page->TestTupleTdStatus(static_cast<TupleTdStatus>(tdStatus), ATTACH_TD_AS_NEW_OWNER) &&
        txn->IsCurrent(td->GetXid())) {
        return CheckTupleChangedByCid(ctid, isChanged, tupleCid, snapshot);
    }

    if (unlikely(GetTableSmgr()->IsGlobalTempTable())) {
        return DSTORE_SUCC;
    }

    /*
     * Step 5:
     * if the td has been reused, the tuple xid must be committed.
     * if the td has not been reused, the td xid is the tuple xid.
     * if the tuple xid is committed before this, the tuple must be unchanged.
     */
    XidStatus xstatus(td->GetXid(), txn, td);
    if (page->TestTupleTdStatus(offset, ATTACH_TD_AS_HISTORY_OWNER) || xstatus.IsCommitted()) {
        bool isDirty;
        bool tupCommitBefore =
            page->JudgeTupCommitBeforeSpecCsn<true>(m_heapRel->m_pdbId, offset, snapshot->GetCsn(), isDirty);
        if (isDirty) {
            (void)m_bufMgr->MarkDirty(m_bufferDesc, false);
        }
        if (likely(tupCommitBefore)) {
            return DSTORE_SUCC;
        }
    }

    /* Step 6: if the xid in td is recycled, the tuple must be unchanged. */
    if (xstatus.IsFrozen()) {
        return DSTORE_SUCC;
    }

    isChanged = true;
    return DSTORE_SUCC;
}

RetStatus HeapLockTupHandler::CheckTupleChangedByCid(ItemPointerData ctid, bool &isChanged, CommandId &tupleCid,
                                                     Snapshot snapshot)
{
    TransactionMgr *txnMgr = m_instance->GetPdb(m_heapRel->m_pdbId)->GetTransactionMgr();

    if (STORAGE_VAR_NULL(m_bufferDesc)) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("CheckTupleChangedByCid failed, m_bufferDesc is nullptr"));
        return DSTORE_FAIL;
    }
    HeapPage *page = static_cast<HeapPage *>(m_bufferDesc->GetPage());
    TdId tdId = page->GetTupleTdId(ctid.GetOffset());
    TD *td = page->GetTd(tdId);
    Transaction *txn = m_thrd->GetActiveTransaction();

    if (CidVisibleToSnapshot(txn, snapshot, td->GetCommandId())) {
        /* tupleCid <= tdCid < snapshotCid */
        isChanged = false;
        return DSTORE_SUCC;
    }

    /* The command id in undo is the tupleCid. tdCid >= tupleCid */
    UndoRecord record;
    Xid xid = td->GetXid();
    UndoRecPtr undoRecPtr = td->GetUndoRecPtr();
    if (STORAGE_FUNC_FAIL(txnMgr->FetchUndoRecordByMatchedCtidInCurXact(xid, &record, undoRecPtr, ctid))) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP,
               ErrMsg("Fetch undo record by matched ctid failed, ctid({%hu, %u}, %hu), "
                      "xid(%d, %lu)",
                      ctid.GetFileId(), ctid.GetBlockNum(), ctid.GetOffset(), static_cast<int32>(xid.m_zoneId),
                      xid.m_logicSlotId));
        return DSTORE_FAIL;
    }
    StorageReleasePanic((record.GetUndoType() == UNDO_UNKNOWN), MODULE_HEAP,
        ErrMsg("Invalid undo record! undo type = %hhu.", static_cast<uint8>(record.GetUndoType())));

    tupleCid = record.GetCid();
    isChanged = (!CidVisibleToSnapshot(txn, snapshot, tupleCid));

    return DSTORE_SUCC;
}

RetStatus HeapLockTupHandler::WaitTupleIfNeed(ItemPointerData ctid)
{
    RetStatus status = DSTORE_SUCC;
    Transaction *txn = m_thrd->GetActiveTransaction();
    StorageAssert(txn != nullptr);
    HeapPage *page = nullptr;
    OffsetNumber offset = ctid.GetOffset();
    bool needWait = false;
    do {
        needWait = false;

        if (unlikely(offset == INVALID_ITEM_OFFSET_NUMBER || offset > MAX_ITEM_OFFSET_NUMBER)) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Offset(%hu) is invalid.", offset));
            TryToUnlockReleaseBufferDesc();
            return DSTORE_FAIL;
        }
        if (unlikely(m_bufferDesc == INVALID_BUFFER_DESC)) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("m_bufferDesc is invalid."));
            return DSTORE_FAIL;
        }

        page = static_cast<HeapPage *>(m_bufferDesc->GetPage());

        /*
         * Step 1: if the tuple is locked, wait locker xid end.
         *     Warning: The page lock will be released when waiting xid, reacquired after waiting xid.
         *     If other thread acquire the page lock and lock the tuple, we should continue to wait.
         */
        TdId lockerTdId = page->GetTupleLockerTdId(offset);
        if (unlikely(lockerTdId != INVALID_TD_SLOT && page->GetTd(lockerTdId)->GetLockerXid() != INVALID_XID)) {
            TD *lockerTd = page->GetTd(lockerTdId);
            Xid lockerXid = lockerTd->GetLockerXid();
            XidStatus lockerXs(lockerXid, txn);
            if (lockerXs.IsCurrentTxn()) {
                /* tuple is locked by me */
                break;
            }
            needWait = (lockerXs.IsInProgress() || lockerXs.IsPendingCommit());
            if (needWait && STORAGE_FUNC_FAIL(WaitTxn(lockerXid, LW_EXCLUSIVE))) {
                ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("WaitTupleIfNeed failed, locker xid(%d, %lu)",
                    static_cast<int32>(lockerXid.m_zoneId), lockerXid.m_logicSlotId));
                return DSTORE_FAIL;
            }
            if (needWait) {
                /* wait txn succ */
                continue;
            }
        }

        /*
         * Step 2: if the tuple is being modified, wait the transaction end.
         *     Consider the following concurrency scenarios between releasing and reacquiring page lock:
         *     case 1: waited xid is committed, other thread lock the page and reuse the td, the tuple td
         *             status will be ATTACH_TD_AS_HISTORY_OWNER.
         *     case 2: waited xid is aborted, the tuple td status will be ATTACH_TD_AS_HISTORY_OWNER.
         *     case 3: waited xid is committed or aborted, other thread lock the page and update or delete
         *             the tuple, the tuple td status will also be ATTACH_TD_AS_NEW_OWNER, continue to wait.
         *     case 4: waited xid fault, need to do page rollback.
         */
        TdId tdId = page->GetTupleTdId(offset);
        if (tdId != INVALID_TD_SLOT && page->TestTupleTdStatus(offset, ATTACH_TD_AS_NEW_OWNER)) {
            TD *td = page->GetTd(tdId);
            Xid xid = td->GetXid();
            StorageAssert(xid != INVALID_XID);
            XidStatus xs(xid, txn, td);
            if (xs.IsCurrentTxn()) {
                /* tuple is modified by me */
                break;
            }
            needWait = (xs.IsInProgress() || xs.IsPendingCommit());
            if (needWait && STORAGE_FUNC_FAIL(WaitTxn(xid, LW_EXCLUSIVE))) {
                ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("WaitTupleIfNeed failed, xid(%d, %lu)",
                    static_cast<int32>(xid.m_zoneId), xid.m_logicSlotId));
                return DSTORE_FAIL;
            }
            if (needWait) {
                /* wait txn succ */
                continue;
            }
        }
    } while (needWait);

    /* Here, we need rollback bigtuple if it is not deleted. It can not be deleted after page rollback */
    if (unlikely(page->GetItemIdPtr(offset)->IsNormal() && page->GetDiskTuple(offset)->IsLinked()) &&
        STORAGE_FUNC_SUCC(status) && likely(!(page->IsDiskTupleDeleted(offset)))) {
        if (STORAGE_FUNC_FAIL(RollbackBigTuple(ctid))) {
            storage_set_error(HEAP_ERROR_ROLLBACK_BIG_TUPLE_FAILED);
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Rollback BigTuple failed when delete, ctid({%hu, %u}, %hu)",
                ctid.GetFileId(), ctid.GetBlockNum(), ctid.GetOffset()));
            status = DSTORE_FAIL;
        }
    }

    return status;
}

RetStatus HeapLockTupHandler::WaitTxn(Xid xid, LWLockMode lockMode)
{
    StoragePdb *pdb = m_instance->GetPdb(m_heapRel->m_pdbId);
    m_bufMgr->UnlockContent(m_bufferDesc, BufferPoolUnlockContentFlag::DontCheckCrc());

    FAULT_INJECTION_NOTIFY(DstoreHeapFI::READY_TO_WAIT_TRX_END);

    StorageAssert(pdb);
    if (!pdb->IsInit()) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("WaitTxn get pdb failed, xid(%d, %lu), PdbId(%u)",
            static_cast<int32>(xid.m_zoneId), xid.m_logicSlotId, m_heapRel->m_pdbId));
        m_bufMgr->Release(m_bufferDesc);
        m_bufferDesc = nullptr;
        return DSTORE_FAIL;
    }
    bool txnFailed = false;
    if (STORAGE_FUNC_FAIL(m_instance->GetPdb(m_heapRel->m_pdbId)->GetTransactionMgr()->
        WaitForTransactionEnd(xid, txnFailed))) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("WaitTxn wait transaction end failed, xid(%d, %lu).",
            static_cast<int32>(xid.m_zoneId), xid.m_logicSlotId));
        m_bufMgr->Release(m_bufferDesc);
        m_bufferDesc = nullptr;
        return DSTORE_FAIL;
    }

    if (txnFailed) {
        if (STORAGE_FUNC_FAIL(m_bufMgr->LockContent(m_bufferDesc, LW_EXCLUSIVE))) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("WaitTxn lock buffer content failed after transaction failed."));
            m_bufMgr->Release(m_bufferDesc);
            m_bufferDesc = nullptr;
            return DSTORE_FAIL;
        }
        /*
         * Consider meeting a failed transaction which should be rolled back in recovery.
         * Failed transaction do not hold xid lock, so should judge transaction status specially.
         * In order to reduce wait time, we do page rollback here.
         */
        HeapPage *page = static_cast<HeapPage *>(m_bufferDesc->GetPage());
        UndoRecordVector undoRecVec;
        if (STORAGE_FUNC_FAIL(undoRecVec.Init())) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP,
                   ErrMsg("UndoRecVec init failed when wait txn, xid(%d, %lu).", static_cast<int32>(xid.m_zoneId),
                          xid.m_logicSlotId));
            m_bufMgr->UnlockAndRelease(m_bufferDesc);
            m_bufferDesc = nullptr;
            return DSTORE_FAIL;
        }
        RetStatus ret = page->RollbackByXid(m_heapRel->m_pdbId, xid, m_bufMgr, m_bufferDesc);
        if (STORAGE_FUNC_FAIL(ret)) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP,
                   ErrMsg("WaitTxn rollback by xid failed, xid(%d, %lu).", static_cast<int32>(xid.m_zoneId),
                          xid.m_logicSlotId));
            m_bufMgr->UnlockAndRelease(m_bufferDesc);
            m_bufferDesc = nullptr;
            return DSTORE_FAIL;
        }
    } else {
        if (STORAGE_FUNC_FAIL(m_bufMgr->LockContent(m_bufferDesc, lockMode))) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("WaitTxn lock buffer content failed after transaction end."));
            m_bufMgr->Release(m_bufferDesc);
            m_bufferDesc = nullptr;
            return DSTORE_FAIL;
        }
    }

    return DSTORE_SUCC;
}

RetStatus HeapLockTupHandler::InitBufferDesc(ItemPointerData ctid)
{
    StorageAssert(ctid != INVALID_ITEM_POINTER);
    StorageAssert(ctid.GetOffset() != 0);

    PageId pageId = ctid.GetPageId();
    m_bufferDesc = m_bufMgr->Read(m_heapRel->m_pdbId, pageId, LW_EXCLUSIVE);
    if (unlikely(m_bufferDesc == INVALID_BUFFER_DESC)) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP,
            ErrMsg("Read page(%hu, %u) failed when init buffer desc for locking.", pageId.m_fileId, pageId.m_blockId));
        return DSTORE_FAIL;
    }
    CheckBufferedPage(m_bufferDesc->GetPage(), pageId);
    return DSTORE_SUCC;
}

bool HeapLockTupHandler::CanRetry(HeapHandlerFailureReason &failReason)
{
    if (!m_tdcontext.NeedRetryAllocTd()) {
        return false;
    }

    bool canRetry = false;
    do {
        HeapPruneHandler prune(m_instance, m_heapRel, m_thrd, m_bufferDesc, m_isLob);
        prune.TryPrunePage();

        HeapPage *page = static_cast<HeapPage *>(m_bufferDesc->GetPage());
        /* If the number of TDs reaches the upper limit, we can't extend and need to wait for others end. */
        if (page->GetFreeSpace<FreeSpaceCondition::RAW>() >= sizeof(TD) && page->GetTdCount() < MAX_TD_COUNT) {
            TryToUnlockReleaseBufferDesc();
            canRetry = true;
            break;
        }

        TryToUnlockReleaseBufferDesc();
        TransactionMgr *transactionMgr = m_instance->GetPdb(m_heapRel->m_pdbId)->GetTransactionMgr();
        if (STORAGE_FUNC_SUCC(transactionMgr->WaitForOneTransactionEnd(m_tdcontext.waitXids.xids,
            m_tdcontext.waitXids.xidNum))) {
            canRetry = true;
            break;
        }
        if (StorageGetErrorCode() == LOCK_ERROR_WAIT_TIMEOUT) {
            failReason = HeapHandlerFailureReason::LOCK_WAIT_TIMEOUT;
        } else if (StorageGetErrorCode() == LOCK_ERROR_WAIT_CANCELED) {
            failReason = HeapHandlerFailureReason::LOCK_WAIT_CANCELED;
        } else {
            failReason = HeapHandlerFailureReason::ALLOC_TD_FAILED;
        }
    } while (0);

    if (canRetry) {
        m_tdcontext.Init(m_heapRel->m_pdbId, NeedWal());
        return true;
    }

    return false;
}

RetStatus HeapLockTupHandler::SetFailureInfo(FailureInfo &failureInfo, ItemPointerData ctid, CommandId tupleCid,
                                             Snapshot snapshot)
{
    HeapPage *page = static_cast<HeapPage *>(m_bufferDesc->GetPage());
    OffsetNumber offset = ctid.GetOffset();
    Xid tdXid = page->GetTd(page->GetTupleTdId(offset))->GetXid();

    Transaction *transaction = m_thrd->GetActiveTransaction();
    StorageAssert(transaction != nullptr);
    HeapHandlerFailureReason reason;
    StorageAssert(!page->TestTupleTdStatus(offset, DETACH_TD) || page->GetTupleLockerTdId(offset) != INVALID_TD_SLOT);
    if (page->TestTupleTdStatus(offset, ATTACH_TD_AS_NEW_OWNER) && transaction->IsCurrent(tdXid)) {
        if (transaction->IsCurCidUsed() && tupleCid >= snapshot->GetCid()) {
            /* SELF_CREATED means tuple was created in the same command, SELF_MODIFIED means tuple was modified in the
             * same command. */
            HeapDiskTupLiveMode mode = page->GetDiskTuple(offset)->GetLiveMode();
            reason = (mode == HeapDiskTupLiveMode::TUPLE_BY_NORMAL_INSERT) ? HeapHandlerFailureReason::SELF_CREATED
                                                                           : HeapHandlerFailureReason::SELF_MODIFIED;
        } else {
            /* In a procedure that contains cursors, the cid of the affected tuple may be larger than the cid of the
             * snapshot. */
            reason = HeapHandlerFailureReason::INVISIBLE_TO_SNAPSHOT;
        }
        failureInfo.SetCid(tupleCid);
        ErrLog(DSTORE_DEBUG1, MODULE_HEAP,
               ErrMsg("Tuple is changed due to same command. reason(%hhu), tuple cid(%u), snapshot cid(%u)",
                      static_cast<uint8>(reason), failureInfo.cid, transaction->GetSnapshotCid()));
    } else {
        ItemPointerData newCtid = INVALID_ITEM_POINTER;
        if (STORAGE_FUNC_FAIL(GetNewestVersionCtid<false>(ctid, newCtid))) {
            if (StorageGetErrorCode() == TRANSACTION_INFO_SAME_THREAD_DEADLOCK ||
                StorageGetErrorCode() == LOCK_ERROR_DEADLOCK) {
                failureInfo.SetReason(HeapHandlerFailureReason::DEADLOCK);
            } else if (StorageGetErrorCode() == LOCK_ERROR_WAIT_TIMEOUT) {
                failureInfo.SetReason(HeapHandlerFailureReason::LOCK_WAIT_TIMEOUT);
            } else if (StorageGetErrorCode() == LOCK_ERROR_WAIT_CANCELED) {
                failureInfo.SetReason(HeapHandlerFailureReason::LOCK_WAIT_CANCELED);
            } else {
                failureInfo.SetReason(HeapHandlerFailureReason::GET_NEWEST_CTID_FAILED);
            }
            ErrLog(DSTORE_DEBUG1, MODULE_HEAP, ErrMsg("SetFailureInfo fail when get newest ctid({%hu, %u}, %hu)",
                ctid.GetFileId(), ctid.GetBlockNum(), ctid.GetOffset()));
            return DSTORE_FAIL;
        }
        if (newCtid == INVALID_ITEM_POINTER) {
            reason = HeapHandlerFailureReason::DELETED;
            failureInfo.SetCtid(ctid);
        } else {
            reason = HeapHandlerFailureReason::UPDATED;
            failureInfo.SetCtid(newCtid);
        }
        ErrLog(DSTORE_DEBUG1, MODULE_HEAP, ErrMsg("Tuple is changed due to other transaction. reason(%hhu, "
            "old ctid({%hu, %u}, %hu), new ctid({%hu, %u}, %hu).", static_cast<uint8>(reason),
            ctid.GetFileId(), ctid.GetBlockNum(), offset,
            newCtid.GetFileId(), newCtid.GetBlockNum(), newCtid.GetOffset()));
    }
    failureInfo.SetReason(reason);

    return DSTORE_SUCC;
}

RetStatus HeapLockTupHandler::RollbackBigTuple(ItemPointerData ctid)
{
    StorageReleasePanic(ctid == INVALID_ITEM_POINTER, MODULE_HEAP, ErrMsg("invalid ctid when rollback big tuple"))
    if (unlikely(m_bufferDesc == INVALID_BUFFER_DESC)) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("m_bufferDesc is invalid."));
        return DSTORE_FAIL;
    }
    ItemPointerData nextCtid =
        (static_cast<HeapPage *>(m_bufferDesc->GetPage()))->GetDiskTuple(ctid.GetOffset())->GetNextChunkCtid();
    BufferDesc *bufferDesc = nullptr;
    while (nextCtid != INVALID_ITEM_POINTER) {
        UNUSE_PARAM AutoMemCxtSwitch autoMemCxtSwitch(m_thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_STACK));
        ctid = nextCtid;
        bufferDesc = m_bufMgr->Read(m_heapRel->m_pdbId, ctid.GetPageId(), LW_EXCLUSIVE);
        if (unlikely(bufferDesc == INVALID_BUFFER_DESC)) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP,
                ErrMsg("Read page failed when Rollback BigTuple, chunkId({%hu, %u}, %hu).",
                ctid.GetFileId(), ctid.GetBlockNum(), ctid.GetOffset()));
            return DSTORE_FAIL;
        }
        CheckBufferedPage(bufferDesc->GetPage(), ctid.GetPageId());
        StorageReleasePanic(bufferDesc->GetPageId() != ctid.GetPageId(), MODULE_HEAP,
            ErrMsg("invalid pageid when read buffer"));
        HeapPage *page = static_cast<HeapPage *>(bufferDesc->GetPage());
        OffsetNumber offset = ctid.GetOffset();
        TdId tdId = page->GetTupleTdId(offset);
        Transaction *txn = m_thrd->GetActiveTransaction();
        if (tdId != INVALID_TD_SLOT && page->TestTupleTdStatus(offset, ATTACH_TD_AS_NEW_OWNER)) {
            TD *td = page->GetTd(tdId);
            Xid xid = td->GetXid();
            StorageReleasePanic(xid == INVALID_XID, MODULE_HEAP, ErrMsg("invalid xid"));
            XidStatus xs(xid, txn, td);
            if (!xs.IsCurrentTxn() && (xs.IsInProgress() || xs.IsPendingCommit())) {
                UndoRecordVector undoRecVec;
                if (STORAGE_FUNC_FAIL(undoRecVec.Init())) {
                    ErrLog(DSTORE_ERROR, MODULE_HEAP,
                        ErrMsg("UndoRecVec init failed when wait txn, xid(%d, %lu).", static_cast<int32>(xid.m_zoneId),
                                xid.m_logicSlotId));
                    m_bufMgr->UnlockAndRelease(bufferDesc);
                    bufferDesc = nullptr;
                    return DSTORE_FAIL;
                }
                RetStatus ret = page->RollbackByXid(m_heapRel->m_pdbId, xid, m_bufMgr, bufferDesc);
                if (STORAGE_FUNC_FAIL(ret)) {
                    ErrLog(DSTORE_ERROR, MODULE_HEAP,
                        ErrMsg("rollback big tuple by xid failed, xid(%d, %lu).", static_cast<int32>(xid.m_zoneId),
                                xid.m_logicSlotId));
                    m_bufMgr->UnlockAndRelease(bufferDesc);
                    bufferDesc = nullptr;
                    return DSTORE_FAIL;
                }
            }
        }
        nextCtid = (static_cast<HeapPage *>(bufferDesc->GetPage()))->GetDiskTuple(ctid.GetOffset())->GetNextChunkCtid();
        m_bufMgr->UnlockAndRelease(bufferDesc);
        bufferDesc = nullptr;
    }
    return DSTORE_SUCC;
}

RetStatus HeapLockTupHandler::BeginLock(HeapLockTupleContext *lockContext)
{
    CommandId tupleCid = INVALID_CID;
    bool tupleChanged = false;

    if (STORAGE_FUNC_FAIL(CheckTupleChanged(lockContext->ctid, tupleChanged, tupleCid, &lockContext->snapshot,
        lockContext->executedEpq))) {
        if (StorageGetErrorCode() == TRANSACTION_INFO_SAME_THREAD_DEADLOCK ||
            StorageGetErrorCode() == LOCK_ERROR_DEADLOCK) {
            lockContext->failureInfo.reason = HeapHandlerFailureReason::DEADLOCK;
        } else if (StorageGetErrorCode() == LOCK_ERROR_WAIT_TIMEOUT) {
            lockContext->failureInfo.reason = HeapHandlerFailureReason::LOCK_WAIT_TIMEOUT;
        } else if (StorageGetErrorCode() == LOCK_ERROR_WAIT_CANCELED) {
            lockContext->failureInfo.reason = HeapHandlerFailureReason::LOCK_WAIT_CANCELED;
        } else if (StorageGetErrorCode() == HEAP_ERROR_ROLLBACK_BIG_TUPLE_FAILED) {
            lockContext->failureInfo.reason = HeapHandlerFailureReason::ROLLBACK_BIG_TUPLE_FAILED;
        } else {
            lockContext->failureInfo.reason = HeapHandlerFailureReason::CHECK_TUPLE_CHANGED_FAILED;
        }
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Check tuple changed fail when lock, ctid({%hu, %u}, %hu)",
            lockContext->ctid.GetFileId(), lockContext->ctid.GetBlockNum(), lockContext->ctid.GetOffset()));
        return DSTORE_FAIL;
    }

    if (tupleChanged) {
        if (STORAGE_FUNC_FAIL(
            SetFailureInfo(lockContext->failureInfo, lockContext->ctid, tupleCid, &lockContext->snapshot))) {
            return DSTORE_FAIL;
        }
        if (lockContext->allowLockSelf && (lockContext->failureInfo.reason == HeapHandlerFailureReason::SELF_CREATED ||
            lockContext->failureInfo.reason == HeapHandlerFailureReason::SELF_MODIFIED)) {
            ErrLog(DSTORE_DEBUG1, MODULE_HEAP, ErrMsg("Allow lock self changed tuple, ctid({%hu, %u}, %hu)",
                lockContext->ctid.GetFileId(), lockContext->ctid.GetBlockNum(), lockContext->ctid.GetOffset()));
            return DSTORE_SUCC;
        } else {
            ErrLog(DSTORE_DEBUG1, MODULE_HEAP, ErrMsg("Tuple is changed when begin lock, ctid({%hu, %u}, %hu)",
                lockContext->ctid.GetFileId(), lockContext->ctid.GetBlockNum(), lockContext->ctid.GetOffset()));
            storage_set_error(HEAP_ERROR_TUPLE_IS_CHANGED);
            return DSTORE_FAIL;
        }
    }

    HeapPage *page = static_cast<HeapPage *>(m_bufferDesc->GetPage());
    if (unlikely(page->IsDiskTupleDeleted(lockContext->ctid.GetOffset()))) {
        lockContext->failureInfo.reason = HeapHandlerFailureReason::DELETED;
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Tuple is deleted when begin lock, ctid({%hu, %u}, %hu)",
            lockContext->ctid.GetFileId(), lockContext->ctid.GetBlockNum(), lockContext->ctid.GetOffset()));
        storage_set_error(HEAP_ERROR_TUPLE_IS_DELETED);
        return DSTORE_FAIL;
    }

    return DSTORE_SUCC;
}

/* The caller must hold page lock. */
RetStatus HeapLockTupHandler::GetNewCtidAnotherPage(
    const ItemPointerData &curCtid, LWLockMode lockMode, ItemPointerData &newCtid)
{
    /* Step 1: fetch matched undo record, because the new ctid id kept in undo data. */
    HeapPage *page = static_cast<HeapPage *>(m_bufferDesc->GetPage());
    UNUSE_PARAM TD *td = page->GetTd(page->GetTupleTdId(curCtid.GetOffset()));
    Xid xid = td->GetXid();
    TransactionMgr *txnMgr = m_instance->GetPdb(m_heapRel->m_pdbId)->GetTransactionMgr();
    UndoRecord undoRec;
    RetStatus ret = txnMgr->FetchUndoRecordByMatchedCtid(xid, &undoRec, td->GetUndoRecPtr(), curCtid);
    StorageReleasePanic((STORAGE_FUNC_FAIL(ret) ||
        (undoRec.GetUndoType() != UNDO_HEAP_ANOTHER_PAGE_APPEND_UPDATE_OLD_PAGE &&
        undoRec.GetUndoType() != UNDO_HEAP_ANOTHER_PAGE_APPEND_UPDATE_OLD_PAGE_TMP)), MODULE_HEAP,
        ErrMsg("Failed to fetch undo record by matched ctid({%hu, %u}, %hu)! undo type = %d",
        curCtid.GetFileId(), curCtid.GetBlockNum(), curCtid.GetOffset(), static_cast<int>(undoRec.GetUndoType())));

    /* Step 2: Get the new ctid and switch to the matched new bufferDesc */
    newCtid = static_cast<UndoDataHeapAnotherPageAppendUpdate *>(undoRec.GetUndoData())->GetNewCtid();
    m_bufMgr->UnlockAndRelease(m_bufferDesc);
    m_bufferDesc = m_bufMgr->Read(m_heapRel->m_pdbId, newCtid.GetPageId(), lockMode);
    if (unlikely(m_bufferDesc == INVALID_BUFFER_DESC)) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP,
               ErrMsg("Read page(%hu, %u) failed when get new ctid another page.",
                      newCtid.GetPageId().m_fileId, newCtid.GetPageId().m_blockId));
        return DSTORE_FAIL;
    }
    CheckBufferedPage(m_bufferDesc->GetPage(), newCtid.GetPageId());
    return DSTORE_SUCC;
}

/* The caller must hold page lock. Get the newest ctid from update ctid chain. */
template<bool needExeclusiveLock>
RetStatus HeapLockTupHandler::GetNewestVersionCtid(const ItemPointerData &curCtid, ItemPointerData &newestCtid)
{
    LWLockMode lockMode = needExeclusiveLock ? LW_EXCLUSIVE : LW_SHARED;

    ItemPointerData tmpCtid = curCtid;
    for (;;) {
        /*
         * Don't worry about the waited transaction being aborted, because we can't wait for a tuple whose live mode is
         * NEW_TUPLE_BY_ANOTHER_PAGE_UPDATE.
         */
        if (STORAGE_FUNC_FAIL(WaitTupleIfNeed(tmpCtid))) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Get newest ctid failed when wait tuple, curCtid({%hu, %u}, %hu)",
                curCtid.GetFileId(), curCtid.GetBlockNum(), curCtid.GetOffset()));
            return DSTORE_FAIL;
        }

        if (STORAGE_FUNC_FAIL(GetNewCtidByTupleLiveMode(tmpCtid, lockMode))) {
            ErrLog(DSTORE_DEBUG1, MODULE_HEAP, ErrMsg("Get newest ctid failed when get tuple by live mode, "
                "curCtid({%hu, %u}, %hu)", curCtid.GetFileId(), curCtid.GetBlockNum(), curCtid.GetOffset()));
            break;
        }
    }

    newestCtid = tmpCtid;
    return DSTORE_SUCC;
}

/* The caller must hold page lock. */
RetStatus HeapLockTupHandler::GetNewCtidByTupleLiveMode(ItemPointerData &newCtid, LWLockMode lockMode)
{
    RetStatus result = DSTORE_SUCC;
    ItemPointerData curCtid = newCtid;
    StorageAssert(m_bufferDesc != nullptr);
    HeapPage *page = static_cast<HeapPage *>(m_bufferDesc->GetPage());
    ItemId *itemId = page->GetItemIdPtr(curCtid.GetOffset());
    /* if done page rollback or transaction rollback, itemid maybe was set unused. */
    if (unlikely(itemId->IsUnused())) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Get new ctid failed, item is unused, curCtid({%hu, %u}, %hu)",
            curCtid.GetFileId(), curCtid.GetBlockNum(), curCtid.GetOffset()));
        newCtid = INVALID_ITEM_POINTER;
        return DSTORE_FAIL;
    }
    HeapDiskTupLiveMode mode = (itemId->IsNoStorage()) ? static_cast<HeapDiskTupLiveMode>(itemId->GetTupLiveMode())
                                                       : page->GetDiskTuple(curCtid.GetOffset())->GetLiveMode();
    switch (mode) {
        case HeapDiskTupLiveMode::TUPLE_BY_NORMAL_DELETE: {
            ErrLog(DSTORE_DEBUG1, MODULE_HEAP, ErrMsg("Invalid heap disk tuple live mode(%d), curCtid({%hu, %u}, %hu)",
                static_cast<int>(mode), curCtid.GetFileId(), curCtid.GetBlockNum(), curCtid.GetOffset()));
            newCtid = INVALID_ITEM_POINTER;
            result = DSTORE_FAIL;
            break;
        }
        case HeapDiskTupLiveMode::NEW_TUPLE_BY_ANOTHER_PAGE_UPDATE:
        case HeapDiskTupLiveMode::TUPLE_BY_NORMAL_INSERT:
        case HeapDiskTupLiveMode::NEW_TUPLE_BY_SAME_PAGE_UPDATE:
        case HeapDiskTupLiveMode::NEW_TUPLE_BY_INPLACE_UPDATE: {
            ErrLog(DSTORE_DEBUG1, MODULE_HEAP, ErrMsg("Invalid heap disk tuple live mode(%d), curCtid({%hu, %u}, %hu)",
                static_cast<int>(mode), curCtid.GetFileId(), curCtid.GetBlockNum(), curCtid.GetOffset()));
            result = DSTORE_FAIL;
            break;
        }
        case HeapDiskTupLiveMode::OLD_TUPLE_BY_ANOTHER_PAGE_UPDATE: {
            /* Warning: page changed. */
            result = GetNewCtidAnotherPage(curCtid, lockMode, newCtid);
            break;
        }
        case HeapDiskTupLiveMode::OLD_TUPLE_BY_SAME_PAGE_UPDATE: {
            ErrLog(DSTORE_PANIC, MODULE_HEAP, ErrMsg("Invalid heap disk tuple live mode(%d), curCtid({%hu, %u}, %hu).",
                static_cast<int>(mode), curCtid.GetFileId(), curCtid.GetBlockNum(), curCtid.GetOffset()));
            break;
        }
        default: {
            ErrLog(DSTORE_PANIC, MODULE_HEAP, ErrMsg("Unknown heap disk tuple live mode(%d), curCtid({%hu, %u}, %hu).",
                static_cast<int>(mode), curCtid.GetFileId(), curCtid.GetBlockNum(), curCtid.GetOffset()));
            break;
        }
    }

    return result;
}

RetStatus HeapLockTupHandler::DoLock(HeapLockTupleContext *lockContext, TdId &tdId)
{
    StorageAssert(lockContext != nullptr);
    StorageAssert(lockContext->ctid != INVALID_ITEM_POINTER);
    StorageAssert(m_thrd->GetActiveTransaction() != nullptr);
    Xid xid = m_thrd->GetActiveTransaction()->GetCurrentXid();

    /* Step 1: get tuple from buffer */
    ItemPointerData ctid = lockContext->ctid;
    HeapPage *page = static_cast<HeapPage *>(m_bufferDesc->GetPage());
    OffsetNumber offset = ctid.GetOffset();
    HeapDiskTuple *diskTuple = page->GetDiskTuple(offset);
    StorageReleasePanic(diskTuple == nullptr, MODULE_HEAP, ErrMsg("Heap disk tuple is null when do locking."));

    /* Step 2: if the tuple has not been locked by me, lock it */
    if (diskTuple->GetLockerTdId() == INVALID_TD_SLOT ||
        page->GetTd(diskTuple->GetLockerTdId())->GetLockerXid() != xid) {
        /* Step 3: try to lock tuple */
        if (tdId == INVALID_TD_SLOT) {
            m_tdcontext.Begin(m_heapRel->m_pdbId, m_instance->GetCsnMgr()->GetRecycleCsnMin(m_heapRel->m_pdbId));
            tdId = page->AllocTd(m_tdcontext);
            if (m_tdcontext.allocTd.isDirty) {
                (void)m_bufMgr->MarkDirty(m_bufferDesc);
                GenerateAllocTdWal(m_bufferDesc);
            }
            if (tdId == INVALID_TD_SLOT) {
                ErrLog(DSTORE_WARNING, MODULE_HEAP, ErrMsg("No available td slot when lock tuple, page(%hu, %u)",
                    ctid.GetFileId(), ctid.GetBlockNum()));
                return DSTORE_FAIL;
            }
        }

        diskTuple->SetLockerTdId(tdId);
        page->GetTd(tdId)->SetLockerXid(xid);
        page->GetTd(tdId)->SetStatus(TDStatus::OCCUPY_TRX_IN_PROGRESS);
        (void)m_bufMgr->MarkDirty(m_bufferDesc, false);
    }

    /* Step 4: copy tuple for caller if needed. */
    if (lockContext->needRetTup) {
        StorageReleasePanic(lockContext->retTup, MODULE_HEAP, ErrMsg("Tuple in lock tuple context should be null."));
        HeapTuple *tuple = page->CopyTuple(offset);
        m_bufMgr->UnlockAndRelease(m_bufferDesc);
        m_bufferDesc = nullptr;
        if (unlikely(tuple == nullptr)) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Allocate memory fail when do lock."));
            return DSTORE_FAIL;
        }
        if (unlikely(tuple->GetDiskTuple()->IsFirstLinkChunk())) {
            HeapScanHandler *scan =
                DstoreNew(g_dstoreCurrentMemoryContext) HeapScanHandler(m_instance, m_thrd, m_heapRel, m_isLob);
            if (unlikely(scan == nullptr)) {
                storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
                ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Failed to create HeapScanHandler!"));
                return DSTORE_FAIL;
            }
            if (STORAGE_FUNC_FAIL(scan->Begin(&lockContext->snapshot))) {
                ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Begin scan failed!"));
                DstorePfreeExt(tuple);
                delete scan;
                return DSTORE_FAIL;
            }
            lockContext->retTup = scan->FetchBigTuple(tuple, false);
            scan->EndFetch();
            delete scan;

            DstorePfreeExt(tuple);
            if (unlikely(lockContext->retTup == nullptr)) {
                ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Fetch big tuple fail for locking, ctid({%hu, %u}, %hu).",
                    ctid.GetFileId(), ctid.GetBlockNum(), ctid.GetOffset()));
                return DSTORE_FAIL;
            }
        } else {
            lockContext->retTup = tuple;
        }
    }

    return DSTORE_SUCC;
}

RetStatus HeapLockTupHandler::LockNewestTuple(HeapLockTupleContext *lockContext)
{
    LatencyStat::Timer timer(&HeapPerfUnit::GetInstance().m_lockNewestTupleLatency);

    StorageReleasePanic(lockContext->retTup, MODULE_HEAP, ErrMsg("Tuple in lock tuple context should be null."));
    AutoMemCxtSwitch autoMemCxtSwitch(m_thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_STACK));
    RetStatus result = DSTORE_FAIL;
    ItemPointerData &ctid = lockContext->ctid;

    /* Step 1: allocate transaction slot if necessary */
    Transaction *transaction = m_thrd->GetActiveTransaction();
    StorageAssert(transaction != nullptr);
    if (STORAGE_FUNC_FAIL(transaction->AllocTransactionSlot())) {
        lockContext->failureInfo.reason = HeapHandlerFailureReason::ALLOC_TRANS_SLOT_FAILED;
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Failed to prepare undo when locking newest tuple"
            "({%hu, %u}, %hu).", ctid.GetFileId(), ctid.GetBlockNum(), ctid.GetOffset()));
        TryToUnlockReleaseBufferDesc();
        return result;
    }

LOCKTUPSTART:
    /* Step 2: init bufferDesc */
    if (STORAGE_FUNC_FAIL(InitBufferDesc(ctid))) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Failed to init buffer desc when locking newest"
            " tuple({%hu, %u}, %hu).",
            lockContext->ctid.GetFileId(), lockContext->ctid.GetBlockNum(), lockContext->ctid.GetOffset()));
        lockContext->failureInfo.reason = HeapHandlerFailureReason::READ_BUFFER_FAILED;
        return DSTORE_FAIL;
    }

    /* Step 3: get the newest tuple */
    ItemPointerData newestCtid = INVALID_ITEM_POINTER;
    if (STORAGE_FUNC_FAIL(GetNewestVersionCtid<true>(ctid, newestCtid))) {
        if (StorageGetErrorCode() == TRANSACTION_INFO_SAME_THREAD_DEADLOCK ||
            StorageGetErrorCode() == LOCK_ERROR_DEADLOCK) {
            lockContext->failureInfo.reason = HeapHandlerFailureReason::DEADLOCK;
        } else if (StorageGetErrorCode() == LOCK_ERROR_WAIT_TIMEOUT) {
            lockContext->failureInfo.reason = HeapHandlerFailureReason::LOCK_WAIT_TIMEOUT;
        } else if (StorageGetErrorCode() == LOCK_ERROR_WAIT_CANCELED) {
            lockContext->failureInfo.reason = HeapHandlerFailureReason::LOCK_WAIT_CANCELED;
        } else {
            lockContext->failureInfo.reason = HeapHandlerFailureReason::GET_NEWEST_CTID_FAILED;
        }
        ErrLog(DSTORE_DEBUG1, MODULE_HEAP, ErrMsg("Failed to get newest version ctid({%hu, %u}, %hu).",
            ctid.GetFileId(), ctid.GetBlockNum(), ctid.GetOffset()));
        TryToUnlockReleaseBufferDesc();
        return result;
    }
    ctid = newestCtid;

    /* Step 4: lock the newest tuple if it is not deleted */
    TdId tdId = INVALID_TD_SLOT;
    if (ctid != INVALID_ITEM_POINTER && STORAGE_FUNC_SUCC(DoLock(lockContext, tdId))) {
        result = DSTORE_SUCC;
    } else {
        if (ctid == INVALID_ITEM_POINTER) {
            lockContext->failureInfo.reason = HeapHandlerFailureReason::DELETED;
        } else {
            lockContext->failureInfo.reason = HeapHandlerFailureReason::LOCK_TUP_FAILED;
        }
        ErrLog(DSTORE_DEBUG1, MODULE_HEAP, ErrMsg("Lock newest tuple failed, ctid({%hu, %u}, %hu)",
            ctid.GetFileId(), ctid.GetBlockNum(), ctid.GetOffset()));
    }

    /* Step 5: release and retry if necessary */
    if (STORAGE_FUNC_FAIL(result) && CanRetry(lockContext->failureInfo.reason)) {
        ErrLog(DSTORE_WARNING, MODULE_HEAP, ErrMsg("Lock newest failed, retry alloctd, ctid({%hu, %u}, %hu)",
            lockContext->ctid.GetFileId(), lockContext->ctid.GetBlockNum(), lockContext->ctid.GetOffset()));
        goto LOCKTUPSTART;
    }

    TryToUnlockReleaseBufferDesc();

    ErrLog(DSTORE_DEBUG1, MODULE_HEAP, ErrMsg("Lock newest tuple, result: %d, ctid({%hu, %u}, %hu)",
        static_cast<int>(result), ctid.GetFileId(), ctid.GetBlockNum(), ctid.GetOffset()));

    return result;
}

} /* End of namespace DSTORE */
