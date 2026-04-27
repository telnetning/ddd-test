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
 * dstore_heap_delete.cpp
 *     This file defines the building blocks that bg page writer needs.
 *
 * IDENTIFICATION
 *        storage/src/heap/dstore_heap_delete.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "common/log/dstore_log.h"
#include "common/dstore_datatype.h"
#include "errorcode/dstore_transaction_error_code.h"
#include "errorcode/dstore_lock_error_code.h"
#include "common/dstore_common_utils.h"
#include "wal/dstore_wal_write_context.h"
#include "transaction/dstore_transaction_mgr.h"
#include "logical_replication/dstore_decode_dict.h"
#include "errorcode/dstore_heap_error_code.h"
#include "heap/dstore_heap_wal_struct.h"
#include "heap/dstore_heap_undo_struct.h"
#include "heap/dstore_heap_prune.h"
#include "heap/dstore_heap_perf_unit.h"
#include "heap/dstore_heap_delete.h"

namespace DSTORE {

HeapDeleteHandler::HeapDeleteHandler(StorageInstance *instance, ThreadContext *thread, StorageRelation heapRel,
                                     bool isLobOperation)
    : HeapHandler(instance, thread, heapRel, isLobOperation),
      HeapLockTupHandler(instance, thread, heapRel, isLobOperation),
      m_isLinkTuple(false)
{}

RetStatus HeapDeleteHandler::Delete(HeapDeleteContext *delContext)
{
    LatencyStat::Timer timer(&HeapPerfUnit::GetInstance().m_heapDeleteLatency);
    RetStatus result = DSTORE_FAIL;
    StorageReleasePanic(delContext->returnTup, MODULE_HEAP, ErrMsg("Tuple in delete context should be null."));
    AutoMemCxtSwitch autoMemCxtSwitch(m_thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_QUERY));

    StorageAssert(m_thrd->GetActiveTransaction() != nullptr);
    if (STORAGE_FUNC_FAIL(m_thrd->GetActiveTransaction()->AllocTransactionSlot())) {
        delContext->failureInfo.reason = HeapHandlerFailureReason::ALLOC_TRANS_SLOT_FAILED;
        ErrLog(DSTORE_ERROR, MODULE_HEAP,
            ErrMsg("Failed to prepare undo when deleting tuple({%hu, %u}, %hu).",
            delContext->ctid.GetFileId(), delContext->ctid.GetBlockNum(), delContext->ctid.GetOffset()));
        return DSTORE_FAIL;
    }

DELETESTART:
    if (STORAGE_FUNC_FAIL(InitBufferDesc(delContext->ctid))) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP,
            ErrMsg("Failed init buffer desc when deleting tuple({%hu, %u}, %hu).",
            delContext->ctid.GetFileId(), delContext->ctid.GetBlockNum(), delContext->ctid.GetOffset()));
        delContext->failureInfo.reason = HeapHandlerFailureReason::READ_BUFFER_FAILED;
        return DSTORE_FAIL;
    }
    do {
        /* Failure info must be set inside the BeginDelete. */
        if (STORAGE_FUNC_FAIL(BeginDelete(delContext))) {
            ErrLog(DSTORE_DEBUG1, MODULE_HEAP, ErrMsg("Begin delete failed, ctid({%hu, %u}, %hu)",
                delContext->ctid.GetFileId(), delContext->ctid.GetBlockNum(), delContext->ctid.GetOffset()));
            break;
        }

        if (STORAGE_FUNC_FAIL(DoDelete(delContext))) {
            delContext->failureInfo.reason = HeapHandlerFailureReason::DELETE_FAILED;
            ErrLog(DSTORE_WARNING, MODULE_HEAP, ErrMsg("Delete failed, ctid({%hu, %u}, %hu)",
                delContext->ctid.GetFileId(), delContext->ctid.GetBlockNum(), delContext->ctid.GetOffset()));
            break;
        }

        if (STORAGE_FUNC_FAIL(EndDelete(delContext))) {
            delContext->failureInfo.reason = HeapHandlerFailureReason::DELETE_FAILED;
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("End delete failed, ctid({%hu, %u}, %hu)",
                delContext->ctid.GetFileId(), delContext->ctid.GetBlockNum(), delContext->ctid.GetOffset()));
            break;
        }

        result = DSTORE_SUCC;
    } while (false);

    /* If delete bigtuple, no need to retry alloctd, beacase we don't know where we are going,
     * which small tuple has been deleted. Transaction excute fail and rollback directly.
     */
    if (STORAGE_FUNC_FAIL(result) && !m_isLinkTuple && CanRetry(delContext->failureInfo.reason)) {
        ErrLog(DSTORE_WARNING, MODULE_HEAP, ErrMsg("Delete failed, retry alloctd, ctid({%hu, %u}, %hu)",
            delContext->ctid.GetFileId(), delContext->ctid.GetBlockNum(), delContext->ctid.GetOffset()));
        goto DELETESTART;
    }

    TryToUnlockReleaseBufferDesc();

    ErrLog(DSTORE_DEBUG1, MODULE_HEAP, ErrMsg("Delete tuple, result: %d, ctid({%hu, %u}, %hu) snapshot csn %lu",
        static_cast<int>(result), delContext->ctid.GetFileId(), delContext->ctid.GetBlockNum(),
        delContext->ctid.GetOffset(), thrd->GetSnapShotCsn()));

    return result;
}

RetStatus HeapDeleteHandler::BeginDelete(HeapDeleteContext *delContext)
{
    CommandId tupleCid = INVALID_CID;
    bool tupleChanged = false;
    if (STORAGE_FUNC_FAIL(CheckTupleChanged(delContext->ctid, tupleChanged, tupleCid, &delContext->snapshot,
        delContext->executedEpq))) {
        if (StorageGetErrorCode() == TRANSACTION_INFO_SAME_THREAD_DEADLOCK ||
            StorageGetErrorCode() == LOCK_ERROR_DEADLOCK) {
            delContext->failureInfo.reason = HeapHandlerFailureReason::DEADLOCK;
        } else if (StorageGetErrorCode() == LOCK_ERROR_WAIT_TIMEOUT) {
            delContext->failureInfo.reason = HeapHandlerFailureReason::LOCK_WAIT_TIMEOUT;
        } else if (StorageGetErrorCode() == LOCK_ERROR_WAIT_CANCELED) {
            delContext->failureInfo.reason = HeapHandlerFailureReason::LOCK_WAIT_CANCELED;
        } else if (StorageGetErrorCode() == HEAP_ERROR_ROLLBACK_BIG_TUPLE_FAILED) {
            delContext->failureInfo.reason = HeapHandlerFailureReason::ROLLBACK_BIG_TUPLE_FAILED;
        } else {
            delContext->failureInfo.reason = HeapHandlerFailureReason::CHECK_TUPLE_CHANGED_FAILED;
        }
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Check tuple changed fail when begin delete, ctid({%hu, %u}, %hu)",
            delContext->ctid.GetFileId(), delContext->ctid.GetBlockNum(), delContext->ctid.GetOffset()));
        return DSTORE_FAIL;
    }

    if (tupleChanged) {
        if (STORAGE_FUNC_FAIL(
            SetFailureInfo(delContext->failureInfo, delContext->ctid, tupleCid, &delContext->snapshot))) {
            return DSTORE_FAIL;
        }
        ErrLog(DSTORE_DEBUG1, MODULE_HEAP, ErrMsg("Tuple is changed when begin delete, ctid({%hu, %u}, %hu)",
            delContext->ctid.GetFileId(), delContext->ctid.GetBlockNum(), delContext->ctid.GetOffset()));
        storage_set_error(HEAP_ERROR_TUPLE_IS_CHANGED);
        return DSTORE_FAIL;
    }

    HeapPage *heapPage = static_cast<HeapPage *>(m_bufferDesc->GetPage());
    if (unlikely(heapPage->IsDiskTupleDeleted(delContext->ctid.GetOffset()))) {
        delContext->failureInfo.reason = HeapHandlerFailureReason::DELETED;
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Tuple is deleted when begin delete, ctid({%hu, %u}, %hu)",
            delContext->ctid.GetFileId(), delContext->ctid.GetBlockNum(), delContext->ctid.GetOffset()));
        storage_set_error(HEAP_ERROR_TUPLE_IS_DELETED);
        return DSTORE_FAIL;
    }

    m_isLinkTuple = heapPage->GetDiskTuple(delContext->ctid.GetOffset())->IsLinked();

    return DSTORE_SUCC;
}

RetStatus HeapDeleteHandler::DeleteSmallTuple(HeapDeleteContext *delContext)
{
    return DeleteDiskTuple(delContext, delContext->ctid.GetOffset());
}

ItemPointerData HeapDeleteHandler::GetNextChunk(BufferDesc *bufferDesc, OffsetNumber offset)
{
    return static_cast<HeapPage *>(bufferDesc->GetPage())->GetDiskTuple(offset)->GetNextChunkCtid();
}

RetStatus HeapDeleteHandler::DeleteBigTuple(HeapDeleteContext *delContext)
{
    uint32 numTupChunk = 0, i = 0;
    bool retTupIsBig = false;
    HeapTuple **tupChunks = nullptr;
    ItemPointerData ctid = delContext->ctid;
    ItemPointerData nextCtid = GetNextChunk(m_bufferDesc, ctid.GetOffset());
    if (STORAGE_FUNC_FAIL(DeleteDiskTuple(delContext, ctid.GetOffset()))) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Failed to delete the first chunk when deleting big tuple. "
            "chunkId({%hu, %u}, %hu).", ctid.GetFileId(), ctid.GetBlockNum(), ctid.GetOffset()));
        return DSTORE_FAIL;
    }

    UpdateFsmForPrune(ctid.GetOffset());

    if (delContext->needReturnTup && delContext->returnTup->GetDiskTuple()->IsLinked()) {
        retTupIsBig = true;
        numTupChunk = delContext->returnTup->GetDiskTuple()->GetNumChunks();
        tupChunks = static_cast<HeapTuple **>(DstorePalloc(sizeof(HeapTuple *) * numTupChunk));
        if (unlikely(tupChunks == nullptr)) {
            DstorePfree(delContext->returnTup);
            ErrLog(DSTORE_ERROR, MODULE_HEAP,
                ErrMsg("DstorePalloc fail size(%u), chunk number(%u) when delete big tuple.",
                static_cast<uint32>(sizeof(HeapTuple *) * numTupChunk), numTupChunk));
            return DSTORE_FAIL;
        }
        tupChunks[i++] = delContext->returnTup;
    }
    /* Delete all tupChunks. */
    while (nextCtid != INVALID_ITEM_POINTER) {
        UNUSE_PARAM AutoMemCxtSwitch autoMemCxtSwitch(m_thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_STACK));
        ctid = nextCtid;
        TryToUnlockReleaseBufferDesc();
        m_bufferDesc = m_bufMgr->Read(m_heapRel->m_pdbId, ctid.GetPageId(), LW_EXCLUSIVE);
        if (unlikely(m_bufferDesc == INVALID_BUFFER_DESC)) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP,
                ErrMsg("Read page failed when delete big tuple, chunkId({%hu, %u}, %hu).",
                ctid.GetFileId(), ctid.GetBlockNum(), ctid.GetOffset()));
            return DSTORE_FAIL;
        }
        CheckBufferedPage(m_bufferDesc->GetPage(), ctid.GetPageId());

        StorageAssert(m_bufferDesc->GetPageId() == ctid.GetPageId());
        nextCtid = GetNextChunk(m_bufferDesc, ctid.GetOffset());
        if (STORAGE_FUNC_FAIL(DeleteDiskTuple(delContext, ctid.GetOffset()))) {
            m_bufMgr->UnlockAndRelease(m_bufferDesc);
            m_bufferDesc = nullptr;
            for (uint32 cnt = 0; cnt < i && retTupIsBig; cnt++) {
                StorageAssert(tupChunks != nullptr);
                DstorePfreeExt(tupChunks[cnt]);
            }
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Failed to delete other chunks when deleting big tuple. "
                "chunkId({%hu, %u}, %hu).", ctid.GetFileId(), ctid.GetBlockNum(), ctid.GetOffset()));
            return DSTORE_FAIL;
        }

        UpdateFsmForPrune(ctid.GetOffset());

        if (delContext->needReturnTup) {
            StorageAssert(tupChunks != nullptr);
            tupChunks[i++] = delContext->returnTup;
        }
    }
    StorageAssert(i == numTupChunk);
    if (retTupIsBig) {
        delContext->returnTup = AssembleTuples(tupChunks, static_cast<uint32>(numTupChunk));
        StorageAssert(tupChunks != nullptr);
        for (uint32 cnt = 0; cnt < i; cnt++) {
            DstorePfree(tupChunks[cnt]);
        }
        if (unlikely(delContext->returnTup == nullptr)) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Assemble tuples fail when deleting big tuple. "
                "Chunk num(%u), current chunkId({%hu, %u}, %hu)",
                numTupChunk, ctid.GetFileId(), ctid.GetBlockNum(), ctid.GetOffset()));
            return DSTORE_FAIL;
        }
    }
    return DSTORE_SUCC;
}

RetStatus HeapDeleteHandler::DoDelete(HeapDeleteContext *delContext)
{
    HeapPruneHandler prune(m_instance, m_heapRel, m_thrd, m_bufferDesc, m_isLob);
    prune.TryPrunePage();

    if (m_isLinkTuple) {
        return DeleteBigTuple(delContext);
    }
    return DeleteSmallTuple(delContext);
}

RetStatus HeapDeleteHandler::DeleteDiskTuple(HeapDeleteContext *delContext, OffsetNumber offset)
{
    HeapTuple tuple;
    AtomicWalWriterContext *walContext = m_thrd->m_walWriterContext;
    HeapPage *page = static_cast<HeapPage *>(m_bufferDesc->GetPage());
    Transaction *transaction = m_thrd->GetActiveTransaction();
    StorageAssert(transaction != nullptr);
    TransactionMgr *transactionMgr = m_instance->GetPdb(m_heapRel->m_pdbId)->GetTransactionMgr();
    Xid xid = transaction->GetCurrentXid();
    /* Step 1: Allocate TD id */
    m_tdcontext.Begin(m_heapRel->m_pdbId, m_instance->GetCsnMgr()->GetRecycleCsnMin(m_heapRel->m_pdbId));
    TdId tdId = page->AllocTd(m_tdcontext);
    /* transaction slot must be reserved before deleting tuple from page */
    if (tdId == INVALID_TD_SLOT) {
        ErrLog(DSTORE_WARNING, MODULE_HEAP, ErrMsg("Td slot is invalid when deleting diskTuple."));
        return DSTORE_FAIL;
    }
    /* Step 2: remove the locker td id */
    page->GetTuple(&tuple, offset);
    HeapDiskTuple *diskTuple = tuple.GetDiskTuple();
    diskTuple->SetLockerTdId(INVALID_TD_SLOT);

    /* Step 3: copy tuple if needed */
    if (delContext->needReturnTup || delContext->needDeleteLob) {
        delContext->returnTup = page->CopyTuple(offset);
        if (unlikely(delContext->returnTup == nullptr)) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Allocate memory fail when delete heap tuple."));
            return DSTORE_FAIL;
        }
    }

    /* Step 4: Generate undo record and redo record */
    uint32 diskTupleSize = tuple.GetDiskTupleSize();
    uint32 size = sizeof(UndoDataHeapDelete) + tuple.GetDiskTupleSize();
    UndoDataHeapDelete* undoData = static_cast<UndoDataHeapDelete*>(DstorePalloc(size));
    if (unlikely(undoData == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("DstorePalloc fail size(%u).", size));
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        return DSTORE_FAIL;
    }
    undoData->Init();
    undoData->Append(static_cast<char *>(static_cast<void *>(diskTuple)), static_cast<uint16>(diskTupleSize));
    StorageAssert(size == undoData->GetSize());
    StorageAssert(tdId < page->GetTdCount());
    ItemPointerData ctid(m_bufferDesc->GetPageId(), offset);
    UndoType type = GetTableSmgr()->IsGlobalTempTable() ? UNDO_HEAP_DELETE_TMP : UNDO_HEAP_DELETE;
    UndoRecord undoRecord(type, tdId, page->GetTd(tdId), ctid, delContext->cid);
    RetStatus ret = undoRecord.Append(static_cast<char *>(static_cast<void *>(undoData)), undoData->GetSize());
    if (STORAGE_FUNC_FAIL(ret)) {
        if (m_tdcontext.allocTd.isDirty) {
            (void)m_bufMgr->MarkDirty(m_bufferDesc);
            GenerateAllocTdWal(m_bufferDesc);
        }
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        ErrLog(DSTORE_ERROR, MODULE_HEAP,
               ErrMsg("DstorePalloc fail size(%u) when append undodata.", undoData->GetSize()));
        DstorePfree(undoData);
        return DSTORE_FAIL;
    }
    DstorePfree(undoData);
    /* now we has done alloctd, so we need record alloctd wal */
    if (STORAGE_FUNC_FAIL(transactionMgr->ExtendUndoSpaceIfNeeded(xid, undoRecord.GetRecordSize()))) {
        if (m_tdcontext.allocTd.isDirty) {
            (void)m_bufMgr->MarkDirty(m_bufferDesc);
            GenerateAllocTdWal(m_bufferDesc);
        }
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Extend undo space fail when delete heap tuple."));
        return DSTORE_FAIL;
    }

    /* Step 5: Delete tuple in page */
    diskTuple->SetTdId(tdId);
    diskTuple->SetXid(xid);
    page->DelTuple(offset);
    page->SetTuplePrunable(true);
    diskTuple->SetTdStatus(ATTACH_TD_AS_NEW_OWNER);
    (void)m_bufMgr->MarkDirty(m_bufferDesc);
    /* Set potential fsm size of page */
    page->AddPotentialDelItemSize(offset);

    walContext->BeginAtomicWal(xid);
    UndoRecPtr undoRecPtr = InsertUndoAndCheck(&undoRecord);
    if (unlikely(undoRecPtr == INVALID_UNDO_RECORD_PTR)) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Insert undo record fail when delete heap tuple."));
        return DSTORE_FAIL;
    }
    page->SetTd(tdId, xid, undoRecPtr, delContext->cid);
    if (likely(NeedWal())) {
        /* Record WalRecordHeapDelete */
        GenerateHeapDeleteWal(m_bufferDesc, offset, undoRecPtr, tdId, delContext->cid, &tuple,
                              delContext->replicaKeyAttrs);
    }
    (void)walContext->EndAtomicWal();
    return DSTORE_SUCC;
}

/* update fsm when deleting big tuple, because we can find the pages to prune them when GetBuffer(). */
void HeapDeleteHandler::UpdateFsmForPrune(OffsetNumber offset)
{
    HeapPage *page = static_cast<HeapPage *>(m_bufferDesc->GetPage());
    uint32 tupAlignedSize = page->GetItemIdPtr(offset)->GetLen();
    RetStatus ret = GetTableSmgr()->UpdateFSM(page->GetFsmIndex(),
                                              page->GetFreeSpace<FreeSpaceCondition::RAW>() + tupAlignedSize);
    StorageReleasePanic(STORAGE_FUNC_FAIL(ret), MODULE_HEAP, ErrMsg("Update fsm failed, size(%u).",
        page->GetFreeSpace<FreeSpaceCondition::RAW>() + tupAlignedSize));
}

RetStatus HeapDeleteHandler::EndDelete(UNUSE_PARAM HeapDeleteContext* delContext) const
{
    StorageAssert(((delContext->needReturnTup && delContext->returnTup) ||
                  (!delContext->needReturnTup && delContext->needDeleteLob && delContext->returnTup) ||
                  (!delContext->needReturnTup && !delContext->needDeleteLob && delContext->returnTup == nullptr)));
    return DSTORE_SUCC;
}

void HeapDeleteHandler::GenerateHeapDeleteWal(BufferDesc *bufferDesc, OffsetNumber offset, UndoRecPtr undoPtr,
                                              uint8 tdId, CommandId cid, HeapTuple *oldTuple,
                                              Bitmapset *replicaKeyAttrs)
{
    AtomicWalWriterContext *walContext = m_thrd->m_walWriterContext;
    StorageReleasePanic(!walContext->HasAlreadyBegin(), MODULE_HEAP, ErrMsg("Wal has not atomic begin."));
    walContext->RememberPageNeedWal(m_bufferDesc);

    HeapPage *page = static_cast<HeapPage *>(bufferDesc->GetPage());
    bool glsnChangedFlag = (page->GetWalId() != walContext->GetWalId());
    uint64 fileVersion = bufferDesc->GetFileVersion();
    uint32 allocTdSize = WalRecordForDataPage::GetAllocTdSize(m_tdcontext);
    uint32 walDataSize = sizeof(WalRecordHeapDelete) + allocTdSize;
#ifdef ENABLE_LOGICAL_REPL
    HeapTuple *oldTupleKey = nullptr;
    HeapDiskTuple *oldDiskTupleKey = nullptr;
    Oid tableOid = GetTableOid();
    if (WalLogicalActive()) {
        oldTupleKey = FormIdentityTuple(oldTuple, replicaKeyAttrs);
        oldDiskTupleKey = (oldTupleKey != nullptr ? oldTupleKey->GetDiskTuple() : nullptr);
        StorageReleasePanic(oldDiskTupleKey == nullptr, MODULE_HEAP, ErrMsg("oldDiskTupleKey is null."));
        walDataSize += oldDiskTupleKey->GetTupleSize();
        walDataSize = walDataSize + sizeof(Oid) + sizeof(CommitSeqNo);
        if (unlikely(IsLogicalDecodeDictNeeded(tableOid))) {
            walDataSize += sizeof(CommandId);
        }
    }
#else
    UNUSED_VARIABLE(cid);
    UNUSED_VARIABLE(oldTuple);
    UNUSED_VARIABLE(replicaKeyAttrs);
#endif

    /* Record WalRecordHeapDelete and AllocTd */
    WalRecordHeapDelete *walData = static_cast<WalRecordHeapDelete *>(walContext->GetTempWalBuf());
    StorageReleasePanic(walDataSize > BLCKSZ, MODULE_HEAP, ErrMsg("The value of dstore palloc exceeds BLCKSZ, "
    "the walDataSize is (%u)", walDataSize));
    walData->SetHeader({WAL_HEAP_DELETE, walDataSize, bufferDesc->GetPageId(), page->GetWalId(), page->GetPlsn(),
        page->GetGlsn(), glsnChangedFlag, fileVersion}, offset, undoPtr, tdId);

#ifdef ENABLE_LOGICAL_REPL
    if (WalLogicalActive()) {
        CommitSeqNo snapshotCsn = m_thrd->GetActiveTransaction()->GetSnapshotCsn();
        CommandId snapshotCid = INVALID_CID;
        if (unlikely(IsLogicalDecodeDictNeeded(tableOid))) {
            snapshotCid = cid;
        }
        walData->SetLogicalDecodeInfo(oldDiskTupleKey, &tableOid, &snapshotCsn, &snapshotCid);
        DstorePfreeExt(oldTupleKey);
    }
#endif
    walData->SetAllocTd(m_tdcontext);
    walContext->PutNewWalRecord(walData);
}

}
