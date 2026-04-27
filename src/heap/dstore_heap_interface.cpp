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
 * dstore_heap_interface.cpp
 *
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/dstore/src/heap/dstore_heap_interface.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "catalog/dstore_fake_type.h"
#include "errorcode/dstore_heap_error_code.h"
#include "heap/dstore_heap_insert.h"
#include "heap/dstore_heap_scan.h"
#include "heap/dstore_heap_update.h"
#include "heap/dstore_heap_delete.h"
#include "heap/dstore_heap_vacuum.h"
#include "heap/dstore_heap_lob.h"
#include "heap/dstore_heap_interface.h"

namespace HeapInterface {
using namespace DSTORE;

HeapInsertHandler *CreateHeapInsertHandler(bool isUseRingBuf /* = false */)
{
    bool isLob = false;
    HeapInsertHandler *heapInsertHandler = DstoreNew(thrd->GetTopTransactionMemoryContext())
        HeapInsertHandler(g_storageInstance, thrd, nullptr, isLob, isUseRingBuf);
    if (unlikely(heapInsertHandler == nullptr)) {
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("heapInsertHandler is nullptr."));
        return nullptr;
    }
    return heapInsertHandler;
}
RetStatus Insert(StorageRelation relation, HeapTuple *tuple, CommandId cid)
{
    if (unlikely(relation == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Invalid input for inserting!"));
        storage_set_error(HEAP_ERROR_INPUT_PARAM_WRONG);
        return DSTORE_FAIL;
    }
    ItemPointerData ctid;
    return Insert(relation, tuple, ctid, cid);
}

RetStatus Insert(StorageRelation relation, HeapTuple *tuple, ItemPointerData &ctid, CommandId cid)
{
    if (unlikely((relation == nullptr || relation->tableSmgr == nullptr || tuple == nullptr))) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Invalid input for inserting!"));
        storage_set_error(HEAP_ERROR_INPUT_PARAM_WRONG);
        return DSTORE_FAIL;
    }

    TableStorageMgr *tableSmgr = relation->tableSmgr;
    TableStorageMgr *lobTableSmgr = relation->lobTableSmgr;

    TupleDesc tupDesc = tableSmgr->GetTupleDesc();
    if (unlikely(tupDesc != nullptr && tupDesc->tdhaslob && lobTableSmgr != nullptr)) {
        HeapLobHandler lobHandler(relation);
        if (STORAGE_FUNC_FAIL(lobHandler.Insert(tuple, cid))) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Failed to insert lob heap before insert heap!"));
            return DSTORE_FAIL;
        }
    }

    HeapInsertContext context;
    HeapInsertHandler heapInsert(g_storageInstance, thrd, relation);

    context.heapTuple = tuple;
    context.ctid = INVALID_ITEM_POINTER;
    context.cid = cid;
    RetStatus status = heapInsert.Insert(&context);
    ctid = context.ctid;
    tuple->SetCtid(ctid);
    heapInsert.ReportHeapBufferReadStat(relation);
    return status;
}

RetStatus BatchInsert(HeapInsertHandler *heapInsertHandler, StorageRelation relation, HeapTuple **tuples,
                      uint16_t nTuples, CommandId cid)
{
    if (unlikely((relation == nullptr || relation->tableSmgr == nullptr || tuples == nullptr))) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Invalid input for batch inserting!"));
        storage_set_error(HEAP_ERROR_INPUT_PARAM_WRONG);
        return DSTORE_FAIL;
    }

    TableStorageMgr *tableSmgr = relation->tableSmgr;
    TableStorageMgr *lobTableSmgr = relation->lobTableSmgr;

    TupleDesc tupDesc = tableSmgr->GetTupleDesc();
    if (tupDesc != nullptr && tupDesc->tdhaslob && lobTableSmgr != nullptr) {
        HeapLobHandler lobHandler(relation);
        if (STORAGE_FUNC_FAIL(lobHandler.BatchInsert(tuples, nTuples, cid))) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Batch insert lob tuple failed!"));
            return DSTORE_FAIL;
        }
    }

    AutoMemCxtSwitch autoMemCxtSwitch(thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_STACK));
    HeapInsertContext *contexts = static_cast<HeapInsertContext *>(DstorePalloc(sizeof(HeapInsertContext) * nTuples));
    if (unlikely(contexts == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("DstorePalloc fail size(%u) when batch insert.",
            static_cast<uint32>(sizeof(HeapInsertContext) * nTuples)));
        return DSTORE_FAIL;
    }

    for (uint16_t i = 0; i < nTuples; i++) {
        if (unlikely(tuples[i] == nullptr)) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("tuples[%hu] is nullptr when batch insert.", i));
            storage_set_error(HEAP_ERROR_INPUT_PARAM_WRONG);
            DstorePfree(contexts);
            return DSTORE_FAIL;
        }
        contexts[i].heapTuple = tuples[i];
        contexts[i].ctid = INVALID_ITEM_POINTER;
        contexts[i].cid = cid;
    }

    HeapBacthInsertContext batchConext{contexts, nTuples};
    RetStatus ret;
    if (unlikely(heapInsertHandler == nullptr)) {
        HeapInsertHandler heapInsert(g_storageInstance, thrd, relation);
        heapInsert.SetHeapRelAndBufMgr(relation);
        ret = heapInsert.BatchInsert(&batchConext);
        heapInsert.ReportHeapBufferReadStat(relation);
    } else {
        HeapInsertHandler *heapInsert = heapInsertHandler;
        heapInsert->SetHeapRelAndBufMgr(relation);
        ret = heapInsert->BatchInsert(&batchConext);
        heapInsert->ReportHeapBufferReadStat(relation);
    }
    DstorePfree(contexts);
    return ret;
}

RetStatus Update(StorageRelation relation, HeapUpdateContext *context, bool isSwappingPartition /* = false */)
{
    if (unlikely((relation == nullptr || relation->tableSmgr == nullptr || context == nullptr ||
                  context->newTuple == nullptr || context->oldCtid == INVALID_ITEM_POINTER))) {
        context->failureInfo.reason = HeapHandlerFailureReason::INVALID_PARAM;
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Invalid input for updating!"));
        storage_set_error(HEAP_ERROR_INPUT_PARAM_WRONG);
        return DSTORE_FAIL;
    }
    StorageReleasePanic(context->snapshot.GetCsn() == INVALID_CSN, MODULE_HEAP,
                        ErrMsg("Invalid snapshot for update."));

    TableStorageMgr *tableSmgr = relation->tableSmgr;
    TableStorageMgr *lobTableSmgr = relation->lobTableSmgr;
    TupleDesc tupDesc = tableSmgr->GetTupleDesc();
    context->needUpdateLob = (tupDesc != nullptr && tupDesc->tdhaslob && lobTableSmgr != nullptr);
    /*
     * when swapping partitions containing external lob, we only need to update the tuple of the heap segment because
     * the table oid of the loblocator has changed. We do not need to update the tuple of the lob segment because the
     * lobvalue no need changed.
     */
    if (unlikely(context->needUpdateLob && !isSwappingPartition)) {
        HeapLobHandler lobHandler(relation);
        if (STORAGE_FUNC_FAIL(lobHandler.Update(context))) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Update lob heap tuple failed."));
            StorageAssert(context->retOldTuple == nullptr);
            return DSTORE_FAIL;
        }
    }
    /*
     * context->retOldTuple contains the old tuple with lob values appended (if any).
     * When context->needReturnOldTup is True, HeapUpdateHandler::Update will change
     * context->retOldTuple to contain the old tuple without lob values appended, we
     * save it to oldTuple so we can return the old tuple with lob values appended
     * to the caller.
     */
    HeapTuple *oldTuple = context->retOldTuple;
    context->retOldTuple = nullptr;

    HeapUpdateHandler heapUpdate(g_storageInstance, thrd, relation);
    RetStatus status = heapUpdate.Update(context);
    heapUpdate.ReportHeapBufferReadStat(relation);

    if (unlikely(context->needUpdateLob)) {
        DstorePfreeExt(context->retOldTuple);
        context->retOldTuple = oldTuple;
    }

    if (likely(!context->needReturnOldTup)) {
        DstorePfreeExt(context->retOldTuple);
    }
    return status;
}

RetStatus ForceUpdateTupleDataNoTrx(StorageRelation relation, HeapUpdateContext *context, bool wait_flush)
{
    if (unlikely((relation == nullptr || relation->tableSmgr == nullptr || context == nullptr ||
                  context->newTuple == nullptr || context->oldCtid == INVALID_ITEM_POINTER))) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Invalid input!"));
        storage_set_error(HEAP_ERROR_INPUT_PARAM_WRONG);
        return DSTORE_FAIL;
    }

    HeapUpdateHandler heapUpdate(g_storageInstance, thrd, relation);
    RetStatus status = heapUpdate.ForceUpdateTupleDataNoTrx(context, wait_flush);
    heapUpdate.ReportHeapBufferReadStat(relation);

    if (!context->needReturnOldTup) {
        DstorePfreeExt(context->retOldTuple);
    }
    return status;
}

RetStatus Delete(StorageRelation relation, HeapDeleteContext *context)
{
    if (unlikely((relation == nullptr || relation->tableSmgr == nullptr || context == nullptr ||
                  context->ctid == INVALID_ITEM_POINTER))) {
        context->failureInfo.reason = HeapHandlerFailureReason::INVALID_PARAM;
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Invalid input for deleting!"));
        storage_set_error(HEAP_ERROR_INPUT_PARAM_WRONG);
        return DSTORE_FAIL;
    }
    StorageReleasePanic(context->snapshot.GetCsn() == INVALID_CSN, MODULE_HEAP,
                        ErrMsg("Invalid snapshot for delete."));

    TableStorageMgr *tableSmgr = relation->tableSmgr;
    TableStorageMgr *lobTableSmgr = relation->lobTableSmgr;
    TupleDesc tupDesc = tableSmgr->GetTupleDesc();
    context->needDeleteLob = (tupDesc != nullptr && tupDesc->tdhaslob && lobTableSmgr != nullptr);

    HeapDeleteHandler heapDelete(g_storageInstance, thrd, relation);
    RetStatus status = heapDelete.Delete(context);
    heapDelete.ReportHeapBufferReadStat(relation);

    if (status == DSTORE_SUCC && context->needDeleteLob) {
        HeapLobHandler lobHandler(relation);
        status = lobHandler.Delete(context);

        if (!context->needReturnTup) {
            DstorePfreeExt(context->returnTup);
        }
    }
    return status;
}

RetStatus LazyVacuum(StorageRelation relation)
{
    if (unlikely(relation == nullptr || relation->tableSmgr == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Invalid input for lazy vacuuming!"));
        storage_set_error(HEAP_ERROR_INPUT_PARAM_WRONG);
        return DSTORE_FAIL;
    }

    HeapVacuumHandler heapVacuum(g_storageInstance, thrd, relation);
    if (STORAGE_FUNC_FAIL(heapVacuum.LazyVacuum())) {
        heapVacuum.ReportHeapBufferReadStat(relation);
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Failed to lazy vacuum. rel oid: %u", relation->relOid));
        return DSTORE_FAIL;
    }

    if (relation->lobTableSmgr != nullptr) {
        HeapVacuumHandler heapLobVacuum(g_storageInstance, thrd, relation, true);
        if (STORAGE_FUNC_FAIL(heapLobVacuum.LazyVacuum())) {
            heapVacuum.ReportHeapBufferReadStat(relation);
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Failed to lazy vacuum for lob. rel oid: %u", relation->relOid));
            return DSTORE_FAIL;
        }
    }
    heapVacuum.ReportHeapBufferReadStat(relation);
    return DSTORE_SUCC;
}

RetStatus LockUnchangedTuple(StorageRelation relation, HeapLockTupleContext *context)
{
    StorageReleasePanic(context == nullptr, MODULE_HEAP,
                        ErrMsg("Invalid context for lock unchanged tuple."));
    if (unlikely((relation == nullptr || relation->tableSmgr == nullptr ||
                  context->ctid == INVALID_ITEM_POINTER))) {
        context->failureInfo.reason = HeapHandlerFailureReason::INVALID_PARAM;
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Invalid input for locking unchanged tuple!"));
        storage_set_error(HEAP_ERROR_INPUT_PARAM_WRONG);
        return DSTORE_FAIL;
    }
    StorageReleasePanic(context->snapshot.GetCsn() == INVALID_CSN, MODULE_HEAP,
                        ErrMsg("Invalid snapshot for lock unchanged tuple."));

    HeapLockTupHandler heapLock(g_storageInstance, thrd, relation);
    RetStatus status = heapLock.LockUnchangedTuple(context);
    heapLock.ReportHeapBufferReadStat(relation);

    return status;
}

RetStatus LockNewestTuple(StorageRelation relation, HeapLockTupleContext *context)
{
    StorageReleasePanic(context == nullptr, MODULE_HEAP,
                        ErrMsg("Invalid context for lock newest tuple."));
    if (unlikely((relation == nullptr || relation->tableSmgr == nullptr ||
                  context->ctid == INVALID_ITEM_POINTER))) {
        context->failureInfo.reason = HeapHandlerFailureReason::INVALID_PARAM;
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Invalid input for locking newest tuple!"));
        storage_set_error(HEAP_ERROR_INPUT_PARAM_WRONG);
        return DSTORE_FAIL;
    }
    StorageReleasePanic(context->snapshot.GetCsn() == INVALID_CSN, MODULE_HEAP,
                        ErrMsg("Invalid snapshot for lock newest tuple."));

    HeapLockTupHandler heapLockTup(g_storageInstance, thrd, relation);
    RetStatus status = heapLockTup.LockNewestTuple(context);
    heapLockTup.ReportHeapBufferReadStat(relation);

    return status;
}

void DestroyHeapInsertHandler(HeapInsertHandler *heapInsertHandler)
{
    if (likely(heapInsertHandler)) {
        delete heapInsertHandler;
    }
}
HeapScanHandler *CreateHeapScanHandler(StorageRelation relation, bool isUseRingBuf /* = false */)
{
    if (unlikely((relation == nullptr || relation->tableSmgr == nullptr))) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Invalid input for creating heap scan!"));
        storage_set_error(HEAP_ERROR_INPUT_PARAM_WRONG);
        return nullptr;
    }

    HeapScanHandler *heapScanHandler = DstoreNew(thrd->GetTopTransactionMemoryContext())
        HeapScanHandler(g_storageInstance, thrd, relation, false, isUseRingBuf);
    if (unlikely(heapScanHandler == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("create heapScanHandler failed!"));
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        return nullptr;
    }
    return heapScanHandler;
}

RetStatus BeginScan(HeapScanHandler *heapScanHandler, Snapshot snapshot, bool showAnyTupForDebug)
{
    StorageReleasePanic(heapScanHandler == nullptr, MODULE_HEAP, ErrMsg("heapScanHandler is nullptr."));
    if (STORAGE_FUNC_FAIL(heapScanHandler->Begin(snapshot, showAnyTupForDebug))) {
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

HeapTuple *SeqScan(HeapScanHandler *heapScanHandler, FailureInfo *failureInfo)
{
    StorageReleasePanic(heapScanHandler == nullptr, MODULE_HEAP, ErrMsg("heapScanHandler is nullptr."));
    HeapTuple* tuple = heapScanHandler->SeqScan();
    /* There are two possible cases when the tuple is nullptr:
     *   1. Scan ends normally and the scanning result is empty.
     *   2. Scan fails, for example, because of READ_BUFFER_FAILED.
     *      When failureInfo is not the default value(UNKNOWN), it is returned to the upper-layer interface.
     */
    if (tuple == nullptr && failureInfo != nullptr &&
        heapScanHandler->m_failureInfo.reason != DSTORE::HeapHandlerFailureReason::UNKNOWN) {
        *failureInfo = heapScanHandler->m_failureInfo;
    }
    return tuple;
}

void ReScan(HeapScanHandler *heapScanHandler)
{
    StorageReleasePanic(heapScanHandler == nullptr, MODULE_HEAP, ErrMsg("heapScanHandler is nullptr."));
    heapScanHandler->ReScan();
}

void ResetController(HeapScanHandler *heapScanHandler)
{
    StorageReleasePanic(heapScanHandler == nullptr, MODULE_HEAP, ErrMsg("heapScanHandler is nullptr."));
    heapScanHandler->ResetController();
}

RetStatus SetParallelController(HeapScanHandler *heapScanHandler, ParallelWorkController *controller, int smpId)
{
    StorageReleasePanic(heapScanHandler == nullptr, MODULE_HEAP, ErrMsg("heapScanHandler is nullptr."));
    StorageReleasePanic(controller == nullptr, MODULE_HEAP, ErrMsg("parallel controller is nullptr."));
    return heapScanHandler->SetParallelController(controller, smpId);
}

RetStatus SampleScan(HeapScanHandler *heapScanHandler, HeapSampleScanContext *context)
{
    if (unlikely((heapScanHandler == nullptr || context == nullptr))) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Invalid input for sample scanning!"));
        storage_set_error(HEAP_ERROR_INPUT_PARAM_WRONG);
        return DSTORE_FAIL;
    }

    return heapScanHandler->SampleScan(context);
}

void EndScan(HeapScanHandler *heapScanHandler)
{
    StorageReleasePanic(heapScanHandler == nullptr, MODULE_HEAP, ErrMsg("heapScanHandler is nullptr."));
    return heapScanHandler->End();
}

ScanKey CreateScanKey(int scanKeyNum)
{
    ScanKey scan_key = static_cast<ScanKeyData *>(DstoreMemoryContextAlloc(thrd->GetTopTransactionMemoryContext(),
        sizeof(ScanKeyData) * static_cast<unsigned int>(scanKeyNum)));
    if (unlikely(scan_key == nullptr)) {
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("CreateScanKey fail."));
        return nullptr;
    }
    return scan_key;
}

void SetScanKey(HeapScanHandler *heapScanHandler, TupleDesc desc, int nkeys, ScanKey key)
{
    return heapScanHandler->SetScanKey(desc, nkeys, key);
}

HeapTuple *FetchTuple(HeapScanHandler *heapScanHandler, ItemPointerData &ctid, bool needCheckVisibility)
{
    return heapScanHandler->FetchTuple(ctid, needCheckVisibility);
}

RetStatus FetchAllVisiableTupleInPage(HeapScanHandler *heapScanHandler, PageId pageId, OffsetNumber *offsetArray,
                                 uint16 arrayLen, uint16 &validLen)
{
    return heapScanHandler->FetchAllVisiableTupleInPage(pageId, offsetArray, arrayLen, validLen);
}

void DestroyHeapScanHandler(HeapScanHandler *heapScanHandler)
{
    if (likely(heapScanHandler)) {
        delete heapScanHandler;
    }
}

varlena *FetchLobValue(StorageRelation relation, ItemPointerData &ctid, Snapshot snapshot, AllocMemFunc allocMem)
{
    if (unlikely((relation == nullptr || ctid == INVALID_ITEM_POINTER))) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Invalid input for fetching lob value!"));
        storage_set_error(HEAP_ERROR_INPUT_PARAM_WRONG);
        return nullptr;
    }

    StorageReleasePanic(snapshot == nullptr, MODULE_HEAP, ErrMsg("Invalid snapshot for fetch lob value."));
    HeapScanHandler *heapScan = DstoreNew(thrd->GetTransactionMemoryContext())
        HeapScanHandler(g_storageInstance, thrd, relation, true);
    if (unlikely(heapScan == nullptr)) {
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Lob fetch new heap scan handler fail!"));
        return nullptr;
    }

    if (STORAGE_FUNC_FAIL(heapScan->Begin(snapshot))) {
        DestroyHeapScanHandler(heapScan);
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Begin scan failed!"));
        return nullptr;
    }
    HeapTuple *tuple = heapScan->FetchTuple(ctid);
    heapScan->EndFetch();
    heapScan->ReportHeapBufferReadStat(relation);
    DestroyHeapScanHandler(heapScan);
    if (unlikely(tuple == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Fetch tuple failed!"));
        return nullptr;
    }
    varlena *result = HeapLobHandler::FetchLobValueFromTuple(tuple, allocMem);
    DstorePfreeExt(tuple);
    return result;
}

HeapTuple *FetchTupleWithLob(StorageRelation relation, HeapTuple *tuple, Snapshot snapshot, AllocMemFunc allocMem)
{
    if (unlikely((relation == nullptr || tuple == nullptr))) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Invalid input for fetching lob tuple!"));
        storage_set_error(HEAP_ERROR_INPUT_PARAM_WRONG);
        return nullptr;
    }
    StorageReleasePanic(snapshot == nullptr, MODULE_HEAP, ErrMsg("Invalid snapshot for fetch tuple."));
    if (tuple->IsExternalMem()) {
        StorageAssert(allocMem != nullptr);
    }
    HeapLobHandler lobHandler(relation);
    return lobHandler.Fetch(tuple, snapshot, allocMem);
}

void RollbackLastRecordWhenConflict(StorageRelation relation, HeapTuple *tuple)
{
    if (unlikely(relation == nullptr || relation->tableSmgr == nullptr || tuple == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Invalid input for rollback last record!"));
        storage_set_error(HEAP_ERROR_INPUT_PARAM_WRONG);
        return;
    }

    HeapInsertHandler heapInsert(g_storageInstance, thrd, relation);
    heapInsert.RollbackLastRecordWhenConflict(*tuple->GetCtid());
    heapInsert.ReportHeapBufferReadStat(relation);
}

void DumpScanPage(DSTORE::HeapScanHandler *heapScanHandler, DSTORE::Datum &fileId, DSTORE::Datum &blockId,
                  DSTORE::Datum &data)
{
    heapScanHandler->DumpScanPage(fileId, blockId, data);
}

bool IsUsingMvccSnapshot(HeapScanHandler *heapScanHandler)
{
    return heapScanHandler->IsUsingMvccSnapshot();
}

}  // namespace HeapInterface
