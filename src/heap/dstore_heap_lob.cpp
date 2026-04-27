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
 * dstore_heap_lob.cpp
 *
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/dstore/src/heap/dstore_heap_lob.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "transaction/dstore_transaction.h"
#include "heap/dstore_heap_scan.h"
#include "heap/dstore_heap_insert.h"
#include "common/instrument/lz4/dstore_lz4.h"
#include "heap/dstore_heap_lob.h"

namespace DSTORE {
HeapLobHandler::HeapLobHandler(StorageRelation rel) : relation(rel)
{}

RetStatus HeapLobHandler::Insert(HeapTuple *tuple, CommandId cid)
{
    HeapInsertHandler heapInsert(g_storageInstance, thrd, relation, m_isLob);
    return DoInsert(tuple, &heapInsert, cid);
}

RetStatus HeapLobHandler::BatchInsert(HeapTuple **tuples, uint16_t nTuples, CommandId cid)
{
    HeapInsertHandler heapInsert(g_storageInstance, thrd, relation, m_isLob);
    for (uint16_t i = 0; i < nTuples; i++) {
        if (STORAGE_FUNC_FAIL(DoInsert(tuples[i], &heapInsert, cid))) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Do insert lob tuple %hu failed!", i));
            return DSTORE_FAIL;
        }
    }

    return DSTORE_SUCC;
}

HeapTuple *HeapLobHandler::Fetch(HeapTuple *tuple, Snapshot snapshot, AllocMemFunc allocMem)
{
    HeapTuple *retTuple = tuple;
    HeapScanHandler *heapScan = DstoreNew(thrd->GetTransactionMemoryContext())
        HeapScanHandler(g_storageInstance, thrd, relation, m_isLob);
    if (unlikely(heapScan == nullptr)) {
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Lob fetch new heap scan handler fail!"));
        return nullptr;
    }

    TupleWithLobContext lobCtx;
    if (STORAGE_FUNC_FAIL(lobCtx.Init(relation->tableSmgr->GetTupleDesc()))) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Lob fetch lobCtx init fail!"));
        delete heapScan;
        return nullptr;
    }

    if (STORAGE_FUNC_FAIL(heapScan->Begin(snapshot))) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Begin scan failed!"));
        delete heapScan;
        return nullptr;
    }
    RetStatus status = DSTORE_FAIL;
    do {
        /* Step 1: fetch lob tuples and calculate the total size of lob values */
        if (STORAGE_FUNC_FAIL(DoFetch(tuple, heapScan, lobCtx))) {
            ErrLog(DSTORE_DEBUG1, MODULE_HEAP, ErrMsg("Lob fetch DoFetch fail!"));
            break;
        }

        /* Step 2: create a new tuple with lob values appended */
        if (STORAGE_FUNC_FAIL(MakeTupleWithLob(tuple, retTuple, lobCtx, allocMem))) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Lob fetch MakeTupleWithLob fail!"));
            break;
        }

        status = DSTORE_SUCC;
    } while (false);

    lobCtx.Clean();
    heapScan->ReportHeapBufferReadStat(relation);
    heapScan->End();
    delete heapScan;

    if (STORAGE_FUNC_SUCC(status)) {
        return retTuple;
    }
    return nullptr;
}

RetStatus HeapLobHandler::Delete(HeapDeleteContext *context)
{
    HeapTuple *retTuple = context->returnTup;
    HeapDeleteContext delCtx;
    delCtx.needReturnTup = context->needReturnTup;
    delCtx.snapshot = context->snapshot;
    delCtx.cid = context->cid;

    HeapDeleteHandler heapDelete(g_storageInstance, thrd, relation, m_isLob);

    TupleWithLobContext lobCtx;
    if (STORAGE_FUNC_FAIL(lobCtx.Init(relation->tableSmgr->GetTupleDesc(), context->needReturnTup))) {
        context->failureInfo.reason = HeapHandlerFailureReason::LOB_CTX_INTI_FAILED;
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Lob delete lobCtx init fail!"));
        return DSTORE_FAIL;
    }
    RetStatus status = DSTORE_FAIL;
    do {
        /* Step 1: delete lob tuples and calculate the total size of lob values if context->needReturnTup */
        if (STORAGE_FUNC_FAIL(DoDelete(context->returnTup, &heapDelete, delCtx, lobCtx))) {
            context->failureInfo.SetReason(delCtx.failureInfo.reason);
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Lob delete DoDelete fail!"));
            break;
        }

        /* Step 2: create a new tuple with lob values appended if context->needReturnTup */
        if (lobCtx.needReturn) {
            if (STORAGE_FUNC_FAIL(MakeTupleWithLob(context->returnTup, retTuple, lobCtx))) {
                context->failureInfo.SetReason(HeapHandlerFailureReason::LOB_MAKE_TUPLE_FAILED);
                ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Lob delete MakeTupleWithLob fail!"));
                break;
            }

            if (context->returnTup != retTuple) {
                DstorePfreeExt(context->returnTup);
                context->returnTup = retTuple;
            }
        }

        status = DSTORE_SUCC;
    } while (false);

    lobCtx.Clean();

    return status;
}

RetStatus HeapLobHandler::Update(HeapUpdateContext *context)
{
    if (STORAGE_FUNC_FAIL(BeginUpdate(context))) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Lob update lock tuple fail!"));
        return DSTORE_FAIL;
    }
    HeapScanHandler *heapScan = DstoreNew(thrd->GetTransactionMemoryContext())
        HeapScanHandler(g_storageInstance, thrd, relation, m_isLob);
    if (unlikely(heapScan == nullptr)) {
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        context->failureInfo.reason = HeapHandlerFailureReason::INIT_HANDLER_FAILED;
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Lob update new heap scan handler fail!"));
        return DSTORE_FAIL;
    }

    if (STORAGE_FUNC_FAIL(heapScan->Begin(&context->snapshot))) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Begin scan failed!"));
        delete heapScan;
        return DSTORE_FAIL;
    }
    HeapTuple *oldTuple = heapScan->FetchTuple(context->oldCtid, false);
    if (unlikely(oldTuple == nullptr)) {
        context->failureInfo.reason = HeapHandlerFailureReason::LOB_FETCH_TUPLE_FAILED;
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Lob update FetchTuple fail!"));
        heapScan->End();
        delete heapScan;
        return DSTORE_FAIL;
    }

    HeapTuple *newTuple = context->newTuple;
    TupleDesc tupDesc = relation->tableSmgr->GetTupleDesc();
    TupleWithLobContext lobCtx;
    if (STORAGE_FUNC_FAIL(lobCtx.Init(tupDesc, true, &(context->failureInfo)))) {
        context->failureInfo.reason = HeapHandlerFailureReason::LOB_CTX_INTI_FAILED;
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Lob update lobCtx init fail!"));
        heapScan->End();
        delete heapScan;
        return DSTORE_FAIL;
    }
    lobCtx.snapshot = context->snapshot;
    lobCtx.cid = context->cid;
    RetStatus status = DSTORE_FAIL;
    do {
        /* Step 1: update lob tuples and calculate the total size of lob values */
        if (STORAGE_FUNC_FAIL(DoUpdate(newTuple, oldTuple, lobCtx))) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Lob update DoUpdate fail!"));
            if (lobCtx.failureInfo->reason == HeapHandlerFailureReason::UNKNOWN) {
                context->failureInfo.SetReason(HeapHandlerFailureReason::LOB_UPDATE_FAILED);
            } else {
                context->failureInfo.SetReason(lobCtx.failureInfo->reason);
            }
            break;
        }

        /* Step 2: create a new tuple with lob values appended and return for ex situ update situation */
        HeapTuple *oldTupleWithLob = oldTuple;
        if (STORAGE_FUNC_FAIL(MakeTupleWithLob(oldTuple, oldTupleWithLob, lobCtx))) {
            context->failureInfo.SetReason(HeapHandlerFailureReason::LOB_MAKE_TUPLE_FAILED);
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Lob update MakeTupleWithLob fail!"));
            break;
        }

        if (oldTuple != oldTupleWithLob) {
            DstorePfreeExt(oldTuple);
        }
        context->retOldTuple = oldTupleWithLob;

        status = DSTORE_SUCC;
    } while (false);

    heapScan->End();
    delete heapScan;
    lobCtx.Clean();

    return status;
}


/*
 * Compress() will create space and copy compressed value to result,
 * so need to call DstorePfree(result).
 */
varlena *HeapLobHandler::Compress(varlena *value)
{
    StorageAssert(!VarAttIs1BE(value));
    StorageAssert(!VarAttIs4BC(value));

    varlena *result = nullptr;
    uint32 valSize = DSTORE_VARSIZE_ANY_EXHDR(value);

    int maxCompressedSize = LZ4_compressBound(valSize);
    result = static_cast<varlena *>(DstorePalloc0(VARHDRSZ_COMPRESSED + maxCompressedSize));
    if (unlikely(result == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("DstorePalloc0 fail size(%lu) when lob compress.",
            VARHDRSZ_COMPRESSED + maxCompressedSize));
        return nullptr;
    }

    int actualCompressedSize = LZ4_compress_default(
        VarDataAny(value), VarData4BC(result), valSize, maxCompressedSize);
    if ((uint32)actualCompressedSize + (uint32)VARHDRSZ_COMPRESSED >= valSize + VARHDRSZ) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("The compressed size(%hu) is larger then origin size(%hu).",
            (uint32)actualCompressedSize + (uint32)VARHDRSZ_COMPRESSED, valSize + VARHDRSZ));
        DstorePfree(result);
        return nullptr;
    }

    STATIC_CAST_PTR_TYPE(result, varattrib_4b *)->va_compressed.va_rawsize = valSize;
    DstoreSetVarSizeCompressed(result, VARHDRSZ_COMPRESSED + actualCompressedSize);
    return result;
}

/*
 * Decompress() will create space and copy decompressed value to result,
 * so need to call DstorePfree(result).
 */
varlena *HeapLobHandler::Decompress(varlena *value, AllocMemFunc allocMem)
{
    StorageAssert(VarAttIs4BC(value));

    varattrib_4b *input = STATIC_CAST_PTR_TYPE(value, varattrib_4b *);
    uint32 rawSize = input->va_compressed.va_rawsize;
    varlena *result = static_cast<varlena *>((allocMem != nullptr) ? allocMem(VARHDRSZ + rawSize)
                                                                   : DstorePalloc0(VARHDRSZ + rawSize));
    if (unlikely(result == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP,
            ErrMsg("DstorePalloc0 fail size(%u) when lob decompress.", VARHDRSZ + rawSize));
        return nullptr;
    }

    if (DstoreVarSize4B(input) < VARHDRSZ_COMPRESSED) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP,
            ErrMsg("The input size(%u) is small then VARHDRSZ_COMPRESSED(%lu)",
            DstoreVarSize4B(input), VARHDRSZ_COMPRESSED));
        return nullptr;
    }
    int decompressedSize = LZ4_decompress_safe(
        VarData4BC(input),
        VarData4B(result),
        DstoreVarSize4B(input) - VARHDRSZ_COMPRESSED,
        rawSize);
    StorageReleasePanic(decompressedSize < 0, MODULE_HEAP, ErrMsg("Failed to decompress!"));
    StorageReleasePanic(
        decompressedSize != static_cast<int>(rawSize),
        MODULE_HEAP,
        ErrMsg("Decompressed size is different from the original!")
    );
    DstoreSetVarSize4B(result, rawSize + VARHDRSZ);
    return result;
}

RetStatus HeapLobHandler::DoInsert(HeapTuple *tuple, HeapInsertHandler *heapInsert, CommandId cid)
{
    TupleDesc tupDesc = relation->tableSmgr->GetTupleDesc();
    TupleDesc lobTupDesc = relation->lobTableSmgr->GetTupleDesc();

    bool isNull = false;
    int numAttrs = tupDesc->natts;
    FormData_pg_attribute **attrs = tupDesc->attrs;
    char *lobValues = STATIC_CAST_PTR_TYPE(tuple->GetDiskTuple(), char *) + tuple->GetDiskTupleSize();

    for (int i = 0; i < numAttrs; i++) {
        if (!AttIsLob(attrs[i])) {
            continue;
        }

        void *tp = DatumGetPointer(tuple->GetAttr(i + 1, tupDesc, &isNull, true));
        if (isNull || !DstoreVarAttIsExternalDlob(tp)) {
            continue;
        }

        VarattLobLocator *lobLocator = STATIC_CAST_PTR_TYPE(VarData1BE(tp), VarattLobLocator *);
        lobLocator->relid = (tuple->GetLobTargetOid() != DSTORE_INVALID_OID) ?
            tuple->GetLobTargetOid() : relation->relOid;
        Datum lobValueDatum = PointerGetDatum(lobValues);
        varlena *compressedLobValue = Compress(STATIC_CAST_PTR_TYPE(lobValues, varlena *));
        Datum compressedLobValueDatum = PointerGetDatum(compressedLobValue);
        bool compressed = (compressedLobValue != nullptr);
        if (compressed) {
            lobLocator->extsize = static_cast<int32_t>(DstoreVarSize4B(DatumGetPointer(compressedLobValueDatum)));
        }
        HeapTuple *lobTuple = TupleInterface::FormHeapTuple(
            lobTupDesc,
            compressed ? &(compressedLobValueDatum) : &(lobValueDatum),
            &(isNull),
            nullptr);
        DstorePfreeExt(compressedLobValue);
        if (unlikely(STORAGE_VAR_NULL(lobTuple))) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Form lob tuple failed!"));
            return DSTORE_FAIL;
        }

        HeapInsertContext insertCtx;
        insertCtx.heapTuple = lobTuple;
        insertCtx.ctid = INVALID_ITEM_POINTER;
        insertCtx.cid = cid;

        if (STORAGE_FUNC_FAIL(heapInsert->Insert(&insertCtx))) {
            DstorePfreeExt(lobTuple);
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Insert lob tuple failed!"));
            return DSTORE_FAIL;
        }

        DstorePfreeExt(lobTuple);
        lobLocator->ctid = insertCtx.ctid.m_placeHolder;
        lobValues += DstoreVarSize4B(lobValues);
    }
    tuple->GetDiskTuple()->ResetHasInlineLobValue();

    return DSTORE_SUCC;
}

RetStatus HeapLobHandler::DoFetch(HeapTuple *tuple, HeapScanHandler *heapScan, TupleWithLobContext &lobCtx)
{
    StorageAssert(heapScan != nullptr);
    StorageAssert(lobCtx.lobTuples != nullptr);

    bool isNull = true;
    for (int i = 0; i < lobCtx.tupDesc->natts; i++) {
        if (!AttIsLob(lobCtx.tupDesc->attrs[i])) {
            continue;
        }

        void *tp = DatumGetPointer(tuple->GetAttr(i + 1, lobCtx.tupDesc, &isNull, true));
        if (isNull || !DstoreVarAttIsExternalDlob(tp)) {
            continue;
        }

        lobCtx.lobIsValids[i] = true;
        VarattLobLocator *lobLocator = STATIC_CAST_PTR_TYPE(VarData1BE(tp), VarattLobLocator *);
        ItemPointerData ctid(lobLocator->ctid);
        lobCtx.lobTuples[i] = heapScan->FetchTuple(ctid);
        if (unlikely(lobCtx.lobTuples[i] == nullptr)) {
            ItemPointerData lobctid(lobLocator->ctid);
            ErrLog(DSTORE_DEBUG1, MODULE_HEAP, ErrMsg("Lob do fetch fetch tuple fail, ctid({%hu, %u}, %hu)!",
                lobctid.GetFileId(), lobctid.GetBlockNum(), lobctid.GetOffset()));
            return DSTORE_FAIL;
        }
        lobLocator->ctid = INVALID_ITEM_POINTER.m_placeHolder;

        uint32 lobValueSize = 0;
        if (VarAttIs4BC(lobCtx.lobTuples[i]->GetValues())) {
            lobValueSize = VARHDRSZ + DstoreVarRawSize4BC(lobCtx.lobTuples[i]->GetValues());
        } else {
            lobValueSize = lobCtx.lobTuples[i]->GetDiskTupleSize() -
                           lobCtx.lobTuples[i]->GetDiskTuple()->GetValuesOffset();
        }
        lobCtx.lobTotalSize += lobValueSize;
    }

    return DSTORE_SUCC;
}

RetStatus HeapLobHandler::DoDelete(HeapTuple *tuple, HeapDeleteHandler *heapDelete,
                                   HeapDeleteContext &delCtx, TupleWithLobContext &lobCtx)
{
    StorageAssert(heapDelete != nullptr);
    if (lobCtx.needReturn) {
        StorageAssert(lobCtx.lobTuples != nullptr);
    }

    bool isNull = false;
    for (int i = 0; i < lobCtx.tupDesc->natts; i++) {
        if (!AttIsLob(lobCtx.tupDesc->attrs[i])) {
            continue;
        }
        /* when there is lob att, the hasvariable is true */
        tuple->m_diskTuple->SetHasVariable();
        void *tp = DatumGetPointer(tuple->GetAttr(i + 1, lobCtx.tupDesc, &isNull, true));
        if (isNull || !DstoreVarAttIsExternalDlob(tp)) {
            continue;
        }

        VarattLobLocator *lobLocator = STATIC_CAST_PTR_TYPE(VarData1BE(tp), VarattLobLocator *);
        delCtx.ctid = ItemPointerData(lobLocator->ctid);
        if (STORAGE_FUNC_FAIL(heapDelete->Delete(&delCtx))) {
            ItemPointerData ctid(lobLocator->ctid);
            ErrLog(DSTORE_DEBUG1, MODULE_HEAP, ErrMsg("Lob do delete delete tuple fail, ctid({%hu, %u}, %hu)!",
                ctid.GetFileId(), ctid.GetBlockNum(), ctid.GetOffset()));
            return DSTORE_FAIL;
        }

        if (lobCtx.needReturn) {
            uint32 lobValueSize = 0;
            lobCtx.lobIsValids[i] = true;
            lobCtx.lobTuples[i] = delCtx.returnTup;
            delCtx.returnTup = nullptr;
            StorageAssert(lobCtx.lobTuples[i] != nullptr);
            if (VarAttIs4BC(lobCtx.lobTuples[i]->GetValues())) {
                lobValueSize = VARHDRSZ + DstoreVarRawSize4BC(lobCtx.lobTuples[i]->GetValues());
            } else {
                lobValueSize = lobCtx.lobTuples[i]->GetDiskTupleSize() -
                               lobCtx.lobTuples[i]->GetDiskTuple()->GetValuesOffset();
            }
            lobCtx.lobTotalSize += lobValueSize;
            lobLocator->ctid = INVALID_ITEM_POINTER.m_placeHolder;
        }
    }

    return DSTORE_SUCC;
}

RetStatus HeapLobHandler::BeginUpdate(HeapUpdateContext *context)
{
    HeapLockTupHandler heapLockTup(g_storageInstance, thrd, relation);
    HeapLockTupleContext lockContext;
    lockContext.needRetTup = false;
    lockContext.allowLockSelf = false;
    lockContext.snapshot = context->snapshot;
    lockContext.ctid = context->oldCtid;
    lockContext.executedEpq = context->executedEpq;
    lockContext.retTup = nullptr;
    if (STORAGE_FUNC_FAIL(heapLockTup.LockUnchangedTuple(&lockContext))) {
        context->failureInfo = lockContext.failureInfo;
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus HeapLobHandler::DoUpdate(HeapTuple *newTuple, HeapTuple *oldTuple,
                                   TupleWithLobContext &lobCtx)
{
    RetStatus status;
    void *tp = nullptr;
    bool oldLobIsNull = false;
    bool newLobIsValid = false;
    DoUpdateContext doUpdateCtx;
    doUpdateCtx.lobTableSmgr = relation->lobTableSmgr;
    doUpdateCtx.newLobValues = STATIC_CAST_PTR_TYPE(newTuple->GetDiskTuple(), char *) + newTuple->GetDiskTupleSize();
    for (int i = 0; i < lobCtx.tupDesc->natts; i++) {
        if (!AttIsLob(lobCtx.tupDesc->attrs[i])) {
            continue;
        }
        doUpdateCtx.i = i;

        tp = DatumGetPointer(newTuple->GetAttr(i + 1, lobCtx.tupDesc, &doUpdateCtx.newLobIsNull, true));
        if (!doUpdateCtx.newLobIsNull && DstoreVarAttIsExternalDlob(tp)) {
            doUpdateCtx.newLobLocator = STATIC_CAST_PTR_TYPE(VarData1BE(tp), VarattLobLocator *);
            newLobIsValid = true;
        } else {
            newLobIsValid = false;
        }

        tp = DatumGetPointer(oldTuple->GetAttr(i + 1, lobCtx.tupDesc, &oldLobIsNull, true));
        if (!oldLobIsNull && DstoreVarAttIsExternalDlob(tp)) {
            doUpdateCtx.oldLobLocator = STATIC_CAST_PTR_TYPE(VarData1BE(tp), VarattLobLocator *);
            lobCtx.lobIsValids[i] = true;
        } else {
            lobCtx.lobIsValids[i] = false;
        }

        if (newLobIsValid && lobCtx.lobIsValids[i]) {
            /* New LOB locator is valid means the update doesn't change this lob value */
            if (doUpdateCtx.newLobLocator->ctid != INVALID_ITEM_POINTER.m_placeHolder) {
                continue;
            }
            status = DoInsertUpdate(doUpdateCtx, lobCtx.cid);
            if (STORAGE_FUNC_SUCC(status)) {
                status = DoDeleteUpdate(doUpdateCtx, lobCtx);
            }
        } else if (newLobIsValid && !lobCtx.lobIsValids[i]) {
            status = DoInsertUpdate(doUpdateCtx, lobCtx.cid);
        } else if (!newLobIsValid && lobCtx.lobIsValids[i]) {
            status = DoDeleteUpdate(doUpdateCtx, lobCtx);
        } else {
            continue;
        }

        if (STORAGE_FUNC_FAIL(status)) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Update lob tuple[%d] failed!", i));
            for (int j = 0; j <= i; j++) {
                DstorePfreeExt(lobCtx.lobTuples[j]);
            }
            DstorePfreeExt(lobCtx.lobTuples);
            DstorePfreeExt(lobCtx.lobIsValids);
            return DSTORE_FAIL;
        }
    }
    newTuple->GetDiskTuple()->ResetHasInlineLobValue();
    return DSTORE_SUCC;
}

RetStatus HeapLobHandler::DoInsertUpdate(DoUpdateContext &doUpdateCtx, CommandId cid)
{
    StorageAssert(doUpdateCtx.newLobLocator != nullptr);

    doUpdateCtx.newLobLocator->relid = relation->relOid;
    Datum newLobValueDatum = PointerGetDatum(doUpdateCtx.newLobValues);
    HeapInsertContext insertCtx;
    varlena *compressedLobValue = Compress(STATIC_CAST_PTR_TYPE(doUpdateCtx.newLobValues, varlena *));
    Datum compressedLobValueDatum = PointerGetDatum(compressedLobValue);
    bool compressed = (compressedLobValue != nullptr);
    if (compressed) {
        doUpdateCtx.newLobLocator->extsize =
            static_cast<int32_t>(DstoreVarSize4B(DatumGetPointer(compressedLobValueDatum)));
    }
    HeapTuple *newLobTuple = TupleInterface::FormHeapTuple(
        doUpdateCtx.lobTableSmgr->GetTupleDesc(),
        compressed ? &(compressedLobValueDatum) : &(newLobValueDatum),
        &(doUpdateCtx.newLobIsNull),
        nullptr);
    DstorePfreeExt(compressedLobValue);
    if (unlikely(newLobTuple == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("newLobTuple is nullptr."));
        return DSTORE_FAIL;
    }
    insertCtx.heapTuple = newLobTuple;
    insertCtx.ctid = INVALID_ITEM_POINTER;
    insertCtx.cid = cid;

    HeapInsertHandler heapInsert(g_storageInstance, thrd, relation, HeapLobHandler::m_isLob);
    RetStatus status = heapInsert.Insert(&insertCtx);
    DstorePfreeExt(newLobTuple);

    if (STORAGE_FUNC_SUCC(status)) {
        doUpdateCtx.newLobLocator->ctid = insertCtx.ctid.m_placeHolder;
        doUpdateCtx.newLobValues += DstoreVarSize4B(doUpdateCtx.newLobValues);
    }

    return status;
}

RetStatus HeapLobHandler::DoDeleteUpdate(const DoUpdateContext &doUpdateCtx, TupleWithLobContext &lobCtx)
{
    StorageAssert(doUpdateCtx.oldLobLocator != nullptr);

    HeapDeleteContext deleteCtx;
    deleteCtx.ctid = ItemPointerData(doUpdateCtx.oldLobLocator->ctid);
    deleteCtx.needReturnTup = lobCtx.needReturn;
    deleteCtx.snapshot = lobCtx.snapshot;
    deleteCtx.cid = lobCtx.cid;

    HeapDeleteHandler heapDelete(g_storageInstance, thrd, relation, HeapLobHandler::m_isLob);
    RetStatus status = heapDelete.Delete(&deleteCtx);
    if (STORAGE_FUNC_SUCC(status) && lobCtx.needReturn) {
        lobCtx.lobTuples[doUpdateCtx.i] = deleteCtx.returnTup;
        uint32 oldLobValueSize = 0;
        if (VarAttIs4BC(lobCtx.lobTuples[doUpdateCtx.i]->GetValues())) {
            oldLobValueSize = VARHDRSZ + DstoreVarRawSize4BC(lobCtx.lobTuples[doUpdateCtx.i]->GetValues());
        } else {
            oldLobValueSize = lobCtx.lobTuples[doUpdateCtx.i]->GetDiskTupleSize() -
                              lobCtx.lobTuples[doUpdateCtx.i]->GetDiskTuple()->GetValuesOffset();
        }
        lobCtx.lobTotalSize += oldLobValueSize;
        doUpdateCtx.oldLobLocator->ctid = INVALID_ITEM_POINTER.m_placeHolder;
    }

    if (STORAGE_FUNC_FAIL(status)) {
        lobCtx.failureInfo->SetCid(deleteCtx.failureInfo.cid);
        lobCtx.failureInfo->SetCtid(deleteCtx.failureInfo.ctid);
        lobCtx.failureInfo->SetReason(deleteCtx.failureInfo.reason);
    }

    return status;
}

RetStatus HeapLobHandler::MakeTupleWithLob(HeapTuple *tuple, HeapTuple *&tupleWithLob, TupleWithLobContext &lobCtx,
                                           AllocMemFunc allocMem)
{
    if (lobCtx.lobTotalSize == 0) {
        return DSTORE_SUCC;
    }

    AutoMemCxtSwitch autoMemCxtSwitch(thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_QUERY));

    StorageAssert(lobCtx.lobTuples != nullptr);
    char *lobValues = nullptr;

    /* Step 1: copy tuple to tupleWithLob */
    tupleWithLob = static_cast<HeapTuple *>(
        (allocMem != nullptr) ? allocMem(tuple->m_head.len + sizeof(HeapTuple) + lobCtx.lobTotalSize)
                              : DstorePalloc(tuple->m_head.len + sizeof(HeapTuple) + lobCtx.lobTotalSize));
    if (unlikely(tupleWithLob == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("DstorePalloc fail size(%u) for making lob tuple.",
            static_cast<uint32>(tuple->m_head.len + sizeof(HeapTuple) + lobCtx.lobTotalSize)));
        return DSTORE_FAIL;
    }
    if (allocMem != nullptr) {
        tupleWithLob->SetExternalMem();
    } else {
        tupleWithLob->SetInternalMem();
    }
    tupleWithLob->m_diskTuple = STATIC_CAST_PTR_TYPE(tupleWithLob + 1, HeapDiskTuple *);
    tupleWithLob->m_head = tuple->m_head;
    tupleWithLob->m_head.type = DSTORE_TUPLE_TYPE;

    errno_t rc = memcpy_s(static_cast<void *>(tupleWithLob->m_diskTuple),
                          tuple->m_head.len,
                          static_cast<void *>(tuple->GetDiskTuple()),
                          tuple->m_head.len);
    storage_securec_check(rc, "\0", "\0");

    lobValues = STATIC_CAST_PTR_TYPE(tupleWithLob->m_diskTuple, char *) + tupleWithLob->GetDiskTupleSize();
    if (unlikely(lobValues == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("lobValues is null."));
        return DSTORE_FAIL;
    }

    /* Step 2: append lob values to tupleWithLob */
    uint32 lobValueSize = 0;
    for (int i = 0; i < lobCtx.tupDesc->natts; i++) {
        if (lobCtx.lobIsValids[i]) {
            StorageAssert(lobCtx.lobTuples[i] != nullptr);
            varlena *lobValue = STATIC_CAST_PTR_TYPE(lobCtx.lobTuples[i]->GetDiskTuple()->GetValues(), varlena *);
            if (VarAttIs4BC(lobValue)) {
                varlena *decompresedLobValue = Decompress(lobValue);
                lobValueSize = DstoreVarSize4B(decompresedLobValue);
                rc = memcpy_s(lobValues, lobValueSize, decompresedLobValue, lobValueSize);
                DstorePfree(decompresedLobValue);
            } else {
                lobValueSize = DstoreVarSize4B(lobValue);
                rc = memcpy_s(lobValues, lobValueSize, lobValue, lobValueSize);
            }
            storage_securec_check(rc, "\0", "\0");
            lobValues += lobValueSize;
        }
    }
    tupleWithLob->GetDiskTuple()->SetHasInlineLobValue();
    return DSTORE_SUCC;
}

/*
 * FetchLobValueFromTuple() will create space and copy lob to result,
 * so need to call DstorePfree(result) is allocmem is nullptr.
 */
varlena *HeapLobHandler::FetchLobValueFromTuple(HeapTuple *tuple, AllocMemFunc allocMem)
{
    StorageAssert(tuple == nullptr || tuple->GetDiskTupleSize() != 0);
    varlena *result = nullptr;
    varlena *lobValue = STATIC_CAST_PTR_TYPE(tuple->GetDiskTuple()->GetValues(), varlena *);
    if (VarAttIs4BC(lobValue)) {
        result = Decompress(lobValue, allocMem);
    } else {
        uint32 lobValueSize = tuple->GetDiskTupleSize() - tuple->GetValuesOffset();
        result =
            static_cast<varlena *>((allocMem != nullptr) ? allocMem(lobValueSize) : DstorePalloc(lobValueSize));
        if (unlikely(result == nullptr)) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("DstorePalloc fail size(%u).", lobValueSize));
            return nullptr;
        }
        errno_t rc = memcpy_s(result, lobValueSize, lobValue, lobValueSize);
        storage_securec_check(rc, "\0", "\0");
    }
    return result;
}
}  // namespace DSTORE
