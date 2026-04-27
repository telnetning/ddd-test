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
 * dstore_seq_interface.cpp
 *
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/dstore/src/seq/dstore_seq_interface.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "errorcode/dstore_heap_error_code.h"
#include "heap/dstore_seq_handler.h"
#include "heap/dstore_seq_interface.h"

namespace SeqInterface {
using namespace DSTORE;

RetStatus GetNextValue(StorageRelation relation, SeqContext *seqCtx, int128 *result)
{
    if (unlikely((relation == nullptr || relation->tableSmgr == nullptr || seqCtx == nullptr ||
                  seqCtx->ctid == INVALID_ITEM_POINTER))) {
        seqCtx->failureInfo.reason = HeapHandlerFailureReason::INVALID_PARAM;
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Invalid input for deleting!"));
        storage_set_error(HEAP_ERROR_INPUT_PARAM_WRONG);
        return DSTORE_FAIL;
    }

    SeqHandler *seqHandler =
        DstoreNew(DSTORE::g_dstoreCurrentMemoryContext) SeqHandler(g_storageInstance, thrd, relation);
    if (unlikely(seqHandler == nullptr)) {
        seqCtx->failureInfo.reason = HeapHandlerFailureReason::INIT_HANDLER_FAILED;
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Get sequence nextval failed."));
        return DSTORE_FAIL;
    }
    RetStatus status = seqHandler->GetNextValue(seqCtx, result);
    seqHandler->ReportHeapBufferReadStat(relation);
    delete seqHandler;

    return status;
}

DSTORE::RetStatus SetSeqValue(StorageRelation relation, DSTORE::SeqContext *seqCtx,
    bool isReset, int128 *newLastValue, bool isCalled)
{
    if (unlikely((relation == nullptr || relation->tableSmgr == nullptr || seqCtx == nullptr ||
                  seqCtx->ctid == INVALID_ITEM_POINTER))) {
        seqCtx->failureInfo.reason = HeapHandlerFailureReason::INVALID_PARAM;
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Invalid input for deleting!"));
        storage_set_error(HEAP_ERROR_INPUT_PARAM_WRONG);
        return DSTORE_FAIL;
    }

    SeqHandler *seqHandler =
        DstoreNew(DSTORE::g_dstoreCurrentMemoryContext) SeqHandler(g_storageInstance, thrd, relation);
    if (unlikely(seqHandler == nullptr)) {
        seqCtx->failureInfo.reason = HeapHandlerFailureReason::INIT_HANDLER_FAILED;
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Set sequence value failed"));
        return DSTORE_FAIL;
    }
    RetStatus status = seqHandler->SetSeqValue(seqCtx, isReset, newLastValue, isCalled);
    seqHandler->ReportHeapBufferReadStat(relation);
    delete seqHandler;

    return status;
}

DSTORE::RetStatus GetSeqTuple(StorageRelation relation, DSTORE::SeqContext *seqCtx)
{
    if (unlikely((relation == nullptr || relation->tableSmgr == nullptr || seqCtx == nullptr ||
                  seqCtx->ctid == INVALID_ITEM_POINTER))) {
        seqCtx->failureInfo.reason = HeapHandlerFailureReason::INVALID_PARAM;
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Invalid input for deleting!"));
        storage_set_error(HEAP_ERROR_INPUT_PARAM_WRONG);
        return DSTORE_FAIL;
    }

    SeqHandler *seqHandler =
        DstoreNew(DSTORE::g_dstoreCurrentMemoryContext) SeqHandler(g_storageInstance, thrd, relation);
    if (unlikely(seqHandler == nullptr)) {
        seqCtx->failureInfo.reason = HeapHandlerFailureReason::INIT_HANDLER_FAILED;
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Get sequence tuple failed."));
        return DSTORE_FAIL;
    }
    RetStatus status = seqHandler->CopySeqTuple(seqCtx);
    seqHandler->ReportHeapBufferReadStat(relation);
    delete seqHandler;

    return status;
}

}  // namespace SeqInterface
