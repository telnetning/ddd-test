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
 * dstore_sequence_handler.cpp
 *
 *
 *
 * IDENTIFICATION
 *        storage/src/heap/dstore_sequence_handler.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "errorcode/dstore_transaction_error_code.h"
#include "errorcode/dstore_lock_error_code.h"
#include "framework/dstore_thread.h"
#include "common/memory/dstore_mctx.h"
#include "common/log/dstore_log.h"
#include "buffer/dstore_buf.h"
#include "common/dstore_common_utils.h"
#include "transaction/dstore_transaction_mgr.h"
#include "wal/dstore_wal_write_context.h"
#include "logical_replication/dstore_decode_dict.h"
#include "errorcode/dstore_heap_error_code.h"
#include "heap/dstore_heap_undo_struct.h"
#include "heap/dstore_heap_wal_struct.h"
#include "heap/dstore_heap_perf_unit.h"
#include "heap/dstore_seq_handler.h"

namespace DSTORE {
RetStatus SeqHandler::GetNextValue(SeqContext *seqCtx, int128 *result)
{
    LatencyStat::Timer timer(&HeapPerfUnit::GetInstance().m_heapUpdateLatency);

    RetStatus retStatus = DSTORE_FAIL;
    StorageReleasePanic(seqCtx->ctid == INVALID_ITEM_POINTER, MODULE_HEAP,
        ErrMsg("ctid should not be invalid tuple in context"));
    AutoMemCxtSwitch autoMemCxtSwitch(m_thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_QUERY));

    if (unlikely(InitBufferDesc(seqCtx->ctid, LW_EXCLUSIVE))) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Failed to init buffer desc for sequence tuple, ctid({%hu, %u}, %hu)",
            seqCtx->ctid.GetFileId(), seqCtx->ctid.GetBlockNum(), seqCtx->ctid.GetOffset()));
        return DSTORE_FAIL;
    }

    do {
        if (STORAGE_FUNC_FAIL(DoGetNextValue(seqCtx, result))) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Get sequence nextval failed, ctid({%hu, %u}, %hu)",
                seqCtx->ctid.GetFileId(), seqCtx->ctid.GetBlockNum(), seqCtx->ctid.GetOffset()));
            break;
        }
        retStatus = DSTORE_SUCC;
    } while (false);

    TryToUnlockReleaseBufferDesc();
    return retStatus;
}

RetStatus SeqHandler::SetSeqValue(SeqContext *seqCtx, bool isReset, int128 *newLastValue, bool isCalled)
{
    LatencyStat::Timer timer(&HeapPerfUnit::GetInstance().m_heapUpdateLatency);

    RetStatus result = DSTORE_FAIL;
    StorageReleasePanic(seqCtx->ctid == INVALID_ITEM_POINTER, MODULE_HEAP,
        ErrMsg("ctid should not be invalid tuple in context"));
    AutoMemCxtSwitch autoMemCxtSwitch(m_thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_QUERY));

    if (unlikely(InitBufferDesc(seqCtx->ctid, LW_EXCLUSIVE))) {
        seqCtx->failureInfo.reason = HeapHandlerFailureReason::READ_BUFFER_FAILED;
        return DSTORE_FAIL;
    }

    StorageAssert(m_thrd->GetActiveTransaction() != nullptr);
    if (STORAGE_FUNC_FAIL(m_thrd->GetActiveTransaction()->AllocTransactionSlot())) {
        seqCtx->failureInfo.reason = HeapHandlerFailureReason::ALLOC_TRANS_SLOT_FAILED;
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Failed to prepare undo when updating tuple({%hu, %u}, %hu).",
            seqCtx->ctid.GetFileId(), seqCtx->ctid.GetBlockNum(),
            seqCtx->ctid.GetOffset()));
        return DSTORE_FAIL;
    }

    do {
        if (STORAGE_FUNC_FAIL(DoSetSeqValue(seqCtx, isReset, newLastValue, isCalled))) {
            ErrLog(DSTORE_WARNING, MODULE_HEAP, ErrMsg("Set sequence value failed,  ctid({%hu, %u}, %hu)",
                seqCtx->ctid.GetFileId(), seqCtx->ctid.GetBlockNum(),
                seqCtx->ctid.GetOffset()));
            break;
        }
        result = DSTORE_SUCC;
    } while (false);

    TryToUnlockReleaseBufferDesc();
    return result;
}

RetStatus SeqHandler::DoUpdateSeqBuffer(HeapPage *page, OffsetNumber offset, HeapTuple *seqTuple)
{
    char *seqTuplePtr = seqTuple->GetDiskTuple()->GetData();
    uint32 seqLen = seqTuple->GetDiskTupleSize() - seqTuple->GetDiskTuple()->GetHeaderSize();

    /* Mark the page is updated */
    AtomicWalWriterContext *walContext = m_thrd->m_walWriterContext;
    (void)m_bufMgr->MarkDirty(m_bufferDesc);

    /* Insert redo log */
    walContext->BeginAtomicWal(INVALID_XID);
    if (NeedWal()) {
        StorageReleasePanic(!walContext->HasAlreadyBegin(), MODULE_HEAP, ErrMsg("Wal has not atomic begin."));
        walContext->RememberPageNeedWal(m_bufferDesc);
        bool glsnChangedFlag = (page->GetWalId() != walContext->GetWalId());
        uint32 walDataSize = sizeof(WalRecordHeapForceUpdateTupleDataNoTrx) + seqLen;

        /* record WalRecordHeapForceUpdateTupleDataNoTrx */
        WalRecordHeapForceUpdateTupleDataNoTrx *walData =
            static_cast<WalRecordHeapForceUpdateTupleDataNoTrx *>(walContext->GetTempWalBuf());
        StorageReleasePanic(walDataSize > BLCKSZ, MODULE_HEAP, ErrMsg("The value of dstore palloc exceeds BLCKSZ, "
        "the walDataSize is (%u)", walDataSize));
        uint64 fileVersion = m_bufferDesc->GetFileVersion();
        walData->SetHeader({WAL_HEAP_FORCE_UPDATE_TUPLE_DATA_NO_TRX, walDataSize, m_bufferDesc->GetPageId(),
            page->GetWalId(), page->GetPlsn(), page->GetGlsn(), glsnChangedFlag, fileVersion},
            offset, seqLen);
        walData->SetData(seqTuplePtr, seqLen);
        walContext->PutNewWalRecord(walData);
    }

    m_bufMgr->UnlockAndRelease(m_bufferDesc);
    (void)walContext->EndAtomicWal();
    m_bufferDesc = nullptr;

    return DSTORE_SUCC;
}

RetStatus SeqHandler::CopySeqTuple(SeqContext *seqCtx)
{
    RetStatus retStatus = DSTORE_FAIL;
    StorageReleasePanic(seqCtx->ctid == INVALID_ITEM_POINTER, MODULE_HEAP,
        ErrMsg("ctid should not be invalid tuple in context"));
    AutoMemCxtSwitch autoMemCxtSwitch(m_thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_QUERY));

    if (unlikely(InitBufferDesc(seqCtx->ctid, LW_SHARED))) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Failed to init buffer desc for sequence tuple, ctid({%hu, %u}, %hu)",
            seqCtx->ctid.GetFileId(), seqCtx->ctid.GetBlockNum(), seqCtx->ctid.GetOffset()));
        return DSTORE_FAIL;
    }

    do {
        if (STORAGE_FUNC_FAIL(DoCopySeqTuple(seqCtx))) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Copy sequence tuple failed, ctid({%hu, %u}, %hu)",
                seqCtx->ctid.GetFileId(), seqCtx->ctid.GetBlockNum(), seqCtx->ctid.GetOffset()));
            break;
        }
        retStatus = DSTORE_SUCC;
    } while (false);

    TryToUnlockReleaseBufferDesc();
    return retStatus;
}

RetStatus SeqHandler::DoCopySeqTuple(SeqContext *seqCtx)
{
    HeapTuple seqTuple;
    OffsetNumber offset = seqCtx->ctid.GetOffset();
    HeapPage *page = static_cast<HeapPage *>(m_bufferDesc->GetPage());
    page->GetTuple(&seqTuple, offset);

    /* Copy tuple for returning */
    seqCtx->retTuple = page->CopyTuple(offset);
    return DSTORE_SUCC;
}

RetStatus SeqHandler::DoGetNextValue(SeqContext *seqCtx, int128 *result)
{
    HeapTuple seqTuple;
    OffsetNumber offset = seqCtx->ctid.GetOffset();
    HeapPage *page = static_cast<HeapPage *>(m_bufferDesc->GetPage());
    page->GetTuple(&seqTuple, offset);

    /* Calculation the last value */
    int128 cachedVal = 0;
    int128 resultVal = 0;
    FormData_gs_sequence *seqform = static_cast<FormData_gs_sequence *>(
        static_cast<void *>(seqTuple.GetStruct()));

    if (unlikely(AdvanceSeqValue(seqCtx, seqform, cachedVal, resultVal) == DSTORE_FAIL)) {
        /* Get old tuple for logging sequence info */
        seqCtx->retTuple = page->CopyTuple(offset);
        return DSTORE_FAIL;
    }

    /* Need return result for nextval function. */
    errno_t rc = memcpy_s(result, sizeof(int128), &resultVal, sizeof(int128));
    storage_securec_check(rc, "\0", "\0");

    /* Update last_value */
    rc = memcpy_s(&seqform->last_value, sizeof(int128), &cachedVal, sizeof(int128));
    storage_securec_check(rc, "\0", "\0");

    /* Update is_called */
    seqform->is_called = true;

    DoUpdateSeqBuffer(page, offset, &seqTuple);

    seqCtx->retTuple = page->CopyTuple(seqCtx->ctid.GetOffset());
    return DSTORE_SUCC;
}

RetStatus SeqHandler::DoSetSeqValue(SeqContext *seqCtx, bool isReset,
    int128 *newLastValue, bool isCalled)
{
    HeapTuple seqTuple;
    OffsetNumber offset = seqCtx->ctid.GetOffset();
    HeapPage *page = static_cast<HeapPage *>(m_bufferDesc->GetPage());
    page->GetTuple(&seqTuple, offset);

    FormData_gs_sequence *seqform = static_cast<FormData_gs_sequence *>(
        static_cast<void *>(seqTuple.GetStruct()));

    if (isReset) {
        /* Reset last_value = min_value */
        errno_t rc = memcpy_s(&seqform->last_value, sizeof(int128), &seqform->min_value, sizeof(int128));
        storage_securec_check(rc, "\0", "\0");

        /* Update is_called */
        seqform->is_called = false;
    } else {
        if (*newLastValue > seqform->max_value || *newLastValue < seqform->min_value) {
            /* Get old tuple for logging sequence info. */
            seqCtx->retTuple = page->CopyTuple(offset);
            seqCtx->failureInfo.reason = HeapHandlerFailureReason::SEQ_LAST_VALUE_INVALID;
            return DSTORE_FAIL;
        }
        StorageAssert(newLastValue != nullptr);
        errno_t rc = memcpy_s(&seqform->last_value, sizeof(int128), newLastValue, sizeof(int128));
        storage_securec_check(rc, "\0", "\0");

        /* Update is_called */
        seqform->is_called = isCalled;
    }

    DoUpdateSeqBuffer(page, offset, &seqTuple);

    seqCtx->retTuple = page->CopyTuple(seqCtx->ctid.GetOffset());
    return DSTORE_SUCC;
}

RetStatus SeqHandler::AdvanceSeqValue(SeqContext *seqCtx, const FormData_gs_sequence *seqform,
    int128 &cachedVal, int128 &resultVal)
{
    int128 fetchNum = seqform->cache_value;
    if (!seqform->is_called) {
        fetchNum--;   /* return last_value if not is_called */
        resultVal = seqform->last_value;
    }

    if (fetchNum <= 0) {
        cachedVal = seqform->last_value;
        return DSTORE_SUCC;
    }

    if (seqform->increment_by > 0) {
        return AdvanceAscSeqValue(seqCtx, seqform, fetchNum, cachedVal, resultVal);
    } else {
        return AdvanceDescSeqValue(seqCtx, seqform, fetchNum, cachedVal, resultVal);
    }

    return DSTORE_SUCC;
}

RetStatus SeqHandler::AdvanceAscSeqValue(SeqContext *seqCtx, const FormData_gs_sequence *seqform,
    int128 &fetchNum, int128 &cachedVal, int128 &resultVal)
{
    /* Step1: Get nextval value to returning */
    if (seqform->is_called) {
        if (seqform->increment_by <= seqform->max_value - seqform->last_value) {
            resultVal = seqform->last_value + seqform->increment_by;
        } else {
            if (seqform->is_cycled) {
                resultVal = seqform->min_value;
            } else {
                /* Exceed the max value */
                ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Exceeded the maximum value for sequence oid: %u.",
                    seqform->seq_oid));
                seqCtx->failureInfo.reason = HeapHandlerFailureReason::SEQ_NEXTVAL_EXCEED_MAXVALUE_FAILED;
                return DSTORE_FAIL;
            }
        }
    }

    /* Step2: Get cached value to update the seqtuple.last_value */
    int128 distance = seqform->increment_by * fetchNum;
    if (distance <= (seqform->max_value - seqform->last_value)) {
        cachedVal = distance + seqform->last_value;
        return DSTORE_SUCC;
    }

    /* Step3: ascending sequence try to cache value extend the max value */
    cachedVal = seqform->last_value;
    while (fetchNum > 0) {
        if ((seqform->max_value >= 0 && cachedVal > seqform->max_value - seqform->increment_by)
            || (seqform->max_value < 0 && cachedVal + seqform->increment_by > seqform->max_value)) {
            if (!seqform->is_cycled) {
                break;  /* stop fetching */
            }
            /* Exceed maxvalue */
            cachedVal = seqform->min_value;
        } else {
            cachedVal += seqform->increment_by;
        }
        fetchNum--;
    }

    return DSTORE_SUCC;
}

RetStatus SeqHandler::AdvanceDescSeqValue(SeqContext *seqCtx, const FormData_gs_sequence *seqform,
    int128 &fetchNum, int128 &cachedVal, int128 &resultVal)
{
    /* Step1: Get nextval value to returning */
    if (seqform->is_called) {
        if (seqform->increment_by >= seqform->min_value - seqform->last_value) {
            resultVal = seqform->last_value + seqform->increment_by;
        } else {
            if (seqform->is_cycled) {
                resultVal = seqform->max_value;
            } else {
                /* Extend the min value */
                ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Exceeded the minimum value for sequence oid: %u.",
                    seqform->seq_oid));
                seqCtx->failureInfo.reason = HeapHandlerFailureReason::SEQ_NEXTVAL_EXCEED_MINVALUE_FAILED;
                return DSTORE_FAIL;
            }
        }
    }

    /* Step2: Get cached value to update the seqtuple.last_value */
    int128 distance = seqform->increment_by * fetchNum;
    if (distance >= (seqform->min_value - seqform->last_value)) {
        cachedVal = distance + seqform->last_value;
        return DSTORE_SUCC;
    }
    /* Step3: descending sequence try to cache value extend the max value */
    cachedVal = seqform->last_value;
    while (fetchNum > 0) {
        /* Extend minvalue */
        if ((seqform->min_value < 0 && cachedVal < seqform->min_value - seqform->increment_by)
            || (seqform->min_value >= 0 && cachedVal + seqform->increment_by < seqform->min_value)) {
            if (!seqform->is_cycled) {
                break;  /* stop fetching */
            }
            cachedVal = seqform->max_value;
        } else {
            cachedVal += seqform->increment_by;
        }
        fetchNum--;
    }
    return DSTORE_SUCC;
}

RetStatus SeqHandler::InitBufferDesc(ItemPointerData ctid, LWLockMode lockMode)
{
    StorageAssert(ctid != INVALID_ITEM_POINTER);
    StorageAssert(ctid.GetOffset() != 0);

    PageId pageId = ctid.GetPageId();
    m_bufferDesc = m_bufMgr->Read(m_heapRel->m_pdbId, pageId, lockMode);
    if (unlikely(m_bufferDesc == INVALID_BUFFER_DESC)) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP,
            ErrMsg("Read page(%hu, %u) failed when init buffer desc for locking.", pageId.m_fileId, pageId.m_blockId));
        return DSTORE_FAIL;
    }
    CheckBufferedPage(m_bufferDesc->GetPage(), pageId);
    return DSTORE_SUCC;
}

SeqHandler::SeqHandler(StorageInstance *instance, ThreadContext *thread, StorageRelation heapRel,
    bool isLobOperation)
    : HeapHandler(instance, thread, heapRel, isLobOperation)
{
}

}  // namespace DSTORE
