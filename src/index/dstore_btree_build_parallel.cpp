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
 * dstore_btree_build_parallel.cpp
 *	  Implementation of index btree build in parallel.
 *
 * IDENTIFICATION
 *    src/gausskernel/dstore/include/index/dstore_btree_build_parallel.cpp
 *
 * -------------------------------------------------------------------------
 */

#include "index/dstore_btree_build_parallel.h"
#include "transaction/dstore_transaction.h"
#include "heap/dstore_heap_scan.h"
#include "index/dstore_btree_build.h"

namespace DSTORE {
ParallelBtreeBuildWorker::ParallelBtreeBuildWorker(TuplesortMgr *tupleSortMgr, Transaction *mainThrdTransaction,
                                                   IndexBuildInfo *indexBuildInfo, int workMem, ScanKey scanKey,
                                                   DstoreMemoryContext parallelCxt, TuplesortMgr *mainTupleSortMgr,
                                                   int workerId)
    : m_workerThrd(nullptr),
      m_taskRes(DSTORE_FAIL),
      m_tupleSortMgr(tupleSortMgr),
      m_mainTupleSortMgr(mainTupleSortMgr),
      m_controller(nullptr),
      m_mainThrdTransaction(mainThrdTransaction),
      m_heapRel(nullptr),
      m_numIndexTuples(0),
      m_numHeapTuples(0),
      m_indexBuildInfo(indexBuildInfo),
      m_workMem(workMem),
      m_scanKey(scanKey),
      m_parallelCxt(parallelCxt),
      m_thrd(nullptr),
      m_threadId(INVALID_THREAD_ID),
      m_workerState(WorkerState::NORMAL_STATE),
      m_workerId(workerId)
{}

void ParallelBtreeBuildWorker::Destroy()
{
    if (m_workerThrd != nullptr && m_workerThrd->joinable()) {
        m_workerThrd->join();
        delete m_workerThrd;
        m_workerThrd = nullptr;
    }
    if (m_tupleSortMgr) {
        m_tupleSortMgr->Destroy();
        m_tupleSortMgr->Clear();
        m_tupleSortMgr = nullptr;
    }
}

RetStatus ParallelBtreeBuildWorker::Run(PdbId pdbId, StorageRelation heapRel, Oid tableOid,
                                        ParallelWorkController *controller, bool isLastPartition)
{
    if (controller == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Unexpected nullptr for Run."));
        return DSTORE_FAIL;
    }
    m_taskRes = DSTORE_FAIL;
    m_controller = controller;
    m_workerThrd = new std::thread(&ParallelBtreeBuildWorker::ScanWorkerMain, this, pdbId, heapRel, tableOid,
        isLastPartition);
    return STORAGE_VAR_NULL(m_workerThrd) ? DSTORE_FAIL : DSTORE_SUCC;
}

RetStatus ParallelBtreeBuildWorker::GetWorkerResult()
{
    if (likely(m_workerThrd != nullptr && m_workerThrd->joinable())) {
        m_workerThrd->join();
    }
    delete m_workerThrd;
    m_workerThrd = nullptr;
    return m_taskRes;
}

void ParallelBtreeBuildWorker::ScanWorkerMain(PdbId pdbId, StorageRelation heapRel, Oid tableOid, bool isLastPartition)
{
    InitSignalMask();
    thrd = nullptr;
    void *sqlThrd = nullptr;
    SQLThrdInitCtx context = {pdbId, "ParallelBuilder", InternalThreadType::THREAD_INDEX_PARALLEL_BUILD, &sqlThrd,
                              false};
    bool initResult = g_storageInstance->InitSQLThreadContext(&context);
    if (!initResult) {
        SetWorkerState(WorkerState::INIT_SQL_THREAD_CONTEXT_FAIL_STATE);
        m_taskRes = DSTORE_FAIL;
        m_thrd = nullptr;
        return;
    }
    if (thrd == nullptr) {
        g_storageInstance->CreateThreadAndRegister(pdbId, false, "ParallelBuilder", true,
                                                   ThreadMemoryLevel::THREADMEM_MEDIUM_PRIORITY);
    }
    thrd->SetNeedCommBuffer(true);
    thrd->RefreshWorkingVersionNum();
    m_thrd = thrd;
    if (thrd == nullptr || thrd->GetCore() == nullptr) {
        m_taskRes = DSTORE_FAIL;
        return;
    }
    m_threadId = thrd->GetThreadId();

    HeapScanHandler *scanHandler = nullptr;
    Datum values[INDEX_MAX_KEY_NUM];
    bool isNulls[INDEX_MAX_KEY_NUM];
    void *exprCxt = nullptr;

    /* ParallelBtreeBuildWorker can only abort in dstore. */
    std::unique_lock<std::mutex> *controllerLock = m_controller->LockController();
    m_controller->m_workerCanAbort[m_workerId] = true;
    m_controller->UnlockController(controllerLock);

    Transaction *txn = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(txn)) {
#ifdef UT
        (void)thrd->InitTransactionRuntime(pdbId, nullptr, nullptr);
        txn = thrd->GetActiveTransaction();
        txn->Start();
#else
        /* InitSQLThreadContext will start a new transaction, no need start again here. */
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("No active transaction found in current thread."));
        m_taskRes = DSTORE_FAIL;
        goto END;
#endif
    }

    m_tupleSortMgr->PrepareParallelSortWorker(m_indexBuildInfo->baseInfo, m_workMem, m_scanKey, m_parallelCxt);
    txn->SetTransactionSnapshotCsn(m_mainThrdTransaction->GetSnapshotCsn());
    txn->SetTransactionSnapshotCid(m_mainThrdTransaction->GetSnapshotCid());
    txn->SetCurrentXid(m_mainThrdTransaction->GetCurrentXid());

    m_taskRes = DSTORE_FAIL;
    if (m_indexBuildInfo->baseInfo.exprInitCallback) {
        exprCxt = BtreeBuild::ExprInit(m_indexBuildInfo->baseInfo.exprInitCallback);
        if (unlikely(exprCxt == nullptr)) {
            storage_set_error(INDEX_ERROR_EXPRESSION_VALUE_ERR);
            goto END;
        }
    }
    scanHandler = DstoreNew(g_dstoreCurrentMemoryContext) HeapScanHandler(g_storageInstance, thrd, heapRel);
    if (unlikely(scanHandler == nullptr)) {
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        goto END;
    }

    if (STORAGE_FUNC_FAIL(scanHandler->Begin(txn->GetSnapshot()))) {
        delete scanHandler;
        scanHandler = nullptr;
        goto END;
    }
    scanHandler->SetParallelController(m_controller, m_workerId);
    HeapTuple *heapTuple;

    while ((heapTuple = scanHandler->SeqScan()) != nullptr) {
        heapTuple->SetTableOid(tableOid);
        DstoreMemoryContext oldContext = DstoreMemoryContextSwitchTo(m_tupleSortMgr->GetTupleMcxt());
        uint32 tupleSize = BLCKSZ;
        if (m_indexBuildInfo->baseInfo.exprCallback) {
            if (STORAGE_FUNC_FAIL(BtreeBuild::GetExprValue(m_indexBuildInfo->baseInfo.exprCallback, heapTuple, values,
                                                           isNulls, exprCxt))) {
                storage_set_error(INDEX_ERROR_EXPRESSION_VALUE_ERR);
                goto END;
            }
        }
        IndexTuple *indexTuple =
            IndexTuple::FormTuple(heapTuple, m_indexBuildInfo, values, isNulls, &tupleSize);
        (void)DstoreMemoryContextSwitchTo(oldContext);
        if (unlikely(tupleSize > MAX_INDEXTUPLE_SIZE_ON_BTREE_PAGE)) {
            DstorePfreeExt(indexTuple);
            controllerLock = m_controller->LockController();
            m_indexBuildInfo->baseInfo.extraInfo = UInt32GetDatum(tupleSize);
            m_controller->UnlockController(controllerLock);
        }
        if (unlikely(indexTuple == nullptr)) {
            goto END;
        }
        indexTuple->SetHeapCtid(heapTuple->GetCtid());
        if (STORAGE_FUNC_FAIL(m_tupleSortMgr->PutIndexTuple(indexTuple))) {
            goto END;
        }
        m_numHeapTuples++;
        m_numIndexTuples++;
    }
    if (isLastPartition) {
        if (STORAGE_FUNC_FAIL(m_tupleSortMgr->PerformSortTuple())) {
            goto END;
        }
    }
    m_taskRes = DSTORE_SUCC;
    StorageClearError();
END:
    SetWorkerState(m_taskRes == DSTORE_SUCC ?
        WorkerState::FINISH_MAIN_JOB_SUCC_STATE : WorkerState::FINISH_MAIN_JOB_FAIL_STATE);

    controllerLock = m_controller->LockController();
    m_controller->m_workerCanAbort[m_workerId] = false;
    m_controller->UnlockController(controllerLock);

    if (m_controller->m_mainThread->GetErrorCode() == STORAGE_OK && /* We'll keep the first error code only */
        thrd->error != nullptr && thrd->GetErrorCode() != STORAGE_OK) {
        controllerLock = m_controller->LockController();
        m_controller->m_mainThread->error->CopyError(thrd->error);
        if (m_tupleSortMgr != nullptr && unlikely(!m_tupleSortMgr->UniqueCheckSucc())) {
            if (m_mainTupleSortMgr->m_uniqueCheckSucc && m_tupleSortMgr->m_duplicateTuple != nullptr) {
                m_mainTupleSortMgr->m_uniqueCheckSucc = false;
                StorageAssert(m_mainTupleSortMgr->m_duplicateTuple == nullptr);
                AutoMemCxtSwitch autoMemSwitch{m_controller->m_mainThread->GetSessionMemoryCtx()};
                m_mainTupleSortMgr->m_duplicateTuple = m_tupleSortMgr->m_duplicateTuple->Copy();
                if (unlikely(m_mainTupleSortMgr->m_duplicateTuple == nullptr &&
                             StorageGetErrorCode() == INDEX_ERROR_TUPLE_DAMAGED)) {
                    Btree::DumpDamagedTuple(m_tupleSortMgr->m_duplicateTuple);
                }
                m_mainTupleSortMgr->m_duplicateHeapCtid1 = m_tupleSortMgr->m_duplicateHeapCtid1;
                m_mainTupleSortMgr->m_duplicateHeapCtid2 = m_tupleSortMgr->m_duplicateHeapCtid2;
            }
            DstorePfreeExt(m_tupleSortMgr->m_duplicateTuple);
        }
        m_controller->UnlockController(controllerLock);
    }
    if (likely(scanHandler != nullptr)) {
        scanHandler->End();
        delete scanHandler;
        scanHandler = nullptr;
    }
#ifdef UT
    if (m_taskRes == DSTORE_SUCC) {
        txn->Commit();
    } else {
        txn->Abort();
    }
#endif
    if (m_indexBuildInfo->baseInfo.exprDestroyCallback && exprCxt) {
        m_indexBuildInfo->baseInfo.exprDestroyCallback(exprCxt);
    }

    /*
     * Txn commit or abort will be handled by ReleaseSQLThreadContext and
     * ReleaseSQLThreadContext will destroy dstore thrd in destroy_work_thread_store_cxt.
     */
    context.isCommit = (m_taskRes == DSTORE_SUCC);
    g_storageInstance->ReleaseSQLThreadContext(&context);
    if (thrd != nullptr) {
        g_storageInstance->UnregisterThread();
    }
    m_thrd = thrd;
}

RetStatus ParallelBtreeBuildWorker::BuildLpiParallelCrossPartition(PdbId pdbId,
                                                                   ParallelWorkController *controller)
{
    if (controller == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Unexpected nullptr for BuildLpiParallelCrossPartition."));
        return DSTORE_FAIL;
    }
    m_taskRes = DSTORE_FAIL;
    m_controller = controller;
    m_workerThrd = new std::thread(&ParallelBtreeBuildWorker::BuildWorkerMain, this, pdbId);
    return STORAGE_VAR_NULL(m_workerThrd) ? DSTORE_FAIL : DSTORE_SUCC;
}

IndexBuildInfo* ParallelBtreeBuildWorker::ConstructIndexBuildInfoForOnePart(IndexBuildInfo* indexBuildInfo,
                                                                            int partIdx)
{
    if (indexBuildInfo == nullptr || indexBuildInfo->heapRels == nullptr ||
        indexBuildInfo->indexRels == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Unexpected nullptr for ConstructIndexBuildInfoForOnePart."));
        return nullptr;
    }
    IndexBuildInfo* result = static_cast<IndexBuildInfo*>(DstorePalloc0(sizeof(IndexBuildInfo)));
    if (unlikely(result == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
               ErrMsg("DstorePalloc fail for IndexBuildInfo when ConstructIndexBuildInfoForOnePart."));
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        return nullptr;
    }
    errno_t rc = memcpy_s(&(result->baseInfo), sizeof(IndexInfo), &(indexBuildInfo->baseInfo), sizeof(IndexInfo));
    storage_securec_check(rc, "\0", "\0");
    result->baseInfo.indexRelId = indexBuildInfo->indexRels[partIdx]->relOid;
    result->heapRelationOid = indexBuildInfo->allPartOids[partIdx];
    rc = memcpy_s(result->indexAttrOffset, sizeof(indexBuildInfo->indexAttrOffset),
        indexBuildInfo->indexAttrOffset, sizeof(indexBuildInfo->indexAttrOffset));
    storage_securec_check(rc, "\0", "\0");
    result->heapAttributes = indexBuildInfo->heapAttributes;
    result->heapRels = static_cast<StorageRelation*>(DstorePalloc0(sizeof(StorageRelation)));
    if (unlikely(result->heapRels == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
               ErrMsg("DstorePalloc fail for heapRels when ConstructIndexBuildInfoForOnePart."));
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        return result;
    }
    result->heapRels[0] = indexBuildInfo->heapRels[partIdx];

    result->indexRels = static_cast<StorageRelation*>(DstorePalloc0(sizeof(StorageRelation)));
    if (unlikely(result->indexRels == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
               ErrMsg("DstorePalloc fail for indexRels when ConstructIndexBuildInfoForOnePart."));
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        return result;
    }
    /* We have to rebuild btreeSmgr because the old btreeSmgr is created in the main thread memory context. */
    result->indexRels[0] = nullptr;
    StorageRelation indexRel = indexBuildInfo->indexRels[partIdx];
    BtreeStorageMgr* oldBtreeSmgr = indexRel->btreeSmgr;
    BtreeStorageMgr* newBtreeSmgr = StorageTableInterface::CreateBtreeSmgr(
        oldBtreeSmgr->GetSegment()->GetPdbId(),
        oldBtreeSmgr->GetSegment()->GetTablespaceId(),
        oldBtreeSmgr->GetSegment()->GetSegmentMetaPageId(),
        oldBtreeSmgr->GetFillFactor(),
        oldBtreeSmgr->GetPersistenceMethod(),
        false);
    if (unlikely(newBtreeSmgr == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to create BtreeSmgr."));
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        return result;
    }
    indexRel->btreeSmgr = newBtreeSmgr;
    result->indexRels[0] = indexRel;

    result->heapRelNum = 1;
    result->allPartOids = nullptr;
    result->lpiParallelMethodIsPartition = false;
    result->currentBuildPartIdx = partIdx;
    result->allPartTuples = nullptr;
    result->duplicateTuple = nullptr;
    result->duplicateHeapCtid1 = UInt64GetDatum(INVALID_ITEM_POINTER.m_placeHolder);
    result->duplicateHeapCtid2 = UInt64GetDatum(INVALID_ITEM_POINTER.m_placeHolder);
    return result;
}

void ParallelBtreeBuildWorker::DestroyIndexBuildInfoForOnePart(IndexBuildInfo* indexBuildInfo,
    BtreeStorageMgr* oldBtreeSmgr)
{
    if (indexBuildInfo) {
        DstorePfreeExt(indexBuildInfo->heapRels);
        if (indexBuildInfo->indexRels && indexBuildInfo->indexRels[0]) {
            BtreeStorageMgr* newBtreeSmgr = indexBuildInfo->indexRels[0]->btreeSmgr;
            indexBuildInfo->indexRels[0]->btreeSmgr = oldBtreeSmgr;
            StorageTableInterface::DestroyBtreeSmgr(newBtreeSmgr);
        }
        DstorePfreeExt(indexBuildInfo->indexRels);
    }
    DstorePfreeExt(indexBuildInfo);
}

void ParallelBtreeBuildWorker::BuildWorkerMain(PdbId pdbId)
{
    InitSignalMask();
    thrd = nullptr;
    void *sqlThrd = nullptr;
    SQLThrdInitCtx context = {pdbId, "ParallelBuilder", InternalThreadType::THREAD_INDEX_PARALLEL_BUILD, &sqlThrd,
                              false};
    bool initResult = g_storageInstance->InitSQLThreadContext(&context);
    if (!initResult) {
        SetWorkerState(WorkerState::INIT_SQL_THREAD_CONTEXT_FAIL_STATE);
        m_taskRes = DSTORE_FAIL;
        m_thrd = nullptr;
        return;
    }
    if (thrd == nullptr) {
        g_storageInstance->CreateThreadAndRegister(pdbId, false, "ParallelBuilder", true,
                                                   ThreadMemoryLevel::THREADMEM_MEDIUM_PRIORITY);
    }
    thrd->SetNeedCommBuffer(true);
    thrd->RefreshWorkingVersionNum();
    m_thrd = thrd;
    if (thrd == nullptr || thrd->GetCore() == nullptr) {
        m_taskRes = DSTORE_FAIL;
        return;
    }
    m_threadId = thrd->GetThreadId();

    int partNums = m_indexBuildInfo->heapRelNum;
    StorageRelation indexRel = nullptr;
    IndexBuildInfo *indexBuildInfoForOnePart = nullptr;
    m_taskRes = DSTORE_FAIL;

    /* ParallelBtreeBuildWorker can only abort in dstore. */
    std::unique_lock<std::mutex> *controllerLock = m_controller->LockController();
    m_controller->m_workerCanAbort[m_workerId] = true;
    m_controller->UnlockController(controllerLock);

    Transaction *txn = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(txn)) {
#ifdef UT
        (void)thrd->InitTransactionRuntime(pdbId, nullptr, nullptr);
        txn = thrd->GetActiveTransaction();
        txn->Start();
#else
        /* InitSQLThreadContext will start a new transaction, no need start again here. */
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("No active transaction found in current thread."));
        m_taskRes = DSTORE_FAIL;
        goto END;
#endif
    }
    txn->SetTransactionSnapshotCsn(m_mainThrdTransaction->GetSnapshotCsn());
    txn->SetTransactionSnapshotCid(m_mainThrdTransaction->GetSnapshotCid());
    txn->SetCurrentXid(m_mainThrdTransaction->GetCurrentXid());

    while (true) {
        /* Check for interrupts. */
        if (thrd != nullptr && STORAGE_FUNC_FAIL(thrd->CheckforInterrupts())) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Statements of the current request are canceled "
                "in ParallelBtreeBuildWorker::BuildWorkerMain."));
            goto END;
        }

        controllerLock = m_controller->LockController();
        int currentBuildPartIdx = m_indexBuildInfo->currentBuildPartIdx;
        m_indexBuildInfo->currentBuildPartIdx = currentBuildPartIdx + 1;
        m_controller->UnlockController(controllerLock);

        if (currentBuildPartIdx >= partNums) {
            break;
        }

        ErrLog(DSTORE_LOG, MODULE_INDEX, ErrMsg("Partition %u begin build index %u, currentBuildPartIdx is %d.",
            m_indexBuildInfo->allPartOids[currentBuildPartIdx],
            m_indexBuildInfo->indexRels[currentBuildPartIdx]->relOid,
            currentBuildPartIdx));

        indexRel = m_indexBuildInfo->indexRels[currentBuildPartIdx];

        if (unlikely(indexRel == nullptr || indexRel->btreeSmgr == nullptr ||
            indexRel->btreeSmgr->GetSegment() == nullptr)) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to get BtreeSmgr."));
            goto END;
        }
        BtreeStorageMgr* oldBtreeSmgr = indexRel->btreeSmgr;
        indexBuildInfoForOnePart = ConstructIndexBuildInfoForOnePart(m_indexBuildInfo, currentBuildPartIdx);
        if (unlikely(indexBuildInfoForOnePart == nullptr || indexBuildInfoForOnePart->heapRels == nullptr ||
            indexBuildInfoForOnePart->indexRels == nullptr ||
            indexBuildInfoForOnePart->indexRels[0] == nullptr ||
            indexBuildInfoForOnePart->indexRels[0]->btreeSmgr == nullptr ||
            indexBuildInfoForOnePart->indexRels[0]->btreeSmgr->GetSegment() == nullptr)) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX,
                ErrMsg("ConstructIndexBuildInfoForOnePart failed."));
            storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
            DestroyIndexBuildInfoForOnePart(indexBuildInfoForOnePart, oldBtreeSmgr);
            goto END;
        }
        BtreeBuild btreeBuilder(indexRel, indexBuildInfoForOnePart, m_scanKey);
        if (STORAGE_FUNC_FAIL(btreeBuilder.BuildIndex())) {
            /* Copy error messages required by SQL engine from current thread to main thread. */
            ErrorCode errCode = StorageGetErrorCode();
            if (errCode == TUPLESORT_ERROR_COULD_NOT_CREATE_UNIQUE_INDEX) {
                ErrLog(DSTORE_LOG, MODULE_INDEX, ErrMsg("Partition %u build index %u failed for unique check.",
                    m_indexBuildInfo->allPartOids[currentBuildPartIdx],
                    m_indexBuildInfo->indexRels[currentBuildPartIdx]->relOid));
                controllerLock = m_controller->LockController();
                if (m_controller->m_mainThread->GetErrorCode() == STORAGE_OK /* We'll keep the first error code only */
                    && thrd->error != nullptr && thrd->GetErrorCode() != STORAGE_OK &&
                    indexBuildInfoForOnePart->duplicateTuple != nullptr) {
                    ErrLog(DSTORE_LOG, MODULE_INDEX, ErrMsg("Copy unique index check error information to "
                        "main thread."));
                    m_controller->m_mainThread->error->CopyError(thrd->error);
                    AutoMemCxtSwitch autoMemSwitch{m_controller->m_mainThread->GetSessionMemoryCtx()};
                    m_indexBuildInfo->duplicateHeapCtid1 = indexBuildInfoForOnePart->duplicateHeapCtid1;
                    m_indexBuildInfo->duplicateHeapCtid2 = indexBuildInfoForOnePart->duplicateHeapCtid2;
                    m_indexBuildInfo->duplicateTuple = indexBuildInfoForOnePart->duplicateTuple->Copy();
                    if (unlikely(m_indexBuildInfo->duplicateTuple == nullptr &&
                                 StorageGetErrorCode() == INDEX_ERROR_TUPLE_DAMAGED)) {
                        Btree::DumpDamagedTuple(indexBuildInfoForOnePart->duplicateTuple);
                    }
                    DstorePfreeExt(indexBuildInfoForOnePart->duplicateTuple);
                }
                m_controller->UnlockController(controllerLock);
            } else if (errCode == INDEX_ERROR_FAIL_FOR_HUGE_INDEX_TUPLE ||
                errCode == TUPLESORT_ERROR_MISSING_SUPPORT_FUNCTION_FOR_FUNCOID ||
                errCode == TUPLESORT_ERROR_MISSING_SUPPORT_FUNCTION_FOR_TYPE) {
                controllerLock = m_controller->LockController();
                if (m_controller->m_mainThread->GetErrorCode() == STORAGE_OK /* We'll keep the first error code only */
                    && thrd->error != nullptr && thrd->GetErrorCode() != STORAGE_OK) {
                    m_controller->m_mainThread->error->CopyError(thrd->error);
                    m_indexBuildInfo->baseInfo.extraInfo = indexBuildInfoForOnePart->baseInfo.extraInfo;
                }
                m_controller->UnlockController(controllerLock);
            }
            DestroyIndexBuildInfoForOnePart(indexBuildInfoForOnePart, oldBtreeSmgr);
            goto END;
        }

        controllerLock = m_controller->LockController();
        m_numHeapTuples += indexBuildInfoForOnePart->heapTuples;
        m_numIndexTuples += indexBuildInfoForOnePart->heapTuples;
        m_indexBuildInfo->heapTuples += indexBuildInfoForOnePart->heapTuples;
        m_indexBuildInfo->indexTuples += indexBuildInfoForOnePart->heapTuples;
        double allHeapTuples = m_indexBuildInfo->heapTuples;
        double allIndexTuples = m_indexBuildInfo->indexTuples;
        m_controller->UnlockController(controllerLock);
        m_indexBuildInfo->allPartTuples[currentBuildPartIdx] = indexBuildInfoForOnePart->heapTuples;

        ErrLog(DSTORE_LOG, MODULE_INDEX, ErrMsg("Partition %u end build index %u, currentBuildPartIdx is %d, "
            "allHeapTuples is %lf, allIndexTuples is %lf, heapTuples is %lf.",
            m_indexBuildInfo->allPartOids[currentBuildPartIdx],
            m_indexBuildInfo->indexRels[currentBuildPartIdx]->relOid,
            currentBuildPartIdx, allHeapTuples, allIndexTuples, indexBuildInfoForOnePart->heapTuples));

        DestroyIndexBuildInfoForOnePart(indexBuildInfoForOnePart, oldBtreeSmgr);
    }
    m_taskRes = DSTORE_SUCC;
    StorageClearError();
END:
    SetWorkerState(m_taskRes == DSTORE_SUCC ?
        WorkerState::FINISH_MAIN_JOB_SUCC_STATE : WorkerState::FINISH_MAIN_JOB_FAIL_STATE);

    controllerLock = m_controller->LockController();
    m_controller->m_workerCanAbort[m_workerId] = false;
    m_controller->UnlockController(controllerLock);

    if (m_controller->m_mainThread->GetErrorCode() == STORAGE_OK && /* We'll keep the first error code only */
        thrd->error != nullptr && thrd->GetErrorCode() != STORAGE_OK) {
        m_controller->m_mainThread->error->CopyError(thrd->error);
    }
    if (likely(txn != nullptr)) {
        txn->SetReadOnly(true);
    }
#ifdef UT
    if (m_taskRes == DSTORE_SUCC) {
        txn->Commit();
    } else {
        txn->Abort();
    }
#endif
    /*
     * Txn commit or abort will be handled by ReleaseSQLThreadContext and
     * ReleaseSQLThreadContext will destroy dstore thrd in destroy_work_thread_store_cxt.
     */
    context.isCommit = (m_taskRes == DSTORE_SUCC);
    g_storageInstance->ReleaseSQLThreadContext(&context);
    if (thrd != nullptr) {
        g_storageInstance->UnregisterThread();
    }
    m_thrd = thrd;
}

ParallelBtreeBuild::ParallelBtreeBuild(int numThrds, int workMem, IndexBuildInfo *indexBuildInfo,
                                       ScanKey scanKey, TuplesortMgr *mainTupleSortMgr)
    : m_numThrds(numThrds),
      m_workMem(workMem),
      m_indexBuildInfo(indexBuildInfo),
      m_bufMgr(g_storageInstance->GetBufferMgr()),
      m_scanKey(scanKey),
      m_parallelCxt(nullptr),
      m_mainTupleSortMgr(mainTupleSortMgr)
{
    for (int i = 0; i < m_numThrds; i++) {
        m_workers[i] = nullptr;
    }
}

RetStatus ParallelBtreeBuild::Init()
{
    InitParallelCxt();
    if (unlikely(m_parallelCxt == nullptr)) {
        return DSTORE_FAIL;
    }
    Transaction *mainThreadTxn = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(mainThreadTxn)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("No active transaction found in current thread."));
        return DSTORE_FAIL;
    }
    for (int i = 0; i < m_numThrds; i++) {
        TuplesortMgr *tuplesortMgr = nullptr;
        if (!m_indexBuildInfo->lpiParallelMethodIsPartition) {
            tuplesortMgr = TuplesortMgr::CreateParallelIdxTupleSortMgr(this->GetPdbId(),
                m_indexBuildInfo->baseInfo.indexRelId, i);
            if (unlikely(tuplesortMgr == nullptr)) {
                storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
                ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to new TuplesortMgr."));
                return DSTORE_FAIL;
            }
        }
        m_workers[i] = DstoreNew(g_dstoreCurrentMemoryContext)
            ParallelBtreeBuildWorker(tuplesortMgr, mainThreadTxn, m_indexBuildInfo, m_workMem, m_scanKey,
                                     m_parallelCxt, m_mainTupleSortMgr, i);
        if (STORAGE_VAR_NULL(m_workers[i])) {
            storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to ParallelBtreeBuildWorker."));
            /* The memory we allocated before will be released along with the memory context. */
            return DSTORE_FAIL;
        }
    }
    return DSTORE_SUCC;
}

RetStatus ParallelBtreeBuild::CollectTuplesFromTable(StorageRelation heapRel, Oid tableOid,
                                                     double &numHeapTuples, bool isLastPartition)
{
    ParallelWorkController *controller = DstoreNew(g_dstoreCurrentMemoryContext) ParallelWorkController(m_numThrds);
    if (STORAGE_VAR_NULL(controller)) {
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to new ParallelWorkController."));
        return DSTORE_FAIL;
    }

    bool *workersToWait = static_cast<bool *>(DstorePalloc0(sizeof(bool) * m_numThrds));
    if (STORAGE_VAR_NULL(workersToWait)) {
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to allocate waiting worker list."));
        delete controller;
        return DSTORE_FAIL;
    }

    StorageClearError();
    RetStatus ret = DSTORE_SUCC;
    for (int i = 0; i < m_numThrds; i++) {
        if (STORAGE_VAR_NULL(m_workers[i])) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to new ParallelBtreeBuildWorker[%d].", i));
            ret = DSTORE_FAIL;
            break;
        }
        if (STORAGE_FUNC_FAIL(m_workers[i]->Run(heapRel->m_pdbId, heapRel, tableOid, controller, isLastPartition))) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX,
                   ErrMsg("Failed to run ParallelBtreeBuildWorker[%d]. tid:%lu", i, m_workers[i]->GetWorkerThreadId()));
            ret = DSTORE_FAIL;
            break;
        }
        workersToWait[i] = true;
    }

LOOP:
    /* Waiting for workers finish main job. */
    bool allFinishMainJob = true;
    bool isInInterrupt = STORAGE_FUNC_FAIL(thrd->CheckforInterrupts());
    for (int i = 0; i < m_numThrds; i++) {
        if (!workersToWait[i]) {
            continue;
        }

        /* If the create index statement is cancelled or one thread is failed, abort other threads. */
        if (unlikely(isInInterrupt || STORAGE_FUNC_FAIL(ret))) {
            std::unique_lock<std::mutex> *controllerLock = controller->LockController();
            if (controller->m_workerCanAbort[i]) {
                m_workers[i]->AbortBuild();
                ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Send AbortBuild to ParallelBtreeBuildWorker[%d]. tid:%lu",
                                                          i, m_workers[i]->GetWorkerThreadId(true)));
            }
            controller->UnlockController(controllerLock);
            ret = DSTORE_FAIL;
        }

        WorkerState state = m_workers[i]->GetWorkerState();
        if (state == WorkerState::NORMAL_STATE) {
            allFinishMainJob = false;
        } else if (state == WorkerState::INIT_SQL_THREAD_CONTEXT_FAIL_STATE ||
            state == WorkerState::FINISH_MAIN_JOB_FAIL_STATE) {
            ret = DSTORE_FAIL;
        }
    }
    if (!allFinishMainJob) {
        GaussUsleep(MICRO_PER_MILLI_SEC);
        goto LOOP;
    }

    /* Waiting for workers done */
    for (int i = 0; i < m_numThrds; i++) {
        if (!workersToWait[i]) {
            continue;
        }

        if (STORAGE_FUNC_FAIL(m_workers[i]->GetWorkerResult())) {
            if (m_workers[i]->GetWorkerThreadId() == INVALID_THREAD_ID) {
                /* m_threadId has never been set. */
                storage_set_error(COMMON_ERROR_CREATE_THREAD_FAIL);
            } else if (m_workers[i]->GetWorkerState() == WorkerState::INIT_SQL_THREAD_CONTEXT_FAIL_STATE) {
                /* InitSQLThreadContext failed. */
                storage_set_error(COMMON_ERROR_INIT_THREAD_FAIL);
            }
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("ParallelBtreeBuildWorker[%d] failed. tid:%lu. errMsg:%s", i,
                                                      m_workers[i]->GetWorkerThreadId(), thrd->GetErrorMessage()));
            ret = DSTORE_FAIL;
        } else {
            numHeapTuples += m_workers[i]->GetHeapTupleCount();
        }
    }
    DstorePfreeExt(workersToWait);
    delete controller;
    return ret;
}

void ParallelBtreeBuild::ClearWorkers()
{
    for (int i = 0; i < m_numThrds; i++) {
        if (m_workers[i] == nullptr) {
            continue;
        }
        m_workers[i]->Destroy();
        delete m_workers[i];
        m_workers[i] = nullptr;
    }
}

void ParallelBtreeBuild::Destroy()
{
    ClearWorkers();
    StorageAssert(DstoreMemoryContextIsValid(m_parallelCxt));
    DstoreMemoryContextDelete(m_parallelCxt);
}

RetStatus ParallelBtreeBuild::MergeWorkerResults()
{
    int availMem = m_workMem;
    for (int i = 0; i < m_numThrds; i++) {
        TuplesortMgr *tuplesort = m_workers[i]->GetTupleSortMgr();
        m_indexBuildInfo->heapTuples += m_workers[i]->GetHeapTupleCount();
        m_indexBuildInfo->indexTuples += m_workers[i]->GetIndexTupleCount();
        availMem -= tuplesort->GetUsedMemory();
    }

    if (STORAGE_FUNC_FAIL(m_mainTupleSortMgr->PrepareParallelSortMainThread(m_indexBuildInfo->baseInfo, availMem,
                                                                            m_scanKey, m_numThrds))) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Prepare parallel sort main thread failed!"));
        return DSTORE_FAIL;
    }

    for (int i = 0; i < m_numThrds; i++) {
        if (STORAGE_FUNC_FAIL(m_mainTupleSortMgr->Aggregate(m_workers[i]->GetTupleSortMgr()))) {
            return DSTORE_FAIL;
        }
    }

    return DSTORE_SUCC;
}

RetStatus ParallelBtreeBuild::BuildLpiParallelCrossPartition(PdbId pdbId)
{
    ParallelWorkController *controller = DstoreNew(g_dstoreCurrentMemoryContext) ParallelWorkController(m_numThrds);
    if (STORAGE_VAR_NULL(controller)) {
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to new ParallelWorkController."));
        return DSTORE_FAIL;
    }

    bool *workersToWait = static_cast<bool *>(DstorePalloc0(sizeof(bool) * m_numThrds));
    if (STORAGE_VAR_NULL(workersToWait)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to allocate waiting worker list."));
        delete controller;
        return DSTORE_FAIL;
    }

    StorageClearError();
    RetStatus ret = DSTORE_SUCC;
    for (int i = 0; i < m_numThrds; i++) {
        if (STORAGE_VAR_NULL(m_workers[i])) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to new ParallelBtreeBuildWorker[%d].", i));
            ret = DSTORE_FAIL;
            break;
        }
        if (STORAGE_FUNC_FAIL(m_workers[i]->BuildLpiParallelCrossPartition(pdbId, controller))) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX,
                   ErrMsg("Failed to run ParallelBtreeBuildWorker[%d]. tid:%lu", i, m_workers[i]->GetWorkerThreadId()));
            ret = DSTORE_FAIL;
            break;
        }
        workersToWait[i] = true;
    }

LOOP:
    /* Waiting for workers finish main job. */
    bool allFinishMainJob = true;
    bool isInInterrupt = STORAGE_FUNC_FAIL(thrd->CheckforInterrupts());
    for (int i = 0; i < m_numThrds; i++) {
        if (!workersToWait[i]) {
            continue;
        }

        /* If the create index statement is cancelled or one thread is failed, abort other threads. */
        if (unlikely(isInInterrupt || STORAGE_FUNC_FAIL(ret))) {
            std::unique_lock<std::mutex> *controllerLock = controller->LockController();
            if (controller->m_workerCanAbort[i]) {
                m_workers[i]->AbortBuild();
                ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Send AbortBuild to ParallelBtreeBuildWorker[%d]. tid:%lu",
                                                          i, m_workers[i]->GetWorkerThreadId(true)));
            }
            controller->UnlockController(controllerLock);
            ret = DSTORE_FAIL;
        }

        WorkerState state = m_workers[i]->GetWorkerState();
        if (state == WorkerState::NORMAL_STATE) {
            allFinishMainJob = false;
        } else if (state == WorkerState::INIT_SQL_THREAD_CONTEXT_FAIL_STATE ||
            state == WorkerState::FINISH_MAIN_JOB_FAIL_STATE) {
            ret = DSTORE_FAIL;
        }
    }
    if (!allFinishMainJob) {
        GaussUsleep(MICRO_PER_MILLI_SEC);
        goto LOOP;
    }

    /* Waiting for workers done */
    for (int i = 0; i < m_numThrds; i++) {
        if (!workersToWait[i]) {
            continue;
        }

        if (STORAGE_FUNC_FAIL(m_workers[i]->GetWorkerResult())) {
            if (m_workers[i]->GetWorkerThreadId() == INVALID_THREAD_ID) {
                /* m_threadId has never been set. */
                storage_set_error(COMMON_ERROR_CREATE_THREAD_FAIL);
            } else if (m_workers[i]->GetWorkerState() == WorkerState::INIT_SQL_THREAD_CONTEXT_FAIL_STATE) {
                /* InitSQLThreadContext failed. */
                storage_set_error(COMMON_ERROR_INIT_THREAD_FAIL);
            }
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("ParallelBtreeBuildWorker[%d] failed. tid:%lu. errMsg:%s", i,
                                                      m_workers[i]->GetWorkerThreadId(), thrd->GetErrorMessage()));
            ret = DSTORE_FAIL;
        }
    }
    DstorePfreeExt(workersToWait);
    delete controller;
    return ret;
}

void ParallelBtreeBuild::InitParallelCxt()
{
    StorageAssert(m_parallelCxt == nullptr);
    m_parallelCxt = DstoreAllocSetContextCreate(
        g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_QUERY), "Parallel btree build share context",
        ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE,
        MemoryContextType::SHARED_CONTEXT);
    if (STORAGE_VAR_NULL(m_parallelCxt)) {
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("CreateMemoryContext fail when InitParallelCxt."));
    }
}

}
