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
 * dstore_ccindex_btree_build_handler.cpp
 *
 * IDENTIFICATION
 *        dstore/src/index/dstore_ccindex_btree_build_handler.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "errorcode/dstore_index_error_code.h"
#include "errorcode/dstore_transaction_error_code.h"
#include "transaction/dstore_transaction.h"
#include "transaction/dstore_transaction_mgr.h"
#include "index/concurrent/dstore_btree_incomplete_index_for_ccindex.h"
#include "index/concurrent/dstore_ccindex_btree_build_handler.h"

namespace DSTORE {

constexpr int EXTRA_COL_NUM_FOR_DELTA_DML_TABLE = 2;

CcindexBtrBuildHandler::CcindexBtrBuildHandler(StorageRelation indexRel, IndexBuildInfo *indexBuildInfo,
                                               ScanKey scanKey, bool isLpi)
    : BtreeBuild(indexRel, indexBuildInfo, scanKey),
      m_isLpi(isLpi),
      m_isRebuild(false),
      m_status(WaitForTrxsEndType::WAIT_FOR_INDEX_META_VISIBLE_FOR_ALL),
      m_dmlScanHandler(nullptr)
{}

void CcindexBtrBuildHandler::Destroy()
{
    if (m_dmlScanHandler != nullptr) {
        m_dmlScanHandler->EndScan();
        delete m_dmlScanHandler;
        m_dmlScanHandler = nullptr;
    }
}

RetStatus CcindexBtrBuildHandler::WaitForTrxVisibleForAll(PdbId pdbId, Xid xid)
{
    StorageAssert(xid != INVALID_XID);
    XidStatus xidStatus(xid, thrd->GetActiveTransaction(), nullptr);
    /* The targetCsn will be invalid, When the localCsnMin is greater than the csn of specail xid. */
    CommitSeqNo targetCsn = xidStatus.GetCsn();
    bool needWaitForActiveTrnEnd = false;
    switch (m_status) {
        case WaitForTrxsEndType::WAIT_FOR_INDEX_META_VISIBLE_FOR_ALL: {
            /*
             * Wait for all transactions that starts before index meta committed end
             * to make sure that these transactions are visible to us so that our btree structure can see any changes
             * they have made when scanning the heap table.
             */
            needWaitForActiveTrnEnd = true;
            m_status = WaitForTrxsEndType::WAIT_FOR_DELTA_DML_TRXS_END;
            break;
        }
        case WaitForTrxsEndType::WAIT_FOR_DELTA_DML_TRXS_END: {
            /*
             * Wait for all transactions that starts before btree building committed end
             * to make sure that these transactions are visible to us so that when doing merge, we can see any changes
             * they have made when scanning the delta dml table.
             */
            needWaitForActiveTrnEnd = true;
            if (m_isRebuild) {
                m_status = WaitForTrxsEndType::WAIT_FOR_OLD_INDEX_TRXS_END;
            } else {
                m_status = WaitForTrxsEndType::NO_MORE_WAIT;
            }
            break;
        }
        case WaitForTrxsEndType::WAIT_FOR_OLD_INDEX_TRXS_END: {
            StorageAssert(m_isRebuild);
            /*
             * Before mark the old index dead, we need to wait for the "invalid" status of old index visible to all,
             * including flashback cases.
             */
            m_status = WaitForTrxsEndType::WAIT_FOR_OLD_INDEX_INVISIBLE_FOR_ALL;
            break;
        }
        case WaitForTrxsEndType::WAIT_FOR_OLD_INDEX_INVISIBLE_FOR_ALL: {
            StorageAssert(m_isRebuild);
            /*
             * Before drop the old index, we need to wait for the "dead" status of old index visible to all,
             * including flashback cases.
             */
            needWaitForActiveTrnEnd = true;
            m_status = WaitForTrxsEndType::NO_MORE_WAIT;
            break;
        }
        default: {
            storage_set_error(INDEX_ERROR_INVALID_WAITING_TYPE, static_cast<uint8>(m_status));
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Invalid WaitForTrxsEndType value: %hhu.",
                static_cast<uint8>(m_status)));
            return DSTORE_FAIL;
        }
    }
    if (!needWaitForActiveTrnEnd) {
        return WaitForGlobalCsnPushToSpecificCsn(pdbId, targetCsn);
    }

    /* Wait for the next csn pushed to be greater than the targetCsn
     * so that all the new transactions can see the changes we've made */
    uint64 csnMinUpdateInterval = static_cast<uint64>(g_storageInstance->GetGuc()->updateCsnMinInterval);
    TimestampTz startTime = GetCurrentTimestampInSecond();
    TimestampTz currTime = startTime;
    CommitSeqNo nextCsn = INVALID_CSN;
    while (targetCsn != INVALID_CSN) {
        nextCsn = thrd->GetNextCsn();
        if (STORAGE_FUNC_FAIL(thrd->CheckforInterrupts())) {
            /* Cancel request sent  */
            return DSTORE_FAIL;
        }
        if (nextCsn > targetCsn) {
            break;
        }
        currTime = GetCurrentTimestampInSecond();
        if (unlikely(currTime - startTime >= TRANSACTON_MAX_WAIT_TIME_IN_SEC)) {
            storage_set_error(TRANSACTION_ERROR_WAIT_TIMEOUT);
            return DSTORE_FAIL;
        }
        GaussUsleep(csnMinUpdateInterval);
    }
    return WaitForAllCurrentActiveTrxsEnd(pdbId, targetCsn, startTime);
}

RetStatus CcindexBtrBuildHandler::WaitForAllCurrentActiveTrxsEnd(PdbId pdbId, CommitSeqNo targetCsn,
                                                                 TimestampTz startTime)
{
    StoragePdb *pdb = g_storageInstance->GetPdb(pdbId);
    if (STORAGE_VAR_NULL(pdb)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr \"StoragePdb\".", __FUNCTION__));
        return DSTORE_FAIL;
    }
    TransactionMgr *transactionMgr = pdb->GetTransactionMgr();
    if (STORAGE_VAR_NULL(transactionMgr)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr \"TransactionMgr\".", __FUNCTION__));
        return DSTORE_FAIL;
    }
    return transactionMgr->WaitEndOfAllActiveTrxsEarlierThanSnapshot(targetCsn, startTime);
}

/* Caller cannot block local csn advance */
RetStatus CcindexBtrBuildHandler::WaitForGlobalCsnPushToSpecificCsn(PdbId pdbId, CommitSeqNo targetCsn)
{
    RetStatus ret = DSTORE_FAIL;
    CommitSeqNo minCsn = INVALID_CSN;
    TimestampTz startTime = GetCurrentTimestampInSecond();
    TimestampTz currTime = startTime;
    uint64 csnMinUpdateInterval = DEFAULT_UPDATE_INTERVAL;
    if (g_storageInstance->GetGuc() != nullptr && g_storageInstance->GetGuc()->updateCsnMinInterval > 0) {
        csnMinUpdateInterval = static_cast<uint64>(g_storageInstance->GetGuc()->updateCsnMinInterval);
    }
    /* By here, we set the localCsn of current thread invalid, to let the localCsnMin of all threads forward,
     * because the caller transaction may call SetSnapshotCsn() previously which will block the localCsnMin forward
     */
    thrd->SetLocalCsn(INVALID_CSN);

    CsnMgr *csnMgr = nullptr;
    csnMgr = g_storageInstance->GetCsnMgr();
    if (STORAGE_VAR_NULL(csnMgr)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr \"CsnMgr\".", __FUNCTION__));
        return DSTORE_FAIL;
    }

    ErrLog(DSTORE_DEBUG1, MODULE_INDEX, ErrMsg("[CCINDEX] start waiting for csn %lu, next csn %lu, "
        "current recmin csn %lu, snapshot csn %lu", targetCsn, thrd->GetNextCsn(),
        csnMgr->GetRecycleCsnMin(pdbId), thrd->GetSnapShotCsn()));

    uint64 retryCounter = 0UL;
    for (;;) {
        if (STORAGE_FUNC_FAIL(thrd->CheckforInterrupts())) {
            /* Cancel request sent  */
            return DSTORE_FAIL;
        }
        minCsn = csnMgr->GetRecycleCsnMin(pdbId);
        if (minCsn > targetCsn) {
            ret = DSTORE_SUCC;
            break;
        }

        currTime = GetCurrentTimestampInSecond();
        if (likely(currTime - startTime >= TRANSACTON_MAX_WAIT_TIME_IN_SEC)) {
            /* Waiting timeout */
            storage_set_error(TRANSACTION_ERROR_WAIT_TIMEOUT, retryCounter);
            break;
        }
        GaussUsleep(csnMinUpdateInterval);
    }

    ErrLog(DSTORE_LOG, MODULE_INDEX, ErrMsg("[CCINDEX]index(%s): %s to wait for recycleCsnMin pushing to %lu. "
        "retried %lu times. minCsn:%lu, nextCsn:%lu", (ret == DSTORE_SUCC) ? "succ" : "fail",
        m_indexInfo->indexRelName, targetCsn, retryCounter, minCsn, thrd->GetNextCsn()));
    return DSTORE_SUCC;
}

RetStatus CcindexBtrBuildHandler::BuildIndexConcurrently()
{
    StorageAssert(m_indexInfo->btrIdxStatus == BtrCcidxStatus::IN_BUILDING_INDEX);

    ErrLog(DSTORE_LOG, MODULE_INDEX,
        ErrMsg("[CCINDEX] build index(%s:%u) on pdb(%u) table(%u): xid(%d, %lu), snapshot(%lu), segment(%hu, %u)",
            m_indexInfo->indexRelName, m_indexInfo->indexRelId, GetPdbId(), m_indexBuildInfo->heapRelationOid,
            static_cast<uint32>(thrd->GetCurrentXid().m_zoneId), thrd->GetCurrentXid().m_logicSlotId,
            thrd->GetSnapShotCsn(),
            GetBtreeSmgr()->GetSegMetaPageId().m_fileId, GetBtreeSmgr()->GetSegMetaPageId().m_blockId));

    RetStatus ret = BuildIndex();
    ErrLog(DSTORE_LOG, MODULE_INDEX,
        ErrMsg("[CCINDEX] build index(%s:%u) on pdb(%u) table(%u) %s: xid(%d, %lu), snapshot(%lu), segment(%hu, %u), "
               "heapTuples(%f), indexTuples(%f)",
            m_indexInfo->indexRelName, m_indexInfo->indexRelId, GetPdbId(), m_indexBuildInfo->heapRelationOid,
            ret == DSTORE_SUCC ? "succ" : "fail",
            static_cast<uint32>(thrd->GetCurrentXid().m_zoneId), thrd->GetCurrentXid().m_logicSlotId,
            thrd->GetSnapShotCsn(),
            GetBtreeSmgr()->GetSegMetaPageId().m_fileId, GetBtreeSmgr()->GetSegMetaPageId().m_blockId,
            m_indexBuildInfo->heapTuples, m_indexBuildInfo->indexTuples));

    if (STORAGE_FUNC_FAIL(ret)) {
        return DSTORE_FAIL;
    }

    /* Allow to merge delta data to btree */
    SetCcindexBtrStatus(BtrCcidxStatus::WRITE_ONLY_INDEX);
    return DSTORE_SUCC;
}

RetStatus CcindexBtrBuildHandler::InitMergeScanHandler(StorageRelation deltaDmlIndexRel, IndexInfo *deltaDmlIndexInfo)
{
    StorageAssert(m_dmlScanHandler == nullptr);
    m_dmlScanHandler = IndexScanHandler::Create(deltaDmlIndexRel, deltaDmlIndexInfo, 0, 0);
    if (unlikely(m_dmlScanHandler == nullptr)) {
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
               ErrMsg("Faild to prepare IndexScanHandler when MergeIndexTupleFromTmp, dmlIndexScan is null."));
        return DSTORE_FAIL;
    }
    m_dmlScanHandler->SetStorageRelOid(deltaDmlIndexRel->relOid);
    m_dmlScanHandler->GetScanDesc()->wantItup = true;
    m_dmlScanHandler->InitSnapshot(thrd->GetActiveTransaction()->GetSnapshot());
    if (STORAGE_FUNC_FAIL(m_dmlScanHandler->BeginScan())) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Faild to prepare IndexScanHandler when MergeIndexTupleFromTmp."));
        delete m_dmlScanHandler;
        m_dmlScanHandler = nullptr;
        return DSTORE_FAIL;
    }
    return m_dmlScanHandler->ReScan(nullptr);
}

RetStatus CcindexBtrBuildHandler::MergeDeltaDmlForNonLocal(StorageRelation deltaDmlIndexRel,
                                                           IndexInfo *deltaDmlIndexInfo)
{
    StorageAssert(m_dmlScanHandler == nullptr);
    RetStatus ret = DSTORE_SUCC;

    /* Step 1. Create IndexScanHandler of delta dml index */
    /* We save real data of index key, heapCtid of index tuple, dml type of INSERT/DELETE to the delta dml table,
     * then build an unique index for the heapCtid column in the delta dml table.
     * Here we traverse the unique index for all real heapCtid from the IndexScanHandler,
     * then use the heapCtid of unique index tuple to read more real data from the HeapScanHandler */
    if (STORAGE_FUNC_FAIL(InitMergeScanHandler(deltaDmlIndexRel, deltaDmlIndexInfo))) {
        return DSTORE_FAIL;
    }

    /* Step 2. Read records from delta dml table */
    bool found;
    IndexTuple *targetIndex = nullptr;
    while (STORAGE_FUNC_SUCC(m_dmlScanHandler->GetNextTuple(ScanDirection::FORWARD_SCAN_DIRECTION, &found)) && found) {
        targetIndex = m_dmlScanHandler->GetScanDesc()->itup;
        if (unlikely(targetIndex == nullptr)) {
            /* We've finished scan successfully. */
            break;
        }
        m_deltaTupleScanned++;
        if (STORAGE_FUNC_FAIL(HandleAndMergeEachDeltaRecord(targetIndex, deltaDmlIndexInfo->attributes))) {
            ret = DSTORE_FAIL;
            break;
        }
    }

    m_dmlScanHandler->EndScan();
    delete m_dmlScanHandler;
    m_dmlScanHandler = nullptr;

    ErrLog(DSTORE_LOG, MODULE_INDEX,
        ErrMsg("[CCINDEX] %s to merge delta data when create index[%d](%s). "
               "insert:%lu, delete:%lu, skipped:%lu, total:%lu",
               ret == DSTORE_SUCC ? "Succed" : "Fail", m_indexInfo->indexRelId, m_indexInfo->indexRelName,
               m_deltaTupleInsert, m_deltaTupleDelete, m_deltaTupleSkipped, m_deltaTupleScanned));
    return ret;
}

RetStatus CcindexBtrBuildHandler::MergeDeltaDmlForLpi(StorageRelation deltaDmlIndexRel, IndexInfo *deltaDmlIndexInfo)
{
    StorageAssert(m_indexInfo->relKind != static_cast<char>(SYS_RELKIND_GLOBAL_INDEX));
    m_indexBuildInfo->baseInfo.extraInfo = ObjectIdGetDatum(DSTORE_INVALID_OID);

    RetStatus ret = DSTORE_FAIL;
    Oid handlerPartOid = m_indexBuildInfo->heapRelationOid;
    Oid scanPartOid = DSTORE_INVALID_OID;
    IndexTuple *currTup = nullptr;
    bool isPartOidNull = false;
    bool found;
    if (m_dmlScanHandler == nullptr) {
        if (STORAGE_FUNC_FAIL(InitMergeScanHandler(deltaDmlIndexRel, deltaDmlIndexInfo))) {
            return DSTORE_FAIL;
        }
        /* Get the first index tuple so that we can step to the partition oid check if the delta table is not empty */
        if (STORAGE_FUNC_FAIL(m_dmlScanHandler->GetNextTuple(ScanDirection::FORWARD_SCAN_DIRECTION, &found))) {
            ret = DSTORE_FAIL;
            goto RETURN_DESTROY_HANDLER;
        }
        if (!found) {
            /* The deltaDmlIndex is empty, we're done. */
            ret = DSTORE_SUCC;
            goto RETURN_DESTROY_HANDLER;
        }
    }

    /* When m_dmlScanHandler != nullptr, we must already have a scaned tuple since we would return without releasing
     * m_dmlScanHandler iff we have a scaned tuple that was unmatched with hanlder's partition.
     * Caller will call this function again using a correct partition and we can continue to merge the tuple we
     * got in the previous loop. */
    currTup = m_dmlScanHandler->GetScanDesc()->itup;
    while (currTup != nullptr) {
        scanPartOid = currTup->GetAttr(1, deltaDmlIndexInfo->attributes, &isPartOidNull);
        StorageAssert(!isPartOidNull);
        if (unlikely(scanPartOid != handlerPartOid)) {
            m_indexBuildInfo->baseInfo.extraInfo = ObjectIdGetDatum(scanPartOid);
            /* Note: we don't destroy m_dmlScanHandler here.
             * Caller should change to the partition in extraInfo for us and we'll continue to merge. */
            return DSTORE_SUCC;
        }
        if (STORAGE_FUNC_FAIL(HandleAndMergeEachDeltaRecord(currTup, deltaDmlIndexInfo->attributes))) {
            break;
        }
        if (STORAGE_FUNC_FAIL(m_dmlScanHandler->GetNextTuple(ScanDirection::FORWARD_SCAN_DIRECTION, &found))) {
            ret = DSTORE_FAIL;
            goto RETURN_DESTROY_HANDLER;
        }
        if (!found) {
            /* We've traversed the whole index. Finish merge here. */
            m_indexBuildInfo->baseInfo.extraInfo = ObjectIdGetDatum(DSTORE_INVALID_OID);
            ret = DSTORE_SUCC;
            break;
        }
        currTup = m_dmlScanHandler->GetScanDesc()->itup;
    }

RETURN_DESTROY_HANDLER:
    m_dmlScanHandler->EndScan();
    delete m_dmlScanHandler;
    m_dmlScanHandler = nullptr;

    ErrLog(DSTORE_LOG, MODULE_INDEX,
        ErrMsg("[CCINDEX] %s to merge delta data when create lpi index[%d](%s): ",
               ret == DSTORE_SUCC ? "Succed" : "Fail", m_indexInfo->indexRelId, m_indexInfo->indexRelName));
    return ret;
}

RetStatus CcindexBtrBuildHandler::MergeDeltaDml(StorageRelation deltaDmlIndexRel, IndexInfo *deltaDmlIndexInfo)
{
    StorageAssert(m_indexInfo->btrIdxStatus == BtrCcidxStatus::WRITE_ONLY_INDEX);
    if (STORAGE_FUNC_FAIL(thrd->GetActiveTransaction()->AllocTransactionSlot())) {
        return DSTORE_FAIL;
    }

    if (m_isLpi) {
        return MergeDeltaDmlForLpi(deltaDmlIndexRel, deltaDmlIndexInfo);
    }
    return MergeDeltaDmlForNonLocal(deltaDmlIndexRel, deltaDmlIndexInfo);
}

RetStatus CcindexBtrBuildHandler::HandleAndMergeEachDeltaRecord(IndexTuple *deltaDmlIndexTup,
                                                                TupleDesc deltaDmlIndexTupDesc)
{
    StorageAssert(deltaDmlIndexTupDesc->natts == m_indexBuildInfo->baseInfo.indexAttrsNum +
                                                 EXTRA_COL_NUM_FOR_DELTA_DML_TABLE + static_cast<int>(m_isLpi));
    bool isNull = false;
    int currAttNo = 1;

    /* Get values for indexTuple forming
     * for LPI:
     *      DeltaDmlTable (partid, heap_ctid, key_values, include_values, oper_type)
     *      DeltaDmlIndex (partid, heap_ctid, key_values) include (include_values, oper_type)
     * for GPI:
     *      DeltaDmlTable (heap_ctid, key_values, include_values(with partid) oper_type)
     *      DeltaDmlIndex (heap_ctid, key_values) include (include_values(with partid), oper_type)
     *  for regular index:
     *      DeltaDmlTable (heap_ctid, key_values, include_values, oper_type)
     *      DeltaDmlIndex (heap_ctid, key_values) include (include_values, oper_type)
     */
    if (m_isLpi) {
        /* For LPI, the first column of DeltaDmlTable is tableOid, HeapCtid is in the second column */
#ifdef DSTORE_USE_ASSERT_CHECKING
        Oid tableOid = DatumGetObjectId(deltaDmlIndexTup->GetAttr(currAttNo, deltaDmlIndexTupDesc, &isNull));
        StorageAssert(!isNull);
        StorageAssert(tableOid != DSTORE_INVALID_OID);
#endif
        currAttNo++;
    }
    ItemPointerData heapCtid = ItemPointerData(DatumGetInt64(deltaDmlIndexTup->GetAttr(currAttNo++,
                                                                                       deltaDmlIndexTupDesc, &isNull)));
    StorageAssert(heapCtid != INVALID_ITEM_POINTER);

    Datum targetItupValues[INDEX_MAX_KEY_NUM];
    bool targetItupIsNulls[INDEX_MAX_KEY_NUM];
    for (int i = 0; i < m_indexInfo->indexAttrsNum; i++) {
        StorageAssert(currAttNo < deltaDmlIndexTupDesc->natts);
        targetItupValues[i] = deltaDmlIndexTup->GetAttr(currAttNo++, deltaDmlIndexTupDesc, &targetItupIsNulls[i]);
    }

    StorageAssert(currAttNo == deltaDmlIndexTupDesc->natts);
    DmlOperationTypeForCcindex tupleOperationType = static_cast<DmlOperationTypeForCcindex>(
        DATUM_GET_INT8(deltaDmlIndexTup->GetAttr(currAttNo, deltaDmlIndexTupDesc, &isNull)));
    if (unlikely(!CheckDmlOperationType(tupleOperationType) || isNull)) {
        storage_set_error(INDEX_ERROR_FAIL_FOR_INVALID_DML_TYPE, tupleOperationType);
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
            ErrMsg("Wrong dml operation type with value of %hhd or whether is null: %d.",
            static_cast<int8>(tupleOperationType), isNull));
        return DSTORE_FAIL;
    }

    if (STORAGE_FUNC_FAIL(DoMerge(targetItupValues, targetItupIsNulls, &heapCtid, tupleOperationType))) {
        ItemPointerData deltaDmlHeapCtid = deltaDmlIndexTup->GetHeapCtid();
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
            ErrMsg("Merge tuple failed with orig heapCtid: (%hu, %u, %hu) deltaDml heapCtid(%hu, %u, %hu) type %hhd.",
            heapCtid.GetFileId(), heapCtid.GetBlockNum(), heapCtid.GetOffset(),
            deltaDmlHeapCtid.GetFileId(), deltaDmlHeapCtid.GetBlockNum(), deltaDmlHeapCtid.GetOffset(),
            static_cast<int8>(tupleOperationType)));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus CcindexBtrBuildHandler::DoMerge(Datum *values, bool *isnull, ItemPointer heapCtid,
                                          DmlOperationTypeForCcindex tupleOperationType)
{
    ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
        ErrMsg("[CCINDEX] create index(%s): merge delta data of dml type(%hhd), heapCtid(%hu, %u), offset:%hu",
        m_indexInfo->indexRelName, static_cast<int8_t>(tupleOperationType),
        heapCtid->GetFileId(), heapCtid->GetBlockNum(), heapCtid->GetOffset()));

    switch (tupleOperationType) {
        case DmlOperationTypeForCcindex::DML_OPERATION_INSERT: {
            /* Insert tuple concurrently */
            return IncompleteBtreeInsertForCcindex::MergeDeltaInsertionToBtree(this, values, isnull, heapCtid);
        }
        case DmlOperationTypeForCcindex::DML_OPERATION_DELETE: {
            /* Delete tuple concurrently */
            return IncompleteBtreeDeleteForCcindex::MergeDeltaDeletionToBtree(this, values, isnull, heapCtid);
        }
        default: {
            storage_set_error(INDEX_ERROR_FAIL_FOR_INVALID_DML_TYPE, tupleOperationType);
            ErrLog(DSTORE_ERROR, MODULE_INDEX,
                ErrMsg("Wrong operation type(%hhd) Reading from delta dml table.",
                static_cast<int8>(tupleOperationType)));
            return DSTORE_FAIL;
        }
    }

    return DSTORE_FAIL;
}
}