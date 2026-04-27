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
 * dstore_btree_delta_index_for_ccindex.cpp
 *
 *
 *
 * IDENTIFICATION
 *        storage/src/index/concurrent/dstore_btree_delta_index_for_ccindex.cpp
 *
 * ---------------------------------------------------------------------------------------*/

#include "index/concurrent/dstore_btree_delta_index_for_ccindex.h"
#include "transaction/dstore_transaction_mgr.h"

namespace DSTORE {

BtreeDeltaDmlForCcindex::BtreeDeltaDmlForCcindex(BtreeInfo btrInfo)
    : BtreeInsert(btrInfo.indexRel, btrInfo.indexInfo, btrInfo.scanKey),
      m_isOrigIndexLpi(btrInfo.isDeltaForLpi),
      m_duplicateHeapCtid(INVALID_ITEM_POINTER),
      m_delDmlHeapCtid(INVALID_ITEM_POINTER),
      m_heapCtid(INVALID_ITEM_POINTER),
      m_operType(DmlOperationTypeForCcindex::INVALID_TYPE),
      m_values(nullptr),
      m_isNulls(nullptr)
{}

RetStatus BtreeDeltaDmlForCcindex::UpdateDeltaDmlRec(BtreeInfo btrInfo, Datum *values, bool *isNulls,
                                                     ItemPointer deltaHeapCtid, ItemPointer duplicateDeltaRec)
{
    if (unlikely(deltaHeapCtid == nullptr || duplicateDeltaRec == nullptr)) {
        storage_set_error(INDEX_ERROR_INPUT_PARAM_WRONG);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Illegal input parameters."));
        return DSTORE_FAIL;
    }
    *duplicateDeltaRec = INVALID_ITEM_POINTER;

    /* Create BtreeDeltaDmlForCcindex object for update DeltaDmlTable record */
    AutoMemCxtSwitch autoMemCxtSwitch(thrd->GetSessionMemoryCtx());
    BtreeDeltaDmlForCcindex *deltaIdx = DstoreNew(g_dstoreCurrentMemoryContext) BtreeDeltaDmlForCcindex(btrInfo);
    if (unlikely(deltaIdx == nullptr)) {
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to alloc size:%lu", sizeof(BtreeDeltaDmlForCcindex)));
        return DSTORE_FAIL;
    }

    if (STORAGE_FUNC_FAIL(thrd->GetActiveTransaction()->AllocTransactionSlot())) {
        return DSTORE_FAIL;
    }

    /* Step 1. We'll check all DeltaDmlTable record with the same key (original table's heapCtid + index keys). */
    PageId insPageId = INVALID_PAGE_ID;
    ErrLevel errLevel = DSTORE_DEBUG1;
    deltaIdx->m_duplicateErrLevel = DSTORE_DEBUG1;
    if (btrInfo.retryTimes == MAX_RETRY_UPDATE_DELTA_DML_INDEX_TIME - 1) {
        errLevel = DSTORE_ERROR;
        deltaIdx->m_duplicateErrLevel = DSTORE_ERROR;
    }
    deltaIdx->SetSearchingInfo(values, isNulls, deltaHeapCtid);
    RetStatus ret = deltaIdx->SearchForPreviousRecord();
    if (likely(deltaIdx->m_insertPageBuf != INVALID_BUFFER_DESC)) {
        insPageId = deltaIdx->m_insertPageBuf->GetPageId();
    }
    if (STORAGE_FUNC_FAIL(ret)) {
        errLevel = DSTORE_ERROR;
        goto CLEAR_AND_RETURN;
    }
    if (deltaIdx->m_duplicateHeapCtid != INVALID_ITEM_POINTER) {
        *duplicateDeltaRec = deltaIdx->m_duplicateHeapCtid;
        /* Duplicate found. We need to stop this round, try clear the expired record then insert again. */
        errLevel = DSTORE_ERROR;
        goto CLEAR_AND_RETURN;
    }

    /* Step 2. Insert the record */
    ret = deltaIdx->InsertDeltaDmlRec();

CLEAR_AND_RETURN:
    Xid xid = thrd->GetCurrentXid();
    bool insertSucc = (ret == DSTORE_SUCC) ? (deltaIdx->m_duplicateHeapCtid == INVALID_ITEM_POINTER) : false;
    ErrLog(errLevel, MODULE_INDEX,
        ErrMsg("[CCINDEX] %s:%d %s to insert indexTuple(%hu, %u, %hu) for deltaDmlRec(%hu, %u, %hu), %s"
        "orig heapCtid(%hu, %u, %hu), type = %hhd, snapshot csn %lu, xid(%d, %lu).",
        btrInfo.indexInfo->indexRelName, btrInfo.indexInfo->indexRelId, insertSucc ? "succ" : "fail",
        insPageId.m_fileId, insPageId.m_blockId, deltaIdx->m_insertOff,
        deltaHeapCtid->GetFileId(), deltaHeapCtid->GetBlockNum(), deltaHeapCtid->GetOffset(),
        (deltaIdx->m_duplicateHeapCtid != INVALID_ITEM_POINTER) ? "conflict found, " : "",
        deltaIdx->m_heapCtid.GetFileId(), deltaIdx->m_heapCtid.GetBlockNum(), deltaIdx->m_heapCtid.GetOffset(),
        static_cast<int8>(deltaIdx->m_operType), thrd->GetSnapShotCsn(),
        static_cast<int>(xid.m_zoneId), xid.m_logicSlotId));

    deltaIdx->Clear();
    delete deltaIdx;
    return ret;
}

RetStatus BtreeDeltaDmlForCcindex::CheckExistence(BtreeInfo btrInfo, Datum *values, bool *isNulls, ItemPointer deltaRec)
{
    if (unlikely(deltaRec == nullptr)) {
        storage_set_error(INDEX_ERROR_INPUT_PARAM_WRONG);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Illegal input parameters."));
        return DSTORE_FAIL;
    }
    *deltaRec = INVALID_ITEM_POINTER;

    /* Create BtreeDeltaDmlForCcindex object for searching DeltaDmlTable record */
    AutoMemCxtSwitch autoMemCxtSwitch(thrd->GetTopTransactionMemoryContext());
    BtreeDeltaDmlForCcindex *deltaIdx = DstoreNew(g_dstoreCurrentMemoryContext) BtreeDeltaDmlForCcindex(btrInfo);
    if (unlikely(deltaIdx == nullptr)) {
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to alloc size:%lu", sizeof(BtreeDeltaDmlForCcindex)));
        return DSTORE_FAIL;
    }
    deltaIdx->SetSearchingInfo(values, isNulls, nullptr);

    DmlOperationTypeForCcindex operType = DmlOperationTypeForCcindex::INVALID_TYPE;
    RetStatus ret = deltaIdx->SearchForPreviousRecord();
    if (STORAGE_FUNC_FAIL(ret)) {
        goto CLEAR_AND_RETURN;
    }

    if (deltaIdx->m_duplicateHeapCtid == INVALID_ITEM_POINTER || deltaIdx->m_insertOff == INVALID_ITEM_OFFSET_NUMBER) {
        goto RETURN_ERROR;
    }

    operType = static_cast<DmlOperationTypeForCcindex>(DATUM_GET_INT8(btrInfo.indexInfo->extraInfo));
    if (operType != deltaIdx->m_operType) {
        /* ret should still be SUCC because we've finished checking successfully. */
        goto RETURN_ERROR;
    }
    /* Target found. */
    *deltaRec = deltaIdx->m_duplicateHeapCtid;
    ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
        ErrMsg("[CCINDX] Found DeltaDmlTable record{%hu, %u} %hu for origHeap{%hu, %u} %hu with operType %hhd",
        deltaIdx->m_duplicateHeapCtid.GetFileId(), deltaIdx->m_duplicateHeapCtid.GetBlockNum(),
        deltaIdx->m_duplicateHeapCtid.GetOffset(), deltaIdx->m_heapCtid.GetFileId(), deltaIdx->m_heapCtid.GetBlockNum(),
        deltaIdx->m_heapCtid.GetOffset(), static_cast<int8>(deltaIdx->m_operType)));

CLEAR_AND_RETURN:
    deltaIdx->Clear();
    delete deltaIdx;
    return ret;

RETURN_ERROR:
    ErrLog(DSTORE_ERROR, MODULE_INDEX,
        ErrMsg("[CCINDX] Failed find record for orig heapCtid(%hu, %u, %hu) with operType %hhd in DeltaDmlTable",
        deltaIdx->m_heapCtid.GetFileId(), deltaIdx->m_heapCtid.GetBlockNum(), deltaIdx->m_heapCtid.GetOffset(),
        static_cast<int8>(deltaIdx->m_operType)));
    goto CLEAR_AND_RETURN;
}

void BtreeDeltaDmlForCcindex::SetSearchingInfo(Datum *values, bool *isNulls, ItemPointer delDmlHeapCtid)
{
    StorageAssert(values != nullptr);
    StorageAssert(isNulls != nullptr);

    m_values = values;
    m_isNulls = isNulls;
    /* For DeltaDmlTable update case, we'll have a delDmlHeapCtid for inserting the record */
    /* For Checking existing record case, we'll have a nullptr for delDmlHeapCtid */
    m_delDmlHeapCtid = (delDmlHeapCtid == nullptr) ? INVALID_ITEM_POINTER : *delDmlHeapCtid;

    /*
     * Extract useful messages from DeltaDmlTable tuple.
     * for LPI:
     *      DeltaDmlTable tuple values = (partid, heap_ctid, values, oper_type)
     *      DeltaDmlTable tuple isnull = (false, false, is_nulls, false)
     * for GPI:
     *      DeltaDmlTable tuple values = (heap_ctid, values, partid, oper_type)
     *      DeltaDmlTable tuple isnull = (false, is_nulls, false, false)
     * for regular index:
     *      DeltaDmlTable tuple values = (heap_ctid, values, oper_type)
     *      DeltaDmlTable tuple isnull = (false, is_nulls, false)
     */
    int currColNo = 0;
    if (m_isOrigIndexLpi) {
        /* For LPI, the first column of DeltaDmlTable is the table oid */
        StorageAssert(!isNulls[currColNo]);
        StorageAssert(DatumGetObjectId(values[currColNo] != DSTORE_INVALID_OID));
        currColNo++;
    }

    StorageAssert(!isNulls[currColNo]);
    m_heapCtid = ItemPointerData(DatumGetUInt64(values[currColNo++]));

    /* Opertype is always the last column of the DeltaDmlTable */
    int operColNo = m_indexInfo->indexAttrsNum;
    StorageAssert(operColNo <= INDEX_MAX_KEY_NUM);
    StorageAssert(operColNo - currColNo > 1); /* We should have at least one index column */
    m_operType = static_cast<DmlOperationTypeForCcindex>(values[operColNo - 1]);
}

RetStatus BtreeDeltaDmlForCcindex::SearchForPreviousRecord()
{
    StorageAssert(m_needCheckUnique);
    StorageAssert(m_insertTuple == nullptr);

    m_needDefferCheck = false;
    m_needRetrySearchBtree = true;

    /* Step 1. Form index tuple and prepare scan key */
    if (STORAGE_FUNC_FAIL(FormIndexTuple(m_values, m_isNulls, &m_delDmlHeapCtid, &m_insertTuple))) {
        return DSTORE_FAIL;
    }
    /* DeltaDml would check every single tuple for uniqueness including NULL. */
    StorageClearError();
    InitSearchingKeys();

    while (m_needRetrySearchBtree) {
        /* Step 2. Search and get the leaf page where the checking tuple should be on. */
        StorageAssert(m_insertPageBuf == INVALID_BUFFER_DESC);
        m_scanKeyValues.heapCtid = INVALID_ITEM_POINTER;
        m_isBoundValid = false;
        if (STORAGE_FUNC_FAIL(SearchBtreeForInsert())) {
            return DSTORE_FAIL;
        }
        StorageAssert(m_insertPageBuf != INVALID_BUFFER_DESC);
        UpdateCachedInsertionPageIdIfNeeded(m_insertPageBuf->GetPageId());

        /* Step 3. Check data conflict */
        /* Check if there's any duplicate for this insertion */
        if (STORAGE_FUNC_FAIL(CheckConflict())) {
            /* Something wrong when checking... */
            return DSTORE_FAIL;
        }

        if (m_satisfiedUnique) {
            /* No conflict found. Return and prepare to insert. */
            break;
        }

        if (!m_needRetrySearchBtree) {
            if (unlikely(m_duplicateTuple->IsPivot())) {
                BtrPage *currPage = static_cast<BtrPage *>(m_insertPageBuf->GetPage());
                storage_set_error(INDEX_ERROR_FAIL_BUILD_INDEX_FOR_PAGE_DAMAGED,
                                  currPage->GetSelfPageId().m_fileId, currPage->GetSelfPageId().m_blockId);
                ErrLog(DSTORE_ERROR, MODULE_INDEX,
                    ErrMsg("Duplicate on DeltaDmlIndex is pivot in(%s), segment(%hu, %u), "
                           "indexCtid (%hu, %u, %hu), tupleInfo(%u)"
                    BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
                    m_indexInfo->indexRelName, GetBtreeSmgr()->GetSegMetaPageId().m_fileId,
                    GetBtreeSmgr()->GetSegMetaPageId().m_blockId, m_insertPageBuf->GetPageId().m_fileId,
                    m_insertPageBuf->GetPageId().m_blockId, m_insertOff, m_duplicateTuple->m_info.m_info,
                    BTR_PAGE_HEADER_VAL(currPage), BTR_PAGE_LINK_AND_STATUS_VAL(currPage->GetLinkAndStatus())));
                return DSTORE_FAIL;
            }
            /* No need to retry means nothing blocks us but duplicate was found. */
            /* Record the duplication so that the outer caller can update it. */
            /* Note that it's OK to hit duplications. Just solve the conflict then. */
            bool isNull = true;
            m_indexInfo->extraInfo = m_duplicateTuple->GetAttr(m_indexInfo->indexAttrsNum,
                                                               m_indexInfo->attributes, &isNull);
            StorageAssert(!isNull);
            m_duplicateHeapCtid = m_duplicateTuple->GetHeapCtid();

            ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
                ErrMsg("[CCINDEX] SearchForPreviousRecord found ([ctid]{%d, %u}, %d) on %s{%hu, %u} %hu "
                "with delta heapCitd{%hu, %u} %hu, snapshot csn %lu",
                m_heapCtid.GetFileId(), m_heapCtid.GetBlockNum(), m_heapCtid.GetOffset(), m_indexInfo->indexRelName,
                m_insertPageBuf->GetPageId().m_fileId, m_insertPageBuf->GetPageId().m_blockId, m_insertOff,
                m_duplicateHeapCtid.GetFileId(), m_duplicateHeapCtid.GetBlockNum(), m_duplicateHeapCtid.GetOffset(),
                thrd->GetSnapShotCsn()));
        }
    }
    /* Note: we still HOLD AN EXCLUSIVE LOCK on m_insertPageBuf!!! */
    return DSTORE_SUCC;
}

void BtreeDeltaDmlForCcindex::InitSearchingKeys()
{
    /* An index of DeltaDmlTable must be an unique index */
    StorageAssert(m_needCheckUnique);

    /* Note: we do not skip unique check for null-including case here.
     * For normal cases, NULL is unequal to any value, including NULL. Thus if a tuple has null column, it must be
     * unique. But for DeltaDml index, unique check is not for a unique constraint, but for keeping data
     * consistence that there must exist one and only one record of any exacting heap record.
     * So in this situation, NULL is equal to NULL since they stand for the same record. */
    /* Btree::CompareKeyToTuple would instinctly consider NULL equals to NULL */

    /* Update scankey values using insertion index tuple to help searching for insertion location. */
    (void)UpdateScanKeyWithValues(m_insertTuple);
}

RetStatus BtreeDeltaDmlForCcindex::CheckConflict()
{
    StorageAssert(m_needCheckUnique);
    m_readyForInsert = false;
    m_needRetrySearchBtree = false;

    ErrLog(DSTORE_DEBUG1, MODULE_INDEX, ErrMsg("[CCINDEX] Start CheckUnique for insertion, on page {%d, %u} "
        "DmlHeapCitd {%d, %u} %hu, keyHeapCtid {%hu, %u} %hu",
        m_insertPageBuf->GetPageId().m_fileId, m_insertPageBuf->GetPageId().m_blockId,
        m_delDmlHeapCtid.GetFileId(), m_delDmlHeapCtid.GetBlockNum(), m_delDmlHeapCtid.GetOffset(),
        m_heapCtid.GetFileId(), m_heapCtid.GetBlockNum(), m_heapCtid.GetOffset()));

    Xid waitXid = INVALID_XID;
    /* OperType is in the including column that doesn't take a part in sorting */
    if (STORAGE_FUNC_FAIL(BtreeInsert::CheckUnique(&waitXid))) {
        /* Something wrong when checking... */
        return DSTORE_FAIL;
    }
    if (unlikely(waitXid != INVALID_XID)) {
        /* Have to unlock curren page before waiting for other transaction */
        PageId pageId = m_insertPageBuf->GetPageId();
        m_bufMgr->UnlockAndRelease(m_insertPageBuf);
        m_insertPageBuf = INVALID_BUFFER_DESC;
        if (STORAGE_FUNC_FAIL(WaitForTxnEndIfNeeded(pageId, waitXid))) {
            /* Do not retry if failed for waiting in-progress transaction */
            StorageAssert(!m_needRetrySearchBtree);
            return DSTORE_FAIL;
        }
        m_needRetrySearchBtree = true;
        return DSTORE_SUCC;
    }
    m_readyForInsert = m_satisfiedUnique;
    m_scanKeyValues.heapCtid = m_insertTuple->GetHeapCtid();
    return DSTORE_SUCC;
}

RetStatus BtreeDeltaDmlForCcindex::InsertDeltaDmlRec()
{
    StorageAssert(m_insertPageBuf != INVALID_BUFFER_DESC);
    StorageAssert(m_insertTuple != nullptr);
    StorageAssert(m_needCheckUnique);

    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr \"ActiveTransaction\".", __FUNCTION__));
        return DSTORE_FAIL;
    }
    m_cid = transaction->GetCurCid();

    m_insertTuple->SetTdId(INVALID_TD_SLOT);
    StorageAssert(m_scanKeyValues.heapCtid == m_insertTuple->GetHeapCtid());

    if (STORAGE_FUNC_FAIL(BtreeInsert::FindInsertLoc())) {
        return DSTORE_FAIL;
    }
    PageId insertPageId = m_insertPageBuf->GetPageId();
    RetStatus ret = BtreeInsert::AddTupleToLeaf();
    /* In any case, m_insertPageBuf should have been unlocked and released after AddTupleToLeaf. */
    StorageAssert(m_insertPageBuf == INVALID_BUFFER_DESC);

    ErrLog(DSTORE_DEBUG1, MODULE_INDEX, ErrMsg("[CCINDEX] insert deltaDmlRec on {%hu, %u} %hu deltaDmlheapCitd "
        "{%hu, %u} %hu, origHeapCtid{%hu, %u} %hu, xid(%d, %lu), snapshot csn %lu",
        insertPageId.m_fileId, insertPageId.m_blockId, m_insertOff,
        m_delDmlHeapCtid.GetFileId(), m_delDmlHeapCtid.GetBlockNum(), m_delDmlHeapCtid.GetOffset(),
        m_heapCtid.GetFileId(), m_heapCtid.GetBlockNum(), m_heapCtid.GetOffset(),
        static_cast<int32>(thrd->GetCurrentXid().m_zoneId), thrd->GetCurrentXid().m_logicSlotId,
        thrd->GetSnapShotCsn()));
    return ret;
}

} /* namespace DSTORE */