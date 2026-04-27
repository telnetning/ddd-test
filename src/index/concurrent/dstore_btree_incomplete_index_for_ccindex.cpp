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
 * dstore_btree_incomplete_index_for_ccindex.cpp
 *
 * IDENTIFICATION
 *        storage/src/index/concurrent/dstore_btree_incomplete_index_for_ccindex.cpp
 *
 * ---------------------------------------------------------------------------------------*/

#include "index/concurrent/dstore_btree_incomplete_index_for_ccindex.h"
#include "transaction/dstore_transaction.h"
#include "index/dstore_btree_wal.h"

namespace DSTORE {

IncompleteBtreeDeleteForCcindex::IncompleteBtreeDeleteForCcindex(StorageRelation indexRel, IndexInfo *indexInfo,
                                                                 ScanKey scanKey, bool forMerge)
    : BtreeDelete(indexRel, indexInfo, scanKey), m_needSkip(false), m_needInsertForDel(false)
{
    if (forMerge) {
        m_deleteType = DeleteType::DELTA_MERGE_DELETE;
    } else {
        m_deleteType = DeleteType::CONCURRENT_DML_DELETE;
    }
}

RetStatus IncompleteBtreeDeleteForCcindex::MergeDeltaDeletionToBtree(CcindexBtrBuildHandler *handler,
    Datum *values, bool *isnull, ItemPointer heapCtid)
{
    /* Check index status */
    if (unlikely(handler->m_indexInfo->btrIdxStatus != BtrCcidxStatus::WRITE_ONLY_INDEX)) {
        storage_set_error(INDEX_ERROR_UNMATCHED_CCINDEX_STATUS,
                          handler->m_indexInfo->btrIdxStatus, BtrCcidxStatus::WRITE_ONLY_INDEX);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[CCINDEX] InsertTupleForDeltaDmlRec can only be called for ccindex "
            "phase3. current btrIdxStatus = %hhu", static_cast<uint8>(handler->m_indexInfo->btrIdxStatus)));
        return DSTORE_FAIL;
    }

    /* Create BtreeInsertForCcindex object for insertion */
    AutoMemCxtSwitch autoMemCxtSwitch(thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_QUERY));
    IncompleteBtreeDeleteForCcindex *incompleteCcidx =
        DstoreNew(g_dstoreCurrentMemoryContext) IncompleteBtreeDeleteForCcindex(handler->m_indexRel,
                                                                                handler->m_indexInfo,
                                                                                handler->m_scanKeyValues.scankeys,
                                                                                true);
    if (unlikely(incompleteCcidx == nullptr)) {
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to alloc size:%lu", sizeof(IncompleteBtreeDeleteForCcindex)));
        return DSTORE_FAIL;
    }

    ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
        ErrMsg("[CCINDEX] Start to merge delete for heapCtid{%hu, %u} %hu, in xid(%d, %lu), snapshotCsn:%lu",
        heapCtid->GetFileId(), heapCtid->GetBlockNum(), heapCtid->GetOffset(),
        static_cast<int32>(thrd->GetCurrentXid().m_zoneId), thrd->GetCurrentXid().m_logicSlotId,
        thrd->GetSnapShotCsn()));

    incompleteCcidx->m_needRecordUndo = false;
    RetStatus ret = incompleteCcidx->DeleteTuple(values, isnull, heapCtid);
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
            ErrMsg("[CCINDEX] Failed to merge delete for orig heapCtid(%hu, %u, %hu), in xid(%d, %lu), errMsg:%s",
            heapCtid->GetFileId(), heapCtid->GetBlockNum(), heapCtid->GetOffset(),
            static_cast<int32>(thrd->GetCurrentXid().m_zoneId), thrd->GetCurrentXid().m_logicSlotId,
            thrd->GetErrorMessage()));
    } else {
        if (incompleteCcidx->m_needSkip) {
            handler->m_deltaTupleSkipped++;
        } else {
            handler->m_deltaTupleDelete++;
        }
    }

    delete incompleteCcidx;
    return ret;
}

RetStatus IncompleteBtreeDeleteForCcindex::DeleteTuple(Datum *values, bool *isnull, ItemPointer ctid,
                                                       UNUSE_PARAM CommandId cid)
{
    RetStatus retStatus = DSTORE_FAIL;
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr \"ActiveTransaction\".", __FUNCTION__));
        return DSTORE_FAIL;
    }
    m_cid = transaction->GetCurCid();
    Xid waitXid = INVALID_XID;

    AutoMemCxtSwitch autoMemCxtSwitch(thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_QUERY));

    if (STORAGE_FUNC_FAIL(retStatus = FormIndexTuple(values, isnull, ctid, &m_searchingTarget))) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to form index tuple for ccindex \"%s\".",
            m_indexInfo->indexRelName));
        goto EXIT;
    }

    StorageClearError();
    UNUSED_VARIABLE(UpdateScanKeyWithValues(m_searchingTarget));
    ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
           ErrMsg("Start to delete index tuple from ccindex(p3) with heap ctid = ({%hu, %u}, %hu)", ctid->GetFileId(),
                  ctid->GetBlockNum(), ctid->GetOffset()));

RETRY_DELETION:
    m_needRetrySearchBtree = false;
    m_needSkip = false;
    m_delTuple = nullptr;
    m_delOffset = INVALID_ITEM_OFFSET_NUMBER;
    /* Step 1. Find the first page containing this key */
    if (STORAGE_FUNC_FAIL(BtreeSplit::SearchBtree(&m_pagePayload.buffDesc, false)) ||
        m_pagePayload.buffDesc == INVALID_BUFFER_DESC) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to search btree for ccindex(%s:%u).",
            m_indexInfo->indexRelName, m_indexInfo->indexRelId));
        goto EXIT;
    }
    m_pagePayload.InitByBuffDesc(m_pagePayload.GetBuffDesc());
    StorageAssert(m_pagePayload.GetLinkAndStatus()->TestType(BtrPageType::LEAF_PAGE));

    /* Step 2. Find the deleting target on leaf page */
    m_delOffset = BinarySearchOnPage(m_pagePayload.GetPage(), false);
    if (unlikely(StorageGetErrorCode() == COMMON_ERROR_FUNCTION_RETURN_NULL)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
                ErrMsg("Failed to binary search btrPage(%d, %u) since compare function returns null.",
                m_pagePayload.GetPageId().m_fileId, m_pagePayload.GetPageId().m_blockId));
        m_pagePayload.Drop(m_bufMgr, false);
        goto EXIT;
    }
    /* looking for itup with the same ctid starting from m_delOffset */
    waitXid = INVALID_XID;
    if (m_deleteType == DeleteType::CONCURRENT_DML_DELETE) {
        retStatus = FindDeleteLocForConcurrentDml(&waitXid);
    } else if (m_deleteType == DeleteType::DELTA_MERGE_DELETE) {
        retStatus = FindDeleteLocForMergeDelta(&waitXid);
    } else {
        storage_set_error(INDEX_ERROR_INPUT_PARAM_WRONG);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Unrecognized deleteType:%hhu of IncompleteBtreeDeleteForCcindex",
                                                  static_cast<uint8>(m_deleteType)));
        goto EXIT;
    }
    if (STORAGE_FUNC_FAIL(retStatus)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to find delete location for ccindex(%s:%u).",
            m_indexInfo->indexRelName, m_indexInfo->indexRelId));
        goto EXIT;
    }

    /* Step 3. Wait for in-progress transaction if any */
    if (waitXid != INVALID_XID) {
        /* Need to unlock the target page before waiting transaction end */
        PageId currPageId = m_pagePayload.GetPageId();
        m_pagePayload.Drop(m_bufMgr);
        if (STORAGE_FUNC_FAIL(retStatus = WaitForTxnEndIfNeeded(currPageId, waitXid))) {
            /* Sth. wrong when waiting for transaction end. Give up retry and return fail */
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to wait running transaction {%d %lu} end"
                    "for ccindex \"%s\".",
                    static_cast<int32>(waitXid.m_zoneId), waitXid.m_logicSlotId, m_indexInfo->indexRelName));
            goto EXIT;
        }
        /* For now, the blocking transaction finished. */
        /* Need to search from root again. retry search */
        goto RETRY_DELETION;
    }

    StorageAssert(!m_needRetrySearchBtree);
    if (m_needSkip) {
        StorageAssert(m_deleteType == DeleteType::DELTA_MERGE_DELETE);
        ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
            ErrMsg("[CCINDX] Skip merging in (%s):({%hu, %u} %hu) with heap ctid ({%hu, %u} %hu), segment(%hu, %u), "
            "snapshot(%lu), xid(%u, %lu), tableOid(%u) " BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
            m_indexInfo->indexRelName, m_pagePayload.GetPageId().m_fileId, m_pagePayload.GetPageId().m_blockId,
            m_delOffset, ctid->GetFileId(), ctid->GetBlockNum(), ctid->GetOffset(),
            GetBtreeSmgr()->GetSegMetaPageId().m_fileId, GetBtreeSmgr()->GetSegMetaPageId().m_blockId,
            thrd->GetSnapShotCsn(), static_cast<uint32>(thrd->GetCurrentXid().m_zoneId),
            thrd->GetCurrentXid().m_logicSlotId,
            (m_indexInfo->relKind == static_cast<char>(SYS_RELKIND_GLOBAL_INDEX) ?
                m_searchingTarget->GetTableOid(m_indexInfo) : 0),
            BTR_PAGE_HEADER_VAL(m_pagePayload.GetPage()),
            BTR_PAGE_LINK_AND_STATUS_VAL(m_pagePayload.GetLinkAndStatus())));
        m_pagePayload.Drop(m_bufMgr);
        retStatus = DSTORE_SUCC;
        goto EXIT;
    }
    if (m_needInsertForDel) {
        StorageAssert(m_deleteType == DeleteType::CONCURRENT_DML_DELETE);
#ifdef DSTORE_USE_ASSERT_CHECKING
        ErrLog(DSTORE_LOG, MODULE_INDEX,
            ErrMsg("[CCINDEX] Missing deleting target, going to insert4delete in (%s:%d) btrPage(%hu, %u, %hu) with "
            "heapCtid(%hu, %u, %hu), segment(%hu, %u), snapshot(%lu), xid(%u, %lu), tableOid(%u) "
            BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
            m_indexInfo->indexRelName, m_indexInfo->indexRelId,
            m_pagePayload.GetPageId().m_fileId, m_pagePayload.GetPageId().m_blockId, m_delOffset,
            ctid->GetFileId(), ctid->GetBlockNum(), ctid->GetOffset(),
            GetBtreeSmgr()->GetSegMetaPageId().m_fileId, GetBtreeSmgr()->GetSegMetaPageId().m_blockId,
            thrd->GetSnapShotCsn(), static_cast<uint32>(thrd->GetCurrentXid().m_zoneId),
            thrd->GetCurrentXid().m_logicSlotId,
            (m_indexInfo->relKind == static_cast<char>(SYS_RELKIND_GLOBAL_INDEX) ?
                m_searchingTarget->GetTableOid(m_indexInfo) : 0),
            BTR_PAGE_HEADER_VAL(m_pagePayload.GetPage()),
            BTR_PAGE_LINK_AND_STATUS_VAL(m_pagePayload.GetLinkAndStatus())));
#endif
        /* NOTE: We still hold a EXCLUSIVE_LOCK on the target page. Deal with the page BuffDesc carefully!!! */
        if (STORAGE_FUNC_FAIL(retStatus = IncompleteBtreeInsertForCcindex::InsertTuple4Delete(this))) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX,
                ErrMsg("[CCINDEX] Failed to do InsertTuple4Delete for ccindex(%s:%d), heapCtid(%hu, %u, %hu).",
                m_indexInfo->indexRelName, m_indexInfo->indexRelId,
                ctid->GetFileId(), ctid->GetBlockNum(), ctid->GetOffset()));
        }
        m_pagePayload.Drop(m_bufMgr);
        goto EXIT;
    }

    /* Step 4. Delete the target */
    StorageAssert(!m_needInsertForDel);
    StorageAssert(m_pagePayload.GetBuffDesc() != INVALID_BUFFER_DESC);
    StorageAssert(m_delOffset != INVALID_ITEM_OFFSET_NUMBER);
    retStatus = DeleteFromLeaf();
    if (m_pagePayload.GetBuffDesc() != INVALID_BUFFER_DESC) {
        m_pagePayload.Drop(m_bufMgr);
    }
    /* fail on td allocation, wait for any transaction on page to finish, then try to find the tuple again. */
    if (STORAGE_FUNC_FAIL(retStatus)) {
        if (m_tdContext.NeedRetryAllocTd() && STORAGE_FUNC_FAIL(WaitTxnEndForTdRealloc())) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to do WaitTxnEndForTdRealloc for ccindex \"%s\".",
                m_indexInfo->indexRelName));
            goto EXIT;
        }
        if (m_needRetrySearchBtree) {
            goto RETRY_DELETION;
        }
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to delete from leaf for ccindex \"%s\".",
                m_indexInfo->indexRelName));
    }

EXIT:
    ClearStack();
    DstorePfreeExt(m_searchingTarget);
    return retStatus;
}

RetStatus IncompleteBtreeDeleteForCcindex::FindDeleteLocForMergeDelta(Xid *waitXid)
{
    int cmpRet = 0;
    *waitXid = INVALID_XID;
    ItemPointerData heapCtid = m_searchingTarget->GetHeapCtid();
    /* Scan over all equal tuples, looking for m_delTuple. */
    for (;;) {
        /* Step 1. Check offset number, step right if there's more to check */
        if (STORAGE_FUNC_FAIL(StepRightForDeleteLocIfNeeded())) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to step right to find delete location when merging"
                "delta table for ccindex \"%s\".", m_indexInfo->indexRelName));
            return DSTORE_FAIL;
        }
        if (m_needSkip) {
            break;
        }
        /* Step 2. Check status and keys of current item */
        ItemId *curItemId = m_pagePayload.GetPage()->GetItemIdPtr(m_delOffset);
        if (!curItemId->IsNormal()) {
            m_delOffset++;
            continue;
        }
        BtrPageLinkAndStatus *linkAndStatus = m_pagePayload.GetLinkAndStatus();
        cmpRet = CompareKeyToTuple(m_pagePayload.GetPage(), linkAndStatus, m_delOffset,
                                   linkAndStatus->TestType(BtrPageType::INTERNAL_PAGE));
        if (unlikely(StorageGetErrorCode() == COMMON_ERROR_FUNCTION_RETURN_NULL)) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to compare tuple since compare function returns null."));
            m_pagePayload.Drop(m_bufMgr);
            return DSTORE_FAIL;
        }
        if (cmpRet != 0) {
            /* We've traversed all the equal tuples, but we haven't found the target to delete */
            ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
                   ErrMsg("[CCINDEX] Failed to find the target to delete heapCtid(%hu, %u, %hu).",
                          heapCtid.GetFileId(), heapCtid.GetBlockNum(), heapCtid.GetOffset()));
            storage_set_error(INDEX_ERROR_FAILED_TO_FIND_DELETING_TARGET);
            /* Since all record in DeltaDmlTable are visible to all, there's no need to insert a deleted tuple that
             * no one could see it any longer. Skip it would be better */
            m_needSkip = true;
            break;
        }

        IndexTuple *curItup = m_pagePayload.GetPage()->GetIndexTuple(curItemId);
        if (curItup->GetHeapCtid() != heapCtid) {
            /* Same key but different heap ctid, go check next tuple */
            m_delOffset++;
            continue;
        }
        /* We've found the exact index tuple */

        /* Step 3. Check if current item is our target */
        /*
         * We've find the index tuple with the same key pointing to the same heapCtid.
         * But it may not be our target! The index tuple on page may be writen by one of the following operations:
         * 1. It was writen when build index structure. It must be committed and have not deleted flag (since we won't
         *    write deleted tuple when building).
         * 2. It was writen by concurrent DML during merging phase of ccindex. It will be both acceptable to have a
         *    deleted flag or not. since our deletion is already committed and visible to all concurrent DML during
         *    merging phase. It is possible for concurrent DML insert the same key at the same heapCtid, and is
         *    possible to delete it then. Thus if the tuple on page has any valid transaction information
         *    (TD ID/TD Status) that indicates the tuple was writen by concurrent DML, we can just skip deleting since
         *    the deleting is already committed and no can should see it anymore.
         *    **ATTENTION: should finish to check all tuples with the same heapCtid in case the built one is still on.
         * 3. This is an impossible situation that it was writen when merging the DeltaDmlTable record to the btree.
         *    because the DeltaDmlTable would keep only one record for each heapCtid and it is guarantted by an unique
         *    index on the column of heapCtid on DeltaDmlTable. Once we found a DeltaDmlTable merging tuple that the
         *    BtrCcidxStatus of the tuple is WRITE_ONLY_INDEX with no transaction information, there must be something
         *    wrong for the ccindex. Abort the merging and return fail to stop building ccindex and drop it.
         */
        if (!curItup->IsDeleted()) {
            if (curItup->GetCcindexStatus() == BtrCcidxStatus::IN_BUILDING_INDEX) {
                /* The DeltaDmlTable can only delete tuples by building phase. */
                /* By here, the index tuple is matched with scan key and it is normal */
                m_needSkip = false;
                break;
            }
            /* The tuple must be inserted by concurrent DML of phase 3 that is later than the DeltaDml record generated.
             * The old version should have been deleted by the current DML，skip merging after checking all keys if
             * no more same-key tuple found. */
            m_needSkip = true;
            m_delOffset++;
            continue;
        }

        /* By here, the index tuple is matched with scan key but deleted. */
        if (curItup->GetCcindexStatus() == BtrCcidxStatus::WRITE_ONLY_INDEX) {
            if (curItup->GetTdId() == INVALID_TD_SLOT) {
                /* We have no knowledge to tell whether the tuple is from merging result or concurrent dml result */
                ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
                    ErrMsg("[CCINDX] FindDeleteLoc targed has been deleted before merge, offset(%hu) "
                    BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT, m_delOffset,
                    BTR_PAGE_HEADER_VAL(m_pagePayload.GetPage()),
                    BTR_PAGE_LINK_AND_STATUS_VAL(m_pagePayload.GetLinkAndStatus())));
            }
            /* But we're sure that it's not our target, keep checking */
            m_delOffset++;
            continue;
        }

        /* Concurrent DML would chage the m_ccindexStatus when deleting. It is impossible that a tuple was deleted
         * during building. */
        StorageAssert(curItup->GetCcindexStatus() == BtrCcidxStatus::IN_BUILDING_INDEX);
        storage_set_error(INDEX_ERROR_FAILED_TO_FIND_DELETING_TARGET);
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
            ErrMsg("[CCINDX] FindDeleteLoc impossible case for merge IN_BUILDING_INDEX tuple deleted, offset(%hu) "
            "heapCtid(%hu, %u, %hu) BtrPage:" BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
            m_delOffset, heapCtid.GetFileId(), heapCtid.GetBlockNum(), heapCtid.GetOffset(),
            BTR_PAGE_HEADER_VAL(m_pagePayload.GetPage()),
            BTR_PAGE_LINK_AND_STATUS_VAL(m_pagePayload.GetLinkAndStatus())));
        return DSTORE_FAIL;
    }

#ifdef DSTORE_USE_ASSERT_CHECKING
    Oid tableOid = m_indexInfo->relKind == static_cast<char>(SYS_RELKIND_GLOBAL_INDEX) ?
                   m_searchingTarget->GetTableOid(m_indexInfo) : DSTORE_INVALID_OID;
    ErrLevel errLevel = m_needSkip ? DSTORE_LOG :DSTORE_DEBUG1;
    ErrLog(errLevel, MODULE_INDEX,
        ErrMsg("[CCINDX] FindDeleteLoc %s target for merge in (%s:%d) with heapCtid(%hu, %u, %hu), "
                "snapshot(%lu), xid(%u, %lu), tableOid(%u) " BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
        m_needSkip ? "skip" : "found", m_indexInfo->indexRelName, m_indexInfo->indexRelId,
        heapCtid.GetFileId(), heapCtid.GetBlockNum(), heapCtid.GetOffset(), thrd->GetSnapShotCsn(),
        static_cast<uint32>(thrd->GetCurrentXid().m_zoneId), thrd->GetCurrentXid().m_logicSlotId, tableOid,
        BTR_PAGE_HEADER_VAL(m_pagePayload.GetPage()), BTR_PAGE_LINK_AND_STATUS_VAL(m_pagePayload.GetLinkAndStatus())));
#endif

    return DSTORE_SUCC;
}


RetStatus IncompleteBtreeDeleteForCcindex::FindDeleteLocForConcurrentDml(Xid *waitXid)
{
    int cmpRet = 0;
    *waitXid = INVALID_XID;
    for (;;) {
        /* Step 1. Check offset number, step right if there's more to check */
        if (STORAGE_FUNC_FAIL(StepRightForDeleteLocIfNeeded())) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to step right to find delete location when dealing with"
                "concurrent DML for ccindex \"%s\".", m_indexInfo->indexRelName));
            return DSTORE_FAIL;
        }
        if (m_needInsertForDel) {
            /* We've reach the end of leaf but no deleting target has been found. The insert may be on DeltaDmlTable
             * and has not been merged yet. Need to do Insert4Delete later */
            break;
        }
        /* Step 2. Check status and keys of current item */
        ItemId *curItemId = m_pagePayload.GetPage()->GetItemIdPtr(m_delOffset);
        if (curItemId->IsNormal()) {
            cmpRet = CompareKeyToTuple(m_pagePayload.GetPage(), m_pagePayload.GetLinkAndStatus(), m_delOffset,
                                       m_pagePayload.GetLinkAndStatus()->TestType(BtrPageType::INTERNAL_PAGE));
            if (unlikely(StorageGetErrorCode() == COMMON_ERROR_FUNCTION_RETURN_NULL)) {
                ErrLog(DSTORE_ERROR, MODULE_INDEX,
                    ErrMsg("Failed to compare tuple since compare function returns null."));
                m_pagePayload.Drop(m_bufMgr);
                return DSTORE_FAIL;
            }
            if (cmpRet != 0) {
                /* we've traversed all the equal tuples, but we haven't found the target to delete. The insert may be
                 * on DeltaDmlTable and has not been merged yet. Need to do Insert4Delete later */
                m_needInsertForDel = true;
                break;
            }

            IndexTuple *curItup = m_pagePayload.GetPage()->GetIndexTuple(curItemId);
            if (curItup->GetHeapCtid() != m_searchingTarget->GetHeapCtid()) {
                /* Same key but different heap ctid, go check next tuple */
                m_delOffset++;
                continue;
            }

            /* The index tuple is matched with scan key and it is normal */
            if (!curItup->IsDeleted()) {
                /* We've found the exact index tuple */
                break;
            }

            /* The index tuple is matched with scan key, but it is deleted or deleting.
            * Check if the deleting is visible to us */
            bool visible = CheckTupleVisibility(m_pagePayload.GetPage(), m_delOffset, waitXid);
            if (!visible) {
                /* The deletion has not been committed. Wait it to commit then re-check. */
                StorageAssert(*waitXid != INVALID_XID);
                break;
            }
        }
        /* Go on check next */
        m_delOffset++;
    }
    return DSTORE_SUCC;
}

RetStatus IncompleteBtreeDeleteForCcindex::StepRightForDeleteLocIfNeeded()
{
    while (m_delOffset > m_pagePayload.GetPage()->GetMaxOffset()) {
        /* By here, We need to advance to next non-dead page to continue checking */
        if (m_pagePayload.GetLinkAndStatus()->IsRightmost() ||
            CompareKeyToTuple(m_pagePayload.GetPage(), m_pagePayload.GetLinkAndStatus(), BTREE_PAGE_HIKEY,
                              m_pagePayload.GetLinkAndStatus()->TestType(BtrPageType::INTERNAL_PAGE)) < 0) {
            storage_set_error(INDEX_ERROR_FAILED_TO_FIND_DELETING_TARGET);
            if (m_deleteType == DeleteType::CONCURRENT_DML_DELETE) {
                m_needInsertForDel = true;
            } else {
                StorageAssert(m_deleteType == DeleteType::DELTA_MERGE_DELETE);
                m_needSkip = true;
            }
            break;
        }
        if (STORAGE_FUNC_FAIL(StepRightWhenFindDelLoc())) {
            /* No more mached tuple. Failed to find deleting target */
            storage_set_error(INDEX_ERROR_MOVE_END, m_indexInfo->indexRelName);
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[CCINDEX] Failed to step right when find the delete location."));
            return DSTORE_FAIL;
        }
        /* We've stepped to the right page, reset m_delOffset */
        m_delOffset = m_pagePayload.GetLinkAndStatus()->GetFirstDataOffset();
    }
    return DSTORE_SUCC;
}

IncompleteBtreeInsertForCcindex::IncompleteBtreeInsertForCcindex(StorageRelation indexRel, IndexInfo *indexInfo,
                                                                 ScanKey scanKey, bool forMerge)
    : BtreeInsert(indexRel, indexInfo, scanKey), m_needSkip(false), m_needEraseIns4DelFlag(false)
{
    if (forMerge) {
        m_insertType = InsertType::DELTA_MERGE_INSERT;
        /* The optimization of last-insert-page-cache does no good for this scenario because no real index is sorted by
         * heapCtid nor tableOid as the very first key. */
        m_useInsertPageCache = false;
    } else {
        m_insertType = InsertType::CONCURRENT_DML_INSERT;
    }
    /* Clear duplicate info before insert */
    m_indexInfo->extraInfo = UInt64GetDatum(INVALID_ITEM_POINTER.m_placeHolder);
}

RetStatus IncompleteBtreeInsertForCcindex::MergeDeltaInsertionToBtree(CcindexBtrBuildHandler *handler,
    Datum *values, bool *isNulls, ItemPointer heapCtid)
{
    /* Check index status */
    if (unlikely(handler->m_indexInfo->btrIdxStatus != BtrCcidxStatus::WRITE_ONLY_INDEX)) {
        storage_set_error(INDEX_ERROR_UNMATCHED_CCINDEX_STATUS,
                          handler->m_indexInfo->btrIdxStatus, BtrCcidxStatus::WRITE_ONLY_INDEX);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[CCINDEX] InsertTupleForDeltaDmlRec can only be called for ccindex "
            "phase3. current btrIdxStatus = %hhu", static_cast<uint8>(handler->m_indexInfo->btrIdxStatus)));
        return DSTORE_FAIL;
    }

    /* Create BtreeInsertForCcindex object for insertion */
    AutoMemCxtSwitch autoMemCxtSwitch(thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_QUERY));
    IncompleteBtreeInsertForCcindex *incompleteCcidx =
        DstoreNew(g_dstoreCurrentMemoryContext) IncompleteBtreeInsertForCcindex(handler->m_indexRel,
                                                                                handler->m_indexInfo,
                                                                                handler->m_scanKeyValues.scankeys,
                                                                                true);
    if (unlikely(incompleteCcidx == nullptr)) {
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to alloc size:%lu", sizeof(IncompleteBtreeInsertForCcindex)));
        return DSTORE_FAIL;
    }

    ErrLevel errLevel = DSTORE_DEBUG1;
    incompleteCcidx->m_needRecordUndo = false;
    RetStatus ret = incompleteCcidx->InsertTuple(values, isNulls, heapCtid);
    if (STORAGE_FUNC_SUCC(ret)) {
        if (incompleteCcidx->m_needSkip) {
            handler->m_deltaTupleSkipped++;
        } else {
            handler->m_deltaTupleInsert++;
        }
    } else {
        errLevel = DSTORE_ERROR;
#ifndef UT
        if (thrd->GetErrorCode() == INDEX_ERROR_INSERT_UNIQUE_CHECK) {
            AutoMemCxtSwitch autoMemSwitch{thrd->GetSessionMemoryCtx()};
            handler->GetIndexBuildInfo()->duplicateTuple = incompleteCcidx->m_insertTuple->Copy();
        }
#endif
    }
    ErrLog(errLevel, MODULE_INDEX,
        ErrMsg("[CCINDEX] %s to merge insert for heapCtid(%hu, %u, %hu), in xid(%d, %lu), snapshotCsn:%lu. %s",
        STORAGE_FUNC_FAIL(ret) ? "Failed" : "Succeed",
        heapCtid->GetFileId(), heapCtid->GetBlockNum(), heapCtid->GetOffset(),
        static_cast<int32>(thrd->GetCurrentXid().m_zoneId), thrd->GetCurrentXid().m_logicSlotId,
        thrd->GetSnapShotCsn(), STORAGE_FUNC_FAIL(ret) ? thrd->GetErrorMessage() : ""));

    incompleteCcidx->Clear();
    delete incompleteCcidx;
    return ret;
}

RetStatus IncompleteBtreeInsertForCcindex::InsertTuple(Datum *values, bool *isnull, ItemPointer heapCtid,
                                                       bool *satisfiesUnique, UNUSE_PARAM CommandId cid)
{
    StorageAssert(m_insertType == InsertType::DELTA_MERGE_INSERT);

    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr \"ActiveTransaction\".", __FUNCTION__));
        return DSTORE_FAIL;
    }
    m_cid = transaction->GetCurCid();

    if (STORAGE_FUNC_FAIL(thrd->GetActiveTransaction()->AllocTransactionSlot())) {
        return DSTORE_FAIL;
    }

    m_needDefferCheck = false;
    m_needRetrySearchBtree = true;

    /* Step 1. Form an Index Tuple */
    if (STORAGE_FUNC_FAIL(FormIndexTuple(values, isnull, heapCtid, &m_insertTuple))) {
        return DSTORE_FAIL;
    }
    /* Step 2. Init Scankeys for searching Insert location */
    StorageClearError();
    InitSearchingKeys();

    /* Step 3. Do searching and checking for Inserting page in a loop.
     *         We may need to retry this step for several times if concurrent transaction is in progress */
    while (m_needRetrySearchBtree) {
        StorageAssert(m_insertPageBuf == INVALID_BUFFER_DESC);
        m_isBoundValid = false;
        if (m_needCheckUnique) {
            /* Clear heapCtid to find the first key with the same key values */
            m_scanKeyValues.heapCtid = INVALID_ITEM_POINTER;
        }
        if (STORAGE_FUNC_FAIL(SearchBtreeForInsert()) || m_insertPageBuf == INVALID_BUFFER_DESC) {
            return DSTORE_FAIL;
        }
        m_needRetrySearchBtree = false;
        UpdateCachedInsertionPageIdIfNeeded(m_insertPageBuf->GetPageId());
        /* Step 3. Check data conflict */
        /* Check if there's any duplicate for this insertion */
        if (STORAGE_FUNC_FAIL(IncompleteBtreeInsertForCcindex::CheckConflict())) {
            /* Something wrong when checking... */
            return DSTORE_FAIL;
        }
        if (m_needRetrySearchBtree) {
            StorageAssert(m_insertPageBuf == INVALID_BUFFER_DESC);
            continue;
        }
        if (satisfiesUnique != nullptr) {
            *satisfiesUnique = m_satisfiedUnique;
        }

        /* Step 4. We may need to skip insertion for some cases */
        if (m_needSkip) {
            StorageAssert(thrd->GetErrorCode() == INDEX_ERROR_DUPLICATE_TO_THE_SAME_RECORD);
            if (m_needEraseIns4DelFlag) {
                EraseIns4DelFlag();
            }
            /* Insertion Failed for duplicate found but they point to the same heap record. It is acceptable that an
             * insertion is both on the btree structure and in the deltaDmlTable. Treat it as success. */
            return DSTORE_SUCC;
        }

        /* Step 4. Find the inserting offset on the page */
        if (unlikely(!m_readyForInsert) || STORAGE_FUNC_FAIL(FindInsertLoc())) {
            return DSTORE_FAIL;
        }

        /* Step 5. Update the page for the new tuple */
        StorageAssert(!m_needRetrySearchBtree);
        if (STORAGE_FUNC_FAIL(BtreeInsert::AddTupleToLeaf()) && m_needRetrySearchBtree) {
            continue;
        }
    }
    /* Note: we still HOLD AN EXCLUSIVE LOCK on m_insertPageBuf!!! */
    return DSTORE_SUCC;
}

void IncompleteBtreeInsertForCcindex::InitSearchingKeys()
{
    /* Note: we do not skip unique check for null-including case here.
     * For normal cases, NULL is unequal to any value, including NULL. Thus if a tuple has null column, it must be
     * unique. But for ccindex merge, unique check is not for a unique constraint, but for keeping data
     * consistence that there must exist one and only one record of any exacting heap record.
     * So in this situation, NULL is equal to NULL since they stand for the same record. */
    /* Btree::CompareKeyToTuple would instinctly consider NULL equals to NULL */

    /* Update scankey values using insertion index tuple to help searching for insertion location. */
    (void)UpdateScanKeyWithValues(m_insertTuple);
}

RetStatus IncompleteBtreeInsertForCcindex::CheckConflict()
{
    StorageAssert(m_insertPageBuf != INVALID_BUFFER_DESC);
    StorageAssert(!m_isBoundValid);
    StorageAssert(!m_needDefferCheck);

    ErrLog(DSTORE_DEBUG1, MODULE_INDEX, ErrMsg("Start CheckConflict for insertion, on page {%d, %u} "
        "heapCitd {%d, %u} %d", m_insertPageBuf->GetPageId().m_fileId, m_insertPageBuf->GetPageId().m_blockId,
        m_insertTuple->GetHeapCtid().GetFileId(), m_insertTuple->GetHeapCtid().GetBlockNum(),
        m_insertTuple->GetHeapCtid().GetOffset()));

    /* init assumption */
    m_readyForInsert = false;
    m_satisfiedUnique = true;

    /* Force CheckUnique for ccindex merge & concurrent insert although the original index may not be an unique index */
    m_needCheckUnique = true;
    if (!m_indexInfo->isUnique) {
        /* For nonunique index, check keys with heapCtid to find out data conflict between each phase of ccindex */
        m_scanKeyValues.heapCtid = m_insertTuple->GetHeapCtid();
    } else {
        /* For unique index, do regular unique check using no heapCtid to scan all tuples with the same keys */
        m_scanKeyValues.heapCtid = INVALID_ITEM_POINTER;
    }

    /* Step 1. Search for the first key >= scan key on page */
    BtrPage *btrPage = static_cast<BtrPage *>(m_insertPageBuf->GetPage());
    m_insertOff = BinarySearchOnLeaf(btrPage);
    if (m_insertOff <= btrPage->GetMaxOffset() && m_insertOff == m_boundStrictHigh) {
        /* Nothing equal to scankey, just insert it then */
        StorageAssert(m_isBoundValid);
        StorageAssert(CompareKeyToTuple(btrPage, btrPage->GetLinkAndStatus(), m_insertOff,
                                        btrPage->GetLinkAndStatus()->TestType(BtrPageType::INTERNAL_PAGE)) < 0);
        m_scanKeyValues.heapCtid = m_insertTuple->GetHeapCtid();
        m_readyForInsert = true;
        return DSTORE_SUCC;
    }

    StorageAssert(m_insertOff > btrPage->GetMaxOffset() ||
                  CompareKeyToTuple(btrPage, btrPage->GetLinkAndStatus(), m_insertOff,
                                    btrPage->GetLinkAndStatus()->TestType(BtrPageType::INTERNAL_PAGE)) == 0);

    /* Step 2. Check if the potential conflict can be ignored */
    RetStatus ret = DSTORE_FAIL;
    Xid waitXid = INVALID_XID;
    if (m_insertType == InsertType::CONCURRENT_DML_INSERT) {
        ret = CheckConflictForConcurrentDml(&waitXid);
    } else if (m_insertType == InsertType::DELTA_MERGE_INSERT) {
        ret = CheckConflictForMergeDelta(&waitXid);
    } else {
        storage_set_error(INDEX_ERROR_INPUT_PARAM_WRONG);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Unrecognized insertType:%hhu of IncompleteBtreeInsertForCcindex",
                                                  static_cast<uint8>(m_insertType)));
    }

    if (STORAGE_FUNC_FAIL(ret)) {
        /* Something wrong when checking... */
        return ret;
    }
    if (unlikely(waitXid != INVALID_XID)) {
        /* Have to unlock curren page to wait for other sessions then recheck page */
        PageId pageId = m_insertPageBuf->GetPageId();
        m_bufMgr->UnlockAndRelease(m_insertPageBuf);
        m_insertPageBuf = INVALID_BUFFER_DESC;
        if (STORAGE_FUNC_FAIL(WaitForTxnEndIfNeeded(pageId, waitXid))) {
            StorageAssert(!m_needRetrySearchBtree);
            return DSTORE_FAIL;
        }
        m_needRetrySearchBtree = true;
    }

    m_needCheckUnique = m_indexInfo->isUnique;
    m_scanKeyValues.heapCtid = m_insertTuple->GetHeapCtid();
    return DSTORE_SUCC;
}

/*
 * For DeltaDml merge, a conflict can by caused by the postpone merge,
 *                                 or by the double copy in both original btree index & and DeltaDmlTable
 *
 * We need to do some extra check if an insertion is conflict with a BtrCcidxStatus::WRITE_ONLY_INDEX tuple because
 * we don't know whether there will be a postpone deletion later or not. In this case, insert it and let outside caller
 * do double check then determine to keep the insertion or rollback it.
 *
 * We also need to skip some of the insertion if the coresponding heap tuple have index tuples both on the btree
 * structure and in the DeltaDmlTable. It is possible because there's a timeslot between the btree meta committing and
 * the btree structure building. Insertions those start during this timeslot would have double records and it is just
 * acceptable. Skip this kind of duplicate would has no impact on data consistence.
 *
 * Any other cases would be a real duplicate, we need to set error and return fail as same as the regular processes do.
 */
RetStatus IncompleteBtreeInsertForCcindex::CheckConflictForMergeDelta(Xid *waitXid)
{
    StorageAssert(m_insertTuple->TestCcindexStatus(BtrCcidxStatus::WRITE_ONLY_INDEX));

    ErrLog(DSTORE_DEBUG1, MODULE_INDEX, ErrMsg("Start conflict check for DeltaInsertionMerge, on page {%d, %u} "
        "heapCitd {%d, %u} %d", m_insertPageBuf->GetPageId().m_fileId, m_insertPageBuf->GetPageId().m_blockId,
        m_insertTuple->GetHeapCtid().GetFileId(), m_insertTuple->GetHeapCtid().GetBlockNum(),
        m_insertTuple->GetHeapCtid().GetOffset()));

    /* init assumption */
    m_readyForInsert = true;
    m_satisfiedUnique = true;

    /* Step 1. Check all equal tuple for uniqueness */
    BufferDesc *rightBuf = INVALID_BUFFER_DESC;
    BtrPage *btrPage = static_cast<BtrPage *>(m_insertPageBuf->GetPage());
    OffsetNumber checkOff = m_insertOff;
    int cmpRet = 0;
    while (m_insertPageBuf != INVALID_BUFFER_DESC) {
        /* Step 2. There may be more equal tuples on right page. Try step right to check them. */
        if (STORAGE_FUNC_FAIL(StepRightForConlictCheckIfNeeded(&rightBuf, &btrPage, checkOff))) {
            m_readyForInsert = false;
            return DSTORE_FAIL;
        }
        if (rightBuf == INVALID_BUFFER_DESC && checkOff == INVALID_OFFSET) {
            /* No more tuple to compare. We're finished */
            return DSTORE_SUCC;
        }

        /* Step 3. Check if current item is a duplicate */
        StorageAssert(checkOff >= btrPage->GetLinkAndStatus()->GetFirstDataOffset());
        ItemId *currItemId = btrPage->GetItemIdPtr(checkOff);
        if (!currItemId->IsNormal()) {
            checkOff++;
            continue;
        }
        /* TO BE FINISHED: For GPI, check if partition visibility checking is needed */
        cmpRet = CompareKeyToTuple(btrPage, btrPage->GetLinkAndStatus(), checkOff,
                                   btrPage->GetLinkAndStatus()->TestType(BtrPageType::INTERNAL_PAGE));
        if (unlikely(StorageGetErrorCode() == COMMON_ERROR_FUNCTION_RETURN_NULL)) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to compare tuple since compare function returns null."));
            if (rightBuf != INVALID_BUFFER_DESC) {
                m_bufMgr->UnlockAndRelease(rightBuf);
            }
            return DSTORE_FAIL;
        }
        if (cmpRet != 0) {
            /* All equal tuples have been checked. */
            break;
        }
        /* Comparing result is "equal", it's a duplicate */

        /* Step 4. Deal with BtrCcindexStatus::IN_BUILDING_INDEX tuples */
        IndexTuple *tupleOnPage = btrPage->GetIndexTuple(currItemId);
        ItemPointerData pageHeapCtid = tupleOnPage->GetHeapCtid();
        ItemPointerData insertHeapCtid = m_insertTuple->GetHeapCtid();
        if (tupleOnPage->TestCcindexStatus(BtrCcidxStatus::IN_BUILDING_INDEX)) {
            /* A tuple connot be deleted during building! */
            StorageAssert(!tupleOnPage->IsDeleted());
            /* All BtrCcidxStatus::IN_BUILDING_INDEX tuples are committed and visible */
            m_satisfiedUnique = false;
            if (pageHeapCtid == insertHeapCtid) {
                /* It is possible that an insertion is both on btree structure and in deltaDmlTable. Just skip */
                m_needSkip = true;
                break;
            }
            /* This is a duplicate. Save info for caller */
            storage_set_error(INDEX_ERROR_INSERT_UNIQUE_CHECK);
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[CCINDEX] duplicate found for merge delta dml for (%s:%d)"
                "page heapCtid(%hu, %u, %hu), inserting heapCtid(%hu, %u, %hu)",
                m_indexInfo->indexRelName, m_indexInfo->indexRelId,
                pageHeapCtid.GetFileId(), pageHeapCtid.GetBlockNum(), pageHeapCtid.GetOffset(),
                insertHeapCtid.GetFileId(), insertHeapCtid.GetBlockNum(), insertHeapCtid.GetOffset()));
            m_indexInfo->extraInfo = UInt64GetDatum(pageHeapCtid.m_placeHolder);
            /* For unique index, it's guaranteed that there's no duplicate on the btree.
             * Just insert it and the outside caller would check DeltaDmlTable for us. */
            m_readyForInsert = false;
            m_needSkip = false;
            break;
        }

        /* Step 5. Check visibility of BtrCcidxStatus::WRITE_ONLY_INDEX tuples that was inserted by concurent DML */
        StorageAssert(tupleOnPage->TestCcindexStatus(BtrCcidxStatus::WRITE_ONLY_INDEX));
        bool visible = BtreeSplit::CheckTupleVisibility(btrPage, checkOff, waitXid);
        if (!visible) {
            StorageAssert(*waitXid != INVALID_XID);
            /* The duplicate is not visible since it haven't been committed yet. We need to wait for it to commit
             * then re-check it. */
            m_satisfiedUnique = false;
            m_readyForInsert = false;
            break;
        }

        /* Step 6. Deal with visible duplicate concurent DML */
        if (pageHeapCtid != insertHeapCtid) {
            if (!tupleOnPage->IsDeleted()) {
                /* A visible duplicate */
                m_satisfiedUnique = false;
                m_readyForInsert = false;
                m_insertOff = checkOff;
                TD *td = btrPage->GetTd(tupleOnPage->GetTdId());
                storage_set_error(INDEX_ERROR_INSERT_UNIQUE_CHECK);
                ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[CCINDEX] duplicate found for merge delta dml for (%s:%d) "
                    "page heapCtid(%hu, %u, %hu), td(%hu){tupleTdStat(%hu), tdStatus(%d), csn(%lu), csnStatus(%hu)} "
                    "inserting heapCtid(%hu, %u, %hu)",
                    m_indexInfo->indexRelName, m_indexInfo->indexRelId,
                    pageHeapCtid.GetFileId(), pageHeapCtid.GetBlockNum(), pageHeapCtid.GetOffset(),
                    tupleOnPage->GetTdId(), static_cast<uint8>(tupleOnPage->GetTdStatus()),
                    static_cast<int>(td->GetStatus()), td->GetCsn(), td->GetCsnStatus(),
                    insertHeapCtid.GetFileId(), insertHeapCtid.GetBlockNum(), insertHeapCtid.GetOffset()));
                    break;
            }
            /* Otherwise keep check next */
        } else if (tupleOnPage->IsDeleted()) {
            /* Found concurrent DML deletion on page. No need to insert this old version */
            m_needSkip = true;
            if (tupleOnPage->IsInsertDeletedForCCindex()) {
                /* Remember that the concurrent DML may insert a deletion if it was not on the btree. */
                /* Need to erase the InsertDeletedForCCindex flag. */
                m_needEraseIns4DelFlag = true;
            }
            break;
        } else {
            /* A visible duplicate */
            m_satisfiedUnique = false;
            m_readyForInsert = false;
            /* There may be two cases:
             * 1. During merging phase, the concurrent DML deleted this tuple first, then insert it with the same key
             *    again. If we can find a concurrent DELETION with m_ccindexInsForDelFlag, then just skip it.
             * 2. The tuple on page was a ccindexInsForDel tuple, while the transaction aborted. Then the deletion
             *    rollback and the m_ccindexInsForDelFlag left. Check if there's a m_ccindexInsForDelFlag, erase it
             *    if so. */
            if (tupleOnPage->IsInsertDeletedForCCindex()) {
                /* Hit case 2. */
                m_needSkip = true;
                m_needEraseIns4DelFlag = true;
                break;
            }
            /* Hit case 1. keep going on to find if we can hit case 2, if not, we'll return with !m_readyForInsert */
            /* We're not going to insert it now, just save the message */
            m_insertOff = checkOff;
        }

        /* Current duplicate is invisible. Go check next tuple. */
        checkOff++;
    }

    if (rightBuf != INVALID_BUFFER_DESC) {
        m_bufMgr->UnlockAndRelease(rightBuf);
    }

    if (unlikely(m_needSkip)) {
        m_readyForInsert = false;
        m_satisfiedUnique = true;
        m_insertOff = checkOff;
        /* Set error so that the caller can tell why we skip insertion. Also the caller can recognize
            * the real exception if the error code been covered. */
        storage_set_error(INDEX_ERROR_DUPLICATE_TO_THE_SAME_RECORD);
        ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
            ErrMsg("[CCINDEX] Skip to merge insert for heapCtid{%hu, %u} %hu on btrPage{%hu, %u} %hu, "
                    "in xid(%d, %lu), snapshotCsn:%lu " BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
            m_insertTuple->GetHeapCtid().GetFileId(), m_insertTuple->GetHeapCtid().GetBlockNum(),
            m_insertTuple->GetHeapCtid().GetOffset(), btrPage->GetSelfPageId().m_fileId,
            btrPage->GetSelfPageId().m_blockId, checkOff, static_cast<int32>(thrd->GetCurrentXid().m_zoneId),
            thrd->GetCurrentXid().m_logicSlotId, thrd->GetSnapShotCsn(), BTR_PAGE_HEADER_VAL(btrPage),
            BTR_PAGE_LINK_AND_STATUS_VAL(btrPage->GetLinkAndStatus())));
    }

    return DSTORE_SUCC;
}

/*
 * For concurrent DML, a conflict can by caused by the postpone merge.
 *
 * We need to do some extra check if an insertion is conflict with a BtrCcidxStatus::WRITE_ONLY_INDEX tuple because
 * we don't know if there will be a postpone deletion later. For this case, insert it and let outside caller do
 * double check then determine to keep the insertion or rollback it.
 *
 * Any other status (except WRITE_ONLY_INDEX) is the final status that should be considered as a real duplicate.
 * We need to set error and return fail as same as the regular processes do.
 * Even for nonunique index, the duplicate is not acceptable because we checked conflict along with HEAPCTID. An index
 * never allows two index tuples pointing to the same heap tuple.
 */
RetStatus IncompleteBtreeInsertForCcindex::CheckConflictForConcurrentDml(Xid *waitXid)
{
    StorageAssert(m_insertTuple->TestCcindexStatus(BtrCcidxStatus::WRITE_ONLY_INDEX));

    ErrLog(DSTORE_DEBUG1, MODULE_INDEX, ErrMsg("Start conflict check for ConcurrentInsertion, on page {%d, %u} "
        "heapCitd {%d, %u} %d", m_insertPageBuf->GetPageId().m_fileId, m_insertPageBuf->GetPageId().m_blockId,
        m_insertTuple->GetHeapCtid().GetFileId(), m_insertTuple->GetHeapCtid().GetBlockNum(),
        m_insertTuple->GetHeapCtid().GetOffset()));

    BufferDesc *rightBuf = INVALID_BUFFER_DESC;
    BtrPage *btrPage = static_cast<BtrPage *>(m_insertPageBuf->GetPage());
    /* init assumption */
    m_satisfiedUnique = true;

    /* Step 1. Check all equal tuple for uniqueness */
    OffsetNumber checkOff = m_insertOff;
    while (m_insertPageBuf != INVALID_BUFFER_DESC) {
        /* Step 2. There may be more equal tuples on right page. Try step right to check them. */
        if (STORAGE_FUNC_FAIL(StepRightForConlictCheckIfNeeded(&rightBuf, &btrPage, checkOff))) {
            return DSTORE_FAIL;
        }
        if (rightBuf == INVALID_BUFFER_DESC && checkOff == INVALID_OFFSET) {
            /* No more tuple to compare. We're good to insert */
            m_readyForInsert = true;
            break;
        }

        /* Step 3. Check if current item */
        StorageAssert(checkOff >= btrPage->GetLinkAndStatus()->GetFirstDataOffset());
        ItemId *currItemId = btrPage->GetItemIdPtr(checkOff);
        if (!currItemId->IsNormal()) {
            checkOff++;
            continue;
        }
        /* TO BE FINISHED: For GPI, check if partition visibility checking is needed */
        if (CompareKeyToTuple(btrPage, btrPage->GetLinkAndStatus(), checkOff,
                              btrPage->GetLinkAndStatus()->TestType(BtrPageType::INTERNAL_PAGE)) != 0) {
            /* All equal tuples have been checked. */
            m_readyForInsert = true;
            break;
        }
        /* Comparing result is "equal", it's a duplicate */

        /* Step 4. Check heapCtid */
        IndexTuple *tupleOnPage = btrPage->GetIndexTuple(currItemId);
        if (tupleOnPage->TestCcindexStatus(BtrCcidxStatus::IN_BUILDING_INDEX)) {
            /* This is a duplicate. But we don't have the knowledge that if the tuple on page would be deleted
             * later when DeltaDmlTable merge finished.
             * We have to check it by outside caller, searching the tupleOnPage's heapCtid from DeltaDmlTable
             * for existence.
             */
            StorageAssert(!tupleOnPage->IsDeleted());
            StorageAssert(m_indexInfo->extraInfo == UInt64GetDatum(INVALID_ITEM_POINTER.m_placeHolder));
            /* Save the checking info for caller */
            m_indexInfo->extraInfo = UInt64GetDatum(tupleOnPage->GetHeapCtid().m_placeHolder);
            /* Keep scan the rest tuples. We'll insert it if no more duplicate found.
             * Caller will do an extra checking for us */
            m_satisfiedUnique = false;
            m_insertOff = checkOff;
            checkOff++;
            continue;
        }

        /* Step 5. Check visibility of current item */
        bool visible = CheckTupleVisibility(btrPage, checkOff, waitXid);
        if (visible || *waitXid != INVALID_XID) {
            /* For visible case:
             * If the duplicated tuple is visible, we need to stop inserting. We've checked the BtrCcidxStatus flag
             * before in step 4 and the duplicate on page is not inserted by BtreeBuild. Thus it must be inserted
             * either by delta data merging or by another concurrent DML operation. In both of the cases, it is
             * definitely a duplicate so that we should stop inserting and return fail to client.
             *
             * For (*waitXid != INVALID_XID) case:
             * The duplicate is not visible since it haven't been committed yet. We need to wait for it to commit
             * then re-check it.
             */
            m_satisfiedUnique = false;
            /* We're not going to insert it but to keep the last checking position. */
            m_insertOff = checkOff;
            m_readyForInsert = false;
            break;
        }
        /* for (!visible && *waitXid == INVALID_XID), the only posibility is that the duplicate has already
         * been deleted. No need to wait and recheck. Go check the next tuple. */
        checkOff++;
    }

    if (rightBuf != INVALID_BUFFER_DESC) {
        m_bufMgr->UnlockAndRelease(rightBuf);
    }
    return DSTORE_SUCC;
}

RetStatus IncompleteBtreeInsertForCcindex::StepRightForConlictCheckIfNeeded(BufferDesc **rightBuf, BtrPage **currPage,
                                                                            OffsetNumber &checkOff)
{
    StorageAssert(rightBuf != nullptr);
    StorageAssert(currPage != nullptr);
    *currPage = static_cast<BtrPage *>(m_insertPageBuf->GetPage());
    OffsetNumber maxOff = (*currPage)->GetMaxOffset();
    while (checkOff > maxOff) {
        if (STORAGE_FUNC_FAIL(StepRightWhenCheckUnique(rightBuf, LW_SHARED))) {
            return DSTORE_FAIL;
        }
        if ((*rightBuf) == INVALID_BUFFER_DESC) {
            /* No more page to check */
            checkOff = INVALID_OFFSET;
            break;
        }
        /* Update current checking page info */
        *currPage = static_cast<BtrPage *>((*rightBuf)->GetPage());
        maxOff = (*currPage)->GetMaxOffset();
        checkOff = (*currPage)->GetLinkAndStatus()->GetFirstDataOffset();
    }
    return DSTORE_SUCC;
}

void IncompleteBtreeInsertForCcindex::EraseIns4DelFlag()
{
    /* Need to write WAL. Complete it after ccindex WAL finished */
    StorageAssert(m_insertPageBuf != INVALID_BUFFER_DESC);
    StorageAssert(m_insertOff != INVALID_OFFSET);

    BtrPage *page = static_cast<BtrPage *>(m_insertPageBuf->GetPage());
    IndexTuple *tupleOnPage = page->GetIndexTuple(m_insertOff);
    StorageAssert(tupleOnPage->IsInsertDeletedForCCindex());
    StorageAssert(tupleOnPage->GetCcindexStatus() == BtrCcidxStatus::WRITE_ONLY_INDEX);
    tupleOnPage->SetNotInsertDeletedForCCindex();
    m_bufMgr->MarkDirty(m_insertPageBuf);

    if (unlikely(!NeedWal())) {
        return;
    }

    /* Generate Redo record for flag changing */
    AtomicWalWriterContext *walContext = thrd->m_walWriterContext;
    StorageAssert(walContext);
    walContext->BeginAtomicWal(thrd->GetActiveTransaction()->GetCurrentXid());

    WalRecordBtreeEraseInsForDelFlag walRec;
    walRec.SetHeader(page, page->GetWalId() != walContext->GetWalId(), m_insertOff, m_insertPageBuf->GetFileVersion());
    walContext->RememberPageNeedWal(m_insertPageBuf);
    walContext->PutNewWalRecord(&walRec);
    UNUSED_VARIABLE(walContext->EndAtomicWal());
}

RetStatus IncompleteBtreeInsertForCcindex::InsertTuple4Delete(IncompleteBtreeDeleteForCcindex *btrDel)
{
    StorageAssert(btrDel->m_needInsertForDel);
    /* Check index status */
    if (unlikely(btrDel->m_indexInfo->btrIdxStatus != BtrCcidxStatus::WRITE_ONLY_INDEX)) {
        storage_set_error(INDEX_ERROR_UNMATCHED_CCINDEX_STATUS,
                          btrDel->m_indexInfo->btrIdxStatus, BtrCcidxStatus::WRITE_ONLY_INDEX);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[CCINDEX] InsertTuple4Delete can only be called for ccindex "
            "phase3. current btrIdxStatus = %hhu", static_cast<uint8>(btrDel->m_indexInfo->btrIdxStatus)));
        return DSTORE_FAIL;
    }

    /* Create BtreeInsertForCcindex object for insertion */
    AutoMemCxtSwitch autoMemCxtSwitch(thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_QUERY));
    IncompleteBtreeInsertForCcindex *incompleteCcidx =
        DstoreNew(g_dstoreCurrentMemoryContext) IncompleteBtreeInsertForCcindex(btrDel->m_indexRel,
                                                                                btrDel->m_indexInfo,
                                                                                btrDel->m_scanKeyValues.scankeys);
    if (unlikely(incompleteCcidx == nullptr)) {
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to alloc size:%lu", sizeof(IncompleteBtreeInsertForCcindex)));
        return DSTORE_FAIL;
    }

    if (STORAGE_FUNC_FAIL(incompleteCcidx->InitInsertInfoForConcurrentDelete(btrDel))) {
        delete incompleteCcidx;
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to init insert info for concurrent delete of ccindex \"%s\".",
            btrDel->m_indexInfo->indexRelName));
        return DSTORE_FAIL;
    }
    if (unlikely(!(incompleteCcidx->IsLastAccessedPageValid()))) {
        /* The page has been changed while we're holing a EXCLUSIVE lock on it! */
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Last accessed page is not valid for ccindex \"%s\".",
            btrDel->m_indexInfo->indexRelName));
        delete incompleteCcidx;
        return DSTORE_FAIL;
    }

    RetStatus ret = incompleteCcidx->BtreeInsert::AddTupleToLeaf();
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to add tuple to leaf for ccindex \"%s\".",
            btrDel->m_indexInfo->indexRelName));
    }
    /* The page buffer would have been unlocked and released after AddTupleToLeaf */
    btrDel->m_pagePayload.buffDesc = INVALID_BUFFER_DESC;
    delete incompleteCcidx;
    return ret;
}

RetStatus IncompleteBtreeInsertForCcindex::InitInsertInfoForConcurrentDelete(IncompleteBtreeDeleteForCcindex *btrDel)
{
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr \"ActiveTransaction\".", __FUNCTION__));
        return DSTORE_FAIL;
    }
    m_cid = transaction->GetCurCid();

    m_insertPageBuf = btrDel->m_pagePayload.GetBuffDesc();
    StorageAssert(m_insertPageBuf != INVALID_BUFFER_DESC);
    StorageAssert(m_insertPageBuf->GetPageId() == btrDel->m_pagePayload.GetPageId());

    m_insertTuple = btrDel->m_searchingTarget;
    StorageAssert(m_insertTuple != nullptr);
    StorageAssert(m_insertTuple->GetCcindexStatus() == BtrCcidxStatus::WRITE_ONLY_INDEX);

    m_insertTuple->SetCcindexInsForDelFlag();
    m_insertTuple->SetDeleted();

    m_insertOff = btrDel->m_delOffset;
    StorageClearError();
    UpdateScanKeyWithValues(m_insertTuple);

    return DSTORE_SUCC;
}

bool IncompleteBtreeInsertForCcindex::IsLastAccessedPageValid()
{
    BtrPage *insertPage = static_cast<BtrPage *>(m_insertPageBuf->GetPage());
    BtrPageLinkAndStatus *linkAndStatus = insertPage->GetLinkAndStatus();
    Xid checkingXid = GetBtreeSmgr()->GetMetaCreateXid();
    if (unlikely(!BtrPage::IsBtrPageValid(insertPage, checkingXid))) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[CCINDEX] Invalid btree page, checkingXid(%d, %lu), "
            BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
            static_cast<int32>(checkingXid.m_zoneId), checkingXid.m_logicSlotId,
            BTR_PAGE_HEADER_VAL(insertPage), BTR_PAGE_LINK_AND_STATUS_VAL(insertPage->GetLinkAndStatus())));
        return false;
    }
    if (insertPage->GetMaxOffset() < linkAndStatus->GetFirstDataOffset()) {
        Xid xid = thrd->GetCurrentXid();
        ErrLog(DSTORE_LOG, MODULE_INDEX,
            ErrMsg("[CCINDEX] Empty btree page when ins4del, xid(%d, %lu), page:"
            BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
            static_cast<int>(xid.m_zoneId), xid.m_logicSlotId,
            BTR_PAGE_HEADER_VAL(insertPage), BTR_PAGE_LINK_AND_STATUS_VAL(insertPage->GetLinkAndStatus())));
        return false;
    }

    if (m_insertOff > linkAndStatus->GetFirstDataOffset()) {
        IndexTuple *prevTuple = insertPage->GetIndexTuple(m_insertOff - 1);
        int result = IndexTuple::Compare(m_insertTuple, prevTuple, m_indexInfo);
        if (result < 0) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[CCINDEX] Compare failed, result %d, prevTuple deleted %d, "
                "m_insertTuple ctid(%hu, %u, %hu), prevTuple ctid(%hu, %u, %hu)."
                BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
                result, prevTuple->IsDeleted(), m_insertTuple->GetHeapCtid().GetFileId(),
                m_insertTuple->GetHeapCtid().GetBlockNum(), m_insertTuple->GetHeapCtid().GetOffset(),
                prevTuple->GetHeapCtid().GetFileId(), prevTuple->GetHeapCtid().GetBlockNum(),
                prevTuple->GetHeapCtid().GetOffset(),
                BTR_PAGE_HEADER_VAL(insertPage), BTR_PAGE_LINK_AND_STATUS_VAL(insertPage->GetLinkAndStatus())));
            return false;
        }
    }
    if (m_insertOff < insertPage->GetMaxOffset()) {
        IndexTuple *nextTuple = insertPage->GetIndexTuple(m_insertOff + 1);
        int result = IndexTuple::Compare(m_insertTuple, nextTuple, m_indexInfo);
        if (result > 0) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[CCINDEX] Compare failed, result %d, nextTuple deleted %d, "
                "offset(%hu), insert heapCtid(%hu, %u, %hu), next heapCtid(%hu, %u, %hu). "
                BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
                result, nextTuple->IsDeleted(), m_insertOff, m_insertTuple->GetHeapCtid().GetFileId(),
                m_insertTuple->GetHeapCtid().GetBlockNum(), m_insertTuple->GetHeapCtid().GetOffset(),
                nextTuple->GetHeapCtid().GetFileId(), nextTuple->GetHeapCtid().GetBlockNum(),
                nextTuple->GetHeapCtid().GetOffset(),
                BTR_PAGE_HEADER_VAL(insertPage), BTR_PAGE_LINK_AND_STATUS_VAL(insertPage->GetLinkAndStatus())));
            return false;
        }
    } else if (!linkAndStatus->IsRightmost()) {
        IndexTuple *highkey = insertPage->GetIndexTuple(BTREE_PAGE_HIKEY);
        int result = IndexTuple::Compare(m_insertTuple, highkey, m_indexInfo);
        if (result > 0) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[CCINDEX] Compare failed, result %d, "
                "offset(%hu), m_insertTuple ctid(%hu, %u, %hu)."
                BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
                result, m_insertOff, m_insertTuple->GetHeapCtid().GetFileId(),
                m_insertTuple->GetHeapCtid().GetBlockNum(), m_insertTuple->GetHeapCtid().GetOffset(),
                BTR_PAGE_HEADER_VAL(insertPage), BTR_PAGE_LINK_AND_STATUS_VAL(insertPage->GetLinkAndStatus())));
            return false;
        }
    }
    return true;
}

} /* namespace DSTORE */
