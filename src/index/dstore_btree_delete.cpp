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
 * This file defines the BtreeDelete class.
 *
 * IDENTIFICATION
 *        src/index/dstore_btree_delete.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "common/log/dstore_log.h"
#include "errorcode/dstore_index_error_code.h"
#include "index/dstore_btree_page_recycle.h"
#include "index/dstore_btree_prune.h"
#include "index/dstore_btree_undo_data_struct.h"
#include "index/dstore_btree_wal.h"
#include "index/dstore_btree_perf_unit.h"
#include "wal/dstore_wal_write_context.h"
#include "transaction/dstore_transaction_mgr.h"
#include "index/dstore_btree_delete.h"

namespace DSTORE {

BtreeDelete::BtreeDelete(StorageRelation indexRel, IndexInfo *indexInfo, ScanKey scanKeyInfo)
    : BtreeSplit(indexRel, indexInfo, scanKeyInfo, false), m_searchingTarget(nullptr), m_delTuple(nullptr),
      m_delOffset(INVALID_ITEM_OFFSET_NUMBER), m_needRetry(false)
{}

BtreeDelete::~BtreeDelete()
{
    m_searchingTarget = nullptr;
    m_delTuple = nullptr;
    m_delOffset = INVALID_ITEM_OFFSET_NUMBER;
}

RetStatus BtreeDelete::DeleteTuple(Datum *values, bool *isnull, ItemPointer ctid, CommandId cid)
{
    LatencyStat::Timer timer(&BtreePerfUnit::GetInstance().m_btreeDeleteLatency);
    RetStatus retStatus = DSTORE_FAIL;
    AutoMemCxtSwitch autoMemCxtSwitch(thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_STACK));

    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr \"ActiveTransaction\".", __FUNCTION__));
        return DSTORE_FAIL;
    }
    if (STORAGE_FUNC_FAIL(transaction->AllocTransactionSlot())) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to allocate transaction slot for index \"%s\".",
            m_indexInfo->indexRelName));
        return DSTORE_FAIL;
    }
    m_cid = (cid == INVALID_CID) ? transaction->GetCurCid() : cid;

    if (STORAGE_FUNC_FAIL(retStatus = FormIndexTuple(values, isnull, ctid, &m_searchingTarget))) {
        DstorePfreeExt(m_searchingTarget);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to form index tuple for index \"%s\".",
            m_indexInfo->indexRelName));
        return DSTORE_FAIL;
    }

    ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
           ErrMsg("Start to delete index tuple with heap ctid = ({%hu, %u}, %hu)", ctid->GetFileId(),
                  ctid->GetBlockNum(), ctid->GetOffset()));

    StorageClearError();
    UNUSED_VARIABLE(UpdateScanKeyWithValues(m_searchingTarget));
    while (STORAGE_FUNC_FAIL(retStatus = DoDelete())) {
        if (STORAGE_FUNC_FAIL(thrd->CheckforInterrupts())) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX,
                ErrMsg("Index delete of heapCtid(%hu, %u, %hu) is canceled by user.",
                       ctid->GetFileId(), ctid->GetBlockNum(), ctid->GetOffset()));
            break;
        }
        FAULT_INJECTION_ACTION(DstoreIndexFI::STOP_RETRY_SEARCH_BTREE, m_needRetrySearchBtree = false);
        if (!m_needRetrySearchBtree) {
            break;
        }
        m_needRetrySearchBtree = false;
        m_delTuple = nullptr;
    }
    ClearStack();
    DstorePfreeExt(m_searchingTarget);
    return retStatus;
}

RetStatus BtreeDelete::StepRightWhenFindDelLoc()
{
    BufferDesc *currBuf = m_pagePayload.GetBuffDesc();
    PageId nextPageId = m_pagePayload.GetLinkAndStatus()->GetRight();
    for (;;) {
        if (unlikely(!nextPageId.IsValid())) {
            storage_set_error(INDEX_ERROR_MOVE_END, m_indexInfo->indexRelName);
            Xid btrCreateXid = GetBtreeSmgr()->GetMetaCreateXid();
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Right page is invalid when StepRightWhenFindDelLoc. "
                "pdb:%u, index(%s:%u), createXid(%d, %lu), segment(%hu, %u), currXid(%d, %lu) "
                BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
                GetPdbId(), m_indexInfo->indexRelName, m_indexRel->relOid,
                static_cast<int>(btrCreateXid.m_zoneId), btrCreateXid.m_logicSlotId,
                GetBtreeSmgr()->GetSegMetaPageId().m_fileId, GetBtreeSmgr()->GetSegMetaPageId().m_blockId,
                static_cast<int>(thrd->GetCurrentXid().m_zoneId), thrd->GetCurrentXid().m_logicSlotId,
                BTR_PAGE_HEADER_VAL(m_pagePayload.GetPage()),
                BTR_PAGE_LINK_AND_STATUS_VAL(m_pagePayload.GetLinkAndStatus())));
            m_pagePayload.Drop(m_bufMgr);
            return DSTORE_FAIL;
        }
        currBuf = ReleaseOldGetNewBuf(currBuf, nextPageId, LW_EXCLUSIVE);
        if (STORAGE_VAR_NULL(currBuf)) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX,
                ErrMsg("Failed to get buffer when step right to btrPage(%hu, %u).",
                nextPageId.m_fileId, nextPageId.m_blockId));
            m_pagePayload.buffDesc = INVALID_BUFFER_DESC;
            return DSTORE_FAIL;
        }
        BtrPage *currPage = static_cast<BtrPage *>(currBuf->GetPage());
        BtrPageLinkAndStatus *currLinkStat = currPage->GetLinkAndStatus();
        /* Finish splitting if incomplete */
        if (unlikely(!m_pagePayload.GetLinkAndStatus()->IsSplitComplete())) {
            if (STORAGE_FUNC_FAIL(CompleteSplit(currBuf, m_leafStack, LW_EXCLUSIVE))) {
                m_pagePayload.buffDesc = INVALID_BUFFER_DESC;
                ErrLog(DSTORE_ERROR, MODULE_INDEX,
                    ErrMsg("Failed to complete split when step right to find delete location."));
                return DSTORE_FAIL;
            }
            /* buffer is released after split */
            currBuf = INVALID_BUFFER_DESC;
            continue;
        }
        if (!currLinkStat->IsUnlinked()) {
            /* Found the first unlinked page. We're done */
            break;
        }
        /* The page is unlinked. Need to go father */
        if (unlikely(currLinkStat->IsRightmost())) {
            storage_set_error(INDEX_ERROR_BTREE_STRUCTURE_DAMAGED);
            ErrLog(DSTORE_ERROR, MODULE_INDEX,
                ErrMsg("Btree structure is damaged. The rightmost btrPage(%hu, %u) is unlinked.",
                nextPageId.m_fileId, nextPageId.m_blockId));
            m_bufMgr->UnlockAndRelease(currBuf);
            m_pagePayload.buffDesc = INVALID_BUFFER_DESC;
            return DSTORE_FAIL;
        }
        nextPageId = currLinkStat->GetRight();
    }
    StorageAssert(currBuf != INVALID_BUFFER_DESC);
    m_pagePayload.InitByBuffDesc(currBuf);
    return DSTORE_SUCC;
}
RetStatus BtreeDelete::FindDeleteLoc(Xid *waitXid)
{
    ItemPointerData heapCtid = m_searchingTarget->GetHeapCtid();
    int cmpRet = 0;
    /* Scan over all equal tuples, looking for m_delTuple. */
    for (;;) {
        *waitXid = INVALID_XID;
        BtrPageLinkAndStatus *linkAndStatus = m_pagePayload.GetLinkAndStatus();
        bool isInternalPage = linkAndStatus->TestType(BtrPageType::INTERNAL_PAGE);
        if (m_delOffset > m_pagePayload.GetPage()->GetMaxOffset()) {
            /* By here, We need to advance to next non-dead page to continue checking */
            if (linkAndStatus->IsRightmost()) {
                /* No more mached tuple. Failed to find deleting target */
                storage_set_error(INDEX_ERROR_MOVE_END, m_indexInfo->indexRelName);
                ErrLog(DSTORE_ERROR, MODULE_INDEX,
                    ErrMsg("FindDeleteLoc reach end in(%s), segment(%hu, %u), curr btrPage(%hu, %u) "
                    BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT, m_indexInfo->indexRelName,
                    GetBtreeSmgr()->GetSegMetaPageId().m_fileId, GetBtreeSmgr()->GetSegMetaPageId().m_blockId,
                    m_pagePayload.GetPage()->GetSelfPageId().m_fileId,
                    m_pagePayload.GetPage()->GetSelfPageId().m_blockId, BTR_PAGE_HEADER_VAL(m_pagePayload.GetPage()),
                    BTR_PAGE_LINK_AND_STATUS_VAL(m_pagePayload.GetLinkAndStatus())));
                break;
            }
            cmpRet = CompareKeyToTuple(m_pagePayload.GetPage(), linkAndStatus, BTREE_PAGE_HIKEY, isInternalPage);
            if (unlikely(StorageGetErrorCode() == COMMON_ERROR_FUNCTION_RETURN_NULL)) {
                StorageAssert(*waitXid == INVALID_XID);
                ErrLog(DSTORE_ERROR, MODULE_INDEX,
                       ErrMsg("Failed to compare tuple since compare function returns null."));
                break;
            }
            if (cmpRet < 0) {
                /* The target connot be on the right page */
                ErrLog(DSTORE_ERROR, MODULE_INDEX,
                    ErrMsg("The target should on index[%u](%s) segment(%hu, %u) btrPage(%hu, %u) but not be found. "
                    BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
                    m_indexRel->relOid, m_indexInfo->indexRelName,
                    GetBtreeSmgr()->GetSegMetaPageId().m_fileId, GetBtreeSmgr()->GetSegMetaPageId().m_blockId,
                    m_pagePayload.GetPageId().m_fileId, m_pagePayload.GetPageId().m_blockId,
                    BTR_PAGE_HEADER_VAL(m_pagePayload.GetPage()),
                    BTR_PAGE_LINK_AND_STATUS_VAL(m_pagePayload.GetLinkAndStatus())));
                break;
            }
            if (STORAGE_FUNC_FAIL(StepRightWhenFindDelLoc())) {
                /* Internal error in btree. We've record errcode, just return */
                ErrLog(DSTORE_ERROR, MODULE_INDEX,
                    ErrMsg("Failed to step right when find delete location."));
                break;
            }
            /* We've steped to the right page, reset m_delOffset */
            linkAndStatus = m_pagePayload.GetLinkAndStatus();
            m_delOffset = linkAndStatus->GetFirstDataOffset();
        }
        if (unlikely(m_delOffset > m_pagePayload.GetPage()->GetMaxOffset())) {
            continue;
        }

        /* Advance to next tuple to continue checking. */
        ItemId *curItemId = m_pagePayload.GetPage()->GetItemIdPtr(m_delOffset);
        if (!curItemId->IsNormal()) {
            m_delOffset++;
            continue;
        }

        cmpRet = CompareKeyToTuple(m_pagePayload.GetPage(), linkAndStatus, m_delOffset, isInternalPage);
        if (unlikely(StorageGetErrorCode() == COMMON_ERROR_FUNCTION_RETURN_NULL)) {
            StorageAssert(*waitXid == INVALID_XID);
            ErrLog(DSTORE_ERROR, MODULE_INDEX,
                    ErrMsg("Failed to compare tuple since compare function returns null."));
            break;
        }
        if (unlikely(cmpRet != 0)) {
            /* we've traversed all the equal tuples, but we haven't found the target to delete */
            ErrLog(DSTORE_ERROR, MODULE_INDEX,
                ErrMsg("haven't found the target to delete. last checking offset(%hu) "
                       BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
                m_delOffset, BTR_PAGE_HEADER_VAL(m_pagePayload.GetPage()),
                BTR_PAGE_LINK_AND_STATUS_VAL(m_pagePayload.GetLinkAndStatus())));
            storage_set_error(INDEX_ERROR_FAILED_TO_FIND_DELETING_TARGET);
            break;
        }

        IndexTuple *curItup = m_pagePayload.GetPage()->GetIndexTuple(curItemId);
        if (curItup->GetHeapCtid() != m_searchingTarget->GetHeapCtid()) {
            /* Same key but different heap ctid, go check next tuple */
            m_delOffset++;
            continue;
        }

        /*
         * We've found the index tuple with specific key values and heapCtid. Now check visibility.
         *
         * Note that this tuple may be written by a failed transaction that needed to be rollback.
         * We'll also do rollback here if the transaction failed.
         */
        bool visible = CheckTupleVisibility(m_pagePayload.GetPage(), m_delOffset, waitXid);
        if (visible) {
            /* We've found the exact index tuple */
            if (!curItup->IsDeleted()) {
                /* By here, the index tuple is matched with scan key and it is normal */
                return DSTORE_SUCC;
            }
            /* The tuple is already deleted. But there may be another one that inserted after the deletion.
             * Go on checking next. */
            m_delOffset++;
            continue;
        }

        StorageAssert(*waitXid != INVALID_XID);
        ErrLog(DSTORE_LOG, MODULE_INDEX,
               ErrMsg("found the indexTuple(%hu, %u, %hu) with heapCtid(%hu, %u, %hu) to delete but someone else "
                      "is %s it and not yet committed with td:%hhu and tupleTdStatus:%hhu. "
                      "Wait for xid:(%d, %lu) end then recheck.",
               m_pagePayload.GetPageId().m_fileId, m_pagePayload.GetPageId().m_blockId, m_delOffset,
               heapCtid.GetFileId(), heapCtid.GetBlockNum(), heapCtid.GetOffset(),
               curItup->IsDeleted() ? "deleting" : "inserting", curItup->GetTdId(), curItup->GetTdStatus(),
               static_cast<int32>(waitXid->m_zoneId), waitXid->m_logicSlotId));
        return DSTORE_FAIL;
    }

    /* No corresponding itup was found, release buffer and return InvalidOffsetNumber */
    ErrLog(DSTORE_LOG, MODULE_INDEX,
        ErrMsg("Failed to find index tuple in (%s) with heapCtid(%hu, %u, %hu), segment(%hu, %u), "
        "snapshot(%lu), xid(%u, %lu), tableOid(%u)",
        m_indexInfo->indexRelName, heapCtid.GetFileId(), heapCtid.GetBlockNum(), heapCtid.GetOffset(),
        GetBtreeSmgr()->GetSegMetaPageId().m_fileId, GetBtreeSmgr()->GetSegMetaPageId().m_blockId,
        thrd->GetSnapShotCsn(), static_cast<uint32>(thrd->GetCurrentXid().m_zoneId),
        thrd->GetCurrentXid().m_logicSlotId,
        (m_indexInfo->relKind == static_cast<char>(SYS_RELKIND_GLOBAL_INDEX) ?
            m_searchingTarget->GetTableOid(m_indexInfo) : 0)));

    m_pagePayload.Drop(m_bufMgr);
    m_delOffset = INVALID_ITEM_OFFSET_NUMBER;
    return DSTORE_FAIL;
}

RetStatus BtreeDelete::GenerateUndoRecord(uint8 currTdId, UndoRecord **undoRecord)
{
    if (unlikely(!m_pagePayload.GetLinkAndStatus()->TestType(BtrPageType::LEAF_PAGE))) {
        ErrLog(DSTORE_PANIC, MODULE_INDEX, ErrMsg("generate undoRecord failed for meta is not leaf page"));
    }

    *undoRecord = nullptr;
    uint32 dataSize = m_delTuple->GetSize();
    ItemPointerData ctid(m_pagePayload.GetPageId(), m_delOffset);

    /* Generate undoData including the index tuple. */
    UndoDataBtreeDelete *undoData =
        static_cast<UndoDataBtreeDelete *>(DstorePalloc(sizeof(UndoDataBtreeDelete) + dataSize));
    if (unlikely(undoData == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("DstorePalloc failed when GenerateUndoRecord."));
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        return DSTORE_FAIL;
    }
    undoData->Init();
    Xid metaCreateXid = GetBtreeSmgr()->GetMetaCreateXid();
    StorageAssert(metaCreateXid != INVALID_XID);
    undoData->SetMetaCreateXid(metaCreateXid);
    undoData->AppendData(static_cast<char *>(static_cast<void *>(m_delTuple)), dataSize);

    /* Generate undo record. */
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr \"Transaction\".", __FUNCTION__));
        return DSTORE_FAIL;
    }
    Xid curXid = transaction->GetCurrentXid();

    UndoType type = GetBtreeSmgr()->IsGlobalTempIndex() ? UNDO_BTREE_DELETE_TMP : UNDO_BTREE_DELETE;
    *undoRecord = DstoreNew(g_dstoreCurrentMemoryContext)
        UndoRecord(type, currTdId, m_pagePayload.GetPage()->GetTd(currTdId), ctid, m_cid);
    if (unlikely(*undoRecord == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("DstoreNew failed when GenerateUndoRecord."));
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        return DSTORE_FAIL;
    }
    RetStatus ret =
        (*undoRecord)->Append(STATIC_CAST_PTR_TYPE(undoData, char *), static_cast<int32>(undoData->GetSize()));
    if (STORAGE_FUNC_FAIL(ret)) {
        DstorePfree(undoData);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Append undo record fail when delete index tuple."));
        return DSTORE_FAIL;
    }
    DstorePfree(undoData);
    StoragePdb *pdb = g_storageInstance->GetPdb(this->GetPdbId());
    if (STORAGE_VAR_NULL(pdb)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr \"StoragePdb\".", __FUNCTION__));
        return DSTORE_FAIL;
    }
    TransactionMgr *txnMgr = pdb->GetTransactionMgr();
    if (STORAGE_VAR_NULL(pdb)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr \"TransactionMgr\".", __FUNCTION__));
        return DSTORE_FAIL;
    }
    if (STORAGE_FUNC_FAIL(txnMgr->ExtendUndoSpaceIfNeeded(curXid, (*undoRecord)->GetRecordSize()))) {
        HandleErrorWhenGetUndoBuffer(m_pagePayload.GetPage(), *undoRecord, m_delTuple);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Extend undo space fail when delete index tuple."));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus BtreeDelete::InsertUndoRecord(uint8 currTdId, UndoRecord *undoRecord)
{
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr \"Transaction\".", __FUNCTION__));
        return DSTORE_FAIL;
    }
    Xid curXid = transaction->GetCurrentXid();
    UndoRecPtr undoRecPtr = transaction->InsertUndoRecord(undoRecord);
    if (unlikely(undoRecPtr == INVALID_UNDO_RECORD_PTR)) {
        /* We haven't change the data page now. No need to rollback */
        return DSTORE_FAIL;
    }
    m_pagePayload.GetPage()->SetTd(currTdId, curXid, undoRecPtr, m_cid);
    return DSTORE_SUCC;
}

WalRecord *BtreeDelete::GenerateWalRecordForLeaf(uint8 tdId)
{
    StorageAssert((m_pagePayload.GetLinkAndStatus()->TestType(BtrPageType::LEAF_PAGE)));
    bool glsnChangedFlag = (m_pagePayload.GetPage()->GetWalId() != thrd->m_walWriterContext->GetWalId());
    /* Leaf pages have already called BeginAtomicWal before writing undo log */
    uint32 allocTdSize = WalRecordForDataPage::GetAllocTdSize(m_tdContext);
    uint32 redoDataSize = sizeof(WalRecordBtreeDeleteOnLeaf) + allocTdSize;
    auto *redoLeaf = static_cast<WalRecordBtreeDeleteOnLeaf *>(DstorePalloc(redoDataSize));
    if (unlikely(redoLeaf == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("DstorePalloc failed when GenerateWalRecordForLeaf."));
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        return nullptr;
    }
    /* get file version */
    if (unlikely(m_pagePayload.GetBuffDesc()->fileVersion == INVALID_FILE_VERSION)) {
        m_pagePayload.GetBuffDesc()->SetFileVersion(WalUtils::GetFileVersion(m_pagePayload.GetBuffDesc()->GetPdbId(),
            m_pagePayload.GetBuffDesc()->GetPageId().m_fileId));
    }
    ItemPointerData ctid = (m_delTuple != nullptr) ? m_delTuple->GetHeapCtid() : INVALID_ITEM_POINTER;
    redoLeaf->SetHeader(m_pagePayload.GetPage(), redoDataSize, glsnChangedFlag, m_delOffset, tdId,
                        m_pagePayload.GetBuffDesc()->GetFileVersion(), ctid);
    redoLeaf->SetAllocTd(m_tdContext);
    return redoLeaf;
}

void BtreeDelete::InsertWalRecordForInternal()
{
    Transaction *transaction = thrd->GetActiveTransaction();
    AtomicWalWriterContext *walContext = thrd->m_walWriterContext;
    walContext->BeginAtomicWal(transaction->GetCurrentXid());
    WalRecordBtreeDeleteOnInternal redoInternal;
    bool glsnChangedFlag = (m_pagePayload.GetPage()->GetWalId() != walContext->GetWalId());
    redoInternal.SetHeader(m_pagePayload.GetPage(), glsnChangedFlag, m_delOffset,
                           m_pagePayload.GetBuffDesc()->GetFileVersion());
    walContext->RememberPageNeedWal(m_pagePayload.GetBuffDesc());
    walContext->PutNewWalRecord(&redoInternal);
    (void)walContext->EndAtomicWal();
}

/*
 *  DeleteFromInternal() -- Delete a tuple on a particular internal page by given offset.
*/
RetStatus BtreeDelete::DeleteFromInternal(BufferDesc *internalBuf, OffsetNumber offset)
{
    if (STORAGE_VAR_NULL(thrd->GetActiveTransaction())) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr \"Transaction\".", __FUNCTION__));
        return DSTORE_FAIL;
    }
    if (unlikely(internalBuf == INVALID_BUFFER_DESC)) {
        storage_set_error(INDEX_ERROR_FAIL_READ_PAGE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr \"internalBuf\".", __FUNCTION__));
        return DSTORE_FAIL;
    }
    m_pagePayload.InitByBuffDesc(internalBuf);

    StorageAssert(m_pagePayload.GetLinkAndStatus()->TestType(BtrPageType::INTERNAL_PAGE));
    StorageAssert(offset <= m_pagePayload.GetPage()->GetMaxOffset());
    m_delOffset = offset;

    /* Delete tuple by set deleted flag */
    m_delTuple = m_pagePayload.GetPage()->GetIndexTuple(m_delOffset);
    m_delTuple->SetDeleted();
    m_pagePayload.GetPage()->SetTuplePrunable(true);

    if (STORAGE_FUNC_FAIL(m_bufMgr->MarkDirty(m_pagePayload.GetBuffDesc()))) {
        return DSTORE_FAIL;
    }

    /* Generate redo record for both leaf and pivot pages if needed */
    if (likely(NeedWal())) {
        InsertWalRecordForInternal();
    }

    ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
        ErrMsg("Deleted index tuple on pivot page ({%hu, %u}, %hu) successfully, downlink {%hu, %u}.",
        m_pagePayload.GetPageId().m_fileId, m_pagePayload.GetPageId().m_blockId, m_delOffset,
        m_delTuple->GetLowlevelIndexpageLink().m_fileId, m_delTuple->GetLowlevelIndexpageLink().m_blockId));
    /*
     * Prune pivot page.
     * Pivot page will be pruned everytime deletion occurs. Deletions on pivot pages would not happen as
     * frequently as on leaf pages since pivot tuple deletion is always a subsequence action of leaf page
     * recycling, which would not be triggerred very often.
     * Remove "holes" (deleted tuples) on pivot page in time will make scan operations smooth
     */
    BtreePagePrune btreePagePrune(m_indexRel, m_indexInfo, m_scanKeyValues.scankeys, m_pagePayload.GetBuffDesc());
    return btreePagePrune.Prune(m_delOffset);
}

/*
 *  DeleteFromLeaf() -- Delete a tuple on a particular leaf page in the index.
 *
 *      If the deletion deleted the last tuple in the page, the page may
 *      become empty after the last transaction committed. So we record it as
 *      empty page for further page recycle logic.
 */
RetStatus BtreeDelete::DeleteFromLeaf()
{
    StorageAssert(m_pagePayload.GetBuffDesc() != INVALID_BUFFER_DESC);
    StorageAssert(m_pagePayload.GetLinkAndStatus()->TestType(BtrPageType::LEAF_PAGE));

    /* Step 1: Allocate TD id. */
    TdId tdId = AllocAndSetTd(m_pagePayload.GetPage(), m_searchingTarget);
    FAULT_INJECTION_ACTION(DstoreTransactionFI::ALLOC_TD_FAIL, tdId = INVALID_TD_SLOT);
    if (m_needRecordUndo && unlikely(tdId == INVALID_TD_SLOT)) {
        ErrLog(DSTORE_WARNING, MODULE_INDEX,
               ErrMsg("failed to allocTd for btrPage(%hu, %u) " BTR_PAGE_HEADER_FMT, m_pagePayload.GetPageId().m_fileId,
                      m_pagePayload.GetPageId().m_blockId, BTR_PAGE_HEADER_VAL(m_pagePayload.GetPage())));

        /* Failed to alloc td slot. Prune the leaf and try again */
        BtreePagePrune *btreePagePrune = DstoreNew(g_dstoreCurrentMemoryContext)
            BtreePagePrune(m_indexRel, m_indexInfo, m_scanKeyValues.scankeys, m_pagePayload.GetBuffDesc());
        if (unlikely(btreePagePrune == nullptr)) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to allocate mem for btreePagePrune when index delete."));
            storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
            return DSTORE_FAIL;
        }
        if (STORAGE_FUNC_FAIL(btreePagePrune->Prune(INVALID_ITEM_OFFSET_NUMBER, m_delOffset))) {
            /* We don't return although Prune failed. We'll retry AllocTd in case some other transaction finished and
             * we can get a reused TD slot even if there's no extra space */
            ErrLog(DSTORE_WARNING, MODULE_INDEX,
                   ErrMsg("failed to prune btrPage(%hu, %u) when allocTd. Will retry. " BTR_PAGE_HEADER_FMT,
                          m_pagePayload.GetPageId().m_fileId, m_pagePayload.GetPageId().m_blockId,
                          BTR_PAGE_HEADER_VAL(m_pagePayload.GetPage())));
        }
        if (m_delOffset != btreePagePrune->GetLeafDelOffset()) {
            ErrLog(DSTORE_WARNING, MODULE_INDEX,
                   ErrMsg("DelOff changed after prune btrPage(%hu, %u), old delOff %hu, new delOff %hu.",
                          m_pagePayload.GetPageId().m_fileId, m_pagePayload.GetPageId().m_blockId,
                          m_delOffset, btreePagePrune->GetLeafDelOffset()));
        }
        m_delOffset = btreePagePrune->GetLeafDelOffset();
        if (m_pagePayload.GetPage()->GetFreeSpace<FreeSpaceCondition::RAW>() > sizeof(TD)) {
            m_tdContext.Init(this->GetPdbId(), NeedWal());
            tdId = m_pagePayload.GetPage()->AllocTd(m_tdContext);
        }
        delete btreePagePrune;
    }

    if (m_needRecordUndo && unlikely(tdId == INVALID_TD_SLOT)) {
        /* Still has insufficient space for td slot after page pruned. */
        ErrLog(DSTORE_WARNING, MODULE_INDEX,
            ErrMsg("failed to allodTd after Prune btrPage(%hu, %u), lower : %hu, upper : %hu." BTR_PAGE_HEADER_FMT
            BTR_PAGE_LINK_AND_STATUS_FMT, m_pagePayload.GetPageId().m_fileId, m_pagePayload.GetPageId().m_blockId,
            m_pagePayload.GetPage()->GetLower(), m_pagePayload.GetPage()->GetUpper(),
            BTR_PAGE_HEADER_VAL(m_pagePayload.GetPage()),
            BTR_PAGE_LINK_AND_STATUS_VAL(m_pagePayload.GetPage()->GetLinkAndStatus())));
        storage_set_error(INDEX_ERROR_FAIL_DELETE_TUPLE_FOR_ALLOCATE_TDID);
        /* need to unlock the current page, wait for any of in-progress transactions to finish and try again. */
        m_needRetrySearchBtree = true;
        return DSTORE_FAIL;
    }

    m_delTuple = m_pagePayload.GetPage()->GetIndexTuple(m_delOffset);
    ItemPointerData ctid = m_delTuple->GetHeapCtid();

    AtomicWalWriterContext *walContext = thrd->m_walWriterContext;
    WalRecordBtreeDeleteOnLeaf *walRecord = nullptr;

    BtreePageRecycle recycle(m_indexRel);
    RetStatus ret = DSTORE_FAIL;

    /* Step 2: Generate Undo record and Wal record */
    /* We've not write deleting flag on index tuple now. No need to restore page if we hits any failure, just reture */
    UndoRecord *undoRecord = nullptr;
    bool allocTdDirty = m_tdContext.allocTd.isDirty;
    if (m_needRecordUndo && STORAGE_FUNC_FAIL(GenerateUndoRecord(tdId, &undoRecord))) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to generate undo record for index \"%s\".",
            m_indexInfo->indexRelName));
        goto EXIT_ERROR;
    }
    StorageAssert(undoRecord != nullptr || !m_needRecordUndo);

    if (likely(NeedWal())) {
        if (unlikely(
            (walRecord = static_cast<WalRecordBtreeDeleteOnLeaf *>(GenerateWalRecordForLeaf(tdId))) == nullptr)) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to generate wal record for leaf of index \"%s\".",
                m_indexInfo->indexRelName));
            goto EXIT_ERROR;
        }
    }

    /* Step 3. Insert Undo and Wal records */
    walContext->BeginAtomicWal(thrd->GetCurrentXid());
    if (m_needRecordUndo && STORAGE_FUNC_FAIL(InsertUndoRecord(tdId, undoRecord))) {
        /* Still, deleted flag is not set yet, just abort wal atomic group and return fail */
        /* It's safe to keep the target index page unchaged. Because InsertUndoRecord would only fail for buffer
         * descriptor reading failure (for this case). The undoRecord would not be found (from neither disk nor memory)
          when Transaction abort, so there would be nothing to rollback. */
        walContext->ResetForAbort();
        delete undoRecord;
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to insert undo record for index \"%s\".",
            m_indexInfo->indexRelName));
        goto EXIT_ERROR;
    }

    /* Now we're changing the target page. */
    m_delTuple->SetTdId(tdId);
    m_delTuple->SetTdStatus(m_searchingTarget->GetTdStatus());
    m_delTuple->SetDeleted();
    m_delTuple->SetCcindexStatus(m_indexInfo->btrIdxStatus);
    m_pagePayload.GetPage()->SetTuplePrunable(true);

    (void)m_bufMgr->MarkDirty(m_pagePayload.GetBuffDesc());

    /* Insert wal record */
    if (likely(NeedWal())) {
        UndoRecPtr undoRecPtr =
            m_needRecordUndo ? m_pagePayload.GetPage()->GetTd(tdId)->GetUndoRecPtr() : INVALID_UNDO_RECORD_PTR;
        walRecord->SetUndoRecPtr(undoRecPtr);
        walContext->RememberPageNeedWal(m_pagePayload.GetBuffDesc());
        walContext->PutNewWalRecord(walRecord);
        DstorePfree(walRecord);
    }
    delete undoRecord;
    (void)walContext->EndAtomicWal();

    ErrLog(DSTORE_DEBUG1, MODULE_INDEX, ErrMsg("Deleted index tuple on leaf page ({%hu, %u}, %hu) td(%hhu) with heap "
        "ctid ({%hu, %u}, %hu) successfully, snapshot csn %lu",
        m_pagePayload.GetPageId().m_fileId, m_pagePayload.GetPageId().m_blockId, m_delOffset, tdId,
        ctid.GetFileId(), ctid.GetBlockNum(), ctid.GetOffset(), thrd->GetSnapShotCsn()));

    /*
    * Recycle leaf page if empty.
    * Pivot page will be recycled as a subsequence of leaf page recycle. Never recycle a pivot page directly.
    *
    * Do not call prune here because we don't want to compact a page as frequently as every time we do
    * deletion, which would badly impact performance. We just mark a to-be-empty page as empty here
    * and put it into PendingFreePageQueue for further recycle and reuse.
    */
    ret = recycle.PutIntoRecycleQueueIfEmpty(m_pagePayload.GetBuffDesc());
    /* m_pagePayload.GetBuffDesc() would be unlock and released no matter succeeded or not */
    m_pagePayload.buffDesc = INVALID_BUFFER_DESC;
    if (ret == DSTORE_SUCC) {
        /*
         * Note that although this to-be-empty page is set to "empty" in RecycleQueue now, it is still on
         * the btree and can be changed later. Make sure to check and fully remove the page from btree before
         * reusing every time we are delivered a page.
         */
        /* Try recycle pending free pages and add them into FreePageQueue if succeeded */
#ifndef UT
        bool needRecycleBackground = !GetBtreeSmgr()->IsGlobalTempIndex() &&
                                     g_storageInstance->GetGuc()->numObjSpaceMgrWorkers > 0;
        if (needRecycleBackground) {
            if (STORAGE_FUNC_FAIL(BtreePageRecycle::TryRegisterRecycleBtreeTask(GetBtreeSmgr()->GetMetaCreateXid(),
                m_indexInfo, m_scanKeyValues.scankeys, GetBtreeSmgr()->GetTablespaceId(),
                GetBtreeSmgr()->GetSegMetaPageId(), GetBtreeSmgr()->m_pdbId))) {
                ErrLog(DSTORE_ERROR, MODULE_INDEX,
                    ErrMsg("Failed to register btree Task for segment(%hu, %u) with tablespace id (%hu)",
                    GetBtreeSmgr()->GetSegMetaPageId().m_fileId, GetBtreeSmgr()->GetSegMetaPageId().m_blockId,
                    GetBtreeSmgr()->GetTablespaceId()));
                return DSTORE_FAIL;
            }
        } else {
            if (STORAGE_FUNC_FAIL(recycle.BatchRecycleBtreePage(m_indexInfo, m_scanKeyValues.scankeys))) {
                ErrLog(DSTORE_ERROR, MODULE_INDEX,
                    ErrMsg("Failed to recycle btree for segment(%hu, %u) with tablespace id (%hu)",
                    GetBtreeSmgr()->GetSegMetaPageId().m_fileId, GetBtreeSmgr()->GetSegMetaPageId().m_blockId,
                    GetBtreeSmgr()->GetTablespaceId()));
                return DSTORE_FAIL;
            }
        }
#endif
    }
    return DSTORE_SUCC;

EXIT_ERROR:
    if (allocTdDirty) {
        /* isDirty flag may has been clear when call GenerateWalRecordForLeaf success */
        m_tdContext.allocTd.isDirty = true;
        (void)m_bufMgr->MarkDirty(m_pagePayload.GetBuffDesc());
        GenerateAllocTdWal(m_pagePayload.GetBuffDesc(), m_tdContext);
    }
    if (unlikely(m_tdContext.allocTd.hasRollbackTd)) {
        (void)m_bufMgr->MarkDirty(m_pagePayload.GetBuffDesc());
        GenerateRollbackTdWal(m_pagePayload.GetBuffDesc(), m_tdContext);
    }
    return DSTORE_FAIL;
}

RetStatus BtreeDelete::DoDelete()
{
    AutoMemCxtSwitch autoMemCxtSwitch(thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_QUERY));

    /* find the first page containing this key */
    if (STORAGE_FUNC_FAIL(BtreeSplit::SearchBtree(&m_pagePayload.buffDesc, false, false))) {
        ItemPointerData heapCtid = m_searchingTarget->m_link.heapCtid;
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to delete indexTuple(%hu, %u, %hu) "
            "because search btree failed. index(%s:%u), segment(%hu, %u), createdXid(%d, %lu)",
            heapCtid.GetFileId(), heapCtid.GetBlockNum(), heapCtid.GetOffset(),
            m_indexInfo->indexRelName, m_indexRel->relOid, GetBtreeSmgr()->GetSegMetaPageId().m_fileId,
            GetBtreeSmgr()->GetSegMetaPageId().m_blockId,
            static_cast<int32>(GetBtreeSmgr()->GetMetaCreateXid().m_zoneId),
            GetBtreeSmgr()->GetMetaCreateXid().m_logicSlotId));
        return DSTORE_FAIL;
    }
    m_pagePayload.InitByBuffDesc(m_pagePayload.GetBuffDesc());
    StorageAssert(m_pagePayload.GetLinkAndStatus()->TestType(BtrPageType::LEAF_PAGE));

    /* looking for the first item >= scankey on the page */
    m_delOffset = BinarySearchOnPage(m_pagePayload.GetPage(), false);
    if (unlikely(StorageGetErrorCode() == COMMON_ERROR_FUNCTION_RETURN_NULL)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
                ErrMsg("Failed to binary search btrPage(%d, %u) since compare function returns null.",
                m_pagePayload.GetPageId().m_fileId, m_pagePayload.GetPageId().m_blockId));
        m_needRetrySearchBtree = false;
        m_pagePayload.Drop(m_bufMgr, false);
        return DSTORE_FAIL;
    }
    /* looking for itup with the same ctid starting from m_delOffset */
    Xid waitXid = INVALID_XID;
    if (STORAGE_FUNC_FAIL(FindDeleteLoc(&waitXid))) {
        if (waitXid != INVALID_XID) {
            /* The index tuple is deleting by an in-progress transaction. Wait the transaction end then check again */
            ErrLog(DSTORE_WARNING, MODULE_INDEX,
                ErrMsg("Target indexTuple(%hu, %u, %hu) is deleting, we will wait for xid(%d, %lu)",
                m_pagePayload.GetPageId().m_fileId, m_pagePayload.GetPageId().m_blockId, m_delOffset,
                static_cast<int32>(waitXid.m_zoneId), waitXid.m_logicSlotId));

            /* Need to unlock the target page before waiting transaction end */
            m_bufMgr->UnlockAndRelease(m_pagePayload.GetBuffDesc());
            m_pagePayload.buffDesc = INVALID_BUFFER_DESC;
            if (STORAGE_FUNC_FAIL(WaitForTxnEndIfNeeded(m_pagePayload.GetPageId(), waitXid))) {
                /* Sth. wrong when waiting for transaction end. Give up retry and return fail */
                ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to wait running transaction xid(%d, %lu) end"
                    "for index \"%s\".",
                    static_cast<int32>(waitXid.m_zoneId), waitXid.m_logicSlotId, m_indexInfo->indexRelName));
                return DSTORE_FAIL;
            }
            /* For now, the blocking transaction finished. */
            /* Need to search from root again. return DSTORE_FAIL and set m_needRetrySearchBtree true,
             * the caller will retry */
            m_needRetrySearchBtree = true;
            return DSTORE_FAIL;
        }

        StorageAssert(!m_needRetrySearchBtree);
        /* Nothing blocks us, just failed to find the deleting target. Give up retry and return fail */
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to find delete location for index \"%s\".",
            m_indexInfo->indexRelName));
        return DSTORE_FAIL;
    }

    ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
           ErrMsg("Found location of to-be-deleted index tuple ({%hu, %u}, %hu)", m_pagePayload.GetPageId().m_fileId,
                  m_pagePayload.GetPageId().m_blockId, m_delOffset));

    RetStatus status = DeleteFromLeaf();
    if (m_pagePayload.GetBuffDesc() != INVALID_BUFFER_DESC) {
        m_bufMgr->UnlockAndRelease(m_pagePayload.GetBuffDesc());
        m_pagePayload.buffDesc = INVALID_BUFFER_DESC;
    }
    /* fail on td allocation, wait for any transaction on page to finish, then try to find the tuple again. */
    if (STORAGE_FUNC_FAIL(status) && m_needRetrySearchBtree && STORAGE_FUNC_FAIL(WaitTxnEndForTdRealloc())) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to wait running transactions end for td allocation"
            "for index \"%s\".", m_indexInfo->indexRelName));
        m_needRetrySearchBtree = false;
        return DSTORE_FAIL;
    }

    return status;
}

#ifdef UT
RetStatus BtreeDelete::DeleteTupleByOffset(BufferDesc *buffer, OffsetNumber offset)
{
    m_pagePayload.InitByBuffDesc(buffer);
    StorageAssert(m_delTuple == nullptr);
    StorageAssert(offset != INVALID_ITEM_OFFSET_NUMBER);

    if (m_pagePayload.GetLinkAndStatus()->TestType(BtrPageType::LEAF_PAGE)) {
        m_delOffset = offset;
        m_searchingTarget = m_pagePayload.GetPage()->GetIndexTuple(m_delOffset);
        return DeleteFromLeaf();
    }
    if (m_pagePayload.GetLinkAndStatus()->TestType(BtrPageType::INTERNAL_PAGE)) {
        return DeleteFromInternal(buffer, offset);
    }
    return DSTORE_FAIL;
}
#endif

}
