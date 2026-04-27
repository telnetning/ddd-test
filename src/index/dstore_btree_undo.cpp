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
 * This file defines the BtreeUndo class.
 *
 * IDENTIFICATION
 *        src/index/dstore_btree_undo.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "index/dstore_btree_undo_data_struct.h"
#include "index/dstore_btree.h"
#include "page/dstore_index_page.h"
#include "transaction/dstore_transaction.h"
#include "undo/dstore_undo_zone.h"

namespace DSTORE {

BtreeUndoContext::BtreeUndoContext(PdbId pdbId, BufMgrInterface *bufMgr)
    : m_btrMetaPageId(INVALID_PAGE_ID),
      m_metaCreateXid(INVALID_XID),
      m_indexInfo(nullptr),
      m_needFreeIndexInfo(true),
      m_currBuf(INVALID_BUFFER_DESC),
      m_currPage(nullptr),
      m_offset(INVALID_ITEM_OFFSET_NUMBER),
      m_undoTuple(nullptr),
      m_isDeletionPruned(false),
      m_isIns4Del(false),
      m_xid(INVALID_XID),
      m_undoRec(nullptr),
      m_forCommittedTxrRollback(false),
      m_undoWithNoWal(false),
      m_endMatch(false),
      m_needCheckMetaPage(false),
      m_doNotRollbackDeleteOnRightPage(false),
      m_rollbackAbortedForCr(false),
      m_bufMgr(bufMgr),
      m_pdbId(pdbId),
      m_undoType(BtreeUndoContextType::ROLLBACK),
      m_skipRollbackTd(false)
{}

BtreeUndoContext::BtreeUndoContext(PdbId pdbId, PageId btrMetaPageId, IndexInfo *indexInfo, BufMgrInterface *bufMgr,
    Xid btrMetaCreateXid, BtreeUndoContextType type)
    : BtreeUndoContext(pdbId, bufMgr)
{
    StorageAssert(indexInfo != nullptr);
    StorageAssert(btrMetaPageId != INVALID_PAGE_ID);
    StorageAssert(btrMetaCreateXid != INVALID_XID);
    m_btrMetaPageId = btrMetaPageId;
    m_metaCreateXid = btrMetaCreateXid;
    m_indexInfo = indexInfo;
    m_needFreeIndexInfo = false;
    m_undoType = type;
}

/* We want only index and page info. Ignore undoRec & undoTuple */
BtreeUndoContext::BtreeUndoContext(const BtreeUndoContext &btrUndoContext, BtreeUndoContextType type)
    : BtreeUndoContext(btrUndoContext.m_pdbId, btrUndoContext.m_btrMetaPageId,
                       btrUndoContext.m_indexInfo, btrUndoContext.m_bufMgr, btrUndoContext.m_metaCreateXid)
{
    m_currBuf = btrUndoContext.m_currBuf;
    m_currPage = static_cast<BtrPage *>(m_currBuf->GetPage());
    StorageAssert(m_currPage == btrUndoContext.m_currPage);
    m_offset = btrUndoContext.m_offset;
    m_undoType = type;
}

RetStatus BtreeUndoContext::SetUndoInfo(Xid xid, UndoRecord *undoRec)
{
    ClearUndoRec();
    StorageAssert(undoRec != nullptr);
    m_xid = xid;
    m_undoRec = undoRec;

    UndoType type = m_undoRec->GetUndoType();
    if (type == UNDO_BTREE_INSERT || type == UNDO_BTREE_INSERT_TMP) {
        m_isIns4Del = static_cast<UndoDataBtreeInsert*>(m_undoRec->GetUndoData())->m_ins4Del;
    }
    m_undoTuple = GetIndexTupleFromUndoRec(m_undoRec);
    if (STORAGE_VAR_NULL(m_undoTuple)) {
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

void BtreeUndoContext::SetUnfoInfoWithIndexTuple(Xid xid, UndoRecord *undoRec, IndexTuple *undoTuple)
{
    ClearUndoRec();
    StorageAssert(undoRec != nullptr);
    StorageAssert(undoTuple != nullptr);
    m_xid = xid;
    m_undoRec = undoRec;
    m_undoTuple = undoTuple;
    m_isIns4Del = undoTuple->IsInsertDeletedForCCindex();
}

void BtreeUndoContext::Destroy()
{
    ClearUndoRec();
    if (m_needFreeIndexInfo && m_indexInfo != nullptr) {
        m_indexInfo->Free();
    }
    m_needFreeIndexInfo = false;
    m_indexInfo = nullptr;
}

RetStatus BtreeUndoContext::GetIndexInfo()
{
    StorageAssert(m_currPage != nullptr);
    if (m_indexInfo != nullptr && m_btrMetaPageId == m_currPage->GetBtrMetaPageId() &&
        /* add btree meta create xid verify to advoid meta page id is reused */
        (!m_needCheckMetaPage || m_metaCreateXid == m_currPage->GetBtrMetaCreateXid())) {
        return DSTORE_SUCC;
    }
    if (m_needFreeIndexInfo && m_indexInfo != nullptr) {
        m_indexInfo->Free();
    }
    m_indexInfo = nullptr;

    /* Do not let ReadAndCheckBtrPage check page for us if m_needCheckMetaPage is set true, because
     * ReadAndCheckBtrPage's checking is too strict for the asynchonorous process. We'll check the meta page ourself
     * with a milder way that it would not be panic if meta page is expired.
     * We'll return SUCC with m_indexInfo = nullptr to tell caller that there's nothing wrong but the undoRec expired */
    BufferDesc *metaBufDesc =
        Btree::ReadAndCheckBtrPage(m_currPage->GetBtrMetaPageId(), LW_SHARED, m_bufMgr, m_pdbId, !m_needCheckMetaPage);
    if (STORAGE_VAR_NULL(metaBufDesc)) {
        return DSTORE_FAIL;
    }
    if (m_needCheckMetaPage && unlikely(!BtreeUndoMetaPageCheck(metaBufDesc, m_undoRec))) {
        ErrLog(DSTORE_LOG, MODULE_INDEX,
            ErrMsg("BtreeUndoMetaPageCheck fail, UndoType(%u) undoRecPtr(%hu, %u, %hu), currPage:" BTR_PAGE_HEADER_FMT
                       BTR_PAGE_LINK_AND_STATUS_FMT,
            m_undoRec->GetUndoType(), m_undoRec->GetCtid().GetFileId(), m_undoRec->GetCtid().GetBlockNum(),
            m_undoRec->GetCtid().GetOffset(), BTR_PAGE_HEADER_VAL(m_currPage),
            BTR_PAGE_LINK_AND_STATUS_VAL(m_currPage->GetLinkAndStatus())));
        m_bufMgr->UnlockAndRelease(metaBufDesc);
        return DSTORE_SUCC;
    }
    m_indexInfo = m_currPage->GetIndexInfoFromMetaPage(metaBufDesc);
    m_bufMgr->UnlockAndRelease(metaBufDesc);
    if (STORAGE_VAR_NULL(m_indexInfo)) {
        return DSTORE_FAIL;
    }
    m_needFreeIndexInfo = true;
    m_metaCreateXid = m_currPage->GetBtrMetaCreateXid();
    m_btrMetaPageId = m_currPage->GetBtrMetaPageId();
    return DSTORE_SUCC;
}

void BtreeUndoContext::ClearUndoRec()
{
    if (m_undoTuple) {
        AutoMemCxtSwitch autoMemCxtSwitch(thrd->GetUndoMemoryCtx());
        DstorePfreeExt(m_undoTuple);
    }
    m_undoRec = nullptr;
    m_offset = INVALID_ITEM_OFFSET_NUMBER;
    m_isDeletionPruned = false;
    m_xid = INVALID_XID;
    m_endMatch = false;
}

RetStatus BtreeUndoContext::InitWithBtrPage(BtrPage *page, BufferDesc *bufDesc)
{
    StorageAssert(page != nullptr);
    m_currPage = page;
    m_currBuf = bufDesc;
    return GetIndexInfo();
}

bool BtreeUndoContext::BtreeUndoMetaPageCheck(BufferDesc* bufDesc, UndoRecord *undoRec)
{
    BtrPage *metaPage = static_cast<BtrPage *>(bufDesc->GetPage());
    PageId metaPageId = m_currPage->GetBtrMetaPageId();
    PageId readPageId = metaPage->GetSelfPageId();
    if (unlikely(metaPageId != readPageId)) {
        ErrLog(DSTORE_LOG, MODULE_INDEX,
            ErrMsg("BtreeUndoMetaPageCheck fail PageId. Meta btrPage(%hu, %u) requested but Page(%hu, %u) returned",
                   metaPageId.m_fileId, metaPageId.m_blockId, readPageId.m_fileId, readPageId.m_blockId));
        return false;
    }
    if (!unlikely(metaPage->TestType(PageType::INDEX_PAGE_TYPE))) {
        ErrLog(DSTORE_LOG, MODULE_INDEX,
            ErrMsg("BtreeUndoMetaPageCheck fail PageType %hu", metaPage->m_header.m_type));
        return false;
    }
    BtrMeta *btrMeta = static_cast<BtrMeta *>(static_cast<void *>(metaPage->GetData()));
    if (unlikely(!metaPage->GetLinkAndStatus()->TestType(BtrPageType::META_PAGE)) ||
        btrMeta->initializedtMagicNum != BTREE_META_STAT_INIT_MAGIC_NUM) {
        ErrLog(DSTORE_LOG, MODULE_INDEX,
            ErrMsg("BtreeUndoMetaPageCheck fail PageType %u", metaPage->GetLinkAndStatus()->GetType()));
        return false;
    }

    Xid undoMetaCreateXid = INVALID_XID;
    UndoType undoType = undoRec->GetUndoType();
    if (undoType == UNDO_BTREE_INSERT || undoType == UNDO_BTREE_INSERT_TMP) {
        undoMetaCreateXid = (static_cast<UndoDataBtreeInsert *>(undoRec->GetUndoData()))->GetMetaCreateXid();
    } else {
        undoMetaCreateXid = (static_cast<UndoDataBtreeDelete *>(undoRec->GetUndoData()))->GetMetaCreateXid();
    }

    if (unlikely(btrMeta->GetCreateXid() != undoMetaCreateXid)) {
        ErrLog(DSTORE_LOG, MODULE_INDEX,
            ErrMsg("BtreeUndoMetaPageCheck fail metaCreateXid(%d, %lu), undoMetaCreateXid(%d, %lu)",
            static_cast<int32>(btrMeta->GetCreateXid().m_zoneId), btrMeta->GetCreateXid().m_logicSlotId,
            static_cast<int32>(undoMetaCreateXid.m_zoneId), undoMetaCreateXid.m_logicSlotId));
        return false;
    }

    return true;
}

/* For abort/rollback-for-recovery that we must found the undo record on some page. */
BtreeUndoContext *BtreeUndoContext::FindUndoRecRelatedPage(BtreeUndoContext *btrUndoContext, PdbId pdbId,
                                                           BufferDesc *currBuf, Xid xid, UndoRecord *undoRec,
                                                           BufMgrInterface *bufMgr, bool &needFreeOutside,
                                                           UndoRecPtr undoRecPtr, bool isCommitTrx)
{
    StorageAssert(currBuf != INVALID_BUFFER_DESC);
    BtrPage *btrPage = static_cast<BtrPage*>(currBuf->GetPage());
    PageId btrMetaPageId = btrPage->GetBtrMetaPageId();
    if (btrUndoContext != nullptr) {
        if (btrMetaPageId != btrUndoContext->m_btrMetaPageId) {
            btrUndoContext->Destroy();
            delete btrUndoContext;
            btrUndoContext = nullptr;
        } else {
            btrUndoContext->ClearUndoRec();
        }
    }
    if (btrUndoContext == nullptr) {
        btrUndoContext = DstoreNew(g_dstoreCurrentMemoryContext) BtreeUndoContext(pdbId, bufMgr);
        if (STORAGE_VAR_NULL(btrUndoContext)) {
            storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("failed to alloc %lu", sizeof(BtreeUndoContext)));
            return nullptr;
        }
    }

    /* When rollback to consistent point for backup restore, all transactions are committed. */
    btrUndoContext->m_forCommittedTxrRollback = isCommitTrx;
    btrUndoContext->m_needCheckMetaPage = true;
    btrUndoContext->m_undoRec = undoRec;
    if (STORAGE_FUNC_FAIL(btrUndoContext->InitWithBtrPage(btrPage, currBuf))) {
        /* Failed to get index info according to the page. Tell caller stop to rollback since some error occurred */
        btrUndoContext->Destroy();
        delete btrUndoContext;
        return nullptr;
    }
    if (STORAGE_VAR_NULL(btrUndoContext->m_indexInfo)) {
        /* There's nothing wrong but the undo record is expired. Release currBuf to notice caller skipping this one. */
        btrUndoContext->m_bufMgr->UnlockAndRelease(currBuf, BufferPoolUnlockContentFlag::DontCheckCrc());
        btrUndoContext->m_currBuf = INVALID_BUFFER_DESC;
        return btrUndoContext;
    }
    if (STORAGE_FUNC_FAIL(btrUndoContext->SetUndoInfo(xid, undoRec))) {
        btrUndoContext->Destroy();
        delete btrUndoContext;
        return nullptr;
    }
    btrUndoContext->m_endMatch = false;
    if (STORAGE_FUNC_FAIL(btrUndoContext->FindUndoRecRelatedPage(undoRecPtr))) {
        needFreeOutside = false;
        btrUndoContext->Destroy();
        delete btrUndoContext;
        return nullptr;
    }

    return btrUndoContext;
}

/*
 * Due to the page split mechanism of btree, index tuple ctid might be changed.
 * However, the invariant we can guarantee is that ctid-changed tuple can always
 * be found through the right sibling link of the origin btree page whose page id
 * is recorded in undo record, this is based on the principle that index tuple never
 * move through the left sibling link of btree page (no btree page merge).
 *
 * We use this function in two situations:
 * 1. Rollback all undo records of one transaction in UndoZone::rollback_trx_slot.
 *    We're goning to find the exact page to apply undo.
 *    If page has been split after undoRec generated, the page id which undoRec points to must be the left page.
 *    we need to step right and search for the correct page where the related tuple currently be on that the tuple
 *    is strictly less than page high key or we've reached the rightmost page.
 * 2. Construct cr page in DataPage::Undo or rollback transaction on a specific page
 *    We're going to check if this undo record belong to the current page.
 *    We only rollback current page, but the undoRec may belong to either left sibling page or right sibling page.
 *    If undoRec's page ID doesn't match the current page ID, we should compare tuple of undoRec with both firstkey
 *    and highkey; If the pages matches, we only need to compare the tuple with highkey.
 * 1. For most cases(highkey doesn't have heap ctid), for example, tuple keys are 1, 2, 3, 4 in current page,
 *    tuple keys are 5, 6, 7 in right page, so current page high key is 5. If cmpHighkey < 0, the tuple
 *    belongs to current or left page, if cmpHighkey > 0, the tuple belongs to right page. cmpHighkey won't
 *    be equal to 0.
 * 2. For special case 1 when highkey has heap ctid. For example, The last tuple(key + heap ctid) in current
 *    page is (2, (2, 123, 4)), the first tuple(key + heap ctid) in right page is (2, (2, 123, 5)), so
 *    current page high key is (2, (2, 123, 5)). If cmpHighkey = 0, the tuple belongs to right page.
 *     <0 and >0 are same as above.
 * 3. For special case 2 when highkey is the same with the last tuple in current page, The last
 *    tuple(key + heap ctid) in current page is (2, (2, 123, 4)), the first tuple(key + heap ctid) in right
 *    page is (2, (2, 123, 4)), so current page high key is (2, (2, 123, 4)). If cmpHighkey = 0, the tuple
 *    can belong to both page. To avoid double-rollback a tuple on both pages, we would process all tuples having the
 *    same key with highkey ON LEFT PAGE iff cmpHighkey has a m_isSameWithLastLeft flag.
 */
bool BtreeUndoContext::DoesUndoRecMatchCurrCrPage()
{
    StorageAssert (m_indexInfo != nullptr);
    StorageAssert(m_currPage != nullptr);
    StorageAssert(m_currBuf == INVALID_BUFFER_DESC);
    StorageAssert(m_undoRec != nullptr);

    PageId currPageId = m_currPage->GetSelfPageId();
    UndoType type = m_undoRec->GetUndoType();
    StorageAssert(!m_isIns4Del);
    bool isDeleted = (type == UNDO_BTREE_DELETE || type == UNDO_BTREE_DELETE_TMP);

    /* Step 1. Early stop in some conditions. */
    if (unlikely(m_currPage->GetMaxOffset() < m_currPage->GetLinkAndStatus()->GetFirstDataOffset())) {
        if (!isDeleted) {
            /* The undoRec of insertion must not belong to an empty page */
            ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
                ErrMsg("BtreeInsert undoRecPtr(%hu, %u, %hu) for CR is not on current btrPage(%hu, %u). Page is empty.",
                m_undoRec->GetCtid().GetFileId(), m_undoRec->GetCtid().GetBlockNum(), m_undoRec->GetCtid().GetOffset(),
                currPageId.m_fileId, currPageId.m_blockId));
            return false;
        }
        StorageAssert(m_currPage->GetMaxOffset() >= BTREE_PAGE_HIKEY);
    }

    /* Step 2. Compare tuple with high key, maybe also check for existence exactly */
    int cmpHighkey = -1;
    bool isSameWithLastLeft = false;
    if (!m_currPage->IsRightmost()) {
        IndexTuple *highKey = m_currPage->GetIndexTuple(BTREE_PAGE_HIKEY);
        cmpHighkey = IndexTuple::Compare(m_undoTuple, highKey, m_indexInfo);
        isSameWithLastLeft = highKey->IsSameWithLastLeft();
    }
    if (cmpHighkey > 0) {
        /* As long as the tuple > highkey, it must not belong to this page. */
        ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
            ErrMsg("%s UndoRecPtr(%hu, %u, %hu) for CR is not on current btrPage(%hu, %u). Greater than hikey.",
            isDeleted ? "BtreeDelete" : "BtreeInsert",
            m_undoRec->GetCtid().GetFileId(), m_undoRec->GetCtid().GetBlockNum(), m_undoRec->GetCtid().GetOffset(),
            currPageId.m_fileId, currPageId.m_blockId));
        return false;
    }

    /* The target is less or equal to hikey now. Should check lowkey */
    int cmpLowkey = 1;
    if (!m_currPage->IsLeftmost()) {
        cmpLowkey = IndexTuple::Compare(m_undoTuple,
            m_currPage->GetIndexTuple(m_currPage->GetLinkAndStatus()->GetFirstDataOffset()), m_indexInfo);
    }
    if (cmpLowkey < 0) {
        /* As long as the tuple < lowkey, it must not belong to this page. */
        ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
            ErrMsg("%s UndoRecPtr(%hu, %u, %hu) for CR is not on current btrPage(%hu, %u). Less than lowkey.",
            isDeleted ? "BtreeDelete" : "BtreeInsert",
            m_undoRec->GetCtid().GetFileId(), m_undoRec->GetCtid().GetBlockNum(), m_undoRec->GetCtid().GetOffset(),
            currPageId.m_fileId, currPageId.m_blockId));
        return false;
    }

    /* Now the tuple value is within the range of [lowkey, highkey]. Try find it */
    bool recordIsOnPage = FindTupleOnPage(isDeleted);
    if (recordIsOnPage ||   /* Found it */
        !isDeleted) {       /* For insertion case, it would never be pruned. Just return the result. */
        return recordIsOnPage;
    }
    if (m_doNotRollbackDeleteOnRightPage) {
        /* Do not rollback delete on right page, because it should rollback on left, return false for right page. */
        goto FAILED_FIND_TUPLE;
    }
    if (cmpHighkey == 0 && isSameWithLastLeft && !m_rollbackAbortedForCr) {
        /* no matter the tuple is on current page or not, always do rollback on left for CR */
        ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
            ErrMsg("Delete undoRecPtr(%hu, %u, %hu) is not on btrPage(%hu, %u). But rollback for CR SameWithLastLeft."
            BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
            m_undoRec->GetCtid().GetFileId(), m_undoRec->GetCtid().GetBlockNum(), m_undoRec->GetCtid().GetOffset(),
            currPageId.m_fileId, currPageId.m_blockId,
            BTR_PAGE_HEADER_VAL(m_currPage), BTR_PAGE_LINK_AND_STATUS_VAL(m_currPage->GetLinkAndStatus())));
            m_isDeletionPruned = true;
            return true;
    }
    if (!m_forCommittedTxrRollback) {
        /* We'll never prune a tuple if it has not been committed */
        ErrLog(DSTORE_LOG, MODULE_INDEX,
            ErrMsg("%s undoRecPtr(%hu, %u, %hu) for CR is not on current btrPage(%hu, %u). Not found on page. "
            BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
            isDeleted ? "BtreeDelete" : "BtreeInsert",
            m_undoRec->GetCtid().GetFileId(), m_undoRec->GetCtid().GetBlockNum(), m_undoRec->GetCtid().GetOffset(),
            currPageId.m_fileId, currPageId.m_blockId,
            BTR_PAGE_HEADER_VAL(m_currPage), BTR_PAGE_LINK_AND_STATUS_VAL(m_currPage->GetLinkAndStatus())));
        goto FAILED_FIND_TUPLE;
    }

    /* The tuple is deleted and it's not on the current page now. */
    /* The target is greater than or equal to the first data tuple on current page. It should be on current page */
    /* Or we're on the left most. The tuple must be on this page. */
    m_isDeletionPruned = true;
    return true;

FAILED_FIND_TUPLE:
    ErrLog(DSTORE_LOG, MODULE_INDEX,
            ErrMsg("%s undoRecPtr(%hu, %u, %hu) for CR is not on current btrPage(%hu, %u). Not found on page. %s"
            BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
            isDeleted ? "BtreeDelete" : "BtreeInsert",
            m_undoRec->GetCtid().GetFileId(), m_undoRec->GetCtid().GetBlockNum(), m_undoRec->GetCtid().GetOffset(),
            currPageId.m_fileId, currPageId.m_blockId, m_rollbackAbortedForCr ? "already aborted" : "",
            BTR_PAGE_HEADER_VAL(m_currPage), BTR_PAGE_LINK_AND_STATUS_VAL(m_currPage->GetLinkAndStatus())));
    return false;
}

bool BtreeUndoContext::DoesUndoRecMatchCurrPage(bool &pageRollbackSkip)
{
    StorageAssert(m_undoTuple != nullptr);

    UndoType type = m_undoRec->GetUndoType();
    /* For uncommitted record, we have a special case when the index is under concurrently building that the
     * insertion undo record is for an InsertionForDeleting case.
     * For this case, the undo tuple is UNDO_BTREE_INSERT while the deleted flag on tuple is true */
    bool isDeleted = (type == UNDO_BTREE_DELETE || type == UNDO_BTREE_DELETE_TMP || m_isIns4Del);
    if (m_forCommittedTxrRollback) {
        return DoesUndoRecMatchCurrPageForCommittedRec(isDeleted);
    }
    return DoesUndoRecMatchCurrPageForUncommittedRec(isDeleted, pageRollbackSkip);
}

bool BtreeUndoContext::DoesUndoRecMatchCurrPageForUncommittedRec(bool isDeleted, bool &pageRollbackSkip)
{
    /* Step 1. Early stop in some conditions. */
    if (unlikely(m_currPage->GetMaxOffset() < m_currPage->GetLinkAndStatus()->GetFirstDataOffset())) {
        /* The uncommitted undoRec must not belong to an empty page */
        return false;
    }

    int cmpHighkey = -1;
    if (!m_currPage->IsRightmost()) {
        cmpHighkey = IndexTuple::Compare(m_undoTuple, m_currPage->GetIndexTuple(BTREE_PAGE_HIKEY), m_indexInfo);
    }
    if (cmpHighkey > 0) {
        /* As long as the tuple > highkey, it must not belong to this page. */
        return false;
    }

    bool tupleExists = FindTupleOnPage(isDeleted);
    if (likely(tupleExists)) {
        return true;
    }

    if (m_currPage->IsRightmost() || cmpHighkey < 0) {
        m_endMatch = true;
    }

    if (unlikely(m_undoType == BtreeUndoContextType::PAGE_ROLLBACK)) {
        if (unlikely(cmpHighkey == 0)) {
            IndexTuple *hikey = m_currPage->GetIndexTuple(BTREE_PAGE_HIKEY);
            pageRollbackSkip = hikey->IsSameWithLastLeft();
        }
    }

    if (unlikely(!tupleExists && m_endMatch)) {
        ItemPointerData heapCtid = m_undoTuple->GetHeapCtid();
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
            ErrMsg("Failed to find undoRec of indexTuple(%hu, %u, %hu) with heapCtid(%hu, %u, %hu), undoType:%hu/%hu, "
            "orig undoRec was indexTuple(%hu, %u, %hu), tdId(%hhu), xid(%d, %lu), cid(%d), %srollback TD. BtrPage:\n%s",
            m_currPage->GetSelfPageId().m_fileId, m_currPage->GetSelfPageId().m_blockId, m_offset,
            heapCtid.GetFileId(), heapCtid.GetBlockNum(), heapCtid.GetOffset(),
            static_cast<uint8>(m_undoRec->GetUndoType()), static_cast<uint8>(m_undoType),
            m_undoRec->GetPageId().m_fileId, m_undoRec->GetPageId().m_blockId, m_undoRec->GetCtid().GetOffset(),
            m_undoRec->GetTdId(), static_cast<int>(m_xid.m_zoneId),
            m_xid.m_logicSlotId, m_undoRec->GetCid(), m_skipRollbackTd ? "skip " : "",
            m_currPage->DumpLeafPageForLogs()));
    }
    return tupleExists;
}

bool BtreeUndoContext::DoesUndoRecMatchCurrPageForCommittedRec(bool isDeleted)
{
    /* Step 1. Early stop in some conditions. */
    if (unlikely(m_currPage->GetMaxOffset() < m_currPage->GetLinkAndStatus()->GetFirstDataOffset())) {
        if (!isDeleted) {
            /* The undoRec of insertion must not belong to an empty page */
            return false;
        }
        if (m_currPage->IsRightmost()) {
            /* It's the right most page and the tuple was deleted and pruned. need to insert it back */
            m_isDeletionPruned = true;
            m_offset = BTREE_PAGE_HIKEY;
            return true;
        }
        StorageAssert(m_currPage->GetMaxOffset() >= BTREE_PAGE_HIKEY);
    }

    /* Step 2. Compare tuple with high key, maybe also check for existence exactly */
    /*
     * 1. For most cases(highkey doesn't have heap ctid), for example, tuple keys are 1, 2, 3, 4 in current page,
     *    tuple keys are 5, 6, 7 in right page, so current page high key is 5. If cmpHighkey < 0, the tuple
     *    belongs to current or left page, if cmpHighkey > 0, the tuple belongs to right page. cmpHighkey won't
     *    be equal to 0.
     * 2. For special case 1 when highkey has heap ctid. For example, The last tuple(key + heap ctid) in current
     *    page is (2, (2, 123, 4)), the first tuple(key + heap ctid) in right page is (2, (2, 123, 5)), so
     *    current page high key is (2, (2, 123, 5)). If cmpHighkey = 0, the tuple belongs to right page.
     *    <0 and >0 are same as above.
     * 3. For special case 2 when highkey is the same with the last tuple in current page, The last
     *    tuple(key + heap ctid) in current page is (2, (2, 123, 4)), the first tuple(key + heap ctid) in right
     *    page is (2, (2, 123, 4)), so current page high key is (2, (2, 123, 4)). If cmpHighkey = 0, the tuple
     *    can belong to both page. Must do some extra check using xid and cid to search for the exact position
     *    of the tuple.
     */
    int cmpHighkey = -1;
    if (!m_currPage->IsRightmost()) {
        cmpHighkey = IndexTuple::Compare(m_undoTuple, m_currPage->GetIndexTuple(BTREE_PAGE_HIKEY), m_indexInfo);
    }
    if (cmpHighkey > 0) {
        /* As long as the tuple > highkey, it must not belong to this page. */
        return false;
    }
    if (m_currPage->IsRightmost() || cmpHighkey < 0) {
        m_endMatch = true;
    }
    /* The rightmost page is the last page we're checking. The undoRecord must belong to some page. */
    bool tupleExists = FindTupleOnPage(isDeleted);
    StorageAssert(!tupleExists || m_offset != INVALID_ITEM_OFFSET_NUMBER);
    if (!isDeleted) {
        /* For insertion, the undo tuple has to be on the page if it's already the rightmost page */
        StorageAssert(!m_currPage->IsRightmost() || tupleExists);
        return tupleExists;
    }
    /* For deleteion, need to check if we need to insert it back */
    if (cmpHighkey == 0) {
        /* Need to go on checking the right page if the tuple is not on current page. */
        return tupleExists;
    }
    /* The tuple must be greater than the left hikey because we got this undoRec from transaction that always record
     * where the tuple actually was on, then traverse from the original page to the current page.
     * Now the tuple is smaller than the current hikey, meaning the record must belong to this page.
     * If the tuple is not on the page now, it must had been pruned and m_offset may be invalid.
     * Set m_isDeletionPruned flag to tell caller to restore it back to the page. */
    m_isDeletionPruned = !tupleExists;
    return true;
}

RetStatus BtreeUndoContext::FindUndoRecRelatedPage(UndoRecPtr undoRecPtr)
{
    AtomicWalWriterContext *walContext = thrd->m_walWriterContext;
    if (STORAGE_VAR_NULL(walContext)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_UNDO,
               ErrMsg("Invalid wal context when FindUndoRecRelatedPage."));
        m_bufMgr->UnlockAndRelease(m_currBuf, BufferPoolUnlockContentFlag::DontCheckCrc());
        m_currBuf = INVALID_BUFFER_DESC;
        return DSTORE_FAIL;
    }
    bool pageRollbackSkip = false;
    while (!DoesUndoRecMatchCurrPage(pageRollbackSkip)) {
        /* The undo record doesn't match current page now, need to rollback td of current page */
        TD *td = m_currPage->GetTd(m_undoRec->GetTdId());
        if (likely(td->GetXid() == m_xid && td->GetUndoRecPtr() == undoRecPtr)) {
            td->RollbackTdInfo(m_undoRec);
            m_undoRec->SetCtid({m_currPage->GetSelfPageId(), INVALID_ITEM_OFFSET_NUMBER});
            (void)m_bufMgr->MarkDirty(m_currBuf);
            walContext->BeginAtomicWal(m_xid);
            UndoZone::GenerateWalForRollback(m_currBuf, *m_undoRec, WAL_UNDO_BTREE);
            (void)walContext->EndAtomicWal();
        } else {
            bool isDelete = (m_undoRec->GetUndoType() == UNDO_BTREE_DELETE ||
                             m_undoRec->GetUndoType() == UNDO_BTREE_DELETE_TMP);
            ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
                ErrMsg("btrPage(%hu, %u) does not have TD info for undoRecPtr(%hu, %u, %hu), "
                       "undoRec was for indexTuple(%hu, %u, %hu) %s, tdId(%hhu), heapCtid(%hu, %u, %hu)"
                       BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
                       m_currBuf->GetPageId().m_fileId, m_currBuf->GetPageId().m_blockId,
                       undoRecPtr.GetFileId(), undoRecPtr.GetBlockNum(), undoRecPtr.GetOffset(),
                       m_undoRec->GetCtid().GetFileId(), m_undoRec->GetCtid().GetBlockNum(),
                       m_undoRec->GetCtid().GetOffset(), isDelete ? "delete" : "insert",
                       m_undoRec->GetTdId(), m_undoTuple->m_link.heapCtid.GetFileId(),
                       m_undoTuple->m_link.heapCtid.GetBlockNum(), m_undoTuple->m_link.heapCtid.GetOffset(),
                       BTR_PAGE_HEADER_VAL(m_currPage), BTR_PAGE_LINK_AND_STATUS_VAL(m_currPage->GetLinkAndStatus())));
        }
        PageId rightPageId = m_currPage->GetRight();
        m_bufMgr->UnlockAndRelease(m_currBuf, BufferPoolUnlockContentFlag::DontCheckCrc());
        m_currBuf = INVALID_BUFFER_DESC;
        if (m_endMatch || rightPageId == INVALID_PAGE_ID) {
            break;
        }
        /* Do not let ReadAndCheckBtrPage check page for us if m_needCheckMetaPage is set true, because
         * ReadAndCheckBtrPage's checking is too strict for the asynchonorous process. We'll check the meta page ourself
         * with a milder way that it would not be panic if meta page is expired, we'll just return fail. */
        m_currBuf = Btree::ReadAndCheckBtrPage(rightPageId, LW_EXCLUSIVE, m_bufMgr, m_pdbId, !m_needCheckMetaPage);
        if (STORAGE_VAR_NULL(m_currBuf)) {
            return DSTORE_FAIL;
        }
        m_currPage = static_cast<BtrPage*>(m_currBuf->GetPage());
        if (m_needCheckMetaPage && unlikely(!BtrPage::IsBtrPageValid(m_currBuf->GetPage(), m_metaCreateXid))) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX,
                ErrMsg("btrPage(%hu, %u) is not valid" BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
                m_currBuf->GetPageId().m_fileId, m_currBuf->GetPageId().m_blockId,
                BTR_PAGE_HEADER_VAL(m_currPage), BTR_PAGE_LINK_AND_STATUS_VAL(m_currPage->GetLinkAndStatus())));
            m_bufMgr->UnlockAndRelease(m_currBuf, BufferPoolUnlockContentFlag::DontCheckCrc());
            m_currBuf = INVALID_BUFFER_DESC;
            break;
        }
    }
    StorageAssert(!pageRollbackSkip);
    return DSTORE_SUCC;
}

bool BtreeUndoContext::FindTupleOnPage(bool isDeleted)
{
    StorageAssert(m_currPage != nullptr);
    StorageAssert(m_currPage->IsRightmost() ||
                  IndexTuple::Compare(m_undoTuple, m_currPage->GetIndexTuple(BTREE_PAGE_HIKEY), m_indexInfo) <= 0);

    bool found = false;
    bool isEqual = false;
    m_offset = INVALID_ITEM_OFFSET_NUMBER;
    OffsetNumber offset = m_currPage->BinarySearch(m_undoTuple, m_indexInfo, &isEqual);
    m_doNotRollbackDeleteOnRightPage = false;
    OffsetNumber firstDataOffset = m_currPage->IsRightmost() ? BTREE_PAGE_HIKEY : BTREE_PAGE_FIRSTKEY;
    if (!isEqual || offset == INVALID_ITEM_OFFSET_NUMBER) {
        /* The tuple at offset is not equal to m_undoTuple, meaning the undoRec is not on current page now. */
        return false;
    }

    for (OffsetNumber checkOff = offset; checkOff <= m_currPage->GetMaxOffset(); checkOff++) {
        ItemId *currId = m_currPage->GetItemIdPtr(checkOff);
        if (unlikely(currId->IsUnused() || currId->IsNoStorage())) {
            isEqual = false;
            continue;
        }
        IndexTuple *currTup = m_currPage->GetIndexTuple(currId);
        if (!isEqual && IndexTuple::Compare(m_undoTuple, currTup, m_indexInfo) != 0) {
            /* We've traversed all tuples that have the same key and failed to find the exact record */
            break;
        }
        /* m_isSameWithLastLeft tuple may be rollback, but the flag is still valid.
         * Must check this flag before skip RangeHolder case */
        if (checkOff == firstDataOffset && currTup->IsSameWithLastLeft() && isDeleted) {
            if (m_undoType == BtreeUndoContextType::PAGE_ROLLBACK) {
                m_doNotRollbackDeleteOnRightPage = true;
                m_skipRollbackTd = true;
                break;
            }
            if (m_undoType == BtreeUndoContextType::CONSTRUCT_CR) {
                m_doNotRollbackDeleteOnRightPage = true;
                if (!m_rollbackAbortedForCr) {
                    /* For aborted rollback, continue to check if the tuple is really on current page.
                     * We'll rollback the aborted deletion if found, and skip it if missed. */
                    break;
                }
            }
        }
        /* Then we can skip range holders, they are invisible to all. */
        if (currId->IsRangePlaceholder()) {
            isEqual = false;
            continue;
        }
        StorageAssert(currId->IsNormal());
        if (isDeleted) {
            if (!currTup->IsDeleted()) {
                /* We'll never find a deletion after an insertion with the same key and heapCtid */
                /* Also we won't rollback a deletion who has the same heapCtid with an insertion because heapCtid
                 * for insertion must always be unique */
                m_endMatch = true;
                return false;
            }
            found = true;
        } else {
            found = !currTup->IsDeleted();
        }
        if (m_isIns4Del) {
            /* When undo insert undo record of Ins4Del record generated during ccindex, the index tuple must
             * have the same flag. Note that we do not remove this index tuple but only set the index tuple as
             * not deleted when rollback in this case. */
            found = found && currTup->IsInsertDeletedForCCindex();
        }
        if (found && m_currPage->GetTd(currTup->GetTdId())->GetXid() == m_xid) {
            if (m_doNotRollbackDeleteOnRightPage && m_rollbackAbortedForCr) {
                /* Aborted case, the tupleTdStatus must be ATTACH_TD_AS_NEW_OWNER because no one can reuse the td
                 * before rollback */
                found = currTup->TestTdStatus(ATTACH_TD_AS_NEW_OWNER);
            } else {
                found = !currTup->TestTdStatus(DETACH_TD);
            }
            if (found) {
                m_offset = checkOff;
                return true;
            }
        }
        if (m_doNotRollbackDeleteOnRightPage) {
            /* Aborted deletion for CR case. it would only be on the first place or already rollback. */
            return false;
        }
        /* We're going to check the next offset and have no knowledge if the next tuple is equal to m_undoTuple or not.
         * Set isEqual false to trigger comparing in next loop. */
        isEqual = false;
        found = false;
    }
    return false;
}

RetStatus BtreeUndoContext::TryRollbackByUndoRec(UndoRecord &undoRecord, TD &tdOnPage, bool &pageRollbackSkip)
{
    pageRollbackSkip = false;
    if (STORAGE_FUNC_FAIL(SetUndoInfo(tdOnPage.GetXid(), &undoRecord))) {
        return DSTORE_FAIL;
    }
    bool undoRecMatchPage = DoesUndoRecMatchCurrPage(pageRollbackSkip);
    if (!undoRecMatchPage) {
        StorageAssert(m_offset == INVALID_ITEM_OFFSET_NUMBER);
        if (unlikely(m_undoType == BtreeUndoContextType::PAGE_ROLLBACK && pageRollbackSkip)) {
            ErrLog(DSTORE_LOG, MODULE_INDEX,
                ErrMsg("Skip page rollback for xid(%d, %lu) for original indexTuple(%hu, %u, %hu), "
                "tdId(%hhu), heapCtid(%hu, %u, %hu).",
                static_cast<int32>(m_xid.m_zoneId), m_xid.m_logicSlotId,
                m_currBuf->GetPageId().m_fileId, m_currBuf->GetPageId().m_blockId, undoRecord.GetCtid().GetOffset(),
                undoRecord.GetTdId(), m_undoTuple->m_link.heapCtid.GetFileId(),
                m_undoTuple->m_link.heapCtid.GetBlockNum(), m_undoTuple->m_link.heapCtid.GetOffset()));
        } else {
            tdOnPage.RollbackTdInfo(&undoRecord);
        }
        return DSTORE_SUCC;
    }
    return m_currPage->RollbackByUndoRec(&undoRecord, this, &tdOnPage);
}

IndexTuple *GetIndexTupleFromUndoRec(UndoRecord *undoRec)
{
    AutoMemCxtSwitch autoMemCxtSwitch(thrd->GetUndoMemoryCtx());
    UndoType type = undoRec->GetUndoType();
    void *undoData = undoRec->GetUndoData();
    StorageReleasePanic(undoData == nullptr, MODULE_INDEX, ErrMsg("undoRecord is damaged. %s", undoRec->Dump()));
    if (type == UNDO_BTREE_DELETE || type == UNDO_BTREE_DELETE_TMP) {
        return static_cast<IndexTuple *>(static_cast<UndoDataBtreeDelete*>(undoData)->GetData())->Copy();
    }
    StorageAssert(type == UNDO_BTREE_INSERT || type == UNDO_BTREE_INSERT_TMP);
    return static_cast<UndoDataBtreeInsert*>(undoData)->GetIndexTuple();
}

IndexTuple *UndoDataBtreeInsert::GetIndexTuple()
{
    ItemPointerData heapCtid = ItemPointerData(m_heapCtid);
    uint32 valueSize = GetValueSize();
    char *valueStart = GetValue();
    uint32 headerSize = IndexTuple::GetDataOffset(m_hasNull);
    uint32 tupleSize = headerSize + valueSize;

    IndexTuple *tuple = static_cast<IndexTuple *>(DstorePalloc0(tupleSize));
    if (STORAGE_VAR_NULL(tuple)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("DstorePalloc fail when GetIndexTuple."));
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        return nullptr;
    }
    tuple->SetSize(tupleSize);
    tuple->SetHeapCtid(&heapCtid);
    if (m_hasNull) {
        /* Fill bitmap in tuple */
        tuple->SetHasNull();
        uint16 natts = *GetData();
        uint8 *bitmaps = tuple->GetNullBitmap();
        uint8 *undoBitmaps = GetData() + sizeof(uint8);
        if (bitmaps != nullptr) {
            for (Size i = 0; i < DataTuple::GetBitmapLen(natts); ++i) {
                bitmaps[i] = undoBitmaps[i];
            }
        }
    }

    if (m_hasVariable) {
        tuple->SetHasVariable();
    }

    if (valueSize == 0) {
        /* This means all keys are null */
        return tuple;
    }

    if (m_ins4Del) {
        tuple->SetCcindexStatus(BtrCcidxStatus::WRITE_ONLY_INDEX);
    }
    errno_t rc = memcpy_s(tuple->GetValues(), valueSize, valueStart, valueSize);
    storage_securec_check(rc, "\0", "\0");
    return tuple;
}

} /* namespace DSTORE */
