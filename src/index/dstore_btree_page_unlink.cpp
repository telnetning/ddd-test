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
 * dstore_btree_page_unlink.cpp
 *
 * IDENTIFICATION
 *        storage/src/index/dstore_btree_page_unlink.cpp
 *
 * ---------------------------------------------------------------------------------------*/
#include "index/dstore_btree_delete.h"
#include "index/dstore_btree_wal.h"
#include "transaction/dstore_transaction.h"
#include "wal/dstore_wal_write_context.h"
#include "index/dstore_btree_perf_unit.h"
#include "index/dstore_btree_page_unlink.h"

namespace DSTORE {

BtreePageUnlink::BtreePageUnlink(BufferDesc *buffDesc, StorageRelation indexRel, IndexInfo *indexInfo, ScanKey scanKey)
    : BtreeSplit(indexRel, indexInfo, scanKey, true),
      m_currMaxCsn(INVALID_CSN),
      m_recyclable(true)
{
    StorageAssert(buffDesc != INVALID_BUFFER_DESC);
    m_payloadLeaf.InitByBuffDesc(buffDesc);
    m_pageId = m_payloadLeaf.GetPageId();
}

RetStatus BtreePageUnlink::TryUnlinkPageFromBtree()
{
    LatencyStat::Timer timer(&BtreePerfUnit::GetInstance().m_btreePageUnlinkLatency);
    if (m_payloadLeaf.GetBuffDesc() == INVALID_BUFFER_DESC &&
        STORAGE_FUNC_FAIL(ReadBufDesc(m_pageId, &m_payloadLeaf, LW_EXCLUSIVE))) {
        ErrLog(DSTORE_DEBUG1, MODULE_INDEX, ErrMsg("Failed to unlink page {%d, %u}, failed to read it",
            m_pageId.m_fileId, m_pageId.m_blockId));
        return DSTORE_FAIL;
    }
    StorageAssert(m_payloadLeaf.GetLinkAndStatus()->TestType(BtrPageType::LEAF_PAGE));

    /* Step 1. Check emptiness of page. Only empty pages are recyclable */
    if (!IsPageEmptyAndRecyclable(true)) { /* "needPrune = true" to avoid duplicate scanning if recycle failed. */
        DropAll();
        return DSTORE_FAIL;
    }

    if (m_payloadLeaf.GetLinkAndStatus()->TestLiveStatus(BtrPageLiveStatus::EMPTY_HAS_PARENT_HAS_SIB)) {
        /* Record page info before unlock */
        PageId left = m_payloadLeaf.GetLinkAndStatus()->GetLeft();

        /* Step 2. Check sibling status of page. We cannot recycle a page at the moment that left is splitting or right
         * is unlinked from parent. (The page is still recyclable, but not now) */
        if (!IsPageRecyclableNow(m_payloadLeaf.GetBuffDesc(), false)) {
            ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
                   ErrMsg("Failed to unlink page {%d, %u} from parent, not recyclable now, will try later",
                          m_pageId.m_fileId, m_pageId.m_blockId));
            DropAll();
            return DSTORE_FAIL;
        }
        /* Note: leaf page is unlocked and released while left sibling is locked after recyclable checking */

#ifdef UT
        if (m_leafStack == nullptr) {
#endif
        /* Step 3. Search leaf page in Btree to get the path to the leaf */
        if (STORAGE_FUNC_FAIL(SearchForLeaf(left))) {
            DropAll();
            return DSTORE_FAIL;
        }
#ifdef UT
        } else {
            ReadBufDesc(m_pageId, &m_payloadLeaf, LW_EXCLUSIVE);
        }
#endif

        /* Step 4. Try unlink page from parent */
        if (STORAGE_FUNC_FAIL(DoUnlinkFromParent())) {
            DropAll();
            return DSTORE_FAIL;
        }
    }

    /* Step 5. Try unlink page from siblings */
    if (STORAGE_FUNC_FAIL(DoUnlinkLeafFromSiblings())) {
        DropAll();
        return DSTORE_FAIL;
    }

    ErrLog(DSTORE_DEBUG1, MODULE_INDEX, ErrMsg("Unlink btrPage{%u} succeed.", m_pageId.m_blockId));
    DropAll();
    return DSTORE_SUCC;
}

RetStatus BtreePageUnlink::SearchForLeaf(const PageId left)
{
    /* Generate a searching key using left page's high key */
    IndexTuple *searchingTarget = nullptr;
    if (left == INVALID_PAGE_ID) {
        searchingTarget = IndexTuple::CreateMinusinfPivotTuple();
    } else {
        StorageAssert(m_payloadLeft.GetBuffDesc() != INVALID_BUFFER_DESC);
        searchingTarget = (m_payloadLeft.GetPage()->GetIndexTuple(BTREE_PAGE_HIKEY))->Copy();
        m_payloadLeft.Drop(m_bufMgr);
    }
    if (unlikely(searchingTarget == nullptr)) {
        return DSTORE_FAIL;
    }
    UNUSED_VARIABLE(UpdateScanKeyWithValues(searchingTarget));

    /* Leaf page will be write-locked again after searching */
    BufferDesc *tempTargetBuf = INVALID_BUFFER_DESC;
    if (STORAGE_FUNC_FAIL(BtreeSplit::SearchBtree(&tempTargetBuf, false, true, true))) {
        return DSTORE_FAIL;
    }
    int cmpRet = 0;
    while (tempTargetBuf->GetPageId() != m_pageId) {
        BtreePagePayload tempTarget;
        tempTarget.InitByBuffDesc(tempTargetBuf);
        PageId nextPageId = INVALID_PAGE_ID;
        /* If the left page's max value = target page's min valud, SearchBtree would lead us to the left page.
         * Need to step right to our real target. */
        cmpRet = CompareKeyToTuple(tempTarget.GetPage(), tempTarget.GetLinkAndStatus(), BTREE_PAGE_HIKEY,
                                   tempTarget.GetLinkAndStatus()->TestType(BtrPageType::INTERNAL_PAGE));
        if (unlikely(StorageGetErrorCode() == COMMON_ERROR_FUNCTION_RETURN_NULL)) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to compare tuple since compare function returns null."));
            tempTarget.Drop(m_bufMgr);
            return DSTORE_FAIL;
        }
        if (cmpRet == 0) {
            nextPageId = tempTarget.GetLinkAndStatus()->GetRight();
        }
        if (nextPageId == INVALID_PAGE_ID) {
            /* left sibling has been splitted after we read last time. We are not sure if splitting is finished now. */
            /* Stop recycle page and maybe try later */
#ifdef DSTORE_COLLECT_INDEX_RECYCLE_INFO
            GetBtreeSmgr()->SetRecycleFailReason(BTR_PAGE_LEFT_SIB_CHANGED);
#endif
            ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
                ErrMsg("Failed to unlink page{%d, %u}. left sibling {%d, %u} has been changed",
                m_pageId.m_fileId, m_pageId.m_blockId, left.m_fileId, left.m_blockId));
            tempTarget.Drop(m_bufMgr);
            StorageAssert(m_payloadLeaf.GetBuffDesc() == INVALID_BUFFER_DESC);
            return DSTORE_FAIL;
        }
        tempTargetBuf = ReleaseOldGetNewBuf(tempTargetBuf, nextPageId, LW_EXCLUSIVE, false);
        if (STORAGE_VAR_NULL(tempTargetBuf)) {
            return DSTORE_FAIL;
        }
        if (unlikely(!BtrPage::IsBtrPageValid(tempTargetBuf->GetPage(), GetBtreeSmgr()->GetMetaCreateXid()))) {
            BtrPage *page = static_cast<BtrPage *>(tempTargetBuf->GetPage());
            ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
                ErrMsg("BtrPage(%hu, %u) is not valid" BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
                tempTargetBuf->GetPageId().m_fileId, tempTargetBuf->GetPageId().m_blockId,
                BTR_PAGE_HEADER_VAL(page), BTR_PAGE_LINK_AND_STATUS_VAL(page->GetLinkAndStatus())));
            m_bufMgr->UnlockAndRelease(tempTargetBuf);
            tempTargetBuf = INVALID_BUFFER_DESC;
            m_recyclable = false;
            return DSTORE_FAIL;
        }
    }
    m_payloadLeaf.InitByBuffDesc(tempTargetBuf);
    DstorePfree(searchingTarget);
    return DSTORE_SUCC;
}

RetStatus BtreePageUnlink::ReadBufDesc(const PageId pageId, BtreePagePayload *payload, LWLockMode access)
{
    if (unlikely(pageId == INVALID_PAGE_ID)) {
        storage_set_error(INDEX_ERROR_FAIL_READ_PAGE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s]Cannont reead invalid pageId", __FUNCTION__));
        return DSTORE_FAIL;
    }
    if (payload->GetBuffDesc() != INVALID_BUFFER_DESC) {
        payload->Drop(m_bufMgr);
    }
    if (STORAGE_FUNC_FAIL(payload->Init(this->GetPdbId(), pageId, access, m_bufMgr, false))) {
        payload->Drop(m_bufMgr);
        return DSTORE_FAIL;
    }
    if (unlikely(payload->GetPage()->GetSelfPageId() != pageId) ||
        unlikely(!payload->GetPage()->TestType(PageType::INDEX_PAGE_TYPE)) ||
        unlikely(payload->GetPage()->GetBtrMetaPageId() != GetBtreeSmgr()->GetBtrMetaPageId()) ||
        unlikely(payload->GetPage()->GetBtrMetaCreateXid() != GetBtreeSmgr()->GetMetaCreateXid())) {
        payload->Drop(m_bufMgr);
        m_recyclable = false;
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

void BtreePageUnlink::DropAll()
{
    m_payloadTarget.Drop(m_bufMgr);
    m_payloadTop.Drop(m_bufMgr);
    m_payloadLeft.Drop(m_bufMgr);
    m_payloadRight.Drop(m_bufMgr);
    m_payloadLeaf.Drop(m_bufMgr);
    ClearStack();
}

void BtreePageUnlink::GenerateWalForUnlinkFromParent(
    OffsetNumber pivotOff, const PageId subtreeRoot, const PageId newTopDownlink)
{
    StorageAssert(m_payloadLeaf.GetLinkAndStatus()->TestLiveStatus(BtrPageLiveStatus::EMPTY_NO_PARENT_HAS_SIB));
    AtomicWalWriterContext *walContext = thrd->m_walWriterContext;
    walContext->BeginAtomicWal(thrd->GetActiveTransaction()->GetCurrentXid());

    /* Generate Redo record for parent page of subtree root */
    walContext->RememberPageNeedWal(m_payloadTop.GetBuffDesc());
    bool glsnChangedFlag = (m_payloadTop.GetPage()->GetWalId() != walContext->GetWalId());
    WalRecordBtreeUpdateDownlink redoTop;
    redoTop.SetHeader(m_payloadTop.GetPage(), glsnChangedFlag, pivotOff, newTopDownlink,
                      m_payloadTop.GetBuffDesc()->GetFileVersion());
    walContext->PutNewWalRecord(&redoTop);

    /* Generate Redo record for recycled leaf page */
    walContext->RememberPageNeedWal(m_payloadLeaf.GetBuffDesc());
    glsnChangedFlag = (m_payloadLeaf.GetPage()->GetWalId() != walContext->GetWalId());
    WalRecordBtreeUpdateDownlink redoLeaf;
    redoLeaf.SetHeader(m_payloadLeaf.GetPage(), glsnChangedFlag, BTREE_PAGE_HIKEY, subtreeRoot,
                       m_payloadLeaf.GetBuffDesc()->GetFileVersion());
    walContext->PutNewWalRecord(&redoLeaf);

    UNUSED_VARIABLE(walContext->EndAtomicWal());
}

void BtreePageUnlink::GenerateWalForUnlinkFromSiblings(
    bool isLeaf, const PageId nextSubtreeRoot, BufferDesc *btreeMetaBuf)
{
    StorageAssert(m_payloadTarget.GetLinkAndStatus()->TestLiveStatus(BtrPageLiveStatus::EMPTY_NO_PARENT_NO_SIB));

    AtomicWalWriterContext *walContext = thrd->m_walWriterContext;
    walContext->BeginAtomicWal(thrd->GetActiveTransaction()->GetCurrentXid());
    bool glsnChangedFlag = false;
    if (btreeMetaBuf != INVALID_BUFFER_DESC) {
        /* Write redo record for a changed the lowest single page on Btree Meta page */
        BtrPage *btreeMeta = static_cast<BtrPage *>(btreeMetaBuf->GetPage());
        WalRecordBtreeUpdateMetaPage redoLowestSingle;
        glsnChangedFlag = (btreeMeta->GetWalId() != walContext->GetWalId());
        redoLowestSingle.SetHeader(btreeMeta, glsnChangedFlag, false, btreeMetaBuf->GetFileVersion());
        walContext->RememberPageNeedWal(btreeMetaBuf);
        walContext->PutNewWalRecord(&redoLowestSingle);
    }
    /* Write redo record for left sibling page of removed pivot if exists */
    if (!m_payloadTarget.GetLinkAndStatus()->IsLeftmost()) {
        PageId sib = m_payloadLeft.GetLinkAndStatus()->GetRight();
        WalRecordBtreeUpdateSibLink redoLeft;
        glsnChangedFlag = (m_payloadLeft.GetPage()->GetWalId() != walContext->GetWalId());
        redoLeft.SetHeader(m_payloadLeft.GetPage(), glsnChangedFlag, sib, false,
                           m_payloadLeft.GetBuffDesc()->GetFileVersion());
        walContext->RememberPageNeedWal(m_payloadLeft.GetBuffDesc());
        walContext->PutNewWalRecord(&redoLeft);
    }

    /* Write redo record for right sibling page of removed pivot */
    PageId sib = m_payloadRight.GetLinkAndStatus()->GetLeft();
    WalRecordBtreeUpdateSibLink redoRight;
    glsnChangedFlag = (m_payloadRight.GetPage()->GetWalId() != walContext->GetWalId());
    redoRight.SetHeader(m_payloadRight.GetPage(), glsnChangedFlag, sib, true,
                        m_payloadRight.GetBuffDesc()->GetFileVersion());
    walContext->RememberPageNeedWal(m_payloadRight.GetBuffDesc());
    walContext->PutNewWalRecord(&redoRight);

    if (!isLeaf) {
        /* Write redo record for next subtree root updating on leaf page is target is not the leaf */
        StorageAssert(m_pageId == m_payloadLeaf.GetPageId());
        StorageAssert(m_payloadLeaf.GetLinkAndStatus()->TestLiveStatus(BtrPageLiveStatus::EMPTY_NO_PARENT_HAS_SIB));
        WalRecordBtreeUpdateDownlink redoSubtreeRoot;
        glsnChangedFlag = (m_payloadLeaf.GetPage()->GetWalId() != walContext->GetWalId());
        redoSubtreeRoot.SetHeader(m_payloadLeaf.GetPage(), glsnChangedFlag, BTREE_PAGE_HIKEY, nextSubtreeRoot,
                                  m_payloadLeaf.GetBuffDesc()->GetFileVersion());
        walContext->RememberPageNeedWal(m_payloadLeaf.GetBuffDesc());
        walContext->PutNewWalRecord(&redoSubtreeRoot);
    }

    UNUSED_VARIABLE(walContext->EndAtomicWal());
}

void BtreePageUnlink::GenerateWalForLiveStatusUpdate(BufferDesc *pageBuf, BtrPageLiveStatus liveStatus)
{
    if (!NeedWal()) {
        return;
    }

    BtrPage *page = static_cast<BtrPage *>(pageBuf->GetPage());
    AtomicWalWriterContext *walContext = thrd->m_walWriterContext;
    walContext->BeginAtomicWal(thrd->GetActiveTransaction()->GetCurrentXid());
    walContext->RememberPageNeedWal(pageBuf);
    WalRecordBtreeUpdateLiveStatus walRecord;
    walRecord.SetHeader(page, (page->GetWalId() != walContext->GetWalId()), liveStatus, pageBuf->GetFileVersion());
    walContext->PutNewWalRecord(&walRecord);
    UNUSED_VARIABLE(walContext->EndAtomicWal());
}

bool BtreePageUnlink::IsPageEmptyAndRecyclable(bool needPrune)
{
    if (m_payloadLeaf.GetLinkAndStatus()->TestLiveStatus(BtrPageLiveStatus::EMPTY_NO_PARENT_NO_SIB)) {
        m_recyclable = false;
        ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
               ErrMsg("page {%d, %u} is alread recycled by others.", m_pageId.m_fileId, m_pageId.m_blockId));
        return false;
    }

    if (m_payloadLeaf.GetLinkAndStatus()->TestLiveStatus(BtrPageLiveStatus::NORMAL_USING)) {
        m_recyclable = false;
        ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
               ErrMsg("page {%d, %u} is reused by others.", m_pageId.m_fileId, m_pageId.m_blockId));
        return false;
    }

    /* Step 0. Never recycle Root, Rightmost and splitting page */
    BtrPageLinkAndStatus *linkStat = m_payloadLeaf.GetLinkAndStatus();
    if (linkStat->IsRoot() || linkStat->IsRightmost() || !linkStat->IsSplitComplete()) {
        m_recyclable = false;
        ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
               ErrMsg("%s page {%d, %u} is unrecyclable",
                      linkStat->IsRoot() ? "Root" : (linkStat->IsRightmost() ? "Rightmost" : "Incomplete splitting"),
                      m_pageId.m_fileId, m_pageId.m_blockId));
        return false;
    }

    /* Step 1. Check is there any non-deleted tuple exists */
    uint16 numNonDel = m_payloadLeaf.GetPage()->GetNonDeletedTupleNum();
    if (numNonDel > 0) {
        StorageAssert(!m_payloadLeaf.GetLinkAndStatus()->IsUnlinked());
        if (m_payloadLeaf.GetLinkAndStatus()->TestLiveStatus(BtrPageLiveStatus::EMPTY_HAS_PARENT_HAS_SIB)) {
            m_payloadLeaf.GetLinkAndStatus()->SetLiveStatus(BtrPageLiveStatus::NORMAL_USING);
            UNUSED_VARIABLE(m_bufMgr->MarkDirty(m_payloadLeaf.GetBuffDesc()));
            GenerateWalForLiveStatusUpdate(m_payloadLeaf.GetBuffDesc(), BtrPageLiveStatus::NORMAL_USING);
        }
#ifdef DSTORE_COLLECT_INDEX_RECYCLE_INFO
        GetBtreeSmgr()->SetRecycleFailReason(BTR_PAGE_NOT_EMPTY);
#endif
        ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
               ErrMsg("Failed to unlink page {%d, %u} from parent, %d non-deleted"
                      " tuples, not empty",
                      m_pageId.m_fileId, m_pageId.m_blockId, numNonDel));
        /* Live tuples found. The page is not recyclable */
        m_recyclable = false;
        return false;
    }

    /* Step 2. Check is the deleted-status visible to all */
    BtreePagePrune *prunePage = DstoreNew(g_dstoreCurrentMemoryContext)
            BtreePagePrune(m_indexRel, m_indexInfo, m_scanKeyValues.scankeys, m_payloadLeaf.GetBuffDesc());
    if (STORAGE_VAR_NULL(prunePage)) {
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Failed to create prunePage object.", __FUNCTION__));
        return false;
    }
    bool isEmpty = prunePage->IsPageEmpty(needPrune);
    if (!isEmpty) {
        /* All tuples on the page are deleted while some of the deleted-status is not visible to all. The page
         * is recyclable, but not now. Will try again later. */
#ifdef DSTORE_COLLECT_INDEX_RECYCLE_INFO
        GetBtreeSmgr()->SetRecycleFailReason(BTR_PAGE_DEL_NOT_VIS_TO_ALL);
#endif
        ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
               ErrMsg("Failed to unlink page {%hu, %u} from parent, "
                      "deleted-status is not visible to all. non-del %hu total %hu",
                      m_pageId.m_fileId, m_pageId.m_blockId, numNonDel, m_payloadLeaf.GetPage()->GetMaxOffset()));
        CommitSeqNo recycleMinCsn = g_storageInstance->GetCsnMgr()->GetRecycleCsnMin(this->GetPdbId());
        CommitSeqNo maxCsn = prunePage->GetCurrMaxCsn();
        if (maxCsn < recycleMinCsn) {
            maxCsn = recycleMinCsn;
        }
        StorageAssert(maxCsn >= recycleMinCsn);
        SetCurrMaxCsn(maxCsn);
    }

    delete prunePage;
    return isEmpty;
}

bool BtreePageUnlink::IsPageRecyclableNow(BufferDesc *pageBuf, bool unlockLeft)
{
    StorageAssert(pageBuf != INVALID_BUFFER_DESC);

    BtrPage *page = static_cast<BtrPage *>(pageBuf->GetPage());
    BtrPageLinkAndStatus *pageMeta = page->GetLinkAndStatus();
    PageId curr = pageBuf->GetPageId();
    PageId left = pageMeta->GetLeft();
    PageId right = pageMeta->GetRight();

    /*
     * We can never delete root pages nor rightmost page of level.
     * We can never delete an incompletely split page to keep the algorithm simple.
     * The INCOMPLETE_SPLIT flag on the page tells us if the page is the left half of an incomplete split.
     */
    if (pageMeta->IsRoot() || pageMeta->IsRightmost() || !pageMeta->IsSplitComplete()) {
        m_bufMgr->UnlockAndRelease(pageBuf);
        return false;
    }

    /* Unlock pageBuf before checking sibling pages to avoid deadlock */
    m_bufMgr->UnlockAndRelease(pageBuf);
    if (curr == m_pageId) {
        m_payloadLeaf.buffDesc = INVALID_BUFFER_DESC;
    }

    /*
     * Check if the right sibling is already unlinked from its parent.
     * A right sibling which has been unlinked from parent would have no downlink from parent, which would be highly
     * confusing later when we delete the downlink. It would fail the "right sibling of target page is also the next
     * child in parent page" cross-check.
     */
    if (STORAGE_FUNC_FAIL(ReadBufDesc(right, &m_payloadRight, LW_SHARED))) {
        return false;
    }
    if (m_payloadRight.GetLinkAndStatus()->GetLeft() != curr ||
        m_payloadRight.GetLinkAndStatus()->TestLiveStatus(BtrPageLiveStatus::EMPTY_NO_PARENT_HAS_SIB)) {
        m_payloadRight.Drop(m_bufMgr);
#ifdef DSTORE_COLLECT_INDEX_RECYCLE_INFO
        GetBtreeSmgr()->SetRecycleFailReason(BTR_PAGE_RIGHT_SIB_UNLINKED);
#endif
        return false;
    }
    m_payloadRight.Drop(m_bufMgr);

    /*
     * Check if the page is the right half of an incomplete split by verifying the INCOMPLETE_SPLIT flag on its left
     * sibling page
     */
    if (left != INVALID_PAGE_ID) {
        if (STORAGE_FUNC_FAIL(ReadBufDesc(left, &m_payloadLeft, LW_SHARED))) {
            return false;
        }
        if (m_payloadLeft.GetLinkAndStatus()->GetRight() != curr ||
            !m_payloadLeft.GetLinkAndStatus()->IsSplitComplete()) {
            m_payloadLeft.Drop(m_bufMgr);
#ifdef DSTORE_COLLECT_INDEX_RECYCLE_INFO
            GetBtreeSmgr()->SetRecycleFailReason(BTR_PAGE_LEFT_SIB_SPLITTING);
#endif
            return false;
        }
        if (unlockLeft) {
            m_payloadLeft.Drop(m_bufMgr);
        }
    }
    return true;
}

RetStatus BtreePageUnlink::GetRecycleSubtree(BtrPageRecycleStat &stat, const PageId childPageId, BtrStack stack,
                                             PageId *topPageId, OffsetNumber *pivotOff, PageId *subtreeRootPageId)
{
    if (stack == nullptr) {
        stat = BtrPageRecycleStat::RECYCLE_FAILED;
        return DSTORE_SUCC;
    }

    /* Get parent page. */
    BufferDesc *parentBuf = GetParentBufDesc(childPageId, stack, LW_SHARED);
    if (unlikely(parentBuf == INVALID_BUFFER_DESC)) {
        if (STORAGE_FUNC_FAIL(CheckNonParentPage(childPageId))) {
            return DSTORE_FAIL;
        }
        stat = BtrPageRecycleStat::RECYCLE_FAILED;
        return DSTORE_SUCC;
    }
    BtrPage *parPage = static_cast<BtrPage *>(parentBuf->GetPage());
    PageId parPageId = parentBuf->GetPageId();

    /* Check if child is the rightmost child of its parent */
    OffsetNumber childOff = stack->currItem.GetOffset();
    if (childOff < parPage->GetMaxOffset()) {
        /* Child is not the rightmost. Found subtree root and its parent */
        *topPageId = stack->currItem.GetPageId();
        *pivotOff = childOff;
        /* return current subtree root PageId here for later validation */
        *subtreeRootPageId = childPageId;
        m_bufMgr->UnlockAndRelease(parentBuf);
        stat = BtrPageRecycleStat::RECYCLABLE;
        return DSTORE_SUCC;
    }

    /*
     * Child is the rightmost child of parent.
     * Deleting the rightmost child (or deleting the subtree whose root/topparent is the child page) is only safe when
     * it's also possible to delete the parent.
     */
    BtrPageRecycleStat ret = CanRightmostUnlink(childOff, parentBuf, *topPageId);
    if (ret != BtrPageRecycleStat::RECYCLABLE) {
        /* parentBuf has been unlocked and released after calling CanRightmostUnlink iff ret != RECYCLABLE */
        stat = ret;
        return DSTORE_SUCC;
    }

    /* We'll release parentBuf. Record the page id of current subtree top page if one-step walking back is needed. */
    *topPageId = stack->currItem.GetPageId();
    *pivotOff = childOff;
    /* return current subtree root PageId here for later validation */
    *subtreeRootPageId = childPageId;

    /* Child is parent's only child, check if parent is recyclable. */
    if (!IsPageRecyclableNow(parentBuf)) {
        /* The lock on parentBuf would be unlocked and release after calling IsPageRecyclableNow */
        /* Parent page is not recyclable, neither its rightmost child. */
        stat = BtrPageRecycleStat::RECYCLE_FAILED;
        return DSTORE_SUCC;
    }

    return GetRecycleSubtree(stat, parPageId, stack->parentStack, topPageId, pivotOff, subtreeRootPageId);
}

RetStatus BtreePageUnlink::CheckNonParentPage(const PageId pageId)
{
    /* Non-parent page may be removed by others. Need to check */
    BtreePagePayload pagePayload;
    if (STORAGE_FUNC_FAIL(pagePayload.Init(this->GetPdbId(), pageId, LW_EXCLUSIVE, m_bufMgr))) {
        return DSTORE_FAIL;
    }
    if (pagePayload.GetLinkAndStatus()->TestLiveStatus(BtrPageLiveStatus::NORMAL_USING)) {
        /* Insertion might happened after page had entered pending free queue. It's a normal using page now. */
        m_recyclable = false;
    } else if (pagePayload.GetPage()->GetNonDeletedTupleNum() > 0) {
        /* Insertion might happend after page had entered pending free queue. Update live status to normal */
        pagePayload.GetLinkAndStatus()->SetLiveStatus(BtrPageLiveStatus::NORMAL_USING);
        UNUSED_VARIABLE(m_bufMgr->MarkDirty(pagePayload.GetBuffDesc()));
        GenerateWalForLiveStatusUpdate(pagePayload.GetBuffDesc(), BtrPageLiveStatus::NORMAL_USING);
        m_recyclable = false;
    } else {
        storage_set_error(INDEX_ERROR_FAIL_REFIND_PARENT_KEY, m_indexInfo->indexRelName, pageId.m_fileId,
                          pageId.m_blockId);
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
            ErrMsg("Failed to unlink page {%d, %u} from parent for failed to get parent in(%s), segment(%hu, %u) "
            BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT, pageId.m_fileId, pageId.m_blockId,
            m_indexInfo->indexRelName, GetBtreeSmgr()->GetSegMetaPageId().m_fileId,
            GetBtreeSmgr()->GetSegMetaPageId().m_blockId, BTR_PAGE_HEADER_VAL(pagePayload.GetPage()),
            BTR_PAGE_LINK_AND_STATUS_VAL(pagePayload.GetLinkAndStatus())));
        m_recyclable = false;
    }
    pagePayload.Drop(m_bufMgr);
#ifdef DSTORE_COLLECT_INDEX_RECYCLE_INFO
    GetBtreeSmgr()->SetRecycleFailReason(BTR_GET_PARENT_FAIL);
#endif
    return DSTORE_SUCC;
}

/* Note: will keep the lock on parentBuf iff the result if BtrPageRecycleStat::RECYCLABLE */
BtrPageRecycleStat BtreePageUnlink::CanRightmostUnlink(OffsetNumber currOff, BufferDesc *parentBuf,
                                                       const PageId subtreeTop)
{
    auto *parentPage = static_cast<BtrPage *>(parentBuf->GetPage());
    StorageAssert(currOff == parentPage->GetMaxOffset());

    if (currOff == parentPage->GetLinkAndStatus()->GetFirstDataOffset() &&
        !parentPage->GetLinkAndStatus()->IsRightmost()) {
        /* Child is the only child of parent and parent is not the rightmost in level, can be unlinked */
        return BtrPageRecycleStat::RECYCLABLE;
    }

    m_bufMgr->UnlockAndRelease(parentBuf);
    /* Child isn't parent's only child, or parent is rightmost on its entire level. Cannot remove child page. */
    if (subtreeTop.IsValid()) {
        /* We've already found a subtree root, just stop here */
        return BtrPageRecycleStat::UNRECYCLABLE;
    }

    /* Cannot recycle child page for now, neither for later tries. Need to take it out of FSM */
    m_payloadLeaf.GetLinkAndStatus()->SetLiveStatus(BtrPageLiveStatus::NORMAL_USING);
    UNUSED_VARIABLE(m_bufMgr->MarkDirty(m_payloadLeaf.GetBuffDesc()));
    GenerateWalForLiveStatusUpdate(m_payloadLeaf.GetBuffDesc(), BtrPageLiveStatus::NORMAL_USING);
    m_payloadLeaf.Drop(m_bufMgr);
    m_recyclable = false;

#ifdef DSTORE_COLLECT_INDEX_RECYCLE_INFO
    GetBtreeSmgr()->SetRecycleFailReason(BTR_PAGE_RIGHTMOST_CHILD_OF_PARENT);
#endif

    return BtrPageRecycleStat::RECYCLE_FAILED;
}

RetStatus BtreePageUnlink::DoUnlinkFromParent()
{
    /* Note: need to keep leaf locked */
    /* Step 1. Check emptiness of leaf page again because the page was unlocked during searching process. */
    StorageAssert(m_payloadLeaf.GetBuffDesc() != INVALID_BUFFER_DESC);
    StorageAssert(m_payloadLeaf.GetLinkAndStatus()->TestLiveStatus(BtrPageLiveStatus::EMPTY_HAS_PARENT_HAS_SIB));
    if (!IsPageEmptyAndRecyclable()) {
        m_payloadLeaf.Drop(m_bufMgr);
        return DSTORE_FAIL;
    }

    /* Step 2. Get the root page of to-be-deleted subtree and its parent */
    PageId topPageId = INVALID_PAGE_ID;
    PageId subtreePageId = INVALID_PAGE_ID;
    OffsetNumber pivotOff = INVALID_ITEM_OFFSET_NUMBER;
    /* Walk back along stack to find the root of to-be-deleted subtree and parent of the root. */
    BtrPageRecycleStat stat;
    if (STORAGE_FUNC_FAIL(GetRecycleSubtree(stat, m_pageId, m_leafStack, &topPageId, &pivotOff, &subtreePageId))) {
        return DSTORE_FAIL;
    }
    if (stat == BtrPageRecycleStat::RECYCLE_FAILED) {
        ErrLog(DSTORE_DEBUG1, MODULE_INDEX, ErrMsg("Failed to remove page {%d, %u}. Fail to get parent, "
            "maybe removed by others.", m_pageId.m_fileId, m_pageId.m_blockId));
        return DSTORE_FAIL;
    }

    /* The highest level we've visited is not recyclable. Let's step down to the lower level. */
    if (STORAGE_FUNC_FAIL(ReadBufDesc(topPageId, &m_payloadTop, LW_EXCLUSIVE))) {
        return DSTORE_FAIL;
    }

    /* Step 3. Get the pivot tuple on top parent page that points to to-be-deleted subtree then check correctness */
    if (STORAGE_FUNC_FAIL(CheckRecyclableSubtreeTop(topPageId, subtreePageId, pivotOff))) {
        return DSTORE_FAIL;
    }
    PageId topPivotRightPageId =
        m_payloadTop.GetPage()->GetIndexTuple(OffsetNumberNext(pivotOff))->GetLowlevelIndexpageLink();

    ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
        ErrMsg("Got to-be-deleted subtree root {%d, %u} of leaf {%d, %u}, locating on top parent ({%d, %u}, %d)",
        subtreePageId.m_fileId, subtreePageId.m_blockId, m_pageId.m_fileId, m_pageId.m_blockId,
        topPageId.m_fileId, topPageId.m_blockId, pivotOff));

    /* Step 4. Update parent of subtree
     *
     * We want to delete the downlink to to-be-deleted subtree, and the *following* key. Easiest way is to copy the
     * right sibling's downlink over the downlink that points to subtree, and then delete the right sibling's original
     * pivot tuple.
     */
    BtreeDelete deleteTopPivot(m_indexRel, m_indexInfo, m_scanKeyValues.scankeys);
    if (STORAGE_FUNC_FAIL(deleteTopPivot.DeleteFromInternal(m_payloadTop.GetBuffDesc(), OffsetNumberNext(pivotOff)))) {
#ifdef DSTORE_COLLECT_INDEX_RECYCLE_INFO
        GetBtreeSmgr()->SetRecycleFailReason(BTR_DEL_PIVOT_FAIL);
#endif
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to unlink page {%d, %u} from parent. "
            "Failed to delete subtree root {%d, %u} locating on top parent ({%d, %u}, %d)",
            m_pageId.m_fileId, m_pageId.m_blockId, subtreePageId.m_fileId, subtreePageId.m_blockId,
            topPageId.m_fileId, topPageId.m_blockId, pivotOff));
        return DSTORE_FAIL;
    }
    IndexTuple *topPivot = m_payloadTop.GetPage()->GetIndexTuple(pivotOff);
    topPivot->SetLowlevelIndexpageLink(topPivotRightPageId);
    UNUSED_VARIABLE(m_bufMgr->MarkDirty(m_payloadTop.GetBuffDesc()));

    /* Step 5. Set leaf page live status to EMPTY_NO_PARENT_HAS_SIB. */
    m_payloadLeaf.GetLinkAndStatus()->SetLiveStatus(BtrPageLiveStatus::EMPTY_NO_PARENT_HAS_SIB);
    /*
     * Set a downlink pointing to root of the subtree to high key of leaf page. Once the downlink to subtree's root
     * from its parent is deleted, we will never find any page inside the subtree by a regular search. Thus, recording
     * the root page of subtree is necessary.
     */
    SetSubtreeRoot(subtreePageId);
    UNUSED_VARIABLE(m_bufMgr->MarkDirty(m_payloadLeaf.GetBuffDesc()));

    /* Step 6. Write WAL if needed. */
    if (NeedWal()) {
        GenerateWalForUnlinkFromParent(pivotOff, subtreePageId, topPivotRightPageId);
    }
    /* Keep a write-lock on leaf page for next step (unlink from siblings) */
    m_payloadTop.Drop(m_bufMgr);
    return DSTORE_SUCC;
}

RetStatus BtreePageUnlink::CheckRecyclableSubtreeTop(const PageId topPageId, const PageId subtreePageId,
                                                     OffsetNumber pivotOff)
{
    StorageAssert(m_payloadTop.GetLinkAndStatus()->GetLevel() > 0);
    if (unlikely(m_payloadTop.GetPage()->GetMaxOffset() == pivotOff)) {
        ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
            ErrMsg("Failed to unlink page {%d, %u} from parent {%d, %u} "
                   "because child is the right most child of %s",
            subtreePageId.m_fileId, subtreePageId.m_blockId, topPageId.m_fileId, topPageId.m_blockId,
            m_payloadTop.GetLinkAndStatus()->IsRightmost() ? "rightmost" : "root"));
        return DSTORE_FAIL;
    }

    IndexTuple *topPivotRight = m_payloadTop.GetPage()->GetIndexTuple(OffsetNumberNext(pivotOff));
    PageId topPivotRightPageId = topPivotRight->GetLowlevelIndexpageLink();

    PageId subtreeRootRightPageId;
    if (subtreePageId == m_pageId) {
        subtreeRootRightPageId = m_payloadLeaf.GetLinkAndStatus()->GetRight();
    } else {
        BtreePagePayload subtreePayload;
        if (STORAGE_FUNC_FAIL(subtreePayload.Init(this->GetPdbId(), subtreePageId, LW_SHARED, m_bufMgr))) {
            return DSTORE_FAIL;
        }
        subtreeRootRightPageId = subtreePayload.GetLinkAndStatus()->GetRight();
        subtreePayload.Drop(m_bufMgr);
    }

    if (subtreeRootRightPageId != topPivotRightPageId) {
#ifdef DSTORE_COLLECT_INDEX_RECYCLE_INFO
        GetBtreeSmgr()->SetRecycleFailReason(BTR_PAGE_PIVOT_CHANGED);
#endif
        /* Pivot page has been changed. Stop recycling. */
        ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
               ErrMsg("Failed to unlink page {%d, %u} from parent "
                      "because right pivot on parent ({%d, %u}, %d) points to {%d, %u} "
                      "while right sibling of subtree root is {%d, %u} when unlinking subtree root {%d, %u}",
                      m_pageId.m_fileId, m_pageId.m_blockId, topPageId.m_fileId, topPageId.m_blockId,
                      OffsetNumberNext(pivotOff), topPivotRightPageId.m_fileId, topPivotRightPageId.m_blockId,
                      subtreeRootRightPageId.m_fileId, subtreeRootRightPageId.m_blockId, subtreePageId.m_fileId,
                      subtreePageId.m_blockId));
        return DSTORE_FAIL;
    }

    return DSTORE_SUCC;
}

BtrPageRecycleStat BtreePageUnlink::GetTargetAndSiblings(const PageId pageId)
{
    bool isLeaf = (pageId == m_pageId);
    if (isLeaf) {
        StorageAssert(pageId == m_payloadLeaf.GetPageId());
        m_payloadTarget.InitByBuffDesc(m_payloadLeaf.GetBuffDesc());
    } else {
        if (STORAGE_FUNC_FAIL(ReadBufDesc(pageId, &m_payloadTarget, LW_SHARED))) {
            return BtrPageRecycleStat::RECYCLE_FAILED;
        }
        StorageAssert(m_payloadTarget.GetLinkAndStatus()->GetLevel() > 0);
    }

    /* Record information of target page */
    PageId leftPageId = m_payloadTarget.GetLinkAndStatus()->GetLeft();
    PageId rightPageId = m_payloadTarget.GetLinkAndStatus()->GetRight();

    /* Release target page to avoid deadlock before locking its left sibling page */
    m_payloadTarget.Drop(m_bufMgr);
    if (isLeaf) {
        m_payloadLeaf.buffDesc = INVALID_BUFFER_DESC;
    }

    /* Get real left of target */
    if (leftPageId != INVALID_PAGE_ID) {
        if (GetLeft(pageId, leftPageId, rightPageId) == DSTORE_FAIL) {
            /* The original left page may be deleted. We cannot unlink the target page now. */
            ErrLog(DSTORE_DEBUG1, MODULE_INDEX, ErrMsg("Failed to remove page{%d, %u}. Failed to get left "
                "sibling {%d, %u}", pageId.m_fileId, pageId.m_blockId, leftPageId.m_fileId, leftPageId.m_blockId));
            return BtrPageRecycleStat::UNRECYCLABLE;
        }
        if (!m_payloadLeft.GetLinkAndStatus()->IsSplitComplete()) {
            /* Need to recycle after left page split complete */
            ErrLog(DSTORE_DEBUG1, MODULE_INDEX, ErrMsg("Failed to remove page{%d, %u}. left sibling {%d, %u} "
                "split is imcomplete", pageId.m_fileId, pageId.m_blockId, leftPageId.m_fileId, leftPageId.m_blockId));
            return BtrPageRecycleStat::UNRECYCLABLE;
        }
    }

    /* Lock target page again. We are trying to do unlink stuff now if still allowed. */
    if (STORAGE_FUNC_FAIL(ReadBufDesc(pageId, &m_payloadTarget, LW_EXCLUSIVE))) {
        return BtrPageRecycleStat::RECYCLE_FAILED;
    }
    if (isLeaf) {
        m_payloadLeaf.InitByBuffDesc(m_payloadTarget.GetBuffDesc());
    }

    if (CheckUnlinkTargetRecyclable() == BtrPageRecycleStat::RECYCLE_FAILED) {
        return BtrPageRecycleStat::RECYCLE_FAILED;
    }

    /*
     * We have checked emptiness for leaf page. For internal page, since a internal page will only get split/deleted as
     * subsequence of leaf split/deleted, insertion or deletion will never happen on a internal page while the leaf is
     * still write-locked. So, we don't need to check tuples on internal page here.
     */
    if (m_payloadTarget.GetLinkAndStatus()->GetLeft() != leftPageId ||
        (leftPageId.IsValid() && m_payloadLeft.GetLinkAndStatus()->GetRight() != pageId)) {
        return BtrPageRecycleStat::UNRECYCLABLE;
    }

    /* Then lock the right sibling */
    if (STORAGE_FUNC_FAIL(ReadBufDesc(rightPageId, &m_payloadRight, LW_EXCLUSIVE))) {
        return BtrPageRecycleStat::RECYCLE_FAILED;
    }
    if (m_payloadTarget.GetLinkAndStatus()->GetRight() != rightPageId ||
        m_payloadRight.GetLinkAndStatus()->GetLeft() != pageId ||
        m_payloadRight.GetLinkAndStatus()->TestLiveStatus(BtrPageLiveStatus::EMPTY_NO_PARENT_HAS_SIB)) {
        return BtrPageRecycleStat::UNRECYCLABLE;
    }
    return BtrPageRecycleStat::RECYCLABLE;
}

BtrPageRecycleStat BtreePageUnlink::CheckUnlinkTargetRecyclable()
{
    if (m_payloadTarget.GetLinkAndStatus()->TestLiveStatus(BtrPageLiveStatus::EMPTY_NO_PARENT_NO_SIB)) {
#ifdef DSTORE_COLLECT_INDEX_RECYCLE_INFO
        GetBtreeSmgr()->SetRecycleFailReason(BTR_PAGE_RECYCLED_BY_OTHERS);
#endif
        /* Page may be reused by other thread. Stop recycling */
        ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
               ErrMsg("Failed to remove page{%d, %u}. Already removed by others", m_payloadTarget.GetPageId().m_fileId,
                      m_payloadTarget.GetPageId().m_blockId));
        return BtrPageRecycleStat::RECYCLE_FAILED;
    }

    if (m_payloadTarget.GetLinkAndStatus()->IsRightmost() || m_payloadTarget.GetLinkAndStatus()->IsRoot() ||
        !m_payloadTarget.GetLinkAndStatus()->IsSplitComplete() ||
        !m_payloadLeaf.GetLinkAndStatus()->TestLiveStatus(BtrPageLiveStatus::EMPTY_NO_PARENT_HAS_SIB) ||
        !IsPageEmptyAndRecyclable()) {
        /* Leaf page content has been changed. Stop recycling the leaf page. */
        storage_set_error(INDEX_ERROR_CHANGED_NO_PARENT_LEAF, m_payloadLeaf.GetPageId().m_fileId,
                          m_payloadLeaf.GetPageId().m_blockId);
        ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
            ErrMsg("leaf page {%d, %u} has been changed after unlinked from parent in(%s), segment(%hu, %u) "
            BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
            m_payloadLeaf.GetPageId().m_fileId, m_payloadLeaf.GetPageId().m_blockId, m_indexInfo->indexRelName,
            GetBtreeSmgr()->GetSegMetaPageId().m_fileId, GetBtreeSmgr()->GetSegMetaPageId().m_blockId,
            BTR_PAGE_HEADER_VAL(m_payloadLeaf.GetPage()),
            BTR_PAGE_LINK_AND_STATUS_VAL(m_payloadLeaf.GetLinkAndStatus())));
        return BtrPageRecycleStat::RECYCLE_FAILED;
    }
    return BtrPageRecycleStat::RECYCLABLE;
}

void BtreePageUnlink::UpdateTargetSiblingLinks()
{
    /* Since we'll never recycle rightmost page, target must have a right sibling */
    PageId right = m_payloadRight.GetPageId();
    /* Bug we could recycle leftmost page, thus left sibling might be invalid. We'll check it later */
    PageId left = INVALID_PAGE_ID;

    if (!m_payloadTarget.GetLinkAndStatus()->IsLeftmost()) {
        StorageAssert(m_payloadLeft.GetLinkAndStatus()->GetRight() == m_payloadTarget.GetPageId());
        /* Target has a left sibling, update page id of left */
        left = m_payloadLeft.GetPageId();
        /* Update page links of left sibling */
        m_payloadLeft.GetLinkAndStatus()->SetRight(right);
        UNUSED_VARIABLE(m_bufMgr->MarkDirty(m_payloadLeft.GetBuffDesc()));
    }

    /* Deal with right sibling */
    StorageAssert(m_payloadRight.GetLinkAndStatus()->GetLeft() == m_payloadTarget.GetPageId());
    m_payloadRight.GetLinkAndStatus()->SetLeft(left);
    UNUSED_VARIABLE(m_bufMgr->MarkDirty(m_payloadRight.GetBuffDesc()));
}

BufferDesc *BtreePageUnlink::UpdateLowestSinglePageIfNeeded(const PageId lowestSinglePageId)
{
    BufferDesc *btreeMetaBuf = INVALID_BUFFER_DESC;
    if (m_payloadTarget.GetLinkAndStatus()->IsLeftmost() && m_payloadRight.GetLinkAndStatus()->IsRightmost()) {
        BtrMeta *btreeMeta = GetBtreeSmgr()->GetBtrMeta(LW_EXCLUSIVE, &btreeMetaBuf);
        if (btreeMeta == nullptr) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Can not get btr meta when UpdateLowestSinglePageIfNeeded"));
            return btreeMetaBuf;
        }
        PageId oldLowestSingle = btreeMeta->GetLowestSinglePage();
        uint32 oldLowestSingleLevel = btreeMeta->GetLowestSinglePageLevel();

        btreeMeta->SetLowestSinglePage(lowestSinglePageId);
        btreeMeta->SetLowestSinglePageLevel(m_payloadRight.GetLinkAndStatus()->GetLevel());
        UNUSED_VARIABLE(m_bufMgr->MarkDirty(btreeMetaBuf));

        ErrLog(
            DSTORE_DEBUG1, MODULE_INDEX,
            ErrMsg("Lowest single page has moved from {%d, %u} to {%d, %u} with level changed from %u to %u",
                   oldLowestSingle.m_fileId, oldLowestSingle.m_blockId, lowestSinglePageId.m_fileId,
                   lowestSinglePageId.m_blockId, oldLowestSingleLevel, m_payloadRight.GetLinkAndStatus()->GetLevel()));

        /* Update BtrMeta cached in m_indexRel->btreeSmgr */
        if (STORAGE_FUNC_SUCC(GetBtreeSmgr()->SetMetaCache(btreeMeta))) {
            GetBtreeSmgr()->UpdateLowestSinglePageCache(m_payloadRight.GetBuffDesc());
        }
    }
    return btreeMetaBuf;
}

BtrPageRecycleStat BtreePageUnlink::DoUnlinkFromSiblings(const PageId pageId)
{
    if (unlikely(pageId == INVALID_PAGE_ID || m_payloadLeaf.GetBuffDesc() == INVALID_BUFFER_DESC)) {
        m_recyclable = false;
        return BtrPageRecycleStat::RECYCLE_FAILED;
    }
    StorageAssert(m_payloadLeaf.GetBuffDesc() != INVALID_BUFFER_DESC);

    BtrPageRecycleStat ret = GetTargetAndSiblings(pageId);
    if (unlikely(ret != BtrPageRecycleStat::RECYCLABLE)) {
        if (pageId == m_payloadLeaf.GetPageId()) {
            /* leaf and target are the same page. Set target invalid to avoid release the same page twice. */
            m_payloadTarget.buffDesc = INVALID_BUFFER_DESC;
        }
        return ret;
    }

    BtrPageLinkAndStatus *linkStat = m_payloadTarget.GetLinkAndStatus();
    /* Update subtree root for next call */
    PageId nextSubtreeRoot;
    bool isLeaf = (pageId == m_pageId);
    FreeQueueSlot freeSlot = {INVALID_PAGE_ID, -1};
    if (isLeaf) {
        /* We are deleting leaf page in this call. Set subtree root as invalid to stop further calling. */
        nextSubtreeRoot = INVALID_PAGE_ID;
    } else {
        OffsetNumber finalOffset = m_payloadTarget.GetPage()->GetMaxOffset();
        IndexTuple *finalTuple = m_payloadTarget.GetPage()->GetIndexTuple(finalOffset);
        nextSubtreeRoot = finalTuple->GetLowlevelIndexpageLink();
        if (STORAGE_FUNC_FAIL(GetBtreeSmgr()->GetSlotFromFreeQueue(freeSlot, pageId)) ||
            !freeSlot.IsValid()) {
            m_recyclable = false;
            return BtrPageRecycleStat::RECYCLE_FAILED;
        }
    }
    SetSubtreeRoot(nextSubtreeRoot);
    if (!isLeaf) {
        UNUSED_VARIABLE(m_bufMgr->MarkDirty(m_payloadLeaf.GetBuffDesc()));
    } else {
        UNUSED_VARIABLE(m_bufMgr->MarkDirty(m_payloadLeaf.GetBuffDesc(), false));
    }

    /* Update sibling links on target's sibling */
    UpdateTargetSiblingLinks();

    /* Mark target page as deleted. */
    linkStat->SetLiveStatus(BtrPageLiveStatus::EMPTY_NO_PARENT_NO_SIB);
    UNUSED_VARIABLE(m_bufMgr->MarkDirty(m_payloadTarget.GetBuffDesc()));

    /*
     * If we are deleting the next-to-last page on the target's level, the rightsib is a candidate to become the
     * new fast root. (In theory, it might be possible to push the fast root even further down, but the odds of doing
     * so are slim, and the locking considerations daunting.)
     */
    BufferDesc *btreeMetaBuf = UpdateLowestSinglePageIfNeeded(linkStat->GetRight());

    if (NeedWal()) {
        GenerateWalForUnlinkFromSiblings(isLeaf, nextSubtreeRoot, btreeMetaBuf);
    }

    /* Release pages. Need to release target page before adding into FSM */
    if (btreeMetaBuf != INVALID_BUFFER_DESC) {
        m_bufMgr->UnlockAndRelease(btreeMetaBuf);
    }
    m_payloadLeft.Drop(m_bufMgr);
    m_payloadRight.Drop(m_bufMgr);

    if (unlikely(static_cast<uint64>(g_traceSwitch) & BTREE_STATISTIC_INFO_MIN_TRACE_LEVEL)) {
        GetBtreeSmgr()->RecordBtreeOperInLevel(BtreeOperType::BTR_OPER_RECYCLED,
                                               m_payloadTarget.GetLinkAndStatus()->GetLevel());
    }

    if (isLeaf) {
        /* We are recycling a page assigned by FSM for later reusing. Do not put it back to FSM again. */
        /* We do not unlock & release leaf buffer descriptor here for later checking. */
        /* Just set target buffer invalid here to avoid double-release in deconstruction. */
        m_payloadTarget.buffDesc = INVALID_BUFFER_DESC;
    } else {
        StorageAssert(pageId == m_payloadTarget.GetPageId());
        m_payloadTarget.GetPage()->EmptyPage();
        m_payloadTarget.Drop(m_bufMgr);
        /* Add pivot page into FSM */
        if (STORAGE_FUNC_FAIL(GetBtreeSmgr()->WriteSlotToFreeQueue(freeSlot, pageId))) {
            m_recyclable = false;
            return BtrPageRecycleStat::RECYCLE_FAILED;
        }
        ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
               ErrMsg("Remove pivot page {%d, %u} and put it into freePageQueue", pageId.m_fileId, pageId.m_blockId));
    }
    return BtrPageRecycleStat::RECYCLABLE;
}

RetStatus BtreePageUnlink::DoUnlinkLeafFromSiblings()
{
    if (unlikely(!m_payloadLeaf.GetLinkAndStatus()->TestLiveStatus(BtrPageLiveStatus::EMPTY_NO_PARENT_HAS_SIB))) {
        m_recyclable = false;
    }
    if (unlikely(!m_recyclable) || unlikely(!IsPageEmptyAndRecyclable())) {
        /* Leaf content has been changed, stop recycling */
        storage_set_error(INDEX_ERROR_CHANGED_NO_PARENT_LEAF, m_pageId.m_fileId, m_pageId.m_blockId);
        ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
            ErrMsg("Page {%d, %u} has been changed after unlinked from parent in(%s), segment(%hu, %u) "
            BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT, m_pageId.m_fileId, m_pageId.m_blockId,
            m_indexInfo->indexRelName, GetBtreeSmgr()->GetSegMetaPageId().m_fileId,
            GetBtreeSmgr()->GetSegMetaPageId().m_blockId, BTR_PAGE_HEADER_VAL(m_payloadLeaf.GetPage()),
            BTR_PAGE_LINK_AND_STATUS_VAL(m_payloadLeaf.GetLinkAndStatus())));
        return DSTORE_FAIL;
    }

    BtrPageRecycleStat ret;
    PageId targetPageId = GetSubtreeRoot();
    while (targetPageId != INVALID_PAGE_ID) {
        ErrLog(
            DSTORE_DEBUG1, MODULE_INDEX,
            ErrMsg("Try to unlink Btree page {%d, %u} from siblings", targetPageId.m_fileId, targetPageId.m_blockId));

        ret = DoUnlinkFromSiblings(targetPageId);
        if (ret == BtrPageRecycleStat::RECYCLE_FAILED) {
            return DSTORE_FAIL;
        }
        if (ret == BtrPageRecycleStat::UNRECYCLABLE) {
            /* Some links on leaf have been changed. Reread the page and try recycle again. */
            DropAll();
            if (STORAGE_FUNC_FAIL(ReadBufDesc(m_pageId, &m_payloadLeaf, LW_EXCLUSIVE))) {
                return DSTORE_FAIL;
            }
            if (!m_payloadLeaf.GetLinkAndStatus()->TestLiveStatus(BtrPageLiveStatus::EMPTY_NO_PARENT_HAS_SIB)) {
                m_recyclable = false;
            }
            if (unlikely(!m_recyclable) || !IsPageEmptyAndRecyclable()) {
                /* Leaf content has been changed, stop recycling */
                storage_set_error(INDEX_ERROR_CHANGED_NO_PARENT_LEAF, m_pageId.m_fileId, m_pageId.m_blockId);
                ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
                    ErrMsg("Page {%d, %u} has been changed after unlinked from parent in(%s), segment(%hu, %u) "
                    BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT, m_pageId.m_fileId, m_pageId.m_blockId,
                    m_indexInfo->indexRelName, GetBtreeSmgr()->GetSegMetaPageId().m_fileId,
                    GetBtreeSmgr()->GetSegMetaPageId().m_blockId, BTR_PAGE_HEADER_VAL(m_payloadLeaf.GetPage()),
                    BTR_PAGE_LINK_AND_STATUS_VAL(m_payloadLeaf.GetLinkAndStatus())));
                return DSTORE_FAIL;
            }
        }
        targetPageId = GetSubtreeRoot();
    }

    StorageAssert(m_payloadLeaf.GetLinkAndStatus()->TestLiveStatus(BtrPageLiveStatus::EMPTY_NO_PARENT_NO_SIB));
    return DSTORE_SUCC;
}

RetStatus BtreePageUnlink::GetLeft(const PageId currPageId, PageId leftPageId, const PageId rightPageId)
{
    StorageAssert(leftPageId != INVALID_PAGE_ID);
    if (STORAGE_FUNC_FAIL(ReadBufDesc(leftPageId, &m_payloadLeft, LW_EXCLUSIVE))) {
        return DSTORE_FAIL;
    }

    bool found = true;
    while (m_payloadLeft.GetLinkAndStatus()->TestLiveStatus(BtrPageLiveStatus::EMPTY_NO_PARENT_NO_SIB) ||
           m_payloadLeft.GetLinkAndStatus()->GetRight() != currPageId) {
        leftPageId = m_payloadLeft.GetLinkAndStatus()->GetRight();
        if (leftPageId == currPageId || leftPageId == rightPageId) {
            found = false;
            break;
        }
        if (STORAGE_FUNC_FAIL(ReadBufDesc(leftPageId, &m_payloadLeft, LW_EXCLUSIVE))) {
            return DSTORE_FAIL;
        }
        if (m_payloadLeft.GetLinkAndStatus()->IsRightmost()) {
            found = false;
            break;
        }
    }

    if (!found) {
        m_payloadLeft.Drop(m_bufMgr);
        return DSTORE_FAIL;
    }
    StorageAssert(m_payloadLeft.GetLinkAndStatus()->GetRight() == currPageId);
    return DSTORE_SUCC;
}

};
