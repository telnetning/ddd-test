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
 * dstore_btree_page_unlink.h
 *
 * IDENTIFICATION
 *        dstore/include/index/dstore_btree_page_unlink.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef SRC_GAUSSKERNEL_INCLUDE_INDEX_STORAGE_BTREE_PAGE_UNLINK
#define SRC_GAUSSKERNEL_INCLUDE_INDEX_STORAGE_BTREE_PAGE_UNLINK

#include "index/dstore_btree_split.h"
#include "index/dstore_btree_prune.h"

namespace DSTORE {

enum class BtrPageRecycleStat {
    RECYCLABLE,
    UNRECYCLABLE,
    RECYCLE_FAILED
};

/*
 * BtreePageUnlink: Second stage of Btree page prune
 *
 * Check if an FSM-assigned page:
 *      1. Has been completely removed from Btree and can be reused directly.
 *      2. Is hanging and has no open transaction running on. Page can be reused after recycle.
 *      3. Is hanging but cannot be recycled at the moment. Do not recycle it now and return it back to FSM.
 *
 * Recycle a hanging page:
 *      1. Search for the to-be-deleted subtree and its parent page leading to the to-be-recycled leaf page.
 *      2. Remove the downlink to root of the subtree and mark leaf page as BTP_EMPTY_NO_PARENT_HAS_SIB.
 *         (We can never mark a pivot page half-dead nor add a pivot page into FSM before completely deleted.)
 *      3. Check every page along the subtree and remove links between their sibling pages. Then pivot pages will be
 *         marked as BTP_EMPTY_NO_PARENT_NO_SIB and be added into FSM.
 */
class BtreePageUnlink : public BtreeSplit {
public:
    BtreePageUnlink(BufferDesc *buffDesc, StorageRelation indexRel, IndexInfo *indexInfo, ScanKey scanKey);

    /*
     * TryUnlinkPageFromBtree
     *
     * Unlink a page from btree if the page is empty and recyclable.
     * return DSTORE_SUCC if page is successfully unlinked, otherwise return DSTORE_FAIL.
     */
    RetStatus TryUnlinkPageFromBtree();
    bool IsPageEmptyAndRecyclable(bool needPrune = false);

    inline BufferDesc *GetBufDesc()
    {
        return m_payloadLeaf.GetBuffDesc();
    }

    inline void Clear(BufMgrInterface *bufMgr)
    {
        m_payloadLeaf.Drop(bufMgr);
    }
    inline bool IsRecyclable() const
    {
        return m_recyclable;
    }
    inline CommitSeqNo GetCurrMaxCsn() const
    {
        return m_currMaxCsn;
    }
    inline void SetCurrMaxCsn(CommitSeqNo maxCsn)
    {
        m_currMaxCsn = maxCsn;
    }

#ifdef UT
    void SetStack(BtrStack stack)
    {
        ClearStack();
        m_leafStack = stack;
    }
#endif

private:
    RetStatus DoUnlinkFromParent();
    RetStatus DoUnlinkLeafFromSiblings();
    void DropAll();

    BtrPageRecycleStat DoUnlinkFromSiblings(const PageId pageId);

    bool IsLeafRecyclableNow();
    bool IsPageRecyclableNow(BufferDesc *pageBuf, bool unlockLeft = true);

    RetStatus SearchForLeaf(const PageId left);
    RetStatus GetRecycleSubtree(BtrPageRecycleStat &stat, const PageId childPageId, BtrStack stack, PageId *topPageId,
                                         OffsetNumber *pivotOff, PageId *subtreeRootPageId);
    BtrPageRecycleStat CanRightmostUnlink(OffsetNumber currOff, BufferDesc *parentBuf, const PageId subtreeTop);
    RetStatus CheckNonParentPage(const PageId pageId);
    RetStatus CheckRecyclableSubtreeTop(const PageId topPageId, const PageId subtreePageId, OffsetNumber pivotOff);

    BtrPageRecycleStat GetTargetAndSiblings(const PageId pageId);
    BtrPageRecycleStat CheckUnlinkTargetRecyclable();
    void UpdateTargetSiblingLinks();
    BufferDesc *UpdateLowestSinglePageIfNeeded(const PageId lowestSinglePageId);

    RetStatus GetLeft(const PageId currPageId, PageId leftPageId, const PageId rightPageId);
    RetStatus ReadBufDesc(const PageId pageId, BtreePagePayload *payload, LWLockMode access);

    void GenerateWalForUnlinkFromParent(OffsetNumber pivotOff, const PageId subtreeRoot, const PageId newTopDownlink);
    void GenerateWalForUnlinkFromSiblings(bool isLeaf, const PageId nextSubtreeRoot, BufferDesc *btreeMetaBuf);
    void GenerateWalForLiveStatusUpdate(BufferDesc *pageBuf, BtrPageLiveStatus liveStatus);

    PageId GetSubtreeRoot() const
    {
        if (unlikely(m_payloadLeaf.buffDesc == INVALID_BUFFER_DESC)) {
             return INVALID_PAGE_ID;
        }
        IndexTuple *hikey = m_payloadLeaf.page->GetIndexTuple(BTREE_PAGE_HIKEY);
        return hikey->GetLowlevelIndexpageLink();
    }

    void SetSubtreeRoot(const PageId pageId)
    {
        IndexTuple *hikey = m_payloadLeaf.page->GetIndexTuple(BTREE_PAGE_HIKEY);
        hikey->SetLowlevelIndexpageLink(pageId);
    }

    PageId m_pageId; /* To-be-recycled page should be given while construction. It must never be changed
                      * during recycling process, otherwise we should give up recycling. */
    CommitSeqNo m_currMaxCsn;
    bool m_recyclable;

    BtreePagePayload m_payloadLeaf;   /* BtreePagePayLoad contains the to-be-deleted leaf page */
    BtreePagePayload m_payloadTop;    /* BtreePagePayLoad contains the parent of to-be-deleted subtree's root */
    BtreePagePayload m_payloadTarget; /* BtreePagePayLoad contains the target page we are dealing with */
    BtreePagePayload m_payloadLeft;   /* BtreePagePayLoad contains the left libling page of the target */
    BtreePagePayload m_payloadRight;  /* BtreePagePayLoad contains the right libling page of the target */
};

}
#endif /* SRC_GAUSSKERNEL_INCLUDE_INDEX_STORAGE_BTREE_PAGE_UNLINK */