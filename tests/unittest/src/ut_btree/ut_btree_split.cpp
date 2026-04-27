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
 */
#include "ut_btree/ut_btree.h"

class IncompleteSplitBtree : public BtreeSplit {
public:
    IncompleteSplitBtree(UTTableHandler *utTableHandler)
        : BtreeSplit(utTableHandler->GetIndexRel(), utTableHandler->GetIndexInfo(), utTableHandler->GetIndexScanKey(), true) {};

    BufferDesc *GetLeftMostLeaf();
    BufferDesc *GetLeafParent(PageId leafPageId);
    void GenerateIncompleteSplitPages(BufferDesc *targetBuf, BufferDesc *childBuf = INVALID_BUFFER_DESC);
};

BufferDesc *IncompleteSplitBtree::GetLeftMostLeaf()
{
    /* Generate an infinity minus searching key */
    IndexTuple *searchingTarget = IndexTuple::CreateMinusinfPivotTuple();

    UpdateScanKeyWithValues(searchingTarget);

    /* Leaf page will be write-locked again after searching */
    BufferDesc *pageBuf = INVALID_BUFFER_DESC;
    SearchBtree(&pageBuf, false);

    return pageBuf;
}

BufferDesc *IncompleteSplitBtree::GetLeafParent(PageId leafPageId)
{
    return GetParentBufDesc(leafPageId, m_leafStack, DSTORE::LW_EXCLUSIVE);
}

void IncompleteSplitBtree::GenerateIncompleteSplitPages(BufferDesc *targetBuf, BufferDesc *childBuf)
{
    Transaction *transaction = thrd->GetActiveTransaction();
    transaction->Start();
    BtrPage *targetPage = static_cast<BtrPage *>(targetBuf->GetPage());
    BtrPageLinkAndStatus *targetLinkStat = targetPage->GetLinkAndStatus();
    OffsetNumber maxOff = targetPage->GetMaxOffset();
    OffsetNumber minOff = targetLinkStat->GetFirstDataOffset();
    OffsetNumber midOff = minOff + ((maxOff - minOff) / 2);
    EXPECT_GE(maxOff, minOff);

    IndexTuple *insertTuple = targetPage->GetIndexTuple(midOff)->Copy();
    UpdateScanKeyWithValues(insertTuple);
    EXPECT_LE(CompareKeyToTuple(targetPage, targetPage->GetLinkAndStatus(), maxOff,
                                targetPage->GetLinkAndStatus()->TestType(BtrPageType::INTERNAL_PAGE)), 0);
    EXPECT_GE(CompareKeyToTuple(targetPage, targetPage->GetLinkAndStatus(), minOff,
                                targetPage->GetLinkAndStatus()->TestType(BtrPageType::INTERNAL_PAGE)), 0);

    insertTuple->SetTdId(INVALID_TD_SLOT);
    while (targetPage->GetFreeSpaceForInsert() >= insertTuple->GetSize()) {
        targetPage->AddTuple(insertTuple, midOff, INVALID_TD_SLOT);
    }
    transaction->AllocTransactionSlot();
    InitSplittingTarget(targetBuf);
    EXPECT_EQ(SplitPage(insertTuple, midOff, childBuf), DSTORE_SUCC);
    transaction->Commit();
}

TEST_F(UTBtree, FinishIncompleteSplitTest_level0)
{
    int rowNum = 100;
    /* Note that we're not going to keep index consistent with heap table in this test.
     * Heap is only used for BtreeBuild */
    m_utTableHandler->FillTableWithRandomData(rowNum);

    int indexCols[] = {1, 2, 3, 4};
    int numKeyAttrs = sizeof(indexCols) / sizeof(int);
    m_utTableHandler->CreateIndex(indexCols, numKeyAttrs, false);
    IncompleteSplitBtree splitBtree(m_utTableHandler);

    /* Get root */
    BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
    BufferDesc *rootBuf = splitBtree.GetLeftMostLeaf();
    PageId rootPageId = rootBuf->GetPageId();
    BtrPage *rootPage = static_cast<BtrPage *>(rootBuf->GetPage());
    BtrPageLinkAndStatus *leafRootLinkStat = rootPage->GetLinkAndStatus();
    EXPECT_TRUE(leafRootLinkStat->TestType(DSTORE::BtrPageType::LEAF_PAGE));
    EXPECT_TRUE(leafRootLinkStat->IsRoot());

    /* Generate incomplete splitting page */
    splitBtree.GenerateIncompleteSplitPages(rootBuf);
    EXPECT_FALSE(leafRootLinkStat->IsSplitComplete());
    bufMgr->MarkDirty(rootBuf);
    bufMgr->UnlockAndRelease(rootBuf);

    /* Finish complete while searching */
    BufferDesc *leftmostBuf = splitBtree.GetLeftMostLeaf();
    EXPECT_EQ(rootPageId, leftmostBuf->GetPageId());
    BtrPage *leftmostPage = static_cast<BtrPage *>(leftmostBuf->GetPage());
    BtrPageLinkAndStatus *leftmostLinkStat = leftmostPage->GetLinkAndStatus();
    EXPECT_TRUE(leftmostLinkStat->IsSplitComplete());
    EXPECT_FALSE(leftmostLinkStat->IsRoot());
    bufMgr->UnlockAndRelease(leftmostBuf);
}

TEST_F(UTBtree, FinishIncompleteSplitTestWithoutNulls_level0)
{
    int rowNum = 100;
    /* Note that we're not going to keep index consistent with heap table in this test.
     * Heap is only used for BtreeBuild */
    m_utTableHandler->FillTableWithRandomData(rowNum);
 
    int indexCols[] = {2};
    int numKeyAttrs = sizeof(indexCols) / sizeof(int);
    m_utTableHandler->CreateIndex(indexCols, numKeyAttrs, false);
    IncompleteSplitBtree splitBtree(m_utTableHandler);
 
    /* Get root */
    BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
    BufferDesc *rootBuf = splitBtree.GetLeftMostLeaf();
    PageId rootPageId = rootBuf->GetPageId();
    splitBtree.m_scanKeyValues.cmpFastFlag = true;
    BtrPage *rootPage = static_cast<BtrPage *>(rootBuf->GetPage());
    BtrPageLinkAndStatus *leafRootLinkStat = rootPage->GetLinkAndStatus();
    EXPECT_TRUE(leafRootLinkStat->TestType(DSTORE::BtrPageType::LEAF_PAGE));
    EXPECT_TRUE(leafRootLinkStat->IsRoot());
 
    /* Generate incomplete splitting page */
    splitBtree.GenerateIncompleteSplitPages(rootBuf);
    EXPECT_FALSE(leafRootLinkStat->IsSplitComplete());
    bufMgr->MarkDirty(rootBuf);
    bufMgr->UnlockAndRelease(rootBuf);
 
    /* Finish complete while searching */
    BufferDesc *leftmostBuf = splitBtree.GetLeftMostLeaf();
    EXPECT_EQ(rootPageId, leftmostBuf->GetPageId());
    BtrPage *leftmostPage = static_cast<BtrPage *>(leftmostBuf->GetPage());
    BtrPageLinkAndStatus *leftmostLinkStat = leftmostPage->GetLinkAndStatus();
    EXPECT_TRUE(leftmostLinkStat->IsSplitComplete());
    EXPECT_FALSE(leftmostLinkStat->IsRoot());
    bufMgr->UnlockAndRelease(leftmostBuf);
}
 

TEST_F(UTBtree, FinishIncompleteSplitLeafAfterRecycleTest_level0) {
    /* Generate a recyclable tree */
    int numLeaves = 3;
    int rootLevel = 2;
    UTRecyclableBtree *recyclableBtree = GenerateRecyclableSubtree(numLeaves, rootLevel);

    /* Recycle a "skinny subtree". */
    /* ---- Since we have 4 leaves in total, according to the rule we built this recyclable Btree, the leftmost leaf
     * page belongs to a "skinny subtree" that we can recycle 3 pages from level 0 to level2.*/
    recyclableBtree->RecyclePageAndCheck(0);
    uint32 lowestSingleLevel = recyclableBtree->GetBtrStorageMgr()->GetLowestSingleLevelFromMetaCache();
    EXPECT_EQ(rootLevel, recyclableBtree->GetBtrStorageMgr()->GetRootLevelFromMetaCache());
    EXPECT_EQ(rootLevel, lowestSingleLevel + 1);

    /* Finish incomplete splitting leaf page */
    /* split leftmost leaf */
    /* Get stack before change parent live status, or we'll fail for searching leaf */
    BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
    IncompleteSplitBtree splitBtree(recyclableBtree->m_utTableHandler);
    BufferDesc *leftmostBuf = splitBtree.GetLeftMostLeaf();
    BtrPageLinkAndStatus *leftmostLinkStat = static_cast<BtrPage *>(leftmostBuf->GetPage())->GetLinkAndStatus();
    splitBtree.GenerateIncompleteSplitPages(leftmostBuf);
    EXPECT_FALSE(leftmostLinkStat->IsSplitComplete());
    bufMgr->MarkDirty(leftmostBuf);
    bufMgr->UnlockAndRelease(leftmostBuf);
    recyclableBtree->m_utTableHandler->GetBtreeSmgr()->ReleaseMetaCache();

    /* Finish split leaf while searching */
    leftmostBuf = splitBtree.GetLeftMostLeaf();
    leftmostLinkStat = static_cast<BtrPage *>(leftmostBuf->GetPage())->GetLinkAndStatus();
    EXPECT_TRUE(leftmostLinkStat->IsSplitComplete());
    bufMgr->UnlockAndRelease(leftmostBuf);
}

TEST_F(UTBtree, FinishIncompleteSplitInternalAfterRecycleTest_level0) {
    BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
    /* Generate a recyclable tree */
    int numLeaves = 3;
    int rootLevel = 2;
    UTRecyclableBtree *recyclableBtree = GenerateRecyclableSubtree(numLeaves, rootLevel);

    /* Recycle a "skinny subtree". */
    /* ---- Since we have 4 leaves in total, according to the rule we built this recyclable Btree, the leftmost leaf
     * page belongs to a "skinny subtree" that we can recycle 3 pages from level 0 to level2.*/
    recyclableBtree->RecyclePageAndCheck(0);
    uint32 lowestSingleLevel = recyclableBtree->GetBtrStorageMgr()->GetLowestSingleLevelFromMetaCache();
    EXPECT_EQ(rootLevel, recyclableBtree->GetBtrStorageMgr()->GetRootLevelFromMetaCache());
    EXPECT_EQ(rootLevel, lowestSingleLevel + 1);

    /* Finish incomplete splitting internal page */
    /* Split parent of leftmost leaf */
    IncompleteSplitBtree splitBtree(recyclableBtree->m_utTableHandler);
    BufferDesc *leftmostBuf = splitBtree.GetLeftMostLeaf();
    PageId leftmostPageId = leftmostBuf->GetPageId();

    BufferDesc *parentBuf = splitBtree.GetLeafParent(leftmostPageId);
    PageId parentPageId = parentBuf->GetPageId();
    BtrPageLinkAndStatus *parentLinkStat = static_cast<BtrPage *>(parentBuf->GetPage())->GetLinkAndStatus();
    /* leftmostBuf would unlocked after parentBuf split */
    splitBtree.GenerateIncompleteSplitPages(parentBuf, leftmostBuf);
    EXPECT_FALSE(parentLinkStat->IsSplitComplete());
    bufMgr->MarkDirty(parentBuf);
    bufMgr->UnlockAndRelease(parentBuf);

    /* Finish split internal page while searching */
    leftmostBuf = splitBtree.GetLeftMostLeaf();
    bufMgr->UnlockAndRelease(leftmostBuf);
    parentBuf = bufMgr->Read(g_defaultPdbId, parentPageId, LW_SHARED);
    parentLinkStat = static_cast<BtrPage *>(parentBuf->GetPage())->GetLinkAndStatus();
    EXPECT_TRUE(parentLinkStat->IsSplitComplete());
    bufMgr->UnlockAndRelease(parentBuf);

    delete recyclableBtree;
}
