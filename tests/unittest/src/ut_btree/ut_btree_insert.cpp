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
#include <gtest/gtest.h>
#include "index/dstore_btree_build.h"
#include "ut_btree/ut_btree.h"

TEST_F(UTBtree, BtreeMetaCacheWhenInsertTest_level0)
{
    /* Build btree index */
    int indexCols[] = {4, 5, 6, 7, 8, 9, 10, 11};
    bool isUnique = false;
    int numAttrs = sizeof(indexCols) / sizeof(int);
    m_utTableHandler->CreateIndex(indexCols, numAttrs, isUnique);
    BtreeStorageMgr *btreeSmgr = m_utTableHandler->GetBtreeSmgr();

    /* Case 1. Clear damaged meta cache and create a new one */
    /* Insert a tuple, then we'll get a btree meta cache in btreeStorageMgr */
    IndexTuple *indexTuple = m_utTableHandler->InsertRandomIndexTuple();
    DstorePfree(indexTuple);
    EXPECT_TRUE(btreeSmgr->HasMetaCache());
    /* Damage the btree meta cache */
    btreeSmgr->SetMetaCacheLowestSinglePageId(INVALID_PAGE_ID);
    /* Make sure that we've made the change in btreeStorageMgr */
    EXPECT_FALSE(btreeSmgr->GetLowestSinglePageIdFromMetaCache().IsValid());

    /* Insert again. Btree should clear the damaged meta cache and create a new one */
    indexTuple = m_utTableHandler->InsertRandomIndexTuple();
    DstorePfree(indexTuple);
    EXPECT_TRUE(btreeSmgr->HasMetaCache());
    EXPECT_TRUE(btreeSmgr->GetLowestSinglePageIdFromMetaCache().IsValid());
}

TEST_F(UTBtree, BtreeInsertTest_level1)
{
    /* Fill table with specific data */
    int rowNum = 200;
    DefaultRowDef *tableDef = (DefaultRowDef*)DstorePalloc(rowNum * sizeof(DefaultRowDef));
    /* Insert (2,2)(4,4)...(400,400) */
    for (int i = 1; i <= rowNum; ++i) {
        int16 r1 = (int16)(i * 2);
        int32 r2 = (int32)(i * 2);
        DefaultRowDef rowDef = m_utTableHandler->GetDefaultRowDef();
        rowDef.column_int16 = r1;
        rowDef.column_int32 = r2;
        tableDef[i - 1] = rowDef;
    }
    m_utTableHandler->FillTableWithSpecificData(tableDef, rowNum);
    DstorePfree(tableDef);

    /* Build btree index */
    int indexCols[] = {1, 2};
    bool isUnique = false;
    int numAttrs = sizeof(indexCols) / sizeof(int);
    m_utTableHandler->CreateIndex(indexCols, numAttrs, isUnique);
    BtreeStorageMgr *btrSmgr = m_utTableHandler->GetBtreeSmgr();
    TupleDesc indexTupleDesc = m_utTableHandler->GetIndexTupleDesc();

    /* Test btree meta page info */
    PageId metaPageId = btrSmgr->GetBtrMetaPageId();
    PageId btrRootPageId;

    /*
     * Insert (1,1)(3,3)...(399,399)(401,401)(401,402)(402,403)(402,404)(403,405)(403,406)...
     * until the btree has 3 level, about 71957 tuples.
     * The position means where the indexTuple should be inserted in the all leaf level.
     * The test includes leaf page split, pivot page split, create new root, add downlink to parent.
     */
    PageId lastRootPageId = INVALID_PAGE_ID;
    bool isThreeLevel = false;
    int remainNum = 10;
    uint32 rootLevel = 0;
    int insertNum = 1;
    int16 r1;
    int32 r2;
    bool reverseFlag = true;
    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    while (true) {
        std::queue<PageId> leafPageIds;
        std::queue<PageId> pivotPageIds;
        r1 = insertNum <= rowNum + 1 ? (int16)(insertNum * 2 - 1) : (int16)(r1 + (!reverseFlag));
        r2 = insertNum <= rowNum + 1 ? (int32)(insertNum * 2 - 1) : (int32)(r2 + 1);
        int position = r2;
        /* Insert specific tuple and check its position */
        DefaultRowDef insertRow = m_utTableHandler->GetDefaultRowDef();
        insertRow.column_int16 = r1;
        insertRow.column_int32 = r2;
        IndexTuple *indexTuple = InsertSpecificIndexTuple(&insertRow, DefaultNullBitMap, true);

        /* check every 10 times for saving ut time */
        btrRootPageId = btrSmgr->GetRootPageIdFromMetaCache();
        if (btrRootPageId != lastRootPageId) {
            rootLevel = btrSmgr->GetRootLevelFromMetaCache();
            if (rootLevel == 0) {
                leafPageIds.push(btrRootPageId);
            } else if (rootLevel == 1) {
                pivotPageIds.push(btrRootPageId);
                leafPageIds = TestPivotTuples(indexTupleDesc, pivotPageIds, 1);
            } else if (rootLevel == 2) {
                /* When btree has 3 levels, insert remainNum tuples finally */
                pivotPageIds.push(btrRootPageId);
                pivotPageIds = TestPivotTuples(indexTupleDesc, pivotPageIds, 2);
                leafPageIds = TestPivotTuples(indexTupleDesc, pivotPageIds, 1);
                isThreeLevel = true;
            } else {
                StorageAssert(0);
            }
            /* Test the inserted tuple position */
            bool found = IsTupleInRightPosition(indexTupleDesc, leafPageIds, position, indexTuple);
            EXPECT_TRUE(found);
            lastRootPageId = btrRootPageId;
        }

        if (isThreeLevel && remainNum-- == 0) {
            break;
        }
        insertNum++;
        DstorePfreeExt(indexTuple);
    }
    txn->Commit();

    /* Insert some random tuples and check order */
    int randomNum = 10;
    for (int i = 0; i < randomNum; ++i) {
        m_utTableHandler->InsertRandomIndexTuple();
    }
    std::queue<PageId> pivotPageIds;
    pivotPageIds.push(btrRootPageId);
    pivotPageIds = TestPivotTuples(indexTupleDesc, pivotPageIds, 2);
    std::queue<PageId> leafPageIds = TestPivotTuples(indexTupleDesc, pivotPageIds, 1);
    TestLeafTuples(indexTupleDesc, leafPageIds, rowNum + insertNum + randomNum);
}

TEST_F(UTBtree, BtreeInsertHugeTupleTest_level0)
{
    /* Build btree index using empty table */
    int indexCols[] = {TEXT_IDX}; /* text */
    bool isUnique = false;
    int numAttrs = sizeof(indexCols) / sizeof(int);
    m_utTableHandler->CreateIndex(indexCols, numAttrs, isUnique);

    DefaultRowDef insertRow = m_utTableHandler->GetDefaultRowDef();
    /* max tuple (header + value) 2630, header 16. textheader:4, data len = 2630 - 16 - 4 = 2610 */
    insertRow.column_text = static_cast<text *>(m_utTableHandler->GenerateTextWithFixedLen(2610));

    int rowNum = 10;
    bool *nullbitmap = DefaultNullBitMap;
    IndexTuple *indexTuple = nullptr;
    for (int i = 0; i < rowNum; i++) {
        indexTuple = InsertSpecificIndexTuple(&insertRow, nullbitmap);
        EXPECT_NE(indexTuple, nullptr);
        DstorePfree(indexTuple);
    }

    /* Insert len > 2633 should fail */
    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    insertRow.column_text = static_cast<text *>(m_utTableHandler->GenerateTextWithFixedLen(2614));
    indexTuple = InsertSpecificIndexTuple(&insertRow, nullbitmap, true);
    EXPECT_EQ(indexTuple, nullptr);
    EXPECT_EQ(StorageGetErrorCode(), INDEX_ERROR_FAIL_FOR_HUGE_INDEX_TUPLE);
    txn->Abort();
}

TEST_F(UTBtree, BtreeInsertHugeTupleWithNullTest_level0)
{
    /* Build btree index using empty table */
    int indexCols[] = {TEXT_IDX, VARCHAR_IDX}; /* text, text */
    bool isUnique = false;
    int numAttrs = sizeof(indexCols) / sizeof(int);
    m_utTableHandler->CreateIndex(indexCols, numAttrs, isUnique);

    DefaultRowDef insertRow = m_utTableHandler->GetDefaultRowDef();
    bool *nullbitmap = DefaultNullBitMap;
    /* make tuple hasNull */
    insertRow.column_varchar = nullptr;
    nullbitmap[VARCHAR_IDX] = true;
    /* max tuple (header + value) 2633, header 16. null bit map 4, aligned to 8. total header + null bit map = 24
     * textheader:4; data len = 2633 - 24 - 4 = 2605 */
    insertRow.column_text = static_cast<text *>(m_utTableHandler->GenerateTextWithFixedLen(2605));

    int rowNum = 10;
    IndexTuple *indexTuple = nullptr;
    for (int i = 0; i < rowNum; i++) {
        indexTuple = InsertSpecificIndexTuple(&insertRow, nullbitmap);
        EXPECT_NE(indexTuple, nullptr);
        DstorePfree(indexTuple);
    }

    /* Insert len > 2633 should fail */
    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    insertRow.column_text = static_cast<text *>(m_utTableHandler->GenerateTextWithFixedLen(2606));
    indexTuple = InsertSpecificIndexTuple(&insertRow, nullbitmap, true);
    EXPECT_EQ(indexTuple, nullptr);
    EXPECT_EQ(StorageGetErrorCode(), INDEX_ERROR_FAIL_FOR_HUGE_INDEX_TUPLE);
    txn->Abort();
}

TEST_F(UTBtree, BtreeInsertNullKeyTest_level0)
{
    Transaction *txn = thrd->GetActiveTransaction();

    /* Build btree index using empty table */
    int indexCols[] = {1, 2};
    bool isUnique = false;
    int numAttrs = sizeof(indexCols) / sizeof(int);
    m_utTableHandler->CreateIndex(indexCols, numAttrs, isUnique);
    TupleDesc indexTupleDesc = m_utTableHandler->GetIndexTupleDesc();

    /* Insert (1,null)(1,1)(null,1)(null,null)(2,null)(2,2)(null,2)(null,null)... into btree */
    int rowNum = 600;
    IndexTuple *indexTuple;
    bool *nullbitmap = DefaultNullBitMap;
    for (int i = 1; i <= rowNum; ++i) {
        DefaultRowDef insertRow = m_utTableHandler->GetDefaultRowDef();
        insertRow.column_int16 = i;
        insertRow.column_int32 = i;

        /* Insert (i,null) */
        nullbitmap[indexCols[1]] = true;
        indexTuple = InsertSpecificIndexTuple(&insertRow, nullbitmap);
        nullbitmap[indexCols[1]] = false;

        /* Insert (i,i) */
        indexTuple = InsertSpecificIndexTuple(&insertRow);
        /* Insert (null,i) twice, but the second rollback */
        nullbitmap[indexCols[0]] = true;
        indexTuple = InsertSpecificIndexTuple(&insertRow, nullbitmap);
        txn->Start();
        indexTuple = InsertSpecificIndexTuple(&insertRow, nullbitmap, true);
        txn->Abort();
        nullbitmap[indexCols[0]] = false;

        /* Insert (null,null) twice, but the second rollback */
        nullbitmap[indexCols[0]] = true;
        nullbitmap[indexCols[1]] = true;
        indexTuple = InsertSpecificIndexTuple(&insertRow, nullbitmap);
        txn->Start();
        indexTuple = InsertSpecificIndexTuple(&insertRow, nullbitmap, true);
        txn->Abort();

        nullbitmap[indexCols[0]] = false;
        nullbitmap[indexCols[1]] = false;
    }

    /* Test btree meta page info */
    PageId metaPageId = m_utTableHandler->GetBtreeSmgr()->GetBtrMetaPageId();
    PageId btrRootPageId = TestBtreeMetaPage(metaPageId);
    TestBtreeRootPage(btrRootPageId, 1);

    /* Test every tuple which should be null or not null */
    std::queue<PageId> pivotPageIds;
    pivotPageIds.push(btrRootPageId);
    std::queue<PageId> leafPageIds = TestPivotTuples(indexTupleDesc, pivotPageIds, 1);
    TestNullLeafTuplesInSpecificOrder(indexTupleDesc, leafPageIds, rowNum * 4);
}

TEST_F(UTBtree, BtreeInsertUniqueCheckTest_level0)
{
    /* Build btree index using empty table */
    int indexCols[] = {1, 2};
    bool isUnique = true;
    int numAttrs = sizeof(indexCols) / sizeof(int);
    m_utTableHandler->CreateIndex(indexCols, numAttrs, isUnique);
    TupleDesc indexTupleDesc = m_utTableHandler->GetIndexTupleDesc();

    /* Insert (1,1)(1,1)(2,2)(2,2)... into btree */
    int rowNum = 600;
    IndexTuple *indexTuple;
    bool *nullbitmap = DefaultNullBitMap;
    for (int i = 1; i <= rowNum; ++i) {
        DefaultRowDef insertRow = m_utTableHandler->GetDefaultRowDef();
        insertRow.column_int16 = i;
        insertRow.column_int32 = i;
        /* Insert (i,i) success */
        indexTuple = InsertSpecificIndexTuple(&insertRow);
        EXPECT_NE(indexTuple, nullptr);
        /* Insert (i,i) fail */
        indexTuple = InsertSpecificIndexTuple(&insertRow);
        EXPECT_EQ(indexTuple, nullptr);
        /* Insert (i,null) success */
        nullbitmap[indexCols[1]] = true;
        indexTuple = InsertSpecificIndexTuple(&insertRow, nullbitmap);
        nullbitmap[indexCols[1]] = false;
        EXPECT_NE(indexTuple, nullptr);
        /* Insert (i,null) success, because null is not equal null */
        nullbitmap[indexCols[1]] = true;
        indexTuple = InsertSpecificIndexTuple(&insertRow, nullbitmap);
        nullbitmap[indexCols[1]] = false;
        EXPECT_NE(indexTuple, nullptr);
    }

    /* Test btree meta page info */
    PageId metaPageId = m_utTableHandler->GetBtreeSmgr()->GetBtrMetaPageId();
    PageId btrRootPageId = TestBtreeMetaPage(metaPageId);
    TestBtreeRootPage(btrRootPageId, 1);

    /* Test all tuples */
    std::queue<PageId> pivotPageIds;
    pivotPageIds.push(btrRootPageId);
    std::queue<PageId> leafPageIds = TestPivotTuples(indexTupleDesc, pivotPageIds, 1);
    TestLeafTuples(indexTupleDesc, leafPageIds, rowNum * 3);
}

TEST_F(UTBtree, BtreeInsertUniqueCheckOverPageTest_level0)
{
    /* Insert more than two pages of same values into table */
    /* Fill table with specific data */
    int rowNum = 600;
    DefaultRowDef *tableDef = (DefaultRowDef*)DstorePalloc(rowNum * sizeof(DefaultRowDef));
    for (int i = 1; i <= rowNum; ++i) {
        DefaultRowDef rowDef = m_utTableHandler->GetDefaultRowDef();
        rowDef.column_int16 = (int16)(100);
        tableDef[i - 1] = rowDef;
    }
    m_utTableHandler->FillTableWithSpecificData(tableDef, rowNum);
    DstorePfree(tableDef);

    /* Case 1. Create unique index. It should be failed  */
    bool isUnique = true;
    int indexCols[] = {1};
    int numKeyAttrs = sizeof(indexCols) / sizeof(int);

    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetSnapshotCsn();
    m_utTableHandler->CreateBtreeContext(indexCols, numKeyAttrs, isUnique);

    BtreeBuild btreeBuildUniq(m_utTableHandler->GetIndexRel(), m_utTableHandler->GetIndexBuildInfo(),
                              m_utTableHandler->GetIndexScanKey());
    EXPECT_EQ(btreeBuildUniq.BuildIndex(), DSTORE_FAIL);

    Datum duplicateValues[DSTORE::INDEX_MAX_KEY_NUM];
    bool duplicateisnulls[DSTORE::INDEX_MAX_KEY_NUM];
    TupleInterface::DeformIndexTuple(m_utTableHandler->GetIndexBuildInfo()->duplicateTuple,
         m_utTableHandler->GetIndexInfo()->attributes, duplicateValues, duplicateisnulls);
    EXPECT_EQ(DatumGetInt16(duplicateValues[0]), 100);

    txn->Abort();
    if (m_utTableHandler->m_btreeTestContext && (m_utTableHandler->m_btreeTestContext->indexRel->btreeSmgr != nullptr)) {
        StorageTableInterface::DestroyBtreeSmgr(m_utTableHandler->m_btreeTestContext->indexRel->btreeSmgr);
        DstorePfree(m_utTableHandler->m_btreeTestContext->indexRel);
    }
    m_utTableHandler->m_btreeTestContext = nullptr;

    /* Case 2. Create index. Delete all duplicates then check unique */
    /* ---- Step 1. Create index */
    txn->Start();
    txn->SetSnapshotCsn();
    isUnique = false;
    m_utTableHandler->CreateBtreeContext(indexCols, numKeyAttrs, isUnique);
    BtreeBuild btreeBuild(m_utTableHandler->GetIndexRel(), m_utTableHandler->GetIndexBuildInfo(),
                          m_utTableHandler->GetIndexScanKey());
    EXPECT_EQ(btreeBuild.BuildIndex(), DSTORE_SUCC);

    txn->Commit();

    /* ---- Step 2. Delele all duplicates except the first indextuple on rightmost page */
    BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
    BufferDesc *metaBuf = INVALID_BUFFER_DESC;
    BtrMeta *btrMeta = m_utTableHandler->GetBtreeSmgr()->GetBtrMeta(LW_SHARED, &metaBuf);
    PageId currPageId = btrMeta->GetLowestSinglePage();
    bufMgr->UnlockAndRelease(metaBuf);

    BufferDesc *currBuf = INVALID_BUFFER_DESC;
    BtrPage *currPage ;
    while (currPageId.IsValid()) {
        if (currBuf != INVALID_BUFFER_DESC) {
            bufMgr->UnlockAndRelease(currBuf);
        }
        currBuf = bufMgr->Read(g_defaultPdbId, currPageId, LW_SHARED);
        currPage = static_cast<BtrPage *>(currBuf->GetPage());
        BtrPageLinkAndStatus *linkStat = currPage->GetLinkAndStatus();
       if (currPage->GetLinkAndStatus()->TestType(BtrPageType::LEAF_PAGE)) {
           EXPECT_EQ(linkStat->GetLevel(), 0);
           EXPECT_TRUE(linkStat->IsLeftmost());
           bufMgr->UnlockAndRelease(currBuf);
           break;
       }
       currPageId = currPage->GetIndexTuple(linkStat->GetFirstDataOffset())->GetLowlevelIndexpageLink();
    }

    int numPages = 0;
    txn->Start();
    txn->SetSnapshotCsn();
    BtreePagePayload currPagePayload;
    while (true) {
        numPages++;
        currPagePayload.Init(g_defaultPdbId, currPageId, LW_EXCLUSIVE, bufMgr);
        PageId nextPageId = currPagePayload.GetLinkAndStatus()->GetRight();
        for (OffsetNumber i = BTREE_PAGE_FIRSTKEY; i <= currPagePayload.GetPage()->GetMaxOffset(); i++) {
            EXPECT_EQ(m_utTableHandler->DeleteWithIndexTuple(currPagePayload, i,
                                                             currPagePayload.GetPage()->GetIndexTuple(i), true),
                      DSTORE_SUCC);
            currPagePayload.Init(g_defaultPdbId, currPageId, LW_EXCLUSIVE, bufMgr);
        }
        currPagePayload.Drop(bufMgr);
        if (!nextPageId.IsValid()) {
            break;
        }
        currPageId = nextPageId;
    }
    txn->Commit();
    EXPECT_GE(numPages, 1);

    /* ---- Step 3. Insert and check unique */
    m_utTableHandler->SetIndexUnique(true);
    DefaultRowDef insertRow = m_utTableHandler->GetDefaultRowDef();
    insertRow.column_int16 = (int16)(100);

    IndexTuple *indexTuple = InsertSpecificIndexTuple(&insertRow);
    EXPECT_EQ(indexTuple, nullptr);
}

TEST_F(UTBtree, BtreeInsertFailAndPruneTest_level0)
{
    /* Build btree index using empty table */
    int indexCols[] = {1, 2};
    bool isUnique = true;
    int numAttrs = sizeof(indexCols) / sizeof(int);
    m_utTableHandler->CreateIndex(indexCols, numAttrs, isUnique);
    /* Insert (1,1)(1,1)(2,2)(2,2)... into btree */
    int rowNum = 1000;
    bool *nullbitmap = DefaultNullBitMap;
    Transaction *txn = thrd->GetActiveTransaction();
    for (int i = 1; i <= rowNum; ++i) {
        txn->Start();
        DefaultRowDef insertRow = m_utTableHandler->GetDefaultRowDef();
        insertRow.column_int16 = i;
        insertRow.column_int32 = i;
        /* Insert (i,i) success */
        nullbitmap[indexCols[0]] = true;
        InsertSpecificIndexTuple(&insertRow, nullbitmap, true);
        txn->Abort();
    }
}