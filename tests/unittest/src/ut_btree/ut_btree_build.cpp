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

TEST_F(UTBtree, BtreeCreateOneLevelTest_level0)
{
    int rowNum = 100;
    m_utTableHandler->FillTableWithRandomData(rowNum);

    int indexCols[] = {1, 2};
    bool isUnique = false;
    int numAttrs = sizeof(indexCols) / sizeof(int);
    g_traceSwitch = BTREE_STATISTIC_INFO_MIN_TRACE_LEVEL;
    m_utTableHandler->CreateIndex(indexCols, numAttrs, isUnique);

    /* Test btree meta page info */
    PageId btrRootPageId = TestBtreeMetaPage(m_utTableHandler->GetBtreeSmgr()->GetBtrMetaPageId());

    /* Test btree root page info */
    TestBtreeRootPage(btrRootPageId, 0);

    /* Test btree leaf page, the btree only has one page in level 0 which is root page */
    std::queue<PageId> leafPageIds;
    leafPageIds.push(btrRootPageId);
    TestLeafTuples(m_utTableHandler->GetIndexTupleDesc(), leafPageIds, rowNum);
}

TEST_F(UTBtree, BtreeCreateTwoLevelTest_level0)
{
    int rowNum = 2000;
    m_utTableHandler->FillTableWithRandomData(rowNum);

    int indexCols[] = {1, 2};
    bool isUnique = false;
    int numAttrs = sizeof(indexCols) / sizeof(int);
    m_utTableHandler->CreateIndex(indexCols, numAttrs, isUnique);

    /* Test btree meta page info */
    PageId btrRootPageId = TestBtreeMetaPage(m_utTableHandler->GetBtreeSmgr()->GetBtrMetaPageId());

    /* Test btree root page info */
    TestBtreeRootPage(btrRootPageId, 1);

    std::queue<PageId> rootPageId;
    rootPageId.push(btrRootPageId);
    /* Test tuples in root page */
    std::queue<PageId> leafPageIds = TestPivotTuples(m_utTableHandler->GetIndexTupleDesc(), rootPageId, 1);

    /* Test tuples in all leaf pages */
    TestLeafTuples(m_utTableHandler->GetIndexTupleDesc(), leafPageIds, rowNum);
}

TEST_F(UTBtree, BtreeCreateThreeLevelTest_level1)
{
    int rowNum = 10000;
    m_utTableHandler->FillTableWithRandomData(rowNum);

    int indexCols[] = {1, 2, 3, 4};
    bool isUnique = false;
    int numAttrs = sizeof(indexCols) / sizeof(int);
    m_utTableHandler->CreateIndex(indexCols, numAttrs, isUnique, BTREE_MIN_FILLFACTOR);

    TupleDesc indexTupleDesc = m_utTableHandler->GetIndexTupleDesc();

    /* Test btree meta page info */
    PageId btrRootPageId = TestBtreeMetaPage(m_utTableHandler->GetBtreeSmgr()->GetBtrMetaPageId());

    /* Test btree root page info */
    TestBtreeRootPage(btrRootPageId, 2);

    /* Test tuples in root page */
    std::queue<PageId> rootPageId;
    rootPageId.push(btrRootPageId);
    std::queue<PageId> pivotPageIds = TestPivotTuples(indexTupleDesc, rootPageId, 2);

    /* Test all pivot pages */
    std::queue<PageId> leafPageIds = TestPivotTuples(indexTupleDesc, pivotPageIds, 1);

    /* Test all leaf pages */
    TestLeafTuples(indexTupleDesc, leafPageIds, rowNum);
}

TEST_F(UTBtree, BtreeCreateNullKeyTest_level0)
{
    int indexCols[] = {1, 2};
    TupleDesc heapTupleDesc = m_utTableHandler->GetHeapTupDesc();

    /* Fill table with specific data with some null */
    int rowNum = 600;
    HeapTuple *heapTuple;
    bool *nullbitmap = DefaultNullBitMap;
    Datum *heapValues = (Datum*)DstorePalloc(heapTupleDesc->natts * sizeof(Datum));
    bool *heapIsnulls = (bool*)DstorePalloc(heapTupleDesc->natts * sizeof(bool));
    /* Insert (...1,null...)(...1,1...)(...null,1...)(...null,null...)
     * (...2,null...)(...2,2...)(...null,2...)(...null,null...)... into heap */
    for (int i = 1; i <= rowNum; ++i) {
        DefaultRowDef insertRow = m_utTableHandler->GetDefaultRowDef();
        insertRow.column_int16 = i;
        insertRow.column_int32 = i;
        /* Insert (i,null) */
        nullbitmap[indexCols[1]] = true;
        heapTuple = m_utTableHandler->GetSpecificHeapTuple(&insertRow, heapValues, heapIsnulls, nullbitmap);
        m_utTableHandler->InsertHeapTupAndCheckResult(heapTuple);
        DstorePfreeExt(heapTuple);
        nullbitmap[indexCols[1]] = false;
        /* Insert (i,i) */
        heapTuple = m_utTableHandler->GetSpecificHeapTuple(&insertRow, heapValues, heapIsnulls, nullbitmap);
        m_utTableHandler->InsertHeapTupAndCheckResult(heapTuple);
        DstorePfreeExt(heapTuple);
        /* Insert (null,i) */
        nullbitmap[indexCols[0]] = true;
        heapTuple = m_utTableHandler->GetSpecificHeapTuple(&insertRow, heapValues, heapIsnulls, nullbitmap);
        m_utTableHandler->InsertHeapTupAndCheckResult(heapTuple);
        DstorePfreeExt(heapTuple);
        nullbitmap[indexCols[0]] = false;
        /* Insert (null,null) */
        nullbitmap[indexCols[0]] = true;
        nullbitmap[indexCols[1]] = true;
        heapTuple = m_utTableHandler->GetSpecificHeapTuple(&insertRow, heapValues, heapIsnulls, nullbitmap);
        m_utTableHandler->InsertHeapTupAndCheckResult(heapTuple);
        DstorePfreeExt(heapTuple);
        nullbitmap[indexCols[0]] = false;
        nullbitmap[indexCols[1]] = false;
    }
    DstorePfree(heapValues);
    DstorePfree(heapIsnulls);

    /* Build btree index */
    bool isUnique = false;
    int numAttrs = sizeof(indexCols) / sizeof(int);
    m_utTableHandler->CreateIndex(indexCols, numAttrs, isUnique);
    TupleDesc indexTupleDesc = m_utTableHandler->GetIndexTupleDesc();

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

TEST_F(UTBtree, BtreeCreateGlobalIndexTest_level0)
{
    /* Destroy the default table handler */
    UTTableHandler::Destroy(m_utTableHandler);
    m_utTableHandler = nullptr;
    /* Create partitioned table handler */
    int numPartitions = 5;
    m_utTableHandler =
        UTTableHandler::CreatePartitionTableHandler(g_defaultPdbId, g_dstoreCurrentMemoryContext, numPartitions);

    /* Fill random data into each partition */
    int numInsertedTuple = 0;
    for (int i = 0; i < numPartitions; i++) {
        m_utTableHandler->FillTableWithRandomData(i, i * 64);
        numInsertedTuple += i * 64;
    }

    /* Generate global index meta */
    int indexCols[] = {1, 2};
    bool isUnique = false;
    int numAttrs = sizeof(indexCols) / sizeof(int);
    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetSnapshotCsn();
    m_utTableHandler->CreateBtreeContext(indexCols, numAttrs, isUnique);

    /* Build index in storage */
    BtreeBuild btreeBuild(m_utTableHandler->GetIndexRel(),
                          m_utTableHandler->GetIndexBuildInfo(), m_utTableHandler->GetIndexScanKey());

    RetStatus ret = btreeBuild.BuildIndex();
    txn->Commit();

    /* Check */
    EXPECT_EQ(ret, DSTORE_SUCC);
    int numAllHeapTuples = 0;
    for (int i = 0; i < numPartitions; i++) {

        int numPartitionedHeapTuple = (int)m_utTableHandler->GetIndexBuildInfo()->allPartTuples[i];
        numAllHeapTuples += numPartitionedHeapTuple;
        EXPECT_EQ(numPartitionedHeapTuple, i * 64);
    }
    EXPECT_EQ(numAllHeapTuples, numInsertedTuple);
    EXPECT_EQ(m_utTableHandler->GetIndexBuildInfo()->heapTuples, m_utTableHandler->GetIndexBuildInfo()->indexTuples);
}