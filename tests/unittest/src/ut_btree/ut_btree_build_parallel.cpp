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


TEST_F(UTBtree, BtreeParallelCreateOneLevelTest_level0)
{
    int rowNum = 100;
    m_utTableHandler->FillTableWithRandomData(rowNum);
    int indexCols[] = {1, 2};
    bool isUnique = false;
    int numAttrs = sizeof(indexCols) / sizeof(int);
    m_utTableHandler->CreateIndexParallel(indexCols, numAttrs, isUnique);

    /* Test btree meta page info */
    PageId btrRootPageId = TestBtreeMetaPage(m_utTableHandler->GetBtreeSmgr()->GetBtrMetaPageId());

    /* Test btree root page info */
    TestBtreeRootPage(btrRootPageId, 0);

    /* Test btree leaf page, the btree only has one page in level 0 which is root page */
    std::queue<PageId> leafPageIds;
    leafPageIds.push(btrRootPageId);
    TestLeafTuples(m_utTableHandler->GetIndexTupleDesc(), leafPageIds, rowNum);
}

TEST_F(UTBtree, BtreeParallelCreateOneLevelTestWithLpi_level0)
{   
    int numPartitions = 5;
    int parallelWorkers = 5;
    /* Destroy the default table handler */
    UTTableHandler::Destroy(m_utTableHandler);
    m_utTableHandler = nullptr;
    /* Create partitioned table handler */
    m_utTableHandler =
        UTTableHandler::CreatePartitionTableHandler(g_defaultPdbId, g_dstoreCurrentMemoryContext, numPartitions);
    /* Partition table handlers */
    UTTableHandler* tableHandlers[5] = {nullptr};
    int numInsertedTuple = 0;
    /* Generate global index meta */
    int indexCols[] = {1, 2};
    bool isUnique = false;
    int numAttrs = sizeof(indexCols) / sizeof(int);
 
    for (int i = 0; i < numPartitions; i++) {
        /* Fill random data into each partition */
        m_utTableHandler->FillTableWithRandomData(i, i * 64);
        numInsertedTuple += i * 64;
        /* Generate partition table handlers and their indexes*/
        tableHandlers[i] = UTTableHandler::CreateTableHandler(g_defaultPdbId, g_dstoreCurrentMemoryContext, false);
        tableHandlers[i]->CreateIndex(indexCols, numAttrs, isUnique);
        Transaction *txn = thrd->GetActiveTransaction();
    }
    
    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetSnapshotCsn();
    m_utTableHandler->CreateBtreeContext(indexCols, numAttrs, isUnique);
 
    /* Enable lpi cross-partition build*/
    m_utTableHandler->GetIndexBuildInfo()->lpiParallelMethodIsPartition = true;
    m_utTableHandler->GetIndexBuildInfo()->baseInfo.relKind = SYS_RELKIND_INDEX;
 
    /* Build index in storage */
    BtreeBuild btreeBuild(m_utTableHandler->GetIndexRel(),
                          m_utTableHandler->GetIndexBuildInfo(), m_utTableHandler->GetIndexScanKey());
 
    btreeBuild.m_indexBuildInfo->indexRels = 
        static_cast<StorageRelationData **>(DstorePalloc0(sizeof(StorageRelationData *) * numPartitions));
 
    for (int i = 0; i < numPartitions; ++i) {
        btreeBuild.m_indexBuildInfo->indexRels[i] = tableHandlers[i]->m_btreeTestContext->indexRel;
    }
 
    RetStatus ret = btreeBuild.BuildIndexParallel(parallelWorkers);
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

TEST_F(UTBtree, BtreeParallelCreateTwoLevelTest_level0)
{
    int rowNum = 2000;
    m_utTableHandler->FillTableWithRandomData(rowNum);

    int indexCols[] = {1, 2};
    bool isUnique = false;
    int numAttrs = sizeof(indexCols) / sizeof(int);
    m_utTableHandler->CreateIndexParallel(indexCols, numAttrs, isUnique);

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

/**
 * Disable for now since it's possible to run overtime in pipeline
*/
TEST_F(UTBtree, DISABLED_BtreeParallelCreateThreeLevelTest)
{
    int rowNum = 50000;
    m_utTableHandler->FillTableWithRandomData(rowNum);

    int indexCols[] = {1, 2, 3, 4};
    bool isUnique = false;
    int numAttrs = sizeof(indexCols) / sizeof(int);
    m_utTableHandler->CreateIndexParallel(indexCols, numAttrs, isUnique);

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

TEST_F(UTBtree, BtreeParallelCreateNullKeyTest_level0)
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
    m_utTableHandler->CreateIndexParallel(indexCols, numAttrs, isUnique);
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

TEST_F(UTBtree, BtreeParallelCreateLocalIndexTest_level0)
{
    int numPartitions = 5;
    UTTableHandler* tableHandlers[5] = {nullptr};
    int numPartitionTuples[5] = {0};

    for (int i = 0; i < numPartitions; i++) {
        /* Create partitioned table handler */
        tableHandlers[i] =
            UTTableHandler::CreateTableHandler(g_defaultPdbId, g_dstoreCurrentMemoryContext, false);

        /* Fill random data into each partition */
        tableHandlers[i]->FillTableWithRandomData(i * 64);
        numPartitionTuples[i] = i * 64;
    }

    /* Generate local index meta */
    int indexCols[] = {1, 2};
    bool isUnique = false;
    int numAttrs = sizeof(indexCols) / sizeof(int);

    for (int i = 0; i < numPartitions; i++) {
        /* Start transaction. */
        Transaction *txn = thrd->GetActiveTransaction();
        txn->Start();
        txn->SetSnapshotCsn();

        /* Create partitioned index for one partition */
        tableHandlers[i]->CreateBtreeContext(indexCols, numAttrs, isUnique);

        /* Build index in storage */
        BtreeBuild btreeBuild(tableHandlers[i]->GetIndexRel(),
                            tableHandlers[i]->GetIndexBuildInfo(), tableHandlers[i]->GetIndexScanKey());
        RetStatus ret = btreeBuild.BuildIndexParallel(4);

        /* Check */
        EXPECT_EQ(ret, DSTORE_SUCC);
        int numPartitionedHeapTuple = (int)tableHandlers[i]->GetIndexBuildInfo()->heapTuples;
        int numPartitionedIndexTuple = (int)tableHandlers[i]->GetIndexBuildInfo()->indexTuples;
        EXPECT_EQ(numPartitionedHeapTuple, numPartitionedIndexTuple);
        EXPECT_EQ(numPartitionedHeapTuple, i * 64);
        EXPECT_EQ(numPartitionedHeapTuple, numPartitionTuples[i]);

        /* Commit transaction. */
        txn->Commit();
    }

    /* Destroy the tableHandlers */
    for (int i = 0; i < numPartitions; i++) {
        UTTableHandler::Destroy(tableHandlers[i]);
    }
}

TEST_F(UTBtree, BtreeParallelCreateGlobalIndexTest_level0)
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
    RetStatus ret = btreeBuild.BuildIndexParallel(4);
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

/* Call BuildIndexParallel with zero worker should return DSTORE_FAIL
*/
TEST_F(UTBtree, BtreeParallelZeroWorkerFail_level0)
{
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
    ASSERT_EQ(btreeBuild.BuildIndexParallel(0), DSTORE_FAIL);
    txn->Commit();
}

TEST_F(UTBtree, BtreeParallelNotEnoughMemoryFail_level0)
{
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

    g_storageInstance->GetGuc()->maintenanceWorkMem = 63;

    ASSERT_EQ(btreeBuild.BuildIndexParallel(4), DSTORE_FAIL);
    txn->Commit();
}

TEST_F(UTBtree, BtreeParallelUniqueSucc_level0)
{
    /* Destroy the default table handler */
    UTTableHandler::Destroy(m_utTableHandler);
    m_utTableHandler = nullptr;

    /* Create a new table handler */
    m_utTableHandler =
            UTTableHandler::CreateTableHandler(g_defaultPdbId, g_dstoreCurrentMemoryContext, false);

    /* Fill table with specific data */
    int rowNum = 200;
    DefaultRowDef rowDef = m_utTableHandler->GetDefaultRowDef();
    DefaultRowDef *tableDef = (DefaultRowDef*)DstorePalloc(rowNum * sizeof(DefaultRowDef));
    for (int i = 1; i <= rowNum; ++i) {
        rowDef.column_int16 = (int16)(i);
        tableDef[i - 1] = rowDef;
    }
    m_utTableHandler->FillTableWithSpecificData(tableDef, rowNum);
    DstorePfree(tableDef);

    /* Build unique index. */
    int indexCols[] = {1};
    bool isUnique = true;
    int numAttrs = sizeof(indexCols) / sizeof(int);

    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetSnapshotCsn();
    m_utTableHandler->CreateBtreeContext(indexCols, numAttrs, isUnique);

    /* Build index in storage */
    BtreeBuild btreeBuild(m_utTableHandler->GetIndexRel(),
                          m_utTableHandler->GetIndexBuildInfo(), m_utTableHandler->GetIndexScanKey());
    ASSERT_EQ(btreeBuild.BuildIndexParallel(4), DSTORE_SUCC);
    int numHeapTuple = m_utTableHandler->GetIndexBuildInfo()->heapTuples;
    int numIndexTuple = m_utTableHandler->GetIndexBuildInfo()->indexTuples;
    IndexTuple *duplicateTuple = m_utTableHandler->GetIndexBuildInfo()->duplicateTuple;
    EXPECT_EQ(numHeapTuple, numIndexTuple);
    EXPECT_EQ(numHeapTuple, 200);
    EXPECT_EQ(duplicateTuple, nullptr);

    txn->Commit();
}

TEST_F(UTBtree, BtreeParallelUniqueFail_level0)
{
    /* Destroy the default table handler */
    UTTableHandler::Destroy(m_utTableHandler);
    m_utTableHandler = nullptr;

    /* Create a new table handler */
    m_utTableHandler =
            UTTableHandler::CreateTableHandler(g_defaultPdbId, g_dstoreCurrentMemoryContext, false);

    /* Fill table with specific data */
    int rowNum = 201;
    int repeatNumber = 55;
    DefaultRowDef rowDef = m_utTableHandler->GetDefaultRowDef();
    DefaultRowDef *tableDef = (DefaultRowDef*)DstorePalloc(rowNum * sizeof(DefaultRowDef));
    for (int i = 1; i <= 100; ++i) {
        rowDef.column_int16 = (int16)(i);
        tableDef[i - 1] = rowDef;
    }
    rowDef.column_int16 = (int16)(repeatNumber);
    tableDef[100] = rowDef;
    for (int i = 101; i <= 200; ++i) {
        rowDef.column_int16 = (int16)(i);
        tableDef[i] = rowDef;
    }

    m_utTableHandler->FillTableWithSpecificData(tableDef, rowNum);
    DstorePfree(tableDef);

    /* Build unique index. */
    int indexCols[] = {1};
    bool isUnique = true;
    int numAttrs = sizeof(indexCols) / sizeof(int);

    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetSnapshotCsn();
    m_utTableHandler->CreateBtreeContext(indexCols, numAttrs, isUnique);

    /* Build index in storage */
    BtreeBuild btreeBuild(m_utTableHandler->GetIndexRel(),
                          m_utTableHandler->GetIndexBuildInfo(), m_utTableHandler->GetIndexScanKey());
    ASSERT_EQ(btreeBuild.BuildIndexParallel(4), DSTORE_FAIL);
    IndexTuple *duplicateTuple = m_utTableHandler->GetIndexBuildInfo()->duplicateTuple;
    EXPECT_NE(duplicateTuple, nullptr);
    txn->Commit();
}

TEST_F(UTBtree, BtreeParallelGlobalIndexUniqueSucc_level0)
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
    int onePartitionRowNum = 10;
    /*
     * Partition 1 (i == 0): insert 0, 1, 2, 3, 4, 5, 6, 7, 8, 9
     * Partition 2 (i == 1): insert 10, 11, 12, 13, 14, 15, 16, 17, 18, 19
     * Partition 3 (i == 2): insert 20, 21, 22, 23, 24, 25, 26, 27, 28, 29
     * Partition 4 (i == 3): insert 30, 31, 32, 33, 34, 35, 36, 37, 38, 39
     * Partition 5 (i == 4): insert 40, 41, 42, 43, 44, 45, 46, 47, 48, 49
     */
    for (int i = 0; i < numPartitions; i++) {
        DefaultRowDef rowDef = m_utTableHandler->GetDefaultRowDef();
        DefaultRowDef *tableDef = (DefaultRowDef*)DstorePalloc(onePartitionRowNum * sizeof(DefaultRowDef));
        for (int j = 0; j < onePartitionRowNum; ++j) {
            rowDef.column_int16 = (int16)(i * onePartitionRowNum + j);
            tableDef[j] = rowDef;
        }

        m_utTableHandler->FillPartitionTableWithSpecificData(i, tableDef, onePartitionRowNum);
        DstorePfree(tableDef);

        numInsertedTuple += onePartitionRowNum;
    }

    /* Generate global index meta */
    int indexCols[] = {1};
    bool isUnique = true;
    int numAttrs = sizeof(indexCols) / sizeof(int);

    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetSnapshotCsn();
    m_utTableHandler->CreateBtreeContext(indexCols, numAttrs, isUnique);

    /* Build index in storage */
    BtreeBuild btreeBuild(m_utTableHandler->GetIndexRel(),
                          m_utTableHandler->GetIndexBuildInfo(), m_utTableHandler->GetIndexScanKey());
    RetStatus ret = btreeBuild.BuildIndexParallel(4);
    txn->Commit();

    /* Check */
    EXPECT_EQ(ret, DSTORE_SUCC);
    int numAllHeapTuples = 0;
    for (int i = 0; i < numPartitions; i++) {
        int numPartitionedHeapTuple = (int)m_utTableHandler->GetIndexBuildInfo()->allPartTuples[i];
        numAllHeapTuples += numPartitionedHeapTuple;
        EXPECT_EQ(numPartitionedHeapTuple, onePartitionRowNum);
    }
    EXPECT_EQ(numAllHeapTuples, numInsertedTuple);
    EXPECT_EQ(m_utTableHandler->GetIndexBuildInfo()->heapTuples, m_utTableHandler->GetIndexBuildInfo()->indexTuples);
    IndexTuple *duplicateTuple = m_utTableHandler->GetIndexBuildInfo()->duplicateTuple;
    EXPECT_EQ(duplicateTuple, nullptr);
}

TEST_F(UTBtree, BtreeParallelGlobalIndexUniqueFail_level0)
{
    /* Destroy the default table handler */
    UTTableHandler::Destroy(m_utTableHandler);
    m_utTableHandler = nullptr;
    /* Create partitioned table handler */
    int numPartitions = 5;
    m_utTableHandler =
        UTTableHandler::CreatePartitionTableHandler(g_defaultPdbId, g_dstoreCurrentMemoryContext, numPartitions);

    /* Fill random data into each partition */
    int onePartitionRowNum = 10;
    /*
     * Partition 1 (i == 0): insert 0, 1, 2, 3, 4, 5, 6, 7, 8, 9
     * Partition 2 (i == 1): insert 10, 11, 12, 13, 14, 15, 16, 17, 18, 19
     * Partition 3 (i == 2): insert 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30
     * Partition 4 (i == 3): insert 30, 31, 32, 33, 34, 35, 36, 37, 38, 39
     * Partition 5 (i == 4): insert 40, 41, 42, 43, 44, 45, 46, 47, 48, 49
     */
    for (int i = 0; i < numPartitions; i++) {
        DefaultRowDef rowDef = m_utTableHandler->GetDefaultRowDef();
        int currentPartitionRowNum = i == 2 ? onePartitionRowNum + 1 : onePartitionRowNum;
        DefaultRowDef *tableDef = (DefaultRowDef*)DstorePalloc(currentPartitionRowNum * sizeof(DefaultRowDef));
        for (int j = 0; j < currentPartitionRowNum; ++j) {
            rowDef.column_int16 = (int16)(i * onePartitionRowNum + j);
            tableDef[j] = rowDef;
        }

        m_utTableHandler->FillPartitionTableWithSpecificData(i, tableDef, currentPartitionRowNum);
        DstorePfree(tableDef);
    }

    /* Generate global index meta */
    int indexCols[] = {1};
    bool isUnique = true;
    int numAttrs = sizeof(indexCols) / sizeof(int);

    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetSnapshotCsn();
    m_utTableHandler->CreateBtreeContext(indexCols, numAttrs, isUnique);

    /* Build index in storage */
    BtreeBuild btreeBuild(m_utTableHandler->GetIndexRel(),
                          m_utTableHandler->GetIndexBuildInfo(), m_utTableHandler->GetIndexScanKey());
    RetStatus ret = btreeBuild.BuildIndexParallel(4);
    txn->Commit();

    /* Check */
    EXPECT_EQ(ret, DSTORE_FAIL);
    IndexTuple *duplicateTuple = m_utTableHandler->GetIndexBuildInfo()->duplicateTuple;
    EXPECT_NE(duplicateTuple, nullptr);
}

TEST_F(UTBtree, BtreeParallelLocalIndexUniqueSucc_level0)
{
    int numPartitions = 5;
    UTTableHandler* tableHandlers[5] = {nullptr};
    int numPartitionTuples[5] = {0};
    int rowNum = 100;

    for (int i = 0; i < numPartitions; i++) {
        /* Create partitioned table handler */
        tableHandlers[i] =
            UTTableHandler::CreateTableHandler(g_defaultPdbId, g_dstoreCurrentMemoryContext, false);

        /* Fill specific data into each partition */
        DefaultRowDef rowDef = m_utTableHandler->GetDefaultRowDef();
        DefaultRowDef *tableDef = (DefaultRowDef*)DstorePalloc(rowNum * sizeof(DefaultRowDef));
        for (int i = 0; i < rowNum; ++i) {
            rowDef.column_int16 = (int16)(rowNum * i + i);
            tableDef[i] = rowDef;
        }
        tableHandlers[i]->FillTableWithSpecificData(tableDef, rowNum);
        DstorePfree(tableDef);
        numPartitionTuples[i] = rowNum;
    }

    /* Generate local index meta */
    int indexCols[] = {1};
    bool isUnique = true;
    int numAttrs = sizeof(indexCols) / sizeof(int);

    for (int i = 0; i < numPartitions; i++) {
        /* Start transaction. */
        Transaction *txn = thrd->GetActiveTransaction();
        txn->Start();
        txn->SetSnapshotCsn();

        /* Create partitioned index for one partition */
        tableHandlers[i]->CreateBtreeContext(indexCols, numAttrs, isUnique);

        /* Build index in storage */
        BtreeBuild btreeBuild(tableHandlers[i]->GetIndexRel(),
                            tableHandlers[i]->GetIndexBuildInfo(), tableHandlers[i]->GetIndexScanKey());
        RetStatus ret = btreeBuild.BuildIndexParallel(4);

        /* Check */
        EXPECT_EQ(ret, DSTORE_SUCC);
        int numPartitionedHeapTuple = (int)tableHandlers[i]->GetIndexBuildInfo()->heapTuples;
        int numPartitionedIndexTuple = (int)tableHandlers[i]->GetIndexBuildInfo()->indexTuples;
        EXPECT_EQ(numPartitionedHeapTuple, numPartitionedIndexTuple);
        EXPECT_EQ(numPartitionedHeapTuple, rowNum);
        EXPECT_EQ(numPartitionedHeapTuple, numPartitionTuples[i]);

        /* Commit transaction. */
        txn->Commit();
    }

    /* Destroy the tableHandlers */
    for (int i = 0; i < numPartitions; i++) {
        UTTableHandler::Destroy(tableHandlers[i]);
    }
}

TEST_F(UTBtree, BtreeParallelLocalIndexUniqueFail_level0)
{
    int numPartitions = 5;
    UTTableHandler* tableHandlers[5] = {nullptr};
    int numPartitionTuples[5] = {0};
    int rowNum = 100;
    int repeatPartitionIdx = 4;

    for (int i = 0; i < numPartitions; i++) {
        /* Create partitioned table handler */
        tableHandlers[i] =
            UTTableHandler::CreateTableHandler(g_defaultPdbId, g_dstoreCurrentMemoryContext, false);

        /* Fill specific data into each partition */
        DefaultRowDef rowDef = tableHandlers[i]->GetDefaultRowDef();
        if (i != repeatPartitionIdx) {
            DefaultRowDef *tableDef = (DefaultRowDef*)DstorePalloc(rowNum * sizeof(DefaultRowDef));
            for (int j = 0; j < rowNum; ++j) {
                rowDef.column_int16 = (int16)(rowNum * i + j);
                tableDef[j] = rowDef;
            }
            tableHandlers[i]->FillTableWithSpecificData(tableDef, rowNum);
            DstorePfree(tableDef);
            numPartitionTuples[i] = rowNum;
        } else {
            DefaultRowDef *tableDef = (DefaultRowDef*)DstorePalloc((rowNum + 1) * sizeof(DefaultRowDef));
            for (int j = 0; j < rowNum; ++j) {
                rowDef.column_int16 = (int16)(rowNum * i + j);
                tableDef[j] = rowDef;
            }
            rowDef.column_int16 = (int16)(rowNum * i + rowNum - 1);
            tableDef[rowNum] = rowDef;
            tableHandlers[i]->FillTableWithSpecificData(tableDef, rowNum + 1);
            DstorePfree(tableDef);
            numPartitionTuples[i] = rowNum + 1;
        }
    }

    /* Generate local index meta */
    int indexCols[] = {1};
    bool isUnique = true;
    int numAttrs = sizeof(indexCols) / sizeof(int);

    for (int i = 0; i < numPartitions; i++) {
        /* Start transaction. */
        Transaction *txn = thrd->GetActiveTransaction();
        txn->Start();
        txn->SetSnapshotCsn();

        /* Create partitioned index for one partition */
        tableHandlers[i]->CreateBtreeContext(indexCols, numAttrs, isUnique);

        /* Build index in storage */
        BtreeBuild btreeBuild(tableHandlers[i]->GetIndexRel(),
                            tableHandlers[i]->GetIndexBuildInfo(), tableHandlers[i]->GetIndexScanKey());
        RetStatus ret = btreeBuild.BuildIndexParallel(4);

        /* Check */
        if (i != repeatPartitionIdx) {
            EXPECT_EQ(ret, DSTORE_SUCC);
            int numPartitionedHeapTuple = (int)tableHandlers[i]->GetIndexBuildInfo()->heapTuples;
            int numPartitionedIndexTuple = (int)tableHandlers[i]->GetIndexBuildInfo()->indexTuples;
            EXPECT_EQ(numPartitionedHeapTuple, numPartitionedIndexTuple);
            EXPECT_EQ(numPartitionedHeapTuple, rowNum);
            EXPECT_EQ(numPartitionedHeapTuple, numPartitionTuples[i]);
            IndexTuple *duplicateTuple = tableHandlers[i]->GetIndexBuildInfo()->duplicateTuple;
            EXPECT_EQ(duplicateTuple, nullptr);
        } else {
            EXPECT_EQ(ret, DSTORE_FAIL);
            IndexTuple *duplicateTuple = tableHandlers[i]->GetIndexBuildInfo()->duplicateTuple;
            EXPECT_NE(duplicateTuple, nullptr);
        }

        /* Commit transaction. */
        txn->Commit();
    }

    /* Destroy the tableHandlers */
    for (int i = 0; i < numPartitions; i++) {
        UTTableHandler::Destroy(tableHandlers[i]);
    }
}
