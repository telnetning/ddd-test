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
#include <algorithm>

#include "ut_btree/ut_btree.h"
#include "heap/dstore_heap_insert.h"

void PrepareIndexTupleForTuplesortMgrArray(TuplesortMgr* tuplesortMgr[], UTTableHandler *m_utTableHandler, uint16 numTuplesortMgr) {
    TupleDesc tupleDesc = m_utTableHandler->GetIndexTupleDesc();
    EXPECT_NE(tupleDesc, nullptr);

    Transaction *transaction = thrd->GetActiveTransaction();
    transaction->Start();

    uint16 keyNum = m_utTableHandler->GetIndexInfo()->indexKeyAttrsNum;
    Datum *values = (Datum*) DstorePalloc(keyNum * sizeof (Datum));
    bool *isnulls = (bool*) DstorePalloc(keyNum * sizeof (bool));
    transaction->SetSnapshotCsn();
    HeapTuple *heapTuple;

    HeapScanHandler heap_handler(g_storageInstance, thrd, m_utTableHandler->GetTableRel());
    heap_handler.Begin(transaction->GetSnapshot());

    EXPECT_NE(heap_handler.SeqScan(), nullptr);
    heapTuple = heap_handler.SeqScan();

    uint16 numIndexTuplesScanned = 0;
    while (heapTuple != nullptr) {
        IndexTuple *indexTuple = IndexTuple::FormTuple(heapTuple, m_utTableHandler->GetIndexBuildInfo(),
                                                       values, isnulls);
        tuplesortMgr[numIndexTuplesScanned % numTuplesortMgr]->PutIndexTuple(indexTuple);
        heapTuple = heap_handler.SeqScan();
        numIndexTuplesScanned++;
    }
    heap_handler.End();

    DstorePfree(isnulls);
    DstorePfree(values);

    transaction->Commit();
}

/* Check if index tuples are sorted in a TuplesortMgr. */
/* The number of tuples in the TuplesortMgr also needs to be the same as the number of tuples created. */
bool UTBtree::IsTupleSorted(TuplesortMgr *tuplesortMgr, uint16 expectedTuplesNum) {
    IndexTuple *itup = nullptr;
    IndexTuple *oldITup = nullptr;
    bool firstFlag = true;
    uint16 numTuples = 0;

    while (true) {
        EXPECT_EQ(tuplesortMgr->GetNextIndexTupleMainThread(&itup), DSTORE_SUCC);
        numTuples++;
        if (itup != nullptr) {
            if (firstFlag) {
                firstFlag = false;
            } else {
                int ret = IndexTuple::Compare(itup, oldITup, m_utTableHandler->GetIndexInfo());
                if (ret < 0) {
                    return false;
                }
            }
            oldITup = itup->Copy();
            EXPECT_EQ(tuplesortMgr->ReadNextTupleFromWorkers(), DSTORE_SUCC);
        } else {
            break;
        }
    }

    return numTuples == expectedTuplesNum;
}

/* TuplesortMgr's tuples are sorted after calling PerformSortTupleMainThread(). */
TEST_F(UTBtree, TuplesortMgrPerformSortTupleMainThread_level0)
{
    uint8 numTuples = 20;
    m_utTableHandler->FillTableWithRandomData(numTuples);

    /* Prepare index context for TupleDesc. */
    int indexCols[] = {CSTRING_IDX};
    int numKeyAttrs = sizeof(indexCols) / sizeof(int);
    Transaction *transaction = thrd->GetActiveTransaction();
    transaction->Start();
    transaction->GetSnapshot();
    m_utTableHandler->CreateBtreeContext(indexCols, numKeyAttrs, false);

    TupleDesc tupleDesc = m_utTableHandler->GetIndexTupleDesc();
    EXPECT_NE(tupleDesc, nullptr);

    /* Create a main thread TuplesortMgr and one worker thread TuplesortMgr. */
    char tmpFileNameBase[MAXPGPATH] = {0};
    int rc = sprintf_s(tmpFileNameBase, MAXPGPATH, "%s_%d_%d_", GetTestCaseName(), 1, 1);
    storage_securec_check_ss(rc);
    TuplesortMgr *tuplesortMgrMain = DstoreNew(g_dstoreCurrentMemoryContext) TuplesortMgr(
        tmpFileNameBase, g_defaultPdbId);
    EXPECT_EQ(tuplesortMgrMain->PrepareParallelSortMainThread(m_utTableHandler->GetIndexBuildInfo()->baseInfo, 64,
        m_utTableHandler->GetIndexScanKey(), 4), DSTORE_SUCC);

    TuplesortMgr *tuplesortMgrArray[1];

    DstoreMemoryContext cxt = DstoreAllocSetContextCreate(
            g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_QUERY), "Parallel btree build share context",
            ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE,
            MemoryContextType::SHARED_CONTEXT);

    char tmpFileNameBase0[MAXPGPATH] = {0};
    rc = sprintf_s(tmpFileNameBase0, MAXPGPATH, "%s_%d_%d_p%d_", GetTestCaseName(), 1, 1, 0);
    storage_securec_check_ss(rc);
    tuplesortMgrArray[0] = DstoreNew(g_dstoreCurrentMemoryContext) TuplesortMgr(tmpFileNameBase0, g_defaultPdbId);
    EXPECT_EQ(tuplesortMgrArray[0]->PrepareParallelSortWorker(m_utTableHandler->GetIndexBuildInfo()->baseInfo, 64,
        m_utTableHandler->GetIndexScanKey(), cxt), DSTORE_SUCC);

    PrepareIndexTupleForTuplesortMgrArray(tuplesortMgrArray, m_utTableHandler, 1);

    EXPECT_EQ(tuplesortMgrArray[0]->PerformSortTuple(), DSTORE_SUCC);

    EXPECT_EQ(tuplesortMgrMain->Aggregate(tuplesortMgrArray[0]), DSTORE_SUCC);

    EXPECT_FALSE(IsTupleSorted(tuplesortMgrMain, numTuples));
    EXPECT_EQ(tuplesortMgrMain->PerformSortTupleMainThread(), DSTORE_SUCC);
    EXPECT_TRUE(IsTupleSorted(tuplesortMgrMain, numTuples));

    tuplesortMgrMain->Destroy();
    delete tuplesortMgrMain;

    tuplesortMgrArray[0]->Destroy();
    delete tuplesortMgrArray[0];

    DstoreMemoryContextDelete(cxt);
    transaction->Commit();
}

/* A TuplesortMgr aggregates other TuplesortMgr to combine all tuples. */
TEST_F(UTBtree, TuplesortMgrAggregate_level0)
{
    uint16 numTuples = 4096;
    m_utTableHandler->FillTableWithRandomData(numTuples);

    /* Prepare index context for TupleDesc. */
    int indexCols[] = {CSTRING_IDX};
    int numKeyAttrs = sizeof(indexCols) / sizeof(int);
    Transaction *transaction = thrd->GetActiveTransaction();
    transaction->Start();
    transaction->GetSnapshot();
    m_utTableHandler->CreateBtreeContext(indexCols, numKeyAttrs, false);

    TupleDesc tupleDesc = m_utTableHandler->GetIndexTupleDesc();
    EXPECT_NE(tupleDesc, nullptr);

    uint8 numTuplesortMgr = 4;
    /* Create a main thread TuplesortMgr and four worker thread TuplesortMgr. */
    char tmpFileNameBase[MAXPGPATH] = {0};
    int rc = sprintf_s(tmpFileNameBase, MAXPGPATH, "%s_%d_%d_", GetTestCaseName(), 1, 1);
    storage_securec_check_ss(rc);
    TuplesortMgr *tuplesortMgrMain = DstoreNew(g_dstoreCurrentMemoryContext) TuplesortMgr(tmpFileNameBase, g_defaultPdbId);
    EXPECT_EQ(tuplesortMgrMain->PrepareParallelSortMainThread(m_utTableHandler->GetIndexBuildInfo()->baseInfo, 64,
        m_utTableHandler->GetIndexScanKey(), numTuplesortMgr), DSTORE_SUCC);

    TuplesortMgr *tuplesortMgrArray[numTuplesortMgr];
    DstoreMemoryContext cxt = DstoreAllocSetContextCreate(
            g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_QUERY), "Parallel btree build share context",
            ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE,
            MemoryContextType::SHARED_CONTEXT);

    for (uint8 i = 0; i < numTuplesortMgr; ++i) {
        char tmpFileNameBasei[MAXPGPATH] = {0};
        int rc = sprintf_s(tmpFileNameBasei, MAXPGPATH, "%s_%d_%d_p%d_", GetTestCaseName(), 1, 1, i);
        storage_securec_check_ss(rc);
        tuplesortMgrArray[i] = DstoreNew(g_dstoreCurrentMemoryContext) TuplesortMgr(tmpFileNameBasei, g_defaultPdbId);
        EXPECT_EQ(tuplesortMgrArray[i]->PrepareParallelSortWorker(m_utTableHandler->GetIndexBuildInfo()->baseInfo, 64,
            m_utTableHandler->GetIndexScanKey(), cxt), DSTORE_SUCC);
    }

    PrepareIndexTupleForTuplesortMgrArray(tuplesortMgrArray, m_utTableHandler, numTuplesortMgr);

    for (uint8 i = 0; i < numTuplesortMgr; ++i) {
        EXPECT_EQ(tuplesortMgrArray[i]->PerformSortTuple(), DSTORE_SUCC);
    }

    for (uint8 i = 0; i < numTuplesortMgr; ++i) {
        EXPECT_EQ(tuplesortMgrMain->Aggregate(tuplesortMgrArray[i]), DSTORE_SUCC);
    }

    EXPECT_EQ(tuplesortMgrMain->PerformSortTupleMainThread(), DSTORE_SUCC);

    EXPECT_TRUE(IsTupleSorted(tuplesortMgrMain, numTuples));

    tuplesortMgrMain->Destroy();
    delete tuplesortMgrMain;

    for (uint8 i = 0; i < numTuplesortMgr; ++i) {
        tuplesortMgrArray[i]->Destroy();
        delete tuplesortMgrArray[i];
    }

    DstoreMemoryContextDelete(cxt);
    transaction->Commit();
}

/* TuplesortMgr cannot get valid index tuple when
 *  1. no tuples are in TuplesortMgr.
 *  2. all TuplesortMgr's tuples are read.
 */
TEST_F(UTBtree, TuplesortMgrGetNextIndexTupleMainThread_level0)
{
    uint8 numTuples = 4;
    m_utTableHandler->FillTableWithRandomData(numTuples);

    /* Prepare index context for TupleDesc. */
    int indexCols[] = {CSTRING_IDX};
    int numKeyAttrs = sizeof(indexCols) / sizeof(int);
    Transaction *transaction = thrd->GetActiveTransaction();
    transaction->Start();
    transaction->GetSnapshot();
    m_utTableHandler->CreateBtreeContext(indexCols, numKeyAttrs, false);

    TupleDesc tupleDesc = m_utTableHandler->GetIndexTupleDesc();
    EXPECT_NE(tupleDesc, nullptr);

    /* Create a main thread TuplesortMgr without inserting any tuple. */
    char tmpFileNameBase[MAXPGPATH] = {0};
    int rc = sprintf_s(tmpFileNameBase, MAXPGPATH, "%s_%d_%d_", GetTestCaseName(), 1, 1);
    storage_securec_check_ss(rc);
    TuplesortMgr *tuplesortMgrMain = DstoreNew(g_dstoreCurrentMemoryContext) TuplesortMgr(tmpFileNameBase, g_defaultPdbId);
    EXPECT_EQ(tuplesortMgrMain->PrepareParallelSortMainThread(m_utTableHandler->GetIndexBuildInfo()->baseInfo, 64,
        m_utTableHandler->GetIndexScanKey(), numTuples), DSTORE_SUCC);

    IndexTuple *itup = nullptr;
    EXPECT_EQ(tuplesortMgrMain->GetNextIndexTupleMainThread(&itup), DSTORE_SUCC);
    EXPECT_TRUE(itup == NULL);

    TuplesortMgr *tuplesortMgrArray[1];

    DstoreMemoryContext cxt = DstoreAllocSetContextCreate(
            g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_QUERY), "Parallel btree build share context",
            ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE,
            MemoryContextType::SHARED_CONTEXT);

    char tmpFileNameBase0[MAXPGPATH] = {0};
    rc = sprintf_s(tmpFileNameBase0, MAXPGPATH, "%s_%d_%dp%d_", GetTestCaseName(), 1, 1, 0);
    storage_securec_check_ss(rc);
    tuplesortMgrArray[0] = DstoreNew(g_dstoreCurrentMemoryContext) TuplesortMgr(tmpFileNameBase0, g_defaultPdbId);
    EXPECT_EQ(tuplesortMgrArray[0]->PrepareParallelSortWorker(m_utTableHandler->GetIndexBuildInfo()->baseInfo, 64,
        m_utTableHandler->GetIndexScanKey(), cxt), DSTORE_SUCC);

    PrepareIndexTupleForTuplesortMgrArray(tuplesortMgrArray, m_utTableHandler, 1);

    EXPECT_EQ(tuplesortMgrArray[0]->PerformSortTuple(), DSTORE_SUCC);

    EXPECT_EQ(tuplesortMgrMain->Aggregate(tuplesortMgrArray[0]), DSTORE_SUCC);
    EXPECT_EQ(tuplesortMgrMain->PerformSortTupleMainThread(), DSTORE_SUCC);

    EXPECT_TRUE(IsTupleSorted(tuplesortMgrMain, numTuples));

    EXPECT_EQ(tuplesortMgrMain->GetNextIndexTupleMainThread(&itup), DSTORE_SUCC);
    EXPECT_TRUE(itup == NULL);

    tuplesortMgrMain->Destroy();
    delete tuplesortMgrMain;

    tuplesortMgrArray[0]->Destroy();
    delete tuplesortMgrArray[0];

    DstoreMemoryContextDelete(cxt);
    transaction->Commit();
}