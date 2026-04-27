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
#include "heap/dstore_heap_scan.h"
#include "ut_heap/ut_heap.h"

TEST_F(UTHeap, HeapScanKeyTest_level0)
{
    /* Step 1: prepare table with specific data */
    constexpr int ROW_NUM = 1000;
    constexpr int KEY_STEP = 10;
    DefaultRowDef *tableDef = static_cast<DefaultRowDef*>(DstorePalloc(ROW_NUM * sizeof(DefaultRowDef)));
    for (int i = 0; i < ROW_NUM; ++i) {
        int16 r1 = static_cast<int16>((i + 1) / KEY_STEP);
        int32 r2 = static_cast<int32>((i + 1) / KEY_STEP);
        DefaultRowDef rowDef = m_utTableHandler->GetDefaultRowDef();
        rowDef.column_int16 = r1;
        rowDef.column_int32 = r2;
        rowDef.column_text = static_cast<text*>(m_utTableHandler->GenerateSpecificDatumPtr(TEXTOID, (i + 1) % 26));
        tableDef[i] = rowDef;
    }
    m_utTableHandler->FillTableWithSpecificData(tableDef, ROW_NUM);

    /* Step 2: prepare scan key */
    constexpr int N_KEYS = 2;
    ScanKey scanKey = static_cast<ScanKey>(DstorePalloc(sizeof(ScanKeyData) * N_KEYS));
    InitScanKeyInt24(scanKey, KEY_STEP, SCAN_ORDER_EQUAL, KEY_STEP, SCAN_ORDER_EQUAL);

    /* Step 3: sequence scan with key */
    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetSnapshotCsn();
    std::unique_ptr<StorageRelationData> heapRel(new StorageRelationData());
    heapRel->tableSmgr = m_utTableHandler->GetHeapTabSmgr();
    heapRel->m_pdbId = g_defaultPdbId;
    HeapScanHandler *scan = DstoreNew(thrd->GetTransactionMemoryContext())
        HeapScanHandler(g_storageInstance, thrd, heapRel.get());
    scan->Begin(txn->GetSnapshot());
    scan->SetScanKey(m_utTableHandler->GetHeapTupDesc(), N_KEYS, scanKey);
    int foundNum = 0;
    while (scan->SeqScan() != nullptr) {
        foundNum++;
    }
    ASSERT_EQ(foundNum, KEY_STEP);

    /* Step 4: rescan */
    scan->ReScan();
    foundNum = 0;
    while (scan->SeqScan() != nullptr) {
        foundNum++;
    }
    ASSERT_EQ(foundNum, KEY_STEP);

    /* Step 5: sequence scan with unmatched key */
    scan->ReScan();
    InitScanKeyInt24(scanKey, KEY_STEP, SCAN_ORDER_EQUAL, KEY_STEP + 1, SCAN_ORDER_EQUAL);
    foundNum = 0;
    while (scan->SeqScan() != nullptr) {
        foundNum++;
    }
    ASSERT_EQ(foundNum, 0);
    scan->End();
    delete scan;
    txn->Commit();
}

TEST_F(UTHeap, HeapScanTest_level0)
{
    /*
     * prepare two pages, 7 tuples in page 1 and 4 tuples in page 2
     *     page 1: tuple 0 -- update and in progress
     *             tuple 1 -- frozen
     *             tuple 2 -- update and commit(invisible)
     *     page 2: tuple 2 -- insert and commit(invisible)
     *             tuple 3 -- current(visible) and update(invisible due to command Id)
     * other tuples are normally inserted and visible
     */

    Transaction *txn = thrd->GetActiveTransaction();
    std::vector<ItemPointerData> ctids;
    constexpr int TUPLE_DATA_LEN = 1024;
    std::string stra(TUPLE_DATA_LEN, 'a');
    constexpr int TUPLE_NUM_PER_PAGE = 7;

    /* prepare tuples, reserve 2 slots for inserting later */
    for (int i = 0; i < TUPLE_NUM_PER_PAGE + DEFAULT_TD_COUNT - 2; ++i) {
        ItemPointerData ctid = InsertSpecificHeapTuple(stra);
        ctids.push_back(ctid);
    }
    SnapshotData snapshot = ConstructCurSnapshot();
    std::string strb(TUPLE_DATA_LEN, 'b');
    HeapTuple *tup = m_utTableHandler->GenerateSpecificHeapTuple(strb);

    /* Case 1: xid is in progress */
    UpdateTupAndCheckResult(&ctids[0], tup);
    SetTupleXidStatus(ctids[0], TXN_STATUS_IN_PROGRESS);
    /* Case 2: xid is frozen */
    SetTupleXidStatus(ctids[1], TXN_STATUS_FROZEN);
    /* Case 3: xid is committed */
    UpdateTupAndCheckResult(&ctids[2], tup);
    ItemPointerData ctid = InsertSpecificHeapTuple(strb);
    ctids.push_back(ctid);

    txn->Start();
    txn->SetSnapshotCsnForFlashback(snapshot.GetCsn());

    /* Case 4: xid is current */
    ctid = InsertSpecificHeapTuple(stra, &snapshot, true);
    ctids.push_back(ctid);
    CommandId cid = txn->GetSnapshotCid();
    UpdateTupAndCheckResult(&ctid, tup, &snapshot, true);
    txn->SetSnapshotCid(cid);

    /* Check 1: SeqScan() */
    std::unique_ptr<StorageRelationData> heapRel(new StorageRelationData());
    heapRel->tableSmgr = m_utTableHandler->GetHeapTabSmgr();
    heapRel->m_pdbId = g_defaultPdbId;
    HeapScanHandler *scan = DstoreNew(thrd->GetTransactionMemoryContext())
        HeapScanHandler(g_storageInstance, thrd, heapRel.get());
    scan->Begin(txn->GetSnapshot());
    HeapTuple *heapTuple = nullptr;
    int tupleNums = 0;
    while ((heapTuple = scan->SeqScan()) != nullptr) {
        ASSERT_EQ(stra.length(), heapTuple->GetDiskTupleSize() - heapTuple->GetDiskTuple()->GetHeaderSize());
        ASSERT_EQ(strncmp(stra.c_str(), heapTuple->GetDiskTuple()->GetData(), stra.length()), 0);
        tupleNums++;
    }
    ASSERT_EQ(tupleNums, TUPLE_NUM_PER_PAGE + DEFAULT_TD_COUNT - 1);

    /* Check 2: FetchTuple() */
    for (int i = 0; i < ctids.size(); ++i) {
        if (i == ctids.size() - 2) {
            ASSERT_EQ(scan->FetchTuple(ctids[i]), nullptr);
            continue;
        }
        heapTuple = scan->FetchTuple(ctids[i]);
        ASSERT_NE(heapTuple, nullptr);
        ASSERT_EQ(strncmp(stra.c_str(), heapTuple->GetDiskTuple()->GetData(), stra.length()), 0);
        DstorePfreeExt(heapTuple);
    }

    scan->End();
    delete scan;
    txn->Commit();
    DstorePfreeExt(tup);
}

TEST_F(UTHeap, HeapSampleScanTest_level0)
{
    Transaction *txn = thrd->GetActiveTransaction();
    std::vector<ItemPointerData> ctids;
    constexpr int TUPLE_DATA_LEN = 1024;
    std::string stra(TUPLE_DATA_LEN, 'a');
    constexpr int TUPLE_NUM_PER_PAGE = 7;

    /* Step 1: prepare tuples */
    for (int i = 0; i < TUPLE_NUM_PER_PAGE - 1; ++i) {
        ItemPointerData ctid = InsertSpecificHeapTuple(stra);
        ctids.push_back(ctid);
    }

    /* delete tuple 0 */
    DeleteTuple(&ctids[0]);

    /* same-page-append update tuple 1 */
    constexpr int SAME_PAGE_APPEND_UPDATE_TUP_DATA_LEN = TUPLE_DATA_LEN + 10;
    std::string strb(SAME_PAGE_APPEND_UPDATE_TUP_DATA_LEN, 'b');
    HeapTuple *tupleb = m_utTableHandler->GenerateSpecificHeapTuple(strb);
    UpdateTupAndCheckResult(&ctids[1], tupleb);
    DstorePfreeExt(tupleb);

    /* another-page update tuple 2 */
    constexpr int ANOTHER_PAGE_UPDATE_TUP_DATA_LEN = TUPLE_DATA_LEN * 4;
    std::string strc(ANOTHER_PAGE_UPDATE_TUP_DATA_LEN, 'c');
    HeapTuple *tuplec = m_utTableHandler->GenerateSpecificHeapTuple(strc);
    UpdateTupAndCheckResult(&ctids[2], tuplec);
    DstorePfreeExt(tuplec);

    /* inplace update tuple 3 -- committed */
    std::string strd(TUPLE_DATA_LEN, 'd');
    HeapTuple *tupled = m_utTableHandler->GenerateSpecificHeapTuple(strd);
    UpdateTupAndCheckResult(&ctids[3], tupled);
    DstorePfreeExt(tupled);

    /* inplace update tuple 4 -- in progress */
    txn->Start();
    std::string stre(TUPLE_DATA_LEN, 'e');
    HeapTuple *tuplee = m_utTableHandler->GenerateSpecificHeapTuple(stre);
    UpdateTupAndCheckResult(&ctids[4], tuplee, INVALID_SNAPSHOT, true);
    DstorePfreeExt(tuplee);

    /* Step 2: Sample scan */
    txn->Start();
    txn->SetSnapshotCsn();
    std::unique_ptr<StorageRelationData> heapRel(new StorageRelationData());
    heapRel->tableSmgr = m_utTableHandler->GetHeapTabSmgr();
    heapRel->m_pdbId = g_defaultPdbId;
    HeapScanHandler *scan = DstoreNew(thrd->GetTransactionMemoryContext())
        HeapScanHandler(g_storageInstance, thrd, heapRel.get());
    scan->Begin(txn->GetSnapshot());
    HeapSampleScanContext context;
    int totalLiveTups = 0, totalDeadTups = 0, totalTups = 0;
    uint64 pageCnt = m_utTableHandler->GetHeapTabSmgr()->GetTableBlockCount();
    RetStatus status;
    for (int i = 0; i < pageCnt; ++i) {
        context.SetSampleBlockNum(i);
        status = scan->SampleScan(&context);
        totalLiveTups += context.numLiveTuples;
        totalDeadTups += context.numDeadTuples;
        totalTups += context.numTuples;
        ASSERT_EQ(status, DSTORE_SUCC);
    }
    ASSERT_EQ(totalLiveTups, 5);
    ASSERT_EQ(totalDeadTups, 2);
    ASSERT_EQ(totalTups, 5);

    scan->End();
    delete scan;
    txn->Commit();
}

TEST_F(UTHeap, HeapScanBigTupleTest_level0)
{
    HeapTuple *resTuple;
    Transaction *transaction = thrd->GetActiveTransaction();
    std::unique_ptr<StorageRelationData> heapRel(new StorageRelationData());
    heapRel->tableSmgr = m_utTableHandler->GetHeapTabSmgr();
    heapRel->m_pdbId = g_defaultPdbId;
    HeapScanHandler *heapScan = DstoreNew(thrd->GetTransactionMemoryContext()) HeapScanHandler(
        g_storageInstance, thrd, heapRel.get());
    /* Construct big tup. */
    int BIG_TUP_LEN = 18000;
    std::string bigString(BIG_TUP_LEN, 'b');

    /* Case 1: insert and scan */
    transaction->Start();
    transaction->SetSnapshotCsn();
    HeapTuple *tup = m_utTableHandler->GenerateSpecificHeapTuple(bigString);
    m_utTableHandler->InsertHeapTupAndCheckResult(tup, true, transaction->GetSnapshotData());
    InsertSpecificHeapTuple("hello", transaction->GetSnapshotData(), true);
    int cnts[2] = {0};
    /* Construct once used cr page and read it */
    heapScan->Begin(transaction->GetSnapshot());
    while ((resTuple = heapScan->SeqScan()) != nullptr) {
        if (strncmp(resTuple->m_diskTuple->GetData(), bigString.c_str(), BIG_TUP_LEN) == 0) {
            cnts[0]++;
        } else if (strncmp(resTuple->m_diskTuple->GetData(), "hello", strlen("hello")) == 0) {
            cnts[1]++;
        }
    }
    EXPECT_EQ(cnts[0], 1);
    EXPECT_EQ(cnts[1], 1);
    heapScan->End();
    delete heapScan;
    transaction->Commit();
}

TEST_F(UTHeap, HeapScanBigTupleReadOnlyTest_TIER1_level0)
{
    HeapTuple *resTuple;
    Transaction *transaction = thrd->GetActiveTransaction();
    ItemPointerData ctid;

    /* Construct big tup. */
    int BIG_TUP_LEN = 18000;
    std::string bigString(BIG_TUP_LEN, 'b');

    /* Step 1: insert */
    transaction->Start();
    transaction->SetSnapshotCsn();
    HeapTuple *tup = m_utTableHandler->GenerateSpecificHeapTuple(bigString);
    ctid = m_utTableHandler->InsertHeapTupAndCheckResult(tup, true, transaction->GetSnapshotData());
    InsertSpecificHeapTuple("hello", transaction->GetSnapshotData(), true);
    int cnts[2] = {0};
    transaction->Commit();

    /* Step 2: read only transaction */
    transaction->Start();
    transaction->SetSnapshotCsn();
    /* FetchTuple() -- read base page using mvcc */
    std::unique_ptr<StorageRelationData> heapRel(new StorageRelationData());
    heapRel->tableSmgr = m_utTableHandler->GetHeapTabSmgr();
    heapRel->m_pdbId = g_defaultPdbId;
    HeapScanHandler *heapScan = DstoreNew(thrd->GetTransactionMemoryContext()) HeapScanHandler(
        g_storageInstance, thrd, heapRel.get());
    heapScan->Begin(transaction->GetSnapshot());
    resTuple = heapScan->FetchTuple(ctid);
    EXPECT_EQ(strncmp(resTuple->m_diskTuple->GetData(), bigString.c_str(), BIG_TUP_LEN), 0);
    DstorePfree(resTuple);
    /* SeqScan() -- construct cr page1 in cr pool and read it */
    while ((resTuple = heapScan->SeqScan()) != nullptr) {
        if (strncmp(resTuple->m_diskTuple->GetData(), bigString.c_str(), BIG_TUP_LEN) == 0) {
            cnts[0]++;
        } else if (strncmp(resTuple->m_diskTuple->GetData(), "hello", strlen("hello")) == 0) {
            cnts[1]++;
        }
    }
    EXPECT_EQ(cnts[0], 1);
    EXPECT_EQ(cnts[1], 1);
    heapScan->End();
    delete heapScan;
    transaction->Commit();
}

TEST_F(UTHeap, HeapScanAnyTupleTest_level0)
{
    std::vector<HeapTuple *> heapTupsForInsert;
    std::vector<HeapTuple *> heapTupsForUpdate1;
    std::vector<HeapTuple *> HeapTupsForUpdate2;

    /* Step1: prepare kinds of data */
    int i = 0;
    constexpr int NUM_TUP = 1;
    for (i = 0; i < NUM_TUP; ++i) {
        HeapTuple *insertTuple = m_utTableHandler->GenerateSpecificHeapTuple("insert_Data1");
        HeapTuple *updateTuple1 = m_utTableHandler->GenerateSpecificHeapTuple("updateData2");
        HeapTuple *updateTuple2 = m_utTableHandler->GenerateSpecificHeapTuple("update_Data3");
        heapTupsForInsert.push_back(insertTuple);
        heapTupsForUpdate1.push_back(updateTuple1);
        HeapTupsForUpdate2.push_back(updateTuple2);
    }
    MultiUpdateTest(heapTupsForInsert, heapTupsForUpdate1, HeapTupsForUpdate2);

    Transaction *transaction = thrd->GetActiveTransaction();
    transaction->Start();
    transaction->SetSnapshotCsn();
    std::unique_ptr<StorageRelationData> heapRel(new StorageRelationData());
    heapRel->tableSmgr = m_utTableHandler->GetHeapTabSmgr();
    heapRel->m_pdbId = g_defaultPdbId;
    
    HeapScanHandler *heapScan = DstoreNew(thrd->GetTransactionMemoryContext()) HeapScanHandler(
        g_storageInstance, thrd, heapRel.get());
    heapScan->Begin(transaction->GetSnapshot(), true);

    int tupleCount = 0;
    HeapTuple *resTuple;
    Xid deleteXid = INVALID_XID;
    while ((resTuple = heapScan->SeqScan()) != nullptr) {
        if (strncmp(resTuple->m_diskTuple->GetData(), "update_Data3", strlen("update_Data3")) == 0) {
        } else if (strncmp(resTuple->m_diskTuple->GetData(), "updateData2", strlen("updateData2")) == 0) {
        } else if (strncmp(resTuple->m_diskTuple->GetData(), "insert_Data1", strlen("insert_Data1")) == 0) {
        } else {
            ASSERT_EQ(0, 1);
        }
        tupleCount++;
        ASSERT_EQ(resTuple->GetDeleteXidForDebug(), deleteXid);
        deleteXid = resTuple->GetXid();
    }

    heapScan->End();
    delete heapScan;
    transaction->Commit();
    EXPECT_EQ (tupleCount, 3);
}