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

TEST_F(UTHeap, HeapInsertTest_level0)
{
    Transaction *txn = thrd->GetActiveTransaction();

    /* Step 1: Insert one tuple */
    txn->Start();
    txn->SetSnapshotCsn();
    HeapTuple *heapTuple = m_utTableHandler->GenerateRandomHeapTuple();
    int diskTupleLen = heapTuple->GetDiskTupleSize();
    ItemPointerData ctid = m_utTableHandler->InsertHeapTupAndCheckResult(heapTuple, true, txn->GetSnapshotData());
    Xid xid = txn->GetCurrentXid();
    txn->Commit();

    /* Step 2: Check TD */
    BufferDesc *bufferDesc = g_storageInstance->GetBufferMgr()->
        Read(g_defaultPdbId, ctid.GetPageId(), DSTORE::LW_SHARED);
    auto page = (HeapPage*)bufferDesc->GetPage();
    HeapDiskTuple *diskTuple = page->GetDiskTuple(ctid.GetOffset());
    TD *td = page->GetTd(diskTuple->GetTdId());
    ASSERT_EQ(td->GetXid(), xid);
    UndoRecord undoRec;
    TransactionMgr *transactionMgr = g_storageInstance->GetPdb(g_defaultPdbId)->GetTransactionMgr();
    if (STORAGE_FUNC_SUCC(transactionMgr->FetchUndoRecord(xid, &undoRec, td->GetUndoRecPtr()))){
        ASSERT_EQ(ctid, undoRec.GetCtid());
    }
    g_storageInstance->GetBufferMgr()->UnlockAndRelease(bufferDesc);

    /* Step 3: Check tuple */
    HeapTuple* tuple = m_utTableHandler->FetchHeapTuple(&ctid);
    ASSERT_EQ(memcmp((void*)tuple->GetDiskTuple(), (void*)heapTuple->GetDiskTuple(), diskTupleLen), 0);

    DstorePfreeExt(tuple);
    DstorePfreeExt(heapTuple);
}

TEST_F(UTHeap, HeapUndoInsertTest_level0)
{
    Transaction *txn = thrd->GetActiveTransaction();
    constexpr int TUPLE_DATA_LEN = 10;
    std::string stra(TUPLE_DATA_LEN, 'a');

    /* Step 1: save a snapshot for cr */
    SnapshotData snapshot = ConstructCurSnapshot();

    /* Step 2: insert one tuple */
    ItemPointerData ctid = InsertSpecificHeapTuple(stra);

    /* Step 3: undo and check */
    txn->Start();
    txn->SetSnapshotCsnForFlashback(snapshot.GetCsn());

    /* ---- cr page ---- */
    CheckCrPage(ctid);

    /* ---- cr tuple ---- */
    CheckCrTuple(ctid);

    txn->Commit();
}

TEST_F(UTHeap, HeapBigTupInsertTest_level0)
{
    /* Construct big tuple. */
    std::string bigString;
    int bigTupleLen = 18000;
    for (int i = 0; i < bigTupleLen; i++) {
        bigString.append("a");
    }

    /* Step 1: Insert one tuple */
    HeapTuple *heapTuple = m_utTableHandler->GenerateSpecificHeapTuple(bigString);
    int diskTupleLen = heapTuple->GetDiskTupleSize();
    ItemPointerData ctid = m_utTableHandler->InsertHeapTupAndCheckResult(heapTuple);

    /* Step 2: Fetch tuple */
    HeapTuple *tuple = m_utTableHandler->FetchHeapTuple(&ctid);

    /* Step 3: Check tuple */
    EXPECT_TRUE(tuple->GetDiskTupleSize() == diskTupleLen);
    EXPECT_EQ(memcmp((void*)tuple->GetDiskTuple()->GetData(), (void*)bigString.c_str(), bigTupleLen), 0);
    DstorePfreeExt(tuple);
    DstorePfreeExt(heapTuple);
}

TEST_F(UTHeap, HeapBatchInsertTest_TIER1_level0)
{
    /* Construct big tuple. */
    std::string bigString;
    int bigTupleLen = 18000;
    for (int i = 0; i < bigTupleLen; i++) {
        bigString.append("a");
    }
    const int STEP = 5;

    Transaction *txn = thrd->GetActiveTransaction();

    /* Step 1: Insert some tuples */
    txn->Start();
    txn->SetSnapshotCsn();
    uint16 nTuples = 60;
    HeapTuple *heapTuples[nTuples];
    int diskTupleLens[nTuples];
    for (int i = 0; i < nTuples; i++) {
        HeapTuple *heapTuple = nullptr;
        int diskTupleLen = 0;
        /* generate some big tuples */
        if (i % STEP == 0) {
            heapTuple = m_utTableHandler->GenerateSpecificHeapTuple(bigString);
            diskTupleLen = heapTuple->GetDiskTupleSize();
        } else {
            heapTuple = m_utTableHandler->GenerateRandomHeapTuple();
            diskTupleLen = heapTuple->GetDiskTupleSize();
        }
        heapTuples[i] = heapTuple;
        diskTupleLens[i] = diskTupleLen;
    }
    ItemPointerData ctids[nTuples];
    m_utTableHandler->BatchInsertHeapTupsAndCheckResult(heapTuples, nTuples, ctids, txn->GetSnapshotData(), true);
    Xid xid = txn->GetCurrentXid();
    txn->Commit();

    /* Step 2: Fecth the UndoRecord and check it */
    UndoRecord undoRec;
    UndoRecPtr undoRecPtr = INVALID_UNDO_RECORD_PTR;
    bool fetchUndoRprFromTd = true;
    TransactionMgr *transactionMgr = g_storageInstance->GetPdb(g_defaultPdbId)->GetTransactionMgr();
    for (int i = nTuples - 1; i >= 0 ; i--) {
        HeapTuple *heapTuple = heapTuples[i];
        ItemPointerData ctid = ctids[i];
        int diskTupleLen = diskTupleLens[i];

        if (i % STEP == 0) {
            /* Check big tuple */
            HeapTuple *tuple = m_utTableHandler->FetchHeapTuple(&ctid);
            EXPECT_TRUE(tuple->GetDiskTupleSize() == diskTupleLen);
            EXPECT_EQ(memcmp((void*)tuple->GetDiskTuple()->GetData(), (void*)bigString.c_str(), bigTupleLen), 0);
            DstorePfreeExt(tuple);
            continue;
        }

        /* Check small tuple */
        BufferDesc *bufferDesc = g_storageInstance->GetBufferMgr()->Read(
            g_defaultPdbId, ctid.GetPageId(), DSTORE::LW_SHARED);
        auto page = (HeapPage *)bufferDesc->GetPage();
        HeapDiskTuple *diskTuple = page->GetDiskTuple(ctid.GetOffset());
        TD *td = page->GetTd(diskTuple->GetTdId());
        ASSERT_EQ(td->GetXid(), xid);
        if (undoRecPtr == INVALID_UNDO_RECORD_PTR) {
            undoRecPtr = td->GetUndoRecPtr();
        }
        if (STORAGE_FUNC_SUCC(transactionMgr->FetchUndoRecord(xid, &undoRec, undoRecPtr))) {
            if (!undoRec.IsMatchedCtid(ctid)) {
                undoRecPtr = undoRec.GetTxnPreUndoPtr();
                if (STORAGE_FUNC_SUCC(transactionMgr->FetchUndoRecord(xid, &undoRec, undoRecPtr))) {
                    ASSERT_EQ(true, undoRec.IsMatchedCtid(ctid));
                }
            }
        }
        g_storageInstance->GetBufferMgr()->UnlockAndRelease(bufferDesc);

        /* Check tuple */
        HeapTuple *tuple = m_utTableHandler->FetchHeapTuple(&ctid);
        ASSERT_EQ(memcmp((void *)tuple->GetDiskTuple(), (void *)heapTuple->GetDiskTuple(), diskTupleLen), 0);
        DstorePfreeExt(tuple);
    }
}

TEST_F(UTHeap, HeapBatchInsertTestWithRanges_level0)
{
    Transaction *txn = thrd->GetActiveTransaction();

    /* Step 1: Insert 5 tuples */
    txn->Start();
    txn->SetSnapshotCsn();
    uint16 nTuples = 5;
    HeapTuple *heapTuples[nTuples];
    for (int i = 0; i < nTuples; i++) {
        /* generate some tuples */
        HeapTuple *heapTuple = m_utTableHandler->GenerateRandomHeapTuple();
        heapTuples[i] = heapTuple;
    }
    ItemPointerData ctids[nTuples];
    m_utTableHandler->BatchInsertHeapTupsAndCheckResult(heapTuples, nTuples, ctids, txn->GetSnapshotData(), true);
    Xid xid = txn->GetCurrentXid();
    txn->Commit();

    /* Step 2: insert one tuple */
    txn->Start();
    txn->SetSnapshotCsn();
    HeapTuple *heapTuple = m_utTableHandler->GenerateRandomHeapTuple();
    m_utTableHandler->InsertHeapTupAndCheckResult(heapTuple, true, txn->GetSnapshotData());
    txn->Commit();

    /* Step 3: undo batch insert */
    TransactionMgr *transactionMgr = g_storageInstance->GetPdb(g_defaultPdbId)->GetTransactionMgr();
    ItemPointerData ctid = ctids[0];

    /* fetch the UndoRecord */
    BufferDesc *bufferDesc =
        g_storageInstance->GetBufferMgr()->
            Read(g_defaultPdbId, ctid.GetPageId(), DSTORE::LW_EXCLUSIVE);
    auto page = (HeapPage *)bufferDesc->GetPage();
    HeapDiskTuple *diskTuple = page->GetDiskTuple(ctid.GetOffset());
    UndoRecord undoRec;
    UndoRecPtr undoRecPtr = page->GetTd(diskTuple->GetTdId())->GetUndoRecPtr();
    if (STORAGE_FUNC_SUCC(transactionMgr->FetchUndoRecord(xid, &undoRec, undoRecPtr))) {
        if (!undoRec.IsMatchedCtid(ctid)) {
            undoRecPtr = undoRec.GetTxnPreUndoPtr();
            if (STORAGE_FUNC_SUCC(transactionMgr->FetchUndoRecord(xid, &undoRec, undoRecPtr))) {
                ASSERT_EQ(true, undoRec.IsMatchedCtid(ctid));
            }
        }
    }
    page->UndoHeap(&undoRec);
    g_storageInstance->GetBufferMgr()->UnlockAndRelease(bufferDesc);

    /* Step 4: insert 10 tuples */
    txn->Start();
    txn->SetSnapshotCsn();

    uint16 newTuples = 10;
    HeapTuple *newHeapTuples[newTuples];
    int newDiskTupleLens[newTuples];
    for (int i = 0; i < newTuples; i++) {
        HeapTuple *heapTuple = m_utTableHandler->GenerateRandomHeapTuple();
        newHeapTuples[i] = heapTuple;
        newDiskTupleLens[i] = heapTuple->GetDiskTupleSize();
    }
    ItemPointerData newCtids[newTuples];
    m_utTableHandler->BatchInsertHeapTupsAndCheckResult(
        newHeapTuples, newTuples, newCtids, txn->GetSnapshotData(), true);
    xid = txn->GetCurrentXid();
    txn->Commit();

    UndoRecord newUndoRec;
    UndoRecPtr newUndoRecPtr = INVALID_UNDO_RECORD_PTR;
    bool fetchUndoRprFromTd = true;
    for (int i = newTuples - 1; i >= 0 ; i--) {
        HeapTuple *heapTuple = newHeapTuples[i];
        ItemPointerData ctid = newCtids[i];
        int diskTupleLen = newDiskTupleLens[i];

        /* Check small tuple */
        BufferDesc *bufferDesc = g_storageInstance->GetBufferMgr()->Read(
            g_defaultPdbId, ctid.GetPageId(), DSTORE::LW_SHARED);
        auto page = (HeapPage *)bufferDesc->GetPage();
        HeapDiskTuple *diskTuple = page->GetDiskTuple(ctid.GetOffset());
        TD *td = page->GetTd(diskTuple->GetTdId());
        ASSERT_EQ(td->GetXid(), xid);
        if (newUndoRecPtr == INVALID_UNDO_RECORD_PTR) {
            newUndoRecPtr = td->GetUndoRecPtr();
        }
        if (STORAGE_FUNC_SUCC(transactionMgr->FetchUndoRecord(xid, &newUndoRec, newUndoRecPtr))) {
            if (!newUndoRec.IsMatchedCtid(ctid)) {
                newUndoRecPtr = newUndoRec.GetTxnPreUndoPtr();
                if (STORAGE_FUNC_SUCC(transactionMgr->FetchUndoRecord(xid, &newUndoRec, newUndoRecPtr))) {
                    ASSERT_EQ(true, newUndoRec.IsMatchedCtid(ctid));
                }
            }
        }
        g_storageInstance->GetBufferMgr()->UnlockAndRelease(bufferDesc);

        /* Check tuple */
        HeapTuple *tuple = m_utTableHandler->FetchHeapTuple(&ctid);
        ASSERT_EQ(memcmp((void *)tuple->GetDiskTuple(), (void *)heapTuple->GetDiskTuple(), diskTupleLen), 0);
        DstorePfreeExt(tuple);
    }
}

/* this test case batch insert, the page has enough space to insert the third tuple, but not the MAXALIGN third tuple */
TEST_F(UTHeap, HeapBatchInsertTestPageNotEnough_level0)
{
    TransactionMgr *transactionMgr = g_storageInstance->GetPdb(g_defaultPdbId)->GetTransactionMgr();
    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetSnapshotCsn();

    /* Insert three tuple, the first two tuple batch insert one page, third tuple another page */
    const int newTuples = 3;
    HeapTuple *newHeapTuples[newTuples];
    uint32 newDiskTupleLens[newTuples];
    HeapTuple *heapTuple = m_utTableHandler->GenerateSpecificHeapTuple(std::string(5000, 'a'));
    newHeapTuples[0] = heapTuple;
    newDiskTupleLens[0] = heapTuple->GetDiskTupleSize();
    heapTuple = m_utTableHandler->GenerateSpecificHeapTuple(std::string(2584, 'a'));
    newHeapTuples[1] = heapTuple;
    newDiskTupleLens[1] = heapTuple->GetDiskTupleSize();
    heapTuple = m_utTableHandler->GenerateSpecificHeapTuple(std::string(350, 'a'));
    newHeapTuples[2] = heapTuple;
    newDiskTupleLens[2] = heapTuple->GetDiskTupleSize();

    ItemPointerData newCtids[newTuples];
    m_utTableHandler->BatchInsertHeapTupsAndCheckResult(
        newHeapTuples, newTuples, newCtids, txn->GetSnapshotData(), true);
    Xid xid = txn->GetCurrentXid();
    txn->Commit();

    /* check pageid and undorec */
    ASSERT_TRUE(newCtids[0].GetPageId() == newCtids[1].GetPageId());
    ASSERT_TRUE(newCtids[0].GetPageId() != newCtids[2].GetPageId());
    UndoRecord newUndoRec;
    UndoRecPtr newUndoRecPtr = INVALID_UNDO_RECORD_PTR;
    for (int i = newTuples - 1; i >= 0 ; i--) {
        ItemPointerData ctid = newCtids[i];
        BufferDesc *bufferDesc = g_storageInstance->GetBufferMgr()->Read(
            g_defaultPdbId, ctid.GetPageId(), DSTORE::LW_SHARED);
        HeapPage* page = static_cast<HeapPage *>(static_cast<void*>(bufferDesc->GetPage()));
        HeapDiskTuple *diskTuple = page->GetDiskTuple(ctid.GetOffset());
        TD *td = page->GetTd(diskTuple->GetTdId());
        ASSERT_EQ(td->GetXid(), xid);
        if (newUndoRecPtr == INVALID_UNDO_RECORD_PTR) {
            newUndoRecPtr = td->GetUndoRecPtr();
        }
        ASSERT_TRUE(STORAGE_FUNC_SUCC(transactionMgr->FetchUndoRecord(xid, &newUndoRec, newUndoRecPtr)));
        if (!newUndoRec.IsMatchedCtid(ctid)) {
            newUndoRecPtr = newUndoRec.GetTxnPreUndoPtr();
            ASSERT_TRUE(STORAGE_FUNC_SUCC(transactionMgr->FetchUndoRecord(xid, &newUndoRec, newUndoRecPtr)));
            ASSERT_EQ(true, newUndoRec.IsMatchedCtid(ctid));
        }
        g_storageInstance->GetBufferMgr()->UnlockAndRelease(bufferDesc);
    }
}

/*
 * Step:
 * 1、construct 10 small tuple and 10 big tuple to insert
 * 2、bigtuple will be inserted by multi call InsertSmallTuple
 * 3、small tuple will be inserted by call BatchInsertSmallTuples
 */
TEST_F(UTHeap, HeapBatchInsertBigAndSmallTuple_level0)
{
    Transaction *txn = thrd->GetActiveTransaction();
    /* Step 1: Insert 20 tuples */
    txn->Start();
    txn->SetSnapshotCsn();
    const uint16 nTuples = 20;
    HeapTuple *newHeapTuples[nTuples];
    uint32 newDiskTupleLens[nTuples];
    for (int i = 0; i < 10; i++) {
        HeapTuple *heapTuple = m_utTableHandler->GenerateSpecificHeapTuple(std::string(10000, 'a'));
        newHeapTuples[i] = heapTuple;
        newDiskTupleLens[i] = heapTuple->GetDiskTupleSize();
    }
    for (int i = 10; i < nTuples; i++) {
        HeapTuple *heapTuple = m_utTableHandler->GenerateSpecificHeapTuple(std::string(200, 'a'));
        newHeapTuples[i] = heapTuple;
        newDiskTupleLens[i] = heapTuple->GetDiskTupleSize();
    }

    ItemPointerData ctids[nTuples];
    m_utTableHandler->BatchInsertHeapTupsAndCheckResult(newHeapTuples, nTuples, ctids, txn->GetSnapshotData(), true);
    Xid xid = txn->GetCurrentXid();
    txn->Commit();

    /* Step 2: scan without key */
    txn->Start();
    txn->SetSnapshotCsn();
    int foundNum = 0;
    std::unique_ptr<StorageRelationData> heapRel(new StorageRelationData());
    heapRel->tableSmgr = m_utTableHandler->GetHeapTabSmgr();
    heapRel->m_pdbId = g_defaultPdbId;
    {
        HeapScanHandler scan(g_storageInstance, thrd, heapRel.get());
        scan.Begin(txn->GetSnapshot());
        while (scan.SeqScan() != nullptr) {
            foundNum++;
        }
        scan.End();
    }
    ASSERT_EQ(foundNum, nTuples);
    txn->Commit();
}

/*
 * Step:
 * 1、construct 6 small tuple and batch insert, they will use offset(1,2,3,4,5,6)
 * 2、delete 1,2,5,6 offset tuple, and update 3,4 offset tuple, because page has enough space, will not be prune.
 * 3、construct 10 small tuple and batch insert, they will use offset(7-16)
 */
TEST_F(UTHeap, HeapBatchInsertUseUnusedItem_level0)
{
    Transaction *txn = thrd->GetActiveTransaction();
    /* Step 1: Insert 6 tuples, offset(1,2,3,4,5,6)  */
    txn->Start();
    const uint16 nTuples = 6;
    HeapTuple *newHeapTuples[nTuples];
    uint32 newDiskTupleLens[nTuples];
    for (int i = 0; i < nTuples; i++) {
        HeapTuple *heapTuple = m_utTableHandler->GenerateSpecificHeapTuple(std::string(100, 'a'));
        newHeapTuples[i] = heapTuple;
        newDiskTupleLens[i] = heapTuple->GetDiskTupleSize();
    }
    ItemPointerData ctids[nTuples];
    m_utTableHandler->BatchInsertHeapTupsAndCheckResult(newHeapTuples, nTuples, ctids, INVALID_SNAPSHOT, true);
    txn->Commit();

    for (int i = 0; i < nTuples; i++) {
        DstorePfree(newHeapTuples[i]);
        ASSERT_EQ(ctids[i].GetOffset(), i + 1);
    }

    /* Step 2: delete 4 tuples, offset(1,2,5,6) */
    txn->Start();
    ASSERT_TRUE(STORAGE_FUNC_SUCC(DeleteTuple(&ctids[0], INVALID_SNAPSHOT, true)));
    ASSERT_TRUE(STORAGE_FUNC_SUCC(DeleteTuple(&ctids[1], INVALID_SNAPSHOT, true)));
    ASSERT_TRUE(STORAGE_FUNC_SUCC(DeleteTuple(&ctids[4], INVALID_SNAPSHOT, true)));
    ASSERT_TRUE(STORAGE_FUNC_SUCC(DeleteTuple(&ctids[5], INVALID_SNAPSHOT, true)));
    txn->Commit();

    /* Step 3: inplace update 2 tuples, offset(3,4) */
    txn->Start();
    HeapTuple* newTuple = m_utTableHandler->GenerateSpecificHeapTuple(std::string(100, 'b'));
    ASSERT_TRUE(UpdateTuple(&ctids[2], newTuple, INVALID_SNAPSHOT, true) == ctids[2]);
    ASSERT_TRUE(UpdateTuple(&ctids[3], newTuple, INVALID_SNAPSHOT, true) == ctids[3]);
    DstorePfreeExt(newTuple);
    txn->Commit();

    /* Step 4: Batch insert 10 tuples, offset(7-16)  */
    txn->Start();
    const uint16 tupleNum = 10;
    HeapTuple *heapTuples[tupleNum];
    uint32 tupleLens[tupleNum];
    for (int i = 0; i < tupleNum; i++) {
        HeapTuple *heapTuple = m_utTableHandler->GenerateSpecificHeapTuple(std::string(100, 'a'));
        heapTuples[i] = heapTuple;
        tupleLens[i] = heapTuple->GetDiskTupleSize();
    }
    ItemPointerData ctidBuf[tupleNum];
    m_utTableHandler->BatchInsertHeapTupsAndCheckResult(heapTuples, tupleNum, ctidBuf, INVALID_SNAPSHOT, true);
    txn->Commit();
    for (int i = 0; i < nTuples; i++) {
        ASSERT_EQ(ctidBuf[i].GetOffset(), i + 7);
    }
}

TEST_F(UTHeap, HeapInsertMultiTuple_level0)
{
    /* Step 1: Insert 10 tuples and check tdid */
    Transaction *txn = thrd->GetActiveTransaction();
    const uint16 nTuples = 10;
    ItemPointerData ctids[nTuples];
    Xid xids[nTuples];
    for (int i = 0; i < nTuples; i++) {
        HeapTuple *heapTuple = m_utTableHandler->GenerateSpecificHeapTuple(std::string(100, 'a'));
        ctids[i] = m_utTableHandler->InsertHeapTupAndCheckResult(heapTuple);
        DstorePfree(heapTuple);
        /* check tdid and record the xid */
        BufferDesc* bufferDesc = g_storageInstance->GetBufferMgr()->Read(g_defaultPdbId,
            ctids[0].GetPageId(), LW_SHARED);
        HeapPage* page = static_cast<HeapPage *>(bufferDesc->GetPage());
        TdId tdId = page->GetTupleTdId(ctids[i].GetOffset());
        TD* td = page->GetTd(tdId);
        ASSERT_EQ(tdId, i % DEFAULT_TD_COUNT);
        xids[i] = td->GetXid();
        g_storageInstance->GetBufferMgr()->UnlockAndRelease(bufferDesc);
    }

    /* Step 2: get first tuple's xid through the undo record and compare the xid */
    HeapTuple tuple;
    BufferDesc* bufferDesc = g_storageInstance->GetBufferMgr()->Read(g_defaultPdbId,
        ctids[0].GetPageId(), LW_SHARED);
    HeapPage* page = static_cast<HeapPage *>(bufferDesc->GetPage());
    page->GetTuple(&tuple, ctids[0].GetOffset());
    g_storageInstance->GetBufferMgr()->UnlockAndRelease(bufferDesc);
    Xid newxid = tuple.GetXid();
    ASSERT_EQ(newxid, xids[0]);
}

TEST_F(UTHeap, HeapCheckUpdateXid_level1)
{
    Transaction *txn = thrd->GetActiveTransaction();

    /* Step 1: Insert one tuple */
    txn->Start();
    txn->SetSnapshotCsn();
    HeapTuple *heapTuple = m_utTableHandler->GenerateSpecificHeapTuple(std::string(100, 'a'));
    int diskTupleLen = heapTuple->GetDiskTupleSize();
    ItemPointerData ctid = m_utTableHandler->InsertHeapTupAndCheckResult(heapTuple, true, txn->GetSnapshotData());
    Xid insertXid = txn->GetCurrentXid();
    txn->Commit();

    /* Step 2: get disktuple */
    BufferDesc *bufferDesc = g_storageInstance->GetBufferMgr()->
        Read(g_defaultPdbId, ctid.GetPageId(), DSTORE::LW_SHARED);
    HeapPage* page = (HeapPage*)bufferDesc->GetPage();
    HeapDiskTuple *diskTuple = page->GetDiskTuple(ctid.GetOffset());
    Xid tupleXiud = diskTuple->GetXid();

    g_storageInstance->GetBufferMgr()->UnlockAndRelease(bufferDesc);
    ASSERT_EQ(tupleXiud, insertXid);

    /* step 3: update one tuple */
    HeapTuple* newTuple = m_utTableHandler->GenerateSpecificHeapTuple(std::string(100, 'b'));
    txn->Start();
    ItemPointerData newCtid = UpdateTupAndCheckResult(&ctid, newTuple, INVALID_SNAPSHOT, true);
    ASSERT_EQ(newCtid, ctid); /* Check ctid is not changed */
    Xid updateXid = txn->GetCurrentXid();
    txn->Commit();

    /* Step 4: get disktuple */
    BufferDesc *newBufferDesc = g_storageInstance->GetBufferMgr()->
        Read(g_defaultPdbId, newCtid.GetPageId(), DSTORE::LW_SHARED);
    HeapPage* newPage = (HeapPage*)newBufferDesc->GetPage();
    HeapDiskTuple *newDiskTuple = newPage->GetDiskTuple(newCtid.GetOffset());
    Xid newTupleXiud = newDiskTuple->GetXid();

    ASSERT_EQ(updateXid, newTupleXiud);
    g_storageInstance->GetBufferMgr()->UnlockAndRelease(newBufferDesc);
    DstorePfreeExt(heapTuple);
    DstorePfreeExt(newTuple);
}

TEST_F(UTHeap, HeapUpdateCheakXidRollback_level1)
{
    Transaction *txn = thrd->GetActiveTransaction();
 
    /* Step 1: Insert one tuple */
    txn->Start();
    txn->SetSnapshotCsn();
    HeapTuple *heapTuple = m_utTableHandler->GenerateSpecificHeapTuple(std::string(100, 'a'));
    int diskTupleLen = heapTuple->GetDiskTupleSize();
    ItemPointerData ctid = m_utTableHandler->InsertHeapTupAndCheckResult(heapTuple, true, txn->GetSnapshotData());
    Xid insertXid = txn->GetCurrentXid();
    txn->Commit();
 
    /* Step 2: get disktuple */
    BufferDesc *bufferDesc = g_storageInstance->GetBufferMgr()->
        Read(g_defaultPdbId, ctid.GetPageId(), DSTORE::LW_SHARED);
    HeapPage* page = (HeapPage*)bufferDesc->GetPage();
    HeapDiskTuple *diskTuple = page->GetDiskTuple(ctid.GetOffset());
    Xid tupleXiud = diskTuple->GetXid();
 
    ASSERT_EQ(tupleXiud, insertXid);
    g_storageInstance->GetBufferMgr()->UnlockAndRelease(bufferDesc);
 
    /* step 3: update and rollback */
    HeapTuple* newTuple = m_utTableHandler->GenerateSpecificHeapTuple(std::string(100, 'b'));
    txn->Start();
    ItemPointerData newCtid = UpdateTupAndCheckResult(&ctid, newTuple, INVALID_SNAPSHOT, true);
    txn->Abort();
 
    BufferDesc *newBufferDesc = g_storageInstance->GetBufferMgr()->
    Read(g_defaultPdbId, newCtid.GetPageId(), DSTORE::LW_SHARED);
    HeapPage* newPagege = (HeapPage*)newBufferDesc->GetPage();
    HeapDiskTuple *newDiskTuple = newPagege->GetDiskTuple(newCtid.GetOffset());
    Xid newTupleXiud = newDiskTuple->GetXid();

    ASSERT_EQ(tupleXiud, newTupleXiud);
    g_storageInstance->GetBufferMgr()->UnlockAndRelease(newBufferDesc);
    DstorePfreeExt(heapTuple);
    DstorePfreeExt(newTuple);
}
