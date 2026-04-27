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

TEST_F(UTHeap, HeapInplaceUpdateDiffTest_level0)
{
    HeapTuple *baseTuple = m_utTableHandler->GenerateSpecificHeapTuple("base_Data");
    HeapTuple *smallerTuple = m_utTableHandler->GenerateSpecificHeapTuple("baseData");
    HeapTuple *biggerTuple = m_utTableHandler->GenerateSpecificHeapTuple("base__Data");
    HeapTuple *appendTuple = m_utTableHandler->GenerateSpecificHeapTuple("appendbase_Data");

    /* Case 1: newTupleSize < oldTupleSize */
    std::unique_ptr<StorageRelationData> heapRel(new StorageRelationData());
    heapRel->tableSmgr = m_utTableHandler->GetHeapTabSmgr();
    heapRel->m_pdbId = g_defaultPdbId;
    HeapUpdateHandler sUpdate(g_storageInstance, thrd, heapRel.get());
    sUpdate.GetDiffBetweenTuples(baseTuple, smallerTuple);
    ASSERT_EQ(sUpdate.m_numDiffPos, 2);
    uint16 diffSize = sUpdate.GetTupleDiffSize(baseTuple->GetDiskTupleSize() - HEAP_DISK_TUP_HEADER_SIZE);
    ASSERT_EQ(sUpdate.m_tupDiffNum, 2);
    ASSERT_EQ(diffSize, 5 + sizeof(uint16) * 2);
    diffSize = sUpdate.GetTupleDiffSize(smallerTuple->GetDiskTupleSize() - HEAP_DISK_TUP_HEADER_SIZE);
    ASSERT_EQ(sUpdate.m_tupDiffNum, 2);
    ASSERT_EQ(diffSize, 4 + sizeof(uint16) * 2);

    /* Case 2: newTupleSize > oldTupleSize */
    HeapUpdateHandler bUpdate(g_storageInstance, thrd, heapRel.get());
    bUpdate.GetDiffBetweenTuples(baseTuple, biggerTuple);
    ASSERT_EQ(bUpdate.m_numDiffPos, 2);
    diffSize = bUpdate.GetTupleDiffSize(baseTuple->GetDiskTupleSize() - HEAP_DISK_TUP_HEADER_SIZE);
    ASSERT_EQ(bUpdate.m_tupDiffNum, 2);
    ASSERT_EQ(diffSize, 4 + sizeof(uint16) * 2);
    diffSize = bUpdate.GetTupleDiffSize(biggerTuple->GetDiskTupleSize() - HEAP_DISK_TUP_HEADER_SIZE);
    ASSERT_EQ(bUpdate.m_tupDiffNum, 2);
    ASSERT_EQ(diffSize, 5 + sizeof(uint16) * 2);

    /* Case 3: append data, just for BACKWARD */
    HeapUpdateHandler aUpdate(g_storageInstance, thrd, heapRel.get());
    aUpdate.GetDiffBetweenTuples(baseTuple, appendTuple);
    ASSERT_EQ(aUpdate.m_numDiffPos, 2);
    diffSize = aUpdate.GetTupleDiffSize(baseTuple->GetDiskTupleSize() - HEAP_DISK_TUP_HEADER_SIZE);
    ASSERT_EQ(aUpdate.m_tupDiffNum, 0);
    ASSERT_EQ(diffSize, 0);
    diffSize = aUpdate.GetTupleDiffSize(appendTuple->GetDiskTupleSize() - HEAP_DISK_TUP_HEADER_SIZE);
    ASSERT_EQ(aUpdate.m_tupDiffNum, 2);
    ASSERT_EQ(diffSize, 6 + sizeof(uint16) * 2);

    DstorePfreeExt(baseTuple);
    DstorePfreeExt(smallerTuple);
    DstorePfreeExt(biggerTuple);
    DstorePfreeExt(appendTuple);
}

TEST_F(UTHeap, HeapInplaceUpdateTest_level0)
{
    std::vector<HeapTuple *> heapTupsForInsert;
    std::vector<HeapTuple *> heapTupsForUpdate1;
    std::vector<HeapTuple *> HeapTupsForUpdate2;

    /* prepare kinds of data */
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
    RollbackUpdateTest(heapTupsForInsert, heapTupsForUpdate1, HeapTupsForUpdate2);
    for (i = 0; i < NUM_TUP; ++i) {
        DstorePfreeExt(heapTupsForInsert[i]);
        DstorePfreeExt(heapTupsForUpdate1[i]);
        DstorePfreeExt(HeapTupsForUpdate2[i]);
    }
}

TEST_F(UTHeap, HeapUndoInplaceUpdateTest_TIER1_level0)
{
    Transaction *txn = thrd->GetActiveTransaction();
    constexpr int TUPLE_DATA_LEN = 10;
    std::string stra(TUPLE_DATA_LEN, 'a');
    std::string strb(TUPLE_DATA_LEN, 'b');

    /* Step 1: insert one tuple for inplace update, other tuples for filling td slot */
    SnapshotData snapshot;
    ItemId originItemId;
    TD originTd;
    ItemPointerData ctid = PrepareTupleForUndo(stra, snapshot, originItemId, originTd, DEFAULT_TD_COUNT);
    HeapTuple *originTup = m_utTableHandler->FetchHeapTuple(&ctid);

    /* Step 2: update the tuple */
    HeapTuple *tupb = m_utTableHandler->GenerateSpecificHeapTuple(strb);
    ItemPointerData newCtid = UpdateTupAndCheckResult(&ctid, tupb);
    ASSERT_EQ(newCtid, ctid);
    DstorePfreeExt(tupb);

    /* Step 3: undo and check */
    txn->Start();
    txn->SetSnapshotCsnForFlashback(snapshot.GetCsn());

    /* ---- cr page ---- */
    CheckCrPage(ctid, originTup, &originItemId, &originTd, snapshot.GetCsn() - 1);

    /* ---- cr tuple ---- */
    CheckCrTuple(ctid, originTup);

    txn->Commit();
    DstorePfreeExt(originTup);
}

TEST_F(UTHeap, HeapBigTupUpdateTest_level0)
{
    ItemPointerData newCtid;
    /* Step 1: Insert big tuple */
    std::string bigString;
    int bigTupleLen = 790000, i = 0;
    for (i = 0; i < bigTupleLen; i++) {
        bigString.append("a");
    }
    HeapTuple *srcHeapTup = m_utTableHandler->GenerateSpecificHeapTuple(bigString);
    ItemPointerData ctid = m_utTableHandler->InsertHeapTupAndCheckResult(srcHeapTup);
    DstorePfreeExt(srcHeapTup);

    /* Step 2: Update the big tuple and new tuple is bigger than old */
    std::string newBigString;
    int newBigTupLen = 1000000;
    for (i = 0; i < newBigTupLen; i++) {
        newBigString.append("b");
    }
    HeapTuple *heapNewTuple = m_utTableHandler->GenerateSpecificHeapTuple(newBigString);
    newCtid = UpdateTupAndCheckResult(&ctid, heapNewTuple);
    DstorePfreeExt(heapNewTuple);

    /* Step 3: Update the big tuple and new tuple is smaller than old */
    newBigTupLen = 100;
    newBigString.clear();
    for (i = 0; i < newBigTupLen; i++) {
        newBigString.append("c");
    }
    heapNewTuple = m_utTableHandler->GenerateSpecificHeapTuple(newBigString);
    newCtid = UpdateTupAndCheckResult(&newCtid, heapNewTuple);
    DstorePfreeExt(heapNewTuple);
}

TEST_F(UTHeap, HeapSamePageUpdateTest_level0)
{
    std::vector<HeapTuple *> heapTupsForInsert;
    std::vector<HeapTuple *> heapTupsForUpdate1;
    std::vector<HeapTuple *> HeapTupsForUpdate2;

    /* prepare kinds of data */
    int i = 0;
    constexpr int NUM_TUP = 1;
    for (i = 0; i < NUM_TUP; ++i) {
        HeapTuple *insertTuple = m_utTableHandler->GenerateSpecificHeapTuple("insert_data1");
        HeapTuple *updateTuple1 = m_utTableHandler->GenerateSpecificHeapTuple("update_data1_same_page1");
        HeapTuple *updateTuple2 = m_utTableHandler->GenerateSpecificHeapTuple("update_data2_same_page2");
        heapTupsForInsert.push_back(insertTuple);
        heapTupsForUpdate1.push_back(updateTuple1);
        HeapTupsForUpdate2.push_back(updateTuple2);
    }
    MultiUpdateTest(heapTupsForInsert, heapTupsForUpdate1, HeapTupsForUpdate2);
    RollbackUpdateTest(heapTupsForInsert, heapTupsForUpdate1, HeapTupsForUpdate2);
    for (i = 0; i < NUM_TUP; ++i) {
        DstorePfreeExt(heapTupsForInsert[i]);
        DstorePfreeExt(heapTupsForUpdate1[i]);
        DstorePfreeExt(HeapTupsForUpdate2[i]);
    }
}

TEST_F(UTHeap, HeapUndoSamePageUpdateTest_TIER1_level0)
{
    Transaction *txn = thrd->GetActiveTransaction();
    constexpr int TUPLE_DATA_LEN = 10;
    std::string stra(TUPLE_DATA_LEN, 'a');
    std::string strb(TUPLE_DATA_LEN * 2, 'b');

    /* Step 1: insert one tuple for update, other tuples for filling td slot */
    SnapshotData snapshot;
    ItemId originItemId;
    TD originTd;
    ItemPointerData ctid = PrepareTupleForUndo(stra, snapshot, originItemId, originTd, DEFAULT_TD_COUNT);
    HeapTuple *originTup = m_utTableHandler->FetchHeapTuple(&ctid);

    /* Step 2: update the tuple */
    HeapTuple *tupb = m_utTableHandler->GenerateSpecificHeapTuple(strb);
    ItemPointerData newCtid = UpdateTupAndCheckResult(&ctid, tupb);
    ASSERT_EQ(newCtid, ctid);
    DstorePfreeExt(tupb);
    BufferDesc *bufferDesc = g_storageInstance->GetBufferMgr()->
        Read(g_defaultPdbId, newCtid.GetPageId(), LW_SHARED);
    auto page = (HeapPage*)bufferDesc->GetPage();
    OffsetNumber newOffset = page->GetItemIdPtr(newCtid.GetOffset())->GetOffset();
    g_storageInstance->GetBufferMgr()->UnlockAndRelease(bufferDesc);

    /* Step 3: undo and check */
    txn->Start();
    txn->SetSnapshotCsnForFlashback(snapshot.GetCsn());

    /* ---- cr page ---- */
    originItemId.SetOffset(newOffset);
    CheckCrPage(ctid, originTup, &originItemId, &originTd, snapshot.GetCsn() - 1);

    /* ---- cr tuple ---- */
    CheckCrTuple(newCtid, originTup);

    txn->Commit();
    DstorePfreeExt(originTup);
}

TEST_F(UTHeap, HeapAnotherPageUpdateTest_level0)
{
    constexpr int TUPLE_DATA_LEN = 1024;
    std::string str(TUPLE_DATA_LEN, 'a');

    /* insert one specific tuple, update it later. */
    HeapTuple *specTuple = m_utTableHandler->GenerateSpecificHeapTuple(str);
    ItemPointerData specCtid = m_utTableHandler->InsertHeapTupAndCheckResult(specTuple);

    /* fill current page for another page update */
    constexpr int TUPLE_NUM_PER_PAGE = 7;
    for (int i = 0; i < TUPLE_NUM_PER_PAGE - 1; ++i) {
        InsertSpecificHeapTuple(str);
    }

    Transaction *transaction = thrd->GetActiveTransaction();

    /* case 1: the new tuple on new page is invisible, because we have not increased command id */
    transaction->Start();
    transaction->SetSnapshotCsn();
    str.append("helloWorld");
    HeapTuple *newTuple = m_utTableHandler->GenerateSpecificHeapTuple(str);
    HeapUpdateContext updateContext;
    updateContext.oldCtid = specCtid;
    updateContext.newTuple = newTuple;
    updateContext.snapshot = *(transaction->GetSnapshotData());
    updateContext.cid = transaction->GetCurCid();
    std::unique_ptr<StorageRelationData> heapRel(new StorageRelationData());
    heapRel->tableSmgr = m_utTableHandler->GetHeapTabSmgr();
    heapRel->m_pdbId = g_defaultPdbId;
    {
        HeapUpdateHandler heapUpdate(g_storageInstance, thrd, heapRel.get());
        heapUpdate.Update(&updateContext);
    }
    ASSERT_NE(specCtid.GetPageId(), updateContext.newCtid.GetPageId());
    HeapTuple *scanTuple = nullptr;
    {
        HeapScanHandler heapScan(g_storageInstance, thrd, heapRel.get());
        heapScan.Begin(transaction->GetSnapshotData());
        while ((scanTuple = heapScan.SeqScan()) != nullptr) {
            ASSERT_NE(updateContext.newCtid, *scanTuple->GetCtid());
        }
        heapScan.End();
    }
    transaction->Commit();

    /* case 2: the new tuple on new page is visible after the last transaction committed */
    transaction->Start();
    transaction->SetSnapshotCsn();
    bool found = false;
    {
        HeapScanHandler scan(g_storageInstance, thrd, heapRel.get());
        scan.Begin(transaction->GetSnapshotData());
        while ((scanTuple = scan.SeqScan()) != nullptr) {
            if (updateContext.newCtid == *scanTuple->GetCtid()) {
                found = true;
            }
        }
        scan.End();
    }
    ASSERT_TRUE(found);
    transaction->Commit();

    DstorePfreeExt(specTuple);
    DstorePfreeExt(newTuple);
}

TEST_F(UTHeap, HeapUndoAnotherPageUpdateTest_level0)
{
    Transaction *txn = thrd->GetActiveTransaction();
    constexpr int TUPLE_DATA_LEN = 1024;
    std::string stra(TUPLE_DATA_LEN, 'a');
    std::string strb(TUPLE_DATA_LEN * 2, 'b');

    /* Step 1: insert one tuple for inplace update, other tuples for filling td slot */
    SnapshotData snapshot;
    ItemId originItemId;
    TD originTd;
    constexpr int TUPLE_NUM_PER_PAGE = 7;
    ItemPointerData ctid = PrepareTupleForUndo(stra, snapshot, originItemId, originTd, TUPLE_NUM_PER_PAGE);
    HeapTuple *originTup = m_utTableHandler->FetchHeapTuple(&ctid);

    /* Step 2: update the tuple */
    HeapTuple *tupb = m_utTableHandler->GenerateSpecificHeapTuple(strb);
    ItemPointerData newCtid = UpdateTupAndCheckResult(&ctid, tupb);
    ASSERT_NE(newCtid.GetPageId(), ctid.GetPageId());
    DstorePfreeExt(tupb);

    /* Step 3: undo and check */
    txn->Start();
    txn->SetSnapshotCsnForFlashback(snapshot.GetCsn());

    /* ---- cr page ---- */
    CheckCrPage(ctid, originTup, &originItemId, &originTd, snapshot.GetCsn() - 1);
    CheckCrPage(newCtid);

    /* ---- cr tuple ---- */
    CheckCrTuple(ctid, originTup);
    CheckCrTuple(newCtid);

    txn->Commit();
    DstorePfreeExt(originTup);
}

TEST_F(UTHeap, HeapUpdateFailedTest_level0)
{
    /* case 1: insert tuple and delete this tuple, then update failed */
    /* insert one tuple */
    ItemPointerData ctid1 = InsertSpecificHeapTuple(std::string(100, 'a'));
    StorageAssert(ctid1 != INVALID_ITEM_POINTER);

    /* delete this tuple */
    RetStatus ret = DeleteTuple(&ctid1);
    StorageAssert(STORAGE_FUNC_SUCC(ret));

    /* delete this tuple again */
    ret = DeleteTuple(&ctid1);
    StorageAssert(STORAGE_FUNC_FAIL(ret));

    /* failed to update this tuple because this tuple has been deleted */
    HeapTuple* newTuple = m_utTableHandler->GenerateSpecificHeapTuple(std::string(100, 'b'));
    ItemPointerData newCtid1 =  UpdateTuple(&ctid1, newTuple);
    StorageAssert(newCtid1 == INVALID_ITEM_POINTER);
    DstorePfreeExt(newTuple);

    /* case 2: insert tuple and same page append update this tuple, then update failed */
    /* insert one tuple */
    ItemPointerData ctid2 = InsertSpecificHeapTuple(std::string(100, 'c'));
    StorageAssert(ctid2 != INVALID_ITEM_POINTER);

    /* another page update this tuple */
    newTuple = m_utTableHandler->GenerateSpecificHeapTuple(std::string(7900, 'd'));
    StorageAssert(UpdateTuple(&ctid2, newTuple) != INVALID_ITEM_POINTER);
    DstorePfreeExt(newTuple);

    /* failed to update this tuple because this tuple has been another page updated */
    ItemPointerData newCtid2 =  UpdateTuple(&ctid2, newTuple);
    StorageAssert(newCtid2 == INVALID_ITEM_POINTER);
    DstorePfreeExt(newTuple);
}

TEST_F(UTHeap, HeapBigTupUpdateRollbackTest_level0)
{
    Transaction *txn = thrd->GetActiveTransaction();
    ItemPointerData newCtid;
    /* Step 1: Insert big tuple */
    std::string bigString;
    int bigTupleLen = 1400;
    for (int i = 0; i < bigTupleLen; i++) {
        bigString.append("1234567890");
    }
    HeapTuple *srcHeapTup = m_utTableHandler->GenerateSpecificHeapTuple(bigString);
    int diskTupleSize = srcHeapTup->GetDiskTupleSize();
    txn->Start();
    txn->SetSnapshotCsn();
    ItemPointerData ctid = m_utTableHandler->InsertHeapTupAndCheckResult(srcHeapTup, true, txn->GetSnapshotData());
    DstorePfreeExt(srcHeapTup);
    txn->Commit();

    /* Step 2: Update the big tuple and new tuple is bigger than old */
    txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetSnapshotCsn();
    std::string newBigString;
    int newBigTupLen = 1402;
    for (int i = 0; i < newBigTupLen; i++) {
        newBigString.append("1234567890");
    }
    HeapTuple *heapNewTuple = m_utTableHandler->GenerateSpecificHeapTuple(newBigString);
    newCtid = UpdateTupAndCheckResult(&ctid, heapNewTuple, txn->GetSnapshotData(), true);
    DstorePfreeExt(heapNewTuple);
    txn->Abort();

    /* scan to fetch tuple, not null */
    txn = thrd->GetActiveTransaction();
    std::unique_ptr<StorageRelationData> heapRel(new StorageRelationData());
    heapRel->tableSmgr = m_utTableHandler->GetHeapTabSmgr();
    heapRel->m_pdbId = g_defaultPdbId;
    HeapScanHandler *scan = DstoreNew(thrd->GetTransactionMemoryContext())
        HeapScanHandler(g_storageInstance, thrd, heapRel.get());
    scan->Begin(txn->GetSnapshot());
    HeapTuple *heapTuple = nullptr;
    int tupleNums = 0;
    while ((heapTuple = scan->SeqScan()) != nullptr) {
        tupleNums++;
        ASSERT_EQ(heapTuple->GetDiskTupleSize(), diskTupleSize);
    }
    ASSERT_EQ(tupleNums, 1);

    scan->End();
    delete scan;
    txn->Commit();
}