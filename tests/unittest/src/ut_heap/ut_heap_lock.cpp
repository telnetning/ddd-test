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
#include <thread>
#include "port/dstore_port.h"
#include "ut_mock/ut_mock.h"
#include "ut_heap/ut_heap.h"

TEST_F(UTHeap, HeapLockTupleTest_level0)
{
    std::string s1 = "tuple1";
    std::string s2 = "tuple2";
    HeapTuple *tuple1 = m_utTableHandler->GenerateSpecificHeapTuple(s1);
    HeapTuple *tuple2 = m_utTableHandler->GenerateSpecificHeapTuple(s2);
    Transaction *transaction = thrd->GetActiveTransaction();

    /* Step 1: insert one tuple */
    ItemPointerData ctid = m_utTableHandler->InsertHeapTupAndCheckResult(tuple1);

    /* Step 2: lock the tuple */
    transaction->Start();
    transaction->SetSnapshotCsn();
    int result = LockUnchangedTuple(ctid, transaction->GetSnapshotData(), true);
    ASSERT_EQ(result, DSTORE_SUCC);

    /* Step 3: tuple can be updated in current transaction */
    ItemPointerData newCtid = UpdateTupAndCheckResult(&ctid, tuple2, transaction->GetSnapshotData(), true);
    ASSERT_EQ(newCtid, ctid);
    transaction->Commit();

    /* Step 4: locker committed, tuple can be updated in another transaction */
    newCtid = UpdateTupAndCheckResult(&ctid, tuple1);
    ASSERT_EQ(newCtid, ctid);

    DstorePfreeExt(tuple1);
    DstorePfreeExt(tuple2);
}

TEST_F(UTHeap, HeapLockSelfCreatedTupleTest_level0)
{
    std::string s1 = "tuple1";
    HeapTuple *tuple1 = m_utTableHandler->GenerateSpecificHeapTuple(s1);
    RetStatus result;

    /* Step 1: start transaction */
    Transaction *transaction = thrd->GetActiveTransaction();
    transaction->Start();
    transaction->SetSnapshotCsn();

    /* Step 2: insert one tuple, and not increase cid to make sure follow LockUnchangedTuple result is SELF_CREATED*/
    ItemPointerData ctid =
        m_utTableHandler->InsertHeapTupAndCheckResult(tuple1, true, transaction->GetSnapshotData(), false, false);

    /* Step 3: try lock the tuple failed on HeapLockTupleContext.allowLockSelf = false */
    HeapLockTupleContext context;
    context.ctid = ctid;
    context.snapshot = *transaction->GetSnapshotData();
    context.allowLockSelf = false;
    thrd->GetActiveTransaction()->SetSnapshotCsn();

    std::unique_ptr<StorageRelationData> heapRel(new StorageRelationData());
    heapRel->tableSmgr = m_utTableHandler->GetHeapTabSmgr();
    heapRel->m_pdbId = g_defaultPdbId;
    HeapLockTupHandler lockTuple(g_storageInstance, thrd, heapRel.get());

    result = lockTuple.LockUnchangedTuple(&context);
    ASSERT_EQ(result, DSTORE_FAIL);
    ASSERT_EQ(context.failureInfo.reason, HeapHandlerFailureReason::SELF_CREATED);

    /* Step 4: try lock the tuple success on HeapLockTupleContext.allowLockSelf = true */
    context.allowLockSelf = true;
    result = lockTuple.LockUnchangedTuple(&context);
    ASSERT_EQ(result, DSTORE_SUCC);

    transaction->Commit();
    DstorePfreeExt(tuple1);
}

TEST_F(UTHeap, HeapLockSelfUpdatedTupleTest_level0)
{
    std::string s1 = "tuple1";
    std::string s2 = "tuple2";
    HeapTuple *tuple1 = m_utTableHandler->GenerateSpecificHeapTuple(s1);
    HeapTuple *tuple2 = m_utTableHandler->GenerateSpecificHeapTuple(s2);
    RetStatus result;

    Transaction *transaction = thrd->GetActiveTransaction();
    /* Step 1: insert one tuple */
    ItemPointerData ctid = m_utTableHandler->InsertHeapTupAndCheckResult(tuple1);

    /*
     * Step 2: start transaction and update tuple, and not increase cid to make sure follow LockUnchangedTuple result
     * is SELF_MODIFIED
     */
    transaction->Start();
    transaction->SetSnapshotCsn();
    ItemPointerData newCtid = UpdateTuple(&ctid, tuple2, transaction->GetSnapshotData(), true, m_utTableHandler, false);
    ASSERT_EQ(newCtid, ctid);

    /* Step 3: try lock the tuple failed on HeapLockTupleContext.allowLockSelf = false */
    HeapLockTupleContext context;
    context.ctid = ctid;
    context.snapshot = *transaction->GetSnapshotData();
    context.allowLockSelf = false;
    thrd->GetActiveTransaction()->SetSnapshotCsn();

    std::unique_ptr<StorageRelationData> heapRel(new StorageRelationData());
    heapRel->tableSmgr = m_utTableHandler->GetHeapTabSmgr();
    heapRel->m_pdbId = g_defaultPdbId;
    HeapLockTupHandler lockTuple(g_storageInstance, thrd, heapRel.get());

    result = lockTuple.LockUnchangedTuple(&context);
    ASSERT_EQ(result, DSTORE_FAIL);
    ASSERT_EQ(context.failureInfo.reason, HeapHandlerFailureReason::SELF_MODIFIED);

    /* Step 4: try lock the tuple failed on HeapLockTupleContext.allowLockSelf = true */
    context.allowLockSelf = true;
    result = lockTuple.LockUnchangedTuple(&context);
    ASSERT_EQ(result, DSTORE_SUCC);

    transaction->Commit();

    DstorePfreeExt(tuple1);
    DstorePfreeExt(tuple2);
}

TEST_F(UTHeap, HeapLockBigTupleTest_level0)
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

    /* Step 2: Lock tuple */
    Transaction *transaction = thrd->GetActiveTransaction();
    HeapLockTupleContext context;
    context.ctid = ctid;
    context.needRetTup = true;
    transaction->Start();
    transaction->SetSnapshotCsn();
    std::unique_ptr<StorageRelationData> heapRel(new StorageRelationData());
    heapRel->tableSmgr = m_utTableHandler->GetHeapTabSmgr();
    heapRel->m_pdbId = g_defaultPdbId;
    {
        HeapLockTupHandler lockTuple(g_storageInstance, thrd, heapRel.get());
        RetStatus result = lockTuple.LockUnchangedTuple(&context);
        EXPECT_EQ(result, DSTORE_SUCC);
    }
    transaction->Commit();

    /* Step 3: Check tuple */
    EXPECT_TRUE(context.retTup->GetDiskTupleSize() == diskTupleLen);
    EXPECT_EQ(memcmp((void*)context.retTup->GetDiskTuple()->GetData(), (void*)bigString.c_str(), bigTupleLen), 0);
    DstorePfreeExt(context.retTup);
    DstorePfreeExt(heapTuple);
}

TEST_F(UTHeap, HeapLockNewestTupleTest_level0)
{
    /* Step 1: Insert tuple to fill the page */
    constexpr int TUPLE_DATA_LEN = 1024;
    std::string str(TUPLE_DATA_LEN, 'a');
    constexpr int TUPLE_NUM_PER_PAGE = 7;
    ItemPointerData specCtid;
    for (int i = 0; i < TUPLE_NUM_PER_PAGE; ++i) {
        specCtid = InsertSpecificHeapTuple(str);
    }

    /* Step 2: Update tuple another page */
    str.append("helloWorld");
    Transaction *transaction = thrd->GetActiveTransaction();
    transaction->Start();
    transaction->SetSnapshotCsn();
    HeapTuple *newTuple = m_utTableHandler->GenerateSpecificHeapTuple(str);
    HeapUpdateContext updateContext;
    updateContext.oldCtid = specCtid;
    updateContext.newTuple = newTuple;
    updateContext.snapshot = *transaction->GetSnapshotData();
    updateContext.cid = transaction->GetCurCid();
    RetStatus ret;
    std::unique_ptr<StorageRelationData> heapRel(new StorageRelationData());
    heapRel->tableSmgr = m_utTableHandler->GetHeapTabSmgr();
    heapRel->m_pdbId = g_defaultPdbId;
    {
        HeapUpdateHandler heapUpdate(g_storageInstance, thrd, heapRel.get());
        ret = heapUpdate.Update(&updateContext);
    }
    ASSERT_TRUE(STORAGE_FUNC_SUCC(ret));
    uint32 tupleLen = newTuple->GetDiskTupleSize();
    DstorePfreeExt(newTuple);
    transaction->Commit();

    /* Step3: Lock newest tuple */
    transaction->Start();
    transaction->SetSnapshotCsn();
    HeapLockTupleContext lockTupContext;
    lockTupContext.ctid = specCtid;
    lockTupContext.needRetTup = true;
    heapRel->tableSmgr = m_utTableHandler->GetHeapTabSmgr();
    heapRel->m_pdbId = g_defaultPdbId;
    {
        HeapLockTupHandler heapLockTup(g_storageInstance, thrd, heapRel.get());
        ret = heapLockTup.LockNewestTuple(&lockTupContext);
    }
    ASSERT_TRUE(STORAGE_FUNC_SUCC(ret));
    transaction->Commit();

    /* Step 4: Check tuple */
    EXPECT_TRUE(lockTupContext.retTup->GetDiskTupleSize() == tupleLen);
    EXPECT_NE(memcmp(&lockTupContext.ctid, &specCtid, sizeof(ItemPointerData)), 0);
    EXPECT_EQ(memcmp((void*)lockTupContext.retTup->GetDiskTuple()->GetData(), (void*)str.c_str(), str.size()), 0);
    DstorePfreeExt(lockTupContext.retTup);
}

TEST_F(UTHeap, HeapFetchUndoRecordTest_TIER1_level0)
{
    /* Step 1: Insert Tuple */
    std::string str("helloWorld");
    Transaction *transaction = thrd->GetActiveTransaction();
    transaction->Start();
    ItemPointerData specCtid = InsertSpecificHeapTuple(str, INVALID_SNAPSHOT, true);

    /* Step 2: Update Tuple */
    str.append("tmp");
    HeapTuple *newTuple = m_utTableHandler->GenerateSpecificHeapTuple(str);
    HeapUpdateContext updateContext;
    updateContext.oldCtid = specCtid;
    updateContext.newTuple = newTuple;
    updateContext.snapshot = *transaction->GetSnapshotData();
    updateContext.cid = transaction->GetCurCid();
    RetStatus ret;
    std::unique_ptr<StorageRelationData> heapRel(new StorageRelationData());
    heapRel->tableSmgr = m_utTableHandler->GetHeapTabSmgr();
    heapRel->m_pdbId = g_defaultPdbId;
    {
        HeapUpdateHandler heapUpdate(g_storageInstance, thrd, heapRel.get());
        ret = heapUpdate.Update(&updateContext);
    }
    ASSERT_TRUE(STORAGE_FUNC_SUCC(ret));
    uint32 tupleLen = newTuple->GetDiskTupleSize();
    DstorePfreeExt(newTuple);

    /* Step 3: Set snapshot cid, delete tuple fail because tuple is changed */
    transaction->SetSnapshotCid(0);
    ASSERT_TRUE(STORAGE_FUNC_FAIL(DeleteTuple(&updateContext.newCtid, INVALID_SNAPSHOT, true)));
    transaction->Commit();
}

/* case desc: insert tuple and delete this tuple, lock this tuple then failed */
TEST_F(UTHeap, HeapLockFailedTest_level0)
{
    /* insert one tuple */
    ItemPointerData ctid = InsertSpecificHeapTuple(std::string(100, 'a'));
    StorageAssert(ctid != INVALID_ITEM_POINTER);

    /* delete this tuple */
    RetStatus ret = DeleteTuple(&ctid);
    StorageAssert(STORAGE_FUNC_SUCC(ret));

    /* failed to lock this tuple because this tuple has been deleted */
    Transaction *transaction = thrd->GetActiveTransaction();
    HeapLockTupleContext context;
    context.ctid = ctid;
    context.needRetTup = true;
    transaction->Start();
    transaction->SetSnapshotCsn();
    std::unique_ptr<StorageRelationData> heapRel(new StorageRelationData());
    heapRel->tableSmgr = m_utTableHandler->GetHeapTabSmgr();
    heapRel->m_pdbId = g_defaultPdbId;
    {
        HeapLockTupHandler lockTuple(g_storageInstance, thrd, heapRel.get());
        RetStatus result = lockTuple.LockUnchangedTuple(&context);
        StorageAssert(STORAGE_FUNC_FAIL(result));
        result = lockTuple.LockNewestTuple(&context);
        StorageAssert(STORAGE_FUNC_FAIL(result));
        StorageAssert(context.failureInfo.reason == HeapHandlerFailureReason::DELETED);
    }
    transaction->Commit();
}