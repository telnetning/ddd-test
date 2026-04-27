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
#include "ut_mock/ut_mock.h"
#include "ut_heap/ut_heap.h"

TEST_F(UTHeap, HeapDeleteTest_level0)
{
    int retVal = 0;

    /* Step 1: Insert one tuple */
    int natts = m_utTableHandler->GetHeapTupDesc()->natts;
    Datum values[natts];
    bool isnull[natts];
    HeapTuple *heapTuple = m_utTableHandler->GenerateRandomHeapTuple(values, isnull);
    int diskTupleLen = heapTuple->GetDiskTupleSize();
    ItemPointerData ctid = m_utTableHandler->InsertHeapTupAndCheckResult(heapTuple);

    /* Step 2: Check whether insert OK */
    HeapTuple* tuple = m_utTableHandler->FetchHeapTuple(&ctid);
    ASSERT_EQ(memcmp((void *)tuple->GetDiskTuple(), (void *)heapTuple->GetDiskTuple(), diskTupleLen), 0);
    DstorePfreeExt(tuple);

    /* Step 3: delete tuple */
    retVal = DeleteTuple(&ctid);
    ASSERT_EQ(retVal, DSTORE_SUCC);

    /* Step 4: Check whether delete OK */
    tuple = m_utTableHandler->FetchHeapTuple(&ctid);
    EXPECT_TRUE(tuple == nullptr);
}

TEST_F(UTHeap, HeapUndoDeleteTest_level0)
{
    Transaction *txn = thrd->GetActiveTransaction();
    constexpr int TUPLE_DATA_LEN = 10;
    std::string stra(TUPLE_DATA_LEN, 'a');

    /* Step 1: insert one tuple for deleting, other tuples for filling td slot */
    SnapshotData snapshot;
    ItemId originItemId;
    TD originTd;
    ItemPointerData ctid = PrepareTupleForUndo(stra, snapshot, originItemId, originTd, DEFAULT_TD_COUNT);
    HeapTuple *originTup = m_utTableHandler->FetchHeapTuple(&ctid);

    /* Step 2: delete the tuple */
    RetStatus ret = DeleteTuple(&ctid);
    ASSERT_EQ(ret, DSTORE_SUCC);

    /* Step 3: undo and check */
    txn->Start();
    txn->SetSnapshotCsnForFlashback(snapshot.GetCsn());

    /* ---- check cr page ---- */
    CheckCrPage(ctid, originTup, &originItemId, &originTd, snapshot.GetCsn() - 1);

    /* ---- check cr tuple ---- */
    CheckCrTuple(ctid, originTup);

    txn->Commit();
    DstorePfreeExt(originTup);
}

TEST_F(UTHeap, HeapBigTupDeleteTest_TIER1_level0)
{
    int retVal = 0;

    /* Construct big tuple. */
    std::string bigString;
    int bigTupleLen = 18000;
    for (int i = 0; i < bigTupleLen; i++) {
        bigString.append("a");
    }

    /* Step 1: Insert one tuple */
    HeapTuple *heapTuple = m_utTableHandler->GenerateSpecificHeapTuple(bigString);
    ItemPointerData ctid = m_utTableHandler->InsertHeapTupAndCheckResult(heapTuple);

    /* Step 2: Check whether insert OK */
    HeapTuple* tuple = m_utTableHandler->FetchHeapTuple(&ctid);
    EXPECT_EQ(memcmp((void*)tuple->GetDiskTuple()->GetData(), (void*)bigString.c_str(), bigTupleLen), 0);
    DstorePfreeExt(tuple);

    /* Step 3: Get a snapshot and save it */
    SnapshotData specialSnapshot = ConstructCurSnapshot();

    /* Step 4: delete tuple */
    retVal = DeleteTuple(&ctid);
    EXPECT_EQ(retVal, DSTORE_SUCC);

    /* Step 6: FetchTuple from special snapshot */
    tuple = m_utTableHandler->FetchHeapTuple(&ctid, &specialSnapshot);
    EXPECT_TRUE(tuple != nullptr);
    EXPECT_EQ(memcmp((void*)tuple->GetDiskTuple()->GetData(), (void*)bigString.c_str(), bigTupleLen), 0);
    DstorePfreeExt(tuple);
}

/* case desc: insert tuple and delete this tuple, delete this tuple again then failed */
TEST_F(UTHeap, HeapDeleteFailedTest_level0)
{
    /* insert one tuple */
    ItemPointerData ctid = InsertSpecificHeapTuple(std::string(100, 'a'));
    StorageAssert(ctid != INVALID_ITEM_POINTER);

    /* delete this tuple */
    RetStatus ret = DeleteTuple(&ctid);
    StorageAssert(STORAGE_FUNC_SUCC(ret));

    /* failed to delete this tuple because this tuple has been deleted */
    ret = DeleteTuple(&ctid);
    StorageAssert(STORAGE_FUNC_FAIL(ret));
}
