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
#include "ut_btree/ut_btree_build_ccindex.h"
#include "index/concurrent/dstore_btree_incomplete_index_for_ccindex.h"
#include "index/concurrent/dstore_btree_delta_index_for_ccindex.h"
#include "index/dstore_index_interface.h"

TEST_F(UTBtreeCcindexTest, BasicFuncTest_level0)
{
    /* Step 1. Preset 2 pages of data */
    int origRowNum = 300;
    int maxKey = origRowNum;
    DefaultRowDef *tableDef = static_cast<DefaultRowDef *>(DstorePalloc(origRowNum * sizeof(DefaultRowDef)));
    /* Insert (1,1)(2,2)... */
    for (int i = 1; i <= origRowNum; ++i) {
        DefaultRowDef rowDef = m_utTableHandler->GetDefaultRowDef();
        rowDef.column_int16 = static_cast<int16>(i);
        rowDef.column_int32 = static_cast<int16>(i);
        tableDef[i - 1] = rowDef;
    }
    m_utTableHandler->FillTableWithSpecificData(tableDef, origRowNum);
    DstorePfree(tableDef);

    /* Step 2. Phase1 -- create Index meta & DeltaDmlTable/Index */
    FakePhase1();
    EXPECT_NE(m_metaBuildXid, INVALID_XID);

    /* Step 3. In phase 2, preset 2 pages of delta dml data */
    int insRowNum = 0;
    int delRowNum = 0;
    for (int i = maxKey + 1; i <= maxKey + 100; i++) {
        ItemPointerData heapCtid = InsertOneInPhase2(i);
        if (i % 2 == 0) {
            DeleteOneInPhase2(i, &heapCtid);
            delRowNum++;
        }
        insRowNum++;
    }
    maxKey += insRowNum;
    UpdateLocalCsnMin();
    /* Above data should be in both btree and deltaDmlTable */
    int buildRowNum = origRowNum + insRowNum - delRowNum;
    /* Build the btree */
    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetTransactionSnapshotCsn(MAX_COMMITSEQNO);
    txn->SetTransactionSnapshotCid(FIRST_CID);
    m_ccindexBtree->BuildIndexConcurrently();
    m_btreeBuildXid = txn->GetCurrentXid();
    m_deltaTableHandler->GetIndexInfo()->btrIdxStatus = BtrCcidxStatus::WRITE_ONLY_INDEX;
    txn->Commit();

    /* Following data should be only in deltaDmlTable */
    for (int i = maxKey + 1; i <= maxKey + 100; i++) {
        ItemPointerData heapCtid = InsertOneInPhase2(i);
        if (i % 2 == 0) {
            DeleteOneInPhase2(i, &heapCtid);
            delRowNum++;
        }
        insRowNum++;
    }
    maxKey = origRowNum + insRowNum;

    /* Step 4. Check data */
    int numRecInTable = GetNumOfDataFromTable();
    EXPECT_EQ(numRecInTable, origRowNum + insRowNum - delRowNum);
    int numRecInBtree = GetNumOfDataFromIndex();
    EXPECT_EQ(numRecInBtree, buildRowNum);

    /* Step 5. Phase 3 -- merge */
    UpdateLocalCsnMin();
    /* before merge, do some concurrent DML */
    int extraInsNum = 0;
    int extraDelNum = 0;
    for (int i = maxKey + 1; i <= maxKey + 100; i++) {
        ItemPointerData heapCtid = InsertOneInPhase3(i);
        if (i % 2 == 0) {
            DeleteOneInPhase3(i, &heapCtid);
            extraDelNum++;
        }
        extraInsNum++;
    }
    /* We should see it on the btree */
    numRecInTable = GetNumOfDataFromTable();
    EXPECT_EQ(numRecInTable, origRowNum + insRowNum - delRowNum + extraInsNum - extraDelNum);
    numRecInBtree = GetNumOfDataFromIndex();
    EXPECT_EQ(numRecInBtree, buildRowNum + extraInsNum - extraDelNum);

    m_ccindexBtree->UpdateIndexRel(m_utTableHandler->GetIndexRel());
    m_ccindexBtree->UpdateBtrBuildInfo(m_utTableHandler->GetIndexBuildInfo());
    m_ccindexBtree->WaitForTrxVisibleForAll(g_defaultPdbId, m_btreeBuildXid);

    txn->Start();
    txn->SetSnapshotCsn();
    m_ccindexBtree->MergeDeltaDml(m_deltaTableHandler->GetIndexRel(), m_deltaTableHandler->GetIndexInfo());
    txn->Commit();

    numRecInTable = GetNumOfDataFromTable();
    EXPECT_EQ(numRecInTable, origRowNum + insRowNum - delRowNum + extraInsNum - extraDelNum);
    numRecInBtree = GetNumOfDataFromIndex();
    EXPECT_EQ(numRecInBtree, numRecInTable);
}

TEST_F(UTBtreeCcindexTest, DmlDuringBuildTest_level0)
{
    /* Step 1. Phase1 -- create Index meta & DeltaDmlTable/Index */
    bool uniqueIndex = true;
    FakePhase1(uniqueIndex);
    EXPECT_NE(m_metaBuildXid, INVALID_XID);

    /* Insert in phase 1: (1, 1) (2, 2) ... (20, 20) */
    int origStartVal = 1;
    int origRowNum = 20;
    int tupleCounter = origRowNum;
    ItemPointerData origHeapCtid[origRowNum];
    EXPECT_EQ(InsertInPhase1(origHeapCtid, 1, origRowNum), tupleCounter);

    /* Step 2. In phase 2, test concurrent dml that should update both btree and DeltaDmlTable */
    /* Insert some tuples: (21, 21) (23, 23) ... (29, 29) */
    int insertStartVal = origRowNum + 1;
    int insertOperTimes = 10;
    int rollbackStep = 2;
    ItemPointerData insertHeapCtid[insertOperTimes];
    int insertCounter = InsertBatchInPhase2(insertHeapCtid, insertStartVal, insertOperTimes, rollbackStep);
    tupleCounter += insertCounter;
    /* Delete some of the tuples: (3, 3) (7, 7) */
    for (int i = 0; i < origRowNum / 2; i += 2) {
        if (i % 4 == 0) {
            /* Delete then rollback */
            DeleteOneInPhase2(origStartVal + i, &origHeapCtid[i], true);
        } else {
            /* Delete then commit */
            DeleteOneInPhase2(origStartVal + i, &origHeapCtid[i]);
            origHeapCtid[i] = INVALID_ITEM_POINTER;
            tupleCounter--;
        }
    }
    /* Update some tuples: (2, 2)->(31, 31), (4, 4)->(4, 4), (6, 6)->(6, 6), (8, 8)->(34, 34), (9, 9)->(9, 9)*/
    int nextVal = origRowNum + insertOperTimes + 1;
    for (int i = 1; i < origRowNum / 2; i += 2) {
        int oldKey = origStartVal + i;
        int newKey = nextVal++;
        if ((i % 3) == 0) {
            /* Update the row but keep the same data */
            newKey = oldKey;
        }
        if ((i - 1) % 4 == 0) {
            /* Update then rollback */
            UpdateOneInPhase2(oldKey, newKey, origHeapCtid[i], true);
        } else {
            /* Update then commit */
            origHeapCtid[i] = UpdateOneInPhase2(oldKey, newKey, origHeapCtid[i]);
        }
    }

    UpdateLocalCsnMin();
    /* Above data should be in both btree and deltaDmlTable */
    int buildRowNum = tupleCounter;

    /* Step 3. Build the btree */
    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetTransactionSnapshotCsn(MAX_COMMITSEQNO);
    txn->SetTransactionSnapshotCid(FIRST_CID);
    m_ccindexBtree->BuildIndexConcurrently();
    m_btreeBuildXid = txn->GetCurrentXid();
    m_deltaTableHandler->GetIndexInfo()->btrIdxStatus = BtrCcidxStatus::WRITE_ONLY_INDEX;
    txn->Commit();
    /* Check data */
    int numRecInTable = GetNumOfDataFromTable();
    EXPECT_EQ(numRecInTable, tupleCounter);
    int numRecInBtree = GetNumOfDataFromIndex();
    EXPECT_EQ(numRecInBtree, buildRowNum);


    /* Step 4. Phase 3 -- merge */
    UpdateLocalCsnMin();
    m_ccindexBtree->UpdateIndexRel(m_utTableHandler->GetIndexRel());
    m_ccindexBtree->UpdateBtrBuildInfo(m_utTableHandler->GetIndexBuildInfo());
    m_ccindexBtree->WaitForTrxVisibleForAll(g_defaultPdbId, m_btreeBuildXid);

    txn->Start();
    txn->SetSnapshotCsn();
    m_ccindexBtree->MergeDeltaDml(m_deltaTableHandler->GetIndexRel(), m_deltaTableHandler->GetIndexInfo());
    txn->Commit();

    numRecInTable = GetNumOfDataFromTable();
    EXPECT_EQ(numRecInTable, tupleCounter);
    numRecInBtree = GetNumOfDataFromIndex();
    EXPECT_EQ(numRecInBtree, numRecInTable);
}

TEST_F(UTBtreeCcindexTest, DeltaConflictDuringBuildTest_level0)
{
    /* Step 1. Phase1 -- create Index meta & DeltaDmlTable/Index */
    bool uniqueIndex = true;
    FakePhase1(uniqueIndex);
    EXPECT_NE(m_metaBuildXid, INVALID_XID);

    /* Insert in phase 1: (1, 1) (2, 2) ... (20, 20) */
    int origStartVal = 1;
    int origRowNum = 20;
    int tupleCounter = origRowNum;
    ItemPointerData origHeapCtid[origRowNum];
    EXPECT_EQ(InsertInPhase1(origHeapCtid, 1, origRowNum), tupleCounter);

    /* Step 2. In phase 2, test concurrent dml that should update both btree and DeltaDmlTable */
    /* Insert some tuples: (21, 21) (23, 23) ... (29, 29) */
    int insertStartVal = origRowNum + 1;
    int insertOperTimes = 10;
    int rollbackStep = 2;
    ItemPointerData insertHeapCtid[insertOperTimes];
    int insertCounter = InsertBatchInPhase2(insertHeapCtid, insertStartVal, insertOperTimes, rollbackStep);
    tupleCounter += insertCounter;
    /* Update some tuples: (2, 2)->(31, 31), (4, 4)->(4, 4), (6, 6)->(6, 6), (8, 8)->(34, 34), (9, 9)->(9, 9)*/
    int nextVal = origRowNum + insertOperTimes + 1;
    for (int i = 1; i < origRowNum / 2; i += 2) {
        int oldKey = origStartVal + i;
        int newKey = nextVal++;
        if ((i % 3) == 0) {
            /* Update the row but keep the same data */
            newKey = oldKey;
        }
        if ((i - 1) % 4 == 0) {
            /* Update then rollback */
            UpdateOneInPhase2(oldKey, newKey, origHeapCtid[i], true);
        } else {
            /* Update then commit */
            origHeapCtid[i] = UpdateOneInPhase2(oldKey, newKey, origHeapCtid[i]);
        }
    }

    UpdateLocalCsnMin();
    /* Above data should be in both btree and deltaDmlTable */
    int buildRowNum = tupleCounter;

    /* Step 3. Build the btree */
    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetTransactionSnapshotCsn(MAX_COMMITSEQNO);
    txn->SetTransactionSnapshotCid(FIRST_CID);
    m_ccindexBtree->BuildIndexConcurrently();
    m_btreeBuildXid = txn->GetCurrentXid();
    m_deltaTableHandler->GetIndexInfo()->btrIdxStatus = BtrCcidxStatus::WRITE_ONLY_INDEX;
    txn->Commit();
    /* Check data */
    int numRecInTable = GetNumOfDataFromTable();
    EXPECT_EQ(numRecInTable, tupleCounter);
    int numRecInBtree = GetNumOfDataFromIndex();
    EXPECT_EQ(numRecInBtree, buildRowNum);


    /* Step 4. In phase 2, test concurrent dml that should update only DeltaDmlTable */
    /* Following data should be only in deltaDmlTable */
    /* Insert some tuples: (36, 36) (37, 37) (38, 38) (40, 40) (41, 41) (42, 42) (44, 44) (45, 45) */
    insertStartVal = nextVal;
    rollbackStep = 4;
    ItemPointerData insertHeapCtidInDeml[insertOperTimes];
    insertCounter = InsertBatchInPhase2(insertHeapCtidInDeml, insertStartVal, insertOperTimes, rollbackStep);
    tupleCounter += insertCounter;
    /* Delete some of the tuples: (11, 11) (15, 15) (19, 19) */
    /* Update some tuples:
     * (41, 41)->(48, 48), (45, 45)->(45, 45) */
    nextVal += insertOperTimes;
    for (int i = 1; i < insertOperTimes; i += 2) {
        int oldKey = insertStartVal + i;
        int newKey = nextVal++;
        if (insertHeapCtidInDeml[i] == INVALID_ITEM_POINTER) {
            continue;
        }
        if ((i % 3) == 0) {
            /* Update the row but keep the same data */
            newKey = oldKey;
        }
        if (i % 5 == 0) {
            /* Update then commit */
            UpdateOneInPhase2(oldKey, newKey, insertHeapCtidInDeml[i]);
        } else {
            /* Update then rollback */
            insertHeapCtidInDeml[i] = UpdateOneInPhase2(oldKey, newKey, insertHeapCtidInDeml[i], true);
        }
    }
    numRecInTable = GetNumOfDataFromTable();
    EXPECT_EQ(numRecInTable, tupleCounter);
    numRecInBtree = GetNumOfDataFromIndex();
    EXPECT_EQ(numRecInBtree, buildRowNum);

    /* Step 5. Phase 3 -- merge */
    UpdateLocalCsnMin();
    m_ccindexBtree->UpdateIndexRel(m_utTableHandler->GetIndexRel());
    m_ccindexBtree->UpdateBtrBuildInfo(m_utTableHandler->GetIndexBuildInfo());
    m_ccindexBtree->WaitForTrxVisibleForAll(g_defaultPdbId, m_btreeBuildXid);

    txn->Start();
    txn->SetSnapshotCsn();
    m_ccindexBtree->MergeDeltaDml(m_deltaTableHandler->GetIndexRel(), m_deltaTableHandler->GetIndexInfo());
    txn->Commit();

    numRecInTable = GetNumOfDataFromTable();
    EXPECT_EQ(numRecInTable, tupleCounter);
    numRecInBtree = GetNumOfDataFromIndex();
    EXPECT_EQ(numRecInBtree, numRecInTable);
}

TEST_F(UTBtreeCcindexTest, DuplicateConflictTest_level0)
{
    /* Step 1. Phase1 -- create Index meta & DeltaDmlTable/Index */
    bool uniqueIndex = false;
    FakePhase1(uniqueIndex);
    EXPECT_NE(m_metaBuildXid, INVALID_XID);

    /* Insert in phase 1: all tuples have the save key value = 1 */
    int keyVal = 1;
    int origRowNum = 400;
    int tupleCounter = origRowNum;
    ItemPointerData heapCtid[origRowNum];
    EXPECT_EQ(InsertInPhase1(heapCtid, 1, origRowNum, 0, 0), tupleCounter);

    /* Step 2. In phase 2, test concurrent dml that should update both btree and DeltaDmlTable */
    /* Insert some tuples */
    int insertOperTimes = 400;
    int rollbackStep = 2;
    ItemPointerData insertHeapCtid[insertOperTimes];
    int insertCounter = InsertBatchInPhase2(insertHeapCtid, keyVal, insertOperTimes, rollbackStep, 0);
    tupleCounter += insertCounter;
    /* Update some tuples */
    rollbackStep = 4;
    for (int i = 0; i < insertOperTimes / 2; i += 2) {
        if (insertHeapCtid[i] == INVALID_ITEM_POINTER) {
            continue;
        }
        if (i % rollbackStep == 0) {
            /* Update then rollback */
            UpdateOneInPhase2(keyVal, keyVal, insertHeapCtid[i], true);
        } else {
            /* Update then commit */
            insertHeapCtid[i] = UpdateOneInPhase2(keyVal, keyVal, insertHeapCtid[i]);
        }
    }

    UpdateLocalCsnMin();
    /* Above data should be in both btree and deltaDmlTable */
    int buildRowNum = tupleCounter;

    /* Step 3. Build the btree */
    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetTransactionSnapshotCsn(MAX_COMMITSEQNO);
    txn->SetTransactionSnapshotCid(FIRST_CID);
    m_ccindexBtree->BuildIndexConcurrently();
    m_btreeBuildXid = txn->GetCurrentXid();
    m_deltaTableHandler->GetIndexInfo()->btrIdxStatus = BtrCcidxStatus::WRITE_ONLY_INDEX;
    txn->Commit();
    /* Check data */
    int numRecInTable = GetNumOfDataFromTable();
    EXPECT_EQ(numRecInTable, tupleCounter);
    int numRecInBtree = GetNumOfDataFromIndex();
    EXPECT_EQ(numRecInBtree, buildRowNum);


    /* Step 4. In phase 2, test concurrent dml that should update only DeltaDmlTable */
    /* Update some tuples */
    rollbackStep = 5;
    ItemPointerData tempHeapCtid;
    for (int i = 0; i < origRowNum; i++) {
        if (heapCtid[i] == INVALID_ITEM_POINTER) {
            continue;
        }
        if (i % rollbackStep == 0) {
            /* Update then rollback */
            tempHeapCtid = UpdateOneInPhase2(keyVal, keyVal, heapCtid[i], true);
            StorageAssert(tempHeapCtid == INVALID_ITEM_POINTER);
        } else {
            /* Update then commit */
            heapCtid[i] = UpdateOneInPhase2(keyVal, keyVal, heapCtid[i]);
        }
    }
    numRecInTable = GetNumOfDataFromTable();
    EXPECT_EQ(numRecInTable, tupleCounter);
    numRecInBtree = GetNumOfDataFromIndex();
    EXPECT_EQ(numRecInBtree, buildRowNum);

    /* Step 5. Phase 3 -- merge */
    UpdateLocalCsnMin();
    m_ccindexBtree->UpdateIndexRel(m_utTableHandler->GetIndexRel());
    m_ccindexBtree->UpdateBtrBuildInfo(m_utTableHandler->GetIndexBuildInfo());
    m_ccindexBtree->WaitForTrxVisibleForAll(g_defaultPdbId, m_btreeBuildXid);

    /* Delete all inserted tuples */
    rollbackStep = 4;
    ItemPointerData deletedInsertion[insertOperTimes];
    for (int i = 0; i < insertOperTimes; i++) {
        if (insertHeapCtid[i] == INVALID_ITEM_POINTER) {
            continue;
        }
        if (i % rollbackStep == 0) {
            DeleteOneInPhase3(keyVal, &insertHeapCtid[i], true); /* Rollback every 4 deletions */
        } else {
            DeleteOneInPhase3(keyVal, &insertHeapCtid[i]);
            tupleCounter--;
        }
        deletedInsertion[i] = insertHeapCtid[i];
    }

    /* Delete all original tuples */
    ItemPointerData deletedOrig[origRowNum];
    for (int i = 0; i < origRowNum; i++) {
        if (heapCtid[i] == INVALID_ITEM_POINTER) {
            continue;
        }
        if (i % rollbackStep == 0) {
            DeleteOneInPhase3(keyVal, &heapCtid[i], true); /* Rollback every 4 deletions */
        } else {
            DeleteOneInPhase3(keyVal, &heapCtid[i]);
            tupleCounter--;
        }
        deletedOrig[i] = heapCtid[i];
    }

    txn->Start();
    txn->SetSnapshotCsn();
    m_ccindexBtree->MergeDeltaDml(m_deltaTableHandler->GetIndexRel(), m_deltaTableHandler->GetIndexInfo());
    txn->Commit();
    UpdateLocalCsnMin();

    numRecInTable = GetNumOfDataFromTable();
    EXPECT_EQ(numRecInTable, tupleCounter);
    numRecInBtree = GetNumOfDataFromIndex();
    EXPECT_EQ(numRecInBtree, numRecInTable);
}

TEST_F(UTBtreeCcindexTest, UniqueIndexTest_level0)
{
    /* Step 1. Phase1 -- create Index meta & DeltaDmlTable/Index */
    bool uniqueIndex = true;
    FakePhase1(uniqueIndex);
    EXPECT_NE(m_metaBuildXid, INVALID_XID);

    /* Insert in phase 1: (1, 1) (2, 2) ... (20, 20) */
    int origStartVal = 1;
    int origRowNum = 20;
    ItemPointerData origHeapCtid[origRowNum];
    EXPECT_EQ(InsertInPhase1(origHeapCtid, 1, origRowNum), origRowNum);

    UpdateLocalCsnMin();
    /* Above data should be in both btree and deltaDmlTable */

    /* Step 2. Build the btree */
    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetTransactionSnapshotCsn(MAX_COMMITSEQNO);
    txn->SetTransactionSnapshotCid(FIRST_CID);
    m_ccindexBtree->BuildIndexConcurrently();
    m_btreeBuildXid = txn->GetCurrentXid();
    m_deltaTableHandler->GetIndexInfo()->btrIdxStatus = BtrCcidxStatus::WRITE_ONLY_INDEX;
    txn->Commit();
    /* Check data */
    int numRecInTable = GetNumOfDataFromTable();
    EXPECT_EQ(numRecInTable, origRowNum);
    int numRecInBtree = GetNumOfDataFromIndex();
    EXPECT_EQ(numRecInBtree, origRowNum);


    /* Step 3. In phase 2, test concurrent dml that should update only DeltaDmlTable */
    /* Following data should be only in deltaDmlTable */
    /* Delete all from orig table */
    ItemPointerData deleteItem = INVALID_ITEM_POINTER;
    for (int i = 0; i < origRowNum; i++) {
        deleteItem = origHeapCtid[i];
        DeleteOneInPhase2(origStartVal + i, &deleteItem);
        StorageAssert(deleteItem == INVALID_ITEM_POINTER);
    }
    /* Insert 2 tuples with deleted-duplicate key values: (1, 1) x 10, (11, 11), (12, 12), ... (20, 20) */
    int insertVal = origStartVal;
    int insertOperTimes = 20;
    int rollbackStep = 0;
    ItemPointerData insertHeapCtidInDml[insertOperTimes];
    InsertBatchInPhase2(insertHeapCtidInDml, insertVal, insertOperTimes / 2, rollbackStep, 0);
    InsertBatchInPhase2(&insertHeapCtidInDml[insertOperTimes / 2], insertVal + 1,
                        insertOperTimes / 2, rollbackStep);
    /* Then delete them */
    for (int i = 0; i < insertOperTimes / 2; i++) {
        deleteItem = insertHeapCtidInDml[i];
        DeleteOneInPhase2(insertVal, &deleteItem);
        StorageAssert(deleteItem == INVALID_ITEM_POINTER);
    }
    for (int i = insertOperTimes / 2; i < insertOperTimes; i++) {
        deleteItem = insertHeapCtidInDml[i];
        DeleteOneInPhase2(i, &deleteItem);
        StorageAssert(deleteItem == INVALID_ITEM_POINTER);
    }

    /* Step 4. Phase 3 -- concurrent DML writing index */
    UpdateLocalCsnMin();
    m_ccindexBtree->UpdateIndexRel(m_utTableHandler->GetIndexRel());
    m_ccindexBtree->UpdateBtrBuildInfo(m_utTableHandler->GetIndexBuildInfo());
    m_ccindexBtree->WaitForTrxVisibleForAll(g_defaultPdbId, m_btreeBuildXid);
    ItemPointerData insertHeapCtidInIndex[insertOperTimes];
    for (int i = 0; i < insertOperTimes; i++) {
        insertHeapCtidInIndex[i] = InsertOneInPhase3(insertVal + i);
        EXPECT_NE(insertHeapCtidInIndex[i], INVALID_ITEM_POINTER);
    }

    for (int i = 0; i < insertOperTimes; i++) {
        insertHeapCtidInIndex[i] = UpdateOneInPhase3(insertVal + i, insertVal + i, insertHeapCtidInIndex[i]);
        EXPECT_EQ(insertHeapCtidInIndex[i], INVALID_ITEM_POINTER);
    }

    /* Step 5. Phase 3 -- merge */
    txn->Start();
    txn->SetSnapshotCsn();
    m_ccindexBtree->MergeDeltaDml(m_deltaTableHandler->GetIndexRel(), m_deltaTableHandler->GetIndexInfo());
    txn->Commit();

    numRecInTable = GetNumOfDataFromTable();
    EXPECT_EQ(numRecInTable, origRowNum);
    numRecInBtree = GetNumOfDataFromIndex();
    EXPECT_EQ(numRecInBtree, numRecInTable);
}

TEST_F(UTBtreeCcindexTest, EraseIns4DelTest_level0)
{
    /* Step 1. Phase1 -- create Index meta & DeltaDmlTable/Index */
    bool uniqueIndex = false;
    FakePhase1(uniqueIndex);
    EXPECT_NE(m_metaBuildXid, INVALID_XID);

    /* Insert in phase 1: (1, 1) (2, 2) ... (20, 20) */
    int origStartVal = 1;
    int origRowNum = 20;
    int tupleCounter = origRowNum;
    ItemPointerData origHeapCtid[origRowNum];
    EXPECT_EQ(InsertInPhase1(origHeapCtid, 1, origRowNum), origRowNum);

    UpdateLocalCsnMin();
    /* Above data should be in both btree and deltaDmlTable */

    /* Step 2. Build the btree */
    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetTransactionSnapshotCsn(MAX_COMMITSEQNO);
    txn->SetTransactionSnapshotCid(FIRST_CID);
    m_ccindexBtree->BuildIndexConcurrently();
    m_btreeBuildXid = txn->GetCurrentXid();
    m_deltaTableHandler->GetIndexInfo()->btrIdxStatus = BtrCcidxStatus::WRITE_ONLY_INDEX;
    txn->Commit();
    /* Check data */
    int numRecInTable = GetNumOfDataFromTable();
    EXPECT_EQ(numRecInTable, origRowNum);
    int numRecInBtree = GetNumOfDataFromIndex();
    EXPECT_EQ(numRecInBtree, origRowNum);

    /* Step 3. In phase 2, concurrent dml should update only DeltaDmlTable */
    /* Following data should be only in deltaDmlTable */
    int insertVal = origStartVal;
    int insertOperTimes = 20;
    tupleCounter += insertOperTimes;
    ItemPointerData insertHeapCtidInDml[insertOperTimes];
    InsertBatchInPhase2(insertHeapCtidInDml, insertVal, insertOperTimes);

    /* Step 4. Phase 3 -- concurrent DML writing index */
    UpdateLocalCsnMin();
    m_ccindexBtree->UpdateIndexRel(m_utTableHandler->GetIndexRel());
    m_ccindexBtree->UpdateBtrBuildInfo(m_utTableHandler->GetIndexBuildInfo());
    m_ccindexBtree->WaitForTrxVisibleForAll(g_defaultPdbId, m_btreeBuildXid);
    /* Delete all in step 3. Phase 3 should write index directly, do insert4delete */
    ItemPointerData insertHeapCtidInIndex[insertOperTimes];
    for (int i = 0; i < insertOperTimes; i++) {
        if (i % 2 == 0) {
            DeleteOneInPhase3(insertVal + i, &insertHeapCtidInDml[i], true);
        } else {
            DeleteOneInPhase3(insertVal + i, &insertHeapCtidInDml[i]);
            tupleCounter--;
        }
    }

    /* Step 5. Phase 3 -- merge */
    txn->Start();
    txn->SetSnapshotCsn();
    /* The insertion merge should insert nothing into the index but erase the ins4del flags */
    m_ccindexBtree->MergeDeltaDml(m_deltaTableHandler->GetIndexRel(), m_deltaTableHandler->GetIndexInfo());
    txn->Commit();

    numRecInTable = GetNumOfDataFromTable();
    EXPECT_EQ(numRecInTable, tupleCounter);
    numRecInBtree = GetNumOfDataFromIndex();
    EXPECT_EQ(numRecInBtree, numRecInTable);

    txn->Start();
    BtreePagePayload leafPage;
    leafPage.Init(g_defaultPdbId, m_utTableHandler->GetBtreeSmgr()->GetLowestSinglePageIdFromMetaCache(), DSTORE::LW_SHARED,
                  g_storageInstance->GetBufferMgr());
    IndexTuple *checkTuple = nullptr;
    for (int i = 1; i <= leafPage.GetPage()->GetMaxOffset(); i++) {
        checkTuple = leafPage.GetPage()->GetIndexTuple(i);
        ItemPointerData checkHeapCtid = checkTuple->GetHeapCtid();
        bool found = false;
        for (int j = 0; j < origRowNum; j++) {
            if (checkHeapCtid == origHeapCtid[j]) {
                EXPECT_FALSE(checkTuple->IsInsertDeletedForCCindex());
                EXPECT_FALSE(checkTuple->IsDeleted());
                found = true;
                break;
            }
        }
        if (found) {
            continue;
        }
        for (int j = 0; j < insertOperTimes; j += 2) {
            if (checkHeapCtid == insertHeapCtidInDml[j]) {
                EXPECT_FALSE(checkTuple->IsInsertDeletedForCCindex());
                EXPECT_FALSE(checkTuple->IsDeleted());
                EXPECT_TRUE(checkTuple->TestTdStatus(TupleTdStatus::DETACH_TD));
                found = true;
                break;
            }
        }
        if (found) {
            continue;
        }
        EXPECT_FALSE(checkTuple->IsInsertDeletedForCCindex());
        EXPECT_TRUE(checkTuple->IsDeleted());
    }
    leafPage.Drop(g_storageInstance->GetBufferMgr());
    txn->Commit();
}

TEST_F(UTBtreeCcindexTest, UpdateDmlTest_level0)
{
    /* Step 1. Phase1 -- create Index meta & DeltaDmlTable/Index */
    bool uniqueIndex = true;
    FakePhase1(uniqueIndex);
    EXPECT_NE(m_metaBuildXid, INVALID_XID);

    /* Insert in phase 1: (1, 1) (2, 2) ... (20, 20) */
    int keyVal = 1;
    int origRowNum = 1;
    ItemPointerData origHeapCtid[origRowNum];
    EXPECT_EQ(InsertInPhase1(origHeapCtid, keyVal, origRowNum), origRowNum);

    UpdateLocalCsnMin();
    /* Above data should be in both btree and deltaDmlTable */

    /* Step 2. Build the btree */
    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetTransactionSnapshotCsn(MAX_COMMITSEQNO);
    txn->SetTransactionSnapshotCid(FIRST_CID);
    m_ccindexBtree->BuildIndexConcurrently();
    m_btreeBuildXid = txn->GetCurrentXid();
    m_deltaTableHandler->GetIndexInfo()->btrIdxStatus = BtrCcidxStatus::WRITE_ONLY_INDEX;
    txn->Commit();
    /* Check data */
    int numRecInTable = GetNumOfDataFromTable();
    EXPECT_EQ(numRecInTable, origRowNum);
    int numRecInBtree = GetNumOfDataFromIndex();
    EXPECT_EQ(numRecInBtree, origRowNum);

    /* Step 3. In phase 2, test concurrent dml that should update only DeltaDmlTable */
    /* Following data should be only in deltaDmlTable */
    /* Update all from orig table */
    for (int i = 0; i < 200; i++) {
        origHeapCtid[0] = UpdateOneInPhase2(keyVal, keyVal, origHeapCtid[0]);
    }

    /* Step 4. Phase 3 -- merge */
    UpdateLocalCsnMin();
    m_ccindexBtree->UpdateIndexRel(m_utTableHandler->GetIndexRel());
    m_ccindexBtree->UpdateBtrBuildInfo(m_utTableHandler->GetIndexBuildInfo());
    m_ccindexBtree->WaitForTrxVisibleForAll(g_defaultPdbId, m_btreeBuildXid);
    txn->Start();
    txn->SetSnapshotCsn();
    m_ccindexBtree->MergeDeltaDml(m_deltaTableHandler->GetIndexRel(), m_deltaTableHandler->GetIndexInfo());
    txn->Commit();

    numRecInTable = GetNumOfDataFromTable();
    EXPECT_EQ(numRecInTable, origRowNum);
    numRecInBtree = GetNumOfDataFromIndex();
    EXPECT_EQ(numRecInBtree, numRecInTable);
}

void UTBtreeCcindexTest::FakePhase1(bool targetIndexInique)
{
    /* Target index */
    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetSnapshotCsn();
    EXPECT_EQ(txn->AllocTransactionSlot(), DSTORE_SUCC);
    m_metaBuildXid = thrd->GetCurrentXid();
    int indexCols[] = {1, 2};
    bool isUnique = targetIndexInique;
    int keyAttrNum = sizeof(indexCols) / sizeof(int);
    m_utTableHandler->CreateBtreeContext(indexCols, keyAttrNum, isUnique);
    m_utTableHandler->SetCcindexBtrStatus(BtrCcidxStatus::IN_BUILDING_INDEX);
    m_ccindexBtree = CcindexBtrBuildHandler::Create(m_utTableHandler->GetIndexRel(),
                                                    m_utTableHandler->GetIndexBuildInfo(),
                                                    m_utTableHandler->GetIndexScanKey(), false);

    /* Dml table and index */
    int deltaNattr = keyAttrNum + 2;
    Oid attTypePidList[] = {INT8OID, INT2OID, INT4OID, INT2OID};
    m_deltaTableHandler = UTTableHandler::CreateTableHandler(
        g_defaultPdbId, m_ut_memory_context, false, false, deltaNattr, attTypePidList);
    int *tmpIndexCols = static_cast<int *>(DstorePalloc(deltaNattr * sizeof(int)));
    for (int i = 0; i < deltaNattr; i++) {
        tmpIndexCols[i] = i;
    }
    m_deltaIndexCols = tmpIndexCols;
    m_deltaAttrNum = deltaNattr;
    m_deltaTableHandler->CreateBtreeContext(m_deltaIndexCols, m_deltaAttrNum - 1, true,
                                            BTREE_DEFAULT_FILLFACTOR, m_deltaAttrNum);
    BtreeBuild btreeBuild(m_deltaTableHandler->GetIndexRel(), m_deltaTableHandler->GetIndexBuildInfo(),
                          m_deltaTableHandler->GetIndexScanKey());
    EXPECT_EQ(btreeBuild.BuildIndex(), DSTORE_SUCC);
    txn->Commit();
}

int UTBtreeCcindexTest::InsertInPhase1(ItemPointer heapCtid, int startVal, int numOper, int rollbackStep, int valStep)
{
    Transaction *txn = thrd->GetActiveTransaction();
    int tableTupCounter = 0;
    bool needRollback = false;
    for (int val = startVal, counter = 0; counter < numOper; val += valStep) {
        needRollback = (rollbackStep != 0 && ((counter) % rollbackStep == 0));
        txn->Start();
        txn->SetSnapshotCsn();
        heapCtid[counter] = InsertHeapTupleIntoTargetTable(val);
        if (needRollback) {
            txn->Abort();
            heapCtid[counter] = INVALID_ITEM_POINTER;
        } else {
            txn->Commit();
            tableTupCounter++;
        }
        counter++;
    }
    return tableTupCounter;
}

ItemPointerData UTBtreeCcindexTest::InsertOneInPhase2(int keyVal, bool needRollback)
{
    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetSnapshotCsn();
    /* Insert into target table */
    ItemPointerData targetHeapCtid = InsertHeapTupleIntoTargetTable(keyVal);

    /* Insert into DeltaDmlTable */
    ItemPointerData deltaHeapCtid = InsertRecordIntoDeltaTable(keyVal, &targetHeapCtid,
                                                               DmlOperationTypeForCcindex::DML_OPERATION_INSERT);

    /* Insert into DeltaDmlIndex */
    RetStatus ret = UpdateDeltaIndex(keyVal, &targetHeapCtid, &deltaHeapCtid,
                                     DmlOperationTypeForCcindex::DML_OPERATION_INSERT);
    EXPECT_EQ(ret, DSTORE_SUCC);

    if (needRollback) {
        txn->Abort();
        return INVALID_ITEM_POINTER;
    }
    txn->Commit();
    ErrLog(DSTORE_DEBUG1, MODULE_INDEX, ErrMsg("[UT] InsertOneInPhase2 insert {%hu, %u} %u, %s",
        targetHeapCtid.GetFileId(), targetHeapCtid.GetBlockNum(), targetHeapCtid.GetOffset(),
        needRollback ? "Abort" : "Commit"));
    return targetHeapCtid;
}

int UTBtreeCcindexTest::InsertBatchInPhase2(ItemPointer heapCtid, int startVal, int numOper,
                                            int rollbackStep, int valStep)
{
    int insertCounter = 0;
    int tableTupleCounter = 0;
    for (int i = 0; i < numOper; i++) {
        if (rollbackStep != 0 && ++insertCounter % rollbackStep == 0) {
            /* Need to rollback */
            InsertOneInPhase2(startVal + (i * valStep), true);
            heapCtid[i] = INVALID_ITEM_POINTER;
        } else {
            heapCtid[i] = InsertOneInPhase2(startVal + (i * valStep));
            tableTupleCounter++;
        }
    }
    return tableTupleCounter;
}

void UTBtreeCcindexTest::DeleteOneInPhase2(int keyVal, ItemPointer heapCtid, bool needRollback)
{
    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetSnapshotCsn();
    /* Delete from target table */
    DeleteHeapTupleFromTargetTable(heapCtid);

    /* Insert Delete record info DeltaTable */
    ItemPointerData deltaHeapCtid = InsertRecordIntoDeltaTable(keyVal, heapCtid,
                                                               DmlOperationTypeForCcindex::DML_OPERATION_DELETE);

    /* Insert Deleting record into DeltaDmlIndex */
    RetStatus ret = UpdateDeltaIndex(keyVal, heapCtid, &deltaHeapCtid,
                                     DmlOperationTypeForCcindex::DML_OPERATION_DELETE);
    StorageAssert(ret == DSTORE_SUCC);
    ErrLog(DSTORE_DEBUG1, MODULE_INDEX, ErrMsg("[UT] DeleteOneInPhase2 delete {%hu, %u} %u, %s",
        heapCtid->GetFileId(), heapCtid->GetBlockNum(), heapCtid->GetOffset(), needRollback ? "Abort" : "Commit"));
    if (needRollback) {
        txn->Abort();
    } else {
        txn->Commit();
        *heapCtid = INVALID_ITEM_POINTER;
    }
}

ItemPointerData UTBtreeCcindexTest::UpdateOneInPhase2(int oldVal, int newVal, ItemPointerData oldHeapCtid,
                                    bool needRollback)
{
    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetSnapshotCsn();
    /* Update target table */
    ItemPointerData newHeapCtid = UpdateHeapTupleFromTargetTable(newVal, oldHeapCtid);

    /* Insert Delete record of old data info DeltaTable */
    ItemPointerData oldDeltaCtid = InsertRecordIntoDeltaTable(oldVal, &oldHeapCtid,
                                                              DmlOperationTypeForCcindex::DML_OPERATION_DELETE);
    /* Insert Deleting record of old data into DeltaDmlIndex */
    RetStatus ret = UpdateDeltaIndex(oldVal, &oldHeapCtid, &oldDeltaCtid,
                                     DmlOperationTypeForCcindex::DML_OPERATION_DELETE);
    StorageAssert(ret == DSTORE_SUCC);

    /* Insert record of new data info DeltaTable */
    ItemPointerData newDeltaCtid = InsertRecordIntoDeltaTable(newVal, &newHeapCtid,
                                                              DmlOperationTypeForCcindex::DML_OPERATION_INSERT);
    /* Insert Deleting record of old data into DeltaDmlIndex */
    ret = UpdateDeltaIndex(newVal, &newHeapCtid, &newDeltaCtid, DmlOperationTypeForCcindex::DML_OPERATION_INSERT);
    StorageAssert(ret == DSTORE_SUCC);

    if (needRollback) {
        txn->Abort();
        return INVALID_ITEM_POINTER;
    }
    txn->Commit();
    ErrLog(DSTORE_DEBUG1, MODULE_INDEX, ErrMsg("[UT] UpdateOneInPhase2 old {%hu, %u} %u, new {%hu, %u} %u, %s",
        oldHeapCtid.GetFileId(), oldHeapCtid.GetBlockNum(), oldHeapCtid.GetOffset(),
        newHeapCtid.GetFileId(), newHeapCtid.GetBlockNum(), newHeapCtid.GetOffset(),
        needRollback ? "Abort" : "Commit"));
    return newHeapCtid;
}

ItemPointerData UTBtreeCcindexTest::InsertOneInPhase3(int keyVal, bool isUnique, bool needRollback)
{
    StorageAssert(m_utTableHandler->GetIndexInfo()->btrIdxStatus == BtrCcidxStatus::WRITE_ONLY_INDEX);

    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetSnapshotCsn();
    /* Insert into target table */
    ItemPointerData targetHeapCtid = InsertHeapTupleIntoTargetTable(keyVal);

    /* Insert into the incomplete btree */
    int indexAttrNum = m_utTableHandler->GetIndexInfo()->indexAttrsNum;
    Datum idxValues[indexAttrNum];
    bool idxIsnulls[indexAttrNum];
    GetTargetIndexDatum(idxValues, idxIsnulls, keyVal);
    bool satisfiesUnique = true;
    IncompleteBtreeInsertForCcindex btrInsert(m_utTableHandler->GetIndexRel(), m_utTableHandler->GetIndexInfo(),
                                              m_utTableHandler->GetIndexScanKey());
    RetStatus ret = btrInsert.BtreeInsert::InsertTuple(idxValues, idxIsnulls, &targetHeapCtid, &satisfiesUnique);
    if (!satisfiesUnique) {
        ItemPointerData dupHeapCtid = ItemPointerData(DatumGetUInt64(m_utTableHandler->GetIndexInfo()->extraInfo));
        StorageAssert(ret == DSTORE_SUCC);
        Datum deltaIdxValues[m_deltaAttrNum];
        bool deltaIdxIsnulls[m_deltaAttrNum];
        GetDeltaDatum(deltaIdxValues, deltaIdxIsnulls, keyVal, &dupHeapCtid,
                      DmlOperationTypeForCcindex::DML_OPERATION_DELETE);
        ItemPointerData deltaRec = INVALID_ITEM_POINTER;
        ret = BtreeDeltaDmlForCcindex::CheckExistence({m_deltaTableHandler->GetIndexRel(),
            m_deltaTableHandler->GetIndexInfo(), m_deltaTableHandler->GetIndexScanKey()},
            deltaIdxValues, deltaIdxIsnulls, &deltaRec);
        StorageAssert(ret == DSTORE_SUCC);
        if (deltaRec != INVALID_ITEM_POINTER) {
            satisfiesUnique = true;
        }
    }
    ErrLog(DSTORE_DEBUG1, MODULE_INDEX, ErrMsg("[UT] InsertOneInPhase3 insert {%hu, %u} %u, %s",
        targetHeapCtid.GetFileId(), targetHeapCtid.GetBlockNum(), targetHeapCtid.GetOffset(),
        needRollback ? "Abort" : "Commit"));
    EXPECT_EQ(satisfiesUnique, isUnique);
    if (!satisfiesUnique || needRollback) {
        txn->Abort();
        targetHeapCtid = INVALID_ITEM_POINTER;
    } else {
        txn->Commit();
    }
    return targetHeapCtid;
}

void UTBtreeCcindexTest::DeleteOneInPhase3(int keyVal, ItemPointer heapCtid, bool needRollback)
{
    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetSnapshotCsn();
    /* Delete from target table */
    DeleteHeapTupleFromTargetTable(heapCtid);

    /* Delete from the incomplete btree */
    int indexAttrNum = m_utTableHandler->GetIndexInfo()->indexAttrsNum;
    Datum idxValues[indexAttrNum];
    bool idxIsnulls[indexAttrNum];
    GetTargetIndexDatum(idxValues, idxIsnulls, keyVal);
    bool satisfiesUnique = true;
    IncompleteBtreeDeleteForCcindex btrDelete(m_utTableHandler->GetIndexRel(), m_utTableHandler->GetIndexInfo(),
                                              m_utTableHandler->GetIndexScanKey());
    RetStatus ret = btrDelete.DeleteTuple(idxValues, idxIsnulls, heapCtid);
    StorageAssert(ret == DSTORE_SUCC);
    ErrLog(DSTORE_DEBUG1, MODULE_INDEX, ErrMsg("[UT] DeleteOneInPhase3 delete {%hu, %u} %u, %s",
        heapCtid->GetFileId(), heapCtid->GetBlockNum(), heapCtid->GetOffset(), needRollback ? "Abort" : "Commit"));
    if (needRollback) {
        txn->Abort();
    } else {
        txn->Commit();
        *heapCtid = INVALID_ITEM_POINTER;
    }
}

ItemPointerData UTBtreeCcindexTest::UpdateOneInPhase3(int oldVal, int newVal, ItemPointerData oldHeapCtid,
                                    bool needRollback)
{
    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetSnapshotCsn();
    /* Update target table */
    ItemPointerData newHeapCtid = UpdateHeapTupleFromTargetTable(newVal, oldHeapCtid);

    /* Delete from the incomplete btree */
    int indexAttrNum = m_utTableHandler->GetIndexInfo()->indexAttrsNum;
    Datum idxValues[indexAttrNum];
    bool idxIsnulls[indexAttrNum];
    GetTargetIndexDatum(idxValues, idxIsnulls, oldVal);
    IncompleteBtreeDeleteForCcindex btrDelete(m_utTableHandler->GetIndexRel(), m_utTableHandler->GetIndexInfo(),
                                              m_utTableHandler->GetIndexScanKey());
    RetStatus ret = btrDelete.DeleteTuple(idxValues, idxIsnulls, &oldHeapCtid);
    StorageAssert(ret == DSTORE_SUCC);

    /* Insert into the incomplete btree */
    bool satisfiesUnique = true;
    GetTargetIndexDatum(idxValues, idxIsnulls, newVal);
    IncompleteBtreeInsertForCcindex btrInsert(m_utTableHandler->GetIndexRel(), m_utTableHandler->GetIndexInfo(),
                                              m_utTableHandler->GetIndexScanKey());
    ret = btrInsert.BtreeInsert::InsertTuple(idxValues, idxIsnulls, &newHeapCtid, &satisfiesUnique);
    if (!satisfiesUnique) {
        StorageAssert(ret == DSTORE_SUCC);
        Datum deltaIdxValues[m_deltaAttrNum];
        bool deltaIdxIsnulls[m_deltaAttrNum];
        GetDeltaDatum(deltaIdxValues, deltaIdxIsnulls, newVal, &newHeapCtid,
                      DmlOperationTypeForCcindex::DML_OPERATION_DELETE);
        ItemPointerData deltaRec = INVALID_ITEM_POINTER;
        ret = BtreeDeltaDmlForCcindex::CheckExistence({m_deltaTableHandler->GetIndexRel(),
            m_deltaTableHandler->GetIndexInfo(), m_deltaTableHandler->GetIndexScanKey()},
            deltaIdxValues, deltaIdxIsnulls, &deltaRec);
        StorageAssert(ret == DSTORE_SUCC);
        if (deltaRec != INVALID_ITEM_POINTER) {
            satisfiesUnique = true;
        }
    }
    ErrLog(DSTORE_DEBUG1, MODULE_INDEX, ErrMsg("[UT] UpdateOneInPhase3 old {%hu, %u} %u, new {%hu, %u} %u, %s",
        oldHeapCtid.GetFileId(), oldHeapCtid.GetBlockNum(), oldHeapCtid.GetOffset(),
        newHeapCtid.GetFileId(), newHeapCtid.GetBlockNum(), newHeapCtid.GetOffset(),
        (needRollback || !satisfiesUnique) ? (!satisfiesUnique ? "Duplicate" : "Abort") : "Commit"));

    if (!satisfiesUnique  || needRollback) {
        txn->Abort();
        return INVALID_ITEM_POINTER;
    }
    StorageAssert(satisfiesUnique);
    txn->Commit();
    ErrLog(DSTORE_DEBUG1, MODULE_INDEX, ErrMsg("[UT] UpdateOneInPhase3 old {%hu, %u} %u, new {%hu, %u} %u, %s",
        oldHeapCtid.GetFileId(), oldHeapCtid.GetBlockNum(), oldHeapCtid.GetOffset(),
        newHeapCtid.GetFileId(), newHeapCtid.GetBlockNum(), newHeapCtid.GetOffset(),
        needRollback ? "Abort" : "Commit"));
    return newHeapCtid;
}

ItemPointerData UTBtreeCcindexTest::InsertHeapTupleIntoTargetTable(int keyVal)
{
    DefaultRowDef rowDef = m_utTableHandler->GetDefaultRowDef();
    rowDef.column_int16 = static_cast<int16>(keyVal);
    rowDef.column_int32 = static_cast<int16>(keyVal);
    Datum targetValue[m_utTableHandler->GetHeapTupDesc()->natts];
    bool targetIsNull[m_utTableHandler->GetHeapTupDesc()->natts];
    HeapTuple *heapTuple = m_utTableHandler->GetSpecificHeapTuple(&rowDef, targetValue, targetIsNull);

    ItemPointerData heapCtid = m_utTableHandler->InsertHeapTupAndCheckResult(heapTuple, true);

    DstorePfree(heapTuple);
    return heapCtid;
}

void UTBtreeCcindexTest::DeleteHeapTupleFromTargetTable(ItemPointer targetHeapCtid)
{
    HeapDeleteContext deleteContext;
    deleteContext.ctid = *targetHeapCtid;
    deleteContext.needReturnTup = false;
    deleteContext.snapshot = *thrd->GetActiveTransaction()->GetSnapshotData();
    deleteContext.cid = thrd->GetActiveTransaction()->GetCurCid();

    HeapDeleteHandler heapDelete(g_storageInstance, thrd, m_utTableHandler->GetTableRel());
    RetStatus ret = heapDelete.Delete(&deleteContext);
    StorageAssert(ret == DSTORE_SUCC);
}

ItemPointerData UTBtreeCcindexTest::UpdateHeapTupleFromTargetTable(int newVal, ItemPointerData targetHeapCtid)
{
    DefaultRowDef rowDef = m_utTableHandler->GetDefaultRowDef();
    rowDef.column_int16 = static_cast<int16>(newVal);
    rowDef.column_int32 = static_cast<int16>(newVal);
    Datum targetValue[m_utTableHandler->GetHeapTupDesc()->natts];
    bool targetIsNull[m_utTableHandler->GetHeapTupDesc()->natts];
    HeapTuple *newTuple = m_utTableHandler->GetSpecificHeapTuple(&rowDef, targetValue, targetIsNull);

    HeapUpdateContext updateContext;
    updateContext.oldCtid = targetHeapCtid;
    updateContext.needUpdateLob = false;
    updateContext.needReturnOldTup = false;
    updateContext.hasIndex = false;
    updateContext.snapshot = *thrd->GetActiveTransaction()->GetSnapshotData();
    updateContext.newTuple = newTuple;
    updateContext.cid = thrd->GetActiveTransaction()->GetCurCid();

    HeapUpdateHandler heapUpdate(g_storageInstance, thrd, m_utTableHandler->GetTableRel());
    RetStatus ret = heapUpdate.Update(&updateContext);
    StorageAssert(ret == DSTORE_SUCC);

    return updateContext.newCtid;
}

ItemPointerData UTBtreeCcindexTest::InsertRecordIntoDeltaTable(int keyVal, ItemPointer targetHeapCtid,
                                                               DmlOperationTypeForCcindex operType)
{
    Datum deltaValues[m_deltaAttrNum];
    bool deltaIsnulls[m_deltaAttrNum];
    GetDeltaDatum(deltaValues, deltaIsnulls, keyVal, targetHeapCtid, operType);
    HeapTuple *deltaHeapTuple = HeapTuple::FormTuple(m_deltaTableHandler->GetHeapTupDesc(), deltaValues, deltaIsnulls);

    ItemPointerData heapCtid = m_deltaTableHandler->InsertHeapTupAndCheckResult(deltaHeapTuple, true);

    DstorePfree(deltaHeapTuple);
    return heapCtid;
}

void UTBtreeCcindexTest::DeleteOldDeltaRecord(ItemPointer oldDeltaCtid)
{
    /* Get the record first */
    HeapScanHandler heapScan(g_storageInstance, thrd, m_deltaTableHandler->GetTableRel());
    heapScan.Begin(thrd->GetActiveTransaction()->GetSnapshot());
    HeapTuple *deltaTuple = heapScan.FetchTuple(*oldDeltaCtid);
    Datum idxValues[m_deltaAttrNum];
    bool idxIsnulls[m_deltaAttrNum];
    GetDeltaIndexDatumFromHeapTuple(idxValues, idxIsnulls, deltaTuple);
    DstorePfreeExt(deltaTuple);
    heapScan.End();

    /* Delete from DeltaDmlTable */
    HeapDeleteContext deleteContext;
    deleteContext.ctid = *oldDeltaCtid;
    deleteContext.needReturnTup = false;
    deleteContext.snapshot = *thrd->GetActiveTransaction()->GetSnapshotData();
    deleteContext.cid = thrd->GetActiveTransaction()->GetCurCid();
    HeapDeleteHandler heapDelete(g_storageInstance, thrd, m_deltaTableHandler->GetTableRel());
    RetStatus ret = heapDelete.Delete(&deleteContext);
    StorageAssert(ret == DSTORE_SUCC);

    /* Delete from DeltaDmlIndex */
    BtreeDelete btreeDelete(m_deltaTableHandler->GetIndexRel(), m_deltaTableHandler->GetIndexInfo(),
                            m_deltaTableHandler->GetIndexScanKey());
    ret = btreeDelete.DeleteTuple(idxValues, idxIsnulls, oldDeltaCtid);
    StorageAssert(ret == DSTORE_SUCC);
}

RetStatus UTBtreeCcindexTest::UpdateDeltaIndex(int keyVal, ItemPointer targetHeapCtid, ItemPointer deltaHeapCtid,
                                               DmlOperationTypeForCcindex operType)
{
    Datum deltaValues[m_deltaAttrNum];
    bool deltaIsnulls[m_deltaAttrNum];
    GetDeltaDatum(deltaValues, deltaIsnulls, keyVal, targetHeapCtid, operType);

    RetStatus ret = DSTORE_FAIL;
    ItemPointerData duplicateDeltaRec = INVALID_ITEM_POINTER;
    int retryTime = 0;
    while (retryTime < 5) {
        retryTime++;
        ret = BtreeDeltaDmlForCcindex::UpdateDeltaDmlRec({m_deltaTableHandler->GetIndexRel(),
            m_deltaTableHandler->GetIndexInfo(), m_deltaTableHandler->GetIndexScanKey()},
            deltaValues, deltaIsnulls, deltaHeapCtid, &duplicateDeltaRec);
        StorageAssert(ret == DSTORE_SUCC);
        if (duplicateDeltaRec == DSTORE::INVALID_ITEM_POINTER) {
            break;
        }
        /* delete duplicate delta heap and index*/
        DeleteOldDeltaRecord(&duplicateDeltaRec);
    }
    return ret;
}

void UTBtreeCcindexTest::GetTargetIndexDatum(Datum *values, bool *isnulls, int keyVal)
{
    values[0] = Int16GetDatum(static_cast<int16>(keyVal));
    isnulls[0] = false;
    values[1] = Int32GetDatum(keyVal);
    isnulls[1] = false;
}

void UTBtreeCcindexTest::GetDeltaDatum(Datum *values, bool *isnulls, int keyVal, ItemPointer heapCtid,
                                       DmlOperationTypeForCcindex operType)
{
    values[0] = UInt64GetDatum(heapCtid->m_placeHolder);
    isnulls[0] = false;

    values[1] = Int16GetDatum(static_cast<int16>(keyVal));
    isnulls[1] = false;
    values[2] = Int32GetDatum(keyVal);
    isnulls[2] = false;

    values[3] = DstoreInt8GetDatum(static_cast<int8>(operType));
    isnulls[3] = false;
}

void UTBtreeCcindexTest::GetDeltaIndexDatumFromHeapTuple(Datum *values, bool *isnulls, HeapTuple *heapTuple)
{
    for (int i = 1; i <= m_deltaTableHandler->GetHeapTupDesc()->natts; i++) {
        values[i - 1] = heapTuple->GetAttr(i, m_deltaTableHandler->GetHeapTupDesc(), &isnulls[i - 1]);
    }
}

int UTBtreeCcindexTest::GetNumOfDataFromIndex(int keyNum, ScanKey skey)
{
    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetSnapshotCsn();
    IndexScanHandler indexScan;
    RetStatus ret = indexScan.InitIndexScanHandler(m_utTableHandler->GetIndexRel(), m_utTableHandler->GetIndexInfo(),
                                                   keyNum, 0);
    EXPECT_EQ(ret, DSTORE_SUCC);
    indexScan.BeginScan();
    indexScan.InitSnapshot(txn->GetSnapshot());
    indexScan.ReScan(skey);
    int num = 0;
    bool found = false;
    while (STORAGE_FUNC_SUCC(indexScan.GetNextTuple(ScanDirection::FORWARD_SCAN_DIRECTION, &found)) && found) {
        num ++;
    }
    indexScan.EndScan();
    txn->Commit();
    return num;
}

int UTBtreeCcindexTest::GetNumOfDataFromTable()
{
    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetSnapshotCsn();
    HeapScanHandler heapScan(g_storageInstance, thrd, m_utTableHandler->GetTableRel());
    heapScan.Begin(txn->GetSnapshot());
    int num = 0;
    while (heapScan.SeqScan() != nullptr) {
        num++;
    }
    heapScan.End();
    txn->Commit();
    return num;
}

void UTBtreeCcindexTest::UpdateLocalCsnMin()
{
    g_storageInstance->GetCsnMgr()->UpdateLocalCsnMin();
}
