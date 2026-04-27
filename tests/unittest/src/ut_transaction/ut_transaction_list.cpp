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
#include "ut_transaction/ut_transaction_list.h"
#include "common/dstore_datatype.h"
#include "errorcode/dstore_transaction_error_code.h"
#include "heap/dstore_heap_update.h"
#include "transaction/dstore_transaction_mgr.h"
#include "lock/dstore_lock_interface.h"
#include "transaction/dstore_transaction_interface.h"

void UTTransactionListTest::CheckActiveTransaction(bool shouldBeAutonm)
{
    /* Get active transaction */
    Transaction *activeTransaction = thrd->GetActiveTransaction();
    ASSERT_TRUE(activeTransaction != nullptr);
    ASSERT_EQ(activeTransaction->IsAutonomousTransaction(), shouldBeAutonm);
}

void UTTransactionListTest::CheckTuples(std::vector<HeapTuple *> tuples)
{
    Transaction *activeTransaction = thrd->GetActiveTransaction();
    activeTransaction->Start();
    activeTransaction->SetSnapshotCsn();
    /* Checks all tuples from scanning are stored in "tuples" */
    HeapTuple *scanTuple = nullptr;
    int foundTupleCount = 0;
    {
        HeapScanHandler heapScan(g_storageInstance, thrd, m_utTableHandler->GetTableRel());
        heapScan.Begin(activeTransaction->GetSnapshot());
        while ((scanTuple = heapScan.SeqScan()) != nullptr) {
            bool found = false;
            for (int i = 0; i < tuples.size(); i++) {
                HeapTuple *tuple = tuples.at(i);
                if (memcmp((void *)tuple->GetDiskTuple()->GetData(), (void *)scanTuple->GetDiskTuple()->GetData(),
                           tuple->GetDiskTupleSize() - tuple->GetDiskTuple()->GetHeaderSize()) == 0) {
                    found = true;
                    break;
                }
            }
            ASSERT_TRUE(found);
            foundTupleCount++;
        }
        heapScan.End();
    }
    activeTransaction->Commit();
    /* Checks if only HeapTuples from "tuples" are stored in TableStorageMgr */
    ASSERT_EQ(foundTupleCount, tuples.size());

    for (int i = 0; i < tuples.size(); i++) {
        DstorePfreeExt(tuples.at(i));
    }
}

RetStatus UTTransactionListTest::StartAndBeginTrxBlock()
{
    Transaction *activeTransaction = thrd->GetActiveTransaction();
    RetStatus ret = DSTORE_FAIL;
    do {
        /* Start a new transaction */
        if (STORAGE_FUNC_FAIL(activeTransaction->Start())) {
            break;
        }
        /* Begin a transaction block */
        if (STORAGE_FUNC_FAIL(activeTransaction->BeginTransactionBlock())) {
            break;
        }
        if (STORAGE_FUNC_FAIL(activeTransaction->Commit())) {
            break;
        }
        ret = DSTORE_SUCC;
    } while (false);
    return ret;
}

RetStatus UTTransactionListTest::EndTrxBlockAndCommitOrRollback(bool isCommit)
{
    Transaction *activeTransaction = thrd->GetActiveTransaction();
    EXPECT_TRUE(activeTransaction->IsTxnBlock());
    RetStatus ret = DSTORE_FAIL;
    do {
        if (!isCommit) {
            /* If it is not commit, then it is abort. */
            if (STORAGE_FUNC_FAIL(activeTransaction->UserAbortTransactionBlock())) {
                break;
            }
        } else {
            /* End transaction block */
            if (STORAGE_FUNC_FAIL(activeTransaction->EndTransactionBlock())) {
                break;
            }
        }
        if (STORAGE_FUNC_FAIL(activeTransaction->Commit())) {
            break;
        }

        ret = DSTORE_SUCC;
    } while (false);
    return ret;
}

void UTTransactionListTest::CreateAutonomousTrx()
{
    RetStatus ret = thrd->CreateAutonomousTrx();
    ASSERT_EQ(ret, DSTORE_SUCC);
    /* Treat begin as a sql command */
    thrd->GetActiveTransaction()->Start();
    thrd->GetActiveTransaction()->BeginTransactionBlock();
    thrd->GetActiveTransaction()->Commit();
}

void UTTransactionListTest::DestroyAutonomousTrx(bool isCommit)
{
    Transaction *trx = thrd->GetActiveTransaction();
    if (!isCommit) {
        /* If it is not commit, then it is abort. */
        if (STORAGE_FUNC_FAIL(trx->UserAbortTransactionBlock())) {
            StorageAssert(0);
        }
    } else {
        /* End transaction block */
        if (STORAGE_FUNC_FAIL(trx->EndTransactionBlock())) {
            StorageAssert(0);
        }
    }
    if (STORAGE_FUNC_FAIL(trx->Commit())) {
        StorageAssert(0);
    }
    thrd->DestroyAutonomousTrx();
}

ItemPointerData UTTransactionListTest::UpdateHeapTupleAndCheckResult(ItemPointerData *ctid, HeapTuple *newTuple,
                                                                     bool inTrxBlock)
{
    /* Execute update in a transaction block */
    Transaction *activeTransaction = thrd->GetActiveTransaction();
    RetStatus ret = DSTORE_FAIL;

    /* Lock the tuple before update */
    if (inTrxBlock) {
        /* Start a new transaction and begin transaction block */
        ret = StartAndBeginTrxBlock();
        EXPECT_TRUE(STORAGE_FUNC_SUCC(ret));
    } else {
        ret = activeTransaction->Start();
        EXPECT_TRUE(STORAGE_FUNC_SUCC(ret));
    }
    ret = activeTransaction->SetSnapshotCsn();
    StorageAssert(STORAGE_FUNC_SUCC(ret));
    HeapLockTupleContext lockTupContext;
    lockTupContext.ctid = *ctid;
    lockTupContext.snapshot = *activeTransaction->GetSnapshot();
    HeapLockTupHandler lockTuple(g_storageInstance, thrd, m_utTableHandler->GetTableRel());
    ret = lockTuple.LockUnchangedTuple(&lockTupContext);
    if (STORAGE_FUNC_FAIL(ret)) {
        return INVALID_ITEM_POINTER;
    }

    /* Update the tuple */
    HeapUpdateContext updateContext;
    updateContext.oldCtid = *ctid;
    updateContext.newTuple = newTuple;
    updateContext.needReturnOldTup = true;
    HeapUpdateHandler heapUpdate(g_storageInstance, thrd, m_utTableHandler->GetTableRel());
    updateContext.snapshot = *activeTransaction->GetSnapshotData();
    updateContext.cid = activeTransaction->GetCurCid();
    if (STORAGE_FUNC_FAIL(heapUpdate.Update(&updateContext))) {
        return INVALID_ITEM_POINTER;
    }
    activeTransaction->IncreaseCommandCounter();
    HeapTuple *tuple = m_utTableHandler->FetchHeapTuple(&updateContext.newCtid);
    if (memcmp((void *)tuple->GetDiskTuple()->GetData(), (void *)newTuple->GetDiskTuple()->GetData(),
               tuple->GetDiskTupleSize() - tuple->GetDiskTuple()->GetHeaderSize()) == 0) {
        return updateContext.newCtid;
    }
    /*
     * Some operations expect INVALID_ITEM_POINTER.
     * For example, if an autonomous transaction tries to update a row
     * that is already modified by the primary transaction, then we would
     * expect this method to return INVALID_ITEM_POINTER.
     * And the caller will do assert accordingly.
     */
    return INVALID_ITEM_POINTER;
}

RetStatus UTTransactionListTest::DeleteHeapTupleAndCheckResult(ItemPointerData *ctid, bool inTrxBlock)
{
    /* Execute update in a transaction block */
    Transaction *activeTransaction = thrd->GetActiveTransaction();
    RetStatus ret = DSTORE_FAIL;
    /* Lock the tuple before update */
    if (inTrxBlock) {
        /* Start a new transaction and begin transaction block */
        ret = StartAndBeginTrxBlock();
        EXPECT_TRUE(STORAGE_FUNC_SUCC(ret));
    } else {
        ret = activeTransaction->Start();
        EXPECT_TRUE(STORAGE_FUNC_SUCC(ret));
    }
    ret = activeTransaction->SetSnapshotCsn();
    StorageAssert(STORAGE_FUNC_SUCC(ret));

    /* Delete the tuple */
    HeapDeleteContext deleteContext;
    deleteContext.ctid = *ctid;
    deleteContext.snapshot = *activeTransaction->GetSnapshotData();
    deleteContext.cid = activeTransaction->GetCurCid();
    HeapDeleteHandler heapDelete(g_storageInstance, thrd, m_utTableHandler->GetTableRel());
    if (STORAGE_FUNC_FAIL(heapDelete.Delete(&deleteContext))) {
        return ret;
    }
    activeTransaction->IncreaseCommandCounter();
    HeapTuple *tuple = m_utTableHandler->FetchHeapTuple(&deleteContext.ctid);
    EXPECT_EQ(tuple, nullptr);
    return DSTORE_SUCC;
}

TEST_F(UTTransactionListTest, TransactionListInitRuntimeWithoutZone_level0)
{
    TransactionList transactionList;

    ASSERT_EQ(transactionList.GetActiveTransaction(), nullptr);

    RetStatus ret = transactionList.InitRuntime(false, g_defaultPdbId);
    ASSERT_EQ(ret, DSTORE_SUCC);

    ASSERT_NE(transactionList.GetActiveTransaction(), nullptr);

    /* Get active transaction */
    Transaction *activeTransaction = transactionList.GetActiveTransaction();
    ASSERT_TRUE(activeTransaction != nullptr);
    /* Active transaction should not be autonomous transaction */
    ASSERT_FALSE(activeTransaction->IsAutonomousTransaction());

    /* Active transaction should not have any assigned undo zone */
    ASSERT_EQ(activeTransaction->GetCurrentZoneId(), INVALID_ZONE_ID);
}

TEST_F(UTTransactionListTest, TransactionListInitRuntimeWithZone_level0)
{
    TransactionList transactionList;

    ASSERT_EQ(transactionList.GetActiveTransaction(), nullptr);

    RetStatus ret = transactionList.InitRuntime(true, g_defaultPdbId);
    ASSERT_EQ(ret, DSTORE_SUCC);

    ASSERT_NE(transactionList.GetActiveTransaction(), nullptr);

    /* Get active transaction */
    Transaction *activeTransaction = transactionList.GetActiveTransaction();
    ASSERT_TRUE(activeTransaction != nullptr);
    /* Active transaction should not be autonomous transaction */
    ASSERT_FALSE(activeTransaction->IsAutonomousTransaction());

    /* Active transaction must have an assigned undo zone */
    ASSERT_TRUE(activeTransaction->GetCurrentZoneId() != INVALID_ZONE_ID);
}

TEST_F(UTTransactionListTest, TransactionListCreateDestroyAutonomousTransaction_level0)
{
    CheckActiveTransaction(false);

    int16 autonmTrxCount = 10;

    thrd->GetActiveTransaction()->Start();
    /* Create autonomous transactions */
    for (int i = 0; i < autonmTrxCount; i++) {
        CreateAutonomousTrx();
        /* Active transaction should be autonomous transaction */
        CheckActiveTransaction(true);

        Transaction *activeTransaction = thrd->GetActiveTransaction();

        /* Autonomous transaction must have an assigned undo zone */
        ASSERT_TRUE(activeTransaction->GetCurrentZoneId() != INVALID_ZONE_ID);
    }

    /* Destroy autonomous transactions */
    for (int i = 0; i < autonmTrxCount; i++) {
        DestroyAutonomousTrx(false);
    }

    thrd->GetActiveTransaction()->Commit();

    /* Active transaction must not be autonomous transaction */
    CheckActiveTransaction(false);
}

TEST_F(UTTransactionListTest, TransactionListContainsTransaction_level0)
{
    CheckActiveTransaction(false);

    /* Step 1: Start a new transaction and begin transaction block */
    StartAndBeginTrxBlock();

    /* Step 2: Insert a tuple */
    HeapTuple *tuple = m_utTableHandler->GenerateSpecificHeapTuple("row-primary-0");
    ItemPointerData ctid = m_utTableHandler->InsertHeapTupAndCheckResult(tuple);
    DstorePfreeExt(tuple);
    ASSERT_NE(ctid, INVALID_ITEM_POINTER);

    /* Step 3: Active transaction should have valid Xid */
    Transaction *activeTransaction = thrd->GetActiveTransaction();
    Xid primaryTrxXid = activeTransaction->GetCurrentXid();
    ASSERT_TRUE(primaryTrxXid != INVALID_XID);

    /*
     * Step 4: Create autonomous transactions.
     */
    int16 autonmTrxCount = 4;
    for (int i = 0; i < autonmTrxCount; i++) {
        /* Step 4.1: Create autonomous transaction */
        CreateAutonomousTrx();

        /* Active transaction should be autonomous transaction */
        CheckActiveTransaction(true);

        /* Step 4.2: Insert a tuple in a transaction block */
        HeapTuple *tuple = m_utTableHandler->GenerateSpecificHeapTuple("row-autonm-0");
        ItemPointerData ctid = m_utTableHandler->InsertHeapTupAndCheckResult(tuple);
        DstorePfreeExt(tuple);
        ASSERT_NE(ctid, INVALID_ITEM_POINTER);

        Transaction *activeTransaction = thrd->GetActiveTransaction();
        Xid xid = activeTransaction->GetCurrentXid();
        ASSERT_TRUE(xid != INVALID_XID);

        /* Step 4.3: Primary transaction Xid must be available in m_transactionList */
        ASSERT_TRUE(thrd->ContainsTransaction(primaryTrxXid));

        /* Step 4.4: Commit autonomous transaction and destroy. */
        DestroyAutonomousTrx(false);

        /*
         * Step 4.5: Active autonomous transaction Xid should not be
         * in m_transactionList anymore, as the active transaction has ended.
         */
        ASSERT_FALSE(thrd->ContainsTransaction(xid));
    }

    /* Check if the active transaction is primary transaction. */
    CheckActiveTransaction(false);

    /*
     * Step 5: Primary transaction should be in a transaction block.
     * End transaction block and commit transaction.
     */
    RetStatus ret = EndTrxBlockAndCommitOrRollback(true);
    ASSERT_EQ(ret, DSTORE_SUCC);
    /*
     * Step 6: Active autonomous transaction Xid should not be
     * in m_transactionList anymore, as the active transaction has ended.
     */
    ASSERT_FALSE(thrd->ContainsTransaction(primaryTrxXid));
}

TEST_F(UTTransactionListTest, AutonomousTrxSameRowUpdate_level0)
{
    CheckActiveTransaction(false);
    std::vector<ItemPointerData> ctids;
    /* Insert tuples */
    int rowCount = 3;
    for (int i = 0; i < rowCount; i++) {
        HeapTuple *tuple = m_utTableHandler->GenerateSpecificHeapTuple("row-default-" + std::to_string(i));
        ItemPointerData ctid = m_utTableHandler->InsertHeapTupAndCheckResult(tuple);
        DstorePfreeExt(tuple);
        ASSERT_NE(ctid, INVALID_ITEM_POINTER);
        ctids.push_back(ctid);
    }

    /*
     * Primary transaction and autonomous transaction
     * try to update the same row.
     */
    int updateRowAt = 2;

    /* Step 1: Update `updateRowAt` tuple in the primary transaction block */
    HeapTuple *newTupleByPrimaryTrx =
        m_utTableHandler->GenerateSpecificHeapTuple("row-primary-" + std::to_string(updateRowAt));
    ItemPointerData newCtidByPrimaryTrx = UpdateHeapTupleAndCheckResult(&(ctids.at(updateRowAt)), newTupleByPrimaryTrx);
    ASSERT_NE(newCtidByPrimaryTrx, INVALID_ITEM_POINTER);

    /*
     * Step 2: Create autonomous transaction
     * We don't commit primary transaction yet.
     * Instead, we start an autonomous transaction and execute
     * an update for same tuple that is already modified by the primary transaction.
     */
    RetStatus ret = thrd->CreateAutonomousTrx();
    ASSERT_EQ(ret, DSTORE_SUCC);

    /* Step 3: Active transaction should be autonomous transaction */
    CheckActiveTransaction(true);

    /* Step 4: Execute update in an autonomous transaction block. */
    HeapTuple *newTupleByAutonmTrx =
        m_utTableHandler->GenerateSpecificHeapTuple("row-autonm-" + std::to_string(updateRowAt));
    ItemPointerData newCtidByAutonmTrx = UpdateHeapTupleAndCheckResult(&(ctids.at(updateRowAt)), newTupleByAutonmTrx,
                                                                       /*
                                                                        * Autonomous transaction has already
                                                                        * started a transaction block
                                                                        */
                                                                       false);
    DstorePfreeExt(newTupleByAutonmTrx);
    ASSERT_EQ(newCtidByAutonmTrx, INVALID_ITEM_POINTER);

    /* Step 5: Commit autonomous transaction and destroy */
    thrd->DestroyAutonomousTrx();

    /* Step 6: Check active transaction should not be autonomous transaction */
    CheckActiveTransaction(false);

    /* Step 7: End transaction block and commit transaction */
    ret = EndTrxBlockAndCommitOrRollback(true);
    ASSERT_EQ(ret, DSTORE_SUCC);
    /*
     * Step 8: Check if tuple updated by primary transaction has not been
     * affected by autonomous transaction.
     */
    HeapTuple *newTuple = m_utTableHandler->FetchHeapTuple(&newCtidByPrimaryTrx);
    ASSERT_EQ(
        memcmp((void *)newTuple->GetDiskTuple()->GetData(), (void *)newTupleByPrimaryTrx->GetDiskTuple()->GetData(),
               newTupleByPrimaryTrx->GetDiskTupleSize() - newTupleByPrimaryTrx->GetDiskTuple()->GetHeaderSize()),
        0);
    DstorePfreeExt(newTuple);
    DstorePfreeExt(newTupleByPrimaryTrx);
}

TEST_F(UTTransactionListTest, AutonomousTrxWaitForTransactionEnd_level0)
{
    CheckActiveTransaction(false);
    std::vector<ItemPointerData> ctids;
    /* Insert tuples */
    int rowCount = 3;
    for (int i = 0; i < rowCount; i++) {
        HeapTuple *tuple = m_utTableHandler->GenerateSpecificHeapTuple("row-default-" + std::to_string(i));
        ItemPointerData ctid = m_utTableHandler->InsertHeapTupAndCheckResult(tuple);
        DstorePfreeExt(tuple);
        ASSERT_NE(ctid, INVALID_ITEM_POINTER);
        ctids.push_back(ctid);
    }

    /*
     * Primary transaction and autonomous transaction
     * try to update the same row.
     */
    int updateRowAt = 2;

    /* Step 1: Update `updateRowAt` tuple in the primary transaction block */
    HeapTuple *newTupleByPrimaryTrx =
        m_utTableHandler->GenerateSpecificHeapTuple("row-primary-" + std::to_string(updateRowAt));
    ItemPointerData newCtidByPrimaryTrx = UpdateHeapTupleAndCheckResult(&(ctids.at(updateRowAt)), newTupleByPrimaryTrx);
    ASSERT_NE(newCtidByPrimaryTrx, INVALID_ITEM_POINTER);

    /* Save primary transaction xid */
    Transaction *activeTransaction = thrd->GetActiveTransaction();
    Xid primaryTrxXid = activeTransaction->GetCurrentXid();
    ASSERT_NE(primaryTrxXid, INVALID_XID);

    /*
     * Step 2: Create autonomous transaction
     * We don't commit primary transaction yet.
     * Instead, we start an autonomous transaction and try to wait for primary transaction end.
     */
    RetStatus ret = thrd->CreateAutonomousTrx();
    ASSERT_EQ(ret, DSTORE_SUCC);

    /* Step 3: Active transaction should be autonomous transaction */
    CheckActiveTransaction(true);

    /* Step 4: Try to wait for primary transaction in autonomous transaction */
    TransactionMgr *trxMgr = g_storageInstance->GetPdb(g_defaultPdbId)->GetTransactionMgr();
    bool txnFailed = false;
    ret = trxMgr->WaitForTransactionEnd(primaryTrxXid, txnFailed);

    /*
     * Step 5: autonomous transaction should not be able to
     * wait for a transaction that is on the same thread.
     */
    ASSERT_EQ(ret, DSTORE_FAIL);
    ASSERT_EQ(StorageGetErrorCode(), TRANSACTION_INFO_SAME_THREAD_DEADLOCK);

    /* Step 6: Rollback autonomous transaction block and destroy */
    thrd->DestroyAutonomousTrx();

    /* Step 7: Check active transaction should not be autonomous transaction */
    CheckActiveTransaction(false);

    /* Step 8: End transaction block and commit in primary transaction. */
    ret = EndTrxBlockAndCommitOrRollback(true);
    ASSERT_EQ(ret, DSTORE_SUCC);

    /*
     * Step 9: Check if tuple updated by primary transaction has not been
     * affected by autonomous transaction.
     */
    HeapTuple *newTuple = m_utTableHandler->FetchHeapTuple(&newCtidByPrimaryTrx);
    ASSERT_EQ(
        memcmp((void *)newTuple->GetDiskTuple()->GetData(), (void *)newTupleByPrimaryTrx->GetDiskTuple()->GetData(),
               newTupleByPrimaryTrx->GetDiskTupleSize() - newTupleByPrimaryTrx->GetDiskTuple()->GetHeaderSize()),
        0);
    DstorePfreeExt(newTuple);
    DstorePfreeExt(newTupleByPrimaryTrx);
}

TEST_F(UTTransactionListTest, AutonomousTrxTransactionStmtIsolationWithHeapInsert_1_level0)
{
    CheckActiveTransaction(false);
    RetStatus ret = DSTORE_FAIL;
    Transaction *activeTransaction = thrd->GetActiveTransaction();
    /* Stores only committed tuples  */
    std::vector<HeapTuple *> tuples;

    ret = activeTransaction->Start();
    ASSERT_EQ(ret, DSTORE_SUCC);

    /* Step 1: Insert tuples */
    int rowCount = 2;
    for (int i = 0; i < rowCount; i++) {
        HeapTuple *tuple = m_utTableHandler->GenerateSpecificHeapTuple("row-primary-" + std::to_string(i));
        ItemPointerData ctid = m_utTableHandler->InsertHeapTupAndCheckResult(tuple, true);
        DstorePfreeExt(tuple);
        ASSERT_NE(ctid, INVALID_ITEM_POINTER);
    }

    /*
     * Step 2: Create autonomous transaction.
     * We don't commit primary transaction yet.
     * Instead, we start an autonomous transaction and we commit inside
     * autonomous transaction. Committing/Rolling back in autonomous transaction
     * should not affect the primary transaction and vice versa.
     */
    CreateAutonomousTrx();

    /* Step 2.1: Insert tuples in autonomous transaction */
    for (int i = 0; i < rowCount; i++) {
        HeapTuple *tuple = m_utTableHandler->GenerateSpecificHeapTuple("row-autonm-" + std::to_string(i));
        ItemPointerData ctid = m_utTableHandler->InsertHeapTupAndCheckResult(tuple, true);
        ASSERT_NE(ctid, INVALID_ITEM_POINTER);
        /* This tuple will be committed later. */
        tuples.push_back(tuple);
    }

    /* Step 2.2: Commit autonomous transaction block and destroy */
    DestroyAutonomousTrx(true);

    /* Step 3: Check active transaction should not be autonomous transaction */
    CheckActiveTransaction(false);

    activeTransaction = thrd->GetActiveTransaction();
    /*
     * Step 4: Aborting the primary transaction should not affect
     * the changes done by autonomous transaction.
     */
    ret = activeTransaction->Abort();
    ASSERT_EQ(ret, DSTORE_SUCC);

    /*
     * Step 5: Check if aborting the primary transaction should not
     * affect the autonomous transaction and committing the autonomous transaction
     * should not affect primary transaction.
     */
    CheckTuples(tuples);
}

TEST_F(UTTransactionListTest, AutonomousTrxTransactionStmtIsolationWithHeapInsert_2_level0)
{
    CheckActiveTransaction(false);
    RetStatus ret = DSTORE_FAIL;
    Transaction *activeTransaction = thrd->GetActiveTransaction();
    /* Stores only committed tuples  */
    std::vector<HeapTuple *> tuples;

    ret = activeTransaction->Start();
    ASSERT_EQ(ret, DSTORE_SUCC);

    /* Step 1: Insert tuples */
    int rowCount = 2;
    for (int i = 0; i < rowCount; i++) {
        HeapTuple *tuple = m_utTableHandler->GenerateSpecificHeapTuple("row-primary-" + std::to_string(i));
        ItemPointerData ctid = m_utTableHandler->InsertHeapTupAndCheckResult(tuple, true);
        ASSERT_NE(ctid, INVALID_ITEM_POINTER);

        /* This tuple will be committed later. */
        tuples.push_back(tuple);
    }

    /*
     * Step 2: Create autonomous transaction.
     * We don't commit primary transaction yet.
     * Instead, we start an autonomous transaction and we rollback the
     * autonomous transaction. Committing/Rolling back in autonomous transaction
     * should not affect the primary transaction and vice versa.
     */
    CreateAutonomousTrx();

    /* Step 2.1: Insert tuples in autonomous transaction */
    for (int i = 0; i < rowCount; i++) {
        HeapTuple *tuple = m_utTableHandler->GenerateSpecificHeapTuple("row-autonm-" + std::to_string(i));
        ItemPointerData ctid = m_utTableHandler->InsertHeapTupAndCheckResult(tuple, true);
        DstorePfreeExt(tuple);
        ASSERT_NE(ctid, INVALID_ITEM_POINTER);
    }

    /* Step 2.2: Rollback autonomous transaction block and destroy */
    DestroyAutonomousTrx(false);

    /* Step 3: Check active transaction should not be autonomous transaction */
    CheckActiveTransaction(false);

    activeTransaction = thrd->GetActiveTransaction();
    /*
     * Step 4: Committing the primary transaction should not affect
     * the autonomous transaction.
     */
    ret = activeTransaction->Commit();
    ASSERT_EQ(ret, DSTORE_SUCC);

    /*
     * Step 5: Check if rolling back the autonomous transaction should not
     * affect the primary transaction and committing the primary transaction
     * should not affect autonomous transaction.
     */
    CheckTuples(tuples);
}

TEST_F(UTTransactionListTest, AutonomousTrxTransactionStmtIsolationWithHeapUpdate_1_level0)
{
    CheckActiveTransaction(false);
    RetStatus ret = DSTORE_FAIL;
    Transaction *activeTransaction = thrd->GetActiveTransaction();
    /* Stores only committed tuples  */
    std::vector<HeapTuple *> tuples;

    activeTransaction->Start();
    activeTransaction->SetSnapshotCsn();
    /* Insert 5 tuples */
    for (int i = 0; i < 5; i++) {
        HeapTuple *tuple = m_utTableHandler->GenerateSpecificHeapTuple("row-default-" + std::to_string(i));
        ItemPointerData ctid = m_utTableHandler->InsertHeapTupAndCheckResult(tuple);
        tuples.push_back(tuple);
        ASSERT_NE(ctid, INVALID_ITEM_POINTER);
    }
    activeTransaction->Commit();

    /* Step 1: Update a tuple in the primary transaction block */
    int updateAtByPrimaryTrx = 2;
    HeapTuple *newTupleByPrimaryTrx =
        m_utTableHandler->GenerateSpecificHeapTuple("row-primary-" + std::to_string(updateAtByPrimaryTrx));
    ItemPointerData newCtidByPrimaryTrx =
        UpdateHeapTupleAndCheckResult(tuples.at(updateAtByPrimaryTrx)->GetCtid(), newTupleByPrimaryTrx);
    ASSERT_NE(newCtidByPrimaryTrx, INVALID_ITEM_POINTER);
    /*
     * We don't store this tuple,
     * as this transaction will be rolled back
     */

    /*
     * Step 2: Create autonomous transaction.
     * We don't commit primary transaction yet.
     * Instead, we start an autonomous transaction and we commit inside
     * autonomous transaction. Committing/Rolling back in autonomous transaction
     * should not affect the primary transaction and vice versa.
     */
    CreateAutonomousTrx();
    CheckActiveTransaction(true);

    /* Step 2.1: Update tuples in autonomous transaction */
    int updateAtByAutonmTrx = 3;
    /* Autonomous transaction should not update the same row modified by primary transaction */
    ASSERT_NE(updateAtByPrimaryTrx, updateAtByAutonmTrx);
    HeapTuple *newTupleByAutonmTrx =
        m_utTableHandler->GenerateSpecificHeapTuple("row-autonm-" + std::to_string(updateAtByAutonmTrx));
    ItemPointerData newCtidByAutonmTrx =
        UpdateHeapTupleAndCheckResult(tuples.at(updateAtByAutonmTrx)->GetCtid(), newTupleByAutonmTrx,
                                      /*
                                       * Autonomous transaction has
                                       * already started a transaction block
                                       */
                                      false);
    ASSERT_NE(newCtidByAutonmTrx, INVALID_ITEM_POINTER);
    /* This tuple will be committed. Hence, update the tuple at this specific index */
    tuples[updateAtByAutonmTrx] = m_utTableHandler->FetchHeapTuple(&newCtidByAutonmTrx);

    /* Step 2.2: Commit autonomous transaction block and destroy */
    DestroyAutonomousTrx(true);

    /* Step 3: Check active transaction should not be autonomous transaction */
    CheckActiveTransaction(false);

    activeTransaction = thrd->GetActiveTransaction();
    /*
     * Step 4: Aborting the primary transaction should not affect
     * the changes done by autonomous transaction.
     */
    ret = activeTransaction->UserAbortTransactionBlock();
    ASSERT_EQ(ret, DSTORE_SUCC);
    ret = activeTransaction->Commit();
    ASSERT_EQ(ret, DSTORE_SUCC);

    /*
     * Step 5: Check if aborting the primary transaction should not
     * affect the autonomous transaction and committing the autonomous transaction
     * should not affect primary transaction.
     */
    CheckTuples(tuples);
}

TEST_F(UTTransactionListTest, AutonomousTrxTransactionStmtIsolationWithHeapUpdate_2_level0)
{
    CheckActiveTransaction(false);
    RetStatus ret = DSTORE_FAIL;
    Transaction *activeTransaction = thrd->GetActiveTransaction();
    /* Stores only committed tuples  */
    std::vector<HeapTuple *> tuples;

    activeTransaction->Start();
    /* Insert 5 tuples */
    for (int i = 0; i < 5; i++) {
        HeapTuple *tuple = m_utTableHandler->GenerateSpecificHeapTuple("row-default-" + std::to_string(i));
        ItemPointerData ctid = m_utTableHandler->InsertHeapTupAndCheckResult(tuple);
        tuples.push_back(tuple);
        ASSERT_NE(ctid, INVALID_ITEM_POINTER);
    }
    activeTransaction->Commit();

    /* Step 1: Update a tuple in the primary transaction block */
    int updateAtByPrimaryTrx = 2;
    HeapTuple *newTupleByPrimaryTrx =
        m_utTableHandler->GenerateSpecificHeapTuple("row-primary-" + std::to_string(updateAtByPrimaryTrx));
    ItemPointerData newCtidByPrimaryTrx =
        UpdateHeapTupleAndCheckResult(tuples.at(updateAtByPrimaryTrx)->GetCtid(), newTupleByPrimaryTrx);
    ASSERT_NE(newCtidByPrimaryTrx, INVALID_ITEM_POINTER);
    /* This tuple will be committed. Hence, update the tuple at this specific index */
    tuples[updateAtByPrimaryTrx] = m_utTableHandler->FetchHeapTuple(&newCtidByPrimaryTrx);

    /*
     * Step 2: Create autonomous transaction.
     * We don't commit primary transaction yet.
     * Instead, we start an autonomous transaction and we rollback the
     * autonomous transaction. Committing/Rolling back in autonomous transaction
     * should not affect the primary transaction and vice versa.
     */
    CreateAutonomousTrx();
    CheckActiveTransaction(true);

    /* Step 2.1: Insert tuples in autonomous transaction */
    int updateAtByAutonmTrx = 3;
    /* Autonomous transaction should not update the same row modified by primary transaction */
    ASSERT_NE(updateAtByPrimaryTrx, updateAtByAutonmTrx);
    HeapTuple *newTupleByAutonmTrx =
        m_utTableHandler->GenerateSpecificHeapTuple("row-autonm-" + std::to_string(updateAtByAutonmTrx));
    ItemPointerData newCtidByAutonmTrx =
        UpdateHeapTupleAndCheckResult(tuples.at(updateAtByAutonmTrx)->GetCtid(), newTupleByAutonmTrx,
                                      /*
                                       * Autonomous transaction has
                                       * already started a transaction block
                                       */
                                      false);
    ASSERT_NE(newCtidByAutonmTrx, INVALID_ITEM_POINTER);
    /*
     * We don't store this tuple,
     * as this autonomous transaction will be rolled back
     */

    /* Step 2.2: Rollback autonomous transaction block and destroy */
    DestroyAutonomousTrx(false);

    /* Step 3: Check active transaction should not be autonomous transaction */
    CheckActiveTransaction(false);

    activeTransaction = thrd->GetActiveTransaction();
    /*
     * Step 4: Committing the primary transaction should not affect
     * the autonomous transaction.
     */
    ret = activeTransaction->Commit();
    ASSERT_EQ(ret, DSTORE_SUCC);

    /*
     * Step 5: Check if rolling back the autonomous transaction should not
     * affect the primary transaction and committing the primary transaction
     * should not affect autonomous transaction.
     */
    CheckTuples(tuples);
}

TEST_F(UTTransactionListTest, AutonomousTrxTransactionStmtIsolationWithHeapDelete_1_level0)
{
    CheckActiveTransaction(false);
    RetStatus ret = DSTORE_FAIL;
    Transaction *activeTransaction = thrd->GetActiveTransaction();
    /* Stores only committed tuples  */
    std::vector<HeapTuple *> tuples;

    /* Insert 5 tuples */
    for (int i = 0; i < 5; i++) {
        HeapTuple *tuple = m_utTableHandler->GenerateSpecificHeapTuple("row-default-" + std::to_string(i));
        ItemPointerData ctid = m_utTableHandler->InsertHeapTupAndCheckResult(tuple);
        tuples.push_back(tuple);
        ASSERT_NE(ctid, INVALID_ITEM_POINTER);
    }

    /* Step 1: Delete a tuple in the primary transaction block */
    int deleteAtByPrimaryTrx = 2;
    ret = DeleteHeapTupleAndCheckResult(tuples.at(deleteAtByPrimaryTrx)->GetCtid());
    ASSERT_EQ(ret, DSTORE_SUCC);
    /*
     * We don't delete this tuple from `tuples`,
     * as this autonomous transaction will be rolled back
     */

    /*
     * Step 2: Create autonomous transaction.
     * We don't commit primary transaction yet.
     * Instead, we start an autonomous transaction and we commit inside
     * autonomous transaction. Committing/Rolling back in autonomous transaction
     * should not affect the primary transaction and vice versa.
     */
    ret = thrd->CreateAutonomousTrx();
    ASSERT_EQ(ret, DSTORE_SUCC);
    CheckActiveTransaction(true);

    /* Step 2.1: Insert tuples in autonomous transaction */
    int deleteAtByAutonmTrx = 3;
    /* Autonomous transaction should not delete the same row modified by primary transaction */
    ASSERT_NE(deleteAtByPrimaryTrx, deleteAtByAutonmTrx);
    ret = DeleteHeapTupleAndCheckResult(tuples.at(deleteAtByAutonmTrx)->GetCtid(),
                                        /*
                                         * Autonomous transaction has
                                         * already started a transaction block
                                         */
                                        false);
    ASSERT_EQ(ret, DSTORE_SUCC);
    /* This tuple changes will be committed. Hence, we remove this deleted tuple from `tuples`. */
    tuples.erase(tuples.begin() + deleteAtByAutonmTrx);

    /* Step 2.2: Commit autonomous transaction block and destroy */
    thrd->DestroyAutonomousTrx();

    /* Step 3: Check active transaction should not be autonomous transaction */
    CheckActiveTransaction(false);

    activeTransaction = thrd->GetActiveTransaction();
    /*
     * Step 4: Aborting the primary transaction should not affect
     * the changes done by autonomous transaction.
     */
    ret = activeTransaction->UserAbortTransactionBlock();
    ASSERT_EQ(ret, DSTORE_SUCC);
    ret = activeTransaction->Commit();
    ASSERT_EQ(ret, DSTORE_SUCC);

    /*
     * Step 5: Check if aborting the primary transaction should not
     * affect the autonomous transaction and committing the autonomous transaction
     * should not affect primary transaction.
     */
    CheckTuples(tuples);
}

TEST_F(UTTransactionListTest, AutonomousTrxTransactionStmtIsolationWithHeapDelete_2_level0)
{
    CheckActiveTransaction(false);
    RetStatus ret = DSTORE_FAIL;
    Transaction *activeTransaction = thrd->GetActiveTransaction();
    /* Stores only committed tuples  */
    std::vector<HeapTuple *> tuples;

    /* Insert 5 tuples */
    for (int i = 0; i < 5; i++) {
        HeapTuple *tuple = m_utTableHandler->GenerateSpecificHeapTuple("row-default-" + std::to_string(i));
        ItemPointerData ctid = m_utTableHandler->InsertHeapTupAndCheckResult(tuple);
        tuples.push_back(tuple);
        ASSERT_NE(ctid, INVALID_ITEM_POINTER);
    }

    /* Step 1: Delete a tuple in the primary transaction block */
    int deleteAtByPrimaryTrx = 2;
    ret = DeleteHeapTupleAndCheckResult(tuples.at(deleteAtByPrimaryTrx)->GetCtid());
    ASSERT_EQ(ret, DSTORE_SUCC);
    /* This tuple changes will be committed. Hence, we remove this deleted tuple from `tuples`. */
    tuples.erase(tuples.begin() + deleteAtByPrimaryTrx);

    /*
     * Step 2: Create autonomous transaction.
     * We don't commit primary transaction yet.
     * Instead, we start an autonomous transaction and we rollback the
     * autonomous transaction. Committing/Rolling back in autonomous transaction
     * should not affect the primary transaction and vice versa.
     */
    CreateAutonomousTrx();
    CheckActiveTransaction(true);

    /* Step 2.1: Insert tuples in autonomous transaction */
    int deleteAtByAutonmTrx = 3;
    /* Autonomous transaction should not delete the same row modified by primary transaction */
    ASSERT_NE(deleteAtByPrimaryTrx, deleteAtByAutonmTrx);
    ret = DeleteHeapTupleAndCheckResult(tuples.at(deleteAtByAutonmTrx)->GetCtid(),
                                        /*
                                         * Autonomous transaction has
                                         * already started a transaction block
                                         */
                                        false);
    ASSERT_EQ(ret, DSTORE_SUCC);
    /*
     * We don't delete this tuple from `tuples`,
     * as this autonomous transaction will be rolled back
     */

    /* Step 2.2: Rollback autonomous transaction block and destroy */
    DestroyAutonomousTrx(false);

    /* Step 3: Check active transaction should not be autonomous transaction */
    CheckActiveTransaction(false);

    activeTransaction = thrd->GetActiveTransaction();
    /*
     * Step 4: Committing the primary transaction should not affect
     * the autonomous transaction.
     */
    ret = activeTransaction->Commit();
    ASSERT_EQ(ret, DSTORE_SUCC);

    /*
     * Step 5: Check if rolling back the autonomous transaction should not
     * affect the primary transaction and committing the primary transaction
     * should not affect autonomous transaction.
     */
    CheckTuples(tuples);
}

TEST_F(UTTransactionListTest, AutonomousTrxMultiCommitRollback_level0)
{
    CheckActiveTransaction(false);
    /* Stores only committed tuples  */
    std::vector<HeapTuple *> tuples;

    /* Step 1: Start a new transaction and begin transaction block */
    StartAndBeginTrxBlock();

    /* Step 2: Insert a tuple */
    HeapTuple *tuple1 = m_utTableHandler->GenerateSpecificHeapTuple("row-primary-0");
    ItemPointerData ctid = m_utTableHandler->InsertHeapTupAndCheckResult(tuple1, m_utTableHandler->GetHeapTabSmgr());
    tuples.push_back(tuple1);
    ASSERT_NE(ctid, INVALID_ITEM_POINTER);

    /* Step 3: Create autonomous transaction */
    CreateAutonomousTrx();

    /* Active transaction should be autonomous transaction */
    CheckActiveTransaction(true);

    /* Step 4.2: Insert a tuple in a transaction block */
    HeapTuple *tuple2 = m_utTableHandler->GenerateSpecificHeapTuple("row-autonm-0");
    ctid = m_utTableHandler->InsertHeapTupAndCheckResult(tuple2, m_utTableHandler->GetHeapTabSmgr());
    tuples.push_back(tuple2);
    ASSERT_NE(ctid, INVALID_ITEM_POINTER);
    TransactionInterface::CommitRollbackAndRestoreTrxState(false);

    HeapTuple *tuple3 = m_utTableHandler->GenerateSpecificHeapTuple("row-autonm-1");
    ctid = m_utTableHandler->InsertHeapTupAndCheckResult(tuple3, m_utTableHandler->GetHeapTabSmgr());
    ASSERT_NE(ctid, INVALID_ITEM_POINTER);
    TransactionInterface::CommitRollbackAndRestoreTrxState(true);

    HeapTuple *tuple4 = m_utTableHandler->GenerateSpecificHeapTuple("row-autonm-2");
    ctid = m_utTableHandler->InsertHeapTupAndCheckResult(tuple4, m_utTableHandler->GetHeapTabSmgr());
    tuples.push_back(tuple4);
    ASSERT_NE(ctid, INVALID_ITEM_POINTER);

    /* Step 4.4: Commit autonomous transaction and destroy. */
    DestroyAutonomousTrx(true);

    /*
     * Step 5: Check if rolling back the autonomous transaction should not
     * affect the primary transaction and other committed autonomous transactions.
     */
    EndTrxBlockAndCommitOrRollback(true);
    CheckTuples(tuples);
}

TEST_F(UTTransactionListTest, AutonomousTrxUserSavepointIsolation_level0)
{
    CheckActiveTransaction(false);
    RetStatus ret = DSTORE_FAIL;
    Transaction *activeTransaction = thrd->GetActiveTransaction();
    /* Stores only committed tuples  */
    std::vector<HeapTuple *> tuples;

    ret = activeTransaction->Start();
    ASSERT_EQ(ret, DSTORE_SUCC);

    /* Step 1: Insert tuples */
    int rowCount = 4;
    int i = 0;
    for (; i < rowCount / 2; i++) {
        HeapTuple *tuple = m_utTableHandler->GenerateSpecificHeapTuple("row-primary-" + std::to_string(i));
        ItemPointerData ctid = m_utTableHandler->InsertHeapTupAndCheckResult(tuple, true);
        ASSERT_NE(ctid, INVALID_ITEM_POINTER);

        /* This tuple will be committed later. */
        tuples.push_back(tuple);
    }

    /* Step 2: Create savepoint on primary transaction */
    ret = activeTransaction->CreateSavepoint("S1");
    ASSERT_EQ(ret, DSTORE_SUCC);

    /* These tuples will be rolled back */
    for (; i < rowCount; i++) {
        HeapTuple *tuple = m_utTableHandler->GenerateSpecificHeapTuple("row-primary-" + std::to_string(i));
        ItemPointerData ctid = m_utTableHandler->InsertHeapTupAndCheckResult(tuple, true);
        DstorePfreeExt(tuple);
        ASSERT_NE(ctid, INVALID_ITEM_POINTER);
    }

    /* Step 3: Create autonomous transaction */
    CreateAutonomousTrx();
    CheckActiveTransaction(true);
    activeTransaction = thrd->GetActiveTransaction();

    /* Step 3.1: Insert tuples in autonomous transaction */
    i = 0;
    for (; i < rowCount / 2; i++) {
        HeapTuple *tuple = m_utTableHandler->GenerateSpecificHeapTuple("row-autonm-" + std::to_string(i));
        ItemPointerData ctid = m_utTableHandler->InsertHeapTupAndCheckResult(tuple, true);
        ASSERT_NE(ctid, INVALID_ITEM_POINTER);

        /* This tuple will be committed later. */
        tuples.push_back(tuple);
    }

    /* Step 3.2: Create savepoint on autonomous transaction */
    ret = activeTransaction->CreateSavepoint("S2");
    ASSERT_EQ(ret, DSTORE_SUCC);

    /* These tuples will be rolled back */
    for (; i < rowCount; i++) {
        HeapTuple *tuple = m_utTableHandler->GenerateSpecificHeapTuple("row-autonm-" + std::to_string(i));
        ItemPointerData ctid = m_utTableHandler->InsertHeapTupAndCheckResult(tuple, true);
        DstorePfreeExt(tuple);
        ASSERT_NE(ctid, INVALID_ITEM_POINTER);
    }

    /* Step 3.3: Rollback to a savepoint */
    ret = activeTransaction->RollbackToSavepoint("S2");
    ASSERT_EQ(ret, DSTORE_SUCC);

    /* Step 3.4: Commit autonomous transaction block and destroy */
    DestroyAutonomousTrx(true);

    /* Step 4: Check active transaction should not be autonomous transaction */
    CheckActiveTransaction(false);

    activeTransaction = thrd->GetActiveTransaction();

    /* Step 5: Rollback to a savepoint */
    ret = activeTransaction->RollbackToSavepoint("S1");
    ASSERT_EQ(ret, DSTORE_SUCC);

    /* Step 6: Commit the primary transaction */
    ret = activeTransaction->Commit();
    ASSERT_EQ(ret, DSTORE_SUCC);

    /*
     * Step 7: Check if rolling back to a savepoint in primary transaction should not
     * affect the autonomous transaction.
     */
    CheckTuples(tuples);
}

TEST_F(UTTransactionListTest, AutonomousTrxInternalSavepointIsolation_level0)
{
    CheckActiveTransaction(false);
    RetStatus ret = DSTORE_FAIL;
    Transaction *activeTransaction = thrd->GetActiveTransaction();
    /* Stores only committed tuples  */
    std::vector<HeapTuple *> tuples;

    ret = activeTransaction->Start();
    ASSERT_EQ(ret, DSTORE_SUCC);

    /* Step 1: Insert tuples */
    int rowCount = 4;
    int i = 0;
    for (; i < rowCount / 2; i++) {
        HeapTuple *tuple = m_utTableHandler->GenerateSpecificHeapTuple("row-primary-" + std::to_string(i));
        ItemPointerData ctid = m_utTableHandler->InsertHeapTupAndCheckResult(tuple, true);
        ASSERT_NE(ctid, INVALID_ITEM_POINTER);

        /* This tuple will be committed later. */
        tuples.push_back(tuple);
    }

    /* Step 2: Create an internal savepoint on primary transaction */
    ret = activeTransaction->CreateSavepoint(nullptr);
    ASSERT_EQ(ret, DSTORE_SUCC);

    /* These tuples will be rolled back */
    for (; i < rowCount; i++) {
        HeapTuple *tuple = m_utTableHandler->GenerateSpecificHeapTuple("row-primary-" + std::to_string(i));
        ItemPointerData ctid = m_utTableHandler->InsertHeapTupAndCheckResult(tuple, true);
        DstorePfreeExt(tuple);
        ASSERT_NE(ctid, INVALID_ITEM_POINTER);
    }

    /* Step 3: Create autonomous transaction */
    CreateAutonomousTrx();
    
    CheckActiveTransaction(true);
    activeTransaction = thrd->GetActiveTransaction();

    /* Step 3.1: Insert tuples in autonomous transaction */
    i = 0;
    for (; i < rowCount / 2; i++) {
        HeapTuple *tuple = m_utTableHandler->GenerateSpecificHeapTuple("row-autonm-" + std::to_string(i));
        ItemPointerData ctid = m_utTableHandler->InsertHeapTupAndCheckResult(tuple, true);
        ASSERT_NE(ctid, INVALID_ITEM_POINTER);

        /* This tuple will be committed later. */
        tuples.push_back(tuple);
    }

    /* Step 3.2: Create an internal savepoint on autonomous transaction */
    ret = activeTransaction->CreateSavepoint(nullptr);
    ASSERT_EQ(ret, DSTORE_SUCC);

    /* These tuples will be rolled back */
    for (; i < rowCount; i++) {
        HeapTuple *tuple = m_utTableHandler->GenerateSpecificHeapTuple("row-autonm-" + std::to_string(i));
        ItemPointerData ctid = m_utTableHandler->InsertHeapTupAndCheckResult(tuple, true);
        DstorePfreeExt(tuple);
        ASSERT_NE(ctid, INVALID_ITEM_POINTER);
    }

    /* Step 3.3: Rollback the current savepoint */
    ret = activeTransaction->RollbackToSavepoint(nullptr);
    ASSERT_EQ(ret, DSTORE_SUCC);

    /* Step 3.4: Commit autonomous transaction block and destroy */
    DestroyAutonomousTrx(true);

    /* Step 4: Check active transaction should not be autonomous transaction */
    CheckActiveTransaction(false);

    activeTransaction = thrd->GetActiveTransaction();

    /* Step 5: Rollback the current internal savepoint */
    ret = activeTransaction->RollbackToSavepoint(nullptr);
    ASSERT_EQ(ret, DSTORE_SUCC);

    /* Step 6: Commit the primary transaction */
    ret = activeTransaction->Commit();
    ASSERT_EQ(ret, DSTORE_SUCC);

    /*
     * Step 7: Check if rolling back to a savepoint in primary transaction should not
     * affect the autonomous transaction.
     */
    CheckTuples(tuples);
}

TEST_F(UTTransactionListTest, AutonomousTrxSameThreadTableLockDeadlock_level0)
{
    CheckActiveTransaction(false);
    RetStatus ret = DSTORE_FAIL;
    Transaction *activeTransaction = thrd->GetActiveTransaction();

    /* Step 1: Primary transaction take a table lock. */
    ret = activeTransaction->Start();
    ASSERT_EQ(ret, DSTORE_SUCC);

    LockInterface::TableLockContext context;
    context.dbId = 0;
    context.relId = 0;
    context.isSessionLock = false;
    context.isPartition = false;
    context.dontWait = false;
    context.mode = DSTORE_EXCLUSIVE_LOCK;
    ret = LockInterface::LockTable(&context);
    ASSERT_EQ(ret, DSTORE_SUCC);

    /* Step 2: Create autonomous transaction. */
    ret = thrd->CreateAutonomousTrx();
    ASSERT_EQ(ret, DSTORE_SUCC);
    CheckActiveTransaction(true);
    activeTransaction = thrd->GetActiveTransaction();

    /* Step 3: Try take a not conflict lock in autonomous transaction, it should success. */
    context.mode = DSTORE_ACCESS_SHARE_LOCK;
    ret = LockInterface::LockTable(&context);
    ASSERT_EQ(ret, DSTORE_SUCC);

    /* Step 4: Try take a conflict lock in autonomous transaction, it should return fail. */
    context.mode = DSTORE_EXCLUSIVE_LOCK;
    ret = LockInterface::LockTable(&context);
    ASSERT_EQ(ret, DSTORE_FAIL);
    ASSERT_EQ(StorageGetErrorCode(), TRANSACTION_INFO_SAME_THREAD_DEADLOCK);

    /* Step 5: Commit autonomous transaction block and destroy */
    thrd->DestroyAutonomousTrx();

    /* Step 6: Try take a lock conflict with autonomous transaction's lock, since the lock is
     * released when autonomous transaction commit, it should return success. */
    CheckActiveTransaction(false);
    context.mode = DSTORE_ACCESS_EXCLUSIVE_LOCK;
    ret = LockInterface::LockTable(&context);
    ASSERT_EQ(ret, DSTORE_SUCC);

    /* Step 7: Commit the primary transaction */
    activeTransaction = thrd->GetActiveTransaction();
    ret = activeTransaction->Commit();
    ASSERT_EQ(ret, DSTORE_SUCC);
}

TEST_F(UTTransactionListTest, AutonomousTrxSameThreadTableLockDeadlockWithSessionLock_level0)
{
    CheckActiveTransaction(false);
    RetStatus ret = DSTORE_FAIL;
    Transaction *activeTransaction = thrd->GetActiveTransaction();

    /* Step 1: Primary transaction take a table lock. */
    ret = activeTransaction->Start();
    ASSERT_EQ(ret, DSTORE_SUCC);

    LockInterface::TableLockContext context;
    context.dbId = 0;
    context.relId = 0;
    context.isPartition = false;
    context.isSessionLock = false;
    context.dontWait = false;
    context.mode = DSTORE_EXCLUSIVE_LOCK;
    ret = LockInterface::LockTable(&context);
    ASSERT_EQ(ret, DSTORE_SUCC);

    /* Step 2: Upgrade to a session lock. */
    context.isSessionLock = true;
    ret = LockInterface::LockTable(&context);
    ASSERT_EQ(ret, DSTORE_SUCC);

    /* Step 3: Release the transaction level lock. */
    context.isSessionLock = false;
    LockInterface::UnlockTable(&context);

    /* Step 4: Create autonomous transaction. */
    ret = thrd->CreateAutonomousTrx();
    ASSERT_EQ(ret, DSTORE_SUCC);
    CheckActiveTransaction(true);
    activeTransaction = thrd->GetActiveTransaction();

    /* Step 5: Try take a not conflict lock in autonomous transaction, it should success. */
    context.mode = DSTORE_ACCESS_SHARE_LOCK;
    ret = LockInterface::LockTable(&context);
    ASSERT_EQ(ret, DSTORE_SUCC);

    /* Step 6: Try take a conflict lock in autonomous transaction, it should also success. */
    context.mode = DSTORE_EXCLUSIVE_LOCK;
    ret = LockInterface::LockTable(&context);
    ASSERT_EQ(ret, DSTORE_SUCC);

    /* Step 7: Commit autonomous transaction block and destroy */
    thrd->DestroyAutonomousTrx();

    /* Step 8: Release the session level lock. */
    CheckActiveTransaction(false);
    context.isSessionLock = true;
    context.mode = DSTORE_EXCLUSIVE_LOCK;
    LockInterface::UnlockTable(&context);

    /* Step 9: Commit the primary transaction */
    activeTransaction = thrd->GetActiveTransaction();
    ret = activeTransaction->Commit();
    ASSERT_EQ(ret, DSTORE_SUCC);
}