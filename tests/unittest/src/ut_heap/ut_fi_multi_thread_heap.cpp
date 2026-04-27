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
#include "lock/dstore_xact_lock_mgr.h"
#include "heap/dstore_heap_scan.h"
#include "ut_heap/ut_heap.h"
#include "ut_heap/ut_heap_multi_thread.h"

void UTHeapMultiThread::UnLockXidThread(SyncPointGroup *syncPointGroup)
{
    BuildThreadLocalVar();
    Transaction *txn = thrd->GetActiveTransaction();
    TransactionMgr *transactionMgr = g_storageInstance->GetPdb(g_defaultPdbId)->GetTransactionMgr();
    UndoMgr *undoMgr = g_storageInstance->GetPdb(g_defaultPdbId)->GetUndoMgr();

    std::string data = GenerateRandomString(2000);
    HeapTuple *heapTuple = UTTableHandler::GenerateSpecificHeapTuple(data);
    ItemPointerData ctid = ThdUtTableHandler->InsertHeapTupAndCheckResult(heapTuple);

    txn->Start();
    txn->SetSnapshotCsn();
    std::string newData = GenerateRandomString(6000); /* make sure another-page-update */
    HeapTuple *newTuple = UTTableHandler::GenerateSpecificHeapTuple(newData);
    ItemPointerData newCtid = UpdateTupAndCheckResult(&ctid, newTuple, txn->GetSnapshotData(), true, ThdUtTableHandler);
    ASSERT_NE(newCtid, INVALID_ITEM_POINTER);
    Xid xid = txn->GetCurrentXid();
    g_storageInstance->GetXactLockMgr()->Unlock(g_defaultPdbId, xid);

    m_mutex.lock();
    m_ctids.push(ctid);
    m_mutex.unlock();

    syncPointGroup->SyncPoint(1);
}

void UTHeapMultiThread::LockUnchangedTupleThread(SyncPointGroup *syncPointGroup)
{
    BuildThreadLocalVar();
    syncPointGroup->SyncPoint(2);
    Transaction *txn = thrd->GetActiveTransaction();
    TransactionMgr *transactionMgr = g_storageInstance->GetPdb(g_defaultPdbId)->GetTransactionMgr();
    UndoMgr *undoMgr = g_storageInstance->GetPdb(g_defaultPdbId)->GetUndoMgr();

    txn->Start();
    txn->SetSnapshotCsn();
    m_mutex.lock();
    ASSERT_TRUE(!m_ctids.empty());
    ItemPointerData ctid = m_ctids.front();
    m_ctids.pop();
    m_mutex.unlock();

    HeapLockTupleContext lockTupContext;
    lockTupContext.ctid = ctid;
    lockTupContext.needRetTup = true;
    std::unique_ptr<StorageRelationData> heapRel(new StorageRelationData());
    heapRel->tableSmgr = ThdUtTableHandler->GetHeapTabSmgr();
    heapRel->m_pdbId = g_defaultPdbId;
    {
        lockTupContext.snapshot = *txn->GetSnapshotData();
        HeapLockTupHandler heapLockTup(g_storageInstance, thrd, heapRel.get());
        RetStatus ret = heapLockTup.LockUnchangedTuple(&lockTupContext);
        ASSERT_TRUE(STORAGE_FUNC_SUCC(ret));
    }
    HeapTuple *retTuple = lockTupContext.retTup->Copy();
    txn->Commit();

    HeapTuple *tuple = ThdUtTableHandler->FetchHeapTuple(&ctid);
    EXPECT_EQ(memcmp(retTuple->GetDiskTuple()->GetData(), tuple->GetDiskTuple()->GetData(),
                     tuple->GetDiskTupleSize() - HEAP_DISK_TUP_HEADER_SIZE), 0);
    DstorePfreeExt(retTuple);
    DstorePfreeExt(tuple);
}

void UTHeapMultiThread::LockTupleUnLockXidThread(SyncPointGroup *syncPointGroup)
{
    BuildThreadLocalVar();
    Transaction *txn = thrd->GetActiveTransaction();
    TransactionMgr *transactionMgr = g_storageInstance->GetPdb(g_defaultPdbId)->GetTransactionMgr();
    UndoMgr *undoMgr = g_storageInstance->GetPdb(g_defaultPdbId)->GetUndoMgr();

    std::string data = GenerateRandomString(2000);
    HeapTuple *heapTuple = UTTableHandler::GenerateSpecificHeapTuple(data);
    ItemPointerData ctid = ThdUtTableHandler->InsertHeapTupAndCheckResult(heapTuple);

    txn->Start();
    txn->SetSnapshotCsn();
    HeapLockTupleContext lockTupContext;
    lockTupContext.ctid = ctid;
    lockTupContext.needRetTup = true;
    std::unique_ptr<StorageRelationData> heapRel(new StorageRelationData());
    heapRel->tableSmgr = ThdUtTableHandler->GetHeapTabSmgr();
    heapRel->m_pdbId = g_defaultPdbId;
    {
        lockTupContext.snapshot = *txn->GetSnapshotData();
        HeapLockTupHandler heapLockTup(g_storageInstance, thrd, heapRel.get());
        RetStatus ret = heapLockTup.LockUnchangedTuple(&lockTupContext);
        ASSERT_TRUE(STORAGE_FUNC_SUCC(ret));
    }
    Xid xid = txn->GetCurrentXid();
    g_storageInstance->GetXactLockMgr()->Unlock(g_defaultPdbId, xid);

    m_mutex.lock();
    m_ctids.push(ctid);
    m_mutex.unlock();

    syncPointGroup->SyncPoint(1);
}

void UTHeapMultiThread::ThreadUnlockXid(UTHeapMultiThread *ptr, SyncPointGroup *syncPointGroup)
{
    ptr->UnLockXidThread(syncPointGroup);
}

void UTHeapMultiThread::ThreadLockUnchangedTuple(UTHeapMultiThread *ptr, SyncPointGroup *syncPointGroup)
{
    ptr->LockUnchangedTupleThread(syncPointGroup);
}

void UTHeapMultiThread::ThreadLockTupleUnlockXid(UTHeapMultiThread *ptr, SyncPointGroup *syncPointGroup)
{
    ptr->LockTupleUnLockXidThread(syncPointGroup);
}

/*
 * Page rollback case.
 * There are two transactions runs parallel.
 *            T1                        T2
 *  Begin, get the snapshot
 *      Insert a tuple
 *    Unlock transaction
 *    record tuple ctid
 *      (SyncPoint)                 (SyncPoint)
 *                             Begin, get the snapshot
 *                  LockUnchangedTuple will rollback the page, and fail
 *                                    commit
 *
 */

TEST_F(UTHeapMultiThread, HeapPageRollbackTest_level0)
{
    SyncPointGroup syncPointGroup{2};

    m_pool.AddTask(ThreadUnlockXid, this, &syncPointGroup);
    m_pool.AddTask(ThreadLockUnchangedTuple, this, &syncPointGroup);

    m_pool.WaitAllTaskFinish();
}

/*
 * Page rollback case.
 * There are two transactions runs parallel.
 *            T1                        T2
 *  Begin, get the snapshot
 *      Insert a tuple
        lock this tuple
 *    Unlock transaction (same as savepage and kill node, txn status is TXN_STATUS_IN_PROGRESS)
 *    record tuple ctid
 *      (SyncPoint)                 (SyncPoint)
 *                             Begin, get the snapshot
 *                  LockUnchangedTuple will rollback the page, and locktuple
 *                                    commit
 *
 */
TEST_F(UTHeapMultiThread, HeapPageRollbackXidTest)
{
    SyncPointGroup syncPointGroup{2};

    m_pool.AddTask(ThreadLockTupleUnlockXid, this, &syncPointGroup);
    m_pool.AddTask(ThreadLockUnchangedTuple, this, &syncPointGroup);

    m_pool.WaitAllTaskFinish();
}

const ZoneId zoneId = 2;

void UTHeapMultiThread::GetUndoZoneThread(SyncPointGroup *syncPointGroup)
{
    UndoMgr *undoMgr = g_storageInstance->GetPdb(g_defaultPdbId)->GetUndoMgr();

    UndoZone *undoZone = nullptr;
    undoMgr->GetUndoZone(zoneId, &undoZone, true);

    m_undozones.push(undoZone);
    syncPointGroup->SyncPoint(1);
}

void UTHeapMultiThread::AsynRollbackThread(SyncPointGroup *syncPointGroup)
{
    UndoMgr *undoMgr = g_storageInstance->GetPdb(g_defaultPdbId)->GetUndoMgr();

    PageId pageId;
    undoMgr->LoadUndoZone(zoneId, pageId);
    UndoZone* newUndoZone = nullptr;
    undoMgr->GetUndoZone(zoneId, &newUndoZone);

    syncPointGroup->SyncPoint(2);
    UndoZone* undoZone = m_undozones.front();

    ASSERT_EQ(undoZone, newUndoZone);
}

void UTHeapMultiThread::ThreadGetUndoZone(UTHeapMultiThread *ptr, SyncPointGroup *syncPointGroup)
{
    ptr->GetUndoZoneThread(syncPointGroup);
}

void UTHeapMultiThread::ThreadAsynRollback(UTHeapMultiThread *ptr, SyncPointGroup *syncPointGroup)
{
    ptr->AsynRollbackThread(syncPointGroup);
}

/*
 * Page rollback case.
 * There are two transactions runs parallel.
 * Assume the zoneId is 2.
 *            T1                              T2
 *       GetUndoZone
 *  Entry  AllocateZoneMemory          Entry AllocateUndoZone
 *  DstoreUndoFI::GET_UNDO_ZONE    <--   DstoreUndoFI::GET_UNDO_ZONE
 *       lock undozone
 * new undozone and init undozone
 *   record m_undoZones[zid] 
 *      unlock undozone
 *    UndoZone addr is P1
 *  DstoreUndoFI::RECOVER_UNDO_ZONE -->  DstoreUndoFI::RECOVER_UNDO_ZONE
 *                                           lock undozone
 *                                       m_undoZones[zid] not null 
 *                                           unlock undozone
 *                                         UndoZone addr is P2
 *  push zone addr P1 into queue
 *      (SyncPoint)                            (SyncPoint)
 *                                     get zone addr P1 from queue
 *                                              P1 == P2
 */

TEST_F(UTHeapMultiThread, UndoZoneAsynRollbackTest_level0)
{
    FAULT_INJECTION_ACTIVE_MODE_LEVEL(DstoreUndoFI::GET_UNDO_ZONE, 0, FI_GLOBAL, 0, 1);
    FAULT_INJECTION_ACTIVE_MODE_LEVEL(DstoreUndoFI::RECOVER_UNDO_ZONE, 0, FI_GLOBAL, 0, 1);
    SyncPointGroup syncPointGroup{2};

    m_pool.AddTask(ThreadGetUndoZone, this, &syncPointGroup);
    m_pool.AddTask(ThreadAsynRollback, this, &syncPointGroup);

    m_pool.WaitAllTaskFinish();
    FAULT_INJECTION_INACTIVE(DstoreUndoFI::GET_UNDO_ZONE, FI_GLOBAL);
    FAULT_INJECTION_INACTIVE(DstoreUndoFI::RECOVER_UNDO_ZONE, FI_GLOBAL);
}


void UTHeapMultiThread::RollbackThread(SyncPointGroup *syncPointGroup)
{
    BuildThreadLocalVar();
    /* wait for scan thread get snapshot first */
    syncPointGroup->SyncPoint(1);

    Transaction *txn = thrd->GetActiveTransaction();
    TransactionMgr *transactionMgr = g_storageInstance->GetPdb(g_defaultPdbId)->GetTransactionMgr();
    UndoMgr *undoMgr = g_storageInstance->GetPdb(g_defaultPdbId)->GetUndoMgr();
    txn->Start();
    txn->SetSnapshotCsn();

    std::string data = GenerateRandomString(100);
    HeapTuple *heapTuple = UTTableHandler::GenerateSpecificHeapTuple(data);
    ItemPointerData ctid = ThdUtTableHandler->InsertHeapTupAndCheckResult(heapTuple, true, txn->GetSnapshotData());
    DstorePfree(heapTuple);
    syncPointGroup->SyncPoint(1);

    ErrLog(DSTORE_DEBUG1, MODULE_HEAP, ErrMsg("notify to start cr read"));
    FAULT_INJECTION_NOTIFY(DstoreHeapFI::CONSTRUCT_CR_BEFORE_READ_BASE_PAGE);

    /* wait for scan thread copy page finish */
    FAULT_INJECTION_WAIT(DstoreHeapFI::CONSTRUCT_CR_AFTER_READ_BASE_PAGE);
    ErrLog(DSTORE_DEBUG1, MODULE_HEAP, ErrMsg("notify, after cr read finish."));
    Xid xid = txn->GetCurrentXid();
    txn->Abort();
    /* UNDO ZONE recycyle */
    UndoZone *undoZone = nullptr;
    undoMgr->GetUndoZone(xid.m_zoneId, &undoZone);
    CommitSeqNo csn = transactionMgr->m_csnMgr->GetLocalCsnMin();
    undoZone->Recycle(csn);

    XidStatus xidStatus{xid, txn};
    ASSERT_EQ(xidStatus.GetStatus(), TXN_STATUS_ABORTED);

    ErrLog(DSTORE_DEBUG1, MODULE_HEAP, ErrMsg("notify to start to construct CR."));
    FAULT_INJECTION_NOTIFY(DstoreHeapFI::CONSTRUCT_CR_PAGE);
}

void UTHeapMultiThread::HeapScanThread(SyncPointGroup *syncPointGroup)
{
    BuildThreadLocalVar();
    Transaction *txn = thrd->GetActiveTransaction();
    TransactionMgr *transactionMgr= g_storageInstance->GetPdb(g_defaultPdbId)->GetTransactionMgr();
    txn->Start();
    txn->SetSnapshotCsn();

    syncPointGroup->SyncPoint(2);

    /* wait for rollback thread insert one record */
    syncPointGroup->SyncPoint(2);
    std::unique_ptr<StorageRelationData> heapRel(new StorageRelationData());
    heapRel->tableSmgr = ThdUtTableHandler->GetHeapTabSmgr();
    heapRel->m_pdbId = g_defaultPdbId;

    {
        HeapScanHandler scan(g_storageInstance, thrd, heapRel.get());
        scan.Begin(txn->GetSnapshot());
        int tupleNums = 0;

        /*
         * There are 2 fault injection point set in SeqScan() function
         * DstoreHeapFI::CONSTRUCT_CR_BEFORE_READ_BASE_PAGE
         *    stop before copy page to CR buffer, and wait for notify.
         * DstoreHeapFI::CONSTRUCT_CR_PAGE
         *    stop before construct CR page, and wait for notify.
         */
        HeapTuple *heapTuple = scan.SeqScan();
        ASSERT_EQ(heapTuple, nullptr); /* we expect query nothing. */
        scan.End();
    }
    txn->Commit();
}

void UTHeapMultiThread::ThreadRollback(UTHeapMultiThread *ptr, SyncPointGroup *syncPointGroup)
{
    ptr->RollbackThread(syncPointGroup);
}

void UTHeapMultiThread::ThreadHeapScan(UTHeapMultiThread *ptr, SyncPointGroup *syncPointGroup)
{
    ptr->HeapScanThread(syncPointGroup);
}

/*
 * Data consistent issue
 * There are two transactions runs parallel.
 *            T1                         T2                        Undo Recycle
 *                               Begin, get the snapshot
 *       (SyncPoint)                 (SyncPoint)
 *  Begin, get the snapshot
 *      Insert a tuple
 *   (FaultInjectNotify)    ->    (FaultInjectWait)
 *                                  Read buffer
 *                               Copy page to CR buffer
 *                                  Unlock buffer
 *   (FaultInjectWait)      <-    (FaultInjectNotify)
 *        Abort
 *   Rollback the page
 *                                                               Set T1 as FROZEN
 *                                 (FaultInjectWait)    <-      (FaultInjectNotify)
 *                                 Construct CR page
 *                             [X]Think tuple is visible
 */
TEST_F(UTHeapMultiThread, DATA_CONSISTENT_ISSUE1_level0)
{
    FAULT_INJECTION_ACTIVE_MODE_LEVEL(DstoreHeapFI::CONSTRUCT_CR_BEFORE_READ_BASE_PAGE, 0, FI_GLOBAL, 0, 1);
    FAULT_INJECTION_ACTIVE_MODE_LEVEL(DstoreHeapFI::CONSTRUCT_CR_AFTER_READ_BASE_PAGE, 0, FI_GLOBAL, 0, 1);
    FAULT_INJECTION_ACTIVE_MODE_LEVEL(DstoreHeapFI::CONSTRUCT_CR_PAGE, 0, FI_GLOBAL, 0, 1);

    SyncPointGroup syncPointGroup{2};

    m_pool.AddTask(ThreadHeapScan, this, &syncPointGroup);
    m_pool.AddTask(ThreadRollback, this, &syncPointGroup);

    m_pool.WaitAllTaskFinish();

    FAULT_INJECTION_INACTIVE(DstoreHeapFI::CONSTRUCT_CR_BEFORE_READ_BASE_PAGE, FI_GLOBAL);
    FAULT_INJECTION_INACTIVE(DstoreHeapFI::CONSTRUCT_CR_AFTER_READ_BASE_PAGE, FI_GLOBAL);
    FAULT_INJECTION_INACTIVE(DstoreHeapFI::CONSTRUCT_CR_PAGE, FI_GLOBAL);
}


void UTHeapMultiThread::UpdateWithTrx1Commit(SyncPointGroup *syncPointGroup, ItemPointerData specCtid)
{
    BuildThreadLocalVar();
    syncPointGroup->SyncPoint(1);

    /* Update the tuple in T1, here T1 will be blocked by T2 until T2 end. */
    HeapTuple *newTuple = UTTableHandler::GenerateSpecificHeapTuple(std::string("c"));
    ItemPointerData newCtid = UpdateTuple(&specCtid, newTuple, INVALID_SNAPSHOT, false, ThdUtTableHandler);
    EXPECT_EQ(newCtid, specCtid);
    DstorePfreeExt(newTuple);
}

void UTHeapMultiThread::UpdateWithTrx2Abort(SyncPointGroup *syncPointGroup, ItemPointerData specCtid)
{
    BuildThreadLocalVar();
    /* Update the tuple in T2 */
    HeapTuple *newTuple = UTTableHandler::GenerateSpecificHeapTuple(std::string("b"));
    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetSnapshotCsn();
    ItemPointerData newCtid = UpdateTuple(&specCtid, newTuple, INVALID_SNAPSHOT, true, ThdUtTableHandler);
    EXPECT_EQ(newCtid, specCtid);

    syncPointGroup->SyncPoint(2);

    /* Wait until T1 see tuple is changed by T2. */
    FAULT_INJECTION_WAIT(DstoreHeapFI::READY_TO_WAIT_TRX_END);

    txn->Abort();

    DstorePfreeExt(newTuple);
}

void UTHeapMultiThread::UpdateThread1(UTHeapMultiThread *ptr, SyncPointGroup *syncPointGroup, ItemPointerData specCtid)
{
    ptr->UpdateWithTrx1Commit(syncPointGroup, specCtid);
}

void UTHeapMultiThread::UpdateThread2(UTHeapMultiThread *ptr, SyncPointGroup *syncPointGroup, ItemPointerData specCtid)
{
    ptr->UpdateWithTrx2Abort(syncPointGroup, specCtid);
}

/*
 * We concurrently update same tuple in 2 transaction bellow and T2 will abort.
 * Regardless of transaction isolation level, T1 should update successfully.
 *
 *           T1                        T2
 *                                    begin
 *                               update "a" to "b"
 *       (SyncPoint)               (SyncPoint)
 *          begin
 *  begin update "a" to "c"
 *   see tuple is changed
 *   (FaultInjectNotify)   ->    (FaultInjectWait)
 *      wait T2 end                  rollback
 *     update success
 *         commit
 *
 * Finally check the tuple value is "c".
 */
TEST_F(UTHeapMultiThread, HeapConcurrentUpdateWithTrxAbortTest_level0)
{
    FAULT_INJECTION_ACTIVE_MODE_LEVEL(DstoreHeapFI::READY_TO_WAIT_TRX_END, 0, FI_GLOBAL, 0, 1);

    /* Insert one specific tuple. */
    HeapTuple *specTuple = UTTableHandler::GenerateSpecificHeapTuple(std::string("a"));
    ItemPointerData specCtid = m_utTableHandler->InsertHeapTupAndCheckResult(specTuple);

    SyncPointGroup syncPointGroup{2};

    m_pool.AddTask(UpdateThread1, this, &syncPointGroup, specCtid);
    m_pool.AddTask(UpdateThread2, this, &syncPointGroup, specCtid);

    m_pool.WaitAllTaskFinish();

    /* Check the tuple value should be "c". */
    HeapTuple *newTuple = UTTableHandler::GenerateSpecificHeapTuple(std::string("c"));
    HeapTuple *resTuple = m_utTableHandler->FetchHeapTuple(&specCtid);
    EXPECT_TRUE(resTuple->GetDiskTupleSize() == newTuple->GetDiskTupleSize());
    int32 dataSize = newTuple->GetDiskTupleSize() - HEAP_DISK_TUP_HEADER_SIZE;
    EXPECT_EQ(memcmp((void*)resTuple->GetDiskTuple()->GetData(), newTuple->GetDiskTuple()->GetData(), dataSize), 0);
    DstorePfreeExt(newTuple);
    DstorePfreeExt(resTuple);
    DstorePfreeExt(specTuple);

    FAULT_INJECTION_INACTIVE(DstoreHeapFI::READY_TO_WAIT_TRX_END, FI_GLOBAL);
}