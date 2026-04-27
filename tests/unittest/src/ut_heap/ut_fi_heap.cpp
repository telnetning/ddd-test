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
#include "tuple/dstore_data_tuple.h"
#include "undo/dstore_undo_zone.h"
#include "page/dstore_heap_page.h"
#include "ut_wal/ut_wal_basic.h"
#include "ut_heap/ut_heap.h"
#include "ut_heap/ut_heap_wal.h"

/*
 * UndoRecPtr: the undo record pointer.
 * nextAppendUndoPtr: next undo record pointer you can insert in this undozone.
 * curTailUndoPtr: the current undo tail pointer of a transaction stored in transaction slot.
 * spaceTailUndoPtr: the tail pointer of the undo space of a transaction stored in transaction slot.
 *
 * Assume Heap Insert Undo Record Size is |-----|
 * Assume Heap Update Undo Record Size is |----------|
 *
 *  Step1 -------------Transaction1-------------
 *
 *  insert one tuple, UndoRecPtr:       |
 *                           undo page: |-----|
 *                    nextAppendUndoPtr:      |
 *
 *  insert one tuple, UndoRecPtr:             |
 *                           undo page: |-----|-----|
 *                    nextAppendUndoPtr:            |
 *
 *  insert one tuple, UndoRecPtr:                   |
 *                           undo page: |-----|-----|-----|
 *                    nextAppendUndoPtr:                  |
 *
 *  Step2 record the current page, used for ConstructCR
 *
 *  Step3 before abort transaction1
 *                    curTailUndoPtr:                     |
 *                           undo page: |-----|-----|-----|
 *                    spaceTailUndoPtr:                   |
 *                    nextAppendUndoPtr:                  |
 *
 *  Step4 after abort transaction1
 *                    curTailUndoPtr:   |
 *                           undo page: |-----|-----|-----|
 *                    spaceTailUndoPtr:                   |
 *                    nextAppendUndoPtr:                  |
 *
 * Step5 release this zone and refresh nextAppendUndoPtr
 *  call ReleaseZoneId
 *  call RestoreUndoZoneFromTxnSlots (refresh nextAppendUndoPtr by spaceTailUndoPtr instead of curTailUndoPtr)
 *
 *  after RestoreUndoZoneFromTxnSlots
 *                    curTailUndoPtr:   |
 *                           undo page: |-----|-----|-----|
 *                    spaceTailUndoPtr:                   |
 *                    nextAppendUndoPtr:                  |
 *
 * Step6 -------------Transaction2-------------
 *
 *  insert one tuple, UndoRecPtr:                         |
 *                           undo page: |-----|-----|-----|-----|
 *                    nextAppendUndoPtr:                        |
 *
 *  update this tuple, UndoRecPtr:                              |
 *                           undo page: |-----|-----|-----|-----|----------|
 *                    nextAppendUndoPtr:                                   |
 *
 *  insert one tuple, UndoRecPtr:                                          |
 *                           undo page: |-----|-----|-----|-----|----------|-----|
 *                    nextAppendUndoPtr:                                         |
 *
 *
 * Step7 -------------Transaction3-------------
 *
 *  ConstructCR, Transaction1 is aborted, TD will rollback, undo record can be fetched.
 *  The undo record related to transaction1 is still in the undo page.
 *
 */
TEST_F(UTHeap, HeapInsertUndoTest_level0)
{
    Transaction *txn = thrd->GetActiveTransaction();
    TransactionMgr *transactionMgr = g_storageInstance->GetPdb(g_defaultPdbId)->GetTransactionMgr();
    UndoMgr *undoMgr = g_storageInstance->GetPdb(g_defaultPdbId)->GetUndoMgr();

    /* step1: start transaction1, insert three tuple */
    txn->Start();
    txn->SetSnapshotCsn();
    const int ctidNum = 3;
    ItemPointerData ctids[ctidNum];
    for (int i = 0; i < ctidNum; i++) {
        ctids[i] = InsertSpecificHeapTuple(std::string("123"), txn->GetSnapshotData(), true);
    }

    /* step2: record the page, used for ConstructCR */
    BufferDesc *bufferDesc = g_storageInstance->GetBufferMgr()->Read(g_defaultPdbId,
        ctids[2].GetPageId(), LW_EXCLUSIVE);
    char page[BLCKSZ];
    errno_t rc = memcpy_s(page, BLCKSZ, bufferDesc->GetPage(), BLCKSZ);
    storage_securec_check(rc, "\0", "\0");
    g_storageInstance->GetBufferMgr()->UnlockAndRelease(bufferDesc);

    /* step3: get undo zone, get curTailUndoPtr and spaceTailUndoPtr */
    Xid xid = txn->GetCurrentXid();
    ZoneId zoneId = xid.m_zoneId;
    UndoZone *undoZone = nullptr;
    undoMgr->GetUndoZone(zoneId, &undoZone);
    UndoRecPtr curTailPtr;
    undoZone->GetSlotCurTailUndoPtr(xid, curTailPtr);
    UndoRecPtr spaceTailPTr = undoZone->m_txnSlotManager->GetLastSpaceUndoRecPtr();
    UndoRecPtr fistTailPtr = INVALID_UNDO_RECORD_PTR;
    int64 fistSlotId = -1; // slot id is from 0
    undoZone->m_txnSlotManager->GetFirstSpaceUndoRecPtr(fistTailPtr, fistSlotId);
    ASSERT_EQ(curTailPtr == spaceTailPTr, true);
    ASSERT_EQ(curTailPtr == fistTailPtr, true);
    ASSERT_EQ(fistSlotId == xid.m_logicSlotId, true);
    /* step4: abort this txn, and get curTailUndoPtr and spaceTailUndoPtr */
    txn->Abort();
    UndoRecPtr afterCurTailPtr;
    undoZone->GetSlotCurTailUndoPtr(xid, afterCurTailPtr);
    UndoRecPtr afterSpaceTailPTr = undoZone->m_txnSlotManager->GetLastSpaceUndoRecPtr();
    undoZone->m_txnSlotManager->GetFirstSpaceUndoRecPtr(fistTailPtr, fistSlotId);
    ASSERT_EQ(afterCurTailPtr != afterSpaceTailPTr, true);
    ASSERT_EQ(afterCurTailPtr == INVALID_UNDO_RECORD_PTR, true);
    ASSERT_EQ(afterSpaceTailPTr == fistTailPtr, true);
    ASSERT_EQ(fistSlotId == xid.m_logicSlotId, true);

    /* step5: release this zone, so next transaction can get the same zone and refresh m_nextAppendUndoPtr */
    undoMgr->ReleaseZoneId(zoneId);
    undoZone->RestoreUndoZoneFromTxnSlots();
    /* step6: start transaction2, insert tuple, update this tuple, insert tuple */
    txn->Start();
    txn->SetSnapshotCsn();
    ItemPointerData ctid = InsertSpecificHeapTuple(std::string("123"), txn->GetSnapshotData(), true);
    HeapTuple *heapTuple = m_utTableHandler->GenerateSpecificHeapTuple(std::string(100, 'a'));
    ItemPointerData updateCtid = UpdateTupAndCheckResult(&ctid, heapTuple, txn->GetSnapshotData(), true);
    ItemPointerData insertCtid = InsertSpecificHeapTuple(std::string("123"), txn->GetSnapshotData(), true);
    xid = txn->GetCurrentXid();
    UndoRecPtr nextCurTailPtr;
    undoZone->GetSlotCurTailUndoPtr(xid, nextCurTailPtr);
    UndoRecPtr nextSpaceTailPTr = undoZone->m_txnSlotManager->GetLastSpaceUndoRecPtr();
    ASSERT_EQ(nextCurTailPtr == nextSpaceTailPTr, true);
    UndoRecPtr nextFistTailPtr = INVALID_UNDO_RECORD_PTR;
    int64 nextfistSlotId = -1; // slot id is from 0
    undoZone->m_txnSlotManager->GetFirstSpaceUndoRecPtr(nextFistTailPtr, nextfistSlotId);
    ASSERT_EQ(nextFistTailPtr == fistTailPtr, true);
    ASSERT_EQ(nextfistSlotId == fistSlotId, true);
    ASSERT_EQ(nextFistTailPtr != nextSpaceTailPTr, true);
    txn->Commit();

    /* step7: strat transaction3, ConstructCR, TD xidStatus is aborted, do TD rollback.
     * it won't core, because old undo record won't be covered.
     */
    HeapPage *heapPage = static_cast<HeapPage *>(static_cast<void *>(page));
    CRContext context;
    txn->Start();
    txn->SetSnapshotCsn();
    context.snapshot = txn->GetSnapshot();
    context.currentXid = txn->GetCurrentXid();
    context.pdbId = g_defaultPdbId;
    RetStatus ret = heapPage->ConstructCR(txn, &context);
    ASSERT_TRUE(ret == DSTORE_SUCC);
    txn->Commit();
}

/*
 * this case when same transaction lock two tuple, when call TupleIsChanged, incase double rollback td.
 */
TEST_F(UTHeap, HeapLockTwoTupleTest_level0)
{
    Transaction *txn = thrd->GetActiveTransaction();

    /* Step 1: Transaction1: Insert tuple1(ctid1), tuple2(ctid2), both will use tdId 0, because of same transaction. */
    txn->Start();
    txn->SetSnapshotCsn();
    HeapTuple* heapTuple = m_utTableHandler->GenerateSpecificHeapTuple("test_tuple");
    ItemPointerData ctid1 = m_utTableHandler->InsertHeapTupAndCheckResult(heapTuple, true, txn->GetSnapshotData());
    ItemPointerData ctid2 = m_utTableHandler->InsertHeapTupAndCheckResult(heapTuple, true, txn->GetSnapshotData());
    CheckTupleTdId(ctid1, 0);
    CheckTupleTdId(ctid2, 0);
    txn->Commit();

    /* Step2: Transaction2: Lock tuple1(ctid1), tuple2(ctid2), both will use tdId 1, because of same transaction. */
    txn->Start();
    txn->SetSnapshotCsn();
    int result = LockUnchangedTuple(ctid1, txn->GetSnapshotData(), true);
    ASSERT_EQ(result, DSTORE_SUCC);
    result = LockUnchangedTuple(ctid2, txn->GetSnapshotData(), true);
    ASSERT_EQ(result, DSTORE_SUCC);
    Xid xid = txn->GetCurrentXid();
    ASSERT_EQ(ctid1.GetPageId(), ctid2.GetPageId());
    CheckTupleLockerTdId(ctid1, 1);
    CheckTupleLockerTdId(ctid2, 1);
    TD* td = GetTd(ctid1.GetPageId(), 1);
    ASSERT_EQ(td->m_xid, INVALID_XID.m_placeHolder);
    ASSERT_EQ(td->m_lockerXid, xid.m_placeHolder);
    ASSERT_EQ(td->m_status, static_cast<uint16>(TDStatus::OCCUPY_TRX_IN_PROGRESS));
    txn->Commit();

    /* Step3: Transaction3: insert tuple3(ctid3), will use tdId 2, and set transactionslot inprogress */
    txn->Start();
    txn->SetSnapshotCsn();
    ItemPointerData ctid3 = m_utTableHandler->InsertHeapTupAndCheckResult(heapTuple, true, txn->GetSnapshotData());
    xid = txn->GetCurrentXid();
    ASSERT_EQ(ctid1.GetPageId(), ctid3.GetPageId());
    CheckTupleTdId(ctid3, 2);
    txn->Commit();
    SetTransactionSlotStatus(xid, TXN_STATUS_IN_PROGRESS);

    /* Step4: Transaction4: insert tuple4(ctid4), will use tdId 3, and set transactionslot inprogress*/
    txn->Start();
    txn->SetSnapshotCsn();
    ItemPointerData ctid4 = m_utTableHandler->InsertHeapTupAndCheckResult(heapTuple, true, txn->GetSnapshotData());
    xid = txn->GetCurrentXid();
    ASSERT_EQ(ctid1.GetPageId(), ctid4.GetPageId());
    CheckTupleTdId(ctid4, 3);
    txn->Commit();
    SetTransactionSlotStatus(xid, TXN_STATUS_IN_PROGRESS);

    /* Step5: Transaction5: update tuple1(ctid1), will use tdid 1. (because tdid 1 will be set unoccupy). */
    txn->Start();
    txn->SetSnapshotCsn();
    ItemPointerData newCtid1 = UpdateTuple(&ctid1, heapTuple, txn->GetSnapshotData(), true);
    xid = txn->GetCurrentXid();
    ASSERT_EQ(ctid1.GetPageId(), newCtid1.GetPageId());
    CheckTupleTdId(newCtid1, 1);
    CheckTupleLockerTdId(newCtid1, INVALID_TD_SLOT);
    td = GetTd(newCtid1.GetPageId(), 1);
    ASSERT_EQ(td->m_lockerXid, INVALID_XID.m_placeHolder);
    ASSERT_EQ(td->m_xid, xid.m_placeHolder);
    txn->Commit();
    SetTransactionSlotStatus(xid, TXN_STATUS_IN_PROGRESS);

    /* Step6: Transaction6: update tuple2(ctid2), will use tdid 0, becase only td0 xid transaction end */
    txn->Start();
    txn->SetSnapshotCsn();
    ItemPointerData newCtid2 = UpdateTuple(&ctid2, heapTuple, txn->GetSnapshotData(), true);
    ASSERT_EQ(ctid1.GetPageId(), newCtid2.GetPageId());
    CheckTupleTdId(newCtid2, 0);
    CheckTupleLockerTdId(newCtid2, INVALID_TD_SLOT);
    txn->Commit();
}

/*
 * this case used to ensure, when alloctd, if td's lockerxid was clear, must clear the related tuple's lockerTdId. 
 */
TEST_F(UTHeap, HeapAllocTdClearTupleLockerTdIdTest_level0)
{
    Transaction *txn = thrd->GetActiveTransaction();

    /* Step 1: Insert six tuple, will use tdId(0, 1, 2, 3, 0, 1) */
    const int tupleNum = 6;
    ItemPointerData ctids[tupleNum];
    Xid xids[tupleNum];
    for (int i = 0; i < tupleNum; i++) {
        txn->Start();
        txn->SetSnapshotCsn();
        HeapTuple* heapTuple = m_utTableHandler->GenerateSpecificHeapTuple("test_tuple");
        ctids[i] = m_utTableHandler->InsertHeapTupAndCheckResult(heapTuple, true, txn->GetSnapshotData());
        xids[i] = txn->GetCurrentXid();
        CheckTupleTdId(ctids[i], i % DEFAULT_TD_COUNT);
        CheckTupleLockerTdId(ctids[i], INVALID_TD_SLOT);
        txn->Commit();
    }

    /* Step2: Lock tuple1(ctids[0]), will use tdId(2) */
    txn->Start();
    txn->SetSnapshotCsn();
    int result = LockUnchangedTuple(ctids[0], txn->GetSnapshotData(), true);
    CommitSeqNo csn = txn->GetSnapshotData()->GetCsn();
    ASSERT_EQ(result, DSTORE_SUCC);
    CheckTupleTdId(ctids[0], 0);
    CheckTupleLockerTdId(ctids[0], 2);
    txn->Commit();

    /* Step3: Lock tuple2(ctids[1]), will use tdId(0), because of csn move on, td reset */
    CsnMgr *csnMgr = g_storageInstance->GetCsnMgr();
    txn->Start();
    txn->SetSnapshotCsn();
    csnMgr->SetLocalCsnMin(csn);
    csnMgr->SetFlashbackCsnMin(csn);
    HeapLockTupleContext lockTupContext;
    lockTupContext.ctid = ctids[1];
    lockTupContext.needRetTup = true;
    std::unique_ptr<StorageRelationData> heapRel(new StorageRelationData());
    heapRel->tableSmgr = m_utTableHandler->GetHeapTabSmgr();
    heapRel->m_pdbId = g_defaultPdbId;
    {
        HeapLockTupHandler heapLockTup(g_storageInstance, thrd, heapRel.get());
        RetStatus ret = heapLockTup.LockNewestTuple(&lockTupContext);
    }
    CheckTupleTdId(ctids[1], 1);
    CheckTupleLockerTdId(ctids[1], 0);
    CheckTupleTdId(ctids[0], 0);
    CheckTupleLockerTdId(ctids[0], INVALID_TD_SLOT);
    txn->Commit();
}

/*
 * this case used to ensure, when undoheap, noneed to set Td's lockerXid invalid, because lock tuple don't record undo. 
 */
TEST_F(UTHeap, HeapUndoLockerTdIdTest_level0)
{
    Transaction *txn = thrd->GetActiveTransaction();

    /* Step 1: Insert one tuple(ctid), will use tdId(0) */
    txn->Start();
    txn->SetSnapshotCsn();
    HeapTuple* heapTuple = m_utTableHandler->GenerateSpecificHeapTuple("test_tuple");
    ItemPointerData ctid = m_utTableHandler->InsertHeapTupAndCheckResult(heapTuple, true, txn->GetSnapshotData());
    CheckTupleTdId(ctid, 0);
    CheckTupleLockerTdId(ctid, INVALID_TD_SLOT);
    txn->Commit();

    /* Step2: Lock tuple(ctid), will use tdId(1) , updata tuple(ctid), will use tdId(1) */
    txn->Start();
    txn->SetSnapshotCsn();
    int result = LockUnchangedTuple(ctid, txn->GetSnapshotData(), true);
    ASSERT_EQ(result, DSTORE_SUCC);
    CheckTupleTdId(ctid, 0);
    CheckTupleLockerTdId(ctid, 1);
    Xid xid = txn->GetCurrentXid();

    ItemPointerData newCtid = UpdateTuple(&ctid, heapTuple, txn->GetSnapshotData(), true);
    ASSERT_EQ(newCtid, ctid);
    CheckTupleTdId(ctid, 1);
    CheckTupleLockerTdId(ctid, INVALID_TD_SLOT);
    TD* td = GetTd(ctid.GetPageId(), 1);
    ASSERT_EQ(td->m_lockerXid, xid.m_placeHolder);

    txn->Abort();
    /* after abort, will rollback update, tuple lockerTdId is INVALID_TD_SLOT because before record undo has set tuple
     * lockerTdId INVALID_TD_SLOT. */
    CheckTupleTdId(ctid, 0);
    CheckTupleLockerTdId(ctid, INVALID_TD_SLOT);
    td = GetTd(ctid.GetPageId(), 1);
    ASSERT_EQ(td->m_lockerXid, xid.m_placeHolder);
}

#ifdef ENABLE_FAULT_INJECTION
/*
 *  Step1 Get endPlsn of WalFile
 *
 *  Step2 Fault injection set undozone fully free page num zero. Start transaction  batch insert small tuples.
 *
 *  Step3 Wait wal flush to disk
 *
 *  Step4 Check all wal type and num. Wal generate sequence list below.
 *  1 WAL_UNDO_SET_TXN_PAGE_INITED | 7 WAL_UNDO_EXTEND_PAGE_RING_NEW_PAGE
 *  1 WAL_UNDO_EXTEND_PAGE_RING_PREV_PAGE | 1 WAL_UNDO_EXTEND_PAGE_RING_NEXT_PAGE | 7 WAL_UNDO_EXTEND_PAGE_RING_NEW_PAGE
 *  1 WAL_UNDO_INSERT_RECORD | 1 WAL_HEAP_BATCH_INSERT
 */
TEST_F(HeapWalTest, HeapBatchInsertExtendUndoPageTest_level0)
{
    /* Step1: get endPlsn of WalFile */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);

    /* Step2: Fault injection, set undo zone fullyfreepage num zero. And batch insert small tuple. */
    FAULT_INJECTION_ACTIVE(DstoreUndoFI::SET_FREE_PAGE_NUM_ZERO, FI_GLOBAL);
    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    const uint16 tupleNum = 10;
    HeapTuple *heapTuples[tupleNum];
    uint32 tupleLens[tupleNum];
    for (int i = 0; i < tupleNum; i++) {
        HeapTuple *heapTuple = m_utTableHandler->GenerateSpecificHeapTuple(std::string(100, 'a'));
        heapTuples[i] = heapTuple;
        tupleLens[i] = heapTuple->GetDiskTupleSize();
    }
    ItemPointerData ctids[tupleNum];
    m_utTableHandler->BatchInsertHeapTupsAndCheckResult(heapTuples, tupleNum, ctids, INVALID_SNAPSHOT, true);
    txn->Commit();
    for (int i = 0; i < tupleNum; i++) {
        ASSERT_EQ(ctids[i].GetOffset(), i + 1);
    }
    FAULT_INJECTION_INACTIVE(DstoreUndoFI::SET_FREE_PAGE_NUM_ZERO, FI_GLOBAL);

    /* Step3: wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());

    /* step4: Check all generate wal type and num. */
    std::vector<WalRecordRedoInfo *> recordList1;
    ASSERT_TRUE(STORAGE_FUNC_SUCC(ReadRecordsAfterPlsn(plsn, WAL_HEAP_BATCH_INSERT, recordList1)));
    ASSERT_EQ(recordList1.size(), 1);

    std::vector<WalRecordRedoInfo *> recordList2;
    ASSERT_TRUE(STORAGE_FUNC_SUCC(ReadRecordsAfterPlsn(plsn, WAL_UNDO_INSERT_RECORD, recordList2)));
    ASSERT_EQ(recordList2.size(), 1);

    std::vector<WalRecordRedoInfo *> recordList3;
    ASSERT_TRUE(STORAGE_FUNC_SUCC(ReadRecordsAfterPlsn(plsn, WAL_UNDO_EXTEND_PAGE_RING_PREV_PAGE, recordList3)));
    ASSERT_EQ(recordList3.size(), 1);

    std::vector<WalRecordRedoInfo *> recordList4;
    ASSERT_TRUE(STORAGE_FUNC_SUCC(ReadRecordsAfterPlsn(plsn, WAL_UNDO_EXTEND_PAGE_RING_NEXT_PAGE, recordList4)));
    ASSERT_EQ(recordList4.size(), 1);

    std::vector<WalRecordRedoInfo *> recordList5;
    ASSERT_TRUE(STORAGE_FUNC_SUCC(ReadRecordsAfterPlsn(plsn, WAL_UNDO_EXTEND_PAGE_RING_NEW_PAGE, recordList5)));
    ASSERT_EQ(recordList5.size(), 14);

    std::vector<WalRecordRedoInfo *> recordList6;
    ASSERT_TRUE(STORAGE_FUNC_SUCC(ReadRecordsAfterPlsn(plsn, WAL_UNDO_SET_TXN_PAGE_INITED, recordList6)));
    ASSERT_EQ(recordList6.size(), 1);
}

#endif

/* this case incase fetch undo record that has been recycled. */
TEST_F(UTHeap, HeapRestoreUndoZoneTest_TIER1_level0)
{
    Transaction *txn = thrd->GetActiveTransaction();
    TransactionMgr *transactionMgr = g_storageInstance->GetPdb(g_defaultPdbId)->GetTransactionMgr();
    UndoMgr *undoMgr = g_storageInstance->GetPdb(g_defaultPdbId)->GetUndoMgr();
    CsnMgr *csnMgr = g_storageInstance->GetCsnMgr();

    /* Step1: start transaction1, insert three tuple */
    txn->Start();
    txn->SetSnapshotCsn();
    const int ctidNum = 3;
    ItemPointerData ctids[ctidNum];
    for (int i = 0; i < ctidNum; i++) {
        ctids[i] = InsertSpecificHeapTuple(std::string("123"), txn->GetSnapshotData(), true);
    }
    Xid xid1 = txn->GetCurrentXid();
    CommitSeqNo csn = txn->GetSnapshotData()->GetCsn();
    txn->Commit();

    /* Step2: move on csn, Transaction1 can recycle, call RestoreUndoZoneFromTxnSlots */
    csnMgr->SetLocalCsnMin(csn + 1);
    csnMgr->SetFlashbackCsnMin(csn + 1);
    UndoZone *undoZone = nullptr;
    undoMgr->GetUndoZone(xid1.m_zoneId, &undoZone);
    undoZone->RestoreUndoZoneFromTxnSlots();

    /* Step3: insert one tuple, update this tuple, insert one tuple */
    txn->Start();
    txn->SetSnapshotCsn();
    ItemPointerData ctid = InsertSpecificHeapTuple(std::string("123"), txn->GetSnapshotData(), true);
    HeapTuple *heapTuple = m_utTableHandler->GenerateSpecificHeapTuple(std::string(100, 'a'));
    ItemPointerData updateCtid = UpdateTupAndCheckResult(&ctid, heapTuple, txn->GetSnapshotData(), true);
    ItemPointerData insertCtid = InsertSpecificHeapTuple(std::string("123"), txn->GetSnapshotData(), true);
    Xid xid2 = txn->GetCurrentXid();
    ASSERT_EQ(xid1.m_zoneId, xid2.m_zoneId);
    txn->Commit();

    /* Step4: call RestoreUndoZoneFromTxnSlots */
    undoZone->RestoreUndoZoneFromTxnSlots();
}

TEST_F(UTHeap, TestCrConstruct)
{
    /* Step 1: Insert a tuple into page */
    Transaction *trx = thrd->GetActiveTransaction();
    trx->Start();
    trx->SetSnapshotCsn();
    HeapTuple* heapTuple1 = m_utTableHandler->GenerateSpecificHeapTuple("test_tuple_1");
    ItemPointerData ctid1 = m_utTableHandler->InsertHeapTupAndCheckResult(heapTuple1, true, trx->GetSnapshotData());
    CheckTupleTdId(ctid1, 0);
    CheckTupleLockerTdId(ctid1, INVALID_TD_SLOT);
    /* Do not commit here */
    Xid xid = thrd->GetCurrentXid();

    /* Step 2: Start a new transaction */
    thrd->CreateAutonomousTrx();
    trx = thrd->GetActiveTransaction();
    trx->Start();
    trx->BeginTransactionBlock();
    trx->Commit(); /* Just commit 'begin' command */

    trx->SetSnapshotCsn();

    HeapTuple* heapTuple2 = m_utTableHandler->GenerateSpecificHeapTuple("test_tuple_2");
    ItemPointerData ctid2 = m_utTableHandler->InsertHeapTupAndCheckResult(heapTuple2, true, trx->GetSnapshotData());
    CheckTupleTdId(ctid2, 1);
    CheckTupleLockerTdId(ctid2, INVALID_TD_SLOT);

    trx->EndTransactionBlock();
    trx->Commit(); 
    thrd->DestroyAutonomousTrx();

    /* Step 3: construct CR */
    HeapPage crPage;
    trx = thrd->GetActiveTransaction();
    ConsistentReadContext crContext;
    crContext.pdbId = g_defaultPdbId;
    crContext.pageId = ctid1.val.m_pageid;
    crContext.currentXid = trx->GetCurrentXid();
    crContext.snapshot = trx->GetSnapshot();
    crContext.dataPageExtraInfo = nullptr;
    crContext.destPage = &crPage;
    crContext.crBufDesc = nullptr;
    g_storageInstance->GetBufferMgr()->ConsistentRead(crContext);
    EXPECT_EQ(crPage.GetMaxOffset(), 2);
    /* Check tuples and TDs */
    HeapTuple tuple;
    crPage.GetTuple(&tuple, ctid1.GetOffset());
    EXPECT_EQ(tuple.m_diskTuple->GetTdStatus(), ATTACH_TD_AS_NEW_OWNER);
    TD *td = crPage.GetTd(tuple.GetTdId());
    EXPECT_EQ(td->GetStatus(), TDStatus::OCCUPY_TRX_IN_PROGRESS);
    EXPECT_EQ(td->GetXid(), xid);
    EXPECT_EQ(crPage.IsValidOffset(ctid2.GetOffset()), false);

    trx->Commit();
}
