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
#include "common/dstore_datatype.h"
#include "tablespace/dstore_data_segment_context.h"
#include "ut_heap/ut_heap.h"
#include "ut_undo/ut_undo_zone.h"
#include "undo/dstore_undo_zone_txn_mgr.h"

void UTHeap::MultiUpdateTest(std::vector<HeapTuple *> heapTupsForInsert, std::vector<HeapTuple *> heapTupsForUpdate,
                        std::vector<HeapTuple *> HeapTupsForUpdate2)
{
    uint32 i = 0;
    std::vector<ItemPointerData> insertedCtids;
    std::vector<ItemPointerData> update1NewCtids;
    std::vector<ItemPointerData> update2NewCtids;
    /* Step 1: Insert tuples */
    for (i = 0; i < heapTupsForInsert.size(); ++i) {
        ItemPointerData ctid = m_utTableHandler->InsertHeapTupAndCheckResult(heapTupsForInsert[i]);
        insertedCtids.push_back(ctid);
    }

    /* Step 2: Get a snapshot and save it */
    SnapshotData insertSpecialSnapshot = ConstructCurSnapshot();

    /* Step 3: update tuple using updateTuple1 */
    for(i = 0; i < heapTupsForUpdate.size(); ++i) {
        ItemPointerData newCtid = UpdateTupAndCheckResult(&insertedCtids[i], heapTupsForUpdate[i]);
        ASSERT_EQ(newCtid, insertedCtids[i]); /* Check ctid is not changed */
        update1NewCtids.push_back(newCtid);
    }

    /* Step 4: Get a snapshot and save it */
    SnapshotData update1SpecialSnapshot = ConstructCurSnapshot();

    /* Step 5: fetch tuple and check result from special insert snapshot */
    for (i = 0; i < insertedCtids.size(); ++i) {
        m_utTableHandler->FetchHeapTupAndCheckResult(heapTupsForInsert[i], &insertedCtids[i], &insertSpecialSnapshot,
                                                     false);
    }

    /* Step 6: update tuple using tuple2 */
    for (i = 0; i < HeapTupsForUpdate2.size(); ++i) {
        ItemPointerData newCtid = UpdateTupAndCheckResult(&update1NewCtids[i], HeapTupsForUpdate2[i]);
        ASSERT_EQ(newCtid, update1NewCtids[i]); /* Check ctid is not changed */
        update2NewCtids.push_back(newCtid);
    }

    /* Step 7: fetch tuple and check result from special insert snapshot */
    for (i = 0; i < insertedCtids.size(); ++i) {
        m_utTableHandler->FetchHeapTupAndCheckResult(heapTupsForInsert[i], &insertedCtids[i], &insertSpecialSnapshot,
                                                     false);
    }

    /* Step 8: fetch tuple and check result from special update1 snapshot */
    for (i = 0; i < update1NewCtids.size(); ++i) {
        m_utTableHandler->FetchHeapTupAndCheckResult(heapTupsForUpdate[i], &update1NewCtids[i], &update1SpecialSnapshot,
                                                     false);
    }
}

void UTHeap::RollbackUpdateTest(std::vector<HeapTuple *> tupsForInsert, std::vector<HeapTuple *> tupsForUpdate,
                        std::vector<HeapTuple *> tupsForUpdate2)
{
    uint32 i = 0;
    std::vector<ItemPointerData> insertedCtids;
    std::vector<ItemPointerData> update1NewCtids;
    std::vector<ItemPointerData> update2NewCtids;
    /* Step 1: Insert tuples */
    for (i = 0; i < tupsForInsert.size(); ++i) {
        ItemPointerData ctid = m_utTableHandler->InsertHeapTupAndCheckResult(tupsForInsert[i]);
        insertedCtids.push_back(ctid);
    }
    /* Step 2: update tuple using updateTuple1 */
    thrd->GetActiveTransaction()->Start();
    for(i = 0; i < tupsForUpdate.size(); ++i) {
        ItemPointerData newCtid = UpdateTupAndCheckResult(&insertedCtids[i], tupsForUpdate[i], INVALID_SNAPSHOT, true);
        ASSERT_EQ(newCtid, insertedCtids[i]); /* Check ctid is not changed */
        update1NewCtids.push_back(newCtid);
    }
    thrd->GetActiveTransaction()->Abort();
    /* Step 3: fetch tuple and check result from special insert snapshot */
    for (i = 0; i < insertedCtids.size(); ++i) {
        m_utTableHandler->FetchHeapTupAndCheckResult(tupsForInsert[i], &insertedCtids[i]);
    }
}

void UTHeap::SetTransactionSlotStatus(Xid xid, TrxSlotStatus status)
{
    UndoMgr *undoMgr = g_storageInstance->GetPdb(g_defaultPdbId)->GetUndoMgr();
    UndoZone* undoZone = nullptr;
    undoMgr->GetUndoZone(static_cast<int>(xid.m_zoneId), &undoZone);
    PageId slotPageId = undoZone->m_txnSlotManager->GetTxnSlotPageId(xid.m_logicSlotId);
    BufferDesc *txnPageBuf = undoZone->m_txnSlotManager->ReadTxnSlotPageBuf(slotPageId, LW_EXCLUSIVE);
    TransactionSlotPage *txnPage = undoZone->m_txnSlotManager->GetTxnSlotPage(txnPageBuf);
    uint32 slotId = TransactionSlotPage::GetSlotId(xid.m_logicSlotId);
    TransactionSlot *slot = txnPage->GetTransactionSlot(slotId);
    slot->status = status;
    (void)g_storageInstance->GetBufferMgr()->MarkDirty(txnPageBuf);
    undoZone->m_txnSlotManager->UnlockAndReleaseTxnSlotPageBuf(txnPageBuf);
}

void UTHeap::CheckTupleTdId(ItemPointerData ctid, TdId expectTdId)
{
    BufferDesc *bufferDesc = g_storageInstance->GetBufferMgr()->Read(g_defaultPdbId, ctid.GetPageId(),
        LW_SHARED);
    HeapPage *page = static_cast<HeapPage *>(bufferDesc->GetPage());
    HeapTuple tuple;
    page->GetTuple(&tuple, ctid.GetOffset());
    StorageAssert(tuple.GetDiskTuple()->GetTdId() == expectTdId);
    g_storageInstance->GetBufferMgr()->UnlockAndRelease(bufferDesc);
}

void UTHeap::CheckTupleLockerTdId(ItemPointerData ctid, TdId expectTdId)
{
    BufferDesc *bufferDesc = g_storageInstance->GetBufferMgr()->Read(g_defaultPdbId, ctid.GetPageId(),
        LW_SHARED);
    HeapPage *page = static_cast<HeapPage *>(bufferDesc->GetPage());
    HeapTuple tuple;
    page->GetTuple(&tuple, ctid.GetOffset());
    StorageAssert(tuple.GetDiskTuple()->GetLockerTdId() == expectTdId);
    g_storageInstance->GetBufferMgr()->UnlockAndRelease(bufferDesc);
}

TD* UTHeap::GetTd(PageId pageId, TdId tdId)
{
    BufferDesc *bufferDesc = g_storageInstance->GetBufferMgr()->Read(g_defaultPdbId, pageId,
        LW_SHARED);
    HeapPage *page = static_cast<HeapPage *>(bufferDesc->GetPage());
    TD* td = page->GetTd(tdId);
    g_storageInstance->GetBufferMgr()->UnlockAndRelease(bufferDesc);
    return td;
}

ItemPointerData UTHeap::InsertSpecificHeapTuple(std::string data, SnapshotData *snapshot, bool alreadyStartXact)
{
    HeapTuple *heapTuple = m_utTableHandler->GenerateSpecificHeapTuple(data);
    ItemPointerData ctid = m_utTableHandler->InsertHeapTupAndCheckResult(heapTuple, alreadyStartXact, snapshot);
    DstorePfreeExt(heapTuple);
    return ctid;
}

Bitmapset *UTHeap::FormReplicaIdentityKeys(UTTableHandler *utTableHandler)
{
    UTTableHandler *tableHandler = (utTableHandler == nullptr) ? m_utTableHandler : utTableHandler;
    Bitmapset *replicaKey = nullptr;
    static int fakeReplicaAttrNum = 1;
    replicaKey = BmsAddMember(replicaKey,
        fakeReplicaAttrNum - static_cast<int>(HeapTupleSystemAttr::DSTORE_FIRST_LOW_INVALID_HEAP_ATTRIBUTE_NUMBER));
    return replicaKey;
}

HeapTuple *UTHeap::InsertRandomHeapTuple()
{
    int natts = m_utTableHandler->GetHeapTupDesc()->natts;
    Datum values[natts];
    bool isnull[natts];
    HeapTuple *heapTuple = m_utTableHandler->GenerateRandomHeapTuple(values, isnull);

    m_utTableHandler->InsertHeapTupAndCheckResult(heapTuple, true);
    return heapTuple;
}

ItemPointerData UTHeap::UpdateTupAndCheckResult(ItemPointerData* ctid, HeapTuple *tuple, SnapshotData *snapshot,
                                                bool alreadyStartXact, UTTableHandler *utTableHandler)
{
    UTTableHandler *tableHandler = (utTableHandler == nullptr) ? m_utTableHandler : utTableHandler;
    Transaction *transaction = thrd->GetActiveTransaction();
    HeapUpdateContext updateContext;
    RetStatus ret = DSTORE::DSTORE_SUCC;
    updateContext.oldCtid = *ctid;
    updateContext.newTuple  = tuple;
    updateContext.needReturnOldTup = true;
    updateContext.cid = transaction->GetCurCid();
    if (g_storageInstance->GetGuc()->walLevel == static_cast<int>(WalLevel::WAL_LEVEL_LOGICAL)) {
        updateContext.replicaKeyAttrs = FormReplicaIdentityKeys(tableHandler);
    }

    if (STORAGE_FUNC_FAIL(LockUnchangedTuple(*ctid, snapshot, alreadyStartXact, tableHandler))) {
        return INVALID_ITEM_POINTER;
    }
    HeapTuple *oldTup = tableHandler->FetchHeapTuple(ctid, snapshot, alreadyStartXact);
    std::unique_ptr<StorageRelationData> heapRel(new StorageRelationData());
    heapRel->tableSmgr = tableHandler->GetHeapTabSmgr();
    heapRel->m_pdbId = g_defaultPdbId;

    if (!alreadyStartXact) {
        transaction->Start();
        if (snapshot == INVALID_SNAPSHOT) {
            thrd->GetActiveTransaction()->SetSnapshotCsn();
        } else {
            thrd->GetActiveTransaction()->SetSnapshotCsnForFlashback(snapshot->GetCsn());
        }
        {
            updateContext.snapshot = *transaction->GetSnapshotData();
            HeapUpdateHandler heapUpdate(g_storageInstance, thrd, heapRel.get());
            ret = heapUpdate.Update(&updateContext);
        }
        transaction->Commit();
    } else {
        if (snapshot == INVALID_SNAPSHOT) {
            thrd->GetActiveTransaction()->SetSnapshotCsn();
        } else {
            thrd->GetActiveTransaction()->SetSnapshotCsnForFlashback(snapshot->GetCsn());
        }
        updateContext.snapshot = *transaction->GetSnapshotData();
        HeapUpdateHandler heapUpdate(g_storageInstance, thrd, heapRel.get());
        ret = heapUpdate.Update(&updateContext);
        transaction->IncreaseCommandCounter();
    }
    if (ret == DSTORE_SUCC) {
        /* Check result */
        HeapTuple *resTuple = tableHandler->FetchHeapTuple(&updateContext.newCtid, snapshot, alreadyStartXact);
        int32 dataSize = tuple->GetDiskTupleSize() - HEAP_DISK_TUP_HEADER_SIZE;
        EXPECT_EQ(memcmp((void*)resTuple->GetDiskTuple()->GetData(), tuple->GetDiskTuple()->GetData(), dataSize), 0);
        DstorePfreeExt(resTuple);
        /* Check the result old tuple whether right */
        int32 oldTupDataSize = oldTup->GetDiskTupleSize() - HEAP_DISK_TUP_HEADER_SIZE;
        EXPECT_EQ(memcmp((void*)oldTup->GetDiskTuple()->GetData(), updateContext.retOldTuple->GetDiskTuple()->GetData(), oldTupDataSize), 0);
    } else {
        StorageAssert(updateContext.newCtid == INVALID_ITEM_POINTER);
    }
    DstorePfreeExt(oldTup);
    return updateContext.newCtid;
}

ItemPointerData UTHeap::UpdateTuple(ItemPointerData* ctid, HeapTuple *tuple, SnapshotData *snapshot,
                                    bool alreadyStartXact, UTTableHandler *utTableHandler, bool addTxnCid)
{
    UTTableHandler *tableHandler = (utTableHandler == nullptr) ? m_utTableHandler : utTableHandler;
    Transaction *transaction = thrd->GetActiveTransaction();
    HeapUpdateContext updateContext;
    RetStatus ret = DSTORE::DSTORE_SUCC;
    updateContext.oldCtid = *ctid;
    updateContext.newTuple  = tuple;
    updateContext.needReturnOldTup = true;
    updateContext.cid = transaction->GetCurCid();
    if (g_storageInstance->GetGuc()->walLevel == static_cast<int>(WalLevel::WAL_LEVEL_LOGICAL)) {
        updateContext.replicaKeyAttrs = FormReplicaIdentityKeys(tableHandler);
    }

    std::unique_ptr<StorageRelationData> heapRel(new StorageRelationData());
    heapRel->tableSmgr = tableHandler->GetHeapTabSmgr();
    heapRel->m_pdbId = g_defaultPdbId;
    if (!alreadyStartXact) {
        transaction->Start();
        if (snapshot == INVALID_SNAPSHOT) {
            thrd->GetActiveTransaction()->SetSnapshotCsn();
        } else {
            thrd->GetActiveTransaction()->SetSnapshotCsnForFlashback(snapshot->GetCsn());
        }
        {
            updateContext.snapshot = *transaction->GetSnapshotData();
            HeapUpdateHandler heapUpdate(g_storageInstance, thrd, heapRel.get());
            ret = heapUpdate.Update(&updateContext);
        }
        transaction->Commit();
    } else {
        if (snapshot == INVALID_SNAPSHOT) {
            thrd->GetActiveTransaction()->SetSnapshotCsn();
        } else {
            thrd->GetActiveTransaction()->SetSnapshotCsnForFlashback(snapshot->GetCsn());
        }
        updateContext.snapshot = *transaction->GetSnapshotData();
        HeapUpdateHandler heapUpdate(g_storageInstance, thrd, heapRel.get());
        ret = heapUpdate.Update(&updateContext);
        if (addTxnCid) {
            transaction->IncreaseCommandCounter();
        }
    }

    ASSERT(STORAGE_FUNC_SUCC(ret));
    return updateContext.newCtid;
}

void UTHeap::ForceUpdateTupleNoTrx(ItemPointerData *ctid, HeapTuple *tuple)
{
    UTTableHandler *tableHandler = m_utTableHandler;
    HeapUpdateContext updateContext;
    RetStatus ret = DSTORE::DSTORE_SUCC;
    updateContext.oldCtid = *ctid;
    updateContext.newTuple  = tuple;
    updateContext.needReturnOldTup = true;
    updateContext.retOldTuple = nullptr;
    // no need set cid to context

    if (g_storageInstance->GetGuc()->walLevel == static_cast<int>(WalLevel::WAL_LEVEL_LOGICAL)) {
        updateContext.replicaKeyAttrs = FormReplicaIdentityKeys(tableHandler);
    }

    std::unique_ptr<StorageRelationData> heapRel(new StorageRelationData());
    heapRel->tableSmgr = tableHandler->GetHeapTabSmgr();
    heapRel->m_pdbId = g_defaultPdbId;
    HeapUpdateHandler heapUpdate(g_storageInstance, thrd, heapRel.get());
    ret = heapUpdate.ForceUpdateTupleDataNoTrx(&updateContext, false);

    ASSERT(STORAGE_FUNC_SUCC(ret));
}

RetStatus UTHeap::DeleteTuple(ItemPointerData* ctid, SnapshotData *snapshot, bool alreadyStartXact,
                              UTTableHandler *utTableHandler)
{
    UTTableHandler *tableHandler = (utTableHandler == nullptr) ? m_utTableHandler : utTableHandler;
    RetStatus retVal;
    Transaction *transaction = thrd->GetActiveTransaction();
    HeapDeleteContext deleteContext;
    deleteContext.ctid = *ctid;
    deleteContext.needReturnTup = true;
    deleteContext.cid = transaction->GetCurCid();

    if (g_storageInstance->GetGuc()->walLevel == static_cast<int>(WalLevel::WAL_LEVEL_LOGICAL)) {
        deleteContext.replicaKeyAttrs = FormReplicaIdentityKeys(tableHandler);
    }
    std::unique_ptr<StorageRelationData> heapRel(new StorageRelationData());
    heapRel->tableSmgr = tableHandler->GetHeapTabSmgr();
    heapRel->m_pdbId = g_defaultPdbId;
    if (alreadyStartXact) {
        if (snapshot == INVALID_SNAPSHOT) {
            thrd->GetActiveTransaction()->SetSnapshotCsn();
        } else {
            thrd->GetActiveTransaction()->SetSnapshotCsnForFlashback(snapshot->GetCsn());
        }
        deleteContext.snapshot = *transaction->GetSnapshotData();
        HeapDeleteHandler heapDelete(g_storageInstance, thrd, heapRel.get());
        retVal = heapDelete.Delete(&deleteContext);
        transaction->IncreaseCommandCounter();

    } else {
        transaction->Start();
        if (snapshot == INVALID_SNAPSHOT) {
            thrd->GetActiveTransaction()->SetSnapshotCsn();
        } else {
            thrd->GetActiveTransaction()->SetSnapshotCsnForFlashback(snapshot->GetCsn());
        }
        {
            deleteContext.snapshot = *transaction->GetSnapshotData();
            HeapDeleteHandler heapDelete(g_storageInstance, thrd, heapRel.get());
            retVal = heapDelete.Delete(&deleteContext);
        }
        transaction->Commit();
    }
    if (retVal == DSTORE_SUCC) {
        StorageAssert(tableHandler->FetchHeapTuple(ctid, snapshot, alreadyStartXact) == nullptr);
    }
    return retVal;
}

int UTHeap::LockUnchangedTuple(ItemPointerData ctid, SnapshotData *snapshot, bool alreadyStartXact,
                               UTTableHandler *utTableHandler)
{
    UTTableHandler *tableHandler = (utTableHandler == nullptr) ? m_utTableHandler : utTableHandler;
    int result;
    Transaction *transaction = thrd->GetActiveTransaction();
    HeapLockTupleContext context;
    context.ctid = ctid;

    std::unique_ptr<StorageRelationData> heapRel(new StorageRelationData());
    heapRel->tableSmgr = tableHandler->GetHeapTabSmgr();
    heapRel->m_pdbId = g_defaultPdbId;
    if (alreadyStartXact) {
        if (snapshot == INVALID_SNAPSHOT) {
            thrd->GetActiveTransaction()->SetSnapshotCsn();
        } else {
            thrd->GetActiveTransaction()->SetSnapshotCsnForFlashback(snapshot->GetCsn());
        }
        context.snapshot = *transaction->GetSnapshotData();
        HeapLockTupHandler lockTuple(g_storageInstance, thrd, heapRel.get());
        result = lockTuple.LockUnchangedTuple(&context);
        transaction->IncreaseCommandCounter();
    } else {
        transaction->Start();
        if (snapshot == INVALID_SNAPSHOT) {
            thrd->GetActiveTransaction()->SetSnapshotCsn();
        } else {
            thrd->GetActiveTransaction()->SetSnapshotCsnForFlashback(snapshot->GetCsn());
        }
        {
            context.snapshot = *transaction->GetSnapshotData();
            HeapLockTupHandler lockTuple(g_storageInstance, thrd, heapRel.get());
            result = lockTuple.LockUnchangedTuple(&context);
        }
        transaction->Commit();
    }
    return result;
}

void UTHeap::ConstructCurSnapshotThrd(std::atomic<bool> *isSnapshotThreadNeedStop)
{
    StorageAssert(thrd == nullptr);
    StorageAssert(g_storageInstance != nullptr);

    (void)g_storageInstance->CreateThreadAndRegister(PDB_TEMPLATE1_ID, false, "SnapshotHolder");
    (void)thrd->InitTransactionRuntime(PDB_TEMPLATE1_ID, nullptr, nullptr);

    Transaction *snapshotTxn = thrd->GetActiveTransaction();
    snapshotTxn->Start();
    snapshotTxn->SetSnapshotCsn();

    ErrLog(DSTORE::DSTORE_DEBUG1,
           DSTORE::MODULE_HEAP,
            ErrMsg("ConstructCurSnapshotThrd snapshotTxn START %lu.", thrd->GetActiveTransaction()->GetSnapshotCsn()));

    while (!(*isSnapshotThreadNeedStop)) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    thrd->GetActiveTransaction()->Commit();
    StorageAssert(thrd != nullptr);
    StorageAssert(g_storageInstance != nullptr);

    g_storageInstance->UnregisterThread();
}

SnapshotData UTHeap::ConstructCurSnapshot()
{
    if (m_snapshotThread == nullptr) {
        /*
         * Warning: we must keep a transaction with the snapshot in progress to prevent undo recycle.
         * This way, the snapshot is always valid.
         */
        m_isSnapshotThreadNeedStop = false;
        m_snapshotThread = new std::thread(ConstructCurSnapshotThrd, &m_isSnapshotThreadNeedStop);
    }

    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetSnapshotCsn();
    SnapshotData snapshot = *txn->GetSnapshotData();
    txn->Commit();

    return snapshot;
}

/* Call GetNewPage and set all other page to 0 space except first data page for test */
void UTHeap::UtPrepareOneDataPage(HeapNormalSegment *heapSegment)
{
    ASSERT_NE(heapSegment->GetNewPage(), INVALID_PAGE_ID);
    SmgrType smgrType = SmgrType::HEAP_SMGR;
    StorageRelationData tmpRel;
    tmpRel.tableSmgr = DstoreNew(m_ut_memory_context)
        TableStorageMgr(heapSegment->GetPdbId(), DEFAULT_HEAP_FILLFACTOR, nullptr, SYS_RELPERSISTENCE_PERMANENT);
    tmpRel.SetTableSmgrSegment(dynamic_cast<HeapSegment *>(heapSegment)); 
    DataSegmentScanContext *segScanContext = DstoreNew(m_ut_memory_context)
            DataSegmentScanContext(g_storageInstance->GetBufferMgr(), &tmpRel, smgrType);
    segScanContext->GetFirstPageId(); /* Reserved first page with full space for test */
    PageId curScanPage = segScanContext->GetNextPageId();
    while (curScanPage != INVALID_PAGE_ID) {
        ASSERT_EQ(heapSegment->UpdateFsm(curScanPage, 0), 0);
        curScanPage = segScanContext->GetNextPageId();
    }
    tmpRel.SetTableSmgrSegment(nullptr);
    delete tmpRel.tableSmgr;
}

void UTHeap::SetTupleXidStatus(DSTORE::ItemPointerData ctid, DSTORE::TrxSlotStatus status, PdbId pdbId)
{
    pdbId = (thrd == nullptr) ? pdbId : g_defaultPdbId;
    BufferDesc *bufferDesc = g_storageInstance->GetBufferMgr()->
        Read(pdbId, ctid.GetPageId(), LW_SHARED);
    auto *page = (HeapPage*)bufferDesc->GetPage();
    Xid xid = page->GetTd(page->GetTupleTdId(ctid.GetOffset()))->GetXid();
    UndoZone *uzone = nullptr;
    g_storageInstance->GetPdb(g_defaultPdbId)->GetUndoMgr()->GetUndoZone(xid.m_zoneId, &uzone);
    TransactionSlot *slot = UZTestUtil::GetSlot(uzone->m_txnSlotManager, xid);
    slot->SetTrxSlotStatus(status);
    g_storageInstance->GetBufferMgr()->UnlockAndRelease(bufferDesc);
}

ItemPointerData UTHeap::PrepareTupleForUndo(std::string &data, SnapshotData &snapshot, ItemId &originItemId,
                                            TD &originTd, int tupleNums)
{
    ItemPointerData ctid = InsertSpecificHeapTuple(data);
    snapshot = ConstructCurSnapshot();
    BufferDesc *bufferDesc = g_storageInstance->GetBufferMgr()->
        Read(g_defaultPdbId, ctid.GetPageId(), LW_SHARED);
    auto page = (HeapPage*)bufferDesc->GetPage();
    originItemId = *page->GetItemIdPtr(ctid.GetOffset());
    originTd = *page->GetTd(page->GetTupleTdId(ctid.GetOffset()));
    g_storageInstance->GetBufferMgr()->UnlockAndRelease(bufferDesc);
    for (int i = 0; i < tupleNums - 1; ++i) {
        InsertSpecificHeapTuple(data);
    }

    return ctid;
}

void UTHeap::InitScanKeyInt24(ScanKey keyInfos, Datum arg1, uint16 strategy1, Datum arg2, uint16 strategy2)
{
    g_storageInstance->GetCacheHashMgr()->GenerateScanKey(INT2OID, Int16GetDatum(arg1), strategy1, keyInfos, 2);
    g_storageInstance->GetCacheHashMgr()->GenerateScanKey(INT4OID, Int32GetDatum(arg2), strategy2, keyInfos + 1, 3);
}

void UTHeap::CheckCrPage(ItemPointerData ctid, HeapTuple *originTup, ItemId *originItemId, TD *originTd,
                         CommitSeqNo originCsn)
{
    Transaction *txn = thrd->GetActiveTransaction();
    BufferDesc *bufferDesc = g_storageInstance->GetBufferMgr()->
        Read(g_defaultPdbId, ctid.GetPageId(), LW_SHARED);
    auto page = (HeapPage*)bufferDesc->GetPage();
    CommitSeqNo expirationCsn, pageCsn;
    CRContext crContext{g_defaultPdbId, INVALID_CSN, nullptr, bufferDesc, nullptr, false, false,
                        txn->GetSnapshot(), txn->GetCurrentXid()};
    page->ConstructCR(txn, &crContext);
    TdId tdId = (originTup != nullptr) ? page->GetTupleTdId(ctid.GetOffset()) : 0;

    ItemId *itemId;
    TD *td;

    if (originTup) {
        /* Check 1: itemId */
        itemId = page->GetItemIdPtr(ctid.GetOffset());
        ASSERT_TRUE(itemId->IsNormal());
        ASSERT_EQ(itemId->GetOffset(), originItemId->GetOffset());
        ASSERT_TRUE(itemId->GetLen() >= originItemId->GetLen());

        /* Check 2: TD */
        td = page->GetTd(tdId);
        ASSERT_EQ(td->GetXid(), originTd->GetXid());
        ASSERT_EQ(td->GetUndoRecPtr(), originTd->GetUndoRecPtr());
        ASSERT_EQ(td->GetStatus(), TDStatus::OCCUPY_TRX_END);
        ASSERT_EQ(td->GetCsn(), originCsn);
        ASSERT_EQ(td->GetCsnStatus(), IS_CUR_XID_CSN);

        /* Check 3: Tuple */
        ASSERT_TRUE(originTup->GetDiskTupleSize() <= itemId->GetLen());
        ASSERT_EQ(originTup->GetDiskTupleSize(), page->GetDiskTuple(ctid.GetOffset())->GetTupleSize());
        ASSERT_EQ(strncmp(reinterpret_cast<const char *>(originTup->GetDiskTuple()),
                          reinterpret_cast<const char *>(page->GetDiskTuple(ctid.GetOffset())),
                          originTup->GetDiskTupleSize()), 0);
    } else {
        /* Check 1: itemId */
        itemId = page->GetItemIdPtr(ctid.GetOffset());
        ASSERT_TRUE(itemId->IsUnused());
        ASSERT_EQ(itemId->GetOffset(), 0);
        ASSERT_EQ(itemId->GetLen(), 0);

        /* Check 2: TD */
        td = page->GetTd(tdId);
        ASSERT_EQ(td->GetXid(), INVALID_XID);
        ASSERT_EQ(td->GetUndoRecPtr(), INVALID_UNDO_RECORD_PTR);
        ASSERT_EQ(td->GetStatus(), TDStatus::UNOCCUPY_AND_PRUNEABLE);
        ASSERT_EQ(td->GetCsn(), INVALID_CSN);
        ASSERT_EQ(td->GetCsnStatus(), IS_INVALID);
    }
    g_storageInstance->GetBufferMgr()->UnlockAndRelease(bufferDesc);
}

void UTHeap::CheckCrTuple(ItemPointerData ctid, HeapTuple *originTup)
{
    Transaction *txn = thrd->GetActiveTransaction();
    HeapTuple *tuple = m_utTableHandler->FetchHeapTuple(&ctid, txn->GetSnapshotData(), true);
    if (originTup) {
        ASSERT_NE(tuple, nullptr);
        ASSERT_EQ(originTup->GetDiskTupleSize(), tuple->GetDiskTupleSize());
        ASSERT_EQ(strncmp(reinterpret_cast<const char *>(originTup->GetDiskTuple()),
                          reinterpret_cast<const char *>(tuple->GetDiskTuple()),
                          originTup->GetDiskTupleSize()), 0);
        DstorePfreeExt(tuple);
    } else {
        ASSERT_EQ(tuple, nullptr);
    }
}

void UTHeap::DestroySnapshotThrdIfNeed()
{
    if (m_snapshotThread == nullptr) {
        return;
    }

    ASSERT_FALSE(m_isSnapshotThreadNeedStop);
    m_isSnapshotThreadNeedStop = true;
    ErrLog(DSTORE::DSTORE_DEBUG1, DSTORE::MODULE_HEAP, ErrMsg("m_isSnapshotThreadNeedStop is set to true"));
    m_snapshotThread->join();
    delete m_snapshotThread;
    m_snapshotThread = nullptr;
}

std::string UTHeap::GenerateRandomString(int len)
{
    std::string data = "";
    for (int i = 0; i < len; i++) {
        data += ('a' + rand() % 26);
    }
    return data;
}

HeapTuple *UTHeap::GenerateTupleWithLob(std::string &value, int len)
{
    Size lobValueSize = VARHDRSZ + len;
    varattrib_4b *lobValue = (varattrib_4b *) DstorePalloc0(lobValueSize);
    DstoreSetVarSize4B(lobValue, lobValueSize);
    EXPECT_EQ(memcpy_s(VarData4B(lobValue), len, (void*)value.c_str(), len), 0);
    Datum values[TYPE_CACHE_NUM];
    bool isNulls[TYPE_CACHE_NUM];
    for (int i = 0; i < TYPE_CACHE_NUM; i++) {
        values[i] = 0;
        isNulls[i] = true;
    }
    values[CLOB_IDX] = PointerGetDatum(lobValue);
    isNulls[CLOB_IDX] = false;
    HeapTuple *heapTuple = HeapTuple::FormTuple(m_utTableHandler->GetHeapTabSmgr()->GetTupleDesc(), values, isNulls);
    DstorePfreeExt(lobValue);
    return heapTuple;
}

varlena *UTHeap::FetchLobValue(varlena *value)
{
    thrd->GetActiveTransaction()->Start();
    thrd->GetActiveTransaction()->SetSnapshotCsn();
    StorageRelationData relation;
    relation.tableSmgr = m_utTableHandler->GetHeapTabSmgr();
    relation.lobTableSmgr = m_utTableHandler->GetLobTabSmgr();
    relation.m_pdbId = g_defaultPdbId;
    VarattLobLocator *lobLocator = STATIC_CAST_PTR_TYPE(VarData1BE(value), VarattLobLocator *);
    ItemPointerData ctid(lobLocator->ctid);
    varlena *result = HeapInterface::FetchLobValue(&relation, ctid, thrd->GetActiveTransaction()->GetSnapshotData());
    thrd->GetActiveTransaction()->Commit();
    return result;
}

RetStatus UTHeap::InsertTupleWithLob(HeapTuple *tuple, ItemPointerData &ctid)
{
    thrd->GetActiveTransaction()->Start();
    thrd->GetActiveTransaction()->SetSnapshotCsn();
    StorageRelationData relation;
    relation.tableSmgr = m_utTableHandler->GetHeapTabSmgr();
    relation.lobTableSmgr = m_utTableHandler->GetLobTabSmgr();
    relation.m_pdbId = g_defaultPdbId;
    RetStatus status = HeapInterface::Insert(&relation, tuple, ctid, thrd->GetActiveTransaction()->GetCurCid());
    thrd->GetActiveTransaction()->Commit();
    return status;
}

RetStatus UTHeap::DeleteTupleWithLob(HeapDeleteContext &deleteContext, ItemPointerData &ctid, bool needReturnTup)
{
    deleteContext.ctid = ctid;
    deleteContext.needReturnTup = needReturnTup;
    deleteContext.returnTup = nullptr;
    thrd->GetActiveTransaction()->Start();
    thrd->GetActiveTransaction()->SetSnapshotCsn();
    deleteContext.snapshot = *thrd->GetActiveTransaction()->GetSnapshotData();
    deleteContext.cid = thrd->GetActiveTransaction()->GetCurCid();
    StorageRelationData relation;
    relation.tableSmgr = m_utTableHandler->GetHeapTabSmgr();
    relation.lobTableSmgr = m_utTableHandler->GetLobTabSmgr();
    relation.m_pdbId = g_defaultPdbId;
    RetStatus status = HeapInterface::Delete(&relation, &deleteContext);
    thrd->GetActiveTransaction()->Commit();
    return status;
}

RetStatus UTHeap::UpdateTupleWithLob(HeapUpdateContext &updateContext, ItemPointerData &oldCtid, HeapTuple *newTuple,
                                     bool needReturnTup)
{
    updateContext.oldCtid = oldCtid;
    updateContext.hasIndex = true;
    updateContext.needUpdateLob = true;
    updateContext.needReturnOldTup = needReturnTup;
    updateContext.newTuple = newTuple;
    updateContext.retOldTuple = nullptr;
    thrd->GetActiveTransaction()->Start();
    thrd->GetActiveTransaction()->SetSnapshotCsn();
    StorageRelationData relation;
    updateContext.snapshot = *thrd->GetActiveTransaction()->GetSnapshotData();
    updateContext.cid = thrd->GetActiveTransaction()->GetCurCid();
    relation.tableSmgr = m_utTableHandler->GetHeapTabSmgr();
    relation.lobTableSmgr = m_utTableHandler->GetLobTabSmgr();
    relation.m_pdbId = g_defaultPdbId;
    RetStatus status = HeapInterface::Update(&relation, &updateContext);
    thrd->GetActiveTransaction()->Commit();
    return status;
}

TEST_F(UTHeap, HeapPageTest_level0)
{
    HeapPage page{};
    page.Init(0, PageType::HEAP_PAGE_TYPE, INVALID_PAGE_ID);
    page.SetRecentDeadTupleMinCsn(INVALID_CSN);
    page.SetPotentialDelSize(0);
    page.SetDataHeaderSize(HEAP_PAGE_HEADER_SIZE);
    page.m_header.m_lower = page.DataHeaderSize();
    page.AllocateTdSpace();
    int natts = m_utTableHandler->GetHeapTupDesc()->natts;
    Datum values[natts];
    bool isnull[natts];
    for (int i = 0; i < 10; i++) {
        HeapTuple *heapTuple = m_utTableHandler->GenerateRandomHeapTuple(values, isnull);
        HeapDiskTuple *diskTuple = heapTuple->GetDiskTuple();
        int diskTupleLen = heapTuple->GetDiskTupleSize();
        int dataLen = diskTupleLen - HEAP_DISK_TUP_HEADER_SIZE;
        diskTuple->SetTdId(0);

        OffsetNumber offset = page.AddTuple(diskTuple, diskTupleLen);
        ASSERT_EQ(offset, i + 1);

        HeapTuple memoryTuple;
        page.GetTuple(&memoryTuple, offset);
        ASSERT_EQ(diskTupleLen, page.GetItemIdPtr(offset)->GetLen());
        int ret = memcmp(heapTuple->GetDiskTuple()->GetData(), memoryTuple.GetDiskTuple()->GetData(), dataLen);
        ASSERT_EQ(ret, 0);
        DstorePfreeExt(heapTuple);
    }
}

TEST_F(UTHeap, HeapPageTDTest_TIER1_level0)
{
    HeapNormalSegment *segment = dynamic_cast<HeapNormalSegment *>(m_utTableHandler->GetHeapTabSmgr()->GetSegment());
    UtPrepareOneDataPage(segment);
    Xid xid;
    PageId pageId = INVALID_PAGE_ID;
    BufferDesc *bufferDesc;
    HeapPage *page;
    Transaction *transaction = thrd->GetActiveTransaction();
    int tdNum = DEFAULT_TD_COUNT;
    SmgrType smgrType = SmgrType::HEAP_SMGR;
    StorageRelationData tmpRel;
    tmpRel.tableSmgr = DstoreNew(m_ut_memory_context)
        TableStorageMgr(segment->GetPdbId(), DEFAULT_HEAP_FILLFACTOR, nullptr, SYS_RELPERSISTENCE_PERMANENT);
    tmpRel.SetTableSmgrSegment(dynamic_cast<HeapSegment *>(segment));
    DataSegmentScanContext *segScanContext = DstoreNew(m_ut_memory_context) DataSegmentScanContext
        (g_storageInstance->GetBufferMgr(), &tmpRel, smgrType);
    segScanContext->Reset();
    for (int i = 0; i < tdNum; i++) {
        transaction->Start();
        HeapTuple *heapTuple = InsertRandomHeapTuple();
        if (pageId == INVALID_PAGE_ID) {
            pageId = segScanContext->GetFirstPageId();
        }
        bufferDesc = g_storageInstance->GetBufferMgr()->
            Read(g_defaultPdbId, pageId, LW_SHARED);
        page = (HeapPage *)bufferDesc->GetPage();
        xid = transaction->GetCurrentXid();
        ASSERT_EQ(page->GetTd(i)->GetXid(), xid);
        g_storageInstance->GetBufferMgr()->UnlockContent(bufferDesc);
        g_storageInstance->GetBufferMgr()->Release(bufferDesc);
        if (i == tdNum - 1) {
            transaction->Abort();
        } else {
            transaction->Commit();
        }
        DstorePfreeExt(heapTuple);
        heapTuple  = nullptr;
    }
    tmpRel.SetTableSmgrSegment(nullptr);
    delete tmpRel.tableSmgr;
    delete segScanContext;

    /* Reuse td slot 3 which is aborted */
    transaction->Start();
    HeapTuple *ht = InsertRandomHeapTuple();
    xid = transaction->GetCurrentXid();
    bufferDesc = g_storageInstance->GetBufferMgr()->
        Read(g_defaultPdbId, pageId, LW_SHARED);
    page = (HeapPage *)bufferDesc->GetPage();
    ASSERT_EQ(page->GetTd(3)->GetXid(), xid);
    g_storageInstance->GetBufferMgr()->UnlockContent(bufferDesc);
    g_storageInstance->GetBufferMgr()->Release(bufferDesc);
    transaction->Commit();
    DstorePfreeExt(ht);

    /* Reuse td slot 0 according global_min_csn */
    g_storageInstance->GetCsnMgr()->SetLocalCsnMin(2);
    transaction->Start();
    ht = InsertRandomHeapTuple();
    xid = transaction->GetCurrentXid();
    bufferDesc = g_storageInstance->GetBufferMgr()->
        Read(g_defaultPdbId, pageId, LW_SHARED);
    page = (HeapPage *)bufferDesc->GetPage();
    ASSERT_EQ(page->GetTd(0)->GetXid(), xid);
    g_storageInstance->GetBufferMgr()->UnlockContent(bufferDesc);
    g_storageInstance->GetBufferMgr()->Release(bufferDesc);
    transaction->Commit();
    DstorePfreeExt(ht);

    /* Freeze all td slots which are committed and reuse td slot 1 */
    transaction->Start();
    ht = InsertRandomHeapTuple();
    xid = transaction->GetCurrentXid();
    bufferDesc = g_storageInstance->GetBufferMgr()->
        Read(g_defaultPdbId, pageId, LW_SHARED);
    page = (HeapPage *)bufferDesc->GetPage();
    ASSERT_EQ(page->GetTd(1)->GetXid(), xid);
    g_storageInstance->GetBufferMgr()->UnlockContent(bufferDesc);
    g_storageInstance->GetBufferMgr()->Release(bufferDesc);
    transaction->Commit();
    DstorePfreeExt(ht);

    /* test alloctd round-robin selection algorighm */
    transaction->Start();
    ht = InsertRandomHeapTuple();
    transaction->Commit();
    ASSERT_EQ(ht->GetTdId(), 2);
    DstorePfreeExt(ht);
    transaction->Start();
    ht = InsertRandomHeapTuple();
    transaction->Commit();
    ASSERT_EQ(ht->GetTdId(), 3);
    DstorePfreeExt(ht);
    transaction->Start();
    ht = InsertRandomHeapTuple();
    transaction->Commit();
    ASSERT_EQ(ht->GetTdId(), 0);
    DstorePfreeExt(ht);
    /* If all xids in td are in-progress, extend td slot and use td slot 4 */
    bufferDesc = g_storageInstance->GetBufferMgr()->
        Read(g_defaultPdbId, pageId, LW_SHARED);
    page = (HeapPage *)bufferDesc->GetPage();
    /* Forcibly set all tds' xid status to in-progress to mock concurrent transaction */
    for (int i = 0; i < tdNum; i++) {
        xid = page->GetTd(i)->GetXid();
        UndoZone *uzone = nullptr;
        g_storageInstance->GetPdb(g_defaultPdbId)->GetUndoMgr()->GetUndoZone(xid.m_zoneId, &uzone);
        TransactionSlot *slot = UZTestUtil::GetSlot(uzone->m_txnSlotManager, xid);
        g_storageInstance->GetPdb(g_defaultPdbId)->GetUndoMgr()->m_txnInfoCache->
            m_cachedEntry[xid.m_zoneId][xid.m_logicSlotId % CACHED_SLOT_NUM_PER_ZONE].txnInfo.status = TXN_STATUS_IN_PROGRESS;
        slot->SetTrxSlotStatus(TXN_STATUS_IN_PROGRESS);
        page->GetTd(i)->SetStatus(TDStatus::OCCUPY_TRX_IN_PROGRESS);
        page->GetTd(i)->SetCsnStatus(IS_INVALID);
    }
    g_storageInstance->GetBufferMgr()->UnlockContent(bufferDesc);
    g_storageInstance->GetBufferMgr()->Release(bufferDesc);
    transaction->Start();
    ht = InsertRandomHeapTuple();
    xid = transaction->GetCurrentXid();
    bufferDesc = g_storageInstance->GetBufferMgr()->
        Read(g_defaultPdbId, pageId, LW_SHARED);
    page = (HeapPage *)bufferDesc->GetPage();
    ASSERT_EQ(page->GetTd(4)->GetXid(), xid);
    g_storageInstance->GetBufferMgr()->UnlockContent(bufferDesc);
    g_storageInstance->GetBufferMgr()->Release(bufferDesc);
    transaction->Commit();
    DstorePfreeExt(ht);
}

TEST_F(UTHeap, TxnAbortTest_level0)
{
    Transaction *txn = thrd->GetActiveTransaction();
    constexpr int TUPLE_DATA_LEN = 1024;
    std::string stra(TUPLE_DATA_LEN, 'a');
    std::string strb(TUPLE_DATA_LEN, 'b');
    std::vector<ItemPointerData> ctids;
    std::vector<HeapTuple*> originTups;

    /* Step 1: prepare 4 tuples for inplace update, same page update, another page update and delete */
    for (int i = 0; i < 4; ++i) {
        ItemPointerData ctid = InsertSpecificHeapTuple(stra);
        HeapTuple *tuple = m_utTableHandler->FetchHeapTuple(&ctid);
        ctids.push_back(ctid);
        originTups.push_back(tuple);
    }

    /* Step 2: DML */
    txn->Start();
    txn->SetSnapshotCsn();

    /* ----insert---- */
    InsertSpecificHeapTuple(strb, txn->GetSnapshotData(), true);
    /* ----delete---- */
    DeleteTuple(&ctids[0], txn->GetSnapshotData(), true);
    /* ----inplace update---- */
    HeapTuple *heapTuple = m_utTableHandler->GenerateSpecificHeapTuple(strb);
    UpdateTupAndCheckResult(&ctids[1], heapTuple, txn->GetSnapshotData(), true);
    DstorePfreeExt(heapTuple);
    /* ----same page update---- */
    strb.append(stra);
    heapTuple = m_utTableHandler->GenerateSpecificHeapTuple(strb);
    UpdateTupAndCheckResult(&ctids[2], heapTuple, txn->GetSnapshotData(), true);
    DstorePfreeExt(heapTuple);
    /* ----another page update---- */
    std::string strc(TUPLE_DATA_LEN * 6, 'c');
    heapTuple = m_utTableHandler->GenerateSpecificHeapTuple(strc);
    UpdateTupAndCheckResult(&ctids[3], heapTuple, txn->GetSnapshotData(), true);
    DstorePfreeExt(heapTuple);

    txn->Abort();

    /* Step 3: check disk tuples */
    for (int i = 0; i < ctids.size(); ++i) {
        HeapTuple *tuple = m_utTableHandler->FetchHeapTuple(&ctids[i]);
        ASSERT_NE(tuple, nullptr);
        ASSERT_EQ(originTups[i]->GetDiskTupleSize(), tuple->GetDiskTupleSize());
        ASSERT_EQ(strncmp(reinterpret_cast<const char *>(originTups[i]->GetDiskTuple()->GetData()),
                          reinterpret_cast<const char *>(tuple->GetDiskTuple()->GetData()),
                          originTups[i]->GetDiskTupleSize() - HeapDiskTuple::GetHeaderSize()), 0);
        DstorePfreeExt(tuple);
        DstorePfreeExt(originTups[i]);
    }
}

