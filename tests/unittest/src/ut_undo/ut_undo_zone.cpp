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
#include "ut_undo/ut_undo_zone.h"
#include "page/dstore_undo_segment_meta_page.h"
#include "common/dstore_common_utils.h"

using namespace DSTORE;

/* undo zone */
TEST_F(UndoZoneTest, TxnSlotAllocTest_level0)
{
    const uint32 zoneId = 2;
    /* meta blk num is 1. */
    const uint32 startBlkNum = 2;
    const uint32 UndoRecSize = 100;
    const uint32 Select = 5;
    uint64 fakeCsn = COMMITSEQNO_FIRST_NORMAL;
    UndoZone uzone(m_segment, m_bufferMgr, zoneId, m_pdbId);
    uzone.Init(g_dstoreCurrentMemoryContext);
    Xid xid(0);
    std::vector<Xid> xidVec;
    ItemPointerData blkFirstSlotPtr;
    ItemPointerData blkLastSlotPtr;
    ItemPointerData prevSlotPtr(0);
    ItemPointerData currentSlotPtr(0);
    TransactionSlot *trxSlot;
    FileId curFileId = m_segment->GetSegmentMetaPageId().m_fileId; /* TODO: Remove this variable */

    /* Allocate all the txn slots and update them. */
    for (uint32 pageId = 0; pageId < TRX_PAGES_PER_ZONE; pageId++) {
        currentSlotPtr.m_placeHolder = 0;
        prevSlotPtr.m_placeHolder = 0;
        for (uint32 i = 0; i < TRX_PAGE_SLOTS_NUM; i++) {
            xid = uzone.AllocSlot();
            ASSERT_NE(xid, INVALID_XID);
            xidVec.push_back(xid);
            if (i == 0) {
                blkFirstSlotPtr = UZTestUtil::GetTrxSlotPtr(xid, curFileId, startBlkNum);
            }
            ASSERT_EQ(xid.m_zoneId, zoneId);
            ASSERT_EQ(xid.m_logicSlotId, pageId * TRX_PAGE_SLOTS_NUM + i);
            currentSlotPtr = UZTestUtil::GetTrxSlotPtr(xid, curFileId, startBlkNum);
            if (currentSlotPtr.m_placeHolder != 0 && prevSlotPtr.m_placeHolder != 0) {
                ASSERT_EQ(currentSlotPtr.GetOffset() - prevSlotPtr.GetOffset(), TRX_SLOT_SIZE);
            }
            prevSlotPtr = currentSlotPtr;
            trxSlot = UZTestUtil::GetSlot(uzone.m_txnSlotManager, xid);
            trxSlot->SetCurTailUndoPtr(UndoRecPtr((i + 1) * UndoRecSize));
            trxSlot->SetSpaceTailUndoPtr(UndoRecPtr((i + 1) * UndoRecSize));
            if (i % Select == 0) {
                trxSlot->UpdateRollbackProgress();
            } else {
                trxSlot->SetCsn(fakeCsn++);
            }
        }
        blkLastSlotPtr = UZTestUtil::GetTrxSlotPtr(xid, curFileId, startBlkNum);
        ASSERT_EQ(blkLastSlotPtr.GetBlockNum(), startBlkNum + pageId);
        ASSERT_EQ(blkLastSlotPtr.GetFileId(), blkFirstSlotPtr.GetFileId());
        ASSERT_EQ(blkLastSlotPtr.GetBlockNum(), blkFirstSlotPtr.GetBlockNum());
        ASSERT_EQ(blkLastSlotPtr.GetOffset() - blkFirstSlotPtr.GetOffset(),
                  (TRX_PAGE_SLOTS_NUM - 1) * TRX_SLOT_SIZE);
    }
    ASSERT_EQ(xid.m_logicSlotId, TRX_PAGES_PER_ZONE * TRX_PAGE_SLOTS_NUM - 1);
    /* no more txn slot usable. */
    ASSERT_EQ(uzone.AllocSlot(), INVALID_XID);

    fakeCsn = COMMITSEQNO_FIRST_NORMAL;
    /* Check the updated txn slots. */
    for (uint32 pageId = 0; pageId < TRX_PAGES_PER_ZONE; pageId++) {
        for (uint32 i = 0; i < TRX_PAGE_SLOTS_NUM; i++) {
            xid = xidVec[pageId * TRX_PAGE_SLOTS_NUM + i];
            trxSlot = UZTestUtil::GetSlot(uzone.m_txnSlotManager, xid);
            ASSERT_EQ(trxSlot->GetCurTailUndoPtr().m_placeHolder, (i + 1) * UndoRecSize);
            ASSERT_EQ(trxSlot->GetSpaceTailUndoPtr().m_placeHolder, (i + 1) * UndoRecSize);
            if (i % Select == 0) {
                ASSERT_EQ(trxSlot->NeedRollback(), false);
                ASSERT_EQ(trxSlot->GetCsn(), 0);
            } else {
                ASSERT_EQ(trxSlot->NeedRollback(), true);
                ASSERT_EQ(trxSlot->GetCsn(), fakeCsn++);
            }
        }
    }
}

TEST_F(UndoZoneTest, TxnSlotRecycleTest_level0)
{
    uint64 fakeCsn = COMMITSEQNO_FIRST_NORMAL;
    uint64 MinSnapShot = COMMITSEQNO_FIRST_NORMAL;
    uint64 TrxSlotReuseCnt = 0;

    ZoneId zoneId = INVALID_ZONE_ID;
    g_storageInstance->GetPdb(g_defaultPdbId)->GetUndoMgr()->AllocateZoneId(zoneId);
    UndoZone *uzone = nullptr;
    g_storageInstance->GetPdb(g_defaultPdbId)->GetUndoMgr()->GetUndoZone(zoneId, &uzone, true);
    Xid xid(0);
    std::vector<Xid> xidVec;

    TransactionSlot *trxSlot = nullptr;

    /* Allocate all the txn slots and update them. */
    for (uint32 pageId = 0; pageId < TRX_PAGES_PER_ZONE; pageId++) {
        for (uint32 i = 0; i < TRX_PAGE_SLOTS_NUM; i++) {
            xid = uzone->AllocSlot();
            xidVec.push_back(xid);
            ASSERT_FALSE(uzone->m_txnSlotManager->IsXidRecycled(g_defaultPdbId, xid));
            trxSlot = UZTestUtil::GetSlot(uzone->m_txnSlotManager, xid);
            trxSlot->SetCsn(fakeCsn++);
            trxSlot->SetTrxSlotStatus(DSTORE::TXN_STATUS_COMMITTED);
        }
    }
    ASSERT_EQ(xid.m_logicSlotId, TRX_PAGES_PER_ZONE * TRX_PAGE_SLOTS_NUM - 1);
    /* no more txn slot usable. */
    ASSERT_EQ(uzone->AllocSlot(), INVALID_XID);

    for (uint32 seq = 1; seq < TRX_PAGES_PER_ZONE; seq++) {
        /* Recycle txn slots. */
        TrxSlotReuseCnt = TRX_PAGE_SLOTS_NUM * seq;
        MinSnapShot += TrxSlotReuseCnt;
        uzone->Recycle(MinSnapShot);
        /* Confirm txn slots are recycled. */
        for (uint32 i = 0; i < TrxSlotReuseCnt; i++) {
            xid = xidVec[i];
            ASSERT_TRUE(uzone->m_txnSlotManager->IsXidRecycled(g_defaultPdbId, xid));
            ASSERT_EQ(UZTestUtil::GetSlot(uzone->m_txnSlotManager, xid)->GetTxnSlotStatus(), TXN_STATUS_FROZEN);
        }
        /* Erase recycled Xid. */
        for (uint32 i = 0; i < TrxSlotReuseCnt; i++) {
            xidVec.erase(xidVec.begin());
        }
        ASSERT_NE(UZTestUtil::GetSlot(uzone->m_txnSlotManager, xidVec[TrxSlotReuseCnt])->GetTxnSlotStatus(), TXN_STATUS_FROZEN);
        for (uint32 i = 0; i < TrxSlotReuseCnt; i++) {
            xid = uzone->AllocSlot();
            xidVec.push_back(xid);
            trxSlot = UZTestUtil::GetSlot(uzone->m_txnSlotManager, xid);
            trxSlot->SetCsn(fakeCsn++);
            trxSlot->SetTrxSlotStatus(DSTORE::TXN_STATUS_COMMITTED);
            ASSERT_NE(xid, INVALID_XID);
        }
        ASSERT_EQ(uzone->AllocSlot(), INVALID_XID);
    }
}

TEST_F(UndoZoneTest, UndoRecAccessTest_level0)
{
    ZoneId zoneId = INVALID_ZONE_ID;
    UndoMgr *undoMgr = g_storageInstance->GetPdb(g_defaultPdbId)->GetUndoMgr();
    undoMgr->AllocateZoneId(zoneId);
    UndoZone *zone = nullptr;
    undoMgr->GetUndoZone(zoneId, &zone, true);
    TD td;
    td.SetUndoRecPtr({{4, 5}, 6});
    td.SetXid(Xid(1));
    UndoRecord *rec1 = UZTestUtil::CreateUndoRecord({{1, 2}, 3}, &td, (UndoType)1, 1, 10);

    td.SetXid(Xid(2));
    UndoRecord *rec2 = UZTestUtil::CreateUndoRecord({{1, 2}, 3}, &td, (UndoType)3, 2, 8000);

    AtomicWalWriterContext *walContext = thrd->m_walWriterContext;
    walContext->BeginAtomicWal(thrd->GetActiveTransaction()->GetCurrentXid());
    UndoRecPtr ptr1 = zone->InsertUndoRecord(rec1);
    walContext->EndAtomicWal();
    walContext->BeginAtomicWal(thrd->GetActiveTransaction()->GetCurrentXid());
    UndoRecPtr ptr2 = zone->InsertUndoRecord(rec2);
    walContext->EndAtomicWal();
    UndoRecord rec3, rec4;
    zone->FetchUndoRecord(g_defaultPdbId, &rec3, ptr1, Xid(zoneId, 0), undoMgr->GetBufferMgr());
    zone->FetchUndoRecord(g_defaultPdbId, &rec4, ptr2, Xid(zoneId, 0), undoMgr->GetBufferMgr());

    ASSERT_EQ(rec1->GetCtid(), rec3.GetCtid());
    ASSERT_EQ(rec1->GetTdPreUndoPtr(), rec3.GetTdPreUndoPtr());
    ASSERT_EQ(rec1->GetTdPreXid(), rec3.GetTdPreXid());
    ASSERT_EQ(rec1->GetUndoType(), rec3.GetUndoType());
    ASSERT_EQ(rec1->GetTdId(), rec3.GetTdId());
    ASSERT_EQ(rec1->m_dataInfo.len, rec3.m_dataInfo.len);
    int result = memcmp(rec1->m_dataInfo.data, rec3.m_dataInfo.data, rec3.m_dataInfo.len);
    ASSERT_EQ(result, 0);
    ASSERT_EQ(rec2->m_dataInfo.len, rec4.m_dataInfo.len);
    result = memcmp(rec2->m_dataInfo.data, rec4.m_dataInfo.data, rec4.m_dataInfo.len);
    ASSERT_EQ(result, 0);

    UZTestUtil::DestroyUndoRecord(rec1);
    UZTestUtil::DestroyUndoRecord(rec2);
}

TEST_F(UndoZoneTest, UndoRecPageRecycleTest_level0) {
    const uint32 zoneId = 3;
    UndoZone zone(m_segment, m_bufferMgr, zoneId, m_pdbId);
    zone.Init(g_dstoreCurrentMemoryContext);
    UndoZoneTrxManager *zoneTxnMgr = zone.m_txnSlotManager;
    ASSERT_EQ(zoneTxnMgr->GetRecycleLogicSlotId(), zoneTxnMgr->GetNextFreeLogicSlotId());
    PageId firstSlotPageId = zoneTxnMgr->GetStartPageId();
    PageId firstUndoPageId = zone.m_undoRecyclePageId;
    int undoPageCnt = UZTestUtil::GetPageCntInBlkList(firstUndoPageId, m_bufferMgr);
    ASSERT_EQ(undoPageCnt, UNDO_ZONE_EXTENT_SIZE - 1);
    uint64 fakeCsn = COMMITSEQNO_FIRST_NORMAL;
    const int undoRecordCnt = 10;
    std::vector<UndoRecPtr> undoRecPtrs;
    std::vector<UndoRecord*> undoRecords;

    /* Fill 2 transaction slot page and 10 undo record pages. */
    AtomicWalWriterContext *walContext = thrd->m_walWriterContext;
    for (int i = 0; i < undoRecordCnt; ++i) {
        UndoRecord *record = UZTestUtil::CreateUndoRecord(8000);
        undoRecords.emplace_back(record);
        zone.ExtendSpaceIfNeeded(record->GetRecordSize());
        walContext->BeginAtomicWal(thrd->GetActiveTransaction()->GetCurrentXid());
        UndoRecPtr undoRecPtr = zone.InsertUndoRecord(record);
        walContext->EndAtomicWal();
        undoRecPtrs.emplace_back(undoRecPtr);
        if (i < 2) {
            UZTestUtil::CreateFullPageTrxSlot(zone, fakeCsn, undoRecPtr);
            fakeCsn++;
        }
    }
    undoPageCnt = UZTestUtil::GetPageCntInBlkList(firstUndoPageId, m_bufferMgr);
    ASSERT_EQ(undoPageCnt, (UNDO_ZONE_EXTENT_SIZE - 1) * 2);

    /* Fill the 3rd transaction slot page and switch m_txnSlotCurPageId to the 4th transaction slot page. */
    UZTestUtil::CreateFullPageTrxSlot(zone, fakeCsn, undoRecPtrs[undoRecordCnt - 1]);
    zoneTxnMgr->AllocSlot();
    ASSERT_EQ(zoneTxnMgr->GetTxnSlotPageId(zoneTxnMgr->GetNextFreeLogicSlotId()).m_blockId - firstSlotPageId.m_blockId, 3);

    /* The 1st transaction slot page and 1 undo record page are recycled. */
    zone.Recycle(COMMITSEQNO_FIRST_NORMAL + 1);
    ASSERT_EQ(zoneTxnMgr->GetTxnSlotPageId(zoneTxnMgr->GetRecycleLogicSlotId()).m_blockId - firstSlotPageId.m_blockId, 1);
    ASSERT_EQ(zone.m_undoRecyclePageId, undoRecPtrs[1].GetPageId());

    /* The 2nd transaction slot page and 1 undo record page are recycled. */
    zone.Recycle(COMMITSEQNO_FIRST_NORMAL + 2);
    ASSERT_EQ(zoneTxnMgr->GetTxnSlotPageId(zoneTxnMgr->GetRecycleLogicSlotId()).m_blockId - firstSlotPageId.m_blockId, 2);
    ASSERT_EQ(zone.m_undoRecyclePageId, undoRecPtrs[2].GetPageId());

    /* The 3rd transaction slot page and 9 undo record pages(except the last one, m_nextAppendUndoPtr) are recycled. */
    zone.Recycle(COMMITSEQNO_FIRST_NORMAL + 3);
    ASSERT_EQ(zoneTxnMgr->GetTxnSlotPageId(zoneTxnMgr->GetRecycleLogicSlotId()).m_blockId - firstSlotPageId.m_blockId, 3);
    ASSERT_EQ(zone.m_undoRecyclePageId.m_blockId, undoRecPtrs[undoRecordCnt - 1].GetPageId().m_blockId + 1);

    for (auto record : undoRecords) {
        UZTestUtil::DestroyUndoRecord(record);
    }
}

TEST_F(UndoZoneTest, TxnSlotRollbackTest_level0)
{
    const uint32 zoneId = 4;
    UndoZone *uzone = DstoreNew(g_dstoreCurrentMemoryContext) UndoZone(m_segment, m_bufferMgr, zoneId, m_pdbId);
    uzone->Init(g_dstoreCurrentMemoryContext);

    Xid rollbackXid;
    ASSERT_EQ(uzone->m_txnSlotManager->IsUndoZoneNeedRollback(rollbackXid), false);

    RollbackTrxTaskMgr *taskMgr =
            g_storageInstance->GetPdb(g_defaultPdbId)->GetTransactionMgr()->m_rollbackTrxTaskMgr;
    ASSERT_EQ(taskMgr->m_dispatchThread, nullptr);

    Xid xid;
    for (uint32 i = 0; i < TRX_PAGE_SLOTS_NUM / 2; ++i) {
        xid = uzone->AllocSlot();
    }

    ASSERT_EQ(uzone->m_txnSlotManager->IsUndoZoneNeedRollback(rollbackXid), true);
    ASSERT_EQ(rollbackXid, xid);

    ASSERT_EQ(taskMgr->AddRollbackTrxTask(xid, uzone), DSTORE_SUCC);
    ASSERT_EQ(uzone->m_txnSlotManager->IsUndoZoneNeedRollback(rollbackXid), true);
    ASSERT_EQ(taskMgr->IsAllTaskFinished(), false);
    uzone->SetAsyncRollbackState(true);

    taskMgr->StartDispatch();
    while (!taskMgr->IsAllTaskFinished()) {
        sleep(1);
    }
    taskMgr->StopDispatch();
    while (taskMgr->IsDispatching()) {
        sleep(1);
    }

    ASSERT_EQ(uzone->m_txnSlotManager->IsUndoZoneNeedRollback(rollbackXid), false);
    ASSERT_EQ(uzone->IsAsyncRollbacking(), false);
}

TEST_F(UndoZoneTest, RestoreTest_level0)
{
    /*
     * zone[0]: fullly free
     * zone[1]: no reuse, m_recycleLogicSlotId and m_nextFreeLogicSlotId are not on the same page
     * zone[2]: no reuse, m_recycleLogicSlotId and m_nextFreeLogicSlotId are on the same page, next is first slot
     * zone[3]: no reuse, m_recycleLogicSlotId and m_nextFreeLogicSlotId are on the same page, next is last slot
     * zone[4]: reuse, m_recycleLogicSlotId and m_nextFreeLogicSlotId are on the same page, next is first slot
     * zone[5]: reuse, m_recycleLogicSlotId and m_nextFreeLogicSlotId are on the same page, next is last slot
     */
    std::vector<UndoZone *> originZones;
    std::vector<UndoZone *> restoreZones;
    std::vector<CommitSeqNo> recycleCsns;
    std::vector<uint64> startSlotIds;
    constexpr int ZONE_NUM = 6;
    for (int i = 0; i < ZONE_NUM; ++i) {
        Segment *segment = static_cast<Segment *>(
            SegmentInterface::AllocUndoSegment(m_pdbId, m_tablespaceId, SegmentType::UNDO_SEGMENT_TYPE, m_bufferMgr));
        UndoZone *zone = DstoreNew(g_dstoreCurrentMemoryContext) UndoZone(segment, m_bufferMgr, i, m_pdbId);
        zone->Init(g_dstoreCurrentMemoryContext);
        originZones.push_back(zone);
        zone = DstoreNew(g_dstoreCurrentMemoryContext) UndoZone(segment, m_bufferMgr, i, m_pdbId);
        restoreZones.push_back(zone);
    }

    /* zone[0] */
    recycleCsns.push_back(0);
    startSlotIds.push_back(0);
    g_storageInstance->GetCsnMgr()->SetLocalCsnMin(recycleCsns[0]);
    restoreZones[0]->Init(g_dstoreCurrentMemoryContext);

    /* zone[1] */
    uint64 totalSlotNum = (TRX_PAGES_PER_ZONE / 2) * TRX_PAGE_SLOTS_NUM - TRX_PAGE_SLOTS_NUM / 2;
    UZTestUtil::PrepareOriginZone(totalSlotNum, TRX_PAGE_SLOTS_NUM, originZones[1], startSlotIds, recycleCsns);
    UZTestUtil::PrepareRestoreZone(recycleCsns[1], restoreZones[1]);

    /* zone[2] */
    totalSlotNum = (TRX_PAGES_PER_ZONE / 2 - 1) * TRX_PAGE_SLOTS_NUM;
    UZTestUtil::PrepareOriginZone(totalSlotNum, 0, originZones[2], startSlotIds, recycleCsns);
    UZTestUtil::PrepareRestoreZone(recycleCsns[2], restoreZones[2]);

    /* zone[3] */
    totalSlotNum = (TRX_PAGES_PER_ZONE / 2) * TRX_PAGE_SLOTS_NUM - 1;
    UZTestUtil::PrepareOriginZone(totalSlotNum, TRX_PAGE_SLOTS_NUM / 2, originZones[3], startSlotIds, recycleCsns);
    UZTestUtil::PrepareRestoreZone(recycleCsns[3], restoreZones[3]);

    /* zone[4] */
    totalSlotNum = (TRX_PAGES_PER_ZONE * 3 / 2 - 1) * TRX_PAGE_SLOTS_NUM;
    UZTestUtil::PrepareOriginZone(totalSlotNum, 0, originZones[4], startSlotIds, recycleCsns);
    UZTestUtil::PrepareRestoreZone(recycleCsns[4], restoreZones[4]);

    /* zone[5] */
    totalSlotNum = (TRX_PAGES_PER_ZONE * 3 / 2) * TRX_PAGE_SLOTS_NUM - 1;
    UZTestUtil::PrepareOriginZone(totalSlotNum, TRX_PAGE_SLOTS_NUM / 2, originZones[5], startSlotIds, recycleCsns);
    UZTestUtil::PrepareRestoreZone(recycleCsns[5], restoreZones[5]);

    /* check */
    for (int i = 0; i < ZONE_NUM; ++i) {
        ASSERT_EQ(originZones[i]->m_txnSlotManager->GetNextFreeLogicSlotId(),
                  restoreZones[i]->m_txnSlotManager->GetNextFreeLogicSlotId());
        ASSERT_EQ(originZones[i]->m_txnSlotManager->GetRecycleLogicSlotId(),
                  restoreZones[i]->m_txnSlotManager->GetRecycleLogicSlotId());
        ASSERT_EQ(originZones[i]->m_undoRecyclePageId, restoreZones[i]->m_undoRecyclePageId);
        ASSERT_EQ(originZones[i]->m_nextAppendUndoPtr, restoreZones[i]->m_nextAppendUndoPtr);
        for (uint64 j = startSlotIds[i]; j < recycleCsns[i]; ++j) {
            Xid xid = {static_cast<uint64>(i), j};
            TransactionSlot *slot = UZTestUtil::GetSlot(restoreZones[i]->m_txnSlotManager, xid);
            ASSERT_EQ(slot->GetTxnSlotStatus(), TXN_STATUS_FROZEN);
        }
    }

    for (int i = 0; i < ZONE_NUM; ++i) {
        originZones[i]->m_segment->DropSegment();
        delete originZones[i];
        delete restoreZones[i];
    }
}

