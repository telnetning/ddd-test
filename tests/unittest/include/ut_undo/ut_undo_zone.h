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
#ifndef UT_UNDO_ZONE_H
#define UT_UNDO_ZONE_H

#include <gtest/gtest.h>

#include "ut_utilities/ut_dstore_framework.h"
#include "undo/dstore_undo_zone.h"
#include "undo/dstore_undo_zone_txn_mgr.h"
#include "ut_mock/ut_tablespace_mock.h"
#include "ut_mock/ut_instance_mock.h"

class UndoZoneTest : public DSTORETEST {
protected:
    void SetUp() override
    {
        DSTORETEST::SetUp();
        MockStorageInstance *instance = DstoreNew(m_ut_memory_context) MockStorageInstance();
        instance->Install(&DSTORETEST::m_guc, m_ut_memory_context);
        instance->Startup(&DSTORETEST::m_guc);

        m_bufferMgr = g_storageInstance->GetBufferMgr();
        m_tablespaceId = static_cast<TablespaceId>(TBS_ID::UNDO_TABLE_SPACE_ID);
        m_segment = (Segment *)SegmentInterface::AllocUndoSegment(g_defaultPdbId, m_tablespaceId,
            SegmentType::UNDO_SEGMENT_TYPE, m_bufferMgr);
        ASSERT_TRUE(SegmentIsValid(m_segment));
    }
    void TearDown() override
    {
        MockStorageInstance *instance = (MockStorageInstance *)g_storageInstance;
        instance->Shutdown();
        delete instance;
        DSTORETEST::TearDown();
    }
    TablespaceId m_tablespaceId;
    BufMgrInterface     *m_bufferMgr;
    Segment    *m_segment;
};

class UZTestUtil {
public:
    static ItemPointerData GetTrxSlotPtr(Xid xid, FileId fileId, uint32 startBlkNum)
    {
        ItemPointerData res;
        uint64 slotId = xid.m_logicSlotId;
        res.SetFileId(fileId);
        res.SetBlockNumber(startBlkNum + (slotId / TRX_PAGE_SLOTS_NUM) % TRX_PAGES_PER_ZONE);
        res.SetOffset((slotId % TRX_PAGE_SLOTS_NUM) * TRX_SLOT_SIZE + TRX_PAGE_HEADER_SIZE);
        StorageAssert(res.GetOffset() + TRX_SLOT_SIZE <= BLCKSZ);
        return res;
    }

    static UndoRecord *CreateUndoRecord(ItemPointerData ctid, TD*  td,
                                        UndoType undoType, uint8 m_tdId, int dataLen)
    {
        UndoRecord *urec = DstoreNew(g_dstoreCurrentMemoryContext) UndoRecord();
        urec->SetCtid(ctid);
        urec->SetPreTdInfo(m_tdId, td);
        urec->SetUndoType(undoType);
        urec->m_dataInfo.len = dataLen;
        urec->m_dataInfo.data = (char*) DstorePalloc0(dataLen);
        return urec;
    }

    static UndoRecord *CreateUndoRecord(int recordSize)
    {
        UndoRecord *urec = DstoreNew(g_dstoreCurrentMemoryContext) UndoRecord();
        urec->m_header.m_undoType = UNDO_HEAP_INPLACE_UPDATE;
        urec->m_dataInfo.len = recordSize - sizeof(UndoRecordHeader) - sizeof(urec->m_dataInfo.len);
        urec->m_dataInfo.data = (char*) DstorePalloc0(urec->m_dataInfo.len);
        return urec;
    }

    static void DestroyUndoRecord(UndoRecord *urec)
    {
        delete urec;
    }

    static void CreateFullPageTrxSlot(UndoZone &uzone, CommitSeqNo csn, UndoRecPtr &undoPtr)
    {
        for (uint32 j = 0; j < TRX_PAGE_SLOTS_NUM; ++j) {
            Xid xid = uzone.AllocSlot();
            auto trxSlot = GetSlot(uzone.m_txnSlotManager, xid);
            trxSlot->SetCsn(csn);
            trxSlot->SetTrxSlotStatus(DSTORE::TXN_STATUS_COMMITTED);
            trxSlot->SetCurTailUndoPtr(undoPtr);
            trxSlot->SetSpaceTailUndoPtr(undoPtr);
        }
    }

    static int GetPageCntInBlkList(PageId &firstUndoPageId, BufMgrInterface *bufferMgr)
    {
        int cnt = 1;
        auto buf = bufferMgr->Read(g_defaultPdbId, firstUndoPageId, LW_SHARED);
        auto page = (UndoRecordPage*)buf->GetPage();
        while (page->m_undoRecPageHeader.prev != firstUndoPageId) {
            cnt++;
            PageId cur = page->m_undoRecPageHeader.prev;
            bufferMgr->UnlockAndRelease(buf);
            buf = bufferMgr->Read(g_defaultPdbId, cur, LW_SHARED);
            page = (UndoRecordPage*)buf->GetPage();
        }
        bufferMgr->UnlockAndRelease(buf);
        return cnt;
    }

    static void AllocSlots(UndoZone *zone, uint64 num)
    {
        TransactionSlot *slot = nullptr;
        for (uint64 i = 0; i < num;) {
            Xid xid = zone->AllocSlot();
            if (xid == INVALID_XID) {
                zone->Recycle(num - i);
                continue;
            }
            slot = GetSlot(zone->m_txnSlotManager, xid);
            slot->SetCsn(i);
            slot->SetTrxSlotStatus(TXN_STATUS_COMMITTED);
            i++;
        }
    }

    static void PrepareOriginZone(uint64 totalSlotNum, uint64 reserveSlotNum, UndoZone *originZone,
                                  std::vector<uint64> &startSlotIds, std::vector<CommitSeqNo> &recycleCsns)
    {
        AllocSlots(originZone, totalSlotNum);
        uint64 slotId = originZone->m_txnSlotManager->GetRecycleLogicSlotId();
        startSlotIds.push_back(slotId);
        CommitSeqNo csn = originZone->m_txnSlotManager->GetNextFreeLogicSlotId() - reserveSlotNum;
        recycleCsns.push_back(csn);
        originZone->Recycle(csn);
        for (; slotId < csn; ++slotId) {
            Xid xid = {static_cast<uint64>(originZone->m_zoneId), slotId};
            TransactionSlot *slot = GetSlot(originZone->m_txnSlotManager, xid);
            slot->SetTrxSlotStatus(TXN_STATUS_COMMITTED);
        }
    }

    static void PrepareRestoreZone(CommitSeqNo csn, UndoZone *restoreZone)
    {
        g_storageInstance->GetCsnMgr()->SetLocalCsnMin(csn);
        restoreZone->Init(g_dstoreCurrentMemoryContext);
    }

    static TransactionSlot *GetSlot(UndoZoneTrxManager *txnMgr, Xid xid)
    {
        /* This is only used for UT signle node, all the zones are owned by node */
        PageId pageId = txnMgr->GetTxnSlotPageId(xid.m_logicSlotId);
        BufferDesc *targetPageBuf = txnMgr->ReadTxnSlotPageBuf(pageId, LW_SHARED);
        TransactionSlotPage *targetPage = txnMgr->GetTxnSlotPage(targetPageBuf);
        uint32 slotId = TransactionSlotPage::GetSlotId(xid.m_logicSlotId);
        TransactionSlot *transSlot = targetPage->GetTransactionSlot(slotId);
        txnMgr->UnlockAndReleaseTxnSlotPageBuf(targetPageBuf);
        return transSlot;
    }

};


#endif
