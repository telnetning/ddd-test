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
#ifndef UT_FI_UNDO_H
#define UT_FI_UNDO_H

#include "framework/dstore_instance_interface.h"

#include <gtest/gtest.h>

#include "ut_undo/ut_undo_mgr.h"

class UndoZoneTest {
public:
/* this struct used to check */
struct UndoZoneTestInfo {
    UndoRecPtr m_nextAppendUndoPtr;
    PageId m_needCheckPageId;
    PageId m_undoRecyclePageId;
    uint64 m_recycleLogicSlotId;
    uint64 m_nextFreeLogicSlotId;

    UndoZoneTestInfo()
    {
        errno_t rc = memset_s(this, sizeof(UndoZoneTestInfo), 0, sizeof(UndoZoneTestInfo));
        storage_securec_check(rc, "\0", "\0");
    }
};

/* this struct used to record inner info of undozone */
struct UndoZoneRecordInfo {
    Xid xids[TRX_PAGES_PER_ZONE * TRX_PAGE_SLOTS_NUM];
    CommitSeqNo csn[TRX_PAGES_PER_ZONE * TRX_PAGE_SLOTS_NUM];
    UndoRecPtr startUndoPtr[TRX_PAGES_PER_ZONE * TRX_PAGE_SLOTS_NUM];
    UndoRecPtr endUndoPtr[TRX_PAGES_PER_ZONE * TRX_PAGE_SLOTS_NUM];
    UndoRecPtr nextAppendUndoPtr;
    uint32 curTxnNum;

    UndoZoneRecordInfo()
    {
        errno_t rc = memset_s(this, sizeof(UndoZoneRecordInfo), 0, sizeof(UndoZoneRecordInfo));
        storage_securec_check(rc, "\0", "\0");
    }
};

protected:
    void RecordUndoZoneInfo(UndoZone* zone, UndoZoneTestInfo* testInfo)
    {
        testInfo->m_nextAppendUndoPtr = zone->m_nextAppendUndoPtr;
        testInfo->m_needCheckPageId = zone->m_needCheckPageId;
        testInfo->m_undoRecyclePageId = zone->m_undoRecyclePageId;
        testInfo->m_recycleLogicSlotId = zone->m_txnSlotManager->GetRecycleLogicSlotId();
        testInfo->m_nextFreeLogicSlotId = zone->m_txnSlotManager->GetNextFreeLogicSlotId();
    }

    uint32 GetUsedUndoRecordPageNum(UndoZone* undoZone)
    {
        if (undoZone->m_nextAppendUndoPtr.GetBlockNum() >= undoZone->m_undoRecyclePageId.m_blockId) {
            return undoZone->m_nextAppendUndoPtr.GetBlockNum() - undoZone->m_undoRecyclePageId.m_blockId + 1;
        }
        return undoZone->m_nextAppendUndoPtr.GetBlockNum() + UNDO_ZONE_EXTENT_SIZE -
            undoZone->m_undoRecyclePageId.m_blockId;
    }

    void StartService(UndoMgr* undoMgr, TransactionMgr* transMgr, UndoZoneRecordInfo* recordInfo,
        uint32 txnNum, uint32 recordNumPerTxn)
    {
        TdId tdId = 0;
        TD td;
        ItemPointerData ctid;
        CommandId cid = 1;
        UndoRecord undoRecord(UNDO_HEAP_INSERT, tdId, &td, ctid, cid);

        Xid xid;
        for (int i = 0; i < txnNum; i++) {
            transMgr->AllocTransactionSlot(xid);
            recordInfo->xids[recordInfo->curTxnNum] = xid;
            for (int j = 0; j < recordNumPerTxn; j++) {
                thrd->m_walWriterContext->BeginAtomicWal(xid);
                UndoRecPtr undoRecPtr = transMgr->InsertUndoRecord(xid, &undoRecord);
                thrd->m_walWriterContext->EndAtomicWal();
                if (j == 0) {
                    recordInfo->startUndoPtr[recordInfo->curTxnNum] = undoRecPtr;
                }
                if (j == recordNumPerTxn - 1) {
                    recordInfo->endUndoPtr[recordInfo->curTxnNum] = undoRecPtr;
                }
                if (i == txnNum - 1 && j == recordNumPerTxn - 1) {
                    UndoRecord undoRec;
                    undoMgr->m_undoZones[xid.m_zoneId]->FetchUndoRecordInternal(DSTORE::g_defaultPdbId,
                        &undoRec, undoRecPtr, undoMgr->GetBufferMgr());
                    recordInfo->nextAppendUndoPtr =
                        undoMgr->m_undoZones[xid.m_zoneId]->GetNextUndoRecPtr(undoRecPtr, undoRec.GetRecordSize());
                }
            }
            transMgr->CommitTransactionSlot(xid);
            /* record csn */
            UndoZoneTrxManager* txnSlotManager = undoMgr->m_undoZones[xid.m_zoneId]->m_txnSlotManager;
            PageId slotPageId = txnSlotManager->GetTxnSlotPageId(recordInfo->curTxnNum);
            BufferDesc *txnPageBuf = txnSlotManager->ReadTxnSlotPageBuf(slotPageId, LW_SHARED);
            TransactionSlotPage *txnPage = txnSlotManager->GetTxnSlotPage(txnPageBuf);
            uint32 slotId = TransactionSlotPage::GetSlotId(recordInfo->curTxnNum);
            CommitSeqNo slotCsn = txnPage->GetTransactionSlot(slotId)->GetCsn();
            txnSlotManager->UnlockAndReleaseTxnSlotPageBuf(txnPageBuf);
            recordInfo->csn[recordInfo->curTxnNum] = slotCsn;

            recordInfo->curTxnNum++;
        }
    }
};

class UndoFiTest : public UndoMgrTest, public UndoZoneTest {
protected:
    void SetUp() override
    {
        UndoMgrTest::SetUp();
    }
    void TearDown() override
    {
        UndoMgrTest::TearDown();
    }

    void StartUp()
    {
        m_undoMgr = DstoreNew(m_ut_memory_context) UndoMgr(m_bufferMgr, m_pdbId);
        m_undoMgr->Init(g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));
        m_undoMgr->LoadUndoMapSegment();
        m_tranMgr = DstoreNew(m_ut_memory_context) TransactionMgr(m_undoMgr, g_storageInstance->GetCsnMgr(),
            g_defaultPdbId);

    }

    void Reboot()
    {
        delete m_tranMgr;
        delete m_undoMgr;
        m_undoMgr = DstoreNew(m_ut_memory_context) UndoMgr(m_bufferMgr, m_pdbId);
        m_undoMgr->Init(g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));
        m_undoMgr->LoadUndoMapSegment();
        m_tranMgr = DstoreNew(m_ut_memory_context) TransactionMgr(m_undoMgr, g_storageInstance->GetCsnMgr(),
            g_defaultPdbId);
    }

    void ShutDown()
    {
        delete m_tranMgr;
        delete m_undoMgr;
    }

    UndoMgr* m_undoMgr;
    TransactionMgr* m_tranMgr;
};

#endif //UT_FI_UNDO_H
