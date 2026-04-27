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

#include "ut_undo/ut_undo_wal.h"

#include "framework/dstore_instance_interface.h"
#include "ut_undo/ut_undo_zone.h"
#include "ut_tablehandler/ut_table_handler.h"

TEST_F(UndoWalTest, WalUndoRecordTest_level0)
{
    /* step 1: init undo zone */
    const uint32 zoneId = 2;
    UndoZone zone(m_segment, m_bufferMgr, zoneId, m_pdbId);
    zone.Init(g_dstoreCurrentMemoryContext);
    m_bufferMgr->FlushAll(false);

    /* step 2: copy dataFile as Image */
    CopyDataFile();

    /* step 3: get endPlsn of WalFile */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);

    /* step 4: insert undo record */
    UndoRecord *rec = UZTestUtil::CreateUndoRecord(BLCKSZ - sizeof(Page) - sizeof(UndoRecordPageHeader) - 10);
    m_walWriter->BeginAtomicWal(INVALID_XID);
    zone.InsertUndoRecord(rec);
    m_walWriter->EndAtomicWal();

    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());

    /* step 5: read walFile to Get WalRecord */
    std::vector<WalRecordRedoInfo *> records;
    RetStatus retStatus = ReadRecordsAfterPlsn(plsn, WAL_UNDO_INSERT_RECORD, records);
    ASSERT_TRUE(STORAGE_FUNC_SUCC(retStatus));
    ASSERT(records.size() == 1);

    /* step 6: use WalRecord to redo for Page in Image */
    char redoPage[BLCKSZ];
    char afterPage[BLCKSZ];
    const WalRecordUndoRecord *walRec = static_cast<WalRecordUndoRecord *>(&(records[0]->walRecord));
    WalRecordUndo::DumpUndoRecord(static_cast<const WalRecordUndo *>(walRec), m_waldump_fp);
    FetchPage(afterPage, walRec->GetPageId());
    ReadDataFileImage(walRec->GetPageId(), redoPage, BLCKSZ);
    RestorePage(redoPage, walRec->GetPageId());
    WalRecordRedoContext redoCtx = {INVALID_XID, m_walWriter->GetWalId(), DSTORE::g_defaultPdbId};
    RedoUndoRecord(&redoCtx, walRec, m_walStream->GetMaxAppendedPlsn());
    FetchPage(redoPage, walRec->GetPageId());

    /* step 7: compare redoPage and normalPage */
    Page *normalPage = static_cast<Page*>(static_cast<void*>(afterPage));
    EXPECT_EQ(reinterpret_cast<Page *>(redoPage)->GetSelfPageId(), normalPage->GetSelfPageId());
    EXPECT_EQ(reinterpret_cast<Page *>(redoPage)->GetType(), normalPage->GetType());
    EXPECT_TRUE(CmpPages(redoPage + sizeof(Page), reinterpret_cast<char *>(normalPage) + sizeof(Page),
                         BLCKSZ - sizeof(Page)));
}

TEST_F(UndoWalTest, WalUndoPageRingInitTest_level0)
{
    /* step 1: copy dataFile as Image */
    CopyDataFile();

    /* step 2: get endPlsn of WalFile */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);

    /* step 3: init undo zone */
    const uint32 zoneId = 2;
    UndoZone zone(m_segment, m_bufferMgr, zoneId, m_pdbId);
    zone.Init(g_dstoreCurrentMemoryContext);

    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());

    /* step 4: check first page */
    std::vector<WalRecordRedoInfo *> records;
    RetStatus retStatus = ReadRecordsAfterPlsn(plsn, WAL_UNDO_INIT_RECORD_SPACE, records);
    ASSERT_TRUE(STORAGE_FUNC_SUCC(retStatus));
    ASSERT(records.size() == 1);

    char redoPage[BLCKSZ];
    char afterPage[BLCKSZ];
    const WalRecordUndoInitRecSpace *walRec = static_cast<WalRecordUndoInitRecSpace *>(&(records[0]->walRecord));
    WalRecordUndo::DumpUndoRecord(static_cast<const WalRecordUndo *>(walRec), m_waldump_fp);
    FetchPage(afterPage, walRec->GetPageId());
    ReadDataFileImage(walRec->GetPageId(), redoPage, BLCKSZ);
    RestorePage(redoPage, walRec->GetPageId());
    WalRecordRedoContext redoCtx = {INVALID_XID, m_walWriter->GetWalId(), DSTORE::g_defaultPdbId};
    RedoUndoRecord(&redoCtx, walRec, m_walStream->GetMaxAppendedPlsn());
    FetchPage(redoPage, walRec->GetPageId());

    EXPECT_EQ(reinterpret_cast<UndoSegmentMetaPage *>(redoPage)->firstUndoPageId,
              reinterpret_cast<UndoSegmentMetaPage *>(afterPage)->firstUndoPageId);

    /* step 5: check inited ring */
    records.clear();
    retStatus = ReadRecordsAfterPlsn(0, WAL_UNDO_EXTEND_PAGE_RING_NEW_PAGE, records);
    ASSERT_TRUE(STORAGE_FUNC_SUCC(retStatus));
    EXPECT_TRUE(CheckUndoRingPageHdr<WalRecordUndoRingNewPage>(records));
}

TEST_F(UndoWalTest, WalUndoPageRingExtendTest_level0)
{
    /* step 1: fill free undo pages */
    const uint32 zoneId = 2;
    UndoZone zone(m_segment, m_bufferMgr, zoneId, m_pdbId);
    zone.Init(g_dstoreCurrentMemoryContext);
    UndoRecord *rec = UZTestUtil::CreateUndoRecord(BLCKSZ - sizeof(Page) - sizeof(UndoRecordPageHeader) - 10);
    for (int i = 0; i < UNDO_ZONE_EXTENT_SIZE - 1; ++i) {
        m_walWriter->BeginAtomicWal(INVALID_XID);
        zone.InsertUndoRecord(rec);
        m_walWriter->EndAtomicWal();
    }
    m_bufferMgr->FlushAll(false);

    /* step 2: copy dataFile as Image */
    CopyDataFile();

    /* step 3: get endPlsn of WalFile */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);

    /* step 4: extend undo page ring */
    zone.ExtendSpaceIfNeeded(BLCKSZ - sizeof(Page) - sizeof(UndoRecordPageHeader) - 10);

    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());

    /* step 5: check previous page */
    std::vector<WalRecordRedoInfo *> records;
    RetStatus retStatus = ReadRecordsAfterPlsn(plsn, WAL_UNDO_EXTEND_PAGE_RING_PREV_PAGE, records);
    ASSERT_TRUE(STORAGE_FUNC_SUCC(retStatus));
    EXPECT_TRUE(CheckUndoRingPageHdr<WalRecordUndoRingOldPage>(records));

    /* step 6: check next page */
    records.clear();
    retStatus = ReadRecordsAfterPlsn(plsn, WAL_UNDO_EXTEND_PAGE_RING_NEXT_PAGE, records);
    ASSERT_TRUE(STORAGE_FUNC_SUCC(retStatus));
    EXPECT_TRUE(CheckUndoRingPageHdr<WalRecordUndoRingOldPage>(records));

    /* step 7: check new page */
    records.clear();
    retStatus = ReadRecordsAfterPlsn(plsn, WAL_UNDO_EXTEND_PAGE_RING_NEW_PAGE, records);
    ASSERT_TRUE(STORAGE_FUNC_SUCC(retStatus));
    EXPECT_TRUE(CheckUndoRingPageHdr<WalRecordUndoRingNewPage>(records));
}

TEST_F(UndoWalTest, WalUndoTxnPageInitTest_level0)
{
    /* step 1: copy dataFile as Image */
    CopyDataFile();

    /* step 2: get endPlsn of WalFile */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);

    /* step 3: init txn pages */
    const uint32 zoneId = 2;
    UndoZone zone(m_segment, m_bufferMgr, zoneId, m_pdbId);
    zone.Init(g_dstoreCurrentMemoryContext);

    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());

    /* step 4: check txn pages */
    std::vector<WalRecordRedoInfo *> records;
    RetStatus retStatus = ReadRecordsAfterPlsn(plsn, WAL_UNDO_INIT_TXN_PAGE, records);
    ASSERT_TRUE(STORAGE_FUNC_SUCC(retStatus));
    ASSERT(recordList.size() == TRX_PAGES_PER_ZONE);

    char redoPage[BLCKSZ];
    char afterPage[BLCKSZ];
    for (WalRecordRedoInfo *recordInfo : records) {
        const WalRecordUndoInitTxnPage *walRec = static_cast<WalRecordUndoInitTxnPage *>(&(recordInfo->walRecord));
        WalRecordUndo::DumpUndoRecord(static_cast<const WalRecordUndo *>(walRec), m_waldump_fp);
        FetchPage(afterPage, walRec->GetPageId());
        ReadDataFileImage(walRec->GetPageId(), redoPage, BLCKSZ);
        RestorePage(redoPage, walRec->GetPageId());
        WalRecordRedoContext redoCtx = {INVALID_XID, m_walWriter->GetWalId(), DSTORE::g_defaultPdbId};
        RedoUndoRecord(&redoCtx, walRec, m_walStream->GetMaxAppendedPlsn());
        FetchPage(redoPage, walRec->GetPageId());
        /* exclude m_glsn, m_plsn and m_checksum */
        EXPECT_TRUE(CmpPages(redoPage + OFFSETOF(Page::PageHeader, m_lower),
                             afterPage + OFFSETOF(Page::PageHeader, m_lower),
                             BLCKSZ - OFFSETOF(Page::PageHeader, m_lower)));
    }

    /* step 5: check meta page */
    records.clear();
    retStatus = ReadRecordsAfterPlsn(plsn, WAL_UNDO_SET_TXN_PAGE_INITED, records);
    ASSERT_TRUE(STORAGE_FUNC_SUCC(retStatus));
    ASSERT(records.size() == 1);

    const WalRecordUndoTxnSlotPageInited *walRec =
        static_cast<WalRecordUndoTxnSlotPageInited *>(&(records[0]->walRecord));
    WalRecordUndo::DumpUndoRecord(static_cast<const WalRecordUndo *>(walRec), m_waldump_fp);
    FetchPage(afterPage, walRec->GetPageId());
    ReadDataFileImage(walRec->GetPageId(), redoPage, BLCKSZ);
    RestorePage(redoPage, walRec->GetPageId());
    WalRecordRedoContext redoCtx = {INVALID_XID, m_walWriter->GetWalId(), DSTORE::g_defaultPdbId};
    RedoUndoRecord(&redoCtx, walRec, m_walStream->GetMaxAppendedPlsn());
    FetchPage(redoPage, walRec->GetPageId());
    EXPECT_EQ(reinterpret_cast<UndoSegmentMetaPage *>(redoPage)->alreadyInitTxnSlotPages,
              reinterpret_cast<UndoSegmentMetaPage *>(afterPage)->alreadyInitTxnSlotPages);
}

TEST_F(UndoWalTest, WalUndoSlotPtrUpdTest_level0)
{
    /* step 1: init undo zone */
    const uint32 zoneId = 2;
    UndoZone zone(m_segment, m_bufferMgr, zoneId, m_pdbId);
    zone.Init(g_dstoreCurrentMemoryContext);
    m_bufferMgr->FlushAll(false);

    /* step 2: copy dataFile as Image */
    CopyDataFile();

    /* step 3: get endPlsn of WalFile */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);

    /* step 4: update slot tail ptr */
    m_walWriter->BeginAtomicWal(INVALID_XID);
    (void)zone.SetSlotUndoPtr({zoneId, 1}, UndoRecPtr(1), true);
    m_walWriter->EndAtomicWal();

    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());

    /* step 5: check txn page */
    std::vector<WalRecordRedoInfo *> records;
    RetStatus retStatus = ReadRecordsAfterPlsn(plsn, WAL_UNDO_UPDATE_TXN_SLOT_PTR, records);
    ASSERT_TRUE(STORAGE_FUNC_SUCC(retStatus));
    ASSERT(records.size() == 1);

    char redoPage[BLCKSZ];
    char afterPage[BLCKSZ];
    const WalRecordUndoUpdateSlotUndoPtr *walRec =
        static_cast<WalRecordUndoUpdateSlotUndoPtr *>(&(records[0]->walRecord));
    WalRecordUndo::DumpUndoRecord(static_cast<const WalRecordUndo *>(walRec), m_waldump_fp);
    FetchPage(afterPage, walRec->GetPageId());
    ReadDataFileImage(walRec->GetPageId(), redoPage, BLCKSZ);
    RestorePage(redoPage, walRec->GetPageId());
    WalRecordRedoContext redoCtx = {INVALID_XID, m_walWriter->GetWalId(), DSTORE::g_defaultPdbId};
    RedoUndoRecord(&redoCtx, walRec, m_walStream->GetMaxAppendedPlsn());
    FetchPage(redoPage, walRec->GetPageId());
    /* exclude m_glsn, m_plsn and m_checksum */
    EXPECT_TRUE(CmpPages(redoPage + OFFSETOF(Page::PageHeader, m_lower),
                         afterPage + OFFSETOF(Page::PageHeader, m_lower),
                         BLCKSZ - OFFSETOF(Page::PageHeader, m_lower)));
}

TEST_F(UndoWalTest, WalUndoTxnSlotAllocateTest_level0)
{
    /* step 1: init undo zone */
    const uint32 zoneId = 2;
    UndoZone zone(m_segment, m_bufferMgr, zoneId, m_pdbId);
    zone.Init(g_dstoreCurrentMemoryContext);
    m_bufferMgr->FlushAll(false);

    /* step 2: copy dataFile as Image */
    CopyDataFile();

    /* step 3: get endPlsn of WalFile */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);

    /* step 4: allocate slots until using next page */
    Xid xid;
    for (int i = 0; i < TRX_PAGE_SLOTS_NUM + 1; ++i) {
        xid = zone.AllocSlot();
    }

    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());

    std::vector<WalRecordRedoInfo *> records;
    RetStatus retStatus = ReadRecordsAfterPlsn(plsn, WAL_UNDO_ALLOCATE_TXN_SLOT, records);
    ASSERT_TRUE(STORAGE_FUNC_SUCC(retStatus));

    /* step 5: check the second txn slot page, because there is only one slot. */
    char redoPage[BLCKSZ];
    char afterPage[BLCKSZ];
    const WalRecordUndoTxnSlotAllocate *walRec =
            static_cast<WalRecordUndoTxnSlotAllocate *>(&(records.back()->walRecord));
    WalRecordUndo::DumpUndoRecord(static_cast<const WalRecordUndo *>(walRec), m_waldump_fp);
    FetchPage(afterPage, walRec->GetPageId());
    ReadDataFileImage(walRec->GetPageId(), redoPage, BLCKSZ);
    RestorePage(redoPage, walRec->GetPageId());
    WalRecordRedoContext redoCtx = {xid, m_walWriter->GetWalId(), DSTORE::g_defaultPdbId};
    RedoUndoRecord(&redoCtx, walRec, m_walStream->GetMaxAppendedPlsn());
    FetchPage(redoPage, walRec->GetPageId());
    /* exclude m_glsn, m_plsn and m_checksum */
    EXPECT_TRUE(CmpPages(redoPage + OFFSETOF(Page::PageHeader, m_lower),
                         afterPage + OFFSETOF(Page::PageHeader, m_lower),
                         BLCKSZ - OFFSETOF(Page::PageHeader, m_lower)));
}

TEST_F(UndoWalTest, WalUndoTxnCommitTest_level0)
{
    /* step 1: init undo zone */
    const uint32 zoneId = 2;
    UndoZone zone(m_segment, m_bufferMgr, zoneId, m_pdbId);
    zone.Init(g_dstoreCurrentMemoryContext);
    m_bufferMgr->FlushAll(false);
    CommitSeqNo csn1 = 1;
    CommitSeqNo csn2 = 2;
    /* step 2: copy dataFile as Image */
    CopyDataFile();

    /* step 3: get endPlsn of WalFile */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);

    /* step 4: transaction commit */
    zone.Commit<TXN_STATUS_COMMITTED>({zoneId, 1}, csn1);
    zone.Commit<TXN_STATUS_PENDING_COMMIT>({zoneId, 2}, csn2);

    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());

    /* step 5: check txn page */
    std::vector<WalRecordRedoInfo *> records;
    RetStatus retStatus = ReadRecordsAfterPlsn(plsn, WAL_TXN_COMMIT, records);
    ASSERT_TRUE(STORAGE_FUNC_SUCC(retStatus));
    ASSERT(records.size() == 2);

    char redoPage[BLCKSZ];
    char afterPage[BLCKSZ];
    PageId pageId = static_cast<WalRecordTransactionCommit *>(&(records[0]->walRecord))->GetPageId();
    FetchPage(afterPage, pageId);
    ReadDataFileImage(pageId, redoPage, BLCKSZ);
    RestorePage(redoPage, pageId);
    plsn = m_walStream->GetMaxAppendedPlsn() - records.size() + 1;
    for (WalRecordRedoInfo *recordInfo : records) {
        const WalRecordTransactionCommit *walRec = static_cast<WalRecordTransactionCommit *>(&(recordInfo->walRecord));
        WalRecordUndo::DumpUndoRecord(static_cast<const WalRecordUndo *>(walRec), m_waldump_fp);
        WalRecordRedoContext redoCtx = {INVALID_XID, m_walWriter->GetWalId(), DSTORE::g_defaultPdbId};
        RedoUndoRecord(&redoCtx, walRec, plsn++);
    }
    FetchPage(redoPage, pageId);
    /* exclude m_glsn, m_plsn and m_checksum */
    for (uint8 i =0; i< TRX_PAGE_SLOTS_NUM; i++) {
        TransactionSlot* originSlot = &((TransactionSlotPage*)redoPage)->m_slots[i];
        TransactionSlot* afterSlot = &((TransactionSlotPage*)afterPage)->m_slots[i];
        ASSERT_EQ(originSlot->curTailUndoPtr, afterSlot->curTailUndoPtr);
        ASSERT_EQ(originSlot->csn, afterSlot->csn);
        ASSERT_EQ(originSlot->status, afterSlot->status);
    }
}

TEST_F(UndoWalTest, WalUndoTxnAbortTest_level0)
{
    /* step 1: init undo zone */
    const uint32 zoneId = 2;
    UndoZone zone(m_segment, m_bufferMgr, zoneId, m_pdbId);
    zone.Init(g_dstoreCurrentMemoryContext);
    m_bufferMgr->FlushAll(false);

    /* step 2: copy dataFile as Image */
    CopyDataFile();

    /* step 3: get endPlsn of WalFile */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);

    /* step 4: transaction abort */
    Xid xid = zone.AllocSlot();
    zone.RollbackUndoZone(xid, false);

    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());

    /* step 5: check txn page */
    std::vector<WalRecordRedoInfo *> records;
    RetStatus retStatus = ReadRecordsAfterPlsn(plsn, WAL_UNDO_ALLOCATE_TXN_SLOT, records);
    ASSERT_TRUE(STORAGE_FUNC_SUCC(retStatus));
    ASSERT(records.size() == 1);

    char redoPage[BLCKSZ];
    char afterPage[BLCKSZ];
    const WalRecordUndoTxnSlotAllocate *walRec =
        static_cast<WalRecordUndoTxnSlotAllocate *>(&(records.front()->walRecord));
    FetchPage(afterPage, walRec->GetPageId());
    ReadDataFileImage(walRec->GetPageId(), redoPage, BLCKSZ);
    RestorePage(redoPage, walRec->GetPageId());
    WalRecordRedoContext redoCtx = {xid, m_walWriter->GetWalId(), DSTORE::g_defaultPdbId};
    RedoUndoRecord(&redoCtx, walRec, m_walStream->GetMaxAppendedPlsn() - 1);
    FetchPage(redoPage, walRec->GetPageId());

    records.clear();
    retStatus = ReadRecordsAfterPlsn(plsn, WAL_TXN_ABORT, records);
    ASSERT_TRUE(STORAGE_FUNC_SUCC(retStatus));
    ASSERT(recordList.size() == 1);

    WalRecordTransactionAbort *walRecord = static_cast<WalRecordTransactionAbort *>(&(records[0]->walRecord));
    WalRecordUndo::DumpUndoRecord(static_cast<const WalRecordUndo *>(walRecord), m_waldump_fp);
    RedoUndoRecord(&redoCtx, walRecord, m_walStream->GetMaxAppendedPlsn());
    FetchPage(redoPage, walRecord->GetPageId());

    /* exclude m_glsn, m_plsn and m_checksum */
    EXPECT_TRUE(CmpPages(redoPage + OFFSETOF(Page::PageHeader, m_lower),
                         afterPage + OFFSETOF(Page::PageHeader, m_lower),
                         BLCKSZ - OFFSETOF(Page::PageHeader, m_lower)));
}

TEST_F(UndoWalTest, WalExecUndoForDataTest_level0)
{
    /* step 1. Prepare an index */
    UTTableHandler *utTableHandler = UTTableHandler::CreateTableHandler(g_defaultPdbId, m_ut_memory_context);
    int indexCols[] = {1, 2};
    bool isUnique = false;
    int numAttrs = sizeof(indexCols) / sizeof(int);
    utTableHandler->CreateIndex(indexCols, numAttrs, false);
    Transaction *txn = thrd->GetActiveTransaction();

    /* step 2: Index insert */
    txn->Start();
    txn->SetSnapshotCsn();
    IndexTuple *indexTuple = utTableHandler->InsertRandomIndexTuple(true);
    EXPECT_NE(indexTuple, nullptr);
    DstorePfree(indexTuple);
    m_bufferMgr->FlushAll(false);

    /* step 3: copy dataFile as Image */
    CopyDataFile();

    /* step 4: get endPlsn of WalFile */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);

    /* step 5: transaction abort */
    txn->Abort();

    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());

    /* step 6: check heap page */
    std::vector<WalRecordRedoInfo *> records;
    RetStatus retStatus = ReadRecordsAfterPlsn(plsn, WAL_UNDO_HEAP, records);
    ASSERT_TRUE(STORAGE_FUNC_SUCC(retStatus));
    ASSERT(records.size() == 1);

    char redoPage[BLCKSZ];
    char afterPage[BLCKSZ];
    const WalRecordRollbackForData *walRec = static_cast<const WalRecordRollbackForData *>(&(records[0]->walRecord));
    WalRecordUndo::DumpUndoRecord(static_cast<const WalRecordUndo *>(walRec), m_waldump_fp);
    FetchPage(afterPage, walRec->GetPageId());
    ReadDataFileImage(walRec->GetPageId(), redoPage, BLCKSZ);
    RestorePage(redoPage, walRec->GetPageId());
    WalRecordRedoContext redoCtx = {INVALID_XID, m_walWriter->GetWalId(), DSTORE::g_defaultPdbId};
    RedoUndoRecord(&redoCtx, walRec, m_walStream->GetMaxAppendedPlsn() - 1);
    FetchPage(redoPage, walRec->GetPageId());

    /* exclude m_glsn, m_plsn and m_checksum */
    EXPECT_TRUE(CmpPages(redoPage + OFFSETOF(Page::PageHeader, m_lower),
                         afterPage + OFFSETOF(Page::PageHeader, m_lower),
                         BLCKSZ - OFFSETOF(Page::PageHeader, m_lower)));

    /* step 6: check btree page */
    ASSERT_TRUE(STORAGE_FUNC_SUCC(ReadRecordsAfterPlsn(plsn, WAL_UNDO_BTREE, records)));
    ASSERT(records.size() == 2);

    walRec = static_cast<WalRecordRollbackForData *>(&(records[1]->walRecord));
    FetchPage(afterPage, walRec->GetPageId());
    ReadDataFileImage(walRec->GetPageId(), redoPage, BLCKSZ);
    RestorePage(redoPage, walRec->GetPageId());
    RedoUndoRecord(&redoCtx, walRec, m_walStream->GetMaxAppendedPlsn() - 1);
    FetchPage(redoPage, walRec->GetPageId());

    /* exclude m_glsn, m_plsn and m_checksum */
    EXPECT_TRUE(CmpPages(redoPage + OFFSETOF(Page::PageHeader, m_lower),
                         afterPage + OFFSETOF(Page::PageHeader, m_lower),
                         BLCKSZ - OFFSETOF(Page::PageHeader, m_lower)));
    UTTableHandler::Destroy(utTableHandler);
}

/*
 * This case ensure if we did't persist WAL_UNDO_SET_TXN_PAGE_INITED and WAL_UNDO_INIT_RECORD_SPACE wal log,
 * undo restore normal.
 */
TEST_F(UndoWalTest, WalUndoInitTxnSlotPageFailTest_level0)
{
    /* step 1: copy dataFile as Image */
    CopyDataFile();

    /* step 2: get endPlsn of WalFile */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);

    /* step 3: init undo zone and init transaction slot fail */
    const uint32 zoneId = 2;
    UndoZone zone(m_segment, m_bufferMgr, zoneId, m_pdbId);
    FAULT_INJECTION_ACTIVE_MODE_LEVEL(DstoreUndoFI::PERSIST_INIT_TRANSACTION_SLOT_FAIL, 0, FI_GLOBAL, 0, 1);
    zone.Init(g_dstoreCurrentMemoryContext);
    FAULT_INJECTION_INACTIVE(DstoreUndoFI::PERSIST_INIT_TRANSACTION_SLOT_FAIL, FI_GLOBAL);

    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());

    /* step 4: check wal record num */
    std::vector<WalRecordRedoInfo *> records;
    RetStatus retStatus = ReadRecordsAfterPlsn(plsn, WAL_UNDO_SET_TXN_PAGE_INITED, records);
    ASSERT_TRUE(STORAGE_FUNC_SUCC(retStatus));
    ASSERT_EQ(records.size(), 0);
    records.clear();
    retStatus = ReadRecordsAfterPlsn(plsn, WAL_UNDO_INIT_RECORD_SPACE, records);
    ASSERT_TRUE(STORAGE_FUNC_SUCC(retStatus));
    ASSERT_EQ(records.size(), 0);

    plsn = m_walStream->GetMaxAppendedPlsn();
    /* step 5: init undo zone again */
    zone.Init(g_dstoreCurrentMemoryContext);

    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());

    /* step 6: check wal record num */
    records.clear();
    retStatus = ReadRecordsAfterPlsn(plsn, WAL_UNDO_SET_TXN_PAGE_INITED, records);
    ASSERT_TRUE(STORAGE_FUNC_SUCC(retStatus));
    ASSERT_EQ(records.size(), 1);
    records.clear();
    retStatus = ReadRecordsAfterPlsn(plsn, WAL_UNDO_INIT_RECORD_SPACE, records);
    ASSERT_TRUE(STORAGE_FUNC_SUCC(retStatus));
    ASSERT_EQ(records.size(), 1);
}

/*
 * This case ensure if we did't persist WAL_UNDO_INIT_RECORD_SPACE wal log, undo restore normal.
 */
TEST_F(UndoWalTest, WalUndoInitUndoRecordPageFailTest_level0)
{
    /* step 1: copy dataFile as Image */
    CopyDataFile();

    /* step 2: get endPlsn of WalFile */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);

    /* step 3: init undo zone and init transaction slot fail */
    const uint32 zoneId = 2;
    UndoZone zone(m_segment, m_bufferMgr, zoneId, m_pdbId);
    FAULT_INJECTION_ACTIVE_MODE_LEVEL(DstoreUndoFI::PERSIST_INIT_UNDO_RECORD_FAIL, 0, FI_GLOBAL, 0, 1);
    zone.Init(g_dstoreCurrentMemoryContext);
    FAULT_INJECTION_INACTIVE(DstoreUndoFI::PERSIST_INIT_UNDO_RECORD_FAIL, FI_GLOBAL);

    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());

    /* step 4: check wal record num */
    std::vector<WalRecordRedoInfo *> records;
    RetStatus retStatus = ReadRecordsAfterPlsn(plsn, WAL_UNDO_SET_TXN_PAGE_INITED, records);
    ASSERT_TRUE(STORAGE_FUNC_SUCC(retStatus));
    ASSERT_EQ(records.size(), 1);
    records.clear();
    retStatus = ReadRecordsAfterPlsn(plsn, WAL_UNDO_INIT_RECORD_SPACE, records);
    ASSERT_TRUE(STORAGE_FUNC_SUCC(retStatus));
    ASSERT_EQ(records.size(), 0);
    records.clear();

    plsn = m_walStream->GetMaxAppendedPlsn();
    /* step 5: init undo zone again */
    zone.Init(g_dstoreCurrentMemoryContext);

    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());

    /* step 6: check wal record num */
    records.clear();
    retStatus = ReadRecordsAfterPlsn(plsn, WAL_UNDO_SET_TXN_PAGE_INITED, records);
    ASSERT_TRUE(STORAGE_FUNC_SUCC(retStatus));
    ASSERT_EQ(records.size(), 0);
    records.clear();
    retStatus = ReadRecordsAfterPlsn(plsn, WAL_UNDO_INIT_RECORD_SPACE, records);
    ASSERT_TRUE(STORAGE_FUNC_SUCC(retStatus));
    ASSERT_EQ(records.size(), 1);
}
TEST_F(UndoWalTest, CompressAndDecompress_SetZoneSegmentIdRecordTest)
{
    uint32 walDataSize = sizeof(WalRecordUndoZoneSegmentId);
    WalRecordUndoZoneSegmentId *walDataPtr =
        (WalRecordUndoZoneSegmentId *)BuildWalForPage(WAL_UNDO_SET_ZONE_SEGMENT_ID, walDataSize);
    walDataPtr->m_offset = 100;
    walDataPtr->m_segmentId = PageId{4, 100};
    WalRecord *logRecord = CompressRecord(walDataPtr);
    WalRecordUndoZoneSegmentId *walRecord = static_cast<WalRecordUndoZoneSegmentId *>(DecompressRecord(logRecord));
    DstorePfreeExt(logRecord);
    CheckWalRecordForPage(walRecord, walDataPtr);
    ASSERT_EQ(walRecord->m_offset, walDataPtr->m_offset);
    ASSERT_EQ(walRecord->m_segmentId, walDataPtr->m_segmentId);
    DstorePfreeExt(walDataPtr);
    DstorePfreeExt(walRecord);
}
TEST_F(UndoWalTest, CompressAndDecompress_HeapPageRollBackRecordTest)
{
    uint32 walDataSize = sizeof(WalRecordRollbackForData);
    WalRecordRollbackForData *walDataPtr =
        (WalRecordRollbackForData *)BuildWalForPage(WAL_UNDO_HEAP_PAGE_ROLL_BACK, walDataSize);
    walDataPtr->m_undoType = UndoType::UNDO_HEAP_INSERT;
    walDataPtr->m_tdId = 1;
    walDataPtr->m_preCsnStatus = TdCsnStatus::IS_CUR_XID_CSN;
    walDataPtr->m_tupOff = 1000;
    walDataPtr->m_preXid = 1300;
    walDataPtr->m_prePtr = 1500;
    walDataPtr->m_preCsn = 100;
    WalRecord *logRecord = CompressRecord(walDataPtr);
    WalRecordRollbackForData *walRecord = static_cast<WalRecordRollbackForData *>(DecompressRecord(logRecord));
    DstorePfreeExt(logRecord);
    CheckWalRecordForPage(walRecord, walDataPtr);
    ASSERT_EQ(walRecord->m_undoType, walDataPtr->m_undoType);
    ASSERT_EQ(walRecord->m_tdId, walDataPtr->m_tdId);
    ASSERT_EQ(walRecord->m_preCsnStatus, walDataPtr->m_preCsnStatus);
    ASSERT_EQ(walRecord->m_tupOff, walDataPtr->m_tupOff);
    ASSERT_EQ(walRecord->m_preXid, walDataPtr->m_preXid);
    ASSERT_EQ(walRecord->m_prePtr, walDataPtr->m_prePtr);
    ASSERT_EQ(walRecord->m_preCsn, walDataPtr->m_preCsn);
    DstorePfreeExt(walDataPtr);
    DstorePfreeExt(walRecord);
}