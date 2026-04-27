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

#include "heap/dstore_heap_prune.h"
#include "heap/dstore_heap_wal_struct.h"
#include "wal/dstore_wal_struct.h"
#include "ut_utilities/ut_dstore_framework.h"
#include "heap/dstore_heap_wal_struct.h"
#include "page/dstore_heap_page.h"
#include "ut_tablehandler/ut_table_handler.h"
#include "ut_wal/ut_wal_basic.h"
#include "ut_heap/ut_heap.h"
#include "ut_heap/ut_heap_wal.h"

TEST_F(HeapWalTest, WalHeapInsertTest_level0)
{
    /* step 1: copy dataFile as Image */
    CopyDataFile();
    /* step 2: get endPlsn of WalFile */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);

    /* Step 3: Insert one tuple */
    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetSnapshotCsn();
    HeapTuple *heapTuple = m_utTableHandler->GenerateRandomHeapTuple();
    ItemPointerData ctid = m_utTableHandler->InsertHeapTupAndCheckResult(heapTuple, true, txn->GetSnapshotData());
    Xid xid = txn->GetCurrentXid();
    txn->Commit();
    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());

    /* step 4: read walFile to Get WalRecord of WAL_HEAP_INSERT type */
    std::vector<WalRecordRedoInfo *> recordList;
    RetStatus retStatus = ReadRecordsAfterPlsn(plsn, WAL_HEAP_INSERT, recordList);
    ASSERT_TRUE(STORAGE_FUNC_SUCC(retStatus));

    /* step 5: use WalRecord to redo for Page in Image */
    char redoPage[BLCKSZ];
    char afterPage[BLCKSZ];
    for (WalRecordRedoInfo *recordInfo: recordList) {
        WalRecordHeap *record = static_cast<WalRecordHeap *>(&(recordInfo->walRecord));
        WalDumpInfo(record);
        FetchPage(afterPage, record->m_pageId);
        InitPage(record->m_pageId);
        WalRecordRedoContext redoCtx = {xid, m_walWriter->GetWalId(), DSTORE::g_defaultPdbId};
        RedoHeapRecord(&redoCtx, record, m_walStream->GetMaxAppendedPlsn());
        FetchPage(redoPage, record->m_pageId);
    }

    /* step 6: compare redoPage and afterPage */
    HeapPage* redoHeapPage = reinterpret_cast<HeapPage *>(redoPage);
    HeapPage* afterHeapPage = reinterpret_cast<HeapPage *>(afterPage);

    /* Compare page info from m_flags to potentialDelSize and recentDeadTupleMinCsn */
    uint32 cmpSize =
        OFFSETOF(HeapPage, m_heapPageHeader.fsmIndex) - OFFSETOF(HeapPage, m_header.m_flags);
    ASSERT_TRUE(CmpPages(redoPage + OFFSETOF(HeapPage, m_header.m_flags),
        afterPage + OFFSETOF(HeapPage, m_header.m_flags), cmpSize));
    ASSERT_TRUE(redoHeapPage->GetRecentDeadTupleMinCsn() == afterHeapPage->GetRecentDeadTupleMinCsn());

    /* Compare first Td */
    ASSERT_TRUE(CmpTDInfo(static_cast<TD*>(static_cast<void*>(redoPage + OFFSETOF(HeapPage, m_data))),
        static_cast<TD*>(static_cast<void*>(afterPage + OFFSETOF(HeapPage, m_data)))));

    /* Compare from second TD to the page end */
    cmpSize = BLCKSZ - OFFSETOF(HeapPage, m_data) - sizeof(TD);
    ASSERT_TRUE(CmpPages(redoPage + OFFSETOF(HeapPage, m_data) + sizeof(TD),
        afterPage + OFFSETOF(HeapPage, m_data) + sizeof(TD), cmpSize));
}

TEST_F(HeapWalTest, WalHeapBatchInsertTest_level0)
{
    /* step 1: copy dataFile as Image */
    CopyDataFile();
    /* step 2: get endPlsn of WalFile */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);

    /* Step 3: Insert some tuples */
    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetSnapshotCsn();
    uint16 nTuples = 4;
    HeapTuple *heapTuples[nTuples];
    uint32_t diskTupleLens[nTuples];
    for (int i = 0; i < nTuples; i++) {
        HeapTuple *heapTuple = nullptr;
        /* generate some big tuples */
        heapTuple = m_utTableHandler->GenerateRandomHeapTuple();
        uint32_t diskTupleLen = heapTuple->GetDiskTupleSize();
        heapTuples[i] = heapTuple;
        diskTupleLens[i] = diskTupleLen;
    }
    ItemPointerData ctids[nTuples];
    m_utTableHandler->BatchInsertHeapTupsAndCheckResult(heapTuples, nTuples, ctids, txn->GetSnapshotData(), true);
    Xid xid = txn->GetCurrentXid();
    txn->Commit();
    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());

    /* step 4: read walFile to Get WalRecord of WAL_HEAP_BATCH_INSERT type */
    std::vector<WalRecordRedoInfo *> recordList;
    RetStatus retStatus = ReadRecordsAfterPlsn(plsn, WAL_HEAP_BATCH_INSERT, recordList);
    ASSERT_TRUE(STORAGE_FUNC_SUCC(retStatus));

    /* step 5: use WalRecord to redo for Page in Image */
    char redoPage[BLCKSZ];
    char afterPage[BLCKSZ];
    for (WalRecordRedoInfo *recordInfo: recordList) {
        WalRecordHeap *record = static_cast<WalRecordHeap *>(&(recordInfo->walRecord));
        WalDumpInfo(record);
        FetchPage(afterPage, record->m_pageId);
        InitPage(record->m_pageId);
        WalRecordRedoContext redoCtx = {xid, m_walWriter->GetWalId(), DSTORE::g_defaultPdbId};
        RedoHeapRecord(&redoCtx, record, m_walStream->GetMaxAppendedPlsn());
        FetchPage(redoPage, record->m_pageId);
    }

    /* step 6: compare redoPage and afterPage */
    HeapPage* redoHeapPage = reinterpret_cast<HeapPage *>(redoPage);
    HeapPage* afterHeapPage = reinterpret_cast<HeapPage *>(afterPage);
    /* Compare page info from m_flags to potentialDelSize and recentDeadTupleMinCsn */
    uint32 cmpSize =
        OFFSETOF(HeapPage, m_heapPageHeader.fsmIndex) - OFFSETOF(HeapPage, m_header.m_flags);
    ASSERT_TRUE(CmpPages(redoPage + OFFSETOF(HeapPage, m_header.m_flags),
        afterPage + OFFSETOF(HeapPage, m_header.m_flags), cmpSize));
    ASSERT_TRUE(redoHeapPage->GetRecentDeadTupleMinCsn() == afterHeapPage->GetRecentDeadTupleMinCsn());

    /* Compare first Td */
    ASSERT_TRUE(CmpTDInfo(static_cast<TD*>(static_cast<void*>(redoPage + OFFSETOF(HeapPage, m_data))),
        static_cast<TD*>(static_cast<void*>(afterPage + OFFSETOF(HeapPage, m_data)))));

    /* Compare form the second TD to the page end */
    cmpSize = BLCKSZ - OFFSETOF(HeapPage, m_data) - sizeof(TD);
    ASSERT_TRUE(CmpPages(redoPage + OFFSETOF(HeapPage, m_data) + sizeof(TD),
        afterPage + OFFSETOF(HeapPage, m_data) + sizeof(TD), cmpSize));
}

TEST_F(HeapWalTest, WalHeapDeleteTest_level0)
{
    /* Step 1: Insert one tuple */
    HeapTuple *heapTuple = m_utTableHandler->GenerateRandomHeapTuple();
    ItemPointerData ctid = m_utTableHandler->InsertHeapTupAndCheckResult(heapTuple);
    g_storageInstance->GetBufferMgr()->FlushAll(false);

    /* step 2: copy dataFile as Image */
    CopyDataFile();
    char tmpPage[BLCKSZ];
    FetchPage(tmpPage, ctid.GetPageId());
    /* step 3: get endPlsn of WalFile */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);

    /* step 4: Delete the tuple */
    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    ASSERT_TRUE(STORAGE_FUNC_SUCC(DeleteTuple(&ctid, INVALID_SNAPSHOT, true)));
    Xid xid = txn->GetCurrentXid();
    txn->Commit();
    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());

    /* step 5: read walFile to Get WalRecord of WAL_HEAP_DELETE type */
    std::vector<WalRecordRedoInfo *> recordList;
    RetStatus retStatus = ReadRecordsAfterPlsn(plsn, WAL_HEAP_DELETE, recordList);
    ASSERT_TRUE(STORAGE_FUNC_SUCC(retStatus));

    /* step 6: use WalRecord to redo for Page in Image */
    char redoPage[BLCKSZ];
    char afterPage[BLCKSZ];
    for (WalRecordRedoInfo *recordInfo: recordList) {
        WalRecordHeap *record = static_cast<WalRecordHeap *>(&(recordInfo->walRecord));
        WalDumpInfo(record);
        FetchPage(afterPage, record->m_pageId);
        RestorePage(tmpPage, record->m_pageId);
        WalRecordRedoContext redoCtx = {xid, m_walWriter->GetWalId(), DSTORE::g_defaultPdbId};
        RedoHeapRecord(&redoCtx, record, m_walStream->GetMaxAppendedPlsn());
        FetchPage(redoPage, record->m_pageId);
    }

    /* step 7: compare redoPage and afterPage */
    HeapPage* redoHeapPage = reinterpret_cast<HeapPage *>(redoPage);
    HeapPage* afterHeapPage = reinterpret_cast<HeapPage *>(afterPage);
    /* Compare page info from m_flags to potentialDelSize and recentDeadTupleMinCsn */
    uint32 cmpSize =
        OFFSETOF(HeapPage, m_heapPageHeader.fsmIndex) - OFFSETOF(HeapPage, m_header.m_flags);
    ASSERT_TRUE(CmpPages(redoPage + OFFSETOF(HeapPage, m_header.m_flags),
        afterPage + OFFSETOF(HeapPage, m_header.m_flags), cmpSize));
    ASSERT_TRUE(redoHeapPage->GetRecentDeadTupleMinCsn() == afterHeapPage->GetRecentDeadTupleMinCsn());

    /* Compare first Td and second Td */
    TD* redoTd = static_cast<TD*>(static_cast<void*>(redoPage + OFFSETOF(HeapPage, m_data)));
    TD* afterTd = static_cast<TD*>(static_cast<void*>(afterPage + OFFSETOF(HeapPage, m_data)));
    /* delete operation will call CheckTupleChanged and fill csn which will refresh tdstatus to OCCUPY_TRX_END.
     * fill csn not record wal */
    ASSERT_EQ(redoTd->GetStatus(), TDStatus::OCCUPY_TRX_IN_PROGRESS);
    redoTd->SetStatus(TDStatus::OCCUPY_TRX_END);
    ASSERT_TRUE(CmpTDInfo(redoTd, afterTd));
    ASSERT_TRUE(CmpTDInfo(&redoTd[1], &afterTd[1]));

    /* Compare page form the third TD to the page end */
    cmpSize = BLCKSZ - OFFSETOF(HeapPage, m_data) - sizeof(TD) * 2;
    ASSERT_TRUE(CmpPages(redoPage + OFFSETOF(HeapPage, m_data) + sizeof(TD) * 2,
        afterPage + OFFSETOF(HeapPage, m_data) + sizeof(TD) * 2, cmpSize));
}

TEST_F(HeapWalTest, WalHeapInplaceUpdateTest_level0)
{
    /* Step 1: Insert one tuple */
    HeapTuple *tuple1 = m_utTableHandler->GenerateSpecificHeapTuple("insert_data1");
    ItemPointerData ctid = m_utTableHandler->InsertHeapTupAndCheckResult(tuple1);
    g_storageInstance->GetBufferMgr()->FlushAll(false);

    /* step 2: copy dataFile as Image */
    CopyDataFile();
    char tmpPage[BLCKSZ];
    FetchPage(tmpPage, ctid.GetPageId());
    /* step 3: get endPlsn of WalFile */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);

    /* Step 4: Inplace Uptate */
    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    HeapTuple *tuple2 = m_utTableHandler->GenerateSpecificHeapTuple("update_data1");
    (void)UpdateTuple(&ctid, tuple2, INVALID_SNAPSHOT, true);
    Xid xid = txn->GetCurrentXid();
    txn->Commit();
    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());

    /* step 5: read walFile to Get WalRecord of WAL_HEAP_INPLACE_UPDATE type */
    std::vector<WalRecordRedoInfo *> recordList;
    RetStatus retStatus = ReadRecordsAfterPlsn(plsn, WAL_HEAP_INPLACE_UPDATE, recordList);
    ASSERT_TRUE(STORAGE_FUNC_SUCC(retStatus));

    /* step 6: use WalRecord to redo for Page in Image */
    char redoPage[BLCKSZ];
    char afterPage[BLCKSZ];
    for (WalRecordRedoInfo *recordInfo: recordList) {
        WalRecordHeap *record = static_cast<WalRecordHeap *>(&(recordInfo->walRecord));
        WalDumpInfo(record);
        FetchPage(afterPage, record->m_pageId);
        RestorePage(tmpPage, record->m_pageId);
        WalRecordRedoContext redoCtx = {xid, m_walWriter->GetWalId(), DSTORE::g_defaultPdbId};
        RedoHeapRecord(&redoCtx, record, m_walStream->GetMaxAppendedPlsn());
        FetchPage(redoPage, record->m_pageId);
    }

    /* step 7: compare redoPage and afterPage */
    HeapPage* redoHeapPage = reinterpret_cast<HeapPage *>(redoPage);
    HeapPage* afterHeapPage = reinterpret_cast<HeapPage *>(afterPage);

    /* Compare page info from m_flags to potentialDelSize and recentDeadTupleMinCsn */
    uint32 cmpSize =
        OFFSETOF(HeapPage, m_heapPageHeader.fsmIndex) - OFFSETOF(HeapPage, m_header.m_flags);
    ASSERT_TRUE(CmpPages(redoPage + OFFSETOF(HeapPage, m_header.m_flags),
        afterPage + OFFSETOF(HeapPage, m_header.m_flags), cmpSize));
    ASSERT_TRUE(redoHeapPage->GetRecentDeadTupleMinCsn() == afterHeapPage->GetRecentDeadTupleMinCsn());

    /* Compare first Td and second Td */
    TD* redoTd = static_cast<TD*>(static_cast<void*>(redoPage + OFFSETOF(HeapPage, m_data)));
    TD* afterTd = static_cast<TD*>(static_cast<void*>(afterPage + OFFSETOF(HeapPage, m_data)));
    /* update operation will call CheckTupleChanged and fill csn which will refresh tdstatus to OCCUPY_TRX_END.
     * fill csn not record wal */
    ASSERT_EQ(redoTd->GetStatus(), TDStatus::OCCUPY_TRX_IN_PROGRESS);
    redoTd->SetStatus(TDStatus::OCCUPY_TRX_END);
    ASSERT_TRUE(CmpTDInfo(redoTd, afterTd));
    ASSERT_TRUE(CmpTDInfo(&redoTd[1], &afterTd[1]));

    /* Compare page form the third TD to the page end */
    cmpSize = BLCKSZ - OFFSETOF(HeapPage, m_data) - sizeof(TD) * 2;
    ASSERT_TRUE(CmpPages(redoPage + OFFSETOF(HeapPage, m_data) + sizeof(TD) * 2,
        afterPage + OFFSETOF(HeapPage, m_data) + sizeof(TD) * 2, cmpSize));
}

TEST_F(HeapWalTest, WalHeapSamePageAppendTest_level0)
{
    /* Step 1: Insert one tuple */
    HeapTuple *tuple1 = m_utTableHandler->GenerateSpecificHeapTuple("insert_data1");
    ItemPointerData ctid = m_utTableHandler->InsertHeapTupAndCheckResult(tuple1);
    g_storageInstance->GetBufferMgr()->FlushAll(false);

    /* step 2: copy dataFile as Image */
    CopyDataFile();
    char tmpPage[BLCKSZ];
    FetchPage(tmpPage, ctid.GetPageId());
    /* step 3: get endPlsn of WalFile */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);

    /* Step 4: Same-page-append Uptate */
    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    HeapTuple *tuple2 = m_utTableHandler->GenerateSpecificHeapTuple("update_data1_same_page1");
    (void)UpdateTuple(&ctid, tuple2, INVALID_SNAPSHOT, true);
    Xid xid = txn->GetCurrentXid();
    txn->Commit();
    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());

    /* step 5: read walFile to Get WalRecord of WAL_HEAP_SAME_PAGE_APPEND type */
    std::vector<WalRecordRedoInfo *> recordList;
    RetStatus retStatus = ReadRecordsAfterPlsn(plsn, WAL_HEAP_SAME_PAGE_APPEND, recordList);
    ASSERT_TRUE(STORAGE_FUNC_SUCC(retStatus));

    /* step 6: use WalRecord to redo for Page in Image */
    char redoPage[BLCKSZ];
    char afterPage[BLCKSZ];
    for (WalRecordRedoInfo *recordInfo: recordList) {
        WalRecordHeap *record = static_cast<WalRecordHeap *>(&(recordInfo->walRecord));
        WalDumpInfo(record);
        FetchPage(afterPage, record->m_pageId);
        RestorePage(tmpPage, record->m_pageId);
        WalRecordRedoContext redoCtx = {xid, m_walWriter->GetWalId(), DSTORE::g_defaultPdbId};
        RedoHeapRecord(&redoCtx, record, m_walStream->GetMaxAppendedPlsn());
        FetchPage(redoPage, record->m_pageId);
    }

    /* step 6: compare redoPage and afterPage */
    HeapPage* redoHeapPage = reinterpret_cast<HeapPage *>(redoPage);
    HeapPage* afterHeapPage = reinterpret_cast<HeapPage *>(afterPage);

    /* Compare page info from m_flags to potentialDelSize and recentDeadTupleMinCsn */
    uint32 cmpSize =
        OFFSETOF(HeapPage, m_heapPageHeader.fsmIndex) - OFFSETOF(HeapPage, m_header.m_flags);
    ASSERT_TRUE(CmpPages(redoPage + OFFSETOF(HeapPage, m_header.m_flags),
        afterPage + OFFSETOF(HeapPage, m_header.m_flags), cmpSize));
    ASSERT_TRUE(redoHeapPage->GetRecentDeadTupleMinCsn() == afterHeapPage->GetRecentDeadTupleMinCsn());

    /* Compare first Td and second Td */
    TD* redoTd = static_cast<TD*>(static_cast<void*>(redoPage + OFFSETOF(HeapPage, m_data)));
    TD* afterTd = static_cast<TD*>(static_cast<void*>(afterPage + OFFSETOF(HeapPage, m_data)));
    /* update operation will call CheckTupleChanged and fill csn which will refresh tdstatus to OCCUPY_TRX_END.
     * fill csn not record wal */
    ASSERT_EQ(redoTd->GetStatus(), TDStatus::OCCUPY_TRX_IN_PROGRESS);
    redoTd->SetStatus(TDStatus::OCCUPY_TRX_END);
    ASSERT_TRUE(CmpTDInfo(redoTd, afterTd));
    ASSERT_TRUE(CmpTDInfo(&redoTd[1], &afterTd[1]));

    /* Compare page form the third TD to the page end */
    cmpSize = BLCKSZ - OFFSETOF(HeapPage, m_data) - sizeof(TD) * 2;
    ASSERT_TRUE(CmpPages(redoPage + OFFSETOF(HeapPage, m_data) + sizeof(TD) * 2,
        afterPage + OFFSETOF(HeapPage, m_data) + sizeof(TD) * 2, cmpSize));
}

TEST_F(HeapWalTest, WalHeapAnotherPageAppendOldTest_level0)
{
    /* Step 1: Insert seven tuple to fill the page they will use tdid(0,1,2,3,0,1,2)*/
    constexpr int TUPLE_DATA_LEN = 1024;
    std::string str(TUPLE_DATA_LEN, 'a');
    constexpr int TUPLE_NUM_PER_PAGE = 7;
    ItemPointerData ctid;
    for (int i = 0; i < TUPLE_NUM_PER_PAGE; ++i) {
        ctid = InsertSpecificHeapTuple(str);
    }
    g_storageInstance->GetBufferMgr()->FlushAll(false);

    /* step 2: copy dataFile as Image */
    CopyDataFile();
    char tmpPage[BLCKSZ];
    FetchPage(tmpPage, ctid.GetPageId());
    /* step 3: get endPlsn of WalFile */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);

    /* Step 4: Another Page Uptate will use tdid(3) */
    Transaction *transaction = thrd->GetActiveTransaction();
    transaction->Start();
    str.append("helloWorld");
    HeapTuple *newTuple = m_utTableHandler->GenerateSpecificHeapTuple(str);
    (void)UpdateTuple(&ctid, newTuple, INVALID_SNAPSHOT, true);
    Xid xid = transaction->GetCurrentXid();
    transaction->Commit();
    DstorePfreeExt(newTuple);
    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());

    /* step 5: read walFile to Get WalRecord */
    std::vector<WalRecordRedoInfo *> allocTdRecordList;
    RetStatus retStatus = ReadRecordsAfterPlsn(plsn, WAL_HEAP_ALLOC_TD, allocTdRecordList);
    ASSERT_TRUE(STORAGE_FUNC_SUCC(retStatus));
    std::vector<WalRecordRedoInfo *> updateOldrecordList;
    retStatus = ReadRecordsAfterPlsn(plsn, WAL_HEAP_ANOTHER_PAGE_APPEND_UPDATE_OLD_PAGE, updateOldrecordList);
    ASSERT_TRUE(STORAGE_FUNC_SUCC(retStatus));

    /* step 6: use WalRecord to redo for Page in Image */
    WalRecordHeap *record;
    char redoPage[BLCKSZ];
    char afterPage[BLCKSZ];
    record = static_cast<WalRecordHeap *>(&(allocTdRecordList[0]->walRecord));
    WalDumpInfo(record);
    FetchPage(afterPage, record->m_pageId);
    RestorePage(tmpPage, record->m_pageId);
    WalRecordRedoContext redoCtx = {xid, m_walWriter->GetWalId(), DSTORE::g_defaultPdbId};
    RedoHeapRecord(&redoCtx, record, m_walStream->GetMaxAppendedPlsn() - 1);
    record = static_cast<WalRecordHeap *>(&(updateOldrecordList[0]->walRecord));
    RedoHeapRecord(&redoCtx, record, m_walStream->GetMaxAppendedPlsn());
    FetchPage(redoPage, record->m_pageId);

    /* step 7: compare redoPage and afterPage */
    /* Compare some page head info */
    HeapPage* redoHeapPage = reinterpret_cast<HeapPage *>(redoPage);
    HeapPage* afterHeapPage = reinterpret_cast<HeapPage *>(afterPage);

    /* Compare page info from m_flags to potentialDelSize and recentDeadTupleMinCsn */
    uint32 cmpSize =
        OFFSETOF(HeapPage, m_heapPageHeader.fsmIndex) - OFFSETOF(HeapPage, m_header.m_flags);
    ASSERT_TRUE(CmpPages(redoPage + OFFSETOF(HeapPage, m_header.m_flags),
        afterPage + OFFSETOF(HeapPage, m_header.m_flags), cmpSize));
    ASSERT_TRUE(redoHeapPage->GetRecentDeadTupleMinCsn() == afterHeapPage->GetRecentDeadTupleMinCsn());

    /* Compare all the Td */
    ASSERT_TRUE(CmpPages(redoPage + OFFSETOF(HeapPage, m_data),
        afterPage + OFFSETOF(HeapPage, m_data), sizeof(TD) * 2));
    TD* redoTd = static_cast<TD*>(static_cast<void*>(redoPage + OFFSETOF(HeapPage, m_data)));
    TD* afterTd = static_cast<TD*>(static_cast<void*>(afterPage + OFFSETOF(HeapPage, m_data)));
    ASSERT_TRUE(CmpTDInfo(&redoTd[2], &afterTd[2]));
    ASSERT_TRUE(CmpTDInfo(&redoTd[3], &afterTd[3]));

    /* Compare from the first ItemId to the page end. */
    cmpSize = BLCKSZ - OFFSETOF(HeapPage, m_data) - sizeof(TD) * DSTORE::DEFAULT_TD_COUNT;
    ASSERT_TRUE(CmpPages(redoHeapPage->m_data + sizeof(TD) * DSTORE::DEFAULT_TD_COUNT,
        afterHeapPage->m_data + sizeof(TD) * DSTORE::DEFAULT_TD_COUNT, cmpSize));
}

TEST_F(HeapWalTest, WalHeapAnotherPageAppendNewTest_level0)
{
    /* Step 1: Insert seven tuple to fill the page */
    constexpr int TUPLE_DATA_LEN = 1024;
    std::string str(TUPLE_DATA_LEN, 'a');
    constexpr int TUPLE_NUM_PER_PAGE = 7;
    ItemPointerData ctid;
    for (int i = 0; i < TUPLE_NUM_PER_PAGE; ++i) {
        ctid = InsertSpecificHeapTuple(str);
    }
    g_storageInstance->GetBufferMgr()->FlushAll(false);

    /* step 2: copy dataFile as Image */
    CopyDataFile();
    /* step 3: get endPlsn of WalFile */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);

    /* Step 4: Another Page Uptate */
    Transaction *transaction = thrd->GetActiveTransaction();
    transaction->Start();
    str.append("helloWorld");
    HeapTuple *newTuple = m_utTableHandler->GenerateSpecificHeapTuple(str);
    (void)UpdateTuple(&ctid, newTuple, INVALID_SNAPSHOT, true);
    Xid xid = transaction->GetCurrentXid();
    transaction->Commit();
    DstorePfreeExt(newTuple);
    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());

    /* step 5: read walFile to Get WalRecord of WAL_HEAP_ANOTHER_PAGE_APPEND_UPDATE_NEW_PAGE type */
    std::vector<WalRecordRedoInfo *> recordList;
    RetStatus retStatus = ReadRecordsAfterPlsn(plsn, WAL_HEAP_ANOTHER_PAGE_APPEND_UPDATE_NEW_PAGE, recordList);
    ASSERT_TRUE(STORAGE_FUNC_SUCC(retStatus));

    /* step 6: use WalRecord to redo for Page in Image */
    char redoPage[BLCKSZ];
    char afterPage[BLCKSZ];
    for (WalRecordRedoInfo *recordInfo: recordList) {
        WalRecordHeap *record =static_cast<WalRecordHeap *>(&(recordInfo->walRecord));
        WalDumpInfo(record);
        FetchPage(afterPage, record->m_pageId);
        InitPage(record->m_pageId);
        WalRecordRedoContext redoCtx = {xid, m_walWriter->GetWalId(), DSTORE::g_defaultPdbId};
        RedoHeapRecord(&redoCtx, record, m_walStream->GetMaxAppendedPlsn());
        FetchPage(redoPage, record->m_pageId);
    }

    /* step 7: compare redoPage and afterPage */
    HeapPage* redoHeapPage = reinterpret_cast<HeapPage *>(redoPage);
    HeapPage* afterHeapPage = reinterpret_cast<HeapPage *>(afterPage);

    /* Compare page info from m_flags to potentialDelSize and recentDeadTupleMinCsn */
    uint32 cmpSize =
        OFFSETOF(HeapPage, m_heapPageHeader.fsmIndex) - OFFSETOF(HeapPage, m_header.m_flags);
    ASSERT_TRUE(CmpPages(redoPage + OFFSETOF(HeapPage, m_header.m_flags),
        afterPage + OFFSETOF(HeapPage, m_header.m_flags), cmpSize));
    ASSERT_TRUE(redoHeapPage->GetRecentDeadTupleMinCsn() == afterHeapPage->GetRecentDeadTupleMinCsn());

    /* Compare first Td */
    ASSERT_TRUE(CmpTDInfo(static_cast<TD*>(static_cast<void*>(redoPage + OFFSETOF(HeapPage, m_data))),
        static_cast<TD*>(static_cast<void*>(afterPage + OFFSETOF(HeapPage, m_data)))));

    /* Compare page form the second TD to the page end */
    cmpSize = BLCKSZ - OFFSETOF(HeapPage, m_data) - sizeof(TD);
    ASSERT_TRUE(CmpPages(redoPage + OFFSETOF(HeapPage, m_data) + sizeof(TD),
        afterPage + OFFSETOF(HeapPage, m_data) + sizeof(TD), cmpSize));
}

TEST_F(HeapWalTest, WalHeapForceUpdateTupleDataNoTrxTest_level0)
{
    /* Step 1: Insert one tuple */
    HeapTuple *tuple1 = m_utTableHandler->GenerateSpecificHeapTuple("insert_data1");
    ItemPointerData ctid = m_utTableHandler->InsertHeapTupAndCheckResult(tuple1);
    g_storageInstance->GetBufferMgr()->FlushAll(false);

    /* step 2: copy dataFile as Image */
    CopyDataFile();
    char tmpPage[BLCKSZ];
    FetchPage(tmpPage, ctid.GetPageId());
    /* step 3: get endPlsn of WalFile */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);

    /* Step 4: Force Uptate */
    HeapTuple *tuple2 = m_utTableHandler->GenerateSpecificHeapTuple("update_data1");
    ForceUpdateTupleNoTrx(&ctid, tuple2);
    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());

    /* step 5: read walFile to Get WalRecord of WAL_HEAP_FORCE_UPDATE_TUPLE_DATA_NO_TRX type */
    std::vector<WalRecordRedoInfo *> recordList;
    RetStatus retStatus = ReadRecordsAfterPlsn(plsn, WAL_HEAP_FORCE_UPDATE_TUPLE_DATA_NO_TRX, recordList);
    ASSERT_TRUE(STORAGE_FUNC_SUCC(retStatus));

    /* step 6: use WalRecord to redo for Page in Image */
    char redoPage[BLCKSZ];
    char afterPage[BLCKSZ];
    for (WalRecordRedoInfo *recordInfo: recordList) {
        WalRecordHeap *record = static_cast<WalRecordHeap *>(&(recordInfo->walRecord));
        WalDumpInfo(record);
        FetchPage(afterPage, record->m_pageId);
        RestorePage(tmpPage, record->m_pageId);
        WalRecordRedoContext redoCtx = {INVALID_XID, m_walWriter->GetWalId(), DSTORE::g_defaultPdbId};;
        RedoHeapRecord(&redoCtx, record, m_walStream->GetMaxAppendedPlsn());
        FetchPage(redoPage, record->m_pageId);
    }

    /* step 7: compare redoPage and afterPage */
    HeapPage* redoHeapPage = reinterpret_cast<HeapPage *>(redoPage);
    HeapPage* afterHeapPage = reinterpret_cast<HeapPage *>(afterPage);

    /* Compare page info from m_flags to potentialDelSize and recentDeadTupleMinCsn */
    uint32 cmpSize =
        OFFSETOF(HeapPage, m_heapPageHeader.fsmIndex) - OFFSETOF(HeapPage, m_header.m_flags);
    ASSERT_TRUE(CmpPages(redoPage + OFFSETOF(HeapPage, m_header.m_flags),
        afterPage + OFFSETOF(HeapPage, m_header.m_flags), cmpSize));
    ASSERT_TRUE(redoHeapPage->GetRecentDeadTupleMinCsn() == afterHeapPage->GetRecentDeadTupleMinCsn());

    /* Compare first Td and second Td */
    TD* redoTd = static_cast<TD*>(static_cast<void*>(redoPage + OFFSETOF(HeapPage, m_data)));
    TD* afterTd = static_cast<TD*>(static_cast<void*>(afterPage + OFFSETOF(HeapPage, m_data)));
    ASSERT_EQ(redoTd->GetStatus(), TDStatus::OCCUPY_TRX_IN_PROGRESS);
    ASSERT_TRUE(CmpTDInfo(redoTd, afterTd));
    ASSERT_TRUE(CmpTDInfo(&redoTd[1], &afterTd[1]));

    /* Compare page form the third TD to the page end */
    cmpSize = BLCKSZ - OFFSETOF(HeapPage, m_data) - sizeof(TD) * 2;
    ASSERT_TRUE(CmpPages(redoPage + OFFSETOF(HeapPage, m_data) + sizeof(TD) * 2,
        afterPage + OFFSETOF(HeapPage, m_data) + sizeof(TD) * 2, cmpSize));
}

TEST_F(HeapWalTest, WalHeapPruneTest_level0)
{
    /* Step 1: Insert seven tuple fill current page*/
    constexpr int TUPLE_DATA_LEN = 1024;
    std::string str(TUPLE_DATA_LEN, 'a');
    constexpr int TUPLE_NUM_PER_PAGE = 7;
    ItemPointerData ctid;
    for (int i = 0; i < TUPLE_NUM_PER_PAGE; ++i) {
        ctid = InsertSpecificHeapTuple(str);
    }

    /* delete the first tuple  and set the td status as DETACH_TD */
    ASSERT_TRUE(STORAGE_FUNC_SUCC(DeleteTuple(&ctid)));
    BufferDesc *bufferDesc = g_storageInstance->GetBufferMgr()->Read(g_defaultPdbId,
        ctid.GetPageId(), LW_EXCLUSIVE);
    HeapPage *page = static_cast<HeapPage *>(bufferDesc->GetPage());
    HeapTuple oldTuple;
    page->GetTuple(&oldTuple, ctid.GetOffset());
    oldTuple.GetDiskTuple()->SetTdStatus(DSTORE::DETACH_TD);
    (void)g_storageInstance->GetBufferMgr()->MarkDirty(bufferDesc);
    g_storageInstance->GetBufferMgr()->UnlockAndRelease(bufferDesc);
    g_storageInstance->GetBufferMgr()->FlushAll(false);

    /* step 2: copy dataFile as Image */
    CopyDataFile();
    char tmpPage[BLCKSZ];
    FetchPage(tmpPage, ctid.GetPageId());
    /* step 3: get endPlsn of WalFile */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);

    /* Step4: Heap Prune. */
    bufferDesc = g_storageInstance->GetBufferMgr()->Read(g_defaultPdbId,
        ctid.GetPageId(), LW_EXCLUSIVE);
    std::unique_ptr<StorageRelationData> heapRel(new StorageRelationData());
    heapRel->tableSmgr = m_utTableHandler->GetHeapTabSmgr();
    heapRel->m_pdbId = g_defaultPdbId;
    HeapPruneHandler prune(g_storageInstance, heapRel.get(), thrd, bufferDesc);
    prune.TryPrunePage(static_cast<uint16>(1000));
    g_storageInstance->GetBufferMgr()->UnlockAndRelease(bufferDesc);
    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());

    /* step 5: read walFile to Get WalRecord of WAL_HEAP_PRUNE type */
    std::vector<WalRecordRedoInfo *> recordList;
    RetStatus retStatus = ReadRecordsAfterPlsn(plsn, WAL_HEAP_PRUNE, recordList);
    ASSERT_TRUE(STORAGE_FUNC_SUCC(retStatus));

    /* step 5: use WalRecord to redo for Page in Image */
    char redoPage[BLCKSZ];
    char afterPage[BLCKSZ];
    for (WalRecordRedoInfo *recordInfo: recordList) {
        WalRecordHeap *record = static_cast<WalRecordHeap *>(&(recordInfo->walRecord));
        WalDumpInfo(record);
        FetchPage(afterPage, record->m_pageId);
        RestorePage(tmpPage, record->m_pageId);
        WalRecordRedoContext redoCtx = {INVALID_XID, m_walWriter->GetWalId(), DSTORE::g_defaultPdbId};
        RedoHeapRecord(&redoCtx, record, m_walStream->GetMaxAppendedPlsn());
        FetchPage(redoPage, record->m_pageId);
    }

    /* step 6: compare redoPage and afterPage */
    HeapPage* redoHeapPage = reinterpret_cast<HeapPage *>(redoPage);
    HeapPage* afterHeapPage = reinterpret_cast<HeapPage *>(afterPage);

    /* Compare page info from m_flags to potentialDelSize and recentDeadTupleMinCsn */
    uint32 cmpSize =
        OFFSETOF(HeapPage, m_heapPageHeader.fsmIndex) - OFFSETOF(HeapPage, m_header.m_flags);
    ASSERT_TRUE(CmpPages(redoPage + OFFSETOF(HeapPage, m_header.m_flags),
        afterPage + OFFSETOF(HeapPage, m_header.m_flags), cmpSize));
    ASSERT_TRUE(redoHeapPage->GetRecentDeadTupleMinCsn() == afterHeapPage->GetRecentDeadTupleMinCsn());

    /* Compare page form the first TD to the page end */
    cmpSize = BLCKSZ - OFFSETOF(HeapPage, m_data);
    ASSERT_TRUE(CmpPages(redoPage + OFFSETOF(HeapPage, m_data), afterPage + OFFSETOF(HeapPage, m_data), cmpSize));
}

TEST_F(HeapWalTest, WalHeapInsertWithAllocTdTest_level0)
{
    /* Step 1: Insert four tuple */
    const int insertTupleNum = 4;
    ItemPointerData ctid;
    for (int i = 0; i < insertTupleNum; ++i) {
        ctid = InsertSpecificHeapTuple("tmp");
    }
    g_storageInstance->GetBufferMgr()->FlushAll(false);

    /* step 2: copy dataFile as Image */
    CopyDataFile();
    char tmpPage[BLCKSZ];
    FetchPage(tmpPage, ctid.GetPageId());
    /* step 3: get endPlsn of WalFile */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);

    /* step 4: insert tuple with alloctd */
    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    (void)InsertSpecificHeapTuple("newtuple", INVALID_SNAPSHOT, true);
    Xid xid = txn->GetCurrentXid();
    txn->Commit();
    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());

    /* step 5: read walFile to Get WalRecord of WAL_HEAP_INSERT and AllocTd */
    std::vector<WalRecordRedoInfo *> recordList;
    RetStatus retStatus = ReadRecordsAfterPlsn(plsn, WAL_HEAP_INSERT, recordList);
    ASSERT_TRUE(STORAGE_FUNC_SUCC(retStatus));

    /* step 6: use WalRecord to redo for Page in Image */
    char redoPage[BLCKSZ];
    char afterPage[BLCKSZ];
    BufferDesc *bufferDesc = nullptr;
    for (WalRecordRedoInfo *recordInfo: recordList) {
        WalRecordHeap *record = static_cast<WalRecordHeap *>(&(recordInfo->walRecord));
        WalDumpInfo(record);
        FetchPage(afterPage, record->m_pageId);
        RestorePage(tmpPage, record->m_pageId);
        WalRecordRedoContext redoCtx = {xid, m_walWriter->GetWalId(), DSTORE::g_defaultPdbId};
        RedoHeapRecord(&redoCtx, record, m_walStream->GetMaxAppendedPlsn());
        FetchPage(redoPage, record->m_pageId);
    }

    /* step 6: compare redoPage and afterPage */
    HeapPage* redoHeapPage = reinterpret_cast<HeapPage *>(redoPage);
    HeapPage* afterHeapPage = reinterpret_cast<HeapPage *>(afterPage);

    /* Compare page info from m_flags to potentialDelSize and recentDeadTupleMinCsn */
    uint32 cmpSize =
        OFFSETOF(HeapPage, m_heapPageHeader.fsmIndex) - OFFSETOF(HeapPage, m_header.m_flags);
    ASSERT_TRUE(CmpPages(redoPage + OFFSETOF(HeapPage, m_header.m_flags),
        afterPage + OFFSETOF(HeapPage, m_header.m_flags), cmpSize));
    ASSERT_TRUE(redoHeapPage->GetRecentDeadTupleMinCsn() == afterHeapPage->GetRecentDeadTupleMinCsn());

    /* Compare all the Td */
    TD* redoTd = static_cast<TD*>(static_cast<void*>(redoPage + OFFSETOF(HeapPage, m_data)));
    TD* afterTd = static_cast<TD*>(static_cast<void*>(afterPage + OFFSETOF(HeapPage, m_data)));
    for (int i = 0; i < DEFAULT_TD_COUNT; i++) {
        ASSERT_TRUE(CmpTDInfo(&redoTd[i], &afterTd[i]));
    }

    /* Compare ItemId info to the page */
    cmpSize = BLCKSZ - OFFSETOF(HeapPage, m_data) - 4 * sizeof(TD);
    ASSERT_TRUE(CmpPages(redoPage + OFFSETOF(HeapPage, m_data) + 4 * sizeof(TD),
        afterPage + OFFSETOF(HeapPage, m_data) + 4 * sizeof(TD), cmpSize));
}

TEST_F(HeapWalTest, WalHeapAllocReuseTdTest_level0)
{
    /* Step 1: Insert two tuple */
    const int insertTupleNum = 4;
    ItemPointerData ctid;
    for (int i = 0; i < insertTupleNum; ++i) {
        ctid = InsertSpecificHeapTuple("tmp");
    }
    g_storageInstance->GetBufferMgr()->FlushAll(false);

    /* step 2: copy dataFile as Image */
    CopyDataFile();
    char tmpPage[BLCKSZ];
    FetchPage(tmpPage, ctid.GetPageId());
    /* step 3: get endPlsn of WalFile */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);

    /* step 4: do lock tuple */
    Transaction *transaction = thrd->GetActiveTransaction();
    transaction->Start();
    transaction->SetSnapshotCsn();
    int ret = LockUnchangedTuple(ctid, transaction->GetSnapshotData(), true);
    ASSERT_EQ(ret, DSTORE_SUCC);
    transaction->Commit();
    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());

    /* step 5: read walFile to Get WalRecord of WAL_HEAP_INSERT and AllocTd */
    std::vector<WalRecordRedoInfo *> recordList;
    RetStatus retStatus = ReadRecordsAfterPlsn(plsn, WAL_HEAP_ALLOC_TD, recordList);
    ASSERT_TRUE(STORAGE_FUNC_SUCC(retStatus));
    WalRecordHeap *allocTdRecord = static_cast<WalRecordHeap *>(&(recordList[0]->walRecord));
    char *wal = ((WalRecordHeapAllocTd *)allocTdRecord)->rawData;
    char *data = wal + sizeof(uint8);
    TrxSlotStatus *status = static_cast<TrxSlotStatus*>(static_cast<void *>(data));
    ASSERT_EQ(static_cast<int>(status[0]), TXN_STATUS_COMMITTED);
    ASSERT_EQ(static_cast<int>(status[1]), TXN_STATUS_COMMITTED);
    ASSERT_EQ(static_cast<int>(status[2]), TXN_STATUS_COMMITTED);
    ASSERT_EQ(static_cast<int>(status[3]), TXN_STATUS_COMMITTED);

    /* step 6: use WalRecord to redo for Page in Image */
    char redoPage[BLCKSZ];
    char afterPage[BLCKSZ];
    BufferDesc *bufferDesc = nullptr;
    for (WalRecordRedoInfo *recordInfo: recordList) {
        WalRecordHeap *record = static_cast<WalRecordHeap *>(&(recordInfo->walRecord));
        WalDumpInfo(record);
        FetchPage(afterPage, record->m_pageId);
        RestorePage(tmpPage, record->m_pageId);
        WalRecordRedoContext redoCtx = {INVALID_XID, m_walWriter->GetWalId(), DSTORE::g_defaultPdbId};
        RedoHeapRecord(&redoCtx, record, m_walStream->GetMaxAppendedPlsn());
        FetchPage(redoPage, record->m_pageId);
    }

    /* step 6: compare redoPage and afterPage */
    HeapPage* redoHeapPage = reinterpret_cast<HeapPage *>(redoPage);
    HeapPage* afterHeapPage = reinterpret_cast<HeapPage *>(afterPage);

    /* Compare page info from m_flags to potentialDelSize and recentDeadTupleMinCsn */
    uint32 cmpSize =
        OFFSETOF(HeapPage, m_heapPageHeader.fsmIndex) - OFFSETOF(HeapPage, m_header.m_flags);
    ASSERT_TRUE(CmpPages(redoPage + OFFSETOF(HeapPage, m_header.m_flags),
        afterPage + OFFSETOF(HeapPage, m_header.m_flags), cmpSize));
}

TEST_F(HeapWalTest, WalHeapAllocTdTest_level0)
{
    /* Step 1: Insert four tuple */
    const int insertTupleNum = 4;
    ItemPointerData ctid;
    for (int i = 0; i < insertTupleNum; ++i) {
        ctid = InsertSpecificHeapTuple("tmp");
    }
    g_storageInstance->GetBufferMgr()->FlushAll(false);

    /* step 2: copy dataFile as Image */
    CopyDataFile();
    char tmpPage[BLCKSZ];
    FetchPage(tmpPage, ctid.GetPageId());
    /* step 3: get endPlsn of WalFile */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);

    /* step 4: do lock tuple */
    Transaction *transaction = thrd->GetActiveTransaction();
    transaction->Start();
    transaction->SetSnapshotCsn();
    int ret = LockUnchangedTuple(ctid, transaction->GetSnapshotData(), true);
    ASSERT_EQ(ret, DSTORE_SUCC);
    transaction->Commit();
    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());

    /* step 5: read walFile to Get WalRecord of WAL_HEAP_INSERT and AllocTd */
    std::vector<WalRecordRedoInfo *> recordList;
    RetStatus retStatus = ReadRecordsAfterPlsn(plsn, WAL_HEAP_ALLOC_TD, recordList);
    ASSERT_TRUE(STORAGE_FUNC_SUCC(retStatus));

    /* step 6: use WalRecord to redo for Page in Image */
    char redoPage[BLCKSZ];
    char afterPage[BLCKSZ];
    BufferDesc *bufferDesc = nullptr;
    for (WalRecordRedoInfo *recordInfo: recordList) {
        WalRecordHeap *record = static_cast<WalRecordHeap *>(&(recordInfo->walRecord));
        WalDumpInfo(record);
        FetchPage(afterPage, record->m_pageId);
        RestorePage(tmpPage, record->m_pageId);
        WalRecordRedoContext redoCtx = {INVALID_XID, m_walWriter->GetWalId(), DSTORE::g_defaultPdbId};
        RedoHeapRecord(&redoCtx, record, m_walStream->GetMaxAppendedPlsn());
        FetchPage(redoPage, record->m_pageId);
    }

    /* step 6: compare redoPage and afterPage */
    HeapPage* redoHeapPage = reinterpret_cast<HeapPage *>(redoPage);
    HeapPage* afterHeapPage = reinterpret_cast<HeapPage *>(afterPage);

    /* Compare page info from m_flags to potentialDelSize and recentDeadTupleMinCsn */
    uint32 cmpSize =
        OFFSETOF(HeapPage, m_heapPageHeader.fsmIndex) - OFFSETOF(HeapPage, m_header.m_flags);
    ASSERT_TRUE(CmpPages(redoPage + OFFSETOF(HeapPage, m_header.m_flags),
        afterPage + OFFSETOF(HeapPage, m_header.m_flags), cmpSize));
    ASSERT_TRUE(redoHeapPage->GetRecentDeadTupleMinCsn() == afterHeapPage->GetRecentDeadTupleMinCsn());

    /* Compare all the Td */
    TD* redoTd = static_cast<TD*>(static_cast<void*>(redoPage + OFFSETOF(HeapPage, m_data)));
    TD* afterTd = static_cast<TD*>(static_cast<void*>(afterPage + OFFSETOF(HeapPage, m_data)));
    ASSERT_EQ(afterTd->GetStatus(), TDStatus::OCCUPY_TRX_IN_PROGRESS);
    afterTd->SetStatus(TDStatus::OCCUPY_TRX_END);
    for (int i = 0; i < DEFAULT_TD_COUNT; i++) {
        ASSERT_TRUE(CmpTDInfo(&redoTd[i], &afterTd[i]));
    }

    /* Compare ItemId info to the page */
    HeapDiskTuple* afterTuple = afterHeapPage->GetDiskTuple(ctid.GetOffset());
    HeapDiskTuple* redoTuple = redoHeapPage->GetDiskTuple(ctid.GetOffset());
    ASSERT_EQ(afterTuple->GetLockerTdId(), 0);
    ASSERT_EQ(redoTuple->GetLockerTdId(), INVALID_TD_SLOT);
    afterTuple->SetLockerTdId(INVALID_TD_SLOT);
    cmpSize = BLCKSZ - OFFSETOF(HeapPage, m_data) - 4 * sizeof(TD);
    ASSERT_TRUE(CmpPages(redoPage + OFFSETOF(HeapPage, m_data) + 4 * sizeof(TD),
        afterPage + OFFSETOF(HeapPage, m_data) + 4 * sizeof(TD), cmpSize));
}

TEST_F(HeapWalTest, WalHeapPageRollbackTest_level0)
{
    /* step 1: insert one tuple */
    Transaction *transaction = thrd->GetActiveTransaction();
    transaction->Start();
    transaction->SetSnapshotCsn();
    ItemPointerData ctid = InsertSpecificHeapTuple(std::string(100, 'a'), transaction->GetSnapshotData(), true);
    Xid xid = transaction->GetCurrentXid();
    transaction->Commit();
    g_storageInstance->GetBufferMgr()->FlushAll(false);

    /* step 2: copy dataFile as Image */
    CopyDataFile();
    char tmpPage[BLCKSZ];
    FetchPage(tmpPage, ctid.GetPageId());
    /* step 3: get endPlsn of WalFile */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);

    /* Step 4: do page rollback and generate page rollback wal */
    transaction->Start();
    transaction->SetSnapshotCsn();
    BufferDesc *bufferDesc = g_storageInstance->GetBufferMgr()->Read(g_defaultPdbId,
        ctid.GetPageId(), LW_EXCLUSIVE);
    HeapPage *page = static_cast<HeapPage *>(bufferDesc->GetPage());
    RetStatus ret = page->RollbackByXid(g_defaultPdbId, xid, g_storageInstance->GetBufferMgr(), bufferDesc);
    ASSERT_TRUE(STORAGE_FUNC_SUCC(ret));
    std::unique_ptr<StorageRelationData> heapRel(new StorageRelationData());
    heapRel->tableSmgr = m_utTableHandler->GetHeapTabSmgr();
    heapRel->m_pdbId = g_defaultPdbId;
    g_storageInstance->GetBufferMgr()->UnlockAndRelease(bufferDesc);
    transaction->Commit();

    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());

    /* step 5: read walFile to Get WalRecord of WAL_UNDO_HEAP_PAGE_ROLL_BACK */
    std::vector<WalRecordRedoInfo *> heapPageRollbackLisk;
    RetStatus retStatus = ReadRecordsAfterPlsn(plsn, WAL_UNDO_HEAP_PAGE_ROLL_BACK, heapPageRollbackLisk);
    ASSERT_TRUE(heapPageRollbackLisk.size() == 1);
    ASSERT_TRUE(STORAGE_FUNC_SUCC(retStatus));

    /* step 6: use WalRecord to redo for Page in Image */
    char redoPage[BLCKSZ];
    char afterPage[BLCKSZ];
    WalRecordUndo *record = static_cast<WalRecordUndo *>(&(heapPageRollbackLisk[0]->walRecord));
    FILE *fp = fopen("waldump.txt", "w+");
    WalRecordUndo::DumpUndoRecord(record, fp);
    fclose(fp);
    FetchPage(afterPage, record->m_pageId);
    RestorePage(tmpPage, record->m_pageId);
    WalRecordRedoContext redoCtx = {xid, 0, g_defaultPdbId, m_walStream->GetMaxAppendedPlsn()};
    bufferDesc = g_storageInstance->GetBufferMgr()->Read(g_defaultPdbId,
            ctid.GetPageId(), LW_EXCLUSIVE);
    WalRecordUndo::RedoUndoRecord(&redoCtx, record, bufferDesc);
    (void)g_storageInstance->GetBufferMgr()->MarkDirty(bufferDesc);
    g_storageInstance->GetBufferMgr()->UnlockAndRelease(bufferDesc);
    FetchPage(redoPage, record->m_pageId);

    /* step 7: compare redoPage and afterPage */
    HeapPage* redoHeapPage = reinterpret_cast<HeapPage *>(redoPage);
    HeapPage* afterHeapPage = reinterpret_cast<HeapPage *>(afterPage);
    ASSERT_TRUE(CmpPages(redoPage, afterPage, BLCKSZ));
}

TEST_F(HeapWalTest, WalHeapAllocTdLockerXidTest_level0)
{
    /* step 1: insert four tuple, and lock first tuple, occupy four TD */
    constexpr int TUPLE_NUM = 4;
    ItemPointerData ctid[TUPLE_NUM];
    Xid xids[TUPLE_NUM];
    Transaction *transaction = thrd->GetActiveTransaction();
    std::unique_ptr<StorageRelationData> heapRel(new StorageRelationData());
    heapRel->tableSmgr = m_utTableHandler->GetHeapTabSmgr();
    heapRel->m_pdbId = g_defaultPdbId;
    for (int i = 0; i < TUPLE_NUM; ++i) {
        transaction->Start();
        transaction->SetSnapshotCsn();
        ctid[i] = InsertSpecificHeapTuple(std::string(100, 'a'), transaction->GetSnapshotData(), true);
        if (i == 0) {
            HeapLockTupleContext lockTupContext;
            lockTupContext.ctid = ctid[0];
            lockTupContext.needRetTup = true;
            lockTupContext.snapshot = *(transaction->GetSnapshotData());
            HeapLockTupHandler lockTuple(g_storageInstance, thrd, heapRel.get());
            lockTuple.LockUnchangedTuple(&lockTupContext);
        }
        xids[i] = transaction->GetCurrentXid();
        transaction->Commit();
    }
    g_storageInstance->GetBufferMgr()->FlushAll(false);

    /* step 2: copy dataFile as Image */
    CopyDataFile();
    char tmpPage[BLCKSZ];
    FetchPage(tmpPage, ctid[0].GetPageId());
    /* step 3: get endPlsn of WalFile */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);

    /* Step 4: allocTd and generate allocTd wal */
    heapRel->tableSmgr = m_utTableHandler->GetHeapTabSmgr();
    heapRel->m_pdbId = g_defaultPdbId;
    HeapHandler heapHandler(g_storageInstance, thrd, heapRel.get());
    transaction->Start();
    transaction->SetSnapshotCsn();
    transaction->AllocTransactionSlot();
    BufferDesc *bufferDesc = g_storageInstance->GetBufferMgr()->Read(g_defaultPdbId,
        ctid[0].GetPageId(), LW_EXCLUSIVE);
    HeapPage *page = static_cast<HeapPage *>(bufferDesc->GetPage());
    heapHandler.m_tdcontext.Begin(g_defaultPdbId, g_storageInstance->GetCsnMgr()->GetRecycleCsnMin(INVALID_PDB_ID));
    TdId tdId = page->AllocTd(heapHandler.m_tdcontext);
    ASSERT_TRUE(tdId == 0);
    (void)g_storageInstance->GetBufferMgr()->MarkDirty(bufferDesc);
    heapHandler.GenerateAllocTdWal(bufferDesc);
    g_storageInstance->GetBufferMgr()->UnlockAndRelease(bufferDesc);
    Xid xid = transaction->GetCurrentXid();
    transaction->Commit();
    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());

    /* step 5: read walFile to Get WalRecord of WAL_HEAP_ALLOC_TD */
    std::vector<WalRecordRedoInfo *> allocTdLisk;
    RetStatus retStatus = ReadRecordsAfterPlsn(plsn, WAL_HEAP_ALLOC_TD, allocTdLisk);
    ASSERT_TRUE(allocTdLisk.size() == 1);
    ASSERT_TRUE(STORAGE_FUNC_SUCC(retStatus));

    /* step 6: use WalRecord to redo for Page in Image */
    char redoPage[BLCKSZ];
    char afterPage[BLCKSZ];
    WalRecordHeap *record = static_cast<WalRecordHeap *>(&(allocTdLisk[0]->walRecord));
    FetchPage(afterPage, record->m_pageId);
    RestorePage(tmpPage, record->m_pageId);
    WalRecordRedoContext redoCtx = {xid, m_walWriter->GetWalId(), DSTORE::g_defaultPdbId};
    RedoHeapRecord(&redoCtx, record, m_walStream->GetMaxAppendedPlsn());
    FetchPage(redoPage, record->m_pageId);

    /* step 7: compare redoPage and afterPage */
    ASSERT_TRUE(CmpPages(redoPage, afterPage, OFFSETOF(HeapPage, m_data), true));
    TD* redoTd = static_cast<TD*>(static_cast<void*>(redoPage + OFFSETOF(HeapPage, m_data)));
    TD* afterTd = static_cast<TD*>(static_cast<void*>(afterPage + OFFSETOF(HeapPage, m_data)));
    /* redo page won't clear lockerXid */
    ASSERT_TRUE(redoTd[0].GetLockerXid() == xids[0]);
    ASSERT_TRUE(afterTd[0].GetLockerXid() == INVALID_XID);
    redoTd[0] = afterTd[0];
    for (int i = 0; i < DEFAULT_TD_COUNT; i++) {
        ASSERT_TRUE(CmpTDInfo(&redoTd[i], &afterTd[i]));
    }
    uint32 cmpSize = BLCKSZ - OFFSETOF(HeapPage, m_data) - 4 * sizeof(TD);
    ASSERT_TRUE(CmpPages(redoPage + OFFSETOF(HeapPage, m_data) + 4 * sizeof(TD),
        afterPage + OFFSETOF(HeapPage, m_data) + 4 * sizeof(TD), cmpSize));
}

/* this case to keep that page is same in primary and standby node when heap insert extend undo space fail */
TEST_F(HeapWalTest, WalHeapInsertExtendUndoSpaceFailTest_level0)
{
    /* Step 1: Insert four tuple to occupy all TD */
    const int insertTupleNum = 4;
    ItemPointerData ctid;
    for (int i = 0; i < insertTupleNum; ++i) {
        ctid = InsertSpecificHeapTuple("tmp");
    }
    g_storageInstance->GetBufferMgr()->FlushAll(false);

    /* step 2: copy dataFile as Image */
    CopyDataFile();
    char tmpPage[BLCKSZ];
    FetchPage(tmpPage, ctid.GetPageId());
    /* step 3: get endPlsn of WalFile */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);

    /* Step 4: Insert one tuple and extend undo space fail, will only record alloctd wal */
    FAULT_INJECTION_ACTIVE_MODE_LEVEL(DstoreTransactionFI::EXTEND_UNDO_SPACE_FAIL, 0, FI_GLOBAL, 0, 1);
    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetSnapshotCsn();
    HeapTuple *heapTuple = m_utTableHandler->GenerateRandomHeapTuple();
    HeapInsertHandler heapInsert(g_storageInstance, thrd, m_utTableHandler->GetTableRel());
    HeapInsertContext insertContext;
    insertContext.heapTuple = heapTuple;
    insertContext.cid = txn->GetCurCid();
    thrd->GetActiveTransaction()->SetSnapshotCsnForFlashback(txn->GetSnapshotData()->GetCsn());
    int retVal = heapInsert.Insert(&insertContext);
    StorageAssert(retVal == DSTORE_FAIL);
    Xid xid = txn->GetCurrentXid();
    txn->Commit();
    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());
    FAULT_INJECTION_INACTIVE(DstoreTransactionFI::EXTEND_UNDO_SPACE_FAIL, FI_GLOBAL);

    /* step 5: read walFile to Get WalRecord of WAL_HEAP_ALLOC_TD type */
    std::vector<WalRecordRedoInfo *> recordList;
    RetStatus retStatus = ReadRecordsAfterPlsn(plsn, WAL_HEAP_ALLOC_TD, recordList);
    ASSERT_TRUE(STORAGE_FUNC_SUCC(retStatus));

    /* step 5: use WalRecord to redo for Page in Image */
    char redoPage[BLCKSZ];
    char afterPage[BLCKSZ];
    WalRecordHeap *record = static_cast<WalRecordHeap *>(&(recordList[0]->walRecord));
    ASSERT_TRUE(record->m_pageId == ctid.GetPageId());
    FetchPage(afterPage, record->m_pageId);
    RestorePage(tmpPage, record->m_pageId);
    WalRecordRedoContext redoCtx = {xid, m_walWriter->GetWalId(), DSTORE::g_defaultPdbId};
    RedoHeapRecord(&redoCtx, record, m_walStream->GetMaxAppendedPlsn());
    FetchPage(redoPage, record->m_pageId);

    /* step 6: compare redoPage and afterPage */
    HeapPage* redoHeapPage = reinterpret_cast<HeapPage *>(redoPage);
    HeapPage* afterHeapPage = reinterpret_cast<HeapPage *>(afterPage);

    /* Compare page info from m_flags to tdCount */
    uint32 cmpSize = OFFSETOF(HeapPage, m_heapPageHeader.potentialDelSize) -
                     OFFSETOF(HeapPage, m_header.m_flags);
    ASSERT_TRUE(redoHeapPage->m_header.m_lower + sizeof(ItemId) == afterHeapPage->m_header.m_lower);
    ASSERT_TRUE(afterHeapPage->m_header.m_flags == (PAGE_TUPLE_PRUNABLE | PAGE_HAS_FREE_LINES));
    redoHeapPage->m_header.m_lower = afterHeapPage->m_header.m_lower;
    redoHeapPage->m_header.m_flags = afterHeapPage->m_header.m_flags;
    ASSERT_TRUE(CmpPages(redoPage + OFFSETOF(HeapPage, m_header.m_flags),
        afterPage + OFFSETOF(HeapPage, m_header.m_flags), cmpSize));

    /* compare all TD */
    TD* redoTd = static_cast<TD*>(static_cast<void*>(redoPage + OFFSETOF(HeapPage, m_data)));
    TD* afterTd = static_cast<TD*>(static_cast<void*>(afterPage + OFFSETOF(HeapPage, m_data)));
    for (int i = 0; i < DEFAULT_TD_COUNT; i++) {
        ASSERT_TRUE(CmpTDInfo(&redoTd[i], &afterTd[i]));
    }

    /* Compare page from upper to the page end */
    cmpSize = BLCKSZ - redoHeapPage->m_header.m_upper;
    ASSERT_TRUE(CmpPages(redoPage + redoHeapPage->m_header.m_upper,
        afterPage + redoHeapPage->m_header.m_upper, cmpSize));
}

/* this case to keep that page is same in primary and standby node when heap batch insert undo record fail */
TEST_F(HeapWalTest, WalHeapBatchInsertUndoRecordFailTest_level0)
{
    /* Step 1: Insert four tuple to occupy all TD */
    const int insertTupleNum = 4;
    ItemPointerData ctid;
    for (int i = 0; i < insertTupleNum; ++i) {
        ctid = InsertSpecificHeapTuple("tmp");
    }
    g_storageInstance->GetBufferMgr()->FlushAll(false);

    /* step 2: copy dataFile as Image */
    CopyDataFile();
    char tmpPage[BLCKSZ];
    FetchPage(tmpPage, ctid.GetPageId());
    /* step 3: get endPlsn of WalFile */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);

    /* Step 4: batch insert four tuple and insert undo record fail, will only record alloctd wal */
    FAULT_INJECTION_ACTIVE_MODE_LEVEL(DstoreTransactionFI::INSERT_UNDO_RECORD_FAIL, 0, FI_GLOBAL, 0, 1);
    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetSnapshotCsn();
    const uint16 nTuples = 4;
    HeapInsertContext contexts[nTuples];
    for (uint16 i = 0; i < nTuples; i++) {
        HeapInsertContext context;
        context.heapTuple = m_utTableHandler->GenerateSpecificHeapTuple("123");
        context.ctid = INVALID_ITEM_POINTER;
        context.cid = txn->GetCurCid();
        contexts[i] = context;
    }
    HeapBacthInsertContext batchConext{contexts, nTuples};
    HeapInsertHandler heapInsert(g_storageInstance, thrd, m_utTableHandler->GetTableRel());
    int retVal = heapInsert.BatchInsert(&batchConext);
    StorageAssert(retVal == DSTORE_FAIL);
    Xid xid = txn->GetCurrentXid();
    txn->Commit();
    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());
    FAULT_INJECTION_INACTIVE(DstoreTransactionFI::INSERT_UNDO_RECORD_FAIL, FI_GLOBAL);

    /* step 5: read walFile to Get WalRecord of WAL_HEAP_ALLOC_TD type */
    std::vector<WalRecordRedoInfo *> recordList;
    RetStatus retStatus = ReadRecordsAfterPlsn(plsn, WAL_HEAP_ALLOC_TD, recordList);
    ASSERT_TRUE(STORAGE_FUNC_SUCC(retStatus));

    /* step 5: use WalRecord to redo for Page in Image */
    char redoPage[BLCKSZ];
    char afterPage[BLCKSZ];
    WalRecordHeap *record = static_cast<WalRecordHeap *>(&(recordList[0]->walRecord));
    ASSERT_TRUE(record->m_pageId == ctid.GetPageId());
    FetchPage(afterPage, record->m_pageId);
    RestorePage(tmpPage, record->m_pageId);
    WalRecordRedoContext redoCtx = {xid, m_walWriter->GetWalId(), DSTORE::g_defaultPdbId};
    RedoHeapRecord(&redoCtx, record, m_walStream->GetMaxAppendedPlsn());
    FetchPage(redoPage, record->m_pageId);

    /* step 6: compare redoPage and afterPage */
    HeapPage* redoHeapPage = reinterpret_cast<HeapPage *>(redoPage);
    HeapPage* afterHeapPage = reinterpret_cast<HeapPage *>(afterPage);

    /* Compare page info from m_flags to tdCount */
    ASSERT_TRUE(redoHeapPage->m_header.m_lower + nTuples * sizeof(ItemId) == afterHeapPage->m_header.m_lower);
    ASSERT_TRUE(afterHeapPage->m_header.m_flags == (PAGE_TUPLE_PRUNABLE | PAGE_HAS_FREE_LINES));
    redoHeapPage->m_header.m_lower = afterHeapPage->m_header.m_lower;
    redoHeapPage->m_header.m_flags = afterHeapPage->m_header.m_flags;
    uint32 cmpSize = OFFSETOF(HeapPage, m_heapPageHeader.potentialDelSize) -
                     OFFSETOF(HeapPage, m_header.m_flags);
    ASSERT_TRUE(CmpPages(redoPage + OFFSETOF(HeapPage, m_header.m_flags),
        afterPage + OFFSETOF(HeapPage, m_header.m_flags), cmpSize));

    /* compare all TD */
    TD* redoTd = static_cast<TD*>(static_cast<void*>(redoPage + OFFSETOF(HeapPage, m_data)));
    TD* afterTd = static_cast<TD*>(static_cast<void*>(afterPage + OFFSETOF(HeapPage, m_data)));
    for (int i = 0; i < DEFAULT_TD_COUNT; i++) {
        ASSERT_TRUE(CmpTDInfo(&redoTd[i], &afterTd[i]));
    }

    /* Compare page from upper to the page end */
    cmpSize = BLCKSZ - redoHeapPage->m_header.m_upper;
    ASSERT_TRUE(CmpPages(redoPage + redoHeapPage->m_header.m_upper,
        afterPage + redoHeapPage->m_header.m_upper, cmpSize));
}