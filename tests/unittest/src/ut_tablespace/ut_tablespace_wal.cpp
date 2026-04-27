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

#include "ut_utilities/ut_dstore_framework.h"
#include "tablespace/dstore_tablespace_wal.h"
#include "page/dstore_undo_segment_meta_page.h"
#include "ut_wal/ut_wal_basic.h"
#include "tablespace/dstore_tablespace.h"
#include "ut_mock/ut_mock.h"
#include "ut_tablespace/ut_segment.h"
#include "pdb/dstore_pdb_interface.h"

const FileId UT_WAL_TABLESPACE_FILE_ID_0 = 900;
const char *UT_WAL_TABLESPACE_FILE_NAME_0 = "ut_tablespace_file_900";

class TableSpaceWalTest : public WALBASICTEST {
protected:
    StorageInstance *m_instance;
    TableSpaceInterface *m_testTbs;
    TableSpaceInterface *m_undoTbs;
    BufMgrInterface *m_testBufMgr;
    InitDataPageCallback m_initHeapPageCallback = HeapPage::InitHeapPage;
    InitDataPageCallback m_initIndexPageCallback = BtrPage::InitBtrPage;
    void SetUp() override
    {
        DSTORETEST::SetUp();
        WALBASICTEST::Prepare();

        m_instance = g_storageInstance;
        m_testBufMgr = g_storageInstance->GetBufferMgr();
        /* use undo tablespace */
        TablespaceMgr *tablespaceMgr = g_storageInstance->GetPdb(g_defaultPdbId)->GetTablespaceMgr();
        m_undoTbs = (TableSpaceInterface *)tablespaceMgr->OpenTablespace(4, DSTORE::DSTORE_ACCESS_EXCLUSIVE_LOCK);
        FileId fileId;
        (void)((TableSpace *)m_undoTbs)->AllocAndAddDataFile(g_defaultPdbId, &fileId, EXTENT_SIZE_ARRAY[0], false);
        tablespaceMgr->CloseTablespace((TableSpace *)m_undoTbs, DSTORE::DSTORE_ACCESS_EXCLUSIVE_LOCK);

        /* use default tablespace */
        tablespaceMgr = g_storageInstance->GetPdb(g_defaultPdbId)->GetTablespaceMgr();
        m_testTbs = (TableSpaceInterface *)tablespaceMgr->OpenTablespace(2, DSTORE::DSTORE_ACCESS_EXCLUSIVE_LOCK);
        for (uint16 i = 0; i < EXTENT_TYPE_COUNT; i++) {
            (void)((TableSpace *)m_testTbs)->AllocAndAddDataFile(g_defaultPdbId, &fileId, EXTENT_SIZE_ARRAY[i], false);
        }
        StoragePdbInterface::FlushAllDirtyPages(DSTORE::g_defaultPdbId);
        StoragePdbInterface::FlushAllDirtyPages(DSTORE::g_defaultPdbId);
        tablespaceMgr->CloseTablespace((TableSpace *)m_testTbs, DSTORE::DSTORE_ACCESS_EXCLUSIVE_LOCK);
        m_testTbs = (TableSpace *)UtMockModule::UtGetTableSpace(2);

        /* enable Wal flushing */
        PrepareControlFileContent();
        NodeId selfNode = 0;
        RetStatus retStatus = m_walStreamManager->Init(m_walControlFile);
        ASSERT_EQ(retStatus, DSTORE_SUCC);
        uint32 streamCount = 0;
        m_walStream = m_walStreamManager->GetWritingWalStream();

        WalReaderConf readerConf = {0, 0, m_walStream, nullptr,
                                    static_cast<uint64>(g_storageInstance->GetGuc()->walFileSize),
                                    DSTORE::WalReadSource::WAL_READ_FROM_DISK};
        retStatus = WalRecordReader::AllocateWalReader(readerConf, &m_walRecordReader, m_ut_memory_context);
        ASSERT_EQ(retStatus, DSTORE_SUCC);
    }

    void TearDown() override
    {
        UtMockModule::UtDropTableSpace(m_testTbs);
        UtInstance *instance = (UtInstance *) g_storageInstance;
        instance->Shutdown();
        delete instance;
        DSTORETEST::TearDown();
    }

    void CheckWalModifyExtMetaNext(uint64 plsn, uint32 recordCount, bool ignorePlsn = false)
    {
        std::vector<WalRecordRedoInfo *> recordList;
        ASSERT_EQ(ReadRecordsAfterPlsn(plsn, WAL_TBS_MODIFY_EXT_META_NEXT, recordList), DSTORE_SUCC);

        char redoPage[BLCKSZ];
        BufferDesc *bufferDesc;
        for (uint32 i = 0; i < recordCount; i++) {
            WalRecordRedoInfo *recordInfo = recordList.at(i);
            WalRecordTbsExtentMetaLinkNext *walRecord =
                    static_cast<WalRecordTbsExtentMetaLinkNext *>(&(recordInfo->walRecord));
            bufferDesc = m_testBufMgr->Read(g_defaultPdbId, walRecord->m_pageId, LW_SHARED);
            ReadDataFileImage(walRecord->m_pageId, redoPage, BLCKSZ);
            walRecord->RedoInternal(g_defaultPdbId, recordInfo->endPlsn, (Page *) redoPage);
            DstorePfreeExt(recordInfo);
            m_testBufMgr->UnlockAndRelease(bufferDesc);
        }

        /* compare redoPage and afterPage */
        Page *afterPage = bufferDesc->GetPage();
        ASSERT_TRUE(CmpPages(redoPage, (char *) afterPage, sizeof(SegExtentMetaPage), ignorePlsn));
    }

    void CheckWalInitMutilDataPages(uint64 plsn, uint32 recordCount, bool ignorePlsn = false)
    {
        std::vector<WalRecordRedoInfo *> recordList;
        /* read walFile to Get WalRecord of WAL_TBS_INIT_MULTIPLE_DATA_PAGES type */
        ASSERT_EQ(ReadRecordsAfterPlsn(plsn, WAL_TBS_INIT_EXT_META, recordList), DSTORE_SUCC);

        char redoPage[BLCKSZ];
        BufferDesc *bufferDesc;
        for (uint32 i = 0; i < recordCount; i++) {
            WalRecordRedoInfo *recordInfo = recordList.at(i);
            WalRecordTbsInitDataPages *walRecord =
                    static_cast<WalRecordTbsInitDataPages *>(&(recordInfo->walRecord));
            DstorePfreeExt(recordInfo);
            m_testBufMgr->UnlockAndRelease(bufferDesc);
        }

        /* compare redoPage and afterPage */
        Page *afterPage = bufferDesc->GetPage();
        ASSERT_TRUE(CmpPages(redoPage, (char *) afterPage, BLCKSZ, ignorePlsn));
    }

    void CheckWalsForOnePage(uint64 plsn, uint32 count, WalType walType[], bool ignorePlsn)
    {
        char redoPage[BLCKSZ];
        BufferDesc *bufferDesc;
        for (uint32 i = 0; i < count; i++) {
            WalType type = walType[i];
            std::vector<WalRecordRedoInfo *> recordList;
            ASSERT_EQ(ReadRecordsAfterPlsn(plsn, type, recordList), DSTORE_SUCC);
            WalRecordRedoInfo *recordInfo = recordList.at(0);
            WalRecordTbs *walRecord = static_cast<WalRecordTbs *>(&(recordInfo->walRecord));
            if (i == 0) {
                ReadDataFileImage(walRecord->m_pageId, redoPage, BLCKSZ);
            }
            walRecord->RedoInternal(g_defaultPdbId, recordInfo->endPlsn, (Page *) redoPage);
            if (i == count - 1) {
                bufferDesc = m_testBufMgr->Read(g_defaultPdbId, walRecord->m_pageId, LW_SHARED);
            }
            DstorePfreeExt(recordInfo);
        }
        Page *afterPage = bufferDesc->GetPage();
        m_testBufMgr->UnlockAndRelease(bufferDesc);
        ASSERT_TRUE(CmpPages(redoPage, (char *) afterPage, BLCKSZ, ignorePlsn));
    }

    void CheckWalsWithIndexForOnePage(uint64 plsn, uint32 count, WalType walType[], uint32 index[], bool ignorePlsn)
    {
        char redoPage[BLCKSZ];
        BufferDesc *bufferDesc;
        for (uint32 i = 0; i < count; i++) {
            WalType type = walType[i];
            std::vector<WalRecordRedoInfo *> recordList;
            ASSERT_EQ(ReadRecordsAfterPlsn(plsn, type, recordList), DSTORE_SUCC);
            WalRecordRedoInfo *recordInfo = recordList.at(index[i]);
            WalRecordTbs *walRecord = static_cast<WalRecordTbs *>(&(recordInfo->walRecord));
            if (i == 0) {
                ReadDataFileImage(walRecord->m_pageId, redoPage, BLCKSZ);
            }
            walRecord->RedoInternal(g_defaultPdbId, recordInfo->endPlsn, (Page *) redoPage);
            if (i == count - 1) {
                bufferDesc = m_testBufMgr->Read(g_defaultPdbId, walRecord->m_pageId, LW_SHARED);
            }
            DstorePfreeExt(recordInfo);
        }
        Page *afterPage = bufferDesc->GetPage();
        m_testBufMgr->UnlockAndRelease(bufferDesc);
        ASSERT_TRUE(CmpPages(redoPage, (char *) afterPage, BLCKSZ, ignorePlsn));
    }

    void RecoveryAndCheckAllPages(uint64 plsn, bool isDataImage)
    {
        /* alloc a large memory for image, and redo based on the memory. */
        uint32 blockCount;
        FileId startFileId = isDataImage ? 5124 : 5121;
        char  **imageBufferArray[EXTENT_TYPE_COUNT];
        for (uint16 i = 0; i < EXTENT_TYPE_COUNT; i++) {
            imageBufferArray[i] = AllocBufferForImage(true, &blockCount, startFileId + i, 0);
        }

        std::vector<WalRecordRedoInfo *> recordList;
        /* read all walRecords */
        ASSERT_EQ(ReadRecordsAfterPlsn(plsn, WAL_TYPE_BUTTOM, recordList), DSTORE_SUCC);
        FileId fileId;
        for (WalRecordRedoInfo *recordInfo : recordList) {
            const WalRecordTbs *walRecord = static_cast<WalRecordTbs *>(&(recordInfo->walRecord));
            if (walRecord->m_type == WAL_TBS_INIT_MULTIPLE_DATA_PAGES ||
                walRecord->m_type == WAL_TBS_INIT_BITMAP_PAGES) {
                WalRecordRedoContext redoCtx = {Xid(-1), 0, DSTORE::g_defaultPdbId, recordInfo->endPlsn};
                WalRecordTbs::RedoTbsRecord(&redoCtx, walRecord, nullptr);
                continue;
            }
            uint32 blockId = walRecord->m_pageId.m_blockId;
            fileId = walRecord->m_pageId.m_fileId;
            if (fileId < startFileId || fileId >= startFileId + EXTENT_TYPE_COUNT) {
                continue;
            }
            uint16 index = fileId - startFileId;
            walRecord->RedoInternal(g_defaultPdbId, recordInfo->endPlsn, (Page *)imageBufferArray[index][blockId]);
        }

        /* check all page */
        for (uint32 i = 2 ; i < blockCount; i++) {
            PageId pageId = {fileId, i};
            BufferDesc *bufferDesc = m_testBufMgr->Read(g_defaultPdbId, pageId, LW_SHARED);
            Page *afterPage = bufferDesc->GetPage();
            m_testBufMgr->UnlockAndRelease(bufferDesc);
            if (afterPage->GetType() == PageType::HEAP_PAGE_TYPE ||
                afterPage->GetType() == PageType::INVALID_PAGE_TYPE) {
                continue;
            }
            fileId = bufferDesc->GetPageId().m_fileId;
            uint16 index = fileId - startFileId;
            char *redoPage = imageBufferArray[index][i];
            ASSERT_TRUE(CmpPages(redoPage, (char *) afterPage, BLCKSZ, true));
        }

        /* free the imageBuffer */
        for (uint16 i = 0; i < EXTENT_TYPE_COUNT; i++) {
            FreeBufferForImage(imageBufferArray[i], blockCount);
        }
    }

    void ReadPageForFsmIndex(const PageId &pageId, LWLockMode mode, FsmIndex *fsmIndex)
    {
        /* Read fsm index in pageId */
        BufferDesc *pageBuf = m_testBufMgr->Read(g_defaultPdbId, pageId, mode);
        auto *dataPage = (HeapPage *) pageBuf->GetPage();
        *fsmIndex = dataPage->GetFsmIndex();
        m_testBufMgr->UnlockAndRelease(pageBuf);
    }
};

TEST_F(TableSpaceWalTest, AllocSegmentTest_level0)
{
    /* step 1: copy dataFile as Image */
    CopyDataFile();
    /* step 2: get endPlsn of WalFile */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);

    /* step 3: do AllocSegment */
    SegmentInterface *segment = SegmentInterface::AllocUndoSegment(g_defaultPdbId, static_cast<TablespaceId>(TBS_ID::UNDO_TABLE_SPACE_ID), SegmentType::UNDO_SEGMENT_TYPE, m_testBufMgr);
    ASSERT_TRUE(SegmentIsValid(segment));
    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());

    WalType walType[1] = {WAL_TBS_BITMAP_ALLOC_BIT_START};
    CheckWalsForOnePage(plsn, 1, walType, false);
    WalType walType2[1] = {WAL_TBS_INIT_UNDO_SEGMENT_META};
    CheckWalsForOnePage(plsn, 1, walType2, false);
}

TEST_F(TableSpaceWalTest, DropSegmentTest_level0)
{
    /* before dropSegment, we need allocSegment */
    SegmentInterface *segment = SegmentInterface::AllocUndoSegment(g_defaultPdbId, static_cast<TablespaceId>(TBS_ID::UNDO_TABLE_SPACE_ID), SegmentType::UNDO_SEGMENT_TYPE, m_testBufMgr);
    ASSERT_TRUE(SegmentIsValid(segment));
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);
    m_testBufMgr->FlushAll(false);

    /* step 1: copy dataFile as Image */
    CopyDataFile();

    ASSERT_EQ(segment->DropSegment(), DSTORE_SUCC);
    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());

    /* step 4: read walFile to Get WalRecord of WAL_TBS_BITMAP_FREE_BIT_START type */
    WalType walType[1] = {WAL_TBS_BITMAP_FREE_BIT_START};
    CheckWalsForOnePage(plsn, 1, walType, false);
}

TEST_F(TableSpaceWalTest, ExtendAndDropSegmentTest_level0)
{
    /* before dropSegment, we need allocSegment */
    SegmentInterface *segment = SegmentInterface::AllocUndoSegment(g_defaultPdbId, static_cast<TablespaceId>(TBS_ID::UNDO_TABLE_SPACE_ID), SegmentType::UNDO_SEGMENT_TYPE, m_testBufMgr);
    ASSERT_TRUE(SegmentIsValid(segment));
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);
    m_testBufMgr->FlushAll(false);

    /* step 1: copy dataFile as Image */
    CopyDataFile();

    PageId newExtPageId;
    /* write WAL_TBS_BITMAP_ALLOC_BIT_START, WAL_TBS_INIT_EXT_META, WAL_TBS_MODIFY_EXT_META_NEXT, WAL_TBS_SEG_ADD_EXT */
    ASSERT_EQ(segment->Extend(EXT_SIZE_8, &newExtPageId), DSTORE_SUCC);
    ASSERT_NE(newExtPageId, INVALID_PAGE_ID);
    /* wait wal flush to disk */
    uint64 plsn2 = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn2);

    WalType walType0[1] = {WAL_TBS_BITMAP_ALLOC_BIT_START};
    CheckWalsForOnePage(plsn, 1, walType0, false);

    WalType walType[1] = {WAL_TBS_INIT_EXT_META};
    CheckWalsForOnePage(plsn, 1, walType, true);

    WalType walType3[1] = {WAL_TBS_SEG_ADD_EXT};
    CheckWalsForOnePage(plsn, 1, walType3, false);

    m_testBufMgr->FlushAll(false);
    CopyDataFile();

    ASSERT_EQ(segment->DropSegment(), DSTORE_SUCC);
    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());

    /* step 4: read walFile to Get WalRecord of WAL_TBS_SEG_UNLINK_EXT type */
    RecoveryAndCheckAllPages(plsn2, false);
}

TEST_F(TableSpaceWalTest, AllocDataSegmentTest_level0)
{
    /* step 1: copy dataFile as Image */
    CopyDataFile();
    /* step 2: get endPlsn of WalFile */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);

    /* step 3: do AllocSegment */
    HeapNormalSegment *segment = (HeapNormalSegment*)SegmentTest::UTAllocSegment(g_defaultPdbId, m_testTbs->GetTablespaceId(), m_testBufMgr, SegmentType::HEAP_SEGMENT_TYPE);
    ASSERT_TRUE(SegmentIsValid(segment));
    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());

    WalType walType[1] = {WAL_TBS_BITMAP_ALLOC_BIT_START};
    CheckWalsForOnePage(plsn, 1, walType, false);

    WalType walType2[2] = {WAL_TBS_INIT_HEAP_SEGMENT_META};
    CheckWalsForOnePage(plsn, 1, walType2, false);
}

TEST_F(TableSpaceWalTest, DropDataSegmentTest_level0)
{
    HeapNormalSegment *segment = (HeapNormalSegment*)SegmentTest::UTAllocSegment(g_defaultPdbId, m_testTbs->GetTablespaceId(), m_testBufMgr, SegmentType::HEAP_SEGMENT_TYPE);
    ASSERT_TRUE(SegmentIsValid(segment));
    /* wait wal flush to disk */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);
    m_testBufMgr->FlushAll(false);

    /* copy dataFile as Image */
    CopyDataFile();

    ASSERT_EQ(segment->DropSegment(), DSTORE_SUCC);
    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());

    WalType walType[1] = {WAL_TBS_BITMAP_FREE_BIT_START};
    CheckWalsForOnePage(plsn, 1, walType, false);
}

TEST_F(TableSpaceWalTest, InitFsmTest_level0)
{
    CopyDataFile();
    HeapNormalSegment *segment = (HeapNormalSegment*)SegmentTest::UTAllocSegment(g_defaultPdbId, m_testTbs->GetTablespaceId(), m_testBufMgr, SegmentType::HEAP_SEGMENT_TYPE);
    ASSERT_TRUE(SegmentIsValid(segment));
    /* wait wal flush to disk */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);
    m_testBufMgr->FlushAll(false);

    WalType walType2[1] = {WAL_TBS_INIT_FSM_PAGE};
    CheckWalsForOnePage(0, 1, walType2, true);
    WalType walType[1] = {WAL_TBS_INIT_FSM_META};
    CheckWalsForOnePage(0, 1, walType, false);
}

TEST_F(TableSpaceWalTest, UpdateFileMetaPageTest_level0)
{
    CopyDataFile();
    HeapNormalSegment *segment = (HeapNormalSegment*)SegmentTest::UTAllocSegment(g_defaultPdbId, m_testTbs->GetTablespaceId(), m_testBufMgr, SegmentType::HEAP_SEGMENT_TYPE);
    ASSERT_TRUE(SegmentIsValid(segment));
    /* wait wal flush to disk */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);
    m_testBufMgr->FlushAll(false);

    WalType walType[1] = {WAL_TBS_UPDATE_TBS_FILE_META_PAGE};
    CheckWalsForOnePage(0, 1, walType, true);
}

TEST_F(TableSpaceWalTest, GetNewPageTest_level0)
{
    HeapNormalSegment *segment = (HeapNormalSegment*)SegmentTest::UTAllocSegment(g_defaultPdbId, m_testTbs->GetTablespaceId(), m_testBufMgr, SegmentType::HEAP_SEGMENT_TYPE);
    ASSERT_TRUE(SegmentIsValid(segment));
    /* wait wal flush to disk */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);
    m_testBufMgr->FlushAll(false);

    ASSERT_NE(segment->GetNewPage(), INVALID_PAGE_ID);
    /* the first GetNewPage won't extend fsm and data extent, the second GetNewPage will extend data extent */
    plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);
    m_testBufMgr->FlushAll(false);

    /* copy dataFile as Image */
    CopyDataFile();

    ASSERT_NE(segment->GetNewPage(), INVALID_PAGE_ID);
    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());

    /* PrepareFreeDataPages writes the following logs:
     * WAL_TBS_BITMAP_ALLOC_BIT_START, WAL_TBS_INIT_EXT_META,  WAL_TBS_DATA_SEG_ADD_EXT */
    /* extend the second extent does not need WAL_TBS_MODIFY_EXT_META_NEXT */
    WalType walType[1] = {WAL_TBS_BITMAP_ALLOC_BIT_START};
    CheckWalsForOnePage(plsn, 1, walType, false);

    WalType walType2[1] = {WAL_TBS_INIT_EXT_META};
    CheckWalsForOnePage(plsn, 1, walType2, true);

    /* AddDataPagesToFsm writes the following logs:
     * WAL_TBS_INIT_MULTIPLE_DATA_PAGES, WAL_TBS_ADD_FSM_SLOT, WAL_TBS_SEG_META_ADJUST_DATA_PAGES_INFO */
    WalType walType3[1] = {WAL_TBS_ADD_FSM_SLOT};
    CheckWalsForOnePage(plsn, 1, walType3, true);
    WalType walType4[2] = {WAL_TBS_DATA_SEG_ADD_EXT, WAL_TBS_SEG_META_ADJUST_DATA_PAGES_INFO};
    CheckWalsForOnePage(plsn, 2, walType4, false);
}

TEST_F(TableSpaceWalTest, UpdateFsmTest_level0)
{
    HeapNormalSegment *segment = (HeapNormalSegment*)SegmentTest::UTAllocSegment(g_defaultPdbId, m_testTbs->GetTablespaceId(), m_testBufMgr, SegmentType::HEAP_SEGMENT_TYPE);
    ASSERT_TRUE(SegmentIsValid(segment));
    ASSERT_NE(segment->GetNewPage(), INVALID_PAGE_ID);
    /* wait wal flush to disk */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);
    m_testBufMgr->FlushAll(false);

    CopyDataFile();

    PageId firstPageId = segment->GetFirstDataPage();
    ASSERT_NE(firstPageId, INVALID_PAGE_ID);
    FsmIndex fsmIndex;
    ReadPageForFsmIndex(firstPageId, LW_SHARED, &fsmIndex);
    ASSERT_EQ(segment->UpdateFsm(firstPageId, 0), DSTORE_SUCC);
    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());

    WalType walType2[1] = {WAL_TBS_MOVE_FSM_SLOT};
    CheckWalsForOnePage(plsn, 1, walType2, true);
}

TEST_F(TableSpaceWalTest, UpdateFsmWithCheckAllPageTest_level0)
{
    HeapNormalSegment *segment = (HeapNormalSegment*)SegmentTest::UTAllocSegment(g_defaultPdbId, m_testTbs->GetTablespaceId(), m_testBufMgr, SegmentType::HEAP_SEGMENT_TYPE);
    ASSERT_TRUE(SegmentIsValid(segment));
    for (uint32 i = 0; i < 21; i++) {
        ASSERT_NE(segment->GetNewPage(), INVALID_PAGE_ID);
    }
    ASSERT_NE(segment->GetNewPage(), INVALID_PAGE_ID);
    /* wait wal flush to disk */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);
    m_testBufMgr->FlushAll(false);

    CopyDataFile();

    PageId firstPageId = segment->GetFirstDataPage();
    ASSERT_NE(firstPageId, INVALID_PAGE_ID);
    FsmIndex fsmIndex;
    ReadPageForFsmIndex(firstPageId, LW_SHARED, &fsmIndex);
    ASSERT_EQ(segment->UpdateFsm(firstPageId, 0), DSTORE_SUCC);

    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());

    RecoveryAndCheckAllPages(plsn, true);
}

TEST_F(TableSpaceWalTest, GetNewPageWithCheckAllPageTest_level0)
{
    HeapNormalSegment *segment = (HeapNormalSegment*)SegmentTest::UTAllocSegment(g_defaultPdbId, m_testTbs->GetTablespaceId(), m_testBufMgr, SegmentType::HEAP_SEGMENT_TYPE);
    ASSERT_TRUE(SegmentIsValid(segment));
    for (uint32 i = 0; i < 2; i++) {
        ASSERT_NE(segment->GetNewPage(), INVALID_PAGE_ID);
    }

    /* wait wal flush to disk */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);
    m_testBufMgr->FlushAll(false);

    CopyDataFile();

    for (uint32 i = 0; i < 3; i++) {
        ASSERT_NE(segment->GetNewPage(), INVALID_PAGE_ID);
    }
    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());

    RecoveryAndCheckAllPages(plsn, true);
}

TEST_F(TableSpaceWalTest, GetNewPageWithExtendFsmTest_level0)
{
    HeapNormalSegment *segment = (HeapNormalSegment*)SegmentTest::UTAllocSegment(g_defaultPdbId, m_testTbs->GetTablespaceId(), m_testBufMgr, SegmentType::HEAP_SEGMENT_TYPE);
    ASSERT_TRUE(SegmentIsValid(segment));
    /* use up the slots in fsm page, to trigger extending fsm */
    for (uint32 i = 0; i < 21; i++) {
        ASSERT_NE(segment->GetNewPage(), INVALID_PAGE_ID);
    }
    /* wait wal flush to disk */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);
    m_testBufMgr->FlushAll(false);

    CopyDataFile();
    /* in this GetNewPage, it will extend fsm */
    ASSERT_NE(segment->GetNewPage(), INVALID_PAGE_ID);

    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());
    /* PrepareFreeSlots writes the following logs */
    WalType walType0[1] = {WAL_TBS_BITMAP_ALLOC_BIT_START};
    CheckWalsForOnePage(plsn, 1, walType0, false);

    WalType walType[1] = {WAL_TBS_INIT_EXT_META};
    CheckWalsForOnePage(plsn, 1, walType, true);

    WalType walType4[1] = {WAL_TBS_MODIFY_FSM_INDEX};
    CheckWalsForOnePage(plsn, 1, walType4, true);

    WalType walType5[4] = {WAL_TBS_SEG_ADD_EXT, WAL_TBS_FSM_META_UPDATE_FSM_TREE,
        WAL_TBS_FSM_META_UPDATE_EXTENSION_STAT, WAL_TBS_FSM_META_UPDATE_NUM_USED_PAGES};
    CheckWalsForOnePage(plsn, 4, walType5, true);

    /* AddDataPagesToFsm writes the following logs */
    WalType walType3[2] = {WAL_TBS_INIT_FSM_PAGE, WAL_TBS_ADD_FSM_SLOT};
    uint32 index[2] = {1, 0};
    CheckWalsWithIndexForOnePage(plsn, 2, walType3, index, true);
    WalType walType6[1] = {WAL_TBS_SEG_META_ADJUST_DATA_PAGES_INFO};
    CheckWalsForOnePage(plsn, 1, walType6, false);
}

TEST_F(TableSpaceWalTest, AddFsmSlotWithLeafFsmPageTest_level0)
{
    HeapNormalSegment *segment = (HeapNormalSegment*)SegmentTest::UTAllocSegment(g_defaultPdbId, m_testTbs->GetTablespaceId(), m_testBufMgr, SegmentType::HEAP_SEGMENT_TYPE);
    ASSERT_TRUE(SegmentIsValid(segment));
    /* use up the slots in fsm page, to trigger extending fsm */
    for (uint32 i = 0; i < 27; i++) {
        ASSERT_NE(segment->GetNewPage(), INVALID_PAGE_ID);
    }
    /* wait wal flush to disk */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);
    m_testBufMgr->FlushAll(false);

    CopyDataFile();
    /* in this GetNewPage, it will extend fsm */
    ASSERT_NE(segment->GetNewPage(), INVALID_PAGE_ID);

    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());
    /* PrepareFreeSlots writes the following logs */
    WalType walType0[2] = {WAL_TBS_ADD_FSM_SLOT, WAL_TBS_MOVE_FSM_SLOT};
    CheckWalsForOnePage(plsn, 2, walType0, true);
}

TEST_F(TableSpaceWalTest, ExtendFileTest_level0)
{
    CopyDataFile();

    PageId page_id = INVALID_PAGE_ID;
    /* An extent (64 MB) is allocated. The space is insufficient. The file size needs to be expanded. */
    bool isReusedFlag = false;
    ASSERT_EQ(((TableSpace *)m_testTbs)->AllocExtent(EXT_SIZE_8192, &page_id, &isReusedFlag), DSTORE_SUCC);

    /* wait wal flush to disk */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);
    m_testBufMgr->FlushAll(false);

    /* PrepareFreeSlots writes the following logs */
    WalType walType0[1] = {WAL_TBS_EXTEND_FILE};
    CheckWalsForOnePage(0, 1, walType0, false);
}

