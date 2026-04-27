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
 *
 * ---------------------------------------------------------------------------------------
 *
 * ut_wal_stream.cpp
 *
 * Description:
 *
 * ---------------------------------------------------------------------------------------
 *
 */

#include "ut_wal/ut_wal_basic.h"
#include "ut_utilities/ut_dstore_framework.h"
#include "ut_mock/ut_mock.h"
#include "ut_buffer/ut_buffer_fake_instance.h"
#include "common/algorithm/dstore_scalable_array.h"

namespace DSTORE {

class WalStreamTest : public WALBASICTEST {
protected:
    void SetUp() override;

    void TearDown() override;

protected:
    uint64 m_startReadPlsn;
    WalRecord *record = nullptr;
};
}

void WalStreamTest::SetUp()
{
    WALBASICTEST::SetUp();
    PrepareControlFileContent();
    int ret = mkdir("dstore_wal", 0777);
    StorageAssert(ret == 0);
    m_walManager->Init(m_walControlFile);
    RetStatus retStatus = m_walStreamManager->Init(m_walControlFile);
    ASSERT_EQ(retStatus, DSTORE_SUCC);
    m_startReadPlsn = m_walStreamManager->GetWalStream(1)->GetMaxWrittenToFilePlsn();
}

void WalStreamTest::TearDown()
{
    if (record != nullptr) {
        free(record);
        record = nullptr;
    }
    WALBASICTEST::TearDown();

}

/* Inserting logs normally */
TEST_F(WalStreamTest, RememberPageNeedWalTest001)
{
    EXPECT_NE(m_walWriter, nullptr);

    uint32 length = 5000;
    record = BuildWal(length);
    WalGroupLsnInfo insertResult = {INVALID_WAL_ID, INVALID_PLSN, INVALID_PLSN};

    int rc = 0;
    BufMgrInterface *m_bufferMgr = g_storageInstance->GetBufferMgr();
    TableSpace *tablespace = (TableSpace *)UtMockModule::UtGetTableSpace(2);
    FileSize fileSize;
    fileSize.initialFileSize = INIT_FILE_SIZE;
    fileSize.maxFileSize = MAX_FILE_SIZE;
    rc = tablespace->AddFile(UT_WAL_FILE0_ID, fileSize, nullptr, EXT_SIZE_8192);
    ASSERT_EQ(rc, 0);

    PageId page_id = {UT_WAL_FILE0_ID, 2};
    BufferDesc *bufferDesc;
    bufferDesc = g_storageInstance->GetBufferMgr()->Read(g_defaultPdbId, page_id, LW_EXCLUSIVE);
    Page *page = (Page *)bufferDesc->GetPage();
    page->SetWalStreamId(2);
    uint64 preGlsn = page->GetGlsn();
    m_startReadPlsn = page->GetPlsn();
    MarkBufferDirty(bufferDesc);

    BufferDesc *bufferDesc2;
    PageId page_id2 = {UT_WAL_FILE0_ID, 3};
    bufferDesc2 = g_storageInstance->GetBufferMgr()->Read(g_defaultPdbId, page_id2, LW_EXCLUSIVE);
    Page *page2 = (Page *)bufferDesc2->GetPage();
    page2->SetWalStreamId(2);
    MarkBufferDirty(bufferDesc2);

    Xid xid = {10, 20};
    m_walWriter->BeginAtomicWal(xid);
    m_walWriter->RememberPageNeedWal(bufferDesc);
    m_walWriter->RememberPageNeedWal(bufferDesc2);
    WalRecordForPage *recordForPage = (WalRecordForPage *)(record);
    recordForPage->m_pageId = page_id;
    recordForPage->m_size = length;
    m_walWriter->PutNewWalRecord(record);
    recordForPage->m_pageId = page_id2;
    recordForPage->m_size = length;
    m_walWriter->PutNewWalRecord(record);
    m_walWriter->Append(record, length);
    /* Change wal id for test */
    m_walWriter->SetWalStream(INVALID_WAL_ID);
    insertResult = m_walWriter->EndAtomicWal();
    uint64 curGlsn = page->GetGlsn();

    ASSERT_EQ(page2->GetGlsn(), preGlsn + 1);
    ASSERT_EQ(page2->GetPlsn(), insertResult.m_endPlsn);
    ASSERT_EQ(page2->GetWalId(), insertResult.m_walId);
    EXPECT_NE(insertResult.m_endPlsn, INVALID_PLSN);
    ASSERT_EQ(curGlsn, preGlsn + 1);
    ASSERT_EQ(page->GetPlsn(), m_startReadPlsn + sizeof(WalRecordAtomicGroup) + length);
    ASSERT_EQ(page->GetWalId(), insertResult.m_walId);

    if (bufferDesc) {
        m_bufferMgr->MarkDirty(bufferDesc);
        g_storageInstance->GetBufferMgr()->UnlockAndRelease(bufferDesc);
        bufferDesc = nullptr;
    }
    if (bufferDesc2) {
        m_bufferMgr->MarkDirty(bufferDesc2);
        g_storageInstance->GetBufferMgr()->UnlockAndRelease(bufferDesc2);
        bufferDesc2 = nullptr;
    }

    UtMockModule::UtDropTableSpace(tablespace);
}

/* inserting logs remember again with same page */
TEST_F(WalStreamTest, RememberPageNeedWalTest002)
{
    EXPECT_NE(m_walWriter, nullptr);

    uint32 length = 5000;
    record = BuildWal(length);
    WalGroupLsnInfo insertResult = {INVALID_WAL_ID, INVALID_PLSN, INVALID_PLSN};

    int rc = 0;
    BufMgrInterface *m_bufferMgr = g_storageInstance->GetBufferMgr();
    TableSpace *tablespace = (TableSpace *)UtMockModule::UtGetTableSpace(2);
    FileSize fileSize;
    fileSize.initialFileSize = INIT_FILE_SIZE;
    fileSize.maxFileSize = MAX_FILE_SIZE;
    rc = tablespace->AddFile(UT_WAL_FILE0_ID, fileSize, nullptr, EXT_SIZE_8192);
    ASSERT_EQ(rc, 0);

    PageId page_id = {UT_WAL_FILE0_ID, 2};
    BufferDesc *bufferDesc;

    bufferDesc = g_storageInstance->GetBufferMgr()->Read(g_defaultPdbId, page_id, LW_EXCLUSIVE);
    Page *page = (Page *)bufferDesc->GetPage();
    page->SetWalStreamId(2);
    uint64 preGlsn = page->GetGlsn();
    MarkBufferDirty(bufferDesc);

    WalRecordForPage *recordForPage = (WalRecordForPage *)(record);
    recordForPage->m_pageId = page_id;
    recordForPage->m_size = length;
    recordForPage->m_type = DSTORE::WAL_UNDO_HEAP;

    Xid xid = {10, 20};
    m_walWriter->BeginAtomicWal(xid);
    m_walWriter->RememberPageNeedWal(bufferDesc);
    m_walWriter->PutNewWalRecord(record);
    m_walWriter->Append(record, length);
     /* Change wal id for test */
    m_walWriter->SetWalStream(INVALID_WAL_ID);
    insertResult = m_walWriter->EndAtomicWal();
    uint64 curGlsn = page->GetGlsn();

    ASSERT_EQ(curGlsn, preGlsn + 1);
    ASSERT_EQ(page->GetPlsn(), insertResult.m_endPlsn);
    ASSERT_EQ(page->GetWalId(), insertResult.m_walId);
    EXPECT_NE(insertResult.m_endPlsn, INVALID_PLSN);

    if (bufferDesc) {
        m_bufferMgr->MarkDirty(bufferDesc);
        g_storageInstance->GetBufferMgr()->UnlockAndRelease(bufferDesc);
        bufferDesc = nullptr;
    }

    UtMockModule::UtDropTableSpace(tablespace);
}

/* inserting logs with no remember */
TEST_F(WalStreamTest, RememberPageNeedWalTest003)
{
    EXPECT_NE(m_walWriter, nullptr);

    uint32 length = 5000;
    record = BuildWal(length);
    WalGroupLsnInfo insertResult = {INVALID_WAL_ID, INVALID_PLSN, INVALID_PLSN};

    int rc = 0;
    BufMgrInterface *m_bufferMgr = g_storageInstance->GetBufferMgr();
    TableSpace *tablespace = (TableSpace *)UtMockModule::UtGetTableSpace(2);
    FileSize fileSize;
    fileSize.initialFileSize = INIT_FILE_SIZE;
    fileSize.maxFileSize = MAX_FILE_SIZE;
    rc = tablespace->AddFile(UT_WAL_FILE0_ID, fileSize, nullptr, EXT_SIZE_8192);
    ASSERT_EQ(rc, 0);

    PageId page_id = {UT_WAL_FILE0_ID, 2};
    BufferDesc *bufferDesc;

    bufferDesc = g_storageInstance->GetBufferMgr()->Read(g_defaultPdbId, page_id, LW_EXCLUSIVE);
    Page *page = (Page *)bufferDesc->GetPage();
    page->SetWalStreamId(1);
    uint64 preGlsn = page->GetGlsn();
    MarkBufferDirty(bufferDesc);
    WalRecordForPage *recordForPage = (WalRecordForPage *)(record);
    recordForPage->m_pageId = page_id;
    recordForPage->m_size = length;
    recordForPage->m_type = DSTORE::WAL_UNDO_HEAP;
    m_walWriter->BeginAtomicWal(INVALID_XID);
    m_walWriter->RememberPageNeedWal(bufferDesc);
    m_walWriter->PutNewWalRecord(record);
    m_walWriter->Append(record, length);
    insertResult = m_walWriter->EndAtomicWal();
    uint64 curGlsn = page->GetGlsn();

    ASSERT_EQ(curGlsn, preGlsn);
    ASSERT_EQ(page->GetWalId(), insertResult.m_walId);
    EXPECT_NE(insertResult.m_endPlsn, INVALID_PLSN);

    if (bufferDesc) {
        m_bufferMgr->MarkDirty(bufferDesc);
        g_storageInstance->GetBufferMgr()->UnlockAndRelease(bufferDesc);
        bufferDesc = nullptr;
    }

    UtMockModule::UtDropTableSpace(tablespace);
}

/* inserting logs that remember before BeginAtomicWal(), core */
TEST_F(WalStreamTest, DISABLED_RememberPageNeedWalTest004)
{
    EXPECT_NE(m_walWriter, nullptr);

    uint32 length = 5000;
    record = BuildWal(length);
    WalGroupLsnInfo insertResult = {INVALID_WAL_ID, INVALID_PLSN, INVALID_PLSN};

    int rc = 0;
    BufMgrInterface *m_bufferMgr = g_storageInstance->GetBufferMgr();
    TableSpace *tablespace = (TableSpace *)UtMockModule::UtGetTableSpace(2);
    FileSize fileSize;
    fileSize.initialFileSize = INIT_FILE_SIZE;
    fileSize.maxFileSize = MAX_FILE_SIZE;
    rc = tablespace->AddFile(UT_WAL_FILE0_ID, fileSize, nullptr, EXT_SIZE_8192);
    ASSERT_EQ(rc, 0);

    PageId page_id = {UT_WAL_FILE0_ID, 2};
    BufferDesc *bufferDesc;

    bufferDesc = g_storageInstance->GetBufferMgr()->Read(g_defaultPdbId, page_id, LW_EXCLUSIVE);
    Page *page = (Page *)bufferDesc->GetPage();
    uint64 preGlsn = page->GetGlsn();
    MarkBufferDirty(bufferDesc);

    m_walWriter->RememberPageNeedWal(bufferDesc);
    m_walWriter->BeginAtomicWal(INVALID_XID);
    m_walWriter->PutNewWalRecord(record);
    m_walWriter->Append(record, length);
    insertResult = m_walWriter->EndAtomicWal();
    uint64 curGlsn = page->GetGlsn();

    ASSERT_EQ(curGlsn, preGlsn);
    ASSERT_EQ(page->GetPlsn(), insertResult.m_endPlsn);
    ASSERT_EQ(page->GetWalId(), insertResult.m_walId);
    EXPECT_NE(insertResult.m_endPlsn, INVALID_PLSN);

    if (bufferDesc) {
        m_bufferMgr->MarkDirty(bufferDesc);
        g_storageInstance->GetBufferMgr()->UnlockAndRelease(bufferDesc);
        bufferDesc = nullptr;
    }

    UtMockModule::UtDropTableSpace(tablespace);
}

/* begin-remember-put-remember-end, core */
TEST_F(WalStreamTest, DISABLED_RememberPageNeedWalTest005)
{
    EXPECT_NE(m_walWriter, nullptr);

    uint32 length = 5000;
    record = BuildWal(length);
    WalGroupLsnInfo insertResult = {INVALID_WAL_ID, INVALID_PLSN, INVALID_PLSN};

    int rc = 0;
    BufMgrInterface *m_bufferMgr = g_storageInstance->GetBufferMgr();
    TableSpace *tablespace = (TableSpace *)UtMockModule::UtGetTableSpace(2);
    FileSize fileSize;
    fileSize.initialFileSize = INIT_FILE_SIZE;
    fileSize.maxFileSize = MAX_FILE_SIZE;
    rc = tablespace->AddFile(UT_WAL_FILE0_ID, fileSize, nullptr, EXT_SIZE_8192);
    ASSERT_EQ(rc, 0);

    PageId page_id = {UT_WAL_FILE0_ID, 2};
    BufferDesc *bufferDesc;
    bufferDesc = g_storageInstance->GetBufferMgr()->Read(g_defaultPdbId, page_id, LW_EXCLUSIVE);
    Page *page = (Page *)bufferDesc->GetPage();
    uint64 preGlsn = page->GetGlsn();
    MarkBufferDirty(bufferDesc);

    BufferDesc *bufferDesc2;
    PageId page_id2 = {UT_WAL_FILE0_ID, 3};
    bufferDesc2 = g_storageInstance->GetBufferMgr()->Read(g_defaultPdbId, page_id2, LW_EXCLUSIVE);
    Page *page2 = (Page *)bufferDesc2->GetPage();
    MarkBufferDirty(bufferDesc2);

    m_walWriter->BeginAtomicWal(INVALID_XID);
    m_walWriter->RememberPageNeedWal(bufferDesc);
    m_walWriter->PutNewWalRecord(record);
    m_walWriter->RememberPageNeedWal(bufferDesc2);
    insertResult = m_walWriter->EndAtomicWal();
    uint64 curGlsn = page->GetGlsn();

    ASSERT_EQ(page2->GetGlsn(), preGlsn + 1);
    ASSERT_EQ(page2->GetPlsn(), insertResult.m_endPlsn);
    ASSERT_EQ(page2->GetWalId(), insertResult.m_walId);
    EXPECT_NE(insertResult.m_endPlsn, INVALID_PLSN);
    ASSERT_EQ(curGlsn, preGlsn + 1);
    ASSERT_EQ(page->GetPlsn(), insertResult.m_endPlsn);
    ASSERT_EQ(page->GetWalId(), insertResult.m_walId);

    if (bufferDesc) {
        m_bufferMgr->MarkDirty(bufferDesc);
        g_storageInstance->GetBufferMgr()->UnlockAndRelease(bufferDesc);
        bufferDesc = nullptr;
    }
    if (bufferDesc2) {
        m_bufferMgr->MarkDirty(bufferDesc2);
        g_storageInstance->GetBufferMgr()->UnlockAndRelease(bufferDesc2);
        bufferDesc2 = nullptr;
    }

    UtMockModule::UtDropTableSpace(tablespace);
}

TEST_F(WalStreamTest, DeleteDroppingWalStreamWhenCreateTest)
{
    WalId droppingWalId = STREAM_COUNT + 1;
    ControlWalStreamPageItemData *itemData =
        (ControlWalStreamPageItemData *) DstorePalloc0(sizeof(ControlWalStreamPageItemData));
    itemData->walId = droppingWalId;
    itemData->streamState = static_cast<uint8>(WalStreamState::CLOSE_DROPPING);
    WalCheckPoint walCheckPoint = {0, 0};
    itemData->lastWalCheckpoint = walCheckPoint;
    itemData->createFilePara.flag = APPEND_WRITE_FILE;
    itemData->createFilePara.rangeSize = WAL_FILE_RANGE_SIZE;
    itemData->createFilePara.maxSize = static_cast<uint64>(g_storageInstance->GetGuc()->walFileSize);
    itemData->createFilePara.mode = FILE_READ_AND_WRITE_MODE;
    itemData->createFilePara.isReplayWrite = false;
    TenantConfig *tenantConfig = g_storageInstance->GetGuc()->tenantConfig;
    StorageAssert(tenantConfig);
    errno_t rc = strcpy_s(itemData->createFilePara.storeSpaceName, STORESPACE_NAME_MAX_LEN,
                        tenantConfig->storeSpaces[0].storeSpaceName);
    storage_securec_check(rc, "\0", "\0");
    itemData->initWalFileCount = static_cast<uint32>(g_storageInstance->GetGuc()->walFileNumber);
    RetStatus retStatus = m_walControlFile->AddWalStream(*itemData);
    ASSERT_EQ(retStatus, DSTORE_SUCC);
    DstorePfreeExt(itemData);

    m_walStreamManager->DestroyWalStreams();
    m_walStreamManager->SetInitializeState(WalInitState::UNINITIALIZED);
    m_walStreamManager->Init(m_walControlFile);
    ControlWalStreamPageItemData *pageInfo = nullptr;
    ASSERT_EQ(m_walControlFile->GetWalStreamInfo(droppingWalId, &pageInfo), DSTORE_FAIL);
}

TEST_F(WalStreamTest, DeleteWalStreamTest)
{
    m_walStreamManager->DeleteWalStream(1);
    EXPECT_EQ(m_walStreamManager->GetTotalWalStreamsCount(), 0);
    EXPECT_EQ(m_walStreamManager->GetWalStream(1), nullptr);
}

TEST_F(WalStreamTest, TakeOverWalStreamWithoutFileTest)
{
    WalId droppingWalId = STREAM_COUNT + 1;
    ControlWalStreamPageItemData *itemData =
        (ControlWalStreamPageItemData *) DstorePalloc0(sizeof(ControlWalStreamPageItemData));
    itemData->walId = droppingWalId;
    itemData->streamState = static_cast<uint8>(WalStreamState::RECOVERY_DROPPING);
    WalCheckPoint walCheckPoint = {1000, 1000};
    itemData->lastWalCheckpoint = walCheckPoint;
    itemData->createFilePara.flag = APPEND_WRITE_FILE;
    itemData->createFilePara.rangeSize = WAL_FILE_RANGE_SIZE;
    itemData->createFilePara.maxSize = static_cast<uint64>(g_storageInstance->GetGuc()->walFileSize);
    itemData->createFilePara.mode = FILE_READ_AND_WRITE_MODE;
    itemData->createFilePara.isReplayWrite = false;
    TenantConfig *tenantConfig = g_storageInstance->GetGuc()->tenantConfig;
    StorageAssert(tenantConfig);
    errno_t rc = strcpy_s(itemData->createFilePara.storeSpaceName, STORESPACE_NAME_MAX_LEN,
                        tenantConfig->storeSpaces[0].storeSpaceName);
    storage_securec_check(rc, "\0", "\0");
    itemData->initWalFileCount = static_cast<uint32>(g_storageInstance->GetGuc()->walFileNumber);
    RetStatus retStatus = m_walControlFile->AddWalStream(*itemData);
    ASSERT_EQ(retStatus, DSTORE_SUCC);
    DstorePfreeExt(itemData);

    ASSERT_EQ(m_walStreamManager->TakeOverStreams(&droppingWalId, 1), DSTORE_SUCC);
}

TEST_F(WalStreamTest, DeleteAndIterateWalStreamConcurrentlyTest)
{
    int walStreamNum = 20;
    ControlWalStreamPageItemData streamInfo;
    WalCheckPoint walCheckPoint = {0, 0};
    streamInfo.lastWalCheckpoint = walCheckPoint;
    streamInfo.createFilePara.flag = APPEND_WRITE_FILE;
    streamInfo.createFilePara.rangeSize = WAL_FILE_RANGE_SIZE;
    streamInfo.createFilePara.maxSize = static_cast<uint64>(g_storageInstance->GetGuc()->walFileSize);
    streamInfo.createFilePara.mode = FILE_READ_AND_WRITE_MODE;
    streamInfo.createFilePara.isReplayWrite = false;
    TenantConfig *tenantConfig = g_storageInstance->GetGuc()->tenantConfig;
    StorageAssert(tenantConfig);
    errno_t rc = strcpy_s(streamInfo.createFilePara.storeSpaceName, STORESPACE_NAME_MAX_LEN,
                        tenantConfig->storeSpaces[0].storeSpaceName);
    storage_securec_check(rc, "\0", "\0");
    streamInfo.initWalFileCount = 1;
    for (int i = 1; i <= walStreamNum; i++) {
        streamInfo.walId = STREAM_COUNT + i;
        if (i % 5 == 0) {
            streamInfo.streamState = static_cast<uint8>(WalStreamState::USING);
        } else {
            streamInfo.streamState = static_cast<uint8>(WalStreamState::RECOVERY_DROPPING);
        }
        ASSERT_EQ(m_walStreamManager->LoadWalStreamForRead(streamInfo), DSTORE_SUCC);
    }
    m_walStreamManager->PrintAllWalStreams(DSTORE_LOG);

    std::thread deleteThread = std::thread([this]() {
        (void)g_storageInstance->CreateThreadAndRegister();
        dlist_mutable_iter iter = {};
        WalStreamNode *walStreamNode = nullptr;
        WalStream *walStream = nullptr;
        while ((walStreamNode =
            m_walStreamManager->GetNextWalStream(&iter, WalStream::IsWalStreamRecoveryDropping)) != nullptr) {
            walStream = walStreamNode->walStream;
            WalId walId = walStream->GetWalId();
            bool isStopRecycle = false;
            if (walStream->IsRecoveryRecycleFinish(&isStopRecycle) && !isStopRecycle) {
                m_walStreamManager->DeleteWalStream(walId);
                printf("delete walstream %lu success\n", walId);
            }
        }
        g_storageInstance->UnregisterThread();
    });
    std::thread iterThread = std::thread([this]() {
        (void)g_storageInstance->CreateThreadAndRegister();
        dlist_mutable_iter iter = {};
        WalStreamNode *walStreamNode = nullptr;
        WalStream *walStream = nullptr;
        bool getNextStream = false;
        uint8 count = 0;
        while ((walStreamNode = m_walStreamManager->GetNextWalStream(&iter, WalStream::IsWalStreamUsing)) != nullptr) {
            walStream = walStreamNode->walStream;
            printf("get walstream %lu success\n", walStream->GetWalId());
            GaussUsleep(1000);
            count++;
        }
        ASSERT_EQ(count, 5);
        g_storageInstance->UnregisterThread();
    });
    deleteThread.join();
    iterThread.join();
}
