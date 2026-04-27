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
#include <vector>

#include "ut_utilities/ut_dstore_framework.h"
#include "ut_mock/ut_mock.h"
#include "ut_buffer/ut_buffer_fake_instance.h"
#include "ut_wal/ut_wal_basic.h"

using namespace DSTORE;

class UTWalTest : public WALBASICTEST {
protected:
    void SetUp() override
    {
        WALBASICTEST::SetUp();
    }

    void TearDown() override
    {
        if (data != nullptr) {
            free(data);
            data = nullptr;
        }
        ClearDirtyFlags();
        WALBASICTEST::TearDown();
    }

protected:
    WalRecord *data = nullptr;
};

/* AtomicWalWriterContext BeginAtomicLog success at first call*/
TEST_F(UTWalTest, RedoInsertInit)
{
    ASSERT_NO_THROW(m_walWriter->BeginAtomicWal(INVALID_XID));
}

TEST_F (UTWalTest, SinglePutTest001)
{
    data = BuildWal(100);

    WalGroupLsnInfo walRecordGroupPtr = {INVALID_WAL_ID, INVALID_PLSN, INVALID_PLSN};
    for (int i = 0;i < 75; i++) {
        m_walWriter->BeginAtomicWal(INVALID_XID);
        m_walWriter->PutNewWalRecord(data);
        walRecordGroupPtr = m_walWriter->EndAtomicWal();
    }
    m_walWriter->WaitTargetPlsnPersist(walRecordGroupPtr.m_walId, walRecordGroupPtr.m_endPlsn);
}

TEST_F(UTWalTest, FileVersionCompressTest)
{
    uint32 walDataSize = sizeof(WalRecordForPage);
    WalRecordForPage *walDataPtr = BuildWalForPage(WAL_HEAP_INSERT, walDataSize, 1);
    WalRecord *logRecord = CompressRecord(walDataPtr);
    uint16 noVersionSize = logRecord->GetSize();
    // conpressed size should not include fileVersion size
    ASSERT_LT(noVersionSize, walDataSize - sizeof(uint64));
    printf("noVersionSize:%d, walDataSize:%d\n", noVersionSize, walDataSize);
    WalRecordForPage *walRecord = static_cast<WalRecordForPage *>(DecompressRecord(logRecord));
    DstorePfreeExt(logRecord);
    CheckWalRecordForPage(walRecord, walDataPtr);
    DstorePfreeExt(walDataPtr);
    DstorePfreeExt(walRecord);

    walDataPtr = BuildWalForPage(WAL_HEAP_INSERT, walDataSize, 2);
    logRecord = CompressRecord(walDataPtr);
    // conpressed size should include fileVersion size
    ASSERT_LT(logRecord->GetSize(), walDataSize);
    ASSERT_LT(noVersionSize, logRecord->GetSize());
    printf("logRecordSize:%d, walDataSize:%d\n", logRecord->GetSize(), walDataSize);
    walRecord = static_cast<WalRecordForPage *>(DecompressRecord(logRecord));
    DstorePfreeExt(logRecord);
    CheckWalRecordForPage(walRecord, walDataPtr);
    DstorePfreeExt(walDataPtr);
    DstorePfreeExt(walRecord);
}

TEST_F (UTWalTest, DISABLED_SetWalStreamTest001)
{
    EXPECT_NE(m_walWriter, nullptr);
    data = BuildWal(100);

    WalId walId = 0;
    WalGroupLsnInfo groupPtr;
    m_walWriter->BeginAtomicWal(INVALID_XID);
    m_walWriter->SetWalStream(walId);
    m_walWriter->PutNewWalRecord(data);
    groupPtr = m_walWriter->EndAtomicWal();
    ASSERT_EQ(groupPtr.m_walId, walId);
    EXPECT_NE(groupPtr.m_endPlsn, INVALID_PLSN);
}

TEST_F (UTWalTest, DISABLED_SetWalStreamTest002)
{
    EXPECT_NE(m_walWriter, nullptr);
    data = BuildWal(100);

    WalGroupLsnInfo groupPtr;
    m_walWriter->BeginAtomicWal(INVALID_XID);
    m_walWriter->SetWalStream(0);
    m_walWriter->SetWalStream(2); /* Wal stream count should more than two */
    m_walWriter->SetWalStream(1);
    m_walWriter->PutNewWalRecord(data);
    groupPtr = m_walWriter->EndAtomicWal();
    ASSERT_EQ(groupPtr.m_walId, 1);
    EXPECT_NE(groupPtr.m_endPlsn, INVALID_PLSN);
}

TEST_F (UTWalTest, DISABLED_SetWalStreamTest003)
{
    EXPECT_NE(m_walWriter, nullptr);
    data = BuildWal(100);

    WalGroupLsnInfo groupPtr;
    m_walWriter->BeginAtomicWal(INVALID_XID);
    /* If walId is INVALID_WAL_ID, a valid walId will be automatically assigned */
    m_walWriter->SetWalStream(INVALID_WAL_ID);
    m_walWriter->PutNewWalRecord(data);
    groupPtr = m_walWriter->EndAtomicWal();
    EXPECT_NE(groupPtr.m_walId, INVALID_WAL_ID);
    EXPECT_NE(groupPtr.m_endPlsn, INVALID_PLSN);
}

TEST_F (UTWalTest, DISABLED_SetWalStreamTest004)
{
    EXPECT_NE(m_walWriter, nullptr);
    data = BuildWal(100);

    WalGroupLsnInfo groupPtr;
    m_walWriter->BeginAtomicWal(INVALID_XID);
    /* Try to specify a invalid WalId will PANIC */
    m_walWriter->SetWalStream(1000); /* Wal stream count should lower than 1000 */
    m_walWriter->PutNewWalRecord(data);
    groupPtr = m_walWriter->EndAtomicWal();
}

TEST_F (UTWalTest, DISABLED_SetWalStreamTest005)
{
    EXPECT_NE(m_walWriter, nullptr);
    data = BuildWal(100);

    WalGroupLsnInfo groupPtr;
    m_walWriter->SetWalStream(0); /* Call this interface before BeginAtomicWal() will PANIC */
    m_walWriter->BeginAtomicWal(INVALID_XID);
    m_walWriter->PutNewWalRecord(data);
    groupPtr = m_walWriter->EndAtomicWal();
}

TEST_F (UTWalTest, DISABLED_SetWalStreamTest006)
{
    EXPECT_NE(m_walWriter, nullptr);
    data = BuildWal(100);

    WalGroupLsnInfo groupPtr;
    m_walWriter->BeginAtomicWal(INVALID_XID);
    m_walWriter->PutNewWalRecord(data);
    groupPtr = m_walWriter->EndAtomicWal();
    m_walWriter->SetWalStream(0); /* Call this interface before BeginAtomicWal(INVALID_XID) will PANIC */
}

TEST_F (UTWalTest, DISABLED_BeginAtomicLogTest001)
{
    EXPECT_NE(m_walWriter, nullptr);

    /* There is an error operation, shall print error message. */
    m_walWriter->BeginAtomicWal(INVALID_XID);
    m_walWriter->BeginAtomicWal(INVALID_XID);
}

TEST_F (UTWalTest, BeginAtomicLogTest002)
{
    EXPECT_NE(m_walWriter, nullptr);
    data = BuildWal(100);
    WalGroupLsnInfo insertResult = {INVALID_WAL_ID, INVALID_PLSN, INVALID_PLSN};

    m_walWriter->BeginAtomicWal(INVALID_XID);
    m_walWriter->EndAtomicWal();
    m_walWriter->BeginAtomicWal(INVALID_XID);
    m_walWriter->EndAtomicWal();

    m_walWriter->BeginAtomicWal(INVALID_XID);
    m_walWriter->PutNewWalRecord(data);
    insertResult = m_walWriter->EndAtomicWal();
}

TEST_F(UTWalTest, RememberPageNeedWalTest001)
{
    EXPECT_NE(m_walWriter, nullptr);

    data = BuildWal(100);
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

    m_walWriter->BeginAtomicWal(INVALID_XID);
    m_walWriter->RememberPageNeedWal(bufferDesc);
    m_walWriter->PutNewWalRecord(data);
    m_walWriter->Append(data, 100);
    insertResult = m_walWriter->EndAtomicWal();
    uint64 curGlsn = page->GetGlsn();

    /*
    ASSERT_EQ(curGlsn, preGlsn + 1);
    ASSERT_EQ(page->GetPlsn(), insertResult.m_endPlsn);
    ASSERT_EQ(page->GetWalId(), insertResult.walId);
    EXPECT_NE(insertResult.m_endPlsn, INVALID_PLSN);
     */

    if (bufferDesc) {
        m_bufferMgr->MarkDirty(bufferDesc);
        g_storageInstance->GetBufferMgr()->UnlockAndRelease(bufferDesc);
        bufferDesc = nullptr;
    }

    UtMockModule::UtDropTableSpace(tablespace);
}

TEST_F(UTWalTest, RememberPageNeedWalTest002)
{
    EXPECT_NE(m_walWriter, nullptr);

    data = BuildWal(100);

    int rc = 0;
    BufMgrInterface *m_bufferMgr = g_storageInstance->GetBufferMgr();
    TableSpace *tablespace = (TableSpace *)UtMockModule::UtGetTableSpace(2);
    FileSize fileSize;
    fileSize.initialFileSize = INIT_FILE_SIZE;
    fileSize.maxFileSize = MAX_FILE_SIZE;
    rc = tablespace->AddFile(UT_WAL_FILE0_ID, fileSize, nullptr, EXT_SIZE_8192);
    ASSERT_EQ(rc, 0);

    PageId page_id[10];
    BufferDesc *bufferDesc[10];
    Page *page[10];
    uint64 preGlsn[10];
    WalGroupLsnInfo insertResult[10];
    uint64 curGlsn[10];

    for (uint16 i = 0; i < 10; i++) {
        page_id[i].m_fileId = UT_WAL_FILE0_ID;
        page_id[i].m_blockId = i;
        insertResult[i].m_walId = INVALID_WAL_ID;
        insertResult[i].m_endPlsn = INVALID_PLSN;
        bufferDesc[i] = g_storageInstance->GetBufferMgr()->Read(g_defaultPdbId, page_id[i], LW_EXCLUSIVE);
        page[i] = (Page *)bufferDesc[i]->GetPage();
        preGlsn[i] = page[i]->GetGlsn();
        MarkBufferDirty(bufferDesc[i]);
    }

    for (uint16 i = 0; i < 10; i++) {
        m_walWriter->BeginAtomicWal(INVALID_XID);
        m_walWriter->RememberPageNeedWal(bufferDesc[i]);
        m_walWriter->PutNewWalRecord(data);
        m_walWriter->Append(data, 100);
        insertResult[i] = m_walWriter->EndAtomicWal();
        curGlsn[i] = page[i]->GetGlsn();
    }

    for (uint16 i = 0; i < 10; i++) {
        /*
        ASSERT_EQ(curGlsn[i], preGlsn[i] + 1);
        ASSERT_EQ(page[i]->GetPlsn(), insertResult[i].m_endPlsn);
        ASSERT_EQ(page[i]->GetWalId(), insertResult[i].walId);
        EXPECT_NE(insertResult[i].m_endPlsn, INVALID_PLSN);
         */
        if (bufferDesc[i]) {
            m_bufferMgr->MarkDirty(bufferDesc[i]);
            g_storageInstance->GetBufferMgr()->UnlockAndRelease(bufferDesc[i]);
            bufferDesc[i] = nullptr;
        }
    }

    UtMockModule::UtDropTableSpace(tablespace);
}

TEST_F(UTWalTest, RememberPageNeedWalTest003)
{
    EXPECT_NE(m_walWriter, nullptr);

    data = BuildWal(100);
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

    m_walWriter->BeginAtomicWal(INVALID_XID);
    for (uint16 i = 0; i < 1; i++) {
        m_walWriter->RememberPageNeedWal(bufferDesc);
        m_walWriter->PutNewWalRecord(data);
        m_walWriter->Append(data, 100);
    }
    insertResult = m_walWriter->EndAtomicWal();

    /*
    ASSERT_EQ(page->GetGlsn(), preGlsn + 1);
    ASSERT_EQ(page->GetPlsn(), insertResult.m_endPlsn);
    ASSERT_EQ(page->GetWalId(), insertResult.walId);
    EXPECT_NE(insertResult.m_endPlsn, INVALID_PLSN);
     */

    if (bufferDesc) {
        m_bufferMgr->MarkDirty(bufferDesc);
        g_storageInstance->GetBufferMgr()->UnlockAndRelease(bufferDesc);
        bufferDesc = nullptr;
    }

    UtMockModule::UtDropTableSpace(tablespace);
}

TEST_F(UTWalTest, RememberPageNeedWalTest004)
{
    EXPECT_NE(m_walWriter, nullptr);

    data = BuildWal(100);
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

    m_walWriter->BeginAtomicWal(INVALID_XID);
    for (uint16 i = 0; i < 1; i++) {
        m_walWriter->RememberPageNeedWal(bufferDesc);
        m_walWriter->PutNewWalRecord(data);
        m_walWriter->Append(data, 100);
    }
    insertResult = m_walWriter->EndAtomicWal();

    /*
    ASSERT_EQ(page->GetGlsn(), preGlsn + 1);
    ASSERT_EQ(page->GetPlsn(), insertResult.m_endPlsn);
    ASSERT_EQ(page->GetWalId(), insertResult.walId);
    EXPECT_NE(insertResult.m_endPlsn, INVALID_PLSN);
     */

    if (bufferDesc) {
        m_bufferMgr->MarkDirty(bufferDesc);
        g_storageInstance->GetBufferMgr()->UnlockAndRelease(bufferDesc);
        bufferDesc = nullptr;
    }

    UtMockModule::UtDropTableSpace(tablespace);
}

TEST_F(UTWalTest, DISABLED_RememberPageNeedWalTest005)
{
    EXPECT_NE(m_walWriter, nullptr);

    data = BuildWal(100);
    WalGroupLsnInfo insertResult = {INVALID_WAL_ID, INVALID_PLSN, INVALID_PLSN};

    BufferDesc *bufferDesc = nullptr;

    m_walWriter->BeginAtomicWal(INVALID_XID);
    m_walWriter->RememberPageNeedWal(bufferDesc); /* bufferDesc is null shall PANIC */
    m_walWriter->PutNewWalRecord(data);
    m_walWriter->Append(data, 100);
    insertResult = m_walWriter->EndAtomicWal();
}

TEST_F(UTWalTest, DISABLED_RememberPageNeedWalTest006)
{
    EXPECT_NE(m_walWriter, nullptr);

    data = BuildWal(100);
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

    m_walWriter->RememberPageNeedWal(bufferDesc); /* Call this interface before BeginAtomicWal() shall PANIC */
    m_walWriter->BeginAtomicWal(INVALID_XID);
    m_walWriter->PutNewWalRecord(data);
    m_walWriter->Append(data, 100);
    insertResult = m_walWriter->EndAtomicWal();
    uint64 curGlsn = page->GetGlsn();

    if (bufferDesc) {
        m_bufferMgr->MarkDirty(bufferDesc);
        g_storageInstance->GetBufferMgr()->UnlockAndRelease(bufferDesc);
        bufferDesc = nullptr;
    }

    UtMockModule::UtDropTableSpace(tablespace);
}

TEST_F(UTWalTest, DISABLED_RememberPageNeedWalTest007)
{
    EXPECT_NE(m_walWriter, nullptr);

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
    MarkBufferDirty(bufferDesc);

    m_walWriter->BeginAtomicWal(INVALID_XID);
    m_walWriter->RememberPageNeedWal(bufferDesc); /* Call this interface but never PutNewWalRecord() shall PANIC */
    insertResult = m_walWriter->EndAtomicWal();

    if (bufferDesc) {
        m_bufferMgr->MarkDirty(bufferDesc);
        g_storageInstance->GetBufferMgr()->UnlockAndRelease(bufferDesc);
        bufferDesc = nullptr;
    }

    UtMockModule::UtDropTableSpace(tablespace);
}

TEST_F(UTWalTest, DISABLED_RememberPageNeedWalTest008)
{
    EXPECT_NE(m_walWriter, nullptr);

    data = BuildWal(100);

    int rc = 0;
    BufMgrInterface *m_bufferMgr = g_storageInstance->GetBufferMgr();
    TableSpace *tablespace = (TableSpace *)UtMockModule::UtGetTableSpace(2);
    FileSize fileSize;
    fileSize.initialFileSize = INIT_FILE_SIZE;
    fileSize.maxFileSize = MAX_FILE_SIZE;
    rc = tablespace->AddFile(UT_WAL_FILE0_ID, fileSize, nullptr, EXT_SIZE_8192);
    ASSERT_EQ(rc, 0);

    PageId page_id[10];
    BufferDesc *bufferDesc[10];
    Page *page[10];
    WalGroupLsnInfo insertResult = {INVALID_WAL_ID, INVALID_PLSN, INVALID_PLSN};

    for (uint16 i = 0; i < 10; i++) {
        page_id[i].m_fileId = UT_WAL_FILE0_ID;
        page_id[i].m_blockId = i;
        bufferDesc[i] = g_storageInstance->GetBufferMgr()->Read(g_defaultPdbId, page_id[i], LW_EXCLUSIVE);
        page[i] = (Page *)bufferDesc[i]->GetPage();
        MarkBufferDirty(bufferDesc[i]);
    }

    m_walWriter->BeginAtomicWal(INVALID_XID);
    for (uint16 i = 0; i < 10; i++) { /* Page number more than MAX_PAGES_COUNT_PER_WAL_GROUP shall PANIC */
        m_walWriter->RememberPageNeedWal(bufferDesc[i]);
        m_walWriter->PutNewWalRecord(data);
        m_walWriter->Append(data, 100);
    }
    insertResult = m_walWriter->EndAtomicWal();

    for (uint16 i = 0; i < 10; i++) {
        if (bufferDesc[i]) {
            m_bufferMgr->MarkDirty(bufferDesc[i]);
            g_storageInstance->GetBufferMgr()->UnlockAndRelease(bufferDesc[i]);
            bufferDesc[i] = nullptr;
        }
    }

    UtMockModule::UtDropTableSpace(tablespace);
}

TEST_F (UTWalTest, PutNewWalRecordTest001)
{
    EXPECT_NE(m_walWriter, nullptr);

    data = BuildWal(100);

    WalGroupLsnInfo insertResult = {INVALID_WAL_ID, INVALID_PLSN, INVALID_PLSN};

    m_walWriter->BeginAtomicWal(INVALID_XID);
    m_walWriter->PutNewWalRecord(data);
    m_walWriter->PutNewWalRecord(data);
    insertResult = m_walWriter->EndAtomicWal();
}

TEST_F (UTWalTest, DISABLED_PutNewWalRecordTest002)
{
    EXPECT_NE(m_walWriter, nullptr);

    data = BuildWal(100);
    WalGroupLsnInfo insertResult = {INVALID_WAL_ID, INVALID_PLSN, INVALID_PLSN};

    m_walWriter->BeginAtomicWal(INVALID_XID);
    m_walWriter->PutNewWalRecord(data);
    m_walWriter->EndAtomicWal();

    /* BeginAtomicWal() is not called, so both PutNewWalRecord() and EndAtomicWal() should output error message. */
    m_walWriter->PutNewWalRecord(data);
    insertResult = m_walWriter->EndAtomicWal();
}

TEST_F (UTWalTest, PutNewWalRecordTest003)
{
    EXPECT_NE(m_walWriter, nullptr);

    data = BuildWal(5000);
    WalGroupLsnInfo insertResult = {INVALID_WAL_ID, INVALID_PLSN, INVALID_PLSN};

    for (uint16 i = 0; i < 50; i++) {
        m_walWriter->BeginAtomicWal(INVALID_XID);

        /*
         * The private buf init size is 4096 byte, this test verify whether the PutNewWalRecord()
         * is ok when put large data.
         */
        m_walWriter->PutNewWalRecord(data);
        m_walWriter->Append(data, 5000);
        insertResult = m_walWriter->EndAtomicWal();
    }
}

TEST_F (UTWalTest, DISABLED_PutNewWalRecordTest004)
{
    EXPECT_NE(m_walWriter, nullptr);

    m_walWriter->BeginAtomicWal(INVALID_XID);
    m_walWriter->PutNewWalRecord(nullptr); /* data is nullptr shall PANIC */
    m_walWriter->EndAtomicWal();
}

TEST_F (UTWalTest, DISABLED_PutNewWalRecordTest005)
{
    EXPECT_NE(m_walWriter, nullptr);

    data = BuildWal(100);
    data->SetSize(0);

    m_walWriter->BeginAtomicWal(INVALID_XID);
    m_walWriter->PutNewWalRecord(data); /* length is 0 shall PANIC */
    m_walWriter->EndAtomicWal();
}

TEST_F (UTWalTest, DISABLED_PutNewWalRecordTest006)
{
    EXPECT_NE(m_walWriter, nullptr);

    data = BuildWal(100);
    data->SetSize(WAL_GROUP_MAX_SIZE + 1);
    WalGroupLsnInfo insertResult = {INVALID_WAL_ID, INVALID_PLSN, INVALID_PLSN};

    m_walWriter->BeginAtomicWal(INVALID_XID);
    /* length bigger than WAL_GROUP_MAX_SIZE shall PANIC */
    m_walWriter->PutNewWalRecord(data); /* Panic here */
    insertResult = m_walWriter->EndAtomicWal();
}

TEST_F (UTWalTest, DISABLED_PutNewWalRecordTest007)
{
    EXPECT_NE(m_walWriter, nullptr);

    data = BuildWal(100);
    WalGroupLsnInfo insertResult = {INVALID_WAL_ID, INVALID_PLSN, INVALID_PLSN};

    m_walWriter->BeginAtomicWal(INVALID_XID);
    m_walWriter->PutNewWalRecord(data);
    data->SetSize(WAL_GROUP_MAX_SIZE + 1);
    /* Total Wal group length bigger than WAL_GROUP_MAX_SIZE shall PANIC */
    m_walWriter->PutNewWalRecord(data); /* Panic here */
    insertResult = m_walWriter->EndAtomicWal();
}

TEST_F (UTWalTest, AppendTest001)
{
    EXPECT_NE(m_walWriter, nullptr);

    uint32 length = 100;
    uint32 length3 = 5000;
    WalRecord *data1 = BuildWal(100);
    WalRecord *data2 = BuildWal(100);
    WalRecord *data3 = BuildWal(5000);


    WalGroupLsnInfo insertResult1 = {INVALID_WAL_ID, INVALID_PLSN, INVALID_PLSN};
    WalGroupLsnInfo insertResult2 = insertResult1;

    /* verify Append() Interface */
    m_walWriter->BeginAtomicWal(INVALID_XID);
    m_walWriter->PutNewWalRecord(data1);
    m_walWriter->Append(data2, length);
    m_walWriter->Append(data3, length3);
    m_walWriter->PutNewWalRecord(data2);
    m_walWriter->Append(data1, length);
    m_walWriter->PutNewWalRecord(data2);
    m_walWriter->Append(data3, length3);
    m_walWriter->Append(data1, length);
    insertResult1 = m_walWriter->EndAtomicWal();

    m_walWriter->BeginAtomicWal(INVALID_XID);
    m_walWriter->PutNewWalRecord(data2);
    m_walWriter->Append(data3, length);
    m_walWriter->Append(data1, length);
    insertResult2 = m_walWriter->EndAtomicWal();

    free(data1);
    free(data2);
    free(data3);
}

TEST_F (UTWalTest, DISABLED_AppendTest002)
{
    EXPECT_NE(m_walWriter, nullptr);

    char data[5] = {'a', 'b', 'c', 'd', '\0'};
    uint16 length = sizeof(data);

    m_walWriter->BeginAtomicWal(INVALID_XID);
    m_walWriter->Append(data, length);
}

TEST_F (UTWalTest, DISABLED_AppendTest003)
{
    EXPECT_NE(m_walWriter, nullptr);

    data = BuildWal(100);
    WalGroupLsnInfo insertResult = {INVALID_WAL_ID, INVALID_PLSN, INVALID_PLSN};

    m_walWriter->BeginAtomicWal(INVALID_XID);
    m_walWriter->PutNewWalRecord(data);
    insertResult = m_walWriter->EndAtomicWal();
    m_walWriter->Append(data, 100);
}

TEST_F (UTWalTest, DISABLED_AppendTest004)
{
    EXPECT_NE(m_walWriter, nullptr);

    data = BuildWal(100);

    m_walWriter->BeginAtomicWal(INVALID_XID);
    m_walWriter->PutNewWalRecord(data);
    m_walWriter->Append(nullptr, 100); /* data is nullptr shall PANIC */
    m_walWriter->EndAtomicWal();
}

TEST_F (UTWalTest, DISABLED_AppendTest005)
{
    EXPECT_NE(m_walWriter, nullptr);

    data = BuildWal(100);

    m_walWriter->BeginAtomicWal(INVALID_XID);
    m_walWriter->PutNewWalRecord(data);
    m_walWriter->Append(data, 0); /* length is 0 shall PANIC */
    m_walWriter->EndAtomicWal();
}

TEST_F (UTWalTest, DISABLED_AppendTest006)
{
    EXPECT_NE(m_walWriter, nullptr);

    data = BuildWal(100);
    uint32 appendLen = WAL_GROUP_MAX_SIZE + 1;
    WalGroupLsnInfo insertResult = {INVALID_WAL_ID, INVALID_PLSN, INVALID_PLSN};

    m_walWriter->BeginAtomicWal(INVALID_XID);
    m_walWriter->PutNewWalRecord(data);
    /* Append data length bigger than WAL_GROUP_MAX_SIZE shall PANIC */
    m_walWriter->Append(data, appendLen); /* Panic here */
    insertResult = m_walWriter->EndAtomicWal();
}

TEST_F (UTWalTest, DISABLED_AppendTest007)
{
    EXPECT_NE(m_walWriter, nullptr);

    uint32 appendLen = WAL_GROUP_MAX_SIZE / 2;

    data = BuildWal(100);
    WalGroupLsnInfo insertResult = {INVALID_WAL_ID, INVALID_PLSN, INVALID_PLSN};

    m_walWriter->BeginAtomicWal(INVALID_XID);
    m_walWriter->PutNewWalRecord(data);
    m_walWriter->Append(data, appendLen);
    /* Total Wal group length bigger than WAL_GROUP_MAX_SIZE shall PANIC */
    m_walWriter->Append(data, appendLen); /* Panic here */
    insertResult = m_walWriter->EndAtomicWal();
}

TEST_F(UTWalTest, DISABLED_EndTest001)
{
    EXPECT_NE(m_walWriter, nullptr);

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
    MarkBufferDirty(bufferDesc);

    m_walWriter->BeginAtomicWal(INVALID_XID);
    m_walWriter->RememberPageNeedWal(bufferDesc); /* Call this interface but don't call PutNewWalRecord() will assert */
    insertResult = m_walWriter->EndAtomicWal();

    if (bufferDesc) {
        g_storageInstance->GetBufferMgr()->UnlockAndRelease(bufferDesc);
        bufferDesc = nullptr;
    }

    UtMockModule::UtDropTableSpace(tablespace);
}

TEST_F (UTWalTest, DISABLED_EndTest002)
{
    EXPECT_NE(m_walWriter, nullptr);

    m_walWriter->EndAtomicWal();
    m_walWriter->EndAtomicWal();
}

TEST_F (UTWalTest, AtomicInsertOneWalTest001)
{
    EXPECT_NE(m_walWriter, nullptr);

    WalRecord *data1 = BuildWal(1000);
    WalRecord *data2 = BuildWal(2000);

    for (uint16 i = 0; i < 50; i++) {
        m_walWriter->AtomicInsertOneWal(data1);
        m_walWriter->AtomicInsertOneWal(data2);
    }

    m_walWriter->BeginAtomicWal(INVALID_XID);
    m_walWriter->PutNewWalRecord(data1);
    m_walWriter->Append(data2, 2000);
    m_walWriter->EndAtomicWal();

    free(data1);
    free(data2);
}

TEST_F (UTWalTest, DISABLED_AtomicInsertOneWalTest002)
{
    EXPECT_NE(m_walWriter, nullptr);

    WalRecord *data = nullptr;

    m_walWriter->AtomicInsertOneWal(data); /* data is nullptr shall PANIC */
}

TEST_F (UTWalTest, DISABLED_AtomicInsertOneWalTest003)
{
    EXPECT_NE(m_walWriter, nullptr);

    data = BuildWal(100);
    data->SetSize(0);

    m_walWriter->AtomicInsertOneWal(data); /* length is 0 shall PANIC */
}

TEST_F (UTWalTest, DISABLED_AtomicInsertOneWalTest004)
{
    EXPECT_NE(m_walWriter, nullptr);

    data = BuildWal(100);

    m_walWriter->BeginAtomicWal(INVALID_XID);
    m_walWriter->AtomicInsertOneWal(data); /* Shall PANIC */
}

TEST_F (UTWalTest, DISABLED_AtomicInsertOneWalTest005)
{
    EXPECT_NE(m_walWriter, nullptr);

    data = BuildWal(100);

    m_walWriter->AtomicInsertOneWal(data); /* Shall PANIC */
    m_walWriter->EndAtomicWal(); /* error condition */
}
