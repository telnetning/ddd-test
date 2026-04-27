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
#include "control/dstore_control_cache.h"
#include "control/dstore_control_file_mgr.h"
#include "control/dstore_control_file.h"
#include "common/memory/dstore_mctx.h"
#include <thread>
#include <chrono>
#include <string>
#include <unordered_set>
 
using namespace DSTORE;

class UTControlFileMgrTest : public DSTORETEST {
 
protected:
    void SetUp() override {
        DSTORETEST::SetUp();
        UtInstance *instance = DstoreNew(m_ut_memory_context)UtInstance();
        instance->Install(&DSTORETEST::m_guc, m_ut_memory_context);
        instance->Startup(&DSTORETEST::m_guc);
        m_controlFile = g_storageInstance->GetPdb(g_defaultPdbId)->GetControlFile();
        m_controlFileMgr = m_controlFile->m_controlFileMgr;
    }
 
    void TearDown() override {
        UtInstance *instance = (UtInstance *)g_storageInstance;
        instance->Shutdown();
        delete instance;
        DSTORETEST::TearDown();
    }

    ControlFile *m_controlFile;
    ControlFileMgr *m_controlFileMgr;
};

TEST_F(UTControlFileMgrTest, ControlFilMgrTest_level0)
{
    ASSERT_NE(m_controlFileMgr, nullptr);
    RetStatus ret = m_controlFileMgr->CreateControlFiles();
    ASSERT_EQ(ret, DSTORE_FAIL);
    ret = m_controlFileMgr->OpenControlFiles();
    ASSERT_EQ(ret, DSTORE_SUCC);
    ret = m_controlFileMgr->LoadControlFile();
    ASSERT_EQ(ret, DSTORE_SUCC);
    ret = m_controlFileMgr->CloseControlFiles();
    ASSERT_EQ(ret, DSTORE_SUCC);
    ret = m_controlFileMgr->OpenControlFiles();
    ASSERT_EQ(ret, DSTORE_SUCC);
    PageHandle *pageHandle =
        static_cast<PageHandle *>(DstoreMemoryContextAllocZero(m_ut_memory_context, sizeof(PageHandle)));
    ret = m_controlFileMgr->GetValidMetaPage(pageHandle, CONTROLFILE_PAGEMAP_TABLESPACE_META);
    ASSERT_EQ(ret, DSTORE_SUCC);
    ASSERT_EQ(pageHandle->file, m_controlFileMgr->m_file1);
    ASSERT_EQ(pageHandle->checkResult, BOTH_META_PAGES_ARE_VALID);
    ASSERT_EQ(pageHandle->numDirtyBlocks, 0);
    char *page = m_controlFileMgr->ReadOnePage(pageHandle, CONTROLFILE_PAGEMAP_TABLESPACE_START);
    bool isValid = m_controlFileMgr->IsPageStateValid(CONTROLFILE_PAGEMAP_TABLESPACE_START);
    ASSERT_EQ(isValid, true);
    ASSERT_EQ(pageHandle->maxSize, 0);
    ret = m_controlFileMgr->MarkPageDirty(pageHandle, CONTROLFILE_PAGEMAP_TABLESPACE_START);
    ASSERT_EQ(ret, DSTORE_SUCC);
    ASSERT_EQ(pageHandle->numDirtyBlocks, 1);
    ASSERT_NE(pageHandle->dirtyBlocks, nullptr);
    ASSERT_EQ(pageHandle->maxSize, 10);
    bool isDirty = m_controlFileMgr->IsPageStateDirty(CONTROLFILE_PAGEMAP_TABLESPACE_START);
    ASSERT_EQ(isDirty, true);
    uint64 term = ((ControlMetaPage *)page)->GetTerm();
    ret = m_controlFileMgr->PostPageHandle(pageHandle, CONTROLFILE_PAGEMAP_TABLESPACE_START);
    ASSERT_EQ(ret, DSTORE_SUCC);
    uint64 term2 = ((ControlMetaPage *)page)->GetTerm();
    ASSERT_EQ(term + 1, term2);
    isDirty = m_controlFileMgr->IsPageStateDirty(CONTROLFILE_PAGEMAP_TABLESPACE_START);
    ASSERT_EQ(isDirty, false);
    DstorePfreeExt(pageHandle);
}

TEST_F(UTControlFileMgrTest, ControlFilCacheTest_level0)
{
    RetStatus ret = m_controlFileMgr->OpenControlFiles();
    ASSERT_EQ(ret, DSTORE_SUCC);
    PageHandle *pageHandle =
        static_cast<PageHandle *>(DstoreMemoryContextAllocZero(m_ut_memory_context, sizeof(PageHandle)));
    ret = m_controlFileMgr->GetValidMetaPage(pageHandle, CONTROLFILE_PAGEMAP_METAPAGE_MAX);
    ASSERT_EQ(ret, DSTORE_SUCC);
    ASSERT_EQ(pageHandle->file, m_controlFileMgr->m_file1);
    ASSERT_EQ(pageHandle->checkResult, BOTH_META_PAGES_ARE_VALID);
    ASSERT_EQ(pageHandle->numDirtyBlocks, 0);
    m_controlFileMgr->CleanPageStateValid(CONTROLFILE_PAGEMAP_METAPAGE_MAX);
    m_controlFileMgr->SetEnableCachePage(false);
    char *page = m_controlFileMgr->ReadOnePage(pageHandle, CONTROLFILE_PAGEMAP_METAPAGE_MAX);
    bool isValid = m_controlFileMgr->IsPageStateValid(CONTROLFILE_PAGEMAP_METAPAGE_MAX);
    ASSERT_EQ(isValid, false);
    ((ControlMetaPage *)page)->SetTerm(10000);
    ret = m_controlFileMgr->MarkPageDirty(pageHandle, CONTROLFILE_PAGEMAP_METAPAGE_MAX);
    ASSERT_EQ(ret, DSTORE_SUCC);
    bool isDirty = m_controlFileMgr->IsPageStateDirty(CONTROLFILE_PAGEMAP_METAPAGE_MAX);
    ASSERT_EQ(isDirty, true);
    char *page2 = m_controlFileMgr->ReadOnePage(pageHandle, CONTROLFILE_PAGEMAP_METAPAGE_MAX);
    uint64 term2 = ((ControlMetaPage *)page2)->GetTerm();
    ASSERT_EQ(term2, 10000);
    ret = m_controlFileMgr->PostPageHandle(pageHandle, CONTROLFILE_PAGEMAP_METAPAGE_MAX);
    ASSERT_EQ(ret, DSTORE_SUCC);
    char *page3 = m_controlFileMgr->ReadOnePage(pageHandle, CONTROLFILE_PAGEMAP_METAPAGE_MAX);
    uint64 term3 = ((ControlMetaPage *)page3)->GetTerm();
    ASSERT_EQ(term3, 10001);
    isValid = m_controlFileMgr->IsPageStateValid(CONTROLFILE_PAGEMAP_METAPAGE_MAX);
    ASSERT_EQ(isValid, false);
    isDirty = m_controlFileMgr->IsPageStateDirty(CONTROLFILE_PAGEMAP_METAPAGE_MAX);
    ASSERT_EQ(isDirty, false);
    m_controlFileMgr->SetEnableCachePage(true);
    isValid = m_controlFileMgr->IsPageStateValid(CONTROLFILE_PAGEMAP_METAPAGE_MAX);
    ASSERT_EQ(isValid, false);
    m_controlFileMgr->ReadOnePage(pageHandle, CONTROLFILE_PAGEMAP_METAPAGE_MAX);
    isValid = m_controlFileMgr->IsPageStateValid(CONTROLFILE_PAGEMAP_METAPAGE_MAX);
    ASSERT_EQ(isValid, true);
    DstorePfreeExt(pageHandle);
}

TEST_F(UTControlFileMgrTest, ControlFilMgrFaultInjectionTest1_level0)
{
    ASSERT_NE(m_controlFileMgr, nullptr);
    RetStatus ret = m_controlFileMgr->OpenControlFiles();
    ASSERT_EQ(ret, DSTORE_SUCC);
    m_controlFileMgr->CleanPageStateValid(CONTROL_TBS_METAPAGE_TYPE);
    m_controlFileMgr->SetEnableCachePage(false);
    PageHandle *pageHandle =
        static_cast<PageHandle *>(DstoreMemoryContextAllocZero(m_ut_memory_context, sizeof(PageHandle)));
    ASSERT_NE(pageHandle, nullptr);
    ret = m_controlFileMgr->GetValidMetaPage(pageHandle, CONTROL_TBS_METAPAGE_TYPE);
    ASSERT_EQ(ret, DSTORE_SUCC);
    ASSERT_EQ(pageHandle->file, m_controlFileMgr->m_file1);
    ASSERT_EQ(pageHandle->numDirtyBlocks, 0);
    char *page = m_controlFileMgr->ReadOnePage(pageHandle, CONTROL_TBS_METAPAGE_TYPE);
    uint32 checkSum = ((ControlBasePage *)page)->GetCheckSum();
    ((ControlBasePage *)page)->SetCheckSum(checkSum + 100);
    ControlDiskFile *file1 = m_controlFileMgr->GetFile1();
    ret = file1->WritePage(CONTROL_TBS_METAPAGE_TYPE, page);
    ASSERT_EQ(ret, DSTORE_SUCC);
    ret = file1->Fsync();
    ASSERT_EQ(ret, DSTORE_SUCC);
    char *page2 = m_controlFileMgr->ReadOnePage(pageHandle, CONTROL_TBS_METAPAGE_TYPE);
    uint32 checkSum2 = ((ControlBasePage *)page2)->GetCheckSum();
    ASSERT_EQ(checkSum2, checkSum);
    ASSERT_EQ(pageHandle->file, m_controlFileMgr->m_file2);
    ret = m_controlFileMgr->PostPageHandle(pageHandle, CONTROL_TBS_METAPAGE_TYPE);
    ASSERT_EQ(ret, DSTORE_SUCC);
    ASSERT_EQ(pageHandle->file, m_controlFileMgr->m_file1);
    char *page3 = m_controlFileMgr->ReadOnePage(pageHandle, CONTROL_TBS_METAPAGE_TYPE);
    uint32 checkSum3 = ((ControlBasePage *)page3)->GetCheckSum();
    /* we need check checkSums here after recovery */
    DstorePfreeExt(pageHandle);
}

TEST_F(UTControlFileMgrTest, ControlFilMgrFaultInjectionTest2_level0)
{
    ASSERT_NE(m_controlFileMgr, nullptr);
    RetStatus ret = m_controlFileMgr->OpenControlFiles();
    ASSERT_EQ(ret, DSTORE_SUCC);
    m_controlFileMgr->CleanPageStateValid(CONTROL_TBS_METAPAGE_TYPE);
    m_controlFileMgr->SetEnableCachePage(false);
    PageHandle *pageHandle =
        static_cast<PageHandle *>(DstoreMemoryContextAllocZero(m_ut_memory_context, sizeof(PageHandle)));
    ASSERT_NE(pageHandle, nullptr);
    ret = m_controlFileMgr->GetValidMetaPage(pageHandle, CONTROL_TBS_METAPAGE_TYPE);
    ASSERT_EQ(ret, DSTORE_SUCC);
    ASSERT_EQ(pageHandle->file, m_controlFileMgr->m_file1);
    ASSERT_EQ(pageHandle->numDirtyBlocks, 0);
    char *page1 = m_controlFileMgr->ReadOnePage(pageHandle, CONTROL_TBS_METAPAGE_TYPE);
    uint32 checkSum = ((ControlBasePage *)page1)->GetCheckSum();
    ((ControlMetaPage *)page1)->SetCheckSum(checkSum + 100);
    ((ControlMetaPage *)page1)->SetTerm(100);
    ControlDiskFile *file1 = m_controlFileMgr->GetFile1();
    ret = file1->WritePage(CONTROL_TBS_METAPAGE_TYPE, page1);
    ASSERT_EQ(ret, DSTORE_SUCC);
    ret = file1->Fsync();
    ASSERT_EQ(ret, DSTORE_SUCC);
    ControlDiskFile *file2 = m_controlFileMgr->GetFile2();
    ret = file2->WritePage(CONTROL_TBS_METAPAGE_TYPE, page1);
    ASSERT_EQ(ret, DSTORE_SUCC);
    ret = file2->Fsync();
    ASSERT_EQ(ret, DSTORE_SUCC);
    bool isFile1Valid = false;
    bool isFile1Writing = false;
    uint64 file1Term = 0;
    ret = m_controlFileMgr->CheckMetaPage(&isFile1Valid, &isFile1Writing, &file1Term, CONTROL_TBS_METAPAGE_TYPE, file1);
    ASSERT_EQ(ret, DSTORE_SUCC);
    ASSERT_EQ(isFile1Valid, false);
    ASSERT_EQ(isFile1Writing, false);
    ASSERT_EQ(file1Term, 100);
    bool isFile2Valid = false;
    bool isFile2Writing = false;
    uint64 file2Term = 0;
    ret = m_controlFileMgr->CheckMetaPage(&isFile2Valid, &isFile2Writing, &file2Term, CONTROL_TBS_METAPAGE_TYPE, file2);
    ASSERT_EQ(ret, DSTORE_SUCC);
    ASSERT_EQ(isFile2Valid, false);
    ASSERT_EQ(isFile1Writing, false);
    ASSERT_EQ(file2Term, 100);
    DstorePfreeExt(pageHandle);
}

TEST_F(UTControlFileMgrTest, ControlFilMgrFaultInjectionTest3_level0)
{
    ASSERT_NE(m_controlFileMgr, nullptr);
    RetStatus ret = m_controlFileMgr->OpenControlFiles();
    ASSERT_EQ(ret, DSTORE_SUCC);
    m_controlFileMgr->CleanPageStateValid(CONTROL_TBS_METAPAGE_TYPE);
    m_controlFileMgr->SetEnableCachePage(false);
    PageHandle *pageHandle =
        static_cast<PageHandle *>(DstoreMemoryContextAllocZero(m_ut_memory_context, sizeof(PageHandle)));
    ASSERT_NE(pageHandle, nullptr);
    ret = m_controlFileMgr->GetValidMetaPage(pageHandle, CONTROL_TBS_METAPAGE_TYPE);
    ASSERT_EQ(ret, DSTORE_SUCC);
    ASSERT_EQ(pageHandle->file, m_controlFileMgr->m_file1);
    ASSERT_EQ(pageHandle->numDirtyBlocks, 0);
    char *page1 = m_controlFileMgr->ReadOnePage(pageHandle, CONTROL_TBS_METAPAGE_TYPE);
    uint32 checkSum = ((ControlBasePage *)page1)->GetCheckSum();
    ((ControlMetaPage *)page1)->MarkWriting();
    ControlDiskFile *file1 = m_controlFileMgr->GetFile1();
    ret = file1->WritePage(CONTROL_TBS_METAPAGE_TYPE, page1);
    ASSERT_EQ(ret, DSTORE_SUCC);
    ret = file1->Fsync();
    ASSERT_EQ(ret, DSTORE_SUCC);
    ControlDiskFile *file2 = m_controlFileMgr->GetFile2();
    ret = file2->WritePage(CONTROL_TBS_METAPAGE_TYPE, page1);
    ASSERT_EQ(ret, DSTORE_SUCC);
    ret = file2->Fsync();
    ASSERT_EQ(ret, DSTORE_SUCC);
    bool isFile1Valid = false;
    bool isFile1Writing = false;
    uint64 file1Term = 0;
    ret = m_controlFileMgr->CheckMetaPage(&isFile1Valid, &isFile1Writing, &file1Term, CONTROL_TBS_METAPAGE_TYPE, file1);
    ASSERT_EQ(ret, DSTORE_SUCC);
    ASSERT_EQ(isFile1Valid, false);
    ASSERT_EQ(isFile1Writing, true);
    bool isFile2Valid = false;
    bool isFile2Writing = false;
    uint64 file2Term = 0;
    ret = m_controlFileMgr->CheckMetaPage(&isFile2Valid, &isFile2Writing, &file2Term, CONTROL_TBS_METAPAGE_TYPE, file2);
    ASSERT_EQ(ret, DSTORE_SUCC);
    ASSERT_EQ(isFile2Valid, false);
    ASSERT_EQ(isFile2Writing, true);
    ret = m_controlFileMgr->GetValidMetaPage(pageHandle, CONTROL_TBS_METAPAGE_TYPE);
    ASSERT_EQ(ret, DSTORE_FAIL);
    DstorePfreeExt(pageHandle);
}

TEST_F(UTControlFileMgrTest, FileMetaPageCheckAndRecoeryTest_level0) {
    RetStatus ret = m_controlFileMgr->OpenControlFiles();
    ASSERT_EQ(ret, DSTORE_SUCC);
    ControlFilePageMap fileMetaPage = CONTROLFILE_PAGEMAP_FILEMETA;
    bool checkCrcMatch = false;
    bool checkConsistency = false;
    ret = m_controlFileMgr->CheckPageCrcMatch(m_controlFileMgr->m_file1, fileMetaPage, checkCrcMatch);
    EXPECT_EQ(ret, DSTORE_SUCC);
    EXPECT_EQ(checkCrcMatch, true);
    ret = m_controlFileMgr->CheckPageCrcMatch(m_controlFileMgr->m_file2, fileMetaPage, checkCrcMatch);
    EXPECT_EQ(ret, DSTORE_SUCC);
    EXPECT_EQ(checkCrcMatch, true);
    checkConsistency = m_controlFileMgr->CheckPageConsistency(fileMetaPage);
    EXPECT_EQ(checkConsistency, true);
    
    /* fileMetaPage of controlfile2 is injected with wrong checksum */
    ret = m_controlFileMgr->UTWriteFileForFaultInjection(fileMetaPage, false, false);
    EXPECT_EQ(ret, DSTORE_SUCC);
    ret = m_controlFileMgr->CheckPageCrcMatch(m_controlFileMgr->m_file2, fileMetaPage, checkCrcMatch);
    EXPECT_EQ(ret, DSTORE_SUCC);
    EXPECT_EQ(checkCrcMatch, false);
    checkConsistency = m_controlFileMgr->CheckPageConsistency(fileMetaPage);
    EXPECT_EQ(checkConsistency, false);
    /* recover fileMetaPage of controlfile2 */
    ret = m_controlFileMgr->CheckCrcAndRecoveryForFileMeta();
    EXPECT_EQ(ret, DSTORE_SUCC);
    checkConsistency = m_controlFileMgr->CheckPageConsistency(fileMetaPage);
    EXPECT_EQ(checkConsistency, true);
 
    /* fileMetaPage of controlfile1 is injected with wrong data and it's checksum will be updated at the same time */
    ret = m_controlFileMgr->UTWriteFileForFaultInjection(fileMetaPage, true, true);
    EXPECT_EQ(ret, DSTORE_SUCC);
    ret = m_controlFileMgr->CheckPageCrcMatch(m_controlFileMgr->m_file1, fileMetaPage, checkCrcMatch);
    EXPECT_EQ(ret, DSTORE_SUCC);
    EXPECT_EQ(checkCrcMatch, true);
    checkConsistency = m_controlFileMgr->CheckPageConsistency(fileMetaPage);
    EXPECT_EQ(checkConsistency, false);
    /* recover fileMetaPage of controlfile1 */
    ret = m_controlFileMgr->CheckCrcAndRecoveryForFileMeta();
    EXPECT_EQ(ret, DSTORE_SUCC);
    checkConsistency = m_controlFileMgr->CheckPageConsistency(fileMetaPage);
    EXPECT_EQ(checkConsistency, true);
}

TEST_F(UTControlFileMgrTest, GroupMetaPageCheckAndRecoeryTest_level0) {
    RetStatus ret = m_controlFileMgr->OpenControlFiles();
    ASSERT_EQ(ret, DSTORE_SUCC);
    ControlFilePageMap groupMetaPage = CONTROLFILE_PAGEMAP_TABLESPACE_META;
    bool checkCrcMatch = false;
    bool checkConsistency = false;
    ret = m_controlFileMgr->CheckPageCrcMatch(m_controlFileMgr->m_file1, groupMetaPage, checkCrcMatch);
    EXPECT_EQ(ret, DSTORE_SUCC);
    EXPECT_EQ(checkCrcMatch, true);
    ret = m_controlFileMgr->CheckPageCrcMatch(m_controlFileMgr->m_file2, groupMetaPage, checkCrcMatch);
    EXPECT_EQ(ret, DSTORE_SUCC);
    EXPECT_EQ(checkCrcMatch, true);
    checkConsistency = m_controlFileMgr->CheckPageConsistency(groupMetaPage);
    EXPECT_EQ(checkConsistency, true);
    
    /* groupMetaPage of controlfile2 is injected with wrong checksum */
    ret = m_controlFileMgr->UTWriteFileForFaultInjection(groupMetaPage, false, false);
    EXPECT_EQ(ret, DSTORE_SUCC);
    ret = m_controlFileMgr->CheckPageCrcMatch(m_controlFileMgr->m_file2, groupMetaPage, checkCrcMatch);
    EXPECT_EQ(ret, DSTORE_SUCC);
    EXPECT_EQ(checkCrcMatch, false);
    checkConsistency = m_controlFileMgr->CheckPageConsistency(groupMetaPage);
    EXPECT_EQ(checkConsistency, false);
    /* recover groupMetaPage of controlfile2 */
    ret = m_controlFileMgr->CheckCrcAndRecoveryForGroup(groupMetaPage, CONTROL_GROUP_TYPE_TABLESPACE);
    EXPECT_EQ(ret, DSTORE_SUCC);
    checkConsistency = m_controlFileMgr->CheckPageConsistency(groupMetaPage);
    EXPECT_EQ(checkConsistency, true);
 
    /* groupMetaPage2 of controlfile1 is injected with wrong data and it's checksum will be updated at the same time */
    ControlFilePageMap groupMetaPage2 = CONTROLFILE_PAGEMAP_PDBINFO_META;
    ret = m_controlFileMgr->UTWriteFileForFaultInjection(groupMetaPage2, true, true);
    EXPECT_EQ(ret, DSTORE_SUCC);
    ret = m_controlFileMgr->CheckPageCrcMatch(m_controlFileMgr->m_file1, groupMetaPage2, checkCrcMatch);
    EXPECT_EQ(ret, DSTORE_SUCC);
    EXPECT_EQ(checkCrcMatch, true);
    checkConsistency = m_controlFileMgr->CheckPageConsistency(groupMetaPage2);
    EXPECT_EQ(checkConsistency, false);
    /* recover groupMetaPage2 of controlfile1 */
    ret = m_controlFileMgr->CheckCrcAndRecoveryForGroup(groupMetaPage2, CONTROL_GROUP_TYPE_PDBINFO);
    EXPECT_EQ(ret, DSTORE_SUCC);
    checkConsistency = m_controlFileMgr->CheckPageConsistency(groupMetaPage2);
    EXPECT_EQ(checkConsistency, true);
}

TEST_F(UTControlFileMgrTest, GroupDataPageCheckAndRecoeryTest_level0) {
    RetStatus ret = m_controlFileMgr->OpenControlFiles();
    ASSERT_EQ(ret, DSTORE_SUCC);

    /* Create enough data to fill more than 4 data pages. */
    ControlWalStreamPageItemData *itemData =
        (ControlWalStreamPageItemData *) DstorePalloc0(sizeof(ControlWalStreamPageItemData));
    itemData->streamState = 1;
    itemData->walBlockSize = 8192;
    itemData->walFileSize = 4 * 1024 * 1024;
    itemData->walMinRecoveryPlsn = 0x2022;
    itemData->archivePlsn = 0x07;
    itemData->lastCheckpointPLsn = 0x30;
    itemData->lastWalCheckpoint.diskRecoveryPlsn = 0x0db0;
    itemData->lastWalCheckpoint.time = 1645427962;
    itemData->createFilePara.flag = APPEND_WRITE_FILE;
    itemData->createFilePara.rangeSize = WAL_FILE_RANGE_SIZE;
    itemData->createFilePara.maxSize = itemData->walFileSize;
    itemData->createFilePara.mode = FILE_READ_AND_WRITE_MODE;
    itemData->createFilePara.isReplayWrite = false;
    TenantConfig *tenantConfig = g_storageInstance->GetGuc()->tenantConfig;
    StorageAssert(tenantConfig);
    errno_t rc = strcpy_s(itemData->createFilePara.storeSpaceName, STORESPACE_NAME_MAX_LEN,
                          tenantConfig->storeSpaces[0].storeSpaceName);
    storage_securec_check(rc, "\0", "\0");
    itemData->initWalFileCount = 111;
    for (int i = 1; i < 64; i++) {
        itemData->walId = i + 1;
        ret = m_controlFile->AddWalStream(*itemData);
        ASSERT_EQ(ret, DSTORE_SUCC);
    }
    DstorePfreeExt(itemData);

    ControlFilePageMap groupMetaPage = CONTROLFILE_PAGEMAP_WALSTREAM_META;
    ControlFilePageMap groupDataPage1 = CONTROLFILE_PAGEMAP_WALSTREAM_START;
    ControlFilePageMap groupDataPage2 = (ControlFilePageMap)(CONTROLFILE_PAGEMAP_WALSTREAM_START + 2);
    bool checkCrcMatch = false;
    bool checkConsistency = false;
    PageHandle *pageHandle = static_cast<PageHandle *>
        (DstoreMemoryContextAllocZero(m_ut_memory_context, sizeof(PageHandle)));
    ASSERT_NE(pageHandle, nullptr);
    ret = m_controlFileMgr->GetValidMetaPage(pageHandle, groupMetaPage);
    ASSERT_EQ(ret, DSTORE_SUCC);
    ASSERT_EQ(pageHandle->file, m_controlFileMgr->m_file1);
    
    /* group data page and meta page of control file1 is injected with wrong checksum */
    ret = m_controlFileMgr->UTWriteFileForFaultInjection(groupDataPage1, true, false);
    EXPECT_EQ(ret, DSTORE_SUCC);
    ret = m_controlFileMgr->UTWriteFileForFaultInjection(groupDataPage2, true, true);
    EXPECT_EQ(ret, DSTORE_SUCC);
    ret = m_controlFileMgr->UTWriteFileForFaultInjection(groupMetaPage, true, true);
    EXPECT_EQ(ret, DSTORE_SUCC);
    checkConsistency = m_controlFileMgr->CheckPageConsistency(groupDataPage1);
    EXPECT_EQ(checkConsistency, false);
    checkConsistency = m_controlFileMgr->CheckPageConsistency(groupDataPage2);
    EXPECT_EQ(checkConsistency, false);
    checkConsistency = m_controlFileMgr->CheckPageConsistency(groupMetaPage);
    EXPECT_EQ(checkConsistency, false);

    /* Recovery group */
    ret = m_controlFileMgr->GetValidMetaPage(pageHandle, groupMetaPage);
    ASSERT_EQ(ret, DSTORE_SUCC);
    ASSERT_EQ(pageHandle->file, m_controlFileMgr->m_file2);
    ret = m_controlFileMgr->MarkPageDirty(pageHandle, groupMetaPage);
    ret = m_controlFileMgr->PostPageHandle(pageHandle, groupMetaPage);
    EXPECT_EQ(ret, DSTORE_SUCC);

    /* Recovery for only one data page fault */
    ControlFilePageMap groupDataPage3 = (ControlFilePageMap)(CONTROLFILE_PAGEMAP_WALSTREAM_START + 3);
    ret = m_controlFileMgr->UTWriteFileForFaultInjection(groupDataPage3, true, false);
    EXPECT_EQ(ret, DSTORE_SUCC);
    ret = m_controlFileMgr->CheckPageCrcMatch(m_controlFileMgr->m_file1, groupDataPage3, checkCrcMatch);
    EXPECT_EQ(checkCrcMatch, false);
    ret = m_controlFileMgr->CheckPageCrcMatch(m_controlFileMgr->m_file2, groupDataPage3, checkCrcMatch);
    EXPECT_EQ(checkCrcMatch, true);
    m_controlFileMgr->CleanPageStateValid(groupDataPage3);
    (void *)m_controlFileMgr->ReadOnePage(pageHandle, groupDataPage3);
    ASSERT_EQ(ret, DSTORE_SUCC);
    ASSERT_EQ(pageHandle->file, m_controlFileMgr->m_file2);
    ASSERT_EQ(pageHandle->checkResult, SECOND_META_PAGE_IS_VALID);
    m_controlFileMgr->MarkPageStateDirty(groupDataPage3);
    ret = m_controlFileMgr->PostPageHandle(pageHandle, groupMetaPage);
    EXPECT_EQ(ret, DSTORE_SUCC);
    ret = m_controlFileMgr->CheckPageCrcMatch(m_controlFileMgr->m_file1, groupDataPage3, checkCrcMatch);
    EXPECT_EQ(checkCrcMatch, true);
    ret = m_controlFileMgr->CheckPageCrcMatch(m_controlFileMgr->m_file2, groupDataPage3, checkCrcMatch);
    EXPECT_EQ(checkCrcMatch, true);
    
    /* All the pages are consistent */
    for (BlockNumber i = CONTROLFILE_PAGEMAP_METAPAGE_MAX + 1; i < CONTROLFILE_PAGEMAP_MAX; i++) {
        checkConsistency = m_controlFileMgr->CheckPageConsistency(i);
        EXPECT_NE(checkConsistency, false) << " with block number:" << i;
    }
    DstorePfreeExt(pageHandle);
}

TEST_F(UTControlFileMgrTest, ControlFilePageTest_level0)
{
    RetStatus ret = m_controlFileMgr->OpenControlFiles();
    ASSERT_EQ(ret, DSTORE_SUCC);
    PageHandle *pageHandle =
        static_cast<PageHandle *>(DstoreMemoryContextAllocZero(m_ut_memory_context, sizeof(PageHandle)));
    ret = m_controlFileMgr->GetValidMetaPage(pageHandle, CONTROLFILE_PAGEMAP_FILEMETA);
    ASSERT_EQ(ret, DSTORE_SUCC);
    ControlFileMetaPage *metaPage =
        (ControlFileMetaPage *)m_controlFileMgr->ReadOnePage(pageHandle, CONTROLFILE_PAGEMAP_FILEMETA);
    ControlFileMetaData *meta = (ControlFileMetaData *)metaPage->GetMetaData();
    ASSERT_NE(meta, nullptr);
    ASSERT_EQ(meta->maxPageType, CONTROL_MAX_PAGE_TYPE);
    ASSERT_EQ(meta->usedGroupType, CONTROL_GROUP_TYPE_MAX);
    ASSERT_EQ(meta->totalPageCount, CONTROLFILE_PAGEMAP_MAX);
    ASSERT_EQ(meta->usedPageCount, CONTROLFILE_PAGEMAP_LOGICALREP_MAX);
    ASSERT_EQ(meta->magic, MAGIC_NUMBER);
    DstorePfreeExt(pageHandle);
}
