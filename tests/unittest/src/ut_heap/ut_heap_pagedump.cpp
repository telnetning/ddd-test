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
#include "ut_heap/ut_heap.h"
#include "page/dstore_data_page.h"
#include "page/dstore_page_diagnose.h"
#include "wal/dstore_wal_dump.h"

extern char g_utDataDir[MAXPGPATH];

TEST_F(UTHeap, PagedumpSetPrintTargetTest_level0)
{
    PageDiagnose::DumpToolHelper::SetPrintTarget(stdout);
}

TEST_F(UTHeap, PagedumpGetPageTypeTest_level0)
{
    HeapPage *page = new HeapPage();
    errno_t rc = memset_s(page, sizeof(HeapPage), 0, sizeof(HeapPage));
    storage_securec_check(rc, "\0", "\0");

    page->Init(0, PageType::HEAP_PAGE_TYPE, INVALID_PAGE_ID);
    PageType type = PageDiagnose::GetPageType(page);
    EXPECT_EQ(type, PageType::HEAP_PAGE_TYPE);
    delete page;
}

TEST_F(UTHeap, PagedumpGetIndexMetaPageBlockIdTest_level0)
{
    HeapPage *page = new HeapPage();
    errno_t rc = memset_s(page, sizeof(HeapPage), 0, sizeof(HeapPage));
    storage_securec_check(rc, "\0", "\0");

    page->Init(0, PageType::INDEX_PAGE_TYPE, INVALID_PAGE_ID);
    
    BlockNumber block = PageDiagnose::GetIndexMetaPageBlockId(page); 
    EXPECT_EQ(block, 4294967295);
    delete page;
}

TEST_F(UTHeap, PagedumpGetIndexMetaFileIdTest_level0)
{
    HeapPage *page = new HeapPage();
    errno_t rc = memset_s(page, sizeof(HeapPage), 0, sizeof(HeapPage));
    storage_securec_check(rc, "\0", "\0");

    page->Init(0, PageType::INDEX_PAGE_TYPE, INVALID_PAGE_ID);

    uint16_t block = PageDiagnose::GetIndexMetaFileId(page);
    EXPECT_EQ(block, 0);
    delete page;
}

TEST_F(UTHeap, PagedumpPageDumpTest_level0)
{
    HeapPage *metaPage = new HeapPage();
    errno_t rc;
    rc = memset_s(metaPage, sizeof(HeapPage), 0, sizeof(HeapPage));
    storage_securec_check(rc, "\0", "\0");

    HeapPage *heap_page = new HeapPage();
    rc = memset_s(heap_page, sizeof(HeapPage), 0, sizeof(HeapPage));
    storage_securec_check(rc, "\0", "\0");
    heap_page->Init(0, PageType::HEAP_PAGE_TYPE, INVALID_PAGE_ID);
    PageDiagnose::PageDump(heap_page, true, metaPage);
    delete heap_page;

    HeapPage *index_page = new HeapPage();
    rc = memset_s(index_page, sizeof(HeapPage), 0, sizeof(HeapPage));
    storage_securec_check(rc, "\0", "\0");
    index_page->Init(0, PageType::INDEX_PAGE_TYPE, INVALID_PAGE_ID);
    PageDiagnose::PageDump(index_page, true, metaPage);
    delete index_page;
    
    HeapPage *transaction_slot_page = new HeapPage();
    rc = memset_s(transaction_slot_page, sizeof(HeapPage), 0, sizeof(HeapPage));
    storage_securec_check(rc, "\0", "\0");
    transaction_slot_page->Init(0, PageType::TRANSACTION_SLOT_PAGE, INVALID_PAGE_ID);
    PageDiagnose::PageDump(transaction_slot_page, true, metaPage);
    delete transaction_slot_page;
    
    HeapPage *undo_page = new HeapPage();
    rc = memset_s(undo_page, sizeof(HeapPage), 0, sizeof(HeapPage));
    storage_securec_check(rc, "\0", "\0");
    undo_page->Init(0, PageType::UNDO_PAGE_TYPE, INVALID_PAGE_ID);
    PageDiagnose::PageDump(undo_page, true, metaPage);
    delete undo_page;
    
    HeapPage *fsm_page = new HeapPage();
    rc = memset_s(fsm_page, sizeof(HeapPage), 0, sizeof(HeapPage));
    storage_securec_check(rc, "\0", "\0");
    fsm_page->Init(0, PageType::FSM_PAGE_TYPE, INVALID_PAGE_ID);
    PageDiagnose::PageDump(fsm_page, true, metaPage);
    delete fsm_page;
    
    HeapPage *fsm_meta_page = new HeapPage();
    rc = memset_s(fsm_meta_page, sizeof(HeapPage), 0, sizeof(HeapPage));
    storage_securec_check(rc, "\0", "\0");
    fsm_meta_page->Init(0, PageType::FSM_META_PAGE_TYPE, INVALID_PAGE_ID);
    PageDiagnose::PageDump(fsm_meta_page, true, metaPage);
    delete fsm_meta_page;
    
    HeapPage *data_segment_meta_page = new HeapPage();
    rc = memset_s(data_segment_meta_page, sizeof(HeapPage), 0, sizeof(HeapPage));
    storage_securec_check(rc, "\0", "\0");
    data_segment_meta_page->Init(0, PageType::DATA_SEGMENT_META_PAGE_TYPE, INVALID_PAGE_ID);
    PageDiagnose::PageDump(data_segment_meta_page, true, metaPage);
    delete data_segment_meta_page;
    
    HeapPage *heap_segment_meta_page = new HeapPage();
    rc = memset_s(heap_segment_meta_page, sizeof(HeapPage), 0, sizeof(HeapPage));
    storage_securec_check(rc, "\0", "\0");
    heap_segment_meta_page->Init(0, PageType::HEAP_SEGMENT_META_PAGE_TYPE, INVALID_PAGE_ID);
    PageDiagnose::PageDump(heap_segment_meta_page, true, metaPage);
    delete heap_segment_meta_page;
    
    HeapPage *undo_segment_meta_page = new HeapPage();
    rc = memset_s(undo_segment_meta_page, sizeof(HeapPage), 0, sizeof(HeapPage));
    storage_securec_check(rc, "\0", "\0");
    undo_segment_meta_page->Init(0, PageType::UNDO_SEGMENT_META_PAGE_TYPE, INVALID_PAGE_ID);
    PageDiagnose::PageDump(undo_segment_meta_page, true, metaPage);
    delete undo_segment_meta_page;
    
    HeapPage *tbs_extent_meta_page = new HeapPage();
    rc = memset_s(tbs_extent_meta_page, sizeof(HeapPage), 0, sizeof(HeapPage));
    storage_securec_check(rc, "\0", "\0");
    tbs_extent_meta_page->Init(0, PageType::TBS_EXTENT_META_PAGE_TYPE, INVALID_PAGE_ID);
    PageDiagnose::PageDump(tbs_extent_meta_page, true, metaPage);
    delete tbs_extent_meta_page;
    
    HeapPage *tbs_bitmap_page = new HeapPage();
    rc = memset_s(tbs_bitmap_page, sizeof(HeapPage), 0, sizeof(HeapPage));
    storage_securec_check(rc, "\0", "\0");
    tbs_bitmap_page->Init(0, PageType::TBS_BITMAP_PAGE_TYPE, INVALID_PAGE_ID);
    PageDiagnose::PageDump(tbs_bitmap_page, true, metaPage);
    delete tbs_bitmap_page;
    
    HeapPage *tbs_bitmap_meta_page = new HeapPage();
    rc = memset_s(tbs_bitmap_meta_page, sizeof(HeapPage), 0, sizeof(HeapPage));
    storage_securec_check(rc, "\0", "\0");
    tbs_bitmap_meta_page->Init(0, PageType::TBS_BITMAP_META_PAGE_TYPE, INVALID_PAGE_ID);
    PageDiagnose::PageDump(tbs_bitmap_meta_page, true, metaPage);
    delete tbs_bitmap_meta_page;
    
    HeapPage *tbs_file_meta_page = new HeapPage();
    rc = memset_s(tbs_file_meta_page, sizeof(HeapPage), 0, sizeof(HeapPage));
    storage_securec_check(rc, "\0", "\0");
    tbs_file_meta_page->Init(0, PageType::TBS_FILE_META_PAGE_TYPE, INVALID_PAGE_ID);
    PageDiagnose::PageDump(tbs_file_meta_page, true, metaPage);
    delete tbs_file_meta_page;
    
    HeapPage *btr_queue_page = new HeapPage();
    rc = memset_s(btr_queue_page, sizeof(HeapPage), 0, sizeof(HeapPage));
    storage_securec_check(rc, "\0", "\0");
    btr_queue_page->Init(0, PageType::BTR_QUEUE_PAGE_TYPE, INVALID_PAGE_ID);
    PageDiagnose::PageDump(btr_queue_page, true, metaPage);
    delete btr_queue_page;
    
    HeapPage *btr_recycle_partition_meta_page = new HeapPage();
    rc = memset_s(btr_recycle_partition_meta_page, sizeof(HeapPage), 0, sizeof(HeapPage));
    storage_securec_check(rc, "\0", "\0");
    btr_recycle_partition_meta_page->Init(0, PageType::BTR_RECYCLE_PARTITION_META_PAGE_TYPE, INVALID_PAGE_ID);
    PageDiagnose::PageDump(btr_recycle_partition_meta_page, true, metaPage);
    delete btr_recycle_partition_meta_page;
    
    HeapPage *btr_recycle_root_meta_page = new HeapPage();
    rc = memset_s(btr_recycle_root_meta_page, sizeof(HeapPage), 0, sizeof(HeapPage));
    storage_securec_check(rc, "\0", "\0");
    btr_recycle_root_meta_page->Init(0, PageType::BTR_RECYCLE_ROOT_META_PAGE_TYPE, INVALID_PAGE_ID);
    PageDiagnose::PageDump(btr_recycle_root_meta_page, true, metaPage);
    delete btr_recycle_root_meta_page;
    
    HeapPage *tbs_space_meta_page = new HeapPage();
    rc = memset_s(tbs_space_meta_page, sizeof(HeapPage), 0, sizeof(HeapPage));
    storage_securec_check(rc, "\0", "\0");
    tbs_space_meta_page->Init(0, PageType::TBS_SPACE_META_PAGE_TYPE, INVALID_PAGE_ID);
    PageDiagnose::PageDump(tbs_space_meta_page, true, metaPage);
    delete tbs_space_meta_page;
    
    HeapPage *invalid_page = new HeapPage();
    rc = memset_s(invalid_page, sizeof(HeapPage), 0, sizeof(HeapPage));
    storage_securec_check(rc, "\0", "\0");
    invalid_page->Init(0, PageType::INVALID_PAGE_TYPE, INVALID_PAGE_ID);
    PageDiagnose::PageDump(invalid_page, true, metaPage);
    delete invalid_page;

    delete metaPage;
    
    char pageBuf[BLCKSZ];
    PageDiagnose::PageDump(STATIC_CAST_PTR_TYPE(pageBuf, ControlFileMetaPage*));
    PageDiagnose::PageDump(STATIC_CAST_PTR_TYPE(pageBuf, ControlPage*));
    PageDiagnose::PageDump(STATIC_CAST_PTR_TYPE(pageBuf, DecodeDictMetaPage*));
    PageDiagnose::PageDump(STATIC_CAST_PTR_TYPE(pageBuf, DecodeDictPage*));
}

TEST_F(UTHeap, PagedumpParseCommThreadNumTest_level0)
{
    PageDiagnose::DumpToolHelper::SetPrintTarget(stdout);
    PageDiagnose::DumpCommConfig dumpCommConfig = {
        UINT32_MAX,
        -1,
        -1,
        nullptr
    };

    char arg[] = "123:456";
    RetStatus ret = PageDiagnose::ParseCommThreadNum("pagedump", arg, &dumpCommConfig);
    EXPECT_EQ(ret, DSTORE_SUCC);

    char arg1[] = "123.a:456";
    ret = PageDiagnose::ParseCommThreadNum("pagedump", arg1, &dumpCommConfig);
    EXPECT_EQ(ret, DSTORE_SUCC); // DSTORE_FAIL
    
    char arg2[] = "123:456.a";
    ret = PageDiagnose::ParseCommThreadNum("pagedump", arg2, &dumpCommConfig);
    EXPECT_EQ(ret, DSTORE_SUCC); // DSTORE_FAIL
    
    char arg3[] = "456:123";
    ret = PageDiagnose::ParseCommThreadNum("pagedump", arg3, &dumpCommConfig);
    EXPECT_EQ(ret, DSTORE_FAIL);
}

TEST_F(UTHeap, PagedumpInitCommConfigTest_level0)
{
    PageDiagnose::DumpCommConfig dumpCommConfig;
    PageDiagnose::InitCommConfig(&dumpCommConfig);
}

TEST_F(UTHeap, PagedumpGetFileNameTest_level0)
{
    uint16_t fileId = 0;
    char fileName[MAXPGPATH];
    PageDiagnose::GetFileName(fileId, fileName, MAXPGPATH);
}

TEST_F(UTHeap, PagedumpInitDefaultCommConfigTest_level0)
{
    PageDiagnose::DumpToolHelper::SetPrintTarget(stdout);
    WalDumpConfig config;
    RetStatus retStatus = WalDumper::InitWalDumpConfig(config);
    config.vfs = g_storageInstance->GetPdb(g_defaultPdbId)->GetVFS();
    config.reuseVfs = true;
    PageDiagnose::DumpToolHelperInitParam param = {
        .reuseVfs = config.reuseVfs,
        .vfs = config.vfs,
        .pdbVfsName = config.pdbVfsName,
        .commConfig = &config.commConfig
    };
    char vfsConfigPath[] = { 0 };
    int read_ret = readlink("/proc/self/exe", vfsConfigPath, MAXPGPATH);
    ASSERT_GT(read_ret, 0);
    PageDiagnose::DumpToolHelper *fileReader = new PageDiagnose::DumpToolHelper(StorageType::PAGESTORE, vfsConfigPath);
    if (unlikely(fileReader == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("GetPageInfoFromControlFile new DumpToolHelper fail."));
    }

    param.vfs = config.vfs;
    RetStatus ret = fileReader->Init(&param);
    EXPECT_EQ(ret, DSTORE_SUCC);

    PageDiagnose::DumpToolHelperInitParam param1 = {
        .reuseVfs = false,
        .vfs = config.vfs,
        .pdbVfsName = config.pdbVfsName,
        .commConfig = &config.commConfig
    };
    param1.vfs = config.vfs;
    ret = fileReader->Init(&param1);
    EXPECT_EQ(ret, DSTORE_FAIL); // Failed to parse tenant config.
    
    delete fileReader;
}

TEST_F(UTHeap, PagedumpDestroyTest_level0)
{
    PageDiagnose::DumpToolHelper::SetPrintTarget(stdout);
    WalDumpConfig config;
    RetStatus retStatus = WalDumper::InitWalDumpConfig(config);
    config.vfs = g_storageInstance->GetPdb(g_defaultPdbId)->GetVFS();
    config.reuseVfs = true;
    PageDiagnose::DumpToolHelperInitParam param = {
        .reuseVfs = config.reuseVfs,
        .vfs = config.vfs,
        .pdbVfsName = config.pdbVfsName,
        .commConfig = &config.commConfig
    };
    char vfsConfigPath[] = { 0 };
    int read_ret = readlink("/proc/self/exe", vfsConfigPath, MAXPGPATH);
    ASSERT_GT(read_ret, 0);
    PageDiagnose::DumpToolHelper *fileReader = new PageDiagnose::DumpToolHelper(StorageType::PAGESTORE, vfsConfigPath);
    if (unlikely(fileReader == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("GetPageInfoFromControlFile new DumpToolHelper fail."));
    }

    param.vfs = config.vfs;
    RetStatus ret = fileReader->Init(&param);
    EXPECT_EQ(ret, DSTORE_SUCC);

    FileDescriptor *file_fd = NULL;
    int64 size = fileReader->Size(file_fd);
    EXPECT_EQ(size, -1);
    
    FileDescriptor *control_file_fd = NULL;
    ret = config.vfs->OpenFile(
        DATABASE_CONTROL_FILE_1_NAME,
        FILE_READ_ONLY_FLAG, &control_file_fd);
    ASSERT_EQ(ret, DSTORE_SUCC);
    uint32_t metaPageId = CONTROLFILE_PAGEMAP_WALSTREAM_META;
    uint8_t firstMetaPageBuf[BLCKSZ];
    ret = fileReader->ReadPage(control_file_fd, metaPageId, firstMetaPageBuf, 8191);
    EXPECT_EQ(ret, DSTORE_FAIL);
    ret = fileReader->ReadPage(control_file_fd, metaPageId, firstMetaPageBuf, BLCKSZ);
    EXPECT_EQ(ret, DSTORE_SUCC);
    ret = config.vfs->CloseFile(control_file_fd);
    ASSERT_EQ(ret, DSTORE_SUCC);

    fileReader->Destroy();
    delete fileReader;
}

TEST_F(UTHeap, PagedumpOpenTest_level0)
{
    PageDiagnose::DumpToolHelper::SetPrintTarget(stdout);
    char vfsConfigPath[] = { 0 };
    int read_ret = readlink("/proc/self/exe", vfsConfigPath, MAXPGPATH);
    ASSERT_GT(read_ret, 0);
    PageDiagnose::DumpToolHelper *fileReader = new PageDiagnose::DumpToolHelper(StorageType::PAGESTORE, vfsConfigPath);
    if (unlikely(fileReader == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("GetPageInfoFromControlFile new DumpToolHelper fail."));
    }

    RetStatus ret = fileReader->Open(DATABASE_CONTROL_FILE_1_NAME, FILE_READ_ONLY_FLAG, nullptr);
    EXPECT_EQ(ret, DSTORE_FAIL);

    FileDescriptor *fd[2] = {nullptr};
    ret = fileReader->Open(DATABASE_CONTROL_FILE_1_NAME, FILE_READ_ONLY_FLAG, &fd[0]);
    EXPECT_EQ(ret, DSTORE_FAIL);

    ret = fileReader->Open(DATABASE_CONTROL_FILE_1_NAME, FILE_READ_ONLY_FLAG, &fd[0]);
    EXPECT_EQ(ret, DSTORE_FAIL);
    delete fileReader;
}

TEST_F(UTHeap, PagedumpCloseTest_level0)
{
    PageDiagnose::DumpToolHelper::SetPrintTarget(stdout);
    WalDumpConfig config;
    RetStatus retStatus = WalDumper::InitWalDumpConfig(config);
    PageDiagnose::DumpToolHelperInitParam param = {
        .reuseVfs = config.reuseVfs,
        .vfs = config.vfs,
        .pdbVfsName = config.pdbVfsName,
        .commConfig = &config.commConfig
    };
    char vfsConfigPath[] = { 0 };
    int read_ret = readlink("/proc/self/exe", vfsConfigPath, MAXPGPATH);
    ASSERT_GT(read_ret, 0);
    PageDiagnose::DumpToolHelper *fileReader = new PageDiagnose::DumpToolHelper(StorageType::PAGESTORE, vfsConfigPath);
    if (unlikely(fileReader == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("GetPageInfoFromControlFile new DumpToolHelper fail."));
    }

    fileReader->Close(nullptr);

    FileDescriptor *fd[2] = {nullptr};
    fileReader->Close(fd[0]);
    delete fileReader;
}

TEST_F(UTHeap, PagedumpReadPageTest_level0)
{
    PageDiagnose::DumpToolHelper::SetPrintTarget(stdout);
    WalDumpConfig config;
    RetStatus retStatus = WalDumper::InitWalDumpConfig(config);
    config.vfs = g_storageInstance->GetPdb(g_defaultPdbId)->GetVFS();
    config.reuseVfs = true;
    PageDiagnose::DumpToolHelperInitParam param = {
        .reuseVfs = config.reuseVfs,
        .vfs = config.vfs,
        .pdbVfsName = config.pdbVfsName,
        .commConfig = &config.commConfig
    };
    char vfsConfigPath[] = { 0 };
    int read_ret = readlink("/proc/self/exe", vfsConfigPath, MAXPGPATH);
    ASSERT_GT(read_ret, 0);
    PageDiagnose::DumpToolHelper *fileReader = new PageDiagnose::DumpToolHelper(StorageType::PAGESTORE, vfsConfigPath);
    if (unlikely(fileReader == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("GetPageInfoFromControlFile new DumpToolHelper fail."));
    }

    param.vfs = config.vfs;
    RetStatus ret = fileReader->Init(&param);
    EXPECT_EQ(ret, DSTORE_SUCC);

    FileDescriptor *file_fd = NULL;
    int64 size = fileReader->Size(file_fd);
    EXPECT_EQ(size, -1);
    
    FileDescriptor *control_file_fd = NULL;
    ret = config.vfs->OpenFile(
        DATABASE_CONTROL_FILE_1_NAME,
        FILE_READ_ONLY_FLAG, &control_file_fd);
    ASSERT_EQ(ret, DSTORE_SUCC);
    uint32_t metaPageId = CONTROLFILE_PAGEMAP_WALSTREAM_META;
    uint8_t firstMetaPageBuf[BLCKSZ];
    ret = fileReader->ReadPage(control_file_fd, metaPageId, firstMetaPageBuf, 8191);
    EXPECT_EQ(ret, DSTORE_FAIL);
    ret = fileReader->ReadPage(control_file_fd, metaPageId, firstMetaPageBuf, BLCKSZ);
    EXPECT_EQ(ret, DSTORE_SUCC);
    ret = config.vfs->CloseFile(control_file_fd);
    ASSERT_EQ(ret, DSTORE_SUCC);
    control_file_fd = NULL;
    delete fileReader;
}

TEST_F(UTHeap, PagedumpSizeTest_level0)
{
    PageDiagnose::DumpToolHelper::SetPrintTarget(stdout);
    WalDumpConfig config;
    RetStatus retStatus = WalDumper::InitWalDumpConfig(config);
    config.vfs = g_storageInstance->GetPdb(g_defaultPdbId)->GetVFS();
    config.reuseVfs = true;
    PageDiagnose::DumpToolHelperInitParam param = {
        .reuseVfs = config.reuseVfs,
        .vfs = config.vfs,
        .pdbVfsName = config.pdbVfsName,
        .commConfig = &config.commConfig
    };
    char vfsConfigPath[] = { 0 };
    int read_ret = readlink("/proc/self/exe", vfsConfigPath, MAXPGPATH);
    ASSERT_GT(read_ret, 0);
    PageDiagnose::DumpToolHelper *fileReader = new PageDiagnose::DumpToolHelper(StorageType::PAGESTORE, vfsConfigPath);
    if (unlikely(fileReader == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("GetPageInfoFromControlFile new DumpToolHelper fail."));
    }

    param.vfs = config.vfs;
    RetStatus ret = fileReader->Init(&param);
    EXPECT_EQ(ret, DSTORE_SUCC);

    FileDescriptor *file_fd = NULL;
    int64 size = fileReader->Size(file_fd);
    EXPECT_EQ(size, -1);
    
    FileDescriptor *control_file_fd = NULL;
    ret = config.vfs->OpenFile(
        DATABASE_CONTROL_FILE_1_NAME,
        FILE_READ_ONLY_FLAG, &control_file_fd);
    ASSERT_EQ(ret, DSTORE_SUCC);

    size = fileReader->Size(control_file_fd);
    ASSERT_EQ(size, 31457280);
    ret = config.vfs->CloseFile(control_file_fd);
    ASSERT_EQ(ret, DSTORE_SUCC);
    delete fileReader;
}

TEST_F(UTHeap, PagedumpFileIsExistTest_level0)
{
    PageDiagnose::DumpToolHelper::SetPrintTarget(stdout);
    WalDumpConfig config;
    RetStatus retStatus = WalDumper::InitWalDumpConfig(config);
    config.vfs = g_storageInstance->GetPdb(g_defaultPdbId)->GetVFS();
    config.reuseVfs = true;
    PageDiagnose::DumpToolHelperInitParam param = {
        .reuseVfs = config.reuseVfs,
        .vfs = config.vfs,
        .pdbVfsName = config.pdbVfsName,
        .commConfig = &config.commConfig
    };
    char vfsConfigPath[] = { 0 };
    int read_ret = readlink("/proc/self/exe", vfsConfigPath, MAXPGPATH);
    ASSERT_GT(read_ret, 0);
    PageDiagnose::DumpToolHelper *fileReader = new PageDiagnose::DumpToolHelper(StorageType::PAGESTORE, vfsConfigPath);
    if (unlikely(fileReader == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("GetPageInfoFromControlFile new DumpToolHelper fail."));
    }

    param.vfs = config.vfs;
    RetStatus ret = fileReader->Init(&param);
    EXPECT_EQ(ret, DSTORE_SUCC);

    bool fileExist = false;
    ret = fileReader->FileIsExist(g_utDataDir, &fileExist);
    EXPECT_EQ(ret, DSTORE_SUCC);
    delete fileReader;
}
