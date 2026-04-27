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
#include "tablespace/dstore_tablespace_internal.h"
#include "ut_buffer/ut_buffer_fake_instance.h"
#include "framework/dstore_vfs_adapter.h"
#include "vfs/vfs_interface.h"
#include "ut_tablespace/ut_tablespace_util.h"
#include "tablespace/dstore_tablespace_wal.h"
#include "tablespace/dstore_tablespace_interface.h"

using namespace DSTORE;

const uint16 PRE_BITMAP_PAGE_COUNT = 3;
const FileId UT_TABLESPACE_FILE_ID_0 = 900;
const FileId UT_TABLESPACE_FILE_ID_1 = 901;
const FileId UT_TABLESPACE_FILE_ID_2 = 902;
const FileId UT_TABLESPACE_FILE_ID_3 = 903;
const FileId UT_TABLESPACE_FILE_ID_4 = 904;
const char *UT_TABLESPACE_FILE_NAME_0 = "ut_tablespace_file_900";
const char *UT_TABLESPACE_FILE_NAME_1 = "ut_tablespace_file_901";
const char *UT_TABLESPACE_FILE_NAME_2 = "ut_tablespace_file_902";
const char *UT_TABLESPACE_FILE_NAME_3 = "ut_tablespace_file_903";
const char *UT_TABLESPACE_FILE_NAME_4 = "ut_tablespace_file_904";
static const uint16 BITS_EXTENT_128 = (ALIGN_BYTE_EXTENT_128 << 3);
static const uint16 BITS_EXTENT_1024 = (ALIGN_BYTE_EXTENT_1024 << 3);
static const uint16 BITS_EXTENT_8192 = (ALIGN_BYTE_EXTENT_8192 << 3);

char curPath[MAXPGPATH];

struct Param {
    TableSpace *tablespace;
    FileId fileId;
    PageId *page_id;
};

struct ExtentParam {
    TableSpace *tablespace;
    ExtentSize extentSize;
    PageId pageId;
    uint16 loopCount;
};

class TableSpaceTest : public DSTORETEST {
protected:
    void SetUp() override
    {
        DSTORETEST::SetUp();
        UtInstance *instance = DstoreNew(m_ut_memory_context) UtInstance();
        instance->Install(&DSTORETEST::m_guc, m_ut_memory_context);
        instance->Startup(&DSTORETEST::m_guc);
        getcwd(curPath, MAXPGPATH);
        m_vfs = g_storageInstance->GetPdb(PDB_TEMPLATE1_ID)->GetVFS();
    }

    void TearDown() override
    {
        UtInstance *instance = (UtInstance *)g_storageInstance;
        instance->Shutdown();
        delete instance;
        DSTORETEST::TearDown();
    }

    static RetStatus UTFreeExtent(TableSpace *tablespace, ExtentSize extentSize, const PageId &extentPageId)
    {
        thrd->m_walWriterContext->BeginAtomicWal(INVALID_XID);
        RetStatus retStatus = tablespace->FreeExtent(extentSize, extentPageId);
        thrd->m_walWriterContext->EndAtomicWal();
        return retStatus;
    }

    static void * AllocExtentInThread(void *args) {
        printf("entering thread %lu\n", pthread_self());
        ((UtInstance *)g_storageInstance)->ThreadSetupAndRegister();
        Param* param = (Param*)args;
        TableSpace *tablespace = param->tablespace;
        bool isReusedFlag = false;
        PageId *page_id = param->page_id;
        for (int i = 0; i < 1; i++) {
            printf("allocate\n");
            tablespace->AllocExtent(EXT_SIZE_8, page_id, &isReusedFlag);
        }
        ((UtInstance *)g_storageInstance)->ThreadUnregisterAndExit();
    }

    static void * FreeExtentInThread(void *args) {
        printf("entering thread %lu\n", pthread_self());
        ((UtInstance *)g_storageInstance)->ThreadSetupAndRegister();
        ExtentParam* param = (ExtentParam*)args;
        TableSpace *tablespace = param->tablespace;
        ExtentSize extentSize = param->extentSize;
        uint16 loopCount = param->loopCount;
        PageId pageId = param->pageId;
        for (int i = 0; i < loopCount; i++) {
            UTFreeExtent(tablespace, extentSize, pageId);
            pageId.m_blockId -= static_cast<uint32>(extentSize);
        }
        ((UtInstance *)g_storageInstance)->ThreadUnregisterAndExit();
    }

    static void * DropSegmentInThread(void *args) {
        printf("entering thread %lu\n", pthread_self());
        ((UtInstance *)g_storageInstance)->ThreadSetupAndRegister();
        HeapNormalSegment* segment = (HeapNormalSegment*)args;
        for (int i = 0; i < 1; i++) {
            (void) segment->DropSegment();
        }
        ((UtInstance *)g_storageInstance)->ThreadUnregisterAndExit();
    }

    VFSAdapter *m_vfs;
};

TEST_F(TableSpaceTest, CheckSystablespace_level0)
{
    char fileName[MAXPGPATH] = {0};
    for (int i = 5121; i <= 5122; i++) {
        RetStatus ret = m_vfs->GetFileNameFromFileId(i, fileName);
        ASSERT_EQ(m_vfs->FileExists(i, fileName), true);
    }
    ASSERT_EQ(m_vfs->FileExists(5123, "5123"), false);
}

TEST_F(TableSpaceTest, AllocAndAddDataFile_level0)
{
    StoragePdb *pdb = g_storageInstance->GetPdb(g_defaultPdbId);
    ControlFile *controlFile = pdb->GetControlFile();
    TablespaceId tablespaceId = 0;
    FileId fileId;

    RetStatus ret = controlFile->AllocTbsId(&tablespaceId, MAX_TABLESPACE_SIZE, false);
    ASSERT_EQ(ret, DSTORE_SUCC);
    ASSERT_EQ(tablespaceId, 25);

    TableSpace tablespace(controlFile, tablespaceId);
    ret = tablespace.AllocAndAddDataFile(pdb->GetPdbId(), &fileId, EXT_SIZE_8, false);
    ASSERT_EQ(ret, DSTORE_SUCC);
    ASSERT_EQ(fileId, 5123);

    ControlTablespacePageItemData *tbsItem = controlFile->GetTbsPageItemPtr(tablespaceId, nullptr);
    ASSERT_EQ(tbsItem->tablespaceId, tablespaceId);
    ASSERT_EQ(tbsItem->hwm, 1);
    ASSERT_EQ(tbsItem->tbsMaxSize, MAX_TABLESPACE_SIZE);
    ASSERT_EQ(tbsItem->reuseVersion, 1);
    ASSERT_NE(tbsItem->ddlXid.m_placeHolder, 0);
    ASSERT_EQ(tbsItem->used, 1);
    ASSERT_EQ(tbsItem->fileIds[0], fileId);
    ASSERT_EQ(tbsItem->fileIds[1], 0);

    ControlDataFilePageItemData *fileItem = controlFile->GetDataFilePageItemPtr(fileId, nullptr);
    ASSERT_EQ(fileItem->fileId, fileId);
    ASSERT_EQ(fileItem->used, 1);
    ASSERT_EQ(fileItem->extentSize, EXT_SIZE_8);
    ASSERT_EQ(fileItem->reuseVersion, 1);
    ASSERT_EQ(fileItem->fileMaxSize, MAX_FILE_SIZE);
    ASSERT_EQ(fileItem->tablespaceId, tablespaceId);
    ASSERT_NE(fileItem->ddlXid.m_placeHolder, 0);

    char fileName[MAXPGPATH] = {0};
    ret = m_vfs->GetFileNameFromFileId(fileId, fileName);
    ASSERT_EQ(m_vfs->FileExists(fileId, fileName), true);

    TablespaceMgr *tablespaceMgr = g_storageInstance->GetPdb(g_defaultPdbId)->GetTablespaceMgr();
    TableSpace *tbs = tablespaceMgr->OpenTablespace(tablespaceId, DSTORE::DSTORE_ACCESS_EXCLUSIVE_LOCK);
    TbsDataFile **datafiles = tablespaceMgr->GetDataFiles();
    ASSERT_EQ(tbs->FreeAndRemoveDataFile(g_defaultPdbId, tablespaceId, datafiles[fileId], DSTORE::INVALID_XID, false),
              DSTORE_SUCC);
    ASSERT_EQ(controlFile->FreeTbsId(tablespaceId, DSTORE::INVALID_XID, false), DSTORE_SUCC);
    tablespaceMgr->CloseTablespace(tbs, DSTORE::DSTORE_ACCESS_EXCLUSIVE_LOCK);
}

TEST_F(TableSpaceTest, AllocTablespaceId_level0)
{
    TablespaceId tablespaceId = 0;
    RetStatus ret = TableSpace_Interface::AllocTablespaceId(DSTORE::INVALID_PDB_ID, MAX_TABLESPACE_SIZE, &tablespaceId);
    ASSERT_EQ(ret, DSTORE_FAIL);
    ret = TableSpace_Interface::AllocTablespaceId(g_defaultPdbId, MAX_TABLESPACE_SIZE, &tablespaceId);
    ASSERT_NE(tablespaceId, DSTORE::INVALID_TABLESPACE_ID);
    ret = TableSpace_Interface::FreeTablespaceId(DSTORE::INVALID_PDB_ID, tablespaceId, 0, false);
    ASSERT_EQ(ret, DSTORE_FAIL);
    ret = TableSpace_Interface::FreeTablespaceId(g_defaultPdbId, tablespaceId, 0, false);
    ASSERT_EQ(ret, DSTORE_SUCC);
}

TEST_F(TableSpaceTest, RemoveTempFileTest001_level1)
{
    StoragePdb *pdb = g_storageInstance->GetPdb(g_defaultPdbId);
    TablespaceId tempTbsId = static_cast<TablespaceId>(TBS_ID::TEMP_TABLE_SPACE_ID);
    TablespaceMgr *tablespaceMgr = pdb->GetTablespaceMgr();
    TableSpace *tablespace = tablespaceMgr->OpenTablespace(tempTbsId, DSTORE::DSTORE_EXCLUSIVE_LOCK);
    FileId fileId;
    ExtentSize extentSize = EXT_SIZE_8;
    NodeId selfNodeId = g_storageInstance->GetGuc()->selfNodeId;/* selfNodeId = 1 */

    RetStatus ret = tablespace->AllocAndAddDataFile(pdb->GetPdbId(), &fileId, extentSize, false);
    ASSERT_EQ(ret, DSTORE_SUCC);
    ASSERT_EQ(fileId, selfNodeId);

    ControlFile *controlFile = pdb->GetControlFile();
    ControlTablespacePageItemData *tbsItem = controlFile->GetTbsPageItemPtr(tempTbsId, nullptr);

    tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_EXCLUSIVE_LOCK);
    tablespace = tablespaceMgr->OpenTablespace(tempTbsId, DSTORE::DSTORE_EXCLUSIVE_LOCK);

    TbsDataFile *tbsDataFile = tablespaceMgr->GetDataFiles()[selfNodeId];
    ASSERT_EQ(tablespace->RemoveTempFilesWithAssociated(tbsDataFile, selfNodeId, selfNodeId, nullptr, 0, true), DSTORE_SUCC);

    char fileName[MAXPGPATH];
    int rc = sprintf_s(fileName, MAXPGPATH, "%d", selfNodeId);
    storage_securec_check_ss(rc);

    ASSERT_FALSE(m_vfs->FileExists(selfNodeId, fileName));
    tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_EXCLUSIVE_LOCK);
}

TEST_F(TableSpaceTest, RemoveTempFileTest002_level1)
{
    StoragePdb *pdb = g_storageInstance->GetPdb(g_defaultPdbId);
    TablespaceId tempTbsId = static_cast<TablespaceId>(TBS_ID::TEMP_TABLE_SPACE_ID);
    TablespaceMgr *tablespaceMgr = pdb->GetTablespaceMgr();
    TableSpace *tablespace = tablespaceMgr->OpenTablespace(tempTbsId, DSTORE::DSTORE_EXCLUSIVE_LOCK);
    TbsDataFile **dataFiles = tablespaceMgr->GetDataFiles();

    FileId fileId;
    ExtentSize extentSize = EXT_SIZE_8;
    NodeId selfNodeId = g_storageInstance->GetGuc()->selfNodeId;
    ASSERT_EQ(selfNodeId, 1);
    /* file 1 is valid and in nodeList, should not be deleted */
    RetStatus ret = tablespace->AllocAndAddDataFile(pdb->GetPdbId(), &fileId, extentSize, false);
    ASSERT_EQ(ret, DSTORE_SUCC);
    ASSERT_EQ(fileId, selfNodeId);

    ControlFile *controlFile = pdb->GetControlFile();
    char fileName[MAXPGPATH];
    FileSize fileSize;
    fileSize.initialFileSize = UT_INITIAL_FILE_SIZE;
    fileSize.maxFileSize = UT_MAX_FILE_SIZE;

    /* file 2 is valid and in nodeList, should not be deleted */
    fileId = 2;
    BlockNumber blkno;
    ControlDataFilePageItemData *datafileItem = controlFile->GetDataFilePageItemPtr(fileId, &blkno);
    datafileItem->SetItem(tempTbsId, extentSize, UT_MAX_FILE_SIZE);
    controlFile->m_controlTablespace->MarkPageDirty(blkno);
    int rc = sprintf_s(fileName, MAXPGPATH, "%d", fileId);
    storage_securec_check_ss(rc);
    ASSERT_EQ(tablespace->AddFile(fileId, fileSize, nullptr, EXT_SIZE_8), DSTORE_SUCC);
    ASSERT_TRUE(m_vfs->FileExists(fileName));

    /* file 3 is valid but not in nodeList, should be deleted */
    fileId = 3;
    datafileItem = controlFile->GetDataFilePageItemPtr(fileId, &blkno);
    datafileItem->SetItem(tempTbsId, extentSize, UT_MAX_FILE_SIZE);
    controlFile->m_controlTablespace->MarkPageDirty(blkno);
    rc = sprintf_s(fileName, MAXPGPATH, "%d", fileId);
    storage_securec_check_ss(rc);
    ASSERT_EQ(tablespace->AddFile(fileId, fileSize, nullptr, EXT_SIZE_8), DSTORE_SUCC);
    ASSERT_TRUE(m_vfs->FileExists(fileName));

    FileParameter m_filePara = {
        "ut_default_storespace",
        VFS_DEFAULT_FILE_STREAM_ID,
        IN_PLACE_WRITE_FILE,
        DATA_FILE_TYPE,
        (64 << 10),           /* 64KB */
        (1 << 30),              /* 1GB */
        0,
        FILE_READ_AND_WRITE_MODE,
        false,
    };
    BlockNumber maxBlock = 1000;

    uint32 nodeCount = 3;
    NodeId *nodeList = (NodeId *)DstorePalloc(sizeof(NodeId) * nodeCount);
    nodeList[0] = 1;
    nodeList[1] = 2;
    nodeList[2] = 5;

    tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_EXCLUSIVE_LOCK);
    tablespace = tablespaceMgr->OpenTablespace(tempTbsId, DSTORE::DSTORE_EXCLUSIVE_LOCK);

    ASSERT_EQ(tablespace->RemoveAllTempFiles(dataFiles, selfNodeId, nodeList, nodeCount, false), DSTORE_SUCC);

    fileId = 1;
    rc = sprintf_s(fileName, MAXPGPATH, "%d", fileId);
    storage_securec_check_ss(rc);
    ASSERT_TRUE(m_vfs->FileExists(fileId, fileName));
    fileId = 2;
    rc = sprintf_s(fileName, MAXPGPATH, "%d", fileId);
    storage_securec_check_ss(rc);
    ASSERT_TRUE(m_vfs->FileExists(fileId, fileName));
    fileId = 3;
    rc = sprintf_s(fileName, MAXPGPATH, "%d", fileId);
    storage_securec_check_ss(rc);
    ASSERT_FALSE(m_vfs->FileExists(fileId, fileName));

    tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_EXCLUSIVE_LOCK);
    DstorePfreeExt(nodeList);
}

TEST_F(TableSpaceTest, TablespaceInfo_level0)
{
    StoragePdb *pdb = g_storageInstance->GetPdb(g_defaultPdbId);
    ControlFile *controlFile = pdb->GetControlFile();
    TablespaceId tablespaceId = 0;
    FileId fileId;

    RetStatus ret = controlFile->AllocTbsId(&tablespaceId, MAX_TABLESPACE_SIZE, false);
    ASSERT_EQ(ret, DSTORE_SUCC);

    TableSpace tablespace(controlFile, tablespaceId);
    ret = tablespace.AllocAndAddDataFile(pdb->GetPdbId(), &fileId, EXT_SIZE_8, false);
    ASSERT_EQ(ret, DSTORE_SUCC);

    FileId *fileIds = nullptr;
    uint32_t fileIdCount = 0;

    PageId segment = TableSpace_Interface::AllocSegment(g_defaultPdbId, tablespaceId, SegmentType::HEAP_SEGMENT_TYPE);
    ASSERT_NE(segment, INVALID_PAGE_ID);

    ret = TableSpace_Interface::GetFileIdsByTablespaceId(DSTORE::INVALID_PDB_ID, tablespaceId, &fileIds, fileIdCount);
    ASSERT_EQ(ret, DSTORE_FAIL);
    ret = TableSpace_Interface::GetFileIdsByTablespaceId(g_defaultPdbId, INVALID_TABLESPACE_ID, &fileIds, fileIdCount);
    ASSERT_EQ(ret, DSTORE_FAIL);

    ret = TableSpace_Interface::GetFileIdsByTablespaceId(g_defaultPdbId, tablespaceId, &fileIds, fileIdCount);
    ASSERT_EQ(ret, DSTORE_SUCC);

    DstoreTablespaceAttr tbsAttr = {0};
    ret = TableSpace_Interface::DstoreGetTablespaceInfo(INVALID_PDB_ID, tablespaceId, &tbsAttr);
    ASSERT_EQ(ret, DSTORE_FAIL);
    ret = TableSpace_Interface::DstoreGetTablespaceInfo(g_defaultPdbId, tablespaceId, NULL);
    ASSERT_EQ(ret, DSTORE_FAIL);

    ret = TableSpace_Interface::DstoreGetTablespaceInfo(g_defaultPdbId, tablespaceId, &tbsAttr);
    ASSERT_EQ(ret, DSTORE_SUCC);

    DstoreDatafileAttr fileAttr = {0};
    ret = TableSpace_Interface::DstoreGetDatafileInfo(INVALID_PDB_ID, fileId, &fileAttr);
    ASSERT_EQ(ret, DSTORE_FAIL);
    ret = TableSpace_Interface::DstoreGetDatafileInfo(g_defaultPdbId, INVALID_DATA_FILE_ID, &fileAttr);
    ASSERT_EQ(ret, DSTORE_FAIL);
    ret = TableSpace_Interface::DstoreGetDatafileInfo(g_defaultPdbId, fileId, NULL);
    ASSERT_EQ(ret, DSTORE_FAIL);

    ret = TableSpace_Interface::DstoreGetDatafileInfo(g_defaultPdbId, fileId, &fileAttr);
    ASSERT_EQ(ret, DSTORE_SUCC);
}

TEST_F(TableSpaceTest, TablespaceMaxsize_level0)
{
    TablespaceId tablespaceId = static_cast<TablespaceId>(TBS_ID::DEFAULT_TABLE_SPACE_ID);
    uint64_t size = 0;

    RetStatus ret = TableSpace_Interface::DstoreGetTablespaceSize(DSTORE::INVALID_PDB_ID, tablespaceId, size);
    ASSERT_EQ(ret, DSTORE_FAIL);
    ret = TableSpace_Interface::DstoreGetTablespaceSize(g_defaultPdbId, INVALID_TABLESPACE_ID, size);
    ASSERT_EQ(ret, DSTORE_FAIL);
    ret = TableSpace_Interface::DstoreGetTablespaceSize(g_defaultPdbId, tablespaceId, size);
    ASSERT_EQ(ret, DSTORE_SUCC);

    ret = TableSpace_Interface::AlterMaxSize(tablespaceId, MAX_TABLESPACE_SIZE, DSTORE::INVALID_PDB_ID);
    ASSERT_EQ(ret, DSTORE_FAIL);

    ret = TableSpace_Interface::AlterMaxSize(tablespaceId, MAX_TABLESPACE_SIZE, g_defaultPdbId);
    ASSERT_EQ(ret, DSTORE_SUCC);
}

TEST_F(TableSpaceTest, RemainDatafile_level0)
{
    StoragePdb *pdb = g_storageInstance->GetPdb(g_defaultPdbId);
    ControlFile *controlFile = pdb->GetControlFile();
    TablespaceId tablespaceId = 0;
    FileId fileId;

    RetStatus ret = controlFile->AllocTbsId(&tablespaceId, MAX_TABLESPACE_SIZE, false);
    ASSERT_EQ(ret, DSTORE_SUCC);

    TableSpace tablespace(controlFile, tablespaceId);
    ret = tablespace.AllocAndAddDataFile(pdb->GetPdbId(), &fileId, EXT_SIZE_8, false);
    ASSERT_EQ(ret, DSTORE_SUCC);

    uint8_t isRemain = TableSpace_Interface::IsRemainDatafile(DSTORE::INVALID_PDB_ID, fileId);
    ASSERT_EQ(isRemain, INVALID_REMAIN_DATAFILE);
    isRemain = TableSpace_Interface::IsRemainDatafile(g_defaultPdbId, fileId);
    ASSERT_NE(isRemain, INVALID_REMAIN_DATAFILE);

   TableSpace_Interface:: FreeRemainDatafile(DSTORE::INVALID_PDB_ID, fileId);
   TableSpace_Interface:: FreeRemainDatafile(g_defaultPdbId, fileId);
}

TEST_F(TableSpaceTest, RemainTablespace_level0)
{
    StoragePdb *pdb = g_storageInstance->GetPdb(g_defaultPdbId);
    ControlFile *controlFile = pdb->GetControlFile();
    TablespaceId tablespaceId = 0;
    FileId fileId;

    RetStatus ret = controlFile->AllocTbsId(&tablespaceId, MAX_TABLESPACE_SIZE, false);
    ASSERT_EQ(ret, DSTORE_SUCC);

    TableSpace tablespace(controlFile, tablespaceId);
    ret = tablespace.AllocAndAddDataFile(pdb->GetPdbId(), &fileId, EXT_SIZE_8, false);
    ASSERT_EQ(ret, DSTORE_SUCC);

    uint8_t isRemain = TableSpace_Interface::IsRemainTablespace(DSTORE::INVALID_PDB_ID, tablespaceId, false);
    ASSERT_EQ(isRemain, INVALID_REMAIN_TABLESPACE);
    isRemain = TableSpace_Interface::IsRemainTablespace(g_defaultPdbId, INVALID_TABLESPACE_ID, false);
    ASSERT_EQ(isRemain, INVALID_REMAIN_TABLESPACE);
    isRemain = TableSpace_Interface::IsRemainTablespace(g_defaultPdbId, tablespaceId, true);
    ASSERT_EQ(isRemain, NOT_REMAIN_TABLESPACE);
    isRemain = TableSpace_Interface::IsRemainTablespace(g_defaultPdbId, tablespaceId, false);
    ASSERT_NE(isRemain, INVALID_REMAIN_TABLESPACE);

    TableSpace_Interface::FreeRemainTablespace(g_defaultPdbId, tablespaceId, true, 0);
    TableSpace_Interface::FreeRemainTablespace(DSTORE::INVALID_PDB_ID, tablespaceId, false, 0);
    TableSpace_Interface::FreeRemainTablespace(g_defaultPdbId, INVALID_TABLESPACE_ID, false, 0);
    TableSpace_Interface::FreeRemainTablespace(g_defaultPdbId, tablespaceId, false, 0);
}

TEST_F(TableSpaceTest, BatchAllocAndAddDataFile_level0)
{
    StoragePdb *pdb = g_storageInstance->GetPdb(g_defaultPdbId);
    ControlFile *controlFile = pdb->GetControlFile();
    TablespaceId tablespaceId = 0;
    FileId fileId;

    RetStatus ret = controlFile->AllocTbsId(&tablespaceId, MAX_TABLESPACE_SIZE, false);
    ASSERT_EQ(ret, DSTORE_SUCC);

    TableSpace tablespace(controlFile, tablespaceId);

    FileId fileIds[MAX_SPACE_FILE_COUNT] = {0};
    uint64_t fileIdCnt = 0;
    ret = TableSpace_Interface::BatchAllocAndAddDataFile(DSTORE::INVALID_PDB_ID, tablespaceId, fileIds, &fileIdCnt, false);
    ASSERT_EQ(ret, DSTORE_FAIL);
    ret = TableSpace_Interface::BatchAllocAndAddDataFile(g_defaultPdbId, INVALID_TABLESPACE_ID, fileIds, &fileIdCnt, false);
    ASSERT_EQ(ret, DSTORE_FAIL);
    ret = TableSpace_Interface::BatchAllocAndAddDataFile(g_defaultPdbId, tablespaceId, NULL, &fileIdCnt, false);
    ASSERT_EQ(ret, DSTORE_FAIL);
    ret = TableSpace_Interface::BatchAllocAndAddDataFile(g_defaultPdbId, tablespaceId, fileIds, &fileIdCnt, false);
    ASSERT_EQ(ret, DSTORE_SUCC);

    ret = TableSpace_Interface::BatchFreeAndRemoveDataFile(DSTORE::INVALID_PDB_ID, tablespaceId, fileIds, fileIdCnt, 0);
    ASSERT_EQ(ret, DSTORE_FAIL);
    ret = TableSpace_Interface::BatchFreeAndRemoveDataFile(g_defaultPdbId, INVALID_TABLESPACE_ID, fileIds, fileIdCnt, 0);
    ASSERT_EQ(ret, DSTORE_FAIL);
    ret = TableSpace_Interface::BatchFreeAndRemoveDataFile(g_defaultPdbId, tablespaceId, NULL, fileIdCnt, 0);
    ASSERT_EQ(ret, DSTORE_FAIL);
    ret = TableSpace_Interface::BatchFreeAndRemoveDataFile(g_defaultPdbId, tablespaceId, fileIds, fileIdCnt, 0);
    ASSERT_EQ(ret, DSTORE_SUCC);
}

TEST_F(TableSpaceTest, LogicalCreateTbsRedo_level0) {

    /* write to control file */
    TablespaceId tablespaceId;
    StoragePdb *pdb = g_storageInstance->GetPdb(g_defaultPdbId);
    ControlFile *controlFile = pdb->GetControlFile();
    controlFile->AllocTbsId(&tablespaceId, MAX_TABLESPACE_SIZE, true);

    BlockNumber blkno;
    ControlTablespacePageItemData *tbsItem = controlFile->GetTbsPageItemPtr(tablespaceId, &blkno);
    ControlTablespacePageItemData copy;
    copy.tablespaceId = tbsItem->tablespaceId;
    copy.hwm = tbsItem->hwm;
    copy.tbsMaxSize = tbsItem->tbsMaxSize;
    copy.reuseVersion = tbsItem->reuseVersion;
    copy.ddlXid = INVALID_XID;
    copy.used = tbsItem->used;
    tbsItem->ResetItem(INVALID_XID);
    controlFile->m_controlTablespace->MarkPageDirty(blkno);

    /* wal record */
    WalRecordTbsCreateTablespace walData;
    walData.SetHeader(WAL_TBS_CREATE_TABLESPACE, sizeof(WalRecordTbsCreateTablespace));
    walData.SetData(tablespaceId, MAX_TABLESPACE_SIZE, tbsItem->reuseVersion - 1, INVALID_XID);

    /* redo wal record  first time*/
    WalRecordRedoContext redoCtx = {INVALID_XID, DSTORE::INVALID_WAL_ID, g_defaultPdbId, DSTORE::INVALID_PLSN};
    walData.Redo(&redoCtx);

    BlockNumber tbs_blkno;
    tbsItem = controlFile->GetTbsPageItemPtr(walData.tablespaceId, &tbs_blkno);

    ASSERT_EQ(tablespaceId, walData.tablespaceId);
    ASSERT_EQ(blkno, tbs_blkno);
    ASSERT_EQ(copy.tablespaceId, tbsItem->tablespaceId);
    ASSERT_EQ(copy.hwm, tbsItem->hwm);
    ASSERT_EQ(copy.tbsMaxSize, tbsItem->tbsMaxSize);
    ASSERT_EQ(copy.reuseVersion + 1, tbsItem->reuseVersion);
    ASSERT_EQ(copy.ddlXid, INVALID_XID);
    ASSERT_EQ(copy.used, tbsItem->used);

    /* redo again */
    walData.Redo(&redoCtx);
    tbsItem = controlFile->GetTbsPageItemPtr(walData.tablespaceId, &tbs_blkno);
    ASSERT_EQ(tablespaceId, walData.tablespaceId);
    ASSERT_EQ(blkno,tbs_blkno);
    ASSERT_EQ(copy.tablespaceId, tbsItem->tablespaceId);
    ASSERT_EQ(copy.hwm, tbsItem->hwm);
    ASSERT_EQ(copy.tbsMaxSize, tbsItem->tbsMaxSize);
    ASSERT_EQ(copy.reuseVersion + 1, tbsItem->reuseVersion);
    ASSERT_EQ(copy.ddlXid, INVALID_XID);
    ASSERT_EQ(copy.used, tbsItem->used);
}

TEST_F(TableSpaceTest, LogicalCreateDataFileRedo_level0) {
    TablespaceId tablespaceId;
    FileId fileId;

    StoragePdb *pdb = g_storageInstance->GetPdb(g_defaultPdbId);
    ControlFile *controlFile = pdb->GetControlFile();
    controlFile->AllocTbsId(&tablespaceId, MAX_TABLESPACE_SIZE, true);

    TablespaceMgr *tablespaceMgr = pdb->GetTablespaceMgr();
    TableSpace *tablespace = tablespaceMgr->OpenTablespace(tablespaceId, DSTORE::DSTORE_EXCLUSIVE_LOCK);
    ExtentSize extentSize = EXT_SIZE_8;
    RetStatus ret = tablespace->AllocAndAddDataFile(tablespaceMgr->GetPdbId(), &fileId, extentSize, false);
    ASSERT_EQ(ret, DSTORE_SUCC);

    VFSAdapter *vfs = pdb->GetVFS();
    char fileName[MAXPGPATH];
    vfs->GetFileNameFromFileId(fileId, fileName);
    ASSERT_EQ(true, vfs->FileExists(fileName));

    BlockNumber blkno;
    ControlDataFilePageItemData *datafileItem = controlFile->GetDataFilePageItemPtr(fileId, &blkno);
    ASSERT_EQ(datafileItem->used, 1);
    ControlDataFilePageItemData copy = *datafileItem;
    tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_EXCLUSIVE_LOCK);
    
    tablespace = tablespaceMgr->OpenTablespace(tablespaceId, DSTORE::DSTORE_EXCLUSIVE_LOCK);
    TbsDataFile *tbsDataFile = tablespaceMgr->GetDataFiles()[fileId];
    ret = tablespace->FreeAndRemoveDataFile(tablespaceMgr->GetPdbId(), tablespaceId, tbsDataFile, INVALID_XID, false);
    ASSERT_EQ(ret, DSTORE_SUCC);
    tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_EXCLUSIVE_LOCK);

    datafileItem = controlFile->GetDataFilePageItemPtr(fileId, &blkno);
    ASSERT_EQ(datafileItem->used, 0);
    ASSERT_EQ(false, vfs->FileExists(fileName));

    /* wal record */
    WalRecordTbsCreateDataFile walData;
    walData.SetHeader(WAL_TBS_CREATE_DATA_FILE, sizeof(WalRecordTbsCreateDataFile));
    walData.SetData(tablespaceId, fileId, copy.fileMaxSize, copy.extentSize, copy.reuseVersion, INVALID_XID);

    WalRecordRedoContext redoCtx = {INVALID_XID, DSTORE::INVALID_WAL_ID, g_defaultPdbId, DSTORE::INVALID_PLSN};
    walData.Redo(&redoCtx);
    BlockNumber file_blkno;
    ControlDataFilePageItemData *fileItem = controlFile->GetDataFilePageItemPtr(fileId, &file_blkno);
    ASSERT_EQ(blkno,file_blkno);
    ASSERT_EQ(copy.tablespaceId, fileItem->tablespaceId);
    ASSERT_EQ(copy.fileMaxSize, fileItem->fileMaxSize);
    ASSERT_EQ(copy.reuseVersion + 1, fileItem->reuseVersion);
    ASSERT_EQ(copy.ddlXid, INVALID_XID);
    ASSERT_EQ(copy.used, fileItem->used);

    ASSERT_EQ(true, vfs->FileExists(fileName));
}

TEST_F(TableSpaceTest, LogicalAddFileToTbsRedo_level0) {
    TablespaceId tablespaceId;
    StoragePdb *pdb = g_storageInstance->GetPdb(g_defaultPdbId);
    ControlFile *controlFile = pdb->GetControlFile();
    controlFile->AllocTbsId(&tablespaceId, MAX_TABLESPACE_SIZE, true);

    BlockNumber blkno;
    ControlTablespacePageItemData *tbsItem = controlFile->GetTbsPageItemPtr(tablespaceId, &blkno);

    /* wal record */
    WalRecordTbsAddFileToTbs walData;
    FileId fileId = 233;
    uint16 slotId = 5;
    walData.SetHeader(WAL_TBS_ADD_FILE_TO_TABLESPACE, sizeof(WalRecordTbsAddFileToTbs));
    walData.SetData(tbsItem->tablespaceId, slotId, fileId, tbsItem->reuseVersion,
            tbsItem->ddlXid, slotId);

    WalRecordRedoContext redoCtx = {INVALID_XID, DSTORE::INVALID_WAL_ID, g_defaultPdbId, DSTORE::INVALID_PLSN};
    walData.Redo(&redoCtx);
    controlFile->m_controlTablespace->MarkPageDirty(blkno);
    BlockNumber tbs_blkno;
    tbsItem = controlFile->GetTbsPageItemPtr(tablespaceId, &tbs_blkno);
    ASSERT_EQ(blkno, tbs_blkno);
    ASSERT_EQ(slotId, tbsItem->hwm);
    ASSERT_EQ(MAX_TABLESPACE_SIZE, tbsItem->tbsMaxSize);
    ASSERT_EQ(walData.preReuseVersion, tbsItem->reuseVersion);
    ASSERT_EQ(INVALID_XID, tbsItem->ddlXid);
    ASSERT_EQ(fileId, tbsItem->fileIds[slotId]);
    ASSERT_EQ(1, tbsItem->used);
}

TEST_F(TableSpaceTest, LogicalDropTbsRedo_level0) {
    TablespaceId tablespaceId;
    StoragePdb *pdb = g_storageInstance->GetPdb(g_defaultPdbId);
    ControlFile *controlFile = pdb->GetControlFile();
    controlFile->AllocTbsId(&tablespaceId, MAX_TABLESPACE_SIZE, true);

    BlockNumber blkno;
    ControlTablespacePageItemData *tbsItem = controlFile->GetTbsPageItemPtr(tablespaceId, &blkno);

    /* wal record */
    WalRecordTbsDropTablespace walData;
    BlockNumber tbs_blkno;
    walData.SetHeader(WAL_TBS_DROP_TABLESPACE, sizeof(WalRecordTbsDropTablespace));
    walData.SetData(tablespaceId, 0, tbsItem->reuseVersion, INVALID_XID);

    WalRecordRedoContext redoCtx = {INVALID_XID, DSTORE::INVALID_WAL_ID, g_defaultPdbId, DSTORE::INVALID_PLSN};
    walData.Redo(&redoCtx);
    controlFile->m_controlTablespace->MarkPageDirty(blkno);
    tbsItem = controlFile->GetTbsPageItemPtr(walData.tablespaceId, &tbs_blkno);
    ASSERT_EQ(blkno,tbs_blkno);
    ASSERT_EQ(0, tbsItem->hwm);
    ASSERT_EQ(0, tbsItem->tbsMaxSize);
    ASSERT_EQ(walData.preReuseVersion + 1, tbsItem->reuseVersion);
    ASSERT_EQ(INVALID_XID, tbsItem->ddlXid);
    ASSERT_EQ(0, tbsItem->used);
}

TEST_F(TableSpaceTest, LogicalAlterTbsRedo_level0) {
    TablespaceId tablespaceId;
    StoragePdb *pdb = g_storageInstance->GetPdb(g_defaultPdbId);
    ControlFile *controlFile = pdb->GetControlFile();
    controlFile->AllocTbsId(&tablespaceId, MAX_TABLESPACE_SIZE, true);

    BlockNumber blkno;
    ControlTablespacePageItemData *tbsItem = controlFile->GetTbsPageItemPtr(tablespaceId, &blkno);

    /* wal record */
    WalRecordTbsAlterTablespace walData;
    BlockNumber tbs_blkno;
    uint64 tbsMaxSize = 1000;
    walData.SetHeader(WAL_TBS_ALTER_TABLESPACE, sizeof(WalRecordTbsAlterTablespace));
    walData.SetData(tablespaceId, tbsMaxSize, tbsItem->reuseVersion, INVALID_XID);

    WalRecordRedoContext redoCtx = {INVALID_XID, DSTORE::INVALID_WAL_ID, g_defaultPdbId, DSTORE::INVALID_PLSN};
    walData.Redo(&redoCtx);
    tbsItem = controlFile->GetTbsPageItemPtr(walData.tablespaceId, &tbs_blkno);
    ASSERT_EQ(blkno,tbs_blkno);
    ASSERT_EQ(tbsItem->tbsMaxSize,tbsMaxSize);
}

TEST_F(TableSpaceTest, LogicalDropDatafileRedo_level0) {
    TablespaceId tablespaceId;
    FileId fileId;
 
    StoragePdb *pdb = g_storageInstance->GetPdb(g_defaultPdbId);
    ControlFile *controlFile = pdb->GetControlFile();
    controlFile->AllocTbsId(&tablespaceId, MAX_TABLESPACE_SIZE, false);
 
    TablespaceMgr *tablespaceMgr = pdb->GetTablespaceMgr();
    TableSpace *tablespace = tablespaceMgr->OpenTablespace(tablespaceId, DSTORE::DSTORE_EXCLUSIVE_LOCK);
 
    BlockNumber blkno;
    BlockNumber tbs_blkno;
    uint16 slotId;
    ExtentSize extentSize = EXT_SIZE_8;
    RetStatus ret = tablespace->AllocAndAddDataFile(g_defaultPdbId, &fileId, extentSize, false);
    ASSERT_EQ(ret, DSTORE_SUCC);
 
    ControlDataFilePageItemData *datafileItem = controlFile->GetDataFilePageItemPtr(fileId, &blkno);
    controlFile->AddFileIdToTbs(tablespaceId, fileId, &slotId, false);
    ControlTablespacePageItemData *tbsItem = controlFile->GetTbsPageItemPtr(tablespaceId, &tbs_blkno);
  
    WalRecordTbsDropDataFile walData;
    walData.SetHeader(WAL_TBS_DROP_DATA_FILE, sizeof(WalRecordTbsDropDataFile));
    walData.SetData(tablespaceId, tbsItem->hwm, fileId, datafileItem->reuseVersion, INVALID_XID, slotId);
    WalRecordRedoContext redoCtx = {INVALID_XID, DSTORE::INVALID_WAL_ID, g_defaultPdbId, DSTORE::INVALID_PLSN};
    walData.Redo(&redoCtx);
    ControlDataFilePageItemData *fileItem = controlFile->GetDataFilePageItemPtr(fileId, &blkno);
    ControlTablespacePageItemData *item = controlFile->GetTbsPageItemPtr(tablespaceId, &tbs_blkno);
    ASSERT_EQ(INVALID_EXT_SIZE, fileItem->extentSize);
    ASSERT_EQ(0, fileItem->fileMaxSize);
    ASSERT_EQ(walData.preReuseVersion + 1, fileItem->reuseVersion);
    ASSERT_EQ(INVALID_XID, fileItem->ddlXid);
    ASSERT_EQ(0, fileItem->used);
    ASSERT_EQ(INVALID_DATA_FILE_ID, item->fileIds[slotId]);
    
    VFSAdapter *vfs = pdb->GetVFS();
    char fileName[MAXPGPATH];
    vfs->GetFileNameFromFileId(fileId, fileName);
    ASSERT_EQ(false, vfs->FileExists(fileName));
    tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_EXCLUSIVE_LOCK);
}

TEST_F(TableSpaceTest, LogicalDropRemainfileRedo_level0) {
    TablespaceId tablespaceId;
    FileId fileId;
 
    StoragePdb *pdb = g_storageInstance->GetPdb(g_defaultPdbId);
    ControlFile *controlFile = pdb->GetControlFile();
    controlFile->AllocTbsId(&tablespaceId, MAX_TABLESPACE_SIZE, false);
 
    TablespaceMgr *tablespaceMgr = pdb->GetTablespaceMgr();
    TableSpace *tablespace = tablespaceMgr->OpenTablespace(tablespaceId, DSTORE::DSTORE_EXCLUSIVE_LOCK);
 
    BlockNumber blkno;
    BlockNumber tbs_blkno;
    uint16 slotId;
    ExtentSize extentSize = EXT_SIZE_8;
    RetStatus ret = tablespace->AllocAndAddDataFile(g_defaultPdbId, &fileId, extentSize, false);
    ASSERT_EQ(ret, DSTORE_SUCC);

    VFSAdapter *vfs = pdb->GetVFS();
    char fileName[MAXPGPATH];
    vfs->GetFileNameFromFileId(fileId, fileName);
    ASSERT_EQ(vfs->FileExists(fileName), true);
    
    ControlDataFilePageItemData *datafileItem = controlFile->GetDataFilePageItemPtr(fileId, &blkno);
    uint64 reuseVersion = datafileItem->reuseVersion;
    controlFile->AddFileIdToTbs(tablespaceId, fileId, &slotId, false);

    WalRecordTbsDropDataFile walData;
    walData.SetHeader(WAL_TBS_DROP_DATA_FILE, sizeof(WalRecordTbsDropDataFile));
    walData.SetData(INVALID_TABLESPACE_ID, 0, fileId, reuseVersion, INVALID_XID, MAX_TBS_DATAFILE_SLOT_CNT);
    WalRecordRedoContext redoCtx = {INVALID_XID, DSTORE::INVALID_WAL_ID, g_defaultPdbId, DSTORE::INVALID_PLSN};
    walData.Redo(&redoCtx);
    ControlDataFilePageItemData *fileItem = controlFile->GetDataFilePageItemPtr(fileId, &blkno);
    ControlTablespacePageItemData *item = controlFile->GetTbsPageItemPtr(tablespaceId, &tbs_blkno);
    ASSERT_EQ(fileItem->extentSize, INVALID_EXT_SIZE);
    ASSERT_EQ(fileItem->used, 0);
    ASSERT_EQ(fileItem->reuseVersion, reuseVersion + 1);
    ASSERT_EQ(fileItem->ddlXid, INVALID_XID);
    ASSERT_EQ(fileItem->fileMaxSize, 0);
    ASSERT_EQ(fileItem->tablespaceId, INVALID_TABLESPACE_ID);
    ASSERT_EQ(fileItem->used, 0);
    ASSERT_EQ(item->fileIds[slotId], fileId);
    
    ASSERT_EQ(vfs->FileExists(fileName), false);
    tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_EXCLUSIVE_LOCK);
}
