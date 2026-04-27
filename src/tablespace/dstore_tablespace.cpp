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
 * dstore_tablespace.cpp
 *
 *
 *
 * IDENTIFICATION
 *        storage/src/page/dstore_tablespace.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "common/log/dstore_log.h"
#include "page/dstore_bitmap_meta_page.h"
#include "page/dstore_tbs_file_meta_page.h"
#include "securec.h"
#include "errorcode/dstore_tablespace_error_code.h"
#include "framework/dstore_vfs_adapter.h"
#include "tablespace/dstore_tablespace_wal.h"
#include "wal/dstore_wal_write_context.h"
#include "framework/dstore_session.h"
#include "tablespace/dstore_tablespace_wal.h"
#include "tablespace/dstore_tablespace.h"

namespace DSTORE {
TableSpace::TableSpace(ControlFile *controlFile, TablespaceId tablespaceId)
    : m_controlFile(controlFile),
      m_tablespaceId(tablespaceId),
      m_tablespaceName{},
      m_files{},
      m_fileCountByType{},
      m_fileCount(0),
      m_initedFileCount(0),
      m_isValid(true)
{
    m_pdbId = (controlFile != nullptr) ? controlFile->GetPdbId() : INVALID_PDB_ID;
    for (uint16 i = 0; i < EXTENT_TYPE_COUNT; i++) {
        m_allocExtents[i].SetPdbId(m_pdbId);
    }
    RWLockInit(&m_tbsCacheRWlock, RWLOCK_PREFER_WRITER_NP);
}

TableSpace::~TableSpace()
{
    m_controlFile = nullptr;
    RWLockDestroy(&m_tbsCacheRWlock);
}

ControlFile *TableSpace::GetControlFile()
{
    return m_controlFile;
}

RetStatus TableSpace::RemoveTempFilesWithAssociated(TbsDataFile *tbsDataFile, const NodeId selfNodeId, NodeId curNode,
                                                    const NodeId *nodeIdList, uint32 nodeCount, bool isStartup)
{
    bool isValidFile = false;
    for (uint16 i = 0; i < nodeCount; i++) {
        if (curNode == nodeIdList[i]) {
            isValidFile = true;
            break;
        }
    }
    /* Only startup need check selfnode temp files leftover */
    if (!isValidFile || (isStartup && curNode == selfNodeId)) {
        StorageReleasePanic(
            STORAGE_FUNC_FAIL(
                FreeAndRemoveDataFile(m_pdbId, m_tablespaceId, tbsDataFile, thrd->GetCurrentXid(), false)),
            MODULE_TABLESPACE,
            ErrMsg("[RemoveAllTempFiles]Failed to remove temp datafile, pdbId %u, fileId %u.", m_pdbId, curNode));
        /* Need to fresh invalid datafile */
        TablespaceMgr *tablespaceMgr = g_storageInstance->GetTablespaceMgrByPdbId(m_pdbId);
        if (STORAGE_VAR_NULL(tablespaceMgr)) {
            ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("[RemoveAllTempFiles]Failed to get temp tablespaceMgr."));
            return DSTORE_FAIL;
        }
        TbsDataFile *datafile = tablespaceMgr->AcquireDatafile(curNode, LW_SHARED);
        tablespaceMgr->ReleaseDatafileLock(datafile);
        ErrLog(DSTORE_LOG, MODULE_TABLESPACE,
               ErrMsg("[RemoveAllTempFiles]Remove temp files with associated success, pdbId %u, fileId %u.", m_pdbId,
                      curNode));
    }
    return DSTORE_SUCC;
}

void TableSpace::DeleteTempFile(VFSAdapter *vfs, char *fileName, FileId fId)
{
    if (vfs->FileExists(fileName)) {
        StorageReleasePanic(STORAGE_FUNC_FAIL(vfs->RemoveFile(fId, fileName)), MODULE_TABLESPACE,
            ErrMsg("[RemoveAllTempFiles]Failed to remove temp datafile(%s), pdbId %u.", fileName, m_pdbId));
        ErrLog(DSTORE_LOG, MODULE_TABLESPACE,
            ErrMsg("[RemoveAllTempFiles]Remove datafile(%s) success, pdbId %u.", fileName, m_pdbId));
    }
}

/* Clean temp files leftover */
RetStatus TableSpace::RemoveAllTempFiles(TbsDataFile **dataFiles, const NodeId selfNodeId,
                                         const NodeId *nodeIdList, uint32 nodeCount, bool isStartup)
{
    if (m_status != TbsStatus::TBS_OPENED || !IsTempTbs()) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("[RemoveAllTempFiles]Temporary tablespace not open, pdbId %u.", m_pdbId));
        return DSTORE_FAIL;
    }
    StoragePdb *pdb = g_storageInstance->GetPdb(m_pdbId);
    if (STORAGE_VAR_NULL(pdb)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to get pdb when remove all tempfiles, pdbId %u.", m_pdbId));
        return DSTORE_FAIL;
    }
    VFSAdapter *vfs = pdb->GetVFS();
    if (STORAGE_VAR_NULL(vfs)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to get vfs when remove all tempfiles, pdbId %u.", m_pdbId));
        return DSTORE_FAIL;
    }

    uint32 count = TMP_TBS_MAX_FILE_ID + 1;
    ControlDataFilePageItemData *datafileItems =
        static_cast<ControlDataFilePageItemData *>(DstorePalloc0(count * sizeof(ControlDataFilePageItemData)));
    if (STORAGE_VAR_NULL(datafileItems)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
                ErrMsg("[RemoveAllTempFiles]Failed to alloc memory for datafileItems."));
        return DSTORE_FAIL;
    }
    if (STORAGE_FUNC_FAIL(m_controlFile->GetALLDataFilePageItemDatasForTemp(datafileItems, count))) {
        DstorePfreeExt(datafileItems);
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("[RemoveAllTempFiles]Get datafile page items failed."));
        return DSTORE_FAIL;
    }
    ErrLog(DSTORE_LOG, MODULE_TABLESPACE, ErrMsg("[RemoveAllTempFiles]Get datafile page items success."));

    for (FileId fId = TMP_TBS_START_FILE_ID; fId <= TMP_TBS_MAX_FILE_ID; fId++) {
        NodeId curNode = fId;
        TbsDataFile *tbsDataFile = dataFiles[fId];

        if (isStartup && fId != selfNodeId) {
            continue;
        }

        /* step1: Clean temp file leftover associated with tablespace */
        if (tbsDataFile && tbsDataFile->GetDataFilePageItemData().used == DSTORE_DATAFILE_IS_USED) {
            ErrLog(DSTORE_LOG, MODULE_TABLESPACE, ErrMsg("[RemoveAllTempFiles]Step1, fileId:%u.", fId));
            RemoveTempFilesWithAssociated(tbsDataFile, selfNodeId, curNode, nodeIdList, nodeCount, isStartup);
            continue;
        }

        /* step2: Clean fileId leftover not associated with tablespace */
        char fileName[MAXPGPATH] = {0};
        vfs->GetFileNameFromFileId(fId, fileName);
        if (datafileItems[fId].used == DSTORE_DATAFILE_IS_USED) {
            ErrLog(DSTORE_LOG, MODULE_TABLESPACE, ErrMsg("[RemoveAllTempFiles]Step2, fileId:%u.", fId));
            /* Delete physical file first, then fileId. */
            DeleteTempFile(vfs, fileName, fId);
            if (STORAGE_FUNC_FAIL(m_controlFile->FreeDataFileId(fId, INVALID_TABLESPACE_ID, MAX_TBS_DATAFILE_SLOT_CNT,
                                                                thrd->GetCurrentXid(), false))) {
                DstorePfreeExt(datafileItems);
                ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("Free fileId(%u) failed.", fId));
                return DSTORE_FAIL;
            }
            ErrLog(DSTORE_LOG, MODULE_TABLESPACE,
                   ErrMsg("[RemoveAllTempFiles]Free fileId %u success, pdbId %u.", fId, m_pdbId));
            continue;
        }

        /* step3: Clean selfnode physical file leftover */
        if (isStartup && fId == selfNodeId) {
            ErrLog(DSTORE_LOG, MODULE_TABLESPACE, ErrMsg("[RemoveAllTempFiles]Step3, fileId:%u.", fId));
            DeleteTempFile(vfs, fileName, fId);
        }
    }
    DstorePfreeExt(datafileItems);
    return DSTORE_SUCC;
}

ControlTablespacePageItemData TableSpace::GetTbsPageItemData()
{
    return m_ctrlTablespaceItem;
}

void TableSpace::SetTbsPageItemData(ControlTablespacePageItemData &tbsPageItemData)
{
    m_ctrlTablespaceItem = tbsPageItemData;
}

static uint16 GetIndexByType(ExtentSize extentSize)
{
    switch (extentSize) {
        case EXT_SIZE_8:
            return 0;
        case EXT_SIZE_128:
            return 1;
        case EXT_SIZE_1024:
            return 2;   // 2 is not magic
        case EXT_SIZE_8192:
            return 3;   // 3 is not magic
        default:
            ErrLog(DSTORE_PANIC, MODULE_TABLESPACE,
                   ErrMsg("Invalid extentSize(%hu).", static_cast<uint16>(extentSize)));
    }
    return 0;
}

TbsAllocExtentContext TableSpace::GetExtentContextBySize(ExtentSize extentSize)
{
    return m_allocExtents[GetIndexByType(extentSize)];
}

void TableSpace::AddFileId(FileId fileid, ExtentSize extentSize)
{
    StorageAssert(extentSize != INVALID_EXT_SIZE);
    uint16 extentIndex = GetIndexByType(extentSize);
    m_allocExtents[extentIndex].SetExtentSize(extentSize);
    m_allocExtents[extentIndex].AddFileId(fileid);
}

RetStatus TableSpace::AllocExtent(ExtentSize extentSize, PageId *newExtentPageId, bool *isReUseFlag,
                                  bool *continueTryAlloc)
{
    StorageAssert(extentSize != INVALID_EXT_SIZE);
    uint16 extentIndex = GetIndexByType(extentSize);
    RetStatus ret = m_allocExtents[extentIndex].AllocExtent(newExtentPageId, isReUseFlag, continueTryAlloc);
    return ret;
}

bool TableSpace::IsTempTbs() const
{
    return (m_tablespaceId == static_cast<TablespaceId>(TBS_ID::TEMP_TABLE_SPACE_ID));
}

RetStatus TableSpace::FreeExtent(ExtentSize extentSize, const PageId &extentPageId)
{
    TablespaceMgr *tablespaceMgr = g_storageInstance->GetTablespaceMgrByPdbId(m_pdbId);
    if (STORAGE_VAR_NULL(tablespaceMgr)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to get tablespaceMgr when free extent, pdbId %u.", m_pdbId));
        return DSTORE_FAIL;
    }
    TbsDataFile **datafiles = tablespaceMgr->GetDataFiles();
    FileId fileId = extentPageId.m_fileId;
    TbsDataFile *dataFile = datafiles[fileId];
    if (STORAGE_VAR_NULL(dataFile)) {
        ErrLog(DSTORE_WARNING, MODULE_TABLESPACE,
               ErrMsg("Tablespace has no datafile with fileId %hu, pdbId %u.", fileId, m_pdbId));
        storage_set_error(TBS_ERROR_PARAMETER_INVALID);
        return DSTORE_FAIL;
    }
    if (extentSize != dataFile->GetExtentSize()) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
            ErrMsg("The extentSize(%hu) is invalid, extentSize in dataFile is %hu, fileId %hu, pdbId %u.",
                   static_cast<uint16>(extentSize), static_cast<uint16>(dataFile->GetExtentSize()), fileId, m_pdbId));
        return DSTORE_FAIL;
    }

    return dataFile->FreeExtent(extentPageId);
}

RetStatus TablespaceMgr::InitDatafile(FileId fileId)
{
    TbsDataFile *datafile = AcquireDatafile(fileId, LW_SHARED);
    if (STORAGE_VAR_NULL(datafile)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
                ErrMsg("Load datafile failed, fileId %hu, pdbId %u.", fileId, m_pdbId));
        return DSTORE_FAIL;
    }

    if (STORAGE_FUNC_FAIL(datafile->Init())) {
        ReleaseDatafileLock(datafile);
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Init datafile failed, fileId %hu, fileName %s, pdbId %u.", fileId, datafile->GetFileName(),
                      m_pdbId));
        return DSTORE_FAIL;
    }

    ReleaseDatafileLock(datafile);

    return DSTORE_SUCC;
}

RetStatus TablespaceMgr::CreateDatafile(ControlDataFilePageItemData fileItem)
{
    TenantConfig *tenantConfig = g_storageInstance->GetGuc()->tenantConfig;
    StorageAssert(tenantConfig);
    const char* storeSpaceName = tenantConfig->storeSpaces[0].storeSpaceName;
    uint64 initialFileSize = IsTemplate(m_pdbId) ? TEMPLATE_PDB_TBS_INIT_FILE_SIZE : INIT_FILE_SIZE;
    uint64 maxFileSize = MAX_FILE_SIZE;
    uint64 initBlockCount = static_cast<uint64>(initialFileSize / BLCKSZ);
    uint64 maxBlockCount = static_cast<uint64>(maxFileSize / BLCKSZ);

    TbsDataFile *datafile = NewDatafile(fileItem);
    if (STORAGE_VAR_NULL(datafile)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
                ErrMsg("New datafile failed when create file, pdbId %u, fileId %hu.", m_pdbId, fileItem.fileId));
        return DSTORE_FAIL;
    }

    /* create physical file */
    datafile->SetStoreSpaceName(storeSpaceName);
    datafile->SetExtentSize(fileItem.extentSize);
    datafile->SetMaxBlockCount(maxBlockCount);
    if (STORAGE_FUNC_FAIL(datafile->Create(initBlockCount, storeSpaceName))) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Init datafile failed, pdbId %u, fileId %hu, fileName %s.", m_pdbId, fileItem.fileId,
                      datafile->GetFileName()));
        DstorePfreeExt(datafile);
        return DSTORE_FAIL;
    }
    
    ErrLog(DSTORE_LOG, MODULE_TABLESPACE, ErrMsg("Create datafile success, pdbId %u, fileId %hu, fileName %s.",
        m_pdbId, fileItem.fileId, datafile->GetFileName()));
    DstorePfreeExt(datafile);
    return DSTORE_SUCC;
}

#ifdef UT
RetStatus TableSpace::AddFile(FileId fileId, const FileSize &fileSize, const char* storeSpaceName,
    ExtentSize extentSize)
{
    const uint64 UT_MAX_FILE_SIZE = (uint64)1024 * 1024 * 1024 * 128;
    BlockNumber blkno;
    ControlDataFilePageItemData *datafileItem = m_controlFile->GetDataFilePageItemPtr(fileId, &blkno);
    datafileItem->SetItem(m_tablespaceId, extentSize, UT_MAX_FILE_SIZE);
    m_controlFile->m_controlTablespace->MarkPageDirty(blkno);

    TablespaceMgr *tablespaceMgr = g_storageInstance->GetTablespaceMgrByPdbId(m_pdbId);
    if (STORAGE_VAR_NULL(tablespaceMgr)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to get tablespaceMgr when add file, pdbId %u.", m_pdbId));
        return DSTORE_FAIL;
    }
    if (STORAGE_FUNC_FAIL(tablespaceMgr->CreateDatafile(*datafileItem))) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Create and init datafile failed, tablespaceId %hu, fileId %hu.", m_tablespaceId, fileId));
        return DSTORE_FAIL;
    }

    if (STORAGE_FUNC_FAIL(tablespaceMgr->InitDatafile(fileId))) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("Failed to init datafile(%hu).", fileId));
        return DSTORE_FAIL;
    }

    uint16 slotId;
    if (STORAGE_FUNC_FAIL(m_controlFile->AddFileIdToTbs(m_tablespaceId, fileId, &slotId, false))) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Add file to tablespace failed, tablespaceId %hu, fileId %hu.", m_tablespaceId, fileId));
        return DSTORE_FAIL;
    }

    return DSTORE_SUCC;
}
#endif

uint32 TableSpace::GetFileCount() const
{
    return this->m_fileCount;
}

void TableSpace::IncreFileCount(uint16 step)
{
    this->m_fileCount += step;
}

void TableSpace::SetTablespaceStatus(TbsStatus status)
{
    this->m_status = status;
}

RetStatus TableSpace::AllocAndAddDataFile(PdbId pdbId, FileId *fileId, ExtentSize extentSize, bool needWal)
{
    /* step1: alloc fileId and create datafile */
    if (STORAGE_FUNC_FAIL(m_controlFile->AllocAndCreateDataFile(fileId, m_tablespaceId, extentSize, needWal))) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Alloc fileId and create datafile failed, pdbId %u, tablespaceId %hu.", pdbId, m_tablespaceId));
        return DSTORE_FAIL;
    }

    /* step2: init datafile */
    TablespaceMgr *tablespaceMgr = g_storageInstance->GetTablespaceMgrByPdbId(pdbId);
    if (STORAGE_VAR_NULL(tablespaceMgr)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to get tablespaceMgr when init datafile, pdbId %u.", pdbId));
        return DSTORE_FAIL;
    }

    if (STORAGE_FUNC_FAIL(tablespaceMgr->InitDatafile(*fileId))) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
            ErrMsg("Failed to init datafile, pdbId %u, tablespaceId %hu, fileId %hu.", pdbId, m_tablespaceId, *fileId));
        return DSTORE_FAIL;
    }

    /* step3: Associating fileId with tablespace */
    uint16 slotId;
    if (STORAGE_FUNC_FAIL(m_controlFile->AddFileIdToTbs(m_tablespaceId, *fileId, &slotId, needWal))) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Add file to tablespace failed, tablespaceId %hu, fileId %hu, pdbId %u.", m_tablespaceId, *fileId,
                      m_pdbId));
        return DSTORE_FAIL;
    }

    ErrLog(DSTORE_LOG, MODULE_TABLESPACE,
        ErrMsg("Alloc and add datafile %hu with extent_size %hu on tablespaceId %hu success. pdbId %u.", *fileId,
            extentSize, m_tablespaceId, m_pdbId));
    return DSTORE_SUCC;
}

/*
 * Need to OpenTablespace before invoke this function.
 */
RetStatus TableSpace::FreeAndRemoveDataFile(PdbId pdbId, TablespaceId tablespaceId, TbsDataFile *dataFile,
                                            Xid ddlXid, bool needWal)
{
    if (unlikely(tablespaceId == INVALID_TABLESPACE_ID || tablespaceId > MAX_TABLESPACE_ID ||
                 pdbId == INVALID_PDB_ID)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to remove datafile due to invalid parameters, pdbId %u, tablespaceId %hu.", pdbId,
                      tablespaceId));
        return DSTORE_FAIL;
    }

    if (STORAGE_VAR_NULL(dataFile)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to remove null datafile, tablespaceId %hu, pdbId %u.", tablespaceId, pdbId));
        return DSTORE_FAIL;
    }

    FileId fileId = dataFile->GetFileId();
    uint16 slotId;
    if (STORAGE_FUNC_FAIL(m_controlFile->FreeFileIdFromTbs(tablespaceId, fileId, &slotId))) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to disassociate the fileId %hu, tablespaceId %hu, pdbId %u.", fileId,
                      tablespaceId, pdbId));
        return DSTORE_FAIL;
    }

    if (STORAGE_FUNC_FAIL(m_controlFile->FreeDataFileId(fileId, tablespaceId, slotId, ddlXid, needWal))) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
            ErrMsg("Failed to free data fileId %hu, tablespaceId %hu, pdbId %u.", fileId,
                    tablespaceId, pdbId));
        return DSTORE_FAIL;
    }

    if (!IsTempTbs() && STORAGE_FUNC_FAIL(dataFile->GetBufferMgr()->InvalidateUsingGivenFileId(pdbId, fileId))) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to invalidate buffer by fileId %hu, tablespaceId %hu, pdbId %u.", fileId, tablespaceId,
                      pdbId));
        return DSTORE_FAIL;
    }

    if (STORAGE_FUNC_FAIL(dataFile->RemoveFile(dataFile->GetFileName()))) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to remove datafile of tablespace, fileId %hu, tablespaceId %hu, pdbId %u.", fileId,
                      tablespaceId, pdbId));
        return DSTORE_FAIL;
    }

    /* The invalidation message is sent to prevent other nodes from accessing dirty handles
     * after the file is deleted from a node. */
    if (STORAGE_FUNC_FAIL(m_controlFile->InvalidateFileId(fileId))) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
            ErrMsg("Failed to send invalid fileId message after remove file, fileId %hu, tablespaceId %hu, pdbId %u.",
                   fileId, tablespaceId, pdbId));
        return DSTORE_FAIL;
    }

    ErrLog(DSTORE_LOG, MODULE_TABLESPACE,
           ErrMsg("Free and remove datafile success, fileId %hu, tablespaceId %hu, pdbId %u.",
                  fileId, tablespaceId, pdbId));
    return DSTORE_SUCC;
}

RetStatus TableSpace::CreateSysTablespace(PdbId pdbId, TablespaceId tablespaceId)
{
    StorageAssert(tablespaceId != INVALID_TABLESPACE_ID);
    StorageAssert(tablespaceId <= static_cast<TablespaceId>(SYS_TBS_MAX_ID));

    if (STORAGE_FUNC_FAIL(m_controlFile->InitTbsId(tablespaceId, MAX_TABLESPACE_SIZE))) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("Init system tablespace(%u) failed", tablespaceId));
        return DSTORE_FAIL;
    }

    if (tablespaceId == static_cast<TablespaceId>(TBS_ID::TEMP_TABLE_SPACE_ID) ||
        tablespaceId == static_cast<TablespaceId>(TBS_ID::GLOBAL_TABLE_SPACE_ID)) {
        return DSTORE_SUCC;
    }

#ifndef UT
    FileId fileId;
    for (uint16 i = 0; i < EXTENT_TYPE_COUNT; i++) {
        if (STORAGE_FUNC_FAIL(AllocAndAddDataFile(pdbId, &fileId, EXTENT_SIZE_ARRAY[i], false))) {
            ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("Alloc and add datafile for system tablespace(%u) failed",
                tablespaceId));
            return DSTORE_FAIL;
        }
    }
#endif

    return DSTORE_SUCC;
}

RetStatus TableSpace::AlterMaxSize(uint64 maxSize)
{
    if (STORAGE_FUNC_FAIL(m_controlFile->AlterTbsMaxSize(m_tablespaceId, maxSize, true))) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to alter tablespace %hu maxsize, pdbId %u.", m_tablespaceId, m_pdbId));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

TbsAllocExtentContext::TbsAllocExtentContext()
    : m_pdbId(INVALID_PDB_ID), m_hwm(0), m_lastAllocedIndex(INVALID_LAST_ALLOCED_INDEX), m_extentSize(INVALID_EXT_SIZE)
{
}

TbsAllocExtentContext::~TbsAllocExtentContext()
{
    m_extentSize = INVALID_EXT_SIZE;
    m_hwm = 0;
    m_lastAllocedIndex = INVALID_LAST_ALLOCED_INDEX;
}

void TbsAllocExtentContext::AddFileId(FileId fileid)
{
    StorageAssert(m_hwm < MAX_SPACE_FILE_COUNT);
    m_files[m_hwm] = fileid;
    m_hwm++;
}

RetStatus TbsAllocExtentContext::AllocExtentFromTo(PageId *newExtentPageId, bool *isReUseFlag, uint16 fromIndex,
                                                   uint16 toIndex, int16 *allocedIndex, bool *continueTryAlloc)
{
    TbsDataFile *dataFile = nullptr;
    ErrorCode errCode;
    bool continueAlloc = true;
    TablespaceMgr *tablespaceMgr = g_storageInstance->GetTablespaceMgrByPdbId(m_pdbId);
    if (STORAGE_VAR_NULL(tablespaceMgr)) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
               ErrMsg("Failed to get tablespaceMgr when TbsAllocExtentContext, pdbId %u.", m_pdbId));
        return DSTORE_FAIL;
    }
    TbsDataFile **datafiles = tablespaceMgr->GetDataFiles();
    
    for (int i = fromIndex; i < toIndex; i++) {
        dataFile = datafiles[m_files[i]];
        if (STORAGE_VAR_NULL(dataFile)) {
            ErrLog(DSTORE_PANIC, MODULE_TABLESPACE, ErrMsg("There is no datafile to use, pdbId %u.", m_pdbId));
        }
        RetStatus ret = dataFile->AllocExtent(newExtentPageId, isReUseFlag);
        if (ret == DSTORE_SUCC) {
            *allocedIndex = i;
            return DSTORE_SUCC;
        }
        errCode = StorageGetErrorCode();
        if (errCode != TBS_ERROR_FILE_SIZE_EXCEED_MAX_LIMIT) {
            continueAlloc = false;
            ErrLog(
                DSTORE_WARNING, MODULE_TABLESPACE,
                ErrMsg("AllocExtent failed, File size is still under the limit TBS_ERROR_FILE_SIZE_EXCEED_MAX_LIMIT."));
            break;
        }
    }
    if (continueTryAlloc != nullptr) {
        *continueTryAlloc = continueAlloc;
    }
    return DSTORE_FAIL;
}

RetStatus TbsAllocExtentContext::AllocExtent(PageId *newExtentPageId, bool *isReUseFlag, bool *continueTryAlloc)
{
    int16 allocedIndex = -1;
    bool continueAlloc = true;
    RetStatus ret = DSTORE_FAIL;
    do {
        ret = AllocExtentFromTo(newExtentPageId,
            isReUseFlag,
            (m_lastAllocedIndex == -1) ? 0 : m_lastAllocedIndex,
            m_hwm,
            &allocedIndex,
            &continueAlloc);
        if (ret == DSTORE_SUCC) {
            m_lastAllocedIndex = allocedIndex;
            break;
        }
        if (!continueAlloc) {
            break;
        }
        allocedIndex = -1;
        ret = AllocExtentFromTo(newExtentPageId,
            isReUseFlag,
            0,
            (m_lastAllocedIndex == -1) ? 0 : m_lastAllocedIndex,
            &allocedIndex,
            &continueAlloc);
        if (ret == DSTORE_SUCC) {
            m_lastAllocedIndex = allocedIndex;
        }
    } while (0);

    if (continueTryAlloc != nullptr) {
        *continueTryAlloc = continueAlloc;
    }

    return ret;
}

ExtentSize TbsAllocExtentContext::GetExtentSize()
{
    return m_extentSize;
}

void TbsAllocExtentContext::SetExtentSize(ExtentSize extentSize)
{
    m_extentSize = extentSize;
}

uint16 TbsAllocExtentContext::GetHwm()
{
    return m_hwm;
}

}
