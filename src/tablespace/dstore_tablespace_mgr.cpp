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
 * dstore_tablespace_mgr.cpp
 *
 *
 *
 * IDENTIFICATION
 *        storage/src/page/dstore_tablespace_mgr.cpp
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
#include "tablespace/dstore_tablespace.h"
#include "lock/dstore_lock_interface.h"
#include "tablespace/dstore_tablespace_internal.h"

namespace DSTORE {
TablespaceMgr::TablespaceMgr(PdbId pdbId, ControlFile *controlFile, VFSAdapter *vfs)
    : m_pdbId(pdbId), m_controlFile(controlFile), m_vfs(vfs), m_tmpTbsHashTable(nullptr)
{
    int rc = memset_s(m_datafiles, MAX_DATAFILE_ITEM_CNT * sizeof(TbsDataFile *), 0,
                      MAX_DATAFILE_ITEM_CNT * sizeof(TbsDataFile *));
    storage_securec_check(rc, "\0", "\0");

    rc = memset_s(m_tablespaces, MAX_TABLESPACE_ITEM_CNT * sizeof(TableSpace *), 0,
                  MAX_TABLESPACE_ITEM_CNT * sizeof(TableSpace *));
    storage_securec_check(rc, "\0", "\0");
    for (uint32 tablespaceId = 0; tablespaceId < MAX_TABLESPACE_ITEM_CNT; tablespaceId++) {
        LWLockInitialize(&m_tablespaceLWLocks[tablespaceId], LWLOCK_GROUP_TABLESPACE);
    }

    for (uint32 fileId = 0; fileId < MAX_DATAFILE_ITEM_CNT; fileId++) {
        LWLockInitialize(&m_datafileLWLocks[fileId], LWLOCK_GROUP_DATAFILE);
    }
}

TablespaceMgr::~TablespaceMgr()
{
    Destroy();
    m_pdbId = INVALID_PDB_ID;
    m_controlFile = nullptr;
    m_vfs = nullptr;
}

RetStatus TablespaceMgr::InitTempTbsBitmapPageTable()
{
    m_tmpTbsHashTable = DstoreNew(g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LONGLIVE))
        TbsTempBitmapPageHashTable();
    if (STORAGE_VAR_NULL(m_tmpTbsHashTable)) {
        return DSTORE_FAIL;
    }
    return m_tmpTbsHashTable->Initialize();
}

TbsTempBitmapPageHashTable *TablespaceMgr::GetTempTbsBitmapPageTable()
{
    return m_tmpTbsHashTable;
}

void TablespaceMgr::ReleaseDatafileLock(TbsDataFile* datafile)
{
    if (STORAGE_VAR_NULL(datafile)) {
        return;
    }
    FileId fileId = datafile->GetFileId();
    LWLockRelease(&m_datafileLWLocks[fileId]);
}

TbsDataFile* TablespaceMgr::NewDatafile(ControlDataFilePageItemData fileItem)
{
    TbsDataFile *datafile = nullptr;
    uint64 maxBlockCount = static_cast<uint64>(fileItem.fileMaxSize / BLCKSZ);
    bool tempDatafile = (fileItem.fileId >= TMP_TBS_START_FILE_ID && fileItem.fileId <= TMP_TBS_MAX_FILE_ID);
    if (!tempDatafile) {
        datafile = DstoreNew(g_storageInstance->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LONGLIVE))
            TbsDataFile(m_pdbId, m_vfs, fileItem.fileId, maxBlockCount, fileItem.extentSize, false);
    } else {
        datafile = DstoreNew(g_storageInstance->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LONGLIVE))
            TempTbsDataFile(m_pdbId, m_vfs, fileItem.fileId, maxBlockCount, m_tmpTbsHashTable);
    }
    if (STORAGE_VAR_NULL(datafile)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
                ErrMsg("Alloc new datafile failed, pdbId %u, fileId %hu, max block count %lu.",
                    m_pdbId, fileItem.fileId, maxBlockCount));
        return nullptr;
    }

    datafile->SetDataFilePageItemData(fileItem);

    if (STORAGE_FUNC_FAIL(datafile->InitBitmapMgr())) {
        DstorePfreeExt(datafile);
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to InitBitmapMgr, pdbId %u, fileId %hu.", m_pdbId, fileItem.fileId));
        return nullptr;
    }

    return datafile;
}

TbsDataFile* TablespaceMgr::AcquireDatafile(FileId fileId, LWLockMode mode)
{
    while (true) {
        DstoreLWLockAcquireByMode(&m_datafileLWLocks[fileId], mode);
        if (likely(m_datafiles[fileId] && m_datafileValid[fileId])) {
            return m_datafiles[fileId];
        }
        LWLockRelease(&m_datafileLWLocks[fileId]);

        ControlDataFilePageItemData datafileItem;
        if (STORAGE_FUNC_FAIL(m_controlFile->GetDataFilePageItemData(fileId, &datafileItem))) {
            LWLockRelease(&m_datafileLWLocks[fileId]);
            ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
                   ErrMsg("Get datafile page item failed, pdbId %u, fileId %hu.", m_pdbId, fileId));
            return nullptr;
        }

        DstoreLWLockAcquireByMode(&m_datafileLWLocks[fileId], LW_EXCLUSIVE);
        if (likely(m_datafiles[fileId] && m_datafileValid[fileId])) {
            LWLockRelease(&m_datafileLWLocks[fileId]);
            continue;
        }

        TbsDataFile *oldDatafile = m_datafiles[fileId];
        m_datafiles[fileId] = nullptr;
        if (oldDatafile) {
            oldDatafile->Destroy();
            DstorePfreeExt(oldDatafile);
        }

        TbsDataFile *datafile = NewDatafile(datafileItem);
        if (STORAGE_VAR_NULL(datafile)) {
            LWLockRelease(&m_datafileLWLocks[fileId]);
            ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
                   ErrMsg("New datafile failed, pdbId %u, fileId %hu.", m_pdbId, fileId));
            return nullptr;
        }

        m_datafiles[fileId] = datafile;
        m_datafileValid[fileId] = true;
        ErrLog(DSTORE_LOG, MODULE_TABLESPACE,
               ErrMsg("Acquire datafile success, pdbId %u, fileId %hu.", m_pdbId, fileId));
        LWLockRelease(&m_datafileLWLocks[fileId]);
    }
}

RetStatus TablespaceMgr::Load(TableSpace *tablespace)
{
    TablespaceId tablespaceId = tablespace->GetTablespaceId();
    ControlTablespacePageItemData tbsItem;
    if (STORAGE_FUNC_FAIL(m_controlFile->GetTbsPageItemData(tablespaceId, &tbsItem))) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Get tablespace page item failed, pdbId %u, tablespaceId %hu.", m_pdbId, tablespaceId));
        return DSTORE_FAIL;
    }

    tablespace->SetTbsPageItemData(tbsItem);

    ControlTablespacePageItemData tbsItemData = tablespace->GetTbsPageItemData();
    for (uint16 i = 0; i < tbsItemData.hwm; i++) {
        FileId fileId = tbsItemData.fileIds[i];
        if (fileId == INVALID_DATA_FILE_ID) {
            continue;
        }
        TbsDataFile *datafile = AcquireDatafile(fileId, LW_SHARED);
        if (STORAGE_VAR_NULL(datafile)) {
            ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
                ErrMsg("Load datafile failed, pdbId %u, tablespaceId %hu, fileId %hu.", m_pdbId, tablespaceId, fileId));
            return DSTORE_FAIL;
        }

        ControlDataFilePageItemData dataFilePageItem = datafile->GetDataFilePageItemData();
        if (unlikely(tablespaceId != dataFilePageItem.tablespaceId)) {
            ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
                   ErrMsg("TablespaceId does not match, pdbId %u, tablespaceId %hu, "
                          "fileId %hu, datafile item tablespaceId is %hu.",
                          m_pdbId, tablespaceId, fileId, dataFilePageItem.tablespaceId));
            ReleaseDatafileLock(datafile);
            return DSTORE_FAIL;
        }

        RetStatus ret = m_vfs->OpenFile(fileId, datafile->GetFileName(),
            (USE_VFS_LOCAL_AIO ? DSTORE_FILE_ADIO_FLAG : DSTORE_FILE_OPEN_FLAG));
        if (ret != DSTORE_SUCC) {
            if (g_storageInstance->IsInBackupRestore(m_pdbId)) {
                ErrLog(DSTORE_LOG, MODULE_TABLESPACE,
                       ErrMsg("Open file failed, pdbId %u, fileId %hu, fileName %s.", m_pdbId, fileId,
                              datafile->GetFileName()));
            } else {
                ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
                       ErrMsg("Open file failed, pdbId %u, fileId %hu, fileName %s.", m_pdbId, fileId,
                              datafile->GetFileName()));
                ReleaseDatafileLock(datafile);
                StorageAssert(0);
                return DSTORE_FAIL;
            }
        }
        if (!tablespace->IsTempTbs() || fileId == g_storageInstance->GetGuc()->selfNodeId) {
            tablespace->AddFileId(fileId, dataFilePageItem.extentSize);
        }
        tablespace->IncreFileCount();
        ErrLog(DSTORE_LOG, MODULE_TABLESPACE,
               ErrMsg("Load the fileId %hu to the tablespace %hu success, pdbId %u, current file count %hu", fileId,
                      tablespaceId, m_pdbId, tablespace->GetFileCount()));
        ReleaseDatafileLock(datafile);
    }
 
    tablespace->SetTablespaceStatus(TbsStatus::TBS_OPENED);
    ErrLog(DSTORE_LOG, MODULE_TABLESPACE,
           ErrMsg("Load the tablespace %hu success, pdbId %u, m_fileCount %hu.", tablespaceId, m_pdbId,
                  tablespace->GetFileCount()));
    return DSTORE_SUCC;
}

RetStatus TablespaceMgr::LockTablespace(TablespaceId tablespaceId, LockMode lockMode, bool dontWait)
{
    LockInterface::TablespaceLockContext context;
    context.pdbId = m_pdbId;
    context.tablespaceId = tablespaceId;
    context.dontWait = dontWait;
    context.mode = lockMode;

    RetStatus ret = LockInterface::LockTablespace(&context);
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Tablespace alloclock failed, pdbId %u, tablespaceId %hu, lockMode %u.", m_pdbId, tablespaceId,
                      lockMode));
    }
    return ret;
}

void TablespaceMgr::UnlockTablespace(TablespaceId tablespaceId, LockMode lockMode)
{
    LockInterface::TablespaceLockContext context;
    context.pdbId = m_pdbId;
    context.tablespaceId = tablespaceId;
    context.dontWait = false;
    context.mode = lockMode;

    LockInterface::UnlockTablespace(&context);
}
 
TableSpace *TablespaceMgr::OpenTablespace(TablespaceId tablespaceId, LockMode lockMode, bool dontWait)
{
    if (unlikely(tablespaceId == INVALID_TABLESPACE_ID || tablespaceId > MAX_TABLESPACE_ID)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("The input parameter is invalid, pdbId %u, tablespaceId %hu.", m_pdbId, tablespaceId));
        storage_set_error(TBS_ERROR_SEGMENT_PARAMETER_INVALID);
        return nullptr;
    }

    if (STORAGE_FUNC_FAIL(LockTablespace(tablespaceId, lockMode, dontWait))) {
        return nullptr;
    }

    if (likely(m_tablespaces[tablespaceId] != nullptr && m_tbsValid[tablespaceId])) {
        ErrLog(DSTORE_DEBUG1, MODULE_TABLESPACE,
               ErrMsg("Open tablespace success, pdbId %u, tablespaceId %hu, lockMode %u.", m_pdbId, tablespaceId,
                      lockMode));
        return m_tablespaces[tablespaceId];
    }

    DstoreLWLockAcquire(&m_tablespaceLWLocks[tablespaceId], LW_EXCLUSIVE);

    if (m_tablespaces[tablespaceId] != nullptr && m_tbsValid[tablespaceId]) {
        ErrLog(DSTORE_DEBUG1, MODULE_TABLESPACE, ErrMsg("Tablespace is already opened, pdbId %u, tablespaceId %hu.",
            m_pdbId, tablespaceId));
        LWLockRelease(&m_tablespaceLWLocks[tablespaceId]);
        return m_tablespaces[tablespaceId];
    }

    DstorePfreeExt(m_tablespaces[tablespaceId]);

    TableSpace *tablespace = DstoreNew(g_storageInstance->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LONGLIVE))
        TableSpace(m_controlFile, tablespaceId);
    if (STORAGE_VAR_NULL(tablespace)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Alloctablespace failed, pdbId %u, tablespaceId %hu.", m_pdbId, tablespaceId));
        LWLockRelease(&m_tablespaceLWLocks[tablespaceId]);
        UnlockTablespace(tablespaceId, lockMode);
        return nullptr;
    }
    if (STORAGE_FUNC_FAIL(Load(tablespace))) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Loadtablespace failed, pdbId %u, tablespaceId %hu.", m_pdbId, tablespaceId));
        DstorePfreeExt(tablespace);
        LWLockRelease(&m_tablespaceLWLocks[tablespaceId]);
        UnlockTablespace(tablespaceId, lockMode);
        return nullptr;
    }

    m_tablespaces[tablespaceId] = tablespace;
    m_tbsValid[tablespaceId] = true;
    LWLockRelease(&m_tablespaceLWLocks[tablespaceId]);
    ErrLog(DSTORE_LOG, MODULE_TABLESPACE,
        ErrMsg("Open tablespace success, pdbId %u, tablespaceId %hu, lockMode %u.", m_pdbId, tablespaceId, lockMode));
    return m_tablespaces[tablespaceId];
}
 
RetStatus TablespaceMgr::SendInvalidTablespace(TablespaceId tablespaceId, FileId fileId)
{
    /* Invalidating the local tablespace cache */
    RetStatus ret = HandleInvalidTablespace(tablespaceId, fileId);
    if (STORAGE_FUNC_FAIL(ret)) {
        return DSTORE_FAIL;
    }
    ErrLog(DSTORE_LOG, MODULE_TABLESPACE,
           ErrMsg("Success to invalidate tablespace cache for the pdb %u, tablespaceId %hu, fileId %hu.", m_pdbId,
                  tablespaceId, fileId));
    return DSTORE_SUCC;
}
 
RetStatus TablespaceMgr::HandleInvalidTablespace(TablespaceId tablespaceId, FileId fileId)
{
    if (tablespaceId > MAX_TABLESPACE_ID) {
        /* Tablespaces that are not in the cache range are not processed. */
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to handle invalidate tablespace message due to tablespaceId is out of range, pdbId %u, "
                      "tablespaceId %hu, fileId %hu.",
                      m_pdbId, tablespaceId, fileId));
        return DSTORE_FAIL;
    }
    DstoreLWLockAcquire(&m_tablespaceLWLocks[tablespaceId], LW_EXCLUSIVE);
    m_tbsValid[tablespaceId] = false;
    LWLockRelease(&m_tablespaceLWLocks[tablespaceId]);

    DstoreLWLockAcquire(&m_datafileLWLocks[fileId], LW_EXCLUSIVE);
    m_datafileValid[fileId] = false;
    LWLockRelease(&m_datafileLWLocks[fileId]);

    if (m_vfs && fileId != INVALID_DATA_FILE_ID && tablespaceId == INVALID_TABLESPACE_ID) {
        if (STORAGE_FUNC_FAIL(m_vfs->Close(fileId))) {
            ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
                   ErrMsg("Failed to handle invalidate tablespace, pdbId %u, tablespaceId %hu, fileId %hu.", m_pdbId,
                          tablespaceId, fileId));
            return DSTORE_FAIL;
        }
    }

    ErrLog(DSTORE_LOG, MODULE_TABLESPACE,
           ErrMsg("Tablespace received invalid message, pdbId %u, tablespaceId %hu, fileId %hu.", m_pdbId,
                  tablespaceId, fileId));
    return DSTORE_SUCC;
}
 
void TablespaceMgr::CloseTablespace(TableSpace *tablespace, LockMode lock_mode)
{
    if (tablespace == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("Tablespace is null when closetablespace, pdbId %u.", m_pdbId));
        return;
    }
 
    LockInterface::TablespaceLockContext context;
    context.pdbId = m_pdbId;
    context.tablespaceId = tablespace->GetTablespaceId();
    context.dontWait = false;
    context.mode = lock_mode;
    LockInterface::UnlockTablespace(&context);
}
 
void TablespaceMgr::Destroy()
{
    if (m_tmpTbsHashTable) {
        m_tmpTbsHashTable->Destroy();
        DstorePfree(m_tmpTbsHashTable);
        m_tmpTbsHashTable = nullptr;
    }

    for (uint16_t tablespaceId = 1; tablespaceId < MAX_TABLESPACE_ITEM_CNT; tablespaceId++) {
        Destroy(tablespaceId);
    }

    for (uint32 fileId = 1; fileId < MAX_DATAFILE_ITEM_CNT; fileId++) {
        if (m_datafiles[fileId] != nullptr) {
            m_datafiles[fileId]->Destroy();
            delete m_datafiles[fileId];
            m_datafiles[fileId] = nullptr;
        }
    }
}
 
void TablespaceMgr::Destroy(TablespaceId tablespaceId)
{
    if (m_tablespaces[tablespaceId] != nullptr) {
        UnLoadDataFiles(tablespaceId);
        delete m_tablespaces[tablespaceId];
        m_tablespaces[tablespaceId] = nullptr;
        ErrLog(DSTORE_LOG, MODULE_TABLESPACE,
               ErrMsg("Tablespace has been destoried, pdbId %u, tablespaceId %hu", m_pdbId, tablespaceId));
    }
}

void TablespaceMgr::UnLoadDataFiles(TablespaceId tablespaceId)
{
    TableSpace *tablespace = m_tablespaces[tablespaceId];
    for (uint16 i = 0; i < MAX_SPACE_FILE_COUNT; i++) {
        uint16 fileid = tablespace->GetTbsPageItemData().fileIds[i];
        if (m_datafiles[fileid] != nullptr) {
            m_datafiles[fileid]->Destroy();
            delete m_datafiles[fileid];
            m_datafiles[fileid] = nullptr;
        }
    }
}

RetStatus TablespaceMgr::GetFileVersion(FileId fileId, uint64 *reuseVersion)
{
    TbsDataFile *datafile = AcquireDatafile(fileId, LW_SHARED);
    if (STORAGE_VAR_NULL(datafile)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Load datafile failed, pdbId %u, fileId %hu.", m_pdbId, fileId));
        return DSTORE_FAIL;
    }

    *reuseVersion = datafile->GetDataFilePageItemData().reuseVersion;
    ReleaseDatafileLock(datafile);
    return DSTORE_SUCC;
}

RetStatus TablespaceMgr::AlterMaxSize(TablespaceId tablespaceId, uint64 maxSize)
{
    TableSpace *tablespace = OpenTablespace(tablespaceId, DSTORE::DSTORE_NO_LOCK);
    if (STORAGE_VAR_NULL(tablespace)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to open tablespace %u, pdbId %u.", tablespaceId, m_pdbId));
        return DSTORE_FAIL;
    }
    RetStatus ret = tablespace->AlterMaxSize(maxSize);
    if (ret == DSTORE_FAIL) {
        CloseTablespace(tablespace, DSTORE::DSTORE_NO_LOCK);
        return DSTORE_FAIL;
    }
    
    CloseTablespace(tablespace, DSTORE::DSTORE_NO_LOCK);
    return DSTORE_SUCC;
}

TbsDataFile **TablespaceMgr::GetDataFiles()
{
    return m_datafiles;
}

PdbId TablespaceMgr::GetPdbId()
{
    return m_pdbId;
}

RetStatus TablespaceMgr::RemoveAllFiles()
{
    char fileName[MAXPGPATH] = {0};
    uint32 fileId;

    for (fileId = 1; fileId <= MAX_VFS_FILE_ID; fileId++) {
        if (STORAGE_FUNC_FAIL(m_vfs->GetFileNameFromFileId(fileId, fileName))) {
            return DSTORE_FAIL;
        }
        if (!m_vfs->FileExists(fileName)) {
            continue;
        }
        if (STORAGE_FUNC_FAIL(m_vfs->RemoveFile(fileId, fileName))) {
            ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
                   ErrMsg("Failed to remove datafile %s, pdbId %u, fileId %u.", fileName, m_pdbId, fileId));
            return DSTORE_FAIL;
        }
        ErrLog(DSTORE_LOG, MODULE_TABLESPACE,
               ErrMsg("Success to remove datafile %s, pdbId %u, fileId %u.", fileName, m_pdbId, fileId));
    }

    ErrLog(DSTORE_LOG, MODULE_TABLESPACE, ErrMsg("Success to remove all tablespace datafiles, pdbId %hu.", m_pdbId));
    return DSTORE_SUCC;
}

RetStatus TablespaceMgr::GetTablespaceSize(const TablespaceId tablespaceId, uint64& size)
{
    int64 currentFileSize = 0;
    size = 0;
    TableSpace *tablespace = OpenTablespace(tablespaceId, DSTORE::DSTORE_NO_LOCK);
    if (STORAGE_VAR_NULL(tablespace)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to open tablespace %hu, pdbId %u.", tablespaceId, m_pdbId));
        return DSTORE_FAIL;
    }
    ControlTablespacePageItemData ctrlTbsItem = tablespace->GetTbsCtrlItem();
    for (int i = 0; i < ctrlTbsItem.hwm; i++) {
        if (ctrlTbsItem.fileIds[i] == INVALID_DATA_FILE_ID) {
            continue;
        }
        currentFileSize = m_vfs->GetSize(ctrlTbsItem.fileIds[i]);
        if (currentFileSize < 0) {
            CloseTablespace(tablespace, DSTORE::DSTORE_NO_LOCK);
            ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
                   ErrMsg("Get file size failed, pdbId %u, tablespaceId %hu, fileId %hu.", m_pdbId, tablespaceId,
                          ctrlTbsItem.fileIds[i]));
            return DSTORE_FAIL;
        }
        size += static_cast<uint64>(currentFileSize);
    }
    CloseTablespace(tablespace, DSTORE::DSTORE_NO_LOCK);
    return DSTORE_SUCC;
}

RetStatus TablespaceMgr::RemoveFileByFileId(FileId fileId, Xid ddlXid)
{
    if (fileId == INVALID_DATA_FILE_ID) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to remove file by invalid fileId %hu, pdbId %u.", fileId, m_pdbId));
        return DSTORE_FAIL;
    }

    char fileName[MAXPGPATH] = {0};
    if (STORAGE_FUNC_FAIL(m_vfs->GetFileNameFromFileId(fileId, fileName))) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to get filename by fileId %hu, pdbId %u.", fileId, m_pdbId));
        return DSTORE_FAIL;
    }

    if (!m_vfs->FileExists(fileName)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("The file(%s) does not exist, fileId %hu, pdbId %u.", fileName, fileId, m_pdbId));
        return DSTORE_FAIL;
    }

    if (STORAGE_FUNC_FAIL(m_vfs->RemoveFile(fileId, fileName))) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
            ErrMsg("Failed to remove file(%s) by fileId %hu, pdbId %u.", fileName, fileId, m_pdbId));
        return DSTORE_FAIL;
    }

    if (STORAGE_FUNC_FAIL(
            m_controlFile->FreeDataFileId(fileId, INVALID_TABLESPACE_ID, MAX_TBS_DATAFILE_SLOT_CNT, ddlXid, true))) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to free datafile fileId %hu, pdbId %u.", fileId, m_pdbId));
        return DSTORE_FAIL;
    }

    ErrLog(DSTORE_LOG, MODULE_TABLESPACE, ErrMsg("Remove file by fileId %hu success, pdbId %u.", fileId, m_pdbId));
    return DSTORE_SUCC;
}
}  // namespace DSTORE
