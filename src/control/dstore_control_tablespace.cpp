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
 * dstore_control_tablespace.cpp
 *
 * IDENTIFICATION
 *        dstore/src/controlfile/dstore_control_tablespace.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "control/dstore_control_tablespace.h"
#include "control/dstore_control_file_lock.h"
#include "transaction/dstore_transaction.h"
#include "framework/dstore_instance.h"
#include "tablespace/dstore_tablespace_wal.h"
#include "tablespace/dstore_tablespace.h"
#include "wal/dstore_wal_write_context.h"

namespace DSTORE {

void ControlTablespacePageItemData::InitItem(TablespaceId tbsId)
{
    this->magic = CF_TABLESPACE_ITEM_MAGIC_NUMBER;
    this->version = 1;
    this->tablespaceId = tbsId;
    this->reuseVersion = 0;
}

void ControlTablespacePageItemData::AlterItemMaxSize(uint64 maxSize)
{
    this->tbsMaxSize = maxSize;
    if (thrd != nullptr && thrd->GetActiveTransaction() != nullptr) {
        this->ddlXid = thrd->GetActiveTransaction()->GetCurrentXid();
    } else {
        this->ddlXid = INVALID_XID;
    }
}

RetStatus ControlTablespacePageItemData::AssociateFile(FileId fileId, uint16 *slotId)
{
    if (unlikely(slotId == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("AssociateFile slotId is nullptr, tablespaceId %hu, fileId %hu.", this->tablespaceId, fileId));
        return DSTORE_FAIL;
    }
    if (this->used == 0) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("The tablespace item is not in use, tablespaceId %hu, fileId %hu, hwm %hu.",
                      this->tablespaceId, fileId, this->hwm));
        return DSTORE_FAIL;
    }

    bool find = false;
    for (uint16 i = 0; i < MAX_SPACE_FILE_COUNT; i++) {
        if (this->fileIds[i] == INVALID_DATA_FILE_ID) {
            this->fileIds[i] = fileId;
            this->hwm = (this->hwm < i + 1) ? (i + 1) : this->hwm;
            find = true;
            *slotId = i;
            ErrLog(DSTORE_LOG, MODULE_TABLESPACE,
                   ErrMsg("Associate fileId %hu with tablespaceId %hu success, hwm %hu, slotId %hu.",
                          fileId, this->tablespaceId, this->hwm, *slotId));
            break;
        }
    }

    if (!find) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("The number of fileIds has reached the maximum value supported by the tablespace, "
                      "tablespaceId %hu, fileId %hu.",
                      this->tablespaceId, fileId));
        return DSTORE_FAIL;
    }

    return DSTORE_SUCC;
}

RetStatus ControlTablespacePageItemData::DisassociateFile(FileId fileId, uint16 *slotId)
{
    if (unlikely(slotId == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("DisassociateFile slotId is nullptr, tablespaceId %hu, fileId %hu.", this->tablespaceId, fileId));
        return DSTORE_FAIL;
    }
    if (this->used == 0) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
            ErrMsg(
                "The tablespace item is not in use, tablespaceId %hu, hwm fileId %hu, fileId %hu, hwm %hu.",
                this->tablespaceId, this->fileIds[this->hwm], fileId, this->hwm));
        return DSTORE_FAIL;
    }

    bool find = false;
    for (uint16 i = 0; i < this->hwm; i++) {
        if (this->fileIds[i] == fileId) {
            this->fileIds[i] = INVALID_DATA_FILE_ID;
            find = true;
            *slotId = i;
            ErrLog(DSTORE_LOG, MODULE_TABLESPACE,
                   ErrMsg("Disassociate fileId %hu with tablespaceId %hu success, hwm %hu, slotId %hu.",
                          fileId, this->tablespaceId, this->hwm, *slotId));
            break;
        }
    }

    if (!find) {
        ErrLog(DSTORE_LOG, MODULE_TABLESPACE,
               ErrMsg("The fileId is not been associated with the tablespace, tablespaceId %hu, fileId %hu.",
                      this->tablespaceId, fileId));
    }

    return DSTORE_SUCC;
}

void ControlTablespacePageItemData::ResetItem(Xid xid)
{
    this->tbsMaxSize = 0;
    this->used = 0;
    this->reuseVersion++;
    this->ddlXid = xid;
    this->hwm = 0;
    errno_t rc = memset_s(this->fileIds, sizeof(this->fileIds), 0, sizeof(this->fileIds));
    storage_securec_check(rc, "\0", "\0");
}

void ControlTablespacePageItemData::SetItem(uint64 maxSize)
{
    this->hwm = 0;
    this->tbsMaxSize = maxSize;
    this->used = 1;
    this->reuseVersion++;
    errno_t rc = memset_s(this->fileIds, sizeof(this->fileIds), 0, sizeof(this->fileIds));
    storage_securec_check(rc, "\0", "\0");
    if (thrd != nullptr && thrd->GetActiveTransaction() != nullptr) {
        this->ddlXid = thrd->GetActiveTransaction()->GetCurrentXid();
    } else {
        this->ddlXid = INVALID_XID;
    }
}

void ControlDataFilePageItemData::InitItem(FileId fId)
{
    this->magic = CF_DATAFILE_ITEM_MAGIC_NUMBER;
    this->version = 1;
    this->fileId = fId;
    this->reuseVersion = 0;
}

void ControlDataFilePageItemData::SetItem(TablespaceId tbsId, ExtentSize extSize, uint64 maxSize)
{
    this->extentSize = extSize;
    this->used = 1;
    this->reuseVersion++;
    if (thrd != nullptr && thrd->GetActiveTransaction() != nullptr) {
        this->ddlXid = thrd->GetActiveTransaction()->GetCurrentXid();
    } else {
        this->ddlXid = INVALID_XID;
    }
    this->fileMaxSize = maxSize;
    this->tablespaceId = tbsId;
}

void ControlDataFilePageItemData::ResetItem(Xid xid)
{
    this->extentSize = INVALID_EXT_SIZE;
    this->used = 0;
    this->reuseVersion++;
    this->ddlXid = xid;
    this->fileMaxSize = 0;
    this->tablespaceId = INVALID_TABLESPACE_ID;
}

ControlDataFilePageItemData *ControlTablespace::GetDataFilePageItemPtrNoCheck(FileId fileId, BlockNumber *blkno)
{
    OffsetNumber offset;
    BlockNumber tmpBlkNo;
    if (unlikely(blkno == nullptr)) {
        return nullptr;
    }

    GetDataFilePageItemCtid(fileId, &tmpBlkNo, &offset);
    char *page = (char *)GetPage(tmpBlkNo);
    if (page == nullptr) {
        return nullptr;
    }

    ControlDataFilePageItemData *item = (ControlDataFilePageItemData *)(page + sizeof(ControlPageHeader) +
                                                                        offset * sizeof(ControlDataFilePageItemData));

    *blkno = tmpBlkNo;

    return item;
}

ControlDataFilePageItemData *ControlTablespace::GetDataFilePageItemPtrInternal(FileId fileId, BlockNumber *blkno)
{
    BlockNumber tmpBlkNo;
    ControlDataFilePageItemData *datafileItem = GetDataFilePageItemPtrNoCheck(fileId, &tmpBlkNo);
    if (unlikely(datafileItem == NULL)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to get datafile item, pdbId %u, fileId %hu.", m_pdbId, fileId));
        return NULL;
    }

    if (unlikely(datafileItem->magic != CF_DATAFILE_ITEM_MAGIC_NUMBER)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Magic check failed for datafile item, pdbId %u, fileId %hu.", m_pdbId, datafileItem->fileId));
        return NULL;
    }

    if (unlikely(datafileItem->fileId != fileId)) {
        ErrLog(
            DSTORE_ERROR, MODULE_TABLESPACE,
            ErrMsg("FileId check failed for datafile item, pdbId %u, fileId in datafile item %hu, target fileId %hu.",
                   m_pdbId, datafileItem->fileId, fileId));
        return NULL;
    }

    if (blkno != nullptr) {
        *blkno = tmpBlkNo;
    }

    return datafileItem;
}

#ifdef UT
ControlDataFilePageItemData *ControlTablespace::GetDataFilePageItemPtr(FileId fileId, BlockNumber *blkno)
{
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_SHARE))) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("Acquire control file lock fail, fileId %hu.", fileId));
        return nullptr;
    }
    ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_SHARE};

    if (unlikely(LoadGroup() == DSTORE_FAIL)) {
        return nullptr;
    }
    return GetDataFilePageItemPtrInternal(fileId, blkno);
}
#endif

ControlTablespacePageItemData *ControlTablespace::GetTbsPageItemPtrNoCheck(TablespaceId tablespaceId,
                                                                           BlockNumber *blkno)
{
    if (unlikely(blkno == nullptr)) {
        return nullptr;
    }
    OffsetNumber offset;
    BlockNumber tmpBlkNo;

    GetTbsPageItemCtid(tablespaceId, &tmpBlkNo, &offset);
    char *page = (char *)GetPage(tmpBlkNo);
    if (unlikely(page == nullptr)) {
        return nullptr;
    }
    ControlTablespacePageItemData *item =
        (ControlTablespacePageItemData *)(page + sizeof(ControlPageHeader) +
                                          offset * sizeof(ControlTablespacePageItemData));

    *blkno = tmpBlkNo;

    return item;
}

ControlTablespacePageItemData *ControlTablespace::GetTbsPageItemPtrInternal(TablespaceId tablespaceId,
                                                                            BlockNumber *blkno)
{
    BlockNumber tmpBlkNo;
    ControlTablespacePageItemData *tbsItem = GetTbsPageItemPtrNoCheck(tablespaceId, &tmpBlkNo);
    if (unlikely(tbsItem == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to get tablespace item, pdbId %u, tablespaceId %hu.", m_pdbId, tablespaceId));
        return nullptr;
    }

    if (tbsItem->magic != CF_TABLESPACE_ITEM_MAGIC_NUMBER) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Magic check failed for tablespace item, pdbId %u, tablespaceId %hu.", m_pdbId, tablespaceId));
        return nullptr;
    }

    if (tbsItem->tablespaceId != tablespaceId) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
            ErrMsg(
                "TablespaceId check failed, pdbId %u, expected tablespaceId %hu, tablespaceId in tablespace item %hu.",
                m_pdbId, tablespaceId, tbsItem->tablespaceId));
        return nullptr;
    }

    if (blkno != nullptr) {
        *blkno = tmpBlkNo;
    }

    return tbsItem;
}
#ifdef UT
ControlTablespacePageItemData *ControlTablespace::GetTbsPageItemPtr(TablespaceId tablespaceId, BlockNumber *blkno)
{
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_SHARE))) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Acquire control file lock fail, pdbId %u, tablespaceId %hu.", m_pdbId, tablespaceId));
        return nullptr;
    }
    ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_SHARE};

    if (unlikely(LoadGroup() == DSTORE_FAIL)) {
        return nullptr;
    }
    return GetTbsPageItemPtrInternal(tablespaceId, blkno);
}
#endif

RetStatus ControlTablespace::InitTbsItem(TablespaceId tablespaceId)
{
    BlockNumber blkno;
    ControlTablespacePageItemData *tbsItem = GetTbsPageItemPtrNoCheck(tablespaceId, &blkno);
    if (unlikely(tbsItem == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Get tablespace item fail, pdbId %u, tablespaceId %hu.", m_pdbId, tablespaceId));
        return DSTORE_FAIL;
    }
    tbsItem->InitItem(tablespaceId);
    MarkPageDirty(blkno);
    return DSTORE_SUCC;
}

RetStatus ControlTablespace::InitDatafileItem(FileId fileId)
{
    BlockNumber blkno;
    ControlDataFilePageItemData *datafileItem = GetDataFilePageItemPtrNoCheck(fileId, &blkno);
    if (unlikely(datafileItem == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Get datafile item fail, pdbId %u, fileId %u.", m_pdbId, fileId));
        return DSTORE_FAIL;
    }
    datafileItem->InitItem(fileId);
    MarkPageDirty(blkno);
    return DSTORE_SUCC;
}

/* the ControlFile struct is MetaPage + TbsPage + DatafilePage + otherPage */
void ControlTablespace::GetTbsPageItemCtid(TablespaceId tablespaceId, BlockNumber *blkno, OffsetNumber *offset)
{
    if (unlikely(blkno == nullptr || offset == nullptr)) {
        return;
    }
    *offset = (tablespaceId % MAX_TABLESPACE_ITEM_CNT_PER_PAGE);
    *blkno = (tablespaceId / MAX_TABLESPACE_ITEM_CNT_PER_PAGE) + DEFAULT_TABLESPACE_PAGE;
}

void ControlTablespace::GetDataFilePageItemCtid(FileId fileId, BlockNumber *blkno, OffsetNumber *offset)
{
    if (unlikely(blkno == nullptr || offset == nullptr)) {
        return;
    }
    *offset = (fileId % MAX_DATAFILE_ITEM_CNT_PER_PAGE);
    *blkno = (fileId / MAX_DATAFILE_ITEM_CNT_PER_PAGE) + DEFAULT_DATAFILE_PAGE;
}

FileId ControlTablespace::GetOneAvailableFileId(bool isTmpTbs)
{
    FileId fileId = INVALID_DATA_FILE_ID;
    if (isTmpTbs) {
        fileId = g_storageInstance->GetGuc()->selfNodeId;
        if (unlikely(fileId < TMP_TBS_START_FILE_ID || fileId > TMP_TBS_MAX_FILE_ID)) {
            ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
                   ErrMsg("Invalid fileId %hu for temp file, pdbId %u.", fileId, m_pdbId));
            return INVALID_DATA_FILE_ID;
        }
        return fileId;
    }

    BlockNumber blkno;
    uint64 minReuseVer = ~0ULL;
    uint itemNo = 0;
    StoragePdb *pdb = g_storageInstance->GetPdb(m_pdbId);
    if (STORAGE_VAR_NULL(pdb)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to get pdb when GetOneAvailableFileId, pdbId %u.", m_pdbId));
        return INVALID_DATA_FILE_ID;
    }
    VFSAdapter *vfs = pdb->GetVFS();
    if (STORAGE_VAR_NULL(vfs)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to get vfs when GetOneAvailableFileId, pdbId %u.", m_pdbId));
        return INVALID_DATA_FILE_ID;
    }

    for (itemNo = START_FILE_ID; itemNo <= MAX_DATA_FILE_ID; itemNo++) {
        ControlDataFilePageItemData *datafileItem = GetDataFilePageItemPtrInternal(itemNo, &blkno);
        if (datafileItem == nullptr) {
            ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
                   ErrMsg("Get datafile item pointer failed, pdbId %u, fileId %u.", m_pdbId, itemNo));
            return INVALID_DATA_FILE_ID;
        }

        if (datafileItem->used != 0) {
            continue;
        }

        /* Check whether the file exists only when the fileId is likely to be allocated. */
        if (datafileItem->reuseVersion == 0 || datafileItem->reuseVersion < minReuseVer) {
            char fileName[MAXPGPATH] = {0};
            if (STORAGE_FUNC_FAIL(vfs->GetFileNameFromFileId(itemNo, fileName))) {
                ErrLog(DSTORE_WARNING, MODULE_TABLESPACE,
                    ErrMsg("Failed to get fileName when GetOneAvailableFileId, pdbId %u, fileId %hu.",
                           m_pdbId, itemNo));
                continue;
            }

            if (vfs->FileExists(fileName)) {
                ErrLog(DSTORE_WARNING, MODULE_TABLESPACE,
                    ErrMsg("Failed to alloc fileId %hu due to filename %s already exists, pdbId %u.",
                           itemNo, fileName, m_pdbId));
                continue;
            }
        }

        if (datafileItem->reuseVersion == 0) {
            fileId = itemNo;
            break;
        }

        if (datafileItem->reuseVersion < minReuseVer) {
            minReuseVer = datafileItem->reuseVersion;
            fileId = itemNo;
        }
    }

    if (unlikely(fileId == INVALID_DATA_FILE_ID)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
            ErrMsg(
                "Failed to find a valid fileId, the number of datafiles has reached the maximum, pdbId %u, itemNo %u.",
                m_pdbId, itemNo));
    }

    return fileId;
}

RetStatus ControlTablespace::AllocAndCreateDataFile(FileId *fileId, TablespaceId tablespaceId,
    ExtentSize extentSize, bool needWal)
{
    if (unlikely(fileId == nullptr)) {
        return DSTORE_FAIL;
    }
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_EXCLUSIVE))) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Lock control file failed, pdbId %u, tablespaceId %hu.", m_pdbId, tablespaceId));
        return DSTORE_FAIL;
    }
    ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_EXCLUSIVE};

    if (unlikely(LoadGroup() == DSTORE_FAIL)) {
        return DSTORE_FAIL;
    }

    /* step1: alloc datafile Id */
    bool isTmpTbs = ((tablespaceId == static_cast<TablespaceId>(TBS_ID::TEMP_TABLE_SPACE_ID)) ? true : false);
    FileId fileIdTmp = GetOneAvailableFileId(isTmpTbs);
    if (unlikely(fileIdTmp == INVALID_DATA_FILE_ID)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to get a valid fileId, pdbId %u, tablespaceId %hu.", m_pdbId, tablespaceId));
        return DSTORE_FAIL;
    }

    TablespaceMgr *tablespaceMgr = g_storageInstance->GetTablespaceMgrByPdbId(m_pdbId);
    if (STORAGE_VAR_NULL(tablespaceMgr)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to get tablespaceMgr when allocating and creating datafile, pdbId %u.", m_pdbId));
        return DSTORE_FAIL;
    }
    RetStatus ret = tablespaceMgr->SendInvalidTablespace(tablespaceId, fileIdTmp);
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to send invalid tablespace message when allocating datafileId, pdbId %u, tablespaceId "
                      "%hu, fileId %hu.",
                      m_pdbId, tablespaceId, fileIdTmp));
        return DSTORE_FAIL;
    }

    BlockNumber blkno;
    ControlDataFilePageItemData *datafileItem = GetDataFilePageItemPtrInternal(fileIdTmp, &blkno);
    if (unlikely(datafileItem == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to get datafile item, pdbId %u, tablespaceId %hu, fileId %hu, blkno %u.", m_pdbId,
                      tablespaceId, fileIdTmp, blkno));
        return DSTORE_FAIL;
    }
    if (datafileItem->used == DSTORE_DATAFILE_IS_USED) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
            ErrMsg("The fileId %hu is already in use, pdbId %u, tablespaceId %hu.", fileIdTmp, m_pdbId, tablespaceId));
        return DSTORE_FAIL;
    }

    /* step3: update controlfile */
    uint64 preReuseVersion = datafileItem->reuseVersion;
    datafileItem->SetItem(tablespaceId, extentSize, MAX_FILE_SIZE);
    MarkPageDirty(blkno);

    /* step4: write WAL */
    if (needWal) {
        AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
        walWriterContext->BeginAtomicWal(thrd->GetCurrentXid());
        WalRecordTbsCreateDataFile walData;
        walData.SetHeader(WAL_TBS_CREATE_DATA_FILE, sizeof(WalRecordTbsCreateDataFile));
        walData.SetData(tablespaceId, fileIdTmp, MAX_FILE_SIZE, extentSize,
            preReuseVersion, thrd->GetCurrentXid());
        walWriterContext->PutNewWalRecord(&walData);
        WalGroupLsnInfo walGroupPtr = walWriterContext->EndAtomicWal();
        walWriterContext->WaitTargetPlsnPersist(walGroupPtr);
    }
 
    /* step5: create and init datafile */
    if (STORAGE_FUNC_FAIL(tablespaceMgr->CreateDatafile(*datafileItem))) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Create and init datafile failed, pdbId %u, tablespaceId %hu, fileId %hu, blkno %u.", m_pdbId,
                      tablespaceId, fileIdTmp, blkno));
        return DSTORE_FAIL;
    }
 
    /* step6: write controlfile */
    if (STORAGE_FUNC_FAIL(PostGroup())) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("PostGroup failed, pdbId %u, tablespaceId %hu.", m_pdbId, tablespaceId));
        return DSTORE_FAIL;
    }

    *fileId = fileIdTmp;
    ErrLog(DSTORE_LOG, MODULE_TABLESPACE,
           ErrMsg("Alloc and create datafile success, pdbId %u, tablespaceId %hu, fileId %hu, blkno %u.", m_pdbId,
                  tablespaceId, fileIdTmp, blkno));
    return DSTORE_SUCC;
}

RetStatus ControlTablespace::FreeDataFileId(FileId fileId, TablespaceId tablespaceId, uint16 slotId, Xid ddlXid,
                                            bool needWal)
{
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_EXCLUSIVE))) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Acquire control file lock fail, pdbId %u, fileId %hu.", m_pdbId, fileId));
        return DSTORE_FAIL;
    }
    ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_EXCLUSIVE};
    
    TablespaceMgr *tablespaceMgr = g_storageInstance->GetTablespaceMgrByPdbId(m_pdbId);
    if (STORAGE_VAR_NULL(tablespaceMgr)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to get tablespaceMgr when releasing datafileId, pdbId %u.", m_pdbId));
        return DSTORE_FAIL;
    }
    RetStatus ret = tablespaceMgr->SendInvalidTablespace(tablespaceId, fileId);
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to send invalid tablespace message when releasing datafileId, pdbId %u, fileId %hu.",
                      m_pdbId, fileId));
        return DSTORE_FAIL;
    }

    if (unlikely(LoadGroup() == DSTORE_FAIL)) {
        return DSTORE_FAIL;
    }

    BlockNumber blkno;
    ControlDataFilePageItemData *datafileItem = GetDataFilePageItemPtrInternal(fileId, &blkno);
    if (STORAGE_VAR_NULL(datafileItem)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Get datafile item pointer failed, pdbId %u, tablespaceId %hu, fileId %hu, blkno %u.", m_pdbId,
                      tablespaceId, fileId, blkno));
        return DSTORE_FAIL;
    }
    uint64 preReuseVersion = datafileItem->reuseVersion;
    datafileItem->ResetItem(ddlXid);
    MarkPageDirty(blkno);

    if (needWal) {
        uint16 hwm = 0;
        if (tablespaceId != INVALID_TABLESPACE_ID) {
            BlockNumber tbsBlkno;
            ControlTablespacePageItemData *tbsItem = GetTbsPageItemPtrInternal(tablespaceId, &tbsBlkno);
            if (tbsItem == nullptr) {
                ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
                       ErrMsg("Get tablespace item failed, pdbId %u, tablespaceId %hu, fileId %hu.", m_pdbId,
                              tablespaceId, fileId));
                return DSTORE_FAIL;
            }
            hwm = tbsItem->hwm;
        }
        AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
        walWriterContext->BeginAtomicWal(ddlXid);
        WalRecordTbsDropDataFile walData;
        walData.SetHeader(WAL_TBS_DROP_DATA_FILE, sizeof(WalRecordTbsDropDataFile));
        walData.SetData(tablespaceId, hwm, fileId, preReuseVersion, ddlXid, slotId);
        walWriterContext->PutNewWalRecord(&walData);
        WalGroupLsnInfo walGroupPtr = walWriterContext->EndAtomicWal();
        walWriterContext->WaitTargetPlsnPersist(walGroupPtr);
    }

    if (STORAGE_FUNC_FAIL(PostGroup())) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("PostGroup failed, pdbId %u, tablespaceId %hu.", m_pdbId, tablespaceId));
        return DSTORE_FAIL;
    }

    ErrLog(DSTORE_LOG, MODULE_TABLESPACE,
           ErrMsg("Free datafileId %hu success, pdbId %u, tablespaceId %hu, blkno %u.", fileId, m_pdbId, tablespaceId,
                  blkno));
    return DSTORE_SUCC;
}

RetStatus ControlTablespace::InvalidateFileId(FileId fileId)
{
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_EXCLUSIVE))) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Acquire control file lock fail, pdbId %u, fileId %hu.", m_pdbId, fileId));
        return DSTORE_FAIL;
    }
    ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_EXCLUSIVE};
    
    TablespaceMgr *tablespaceMgr = g_storageInstance->GetTablespaceMgrByPdbId(m_pdbId);
    if (STORAGE_VAR_NULL(tablespaceMgr)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to get tablespaceMgr when invalidate fileId %hu, pdbId %u.", fileId, m_pdbId));
        return DSTORE_FAIL;
    }
    RetStatus ret = tablespaceMgr->SendInvalidTablespace(INVALID_TABLESPACE_ID, fileId);
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to send invalid fileId %hu message, pdbId %u.", fileId, m_pdbId));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus ControlTablespace::InitTbsId(TablespaceId tablespaceId, uint64 tbsMaxSize)
{
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_EXCLUSIVE))) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Acquire control file lock fail, pdbId %u, tablespaceId %hu.", m_pdbId, tablespaceId));
        return DSTORE_FAIL;
    }
    ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_EXCLUSIVE};

    if (unlikely(LoadGroup() == DSTORE_FAIL)) {
        return DSTORE_FAIL;
    }

    BlockNumber blkno;
    ControlTablespacePageItemData *tbsItem = GetTbsPageItemPtrInternal(tablespaceId, &blkno);
    if (tbsItem == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Get tablespace page item pointer failed, pdbId %u, tablespaceId %hu.", m_pdbId, tablespaceId));
        return DSTORE_FAIL;
    }

    tbsItem->SetItem(tbsMaxSize);
    MarkPageDirty(blkno);

    if (STORAGE_FUNC_FAIL(PostGroup())) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("PostGroup failed, pdbId %u, tablespaceId %hu.", m_pdbId, tablespaceId));
        return DSTORE_FAIL;
    }

    return DSTORE_SUCC;
}

RetStatus ControlTablespace::AllocTbsId(TablespaceId *tablespaceId, uint64 tbsMaxSize, bool needWal)
{
    if (unlikely(tablespaceId == nullptr)) {
        return DSTORE_FAIL;
    }
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_EXCLUSIVE))) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("Acquire control file lock fail, pdbId %u.", m_pdbId));
        return DSTORE_FAIL;
    }
    ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_EXCLUSIVE};

    if (unlikely(LoadGroup() == DSTORE_FAIL)) {
        return DSTORE_FAIL;
    }

    TablespaceId tbsId = INVALID_TABLESPACE_ID;
    BlockNumber blkno;
    ControlTablespacePageItemData *tbsItem = NULL;
    uint64 minReuseVer = ~0ULL;
    uint itemNo = 0;
    for (itemNo = CUSTOMISED_TABLESPACE_START_ID; itemNo <= MAX_TABLESPACE_ID; itemNo++) {
        tbsItem = GetTbsPageItemPtrInternal(itemNo, &blkno);
        if (tbsItem == nullptr) {
            ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
                   ErrMsg("Get tablespace page item pointer failed, pdbId %u, tablespaceId %hu.", m_pdbId, itemNo));
            return DSTORE_FAIL;
        }

        if (tbsItem->used != 0) {
            continue;
        }

        if (tbsItem->reuseVersion == 0) {
            tbsId = itemNo;
            break;
        }

        if (tbsItem->reuseVersion < minReuseVer) {
            minReuseVer = tbsItem->reuseVersion;
            tbsId = itemNo;
        }
    }

    if (unlikely(tbsId == INVALID_TABLESPACE_ID)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to find a valid tablespaceId. The number of tablespaces has reached the maximum, pdbId "
                      "%u, itemNo %u.",
                      m_pdbId, itemNo));
        return DSTORE_FAIL;
    }

    TablespaceMgr *tablespaceMgr = g_storageInstance->GetTablespaceMgrByPdbId(m_pdbId);
    if (STORAGE_VAR_NULL(tablespaceMgr)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to get tablespaceMgr when allocating tablespaceId, pdbId %u.", m_pdbId));
        return DSTORE_FAIL;
    }
    RetStatus ret = tablespaceMgr->SendInvalidTablespace(tbsId, INVALID_DATA_FILE_ID);
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
            ErrMsg(
                "Failed to send invalid tablespace message when allocating tablespaceId, pdbId %u, tablespaceId %hu.",
                m_pdbId, tbsId));
        return DSTORE_FAIL;
    }

    tbsItem = GetTbsPageItemPtrInternal(tbsId, &blkno);
    if (STORAGE_VAR_NULL(tbsItem)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Get tablespace page item pointer failed, pdbId %u, tablespaceId %hu, blkno %u.", m_pdbId, tbsId,
                      blkno));
        return DSTORE_FAIL;
    }
    uint64 preReuseVersion = tbsItem->reuseVersion;
    tbsItem->SetItem(tbsMaxSize);
    MarkPageDirty(blkno);

    if (needWal) {
        AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
        walWriterContext->BeginAtomicWal(thrd->GetCurrentXid());
        WalRecordTbsCreateTablespace walData;
        walData.SetHeader(WAL_TBS_CREATE_TABLESPACE, sizeof(WalRecordTbsCreateTablespace));
        walData.SetData(tbsId, tbsMaxSize, preReuseVersion, thrd->GetCurrentXid());
        walWriterContext->PutNewWalRecord(&walData);
        WalGroupLsnInfo walGroupPtr = walWriterContext->EndAtomicWal();
        walWriterContext->WaitTargetPlsnPersist(walGroupPtr);
    }

    if (STORAGE_FUNC_FAIL(PostGroup())) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("PostGroup failed, pdbId %u, tablespaceId %hu.", m_pdbId, tbsId));
        return DSTORE_FAIL;
    }

    *tablespaceId = tbsId;
    ErrLog(DSTORE_LOG, MODULE_TABLESPACE,
           ErrMsg("Alloc tablespaceId %u success, pdbId %u, blkno %u.", m_pdbId, *tablespaceId, blkno));
    return DSTORE_SUCC;
}

RetStatus ControlTablespace::FreeTbsId(TablespaceId tablespaceId, Xid ddlXid, bool needWal)
{
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_EXCLUSIVE))) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Acquire control file lock fail, pdbId %u, tablespaceId %hu.", m_pdbId, tablespaceId));
        return DSTORE_FAIL;
    }
    ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_EXCLUSIVE};

    TablespaceMgr *tablespaceMgr = g_storageInstance->GetTablespaceMgrByPdbId(m_pdbId);
    if (STORAGE_VAR_NULL(tablespaceMgr)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to get tablespaceMgr when releasing tablespaceId, pdbId %u, tablespaceId %hu.", m_pdbId,
                      tablespaceId));
        return DSTORE_FAIL;
    }
    RetStatus ret = tablespaceMgr->SendInvalidTablespace(tablespaceId, INVALID_DATA_FILE_ID);
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
            ErrMsg(
                "Failed to send invalid tablespace message when releasing tablespaceId, pdbId %u, tablespaceId %hu.",
                m_pdbId, tablespaceId));
        return DSTORE_FAIL;
    }

    if (unlikely(LoadGroup() == DSTORE_FAIL)) {
        return DSTORE_FAIL;
    }

    BlockNumber blkno;
    ControlTablespacePageItemData *tbsItem = GetTbsPageItemPtrInternal(tablespaceId, &blkno);
    if (STORAGE_VAR_NULL(tbsItem)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
            ErrMsg("Get tablespace item failed, pdbId %u, tablespaceId %hu, blkno %u.", m_pdbId, tablespaceId, blkno));
        return DSTORE_FAIL;
    }

    uint64 reuseVersion = tbsItem->reuseVersion;
    uint16 hwm = tbsItem->hwm;
    tbsItem->ResetItem(ddlXid);
    MarkPageDirty(blkno);

    if (needWal) {
        AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
        walWriterContext->BeginAtomicWal(ddlXid);
        WalRecordTbsDropTablespace walData;
        walData.SetHeader(WAL_TBS_DROP_TABLESPACE, sizeof(WalRecordTbsDropTablespace));
        walData.SetData(tablespaceId, hwm, reuseVersion, ddlXid);
        walWriterContext->PutNewWalRecord(&walData);
        WalGroupLsnInfo walGroupPtr = walWriterContext->EndAtomicWal();
        walWriterContext->WaitTargetPlsnPersist(walGroupPtr);
    }

    if (STORAGE_FUNC_FAIL(PostGroup())) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("PostGroup failed, pdbId %u, tablespaceId %hu.", m_pdbId, tablespaceId));
        return DSTORE_FAIL;
    }

    ErrLog(DSTORE_LOG, MODULE_TABLESPACE,
            ErrMsg("Free tablespaceId success, pdbId %u, tablespaceId %hu, blkno %u.", m_pdbId, tablespaceId, blkno));
    return DSTORE_SUCC;
}

RetStatus ControlTablespace::OperateTbsItemFileId(TablespaceId tablespaceId, FileId fileId, bool isAssociate,
                                                  uint16 *slotId, bool needWal)
{
    if (unlikely(slotId == nullptr)) {
        return DSTORE_FAIL;
    }
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_EXCLUSIVE))) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Acquire control file lock fail, pdbId %u, tablespaceId %u, fileId %u, isAssociate %u.", m_pdbId,
                      tablespaceId, fileId, isAssociate));
        return DSTORE_FAIL;
    }
    ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_EXCLUSIVE};

    TablespaceMgr *tablespaceMgr = g_storageInstance->GetTablespaceMgrByPdbId(m_pdbId);
    if (STORAGE_VAR_NULL(tablespaceMgr)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to get tablespaceMgr when operating tablespace item, pdbId %u, tablespaceId %hu, fileId "
                      "%hu, isAssociate %u.",
                      m_pdbId, tablespaceId, fileId, isAssociate));
        return DSTORE_FAIL;
    }
    RetStatus ret = tablespaceMgr->SendInvalidTablespace(tablespaceId, fileId);
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to send invalid tablespace message when operating fileId, pdbId %u, tablespaceId %hu, "
                      "fileId %hu.",
                      m_pdbId, tablespaceId, fileId));
        return DSTORE_FAIL;
    }

    if (unlikely(LoadGroup() == DSTORE_FAIL)) {
        return DSTORE_FAIL;
    }

    BlockNumber blkno;
    ControlTablespacePageItemData *tbsItem = GetTbsPageItemPtrInternal(tablespaceId, &blkno);
    if (tbsItem == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Get tablespace page item pointer failed, pdbId %u, tablespaceId %u.", m_pdbId, tablespaceId));
        return DSTORE_FAIL;
    }

    ret = DSTORE_FAIL;
    uint16 slotIdTmp = 0;
    if (isAssociate) {
        ret = tbsItem->AssociateFile(fileId, &slotIdTmp);
    } else {
        ret = tbsItem->DisassociateFile(fileId, &slotIdTmp);
    }
    if (ret == DSTORE_FAIL) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Associate or disassociate file failed, pdbId %u, tablespaceId %u, fileId %u, isAssociate %u",
                      m_pdbId, tablespaceId, fileId, isAssociate));
        return DSTORE_FAIL;
    }

    MarkPageDirty(blkno);

    if (isAssociate && needWal) {
        AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
        walWriterContext->BeginAtomicWal(thrd->GetCurrentXid());
        WalRecordTbsAddFileToTbs walData;
        walData.SetHeader(WAL_TBS_ADD_FILE_TO_TABLESPACE, sizeof(WalRecordTbsAddFileToTbs));
        walData.SetData(tbsItem->tablespaceId, tbsItem->hwm, fileId, tbsItem->reuseVersion,
            tbsItem->ddlXid, slotIdTmp);
        walWriterContext->PutNewWalRecord(&walData);
        WalGroupLsnInfo walGroupPtr = walWriterContext->EndAtomicWal();
        walWriterContext->WaitTargetPlsnPersist(walGroupPtr);
    }

    if (STORAGE_FUNC_FAIL(PostGroup())) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("PostGroup failed."));
        return DSTORE_FAIL;
    }

    *slotId = slotIdTmp;
    ErrLog(DSTORE_LOG, MODULE_TABLESPACE,
           ErrMsg("Associate or disassociate file success, pdbId %u, tablespaceId %u, fileId %u, slotId %hu, blkno %u, "
                  "isAssociate %u.",
                  m_pdbId, tablespaceId, fileId, *slotId, blkno, isAssociate));
    return DSTORE_SUCC;
}

RetStatus ControlTablespace::AddFileIdToTbs(TablespaceId tablespaceId, FileId fileId, uint16 *slotId, bool needWal)
{
    return OperateTbsItemFileId(tablespaceId, fileId, true, slotId, needWal);
}

RetStatus ControlTablespace::FreeFileIdFromTbs(TablespaceId tablespaceId, FileId fileId, uint16 *slotId)
{
    return OperateTbsItemFileId(tablespaceId, fileId, false, slotId, false);
}

RetStatus ControlTablespace::AddFileIdsToList(FileId *fileIds, uint16 size, dlist_head *fileIdList, uint32 *fileCount)
{
    if (unlikely(fileIds == nullptr || fileIdList == nullptr || fileCount == nullptr)) {
        return DSTORE_FAIL;
    }
    uint32 cnt = 0;
    RetStatus ret = DSTORE_SUCC;
    AutoMemCxtSwitch autoSwitch(GetMemCtx());
    for (uint16 num = 0; num < size; num++) {
        if (fileIds[num] != INVALID_DATA_FILE_ID) {
            FileIdNode *fileIdNode = static_cast<FileIdNode *>(DstorePalloc0(sizeof(FileIdNode)));
            if (fileIdNode == nullptr) {
                ret = DSTORE_FAIL;
                break;
            }
            fileIdNode->fileId = fileIds[num];
            DListPushTail(fileIdList, &fileIdNode->node);
            cnt++;
        }
    }

    if (ret != DSTORE_SUCC) {
        for (uint32 i = 0; i < cnt; i++) {
            dlist_node *tailNode = DListPopTailNode(fileIdList);
            FileIdNode *fileIdNode = dlist_container(FileIdNode, node, tailNode);
            DstorePfreeExt(fileIdNode);
        }
        return ret;
    }

    *fileCount = cnt;
    return DSTORE_SUCC;
}

RetStatus ControlTablespace::GetFilesFromTablespace(TablespaceId tablespaceId, dlist_head *fileIdList,
                                                    uint32 *fileCount)
{
    if (fileIdList == nullptr || fileCount == nullptr || tablespaceId == INVALID_TABLESPACE_ID ||
        tablespaceId > MAX_TABLESPACE_ID) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("fileIdList is null, fileCount is null, or tablespaceId %hu is invalid, pdbId %u.",
                      tablespaceId, m_pdbId));
        return DSTORE_FAIL;
    }

    if (m_lock->Lock(CFLockMode::CF_SHARE) == DSTORE_FAIL) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Acquire control file lock fail, pdbId %u, tablespaceId %hu.", m_pdbId, tablespaceId));
        return DSTORE_FAIL;
    }
    ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_SHARE};

    if (unlikely(LoadGroup() == DSTORE_FAIL)) {
        return DSTORE_FAIL;
    }

    RetStatus ret = GetFilesFromTablespaceInternal(tablespaceId, fileIdList, fileCount);
    return ret;
}

RetStatus ControlTablespace::GetFilesFromTablespaceInternal(TablespaceId tablespaceId, dlist_head *fileIdList,
                                                            uint32 *fileCount)
{
    uint32 fileCountTmp = 0;
    RetStatus ret = DSTORE_SUCC;
    ControlTablespacePageItemData *tbsItem = NULL;
    do {
        tbsItem = GetTbsPageItemPtrInternal(tablespaceId, nullptr);
        if (tbsItem == nullptr) {
            ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
                   ErrMsg("Tablespace item is null, pdbId %u, tablespaceId %hu.", m_pdbId, tablespaceId));
            ret = DSTORE_FAIL;
            break;
        }
        if (tbsItem->used == 0 || tbsItem->hwm == 0) {
            ErrLog(DSTORE_LOG, MODULE_TABLESPACE,
                   ErrMsg("The tablespace is not in use or there is no file in the tablespace, pdbId %u, tablespaceId "
                          "%hu, hwm %hu.",
                          m_pdbId, tablespaceId, tbsItem->hwm));
            break;
        }
        FileId *fileIds =
            reinterpret_cast<FileId *>((char *)tbsItem + offsetof(ControlTablespacePageItemData, fileIds));
        ret = AddFileIdsToList(fileIds, tbsItem->hwm, fileIdList, &fileCountTmp);
        if (ret != DSTORE_SUCC) {
            ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
                   ErrMsg("Add fileIds to list failed, pdbId %u, tablespaceId %hu, hwm %hu", m_pdbId, tablespaceId,
                          tbsItem->hwm));
            break;
        }

        *fileCount = fileCountTmp;
    } while (0);

    return ret;
}

RetStatus ControlTablespace::GetFilesFromAllTablespace(dlist_head *fileIdList, uint32 *fileCount)
{
    if (fileIdList == nullptr || fileCount == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("fileIdList is null, or fileCount is null, pdbId %u.", m_pdbId));
        return DSTORE_FAIL;
    }

    if (m_lock->Lock(CFLockMode::CF_SHARE) == DSTORE_FAIL) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("Acquire control file lock fail, pdbId %u.", m_pdbId));
        return DSTORE_FAIL;
    }
    ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_SHARE};

    if (unlikely(LoadGroup() == DSTORE_FAIL)) {
        return DSTORE_FAIL;
    }

    uint32 totalfileCount = 0;
    RetStatus ret = DSTORE_SUCC;
    uint16 maxItemNo = MAX_TABLESPACE_ID;
    int itemNo = 1;
    while (itemNo <= maxItemNo) {
        uint32 fileCountTmp = 0;
        ret = GetFilesFromTablespaceInternal(itemNo, fileIdList, &fileCountTmp);
        if (ret != DSTORE_SUCC) {
            ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
                   ErrMsg("Get files from tablespace failed, pdbId %u, tablespaceId %hu.", m_pdbId, itemNo));
            FreeFileIdList(fileIdList);
            totalfileCount = 0;
            break;
        }
        totalfileCount += fileCountTmp;
        itemNo++;
    }

    *fileCount = totalfileCount;
    return ret;
}

void ControlTablespace::FreeFileIdList(dlist_head *fileIdListHead)
{
    StorageAssert(fileIdListHead != nullptr);
    if (unlikely(fileIdListHead == nullptr)) {
        return;
    }

    dlist_mutable_iter iter;
    dlist_foreach_modify(iter, fileIdListHead)
    {
        FileIdNode *fileIdNode = dlist_container(FileIdNode, node, iter.cur);
        DListDelete(iter.cur);
        DstorePfreeExt(fileIdNode);
    }
}

RetStatus ControlTablespace::AlterTbsMaxSize(TablespaceId tablespaceId, uint64 tbsMaxSize, bool needWal)
{
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_EXCLUSIVE))) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("AlterTbsMaxSize Acquire control file lock fail, pdbId %u.", m_pdbId));
        return DSTORE_FAIL;
    }
    ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_EXCLUSIVE};

    TablespaceMgr *tablespaceMgr = g_storageInstance->GetTablespaceMgrByPdbId(m_pdbId);
    if (STORAGE_VAR_NULL(tablespaceMgr)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to get tablespaceMgr when altering maxsize, pdbId %u, tablespaceId %hu.",
                      m_pdbId, tablespaceId));
        return DSTORE_FAIL;
    }
    RetStatus ret = tablespaceMgr->SendInvalidTablespace(tablespaceId, INVALID_DATA_FILE_ID);
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to send invalid tablespace message when altering maxsize, pdbId %u, tablespaceId %hu.",
                      m_pdbId, tablespaceId));
        return DSTORE_FAIL;
    }

    if (unlikely(LoadGroup() == DSTORE_FAIL)) {
        return DSTORE_FAIL;
    }

    BlockNumber blkno;
    OffsetNumber offset;
    GetTbsPageItemCtid(tablespaceId, &blkno, &offset);

    ControlTablespacePageItemData *tbsItem = GetTbsPageItemPtrInternal(tablespaceId, &blkno);
    if (unlikely(tbsItem == nullptr)) {
        ErrLog(
            DSTORE_ERROR, MODULE_TABLESPACE,
            ErrMsg("AlterTbsMaxSize Get tablespace item failed, pdbId %u, tablespaceId %hu.", m_pdbId, tablespaceId));
        return DSTORE_FAIL;
    }

    tbsItem->AlterItemMaxSize(tbsMaxSize);
    MarkPageDirty(blkno);

    if (needWal) {
        AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
        walWriterContext->BeginAtomicWal(thrd->GetCurrentXid());
        WalRecordTbsAlterTablespace walData;
        walData.SetHeader(WAL_TBS_ALTER_TABLESPACE, sizeof(WalRecordTbsAlterTablespace));
        walData.SetData(tablespaceId, tbsMaxSize, tbsItem->reuseVersion, thrd->GetCurrentXid());
        walWriterContext->PutNewWalRecord(&walData);
        WalGroupLsnInfo walGroupPtr = walWriterContext->EndAtomicWal();
        walWriterContext->WaitTargetPlsnPersist(walGroupPtr);
    }

    if (STORAGE_FUNC_FAIL(PostGroup())) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("AlterTbsMaxSize PostGroup failed, pdbId %u, tablespaceId %hu.", m_pdbId, tablespaceId));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus ControlTablespace::InitTbsOrDatafileItemPages(ControlPageType pageType)
{
    if (pageType != ControlPageType::CONTROL_TBS_TABLESPACE_DATAPAGE_TYPE &&
        pageType != ControlPageType::CONTROL_TBS_DATAFILE_DATAPAGE_TYPE) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("pageType %u is invalid, pdbId %u.", pageType, m_pdbId));
        return DSTORE_FAIL;
    }

    BlockNumber startPageNo;
    BlockNumber endPageNo;
    uint32 maxItemNo;
    uint32 itemCntPerPage;
    ControlTablespacePageItemData *tbsItem = nullptr;
    ControlDataFilePageItemData *datafileItem = nullptr;
    AutoMemCxtSwitch autoSwitch(GetMemCtx());
    if (pageType == ControlPageType::CONTROL_TBS_TABLESPACE_DATAPAGE_TYPE) {
        startPageNo = DEFAULT_TABLESPACE_PAGE;
        endPageNo = DEFAULT_TABLESPACE_PAGE + MAX_TABLESPACE_PAGE_CNT - 1;
        maxItemNo = MAX_TABLESPACE_ID;
        itemCntPerPage = MAX_TABLESPACE_ITEM_CNT_PER_PAGE;
        tbsItem = static_cast<ControlTablespacePageItemData *>(DstorePalloc0(sizeof(ControlTablespacePageItemData)));
    } else {
        startPageNo = DEFAULT_DATAFILE_PAGE;
        endPageNo = DEFAULT_DATAFILE_PAGE + MAX_DATAFILE_PAGE_CNT - 1;
        maxItemNo = MAX_VFS_FILE_ID;
        itemCntPerPage = MAX_DATAFILE_ITEM_CNT_PER_PAGE;
        datafileItem = static_cast<ControlDataFilePageItemData *>(DstorePalloc0(sizeof(ControlDataFilePageItemData)));
    }
    if (unlikely(tbsItem == nullptr && datafileItem == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Init tablespace or datafile item DstorePalloc0 failed, pdbId %u.", m_pdbId));
        return DSTORE_FAIL;
    }

    uint32 itemNo = 0;
    for (BlockNumber blockNumber = startPageNo; blockNumber <= endPageNo; blockNumber++) {
        ControlDataPage *controlPage = GetPage(blockNumber);
        if (controlPage == nullptr) {
            ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
                   ErrMsg("GatPage failed, pdbId %u, blockNumber %u.", m_pdbId, blockNumber));
            DstorePfreeExt(tbsItem);
            DstorePfreeExt(datafileItem);
            return DSTORE_FAIL;
        }

        controlPage->InitDataPage(pageType);
        for (uint32 i = 0; i < itemCntPerPage; i++) {
            if (itemNo > maxItemNo) {
                break;
            }
            RetStatus ret = DSTORE_FAIL;
            if (pageType == ControlPageType::CONTROL_TBS_TABLESPACE_DATAPAGE_TYPE) {
                tbsItem->InitItem(itemNo);
                ret = controlPage->AddItem(tbsItem, sizeof(ControlTablespacePageItemData));
            } else {
                datafileItem->InitItem(itemNo);
                ret = controlPage->AddItem(datafileItem, sizeof(ControlDataFilePageItemData));
            }
            if (ret == DSTORE_FAIL) {
                ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
                       ErrMsg("Init item failed, page(%u-%u), pdbId %u.", pageType, blockNumber, m_pdbId));
                DstorePfreeExt(tbsItem);
                DstorePfreeExt(datafileItem);
                return DSTORE_FAIL;
            }
            itemNo++;
        }
        MarkPageDirty(blockNumber);
    }
    DstorePfreeExt(tbsItem);
    DstorePfreeExt(datafileItem);

    return DSTORE_SUCC;
}

RetStatus ControlTablespace::UpdateCreateTablespace(TablespaceId tablespaceId, Xid ddlXid, uint64 tbsMaxSize,
                                                    uint64 preReuseVersion)
{
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_EXCLUSIVE))) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("WAL_TBS_CREATE_TABLESPACE: Acquire control file lock fail, pdbId %u, tablespaceId %hu.", m_pdbId,
                      tablespaceId));
        return DSTORE_FAIL;
    }
    ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_EXCLUSIVE};

    TablespaceMgr *tablespaceMgr = g_storageInstance->GetTablespaceMgrByPdbId(m_pdbId);
    if (STORAGE_VAR_NULL(tablespaceMgr)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("WAL_TBS_CREATE_TABLESPACE: Failed to get tablespaceMgr, pdbId %u.", m_pdbId));
        return DSTORE_FAIL;
    }
    RetStatus ret = tablespaceMgr->SendInvalidTablespace(tablespaceId, INVALID_DATA_FILE_ID);
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
            ErrMsg("WAL_TBS_CREATE_TABLESPACE: Failed to send invalid tablespace message, pdbId %u, tablespaceId %hu.",
                   m_pdbId, tablespaceId));
        return DSTORE_FAIL;
    }

    if (unlikely(LoadGroup() == DSTORE_FAIL)) {
        return DSTORE_FAIL;
    }

    BlockNumber blkno;
    ControlTablespacePageItemData *tbsItem = GetTbsPageItemPtrInternal(tablespaceId, &blkno);
    if (tbsItem == nullptr || tbsItem->tablespaceId != tablespaceId) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("WAL_TBS_CREATE_TABLESPACE: Failed to get tablespace item, pdbId %u, tablespaceId %hu.", m_pdbId,
                      tablespaceId));
        return DSTORE_FAIL;
    }

    tbsItem->used = 1;
    tbsItem->tbsMaxSize = tbsMaxSize;
    tbsItem->ddlXid = ddlXid;
    if (preReuseVersion == tbsItem->reuseVersion) {
        tbsItem->reuseVersion++;
    }
    MarkPageDirty(blkno);
    if (STORAGE_FUNC_FAIL(PostGroup())) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
            ErrMsg("WAL_TBS_CREATE_TABLESPACE: PostGroup failed, pdbId %u, tablespaceId %hu.", m_pdbId, tablespaceId));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus ControlTablespace::UpdateCreateDataFile(TablespaceId tablespaceId, FileId fileId, uint64 fileMaxSize,
                                                  ExtentSize extentSize, uint64 preReuseVersion, Xid ddlXid)
{
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_EXCLUSIVE))) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("WAL_TBS_CREATE_DATA_FILE: Acquire control file lock fail, pdbId %u, tablespaceId %hu.", m_pdbId,
                      tablespaceId));
        return DSTORE_FAIL;
    }
    ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_EXCLUSIVE};

    TablespaceMgr *tablespaceMgr = g_storageInstance->GetTablespaceMgrByPdbId(m_pdbId);
    if (STORAGE_VAR_NULL(tablespaceMgr)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("WAL_TBS_CREATE_DATA_FILE: Failed to get tablespaceMgr, pdbId %u.", m_pdbId));
        return DSTORE_FAIL;
    }
    RetStatus ret = tablespaceMgr->SendInvalidTablespace(tablespaceId, fileId);
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("WAL_TBS_CREATE_DATA_FILE: Failed to send invalid tablespace message, pdbId %u, tablespaceId "
                      "%hu, fileId %hu.",
                      m_pdbId, tablespaceId, fileId));
        return DSTORE_FAIL;
    }

    if (unlikely(LoadGroup() == DSTORE_FAIL)) {
        return DSTORE_FAIL;
    }

    BlockNumber blkno;
    ControlDataFilePageItemData *fileItem = GetDataFilePageItemPtrInternal(fileId, &blkno);
    if (fileItem == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("WAL_TBS_CREATE_DATA_FILE: datafile item is NULL, pdbId %u, tablespaceId %hu, fileId %hu.",
                      m_pdbId, tablespaceId, fileId));
        return DSTORE_FAIL;
    }

    ErrLog(DSTORE_LOG, MODULE_TABLESPACE, ErrMsg("WAL_TBS_CREATE_DATA_FILE BEGIN"));
    ErrLog(DSTORE_LOG, MODULE_TABLESPACE,
           ErrMsg("WAL_TBS_CREATE_DATA_FILE ORIGIN DATA: pdbId:%u; tablespaceId:%hu; "
                  "filedId:%hu; ZoneId:%u; slotId:%lu, prev reuse version:%lu; fileMaxSize:%lu",
                  m_pdbId, fileItem->tablespaceId, fileItem->fileId, static_cast<uint32>(fileItem->ddlXid.m_zoneId),
                  fileItem->ddlXid.m_logicSlotId, fileItem->reuseVersion, fileItem->fileMaxSize));

    fileItem->used = 1;
    if (fileItem->reuseVersion == preReuseVersion) {
        fileItem->reuseVersion++;
    }
    fileItem->fileMaxSize = fileMaxSize;
    fileItem->extentSize = extentSize;
    fileItem->ddlXid = ddlXid;
    fileItem->tablespaceId = tablespaceId;

    MarkPageDirty(blkno);
    if (STORAGE_FUNC_FAIL(PostGroup())) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
            ErrMsg("WAL_TBS_CREATE_DATA_FILE: PostGroup failed, pdbId %u, tablespaceId %hu.", m_pdbId, tablespaceId));
        return DSTORE_FAIL;
    }
    ErrLog(DSTORE_LOG, MODULE_TABLESPACE,
           ErrMsg("WAL_TBS_CREATE_DATA_FILE WAL_RECORD DATA: tablespaceId:%hu; "
                  "filedId:%hu; ZoneId:%u; slotId:%lu, prev reuse version:%lu; fileMaxSize:%lu",
                  tablespaceId, fileId, static_cast<uint32>(ddlXid.m_zoneId), ddlXid.m_logicSlotId, preReuseVersion,
                  fileMaxSize));
    return DSTORE_SUCC;
}

RetStatus ControlTablespace::UpdateAddFileToTbs(TablespaceId tablespaceId, FileId fileId, uint16 slotId, uint16 hwm,
    uint64 preReuseVersion, Xid ddlXid)
{
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_EXCLUSIVE))) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("WAL_TBS_ADD_FILE_TO_TABLESPACE: Acquire control file lock fail, pdbId %u, tablespaceId %hu.",
                      m_pdbId, tablespaceId));
        return DSTORE_FAIL;
    }
    ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_EXCLUSIVE};

    TablespaceMgr *tablespaceMgr = g_storageInstance->GetTablespaceMgrByPdbId(m_pdbId);
    if (STORAGE_VAR_NULL(tablespaceMgr)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("WAL_TBS_ADD_FILE_TO_TABLESPACE: Failed to get tablespaceMgr, pdbId %u.", m_pdbId));
        return DSTORE_FAIL;
    }
    RetStatus ret = tablespaceMgr->SendInvalidTablespace(tablespaceId, fileId);
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("WAL_TBS_ADD_FILE_TO_TABLESPACE: Failed to send invalid tablespace message, pdbId %u, "
                      "tablespaceId %hu, fileId %hu.",
                      m_pdbId, tablespaceId, fileId));
        return DSTORE_FAIL;
    }

    if (unlikely(LoadGroup() == DSTORE_FAIL)) {
        return DSTORE_FAIL;
    }

    BlockNumber blkno;
    ControlTablespacePageItemData *tbsItem = GetTbsPageItemPtrInternal(tablespaceId, &blkno);
    if (tbsItem == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
            ErrMsg("WAL_TBS_ADD_FILE_TO_TABLESPACE: tablespace item is NULL, pdbId %u, tablespaceId %hu, fileId %hu.",
                   m_pdbId, tablespaceId, fileId));
        return DSTORE_FAIL;
    }

    ErrLog(DSTORE_LOG, MODULE_TABLESPACE, ErrMsg("WAL_TBS_ADD_FILE_TO_TABLESPACE BEGIN"));
    ErrLog(DSTORE_LOG, MODULE_TABLESPACE,
        ErrMsg("WAL_TBS_ADD_FILE_TO_TABLESPACE ORIGIN DATA: pdbId:%u; tablespaceId:%hu; filedId:%hu; ZoneId:%u; "
               "slotId:%lu, prev reuse version:%lu; hwm:%hu; slotId:%hu",
               m_pdbId, tbsItem->tablespaceId, tbsItem->fileIds[slotId], static_cast<uint32>(tbsItem->ddlXid.m_zoneId),
               tbsItem->ddlXid.m_logicSlotId, tbsItem->reuseVersion, tbsItem->hwm, slotId));

    /* mark dirty related tablespace item data in control file */
    if (tbsItem->fileIds[slotId] == fileId) {
        ErrLog(
            DSTORE_LOG, MODULE_TABLESPACE,
            ErrMsg("WAL_TBS_ADD_FILE_TO_TABLESPACE slotid has added already, pdbId %u, tablespaceId %hu, fileId %hu.",
                   m_pdbId, tablespaceId, fileId));
    } else {
        tbsItem->fileIds[slotId] = fileId;
    }
    tbsItem->hwm = hwm;

    MarkPageDirty(blkno);
    if (STORAGE_FUNC_FAIL(PostGroup())) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("WAL_TBS_ADD_FILE_TO_TABLESPACE: PostGroup failed, pdbId %u, tablespaceId %hu.", m_pdbId,
                      tablespaceId));
        return DSTORE_FAIL;
    }
    ErrLog(DSTORE_LOG, MODULE_TABLESPACE,
           ErrMsg("WAL_TBS_ADD_FILE_TO_TABLESPACE WAL_RECORD DATA: tablespaceId:%hu; "
                  "filedId:%hu; ZoneId:%u; slotId:%lu, prev reuse version:%lu; hwm:%hu; slotId:%hu",
                  tablespaceId, fileId, static_cast<uint32>(ddlXid.m_zoneId), ddlXid.m_logicSlotId, preReuseVersion,
                  hwm, slotId));
    return DSTORE_SUCC;
}

RetStatus ControlTablespace::UpdateDropTablespace(TablespaceId tablespaceId, uint64 preReuseVersion, Xid ddlXid,
                                                  uint16 hwm)
{
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_EXCLUSIVE))) {
        ErrLog(DSTORE_PANIC, MODULE_TABLESPACE,
               ErrMsg("WAL_TBS_DROP_TABLESPACE: Acquire control file lock fail, pdbId %u, tablespaceId %hu.",
                      m_pdbId, tablespaceId));
    }
    ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_EXCLUSIVE};

    TablespaceMgr *tablespaceMgr = g_storageInstance->GetTablespaceMgrByPdbId(m_pdbId);
    if (STORAGE_VAR_NULL(tablespaceMgr)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("WAL_TBS_DROP_TABLESPACE: Failed to get tablespaceMgr, pdbId %u.", m_pdbId));
        return DSTORE_FAIL;
    }
    RetStatus ret = tablespaceMgr->SendInvalidTablespace(tablespaceId, INVALID_DATA_FILE_ID);
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("WAL_TBS_DROP_TABLESPACE: Failed to send invalid tablespace message, pdbId %u, tablespaceId %hu.",
                      m_pdbId, tablespaceId));
        return DSTORE_FAIL;
    }

    if (unlikely(LoadGroup() == DSTORE_FAIL)) {
        return DSTORE_FAIL;
    }

    BlockNumber blkno;
    ControlTablespacePageItemData *tbsItem = GetTbsPageItemPtrInternal(tablespaceId, &blkno);
    if (tbsItem == nullptr || tbsItem->tablespaceId != tablespaceId) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("WAL_TBS_DROP_TABLESPACE: Failed to get tbsItem, pdbId %u, tablespaceId %hu.",
                      m_pdbId, tablespaceId));
        return DSTORE_FAIL;
    }

    ErrLog(DSTORE_LOG, MODULE_TABLESPACE, ErrMsg("WAL_TBS_DROP_TABLESPACE BEGIN"));
    ErrLog(DSTORE_LOG, MODULE_TABLESPACE,
           ErrMsg("WAL_TBS_DROP_TABLESPACE ORIGIN DATA: pdbId:%u; tablespaceId:%hu;"
                  "ZoneId:%u; slotId:%lu; prev reuse version:%lu; hwm:%hu",
                  m_pdbId, tbsItem->tablespaceId, static_cast<uint32>(tbsItem->ddlXid.m_zoneId),
                  tbsItem->ddlXid.m_logicSlotId, tbsItem->reuseVersion, tbsItem->hwm));

    tbsItem->used = 0;
    tbsItem->tbsMaxSize = 0;
    tbsItem->hwm = 0;
    tbsItem->ddlXid = INVALID_XID;
    if (tbsItem->reuseVersion == preReuseVersion) {
        tbsItem->reuseVersion++;
    }
    MarkPageDirty(blkno);

    if (STORAGE_FUNC_FAIL(PostGroup())) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("WAL_TBS_DROP_TABLESPACE: PostGroup failed, pdbId %u, tablespaceId %hu.", m_pdbId, tablespaceId));
        return DSTORE_FAIL;
    }
    ErrLog(DSTORE_LOG, MODULE_TABLESPACE,
           ErrMsg("WAL_TBS_DROP_TABLESPACE WAL_RECORD DATA: tablespaceId:%hu;"
                  "ZoneId:%u; slotId:%lu; prev reuse version:%lu; hwm:%hu",
                  tablespaceId, static_cast<uint32>(ddlXid.m_zoneId), ddlXid.m_logicSlotId, preReuseVersion, hwm));
    return DSTORE_SUCC;
}

RetStatus ControlTablespace::UpdateDropDataFile(TablespaceId tablespaceId, FileId fileId, uint64 preReuseVersion,
                                                uint16 slotId, uint16 hwm)
{
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_EXCLUSIVE))) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("WAL_TBS_DROP_DATA_FILE: Acquire control file lock fail, pdbId %u, tablespaceId %hu, fileId %hu.",
                      m_pdbId, tablespaceId, fileId));
        return DSTORE_FAIL;
    }
    ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_EXCLUSIVE};

    TablespaceMgr *tablespaceMgr = g_storageInstance->GetTablespaceMgrByPdbId(m_pdbId);
    if (STORAGE_VAR_NULL(tablespaceMgr)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("WAL_TBS_DROP_DATA_FILE: Failed to get tablespaceMgr, pdbId %u.", m_pdbId));
        return DSTORE_FAIL;
    }
    RetStatus ret = tablespaceMgr->SendInvalidTablespace(tablespaceId, fileId);
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("WAL_TBS_DROP_DATA_FILE: Failed to send invalid tablespace message, pdbId %u, tablespaceId %hu, "
                      "fileId %hu.",
                      m_pdbId, tablespaceId, fileId));
        return DSTORE_FAIL;
    }

    if (unlikely(LoadGroup() == DSTORE_FAIL)) {
        return DSTORE_FAIL;
    }

    BlockNumber blkno = DSTORE_INVALID_BLOCK_NUMBER;
    ControlDataFilePageItemData *fileItem = GetDataFilePageItemPtrInternal(fileId, &blkno);
    if (fileItem == nullptr || fileItem->fileId != fileId) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
            ErrMsg("WAL_TBS_DROP_DATA_FILE: Failed to get fileItem, pdbId %u, tablespaceId %hu, fileId %hu, blkno %u.",
                   m_pdbId, tablespaceId, fileId, blkno));
        return DSTORE_FAIL;
    }

    ErrLog(DSTORE_LOG, MODULE_TABLESPACE, ErrMsg("WAL_TBS_DROP_DATA_FILE BEGIN"));

    /* write file item data in control file */
    fileItem->extentSize = INVALID_EXT_SIZE;
    fileItem->used = 0;
    fileItem->ddlXid = INVALID_XID;
    fileItem->fileMaxSize = 0;
    fileItem->tablespaceId = INVALID_TABLESPACE_ID;
    if (fileItem->reuseVersion == preReuseVersion) {
        fileItem->reuseVersion++;
    }

    if (tablespaceId != INVALID_TABLESPACE_ID && slotId < MAX_TBS_DATAFILE_SLOT_CNT) {
        BlockNumber tbs_blkno;
        ControlTablespacePageItemData *tbsItem = GetTbsPageItemPtrInternal(tablespaceId, &tbs_blkno);
        if (tbsItem == nullptr) {
            ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("WAL_TBS_DROP_DATA_FILE: Failed to get tbsItem, pdbId %u, "
                "tablespaceId %hu, fileId %hu.", m_pdbId, tablespaceId, fileId));
            return DSTORE_FAIL;
        }
        ErrLog(DSTORE_LOG, MODULE_TABLESPACE,
               ErrMsg("WAL_TBS_DROP_DATA_FILE ORIGIN DATA, pdbId:%u, tablespaceId:%hu, fileId:%hu,"
                      "slotId:%hu, hwm:%hu",
                      m_pdbId, fileItem->tablespaceId, tbsItem->fileIds[slotId], slotId, tbsItem->hwm));
        if (tbsItem->fileIds[slotId] == INVALID_DATA_FILE_ID) {
            ErrLog(DSTORE_LOG, MODULE_TABLESPACE,
                   ErrMsg("WAL_TBS_DROP_DATA_FILE: slotId has delete already, pdbId %u, tablespaceId %hu, fileId %hu.",
                          m_pdbId, tablespaceId, fileId));
        } else {
            tbsItem->fileIds[slotId] = INVALID_DATA_FILE_ID;
        }
        MarkPageDirty(tbs_blkno);
    }

    MarkPageDirty(blkno);

    if (STORAGE_FUNC_FAIL(PostGroup())) {
        ErrLog(DSTORE_PANIC, MODULE_TABLESPACE,
               ErrMsg("WAL_TBS_DROP_DATA_FILE: PostGroup failed, pdbId %u, tablespaceId %hu, fileId %hu.", m_pdbId,
                      tablespaceId, fileId));
    }
    ErrLog(DSTORE_LOG, MODULE_TABLESPACE,
           ErrMsg("WAL_TBS_DROP_DATA_FILE WAL_RECORD DATA: pdbId:%u, tablespaceId:%hu, fileId:%hu, slotId:%hu, hwm:%hu",
                  m_pdbId, tablespaceId, fileId, slotId, hwm));
    return DSTORE_SUCC;
}

RetStatus ControlTablespace::UpdateAlterTablespace(TablespaceId tablespaceId, uint64 tbsMaxSize, Xid ddlXid,
                                                   uint64 preReuseVersion)
{
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_EXCLUSIVE))) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("WAL_TBS_ALTER_TABLESPACE: Acquire control file lock fail, pdbId %u, tablespaceId %hu.", m_pdbId,
                      tablespaceId));
        return DSTORE_FAIL;
    }
    ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_EXCLUSIVE};

    TablespaceMgr *tablespaceMgr = g_storageInstance->GetTablespaceMgrByPdbId(m_pdbId);
    if (STORAGE_VAR_NULL(tablespaceMgr)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("WAL_TBS_ALTER_TABLESPACE: Failed to get tablespaceMgr, pdbId %u.", m_pdbId));
        return DSTORE_FAIL;
    }
    RetStatus ret = tablespaceMgr->SendInvalidTablespace(tablespaceId, INVALID_DATA_FILE_ID);
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
            ErrMsg("WAL_TBS_ALTER_TABLESPACE: Failed to send invalid tablespace message, pdbId %u, tablespaceId %hu.",
                   m_pdbId, tablespaceId));
        return DSTORE_FAIL;
    }

    if (unlikely(LoadGroup() == DSTORE_FAIL)) {
        return DSTORE_FAIL;
    }

    BlockNumber blkno;
    ControlTablespacePageItemData *tbsItem = GetTbsPageItemPtrInternal(tablespaceId, &blkno);
    if (tbsItem == nullptr || tbsItem->tablespaceId != tablespaceId) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("WAL_TBS_ALTER_TABLESPACE: Failed to get tablespace item, pdbId %u, tablespaceId %hu.", m_pdbId,
                      tablespaceId));
        return DSTORE_FAIL;
    }

    ErrLog(DSTORE_LOG, MODULE_TABLESPACE, ErrMsg("WAL_TBS_ALTER_TABLESPACE REDO BEGIN"));
    ErrLog(DSTORE_LOG, MODULE_TABLESPACE,
           ErrMsg("WAL_TBS_ALTER_TABLESPACE ORIGIN DATA: pdbId:%u, tablespaceId:%hu; ZoneId:%u; slotId:%lu;"
                  "prev reuse version:%lu; hwm:%hu",
                  m_pdbId, tbsItem->tablespaceId, static_cast<uint32>(tbsItem->ddlXid.m_zoneId),
                  tbsItem->ddlXid.m_logicSlotId, tbsItem->reuseVersion, tbsItem->hwm));

    tbsItem->used = 1;
    tbsItem->tbsMaxSize = tbsMaxSize;
    tbsItem->ddlXid = ddlXid;
    MarkPageDirty(blkno);

    if (STORAGE_FUNC_FAIL(PostGroup())) {
        ErrLog(
            DSTORE_ERROR, MODULE_TABLESPACE,
            ErrMsg("WAL_TBS_ALTER_TABLESPACE: PostGroup failed, pdbId %u, tablespaceId %hu.", m_pdbId, tablespaceId));
        return DSTORE_FAIL;
    }
    ErrLog(DSTORE_LOG, MODULE_TABLESPACE,
           ErrMsg("WAL_TBS_ALTER_TABLESPACE WAL_RECORD DATA: tablespaceId:%hu; "
                  "ZoneId:%u; slotId:%lu; prev reuse version:%lu;",
                  tablespaceId, static_cast<uint32>(ddlXid.m_zoneId), ddlXid.m_logicSlotId, preReuseVersion));
    return DSTORE_SUCC;
}

RetStatus ControlTablespace::GetTbsPageItemData(TablespaceId tablespaceId, ControlTablespacePageItemData *tbsItem)
{
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_SHARE))) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Acquire control file share lock fail, pdbId %u, tablespaceId %hu.", m_pdbId, tablespaceId));
        return DSTORE_FAIL;
    }
    ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_SHARE};

    if (unlikely(LoadGroup() == DSTORE_FAIL)) {
        return DSTORE_FAIL;
    }

    BlockNumber blkno;
    ControlTablespacePageItemData *item = GetTbsPageItemPtrInternal(tablespaceId, &blkno);
    if (item == NULL) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to get tablespace item, pdbId %u, tablespaceId %hu.", m_pdbId, tablespaceId));
        return DSTORE_FAIL;
    }

    *tbsItem = *item;
    return DSTORE_SUCC;
}

RetStatus ControlTablespace::GetDataFilePageItemData(FileId fileId, ControlDataFilePageItemData *filePageItem)
{
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_SHARE))) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Acquire control file share lock fail, pdbId %u, fileId %hu.", m_pdbId, fileId));
        return DSTORE_FAIL;
    }
    ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_SHARE};

    if (unlikely(LoadGroup() == DSTORE_FAIL)) {
        return DSTORE_FAIL;
    }

    BlockNumber blkno;
    ControlDataFilePageItemData *item = GetDataFilePageItemPtrInternal(fileId, &blkno);
    if (item == NULL) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to get datafile item, pdbId %u, fileId %hu.", m_pdbId, fileId));
        return DSTORE_FAIL;
    }

    *filePageItem = *item;
    return DSTORE_SUCC;
}

RetStatus ControlTablespace::GetALLDataFilePageItemDatasForTemp(
                            ControlDataFilePageItemData *filePageItems, uint32 count)
{
    if (count <= TMP_TBS_MAX_FILE_ID) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Invalid count:%u.", count));
        return DSTORE_FAIL;
    }
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_SHARE))) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Acquire control file share lock fail for temp tablespace."));
        return DSTORE_FAIL;
    }
    if (unlikely(LoadGroup() == DSTORE_FAIL)) {
        m_lock->Unlock(CFLockMode::CF_SHARE);
        return DSTORE_FAIL;
    }

    BlockNumber blkno;
    for (FileId fileId = TMP_TBS_START_FILE_ID; fileId <= TMP_TBS_MAX_FILE_ID; fileId++) {
        ControlDataFilePageItemData *item = GetDataFilePageItemPtrInternal(fileId, &blkno);
        if (item == NULL) {
            m_lock->Unlock(CFLockMode::CF_SHARE);
            return DSTORE_FAIL;
        }
        filePageItems[fileId] = *item;
    }
    m_lock->Unlock(CFLockMode::CF_SHARE);
    return DSTORE_SUCC;
}

RetStatus ControlTablespace::Create()
{
    /* step 1, init metapage */
    ControlMetaPage *metaPage = GetMetaPage();
    if (unlikely(metaPage == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("group %hhu get metapage fail, pdbId %u.", GetGroupType(), m_pdbId));
        return DSTORE_FAIL;
    }
    metaPage->InitMetaPage(CONTROL_TBS_METAPAGE_TYPE, 0);

    ControlMetaHeader *metaHeader = metaPage->GetControlMetaHeader();
    metaHeader->m_lastPageId = CONTROLFILE_PAGEMAP_TABLESPACE_START;
    ControlPageRange *pageRange = &metaHeader->m_pageRange[0];
    pageRange[0].m_start = CONTROLFILE_PAGEMAP_TABLESPACE_START;
    pageRange[0].m_end = CONTROLFILE_PAGEMAP_TABLESPACE_MAX;
    for (uint32 i = 1; i < CONTROL_MAX_PAGERANGE_NUM; i++) {
        pageRange[i].m_start = DSTORE_INVALID_BLOCK_NUMBER;
        pageRange[i].m_end = DSTORE_INVALID_BLOCK_NUMBER;
    }
    MarkPageDirty(CONTROLFILE_PAGEMAP_TABLESPACE_META);

    /* step 2 initialize tablespace item page and datafile item page */
    if (STORAGE_FUNC_FAIL(InitTbsOrDatafileItemPages(ControlPageType::CONTROL_TBS_TABLESPACE_DATAPAGE_TYPE)) ||
        STORAGE_FUNC_FAIL(InitTbsOrDatafileItemPages(ControlPageType::CONTROL_TBS_DATAFILE_DATAPAGE_TYPE))) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Init tablespace or datafile item pages failed, pdbId %u.", m_pdbId));
        return DSTORE_FAIL;
    }

    return DSTORE_SUCC;
}
}  // namespace DSTORE