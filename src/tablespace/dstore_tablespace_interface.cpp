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

#include "tablespace/dstore_tablespace_interface.h"
#include "heap/dstore_heap_handler.h"
#include "page/dstore_heap_page.h"
#include "page/dstore_index_page.h"
#include "table/dstore_table_interface.h"
#include "tablespace/dstore_heap_normal_segment.h"
#include "tablespace/dstore_heap_temp_segment.h"
#include "tablespace/dstore_index_normal_segment.h"
#include "tablespace/dstore_index_temp_segment.h"
#include "framework/dstore_instance.h"
#include "common/log/dstore_log.h"
#include "transaction/dstore_transaction_interface.h"
#include "index/dstore_btree.h"
#include "lock/dstore_lock_interface.h"

namespace TableSpace_Interface {
using namespace DSTORE;
PageId AllocSegment(PdbId pdbId, TablespaceId tablespaceId, SegmentType type, Oid tableOid)
{
    if (g_storageInstance->GetPdb(pdbId) != nullptr &&
        g_storageInstance->GetPdb(pdbId)->GetPdbRoleMode() != PdbRoleMode::PDB_PRIMARY) {
        storage_set_error(TBS_ERROR_PDB_FENCE_WRITE);
        return INVALID_PAGE_ID;
    }
    if (tablespaceId == INVALID_TABLESPACE_ID) {
        storage_set_error(TBS_ERROR_SEGMENT_PARAMETER_INVALID);
        return INVALID_PAGE_ID;
    }
    PageId segmentId = INVALID_PAGE_ID;
    uint8_t segType = static_cast<uint8_t>(type);
    switch (type) {
        case SegmentType::HEAP_SEGMENT_TYPE:
            segmentId = HeapNormalSegment::AllocHeapNormalSegment(pdbId, tablespaceId,
                                                                   g_storageInstance->GetBufferMgr(), tableOid);
            break;
        case SegmentType::INDEX_SEGMENT_TYPE:
            segmentId = IndexNormalSegment::AllocIndexNormalSegment(pdbId, tablespaceId,
                                                                     g_storageInstance->GetBufferMgr(), tableOid);
            break;
        case SegmentType::HEAP_TEMP_SEGMENT_TYPE:
            segmentId =
                HeapTempSegment::AllocHeapTempSegment(pdbId, tablespaceId, thrd->GetTmpLocalBufMgr(), tableOid);
            break;
        case SegmentType::INDEX_TEMP_SEGMENT_TYPE:
            segmentId =
                IndexTempSegment::AllocIndexTempSegment(pdbId, tablespaceId, thrd->GetTmpLocalBufMgr(), tableOid);
            break;
        case SegmentType::UNDO_SEGMENT_TYPE:
        default:
            ErrLog(DSTORE_ERROR, MODULE_TRANSACTION, ErrMsg("Invalid segment type(%hhu) while AllocSegment!", segType));
    }
    return segmentId;
}

static RetStatus DropSegmentInternal(DataSegment &segment)
{
    uint8_t segType = static_cast<uint8_t>(segment.GetSegmentType());
    PageId segmentId = segment.GetSegmentMetaPageId();
    RetStatus ret = segment.InitSegment();
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("Init (%hhu)segment(%hu, %u) fail in DropSegment",
            segType, segmentId.m_fileId, segmentId.m_blockId));
        return DSTORE_FAIL;
    }
    ret = segment.DropSegment();
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("Drop (%hhu)segment(%hu, %u) fail in DropSegment",
            segType, segmentId.m_fileId, segmentId.m_blockId));
        return DSTORE_FAIL;
    }
    ErrLog(DSTORE_LOG, MODULE_SEGMENT, ErrMsg("Finish drop (%hhu)segment(%hu, %u) in pdb %u tablespace %hu",
        segType, segmentId.m_fileId, segmentId.m_blockId, segment.GetPdbId(), segment.GetTablespaceId()));
    return DSTORE_SUCC;
}

RetStatus DropSegment(PdbId pdbId, TablespaceId tablespaceId, SegmentType segmentType, PageId segmentId)
{
    if (tablespaceId == INVALID_TABLESPACE_ID) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("Drop segment with INVALID tablespace ID."));
        return DSTORE_FAIL;
    }
    uint8_t segType = static_cast<uint8_t>(segmentType);
    ErrLog(DSTORE_LOG, MODULE_SEGMENT, ErrMsg("Begin to drop (%hhu)segment(%hu, %u) in pdb %u tablespace %hu",
        segType, segmentId.m_fileId, segmentId.m_blockId, pdbId, tablespaceId));

    TablespaceMgr *tablespaceMgr = g_storageInstance->GetTablespaceMgrByPdbId(pdbId);
    if (STORAGE_VAR_NULL(tablespaceMgr)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to get tablespaceMgr when drop segment, pdbId %u.", pdbId));
        return DSTORE_FAIL;
    }
    TableSpace *tablespace = tablespaceMgr->OpenTablespace(tablespaceId, DSTORE::DSTORE_ACCESS_SHARE_LOCK);
    if (STORAGE_VAR_NULL(tablespace)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to open tablespace %u, the pdbId is %u.", tablespaceId, pdbId));
        return DSTORE_FAIL;
    }
    if (tablespace->GetTbsCtrlItem().used != 1) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
                ErrMsg("Failed to open tablespace %hu due to it is unused when drop segment.", tablespaceId));
        tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_ACCESS_SHARE_LOCK);
        storage_set_error(TBS_ERROR_TABLESPACE_NOT_EXIST);
        return DSTORE_FAIL;
    }
    RetStatus ret = DSTORE_FAIL;
    switch (segmentType) {
        case SegmentType::HEAP_SEGMENT_TYPE: {
            HeapNormalSegment segment(pdbId, segmentId,
                tablespaceId, g_storageInstance->GetBufferMgr(), g_dstoreCurrentMemoryContext);
            ret = DropSegmentInternal(segment);
            break;
        }
        case SegmentType::INDEX_SEGMENT_TYPE: {
            IndexNormalSegment segment(pdbId, segmentId,
                tablespaceId, g_storageInstance->GetBufferMgr(), g_dstoreCurrentMemoryContext);
            ret = DropSegmentInternal(segment);
            break;
        }
        case SegmentType::HEAP_TEMP_SEGMENT_TYPE: {
            HeapTempSegment segment(pdbId, segmentId,
                tablespaceId, thrd->GetTmpLocalBufMgr(), g_dstoreCurrentMemoryContext);
            ret = DropSegmentInternal(segment);
            break;
        }
        case SegmentType::INDEX_TEMP_SEGMENT_TYPE: {
            IndexTempSegment segment(pdbId, segmentId,
                tablespaceId, thrd->GetTmpLocalBufMgr(), g_dstoreCurrentMemoryContext);
            ret = DropSegmentInternal(segment);
            break;
        }
        case SegmentType::UNDO_SEGMENT_TYPE:
        default:
            ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("Invalid segment type(%hhu)!", segType));
            tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_ACCESS_SHARE_LOCK);
            return DSTORE_FAIL;
    }
    tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_ACCESS_SHARE_LOCK);
    return ret;
}

DSTORE::RetStatus AllocTablespaceId(DSTORE::PdbId pdbId, uint64_t tbsMaxSize, DSTORE::TablespaceId *tablespaceId)
{
    if (tbsMaxSize == 0) {
        tbsMaxSize = MAX_FILE_SIZE * MAX_SPACE_FILE_COUNT;
    }

    StoragePdb *pdb = g_storageInstance->GetPdb(pdbId);
    if (unlikely(pdb == nullptr || !pdb->IsInit())) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Pdb %u is not initialized.", pdbId));
        return DSTORE_FAIL;
    }
    
    ControlFile *controlFile = pdb->GetControlFile();
    if (controlFile == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("ControlFile is not exits, the pdbId is %u.", pdbId));
        return DSTORE_FAIL;
    }

    RetStatus ret = controlFile->AllocTbsId(tablespaceId, tbsMaxSize, true);
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
            ErrMsg("Failed to alloc tablespaceId, the PdbId is %u", pdbId));
        return DSTORE_FAIL;
    }

    return DSTORE_SUCC;
}

DSTORE::RetStatus FreeTablespaceId(DSTORE::PdbId pdbId, DSTORE::TablespaceId tablespaceId, uint64_t ddlXid,
                                   bool needWal)
{
    StoragePdb *pdb = g_storageInstance->GetPdb(pdbId);
    if (STORAGE_VAR_NULL(pdb)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("Pdb %u does not exist.", pdbId));
        return DSTORE_FAIL;
    }
    
    ControlFile *controlFile = pdb->GetControlFile();
    if (STORAGE_VAR_NULL(controlFile)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("Control file does not exist, pdbId %u.", pdbId));
        return DSTORE_FAIL;
    }

    /* The tablespace is already locked before it is dropped. See sqlcmd_drop_tablespace. */
    if (STORAGE_FUNC_FAIL(controlFile->FreeTbsId(tablespaceId, static_cast<Xid>(ddlXid), needWal))) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to free the tablespaceId %hu from control file, pdbId %u.",
                      tablespaceId, pdbId));
        return DSTORE_FAIL;
    }

    return DSTORE_SUCC;
}

DSTORE::RetStatus BatchAllocAndAddDataFile(DSTORE::PdbId pdbId, DSTORE::TablespaceId tablespaceId,
    DSTORE::FileId *fileIds, uint64_t *fileIdCnt, bool needWal)
{
    if (fileIds == NULL) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("The param fileIds is null."));
        return DSTORE_FAIL;
    }

    TablespaceMgr *tablespaceMgr = g_storageInstance->GetTablespaceMgrByPdbId(pdbId);
    if (STORAGE_VAR_NULL(tablespaceMgr)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to get tablespaceMgr when BatchAllocAndAddDataFile, pdbId %u.", pdbId));
        return DSTORE_FAIL;
    }
    TableSpace *tablespace = tablespaceMgr->OpenTablespace(tablespaceId, DSTORE::DSTORE_NO_LOCK);
    if (STORAGE_VAR_NULL(tablespace)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to open tablespace %u, the pdbId is %u.", tablespaceId, pdbId));
        return DSTORE_FAIL;
    }
    for (int i = 0; i < EXTENT_TYPE_COUNT; i++) {
        RetStatus ret = tablespace->AllocAndAddDataFile(pdbId, &(fileIds[i]), EXTENT_SIZE_ARRAY[i], needWal);
        if (ret != DSTORE_SUCC) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
                ErrMsg("Alloc and add datafile failed, pdbId(%u)tablespaceId(%u).", tablespaceId, pdbId));
            return DSTORE_FAIL;
        }
        (*fileIdCnt)++;
    }
    return DSTORE_SUCC;
}

DSTORE::RetStatus BatchFreeAndRemoveDataFile(DSTORE::PdbId pdbId, DSTORE::TablespaceId tablespaceId,
    DSTORE::FileId *fileIds, uint64_t fileIdCnt, uint64_t ddlXid)
{
    if (STORAGE_VAR_NULL(fileIds) || fileIdCnt > MAX_SPACE_FILE_COUNT || tablespaceId == INVALID_TABLESPACE_ID ||
        tablespaceId > MAX_TABLESPACE_ID) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to remove datafile due to invalid param, fileIdCnt %lu, tablespaceId %u", fileIdCnt,
                      tablespaceId));
        return DSTORE_FAIL;
    }

    TablespaceMgr *tablespaceMgr = g_storageInstance->GetTablespaceMgrByPdbId(pdbId);
    if (STORAGE_VAR_NULL(tablespaceMgr)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to get tablespaceMgr when BatchFreeAndRemoveDataFile, pdbId %u.", pdbId));
        return DSTORE_FAIL;
    }

    /* The tablespace is already locked before it is dropped. See sqlcmd_drop_tablespace. */
    TableSpace *tablespace = tablespaceMgr->OpenTablespace(tablespaceId, DSTORE_NO_LOCK);
    if (STORAGE_VAR_NULL(tablespace)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to open tablespace %u, the pdbId is %u.", tablespaceId, pdbId));
        return DSTORE_FAIL;
    }
    
    TbsDataFile **datafiles = tablespaceMgr->GetDataFiles();
    if (STORAGE_VAR_NULL(datafiles)) {
        tablespaceMgr->CloseTablespace(tablespace, DSTORE_NO_LOCK);
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to get datafiles from tablespace %u, the pdbId is %u.", tablespaceId, pdbId));
        return DSTORE_FAIL;
    }

    for (uint64_t i = 0; i < fileIdCnt; i++) {
        FileId fileId = fileIds[i];
        if (unlikely(fileId == INVALID_DATA_FILE_ID)) {
            ErrLog(DSTORE_WARNING, MODULE_TABLESPACE,
                   ErrMsg("Skip remove data file, fileId %u, fileIds idx: %lu.", fileId, i));
            continue;
        }

        TbsDataFile *dataFile = datafiles[fileId];
        RetStatus ret =
            tablespace->FreeAndRemoveDataFile(pdbId, tablespaceId, dataFile, static_cast<Xid>(ddlXid), true);
        if (STORAGE_FUNC_FAIL(ret)) {
            tablespaceMgr->CloseTablespace(tablespace, DSTORE_NO_LOCK);
            ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
                   ErrMsg("Free and remove datafile failed, fileId %hu, tablespaceId %u, pdbId %u.", fileId,
                          tablespaceId, pdbId));
            return DSTORE_FAIL;
        }
    }

    tablespaceMgr->CloseTablespace(tablespace, DSTORE_NO_LOCK);
    return DSTORE_SUCC;
}

RetStatus GetFileIdsByTablespaceId(PdbId pdbId, Oid tablespaceId, FileId **fileIds,
                                   uint32_t &fileIdCount)
{
    fileIdCount = 0;
    dlist_head fileIdListHead;

    if (unlikely(pdbId == DSTORE::INVALID_PDB_ID || tablespaceId == DSTORE::INVALID_TABLESPACE_ID)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to get fileIds due to invalid parameter, pdbId %u, tablespaceId %u.", pdbId,
                      tablespaceId));
        return DSTORE_FAIL;
    }

    TablespaceMgr *tablespaceMgr = g_storageInstance->GetTablespaceMgrByPdbId(pdbId);
    if (STORAGE_VAR_NULL(tablespaceMgr)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failedto get tablespaceMgr when GetFileIdsByTablespaceId, pdbId %u.", pdbId));
        return DSTORE_FAIL;
    }

    TableSpace *tablespace =
        tablespaceMgr->OpenTablespace(static_cast<FileId>(tablespaceId), DSTORE::DSTORE_ACCESS_SHARE_LOCK);
    if (STORAGE_VAR_NULL(tablespace)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Tablespace %u does not exist, pdbId %u.", tablespaceId, pdbId));
        return DSTORE_FAIL;
    }

    ControlFile *controlFile = tablespace->GetControlFile();
    if (STORAGE_VAR_NULL(controlFile)) {
        tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_ACCESS_SHARE_LOCK);
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("ControlFile does not exist, pdbId %u.", pdbId));
        return DSTORE_FAIL;
    }

    DListInit(&fileIdListHead);
    if (STORAGE_FUNC_FAIL(controlFile->GetFilesFromTablespace(tablespaceId, &fileIdListHead, &fileIdCount))) {
        controlFile->FreeFileIdList(&fileIdListHead);
        tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_ACCESS_SHARE_LOCK);
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to get files info from the control file, pdbId %u.", pdbId));
        return DSTORE_FAIL;
    }

    if (fileIdCount != 0) {
        *fileIds = static_cast<FileId *>(DstorePalloc0(sizeof(FileId) * fileIdCount));
        if (STORAGE_VAR_NULL(*fileIds)) {
            controlFile->FreeFileIdList(&fileIdListHead);
            tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_ACCESS_SHARE_LOCK);
            ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
                   ErrMsg("Failed to alloc space for tablespace %hu, alloc size: %lu.", tablespaceId,
                          (sizeof(FileId) * fileIdCount)));
            return DSTORE_FAIL;
        }

        dlist_iter iter;
        uint32 i = 0;
        dlist_foreach(iter, &fileIdListHead)
        {
            FileIdNode *fileIdNode = dlist_container(FileIdNode, node, iter.cur);
            (*fileIds)[i++] = fileIdNode->fileId;
        }
    }

    controlFile->FreeFileIdList(&fileIdListHead);
    tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_ACCESS_SHARE_LOCK);
    return DSTORE_SUCC;
}

// get tablespace info. We have got the lock of tablespace mgr and tablespace.
RetStatus DstoreGetTablespaceInfo(const PdbId pdbId, const TablespaceId tablespaceId, DstoreTablespaceAttr* tbsAttr)
{
    if (tablespaceId == INVALID_TABLESPACE_ID || tbsAttr == NULL || pdbId == INVALID_PDB_ID) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
            ErrMsg("Invalid para on DstoreGetTablespaceInfo. pdb [%u] tbs [%u]", pdbId, tablespaceId));
        return DSTORE_FAIL;
    }
    TablespaceMgr *tablespaceMgr = g_storageInstance->GetTablespaceMgrByPdbId(pdbId);
    if (STORAGE_VAR_NULL(tablespaceMgr)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
            ErrMsg("Failed to get tablespaceMgr when get tablespace info, pdbId %u.", pdbId));
        return DSTORE_FAIL;
    }
    TableSpace *tablespace = tablespaceMgr->OpenTablespace(tablespaceId, DSTORE::DSTORE_NO_LOCK);
    if (STORAGE_VAR_NULL(tablespace)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to open tablespace %u, the pdbId is %u.", tablespaceId, pdbId));
        return DSTORE_FAIL;
    }
    ControlTablespacePageItemData ctrlTbsItem = tablespace->GetTbsCtrlItem();
    if (ctrlTbsItem.used != DSTORE_TABLESPACE_IS_USED) {
        storage_set_error(TBS_ERROR_SPACE_NOT_EXIST);
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
            ErrMsg("[DstoreGetTablespaceInfo]TablespaceId %u is not used.", tablespaceId));
        tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_NO_LOCK);
        return DSTORE_FAIL;
    }
    if (ctrlTbsItem.hwm > DSTORE::MAX_SPACE_FILE_COUNT) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
            ErrMsg("File number reached upper limit. tbs %u's hwm is %u.", tablespaceId, tbsAttr->hwm));
        tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_NO_LOCK);
        return DSTORE_FAIL;
    }
    tbsAttr->magic = ctrlTbsItem.magic;
    tbsAttr->version = ctrlTbsItem.version;
    tbsAttr->tablespaceId = ctrlTbsItem.tablespaceId;
    tbsAttr->hwm = ctrlTbsItem.hwm;
    tbsAttr->tbsMaxSize = ctrlTbsItem.tbsMaxSize;
    tbsAttr->reuseVersion = ctrlTbsItem.reuseVersion;
    tbsAttr->ddlXid = ctrlTbsItem.ddlXid.m_placeHolder;
    tbsAttr->used = ctrlTbsItem.used;
    tbsAttr->fileNum = 0;
    for (int i = 0; i < ctrlTbsItem.hwm; i++) {
        if (ctrlTbsItem.fileIds[i] == DSTORE::INVALID_DATA_FILE_ID) {
            continue;
        }
        tbsAttr->fileIds[tbsAttr->fileNum] = ctrlTbsItem.fileIds[i];
        tbsAttr->fileNum++;
    }

    tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_NO_LOCK);
    return DSTORE_SUCC;
}

RetStatus DstoreGetDatafileInfo(const PdbId pdbId, const FileId fileId, DstoreDatafileAttr* fileAttr)
{
    if (fileId == INVALID_DATA_FILE_ID || fileAttr == NULL || pdbId == INVALID_PDB_ID) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
            ErrMsg("Invalid para on DstoreGetDatafileInfo. pdb [%u] file [%u]", pdbId, fileId));
        return DSTORE_FAIL;
    }

    TablespaceMgr *tablespaceMgr = g_storageInstance->GetTablespaceMgrByPdbId(pdbId);
    if (STORAGE_VAR_NULL(tablespaceMgr)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
            ErrMsg("Failed to get tablespaceMgr when get datafile info, pdbId %u.", pdbId));
        return DSTORE_FAIL;
    }

    TbsDataFile *datafile = tablespaceMgr->AcquireDatafile(fileId, LW_SHARED);
    if (STORAGE_VAR_NULL(datafile)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Load datafile failed, fileId is %hu.", fileId));
        return DSTORE_FAIL;
    }

    ControlDataFilePageItemData ctrlPageItemData = datafile->GetDataFilePageItemData();

    fileAttr->fileId = ctrlPageItemData.fileId;
    fileAttr->extentSize = ctrlPageItemData.extentSize;
    fileAttr->used = ctrlPageItemData.used;
    fileAttr->magic = ctrlPageItemData.magic;
    fileAttr->version = ctrlPageItemData.version;
    fileAttr->reuseVersion = ctrlPageItemData.reuseVersion;
    fileAttr->ddlXid = ctrlPageItemData.ddlXid.m_placeHolder;
    fileAttr->maxSize = ctrlPageItemData.fileMaxSize;
    fileAttr->tablespaceId = ctrlPageItemData.tablespaceId;
    int rc = sprintf_s(fileAttr->fileName, DSTORE_FILE_NAME_LEN, "%hu", fileAttr->fileId);
    storage_securec_check_ss(rc);

    tablespaceMgr->ReleaseDatafileLock(datafile);
    return DSTORE_SUCC;
}

RetStatus DstoreGetTablespaceSize(const PdbId pdbId, const TablespaceId tablespaceId, uint64_t& size)
{
    if (tablespaceId == INVALID_TABLESPACE_ID || pdbId == INVALID_PDB_ID) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
            ErrMsg("Invalid para on DstoreGetTablespaceSize. pdb [%u] tbs [%u]", pdbId, tablespaceId));
        return DSTORE_FAIL;
    }
    TablespaceMgr *tablespaceMgr = g_storageInstance->GetTablespaceMgrByPdbId(pdbId);
    if (STORAGE_VAR_NULL(tablespaceMgr)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to get tablespaceMgr when get tablespace size, pdbId %u, tablespaceId %hu.", pdbId,
                      tablespaceId));
        return DSTORE_FAIL;
    }
    return tablespaceMgr->GetTablespaceSize(tablespaceId, size);
}

DSTORE::RetStatus AlterMaxSize(DSTORE::TablespaceId tablespaceId, uint64_t maxSize, PdbId pdbId)
{
    TablespaceMgr *tablespaceMgr = g_storageInstance->GetTablespaceMgrByPdbId(pdbId);
    if (STORAGE_VAR_NULL(tablespaceMgr)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to get tablespaceMgr when alter tablespace maxsize, pdbId %u, tablespaceId %hu.", pdbId,
                      tablespaceId));
        return DSTORE_FAIL;
    }
    if (maxSize == 0) {
        maxSize = MAX_TABLESPACE_SIZE;
    }
    return tablespaceMgr->AlterMaxSize(tablespaceId, maxSize);
}

uint8_t IsRemainTablespace(PdbId pdbId, TablespaceId tablespaceId, bool isExistSysTableRecord)
{
    StoragePdb *pdb = g_storageInstance->GetPdb(pdbId);
    if (STORAGE_VAR_NULL(pdb)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("Failed to get pdb when checking remain tablespace."));
        return INVALID_REMAIN_TABLESPACE;
    }
    /* check is standy pdb or not. */
    if (pdb->GetPdbRoleMode() != PdbRoleMode::PDB_PRIMARY) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("Only the primary PDB can check remain tablespace."));
        return INVALID_REMAIN_TABLESPACE;
    }

    /* Check whether records in the system table exist. */
    if (isExistSysTableRecord) {
        return NOT_REMAIN_TABLESPACE;
    }

    TablespaceMgr *tablespaceMgr = pdb->GetTablespaceMgr();
    if (STORAGE_VAR_NULL(tablespaceMgr)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("TablespaceMgr does not exist, the pdbId is %u.", pdbId));
        return INVALID_REMAIN_TABLESPACE;
    }

    TableSpace *tablespace =
        tablespaceMgr->OpenTablespace(static_cast<FileId>(tablespaceId), DSTORE::DSTORE_NO_LOCK);
    if (STORAGE_VAR_NULL(tablespace)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Tablespace(%u) does not exist, the pdbId is %u.", tablespaceId, pdbId));
        return INVALID_REMAIN_TABLESPACE;
    }

    ControlTablespacePageItemData tbsItem = tablespace->GetTbsCtrlItem();
    if (tbsItem.used != DSTORE_TABLESPACE_IS_USED && tbsItem.used != DSTORE_TABLESPACE_NOT_USED) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("The used field in the tablespace metadata is abnormal."));
        tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_NO_LOCK);
        return INVALID_REMAIN_TABLESPACE;
    }

    /* On the tablespace metadata page, check whether the value of the used field is 1,
     indicating that the tablespace is in use. */
    if (tbsItem.used == DSTORE_TABLESPACE_IS_USED) {
        /* If the tablespace is in use. */
        tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_NO_LOCK);
        return REMAIN_TABLESPACE;
    }

    /* If the tablespace is not in use. */
    tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_NO_LOCK);
    return NOT_REMAIN_TABLESPACE;
}

void FreeRemainTablespace(PdbId pdbId, TablespaceId tablespaceId, bool isExistSysTableRecord, uint64_t ddlXid)
{
    StoragePdb *pdb = g_storageInstance->GetPdb(pdbId);
    if (STORAGE_VAR_NULL(pdb)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("Failed to get pdb when free remain tablespace."));
        return;
    }
    /* check is standy pdb or not. */
    if (pdb->GetPdbRoleMode() != PdbRoleMode::PDB_PRIMARY) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("Only the primary PDB can free remain tablespace."));
        return;
    }

    /* Check whether records in the system table exist. */
    if (isExistSysTableRecord) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
            ErrMsg("There is a record about this tablespace %hu in the system table,"
                "so it can not be deleted.", tablespaceId));
        return;
    }

    TablespaceMgr *tablespaceMgr = pdb->GetTablespaceMgr();
    if (STORAGE_VAR_NULL(tablespaceMgr)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("TablespaceMgr does not exist, the pdbId is %u.", pdbId));
        return;
    }

    /* Reads the control object content and parses the ControlTablespacePageItemData structure. */
    TableSpace *tablespace =
        tablespaceMgr->OpenTablespace(static_cast<FileId>(tablespaceId), DSTORE::DSTORE_NO_LOCK);
    if (STORAGE_VAR_NULL(tablespace)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Tablespace(%u) does not exist, the pdbId is %u.", tablespaceId, pdbId));
        return;
    }

    ControlTablespacePageItemData tbsItem = tablespace->GetTbsCtrlItem();
    if (tbsItem.used != DSTORE_TABLESPACE_IS_USED && tbsItem.used != DSTORE_TABLESPACE_NOT_USED) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("The used field in the tablespace metadata is abnormal."));
        tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_NO_LOCK);
        return;
    }

    /* On the tablespace metadata page, check whether the value of the used field is 1,
     indicating that the tablespace is in use. */
    if (tbsItem.used == DSTORE_TABLESPACE_IS_USED) {
        FileId tempFileIds[MAX_TBS_DATAFILE_SLOT_CNT];
        for (uint32_t i = 0; i < MAX_TBS_DATAFILE_SLOT_CNT; ++i) {
            tempFileIds[i] = tbsItem.fileIds[i];
        }
        /* If the tablespace is in use, that is remain tablespace, need to be freed. */
        RetStatus rc = BatchFreeAndRemoveDataFile(pdbId, tablespaceId, tempFileIds, tbsItem.hwm, ddlXid);
        if (STORAGE_FUNC_FAIL(rc)) {
            ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("Failed to delete data files belong to tablespace %hu.",
            tablespaceId));
            tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_NO_LOCK);
            return;
        } else {
            ErrLog(DSTORE_LOG, MODULE_TABLESPACE, ErrMsg("Succeed to delete data files belong to tablespace %hu.",
            tablespaceId));
        }

        RetStatus ret = FreeTablespaceId(pdbId, tablespaceId, ddlXid, true);
        if (STORAGE_FUNC_FAIL(ret)) {
            ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("Failed to free tablespaceId %hu.", tablespaceId));
            tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_NO_LOCK);
            return;
        } else {
            ErrLog(DSTORE_LOG, MODULE_TABLESPACE, ErrMsg("Succeed to free tablespaceId %hu.", tablespaceId));
        }
    }

    /* If the tablespace is not in use, return directly. */
    tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_NO_LOCK);
    return;
}

bool is_contain_fileId(const FileId* fileid, uint16_t size, FileId targetFileId)
{
    for (size_t i = 0; i < size; ++i) {
        if (fileid[i] == targetFileId) {
            return true;
        }
    }
    return false;
}

uint8_t IsRemainDatafile(PdbId pdbId, FileId fileId)
{
    StoragePdb *pdb = g_storageInstance->GetPdb(pdbId);
    if (STORAGE_VAR_NULL(pdb)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to get pdb when check remain datafile, pdbId %u, fileId %hu.", pdbId, fileId));
        return INVALID_REMAIN_DATAFILE;
    }
    /* check is standy pdb or not. */
    if (pdb->GetPdbRoleMode() != PdbRoleMode::PDB_PRIMARY) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("Only the primary PDB can check remain datafile."));
        return INVALID_REMAIN_DATAFILE;
    }

    /* Obtain the S lock of the control file. */
    ControlFile* controlFile = pdb->GetControlFile();
    if (STORAGE_VAR_NULL(controlFile)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to get controlFile when check remain datafile, pdbId %u, fileId %hu.", pdbId, fileId));
        return INVALID_REMAIN_DATAFILE;
    }

    /* Invoke ControlFile::GetDataFilePageItemData to check whether the corresponding data file tuple can be found. */
    ControlDataFilePageItemData datafileItem;
    if (STORAGE_FUNC_FAIL(controlFile->GetDataFilePageItemData(fileId, &datafileItem))) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("Get datafile page item failed."));
        return INVALID_REMAIN_DATAFILE;
    }

    /* Check whether the used field is valid. */
    if (unlikely(datafileItem.used != DSTORE_DATAFILE_IS_USED && datafileItem.used != DSTORE_DATAFILE_NOT_USED)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("The used field in the datafile metadata is abnormal."));
        return INVALID_REMAIN_DATAFILE;
    }

    /* On the datafile metadata page, check whether the value of the used field is 1,
     indicating that the tablespace is in use. */
    if (datafileItem.used == DSTORE_DATAFILE_IS_USED) {
        /* Query the corresponding tablespace information and check whether the data file is referenced. */
        ControlTablespacePageItemData tbsItem;
        if (STORAGE_FUNC_FAIL(controlFile->GetTbsPageItemData(datafileItem.tablespaceId, &tbsItem))) {
            ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("Get tbs page item failed."));
            return INVALID_REMAIN_DATAFILE;
        }

        /* Check whether the file ID is referenced by a tablespace. */
        FileId tempFileIds[MAX_TBS_DATAFILE_SLOT_CNT];
        for (uint32_t i = 0; i < MAX_TBS_DATAFILE_SLOT_CNT; ++i) {
            tempFileIds[i] = tbsItem.fileIds[i];
        }
        if (is_contain_fileId(tempFileIds, MAX_TBS_DATAFILE_SLOT_CNT, fileId)) {
            /* It indicates that the logic exists and is not residual. */
            return NOT_REMAIN_DATAFILE;
        }

        /* It indicates that the logic does not exist, that is, the residual */
        return REMAIN_DATAFILE;
    }

    /* If the tablespace corresponding to the data file does not reference
    the data file, check whether the file exists physically. */
    VFSAdapter *vfs = pdb->GetVFS();
    if (STORAGE_VAR_NULL(vfs)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to get vfs when check remain datafile, pdbId %u, fileId %hu.", pdbId, fileId));
        return INVALID_REMAIN_DATAFILE;
    }

    char file_name[MAXPGPATH];
    if (STORAGE_FUNC_FAIL(vfs->GetFileNameFromFileId(fileId, file_name))) {
        return INVALID_REMAIN_DATAFILE;
    }
    
    if (vfs->FileExists(file_name)) {
        /* It indicates that the logic does not exist, but pyhical exists, that is the residual. */
        return REMAIN_DATAFILE;
    }
    
    /* It indicates that the pyhical not exists, that is not the residual. */
    return NOT_REMAIN_DATAFILE;
}

void FreeRemainDatafile(PdbId pdbId, FileId fileId)
{
    StoragePdb *pdb = g_storageInstance->GetPdb(pdbId);
    if (STORAGE_VAR_NULL(pdb)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to get pdb when free remain datafile, pdbId %u, fileId %hu.", pdbId, fileId));
        return;
    }
    /* check is standy pdb or not. */
    if (pdb->GetPdbRoleMode() != PdbRoleMode::PDB_PRIMARY) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("Only the primary PDB can check remain datafile."));
        return;
    }

    ControlFile* controlFile = pdb->GetControlFile();
    if (STORAGE_VAR_NULL(controlFile)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to get controlFile when free remain datafile, pdbId %u, fileId %hu.", pdbId, fileId));
        return;
    }

    /* Invoke ControlFile::GetDataFilePageItemData to check whether the corresponding data file tuple can be found. */
    ControlDataFilePageItemData datafileItem;
    if (STORAGE_FUNC_FAIL(controlFile->GetDataFilePageItemData(fileId, &datafileItem))) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("Get datafile page item failed."));
        return;
    }

    /* Check whether the used field is valid. */
    if (unlikely(datafileItem.used != DSTORE_DATAFILE_IS_USED && datafileItem.used != DSTORE_DATAFILE_NOT_USED)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("The used field in the datafile metadata is abnormal."));
        return;
    }

    TablespaceMgr *tablespaceMgr = pdb->GetTablespaceMgr();
    if (STORAGE_VAR_NULL(tablespaceMgr)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to get tablespaceMgr when free remain datafile, pdbId %u, fileId %hu.", pdbId, fileId));
        return;
    }

    /* On the datafile metadata page, check whether the value of the used field is 1,
    indicating that the tablespace is in use. */
    if (datafileItem.used == DSTORE_DATAFILE_IS_USED) {
        /* Traverse all tablespace metadata and find all referenced file_ids. */
        uint32 fileCount = 0;
        dlist_head fileIdListHead;
        DListInit(&fileIdListHead);
        if (STORAGE_FUNC_FAIL(controlFile->GetFilesFromAllTablespace(&fileIdListHead, &fileCount))) {
            ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("Fail to get all the datafile meta information."));
            return;
        }

        if (DListIsEmpty(&fileIdListHead)) {
            ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("No file exists in all tablespaces."));
            return;
        }

        /* Get Files From source pdb */
        dlist_iter iter;
        bool isContainFieldFromAllTablespace = false;
        dlist_foreach(iter, &fileIdListHead) {
            FileIdNode *fileIdNode = dlist_container(FileIdNode, node, iter.cur);
            if (fileId == fileIdNode->fileId) {
                isContainFieldFromAllTablespace = true;
            }
        }
        controlFile->FreeFileIdList(&fileIdListHead);

        /* If the data file ID is referenced by any tablespace, the data file cannot be deleted. */
        if (isContainFieldFromAllTablespace) {
            ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("This datafile has been referenced, fileId %hu.", fileId));
            return;
        }

        /* Delete the metadata of the data file. */
        Xid ddlXid = (thrd == nullptr) ? INVALID_XID : thrd->GetCurrentXid();
        if (STORAGE_FUNC_FAIL(tablespaceMgr->RemoveFileByFileId(fileId, ddlXid))) {
            ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("Fail to remove the datafile, fileId %hu.", fileId));
            return;
        }
        ErrLog(DSTORE_LOG, MODULE_TABLESPACE, ErrMsg("Succeed to remove the datafile, fileId %hu.", fileId));
        return;
    }

    /* If the tablespace corresponding to the data file does not reference
    the data file, check whether the file exists physically. */
    VFSAdapter *vfs = pdb->GetVFS();
    StorageAssert(vfs != nullptr);
    char file_name[MAXPGPATH];
    if (STORAGE_FUNC_FAIL(vfs->GetFileNameFromFileId(fileId, file_name))) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("Fail to translate fildId %hu to fileName.", fileId));
        return;
    }

    if (vfs->FileExists(file_name)) {
        /* It indicates that the logic does not exist, but pyhical exists, that is the residual. */
        if (STORAGE_FUNC_FAIL(vfs->RemoveFile(fileId, file_name))) {
            ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("Fail to remove file named %s.", file_name));
            return;
        }
        ErrLog(DSTORE_LOG, MODULE_TABLESPACE, ErrMsg("Fail to remove file named %s.", file_name));
        return;
    }

    /* It indicates that the pyhical not exists, that is not the residual. */
    return;
}

}  // namespace TableSpace_Interface

