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
 * dstore_control_tablespace.h
 *  Record tablespace and file of the tablespace in the control file..
 *
 *
 * IDENTIFICATION
 *        dstore/include/control/dstore_control_tablespace.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_CONTROL_TABLESPACE_H
#define DSTORE_CONTROL_TABLESPACE_H
#include "dstore_control_struct.h"
#include "common/algorithm/dstore_string_info.h"
#include "control/dstore_control_file_mgr.h"
#include "common/dstore_common_utils.h"
#include "control/dstore_control_group.h"
#include "tablespace/dstore_tablespace_utils_internal.h"
namespace DSTORE {
struct FileIdNode {
    dlist_node node;
    FileId fileId;
};
/* DataFilePage = ControlPageHeader + ControlDataFilePageItemData */
class ControlDataFilePageItemData : public BaseObject {
public:
    uint32 magic;
    uint32 version;       /* struct version */
    FileId fileId;        /* fileId of the file */
    ExtentSize extentSize;    /* size of a extent in the file */
    uint8 used : 1;       /* indicates whether fileId is used */
    uint8 padding : 7;
    uint64 reuseVersion; /* indicates the reused version of the fileId */
    Xid ddlXid;          /* the xid of the transaction that created the file */
    uint64 fileMaxSize;  /* max size of the file */
    TablespaceId tablespaceId;
    uint8 reserved[72];

    void InitItem(FileId fId);

    void SetItem(TablespaceId tbsId, ExtentSize extSize, uint64 fileMaxSize);

    void ResetItem(Xid xid);

    static void Dump(void* item, StringInfoData& dumpInfo)
    {
        ControlDataFilePageItemData *data = static_cast<ControlDataFilePageItemData *>(item);
        dumpInfo.append("magic 0x%x, version %u, fileId %d, extentSize %u, used %u, reuseVersion %lu, "
            "ddlXid %lu, fileMaxSize %lu, tablespaceId %u.\n",
            data->magic, data->version, data->fileId, data->extentSize, data->used, data->reuseVersion,
            data->ddlXid.m_placeHolder, data->fileMaxSize, data->tablespaceId);
    }
} PACKED;
static_assert(sizeof(ControlDataFilePageItemData) == 111, "tbs page item must be packed");

/* TablespacePage = ControlPageHeader + ControlTablespacePageItemData */
constexpr uint64 MAX_TBS_DATAFILE_SLOT_CNT = 1024;
class ControlTablespacePageItemData : public BaseObject {
public:
    uint32 magic;
    uint32 version;            /* struct version */
    TablespaceId tablespaceId;
    uint16 hwm;                /* indicates the maximum number of files that has been reached */
    FileId fileIds[MAX_TBS_DATAFILE_SLOT_CNT];
    uint64 tbsMaxSize;         /* maximum size of a tablespace */
    uint64 reuseVersion;       /* indicates the reused version of the fileId */
    Xid ddlXid;                /* the xid of the transaction that created the tablespace */
    uint8 used : 1;            /* indicates whether fileId is used */
    uint8 padding : 7;
    uint8 reserved[563];

    void InitItem(TablespaceId tbsId);

    void SetItem(uint64 maxSize);

    void ResetItem(Xid xid);

    RetStatus AssociateFile(FileId fId, uint16 *slotId);
    RetStatus DisassociateFile(FileId fId, uint16 *slotId);
    void AlterItemMaxSize(uint64 maxSize);

    static void Dump(void* item, StringInfoData& dumpInfo)
    {
        ControlTablespacePageItemData *data = static_cast<ControlTablespacePageItemData *>(item);
        dumpInfo.append("magic 0x%x, version %u, tablespaceId %u, hwm %u, tbsMaxSize %lu, reuseVersion %lu, "
            "ddlXid %lu, used %u.\n",
            data->magic, data->version, data->tablespaceId, data->hwm, data->tbsMaxSize, data->reuseVersion,
            data->ddlXid.m_placeHolder, data->used);
        for (uint16 i = 0; i < data->hwm && i < MAX_TBS_DATAFILE_SLOT_CNT; i++) {
            dumpInfo.append("FileIds[%d]: %u\n", i, data->fileIds[i]);
        }
    }
} PACKED;
static_assert(sizeof(ControlTablespacePageItemData) == 2648, "tbs page item must be packed");
inline constexpr uint16 my_ceil(float value)
{
    return static_cast<uint16>(value) + (value > static_cast<float>(static_cast<uint16>(value)) ? 1 : 0);
}
constexpr uint32 MAX_TABLESPACE_ITEM_CNT = MAX_TABLESPACE_ID + 1; /* No.0 is reserved */
constexpr uint32 MAX_DATAFILE_ITEM_CNT = MAX_VFS_FILE_ID + 1; /* No.0 is reserved */

constexpr uint16 MAX_TABLESPACE_ITEM_CNT_PER_PAGE =
    ((BLCKSZ - sizeof(ControlPageHeader)) / sizeof(ControlTablespacePageItemData));
constexpr uint16 MAX_DATAFILE_ITEM_CNT_PER_PAGE =
    ((BLCKSZ - sizeof(ControlPageHeader)) / sizeof(ControlDataFilePageItemData));

constexpr uint16 MAX_TABLESPACE_PAGE_CNT =
    my_ceil(static_cast<float>(MAX_TABLESPACE_ITEM_CNT) / MAX_TABLESPACE_ITEM_CNT_PER_PAGE);
constexpr uint16 MAX_DATAFILE_PAGE_CNT =
    my_ceil(static_cast<float>(MAX_DATAFILE_ITEM_CNT) / MAX_DATAFILE_ITEM_CNT_PER_PAGE);
static_assert(MAX_TABLESPACE_PAGE_CNT == 342, "The number of TBS pages must be 342.");
static_assert(MAX_DATAFILE_PAGE_CNT == 898, "The number of datafile pages must be 898.");

constexpr uint16 MAX_RESERVED_PAGE_CNT = MAX_TABLESPACE_PAGE_CNT + MAX_DATAFILE_PAGE_CNT;

constexpr uint16 DEFAULT_TABLESPACE_PAGE = CONTROLFILE_PAGEMAP_TABLESPACE_START;
constexpr uint16 DEFAULT_DATAFILE_PAGE = DEFAULT_TABLESPACE_PAGE + MAX_TABLESPACE_PAGE_CNT;
const uint32 CF_TABLESPACE_ITEM_MAGIC_NUMBER = 0xE1E2E3E4;
const uint32 CF_DATAFILE_ITEM_MAGIC_NUMBER = 0xF1F2F3F4;
class ControlTablespace : public ControlGroup {
public:
    ControlTablespace(ControlFileMgr *controlFileMgr, DstoreMemoryContext memCtx, PdbId pdbId)
        : ControlGroup(controlFileMgr, memCtx, CONTROL_GROUP_TYPE_TABLESPACE, CONTROLFILE_PAGEMAP_TABLESPACE_META,
                       pdbId)
    {}
    ~ControlTablespace() {}
    DISALLOW_COPY_AND_MOVE(ControlTablespace);
    RetStatus Init(DeployType deployType)
    {
        if (unlikely(ControlGroup::Init(deployType) != DSTORE_SUCC)) {
            return DSTORE_FAIL;
        }
        m_isInitialized = true;
        return DSTORE_SUCC;
    }
    RetStatus Create();
    void Reload()
    {}
    RetStatus InvalidateFileId(FileId fileId);
    void GetDataFilePageItemCtid(FileId fileId, BlockNumber *blkno, OffsetNumber *offset);
    void GetTbsPageItemCtid(TablespaceId tablespaceId, BlockNumber *blkno, OffsetNumber *offset);
    RetStatus AllocAndCreateDataFile(FileId *fileId, TablespaceId tablespaceId, ExtentSize extentSize,
        bool needWal);
    RetStatus FreeDataFileId(FileId fileId, TablespaceId tablespaceId, uint16 slotId, Xid ddlXid, bool needWal);
    RetStatus InitTbsId(TablespaceId tablespaceId, uint64 tbsMaxSize);
    RetStatus AllocTbsId(TablespaceId *tablespaceId, uint64 tbsMaxSize, bool needWal);
    RetStatus FreeTbsId(TablespaceId tablespaceId, Xid ddlXid, bool needWal);
#ifdef UT
    ControlDataFilePageItemData *GetDataFilePageItemPtr(FileId fileId, BlockNumber *blkno);
    ControlTablespacePageItemData *GetTbsPageItemPtr(TablespaceId tablespaceId, BlockNumber *blkno);
#endif
    RetStatus AddFileIdToTbs(TablespaceId tablespaceId, FileId fileId, uint16 *slotId, bool needWal);
    RetStatus AlterTbsMaxSize(TablespaceId tablespaceId, uint64 tbsMaxSize, bool needWal);
    RetStatus FreeFileIdFromTbs(TablespaceId tablespaceId, FileId fileId, uint16 *slotId);
    RetStatus GetFilesFromTablespace(TablespaceId tablespaceId, dlist_head *fileIdList, uint32 *fileCount);
    RetStatus GetFilesFromAllTablespace(dlist_head *fileIdList, uint32 *fileCount);
    void FreeFileIdList(dlist_head *fileIdListHead);
    RetStatus GetDataFilePageItemData(FileId fileId, ControlDataFilePageItemData *filePageItem);
    RetStatus GetALLDataFilePageItemDatasForTemp(ControlDataFilePageItemData *filePageItems, uint32 count);
    RetStatus GetTbsPageItemData(TablespaceId tablespaceId, ControlTablespacePageItemData *tbsItem);
    RetStatus UpdateCreateTablespace(TablespaceId tablespaceId, Xid ddlXid, uint64 tbsMaxSize, uint64 preReuseVersion);
    RetStatus UpdateCreateDataFile(TablespaceId tablespaceId, FileId fileId, uint64 fileMaxSize, ExtentSize extentSize,
        uint64 preReuseVersion, Xid ddlXid);
    RetStatus UpdateAddFileToTbs(TablespaceId tablespaceId, FileId fileId, uint16 slotId, uint16 hwm,
        uint64 preReuseVersion, Xid ddlXid);
    RetStatus UpdateDropTablespace(TablespaceId tablespaceId, uint64 preReuseVersion, Xid ddlXid, uint16 hwm);
    RetStatus UpdateDropDataFile(TablespaceId tablespaceId, FileId fileId, uint64 preReuseVersion,
        uint16 slotId, uint16 hwm);
    RetStatus UpdateAlterTablespace(TablespaceId tablespaceId, uint64 tbsMaxSize, Xid ddlXid, uint64 preReuseVersion);

private:
    RetStatus InitTbsItem(TablespaceId tablespace);
    RetStatus InitDatafileItem(FileId fileId);
    ControlTablespacePageItemData *GetTbsPageItemPtrInternal(TablespaceId tablespaceId, BlockNumber *blkno);
    ControlDataFilePageItemData *GetDataFilePageItemPtrInternal(FileId fileId, BlockNumber *blkno);
    RetStatus AddFileIdsToList(FileId *fileIds, uint16 size, dlist_head *fileIdList, uint32 *fileCount);
    ControlDataFilePageItemData *GetDataFilePageItemPtrNoCheck(FileId fileId, BlockNumber *blkno);
    ControlTablespacePageItemData *GetTbsPageItemPtrNoCheck(TablespaceId tablespaceId, BlockNumber *blkno);
    RetStatus GetFilesFromTablespaceInternal(TablespaceId tablespaceId, dlist_head *fileIdList, uint32 *fileCount);
    RetStatus InitTbsOrDatafileItemPages(ControlPageType pageType);
    RetStatus OperateTbsItemFileId(TablespaceId tablespaceId, FileId fileId, bool isAssociate,
        uint16 *slotId, bool needWal);
    FileId GetOneAvailableFileId(bool isTmpTbs);
};
} // namespace DSTORE
#endif // DSTORE_CONTROL_TABLESPACE_H