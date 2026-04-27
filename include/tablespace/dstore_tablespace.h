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
 * dstore_tablespace.h
 *
 *
 *
 * IDENTIFICATION
 *        include/tablespace/dstore_tablespace.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_TABLESPACE_H
#define DSTORE_TABLESPACE_H

#include "dstore_tablespace_internal.h"
#include "dstore_tbs_temp_bitmappage_hashtable.h"
#include "port/posix_rwlock.h"

namespace DSTORE {

/* system tablespaceId */
enum class TBS_ID : TablespaceId {
    INVALID_TABLE_SPACE_ID = 0,
    GLOBAL_TABLE_SPACE_ID = 1,
    DEFAULT_TABLE_SPACE_ID = 2,
    CATALOG_TABLE_SPACE_ID = 3,
    UNDO_TABLE_SPACE_ID = 4,
    TEMP_TABLE_SPACE_ID = 5,
    CATALOG_AUX_TABLE_SPACE_ID = 6,
    UNLOGGED_TABLE_SPACE_ID = 7
};

struct FileSize {
    uint64 initialFileSize;
    uint64 maxFileSize;
};

class TbsAllocExtentContext : public BaseObject {
public:
    TbsAllocExtentContext();
    ~TbsAllocExtentContext();
    void AddFileId(FileId fileid);
    RetStatus AllocExtentFromTo(PageId *newExtentPageId, bool *isReUseFlag, uint16 fromIndex, uint16 toIndex,
        int16 *allocedIndex, bool *continueTryAlloc = nullptr);
    RetStatus AllocExtent(PageId *newExtentPageId, bool *isReUseFlag, bool *continueTryAlloc = nullptr);
    ExtentSize GetExtentSize();
    void SetExtentSize(ExtentSize extentSize);
    uint16 GetHwm();
    inline void SetPdbId(PdbId pdbId)
    {
        m_pdbId = pdbId;
    }
    inline PdbId GetPdbId()
    {
        return m_pdbId;
    }

private:
    PdbId m_pdbId;
    uint16 m_hwm;
    int16 m_lastAllocedIndex;
    ExtentSize m_extentSize;
    FileId m_files[MAX_SPACE_FILE_COUNT] = {0};
};

class TableSpaceInterface : public BaseObject {
public:
    virtual ~TableSpaceInterface() = default;

    virtual RetStatus FreeExtent(ExtentSize extentSize, const PageId &extentPageId) = 0;

    virtual RetStatus AllocExtent(ExtentSize extentSize, PageId *newExtentPageId, bool *isReuseFlag,
                                  bool *continueTryAlloc = nullptr) = 0;

    virtual TablespaceId GetTablespaceId() const = 0;
};

// Obtains available m_extents from the bitmap and sends them to the segment.
class TableSpace : public TableSpaceInterface {
public:
    TableSpace(ControlFile *controlFile, TablespaceId tablespaceId);

    ~TableSpace() override;

    DISALLOW_COPY_AND_MOVE(TableSpace);

    /**
     * Used to allocate an extent of a specified size from a tablespace.
     * @param extentSize
     * @param newExtentPageId out parameter, PageId of the first Page of extent
     * @return
     */
    RetStatus AllocExtent(ExtentSize extentSize, PageId *newExtentPageId, bool *isReUseFlag,
                          bool *continueTryAlloc = nullptr) final;
    /**
     * Releases an extent of a specified size from a tablespace.
     * @param extentSize
     * @param extentPageId PageId of the first Page of extent
     * @return
     */
    RetStatus FreeExtent(ExtentSize extentSize, const PageId& extentPageId) final;

#ifdef UT
    /**
     *  Used to add 1 file to the tablespace
     * @param fileId
     * @param fileName
     * @param initialFileSize Specifies the initial size of the file. This parameter is optional.
     * @return
     */
    RetStatus AddFile(FileId fileId, const FileSize &fileSize, const char *storeSpaceName,
                      ExtentSize extentSize);
#endif

    BufMgrInterface *GetBufferMgr();

    // the following interface is used for unit tests
#ifdef UT
    inline void SetTableSpaceId(TablespaceId tablespaceId)
    {
        m_tablespaceId = tablespaceId;
    }

    void TableSpaceInitlock();
    void TableSpaceDestroyLock();
#endif
    uint32 GetFileCount() const;

    void IncreFileCount(uint16 step = 1);

    void SetTablespaceStatus(TbsStatus status);

    FreeBitsSearchPos *GetFreeBitsSearchPos(uint32 fileIndex, uint32 bitmapPageNo);

    TablespaceId GetTablespaceId() const
    {
        return m_tablespaceId;
    }

    inline bool IsValid() const
    {
        return m_isValid.load();
    }

    inline void SetValid(bool isValid)
    {
        m_isValid = isValid;
    }

    inline ControlTablespacePageItemData GetTbsCtrlItem()
    {
        return m_ctrlTablespaceItem;
    }
    /**
     * Used to remove temprory files from the temporary tablespace(id 5).
     */
    void DeleteTempFile(VFSAdapter *vfs, char *fileName, FileId fId);
    RetStatus RemoveTempFilesWithAssociated(TbsDataFile *tbsDataFile, const NodeId selfNodeId,
                                            NodeId curNode, const NodeId *nodeIdList, uint32 nodeCount, bool isStartup);
    RetStatus RemoveAllTempFiles(TbsDataFile **dataFiles, const NodeId selfNodeId,
                                 const NodeId *nodeIdList, uint32 nodeCount, bool isStartup);
    RetStatus AlterMaxSize(uint64 maxSize);
    bool IsTempTbs() const;

    RetStatus AllocAndAddDataFile(PdbId pdbId, FileId *fileId, ExtentSize extentSize, bool needWal);
    RetStatus FreeAndRemoveDataFile(PdbId pdbId, TablespaceId tablespaceId, TbsDataFile *dataFile, Xid ddlXid,
                                    bool needWal);
    void AddFileId(FileId fileid, ExtentSize extentSize);

    RetStatus CreateSysTablespace(PdbId pdbId, TablespaceId tablespaceId);
    FileId *GetFileId();
    ControlFile *GetControlFile();
    ControlTablespacePageItemData GetTbsPageItemData();
    void SetTbsPageItemData(ControlTablespacePageItemData &tbsPageItemData);
    TbsAllocExtentContext GetExtentContextBySize(ExtentSize extentSize);
private:
    PdbId m_pdbId;
    ControlFile *m_controlFile;
    BufMgrInterface *m_bufferMgr;
    TablespaceId m_tablespaceId;
    char m_tablespaceName[MAXTABLESPACENAME];
    TbsDataFile *m_files[MAX_SPACE_FILE_COUNT];
    uint16 m_fileCountByType[EXTENT_TYPE_COUNT];
    uint16 m_fileCount;
    uint16 m_initedFileCount;
    TbsStatus m_status = TbsStatus::TBS_INVALID;
    ControlTablespacePageItemData m_ctrlTablespaceItem;
    TbsAllocExtentContext m_allocExtents[EXTENT_TYPE_COUNT];
    std::atomic<bool> m_isValid;
    RWLock m_tbsCacheRWlock;
};

class TablespaceMgr : public BaseObject {
public:
    TablespaceMgr(PdbId pdbId, ControlFile *controlFile, VFSAdapter *vfs);
    ~TablespaceMgr();
    RetStatus InitTempTbsBitmapPageTable();
    TbsTempBitmapPageHashTable *GetTempTbsBitmapPageTable();
    void Destroy();
    void Destroy(TablespaceId tablespaceId);

    RetStatus LockTablespace(TablespaceId tablespaceId, LockMode lockMode, bool dontWait = false);
    void UnlockTablespace(TablespaceId tablespaceId, LockMode lockMode);

    TableSpace *OpenTablespace(TablespaceId tablespaceId, LockMode lock_mode, bool dontWait = false);
    void CloseTablespace(TableSpace *tablespace, LockMode lock_mode);

    RetStatus SendInvalidTablespace(TablespaceId tablespaceId, FileId fileId);
    RetStatus HandleInvalidTablespace(TablespaceId tablespaceId, FileId fileId);

    RetStatus Load(TableSpace *tablespace);
    TbsDataFile* NewDatafile(ControlDataFilePageItemData fileItem);
    RetStatus LoadDataFiles(TableSpace *tablespace, ControlTablespacePageItemData *files);
    void UnLoadDataFiles(TablespaceId tablespaceId);

    RetStatus RemoveAllFiles();
    RetStatus GetTablespaceSize(const TablespaceId tablespaceId, uint64& size);
    TbsDataFile **GetDataFiles();
    PdbId GetPdbId();
    RetStatus AlterMaxSize(TablespaceId tablespaceId, uint64 maxSize);
    RetStatus RemoveFileByFileId(FileId fileId, Xid ddlXid);
   
    void ReleaseDatafileLock(TbsDataFile* datafile);
    TbsDataFile* AcquireDatafile(FileId fileId, LWLockMode mode);

    RetStatus GetFileVersion(FileId fileId, uint64 *reuseVersion);

    RetStatus CreateDatafile(ControlDataFilePageItemData fileItem);
    RetStatus InitDatafile(FileId fileId);
private:
    TbsDataFile *m_datafiles[MAX_DATAFILE_ITEM_CNT];
    TableSpace *m_tablespaces[MAX_TABLESPACE_ITEM_CNT];
    PdbId m_pdbId;
    ControlFile *m_controlFile;
    VFSAdapter *m_vfs;
    TbsTempBitmapPageHashTable *m_tmpTbsHashTable; /* Temp tablespace only need one BitmapPageHashTable. */
    bool m_tbsValid[MAX_TABLESPACE_ITEM_CNT] = {false};
    bool m_datafileValid[MAX_DATAFILE_ITEM_CNT] = {false};
    LWLock m_tablespaceLWLocks[MAX_TABLESPACE_ITEM_CNT];
    LWLock m_datafileLWLocks[MAX_DATAFILE_ITEM_CNT];
};

}  // namespace DSTORE
#endif
