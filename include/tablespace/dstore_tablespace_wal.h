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
 * dstore_tablespace_wal.h
 *     This file defines the building blocks that bg page writer needs.
 *
 * IDENTIFICATION
 *        include/tablespace/dstore_tablespace_wal.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_TABLESPACE_WAL_H
#define DSTORE_TABLESPACE_WAL_H

#include "wal/dstore_wal_struct.h"
#include "page/dstore_fsm_page.h"
#include "tablespace/dstore_tablespace.h"
#include "framework/dstore_instance.h"

namespace DSTORE {

struct WalRecordTbs : public WalRecordForPage {
    static void RedoTbsRecord(WalRecordRedoContext *redoCtx, const WalRecord *tbsRecord, BufferDesc *bufferDesc);

    static void DumpTbsRecord(const WalRecordTbs *tbsRecord, FILE *fp);
    static bool DumpTbsRecordPart1(const WalRecordTbs *tbsRecord, FILE *fp);
    static bool DumpTbsRecordPart2(const WalRecordTbs *tbsRecord, FILE *fp);
    static bool DumpTbsRecordPart3(const WalRecordTbs *tbsRecord, FILE *fp);
    static bool DumpTbsRecordPart4(const WalRecordTbs *tbsRecord, FILE *fp);
    static bool DumpTbsRecordPart5(const WalRecordTbs *tbsRecord, FILE *fp);
    static bool DumpTbsRecordPart6(const WalRecordTbs *tbsRecord, FILE *fp);
    static bool DumpTbsRecordPart7(const WalRecordTbs *tbsRecord, FILE *fp);

    inline void SetHeader(const WalPageHeaderContext &walPageHeaderContext)
    {
        SetWalPageHeader(walPageHeaderContext);
    }
    /* for ut test redo to one page */
    void RedoInternal(PdbId pdbId, uint64 plsn, Page *page) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordTbs);

struct WalRecordTbsInitBitmapMetaPage : public WalRecordTbs {
    uint8 totalBlockCount;
    ExtentSize extentSize;
    void Redo(void *page) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordTbsInitBitmapMetaPage);

struct WalRecordTbsInitTbsFileMetaPage : public WalRecordTbs {
    uint64 m_reuseVersion;       /* indicates the reused version of the fileId */
    Xid m_ddlXid;                /* the xid of the transaction that created the tablespace */
    void Redo(void *page) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordTbsInitTbsFileMetaPage);

struct WalRecordTbsUpdateTbsFileMetaPage : public WalRecordTbs {
    uint32 hwm;
    uint32 oid;
    void Redo(void *page) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordTbsUpdateTbsFileMetaPage);

struct WalRecordTbsInitTbsSpaceMetaPage : public WalRecordTbs {
    void Redo(void *page) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordTbsInitTbsSpaceMetaPage);

struct WalRecordTbsBitmapSetBit : public WalRecordTbs {
    uint16 allocatedExtentCount;
    uint16 startBitPos;
    uint8 value;
    void Redo(void *page) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordTbsBitmapSetBit);

struct WalRecordTbsAddBitmapPages : public WalRecordTbs {
    uint16 groupCount;
    uint16 groupIndex;
    PageId groupFirstPage;
    uint8 groupFreePage;
    uint8 groupPageCount;
    uint16 validOffset;
    void Redo(void *page) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordTbsAddBitmapPages);

struct WalRecordTbsUpdateFirstFreeBitmapPageId : public WalRecordTbs {
    uint16 groupIndex;  /* which bitmapGroup to modify */
    uint8 firstFreePageNo;  /* the number of first bitmap Page which has free bit in group */
    inline void SetData(uint16 groupIndexInput, uint8 firstFreePageNoInput)
    {
        groupIndex = groupIndexInput;
        firstFreePageNo = firstFreePageNoInput;
    }
    void Redo(void *page) const;
} PACKED;

struct WalRecordTbsExtendFile : public WalRecordTbs {
    FileId fileId;
    uint64 totalBlockCount;
    void Redo(PdbId pdbId, void *page) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordTbsExtendFile);

// Init UndoSegment
struct WalRecordTbsInitUndoSegment : public WalRecordTbs {
    SegmentType segmentType;

    uint64 plsn;
    uint64 glsn;

    inline void SetData(SegmentType type, uint64 plsnInput, uint64 glsnInput)
    {
        segmentType = type;
        plsn = plsnInput;
        glsn = glsnInput;
    }
    void Redo(void *page) const;
} PACKED;

struct WalRecordTbsInitDataSegment : public WalRecordTbs {
    SegmentType segmentType;
    PageId addedPageId;
    uint64 plsn;
    uint64 glsn;
    bool isReUsedFlag;

    inline void SetData(SegmentType type, const PageId &addedPageIdInput, uint64 plsnInput, uint64 glsnInput,
        bool isReUsedFlagInput)
    {
        segmentType = type;
        addedPageId = addedPageIdInput;
        plsn = plsnInput;
        glsn = glsnInput;
        isReUsedFlag = isReUsedFlagInput;
    }
    void Redo(void *page) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordTbsInitDataSegment);

struct WalRecordTbsInitHeapSegment : public WalRecordTbs {
    PageId addedPageId;
    PageId fsmMetaPageId;
    NodeId assignedNodeId;
    uint16 fsmId;
    bool isReUsedFlag;
    uint64 plsn;
    uint64 glsn;

    inline void SetData(const PageId &addedPageIdInput, const PageId &fsmMetaPageIdInput, NodeId nodeIdInput,
        uint16 fsmIdInput, uint64 plsnInput, uint64 glsnInput)
    {
        addedPageId = addedPageIdInput;
        fsmMetaPageId = fsmMetaPageIdInput;
        assignedNodeId = nodeIdInput;
        fsmId = fsmIdInput;
        plsn = plsnInput;
        glsn = glsnInput;
    }

    inline void SetReUseFlag(bool isReUsedFlagInput)
    {
        isReUsedFlag = isReUsedFlagInput;
    }
    void Redo(void *page) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordTbsInitHeapSegment);

struct WalRecordTbsInitFreeSpaceMap : public WalRecordTbs {
    PageId fsmRootPageId;
    TimestampTz accessTimestamp;

    inline void SetData(const PageId &fsmRootPageIdInput, const TimestampTz accessTimestampInput)
    {
        fsmRootPageId = fsmRootPageIdInput;
        accessTimestamp = accessTimestampInput;
    }
    void Redo(void *page) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordTbsInitFreeSpaceMap);

struct WalRecordTbsExtentMetaInit : public WalRecordTbs {
    ExtentSize curExtSize;

    inline void SetData(ExtentSize curExtSizeInput)
    {
        curExtSize = curExtSizeInput;
    }
    void Redo(void *page) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordTbsExtentMetaInit);

struct WalRecordTbsExtentMetaLinkNext : public WalRecordTbs {
    PageId nextExtMetaPageId;

    inline void SetData(const PageId &nextExtMetaPageIdInput)
    {
        nextExtMetaPageId = nextExtMetaPageIdInput;
    }
    void Redo(void *page) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordTbsExtentMetaLinkNext);

struct WalRecordTbsMoveFsmSlot : public WalRecordTbs {
    uint16 moveSlotId;
    uint16 newListId;

    inline void SetData(uint16 moveSlotIdInput, uint16 newListIdInput)
    {
        moveSlotId = moveSlotIdInput;
        newListId = newListIdInput;
    }
    void Redo(void *page) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordTbsMoveFsmSlot);

struct WalRecordTbsAddMultiplePagesToFsmSlots : public WalRecordTbs {
    uint16 addPageCount;
    uint16 firstSlotId;
    uint16 addListId;
    PageId addPageIdList[0];

    inline void SetData(uint16 addPageCountInput, uint16 firstSlotIdInput, uint16 addListIdInput,
                        PageId *addPageIdListInput)
    {
        addPageCount = addPageCountInput;
        firstSlotId = firstSlotIdInput;
        addListId = addListIdInput;
        for (uint16 i = 0; i < addPageCountInput; ++i) {
            addPageIdList[i] = addPageIdListInput[i];
        }
    }
    void Redo(void *page) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordTbsAddMultiplePagesToFsmSlots);

struct WalRecordTbsInitFsmPage : public WalRecordTbs {
    PageId fsmMetaPageId;
    FsmIndex upperFsmIndex;
    uint16 initSlotCount;
    FsmNode fsmNodeData[0];

    inline void SetData(const PageId fsmMetaPageIdInput, const FsmIndex &upperFsmIndexInput, uint16 slotCountInput,
        FsmNode *fsmNodeDataInput)
    {
        fsmMetaPageId = fsmMetaPageIdInput;
        upperFsmIndex = upperFsmIndexInput;
        initSlotCount = slotCountInput;
        for (uint16 i = 0; i < slotCountInput; ++i) {
            fsmNodeData[i] = fsmNodeDataInput[i];
        }
    }
    void Redo(void *page) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordTbsInitFsmPage);

struct WalRecordTbsUpdateFsmIndex : public WalRecordTbs {
    FsmIndex upperFsmIndex;

    inline void SetData(const FsmIndex &upperFsmIndexInput)
    {
        upperFsmIndex = upperFsmIndexInput;
    }
    void Redo(void *page) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordTbsUpdateFsmIndex);

struct WalRecordTbsSegmentAddExtent : public WalRecordTbs {
    PageId extMetaPageId;
    ExtentSize extSize;
    ExtentUseType extUseType; /* 1 for data page extent, 2 for fsm page extent, 3 for undo page extent */

    inline void SetData(const PageId &extMetaPageIdInput, ExtentSize extSizeInput, ExtentUseType extUseTypeInput)
    {
        extMetaPageId = extMetaPageIdInput;
        extSize = extSizeInput;
        extUseType = extUseTypeInput;
    }
    void Redo(void *page) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordTbsSegmentAddExtent);

struct WalRecordTbsDataSegmentAddExtent : public WalRecordTbs {
    ExtentSize extSize;
    PageId addedPageId; /* has added to fsm */
    bool isReUsedFlag;

    inline void SetData(ExtentSize extSizeInput, const PageId &addedPageIdInput, const bool isReUsedFlagInput)
    {
        extSize = extSizeInput;
        addedPageId = addedPageIdInput;
        isReUsedFlag = isReUsedFlagInput;
    }
    void Redo(void *page) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordTbsDataSegmentAddExtent);

struct WalRecordTbsDataSegmentAssignDataPages : public WalRecordTbs {
    PageId addedPageId; /* has added to fsm */

    inline void SetDataSegmentMeta(const PageId &addedPageIdInput)
    {
        addedPageId = addedPageIdInput;
    }
    void Redo(void *page) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordTbsDataSegmentAssignDataPages);

struct WalRecordTbsSegmentUnlinkExtent : public WalRecordTbs {
    PageId nextExtMetaPageId;
    PageId unlinkExtMetaPageId;
    ExtentSize unlinkExtSize;
    ExtentUseType extUseType; /* 1 for data page extent, 2 for fsm page extent, 3 for undo page extent */

    inline void
    SetData(const PageId &nextExtMetaPageIdInput, const PageId &unlinkExtMetaPageIdInput, ExtentSize unlinkExtSizeInput,
            ExtentUseType extUseTypeInput)
    {
        nextExtMetaPageId = nextExtMetaPageIdInput;
        unlinkExtMetaPageId = unlinkExtMetaPageIdInput;
        unlinkExtSize = unlinkExtSizeInput;
        extUseType = extUseTypeInput;
    }
    void Redo(void *page) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordTbsSegmentUnlinkExtent);

struct WalRecordTbsSegMetaAddFsmTree : public WalRecordTbs {
    PageId fsmMetaPageId;
    NodeId assignedNodeId;
    uint16 fsmId;

    inline void SetData(const PageId &fsmMetaPageIdInput, const NodeId &nodeId, const uint16 fsmIdInput)
    {
        fsmMetaPageId = fsmMetaPageIdInput;
        assignedNodeId = nodeId;
        fsmId = fsmIdInput;
    }
    void Redo(void *page) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordTbsSegMetaAddFsmTree);

struct WalRecordTbsSegMetaRecycleFsmTree : public WalRecordTbs {
    PageId fsmMetaPageId;
    NodeId assignedNodeId;
    uint16 fsmId;

    inline void SetData(const PageId &fsmMetaPageIdInput, const NodeId &assignedNodeIdInput, const uint16 fsmIdInput)
    {
        fsmMetaPageId = fsmMetaPageIdInput;
        assignedNodeId = assignedNodeIdInput;
        fsmId = fsmIdInput;
    }

    void Redo(void *page) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordTbsSegMetaRecycleFsmTree);

struct WalRecordTbsSegMetaAdjustDataPagesInfo : public WalRecordTbs {
    PageId firstDataPageId;
    PageId lastDataPageId;
    PageId addedPageId;
    uint64 totalDataPageCount;

    inline void SetData(const PageId &firstDataPageIdInput, const PageId &lastDataPageIdInput,
                        uint64 totalDataPageCountInput, const PageId &addedPageIdInput)
    {
        firstDataPageId = firstDataPageIdInput;
        lastDataPageId = lastDataPageIdInput;
        totalDataPageCount = totalDataPageCountInput;
        addedPageId = addedPageIdInput;
    }
    void Redo(void *page) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordTbsSegMetaAdjustDataPagesInfo);

struct WalRecordTbsFsmMetaUpdateFsmTree : public WalRecordTbs {
    ExtentRange fsmExtents;
    uint8 numFsmLevels;
    uint64 mapCount[HEAP_MAX_MAP_LEVEL];
    PageId currMap[HEAP_MAX_MAP_LEVEL];
    PageId usedFsmPageId;
    PageId lastFsmPageId;
    PageId curFsmExtMetaPageId;

    inline void SetData(FreeSpaceMapMetaPage *fsmMetaPage)
    {
        fsmExtents = fsmMetaPage->fsmExtents;
        numFsmLevels = fsmMetaPage->numFsmLevels;
        for (int i = 0; i < HEAP_MAX_MAP_LEVEL; ++i) {
            mapCount[i] = fsmMetaPage->mapCount[i];
            currMap[i] = fsmMetaPage->currMap[i];
        }
        usedFsmPageId = fsmMetaPage->usedFsmPage;
        lastFsmPageId = fsmMetaPage->lastFsmPage;
        curFsmExtMetaPageId = fsmMetaPage->curFsmExtMetaPageId;
    }
    void Redo(void *page) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordTbsFsmMetaUpdateFsmTree);

struct WalRecordTbsFsmMetaUpdateNumUsedPages : public WalRecordTbs {
    uint64 numUsedPages;

    inline void SetData(uint64 numUsedPagesInput)
    {
        numUsedPages = numUsedPagesInput;
    }
    void Redo(void *page) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordTbsFsmMetaUpdateNumUsedPages);

struct WalRecordTbsFsmMetaUpdateExtensionStat : public WalRecordTbs {
    uint64 numTotalPages;
    uint16 extendCoefficient;

    inline void SetData(const uint64 numTotalPagesInput, const uint16 extendCoefficientInput)
    {
        numTotalPages = numTotalPagesInput;
        extendCoefficient = extendCoefficientInput;
    }
    void Redo(void *page) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordTbsFsmMetaUpdateExtensionStat);

struct WalRecordTbsInitOneDataPage : public WalRecordTbs {
    PageType dataPageType;
    FsmIndex curFsmIndex;
    inline void SetData(const PageId &pageId, PageType pageType,
                        const FsmIndex fsmIndex, const WalRecordLsnInfo &lsnInfo, const uint64 fileVersion)
    {
        m_pageId = pageId;
        m_size = sizeof(WalRecordTbsInitOneDataPage);
        m_type = WAL_TBS_INIT_ONE_DATA_PAGE;
        dataPageType = pageType;
        curFsmIndex = fsmIndex;
        m_pagePreWalId = lsnInfo.walId;
        m_pagePrePlsn = lsnInfo.endPlsn;
        m_pagePreGlsn = lsnInfo.glsn;
        m_filePreVersion = fileVersion;
        m_flags.m_placeHolder = 0;
    }
    void Redo(BufferDesc *bufDesc) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordTbsSegmentUnlinkExtent);

struct WalRecordTbsInitOneBitmapPage : public WalRecordTbs {
    PageType pageType;
    PageId curDataPageId;
    inline void SetData(const PageId &pageId, PageType type, const PageId dataPageId, const WalRecordLsnInfo &lsnInfo,
        const uint64 fileVersion)
    {
        m_pageId = pageId;
        m_type = WAL_TBS_INIT_ONE_BITMAP_PAGE;
        m_size = sizeof(WalRecordTbsInitOneBitmapPage);
        pageType = type;
        curDataPageId = dataPageId;
        m_pagePreWalId = lsnInfo.walId;
        m_pagePrePlsn = lsnInfo.endPlsn;
        m_pagePreGlsn = lsnInfo.glsn;
        m_filePreVersion = fileVersion;
        m_flags.m_placeHolder = 0;
    }
    void Redo(void *page) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordTbsSegmentUnlinkExtent);

/* Logic xlog of init multiple data pages */
struct WalRecordTbsInitDataPages : public WalRecord {
    uint64 filePreVersion;
    PageType dataPageType;
    PageId firstDataPageId;
    FsmIndex firstFsmIndex;
    uint16 dataPageCount;
    WalRecordLsnInfo preWalPointer[0];

    inline void SetFileVersion(uint64 fileVersion)
    {
        filePreVersion = fileVersion;
    }

    inline void SetHeader(WalType walType, Size walTotalSize, uint64 fileVersion)
    {
        SetType(walType);
        SetSize(walTotalSize);
        SetFileVersion(fileVersion);
    }

    inline void SetData(PageType dataPageTypeInput, const PageId &firstDataPageIdInput,
                        const FsmIndex &firstFsmIndexInput,
                        uint16 dataPageCountInput, const WalRecordLsnInfo *preWalPointerInput)
    {
        dataPageType = dataPageTypeInput;
        firstDataPageId = firstDataPageIdInput;
        firstFsmIndex = firstFsmIndexInput;
        dataPageCount = dataPageCountInput;
        for (uint16 i = 0; i < dataPageCountInput; ++i) {
            preWalPointer[i].glsn = preWalPointerInput[i].glsn;
            preWalPointer[i].endPlsn = preWalPointerInput[i].endPlsn;
            preWalPointer[i].walId = preWalPointerInput[i].walId;
        }
    }

    inline void GetPageIdRange(PageId &firstPageIdOutput, uint16 &pageCountOutput) const
    {
        firstPageIdOutput = firstDataPageId;
        pageCountOutput = dataPageCount;
    }
    
    inline uint64 GetFilePreVersion() const
    {
        return filePreVersion;
    }

    inline PageId GetPageId() const
    {
        return firstDataPageId;
    }

    void Redo(WalRecordRedoContext *redoCtx) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordTbsInitDataPages);

struct WalRecordTbsInitBitmapPages : public WalRecord {
    uint64 filePreVersion;
    PageType pageType;
    PageId firstPageId;
    PageId firstDataPageId;
    uint16 pageCount;
    ExtentSize extentSize;
    WalRecordLsnInfo preWalPointer[0];

    inline void SetFileVersion(uint64 fileVersion)
    {
        filePreVersion = fileVersion;
    }

    inline void SetHeader(WalType walType, Size walTotalSize, uint64 fileVersion)
    {
        SetType(walType);
        SetSize(walTotalSize);
        SetFileVersion(fileVersion);
    }

    inline void SetData(PageType pageTypeInput, const PageId &firstPageIdInput, const PageId &firstDataPageIdInput,
                        uint16 pageCountInput, const ExtentSize extentSizeInput,
                        const WalRecordLsnInfo *preWalPointerInput)
    {
        pageType = pageTypeInput;
        firstPageId = firstPageIdInput;
        firstDataPageId = firstDataPageIdInput;
        pageCount = pageCountInput;
        extentSize = extentSizeInput;
        for (uint16 i = 0; i < pageCountInput; ++i) {
            preWalPointer[i].glsn = preWalPointerInput[i].glsn;
            preWalPointer[i].endPlsn = preWalPointerInput[i].endPlsn;
            preWalPointer[i].walId = preWalPointerInput[i].walId;
        }
    }
    inline void GetPageIdRange(PageId &firstPageIdOutput, uint16 &pageCountOutput) const
    {
        firstPageIdOutput = firstPageId;
        pageCountOutput = pageCount;
    }
    
    inline uint64 GetFilePreVersion() const
    {
        return filePreVersion;
    }

    inline PageId GetPageId() const
    {
        return firstPageId;
    }
    void Redo(WalRecordRedoContext *redoCtx) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordTbsInitBitmapPages);

struct WalRecordTbsLogical : public WalRecord {
    TablespaceId tablespaceId;
    uint64 preReuseVersion;
    inline TablespaceId GetTablespaceId() const
    {
        return tablespaceId;
    }
    inline uint64 GetPreReuseVersion() const
    {
        return preReuseVersion;
    }
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordTbsLogical);

/* Logic wal when create tablespace page item */
struct WalRecordTbsCreateTablespace : public WalRecordTbsLogical {
    uint64 tbsMaxSize;
    Xid ddlXid;
    inline void SetHeader(WalType walType, Size walTotalSize)
    {
        SetType(walType);
        SetSize(walTotalSize);
    }

    inline void SetData(TablespaceId tbsId, uint64 size, uint64 version, Xid xid)
    {
        tablespaceId = tbsId;
        tbsMaxSize = size;
        preReuseVersion = version;
        ddlXid = xid;
    }
    void Redo(WalRecordRedoContext *redoCtx) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordTbsCreateTablespace);

/* Logic wal when alloc datafile id */
struct WalRecordTbsCreateDataFile : public WalRecordTbsLogical {
    FileId fileId;
    uint64 fileMaxSize;
    ExtentSize extentSize;
    Xid ddlXid;
    inline void SetHeader(WalType walType, Size walTotalSize)
    {
        SetType(walType);
        SetSize(walTotalSize);
    }

    inline void SetData(TablespaceId tbsId, FileId id, uint64 fileSize, ExtentSize extsize, uint64 version, Xid xid)
    {
        tablespaceId = tbsId;
        fileId = id;
        fileMaxSize = fileSize;
        extentSize = extsize;
        preReuseVersion = version;
        ddlXid = xid;
    }

    inline FileId GetFileId() const
    {
        return fileId;
    }

    void Redo(WalRecordRedoContext *redoCtx) const;
    RetStatus CreateFile(StoragePdb *pdb) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordTbsCreateDataFile);

/* Logic wal when add datafile to tablespace */
struct WalRecordTbsAddFileToTbs : public WalRecordTbsLogical {
    uint16 hwm;
    FileId fileId;
    Xid ddlXid;
    uint16 slotId;
    inline void SetHeader(WalType walType, Size walTotalSize)
    {
        SetType(walType);
        SetSize(walTotalSize);
    }

    inline void SetData(TablespaceId tbsId, uint16 hwm_new, FileId id, uint64 version, Xid xid, uint16 slot)
    {
        tablespaceId = tbsId;
        hwm = hwm_new;
        fileId = id;
        preReuseVersion = version;
        ddlXid = xid;
        slotId = slot;
    }

    inline FileId GetFileId() const
    {
        return fileId;
    }

    void Redo(WalRecordRedoContext *redoCtx) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordTbsAddFileToTbs);

/* Logic wal when drop tablespace page item */
struct WalRecordTbsDropTablespace : public WalRecordTbsLogical {
    uint16 hwm;
    Xid ddlXid;
    inline void SetHeader(WalType walType, Size walTotalSize)
    {
        SetType(walType);
        SetSize(walTotalSize);
    }

    inline void SetData(TablespaceId tbsId, uint16 hwm_new,
                        uint64 version, Xid xid)
    {
        tablespaceId = tbsId;
        hwm = hwm_new;
        preReuseVersion = version;
        ddlXid = xid;
    }
    void Redo(WalRecordRedoContext *redoCtx) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordTbsDropTablespace);

/* Logic wal when drop datafile page item */
struct WalRecordTbsDropDataFile : public WalRecordTbsLogical {
    uint16 hwm;
    FileId fileId;
    Xid ddlXid;
    uint16 slotId;
    inline void SetHeader(WalType walType, Size walTotalSize)
    {
        SetType(walType);
        SetSize(walTotalSize);
    }

    inline void SetData(TablespaceId tbsId, uint16 hwm_new,
                        FileId id, uint64 version, Xid xid, uint16 slot)
    {
        tablespaceId = tbsId;
        hwm = hwm_new;
        fileId = id;
        preReuseVersion = version;
        ddlXid = xid;
        slotId = slot;
    }
    inline FileId GetFileId() const
    {
        return fileId;
    }

    void Redo(WalRecordRedoContext *redoCtx) const;
    RetStatus DropFile(StoragePdb *pdb) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordTbsDropDataFile);

/* Logic wal when modify tablespace page item */
struct WalRecordTbsAlterTablespace : public WalRecordTbsLogical {
    uint64 tbsMaxSize;
    Xid ddlXid;
    inline void SetHeader(WalType walType, Size walTotalSize)
    {
        SetType(walType);
        SetSize(walTotalSize);
    }

    inline void SetData(TablespaceId tbsId, uint64 size, uint64 version, Xid xid)
    {
        tablespaceId = tbsId;
        tbsMaxSize = size;
        preReuseVersion = version;
        ddlXid = xid;
    }
    void Redo(WalRecordRedoContext *redoCtx) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordTbsAlterTablespace);
}  // namespace DSTORE

#endif  // STORAGE_TABLESPACE_WAL_H
