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
 * dstore_btree_wal.h
 *
 * IDENTIFICATION
 *        dstore/include/index/dstore_btree_wal.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_STORAGE_BTREE_WAL
#define DSTORE_STORAGE_BTREE_WAL

#include "index/dstore_btree_prune.h"
#include "wal/dstore_wal_struct.h"
#include "page/dstore_index_page.h"
#include "index/dstore_btree_split.h"

namespace DSTORE {

struct WalRecordIndex : public WalRecordForDataPage {
public:
    inline void SetWalHeader(WalType type, BtrPage *page, uint32 size, bool isGlsnChanged, uint64 fileVersion)
    {
        SetWalPageHeader({type, size, page->GetSelfPageId(), page->GetWalId(), page->GetPlsn(), page->GetGlsn(),
                          isGlsnChanged, fileVersion});
    }
    static void RedoIndexRecord(WalRecordRedoContext *redoCtx, const WalRecordIndex *indexRecord,
                                BufferDesc *bufferDesc);
    static void DumpIndexRecord(const WalRecordIndex *indexRecord, FILE *fp);
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordIndex);

struct WalRecordBtreeBuild : public WalRecordIndex {
public:
    char pageData[BLCKSZ];

    inline void SetHeader(BtrPage *page, bool isGlsnChanged, uint64 fileVersion)
    {
        SetWalHeader(WAL_BTREE_BUILD, page, sizeof(WalRecordBtreeBuild), isGlsnChanged, fileVersion);
    }
    void SetData(BtrPage *page);
    void Redo(BtrPage *page, Xid xid) const;
    void Describe(FILE *fp) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordBtreeBuild);

struct WalRecordBtreeInitMetaPage : public WalRecordIndex {
public:
    Xid btrCreateXid;
    char btrMeta[BTR_META_SIZE];

    inline void SetHeader(BtrPage *page, bool isGlsnChanged, uint64 fileVersion)
    {
        SetWalHeader(WAL_BTREE_INIT_META_PAGE, page, sizeof(WalRecordBtreeInitMetaPage), isGlsnChanged, fileVersion);
    }
    void SetData(BtrPage *page);
    void Redo(BtrPage *page, Xid xid) const;
    void Describe(FILE *fp) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordBtreeInitMetaPage);

struct WalRecordBtreeNewInternalRoot : public WalRecordIndex {
public:
    PageId origRoot;
    PageId btrMeta;
    Xid btrMetaCreateXid;
    uint32 rootLevel;
    char rawData[];

    inline void SetHeader(BtrPage *page, uint32 size, bool isGlsnChanged, const PageId root, uint64 fileVersion)
    {
        SetWalHeader(WAL_BTREE_NEW_INTERNAL_ROOT, page, size, isGlsnChanged, fileVersion);
        BtrPageLinkAndStatus *linkStat = page->GetLinkAndStatus();
        origRoot = root;
        btrMeta = linkStat->btrMetaPageId;
        btrMetaCreateXid = page->GetBtrMetaCreateXid();
        rootLevel = linkStat->GetLevel();
    }

    inline void SetTupleData(IndexTuple *tuple)
    {
        CopyData(rawData, static_cast<uint32>(m_size - (sizeof(WalRecordBtreeNewInternalRoot))),
                 static_cast<char *>(static_cast<void *>(tuple)), tuple->GetSize());
    }
    void Redo(BtrPage *page, Xid xid) const;
    void Describe(FILE *fp) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordBtreeNewInternalRoot);

struct WalRecordBtreeNewLeafRoot : public WalRecordIndex {
public:
    PageId btrMeta;
    Xid metaCreateXid;
    inline void SetHeader(BtrPage *page, bool isGlsnChanged, const PageId btrMetaPageId, Xid xid, uint64 fileVersion)
    {
        SetWalHeader(WAL_BTREE_NEW_LEAF_ROOT, page, sizeof(WalRecordBtreeNewLeafRoot), isGlsnChanged, fileVersion);
        btrMeta = btrMetaPageId;
        metaCreateXid = xid;
    }

    void Redo(BtrPage *page, Xid xid) const;
    void Describe(FILE *fp) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordBtreeNewLeafRoot);

struct WalRecordBtreeInsertOnInternal : public WalRecordIndex {
public:
    OffsetNumber m_offset;
    char m_rawData[];

    inline void SetHeader(BtrPage *page, uint32 size, bool isGlsnChanged, OffsetNumber offset, uint64 fileVersion)
    {
        SetWalHeader(WAL_BTREE_INSERT_ON_INTERNAL, page, size, isGlsnChanged, fileVersion);
        m_offset = offset;
    }

    inline void SetTupleData(IndexTuple *tuple)
    {
        CopyData(m_rawData, static_cast<uint32>(m_size - (sizeof(WalRecordBtreeInsertOnInternal))),
                 static_cast<char *>(static_cast<void *>(tuple)), tuple->GetSize());
    }

    void Redo(BtrPage *page, Xid xid) const;
    void Describe(FILE *fp) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordBtreeInsertOnInternal);

struct WalRecordBtreeInsertOnLeaf : public WalRecordIndex {
public:
    OffsetNumber m_offset;
    uint64 m_undoRecPtr;
    char m_rawData[]; /* IndexTuple + AllocTd */

    inline void SetHeader(BtrPage *page, uint32 size, bool isGlsnChanged, OffsetNumber offset, uint64 fileVersion)
    {
        SetWalHeader(WAL_BTREE_INSERT_ON_LEAF, page, size, isGlsnChanged, fileVersion);
        m_offset = offset;
    }

    inline void SetUndoRecPtr(UndoRecPtr undoRecPtr)
    {
        m_undoRecPtr = undoRecPtr.m_placeHolder;
    }

    inline void SetAllocTd(TDAllocContext &tdContext)
    {
        IndexTuple *tuple = static_cast<IndexTuple *>(static_cast<void *>(m_rawData));
        SetAllocTdWal(
            tdContext, m_rawData + tuple->GetSize(), m_size - (sizeof(WalRecordBtreeInsertOnLeaf) + tuple->GetSize()));
    }

    inline void SetTupleData(IndexTuple *tuple)
    {
        CopyData(m_rawData, static_cast<uint32>(m_size - sizeof(WalRecordBtreeInsertOnLeaf)),
                 static_cast<char *>(static_cast<void *>(tuple)), tuple->GetSize());
    }

    void Redo(BtrPage *page, Xid xid) const;
    void Describe(FILE *fp) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordBtreeInsertOnLeaf);

struct WalRecordBtreeSplit : public WalRecordIndex {
public:
    PageId newRightBlockNumber;
    OffsetNumber firstRightOffNum;

    inline uint32 AppendTupleData(char *walData, IndexTuple *tuple, uint32 from = 0U) const
    {
        uint32 tupleSize = tuple->GetSize();
        uint32 end = from + tupleSize;
        char *buf = walData + from;
        StorageAssert(walData + end <= static_cast<const char *>(static_cast<const void *>(this)) + m_size);
        CopyData(buf, m_size - from, static_cast<char *>(static_cast<void *>(tuple)), tupleSize);
        return end;
    }

    inline void SetAllocTd(char *walData, TDAllocContext &tdContext, uint32 from, uint32 dataSize)
    {
        StorageAssert(walData + from + dataSize <= static_cast<char *>(static_cast<void *>(this)) + m_size);
        SetAllocTdWal(tdContext, walData + from, dataSize);
    }

    inline void DescSplit(FILE *fp) const
    {
        (void)fprintf(fp, "new right(%hu, %u), first right off(%hu), ", newRightBlockNumber.m_fileId,
                    newRightBlockNumber.m_blockId, firstRightOffNum);
    }

    void RedoSplitOnly(BtrPage *page, const char *walData, uint32 hikeyFrom = 0U) const;
    uint8 RedoSplitInsert(BtrPage *page, OffsetNumber insertOff, const char *walData) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordBtreeSplit);

struct WalRecordBtreeSplitInternal : public WalRecordBtreeSplit {
public:
    char m_rawData[];

    inline void SetHeader(BtrPage *page, uint32 size, bool isGlsnChanged, const PageId newRight,
                          OffsetNumber firstRightOff, uint64 fileVersion)
    {
        SetWalHeader(WAL_BTREE_SPLIT_INTERNAL, page, size, isGlsnChanged, fileVersion);
        newRightBlockNumber = newRight;
        firstRightOffNum = firstRightOff;
    }
    inline char *GetDataField()
    {
        return static_cast<char *>(m_rawData);
    }
    void Redo(BtrPage *page, Xid xid) const;
    void Describe(FILE *fp) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordBtreeSplitInternal);

struct WalRecordBtreeSplitLeaf : public WalRecordBtreeSplit {
public:
    bool needSetTD;
    char m_rawData[];

    inline void SetHeader(BtrPage *page, uint32 size, bool isGlsnChanged, const PageId newRight,
                          OffsetNumber firstRightOff, uint64 fileVersion)
    {
        SetWalHeader(WAL_BTREE_SPLIT_LEAF, page, size, isGlsnChanged, fileVersion);
        newRightBlockNumber = newRight;
        firstRightOffNum = firstRightOff;
        needSetTD = false;
    }
    inline char *GetDataField()
    {
        return static_cast<char *>(m_rawData);
    }

    inline void SetUndoRecPtr(uint64 undoRecPtr, UNUSE_PARAM uint64 oldUndoRecPtr)
    {
        StorageAssert(needSetTD);
        uint64 *undoRecPtrInWal = STATIC_CAST_PTR_TYPE(m_rawData + sizeof(uint8), uint64 *);
        StorageAssert(*undoRecPtrInWal == oldUndoRecPtr);
        *undoRecPtrInWal = undoRecPtr;
    }

    uint32 SetTdData(uint32 from, BtrPage *page, uint8 tdID);

    void Redo(BtrPage *page, Xid xid) const;
    void Describe(FILE *fp) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordBtreeSplitLeaf);

struct WalRecordBtreeSplitInsertInternal : public WalRecordBtreeSplit {
public:
    OffsetNumber m_insertOff;
    char m_rawData[];

    inline void SetHeader(BtrPage *page, uint32 size, bool isGlsnChanged, const PageId newRight,
                          const SplitContext &splitContext, uint64 fileVersion)
    {
        SetWalHeader(WAL_BTREE_SPLIT_INSERT_INTERNAL, page, size, isGlsnChanged, fileVersion);
        newRightBlockNumber = newRight;
        firstRightOffNum = splitContext.firstRightOff;
        m_insertOff = splitContext.insertOff;
    }
    inline char *GetDataField()
    {
        return static_cast<char *>(m_rawData);
    }
    void Redo(BtrPage *page, Xid xid) const;
    void Describe(FILE *fp) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordBtreeSplitInsertInternal);

struct WalRecordBtreeSplitInsertLeaf : public WalRecordBtreeSplit {
public:
    OffsetNumber m_insertOff;
    uint64 m_undoRecPtr;
    char m_rawData[];

    inline void SetHeader(BtrPage *page, uint32 size, bool isGlsnChanged, const PageId newRight,
                          const SplitContext &splitContext, uint64 fileVersion)
    {
        SetWalHeader(WAL_BTREE_SPLIT_INSERT_LEAF, page, size, isGlsnChanged, fileVersion);
        newRightBlockNumber = newRight;
        firstRightOffNum = splitContext.firstRightOff;
        m_insertOff = splitContext.insertOff;
    }
    inline char *GetDataField()
    {
        return static_cast<char *>(m_rawData);
    }

    inline void SetUndoRecPtr(UndoRecPtr undoRecPtr)
    {
        m_undoRecPtr = undoRecPtr.m_placeHolder;
    }
    void Redo(BtrPage *page, Xid xid) const;
    void Describe(FILE *fp) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordBtreeSplitInsertLeaf);

struct WalRecordBtreeNewRight : public WalRecordIndex {
public:
    /* Page header */
    uint16 lower;
    uint16 upper;
    uint16 pageFlags;
    uint8 tdCount;
    Xid btrCreatedXid;
    char m_rawData[];

    inline void SetHeader(BtrPage *page, uint32 size, bool isGlsnChanged, uint64 fileVersion)
    {
        lower = page->GetLower();
        upper = page->GetUpper();
        tdCount = page->GetTdCount();
        pageFlags = page->m_header.m_flags;

        WalType walType = (page->GetLinkAndStatus()->TestType(BtrPageType::LEAF_PAGE)) ? WAL_BTREE_NEW_LEAF_RIGHT
                                                                                       : WAL_BTREE_NEW_INTERNAL_RIGHT;
        SetWalPageHeader({walType, size, page->GetSelfPageId(), page->GetWalId(), page->GetPlsn(), page->GetGlsn(),
                          isGlsnChanged, fileVersion});
    }

    void SetPage(BtrPage *page);
    void Redo(BtrPage *page, Xid xid, bool forDump = false) const;
    void Describe(FILE *fp) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordBtreeNewRight);

struct WalRecordBtreeUpdateSplitStatus : public WalRecordIndex {
public:
    inline void SetHeader(BtrPage *page, bool isGlsnChanged, uint64 fileVersion)
    {
        SetWalHeader(WAL_BTREE_UPDATE_SPLITSTATUS, page, sizeof(WalRecordBtreeUpdateSplitStatus), isGlsnChanged,
                     fileVersion);
    }

    void Redo(BtrPage *page, UNUSE_PARAM Xid xid) const
    {
        page->GetLinkAndStatus()->SetSplitStatus(BtrPageSplitStatus::SPLIT_COMPLETE);
        page->GetLinkAndStatus()->SetRoot(false);
    }
    void Describe(UNUSE_PARAM FILE *fp) const {}
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordBtreeUpdateSplitStatus);

struct WalRecordBtreeDeleteOnInternal : public WalRecordIndex {
public:
    OffsetNumber m_offset;

    inline void SetHeader(BtrPage *page, bool isGlsnChanged, OffsetNumber delOff, uint64 fileVersion)
    {
        SetWalHeader(WAL_BTREE_DELETE_ON_INTERNAL, page, sizeof(WalRecordBtreeDeleteOnInternal), isGlsnChanged,
                     fileVersion);
        m_offset = delOff;
    }

    void Redo(BtrPage *page, Xid xid) const;
    void Describe(FILE *fp) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordBtreeDeleteOnInternal);

struct WalRecordBtreeDeleteOnLeaf : public WalRecordIndex {
public:
    OffsetNumber m_offset;
    uint8 m_tdId;
    uint64 m_undoRecPtr;
    uint64 m_heapCtid;
    char m_rawData[];

    inline void SetHeader(BtrPage *page, uint32 size, bool isGlsnChanged, OffsetNumber delOff, uint8 tdId,
                          uint64 fileVersion, ItemPointerData delHeapCtid)
    {
        SetWalHeader(WAL_BTREE_DELETE_ON_LEAF, page, size, isGlsnChanged, fileVersion);
        m_offset = delOff;
        m_tdId = tdId;
        m_heapCtid = delHeapCtid.m_placeHolder;
    }

    inline void SetUndoRecPtr(UndoRecPtr undoRecPtr)
    {
        m_undoRecPtr = undoRecPtr.m_placeHolder;
    }

    inline void SetAllocTd(TDAllocContext &tdContext)
    {
        SetAllocTdWal(tdContext, m_rawData, m_size - sizeof(WalRecordBtreeDeleteOnLeaf));
    }

    void Redo(BtrPage *page, Xid xid) const;
    void Describe(FILE *fp) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordBtreeDeleteOnLeaf);

struct WalRecordBtreePagePrune : public WalRecordIndex {
public:
    bool prunable;
    uint8 fixedTdCount;
    uint8 reserved;
    uint16 numLiveTuples;
    uint32 pruneStatusDataLen;
    char   m_rawData[];

    inline void SetHeader(BtrPage *page, uint32 size, bool isGlsnChanged, uint16 tdCount, uint16 nTuples,
                          uint64 fileVersion)
    {
        SetWalHeader(WAL_BTREE_PAGE_PRUNE, page, size, isGlsnChanged, fileVersion);
        prunable = page->HasPrunableTuple();
        fixedTdCount = static_cast<uint8>(tdCount);
        reserved = 0;
        numLiveTuples = nTuples;
    }

    uint32 SetData(const bool *isTupleLive, uint16 numTotalItemIds);

    inline void SetAllocTd(TDAllocContext &context, uint32 offset)
    {
        SetAllocTdWal(context, m_rawData + offset, m_size - (sizeof(WalRecordBtreePagePrune) + offset));
    }

    void Redo(PdbId pdbId, BtrPage *page, Xid xid) const;
    void Describe(FILE *fp) const;
private:
    uint32 AppendData(uint32 fromPos, const char *srcData, uint32 srcSize);
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordBtreePagePrune);

struct WalRecordBtreeAllocTd : public WalRecordIndex {
public:
    char rawData[]; /* AllocTd */

    inline void SetHeader(BtrPage *page, uint32 size, bool isGlsnChanged, uint64 fileVersion)
    {
        SetWalHeader(WAL_BTREE_ALLOC_TD, page, size, isGlsnChanged, fileVersion);
    }
    inline void SetAllocTd(TDAllocContext &context)
    {
        SetAllocTdWal(context, rawData, m_size - sizeof(WalRecordBtreeAllocTd));
    }

    void Redo(BtrPage *page, UNUSE_PARAM Xid xid) const
    {
        RedoAllocTdWal(page, PageType::INDEX_PAGE_TYPE, rawData, m_size - sizeof(WalRecordBtreeAllocTd));
    }
    void Describe(FILE *fp) const
    {
        WalRecordForPage::Dump(fp);
        if (m_size == sizeof(WalRecordBtreeAllocTd)) {
            return;
        }
        DescAllocTd(fp, rawData, m_size - sizeof(WalRecordBtreeAllocTd));
    }
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordBtreeAllocTd);
static_assert((sizeof(WalRecordBtreeAllocTd) + sizeof(WalRecordForDataPage::AllocTdRecord) +
               sizeof(TrxSlotStatus) * MAX_TD_COUNT) <= MAX_TD_WAL_DATA,
              "Btree TD wal record size is too large");

struct WalRecordBtreeUpdateLiveStatus : public WalRecordIndex {
public:
    uint16 liveStatus;

    inline void SetHeader(BtrPage *page, bool isGlsnChanged, BtrPageLiveStatus status, uint64 fileVersion)
    {
        SetWalHeader(WAL_BTREE_UPDATE_LIVESTATUS, page, sizeof(WalRecordBtreeUpdateLiveStatus), isGlsnChanged,
                     fileVersion);
        liveStatus = static_cast<uint16>(status);
    }

    void Redo(BtrPage *page, UNUSE_PARAM Xid xid) const
    {
        page->GetLinkAndStatus()->SetLiveStatus(static_cast<BtrPageLiveStatus>(liveStatus));
    }
    void Describe(FILE *fp) const
    {
        (void)fprintf(fp, "liveStatus(%hu)", liveStatus);
    }
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordBtreeUpdateLiveStatus);

struct WalRecordBtreeUpdateSibLink : public WalRecordIndex {
public:
    PageId sibLinkPageId;

    inline void SetHeader(BtrPage *page, bool isGlsnChanged, const PageId sibLink, bool left, uint64 fileVersion)
    {
        WalType walType = (left) ? WAL_BTREE_UPDATE_LEFT_SIB_LINK : WAL_BTREE_UPDATE_RIGHT_SIB_LINK;
        SetWalHeader(walType, page, sizeof(WalRecordBtreeUpdateSibLink), isGlsnChanged, fileVersion);
        sibLinkPageId = sibLink;
    }
    void Redo(BtrPage *page, Xid xid) const;
    void Describe(FILE *fp) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordBtreeUpdateSibLink);

struct WalRecordBtreeUpdateMetaPage : public WalRecordIndex {
public:
    PageId rootPageId;
    uint32 level;

    inline void SetHeader(BtrPage *page, bool isGlsnChanged, bool isRoot, uint64 fileVersion)
    {
        WalType walType;
        BtrMeta *btrMeta = static_cast<BtrMeta *>(static_cast<void *>(page->GetData()));
        if (isRoot) {
            walType = WAL_BTREE_UPDATE_META_ROOT;
            rootPageId = btrMeta->GetRootPageId();
            level = btrMeta->GetRootLevel();
        } else {
            walType = WAL_BTREE_UPDATE_LOWEST_SINGLE_PAGE;
            rootPageId = btrMeta->GetLowestSinglePage();
            level = btrMeta->GetLowestSinglePageLevel();
        }
        SetWalHeader(walType, page, sizeof(WalRecordBtreeUpdateMetaPage), isGlsnChanged, fileVersion);
    }
    void Redo(BtrPage *page, Xid xid) const;
    void Describe(FILE *fp) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordBtreeUpdateMetaPage);

struct WalRecordBtreeUpdateDownlink : public WalRecordIndex {
public:
    OffsetNumber tupleOffset;
    PageId downlinkPageId;

    inline void SetHeader(BtrPage *page, bool isGlsnChanged, OffsetNumber offsetNum, const PageId downlinkPage,
                          uint64 fileVersion)
    {
        SetWalHeader(WAL_BTREE_UPDATE_DOWNLINK, page, sizeof(WalRecordBtreeUpdateDownlink), isGlsnChanged, fileVersion);
        tupleOffset = offsetNum;
        downlinkPageId = downlinkPage;
    }

    void Redo(BtrPage *page, Xid xid) const;
    void Describe(FILE *fp) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordBtreeUpdateDownlink);

struct WalRecordBtreeEraseInsForDelFlag : public WalRecordIndex {
public:
    OffsetNumber tupleOffset;

    inline void SetHeader(BtrPage *page, bool isGlsnChanged, OffsetNumber offsetNum, uint64 fileVersion)
    {
        SetWalHeader(WAL_BTREE_ERASE_INS_FOR_DEL_FLAG, page, sizeof(WalRecordBtreeEraseInsForDelFlag), isGlsnChanged,
                     fileVersion);
        tupleOffset = offsetNum;
    }

    void Redo(BtrPage *page, Xid xid) const;
    void Describe(UNUSE_PARAM FILE *fp) const {}
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordBtreeEraseInsForDelFlag);

}

#endif // DSTORE_STORAGE_BTREE_WAL
