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
 * dstore_btree.h
 *
 * IDENTIFICATION
 *        dstore/include/index/dstore_btree.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_GAUSSKERNEL_INCLUDE_INDEX_DSTORE_BTREE_H
#define SRC_GAUSSKERNEL_INCLUDE_INDEX_DSTORE_BTREE_H

#include "common/log/dstore_log.h"
#include "systable/systable_relation.h"
#include "index/dstore_scankey.h"
#include "tablespace/dstore_tablespace.h"
#include "tablespace/dstore_index_normal_segment.h"
#include "page/dstore_index_page.h"
#include "wal/dstore_wal_write_context.h"
#include "framework/dstore_instance.h"
#include "errorcode/dstore_index_error_code.h"

#ifdef UT
#define private public
#define protected public
#endif

namespace DSTORE {

constexpr int PERCENTAGE_DIVIDER = 100;
/* Index options */
constexpr int16 INDEX_OPTION_DESC = 0x0001;             /* values are in reverse order */
constexpr int16 INDEX_OPTION_NULLS_FIRST = 0x0002;      /* NULLs are first instead of last */
/* Scan key flag: 16-31 bits. the upper 16 bits are conrresponding to index options (indoption[]) */

/* If an index tuple fail to match the scankey, the flag means it can stop the forward scan. */
constexpr uint32 SCANKEY_STOP_FORWARD = 0x00010000;
/* If an index tuple fail to match the scankey, the flag means it can stop the backward scan. */
constexpr uint32 SCANKEY_STOP_BACKWARD = 0x00020000;
/* scan key flag is uint32 while index option is int16, need to shift and clear the above bits first */
constexpr int16 SCANKEY_INDEX_OPTION_SHIFT = 24;
/* Is index rank desc? */
constexpr uint32 SCANKEY_DESC = INDEX_OPTION_DESC << SCANKEY_INDEX_OPTION_SHIFT;
/* Is Nulls rank first in index? */
constexpr uint32 SCANKEY_NULLS_FIRST = INDEX_OPTION_NULLS_FIRST << SCANKEY_INDEX_OPTION_SHIFT;
using StrategyNumber = uint16;

#define BTREEINT4CMP   351
#define BTREEINT8CMP   842
#define BTREEINT48CMP  2188
#define BTREEINT84CMP  2189

struct BtrStackData {
    ItemPointerData      currItem;
    struct BtrStackData *parentStack;

    static BtrStackData *SaveNewStack(const PageId currPageId, OffsetNumber currOffset, BtrStackData *higherLevelStack)
    {
        BtrStackData *newStack = static_cast<BtrStackData *>(DstorePalloc(sizeof(BtrStackData)));
        if (unlikely(newStack == nullptr)) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("DstorePalloc fail when SaveNewStack."));
            storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
            return nullptr;
        }
        newStack->currItem.SetPageId(currPageId);
        newStack->currItem.SetOffset(currOffset);
        newStack->parentStack = higherLevelStack;
        return newStack;
    }
};
using BtrStack = BtrStackData *;

struct BtrScanKeyValues {
    bool        cmpFastFlag;
    ItemPointerData heapCtid;                     /* tiebreaker for scankeys */
    uint16      keySize;                      /* Size of scankeys array */
    void       *values;                       /* all pass by value data */
    Datum       fastKeys[INDEX_MAX_KEY_NUM];
    ScanKeyData scankeys[INDEX_MAX_KEY_NUM];     /* Scankey are used to descend the tree */
    Oid         tableOid;                      /* partition oid for global pratition index */

    inline void Init()
    {
        cmpFastFlag = true;
        heapCtid = INVALID_ITEM_POINTER;
        keySize = 0;
        values = 0;
        tableOid = DSTORE_INVALID_OID;
        /* fastKeys and scankeys are not initialized here, which is protect by keySize */
    }
};

/* Help structure for gather all the info for one page */
struct BtreePagePayload;

class BtreeStorageMgr : public BaseObject {
public:
    explicit BtreeStorageMgr(PdbId pdbId, const int fillFactor, const RelationPersistence persistenceMethod);
    ~BtreeStorageMgr();

    bool IsBtrSmgrValid(BufMgrInterface *bufMgr, Xid *createdXid);
    static RetStatus InitBtrRecyclePartition(IndexSegment *segment, BufMgrInterface *bufMgr, Xid createdXid);

    RetStatus Init(const PageId segmentId, TablespaceId tablespaceId, DstoreMemoryContext context);

    BtrMeta *GetBtrMeta(LWLockMode access, BufferDesc **desc);

    RetStatus GetNewPage(BtreePagePayload &payload, uint8 tdcount = 0, Xid checkingXid = INVALID_XID);

    RetStatus PutIntoRecycleQueue(const RecyclablePage recyclablePage);
    RetStatus PutIntoFreeQueue(const PageId pageId);
    RetStatus GetFromRecycleQueue(PageId &recyclablePageId, CommitSeqNo minCsn, uint64 *numSkippedPage = nullptr);
    PageId GetFromFreeQueue(Xid checkingXid = INVALID_XID);
    RetStatus GetSlotFromFreeQueue(FreeQueueSlot &slot, const PageId emptyPage);
    RetStatus WriteSlotToFreeQueue(const FreeQueueSlot slot, const PageId emptyPage);

    inline PageId GetSegMetaPageId() const
    {
        return m_segMetaPageId;
    }
    inline PageId GetBtrMetaPageId() const
    {
        return m_btrMetaPageId;
    }
    inline TablespaceId GetTablespaceId() const
    {
        return m_segment->GetTablespaceId();
    }
    inline uint64 GetIndexBlockCount() const
    {
        return m_segment->GetDataBlockCount();
    }
    inline PageId GetLastInsertionPageId() const
    {
        return m_lastInsertionPageId;
    }
    inline void SetLastInsertionPageId(const PageId pageId)
    {
        m_lastInsertionPageId = pageId;
    }
    inline bool HasMetaCache() const
    {
        return m_btrMetaCache != nullptr;
    }
    inline PageId GetRootPageIdFromMetaCache() const
    {
        if (m_btrMetaCache == nullptr) {
            return INVALID_PAGE_ID;
        }
        return m_btrMetaCache->GetRootPageId();
    }
    inline BufferDesc *GetLowestSinglePageDescFromCache() const
    {
        return m_lowestSingleDescCache;
    }
    inline PageId GetLowestSinglePageIdFromMetaCache() const
    {
        if (m_btrMetaCache == nullptr) {
            return INVALID_PAGE_ID;
        }
        return m_btrMetaCache->GetLowestSinglePage();
    }
    inline uint32 GetRootLevelFromMetaCache() const
    {
        if (unlikely(m_btrMetaCache == nullptr)) {
            return BTREE_HIGHEST_LEVEL;
        }
        return m_btrMetaCache->GetRootLevel();
    }
    inline uint32 GetLowestSingleLevelFromMetaCache() const
    {
        if (unlikely(m_btrMetaCache == nullptr)) {
            return BTREE_HIGHEST_LEVEL;
        }
        return m_btrMetaCache->GetLowestSinglePageLevel();
    }
    inline RetStatus SetMetaCache(BtrMeta *btrMeta)
    {
        ReleaseMetaCache();
        if (STORAGE_VAR_NULL(m_ctx)) {
            storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("SetMetaCache m_ctx is nullptr."));
            return DSTORE_FAIL;
        }
        m_btrMetaCache = static_cast<BtrMeta *>(DstoreMemoryContextAlloc(m_ctx, sizeof(BtrMeta)));
        if (STORAGE_VAR_NULL(m_btrMetaCache)) {
            storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Failed to allocate size %lu.", sizeof(BtrMeta)));
            return DSTORE_FAIL;
        }
        error_t rc = memcpy_s(m_btrMetaCache, sizeof(BtrMeta), btrMeta, sizeof(BtrMeta));
        storage_securec_check(rc, "\0", "\0");
        return DSTORE_SUCC;
    }
    inline void UpdateLowestSinglePageCache(BufferDesc *bufDesc)
    {
        m_lowestSingleDescCache = bufDesc;
    }
    inline void ReleaseLowestSinglePageCache()
    {
        m_lowestSingleDescCache = nullptr;
    }
    inline void ReleaseMetaCache() noexcept
    {
        if (m_btrMetaCache == nullptr) {
            return;
        }
        DstorePfree(m_btrMetaCache);
        m_btrMetaCache = nullptr;
    }
#ifdef UT
    inline void SetMetaCacheLowestSinglePageId(PageId pageId)
    {
        m_btrMetaCache->lowestSinglePage = pageId;
    }
#endif
    inline int GetFillFactor() const
    {
        return m_fillFactor;
    }
    inline void SetFillFactor(int fillFactor)
    {
        m_fillFactor = fillFactor;
    }
    inline RelationPersistence GetPersistenceMethod() const
    {
        return m_persistenceMethod;
    }
    inline bool IsGlobalTempIndex() const
    {
        return m_persistenceMethod == SYS_RELPERSISTENCE_GLOBAL_TEMP;
    }
    inline IndexSegment* GetSegment()
    {
        return m_segment;
    }

    inline void SetBuildingXid(Xid creatingXid)
    {
        StorageAssert(m_btrMetaCache == nullptr);
        m_buildingXid = creatingXid;
    }

    inline Xid GetMetaCreateXid()
    {
        if (unlikely(m_buildingXid != INVALID_XID)) {
            return m_buildingXid;
        }
        if (unlikely(m_btrMetaCache == nullptr)) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX,
                ErrMsg("m_btrMetaCache null metaPageId(%hu, %u)", m_segMetaPageId.m_fileId, m_segMetaPageId.m_blockId));
            BufferDesc *btrMetaBuf = INVALID_BUFFER_DESC;
            BtrMeta *btrMeta = GetBtrMeta(LW_SHARED, &btrMetaBuf);
            StorageReleasePanic(btrMeta == nullptr, MODULE_INDEX, ErrMsg("Can not get btrMeta when GetMetaCreateXid."));
            Xid metaCreateXid = btrMeta->GetCreateXid();
            /* We don't care if meta cache set successful or not for now. */
            (void)SetMetaCache(btrMeta);
            BufMgrInterface *bufMgr =
                IsGlobalTempIndex() ? thrd->GetTmpLocalBufMgr() : g_storageInstance->GetBufferMgr();
            bufMgr->UnlockAndRelease(btrMetaBuf);
            PrintBackTrace();
            return metaCreateXid;
        }
        return m_btrMetaCache->GetCreateXid();
    }

    void RecordBtreeOperInLevel(BtreeOperType type, uint32 level);

#ifdef DSTORE_COLLECT_INDEX_RECYCLE_INFO
    void SetRecycleFailReason(BtrPageRecycleFailReason recycleFailReasonCode);
#endif

    IndexSegment         *m_segment;
    PageId                m_segMetaPageId;
    BufferDesc           *m_lowestSingleDescCache;
    PdbId   m_pdbId;
protected:
    BtreeStorageMgr(const BtreeStorageMgr &) = delete;
    BtreeStorageMgr &operator=(const BtreeStorageMgr &) = delete;
private:
    PageId m_btrMetaPageId;
    PageId m_lastInsertionPageId;
    BtrMeta *m_btrMetaCache;
    int m_fillFactor;
    const RelationPersistence m_persistenceMethod;
    DstoreMemoryContext m_ctx;
    Xid m_buildingXid; /* Only used when btree is under building, or has no root. Btree does not have a valid
                        * BtreeMeta in these cases to support the CreatedXid getting */
};

class Btree : public BaseObject {
public:
    Btree() = delete;
    explicit Btree(StorageRelation indexRel);
    Btree(StorageRelation indexRel, IndexInfo *indexInfo);
    Btree(StorageRelation indexRel, IndexInfo *indexInfo, ScanKey scanKeyInfo);
    virtual ~Btree();

    static BufferDesc *ReadAndCheckBtrPage(
        const PageId pageId, LWLockMode access, BufMgrInterface *bufMgr, PdbId pdbId, bool checkBtrPage = true);

    static IndexInfo *CopyIndexInfo(IndexInfo *oldInfo);

    /* Copy multiple ScanKeyData */
    static ScanKeyData *CopyScanKeys(ScanKeyData* scanKeys, uint16_t nKeys);

    static void DumpDamagedTuple(IndexTuple *tuple, BtrPage *page = nullptr,
                                 OffsetNumber offset = INVALID_ITEM_OFFSET_NUMBER);
    RetStatus GetFirstLeafPage(PageId &firstLeafPageId);

    inline BtreeStorageMgr *GetBtreeSmgr() const
    {
        return m_indexRel->btreeSmgr;
    }

    inline virtual PdbId GetPdbId()
    {
        return (m_indexRel != nullptr) ? m_indexRel->m_pdbId : INVALID_PDB_ID;
    }

    uint64                m_btrMagicNum;
    StorageRelation       m_indexRel;
    IndexInfo            *m_indexInfo;
    BtrScanKeyValues      m_scanKeyValues;

protected:
    void SetScanKeyWithVals(BtrScanKeyValues *scanKeyValues);

    BufferDesc *ReleaseOldGetNewBuf(BufferDesc *old, const PageId newPage, LWLockMode access, bool needCheck = true);

    IndexTuple *CreateTruncateInternalTuple(IndexTuple *left, IndexTuple *right, bool needTruncateHikey = true);
    int32 CompareKeyToTuple(BtrPage *btrPage, BtrPageLinkAndStatus *linkAndStatus, OffsetNumber offsetNumber,
                            bool isInternalPage);

    RetStatus GetRoot(BufferDesc **bufDesc, bool forUpdate);
    virtual RetStatus SearchBtree(BufferDesc **leafBuf, bool strictlyGreaterThanKey, bool forceUpdate,
                                  bool needWriteLock, bool needCheckCreatedXid);
    virtual RetStatus StepRightIfNeeded(BufferDesc **buf, LWLockMode access, bool strictlyGreaterThanKey,
                                        bool needCheckCreatedXid);

    void InitScanKey(ScanKey scanKeyInfo);
    bool UpdateScanKeyWithValues(IndexTuple *comparingKeyTuple);
    OffsetNumber BinarySearchOnPage(BtrPage *btrPage, bool strictlyGreaterThanKey = false);
    void GenerateAllocTdWal(BufferDesc *bufferDesc, TDAllocContext &tdContext);
    void GenerateRollbackTdWal(BufferDesc *bufferDesc, TDAllocContext &tdContext);

    bool IsGlobalIndex() const
    {
        return m_indexInfo->relKind == static_cast<char>(SYS_RELKIND_GLOBAL_INDEX);
    }

    bool NeedWal()
    {
        return (!(m_indexRel && GetBtreeSmgr() && GetBtreeSmgr()->IsGlobalTempIndex()));
    }

    BufMgrInterface      *m_bufMgr;

private:
    RetStatus GetRootFromMetaCache(BufferDesc **bufDesc);
    RetStatus CreateFirstNewRoot(BufferDesc *btrMetaBuf, BufferDesc **newRootBuf);
    void GenerateNewLeafRootWal(BufferDesc *leafRootBuf, BufferDesc *btrMetaBuf);

    int CompareNIntKeyWithoutNulls(IndexTuple *cmpTuple, int numCmpAtts);
    int CompareNKeys(IndexTuple *cmpTuple, int numCmpAtts);

    void PinBuffer(BufferDesc *bufDesc)
    {
        if (unlikely(GetBtreeSmgr()->IsGlobalTempIndex())) {
            bufDesc->Pin<true>();
        } else {
            bufDesc->Pin<false>();
        }
    }
    void UnpinBuffer(BufferDesc *bufDesc)
    {
        if (unlikely(GetBtreeSmgr()->IsGlobalTempIndex())) {
            bufDesc->Unpin<true>();
        } else {
            bufDesc->Unpin<false>();
        }
    }
};

/* Help structure for gather all the info for one page */
struct BtreePagePayload {
    PageId pageId;
    BufferDesc *buffDesc;
    BtrPage *page;
    BtrPageLinkAndStatus *linkAndStatus;

    BtreePagePayload() : pageId(INVALID_PAGE_ID), buffDesc(nullptr), page(nullptr), linkAndStatus(nullptr)
    {}

    inline BtrPageLinkAndStatus *GetLinkAndStatus()
    {
        return linkAndStatus;
    }

    inline BtrPage *GetPage()
    {
        return page;
    }

    inline BufferDesc *GetBuffDesc()
    {
        return buffDesc;
    }

    inline PageId GetPageId() const
    {
        return pageId;
    }

    inline RetStatus Init(PdbId pdbId, const PageId btrPageId, const LWLockMode mode, BufMgrInterface *bufMgr,
                     bool needCheckBtrPage = true)
    {
        BufferDesc *buf = Btree::ReadAndCheckBtrPage(btrPageId, mode, bufMgr, pdbId, needCheckBtrPage);
        if (STORAGE_VAR_NULL(buf)) {
            return DSTORE_FAIL;
        }
        InitByBuffDesc(buf);
        return DSTORE_SUCC;
    }

    inline void InitByBuffDesc(BufferDesc *srcBuffDesc)
    {
        if (unlikely(srcBuffDesc == INVALID_BUFFER_DESC)) {
            PrintBackTrace();
            ErrLog(DSTORE_PANIC, MODULE_INDEX, ErrMsg("srcBuffDesc is nullptr when InitByBuffDesc"));
            return;
        }
        buffDesc = srcBuffDesc;
        pageId = buffDesc->GetPageId();
        page = static_cast<BtrPage *>(buffDesc->GetPage());
        linkAndStatus = page->GetLinkAndStatus();
    }

    inline void Drop(BufMgrInterface *bufMgr, bool checkCrc = true)
    {
        if (buffDesc != INVALID_BUFFER_DESC) {
            bufMgr->UnlockAndRelease(
                buffDesc, checkCrc ? BufferPoolUnlockContentFlag() : BufferPoolUnlockContentFlag::DontCheckCrc());
        }
        pageId = INVALID_PAGE_ID;
        buffDesc = INVALID_BUFFER_DESC;
        page = nullptr;
        linkAndStatus = nullptr;
    }
};

}

#endif /* SRC_GAUSSKERNEL_INCLUDE_INDEX_STORAGE_BTREE_H */
