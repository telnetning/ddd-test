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
 * dstore_partition_fsm.h
 *     This file defines the building blocks that bg page writer needs.
 *
 * IDENTIFICATION
 *        include/fsm/dstore_partition_fsm.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_PARTITION_FSM_H
#define DSTORE_PARTITION_FSM_H

#include "diagnose/dstore_heap_diagnose.h"
#include "framework/dstore_diagnose_iterator.h"
#include "page/dstore_fsm_page.h"
#include "page/dstore_segment_meta_page.h"
#include "buffer/dstore_buf_mgr.h"
#include "page/dstore_heap_segment_meta_page.h"
#include "tablespace/dstore_tablespace.h"

namespace DSTORE {

/* the maximum number of retries to do FSM search before extending it */
constexpr uint16 MAX_FSM_SEARCH_RETRY_TIME = 1000;
constexpr uint16 FSM_SEARCH_UPGRADE_RETRY_TIME = 100;

struct FsmUpdateSpaceContext {
    FsmIndex fsmIndex;
    uint32 space;
    bool *fsmIsChanged;
    uint16 retryTimes;
    uint32 spaceNeeded;
    PageId *pageId;
    BufMgrInterface *bufMgr;
    PageId *fsmMetaPageId;
    uint32* spaceInFsm;
    bool needWal;
    PdbId pdbId;
} PACKED;

STATIC_ASSERT_TRIVIAL(FsmUpdateSpaceContext);

/* Statistic structure used when try to recycle unused FSM or reassign FSMs */
struct NodeFsmInfo {
    NodeId nodeId;
    uint64 numTotalPages;
    uint64 numFsms;

    static int NumTotalPagesComparator(Datum a, Datum b, void *arg)
    {
        (void)arg;
        NodeFsmInfo *infoA = static_cast<NodeFsmInfo *>(static_cast<void *>(DatumGetPointer(a)));
        NodeFsmInfo *infoB = static_cast<NodeFsmInfo *>(static_cast<void *>(DatumGetPointer(b)));

        /* we want a max-heap, so return 1 when a > b. */
        if (infoA->numTotalPages > infoB->numTotalPages) {
            return 1;
        } else if (infoA->numTotalPages == infoB->numTotalPages) {
            return 0;
        } else {
            return -1;
        }
    }
};

struct RecycleFsmInfo {
    FsmInfo fsmInfo;
    uint16 fsmId;
    uint64 numTotalPages;

    static int Comparator(Datum a, Datum b, void *arg)
    {
        (void)arg;
        RecycleFsmInfo *infoA = static_cast<RecycleFsmInfo *>(static_cast<void *>(DatumGetPointer(a)));
        RecycleFsmInfo *infoB = static_cast<RecycleFsmInfo *>(static_cast<void *>(DatumGetPointer(b)));

        /* we want a max-heap, so return 1 when a > b. */
        if (infoA->numTotalPages > infoB->numTotalPages) {
            return 1;
        } else if (infoA->numTotalPages == infoB->numTotalPages) {
            return 0;
        } else {
            return -1;
        }
    }
};

/**
 * In default, BLCKSZ = 8K
 * fsm list0 range 0
 * fsm list1 range (0, 64]
 * fsm list2 range (64, 128]
 * fsm list3 range (128, 256]
 * fsm list4 range (256, 512]
 * fsm list5 range (512, 1024]
 * fsm list6 range (1024, 2048]
 * fsm list7 range (2048, 4096]
 * fsm list8 range (4096, 8192]
 */
class PartitionFreeSpaceMap : public BaseObject {
public:
    PdbId m_pdbId;

    PartitionFreeSpaceMap(const PageId segMetaPageId, const PageId fsmMetaPageId, BufMgrInterface *bufMgr,
                          const TablespaceId tablespaceId, PdbId pdbId);
    PageId GetPage(PageId heapSegMetaPageId, uint32 space, uint16 retryTime, uint32 *spaceInFsm,
                   bool *needExtensionTask);
    static void SearchPageIdOfChild(FsmPage *fsmPage, uint32 spaceNeeded, PageId *pageId, uint16 retryTimes,
                                    uint32 *spaceInfsm);
    static RetStatus UpdateSpace(FsmUpdateSpaceContext context);

    static bool IsInSameFreeList(uint16 size1, uint16 size2)
    {
        return GetListId(size1) == GetListId(size2);
    }
    BufferDesc *ReadFsmMetaPageBuf(LWLockMode mode)
    {
        BufferDesc *fsmMetaPageBuf = m_bufMgr->Read(m_pdbId, m_fsmMetaPageId, mode);
        STORAGE_CHECK_BUFFER_PANIC(fsmMetaPageBuf, MODULE_SEGMENT, m_fsmMetaPageId);
        return fsmMetaPageBuf;
    }
    void UnlockAndReleaseFsmMetaPageBuf(BufferDesc *&fsmMetaBuf)
    {
        m_bufMgr->UnlockAndRelease(fsmMetaBuf);
        fsmMetaBuf = nullptr;
    }

    PageId GetFsmMetaPageId()
    {
        return m_fsmMetaPageId;
    }

    static uint16 GetListId(uint16 space)
    {
        uint16 resList = 0;
        for (uint16 list = 0; list < FSM_FREE_LIST_COUNT; ++list) {
            if (space <= FSM_SPACE_LINE[list]) {
                resList = list;
                break;
            }
        }
        return resList;
    }

    bool HasEnoughUnusedPages(uint64 pageCount);
    uint16 GetExtendCoefficient();

    RetStatus GetFsmStatus(uint16 *freeSlotCount, uint16 *needExtendPageCount);
    RetStatus AdjustFsmTree(uint16 needExtendPageCount);
    RetStatus AddMultipleNewPageToFsm(const PageId &leafFsmPageId, uint16 pageCount, DSTORE::PageId *pageIdList);
    RetStatus GetFirstFreeFsmIndex(const PageId &fsmPageId, FsmIndex *firstFreeFsmIndex);
    void UpdateFsmStatAfterExtend(uint64 pageCount, bool needUpdateExtendCoeff);
    RetStatus ConditionalUpdateFsmAccessTimestamp();
    RetStatus UpdateNumUsedPagesIfNeeded(const PageId &pageId);
    static void UpdateFsmAccessTimestamp(BufferDesc *fsmMetaPageBuf, BufMgrInterface *bufMgr);

#ifdef UT
    static RetStatus MarkFreeSpaceMapExpired(BufMgrInterface *bufMgr, PageId fsmMetaPageId, PdbId pdbId);
#endif
private:
    PageId m_segMetaPageId;
    PageId m_fsmMetaPageId;
    TablespaceId m_tablespaceId;
    BufMgrInterface *m_bufMgr;

    HeapSegmentMetaPage *GetSegMetaPage(BufferDesc *segMetaBuf)
    {
        if (!IsTemporaryTableFsm()) {
            StorageAssert(segMetaBuf->IsHeldContentLockByMe() ||
                          (thrd->GetPrivateBuffer(segMetaBuf->GetBufferTag()) != nullptr));
        }
        return static_cast<HeapSegmentMetaPage *>(segMetaBuf->GetPage());
    }
    FreeSpaceMapMetaPage *GetFsmMetaPage(BufferDesc *fsmMetaBuf)
    {
        StorageAssert(fsmMetaBuf != nullptr);
        if (!IsTemporaryTableFsm()) {
            StorageAssert(fsmMetaBuf->IsHeldContentLockByMe() ||
                          (thrd->GetPrivateBuffer(fsmMetaBuf->GetBufferTag()) != nullptr));
        }
        return static_cast<FreeSpaceMapMetaPage *>(fsmMetaBuf->GetPage());
    }

    static RetStatus UpdateFsmPage(FsmUpdateSpaceContext context);
    static FsmNode *SearchChild(uint32 space, FsmPage *currentFsmPage, uint16 retryTime);
    PageId GetPageInternal(uint32 space, uint16 retryTime, uint16 currentLevel, PageId currentFsmPageId,
                           uint32 *spaceInFsm);
    bool NeedExtensionTask(PageId heapSegMetaPageId);
    bool NeedRecycleFsmTask();

    static uint16 SelectSlotIndex(uint16 fitSlotCount, uint16 searchSeed);
    PageId GetNewFsmPage(FreeSpaceMapMetaPage *fsmMetaPage);
    RetStatus AdjustFsmTreeWithNewRoot(FreeSpaceMapMetaPage *fsmMetaPage, PageId newFsmPageIdList[5], uint16 pageCount);
    RetStatus AdjustFsmTreeWithOldRoot(FreeSpaceMapMetaPage *fsmMetaPage, PageId newFsmPageIdList[5], uint16 pageCount);
    void UpdateFsmTree(uint16 needExtendPageCount, PageId *pageIdList, BufferDesc *fsmMetaBuf);
    void UpdateUpperFsmIndex(BufferDesc *fsmDescBuf, const FsmIndex &upperFsmIndex);
    RetStatus InitFsmPage(BufferDesc *fsmBuf, const FsmIndex &upperFsmIndex, uint16 addPageCount,
                          PageId *addPageList = nullptr, uint16 *addListId = nullptr);
    RetStatus UpdateSlotFreeSpace(const PageId &fsmPageId, uint16 slotId, uint16 newListId);
    bool NeedWal()
    {
        return (m_tablespaceId != static_cast<TablespaceId>(TBS_ID::TEMP_TABLE_SPACE_ID));
    }
    bool IsTemporaryTableFsm()
    {
        return (m_tablespaceId == static_cast<TablespaceId>(TBS_ID::TEMP_TABLE_SPACE_ID));
    }
};

class FreeSpaceMapNode : public BaseObject {
public:
    PartitionFreeSpaceMap *m_fsm;
    dlist_node m_node;

    explicit FreeSpaceMapNode(PartitionFreeSpaceMap *fsm);
    ~FreeSpaceMapNode();
    DISALLOW_COPY_AND_MOVE(FreeSpaceMapNode);

    inline bool IsCurrentFsm(const PageId fsmMetaPageId) const
    {
        return (fsmMetaPageId == m_fsm->GetFsmMetaPageId());
    }
};

class FreeSpaceMapList : public BaseObject {
public:
    dlist_head m_list;

    FreeSpaceMapList();
    ~FreeSpaceMapList();

    /* Return the first usable PartitionFreeSpaceMap */
    FreeSpaceMapNode *GetFreeSpaceMapForSpace();

    /* Return the PartitionFreeSpaceMap based on given PageId */
    FreeSpaceMapNode *SearchFreeSpaceMap(const PageId fsmMetaPageId);

    /* Move a given FSM to the end of list */
    void MoveFreeSpaceMapToEnd(FreeSpaceMapNode *fsmNode);

    /* Move a given FSM to the front of the list */
    void MoveFreeSpaceMapToFront(FreeSpaceMapNode *fsmNode);

    /* We got new PartitionFreeSpaceMap assigned to the current node. Load its information */
    RetStatus LoadNewFreeSpaceMap(const PageId segMetaPageId, const PageId fsmMetaPageId, BufMgrInterface *bufMgr,
                                  const TablespaceId tablespaceId, PdbId pdbId);
};

class FreeSpaceMapDiagnose : public AbstractDiagnoseIterator {
public:
    FreeSpaceMapDiagnose(BufMgrInterface *bufMgr, PdbId pdbId, PageId fsmMetaPageId);
    bool Init();
    void Bind(FsmPage *fsmPageCache, PageFreespace *pfs);

    bool Begin() override;
    bool HasNext() override;
    DiagnoseItem *GetNext() override;
    void End() override;
    uint8 GetFsmLevel() { return m_numFsmLevels; };

private:
    bool CacheOneFsmPage(PageId fsmPageId, uint8 cacheIdx);

private:
    BufMgrInterface *m_bufMgr;
    PdbId m_pdbId;
    PageId m_fsmMetaPageId;         /* PageId of the FSM metaPage */

    PageId m_firstFsmRootPageId;    /* the left-most leaf pageId */
    uint8 m_numFsmLevels;           /* total number level of the fsm */
    uint8 m_curFsmLevel;            /* level of the traversed FSM's subtree */
    uint16 m_slotId;                /* next slot of the latest traversed fsm page */
    FsmPage *m_cachedFsmPages;      /* fsm pages cache, to reduce reading of non-leaf page  */
    PageFreespace *m_pfs;           /* storage space for the fetch result */
};
} /* namespace DSTORE */

#endif  // DSTORE_PARTITION_FSM_H
