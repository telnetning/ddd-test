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
 * dstore_heap_segment.h
 *
 * IDENTIFICATION
 *        include/tablespace/dstore_heap_segment.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_HEAP_SEGMENT_H
#define DSTORE_HEAP_SEGMENT_H

#include "diagnose/dstore_heap_diagnose.h"
#include "framework/dstore_diagnose_iterator.h"
#include "fsm/dstore_partition_fsm.h"
#include "page/dstore_fsm_page.h"
#include "tablespace/dstore_data_segment.h"

namespace DSTORE {

constexpr uint16 INIT_FSM_NEED_PAGE_COUNT = 2;

struct HeapRecycleFsmContext {
    /* Segment meta page info */
    BufferDesc *segMetaPageBuf = nullptr;
    HeapSegmentMetaPage *segMetaPage = nullptr;

    /* Recyclable FSMs info */
    RecycleFsmInfo recycleFsmInfo[MAX_FSM_TREE_PER_RELATION];
    int numRecycleFsm = 0;
    uint64 numTotalPagesSumForRecycleFsm = 0;
    bool needRecycle[MAX_FSM_TREE_PER_RELATION] = {0};
    bool allFsmToSameNode = true;

    /* Node FSM assignment info */
    NodeFsmInfo nodeFsmInfo[MAX_FSM_TREE_PER_RELATION];
    int numNodeFsmInfo = 0;
    uint64 numTotalPagesSumForNodes = 0;

    /* binary heap used to sort FSM and Node info */
    binaryheap *recycleFsmInfoHeap = nullptr;
    binaryheap *nodeFsmInfoHeap = nullptr;
};

class HeapSegment : public DataSegment {
public:
    /**
     * Construct a instance to read a existing heap segment, must call Init() to use Segment functions.
     * @param segmentId First PageId of Segment
     * @param tablespace TableSpace instance of the Segment
     * @param bufMgr Buffer manager instance
     * @param ctx DstoreMemoryContext used for internal allocation
     */
    HeapSegment(PdbId pdbId, const PageId &segmentId, TablespaceId tablespaceId, BufMgrInterface *bufMgr,
                DstoreMemoryContext ctx = nullptr, SegmentType segmentType = SegmentType::HEAP_SEGMENT_TYPE)
        : DataSegment(pdbId, segmentId, segmentType, tablespaceId, bufMgr, ctx),
          m_fsmList(nullptr),
          m_isFsmInitialized(false)
    {}

    ~HeapSegment() override;

    DISALLOW_COPY_AND_MOVE(HeapSegment);

    /**
     * Alloc new heap segment from tablespace
     * @param tablespace TableSpace instance of the Segment
     * @param bufMgr Buffer manager instance
     * @return HeapSegment instance
     */
    static RetStatus InitHeapSegMetaInfo(
        PdbId pdbId, BufMgrInterface *bufMgr, SegmentType segType, PageId segMetaPageId, bool isReUsedFlag);

    /**
     * Init new fsm tree for a segment
     * @param fsmMetaPageId PageId of fsmMetaPage
     * @param fsmRootPageId PageId of fsmRootPage
     * @return  0 means success, other means failure
     */
    static RetStatus InitNewFsmTree(
        PdbId pdbId, const PageId &fsmMetaPageId, const PageId &fsmRootPageId, BufMgrInterface *bufMgr, bool needWal);

    /**
     * Call after HeapSegment constructor to get segment head buffer (do not release)
     * @return DSTORE_SUCC if Segment is ready, or DSTORE_FAIL if something wrong
     */
    RetStatus InitSegment() final;

    /**
     * Delete all data of this heap segment in storage
     * @return 0 means success, other means failure
     */
    RetStatus DropSegment() override;

    /**
     * Get a data page from FSM in data segment with {spaceNeeded} free space
     * @param spaceNeeded space needed from table
     * @param retryTime used to increase space search level
     * @return PageId if found in FSM with required space, INVALID_PAGE_ID if no page matched
     */
    PageId GetPageFromFsm(uint32 spaceNeeded, uint16 retryTime);
    PageId GetPageFromFsm(uint32 spaceNeeded, uint16 retryTime, uint32 *spaceInFsm);

    /**
     * Get a new page from data segment, DataSegment will add >=1 new pages to FSM
     * @param fsmMetaPageId PartitionFreeSpaceMap that current extension should happens on
     * @return one PageId of new pages, INVALID_PAGE_ID if DataSegment cannot get one new page
     */
    PageId GetNewPage(const PageId fsmMetaPageId = INVALID_PAGE_ID);

    /**
     * Update a page free space in FSM
     * @param dataPageId data page id
     * @param remainSpace remain space of {pageId}
     * @return 0 means success, other means failure
     */
    RetStatus UpdateFsm(const PageId &dataPageId, uint32 remainSpace);

    /**
     * Update a page free space in FSM
     * @param dataPageId data page id
     * @param remainSpace remain space of {pageId}
     * @return 0 means success, other means failure
     */
    RetStatus UpdateFsm(const FsmIndex &fsmIndex, uint32 remainSpace);
    RetStatus UpdateFsmAndSearch(const FsmIndex &fsmIndex, uint32 remainSpace, uint32 spaceNeeded, uint16 retryTimes,
                                 PageId *pageId, uint32* spaceInfsm);

    /**
     * Recycle unused PartitionFreeSpaceMap and reassign them to other nodes
     * @param isBackground if this function is called by a background thread
     * @return 0 means success, other means failure
     */
    RetStatus RecycleUnusedFsm();

    bool NeedUpdateFsm(uint32 prevSize, uint32 curSize) const;
    RetStatus GetFsmPageIds(PageId **pageIds, Size *length, char **errInfo);
    void GetFsmMetaPageId(uint32 fsmId, PageId &pageId, NodeId &nodeId);
    /* HeapSegment Drop functions */
    RetStatus DropHeapSegmentInternal();
    virtual RetStatus InitNewDataPageWithFsmIndex(uint16 pageCount, PageId *pageIdList, BufferDesc **pageBufList,
        FsmIndex *fsmIndexList, bool pagesIsReused);
#ifdef UT
    RetStatus UtAllocNewFsmTree(NodeId assignedNodeId, bool fsmExpired);
    bool IsFsmInitInitialized();
#endif

    DiagnoseIterator *HeapFsmScan();

private:
    /* FSM related functions */
    RetStatus InitFreeSpaceMap();
    RetStatus InitFreeSpaceMapInternal(PageId &fsmMetaPageId);
    void FindFsmForCurrentNode(PageId *fsmMetaPageIds, int &numFsm);
    RetStatus InitFreeSpaceMapList(PageId *fsmMetaPageIds, int numFsm);
    RetStatus AllocateNewFsmTree(PageId &fsmMetaPageId);
    RetStatus AllocateNewFsmTree(NodeId assignedNodeId, PageId &fsmMetaPageId);
    void CollectNumFsmsForEachNode(HeapSegmentMetaPage *segMetaPage, NodeFsmInfo *nodeFsmInfo,
                                   int &numNodeFsmInfo) const;
    RetStatus ReassignColdestFsmFromNode(BufferDesc *segMetaBuf, NodeFsmInfo *candidateNode, uint16 &candidateFsmId);
    RetStatus ReuseExistingFsmTree(PageId &fsmMetaPageId);
    RetStatus GetNewPagesForFSM(PageId *pageIdList, uint16 targetPageCount);
    RetStatus UnlinkFsmExtent(const PageId &newNextExtMetaPageId, const PageId &curFsmMetaPageId);
    RetStatus FreeFsmExtents(const PageId &firstFsmPageId, const PageId &curFsmMetaPageId, const PageId &usedFsmPageId);
    RetStatus ExtendFsmPages(PartitionFreeSpaceMap *fsm, uint16 needExtendPageCount);
    RetStatus GetNewFsmExtent(PartitionFreeSpaceMap *fsm, const PageId &lastFsmExtMetaPageId);
    RetStatus DataSegMetaLinkFsmExtent(PartitionFreeSpaceMap *fsm, const PageId &newExtMetaPageId, ExtentSize extSize);
    RetStatus CheckAndInitFreeSpaceMap();
    RetStatus UpdateFsmInternal(FsmUpdateSpaceContext context);
    RetStatus RecycleUnusedFsmInternal(HeapRecycleFsmContext &context);
    RetStatus FindRecyclableFsm();
    binaryheap *OrderRecyclableFsmsBasedOnNumTotalPages(HeapRecycleFsmContext &context) const;
    binaryheap *OrderNodesBasedOnNumTotalPages(HeapRecycleFsmContext &context);
    RetStatus ReassignRecyclableFsm(HeapRecycleFsmContext &context);

    void GenerateWalForAllocNewFsmTree(BufferDesc *segMetaPageBuf, uint16 fsmId);
    void GenerateWalForRecycleFsmTree(BufferDesc *segMetaPageBuf, uint16 fsmId);

    /* HeapSegment Extension functions */
    RetStatus GetNewPageInternal(PartitionFreeSpaceMap *fsm, PageId *targetPageId, bool isBgExtension);
    RetStatus PrepareFreeSlots(PartitionFreeSpaceMap *fsm, uint16 *freeSlotCount);
    RetStatus GetFreeFsmIndex(PartitionFreeSpaceMap *fsm, uint16 pageCount, FsmIndex *fsmIndexList);
    RetStatus AddDataPagesToFsm(PartitionFreeSpaceMap *fsm, PageId *addPageList,
                                uint16 addPageCount, bool needUpdateExtendCoeff, bool pagesIsReused);

    /* We need to update the amount of Pages extended based on how many extension has been done on the SegmentMetaPage;
     needUpdateExtendCoeff is introduced to decide if we need extend more pages each time */
    RetStatus AddDataPagesToMetaPages(uint16 slotCount, uint16 dataPageCount, bool isBgExtension,
                                      BufferDesc *segMetaPageBuf, PageId *addPageList, uint16 &addPageCount,
                                      bool &needUpdateExtendCoeff);
    bool NeedWal()
    {
        return (!IsTempSegment());
    }
    FreeSpaceMapList *m_fsmList;
    bool m_isFsmInitialized;
};

class HeapFreespaceDiagnose : public AbstractDiagnoseIterator {
public:
    HeapFreespaceDiagnose(BufMgrInterface *bufMgr, PdbId pdbId, uint16 numFsms);
    bool AddOneFsmTreeTask(PageId fsmMetaPageId);
    bool Init();

    bool Begin() override;
    bool HasNext() override;
    DiagnoseItem *GetNext() override;
    void End() override;

private:
    BufMgrInterface *m_bufMgr;
    PdbId m_pdbId;

    uint16 m_numFsmTree;            /* how many fsmtree to be processed */
    uint16 m_curFsmTree;            /* start with 0, which fsmtree is being processed. */
    FreeSpaceMapDiagnose **m_fsmDiags;  /* iterator scan handler for each FSM Tree */
    FsmPage *m_cachedFsmPages;      /* size is m_maxFsmLevel, and [0, m_curFsmLevel] is used. */
    PageFreespace pfs;              /* storage space for the fetch result */
};

} /* namespace DSTORE */
#endif  /* DSTORE_HEAP_SEGMENT_H */
