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
 * dstore_index_segment.h
 *
 * IDENTIFICATION
 *        include/tablespace/dstore_index_segment.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_INDEX_SEGMENT_H
#define DSTORE_INDEX_SEGMENT_H

#include "page/dstore_btr_queue_page.h"
#include "tablespace/dstore_data_segment.h"
#include "lock/dstore_lock_mgr.h"

namespace DSTORE {
/* Statically allocate two pages, BtrMeta and RecycleRootMeta page. */
constexpr uint16 NUM_BTR_META_PAGE = 1;
constexpr uint16 NUM_RECYCLE_ROOT_META_PAGE = 1;

struct BtrRecyclePartReclaimContext {
    /* Recyclable partition */
    PageId coldPartitionMetaPageIds;
    NodeId coldPartitionNodeIds;
};

struct BtreeRecyclePartition;
class IndexSegment : public DataSegment {
public:
    /**
     * Construct a instance to read a existing index segment, must call Init() to use Segment functions.
     * @param segmentId First PageId of Segment
     * @param tablespace TableSpace instance of the Segment
     * @param bufMgr Buffer manager instance
     * @param ctx DstoreMemoryContext for internal allocation
     */
    IndexSegment(PdbId pdbId, const PageId &segmentId, TablespaceId tablespaceId, BufMgrInterface *bufMgr,
        DstoreMemoryContext ctx = nullptr, SegmentType segmentType = SegmentType::INDEX_SEGMENT_TYPE);
    ~IndexSegment() override;

    DISALLOW_COPY_AND_MOVE(IndexSegment);

    /**
     * Get a new page from data segment, DataSegment will add >=1 new pages to FSM
     * @return one PageId of new pages, INVALID_PAGE_ID if DataSegment cannot get one new page
     */
    virtual PageId GetNewPage(bool isExtendBg = false);
    virtual PageId GetNewPageFromUnassignedPages();
    /**
     * Push pages into Recycle Queue
     * @param recyclablePage csn and PageId of destinated page
     */
    RetStatus PutIntoRecycleQueue(const RecyclablePage recyclablePage);

    /**
     * Push pages into Free Queue
     * @param pageId PageId of destinated page
     */
    RetStatus PutIntoFreeQueue(const PageId pageId);

    /**
     * Allocate a unused slot from Free Queue
     * @param emptyPage pageId of the unlink page
     * @return FreeQueueSlot location of usable slot
     */
    RetStatus GetSlotFromFreeQueue(FreeQueueSlot &slot, const PageId emptyPage);

    /**
     * Write to a prefectehd Slot
     * @param FreeQueueSlot location of slot
     * @param reusablePage information of destinated page
     */
    RetStatus WriteSlotToFreeQueue(const FreeQueueSlot slot, const ReusablePage reusablePage);
    /**
     * Fetch a page from Recycle Queue
     * @param minCsn csn used to fetch page
     * @return PageId of fetched page
     */
    RetStatus GetFromRecycleQueue(PageId &recyclablePageId, CommitSeqNo minCsn, uint64 *numSkippedPage = nullptr);

    /**
     * Fetch a page from Free Queue
     * @param minCsn csn used to fetch page
     * @return PageId of fetched page
     */
    PageId GetFromFreeQueue();

    /**
     * Call after IndexSegment constructor to get segment head buffer (do not release)
     * @return DSTORE_SUCC if Segment is ready, or DSTORE_FAIL if something wrong
     */
    RetStatus InitSegment() final;

    RetStatus TraverseFSMQueue(BufMgrInterface *bufMgr, PageId queueHeadPageId, dlist_head &pageIdsList,
        bool isFreeQueue, char **errInfo);
    RetStatus GetFsmPageIds(BufMgrInterface *bufMgr, PageId **pageIds, Size *length, char **errInfo);

    void CreateNewPages(PageId *newPages, uint16 numNewPages, bool pagesIsReused);

    /**
     * Alloacate a recycle partition iff it does not already have one for the current node
     * and init recycle partition meta
     * @return DSTORE_SUCC if the recycle partition is ready, or DSTORE_FAIL if something wrong
     */
    RetStatus InitBtrRecyclePartition();

    /* IndexSegment Extension functions */
    RetStatus AddNewPagesToBtreeRecycle(PageId *newPageId, bool isExtendBg);

    /* Batch UnlockAndRelease BufferDesc */
    void BatchBufferDescUnlockAndRelease(BufferDesc **bufDescs, uint16 numBufferDesc);

    /**
     * Find and return the cold btree recycle partition using similar logic as fsm recycle:
     * 1. whose newest access times is old enough based on a pre-set interval
     * 2. which is assigned to non-existing NodeId based on current MemberView
    */
    RetStatus GetColdBtreeRecyclePartition(BtrRecyclePartReclaimContext &coldPartitionContext, Xid createdXid);

    RetStatus TryRecycleColdBtrRecyclePartition(Xid createdXid);

    RetStatus RecycleBtrRecyclePartition(BtrRecyclePartReclaimContext &coldPartitionContext, Xid createdXid);
#ifdef DSTORE_COLLECT_INDEX_RECYCLE_INFO
    /**
     * Set the recycle failure reason
     * @param recycleFailReasonCode recycle failure reason code
     */
    void SetRecycleFailReason(BtrPageRecycleFailReason recycleFailReasonCode);
    void RecordPageRecycleOper(BtrPageRecycleOperType recyleOperType);
#endif
    BtreeRecyclePartition *m_btrRecyclePartition{nullptr};
};

} /* namespace DSTORE */
#endif  /* DSTORE_INDEX_SEGMENT_H */
