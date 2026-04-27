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
 * dstore_btree_recycle_partition.h
 *
 * IDENTIFICATION
 *        dstore/include/index/dstore_btree_recycle_partition.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef SRC_GAUSSKERNEL_INCLUDE_INDEX_DSTORE_BTREE_RECYCLE_PARTITION_H
#define SRC_GAUSSKERNEL_INCLUDE_INDEX_DSTORE_BTREE_RECYCLE_PARTITION_H

#include "buffer/dstore_buf_mgr.h"
#include "page/dstore_btr_recycle_partition_meta_page.h"
#include "index/dstore_btree_recycle_queue.h"
#include "tablespace/dstore_index_segment.h"

namespace DSTORE {

#ifdef DSTORE_COLLECT_INDEX_RECYCLE_INFO

enum BtrPageRecycleOperType : int {
    BTR_PUT_INTO_PENDING_QUEUE = 0,
    BTR_GET_FROM_PENDING_QUEUE,
    BTR_PUT_INTO_FREE_QUEUE,
    BTR_GET_FROM_FREE_QUEUE,
    MAX_BTR_RECYCLE_OPER_TYPE_NUM    /* Note that it must be the last one */
};

enum BtrPageRecycleFailReason : int {
    BTR_PAGE_NOT_EMPTY = 0,
    BTR_PAGE_DEL_NOT_VIS_TO_ALL,
    BTR_PAGE_LEFT_SIB_SPLITTING,
    BTR_PAGE_LEFT_SIB_CHANGED,
    BTR_PAGE_RIGHT_SIB_UNLINKED,
    BTR_GET_PARENT_FAIL,
    BTR_PAGE_RIGHTMOST_CHILD_OF_PARENT,
    BTR_PAGE_PIVOT_CHANGED,
    BTR_DEL_PIVOT_FAIL,
    BTR_PAGE_RECYCLED_BY_OTHERS,
    MAX_BTR_RECYCLE_FAILED_REASON_NUM    /* Note that it must be the last one */
};

struct RecycleFailReasonCounter {
    std::atomic<unsigned int> indexRecycleFailReason[MAX_BTR_RECYCLE_FAILED_REASON_NUM];
    void Init()
    {
        for (int i = 0; i < MAX_BTR_RECYCLE_FAILED_REASON_NUM; i++) {
            indexRecycleFailReason[i] = 0;
        }
    }
};
struct QueueOperationCounter {
    std::atomic<unsigned int> indexFreeQueueOper[MAX_BTR_RECYCLE_OPER_TYPE_NUM];
    void Init()
    {
        for (int i = 0; i < MAX_BTR_RECYCLE_OPER_TYPE_NUM; i++) {
            indexFreeQueueOper[i] = 0;
        }
    }
};
#endif

struct BtrRecyclePartitionWalInfo {
    BtrRecycleQueueType type;
    int16 pos;
};

struct BtreeRecyclePartition : public BaseObject {
public:
    PdbId pdbId;
    IndexSegment *segment;
    PageId recyclePartitionMeta;
    const Xid createdXid;

#ifdef DSTORE_COLLECT_INDEX_RECYCLE_INFO
    RecycleFailReasonCounter recycleFailReasonCounter;
    QueueOperationCounter queueOperCounter;
#endif

    BtreeRecyclePartition(IndexSegment *indexSegment, const PageId recyclePartitionMetaPage, Xid btrCreatedXid,
                          BufMgrInterface *bufferMgr)
        : pdbId(INVALID_PDB_ID), segment(indexSegment), recyclePartitionMeta(recyclePartitionMetaPage),
          createdXid(btrCreatedXid), bufMgr(bufferMgr), needCheckCreatedXid(true)
    {
        pdbId = (indexSegment != nullptr) ? indexSegment->GetPdbId() : pdbId;
    }
    /* BtreeRecyclePartition is deleted whenever the segment is deleted */
    ~BtreeRecyclePartition() = default;

    RetStatus Init();

    bool IsValid(bool &locksucc);

    inline void SetCreatedXidCheckingOn()
    {
        needCheckCreatedXid = true;
    }
    inline void SetCreatedXidCheckingOff()
    {
        needCheckCreatedXid = false;
    }
    inline PdbId GetPdbId()
    {
        return pdbId;
    }

    RetStatus RecycleListPush(const RecyclablePage recyclablePage);

    RetStatus RecycleListPop(PageId &recyclablePageId, CommitSeqNo minCsn, uint64 *numSkippedPage = nullptr);

    PageId FreeListPop(bool &needRetry);

    RetStatus FreeListPush(const PageId reusablePage);

    RetStatus AllocUnusedSlot(FreeQueueSlot &slot, const PageId emptyPage);

    RetStatus WriteSlot(const FreeQueueSlot slot, const ReusablePage reusablePage);

    PageId AllocNewPage();

    RetStatus TakeOverColdRecyclePartition(BtreeRecyclePartition *partition);

    /* Get the queue for specific type, must be called after RecyclePartition MetaPage is locked */
    BtreeRecycleQueue GetBtreeRecycleQueue(BtrRecycleQueueType type);

    RetStatus AcquireRecyclePartitionMetaBuf(LWLockMode access);

    void ReleaseRecyclePartitionMetaBuf(bool unlock = true);

    RetStatus FreeListBatchPushNewPages(PageId *newPages, uint16 numNewPages, uint16 &numNewBtrPages,
                                        bool isExtendBg = false);

    /* pass in the mode of lock that's held by the meta buf for checking the need of updating timestamp */
    RetStatus UpdateTimestampIfNecessary(LWLockMode access);

    void TryRegisterExtendTask();

private:
    BufMgrInterface *bufMgr{nullptr};
    bool needCheckCreatedXid;

    BufferDesc *recyclePartitionMetaBuf{INVALID_BUFFER_DESC};

    BufMgrInterface *GetBufMgr();

    BtrQueuePage *GetBtrQueuePage(BufferDesc *buf);

    BtrRecyclePartitionMeta GetRecyclePartitionMeta();

    PageId GetNewPage();

    template <typename T>
    RetStatus InitNewQueueHead(BtrRecycleQueueType type);

    PageId RemoveQueueHead(BtrRecycleQueueType type, BufferDesc *headBuf);

    template <typename T>
    BufferDesc *InitNewQueuePage(BtrRecycleQueueType type, const PageId page);

    void AddQueueTail(BtrRecycleQueueType type, BufferDesc *tailBuf, const PageId newTail);

    PageId RemoveQueueTail(BtrRecycleQueueType type, BufferDesc *buf);

    PageId RemoveQueuePage(BtrRecycleQueueType type, BufferDesc *buf, BufferDesc *nextBuf);

    RetStatus ReusablePageQueueAllocSlot(FreeQueueSlot &slot, BufferDesc *qBuf, const PageId emptyPage, bool needInit);

    PageId ReusablePageQueuePop(BtrRecycleQueueType type, BufferDesc *buf, CommitSeqNo minCsn);

    RetStatus ReusablePageQueuePush(BtrRecycleQueueType type, BufferDesc *buf, ReusablePage reusablePage,
        bool needInit);

    PageId RecyclablePageQueuePop(BtrRecycleQueueType type, BufferDesc *buf, CommitSeqNo minCsn,
                                  uint64 *numSkippedPage = nullptr);

    RetStatus RecyclablePageQueuePush(BtrRecycleQueueType type, BufferDesc *buf, RecyclablePage recyclablePage,
        bool needInit);

    RetStatus BatchPushFreeQueue(BufferDesc *buf, PageId *newPages, uint16 &startPagePos,
        uint16 &endPagePos, bool &needResetFreePage);

    void GenerateWalForRecyclePartitionInitPage(BufferDesc *qBuf, BtrRecycleQueueType type);

    void GenerateWalForRecyclePartitionMetaSetHead(BtrRecycleQueueType type, const PageId headPageId);

    void GenerateWalForRecycleQueuePageMetaSetNext(BufferDesc *qBuf, const PageId next);

    void GenerateWalForRecyclePartitionPush(BufferDesc *qBuf, BtrRecyclePartitionWalInfo recyclePartitionInfo,
                                            PageId pagePush, CommitSeqNo pageCsn, bool needReset);

    void GenerateWalForRecyclePartitionPop(BufferDesc *qBuf, BtrRecycleQueueType type, int16 pos);

    void GenerateWalForRecyclePartitionAllocSlot(BufferDesc *qBuf, ReusablePage page, int16 pos, bool needReset);

    void GenerateWalForRecyclePartitionWriteSlot(BufferDesc *qBuf, ReusablePage page, int16 pos);

    RetStatus GenerateWalForRecyclePartitionBatchPush(BufferDesc *qBuf, int16 pos,
        PageId *newBtrPages, uint16 numBtrPages, bool needReset, const PageId nextFreeQueuePage = INVALID_PAGE_ID);

    void GenerateWalForTimestampUpdate();

    /** Take pages from another partition's recycle queue, the meta page of this partition is not changed
     *  Needs to acquire lock on the partition's meta page before calling
    */
    template<typename T>
    RetStatus TakePagesFromColdRecyclePartitionQueue(BtreeRecyclePartition *coldPartition, BtrRecycleQueueType type);

    /** MUST hold a lock on recyclePartitionMetaBuf befor calling TimestampNeedUpdate! */
    inline bool TimestampNeedUpdate()
    {
        static constexpr float TIMESTAMP_CLOSE_THRESHOLD = 0.8;

        BtrRecyclePartitionMetaPage *metaPage =
            static_cast<BtrRecyclePartitionMetaPage*>(recyclePartitionMetaBuf->GetPage());
        TimestampTz lastTimestamp = metaPage->accessTimestamp;
        TimestampTz current = GetCurrentTimestampInSecond();
        if (unlikely(current < lastTimestamp)) {
            /* Systime has been reset. Earlier timesmapts are unreliable now, need to restart timer */
            return true;
        }

        TimestampTz recycleInterval = static_cast<TimestampTz>(g_storageInstance->GetGuc()->recycleFsmTimeInterval);
        TimestampTz interval = static_cast<TimestampTz>(recycleInterval * STORAGE_SECS_PER_MIN);

        return static_cast<float>(current - lastTimestamp) >= TIMESTAMP_CLOSE_THRESHOLD * static_cast<float>(interval);
    }
};

}

#endif /* SRC_GAUSSKERNEL_INCLUDE_INDEX_STORAGE_BTREE_RECYCLE_PARTITION_H */
