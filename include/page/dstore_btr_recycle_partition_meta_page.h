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
 * dstore_index_temp_segment.cpp
 *
 *
 * IDENTIFICATION
 *        include/page/dstore_btr_recycle_partition_meta_page.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_GAUSSKERNEL_INCLUDE_PAGE_DSTORE_BTR_RECYCLE_PARTITION_META_PAGE_H
#define SRC_GAUSSKERNEL_INCLUDE_PAGE_DSTORE_BTR_RECYCLE_PARTITION_META_PAGE_H

#include "buffer/dstore_buf_mgr.h"
#include "page/dstore_page.h"

namespace DSTORE {

enum class BtrRecycleQueueType { RECYCLE, FREE };

/*
 * BtrRecyclePartitionMetaPage
 *
 * The disk format of a BtrRecyclePartitionMetaPage.
 * +------------+------------------------+----------------+
 * | PageHeader | RecycleQueueHeadPageId |      ...       |
 * +------------+--------+---------------+----------------+
 * | freeQueueHeadPageId |                                |
 * +---------------------+--------------------------------+
 * |                    ... UNUSED ...                    |
 * +------------------------------------------------------+
 * |                        ......                        |
 * +------------------------------------------------------+
 * |                        ......                        |
 * +------------------------------------------------------+
 *
 * The BtrRecyclePartitionMetaPage is a page that keep track and persisting
 * the Btree page recycling meta data. The Btree page recycling uses three
 * queues, the meta page contains the PageId of the head of each queue.
 *
 * RecycleQueue:
 * The RecycleQueue stores pages that are potential empty. It means that it may
 * contain only dead index tuples. When the last index tuples is deleted from
 * a BtrPage, it is then added to the recycle queue.
 *
 * When the BtrPage becomes dead, it is then unlinked from Btree and
 * placed into the FreeQueue with a logical timestamp representing the
 * earliest time available for reuse. A BtrPage becomes dead when the RecycleCSN
 * moves pass the CSN assigned to the page when pushed onto the queue.
 *
 * FreeQueue:
 * The FreeQueue stores pages that can potentially be reused by the Btree.
 * Each page in the FreeQueue has a logical timestamp attached. The logical
 * timestamp is used to detemine whether the pages may still have potential
 * readers accessing the page.
 */
struct BtrRecyclePartitionMetaPage : public Page {
public:
    PageId recycleQueueHead;
    PageId freeQueueHead;
    TimestampTz accessTimestamp;
    Xid createdXid;
    uint32 versionNum;

    void InitRecyclePartitionMetaPage(const PageId selfPageId, Xid btrCreatedXid)
    {
        Page::Init(0, PageType::BTR_RECYCLE_PARTITION_META_PAGE_TYPE, selfPageId);
        recycleQueueHead = INVALID_PAGE_ID;
        freeQueueHead = INVALID_PAGE_ID;
        accessTimestamp = GetCurrentTimestampInSecond();
        createdXid = btrCreatedXid;
    }

    inline PageId GetRecycleQueueHead()
    {
        return recycleQueueHead;
    }

    inline void SetRecycleQueueHead(const PageId page)
    {
        recycleQueueHead = page;
    }

    inline PageId GetFreeQueueHead()
    {
        return freeQueueHead;
    }

    inline void SetFreeQueueHead(const PageId page)
    {
        freeQueueHead = page;
    }

    char *Dump();
};
STATIC_ASSERT_TRIVIAL(BtrRecyclePartitionMetaPage);

struct BtrRecyclePartitionMeta {
public:
    PdbId m_pdbId;
    BufferDesc *recyclePartitionMetaBuf{nullptr};
    PageId recyclePartitionMeta{INVALID_PAGE_ID};
    bool isGlobalTempIndex{false};  /* flag of global temporary index. use temporary buffer and no need wal */

    explicit BtrRecyclePartitionMeta(
        PdbId pdbId, const PageId recyclePartitionMetaPage, BufferDesc *recyclePartitionMetaBufDesc, bool useGti)
        : m_pdbId(pdbId), recyclePartitionMetaBuf(recyclePartitionMetaBufDesc),
          recyclePartitionMeta(recyclePartitionMetaPage), isGlobalTempIndex(useGti){};

    ~BtrRecyclePartitionMeta()
    {
        bufMgr = nullptr;
        recyclePartitionMetaBuf = nullptr;
    }

    PageId GetQueueHead(BtrRecycleQueueType type) const;

    void SetQueueHead(BtrRecycleQueueType type, const PageId head);

    bool IsEmpty() const;

    void GenerateWalForRecyclePartitionMetaInit();
    inline bool IsPartitionExpired(const TimestampTz recycleTimeThreshold) const
    {
        BtrRecyclePartitionMetaPage *metaPage =
            static_cast<BtrRecyclePartitionMetaPage *>(recyclePartitionMetaBuf->GetPage());
        StorageAssert(metaPage->accessTimestamp != 0);
        TimestampTz currentTimestamp = GetCurrentTimestampInSecond();
        /* unsigned type, make sure currentTimestamp is greater */
        return ((currentTimestamp - metaPage->accessTimestamp >= recycleTimeThreshold) &&
                likely(currentTimestamp >= metaPage->accessTimestamp));
    }
     
    inline PdbId GetPdbId()
    {
        return m_pdbId;
    }

private:
    BufMgrInterface *bufMgr{nullptr};

    BufMgrInterface *GetBufMgr();
};
}

#endif /* SRC_GAUSSKERNEL_INCLUDE_PAGE_STORAGE_BTR_RECYCLE_PARTITION_META_PAGE_H */