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
 * dstore_btree_recycle_wal.h
 *
 * IDENTIFICATION
 *        dstore/include/index/dstore_btree_recycle_wal.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_DSTORE_BTREE_RECYCLE_WAL_H
#define DSTORE_DSTORE_BTREE_RECYCLE_WAL_H

#include "wal/dstore_wal_struct.h"
#include "page/dstore_btr_recycle_root_meta_page.h"
#include "index/dstore_btree_recycle_partition.h"

namespace DSTORE {

enum class BtrRecycleQueueType;

struct WalRecordBtrRecycle : public WalRecordForDataPage {
    static void RedoBtrRecycleRecord(WalRecordRedoContext *redoCtx, const WalRecordBtrRecycle *btrRecycleRecord,
                                     BufferDesc *bufferDesc);
    static void DumpBtrRecycleRecord(const WalRecordBtrRecycle *btrRecycleRecord, FILE *fp);
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordBtrRecycle);

struct WalRecordBtrRecycleRootMetaInit : public WalRecordBtrRecycle {
    Xid btrCreatedXid;

    inline void SetHeader(const WalPageHeaderContext &header, Xid createdXid)
    {
        SetWalPageHeader(header);
        btrCreatedXid = createdXid;
    }

    void Redo(Page *page) const;
    void Describe(FILE *fp) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordBtrRecycleRootMetaInit);

struct WalRecordBtrRecycleRootMetaSetPartitionMeta : public WalRecordBtrRecycle {
    PageId recyclePartitionMeta;
    uint16 recyclePartitionId;

    inline void SetHeader(const WalPageHeaderContext &header, const PageId partitionMeta, uint16 partitionId)
    {
        SetWalPageHeader(header);
        recyclePartitionMeta = partitionMeta;
        recyclePartitionId = partitionId;
    }

    void Redo(Page *page) const;
    void Describe(FILE *fp) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordBtrRecycleRootMetaSetPartitionMeta);

struct WalRecordBtrRecyclePartitionMetaInit : public WalRecordBtrRecycle {
    Xid createdXid;
    inline void SetHeader(const WalPageHeaderContext &header, Xid btrCreatedXid)
    {
        SetWalPageHeader(header);
        createdXid = btrCreatedXid;
    }

    void Redo(Page *page) const;
    void Describe(FILE *fp) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordBtrRecyclePartitionMetaInit);

struct WalRecordBtrRecyclePartitionMetaSetHead : public WalRecordBtrRecycle {
    PageId queueHead;
    BtrRecycleQueueType queueType;

    inline void SetHeader(const WalPageHeaderContext &header, const PageId qHead, BtrRecycleQueueType qType)
    {
        SetWalPageHeader(header);
        queueHead = qHead;
        queueType = qType;
    }

    void Redo(Page *page) const;
    void Describe(FILE *fp) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordBtrRecyclePartitionMetaSetHead);

struct WalRecordBtrRecyclePartitionMetaSetTimestamp : public WalRecordBtrRecycle {
    TimestampTz timestamp;
    inline void SetHeader(const WalPageHeaderContext &header, TimestampTz accessTimestamp)
    {
        SetWalPageHeader(header);
        timestamp = accessTimestamp;
    }
    void Redo(Page *page) const;
    void Describe(FILE *fp) const;
} PACKED;

struct WalRecordBtrQueuePageMetaSetNext : public WalRecordBtrRecycle {
    PageId nextPage;

    inline void SetHeader(const WalPageHeaderContext &header, const PageId next)
    {
        SetWalPageHeader(header);
        nextPage = next;
    }

    void Redo(Page *page) const;
    void Describe(FILE *fp) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordBtrQueuePageMetaSetNext);

struct WalRecordBtrRecyclePartition : public WalRecordBtrRecycle {
    BtrRecycleQueueType queueType;

    inline void SetWalHeader(const WalPageHeaderContext &header, BtrRecycleQueueType qType)
    {
        SetWalPageHeader(header);
        queueType = qType;
    }
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordBtrRecyclePartition);

struct WalRecordBtrRecyclePartitionInitPage : public WalRecordBtrRecyclePartition {
    Xid btrCreatedXid;
    inline void SetHeader(const WalPageHeaderContext &header, BtrRecycleQueueType qType, Xid createdXid)
    {
        WalRecordBtrRecyclePartition::SetWalHeader(header, qType);
        btrCreatedXid = createdXid;
    }

    void Redo(Page *page) const;
    void Describe(FILE *fp) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordBtrRecyclePartitionInitPage);

struct WalRecordBtrRecyclePartitionPush : public WalRecordBtrRecyclePartition {
    int16 tail;
    bool needResetPage;
    Xid btrCreatedXid;
    CommitSeqNo csn;
    PageId pageId;

    inline void SetHeader(const WalPageHeaderContext &header, const BtrRecyclePartitionWalInfo &recyclePartitionInfo,
                          bool needReset, Xid createdXid)
    {
        WalRecordBtrRecyclePartition::SetWalHeader(header, recyclePartitionInfo.type);
        btrCreatedXid = createdXid;
        tail = recyclePartitionInfo.pos;
        needResetPage = needReset;
    }

    void SetPage(PageId pagePush, CommitSeqNo pageCsn)
    {
        pageId = pagePush;
        csn = pageCsn;
    }

    void Redo(Page *page) const;
    void Describe(FILE *fp) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordBtrRecyclePartitionPush);

struct WalRecordBtrRecyclePartitionBatchPush : public WalRecordBtrRecyclePartition {
    Xid btrCreatedXid;
    int16 tail;
    uint16 numPages;
    char rawData[];

    inline void SetHeader(const WalPageHeaderContext &header, const BtrRecyclePartitionWalInfo &recyclePartitionInfo,
                          uint16 numNewPages, Xid createdXid)
    {
        WalRecordBtrRecyclePartition::SetWalHeader(header, recyclePartitionInfo.type);
        tail = recyclePartitionInfo.pos;
        numPages = numNewPages;
        btrCreatedXid = createdXid;
    }

    void SetData(char *data, uint16 dataSize, bool needReset, const PageId nextFreeQueuePage = INVALID_PAGE_ID)
    {
        uint16 maxSize = m_size - sizeof(WalRecordBtrRecyclePartitionBatchPush);
        uint16 usedSize = 0;
        if (likely(dataSize != 0)) {
            CopyData(rawData, maxSize, data, dataSize);
            usedSize += dataSize;
        }
        if (likely(nextFreeQueuePage != INVALID_PAGE_ID)) {
            CopyData(rawData + dataSize, static_cast<uint32>(maxSize - dataSize),
                     static_cast<const char *>(static_cast<const void *>(&nextFreeQueuePage)), sizeof(PageId));
            usedSize += sizeof(PageId);
        }

        CopyData(rawData + usedSize, maxSize - usedSize,
            static_cast<const char *>(static_cast<const void *>(&needReset)), sizeof(bool));
        usedSize += sizeof(bool);
    }

    void Redo(Page *page) const;
    void Describe(FILE *fp) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordBtrRecyclePartitionBatchPush);

struct WalRecordBtrRecyclePartitionPop : public WalRecordBtrRecyclePartition {
    int16 head;

    inline void SetHeader(const WalPageHeaderContext &header, const BtrRecyclePartitionWalInfo &recyclePartitionInfo)
    {
        WalRecordBtrRecyclePartition::SetWalHeader(header, recyclePartitionInfo.type);
        head = recyclePartitionInfo.pos;
    }

    void Redo(Page *page) const;
    void Describe(FILE *fp) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordBtrRecyclePartitionPop);

struct WalRecordBtrRecyclePartitionAllocSlot : public WalRecordBtrRecyclePartition {
    Xid btrCreatedXid;
    int16 tail;
    bool needResetPage;
    ReusablePage reusablePage;

    inline void SetHeader(const WalPageHeaderContext &header, const BtrRecyclePartitionWalInfo &recyclePartitionInfo,
                          bool needReset, Xid createdXid)
    {
        WalRecordBtrRecyclePartition::SetWalHeader(header, recyclePartitionInfo.type);
        btrCreatedXid = createdXid;
        needResetPage = needReset;
        tail = recyclePartitionInfo.pos;
    }

    void SetPage(ReusablePage page)
    {
        reusablePage.csn = page.csn;
        reusablePage.pageId = page.pageId;
    }

    void Redo(Page *page) const;
    void Describe(FILE *fp) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordBtrRecyclePartitionAllocSlot);

struct WalRecordBtrRecyclePartitionWriteSlot : public WalRecordBtrRecyclePartition {
    int16 pos;
    ReusablePage reusablePage;

    inline void SetHeader(const WalPageHeaderContext &header, const BtrRecyclePartitionWalInfo &recyclePartitionInfo)
    {
        WalRecordBtrRecyclePartition::SetWalHeader(header, recyclePartitionInfo.type);
        pos = recyclePartitionInfo.pos;
    }

    void SetPage(ReusablePage page)
    {
        reusablePage.csn = page.csn;
        reusablePage.pageId = page.pageId;
    }

    void Redo(Page *page) const;
    void Describe(FILE *fp) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordBtrRecyclePartitionWriteSlot);
}
#endif /* DSTORE_STORAGE_BTREE_RECYCLE_WAL_H */