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
 */
#include "ut_btree/ut_btree_recycle_partition.h"
#include "tablespace/dstore_tablespace_struct.h"
#include "ut_tablespace/ut_segment.h"

IndexNormalSegment* BtreeRecyclePartitionTest::PrepareForIndexSegment(PdbId pdbId, TablespaceId tablespaceId,
    BufMgrInterface *bufMgr)
{
    return (IndexNormalSegment*)SegmentTest::UTAllocSegment(pdbId, tablespaceId, bufMgr,
        SegmentType::INDEX_SEGMENT_TYPE);
}

namespace DSTORE {

TEST_F(BtreeRecyclePartitionTest, RecycleListPushAndPop_level0) {
    /* Step 1. Initialization */
    PageId recyclablePageId = {1, 1};
    PageId recyclablePageId2 = {1, 2};
    RecyclablePage recyclablePage = (RecyclablePage){1, recyclablePageId};
    RecyclablePage recyclablePage2 = (RecyclablePage){2, recyclablePageId2};

    Xid createdXid = indexSegment->m_btrRecyclePartition->createdXid;
    BtreeRecyclePartition reyclePartition(indexSegment, recycleMetaPageId, createdXid, m_bufMgr);
    BtreeRecycleQueue recycleList(g_defaultPdbId, recycleQPageId, BtrRecycleQueueType::RECYCLE, m_bufMgr);
    BtreeRecycleQueue freeList(g_defaultPdbId, freeQPageId, BtrRecycleQueueType::FREE, m_bufMgr);

    EXPECT_EQ(reyclePartition.Init(), DSTORE_SUCC);
    PageId tailPageId = reyclePartition.AllocNewPage();

    reyclePartition.RecycleListPush(recyclablePage);
    reyclePartition.RecycleListPush(recyclablePage2);

    /* Step 2. Test push and pop while the list has size > 1 */
    PageId item1;
    reyclePartition.RecycleListPop(item1, 3);
    EXPECT_EQ(item1, recyclablePageId);

    /* Step 3. Test push and pop while the list has size == 1 and it's not tail */
    BufferDesc *oldTailBuf = m_bufMgr->Read(g_defaultPdbId, recycleQPageId, LW_EXCLUSIVE);

    /* Step 4. Init the tail page */
    BufferDesc *newTailBuf = m_bufMgr->Read(g_defaultPdbId, tailPageId, LW_EXCLUSIVE);
    BtrQueuePage *newTailPage = (BtrQueuePage *)newTailBuf->GetPage();
    newTailPage->Init<RecyclablePageQueue>(tailPageId, createdXid);
    m_bufMgr->MarkDirty(newTailBuf);
    m_bufMgr->UnlockAndRelease(newTailBuf);

    recycleList.AddTailPage(oldTailBuf, tailPageId);
    m_bufMgr->UnlockAndRelease(oldTailBuf);

    PageId item2;
    reyclePartition.RecycleListPop(item2, 3);
    EXPECT_EQ(item2, recyclablePageId2);

    BufferDesc *recycleMetaBuf = m_bufMgr->Read(g_defaultPdbId, recycleMetaPageId, LW_SHARED);
    BtrRecyclePartitionMetaPage *recycleMetaPage;
    recycleMetaPage = reinterpret_cast<BtrRecyclePartitionMetaPage *>(recycleMetaBuf->GetPage());
    /* Step 5. Test the head of the RecycleList reference to the next page */
    EXPECT_EQ(recycleMetaPage->GetRecycleQueueHead(), tailPageId);
    m_bufMgr->UnlockAndRelease(recycleMetaBuf);
}

TEST_F(BtreeRecyclePartitionTest, RecycleListFailToPopLastPageFromHead_level0) {
    /* Step 1. Initialization */
    PageId recyclablePageId = {1, 1};
    PageId recyclablePageId2 = {1, 2};
    RecyclablePage recyclablePage = (RecyclablePage){1, recyclablePageId};
    RecyclablePage recyclablePage2 = (RecyclablePage){2, recyclablePageId2};

    Xid createdXid = indexSegment->m_btrRecyclePartition->createdXid;
    BtreeRecyclePartition reyclePartition(indexSegment, recycleMetaPageId, createdXid, m_bufMgr);
    BtreeRecycleQueue recycleList(g_defaultPdbId, recycleQPageId, BtrRecycleQueueType::RECYCLE, m_bufMgr);
    BtreeRecycleQueue freeList(g_defaultPdbId, freeQPageId, BtrRecycleQueueType::FREE, m_bufMgr);

    EXPECT_EQ(reyclePartition.Init(), DSTORE_SUCC);
    PageId tailPageId = reyclePartition.AllocNewPage();

    reyclePartition.RecycleListPush(recyclablePage);
    reyclePartition.RecycleListPush(recyclablePage2);

    /* Step 2. Test push and pop while the list has size > 1 */
    PageId item1;
    reyclePartition.RecycleListPop(item1, 3);
    EXPECT_EQ(item1, recyclablePageId);

    /* Step 3. Test push and pop while the list has size == 1 and it's not tail */
    BufferDesc *oldTailBuf = m_bufMgr->Read(g_defaultPdbId, recycleQPageId, LW_EXCLUSIVE);

    /* Step 4. Init the tail page */
    BufferDesc *newTailBuf = m_bufMgr->Read(g_defaultPdbId, tailPageId, LW_EXCLUSIVE);
    BtrQueuePage *newTailPage = (BtrQueuePage *)newTailBuf->GetPage();
    newTailPage->Init<RecyclablePageQueue>(tailPageId, createdXid);
    m_bufMgr->MarkDirty(newTailBuf);
    m_bufMgr->UnlockAndRelease(newTailBuf);

    recycleList.AddTailPage(oldTailBuf, tailPageId);
    m_bufMgr->UnlockAndRelease(oldTailBuf);

    PageId item2;
    reyclePartition.RecycleListPop(item2, 2);
    EXPECT_EQ(item2, INVALID_PAGE_ID);

    BufferDesc *recycleMetaBuf = m_bufMgr->Read(g_defaultPdbId, recycleMetaPageId, LW_SHARED);
    BtrRecyclePartitionMetaPage *recycleMetaPage;
    recycleMetaPage = reinterpret_cast<BtrRecyclePartitionMetaPage *>(recycleMetaBuf->GetPage());
    /* Step 5. Test the head of the RecycleList reference to the next page */
    EXPECT_EQ(recycleMetaPage->GetRecycleQueueHead(), recycleQPageId);
    m_bufMgr->UnlockAndRelease(recycleMetaBuf);
}

TEST_F(BtreeRecyclePartitionTest, AllocNewPageAndFreeListPush_level0) {
    /* Step 1. Initialization */
    Xid createdXid = indexSegment->m_btrRecyclePartition->createdXid;
    BtreeRecyclePartition reyclePartition(indexSegment, recycleMetaPageId, createdXid, m_bufMgr);
    BtreeRecycleQueue recycleList(g_defaultPdbId, recycleQPageId, BtrRecycleQueueType::RECYCLE, m_bufMgr);
    BtreeRecycleQueue freeList(g_defaultPdbId, freeQPageId, BtrRecycleQueueType::FREE, m_bufMgr);

    EXPECT_EQ(reyclePartition.Init(), DSTORE_SUCC);

    /* Step 2. Test release and acquire free page from the segment meta */
    PageId freePageId = reyclePartition.AllocNewPage();
    EXPECT_TRUE(freePageId.IsValid());
    reyclePartition.FreeListPush(freePageId);

    /* Step 3. Test acquire free page from the free queue */
    PageId allocPageId = reyclePartition.AllocNewPage();
    EXPECT_EQ(freePageId, allocPageId);
}

TEST_F(BtreeRecyclePartitionTest, AllocNewPageAndFreeFullQueue_level0) {
    /* Step 1. Initialization */
    Xid createdXid = indexSegment->m_btrRecyclePartition->createdXid;
    BtreeRecyclePartition reyclePartition(indexSegment, recycleMetaPageId, createdXid, m_bufMgr);
    BtreeRecycleQueue recycleList(g_defaultPdbId, recycleQPageId, BtrRecycleQueueType::RECYCLE, m_bufMgr);
    BtreeRecycleQueue freeList(g_defaultPdbId, freeQPageId, BtrRecycleQueueType::FREE, m_bufMgr);

    EXPECT_EQ(reyclePartition.Init(), DSTORE_SUCC);

    /*
     * Step 2. Test when one BtrQueuePage in the BtreeRecycleQueue of freeList is full
     * When we release a free page, the freeList should result in 2 BtrQueuePage
     */
    PageId freePageId = reyclePartition.AllocNewPage();
    PageId freePageId2 = reyclePartition.AllocNewPage();
    PageId tailPageId = reyclePartition.AllocNewPage();

    reyclePartition.FreeListPush(freePageId);
    reyclePartition.FreeListPush(freePageId2);

    BufferDesc *freeQBuf = m_bufMgr->Read(g_defaultPdbId, freeQPageId, LW_SHARED);
    BtrQueuePage *freeQPage = reinterpret_cast<BtrQueuePage *>(freeQBuf->GetPage());
    ReusablePageQueue *freeQ = freeQPage->GetQueue<ReusablePageQueue>();
    EXPECT_EQ(freeQ->IsFull(), true);
    m_bufMgr->UnlockAndRelease(freeQBuf);

    reyclePartition.FreeListPush(tailPageId);
    BufferDesc *buffer = freeList.GetQueueTailBuf(LW_EXCLUSIVE, createdXid);
    EXPECT_EQ(buffer->GetPageId(), tailPageId);
    m_bufMgr->UnlockAndRelease(buffer);

    /*
     * Step 3. Test acquire free page when we have two BtrQueuePage in the BtreeRecycleQueue for freeList
     * when first one is full, second one is empty
     */
    PageId pageId = reyclePartition.AllocNewPage();
    EXPECT_EQ(pageId, freePageId);
}

TEST_F(BtreeRecyclePartitionTest, AllocSlotAndWriteSlot_level0) {
    /* Step 1. Initialization */
    Xid createdXid = indexSegment->m_btrRecyclePartition->createdXid;
    BtreeRecyclePartition reyclePartition(indexSegment, recycleMetaPageId, createdXid, m_bufMgr);
    BtreeRecycleQueue recycleList(g_defaultPdbId, recycleQPageId, BtrRecycleQueueType::RECYCLE, m_bufMgr);
    BtreeRecycleQueue freeList(g_defaultPdbId, freeQPageId, BtrRecycleQueueType::FREE, m_bufMgr);

    EXPECT_EQ(reyclePartition.Init(), DSTORE_SUCC);

    PageId freePageId = reyclePartition.AllocNewPage();
    PageId freePageId2 = reyclePartition.AllocNewPage();

    /* Step 2. Set local csn to a large value, push to FreeListQueue to make it full */
    g_storageInstance->GetCsnMgr()->SetLocalCsnMin(100);
    reyclePartition.FreeListPush(freePageId);
    reyclePartition.FreeListPush(freePageId2);

    BufferDesc *freeQBuf = m_bufMgr->Read(g_defaultPdbId, freeQPageId, LW_SHARED);
    BtrQueuePage *freeQPage = reinterpret_cast<BtrQueuePage *>(freeQBuf->GetPage());
    ReusablePageQueue *freeQ = freeQPage->GetQueue<ReusablePageQueue>();
    EXPECT_EQ(freeQ->IsFull(), true);
    m_bufMgr->UnlockAndRelease(freeQBuf);

    /* Step 3. Try allocate slot, check it allocate the first slot of the head page */
    FreeQueueSlot slot;
    reyclePartition.AllocUnusedSlot(slot, freePageId);
    EXPECT_EQ(slot.qPage, freeQPageId);
    EXPECT_EQ(slot.pos, 0);

    /* Step 4. Try allocate slot, check it allocate the first slot of the new tail page */
    reyclePartition.AllocUnusedSlot(slot, freePageId2);
    EXPECT_EQ(slot.qPage, freePageId);
    EXPECT_EQ(slot.pos, 0);
}

TEST_F(BtreeRecyclePartitionTest, FreeListBatchPushNewPages_level0) {
    /* Step 1. Initialization */
    Xid createdXid = indexSegment->m_btrRecyclePartition->createdXid;
    BtreeRecyclePartition reyclePartition(indexSegment, recycleMetaPageId, createdXid, m_bufMgr);
    BtreeRecycleQueue recycleList(g_defaultPdbId, recycleQPageId, BtrRecycleQueueType::RECYCLE, m_bufMgr);
    BtreeRecycleQueue freeList(g_defaultPdbId, freeQPageId, BtrRecycleQueueType::FREE, m_bufMgr);

    uint16 numNewPages = 5;
    uint16 numNewBtrPages = numNewPages;
    PageId pageId1 = reyclePartition.AllocNewPage();
    PageId pageId2 = reyclePartition.AllocNewPage();
    PageId pageId3 = reyclePartition.AllocNewPage();
    PageId pageId4 = reyclePartition.AllocNewPage();
    PageId pageId5 = reyclePartition.AllocNewPage();
    PageId newPages[5] = {pageId1, pageId2, pageId3, pageId4, pageId5};

    EXPECT_EQ(reyclePartition.Init(), DSTORE_SUCC);
    reyclePartition.FreeListBatchPushNewPages(&newPages[1], static_cast<uint16>(numNewPages - 1), numNewBtrPages);
    reyclePartition.segment->CreateNewPages(newPages, numNewBtrPages, false);

    /*
     * Step 2. Test when amount of pages is greater than the max capacity of free queue page
     * it used a page from the unassigned pages as a new page from the free queue
     */
    BufferDesc *freeQBuf = m_bufMgr->Read(g_defaultPdbId, freeQPageId, LW_SHARED);
    BtrQueuePage *freeQPage = reinterpret_cast<BtrQueuePage *>(freeQBuf->GetPage());
    ReusablePageQueue *freeQ = freeQPage->GetQueue<ReusablePageQueue>();
    EXPECT_EQ(freeQ->IsFull(), true);
    EXPECT_EQ(freeQPage->GetNext(), pageId5);
    m_bufMgr->UnlockAndRelease(freeQBuf);

    /* Step 3. Test all pages are pushed to the free queue */
    EXPECT_EQ(reyclePartition.AllocNewPage(), pageId2);
    EXPECT_EQ(reyclePartition.AllocNewPage(), pageId3);
    EXPECT_EQ(reyclePartition.AllocNewPage(), freeQPageId);
    EXPECT_EQ(reyclePartition.AllocNewPage(), pageId4);
    /* Step 4. Free Queue is empty, it should allocate from the segment */
    EXPECT_NE(reyclePartition.AllocNewPage(), pageId5);
}

TEST_F(BtreeRecyclePartitionTest, FreeListBatchPushNewPagesToFullQueue_level0) {
    /* Step 1. Initialization */
    Xid createdXid = indexSegment->m_btrRecyclePartition->createdXid;
    BtreeRecyclePartition reyclePartition(indexSegment, recycleMetaPageId, createdXid, m_bufMgr);
    BtreeRecycleQueue recycleList(g_defaultPdbId, recycleQPageId, BtrRecycleQueueType::RECYCLE, m_bufMgr);
    BtreeRecycleQueue freeList(g_defaultPdbId, freeQPageId, BtrRecycleQueueType::FREE, m_bufMgr);

    uint16 numNewPages = 3;
    uint16 numNewBtrPages = numNewPages;
    PageId pageId1 = reyclePartition.AllocNewPage();
    PageId pageId2 = reyclePartition.AllocNewPage();
    PageId pageId3 = reyclePartition.AllocNewPage();
    PageId pageId4 = reyclePartition.AllocNewPage();
    PageId pageId5 = reyclePartition.AllocNewPage();
    PageId newPages[3] = {pageId3, pageId4, pageId5};

    EXPECT_EQ(reyclePartition.Init(), DSTORE_SUCC);

    g_storageInstance->GetCsnMgr()->SetLocalCsnMin(100);

    /*
     * Step 2. Push 2 pages in the freeQueue and ensure the freeQueue is full
     */

    BufferDesc *freeQBuf = m_bufMgr->Read(g_defaultPdbId, freeQPageId, LW_SHARED);
    BtrQueuePage *freeQPage = reinterpret_cast<BtrQueuePage *>(freeQBuf->GetPage());
    ReusablePageQueue *freeQ = freeQPage->GetQueue<ReusablePageQueue>();
    freeQ->Push((ReusablePage) {1, pageId1});
    freeQ->Push((ReusablePage) {1, pageId2});

    EXPECT_EQ(freeQ->IsFull(), true);
    m_bufMgr->UnlockAndRelease(freeQBuf);

    reyclePartition.FreeListBatchPushNewPages(&newPages[1], static_cast<uint16>(numNewPages - 1), numNewBtrPages);
    reyclePartition.segment->CreateNewPages(newPages, numNewBtrPages, false);

    /*
     * Step 3. Test when amount of pages is greater than the max capacity of free queue page
     * it used a page from the unassigned pages as a new page from the free queue
     */
    freeQBuf = m_bufMgr->Read(g_defaultPdbId, freeQPageId, LW_SHARED);
    freeQPage = reinterpret_cast<BtrQueuePage *>(freeQBuf->GetPage());
    EXPECT_EQ(freeQPage->GetNext(), pageId5);
    m_bufMgr->UnlockAndRelease(freeQBuf);

    /* Step 4. Test all pages are pushed to the free queue */
    EXPECT_EQ(reyclePartition.AllocNewPage(), pageId1);
    EXPECT_EQ(reyclePartition.AllocNewPage(), pageId2);
    EXPECT_EQ(reyclePartition.AllocNewPage(), freeQPageId);
    EXPECT_EQ(reyclePartition.AllocNewPage(), pageId4);

    /* Step 5. Free Queue is empty, it should allocate from the segment instead of popping the tail page */
    EXPECT_NE(reyclePartition.AllocNewPage(), pageId5);
}

TEST_F(BtreeRecyclePartitionTest, InvalidRecycleQueueHead_level0) {
    /* Step 1. Initialization */
    MockIndexSegment *indexSegment = MockIndexSegment::AllocMockIndexSegment(g_defaultPdbId,
        SegmentType::INDEX_SEGMENT_TYPE, m_utTableHandler->m_tablespaceId, m_bufMgr);
    BtreeRecyclePartition reyclePartition(indexSegment, recycleMetaPageId, INVALID_XID, m_bufMgr);

    /* Step 2. Make recycle head invalid */
    BtrRecyclePartitionMetaPage *recycleMetaPage;
    BufferDesc *recycleMetaBuf = m_bufMgr->Read(g_defaultPdbId, recycleMetaPageId, LW_SHARED);
    recycleMetaPage = reinterpret_cast<BtrRecyclePartitionMetaPage *>(recycleMetaBuf->GetPage());
    m_bufMgr->UnlockAndRelease(recycleMetaBuf);

    recycleMetaPage->SetRecycleQueueHead(INVALID_PAGE_ID);
    EXPECT_EQ(recycleMetaPage->GetRecycleQueueHead(), INVALID_PAGE_ID);

    /* Step 3. reyclePartition.Init() returns DSTORE_FAIL*/
    using ::testing::Return;
    EXPECT_CALL(*indexSegment, GetNewPageFromUnassignedPages).WillRepeatedly(Return(INVALID_PAGE_ID));
    ASSERT_EQ(reyclePartition.Init(), DSTORE_FAIL);
    delete indexSegment;
}

TEST_F(BtreeRecyclePartitionTest, InvalidFreeQueueHead_level0) {
    /* Step 1. Initialization */
    MockIndexSegment *indexSegment = MockIndexSegment::AllocMockIndexSegment(g_defaultPdbId,
        SegmentType::INDEX_SEGMENT_TYPE, m_utTableHandler->m_tablespaceId, m_bufMgr);
    BtreeRecyclePartition reyclePartition(((IndexNormalSegment* )indexSegment), recycleMetaPageId, INVALID_XID, m_bufMgr);

    /* Step 2. Make free queue head invalid */
    BtrRecyclePartitionMetaPage *recycleMetaPage;
    BufferDesc *recycleMetaBuf = m_bufMgr->Read(g_defaultPdbId, recycleMetaPageId, LW_SHARED);
    recycleMetaPage = reinterpret_cast<BtrRecyclePartitionMetaPage *>(recycleMetaBuf->GetPage());
    m_bufMgr->UnlockAndRelease(recycleMetaBuf);

    recycleMetaPage->SetFreeQueueHead(INVALID_PAGE_ID);
    EXPECT_EQ(recycleMetaPage->GetFreeQueueHead(), INVALID_PAGE_ID);

    /* Step 3. reyclePartition.Init() returns DSTORE_FAIL*/
    using ::testing::Return;
    EXPECT_CALL(*indexSegment, GetNewPageFromUnassignedPages).WillRepeatedly(Return(INVALID_PAGE_ID));
    ASSERT_EQ(reyclePartition.Init(), DSTORE_FAIL);
    delete indexSegment;
}

TEST_F(BtreeRecyclePartitionTest, IsRecycleQueueHeadValid_level0) {
    Xid createdXid = indexSegment->m_btrRecyclePartition->createdXid;
    BtreeRecyclePartition reyclePartition(indexSegment, recycleMetaPageId, createdXid, m_bufMgr);
    EXPECT_EQ(reyclePartition.AcquireRecyclePartitionMetaBuf(LW_SHARED), DSTORE_SUCC);
    bool locksucc = true;
    EXPECT_TRUE(reyclePartition.IsValid(locksucc));
    reyclePartition.ReleaseRecyclePartitionMetaBuf();

    BtrRecyclePartitionMetaPage *recycleMetaPage;
    BufferDesc *recycleMetaBuf = m_bufMgr->Read(g_defaultPdbId, recycleMetaPageId, LW_SHARED);
    recycleMetaPage = reinterpret_cast<BtrRecyclePartitionMetaPage *>(recycleMetaBuf->GetPage());
    m_bufMgr->UnlockAndRelease(recycleMetaBuf);

    recycleMetaPage->SetRecycleQueueHead(INVALID_PAGE_ID);
    EXPECT_EQ(recycleMetaPage->GetRecycleQueueHead(), INVALID_PAGE_ID);

    EXPECT_EQ(reyclePartition.AcquireRecyclePartitionMetaBuf(LW_SHARED), DSTORE_SUCC);
    EXPECT_FALSE(reyclePartition.IsValid(locksucc));
    reyclePartition.ReleaseRecyclePartitionMetaBuf();
}

TEST_F(BtreeRecyclePartitionTest, IsFreeQueueHeadValid_level0) {
    Xid createdXid = indexSegment->m_btrRecyclePartition->createdXid;
    BtreeRecyclePartition reyclePartition(indexSegment, recycleMetaPageId, createdXid, m_bufMgr);
    EXPECT_EQ(reyclePartition.AcquireRecyclePartitionMetaBuf(LW_SHARED), DSTORE_SUCC);
    bool locksucc = true;
    EXPECT_TRUE(reyclePartition.IsValid(locksucc));
    reyclePartition.ReleaseRecyclePartitionMetaBuf();

    BtrRecyclePartitionMetaPage *recycleMetaPage;
    BufferDesc *recycleMetaBuf = m_bufMgr->Read(g_defaultPdbId, recycleMetaPageId, LW_SHARED);
    recycleMetaPage = reinterpret_cast<BtrRecyclePartitionMetaPage *>(recycleMetaBuf->GetPage());
    m_bufMgr->UnlockAndRelease(recycleMetaBuf);

    recycleMetaPage->SetFreeQueueHead(INVALID_PAGE_ID);
    EXPECT_EQ(recycleMetaPage->GetFreeQueueHead(), INVALID_PAGE_ID);

    EXPECT_EQ(reyclePartition.AcquireRecyclePartitionMetaBuf(LW_SHARED), DSTORE_SUCC);
    EXPECT_FALSE(reyclePartition.IsValid(locksucc));
    reyclePartition.ReleaseRecyclePartitionMetaBuf();
}

TEST_F(BtreeRecyclePartitionTest, IsBtreeRecyclePartitionValid_level0) {
    Xid createdXid = indexSegment->m_btrRecyclePartition->createdXid;
    BtreeRecyclePartition reyclePartition(indexSegment, recycleMetaPageId, createdXid, m_bufMgr);
    PageId newPage = reyclePartition.AllocNewPage();
    EXPECT_EQ(reyclePartition.Init(), DSTORE_SUCC);

    EXPECT_EQ(reyclePartition.AcquireRecyclePartitionMetaBuf(LW_SHARED), DSTORE_SUCC);
    bool locksucc = true;
    EXPECT_TRUE(reyclePartition.IsValid(locksucc));
    reyclePartition.ReleaseRecyclePartitionMetaBuf();
}

TEST_F(BtreeRecyclePartitionTest, AccessTimestampTest_level0) {
    Xid createdXid = indexSegment->m_btrRecyclePartition->createdXid;
    BtreeRecyclePartition recyclePartition(indexSegment, recycleMetaPageId, createdXid, m_bufMgr);
    BtrRecyclePartitionMetaPage *recycleMetaPage;
    PageId pageId1 = indexSegment->GetNewPage();
    PageId pageId2 = indexSegment->GetNewPage();
    /**
     * Force set the timestamp to 0 so that
     *  we guarantee to update the timestamp
    */
    BufferDesc *recycleMetaBuf = m_bufMgr->Read(g_defaultPdbId, recycleMetaPageId, LW_EXCLUSIVE);
    recycleMetaPage = reinterpret_cast<BtrRecyclePartitionMetaPage *>(recycleMetaBuf->GetPage());
    recycleMetaPage->accessTimestamp = 0;
    TimestampTz ts = recycleMetaPage->accessTimestamp;
    m_bufMgr->UnlockAndRelease(recycleMetaBuf, BufferPoolUnlockContentFlag());
    recyclePartition.FreeListPush(pageId1);
    recycleMetaBuf = m_bufMgr->Read(g_defaultPdbId, recycleMetaPageId, LW_SHARED, BufferPoolReadFlag());
    recycleMetaPage = reinterpret_cast<BtrRecyclePartitionMetaPage *>(recycleMetaBuf->GetPage());
    TimestampTz ts_2 = recycleMetaPage->accessTimestamp;
    uint64 plsn = recycleMetaPage->GetPlsn();
    m_bufMgr->UnlockAndRelease(recycleMetaBuf, BufferPoolUnlockContentFlag());
    ASSERT_TRUE(ts_2 > ts);
    /**
     * The second time we access recycle list
     *  The time interval between the last time we update the timestamp is small,
     *   in this case the page shouldn't be updated at all.
    */
    recyclePartition.RecycleListPush({INVALID_CSN, pageId2});
    recycleMetaBuf = m_bufMgr->Read(g_defaultPdbId, recycleMetaPageId, LW_SHARED, BufferPoolReadFlag());
    recycleMetaPage = reinterpret_cast<BtrRecyclePartitionMetaPage *>(recycleMetaBuf->GetPage());
    recycleMetaPage->GetRecycleQueueHead();
    TimestampTz ts_3 = recycleMetaPage->accessTimestamp;
    ASSERT_EQ(recycleMetaPage->GetPlsn(), plsn);
    m_bufMgr->UnlockAndRelease(recycleMetaBuf, BufferPoolUnlockContentFlag());
    ASSERT_TRUE(ts_3 == ts_2);
}

TEST_F(BtreeRecyclePartitionTest, TakeOverTest_level0) {
    IndexSegment *indexSegment2 = DstoreNew(g_dstoreCurrentMemoryContext) IndexSegment(
        g_defaultPdbId, indexSegment->GetSegmentMetaPageId(), m_utTableHandler->m_tablespaceId, m_bufMgr, m_ut_memory_context);
    ASSERT_EQ(indexSegment2->InitSegment(), DSTORE_SUCC);
    g_storageInstance->GetGuc()->selfNodeId = 2;
    PageId newPageId = indexSegment2->GetNewPage();
    Xid createdXid1 = indexSegment->m_btrRecyclePartition->createdXid;
    Xid createdXid2 = indexSegment2->m_btrRecyclePartition->createdXid;
    ASSERT_TRUE(newPageId.IsValid());
    BtreeRecyclePartition *partition1 = indexSegment->m_btrRecyclePartition;
    BtreeRecyclePartition *partition2 = indexSegment2->m_btrRecyclePartition;

    partition1->AcquireRecyclePartitionMetaBuf(LW_SHARED);
    partition2->AcquireRecyclePartitionMetaBuf(LW_SHARED);

    BtreeRecycleQueue freeQ2 = partition2->GetBtreeRecycleQueue(BtrRecycleQueueType::FREE);
    BtreeRecycleQueue reuseQ2 = partition2->GetBtreeRecycleQueue(BtrRecycleQueueType::RECYCLE);
    BtreeRecycleQueue freeQ1 = partition1->GetBtreeRecycleQueue(BtrRecycleQueueType::FREE);
    BtreeRecycleQueue reuseQ1 = partition1->GetBtreeRecycleQueue(BtrRecycleQueueType::RECYCLE);

    partition1->ReleaseRecyclePartitionMetaBuf();
    partition2->ReleaseRecyclePartitionMetaBuf();

    BufferDesc *freeQ1HeadBuf = freeQ1.GetQueueHeadBuf(LW_SHARED);
    BufferDesc *reuseQ1HeadBuf = reuseQ1.GetQueueHeadBuf(LW_SHARED);
    PageId freeQ1HeadId = freeQ1HeadBuf->GetPageId();
    PageId reuseQ1HeadId = reuseQ1HeadBuf->GetPageId();

    BufferDesc *freeQ2HeadBuf = freeQ2.GetQueueHeadBuf(LW_SHARED);
    BufferDesc *reuseQ2HeadBuf = reuseQ2.GetQueueHeadBuf(LW_SHARED);
    PageId freeQ2HeadId = freeQ2HeadBuf->GetPageId();
    PageId reuseQ2HeadId = reuseQ2HeadBuf->GetPageId();

    m_bufMgr->UnlockAndRelease(freeQ1HeadBuf);
    m_bufMgr->UnlockAndRelease(reuseQ1HeadBuf);
    m_bufMgr->UnlockAndRelease(freeQ2HeadBuf);
    m_bufMgr->UnlockAndRelease(reuseQ2HeadBuf);

    ASSERT_NE(freeQ1HeadId, freeQ2HeadId);
    ASSERT_NE(reuseQ1HeadId, reuseQ2HeadId);

    BufferDesc* freeQ2TailBuf = freeQ2.GetQueueTailBuf(LW_SHARED, createdXid2);
    BufferDesc* reuseQ2TailBuf = reuseQ2.GetQueueTailBuf(LW_SHARED, createdXid2);
    BufferDesc* freeQ1TailBuf = freeQ1.GetQueueTailBuf(LW_SHARED, createdXid2);
    BufferDesc* reuseQ1TailBuf = reuseQ1.GetQueueTailBuf(LW_SHARED, createdXid2);

    PageId freeQ1TailId = freeQ1TailBuf->GetPageId();
    PageId reuseQ1TailId = reuseQ1TailBuf->GetPageId();
    PageId freeQ2TailId = freeQ2TailBuf->GetPageId();
    PageId reuseQ2TailId = reuseQ2TailBuf->GetPageId();

    m_bufMgr->UnlockAndRelease(freeQ1TailBuf);
    m_bufMgr->UnlockAndRelease(reuseQ1TailBuf);
    m_bufMgr->UnlockAndRelease(freeQ2TailBuf);
    m_bufMgr->UnlockAndRelease(reuseQ2TailBuf);

    ASSERT_NE(freeQ1TailId, freeQ2TailId);
    ASSERT_NE(reuseQ1TailId, reuseQ2TailId);

    indexSegment->m_btrRecyclePartition->AcquireRecyclePartitionMetaBuf(LW_SHARED);
    ASSERT_EQ(indexSegment->m_btrRecyclePartition->TakeOverColdRecyclePartition(indexSegment2->m_btrRecyclePartition),
              DSTORE_SUCC);
    indexSegment->m_btrRecyclePartition->ReleaseRecyclePartitionMetaBuf();
    partition1->AcquireRecyclePartitionMetaBuf(LW_SHARED);
    BtreeRecycleQueue freeQ1After = partition1->GetBtreeRecycleQueue(BtrRecycleQueueType::FREE);
    BtreeRecycleQueue reuseQ1After = partition1->GetBtreeRecycleQueue(BtrRecycleQueueType::RECYCLE);
    partition1->ReleaseRecyclePartitionMetaBuf();
    BufferDesc *freeQ1AfterHeadBuf = freeQ1After.GetQueueHeadBuf(LW_SHARED);
    BufferDesc *reuseQ1AfterHeadBuf = reuseQ1After.GetQueueHeadBuf(LW_SHARED);
    PageId freeQ1AfterHeadId = freeQ1AfterHeadBuf->GetPageId();
    PageId reuseQ1AfterHeadId = reuseQ1AfterHeadBuf->GetPageId();

    m_bufMgr->UnlockAndRelease(freeQ1AfterHeadBuf);
    m_bufMgr->UnlockAndRelease(reuseQ1AfterHeadBuf);

    BufferDesc* freeQ1AfterTailBuf = freeQ1After.GetQueueTailBuf(LW_SHARED, createdXid1);
    BufferDesc* reuseQ1AfterTailBuf = reuseQ1After.GetQueueTailBuf(LW_SHARED, createdXid1);
    PageId freeQ1AfterTailId = freeQ1AfterTailBuf->GetPageId();
    PageId reuseQ1AfterTailId = reuseQ1AfterTailBuf->GetPageId();
    m_bufMgr->UnlockAndRelease(freeQ1AfterTailBuf);
    m_bufMgr->UnlockAndRelease(reuseQ1AfterTailBuf);
    ASSERT_EQ(freeQ1AfterTailId, freeQ2TailId);
    ASSERT_EQ(freeQ1AfterHeadId, freeQ1HeadId);
    freeQ1TailBuf = m_bufMgr->Read(g_defaultPdbId, freeQ1TailId, LW_SHARED);
    BtrQueuePage *freeQ1TailPage = static_cast<BtrQueuePage*>(freeQ1TailBuf->GetPage());
    ASSERT_EQ(freeQ1TailPage->GetNext(), freeQ2HeadId);
    m_bufMgr->UnlockAndRelease(freeQ1TailBuf);
}

TEST_F(BtreeRecyclePartitionTest, AllocPartitionBgExtendTest) {
    /* Step 1. Initialization */
    MockIndexSegment *indexSegment = MockIndexSegment::AllocMockIndexSegment(
        g_defaultPdbId, SegmentType::INDEX_SEGMENT_TYPE, m_utTableHandler->m_tablespaceId, m_bufMgr);
    BtreeRecyclePartition reyclePartition(indexSegment, recycleMetaPageId, INVALID_XID, m_bufMgr);

    /* Step 2. Make recycle head invalid */
    BtrRecyclePartitionMetaPage *recycleMetaPage;
    LockTag tag;
    tag.SetTableExtensionLockTag(g_defaultPdbId, reyclePartition.recyclePartitionMeta);
    LockErrorInfo errorInfo = {0};
    LockMgr *lockMgr = g_storageInstance->GetLockMgr();
    ASSERT_EQ(lockMgr->Lock(&tag, DSTORE_EXCLUSIVE_LOCK, false, &errorInfo), DSTORE_SUCC);
    reyclePartition.TryRegisterExtendTask();
    bool isTaskRegister = ObjSpaceMgr::IsExtensionIndexTaskRegistered(
        indexSegment->GetTablespaceId(), indexSegment->GetSegmentMetaPageId(), reyclePartition.recyclePartitionMeta,
        INVALID_XID, g_defaultPdbId);
    ASSERT_EQ(isTaskRegister, true);
    lockMgr->Unlock(&tag, DSTORE_EXCLUSIVE_LOCK);
    
    delete indexSegment;
}
}
