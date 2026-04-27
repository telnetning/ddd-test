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
#include "ut_btree/ut_btree_recycle_queue.h"
#include "page/dstore_btr_recycle_partition_meta_page.h"

namespace DSTORE {

TEST_F(BtreeRecycleQueueTest, Initialization_level0) {
    /* Test QueueMetaPage is initialized correctly as invalid page id */
    BufferDesc *qBuf = m_bufMgr->Read(g_defaultPdbId, qPageId1, LW_SHARED);
    BtrQueuePage *qPage = reinterpret_cast<BtrQueuePage *>(qBuf->GetPage());
    uint16 metaPageOffset = qPage->GetSpecialOffset();
    PageId *metaPageId = (PageId *) &qBuf->bufBlock[metaPageOffset];
    EXPECT_FALSE(metaPageId->IsValid());
    m_bufMgr->UnlockAndRelease(qBuf);
}

TEST_F(BtreeRecycleQueueTest, AddAndRemoveTailPage_level0) {
    /* Step 1. Initialize the BtreeRecycleQueue */
    BtreeRecycleQueue recycleQueue(g_defaultPdbId, qPageId1, BtrRecycleQueueType::RECYCLE, m_bufMgr);

    /* Step 2. Test AddTailPage */
    BufferDesc *oldTailBuf;
    oldTailBuf = m_bufMgr->Read(g_defaultPdbId,qPageId1, LW_EXCLUSIVE);
    recycleQueue.AddTailPage(oldTailBuf, qPageId2);
    m_bufMgr->UnlockAndRelease(oldTailBuf);

    oldTailBuf = m_bufMgr->Read(g_defaultPdbId, qPageId2, LW_EXCLUSIVE);
    recycleQueue.AddTailPage(oldTailBuf, qPageId3);
    m_bufMgr->UnlockAndRelease(oldTailBuf);

    BufferDesc *qBuf1 = m_bufMgr->Read(g_defaultPdbId, qPageId1, LW_SHARED);
    BtrQueuePage *qPage1 = reinterpret_cast<BtrQueuePage *>(qBuf1->GetPage());
    EXPECT_EQ(qPage1->GetNext(), qPageId2);
    m_bufMgr->UnlockAndRelease(qBuf1);

    BufferDesc *qBuf2 = m_bufMgr->Read(g_defaultPdbId, qPageId2, LW_SHARED);
    BtrQueuePage *qPage2 = reinterpret_cast<BtrQueuePage *>(qBuf2->GetPage());
    EXPECT_EQ(qPage2->GetNext(), qPageId3);
    m_bufMgr->UnlockAndRelease(qBuf2);

    /* Step 3. Test RemoveTailPage */
    qBuf1 = m_bufMgr->Read(g_defaultPdbId, qPageId1, LW_EXCLUSIVE);
    qBuf2 = recycleQueue.RemoveTailPage(qBuf1);;
    EXPECT_EQ(qPageId2, qBuf2->GetPageId());
    m_bufMgr->UnlockAndRelease(qBuf1);
    m_bufMgr->UnlockAndRelease(qBuf2);
}

TEST_F(BtreeRecycleQueueTest, RemovePageTest_level0) {
    /* Step 1. Initialize the BtreeRecycleQueue */
    BtreeRecycleQueue recycleQueue(g_defaultPdbId, qPageId1, BtrRecycleQueueType::RECYCLE, m_bufMgr);

    /* Step 2. Test AddTailPage */
    BufferDesc *oldTailBuf;
    oldTailBuf = m_bufMgr->Read(g_defaultPdbId, qPageId1, LW_EXCLUSIVE);
    recycleQueue.AddTailPage(oldTailBuf, qPageId2);
    m_bufMgr->UnlockAndRelease(oldTailBuf);

    oldTailBuf = m_bufMgr->Read(g_defaultPdbId, qPageId2, LW_EXCLUSIVE);
    recycleQueue.AddTailPage(oldTailBuf, qPageId3);
    m_bufMgr->UnlockAndRelease(oldTailBuf);

    BufferDesc *qBuf1 = m_bufMgr->Read(g_defaultPdbId, qPageId1, LW_EXCLUSIVE);
    BtrQueuePage *qPage1 = reinterpret_cast<BtrQueuePage *>(qBuf1->GetPage());
    EXPECT_EQ(qPage1->GetNext(), qPageId2);

    BufferDesc *qBuf2 = m_bufMgr->Read(g_defaultPdbId, qPageId2, LW_EXCLUSIVE);
    BtrQueuePage *qPage2 = reinterpret_cast<BtrQueuePage *>(qBuf2->GetPage());
    EXPECT_EQ(qPage2->GetNext(), qPageId3);

    /* Step 3. Test RemovePage */
    recycleQueue.RemovePage(qBuf1, qBuf2);

    EXPECT_EQ(qPage1->GetNext(), qPageId3);

    m_bufMgr->UnlockAndRelease(qBuf1);
    m_bufMgr->UnlockAndRelease(qBuf2);
}

TEST_F(BtreeRecycleQueueTest, StepNextPageTest_level0) {
    /* Step 1. Initialize the BtreeRecycleQueue */
    BtreeRecycleQueue recycleQueue(g_defaultPdbId, qPageId1, BtrRecycleQueueType::RECYCLE, m_bufMgr);

    /* Step 2. Test AddTailPage */
    BufferDesc *oldTailBuf;
    oldTailBuf = m_bufMgr->Read(g_defaultPdbId, qPageId1, LW_EXCLUSIVE);
    BtrQueuePage *oldTailPage = reinterpret_cast<BtrQueuePage *>(oldTailBuf->GetPage());
    recycleQueue.AddTailPage(oldTailBuf, qPageId2);
    EXPECT_EQ(oldTailPage->GetNext(), qPageId2);
    m_bufMgr->UnlockAndRelease(oldTailBuf);

    /* Step 3. Test StepNextPage when pass in invalid buffer */
    BufferDesc *nextBuf = recycleQueue.StepNextPage(INVALID_BUFFER_DESC, LW_EXCLUSIVE);
    EXPECT_EQ(nextBuf, nullptr);

    /* Step 4. Test StepNextPage when pass in valid buffer */
    BufferDesc *firstPageBuf = m_bufMgr->Read(g_defaultPdbId, qPageId1, LW_EXCLUSIVE);
    nextBuf = recycleQueue.StepNextPage(firstPageBuf, LW_EXCLUSIVE);
    EXPECT_EQ(nextBuf->GetPageId(), qPageId2);

    /* Step 5. Test StepNextPage on tail page */
    nextBuf = recycleQueue.StepNextPage(nextBuf, LW_EXCLUSIVE);
    EXPECT_EQ(nextBuf, nullptr);
}

TEST_F(BtreeRecycleQueueTest, GetQueueTailBuf_level0) {
    /* Step 1. Initialize the Queue */
    PageId pageId1 = {0, 1};

    BufferDesc *qBuf1 = m_bufMgr->Read(g_defaultPdbId, qPageId1, LW_EXCLUSIVE);
    BtrQueuePage *qPage1 = reinterpret_cast<BtrQueuePage *>(qBuf1->GetPage());
    BufferDesc *qBuf2 = m_bufMgr->Read(g_defaultPdbId, qPageId2, LW_EXCLUSIVE);
    BtrQueuePage *qPage2 = reinterpret_cast<BtrQueuePage *>(qBuf2->GetPage());

    RecyclablePageQueue *queue1 = qPage1->GetQueue<RecyclablePageQueue>();
    RecyclablePageQueue *queue2 = qPage2->GetQueue<RecyclablePageQueue>();
    queue1->Init(queueFreeSpace);
    queue2->Init(queueFreeSpace);

    m_bufMgr->MarkDirty(qBuf1);
    m_bufMgr->MarkDirty(qBuf2);
    m_bufMgr->UnlockAndRelease(qBuf1);
    m_bufMgr->UnlockAndRelease(qBuf2);

    /* Step 2. Initialize the BtreeRecycleQueue */
    BtreeRecycleQueue recycleQueue(g_defaultPdbId, qPageId1, BtrRecycleQueueType::RECYCLE, m_bufMgr);

    BufferDesc *oldTailBuf = m_bufMgr->Read(g_defaultPdbId, qPageId1, LW_EXCLUSIVE);
    recycleQueue.AddTailPage(oldTailBuf, qPageId2);
    m_bufMgr->UnlockAndRelease(oldTailBuf);

    /* Step 3. Test GetQueueTailBuf when the first queue has item and second queue is empty */
    queue1->Push({1, pageId1});
    EXPECT_EQ(queue1->IsEmpty(INVALID_PDB_ID), false);
    EXPECT_EQ(queue2->IsEmpty(INVALID_PDB_ID), true);

    BufferDesc *buffer = recycleQueue.GetQueueTailBuf(LW_EXCLUSIVE, createdXid);
    BtrQueuePage *queuePage = (BtrQueuePage *)buffer->GetPage();
    EXPECT_EQ(qPageId2, queuePage->GetSelfPageId());
    m_bufMgr->UnlockAndRelease(buffer);
}
}
