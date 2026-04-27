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
#include "gtest/gtest.h"
#include "page/dstore_btr_queue_page.h"
#include "ut_btree/ut_btree.h"

namespace DSTORE {

class UTBtreeReusablePageQueueTest : public DSTORETEST {
protected:
    void SetUp() override {
      DSTORETEST::SetUp();
      MockStorageInstance *instance = DstoreNew(m_ut_memory_context) MockStorageInstance();
      instance->Install(&DSTORETEST::m_guc, m_ut_memory_context);
      instance->Startup(&DSTORETEST::m_guc);
      uint16 offset = offsetof(ReusablePageQueue, reusablePages);
      uint16 capacity = 3;
      queue->Init(capacity * sizeof(ReusablePageSlot) + offset);
    }

    void TearDown() override {
      MockStorageInstance *instance = (MockStorageInstance *)g_storageInstance;
      instance->Shutdown();
      delete instance;
      DSTORETEST::TearDown();
    }

    char block[BLCKSZ];
    ReusablePageQueue *queue = (ReusablePageQueue *)block;
    PageId pageId1 = {0, 1};
    PageId pageId2 = {0, 2};
    PageId pageId3 = {0, 3};
    ReusablePage reusablePage1 = (ReusablePage) {1, pageId1};
    ReusablePage reusablePage2 = (ReusablePage) {2, pageId2};
    ReusablePage reusablePage3 = (ReusablePage) {3, pageId3};
};

TEST_F(UTBtreeReusablePageQueueTest, IsEmptyInitially_level0) {
  EXPECT_EQ(queue->IsEmpty(INVALID_PDB_ID), true);
}

TEST_F(UTBtreeReusablePageQueueTest, IsFull_level0) {
  queue->Push(reusablePage1);
  queue->Push(reusablePage2);
  queue->Push(reusablePage3);
  EXPECT_EQ(queue->IsFull(), true);
}

TEST_F(UTBtreeReusablePageQueueTest, Peek_level0) {
  /* Test Peek when queue is empty */
  int index = queue->Peek(0);
  EXPECT_EQ(index, -1);

  queue->Push(reusablePage1);
  queue->Push(reusablePage2);

  /* Test Peek when queue has items */
  index = queue->Peek(2);
  EXPECT_EQ(index, 0);

  /* Test Peek when no item is smaller than the minCsn that is passed in */
  index = queue->Peek(1);
  EXPECT_EQ(index, -1);

  index = queue->Peek(0);
  EXPECT_EQ(index, -1);
}

TEST_F(UTBtreeReusablePageQueueTest, PushAndPop_level0) {
  queue->Push(reusablePage1);
  queue->Push(reusablePage2);

  PageId item = queue->Pop(0);
  EXPECT_EQ(item, pageId1);

  item = queue->Pop(1);
  EXPECT_EQ(item, pageId2);
}

TEST_F(UTBtreeReusablePageQueueTest, AllocAndWriteSlot_level0) {
  CommitSeqNo nextCsn = INVALID_CSN;
  g_storageInstance->GetCsnMgr()->GetNextCsn(nextCsn, false);

  /* Test AllocSlot and WriteSlot when queue has item */
  queue->Push(reusablePage1);

  uint16 slot = queue->AllocSlot({nextCsn, reusablePage2.pageId});
  queue->WriteSlot(slot, reusablePage2);

  PageId item = queue->Pop(0);
  EXPECT_EQ(item, pageId1);

  item = queue->Pop(1);
  EXPECT_EQ(item, pageId2);

  /* Test AllocSlot and WriteSlot when queue is empty */
  slot = queue->AllocSlot({nextCsn, reusablePage1.pageId});
  queue->WriteSlot(slot, reusablePage1);

  item = queue->Pop(0);
  EXPECT_EQ(item, pageId1);
}

TEST_F(UTBtreeReusablePageQueueTest, AllocSlotAfterPop_level0) {
  CommitSeqNo nextCsn = INVALID_CSN;
  g_storageInstance->GetCsnMgr()->GetNextCsn(nextCsn, false);

  uint16 slot1 = queue->AllocSlot({nextCsn, reusablePage1.pageId});
  uint16 slot2 = queue->AllocSlot({nextCsn, reusablePage2.pageId});
  queue->Push(reusablePage3);

  PageId item = queue->Pop(2);
  EXPECT_EQ(item, pageId3);

  /* Verify allocated slot is swapped by Pop */
  EXPECT_EQ(queue->reusablePages[1].reusablePage.pageId, reusablePage2.pageId);
  EXPECT_EQ(queue->reusablePages[2].reusablePage.pageId, reusablePage1.pageId);

  /* Try WriteSlot to the unmatched slot number */
  EXPECT_EQ(queue->numAllocatedSlots, 2);
  EXPECT_EQ(queue->WriteSlot(slot1, reusablePage1), 2);
  EXPECT_EQ(queue->WriteSlot(slot2, reusablePage2), 1);

  /* Verify slots are written correctly */
  EXPECT_EQ(queue->numAllocatedSlots, 0);
  EXPECT_EQ(queue->reusablePages[1].isUsed, 1);
  EXPECT_EQ(queue->reusablePages[2].isUsed, 1);
  EXPECT_EQ(queue->reusablePages[1].reusablePage.pageId, reusablePage2.pageId);
  EXPECT_EQ(queue->reusablePages[2].reusablePage.pageId, reusablePage1.pageId);
}

TEST_F(UTBtreeReusablePageQueueTest, IsEmptyWhenAllocated_level0) {
  CommitSeqNo nextCsn = INVALID_CSN;
  g_storageInstance->GetCsnMgr()->GetNextCsn(nextCsn, false);

  /* Test IsEmpty when queue is full of allocated slots */
  queue->AllocSlot({nextCsn, reusablePage1.pageId});
  queue->AllocSlot({nextCsn, reusablePage2.pageId});
  queue->AllocSlot({nextCsn, reusablePage3.pageId});
  EXPECT_EQ(queue->IsFull(), true);
  EXPECT_EQ(queue->IsEmpty(INVALID_PDB_ID), false);

  /* Set local csn, test IsEmpty when all slots are reusable */
  g_storageInstance->GetCsnMgr()->SetLocalCsnMin(100);
  EXPECT_EQ(queue->IsEmpty(INVALID_PDB_ID), true);
}
}