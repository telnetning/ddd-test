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

class UTBtreeRecyclablePageQueueTest : public DSTORETEST {
protected:
    void SetUp() override {
      DSTORETEST::SetUp();
      MockStorageInstance *instance = DstoreNew(m_ut_memory_context) MockStorageInstance();
      instance->Install(&DSTORETEST::m_guc, m_ut_memory_context);
      instance->Startup(&DSTORETEST::m_guc);
      uint16 offset = offsetof(RecyclablePageQueue, recyclablePages);
      uint16 capacity = 2;
      queue->Init(capacity * sizeof(RecyclablePage) + offset);
    }

    void TearDown() override {
      MockStorageInstance *instance = (MockStorageInstance *)g_storageInstance;
      instance->Shutdown();
      delete instance;
      DSTORETEST::TearDown();
    }

    char block[BLCKSZ];
    RecyclablePageQueue *queue = (RecyclablePageQueue *)block;
    PageId recyclablePageId = {0, 1};
    PageId recyclablePageId2 = {0, 2};
    RecyclablePage recyclablePage = (RecyclablePage) {1, recyclablePageId};
    RecyclablePage recyclablePage2 = (RecyclablePage) {2, recyclablePageId2};
};

TEST_F(UTBtreeRecyclablePageQueueTest, IsEmptyInitially_level0) {
  EXPECT_EQ(queue->IsEmpty(INVALID_PDB_ID), true);
}

TEST_F(UTBtreeRecyclablePageQueueTest, IsFull_level0) {
  queue->Push(recyclablePage);
  queue->Push(recyclablePage2);
  EXPECT_EQ(queue->IsFull(), true);
}

TEST_F(UTBtreeRecyclablePageQueueTest, Peek_level0) {
  /* Test Peek when queue is empty */
  int index = queue->Peek(0);
  EXPECT_EQ(index, -1);

  queue->Push(recyclablePage);
  queue->Push(recyclablePage2);

  /* Test Peek when queue has items */
  index = queue->Peek(2);
  EXPECT_EQ(index, 0);

  /* Test Peek when no item is smaller than the minCsn that is passed in */
  index = queue->Peek(1);
  EXPECT_EQ(index, -1);

  index = queue->Peek(0);
  EXPECT_EQ(index, -1);
}

TEST_F(UTBtreeRecyclablePageQueueTest, PushAndPop_level0) {
  queue->Push(recyclablePage);
  queue->Push(recyclablePage2);

  PageId item = queue->Pop(0);
  EXPECT_EQ(item, recyclablePageId);

  item = queue->Pop(1);
  EXPECT_EQ(item, recyclablePageId2);
}
}