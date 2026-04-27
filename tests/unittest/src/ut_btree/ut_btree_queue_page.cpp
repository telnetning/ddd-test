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

class UTBtreeQueuePageTest : public DSTORETEST {
protected:
    void SetUp() override {
      queuePage1->Init<ReusablePageQueue>(pageId1, createXid);
      queuePage2->Init<ReusablePageQueue>(pageId2, createXid);
    }

    void TearDown() override {
    }
    char block1[BLCKSZ];
    char block2[BLCKSZ];
    BtrQueuePage *queuePage1 = (BtrQueuePage *)block1;
    BtrQueuePage *queuePage2 = (BtrQueuePage *)block2;

    PageId pageId1 = {0, 1};
    PageId pageId2 = {0, 2};
    Xid createXid = Xid(0, 0);
};

TEST_F(UTBtreeQueuePageTest, IsEmptyInitially_level0) {
  EXPECT_EQ(queuePage1->IsTail(), true);
}

TEST_F(UTBtreeQueuePageTest, SetNextAndGetNext_level0) {
  queuePage1->SetNext(pageId2);
  PageId item = queuePage1->GetNext();
  EXPECT_EQ(item, pageId2);
  EXPECT_EQ(queuePage1->IsTail(), false);
}
}