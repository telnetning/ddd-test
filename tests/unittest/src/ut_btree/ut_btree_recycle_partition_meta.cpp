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
#include <gtest/gtest.h>
#include "page/dstore_btr_recycle_partition_meta_page.h"
#include "ut_btree/ut_btree.h"
#include "ut_mock/ut_buf_mgr_mock.h"
#include "ut_tablespace/ut_segment.h"

namespace DSTORE {

class UTBtreeRecyclePartitionMetaPageTest: public DSTORETEST {
protected:
    void SetUp() override {
        DSTORETEST::SetUp();
        MockStorageInstance *instance = DstoreNew(m_ut_memory_context) MockStorageInstance();
        instance->Install(&DSTORETEST::m_guc, m_ut_memory_context);
        instance->Startup(&DSTORETEST::m_guc);
        m_bufMgr = g_storageInstance->GetBufferMgr();
        m_utTableHandler = UTTableHandler::CreateTableHandler(g_defaultPdbId, m_ut_memory_context, false);
        m_transaction = thrd->GetActiveTransaction();
        m_transaction->Start();
        indexSegment = (IndexNormalSegment*)SegmentTest::UTAllocSegment(g_defaultPdbId,
            m_utTableHandler->m_tablespaceId, m_bufMgr, SegmentType::INDEX_SEGMENT_TYPE);
        recyclePartitionMeta = indexSegment->GetNewPageFromUnassignedPages();
        recycleQueueHead = indexSegment->GetNewPageFromUnassignedPages();
        unlinkQueueHead = indexSegment->GetNewPageFromUnassignedPages();
        freeQueueHead = indexSegment->GetNewPageFromUnassignedPages();
    }

    void TearDown() override {
        m_transaction->Commit();
        indexSegment->DropSegment();
        indexSegment = nullptr;
        UTTableHandler::Destroy(m_utTableHandler);
        m_utTableHandler = nullptr;
        MockStorageInstance *instance = (MockStorageInstance *)g_storageInstance;
        instance->Shutdown();
        delete instance;
        DSTORETEST::TearDown();
    }
    Transaction *m_transaction;
    BufMgrInterface *m_bufMgr;
    UTTableHandler *m_utTableHandler;
    IndexNormalSegment *indexSegment;
    PageId recyclePartitionMeta;
    PageId recycleQueueHead;
    PageId unlinkQueueHead;
    PageId freeQueueHead;
};

TEST_F(UTBtreeRecyclePartitionMetaPageTest, SetAndGetHead_level0) {
    BufferDesc *buffer = m_bufMgr->Read(g_defaultPdbId, recyclePartitionMeta, LW_EXCLUSIVE);
    BtrRecyclePartitionMeta partitionMeta(g_defaultPdbId, recyclePartitionMeta, buffer, false);
    BtrRecyclePartitionMetaPage *page = (BtrRecyclePartitionMetaPage *)buffer->GetPage();
    page->InitRecyclePartitionMetaPage(recyclePartitionMeta, INVALID_XID);
    partitionMeta.SetQueueHead(BtrRecycleQueueType::RECYCLE, recycleQueueHead);
    partitionMeta.SetQueueHead(BtrRecycleQueueType::FREE, freeQueueHead);
    m_bufMgr->MarkDirty(buffer);

    EXPECT_EQ(partitionMeta.GetQueueHead(BtrRecycleQueueType::RECYCLE), recycleQueueHead);
    EXPECT_EQ(partitionMeta.GetQueueHead(BtrRecycleQueueType::FREE), freeQueueHead);
    m_bufMgr->UnlockAndRelease(buffer);
}
}