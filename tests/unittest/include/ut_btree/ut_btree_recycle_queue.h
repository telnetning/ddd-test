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
#ifndef UT_BTREE_RECYCLE_QUEUE_H
#define UT_BTREE_RECYCLE_QUEUE_H

#include <gtest/gtest.h>
#include "ut_mock/ut_buf_mgr_mock.h"
#include "ut_mock/ut_instance_mock.h"
#include "index/dstore_btree_recycle_queue.h"
#include "page/dstore_btr_queue_page.h"
#include "ut_btree/ut_btree.h"
#include "ut_tablespace/ut_segment.h"

void InitRecyclablePageQueue(BufMgrInterface *bufMgr, PageId pageId, Xid xid) {
    BufferDesc *qBuf = bufMgr->Read(g_defaultPdbId, pageId, LW_EXCLUSIVE);
    BtrQueuePage *qPage = reinterpret_cast<BtrQueuePage *>(qBuf->GetPage());
    qPage->Init<RecyclablePageQueue>(pageId, xid);
    bufMgr->MarkDirty(qBuf);
    bufMgr->UnlockAndRelease(qBuf);
}
class BtreeRecycleQueueTest : public DSTORETEST {
protected:
    void SetUp() override {
        DSTORETEST::SetUp();
        MockStorageInstance *instance = DstoreNew(m_ut_memory_context) MockStorageInstance();
        instance->Install(&DSTORETEST::m_guc, m_ut_memory_context);
        instance->Startup(&DSTORETEST::m_guc);
        m_bufMgr = g_storageInstance->GetBufferMgr();
        Transaction *transaction = thrd->GetActiveTransaction();
        transaction->Start();
        transaction->GetSnapshot();
        transaction->AllocTransactionSlot();
        createdXid = thrd->GetCurrentXid();
        m_utTableHandler = UTTableHandler::CreateTableHandler(g_defaultPdbId, m_ut_memory_context, false);
        indexSegment = (IndexNormalSegment*)SegmentTest::UTAllocSegment(g_defaultPdbId,
            m_utTableHandler->m_tablespaceId, m_bufMgr, SegmentType::INDEX_SEGMENT_TYPE);

        qPageId1 = indexSegment->GetNewPageFromUnassignedPages();
        qPageId2 = indexSegment->GetNewPageFromUnassignedPages();
        qPageId3 = indexSegment->GetNewPageFromUnassignedPages();
        InitRecyclablePageQueue(m_bufMgr, qPageId1, createdXid);
        InitRecyclablePageQueue(m_bufMgr, qPageId2, createdXid);
        InitRecyclablePageQueue(m_bufMgr, qPageId3, createdXid);
        transaction->Commit();
    }

    void TearDown() override {
        indexSegment->DropSegment();
        indexSegment = nullptr;
        UTTableHandler::Destroy(m_utTableHandler);
        m_utTableHandler = nullptr;
        MockStorageInstance *instance = (MockStorageInstance *)g_storageInstance;
        instance->Shutdown();
        delete instance;
        DSTORETEST::TearDown();
    }

    BufMgrInterface *m_bufMgr;
    UTTableHandler *m_utTableHandler;
    IndexNormalSegment *indexSegment;
    PageId qPageId1;
    PageId qPageId2;
    PageId qPageId3;
    Xid createdXid;
    const uint16 queueFreeSpace = 2 * sizeof(RecyclablePage) + offsetof(RecyclablePageQueue, recyclablePages);
};

#endif //UT_BTREE_RECYCLE_QUEUE_H
