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
#ifndef UT_LOCAL_BTREE_RECYCLE_H
#define UT_LOCAL_BTREE_RECYCLE_H

#include "gtest/gtest.h"
#include "index/dstore_btree_recycle_partition.h"
#include "page/dstore_btr_queue_page.h"
#include "ut_tablehandler/ut_table_handler.h"
#include "index/dstore_btree_insert.h"
#include "index/dstore_index_handler.h"
#include "ut_mock/ut_segment_mock.h"
#include "page/dstore_segment_meta_page.h"

using namespace DSTORE;

class BtreeRecyclePartitionTest : public DSTORETEST {
protected:
    void SetUp() override {
        DSTORETEST::SetUp();
        MockStorageInstance *instance = DstoreNew(m_ut_memory_context) MockStorageInstance();
        instance->Install(&DSTORETEST::m_guc, m_ut_memory_context);
        instance->Startup(&DSTORETEST::m_guc);
        Transaction *trans = thrd->GetActiveTransaction();
        trans->Start();
        m_utTableHandler = UTTableHandler::CreateTableHandler(g_defaultPdbId, m_ut_memory_context, false);
        m_bufMgr = g_storageInstance->GetBufferMgr();
        indexSegment = PrepareForIndexSegment(g_defaultPdbId, m_utTableHandler->m_tablespaceId, m_bufMgr);
        StorageAssert(indexSegment != NULL);

        /* Init Recyclable Page Queue */
        recycleQPageId = indexSegment->GetFromFreeQueue();
        Xid currXid = indexSegment->m_btrRecyclePartition->createdXid;
        StorageAssert(currXid == thrd->GetCurrentXid());
        BufferDesc *recycleQBuf = m_bufMgr->Read(g_defaultPdbId, recycleQPageId, LW_EXCLUSIVE);
        BtrQueuePage *recycleQPage = reinterpret_cast<BtrQueuePage *>(recycleQBuf->GetPage());
        recycleQPage->Init<RecyclablePageQueue>(recycleQPageId, currXid);
        m_bufMgr->MarkDirty(recycleQBuf);
        m_bufMgr->UnlockAndRelease(recycleQBuf);

        /* Init Reusable Page Queue */
        freeQPageId = indexSegment->GetFromFreeQueue();
        BufferDesc *freeQBuf = m_bufMgr->Read(g_defaultPdbId, freeQPageId, LW_EXCLUSIVE);
        BtrQueuePage *freeQPage = reinterpret_cast<BtrQueuePage *>(freeQBuf->GetPage());
        freeQPage->Init<ReusablePageQueue>(freeQPageId, currXid);

        /* Init 2 slotted free queue. */
        uint16 queueFreeSpace = 2 * sizeof(ReusablePageSlot) + offsetof(ReusablePageQueue, reusablePages);
        ReusablePageQueue *freeQ = freeQPage->GetQueue<ReusablePageQueue>();
        freeQ->Init(queueFreeSpace);

        m_bufMgr->MarkDirty(freeQBuf);
        m_bufMgr->UnlockAndRelease(freeQBuf);

        /* Init Recycle Partition Meta Page */
        recycleMetaPageId = indexSegment->GetFromFreeQueue();
        BufferDesc *recycleMetaBuf = m_bufMgr->Read(g_defaultPdbId, recycleMetaPageId, LW_EXCLUSIVE);
        BtrRecyclePartitionMetaPage *recycleMetaPage;
        recycleMetaPage = reinterpret_cast<BtrRecyclePartitionMetaPage *>(recycleMetaBuf->GetPage());
        recycleMetaPage->InitRecyclePartitionMetaPage(recycleMetaPageId, currXid);
        recycleMetaPage->SetRecycleQueueHead(recycleQPageId);
        recycleMetaPage->SetFreeQueueHead(freeQPageId);
        m_bufMgr->MarkDirty(recycleMetaBuf);
        m_bufMgr->UnlockAndRelease(recycleMetaBuf);
        trans->Commit();
    }

    void TearDown() override {
        UTTableHandler::Destroy(m_utTableHandler);
        m_utTableHandler = nullptr;
        MockStorageInstance *instance = (MockStorageInstance *)g_storageInstance;
        instance->Shutdown();
        delete instance;
        DSTORETEST::TearDown();
    }
    IndexNormalSegment* PrepareForIndexSegment(PdbId pdbId, TablespaceId tablespaceId, BufMgrInterface *bufMgr);
    UTTableHandler *m_utTableHandler;
    BufMgrInterface *m_bufMgr;
    IndexNormalSegment *indexSegment;

    PageId recycleMetaPageId;
    PageId recycleQPageId;
    PageId freeQPageId;
};

#endif // UT_LOCAL_BTREE_RECYCLE_H
