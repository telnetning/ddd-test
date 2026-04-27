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
 * dstore_btr_recycle_partition_meta_page.cpp
 *
 * IDENTIFICATION
 *        src/page/dstore_btr_recycle_partition_meta_page.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "framework/dstore_instance.h"
#include "index/dstore_btree_recycle_wal.h"
#include "wal/dstore_wal_write_context.h"
#include "page/dstore_btr_recycle_partition_meta_page.h"

namespace DSTORE {

bool BtrRecyclePartitionMeta::IsEmpty() const
{
    PageId recycleQueueHeadPageId = GetQueueHead(BtrRecycleQueueType::RECYCLE);
    BtreeRecycleQueue recycleQueue(m_pdbId, recycleQueueHeadPageId, BtrRecycleQueueType::RECYCLE, bufMgr);
    /* Note: BtreeRecycleQueue::IsEmpty would LW_SHARED lock the head page.
     * we'll check the recycle queue first and return once it's not empty, which can reduce an unnecessary lock on
     * free queue if recycle queue is not empty. */
    if (!recycleQueue.IsEmpty<RecyclablePageQueue>()) {
        return false;
    }

    /* Recycle queue is empty, now lock and check free queue */
    PageId freeQueueHeadPageId = GetQueueHead(BtrRecycleQueueType::FREE);
    BtreeRecycleQueue freeQueue(m_pdbId, freeQueueHeadPageId, BtrRecycleQueueType::FREE, bufMgr);
    return freeQueue.IsEmpty<ReusablePageQueue>();
}

PageId BtrRecyclePartitionMeta::GetQueueHead(BtrRecycleQueueType type) const
{
    BtrRecyclePartitionMetaPage *recyclePartitionMetaPage =
        static_cast<BtrRecyclePartitionMetaPage *>(recyclePartitionMetaBuf->GetPage());
    if (STORAGE_VAR_NULL(recyclePartitionMetaPage) ||
        unlikely(!recyclePartitionMetaPage->TestType(PageType::BTR_RECYCLE_PARTITION_META_PAGE_TYPE))) {
        return INVALID_PAGE_ID;
    }
    switch (type) {
        case BtrRecycleQueueType::RECYCLE:
            return recyclePartitionMetaPage->GetRecycleQueueHead();
        case BtrRecycleQueueType::FREE:
            return recyclePartitionMetaPage->GetFreeQueueHead();
        default:
            break;
    }
    return INVALID_PAGE_ID;
}

void BtrRecyclePartitionMeta::SetQueueHead(BtrRecycleQueueType type, const PageId head)
{
    BtrRecyclePartitionMetaPage *recyclePartitionMetaPage =
    static_cast<BtrRecyclePartitionMetaPage *>(recyclePartitionMetaBuf->GetPage());
    if (type == BtrRecycleQueueType::RECYCLE) {
        recyclePartitionMetaPage->SetRecycleQueueHead(head);
    } else {
        recyclePartitionMetaPage->SetFreeQueueHead(head);
    }
    (void)GetBufMgr()->MarkDirty(recyclePartitionMetaBuf);
}

BufMgrInterface *BtrRecyclePartitionMeta::GetBufMgr()
{
    if (bufMgr == nullptr) {
        bufMgr = (isGlobalTempIndex ? thrd->GetTmpLocalBufMgr() : g_storageInstance->GetBufferMgr());
    }
    return bufMgr;
}

void BtrRecyclePartitionMeta::GenerateWalForRecyclePartitionMetaInit()
{
    AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
    walWriterContext->RememberPageNeedWal(recyclePartitionMetaBuf);
    BtrRecyclePartitionMetaPage *metaPage =
        static_cast<BtrRecyclePartitionMetaPage*>(recyclePartitionMetaBuf->GetPage());
    uint16 redoDataSize = sizeof(WalRecordBtrRecyclePartitionMetaInit);
    WalRecordBtrRecyclePartitionMetaInit redoData;

    WalPageHeaderContext header{.type = WAL_BTREE_RECYCLE_PARTITION_META_INIT,
                                .size = redoDataSize,
                                .pageId = recyclePartitionMetaBuf->GetPageId(),
                                .preWalId = metaPage->GetWalId(),
                                .prePlsn = metaPage->GetPlsn(),
                                .preGlsn = metaPage->GetGlsn(),
                                .glsnChangedFlag = metaPage->GetWalId() != walWriterContext->GetWalId(),
                                .preVersion = recyclePartitionMetaBuf->GetFileVersion()};
    redoData.SetHeader(header, metaPage->createdXid);
    walWriterContext->PutNewWalRecord(&redoData);
}

char *BtrRecyclePartitionMetaPage::Dump()
{
    StringInfoData str;
    str.init();

    str.append("Page Header: \n");
    Page::DumpHeader(&str);
    str.append("\n");
    str.append("Last access Timestamp: %ld\n", accessTimestamp);
    str.append("Created Xid: (%d, %lu)\n", static_cast<int32>(createdXid.m_zoneId), createdXid.m_logicSlotId);

    PageId recycleQueue = GetRecycleQueueHead();
    str.append("Recycle Queue Head: [page id] (%hu, %u)\n", recycleQueue.m_fileId, recycleQueue.m_blockId);
    PageId freeQueue = GetFreeQueueHead();
    str.append("Free Queue Head: [page id] (%hu, %u)\n", freeQueue.m_fileId, freeQueue.m_blockId);

    return str.data;
}

}
