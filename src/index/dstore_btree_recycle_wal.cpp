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
 * dstore_btree_recycle_wal.cpp
 *
 * IDENTIFICATION
 *        dstore/src/index/dstore_btree_recycle_wal.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include <functional>
#include "common/log/dstore_log.h"
#include "page/dstore_btr_recycle_partition_meta_page.h"
#include "index/dstore_btree_recycle_wal.h"

namespace DSTORE {

struct BtrRecycleWalRedoItem {
    WalType type;
    std::function<void(const WalRecordBtrRecycle *, Page *)> redo;

    BtrRecycleWalRedoItem(WalType walType, std::function<void(const WalRecordBtrRecycle *, Page *)> redoFunc) noexcept
        : type(walType), redo(std::move(redoFunc)) {}
};

static const BtrRecycleWalRedoItem BTR_RECYCLE_WAL_REDO_TABLE[] { /* NOLINT */
    {WAL_BTREE_RECYCLE_PARTITION_INIT_PAGE, [](const WalRecordBtrRecycle *self, Page *page)
        { (static_cast<const WalRecordBtrRecyclePartitionInitPage *>(self))->Redo(page); }},
    {WAL_BTREE_RECYCLE_PARTITION_PUSH, [](const WalRecordBtrRecycle *self, Page *page)
        { (static_cast<const WalRecordBtrRecyclePartitionPush *>(self))->Redo(page); }},
    {WAL_BTREE_RECYCLE_PARTITION_BATCH_PUSH, [](const WalRecordBtrRecycle *self, Page *page)
        { (static_cast<const WalRecordBtrRecyclePartitionBatchPush *>(self))->Redo(page); }},
    {WAL_BTREE_RECYCLE_PARTITION_POP, [](const WalRecordBtrRecycle *self, Page *page)
        { (static_cast<const WalRecordBtrRecyclePartitionPop *>(self))->Redo(page); }},
    {WAL_BTREE_RECYCLE_PARTITION_ALLOC_SLOT, [](const WalRecordBtrRecycle *self, Page *page)
        { (static_cast<const WalRecordBtrRecyclePartitionAllocSlot *>(self))->Redo(page); }},
    {WAL_BTREE_RECYCLE_PARTITION_WRITE_SLOT, [](const WalRecordBtrRecycle *self, Page *page)
        { (static_cast<const WalRecordBtrRecyclePartitionWriteSlot *>(self))->Redo(page); }},
    {WAL_BTREE_RECYCLE_QUEUE_PAGE_META_SET_NEXT, [](const WalRecordBtrRecycle *self, Page *page)
        { (static_cast<const WalRecordBtrQueuePageMetaSetNext *>(self))->Redo(page); }},
    {WAL_BTREE_RECYCLE_PARTITION_META_INIT, [](const WalRecordBtrRecycle *self, Page *page)
        { (static_cast<const WalRecordBtrRecyclePartitionMetaInit *>(self))->Redo(page); }},
    {WAL_BTREE_RECYCLE_PARTITION_META_SET_HEAD, [](const WalRecordBtrRecycle *self, Page *page)
        { (static_cast<const WalRecordBtrRecyclePartitionMetaSetHead *>(self))->Redo(page); }},
    {WAL_BTREE_RECYCLE_PARTITION_META_TIMESTAMP_UPDATE, [](const WalRecordBtrRecycle *self, Page *page)
        { (static_cast<const WalRecordBtrRecyclePartitionMetaSetTimestamp *>(self))->Redo(page); }},
    {WAL_BTREE_RECYCLE_ROOT_META_INIT, [](const WalRecordBtrRecycle *self, Page *page)
        { (static_cast<const WalRecordBtrRecycleRootMetaInit *>(self))->Redo(page); }},
    {WAL_BTREE_RECYCLE_ROOT_META_SET_PARTITION_META, [](const WalRecordBtrRecycle *self, Page *page)
        { (static_cast<const WalRecordBtrRecycleRootMetaSetPartitionMeta *>(self))->Redo(page); }}
};

struct BtrRecycleWalDescItem {
    WalType type;
    std::function<void(const WalRecordBtrRecycle *, FILE *)> desc;

    BtrRecycleWalDescItem(WalType walType, std::function<void(const WalRecordBtrRecycle *, FILE *)> descFunc)
        noexcept : type(walType), desc(std::move(descFunc)) {}
};

static const BtrRecycleWalDescItem BTR_RECYCLE_WAL_DESC_TABLE[] {  /* NOLINT */
    {WAL_BTREE_RECYCLE_PARTITION_INIT_PAGE,
     [](const WalRecordBtrRecycle *self, FILE *fp) {
         (static_cast<const WalRecordBtrRecyclePartitionInitPage *>(self))->Describe(fp); }},
    {WAL_BTREE_RECYCLE_PARTITION_PUSH,
     [](const WalRecordBtrRecycle *self, FILE *fp) {
         (static_cast<const WalRecordBtrRecyclePartitionPush *>(self))->Describe(fp); }},
    {WAL_BTREE_RECYCLE_PARTITION_BATCH_PUSH,
     [](const WalRecordBtrRecycle *self, FILE *fp) {
         (static_cast<const WalRecordBtrRecyclePartitionBatchPush *>(self))->Describe(fp); }},
    {WAL_BTREE_RECYCLE_PARTITION_POP,
     [](const WalRecordBtrRecycle *self, FILE *fp) {
         (static_cast<const WalRecordBtrRecyclePartitionPop *>(self))->Describe(fp); }},
    {WAL_BTREE_RECYCLE_PARTITION_ALLOC_SLOT,
     [](const WalRecordBtrRecycle *self, FILE *fp) {
         (static_cast<const WalRecordBtrRecyclePartitionAllocSlot *>(self))->Describe(fp); }},
    {WAL_BTREE_RECYCLE_PARTITION_WRITE_SLOT,
     [](const WalRecordBtrRecycle *self, FILE *fp) {
         (static_cast<const WalRecordBtrRecyclePartitionWriteSlot *>(self))->Describe(fp); }},
    {WAL_BTREE_RECYCLE_QUEUE_PAGE_META_SET_NEXT,
     [](const WalRecordBtrRecycle *self, FILE *fp) {
         (static_cast<const WalRecordBtrQueuePageMetaSetNext *>(self))->Describe(fp); }},
    {WAL_BTREE_RECYCLE_PARTITION_META_INIT,
     [](const WalRecordBtrRecycle *self, FILE *fp) {
         (static_cast<const WalRecordBtrRecyclePartitionMetaInit *>(self))->Describe(fp); }},
    {WAL_BTREE_RECYCLE_PARTITION_META_SET_HEAD,
     [](const WalRecordBtrRecycle *self, FILE *fp) {
         (static_cast<const WalRecordBtrRecyclePartitionMetaSetHead *>(self))->Describe(fp); }},
    {WAL_BTREE_RECYCLE_PARTITION_META_TIMESTAMP_UPDATE,
     [](const WalRecordBtrRecycle *self, FILE *fp) {
         (static_cast<const WalRecordBtrRecyclePartitionMetaSetTimestamp *>(self))->Describe(fp); }},
    {WAL_BTREE_RECYCLE_ROOT_META_INIT,
     [](const WalRecordBtrRecycle *self, FILE *fp) {
         (static_cast<const WalRecordBtrRecycleRootMetaInit *>(self))->Describe(fp); }},
    {WAL_BTREE_RECYCLE_ROOT_META_SET_PARTITION_META,
     [](const WalRecordBtrRecycle *self, FILE *fp) {
         (static_cast<const WalRecordBtrRecycleRootMetaSetPartitionMeta *>(self))->Describe(fp); }}
};

void WalRecordBtrRecycle::RedoBtrRecycleRecord(WalRecordRedoContext *redoCtx,
    const WalRecordBtrRecycle *btrRecycleRecord, BufferDesc *bufferDesc)
{
    StorageAssert(btrRecycleRecord != nullptr);

    PageId pageId = btrRecycleRecord->m_pageId;
    STORAGE_CHECK_BUFFER_PANIC(bufferDesc, MODULE_INDEX, pageId);
    Page *page = bufferDesc->GetPage();
    for (auto &redoItem: DSTORE::BTR_RECYCLE_WAL_REDO_TABLE) {
        if (btrRecycleRecord->m_type == redoItem.type) {
            redoItem.redo(btrRecycleRecord, page);
            break;
        }
    }
    /* Write back wal id, plsn, glsn */
    const uint64 glsn = btrRecycleRecord->m_pagePreWalId != redoCtx->walId ? btrRecycleRecord->m_pagePreGlsn + 1
                                                                           : btrRecycleRecord->m_pagePreGlsn;
    page->SetLsn(redoCtx->walId, redoCtx->recordEndPlsn, glsn);
}

void WalRecordBtrRecycle::DumpBtrRecycleRecord(const WalRecordBtrRecycle *btrRecycleRecord, FILE *fp)
{
    StorageAssert(btrRecycleRecord != nullptr);
    btrRecycleRecord->Dump(fp);
    PageId pageId = btrRecycleRecord->m_pageId;
    (void)fprintf(fp, "Btree Recycle Wal Record for m_pageId=(%hu, %u) ", pageId.m_fileId, pageId.m_blockId);
    for (uint32 i = 0; i < sizeof(BTR_RECYCLE_WAL_DESC_TABLE) / sizeof(BTR_RECYCLE_WAL_DESC_TABLE[0]); i++) {
        if (BTR_RECYCLE_WAL_DESC_TABLE[i].type == btrRecycleRecord->m_type) {
            BTR_RECYCLE_WAL_DESC_TABLE[i].desc(btrRecycleRecord, fp);
            return;
        }
    }
}

void WalRecordBtrRecyclePartitionInitPage::Redo(Page *page) const
{
    BtrQueuePage *queuePage = static_cast<BtrQueuePage *>(page);
    if (queueType == BtrRecycleQueueType::RECYCLE) {
        queuePage->Reset<RecyclablePageQueue>(m_pageId, btrCreatedXid);
    } else {
        queuePage->Reset<ReusablePageQueue>(m_pageId, btrCreatedXid);
    }
}

void WalRecordBtrRecyclePartitionInitPage::Describe(FILE *fp) const
{
    WalRecordForPage::Dump(fp);
    (void)fprintf(fp, " BtreeCreateXid:(%d, %lu)", static_cast<int32>(btrCreatedXid.m_zoneId),
                  btrCreatedXid.m_logicSlotId);
}

void WalRecordBtrRecyclePartitionPush::Redo(Page *page) const
{
    int16 pageTail = 0;
    BtrQueuePage *queuePage = static_cast<BtrQueuePage *>(page);
    if (queueType == BtrRecycleQueueType::RECYCLE) {
        if (needResetPage) {
            queuePage->Reset<RecyclablePageQueue>(m_pageId, btrCreatedXid);
        }
        RecyclablePageQueue *queue = queuePage->GetQueue<RecyclablePageQueue>();
        const RecyclablePage recyclablePage {
            .csn = csn,
            .pageId = pageId
        };
        pageTail = queue->GetTail();
        queue->Push(recyclablePage);
    } else {
        if (needResetPage) {
            queuePage->Reset<ReusablePageQueue>(m_pageId, btrCreatedXid);
        }
        ReusablePageQueue *queue = queuePage->GetQueue<ReusablePageQueue>();
        const ReusablePage reusablePage{.csn = csn, .pageId = pageId};
        pageTail = queue->GetTail();
        queue->Push(reusablePage);
    }
    if (unlikely(pageTail != tail)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
            ErrMsg("tail check failed when WalRecordBtrRecyclePartitionPush::Redo, queuePage(%hu, %u), "
            "pagePlsn(%lu), pageGlsn(%lu), createXid(%d, %lu), pageTail(%hd), walTail(%hu), pushPage(%hu, %u, %lu)",
            m_pageId.m_fileId, m_pageId.m_blockId, page->GetPlsn(), page->GetGlsn(),
            static_cast<int>(btrCreatedXid.m_zoneId), btrCreatedXid.m_logicSlotId, pageTail, tail,
            pageId.m_fileId, pageId.m_blockId, csn));
    }
}

void WalRecordBtrRecyclePartitionPush::Describe(FILE *fp) const
{
    (void)fprintf(fp, "Type WAL_BTREE_RECYCLE_PARTITION_PUSH: ");
    (void)fprintf(fp, "BtreeCreateXid:(%d, %lu) ", static_cast<int32>(btrCreatedXid.m_zoneId),
                  btrCreatedXid.m_logicSlotId);
    if (queueType == BtrRecycleQueueType::RECYCLE) {
        const RecyclablePage recyclablePage {
            .csn = csn,
            .pageId = pageId
        };
        (void)fprintf(fp, "BtrRecycleQueueType RECYCLE: ");
        (void)fprintf(fp, "Push pageId=(%hu, %u) to position [%d]", recyclablePage.pageId.m_fileId,
                      recyclablePage.pageId.m_blockId, tail);
    } else {
        const ReusablePage reusablePage {
            .csn = csn,
            .pageId = pageId
        };
        (void)fprintf(fp, "BtrRecycleQueueType FREE: ");
        (void)fprintf(fp, "Push pageId=(%hu, %u) to position [%d]", reusablePage.pageId.m_fileId,
                      reusablePage.pageId.m_blockId, tail);
    }
}

void WalRecordBtrRecyclePartitionBatchPush::Redo(Page *page) const
{
    BtrQueuePage *queuePage = static_cast<BtrQueuePage *>(page);
    bool needReset = false;
    uint16 pageIdSize = numPages * sizeof(PageId);
    uint16 dataSize = m_size - sizeof(WalRecordBtrRecyclePartitionBatchPush);
    if (dataSize > pageIdSize) {
        needReset = *(static_cast<const bool *>(static_cast<const void *>(rawData + pageIdSize)));
    }
    if (needReset || queuePage->GetType() != PageType::BTR_QUEUE_PAGE_TYPE || queuePage->GetSelfPageId() != m_pageId) {
        queuePage->Reset<ReusablePageQueue>(m_pageId, btrCreatedXid);
    }
    StorageAssert(queueType != BtrRecycleQueueType::RECYCLE);
    ReusablePageQueue *queue = queuePage->GetQueue<ReusablePageQueue>();
    uint16 i = 0;
    while (!queue->IsFull() && i < numPages) {
        uint16 offset = static_cast<uint16>(sizeof(PageId) * i);
        const PageId *newPage = static_cast<const PageId *>(static_cast<const void *>(rawData + offset));
        ReusablePage reusablePage = {INVALID_CSN, *newPage};
        queue->Push(reusablePage);
        i++;
    }

    if (i < numPages) {
        uint16 offset = static_cast<uint16>(sizeof(PageId) * i);
        const PageId *newPage = static_cast<const PageId *>(static_cast<const void *>(rawData + offset));
        queuePage->SetNext(*newPage);
        i++;
    }
    if (unlikely(i != numPages)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
            ErrMsg("numPages check failed when WalRecordBtrRecyclePartitionBatchPush::Redo, queuePage(%hu, %u), "
            "pagePlsn(%lu), pageGlsn(%lu), createXid(%d, %lu), numPagesI(%hd), numPagesWal(%hu)",
            m_pageId.m_fileId, m_pageId.m_blockId, page->GetPlsn(), page->GetGlsn(),
            static_cast<int>(btrCreatedXid.m_zoneId), btrCreatedXid.m_logicSlotId, i, numPages));
    }
}

void WalRecordBtrRecyclePartitionBatchPush::Describe(FILE *fp) const
{
    (void)fprintf(fp, "Type WAL_BTREE_RECYCLE_PARTITION_BATCH_PUSH:");
    (void)fprintf(fp, "number of pages: %hu ", numPages);
    for (uint16 i = 0; i < numPages; i++) {
        uint16 offset = static_cast<uint16>(sizeof(PageId) * i);
        const PageId *newPage = static_cast<const PageId *>(static_cast<const void *>(rawData + offset));
        (void)fprintf(fp, "Push pageId=(%hu, %u) to position [%hu] ", newPage->m_fileId, newPage->m_blockId,
                      static_cast<uint16>(tail + i));
    }
    (void)fprintf(fp, "btrCreatedXid: (%d, %lu) ", static_cast<int32>(btrCreatedXid.m_zoneId),
                  btrCreatedXid.m_logicSlotId);
    uint16 pageIdSize = numPages * sizeof(PageId);
    uint16 dataSize = m_size - sizeof(WalRecordBtrRecyclePartitionBatchPush);
    bool needReset = false;
    if (dataSize > pageIdSize) {
        needReset = *(static_cast<const bool *>(static_cast<const void *>(rawData + pageIdSize)));
    }
    (void)fprintf(fp, "needReset: (%u) ", needReset);
}

void WalRecordBtrRecyclePartitionPop::Redo(Page *page) const
{
    BtrQueuePage *queuePage = static_cast<BtrQueuePage *>(page);
    if (queueType == BtrRecycleQueueType::RECYCLE) {
        RecyclablePageQueue *queue = queuePage->GetQueue<RecyclablePageQueue>();
        UNUSED_VARIABLE(queue->Pop(head));
    } else {
        ReusablePageQueue *queue = queuePage->GetQueue<ReusablePageQueue>();
        UNUSED_VARIABLE(queue->Pop(head));
    }
}

void WalRecordBtrRecyclePartitionPop::Describe(FILE *fp) const
{
    (void)fprintf(fp, "Type WAL_BTREE_RECYCLE_PARTITION_POP: head(%hd), type(%d)", head, static_cast<int>(queueType));
}

void WalRecordBtrRecyclePartitionAllocSlot::Redo(Page *page) const
{
    BtrQueuePage *queuePage = static_cast<BtrQueuePage *>(page);
    if (needResetPage) {
        queuePage->Reset<ReusablePageQueue>(m_pageId, btrCreatedXid);
    }
    StorageAssert(queueType != BtrRecycleQueueType::RECYCLE);
    ReusablePageQueue *queue = queuePage->GetQueue<ReusablePageQueue>();
    int16 pageTail = queue->GetTail();
    if (unlikely(pageTail != tail)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
            ErrMsg("tail check failed when WalRecordBtrRecyclePartitionAllocSlot::Redo, queuePage(%hu, %u), "
            "pagePlsn(%lu), pageGlsn(%lu), createXid(%d, %lu), pageTail(%hd), walTail(%hu), AllocForPage(%hu, %u, %lu)",
            m_pageId.m_fileId, m_pageId.m_blockId, page->GetPlsn(), page->GetGlsn(),
            static_cast<int>(btrCreatedXid.m_zoneId), btrCreatedXid.m_logicSlotId, pageTail, tail,
            reusablePage.pageId.m_fileId, reusablePage.pageId.m_blockId, reusablePage.csn));
    }
    UNUSED_VARIABLE(queue->AllocSlot(reusablePage));
}

void WalRecordBtrRecyclePartitionAllocSlot::Describe(FILE *fp) const
{
    (void)fprintf(fp, "Type WAL_BTREE_RECYCLE_PARTITION_ALLOC_SLOT: ");
    (void)fprintf(fp, "BtreeCreateXid:(%d, %lu) ", static_cast<int32>(btrCreatedXid.m_zoneId),
                  btrCreatedXid.m_logicSlotId);
    (void)fprintf(fp, "Allocate slot for pageId=(%hu, %u) to position [%d] with recycleCsn %lu",
                  reusablePage.pageId.m_fileId, reusablePage.pageId.m_blockId, tail, reusablePage.csn);
}

void WalRecordBtrRecyclePartitionWriteSlot::Redo(Page *page) const
{
    BtrQueuePage *queuePage = static_cast<BtrQueuePage *>(page);
    StorageAssert(queueType != BtrRecycleQueueType::RECYCLE);
    ReusablePageQueue *queue = queuePage->GetQueue<ReusablePageQueue>();
    int16 writePos = queue->WriteSlot(pos, reusablePage);
    if (unlikely(writePos != pos)) {
        ErrLog(writePos, MODULE_INDEX,
            ErrMsg("tail check failed when WalRecordBtrRecyclePartitionWriteSlot::Redo, queuePage(%hu, %u), "
            "pagePlsn(%lu), pageGlsn(%lu), writePos(%hd), walPos(%hu), writePage(%hu, %u, %lu)",
            m_pageId.m_fileId, m_pageId.m_blockId, page->GetPlsn(), page->GetGlsn(), writePos, pos,
            reusablePage.pageId.m_fileId, reusablePage.pageId.m_blockId, reusablePage.csn));
    }
}

void WalRecordBtrRecyclePartitionWriteSlot::Describe(FILE *fp) const
{
    (void)fprintf(fp, "Type WAL_BTREE_RECYCLE_PARTITION_WRITE_SLOT: ");
    (void)fprintf(fp, "Write slot for pageId=(%hu, %u) to position [%d] with recycleCsn %lu",
                  reusablePage.pageId.m_fileId, reusablePage.pageId.m_blockId, pos, reusablePage.csn);
}

void WalRecordBtrQueuePageMetaSetNext::Redo(Page *page) const
{
    BtrQueuePage *queuePage = static_cast<BtrQueuePage *>(page);
    queuePage->SetNext(nextPage);
}

void WalRecordBtrQueuePageMetaSetNext::Describe(FILE *fp) const
{
    (void)fprintf(fp, "Type WAL_BTREE_RECYCLE_QUEUE_PAGE_META_SET_NEXT: ");
    (void)fprintf(fp, "Set next pageId=(%hu, %u)", nextPage.m_fileId, nextPage.m_blockId);
}

void WalRecordBtrRecyclePartitionMetaInit::Redo(Page *page) const
{
    BtrRecyclePartitionMetaPage *recyclePartitionMetaPage = static_cast<BtrRecyclePartitionMetaPage *>(page);
    recyclePartitionMetaPage->InitRecyclePartitionMetaPage(m_pageId, createdXid);
}

void WalRecordBtrRecyclePartitionMetaInit::Describe(FILE *fp) const
{
    (void)fprintf(fp, "Type WAL_BTREE_RECYCLE_PARTITION_META_INIT: ");
    WalRecordForPage::Dump(fp);
}

void WalRecordBtrRecyclePartitionMetaSetHead::Redo(Page *page) const
{
    BtrRecyclePartitionMetaPage *recyclePartitionMetaPage = static_cast<BtrRecyclePartitionMetaPage *>(page);
    switch (queueType) {
        case BtrRecycleQueueType::RECYCLE:
            recyclePartitionMetaPage->SetRecycleQueueHead(queueHead);
            break;
        case BtrRecycleQueueType::FREE:
            recyclePartitionMetaPage->SetFreeQueueHead(queueHead);
            break;
        default:
            ErrLog(DSTORE_PANIC, MODULE_INDEX,
                   ErrMsg("Redo BtrRecyclePartitionMetaSetHead Failed, BtrRecycleQueueType is not valid."));
            return;
    }
}

void WalRecordBtrRecyclePartitionMetaSetHead::Describe(FILE *fp) const
{
    if (queueType == BtrRecycleQueueType::RECYCLE) {
        (void)fprintf(fp, "BtrRecycleQueueType RECYCLE: ");
    } else {
        (void)fprintf(fp, "BtrRecycleQueueType FREE: ");
    }
    (void)fprintf(fp, "Set QueueHead pageId=(%hu, %u)", queueHead.m_fileId, queueHead.m_blockId);
}

void WalRecordBtrRecyclePartitionMetaSetTimestamp::Redo(Page *page) const
{
    BtrRecyclePartitionMetaPage *recyclePartitionMetaPage = static_cast<BtrRecyclePartitionMetaPage *>(page);
    recyclePartitionMetaPage->accessTimestamp = timestamp;
}

void WalRecordBtrRecyclePartitionMetaSetTimestamp::Describe(FILE *fp) const
{
    (void)fprintf(fp, "timestamp(%ld)", timestamp);
}

void WalRecordBtrRecycleRootMetaInit::Redo(Page *page) const
{
    BtrRecycleRootMetaPage *recycleRootMetaPage = static_cast<BtrRecycleRootMetaPage *>(page);
    recycleRootMetaPage->InitRecycleRootMetaPage(m_pageId, btrCreatedXid);
}

void WalRecordBtrRecycleRootMetaInit::Describe(FILE *fp) const
{
    (void)fprintf(fp, "BtreeCreateXid:(%d, %lu)", static_cast<int32>(btrCreatedXid.m_zoneId),
                  btrCreatedXid.m_logicSlotId);
}

void WalRecordBtrRecycleRootMetaSetPartitionMeta::Redo(Page *page) const
{
    BtrRecycleRootMetaPage *recycleRootMetaPage = static_cast<BtrRecycleRootMetaPage *>(page);
    recycleRootMetaPage->SetRecyclePartitionMeta(recyclePartitionId, recyclePartitionMeta);
}

void WalRecordBtrRecycleRootMetaSetPartitionMeta::Describe(FILE *fp) const
{
    (void)fprintf(fp, "Type WAL_BTREE_RECYCLE_ROOT_META_SET_PARTITION_META: ");
    (void)fprintf(fp, "Set recyclePartitionMeta pageId=(%hu, %u) at position [%hu]", recyclePartitionMeta.m_fileId,
                  recyclePartitionMeta.m_blockId, recyclePartitionId);
}
}