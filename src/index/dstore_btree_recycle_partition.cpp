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
 * dstore_btree_recycle_partition.cpp
 *
 * IDENTIFICATION
 *        storage/src/index/dstore_btree_recycle_partition.cpp
 *
 * ---------------------------------------------------------------------------------------*/

#include "common/log/dstore_log.h"
#include "index/dstore_btree_recycle_wal.h"
#include "index/dstore_btree_recycle_queue.h"
#include "transaction/dstore_csn_mgr.h"
#include "wal/dstore_wal_write_context.h"
#include "index/dstore_btree_perf_unit.h"
#include "page/dstore_index_page.h"
#include "tablespace/dstore_tablespace_wal.h"
#include "index/dstore_btree_recycle_partition.h"

namespace DSTORE {

BufMgrInterface *BtreeRecyclePartition::GetBufMgr()
{
    StorageAssert(bufMgr != nullptr);
    return bufMgr;
}

BtrQueuePage *BtreeRecyclePartition::GetBtrQueuePage(BufferDesc *buf)
{
    StorageAssert(buf != INVALID_BUFFER_DESC);
    return static_cast<BtrQueuePage *>(buf->GetPage());
}

PageId BtreeRecyclePartition::GetNewPage()
{
    return segment->GetNewPageFromUnassignedPages();
}

RetStatus BtreeRecyclePartition::AcquireRecyclePartitionMetaBuf(LWLockMode access)
{
    recyclePartitionMetaBuf = GetBufMgr()->Read(this->GetPdbId(), recyclePartitionMeta, access);
    if (unlikely(recyclePartitionMetaBuf == INVALID_BUFFER_DESC)) {
        ErrLog(DSTORE_WARNING, MODULE_INDEX, ErrMsg("Faild to read page (%hu, %u).",
            recyclePartitionMeta.m_fileId, recyclePartitionMeta.m_blockId));
        return DSTORE_FAIL;
    }
    if (likely(!needCheckCreatedXid)) {
        return DSTORE_SUCC;
    }

    BtrRecyclePartitionMetaPage *page = static_cast<BtrRecyclePartitionMetaPage *>(recyclePartitionMetaBuf->GetPage());
    if (unlikely(!page->TestType(PageType::BTR_RECYCLE_PARTITION_META_PAGE_TYPE)) ||
        unlikely(page->createdXid != createdXid)) {
        ReleaseRecyclePartitionMetaBuf();
        ErrLog(DSTORE_WARNING, MODULE_INDEX, ErrMsg("page (%hu, %u) is no longer recyclePartitionMeta",
            recyclePartitionMeta.m_fileId, recyclePartitionMeta.m_blockId));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

void BtreeRecyclePartition::ReleaseRecyclePartitionMetaBuf(bool unlock)
{
    if (likely(unlock)) {
        GetBufMgr()->UnlockAndRelease(recyclePartitionMetaBuf);
    } else {
        GetBufMgr()->Release(recyclePartitionMetaBuf);
    }
    recyclePartitionMetaBuf = INVALID_BUFFER_DESC;
}

BtrRecyclePartitionMeta BtreeRecyclePartition::GetRecyclePartitionMeta()
{
    StorageAssert(recyclePartitionMetaBuf != INVALID_BUFFER_DESC);
    return BtrRecyclePartitionMeta(
        this->GetPdbId(), recyclePartitionMeta, recyclePartitionMetaBuf, segment->IsTempSegment());
}

RetStatus BtreeRecyclePartition::UpdateTimestampIfNecessary(LWLockMode access)
{
    BtrRecyclePartitionMetaPage *metaPage = static_cast<BtrRecyclePartitionMetaPage*>
                                            (recyclePartitionMetaBuf->GetPage());
    if (!TimestampNeedUpdate()) {
        return DSTORE_SUCC;
    }
    if (access == LWLockMode::LW_SHARED) {
        /* Switch to a exclusive lock for updating accessTimestamp and switch back to share lock */
        bufMgr->UnlockContent(recyclePartitionMetaBuf);
        if (STORAGE_FUNC_FAIL(bufMgr->LockContent(recyclePartitionMetaBuf, LW_EXCLUSIVE))) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Lock content {%d, %u} failed when execute function %s.",
                recyclePartitionMetaBuf->GetPageId().m_fileId, recyclePartitionMetaBuf->GetPageId().m_blockId,
                __FUNCTION__));
            return DSTORE_FAIL;
        }
        if (TimestampNeedUpdate()) {
            metaPage = static_cast<BtrRecyclePartitionMetaPage*>(recyclePartitionMetaBuf->GetPage());
            metaPage->accessTimestamp = GetCurrentTimestampInSecond();
            bufMgr->MarkDirty(recyclePartitionMetaBuf);
            GenerateWalForTimestampUpdate();
        }
        bufMgr->UnlockContent(recyclePartitionMetaBuf);
        if (STORAGE_FUNC_FAIL(bufMgr->LockContent(recyclePartitionMetaBuf, LW_SHARED))) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Lock content {%d, %u} shared failed when execute function %s.",
                recyclePartitionMetaBuf->GetPageId().m_fileId, recyclePartitionMetaBuf->GetPageId().m_blockId,
                __FUNCTION__));
            return DSTORE_FAIL;
        }
    } else if (access == LWLockMode::LW_EXCLUSIVE) {
        metaPage->accessTimestamp = GetCurrentTimestampInSecond();
        bufMgr->MarkDirty(recyclePartitionMetaBuf);
        GenerateWalForTimestampUpdate();
    }
    return DSTORE_SUCC;
}

BtreeRecycleQueue BtreeRecyclePartition::GetBtreeRecycleQueue(BtrRecycleQueueType type)
{
    BtrRecyclePartitionMeta partitionMeta = GetRecyclePartitionMeta();
    PageId queueHead = partitionMeta.GetQueueHead(type);
    return BtreeRecycleQueue(this->GetPdbId(), queueHead, type, GetBufMgr());
}

bool BtreeRecyclePartition::IsValid(bool &locksucc)
{
    BtrRecyclePartitionMeta partitionMeta = GetRecyclePartitionMeta();

    PageId recycleHead = partitionMeta.GetQueueHead(BtrRecycleQueueType::RECYCLE);
    if (!recycleHead.IsValid()) {
        return false;
    }

    PageId freeHead = partitionMeta.GetQueueHead(BtrRecycleQueueType::FREE);
    if (!freeHead.IsValid()) {
        return false;
    }
    if (STORAGE_FUNC_FAIL(UpdateTimestampIfNecessary(LW_SHARED))) {
        locksucc = false;
        return false;
    }

    return true;
}

RetStatus BtreeRecyclePartition::Init()
{
    if (STORAGE_FUNC_FAIL(AcquireRecyclePartitionMetaBuf(LW_EXCLUSIVE))) {
        return DSTORE_FAIL;
    }
    BtrRecyclePartitionMeta partitionMeta = GetRecyclePartitionMeta();

    PageId recycleHead = partitionMeta.GetQueueHead(BtrRecycleQueueType::RECYCLE);
    if (!recycleHead.IsValid()) {
        if (STORAGE_FUNC_FAIL(InitNewQueueHead<RecyclablePageQueue>(BtrRecycleQueueType::RECYCLE))) {
            ErrLog(DSTORE_LOG, MODULE_INDEX,
                ErrMsg("Failed to init new queue head for Btree recycleHead."));
            ReleaseRecyclePartitionMetaBuf();
            return DSTORE_FAIL;
        }
    }

    PageId freeHead = partitionMeta.GetQueueHead(BtrRecycleQueueType::FREE);
    if (!freeHead.IsValid()) {
        if (STORAGE_FUNC_FAIL(InitNewQueueHead<ReusablePageQueue>(BtrRecycleQueueType::FREE))) {
            ErrLog(DSTORE_LOG, MODULE_INDEX,
                ErrMsg("Failed to init new queue head for Btree freeHead."));
            ReleaseRecyclePartitionMetaBuf();
            return DSTORE_FAIL;
        }
    }
    if (STORAGE_FUNC_FAIL(UpdateTimestampIfNecessary(LW_EXCLUSIVE))) {
        ReleaseRecyclePartitionMetaBuf(false);
        return DSTORE_FAIL;
    }
    ReleaseRecyclePartitionMetaBuf();

    return DSTORE_SUCC;
}

RetStatus BtreeRecyclePartition::RecycleListPush(const RecyclablePage recyclablePage)
{
    const BtrRecycleQueueType type = BtrRecycleQueueType::RECYCLE;
    BufferDesc *qBuf = INVALID_BUFFER_DESC;
    PageId newPage = INVALID_PAGE_ID;

    bool needUpdateTimestamp = true;
    Xid checkingXid = unlikely(needCheckCreatedXid) ? createdXid : INVALID_XID;
    while (qBuf == INVALID_BUFFER_DESC) {
        if (STORAGE_FUNC_FAIL(AcquireRecyclePartitionMetaBuf(LW_SHARED))) {
            return DSTORE_FAIL;
        }
        needUpdateTimestamp = TimestampNeedUpdate();
        BtreeRecycleQueue recycleList = GetBtreeRecycleQueue(type);
        /* Check the last available queue. */
        qBuf = recycleList.GetQueueTailBuf(LW_EXCLUSIVE, checkingXid);
        ReleaseRecyclePartitionMetaBuf();
        if (unlikely(qBuf == INVALID_BUFFER_DESC)) {
            return DSTORE_FAIL;
        }

        RecyclablePageQueue *queue = GetBtrQueuePage(qBuf)->GetQueue<RecyclablePageQueue>();
        if (!queue->IsFull() || newPage.IsValid()) {
            break;
        }

        GetBufMgr()->UnlockAndRelease(qBuf);
        qBuf = INVALID_BUFFER_DESC;
        newPage = AllocNewPage();
        needUpdateTimestamp = false;
        if (!newPage.IsValid()) {
            ErrLog(DSTORE_LOG, MODULE_INDEX,
                   ErrMsg("Failed to get new page from Btree FreeQueue. "
                          "Tried to push page (%hu, %u) with CSN %lu to RecycleQueue.",
                          recyclablePage.pageId.m_fileId, recyclablePage.pageId.m_blockId,
                          recyclablePage.csn));
            return DSTORE_SUCC;
        }
    }

    RecyclablePageQueue *queue = GetBtrQueuePage(qBuf)->GetQueue<RecyclablePageQueue>();
    BufferDesc *oldTailBuf = INVALID_BUFFER_DESC;
    AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
    walWriterContext->BeginAtomicWal(thrd->GetCurrentXid());
    if (queue->IsFull()) {
        StorageAssert(newPage.IsValid());
        oldTailBuf = qBuf;
        AddQueueTail(type, qBuf, newPage);
        qBuf = GetBufMgr()->Read(this->GetPdbId(), newPage, LW_EXCLUSIVE);
        if (unlikely(qBuf == INVALID_BUFFER_DESC)) {
            GetBufMgr()->UnlockAndRelease(oldTailBuf);
            (void)walWriterContext->EndAtomicWal();
            return DSTORE_FAIL;
        }
        newPage = INVALID_PAGE_ID;
    }

    bool needInit = oldTailBuf != INVALID_BUFFER_DESC;
    StorageAssert(needInit || !queue->IsFull());
    /* Push the recyclablePage into the new queue. */
    if (STORAGE_FUNC_FAIL(RecyclablePageQueuePush(type, qBuf, recyclablePage, needInit))) {
        (void)walWriterContext->EndAtomicWal();
        GetBufMgr()->UnlockAndRelease(qBuf);

        if (oldTailBuf != INVALID_BUFFER_DESC) {
            GetBufMgr()->UnlockAndRelease(oldTailBuf);
        }
        return DSTORE_FAIL;
    }
    (void)walWriterContext->EndAtomicWal();
    GetBufMgr()->UnlockAndRelease(qBuf);

    if (oldTailBuf != INVALID_BUFFER_DESC) {
        GetBufMgr()->UnlockAndRelease(oldTailBuf);
    }

    /* Push any excess pages we've allocated to the FreeQueue. */
    if (newPage.IsValid()) {
        needUpdateTimestamp = false;
        if (STORAGE_FUNC_FAIL(FreeListPush(newPage))) {
            return DSTORE_FAIL;
        }
    }
    if (unlikely(needUpdateTimestamp)) {
        if (STORAGE_FUNC_FAIL(AcquireRecyclePartitionMetaBuf(LW_EXCLUSIVE))) {
            return DSTORE_FAIL;
        }
        if (STORAGE_FUNC_FAIL(UpdateTimestampIfNecessary(LW_EXCLUSIVE))) {
            ReleaseRecyclePartitionMetaBuf(false);
            return DSTORE_FAIL;
        }
        ReleaseRecyclePartitionMetaBuf();
    }
    return DSTORE_SUCC;
}

RetStatus BtreeRecyclePartition::RecycleListPop(PageId &recyclablePageId, CommitSeqNo minCsn, uint64 *numSkippedPage)
{
    const BtrRecycleQueueType type = BtrRecycleQueueType::RECYCLE;
    LWLockMode partitionMetaAccess = LW_SHARED;
    if (STORAGE_FUNC_FAIL(AcquireRecyclePartitionMetaBuf(partitionMetaAccess))) {
        return DSTORE_FAIL;
    }
    bool needUpdateTimestamp = TimestampNeedUpdate();
    bool retried = false;
    BufferDesc *head = nullptr;
    AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;

    while (!retried) {
        /* Pop from the head of the recycle list. */
        BtreeRecycleQueue recycleList = GetBtreeRecycleQueue(type);
        head = recycleList.GetQueueHeadBuf(LW_EXCLUSIVE);
        if (unlikely(head == INVALID_BUFFER_DESC)) {
            ReleaseRecyclePartitionMetaBuf();
            return DSTORE_FAIL;
        }
        if (unlikely(needCheckCreatedXid) &&
            unlikely(!BtrQueuePage::isQueuePageValid(head, createdXid))) {
            if (head != INVALID_BUFFER_DESC) {
                GetBufMgr()->UnlockAndRelease(head);
            }
            ReleaseRecyclePartitionMetaBuf();
            return DSTORE_FAIL;
        }
        BtrQueuePage *qPage = GetBtrQueuePage(head);
        StorageAssert(qPage != nullptr);
        RecyclablePageQueue *queue = qPage->GetQueue<RecyclablePageQueue>();
        if (qPage->IsTail() || queue->GetSize() > 1) {
            walWriterContext->BeginAtomicWal(thrd->GetCurrentXid());
            recyclablePageId = RecyclablePageQueuePop(type, head, minCsn, numSkippedPage);
            (void)walWriterContext->EndAtomicWal();
            GetBufMgr()->UnlockAndRelease(head);
            if (unlikely(needUpdateTimestamp)) {
                if (STORAGE_FUNC_FAIL(UpdateTimestampIfNecessary(partitionMetaAccess))) {
                    ReleaseRecyclePartitionMetaBuf(false);
                    return DSTORE_FAIL;
                }
            }
            ReleaseRecyclePartitionMetaBuf();
            return DSTORE_SUCC;
        }

        retried = partitionMetaAccess == LW_EXCLUSIVE;
        if (!retried) {
            /* The recycle list consists of more than one page and the head will
             * be empty after we pop from it.  Upgrade our lock to exclusive
             * lock and try to update the reference to the head page. */
            GetBufMgr()->UnlockAndRelease(head);
            partitionMetaAccess = LW_EXCLUSIVE;
            GetBufMgr()->UnlockContent(recyclePartitionMetaBuf);
            if (STORAGE_FUNC_FAIL(GetBufMgr()->LockContent(recyclePartitionMetaBuf, partitionMetaAccess))) {
                ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Lock content {%d, %u} failed when execute function %s.",
                    recyclePartitionMetaBuf->GetPageId().m_fileId, recyclePartitionMetaBuf->GetPageId().m_blockId,
                    __FUNCTION__));
                GetBufMgr()->Release(recyclePartitionMetaBuf);
                recyclePartitionMetaBuf = INVALID_BUFFER_DESC;
                return DSTORE_FAIL;
            }
            BtrRecyclePartitionMetaPage *partMetaPage =
                static_cast<BtrRecyclePartitionMetaPage *>(recyclePartitionMetaBuf->GetPage());
            if (unlikely(needCheckCreatedXid) &&
                (unlikely(!partMetaPage->TestType(PageType::BTR_RECYCLE_PARTITION_META_PAGE_TYPE) ||
                 unlikely(partMetaPage->createdXid != createdXid)))) {
                ReleaseRecyclePartitionMetaBuf();
                return DSTORE_FAIL;
            }
            if (STORAGE_FUNC_FAIL(UpdateTimestampIfNecessary(partitionMetaAccess))) {
                ReleaseRecyclePartitionMetaBuf(false);
                return DSTORE_FAIL;
            }
            needUpdateTimestamp = false;
        }
    }

    walWriterContext->BeginAtomicWal(thrd->GetCurrentXid());
    recyclablePageId = RecyclablePageQueuePop(type, head, minCsn, numSkippedPage);
    RecyclablePageQueue *queue = GetBtrQueuePage(head)->GetQueue<RecyclablePageQueue>();
    if (!queue->IsEmpty(GetPdbId())) {
        (void)walWriterContext->EndAtomicWal();
        ReleaseRecyclePartitionMetaBuf();
        GetBufMgr()->UnlockAndRelease(head);
        return DSTORE_SUCC;
    }
    /* Update the head reference to the next page. */
    PageId oldHead = RemoveQueueHead(type, head);
    (void)walWriterContext->EndAtomicWal();
    ReleaseRecyclePartitionMetaBuf();
    GetBufMgr()->UnlockAndRelease(head);

    return FreeListPush(oldHead);
}

PageId BtreeRecyclePartition::AllocNewPage()
{
    LatencyStat::Timer timer(&BtreePerfUnit::GetInstance().m_btreeAllocNewPage);
    bool needRetry = true;
    PageId freePage = FreeListPop(needRetry);
    if (unlikely(!freePage.IsValid() && needRetry)) {
        freePage = segment->GetNewPage();
        ErrLog(DSTORE_DEBUG1, MODULE_INDEX,
            ErrMsg("FreeQueue does not contain any reusable page. "
                "Allocating some more pages from the index segment."));
    }

    return freePage;
}

RetStatus BtreeRecyclePartition::AllocUnusedSlot(FreeQueueSlot &slot, const PageId emptyPage)
{
    const BtrRecycleQueueType type = BtrRecycleQueueType::FREE;
    BufferDesc *qBuf = INVALID_BUFFER_DESC;
    PageId newPage = INVALID_PAGE_ID;

    bool needUpdateTimestamp = true;
    Xid checkingXid = unlikely(needCheckCreatedXid) ? createdXid : INVALID_XID;
    while (qBuf == INVALID_BUFFER_DESC) {
        if (STORAGE_FUNC_FAIL(AcquireRecyclePartitionMetaBuf(LW_SHARED))) {
            return DSTORE_FAIL;
        }
        needUpdateTimestamp = TimestampNeedUpdate();
        BtreeRecycleQueue freeList = GetBtreeRecycleQueue(type);
        /* Check the last available queue of the free list. */
        qBuf = freeList.GetQueueTailBuf(LW_EXCLUSIVE, checkingXid);
        ReleaseRecyclePartitionMetaBuf();
        if (unlikely(qBuf == INVALID_BUFFER_DESC)) {
            return DSTORE_FAIL;
        }

        ReusablePageQueue *queue = GetBtrQueuePage(qBuf)->GetQueue<ReusablePageQueue>();
        if (!queue->IsFull() || newPage.IsValid()) {
            break;
        }

        /* Get a new page from the DataSegment since the tail page is not valid.
         * Release the tail page buffer to avoid deadlock in GetNewPage */
        GetBufMgr()->UnlockAndRelease(qBuf);
        qBuf = INVALID_BUFFER_DESC;
        newPage = AllocNewPage();
        needUpdateTimestamp = false;
        if (!newPage.IsValid()) {
            ErrLog(DSTORE_DEBUG1, MODULE_INDEX, ErrMsg("Failed to get new page from the index segment."));
            slot = {INVALID_PAGE_ID, -1};
            return DSTORE_SUCC;
        }
    }

    ReusablePageQueue *queue = GetBtrQueuePage(qBuf)->GetQueue<ReusablePageQueue>();
    BufferDesc *oldTailBuf = INVALID_BUFFER_DESC;
    AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
    walWriterContext->BeginAtomicWal(thrd->GetCurrentXid());
    if (queue->IsFull()) {
        StorageAssert(newPage.IsValid());
        oldTailBuf = qBuf;
        AddQueueTail(type, oldTailBuf, newPage);
        qBuf = GetBufMgr()->Read(this->GetPdbId(), newPage, LW_EXCLUSIVE);
        if (unlikely(qBuf == INVALID_BUFFER_DESC)) {
            (void)walWriterContext->EndAtomicWal();
            GetBufMgr()->UnlockAndRelease(oldTailBuf);
            return DSTORE_FAIL;
        }
        newPage = INVALID_PAGE_ID;
    }

    /* By this point, qBuf should be the tail page of the FreeQueue with at
     * least one free slot. */
    bool needInit = oldTailBuf != INVALID_BUFFER_DESC;
    StorageAssert(needInit || !queue->IsFull());
    if (STORAGE_FUNC_FAIL(ReusablePageQueueAllocSlot(slot, qBuf, emptyPage, needInit))) {
        (void)walWriterContext->EndAtomicWal();
        if (oldTailBuf != INVALID_BUFFER_DESC) {
            GetBufMgr()->UnlockAndRelease(oldTailBuf);
        }
        GetBufMgr()->UnlockAndRelease(qBuf);
        return DSTORE_FAIL;
    }
    (void)walWriterContext->EndAtomicWal();

    /* Since we release all our locks to allocate a new page, the whole world
     * might have change under us.  Double check the status of the tail page to
     * determine what to do with the allocated page. */
    if (newPage.IsValid()) {
        /* If tail page is full after we allocated our slot, use the allocated
         * page as a tail page for the FreeQueue.  Otherwise, just push the
         * page into the tail page for future reuse. */
        walWriterContext->BeginAtomicWal(thrd->GetCurrentXid());
        if (queue->IsFull()) {
            oldTailBuf = qBuf;
            AddQueueTail(type, qBuf, newPage);
            qBuf = InitNewQueuePage<ReusablePageQueue>(type, newPage);
        } else {
            ReusablePage reusablePage = {INVALID_CSN, newPage};
            if (STORAGE_FUNC_FAIL(ReusablePageQueuePush(type, qBuf, reusablePage, false))) {
                (void)walWriterContext->EndAtomicWal();
                GetBufMgr()->UnlockAndRelease(qBuf);
                return DSTORE_FAIL;
            }
        }
        (void)walWriterContext->EndAtomicWal();
    }

    if (oldTailBuf != INVALID_BUFFER_DESC) {
        GetBufMgr()->UnlockAndRelease(oldTailBuf);
    }

    GetBufMgr()->UnlockAndRelease(qBuf);
    if (unlikely(needUpdateTimestamp)) {
        if (STORAGE_FUNC_FAIL(AcquireRecyclePartitionMetaBuf(LW_EXCLUSIVE))) {
            return DSTORE_FAIL;
        }
        if (STORAGE_FUNC_FAIL(UpdateTimestampIfNecessary(LW_EXCLUSIVE))) {
            ReleaseRecyclePartitionMetaBuf(false);
            return DSTORE_FAIL;
        }
        ReleaseRecyclePartitionMetaBuf();
    }

    return DSTORE_SUCC;
}

RetStatus BtreeRecyclePartition::WriteSlot(const FreeQueueSlot slot, const ReusablePage reusablePage)
{
    BufferDesc *qBuf = GetBufMgr()->Read(this->GetPdbId(), slot.qPage, LW_EXCLUSIVE);
    if (unlikely(qBuf == INVALID_BUFFER_DESC)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Faild to read page (%hu, %u) in function WriteSlot.",
            slot.qPage.m_fileId, slot.qPage.m_blockId));
        return DSTORE_FAIL;
    }
    if (unlikely(needCheckCreatedXid) && unlikely(!BtrQueuePage::isQueuePageValid(qBuf, createdXid))) {
        if (qBuf != INVALID_BUFFER_DESC) {
            GetBufMgr()->UnlockAndRelease(qBuf);
        }
        return DSTORE_FAIL;
    }
    ReusablePageQueue *queue = GetBtrQueuePage(qBuf)->GetQueue<ReusablePageQueue>();
    int16 pos = queue->WriteSlot(slot.pos, reusablePage);
    StorageAssert(pos != -1);
    (void)GetBufMgr()->MarkDirty(qBuf);
    AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
    walWriterContext->BeginAtomicWal(thrd->GetCurrentXid());
    GenerateWalForRecyclePartitionWriteSlot(qBuf, reusablePage, pos);
    (void)walWriterContext->EndAtomicWal();
    GetBufMgr()->UnlockAndRelease(qBuf);
    return DSTORE_SUCC;
}

void BtreeRecyclePartition::TryRegisterExtendTask()
{
    StoragePdb *curPdb = g_storageInstance->GetPdb(segment->GetPdbId());
    if (STORAGE_VAR_NULL(curPdb)) {
        storage_set_error(INDEX_ERROR_UNEXPECTED_NULL_VALUE);
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("[%s] Unexpected nullptr \"Pdb\".", __FUNCTION__));
        return;
    }
    if (ObjSpaceMgr::IsExtensionIndexTaskRegistered(segment->GetTablespaceId(), segment->GetSegmentMetaPageId(),
                                                    recyclePartitionMeta, createdXid, segment->GetPdbId())) {
        return;
    }
    ObjSpaceMgrExtendIndexTaskInfo extendTaskInfo(segment->GetPdbId(), segment->GetTablespaceId(),
                                                  segment->GetSegmentMetaPageId(), recyclePartitionMeta, createdXid);
    ObjSpaceMgr *objSpaceMgr = curPdb->GetObjSpaceMgr();

    if (STORAGE_FUNC_FAIL(objSpaceMgr->RegisterObjSpaceMgrTaskIfNeeded(&extendTaskInfo))) {
        /* Something wrong with memory allocation. */
        return;
    }
}

PageId BtreeRecyclePartition::FreeListPop(bool &needRetry)
{
    needRetry = true;
    bool needBgExtend = false;
    const BtrRecycleQueueType type = BtrRecycleQueueType::FREE;
    LWLockMode recycleMetaAccess = LW_SHARED;
    if (STORAGE_FUNC_FAIL(AcquireRecyclePartitionMetaBuf(recycleMetaAccess))) {
        needRetry = false;
        return INVALID_PAGE_ID;
    }
    bool needUpdateTimestamp = TimestampNeedUpdate();
    bool retried = false;
    BufferDesc *qBuf = nullptr;
    AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;

    while (!retried) {
        BtreeRecycleQueue freeList = GetBtreeRecycleQueue(type);
        qBuf = freeList.GetQueueHeadBuf(LW_EXCLUSIVE);
        if (unlikely(qBuf == INVALID_BUFFER_DESC)) {
            ReleaseRecyclePartitionMetaBuf();
            needRetry = false;
            return INVALID_PAGE_ID;
        }
        if (unlikely(needCheckCreatedXid) && unlikely(!BtrQueuePage::isQueuePageValid(qBuf, createdXid))) {
            if (qBuf != INVALID_BUFFER_DESC) {
                GetBufMgr()->UnlockAndRelease(qBuf);
            }
            ReleaseRecyclePartitionMetaBuf();
            needRetry = false;
            return INVALID_PAGE_ID;
        }
        BtrQueuePage *qPage = GetBtrQueuePage(qBuf);
        ReusablePageQueue *queue = qPage->GetQueue<ReusablePageQueue>();

        if (qPage->IsTail() || !queue->IsEmpty(pdbId)) {
            break;
        }

        /* Try to reuse the empty head page. */
        retried = recycleMetaAccess == LW_EXCLUSIVE;
        if (retried) {
            walWriterContext->BeginAtomicWal(thrd->GetCurrentXid());
            PageId oldHead = RemoveQueueHead(type, qBuf);
            (void)walWriterContext->EndAtomicWal();
            ReleaseRecyclePartitionMetaBuf();
            GetBufMgr()->UnlockAndRelease(qBuf);
            return oldHead;
        }
        GetBufMgr()->UnlockAndRelease(qBuf);
        recycleMetaAccess = LW_EXCLUSIVE;
        GetBufMgr()->UnlockContent(recyclePartitionMetaBuf);
        if (STORAGE_FUNC_FAIL(GetBufMgr()->LockContent(recyclePartitionMetaBuf, recycleMetaAccess))) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Lock content {%d, %u} failed when execute function %s.",
                recyclePartitionMetaBuf->GetPageId().m_fileId, recyclePartitionMetaBuf->GetPageId().m_blockId,
                __FUNCTION__));
            GetBufMgr()->Release(recyclePartitionMetaBuf);
            recyclePartitionMetaBuf = INVALID_BUFFER_DESC;
            return INVALID_PAGE_ID;
        }
        BtrRecyclePartitionMetaPage *partMetaPage =
            static_cast<BtrRecyclePartitionMetaPage *>(recyclePartitionMetaBuf->GetPage());
        if (unlikely(needCheckCreatedXid) &&
            (unlikely(!partMetaPage->TestType(PageType::BTR_RECYCLE_PARTITION_META_PAGE_TYPE) ||
             unlikely(partMetaPage->createdXid != createdXid)))) {
            ReleaseRecyclePartitionMetaBuf();
            return INVALID_PAGE_ID;
        }
        if (STORAGE_FUNC_FAIL(UpdateTimestampIfNecessary(recycleMetaAccess))) {
            ReleaseRecyclePartitionMetaBuf(false);
            return INVALID_PAGE_ID;
        }
        needUpdateTimestamp = false;
    }
    ReleaseRecyclePartitionMetaBuf();

    CommitSeqNo recycleMinCsn = g_storageInstance->GetCsnMgr()->GetRecycleCsnMin(pdbId);
    PageId freePage = INVALID_PAGE_ID;
    walWriterContext->BeginAtomicWal(thrd->GetCurrentXid());
    while (qBuf != INVALID_BUFFER_DESC) {
        freePage = ReusablePageQueuePop(type, qBuf, recycleMinCsn);
        BtrQueuePage *qPage = GetBtrQueuePage(qBuf);
        if (freePage.IsValid() || qPage->IsTail()) {
            break;
        }

        BufferDesc *nextBuf = GetBufMgr()->Read(pdbId, qPage->GetNext(), LW_EXCLUSIVE);
        if (unlikely(nextBuf == INVALID_BUFFER_DESC)) {
            ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Faild to read page (%hu, %u) in function FreeListPop.",
                qPage->GetNext().m_fileId, qPage->GetNext().m_blockId));
            GetBufMgr()->UnlockAndRelease(qBuf);
            (void)walWriterContext->EndAtomicWal();
            return INVALID_PAGE_ID;
        }
        if (unlikely(needCheckCreatedXid) && unlikely(!BtrQueuePage::isQueuePageValid(nextBuf, createdXid))) {
            if (nextBuf != INVALID_BUFFER_DESC) {
                GetBufMgr()->UnlockAndRelease(nextBuf);
            }
            GetBufMgr()->UnlockAndRelease(qBuf);
            (void)walWriterContext->EndAtomicWal();
            return INVALID_PAGE_ID;
        }
        BtrQueuePage *nextQPage = GetBtrQueuePage(nextBuf);
        ReusablePageQueue *nextQueue = nextQPage->GetQueue<ReusablePageQueue>();
        if (nextQPage->IsTail() || !nextQueue->IsEmpty(pdbId)) {
            GetBufMgr()->UnlockAndRelease(qBuf);
            qBuf = nextBuf;
        } else {
            /* Next page in the list has an empty queue */
            freePage = RemoveQueuePage(type, qBuf, nextBuf);
            break;
        }
    }
    (void)walWriterContext->EndAtomicWal();
    BtrQueuePage *qPage = GetBtrQueuePage(qBuf);
    if (freePage.IsValid() && qPage->IsTail()) {
        needBgExtend = true;
    }
    GetBufMgr()->UnlockAndRelease(qBuf);

    if (unlikely(needUpdateTimestamp)) {
        if (STORAGE_FUNC_SUCC(AcquireRecyclePartitionMetaBuf(LW_EXCLUSIVE))) {
            if (STORAGE_FUNC_FAIL(UpdateTimestampIfNecessary(LW_EXCLUSIVE))) {
                ReleaseRecyclePartitionMetaBuf(false);
                return INVALID_PAGE_ID;
            }
            ReleaseRecyclePartitionMetaBuf();
            return freePage;
        }
        needRetry = false;
        return INVALID_PAGE_ID;
    }

    if (needBgExtend && g_storageInstance->GetGuc()->numObjSpaceMgrWorkers > 0 && !segment->IsTempSegment()) {
        TryRegisterExtendTask();
    }

    return freePage;
}

RetStatus BtreeRecyclePartition::FreeListPush(const PageId reusablePage)
{
    const BtrRecycleQueueType type = BtrRecycleQueueType::FREE;
    if (STORAGE_FUNC_FAIL(AcquireRecyclePartitionMetaBuf(LW_SHARED))) {
        return DSTORE_FAIL;
    }
    if (STORAGE_FUNC_FAIL(UpdateTimestampIfNecessary(LW_SHARED))) {
        ReleaseRecyclePartitionMetaBuf(false);
        return DSTORE_FAIL;
    }
    BtreeRecycleQueue freeList = GetBtreeRecycleQueue(type);

    /* Check the last available queue of the free list. */
    BufferDesc *qBuf = freeList.GetQueueTailBuf(LW_EXCLUSIVE, unlikely(needCheckCreatedXid) ? createdXid : INVALID_XID);
    ReleaseRecyclePartitionMetaBuf();
    if (unlikely(qBuf == INVALID_BUFFER_DESC)) {
        return DSTORE_FAIL;
    }
    AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
    ReusablePageQueue *queue = GetBtrQueuePage(qBuf)->GetQueue<ReusablePageQueue>();
    if (!queue->IsFull()) {
        walWriterContext->BeginAtomicWal(thrd->GetCurrentXid());
        if (STORAGE_FUNC_FAIL(ReusablePageQueuePush(type, qBuf, {INVALID_CSN, reusablePage}, false))) {
            (void)walWriterContext->EndAtomicWal();
            GetBufMgr()->UnlockAndRelease(qBuf);
            return DSTORE_FAIL;
        }
        (void)walWriterContext->EndAtomicWal();
        GetBufMgr()->UnlockAndRelease(qBuf);
        return DSTORE_SUCC;
    }
    /* The tail page is full, utilize the new page as a new tail page and
     * add it to the end of the list. */
    walWriterContext->BeginAtomicWal(thrd->GetCurrentXid());
    AddQueueTail(type, qBuf, reusablePage);
    BufferDesc *tailBuf = InitNewQueuePage<ReusablePageQueue>(type, reusablePage);
    (void)walWriterContext->EndAtomicWal();
    GetBufMgr()->UnlockAndRelease(qBuf);
    GetBufMgr()->UnlockAndRelease(tailBuf);
    return DSTORE_SUCC;
}

/*
 * FreeListBatchPushNewPages
 * Push the btree pages from the new pages to FreeQueuePages in order
 * Create the free pages from the new pages in reverse order
 * Return numNewBtrPages: the number of pages will be created to be btree pages
 */
RetStatus BtreeRecyclePartition::FreeListBatchPushNewPages(PageId *newPages, uint16 numNewPages, uint16 &numNewBtrPages,
                                                           bool isExtendBg)
{
    const BtrRecycleQueueType type = BtrRecycleQueueType::FREE;
    if (STORAGE_FUNC_FAIL(AcquireRecyclePartitionMetaBuf(LW_SHARED))) {
        return DSTORE_FAIL;
    }
    BtreeRecycleQueue freeList = GetBtreeRecycleQueue(type);
    /* Check the last available queue of the free list. */
    BufferDesc *qBuf = freeList.GetQueueTailBuf(LW_EXCLUSIVE, INVALID_XID);
    if (unlikely(qBuf == INVALID_BUFFER_DESC)) {
        ReleaseRecyclePartitionMetaBuf();
        return DSTORE_FAIL;
    }

    bool needResetFreePage = false;
    uint16 startPagePos = 0;
    uint16 endPagePos = numNewPages;
    AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
    bool needWal = !(segment->IsTempSegment());
    if (likely(needWal)) {
        walWriterContext->BeginAtomicWal(thrd->GetCurrentXid());
    }
    while (startPagePos < endPagePos) {
        if (unlikely(STORAGE_FUNC_FAIL(
            BatchPushFreeQueue(qBuf, newPages, startPagePos, endPagePos, needResetFreePage)))) {
            if (likely(needWal)) {
                (void)walWriterContext->ResetForAbort();
            }
            GetBufMgr()->UnlockAndRelease(qBuf);
            ReleaseRecyclePartitionMetaBuf();
            ErrLog(DSTORE_ERROR, MODULE_INDEX,
                ErrMsg("BatchPushFreeQueue fail when FreeList batch push new BtrPages."));
            return DSTORE_FAIL;
        }

        PageId nextFreeQueuePage = GetBtrQueuePage(qBuf)->GetNext();
        if (nextFreeQueuePage.IsValid()) {
            BufferDesc *nextBuf = GetBufMgr()->Read(this->GetPdbId(), nextFreeQueuePage, LW_EXCLUSIVE,
                BufferPoolReadFlag::CreateNewPage());
            GetBufMgr()->UnlockAndRelease(qBuf);
            qBuf = nextBuf;
            if (unlikely(qBuf == INVALID_BUFFER_DESC)) {
                if (likely(needWal)) {
                    (void)walWriterContext->ResetForAbort();
                }
                ReleaseRecyclePartitionMetaBuf();
                ErrLog(DSTORE_ERROR, MODULE_INDEX,
                       ErrMsg("Create FreeQueuePage fail when FreeList batch push new BtrPages."));
                return DSTORE_FAIL;
            }
        }
    }
    numNewBtrPages = isExtendBg ? static_cast<uint16>(endPagePos) : static_cast<uint16>(endPagePos + 1);

    if (needResetFreePage) {
        StorageAssert(qBuf != nullptr);
        GetBtrQueuePage(qBuf)->Reset<ReusablePageQueue>(qBuf->GetPageId(), INVALID_XID);
        (void)GetBufMgr()->MarkDirty(qBuf);
        GenerateWalForRecyclePartitionInitPage(qBuf, type);
    }

    if (likely(needWal)) {
        (void)walWriterContext->EndAtomicWal();
    }
    GetBufMgr()->UnlockAndRelease(qBuf);
    ReleaseRecyclePartitionMetaBuf();
    return DSTORE_SUCC;
}

template<typename T>
RetStatus BtreeRecyclePartition::TakePagesFromColdRecyclePartitionQueue(BtreeRecyclePartition *coldPartition,
                                                                        BtrRecycleQueueType type)
{
    /* Get queue of the to-be-recycled partion */
    /* Note: we must hold an exclusive lock here before reading the RecycleRootMeta page! */
    if (STORAGE_FUNC_FAIL(coldPartition->AcquireRecyclePartitionMetaBuf(LW_EXCLUSIVE))) {
        return DSTORE_FAIL;
    }
    BtreeRecycleQueue coldQueue = coldPartition->GetBtreeRecycleQueue(type);
    BufferDesc *coldQueueHeadBuf = coldQueue.GetQueueHeadBuf(LW_SHARED);
    if (unlikely(!BtrQueuePage::isQueuePageValid(coldQueueHeadBuf, createdXid))) {
        if (coldQueueHeadBuf != INVALID_BUFFER_DESC) {
            GetBufMgr()->UnlockAndRelease(coldQueueHeadBuf);
        }
        coldPartition->ReleaseRecyclePartitionMetaBuf();
        return DSTORE_FAIL;
    }

    BtreeRecycleQueue currQueue = GetBtreeRecycleQueue(type);
    /* Check the last available queue of the free list. */
    BufferDesc *currQueueTailBuf = currQueue.GetQueueTailBuf(LW_EXCLUSIVE, createdXid);
    if (unlikely(!BtrQueuePage::isQueuePageValid(currQueueTailBuf, createdXid))) {
        if (currQueueTailBuf != INVALID_BUFFER_DESC) {
            GetBufMgr()->UnlockAndRelease(currQueueTailBuf);
        }
        GetBufMgr()->UnlockAndRelease(currQueueTailBuf);
        GetBufMgr()->UnlockAndRelease(coldQueueHeadBuf);
        coldPartition->ReleaseRecyclePartitionMetaBuf();
        return DSTORE_FAIL;
    }

    /**
     * Three locks are acquired that need to be released later:
     *  1. The queue tail page of this coldPartition's recycle queue
     *  2. The head page of the other coldPartition's recycle queue (queue to be taken over)
     *  3. The partition meta page of the coldPartition.
    */

    /* We've got the old head of coldPartition, now initialize a new head for it */
    RetStatus retStatus = coldPartition->InitNewQueueHead<T>(type);
    if (STORAGE_FUNC_SUCC(retStatus)) {
        AtomicWalWriterContext *writerContext = thrd->m_walWriterContext;
        writerContext->BeginAtomicWal(thrd->GetCurrentXid());
        /* Add the head page of the coldPartition's free queue to tail page of the current free queue */
        AddQueueTail(type, currQueueTailBuf, coldQueueHeadBuf->GetPageId());
        writerContext->EndAtomicWal();
    }

    coldPartition->ReleaseRecyclePartitionMetaBuf();
    bufMgr->UnlockAndRelease(coldQueueHeadBuf, BufferPoolUnlockContentFlag());
    bufMgr->UnlockAndRelease(currQueueTailBuf, BufferPoolUnlockContentFlag());
    return retStatus;
}

RetStatus BtreeRecyclePartition::TakeOverColdRecyclePartition(BtreeRecyclePartition *coldPartition)
{
    /* Take over all pages in free queue */
    if (STORAGE_FUNC_FAIL(
        TakePagesFromColdRecyclePartitionQueue<RecyclablePageQueue>(coldPartition, BtrRecycleQueueType::RECYCLE))) {
        return DSTORE_FAIL;
    }

    if (STORAGE_FUNC_FAIL(
        TakePagesFromColdRecyclePartitionQueue<ReusablePageQueue>(coldPartition, BtrRecycleQueueType::FREE))) {
        /* It's acceptable to take over only recycle queue in this round */
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

template <typename T>
RetStatus BtreeRecyclePartition::InitNewQueueHead(BtrRecycleQueueType type)
{
    PageId head = GetNewPage();
    if (!head.IsValid()) {
        return DSTORE_FAIL;
    }
    BufferDesc *headBuf = GetBufMgr()->Read(this->GetPdbId(), head, LW_EXCLUSIVE, BufferPoolReadFlag::CreateNewPage());
    if (headBuf == INVALID_BUFFER_DESC) {
        return DSTORE_FAIL;
    }

    BtrQueuePage *qPage = GetBtrQueuePage(headBuf);
    StorageAssert(qPage != nullptr);
    qPage->Init<T>(head, createdXid);
    (void)GetBufMgr()->MarkDirty(headBuf);

    AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
    walWriterContext->BeginAtomicWal(thrd->GetCurrentXid());
    GenerateWalForRecyclePartitionInitPage(headBuf, type);

    BtrRecyclePartitionMeta partitionMeta = GetRecyclePartitionMeta();
    partitionMeta.SetQueueHead(type, head);
    GenerateWalForRecyclePartitionMetaSetHead(type, head);
    (void)walWriterContext->EndAtomicWal();
    GetBufMgr()->UnlockAndRelease(headBuf);

    return DSTORE_SUCC;
}

PageId BtreeRecyclePartition::RemoveQueueHead(BtrRecycleQueueType type, BufferDesc *headBuf)
{
    PageId newHead = GetBtrQueuePage(headBuf)->GetNext();
    assert(newHead.IsValid()); /* Not tail page. */
    BtrRecyclePartitionMeta partitionMeta = GetRecyclePartitionMeta();
    partitionMeta.SetQueueHead(type, newHead);
    GenerateWalForRecyclePartitionMetaSetHead(type, newHead);

    /* No one else should have reference to headBuf. */
    PageId oldHead = headBuf->GetPageId();
    return oldHead;
}

template <typename T>
BufferDesc *BtreeRecyclePartition::InitNewQueuePage(BtrRecycleQueueType type, const PageId page)
{
    BufferDesc *buf = GetBufMgr()->Read(this->GetPdbId(), page, LW_EXCLUSIVE);
    if (unlikely(buf == INVALID_BUFFER_DESC)) {
        return INVALID_BUFFER_DESC;
    }
    BtrQueuePage *qPage = GetBtrQueuePage(buf);
    StorageAssert(qPage != nullptr);
    qPage->Reset<T>(page, createdXid);
    (void)GetBufMgr()->MarkDirty(buf);
    GenerateWalForRecyclePartitionInitPage(buf, type);
    return buf;
}

void BtreeRecyclePartition::AddQueueTail(BtrRecycleQueueType type, BufferDesc *tailBuf, const PageId newTail)
{
    BtreeRecycleQueue recycleQueue(this->GetPdbId(), INVALID_PAGE_ID, type, GetBufMgr());
    /* Add a new page to list. */
    recycleQueue.AddTailPage(tailBuf, newTail);
    GenerateWalForRecycleQueuePageMetaSetNext(tailBuf, newTail);
}

PageId BtreeRecyclePartition::RemoveQueueTail(BtrRecycleQueueType type, BufferDesc *buf)
{
    StorageAssert(buf != INVALID_BUFFER_DESC);
    BtreeRecycleQueue recycleQueue(this->GetPdbId(), INVALID_PAGE_ID, type, GetBufMgr());
    BufferDesc *tailBuf = recycleQueue.RemoveTailPage(buf);
    GenerateWalForRecycleQueuePageMetaSetNext(buf, INVALID_PAGE_ID);
    if (likely(tailBuf != INVALID_BUFFER_DESC)) {
        PageId tailPageId = tailBuf->GetPageId();
        GetBufMgr()->UnlockAndRelease(tailBuf);
        return tailPageId;
    }
    return INVALID_PAGE_ID;
}

PageId BtreeRecyclePartition::RemoveQueuePage(BtrRecycleQueueType type, BufferDesc *buf, BufferDesc *nextBuf)
{
    StorageAssert(buf != INVALID_BUFFER_DESC);
    StorageAssert(nextBuf != INVALID_BUFFER_DESC);
    BtreeRecycleQueue recycleQueue(this->GetPdbId(), INVALID_PAGE_ID, type, GetBufMgr());
    recycleQueue.RemovePage(buf, nextBuf);
    GenerateWalForRecycleQueuePageMetaSetNext(buf, GetBtrQueuePage(buf)->GetNext());
    PageId next = nextBuf->GetPageId();
    GetBufMgr()->UnlockAndRelease(nextBuf);
    return next;
}

RetStatus BtreeRecyclePartition::ReusablePageQueueAllocSlot(FreeQueueSlot &slot, BufferDesc *qBuf,
                                                            const PageId emptyPage, bool needInit)
{
    CommitSeqNo nextCsn = INVALID_CSN;
    if (STORAGE_FUNC_FAIL(g_storageInstance->GetCsnMgr()->GetNextCsn(nextCsn, false))) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Get next csn failed."));
        return DSTORE_FAIL;
    }
    ReusablePage reusablePage = {nextCsn, emptyPage};

    BtrQueuePage *qPage = GetBtrQueuePage(qBuf);
    StorageAssert(qPage != nullptr);
    if (needInit) {
        qPage->Reset<ReusablePageQueue>(qBuf->GetPageId(), createdXid);
    }
    ReusablePageQueue *queue = qPage->GetQueue<ReusablePageQueue>();
    StorageAssert(queue != nullptr);
    int16 pos = queue->AllocSlot(reusablePage);
    slot = {qBuf->GetPageId(), pos};
    (void)GetBufMgr()->MarkDirty(qBuf);
    GenerateWalForRecyclePartitionAllocSlot(qBuf, reusablePage, pos, needInit);
    return DSTORE_SUCC;
}

PageId BtreeRecyclePartition::ReusablePageQueuePop(BtrRecycleQueueType type, BufferDesc *buf, CommitSeqNo minCsn)
{
    ReusablePageQueue *queue = GetBtrQueuePage(buf)->GetQueue<ReusablePageQueue>();
    StorageAssert(queue != nullptr);
    int16 pos = queue->Peek(minCsn);
    StorageAssert(pos < queue->GetCapacity());
    PageId reusablePageId = queue->Pop(pos);
    if (reusablePageId.IsValid()) {
        (void)GetBufMgr()->MarkDirty(buf);
        GenerateWalForRecyclePartitionPop(buf, type, pos);
    }
    return reusablePageId;
}

RetStatus BtreeRecyclePartition::ReusablePageQueuePush(BtrRecycleQueueType type, BufferDesc *buf,
                                                       ReusablePage reusablePage, bool needInit)
{
    ReusablePageQueue *queue = GetBtrQueuePage(buf)->GetQueue<ReusablePageQueue>();
    StorageAssert(queue != nullptr);
    int16 pos = static_cast<int16>(queue->GetTail());
    queue->Push(reusablePage);
    (void)GetBufMgr()->MarkDirty(buf);
    BtrRecyclePartitionWalInfo recyclePartitionInfo{.type = type, .pos = pos};
    GenerateWalForRecyclePartitionPush(buf, recyclePartitionInfo, reusablePage.pageId, reusablePage.csn, needInit);
    return DSTORE_SUCC;
}

PageId BtreeRecyclePartition::RecyclablePageQueuePop(BtrRecycleQueueType type, BufferDesc *buf, CommitSeqNo minCsn,
                                                     uint64 *numSkippedPage)
{
    RecyclablePageQueue *queue = GetBtrQueuePage(buf)->GetQueue<RecyclablePageQueue>();
    StorageAssert(queue != nullptr);
    int16 pos = queue->Peek(minCsn, numSkippedPage);
    StorageAssert(pos < queue->GetCapacity());
    PageId recyclablePageId = queue->Pop(pos);
    if (recyclablePageId.IsValid()) {
        (void)GetBufMgr()->MarkDirty(buf);
        GenerateWalForRecyclePartitionPop(buf, type, pos);
    }
    return recyclablePageId;
}

RetStatus BtreeRecyclePartition::RecyclablePageQueuePush(BtrRecycleQueueType type, BufferDesc *buf,
                                                         RecyclablePage recyclablePage, bool needInit)
{
    BtrQueuePage *qPage = GetBtrQueuePage(buf);
    StorageAssert(qPage != nullptr);
    if (needInit) {
        qPage->Reset<RecyclablePageQueue>(buf->GetPageId(), createdXid);
    }
    RecyclablePageQueue *queue = qPage->GetQueue<RecyclablePageQueue>();
    int16 pos = static_cast<int16>(queue->GetTail());
    queue->Push(recyclablePage);
    (void)GetBufMgr()->MarkDirty(buf);
    BtrRecyclePartitionWalInfo recyclePartitionInfo{.type = type, .pos = pos};
    GenerateWalForRecyclePartitionPush(buf, recyclePartitionInfo, recyclablePage.pageId, recyclablePage.csn, needInit);
    return DSTORE_SUCC;
}

RetStatus BtreeRecyclePartition::BatchPushFreeQueue(BufferDesc *buf, PageId *newPages, uint16 &startPagePos,
                                                    uint16 &endPagePos, bool &needResetFreePage)
{
    BtrQueuePage *qPage = GetBtrQueuePage(buf);
    StorageAssert(qPage != nullptr);
    StorageAssert(createdXid != INVALID_XID);
    bool curPageNeedReset = needResetFreePage;
    if (needResetFreePage) {
        qPage->Reset<ReusablePageQueue>(buf->GetPageId(), createdXid);
        needResetFreePage = false;
    }
    ReusablePageQueue *queue = qPage->GetQueue<ReusablePageQueue>();
    int16 pos = static_cast<int16>(queue->GetTail());
    uint16 currBatch = 0;
    uint16 numRemainingPages = static_cast<uint16>(endPagePos - startPagePos);
    while (!queue->IsFull() && currBatch < numRemainingPages) {
        RecyclablePage reusablePage{INVALID_CSN, newPages[startPagePos + currBatch]};
        queue->Push(reusablePage);
        currBatch++;
    }

    RetStatus status = DSTORE_FAIL;
    PageId nextFreeQueuePage = INVALID_PAGE_ID;
    if (currBatch < numRemainingPages) {
        StorageAssert(!qPage->GetNext().IsValid());
        nextFreeQueuePage = newPages[--endPagePos];
        qPage->SetNext(nextFreeQueuePage);
        needResetFreePage = true;
    }

    StorageAssert(currBatch <= numRemainingPages);
    (void)GetBufMgr()->MarkDirty(buf);
    status = GenerateWalForRecyclePartitionBatchPush(buf, pos, &newPages[startPagePos], currBatch, curPageNeedReset,
                                                     nextFreeQueuePage);
    startPagePos += currBatch;

    return status;
}

void BtreeRecyclePartition::GenerateWalForRecyclePartitionInitPage(BufferDesc *qBuf, BtrRecycleQueueType type)
{
    if (segment->IsTempSegment()) {
        return;
    }
    AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
    walWriterContext->RememberPageNeedWal(qBuf);
    BtrQueuePage *qPage = GetBtrQueuePage(qBuf);
    StorageAssert(qPage != nullptr);
    uint16 redoDataSize = sizeof(WalRecordBtrRecyclePartitionInitPage);

    WalRecordBtrRecyclePartitionInitPage redoData;
    WalPageHeaderContext header{.type = WAL_BTREE_RECYCLE_PARTITION_INIT_PAGE,
                                .size = redoDataSize,
                                .pageId = qBuf->GetPageId(),
                                .preWalId = qPage->GetWalId(),
                                .prePlsn = qPage->GetPlsn(),
                                .preGlsn = qPage->GetGlsn(),
                                .glsnChangedFlag = (qPage->GetWalId() != walWriterContext->GetWalId()),
                                .preVersion = qBuf->GetFileVersion()};
    redoData.SetHeader(header, type, createdXid);
    walWriterContext->PutNewWalRecord(&redoData);
}

void BtreeRecyclePartition::GenerateWalForTimestampUpdate()
{
    if (segment->IsTempSegment()) {
        return;
    }
    BtrRecyclePartitionMeta partitionMeta = GetRecyclePartitionMeta();
    AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
    walWriterContext->BeginAtomicWal(thrd->GetCurrentXid());
    walWriterContext->RememberPageNeedWal(partitionMeta.recyclePartitionMetaBuf);
    BtrRecyclePartitionMetaPage *metaPage = static_cast<BtrRecyclePartitionMetaPage*>(
                                            partitionMeta.recyclePartitionMetaBuf->GetPage());
    uint16 redoDataSize = sizeof(WalRecordBtrRecyclePartitionMetaSetTimestamp);
    WalRecordBtrRecyclePartitionMetaSetTimestamp redoData;
    WalPageHeaderContext header{.type = WAL_BTREE_RECYCLE_PARTITION_META_TIMESTAMP_UPDATE,
                                .size = redoDataSize,
                                .pageId = partitionMeta.recyclePartitionMetaBuf->GetPageId(),
                                .preWalId = metaPage->GetWalId(),
                                .prePlsn = metaPage->GetPlsn(),
                                .preGlsn = metaPage->GetGlsn(),
                                .glsnChangedFlag = (metaPage->GetWalId() != walWriterContext->GetWalId()),
                                .preVersion = partitionMeta.recyclePartitionMetaBuf->GetFileVersion()};
    redoData.SetHeader(header, metaPage->accessTimestamp);
    walWriterContext->PutNewWalRecord(&redoData);
    walWriterContext->EndAtomicWal();
}

void BtreeRecyclePartition::GenerateWalForRecyclePartitionMetaSetHead(BtrRecycleQueueType type, const PageId headPageId)
{
    if (segment->IsTempSegment()) {
        return;
    }
    BtrRecyclePartitionMeta partitionMeta = GetRecyclePartitionMeta();
    AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
    walWriterContext->RememberPageNeedWal(partitionMeta.recyclePartitionMetaBuf);
    Page *metaPage = partitionMeta.recyclePartitionMetaBuf->GetPage();
    uint16 redoDataSize = sizeof(WalRecordBtrRecyclePartitionMetaSetHead);

    WalRecordBtrRecyclePartitionMetaSetHead redoData;
    WalPageHeaderContext header{.type = WAL_BTREE_RECYCLE_PARTITION_META_SET_HEAD,
                                .size = redoDataSize,
                                .pageId = partitionMeta.recyclePartitionMetaBuf->GetPageId(),
                                .preWalId = metaPage->GetWalId(),
                                .prePlsn = metaPage->GetPlsn(),
                                .preGlsn = metaPage->GetGlsn(),
                                .glsnChangedFlag = (metaPage->GetWalId() != walWriterContext->GetWalId()),
                                .preVersion = partitionMeta.recyclePartitionMetaBuf->GetFileVersion()};
    redoData.SetHeader(header, headPageId, type);
    walWriterContext->PutNewWalRecord(&redoData);
}

void BtreeRecyclePartition::GenerateWalForRecycleQueuePageMetaSetNext(BufferDesc *qBuf, const PageId next)
{
    if (segment->IsTempSegment()) {
        return;
    }
    AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
    walWriterContext->RememberPageNeedWal(qBuf);
    BtrQueuePage *qPage = GetBtrQueuePage(qBuf);
    StorageAssert(qPage != nullptr);
    uint16 redoDataSize = sizeof(WalRecordBtrQueuePageMetaSetNext);

    WalRecordBtrQueuePageMetaSetNext redoData;
    WalPageHeaderContext header{.type = WAL_BTREE_RECYCLE_QUEUE_PAGE_META_SET_NEXT,
                                .size = redoDataSize,
                                .pageId = qBuf->GetPageId(),
                                .preWalId = qPage->GetWalId(),
                                .prePlsn = qPage->GetPlsn(),
                                .preGlsn = qPage->GetGlsn(),
                                .glsnChangedFlag = (qPage->GetWalId() != walWriterContext->GetWalId()),
                                .preVersion = qBuf->GetFileVersion()};
    redoData.SetHeader(header, next);
    walWriterContext->PutNewWalRecord(&redoData);
}

void BtreeRecyclePartition::GenerateWalForRecyclePartitionPush(BufferDesc *qBuf,
    BtrRecyclePartitionWalInfo recyclePartitionInfo, PageId pagePush, CommitSeqNo pageCsn, bool needReset)
{
    if (segment->IsTempSegment()) {
        return;
    }
    AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
    walWriterContext->RememberPageNeedWal(qBuf);
    BtrQueuePage *qPage = GetBtrQueuePage(qBuf);
    StorageAssert(qPage != nullptr);

    WalRecordBtrRecyclePartitionPush redoData;
    WalPageHeaderContext header{.type = WAL_BTREE_RECYCLE_PARTITION_PUSH,
                                .size = sizeof(WalRecordBtrRecyclePartitionPush),
                                .pageId = qBuf->GetPageId(),
                                .preWalId = qPage->GetWalId(),
                                .prePlsn = qPage->GetPlsn(),
                                .preGlsn = qPage->GetGlsn(),
                                .glsnChangedFlag = (qPage->GetWalId() != walWriterContext->GetWalId()),
                                .preVersion = qBuf->GetFileVersion()};

    redoData.SetHeader(header, recyclePartitionInfo, needReset, qPage->GetCreatedXid());
    redoData.SetPage(pagePush, pageCsn);
    walWriterContext->PutNewWalRecord(&redoData);
}

void BtreeRecyclePartition::GenerateWalForRecyclePartitionPop(BufferDesc *qBuf, BtrRecycleQueueType type, int16 pos)
{
    if (segment->IsTempSegment()) {
        return;
    }
    AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
    walWriterContext->RememberPageNeedWal(qBuf);
    BtrQueuePage *qPage = GetBtrQueuePage(qBuf);
    StorageAssert(qPage != nullptr);
    uint16 redoDataSize = sizeof(WalRecordBtrRecyclePartitionPop);

    WalRecordBtrRecyclePartitionPop redoData;
    WalPageHeaderContext header{.type = WAL_BTREE_RECYCLE_PARTITION_POP,
                                .size = redoDataSize,
                                .pageId = qBuf->GetPageId(),
                                .preWalId = qPage->GetWalId(),
                                .prePlsn = qPage->GetPlsn(),
                                .preGlsn = qPage->GetGlsn(),
                                .glsnChangedFlag = (qPage->GetWalId() != walWriterContext->GetWalId()),
                                .preVersion = qBuf->GetFileVersion()};

    BtrRecyclePartitionWalInfo recyclePartitionInfo{.type = type,
                                                    .pos = pos};
    redoData.SetHeader(header, recyclePartitionInfo);
    walWriterContext->PutNewWalRecord(&redoData);
}

RetStatus BtreeRecyclePartition::GenerateWalForRecyclePartitionBatchPush(BufferDesc *qBuf,
    int16 pos, PageId *newBtrPages, uint16 numBtrPages, bool needReset, const PageId nextFreeQueuePage)
{
    if (segment->IsTempSegment()) {
        return DSTORE_SUCC;
    }
    bool hasNextFreeQueuePage = (nextFreeQueuePage != INVALID_PAGE_ID);
    AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
    walWriterContext->RememberPageNeedWal(qBuf);
    BtrQueuePage *qPage = GetBtrQueuePage(qBuf);
    StorageAssert(qPage != nullptr);
    uint16 newBtrPagesSize = static_cast<uint16>(numBtrPages * sizeof(PageId));
    /* newBtrPages(PageId list) + nextFreeQueuePage(optional) + needReset(bool) */
    uint16 redoDataSize = sizeof(WalRecordBtrRecyclePartitionBatchPush) + newBtrPagesSize + sizeof(bool);
    if (hasNextFreeQueuePage) {
        redoDataSize += sizeof(PageId);
    }

    WalRecordBtrRecyclePartitionBatchPush *redoData = nullptr;
    redoData = (WalRecordBtrRecyclePartitionBatchPush *)DstorePalloc(redoDataSize);
    if (unlikely(redoData == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
            ErrMsg("DstorePalloc fail when GenerateWalForRecyclePartitionBatchPush."));
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        return DSTORE_FAIL;
    }
    WalPageHeaderContext header{.type = WAL_BTREE_RECYCLE_PARTITION_BATCH_PUSH,
                                .size = redoDataSize,
                                .pageId = qBuf->GetPageId(),
                                .preWalId = qPage->GetWalId(),
                                .prePlsn = qPage->GetPlsn(),
                                .preGlsn = qPage->GetGlsn(),
                                .glsnChangedFlag = (qPage->GetWalId() != walWriterContext->GetWalId()),
                                .preVersion = qBuf->GetFileVersion()};

    BtrRecyclePartitionWalInfo recyclePartitionInfo{.type = BtrRecycleQueueType::FREE,
                                                    .pos = pos};
    redoData->SetHeader(header, recyclePartitionInfo, hasNextFreeQueuePage ? numBtrPages + 1 : numBtrPages, createdXid);
    redoData->SetData(static_cast<char *>(static_cast<void *>(newBtrPages)), newBtrPagesSize, needReset,
                      nextFreeQueuePage);
    walWriterContext->PutNewWalRecord(redoData);
    DstorePfree(redoData);
    return DSTORE_SUCC;
}

void BtreeRecyclePartition::GenerateWalForRecyclePartitionAllocSlot(BufferDesc *qBuf,
    ReusablePage page, int16 pos, bool needReset)
{
    if (segment->IsTempSegment()) {
        return;
    }
    const BtrRecycleQueueType type = BtrRecycleQueueType::FREE;
    AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
    walWriterContext->RememberPageNeedWal(qBuf);
    BtrQueuePage *qPage = GetBtrQueuePage(qBuf);
    StorageAssert(qPage != nullptr);

    WalRecordBtrRecyclePartitionAllocSlot redoData;
    WalPageHeaderContext header{.type = WAL_BTREE_RECYCLE_PARTITION_ALLOC_SLOT,
                                .size = sizeof(WalRecordBtrRecyclePartitionAllocSlot),
                                .pageId = qBuf->GetPageId(),
                                .preWalId = qPage->GetWalId(),
                                .prePlsn = qPage->GetPlsn(),
                                .preGlsn = qPage->GetGlsn(),
                                .glsnChangedFlag = (qPage->GetWalId() != walWriterContext->GetWalId()),
                                .preVersion = qBuf->GetFileVersion()};

    BtrRecyclePartitionWalInfo recyclePartitionInfo{.type = type,
                                                    .pos = pos};
    redoData.SetHeader(header, recyclePartitionInfo, needReset, qPage->GetCreatedXid());
    redoData.SetPage(page);
    walWriterContext->PutNewWalRecord(&redoData);
}

void BtreeRecyclePartition::GenerateWalForRecyclePartitionWriteSlot(BufferDesc *qBuf, ReusablePage page, int16 pos)
{
    if (segment->IsTempSegment()) {
        return;
    }
    const BtrRecycleQueueType type = BtrRecycleQueueType::FREE;
    AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
    walWriterContext->RememberPageNeedWal(qBuf);
    BtrQueuePage *qPage = GetBtrQueuePage(qBuf);
    StorageAssert(qPage != nullptr);

    WalRecordBtrRecyclePartitionWriteSlot redoData;
    WalPageHeaderContext header{.type = WAL_BTREE_RECYCLE_PARTITION_WRITE_SLOT,
                                .size = sizeof(WalRecordBtrRecyclePartitionWriteSlot),
                                .pageId = qBuf->GetPageId(),
                                .preWalId = qPage->GetWalId(),
                                .prePlsn = qPage->GetPlsn(),
                                .preGlsn = qPage->GetGlsn(),
                                .glsnChangedFlag = (qPage->GetWalId() != walWriterContext->GetWalId()),
                                .preVersion = qBuf->GetFileVersion()};

    BtrRecyclePartitionWalInfo recyclePartitionInfo{.type = type,
                                                    .pos = pos};
    redoData.SetHeader(header, recyclePartitionInfo);
    redoData.SetPage(page);
    walWriterContext->PutNewWalRecord(&redoData);
}
}
