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
 * dstore_index_segment.cpp
 *
 * IDENTIFICATION
 *        src/tablespace/dstore_index_segment.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "tablespace/dstore_index_segment.h"
#include "common/log/dstore_log.h"
#include "index/dstore_btree_recycle_wal.h"
#include "lock/dstore_lock_datatype.h"
#include "transaction/dstore_transaction.h"
#include "page/dstore_index_page.h"
#include "wal/dstore_wal_write_context.h"
#include "tablespace/dstore_tablespace_wal.h"
#include "tablespace/dstore_tablespace_utils.h"
#include "index/dstore_btree_recycle_partition.h"
#include "page/dstore_btr_recycle_root_meta_page.h"
#include "page/dstore_btr_recycle_partition_meta_page.h"
#include "tablespace/dstore_table_space_perfunit.h"
#include "index/dstore_btree_perf_unit.h"

namespace DSTORE {

IndexSegment::IndexSegment(PdbId pdbId, const PageId &segmentId, TablespaceId tablespaceId,
    BufMgrInterface *bufMgr, DstoreMemoryContext ctx, SegmentType segmentType)
    : DataSegment(pdbId, segmentId, segmentType, tablespaceId, bufMgr, ctx),
      m_btrRecyclePartition(nullptr)
{}

RetStatus IndexSegment::InitSegment()
{
    if (STORAGE_FUNC_FAIL(Init())) {
        return DSTORE_FAIL;
    }

    /* BtreeRecyclePartition initialization will be delayed till first Free page request */
    m_btrRecyclePartition = nullptr;

    m_isInitialized = true;
    return DSTORE_SUCC;
}

IndexSegment::~IndexSegment()
{
    if (m_btrRecyclePartition != nullptr) {
        delete m_btrRecyclePartition;
    }
}

/* AddNewPagesToBtreeRecycle
 * Add the all new pages to BtreeRecyclePartition
 * The first page is returned to the caller, and the rest are push to the free page list.
 */
RetStatus IndexSegment::AddNewPagesToBtreeRecycle(PageId *newPageId, bool isExtendBg)
{
    LatencyStat::Timer timer(&TableSpacePerfUnit::GetInstance().m_indexNewPageLatency);

    uint16 numFreeDataPages;
    AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
    BufferDesc *segMetaPageBuf = ReadSegMetaPageBuf(LW_EXCLUSIVE);
    if (STORAGE_FUNC_FAIL(PrepareFreeDataPages(&numFreeDataPages, segMetaPageBuf))) {
        UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);
        return DSTORE_FAIL;
    }

    if (unlikely(numFreeDataPages == 0)) {
        storage_set_error(TBS_ERROR_SEGMENT_HAS_NO_SPACE);
        UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);
        return DSTORE_FAIL;
    }

    uint16 numNewPages = DstoreMin(numFreeDataPages, m_maxAddNewPageCount);
    PageId newPages[PAGES_ADD_TO_FSM_PER_TIME];
    if (STORAGE_FUNC_FAIL(GetFreeDataPageIds(segMetaPageBuf, numNewPages, newPages))) {
        UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);
        return DSTORE_FAIL;
    }

    DataSegmentMetaPage *metaPage = static_cast<DataSegmentMetaPage *>(segMetaPageBuf->GetPage());
    metaPage->AddDataPages(newPages[0], newPages[numNewPages - 1], numNewPages);
    bool pagesIsReused = metaPage->lastExtentIsReused;

    (void)m_bufMgr->MarkDirty(segMetaPageBuf);

    /* wal for adding pages to BtreeRecycle */
    walWriterContext->BeginAtomicWal(thrd->GetCurrentXid());
    bool glsnChangedFlag = (metaPage->GetWalId() != walWriterContext->GetWalId());
    WalRecordTbsSegMetaAdjustDataPagesInfo walData;
    walData.SetHeader({WAL_TBS_SEG_META_ADJUST_DATA_PAGES_INFO, sizeof(walData), GetSegmentMetaPageId(),
                       metaPage->GetWalId(), metaPage->GetPlsn(), metaPage->GetGlsn(), glsnChangedFlag,
                       segMetaPageBuf->GetFileVersion()});
    walData.SetData(metaPage->dataFirst, metaPage->dataLast, metaPage->dataBlockCount, metaPage->addedPageId);
    walWriterContext->RememberPageNeedWal(segMetaPageBuf);
    walWriterContext->PutNewWalRecord(&walData);
    /* Some new pages has been added to the free queue, and at this point, a core occurs, but modify of segment meta
     * page wal not save; These pages may be repeatedly added to the free queue later. Need to save wal for segment
     * meta first. */
    (void)walWriterContext->EndAtomicWal();

    /* numNewPages indicates total count of new pages; numNewBtrPages indicates how many pages are used as btree pages.
       The first page will not be pushed into free list when extending new pages in worker threads . */
    uint16 numNewBtrPages = numNewPages;
    /* Everywhere a new page is fetched will initialize the page, so there's no need to InitBtrPage here. */
    CreateNewPages(newPages, numNewBtrPages, pagesIsReused);

    if (isExtendBg) {
        if (STORAGE_FUNC_FAIL(m_btrRecyclePartition->FreeListBatchPushNewPages(
            &newPages[0], static_cast<uint16>(numNewPages), numNewBtrPages, isExtendBg))) {
            UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);
            ErrLog(DSTORE_PANIC, MODULE_SEGMENT, ErrMsg("Failed FreeListBatchPushNewPages"));
            return DSTORE_FAIL;
        }
    } else {
        if (numNewPages > 1) {
            if (STORAGE_FUNC_FAIL(m_btrRecyclePartition->FreeListBatchPushNewPages(
                &newPages[1], static_cast<uint16>(numNewPages - 1), numNewBtrPages, isExtendBg))) {
                UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);
                ErrLog(DSTORE_PANIC, MODULE_SEGMENT, ErrMsg("Failed FreeListBatchPushNewPages"));
                return DSTORE_FAIL;
            }
        }
    }

    UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);
    /* Make sure we have a valid PageId returned here */
    *newPageId = newPages[0];
    return DSTORE_SUCC;
}

/* Create all the new btree pages in a batch */
void IndexSegment::CreateNewPages(PageId *newPages, uint16 numNewPages, bool pagesIsReused)
{
    if (pagesIsReused && !IsTempSegment()) {
        return;
    }
    BufferDesc **bufDescs = static_cast<BufferDesc **>(DstorePalloc0(numNewPages * sizeof(BufferDesc *)));
    if (unlikely(bufDescs == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("DstorePalloc fail when CreateNewPages."));
        storage_set_error(INDEX_ERROR_MEMORY_ALLOC);
        return;
    }

    /* BatchCreateNewPage would fail if create PD failed. We' don't care if pd created successfully because
         * we don't use these pages immediately. The page would get a PD when Read. */
    (void)m_bufMgr->BatchCreateNewPage(this->GetPdbId(), newPages, numNewPages, bufDescs);
    for (uint16 i = 0; i < numNewPages; i++) {
        if (bufDescs[i] != INVALID_BUFFER_DESC) {
            m_bufMgr->UnlockAndRelease(bufDescs[i]);
        }
    }

    DstorePfree(bufDescs);
}

void IndexSegment::BatchBufferDescUnlockAndRelease(BufferDesc **bufDescs, uint16 numBufferDesc)
{
    if (unlikely(bufDescs == nullptr)) {
        return;
    }

    for (uint16 i = 0; i < numBufferDesc; i++) {
        m_bufMgr->UnlockAndRelease(bufDescs[i]);
    }
}

PageId IndexSegment::GetNewPage(bool isExtendBg)
{
    if (unlikely(!m_isInitialized)) {
        storage_set_error(TBS_ERROR_SEGMENT_IS_NOT_INIT);
        return INVALID_PAGE_ID;
    }
    if (unlikely(m_isDrop)) {
        storage_set_error(TBS_ERROR_SEGMENT_USE_AFTER_DROP);
        return INVALID_PAGE_ID;
    }
    if (unlikely(m_btrRecyclePartition == nullptr) && STORAGE_FUNC_FAIL(InitBtrRecyclePartition())) {
        return INVALID_PAGE_ID;
    }

    StorageAssert(m_btrRecyclePartition != nullptr);
    StorageAssert(m_btrRecyclePartition->createdXid != INVALID_XID);
    PageId result = INVALID_PAGE_ID;
    PageId oldAddedPageId = INVALID_PAGE_ID;
    BufferDesc *segMetaPageBuf;
    DataSegmentMetaPage *dataSegmentMetaPage;
    segMetaPageBuf = ReadSegMetaPageBuf(LW_SHARED);
    dataSegmentMetaPage = static_cast<DataSegmentMetaPage *>(segMetaPageBuf->GetPage());
    oldAddedPageId = dataSegmentMetaPage->addedPageId;
    UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);

    PageId targetPageId = INVALID_PAGE_ID;
    LockTag tag;
    tag.SetTableExtensionLockTag(GetPdbId(), m_btrRecyclePartition->recyclePartitionMeta);
    LockErrorInfo errorInfo = {0};
    LockMgr *lockMgr = g_storageInstance->GetLockMgr();
    if (STORAGE_FUNC_FAIL(lockMgr->Lock(&tag, DSTORE_EXCLUSIVE_LOCK, false, &errorInfo))) {
        return INVALID_PAGE_ID;
    }

    ErrLog(DSTORE_DEBUG1, MODULE_SEGMENT,
           ErrMsg("Index data segment (%d, %u) attempt to get new page", GetSegmentMetaPageId().m_fileId,
                  GetSegmentMetaPageId().m_blockId));
    segMetaPageBuf = ReadSegMetaPageBuf(LW_SHARED);
    dataSegmentMetaPage = static_cast<DataSegmentMetaPage *>(segMetaPageBuf->GetPage());
    PageId addPageId = dataSegmentMetaPage->addedPageId;
    UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);
    if (unlikely(addPageId != oldAddedPageId)) {
        bool needRetry = false;
        targetPageId = m_btrRecyclePartition->FreeListPop(needRetry);
        if (targetPageId != INVALID_PAGE_ID) {
            lockMgr->Unlock(&tag, DSTORE_EXCLUSIVE_LOCK);
            return targetPageId;
        }
    }

    if (STORAGE_FUNC_FAIL(AddNewPagesToBtreeRecycle(&result, isExtendBg))) {
        ErrLog(DSTORE_ERROR, MODULE_INDEX,
               ErrMsg("Fail extend cur page {%u,%u}.", GetSegmentMetaPageId().m_fileId,
                      GetSegmentMetaPageId().m_blockId));
        lockMgr->Unlock(&tag, DSTORE_EXCLUSIVE_LOCK);
        return INVALID_PAGE_ID;
    }
    lockMgr->Unlock(&tag, DSTORE_EXCLUSIVE_LOCK);
    return result;
}

PageId IndexSegment::GetNewPageFromUnassignedPages()
{
    LockTag tag;
    tag.SetTableExtensionLockTag(this->GetPdbId(), GetSegmentMetaPageId());
    LockErrorInfo errorInfo = {0};
    LockMgr *lockMgr = g_storageInstance->GetLockMgr();
    if (STORAGE_FUNC_FAIL(lockMgr->Lock(&tag, DSTORE_EXCLUSIVE_LOCK, false, &errorInfo))) {
        return INVALID_PAGE_ID;
    }

    BufferDesc *segMetaPageBuf = ReadSegMetaPageBuf(LW_EXCLUSIVE);
    DataSegmentMetaPage *metaPage = static_cast<DataSegmentMetaPage *>(segMetaPageBuf->GetPage());
    uint16 unassignedCount = metaPage->GetUnassignedPageCount();
    if (unassignedCount == 0) {
        PageId newExtMetaPageId = INVALID_PAGE_ID;
        if (STORAGE_FUNC_FAIL(GetNewExtent(&newExtMetaPageId, segMetaPageBuf))) {
            UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);
            lockMgr->Unlock(&tag, DSTORE_EXCLUSIVE_LOCK);
            return INVALID_PAGE_ID;
        }
    }

    metaPage = static_cast<DataSegmentMetaPage *>(segMetaPageBuf->GetPage());
    PageId newPageId = {metaPage->addedPageId.m_fileId,
                        metaPage->addedPageId.m_blockId + 1};
    metaPage->addedPageId.m_blockId += 1;
    (void)m_bufMgr->MarkDirty(segMetaPageBuf);

    AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
    walWriterContext->BeginAtomicWal(thrd->GetCurrentXid());
    WalRecordTbsDataSegmentAssignDataPages walRecord;
    bool glsnChangedFlag = (metaPage->GetWalId() != walWriterContext->GetWalId());
    walRecord.SetHeader({WAL_TBS_SEG_META_ASSIGN_DATA_PAGES, sizeof(walRecord), segMetaPageBuf->GetPageId(),
                         metaPage->GetWalId(), metaPage->GetPlsn(), metaPage->GetGlsn(), glsnChangedFlag,
                         segMetaPageBuf->GetFileVersion()});
    walRecord.SetDataSegmentMeta(metaPage->addedPageId);
    walWriterContext->RememberPageNeedWal(segMetaPageBuf);
    walWriterContext->PutNewWalRecord(&walRecord);
    (void)walWriterContext->EndAtomicWal();
    UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);
    lockMgr->Unlock(&tag, DSTORE_EXCLUSIVE_LOCK);

    return newPageId;
}

RetStatus TraverseFreeQueuePage(BtrQueuePage *recyclePage, dlist_head &pageIdsList)
{
    ReusablePageQueue *reusableQueue = recyclePage->GetQueue<ReusablePageQueue>();
    for (uint16 i = 0; i < reusableQueue->GetSize(); i++) {
        uint16 pos = static_cast<uint16>(reusableQueue->GetHead() + i) % reusableQueue->GetCapacity();
        ReusablePageSlot slot = reusableQueue->reusablePages[pos];
        if (slot.isUsed) {
            if (STORAGE_FUNC_FAIL(AppendToPageIdList(pageIdsList, slot.reusablePage.pageId))) {
                return DSTORE_FAIL;
            }
        }
    }
    return DSTORE_SUCC;
}

RetStatus IndexSegment::RecycleBtrRecyclePartition(BtrRecyclePartReclaimContext &coldPartitionContext, Xid createdXid)
{
    PageId partMetaPageId = coldPartitionContext.coldPartitionMetaPageIds;
    NodeId nodId = coldPartitionContext.coldPartitionNodeIds;
    StorageAssert(partMetaPageId.IsValid());
    StorageAssert(nodId != INVALID_NODE_ID);
    BtreeRecyclePartition coldPartition {this, partMetaPageId, createdXid, m_bufMgr};
    coldPartition.SetCreatedXidCheckingOn();
    if (STORAGE_FUNC_FAIL(coldPartition.AcquireRecyclePartitionMetaBuf(LW_SHARED))) {
        return DSTORE_FAIL;
    }
    bool locksucc = true;
    bool isColdPartitionValid = coldPartition.IsValid(locksucc);
    coldPartition.ReleaseRecyclePartitionMetaBuf(locksucc);

    if (!isColdPartitionValid) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
            ErrMsg("Reclaim partition(%hu, %u) failed. node(%u) segment(%hu, %u), partition is invalid",
            partMetaPageId.m_fileId, partMetaPageId.m_blockId, nodId, m_segmentId.m_fileId, m_segmentId.m_blockId));
        return DSTORE_FAIL;
    }

    if (STORAGE_FUNC_FAIL(m_btrRecyclePartition->AcquireRecyclePartitionMetaBuf(LW_SHARED))) {
        return DSTORE_FAIL;
    }
    /* Let current partition take over the cold partition's recycle queue and free queue */
    if (STORAGE_FUNC_FAIL(m_btrRecyclePartition->TakeOverColdRecyclePartition(&coldPartition))) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("Reclaim partition(%hu, %u) failed. node(%u) segment(%hu, %u)",
            partMetaPageId.m_fileId, partMetaPageId.m_blockId, nodId, m_segmentId.m_fileId, m_segmentId.m_blockId));
        m_btrRecyclePartition->ReleaseRecyclePartitionMetaBuf();
        return DSTORE_FAIL;
    }

    m_btrRecyclePartition->ReleaseRecyclePartitionMetaBuf();
    return DSTORE_SUCC;
}

RetStatus IndexSegment::GetColdBtreeRecyclePartition(BtrRecyclePartReclaimContext &coldPartitionContext, Xid createdXid)
{
    RetStatus ret = DSTORE_FAIL;
    coldPartitionContext.coldPartitionMetaPageIds = INVALID_PAGE_ID;
    coldPartitionContext.coldPartitionNodeIds = INVALID_NODE_ID;

    BtrRecycleRootMeta recycleRootMetaObj(this, m_bufMgr);
    if (STORAGE_FUNC_FAIL(recycleRootMetaObj.AcquireRecycleRootMetaBuf(LW_SHARED, createdXid))) {
        return ret;
    }
    if (unlikely(recycleRootMetaObj.recycleRootMetaBuf == INVALID_BUFFER_DESC)) {
        /* It is acceptable because the segment may be dropped and reused before this asychronous task executed. */
        ErrLog(DSTORE_LOG, MODULE_SEGMENT, ErrMsg("Failed to get cold partition. RecycleRoot(%hu, %u) is invalid",
            recycleRootMetaObj.recycleRootMeta.m_fileId, recycleRootMetaObj.recycleRootMeta.m_blockId));
        return ret;
    }
    BtrRecycleRootMetaPage *recycleRootMetaPage = recycleRootMetaObj.GetRecycleRootMetaPage();
    if (unlikely(!recycleRootMetaObj.GetRecycleRootMetaPage()->TestType(PageType::BTR_RECYCLE_ROOT_META_PAGE_TYPE))) {
        /* It is acceptable because the segment may be dropped and reused before this asychronous task executed. */
        ErrLog(DSTORE_LOG, MODULE_SEGMENT, ErrMsg("Failed to get cold partition. Invalid RecycleRoot(%hu, %u) type %u",
            recycleRootMetaObj.recycleRootMeta.m_fileId, recycleRootMetaObj.recycleRootMeta.m_blockId,
            static_cast<uint32>(recycleRootMetaObj.GetRecycleRootMetaPage()->GetType())));
        recycleRootMetaObj.ReleaseRecycleRootMetaBuf();
        return ret;
    }

    TimestampTz recycleFsmTimeInterval = static_cast<TimestampTz>(g_storageInstance->GetGuc()->recycleFsmTimeInterval);
    TimestampTz recycleTimeThreshold = static_cast<TimestampTz>(recycleFsmTimeInterval * STORAGE_SECS_PER_MIN);

    /* Get current member node list. MUST release it before return. */

    for (uint16 i = 0; i < MAX_BTR_RECYCLE_PARTITION; i++) {
        if (STORAGE_FUNC_FAIL(thrd->CheckforInterrupts())) {
            /* Cancel request sent  */
            break;
        }
        NodeId assigendNodeId = static_cast<NodeId>(i);
        PageId partitionMetaPageId = recycleRootMetaPage->GetRecyclePartitionMetaPageId(i);
        if (!partitionMetaPageId.IsValid() ||
            g_storageInstance->GetGuc()->selfNodeId % MAX_BTR_RECYCLE_PARTITION == assigendNodeId) {
            /* skip if the partition is invalid or belongs to self node */
            continue;
        }

        /* Case 1. Check if the partition is old enough for recycle */
        BufferDesc *partitionMetaBuf = m_bufMgr->Read(this->GetPdbId(), partitionMetaPageId, LW_SHARED);
        if (unlikely(partitionMetaBuf == INVALID_BUFFER_DESC)) {
            /* Something wrong with the page. No need to handle it here, just skip the task. */
            break;
        }

        BtrRecyclePartitionMetaPage *partitionMetaPage =
            static_cast<BtrRecyclePartitionMetaPage*>(partitionMetaBuf->GetPage());
        if (unlikely(!partitionMetaPage->TestType(PageType::BTR_RECYCLE_PARTITION_META_PAGE_TYPE)) ||
            unlikely(partitionMetaPage->createdXid != createdXid) ||
            unlikely(partitionMetaPage->accessTimestamp == 0L)) {
            /* It's possible that the IndexSegment is already reused. Just skip this task */
            m_bufMgr->UnlockAndRelease(partitionMetaBuf, BufferPoolUnlockContentFlag());
            break;
        }

        BtrRecyclePartitionMeta partitionMeta(this->GetPdbId(), partitionMetaPageId, partitionMetaBuf, false);
        bool expired = partitionMeta.IsPartitionExpired(recycleTimeThreshold);
        bool isEmpty = partitionMeta.IsEmpty();
        /* Unlock the shared lock of partition meta now. We'll use an exclusive lock later if recyclable */
        m_bufMgr->UnlockAndRelease(partitionMetaBuf, BufferPoolUnlockContentFlag());
        if (isEmpty) {
            /* Nothing to take over, go on checking next */
            continue;
        }
        if (expired) {
            coldPartitionContext.coldPartitionMetaPageIds = partitionMetaPageId;
            coldPartitionContext.coldPartitionNodeIds = assigendNodeId;
            ret = DSTORE_SUCC;
            break;
        }

        /* Case 2. Check if the assigned node of partition still exists */
        bool isPartitionUnderControl = false;
        if (!isPartitionUnderControl) {
            /* Recycle the partition that belongs to a dead node */
            coldPartitionContext.coldPartitionMetaPageIds = partitionMetaPageId;
            coldPartitionContext.coldPartitionNodeIds = assigendNodeId;
            /* We'll take over only one partition every time to avoid partitions concentrate on the same node */
            ret = DSTORE_SUCC;
            break;
        }
    }
    recycleRootMetaObj.ReleaseRecycleRootMetaBuf();
    return ret;
}

RetStatus IndexSegment::TryRecycleColdBtrRecyclePartition(Xid createdXid)
{
    if (unlikely(m_isDrop)) {
        storage_set_error(TBS_ERROR_SEGMENT_USE_AFTER_DROP);
        return DSTORE_FAIL;
    }
    if (unlikely(!m_isInitialized)) {
        storage_set_error(TBS_ERROR_SEGMENT_IS_NOT_INIT);
        return DSTORE_FAIL;
    }
    m_btrRecyclePartition->SetCreatedXidCheckingOn();

    /* Find a potential recyclable BtrRecyclePartition */
    BtrRecyclePartReclaimContext coldPartitionContext;
    
    if (STORAGE_FUNC_FAIL(GetColdBtreeRecyclePartition(coldPartitionContext, createdXid))) {
        return DSTORE_FAIL;
    }
    if (coldPartitionContext.coldPartitionMetaPageIds.IsInvalid() ||
        coldPartitionContext.coldPartitionNodeIds == INVALID_NODE_ID) {
        /* There's nothing to reclaim */
        return DSTORE_SUCC;
    }

    /* Do the real recycle */
    return RecycleBtrRecyclePartition(coldPartitionContext, createdXid);
}

RetStatus IndexSegment::TraverseFSMQueue(BufMgrInterface *bufMgr, PageId queueHeadPageId,
    dlist_head &pageIdsList, bool isFreeQueue, char **errInfo)
{
    StringInfoData dumpInfo;

    while (queueHeadPageId != INVALID_PAGE_ID) {
        if (STORAGE_FUNC_FAIL(AppendToPageIdList(pageIdsList, queueHeadPageId))) {
            if (unlikely(!dumpInfo.init())) {
                ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("cannot allocate memory for dump info."));
                return DSTORE_FAIL;
            }
            dumpInfo.append("Failed to record queue head pageId.");
            *errInfo = dumpInfo.data;
            return DSTORE_FAIL;
        }
        BufferDesc *recycleBuf = bufMgr->Read(this->GetPdbId(), queueHeadPageId, LW_SHARED);
        if (unlikely(recycleBuf == INVALID_BUFFER_DESC)) {
            if (unlikely(!dumpInfo.init())) {
                ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("cannot allocate memory for dump info."));
                return DSTORE_FAIL;
            }
            dumpInfo.append("Invalid FSM queue buffer");
            *errInfo = dumpInfo.data;
            return DSTORE_FAIL;
        }
        BtrQueuePage *recyclePage = static_cast<BtrQueuePage *>(recycleBuf->GetPage());

        if (isFreeQueue) {
            if (STORAGE_FUNC_FAIL(TraverseFreeQueuePage(recyclePage, pageIdsList))) {
                if (unlikely(!dumpInfo.init())) {
                    ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("cannot allocate memory for dump info."));
                    bufMgr->UnlockAndRelease(recycleBuf);
                    return DSTORE_FAIL;
                }
                dumpInfo.append("Failed to tranverse freeQueue.");
                *errInfo = dumpInfo.data;
                bufMgr->UnlockAndRelease(recycleBuf);
                return DSTORE_FAIL;
            }
        }

        queueHeadPageId = recyclePage->GetNext();
        bufMgr->UnlockAndRelease(recycleBuf);
    }

    return DSTORE_SUCC;
}

RetStatus IndexSegment::GetFsmPageIds(BufMgrInterface *bufMgr, PageId **pageIds, Size *length, char **errInfo)
{
    *errInfo = nullptr;
    dlist_head pageIdsList;
    DListInit(&pageIdsList);
    StringInfoData dumpInfo;

    BtrRecycleRootMeta recycleRootMetaObj(this, bufMgr);
    PageId recycleRootMetaPageId = recycleRootMetaObj.recycleRootMeta;
    if (unlikely(recycleRootMetaPageId == INVALID_PAGE_ID)) {
        *length = 0;    /* no output */
        if (unlikely(!dumpInfo.init())) {
            ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("cannot allocate memory for dump info."));
            return DSTORE_FAIL;
        }
        dumpInfo.append("Invalid recycle root meta pageId");
        *errInfo = dumpInfo.data;
        return DSTORE_FAIL;
    }

    if (STORAGE_FUNC_FAIL(AppendToPageIdList(pageIdsList, recycleRootMetaPageId))) {
        *length = 0;    /* no output */
        if (unlikely(!dumpInfo.init())) {
            ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("cannot allocate memory for dump info."));
            return DSTORE_FAIL;
        }
        dumpInfo.append("Failed to record root meta pageId.");
        *errInfo = dumpInfo.data;
        return DSTORE_FAIL;
    }
    BufferDesc *recycleRootMetaBuf = bufMgr->Read(this->GetPdbId(), recycleRootMetaPageId, LW_SHARED);
    if (unlikely(recycleRootMetaBuf == INVALID_BUFFER_DESC)) {
        *length = 0;    /* no output */
        if (unlikely(!dumpInfo.init())) {
            ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("cannot allocate memory for dump info."));
            return DSTORE_FAIL;
        }
        dumpInfo.append("Invalid recycle root meta buffer");
        *errInfo = dumpInfo.data;
        return DSTORE_FAIL;
    }

    BtrRecycleRootMetaPage *metaPage = static_cast<BtrRecycleRootMetaPage *>(recycleRootMetaBuf->GetPage());
    for (uint16 id = 0; id < MAX_BTR_RECYCLE_PARTITION; id++) {
        PageId recyclePartitionPageId = metaPage->GetRecyclePartitionMetaPageId(id);
        if (recyclePartitionPageId == INVALID_PAGE_ID) {
            continue;
        }
        if (STORAGE_FUNC_FAIL(AppendToPageIdList(pageIdsList, recyclePartitionPageId))) {
            bufMgr->UnlockAndRelease(recycleRootMetaBuf);
            *length = 0; /* no output */
            if (unlikely(!dumpInfo.init())) {
                ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("cannot allocate memory for dump info."));
                return DSTORE_FAIL;
            }
            dumpInfo.append("Failed to record recycle partition PageId.");
            *errInfo = dumpInfo.data;
            return DSTORE_FAIL;
        }
        BufferDesc *recyclePartitionMetaBuf = bufMgr->Read(this->GetPdbId(), recyclePartitionPageId, LW_SHARED);
        if (unlikely(recyclePartitionMetaBuf == INVALID_BUFFER_DESC)) {
            bufMgr->UnlockAndRelease(recycleRootMetaBuf);
            *length = 0;    /* no output */
            if (unlikely(!dumpInfo.init())) {
                ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("cannot allocate memory for dump info."));
                return DSTORE_FAIL;
            }
            dumpInfo.append("Invalid recycle partition meta buffer");
            *errInfo = dumpInfo.data;
            return DSTORE_FAIL;
        }

        BtrRecyclePartitionMetaPage *partitionMetaPage =
            static_cast<BtrRecyclePartitionMetaPage *>(recyclePartitionMetaBuf->GetPage());

        PageId currRecyclePageId = partitionMetaPage->GetRecycleQueueHead();
        if STORAGE_FUNC_FAIL(TraverseFSMQueue(bufMgr, currRecyclePageId, pageIdsList, false, errInfo)) {
            *length = 0;   /* no output */
            bufMgr->UnlockAndRelease(recyclePartitionMetaBuf);
            bufMgr->UnlockAndRelease(recycleRootMetaBuf);
            return DSTORE_FAIL;
        }

        PageId currFreePageId = partitionMetaPage->GetFreeQueueHead();
        if STORAGE_FUNC_FAIL(TraverseFSMQueue(bufMgr, currFreePageId, pageIdsList, true, errInfo)) {
            *length = 0;   /* no output */
            bufMgr->UnlockAndRelease(recyclePartitionMetaBuf);
            bufMgr->UnlockAndRelease(recycleRootMetaBuf);
            return DSTORE_FAIL;
        }

        bufMgr->UnlockAndRelease(recyclePartitionMetaBuf);
    }

    bufMgr->UnlockAndRelease(recycleRootMetaBuf);

    if (STORAGE_FUNC_FAIL(GetPagesFromPageIdList(&pageIdsList, pageIds, length, errInfo))) {
        return DSTORE_FAIL;
    }

    return DSTORE_SUCC;
}

RetStatus IndexSegment::InitBtrRecyclePartition()
{
    NodeId nodeId = g_storageInstance->GetGuc()->selfNodeId;
    BtrRecycleRootMeta recycleRootMeta(this, m_bufMgr);
    PageId recyclePartMetaPageId;
    Xid createdXid = INVALID_XID;
    if (STORAGE_FUNC_FAIL(recycleRootMeta.GetRecyclePartitionMetaPageId(nodeId, recyclePartMetaPageId, createdXid))) {
        return DSTORE_FAIL;
    }
    StorageAssert(createdXid != INVALID_XID);

    if (!recyclePartMetaPageId.IsValid() &&
        STORAGE_FUNC_FAIL(recycleRootMeta.InitRecyclePartitionMeta(nodeId, recyclePartMetaPageId))) {
        return DSTORE_FAIL;
    }

    m_btrRecyclePartition = DstoreNew(m_ctx) BtreeRecyclePartition(this, recyclePartMetaPageId, createdXid, m_bufMgr);
    if (m_btrRecyclePartition == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
            ErrMsg("Index data segment (%hu, %u) failed to initialize BtreeRecyclePartition.",
                GetSegmentMetaPageId().m_fileId, GetSegmentMetaPageId().m_blockId));
        return DSTORE_FAIL;
    }

    if (STORAGE_FUNC_FAIL(m_btrRecyclePartition->AcquireRecyclePartitionMetaBuf(LW_SHARED))) {
        DstorePfreeExt(m_btrRecyclePartition);
        return DSTORE_FAIL;
    }
    bool locksucc = true;
    bool isRecyclePartitionValid = m_btrRecyclePartition->IsValid(locksucc);
    m_btrRecyclePartition->ReleaseRecyclePartitionMetaBuf(locksucc);

    if (!isRecyclePartitionValid && STORAGE_FUNC_FAIL(m_btrRecyclePartition->Init())) {
        DstorePfreeExt(m_btrRecyclePartition);
        return DSTORE_FAIL;
    }
    /* Only check createdXid for background tasks. */
    m_btrRecyclePartition->SetCreatedXidCheckingOff();

    return DSTORE_SUCC;
}

RetStatus IndexSegment::PutIntoRecycleQueue(const RecyclablePage recyclablePage)
{
    if (unlikely(m_btrRecyclePartition == nullptr) && STORAGE_FUNC_FAIL(InitBtrRecyclePartition())) {
        return DSTORE_FAIL;
    }
#ifdef DSTORE_COLLECT_INDEX_RECYCLE_INFO
    RecordPageRecycleOper(BTR_PUT_INTO_PENDING_QUEUE);
#endif
    return m_btrRecyclePartition->RecycleListPush(recyclablePage);
}

RetStatus IndexSegment::PutIntoFreeQueue(const PageId pageId)
{
    if (unlikely(m_btrRecyclePartition == nullptr) && STORAGE_FUNC_FAIL(InitBtrRecyclePartition())) {
        return DSTORE_FAIL;
    }
#ifdef DSTORE_COLLECT_INDEX_RECYCLE_INFO
    RecordPageRecycleOper(BTR_PUT_INTO_FREE_QUEUE);
#endif
    return m_btrRecyclePartition->FreeListPush(pageId);
}

RetStatus IndexSegment::GetFromRecycleQueue(PageId &recyclablePageId, CommitSeqNo minCsn, uint64 *numSkippedPage)
{
    if (unlikely(m_btrRecyclePartition == nullptr) && STORAGE_FUNC_FAIL(InitBtrRecyclePartition())) {
        return DSTORE_FAIL;
    }
    return m_btrRecyclePartition->RecycleListPop(recyclablePageId, minCsn, numSkippedPage);
}

PageId IndexSegment::GetFromFreeQueue()
{
    if (unlikely(m_btrRecyclePartition == nullptr) && STORAGE_FUNC_FAIL(InitBtrRecyclePartition())) {
        return INVALID_PAGE_ID;
    }
    return m_btrRecyclePartition->AllocNewPage();
}

RetStatus IndexSegment::GetSlotFromFreeQueue(FreeQueueSlot &slot, const PageId emptyPage)
{
    if (unlikely(m_btrRecyclePartition == nullptr) && STORAGE_FUNC_FAIL(InitBtrRecyclePartition())) {
        return DSTORE_FAIL;
    }
    return m_btrRecyclePartition->AllocUnusedSlot(slot, emptyPage);
}

RetStatus IndexSegment::WriteSlotToFreeQueue(const FreeQueueSlot slot, const ReusablePage reusablePage)
{
    if (unlikely(m_btrRecyclePartition == nullptr) && STORAGE_FUNC_FAIL(InitBtrRecyclePartition())) {
        return DSTORE_FAIL;
    }
    return m_btrRecyclePartition->WriteSlot(slot, reusablePage);
}

#ifdef DSTORE_COLLECT_INDEX_RECYCLE_INFO
void IndexSegment::SetRecycleFailReason(BtrPageRecycleFailReason recycleFailReasonCode)
{
    StorageAssert(recycleFailReasonCode < MAX_BTR_RECYCLE_FAILED_REASON_NUM);
    m_btrRecyclePartition->recycleFailReasonCounter.indexRecycleFailReason[recycleFailReasonCode]++;
}

void IndexSegment::RecordPageRecycleOper(BtrPageRecycleOperType recyleOperType)
{
    StorageAssert(recyleOperType < MAX_BTR_RECYCLE_OPER_TYPE_NUM);
    m_btrRecyclePartition->queueOperCounter.indexFreeQueueOper[recyleOperType]++;
}
#endif
}
