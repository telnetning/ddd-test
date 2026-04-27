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
 * Description: Btree Recycle Wal unittest
 */
#include "ut_btree/ut_btree_wal.h"
#include "ut_utilities/ut_dstore_framework.h"
#include "ut_mock/ut_mock.h"
#include "ut_btree/ut_btree.h"
#include "index/dstore_btree_recycle_wal.h"
#include "ut_tablespace/ut_segment.h"

TEST_F(UTBtreeWal, RecyclePartitionInitPageWalTest_level0)
{
    /* Generate a recyclable tree */
    int numLeaves = 2;
    int rootLevel = 1;
    UTRecyclableBtree *recyclableBtree = GenerateRecyclableSubtree(numLeaves, rootLevel);

    BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
    IndexSegment *indexSegment = recyclableBtree->GetBtrStorageMgr()->GetSegment();

    /* Get the recycle root meta page */
    BtrRecycleRootMeta recycleRootMeta(indexSegment, bufMgr);
    PageId recyclePartitionMetaPage;
    Xid createdXid = INVALID_XID;
    recycleRootMeta.GetRecyclePartitionMetaPageId(1, recyclePartitionMetaPage, createdXid);

    /* Get the recycle partition meta page, then the queue head of the RECYCLE queue */
    BufferDesc *recyclePartitionMetaBuf = bufMgr->Read(g_defaultPdbId, recyclePartitionMetaPage, LW_SHARED);
    BtrRecyclePartitionMeta recyclePartitionMeta(g_defaultPdbId, recyclePartitionMetaPage, recyclePartitionMetaBuf, false);
    PageId qHead = recyclePartitionMeta.GetQueueHead(BtrRecycleQueueType::FREE);
    bufMgr->UnlockAndRelease(recyclePartitionMetaBuf);

    // Get a page from the FreeQueue to use as the tail.
    PageId tailPage = indexSegment->GetFromFreeQueue();

    /* Get the queue from the queue page and calculate the available space on the page */
    BufferDesc *qBuf = bufMgr->Read(g_defaultPdbId, qHead, LW_SHARED);
    BtrQueuePage *qPage = static_cast<BtrQueuePage *>(qBuf->GetPage());
    ReusablePageQueue *queue = qPage->GetQueue<ReusablePageQueue>();
    uint16 remainingPages = queue->GetCapacity() - queue->GetSize();
    bufMgr->UnlockAndRelease(qBuf);

    /* Fill up the queue head such that the next push will cause a new page to be added to the queue */
    for (uint16 i = 0; i < remainingPages; i++) {
        PageId reusablePage = {123, i};
        indexSegment->PutIntoFreeQueue(reusablePage);
    }

    (void)bufMgr->FlushAll(false);
    CopyDataFile();

    /* Get endPlsn of WalFile */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);

    indexSegment->PutIntoFreeQueue(tailPage);

    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());
    /* read walFile to Get WalRecord */
    std::vector<WalRecordRedoInfo *> recordList;
    EXPECT_EQ(ReadRecordsAfterPlsn(plsn, WAL_BTREE_RECYCLE_PARTITION_INIT_PAGE, recordList), DSTORE_SUCC);
    EXPECT_EQ(recordList.size(), 1);

    /* Redo and check */
    BtrPage redoPage;
    BtrPage afterPage;
    WalRecordRedoContext redoCtx = {INVALID_XID, m_walWriter->GetWalId(), DSTORE::g_defaultPdbId};
    WalRecordRedoInfo *recordInfo = recordList.front();
    WalRecordBtrRecyclePartitionInitPage *record = static_cast<WalRecordBtrRecyclePartitionInitPage *>(&(recordInfo->walRecord));
    FetchPage(afterPage, record->m_pageId);
    ReadDataFileImage(record->m_pageId, (char *)&redoPage, BLCKSZ);
    RestorePage(redoPage, record->m_pageId);

    RedoBtrRecycleRecord(&redoCtx, record, recordInfo->endPlsn);
    FetchPage(redoPage, record->m_pageId);


    EXPECT_EQ(redoPage.m_header.m_type, afterPage.m_header.m_type);
}

TEST_F(UTBtreeWal, RecyclePartitionPushWalTest_level0)
{
    /* Generate a recyclable tree */
    int numLeaves = 2;
    int rootLevel = 1;
    UTRecyclableBtree *recyclableBtree = GenerateRecyclableSubtree(numLeaves, rootLevel);

    BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
    IndexSegment *indexSegment = recyclableBtree->GetBtrStorageMgr()->GetSegment();

    (void)bufMgr->FlushAll(false);
    CopyDataFile();

    /* Get endPlsn of WalFile */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);

    indexSegment->PutIntoRecycleQueue({INVALID_CSN, 123, 789});
    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());
    /* read walFile to Get WalRecord */
    std::vector<WalRecordRedoInfo *> recordList;
    EXPECT_EQ(ReadRecordsAfterPlsn(plsn, WAL_BTREE_RECYCLE_PARTITION_PUSH, recordList), DSTORE_SUCC);
    EXPECT_EQ(recordList.size(), 1);

    BtrPage redoPage;
    BtrPage afterPage;
    WalRecordRedoContext redoCtx = {INVALID_XID, m_walWriter->GetWalId(), DSTORE::g_defaultPdbId};
    WalRecordRedoInfo *recordInfo = recordList.front();
    WalRecordBtrRecyclePartitionPush *record = static_cast<WalRecordBtrRecyclePartitionPush *>(&(recordInfo->walRecord));
    FetchPage(afterPage, record->m_pageId);
    ReadDataFileImage(record->m_pageId, (char *)&redoPage, BLCKSZ);
    RestorePage(redoPage, record->m_pageId);

    RedoBtrRecycleRecord(&redoCtx, record, recordInfo->endPlsn);
    FetchPage(redoPage, record->m_pageId);
    RecyclablePageQueue *redoPageQueue = ((BtrQueuePage *)&redoPage)->GetQueue<RecyclablePageQueue>();
    RecyclablePageQueue *afterPageQueue = ((BtrQueuePage *)&afterPage)->GetQueue<RecyclablePageQueue>();
    EXPECT_EQ(redoPageQueue->GetSize(), afterPageQueue->GetSize());
    EXPECT_EQ(redoPageQueue->Pop(0), afterPageQueue->Pop(0));
}

TEST_F(UTBtreeWal, RecyclePartitionBatchPushWalTest_level0)
{
    /* Generate a recyclable tree */
    int numLeaves = 2;
    int rootLevel = 1;
    UTRecyclableBtree *recyclableBtree = GenerateRecyclableSubtree(numLeaves, rootLevel);

    BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
    IndexSegment *indexSegment = recyclableBtree->GetBtrStorageMgr()->GetSegment();

    /* Get the recycle root meta page */
    BtrRecycleRootMeta recycleRootMeta(indexSegment, bufMgr);
    PageId recyclePartitionMetaPage;
    Xid createdXid = INVALID_XID;
    recycleRootMeta.GetRecyclePartitionMetaPageId(1, recyclePartitionMetaPage, createdXid);

    BtreeRecyclePartition recyclePartition(indexSegment, recyclePartitionMetaPage, createdXid, bufMgr);

    /* Get the recycle partition meta page, then the queue head of the RECYCLE queue */
    BufferDesc *recyclePartitionMetaBuf = bufMgr->Read(g_defaultPdbId, recyclePartitionMetaPage, LW_SHARED);
    BtrRecyclePartitionMeta recyclePartitionMeta(g_defaultPdbId, recyclePartitionMetaPage, recyclePartitionMetaBuf, false);
    PageId qHead = recyclePartitionMeta.GetQueueHead(BtrRecycleQueueType::FREE);
    bufMgr->UnlockAndRelease(recyclePartitionMetaBuf);

    /* Get the queue from the queue page and calculate the available space on the page */
    BufferDesc *qBuf = bufMgr->Read(g_defaultPdbId, qHead, LW_SHARED);
    BtrQueuePage *qPage = static_cast<BtrQueuePage *>(qBuf->GetPage());
    ReusablePageQueue *queue = qPage->GetQueue<ReusablePageQueue>();
    uint16 numPages = queue->GetSize();
    bufMgr->UnlockAndRelease(qBuf);

    /* We use the last page as the new tail page */
    PageId newPages[numPages];
    for (uint16 i = 0; i < numPages; i++) {
        newPages[i] = indexSegment->GetFromFreeQueue();
    }

    (void)bufMgr->FlushAll(false);
    CopyDataFile();

    /* Get endPlsn of WalFile */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);

    uint16 numNewBtrPages = numPages;
    recyclePartition.FreeListBatchPushNewPages(&newPages[1], static_cast<uint16>(numPages - 1), numNewBtrPages);
    recyclePartition.segment->CreateNewPages(newPages, numNewBtrPages, false);
    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());

    /* read walFile to Get WalRecord */
    std::vector<WalRecordRedoInfo *> recordList;
    EXPECT_EQ(ReadRecordsAfterPlsn(plsn, WAL_BTREE_RECYCLE_PARTITION_BATCH_PUSH, recordList), DSTORE_SUCC);
    EXPECT_EQ(recordList.size(), 1);

    BtrPage redoPage;
    BtrPage afterPage;
    WalRecordRedoContext redoCtx = {INVALID_XID, m_walWriter->GetWalId(), DSTORE::g_defaultPdbId};
    WalRecordRedoInfo *recordInfo = recordList.front();
    WalRecordBtrRecyclePartitionBatchPush *record = static_cast<WalRecordBtrRecyclePartitionBatchPush *>(&(recordInfo->walRecord));
    FetchPage(afterPage, record->m_pageId);
    ReadDataFileImage(record->m_pageId, (char *)&redoPage, BLCKSZ);
    RestorePage(redoPage, record->m_pageId);

    RedoBtrRecycleRecord(&redoCtx, record, recordInfo->endPlsn);
    FetchPage(redoPage, record->m_pageId);
    ReusablePageQueue *redoPageQueue = ((BtrQueuePage *)&redoPage)->GetQueue<ReusablePageQueue>();
    ReusablePageQueue *afterPageQueue = ((BtrQueuePage *)&afterPage)->GetQueue<ReusablePageQueue>();
    for (uint16 i = 0; i < record->numPages; i++) {
        uint16 pos = (record->tail + i) % queue->GetCapacity();
        EXPECT_EQ(redoPageQueue->reusablePages[pos], afterPageQueue->reusablePages[pos]);
    }
}

TEST_F(UTBtreeWal, RecyclePartitionBatchPushSetNextWalTest_level0)
{
    /* Generate a recyclable tree */
    int numLeaves = 2;
    int rootLevel = 1;
    UTRecyclableBtree *recyclableBtree = GenerateRecyclableSubtree(numLeaves, rootLevel);

    BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
    IndexSegment *indexSegment = recyclableBtree->GetBtrStorageMgr()->GetSegment();

    /* Get the recycle root meta page */
    BtrRecycleRootMeta recycleRootMeta(indexSegment, bufMgr);
    PageId recyclePartitionMetaPage;
    Xid createdXid = INVALID_XID;
    recycleRootMeta.GetRecyclePartitionMetaPageId(1, recyclePartitionMetaPage, createdXid);

    BtreeRecyclePartition recyclePartition(indexSegment, recyclePartitionMetaPage, createdXid, bufMgr);

    /* Get the recycle partition meta page, then the queue head of the RECYCLE queue */
    BufferDesc *recyclePartitionMetaBuf = bufMgr->Read(g_defaultPdbId, recyclePartitionMetaPage, LW_SHARED);
    BtrRecyclePartitionMeta recyclePartitionMeta(g_defaultPdbId, recyclePartitionMetaPage, recyclePartitionMetaBuf, false);
    PageId qHead = recyclePartitionMeta.GetQueueHead(BtrRecycleQueueType::FREE);
    bufMgr->UnlockAndRelease(recyclePartitionMetaBuf);

    /* Get the queue from the queue page and calculate the available space on the page */
    BufferDesc *qBuf = bufMgr->Read(g_defaultPdbId, qHead, LW_SHARED);
    BtrQueuePage *qPage = static_cast<BtrQueuePage *>(qBuf->GetPage());
    ReusablePageQueue *queue = qPage->GetQueue<ReusablePageQueue>();
    uint16 numPages = queue->GetCapacity() + 2;
    bufMgr->UnlockAndRelease(qBuf);

    /* We use the last page as the new tail page */
    PageId newPages[numPages];
    for (uint16 i = 0; i < numPages; i++) {
        newPages[i] = indexSegment->GetFromFreeQueue();
    }

    (void)bufMgr->FlushAll(false);
    CopyDataFile();

    /* Get endPlsn of WalFile */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);

    uint16 numNewBtrPages = numPages;
    recyclePartition.FreeListBatchPushNewPages(&newPages[1], static_cast<uint16>(numPages - 1), numNewBtrPages);
    recyclePartition.segment->CreateNewPages(newPages, numNewBtrPages, false);
    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());

    /* read walFile to Get WalRecord */
    std::vector<WalRecordRedoInfo *> recordList;
    EXPECT_EQ(ReadRecordsAfterPlsn(plsn, WAL_BTREE_RECYCLE_PARTITION_BATCH_PUSH, recordList), DSTORE_SUCC);
    EXPECT_EQ(recordList.size(), 2);

    BtrPage redoPage;
    BtrPage afterPage;
    WalRecordRedoContext redoCtx = {INVALID_XID, m_walWriter->GetWalId(), DSTORE::g_defaultPdbId};
    WalRecordRedoInfo *recordInfo = recordList.front();
    WalRecordBtrRecyclePartitionBatchPush *record = static_cast<WalRecordBtrRecyclePartitionBatchPush *>(&(recordInfo->walRecord));
    FetchPage(afterPage, record->m_pageId);
    ReadDataFileImage(record->m_pageId, (char *)&redoPage, BLCKSZ);
    RestorePage(redoPage, record->m_pageId);

    RedoBtrRecycleRecord(&redoCtx, record, recordInfo->endPlsn);
    FetchPage(redoPage, record->m_pageId);
    ReusablePageQueue *redoPageQueue = ((BtrQueuePage *)&redoPage)->GetQueue<ReusablePageQueue>();
    ReusablePageQueue *afterPageQueue = ((BtrQueuePage *)&afterPage)->GetQueue<ReusablePageQueue>();
    for (uint16 i = 0; i < record->numPages; i++) {
        uint16 pos = (record->tail + i) % queue->GetCapacity();
        EXPECT_EQ(redoPageQueue->reusablePages[pos], afterPageQueue->reusablePages[pos]);
    }
    EXPECT_EQ(((BtrQueuePage *)&redoPage)->GetNext(), ((BtrQueuePage *)&afterPage)->GetNext());

    // Replay second batch push record
    recordInfo = recordList.back();
    record = static_cast<WalRecordBtrRecyclePartitionBatchPush *>(&(recordInfo->walRecord));
    FetchPage(afterPage, record->m_pageId);
    ReadDataFileImage(record->m_pageId, (char *)&redoPage, BLCKSZ);
    RestorePage(redoPage, record->m_pageId);

    RedoBtrRecycleRecord(&redoCtx, record, recordInfo->endPlsn);
    FetchPage(redoPage, record->m_pageId);
    redoPageQueue = ((BtrQueuePage *)&redoPage)->GetQueue<ReusablePageQueue>();
    afterPageQueue = ((BtrQueuePage *)&afterPage)->GetQueue<ReusablePageQueue>();
    for (uint16 i = 0; i < record->numPages; i++) {
        uint16 pos = (record->tail + i) % queue->GetCapacity();
        EXPECT_EQ(redoPageQueue->reusablePages[pos], afterPageQueue->reusablePages[pos]);
    }
}

TEST_F(UTBtreeWal, RecyclePartitionBatchPushNeedResetWalTest_level0)
{
    /* Generate a recyclable tree */
    int numLeaves = 2;
    int rootLevel = 1;
    UTRecyclableBtree *recyclableBtree = GenerateRecyclableSubtree(numLeaves, rootLevel);

    BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
    IndexSegment *indexSegment = recyclableBtree->GetBtrStorageMgr()->GetSegment();

    /* Get the recycle root meta page */
    BtrRecycleRootMeta recycleRootMeta(indexSegment, bufMgr);
    PageId recyclePartitionMetaPage;
    Xid createdXid = INVALID_XID;
    recycleRootMeta.GetRecyclePartitionMetaPageId(1, recyclePartitionMetaPage, createdXid);

    BtreeRecyclePartition recyclePartition(indexSegment, recyclePartitionMetaPage, createdXid, bufMgr);

    /* Get the recycle partition meta page, then the queue head of the RECYCLE queue */
    BufferDesc *recyclePartitionMetaBuf = bufMgr->Read(g_defaultPdbId, recyclePartitionMetaPage, LW_SHARED);
    BtrRecyclePartitionMeta recyclePartitionMeta(g_defaultPdbId, recyclePartitionMetaPage, recyclePartitionMetaBuf, false);
    PageId qHead = recyclePartitionMeta.GetQueueHead(BtrRecycleQueueType::FREE);
    bufMgr->UnlockAndRelease(recyclePartitionMetaBuf);

    /* Get the queue from the queue page and calculate the available space on the page */
    BufferDesc *qBuf = bufMgr->Read(g_defaultPdbId, qHead, LW_SHARED);
    BtrQueuePage *qPage = static_cast<BtrQueuePage *>(qBuf->GetPage());
    ReusablePageQueue *queue = qPage->GetQueue<ReusablePageQueue>();
    uint16 numPages = queue->GetCapacity() + 2;
    bufMgr->UnlockAndRelease(qBuf);

    /* We use the last page as the new tail page */
    PageId newPages[numPages];
    for (uint16 i = 0; i < numPages; i++) {
        newPages[i] = indexSegment->GetFromFreeQueue();
    }

    (void)bufMgr->FlushAll(false);
    CopyDataFile();

    /* Get endPlsn of WalFile */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);

    uint16 numNewBtrPages = numPages;
    recyclePartition.FreeListBatchPushNewPages(&newPages[1], static_cast<uint16>(numPages - 1), numNewBtrPages);
    recyclePartition.segment->CreateNewPages(newPages, numNewBtrPages, false);
    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());

    /* read walFile to Get WalRecord */
    std::vector<WalRecordRedoInfo *> recordList;
    EXPECT_EQ(ReadRecordsAfterPlsn(plsn, WAL_BTREE_RECYCLE_PARTITION_BATCH_PUSH, recordList), DSTORE_SUCC);
    EXPECT_EQ(recordList.size(), 2);

    BtrPage redoPage;
    BtrPage afterPage;
    WalRecordRedoContext redoCtx = {INVALID_XID, m_walWriter->GetWalId(), DSTORE::g_defaultPdbId};
    WalRecordRedoInfo *recordInfo = recordList.front();
    WalRecordBtrRecyclePartitionBatchPush *record = static_cast<WalRecordBtrRecyclePartitionBatchPush *>(&(recordInfo->walRecord));
    FetchPage(afterPage, record->m_pageId);
    ReadDataFileImage(record->m_pageId, (char *)&redoPage, BLCKSZ);
    RestorePage(redoPage, record->m_pageId);

    RedoBtrRecycleRecord(&redoCtx, record, recordInfo->endPlsn);
    FetchPage(redoPage, record->m_pageId);
    ReusablePageQueue *redoPageQueue = ((BtrQueuePage *)&redoPage)->GetQueue<ReusablePageQueue>();
    ReusablePageQueue *afterPageQueue = ((BtrQueuePage *)&afterPage)->GetQueue<ReusablePageQueue>();
    for (uint16 i = 0; i < record->numPages; i++) {
        uint16 pos = (record->tail + i) % queue->GetCapacity();
        EXPECT_EQ(redoPageQueue->reusablePages[pos], afterPageQueue->reusablePages[pos]);
    }
    EXPECT_EQ(((BtrQueuePage *)&redoPage)->GetNext(), ((BtrQueuePage *)&afterPage)->GetNext());

    // Replay second batch push record
    recordInfo = recordList.back();
    record = static_cast<WalRecordBtrRecyclePartitionBatchPush *>(&(recordInfo->walRecord));
    FetchPage(afterPage, record->m_pageId);
    ReadDataFileImage(record->m_pageId, (char *)&redoPage, BLCKSZ);
    RestorePage(redoPage, record->m_pageId);

    /* add dummy data, need to be reset when Redo DTS2025080419100 */
    BufferDesc *tailBuf = bufMgr->Read(g_defaultPdbId, record->m_pageId, LW_EXCLUSIVE);
    BtrQueuePage *tailPage = static_cast<BtrQueuePage *>(tailBuf->GetPage());
    tailPage->Reset<ReusablePageQueue>(record->m_pageId, createdXid);
    tailPage->SetNext(INVALID_PAGE_ID);
    bufMgr->UnlockAndRelease(tailBuf);
    PageId dummyPageId = {123, 456};
    indexSegment->PutIntoFreeQueue(dummyPageId);

    RedoBtrRecycleRecord(&redoCtx, record, recordInfo->endPlsn);
    FetchPage(redoPage, record->m_pageId);
    redoPageQueue = ((BtrQueuePage *)&redoPage)->GetQueue<ReusablePageQueue>();
    afterPageQueue = ((BtrQueuePage *)&afterPage)->GetQueue<ReusablePageQueue>();
    for (uint16 i = 0; i < record->numPages; i++) {
        uint16 pos = (record->tail + i) % queue->GetCapacity();
        EXPECT_EQ(redoPageQueue->reusablePages[pos], afterPageQueue->reusablePages[pos]);
    }
}

TEST_F(UTBtreeWal, RecyclePartitionBatchPushInitPagetWalTest_level0)
{
    /* Generate a recyclable tree */
    int numLeaves = 2;
    int rootLevel = 1;
    UTRecyclableBtree *recyclableBtree = GenerateRecyclableSubtree(numLeaves, rootLevel);

    BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
    IndexSegment *indexSegment = recyclableBtree->GetBtrStorageMgr()->GetSegment();

    /* Get the recycle root meta page */
    BtrRecycleRootMeta recycleRootMeta(indexSegment, bufMgr);
    PageId recyclePartitionMetaPage;
    Xid createdXid = INVALID_XID;
    recycleRootMeta.GetRecyclePartitionMetaPageId(1, recyclePartitionMetaPage, createdXid);

    BtreeRecyclePartition recyclePartition(indexSegment, recyclePartitionMetaPage, createdXid, bufMgr);

    /* Get the recycle partition meta page, then the queue head of the RECYCLE queue */
    BufferDesc *recyclePartitionMetaBuf = bufMgr->Read(g_defaultPdbId, recyclePartitionMetaPage, LW_SHARED);
    BtrRecyclePartitionMeta recyclePartitionMeta(g_defaultPdbId, recyclePartitionMetaPage, recyclePartitionMetaBuf, false);
    PageId qHead = recyclePartitionMeta.GetQueueHead(BtrRecycleQueueType::FREE);
    bufMgr->UnlockAndRelease(recyclePartitionMetaBuf);

    /* Get the queue from the queue page and calculate the available space on the page */
    BufferDesc *qBuf = bufMgr->Read(g_defaultPdbId, qHead, LW_SHARED);
    BtrQueuePage *qPage = static_cast<BtrQueuePage *>(qBuf->GetPage());
    ReusablePageQueue *queue = qPage->GetQueue<ReusablePageQueue>();
    uint16 numPages = queue->GetSize();
    bufMgr->UnlockAndRelease(qBuf);

    /* We use the last page as the new tail page */
    PageId newPages[numPages];
    for (uint16 i = 0; i < numPages; i++) {
        newPages[i] = indexSegment->GetFromFreeQueue();
    }

    // Fill in the page but leave room for numPages - 1 pages.
    // The additional page is used to return to the caller of FreeListBatchPushNewPages.
    for (uint16 i = 0; i < queue->GetCapacity() - (numPages - 2); i++) {
        PageId dummyPageId = {123, i};
        indexSegment->PutIntoFreeQueue(dummyPageId);
    }

    (void)bufMgr->FlushAll(false);
    CopyDataFile();

    /* Get endPlsn of WalFile */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);

    uint16 numNewBtrPages = numPages;
    recyclePartition.FreeListBatchPushNewPages(&newPages[1], static_cast<uint16>(numPages - 1), numNewBtrPages);
    recyclePartition.segment->CreateNewPages(newPages, numNewBtrPages, false);
    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());

    /* read walFile to Get WalRecord */
    std::vector<WalRecordRedoInfo *> recordList;
    EXPECT_EQ(ReadRecordsAfterPlsn(plsn, WAL_BTREE_RECYCLE_PARTITION_BATCH_PUSH, recordList), DSTORE_SUCC);
    EXPECT_EQ(recordList.size(), 1);
    EXPECT_EQ(ReadRecordsAfterPlsn(plsn, WAL_BTREE_RECYCLE_PARTITION_INIT_PAGE, recordList), DSTORE_SUCC);
    EXPECT_EQ(recordList.size(), 2);

    BtrPage redoPage;
    BtrPage afterPage;
    WalRecordRedoContext redoCtx = {INVALID_XID, m_walWriter->GetWalId(), DSTORE::g_defaultPdbId};
    WalRecordRedoInfo *recordInfo = recordList.front();
    WalRecordBtrRecyclePartitionBatchPush *record = static_cast<WalRecordBtrRecyclePartitionBatchPush *>(&(recordInfo->walRecord));
    FetchPage(afterPage, record->m_pageId);
    ReadDataFileImage(record->m_pageId, (char *)&redoPage, BLCKSZ);
    RestorePage(redoPage, record->m_pageId);

    RedoBtrRecycleRecord(&redoCtx, record, recordInfo->endPlsn);
    FetchPage(redoPage, record->m_pageId);
    ReusablePageQueue *redoPageQueue = ((BtrQueuePage *)&redoPage)->GetQueue<ReusablePageQueue>();
    ReusablePageQueue *afterPageQueue = ((BtrQueuePage *)&afterPage)->GetQueue<ReusablePageQueue>();
    for (uint16 i = 0; i < record->numPages; i++) {
        uint16 pos = (record->tail + i) % queue->GetCapacity();
        EXPECT_EQ(redoPageQueue->reusablePages[pos], afterPageQueue->reusablePages[pos]);
    }
    EXPECT_EQ(((BtrQueuePage *)&redoPage)->GetNext(), ((BtrQueuePage *)&afterPage)->GetNext());

    // Replay second batch push record
    recordInfo = recordList.back();
    record = static_cast<WalRecordBtrRecyclePartitionBatchPush *>(&(recordInfo->walRecord));
    FetchPage(afterPage, record->m_pageId);
    ReadDataFileImage(record->m_pageId, (char *)&redoPage, BLCKSZ);
    RestorePage(redoPage, record->m_pageId);

    RedoBtrRecycleRecord(&redoCtx, record, recordInfo->endPlsn);
    FetchPage(redoPage, record->m_pageId);
    redoPageQueue = ((BtrQueuePage *)&redoPage)->GetQueue<ReusablePageQueue>();
    afterPageQueue = ((BtrQueuePage *)&afterPage)->GetQueue<ReusablePageQueue>();
    EXPECT_EQ(redoPageQueue->GetSize(), 0);
    EXPECT_EQ(afterPageQueue->GetSize(), 0);
}

TEST_F(UTBtreeWal, RecyclePartitionPopWalTest_level0)
{
    /* Generate a recyclable tree */
    int numLeaves = 2;
    int rootLevel = 1;
    UTRecyclableBtree *recyclableBtree = GenerateRecyclableSubtree(numLeaves, rootLevel);

    BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
    IndexSegment *indexSegment = recyclableBtree->GetBtrStorageMgr()->GetSegment();

    (void)bufMgr->FlushAll(false);
    CopyDataFile();

    /* Get endPlsn of WalFile */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);

    PageId freePage = indexSegment->GetFromFreeQueue();
    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());
    /* read walFile to Get WalRecord */
    std::vector<WalRecordRedoInfo *> recordList;
    EXPECT_EQ(ReadRecordsAfterPlsn(plsn, WAL_BTREE_RECYCLE_PARTITION_POP, recordList), DSTORE_SUCC);
    EXPECT_EQ(recordList.size(), 1);

    BtrPage redoPage;
    BtrPage afterPage;
    WalRecordRedoContext redoCtx = {INVALID_XID, m_walWriter->GetWalId(), DSTORE::g_defaultPdbId};
    WalRecordRedoInfo *recordInfo = recordList.front();
    WalRecordBtrRecyclePartitionPop *record = static_cast<WalRecordBtrRecyclePartitionPop *>(&(recordInfo->walRecord));
    FetchPage(afterPage, record->m_pageId);
    ReadDataFileImage(record->m_pageId, (char *)&redoPage, BLCKSZ);
    RestorePage(redoPage, record->m_pageId);

    RedoBtrRecycleRecord(&redoCtx, record, recordInfo->endPlsn);
    FetchPage(redoPage, record->m_pageId);
    ReusablePageQueue *redoPageQueue = ((BtrQueuePage *)&redoPage)->GetQueue<ReusablePageQueue>();
    ReusablePageQueue *afterPageQueue = ((BtrQueuePage *)&afterPage)->GetQueue<ReusablePageQueue>();
    EXPECT_EQ(redoPageQueue->reusablePages[record->head], afterPageQueue->reusablePages[record->head]);
    EXPECT_EQ(redoPageQueue->GetSize(), afterPageQueue->GetSize());
}

TEST_F(UTBtreeWal, RecyclePartitionAllocSlotWalTest_level0)
{
    /* Generate a recyclable tree */
    int numLeaves = 2;
    int rootLevel = 1;
    UTRecyclableBtree *recyclableBtree = GenerateRecyclableSubtree(numLeaves, rootLevel);

    BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
    IndexSegment *indexSegment = recyclableBtree->GetBtrStorageMgr()->GetSegment();

    (void)bufMgr->FlushAll(false);
    CopyDataFile();

    /* Get endPlsn of WalFile */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);

    PageId reusablePageId = {1, 1};
    FreeQueueSlot slot;
    indexSegment->GetSlotFromFreeQueue(slot, reusablePageId);
    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());
    /* read walFile to Get WalRecord */
    std::vector<WalRecordRedoInfo *> recordList;
    EXPECT_EQ(ReadRecordsAfterPlsn(plsn, WAL_BTREE_RECYCLE_PARTITION_ALLOC_SLOT, recordList), DSTORE_SUCC);
    EXPECT_EQ(recordList.size(), 1);

    BtrPage redoPage;
    BtrPage afterPage;
    WalRecordRedoContext redoCtx = {INVALID_XID, m_walWriter->GetWalId(), DSTORE::g_defaultPdbId};
    WalRecordRedoInfo *recordInfo = recordList.front();
    WalRecordBtrRecyclePartitionAllocSlot *record = static_cast<WalRecordBtrRecyclePartitionAllocSlot *>(&(recordInfo->walRecord));
    FetchPage(afterPage, record->m_pageId);
    ReadDataFileImage(record->m_pageId, (char *)&redoPage, BLCKSZ);
    RestorePage(redoPage, record->m_pageId);

    RedoBtrRecycleRecord(&redoCtx, record, recordInfo->endPlsn);
    FetchPage(redoPage, record->m_pageId);
    ReusablePageQueue *redoPageQueue = ((BtrQueuePage *)&redoPage)->GetQueue<ReusablePageQueue>();
    ReusablePageQueue *afterPageQueue = ((BtrQueuePage *)&afterPage)->GetQueue<ReusablePageQueue>();
    EXPECT_EQ(redoPageQueue->reusablePages[slot.pos].isUsed, 0);
    EXPECT_EQ(afterPageQueue->reusablePages[slot.pos].isUsed, 0);
    EXPECT_EQ(redoPageQueue->reusablePages[slot.pos].reusablePage.pageId, reusablePageId);
    EXPECT_EQ(afterPageQueue->reusablePages[slot.pos].reusablePage.pageId, reusablePageId);
}

TEST_F(UTBtreeWal, RecyclePartitionWriteSlotWalTest_level0)
{
    /* Generate a recyclable tree */
    int numLeaves = 2;
    int rootLevel = 1;
    UTRecyclableBtree *recyclableBtree = GenerateRecyclableSubtree(numLeaves, rootLevel);

    BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
    IndexSegment *indexSegment = recyclableBtree->GetBtrStorageMgr()->GetSegment();

    (void)bufMgr->FlushAll(false);
    CopyDataFile();

    /* Get endPlsn of WalFile */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);

    PageId reusablePageId = {1, 1};
    FreeQueueSlot slot;
    indexSegment->GetSlotFromFreeQueue(slot, reusablePageId);
    ReusablePage reusablePage = (ReusablePage) {1, reusablePageId};
    indexSegment->WriteSlotToFreeQueue(slot, reusablePage);
    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());
    /* read walFile to Get WalRecord */
    std::vector<WalRecordRedoInfo *> allocRecordList;
    std::vector<WalRecordRedoInfo *> writeRecordList;
    EXPECT_EQ(ReadRecordsAfterPlsn(plsn, WAL_BTREE_RECYCLE_PARTITION_ALLOC_SLOT, allocRecordList), DSTORE_SUCC);
    EXPECT_EQ(ReadRecordsAfterPlsn(plsn, WAL_BTREE_RECYCLE_PARTITION_WRITE_SLOT, writeRecordList), DSTORE_SUCC);
    EXPECT_EQ(allocRecordList.size(), 1);
    EXPECT_EQ(writeRecordList.size(), 1);

    BtrPage redoPage;
    BtrPage afterPage;
    WalRecordRedoContext redoCtx = {INVALID_XID, m_walWriter->GetWalId(), DSTORE::g_defaultPdbId};
    WalRecordRedoInfo *allocRecordInfo = allocRecordList.front();
    WalRecordRedoInfo *writeRecordInfo = writeRecordList.front();
    WalRecordBtrRecycle *allocRecord = static_cast<WalRecordBtrRecycle *>(&(allocRecordInfo->walRecord));
    WalRecordBtrRecycle *writeRecord = static_cast<WalRecordBtrRecycle *>(&(writeRecordInfo->walRecord));
    FetchPage(afterPage, allocRecord->m_pageId);
    ReadDataFileImage(allocRecord->m_pageId, (char *)&redoPage, BLCKSZ);
    RestorePage(redoPage, allocRecord->m_pageId);

    RedoBtrRecycleRecord(&redoCtx, allocRecord, allocRecordInfo->endPlsn);
    RedoBtrRecycleRecord(&redoCtx, writeRecord, writeRecordInfo->endPlsn);
    FetchPage(redoPage, allocRecord->m_pageId);
    ReusablePageQueue *redoPageQueue = ((BtrQueuePage *)&redoPage)->GetQueue<ReusablePageQueue>();
    ReusablePageQueue *afterPageQueue = ((BtrQueuePage *)&afterPage)->GetQueue<ReusablePageQueue>();
    EXPECT_EQ(redoPageQueue->reusablePages[slot.pos].isUsed, 1);
    EXPECT_EQ(afterPageQueue->reusablePages[slot.pos].isUsed, 1);
    EXPECT_EQ(redoPageQueue->reusablePages[slot.pos].reusablePage, reusablePage);
    EXPECT_EQ(afterPageQueue->reusablePages[slot.pos].reusablePage, reusablePage);
}

TEST_F(UTBtreeWal, BtrQueuePageMetaSetNextWalTest_level0)
{
    /* Generate a recyclable tree */
    int numLeaves = 2;
    int rootLevel = 1;
    UTRecyclableBtree *recyclableBtree = GenerateRecyclableSubtree(numLeaves, rootLevel);

    BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
    IndexSegment *indexSegment = recyclableBtree->GetBtrStorageMgr()->GetSegment();

    /* Get the recycle root meta page */
    BtrRecycleRootMeta recycleRootMeta(indexSegment, bufMgr);
    PageId recyclePartitionMetaPage;
    Xid createdXid = INVALID_XID;
    recycleRootMeta.GetRecyclePartitionMetaPageId(1, recyclePartitionMetaPage, createdXid);

    /* Get the recycle partition meta page, then the queue head of the RECYCLE queue */
    BufferDesc *recyclePartitionMetaBuf = bufMgr->Read(g_defaultPdbId, recyclePartitionMetaPage, LW_SHARED);
    BtrRecyclePartitionMeta recyclePartitionMeta(g_defaultPdbId, recyclePartitionMetaPage, recyclePartitionMetaBuf, false);
    PageId qHead = recyclePartitionMeta.GetQueueHead(BtrRecycleQueueType::RECYCLE);
    bufMgr->UnlockAndRelease(recyclePartitionMetaBuf);

    /* Get the queue from the queue page and calculate the available space on the page */
    BufferDesc *qBuf = bufMgr->Read(g_defaultPdbId, qHead, LW_SHARED);
    BtrQueuePage *qPage = static_cast<BtrQueuePage *>(qBuf->GetPage());
    RecyclablePageQueue *queue = qPage->GetQueue<RecyclablePageQueue>();
    int remainingPages = queue->GetCapacity() - queue->GetSize();
    bufMgr->UnlockAndRelease(qBuf);

    /* Fill up the queue head such that the next push will cause a new page to be added to the queue */
    for (uint32 i = 0; i < remainingPages; i++) {
        RecyclablePage recyclablePage = {INVALID_CSN, 123, i};
        indexSegment->PutIntoRecycleQueue(recyclablePage);
    }

    (void)bufMgr->FlushAll(false);
    CopyDataFile();

    /* Get endPlsn of WalFile */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);

    indexSegment->PutIntoRecycleQueue({INVALID_CSN, 123, 789});

    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());
    /* read walFile to Get WalRecord */
    std::vector<WalRecordRedoInfo *> recordList;
    EXPECT_EQ(ReadRecordsAfterPlsn(plsn, WAL_BTREE_RECYCLE_QUEUE_PAGE_META_SET_NEXT, recordList), DSTORE_SUCC);
    EXPECT_EQ(recordList.size(), 1);

    /* Redo and check */
    BtrPage redoPage;
    BtrPage afterPage;
    WalRecordRedoContext redoCtx = {INVALID_XID, m_walWriter->GetWalId(), DSTORE::g_defaultPdbId};
    WalRecordRedoInfo *recordInfo = recordList.front();
    WalRecordBtrRecycle *record = static_cast<WalRecordBtrRecycle *>(&(recordInfo->walRecord));
    FetchPage(afterPage, record->m_pageId);
    ReadDataFileImage(record->m_pageId, (char *)&redoPage, BLCKSZ);
    RestorePage(redoPage, record->m_pageId);

    RedoBtrRecycleRecord(&redoCtx, record, recordInfo->endPlsn);
    FetchPage(redoPage, record->m_pageId);

    BtrQueuePage *testQPage = (BtrQueuePage *)&redoPage;
    BtrQueuePage *afterQPage = (BtrQueuePage *)&afterPage;
    EXPECT_EQ(testQPage->GetNext(), afterQPage->GetNext());
}

TEST_F(UTBtreeWal, RecyclePartitionMetaInitWalTest_level0)
{
    Transaction *trans = thrd->GetActiveTransaction();
    trans->Start();
    /* Generate TableSpace */
    BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
    TableSpaceInterface *m_testTbs = UtMockModule::UtGetTableSpace(2, bufMgr);
    IndexNormalSegment *indexSegment = (IndexNormalSegment*)SegmentTest::UTAllocSegment(g_defaultPdbId,
        m_testTbs->GetTablespaceId(), bufMgr, SegmentType::INDEX_SEGMENT_TYPE);
    trans->Commit();
    (void)bufMgr->FlushAll(false);

    /* Copy dataFile as Image */
    CopyDataFile();
    /* Get endPlsn of WalFile */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);

    /* Get a free page to initialize the btree recycle partition. */
    EXPECT_NE(indexSegment->GetFromFreeQueue(), INVALID_PAGE_ID);
    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());
    /* read walFile to Get WalRecord */
    std::vector<WalRecordRedoInfo *> recordList;
    EXPECT_EQ(ReadRecordsAfterPlsn(plsn, WAL_BTREE_RECYCLE_PARTITION_META_INIT, recordList), DSTORE_SUCC);
    EXPECT_EQ(recordList.size(), 1);

    /* Redo and check */
    BtrPage redoPage;
    BtrPage afterPage;
    WalRecordRedoContext redoCtx = {INVALID_XID, m_walWriter->GetWalId(), DSTORE::g_defaultPdbId};
    WalRecordRedoInfo *recordInfo = recordList.front();
    WalRecordBtrRecycle *record = static_cast<WalRecordBtrRecycle *>(&(recordInfo->walRecord));
    FetchPage(afterPage, record->m_pageId);
    ReadDataFileImage(record->m_pageId, (char *)&redoPage, BLCKSZ);
    RestorePage(redoPage, record->m_pageId);
    RedoBtrRecycleRecord(&redoCtx, record, recordInfo->endPlsn);
    FetchPage(redoPage, record->m_pageId);

    BtrRecyclePartitionMetaPage *testPartitionMetaPage = (BtrRecyclePartitionMetaPage *)(&redoPage);
    BtrRecyclePartitionMetaPage *afterPartitionMetaPage = (BtrRecyclePartitionMetaPage *)(&afterPage);
    EXPECT_TRUE(testPartitionMetaPage->TestType(PageType::BTR_RECYCLE_PARTITION_META_PAGE_TYPE));
    EXPECT_TRUE(afterPartitionMetaPage->TestType(PageType::BTR_RECYCLE_PARTITION_META_PAGE_TYPE));
}

TEST_F(UTBtreeWal, RecyclePartitionMetaSetHeadWalTest_level0)
{
    Transaction *trans = thrd->GetActiveTransaction();
    trans->Start();
    /* Generate TableSpace */
    BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
    TableSpaceInterface *m_testTbs = UtMockModule::UtGetTableSpace(2, bufMgr);
    IndexNormalSegment *indexSegment = (IndexNormalSegment*)SegmentTest::UTAllocSegment(g_defaultPdbId,
        m_testTbs->GetTablespaceId(), bufMgr, SegmentType::INDEX_SEGMENT_TYPE);
    trans->Commit();
    (void)bufMgr->FlushAll(false);

    /* Copy dataFile as Image */
    CopyDataFile();
    /* Get endPlsn of WalFile */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);

    /* Get a free page to initialize the btree recycle partition. */
    EXPECT_NE(indexSegment->GetFromFreeQueue(), INVALID_PAGE_ID);
    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());
    /* read walFile to Get WalRecord */
    std::vector<WalRecordRedoInfo *> recordList;
    EXPECT_EQ(ReadRecordsAfterPlsn(plsn, WAL_BTREE_RECYCLE_PARTITION_META_SET_HEAD, recordList), DSTORE_SUCC);
    EXPECT_EQ(recordList.size(), 2);

    /* Redo and check */
    BtrPage redoPage;
    BtrPage afterPage;
    WalRecordRedoContext redoCtx = {INVALID_XID, m_walWriter->GetWalId(), DSTORE::g_defaultPdbId};
    WalRecordRedoInfo *recordInfo = recordList.front();
    WalRecordRedoInfo *recordInfo2 = recordList.back();
    WalRecordBtrRecycle *record = static_cast<WalRecordBtrRecycle *>(&(recordInfo->walRecord));
    WalRecordBtrRecycle *record2 = static_cast<WalRecordBtrRecycle *>(&(recordInfo2->walRecord));
    FetchPage(afterPage, record->m_pageId);
    ReadDataFileImage(record->m_pageId, (char *)&redoPage, BLCKSZ);
    RestorePage(redoPage, record->m_pageId);
    RedoBtrRecycleRecord(&redoCtx, record, recordInfo->endPlsn);
    RedoBtrRecycleRecord(&redoCtx, record2, recordInfo2->endPlsn);
    FetchPage(redoPage, record->m_pageId);

    BtrRecyclePartitionMetaPage *testPartitionMetaPage = (BtrRecyclePartitionMetaPage *)(&redoPage);
    BtrRecyclePartitionMetaPage *afterPartitionMetaPage = (BtrRecyclePartitionMetaPage *)(&afterPage);
    EXPECT_EQ(testPartitionMetaPage->GetRecycleQueueHead(), afterPartitionMetaPage->GetRecycleQueueHead());
    EXPECT_EQ(testPartitionMetaPage->GetFreeQueueHead(), afterPartitionMetaPage->GetFreeQueueHead());
}

TEST_F(UTBtreeWal, RecycleRootMetaInitWalTest_level0)
{
    /* Generate TableSpace */
    BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
    TableSpaceInterface *m_testTbs = UtMockModule::UtGetTableSpace(2, bufMgr);
    (void)bufMgr->FlushAll(false);

    /* Copy dataFile as Image */
    CopyDataFile();
    /* Get endPlsn of WalFile */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);

    /* Init Recycle Root Meta */
    Transaction *trans = thrd->GetActiveTransaction();
    trans->Start();
    IndexNormalSegment::AllocIndexNormalSegment(g_defaultPdbId, m_testTbs->GetTablespaceId(), bufMgr);
    trans->Commit();
    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());
    /* read walFile to Get WalRecord */
    std::vector<WalRecordRedoInfo *> recordList;
    EXPECT_EQ(ReadRecordsAfterPlsn(plsn, WAL_BTREE_RECYCLE_ROOT_META_INIT, recordList), DSTORE_SUCC);
    EXPECT_EQ(recordList.size(), 1);

    /* Redo and check */
    BtrPage redoPage;
    BtrPage afterPage;
    WalRecordRedoContext redoCtx = {INVALID_XID, m_walWriter->GetWalId(), DSTORE::g_defaultPdbId};
    WalRecordRedoInfo *recordInfo = recordList.front();
    WalRecordBtrRecycle *record = static_cast<WalRecordBtrRecycle *>(&(recordInfo->walRecord));
    FetchPage(afterPage, record->m_pageId);
    ReadDataFileImage(record->m_pageId, (char *)&redoPage, BLCKSZ);
    RestorePage(redoPage, record->m_pageId);
    RedoBtrRecycleRecord(&redoCtx, record, recordInfo->endPlsn);
    FetchPage(redoPage, record->m_pageId);

    BtrRecycleRootMetaPage *testPageMeta = (BtrRecycleRootMetaPage *)&redoPage;
    BtrRecycleRootMetaPage *afterPageMeta = (BtrRecycleRootMetaPage *)&afterPage;
    for (int i = 0; i < MAX_BTR_RECYCLE_PARTITION; i++) {
        EXPECT_EQ(testPageMeta->GetRecyclePartitionMetaPageId(i), afterPageMeta->GetRecyclePartitionMetaPageId(i));
    }
}

TEST_F(UTBtreeWal, RecycleRootMetaSetPartitionMetaWalTest_level0)
{
    /* Generate TableSpace */
    BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
    TableSpaceInterface *m_testTbs = UtMockModule::UtGetTableSpace(2, bufMgr);
    (void)bufMgr->FlushAll(false);

    /* Copy dataFile as Image */
    CopyDataFile();
    /* Get endPlsn of WalFile */
    uint64 plsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), plsn);

    /* Allocate Index Segment */
    Transaction *trans = thrd->GetActiveTransaction();
    trans->Start();
    IndexNormalSegment *indexSegment = (IndexNormalSegment*)SegmentTest::UTAllocSegment(g_defaultPdbId,
        m_testTbs->GetTablespaceId(), bufMgr, SegmentType::INDEX_SEGMENT_TYPE);
    trans->Commit();
    /* Get a free page to initialize the btree recycle partition. */
    EXPECT_NE(indexSegment->GetFromFreeQueue(), INVALID_PAGE_ID);
    /* wait wal flush to disk */
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), m_walStream->GetMaxAppendedPlsn());
    /* read walFile to Get WalRecord */
    std::vector<WalRecordRedoInfo *> recordList;
    EXPECT_EQ(ReadRecordsAfterPlsn(plsn, WAL_BTREE_RECYCLE_ROOT_META_INIT, recordList), DSTORE_SUCC);
    EXPECT_EQ(recordList.size(), 1);
    EXPECT_EQ(ReadRecordsAfterPlsn(plsn, WAL_BTREE_RECYCLE_ROOT_META_SET_PARTITION_META, recordList), DSTORE_SUCC);
    EXPECT_EQ(recordList.size(), 2);

    /* Redo and check */
    BtrPage redoPage;
    BtrPage afterPage;
    WalRecordRedoContext redoCtx = {INVALID_XID, m_walWriter->GetWalId(), DSTORE::g_defaultPdbId};
    WalRecordRedoInfo *initRecordInfo = recordList.front();
    WalRecordRedoInfo *setMetaRecordInfo = recordList.back();

    WalRecordBtrRecycle *initRecord = static_cast<WalRecordBtrRecycle *>(&(initRecordInfo->walRecord));
    WalRecordBtrRecycle *setMetaRecord = static_cast<WalRecordBtrRecycle *>(&(setMetaRecordInfo->walRecord));

    FetchPage(afterPage, setMetaRecord->m_pageId);
    ReadDataFileImage(setMetaRecord->m_pageId, (char *)&redoPage, BLCKSZ);
    RestorePage(redoPage, setMetaRecord->m_pageId);
    RedoBtrRecycleRecord(&redoCtx, initRecord, initRecordInfo->endPlsn);
    RedoBtrRecycleRecord(&redoCtx, setMetaRecord, setMetaRecordInfo->endPlsn);
    FetchPage(redoPage, setMetaRecord->m_pageId);

    BtrRecycleRootMetaPage *testPageMeta = (BtrRecycleRootMetaPage *)&redoPage;
    BtrRecycleRootMetaPage *afterPageMeta = (BtrRecycleRootMetaPage *)&afterPage;
    EXPECT_EQ(testPageMeta->GetRecyclePartitionMetaPageId(0), afterPageMeta->GetRecyclePartitionMetaPageId(0));
}