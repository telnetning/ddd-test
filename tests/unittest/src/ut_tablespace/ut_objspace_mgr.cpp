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
#include "ut_tablespace/ut_objspace_mgr.h"

/**
 * ObjSpaceMgrTest: PushAndPopFromObjSpaceMgrQueueTest
 */
TEST_F(ObjSpaceMgrTest, PushAndPopFromObjSpaceMgrQueueTest_level0)
{
    const uint16 task_num = 10;
    TablespaceId tablespaceIdList[task_num];
    PageId segmentIdList[task_num];
    PageId fsmMetaPageIdList[task_num];

    /**
     * Create segment(s) and add them to the task queue
     */
    for(int i = 0; i < task_num; i++){
        HeapNormalSegment *segment = UtAllocNewHeapSegment();
        HeapNormalSegment *heapSegment = UtAllocNewHeapSegment();
        TablespaceId tbsId = segment->GetTablespaceId();
        PageId segmentId = segment->GetSegmentMetaPageId();
        PageId heapSegmentId = heapSegment->GetSegmentMetaPageId();
        PageId fsmMetaPageId = INVALID_PAGE_ID;
        NodeId nodeId = INVALID_NODE_ID;
        /* Get the fsmMetaPageId of the first PartitionFreeSpaceMap */
        segment->GetFsmMetaPageId(0, fsmMetaPageId, nodeId);

        ASSERT_TRUE(tbsId == m_testTbsId);
        ASSERT_TRUE(segmentId != INVALID_PAGE_ID);
        ASSERT_TRUE(fsmMetaPageId != INVALID_PAGE_ID);

        tablespaceIdList[i] = tbsId;
        segmentIdList[i] = segmentId;
        fsmMetaPageIdList[i] = fsmMetaPageId;

        ObjSpaceMgrExtendTaskInfo taskInfo(g_defaultPdbId, heapSegmentId, tbsId, segmentId, fsmMetaPageId);
        ASSERT_EQ(m_objSpaceMgr->RegisterObjSpaceMgrTaskIfNeeded(&taskInfo), DSTORE_SUCC);
    }

    /**
     * Check if poped ObjSpaceMgrTask has the same PageId as the one we passed
     */
    for(int i = 0; i < task_num - 2; i++){
        ObjSpaceMgrTask *taskCheck = m_objSpaceMgr->GetObjSpaceMgrTask(segmentIdList[i].m_blockId);
        ObjSpaceMgrExtendTaskInfo *extendTaskInfo = static_cast<ObjSpaceMgrExtendTaskInfo *>(taskCheck->m_taskInfo);

        ASSERT_TRUE(EXTEND_TASK == extendTaskInfo->GetTaskType());
        ASSERT_TRUE(tablespaceIdList[i] == extendTaskInfo->GetTablespaceId());
        ASSERT_TRUE(segmentIdList[i] == extendTaskInfo->GetSegmentId());
        ASSERT_TRUE(fsmMetaPageIdList[i] == extendTaskInfo->GetFsmMetaPageId());
    }
    /**
     * End the test with two tasks left in the queue to test the destructor of ObjSpaceMgrQueue
     */
}

/**
 * ObjSpaceMgrTest: ExecuteExtendTest
 */
TEST_F(ObjSpaceMgrTest, ExecuteExtendTaskTest_level0)
{
    const uint16 expect_block_count = 5;
    HeapNormalSegment *segment = UtAllocNewHeapSegment();
    HeapNormalSegment *heapSegment = UtAllocNewHeapSegment();
    PageId segmentId = segment->GetSegmentMetaPageId();
    PageId heapSegmentId = heapSegment->GetSegmentMetaPageId();
    PageId fsmMetaPageId = INVALID_PAGE_ID;
    NodeId nodeId = INVALID_NODE_ID;
    /* Get the fsmMetaPageId of the first PartitionFreeSpaceMap */
    segment->GetFsmMetaPageId(0, fsmMetaPageId, nodeId);

    ASSERT_TRUE(segmentId != INVALID_PAGE_ID);
    ASSERT_TRUE(fsmMetaPageId != INVALID_PAGE_ID);
    ASSERT_TRUE(segment->GetDataBlockCount() == 0);

    ObjSpaceMgrExtendTaskInfo taskInfo(g_defaultPdbId, heapSegmentId, m_testTbsId, segmentId, fsmMetaPageId);
    ObjSpaceMgrTask *task = m_objSpaceMgr->AllocateObjSpaceMgrTask(&taskInfo);
    task->Execute();
    ASSERT_TRUE(segment->GetDataBlockCount() >= expect_block_count || segment->GetDataBlockCount() == 0);

    delete task;
}

TEST_F(ObjSpaceMgrTest, ExecuteRecycleBtreeTaskTest_level0)
{
    /* Generate a recyclable tree */
    int numLeaves = 4;
    int rootLevel = 1;
    UTRecyclableBtree *recyclableBtree = GenerateRecyclableSubtree(numLeaves, rootLevel);
    BtreeStorageMgr *btrSmgr = recyclableBtree->GetBtrStorageMgr();
    CommitSeqNo nextCsn = INVALID_CSN;
    g_storageInstance->GetCsnMgr()->GetNextCsn(nextCsn, false);

    /* Unlink middle two pages from siblings*/
    /* Step 1. Put pages into PruneQueue, set up page meta and delete tuples */
    PageId leftPageId = recyclableBtree->MakePageEmpty(1);
    PageId rightPageId = recyclableBtree->MakePageEmpty(2);

    /* Step 2. call BatchRecycleBtreePage and check pages are in FreeQueue */
    ObjSpaceMgrRecycleBtreeTaskInfo taskInfo(g_defaultPdbId, recyclableBtree->GetBtrStorageMgr()->GetTablespaceId(),
                                             recyclableBtree->GetBtrStorageMgr()->GetSegMetaPageId(),
                                             recyclableBtree->GetBtrStorageMgr()->GetMetaCreateXid(),
                                             recyclableBtree->GetIndexInfo(),
                                             recyclableBtree->GetScanKey());
    taskInfo.NeedFreeIndexMeta(false);
    BtreeRecycleWorker btrRecycleWorker(0, g_defaultPdbId);
    ObjSpaceMgrTask *task = m_objSpaceMgr->AllocateObjSpaceMgrTask(&taskInfo);
    RetStatus status = btrRecycleWorker.BtreeRecycleExecute(task);
    if (status == DSTORE_FAIL) {
        printf("RECYCLE_FSM_TASK failed: error %s\n", StorageGetMessage());
    }
    btrRecycleWorker.BtreeRecycleWorkerStop();
    ASSERT_EQ(status, DSTORE_SUCC);

    g_storageInstance->GetCsnMgr()->SetLocalCsnMin(100);

    bool foundLeft = false;
    bool foudRight = false;
    for (int i = 0; i < 10; i++) {
        PageId freePageId = btrSmgr->GetFromFreeQueue();
        if (freePageId == leftPageId) {
            foundLeft = true;
        } else if (freePageId == rightPageId) {
            foudRight = true;
        }
    }
    EXPECT_TRUE(foundLeft);
    EXPECT_TRUE(foudRight);

    delete recyclableBtree;
}
