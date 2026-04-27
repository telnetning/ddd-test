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
#include "ut_tablespace/ut_free_space_map.h"

/**
 * FreeSpaceMapTest: AccessTimestampTest
 */
TEST_F(FreeSpaceMapTest, AccessTimestampTest_level0)
{
    /**
     * Create a segment
     */
    BufferDesc *headBuf = nullptr, *fsmMetaBuf = nullptr;
    FsmPage *fsmPage = nullptr;
    HeapSegmentMetaPage *headPage = nullptr;
    FreeSpaceMapMetaPage *fsmMetaPage = nullptr;
    uint64 timestamp_t = 0;

    HeapNormalSegment *segment = UtAllocNewHeapSegment();
    PageId segmentId = segment->GetSegmentMetaPageId();

    ASSERT_TRUE(segmentId != INVALID_PAGE_ID);

    /* Check the initial timestamp is non-zero */
    ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_SHARED, headBuf, headPage);
    PageId fsmMetaPageId = headPage->fsmInfos[0].fsmMetaPageId;
    ASSERT_TRUE(fsmMetaPageId != INVALID_PAGE_ID);
    ReadBufferAndPage(fsmMetaPageId, LW_SHARED, fsmMetaBuf, fsmMetaPage);
    timestamp_t = fsmMetaPage->accessTimestamp;
    ASSERT_TRUE(timestamp_t != 0);
    m_testBufMgr->UnlockAndRelease(fsmMetaBuf);
    m_testBufMgr->UnlockAndRelease(headBuf);

    /* Sleep for 2 seconds here */
    dstore_usleep(2000000L);

    /**
     * Access FSM of this segment and check accessTimestamp varies
     */
    segment->GetPageFromFsm(BLCKSZ, 0);
    ReadBufferAndPage(fsmMetaPageId, LW_SHARED, fsmMetaBuf, fsmMetaPage);
    ASSERT_TRUE(fsmMetaPage->accessTimestamp > timestamp_t);
    m_testBufMgr->UnlockAndRelease(fsmMetaBuf);

    delete segment;
}

/**
 * FreeSpaceMapTest: ExtendCoefficientTest
 */
TEST_F(FreeSpaceMapTest, DefaultExtendCoefficientTest_level0)
{
    /**
     * Create a segment
     */
    BufferDesc *headBuf = nullptr, *fsmMetaBuf = nullptr;
    FsmPage *fsmPage = nullptr;
    HeapSegmentMetaPage *headPage = nullptr;
    FreeSpaceMapMetaPage *fsmMetaPage = nullptr;

    HeapNormalSegment *segment = UtAllocNewHeapSegment();
    PageId segmentId = segment->GetSegmentMetaPageId();

    ASSERT_TRUE(segmentId != INVALID_PAGE_ID);

    /**
     * Check m_extendCoefficient of FSM belonged to created segment to be initialized as 1
     */
    ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_SHARED, headBuf, headPage);
    PageId fsmMetaPageId = headPage->fsmInfos[0].fsmMetaPageId;
    ASSERT_TRUE(fsmMetaPageId != INVALID_PAGE_ID);
    ReadBufferAndPage(fsmMetaPageId, LW_SHARED, fsmMetaBuf, fsmMetaPage);
    ASSERT_TRUE(fsmMetaPage->extendCoefficient == 1);
    m_testBufMgr->UnlockAndRelease(fsmMetaBuf);
    m_testBufMgr->UnlockAndRelease(headBuf);

    /**
     * Acquire new page with fsmMetaPageId and check number of unused page is more than PAGES_ADD_TO_FSM_PER_TIME
     */
    segment->GetNewPage(fsmMetaPageId);
    ReadBufferAndPage(fsmMetaPageId, LW_SHARED, fsmMetaBuf, fsmMetaPage);
    ASSERT_TRUE(fsmMetaPage->GetNumUnusedPages() > PAGES_ADD_TO_FSM_PER_TIME);
    m_testBufMgr->UnlockAndRelease(fsmMetaBuf);

    delete segment;
}

/**
 * FreeSpaceMapTest: UpdateExtendCoefficientTest
 */
TEST_F(FreeSpaceMapTest, UpdateExtendCoefficientTest_TIER2_level1)
{
    /**
     * Create a segment
     */
    BufferDesc *headBuf = nullptr, *fsmMetaBuf = nullptr;
    FsmPage *fsmPage = nullptr;
    HeapSegmentMetaPage *headPage = nullptr;
    FreeSpaceMapMetaPage *fsmMetaPage = nullptr;
    uint64 extent_count = 0;

    HeapNormalSegment *segment = UtAllocNewHeapSegment();
    PageId segmentId = segment->GetSegmentMetaPageId();

    ASSERT_TRUE(segmentId != INVALID_PAGE_ID);

    /**
     * Get new page on segment until we have EXT_NUM_LINE[EXTENT_SIZE_COUNT-1] extents, then
     * Keep getting new page to mimic foreground extension and check m_extendCoefficient to be 2
     */

    while(extent_count <= EXT_NUM_LINE[EXTENT_SIZE_COUNT-1]){
        segment->GetNewPage(INVALID_PAGE_ID);
        ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_SHARED, headBuf, headPage);
        extent_count = headPage->GetExtentCount();
        m_testBufMgr->UnlockAndRelease(headBuf);
    }

    ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_SHARED, headBuf, headPage);
    PageId fsmMetaPageId = headPage->fsmInfos[0].fsmMetaPageId;
    ASSERT_TRUE(fsmMetaPageId != INVALID_PAGE_ID);
    ReadBufferAndPage(fsmMetaPageId, LW_SHARED, fsmMetaBuf, fsmMetaPage);
    ASSERT_TRUE(fsmMetaPage->extendCoefficient == 2);
    m_testBufMgr->UnlockAndRelease(fsmMetaBuf);
    m_testBufMgr->UnlockAndRelease(headBuf);

    delete segment;
}

/**
 * FreeSpaceMapTest: backgroudExtensionTest
 */
TEST_F(FreeSpaceMapTest, ExtendCoefficientInBgExtensionTest_level0)
{
    /**
     * Create a segment
     */
    BufferDesc *headBuf = nullptr, *fsmMetaBuf = nullptr;
    FsmPage *fsmPage = nullptr;
    HeapSegmentMetaPage *headPage = nullptr;
    FreeSpaceMapMetaPage *fsmMetaPage = nullptr;

    HeapNormalSegment *segment = UtAllocNewHeapSegment();
    PageId segmentId = segment->GetSegmentMetaPageId();

    ASSERT_TRUE(segmentId != INVALID_PAGE_ID);

    segment->GetNewPage(INVALID_PAGE_ID);
    ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_SHARED, headBuf, headPage);
    PageId fsmMetaPageId = headPage->fsmInfos[0].fsmMetaPageId;
    ASSERT_TRUE(fsmMetaPageId != INVALID_PAGE_ID);
    ReadBufferAndPage(fsmMetaPageId, LW_EXCLUSIVE, fsmMetaBuf, fsmMetaPage);
    m_testBufMgr->UnlockAndRelease(headBuf);

    /*
     * Manually update m_extendCoefficient to be 2 here and mimic the background extension
     * Check unused page > (2 * PAGES_ADD_TO_FSM_PER_TIME) after the extension.
     */
    fsmMetaPage->extendCoefficient = 2;
    m_testBufMgr->UnlockAndRelease(fsmMetaBuf);
    segment->GetNewPage(fsmMetaPageId);
    ASSERT_TRUE(fsmMetaPage->GetNumUnusedPages() > 2 * PAGES_ADD_TO_FSM_PER_TIME);

    delete segment;
}

/**
 * FreeSpaceMapTest: FreeSpaceMapDiagnoseTest
 */
TEST_F(FreeSpaceMapTest, FreeSpaceMapDiagnoseTest_level1)
{
    HeapNormalSegment *segment = UtAllocNewHeapSegment();
    PageId segmentId = segment->GetSegmentMetaPageId();
    ASSERT_TRUE(segmentId != INVALID_PAGE_ID);

    /* create maxmimum number of FSM on unique node Id */
    for (int i = 1; i < MAX_FSM_TREE_PER_RELATION; i++) {
        segment->UtAllocNewFsmTree(INVALID_NODE_ID - i, false);
    }

    uint64 zeroPageCnt = 1000;
    uint16 remainSpace = 345;
    PageId pageId = segment->GetNewPage(INVALID_PAGE_ID);
    ASSERT_TRUE(segment->UpdateFsm(pageId, remainSpace) == DSTORE_SUCC);

    /* extend 500 times to grow up FSM tree, and mark 500 pages with zero free space */
    uint64 pageCnt = zeroPageCnt;
    while (pageCnt > 0) {
        segment->UpdateFsm(segment->GetNewPage(INVALID_PAGE_ID), 0);
        pageCnt--;
    }

    bool found = false;
    DiagnoseIterator *iter = segment->HeapFsmScan();
    ASSERT_TRUE(iter != nullptr);
    iter->Begin();
    while (iter->HasNext()) {
        PageFreespace *pfs = (PageFreespace *) iter->GetNext();
        ASSERT_TRUE(pfs != nullptr);
        if (pfs->GetPageId() == pageId) {
            ASSERT_FALSE(found);
            ASSERT_TRUE(pfs->GetSpaceline() == PartitionFreeSpaceMap::GetListId(remainSpace));
            found = true;
        } else if (pfs->GetSpaceline() == 0) {
            pageCnt++;
        } else {
            ASSERT_TRUE(pfs->GetSpaceline() == FULL_SPACE_LIST_ID);
        }
    }
    iter->End();
    delete iter;
    ASSERT_TRUE(found);
    ASSERT_EQ(pageCnt, zeroPageCnt);
    delete segment;
}
