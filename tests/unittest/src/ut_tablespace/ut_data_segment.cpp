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

#include "ut_tablespace/ut_data_segment.h"
#include "tablespace/dstore_data_segment_context.h"
#include "index/dstore_btree_recycle_partition.h"
#include "diagnose/dstore_tablespace_diagnose.h"
#include "tablespace/dstore_index_temp_segment.h"
#include "ut_tablespace/ut_temp_tablespace.h"

const uint16 PRE_BITMAP_PAGE_COUNT = 3;

void AllocDataSegmentInThread(InitDataPageCallback initCallback, TablespaceId tableSpaceId, BufMgrInterface *bufMgr, uint32 extentCount)
{
    ((UtInstance *)g_storageInstance)->ThreadSetupAndRegister();
    TablespaceMgr *tablespaceMgr = g_storageInstance->GetPdb(g_defaultPdbId)->GetTablespaceMgr();
    TableSpace *tablespace = tablespaceMgr->OpenTablespace(tableSpaceId, DSTORE::DSTORE_ACCESS_SHARE_LOCK);
    HeapNormalSegment *segment = (HeapNormalSegment*)SegmentTest::UTAllocSegment(g_defaultPdbId, tablespace->GetTablespaceId(), bufMgr, SegmentType::HEAP_SEGMENT_TYPE);
    for (;;) {
        PageId extPageId = segment->GetNewPage();
        ASSERT_NE(extPageId, INVALID_PAGE_ID);
        BufferDesc *headBuf = bufMgr->Read(g_defaultPdbId, segment->GetSegmentMetaPageId(), LW_SHARED);
        HeapSegmentMetaPage *headPage = (HeapSegmentMetaPage *)headBuf->GetPage();
        uint64 curExtCount = headPage->GetExtentCount();
        bufMgr->UnlockAndRelease(headBuf);
        if (curExtCount == extentCount) {
            break;
        }
    }
    delete segment;
    tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_ACCESS_SHARE_LOCK);
    ((UtInstance *)g_storageInstance)->ThreadUnregisterAndExit();
}

void InitSegmentInThread(PageId segmentId, InitDataPageCallback initCallback, TableSpaceInterface *tablespace,
                         BufMgrInterface *bufMgr)
{
    ((UtInstance *)g_storageInstance)->ThreadSetupAndRegister();
    HeapNormalSegment *readSegment =
        DstoreNew(g_dstoreCurrentMemoryContext) HeapNormalSegment(g_defaultPdbId, segmentId, tablespace->GetTablespaceId(), bufMgr);
    ASSERT_EQ(readSegment->InitSegment(), DSTORE_SUCC);
    delete readSegment;
    ((UtInstance *)g_storageInstance)->ThreadUnregisterAndExit();
}

void GetPageInThread(IndexNormalSegment *segment, uint32 i) {
    create_thread_and_register();
    ut_init_transaction_runtime();
    for (uint32 j = 0; j < 100; j++) {
        PageId pageId = segment->GetNewPage();
        /* sleep 0.1 milliseconds */
        usleep(100);
    }
    unregister_thread();
}

void AddPageInThread(IndexNormalSegment *segment, PageId startPageId) {
    create_thread_and_register();
    ut_init_transaction_runtime();
    for (uint32 j = 0; j < 100; j++) {
        startPageId.m_blockId++;
        segment->PutIntoFreeQueue(startPageId);
    }
    unregister_thread();
}

void DataSegmentTest::UtCheckInitInfo(DataSegment *segment, SegmentType type)
{
    PageId headPageId = segment->GetSegmentMetaPageId();
    BufferDesc *headBuf = m_testBufMgr->Read(g_defaultPdbId, headPageId, LW_SHARED);
    DataSegmentMetaPage *headPage = (DataSegmentMetaPage *)headBuf->GetPage();

    /* Base page info */
    ASSERT_EQ(headPage->m_header.m_myself, headPageId);

    /* Basic segment head info */
    ASSERT_EQ(headPage->segmentHeader.segmentType, type);
    ASSERT_EQ(headPage->GetTotalBlockCount(), EXT_SIZE_8);
    ASSERT_EQ(headPage->GetExtentCount(), 1);
    ASSERT_EQ(headPage->segmentHeader.extents.first, headPageId);
    ASSERT_EQ(headPage->GetLastExtent(), headPageId);

    PageId expectFsm = INVALID_PAGE_ID;
    if (type == DSTORE::SegmentType::INDEX_SEGMENT_TYPE) {
        ASSERT_EQ(headPage->GetType(), PageType::DATA_SEGMENT_META_PAGE_TYPE);
        PageId extentEndPage = {headPageId.m_fileId, headPageId.m_blockId + EXT_SIZE_8 - 1};
        ASSERT_EQ(headPage->extendedPageId, extentEndPage);
        m_testBufMgr->UnlockAndRelease(headBuf);
    } else {
        HeapSegmentMetaPage *heapHeadPage = (HeapSegmentMetaPage *)headBuf->GetPage();
        ASSERT_EQ(heapHeadPage->GetType(), PageType::HEAP_SEGMENT_META_PAGE_TYPE);

        PageId fsmMetaPageId = heapHeadPage->fsmInfos[0].fsmMetaPageId;
        BufferDesc *fsmMetaPageBuf = m_testBufMgr->Read(g_defaultPdbId, fsmMetaPageId, LW_SHARED);
        FreeSpaceMapMetaPage *fsmMetaPage = (FreeSpaceMapMetaPage*)fsmMetaPageBuf->GetPage();

        /* FSM related info (used in HeapNormalSegment) */
        expectFsm = {headPageId.m_fileId, headPageId.m_blockId + 2};
        ASSERT_EQ(fsmMetaPage->GetFsmRoot(), expectFsm);
        ASSERT_EQ(fsmMetaPage->GetFsmRootLevel(), 0);
        ASSERT_EQ(heapHeadPage->GetDataBlockCount(), 0);
        ASSERT_EQ(heapHeadPage->dataFirst, INVALID_PAGE_ID);
        ASSERT_EQ(heapHeadPage->dataLast, INVALID_PAGE_ID);
        ASSERT_EQ(fsmMetaPage->mapCount[0], 1);
        ASSERT_EQ(fsmMetaPage->currMap[0], expectFsm);
        for (int i = 1; i < HEAP_MAX_MAP_LEVEL; ++i) {
            ASSERT_EQ(fsmMetaPage->mapCount[i], 0);
            ASSERT_EQ(fsmMetaPage->currMap[i], INVALID_PAGE_ID);
        }
        ASSERT_EQ(heapHeadPage->addedPageId, expectFsm);
        m_testBufMgr->UnlockAndRelease(fsmMetaPageBuf);

        PageId extentEndPage = {headPageId.m_fileId, headPageId.m_blockId + EXT_SIZE_8 - 1};
        ASSERT_EQ(heapHeadPage->extendedPageId, extentEndPage);
        m_testBufMgr->UnlockAndRelease(headBuf);

        /* Read FSM root page info */
        BufferDesc *fsmBuf = m_testBufMgr->Read(g_defaultPdbId, expectFsm, LW_SHARED);
        FsmPage *fsmPage = (FsmPage*)fsmBuf->GetPage();
        ASSERT_EQ(fsmPage->fsmPageHeader.upperIndex.page, INVALID_PAGE_ID);
        ASSERT_EQ(fsmPage->fsmPageHeader.upperIndex.slot, INVALID_FSM_SLOT_NUM);
        for (auto &fsmList : fsmPage->fsmPageHeader.lists) {
            ASSERT_EQ(fsmList.count, 0);
            ASSERT_EQ(fsmList.first, INVALID_FSM_SLOT_NUM);
        }
        ASSERT_EQ(fsmPage->fsmPageHeader.hwm, 0);
        m_testBufMgr->UnlockAndRelease(fsmBuf);
    }
}

/**
 * AllocSegment: Check Invalid parameter.
 */
TEST_F(DataSegmentTest, AllocDataSegmentTest001_level0)
{
    HeapNormalSegment *heapSegment = (HeapNormalSegment*)SegmentTest::UTAllocSegment(g_defaultPdbId, 0, m_testBufMgr, SegmentType::HEAP_SEGMENT_TYPE);
    ASSERT_FALSE(SegmentIsValid(heapSegment));
    heapSegment = (HeapNormalSegment*)SegmentTest::UTAllocSegment(g_defaultPdbId, m_testTbs->GetTablespaceId(), nullptr, SegmentType::HEAP_SEGMENT_TYPE);
    ASSERT_FALSE(SegmentIsValid(heapSegment));
    heapSegment = (HeapNormalSegment*)SegmentTest::UTAllocSegment(g_defaultPdbId, 0, nullptr, SegmentType::HEAP_SEGMENT_TYPE);
    ASSERT_FALSE(SegmentIsValid(heapSegment));
}

/**
 * AllocSegment: basic function.
 */
TEST_F(DataSegmentTest, AllocDataSegmentTest002_level0)
{
    Transaction *trans = thrd->GetActiveTransaction();
    trans->Start();
    HeapNormalSegment *heapSegment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(heapSegment));
    UtCheckInitInfo(heapSegment, SegmentType::HEAP_SEGMENT_TYPE);
    IndexNormalSegment *indexSegment = UtAllocNewIndexSegment();
    ASSERT_TRUE(SegmentIsValid(indexSegment));
    UtCheckInitInfo(indexSegment, SegmentType::INDEX_SEGMENT_TYPE);
    heapSegment->DropSegment();
    delete heapSegment;
    indexSegment->DropSegment();
    delete indexSegment;
    trans->Commit();
}

/**
 * AllocSegment: AllocMultipleDataSegment
 */
TEST_F(DataSegmentTest, AllocDataSegmentMultipleSegment001_level0)
{
    HeapNormalSegment *segment1 = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment1));
    HeapNormalSegment *segment2 = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment2));
    PageId head1 = segment1->GetSegmentMetaPageId();
    PageId head2 = segment2->GetSegmentMetaPageId();
    for (int i = 0; i < EXT_SIZE_8; ++i) {
        for (int j = 0; j < EXT_SIZE_8; ++j) {
            PageId page1 = {head1.m_fileId, head1.m_blockId + i};
            PageId page2 = {head2.m_fileId, head2.m_blockId + j};
            ASSERT_NE(page1, page2);
        }
    }

    segment1->DropSegment();
    delete segment1;
    segment2->DropSegment();
    delete segment2;
}

/**
 * DropSegment: drop segment will release all space in segment.
 */
TEST_F(DataSegmentTest, DropSegmentTest001_level0)
{
    HeapNormalSegment *segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment));
    PageId segmentId = segment->GetSegmentMetaPageId();
    ASSERT_NE(segmentId, INVALID_PAGE_ID);
    ASSERT_TRUE(SegmentIsValid(segment));

    /* GetNewPage will extend the segment. */
    for (uint64 i = 0; i < 100; ++i) {
        segment->GetNewPage();
    }
    /* DropSegment will release all space in segment */
    ASSERT_EQ(segment->DropSegment(), DSTORE_SUCC);
    ASSERT_TRUE(SegmentIsValid(segment));
    delete segment;

    HeapNormalSegment *newSegment = UtAllocNewHeapSegment();
    PageId newSegmentId = newSegment->GetSegmentMetaPageId();
    ASSERT_NE(newSegmentId, INVALID_PAGE_ID);
    ASSERT_EQ(newSegment->DropSegment(), DSTORE_SUCC);
    delete newSegment;

    /* if segmentId == newSegmentId, indicates that all space of the segment is released. */
    ASSERT_EQ(segmentId.m_fileId, newSegmentId.m_fileId);
    ASSERT_EQ(segmentId.m_blockId, newSegmentId.m_blockId);
}

/**
 * DropSegment: after drop segment, all interface(InitSegment/GetNewPage/DropSegment) in Segment is unusable.
 */
TEST_F(DataSegmentTest, DropSegmentTest002_level0)
{
    HeapNormalSegment *segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment));
    ASSERT_NE(segment->GetSegmentMetaPageId(), INVALID_PAGE_ID);
    PageId heapSegmentId = segment->GetSegmentMetaPageId();
    delete segment;

    HeapNormalSegment *readSegment = DstoreNew(g_dstoreCurrentMemoryContext)HeapNormalSegment(g_defaultPdbId, heapSegmentId, m_testTbs->GetTablespaceId(), m_testBufMgr);
    ASSERT_EQ(readSegment->InitSegment(), DSTORE_SUCC);
    ASSERT_EQ(readSegment->DropSegment(), DSTORE_SUCC);
    ASSERT_EQ(readSegment->Init(), DSTORE_FAIL);
    ASSERT_EQ(readSegment->InitSegment(), DSTORE_FAIL);
    ASSERT_EQ(readSegment->GetNewPage(), INVALID_PAGE_ID);
    /* Could not use any function after segment is dropped */
    PageId tempPage;
    ASSERT_EQ(readSegment->Extend(EXT_SIZE_8, &tempPage), DSTORE_FAIL);
    ASSERT_EQ(tempPage, INVALID_PAGE_ID);
    ASSERT_EQ(readSegment->GetSegmentMetaPageId(), INVALID_PAGE_ID);
    ASSERT_EQ(readSegment->DropSegment(), DSTORE_FAIL);

    delete readSegment;
}


TEST_F(DataSegmentTest, DropSegmentTest003_level0)
{
    Transaction *trans = thrd->GetActiveTransaction();
    trans->Start();
    HeapNormalSegment *heapSegment = UtAllocNewHeapSegment();

    ASSERT_TRUE(SegmentIsValid(heapSegment));
    IndexNormalSegment *indexSegment = UtAllocNewIndexSegment();
    ASSERT_TRUE(SegmentIsValid(indexSegment));

    heapSegment->DropSegment();
    indexSegment->DropSegment();
    delete heapSegment;
    delete indexSegment;
    trans->Commit();
}

/**
 * @tc.name:  DropSegmentTest004_level0
 * @tc.desc:  1.After DropSegment() called, all interfaces will return false
 * @tc.type: FUNC
 * @tc.require:
 * @tc.author: liangyanzhao
*/
TEST_F(DataSegmentTest, DropSegmentTest004_level0)
{
    HeapNormalSegment *segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment));
    ASSERT_NE(segment->GetNewPage(), INVALID_PAGE_ID);
    PageId firstPageId = segment->GetFirstDataPage();
    ASSERT_NE(firstPageId, INVALID_PAGE_ID);
    FsmIndex fsmIndex;
    ReadPageForFsmIndex(firstPageId, LW_SHARED, &fsmIndex);
    segment->DropSegment();
    ASSERT_EQ(segment->GetPageFromFsm(1, 0), INVALID_PAGE_ID);
    ASSERT_EQ(segment->GetPageFromFsm(1, 0, nullptr), INVALID_PAGE_ID);
    ASSERT_EQ(segment->UpdateFsm(firstPageId, 0), DSTORE_FAIL);
    ASSERT_EQ(segment->UpdateFsm(fsmIndex, 0), DSTORE_FAIL);
    PageId newPageId;
    uint32 spaceInFsm = 0;
    ASSERT_EQ(segment->UpdateFsmAndSearch(fsmIndex, 0, 1, 0, &newPageId, &spaceInFsm), DSTORE_FAIL);
    delete segment;
}

/**
 * InitSegment: Check invalid parameter.
 */
TEST_F(DataSegmentTest, InitSegmentTest001_level1)
{
    HeapNormalSegment *heapSegment = DstoreNew(g_dstoreCurrentMemoryContext)HeapNormalSegment(g_defaultPdbId,
            INVALID_PAGE_ID, m_testTbs->GetTablespaceId(), m_testBufMgr);
    ASSERT_EQ(heapSegment->InitSegment(), DSTORE_FAIL);
    delete heapSegment;

    heapSegment = DstoreNew(g_dstoreCurrentMemoryContext)HeapNormalSegment(g_defaultPdbId, 
        INVALID_PAGE_ID, 0, m_testBufMgr);
    ASSERT_EQ(heapSegment->InitSegment(), DSTORE_FAIL);
    delete heapSegment;

    heapSegment = DstoreNew(g_dstoreCurrentMemoryContext)HeapNormalSegment(g_defaultPdbId,
        INVALID_PAGE_ID, m_testTbs->GetTablespaceId(), nullptr);
    ASSERT_EQ(heapSegment->InitSegment(), DSTORE_FAIL);
    delete heapSegment;

    heapSegment = DstoreNew(g_dstoreCurrentMemoryContext)HeapNormalSegment(g_defaultPdbId,
        INVALID_PAGE_ID, m_testTbs->GetTablespaceId(), m_testBufMgr);
    ASSERT_EQ(heapSegment->InitSegment(), DSTORE_FAIL);
    delete heapSegment;

    /* a valid pageId but not segmentId, init failed */
    HeapNormalSegment *newSegment1 = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(newSegment1));
    PageId segmentId = newSegment1->GetSegmentMetaPageId();
    segmentId.m_blockId++;
    heapSegment = DstoreNew(g_dstoreCurrentMemoryContext)HeapNormalSegment(g_defaultPdbId,
        segmentId, m_testTbs->GetTablespaceId(), m_testBufMgr);
    ASSERT_EQ(heapSegment->InitSegment(), DSTORE_FAIL);
    newSegment1->DropSegment();
    delete newSegment1;
    delete heapSegment;
}

/**
 * InitSegment: duplicate Init
 */
TEST_F(DataSegmentTest, InitSegmentTest002_level0)
{
    HeapNormalSegment *segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment));
    ASSERT_NE(segment->GetSegmentMetaPageId(), INVALID_PAGE_ID);
    PageId heapSegmentId = segment->GetSegmentMetaPageId();
    delete segment;

    HeapNormalSegment *readSegment = DstoreNew(g_dstoreCurrentMemoryContext)HeapNormalSegment(g_defaultPdbId,
        heapSegmentId, m_testTbs->GetTablespaceId(), m_testBufMgr);
    for (int i = 0; i < 10; i++) {
        ASSERT_EQ(readSegment->InitSegment(), DSTORE_SUCC);
    }
    readSegment->DropSegment();
    delete readSegment;
}

/**
 * InitSegment: Init a segment that is created before.
 * 1. New(DataSegment)-GetPageFromFsm, failed because of not Init
 * 2. New(DataSegment)-Init-GetPageFromFsm, success.
 */
TEST_F(DataSegmentTest, InitSegmentTest003_level0)
{
    HeapNormalSegment *segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment));
    ASSERT_NE(segment->GetSegmentMetaPageId(), INVALID_PAGE_ID);
    PageId heapSegmentId = segment->GetSegmentMetaPageId();
    ASSERT_NE(segment->GetNewPage(), INVALID_PAGE_ID);
    PageId firstDataPageId = segment->GetFirstDataPage();
    ASSERT_NE(firstDataPageId, INVALID_PAGE_ID);
    FsmIndex fsmIndex;
    ReadPageForFsmIndex(firstDataPageId, LW_SHARED, &fsmIndex);
    PageId newPageId;
    delete segment;

    HeapNormalSegment *readSegment = DstoreNew(g_dstoreCurrentMemoryContext)HeapNormalSegment(g_defaultPdbId,
        heapSegmentId, m_testTbs->GetTablespaceId(), m_testBufMgr);
    uint32 spaceInFsm = 0;
    ASSERT_TRUE(SegmentIsValid(readSegment));
    /* Could not use any function except Init() and GetSegmentId() */
    ASSERT_EQ(readSegment->GetSegmentMetaPageId(), heapSegmentId);
    ASSERT_EQ(readSegment->GetNewPage(), INVALID_PAGE_ID);
    ASSERT_EQ(readSegment->GetPageFromFsm(0, 0), INVALID_PAGE_ID);
    ASSERT_EQ(readSegment->GetPageFromFsm(0, 0, nullptr), INVALID_PAGE_ID);
    ASSERT_EQ(readSegment->UpdateFsm(firstDataPageId, 0), DSTORE_FAIL);
    ASSERT_EQ(readSegment->UpdateFsm(fsmIndex, 0), DSTORE_FAIL);
    ASSERT_EQ(readSegment->UpdateFsmAndSearch(fsmIndex, 0, 1, 0, &newPageId, &spaceInFsm), DSTORE_FAIL);
    ASSERT_EQ(readSegment->DropSegment(), DSTORE_FAIL);

    ASSERT_EQ(readSegment->InitSegment(), DSTORE_SUCC);
    ASSERT_NE(readSegment->GetNewPage(), INVALID_PAGE_ID);
    ASSERT_NE(readSegment->GetPageFromFsm(0, 0), INVALID_PAGE_ID);
    ASSERT_EQ(readSegment->GetPageFromFsm(BLCKSZ + 1, 0, nullptr), INVALID_PAGE_ID);
    ASSERT_EQ(readSegment->UpdateFsm(firstDataPageId, BLCKSZ + 1), DSTORE_FAIL);
    ASSERT_EQ(readSegment->UpdateFsm(fsmIndex, BLCKSZ + 1), DSTORE_FAIL);
    ASSERT_EQ(readSegment->UpdateFsmAndSearch(fsmIndex, BLCKSZ + 1, 1, 0, &newPageId, &spaceInFsm), DSTORE_FAIL);
    ASSERT_EQ(readSegment->UpdateFsmAndSearch(fsmIndex, 1, BLCKSZ + 1, 0, &newPageId, &spaceInFsm), DSTORE_FAIL);
    ASSERT_NE(readSegment->DropSegment(), DSTORE_FAIL);
    delete readSegment;
}

/**
 * InitSegment: Init an existing segment
 */
TEST_F(DataSegmentTest, InitSegmentTest004_level0)
{
    int rc;
    HeapNormalSegment *newHeapSegment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(newHeapSegment));
    PageId heapSegmentId = newHeapSegment->GetSegmentMetaPageId();
    delete newHeapSegment;
    HeapNormalSegment *heapSegment = DstoreNew(g_dstoreCurrentMemoryContext)HeapNormalSegment(g_defaultPdbId,
        heapSegmentId, m_testTbs->GetTablespaceId(), m_testBufMgr);
    heapSegment->InitSegment();
    ASSERT_TRUE(SegmentIsValid(heapSegment));
    PageId newPage = heapSegment->GetNewPage();
    ASSERT_NE(newPage, INVALID_PAGE_ID);
    BufferDesc *headBuf = m_testBufMgr->Read(g_defaultPdbId, heapSegment->GetSegmentMetaPageId(), LW_SHARED);
    HeapSegmentMetaPage *headPage = (HeapSegmentMetaPage *)headBuf->GetPage();
    headPage->DumpHeapSegmentMetaPage();
    uint64 curDataBlockCount = headPage->GetDataBlockCount();
    m_testBufMgr->UnlockAndRelease(headBuf);
    PageId pageFromFsm{};
    for (uint64 i = 0; i < curDataBlockCount; ++i) {
        pageFromFsm = heapSegment->GetPageFromFsm(1, 0);
        ASSERT_NE(pageFromFsm, INVALID_PAGE_ID);
        rc = heapSegment->UpdateFsm(pageFromFsm, 0);
        ASSERT_EQ(rc, 0);
    }
    pageFromFsm = heapSegment->GetPageFromFsm(1, 0);
    ASSERT_EQ(pageFromFsm, INVALID_PAGE_ID);
    heapSegment->DropSegment();
    delete heapSegment;

    HeapNormalSegment *newIndexSegment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(newIndexSegment));
    PageId indexSegmentId = newIndexSegment->GetSegmentMetaPageId();
    newPage = newIndexSegment->GetNewPage();
    ASSERT_NE(newPage, INVALID_PAGE_ID);
    headBuf = m_testBufMgr->Read(g_defaultPdbId, indexSegmentId, LW_SHARED);
    headPage = (HeapSegmentMetaPage *)headBuf->GetPage();
    curDataBlockCount = headPage->GetDataBlockCount();
    m_testBufMgr->UnlockAndRelease(headBuf);
    UpdatePagesFreeSpace(newIndexSegment, curDataBlockCount - 1, 0);
    UpdatePagesFreeSpace(newIndexSegment, 1, 1024);
    delete newIndexSegment;
    HeapNormalSegment *indexSegment = DstoreNew(g_dstoreCurrentMemoryContext)HeapNormalSegment(g_defaultPdbId,
        indexSegmentId, m_testTbs->GetTablespaceId(), m_testBufMgr);
    indexSegment->InitSegment();
    ASSERT_TRUE(SegmentIsValid(indexSegment));
    pageFromFsm = indexSegment->GetPageFromFsm(1025, 0);
    ASSERT_EQ(pageFromFsm, INVALID_PAGE_ID);
    pageFromFsm = indexSegment->GetPageFromFsm(1024, 0);
    ASSERT_NE(pageFromFsm, INVALID_PAGE_ID);
    indexSegment->DropSegment();
    delete indexSegment;
}

/**
  *@tc.name:  InitConcurrency001
  *@tc.desc:  创建1个DataSegment之后，生成1个改DataSegment对象，预期在10个线程里面Init都能成功。
 * @tc.type: FUNC
 * @tc. require:
  *@tc.author: wangsiyuan
*/
TEST_F(DataSegmentTest, InitSegmentConcurrency001_level1)
{
    HeapNormalSegment *segment = (HeapNormalSegment*)SegmentTest::UTAllocSegment(g_defaultPdbId, m_testTbs->GetTablespaceId(), m_testBufMgr, SegmentType::HEAP_SEGMENT_TYPE);
    ASSERT_TRUE(SegmentIsValid(segment));
    PageId segmentId = segment->GetSegmentMetaPageId();
    ASSERT_NE(segmentId, INVALID_PAGE_ID);
    for (int i = 0; i < 10; i++) {
        PageId pageId = segment->GetNewPage();
        ASSERT_NE(pageId, INVALID_PAGE_ID);
    }

    std::thread threads[10];
    for (int i = 0; i < 10; i++) {
        threads[i] = std::thread(InitSegmentInThread, segmentId, m_initHeapPageCallback, m_testTbs, m_testBufMgr);
        threads[i].join();
    }
    ASSERT_EQ(segment->DropSegment(), DSTORE_SUCC);
    delete segment;
}

/**
 * GetNewPage:
 * 1. GetNewPage will add PAGES_ADD_TO_FSM_PER_TIME pages(free size is BLCKSZ),
 * 2. GetPageFromFsm(BLCKSZ) will get one of them.
 */
TEST_F(DataSegmentTest, GetNewPageTest001_level0)
{
    HeapNormalSegment *segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment));
    BufferDesc *dataBuf, *fsmBuf, *headBuf;
    DataPage *dataPage;
    FsmPage *fsmPage;
    HeapSegmentMetaPage *headPage;
    SmgrType smgrType = SmgrType::HEAP_SMGR;
    StorageRelationData tmpRel;
    tmpRel.tableSmgr = DstoreNew(m_ut_memory_context)
        TableStorageMgr(segment->GetPdbId(), DEFAULT_HEAP_FILLFACTOR, nullptr, SYS_RELPERSISTENCE_PERMANENT);
    tmpRel.SetTableSmgrSegment(dynamic_cast<HeapSegment *>(segment));
    DataSegmentScanContext  segmentScanContext(g_storageInstance->GetBufferMgr(), &tmpRel, smgrType);
    /* Call GetNewPage first time */
    PageId newPage1 = segment->GetNewPage();
    ASSERT_NE(newPage1, INVALID_PAGE_ID);
    ASSERT_EQ(segmentScanContext.GetFirstPageId(), newPage1);
    ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_SHARED, headBuf, headPage);
    PageId lastDataPage = headPage->dataLast;
    /* 1 seg meta, 1 fsm meta, 1 fsm root at initialization */
    uint16 expectAddPageCount = DstoreMin(FIRST_EXT_SIZE - 3, PAGES_ADD_TO_FSM_PER_TIME);
    ASSERT_EQ(headPage->GetDataBlockCount(), expectAddPageCount);
    m_testBufMgr->UnlockAndRelease(headBuf);
    PageId newPageList[PAGES_ADD_TO_FSM_PER_TIME];
    uint16 index = 0;
    PageId curPage = segmentScanContext.GetFirstPageId();
    while (curPage != INVALID_PAGE_ID) {
        newPageList[index] = curPage;
        index += 1;
        curPage = segmentScanContext.GetNextPageId();
    }
    index = 0;
    while (index < expectAddPageCount) {
        ReadBufferAndPage(newPageList[index], LW_SHARED, dataBuf, dataPage);
        ASSERT_EQ(dataPage->m_header.m_myself, newPageList[index]);
        ASSERT_EQ(dataPage->GetType(), PageType::HEAP_PAGE_TYPE);
        m_testBufMgr->UnlockAndRelease(dataBuf);
        index += 1;
    }

    /* the result of GetPageFromFsm should be in newPageList */
    PageId pageId1 = segment->GetPageFromFsm(BLCKSZ, 0);
    bool pageInList = false;
    for (int i = 0; i < PAGES_ADD_TO_FSM_PER_TIME; i++) {
        pageInList = pageInList || (pageId1 == newPageList[i]);
    }
    ASSERT_TRUE(pageInList);

    segment->DropSegment();
    delete segment;
    tmpRel.SetTableSmgrSegment(nullptr);
    delete tmpRel.tableSmgr;
}

/**
 * GetNewPage:
 * Test Segment of HEAP_SEGMENT_TYPE type.
 */
TEST_F(DataSegmentTest, GetNewPageTest002_level0)
{
    HeapNormalSegment *segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment));
    BufferDesc *headBuf;
    HeapSegmentMetaPage *headPage;
    uint64 currentBlockCount = 0;

    /* Get new index page */
    PageId newPage1 = segment->GetNewPage();
    ASSERT_NE(newPage1, INVALID_PAGE_ID);
    /* 1 seg meta, 1 fsm meta, 1 fsm root at initialization */
    uint16 expectAddPageCount = DstoreMin(FIRST_EXT_SIZE - 3, PAGES_ADD_TO_FSM_PER_TIME);
    currentBlockCount += expectAddPageCount;
    PageId firstPageId = segment->GetFirstDataPage();
    ASSERT_NE(firstPageId, INVALID_PAGE_ID);
    ASSERT_EQ(firstPageId, newPage1);
    ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_SHARED, headBuf, headPage);
    ASSERT_EQ(headPage->GetDataBlockCount(), currentBlockCount);
    uint64 curExtCount = headPage->GetExtentCount();
    m_testBufMgr->UnlockAndRelease(headBuf);
    BufferDesc *pageBuf = m_testBufMgr->Read(g_defaultPdbId, newPage1, LW_SHARED);
    BtrPage *page = (BtrPage*)pageBuf->GetPage();
    ASSERT_EQ(page->m_header.m_myself, newPage1);
    ASSERT_EQ(page->GetType(), PageType::HEAP_PAGE_TYPE);
    m_testBufMgr->UnlockAndRelease(pageBuf);

    /* Get new page until segment has 2 extents */
    while (curExtCount < 2) {
        PageId newPage = segment->GetNewPage();
        ASSERT_NE(newPage, INVALID_PAGE_ID);
        ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_SHARED, headBuf, headPage);
        curExtCount = headPage->GetExtentCount();
        m_testBufMgr->UnlockAndRelease(headBuf);
    }

    ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_SHARED, headBuf, headPage);
    PageId lastExt = headPage->GetLastExtent();
    curExtCount = headPage->GetExtentCount();
    ASSERT_LE(curExtCount, 16);
    m_testBufMgr->UnlockAndRelease(headBuf);
    BufferDesc *extBuf;
    SegExtentMetaPage *extMetaPage;
    ReadBufferAndPage(lastExt, LW_SHARED, extBuf, extMetaPage);
    ASSERT_EQ(extMetaPage->extentMeta.extSize, EXT_SIZE_8);
    m_testBufMgr->UnlockAndRelease(extBuf);

    segment->DropSegment();
    delete segment;
}

TEST_F(DataSegmentTest, GetNewPageIncreaseFsmLevel001_level0)
{
    int rc;
    HeapNormalSegment *segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment));
    HeapSegmentMetaPage *headPage;
    FreeSpaceMapMetaPage *fsmMetaPage;
    BufferDesc *fsmBuf, *dataBuf, *headBuf, *fsmMetaPageBuf;
    FsmPage *fsmPage;
    HeapPage *dataPage;

    /* Call GetNewPage(), set all new pages to 0 space and simulate level-0 FSM tree is full */
    PageId newPage = segment->GetNewPage();
    ASSERT_NE(newPage, INVALID_PAGE_ID);
    ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_SHARED, headBuf, headPage);
    uint64 curDataBlockCount = headPage->GetDataBlockCount();
    m_testBufMgr->UnlockAndRelease(headBuf);
    UpdatePagesFreeSpace(segment, curDataBlockCount, 0);
    SimulateFsmLevelFull(0, segment);

    /* FSM level increase to 1 */
    newPage = segment->GetNewPage();
    ASSERT_NE(newPage, INVALID_PAGE_ID);
    ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_SHARED, headBuf, headPage);
    uint64 addDataPageCount = headPage->GetDataBlockCount() - curDataBlockCount;
    m_testBufMgr->UnlockAndRelease(headBuf);
    UpdatePagesFreeSpace(segment, addDataPageCount - 1, 0);
    PageId targetPage = segment->GetPageFromFsm(1, 0);
    ASSERT_NE(targetPage, INVALID_PAGE_ID);
    ReadBufferAndPage(targetPage, LW_SHARED, dataBuf, dataPage);
    uint16 expectSlot = dataPage->GetFsmIndex().slot;
    m_testBufMgr->UnlockAndRelease(dataBuf);
    ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_SHARED, headBuf, headPage);
    PageId fsmMetaPageId = headPage->fsmInfos[0].fsmMetaPageId;
    ReadBufferAndPage(fsmMetaPageId, LW_SHARED, fsmMetaPageBuf, fsmMetaPage);
    ASSERT_EQ(fsmMetaPage->GetFsmRootLevel(), 1);
    PageId level0Fsm = fsmMetaPage->currMap[0];
    PageId level1Fsm = fsmMetaPage->currMap[1];
    ASSERT_NE(level1Fsm, INVALID_PAGE_ID);
    m_testBufMgr->UnlockAndRelease(fsmMetaPageBuf);
    m_testBufMgr->UnlockAndRelease(headBuf);
    ReadBufferAndPage(level1Fsm, LW_SHARED, fsmBuf, fsmPage);
    ASSERT_EQ(fsmPage->FsmNodePtr(1)->page, level0Fsm);
    ASSERT_EQ(fsmPage->FsmNodePtr(1)->listId, 8);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->first, 1);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->count, 1);
    m_testBufMgr->UnlockAndRelease(fsmBuf);
    ReadBufferAndPage(level0Fsm, LW_SHARED, fsmBuf, fsmPage);
    ASSERT_EQ(fsmPage->fsmPageHeader.upperIndex.page, level1Fsm);
    ASSERT_EQ(fsmPage->fsmPageHeader.upperIndex.slot, 1);
    m_testBufMgr->UnlockAndRelease(fsmBuf);

    /* Update targetPage to free space 512, level0Fsm move from list 5 to list 1 in level1Fsm */
    rc = segment->UpdateFsm(targetPage, 512);
    ASSERT_EQ(rc, 0);
    ReadBufferAndPage(level1Fsm, LW_SHARED, fsmBuf, fsmPage);
    ASSERT_EQ(fsmPage->FsmNodePtr(1)->listId, 4);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->first, INVALID_FSM_SLOT_NUM);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->count, 0);
    ASSERT_EQ(fsmPage->FsmListPtr(4)->first, 1);
    ASSERT_EQ(fsmPage->FsmListPtr(4)->count, 1);
    m_testBufMgr->UnlockAndRelease(fsmBuf);

    segment->DropSegment();
    delete segment;
}

TEST_F(DataSegmentTest, GetNewPageIncreaseFsmLevel002_level0)
{
    int rc;
    HeapNormalSegment *segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment));
    BufferDesc *fsmBuf, *dataBuf, *headBuf, *fsmMetaPageBuf;
    FsmPage *fsmPage;
    HeapPage *dataPage;
    HeapSegmentMetaPage *headPage;
    FreeSpaceMapMetaPage *fsmMetaPage;

    /* Call GetNewPage(), set all new pages to 0 space and simulate level-1 FSM tree is full */
    PageId newPage = segment->GetNewPage();
    ASSERT_NE(newPage, INVALID_PAGE_ID);
    ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_SHARED, headBuf, headPage);
    uint64 curDataBlockCount = headPage->GetDataBlockCount();
    m_testBufMgr->UnlockAndRelease(headBuf);
    UpdatePagesFreeSpace(segment, curDataBlockCount, 0);
    SimulateFsmLevelFull(1, segment);

    /* FSM level increase to 2 */
    newPage = segment->GetNewPage();
    ASSERT_NE(newPage, INVALID_PAGE_ID);
    ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_SHARED, headBuf, headPage);
    uint64 addDataPageCount = headPage->GetDataBlockCount() - curDataBlockCount;
    m_testBufMgr->UnlockAndRelease(headBuf);
    UpdatePagesFreeSpace(segment, addDataPageCount - 1, 0);
    PageId targetPage = segment->GetPageFromFsm(1, 0);
    ASSERT_NE(targetPage, INVALID_PAGE_ID);
    ReadBufferAndPage(targetPage, LW_SHARED, dataBuf, dataPage);
    uint16 expectSlot = dataPage->GetFsmIndex().slot;
    m_testBufMgr->UnlockAndRelease(dataBuf);
    ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_SHARED, headBuf, headPage);
    PageId fsmMetaPageId = headPage->fsmInfos[0].fsmMetaPageId;
    ReadBufferAndPage(fsmMetaPageId, LW_SHARED, fsmMetaPageBuf, fsmMetaPage);
    ASSERT_EQ(fsmMetaPage->GetFsmRootLevel(), 2);
    PageId level0Fsm = fsmMetaPage->currMap[0];
    PageId level1Fsm = fsmMetaPage->currMap[1];
    PageId level2Fsm = fsmMetaPage->currMap[2];
    ASSERT_NE(level2Fsm, INVALID_PAGE_ID);
    m_testBufMgr->UnlockAndRelease(fsmMetaPageBuf);
    m_testBufMgr->UnlockAndRelease(headBuf);
    ReadBufferAndPage(level2Fsm, LW_SHARED, fsmBuf, fsmPage);
    ASSERT_EQ(fsmPage->FsmNodePtr(1)->page, level1Fsm);
    ASSERT_EQ(fsmPage->FsmNodePtr(1)->listId, 8);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->first, 1);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->count, 1);
    m_testBufMgr->UnlockAndRelease(fsmBuf);
    ReadBufferAndPage(level1Fsm, LW_SHARED, fsmBuf, fsmPage);
    ASSERT_EQ(fsmPage->fsmPageHeader.upperIndex.page, level2Fsm);
    ASSERT_EQ(fsmPage->fsmPageHeader.upperIndex.slot, 1);
    ASSERT_EQ(fsmPage->FsmNodePtr(0)->page, level0Fsm);
    ASSERT_EQ(fsmPage->FsmNodePtr(0)->listId, 8);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->first, 0);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->count, 1);
    m_testBufMgr->UnlockAndRelease(fsmBuf);
    ReadBufferAndPage(level0Fsm, LW_SHARED, fsmBuf, fsmPage);
    ASSERT_EQ(fsmPage->fsmPageHeader.upperIndex.page, level1Fsm);
    ASSERT_EQ(fsmPage->fsmPageHeader.upperIndex.slot, 0);
    m_testBufMgr->UnlockAndRelease(fsmBuf);

    /* Update targetPage to free space 1024, level0Fsm and level1Fsm move from list 5 to list 2 */
    rc = segment->UpdateFsm(targetPage, 1024);
    ASSERT_EQ(rc, 0);
    ReadBufferAndPage(level1Fsm, LW_SHARED, fsmBuf, fsmPage);
    ASSERT_EQ(fsmPage->FsmNodePtr(0)->listId, 5);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->first, INVALID_FSM_SLOT_NUM);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->count, 0);
    ASSERT_EQ(fsmPage->FsmListPtr(5)->first, 0);
    ASSERT_EQ(fsmPage->FsmListPtr(5)->count, 1);
    m_testBufMgr->UnlockAndRelease(fsmBuf);
    ReadBufferAndPage(level2Fsm, LW_SHARED, fsmBuf, fsmPage);
    ASSERT_EQ(fsmPage->FsmNodePtr(1)->listId, 5);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->first, INVALID_FSM_SLOT_NUM);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->count, 0);
    ASSERT_EQ(fsmPage->FsmListPtr(5)->first, 1);
    ASSERT_EQ(fsmPage->FsmListPtr(5)->count, 1);
    m_testBufMgr->UnlockAndRelease(fsmBuf);

    segment->DropSegment();
    delete segment;
}

TEST_F(DataSegmentTest, GetNewPageIncreaseFsmLevel003_level0)
{
    int rc;
    HeapNormalSegment *segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment));
    BufferDesc *fsmBuf, *dataBuf, *headBuf, *fsmMetaPageBuf;
    FsmPage *fsmPage;
    HeapPage *dataPage;
    HeapSegmentMetaPage *headPage;
    FreeSpaceMapMetaPage *fsmMetaPage;

    /* Call GetNewPage(), set all new pages to 0 space and simulate level-2 FSM tree is full */
    PageId newPage = segment->GetNewPage();
    ASSERT_NE(newPage, INVALID_PAGE_ID);
    ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_SHARED, headBuf, headPage);
    uint64 curDataBlockCount = headPage->GetDataBlockCount();
    m_testBufMgr->UnlockAndRelease(headBuf);
    UpdatePagesFreeSpace(segment, curDataBlockCount, 0);
    SimulateFsmLevelFull(2, segment);

    /* FSM level increase to 3 */
    newPage = segment->GetNewPage();
    ASSERT_NE(newPage, INVALID_PAGE_ID);
    ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_SHARED, headBuf, headPage);
    uint64 addDataPageCount = headPage->GetDataBlockCount() - curDataBlockCount;
    m_testBufMgr->UnlockAndRelease(headBuf);
    UpdatePagesFreeSpace(segment, addDataPageCount - 1, 0);
    PageId targetPage = segment->GetPageFromFsm(1, 0);
    ASSERT_NE(targetPage, INVALID_PAGE_ID);
    ReadBufferAndPage(targetPage, LW_SHARED, dataBuf, dataPage);
    uint16 expectSlot = dataPage->GetFsmIndex().slot;
    m_testBufMgr->UnlockAndRelease(dataBuf);
    ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_SHARED, headBuf, headPage);
    PageId fsmMetaPageId = headPage->fsmInfos[0].fsmMetaPageId;
    ReadBufferAndPage(fsmMetaPageId, LW_SHARED, fsmMetaPageBuf, fsmMetaPage);
    ASSERT_EQ(fsmMetaPage->GetFsmRootLevel(), 3);
    PageId level0Fsm = fsmMetaPage->currMap[0];
    PageId level1Fsm = fsmMetaPage->currMap[1];
    PageId level2Fsm = fsmMetaPage->currMap[2];
    PageId level3Fsm = fsmMetaPage->currMap[3];
    ASSERT_NE(level3Fsm, INVALID_PAGE_ID);
    m_testBufMgr->UnlockAndRelease(fsmMetaPageBuf);
    m_testBufMgr->UnlockAndRelease(headBuf);
    ReadBufferAndPage(level3Fsm, LW_SHARED, fsmBuf, fsmPage);
    ASSERT_EQ(fsmPage->FsmNodePtr(1)->page, level2Fsm);
    ASSERT_EQ(fsmPage->FsmNodePtr(1)->listId, 8);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->first, 1);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->count, 1);
    m_testBufMgr->UnlockAndRelease(fsmBuf);
    ReadBufferAndPage(level2Fsm, LW_SHARED, fsmBuf, fsmPage);
    ASSERT_EQ(fsmPage->fsmPageHeader.upperIndex.page, level3Fsm);
    ASSERT_EQ(fsmPage->fsmPageHeader.upperIndex.slot, 1);
    ASSERT_EQ(fsmPage->FsmNodePtr(0)->page, level1Fsm);
    ASSERT_EQ(fsmPage->FsmNodePtr(0)->listId, 8);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->first, 0);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->count, 1);
    m_testBufMgr->UnlockAndRelease(fsmBuf);
    ReadBufferAndPage(level1Fsm, LW_SHARED, fsmBuf, fsmPage);
    ASSERT_EQ(fsmPage->fsmPageHeader.upperIndex.page, level2Fsm);
    ASSERT_EQ(fsmPage->fsmPageHeader.upperIndex.slot, 0);
    ASSERT_EQ(fsmPage->FsmNodePtr(0)->page, level0Fsm);
    ASSERT_EQ(fsmPage->FsmNodePtr(0)->listId, 8);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->first, 0);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->count, 1);
    m_testBufMgr->UnlockAndRelease(fsmBuf);
    ReadBufferAndPage(level0Fsm, LW_SHARED, fsmBuf, fsmPage);
    ASSERT_EQ(fsmPage->fsmPageHeader.upperIndex.page, level1Fsm);
    ASSERT_EQ(fsmPage->fsmPageHeader.upperIndex.slot, 0);
    m_testBufMgr->UnlockAndRelease(fsmBuf);

    /* Update targetPage to free space 2048, level0Fsm, level1Fsm and level2Fsm move from list 5 to list 3 */
    rc = segment->UpdateFsm(targetPage, 2048);
    ASSERT_EQ(rc, 0);
    ReadBufferAndPage(level1Fsm, LW_SHARED, fsmBuf, fsmPage);
    ASSERT_EQ(fsmPage->FsmNodePtr(0)->listId, 6);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->first, INVALID_FSM_SLOT_NUM);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->count, 0);
    ASSERT_EQ(fsmPage->FsmListPtr(6)->first, 0);
    ASSERT_EQ(fsmPage->FsmListPtr(6)->count, 1);
    m_testBufMgr->UnlockAndRelease(fsmBuf);
    ReadBufferAndPage(level2Fsm, LW_SHARED, fsmBuf, fsmPage);
    ASSERT_EQ(fsmPage->FsmNodePtr(0)->listId, 6);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->first, INVALID_FSM_SLOT_NUM);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->count, 0);
    ASSERT_EQ(fsmPage->FsmListPtr(6)->first, 0);
    ASSERT_EQ(fsmPage->FsmListPtr(6)->count, 1);
    m_testBufMgr->UnlockAndRelease(fsmBuf);
    ReadBufferAndPage(level3Fsm, LW_SHARED, fsmBuf, fsmPage);
    ASSERT_EQ(fsmPage->FsmNodePtr(1)->listId, 6);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->first, INVALID_FSM_SLOT_NUM);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->count, 0);
    ASSERT_EQ(fsmPage->FsmListPtr(6)->first, 1);
    ASSERT_EQ(fsmPage->FsmListPtr(6)->count, 1);
    m_testBufMgr->UnlockAndRelease(fsmBuf);

    segment->DropSegment();
    delete segment;
}

TEST_F(DataSegmentTest, GetNewPageIncreaseFsmLevel004_level0)
{
    int rc;
    HeapNormalSegment *segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment));
    BufferDesc *fsmBuf, *dataBuf, *headBuf, *fsmMetaPageBuf;
    FsmPage *fsmPage;
    HeapPage *dataPage;
    HeapSegmentMetaPage *headPage;
    FreeSpaceMapMetaPage *fsmMetaPage;

    /* Call GetNewPage(), set all new pages to 0 space and simulate level-3 FSM tree is full */
    PageId newPage = segment->GetNewPage();
    ASSERT_NE(newPage, INVALID_PAGE_ID);
    ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_SHARED, headBuf, headPage);
    uint64 curDataBlockCount = headPage->GetDataBlockCount();
    m_testBufMgr->UnlockAndRelease(headBuf);
    UpdatePagesFreeSpace(segment, curDataBlockCount, 0);
    SimulateFsmLevelFull(3, segment);

    /* FSM level increase to 4 */
    newPage = segment->GetNewPage();
    ASSERT_NE(newPage, INVALID_PAGE_ID);
    ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_SHARED, headBuf, headPage);
    uint64 addDataPageCount = headPage->GetDataBlockCount() - curDataBlockCount;
    m_testBufMgr->UnlockAndRelease(headBuf);
    UpdatePagesFreeSpace(segment, addDataPageCount - 1, 0);
    PageId targetPage = segment->GetPageFromFsm(1, 0);
    ASSERT_NE(targetPage, INVALID_PAGE_ID);
    ReadBufferAndPage(targetPage, LW_SHARED, dataBuf, dataPage);
    uint16 expectSlot = dataPage->GetFsmIndex().slot;
    m_testBufMgr->UnlockAndRelease(dataBuf);
    ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_SHARED, headBuf, headPage);
    PageId fsmMetaPageId = headPage->fsmInfos[0].fsmMetaPageId;
    ReadBufferAndPage(fsmMetaPageId, LW_SHARED, fsmMetaPageBuf, fsmMetaPage);
    ASSERT_EQ(fsmMetaPage->GetFsmRootLevel(), 4);
    PageId level0Fsm = fsmMetaPage->currMap[0];
    PageId level1Fsm = fsmMetaPage->currMap[1];
    PageId level2Fsm = fsmMetaPage->currMap[2];
    PageId level3Fsm = fsmMetaPage->currMap[3];
    PageId level4Fsm = fsmMetaPage->currMap[4];
    ASSERT_NE(level4Fsm, INVALID_PAGE_ID);
    m_testBufMgr->UnlockAndRelease(fsmMetaPageBuf);
    m_testBufMgr->UnlockAndRelease(headBuf);
    ReadBufferAndPage(level4Fsm, LW_SHARED, fsmBuf, fsmPage);
    ASSERT_EQ(fsmPage->FsmNodePtr(1)->page, level3Fsm);
    ASSERT_EQ(fsmPage->FsmNodePtr(1)->listId, 8);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->first, 1);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->count, 1);
    m_testBufMgr->UnlockAndRelease(fsmBuf);
    ReadBufferAndPage(level3Fsm, LW_SHARED, fsmBuf, fsmPage);
    ASSERT_EQ(fsmPage->fsmPageHeader.upperIndex.page, level4Fsm);
    ASSERT_EQ(fsmPage->fsmPageHeader.upperIndex.slot, 1);
    ASSERT_EQ(fsmPage->FsmNodePtr(0)->page, level2Fsm);
    ASSERT_EQ(fsmPage->FsmNodePtr(0)->listId, 8);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->first, 0);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->count, 1);
    m_testBufMgr->UnlockAndRelease(fsmBuf);
    ReadBufferAndPage(level2Fsm, LW_SHARED, fsmBuf, fsmPage);
    ASSERT_EQ(fsmPage->fsmPageHeader.upperIndex.page, level3Fsm);
    ASSERT_EQ(fsmPage->fsmPageHeader.upperIndex.slot, 0);
    ASSERT_EQ(fsmPage->FsmNodePtr(0)->page, level1Fsm);
    ASSERT_EQ(fsmPage->FsmNodePtr(0)->listId, 8);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->first, 0);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->count, 1);
    m_testBufMgr->UnlockAndRelease(fsmBuf);
    ReadBufferAndPage(level1Fsm, LW_SHARED, fsmBuf, fsmPage);
    ASSERT_EQ(fsmPage->fsmPageHeader.upperIndex.page, level2Fsm);
    ASSERT_EQ(fsmPage->fsmPageHeader.upperIndex.slot, 0);
    ASSERT_EQ(fsmPage->FsmNodePtr(0)->page, level0Fsm);
    ASSERT_EQ(fsmPage->FsmNodePtr(0)->listId, 8);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->first, 0);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->count, 1);
    m_testBufMgr->UnlockAndRelease(fsmBuf);
    ReadBufferAndPage(level0Fsm, LW_SHARED, fsmBuf, fsmPage);
    ASSERT_EQ(fsmPage->fsmPageHeader.upperIndex.page, level1Fsm);
    ASSERT_EQ(fsmPage->fsmPageHeader.upperIndex.slot, 0);
    m_testBufMgr->UnlockAndRelease(fsmBuf);

    /* Update targetPage to free space 4096, level0Fsm, level1Fsm, level2Fsm and level3Fsm move from list 5 to list 4 */
    rc = segment->UpdateFsm(targetPage, 4096);
    ASSERT_EQ(rc, 0);
    ReadBufferAndPage(level1Fsm, LW_SHARED, fsmBuf, fsmPage);
    ASSERT_EQ(fsmPage->FsmNodePtr(0)->listId, 7);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->first, INVALID_FSM_SLOT_NUM);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->count, 0);
    ASSERT_EQ(fsmPage->FsmListPtr(7)->first, 0);
    ASSERT_EQ(fsmPage->FsmListPtr(7)->count, 1);
    m_testBufMgr->UnlockAndRelease(fsmBuf);
    ReadBufferAndPage(level2Fsm, LW_SHARED, fsmBuf, fsmPage);
    ASSERT_EQ(fsmPage->FsmNodePtr(0)->listId, 7);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->first, INVALID_FSM_SLOT_NUM);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->count, 0);
    ASSERT_EQ(fsmPage->FsmListPtr(7)->first, 0);
    ASSERT_EQ(fsmPage->FsmListPtr(7)->count, 1);
    m_testBufMgr->UnlockAndRelease(fsmBuf);
    ReadBufferAndPage(level3Fsm, LW_SHARED, fsmBuf, fsmPage);
    ASSERT_EQ(fsmPage->FsmNodePtr(0)->listId, 7);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->first, INVALID_FSM_SLOT_NUM);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->count, 0);
    ASSERT_EQ(fsmPage->FsmListPtr(7)->first, 0);
    ASSERT_EQ(fsmPage->FsmListPtr(7)->count, 1);
    m_testBufMgr->UnlockAndRelease(fsmBuf);
    ReadBufferAndPage(level4Fsm, LW_SHARED, fsmBuf, fsmPage);
    ASSERT_EQ(fsmPage->FsmNodePtr(1)->listId, 7);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->first, INVALID_FSM_SLOT_NUM);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->count, 0);
    ASSERT_EQ(fsmPage->FsmListPtr(7)->first, 1);
    ASSERT_EQ(fsmPage->FsmListPtr(7)->count, 1);
    m_testBufMgr->UnlockAndRelease(fsmBuf);

    segment->DropSegment();
    delete segment;
}

TEST_F(DataSegmentTest, GetNewPageIncreaseFsmLevel005_level0)
{
    /* If level-4 FSM tree is full, could not get new page in this segment */
    HeapNormalSegment *segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment));
    BufferDesc *headBuf, *fsmMetaPageBuf;
    HeapSegmentMetaPage *headPage;
    FreeSpaceMapMetaPage *fsmMetaPage;

    /* Add 1 new page with 0 space and simulate level-4 FSM tree is full */
    PageId newPage = segment->GetNewPage();
    ASSERT_NE(newPage, INVALID_PAGE_ID);
    SimulateFsmLevelFull(4, segment);
    ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_SHARED, headBuf, headPage);
    PageId fsmMetaPageId = headPage->fsmInfos[0].fsmMetaPageId;
    ReadBufferAndPage(fsmMetaPageId, LW_SHARED, fsmMetaPageBuf, fsmMetaPage);
    ASSERT_EQ(fsmMetaPage->GetFsmRootLevel(), 4);
    m_testBufMgr->UnlockAndRelease(fsmMetaPageBuf);
    m_testBufMgr->UnlockAndRelease(headBuf);

    /* If level-4 FSM tree is full, could not get new page in this segment */
    newPage = segment->GetNewPage();
    ASSERT_EQ(newPage, INVALID_PAGE_ID);
    ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_SHARED, headBuf, headPage);
    fsmMetaPageId = headPage->fsmInfos[0].fsmMetaPageId;
    ReadBufferAndPage(fsmMetaPageId, LW_SHARED, fsmMetaPageBuf, fsmMetaPage);
    ASSERT_EQ(fsmMetaPage->GetFsmRootLevel(), 4);
    m_testBufMgr->UnlockAndRelease(fsmMetaPageBuf);
    m_testBufMgr->UnlockAndRelease(headBuf);
    segment->DropSegment();
    delete segment;
}

TEST_F(DataSegmentTest, GetNewPageIncreaseExtentLevel001_level0)
{
    HeapNormalSegment *segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment));
    /* Set current extent count to 16, next extent size should be 1M */
    BufferDesc *headBuf;
    HeapSegmentMetaPage *headPage;
    ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_EXCLUSIVE, headBuf, headPage);
    headPage->segmentHeader.extents.count = 16;
    m_testBufMgr->UnlockAndRelease(headBuf);
    uint64 currentExtCount = 16;

    for (;;) {
        PageId newPage = segment->GetNewPage();
        ASSERT_NE(newPage, INVALID_PAGE_ID);
        ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_SHARED, headBuf, headPage);
        uint64 curExtCount = headPage->GetExtentCount();
        m_testBufMgr->UnlockAndRelease(headBuf);
        if (headPage->GetExtentCount() > currentExtCount) {
            break;
        }
    }
    ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_SHARED, headBuf, headPage);
    PageId lastExtent = headPage->GetLastExtent();
    PageId extEndPage = headPage->extendedPageId;
    m_testBufMgr->UnlockAndRelease(headBuf);
    BufferDesc *extentBuf = m_testBufMgr->Read(g_defaultPdbId, lastExtent, LW_SHARED);
    auto *extentPage = (SegExtentMetaPage *)extentBuf->GetPage();
    ASSERT_EQ(extentPage->extentMeta.extSize, EXT_SIZE_128);
    m_testBufMgr->UnlockAndRelease(extentBuf);
    PageId endPage = {lastExtent.m_fileId, lastExtent.m_blockId + EXT_SIZE_128 - 1};
    ASSERT_EQ(extEndPage, endPage);

    segment->DropSegment();
    delete segment;
}

TEST_F(DataSegmentTest, GetNewPageIncreaseExtentLevel002_level0)
{
    HeapNormalSegment *segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment));
    /* Set current extent count to 144, next extent size should be 8M */
    BufferDesc *headBuf;
    HeapSegmentMetaPage *headPage;
    ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_EXCLUSIVE, headBuf, headPage);
    headPage->segmentHeader.extents.count = 144;
    m_testBufMgr->UnlockAndRelease(headBuf);
    uint64 currentExtCount = 144;

    for (;;) {
        PageId newPage = segment->GetNewPage();
        ASSERT_NE(newPage, INVALID_PAGE_ID);
        ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_SHARED, headBuf, headPage);
        uint64 curExtCount = headPage->GetExtentCount();
        m_testBufMgr->UnlockAndRelease(headBuf);
        if (headPage->GetExtentCount() > currentExtCount) {
            break;
        }
    }
    ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_SHARED, headBuf, headPage);
    PageId lastExtent = headPage->GetLastExtent();
    PageId extEndPage = headPage->extendedPageId;
    m_testBufMgr->UnlockAndRelease(headBuf);
    BufferDesc *extentBuf = m_testBufMgr->Read(g_defaultPdbId, lastExtent, LW_SHARED);
    auto *extentPage = (SegExtentMetaPage *)extentBuf->GetPage();
    ASSERT_EQ(extentPage->extentMeta.extSize, EXT_SIZE_1024);
    m_testBufMgr->UnlockAndRelease(extentBuf);
    PageId endPage = {lastExtent.m_fileId, lastExtent.m_blockId + EXT_SIZE_1024 - 1};
    ASSERT_EQ(extEndPage, endPage);

    segment->DropSegment();
    delete segment;
}

TEST_F(DataSegmentTest, GetNewPageIncreaseExtentLevel003_level0)
{
    HeapNormalSegment *segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment));
    /* Set current extent count to 272, next extent size should be 64M */
    BufferDesc *headBuf;
    HeapSegmentMetaPage *headPage;
    ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_EXCLUSIVE, headBuf, headPage);
    headPage->segmentHeader.extents.count = 272;
    m_testBufMgr->UnlockAndRelease(headBuf);
    uint64 currentExtCount = 272;

    /* New page is added to fsm list 5 */
    PageId newPage = segment->GetNewPage();
    ASSERT_NE(newPage, INVALID_PAGE_ID);
    /* 1 seg meta, 1 fsm meta, 1 fsm root at initialization */
    uint16 expectAddPageCount = DstoreMin(FIRST_EXT_SIZE - 3, PAGES_ADD_TO_FSM_PER_TIME);
    for (;;) {
        PageId newPage = segment->GetNewPage();
        ASSERT_NE(newPage, INVALID_PAGE_ID);
        ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_SHARED, headBuf, headPage);
        uint64 curExtCount = headPage->GetExtentCount();
        m_testBufMgr->UnlockAndRelease(headBuf);
        if (headPage->GetExtentCount() > currentExtCount) {
            break;
        }
    }

    ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_SHARED, headBuf, headPage);
    PageId lastExtent = headPage->GetLastExtent();
    PageId extEndPage = headPage->extendedPageId;
    m_testBufMgr->UnlockAndRelease(headBuf);
    BufferDesc *extentBuf = m_testBufMgr->Read(g_defaultPdbId, lastExtent, LW_SHARED);
    auto *extentPage = (SegExtentMetaPage *)extentBuf->GetPage();
    ASSERT_EQ(extentPage->extentMeta.extSize, EXT_SIZE_8192);
    m_testBufMgr->UnlockAndRelease(extentBuf);
    PageId endPage = {lastExtent.m_fileId, lastExtent.m_blockId + EXT_SIZE_8192 - 1};
    ASSERT_EQ(extEndPage, endPage);

    segment->DropSegment();
    delete segment;
}

/**
 * GetPageFromFsm: Check Invalid parameter.
 */
TEST_F(DataSegmentTest, GetPageFromFsmTest001_level0)
{
    HeapNormalSegment *dataSegment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(dataSegment));
    /* Ensure that an available page exists in FSM. */
    dataSegment->GetNewPage();
    ASSERT_NE(dataSegment->GetPageFromFsm(0, 0), INVALID_PAGE_ID);
    ASSERT_NE(dataSegment->GetPageFromFsm(BLCKSZ, 0), INVALID_PAGE_ID);
    /* invalid parameter. */
    ASSERT_EQ(dataSegment->GetPageFromFsm(BLCKSZ + 1, 0), INVALID_PAGE_ID);
    dataSegment->DropSegment();
    delete dataSegment;
}

/**
 * GetPageFromFsm:
 */
TEST_F(DataSegmentTest, GetPageFromFsmTest002_level0)
{
    HeapNormalSegment *test_segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(test_segment));
    BufferDesc *headBuf, *fsmMetaPageBuf;
    HeapSegmentMetaPage *headPage;
    FreeSpaceMapMetaPage *fsmMetaPage;
    ReadBufferAndPage(test_segment->GetSegmentMetaPageId(), LW_SHARED, headBuf, headPage);
    PageId fsmMetaPageId = headPage->fsmInfos[0].fsmMetaPageId;
    ASSERT_NE(fsmMetaPageId, INVALID_PAGE_ID);
    ReadBufferAndPage(fsmMetaPageId, LW_SHARED, fsmMetaPageBuf, fsmMetaPage);
    PageId fsm_root = fsmMetaPage->GetFsmRoot();
    m_testBufMgr->UnlockAndRelease(fsmMetaPageBuf);
    m_testBufMgr->UnlockAndRelease(headBuf);
    BufferDesc *test_fsm_buf;
    FsmPage *test_fsm_page;

    /* FSM has no page at first */
    PageId page_from_fsm = test_segment->GetPageFromFsm(5000, 0);
    ASSERT_EQ(page_from_fsm, INVALID_PAGE_ID);

    test_fsm_buf = m_testBufMgr->Read(g_defaultPdbId, fsm_root, LW_SHARED);
    test_fsm_page = (FsmPage*)test_fsm_buf->GetPage();
    ASSERT_EQ(test_fsm_page->fsmPageHeader.hwm, 0);
    m_testBufMgr->UnlockAndRelease(test_fsm_buf);

    /* Call get_new_page to add page to FSM, and test get_page_from_fsm interface */
    PageId newPage = test_segment->GetNewPage();
    ASSERT_NE(newPage, INVALID_PAGE_ID);
    ReadBufferAndPage(test_segment->GetSegmentMetaPageId(), LW_SHARED, headBuf, headPage);
    uint64 addedPageCount = headPage->GetDataBlockCount();
    m_testBufMgr->UnlockAndRelease(headBuf);
    page_from_fsm = test_segment->GetPageFromFsm(5000, 0);
    ASSERT_NE(page_from_fsm, INVALID_PAGE_ID);
    test_fsm_buf = m_testBufMgr->Read(g_defaultPdbId, fsm_root, LW_SHARED);
    test_fsm_page = (FsmPage*)test_fsm_buf->GetPage();
    ASSERT_EQ(test_fsm_page->fsmPageHeader.hwm, addedPageCount);
    ASSERT_EQ(test_fsm_page->FsmListPtr(8)->count, addedPageCount);
    ASSERT_EQ(test_fsm_page->FsmListPtr(8)->first, addedPageCount - 1);
    ASSERT_EQ(test_fsm_page->FsmNodePtr(0)->page, newPage);
    m_testBufMgr->UnlockAndRelease(test_fsm_buf);

    test_segment->DropSegment();
    delete test_segment;
}

/**
 * GetPageFromFsm:
 * 1. if FSM has pages meeting the requirements of different free space, return the valid PageId;
 * 2. if FSM has no page meeting condition, return INVALID_PAGE_ID.
 */
TEST_F(DataSegmentTest, GetPageFromFsmTest003_level0)
{
    HeapNormalSegment *segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment));
    PageId newPage = segment->GetNewPage();
    ASSERT_NE(newPage, INVALID_PAGE_ID);
    uint16 sizeList[] = {500, 1000, 2000, 4000, 8000};
    PageId pageFromFsm{};
    for (auto &size : sizeList) {
        pageFromFsm = segment->GetPageFromFsm(size, 0);
        ASSERT_NE(pageFromFsm, INVALID_PAGE_ID);
    }
    BufferDesc *headBuf = m_testBufMgr->Read(g_defaultPdbId, segment->GetSegmentMetaPageId(), LW_SHARED);
    HeapSegmentMetaPage *headPage = (HeapSegmentMetaPage *)headBuf->GetPage();
    uint64 curDataBlockCount = headPage->GetDataBlockCount();
    m_testBufMgr->UnlockAndRelease(headBuf);
    UpdatePagesFreeSpace(segment, curDataBlockCount, 0);
    for (auto &size : sizeList) {
        pageFromFsm = segment->GetPageFromFsm(size, 0);
        ASSERT_EQ(pageFromFsm, INVALID_PAGE_ID);
    }
    segment->DropSegment();
    delete segment;
}

/**
 * GetPageFromFsm:
 * 1. if FSM has more than one page meeting condition, Multiple Invocations GetPageFromFsm, return one of the PageIds;
 * 2. if FSM has only one page meeting condition, Multiple Invocations GetPageFromFsm, return the pageId.
 */
TEST_F(DataSegmentTest, GetPageFromFsmTest004_level0)
{
    HeapNormalSegment *segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment));
    /* add PAGES_ADD_TO_FSM_PER_TIME pages to fsm */
    PageId newPage = segment->GetNewPage();
    ASSERT_NE(newPage, INVALID_PAGE_ID);

    PageId pageIds[PAGES_ADD_TO_FSM_PER_TIME];
    /* Multiple Invocations GetPageFromFsm */
    for (int i = 0; i < PAGES_ADD_TO_FSM_PER_TIME; i++) {
        pageIds[i] = segment->GetPageFromFsm(BLCKSZ, 0);
    }
    bool allSame = true;
    for (int i = 1; i < PAGES_ADD_TO_FSM_PER_TIME; i++) {
        allSame = allSame && (pageIds[0] == pageIds[i]);
    }
    ASSERT_FALSE(allSame);

    BufferDesc *headBuf = m_testBufMgr->Read(g_defaultPdbId, segment->GetSegmentMetaPageId(), LW_SHARED);
    HeapSegmentMetaPage *headPage = (HeapSegmentMetaPage *)headBuf->GetPage();
    uint64 curDataBlockCount = headPage->GetDataBlockCount();
    m_testBufMgr->UnlockAndRelease(headBuf);
    /* after change curDataBlockCount - 1 pages' free space ,only one page meet condition */
    UpdatePagesFreeSpace(segment, curDataBlockCount - 1, 0);

    /* Multiple Invocations GetPageFromFsm */
    for (int i = 0; i < PAGES_ADD_TO_FSM_PER_TIME; i++) {
        pageIds[i] = segment->GetPageFromFsm(BLCKSZ, 0);
    }
    allSame = true;
    for (int i = 1; i < PAGES_ADD_TO_FSM_PER_TIME; i++) {
        allSame = allSame && (pageIds[0] == pageIds[i]);
    }
    ASSERT_TRUE(allSame);

    segment->DropSegment();
    delete segment;
}

/**
 * @tc.name:  GetPageFromFsmTest005_level0
 * @tc.desc:  1.GetNewPage() to get some new pages
 *            2.GetPageFromFsm to get page with BLCKSZ, and UpdateFsm() to free space less than BLCKSZ
 *            3.Call GetPageFromFsm to get page with 1 size, check return spaceInFsm value
 * @tc.type: FUNC
 * @tc.require:
 * @tc.author: liangyanzhao
*/
TEST_F(DataSegmentTest, GetPageFromFsmTest005_level0)
{
    HeapNormalSegment *segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment));
    ASSERT_NE(segment->GetNewPage(), INVALID_PAGE_ID);
    ASSERT_NE(segment->GetNewPage(), INVALID_PAGE_ID);

    uint64 totalDataBlock = segment->GetDataBlockCount();
    uint32 spaceList[FSM_FREE_LIST_COUNT - 2];
    uint32 spaceListCount = FSM_FREE_LIST_COUNT - 2;
    ASSERT_GT(spaceListCount, 0);
    for (int i = 0; i < FSM_FREE_LIST_COUNT - 2; ++i) {
        spaceList[i] = FSM_SPACE_LINE[i + 1];
    }
    std::vector<PageId> pageIdList;
    std::vector<uint32> remainSpaceList;

    for (uint64 i = 0; i < totalDataBlock; ++i) {
        uint32 spaceInFsm = 0;
        PageId pageId = segment->GetPageFromFsm(BLCKSZ, 0, &spaceInFsm);
        ASSERT_NE(pageId, INVALID_PAGE_ID);
        ASSERT_EQ(spaceInFsm, BLCKSZ);
        pageIdList.push_back(pageId);
        ASSERT_EQ(segment->UpdateFsm(pageId, spaceList[i % spaceListCount]), 0);
        remainSpaceList.push_back(spaceList[i % spaceListCount]);
    }
    ASSERT_EQ(segment->GetPageFromFsm(BLCKSZ, 0), INVALID_PAGE_ID);

    for (uint64 i = 0; i < totalDataBlock; ++i) {
        uint32 spaceInFsm = 0;
        PageId pageId = segment->GetPageFromFsm(1, 0, &spaceInFsm);
        ASSERT_NE(pageId, INVALID_PAGE_ID);
        uint32 j = 0;
        for (; j < pageIdList.size(); ++j) {
            if (pageIdList[j] == pageId) {
                ASSERT_EQ(spaceInFsm, remainSpaceList[j]);
                break;
            }
        }
        ASSERT_NE(j, pageIdList.size());
    }

    segment->DropSegment();
    delete segment;
}

/**
 * @tc.name:  GetPageFromFsmTest006_level0
 * @tc.desc:  1.GetNewPage() to get some new pages, UpdateFsm() to 1024 space
 *            2.GetPageFromFsm() to get 1024 space pages, with retry time increasing
 *            3.Retry time exceed threshold, return INVALID_PAGE_ID
 * @tc.type: FUNC
 * @tc.require:
 * @tc.author: liangyanzhao
*/
TEST_F(DataSegmentTest, GetPageFromFsmTest006_level0)
{
    HeapNormalSegment *segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment));
    ASSERT_NE(segment->GetNewPage(), INVALID_PAGE_ID);

    /* Set all data pages to 1024 space */
    for (;;) {
        PageId pageId = segment->GetPageFromFsm(BLCKSZ, 0);
        if (pageId == INVALID_PAGE_ID) {
            break;
        }
        ASSERT_EQ(segment->UpdateFsm(pageId, 1024), 0);
    }

    for (uint16 retryTime = 0; retryTime < m_retryUpgradeThreshold; ++retryTime) {
        PageId pageId = segment->GetPageFromFsm(1024, retryTime);
        ASSERT_NE(pageId, INVALID_PAGE_ID);
    }
    ASSERT_EQ(segment->GetPageFromFsm(1024, m_retryUpgradeThreshold), INVALID_PAGE_ID);

    PageId firstPage = segment->GetFirstDataPage();
    ASSERT_NE(firstPage, INVALID_PAGE_ID);
    ASSERT_EQ(segment->UpdateFsm(firstPage, 1024 * 2), 0);
    ASSERT_EQ(segment->GetPageFromFsm(1024, m_retryUpgradeThreshold), firstPage);
    ASSERT_EQ(segment->UpdateFsm(firstPage, 1024), 0);
    ASSERT_EQ(segment->GetPageFromFsm(1024, m_retryUpgradeThreshold), INVALID_PAGE_ID);

    segment->DropSegment();
    delete segment;
}

/**
 * @tc.name:  GetPageFromFsmTest007_level0
 * @tc.desc:  1.GetNewPage() to get some new pages, UpdateFsm() to 1024 or 2048 space
 *            2.GetPageFromFsm() to get 1024 space pages with retryTime = threshold, return page with 1024 or 2048 space
 *            3.GetPageFromFsm() to get 1024 space pages with retryTime > threshold, return page with space
 * @tc.type: FUNC
 * @tc.require:
 * @tc.author: liangyanzhao
*/
TEST_F(DataSegmentTest, GetPageFromFsmTest007_level0)
{
    HeapNormalSegment *segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment));
    ASSERT_NE(segment->GetNewPage(), INVALID_PAGE_ID);
    ASSERT_GE(segment->GetDataBlockCount(), 2);

    /* Set all data pages to 1024 or 2048 space */
    std::vector<PageId> pageList;
    std::vector<uint32> spaceList;
    int index = 0;
    for (;;) {
        PageId pageId = segment->GetPageFromFsm(BLCKSZ, 0);
        if (pageId == INVALID_PAGE_ID) {
            break;
        }
        uint32 space = 1024;
        if (index % 2 != 0) {
            space *= 2;
        }
        FsmIndex fsmIndex;
        ReadPageForFsmIndex(pageId, LW_SHARED, &fsmIndex);
        ASSERT_EQ(segment->UpdateFsm(fsmIndex, space), 0);
        pageList.push_back(pageId);
        spaceList.push_back(space);
        index++;
    }

    for (uint32 i = 0; i < segment->GetDataBlockCount(); ++i) {
        uint32 spaceInFsm = 0;
        PageId pageId = segment->GetPageFromFsm(1024, m_retryUpgradeThreshold, &spaceInFsm);
        ASSERT_NE(pageId, INVALID_PAGE_ID);
        ASSERT_TRUE(spaceInFsm == 1024 || spaceInFsm == 2048);
        uint32 j = 0;
        for (; j < pageList.size(); ++j) {
            if (pageList[j] == pageId) {
                ASSERT_EQ(spaceInFsm, spaceList[j]);
                break;
            }
        }
        ASSERT_NE(j, pageList.size());
    }

    for (uint32 i = 0; i < segment->GetDataBlockCount(); ++i) {
        uint32 spaceInFsm = 0;
        PageId pageId = segment->GetPageFromFsm(1024, m_retryUpgradeThreshold + 1, &spaceInFsm);
        ASSERT_NE(pageId, INVALID_PAGE_ID);
        ASSERT_EQ(spaceInFsm, 2048);
        uint32 j = 0;
        for (; j < pageList.size(); ++j) {
            if (pageList[j] == pageId) {
                ASSERT_EQ(spaceInFsm, spaceList[j]);
                break;
            }
        }
        ASSERT_NE(j, pageList.size());
    }

    segment->DropSegment();
    delete segment;
}

TEST_F(DataSegmentTest, GetPageFromFsmMultiPage001_Levle0)
{
    int rc;
    HeapNormalSegment *segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment));
    HeapSegmentMetaPage *headPage;
    BufferDesc *headBuf;
    uint64 targetPageCount = 200;
    ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_SHARED, headBuf, headPage);
    uint64 curDataBlockCount = headPage->GetDataBlockCount();
    m_testBufMgr->UnlockAndRelease(headBuf);
    while (curDataBlockCount < targetPageCount) {
        PageId newPage = segment->GetNewPage();
        ASSERT_NE(newPage, INVALID_PAGE_ID);
        ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_SHARED, headBuf, headPage);
        curDataBlockCount = headPage->GetDataBlockCount();
        m_testBufMgr->UnlockAndRelease(headBuf);
    }
    PageId pageFromFsm{};
    ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_SHARED, headBuf, headPage);
    curDataBlockCount = headPage->GetDataBlockCount();
    m_testBufMgr->UnlockAndRelease(headBuf);
    for (uint64 i = 0; i < curDataBlockCount; ++i) {
        pageFromFsm = segment->GetPageFromFsm(1, 0);
        ASSERT_NE(pageFromFsm, INVALID_PAGE_ID);
        rc = segment->UpdateFsm(pageFromFsm, 0);
        ASSERT_EQ(rc, 0);
    }
    pageFromFsm = segment->GetPageFromFsm(1, 0);
    ASSERT_EQ(pageFromFsm, INVALID_PAGE_ID);

    segment->DropSegment();
    delete segment;
}

TEST_F(DataSegmentTest, GetPageFromFsmSpecfiedPage001_Levle0)
{
    int rc;
    HeapNormalSegment *segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment));
    HeapSegmentMetaPage *headPage;
    BufferDesc *headBuf;
    uint64 targetPageCount = 200;
    ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_SHARED, headBuf, headPage);
    uint64 curDataBlockCount = headPage->GetDataBlockCount();
    m_testBufMgr->UnlockAndRelease(headBuf);
    while (curDataBlockCount < targetPageCount) {
        PageId newPage = segment->GetNewPage();
        ASSERT_NE(newPage, INVALID_PAGE_ID);
        ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_SHARED, headBuf, headPage);
        curDataBlockCount = headPage->GetDataBlockCount();
        m_testBufMgr->UnlockAndRelease(headBuf);
    }
    ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_SHARED, headBuf, headPage);
    curDataBlockCount = headPage->GetDataBlockCount();
    m_testBufMgr->UnlockAndRelease(headBuf);
    PageId targetPage = segment->GetPageFromFsm(BLCKSZ, 0);
    ASSERT_NE(targetPage, INVALID_PAGE_ID);
    rc = segment->UpdateFsm(targetPage, 1);
    ASSERT_EQ(rc, 0);
    PageId pageFromFsm{};
    for (uint64 i = 0; i < curDataBlockCount - 1; ++i) {
        pageFromFsm = segment->GetPageFromFsm(BLCKSZ, 0);
        ASSERT_NE(pageFromFsm, INVALID_PAGE_ID);
        rc = segment->UpdateFsm(pageFromFsm, 0);
        ASSERT_EQ(rc, 0);
    }
    pageFromFsm = segment->GetPageFromFsm(BLCKSZ, 0);
    ASSERT_EQ(pageFromFsm, INVALID_PAGE_ID);
    pageFromFsm = segment->GetPageFromFsm(1, 0);
    ASSERT_EQ(pageFromFsm, targetPage);

    segment->DropSegment();
    delete segment;
}

/**
 * UpdateFsm: basic function.
 */
TEST_F(DataSegmentTest, UpdateFsmBasic001_level0)
{
    int rc;
    HeapNormalSegment *segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment));
    BufferDesc *headBuf, *fsmMetaPageBuf;
    HeapSegmentMetaPage *headPage;
    FreeSpaceMapMetaPage *fsmMetaPage;
    ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_SHARED, headBuf, headPage);
    PageId fsmMetaPageId = headPage->fsmInfos[0].fsmMetaPageId;
    ReadBufferAndPage(fsmMetaPageId, LW_SHARED, fsmMetaPageBuf, fsmMetaPage);
    PageId leafFsm = fsmMetaPage->GetFsmRoot();
    m_testBufMgr->UnlockAndRelease(fsmMetaPageBuf);
    m_testBufMgr->UnlockAndRelease(headBuf);
    BufferDesc *fsmBuf, *dataBuf;
    FsmPage *fsmPage;
    HeapPage *dataPage;

    /* Add pages to FSM */
    PageId newPage = segment->GetNewPage();
    ASSERT_NE(newPage, INVALID_PAGE_ID);
    ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_SHARED, headBuf, headPage);
    uint64 currentDataBlock = headPage->GetDataBlockCount();
    m_testBufMgr->UnlockAndRelease(headBuf);
    PageId pageFromFsm{};
    for (uint64 i = 0; i < currentDataBlock - 1; ++i) {
        pageFromFsm = segment->GetPageFromFsm(1, 0);
        ASSERT_NE(pageFromFsm, INVALID_PAGE_ID);
        rc = segment->UpdateFsm(pageFromFsm, 0);
        ASSERT_EQ(rc, 0);
    }

    /* Decrease free space of testPage to 2000, move from fsm list 8 to 6 */
    PageId testPage = segment->GetPageFromFsm(1, 0);
    ASSERT_NE(testPage, INVALID_PAGE_ID);
    ReadBufferAndPage(testPage, LW_SHARED, dataBuf, dataPage);
    uint16 pageSlot = dataPage->GetFsmIndex().slot;
    m_testBufMgr->UnlockAndRelease(dataBuf);
    rc = segment->UpdateFsm(testPage, 2000);
    ASSERT_EQ(rc, 0);
    ReadBufferAndPage(leafFsm, LW_SHARED, fsmBuf, fsmPage);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->count, 0);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->first, INVALID_FSM_SLOT_NUM);
    ASSERT_EQ(fsmPage->FsmListPtr(6)->count, 1);
    ASSERT_EQ(fsmPage->FsmListPtr(6)->first, pageSlot);
    m_testBufMgr->UnlockAndRelease(fsmBuf);

    /* Increase free space of testPage to 3000, move from fsm list 6 to 7 */
    rc = segment->UpdateFsm(testPage, 3000);
    ASSERT_EQ(rc, 0);
    fsmBuf = m_testBufMgr->Read(g_defaultPdbId, leafFsm, LW_SHARED);
    fsmPage = (FsmPage*)fsmBuf->GetPage();
    ASSERT_EQ(fsmPage->FsmListPtr(6)->count, 0);
    ASSERT_EQ(fsmPage->FsmListPtr(6)->first, INVALID_FSM_SLOT_NUM);
    ASSERT_EQ(fsmPage->FsmListPtr(7)->count, 1);
    ASSERT_EQ(fsmPage->FsmListPtr(7)->first, pageSlot);
    m_testBufMgr->UnlockAndRelease(fsmBuf);

    /* FSM has 0 page in fsm list 8 (4096, 8192], cannot get a page with 5000 free space in FSM */
    PageId page_from_fsm = segment->GetPageFromFsm(5000, 0);
    ASSERT_EQ(page_from_fsm, INVALID_PAGE_ID);

    segment->DropSegment();
    delete segment;
}

TEST_F(DataSegmentTest, UpdateFsmConcurrently_level1)
{
    int rc;
    HeapNormalSegment *segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment));
    BufferDesc *headBuf, *fsmMetaPageBuf;
    HeapSegmentMetaPage *headPage;
    FreeSpaceMapMetaPage *fsmMetaPage;
    ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_SHARED, headBuf, headPage);
    PageId fsmMetaPageId = headPage->fsmInfos[0].fsmMetaPageId;
    ReadBufferAndPage(fsmMetaPageId, LW_SHARED, fsmMetaPageBuf, fsmMetaPage);
    PageId leafFsm = fsmMetaPage->GetFsmRoot();
    m_testBufMgr->UnlockAndRelease(fsmMetaPageBuf);
    m_testBufMgr->UnlockAndRelease(headBuf);
    BufferDesc *fsmBuf, *dataBuf;
    FsmPage *fsmPage;
    HeapPage *dataPage;

    /* Add pages to FSM */
    PageId newPage = segment->GetNewPage();
    ASSERT_NE(newPage, INVALID_PAGE_ID);
    ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_SHARED, headBuf, headPage);
    uint64 currentDataBlock = headPage->GetDataBlockCount();
    m_testBufMgr->UnlockAndRelease(headBuf);
    PageId pageFromFsm{};
    for (uint64 i = 0; i < currentDataBlock - 1; ++i) {
        pageFromFsm = segment->GetPageFromFsm(1, 0);
        ASSERT_NE(pageFromFsm, INVALID_PAGE_ID);
        rc = segment->UpdateFsm(pageFromFsm, 0);
        ASSERT_EQ(rc, 0);
    }

    /* Decrease free space of testPage to 2000, move from fsm list 8 to 6 */
    PageId testPage = segment->GetPageFromFsm(1, 0);
    ASSERT_NE(testPage, INVALID_PAGE_ID);
    ReadBufferAndPage(testPage, LW_SHARED, dataBuf, dataPage);
    uint16 pageSlot = dataPage->GetFsmIndex().slot;
    m_testBufMgr->UnlockAndRelease(dataBuf);

    FsmParam param;
    param.segment = segment;
    param.pageId = testPage;
    param.remainSpace = 2000;
 
    pthread_t t[2];
    for (uint16 i = 0; i < 2; i++) {
        pthread_create(&t[i], NULL, UpdateFsmInThread, &param);
    }
 
    for (uint16 i = 0; i < 2; i++) {
        pthread_join(t[i], NULL);
    }

    ReadBufferAndPage(leafFsm, LW_SHARED, fsmBuf, fsmPage);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->count, 0);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->first, INVALID_FSM_SLOT_NUM);
    ASSERT_EQ(fsmPage->FsmListPtr(6)->count, 1);
    ASSERT_EQ(fsmPage->FsmListPtr(6)->first, pageSlot);
    m_testBufMgr->UnlockAndRelease(fsmBuf);

    segment->DropSegment();
    delete segment;
}

/**
 * UpdateFsmAndSearch: basic function.
 */
TEST_F(DataSegmentTest, UpdateFsmAndSearchBasic001_level0)
{
    RetStatus rc;
    HeapNormalSegment *segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment));
    BufferDesc *headBuf, *fsmMetaPageBuf;
    HeapSegmentMetaPage *headPage;
    FreeSpaceMapMetaPage *fsmMetaPage;
    ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_SHARED, headBuf, headPage);
    PageId fsmMetaPageId = headPage->fsmInfos[0].fsmMetaPageId;
    ReadBufferAndPage(fsmMetaPageId, LW_SHARED, fsmMetaPageBuf, fsmMetaPage);
    PageId leafFsm = fsmMetaPage->GetFsmRoot();
    m_testBufMgr->UnlockAndRelease(fsmMetaPageBuf);
    m_testBufMgr->UnlockAndRelease(headBuf);
    BufferDesc *fsmBuf, *dataBuf;
    FsmPage *fsmPage;
    HeapPage *dataPage;

    /* Add pages to FSM */
    PageId newPage = segment->GetNewPage();
    ASSERT_NE(newPage, INVALID_PAGE_ID);
    ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_SHARED, headBuf, headPage);
    uint64 currentDataBlock = headPage->GetDataBlockCount();
    m_testBufMgr->UnlockAndRelease(headBuf);

    FsmIndex fsmIndex;
    PageId pageFromFsm{};
    PageId searchResultPageId;
    uint32 spaceInFsm = 0;
    for (uint64 i = 0; i < currentDataBlock - 1; ++i) {
        pageFromFsm = segment->GetPageFromFsm(1, 0);
        ASSERT_NE(pageFromFsm, INVALID_PAGE_ID);
        ReadPageForFsmIndex(pageFromFsm, LW_SHARED, &fsmIndex);
        rc = segment->UpdateFsmAndSearch(fsmIndex, 0, BLCKSZ, 0, &searchResultPageId, &spaceInFsm);
        ASSERT_NE(searchResultPageId, INVALID_PAGE_ID);
        ASSERT_EQ(rc, DSTORE_SUCC);
    }

    /* Decrease free space of testPage to 2000, move from fsm list 8 to 6 */
    PageId testPage = segment->GetPageFromFsm(1, 0);
    ASSERT_NE(testPage, INVALID_PAGE_ID);
    ReadBufferAndPage(testPage, LW_SHARED, dataBuf, dataPage);
    uint16 pageSlot = dataPage->GetFsmIndex().slot;
    m_testBufMgr->UnlockAndRelease(dataBuf);
    rc = segment->UpdateFsmAndSearch(dataPage->GetFsmIndex(), 2000, BLCKSZ, 0, &searchResultPageId, &spaceInFsm);
    ASSERT_EQ(rc, DSTORE_SUCC);
    ASSERT_EQ(searchResultPageId, INVALID_PAGE_ID);

    ReadBufferAndPage(leafFsm, LW_SHARED, fsmBuf, fsmPage);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->count, 0);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->first, INVALID_FSM_SLOT_NUM);
    ASSERT_EQ(fsmPage->FsmListPtr(6)->count, 1);
    ASSERT_EQ(fsmPage->FsmListPtr(6)->first, pageSlot);
    m_testBufMgr->UnlockAndRelease(fsmBuf);

    /* Increase free space of testPage to 3000, move from fsm list 6 to 7 */
    rc = segment->UpdateFsmAndSearch(dataPage->GetFsmIndex(), 3000, 3000, 0, &searchResultPageId, &spaceInFsm);
    ASSERT_EQ(rc, DSTORE_SUCC);
    ASSERT_NE(searchResultPageId, INVALID_PAGE_ID);
    searchResultPageId = INVALID_PAGE_ID;
    rc = segment->UpdateFsmAndSearch(dataPage->GetFsmIndex(), 3000, 3000, 0, &searchResultPageId, &spaceInFsm);
    ASSERT_EQ(rc, DSTORE_SUCC);
    ASSERT_NE(searchResultPageId, INVALID_PAGE_ID);
    fsmBuf = m_testBufMgr->Read(g_defaultPdbId, leafFsm, LW_SHARED);
    fsmPage = (FsmPage*)fsmBuf->GetPage();
    ASSERT_EQ(fsmPage->FsmListPtr(6)->count, 0);
    ASSERT_EQ(fsmPage->FsmListPtr(6)->first, INVALID_FSM_SLOT_NUM);
    ASSERT_EQ(fsmPage->FsmListPtr(7)->count, 1);
    ASSERT_EQ(fsmPage->FsmListPtr(7)->first, pageSlot);
    m_testBufMgr->UnlockAndRelease(fsmBuf);

    /* FSM has 0 page in fsm list 8 (4096, 8192], cannot get a page with 5000 free space in FSM */
    PageId page_from_fsm = segment->GetPageFromFsm(5000, 0);
    ASSERT_EQ(page_from_fsm, INVALID_PAGE_ID);

    segment->DropSegment();
    delete segment;
}

/**
 * @tc.name:  UpdateFsmAndSearchBasic002_level0
 * @tc.desc:  1.Add new data pages until segment has 2 leaf fsm pages
 *            2.Use UpdateFsmAndSearch to set all data pages from first leaf fsm page to 0 space
 *            3.Expect phase 2 will repeat FSM_MAX_HWM, and all data pages in first leaf fsm has 0 space
 *            4.Call GetPageFromFsm with BLCKSZ, return valid page id from other leaf fsm page
 * @tc.type: FUNC
 * @tc.require:
 * @tc.author: liangyanzhao
*/
TEST_F(DataSegmentTest, UpdateFsmAndSearchBasic002_level0)
{
    RetStatus rc;
    HeapNormalSegment *segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment));

    /* GetNewPage until DataSegment has at least 2 leaf fsm pages */
    while (segment->GetDataBlockCount() <= FSM_MAX_HWM) {
        ASSERT_NE(segment->GetNewPage(), INVALID_PAGE_ID);
    }

    /* First DataPage is in first fsm page, which has FSM_MAX_HWM data pages of full space */
    PageId curPageId = segment->GetFirstDataPage();
    ASSERT_NE(curPageId, INVALID_PAGE_ID);
    BufferDesc *buf;
    HeapPage *dataPage;
    std::vector<PageId> pageIdList;
    uint32 spaceInFsm = 0;
    while (curPageId != INVALID_PAGE_ID) {
        pageIdList.push_back(curPageId);
        ReadBufferAndPage(curPageId, LW_SHARED, buf, dataPage);
        FsmIndex curFsmIndex = dataPage->GetFsmIndex();
        m_testBufMgr->UnlockAndRelease(buf);
        ASSERT_EQ(segment->UpdateFsmAndSearch(curFsmIndex, 0, BLCKSZ, 0, &curPageId, &spaceInFsm), 0);
    }
    ASSERT_EQ(pageIdList.size(), FSM_MAX_HWM);

    /* All data pages in first leaf fsm page have 0 space,
     * but there are other data pages in other leaf fsm pages have full space */
    int retryTime = 100;
    while (retryTime--) {
        PageId newPageId = segment->GetPageFromFsm(BLCKSZ, 0);
        ASSERT_NE(newPageId, INVALID_PAGE_ID);
        for (auto &pageId : pageIdList) {
            ASSERT_NE(newPageId, pageId);
        }
    }

    segment->DropSegment();
    delete segment;
}

/**
 * @tc.name:  UpdateFsmAndSearchBasic003_level0
 * @tc.desc:  1.Call GetNewPage() to get a new page, set to curPageId, get curAddDataBlock for this GetNewPage() call
 *            2.Call GetDataBlockCount to get total data block count
 *            3.Call UpdateFsmAndSearch to set curPageId to 0 space and search page with BLCKSZ space
 *            4.Expect phase 3 will repeat curAddDataBlock time
 *            5.Repeat 1-4 until total data block count reach FSM_MAX_HWM
 * @tc.type: FUNC
 * @tc.require:
 * @tc.author: liangyanzhao
*/
TEST_F(DataSegmentTest, UpdateFsmAndSearchBasic003_level0)
{
    RetStatus rc;
    HeapNormalSegment *segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment));

    PageId curPageId = segment->GetNewPage();
    ASSERT_NE(curPageId, INVALID_PAGE_ID);
    uint64 curDataBlockCount = segment->GetDataBlockCount();
    uint64 curAddDataBlock = curDataBlockCount;

    while (curDataBlockCount <= FSM_MAX_HWM) {
        FsmIndex fsmIndex;
        uint16 searchTime = 0;
        uint32 spaceInFsm = 0;
        while (curPageId != INVALID_PAGE_ID) {
            ReadPageForFsmIndex(curPageId, LW_SHARED, &fsmIndex);
            ASSERT_EQ(segment->UpdateFsmAndSearch(fsmIndex, 0, BLCKSZ, 0, &curPageId, &spaceInFsm), 0);
            searchTime++;
        }
        ASSERT_EQ(searchTime, curAddDataBlock);
        curPageId = segment->GetNewPage();
        uint16 newCurDataBlock = segment->GetDataBlockCount();
        curAddDataBlock = newCurDataBlock - curDataBlockCount;
        curDataBlockCount = newCurDataBlock;
    }

    segment->DropSegment();
    delete segment;
}

/**
  *@tc.name:  UpdateFsmBountry001
  *@tc.desc:  1.将Segment中所有的Page空闲度置为0；
  *           2.获取空闲度为1的Page，预期返回INVALID_PAGE_ID；
  *           3.将1个Page的空闲度改为BLCKSZ，查询空闲度BLCKSZ的Page，预期两个Page的PageId一样。
 * @tc.type: FUNC
 * @tc. require:
  *@tc.author: liangyanzhao
*/
TEST_F(DataSegmentTest, UpdateFsmBountry001_level0)
{
    int rc;
    HeapNormalSegment *segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment));
    BufferDesc *headBuf, *fsmMetaPageBuf;
    HeapSegmentMetaPage *headPage;
    FreeSpaceMapMetaPage *fsmMetaPage;
    ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_SHARED, headBuf, headPage);
    PageId fsmMetaPageId = headPage->fsmInfos[0].fsmMetaPageId;
    ReadBufferAndPage(fsmMetaPageId, LW_SHARED, fsmMetaPageBuf, fsmMetaPage);
    PageId leafFsm = fsmMetaPage->GetFsmRoot();
    m_testBufMgr->UnlockAndRelease(fsmMetaPageBuf);
    m_testBufMgr->UnlockAndRelease(headBuf);
    BufferDesc *fsmBuf;
    FsmPage *fsmPage;
    PageId newPage = segment->GetNewPage();
    ASSERT_NE(newPage, INVALID_PAGE_ID);
    ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_SHARED, headBuf, headPage);
    uint64 currentDataBlock = headPage->GetDataBlockCount();
    m_testBufMgr->UnlockAndRelease(headBuf);
    PageId pageFromFsm{};
    for (uint64 i = 0; i < currentDataBlock; ++i) {
        pageFromFsm = segment->GetPageFromFsm(1, 0);
        ASSERT_NE(pageFromFsm, INVALID_PAGE_ID);
        rc = segment->UpdateFsm(pageFromFsm, 0);
        ASSERT_EQ(rc, 0);
    }

    rc = segment->UpdateFsm(newPage, 0);
    ASSERT_EQ(rc, 0);
    pageFromFsm = segment->GetPageFromFsm(1, 0);
    ASSERT_EQ(pageFromFsm, INVALID_PAGE_ID);
    rc = segment->UpdateFsm(newPage, BLCKSZ);
    ASSERT_EQ(rc, 0);
    pageFromFsm = segment->GetPageFromFsm(BLCKSZ, 0);
    ASSERT_EQ(pageFromFsm, newPage);

    segment->DropSegment();
    delete segment;
}

TEST_F(DataSegmentTest, UpdateFsmUpperBoundTest_level0)
{
    RetStatus rc;
    HeapNormalSegment *segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment));
    BufferDesc *headBuf, *fsmMetaPageBuf;
    HeapSegmentMetaPage *headPage;
    FreeSpaceMapMetaPage *fsmMetaPage;
    ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_SHARED, headBuf, headPage);
    PageId fsmMetaPageId = headPage->fsmInfos[0].fsmMetaPageId;
    ReadBufferAndPage(fsmMetaPageId, LW_SHARED, fsmMetaPageBuf, fsmMetaPage);
    PageId leafFsm = fsmMetaPage->GetFsmRoot();
    m_testBufMgr->UnlockAndRelease(fsmMetaPageBuf);
    m_testBufMgr->UnlockAndRelease(headBuf);
    BufferDesc *fsmBuf, *dataBuf;
    FsmPage *fsmPage;
    HeapPage *dataPage;

    /* New page is added to fsm list 5 */
    PageId newPage = segment->GetNewPage();
    ASSERT_NE(newPage, INVALID_PAGE_ID);
    /* 1 seg meta, 1 fsm meta, 1 fsm root at initialization */
    uint16 expectAddPageCount = DstoreMin(FIRST_EXT_SIZE - 3, PAGES_ADD_TO_FSM_PER_TIME);
    ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_SHARED, headBuf, headPage);
    uint64 curDataBlockCount = headPage->GetDataBlockCount();
    ASSERT_EQ(expectAddPageCount, curDataBlockCount);
    m_testBufMgr->UnlockAndRelease(headBuf);
    UpdatePagesFreeSpace(segment, curDataBlockCount - 1, 0);
    newPage = segment->GetPageFromFsm(1, 0);
    ASSERT_NE(newPage, INVALID_PAGE_ID);
    ReadBufferAndPage(newPage, LW_SHARED, dataBuf, dataPage);
    uint16 expectSlot = dataPage->GetFsmIndex().slot;
    m_testBufMgr->UnlockAndRelease(dataBuf);
    ReadBufferAndPage(leafFsm, LW_SHARED, fsmBuf, fsmPage);
    ASSERT_EQ(fsmPage->FsmNodePtr(expectSlot)->page, newPage);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->count, 1);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->first, expectSlot);
    m_testBufMgr->UnlockAndRelease(fsmBuf);

    /* Update page free space to 0, move from fsm list 5 to fsm list 0 */
    rc = segment->UpdateFsm(newPage, 0);
    ASSERT_EQ(rc, 0);
    ReadBufferAndPage(leafFsm, LW_SHARED, fsmBuf, fsmPage);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->count, 0);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->first, INVALID_FSM_SLOT_NUM);
    ASSERT_EQ(fsmPage->FsmListPtr(0)->count, expectAddPageCount);
    ASSERT_EQ(fsmPage->FsmListPtr(0)->first, expectSlot);
    m_testBufMgr->UnlockAndRelease(fsmBuf);

    /* Update page free space to 512, move from fsm list 0 to fsm list 1 */
    rc = segment->UpdateFsm(newPage, 512);
    ASSERT_EQ(rc, 0);
    ReadBufferAndPage(leafFsm, LW_SHARED, fsmBuf, fsmPage);
    ASSERT_EQ(fsmPage->FsmListPtr(0)->count, expectAddPageCount - 1);
    ASSERT_EQ(fsmPage->FsmListPtr(4)->count, 1);
    ASSERT_EQ(fsmPage->FsmListPtr(4)->first, expectSlot);
    m_testBufMgr->UnlockAndRelease(fsmBuf);

    /* Update page free space to 1024, move from fsm list 1 to fsm list 2 */
    rc = segment->UpdateFsm(newPage, 1024);
    ASSERT_EQ(rc, 0);
    ReadBufferAndPage(leafFsm, LW_SHARED, fsmBuf, fsmPage);
    ASSERT_EQ(fsmPage->FsmListPtr(4)->count, 0);
    ASSERT_EQ(fsmPage->FsmListPtr(4)->first, INVALID_FSM_SLOT_NUM);
    ASSERT_EQ(fsmPage->FsmListPtr(5)->count, 1);
    ASSERT_EQ(fsmPage->FsmListPtr(5)->first, expectSlot);
    m_testBufMgr->UnlockAndRelease(fsmBuf);

    /* Update page free space to 2048, move from fsm list 2 to fsm list 3 */
    rc = segment->UpdateFsm(newPage, 2048);
    ASSERT_EQ(rc, 0);
    ReadBufferAndPage(leafFsm, LW_SHARED, fsmBuf, fsmPage);
    ASSERT_EQ(fsmPage->FsmListPtr(5)->count, 0);
    ASSERT_EQ(fsmPage->FsmListPtr(5)->first, INVALID_FSM_SLOT_NUM);
    ASSERT_EQ(fsmPage->FsmListPtr(6)->count, 1);
    ASSERT_EQ(fsmPage->FsmListPtr(6)->first, expectSlot);
    m_testBufMgr->UnlockAndRelease(fsmBuf);

    /* Update page free space to 4096, move from fsm list 3 to fsm list 4 */
    rc = segment->UpdateFsm(newPage, 4096);
    ASSERT_EQ(rc, 0);
    ReadBufferAndPage(leafFsm, LW_SHARED, fsmBuf, fsmPage);
    ASSERT_EQ(fsmPage->FsmListPtr(6)->count, 0);
    ASSERT_EQ(fsmPage->FsmListPtr(6)->first, INVALID_FSM_SLOT_NUM);
    ASSERT_EQ(fsmPage->FsmListPtr(7)->count, 1);
    ASSERT_EQ(fsmPage->FsmListPtr(7)->first, expectSlot);
    m_testBufMgr->UnlockAndRelease(fsmBuf);

    /* Update page free space to 8192, move from fsm list 4 to fsm list 5 */
    rc = segment->UpdateFsm(newPage, 8192);
    ASSERT_EQ(rc, 0);
    ReadBufferAndPage(leafFsm, LW_SHARED, fsmBuf, fsmPage);
    ASSERT_EQ(fsmPage->FsmListPtr(7)->count, 0);
    ASSERT_EQ(fsmPage->FsmListPtr(7)->first, INVALID_FSM_SLOT_NUM);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->count, 1);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->first, expectSlot);
    m_testBufMgr->UnlockAndRelease(fsmBuf);

    segment->DropSegment();
    delete segment;
}

TEST_F(DataSegmentTest, UpdateFsmLowerBoundTest_level1)
{
    int rc;
    HeapNormalSegment *segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment));
    BufferDesc *headBuf, *fsmMetaPageBuf;
    HeapSegmentMetaPage *headPage;
    FreeSpaceMapMetaPage *fsmMetaPage;
    ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_SHARED, headBuf, headPage);
    PageId fsmMetaPageId = headPage->fsmInfos[0].fsmMetaPageId;
    ReadBufferAndPage(fsmMetaPageId, LW_SHARED, fsmMetaPageBuf, fsmMetaPage);
    PageId leafFsm = fsmMetaPage->GetFsmRoot();
    m_testBufMgr->UnlockAndRelease(fsmMetaPageBuf);
    m_testBufMgr->UnlockAndRelease(headBuf);
    BufferDesc *fsmBuf, *dataBuf;
    FsmPage *fsmPage;
    HeapPage *dataPage;

    /* New page is added to fsm list 5 */
    PageId newPage = segment->GetNewPage();
    ASSERT_NE(newPage, INVALID_PAGE_ID);
    ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_SHARED, headBuf, headPage);
    uint64 curDataBlockCount = headPage->GetDataBlockCount();
    m_testBufMgr->UnlockAndRelease(headBuf);
    UpdatePagesFreeSpace(segment, curDataBlockCount - 1, 0);
    newPage = segment->GetPageFromFsm(1, 0);
    ASSERT_NE(newPage, INVALID_PAGE_ID);
    ReadBufferAndPage(newPage, LW_SHARED, dataBuf, dataPage);
    uint16 expectSlot = dataPage->GetFsmIndex().slot;
    m_testBufMgr->UnlockAndRelease(dataBuf);
    ASSERT_NE(newPage, INVALID_PAGE_ID);
    ReadBufferAndPage(leafFsm, LW_SHARED, fsmBuf, fsmPage);
    ASSERT_EQ(fsmPage->FsmNodePtr(expectSlot)->page, newPage);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->count, 1);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->first, expectSlot);
    m_testBufMgr->UnlockAndRelease(fsmBuf);

    /* Update page free space to 1, move from fsm list 5 to fsm list 1 */
    rc = segment->UpdateFsm(newPage, 1);
    ASSERT_EQ(rc, 0);
    ReadBufferAndPage(leafFsm, LW_SHARED, fsmBuf, fsmPage);
    ASSERT_EQ(fsmPage->FsmListPtr(1)->count, 1);
    ASSERT_EQ(fsmPage->FsmListPtr(1)->first, expectSlot);
    m_testBufMgr->UnlockAndRelease(fsmBuf);

    /* Update page free space to 513, move from fsm list 1 to fsm list 2 */
    rc = segment->UpdateFsm(newPage, 513);
    ASSERT_EQ(rc, 0);
    ReadBufferAndPage(leafFsm, LW_SHARED, fsmBuf, fsmPage);
    ASSERT_EQ(fsmPage->FsmListPtr(4)->count, 0);
    ASSERT_EQ(fsmPage->FsmListPtr(4)->first, INVALID_FSM_SLOT_NUM);
    ASSERT_EQ(fsmPage->FsmListPtr(5)->count, 1);
    ASSERT_EQ(fsmPage->FsmListPtr(5)->first, expectSlot);
    m_testBufMgr->UnlockAndRelease(fsmBuf);

    /* Update page free space to 1025, move from fsm list 2 to fsm list 3 */
    rc = segment->UpdateFsm(newPage, 1025);
    ASSERT_EQ(rc, 0);
    ReadBufferAndPage(leafFsm, LW_SHARED, fsmBuf, fsmPage);
    ASSERT_EQ(fsmPage->FsmListPtr(5)->count, 0);
    ASSERT_EQ(fsmPage->FsmListPtr(5)->first, INVALID_FSM_SLOT_NUM);
    ASSERT_EQ(fsmPage->FsmListPtr(6)->count, 1);
    ASSERT_EQ(fsmPage->FsmListPtr(6)->first, expectSlot);
    m_testBufMgr->UnlockAndRelease(fsmBuf);

    /* Update page free space to 5121, move from fsm list 3 to fsm list 4 */
    rc = segment->UpdateFsm(newPage, 5121);
    ASSERT_EQ(rc, 0);
    ReadBufferAndPage(leafFsm, LW_SHARED, fsmBuf, fsmPage);
    ASSERT_EQ(fsmPage->FsmListPtr(6)->count, 0);
    ASSERT_EQ(fsmPage->FsmListPtr(6)->first, INVALID_FSM_SLOT_NUM);
    ASSERT_EQ(fsmPage->FsmListPtr(7)->count, 1);
    ASSERT_EQ(fsmPage->FsmListPtr(7)->first, expectSlot);
    m_testBufMgr->UnlockAndRelease(fsmBuf);

    /* Update page free space to 4097, move from fsm list 4 to fsm list 5 */
    rc = segment->UpdateFsm(newPage, 4097);
    ASSERT_EQ(rc, 0);
    ReadBufferAndPage(leafFsm, LW_SHARED, fsmBuf, fsmPage);
    ASSERT_EQ(fsmPage->FsmListPtr(7)->count, 0);
    ASSERT_EQ(fsmPage->FsmListPtr(7)->first, INVALID_FSM_SLOT_NUM);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->count, 1);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->first, expectSlot);
    m_testBufMgr->UnlockAndRelease(fsmBuf);

    segment->DropSegment();
    delete segment;
}


/**
  *@tc.name:  UpdateFsmIncreaseLevel001
  *@tc.desc:  1.
 * @tc.type: FUNC
 * @tc. require:
  *@tc.author: liangyanzhao
*/
TEST_F(DataSegmentTest, UpdateFsmIncreaseLevel001_level0)
{
    HeapNormalSegment *segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment));
    uint64 current_block_count = 0;
    PageId newPage{};
    BufferDesc *fsmBuf, *headBuf, *fsmMetaPageBuf;
    FsmPage *fsmPage;
    HeapSegmentMetaPage *headPage;
    FreeSpaceMapMetaPage *fsmMetaPage;
    ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_SHARED, headBuf, headPage);
    PageId fsmMetaPageId = headPage->fsmInfos[0].fsmMetaPageId;
    ReadBufferAndPage(fsmMetaPageId, LW_SHARED, fsmMetaPageBuf, fsmMetaPage);
    uint16 curFsmLevel = fsmMetaPage->GetFsmRootLevel();
    ASSERT_EQ(curFsmLevel, 0);
    PageId oldLeafFsm = fsmMetaPage->currMap[0];
    ASSERT_EQ(oldLeafFsm, fsmMetaPage->GetFsmRoot());
    ASSERT_EQ(fsmMetaPage->mapCount[0], 1);
    m_testBufMgr->UnlockAndRelease(fsmMetaPageBuf);
    m_testBufMgr->UnlockAndRelease(headBuf);
    ReadBufferAndPage(oldLeafFsm, LW_SHARED, fsmBuf, fsmPage);
    ASSERT_EQ(fsmPage->GetUpperFsmPageId(), INVALID_PAGE_ID);
    m_testBufMgr->UnlockAndRelease(fsmBuf);

    /* Add pages to fsm until FSM increase to level-1 */
    while (curFsmLevel == 0) {
        newPage = segment->GetNewPage();
        ASSERT_NE(newPage, INVALID_PAGE_ID);
        ReadBufferAndPage(fsmMetaPageId, LW_SHARED, fsmMetaPageBuf, fsmMetaPage);
        curFsmLevel = fsmMetaPage->GetFsmRootLevel();
        m_testBufMgr->UnlockAndRelease(fsmMetaPageBuf);
    }

    ReadBufferAndPage(fsmMetaPageId, LW_SHARED, fsmMetaPageBuf, fsmMetaPage);
    ASSERT_EQ(fsmMetaPage->GetFsmRootLevel(), 1);
    PageId newRootFsm = fsmMetaPage->GetFsmRoot();
    ASSERT_NE(oldLeafFsm, newRootFsm);
    ASSERT_EQ(newRootFsm, fsmMetaPage->currMap[1]);
    PageId newLeafFsm = fsmMetaPage->currMap[0];
    ASSERT_NE(oldLeafFsm, newLeafFsm);
    ASSERT_EQ(fsmMetaPage->mapCount[0], 2);
    ASSERT_EQ(fsmMetaPage->mapCount[1], 1);
    m_testBufMgr->UnlockAndRelease(fsmMetaPageBuf);

    /*
     * oldLeafFsm will be added to slot-0 of newRootFsm
     * all heap pages in oldLeafFsm do not change free space
     */
    ReadBufferAndPage(oldLeafFsm, LW_SHARED, fsmBuf, fsmPage);
    ASSERT_EQ(fsmPage->GetUpperFsmPageId(), newRootFsm);
    ASSERT_EQ(fsmPage->fsmPageHeader.upperIndex.slot, 0);
    m_testBufMgr->UnlockAndRelease(fsmBuf);

    /*
     * newLeafFsm will be added to slot-1 of newRootFsm
     * newLeafFsm has one new heap page
     */
    ReadBufferAndPage(newLeafFsm, LW_SHARED, fsmBuf, fsmPage);
    ASSERT_EQ(fsmPage->GetUpperFsmPageId(), newRootFsm);
    ASSERT_EQ(fsmPage->fsmPageHeader.upperIndex.slot, 1);
    m_testBufMgr->UnlockContent(fsmBuf);
    m_testBufMgr->Release(fsmBuf);

    /*
     * newRootFsm has 2 child fsm page (oldLeafFsm and newLeafFsm) in list 5
     * oldLeafFsm in slot 0, newLeafFsm in slot 1
     * newRootFsm list 5 has 2 slots, first is slot 1 (newLeafFsm), second is slot 0 (oldLeafFsm)
     */
    ReadBufferAndPage(newRootFsm, LW_SHARED, fsmBuf, fsmPage);
    ASSERT_EQ(fsmPage->fsmPageHeader.hwm, 2);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->count, 2);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->first, 1);
    ASSERT_EQ(fsmPage->FsmNodePtr(1)->page, newLeafFsm);
    ASSERT_EQ(fsmPage->FsmNodePtr(1)->listId, 8);
    ASSERT_EQ(fsmPage->FsmNodePtr(1)->next, 0);
    ASSERT_EQ(fsmPage->FsmNodePtr(1)->prev, INVALID_FSM_SLOT_NUM);
    ASSERT_EQ(fsmPage->FsmNodePtr(0)->page, oldLeafFsm);
    ASSERT_EQ(fsmPage->FsmNodePtr(0)->listId, 8);
    ASSERT_EQ(fsmPage->FsmNodePtr(0)->next, INVALID_FSM_SLOT_NUM);
    ASSERT_EQ(fsmPage->FsmNodePtr(0)->prev, 1);
    m_testBufMgr->UnlockContent(fsmBuf);
    m_testBufMgr->Release(fsmBuf);

    segment->DropSegment();
    delete segment;
}

TEST_F(DataSegmentTest, UpdateFsmDecreaseFreeSpace001_level0)
{
    int rc;
    HeapNormalSegment *segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment));
    HeapSegmentMetaPage *headPage;
    FreeSpaceMapMetaPage *fsmMetaPage;
    BufferDesc *fsmBuf, *headBuf, *fsmMetaPageBuf;
    FsmPage *fsmPage;

    /* Call GetNewPage() and simulate level-0 FSM tree is full */
    PageId newPage = segment->GetNewPage();
    ASSERT_NE(newPage, INVALID_PAGE_ID);
    ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_SHARED, headBuf, headPage);
    uint64 oldDataBlock = headPage->GetDataBlockCount();
    m_testBufMgr->UnlockAndRelease(headBuf);
    UpdatePagesFreeSpace(segment, oldDataBlock, 0);
    SimulateFsmLevelFull(0, segment);
    uint64 curDataBlock = oldDataBlock;

    /* Add >= 2 new pages, FSM level increase to level-1 */
    while (curDataBlock - oldDataBlock < 2) {
        newPage = segment->GetNewPage();
        ASSERT_NE(newPage, INVALID_PAGE_ID);
        ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_SHARED, headBuf, headPage);
        curDataBlock = headPage->GetDataBlockCount();
        m_testBufMgr->UnlockAndRelease(headBuf);
    }
    ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_SHARED, headBuf, headPage);
    PageId fsmMetaPageId = headPage->fsmInfos[0].fsmMetaPageId;
    ReadBufferAndPage(fsmMetaPageId, LW_SHARED, fsmMetaPageBuf, fsmMetaPage);
    curDataBlock = headPage->GetDataBlockCount();
    PageId level0Fsm = fsmMetaPage->currMap[0];
    ASSERT_NE(level0Fsm, INVALID_PAGE_ID);
    PageId level1Fsm = fsmMetaPage->currMap[1];
    ASSERT_NE(level1Fsm, INVALID_PAGE_ID);
    m_testBufMgr->UnlockAndRelease(fsmMetaPageBuf);
    m_testBufMgr->UnlockAndRelease(headBuf);
    UpdatePagesFreeSpace(segment, curDataBlock - oldDataBlock - 2, 0);
    ReadBufferAndPage(level0Fsm, LW_SHARED, fsmBuf, fsmPage);
    uint16 level0UpperSlot = fsmPage->GetUpperSlot();
    PageId newPage1 = fsmPage->FsmNodePtr(fsmPage->FsmListPtr(8)->first)->page;
    PageId newPage2 = fsmPage->FsmNodePtr(fsmPage->FsmNodePtr(fsmPage->FsmListPtr(8)->first)->next)->page;
    m_testBufMgr->UnlockAndRelease(fsmBuf);
    ReadBufferAndPage(level1Fsm, LW_SHARED, fsmBuf, fsmPage);
    ASSERT_EQ(fsmPage->FsmNodePtr(level0UpperSlot)->listId, 8);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->count, 1);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->first, 1);
    m_testBufMgr->UnlockAndRelease(fsmBuf);

    /* Update newPage1 free space to 1024, FSM tree do not update (newPage2 has full free space) */
    rc = segment->UpdateFsm(newPage1, 1024);
    ASSERT_EQ(rc, 0);
    ReadBufferAndPage(level1Fsm, LW_SHARED, fsmBuf, fsmPage);
    ASSERT_EQ(fsmPage->FsmNodePtr(level0UpperSlot)->listId, 8);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->count, 1);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->first, 1);
    m_testBufMgr->UnlockAndRelease(fsmBuf);

    /* Update newPage2 free space to 0, level0Fsm move from list 5 to list 2 in level1Fsm (newPage1 has 1024 space) */
    rc = segment->UpdateFsm(newPage2, 0);
    ASSERT_EQ(rc, 0);
    ReadBufferAndPage(level1Fsm, LW_SHARED, fsmBuf, fsmPage);
    ASSERT_EQ(fsmPage->FsmNodePtr(level0UpperSlot)->listId, 5);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->count, 0);
    ASSERT_EQ(fsmPage->FsmListPtr(8)->first, INVALID_FSM_SLOT_NUM);
    ASSERT_EQ(fsmPage->FsmListPtr(5)->count, 1);
    ASSERT_EQ(fsmPage->FsmListPtr(5)->first, 1);
    m_testBufMgr->UnlockAndRelease(fsmBuf);

    segment->DropSegment();
    delete segment;
}

TEST_F(DataSegmentTest, UpdateFsmIncreaseFreeSpace001_level0)
{
    int rc;
    HeapNormalSegment *segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment));

    SmgrType smgrType = SmgrType::HEAP_SMGR;
    StorageRelationData tmpRel;
    tmpRel.tableSmgr = DstoreNew(m_ut_memory_context)
        TableStorageMgr(segment->GetPdbId(), DEFAULT_HEAP_FILLFACTOR, nullptr, SYS_RELPERSISTENCE_PERMANENT);
    tmpRel.SetTableSmgrSegment(dynamic_cast<HeapSegment *>(segment));

    DataSegmentScanContext segScanContext(g_storageInstance->GetBufferMgr(), &tmpRel, smgrType);
    HeapSegmentMetaPage *headPage;
    FreeSpaceMapMetaPage *fsmMetaPage;
    BufferDesc *fsmBuf, *dataBuf, *headBuf, *fsmMetaPageBuf;
    FsmPage *fsmPage;
    HeapPage *dataPage;

    ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_SHARED, headBuf, headPage);
    uint64 oldDataBlock = headPage->GetDataBlockCount();
    uint64 curDataBlock = oldDataBlock;
    m_testBufMgr->UnlockAndRelease(headBuf);

    /* Add >= 2 new pages and simulate level-0 FSM tree is full */
    PageId newPage{};
    while (curDataBlock - oldDataBlock < 2) {
        newPage = segment->GetNewPage();
        ASSERT_NE(newPage, INVALID_PAGE_ID);
        ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_SHARED, headBuf, headPage);
        curDataBlock = headPage->GetDataBlockCount();
        m_testBufMgr->UnlockAndRelease(headBuf);
    }
    ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_SHARED, headBuf, headPage);
    curDataBlock = headPage->GetDataBlockCount();
    m_testBufMgr->UnlockAndRelease(headBuf);
    UpdatePagesFreeSpace(segment, curDataBlock, 0);
    SimulateFsmLevelFull(0, segment);

    /* Get 1 new page, FSM increase to level-1 */
    newPage = segment->GetNewPage();
    ASSERT_NE(newPage, INVALID_PAGE_ID);
    ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_SHARED, headBuf, headPage);
    PageId fsmMetaPageId = headPage->fsmInfos[0].fsmMetaPageId;
    ReadBufferAndPage(fsmMetaPageId, LW_SHARED, fsmMetaPageBuf, fsmMetaPage);
    PageId level1Fsm = fsmMetaPage->currMap[1];
    m_testBufMgr->UnlockAndRelease(fsmMetaPageBuf);
    m_testBufMgr->UnlockAndRelease(headBuf);

    PageId page1 = segScanContext.GetNextPageId();
    ReadBufferAndPage(page1, LW_SHARED, dataBuf, dataPage);
    PageId page2 = segScanContext.GetNextPageId();
    ASSERT_NE(page2, INVALID_PAGE_ID);
    PageId level0Fsm = dataPage->GetFsmIndex().page;
    m_testBufMgr->UnlockAndRelease(dataBuf);
    ReadBufferAndPage(level0Fsm, LW_SHARED, fsmBuf, fsmPage);
    uint16 level0UpperSlot = fsmPage->GetUpperSlot();
    m_testBufMgr->UnlockAndRelease(fsmBuf);
    ReadBufferAndPage(level1Fsm, LW_SHARED, fsmBuf, fsmPage);
    ASSERT_EQ(fsmPage->FsmNodePtr(level0UpperSlot)->listId, 0);
    ASSERT_EQ(fsmPage->FsmListPtr(0)->count, 1);
    ASSERT_EQ(fsmPage->FsmListPtr(0)->first, 0);
    m_testBufMgr->UnlockAndRelease(fsmBuf);

    /* Update page1 to free space 1024, level0Fsm move from list 0 to list 2 in level1Fsm */
    rc = segment->UpdateFsm(page1, 1024);
    ASSERT_EQ(rc, 0);
    ReadBufferAndPage(level1Fsm, LW_SHARED, fsmBuf, fsmPage);
    ASSERT_EQ(fsmPage->FsmNodePtr(level0UpperSlot)->listId, 5);
    ASSERT_EQ(fsmPage->FsmListPtr(0)->count, 0);
    ASSERT_EQ(fsmPage->FsmListPtr(0)->first, INVALID_FSM_SLOT_NUM);
    ASSERT_EQ(fsmPage->FsmListPtr(5)->count, 1);
    ASSERT_EQ(fsmPage->FsmListPtr(5)->first, 0);
    m_testBufMgr->UnlockAndRelease(fsmBuf);

    /* Update page2 to free space 512, level0Fsm does not move */
    rc = segment->UpdateFsm(page2, 512);
    ASSERT_EQ(rc, 0);
    ReadBufferAndPage(level1Fsm, LW_SHARED, fsmBuf, fsmPage);
    ASSERT_EQ(fsmPage->FsmNodePtr(level0UpperSlot)->listId, 5);
    ASSERT_EQ(fsmPage->FsmListPtr(5)->count, 1);
    ASSERT_EQ(fsmPage->FsmListPtr(5)->first, 0);
    m_testBufMgr->UnlockAndRelease(fsmBuf);

    /* Update page2 to free space 2048, level0Fsm move from list 2 to list 3 in level1Fsm */
    rc = segment->UpdateFsm(page2, 2048);
    ASSERT_EQ(rc, 0);
    ReadBufferAndPage(level1Fsm, LW_SHARED, fsmBuf, fsmPage);
    ASSERT_EQ(fsmPage->FsmNodePtr(level0UpperSlot)->listId, 6);
    ASSERT_EQ(fsmPage->FsmListPtr(5)->count, 0);
    ASSERT_EQ(fsmPage->FsmListPtr(5)->first, INVALID_FSM_SLOT_NUM);
    ASSERT_EQ(fsmPage->FsmListPtr(6)->count, 1);
    ASSERT_EQ(fsmPage->FsmListPtr(6)->first, 0);
    m_testBufMgr->UnlockAndRelease(fsmBuf);

    segment->DropSegment();
    delete segment;
    tmpRel.SetTableSmgrSegment(nullptr);
    delete tmpRel.tableSmgr;
}

TEST_F(DataSegmentTest, UpdateFsmMoveFsmSlot001_level0)
{
    HeapNormalSegment *segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment));

    /* Add at least 2 new pages to FSM */
    ASSERT_NE(segment->GetNewPage(), INVALID_PAGE_ID);
    ASSERT_NE(segment->GetNewPage(), INVALID_PAGE_ID);
    HeapSegmentMetaPage *headPage;
    FreeSpaceMapMetaPage *fsmMetaPage;
    FsmPage *fsmPage;
    BufferDesc *bufferDesc, *fsmMetaPageBuf;
    ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_SHARED, bufferDesc, headPage);
    ReadBufferAndPage(headPage->fsmInfos[0].fsmMetaPageId, LW_SHARED,
                      fsmMetaPageBuf, fsmMetaPage);
    PageId leafFsm = fsmMetaPage->currMap[0];
    m_testBufMgr->UnlockAndRelease(fsmMetaPageBuf);
    m_testBufMgr->UnlockAndRelease(bufferDesc);

    /* Get first 2 slot PageId in FSM */
    ReadBufferAndPage(leafFsm, LW_SHARED, bufferDesc, fsmPage);
    ASSERT_GE(fsmPage->fsmPageHeader.hwm, 2);
    PageId page0 = fsmPage->FsmNodePtr(0)->page;
    PageId page1 = fsmPage->FsmNodePtr(1)->page;
    ASSERT_NE(page0, page1);
    m_testBufMgr->UnlockAndRelease(bufferDesc);

    /* set page1 free space to 0, check FSM page status */
    ASSERT_EQ(segment->UpdateFsm(page1, 0), 0);
    ReadBufferAndPage(leafFsm, LW_SHARED, bufferDesc, fsmPage);
    ASSERT_EQ(fsmPage->FsmListPtr(0)->first, 1);
    ASSERT_EQ(fsmPage->FsmNodePtr(1)->prev, INVALID_FSM_SLOT_NUM);
    m_testBufMgr->UnlockAndRelease(bufferDesc);

    /* set page0 free space to 0, page1's prev_slot points to page0's slot */
    ASSERT_EQ(segment->UpdateFsm(page0, 0), 0);
    ReadBufferAndPage(leafFsm, LW_SHARED, bufferDesc, fsmPage);
    ASSERT_EQ(fsmPage->FsmListPtr(0)->first, 0);
    ASSERT_EQ(fsmPage->FsmNodePtr(1)->prev, 0);
    m_testBufMgr->UnlockAndRelease(bufferDesc);

    segment->DropSegment();
    delete segment;
}

TEST_F(DataSegmentTest, GetPage001_level0)
{
    Transaction *trans = thrd->GetActiveTransaction();
    trans->Start();
    IndexNormalSegment *segment = UtAllocNewIndexSegment();
    ASSERT_TRUE(SegmentIsValid(segment));

    PageId segMetaPageId = segment->GetSegmentMetaPageId();
    for (int i = 1; i < EXT_SIZE_8 - 1; i++) {
        PageId pageId = segment->GetFromFreeQueue();
        ASSERT_NE(pageId, INVALID_PAGE_ID);
    }
    delete segment;
    trans->Commit();
}

TEST_F(DataSegmentTest, DISABLED_GetPageConcurrency001_level0)
{
    Transaction *trans = thrd->GetActiveTransaction();
    trans->Start();
    IndexNormalSegment *segment = UtAllocNewIndexSegment();
    ASSERT_TRUE(SegmentIsValid(segment));

    PageId segMetaPageId = segment->GetSegmentMetaPageId();
    PageId extentMetaPageId = segMetaPageId;
    extentMetaPageId.m_blockId += 1024;
    /* 启动10个线程，每个线程都连续获取100次调用GetPage，该过程会涉及到扩展Extent(EXT_SIZE_8192) */
    std::thread threads[10];
    for (uint32 i = 0; i < 10; i++) {
        threads[i] = std::thread(GetPageInThread, segment, i);
    }

    for (int i = 0; i < 10; i++) {
        threads[i].join();
    }

    /* 10个线程一共获取1000个Page，在第1个extent里面使用占用6个，在扩展的extent里面占用994个;再次GetPage获取第995个 */
    PageId newPageId = segment->GetFromFreeQueue();
    ASSERT_EQ(newPageId.m_blockId, extentMetaPageId.m_blockId + 995);
    delete segment;
    trans->Commit();
}

TEST_F(DataSegmentTest, AddPage001_level0)
{
    Transaction *trans = thrd->GetActiveTransaction();
    trans->Start();
    IndexNormalSegment *segment = UtAllocNewIndexSegment();
    ASSERT_TRUE(SegmentIsValid(segment));

    PageId pageId;
    for (int i = 1; i < EXT_SIZE_8 - 1; i++) {
        pageId = segment->GetFromFreeQueue();
        ASSERT_NE(pageId, INVALID_PAGE_ID);
    }

    for (int i = 1; i < EXT_SIZE_1024; i++) {
        pageId = segment->GetFromFreeQueue();
        ASSERT_NE(pageId, INVALID_PAGE_ID);
    }

    PageId segMetaPageId = segment->GetSegmentMetaPageId();
    BufferDesc *metaBuf = m_testBufMgr->Read(g_defaultPdbId, segMetaPageId, LW_SHARED);
    DataSegmentMetaPage *metaPage = (DataSegmentMetaPage *)metaBuf->GetPage();
    uint64 extentCount = metaPage->GetExtentCount();
    m_testBufMgr->UnlockContent(metaBuf);

    segment->PutIntoFreeQueue(pageId);
    /* From this point, we should NOT extend our extent. */
    PageId newPageId = segment->GetFromFreeQueue();
    ASSERT_NE(newPageId, INVALID_PAGE_ID);
    PageId nextPageId = {pageId.m_fileId, pageId.m_blockId + 1};
    ASSERT_EQ(newPageId, nextPageId);

    m_testBufMgr->LockContent(metaBuf, LW_SHARED);
    ASSERT_EQ(extentCount, metaPage->GetExtentCount());
    m_testBufMgr->UnlockAndRelease(metaBuf);
    delete segment;
    trans->Commit();
}

TEST_F(DataSegmentTest, AddPageConcurrency001_level0)
{
    Transaction *trans = thrd->GetActiveTransaction();
    trans->Start();
    IndexNormalSegment *segment = UtAllocNewIndexSegment();
    ASSERT_TRUE(SegmentIsValid(segment));

    PageId segMetaPageId = segment->GetSegmentMetaPageId();
    segMetaPageId.m_blockId += 100; /* Some Pages after the segment meta id is used by Btree Recycle. */
    /* 启动10个线程，每个线程都连续获取100次调用AddPage */
    std::thread threads[10];
    for (uint32 i = 0; i < 10; i++) {
        threads[i] = std::thread(AddPageInThread, segment, segMetaPageId);
        segMetaPageId.m_blockId += 100;
        threads[i].join();
    }

    for (int i = 0; i < 1000; i++) {
        PageId resultPageId = segment->GetFromFreeQueue();
        ASSERT_LE(resultPageId.m_blockId, segMetaPageId.m_blockId);
    }
    delete segment;
    trans->Commit();
}

TEST_F(DataSegmentTest, PageCheckTest_level0)
{
    HeapNormalSegment *segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment));
    PageId segMetaPageId = segment->GetSegmentMetaPageId();
    BufferDesc *segMetaBufferDesc = m_testBufMgr->Read(g_defaultPdbId, segMetaPageId, LW_SHARED);
    Page *page = segMetaBufferDesc->GetPage();
    ASSERT_TRUE(PageHasAlloc(page));
    m_testBufMgr->UnlockAndRelease(segMetaBufferDesc);
    segment->DropSegment();
    delete segment;
}

TEST_F(DataSegmentTest, ExtentScanTest_level0)
{   
    HeapNormalSegment *segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment));
    PageId segMetaPageId = segment->GetSegmentMetaPageId();
    ExtentsScanner *scanner = DstoreNew(g_dstoreCurrentMemoryContext) ExtentsScanner(g_defaultPdbId, segMetaPageId, m_testBufMgr);
    ASSERT_TRUE(!STORAGE_VAR_NULL(scanner));
    if (scanner->CheckSegmentMeta()) {
        scanner->GetExtMetaPageId();
        scanner->GetExtSize();
        scanner->Next();
    }
    delete scanner;

    segment->DropSegment();
    delete segment;
}

TEST_F(DataSegmentTest, DropTempSegmentTest_level0)
{
    HeapTempSegment *heapSegment =
        (HeapTempSegment*)SegmentTest::UTAllocSegment(g_defaultPdbId, 5, m_testBufMgr, SegmentType::HEAP_TEMP_SEGMENT_TYPE);
    ASSERT_NE(heapSegment, nullptr);
    PageId newPage1 = heapSegment->GetNewPage();
    ASSERT_NE(newPage1, INVALID_PAGE_ID);
    heapSegment->DropSegment();
    delete heapSegment;
}
