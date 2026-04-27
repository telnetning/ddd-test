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

#include "ut_tablespace/ut_segment.h"
#include "tablespace/dstore_heap_normal_segment.h"
#include "tablespace/dstore_heap_temp_segment.h"
#include "tablespace/dstore_index_normal_segment.h"
#include "tablespace/dstore_index_temp_segment.h"
#include "tablespace/dstore_tablespace_interface.h"

void AllocExtentInThread(PdbId pdbId, TablespaceId tablespaceId, BufMgrInterface *bufMgr, uint32 extentCount)
{
    create_thread_and_register();
    ut_init_transaction_runtime();
    TableSpace *tablespace = (TableSpace *)UtMockModule::UtGetTableSpace(tablespaceId);
    SegmentInterface *segment = SegmentInterface::AllocUndoSegment(pdbId, tablespace->GetTablespaceId(),  SegmentType::UNDO_SEGMENT_TYPE, bufMgr);
    for (int i = 1; i < extentCount; i++) {
        PageId extPageId;
        segment->Extend(DSTORE::EXT_SIZE_8, &extPageId);
        ASSERT_NE(extPageId, INVALID_PAGE_ID);
    }
    delete segment;
    UtMockModule::UtDropTableSpace(tablespace);
    unregister_thread();
}

void InitInThread(PdbId pdbId, PageId segmentId, TableSpaceInterface *tablespace, BufMgrInterface *bufMgr)
{
    create_thread_and_register();
    ut_init_transaction_runtime();
    Segment *readSegment = DstoreNew(g_dstoreCurrentMemoryContext)
        Segment(pdbId, segmentId, SegmentType::UNDO_SEGMENT_TYPE, tablespace->GetTablespaceId(), bufMgr);
    ASSERT_EQ(readSegment->Init(), DSTORE_SUCC);

    delete readSegment;
    unregister_thread();
}

DataSegment* UTGetSegmentInstance(PdbId pdbId, TablespaceId tablespaceId, BufMgrInterface *bufMgr,
    PageId segMetaPageId, DSTORE::SegmentType segType)
{
    if (segMetaPageId == INVALID_PAGE_ID) {
        return INVALID_SEGMENT;
    }
    DataSegment* segment = NULL;
    switch (segType) {
        case DSTORE::SegmentType::HEAP_SEGMENT_TYPE:
            segment = (DataSegment*)DstoreNew(g_dstoreCurrentMemoryContext) HeapNormalSegment(pdbId, segMetaPageId,
                tablespaceId, bufMgr, g_dstoreCurrentMemoryContext);
            break;
        case DSTORE::SegmentType::INDEX_SEGMENT_TYPE:
            segment = (DataSegment*)DstoreNew(g_dstoreCurrentMemoryContext) IndexNormalSegment(pdbId, segMetaPageId,
                tablespaceId, bufMgr, g_dstoreCurrentMemoryContext);
            break;
        case DSTORE::SegmentType::HEAP_TEMP_SEGMENT_TYPE:
            segment = (DataSegment*)DstoreNew(g_dstoreCurrentMemoryContext) HeapTempSegment(pdbId, segMetaPageId,
                tablespaceId, bufMgr, g_dstoreCurrentMemoryContext);
            break;
        case DSTORE::SegmentType::INDEX_TEMP_SEGMENT_TYPE:
            segment = (DataSegment*)DstoreNew(g_dstoreCurrentMemoryContext) IndexTempSegment(pdbId, segMetaPageId,
                tablespaceId, bufMgr, g_dstoreCurrentMemoryContext);
            break;
        case DSTORE::SegmentType::UNDO_SEGMENT_TYPE:
        default:
            return INVALID_SEGMENT;
    }
    return segment;
}

DataSegment* SegmentTest::UTAllocSegment(PdbId pdbId, TablespaceId tablespaceId, BufMgrInterface *bufMgr,
    DSTORE::SegmentType segType, Oid tableOid)
{
    PageId segMetaPageId = INVALID_PAGE_ID;
    switch (segType) {
        case DSTORE::SegmentType::HEAP_SEGMENT_TYPE:
            segMetaPageId = HeapNormalSegment::AllocHeapNormalSegment(pdbId, tablespaceId, bufMgr, tableOid);
            break;
        case DSTORE::SegmentType::INDEX_SEGMENT_TYPE:
            segMetaPageId = IndexNormalSegment::AllocIndexNormalSegment(pdbId, tablespaceId, bufMgr, tableOid);
            break;
        case DSTORE::SegmentType::HEAP_TEMP_SEGMENT_TYPE:
            segMetaPageId = HeapTempSegment::AllocHeapTempSegment(pdbId, tablespaceId, bufMgr, tableOid);
            break;
        case DSTORE::SegmentType::INDEX_TEMP_SEGMENT_TYPE:
            segMetaPageId = IndexTempSegment::AllocIndexTempSegment(pdbId, tablespaceId, bufMgr, tableOid);
            break;
        case DSTORE::SegmentType::UNDO_SEGMENT_TYPE:
        default:
            return INVALID_SEGMENT;
    }
    if (segMetaPageId == INVALID_PAGE_ID) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("UTAllocSegment failed, oid %u pdb %u tablespace %hu.",
            tableOid, pdbId, tablespaceId));
        return INVALID_SEGMENT;
    }
    DataSegment* segment = UTGetSegmentInstance(pdbId, tablespaceId, bufMgr, segMetaPageId, segType);
    if (STORAGE_VAR_NULL(segment)) {
        return INVALID_SEGMENT;
    }
    if (STORAGE_FUNC_FAIL(segment->InitSegment())) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
            ErrMsg("UTAllocSegment(%hu, %u) initialization failed, oid %u. pdb %u, tablespace %hu",
                segMetaPageId.m_fileId, segMetaPageId.m_blockId, tableOid, pdbId, tablespaceId));
        delete segment;
        return INVALID_SEGMENT;
    }
    return segment;
}

/**
 * AllocSegment: Check Invalid parameter.
 */
TEST_F(SegmentTest, AllocSegmentTest001_level0)
{
    SegmentInterface *segment1 = SegmentInterface::AllocUndoSegment(g_defaultPdbId, m_testTbs->GetTablespaceId(), SegmentType::UNDO_SEGMENT_TYPE, nullptr);
    ASSERT_EQ(segment1, INVALID_SEGMENT);
    SegmentInterface *segment2 = SegmentInterface::AllocUndoSegment(g_defaultPdbId, 0, SegmentType::UNDO_SEGMENT_TYPE, nullptr);
    ASSERT_EQ(segment2, INVALID_SEGMENT);
}

/**
 * AllocSegment: Check segmentMetaPage Info.
 */
TEST_F(SegmentTest, AllocSegmentTest002_level0)
{
    SegmentInterface *segment = SegmentInterface::AllocUndoSegment(g_defaultPdbId, m_testTbs->GetTablespaceId(), SegmentType::UNDO_SEGMENT_TYPE, m_testBufMgr);
    ASSERT_TRUE(SegmentIsValid(segment));
    PageId headPageId = segment->GetSegmentMetaPageId();
    BufferDesc *headBuf = m_testBufMgr->Read(g_defaultPdbId, headPageId, LW_SHARED);
    SegmentMetaPage *headPage = (SegmentMetaPage*)headBuf->GetPage();

    /* Base page info */
    ASSERT_EQ(headPage->m_header.m_myself, headPageId);
    ASSERT_EQ(headPage->GetType(), PageType::UNDO_SEGMENT_META_PAGE_TYPE);

    /* Basic extent meta info; the first extent of UndoSegment is EXT_SIZE_128 */
    ASSERT_EQ(headPage->extentMeta.extSize, EXT_SIZE_128);
    ASSERT_EQ(headPage->extentMeta.nextExtMetaPageId, INVALID_PAGE_ID);

    /* Basic segment head info */
    ASSERT_EQ(headPage->segmentHeader.segmentType, SegmentType::UNDO_SEGMENT_TYPE);
    ASSERT_EQ(headPage->GetTotalBlockCount(), EXT_SIZE_128);
    ASSERT_EQ(headPage->GetExtentCount(), 1);
    ASSERT_EQ(headPage->segmentHeader.extents.first, headPageId);
    ASSERT_EQ(headPage->GetLastExtent(), headPageId);

    m_testBufMgr->UnlockAndRelease(headBuf);
    segment->DropSegment();
    delete segment;
}

/**
 * AllocSegment: Invoke the AllocSegment interface multiple times
 */
TEST_F(SegmentTest, AllocSegmentTest003_level0)
{
    SegmentInterface *segment1 = SegmentInterface::AllocUndoSegment(g_defaultPdbId, m_testTbs->GetTablespaceId(), SegmentType::UNDO_SEGMENT_TYPE, m_testBufMgr);
    ASSERT_TRUE(SegmentIsValid(segment1));
    SegmentInterface *segment2 = SegmentInterface::AllocUndoSegment(g_defaultPdbId, m_testTbs->GetTablespaceId(), SegmentType::UNDO_SEGMENT_TYPE, m_testBufMgr);
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

TEST_F(SegmentTest, AllocSegmentTest004_level0)
{
    PageId segment1 = TableSpace_Interface::AllocSegment(g_defaultPdbId, m_testTbs->GetTablespaceId(), SegmentType::HEAP_SEGMENT_TYPE);
    ASSERT_NE(segment1, INVALID_PAGE_ID);
    PageId segment2 = TableSpace_Interface::AllocSegment(g_defaultPdbId, m_testTbs->GetTablespaceId(), SegmentType::HEAP_TEMP_SEGMENT_TYPE);
    ASSERT_NE(segment2, INVALID_PAGE_ID);
    PageId segment3 = TableSpace_Interface::AllocSegment(g_defaultPdbId, m_testTbs->GetTablespaceId(), SegmentType::UNDO_SEGMENT_TYPE);
    ASSERT_EQ(segment3, INVALID_PAGE_ID);
    PageId segment4 = TableSpace_Interface::AllocSegment(INVALID_PDB_ID, m_testTbs->GetTablespaceId(), SegmentType::HEAP_SEGMENT_TYPE);
    ASSERT_EQ(segment4, INVALID_PAGE_ID);
    PageId segment5 = TableSpace_Interface::AllocSegment(g_defaultPdbId, INVALID_TABLESPACE_ID, SegmentType::HEAP_SEGMENT_TYPE);
    ASSERT_EQ(segment5, INVALID_PAGE_ID);

    RetStatus ret = TableSpace_Interface::DropSegment(g_defaultPdbId, INVALID_TABLESPACE_ID, SegmentType::HEAP_SEGMENT_TYPE, segment1);
    ASSERT_EQ(ret, DSTORE_FAIL);
    ret = TableSpace_Interface::DropSegment(INVALID_PDB_ID, m_testTbs->GetTablespaceId(), SegmentType::HEAP_SEGMENT_TYPE, segment1);
    ASSERT_EQ(ret, DSTORE_FAIL);
    ret = TableSpace_Interface::DropSegment(g_defaultPdbId, m_testTbs->GetTablespaceId(), SegmentType::HEAP_SEGMENT_TYPE, segment1);
    ASSERT_EQ(ret, DSTORE_SUCC);
}

/**
 * Init: Check Invalid parameter
 */
TEST_F(SegmentTest, InitTest001_level0)
{
    /* invalid pageId */
    Segment *newSegment = DstoreNew(g_dstoreCurrentMemoryContext)Segment(g_defaultPdbId, INVALID_PAGE_ID,
        SegmentType::UNDO_SEGMENT_TYPE, m_testTbs->GetTablespaceId(), m_testBufMgr);
    ASSERT_EQ(newSegment->Init(), DSTORE_FAIL);
    delete newSegment;
    /* invalid tablespace */
    newSegment = DstoreNew(g_dstoreCurrentMemoryContext)Segment(g_defaultPdbId, INVALID_PAGE_ID,
        SegmentType::UNDO_SEGMENT_TYPE, 0, m_testBufMgr);
    ASSERT_EQ(newSegment->Init(), DSTORE_FAIL);
    delete newSegment;
    /* invalid bufferMgr */
    newSegment = DstoreNew(g_dstoreCurrentMemoryContext)Segment(g_defaultPdbId, INVALID_PAGE_ID,
        SegmentType::UNDO_SEGMENT_TYPE, m_testTbs->GetTablespaceId(), nullptr);
    ASSERT_EQ(newSegment->Init(), DSTORE_FAIL);
    delete newSegment;

    /* a valid pageId but not segmentId, init failed */
    SegmentInterface *newSegment1 = SegmentInterface::AllocUndoSegment(g_defaultPdbId, m_testTbs->GetTablespaceId(),
        SegmentType::UNDO_SEGMENT_TYPE, m_testBufMgr);
    ASSERT_TRUE(SegmentIsValid(newSegment1));
    PageId segmentId = newSegment1->GetSegmentMetaPageId();
    segmentId.m_blockId++;
    newSegment = DstoreNew(g_dstoreCurrentMemoryContext)Segment(g_defaultPdbId, segmentId,
        SegmentType::UNDO_SEGMENT_TYPE, m_testTbs->GetTablespaceId(), m_testBufMgr);
    ASSERT_EQ(newSegment->Init(), DSTORE_FAIL);
    newSegment1->DropSegment();
    delete newSegment1;
    delete newSegment;
}

/**
 * Init: duplicate Init
 */
TEST_F(SegmentTest, InitTest002_level0)
{
    SegmentInterface *newSegment = SegmentInterface::AllocUndoSegment(g_defaultPdbId, m_testTbs->GetTablespaceId(),
        SegmentType::UNDO_SEGMENT_TYPE, m_testBufMgr);
    ASSERT_TRUE(SegmentIsValid(newSegment));
    PageId segmentId = newSegment->GetSegmentMetaPageId();
    ASSERT_NE(segmentId, INVALID_PAGE_ID);
    AllocNewExtentTest(newSegment, EXT_SIZE_8, PageType::UNDO_PAGE_TYPE);
    delete newSegment;

    Segment *readSegment = DstoreNew(g_dstoreCurrentMemoryContext)Segment(g_defaultPdbId, segmentId,
        SegmentType::UNDO_SEGMENT_TYPE, m_testTbs->GetTablespaceId(), m_testBufMgr);
    for (int i = 0; i < 10; i++) {
        ASSERT_EQ(readSegment->Init(), DSTORE_SUCC);
    }
    readSegment->DropSegment();
    delete readSegment;
}

/**
 * Init: Init a segment that is created before.
 * 1. New(Segment)-Extend, failed because of not Init
 * 2. New(Segment)-Init-Extend, success.
 */
TEST_F(SegmentTest, InitTest003_level0)
{
    SegmentInterface *newSegment = SegmentInterface::AllocUndoSegment(g_defaultPdbId, m_testTbs->GetTablespaceId(),
        SegmentType::UNDO_SEGMENT_TYPE, m_testBufMgr);
    ASSERT_TRUE(SegmentIsValid(newSegment));
    PageId segmentId = newSegment->GetSegmentMetaPageId();
    ASSERT_NE(segmentId, INVALID_PAGE_ID);
    AllocNewExtentTest(newSegment, EXT_SIZE_8, PageType::UNDO_PAGE_TYPE);
    delete newSegment;

    Segment *readSegment = DstoreNew(g_dstoreCurrentMemoryContext)Segment(g_defaultPdbId, segmentId, 
        SegmentType::UNDO_SEGMENT_TYPE, m_testTbs->GetTablespaceId(), m_testBufMgr);
    ASSERT_TRUE(SegmentIsValid(readSegment));

    /* Could not use any function except Init() and GetSegmentId() */
    ASSERT_EQ(readSegment->GetSegmentMetaPageId(), segmentId);
    PageId firstPage;
    ASSERT_NE(readSegment->Extend(EXT_SIZE_8, &firstPage), 0);
    ASSERT_EQ(firstPage, INVALID_PAGE_ID);
    ASSERT_NE(readSegment->DropSegment(), 0);

    ASSERT_EQ(readSegment->Init(), 0);
    AllocNewExtentTest(readSegment, EXT_SIZE_8, PageType::UNDO_PAGE_TYPE);

    readSegment->DropSegment();
    delete readSegment;
}

/**
  *@tc.name:  InitConcurrency001
  *@tc.desc:  创建1个Segment之后，生成1个改Segment对象，预期在10个线程里面Init都能成功。
 * @tc.type: FUNC
 * @tc. require:
  *@tc.author: wangsiyuan
*/
TEST_F(SegmentTest, DISABLED_InitConcurrency001_level0)
{
    SegmentInterface *newSegment = SegmentInterface::AllocUndoSegment(g_defaultPdbId, m_testTbs->GetTablespaceId(),
        SegmentType::UNDO_SEGMENT_TYPE, m_testBufMgr);
    ASSERT_TRUE(SegmentIsValid(newSegment));
    PageId segmentId = newSegment->GetSegmentMetaPageId();
    ASSERT_NE(segmentId, INVALID_PAGE_ID);
    AllocNewExtentTest(newSegment, EXT_SIZE_8, PageType::UNDO_PAGE_TYPE);
    delete newSegment;


    std::thread threads[10];
    for (int i = 0; i < 10; i++) {
        threads[i] = std::thread(InitInThread, g_defaultPdbId, segmentId, m_testTbs, m_testBufMgr);
    }
    for (int i = 0; i < 10; i++) {
        threads[i].join();
    }
}

/**
 * Extend: Extend EXT_SIZE_8 extent
 */
TEST_F(SegmentTest, ExtendTest001_level0)
{
    SegmentInterface *segment = SegmentInterface::AllocUndoSegment(g_defaultPdbId, m_testTbs->GetTablespaceId(),
        SegmentType::UNDO_SEGMENT_TYPE, m_testBufMgr);
    ASSERT_TRUE(SegmentIsValid(segment));

    /* Alloc new extent of EXT_SIZE_8 */
    AllocNewExtentTest(segment, EXT_SIZE_8, PageType::UNDO_PAGE_TYPE);

    segment->DropSegment();
    delete segment;
}

/**
 * Extend: Extend EXT_SIZE_128 extent
 */
TEST_F(SegmentTest, ExtendTest002_level0)
{
    SegmentInterface *segment = SegmentInterface::AllocUndoSegment(g_defaultPdbId, m_testTbs->GetTablespaceId(), 
        SegmentType::UNDO_SEGMENT_TYPE, m_testBufMgr);
    ASSERT_TRUE(SegmentIsValid(segment));

    /* Alloc new extent of EXT_SIZE_128 */
    AllocNewExtentTest(segment, EXT_SIZE_128, PageType::UNDO_PAGE_TYPE);

    segment->DropSegment();
    delete segment;
}

/**
 * Extend: Extend EXT_SIZE_1024 extent
 */
TEST_F(SegmentTest, ExtendTest003_level0)
{
    SegmentInterface *segment = SegmentInterface::AllocUndoSegment(g_defaultPdbId, m_testTbs->GetTablespaceId(),
        SegmentType::UNDO_SEGMENT_TYPE, m_testBufMgr);
    ASSERT_TRUE(SegmentIsValid(segment));

    /* Alloc new extent of EXT_SIZE_1024 */
    AllocNewExtentTest(segment, EXT_SIZE_1024, PageType::UNDO_PAGE_TYPE);

    segment->DropSegment();
    delete segment;
}

/**
 * Extend: Extend EXT_SIZE_8192 extent
 */
TEST_F(SegmentTest, ExtendTest004_level0)
{
    SegmentInterface *segment = SegmentInterface::AllocUndoSegment(g_defaultPdbId, m_testTbs->GetTablespaceId(),
        SegmentType::UNDO_SEGMENT_TYPE, m_testBufMgr);
    ASSERT_TRUE(SegmentIsValid(segment));

    /* Alloc new extent of EXT_SIZE_8192 */
    AllocNewExtentTest(segment, EXT_SIZE_8192, PageType::UNDO_PAGE_TYPE);

    segment->DropSegment();
    delete segment;
}

/**
 * Extend: Invoke the Extend interface multiple times, for different extent types.
 */
TEST_F(SegmentTest, ExtendTest005_level0)
{
    SegmentInterface *segment = SegmentInterface::AllocUndoSegment(g_defaultPdbId, m_testTbs->GetTablespaceId(),
        SegmentType::UNDO_SEGMENT_TYPE, m_testBufMgr);
    ASSERT_TRUE(SegmentIsValid(segment));

    /* Continuous extend segment with different extent size */
    AllocNewExtentTest(segment, EXT_SIZE_8, PageType::UNDO_PAGE_TYPE);
    AllocNewExtentTest(segment, EXT_SIZE_8192, PageType::UNDO_PAGE_TYPE);
    AllocNewExtentTest(segment, EXT_SIZE_128, PageType::UNDO_PAGE_TYPE);
    AllocNewExtentTest(segment, EXT_SIZE_1024, PageType::UNDO_PAGE_TYPE);

    segment->DropSegment();
    delete segment;
}

/**
 * Extend: extend EXT_SIZE_8 extent to 128M.
 */
TEST_F(SegmentTest, ExtendTest006_level0)
{
    /* Need to extend 128M/64K times (include first EXT_SIZE_128 extent) */
    uint64 targetExtentCount = ((uint64)128 * 1024 * 1024) / (64 * 1024);
    SegmentInterface *segment = SegmentInterface::AllocUndoSegment(g_defaultPdbId, m_testTbs->GetTablespaceId(),
        SegmentType::UNDO_SEGMENT_TYPE, m_testBufMgr);
    ASSERT_TRUE(SegmentIsValid(segment));

    for (uint64 i = 1; i < targetExtentCount; ++i) {
        AllocNewExtentTest(segment, EXT_SIZE_8, PageType::UNDO_PAGE_TYPE);
    }
    BufferDesc *headBuf = m_testBufMgr->Read(g_defaultPdbId, segment->GetSegmentMetaPageId(), LW_SHARED);
    SegmentMetaPage *headPage = (SegmentMetaPage*)headBuf->GetPage();
    ASSERT_EQ(headPage->GetExtentCount(), targetExtentCount);
    /* the first extent of UndoSegment is EXT_SIZE_128 */
    ASSERT_EQ(headPage->GetTotalBlockCount(), (targetExtentCount - 1) * EXT_SIZE_8 + EXT_SIZE_128);
    m_testBufMgr->UnlockAndRelease(headBuf);

    segment->DropSegment();
    delete segment;
}

/**
 * Extend: extend EXT_SIZE_128 extent to 2G.
 */
TEST_F(SegmentTest, ExtendTest007_level0)
{
    /* Need to extend 2G/1M times (exclude first EXT_SIZE_128 extent) */
    uint64 targetExtentCount = ((uint64)2 * 1024 * 1024 * 1024) / (1 * 1024 * 1024);
    SegmentInterface *segment = SegmentInterface::AllocUndoSegment(g_defaultPdbId, m_testTbs->GetTablespaceId(),
        SegmentType::UNDO_SEGMENT_TYPE, m_testBufMgr);
    ASSERT_TRUE(SegmentIsValid(segment));

    for (uint64 i = 0; i < targetExtentCount; ++i) {
        AllocNewExtentTest(segment, EXT_SIZE_128, PageType::UNDO_PAGE_TYPE);
    }
    BufferDesc *headBuf = m_testBufMgr->Read(g_defaultPdbId, segment->GetSegmentMetaPageId(), LW_SHARED);
    SegmentMetaPage *headPage = (SegmentMetaPage*)headBuf->GetPage();
    ASSERT_EQ(headPage->GetExtentCount(), targetExtentCount + 1);
    ASSERT_EQ(headPage->GetTotalBlockCount(), targetExtentCount * EXT_SIZE_128 + EXT_SIZE_128);
    m_testBufMgr->UnlockAndRelease(headBuf);

    delete segment;
}

/**
 * Extend: extend EXT_SIZE_1024 extent to 10G.
 */
TEST_F(SegmentTest, ExtendTest008_level0)
{
    /* Need to extend 10G/8M times (exclude first 64K extent) */
    uint64 targetExtentCount = ((uint64) 10 * 1024 * 1024 * 1024) / (8 * 1024 * 1024);
    SegmentInterface *segment = SegmentInterface::AllocUndoSegment(g_defaultPdbId, m_testTbs->GetTablespaceId(),
        SegmentType::UNDO_SEGMENT_TYPE, m_testBufMgr);
    ASSERT_TRUE(SegmentIsValid(segment));

    for (uint64 i = 0; i < targetExtentCount; ++i) {
        AllocNewExtentTest(segment, EXT_SIZE_1024, PageType::UNDO_PAGE_TYPE);
    }
    BufferDesc *headBuf = m_testBufMgr->Read(g_defaultPdbId, segment->GetSegmentMetaPageId(), LW_SHARED);
    SegmentMetaPage *headPage = (SegmentMetaPage *) headBuf->GetPage();
    ASSERT_EQ(headPage->GetExtentCount(), targetExtentCount + 1);
    ASSERT_EQ(headPage->GetTotalBlockCount(), targetExtentCount * EXT_SIZE_1024 + EXT_SIZE_128);
    m_testBufMgr->UnlockAndRelease(headBuf);

    delete segment;
}

/**
 * Extend: extend EXT_SIZE_8192 extent to 100G.
 */
TEST_F(SegmentTest, ExtendTest009_level0)
{
    /* Need to extend 100G/64M times (exclude first 64K extent) */
    uint64 targetExtentCount = ((uint64) 100 * 1024 * 1024 * 1024) / (64 * 1024 * 1024);
    SegmentInterface *segment = SegmentInterface::AllocUndoSegment(g_defaultPdbId, m_testTbs->GetTablespaceId(),
        SegmentType::UNDO_SEGMENT_TYPE, m_testBufMgr);
    ASSERT_TRUE(SegmentIsValid(segment));

    for (uint64 i = 0; i < targetExtentCount; ++i) {
        AllocNewExtentTest(segment, EXT_SIZE_8192, PageType::UNDO_PAGE_TYPE);
    }
    BufferDesc *headBuf = m_testBufMgr->Read(g_defaultPdbId, segment->GetSegmentMetaPageId(), LW_SHARED);
    SegmentMetaPage *headPage = (SegmentMetaPage *) headBuf->GetPage();
    ASSERT_EQ(headPage->GetExtentCount(), targetExtentCount + 1);
    ASSERT_EQ(headPage->GetTotalBlockCount(), targetExtentCount * EXT_SIZE_8192 + EXT_SIZE_128);
    m_testBufMgr->UnlockAndRelease(headBuf);

    delete segment;
}

/**
 * Extend: extend 4 different extents until to 100G.
 */
TEST_F(SegmentTest, ExtendTest010_level0)
{
    ExtentSize extSizeList[EXTENT_SIZE_COUNT] = {EXT_SIZE_8, EXT_SIZE_128, EXT_SIZE_1024, EXT_SIZE_8192};

    /* 20G Segment has 20G/8K blocks */
    uint64 targetBlockCount = ((uint64) 20 * 1024 * 1024 * 1024) / BLCKSZ;
    SegmentInterface *segment = SegmentInterface::AllocUndoSegment(g_defaultPdbId, m_testTbs->GetTablespaceId(),
        SegmentType::UNDO_SEGMENT_TYPE, m_testBufMgr);
    ASSERT_TRUE(SegmentIsValid(segment));
    uint64 currentExtentCount = 1;
    uint64 currentBlockCount = EXT_SIZE_128;

    while (currentBlockCount < targetBlockCount) {
        ExtentSize curExtSize = extSizeList[currentExtentCount % EXTENT_SIZE_COUNT];
        AllocNewExtentTest(segment, curExtSize, PageType::UNDO_PAGE_TYPE);

        BufferDesc *headBuf = m_testBufMgr->Read(g_defaultPdbId, segment->GetSegmentMetaPageId(), LW_SHARED);
        SegmentMetaPage *headPage = (SegmentMetaPage *) headBuf->GetPage();
        currentExtentCount += 1;
        ASSERT_EQ(headPage->GetExtentCount(), currentExtentCount);
        currentBlockCount += curExtSize;
        ASSERT_EQ(headPage->GetTotalBlockCount(), currentBlockCount);
        m_testBufMgr->UnlockAndRelease(headBuf);
    }

    delete segment;
}

/**
 * Drop: after drop segment, all interface(Init/Extend/DropSegment) in Segment is unusable.
 */
TEST_F(SegmentTest, DropSegmentTest001_level0)
{
    SegmentInterface *segment = SegmentInterface::AllocUndoSegment(g_defaultPdbId, m_testTbs->GetTablespaceId(),
        SegmentType::UNDO_SEGMENT_TYPE, m_testBufMgr);
    ASSERT_TRUE(SegmentIsValid(segment));
    ASSERT_NE(segment->GetSegmentMetaPageId(), INVALID_PAGE_ID);
    PageId segmentId = segment->GetSegmentMetaPageId();
    delete segment;

    Segment *readSegment = DstoreNew(g_dstoreCurrentMemoryContext)Segment(g_defaultPdbId, segmentId, 
        SegmentType::UNDO_SEGMENT_TYPE, m_testTbs->GetTablespaceId(), m_testBufMgr);
    ASSERT_EQ(readSegment->Init(), DSTORE_SUCC);
    ASSERT_EQ(readSegment->DropSegment(), DSTORE_SUCC);
    ASSERT_EQ(readSegment->Init(), DSTORE_FAIL);
    /* Could not use any function after segment is dropped */
    PageId tempPage;
    ASSERT_EQ(readSegment->Extend(EXT_SIZE_8, &tempPage), DSTORE_FAIL);
    ASSERT_EQ(tempPage, INVALID_PAGE_ID);
    ASSERT_EQ(readSegment->GetSegmentMetaPageId(), INVALID_PAGE_ID);
    ASSERT_EQ(readSegment->DropSegment(), DSTORE_FAIL);

    delete readSegment;
}

/**
 * DropSegment: drop segment will release space.
 */
TEST_F(SegmentTest, DropSegmentTest002_level0_TIER2)
{
   SegmentInterface *segment = SegmentInterface::AllocUndoSegment(g_defaultPdbId, m_testTbs->GetTablespaceId(), SegmentType::UNDO_SEGMENT_TYPE, m_testBufMgr);
   ASSERT_TRUE(SegmentIsValid(segment));
   PageId segmentId = segment->GetSegmentMetaPageId();
   ASSERT_NE(segmentId, INVALID_PAGE_ID);

   uint64 targetExtentCount = ((uint64)2 * 1024 * 1024 * 1024) / (1 * 1024 * 1024);
   ASSERT_TRUE(SegmentIsValid(segment));
   for (uint64 i = 0; i < targetExtentCount; ++i) {
       AllocNewExtentTest(segment, EXT_SIZE_128, PageType::UNDO_PAGE_TYPE);
   }
   /* DropSegment will release all space in segment */
   ASSERT_EQ(segment->DropSegment(), DSTORE_SUCC);
   ASSERT_TRUE(SegmentIsValid(segment));
   delete segment;

   SegmentInterface *newSegment = SegmentInterface::AllocUndoSegment(g_defaultPdbId, m_testTbs->GetTablespaceId(),
       SegmentType::UNDO_SEGMENT_TYPE, m_testBufMgr);
   PageId newSegmentId = newSegment->GetSegmentMetaPageId();
   ASSERT_NE(newSegmentId, INVALID_PAGE_ID);
   ASSERT_EQ(newSegment->DropSegment(), DSTORE_SUCC);
   delete newSegment;

   /* if segmentId == newSegmentId, indicates that all space of the segment is released. */
   ASSERT_EQ(segmentId.m_fileId, newSegmentId.m_fileId);
   ASSERT_EQ(segmentId.m_blockId, newSegmentId.m_blockId);
}
