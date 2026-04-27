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

#include "ut_tablespace/ut_data_segment_scan_context.h"
#include "heap/dstore_heap_handler.h"

void ScanSegment(DataSegmentScanContext *segScanContext, uint32 expectPageCount)
{
    ((UtInstance *)g_storageInstance)->ThreadSetupAndRegister();
    int count = 1;
    while(segScanContext->GetNextPageId() != segScanContext->GetEndPageId()) {
        count++;
    }
    ASSERT_EQ(count, expectPageCount);
    ((UtInstance *)g_storageInstance)->ThreadUnregisterAndExit();
}

void GetFirstDataPage(DataSegmentScanContext *segScanContext, PageId firstPageId)
{
    ((UtInstance *)g_storageInstance)->ThreadSetupAndRegister();
    PageId pageId = segScanContext->GetFirstPageId();
    ASSERT_EQ(pageId, firstPageId);
    ((UtInstance *)g_storageInstance)->ThreadUnregisterAndExit();
}

void ExtendSegment(HeapNormalSegment *segment)
{
    ((UtInstance *)g_storageInstance)->ThreadSetupAndRegister();
    for (int i = 0; i < 100; ++i) {
        ASSERT_NE(segment->GetNewPage(), INVALID_PAGE_ID);
    }
    ((UtInstance *)g_storageInstance)->ThreadUnregisterAndExit();
}

void UpdateFSMinSegment(BufMgrInterface *testBufMgr, HeapNormalSegment *segment)
{
    ((UtInstance *)g_storageInstance)->ThreadSetupAndRegister();
    /* Add pages to FSM */
    PageId newPage = segment->GetNewPage();
    ASSERT_NE(newPage, INVALID_PAGE_ID);
    BufferDesc *headBuf = testBufMgr->Read(g_defaultPdbId, segment->GetSegmentMetaPageId(), LW_SHARED);
    DataSegmentMetaPage *headPage = (DataSegmentMetaPage *)headBuf->GetPage();
    uint64 currentDataBlock = headPage->GetDataBlockCount();
    testBufMgr->UnlockAndRelease(headBuf);
    PageId pageFromFsm{};
    for (uint64 i = 0; i < currentDataBlock - 1; ++i) {
        pageFromFsm = segment->GetPageFromFsm(1, 0);
        ASSERT_NE(pageFromFsm, INVALID_PAGE_ID);
        ASSERT_EQ(segment->UpdateFsm(pageFromFsm, 0), DSTORE_SUCC);
    }
    ((UtInstance *)g_storageInstance)->ThreadUnregisterAndExit();
}

/**
  *@tc.name:  GetNextPage001
  *@tc.desc:  1.构建一批数据页面；
  *           2.如果没有调用GetFirstPageId，则第一次调用GetNextPageId相当于调用了GetFirstPageId
  *           3.在这一批页面数量内，GetNextPageId返回可用的PageId；
  *           4.如果上一次调用GetNextPageId已经是最后一个数据页面ID，则返回INVALID_PAGE_ID；
 * @tc.type: FUNC
 * @tc. require:
  *@tc.author: liangyanzhao
*/
TEST_F(DataSegmentScanContextTest, GetNextPage001_Level0)
{
    HeapNormalSegment *segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment));

    SmgrType smgrType = SmgrType::HEAP_SMGR;
    StorageRelationData tmpRel;
    tmpRel.tableSmgr = DstoreNew(m_ut_memory_context)
        TableStorageMgr(segment->GetPdbId(), DEFAULT_HEAP_FILLFACTOR, nullptr, SYS_RELPERSISTENCE_PERMANENT);
    tmpRel.SetTableSmgrSegment(dynamic_cast<HeapSegment *>(segment));

    DataSegmentScanContext segScanContext(g_storageInstance->GetBufferMgr(), &tmpRel, smgrType);
    ASSERT_EQ(segScanContext.GetNextPageId(), INVALID_PAGE_ID);

    PageId firstDataPage = segment->GetNewPage();
    ASSERT_NE(firstDataPage, INVALID_PAGE_ID);
    for (int i = 0; i < 20; ++i) {
        ASSERT_NE(segment->GetNewPage(), INVALID_PAGE_ID);
    }

    DataSegmentMetaPage *headPage;
    BufferDesc *bufferDesc;
    ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_SHARED, bufferDesc, headPage);
    uint64 totalDataPage = headPage->dataBlockCount;
    m_testBufMgr->UnlockAndRelease(bufferDesc);

    ASSERT_EQ(segScanContext.GetNextPageId(), firstDataPage); /* First call of GetNextDataPage, return first page */
    for (uint64 i = 1; i < totalDataPage; ++i) {
        ASSERT_NE(segScanContext.GetNextPageId(), INVALID_PAGE_ID);
    }
    ASSERT_EQ(segScanContext.GetNextPageId(), INVALID_PAGE_ID);

    segment->DropSegment();
    delete segment;
    tmpRel.SetTableSmgrSegment(nullptr);
    delete tmpRel.tableSmgr;
}

/**
  *@tc.name:  GetNextPage001
  *@tc.desc:  使用GetNextPage对Segment扫描2次
 * @tc.type: FUNC
 * @tc. require:
  *@tc.author: liangyanzhao
*/
TEST_F(DataSegmentScanContextTest, GetNextPage002_Level0)
{
    HeapNormalSegment *segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment));
    for (int i = 0; i < 20; ++i) {
        ASSERT_NE(segment->GetNewPage(), INVALID_PAGE_ID);
    }

    SmgrType smgrType = SmgrType::HEAP_SMGR;
    StorageRelationData tmpRel;
    tmpRel.tableSmgr = DstoreNew(m_ut_memory_context)
        TableStorageMgr(segment->GetPdbId(), DEFAULT_HEAP_FILLFACTOR, nullptr, SYS_RELPERSISTENCE_PERMANENT);
    tmpRel.SetTableSmgrSegment(dynamic_cast<HeapSegment *>(segment));

    DataSegmentScanContext segScanContext(g_storageInstance->GetBufferMgr(), &tmpRel, smgrType);
    DataSegmentMetaPage *headPage;
    BufferDesc *bufferDesc;
    ReadBufferAndPage(segment->GetSegmentMetaPageId(), LW_SHARED, bufferDesc, headPage);
    uint64 totalDataPage = headPage->dataBlockCount;
    m_testBufMgr->UnlockAndRelease(bufferDesc);

    std::vector<PageId> pageVec;
    pageVec.push_back(segScanContext.GetFirstPageId());
    for (uint64 i = 1; i < totalDataPage; ++i) {
        pageVec.push_back(segScanContext.GetNextPageId());
    }
    ASSERT_EQ(segScanContext.GetNextPageId(), INVALID_PAGE_ID);

    /* Second scan */
    ASSERT_EQ(segScanContext.GetFirstPageId(), pageVec[0]);
    for (uint64 i = 1; i < totalDataPage; ++i) {
        ASSERT_EQ(segScanContext.GetNextPageId(), pageVec[i]);
    }

    segment->DropSegment();
    delete segment;
    tmpRel.SetTableSmgrSegment(nullptr);
    delete tmpRel.tableSmgr;
}

/**
  *@tc.name:  GetNextPage003
  *@tc.desc:  并发（GetNextPage和GetNewPage并发调用）
 * @tc.type: FUNC
 * @tc. require:
  *@tc.author: wangsiyuan
*/
TEST_F(DataSegmentScanContextTest, DISABLED_GetNextPage003_Level0)
{
    HeapNormalSegment *segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment));

    SmgrType smgrType = SmgrType::HEAP_SMGR;
    StorageRelationData tmpRel;
    tmpRel.tableSmgr = DstoreNew(m_ut_memory_context)
        TableStorageMgr(segment->GetPdbId(), DEFAULT_HEAP_FILLFACTOR, nullptr, SYS_RELPERSISTENCE_PERMANENT);
    tmpRel.SetTableSmgrSegment(dynamic_cast<HeapSegment *>(segment));

    DataSegmentScanContext segScanContext(g_storageInstance->GetBufferMgr(), &tmpRel, smgrType);
    ASSERT_EQ(segScanContext.GetNextPageId(), INVALID_PAGE_ID);
    for (int i = 0; i < 200; ++i) {
        ASSERT_NE(segment->GetNewPage(), INVALID_PAGE_ID);
    }
    PageId firstDataPage = segScanContext.GetFirstPageId();
    ASSERT_NE(firstDataPage, INVALID_PAGE_ID);
    uint32 count = 1;
    while (segScanContext.GetNextPageId() != segScanContext.GetEndPageId()) {
        count++;
    }
    firstDataPage = segScanContext.GetFirstPageId();

    std::thread scanThread = std::thread(ScanSegment, &segScanContext, count);
    std::thread extendThread = std::thread(ExtendSegment, segment);
    scanThread.join();
    extendThread.join();

    segment->DropSegment();
    delete segment;
    tmpRel.SetTableSmgrSegment(nullptr);
    delete tmpRel.tableSmgr;
}

/**
  *@tc.name:  GetNextPage004
  *@tc.desc:  并发（GetNextPage和UpdateFsm、 GetPageFromFsm并发调用）
 * @tc.type: FUNC
 * @tc. require:
  *@tc.author: wangsiyuan
*/
TEST_F(DataSegmentScanContextTest, DISABLED_GetNextPage004_Level0)
{
    HeapNormalSegment *segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment));

    SmgrType smgrType = SmgrType::HEAP_SMGR;
    StorageRelationData tmpRel;
    tmpRel.tableSmgr = DstoreNew(m_ut_memory_context)
        TableStorageMgr(segment->GetPdbId(), DEFAULT_HEAP_FILLFACTOR, nullptr, SYS_RELPERSISTENCE_PERMANENT);
    tmpRel.SetTableSmgrSegment(dynamic_cast<HeapSegment *>(segment));

    DataSegmentScanContext segScanContext(g_storageInstance->GetBufferMgr(), &tmpRel, smgrType);
    ASSERT_EQ(segScanContext.GetNextPageId(), INVALID_PAGE_ID);
    for (int i = 0; i < 200; ++i) {
        ASSERT_NE(segment->GetNewPage(), INVALID_PAGE_ID);
    }
    PageId firstDataPage = segScanContext.GetFirstPageId();
    ASSERT_NE(firstDataPage, INVALID_PAGE_ID);
    uint32 count = 1;
    while (segScanContext.GetNextPageId() != segScanContext.GetEndPageId()) {
        count++;
    }
    firstDataPage = segScanContext.GetFirstPageId();

    std::thread scanThread = std::thread(ScanSegment, &segScanContext, count);
    std::thread extendThread = std::thread(UpdateFSMinSegment, m_testBufMgr, segment);
    scanThread.join();
    extendThread.join();

    segment->DropSegment();
    delete segment;
    tmpRel.SetTableSmgrSegment(nullptr);
    delete tmpRel.tableSmgr;
}

/**
  *@tc.name:  GetFirstPage001
  *@tc.desc:  1.对于初始化的DataSegmentScanContext对象，第一次调用GetFirstPageId返回INVALID_PAGE_ID；
  *           2.调用segment->GetNewPage之后，GetFirstPageId返回非INVALID_PAGE_ID；
  *           3.GetFirstPageId会重置当前ScanContext对象的扫描进度；
 * @tc.type: FUNC
 * @tc. require:
  *@tc.author: wangsiyuan
*/
TEST_F(DataSegmentScanContextTest, GetFirstPage001_Level0)
{
    HeapNormalSegment *segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment));

    SmgrType smgrType = SmgrType::HEAP_SMGR;
    StorageRelationData tmpRel;
    tmpRel.tableSmgr = DstoreNew(m_ut_memory_context)
        TableStorageMgr(segment->GetPdbId(), DEFAULT_HEAP_FILLFACTOR, nullptr, SYS_RELPERSISTENCE_PERMANENT);
    tmpRel.SetTableSmgrSegment(dynamic_cast<HeapSegment *>(segment));
    
    DataSegmentScanContext segScanContext(g_storageInstance->GetBufferMgr(), &tmpRel, smgrType);
    ASSERT_EQ(segScanContext.GetFirstPageId(), INVALID_PAGE_ID);
    PageId firstDataPage = segment->GetNewPage();
    ASSERT_NE(firstDataPage, INVALID_PAGE_ID);
    ASSERT_EQ(segScanContext.GetFirstPageId(), firstDataPage);

    PageId curPageId = segScanContext.GetNextPageId();
    ASSERT_NE(firstDataPage, curPageId);
    PageId newFirstPageId = segScanContext.GetFirstPageId();
    ASSERT_NE(newFirstPageId, curPageId);
    ASSERT_EQ(newFirstPageId, firstDataPage);
    segment->DropSegment();
    delete segment;
    tmpRel.SetTableSmgrSegment(nullptr);
    delete tmpRel.tableSmgr;
}

/**
  *@tc.name:  GetFirstPage002
  *@tc.desc:  并发（GetFirstPage和GetNewPage并发调用）
 * @tc.type: FUNC
 * @tc. require:
  *@tc.author: wangsiyuan
*/
TEST_F(DataSegmentScanContextTest, DISABLED_GetFirstPage002_Level0)
{
    HeapNormalSegment *segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment));

    SmgrType smgrType = SmgrType::HEAP_SMGR;
    StorageRelationData tmpRel;
    tmpRel.tableSmgr = DstoreNew(m_ut_memory_context)
        TableStorageMgr(segment->GetPdbId(), DEFAULT_HEAP_FILLFACTOR, nullptr, SYS_RELPERSISTENCE_PERMANENT);
    tmpRel.SetTableSmgrSegment(dynamic_cast<HeapSegment *>(segment));

    DataSegmentScanContext segScanContext(g_storageInstance->GetBufferMgr(), &tmpRel, smgrType);

    ASSERT_EQ(segScanContext.GetNextPageId(), INVALID_PAGE_ID);
    for (int i = 0; i < 200; ++i) {
        ASSERT_NE(segment->GetNewPage(), INVALID_PAGE_ID);
    }
    PageId firstDataPage = segScanContext.GetFirstPageId();
    ASSERT_NE(firstDataPage, INVALID_PAGE_ID);
    uint32 count = 1;
    while (segScanContext.GetNextPageId() != segScanContext.GetEndPageId()) {
        count++;
    }

    std::thread scanThread = std::thread(GetFirstDataPage, &segScanContext, firstDataPage);
    std::thread extendThread = std::thread(ExtendSegment, segment);
    scanThread.join();
    extendThread.join();

    segment->DropSegment();
    delete segment;
    tmpRel.SetTableSmgrSegment(nullptr);
    delete tmpRel.tableSmgr;
}

/**
  *@tc.name:  GetEndPage001
  *@tc.desc: 1. 如果没有调用GetFirstPageId，GetNextPageId返回INVALID_PAGE_ID;
  *          2. 返回该ScanContext在GetFirstPageId时获取的最后一个数据页面ID
 * @tc.type: FUNC
 * @tc. require:
  *@tc.author: wangsiyuan
*/
TEST_F(DataSegmentScanContextTest, GetEndPage001_Level0)
{
    HeapNormalSegment *segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment));

    SmgrType smgrType = SmgrType::HEAP_SMGR;
    StorageRelationData tmpRel;
    tmpRel.tableSmgr = DstoreNew(m_ut_memory_context)
        TableStorageMgr(segment->GetPdbId(), DEFAULT_HEAP_FILLFACTOR, nullptr, SYS_RELPERSISTENCE_PERMANENT);
    tmpRel.SetTableSmgrSegment(dynamic_cast<HeapSegment *>(segment));

    DataSegmentScanContext segScanContext(g_storageInstance->GetBufferMgr(), &tmpRel, smgrType);

    PageId firstDataPage = segment->GetNewPage();
    ASSERT_NE(firstDataPage, INVALID_PAGE_ID);

    ASSERT_EQ(segScanContext.GetEndPageId(), INVALID_PAGE_ID);
    ASSERT_NE(segScanContext.GetFirstPageId(), INVALID_PAGE_ID);
    PageId endPageId = segScanContext.GetEndPageId();
    ASSERT_NE(endPageId, INVALID_PAGE_ID);

    while (segScanContext.GetNextPageId() != endPageId) {}
    ASSERT_EQ(segScanContext.GetNextPageId(), INVALID_PAGE_ID);

    segment->DropSegment();
    delete segment;
    tmpRel.SetTableSmgrSegment(nullptr);
    delete tmpRel.tableSmgr;
}

/**
  *@tc.name:  Reset001
  *@tc.desc: 重置扫描进度，下一次调用GetNextPageId返回第一个数据页面ID
 * @tc.type: FUNC
 * @tc. require:
  *@tc.author: wangsiyuan
*/
TEST_F(DataSegmentScanContextTest, Reset001_Level0)
{
    HeapNormalSegment *segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment));

    SmgrType smgrType = SmgrType::HEAP_SMGR;
    StorageRelationData tmpRel;
    tmpRel.tableSmgr = DstoreNew(m_ut_memory_context)
        TableStorageMgr(segment->GetPdbId(), DEFAULT_HEAP_FILLFACTOR, nullptr, SYS_RELPERSISTENCE_PERMANENT);
    tmpRel.SetTableSmgrSegment(dynamic_cast<HeapSegment *>(segment));

    DataSegmentScanContext segScanContext(g_storageInstance->GetBufferMgr(), &tmpRel, smgrType);
    PageId firstDataPage = segment->GetNewPage();
    ASSERT_NE(firstDataPage, INVALID_PAGE_ID);

    ASSERT_EQ(segScanContext.GetNextPageId(), firstDataPage);
    ASSERT_NE(segScanContext.GetNextPageId(), INVALID_PAGE_ID);
    ASSERT_NE(segScanContext.GetNextPageId(), INVALID_PAGE_ID);

    segScanContext.Reset();
    ASSERT_EQ(segScanContext.GetNextPageId(), firstDataPage);

    segment->DropSegment();
    delete segment;
    tmpRel.SetTableSmgrSegment(nullptr);
    delete tmpRel.tableSmgr;
}
