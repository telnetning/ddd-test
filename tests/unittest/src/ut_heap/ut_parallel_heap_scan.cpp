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

#include <gtest/gtest.h>
#include "ut_heap/ut_heap.h"
#include "heap/dstore_heap_scan.h"
#include "framework/dstore_parallel.h"
#include "ut_utilities/ut_dstore_framework.h"
#include "ut_tablespace/ut_data_segment.h"
#include "heap/dstore_heap_scan_parallel.h"
#include "framework/dstore_parallel_interface.h"
#include "ut_tablespace/ut_parallel_heap_scan.h"

void ScanWorkloadInfo(ParallelWorkController *parallelController, std::atomic<int> &numPage,
    std::atomic<int> &numWorkloadInfoMetaPage, int smpId)
{
    ((UtInstance *)g_storageInstance)->ThreadSetupAndRegister();

    ParallelHeapScanWorkloadInfo *workloadInfo =
        (ParallelHeapScanWorkloadInfo *)parallelController->GetWork(smpId);
    while (workloadInfo != NULL) {
        PageId firstPage = workloadInfo->GetStartPage();

        numPage++;
        numPage += workloadInfo->GetNumPagesLeft();
        numWorkloadInfoMetaPage++;

        workloadInfo = (ParallelHeapScanWorkloadInfo *)parallelController->GetWork(smpId);
    }

    ((UtInstance *)g_storageInstance)->ThreadUnregisterAndExit();
}

void ScanExtent(ParallelHeapScanWorkloadInfo *parallelHeapScan, uint32 expectPageCount, std::vector<PageId> pageVec)
{
    ((UtInstance *)g_storageInstance)->ThreadSetupAndRegister();

    EXPECT_EQ(parallelHeapScan->GetStartPage(), pageVec[0]);
    for (uint8 i = 1; i < expectPageCount - 1; i++) {
        EXPECT_EQ(parallelHeapScan->GetNextPage(), pageVec[i]);
    }

    ((UtInstance *)g_storageInstance)->ThreadUnregisterAndExit();
}

bool UTParallelHeapScanWorkloadInfoTest::IsGetNextPageValid(ParallelHeapScanWorkloadInfo parallelHeapScan, PageId extentMetaPage) {
    BufferDesc *extMetaBufDesc = m_testBufMgr->Read(g_defaultPdbId, extentMetaPage, LW_SHARED);
    if (extMetaBufDesc == INVALID_BUFFER_DESC) {
        m_testBufMgr->UnlockAndRelease(extMetaBufDesc);
        return false;
    }

    SegExtentMetaPage *extMetaPage = static_cast<SegExtentMetaPage *>(extMetaBufDesc->GetPage());
    uint16 extSz = extMetaPage->GetSelfExtentSize();
    m_testBufMgr->UnlockAndRelease(extMetaBufDesc);

    for (uint16 i = 1; i < extSz; ++i) {
        if (parallelHeapScan.GetNextPage() == INVALID_PAGE_ID) {
            return false;
        }
    }

    return true;
}

/*
 * ParallelHeapScanWorkloadInfo's start page must match the first extent data page.
*/
TEST_F(UTParallelHeapScanWorkloadInfoTest, GetStartPage_level0)
{
    HeapSegment *segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment));

    DataSegmentMetaPage *headPage;
    BufferDesc *bufferDesc;
    PageId segmentPageId = segment->GetSegmentMetaPageId();

    ReadBufferAndPage(segmentPageId, LW_SHARED, bufferDesc, headPage);
    PageId lastPageId = headPage->GetLastPageId();
    m_testBufMgr->UnlockAndRelease(bufferDesc);

    ParallelHeapScanWorkloadInfo parallelHeapScan(g_defaultPdbId, segmentPageId, m_testBufMgr, lastPageId);

    PageId parallelHeapScanStartPage = parallelHeapScan.GetStartPage();
    ASSERT_NE(parallelHeapScanStartPage, INVALID_PAGE_ID);

    /* (segmentPageId.m_blockId + (uint32_t)1) is the page after extent meta page. */
    EXPECT_EQ(parallelHeapScanStartPage.m_blockId, (segmentPageId.m_blockId + (uint32_t)1));

    segment->DropSegment();
    delete segment;
}

/*
 * ParallelHeapScanWorkloadInfo is able to get new pages within an extent.
 * If the number of pages exceeds current extent's size, return INVALID PAGE ID.
*/
TEST_F(UTParallelHeapScanWorkloadInfoTest, GetNextPage_level0)
{
    HeapSegment *segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment));

    for (uint8 i = 0; i < 20; ++i) {
        EXPECT_NE(segment->GetNewPage(), INVALID_PAGE_ID);
    }

    DataSegmentMetaPage *headPage;
    BufferDesc *bufferDesc;
    PageId segmentPageId = segment->GetSegmentMetaPageId();

    ReadBufferAndPage(segmentPageId, LW_SHARED, bufferDesc, headPage);
    PageId lastPageId = headPage->GetLastPageId();
    m_testBufMgr->UnlockAndRelease(bufferDesc);

    ParallelHeapScanWorkloadInfo parallelHeapScan(g_defaultPdbId, segmentPageId, m_testBufMgr, lastPageId);

    EXPECT_TRUE(IsGetNextPageValid(parallelHeapScan, segmentPageId));

    segment->DropSegment();
    delete segment;
}

/*
 * ParallelHeapScanWorkloadInfo is able to get the number of pages remaining in the current extent.
 * If getting more pages than the number of pages remaining, return INVALID PAGE ID.
*/
TEST_F(UTParallelHeapScanWorkloadInfoTest, GetNumPagesLeft_level0)
{
    HeapSegment *segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment));

    for (uint8 i = 0; i < 20; ++i) {
        EXPECT_NE(segment->GetNewPage(), INVALID_PAGE_ID);
    }

    DataSegmentMetaPage *headPage;
    BufferDesc *bufferDesc;
    PageId segmentPageId = segment->GetSegmentMetaPageId();

    ReadBufferAndPage(segmentPageId, LW_SHARED, bufferDesc, headPage);
    PageId lastPageId = headPage->GetLastPageId();
    m_testBufMgr->UnlockAndRelease(bufferDesc);

    ParallelHeapScanWorkloadInfo parallelHeapScan(g_defaultPdbId, segmentPageId, m_testBufMgr, lastPageId);

    EXPECT_NE(parallelHeapScan.GetNextPage(), INVALID_PAGE_ID);
    uint8 numPagesLeft = parallelHeapScan.GetNumPagesLeft();

    for (uint8 i = 0; i < numPagesLeft; ++i) {
        EXPECT_NE(parallelHeapScan.GetNextPage(), INVALID_PAGE_ID);
    }
    EXPECT_EQ(parallelHeapScan.GetNextPage(), INVALID_PAGE_ID);

    segment->DropSegment();
    delete segment;
}

/*
 * ParallelHeapScanWorkloadInfo scan two different extents with GetNextPage.
*/
TEST_F(UTParallelHeapScanWorkloadInfoTest, GetNextPageParallel_level0)
{
    HeapSegment *segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment));

    for (uint8 i = 0; i < 20; ++i) {
        EXPECT_NE(segment->GetNewPage(), INVALID_PAGE_ID);
    }

    BufferDesc *bufferDesc;
    PageId segmentPageId = segment->GetSegmentMetaPageId();
    EXPECT_NE(segmentPageId, INVALID_PAGE_ID);

    PageId curExtMetaId1 = segmentPageId;
    /* Get segment's last page id, first extent's size and second extent's meta page id */
    BufferDesc *extMetaBufDesc1 = m_testBufMgr->Read(g_defaultPdbId, segmentPageId, LW_SHARED);
    EXPECT_NE(extMetaBufDesc1, INVALID_BUFFER_DESC);

    SegExtentMetaPage *extMetaPage1 = static_cast<SegExtentMetaPage *>(extMetaBufDesc1->GetPage());
    PageId lastPageId = (static_cast<DataSegmentMetaPage *>(extMetaPage1))->GetLastPageId();
    uint16 extSz1 = extMetaPage1->GetSelfExtentSize();
    PageId curExtMetaId2 = extMetaPage1->GetNextExtentMetaPageId();
    m_testBufMgr->UnlockAndRelease(extMetaBufDesc1);

    EXPECT_NE(curExtMetaId2, INVALID_PAGE_ID);
    EXPECT_NE(curExtMetaId1, curExtMetaId2);

    ParallelHeapScanWorkloadInfo parallelHeapScan1(g_defaultPdbId, curExtMetaId1, m_testBufMgr, lastPageId);
    ParallelHeapScanWorkloadInfo parallelHeapScan2(g_defaultPdbId, curExtMetaId2, m_testBufMgr, lastPageId);

    BufferDesc *extMetaBufDesc2 = m_testBufMgr->Read(g_defaultPdbId, curExtMetaId2, LW_SHARED);
    SegExtentMetaPage *extMetaPage2 = static_cast<SegExtentMetaPage *>(extMetaBufDesc2->GetPage());
    uint16 extSz2 = extMetaPage2->GetSelfExtentSize();
    m_testBufMgr->UnlockAndRelease(extMetaBufDesc2);

    std::vector<PageId> pageVec1;
    pageVec1.push_back(parallelHeapScan1.GetStartPage());
    for (uint16 i = 1; i < extSz1 - 1; ++i) {
        pageVec1.push_back(parallelHeapScan1.GetNextPage());
    }

    std::vector<PageId> pageVec2;
    pageVec2.push_back(parallelHeapScan2.GetStartPage());
    for (uint16 i = 1; i < extSz2 - 1; ++i) {
        pageVec2.push_back(parallelHeapScan2.GetNextPage());
    }

    std::thread scanThread1 = std::thread(ScanExtent, &parallelHeapScan1, extSz1, pageVec1);
    std::thread scanThread2 = std::thread(ScanExtent, &parallelHeapScan2, extSz2, pageVec2);

    scanThread1.join();
    scanThread2.join();

    segment->DropSegment();
    delete segment;
}

/*
 * ParallelHeapScanWorkSource's GetWork() returns a valid ParallelHeapScanWorkloadInfo which is able to GetNextPage.
*/
TEST_F(UTParallelHeapScanWorkloadInfoTest, GetWork001_level0)
{
    HeapSegment *segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment));

    for (uint8 i = 0; i < 20; ++i) {
        EXPECT_NE(segment->GetNewPage(), INVALID_PAGE_ID);
    }

    PageId segmentPageId = segment->GetSegmentMetaPageId();

    ParallelHeapScanWorkSource parallelHeapScan(g_defaultPdbId, segmentPageId, m_testBufMgr);
    EXPECT_EQ(parallelHeapScan.Init(), DSTORE_SUCC);
    ParallelHeapScanWorkloadInfo *result = (ParallelHeapScanWorkloadInfo *)parallelHeapScan.GetWork();

    EXPECT_TRUE(IsGetNextPageValid(*result, segmentPageId));

    segment->DropSegment();
    delete segment;
}

/*
 * ParallelHeapScanWorkSource's Reset() test.
*/
TEST_F(UTParallelHeapScanWorkloadInfoTest, Reset_level0)
{
    HeapSegment *segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment));

    for (uint8 i = 0; i < 10; ++i) {
        EXPECT_NE(segment->GetNewPage(), INVALID_PAGE_ID);
    }

    PageId segmentPageId = segment->GetSegmentMetaPageId();

    ParallelHeapScanWorkSource parallelHeapScan(g_defaultPdbId, segmentPageId, m_testBufMgr);
    EXPECT_EQ(parallelHeapScan.Init(), DSTORE_SUCC);

    uint8 getWorkBeforeReset = 0, getWorkAfterReset = 0;
    while (parallelHeapScan.GetWork() != nullptr) {
        getWorkBeforeReset++;
    }

    EXPECT_EQ(parallelHeapScan.Reset(), DSTORE_SUCC);

    while (parallelHeapScan.GetWork() != nullptr) {
        getWorkAfterReset++;
    }

    EXPECT_EQ(getWorkBeforeReset, getWorkAfterReset);

    segment->DropSegment();
    delete segment;
}

/*
 * ParallelWorkController IsInitialized() returns true when ParallelHeapScan is initialized.
*/
TEST_F(UTParallelHeapScanWorkloadInfoTest, IsInitialized_level0)
{
    HeapSegment *segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment));

    PageId segmentPageId = segment->GetSegmentMetaPageId();

    ParallelWorkController parallelController(1);
    EXPECT_FALSE(parallelController.IsInitialized());

    parallelController.InitParallelHeapScan(g_defaultPdbId, segmentPageId);
    EXPECT_TRUE(parallelController.IsInitialized());

    segment->DropSegment();
    delete segment;
}

/*
 * ParallelWorkController's GetWork() returns ParallelHeapScanWorkloadInfo with pages.
*/
TEST_F(UTParallelHeapScanWorkloadInfoTest, GetWork002_level0)
{
    HeapSegment *segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment));

    for (uint8 i = 0; i < 4; ++i) {
        EXPECT_NE(segment->GetNewPage(), INVALID_PAGE_ID);
    }

    PageId segmentPageId = segment->GetSegmentMetaPageId();

    ParallelWorkController *parallelController = ParallelInterface::CreateParallelWorkController(1);
    parallelController->InitParallelHeapScan(g_defaultPdbId, segmentPageId);

    ParallelHeapScanWorkloadInfo *workloadInfo = (ParallelHeapScanWorkloadInfo *)parallelController->GetWork(0);
    PageId firstPage = workloadInfo->GetStartPage();
    uint pageNum = workloadInfo->GetNumPagesLeft();

    for (uint8 i = 0; i < pageNum; ++i) {
        EXPECT_NE(workloadInfo->GetNextPage(), INVALID_PAGE_ID);
    }

    EXPECT_EQ(workloadInfo->GetNextPage(), INVALID_PAGE_ID);

    ParallelInterface::DestroyParallelWorkController(parallelController);

    segment->DropSegment();
    delete segment;
}

/*
 * The number of extents read from ParallelHeapScanWorkSource's GetWork() is the same as
 * extMetaBufDesc's extent count.
*/
TEST_F(UTParallelHeapScanWorkloadInfoTest, GetWork003_level0)
{
    HeapSegment *segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment));

    for (uint8 i = 0; i < 20; i++) {
        PageId newPage = segment->GetNewPage();
    }
    PageId segmentPageId = segment->GetSegmentMetaPageId();

    BufferDesc *extMetaBufDesc = m_testBufMgr->
        Read(g_defaultPdbId, segmentPageId, LW_SHARED);
    DataSegmentMetaPage *headPage = static_cast<DataSegmentMetaPage*>(extMetaBufDesc->GetPage());
    uint8 numExtent = headPage->GetExtentCount();
    m_testBufMgr->UnlockAndRelease(extMetaBufDesc);

    ParallelWorkController *parallelController = ParallelInterface::CreateParallelWorkController(1);
    parallelController->InitParallelHeapScan(g_defaultPdbId, segmentPageId);

    ParallelHeapScanWorkloadInfo *workloadInfo = (ParallelHeapScanWorkloadInfo *)parallelController->GetWork(0);

    uint8 extentCount = 0;
    while (workloadInfo != nullptr) {
        extentCount++;
        workloadInfo = (ParallelHeapScanWorkloadInfo *)parallelController->GetWork(0);
    }

    EXPECT_EQ(numExtent, extentCount);

    ParallelInterface::DestroyParallelWorkController(parallelController);

    segment->DropSegment();
    delete segment;
}

TEST_F(UTParallelHeapScanWorkloadInfoTest, GetWork004_level0)
{
    HeapSegment *segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment));

    PageId segmentPageId = segment->GetSegmentMetaPageId();

    ParallelWorkController *parallelController = ParallelInterface::CreateParallelWorkController(1);
    parallelController->InitParallelHeapScan(g_defaultPdbId, segmentPageId);

    ParallelHeapScanWorkloadInfo *workloadInfo = (ParallelHeapScanWorkloadInfo *)parallelController->GetWork(0);
    EXPECT_EQ(workloadInfo, nullptr);

    ParallelInterface::DestroyParallelWorkController(parallelController);

    segment->DropSegment();
    delete segment;
}

/*
 * ParallelWorkController Done() returns true when parallelController has no more ParallelHeapScanWorkloadInfo.
*/
TEST_F(UTParallelHeapScanWorkloadInfoTest, Done_level0)
{
    HeapSegment *segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment));

    for (uint8 i = 0; i < 4; ++i) {
        EXPECT_NE(segment->GetNewPage(), INVALID_PAGE_ID);
    }
    PageId segmentPageId = segment->GetSegmentMetaPageId();

    ParallelWorkController *parallelController = ParallelInterface::CreateParallelWorkController(1);
    parallelController->InitParallelHeapScan(g_defaultPdbId, segmentPageId);

    ParallelHeapScanWorkloadInfo *workloadInfo = (ParallelHeapScanWorkloadInfo *)parallelController->GetWork(0);
    while (workloadInfo != nullptr) {
        PageId firstPage = workloadInfo->GetStartPage();
        uint8 pageNum = workloadInfo->GetNumPagesLeft();
        for (uint8 i = 0; i < pageNum; ++i) {
            EXPECT_NE(workloadInfo->GetNextPage(), INVALID_PAGE_ID);
        }
        EXPECT_EQ(workloadInfo->GetNextPage(), INVALID_PAGE_ID);

        workloadInfo = (ParallelHeapScanWorkloadInfo *)parallelController->GetWork(0);
    }

    EXPECT_EQ(workloadInfo, nullptr);
    EXPECT_TRUE(parallelController->Done());

    ParallelInterface::DestroyParallelWorkController(parallelController);

    segment->DropSegment();
    delete segment;
}

/*
 * ParallelWorkController distributes ParallelHeapScanWorkloadInfo to threads, and pages are scanned in parallel.
*/
TEST_F(UTParallelHeapScanWorkloadInfoTest, WorkloadInfoScanParallel_level0)
{
    HeapSegment *segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment));

    DataSegmentMetaPage *headPage;
    BufferDesc *bufferDesc;
    for (uint8 i = 0; i < 8; i++) {
        PageId newPage = segment->GetNewPage();
    }
    PageId segmentPageId = segment->GetSegmentMetaPageId();

    ReadBufferAndPage(segmentPageId, LW_SHARED, bufferDesc, headPage);
    uint64 pageCount = headPage->GetTotalBlockCount();
    m_testBufMgr->UnlockAndRelease(bufferDesc);

    ParallelWorkController *parallelController = ParallelInterface::CreateParallelWorkController(4);
    parallelController->InitParallelHeapScan(g_defaultPdbId, segmentPageId);

    std::unique_ptr<StorageRelationData> heapRel(new StorageRelationData());
    heapRel->tableSmgr = m_utTableHandler->GetHeapTabSmgr();
    HeapScanHandler *heapScanHandler = DstoreNew(thrd->GetTransactionMemoryContext())
        HeapScanHandler(g_storageInstance, thrd, heapRel.get());

    std::thread threads[4];

    std::atomic<int> numPage(0);
    std::atomic<int> numMetaPage(0);

    for (uint8 i = 0; i < 4; ++i) {
        threads[i] = std::thread(ScanWorkloadInfo, parallelController, std::ref(numPage), std::ref(numMetaPage), i);
        threads[i].join();
    }

    heapScanHandler->End();

    ParallelInterface::DestroyParallelWorkController(parallelController);

    uint64 sumWorkloadInfoPage = numPage + numMetaPage;
    EXPECT_EQ(pageCount, sumWorkloadInfoPage);

    segment->DropSegment();
    delete segment;
}

/*
 * The difference between ParallelHeapScanWorkloadInfo's start page and end page must equal the extent size.
 * Additionally, the file IDs of startPageId and endPageId must be the same.
*/
TEST_F(UTParallelHeapScanWorkloadInfoTest, StartEndPageDifference_level0)
{
    HeapSegment *segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment));

    DataSegmentMetaPage *headPage;
    BufferDesc *bufferDesc;
    PageId segmentPageId = segment->GetSegmentMetaPageId();

    ReadBufferAndPage(segmentPageId, LW_SHARED, bufferDesc, headPage);
    PageId lastPageId = headPage->GetLastPageId();
    m_testBufMgr->UnlockAndRelease(bufferDesc);

    ParallelHeapScanWorkloadInfo parallelHeapScan(g_defaultPdbId, segmentPageId, m_testBufMgr, lastPageId);

    PageId startPageId = parallelHeapScan.GetStartPage();
    PageId endPageId = parallelHeapScan.GetEndPage();

    // Verify that the file IDs of startPageId and endPageId are the same
    EXPECT_EQ(startPageId.m_fileId, endPageId.m_fileId);

    BufferDesc *extMetaBufDesc = m_testBufMgr->Read(g_defaultPdbId, segmentPageId, LW_SHARED);
    SegExtentMetaPage *extMetaPage = static_cast<SegExtentMetaPage *>(extMetaBufDesc->GetPage());
    uint16 extSize = extMetaPage->GetSelfExtentSize();
    m_testBufMgr->UnlockAndRelease(extMetaBufDesc);

    // Calculate the difference between startPageId.m_blockId and endPageId.m_blockId,
    uint32_t blockDiff = endPageId.m_blockId - startPageId.m_blockId + 1;
    // The sum of difference and extMetaPage should be equal to extSize
    EXPECT_EQ(blockDiff + 1, extSize);

    segment->DropSegment();
    delete segment;
}

/*
 * All pages within the extent size must be valid when calling GetNextPage().
*/
TEST_F(UTParallelHeapScanWorkloadInfoTest, GetNextPageValid_level0)
{
    // Create a HeapSegment
    HeapSegment *segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment));

    // Get the segment's metadata page ID
    PageId segmentPageId = segment->GetSegmentMetaPageId();

    // Read the metadata page
    BufferDesc *bufferDesc;
    DataSegmentMetaPage *headPage;
    ReadBufferAndPage(segmentPageId, LW_SHARED, bufferDesc, headPage);
    PageId lastPageId = headPage->GetLastPageId();
    m_testBufMgr->UnlockAndRelease(bufferDesc);

    // Read the extent's metadata page to get extSz
    BufferDesc *extMetaBufDesc = m_testBufMgr->Read(g_defaultPdbId, segmentPageId, LW_SHARED);
    SegExtentMetaPage *extMetaPage = static_cast<SegExtentMetaPage *>(extMetaBufDesc->GetPage());
    uint16 extSz = extMetaPage->GetSelfExtentSize();
    m_testBufMgr->UnlockAndRelease(extMetaBufDesc);

    // Initialize the ParallelHeapScanWorkloadInfo object
    ParallelHeapScanWorkloadInfo parallelHeapScan(g_defaultPdbId, segmentPageId, m_testBufMgr, lastPageId);

    // Loop extSz-1 times to check if each PageId is valid
    for (uint16 i = 0; i < extSz - 1; ++i) {
        PageId nextPage = parallelHeapScan.GetNextPage();
        EXPECT_NE(nextPage, INVALID_PAGE_ID);
    }

    // Check the termination condition
    PageId nextPage = parallelHeapScan.GetNextPage();
    EXPECT_EQ(nextPage, INVALID_PAGE_ID);

    // Clean up resources
    segment->DropSegment();
    delete segment;
}

/*
 * ParallelHeapScanWorkSource's GetWork() and Reset() test, ensuring that after two GetWork() calls and a Reset(),
 * the first extent information remains consistent.
 */
TEST_F(UTParallelHeapScanWorkloadInfoTest, GetWork_Reset_ExtentConsistency_level0)
{
    HeapSegment *segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment));

    // Allocate 20 pages to create two extents
    for (uint8 i = 0; i < 30; ++i) {
        EXPECT_NE(segment->GetNewPage(), INVALID_PAGE_ID);
    }

    DataSegmentMetaPage *headPage;
    BufferDesc *bufferDesc;
    PageId segmentPageId = segment->GetSegmentMetaPageId();

    ReadBufferAndPage(segmentPageId, LW_SHARED, bufferDesc, headPage);
    PageId lastPageId = headPage->GetLastPageId();
    m_testBufMgr->UnlockAndRelease(bufferDesc);

    // Create ParallelHeapScanWorkSource and initialize
    ParallelHeapScanWorkSource parallelHeapScan(g_defaultPdbId, segmentPageId, m_testBufMgr);
    EXPECT_EQ(parallelHeapScan.Init(), DSTORE_SUCC);

    // First call to GetWork()
    ParallelHeapScanWorkloadInfo *result1 = (ParallelHeapScanWorkloadInfo *)parallelHeapScan.GetWork();
    PageId startPage1 = result1->GetStartPage();

    // Second call to GetWork()
    ParallelHeapScanWorkloadInfo *result2 = (ParallelHeapScanWorkloadInfo *)parallelHeapScan.GetWork();

    // Reset the scan
    EXPECT_EQ(parallelHeapScan.Reset(), DSTORE_SUCC);

    ParallelHeapScanWorkloadInfo *result3 = (ParallelHeapScanWorkloadInfo *)parallelHeapScan.GetWork();
    PageId startPage3 = result3->GetStartPage();
    EXPECT_EQ(startPage3, startPage1);

    segment->DropSegment();
    delete segment;
}

/*
 * ParallelHeapScanWorkloadInfo can correctly scan two different extents in a multi-threaded environment,
 * ensuring that each thread's start and end pages match the extent's first and last data pages,
 * and all pages within the extent are valid.
 */
TEST_F(UTParallelHeapScanWorkloadInfoTest, GetNextPageParallel_ExtentValidation_level0)
{
    HeapSegment *segment = UtAllocNewHeapSegment();
    ASSERT_TRUE(SegmentIsValid(segment));

    // Allocate 20 pages to create two extents
    for (uint8 i = 0; i < 20; ++i) {
        EXPECT_NE(segment->GetNewPage(), INVALID_PAGE_ID);
    }

    DataSegmentMetaPage *headPage;
    BufferDesc *bufferDesc;
    PageId segmentPageId = segment->GetSegmentMetaPageId();
    EXPECT_NE(segmentPageId, INVALID_PAGE_ID);

    ReadBufferAndPage(segmentPageId, LW_SHARED, bufferDesc, headPage);
    PageId lastPageId = headPage->GetLastPageId();
    m_testBufMgr->UnlockAndRelease(bufferDesc);

    // Read the first extent's metadata
    BufferDesc *extMetaBufDesc1 = m_testBufMgr->Read(g_defaultPdbId, segmentPageId, LW_SHARED);
    SegExtentMetaPage *extMetaPage1 = static_cast<SegExtentMetaPage *>(extMetaBufDesc1->GetPage());
    uint16 extSz1 = extMetaPage1->GetSelfExtentSize();
    PageId curExtMetaId2 = extMetaPage1->GetNextExtentMetaPageId();
    m_testBufMgr->UnlockAndRelease(extMetaBufDesc1);

    EXPECT_NE(curExtMetaId2, INVALID_PAGE_ID);
    EXPECT_NE(segmentPageId, curExtMetaId2);

    // Read the second extent's metadata
    BufferDesc *extMetaBufDesc2 = m_testBufMgr->Read(g_defaultPdbId, curExtMetaId2, LW_SHARED);
    SegExtentMetaPage *extMetaPage2 = static_cast<SegExtentMetaPage *>(extMetaBufDesc2->GetPage());
    uint16 extSz2 = extMetaPage2->GetSelfExtentSize();
    m_testBufMgr->UnlockAndRelease(extMetaBufDesc2);

    // Create workload info for both extents
    ParallelHeapScanWorkloadInfo parallelHeapScan1(g_defaultPdbId, segmentPageId, m_testBufMgr, lastPageId);
    ParallelHeapScanWorkloadInfo parallelHeapScan2(g_defaultPdbId, curExtMetaId2, m_testBufMgr, lastPageId);

    // Verify start and end pages for the first extent
    PageId startPage1 = parallelHeapScan1.GetStartPage();
    PageId endPage1 = parallelHeapScan1.GetEndPage();
    EXPECT_NE(startPage1, INVALID_PAGE_ID);
    EXPECT_NE(endPage1, INVALID_PAGE_ID);
    EXPECT_LT(startPage1.m_blockId, endPage1.m_blockId);

    // Verify start and end pages for the second extent
    PageId startPage2 = parallelHeapScan2.GetStartPage();
    PageId endPage2 = parallelHeapScan2.GetEndPage();
    EXPECT_NE(startPage2, INVALID_PAGE_ID);
    EXPECT_NE(endPage2, INVALID_PAGE_ID);
    EXPECT_LT(startPage2.m_blockId, endPage2.m_blockId);

    // Prepare page vectors for each extent
    std::vector<PageId> pageVec1;
    pageVec1.push_back(startPage1);
    for (uint16 i = 1; i < extSz1 - 1; ++i) {
        PageId nextPage = parallelHeapScan1.GetNextPage();
        EXPECT_NE(nextPage, INVALID_PAGE_ID);
        pageVec1.push_back(nextPage);
    }

    std::vector<PageId> pageVec2;
    pageVec2.push_back(startPage2);
    for (uint16 i = 1; i < extSz2 - 1; ++i) {
        PageId nextPage = parallelHeapScan2.GetNextPage();
        EXPECT_NE(nextPage, INVALID_PAGE_ID);
        pageVec2.push_back(nextPage);
    }

    // Scan each extent in separate threads
    std::thread scanThread1 = std::thread(ScanExtent, &parallelHeapScan1, extSz1, pageVec1);
    std::thread scanThread2 = std::thread(ScanExtent, &parallelHeapScan2, extSz2, pageVec2);

    scanThread1.join();
    scanThread2.join();

    segment->DropSegment();
    delete segment;
}
