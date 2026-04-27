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
 * dstore_heap_scan_parallel.cpp
 *
 *
 *
 * IDENTIFICATION
 *        storage/src/heap/dstore_heap_scan_parallel.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "heap/dstore_heap_scan_parallel.h"
#include "page/dstore_data_segment_meta_page.h"

namespace DSTORE {

ParallelHeapScanWorkSource::ParallelHeapScanWorkSource(PdbId pdbId, PageId segmentPageId, BufMgrInterface *bufMgr)
    : m_pdbId(pdbId),
      m_segmentMetaPageId(segmentPageId),
      m_curExtMetaPageId(INVALID_PAGE_ID),
      m_curExtEndPageId(INVALID_PAGE_ID),
      m_segEndPageId(INVALID_PAGE_ID),
      m_firstExtentSize(0),
      m_bufMgr(bufMgr)
{}

ParallelHeapScanWorkSource::~ParallelHeapScanWorkSource()
{}

RetStatus ParallelHeapScanWorkSource::Init()
{
    StorageAssert(m_segmentMetaPageId.IsValid());
    m_curExtMetaPageId = m_segmentMetaPageId;
    BufferDesc *segMetaBufDesc = m_bufMgr->Read(m_pdbId, m_segmentMetaPageId, LW_SHARED);
    if (STORAGE_VAR_NULL(segMetaBufDesc)) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP,
            ErrMsg("Read page(%hu, %u) failed when init buffer desc for parallel heap scan.",
                m_segmentMetaPageId.m_fileId, m_segmentMetaPageId.m_blockId));
        return DSTORE_FAIL;
    }
    DataSegmentMetaPage *segMetaPage = static_cast<DataSegmentMetaPage*>(segMetaBufDesc->GetPage());
    m_firstExtentSize = segMetaPage->GetSelfExtentSize();
    m_curExtEndPageId = {m_curExtMetaPageId.m_fileId, m_curExtMetaPageId.m_blockId + m_firstExtentSize - 1};
    m_segEndPageId = segMetaPage->GetLastPageId();
    m_bufMgr->UnlockAndRelease(segMetaBufDesc);
    if (!m_segEndPageId.IsValid()) {
        ErrLog(DSTORE_LOG, MODULE_HEAP,
            ErrMsg("Last page(%hu, %u) is invalid.", m_segEndPageId.m_fileId, m_segEndPageId.m_blockId));
    }
    return DSTORE_SUCC;
}

PageId ParallelHeapScanWorkSource::GetMetaPageId()
{
    return m_segmentMetaPageId;
}

bool ParallelHeapScanWorkSource::InProgress()
{
    return m_curExtMetaPageId.IsValid();
}

RetStatus ParallelHeapScanWorkSource::Reset()
{
    StorageAssert(m_segmentMetaPageId.IsValid());
    m_curExtMetaPageId = m_segmentMetaPageId;
    m_curExtEndPageId = {m_curExtMetaPageId.m_fileId, m_curExtMetaPageId.m_blockId + m_firstExtentSize - 1};
    return DSTORE_SUCC;
}

ParallelWorkloadInfoInterface *ParallelHeapScanWorkSource::GetWork()
{
    if (!m_curExtMetaPageId.IsValid() || !m_segEndPageId.IsValid()) {
        return nullptr;
    }

    ParallelHeapScanWorkloadInfo *workload = DstoreNew(g_dstoreCurrentMemoryContext)
        ParallelHeapScanWorkloadInfo(m_pdbId, m_curExtMetaPageId, m_bufMgr, m_segEndPageId);
    if (unlikely(workload == nullptr)) {
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("ParallelHeapScanWorkSource GetWork fail."));
        return nullptr;
    }

    if (STORAGE_FUNC_FAIL(AdvanceExtent())) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
               ErrMsg("Parallel Heap Scan fail to scan extent with meta page id: (%hu, %u)",
                      m_curExtMetaPageId.m_fileId, m_curExtMetaPageId.m_blockId));
        return nullptr;
    }

    return workload;
}

RetStatus ParallelHeapScanWorkSource::AdvanceExtent()
{
    BufferDesc *extMetaBufDesc = m_bufMgr->Read(m_pdbId, m_curExtMetaPageId, LW_SHARED);
    STORAGE_CHECK_BUFFER_PANIC(extMetaBufDesc, MODULE_SEGMENT, m_curExtMetaPageId);

    if (m_curExtEndPageId.IsValid()) {
        if ((m_curExtMetaPageId < m_segEndPageId && m_segEndPageId < m_curExtEndPageId)
            || (m_curExtMetaPageId == m_segEndPageId) || (m_segEndPageId == m_curExtEndPageId)) {
            m_curExtMetaPageId = INVALID_PAGE_ID;
            m_bufMgr->UnlockAndRelease(extMetaBufDesc);
            return DSTORE_SUCC;
        }
    }

    SegExtentMetaPage *extMetaPage = static_cast<SegExtentMetaPage*>(extMetaBufDesc->GetPage());
    m_curExtMetaPageId = extMetaPage->GetNextExtentMetaPageId();
    m_bufMgr->UnlockAndRelease(extMetaBufDesc);

    if (m_curExtMetaPageId.IsValid()) {
        BufferDesc *newExtMetaBufDesc = m_bufMgr->Read(m_pdbId, m_curExtMetaPageId, LW_SHARED);
        SegExtentMetaPage *newExtMetaPage = static_cast<SegExtentMetaPage*>(newExtMetaBufDesc->GetPage());
        m_curExtEndPageId = {m_curExtMetaPageId.m_fileId, m_curExtMetaPageId.m_blockId +
            newExtMetaPage->GetSelfExtentSize() - 1};
        m_bufMgr->UnlockAndRelease(newExtMetaBufDesc);
    }

    return DSTORE_SUCC;
}

ParallelHeapScanWorkloadInfo::ParallelHeapScanWorkloadInfo(PdbId pdbId, PageId extMetaPageId,
                                                           BufMgrInterface *bufMgr, PageId segEndPageId)
    : m_pdbId(pdbId),
      m_curExtMetaPageId(extMetaPageId),
      m_lastScanPageId(INVALID_PAGE_ID),
      m_extEndPageId(INVALID_PAGE_ID),
      m_segEndPageId(segEndPageId),
      m_bufMgr(bufMgr),
      m_done(false)
{
}

ParallelHeapScanWorkloadInfo::~ParallelHeapScanWorkloadInfo()
{}

PageId ParallelHeapScanWorkloadInfo::GetStartPage()
{
    StorageAssert(m_curExtMetaPageId.IsValid());
    BufferDesc *extMetaBufDesc = m_bufMgr->Read(m_pdbId, m_curExtMetaPageId, LW_SHARED);
    STORAGE_CHECK_BUFFER_PANIC(extMetaBufDesc, MODULE_SEGMENT, m_curExtMetaPageId);
    SegExtentMetaPage *extMetaPage = static_cast<SegExtentMetaPage*>(extMetaBufDesc->GetPage());
    PageId extStartPageId = {m_curExtMetaPageId.m_fileId, m_curExtMetaPageId.m_blockId + 1};
    PageId extEndPageId = {m_curExtMetaPageId.m_fileId, m_curExtMetaPageId.m_blockId +
                           extMetaPage->GetSelfExtentSize() - 1};
    m_bufMgr->UnlockAndRelease(extMetaBufDesc);
    m_lastScanPageId = extStartPageId;
    m_extEndPageId = extEndPageId;
    if (!m_segEndPageId.IsValid()) {
        m_segEndPageId = m_extEndPageId;
    }
    return m_lastScanPageId;
}

PageId ParallelHeapScanWorkloadInfo::GetNextPage()
{
    PageId nextPageId = INVALID_PAGE_ID;
    if (unlikely((!m_lastScanPageId.IsValid()))) {
        return GetStartPage();
    }

    if (m_lastScanPageId < m_extEndPageId && m_lastScanPageId != m_segEndPageId) {
        /* Next page of m_lastScanPage */
        StorageAssert(m_lastScanPageId.m_blockId + 1 <= DSTORE_MAX_BLOCK_NUMBER);
        nextPageId = {m_lastScanPageId.m_fileId, m_lastScanPageId.m_blockId + 1};
        StorageAssert(nextPageId == INVALID_PAGE_ID || (nextPageId.m_blockId <= m_extEndPageId.m_blockId));
    }
    m_lastScanPageId = nextPageId;
    ErrLog(DSTORE_DEBUG1, MODULE_SEGMENT,
           ErrMsg("Data segment scan next data page (%hu, %u)", nextPageId.m_fileId, nextPageId.m_blockId));
    return nextPageId;
}

uint ParallelHeapScanWorkloadInfo::GetNumPagesLeft()
{
    if (!m_lastScanPageId.IsValid()) {
        return 0;
    }

    if (m_segEndPageId < m_curExtMetaPageId) {
        return m_extEndPageId.m_blockId - m_lastScanPageId.m_blockId;
    }

    PageId endPageId = DstoreMin(m_segEndPageId, m_extEndPageId);
    return endPageId.m_blockId - m_lastScanPageId.m_blockId;
}

PageId ParallelHeapScanWorkloadInfo::GetEndPage()
{
    if (!m_segEndPageId.IsValid()) {
        return m_extEndPageId;
    }

    if (m_segEndPageId < m_curExtMetaPageId) {
        return m_extEndPageId;
    }

    return DstoreMin(m_segEndPageId, m_extEndPageId);
}

void ParallelHeapScanWorkloadInfo::Reset()
{
    m_lastScanPageId = INVALID_PAGE_ID;
}

}
