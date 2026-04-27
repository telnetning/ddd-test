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
 * dstore_data_segment_context.cpp
 *
 * IDENTIFICATION
 *        src/tablespace/dstore_data_segment_context.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "tablespace/dstore_data_segment_context.h"
#include "common/log/dstore_log.h"
#include "page/dstore_data_segment_meta_page.h"

namespace DSTORE {
DataSegmentScanContext::DataSegmentScanContext(BufMgrInterface *bufMgr, StorageRelation stoRel, SmgrType smgrType)
{
    m_rel = stoRel;
    m_bufMgr = bufMgr;
    m_smgrType = smgrType;
    Reset();
}

DataSegmentScanContext::~DataSegmentScanContext()
{
    m_bufMgr = nullptr;
    m_rel = nullptr;
}

void DataSegmentScanContext::Reset()
{
    m_lastScanPageId = INVALID_PAGE_ID;
}

Segment* DataSegmentScanContext::GetSegment()
{
    StorageAssert(m_smgrType != SmgrType::INVALID_SMGR_TYPE);
    if (unlikely(m_rel == nullptr)) {
        return nullptr;
    }
    if (m_smgrType == SmgrType::INDEX_SMGR) {
        return dynamic_cast<Segment*>(m_rel->GetBtreeSmgrSegment());
    }
    return dynamic_cast<Segment*>(m_smgrType == SmgrType::LOB_SMGR ?
        m_rel->GetLobSmgrSegment() : m_rel->GetTableSmgrSegment());
}

PageId DataSegmentScanContext::GetFirstPageId()
{
    Segment* dataSegment = GetSegment();
    if (unlikely(dataSegment == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
               ErrMsg("Get datasegment failed when scan first data page, smgrType : %d", (int)m_smgrType));
        return INVALID_PAGE_ID;
    }
    BufferDesc *segMetaPageBufDesc =
        m_bufMgr->Read(dataSegment->GetPdbId(), dataSegment->GetSegmentMetaPageId(), LW_SHARED);
    STORAGE_CHECK_BUFFER_PANIC(segMetaPageBufDesc, MODULE_HEAP,
                               dataSegment->GetSegmentMetaPageId());
    DataSegmentMetaPage *segmentMetaPage = static_cast<DataSegmentMetaPage *>(segMetaPageBufDesc->GetPage());
    m_curScanExtMetaPageId = segmentMetaPage->GetSelfPageId();
    m_curScanExtPageNum = segmentMetaPage->GetSelfExtentSize();
    m_endScanPageId = segmentMetaPage->GetLastPageId();
    PageId firstPageId = segmentMetaPage->GetFirstPageId();
    m_bufMgr->UnlockAndRelease(segMetaPageBufDesc);
    m_lastScanPageId = firstPageId;

    if (InParallelMode()) {
        return GetFirstPageParallel();
    }

    ErrLog(DSTORE_DEBUG1, MODULE_SEGMENT,
           ErrMsg("Data segment scan first data page (%hu, %u)", firstPageId.m_fileId, firstPageId.m_blockId));
    return firstPageId;
}

PageId DataSegmentScanContext::GetEndPageId()
{
    return m_endScanPageId;
}

PageId DataSegmentScanContext::MoveExtents()
{
    Segment* dataSegment = GetSegment();
    if (unlikely(dataSegment == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
               ErrMsg("Get datasegment failed when MoveExtents, smgrType : %d", (int)m_smgrType));
        return INVALID_PAGE_ID;
    }
    PageId nextPageId = INVALID_PAGE_ID;
    PageId nextExtentMetaPageId = INVALID_PAGE_ID;
    BufferDesc *extMetaBufDesc = m_bufMgr->Read(dataSegment->GetPdbId(), m_curScanExtMetaPageId, LW_SHARED);
    STORAGE_CHECK_BUFFER_PANIC(extMetaBufDesc, MODULE_HEAP,
                               m_curScanExtMetaPageId);
    SegExtentMetaPage *extMetaPage = static_cast<SegExtentMetaPage *>(extMetaBufDesc->GetPage());
    nextExtentMetaPageId = extMetaPage->GetNextExtentMetaPageId();
    m_bufMgr->UnlockAndRelease(extMetaBufDesc);

    /*
        * we tried to move extents, but no extents remain
        * return invalid page id  to terminate scan
    */
    if (nextExtentMetaPageId == INVALID_PAGE_ID) {
        nextPageId = INVALID_PAGE_ID;
        return nextPageId;
    }

    /* Read next extent meta page */
    extMetaBufDesc = m_bufMgr->Read(dataSegment->GetPdbId(), nextExtentMetaPageId, LW_SHARED);
    STORAGE_CHECK_BUFFER_PANIC(extMetaBufDesc, MODULE_HEAP,
                               nextExtentMetaPageId);
    extMetaPage = static_cast<SegExtentMetaPage *>(extMetaBufDesc->GetPage());
    m_curScanExtMetaPageId = extMetaPage->GetSelfPageId();
    m_curScanExtPageNum = extMetaPage->GetSelfExtentSize();
    m_bufMgr->UnlockAndRelease(extMetaBufDesc);
    nextPageId = {m_curScanExtMetaPageId.m_fileId, m_curScanExtMetaPageId.m_blockId + 1};
#ifdef DSTORE_USE_ASSERT_CHECKING
    ErrLog(DSTORE_DEBUG1, MODULE_SEGMENT,
           ErrMsg("Data segment scan next data page (%hu, %u)", m_curScanExtMetaPageId.m_fileId,
                  m_curScanExtMetaPageId.m_blockId));
#endif
    return nextPageId;
}

PageId DataSegmentScanContext::GetNextPageId()
{
    if (InParallelMode()) {
        return GetNextPageParallel();
    }
    PageId nextPageId = INVALID_PAGE_ID;
    if (unlikely((!m_lastScanPageId.IsValid()))) {
        return GetFirstPageId();
    }
    if (m_lastScanPageId != m_endScanPageId) {
        PageId curExtentEndPageId = {m_curScanExtMetaPageId.m_fileId,
                                     m_curScanExtMetaPageId.m_blockId + m_curScanExtPageNum - 1};
        /* Last scan page is extent end page, read current extent meta page to get next extent */
        if (curExtentEndPageId == m_lastScanPageId) {
            nextPageId = MoveExtents();
        } else {
            /* Next page of m_lastScanPage */
            StorageAssert(m_lastScanPageId.m_blockId + 1 <= DSTORE_MAX_BLOCK_NUMBER);
            nextPageId = {m_lastScanPageId.m_fileId, m_lastScanPageId.m_blockId + 1};
        }
        StorageAssert(nextPageId == INVALID_PAGE_ID ||
           (nextPageId.m_blockId <= m_curScanExtMetaPageId.m_blockId + m_curScanExtPageNum));
    }
    m_lastScanPageId = nextPageId;
    ErrLog(DSTORE_DEBUG1, MODULE_SEGMENT,
           ErrMsg("Data segment scan next data page (%hu, %u)", nextPageId.m_fileId, nextPageId.m_blockId));
    return nextPageId;
}

void DataSegmentScanContext::SetParallelController(ParallelWorkController *controller, int smpId)
{
    m_controller = controller;
    m_smpId = smpId;
}

PageId DataSegmentScanContext::GetFirstPageParallel()
{
    StorageAssert(m_controller);
    m_curWorkload = m_controller->GetWork(m_smpId);
    if (!m_curWorkload) {
        return INVALID_PAGE_ID;
    }
    m_lastScanPageId = m_curWorkload->GetStartPage();
    return m_lastScanPageId;
}

PageId DataSegmentScanContext::GetNextPageParallel()
{
    /* Get the first page */
    if (!m_curWorkload) {
        return GetFirstPageParallel();
    }
    /* Get next page from the workload */
    PageId nextPageId = m_curWorkload->GetNextPage();
    if (!nextPageId.IsValid() && !m_controller->Done()) {
        m_curWorkload = m_controller->GetWork(m_smpId);
        if (m_curWorkload) {
            nextPageId = m_curWorkload->GetStartPage();
        }
    }

    m_lastScanPageId = nextPageId;
    return nextPageId;
}

void DataSegmentScanContext::SetRescan()
{
    if (InParallelMode()) {
        m_controller->Rescan(m_smpId);
    }
}
} /* The end of namespace DSTORE */
