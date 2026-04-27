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
 * dstore_data_segment.cpp
 *
 * IDENTIFICATION
 *        src/tablespace/dstore_data_segment.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "tablespace/dstore_data_segment.h"
#include "common/log/dstore_log.h"
#include "page/dstore_data_segment_meta_page.h"
#include "page/dstore_index_page.h"
#include "page/dstore_heap_page.h"
#include "wal/dstore_wal_write_context.h"
#include "tablespace/dstore_tablespace_wal.h"
#include "tablespace/dstore_tablespace.h"
#include "lock/dstore_lock_interface.h"

using namespace DSTORE;

DataSegment::DataSegment(PdbId pdbId, const PageId &segmentId, SegmentType type, TablespaceId tablespaceId,
                         BufMgrInterface *bufMgr, DstoreMemoryContext ctx)
    : Segment(pdbId, segmentId, type, tablespaceId, bufMgr), m_maxAddNewPageCount(PAGES_ADD_TO_FSM_PER_TIME),
      m_initDataPageCallback(nullptr), m_ctx(ctx)
{
    switch (type) {
        case SegmentType::HEAP_SEGMENT_TYPE:
        case SegmentType::HEAP_TEMP_SEGMENT_TYPE:
            m_initDataPageCallback = HeapPage::InitHeapPage;
            break;
        case SegmentType::INDEX_SEGMENT_TYPE:
        case SegmentType::INDEX_TEMP_SEGMENT_TYPE:
            m_initDataPageCallback = BtrPage::InitBtrPage;
            break;
        case SegmentType::UNDO_SEGMENT_TYPE:
            ErrLog(DSTORE_PANIC, MODULE_SEGMENT, ErrMsg("UNDO_SEGMENT_TYPE is not a DataSegment."));
            break;
        default:
            ErrLog(DSTORE_PANIC, MODULE_SEGMENT, ErrMsg("Unrecognized SegmentType: %hhu",
                static_cast<uint8>(type)));
            break;
    }

    /* Use current memory context for internal allocation if given ctx is null */
    if (m_ctx == nullptr) {
        m_ctx = g_dstoreCurrentMemoryContext;
    }
}

DataSegment::~DataSegment()
{
    m_maxAddNewPageCount = 0;
    m_initDataPageCallback = nullptr;
    m_ctx = nullptr;
}

PageId DataSegment::GetFirstDataPage()
{
    BufferDesc *segMetaBufferDesc = m_bufMgr->Read(m_pdbId, Segment::GetSegmentMetaPageId(), LW_SHARED);
    if (unlikely(segMetaBufferDesc == nullptr)) {
        return INVALID_PAGE_ID;
    }
    DataSegmentMetaPage *segmentMetaPage = static_cast<DataSegmentMetaPage *>(segMetaBufferDesc->GetPage());
    PageId firstDataPageId = segmentMetaPage->GetFirstPageId();
    m_bufMgr->UnlockAndRelease(segMetaBufferDesc);
    return firstDataPageId;
}

uint64 DataSegment::GetDataBlockCount()
{
    BufferDesc *segMetaBufferDesc = m_bufMgr->Read(m_pdbId, Segment::GetSegmentMetaPageId(), LW_SHARED);
    StorageReleasePanic(segMetaBufferDesc == nullptr, MODULE_SEGMENT,
        ErrMsg("GetDataBlockCount read buffer(%hu %u) failed.",
            Segment::GetSegmentMetaPageId().m_fileId, Segment::GetSegmentMetaPageId().m_blockId));
    DataSegmentMetaPage* segmentMetaPage = static_cast<DataSegmentMetaPage *>(segMetaBufferDesc->GetPage());
    uint64 dataBlockCount = segmentMetaPage->GetDataBlockCount();
    m_bufMgr->UnlockAndRelease(segMetaBufferDesc);
    return dataBlockCount;
}

RetStatus DataSegment::GetNewExtent(PageId *newExtMetaPageId, BufferDesc *segMetaPageBuf)
{
    DataSegmentMetaPage *metaPage = static_cast<DataSegmentMetaPage *>(segMetaPageBuf->GetPage());
    uint64 currentExtentCount = metaPage->GetExtentCount();
    PageId lastDataExtMetaPageId = metaPage->GetLastExtent();

    /* Calculate extent size base on extent number */
    ExtentSize targetExtSize = EXT_SIZE_8192;
    if (!IsTempSegment()) {
        for (int i = 0; i < EXTENT_SIZE_COUNT - 1; i++) {
            if (currentExtentCount < EXT_NUM_LINE[i + 1]) {
                targetExtSize = EXT_SIZE_LIST[i];
                break;
            }
        }
    } else {
        targetExtSize = TEMP_TABLE_EXT_SIZE;
    }
    if (STORAGE_FUNC_FAIL(DoExtend(lastDataExtMetaPageId, targetExtSize, newExtMetaPageId, segMetaPageBuf))) {
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus DataSegment::DoExtend(const PageId &lastDataExtMetaPageId, ExtentSize targetExtSize,
                                PageId *extMetaPageId, BufferDesc *segMetaPageBuf)
{
    PageId newExtMetaPageId = INVALID_PAGE_ID;
    bool isSecondExtent = (lastDataExtMetaPageId == GetSegmentMetaPageId());
    /* Step 1: Alloc extent from tablespace */
    bool isReuseFlag = false;
    if (STORAGE_FUNC_FAIL(
        AllocExtent(this->GetPdbId(), m_tablespaceId, targetExtSize, &newExtMetaPageId, &isReuseFlag))) {
        return DSTORE_FAIL;
    }
    AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
    if (!IsTempSegment()) {
        walWriterContext->BeginAtomicWal(thrd->GetCurrentXid());
    }
    /* Step 2: Init new extent meta page */
    if (STORAGE_FUNC_FAIL(InitExtMetaPage(newExtMetaPageId, targetExtSize))) {
        ErrLog(DSTORE_PANIC, MODULE_SEGMENT, ErrMsg("Failed in atomic wal progress"));
        return DSTORE_FAIL;
    }

    /* Step 3: Modify next pointer of last extent meta page if new extent is not second extent */
    if (likely(!isSecondExtent)) {
        if (STORAGE_FUNC_FAIL(LinkNextExtInPrevExt(lastDataExtMetaPageId, newExtMetaPageId))) {
            ErrLog(DSTORE_PANIC, MODULE_SEGMENT, ErrMsg("Failed in atomic wal progress"));
            return DSTORE_FAIL;
        }
    }
    /* Step 4: Modify Data Segment Meta Page */
    if (STORAGE_FUNC_FAIL(DataSegMetaLinkDataExtent(newExtMetaPageId, targetExtSize, isSecondExtent,
        segMetaPageBuf, isReuseFlag))) {
        ErrLog(DSTORE_PANIC, MODULE_SEGMENT, ErrMsg("Failed in atomic wal progress"));
        return DSTORE_FAIL;
    }
    if (!IsTempSegment()) {
        (void)walWriterContext->EndAtomicWal();
    }
    *extMetaPageId = newExtMetaPageId;
    return DSTORE_SUCC;
}

RetStatus DataSegment::PrepareFreeDataPages(uint16 *freeDataPageCount, BufferDesc *segMetaPageBuf)
{
    DataSegmentMetaPage *metaPage = static_cast<DataSegmentMetaPage *>(segMetaPageBuf->GetPage());
    uint16 curFreeDataPageCount = metaPage->GetUnassignedPageCount();
    uint64 currentExtentCount = metaPage->GetExtentCount();
    PageId lastDataExtMetaPageId = metaPage->GetLastExtent();
    if (likely(curFreeDataPageCount > 0)) {
        *freeDataPageCount = curFreeDataPageCount;
        return DSTORE_SUCC;
    }
    /* Current free data page count is zero, need to alloc new extent from tablespace */
    if (unlikely(currentExtentCount == MAX_EXTENT_COUNT)) {
        storage_set_error(TBS_ERROR_SEGMENT_EXTENT_COUNT_REACH_MAX);
        return DSTORE_FAIL;
    }
    /* Calculate extent size base on extent number */
    ExtentSize targetExtSize = EXT_SIZE_8192;
    if (!IsTempSegment()) {
        for (int i = 0; i < EXTENT_SIZE_COUNT - 1; i++) {
            if (currentExtentCount < EXT_NUM_LINE[i + 1]) {
                targetExtSize = EXT_SIZE_LIST[i];
                break;
            }
        }
    } else {
        targetExtSize = TEMP_TABLE_EXT_SIZE;
    }

    PageId newExtMetaPageId;
    if (STORAGE_FUNC_FAIL(DoExtend(lastDataExtMetaPageId, targetExtSize, &newExtMetaPageId, segMetaPageBuf))) {
        return DSTORE_FAIL;
    }
#ifdef DSTORE_USE_ASSERT_CHECKING
    ErrLog(DSTORE_DEBUG1, MODULE_SEGMENT,
        ErrMsg("During the table extension, newly created extent has pageId: (fileId = %hu, blockId = %u)",
            newExtMetaPageId.m_fileId, newExtMetaPageId.m_blockId));
#endif
    metaPage = static_cast<DataSegmentMetaPage *>(segMetaPageBuf->GetPage());
    curFreeDataPageCount = metaPage->GetUnassignedPageCount();
    *freeDataPageCount = curFreeDataPageCount;
    return DSTORE_SUCC;
}

RetStatus DataSegment::GetFreeDataPageIds(BufferDesc *metaBuf, uint16 pageCount, PageId *pageIdList)
{
    if (unlikely(pageCount > PAGES_ADD_TO_FSM_PER_TIME)) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("Get free page id with invalid page count %hu.", pageCount));
        return DSTORE_FAIL;
    }
    DataSegmentMetaPage *metaPage = static_cast<DataSegmentMetaPage *>(metaBuf->GetPage());
    uint16 unassignedPageCount = metaPage->GetUnassignedPageCount();
    if (unlikely(pageCount > unassignedPageCount)) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("Request page count %hu is more than unassignedPageCount %hu.",
            pageCount, unassignedPageCount));
        return DSTORE_FAIL;
    }
    PageId firstPageId = {metaPage->addedPageId.m_fileId, metaPage->addedPageId.m_blockId + 1};
    for (uint16 i = 0; i < pageCount; ++i) {
        pageIdList[i] = {firstPageId.m_fileId, firstPageId.m_blockId + i};
    }

    BufferTag bufTag = metaBuf->GetBufferTag();
    ErrLog(DSTORE_LOG, MODULE_SEGMENT, ErrMsg("Get free data page, metaPage bufTag:(%hhu, %hu, %u), segmentType=%d, "
        "totalBlockCount %lu, extents{%lu, (%hu, %u), (%hu, %u)}, plsn %lu, glsn %lu, dataBlockCount %lu, "
        "dataFirst(%hu, %u), dataLast(%hu, %u), addedPageId(%hu, %u), extendedPageId(%hu, %u), lastExtentIsReused %d, "
        "pageIdList first(%hu, %u), end(%hu, %u), pageCount=%hu.", bufTag.pdbId, bufTag.pageId.m_fileId,
        bufTag.pageId.m_blockId, static_cast<int>(metaPage->segmentHeader.segmentType),
        metaPage->segmentHeader.totalBlockCount, metaPage->segmentHeader.extents.count,
        metaPage->segmentHeader.extents.first.m_fileId, metaPage->segmentHeader.extents.first.m_blockId,
        metaPage->segmentHeader.extents.last.m_fileId, metaPage->segmentHeader.extents.last.m_blockId,
        metaPage->segmentHeader.plsn, metaPage->segmentHeader.glsn, metaPage->dataBlockCount,
        metaPage->dataFirst.m_fileId, metaPage->dataFirst.m_blockId, metaPage->dataLast.m_fileId,
        metaPage->dataLast.m_blockId, metaPage->addedPageId.m_fileId, metaPage->addedPageId.m_blockId,
        metaPage->extendedPageId.m_fileId, metaPage->extendedPageId.m_blockId,
        static_cast<int>(metaPage->lastExtentIsReused), pageIdList[0].m_fileId, pageIdList[0].m_blockId,
        pageIdList[pageCount - 1].m_fileId, pageIdList[pageCount - 1].m_blockId, pageCount));

    return DSTORE_SUCC;
}

RetStatus DataSegment::DataSegMetaLinkDataExtent(const PageId &newExtMetaPageId, ExtentSize extSize,
                                                 bool isSecondExtent, BufferDesc *segMetaPageBuf, bool isReUsedFlag)
{
    DataSegmentMetaPage *metaPage = static_cast<DataSegmentMetaPage *>(segMetaPageBuf->GetPage());
    metaPage->LinkExtent(newExtMetaPageId, extSize);
    if (isSecondExtent) {
        metaPage->extentMeta.nextExtMetaPageId = newExtMetaPageId;
    }
    metaPage->AddAssignedPage(newExtMetaPageId, extSize, isReUsedFlag);
    (void)m_bufMgr->MarkDirty(segMetaPageBuf);
    if (IsTempSegment()) {
        return DSTORE_SUCC;
    }
    /* Write Wal of DataSegMetaPage */
    AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
    bool glsnChangedFlag = (metaPage->GetWalId() != walWriterContext->GetWalId());
    WalRecordTbsDataSegmentAddExtent walData;
    walData.SetHeader({WAL_TBS_DATA_SEG_ADD_EXT, sizeof(walData), GetSegmentMetaPageId(), metaPage->GetWalId(),
                       metaPage->GetPlsn(), metaPage->GetGlsn(), glsnChangedFlag, segMetaPageBuf->GetFileVersion()});
    walData.SetData(extSize, newExtMetaPageId, isReUsedFlag);
    walWriterContext->RememberPageNeedWal(segMetaPageBuf);
    walWriterContext->PutNewWalRecord(&walData);

    ErrLog(DSTORE_LOG, MODULE_SEGMENT,
        ErrMsg("Add extent (%hu, %u) for segment (%hu, %u) success, plsn is %lu, glsn is %lu.",
        newExtMetaPageId.m_fileId, newExtMetaPageId.m_blockId,
        GetSegmentMetaPageId().m_fileId, GetSegmentMetaPageId().m_blockId,
        metaPage->GetPlsn(), metaPage->GetGlsn()));

    return DSTORE_SUCC;
}

