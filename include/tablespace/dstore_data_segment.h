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
 * dstore_data_segment.h
 *
 * IDENTIFICATION
 *        include/tablespace/dstore_data_segment.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_DATA_SEGMENT_H
#define DSTORE_DATA_SEGMENT_H

#include "buffer/dstore_buf_mgr.h"
#include "tablespace/dstore_segment.h"
#include "page/dstore_data_segment_meta_page.h"

namespace DSTORE {

using InitDataPageCallback = void (*)(BufferDesc *bufDesc, const PageId &selfPageId, const FsmIndex &fsmIndex);
class DataSegment : public Segment {
public:
    /**
     * Construct a instance to read a existing data segment, must call Init() to use Segment functions.
     * @param segmentId First PageId of Segment
     * @param type Segment type
     * @param tablespace TableSpace instance of the Segment
     * @param bufMgr Buffer manager instance
     * @param ctx MemoryContext used for internal allocation
     */
    DataSegment(PdbId pdbId, const PageId &segmentId, SegmentType type, TablespaceId tablespaceId,
                BufMgrInterface *bufMgr, DstoreMemoryContext ctx);
    ~DataSegment() override;

    /**
     * Call after DataSegment constructor to get segment head buffer (do not release)
     * @return 0 means success, other means failure
     */
    virtual RetStatus InitSegment() = 0;

    /**
     * Get first data page in this data segment
     * @return first page id, or INVALID_PAGE_ID if no page in this data segment
     */
    PageId GetFirstDataPage();

    /**
     * Get total data page count in this data segment
     * @return data page count, or INVALID_DATA_PAGE_COUNT if running error in this function
     */
    uint64 GetDataBlockCount();

protected:
    /* DataSegment Extension functions */
    RetStatus GetNewExtent(PageId *newExtMetaPageId, BufferDesc *segMetaPageBuf);
    RetStatus DoExtend(const PageId &lastDataExtMetaPageId, ExtentSize targetExtSize, PageId *extMetaPageId,
                       BufferDesc *segMetaPageBuf);
    RetStatus PrepareFreeDataPages(uint16 *freeDataPageCount, BufferDesc *segMetaPageBuf);
    RetStatus GetFreeDataPageIds(BufferDesc *metaBuf, uint16 pageCount, PageId *pageIdList);
    RetStatus DataSegMetaLinkDataExtent(const PageId &newExtMetaPageId, ExtentSize extSize, bool isSecondExtent,
                                        BufferDesc *segMetaPageBuf, bool isReUsedFlag);

    uint16 m_maxAddNewPageCount;
    InitDataPageCallback m_initDataPageCallback;
    DstoreMemoryContext m_ctx;
};

constexpr uint32 NUM_PAGES_FOR_SEGMENT_META = 1;
} /* namespace DSTORE */
#endif  // STORAGE_DATA_SEGMENT_H
