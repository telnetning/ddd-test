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
 * dstore_segment_meta_page.h
 *     This file defines the building blocks that bg page writer needs.
 *
 * IDENTIFICATION
 *        include/page/dstore_segment_meta_page.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_DSTORE_SEGMENT_META_PAGE_H
#define DSTORE_DSTORE_SEGMENT_META_PAGE_H

#include "page/dstore_page.h"
#include "page/dstore_extent_meta_page.h"
#include "tablespace/dstore_tablespace_struct.h"
#include "tablespace/dstore_tablespace_utils_internal.h"

namespace DSTORE {

constexpr uint64 SEGMENT_META_MAGIC = 0xA1A2A3A4A5A6A7A8;

struct ExtentRange {
    uint64 count;  // extent number allocated to this segment
    PageId first;
    PageId last;
};

// SegmentMetaPage inherit Page, include segmentHeader info
struct SegmentMetaPage : public SegExtentMetaPage {
public:

    struct SegmentMetaPageHeader {
        /* Basic segment head info */
        SegmentType segmentType; /* heap segment or undo segment */
        uint64 totalBlockCount; /* total blocks in segment */
        ExtentRange extents; /* Only use to store data page */
        uint64 plsn;
        uint64 glsn;
        uint8 reserved[32];
    } PACKED;
    SegmentMetaPageHeader segmentHeader;

    void InitSegmentMetaPage(SegmentType segmentType, PageType pageType, const PageId& extMetaPageId,
        const ExtentSize extSize, uint64 plsn, uint64 glsn)
    {
        /* Step 1: Call Extent Meta Page Init() */
        SegExtentMetaPage::InitSegExtentMetaPage(extMetaPageId, extSize, pageType);
        extentMeta.magic = SEGMENT_META_MAGIC;

        /* Step 2: Init basic segment head info */
        segmentHeader.segmentType = segmentType;
        segmentHeader.totalBlockCount = static_cast<uint16>(extSize);
        segmentHeader.extents.count = 1;
        segmentHeader.extents.first = segmentHeader.extents.last = extMetaPageId;
        segmentHeader.plsn = plsn;
        segmentHeader.glsn = glsn;
    }
    
    void DumpSegmentMetaPageInfo(StringInfoData &str)
    {
        /* Extent meta info */
        DumpExtentMetaInfo(str);
        /* Basic segment meta info */
        str.append("Basic segment meta info\n");
        str.append("  Segment type = %d\n", static_cast<uint8>(segmentHeader.segmentType));
        str.append("  Segment total block count = %lu\n", segmentHeader.totalBlockCount);
        str.append("  Segment total extent count = %lu\n", segmentHeader.extents.count);
        str.append("  Segment first extent meta page id = (%d, %u)\n",
                   segmentHeader.extents.first.m_fileId, segmentHeader.extents.first.m_blockId);
        str.append("  Segment last extent meta page id = (%d, %u)\n",
                   segmentHeader.extents.last.m_fileId, segmentHeader.extents.last.m_blockId);
    }

    uint64 GetTotalBlockCount() const
    {
        return segmentHeader.totalBlockCount;
    }

    inline PageId GetLastExtent() const
    {
        return segmentHeader.extents.last;
    }

    inline uint64 GetExtentCount() const
    {
        return segmentHeader.extents.count;
    }

    inline void LinkExtent(const PageId& extMetaPageId, ExtentSize extSize)
    {
        segmentHeader.extents.last = extMetaPageId;
        if (segmentHeader.extents.count == 1) {
            extentMeta.nextExtMetaPageId = extMetaPageId;
        }
        segmentHeader.extents.count += 1;
        segmentHeader.totalBlockCount += static_cast<uint16>(extSize);
    }

    inline void UnlinkExtent(const PageId &nextExtMetaPageId, const PageId &unlinkExtMetaPageId, uint16 unlinkExtSize)
    {
        extentMeta.nextExtMetaPageId = nextExtMetaPageId;
        segmentHeader.totalBlockCount -= unlinkExtSize;
        segmentHeader.extents.count -= 1;
        if (GetLastExtent() == unlinkExtMetaPageId) {
            segmentHeader.extents.last = GetSelfPageId();
        }
    }
};
STATIC_ASSERT_TRIVIAL(SegmentMetaPage);

static_assert(sizeof(SegmentMetaPage) <= BLCKSZ, "SegmentMetaPage cannot exceed BLCKSZ");

} // namespace DSTORE

#endif /* DSTORE_STORAGE_SEGMENT_META_PAGE_H */
