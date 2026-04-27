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
 * dstore_data_segment_meta_page.h
 *
 * IDENTIFICATION
 *        include/page/dstore_data_segment_meta_page.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_DSTORE_DATA_SEGMENT_META_PAGE_H
#define DSTORE_DSTORE_DATA_SEGMENT_META_PAGE_H
#include "page/dstore_page.h"
#include "page/dstore_extent_meta_page.h"
#include "page/dstore_segment_meta_page.h"
#include "tablespace/dstore_tablespace_interface.h"
namespace DSTORE {

struct DataSegmentMetaPage : public SegmentMetaPage {
public:
    uint64 dataBlockCount; /* data blocks (exclude segment head and fsm page) in segment */
    PageId dataFirst;
    PageId dataLast;
    PageId addedPageId; /* has added to fsm */
    PageId extendedPageId; /* Pages in (addedPageId, extendedPageId] are not in fsm */
    bool lastExtentIsReused;
    uint8 reserved[15];

    RetStatus InitDataSegmentMetaPage(SegmentType type, const PageId& extMetaPageId, const ExtentSize extSize,
        uint64 plsn, uint64 glsn)
    {
        if (type != SegmentType::HEAP_SEGMENT_TYPE && type != SegmentType::INDEX_SEGMENT_TYPE &&
            type != SegmentType::HEAP_TEMP_SEGMENT_TYPE && type != SegmentType::INDEX_TEMP_SEGMENT_TYPE) {
            ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("InitDataSegmentMetaPage with invalid type %hhu!",
                static_cast<uint8_t>(type)));
            return DSTORE_FAIL;
        }
        SegmentMetaPage::InitSegmentMetaPage(type, PageType::DATA_SEGMENT_META_PAGE_TYPE,
            extMetaPageId, extSize, plsn, glsn);
        return DSTORE_SUCC;
    }

    void InitSegmentInfo(const PageId& addedPageIdInput, bool isReUsedFlag)
    {
        dataBlockCount = 0;
        dataFirst = dataLast = INVALID_PAGE_ID;
        addedPageId = addedPageIdInput;
        PageId curExtMeta = m_header.m_myself;
        PageId curExtEnd = {curExtMeta.m_fileId,
                            curExtMeta.m_blockId + static_cast<uint32>(extentMeta.extSize) - 1};
        extendedPageId = curExtEnd;
        lastExtentIsReused = isReUsedFlag;

        /* Make sure we do not exceed the range of current extent */
        StorageReleasePanic(addedPageId.m_blockId > extendedPageId.m_blockId, MODULE_SEGMENT,
            ErrMsg("InitSegmentInfo with invalid unassigned page count! added(%hu %u) vs. extended(%hu, %u).",
                addedPageId.m_fileId, addedPageId.m_blockId, extendedPageId.m_fileId, extendedPageId.m_blockId));
    }
    uint64 GetDataBlockCount() const
    {
        return dataBlockCount;
    }
    PageId GetFirstPageId() const
    {
        return dataFirst;
    }
    PageId GetLastPageId() const
    {
        return dataLast;
    }
    bool HasUnassignedPage() const
    {
        return addedPageId != extendedPageId;
    }
    uint16 GetUnassignedPageCount() const
    {
        StorageReleasePanic(extendedPageId.m_blockId < addedPageId.m_blockId, MODULE_SEGMENT,
            ErrMsg("GetUnassignedPageCount with invalid unassigned page count! added(%hu %u) vs. extended(%hu, %u).",
                addedPageId.m_fileId, addedPageId.m_blockId, extendedPageId.m_fileId, extendedPageId.m_blockId));
        return static_cast<uint16>(extendedPageId.m_blockId - addedPageId.m_blockId);
    }
    void AddAssignedPage(const PageId& extHead, ExtentSize extSize, bool isReUsedFlag)
    {
        PageId extEnd = {extHead.m_fileId, extHead.m_blockId + static_cast<uint16>(extSize) - 1};
        addedPageId = extHead;
        extendedPageId = extEnd;
        lastExtentIsReused = isReUsedFlag;
    }

    void AddDataPages(const PageId& firstDataPageId, const PageId& lastDataPageId, uint16 addPageCount)
    {
        if (dataBlockCount == 0) {
            dataFirst = firstDataPageId;
        }
        dataLast = lastDataPageId;
        dataBlockCount += addPageCount;
        addedPageId.m_blockId += addPageCount;
    }

    char *DumpDataSegmentMetaPage()
    {
        StringInfoData str;
        str.init();
        Page::DumpHeader(&str);
        DumpSegmentMetaPageInfo(str);

        /* Data segment meta info */
        str.append("Data segment header\n");
        str.append("  Data segment total data page count = %lu\n", dataBlockCount);
        str.append("  Data segment first data page id = (%d, %u)\n",
                   dataFirst.m_fileId, dataFirst.m_blockId);
        str.append("  Data segment last data page id = (%d, %u)\n",
                   dataLast.m_fileId, dataLast.m_blockId);
        str.append("  PageId (%d, %u) has added to fsm in last extent\n",
                   addedPageId.m_fileId, addedPageId.m_blockId);
        str.append("  PageId (%d, %u) is last page in last extent\n",
                   extendedPageId.m_fileId, extendedPageId.m_blockId);
        str.append("  Last extent %s reused.\n",
                   lastExtentIsReused ? "is" : "is not");
        return str.data;
    }
} PACKED;
STATIC_ASSERT_TRIVIAL(DataSegmentMetaPage);

static_assert(sizeof(DataSegmentMetaPage) <= BLCKSZ, "DataSegmentMetaPage cannot exceed BLCKSZ");

}  /* The end of namespace */
#endif
