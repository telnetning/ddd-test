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
 * dstore_heap_temp_segment.cpp
 *
 *
 * IDENTIFICATION
 *        include/page/dstore_extent_meta_page.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_DSTORE_EXTENT_META_PAGE_H
#define DSTORE_DSTORE_EXTENT_META_PAGE_H

#include "page/dstore_page.h"
#include "tablespace/dstore_tablespace_utils_internal.h"

namespace DSTORE {

constexpr uint64 EXTENT_META_MAGIC = 0xB1B2B3B4B5B6B7B8;

struct SegExtentMetaPage : public Page {
public:
    struct ExtentMeta {
        uint64 magic;
        ExtentSize extSize;
        PageId nextExtMetaPageId;
        uint8 reserved[32];
    } PACKED;

    ExtentMeta extentMeta;

    void InitSegExtentMetaPage(const PageId &extMetaPageId, ExtentSize extSize, PageType type)
    {
        Page::Init(0, type, extMetaPageId);
        extentMeta.magic = EXTENT_META_MAGIC;
        extentMeta.extSize = extSize;
        extentMeta.nextExtMetaPageId = INVALID_PAGE_ID;
    }

    void LinkNextExtent(const PageId &nextExt)
    {
        extentMeta.nextExtMetaPageId = nextExt;
    }
    inline uint16 GetSelfExtentSize() const
    {
        return static_cast<uint16>(extentMeta.extSize);
    }
    inline PageId GetNextExtentMetaPageId() const
    {
        return extentMeta.nextExtMetaPageId;
    }

    void DumpExtentMetaInfo(StringInfoData &str)
    {
        /* Extent meta info */
        str.append("Extent meta info\n");
        str.append("  Magic = 0x%lx\n", extentMeta.magic);
        str.append("  Current extent size = %d\n", static_cast<int>(extentMeta.extSize));
        str.append("  Next extent meta page id = (%d, %u)\n", extentMeta.nextExtMetaPageId.m_fileId,
                   extentMeta.nextExtMetaPageId.m_blockId);
    }

    char *Dump()
    {
        StringInfoData str;
        str.init();
        Page::DumpHeader(&str);
        /* Extent meta page info */
        DumpExtentMetaInfo(str);
        return str.data;
    }
} PACKED;

STATIC_ASSERT_TRIVIAL(SegExtentMetaPage);

static_assert(sizeof(SegExtentMetaPage) <= BLCKSZ, "Extent Meta Page cannot exceed BLCKSZ");

} /* namespace DSTORE */

#endif  // DSTORE_STORAGE_EXTENT_META_PAGE_H
