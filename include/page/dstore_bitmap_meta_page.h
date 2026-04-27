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
 * dstore_bitmap_meta_page.h
 *
 * IDENTIFICATION
 *        include/page/dstore_bitmap_meta_page.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_DSTORE_BITMAP_META_PAGE_H
#define DSTORE_DSTORE_BITMAP_META_PAGE_H

#include "page/dstore_page.h"
#include "tablespace/dstore_tablespace_utils_internal.h"

namespace DSTORE {
/* Each bitmapPage has 63 * 128 Byte for bitmap; the remaining one 128 Byte for the other header info */
/**
 * The maximum space of one file is 4 * 1024 * 1024 * 1024 * BLCKSZ.
 * One bitmap page indicates the space of 63 * 128 * 8 * 64K;
 * One file has one bitmapMeta page, 512 BitMapGroups, and each BitMapGroup has 128 bitmap pages.
 * Therefore, a bitmapMeta indicates a space of 512 * 128 * 63 * 1024 * 64K, which is greater than
 * 4 * 1024 * 1024 * 1024 * BLCKSZ.
 * Currently, one bitmap group has 128 pages, which can be adjusted later, as required.
 */
#ifdef UT
constexpr uint16 BITMAP_PAGES_PER_GROUP = 1;
#else
constexpr uint16 BITMAP_PAGES_PER_GROUP = 128;
#endif
const int MAX_BITMAP_GROUP_CNT = 512;
const int BITMAP_GROUP_RESERVED = 7;

struct TbsBitMapGroup {
    PageId firstBitmapPageId; /* start page id of bitmap pages of this group */
    uint8 firstFreePageNo;       /* first free page of this group */
    uint8 reserved[BITMAP_GROUP_RESERVED];
} PACKED;

struct TbsBitmapMetaPage : public Page {
public:
    ExtentSize extentSize;    /* extentSize managed by 1 bit in bitmap  */
    uint16 groupCount; /* count of bitmap group that already exists */
    uint16 idleGroupHints; /* first free group */
    uint16 validOffset;    /* the max valid offset in the page */
    uint64 totalBlockCount;  /* the current block count of the file. */
    uint32 bitmapPagesPerGroup;
    TbsBitMapGroup bitmapGroups[MAX_BITMAP_GROUP_CNT];
    uint8 reserved[64];

    void InitBitmapMetaPage(const PageId &bitmapMetaPageId, uint64 totalBlockCountInput, ExtentSize extentSizeInput)
    {
        Page::Init(0, PageType::TBS_BITMAP_META_PAGE_TYPE, bitmapMetaPageId);
        extentSize = extentSizeInput;
        groupCount = 0;
        idleGroupHints = 0;
        totalBlockCount = totalBlockCountInput;
        bitmapPagesPerGroup = BITMAP_PAGES_PER_GROUP;
        validOffset = static_cast<uint16>(OFFSETOF(TbsBitmapMetaPage, bitmapGroups));
    }

    char *Dump()
    {
        StringInfoData str;
        bool ret = str.init();
        if (unlikely(ret == false)) {
            ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("Failed to initialize message string infomation data."));
            return nullptr;
        }

        Page::DumpHeader(&str);
        str.append("Bitmap Meta Page Info\n");
        str.append("  extentSize = %hu \n", extentSize);
        str.append("  groupCount = %hu \n", groupCount);
        str.append("  idleGroupHints = %hu \n", idleGroupHints);
        str.append("  validOffset = %hu \n", validOffset);
        str.append("  totalBlockCount = %lu \n", totalBlockCount);
        for (uint16 i = 0; i < groupCount; i++) {
            str.append("    bitmapGroup(%hu):  \n", i);
            str.append("    firstBitmapPageId = (%hu, %u) \n", bitmapGroups[i].firstBitmapPageId.m_fileId,
                       bitmapGroups[i].firstBitmapPageId.m_blockId);
        }
        return str.data;
    }
} PACKED;
STATIC_ASSERT_TRIVIAL(TbsBitmapMetaPage);

static_assert(sizeof(TbsBitmapMetaPage) <= BLCKSZ);

}  // namespace DSTORE
#endif  // DSTORE_STORAGE_BITMAP_META_PAGE_H
