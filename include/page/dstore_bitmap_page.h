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
 * -------------------------------------------------------------------------
 *
 * dstore_wal_read_buffer.cpp
 *
 * IDENTIFICATION
 *      include/page/dstore_bitmap_page.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef DSTORE_DSTORE_BITMAP_PAGE_H
#define DSTORE_DSTORE_BITMAP_PAGE_H

#include "page/dstore_page.h"

namespace DSTORE {
constexpr uint16 ALIGN_BYTE_EXTENT_128 = 2;
constexpr uint16 ALIGN_BYTE_EXTENT_1024 = 16;
constexpr uint16 ALIGN_BYTE_EXTENT_8192 = 128;

#define DF_BITMAP_BYTE_CNT \
    ((static_cast<uint16>((BLCKSZ - OFFSETOF(TbsBitmapPage, bitmap))) / ALIGN_BYTE_EXTENT_8192) \
    * ALIGN_BYTE_EXTENT_8192)
#define DF_BITMAP_BIT_CNT (DF_BITMAP_BYTE_CNT * BYTE_TO_BIT_MULTIPLIER)

struct TbsBitmapPage : public Page {
public:

    PageId firstDataPageId; /* first data page managed by this bitmap */
    uint16 allocatedExtentCount;  /* max value is static_cast<uint16>(DF_BITMAP_BIT_CNT) */
    uint8 reserved[64]; /* bitmap must be the last member of struct */
    uint8 bitmap[];       /* bitmap: Used to allocate reclaimed space */

    void InitBitmapPage(const PageId& myselfPageId, const PageId& firstDataPageIdInput)
    {
        Page::Init(0, PageType::TBS_BITMAP_PAGE_TYPE, myselfPageId);
        firstDataPageId = firstDataPageIdInput;
        allocatedExtentCount = 0;
    }

    inline bool TestBitZero(uint32 pos) const
    {
        /* Right shift 'pos' by 3, to obtain the byte of the bit in the bitmap. */
        /* (pos) & 0x07 get the bit in the byte obtained */
        /* the operation '&' is to test if the bit is 1 */
        return !((bitmap)[(pos) >> BYTE_TO_BIT_SHIFT] & (1 << ((pos) & 0x07)));
    }

    inline void SetByBit(uint32 pos)
    {
        /* Right shift 'pos' by 3, to obtain the byte of the bit in the bitmap. */
        /* (pos) & 0x07 get the bit in the byte obtained */
        /* the operation '|=' is to set 1 */
        (bitmap)[(pos) >> BYTE_TO_BIT_SHIFT] |= (static_cast<uint8>((1U) << ((pos) & 0x07)));
        allocatedExtentCount++;
    }

    inline void UnsetByBit(uint32 pos)
    {
        /* unset bitmap */
        /* Right shift 'pos' by 3, to obtain the byte of the bit in the bitmap. */
        /* (pos) & 0x07 get the bit in the byte obtained */
        /* the operation '&= ~' is to set 0 */
        (bitmap)[(pos) >> BYTE_TO_BIT_SHIFT] &= static_cast<uint8>(~(1U << ((pos) & 0x07)));
        allocatedExtentCount--;
    }

    inline uint32 FindFirstFreeBitByBit(uint32 pos) const
    {
        while (pos < DF_BITMAP_BIT_CNT) {
            if (TestBitZero(static_cast<uint32>(pos))) {
                return pos;
            }
            pos++;
        }
        return pos;
    }

    char* Dump()
    {
        StringInfoData str;
        bool ret = str.init();
        if (unlikely(ret == false)) {
            ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("Failed to initialize message string infomation data."));
            return nullptr;
        }
        Page::DumpHeader(&str);
        str.append("TbsBitmapPage\n");
        str.append("  firstDataPageId = (%hu,%u) \n", firstDataPageId.m_fileId, firstDataPageId.m_blockId);
        str.append("  allocatedExtentCount = %hu \n", allocatedExtentCount);

        uint32 i = 0;
        while (i < DF_BITMAP_BYTE_CNT) {
            str.append("line %3u: (%5u - %-5u) ", i / ALIGN_BYTE_EXTENT_1024, i * BITS_PER_BYTE,
                (i + ALIGN_BYTE_EXTENT_1024) * BITS_PER_BYTE - 1);
            for (uint32 j = 0; j < ALIGN_BYTE_EXTENT_1024; j++) {
                str.append("0x%02hhX ", bitmap[i + j]);
            }
            str.append("\n");
            i += ALIGN_BYTE_EXTENT_1024;
        }
        return str.data;
    }
} PACKED;

STATIC_ASSERT_TRIVIAL(TbsBitmapPage);

static_assert(sizeof(TbsBitmapPage) <= BLCKSZ);

}  // namespace DSTORE
#endif  // DSTORE_STORAGE_BITMAP_PAGE_H
