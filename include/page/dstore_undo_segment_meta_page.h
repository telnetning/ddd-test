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
 * dstore_page.cpp
 *
 *
 * IDENTIFICATION
 *        include/page/dstore_undo_segment_meta_page.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_DSTORE_UNDO_SEGMENT_META_PAGE_H
#define DSTORE_DSTORE_UNDO_SEGMENT_META_PAGE_H
#include "page/dstore_segment_meta_page.h"
#include "tablespace/dstore_tablespace_utils_internal.h"

namespace DSTORE {

constexpr ExtentSize UNDO_SEG_FIRST_EXT_SIZE = EXT_SIZE_128;

struct UndoSegmentMetaPage : public SegmentMetaPage {
    PageId firstUndoPageId;
    bool   alreadyInitTxnSlotPages;

    inline void InitUndoSegmentMetaPage(const PageId selfPageId, uint64 plsn, uint64 glsn)
    {
        SegmentMetaPage::InitSegmentMetaPage(SegmentType::UNDO_SEGMENT_TYPE, PageType::UNDO_SEGMENT_META_PAGE_TYPE,
            selfPageId, UNDO_SEG_FIRST_EXT_SIZE, plsn, glsn);
        firstUndoPageId = INVALID_PAGE_ID;
        alreadyInitTxnSlotPages = false;
    }

    char* DumpUndoSegmentMetaPage()
    {
        StringInfoData str;
        str.init();
        /* Page header info */
        str.append("Page Header\n");
        Page::DumpHeader(&str);

        DumpSegmentMetaPageInfo(str);

        /* Undo segment meta info */
        str.append("Undo segment header\n");
        str.append("  Undo segment firstUndoPageId = (%hu, %u)\n", firstUndoPageId.m_fileId,
            firstUndoPageId.m_blockId);
        str.append("  Undo segment m_alreadyInitTxnSlotPages = %s", (alreadyInitTxnSlotPages ? "true" : "false"));

        return str.data;
    }
};

STATIC_ASSERT_TRIVIAL(UndoSegmentMetaPage);

static_assert(sizeof(UndoSegmentMetaPage) <= BLCKSZ, "UndoSegmentMetaPage cannot exceed BLCKSZ");

} /* The end of namespace DSTORE */
#endif
