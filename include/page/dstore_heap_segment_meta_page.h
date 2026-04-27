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
 * dstore_heap_segment_meta_page.h
 *
 * IDENTIFICATION
 *        include/page/dstore_heap_segment_meta_page.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_DSTORE_HEAP_SEGMENT_META_PAGE_H
#define DSTORE_DSTORE_HEAP_SEGMENT_META_PAGE_H

#include "page/dstore_segment_meta_page.h"
#include "page/dstore_data_segment_meta_page.h"

namespace DSTORE {

struct FsmInfo {
    PageId fsmMetaPageId;
    NodeId assignedNodeId;
};

struct HeapSegmentMetaPage : public DataSegmentMetaPage {
public:
    uint16 numFsms; /* Number of avaliable fsm */
    FsmInfo fsmInfos[MAX_FSM_TREE_PER_RELATION]; /* fsmInfo of PartitionFreeSpaceMap partitions this segment owns */

    void InitHeapSegmentMetaPage(SegmentType type, const PageId& extMetaPageId, const ExtentSize extSize, uint64 plsn,
        uint64 glsn)
    {
        StorageAssert(type == SegmentType::HEAP_SEGMENT_TYPE || type == SegmentType::HEAP_TEMP_SEGMENT_TYPE);
        SegmentMetaPage::InitSegmentMetaPage(type, PageType::HEAP_SEGMENT_META_PAGE_TYPE, extMetaPageId, extSize, plsn,
            glsn);

        numFsms = 0;
        /* Initialize all entries in fsmInfos to be INVALID_PAGE_ID */
        for (int i = 0; i < MAX_FSM_TREE_PER_RELATION; i++) {
            fsmInfos[i] = {INVALID_PAGE_ID, INVALID_NODE_ID};
        }
    }

    char *DumpHeapSegmentMetaPage()
    {
        StringInfoData str;
        str.init();
        DumpSegmentMetaPageInfo(str);

        /* Heap segment meta info */
        str.append("Heap segment header\n");
        str.append("  Heap segment total data page count = %lu\n", dataBlockCount);
        str.append("  Heap segment first data page id = (%d, %u)\n",
                   dataFirst.m_fileId, dataFirst.m_blockId);
        str.append("  Heap segment last data page id = (%d, %u)\n",
                   dataLast.m_fileId, dataLast.m_blockId);
        str.append("  PageId (%d, %u) has added to fsm in last extent\n",
                   addedPageId.m_fileId, addedPageId.m_blockId);
        str.append("  PageId (%d, %u) is last page in last extent\n",
                   extendedPageId.m_fileId, extendedPageId.m_blockId);
        str.append("  Last extent %s reused.\n",
                   lastExtentIsReused ? "is" : "is not");
        str.append("  Heap segment has %hu FreeSpaceMaps", numFsms);
        for (int i = 0; i < numFsms; i++) {
            str.append("    PageId of the FSM meta page %d = (%d, %u), assigned Node %u\n",
                       i, fsmInfos[i].fsmMetaPageId.m_fileId, fsmInfos[i].fsmMetaPageId.m_blockId,
                       fsmInfos[i].assignedNodeId);
        }
        return str.data;
    }
} PACKED;
STATIC_ASSERT_TRIVIAL(HeapSegmentMetaPage);

static_assert(sizeof(HeapSegmentMetaPage) <= BLCKSZ, "HeapSegmentMetaPage cannot exceed BLCKSZ");

}  /* The end of namespace */
#endif
