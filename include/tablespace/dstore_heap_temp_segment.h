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
 * dstore_heap_segment.h
 *
 * IDENTIFICATION
 *        include/tablespace/dstore_heap_temp_segment.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_HEAP_TEMP_SEGMENT_H
#define DSTORE_HEAP_TEMP_SEGMENT_H

#include "tablespace/dstore_heap_segment.h"

namespace DSTORE {

class HeapTempSegment final : public HeapSegment {
public:
    /**
     * Construct a instance to read a existing temp heap segment, must call Init() to use Segment functions.
     * @param segmentId First PageId of Segment
     * @param tablespace TableSpace instance of the Segment
     * @param bufMgr Buffer manager instance
     * @param ctx DstoreMemoryContext used for internal allocation
     */
    HeapTempSegment(PdbId pdbId, const PageId &segmentId, TablespaceId tablespaceId, BufMgrInterface *bufMgr,
                    DstoreMemoryContext ctx = nullptr);

    ~HeapTempSegment() override;

    /**
     * Alloc new heap temp segment from tablespace
     * @param tablespace TableSpace instance of the Segment
     * @param bufMgr Buffer manager instance
     * @return HeapTempSegment instance
     */
    static PageId AllocHeapTempSegment(PdbId pdbId, TablespaceId tablespaceId, BufMgrInterface *bufMgr,
        Oid tableOid = DSTORE_INVALID_OID);
    /**
     * Delete all data of this HeapTempSegment in storage
     * @return 0 means success, other means failure
     */
    RetStatus DropSegment() override;
    RetStatus InitNewDataPageWithFsmIndex(uint16 pageCount, PageId *pageIdList,
        BufferDesc **pageBufList, FsmIndex *fsmIndexList, bool pagesIsReused) override;
};

} /* namespace DSTORE */
#endif  /* DSTORE_HEAP_TEMP_SEGMENT_H */
