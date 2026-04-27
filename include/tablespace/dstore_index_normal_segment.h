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
 * dstore_index_normal_segment.h
 *
 * IDENTIFICATION
 *        include/tablespace/dstore_index_normal_segment.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_INDEX_NORMAL_SEGMENT_H
#define DSTORE_INDEX_NORMAL_SEGMENT_H

#include "page/dstore_btr_queue_page.h"
#include "tablespace/dstore_index_segment.h"

namespace DSTORE {

class IndexNormalSegment final : public IndexSegment {
public:
    /**
     * Construct a instance to read a existing index segment, must call Init() to use Segment functions.
     * @param segmentId First PageId of Segment
     * @param tablespace TableSpace instance of the Segment
     * @param bufMgr Buffer manager instance
     * @param ctx DstoreMemoryContext for internal allocation
     */
    IndexNormalSegment(PdbId pdbId, const PageId &segmentId, TablespaceId tablespaceId, BufMgrInterface *bufMgr,
                       DstoreMemoryContext ctx = nullptr);
    ~IndexNormalSegment() final;
    /**
     * Delete all data of this heap segment in storage
     * @return 0 means success, other means failure
     */
    RetStatus DropSegment() final;

    static PageId AllocIndexNormalSegment(PdbId pdbId, TablespaceId tablespaceId, BufMgrInterface *bufMgr,
                                                       Oid tableOid = DSTORE_INVALID_OID);
private:
    /**
     * Alloc new index segment from tablespace
     * @param tablespace TableSpace instance of the Segment
     * @param bufMgr Buffer manager instance
     * @return IndexNormalSegment instance
     */
    static RetStatus InitIndexSegMetaInfo(
        PdbId pdbId, BufMgrInterface *bufMgr, PageId segMetaPageId, bool isReUsedFlag);

    /**
     * Alloacate a recycle root and init recycle root meta
     * @return DSTORE_SUCC if Segment is ready, or DSTORE_FAIL if something wrong
     */
    static void AllocRecycleRoot(PdbId pdbId, PageId segMetaPageId, BufMgrInterface *bufMgr, Xid currXid);
};

} /* namespace DSTORE */
#endif  /* DSTORE_INDEX_NORMAL_SEGMENT_H */
