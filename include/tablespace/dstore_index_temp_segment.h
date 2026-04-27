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
 * dstore_index_temp_segment.h
 *     This file defines the building blocks that bg page writer needs.
 *
 * IDENTIFICATION
 *        include/tablespace/dstore_index_temp_segment.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_INDEX_TEMP_SEGMENT_H
#define DSTORE_INDEX_TEMP_SEGMENT_H

#include "tablespace/dstore_index_segment.h"

namespace DSTORE {

class IndexTempSegment final : public IndexSegment {
public:
    /**
     * Construct a instance to read a existing index temp segment, must call Init() to use Segment functions.
     * @param segmentId First PageId of Segment
     * @param tablespace TableSpace instance of the Segment
     * @param bufMgr Buffer manager instance
     * @param ctx DstoreMemoryContext for internal allocation
     */
    IndexTempSegment(PdbId pdbId, const PageId &segmentId, TablespaceId tablespaceId, BufMgrInterface *bufMgr,
                     DstoreMemoryContext ctx = nullptr);
    ~IndexTempSegment() final;

    RetStatus DropSegment() final;

    static PageId AllocIndexTempSegment(PdbId pdbId, TablespaceId tablespaceId, BufMgrInterface *bufMgr,
                                                   Oid tableOid = DSTORE_INVALID_OID);
    /**
     * Get a new page from data segment
     * @return one PageId of new pages, INVALID_PAGE_ID if DataSegment cannot get one new page
     */
    PageId GetNewPage(bool isExtendBg = false) override;
    PageId GetNewPageFromUnassignedPages() override;

private:
    PageId m_nextPageId;
    RetStatus GetNewPageInternal(PageId *newPageId);
    static RetStatus InitIndexTempSegMetaInfo(PdbId pdbId, BufMgrInterface *bufMgr,
        PageId segMetaPageId, bool isReuseFlag);
    static void AllocTempRecycleRoot(PdbId pdbId, PageId segMetaPageId, BufMgrInterface *bufMgr);
};

} /* namespace DSTORE */
#endif  /* DSTORE_INDEX_TEMP_SEGMENT_H */
