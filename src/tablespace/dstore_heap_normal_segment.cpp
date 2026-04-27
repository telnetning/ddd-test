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
 * dstore_heap_normal_segment.cpp
 *
 * IDENTIFICATION
 *        src/tablespace/dstore_heap_normal_segment.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
 
#include "framework/dstore_instance.h"
#include "lock/dstore_table_lock_mgr.h"
#include "tablespace/dstore_heap_normal_segment.h"

using namespace DSTORE;

HeapNormalSegment::HeapNormalSegment(PdbId pdbId, const PageId &segmentId, TablespaceId tablespaceId,
    BufMgrInterface *bufMgr, DstoreMemoryContext ctx)
    : HeapSegment(pdbId, segmentId, tablespaceId, bufMgr, ctx)
{
}

HeapNormalSegment::~HeapNormalSegment()
{}

PageId HeapNormalSegment::AllocHeapNormalSegment(PdbId pdbId, TablespaceId tablespaceId,
    BufMgrInterface *bufMgr, Oid tableOid)
{
    if (unlikely(tablespaceId == INVALID_TABLESPACE_ID || bufMgr == nullptr)) {
        storage_set_error(TBS_ERROR_SEGMENT_PARAMETER_INVALID);
        return INVALID_PAGE_ID;
    }
    PageId segMetaPageId;

    /* Alloc extent from tablespace */
    bool isReuseFlag = false;
    if (STORAGE_FUNC_FAIL(AllocExtent(pdbId, tablespaceId, FIRST_EXT_SIZE, &segMetaPageId, &isReuseFlag))) {
        if (StorageGetErrorCode() == TBS_ERROR_TABLESPACE_USE_UP) {
            ErrLog(DSTORE_WARNING, MODULE_SEGMENT,
                ErrMsg("Alloc new heap segment for oid %u fail, pdb %u tablespace %hu has no space",
                tableOid, pdbId, tablespaceId));
        }
        return INVALID_PAGE_ID;
    }

    /* Init segment head page */
    if (STORAGE_FUNC_FAIL(HeapSegment::InitHeapSegMetaInfo(pdbId, bufMgr, SegmentType::HEAP_SEGMENT_TYPE,
        segMetaPageId, isReuseFlag))) {
        return INVALID_PAGE_ID;
    }

    /* Construct DataSegment object */
    HeapNormalSegment segment(pdbId, segMetaPageId, tablespaceId, bufMgr, g_dstoreCurrentMemoryContext);
    if (STORAGE_FUNC_FAIL(segment.InitSegment())) {
        /* HeapNormalSegment init failed */
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
            ErrMsg("Heap data segment(%hu, %u) initialization failed, oid %u. pdb %u, tablespace %hu",
                segMetaPageId.m_fileId, segMetaPageId.m_blockId, tableOid, pdbId, tablespaceId));
        return INVALID_PAGE_ID;
    }

    ErrLog(DSTORE_LOG, MODULE_SEGMENT,
           ErrMsg("Alloc new heap segment for oid %u success, SegmentId(%hu, %u) %s reused. pdb %u, tablespace %hu",
               tableOid, segMetaPageId.m_fileId, segMetaPageId.m_blockId,
               (isReuseFlag ? "is" : "isn't"), pdbId, tablespaceId));
    return segment.GetSegmentMetaPageId();
}

