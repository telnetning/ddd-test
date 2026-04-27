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
 * dstore_tablespace_diagnose.cpp
 *
 *
 * IDENTIFICATION
 *        include/tablespace/dstore_data_segment_context.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_DATA_SEGMENT_CONTEXT_H
#define DSTORE_DATA_SEGMENT_CONTEXT_H

#include "buffer/dstore_buf_mgr.h"
#include "tablespace/dstore_data_segment.h"
#include "framework/dstore_parallel.h"

namespace DSTORE {
enum class SmgrType {
    INVALID_SMGR_TYPE = 0,
    INDEX_SMGR,
    HEAP_SMGR,
    LOB_SMGR
};
class DataSegmentScanContext : public BaseObject {
public:
    DataSegmentScanContext(BufMgrInterface *bufMgr, StorageRelation stoRel, SmgrType smgrType);
    virtual ~DataSegmentScanContext();
    DISALLOW_COPY_AND_MOVE(DataSegmentScanContext);

    PageId GetNextPageId();
    PageId GetFirstPageId();
    PageId GetEndPageId();
    void Reset();
    Segment* GetSegment();

    /* Parallel Heap Scan Support */
    void SetParallelController(ParallelWorkController *controller, int smpId);
    void SetRescan();
    PageId GetNextPageParallel();
    PageId GetFirstPageParallel();
    inline ParallelWorkController* GetParallelController()
    {
        return m_controller;
    }

private:

    /* Private helper function to move extents */
    PageId MoveExtents();
    inline bool InParallelMode()
    {
        return m_controller != nullptr;
    }

    /* Local variables used in sequence scan */
    PageId m_curScanExtMetaPageId = INVALID_PAGE_ID;
    uint16 m_curScanExtPageNum = 0;
    PageId m_lastScanPageId = INVALID_PAGE_ID;
    PageId m_endScanPageId = INVALID_PAGE_ID;
    StorageRelation m_rel = nullptr;
    SmgrType m_smgrType = SmgrType::INVALID_SMGR_TYPE;
    BufMgrInterface *m_bufMgr = nullptr;
    /* Parallel Heap Scan Support */
    ParallelWorkController *m_controller = nullptr;
    ParallelWorkloadInfoInterface *m_curWorkload = nullptr;
    int m_smpId = 0;
};

} /* namespace DSTORE */
#endif  // STORAGE_DATA_SEGMENT_CONTEXT_H
