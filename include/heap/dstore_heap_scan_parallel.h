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
 * dstore_heap_scan_parallel.h
 *
 * IDENTIFICATION
 *        include/heap/dstore_heap_scan_parallel.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_HEAP_SCAN_PARALLEL_H
#define DSTORE_HEAP_SCAN_PARALLEL_H


#include "framework/dstore_parallel.h"
#include "buffer/dstore_buf_mgr.h"


namespace DSTORE {
class ParallelHeapScanWorkSource : public WorkSourceInterface {
public:
    ParallelHeapScanWorkSource(PdbId pdbId, PageId segmentPageId, BufMgrInterface *bufMgr);
    ~ParallelHeapScanWorkSource() final;
    RetStatus Init() override;
    ParallelWorkloadInfoInterface *GetWork() override;
    bool InProgress() override;
    RetStatus Reset() override;
    PageId GetMetaPageId() override;
    ParallelWorkloadInfoInterface *GetWork(int smpId, bool isRescan);
private:
    RetStatus AdvanceExtent();
    PdbId m_pdbId;
    PageId m_segmentMetaPageId;
    PageId m_curExtMetaPageId;
    PageId m_curExtEndPageId;
    PageId m_segEndPageId;
    uint16 m_firstExtentSize;
    BufMgrInterface *m_bufMgr;
};

/**
 * Workload info for parallel heap scan. Pages are continuous in one workload batch
*/
class ParallelHeapScanWorkloadInfo : public ParallelWorkloadInfoInterface {
public:
    ParallelHeapScanWorkloadInfo() {}
    ParallelHeapScanWorkloadInfo(PdbId pdbId, PageId extMetaPageId, BufMgrInterface *bufMgr, PageId segEndPageId);
    ~ParallelHeapScanWorkloadInfo() final;
    PageId GetStartPage() override;
    PageId GetEndPage() override;
    PageId GetNextPage() override;
    uint GetNumPagesLeft() override;
    void Reset() override;
private:
    PdbId m_pdbId;
    PageId m_curExtMetaPageId;
    PageId m_lastScanPageId;
    PageId m_extEndPageId;
    PageId m_segEndPageId;
    BufMgrInterface *m_bufMgr;
    bool m_done;
};

} /* namespace DSTORE */

#endif