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
 */

#ifndef DSTORE_UT_PARALLEL_HEAP_H
#define DSTORE_UT_PARALLEL_HEAP_H

#include "ut_heap/ut_heap.h"
#include "heap/dstore_heap_scan.h"
#include "ut_tablespace/ut_data_segment.h"
#include "heap/dstore_heap_scan_parallel.h"
#include "ut_utilities/ut_dstore_framework.h"

using namespace DSTORE;

class UTParallelHeapScanWorkloadInfoTest : public DataSegmentTest {
protected:
    void SetUp() override
    {
        DataSegmentTest::SetUp();
        m_utTableHandler = UTTableHandler::CreateTableHandler(g_defaultPdbId, m_ut_memory_context);
    }
    void TearDown() override
    {
        UTTableHandler::Destroy(m_utTableHandler);
        m_utTableHandler = nullptr;
        DataSegmentTest::TearDown();
    }

    UTTableHandler *m_utTableHandler;
    bool IsGetNextPageValid(ParallelHeapScanWorkloadInfo parallelHeapScan, PageId extentMetaPage);
};

#endif /* DSTORE_UT_PARALLEL_HEAP_H */
