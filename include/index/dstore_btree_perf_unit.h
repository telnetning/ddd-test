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
 * Description: The performance monitoring unit of UpdateFile.
 */

#ifndef DSTORE_BTREE_PERF_UNIT_H
#define DSTORE_BTREE_PERF_UNIT_H

#include "perfcounter/dstore_perf_catalog.h"

#define PERF_COUNTER_BTREE_PERF_UNIT(_def)  \
    _def(DSTORE::LatencyStat, m_btreeSearchForReadLatency,  \
         btreeSearchForReadLatency, DSTORE::PerfLevel::PERF_DEBUG);  \
    _def(DSTORE::LatencyStat, m_crPageMakeLatency, crPageMakeLatency, DSTORE::PerfLevel::PERF_DEBUG);  \
    _def(DSTORE::LatencyStat, m_btreeGetNextTupleLatency, btreeGetNextTupleLatency, DSTORE::PerfLevel::PERF_DEBUG);  \
    _def(DSTORE::LatencyStat, m_leafPageScanLatency, leafPageScanLatency, DSTORE::PerfLevel::PERF_DEBUG);  \
    _def(DSTORE::LatencyStat, m_walkRightOnLeafLatency, walkRightOnLeafLatency, DSTORE::PerfLevel::PERF_DEBUG);  \
    _def(DSTORE::LatencyStat, m_btreeSearchHitCacheForScanLatency,  \
         btreeSearchHitCacheForScanLatency, DSTORE::PerfLevel::PERF_DEBUG);  \
    _def(DSTORE::LatencyStat, m_btreeSearchForWriteLatency,  \
         btreeSearchForWriteLatency, DSTORE::PerfLevel::PERF_DEBUG);  \
    _def(DSTORE::LatencyStat, m_waitForTxnEndLatency, waitForTxnEndLatency, DSTORE::PerfLevel::PERF_DEBUG);  \
    _def(DSTORE::LatencyStat, m_btreeInsertTupleLatency, btreeInsertTupleLatency, DSTORE::PerfLevel::PERF_DEBUG);  \
    _def(DSTORE::LatencyStat, m_btreePageSplitLatency, btreePageSplitLatency, DSTORE::PerfLevel::PERF_DEBUG);  \
    _def(DSTORE::LatencyStat, m_findSplitLocLatency, findSplitLocLatency, DSTORE::PerfLevel::PERF_DEBUG);  \
    _def(DSTORE::LatencyStat, m_stepRightLatency, stepRightLatency, DSTORE::PerfLevel::PERF_DEBUG);  \
    _def(DSTORE::LatencyStat, m_btreeDepthIncreaseLatency, btreeDepthIncreaseLatency, DSTORE::PerfLevel::RELEASE);  \
    _def(DSTORE::LatencyStat, m_btreeSearchHitCacheForInsertLatency,  \
         btreeSearchHitCacheForInsertLatency, DSTORE::PerfLevel::PERF_DEBUG);  \
    _def(DSTORE::LatencyStat, m_btreePagePruneLatency, btreePagePruneLatency, DSTORE::PerfLevel::PERF_DEBUG);  \
    _def(DSTORE::LatencyStat, m_btreeDeleteLatency, btreeDeleteLatency, DSTORE::PerfLevel::PERF_DEBUG);  \
    _def(DSTORE::LatencyStat, m_btreePageBatchRecycleLatency,  \
         btreePageBatchRecycleLatency, DSTORE::PerfLevel::PERF_DEBUG);  \
    _def(DSTORE::LatencyStat, m_btreePageUnlinkLatency, btreePageUnlinkLatency, DSTORE::PerfLevel::PERF_DEBUG);  \
    _def(DSTORE::LatencyStat, m_btreeBuildLatency, btreeBuildLatency, DSTORE::PerfLevel::RELEASE);  \
    _def(DSTORE::LatencyStat, m_btreeBatchWriteLatency, btreeBatchWriteLatency, DSTORE::PerfLevel::RELEASE);  \
    _def(DSTORE::LatencyStat, m_btreeAllocNewPage, btreeAllocNewPage, DSTORE::PerfLevel::RELEASE)  \

namespace DSTORE {

class BtreePerfUnit : public DstorePerfUnit {
public:
    static BtreePerfUnit &GetInstance()
    {
        static BtreePerfUnit perfUnit{};

        return perfUnit;
    };

    ~BtreePerfUnit() override = default;

    bool Init(DstoreMemoryContext memCtxPtr)
    {
        if (!InitUnit("BtreePerfUnit", memCtxPtr, PERF_UNIT_DEFAULT_TASK_INDEX)) {
            return false;
        }

        PERF_COUNTER_BTREE_PERF_UNIT(REGIST_PERFITEM);
        return true;
    }

    PERF_COUNTER_BTREE_PERF_UNIT(DECLARE_PERF_ITEM);

private:
    BtreePerfUnit() = default;
};
}  // namespace DSTORE
#endif /* STORAGE_BTREE_PERF_UNIT_H */
