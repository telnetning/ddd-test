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

#ifndef DSTORE_PDBREPLICA_PERF_UNIT_H
#define DSTORE_PDBREPLICA_PERF_UNIT_H

#include "perfcounter/dstore_perf_catalog.h"

#define PERF_COUNTER_CDBREPLICA_PERF_UNIT(_def) \
    _def(DSTORE::LatencyStat, m_searchPrimaryPdb, searchPrimaryPdb, DSTORE::PerfLevel::RELEASE); \
    _def(DSTORE::LatencyStat, m_queryPdbReplicaInfo, queryPdbReplicaInfo, DSTORE::PerfLevel::RELEASE); \
    _def(DSTORE::LatencyStat, m_clearResourceInPrimary, clearResourceInPrimary, DSTORE::PerfLevel::RELEASE); \
    _def(DSTORE::LatencyStat, m_clearResourceInStandby, clearResourceInStandby, DSTORE::PerfLevel::RELEASE); \
    _def(DSTORE::LatencyStat, m_stopAllBgThreads, stopAllBgThreads, DSTORE::PerfLevel::RELEASE); \
    _def(DSTORE::LatencyStat, m_doSwichoverInStandby, doSwichoverInStandby, DSTORE::PerfLevel::RELEASE); \
    _def(DSTORE::LatencyStat, m_sendWalToStandby, sendWalToStandby, DSTORE::PerfLevel::RELEASE); \
    _def(DSTORE::LatencyStat, m_standbyWriteWalTobuffer, standbyWriteWalTobuffer, DSTORE::PerfLevel::RELEASE); \
    _def(DSTORE::LatencyStat, m_sendWalHeartbeat, sendWalHeartbeat, DSTORE::PerfLevel::RELEASE); \
    _def(DSTORE::LatencyStat, m_handleWalHeartbeat, handleWalHeartbeat, DSTORE::PerfLevel::RELEASE)

namespace DSTORE {
class PdbReplicaPerfUnit : public DstorePerfUnit {
public:
    static PdbReplicaPerfUnit &GetInstance()
    {
        static PdbReplicaPerfUnit perfUnit{};

        return perfUnit;
    };

    ~PdbReplicaPerfUnit() override = default;

    bool Init(DstoreMemoryContext memCtxPtr)
    {
        if (!InitUnit("PdbReplicaPerfUnit", memCtxPtr, PERF_UNIT_DEFAULT_TASK_INDEX)) {
            return false;
        }

        PERF_COUNTER_CDBREPLICA_PERF_UNIT(REGIST_PERFITEM);
        return true;
    }

    PERF_COUNTER_CDBREPLICA_PERF_UNIT(DECLARE_PERF_ITEM);

private:
    PdbReplicaPerfUnit() = default;
};
}  // namespace DSTORE
#endif /* DSTORE_PDBREPLICA_PERF_UNIT_H */
