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

#ifndef DSTORE_FRAMEWORK_PERF_UNIT_H
#define DSTORE_FRAMEWORK_PERF_UNIT_H

#include "perfcounter/dstore_perf_catalog.h"

#define PERF_COUNTER_FRAMEWORK_PERF_UNIT(_def) \
    _def(DSTORE::LatencyStat, m_createThreadAndRegister, createThreadAndRegister, DSTORE::PerfLevel::PERF_DEBUG); \
    _def(DSTORE::LatencyStat, m_controlPageIterate, controlPageIterate, DSTORE::PerfLevel::PERF_DEBUG); \
    _def(DSTORE::LatencyStat, m_getAllWalStreams, getAllWalStreams, DSTORE::PerfLevel::RELEASE); \
    _def(DSTORE::LatencyStat, m_lockClusterMemberChange, lockClusterMemberChange, DSTORE::PerfLevel::RELEASE); \
    _def(DSTORE::LatencyStat, m_memberViewChange, memberViewChange, DSTORE::PerfLevel::RELEASE); \
    _def(DSTORE::LatencyStat, m_memnodeRemoteCall, memnodeRemoteCall, DSTORE::PerfLevel::RELEASE)

namespace DSTORE {
class FrameworkPerfUnit : public DstorePerfUnit {
public:
    static FrameworkPerfUnit &GetInstance()
    {
        static FrameworkPerfUnit perfUnit{};

        return perfUnit;
    };

    ~FrameworkPerfUnit() override = default;

    bool Init(DstoreMemoryContext memCtxPtr)
    {
        if (!InitUnit("FrameworkPerfUnit", memCtxPtr, PERF_UNIT_DEFAULT_TASK_INDEX)) {
            return false;
        }

        PERF_COUNTER_FRAMEWORK_PERF_UNIT(REGIST_PERFITEM);
        return true;
    }

    PERF_COUNTER_FRAMEWORK_PERF_UNIT(DECLARE_PERF_ITEM);

private:
    FrameworkPerfUnit() = default;
};
}  // namespace DSTORE
#endif /* STORAGE_FRAMEWORK_PERF_UNIT_H */
