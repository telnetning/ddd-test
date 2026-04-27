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

#ifndef DSTORE_WAL_PERF_UNIT_H
#define DSTORE_WAL_PERF_UNIT_H

#include "perfcounter/dstore_perf_catalog.h"

#define PERF_COUNTER_WAL_PERF_UNIT(_def)                                                                     \
    _def(DSTORE::LatencyStat, m_waitPlsnFlushedLatency, waitPlsnFlushedLatency, DSTORE::PerfLevel::RELEASE); \
    _def(DSTORE::LatencyStat, m_buildDirtyPageSet, buildDirtyPageSet, DSTORE::PerfLevel::RELEASE); \
    _def(DSTORE::LatencyStat, m_broadcastTakeOverTerm, broadcastTakeOverTerm, DSTORE::PerfLevel::RELEASE); \
    _def(DSTORE::LatencyStat, m_endAtomicWalLatency, endAtomicWalLatency, DSTORE::PerfLevel::PERF_DEBUG); \
    _def(DSTORE::LatencyStat, m_walWriteBufferLatency, walWriteBufferLatency, DSTORE::PerfLevel::PERF_DEBUG); \
    _def(DSTORE::LatencyStat, m_walFlushToDiskLatency, walFlushToDiskLatency, DSTORE::PerfLevel::PERF_DEBUG); \
    _def(DSTORE::LatencyStat, m_walFlushWaitMoreData, walFlushWaitMoreData, DSTORE::PerfLevel::PERF_DEBUG); \
    _def(DSTORE::LatencyStat, m_walFlushWaitUsableFile, walFlushWaitUsableFile, DSTORE::PerfLevel::PERF_DEBUG); \
    _def(DSTORE::LatencyStat, m_walFlushCallbackLatency, walFlushCallbackLatency, DSTORE::PerfLevel::PERF_DEBUG); \
    _def(DSTORE::LatencyStat, m_walRedoPreloadBufferBlock, walRedoPreloadBufferBlock, DSTORE::PerfLevel::RELEASE); \
    _def(DSTORE::LatencyStat, m_walRedoReadNextWalGroup, walRedoReadNextWalGroup, DSTORE::PerfLevel::PERF_DEBUG); \
    _def(DSTORE::LatencyStat, m_walRedoDispatchWalRecord, walRedoDispatchWalRecord, DSTORE::PerfLevel::PERF_DEBUG); \
    _def(DSTORE::LatencyStat, m_walRedoSingleWalRecord, walRedoSingleWalRecord, DSTORE::PerfLevel::RELEASE)

namespace DSTORE {
class WalPerfUnit : public PerfUnit {
public:
    static WalPerfUnit &GetInstance()
    {
        static WalPerfUnit perfUnit{};

        return perfUnit;
    };

    ~WalPerfUnit() override = default;

    bool Init(DstoreMemoryContext memCtxPtr)
    {
        if (!InitUnit("WalPerfUnit", memCtxPtr, PERF_UNIT_DEFAULT_TASK_INDEX)) {
            return false;
        }

        PERF_COUNTER_WAL_PERF_UNIT(REGIST_PERFITEM);
        return true;
    }

    bool Destroy()
    {
        if (!DestroyUnit()) {
            return false;
        }
        return true;
    }

    PERF_COUNTER_WAL_PERF_UNIT(DECLARE_PERF_ITEM);

private:
    WalPerfUnit() = default;
};
}  // namespace DSTORE
#endif /* STORAGE_WAL_PERF_UNIT_H */
