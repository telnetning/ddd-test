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
 * dstore_wal_perf_statistic.h
 *
 * Description:
 * Define Wal performance statistic related data structure and method
 * ---------------------------------------------------------------------------------------
 *
 */
#ifndef DSTORE_DSTORE_WAL_PERF_STATISTIC_H
#define DSTORE_DSTORE_WAL_PERF_STATISTIC_H

#include "securec.h"
#include "common/dstore_datatype.h"
#include "framework/dstore_instance_interface.h"
#include "common/instrument/perf/dstore_perf.h"
#include "dstore_wal_struct.h"

namespace DSTORE {

/*
 * Wal performance statistic core data structure
 */
struct WalPerfCounter {
    PerfTracePoint streamWriteCounter;
    PerfTracePoint streamFlushCounter;

    PerfTracePoint bufferGetCounter;
    PerfTracePoint bufferMarkInsertCounter;
    PerfTracePoint typeCounter[WAL_TYPE_BUTTOM];

    void Init()
    {
        /*
        ENABLE_RECORD_TIME:Statistics call duration
        ENABLE_RECORD_COUNT:Statistics the number of calls
        ENABLE_RECORD_COMMON:Statistics the cumulative size
        */
        int traceFlag = ENABLE_RECORD_TIME | ENABLE_RECORD_COUNT;
        streamWriteCounter.Init("WalWriteTracer", nullptr, traceFlag);
        streamFlushCounter.Init("WalFlushTracer", nullptr, traceFlag);
        traceFlag = ENABLE_RECORD_COUNT | ENABLE_RECORD_COMMON;
        bufferGetCounter.Init("WalGetBufferTracer", nullptr, traceFlag);
        bufferMarkInsertCounter.Init("WalMarkInsertTracer", nullptr, traceFlag);
        for (uint16 type = 0; type < static_cast<uint16>(WAL_TYPE_BUTTOM); type++) {
            typeCounter[type].Init("WalTypeTracer", nullptr, traceFlag);
        }
    }

    void Reset()
    {
        streamWriteCounter.Reset();
        streamFlushCounter.Reset();
        bufferGetCounter.Reset();
        bufferMarkInsertCounter.Reset();
    }
    inline void StartWalTracePoint(PerfTracePoint &perfTracePoint)
    {
        if (unlikely(static_cast<uint64>(g_traceSwitch) & WAL_TRACE_SWITCH)) {
            perfTracePoint.Start();
        }
    }
    inline void EndWalTracePoint(PerfTracePoint &perfTracePoint)
    {
        if (unlikely(static_cast<uint64>(g_traceSwitch) & WAL_TRACE_SWITCH)) {
            perfTracePoint.End();
        }
    }
    inline void IncreaseWalTraceWaitCount(PerfTracePoint &perfTracePoint)
    {
        if (unlikely(static_cast<uint64>(g_traceSwitch) & WAL_TRACE_SWITCH)) {
            perfTracePoint.IncreaseCommonCount();
        }
    }
    inline void IncreaseWalTypeCount(WalType type, uint64_t weight)
    {
        if (unlikely(static_cast<uint64>(g_traceSwitch) & WAL_TRACE_SWITCH)) {
            StorageAssert(type < WAL_TYPE_BUTTOM);
            typeCounter[static_cast<int>(type)].IncreaseCalledCount();
            typeCounter[static_cast<int>(type)].IncreaseCommonCount(weight);
        }
    }
};
}

#endif /* DSTORE_STORAGE_WAL_PERF_STATISTIC_H */
