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
 * dstore_perf.h
 *
 *  Created on: May 23, 2022
 *  For performance counters
 */

#ifndef SRC_GAUSSKERNEL_DSTORE_INCLUDE_COMMON_PERFUMENT_PERF_DSTORE_PERF_H_
#define SRC_GAUSSKERNEL_DSTORE_INCLUDE_COMMON_PERFUMENT_PERF_DSTORE_PERF_H_

#include <sys/ioctl.h>
#include <sys/time.h>
#include "common/memory/dstore_mctx.h"
#include "common/dstore_datatype.h"

namespace DSTORE {

typedef struct timeval perf_time;

constexpr int SECONDS_TO_MICROSECONDS = 1000000;

inline void PerfTimeCompDiff(perf_time &x, const perf_time y, const perf_time z);

#define PERF_TIME_IS_ZERO(t) ((t).tv_usec == 0 && (t).tv_sec == 0)
#define PERF_TIME_IS_INTMAX(t) ((t).tv_usec == LONG_MAX && (t).tv_sec == LONG_MAX)
#define PERF_TIME_INITIAL_MIN(t) ((t).tv_usec = LONG_MAX, (t).tv_sec = LONG_MAX)
#define PERF_TIME_IS_BIGGER(x, y) \
    ((x).tv_sec > (y).tv_sec || (((x).tv_sec == (y).tv_sec) && ((x).tv_usec > (y).tv_usec)))
#define PERF_TIME_SET_ZERO(t) ((t).tv_sec = 0, (t).tv_usec = 0)
#define PERF_TIME_SET_TIME(x, y) ((x).tv_sec = (y).tv_sec, (x).tv_usec = (y).tv_usec)
inline void PerfTimeSetCurrent(perf_time &t)
{
    (void)gettimeofday(&(t), nullptr);
}

#define PERF_TIME_ADD(x, y)                                 \
    do {                                                    \
        (x).tv_sec += (y).tv_sec;                           \
        (x).tv_usec += (y).tv_usec;                         \
        /* Normalize */                                     \
        while ((x).tv_usec >= SECONDS_TO_MICROSECONDS) {    \
            (x).tv_usec -= SECONDS_TO_MICROSECONDS;         \
            (x).tv_sec++;                                   \
        }                                                   \
    } while (0)

#define PERF_TIME_SUBTRACT(x, y)                    \
    do {                                            \
        (x).tv_sec -= (y).tv_sec;                   \
        (x).tv_usec -= (y).tv_usec;                 \
        /* Normalize */                             \
        while ((x).tv_usec < 0) {                   \
            (x).tv_usec += SECONDS_TO_MICROSECONDS; \
            (x).tv_sec--;                           \
        }                                           \
    } while (0)

inline double PerfTimeGetDouble(const perf_time t)
{
    return ((t).tv_sec) + (((t).tv_usec) / static_cast<double>(STORAGE_USECS_PER_SEC));
}

inline double PerfTimeGetMillisec(const perf_time t)
{
    return (((t).tv_sec * static_cast<double>(STORAGE_MSECS_PER_SEC)) +
        ((t).tv_usec) / static_cast<double>(STORAGE_MSECS_PER_SEC));
}

inline uint64 PerfTimeGetMicrosrc(const perf_time t)
{
    return ((static_cast<uint64>((t).tv_sec) * STORAGE_USECS_PER_SEC) + static_cast<uint64>((t).tv_usec));
}
enum PerfCpuEvent : uint32 {
    /* Total cycles */
    PERF_CPU_CYCLES = 0,
    /* Retired PERFuctions */
    PERF_CPU_PERFUCIONS,
    /* Cache accesses.  Usually this indicates Last
      Level Cache accesses but this may vary
      depending on your CPU.  This may include
      prefetches and coherency messages */
    PERF_CACHE_REFERENCE,
    /* Cache misses.  Usually this indicates Last
      Level Cache misses; */
    PERF_CACHE_MISS,
    /* Retired branch PERFuctions. */
    PERF_BRANCH_PERFUCTION,
    /*
     * Mispredicted branch PERFuctions.
     */
    PERF_BRANCH_MISS,
    /* track trace point time */
    PERF_RECORD_TIME,
    /* trace trace point enter count */
    PERF_RECORD_COUNT,
    /* trace for common point */
    PERF_RECORD_COMMON
};

struct PerfCpuCounter {
    int           event;
    const char*   name;
};

#define ENABLE_CPU_CYCYLES  (1 << static_cast<uint32>(PERF_CPU_CYCLES))
#define ENABLE_CPU_PERFUCTION (1 << static_cast<uint32>(PERF_CPU_PERFUCIONS))
#define ENABLE_CACHE_REF  (1 << static_cast<uint32>(PERF_CACHE_REFERENCE))
#define ENABLE_CACHE_MISS  (1 << static_cast<uint32>(PERF_CACHE_MISS))
#define ENABLE_BRANCH_PERFUCTION (1 << static_cast<uint32>(PERF_BRANCH_PERFUCTION))
#define ENABLE_BRANCH_MISS     (1 << static_cast<uint32>(PERF_BRANCH_MISS))
#define ENABLE_RECORD_TIME      (1 << static_cast<uint32>(PERF_RECORD_TIME))
#define ENABLE_RECORD_COUNT     (1 << static_cast<uint32>(PERF_RECORD_COUNT))
#define ENABLE_RECORD_COMMON      (1 << static_cast<uint32>(PERF_RECORD_COMMON))

const int PERF_TRACE_DEFAULT = 0b00101011;
const int PERF_TRACE_ALL = 0b11111111;
const int PERF_TRACE_TIME = 0b01000000;
const int PERF_CPU_MAX_EVENT = 6;

class PerfCpuMonitor {
public:

    PerfCpuMonitor();
    RetStatus Init();
    void      Destroy();
    void ReadCpuCounter(int event, int64  *counter) const;
    static PerfCpuCounter  m_event[PERF_CPU_MAX_EVENT];
    inline bool IsInit() const { return m_init;}

private:
    bool m_init;
    int  m_fd[PERF_CPU_MAX_EVENT];
};

class PerfTracePoint {
public:
    void Init(const char* name, PerfCpuMonitor* cpumon, int traceFlag = PERF_TRACE_DEFAULT);
    void Start();
    void End();
    void Reset();
    void Print() const;
    void IncreaseCalledCount();
    void IncreaseCommonCount(uint64_t weight = 1);

    uint64_t GetCalledCount() const;
    uint64_t GetCommonCount() const;
    uint64_t GetTotalCallMicroSec();
    uint64_t GetMaxCallMicroSec();
    uint64_t GetMinCallMicroSec();

private:
    /* trace cpu */
    int64 m_tempCounter[PERF_CPU_MAX_EVENT];
    int64 m_counter[PERF_CPU_MAX_EVENT];
    bool  m_counter_enable[PERF_CPU_MAX_EVENT];

    /* called enter count */
    uint64_t m_called_count;
    bool  m_enable_called_count;

    /* common count */
    uint64_t m_common_count;
    bool m_enable_common_count;

    /* trace time */
    perf_time m_time_start;
    perf_time m_time_total;
    perf_time m_timeMax;
    perf_time m_timeMin;
    bool      m_enable_trace_time;

    bool m_traceStarted;
    const char* m_trace_name;
    PerfCpuMonitor  *m_cpu_moniter;
};

}
#endif /* SRC_GAUSSKERNEL_DSTORE_INCLUDE_COMMON_PERFUMENT_PERF_STORAGE_PERF_H_ */
