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
 * dstore_perf.cpp
 *
 *  Created on: May 23, 2022
 *      Author: zanky
 */

#include "common/instrument/perf/dstore_perf.h"
#include <cstdio>
#include <sys/syscall.h>
#include <unistd.h>
#include <linux/perf_event.h>
#include "securec.h"

namespace DSTORE {

inline void PerfTimeCompDiff(perf_time &x, const perf_time y, const perf_time z)
{
    x.tv_sec = y.tv_sec - z.tv_sec;
    x.tv_usec = y.tv_usec - z.tv_usec;
    /* Normalize after each add to avoid overflow/underflow of tv_usec */
    while (x.tv_usec < 0) {
        x.tv_usec += SECONDS_TO_MICROSECONDS;
        x.tv_sec--;
    }
    while (x.tv_usec >= SECONDS_TO_MICROSECONDS) {
        x.tv_usec -= SECONDS_TO_MICROSECONDS;
        x.tv_sec++;
    }
}

PerfCpuCounter PerfCpuMonitor::m_event[PERF_CPU_MAX_EVENT] = {
    {static_cast<int>(PERF_COUNT_HW_CPU_CYCLES), "cpu cycles"},
    {static_cast<int>(PERF_COUNT_HW_INSTRUCTIONS), "cpu instructions"},
    {static_cast<int>(PERF_COUNT_HW_CACHE_REFERENCES), "cache ref"},
    {static_cast<int>(PERF_COUNT_HW_CACHE_MISSES), "cache miss"},
    {static_cast<int>(PERF_COUNT_HW_BRANCH_INSTRUCTIONS), "branch instructions"},
    {static_cast<int>(PERF_COUNT_HW_BRANCH_MISSES), "branch miss"},
};

void PerfTracePoint::Init(const char *name, PerfCpuMonitor* cpumon, int traceFlag)
{
    m_traceStarted = false;
    m_trace_name = name;
    m_cpu_moniter = cpumon;
    bool cpuInit = (cpumon != nullptr && cpumon->IsInit());
    for (int i = 0 ; i < PERF_CPU_MAX_EVENT ; i++) {
        m_counter[i] = 0;
        m_tempCounter[i] = 0;
        if (cpuInit && (static_cast<uint>(traceFlag) & (1u << static_cast<uint>(i)))) {
            m_counter_enable[i] = true;
        } else {
            m_counter_enable[i] = false;
        }
    }

    if (static_cast<uint>(traceFlag) & ENABLE_RECORD_COUNT) {
        m_enable_called_count = true;
        m_called_count = 0;
    } else {
        m_enable_called_count = false;
    }

    if (static_cast<uint>(traceFlag) & ENABLE_RECORD_COMMON) {
        m_enable_common_count = true;
        m_common_count = 0;
    } else {
        m_enable_common_count = false;
    }

    if (static_cast<uint>(traceFlag) & ENABLE_RECORD_TIME) {
        m_enable_trace_time = true;
        PERF_TIME_SET_ZERO(m_time_start);
        PERF_TIME_SET_ZERO(m_time_total);
        PERF_TIME_SET_ZERO(m_timeMax);
        PERF_TIME_SET_ZERO(m_timeMin);
    } else {
        m_enable_trace_time = false;
    }
}

void PerfTracePoint::Reset()
{
    for (int i = 0 ; i < PERF_CPU_MAX_EVENT; i++) {
        m_counter[i] = 0;
        m_tempCounter[i] = 0;
    }
    m_called_count = 0;
    m_common_count = 0;
    PERF_TIME_SET_ZERO(m_time_start);
    PERF_TIME_SET_ZERO(m_time_total);
    PERF_TIME_SET_ZERO(m_timeMax);
    PERF_TIME_SET_ZERO(m_timeMin);
    m_traceStarted = false;
}

void PerfTracePoint::Start()
{
    if (m_traceStarted) {
        return;
    }
    for (int i = 0 ; i < PERF_CPU_MAX_EVENT; i++) {
        if (m_counter_enable[i]) {
            m_cpu_moniter->ReadCpuCounter(i, &m_tempCounter[i]);
        }
    }

    if (m_enable_called_count) {
        m_called_count++;
    }

    if (m_enable_trace_time) {
        PerfTimeSetCurrent(m_time_start);
    }
    m_traceStarted = true;
}

void PerfTracePoint::End()
{
    if (!m_traceStarted) {
        return;
    }
    m_traceStarted = false;
    int64 val = 0;
    int64 oval = 0;
    for (int i = 0; i < PERF_CPU_MAX_EVENT; i++) {
        if (m_counter_enable[i]) {
            m_cpu_moniter->ReadCpuCounter(i, &val);
            oval = m_tempCounter[i];
            m_counter[i] += val > oval ? (val - oval) : 0;
        }
    }

    if (m_enable_trace_time) {
        perf_time end_time;
        PerfTimeSetCurrent(end_time);
        perf_time diff_time;
        PerfTimeCompDiff(diff_time, end_time, m_time_start);
        PERF_TIME_ADD(m_time_total, diff_time);
        PERF_TIME_SET_ZERO(m_time_start);

        if (PERF_TIME_IS_ZERO(m_timeMax) || PERF_TIME_IS_BIGGER(diff_time, m_timeMax)) {
            PERF_TIME_SET_TIME(m_timeMax, diff_time);
        }
        if (PERF_TIME_IS_ZERO(m_timeMin) || PERF_TIME_IS_BIGGER(m_timeMin, diff_time)) {
            PERF_TIME_SET_TIME(m_timeMin, diff_time);
        }
    }
}

void PerfTracePoint::Print() const
{
    (void)printf("=========[trace point: %s]============\n", m_trace_name);
    for (int i = 0; i < PERF_CPU_MAX_EVENT; i++) {
        if (m_counter_enable[i]) {
            (void)printf("[cpu event:%s] %ld\n", PerfCpuMonitor::m_event[i].name, m_counter[i]);
        } else {
            (void)printf("[cpu event:%s] disabled \n", PerfCpuMonitor::m_event[i].name);
        }
    }

    if (m_enable_called_count) {
        (void)printf("[trace count] %lu\n", m_called_count);
    } else {
        (void)printf("[trace count] disabled \n");
    }

    if (m_enable_trace_time) {
        double totalTime = PerfTimeGetMillisec(m_time_total);
        (void)printf("[total time] %.3f(ms)\n", totalTime);
    } else {
        (void)printf("[total time] disabled \n");
    }

    (void)printf("=========[trace point: %s]============\n", m_trace_name);
}

uint64_t PerfTracePoint::GetCalledCount() const
{
    if (!m_enable_called_count) {
        return 0;
    }
    return m_called_count;
}

uint64_t PerfTracePoint::GetTotalCallMicroSec()
{
    if (!m_enable_trace_time) {
        return 0;
    }
    return PerfTimeGetMicrosrc(m_time_total);
}

uint64_t PerfTracePoint::GetMaxCallMicroSec()
{
    if (!m_enable_trace_time) {
        return 0;
    }
    return PerfTimeGetMicrosrc(m_timeMax);
}

uint64_t PerfTracePoint::GetMinCallMicroSec()
{
    if (!m_enable_trace_time) {
        return 0;
    }
    return PerfTimeGetMicrosrc(m_timeMin);
}

void PerfTracePoint::IncreaseCalledCount()
{
    if (m_enable_called_count) {
        m_called_count++;
    }
}

void PerfTracePoint::IncreaseCommonCount(uint64_t weight)
{
    if (m_enable_common_count) {
        if (weight >= UINT64_MAX - m_common_count) {
            m_common_count = UINT64_MAX;
        } else {
            m_common_count += weight;
        }
    }
}

uint64_t PerfTracePoint::GetCommonCount() const
{
    if (!m_enable_common_count) {
        return 0;
    }
    return m_common_count;
}

PerfCpuMonitor::PerfCpuMonitor()
    :m_init(false)
{
    for (int i = 0; i < PERF_CPU_MAX_EVENT; i++) {
        m_fd[i] = -1;
    }
}

RetStatus PerfCpuMonitor::Init()
{
    struct perf_event_attr hw_event;
    const pid_t pid = 0;  // current thread
    const int cpu = -1;
    const int group_fd = -1;
    const unsigned long flags = 0;
    int ret;
    errno_t rc = 0;
    if (m_init) {
        return DSTORE_SUCC;
    }

    /* Create a set of counters */
    rc = memset_s(&hw_event, sizeof(hw_event), 0, sizeof(hw_event));
    storage_securec_check(rc, "\0", "\0");
    hw_event.type = static_cast<uint32_t>(PERF_TYPE_HARDWARE);
    hw_event.size = sizeof(struct perf_event_attr);
    hw_event.disabled = 1;
    hw_event.exclude_kernel = 1;
    hw_event.exclude_hv = 1;
    for (int i = 0; i < PERF_CPU_MAX_EVENT; i++) {
        hw_event.config = static_cast<uint64_t>(static_cast<uint>(m_event[i].event));
        auto fd = syscall(__NR_perf_event_open, &hw_event, pid, cpu, group_fd, flags);
        if (fd < 0) {
            return DSTORE_FAIL;
        }
        m_fd[i] = static_cast<int>(fd);
    }

    /* Reset and start up counters */
    for (int i = 0; i < PERF_CPU_MAX_EVENT; i++) {
        ret = ioctl(m_fd[i], PERF_EVENT_IOC_RESET, 0);
        if (ret != 0) {
            return DSTORE_FAIL;
        }

        ret = ioctl(m_fd[i], PERF_EVENT_IOC_ENABLE, 0);
        if (ret != 0) {
            return DSTORE_FAIL;
        }
    }

    m_init = true;
    return DSTORE_SUCC;
}

void PerfCpuMonitor::Destroy()
{
    if (m_init) {
        return;
    }

    for (int i = 0; i < PERF_CPU_MAX_EVENT; i++) {
        if (m_fd[i] != -1) {
            int ret = ioctl(m_fd[i], PERF_EVENT_IOC_DISABLE, 0);
            if (ret != 0) {
                StorageAssert(0);
            }
            (void)close(m_fd[i]);
            m_fd[i] = -1;
        }
    }
    m_init = false;
}

void PerfCpuMonitor::ReadCpuCounter(int event, int64 *counter) const
{
    auto ret = read(m_fd[event], counter, sizeof(int64));
    if (static_cast<size_t>(ret) != sizeof(int64)) {
        StorageAssert(0);
    }
}
}
