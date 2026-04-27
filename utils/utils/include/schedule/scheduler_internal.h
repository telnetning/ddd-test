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

#ifndef UTILS_SCHEDULER_INTERNAL_H
#define UTILS_SCHEDULER_INTERNAL_H

#include "syslog/err_log.h"

#define SCHEDULE_STATE_OK      0
#define SCHEDULE_STATE_TIMEOUT (-1)
#define SCHEDULE_STATE_EXIT    (-2)
#define SCHEDULE_STATE_SHRINK  (-3)

#define TIME_CONVERSION_FACTOR (uint64_t)1000
#define ONE_MINUTE_TO_US       (60 * TIME_CONVERSION_FACTOR * TIME_CONVERSION_FACTOR)

static inline uint64_t GetMonotonicTimeUs(void)
{
    ErrorCode errCode = ERROR_SYS_OK;
    TimeValue timeNow = GetClockValue(CLOCKTYPE_MONOTONIC, &errCode);
    if (errCode == ERROR_SYS_OK) {
        return (uint64_t)timeNow.seconds * TIME_CONVERSION_FACTOR * TIME_CONVERSION_FACTOR + (uint64_t)timeNow.useconds;
    }
    ErrLog(ERROR, ErrMsg("Get monotonic time fail."));
    return (uint64_t)-1;
}

static inline void SetMaxValueSafety(size_t *ptr, size_t val)
{
    size_t oldVal = *ptr;
    do {
        if (likely(oldVal >= val)) {
            /* if we value(val) is not greater than the original value in ptr, go out */
            return;
        }
    } while (!__atomic_compare_exchange_n(ptr, &oldVal, val, true, __ATOMIC_RELAXED, __ATOMIC_RELAXED));
}

struct ScheduleCallback {
    void *context;
    uint64_t idleTimeThreshold;
    void (*newWorkerCallback)(void *userContext);
    void (*incIdleWorkerCallback)(void *userContext);
    void (*decIdleWorkerCallback)(void *userContext);
    void (*updateSchedStatsCallback)(void *userContext, uint64_t startTime);
    void (*taskPreRunCallback)(void *userContext, void *task, void *func, void *args);
    void (*taskPostRunCallback)(void *userContext);
};

static inline void DecIdleWorkerCnt(ScheduleCallback *callback)
{
    if (callback && callback->decIdleWorkerCallback) {
        callback->decIdleWorkerCallback(callback->context);
    }
}

static inline void TestNewWorker(ScheduleCallback *callback)
{
    if (callback && callback->newWorkerCallback) {
        callback->newWorkerCallback(callback->context);
    }
}

static inline void TaskPreRun(ScheduleCallback *callback, void *func, void *task, void *args)
{
    if (callback && callback->taskPreRunCallback) {
        callback->taskPreRunCallback(callback->context, task, func, args);
    }
}

static inline void UpdateSchedStats(ScheduleCallback *callback, uint64_t startTime)
{
    if (callback && callback->updateSchedStatsCallback) {
        callback->updateSchedStatsCallback(callback->context, startTime);
    }
}

static inline void TaskPostRun(ScheduleCallback *callback)
{
    if (callback && callback->taskPostRunCallback) {
        callback->taskPostRunCallback(callback->context);
    }
}

#endif
