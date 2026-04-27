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
 * thread_pool_internal.h
 *
 * Description:
 * the internal header file of the thread pool.
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef UTILS_THREAD_POOL_INTERNAL_H
#define UTILS_THREAD_POOL_INTERNAL_H

#include "container/linked_list.h"
#include "types/ref_object.h"
#include "thread/thread_pool.h"
#include "schedule/scheduler.h"

GSDB_BEGIN_C_CODE_DECLS

#define DEFAULT_DETECT_THRESHOLD  (2 * 60) // 2 minutes
#define RUN_CRITICAL_TIMEOUT_TIME 3

typedef enum PoolState { READY, RUNNING, SHUTDOWN } PoolState;

/* some statistical information about the thread pool itself, such as thread pool task submission delay, number of
 * worker threads, number of idle threads, <idleWorkerCnt and aliveWorkerCnt put in it soon after> and etc. */
typedef struct {
    uint64_t submitMaxLatency;
    uint64_t submitTotalLatency;
    uint64_t submitCount;
} ThreadPoolStats;

struct ThreadPool {
    void *oriPtr;
    PoolState state;
    ThreadPoolParams params;
    /* task run time over long detect thresholdhold, set to zero is disable, unit: seconds */
    uint16_t taskRunTimeThreshold;
    /* threadpool overload detect thresholdhold, set to zero is disable, unit: seconds */
    uint16_t overloadTimeThreshold;
    uint16_t scheduleLatencyThreshold;
    MemAllocator *allocator;
    Scheduler *scheduler;
    Mutex workerLock;
    uint32_t idleWorkerCnt;
    uint32_t aliveWorkerCnt;
    uint64_t overLoadStartTime;
    DListHead workerQueue;
    Mutex poolLock;
    uint32_t poolRef;
    Mutex detectThreadLock;
    ConditionVariable terminatedCond;
    uint64_t poolStatsControl;
    /* current update index of poolStatsData, can atomic change poolStats easily */
    ThreadPoolStats poolStats[2]; /* 2, two buffer, to atomic change poolStatsData all data will hardly */
} __attribute__((aligned(GS_CACHE_LINE_SIZE)));

/* statistics about the scheduling delay and task running status of thread pool tasks */
typedef struct {
    uint64_t minDelay;
    uint64_t maxDelay;
    uint64_t totalDelay;
    uint64_t taskRunningTime;
    uint64_t taskRunningMaxTime;
    uint64_t lastShowStatTime;
    uint64_t count;
} SchedStats;

typedef struct {
    SchedStats stats;
    uint64_t startRunTime;
    uint64_t lastRunTime;
    void *runFunc; // low 32bit of user routine or run function
    void *runArgs; // low 32bit of user threadpool task args
    void *currRun; // user threadpool task
    int64_t tid;
    DListNode node;
    Tid threadId;
    void *(*routine)(void *args);
    ThreadPool *pool;
    uint8_t workRef;
    bool isInitialWorker;
} WorkerCtx;

GSDB_END_C_CODE_DECLS
#endif // UTILS_THREAD_POOL_INTERNAL_H
