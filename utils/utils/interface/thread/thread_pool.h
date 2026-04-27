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
 * thread_pool.h
 *
 * Description:
 * This file describes the interface for using the thread pool, Creating a thread pool, scheduling tasks,
 * canceling a task, and destroying thread pool resources.
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef UTILS_THREAD_POOL_H
#define UTILS_THREAD_POOL_H

#include <pthread.h>

#include "memory/memory_allocator.h"
#include "defines/err_code.h"

GSDB_BEGIN_C_CODE_DECLS

#define THREAD_NAME_LEN 16
/* The following flags can be used together with other flags by |(OR),
 * but THREAD_POOL_FLAG_FIXED and THREAD_POOL_FLAG_SCALING is mutual exclusion */
#define THREAD_POOL_FLAG_FIXED         (1U)
#define THREAD_POOL_FLAG_SCALING       (1U << 1)
#define THREAD_POOL_FLAG_STAT          (1U << 31)
#define THREAD_POOL_FLAG_REALTIME      (1U << 30)
#define THREAD_POOL_FLAG_ENABLE_DETECT (1U << 29)
#define THREAD_POOL_FLAG_DETECT_EXIT   (1U << 28)
#define THREAD_POOL_FLAG_SET_TIMEOUT   (1U << 27)

#define THREAD_POOL_INVALID_TASK_ID 0xFFFFFFFFFFFFFFFFU

typedef struct ThreadPool ThreadPool;
typedef struct Scheduler Scheduler;

typedef uint64_t TaskId;

/**
 * User defined thread enter callback function
 */
typedef void (*TPThreadEnterCallback)(void);

/**
 * User defined thread exit callback function
 */
typedef void (*TPThreadExitCallback)(void);

typedef struct ThreadPoolTask ThreadPoolTask;
struct ThreadPoolTask {
    /*
     * The routine function to be executed, The routine and run cannot be NULL at the same time.
     */
    void *(*routine)(void *args);
    /*
     * The run function to be executed, The routine and run cannot be NULL at the same time.
     */
    void *(*run)(void **args);
    /*
     * The finalize function of the parameter of the task, It can be set to NULL.
     */
    void (*finalize)(void *args);
    /*
     * When the thread pool is destroyed, the not executed tasks will be canceled.
     * This function is used to notify the task owner, It can be set to NULL.
     */
    void (*cancel)(void *args);
    /*
     * The task parameters, including the parameters of the execution function, finalize function, and cancel function.
     */
    void *args;
};

typedef struct ThreadPoolParams ThreadPoolParams;
struct ThreadPoolParams {
    /*
     * The flags identify the mode of the thread pool, include
     * THREAD_POOL_FLAG_FIXED
     * THREAD_POOL_FLAG_SCALING
     * THREAD_POOL_FLAG_STAT
     * THREAD_POOL_FLAG_ENABLE_DETECT
     * THREAD_POOL_FLAG_DETECT_EXIT, will exit the process when detect schedule time out of ScheduleLatencyThreshold
     */
    uint32_t flags;
    /*
     * The minimum number of threads in the thread pool is 1.
     */
    uint32_t minThrdCnt;
    /*
     * The Maximum number of threads in the thread pool,
     * the maxThreadCount must be greater than or equal to minThrdCnt.
     */
    uint32_t maxThrdCnt;
    /*
     * The thread stack size. If the value is 0, the default value is used.
     */
    uint32_t stackSize;
    /*
     * The maximum task queue size. If the value is 0, the default value is used.
     */
    uint32_t maxQueueSize;
    /*
     * The time (ms) a thread can wait for a new task before been terminated
     * if the number of active threads is greater than minThrdCnt.
     */
    uint64_t idleTimeThreshold;
    /*
     * User defined thread enter callback function
     */
    TPThreadEnterCallback threadEnterCallback;
    /*
     * User defined thread exit callback function
     */
    TPThreadExitCallback threadExitCallback;
    /*
     * User defined thread name, if a string include '\0' length <16 bytes, will set the name to OS thread name,
     * on the contrary, not set, such as not init the name array (name not end with '\0' in 16(THREAD_NAME_LEN) bytes).
     */
    char name[THREAD_NAME_LEN];
    /*
     * User defined thread cpu affinity
     */
    cpu_set_t cpuset;

    /* task run time over long detect thresholdhold, set to zero is disable, unit: seconds */
    uint16_t taskRunTimeThres;
};

/**
 * Create one thread pool with the specific parameters.
 *
 * @param[in] params: the attribute of the thread pool to be created.
 * @param[in] allocator: the specified memory manager, if the allocator parameter is not specified, global memory
 * manager is used for internal allocation.
 * @param[out] code: SUCCESS if create thread pool success, or other result code if fail.
 * @return the ThreadPool.
 */
ThreadPool *CreateThreadPool(const ThreadPoolParams *params, MemAllocator *allocator, ErrorCode *code);

/**
 * Create one thread pool with the specific parameters.
 *
 * @param[in] params: the attribute of the thread pool to be created.
 * @param[in] scheduler: the specified task scheduler.
 * @param[in] allocator: the specified memory manager, if the allocator parameter is not specified, global memory
 * manager is used for internal allocation.
 * @param[out] code: SUCCESS if create thread pool success, or other result code if fail.
 * @return the ThreadPool.
 */
ThreadPool *CreateThreadPoolEX(const ThreadPoolParams *params, Scheduler *scheduler, MemAllocator *allocator,
                               ErrorCode *code);

/**
 * Schedule a thread pool task.
 *
 * @param[in] pool: the thread pool to schedule task.
 * @param[in] task: the task specified to be scheduled.
 * @param[out] taskId: return the id of the task.
 * @return SUCCESS if schedule task success, or other result code if fail.
 */
ErrorCode ScheduleThreadPoolTask(ThreadPool *pool, ThreadPoolTask *task, TaskId *taskId);

/**
 * Schedule a thread pool emergency task.
 *
 * @param[in] pool: the thread pool to schedule task.
 * @param[in] task: the task specified to be scheduled. The queue default only can buffer 32 tasks.
 * @param[out] taskId: return the id of the task.
 * @return SUCCESS if schedule task success, or other result code if fail.
 */
ErrorCode ScheduleThreadPoolTaskEmgc(ThreadPool *pool, ThreadPoolTask *task, TaskId *taskId);

/**
 * Cancel the task in the thread pool task queue.
 *
 * @param[in] pool: the thread pool to cancel task.
 * @param[in] taskId: the task to be cancel.
 * @return SUCCESS if cancel task success, or other result code if fail.
 */
ErrorCode CancelThreadPoolTask(ThreadPool *pool, TaskId taskId);

/**
 * The thread pool is notified that the task that runs for a long time is alive,
 * preventing the task from being incorrectly detected as a hang task.
 */
void NotifyThreadPoolTaskAlive(void);

/**
 * Destroy the thread pool.
 *
 * @param[in] pool: the thread pool to be destroyed.
 */
void DestroyThreadPool(ThreadPool *pool);

/* Change threadpool cpu bind set at runtime dynamically
 *
 * @param[in] pool,      the thread pool object pointer
 * @param[in] set,       specific cpu set need to bind, this can changed by cgroup cpubind
 */
void SetThreadPoolCpuset(ThreadPool *pool, const cpu_set_t *set);

/* Change threadpool scheduler statstics function at runtime dynamically
 *
 * @param[in] pool,      the thread pool object pointer
 * @param[in] enable,    true is enable and false is disable scheduler statstics
 */
void SetThreadPoolStatstics(ThreadPool *pool, bool enable);

/* Change task running detect threshold at runtime dynamically
 *
 * @param[in] pool,      the thread pool object pointer
 * @param[in] threshold, task run time over long detect thresholdhold, set to zero is disable, unit: seconds
 */
void SetDetectTaskRunThreshold(ThreadPool *pool, uint16_t threshold);

/* Change overload detect threshold at runtime dynamically
 *
 * @param[in] pool,      the thread pool object pointer
 * @param[in] threshold,  threadpool overload detect thresholdhold, set to zero is disable, unit: seconds
 */
void SetDetectOverloadThreshold(ThreadPool *pool, uint16_t threshold);

/* set decide the task maximum time blocked in task queue, if the time is greater than the set threshold, then
 * will report a warning to errlog
 *
 * @param[in] pool,      the thread pool object pointer
 * @param[in] threshold,  the time thresholdhold to decide task blocked too long, set to zero is disable detect,
                          unit: seconds
 */
void SetDetectScheduleLatencyThreshold(ThreadPool *pool, uint16_t threshold);

/* Change the thread poolthread count at runtime dynamically, minThrdCnt must less equal than maxThrdCnt and not zero.
 * when increasing minThrdCnt, expansion conditions need to be met to make the number of thread pool threads truly
 * reach the minimum value, so if the thread pool is always idle, the thread pool thread counts will is still previous
 * value. the situation is similar to adjust maxmum value.
 *
 * @param[in] pool,       the thread pool object pointer
 * @param[in] minThrdCnt, the min thread count
 * @param[in] maxThrdCnt, the max thread count
 */
void SetThreadPoolThreadCnt(ThreadPool *pool, uint32_t minThrdCnt, uint32_t maxThrdCnt);

/*
 * Obtains the number of alive threads in the thread pool.
 *
 * @param[in] pool:       the thread pool object pointer
 * @return current alive threads count
 */
uint32_t GetCurAliveThreadCnt(ThreadPool *pool);

GSDB_END_C_CODE_DECLS
#endif /* UTILS_THREAD_POOL_H */
