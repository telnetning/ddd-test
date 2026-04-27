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
 * ---------------------------------------------------------------------------------
 *
 * thread_pool.c
 *
 * Description:
 * 1. The thread pool supports the reuse of created threads to process tasks.
 *
 * ---------------------------------------------------------------------------------
 */

#include <math.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/syscall.h>
#include <sched.h>
#include "types/data_types.h"
#include "container/linked_list.h"
#include "defines/utils_errorcode.h"
#include "schedule/scheduler.h"
#include "schedule/scheduler_internal.h"
#include "schedule/fifo_scheduler.h"
#include "thread/thread_pool_internal.h"
#include "fault_injection/fault_injection.h"
#include "thread/thread_pool.h"

#define DEFAULT_IDLE_TIME_THRESHOLD 10000   /* Unit: millisecond */
#define MAX_IDLE_TIME_THRESHOLD     1000000 /* Unit: millisecond */
/* In scaling mode, when the number of idle worker thread is less than this threshold,
 * the thread pool starts to expand. */
#define DEFAULT_IDLE_WORKER_THRESHOLD 3
#define DEFAULT_EXTENSION_NUM         2
#define USER_THREAD_HIGHEST_PRIORITY  (-20)

#ifdef LOCAL_MODULE_NAME
#undef LOCAL_MODULE_NAME
#endif
#define LOCAL_MODULE_NAME "thread_pool"

THR_LOCAL void *g_tlWorkCtx = NULL;

static void SetThreadLocalWorkCtx(WorkerCtx *workerCtx)
{
    g_tlWorkCtx = workerCtx;
}

static WorkerCtx *GetThreadLocalWorkCtx(void)
{
    return (WorkerCtx *)g_tlWorkCtx;
}

static void *ThreadRunRoutine(void *arg);

static void *TPMemAlloc(MemAllocator *allocator, uint64_t size)
{
    if (allocator != NULL && allocator->alloc != NULL) {
        return allocator->alloc(allocator, size);
    }
    return malloc(size);
}

static void TPMemFree(MemAllocator *allocator, void *ptr)
{
    if (allocator != NULL && allocator->free != NULL) {
        allocator->free(allocator, ptr);
        return;
    }
    free(ptr);
}

static inline bool IsModeFixed(uint32_t flags)
{
    return (flags & THREAD_POOL_FLAG_FIXED) != 0;
}

static inline bool IsModeScaling(uint32_t flags)
{
    return (flags & THREAD_POOL_FLAG_SCALING) != 0;
}

static ErrorCode VerifyParams(const ThreadPoolParams *params)
{
    if (params == NULL) {
        ErrLog(ERROR, ErrMsg("The input params is null."));
        return UTILS_ERROR_THREADPOOL_NULLPARAMETER;
    }
    if (!IsModeFixed(params->flags) && !IsModeScaling(params->flags)) {
        ErrLog(ERROR, ErrMsg("Invalid mode."));
        return UTILS_ERROR_THREADPOOL_PARAMETERERROR;
    }
    if (params->minThrdCnt == 0 || params->maxThrdCnt < params->minThrdCnt) {
        ErrLog(ERROR, ErrMsg("Number of incorrect threads, minThrdCnt: %u, maxThrdCnt: %u.", params->minThrdCnt,
                             params->maxThrdCnt));
        return UTILS_ERROR_THREADPOOL_PARAMETERERROR;
    }
    if (params->stackSize > 0 && params->stackSize < PTHREAD_STACK_MIN) {
        ErrLog(ERROR, ErrMsg("Invalid thread stack size."));
        return UTILS_ERROR_THREADPOOL_PARAMETERERROR;
    }
    if (params->idleTimeThreshold > MAX_IDLE_TIME_THRESHOLD) {
        ErrLog(ERROR, ErrMsg("idle Time Thresholdhold exceed the limit."));
        return UTILS_ERROR_THREADPOOL_PARAMETERERROR;
    }

    return ERROR_SYS_OK;
}

static void WaitingAllThreadsExit(ThreadPool *self)
{
    MutexLock(&self->poolLock);
    /*
     * Releases the thread pool lock, enters the sleep state, waits for all threads to exit, notifies the current
     * thread when the last thread exits, and executes the last resource clearing task in the thread pool.
     */
    while (self->poolRef > 0) {
        ConditionVariableWait(&self->terminatedCond, &self->poolLock);
    }
    MutexUnlock(&self->poolLock);
}

static void DestroyPoolLocks(ThreadPool *self)
{
    MutexDestroy(&self->workerLock);
    MutexDestroy(&self->poolLock);
    MutexDestroy(&self->detectThreadLock);
    ConditionVariableDestroy(&self->terminatedCond);
}

static inline void ThreadPoolCheckName(char *name, size_t size)
{
    // secure check end of '\0', not use strlen, not need the specific length
    for (size_t i = 0; i < size; i++) {
        if (name[i] == '\0') {
            return;
        }
    }
    name[0] = '\0';
}

static void InitPool(ThreadPool *self, const ThreadPoolParams *params, Scheduler *scheduler, MemAllocator *allocator)
{
    self->scheduler = scheduler;
    self->allocator = allocator;
    self->params = *params;
    if ((params->flags & THREAD_POOL_FLAG_SET_TIMEOUT) != 0) {
        self->taskRunTimeThreshold = params->taskRunTimeThres;
    } else {
        self->taskRunTimeThreshold = DEFAULT_DETECT_THRESHOLD;
    }
    self->overloadTimeThreshold = DEFAULT_DETECT_THRESHOLD;
    self->scheduleLatencyThreshold = DEFAULT_DETECT_THRESHOLD;
    if (self->params.idleTimeThreshold == 0) {
        self->params.idleTimeThreshold = DEFAULT_IDLE_TIME_THRESHOLD;
    }
    ThreadPoolCheckName(self->params.name, sizeof(self->params.name));

    MutexInit(&self->workerLock);
    self->idleWorkerCnt = 0;
    self->aliveWorkerCnt = 0;
    DListInit(&self->workerQueue);

    MutexInit(&self->poolLock);
    self->poolRef = 0;
    MutexInit(&self->detectThreadLock);
    ConditionVariableInit(&self->terminatedCond);
    self->poolStatsControl = 0;
    (void)memset_s(self->poolStats, sizeof(self->poolStats), 0, sizeof(self->poolStats));
    self->state = READY;
}

static ErrorCode SetThreadAttr(pthread_attr_t *attr, uint32_t stackSize)
{
    /*
     * Setting thread detachment attributes. In this way, when the thread pool is destroyed or scaled in,
     * does not need to wait for the thread to end and the main thread is not blocked.
     */
    int32_t ret;
    ret = pthread_attr_setdetachstate(attr, PTHREAD_CREATE_DETACHED);
    if (ret != 0) {
        ErrLog(ERROR, ErrMsg("Failed to set detach state."));
        return UTILS_ERROR_THREADPOOL_INITFAILED;
    }
    if (stackSize != 0) {
        ret = pthread_attr_setstacksize(attr, stackSize);
        if (ret != 0) {
            ErrLog(ERROR, ErrMsg("Failed to set stack size."));
            return UTILS_ERROR_THREADPOOL_INITFAILED;
        }
    }

    return ERROR_SYS_OK;
}

static inline PoolState GetPoolState(ThreadPool *self)
{
    return self->state;
}

static void SetTPThreadAffinity(pthread_t threadId, cpu_set_t *cpuset)
{
    if (CPU_COUNT(cpuset) == 0) {
        ErrLog(DEBUG, ErrMsg("CPU set is empty, no need to set affinity."));
        return;
    }

    int err = pthread_setaffinity_np(threadId, sizeof(cpu_set_t), cpuset);
    if (err != 0) {
        ErrLog(ERROR, ErrMsg("Set CPU affinity fail for %s.", strerror(err)));
        return;
    }

    cpu_set_t get;
    CPU_ZERO(&get);
    err = pthread_getaffinity_np(threadId, sizeof(get), &get);
    if (err != 0) {
        ErrLog(ERROR, ErrMsg("Get CPU affinity fail for %s.", strerror(err)));
        return;
    }

    for (int cpuId = 0; cpuId < CPU_SETSIZE; ++cpuId) {
        if (CPU_ISSET((uint32_t)cpuId, &get)) {
            ErrLog(INFO, ErrMsg("ThreadPool thread bind to cpu:%d.", cpuId));
        }
    }
}

static inline void IncPoolRef(ThreadPool *self)
{
    MutexLock(&self->poolLock);
    self->poolRef++;
    MutexUnlock(&self->poolLock);
}

static inline void DecPoolRef(ThreadPool *self)
{
    MutexLock(&self->poolLock);
    self->poolRef--;
    MutexUnlock(&self->poolLock);
}

static ErrorCode ExecuteEntity(pthread_t *thread, uint32_t stackSize, void *(*routine)(void *), void *arg)
{
    ASSERT(thread != NULL);
    ASSERT(routine != NULL);

    pthread_attr_t attr;
    int32_t ret = pthread_attr_init(&attr);
    if (ret != 0) {
        ErrLog(ERROR, ErrMsg("Failed to initialize ThreadAttrInit."));
        return UTILS_ERROR_THREADPOOL_INITFAILED;
    }

    ErrorCode errCode = SetThreadAttr(&attr, stackSize);
    if (errCode != ERROR_SYS_OK) {
        (void)pthread_attr_destroy(&attr);
        return errCode;
    }

    ret = pthread_create(thread, &attr, routine, arg);
    if (ret != 0) {
        ErrLog(ERROR, ErrMsg("Failed to create a new thread. ret = %d", ret));
        (void)pthread_attr_destroy(&attr);
        return UTILS_ERROR_THREADPOOL_INITFAILED;
    }
    ErrLog(INFO, ErrMsg("StartThread success, threadId : %lu.", *thread));
    return ERROR_SYS_OK;
}

static inline void DetachWorker(WorkerCtx *worker)
{
    worker->workRef = 0;
}

#define SLEEP_TIME 1000
static inline void WaitDetachedWorker(WorkerCtx *worker)
{
    while (__atomic_load_n(&worker->workRef, __ATOMIC_RELAXED) != 0) {
        Usleep(SLEEP_TIME); /* sleep 1000us (1ms) */
    }
    ASSERT(worker->workRef == 0);
}

static WorkerCtx *PrepareWorker(ThreadPool *self, bool isInitialWorker, ErrorCode *errCode)
{
    ASSERT(errCode != NULL);
    WorkerCtx *worker = TPMemAlloc(self->allocator, sizeof(WorkerCtx));
    if (worker == NULL) {
        ErrLog(ERROR, ErrMsg("Failed to apply for memory."));
        *errCode = UTILS_ERROR_THREADPOOL_NOMEMORY;
        return NULL;
    }
    (void)memset_s(worker, sizeof(WorkerCtx), 0, sizeof(WorkerCtx));
    worker->routine = ThreadRunRoutine;
    worker->pool = self;
    worker->workRef = 1;
    worker->isInitialWorker = isInitialWorker;

    IncPoolRef(self);
    *errCode = ExecuteEntity(&worker->threadId.tid, self->params.stackSize, ThreadRunRoutine, worker);
    if (*errCode != ERROR_SYS_OK) {
        DecPoolRef(self);
        TPMemFree(self->allocator, worker);
        return NULL;
    }
    SetTPThreadAffinity(worker->threadId.tid, &self->params.cpuset);
    return worker;
}

static void ExpandWorker(ThreadPool *self, uint32_t extendNum)
{
    ErrorCode errCode;
    for (uint32_t i = 0; i < extendNum; i++) {
        /* first simple check with fetch val directly */
        if (self->aliveWorkerCnt >= self->params.maxThrdCnt) {
            return;
        }
        /* then check heavy with atomic add write and fetch */
        uint32_t cnt = __atomic_add_fetch(&self->aliveWorkerCnt, 1, __ATOMIC_RELAXED);
        if (cnt > self->params.maxThrdCnt) {
            cnt = __atomic_sub_fetch(&self->aliveWorkerCnt, 1, __ATOMIC_RELAXED);
            if (cnt < self->params.maxThrdCnt) {
                __atomic_store_n(&self->overLoadStartTime, UINT64_MAX, __ATOMIC_RELAXED); // us
            }
            return;
        }
        if (cnt == self->params.maxThrdCnt) {
            __atomic_store_n(&self->overLoadStartTime, GetMonotonicTimeUs(), __ATOMIC_RELAXED); // us
        }
        WorkerCtx *worker = PrepareWorker(self, false, &errCode);
        if (worker == NULL) {
            return;
        }
        MutexLock(&self->workerLock);
        DListPushTail(&self->workerQueue, &worker->node);
        MutexUnlock(&self->workerLock);
        DetachWorker(worker);
        ErrLog(DEBUG, ErrMsg("Now alive WorkerCtx Cnt: %u.", cnt));
    }
}

static void AddNewWorker(void *context)
{
    ASSERT(context != NULL);
    ThreadPool *pool = ((WorkerCtx *)context)->pool;
    ASSERT(pool != NULL);
    if (GetPoolState(pool) != RUNNING) {
        return;
    }

    if ((__atomic_load_n(&pool->idleWorkerCnt, __ATOMIC_RELAXED) <= DEFAULT_IDLE_WORKER_THRESHOLD) &&
        (__atomic_load_n(&pool->aliveWorkerCnt, __ATOMIC_RELAXED) < pool->params.maxThrdCnt)) {
        SchedulerWakeUpWorker(pool->scheduler);
        ExpandWorker(pool, DEFAULT_EXTENSION_NUM);
    }
}

static void IncIdleWorker(void *context)
{
    ASSERT(context != NULL);
    ThreadPool *pool = ((WorkerCtx *)context)->pool;
    ASSERT(pool != NULL);
    (void)__atomic_add_fetch(&pool->idleWorkerCnt, 1, __ATOMIC_RELAXED);
}

static void DecIdleWorker(void *context)
{
    ASSERT(context != NULL);
    ThreadPool *pool = ((WorkerCtx *)context)->pool;
    ASSERT(pool != NULL);
    (void)__atomic_sub_fetch(&pool->idleWorkerCnt, 1, __ATOMIC_RELAXED);
}

static inline bool IsEnableStats(const ThreadPool *pool)
{
    return ((pool->params.flags & THREAD_POOL_FLAG_STAT) != 0);
}

static void UpdateSchedStatsCallback(void *context, uint64_t startTime)
{
    ASSERT(context != NULL);
    SchedStats *st = &((WorkerCtx *)context)->stats;
    ThreadPool *pool = ((WorkerCtx *)context)->pool;
    ASSERT(pool != NULL);
    if (!IsEnableStats(pool)) {
        /* disabled */
        if (st->count != 0) {
            (void)memset_s(st, sizeof(*st), 0, sizeof(*st)); // clear
        }
        return;
    }
    FAULT_INJECTION_CALL(THREAD_POOL_FLAG_STAT_FAULT_INJECT, NULL);
    uint64_t currentTime = GetMonotonicTimeUs();
    ASSERT(currentTime >= startTime);
    uint64_t lastDelay = currentTime - startTime;
    st->count++;
    if (lastDelay < st->minDelay) {
        st->minDelay = lastDelay;
    }
    if (lastDelay > st->maxDelay) {
        st->maxDelay = lastDelay;
    }
    st->totalDelay += lastDelay;
}

static void TaskPreRunCallback(void *context, void *task, void *func, void *args)
{
    ASSERT(context != NULL);
    ThreadPool *pool = ((WorkerCtx *)context)->pool;
    ASSERT(pool != NULL);
    if (pool->taskRunTimeThreshold != 0) {
        ((WorkerCtx *)context)->runFunc = func;
        ((WorkerCtx *)context)->runArgs = args;
        ((WorkerCtx *)context)->currRun = task;
        uint64_t currTimeUs = GetMonotonicTimeUs();
        __atomic_store_n(&((WorkerCtx *)context)->lastRunTime, currTimeUs, __ATOMIC_RELAXED);
        __atomic_store_n(&((WorkerCtx *)context)->startRunTime, currTimeUs, __ATOMIC_RELAXED);
    } else if (IsEnableStats(pool)) {
        ((WorkerCtx *)context)->startRunTime = GetMonotonicTimeUs();
    }
}

static void TaskPostRunCallback(void *context)
{
    ASSERT(context != NULL);
    uint64_t startRunTime = ((WorkerCtx *)context)->startRunTime;
    if (startRunTime == 0) {
        return;
    }

    /* zero means end task, uint64_t timmstamp overflow at 292277026596-12-4 15:30:08 */
    __atomic_store_n(&((WorkerCtx *)context)->startRunTime, 0, __ATOMIC_RELAXED);
    __atomic_store_n(&((WorkerCtx *)context)->lastRunTime, 0, __ATOMIC_RELAXED);

    SchedStats *st = &((WorkerCtx *)context)->stats;
    if (st->count == 0) {
        /* no recored SchedStats */
        return;
    }
    FAULT_INJECTION_CALL(THREAD_POOL_FLAG_STAT_FAULT_INJECT, NULL);
    uint64_t currentTime = GetMonotonicTimeUs();
    /* schedule statistics */
    ASSERT(currentTime >= startRunTime);
    uint64_t taskRunningTime = currentTime - startRunTime;
    st->taskRunningTime += taskRunningTime;
    if (taskRunningTime > st->taskRunningMaxTime) {
        st->taskRunningMaxTime = taskRunningTime;
    }
    if (currentTime - st->lastShowStatTime > ONE_MINUTE_TO_US) {
        ThreadPool *pool = ((WorkerCtx *)context)->pool;
        ASSERT(pool != NULL);
        ErrLog(LOG, ErrMsg("[Thread pool] %s-%ld schedstat: count %zd, sched delay(us) avg/min/max/sum = "
                           "%zu/%zu/%zu/%zu, task running(us) avg/max = %zu/%zu",
                           pool->params.name, ((WorkerCtx *)context)->tid, st->count, st->totalDelay / st->count,
                           st->minDelay, st->maxDelay, st->totalDelay, st->taskRunningTime / st->count,
                           st->taskRunningMaxTime));
        st->minDelay = (uint64_t)-1;
        st->maxDelay = 0;
        st->totalDelay = 0;
        st->taskRunningTime = 0;
        st->taskRunningMaxTime = 0;
        st->lastShowStatTime = currentTime;
        st->count = 0;
    }
}

static inline void InitScheduleCallback(ScheduleCallback *callback, WorkerCtx *ctx, bool scaling)
{
    callback->context = (void *)ctx;
    if (scaling) {
        callback->newWorkerCallback = AddNewWorker;
        callback->incIdleWorkerCallback = IncIdleWorker;
        callback->decIdleWorkerCallback = DecIdleWorker;
        callback->idleTimeThreshold = ctx->pool->params.idleTimeThreshold;
    }
    callback->updateSchedStatsCallback = UpdateSchedStatsCallback;
    callback->taskPreRunCallback = TaskPreRunCallback;
    callback->taskPostRunCallback = TaskPostRunCallback;
}

static void DecAliveWorkerCnt(ThreadPool *self)
{
    uint32_t cnt = __atomic_sub_fetch(&self->aliveWorkerCnt, 1, __ATOMIC_RELAXED);
    if (cnt < self->params.maxThrdCnt) {
        __atomic_store_n(&self->overLoadStartTime, UINT64_MAX, __ATOMIC_RELAXED); // us
    }
}

static inline void MigratePoolState(ThreadPool *self, PoolState state)
{
    MutexLock(&self->poolLock);
    self->state = state;
    /* only reuse the condition variable */
    ConditionVariableBroadcast(&self->terminatedCond);
    MutexUnlock(&self->poolLock);
}

static inline void WaitThreadPoolReadyFinish(ThreadPool *pool)
{
    MutexLock(&pool->poolLock);
    while (GetPoolState(pool) == READY) {
        /* only reuse the condition variable */
        ConditionVariableWait(&pool->terminatedCond, &pool->poolLock);
    }
    MutexUnlock(&pool->poolLock);
    ErrLog(DEBUG, ErrMsg("Thread wait start finish, threadID : %lu", GetCurrentTid().tid));
}

static void WorkerExit(ThreadPool *pool, WorkerCtx *worker)
{
    // need wait detach worker reference in thread pool quite scene
    WaitDetachedWorker(worker);
    MutexLock(&pool->workerLock);
    DListDelete(&worker->node); // delete worker from pool's workerQueue
    MutexUnlock(&pool->workerLock);
    SetThreadLocalWorkCtx(NULL);
    TPMemFree(pool->allocator, worker); // release memory

    if (pool->params.threadExitCallback != NULL) {
        pool->params.threadExitCallback();
    }
    MutexLock(&pool->poolLock);
    ErrLog(INFO, ErrMsg("Thread exit, threadID : %lu, poolRef: %u.", GetCurrentTid().tid, pool->poolRef));
    pool->poolRef--;
    if (GetPoolState(pool) != RUNNING && pool->poolRef == 0) {
        ConditionVariableSignal(&pool->terminatedCond);
    }
    MutexUnlock(&pool->poolLock);
    CloseLogger();
}

static bool IsThreadPoolShrink(ThreadPool *pool)
{
    ASSERT(pool != NULL);
    uint32_t oldAliveWorkerCnt = __atomic_load_n(&pool->aliveWorkerCnt, __ATOMIC_RELAXED);
    if (oldAliveWorkerCnt <= pool->params.minThrdCnt) {
        /* keep the params.minThrdCnt quantity threads in running */
        return false;
    }
    /* Reserve an idle thread for expanding new threads. */
    if (pool->idleWorkerCnt <= 1) {
        return false;
    }
    while (!__atomic_compare_exchange_n(&pool->aliveWorkerCnt, &oldAliveWorkerCnt, oldAliveWorkerCnt - 1, true,
                                        __ATOMIC_RELAXED, __ATOMIC_RELAXED)) {
        if (oldAliveWorkerCnt <= pool->params.minThrdCnt) {
            /* ok, we already only params.minThrdCnt quantity threads in running, can't shrink a thread */
            return false;
        }
    }
    /* if shrink success, the pool->aliveWorkerCnt must be less then pool->params.maxThrdCnt,
     * so clear the overLoad status */
    __atomic_store_n(&pool->overLoadStartTime, UINT64_MAX, __ATOMIC_RELAXED); // us
    return true;
}

static void *ThreadRunRoutine(void *arg)
{
    WorkerCtx *worker = (WorkerCtx *)arg;
    ThreadPool *pool = (ThreadPool *)worker->pool;
#if defined __linux__ || defined linux
    worker->tid = syscall(SYS_gettid);
#endif
    bool isInitialWorker = worker->isInitialWorker;
    bool exitThread = false;

    SetThreadLocalWorkCtx(worker);

    if (pool->params.threadEnterCallback != NULL) {
        pool->params.threadEnterCallback();
    }

    ScheduleCallback callback = {};
    /* Threads need to be scaled in or out based on the thread pool status only in scaling mode. */
    if (IsModeScaling(pool->params.flags)) {
        InitScheduleCallback(&callback, worker, true);
        /* If the thread is an initial thread, the extension of other threads does not need to be considered
         * when the thread is started. If the thread is an extension thread, the extension of other threads
         * needs to be considered. */
        if (!isInitialWorker) {
            AddNewWorker((void *)worker);
        }
    } else {
        InitScheduleCallback(&callback, worker, false);
    }

    if ((pool->params.flags & THREAD_POOL_FLAG_REALTIME) != 0) {
        FAULT_INJECTION_CALL_REPLACE(THREAD_POOL_FLAG_REALTIME_FAULT_INJECT, 0);
        /* set current thread nice -20 to up priority */
        errno = 0;
        if ((nice(USER_THREAD_HIGHEST_PRIORITY) == -1) && (errno != 0)) {
            ErrLog(DEBUG, ErrMsg("Thread change to priority fail, errno: %d, threadID : %lu, poolRef: %u.", errno,
                                 GetCurrentTid().tid, pool->poolRef));
        }
        FAULT_INJECTION_CALL_REPLACE_END;
    }

    if (pool->params.name[0] != '\0') {
        if (pthread_setname_np(pthread_self(), pool->params.name) != 0) {
            ErrLog(DEBUG, ErrMsg("Thread pool set thread name  %s fail.", pool->params.name));
        }
    }

    WaitThreadPoolReadyFinish(pool);
    exitThread = (GetPoolState(pool) != RUNNING);

    FAULT_INJECTION_WAIT(THREAD_POOL_GET_TASK_FAULT_INJECT);
    FAULT_INJECTION_NOTIFY(THREAD_POOL_GET_TASK_FAULT_INJECT);

    while (!exitThread) {
        /* The scheduler schedules and executes tasks. */
        int32_t scheduleState = ScheduleTask(pool->scheduler, &callback);
        if (scheduleState == SCHEDULE_STATE_OK) {
            if (GetPoolState(pool) == RUNNING) {
                exitThread = false;
                continue;
            } else {
                DecAliveWorkerCnt(pool);
                exitThread = true;
                continue;
            }
        } else if (scheduleState == SCHEDULE_STATE_TIMEOUT) {
            if (!IsModeScaling(pool->params.flags)) {
                exitThread = false;
                continue;
            }
            if (IsThreadPoolShrink(pool)) {
                ErrLog(INFO, ErrMsg("The thread timeout need to exit, threadId : %lu.", pthread_self()));
                exitThread = true;
                continue;
            }
        } else if (scheduleState == SCHEDULE_STATE_SHRINK) {
            DecAliveWorkerCnt(pool);
            exitThread = true;
            ErrLog(INFO, ErrMsg("The thread driving exit, threadId : %lu.", pthread_self()));
        } else {
            DecAliveWorkerCnt(pool);
            exitThread = true;
        }
    }
    WorkerExit(pool, worker);
    return NULL;
}

static inline uint64_t TimeSecToMs(uint16_t sec)
{
    FAULT_INJECTION_ACTION(THREAD_POOL_DETECT_FAULT_INJECT, return sec);
    return (uint64_t)sec * TIME_CONVERSION_FACTOR;
}

static inline void DetectExit(ThreadPool *pool)
{
    if ((pool->params.flags & THREAD_POOL_FLAG_DETECT_EXIT) != 0) {
        /**
         * The possible cause is that the working thread lasts for a long time due to service exceptions.
         * The process is exited and the service can be restarted for quick recovery.
         */
        ASSERT(0);
        _exit(1);
    }
}

static void DetectTaskRun(ThreadPool *pool, uint64_t now)
{
    DListMutableIter iter;
    DListHead *head = &pool->workerQueue;
    MutexLock(&pool->workerLock);
    DLIST_MODIFY_FOR_EACH(iter, head)
    {
        WorkerCtx *worker = DLIST_CONTAINER(WorkerCtx, node, iter.cur);
        ASSERT(worker != NULL);
#ifdef ENABLE_FAULT_INJECTION
        void *args = worker->runArgs;
#endif

        uint64_t lastRunTime = __atomic_load_n(&worker->lastRunTime, __ATOMIC_RELAXED);
        if ((lastRunTime == 0) || (now < lastRunTime)) {
            continue;
        }

        uint64_t deta = now - lastRunTime; // us
        uint64_t runTimeThreshold = TimeSecToMs(pool->taskRunTimeThreshold) * TIME_CONVERSION_FACTOR;
        uint64_t timeoutTimes = deta / runTimeThreshold;
        if (timeoutTimes >= RUN_CRITICAL_TIMEOUT_TIME) {
            ErrLog(WARNING, ErrMsg("[Thead pool]name:%s Task run critical timeout, lastRunTime:%lu, "
                                   "currentTime:%lu, runTimeThreshold:%hu, detectCnt:%lu, defaultCnt:%d.",
                                   pool->params.name, lastRunTime, now, pool->scheduleLatencyThreshold, timeoutTimes,
                                   RUN_CRITICAL_TIMEOUT_TIME));
            FAULT_INJECTION_CALL(THREAD_POOL_DETECT_TASK_RUN_CRITICAL_FAULT_INJECT, lastRunTime, args,
                                 &worker->lastRunTime);
            DetectExit(pool);
        }
        if (deta > runTimeThreshold) {
            ErrLog(WARNING, ErrMsg("[Thread pool]name:%s-%ld Task run timeout %ds", pool->params.name, worker->tid,
                                   pool->taskRunTimeThreshold));
            FAULT_INJECTION_CALL(THREAD_POOL_DETECT_TASK_RUN_FAULT_INJECT, lastRunTime, args, &worker->lastRunTime);
            FAULT_INJECTION_CALL(THREAD_POOL_SKIP_DETECT_TASK_RUN_FAULT_INJECT, args);
        }
    }
    MutexUnlock(&pool->workerLock);
}

static inline void DetectOverload(ThreadPool *pool, uint64_t now)
{
    uint64_t overLoadStartTime = __atomic_load_n(&pool->overLoadStartTime, __ATOMIC_RELAXED); // us
    bool overload = ((now > overLoadStartTime) &&
                     ((now - overLoadStartTime) > TimeSecToMs(pool->overloadTimeThreshold) * TIME_CONVERSION_FACTOR));

    FAULT_INJECTION_CALL(THREAD_POOL_DETECT_OVERLOAD_FAULT_INJECT, overload);

    if (overload) {
        ErrLog(WARNING, ErrMsg("[Thead pool]name:%s %u threads overload continuous in %zds", pool->params.name,
                               pool->aliveWorkerCnt,
                               ((now - overLoadStartTime) / (TIME_CONVERSION_FACTOR * TIME_CONVERSION_FACTOR))));
    }
}

static inline void DetectScheduleLatency(ThreadPool *pool, uint64_t now)
{
    uint64_t earliestTaskSubmitTime = SchedulerGetEarliestTaskTime(pool->scheduler);
    uint64_t latency = now > earliestTaskSubmitTime ? now - earliestTaskSubmitTime : 0; // us
    bool needWarn = (latency >= TimeSecToMs(pool->scheduleLatencyThreshold) * TIME_CONVERSION_FACTOR);
    if (unlikely(needWarn)) {
        ErrLog(WARNING, ErrMsg("[Thead pool]name:%s detected task schedule latency %zu(us) (>%ds)", pool->params.name,
                               latency, pool->scheduleLatencyThreshold));
    }
    FAULT_INJECTION_CALL(THREAD_POOL_DETECT_SCHED_LATENCY_FAULT_INJECT, needWarn);
}

static inline uint16_t ClcDetectPeriod(uint16_t t1, uint16_t t2)
{
    uint16_t min = Min(t1, t2);
    uint16_t max = Max(t1, t2);
    // calculate the greatest common divisor by euclidean division algorithm
    while (min != 0) {
        uint16_t temp = max % min;
        max = min;
        min = temp;
    }
    // t1 && t2 all zero, return UINT16_MAX
    uint16_t res = max == 0 ? UINT16_MAX : max;
    FAULT_INJECTION_CALL(THREAD_POOL_DETECT_PERIOD_FAULT_INJECT, t1, t2, res);
    // t1 && t2 all zero, return UINT16_MAX
    return res;
}

static inline void YieldDetectThread(ThreadPool *pool)
{
    Mutex *lock = &pool->detectThreadLock;
    MutexLock(lock);
    if (GetPoolState(pool) == RUNNING) {
        uint16_t sec = ClcDetectPeriod(pool->taskRunTimeThreshold, pool->overloadTimeThreshold);
        sec = ClcDetectPeriod(sec, pool->scheduleLatencyThreshold);
        // always timeout
        (void)ConditionVariableTimedWait(&pool->terminatedCond, lock, (time_t)(TimeSecToMs(sec)));
    }
    MutexUnlock(lock);
}

/* strLen is include '\0' */
static void SetThreadNameEndWith(char *dest, size_t destMax, const char *appendStr, size_t strLen)
{
    size_t last = destMax - strLen;
    size_t nameLen = strlen(dest);
    size_t offset = Min(nameLen, last);
    // (destMax - offset) >= strLen && < 16, memcpy_s always success
    if (memcpy_s(&dest[offset], destMax - offset, appendStr, strLen) != EOK) {
        return;
    }
    (void)pthread_setname_np(pthread_self(), dest);
}

static void *TheadPoolDetectRoutine(void *arg)
{
    ASSERT(arg != NULL);
    ThreadPool *pool = (ThreadPool *)arg;

    char name[THREAD_NAME_LEN] = {0};
    if (pool->params.name[0] == '\0') {
        (void)pthread_getname_np(pthread_self(), name, sizeof(name));
    } else {
        (void)strncpy_s(name, sizeof(name), pool->params.name, sizeof(name) - 1);
    }
    SetThreadNameEndWith(name, sizeof(name), "/d", sizeof("/d")); // set the name end with /d as threadpool daemon
    WaitThreadPoolReadyFinish(pool);

    while (GetPoolState(pool) == RUNNING) {
        uint64_t now = GetMonotonicTimeUs();
        if (pool->taskRunTimeThreshold) {
            DetectTaskRun(pool, now);
        }
        if (pool->overloadTimeThreshold) {
            DetectOverload(pool, now);
        }
        if (pool->scheduleLatencyThreshold) {
            DetectScheduleLatency(pool, now);
        }
        YieldDetectThread(pool);
    }
    MutexLock(&pool->poolLock);
    ErrLog(INFO, ErrMsg("[Thead pool] Detect Thread exit, poolRef: %u.", pool->poolRef));
    pool->poolRef--;
    if (pool->poolRef == 0) {
        ConditionVariableSignal(&pool->terminatedCond);
    }
    MutexUnlock(&pool->poolLock);
    CloseLogger();
    return NULL;
}

static ErrorCode StartThreadPool(ThreadPool *self)
{
    ErrorCode errCode = ERROR_SYS_OK;
    uint32_t startCnt = 0;
    while (startCnt < self->params.minThrdCnt) {
        WorkerCtx *worker = PrepareWorker(self, true, &errCode);
        if (worker == NULL) {
            return errCode;
        }
        MutexLock(&self->workerLock);
        /* add the worker to the workerQueue. */
        DListPushTail(&self->workerQueue, &worker->node);
        MutexUnlock(&self->workerLock);
        DetachWorker(worker);
        uint32_t cnt = __atomic_add_fetch(&self->aliveWorkerCnt, 1, __ATOMIC_RELAXED);
        if (cnt == self->params.maxThrdCnt) {
            __atomic_store_n(&self->overLoadStartTime, GetMonotonicTimeUs(), __ATOMIC_RELAXED); // us
        }
        startCnt++;
    }
    if ((self->params.flags & THREAD_POOL_FLAG_ENABLE_DETECT) != 0) {
        pthread_t thread;
        IncPoolRef(self);
        /* execute runtime detect routine entity */
        errCode = ExecuteEntity(&thread, self->params.stackSize, TheadPoolDetectRoutine, self);
        if (errCode != ERROR_SYS_OK) {
            DecPoolRef(self);
            return errCode;
        }
        ErrLog(DEBUG, ErrMsg("Start thread pool detect success."));
    }
    /* only used in UT, simulate create a threads appear to error */
    FAULT_INJECTION_ACTION(START_THREADPOOL_FAULT_INJECTION, return UTILS_ERROR_THREADPOOL_NOMEMORY);

    return ERROR_SYS_OK;
}

static ThreadPool *CreateAndStartThreadPool(const ThreadPoolParams *params, Scheduler *scheduler,
                                            MemAllocator *allocator, ErrorCode *errCode)
{
    void *oriPoolPtr = TPMemAlloc(allocator, sizeof(ThreadPool) + GS_CACHE_LINE_SIZE);
    if (oriPoolPtr == NULL) {
        ErrLog(ERROR, ErrMsg("Failed to apply for memory."));
        *errCode = UTILS_ERROR_THREADPOOL_NOMEMORY;
        return NULL;
    }

    uint32_t paddingLen = GS_CACHE_LINE_SIZE - ((uint32_t)(uintptr_t)oriPoolPtr % GS_CACHE_LINE_SIZE);
    ThreadPool *pool = (ThreadPool *)(oriPoolPtr + paddingLen);
    pool->oriPtr = oriPoolPtr;

    InitPool(pool, params, scheduler, allocator);

    *errCode = StartThreadPool(pool);
    if (*errCode != ERROR_SYS_OK) {
        ErrLog(ERROR, ErrMsg("Start thread pool failed."));
        /* start threadpool fail, shutdown and clear threadpool resource */
        MigratePoolState(pool, SHUTDOWN);
        /* Block waiting for all threads to exit. */
        WaitingAllThreadsExit(pool);
        DestroyPoolLocks(pool);
        TPMemFree(pool->allocator, pool->oriPtr);
        return NULL;
    }

    MigratePoolState(pool, RUNNING);
    ErrLog(INFO, ErrMsg("Create thread pool success."));
    return pool;
}

UTILS_EXPORT ThreadPool *CreateThreadPool(const ThreadPoolParams *params, MemAllocator *allocator, ErrorCode *code)
{
    *code = VerifyParams(params);
    if (*code != ERROR_SYS_OK) {
        ErrLog(ERROR, ErrMsg("Parameter verification failed."));
        return NULL;
    }

    SchedulerInitParams initParam = {.allocator = allocator, .maxQueueSize = params->maxQueueSize};
    Scheduler *scheduler = (Scheduler *)(void *)NewFIFOScheduler((TypeInitParams *)(void *)(&initParam), code);
    if (*code != ERROR_SYS_OK) {
        ErrLog(ERROR, ErrMsg("NewFIFOScheduler failed."));
        return NULL;
    }

    ThreadPool *pool = CreateAndStartThreadPool(params, scheduler, allocator, code);
    if (pool == NULL) {
        SchedulerDestroy(scheduler);
        RefObjectKillAndDecRef(UP_TYPE_CAST(scheduler, RefObject));
    }
    return pool;
}

UTILS_EXPORT ThreadPool *CreateThreadPoolEX(const ThreadPoolParams *params, Scheduler *scheduler,
                                            MemAllocator *allocator, ErrorCode *code)
{
    *code = VerifyParams(params);
    if (*code != ERROR_SYS_OK) {
        ErrLog(ERROR, ErrMsg("Parameter verification failed."));
        return NULL;
    }

    if (scheduler == NULL) {
        ErrLog(INFO, ErrMsg("the scheduler is NULL."));
        *code = UTILS_ERROR_THREADPOOL_NULLPARAMETER;
        return NULL;
    }

    return CreateAndStartThreadPool(params, scheduler, allocator, code);
}

/* 63 bit time max (INT64_MAX) is enough, and the bit0 is used for index of poolStats to record statistics */
#define POOL_STATS_LAST_SHOW_TIME_MASK (~((uint64_t)0b1 << 63))

static void ThreadPoolUpdateSubmitStats(ThreadPool *pool, uint64_t submitStartTime)
{
    ASSERT(pool != NULL);
    ASSERT(submitStartTime != 0);

    bool wrapAround = false;
    bool needShow = false;
    uint64_t submitFinishTime = GetMonotonicTimeUs();
    /* the variable will be optimize by compiler, so it ok for fault inject to change showPeriod */
    uint64_t showPeriod = ONE_MINUTE_TO_US;
    /* fault inject change to 50 ms */
    FAULT_INJECTION_ACTION(THREADPOOL_SUBMIT_LATENCY_FAULT_INJECTION, (showPeriod = 50 * TIME_CONVERSION_FACTOR));
    uint64_t poolStatsControl = pool->poolStatsControl;
    /* in poolStatsControl, the last bit is statsIdx and the high 63 bits is lastShowTime */
    uint32_t statsIdx = poolStatsControl & 0x1;
    /* firstly, check show time */
    uint64_t lastShowTime = ((poolStatsControl >> 1) & POOL_STATS_LAST_SHOW_TIME_MASK);
    if (submitFinishTime - lastShowTime > showPeriod) {
        /* we found it't time to show submit latency, exchange the stats->lastShowTime and show it */
        uint64_t desire = (submitFinishTime << 1) | ((poolStatsControl + 1) & 0x1);
        if (__atomic_compare_exchange_n(&pool->poolStatsControl, &poolStatsControl, desire, true, __ATOMIC_ACQUIRE,
                                        __ATOMIC_ACQUIRE)) {
            /* check pool->poolStatsControl will not change soon after we exchange success, but in some rare case,
             * such as, this thread schedule switch by os over (POOL_STATS_LAST_SHOW_TIME_MASK) 1 minutes after exchange
             * pool->poolStatsControl, will wrap around the statsIdx and the stats data became not trustworthy */
            if (unlikely((desire & 0x1) != (pool->poolStatsControl & 0x1))) {
                wrapAround = true;
            }
            /* exchange success, show stats by us */
            needShow = true;
        } else {
            /* exchange fail, some one other had change it. update statsIdx to new one, because in ONE_MINUTE_TO_US
               will not change it */
            statsIdx = poolStatsControl & 0x1;
        }
    }
    ThreadPoolStats *stats = &pool->poolStats[statsIdx];
    uint64_t currentLatency = submitFinishTime - submitStartTime;
    if (needShow) {
        const char *wrapAroundStr = (wrapAround ? "[dirty]" : "");
        /* record errlog */
        FAULT_INJECTION_CALL(THREADPOOL_SUBMIT_COUNT_STATS_FAULT_INJECTION, 0);
        ErrLog(LOG, ErrMsg("%s[Thread pool] %s poolstat: submit latency(us) avg/max/count = %zu/%zu/%zu", wrapAroundStr,
                           pool->params.name, (stats->submitTotalLatency + currentLatency) / (stats->submitCount + 1),
                           stats->submitMaxLatency > currentLatency ? stats->submitMaxLatency : currentLatency,
                           stats->submitCount + 1));
        /* clear old data */
        stats->submitMaxLatency = 0;
        stats->submitTotalLatency = 0;
        stats->submitCount = 0;
    } else {
        /* those data must be matched */
        /* here no lock, but we changing idx is atomic, so will only miss one of stats data at that moment */
        SetMaxValueSafety(&(stats->submitMaxLatency), currentLatency);
        (void)__atomic_add_fetch(&(stats->submitTotalLatency), currentLatency, __ATOMIC_RELAXED);
        (void)__atomic_add_fetch(&(stats->submitCount), 1, __ATOMIC_RELAXED);
    }
}

static ErrorCode DoScheduleTask(ThreadPool *pool, ThreadPoolTask *task, TaskId *taskId, bool emergency)
{
    if (pool == NULL) {
        ErrLog(ERROR, ErrMsg("Null Parameter."));
        return UTILS_ERROR_THREADPOOL_NULLPARAMETER;
    }
    if (task == NULL || (task->routine == NULL && task->run == NULL)) {
        ErrLog(ERROR, ErrMsg("Error Task."));
        return UTILS_ERROR_THREADPOOL_ERRTASK;
    }
    if (GetPoolState(pool) != RUNNING) {
        ErrLog(INFO, ErrMsg("The thread pool state is SHUTDOWN."));
        return UTILS_ERROR_THREADPOOL_ERRSTATE;
    }

    uint64_t submitStartTime = 0;
    bool enableStats = IsEnableStats(pool);
    if (enableStats) {
        submitStartTime = GetMonotonicTimeUs();
    }

    ErrorCode errCode = SchedulerSubmitTask(pool->scheduler, task, taskId, emergency);
    if (errCode != ERROR_SYS_OK) {
        return errCode;
    }

    if (enableStats) {
        ThreadPoolUpdateSubmitStats(pool, submitStartTime);
    }

    return ERROR_SYS_OK;
}

UTILS_EXPORT ErrorCode ScheduleThreadPoolTask(ThreadPool *pool, ThreadPoolTask *task, TaskId *taskId)
{
    return DoScheduleTask(pool, task, taskId, false);
}

UTILS_EXPORT ErrorCode ScheduleThreadPoolTaskEmgc(ThreadPool *pool, ThreadPoolTask *task, TaskId *taskId)
{
    return DoScheduleTask(pool, task, taskId, true);
}

UTILS_EXPORT ErrorCode CancelThreadPoolTask(ThreadPool *pool, TaskId taskId)
{
    if (pool == NULL) {
        ErrLog(ERROR, ErrMsg("The pool is NULL."));
        return UTILS_ERROR_THREADPOOL_NULLPARAMETER;
    }

    ErrorCode errCode = SchedulerCancelTask(pool->scheduler, taskId);
    if (errCode != ERROR_SYS_OK) {
        return errCode;
    }

    return ERROR_SYS_OK;
}

UTILS_EXPORT void NotifyThreadPoolTaskAlive(void)
{
    WorkerCtx *workerCtx = GetThreadLocalWorkCtx();
    if (unlikely(workerCtx == NULL)) {
        ErrLog(ERROR, ErrMsg("Current thread workCtx is NULL."));
        return;
    }
    __atomic_store_n(&workerCtx->lastRunTime, GetMonotonicTimeUs(), __ATOMIC_RELAXED);
}

UTILS_EXPORT void DestroyThreadPool(ThreadPool *pool)
{
    if (pool == NULL) {
        ErrLog(ERROR, ErrMsg("The pool is NULL."));
        return;
    }
    ErrLog(INFO, ErrMsg("Start to destroy thread pool."));
    /*
     * Set the thread pool state to SHUTDOWN and notify all threads, All threads stop executing new tasks,
     * but the ongoing tasks are not affected. and the thread pool cannot accept new tasks.
     */
    MutexLock(&pool->detectThreadLock);
    pool->state = SHUTDOWN;
    /* no need change state with lock poolLock, only wirte SHUTDOWN happen before WaitingAllThreadsExit is ok */
    ConditionVariableSignal(&pool->terminatedCond); /* signal to exit detect thread if it on run */
    MutexUnlock(&pool->detectThreadLock);

    /*
     * Destroy the Scheduler and cancel the executing tasks and the waiting tasks.
     */
    SchedulerDestroy(pool->scheduler);

    /* Block waiting for all threads to exit. */
    WaitingAllThreadsExit(pool); // wait in terminatedCond

    /* Destroy Scheduler(Clearing the Task Queues That Are Being Executed and Unexecuted) */
    RefObjectKillAndDecRef(UP_TYPE_CAST(pool->scheduler, RefObject));

    DestroyPoolLocks(pool);
    TPMemFree(pool->allocator, pool->oriPtr);
    ErrLog(INFO, ErrMsg("Destroy thread pool done."));
}

UTILS_EXPORT void SetThreadPoolCpuset(ThreadPool *pool, const cpu_set_t *set)
{
    if ((pool == NULL) || (set == NULL) || (CPU_EQUAL(set, &pool->params.cpuset)) || (pool->state == SHUTDOWN)) {
        return;
    }
    pool->params.cpuset = *set; // modify params

    DListMutableIter iter = {};
    DListHead *head = &pool->workerQueue;

    MutexLock(&pool->workerLock);
    DLIST_MODIFY_FOR_EACH(iter, head)
    {
        WorkerCtx *worker = DLIST_CONTAINER(WorkerCtx, node, iter.cur);
        ASSERT(worker != NULL);
        SetTPThreadAffinity(worker->threadId.tid, &pool->params.cpuset);
    }
    MutexUnlock(&pool->workerLock);
}

UTILS_EXPORT void SetThreadPoolStatstics(ThreadPool *pool, bool enable)
{
    if ((pool != NULL) && (pool->state != SHUTDOWN)) {
        /* log2(<constant literal>) will float optimize to constant literal, here is 31 in release */
        pool->params.flags = (pool->params.flags & ~(THREAD_POOL_FLAG_STAT)) |
                             ((uint32_t)enable << (uint32_t)log2(THREAD_POOL_FLAG_STAT));
    }
}

UTILS_EXPORT void SetDetectTaskRunThreshold(ThreadPool *pool, uint16_t threshold)
{
    if ((pool != NULL) && (pool->state != SHUTDOWN)) {
        MutexLock(&pool->detectThreadLock);
        pool->taskRunTimeThreshold = threshold;
        ConditionVariableSignal(&pool->terminatedCond);
        MutexUnlock(&pool->detectThreadLock);
    }
}

UTILS_EXPORT void SetDetectOverloadThreshold(ThreadPool *pool, uint16_t threshold)
{
    if ((pool != NULL) && (pool->state != SHUTDOWN)) {
        MutexLock(&pool->detectThreadLock);
        pool->overloadTimeThreshold = threshold;
        ConditionVariableSignal(&pool->terminatedCond);
        MutexUnlock(&pool->detectThreadLock);
    }
}

UTILS_EXPORT void SetDetectScheduleLatencyThreshold(ThreadPool *pool, uint16_t threshold)
{
    if ((pool != NULL) && (pool->state != SHUTDOWN)) {
        MutexLock(&pool->detectThreadLock);
        pool->scheduleLatencyThreshold = threshold;
        ConditionVariableSignal(&pool->terminatedCond);
        MutexUnlock(&pool->detectThreadLock);
    }
}

static void *ShrinkWorkThreadTask(SYMBOL_UNUSED void *args)
{
    ErrLog(DEBUG, ErrMsg("thread pool proactively shrinks one thread"));
    return (void *)(intptr_t)SCHEDULE_STATE_SHRINK;
}

static inline void ShrinkWorker(ThreadPool *self, uint32_t num)
{
    for (uint32_t i = num; i > 0; i--) {
        ThreadPoolTask task = {ShrinkWorkThreadTask, NULL, NULL, NULL, NULL};
        TaskId taskId = THREAD_POOL_INVALID_TASK_ID;
        ErrorCode errCode = ScheduleThreadPoolTask(self, &task, &taskId);
        ErrLog(DEBUG, ErrMsg("thread pool proactively shrinks one thread, %lld", errCode));
    }
}

UTILS_EXPORT void SetThreadPoolThreadCnt(ThreadPool *pool, uint32_t minThrdCnt, uint32_t maxThrdCnt)
{
    if ((pool != NULL) && (pool->state != SHUTDOWN)) {
        uint32_t expand = 0;
        uint32_t shrink = 0;
        if ((minThrdCnt != 0) && ((minThrdCnt <= maxThrdCnt) || (minThrdCnt <= pool->params.maxThrdCnt))) {
            /* minThrdCnt must less equal than maxThrdCnt will set, or less equal than old params.maxThrdCnt */
            if (minThrdCnt > pool->params.minThrdCnt) {
                expand = minThrdCnt - pool->params.minThrdCnt;
            }
            pool->params.minThrdCnt = minThrdCnt;
        }
        if (maxThrdCnt >= pool->params.minThrdCnt) {
            pool->params.maxThrdCnt = maxThrdCnt;
            if (pool->aliveWorkerCnt > pool->params.maxThrdCnt) {
                shrink = pool->aliveWorkerCnt - pool->params.maxThrdCnt;
            }
        }
        if (shrink > 0) {
            ShrinkWorker(pool, shrink);
        }
        if (expand > 0) {
            ExpandWorker(pool, expand);
        }
    }
}

/**
 * Obtains the number of alive threads in the thread pool.
 */
UTILS_EXPORT uint32_t GetCurAliveThreadCnt(ThreadPool *self)
{
    uint32_t threadCnt = 0;
    MutexLock(&self->poolLock);
    threadCnt = self->poolRef;
    MutexUnlock(&self->poolLock);
    return threadCnt;
}
