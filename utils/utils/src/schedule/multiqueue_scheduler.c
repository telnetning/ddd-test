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
 * multiqueue_scheduler.c
 *
 * Description:
 * the implementation of the task scheduler public interface.
 * ---------------------------------------------------------------------------------
 */

#include "schedule/scheduler.h"
#include "fault_injection/fault_injection.h"
#include "schedule/scheduler_internal.h"
#include "schedule/multiqueue_scheduler.h"

#define DEFAULT_THREAD_WAIT_TIME 3000 /* unit: ms */
#define MAX_QUEUE_COUNT          100
#define MAX_TASK_QUEUE_SIZE      10000000

#ifdef LOCAL_MODULE_NAME
#undef LOCAL_MODULE_NAME
#endif
#define LOCAL_MODULE_NAME "thread_pool"

typedef struct TaskEntry TaskEntry;
struct TaskEntry {
    DListNode node;
    ThreadPoolTask task;
    uint64_t enqueueTime;
    bool highPriority;
    void *oriPtr;
} __attribute__((aligned(GS_CACHE_LINE_SIZE)));

static void *Run(TaskEntry *self)
{
    if (self->task.routine) {
        return self->task.routine(self->task.args);
    }
    if (self->task.run) {
        return self->task.run(&self->task.args);
    }
    return NULL;
}

static void Finalize(TaskEntry *self)
{
    if (self->task.finalize) {
        self->task.finalize(self->task.args);
    }
}

static void *MQSchedulerMemAlloc(MultiQueueScheduler *self, uint64_t size)
{
    if (self->allocator != NULL && self->allocator->alloc != NULL) {
        return self->allocator->alloc(self->allocator, size);
    }
    return malloc(size);
}

static void MQSchedulerMemFree(MultiQueueScheduler *self, void *ptr)
{
    if (self->allocator != NULL && self->allocator->free != NULL) {
        self->allocator->free(self->allocator, ptr);
        return;
    }
    free(ptr);
}

static inline uint32_t GetConsumeQueueIndex(const MultiQueueScheduler *self, uint32_t startQueueId, uint32_t retryCnt)
{
    return (startQueueId + retryCnt) % self->taskQueueCnt;
}

static inline void UpdateLastConsumeQueueIndex(MultiQueueScheduler *self)
{
    self->lastConsumeQueueIndex++;
    self->lastConsumeQueueIndex %= self->taskQueueCnt;
}

static TaskEntry *GetHighPriorityTask(MultiQueueScheduler *self)
{
    TaskEntry *taskEntry = NULL;
    if (self->prorityTaskCnt == 0) {
        return NULL;
    }
    for (uint32_t i = 0; i < self->taskQueueCnt; i++) {
        TaskQueue *taskQueue = &self->queueArray[i];
        if (self->prorityTaskCnt == 0) {
            break;
        }
        SpinLockAcquire(&taskQueue->spinLock);
        if (taskQueue->waitingTaskCnt > 0) {
            DListNode *taskNode = DListHeadNode(&taskQueue->taskQueue);
            TaskEntry *entry = DLIST_CONTAINER(TaskEntry, node, taskNode);
            if (!entry->highPriority) {
                SpinLockRelease(&taskQueue->spinLock);
                continue;
            }
            taskEntry = entry;

            taskQueue->waitingTaskCnt--;
            (void)DListPopHeadNode(&taskQueue->taskQueue);
            (void)__atomic_sub_fetch(&self->prorityTaskCnt, 1, __ATOMIC_RELEASE);
            SpinLockRelease(&taskQueue->spinLock);
            break;
        }
        SpinLockRelease(&taskQueue->spinLock);
    }
    return taskEntry;
}

static TaskEntry *GetTaskFromQueue(MultiQueueScheduler *self)
{
    TaskEntry *taskEntry = GetHighPriorityTask(self);
    if (taskEntry != NULL) {
        return taskEntry;
    }

    uint32_t retryCnt = 0;
    uint32_t startIndex = self->lastConsumeQueueIndex;
    /* Polling Task Queues. */
    UpdateLastConsumeQueueIndex(self);
    while (retryCnt < self->taskQueueCnt && self->state == SCHEDULING) {
        uint32_t queueIndex = GetConsumeQueueIndex(self, startIndex, retryCnt);
        TaskQueue *taskQueue = &self->queueArray[queueIndex];
        SpinLockAcquire(&taskQueue->spinLock);
        if (taskQueue->waitingTaskCnt > 0) {
            DListNode *taskNode = DListPopHeadNode(&taskQueue->taskQueue);
            taskQueue->waitingTaskCnt--;
            taskEntry = DLIST_CONTAINER(TaskEntry, node, taskNode);
            SpinLockRelease(&taskQueue->spinLock);
            break;
        } else {
            SpinLockRelease(&taskQueue->spinLock);
            retryCnt++;
        }
    }
    return taskEntry;
}

static inline void IncIdleWorkerCnt(ScheduleCallback *callback)
{
    if (callback && callback->incIdleWorkerCallback) {
        callback->incIdleWorkerCallback(callback->context);
    }
}

static inline void SignalWaitSubmitThread(MultiQueueScheduler *self)
{
    MutexLock(&self->lock);
    ConditionVariableSignal(&self->waitSubmitCond);
    MutexUnlock(&self->lock);
}

static inline time_t GetWaitTime(ScheduleCallback *callback)
{
    if (callback) {
        return callback->idleTimeThreshold == 0 ? DEFAULT_THREAD_WAIT_TIME : (time_t)callback->idleTimeThreshold;
    } else {
        return DEFAULT_THREAD_WAIT_TIME;
    }
}

static void DestroyTaskEntry(MultiQueueScheduler *self, TaskEntry *taskEntry)
{
    MQSchedulerMemFree(self, taskEntry->oriPtr);
}

static int32_t MultiQueueScheduleTaskImpl(Scheduler *super, ScheduleCallback *callback)
{
    ASSERT(super != NULL);
    MultiQueueScheduler *self = DOWN_TYPE_CAST(super, MultiQueueScheduler);

    if (self->state != SCHEDULING) {
        return SCHEDULE_STATE_EXIT;
    }

    TaskEntry *taskEntry = GetTaskFromQueue(self);
    if (taskEntry == NULL) {
        IncIdleWorkerCnt(callback);
        MutexLock(&self->lock);
        if (self->state == SCHEDULING) {
            time_t timeWait = GetWaitTime(callback);
            FAULT_INJECTION_ACTION(THREAD_POOL_SET_THREAD_CNT_FAULT_INJECT, (timeWait = 10)); // change to 10 ms
            bool isSignal = ConditionVariableTimedWait(&self->cond, &self->lock, timeWait);
            if (self->state != SCHEDULING) {
                MutexUnlock(&self->lock);
                /* Thread exit. */
                return SCHEDULE_STATE_EXIT;
            }
            MutexUnlock(&self->lock);

            DecIdleWorkerCnt(callback);
            if (!isSignal) {
                return SCHEDULE_STATE_TIMEOUT;
            }
            TestNewWorker(callback);
            return SCHEDULE_STATE_OK;
        }
        MutexUnlock(&self->lock);
        /* Thread exit. */
        return SCHEDULE_STATE_EXIT;
    }
    if (self->waitSubmitCnt > 0) {
        SignalWaitSubmitThread(self);
    }

    UpdateSchedStats(callback, taskEntry->enqueueTime);
    TaskPreRun(callback,
               taskEntry->task.routine == NULL ? (void *)taskEntry->task.run : (void *)taskEntry->task.routine,
               (void *)&taskEntry->task, taskEntry->task.args);
    /* call the task routine function */
    int ret = Run(taskEntry) == (void *)(intptr_t)SCHEDULE_STATE_SHRINK ? SCHEDULE_STATE_SHRINK : SCHEDULE_STATE_OK;
    TaskPostRun(callback);
    /* call the task finalize function */
    Finalize(taskEntry);

    DestroyTaskEntry(self, taskEntry);
    return ret;
}

static TaskEntry *CreateTaskEntry(MultiQueueScheduler *self, const ThreadPoolTask *task, bool highPriority,
                                  ErrorCode *errCode)
{
    void *oriTaskEntry = MQSchedulerMemAlloc(self, sizeof(TaskEntry) + GS_CACHE_LINE_SIZE);
    if (oriTaskEntry == NULL) {
        ErrLog(ERROR, ErrMsg("Failed to new TaskEntry."));
        *errCode = UTILS_ERROR_THREADPOOL_NOMEMORY;
        return NULL;
    }
    uint32_t paddingLen = GS_CACHE_LINE_SIZE - ((uint32_t)(uintptr_t)oriTaskEntry % GS_CACHE_LINE_SIZE);
    TaskEntry *taskEntry = (TaskEntry *)(oriTaskEntry + paddingLen);
    taskEntry->oriPtr = oriTaskEntry;
    taskEntry->highPriority = highPriority;
    taskEntry->task = *task;
    taskEntry->node.next = NULL;
    return taskEntry;
}

static ErrorCode TryAddTaskNodeToQueue(TaskQueue *taskQueue, TaskEntry *taskEntry)
{
    SpinLockAcquire(&taskQueue->spinLock);
    if (taskQueue->waitingTaskCnt >= taskQueue->maxQueueSize) {
        SpinLockRelease(&taskQueue->spinLock);
        return UTILS_ERROR_THREADPOOL_TOOMANYTASK;
    }
    if (taskEntry->highPriority) {
        DListPushHead(&taskQueue->taskQueue, &taskEntry->node);
    } else {
        DListPushTail(&taskQueue->taskQueue, &taskEntry->node);
    }
    taskQueue->waitingTaskCnt++;
    taskEntry->enqueueTime = GetMonotonicTimeUs();
    SpinLockRelease(&taskQueue->spinLock);

    return ERROR_SYS_OK;
}

static inline uint32_t GetProduceQueueIndex(const MultiQueueScheduler *self, uint32_t startQueueId, uint32_t retryCnt)
{
    return (startQueueId + retryCnt) % self->taskQueueCnt;
}

static inline void UpdateLastProduceQueueIndex(MultiQueueScheduler *self)
{
    /* The produce queue is switched in polling mode. To improve concurrency performance,
     * no lock is added during polling, but the correctness is not affected.
     * The minimum taskQueueCnt is 1. */
    self->lastProduceQueueIndex++;
    self->lastProduceQueueIndex %= self->taskQueueCnt;
}

static ErrorCode AddTaskToQueue(MultiQueueScheduler *self, TaskEntry *taskEntry)
{
    ErrorCode errCode = ERROR_SYS_OK;
    uint32_t retryCnt = 0;
    uint32_t startIndex = self->lastProduceQueueIndex;
    bool highPriority = taskEntry->highPriority;
    UpdateLastProduceQueueIndex(self);
    while (retryCnt < self->taskQueueCnt && self->state == SCHEDULING) {
        /* If the start queue is full, the next queue is selected until all queues are polled. */
        uint32_t queueIndex = GetProduceQueueIndex(self, startIndex, retryCnt);
        TaskQueue *taskQueue = &self->queueArray[queueIndex];
        errCode = TryAddTaskNodeToQueue(taskQueue, taskEntry);
        if (errCode == ERROR_SYS_OK) {
            if (highPriority) {
                (void)__atomic_add_fetch(&self->prorityTaskCnt, 1, __ATOMIC_RELEASE);
            }
            return errCode;
        }

        retryCnt++;
    }
    return errCode;
}

static ErrorCode SubmitTask(MultiQueueScheduler *self, TaskEntry *taskEntry)
{
    while (self->state == SCHEDULING && AddTaskToQueue(self, taskEntry) != ERROR_SYS_OK) {
        /* If all queues are full at a polling time, the queue is blocked. When there is idle space in the task queue,
         * the blocked thread is woken up to add tasks to prevent task loss. */
        MutexLock(&self->lock);
        if (self->state == SCHEDULING) {
            self->waitSubmitCnt++;
            ErrLog(ERROR, ErrMsg("All task queues are full, Thread %lu start waiting.", GetCurrentTid().tid));
            ConditionVariableWait(&self->waitSubmitCond, &self->lock);
            self->waitSubmitCnt--;
            MutexUnlock(&self->lock);
        } else {
            MutexUnlock(&self->lock);
            return UTILS_ERROR_THREADPOOL_ERRSTATE;
        }
    }
    return ERROR_SYS_OK;
}

static ErrorCode MultiQueueSchedulerSubmitTaskImpl(Scheduler *super, ThreadPoolTask *task, SYMBOL_UNUSED TaskId *taskId,
                                                   bool highPriority)
{
    ASSERT(super != NULL);
    MultiQueueScheduler *self = DOWN_TYPE_CAST(super, MultiQueueScheduler);

    ErrorCode errCode = ERROR_SYS_OK;
    TaskEntry *taskEntry = CreateTaskEntry(self, task, highPriority, &errCode);
    if (taskEntry == NULL) {
        return errCode;
    }
    if (self->state != SCHEDULING) {
        DestroyTaskEntry(self, taskEntry);
        return UTILS_ERROR_THREADPOOL_ERRSTATE;
    }

    errCode = SubmitTask(self, taskEntry);
    if (errCode != ERROR_SYS_OK) {
        DestroyTaskEntry(self, taskEntry);
        return errCode;
    }

    ConditionVariableSignal(&self->cond);
    return ERROR_SYS_OK;
}

static ErrorCode MultiQueueSchedulerCancelTaskImpl(SYMBOL_UNUSED Scheduler *super, SYMBOL_UNUSED TaskId taskId)
{
    ASSERT(super != NULL);
    ErrLog(INFO, ErrMsg("Cancel task not support int Multi-Queue scheduler yet."));
    return UTILS_ERROR_OPERATION_NOT_SUPPORT;
}

static void MultiQueueSchedulerWakeUpWorkerImpl(Scheduler *super)
{
    ASSERT(super != NULL);
    MultiQueueScheduler *self = DOWN_TYPE_CAST(super, MultiQueueScheduler);
    ConditionVariableBroadcast(&self->cond);
}

static void DestroySchedulerBroadcast(MultiQueueScheduler *self)
{
    MutexLock(&self->lock);
    self->state = DESTROYED;
    ConditionVariableBroadcast(&self->cond);
    ConditionVariableBroadcast(&self->waitSubmitCond);
    MutexUnlock(&self->lock);
}

static void DestroyQueueWaitingTasks(MultiQueueScheduler *self, TaskQueue *taskQueue)
{
    SpinLockAcquire(&taskQueue->spinLock);
    DListHead *head = &taskQueue->taskQueue;
    DListMutableIter iter;
    DLIST_MODIFY_FOR_EACH(iter, head)
    {
        TaskEntry *taskEntry = DLIST_CONTAINER(TaskEntry, node, iter.cur);
        /* The tasks in the waiting queue are not executed, delete them directly. */
        ErrLog(DEBUG, ErrMsg("Destroy queue waiting task."));
        DListDelete(&taskEntry->node);
        taskQueue->waitingTaskCnt--;
        Finalize(taskEntry);
        DestroyTaskEntry(self, taskEntry);
    }
    SpinLockRelease(&taskQueue->spinLock);
}

static void DestroyTaskQueues(MultiQueueScheduler *self)
{
    if (self->queueArray == NULL) {
        return;
    }
    TaskQueue *queueArray = self->queueArray;
    for (uint32_t i = 0; i < self->taskQueueCnt; i++) {
        DestroyQueueWaitingTasks(self, &queueArray[i]);
    }
}

static void MultiQueueSchedulerDestroyImpl(Scheduler *super)
{
    ASSERT(super != NULL);
    MultiQueueScheduler *self = DOWN_TYPE_CAST(super, MultiQueueScheduler);

    ErrLog(INFO, ErrMsg("Start to destroy the multi queue scheduler."));

    DestroySchedulerBroadcast(self);
    DestroyTaskQueues(self);
}

static void InitTaskQueue(TaskQueue *taskQueue, uint32_t maxQueueSize)
{
    SpinLockInit(&taskQueue->spinLock);
    DListInit(&taskQueue->taskQueue);
    taskQueue->maxQueueSize = maxQueueSize;
    taskQueue->waitingTaskCnt = 0;
}

static ErrorCode MultiQueueSchedulerInit(MultiQueueScheduler *self, MultiQueueSchedulerInitParams *initParams)
{
    ASSERT(self != NULL);
    ASSERT(initParams != NULL);

    if (initParams->taskQueueCnt == 0 || initParams->taskQueueCnt > MAX_QUEUE_COUNT) {
        ErrLog(ERROR, ErrMsg("Invalid taskQueueCnt : %u.", initParams->taskQueueCnt));
        return ERROR_UTILS_COMMON_INVALID_PARAMETER;
    }
    if (initParams->maxQueueSize == 0 || initParams->maxQueueSize > MAX_TASK_QUEUE_SIZE) {
        ErrLog(ERROR, ErrMsg("Invalid maxQueueSize : %u.", initParams->maxQueueSize));
        return ERROR_UTILS_COMMON_INVALID_PARAMETER;
    }

    self->allocator = initParams->allocator;
    self->taskQueueCnt = initParams->taskQueueCnt;

    TaskQueue *queueArray = MQSchedulerMemAlloc(self, (initParams->taskQueueCnt * sizeof(TaskQueue)));
    if (queueArray == NULL) {
        return ERROR_UTILS_COMMON_NOENOUGH_MEMORY;
    }
    ErrLog(INFO, ErrMsg("Init MultiQueueScheduler, the taskQueue count: %u.", initParams->taskQueueCnt));
    for (uint32_t i = 0; i < self->taskQueueCnt; i++) {
        InitTaskQueue(&queueArray[i], initParams->maxQueueSize);
    }
    self->queueArray = queueArray;

    MutexInit(&self->lock);
    ConditionVariableInit(&self->cond);
    ConditionVariableInit(&self->waitSubmitCond);
    self->waitSubmitCnt = 0;
    self->state = SCHEDULING;

    ErrLog(INFO, ErrMsg("Init multi queue scheduler done."));
    return ERROR_SYS_OK;
}

static void MultiQueueSchedulerFinalize(MultiQueueScheduler *self)
{
    TaskQueue *queueArray = self->queueArray;
    for (uint32_t i = 0; i < self->taskQueueCnt; i++) {
        TaskQueue *taskQueue = &queueArray[i];
        SpinLockDestroy(&taskQueue->spinLock);
    }
    MQSchedulerMemFree(self, queueArray);

    MutexDestroy(&self->lock);
    ConditionVariableDestroy(&self->cond);
    ConditionVariableDestroy(&self->waitSubmitCond);
    ErrLog(INFO, ErrMsg("Finalize multi queue scheduler done."));
}

static uint64_t MultiQueueScheduleGetEarliestImpl(Scheduler *super)
{
    ASSERT(super != NULL);
    MultiQueueScheduler *self = DOWN_TYPE_CAST(super, MultiQueueScheduler);
    uint64_t earliestEnqueueTime = UINT64_MAX;
    for (uint32_t i = 0; (self->state == SCHEDULING) && (i < self->taskQueueCnt); i++) {
        TaskQueue *taskQueue = &self->queueArray[i];
        SpinLockAcquire(&taskQueue->spinLock);
        DListHead *head = &taskQueue->taskQueue;
        DListMutableIter iter;
        DLIST_MODIFY_FOR_EACH(iter, head)
        {
            /* loop this queue */
            TaskEntry *taskEntry = DLIST_CONTAINER(TaskEntry, node, iter.cur);
            earliestEnqueueTime = Min(earliestEnqueueTime, taskEntry->enqueueTime);
            if (!taskEntry->highPriority) {
                /* if the task is high priority, check until it is normal task, because the normal task
                 * maybe had submited more earily */
                break;
            }
        }
        /* go next queue */
        SpinLockRelease(&taskQueue->spinLock);
    }
    return earliestEnqueueTime;
}

static void MultiQueueSchedulerOpsInit(MultiQueueScheduler *self)
{
    GET_FOPS(Scheduler)->getEarliestTaskSubmitTime = MultiQueueScheduleGetEarliestImpl;
    GET_FOPS(Scheduler)->scheduleTask = MultiQueueScheduleTaskImpl;
    GET_FOPS(Scheduler)->submitTask = MultiQueueSchedulerSubmitTaskImpl;
    GET_FOPS(Scheduler)->cancelTask = MultiQueueSchedulerCancelTaskImpl;
    GET_FOPS(Scheduler)->wakeUpWorker = MultiQueueSchedulerWakeUpWorkerImpl;
    GET_FOPS(Scheduler)->destroy = MultiQueueSchedulerDestroyImpl;
}

DEFINE_NEW_TYPED_CLASS(MultiQueueScheduler, Scheduler)
