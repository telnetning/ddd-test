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
 * fifo_scheduler.c
 *
 * Description:
 * 1. the implementation of the Task Scheduler Public Interface.
 *
 * ---------------------------------------------------------------------------------------
 */

#include "schedule/scheduler.h"
#include "defines/utils_errorcode.h"
#include "types/data_types.h"
#include "types/atomic_type.h"
#include "defines/err_code.h"
#include "fault_injection/fault_injection.h"
#include "schedule/scheduler_internal.h"
#include "schedule/fifo_scheduler.h"

#ifdef GSDB_DEBUG
#define MAX_TASK_ENTRY_REFCOUNT 32
#define ASSERT_REFCOUNT(self)                                                    \
    do {                                                                         \
        ASSERT((self) != NULL);                                                  \
        ASSERT(GSDB_ATOMIC32_GET(&(self)->refCount) > 0);                        \
        ASSERT(GSDB_ATOMIC32_GET(&(self)->refCount) <= MAX_TASK_ENTRY_REFCOUNT); \
    } while (0)
#else
#define ASSERT_REFCOUNT(self)
#endif /* GSDB_DEBUG */

#define DEFAULT_FIRST_TASK_ID    1
#define DEFAULT_TASK_QUEUE_SIZE  1000
#define MAX_TASK_QUEUE_SIZE      10000000
#define DEFAULT_THREAD_WAIT_TIME 3000 /* unit: ms */

#ifdef LOCAL_MODULE_NAME
#undef LOCAL_MODULE_NAME
#endif
#define LOCAL_MODULE_NAME "thread_pool"

typedef struct TaskEntry TaskEntry;
struct TaskEntry {
    Atomic32 refCount;
    Atomic32 killed;
    DListNode node;
    uint64_t taskId;
    uint64_t enqueueTime;
    ThreadPoolTask task;
    Mutex lock;
    TaskState state;
    void *oriPtr;
} __attribute__((aligned(GS_CACHE_LINE_SIZE)));

static void *SchedulerMemAlloc(MemAllocator *allocator, uint64_t size)
{
    if (allocator != NULL && allocator->alloc != NULL) {
        return allocator->alloc(allocator, size);
    }
    return malloc(size);
}

static void SchedulerMemFree(MemAllocator *allocator, void *ptr)
{
    if (allocator != NULL && allocator->free != NULL) {
        allocator->free(allocator, ptr);
        return;
    }
    free(ptr);
}

static void TaskEntryFinalize(TaskEntry *self, MemAllocator *allocator)
{
    MutexDestroy(&self->lock);
    SchedulerMemFree(allocator, self->oriPtr);
}

static void TaskEntryIncRef(TaskEntry *self)
{
    ASSERT_REFCOUNT(self);
    GSDB_ATOMIC32_INC(&self->refCount);
}

static bool TaskEntryKill(TaskEntry *self)
{
    ASSERT_REFCOUNT(self);
    if (GSDB_ATOMIC32_CAS(&self->killed, 0, 1)) {
        return true;
    }
    return false;
}

static void TaskEntryDecRef(TaskEntry *self, MemAllocator *allocator)
{
    ASSERT_REFCOUNT(self);
    if (GSDB_ATOMIC32_DEC_AND_TEST_ZERO(&self->refCount)) {
        TaskEntryFinalize(self, allocator);
    }
}

static void TaskEntryKillAndDecRef(TaskEntry *self, MemAllocator *allocator)
{
    ASSERT(self != NULL);
    (void)TaskEntryKill(self);
    TaskEntryDecRef(self, allocator);
}

static bool SetStatus(TaskEntry *self, TaskState oldStatus, TaskState newStatus)
{
    MutexLock(&self->lock);
    if (self->state != oldStatus) {
        ErrLog(WARNING, ErrMsg("Set task:%lu state error, old: %u, new: %u.", self->taskId, (uint32_t)oldStatus,
                               (uint32_t)newStatus));
        MutexUnlock(&self->lock);
        return false;
    }
    self->state = newStatus;
    MutexUnlock(&self->lock);
    return true;
}

static void *Run(TaskEntry *self)
{
    if (SetStatus(self, INITIAL, EXECUTING)) {
        if (self->task.routine) {
            return self->task.routine(self->task.args);
        }
    }
    return NULL;
}

static void Finalize(TaskEntry *self, bool cancel)
{
    if ((!cancel && SetStatus(self, EXECUTING, FINISHED)) || (cancel && SetStatus(self, INITIAL, FINISHED))) {
        if (self->task.finalize) {
            self->task.finalize(self->task.args);
        }
    }
}

static void Cancel(TaskEntry *self)
{
    MutexLock(&self->lock);
    if (self->state < FINISHED) {
        ErrLog(DEBUG, ErrMsg("Cancel executing task, taskId = %lu.", self->taskId));
        if (self->task.cancel) {
            self->task.cancel(self->task.args);
        }
    } else {
        ErrLog(DEBUG, ErrMsg("Cancel executing task failed, the task is finished, taskId = %lu.", self->taskId));
    }
    MutexUnlock(&self->lock);
}

static inline void IncIdleWorkerCnt(ScheduleCallback *callback)
{
    if (callback && callback->incIdleWorkerCallback) {
        callback->incIdleWorkerCallback(callback->context);
    }
}

static TaskEntry *GetTaskFromQueue(FIFOScheduler *self)
{
    TaskEntry *taskEntry = NULL;
    DListNode *taskNode = NULL;

    MutexLock(&self->lock);
    if (self->state != SCHEDULING) {
        MutexUnlock(&self->lock);
        return NULL;
    }
    if (self->waitingTaskCnt == 0) {
        MutexUnlock(&self->lock);
        return NULL;
    }
    /*
     * When a task is removed from the waiting queue and placed in the execution queue, the whole process is
     * locked to ensure the atomicity of concurrency.
     */
    taskNode = DListPopHeadNode(&self->waitingQueue);
    self->waitingTaskCnt--;
    DListPushTail(&self->executingQueue, taskNode);
    MutexUnlock(&self->lock);

    taskEntry = DLIST_CONTAINER(TaskEntry, node, taskNode);
    return taskEntry;
}

static inline time_t GetWaitTime(ScheduleCallback *callback)
{
    if (callback) {
        return callback->idleTimeThreshold == 0 ? DEFAULT_THREAD_WAIT_TIME : (time_t)callback->idleTimeThreshold;
    } else {
        return DEFAULT_THREAD_WAIT_TIME;
    }
}

static int32_t FIFOSchedulerTaskImpl(Scheduler *super, ScheduleCallback *callback)
{
    FIFOScheduler *self = DOWN_TYPE_CAST(super, FIFOScheduler);
    TaskEntry *taskEntry = GetTaskFromQueue(self);
    if (taskEntry == NULL) {
        IncIdleWorkerCnt(callback);
        MutexLock(&self->lock);
        if (self->state != SCHEDULING) {
            MutexUnlock(&self->lock);
            return SCHEDULE_STATE_EXIT;
        }
        time_t timeWait = GetWaitTime(callback);
        FAULT_INJECTION_ACTION(THREAD_POOL_SET_THREAD_CNT_FAULT_INJECT, (timeWait = 10)); // change to 10 ms
        bool isSignal = ConditionVariableTimedWait(&self->cond, &self->lock, timeWait);
        if (self->state != SCHEDULING) {
            MutexUnlock(&self->lock);
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

    /* before run */
    UpdateSchedStats(callback, taskEntry->enqueueTime);
    TaskPreRun(callback,
               taskEntry->task.routine == NULL ? (void *)taskEntry->task.run : (void *)taskEntry->task.routine,
               &taskEntry->task, taskEntry->task.args);

    /* call the task routine function */
    int ret = Run(taskEntry) == (void *)(intptr_t)SCHEDULE_STATE_SHRINK ? SCHEDULE_STATE_SHRINK : SCHEDULE_STATE_OK;
    /* after run */
    TaskPostRun(callback);
    /* call the task finalize function */
    Finalize(taskEntry, false);

    /*
     * To solve the concurrency problem of canceling an ongoing task and deleting a task after the task
     * is executed, we still need to obtain the lock again.
     */
    MutexLock(&self->lock);
    DListDelete(&taskEntry->node);
    TaskEntryKillAndDecRef(taskEntry, self->allocator);
    MutexUnlock(&self->lock);

    return ret;
}

static inline int32_t IsTaskQueueFull(const FIFOScheduler *self)
{
    return self->waitingTaskCnt >= self->maxQueueSize;
}

static TaskEntry *CreateTaskEntry(FIFOScheduler *self, const ThreadPoolTask *task, ErrorCode *errCode)
{
    void *oriTaskEntry = SchedulerMemAlloc(self->allocator, sizeof(TaskEntry) + GS_CACHE_LINE_SIZE);
    if (oriTaskEntry == NULL) {
        ErrLog(ERROR, ErrMsg("Failed to new TaskEntry."));
        *errCode = UTILS_ERROR_THREADPOOL_NOMEMORY;
        return NULL;
    }

    uint32_t paddingLen = GS_CACHE_LINE_SIZE - ((uint32_t)(uintptr_t)oriTaskEntry % GS_CACHE_LINE_SIZE);
    TaskEntry *taskEntry = (TaskEntry *)(oriTaskEntry + paddingLen);
    taskEntry->oriPtr = oriTaskEntry;

    GSDB_ATOMIC32_SET(&taskEntry->refCount, 1);
    GSDB_ATOMIC32_SET(&taskEntry->killed, 0);
    taskEntry->task = *task;
    taskEntry->state = INITIAL;
    MutexInit(&taskEntry->lock);
    DListNodeInit(&taskEntry->node);
    return taskEntry;
}

static void SetTaskId(TaskEntry *self, uint64_t taskId)
{
    self->taskId = taskId;
}

static ErrorCode FIFOSchedulerSubmitTaskImpl(Scheduler *super, ThreadPoolTask *task, TaskId *taskId, bool highPriority)
{
    FIFOScheduler *self = DOWN_TYPE_CAST(super, FIFOScheduler);
    ErrorCode errCode;
    TaskEntry *taskEntry = CreateTaskEntry(self, task, &errCode);
    if (taskEntry == NULL) {
        return errCode;
    }

    MutexLock(&self->lock);
    if (IsTaskQueueFull(self)) {
        MutexUnlock(&self->lock);
        SchedulerMemFree(self->allocator, taskEntry->oriPtr);
        ErrLog(INFO, ErrMsg("Schedule task failed, too many tasks."));
        return UTILS_ERROR_THREADPOOL_TOOMANYTASK;
    }

    SetTaskId(taskEntry, self->nextTaskId);
    if (highPriority) {
        DListPushHead(&self->waitingQueue, &taskEntry->node);
    } else {
        DListPushTail(&self->waitingQueue, &taskEntry->node);
    }
    taskEntry->enqueueTime = GetMonotonicTimeUs();
    *taskId = self->nextTaskId;
    self->waitingTaskCnt++;
    self->nextTaskId++;

    ConditionVariableSignal(&self->cond);
    MutexUnlock(&self->lock);

    return ERROR_SYS_OK;
}

static ErrorCode CancelFromWaitingQueue(FIFOScheduler *self, TaskId taskId)
{
    TaskEntry *taskEntry = NULL;

    MutexLock(&self->lock);
    DListHead *head = &self->waitingQueue;
    DListMutableIter iter;
    DLIST_MODIFY_FOR_EACH(iter, head)
    {
        taskEntry = DLIST_CONTAINER(TaskEntry, node, iter.cur);
        if (taskEntry == NULL) {
            ErrLog(ERROR, ErrMsg("Cancel task catch error, the task is null."));
            continue;
        }
        if (taskEntry->taskId == taskId) {
            /*
             * Delete the task from the queue and release the lock. In this case, the Finalize function
             * can be executed safely.
             */
            DListDelete(iter.cur);
            self->waitingTaskCnt--;
            MutexUnlock(&self->lock);

            Finalize(taskEntry, true);

            /* Free the taskEntry */
            TaskEntryKillAndDecRef(taskEntry, self->allocator);
            ErrLog(INFO, ErrMsg("Cancel task success, taskId = %lu.", taskId));
            return ERROR_SYS_OK;
        }
    }
    MutexUnlock(&self->lock);

    ErrLog(INFO, ErrMsg("Cancel task failed, The task has been executed, taskId = %lu.", taskId));
    return UTILS_ERROR_THREADPOOL_TASKNOTEXIST;
}

static ErrorCode CancelFromExecutingQueue(FIFOScheduler *self, TaskId taskId)
{
    TaskEntry *taskEntry = NULL;
    MutexLock(&self->lock);
    DListHead *head = &self->executingQueue;
    DListMutableIter iter;
    DLIST_MODIFY_FOR_EACH(iter, head)
    {
        taskEntry = DLIST_CONTAINER(TaskEntry, node, iter.cur);
        if (taskEntry == NULL) {
            ErrLog(ERROR, ErrMsg("Cancel task catch error, the task is null."));
            continue;
        }
        if (taskEntry->taskId == taskId) {
            /* Add references to prevent concurrent threads from deleting task objects. */
            TaskEntryIncRef(taskEntry);
            MutexUnlock(&self->lock);

            Cancel(taskEntry);

            /* Release References */
            TaskEntryKillAndDecRef(taskEntry, self->allocator);
            return UTILS_ERROR_THREADPOOL_TASKISEXECUTING;
        }
    }
    MutexUnlock(&self->lock);

    ErrLog(DEBUG, ErrMsg("Cancel task failed, The task has been executed, taskId = %lu.", taskId));
    return UTILS_ERROR_THREADPOOL_TASKNOTEXIST;
}

static ErrorCode FIFOSchedulerCancelTaskImpl(Scheduler *super, TaskId taskId)
{
    FIFOScheduler *self = DOWN_TYPE_CAST(super, FIFOScheduler);
    ErrorCode errCode = CancelFromWaitingQueue(self, taskId);
    if (errCode == ERROR_SYS_OK) {
        return ERROR_SYS_OK;
    }

    return CancelFromExecutingQueue(self, taskId);
}

static uint32_t GetQueueSize(DListHead *head)
{
    DListIter iter;
    uint32_t size = 0;
    DLIST_FOR_EACH(iter, head)
    {
        size++;
    }
    return size;
}

static void ProcessCancel(TaskEntry **taskList, uint32_t listSize, MemAllocator *allocator)
{
    TaskEntry *taskEntry = NULL;
    for (uint32_t i = 0; i < listSize; i++) {
        taskEntry = taskList[i];
        /*
         * Execute the cancel function to notify that the task is canceled, The task is interrupted by the task
         * designator itself. and then the task execution thread free the task from the execution queue.
         */
        Cancel(taskEntry);

        ErrLog(DEBUG, ErrMsg("Cancel executing tasks, taskId = %lu.", taskEntry->taskId));
        /* Release References */
        TaskEntryKillAndDecRef(taskEntry, allocator);
    }
}

static void CancelExecutingTasks(FIFOScheduler *self)
{
    MutexLock(&self->lock);
    DListHead *head = &self->executingQueue;
    uint32_t queueSize = GetQueueSize(head);
    TaskEntry **taskList = SchedulerMemAlloc(self->allocator, (sizeof(TaskEntry *) * queueSize));
    if (taskList == NULL) {
        MutexUnlock(&self->lock);
        ErrLog(ERROR, ErrMsg("CancelExecutingTasks failed, out of memory"));
        return;
    }
    DListMutableIter iter;
    TaskEntry *taskEntry = NULL;
    uint32_t locator = 0;
    DLIST_MODIFY_FOR_EACH(iter, head)
    {
        taskEntry = DLIST_CONTAINER(TaskEntry, node, iter.cur);
        if (taskEntry == NULL) {
            ErrLog(ERROR, ErrMsg("Cancel executing tasks catch error, the task is null."));
            continue;
        }

        /* Add references to prevent concurrent threads from deleting task objects. */
        TaskEntryIncRef(taskEntry);
        taskList[locator] = taskEntry;
        locator++;
    }
    MutexUnlock(&self->lock);

    ProcessCancel(taskList, locator, self->allocator);
    SchedulerMemFree(self->allocator, taskList);
}

static void CancelWaitingTasks(FIFOScheduler *self)
{
    MutexLock(&self->lock);
    TaskEntry *taskEntry = NULL;
    DListHead *head = &self->waitingQueue;
    DListMutableIter iter;
    DLIST_MODIFY_FOR_EACH(iter, head)
    {
        taskEntry = DLIST_CONTAINER(TaskEntry, node, iter.cur);
        if (taskEntry == NULL) {
            ErrLog(ERROR, ErrMsg("Free waiting task catch error, the task is null."));
            continue;
        }
        ErrLog(DEBUG, ErrMsg("Cancel waiting tasks, taskId = %lu.", taskEntry->taskId));

        /* The tasks in the waiting queue are not executed, delete them directly. */
        Finalize(taskEntry, true);
        DListDelete(iter.cur);

        /* Free the taskEntry */
        TaskEntryKillAndDecRef(taskEntry, self->allocator);
    }
    MutexUnlock(&self->lock);
}

static void DestroyScheduler(FIFOScheduler *self)
{
    MutexLock(&self->lock);
    self->state = DESTROYED;
    ConditionVariableBroadcast(&self->cond);
    MutexUnlock(&self->lock);
}

static void FIFOSchedulerDestroyImpl(Scheduler *super)
{
    FIFOScheduler *self = DOWN_TYPE_CAST(super, FIFOScheduler);
    DestroyScheduler(self);

    /* Cancel the tasks in the execution queue */
    CancelExecutingTasks(self);

    /* Cancel the tasks in the waiting queue */
    CancelWaitingTasks(self);
}

/*
 * ------------------------------------------------
 * Define class of FIFOScheduler
 * ------------------------------------------------
 */
static ErrorCode FIFOSchedulerInit(FIFOScheduler *self, SchedulerInitParams *initParams)
{
    if (initParams == NULL) {
        ErrLog(ERROR, ErrMsg("New FIFOScheduler failed, the initParams is NULL."));
        return UTILS_ERROR_THREADPOOL_NULLPARAMETER;
    }
    if (initParams->maxQueueSize > MAX_TASK_QUEUE_SIZE) {
        ErrLog(ERROR, ErrMsg("Invalid maxQueueSize : %u", initParams->maxQueueSize));
        return UTILS_ERROR_THREADPOOL_PARAMETERERROR;
    }

    self->allocator = initParams->allocator;
    self->maxQueueSize = initParams->maxQueueSize;
    if (self->maxQueueSize == 0) {
        self->maxQueueSize = DEFAULT_TASK_QUEUE_SIZE;
    }
    self->waitingTaskCnt = 0;
    self->nextTaskId = DEFAULT_FIRST_TASK_ID;
    self->state = SCHEDULING;

    MutexInit(&self->lock);
    ConditionVariableInit(&self->cond);

    DListInit(&self->waitingQueue);
    DListInit(&self->executingQueue);

    ErrLog(INFO, ErrMsg("Init FIFO scheduler done."));
    return ERROR_SYS_OK;
}

static void FIFOSchedulerFinalize(FIFOScheduler *self)
{
    ErrLog(INFO, ErrMsg("Finalize FIFO scheduler done."));
    MutexDestroy(&self->lock);
    ConditionVariableDestroy(&self->cond);
}

static uint64_t FIFOScheduleGetEarliestImpl(Scheduler *super)
{
    ASSERT(super != NULL);
    FIFOScheduler *self = DOWN_TYPE_CAST(super, FIFOScheduler);

    uint64_t earliestEnqueueTime = UINT64_MAX;

    MutexLock(&self->lock);
    if ((self->state != SCHEDULING) || (self->waitingTaskCnt == 0)) {
        MutexUnlock(&self->lock);
        return earliestEnqueueTime;
    }
    TaskEntry *taskEntry = (TaskEntry *)DListHeadElementOff(&self->waitingQueue, offsetof(TaskEntry, node));
    earliestEnqueueTime = taskEntry->enqueueTime;
    MutexUnlock(&self->lock);
    return earliestEnqueueTime;
}

static void FIFOSchedulerOpsInit(FIFOScheduler *self)
{
    GET_FOPS(Scheduler)->getEarliestTaskSubmitTime = FIFOScheduleGetEarliestImpl;
    GET_FOPS(Scheduler)->scheduleTask = FIFOSchedulerTaskImpl;
    GET_FOPS(Scheduler)->submitTask = FIFOSchedulerSubmitTaskImpl;
    GET_FOPS(Scheduler)->cancelTask = FIFOSchedulerCancelTaskImpl;
    GET_FOPS(Scheduler)->destroy = FIFOSchedulerDestroyImpl;
}

DEFINE_NEW_TYPED_CLASS(FIFOScheduler, Scheduler)
