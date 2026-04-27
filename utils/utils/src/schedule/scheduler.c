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
 * scheduler.c
 *
 * Description:
 * 1. the implementation of the Task Scheduler Public Interface.
 *
 * ---------------------------------------------------------------------------------------
 */

#include "schedule/scheduler.h"
#include "defines/err_code.h"
#include "types/type_object.h"
#include "thread/thread_pool.h"
#include "schedule/scheduler_internal.h"

static int32_t SchedulerScheduleTaskImpl(SYMBOL_UNUSED Scheduler *self, SYMBOL_UNUSED ScheduleCallback *callback)
{
    ASSERT(self != NULL);
    return false;
}

static ErrorCode SchedulerSubmitTaskImpl(SYMBOL_UNUSED Scheduler *self, SYMBOL_UNUSED ThreadPoolTask *task,
                                         SYMBOL_UNUSED TaskId *taskId, SYMBOL_UNUSED bool highPriority)
{
    ASSERT(self != NULL);
    return ERROR_SYS_OK;
}

static ErrorCode SchedulerCancelTaskImpl(SYMBOL_UNUSED Scheduler *self, SYMBOL_UNUSED TaskId taskId)
{
    ASSERT(self != NULL);
    return ERROR_SYS_OK;
}

static void SchedulerWakeUpWorkerImpl(SYMBOL_UNUSED Scheduler *self)
{
    ASSERT(self != NULL);
}

static void SchedulerDestroyImpl(SYMBOL_UNUSED Scheduler *self)
{
    ASSERT(self != NULL);
}

static ErrorCode SchedulerInit(SYMBOL_UNUSED Scheduler *self, SYMBOL_UNUSED TypeInitParams *initData)
{
    return ERROR_SYS_OK;
}

static void SchedulerFinalize(SYMBOL_UNUSED Scheduler *self)
{}

static uint64_t ScheduleGetEarliestImpl(SYMBOL_UNUSED Scheduler *self)
{
    return ~(uint64_t)0;
}

static void SchedulerOpsInit(SchedulerOps *self)
{
    ASSERT(self != NULL);
    GET_FOPS(Scheduler)->getEarliestTaskSubmitTime = ScheduleGetEarliestImpl;
    GET_FOPS(Scheduler)->scheduleTask = SchedulerScheduleTaskImpl;
    GET_FOPS(Scheduler)->submitTask = SchedulerSubmitTaskImpl;
    GET_FOPS(Scheduler)->cancelTask = SchedulerCancelTaskImpl;
    GET_FOPS(Scheduler)->wakeUpWorker = SchedulerWakeUpWorkerImpl;
    GET_FOPS(Scheduler)->destroy = SchedulerDestroyImpl;
}

DEFINE_NEW_TYPED_CLASS(Scheduler, RefObject);

UTILS_EXPORT uint64_t SchedulerGetEarliestTaskTime(Scheduler *self)
{
    if (self != NULL) {
        return GET_FAP(Scheduler)->getEarliestTaskSubmitTime(self);
    }
    return UINT64_MAX;
}

UTILS_EXPORT int32_t ScheduleTask(Scheduler *self, ScheduleCallback *callback)
{
    if (self != NULL) {
        return GET_FAP(Scheduler)->scheduleTask(self, callback);
    }
    return SCHEDULE_STATE_EXIT;
}

UTILS_EXPORT ErrorCode SchedulerSubmitTask(Scheduler *self, ThreadPoolTask *task, TaskId *taskId, bool highPriority)
{
    if (self != NULL) {
        return GET_FAP(Scheduler)->submitTask(self, task, taskId, highPriority);
    }
    return -1;
}

UTILS_EXPORT ErrorCode SchedulerCancelTask(Scheduler *self, TaskId taskId)
{
    if (self != NULL) {
        return GET_FAP(Scheduler)->cancelTask(self, taskId);
    }
    return -1;
}

UTILS_EXPORT void SchedulerWakeUpWorker(Scheduler *self)
{
    if (self != NULL) {
        GET_FAP(Scheduler)->wakeUpWorker(self);
    }
}

UTILS_EXPORT void SchedulerDestroy(Scheduler *self)
{
    if (self != NULL) {
        GET_FAP(Scheduler)->destroy(self);
    }
}
