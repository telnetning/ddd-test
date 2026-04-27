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
 * scheduler.h
 *
 * Description:
 * 1. the interface of the scheduler.
 * ---------------------------------------------------------------------------------------
 */

#ifndef UTILS_SCHEDULER_H
#define UTILS_SCHEDULER_H

#include "types/ref_object.h"
#include "thread/thread_pool.h"

GSDB_BEGIN_C_CODE_DECLS

typedef struct ScheduleCallback ScheduleCallback;

typedef enum SchedulerState { SCHEDULING, DESTROYED } SchedulerState;

struct Scheduler {
    RefObject super;
};

typedef struct SchedulerOps SchedulerOps;
struct SchedulerOps {
    RefObjectOps super;
    uint64_t (*getEarliestTaskSubmitTime)(Scheduler *self);
    int32_t (*scheduleTask)(Scheduler *self, ScheduleCallback *callback);
    ErrorCode (*submitTask)(Scheduler *self, ThreadPoolTask *task, TaskId *taskId, bool highPriority);
    ErrorCode (*cancelTask)(Scheduler *self, TaskId taskId);
    void (*wakeUpWorker)(Scheduler *self);
    void (*destroy)(Scheduler *self);
};

DECLARE_NEW_TYPED_CLASS(Scheduler)

/**
 * @brief Get the earliest task submit time in scheduler task queue
 * @return UINT64_MAX, no task in queue, or return the earliest task submit time
 */
uint64_t SchedulerGetEarliestTaskTime(Scheduler *self);

int32_t ScheduleTask(Scheduler *self, ScheduleCallback *callback);

ErrorCode SchedulerSubmitTask(Scheduler *self, ThreadPoolTask *task, TaskId *taskId, bool highPriority);

ErrorCode SchedulerCancelTask(Scheduler *self, TaskId taskId);

void SchedulerWakeUpWorker(Scheduler *self);

void SchedulerDestroy(Scheduler *self);

GSDB_END_C_CODE_DECLS

#endif // UTILS_SCHEDULER_H
