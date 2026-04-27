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
 * fifo_scheduler.h
 *
 * Description:
 * the internal header file of the fifo_scheduler.
 * ---------------------------------------------------------------------------------
 */

#ifndef UTILS_FIFO_SCHEDULER_H
#define UTILS_FIFO_SCHEDULER_H

#include "container/linked_list.h"
#include "types/ref_object.h"
#include "defines/common.h"
#include "schedule/scheduler.h"
#include "schedule/scheduler_internal.h"
#include "thread/thread_pool.h"

GSDB_BEGIN_C_CODE_DECLS

typedef struct FIFOScheduler FIFOScheduler;
struct FIFOScheduler {
    Scheduler super;
    MemAllocator *allocator;
    uint32_t maxQueueSize;
    Mutex lock;
    uint32_t waitingTaskCnt;
    DListHead waitingQueue;
    DListHead executingQueue;
    uint64_t nextTaskId;
    ConditionVariable cond;
    SchedulerState state;
};

typedef struct FIFOSchedulerOps FIFOSchedulerOps;
struct FIFOSchedulerOps {
    SchedulerOps super;
};

DECLARE_NEW_TYPED_CLASS(FIFOScheduler)

typedef struct SchedulerInitParams SchedulerInitParams;
struct SchedulerInitParams {
    MemAllocator *allocator;
    uint32_t maxQueueSize;
};

typedef enum TaskState { INITIAL = 1, EXECUTING, FINISHED } TaskState;

GSDB_END_C_CODE_DECLS

#endif // UTILS_FIFO_SCHEDULER_H
