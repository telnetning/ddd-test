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
 * multiqueue_scheduler.h
 *
 * Description:
 * the internal header file of the multiqueue_scheduler.
 * ---------------------------------------------------------------------------------
 */

#ifndef GSDB_MULTIQUEUE_SCHEDULER_H
#define GSDB_MULTIQUEUE_SCHEDULER_H

#include "container/linked_list.h"
#include "types/ref_object.h"
#include "defines/common.h"
#include "schedule/scheduler.h"

GSDB_BEGIN_C_CODE_DECLS

typedef struct TaskQueue TaskQueue;
struct TaskQueue {
    SpinLock spinLock;
    DListHead taskQueue;
    uint32_t maxQueueSize;
    uint32_t waitingTaskCnt;
} __attribute__((aligned(GS_CACHE_LINE_SIZE)));

typedef struct MultiQueueScheduler MultiQueueScheduler;
struct MultiQueueScheduler {
    Scheduler super;
    MemAllocator *allocator;
    TaskQueue *queueArray;
    uint32_t taskQueueCnt;
    uint32_t lastProduceQueueIndex;
    uint32_t lastConsumeQueueIndex;
    Mutex lock;
    ConditionVariable cond;
    ConditionVariable waitSubmitCond;
    uint32_t waitSubmitCnt;
    SchedulerState state;
    uint32_t prorityTaskCnt;
};

typedef struct MultiQueueSchedulerOps MultiQueueSchedulerOps;
struct MultiQueueSchedulerOps {
    SchedulerOps super;
};

DECLARE_NEW_TYPED_CLASS(MultiQueueScheduler)

typedef struct MultiQueueSchedulerInitParams MultiQueueSchedulerInitParams;
struct MultiQueueSchedulerInitParams {
    ThreadPoolParams super;
    MemAllocator *allocator;
    uint32_t taskQueueCnt;
    uint32_t maxQueueSize;
};

GSDB_END_C_CODE_DECLS

#endif // GSDB_MULTIQUEUE_SCHEDULER_H
