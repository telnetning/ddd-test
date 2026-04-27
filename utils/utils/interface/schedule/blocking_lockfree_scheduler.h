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
 * Description: Implement the header of blocking lock-free scheduler
 *
 * multi-queue scheduler use pthread_cond_signl/pthread_cond_timewait to mplement the waiting
 * notification mechanism, but pthread_cond_signal due to the mechanism of G1G2, it has a small flaw :
 * when G1G2 need switch at pthread_cond_signal flow chart, will get a lock which may hold by
 * pthread_cond_timewait, a scene occurs the producer needs to wait for consumer in reverse,
 * and the probability of occurrence not low.
 *
 * so, Implement a new scheduler to optimize the reverse wait problem, reduce glitches in task submit latency
 *
 * the solution is simply to spread the equivalent of cond_var to the elements of each task queue.
 * ensure that at most one thread is waiting at each waiting point. but our implementation does not
 * use condition variables but directly simple encapsulates futex
 */

#ifndef UTILS_BLOCKING_LOCKFREE_SCHEDULER_H
#define UTILS_BLOCKING_LOCKFREE_SCHEDULER_H

#include "container/linked_list.h"
#include "types/ref_object.h"
#include "defines/common.h"
#include "schedule/scheduler.h"

GSDB_BEGIN_C_CODE_DECLS

typedef struct TaskEntry TaskEntry;
typedef struct {
    /* on 64bit system, malloc memory always 8byte aligned, the last three bit alway zero.
       here, use the 3bit for other uses:
       bit 0: 1, the scheduler need quit
              0, the scheduler is normal running
       bit 1: 1, have consumer wait the slot, producer need wakeup it after success produced a task
              0, no consumer wait
       bit 2: 1, have producer wait the slot, consumer need wakeup it after taken out slot task

       other bit is an task entry address */
    TaskEntry *entry;
} BLFTaskQueue;

typedef struct {
    Scheduler super;
    MemAllocator *allocator;
    SchedulerState state;
    uint32_t taskQueueSize;
    uint32_t produceIndex;
    uint32_t consumeIndex;
    uint64_t lastExecTaskTime;
    BLFTaskQueue *queue;
} BLFScheduler;

typedef struct {
    ThreadPoolParams super;
    uint32_t taskQueueSize;
    MemAllocator *allocator;
} BLFSchedulerInitParams;

typedef struct BLFSchedulerOps {
    SchedulerOps super;
} BLFSchedulerOps;

DECLARE_NEW_TYPED_CLASS(BLFScheduler)

GSDB_END_C_CODE_DECLS

#endif // UTILS_BLOCKING_LOCKFREE_SCHEDULER_H
