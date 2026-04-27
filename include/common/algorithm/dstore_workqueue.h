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
 * dstore_workqueue.h
 *    Definition of a work queue.
 *    NOTE: all types in this file should be used in shared memory context!!!!
 *
 * Usage:
 * Step 1:
 *   create a work queue.
 * Step 2:
 *   create a work, stuff func/args/result of the work
 * Step 3:
 *   submit the work to your work queue, then wait until it is completed
 *
 * IDENTIFICATION
 *    src/gausskernel/dstore/include/common/algorithm/dstore_workqueue.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef DSTORE_WORKQUEUE_H
#define DSTORE_WORKQUEUE_H
#include "thread/thread_pool.h"
#include "common/memory/dstore_mctx.h"
#include "common/concurrent/dstore_atomic.h"
#include "common/concurrent/dstore_futex.h"
#include "common/dstore_datatype.h"
#include "common/algorithm/dstore_ilist.h"

namespace DSTORE {
class Work;
/* Work queue used by users, which is responsible for distributing works to a proper work queue on cpu. */
class WorkQueue : public BaseObject {
public:
    WorkQueue();
    DISALLOW_COPY_AND_MOVE(WorkQueue);
    ~WorkQueue();

    RetStatus Init();
    void Destroy();

    ThreadPool* GetThreadPool();
    void IncWorkNum();
    void DecWorkNum();
    RetStatus ScheduleWork(Work* work);

private:
    bool IsInited();
    bool IsDestroying();

    gs_atomic_uint32 inited;
    gs_atomic_uint32 destroying;
    gs_atomic_uint32 workNum;
    ThreadPool* threadPool;
};

struct Work : public BaseObject {
    using WorkFunc = void(*)(Work*);

    Work();
    DISALLOW_COPY_AND_MOVE(Work);
    ~Work();

    void Reset();
    /* Tell the user the work has been done. */
    void MarkCompleted();
    /* Wait the work to be done. */
    void WaitCompleted();

    /* If there is a long sleep in func, use NotifyThreadPoolTaskAlive before sleeping */
    WorkFunc func;
    void* args;
    void* result;
    DstoreFutex futex;
    WorkQueue* workQueue;
    TaskId taskId;
    ThreadPoolTask* task;
};

} /* namespace DSTORE */

#endif /* DSTORE_WORKQUEUE_H */