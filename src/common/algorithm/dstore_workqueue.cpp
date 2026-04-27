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
 * dstore_workqueue.cpp
 *
 *
 *
 * IDENTIFICATION
 *        dstore/src/common/algorithm/dstore_workqueue.cpp
 *
 * ---------------------------------------------------------------------------------------*/
#include <sched.h>
#include "common/log/dstore_log.h"
#include "framework/dstore_instance.h"
#include "common/algorithm/dstore_workqueue.h"

constexpr uint16 MIN_THREAD_NUM = 128;
constexpr uint16 MAX_THREAD_NUM = 1024;
constexpr uint16 THREAD_POOL_IDLE_THRESHHOLD = 1000;
namespace DSTORE {
RetStatus WorkQueueCreateThreadAndRegister()
{
    if (thrd == nullptr) {
        thrd = DstoreNew(g_storageInstance->GetMemoryMgr()->GetGroupContext(DSTORE::MEMORY_CONTEXT_LONGLIVE))
            ThreadContext();
        StorageReleasePanic(thrd == nullptr, MODULE_RECOVERY,
            ErrMsg("WorkQueueCreateThreadAndRegister Alloc thrd failed."));
        if (STORAGE_FUNC_FAIL(thrd->InitializeBasic())) {
            ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("Failed to initialize basic thread context."));
            g_storageInstance->RemoveVisibleThread(thrd);
            thrd->Destroy();
            DstorePfreeExt(thrd);
            return DSTORE_FAIL;
        }
        (void)thrd->InitStorageContext(INVALID_PDB_ID);
        thrd->SetNeedCommBuffer(true);
        g_storageInstance->AddVisibleThread(thrd, INVALID_PDB_ID, "WorkQueueWorker");
    }
    return DSTORE_SUCC;
}

void WorkQueueUnregisterThread()
{
    if (thrd != nullptr) {
        g_storageInstance->RemoveVisibleThread(thrd);
        thrd->Destroy();
        DstorePfreeExt(thrd);
    }
}

void *WorkQueueAlloc(MemAllocator *self, uint64_t size)
{
    return DstoreMemoryContextAlloc(static_cast<DstoreMemoryContext>(self->context), size);
}

void WorkQueueFree(UNUSE_PARAM MemAllocator *self, void *ptr)
{
    DstorePfreeExt(ptr);
}

struct MemAllocator g_workQueueAllocator {
    .context = nullptr,
    .alloc = &WorkQueueAlloc,
    .free = &WorkQueueFree
} ;

WorkQueue::WorkQueue() : inited(0), destroying(0), workNum(0), threadPool(nullptr)
{}

WorkQueue::~WorkQueue()
{}

RetStatus WorkQueue::Init()
{
    ThreadPoolParams param;
    param.flags = THREAD_POOL_FLAG_SCALING | THREAD_POOL_FLAG_ENABLE_DETECT;
    param.minThrdCnt = MIN_THREAD_NUM;
    param.maxThrdCnt = MAX_THREAD_NUM;
    param.stackSize = 0;
    param.maxQueueSize = 0;
    param.idleTimeThreshold = THREAD_POOL_IDLE_THRESHHOLD;
    if (STORAGE_FUNC_FAIL(WorkQueueCreateThreadAndRegister())) {
        ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("Failed to create and register work queue thread."));
        return DSTORE_FAIL;
    } else {
        param.threadEnterCallback = (void (*)())&WorkQueueCreateThreadAndRegister;
    }
    param.threadExitCallback = &WorkQueueUnregisterThread;
    const int nameSize = 9;
    char name[] = "WorkQueue";
    errno_t rc = strcpy_s(STATIC_CAST_PTR_TYPE(&param.name, char*), nameSize + 1, name);
    storage_securec_check(rc, "\0", "\0");
    rc = memset_s(&param.cpuset, sizeof(cpu_set_t), 0, sizeof(cpu_set_t));
    storage_securec_check(rc, "\0", "\0");

    ErrorCode err;
    g_workQueueAllocator.context = g_storageInstance->GetMemoryMgr()->GetGroupContext(DSTORE::MEMORY_CONTEXT_LONGLIVE);
    threadPool = CreateThreadPool(&param, &g_workQueueAllocator, &err);
    int32 retryCnt = 0;
    while ((err != 0) || !threadPool) {
        constexpr int32 retryMax = 10;
        constexpr long sleepTime = 1000;
        StorageExit0(retryCnt++ > retryMax, MODULE_BUFMGR, ErrMsg("No resource for workqueue threadPool."));
        ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("CreateThreadPool failed err(%lld).", err));
        GaussUsleep(sleepTime);
        threadPool = CreateThreadPool(&param, &g_workQueueAllocator, &err);
    }

    (void)GsAtomicWriteU32(&inited, 1);
    return DSTORE_SUCC;
}

void WorkQueue::Destroy()
{
    (void)GsAtomicWriteU32(&destroying, 1);
    while (GsAtomicReadU32(&workNum) != 0) {
        const uint32 sleepTimeInUs = 1000;
        GaussUsleep(sleepTimeInUs);
    }
    
    DestroyThreadPool(threadPool);
}

ThreadPool* WorkQueue::GetThreadPool()
{
    return threadPool;
}

void WorkQueue::IncWorkNum()
{
    (void)GsAtomicFetchAddU32(&workNum, 1);
}

void WorkQueue::DecWorkNum()
{
    (void)GsAtomicFetchSubU32(&workNum, 1);
}

void* WorkQueueRoutine(void* args)
{
    Work* self = static_cast<Work*>(args);
    self->func(self);
    self->workQueue->DecWorkNum();
    self->MarkCompleted();
    return nullptr;
}

RetStatus WorkQueue::ScheduleWork(Work* work)
{
    if (!IsInited() || IsDestroying()) {
        return DSTORE_FAIL;
    }
    ThreadPoolTask* task = static_cast<ThreadPoolTask*>(DstoreMemoryContextAllocZero(
        g_storageInstance->GetMemoryMgr()->GetGroupContext(DSTORE::MEMORY_CONTEXT_LONGLIVE),
        sizeof(ThreadPoolTask)));
    if (!task) {
        ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("Create ThreadPoolTask failed."));
        return DSTORE_FAIL;
    }
    work->task = task;
    work->workQueue = this;
    task->args = work;
    task->routine = &WorkQueueRoutine;
    IncWorkNum();
    ErrorCode err = ScheduleThreadPoolTask(threadPool, task, &work->taskId);
    if (err != 0) {
        DecWorkNum();
        ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("ScheduleThreadPoolTask failed."));
        DstorePfreeExt(task);
        work->task = nullptr;
        return DSTORE_FAIL;
    }

    return DSTORE_SUCC;
}

bool WorkQueue::IsInited()
{
    return GsAtomicReadU32(&inited) == 1;
}

bool WorkQueue::IsDestroying()
{
    return GsAtomicReadU32(&destroying) == 1;
}

Work::Work() : func(nullptr), args(nullptr), result(nullptr),
    workQueue(nullptr), taskId(0), task(nullptr)
{
    futex.DstoreFutexInit();
}

Work::~Work()
{}

void Work::Reset()
{
    futex.DstoreFutexInit();
    workQueue = nullptr;
    task = nullptr;
}

void Work::MarkCompleted()
{
    futex.DstoreFutexPost();
}

void Work::WaitCompleted()
{
    futex.DstoreFutexWait(false);
    DstorePfreeExt(task);
}

}
