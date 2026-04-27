
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
 * ut_threadpool.cpp
 * unit tests of thread pool
 *
 * ---------------------------------------------------------------------------------
 */

#include <gtest/gtest.h>
#include <unordered_map>
#include <atomic>
#include <thread>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <gmock/gmock.h>
#include <mockcpp/mockcpp.hpp>
#include "thread/thread_pool_internal.h"
#include "schedule/fifo_scheduler.h"
#include "container/linked_list.h"
#include "securec.h"
#include "syslog/err_log.h"
#include "defines/utils_errorcode.h"
#include "memory/memory_allocator.h"
#include "thread/thread_pool.h"
#include "schedule/multiqueue_scheduler.h"
#include "schedule/blocking_lockfree_scheduler.h"
#include "fault_injection/fault_injection.h"

using namespace std;

typedef struct TestThreadPoolCtx TestThreadPoolCtx;
struct TestThreadPoolCtx
{
    ThreadPool *pool;
    pthread_mutex_t lock_task;
    pthread_mutex_t lock_cnt;

    DListHead scheduleTaskQueue;
    DListHead taskIdQueue;

    uint64_t scheduleSuccessTaskCnt;
    uint64_t scheduleFailedTaskCnt;

    uint64_t cancelSuccessTaskCnt;
    uint64_t cancelExecutingTaskCnt;
    uint64_t cancelFinishTaskCnt;

    uint64_t routineCnt;
    uint64_t finalizeCnt;
    uint64_t cancelCnt;
};

typedef struct TestScheduleTaskEntry TestScheduleTaskEntry;
struct TestScheduleTaskEntry
{
    DListNode node;
    ThreadPoolTask task;
};

typedef struct TestTaskEntry TestTaskEntry;
struct TestTaskEntry
{
    DListNode node;
    uint64_t taskId;
};

/** ************************************************************************************************************* **/
static bool g_cancel_flag_long = false;
static bool g_cancel_flag_cnt_long = false;

void *TestMemAlloc(MemAllocator *self, uint64_t size)
{
    return malloc(size);
}

void TestMemFree(MemAllocator *self, void *ptr)
{
    if (ptr != NULL) {
        free(ptr);
    }
}

void *routine_cnt(void *arg)
{
    TestThreadPoolCtx *context = (TestThreadPoolCtx *)arg;
    pthread_mutex_lock(&context->lock_cnt);
    context->routineCnt++;
    pthread_mutex_unlock(&context->lock_cnt);

    ErrLog(DEBUG, ErrMsg("[test] routine_cnt EXIT. \n"));
    return nullptr;
}

void *routine_null(void *arg)
{
    ErrLog(DEBUG, ErrMsg("[test] routine_null EXIT. \n"));
    return nullptr;
}

void *routine_null_long(void *arg)
{
    while (!g_cancel_flag_long) {
        ErrLog(DEBUG, ErrMsg("[test] routine_long threadId : %lu, hang~~~ \n", pthread_self()));
        usleep(100000);
    }

    ErrLog(DEBUG, ErrMsg("[test] routine_null_delay EXIT. \n"));
    return NULL;
}

void *routine_Cnt_Long(void *arg) {
    TestThreadPoolCtx *context = (TestThreadPoolCtx *)arg;
    pthread_mutex_lock(&context->lock_cnt);
    context->routineCnt++;
    pthread_mutex_unlock(&context->lock_cnt);

    while (!g_cancel_flag_cnt_long) {
        ErrLog(DEBUG, ErrMsg("[test] routine_long threadId : %lu, hang~~~ \n", pthread_self()));
        usleep(100000);
    }

    ErrLog(DEBUG, ErrMsg("[test] routine_long EXIT. \n"));
    return NULL;
}

void *routine_cnt_1s(void *arg) {
    TestThreadPoolCtx *context = (TestThreadPoolCtx *)arg;
    pthread_mutex_lock(&context->lock_cnt);
    context->routineCnt++;
    pthread_mutex_unlock(&context->lock_cnt);

    usleep(1000000);

    ErrLog(DEBUG, ErrMsg("[test] routine_long EXIT. \n"));
    return NULL;
}

void cancel_null(void *arg)
{
    ErrLog(DEBUG, ErrMsg("[test] cancel_null :Cancel threadID : %lu, ~~ \n", pthread_self()));
}

void cancel_null_long(void *arg)
{
    ErrLog(DEBUG, ErrMsg("[test] cancel_null_long :Cancel threadID : %lu, ~~ \n", pthread_self()));
    g_cancel_flag_long = true;
}

void cancel_Cnt_long(void *arg)
{
    TestThreadPoolCtx *context = (TestThreadPoolCtx *)arg;
    pthread_mutex_lock(&context->lock_cnt);
    context->cancelCnt++;
    pthread_mutex_unlock(&context->lock_cnt);
    ErrLog(DEBUG, ErrMsg("[test] cancel_2 :Cancel threadID : %lu, ~~ \n", pthread_self()));

    g_cancel_flag_cnt_long = true;
}

void finalize_cnt(void *arg) {

    TestThreadPoolCtx *context = (TestThreadPoolCtx *)arg;
    pthread_mutex_lock(&context->lock_cnt);
    context->finalizeCnt++;
    pthread_mutex_unlock(&context->lock_cnt);
    ErrLog(DEBUG, ErrMsg("[test] finalize_cnt : threadID : %lu, ~~ \n", pthread_self()));
}

void finalize_null(void *arg) {
    ErrLog(DEBUG, ErrMsg("[test] finalize_null : threadID : %lu, ~~ \n", pthread_self()));
}

void finalize_Cnt(void *arg) {
    TestThreadPoolCtx *context = (TestThreadPoolCtx *)arg;
    pthread_mutex_lock(&context->lock_cnt);
    context->finalizeCnt++;
    pthread_mutex_unlock(&context->lock_cnt);

    ErrLog(DEBUG, ErrMsg("[test] finalize_1 : threadID : %lu, ~~ \n", pthread_self()));
}

/** ************************************************************************************************************* **/

class ThreadPoolTest : public testing::Test {
public:
    static void SetUpTestSuite()
    {
        ErrorCode err = ERROR_SYS_OK;
        /* default init  */
        err = StartLogger();
        ASSERT_EQ(err, ERROR_SYS_OK);
        err = OpenLogger();
        ASSERT_EQ(err, ERROR_SYS_OK);
    }

    static void TearDownTestSuite()
    {
        /* default init  */
        usleep(100);
        CloseLogger();
        StopLogger();

        DestroyFaultInjectionHash(FI_GLOBAL); // last clear
    }

    void SetUp() override {
        g_cancel_flag_long = false;
        g_cancel_flag_cnt_long = false;
    };

    void TearDown() override {
        g_cancel_flag_cnt_long = false;
        GlobalMockObject::verify();
    };

    ErrorCode errCode_ = ERROR_SYS_OK;
    const ThreadPoolParams tpParams_ { /* default paramters for thread pool */
        .flags = THREAD_POOL_FLAG_SCALING,
        .minThrdCnt = 20,
        .maxThrdCnt = 50,
        .stackSize = 0,
        .maxQueueSize = 50000,
        .idleTimeThreshold = 0,
        .threadEnterCallback = nullptr,
        .threadExitCallback = nullptr,
        {.name = "ut_threadpool"},
        .cpuset = {},
    };
    const MultiQueueSchedulerInitParams mqParams_ { /* default paramters for multiqueue scheduler */
        .super = {},
        .allocator = nullptr,
        .taskQueueCnt = 50,
        .maxQueueSize = 50000,
    };
};

void buildThreadPoolParams(ThreadPoolParams *params, uint32_t flags, uint32_t minThrdCnt, uint32_t maxThrdCnt,
    uint32_t stackSize, uint32_t maxQueueSize, uint32_t idleTimeThreshold) {
    params->flags = flags;
    params->minThrdCnt = minThrdCnt;
    params->maxThrdCnt = maxThrdCnt;
    params->stackSize = stackSize;
    params->maxQueueSize = maxQueueSize;
    params->idleTimeThreshold = idleTimeThreshold;
    params->threadEnterCallback = NULL;
    params->threadExitCallback = NULL;
}

void buildTask(ThreadPoolTask *task, void *(*routine)(void *args), void *(*run)(void **args),
               void (*finalize)(void *args), void (*cancel)(void *args), void *args) {
    task->routine = routine;
    task->run = run;
    task->finalize = finalize;
    task->cancel = cancel;
    task->args = args;
}

TEST_F(ThreadPoolTest, CreateThreadPoolTest)
{
    MemAllocator *testMemAllocator = (MemAllocator *)malloc(sizeof(MemAllocator));
    testMemAllocator->alloc = TestMemAlloc;
    testMemAllocator->free = TestMemFree;

    // params is null.
    ErrorCode errCode = ERROR_SYS_OK;
    ThreadPool *pool = CreateThreadPool(NULL, testMemAllocator, &errCode);
    ASSERT_TRUE(pool == NULL);
    ASSERT_TRUE(errCode == UTILS_ERROR_THREADPOOL_NULLPARAMETER);

    ThreadPoolParams params;
    // flags is other
    buildThreadPoolParams(&params, 0x00000003, 0, 0, 0, 0, 0);
    pool = CreateThreadPool(&params, testMemAllocator, &errCode);
    ASSERT_TRUE(pool == NULL);
    ASSERT_EQ(errCode, UTILS_ERROR_THREADPOOL_PARAMETERERROR);

    // minThrdCnt == 0
    buildThreadPoolParams(&params, THREAD_POOL_FLAG_FIXED, 0, 10, 0, 1000, 0);
    pool = CreateThreadPool(&params, testMemAllocator, &errCode);
    ASSERT_TRUE(pool == NULL);
    ASSERT_TRUE(errCode == UTILS_ERROR_THREADPOOL_PARAMETERERROR);

    // maxThrdCnt > 1000
    buildThreadPoolParams(&params, THREAD_POOL_FLAG_SCALING, 1, 1001, 0, 1000, 0);
    pool = CreateThreadPool(&params, testMemAllocator, &errCode);
    ASSERT_NE(pool, nullptr);
    ASSERT_EQ(errCode, ERROR_SYS_OK);
    DestroyThreadPool(pool);
    // maxThrdCnt > 1000
    buildThreadPoolParams(&params, THREAD_POOL_FLAG_SCALING, 1, (uint32_t)-1, 0, 1000, 0);
    pool = CreateThreadPool(&params, testMemAllocator, &errCode);
    ASSERT_NE(pool, nullptr);
    ASSERT_EQ(errCode, ERROR_SYS_OK);
    DestroyThreadPool(pool);

    // minThrdCnt > maxThrdCnt
    buildThreadPoolParams(&params, THREAD_POOL_FLAG_SCALING, 20, 0, 0, 1000, 0);
    pool = CreateThreadPool(&params, testMemAllocator, &errCode);
    ASSERT_TRUE(pool == NULL);
    ASSERT_TRUE(errCode == UTILS_ERROR_THREADPOOL_PARAMETERERROR);

    // 0 < stackSize < 16384
    buildThreadPoolParams(&params, THREAD_POOL_FLAG_FIXED, 10, 20, 1024, 1000, 0);
    pool = CreateThreadPool(&params, testMemAllocator, &errCode);
    ASSERT_TRUE(pool == NULL);
    ASSERT_TRUE(errCode == UTILS_ERROR_THREADPOOL_PARAMETERERROR);

    // maxQueueSize > 10000000
    buildThreadPoolParams(&params, THREAD_POOL_FLAG_FIXED, 10, 20, 0, 10000001, 0);
    pool = CreateThreadPool(&params, testMemAllocator, &errCode);
    ASSERT_TRUE(pool == NULL);
    ASSERT_TRUE(errCode == UTILS_ERROR_THREADPOOL_PARAMETERERROR);

    // normal
    buildThreadPoolParams(&params, THREAD_POOL_FLAG_FIXED, 10, 20, 0, 1000, 0);
    pool = CreateThreadPool(&params, testMemAllocator, &errCode);
    ASSERT_TRUE(pool != NULL);
    ASSERT_TRUE(errCode == ERROR_SYS_OK);

    DestroyThreadPool(pool);
    free(testMemAllocator);
}

TEST_F(ThreadPoolTest, CreateThreadPoolEXTest)
{
    MemAllocator *testMemAllocator = (MemAllocator *)malloc(sizeof(MemAllocator));
    testMemAllocator->alloc = TestMemAlloc;
    testMemAllocator->free = TestMemFree;

    // params is null.
    ErrorCode errCode = ERROR_SYS_OK;
    SchedulerInitParams initParam = {
            .allocator = testMemAllocator,
            .maxQueueSize = 1000
    };
    FIFOScheduler *fifoScheduler = NewFIFOScheduler((TypeInitParams *)&initParam, &errCode);
    ThreadPool *pool = CreateThreadPoolEX(NULL, (Scheduler *)fifoScheduler, testMemAllocator, &errCode);
    ASSERT_TRUE(pool == NULL);
    ASSERT_TRUE(errCode == UTILS_ERROR_THREADPOOL_NULLPARAMETER);

    ThreadPoolParams params;
    // flags is other
    buildThreadPoolParams(&params, 0x00000003, 0, 0, 0, 0, 0);

    pool = CreateThreadPoolEX(&params, (Scheduler *)fifoScheduler, testMemAllocator, &errCode);
    ASSERT_TRUE(pool == NULL);
    ASSERT_EQ(errCode, UTILS_ERROR_THREADPOOL_PARAMETERERROR);

    // fixed & minThrdCnt == 0
    buildThreadPoolParams(&params, THREAD_POOL_FLAG_FIXED, 0, 10, 0, 1000, 0);

    pool = CreateThreadPoolEX(&params, (Scheduler *)fifoScheduler, testMemAllocator, &errCode);
    ASSERT_TRUE(pool == NULL);
    ASSERT_TRUE(errCode == UTILS_ERROR_THREADPOOL_PARAMETERERROR);

    // minThrdCnt > maxQueueSize
    buildThreadPoolParams(&params, THREAD_POOL_FLAG_FIXED, 20, 0, 0, 1000, 0);

    pool = CreateThreadPoolEX(&params, (Scheduler *)fifoScheduler, testMemAllocator, &errCode);
    ASSERT_TRUE(pool == NULL);
    ASSERT_TRUE(errCode == UTILS_ERROR_THREADPOOL_PARAMETERERROR);

    // 0 < stackSize < 16384
    buildThreadPoolParams(&params, THREAD_POOL_FLAG_FIXED, 10, 20, 1024, 1000, 0);

    pool = CreateThreadPoolEX(&params, (Scheduler *)fifoScheduler, testMemAllocator, &errCode);
    ASSERT_TRUE(pool == NULL);
    ASSERT_TRUE(errCode == UTILS_ERROR_THREADPOOL_PARAMETERERROR);

    // Scheduler is NULL
    buildThreadPoolParams(&params, THREAD_POOL_FLAG_FIXED, 10, 20, 0, 1000, 0);
    pool = CreateThreadPoolEX(&params, NULL, testMemAllocator, &errCode);
    ASSERT_TRUE(pool == NULL);
    ASSERT_TRUE(errCode == UTILS_ERROR_THREADPOOL_NULLPARAMETER);

    // normal
    buildThreadPoolParams(&params, THREAD_POOL_FLAG_FIXED, 10, 20, 0, 1000, 0);
    pool = CreateThreadPoolEX(&params, (Scheduler *)fifoScheduler, testMemAllocator, &errCode);
    ASSERT_TRUE(pool != NULL);
    ASSERT_TRUE(errCode == ERROR_SYS_OK);

    DestroyThreadPool(pool);
    free(testMemAllocator);
}

TEST_F(ThreadPoolTest, CreateThreadPoolByFault)
{
    FaultInjectionEntry entries[] = {
        FAULT_INJECTION_ENTRY(START_THREADPOOL_FAULT_INJECTION, false, nullptr),
    };
    ASSERT_EQ(RegisterFaultInjection(entries, sizeof(entries) / sizeof(entries[0]), FI_GLOBAL), ERROR_SYS_OK);
    FAULT_INJECTION_ACTIVE(START_THREADPOOL_FAULT_INJECTION, FI_GLOBAL);

    ThreadPool *pool = nullptr;
    ErrorCode errCode = ERROR_SYS_OK;
    MemAllocator testMemAllocator = {0};
    testMemAllocator.alloc = TestMemAlloc;
    testMemAllocator.free = TestMemFree;
    // normal param
    ThreadPoolParams params;
    buildThreadPoolParams(&params, THREAD_POOL_FLAG_FIXED, 10, 20, 0, 1000, 0);
    pool = CreateThreadPool(&params, &testMemAllocator, &errCode);
    ASSERT_EQ(pool, nullptr);
    // normal param
    SchedulerInitParams fParam = {
            .allocator = &testMemAllocator,
            .maxQueueSize = 1000
    };
    FIFOScheduler *fifoScheduler = NewFIFOScheduler((TypeInitParams *)&fParam, &errCode);
    ASSERT_NE(fifoScheduler, nullptr);

    pool = CreateThreadPoolEX(&params, (Scheduler *)fifoScheduler, &testMemAllocator, &errCode);
    ASSERT_EQ(pool, nullptr);
    SchedulerDestroy((Scheduler *)fifoScheduler);
    RefObjectKillAndDecRef(UP_TYPE_CAST(fifoScheduler, RefObject));
    // normal param
    MultiQueueSchedulerInitParams mParam = {
        .super = params,
        .allocator = &testMemAllocator,
        .taskQueueCnt = 100,
        .maxQueueSize = 10000000,
    };
    errCode = ERROR_SYS_OK;
    MultiQueueScheduler *multiScheduler = NewMultiQueueScheduler((TypeInitParams *)&mParam, &errCode);
    ASSERT_NE(multiScheduler, nullptr);

    pool = CreateThreadPoolEX(&params, (Scheduler *)multiScheduler, &testMemAllocator, &errCode);
    ASSERT_EQ(pool, nullptr);
    SchedulerDestroy((Scheduler *)multiScheduler);
    RefObjectKillAndDecRef(UP_TYPE_CAST(multiScheduler, RefObject));
    FAULT_INJECTION_INACTIVE(START_THREADPOOL_FAULT_INJECTION, FI_GLOBAL);
}

TEST_F(ThreadPoolTest, ScheduleTaskTest)
{
    MemAllocator *testMemAllocator = (MemAllocator *)malloc(sizeof(MemAllocator));
    testMemAllocator->alloc = TestMemAlloc;
    testMemAllocator->free = TestMemFree;

    ThreadPoolParams params;
    buildThreadPoolParams(&params, THREAD_POOL_FLAG_FIXED, 10, 20, 0, 1000, 0);

    ErrorCode errCode = ERROR_SYS_OK;
    SchedulerInitParams initParam = {
            .allocator = testMemAllocator,
            .maxQueueSize = 1000
    };
    FIFOScheduler *fifoScheduler = NewFIFOScheduler((TypeInitParams *)&initParam, &errCode);
    ThreadPool *pool = CreateThreadPoolEX(&params, (Scheduler *)fifoScheduler, testMemAllocator, &errCode);
    ASSERT_TRUE(pool != NULL);
    ASSERT_TRUE(errCode == ERROR_SYS_OK);

    // pool is NULL
    TaskId taskId0 = THREAD_POOL_INVALID_TASK_ID;
    errCode = ScheduleThreadPoolTask(NULL, NULL, &taskId0);
    ASSERT_TRUE(errCode == UTILS_ERROR_THREADPOOL_NULLPARAMETER);
    ASSERT_TRUE(taskId0 == THREAD_POOL_INVALID_TASK_ID);

    // task is NULL
    TaskId taskId = THREAD_POOL_INVALID_TASK_ID;
    errCode = ScheduleThreadPoolTask(pool, NULL, &taskId);
    ASSERT_TRUE(errCode == UTILS_ERROR_THREADPOOL_ERRTASK);
    ASSERT_TRUE(taskId == THREAD_POOL_INVALID_TASK_ID);

    // task's routine is NULL
    ThreadPoolTask task;
    buildTask(&task, NULL, NULL, NULL, NULL, NULL);

    errCode = ScheduleThreadPoolTask(pool, &task, &taskId);
    ASSERT_TRUE(errCode == UTILS_ERROR_THREADPOOL_ERRTASK);
    ASSERT_TRUE(taskId == THREAD_POOL_INVALID_TASK_ID);

    // task is normal
    buildTask(&task, routine_null, NULL, finalize_null, cancel_null, NULL);

    errCode = ScheduleThreadPoolTask(pool, &task, &taskId);
    ASSERT_TRUE(errCode == ERROR_SYS_OK);
    ASSERT_TRUE(taskId != THREAD_POOL_INVALID_TASK_ID);

    DestroyThreadPool(pool);
    free(testMemAllocator);
}

TEST_F(ThreadPoolTest, ThreadPoolSetName)
{
    static auto getThreadName = []()->std::string {
        char name[THREAD_NAME_LEN] = {};
        EXPECT_EQ(pthread_getname_np(pthread_self(), name, sizeof(name)), 0);
        return name;
    };
    auto processName = getThreadName();
    std::unordered_map<std::string, std::string> testcase = {
        // input   ,            expect
        {"TestPool",            "TestPool"},
        {"TestPoolxxxxxxxxxxx", processName},
        {"",                    processName},
    };

    for (auto &tcase : testcase) {
        auto input = tcase.first;
        auto expect = tcase.second;

        ErrorCode errCode = ERROR_SYS_OK;
        // normal param
        // flags; minThrdCnt; maxThrdCnt; stackSize; maxQueueSize; idleTimeThreshold; threadEnterCallback; threadExitCallback; name[16]; cpuset;
        ThreadPoolParams params {THREAD_POOL_FLAG_SCALING, 2, 5, 0, 1000, 0, nullptr, nullptr};
        auto copyLen = input.length() >= sizeof(params.name) ? sizeof(params.name) : input.length() + 1;
        memcpy_s(params.name, sizeof(params.name), input.c_str(), copyLen);
        auto pool = CreateThreadPool(&params, nullptr, &errCode);
        ASSERT_NE(pool, nullptr);

        // task is normal
        static auto routine = [](void *arg)->void *{
            EXPECT_EQ(strcmp(getThreadName().c_str(), (const char *)arg), 0);
            return nullptr;
        };
        ThreadPoolTask task {
            .routine = routine,
            .run = nullptr,
            .finalize = nullptr,
            .cancel = nullptr,
            .args = (void *)expect.c_str(),
        };

        for (auto i = 0; i < 30; i++) {
            TaskId taskId = THREAD_POOL_INVALID_TASK_ID;
            ASSERT_EQ(ScheduleThreadPoolTask(pool, &task, &taskId), ERROR_SYS_OK);
            ASSERT_NE(taskId, THREAD_POOL_INVALID_TASK_ID);
        }

        DestroyThreadPool(pool);
    }
}

TEST_F(ThreadPoolTest, CancelTaskTest)
{
    MemAllocator *testMemAllocator = (MemAllocator *)malloc(sizeof(MemAllocator));
    testMemAllocator->alloc = TestMemAlloc;
    testMemAllocator->free = TestMemFree;

    ErrorCode errCode = ERROR_SYS_OK;
    ThreadPoolParams params;
    buildThreadPoolParams(&params, THREAD_POOL_FLAG_FIXED, 1, 20, 0, 1000, 0);
    SchedulerInitParams initParam = {
            .allocator = testMemAllocator,
            .maxQueueSize = 1000
    };
    FIFOScheduler *fifoScheduler = NewFIFOScheduler((TypeInitParams *)&initParam, &errCode);
    ThreadPool *pool = CreateThreadPoolEX(&params, (Scheduler *)fifoScheduler, testMemAllocator, &errCode);
    ASSERT_TRUE(pool != NULL);
    ASSERT_TRUE(errCode == ERROR_SYS_OK);

    ThreadPoolTask task;
    buildTask(&task, routine_null_long, NULL, finalize_null, cancel_null_long, NULL);

    TaskId taskId = THREAD_POOL_INVALID_TASK_ID;
    errCode = ScheduleThreadPoolTask(pool, &task, &taskId);
    ASSERT_TRUE(errCode == ERROR_SYS_OK);
    ASSERT_TRUE(taskId != THREAD_POOL_INVALID_TASK_ID);

    TaskId taskId2 = THREAD_POOL_INVALID_TASK_ID;
    errCode = ScheduleThreadPoolTask(pool, &task, &taskId2);
    ASSERT_TRUE(errCode == ERROR_SYS_OK);
    ASSERT_TRUE(taskId2 != THREAD_POOL_INVALID_TASK_ID);
    // 需要等待一段时间让任务被线程抢到执行
    usleep(10000);
    // task is waiting
    errCode = CancelThreadPoolTask(pool, taskId2);
    ASSERT_TRUE(errCode == ERROR_SYS_OK);

    TaskId taskId3 = THREAD_POOL_INVALID_TASK_ID;
    errCode = ScheduleThreadPoolTask(pool, &task, &taskId3);
    ASSERT_TRUE(errCode == ERROR_SYS_OK);
    ASSERT_TRUE(taskId3 != THREAD_POOL_INVALID_TASK_ID);

    usleep(10000);
    // task is executing
    errCode = CancelThreadPoolTask(pool, taskId);
    ASSERT_TRUE(errCode == UTILS_ERROR_THREADPOOL_TASKISEXECUTING);

    // pool is NULL
    errCode = CancelThreadPoolTask(NULL, taskId);
    ASSERT_TRUE(errCode == UTILS_ERROR_THREADPOOL_NULLPARAMETER);

    // task is not exist
    taskId = THREAD_POOL_INVALID_TASK_ID;
    errCode = ERROR_SYS_OK;
    errCode = CancelThreadPoolTask(pool, taskId);
    ASSERT_TRUE(errCode == UTILS_ERROR_THREADPOOL_TASKNOTEXIST);

    DestroyThreadPool(pool);
    free(testMemAllocator);
}

TEST_F(ThreadPoolTest, DestroyPoolTest)
{
    MemAllocator *testMemAllocator = (MemAllocator *)malloc(sizeof(MemAllocator));
    testMemAllocator->alloc = TestMemAlloc;
    testMemAllocator->free = TestMemFree;

    ErrorCode errCode = ERROR_SYS_OK;
    ThreadPoolParams params;
    buildThreadPoolParams(&params, THREAD_POOL_FLAG_FIXED, 10, 20, 0, 1000, 0);

    SchedulerInitParams initParam = {
            .allocator = testMemAllocator,
            .maxQueueSize = 1000
    };
    FIFOScheduler *fifoScheduler = NewFIFOScheduler((TypeInitParams *)&initParam, &errCode);
    ThreadPool *pool = CreateThreadPoolEX(&params, (Scheduler *)fifoScheduler, testMemAllocator, &errCode);
    ASSERT_TRUE(pool != NULL);
    ASSERT_TRUE(errCode == ERROR_SYS_OK);

    // normal pool
    DestroyThreadPool(pool);
    free(testMemAllocator);
}

TestThreadPoolCtx *GetTestThreadPoolCtx(ThreadPool *pool, DListHead *scheduleTaskQueue) {
    TestThreadPoolCtx *context = (TestThreadPoolCtx *)malloc(sizeof(TestThreadPoolCtx));
    int ret = pthread_mutex_init(&context->lock_task, NULL);
    if (ret != 0) {
        ErrLog(DEBUG, ErrMsg("[test] pthread_mutex_init lock_a failed, ret = %d \n", ret));
    }
    ret = pthread_mutex_init(&context->lock_cnt, NULL);
    if (ret != 0) {
        ErrLog(DEBUG, ErrMsg("[test] pthread_mutex_init lock_cnt failed, ret = %d \n", ret));
    }

    DListInit(&context->scheduleTaskQueue);
    DListInit(&context->taskIdQueue);

    context->pool = pool;

    while (!DListIsEmpty(scheduleTaskQueue)) {
        DListNode *node = DListPopHeadNode(scheduleTaskQueue);
        DListPushTail(&context->scheduleTaskQueue, node);
    }

    context->scheduleSuccessTaskCnt = 0;
    context->scheduleFailedTaskCnt = 0;

    context->cancelSuccessTaskCnt = 0;
    context->cancelExecutingTaskCnt = 0;
    context->cancelFinishTaskCnt = 0;

    context->routineCnt = 0;
    context->finalizeCnt = 0;
    context->cancelCnt = 0;

    return context;
}

void *TestScheduleTask(void *context) {
    TestThreadPoolCtx *testContext = (TestThreadPoolCtx *) context;

    DListNode *pos = NULL;
    TaskId taskId;
    ErrorCode code = ERROR_SYS_OK;
    TestScheduleTaskEntry *scheduleTaskEntry = NULL;
    pthread_mutex_lock(&testContext->lock_task);
    DListHead *scheduleTaskQueue = &testContext->scheduleTaskQueue;
    if (DListIsEmpty(scheduleTaskQueue)) {
        ErrLog(DEBUG, ErrMsg("[test] TestScheduleTask error: threadId = %lu, the task queue is empty \n", pthread_self()));
        return NULL;
    }

    pos = DListPopHeadNode(&testContext->scheduleTaskQueue);
    scheduleTaskEntry = DLIST_CONTAINER(TestScheduleTaskEntry, node, pos);
    scheduleTaskEntry->task.args = testContext;
    pthread_mutex_unlock(&testContext->lock_task);

    for (int i = 0; i < 50; i++) {
        code = ScheduleThreadPoolTask(testContext->pool, &scheduleTaskEntry->task, &taskId);
        TestTaskEntry *testTaskEntry = (TestTaskEntry *)malloc(sizeof(TestTaskEntry));
        testTaskEntry->taskId = taskId;
        testTaskEntry->node.next = NULL;
        pthread_mutex_lock(&testContext->lock_task);
        if (code == ERROR_SYS_OK) {
            testContext->scheduleSuccessTaskCnt++;
            DListPushTail(&testContext->taskIdQueue, &testTaskEntry->node);
            ErrLog(DEBUG, ErrMsg("[test] TestScheduleTask success threadId: %lu, taskId : %lu \n", pthread_self(), taskId));
        } else {
            free(testTaskEntry);
            testContext->scheduleFailedTaskCnt++;
            ErrLog(DEBUG, ErrMsg("[test] TestScheduleTask failed threadId : %lu, error code : %lld \n", pthread_self(), code));
        }
        pthread_mutex_unlock(&testContext->lock_task);
        usleep(5000);
    }
    free(scheduleTaskEntry);
    CloseLogger();
    return NULL;
}

static void FreeTestTaskQueue(DListHead *head) {
    TestTaskEntry *taskEntry = NULL;
    DListMutableIter iter;
    DLIST_MODIFY_FOR_EACH(iter, head) {
        taskEntry = DLIST_CONTAINER(TestTaskEntry, node, iter.cur);
        if (taskEntry == NULL) {
            continue;
        }
        DListDelete(iter.cur);
        free(taskEntry);
    }
}

TEST_F(ThreadPoolTest, ConcurrentTest_FLAG_FIXED)
{
    MemAllocator *testMemAllocator = (MemAllocator *)malloc(sizeof(MemAllocator));
    testMemAllocator->alloc = TestMemAlloc;
    testMemAllocator->free = TestMemFree;

    uint32_t minThrdCnt = 10;
    uint32_t maxThrdCnt = 20;
    uint32_t stackSize = 0;
    uint32_t maxQueueSize = 20;
    uint32_t idleTimeThreshold = 0;
    uint32_t taskNum = 50 * 2;

    ThreadPoolParams params;
    buildThreadPoolParams(&params, THREAD_POOL_FLAG_FIXED, minThrdCnt, maxThrdCnt, stackSize, maxQueueSize, idleTimeThreshold);

    ErrorCode errCode = ERROR_SYS_OK;
    ThreadPool *pool = CreateThreadPool(&params, testMemAllocator, &errCode);
    ASSERT_TRUE(pool != nullptr);

    DListHead scheduleTaskQueue;
    DListInit(&scheduleTaskQueue);

    ThreadPoolTask task;
    buildTask(&task, routine_Cnt_Long, NULL, finalize_Cnt, cancel_Cnt_long, NULL);

    TestScheduleTaskEntry *taskEntry1 = (TestScheduleTaskEntry *)malloc(sizeof (TestScheduleTaskEntry));
    taskEntry1->task = task;
    taskEntry1->node.next = NULL;

    TestScheduleTaskEntry *taskEntry2 = (TestScheduleTaskEntry *)malloc(sizeof (TestScheduleTaskEntry));
    taskEntry2->task = task;
    taskEntry2->node.next = NULL;

    DListPushTail(&scheduleTaskQueue, &taskEntry1->node);
    DListPushTail(&scheduleTaskQueue, &taskEntry2->node);
    TestThreadPoolCtx *context = GetTestThreadPoolCtx(pool, &scheduleTaskQueue);

    pthread_t worker_t1, worker_t2;
    pthread_create(&worker_t1, NULL, TestScheduleTask, context);
    pthread_create(&worker_t2, NULL, TestScheduleTask, context);

    pthread_join(worker_t1, NULL);
    pthread_join(worker_t2, NULL);

    ErrLog(DEBUG, ErrMsg("[test] ConcurrentTest_FLAG_FIXED scheduleSuccessTaskCnt = %lu \n", context->scheduleSuccessTaskCnt));
    ErrLog(DEBUG, ErrMsg("[test] ConcurrentTest_FLAG_FIXED scheduleFailedTaskCnt = %lu \n", context->scheduleFailedTaskCnt));
    ASSERT_TRUE(context->scheduleSuccessTaskCnt == minThrdCnt + maxQueueSize);
    ASSERT_TRUE(context->scheduleFailedTaskCnt + context->scheduleSuccessTaskCnt == taskNum);

    DestroyThreadPool(pool);

    ErrLog(DEBUG, ErrMsg("[test] ConcurrentTest_FLAG_FIXED routineCnt = %lu \n", context->routineCnt));
    ErrLog(DEBUG, ErrMsg("[test] ConcurrentTest_FLAG_FIXED finalizeCnt = %lu \n", context->finalizeCnt));
    ErrLog(DEBUG, ErrMsg("[test] ConcurrentTest_FLAG_FIXED minThrdCnt = %u \n", minThrdCnt));
    ASSERT_TRUE(context->routineCnt == minThrdCnt);
    ASSERT_TRUE(context->finalizeCnt == minThrdCnt + maxQueueSize);

    FreeTestTaskQueue(&context->taskIdQueue);
    free(testMemAllocator);
    free(context);
}

void *TestCancelTask(void *context) {
    TestThreadPoolCtx *testContext = (TestThreadPoolCtx *) context;

    DListHead *taskIdQueue = &testContext->taskIdQueue;

    TestTaskEntry *taskEntry = NULL;
    DListMutableIter iter;
    ErrorCode code = ERROR_SYS_OK;
    DLIST_MODIFY_FOR_EACH(iter, taskIdQueue)
    {
        taskEntry = DLIST_CONTAINER(TestTaskEntry, node, iter.cur);
        ErrLog(DEBUG, ErrMsg("[test] TestCancelTask threadId: %ld, taskId : %lu \n", pthread_self(), taskEntry->taskId));
        code = CancelThreadPoolTask(testContext->pool, taskEntry->taskId);

        pthread_mutex_lock(&testContext->lock_cnt);
        if (code == ERROR_SYS_OK) {
            testContext->cancelSuccessTaskCnt++;
            ErrLog(DEBUG, ErrMsg("[test] TestCancelTask success threadId: %lu, taskId : %lu \n", pthread_self(), taskEntry->taskId));
        } else if (code == UTILS_ERROR_THREADPOOL_TASKISEXECUTING) {
            testContext->cancelExecutingTaskCnt++;
            ErrLog(DEBUG, ErrMsg("[test] TestCancelTask failed taskId: %lu, Executing  \n", taskEntry->taskId));
        } else if (code == UTILS_ERROR_THREADPOOL_TASKNOTEXIST) {
            testContext->cancelFinishTaskCnt++;
            ErrLog(DEBUG, ErrMsg("[test] TestCancelTask failed taskId: %lu, finish \n", taskEntry->taskId));
        } else {
            ErrLog(DEBUG, ErrMsg("[test] TestCancelTask failed taskId: %lu \n", taskEntry->taskId));
        }

        pthread_mutex_unlock(&testContext->lock_cnt);
    }
    CloseLogger();
    return NULL;
}

TEST_F(ThreadPoolTest, ConcurrentTest_Cancel_test)
{
    uint32_t minThrdCnt = 10;
    uint32_t maxThrdCnt = 20;
    uint32_t stackSize = 0;
    uint32_t maxQueueSize = 50;
    uint32_t idleTimeThreshold = 0;
    uint32_t taskNum = 50 * 2;

    ThreadPoolParams params;
    buildThreadPoolParams(&params, THREAD_POOL_FLAG_FIXED, minThrdCnt, maxThrdCnt, stackSize, maxQueueSize, idleTimeThreshold);

    MemAllocator *testMemAllocator = (MemAllocator *)malloc(sizeof(MemAllocator));
    testMemAllocator->alloc = TestMemAlloc;
    testMemAllocator->free = TestMemFree;

    ErrorCode errCode = ERROR_SYS_OK;
    SchedulerInitParams initParam = {
            .allocator = testMemAllocator,
            .maxQueueSize = maxQueueSize
    };
    FIFOScheduler *fifoScheduler = NewFIFOScheduler((TypeInitParams *)&initParam, &errCode);
    ThreadPool *pool = CreateThreadPoolEX(&params, (Scheduler *)fifoScheduler, testMemAllocator, &errCode);
    ASSERT_TRUE(pool != nullptr);

    DListHead scheduleTaskQueue;
    DListInit(&scheduleTaskQueue);

    ThreadPoolTask task;
    buildTask(&task, routine_Cnt_Long, NULL, finalize_Cnt, cancel_Cnt_long, NULL);

    TestScheduleTaskEntry *taskEntry1 = (TestScheduleTaskEntry *)malloc(sizeof (TestScheduleTaskEntry));
    taskEntry1->task = task;
    taskEntry1->node.next = NULL;

    TestScheduleTaskEntry *taskEntry2 = (TestScheduleTaskEntry *)malloc(sizeof (TestScheduleTaskEntry));
    taskEntry2->task = task;
    taskEntry2->node.next = NULL;
    DListPushTail(&scheduleTaskQueue, &taskEntry1->node);
    DListPushTail(&scheduleTaskQueue, &taskEntry2->node);
    TestThreadPoolCtx *context = GetTestThreadPoolCtx(pool, &scheduleTaskQueue);

    pthread_t worker_t1, worker_t2;
    pthread_create(&worker_t1, NULL, TestScheduleTask, context);
    pthread_create(&worker_t2, NULL, TestScheduleTask, context);

    pthread_join(worker_t1, NULL);
    pthread_join(worker_t2, NULL);

    ErrLog(DEBUG, ErrMsg("[test] ConcurrentTest_Cancel_test scheduleSuccessTaskCnt = %lu \n", context->scheduleSuccessTaskCnt));
    ErrLog(DEBUG, ErrMsg("[test] ConcurrentTest_Cancel_test scheduleFailedTaskCnt = %lu \n", context->scheduleFailedTaskCnt));
    ASSERT_TRUE(context->scheduleSuccessTaskCnt == minThrdCnt + maxQueueSize);
    ASSERT_TRUE(context->scheduleFailedTaskCnt + context->scheduleSuccessTaskCnt == taskNum);

    // 单独起线程执行取消任务
    pthread_t worker_t3;
    pthread_create(&worker_t3, NULL, TestCancelTask, context);
    pthread_join(worker_t3, NULL);

    DestroyThreadPool(pool);

    ErrLog(DEBUG, ErrMsg("[test] ConcurrentTest_Cancel_test routineCnt = %lu \n", context->routineCnt));
    ErrLog(DEBUG, ErrMsg("[test] ConcurrentTest_Cancel_test minThrdCnt = %u \n", minThrdCnt));
    ErrLog(DEBUG, ErrMsg("[test] ConcurrentTest_Cancel_test finalizeCnt = %lu \n", context->finalizeCnt));
    ASSERT_TRUE(context->finalizeCnt == minThrdCnt + maxQueueSize);

    ErrLog(DEBUG, ErrMsg("[test] ConcurrentTest_Cancel_test cancelSuccessTaskCnt: %lu \n", context->cancelSuccessTaskCnt));
    ErrLog(DEBUG, ErrMsg("[test] ConcurrentTest_Cancel_test cancelExecutingTaskCnt: %lu \n", context->cancelExecutingTaskCnt));
    ErrLog(DEBUG, ErrMsg("[test] ConcurrentTest_Cancel_test cancelFinishTaskCnt: %lu \n", context->cancelFinishTaskCnt));

    FreeTestTaskQueue(&context->taskIdQueue);
    free(testMemAllocator);
    free(context);
}

TEST_F(ThreadPoolTest, MultiQueueScheduler_InterfaceTest)
{
    MemAllocator *testMemAllocator = (MemAllocator *)malloc(sizeof(MemAllocator));
    testMemAllocator->alloc = TestMemAlloc;
    testMemAllocator->free = TestMemFree;

    MultiQueueSchedulerInitParams initParam = {
        .super = {},
        .allocator = testMemAllocator,
        .taskQueueCnt = 10,
        .maxQueueSize = 0,
    };
    ErrorCode errCode = ERROR_SYS_OK;
    MultiQueueScheduler *multiScheduler = NewMultiQueueScheduler((TypeInitParams *)&initParam, &errCode);
    ASSERT_TRUE(multiScheduler == nullptr);
    ASSERT_TRUE(errCode == ERROR_UTILS_COMMON_INVALID_PARAMETER);

    initParam = {
         .super = {},
        .allocator = testMemAllocator,
        .taskQueueCnt = 10,
        .maxQueueSize = 10000001,
    };
    errCode = ERROR_SYS_OK;
    multiScheduler = NewMultiQueueScheduler((TypeInitParams *)&initParam, &errCode);
    ASSERT_TRUE(multiScheduler == nullptr);
    ASSERT_TRUE(errCode == ERROR_UTILS_COMMON_INVALID_PARAMETER);

    initParam = {
        .super = {},
        .allocator = testMemAllocator,
        .taskQueueCnt = 0,
        .maxQueueSize = 10000,
    };
    errCode = ERROR_SYS_OK;
    multiScheduler = NewMultiQueueScheduler((TypeInitParams *)&initParam, &errCode);
    ASSERT_TRUE(multiScheduler == nullptr);
    ASSERT_TRUE(errCode == ERROR_UTILS_COMMON_INVALID_PARAMETER);

    initParam = {
        .super = {},
        .allocator = testMemAllocator,
        .taskQueueCnt = 101,
        .maxQueueSize = 10000,
    };
    errCode = ERROR_SYS_OK;
    multiScheduler = NewMultiQueueScheduler((TypeInitParams *)&initParam, &errCode);
    ASSERT_TRUE(multiScheduler == nullptr);
    ASSERT_TRUE(errCode == ERROR_UTILS_COMMON_INVALID_PARAMETER);

    initParam = {
        .super = {},
        .allocator = testMemAllocator,
        .taskQueueCnt = 100,
        .maxQueueSize = 10000000,
    };
    errCode = ERROR_SYS_OK;
    multiScheduler = NewMultiQueueScheduler((TypeInitParams *)&initParam, &errCode);
    ASSERT_TRUE(multiScheduler != nullptr);
    ASSERT_TRUE(errCode == ERROR_SYS_OK);
    SchedulerDestroy((Scheduler *)multiScheduler);
    RefObjectKillAndDecRef(UP_TYPE_CAST(multiScheduler, RefObject));

    ThreadPoolParams params;
    buildThreadPoolParams(&params, THREAD_POOL_FLAG_FIXED, 10, 20, 0, 50, 0);
    initParam = {
        .super = params,
        .allocator = testMemAllocator,
        .taskQueueCnt = 100,
        .maxQueueSize = 10000000,
    };
    errCode = ERROR_SYS_OK;
    multiScheduler = NewMultiQueueScheduler((TypeInitParams *)&initParam, &errCode);
    ASSERT_NE(multiScheduler, nullptr);
    ASSERT_EQ(errCode, ERROR_SYS_OK);

    ThreadPool *pool = CreateThreadPoolEX(&params, (Scheduler *)multiScheduler, testMemAllocator, &errCode);
    ASSERT_NE(pool, nullptr);
    DestroyThreadPool(pool);
    pool = nullptr;

    buildThreadPoolParams(&params, THREAD_POOL_FLAG_FIXED, 10, 20, 0, 50, 0);
    initParam = {
        .super = params,
        .allocator = testMemAllocator,
        .taskQueueCnt = 100,
        .maxQueueSize = 10000000,
    };
    errCode = ERROR_SYS_OK;
    multiScheduler = NewMultiQueueScheduler((TypeInitParams *)&initParam, &errCode);
    ASSERT_NE(multiScheduler, nullptr);
    ASSERT_EQ(errCode, ERROR_SYS_OK);

    pool = CreateThreadPoolEX(&params, (Scheduler *)multiScheduler, testMemAllocator, &errCode);
    ASSERT_NE(pool, nullptr);
    DestroyThreadPool(pool);
    pool = nullptr;

    buildThreadPoolParams(&params, THREAD_POOL_FLAG_SCALING, 10, 20, 0, 50, 0);
    initParam = {
        .super = params,
        .allocator = testMemAllocator,
        .taskQueueCnt = 100,
        .maxQueueSize = 10000000,
    };
    errCode = ERROR_SYS_OK;
    multiScheduler = NewMultiQueueScheduler((TypeInitParams *)&initParam, &errCode);
    ASSERT_NE(multiScheduler, nullptr);
    ASSERT_EQ(errCode, ERROR_SYS_OK);

    pool = CreateThreadPoolEX(&params, (Scheduler *)multiScheduler, testMemAllocator, &errCode);
    ASSERT_NE(pool, nullptr);
    DestroyThreadPool(pool);

    free(testMemAllocator);
}

TEST_F(ThreadPoolTest, MutliQueue_ConcurrentTest)
{
    MemAllocator *testMemAllocator = (MemAllocator *)malloc(sizeof(MemAllocator));
    testMemAllocator->alloc = TestMemAlloc;
    testMemAllocator->free = TestMemFree;

    uint32_t minThrdCnt = 10;
    uint32_t maxThrdCnt = 20;
    uint32_t stackSize = 0;
    uint32_t maxQueueSize = 50;
    uint32_t idleTimeThreshold = 0;
    uint32_t taskNum = 50 * 2;

    ThreadPoolParams params;
    buildThreadPoolParams(&params, THREAD_POOL_FLAG_FIXED, minThrdCnt, maxThrdCnt, stackSize, maxQueueSize, idleTimeThreshold);

    MultiQueueSchedulerInitParams initParam = {
        .super = params,
        .allocator = testMemAllocator,
        .taskQueueCnt = 10,
        .maxQueueSize = 100000,
    };

    ErrorCode errCode = ERROR_SYS_OK;
    MultiQueueScheduler *multiScheduler = NewMultiQueueScheduler((TypeInitParams *)&initParam, &errCode);
    ThreadPool *pool = CreateThreadPoolEX(&initParam.super, (Scheduler *)multiScheduler, testMemAllocator, &errCode);
    ASSERT(pool != nullptr);

    DListHead scheduleTaskQueue;
    DListInit(&scheduleTaskQueue);

    ThreadPoolTask task;
    buildTask(&task, routine_cnt, NULL, finalize_cnt, NULL, NULL);

    TestScheduleTaskEntry *taskEntry1 = (TestScheduleTaskEntry *)malloc(sizeof (TestScheduleTaskEntry));
    taskEntry1->task = task;
    taskEntry1->node.next = NULL;

    TestScheduleTaskEntry *taskEntry2 = (TestScheduleTaskEntry *)malloc(sizeof (TestScheduleTaskEntry));
    taskEntry2->task = task;
    taskEntry2->node.next = NULL;

    DListPushTail(&scheduleTaskQueue, &taskEntry1->node);
    DListPushTail(&scheduleTaskQueue, &taskEntry2->node);
    TestThreadPoolCtx *context = GetTestThreadPoolCtx(pool, &scheduleTaskQueue);
    pthread_t worker_t1, worker_t2;
    pthread_create(&worker_t1, NULL, TestScheduleTask, context);
    pthread_create(&worker_t2, NULL, TestScheduleTask, context);

    pthread_join(worker_t1, NULL);
    pthread_join(worker_t2, NULL);

    ErrLog(DEBUG, ErrMsg("[test] MutliQueue_ConcurrentTest scheduleSuccessTaskCnt = %lu", context->scheduleSuccessTaskCnt));
    ErrLog(DEBUG, ErrMsg("[test] MutliQueue_ConcurrentTest scheduleFailedTaskCnt = %lu", context->scheduleFailedTaskCnt));
    ASSERT_TRUE(context->scheduleSuccessTaskCnt == taskNum);
    ASSERT_TRUE(context->scheduleFailedTaskCnt == 0);

    DestroyThreadPool(pool);

    ErrLog(DEBUG, ErrMsg("[test] MutliQueue_ConcurrentTest routineCnt = %lu", context->routineCnt));
    ErrLog(DEBUG, ErrMsg("[test] MutliQueue_ConcurrentTest finalizeCnt = %lu", context->finalizeCnt));
    ErrLog(DEBUG, ErrMsg("[test] MutliQueue_ConcurrentTest minThrdCnt = %u", minThrdCnt));
    ASSERT_TRUE(context->routineCnt == taskNum);
    ASSERT_TRUE(context->finalizeCnt == taskNum);

    FreeTestTaskQueue(&context->taskIdQueue);
    free(testMemAllocator);
    free(context);
}

static void CancelTestTask(ThreadPool *pool,  TestThreadPoolCtx *context) {
    TestTaskEntry *taskEntry = NULL;
    pthread_mutex_lock(&context->lock_task);
    DListMutableIter iter;
    DLIST_MODIFY_FOR_EACH(iter, &context->taskIdQueue) {
        taskEntry = DLIST_CONTAINER(TestTaskEntry, node, iter.cur);
        if (taskEntry == NULL) {
            continue;
        }
        CancelThreadPoolTask(pool, taskEntry->taskId);
    }
    pthread_mutex_unlock(&context->lock_task);
}

TEST_F(ThreadPoolTest, FIFO_TEST_FLAG_SCALING)
{
    MemAllocator *testMemAllocator = (MemAllocator *)malloc(sizeof(MemAllocator));
    testMemAllocator->alloc = TestMemAlloc;
    testMemAllocator->free = TestMemFree;

    uint32_t minThrdCnt = 10;
    uint32_t maxThrdCnt = 20;
    uint32_t stackSize = 0;
    uint32_t maxQueueSize = 50;
    uint32_t idleTimeThreshold = 100;
    uint32_t taskNum = 50 * 2;

    ThreadPoolParams params;
    buildThreadPoolParams(&params, THREAD_POOL_FLAG_SCALING, minThrdCnt, maxThrdCnt, stackSize, maxQueueSize, idleTimeThreshold);

    ErrorCode errCode = ERROR_SYS_OK;
    ThreadPool *pool = CreateThreadPool(&params, testMemAllocator, &errCode);
    ASSERT_TRUE(pool != nullptr);

    usleep(500000);
    // 添加任务前活跃线程数为最小线程数
    uint32_t threadCnt = GetCurAliveThreadCnt(pool);
    ASSERT_TRUE(threadCnt == minThrdCnt);

    DListHead scheduleTaskQueue;
    DListInit(&scheduleTaskQueue);

    ThreadPoolTask task;
    buildTask(&task, routine_Cnt_Long, NULL, finalize_Cnt, cancel_Cnt_long, NULL);

    TestScheduleTaskEntry *taskEntry1 = (TestScheduleTaskEntry *)malloc(sizeof (TestScheduleTaskEntry));
    taskEntry1->task = task;
    taskEntry1->node.next = NULL;

    TestScheduleTaskEntry *taskEntry2 = (TestScheduleTaskEntry *)malloc(sizeof (TestScheduleTaskEntry));
    taskEntry2->task = task;
    taskEntry2->node.next = NULL;

    DListPushTail(&scheduleTaskQueue, &taskEntry1->node);
    DListPushTail(&scheduleTaskQueue, &taskEntry2->node);
    TestThreadPoolCtx *context = GetTestThreadPoolCtx(pool, &scheduleTaskQueue);

    pthread_t worker_t1, worker_t2;
    pthread_create(&worker_t1, NULL, TestScheduleTask, context);
    pthread_create(&worker_t2, NULL, TestScheduleTask, context);

    pthread_join(worker_t1, NULL);
    pthread_join(worker_t2, NULL);

    ErrLog(DEBUG, ErrMsg("[test] FIFO_TEST_FLAG_SCALING scheduleSuccessTaskCnt = %lu \n", context->scheduleSuccessTaskCnt));
    ErrLog(DEBUG, ErrMsg("[test] FIFO_TEST_FLAG_SCALING scheduleFailedTaskCnt = %lu \n", context->scheduleFailedTaskCnt));
    ASSERT_EQ(context->scheduleSuccessTaskCnt, maxThrdCnt + maxQueueSize);
    ASSERT_EQ(context->scheduleFailedTaskCnt + context->scheduleSuccessTaskCnt, taskNum);

    // 添加任务后活跃线程数为最大线程数
    threadCnt = GetCurAliveThreadCnt(pool);
    ASSERT_TRUE(threadCnt == maxThrdCnt);

    // 取消所有task，让用例执行完
    CancelTestTask(pool, context);

    usleep(500000);
    // 所有任务执行完后活跃线程数为最小线程数
    threadCnt = GetCurAliveThreadCnt(pool);
    ASSERT_TRUE(threadCnt == minThrdCnt);

    DestroyThreadPool(pool);

    ErrLog(DEBUG, ErrMsg("[test] FIFO_TEST_FLAG_SCALING routineCnt = %lu.", context->routineCnt));
    ErrLog(DEBUG, ErrMsg("[test] FIFO_TEST_FLAG_SCALING finalizeCnt = %lu.", context->finalizeCnt));
    ErrLog(DEBUG, ErrMsg("[test] FIFO_TEST_FLAG_SCALING minThrdCnt = %u.", minThrdCnt));
    //ASSERT_TRUE(context->routineCnt == maxThrdCnt);
    ASSERT_TRUE(context->finalizeCnt == maxThrdCnt + maxQueueSize);

    FreeTestTaskQueue(&context->taskIdQueue);
    free(testMemAllocator);
    free(context);
}

TEST_F(ThreadPoolTest, MutliQueue_TEST_FLAG_SCALING)
{
    MemAllocator *testMemAllocator = (MemAllocator *)malloc(sizeof(MemAllocator));
    testMemAllocator->alloc = TestMemAlloc;
    testMemAllocator->free = TestMemFree;

    uint32_t minThrdCnt = 10;
    uint32_t maxThrdCnt = 40;
    uint32_t stackSize = 0;
    uint32_t maxQueueSize = 50;
    uint32_t idleTimeThreshold = 100;
    uint32_t taskNum = 50 * 2;

    ThreadPoolParams params;
    buildThreadPoolParams(&params, THREAD_POOL_FLAG_SCALING, minThrdCnt, maxThrdCnt, stackSize, maxQueueSize, idleTimeThreshold);

    ErrorCode errCode = ERROR_SYS_OK;
    MultiQueueSchedulerInitParams initParam;
    initParam.allocator = testMemAllocator;
    initParam.taskQueueCnt = 5;
    initParam.maxQueueSize = 10;

    MultiQueueScheduler *multiScheduler = NewMultiQueueScheduler((TypeInitParams *)&initParam, &errCode);
    ASSERT_TRUE(errCode == ERROR_SYS_OK);

    ThreadPool *pool = CreateThreadPoolEX(&params, (Scheduler *)multiScheduler, testMemAllocator, &errCode);
    ASSERT_TRUE(pool != nullptr);

    usleep(500000);
    // 添加任务前活跃线程数为最小线程数
    uint32_t threadCnt = GetCurAliveThreadCnt(pool);
    ASSERT_TRUE(threadCnt == minThrdCnt);

    DListHead scheduleTaskQueue;
    DListInit(&scheduleTaskQueue);

    ThreadPoolTask task;
    buildTask(&task, routine_cnt_1s, NULL, finalize_Cnt, NULL, NULL);

    TestScheduleTaskEntry *taskEntry1 = (TestScheduleTaskEntry *)malloc(sizeof (TestScheduleTaskEntry));
    taskEntry1->task = task;
    taskEntry1->node.next = NULL;

    TestScheduleTaskEntry *taskEntry2 = (TestScheduleTaskEntry *)malloc(sizeof (TestScheduleTaskEntry));
    taskEntry2->task = task;
    taskEntry2->node.next = NULL;

    DListPushTail(&scheduleTaskQueue, &taskEntry1->node);
    DListPushTail(&scheduleTaskQueue, &taskEntry2->node);
    TestThreadPoolCtx *context = GetTestThreadPoolCtx(pool, &scheduleTaskQueue);

    pthread_t worker_t1, worker_t2;
    pthread_create(&worker_t1, NULL, TestScheduleTask, context);
    pthread_create(&worker_t2, NULL, TestScheduleTask, context);

    usleep(500000);
    ErrLog(DEBUG, ErrMsg("[test] MutliQueue_TEST_FLAG_SCALING scheduleSuccessTaskCnt = %lu.", context->scheduleSuccessTaskCnt));
    ErrLog(DEBUG, ErrMsg("[test] MutliQueue_TEST_FLAG_SCALING scheduleFailedTaskCnt = %lu.", context->scheduleFailedTaskCnt));
    ASSERT_TRUE(context->scheduleSuccessTaskCnt == (maxThrdCnt + initParam.taskQueueCnt * initParam.maxQueueSize));
    // 由于是阻塞性的添加任务，不会出现添加任务失败
    ASSERT_TRUE(context->scheduleFailedTaskCnt == 0);

    // 添加任务后活跃线程数为最大线程数,
    threadCnt = GetCurAliveThreadCnt(pool);
    ASSERT_TRUE(threadCnt == maxThrdCnt);

    usleep(1000000);
    ASSERT_TRUE(context->scheduleSuccessTaskCnt == taskNum);

    pthread_join(worker_t1, NULL);
    pthread_join(worker_t2, NULL);

    // 100任务，每次执行40个，40-40-20，三个task周期执行完
    usleep(2000000);
    // 所有任务执行完后活跃线程数为最小线程数
    threadCnt = GetCurAliveThreadCnt(pool);
    ASSERT_TRUE(threadCnt == minThrdCnt);

    DestroyThreadPool(pool);
    ASSERT_TRUE(context->routineCnt == taskNum);
    ASSERT_TRUE(context->finalizeCnt == taskNum);

    FreeTestTaskQueue(&context->taskIdQueue);
    free(testMemAllocator);
    free(context);
}

TEST_F(ThreadPoolTest, Dfx_FlagStatsticsForSchedStat)
{
    static atomic_int runThreadPoolStatstics;
    runThreadPoolStatstics = 0;
    // THREAD_POOL_FLAG_STAT_FAULT_INJECT fault inject point to set runThreadPoolStatstics to true
    void(*callback)(const FaultInjectionEntry *, void *) = [](const FaultInjectionEntry *fe, void *ptr) -> void {
        runThreadPoolStatstics++;
    };
    FaultInjectionEntry entries[] = {
        FAULT_INJECTION_ENTRY(THREAD_POOL_FLAG_STAT_FAULT_INJECT, false, callback),
    };
    ASSERT_EQ(RegisterFaultInjection(entries, sizeof(entries) / sizeof(entries[0]), FI_GLOBAL), ERROR_SYS_OK);
    FAULT_INJECTION_ACTIVE(THREAD_POOL_FLAG_STAT_FAULT_INJECT, FI_GLOBAL);
    // create mq scheduler
    MultiQueueSchedulerInitParams initParam = mqParams_;
    MultiQueueScheduler *multiScheduler = NewMultiQueueScheduler((TypeInitParams *)&initParam, &errCode_);
    ASSERT_EQ(errCode_, ERROR_SYS_OK);
    // create thread pool
    ThreadPoolParams params = tpParams_;
    params.flags = THREAD_POOL_FLAG_FIXED | THREAD_POOL_FLAG_STAT;
    ThreadPool *pool = CreateThreadPoolEX(&params, (Scheduler *)multiScheduler, NULL, &errCode_);
    ASSERT_TRUE(pool != nullptr);
    // submit task
    auto taskFunc = [](void *) -> void * {
        EXPECT_EQ(runThreadPoolStatstics, 1);
        runThreadPoolStatstics++;
        return nullptr;
    };
    TaskId taskId;
    ThreadPoolTask task {taskFunc};
    ASSERT_EQ(ScheduleThreadPoolTask(pool, &task, &taskId), ERROR_SYS_OK);
    // wait
    auto waitfunc = [](int expect) -> int {
        auto t1 = time(nullptr);
        while ((runThreadPoolStatstics != expect) && (time(nullptr) - t1 < 2)); // run all task in < 2s, time() precision is second.
        return runThreadPoolStatstics;
    };
    ASSERT_EQ(waitfunc(3), 3); // task prerun and postrun enter fault inject once
    // stop Statstics
    runThreadPoolStatstics = 1;
    SetThreadPoolStatstics(pool, false);
    ASSERT_EQ(ScheduleThreadPoolTask(pool, &task, &taskId), ERROR_SYS_OK);
    ASSERT_EQ(waitfunc(2), 2);
    // enable Statstics
    runThreadPoolStatstics = 0;
    SetThreadPoolStatstics(pool, true);
    ASSERT_EQ(ScheduleThreadPoolTask(pool, &task, &taskId), ERROR_SYS_OK);
    ASSERT_EQ(waitfunc(3), 3);
    // clear resources
    DestroyThreadPool(pool);
    FAULT_INJECTION_INACTIVE(THREAD_POOL_FLAG_STAT_FAULT_INJECT, FI_GLOBAL);
}

TEST_F(ThreadPoolTest, Dfx_FlagStatsticsForSubmitStat)
{
    atomic_bool testing(true);
    /* check the count in submit statistics is equal the submit count actually */
    static atomic_uint submitShowTimes;
    submitShowTimes = 0;
    void(*callback)(const FaultInjectionEntry *, int) = [](const FaultInjectionEntry *fe, int) -> void {
        submitShowTimes++;
    };

    FaultInjectionEntry entries[] = {
        FAULT_INJECTION_ENTRY(THREADPOOL_SUBMIT_LATENCY_FAULT_INJECTION, false, nullptr),
        FAULT_INJECTION_ENTRY(THREADPOOL_SUBMIT_COUNT_STATS_FAULT_INJECTION, false, callback),
    };
    ASSERT_EQ(RegisterFaultInjection(entries, sizeof(entries) / sizeof(entries[0]), FI_GLOBAL), ERROR_SYS_OK);
    FAULT_INJECTION_ACTIVE(THREADPOOL_SUBMIT_LATENCY_FAULT_INJECTION, FI_GLOBAL);
    FAULT_INJECTION_ACTIVE(THREADPOOL_SUBMIT_COUNT_STATS_FAULT_INJECTION, FI_GLOBAL);

    // create thread pool
    ThreadPoolParams params = tpParams_;
    params.flags = THREAD_POOL_FLAG_SCALING | THREAD_POOL_FLAG_STAT;
    auto pool = CreateThreadPool(&params, nullptr, &errCode_);
    ASSERT_TRUE(pool != nullptr);
    // submit task
    vector<thread> vThread(10);
    for (int i = 0; i < vThread.size(); i++) {
        vThread[i] = thread([](ThreadPool *pool, atomic_bool *testing) {
            TaskId taskId;
            ThreadPoolTask task {[](void *) -> void * { return nullptr; }};
            while (*testing) {
                if (ScheduleThreadPoolTask(pool, &task, &taskId) != ERROR_SYS_OK) {
                    usleep(2000); // 2ms
                }
            }
            CloseLogger();
        }, pool, &testing);
    }
    // wait 115 ms and finish testing, the first show stats log at 0. THREADPOOL_SUBMIT_LATENCY_FAULT_INJECTION change
    // show threshold to 50ms, so the second show at 50ms, third show at 100ms, total 3 times
    usleep(115 * 1000);
    testing = false;
    // clear resources
    for (auto &th : vThread) {
        th.join();
    }
    DestroyThreadPool(pool);
    FAULT_INJECTION_INACTIVE(THREADPOOL_SUBMIT_LATENCY_FAULT_INJECTION, FI_GLOBAL);
    FAULT_INJECTION_INACTIVE(THREADPOOL_SUBMIT_COUNT_STATS_FAULT_INJECTION, FI_GLOBAL);
    // check errlog, THREADPOOL_SUBMIT_LATENCY_FAULT_INJECTION change show period to 100 ms, so about show 10 times
    ASSERT_GE(submitShowTimes, 3);
}

TEST_F(ThreadPoolTest, Dfx_DetectPeriodTest)
{
    vector<vector<uint16_t>> testData = {
        {1,   1,   1}, // t0, t1, gcd
        {2,   2,   2},
        {100, 2,   2},
        {337, 937, 1},
        {15,  20,  5},
        {30,  60,  30},
        {20,  0,   20},
        {0,   60,  60},
        {0,   0,   UINT16_MAX},
        {UINT16_MAX, UINT16_MAX, UINT16_MAX},
    };
    static atomic_int expect[3]; // {t0, t1}, gcd
    // THREAD_POOL_DETECT_PERIOD_FAULT_INJECT fault inject point to test sleep timeout for detect thread
    void(*callback)(const FaultInjectionEntry *, uint16_t, uint16_t, uint16_t) = [](const FaultInjectionEntry *fe,
        uint16_t t0, uint16_t t1, uint16_t timeout) -> void {
        if ((expect[0] == t0) && (expect[1] == t1)) {
            EXPECT_EQ(timeout, expect[2]);
            __atomic_thread_fence(__ATOMIC_SEQ_CST);
            expect[2] = -1; // end test
        }
    };
    FaultInjectionEntry entries[] = {
        FAULT_INJECTION_ENTRY(THREAD_POOL_DETECT_PERIOD_FAULT_INJECT, false, callback),
    };
    ASSERT_EQ(RegisterFaultInjection(entries, sizeof(entries) / sizeof(entries[0]), FI_GLOBAL), ERROR_SYS_OK);
    // create mq scheduler
    MultiQueueSchedulerInitParams initParam = mqParams_;
    MultiQueueScheduler *multiScheduler = NewMultiQueueScheduler((TypeInitParams *)&initParam, &errCode_);
    ASSERT_EQ(errCode_, ERROR_SYS_OK);
    // create thread pool
    ThreadPoolParams params = tpParams_;
    params.flags = THREAD_POOL_FLAG_FIXED | THREAD_POOL_FLAG_ENABLE_DETECT;
    ThreadPool *pool = CreateThreadPoolEX(&params, (Scheduler *)multiScheduler, NULL, &errCode_);
    ASSERT_TRUE(pool != nullptr);
    // do test
    for (auto &v : testData) {
        expect[0] = v[0];
        expect[1] = v[1];
        expect[2] = v[2];
        FAULT_INJECTION_ACTIVE(THREAD_POOL_DETECT_PERIOD_FAULT_INJECT, FI_GLOBAL);
        SetDetectTaskRunThreshold(pool, expect[0]);
        SetDetectOverloadThreshold(pool, expect[1]);
        // wait test
        auto t1 = time(nullptr);
        // task prerun and postrun enter fault inject once, one task total twice
        while ((expect[2] != -1) && (time(nullptr) - t1 < 2)); // run all task in < 2s, time() precision is second.
        ASSERT_EQ(expect[2], -1);
        FAULT_INJECTION_INACTIVE(THREAD_POOL_DETECT_PERIOD_FAULT_INJECT, FI_GLOBAL);
    }
    // clear resources
    DestroyThreadPool(pool);
}

TEST_F(ThreadPoolTest, Dfx_DetectTaskRunWaningTest)
{
#define TASK_RUN_THRES 3
#define CHECK_SCOPE (3 * TASK_RUN_THRES)
#define TEST_RANGE  (5 * TASK_RUN_THRES)
    int expectCnt = 0;
    static atomic_int detectCnt, runCnt;
    detectCnt = runCnt = 0;
    SetErrLogServerLevel(WARNING);

    // THREAD_POOL_DETECT_PERIOD_FAULT_INJECT fault inject point to test sleep timeout for detect thread
    void(*callback)(const FaultInjectionEntry *, uint64_t, uint32_t, uint64_t *) = [](const FaultInjectionEntry *fe,
        uint64_t oldStartTime, uint32_t args, uint64_t *startTime) -> void {
        reinterpret_cast<atomic_ulong&>(*startTime).compare_exchange_strong(oldStartTime, 0, std::memory_order_relaxed); // clear this startTime, avoid to trigger next time
        if (args > CHECK_SCOPE) {
            detectCnt++;
        }
    };
    FaultInjectionEntry entries[] = {
        FAULT_INJECTION_ENTRY(THREAD_POOL_DETECT_TASK_RUN_FAULT_INJECT, false, callback),
        FAULT_INJECTION_ENTRY(THREAD_POOL_DETECT_FAULT_INJECT, false, nullptr),
    };
    ASSERT_EQ(RegisterFaultInjection(entries, sizeof(entries) / sizeof(entries[0]), FI_GLOBAL), ERROR_SYS_OK);
    FAULT_INJECTION_ACTIVE(THREAD_POOL_DETECT_TASK_RUN_FAULT_INJECT, FI_GLOBAL);
    FAULT_INJECTION_ACTIVE(THREAD_POOL_DETECT_FAULT_INJECT, FI_GLOBAL);
    // create mq scheduler
    MultiQueueSchedulerInitParams initParam = mqParams_;
    MultiQueueScheduler *multiScheduler = NewMultiQueueScheduler((TypeInitParams *)&initParam, &errCode_);
    ASSERT_EQ(errCode_, ERROR_SYS_OK);
    // create thread pool
    ThreadPoolParams params = tpParams_;
    params.flags = THREAD_POOL_FLAG_FIXED | THREAD_POOL_FLAG_ENABLE_DETECT;
    ThreadPool *pool = CreateThreadPoolEX(&params, (Scheduler *)multiScheduler, NULL, &errCode_);
    ASSERT_TRUE(pool != nullptr);
    SetDetectTaskRunThreshold(pool, TASK_RUN_THRES);
    SetDetectOverloadThreshold(pool, 0);
    // create task
    auto taskFunc = [](void *args) -> void * {
        usleep((uint64_t)args * 1000); // args is ms
        runCnt++;
        return nullptr;
    };
    TaskId taskId;
    ThreadPoolTask task {taskFunc};
    // do test
    srand(time(NULL));
#define TEST_RUN_TIMES 2000
    for (int i = 0; i < TEST_RUN_TIMES; i++) {
        uint64_t run = rand() % (TEST_RANGE) + 1; // [1, 5*TASK_RUN_THRES]
        if (run > CHECK_SCOPE) {
            expectCnt++;
        }
        task.args = (void *)run;
        ASSERT_EQ(ScheduleThreadPoolTask(pool, &task, &taskId), ERROR_SYS_OK);
    }
    // wait
    auto t1 = time(nullptr);
    // task prerun and postrun enter fault inject once, one task total twice
    while ((runCnt != TEST_RUN_TIMES) && (time(nullptr) - t1 < 3)); // run all task in < 3s, time() precision is second.
    ASSERT_EQ(runCnt, TEST_RUN_TIMES);
    ASSERT_LE(detectCnt, expectCnt); // detectCnt < expectCnt is missing detection, detectCnt > expectCnt is mistaking detection
    ASSERT_GT(detectCnt, 1); // at lease 1 times
    // clear resources
    DestroyThreadPool(pool);
    FAULT_INJECTION_INACTIVE(THREAD_POOL_DETECT_TASK_RUN_FAULT_INJECT, FI_GLOBAL);
    FAULT_INJECTION_INACTIVE(THREAD_POOL_DETECT_FAULT_INJECT, FI_GLOBAL);
}

TEST_F(ThreadPoolTest, Dfx_DetectTaskRunCriticalTest)
{
#define TASK_RUN_THRES 3
#define CHECK_SCOPE (3 * TASK_RUN_THRES)
#define TEST_RANGE  (5 * TASK_RUN_THRES)
    int expectCnt = 0;
    static atomic_int detectCnt, runCnt;
    detectCnt = runCnt = 0;
    SetErrLogServerLevel(WARNING);

    // THREAD_POOL_DETECT_PERIOD_FAULT_INJECT fault inject point to test sleep timeout for detect thread
    void(*callback)(const FaultInjectionEntry *, uint64_t, uint32_t, uint64_t *) = [](const FaultInjectionEntry *fe,
                                                                                       uint64_t oldStartTime, uint32_t args, uint64_t *startTime) -> void {
        reinterpret_cast<atomic_ulong&>(*startTime).compare_exchange_strong(oldStartTime, 0, std::memory_order_relaxed); // clear this startTime, avoid to trigger next time
        if (args > CHECK_SCOPE) {
            detectCnt++;
        }
    };
    FaultInjectionEntry entries[] = {
        FAULT_INJECTION_ENTRY(THREAD_POOL_DETECT_TASK_RUN_CRITICAL_FAULT_INJECT, false, callback),
        FAULT_INJECTION_ENTRY(THREAD_POOL_DETECT_FAULT_INJECT, false, nullptr),
    };
    ASSERT_EQ(RegisterFaultInjection(entries, sizeof(entries) / sizeof(entries[0]), FI_GLOBAL), ERROR_SYS_OK);
    FAULT_INJECTION_ACTIVE(THREAD_POOL_DETECT_TASK_RUN_CRITICAL_FAULT_INJECT, FI_GLOBAL);
    FAULT_INJECTION_ACTIVE(THREAD_POOL_DETECT_FAULT_INJECT, FI_GLOBAL);
    // create mq scheduler
    MultiQueueSchedulerInitParams initParam = mqParams_;
    MultiQueueScheduler *multiScheduler = NewMultiQueueScheduler((TypeInitParams *)&initParam, &errCode_);
    ASSERT_EQ(errCode_, ERROR_SYS_OK);
    // create thread pool
    ThreadPoolParams params = tpParams_;
    params.flags = THREAD_POOL_FLAG_FIXED | THREAD_POOL_FLAG_ENABLE_DETECT;
    ThreadPool *pool = CreateThreadPoolEX(&params, (Scheduler *)multiScheduler, NULL, &errCode_);
    ASSERT_TRUE(pool != nullptr);
    SetDetectTaskRunThreshold(pool, TASK_RUN_THRES);
    SetDetectOverloadThreshold(pool, 0);
    // create task
    auto taskFunc = [](void *args) -> void * {
        usleep((uint64_t)args * (RUN_CRITICAL_TIMEOUT_TIME + 1) * 1000); // args is ms
        runCnt++;
        return nullptr;
    };
    TaskId taskId;
    ThreadPoolTask task {taskFunc};
    // do test
    srand(time(NULL));
#define TEST_RUN_TIMES 2000
    for (int i = 0; i < TEST_RUN_TIMES; i++) {
        uint64_t run = rand() % (TEST_RANGE) + 1; // [1, 5*TASK_RUN_THRES]
        if (run > CHECK_SCOPE) {
            expectCnt++;
        }
        task.args = (void *)run;
        ASSERT_EQ(ScheduleThreadPoolTask(pool, &task, &taskId), ERROR_SYS_OK);
    }
    // wait
    auto t1 = time(nullptr);
    // task prerun and postrun enter fault inject once, one task total twice
    while ((runCnt != TEST_RUN_TIMES) && (time(nullptr) - t1 < 15)); // run all task in < 3s, time() precision is second.
    ASSERT_EQ(runCnt, TEST_RUN_TIMES);
    ASSERT_LE(detectCnt, expectCnt); // detectCnt < expectCnt is missing detection, detectCnt > expectCnt is mistaking detection
    ASSERT_GT(detectCnt, 1); // at lease 1 times
    // clear resources
    DestroyThreadPool(pool);
    FAULT_INJECTION_INACTIVE(THREAD_POOL_DETECT_TASK_RUN_CRITICAL_FAULT_INJECT, FI_GLOBAL);
    FAULT_INJECTION_INACTIVE(THREAD_POOL_DETECT_FAULT_INJECT, FI_GLOBAL);
}

TEST_F(ThreadPoolTest, Dfx_DetectTaskRunSkipDetectTest)
{
#define TASK_RUN_THRES 1
    int expectCnt = 0;
    static atomic_int detectCnt, runCnt;
    detectCnt = runCnt = 0;
    SetErrLogServerLevel(WARNING);

    static atomic_int invokeCnt;
    invokeCnt = 0;
    void(*callback)(const FaultInjectionEntry *, void *) = [](const FaultInjectionEntry *fe, void *args) -> void {
        invokeCnt++;
    };
    FaultInjectionEntry entries[] = {
        FAULT_INJECTION_ENTRY(THREAD_POOL_SKIP_DETECT_TASK_RUN_FAULT_INJECT, false, callback),
        FAULT_INJECTION_ENTRY(THREAD_POOL_DETECT_FAULT_INJECT, false, nullptr),
    };
    ASSERT_EQ(RegisterFaultInjection(entries, sizeof(entries) / sizeof(entries[0]), FI_GLOBAL), ERROR_SYS_OK);
    FAULT_INJECTION_ACTIVE(THREAD_POOL_SKIP_DETECT_TASK_RUN_FAULT_INJECT, FI_GLOBAL);
    FAULT_INJECTION_ACTIVE(THREAD_POOL_DETECT_FAULT_INJECT, FI_GLOBAL);
    // create mq scheduler
    MultiQueueSchedulerInitParams initParam = mqParams_;
    MultiQueueScheduler *multiScheduler = NewMultiQueueScheduler((TypeInitParams *)&initParam, &errCode_);
    ASSERT_EQ(errCode_, ERROR_SYS_OK);
    // create thread pool
    ThreadPoolParams params = tpParams_;
    params.flags = THREAD_POOL_FLAG_FIXED | THREAD_POOL_FLAG_ENABLE_DETECT|THREAD_POOL_FLAG_DETECT_EXIT;
    ThreadPool *pool = CreateThreadPoolEX(&params, (Scheduler *)multiScheduler, NULL, &errCode_);
    ASSERT_TRUE(pool != nullptr);
    SetDetectTaskRunThreshold(pool, TASK_RUN_THRES);
    SetDetectOverloadThreshold(pool, 0);
    // create task
    auto taskFunc1 = [](void *args) -> void * {
        for (int i = 0; i < (RUN_CRITICAL_TIMEOUT_TIME + 2); ++i) {
            sleep(TASK_RUN_THRES);
            NotifyThreadPoolTaskAlive();
        }
        return nullptr;
    };
    auto taskFunc2 = [](void *args) -> void * {
        sleep(TASK_RUN_THRES);
        return nullptr;
    };
    TaskId taskId;
    ThreadPoolTask task {};
    // do test
    srand(time(NULL));
    int skipDetectTaskCnt = 0;
    int longTimeTaskCnt = 0;
    for (int i = 0; i < 10; i++) {
        if (i % 2) {
            task.routine = taskFunc1;
            task.args = (void *)false;
            skipDetectTaskCnt++;
        } else {
            task.routine = taskFunc2;
            task.args = (void *)true;
            longTimeTaskCnt++;
        }
        ASSERT_EQ(ScheduleThreadPoolTask(pool, &task, &taskId), ERROR_SYS_OK);
    }
    auto t1 = time(nullptr);
    while ((invokeCnt != longTimeTaskCnt) &&
           (time(nullptr) - t1 < (TASK_RUN_THRES * (RUN_CRITICAL_TIMEOUT_TIME + 2) + 2)));
    ASSERT_GE(invokeCnt, longTimeTaskCnt);
    DestroyThreadPool(pool);
    FAULT_INJECTION_INACTIVE(THREAD_POOL_SKIP_DETECT_TASK_RUN_FAULT_INJECT, FI_GLOBAL);
    FAULT_INJECTION_INACTIVE(THREAD_POOL_DETECT_FAULT_INJECT, FI_GLOBAL);
}

TEST_F(ThreadPoolTest, Dfx_DetectOverload)
{
#define TASK_RUN_THRES 3
    SetErrLogServerLevel(WARNING);

    static atomic_bool detected(false);
    void(*callback)(const FaultInjectionEntry *, bool overLoad) = [](const FaultInjectionEntry *fe, bool overLoad) -> void {
        detected = overLoad;
    };
    FaultInjectionEntry entries[] = {
        FAULT_INJECTION_ENTRY(THREAD_POOL_DETECT_OVERLOAD_FAULT_INJECT, false, callback),
        FAULT_INJECTION_ENTRY(THREAD_POOL_DETECT_FAULT_INJECT, false, nullptr),
        FAULT_INJECTION_ENTRY(THREAD_POOL_SET_THREAD_CNT_FAULT_INJECT, false, nullptr),
    };
    ASSERT_EQ(RegisterFaultInjection(entries, sizeof(entries) / sizeof(entries[0]), FI_GLOBAL), ERROR_SYS_OK);
    FAULT_INJECTION_ACTIVE(THREAD_POOL_DETECT_OVERLOAD_FAULT_INJECT, FI_GLOBAL);
    FAULT_INJECTION_ACTIVE(THREAD_POOL_DETECT_FAULT_INJECT, FI_GLOBAL);
    FAULT_INJECTION_ACTIVE(THREAD_POOL_SET_THREAD_CNT_FAULT_INJECT, FI_GLOBAL);
    // create mq scheduler
    MultiQueueSchedulerInitParams initParam = mqParams_;
    MultiQueueScheduler *multiScheduler = NewMultiQueueScheduler((TypeInitParams *)&initParam, &errCode_);
    ASSERT_EQ(errCode_, ERROR_SYS_OK);
    // create thread pool
    ThreadPoolParams params = tpParams_;
    params.minThrdCnt = 5,
    params.maxThrdCnt = 10,
    params.flags = THREAD_POOL_FLAG_SCALING | THREAD_POOL_FLAG_ENABLE_DETECT;
    ThreadPool *pool = CreateThreadPoolEX(&params, (Scheduler *)multiScheduler, NULL, &errCode_);
    ASSERT_TRUE(pool != nullptr);
    SetDetectTaskRunThreshold(pool, TASK_RUN_THRES);
    SetDetectOverloadThreshold(pool, TASK_RUN_THRES);
    /* The current expansion opportunity is when the thread pool thread wakes up after sleeping since it cannot obtain the task,
       so it needs to sleep for 10ms, otherwise the thread pool will be able to obtain the task for the first time and there will
       be no expansion opportunity */
    usleep(10 * 1000);
    // waitfunc
    auto waitFunc = [&](bool expect){
        auto t1 = time(nullptr);
        while ((detected != expect) && (time(nullptr) - t1 < 2)); // wait detect in 2s
    };
    // create task
    auto taskFunc = [](void *args) -> void * {
        usleep(TASK_RUN_THRES * 1000); // to sleep TASK_RUN_THRES ms
        return nullptr;
    };
    // do test
#define TEST_RUN_TIMES 20
    for (int i = 0; i < TEST_RUN_TIMES; i++) {
        TaskId taskId;
        ThreadPoolTask task {taskFunc};
        ASSERT_EQ(ScheduleThreadPoolTask(pool, &task, &taskId), ERROR_SYS_OK);
    }
    // wait detected overload state
    waitFunc(true);
    ASSERT_TRUE(detected);
    // task run finish, must not detected overload state
    waitFunc(false);
    ASSERT_FALSE(detected);

    // clear resources
    DestroyThreadPool(pool);
    FAULT_INJECTION_INACTIVE(THREAD_POOL_DETECT_OVERLOAD_FAULT_INJECT, FI_GLOBAL);
    FAULT_INJECTION_INACTIVE(THREAD_POOL_DETECT_FAULT_INJECT, FI_GLOBAL);
    FAULT_INJECTION_INACTIVE(THREAD_POOL_SET_THREAD_CNT_FAULT_INJECT, FI_GLOBAL);
}

TEST_F(ThreadPoolTest, Dfx_DetectScheduleLatency)
{
#define TASK_RUN_THRES 3
    SetErrLogServerLevel(WARNING);

    static atomic_bool detected(false);
    void(*callback)(const FaultInjectionEntry *, bool isDetected) = [](const FaultInjectionEntry *fe, bool isDetected) -> void {
        detected = isDetected;
    };
    FaultInjectionEntry entries[] = {
        FAULT_INJECTION_ENTRY(THREAD_POOL_DETECT_SCHED_LATENCY_FAULT_INJECT, false, callback),
        FAULT_INJECTION_ENTRY(THREAD_POOL_DETECT_FAULT_INJECT, false, nullptr),
    };
    ASSERT_EQ(RegisterFaultInjection(entries, sizeof(entries) / sizeof(entries[0]), FI_GLOBAL), ERROR_SYS_OK);
    FAULT_INJECTION_ACTIVE(THREAD_POOL_DETECT_SCHED_LATENCY_FAULT_INJECT, FI_GLOBAL);
    FAULT_INJECTION_ACTIVE(THREAD_POOL_DETECT_FAULT_INJECT, FI_GLOBAL);
    // create mq scheduler
    MultiQueueSchedulerInitParams initParam = mqParams_;
    MultiQueueScheduler *multiScheduler = NewMultiQueueScheduler((TypeInitParams *)&initParam, &errCode_);
    ASSERT_EQ(errCode_, ERROR_SYS_OK);
    // create blf scheduler
    BLFSchedulerInitParams blfParam = {0};
    blfParam.taskQueueSize = 100000;
    BLFScheduler *blfScheduler = NewBLFScheduler((TypeInitParams *)&blfParam, &errCode_);
    // create thread pool
    ThreadPoolParams params = tpParams_;
    params.minThrdCnt = 5,
    params.maxThrdCnt = 10,
    params.flags = THREAD_POOL_FLAG_SCALING | THREAD_POOL_FLAG_ENABLE_DETECT;
    ThreadPool *mqPool = CreateThreadPoolEX(&params, (Scheduler *)multiScheduler, NULL, &errCode_);
    ASSERT_NE(mqPool, nullptr);

    ThreadPool *blfPool = CreateThreadPoolEX(&params, (Scheduler *)blfScheduler, NULL, &errCode_);
    ASSERT_NE(blfPool, nullptr);

    ThreadPool *fifoPool = CreateThreadPool(&params, nullptr, &errCode_);
    ASSERT_NE(fifoPool, nullptr);
    // waitfunc
    auto waitFunc = [&](bool expect){
        auto t1 = time(nullptr);
        while ((detected != expect) && (time(nullptr) - t1 < 5)); // wait detect in 5s
    };
    // create task
    auto taskFunc = [](void *args) -> void * {
        usleep(TASK_RUN_THRES * 3 * 1000); // to sleep (TASK_RUN_THRES * 3) ms
        return nullptr;
    };
    for (auto &pool : {mqPool, fifoPool, blfPool}) {
        SetDetectScheduleLatencyThreshold(pool, TASK_RUN_THRES);
        // do test
        for (int i = 0; i < 20; i++) {
            TaskId taskId;
            ThreadPoolTask task {taskFunc};
            ASSERT_EQ(ScheduleThreadPoolTask(pool, &task, &taskId), ERROR_SYS_OK);
        }
        // wait detected task schedule latentency over threshold, waitFunc(true);
        waitFunc(true);
        ASSERT_TRUE(detected);
        // task run finish, must not detected schedule latentency
        waitFunc(false);
        ASSERT_FALSE(detected);

        // clear resources
        DestroyThreadPool(pool);
    }
    FAULT_INJECTION_INACTIVE(THREAD_POOL_DETECT_SCHED_LATENCY_FAULT_INJECT, FI_GLOBAL);
    FAULT_INJECTION_INACTIVE(THREAD_POOL_DETECT_FAULT_INJECT, FI_GLOBAL);
}

TEST_F(ThreadPoolTest, SetCpuSet)
{
    static atomic_int runCnt;
    runCnt = 0;
    // create thread pool
    ThreadPoolParams params = tpParams_;
    CPU_SET(0, &params.cpuset);  // bind to cpu0
    ThreadPool *pool = CreateThreadPool(&params, nullptr, &errCode_);
    ASSERT_NE(pool, nullptr);
    // create task
    auto taskFunc = [](void *args) -> void * {
        cpu_set_t get;
        EXPECT_EQ(pthread_getaffinity_np(pthread_self(), sizeof(get), &get), 0);
        CPU_EQUAL(&get, (cpu_set_t *)args);
        runCnt++;
        return nullptr;
    };
    TaskId taskId;
    ThreadPoolTask task {taskFunc};
    // do test
    task.args = &params.cpuset;
    int expectCnt = 0;
    for (; expectCnt < params.minThrdCnt + (params.minThrdCnt + params.maxThrdCnt) / 2; expectCnt++) {
        ASSERT_EQ(ScheduleThreadPoolTask(pool, &task, &taskId), ERROR_SYS_OK);
    }
    // wait
    auto waitfunc = [&runCnt](int expect) -> bool {
        auto t1 = time(nullptr);
        while ((runCnt != expect) && (time(nullptr) - t1 < 2)); // run all task in < 2s, time() precision is second.
        return runCnt == expect;
    };
    ASSERT_TRUE(waitfunc(expectCnt)); // task prerun and postrun enter fault inject once, one task total twice
    // change
    CPU_SET(1, &params.cpuset);
    SetThreadPoolCpuset(pool, &params.cpuset);
    task.args = &params.cpuset;
    for (; expectCnt < 2 * params.maxThrdCnt; expectCnt++) {
        ASSERT_EQ(ScheduleThreadPoolTask(pool, &task, &taskId), ERROR_SYS_OK);
    }
    ASSERT_TRUE(waitfunc(expectCnt));
    // clear
    DestroyThreadPool(pool);
}

TEST_F(ThreadPoolTest, SetTheadCnt)
{
    FaultInjectionEntry entries[] = {
        FAULT_INJECTION_ENTRY(THREAD_POOL_SET_THREAD_CNT_FAULT_INJECT, false, nullptr),
    };
    ASSERT_EQ(RegisterFaultInjection(entries, sizeof(entries) / sizeof(entries[0]), FI_GLOBAL), ERROR_SYS_OK);

    thread_local int64_t idx = 0;
    static atomic_int runCnt, runIdx;
    runCnt = 0;
    vector<vector<uint32_t>> testcase = {{10, 50, 0}, {10, 70, 1}, {30, 50, 0}, {30, 40, 1}, {50, 50, 0}}; // {minThrdCnt, maxThrdCnt, [0, shrink; 1, expand]}
    // create mq scheduler
    MultiQueueSchedulerInitParams initParam = mqParams_;
    MultiQueueScheduler *multiScheduler = NewMultiQueueScheduler((TypeInitParams *)&initParam, &errCode_);
    ASSERT_EQ(errCode_, ERROR_SYS_OK);
    // create thread pool
    ThreadPoolParams params = tpParams_;  // [20, 50]
    params.minThrdCnt = 20;
    params.maxThrdCnt = 50;
    ThreadPool *pool = CreateThreadPoolEX(&params, (Scheduler *)multiScheduler, NULL, &errCode_);
    ASSERT_NE(pool, nullptr);
    // create task
    auto taskFunc = [](void *args) -> void * {
        runCnt++;
        idx = idx == 0 ? (int64_t)args : idx;
        while (runCnt > 0 && idx != runIdx); // all in loop, exclude runIdx thread
        return nullptr;
    };
    TaskId taskId;
    ThreadPoolTask task {taskFunc};
    // waitfunc
    auto waitTheadPoolThreadCnt = [](ThreadPool *pool, int expect) -> uint32_t {
        auto t1 = time(nullptr);
        while ((GetCurAliveThreadCnt(pool) != expect) && (time(nullptr) - t1 < 10)); // wait current thead numbers to become expect
        return GetCurAliveThreadCnt(pool);
    };
    auto waitRunCnt = [](int expect) -> uint32_t {
        auto t1 = time(nullptr);
        while ((runCnt != expect) && (time(nullptr) - t1 < 10)); // wait current thead numbers to become expect
        return runCnt;
    };
    // do test
    for (auto &tcase : testcase) {
        int64_t taskCnt = 0;
        switch (tcase[2]) {
            case 0: // shrink
                FAULT_INJECTION_ACTIVE(THREAD_POOL_SET_THREAD_CNT_FAULT_INJECT, FI_GLOBAL);
                SetThreadPoolThreadCnt(pool, tcase[0], tcase[1]);
                ASSERT_GE(waitTheadPoolThreadCnt(pool, tcase[0]), tcase[0]);
                FAULT_INJECTION_INACTIVE(THREAD_POOL_SET_THREAD_CNT_FAULT_INJECT, FI_GLOBAL);
                break;
            case 1: // expand
                for (int i = 0; i < 1000; i++) {
                    SetThreadPoolThreadCnt(pool, tcase[0], tcase[1]);
                    runIdx = taskCnt;  // hold the run first task thread, because need it to do expand routine
                    task.args = (void *)(intptr_t)taskCnt++;
                    ASSERT_EQ(ScheduleThreadPoolTask(pool, &task, &taskId), ERROR_SYS_OK);
                }
                ASSERT_LE(waitTheadPoolThreadCnt(pool, tcase[1]), tcase[1]);
                runCnt -= 1000;
                ASSERT_EQ(waitRunCnt(0), 0); // all task are runed complete
                break;
            default:
                ASSERT_TRUE(0);
        }
    }
    // clear
    DestroyThreadPool(pool);

    FAULT_INJECTION_INACTIVE(THREAD_POOL_SET_THREAD_CNT_FAULT_INJECT, FI_GLOBAL);
}

TEST_F(ThreadPoolTest, ThreadExitTest)
{
    FaultInjectionEntry entries[] = {
        FAULT_INJECTION_ENTRY(THREAD_POOL_SET_THREAD_CNT_FAULT_INJECT, false, nullptr),
    };
    ASSERT_EQ(RegisterFaultInjection(entries, sizeof(entries) / sizeof(entries[0]), FI_GLOBAL), ERROR_SYS_OK);
    FAULT_INJECTION_ACTIVE(THREAD_POOL_SET_THREAD_CNT_FAULT_INJECT, FI_GLOBAL);

    atomic_ulong runCnt(0);
    // create thread pool
    ThreadPoolParams params = tpParams_;  // [20, 50]
    // here set to minThrdCnt to 1, can increase the probability of problems occurring
    params.minThrdCnt = 1;
    ThreadPool *pool = CreateThreadPool(&params, nullptr, &errCode_);
    ASSERT_NE(pool, nullptr);
    // create task
    auto taskFunc = [](void *args) -> void * {
        (*(atomic_ulong *)args)++;
        return nullptr;
    };
    TaskId taskId;
    ThreadPoolTask task {taskFunc};
    task.args = &runCnt;
    // waitfunc
    auto waitTheadPoolThreadCnt = [](ThreadPool *pool, int expect) -> uint32_t {
        auto t1 = time(nullptr);
        while ((GetCurAliveThreadCnt(pool) != expect) && (time(nullptr) - t1 < 10)); // wait current thead numbers to become expect
        return GetCurAliveThreadCnt(pool);
    };
    auto waitRunCnt = [&](int expect) -> uint32_t {
        auto t1 = time(nullptr);
        while ((runCnt != expect) && (time(nullptr) - t1 < 10)); // wait current thead numbers to become expect
        ulong temp = runCnt;
        runCnt = 0;
        return temp;
    };
    // do test more than 2 times, becase we only check the moment of GetCurAliveThreadCnt() to minThrdCnt. this condition must be
    // occurred, but if the issue occurred, that is to say the GetCurAliveThreadCnt is became to 0. In this case, if we re-test
    // again, the thread poll will no can expansion, so waitRunCnt will timeout and then we found the issue and head off it.
    for (int i = 0; i < 10; i++) {
        int taskCount = 1000;
        usleep(1000);
        for (int i = 0; i < taskCount; i++) {
            ASSERT_EQ(ScheduleThreadPoolTask(pool, &task, &taskId), ERROR_SYS_OK);
        }
        ASSERT_EQ(waitRunCnt(taskCount), taskCount);
        ASSERT_GE(waitTheadPoolThreadCnt(pool, params.minThrdCnt + 1), params.minThrdCnt + 1);
    }
    // clear
    DestroyThreadPool(pool);

    FAULT_INJECTION_INACTIVE(THREAD_POOL_SET_THREAD_CNT_FAULT_INJECT, FI_GLOBAL);
}

TEST_F(ThreadPoolTest, BLFSchedulerTest)
{
    ErrorCode errorCode;
    // create scheduler
    BLFSchedulerInitParams initParam = {0};
    initParam.taskQueueSize = 2000;
    BLFScheduler *scheduler = NewBLFScheduler((TypeInitParams *)&initParam, &errorCode);
    // create thread pool
    const ThreadPoolParams params = {
        .flags = THREAD_POOL_FLAG_FIXED | THREAD_POOL_FLAG_STAT,
        .minThrdCnt = 15,
        .maxThrdCnt = 15,
        .stackSize = 0,
        .maxQueueSize = 50000,
        .idleTimeThreshold = 0,
        .threadEnterCallback = NULL,
        .threadExitCallback = NULL,
        {.name = "schedule_tp"},
        .cpuset = {},
    };
    ThreadPool *pool = CreateThreadPoolEX(&params, (Scheduler *)scheduler, NULL, &errorCode);
    ASSERT_NE(pool, nullptr);

    atomic_ulong runCnt(0);
    // create task
    auto taskFunc = [](void *args) -> void * {
        int sleep = (int)(((atomic_ulong *)args)->fetch_add(1) % 5) == 0;
        usleep(sleep);
        return nullptr;
    };
    TaskId taskId;
    ThreadPoolTask task {taskFunc};
    task.args = &runCnt;
    uint64_t testcount = 0;
    SetThreadPoolThreadCnt(pool, 14, 14);
    for (int i = 0; i < 500; i++) {
        for (int j = 0; j < 50; j++) {
            ScheduleThreadPoolTask(pool, &task, &taskId);
            testcount++;
        }
        /* slow submit */
        usleep(1000);
    }
    for (int i = 0; i < 10000; i++) {
        /* fast submit */
        ScheduleThreadPoolTask(pool, &task, &taskId);
        testcount++;
    }

    while (testcount != runCnt) {
        usleep(1);
    }
    ASSERT_EQ(GetCurAliveThreadCnt(pool), 14);
    DestroyThreadPool(pool);
}
