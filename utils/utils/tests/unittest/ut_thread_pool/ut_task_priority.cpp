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
 * Description: unit test for lwlock
 */

#include <gtest/gtest.h>
#include <thread>
#include <vector>
#include <atomic>
#include <functional>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include "thread/thread_pool_internal.h"
#include "schedule/fifo_scheduler.h"
#include "container/linked_list.h"
#include "defines/utils_errorcode.h"
#include "memory/memory_allocator.h"
#include "thread/thread_pool.h"
#include "schedule/multiqueue_scheduler.h"
#include "fault_injection/fault_injection.h"

using namespace std;

class ThreadPoolTaskPriorityTest : public testing::Test {
public:
    static void SetUpTestSuite() { }

    static void TearDownTestSuite() { CloseLogger(); }

    void SetUp() override { };

    void TearDown() override { };
};

static void *MyAlloc(MemAllocator *self, size_t size)
{
    return malloc(size);
}

static void MyFree(MemAllocator *self, void *p)
{
    free(p);
}

static MemAllocator alloctor = {
    .context = NULL,
    .alloc = MyAlloc,
    .free  = MyFree,
};

#define COMM_TASK_VAL   1
#define EMGC_TASK_VAL   2
static atomic_int walk(0);
static int testdata = 0;

void *TestMain1(void *arg)
{
    testdata = COMM_TASK_VAL;
    atomic_uint *count = (atomic_uint *)arg;
    (*count)++;
    CloseLogger();
    return NULL;
}

void *TestMain2(void *arg)
{
    testdata = EMGC_TASK_VAL;
    atomic_uint *count = (atomic_uint *)arg;
    (*count)++;
    CloseLogger();
    return NULL;
}

void setwalk(const FaultInjectionEntry *entry, int nouse)
{
    walk++;
}

#define THEAD_POOL_DEFAULT  10, 20, 0, 2000, 0, NULL, NULL,
#define THEAD_POOL_1_THREAD 1, 1, 0, 2000, 0, NULL, NULL,

TEST_F(ThreadPoolTaskPriorityTest, TaskRealTimeFlagtest001)
{
    FaultInjectionEntry entries[] = {
        FAULT_INJECTION_ENTRY(THREAD_POOL_FLAG_REALTIME_FAULT_INJECT, false, setwalk),
    };
    ASSERT_EQ(RegisterFaultInjection(entries, sizeof(entries) / sizeof(entries[0]), FI_GLOBAL), ERROR_SYS_OK);
    FAULT_INJECTION_ACTIVE(THREAD_POOL_FLAG_REALTIME_FAULT_INJECT, FI_GLOBAL);

    ErrorCode errCode = ERROR_SYS_OK;
    ThreadPoolParams params = {THREAD_POOL_FLAG_FIXED | THREAD_POOL_FLAG_REALTIME, THEAD_POOL_DEFAULT};

    ThreadPool *pool1 = CreateThreadPool(&params, &alloctor, &errCode);
    ASSERT_NE(pool1, nullptr);

    atomic_uint runCount(0);
    ThreadPoolTask task {};
    task.routine = TestMain1;
    task.args = &runCount;

    MultiQueueSchedulerInitParams initParam = {
        .super = params,
        .allocator = &alloctor,
        .taskQueueCnt = 10,
        .maxQueueSize = 100000,
    };

    MultiQueueScheduler *multiScheduler = NewMultiQueueScheduler((TypeInitParams *)&initParam, &errCode);
    ThreadPool *pool2 = CreateThreadPoolEX(&initParam.super, (Scheduler *)multiScheduler, &alloctor, &errCode);
    ASSERT_NE(pool2, nullptr);

    TaskId taskId;
    errCode = ScheduleThreadPoolTask(pool1, &task, &taskId);
    ASSERT_EQ(errCode, ERROR_SYS_OK);

    errCode = ScheduleThreadPoolTask(pool2, &task, &taskId);
    ASSERT_EQ(errCode, ERROR_SYS_OK);

    DestroyThreadPool(pool1);
    DestroyThreadPool(pool2);
    ASSERT_EQ(walk, 10 * 2); // taskQueueCnt * 2

    FAULT_INJECTION_INACTIVE(THREAD_POOL_FLAG_REALTIME_FAULT_INJECT, FI_GLOBAL);
    CloseLogger();
    DestroyFaultInjectionHash(FI_GLOBAL);
}

TEST_F(ThreadPoolTaskPriorityTest, SubmitTaskEmergencytest001)
{
    vector<thread> th(2);
    atomic_uint runCount(0);
    atomic_uint submitCount(0);
    ErrorCode errCode = ERROR_SYS_OK;
    ThreadPoolParams params = {THREAD_POOL_FLAG_SCALING, THEAD_POOL_DEFAULT};

    ThreadPool *pool1 = CreateThreadPool(&params, &alloctor, &errCode);
    ASSERT_NE(pool1, nullptr);

    MultiQueueSchedulerInitParams initParam = {
        .super = params,
        .allocator = &alloctor,
        .taskQueueCnt = 10,
        .maxQueueSize = 100000,
    };

    MultiQueueScheduler *multiScheduler = NewMultiQueueScheduler((TypeInitParams *)&initParam, &errCode);
    ThreadPool *pool2 = CreateThreadPoolEX(&initParam.super, (Scheduler *)multiScheduler, &alloctor, &errCode);
    ASSERT_NE(pool2, nullptr);

    ThreadPoolTask task {};
    task.routine = TestMain2;
    task.args = &runCount;

    auto submitTask = [&] () {
        TaskId taskId;
        while (submitCount < 100) {
            if (ScheduleThreadPoolTaskEmgc(pool1, &task, &taskId) == ERROR_SYS_OK) {
                submitCount++;
            } 
            if (ScheduleThreadPoolTaskEmgc(pool2, &task, &taskId) == ERROR_SYS_OK) {
                submitCount++;
            } 
        }
        CloseLogger();
    };

    for (auto &t : th) {
        t = thread(submitTask);
    } 
    for (auto &t : th) {
        t.join();
    } 

    auto t1 = time(nullptr);
    while ((runCount != submitCount) && (time(nullptr) - t1 < 2)); // run all task in < 2s, time() precision is second.
    ASSERT_EQ(runCount, submitCount);
    DestroyThreadPool(pool2);
    DestroyThreadPool(pool1);
}

TEST_F(ThreadPoolTaskPriorityTest, FiFoEmergencyPriorityTest)
{

    FaultInjectionEntry entries[] = {
        FAULT_INJECTION_ENTRY(THREAD_POOL_GET_TASK_FAULT_INJECT, false, nullptr),
    };
    ASSERT_EQ(RegisterFaultInjection(entries, sizeof(entries) / sizeof(entries[0]), FI_GLOBAL), ERROR_SYS_OK);

    // enable fault before create thread pool
    FAULT_INJECTION_ACTIVE(THREAD_POOL_GET_TASK_FAULT_INJECT, FI_GLOBAL);

    ErrorCode errCode = ERROR_SYS_OK;
    ThreadPoolParams params = {THREAD_POOL_FLAG_FIXED, THEAD_POOL_1_THREAD}; // maxThrdCnt = 1
    ThreadPool *pool1 = CreateThreadPool(&params, &alloctor, &errCode);
    ASSERT_NE(pool1, nullptr);

    atomic_uint runCount(0);
    ThreadPoolTask commonTask {};
    commonTask.routine = TestMain1;
    commonTask.args = &runCount;

    ThreadPoolTask emgcTask {};
    emgcTask.routine = TestMain2;
    emgcTask.args = &runCount;

    TaskId taskId;
    ASSERT_EQ(ScheduleThreadPoolTask(pool1, &commonTask, &taskId), ERROR_SYS_OK);
    ASSERT_EQ(ScheduleThreadPoolTaskEmgc(pool1, &emgcTask, &taskId), ERROR_SYS_OK);
    FAULT_INJECTION_NOTIFY(THREAD_POOL_GET_TASK_FAULT_INJECT);
    auto t1 = time(nullptr);
    while ((runCount != 2) && (time(nullptr) - t1 < 2)); // run all task in < 2s, time() precision is second.
    ASSERT_TRUE(runCount == 2);
    ASSERT_TRUE(testdata == COMM_TASK_VAL);  // commonTask TestMain1 write testdata = COMM_TASK_VAL;
    DestroyThreadPool(pool1);
    CloseLogger();
    DestroyFaultInjectionHash(FI_GLOBAL);
}

TEST_F(ThreadPoolTaskPriorityTest, MQEmergencyPriorityTest)
{

    FaultInjectionEntry entries[] = {
        FAULT_INJECTION_ENTRY(THREAD_POOL_GET_TASK_FAULT_INJECT, false, nullptr),
    };
    ASSERT_EQ(RegisterFaultInjection(entries, sizeof(entries) / sizeof(entries[0]), FI_GLOBAL), ERROR_SYS_OK);
    // enable fault before create thread pool
    FAULT_INJECTION_ACTIVE(THREAD_POOL_GET_TASK_FAULT_INJECT, FI_GLOBAL);

    ErrorCode errCode = ERROR_SYS_OK;
    ThreadPoolParams params = {THREAD_POOL_FLAG_FIXED, THEAD_POOL_1_THREAD}; // maxThrdCnt = 1
    // MultiQueueScheduler
    MultiQueueSchedulerInitParams initParam = {
        .super = params,
        .allocator = &alloctor,
        .taskQueueCnt = 10,
        .maxQueueSize = 100000,
    };
    MultiQueueScheduler *multiScheduler = NewMultiQueueScheduler((TypeInitParams *)&initParam, &errCode);
    ThreadPool *pool2 = CreateThreadPoolEX(&initParam.super, (Scheduler *)multiScheduler, &alloctor, &errCode);
    ASSERT_NE(pool2, nullptr);

    atomic_uint runCount(0);
    ThreadPoolTask commonTask {};
    commonTask.routine = TestMain1;
    commonTask.args = &runCount;

    ThreadPoolTask emgcTask {};
    emgcTask.routine = TestMain2;
    emgcTask.args = &runCount;

    TaskId taskId;
    ASSERT_EQ(ScheduleThreadPoolTask(pool2, &commonTask, &taskId), ERROR_SYS_OK);
    ASSERT_EQ(ScheduleThreadPoolTaskEmgc(pool2, &emgcTask, &taskId), ERROR_SYS_OK);
    FAULT_INJECTION_NOTIFY(THREAD_POOL_GET_TASK_FAULT_INJECT);
    auto t2 = time(nullptr);
    while ((runCount != 2) && (time(nullptr) - t2 < 2)); // run all task in < 2s, time() precision is second.
    ASSERT_TRUE(runCount == 2);
    ASSERT_TRUE(testdata == COMM_TASK_VAL);  // commonTask TestMain1 write testdata = COMM_TASK_VAL;
    DestroyThreadPool(pool2);
    CloseLogger();
    DestroyFaultInjectionHash(FI_GLOBAL);
}

TEST_F(ThreadPoolTaskPriorityTest, FiFoMixHighAndLowPriorityTest)
{

    FaultInjectionEntry entries[] = {
        FAULT_INJECTION_ENTRY(THREAD_POOL_GET_TASK_FAULT_INJECT, false, nullptr),
    };
    ASSERT_EQ(RegisterFaultInjection(entries, sizeof(entries) / sizeof(entries[0]), FI_GLOBAL), ERROR_SYS_OK);

    ErrorCode errCode = ERROR_SYS_OK;
    ThreadPoolParams params = {THREAD_POOL_FLAG_FIXED, THEAD_POOL_DEFAULT};

    FAULT_INJECTION_ACTIVE(THREAD_POOL_GET_TASK_FAULT_INJECT, FI_GLOBAL);
    ThreadPool *fifoPool = CreateThreadPool(&params, &alloctor, &errCode);
    ASSERT_NE(fifoPool, nullptr);

    FAULT_INJECTION_ACTIVE(THREAD_POOL_GET_TASK_FAULT_INJECT2, FI_GLOBAL);

    atomic_uint runCount(0);
    ThreadPoolTask commonTask {};
    commonTask.routine = TestMain1;
    commonTask.args = &runCount;

    ThreadPoolTask emgcTask {};
    emgcTask.routine = TestMain2;
    emgcTask.args = &runCount;

    TaskId taskId;
    auto testCount = 100;
    // put task
    for (int i = 0; i < testCount; i++) {
        ASSERT_EQ(ScheduleThreadPoolTask(fifoPool, &commonTask, &taskId), ERROR_SYS_OK);
        ASSERT_EQ(ScheduleThreadPoolTaskEmgc(fifoPool, &emgcTask, &taskId), ERROR_SYS_OK);
    }

    FAULT_INJECTION_NOTIFY(THREAD_POOL_GET_TASK_FAULT_INJECT);
    auto t1 = time(nullptr);
    while ((runCount != 2 * testCount) && (time(nullptr) - t1 < 2)); // run all task in < 2s, time() precision is second.
    ASSERT_TRUE(runCount == 2 * testCount);
    runCount = 0;

    DestroyThreadPool(fifoPool);
    CloseLogger();
    DestroyFaultInjectionHash(FI_GLOBAL);
}

TEST_F(ThreadPoolTaskPriorityTest, MQMixHighAndLowPriorityTest)
{

    FaultInjectionEntry entries[] = {
        FAULT_INJECTION_ENTRY(THREAD_POOL_GET_TASK_FAULT_INJECT, false, nullptr),
    };
    ASSERT_EQ(RegisterFaultInjection(entries, sizeof(entries) / sizeof(entries[0]), FI_GLOBAL), ERROR_SYS_OK);

    ErrorCode errCode = ERROR_SYS_OK;
    ThreadPoolParams params = {THREAD_POOL_FLAG_FIXED, THEAD_POOL_DEFAULT};

    atomic_uint runCount(0);
    ThreadPoolTask commonTask {};
    commonTask.routine = TestMain1;
    commonTask.args = &runCount;

    ThreadPoolTask emgcTask {};
    emgcTask.routine = TestMain2;
    emgcTask.args = &runCount;

    TaskId taskId;
    auto testCount = 100;

    FAULT_INJECTION_NOTIFY(THREAD_POOL_GET_TASK_FAULT_INJECT);

    MultiQueueSchedulerInitParams initParam = {
        .super = params,
        .allocator = &alloctor,
        .taskQueueCnt = 10,
        .maxQueueSize = 100000,
    };

    MultiQueueScheduler *multiScheduler = NewMultiQueueScheduler((TypeInitParams *)&initParam, &errCode);
    ThreadPool *mqPool = CreateThreadPoolEX(&initParam.super, (Scheduler *)multiScheduler, &alloctor, &errCode);
    ASSERT_NE(mqPool, nullptr);

    // put task
    for (int i = 0; i < testCount; i++) {
        ASSERT_EQ(ScheduleThreadPoolTask(mqPool, &commonTask, &taskId), ERROR_SYS_OK);
        ASSERT_EQ(ScheduleThreadPoolTaskEmgc(mqPool, &emgcTask, &taskId), ERROR_SYS_OK);
    }

    FAULT_INJECTION_NOTIFY(THREAD_POOL_GET_TASK_FAULT_INJECT);
    auto t2 = time(nullptr);
    while ((runCount != 2 * testCount) && (time(nullptr) - t2 < 2)); // run all task in < 2s, time() precision is second.
    ASSERT_TRUE(runCount == 2 * testCount);

    DestroyThreadPool(mqPool);
    CloseLogger();
    DestroyFaultInjectionHash(FI_GLOBAL);
}
