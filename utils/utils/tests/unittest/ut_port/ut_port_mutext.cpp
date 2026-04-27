
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
 * ut_port_mutex.cpp
 * Developer test of mutex.
 *
 * ---------------------------------------------------------------------------------
 */
#include <gtest/gtest.h>
#include <mockcpp/mockcpp.hpp>
#include "port/platform_port.h"

/** ************************************************************************************************************* **/
#define MUTEX_LOCK_TEST_MAX_WAIT_TIME_MS 10000  /* 10 seconds */
Mutex g_mutex;

struct MutexLockGuard {
    bool isLocked = false;
    bool canUnlock = false;
};

static bool TestWaitBool(const bool *waitArgs, bool target)
{
    if (*waitArgs == target) {
        return true;
    }
    for (int i = 0; i < MUTEX_LOCK_TEST_MAX_WAIT_TIME_MS; ++i) {
        Usleep(1000);
        if (*waitArgs == target) {
            return true;
        }
    }
    return false;
}

/*
 * The mutex thread function. Lock and sleep for a specified time.
 */
void *MutexLockThreadFunc(void *args)
{
    auto *lockGuard = (MutexLockGuard *)args;
    MutexLock(&g_mutex);
    lockGuard->isLocked = true;
    EXPECT_TRUE(TestWaitBool(&(lockGuard->canUnlock), true));
    MutexUnlock(&g_mutex);
    return args;
}

ConditionVariable g_cond;
bool g_variable = false;

struct MutexCondGuard {
    bool isWaitingCond = false;
    bool isSignal = false;
    bool isWakeUp = false;
    bool result = false;
    void Reset()
    {
        isWaitingCond = isSignal = isWakeUp = result = false;
    }
};

void *MutexLockAndWaitCondVarTimeout(void *args)
{
    auto *guard = (MutexCondGuard *)args;
    bool rc = MutexTrylock(&g_mutex);
    EXPECT_TRUE(rc);
    if (!rc) {
        return nullptr;
    }
    guard->isWaitingCond = true;
    rc = ConditionVariableTimedWait(&g_cond, &g_mutex, MUTEX_LOCK_TEST_MAX_WAIT_TIME_MS);
    EXPECT_TRUE(rc);
    if (rc) {
        guard->isWakeUp = true;
        MutexUnlock(&g_mutex);
        guard->result = true;
    }
    return args;
}

void *MutexLockAndWaitCondVar(void *args)
{
    auto *guard = (MutexCondGuard *)args;
    bool rc = MutexTrylock(&g_mutex);
    EXPECT_TRUE(rc);
    if (!rc) {
        return nullptr;
    }
    guard->isWaitingCond = true;
    ConditionVariableWait(&g_cond, &g_mutex);
    guard->isWakeUp = true;
    MutexUnlock(&g_mutex);
    guard->result = true;
    return args;
}

void *MutexLockAndSignalCondVar(void *args)
{
    auto *guard = (MutexCondGuard *)args;
    EXPECT_TRUE(TestWaitBool(&(guard->isWaitingCond), true));
    MutexLock(&g_mutex);
    ConditionVariableSignal(&g_cond);
    guard->isSignal = true;
    MutexUnlock(&g_mutex);
    return args;
}

void *MutexLockAndBroadcastCondVar(void *args)
{
    auto *guard = (MutexCondGuard *)args;
    EXPECT_TRUE(TestWaitBool(&(guard->isWaitingCond), true));
    MutexLock(&g_mutex);
    ConditionVariableBroadcast(&g_cond);
    guard->isSignal = true;
    MutexUnlock(&g_mutex);
    return args;
}

/** ************************************************************************************************************* **/

class MutexTest : public testing::Test {
public:
    void SetUp() override
    {
        g_variable = false;
    };

    void TearDown() override
    {
        GlobalMockObject::verify();
    };
};

/**
 * @tc.name:  MutexFunction001_Level0
 * @tc.desc:  Test the mutex create,lock,unlock and destroy.
 * @tc.type: FUNC
 */
TEST_F(MutexTest, MutexFunction001_Level0)
{
    /**
    * @tc.steps: step1. Test the single thread mutex.
    * @tc.expected: step1.The mutex functions are correct.
    */
    MutexInit(&g_mutex);
    MutexLock(&g_mutex);
    MutexUnlock(&g_mutex);

    /**
    * @tc.steps: step2. Create child thread and lock mutex, then main thread try lock mutex.
    * @tc.expected: step2. Child thread lock success, main thread try lock failed.
    */
    ErrorCode errorCode;
    Tid tid;
    MutexLockGuard lockGuard;
    errorCode = ThreadCreate(&tid, MutexLockThreadFunc, &lockGuard);
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    ASSERT_TRUE(TestWaitBool(&(lockGuard.isLocked), true));
    bool rc;
    rc = MutexTrylock(&g_mutex);
    ASSERT_FALSE(rc);

    /**
    * @tc.steps: step3. Child thread unlock mutex, then main thread try lock mutex.
    * @tc.expected: step3. Child thread unlock success, main thread try lock success.
    */
    lockGuard.canUnlock = true;
    errorCode = ThreadJoin(tid, nullptr);
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    rc = MutexTrylock(&g_mutex);
    ASSERT_TRUE(rc);
    MutexUnlock(&g_mutex);
    MutexDestroy(&g_mutex);
}

/**
 * @tc.name:  ConditionVariableFunction001_Level0
 * @tc.desc:  Test the condition variable create,wait,signal and destroy.
 * @tc.type: FUNC
 */
TEST_F(MutexTest, ConditionVariableFunction001_Level0)
{
    /**
    * @tc.steps: step1. Create the child thread and wait child thread signal.
    * @tc.expected: step1.First wait timeout,second wait succeed.
    */
    MutexInit(&g_mutex);
    ConditionVariableInit(&g_cond);

    Tid waitThread, signalThread;
    MutexCondGuard guard;
    ASSERT_EQ(ThreadCreate(&waitThread, MutexLockAndWaitCondVarTimeout, &guard), 0);
    ASSERT_EQ(ThreadCreate(&signalThread, MutexLockAndSignalCondVar, &guard), 0);
    ASSERT_TRUE(TestWaitBool(&(guard.isSignal), true));
    ASSERT_EQ(ThreadJoin(waitThread, nullptr), 0);
    ASSERT_EQ(ThreadJoin(signalThread, nullptr), 0);
    ASSERT_TRUE(guard.result);

    guard.Reset();
    ASSERT_EQ(ThreadCreate(&waitThread, MutexLockAndWaitCondVar, &guard), 0);
    ASSERT_EQ(ThreadCreate(&signalThread, MutexLockAndSignalCondVar, &guard), 0);
    ASSERT_TRUE(TestWaitBool(&(guard.isSignal), true));
    ASSERT_TRUE(TestWaitBool(&(guard.isWakeUp), true));
    ASSERT_EQ(ThreadJoin(waitThread, nullptr), 0);
    ASSERT_EQ(ThreadJoin(signalThread, nullptr), 0);
    ASSERT_TRUE(guard.result);

    MutexDestroy(&g_mutex);
    ConditionVariableDestroy(&g_cond);
}

/**
 * @tc.name:  ConditionVariableFunction002_Level0
 * @tc.desc:  Test the condition variable create,wait,broadcast and destroy.
 * @tc.type: FUNC
 */
TEST_F(MutexTest, ConditionVariableFunction002_Level0)
{
    /**
    * @tc.steps: step1. Create the child thread and wait child thread broadcast.
    * @tc.expected: step1.Wait forever until broadcast.
    */
    MutexInit(&g_mutex);
    ConditionVariableInit(&g_cond);

    Tid waitThread1, waitThread2, signalThread;
    MutexCondGuard guard1, guard2;
    ASSERT_EQ(ThreadCreate(&waitThread1, MutexLockAndWaitCondVar, &guard1), 0);
    ASSERT_TRUE(TestWaitBool(&(guard1.isWaitingCond), true));
    ASSERT_EQ(ThreadCreate(&waitThread2, MutexLockAndWaitCondVar, &guard2), 0);
    ASSERT_TRUE(TestWaitBool(&(guard2.isWaitingCond), true));
    ASSERT_EQ(ThreadCreate(&signalThread, MutexLockAndBroadcastCondVar, &guard1), 0);
    ASSERT_TRUE(TestWaitBool(&(guard1.isSignal), true));
    ASSERT_TRUE(TestWaitBool(&(guard1.isWakeUp), true));
    ASSERT_TRUE(TestWaitBool(&(guard2.isWakeUp), true));
    ASSERT_EQ(ThreadJoin(waitThread1, nullptr), 0);
    ASSERT_EQ(ThreadJoin(waitThread2, nullptr), 0);
    ASSERT_EQ(ThreadJoin(signalThread, nullptr), 0);
    ASSERT_TRUE(guard1.result);
    ASSERT_TRUE(guard2.result);

    MutexDestroy(&g_mutex);
    ConditionVariableDestroy(&g_cond);
}
