
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
 * ut_port_thread.cpp
 * Developer test of thread.
 *
 * ---------------------------------------------------------------------------------
 */
#include <gtest/gtest.h>
#include <mockcpp/mockcpp.hpp>
#include "securec.h"
#include "port/platform_port.h"

/** ************************************************************************************************************* **/
/*
 * The thread test function. Increases the value of the input parameter by 1,
 * and then returns the input parameter.
 */
void *ThreadRoutineAddOne(void *arg)
{
    int *intPtr = (int *) arg;
    ThreadYield();
    *intPtr += 1;
    ThreadExit(arg);
    return arg;
}

/*
 * The thread test function. Wait until the input parameter value changes from false to true, and then exit.
 */
#define SLEEP_MICROSECONDS_COUNT    (1000 * 1)

void *ThreadRoutineWaitingConditionalExit(void *arg)
{
    bool *boolPtr = (bool *) arg;
    while (!(*boolPtr)) {
        Usleep(SLEEP_MICROSECONDS_COUNT);
    }
    ThreadExit(arg);
    return arg;
}

/*
 * The thread test function.Test that objects obtained by different threads using the same key do
 * not affect each other.
 */
ThreadKey g_threadLocalKey;

void *ThreadRoutineLocalKeyOperation(void *arg)
{
    int integerValue = 0;
    ErrorCode errorCode = ThreadSetSpecific(g_threadLocalKey, &integerValue);
    if (errorCode != ERROR_SYS_OK) {
        return arg;
    }
    int *integerValuePtr = (int *) ThreadGetSpecific(g_threadLocalKey);
    *integerValuePtr += 1;
    return arg;
}

/** ************************************************************************************************************* **/

class ThreadTest : public testing::Test {
public:
    void SetUp() override
    {
        return;
    };

    void TearDown() override
    {
        GlobalMockObject::verify();
    };
};

TEST_F(ThreadTest, TestPid2StringException)
{          
#define PID_VALUE_STRING_LEN          4
    Pid pid;
    char pidStr[PID_VALUE_STRING_LEN] = {0};
    SetPid(&pid, 9999);

    Pid2String(&pid, pidStr, PID_VALUE_STRING_LEN);
    EXPECT_STREQ(pidStr, "???");
}

TEST_F(ThreadTest, TestTid2StringException)
{          
#define TID_VALUE_STRING_LEN          4
    Tid tid;
    char tidStr[TID_VALUE_STRING_LEN] = {0};
    SetTid(&tid, 9999);

    Tid2String(&tid, tidStr, TID_VALUE_STRING_LEN);
    EXPECT_STREQ(tidStr, "???");
}

/**
 * @tc.name:  ThreadIdentifierFunction001_Level0
 * @tc.desc:  Test the process ID, thread ID, and name API.
 * @tc.type: FUNC
 */
TEST_F(ThreadTest, ThreadIdentifierFunction001_Level0)
{
/**
 * @tc.steps: step1. Test the process ID functions.
 * @tc.expected: step1.The PID functions are correct.
 */
#define PID_VALUE_INTEGER             9999
#define PID_VALUE_STRING             "9999"
#define PID_VALUE_STRING_LEN          5
#define PID_DIFFERENT_VALUE_INTEGER   8888
    Pid pid1;
    Pid pid2;
    char pidStr[PID_VALUE_STRING_LEN] = {0};
    SetPid(&pid1, PID_VALUE_INTEGER);
    Pid2String(&pid1, pidStr, PID_VALUE_STRING_LEN);
    ASSERT_TRUE(!strcmp(pidStr, PID_VALUE_STRING));
    SetPid(&pid2, PID_VALUE_INTEGER);
    ASSERT_TRUE(PidIsEqual(&pid1, &pid2));
    SetPid(&pid2, PID_DIFFERENT_VALUE_INTEGER);
    ASSERT_FALSE(PidIsEqual(&pid1, &pid2));
    pid1 = GetCurrentPid();
    pid2 = GetCurrentPid();
    ASSERT_TRUE(PidIsEqual(&pid1, &pid2));

    /**
     * @tc.steps: step1. Test the thread ID functions.
     * @tc.expected: step1.The TID functions are correct.
     */
#define TID_VALUE_INTEGER             9999
#define TID_VALUE_STRING             "9999"
#define TID_VALUE_STRING_LEN          5
#define TID_DIFFERENT_VALUE_INTEGER   8888
    Tid tid1;
    Tid tid2;
    char tidStr[TID_VALUE_STRING_LEN] = {0};
    SetTid(&tid1, TID_VALUE_INTEGER);
    Tid2String(&tid1, tidStr, TID_VALUE_STRING_LEN);
    ASSERT_TRUE(!strcmp(tidStr, TID_VALUE_STRING));
    SetTid(&tid2, TID_VALUE_INTEGER);
    ASSERT_TRUE(TidIsEqual(&tid1, &tid2));
    SetTid(&tid2, TID_DIFFERENT_VALUE_INTEGER);
    ASSERT_FALSE(TidIsEqual(&tid1, &tid2));
    tid1 = GetCurrentTid();
    tid2 = GetCurrentTid();
    ASSERT_TRUE(TidIsEqual(&tid1, &tid2));

    /**
    * @tc.steps: step1. Test the thread name functions.
    * @tc.expected: step1.The name functions are correct.
    */
#define TEST_THREAD_NAME   "Test thread"
#define TEST_THREAD_NAME_LEN  20
    ErrorCode errorCode;
    errorCode = ThreadSetName(TEST_THREAD_NAME);
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    char threadName[TEST_THREAD_NAME_LEN] = {0};
    errorCode = ThreadGetName(threadName, TEST_THREAD_NAME_LEN);
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    ASSERT_TRUE(!strcmp(threadName, TEST_THREAD_NAME));
    char threadName2[12] = {0};
    /* according to man pthread_getname_np, the length of the char array must be at least 16. */
    errorCode = ThreadGetName(threadName2, 12);
    EXPECT_EQ(errorCode, ERROR_UTILS_PORT_ERANGE);

#define TEST_THREAD_NAME_TOO_LONG "0123456789ABCDEF"
    errorCode = ThreadSetName(TEST_THREAD_NAME_TOO_LONG);
    EXPECT_EQ(errorCode, ERROR_UTILS_PORT_ERANGE);
}

static bool g_threadRunningConditionVariable = false;

/**
 * @tc.name:  ThreadRunningFunction001_Level0
 * @tc.desc:  Test the thread create,exit,join API.
 * @tc.type: FUNC
 */
TEST_F(ThreadTest, ThreadRunningFunction001_Level0)
{
    /**
    * @tc.steps: step1. Test the thread create, exit and join functions.
    * @tc.expected: step1.The thread functions are correct.
    */
#define INITIAL_INTEGER_VALUE  1
#define RESULT_INTEGER_VALUE   2
    ErrorCode errorCode;
    Tid tid;
    int initValue = INITIAL_INTEGER_VALUE;
    errorCode = ThreadCreate(&tid, ThreadRoutineAddOne, &initValue);
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    int *resultValue;
    errorCode = ThreadJoin(tid, (void **) &resultValue);
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    EXPECT_EQ(*resultValue, RESULT_INTEGER_VALUE);

    /**
    * @tc.steps: step2. Test the thread detach and priority functions.
    * @tc.expected: step2.The thread functions are correct.
    */
    ThreadPriority pri;
    g_threadRunningConditionVariable = false;
    errorCode = ThreadCreate(&tid, ThreadRoutineWaitingConditionalExit, &g_threadRunningConditionVariable);
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    errorCode = ThreadDetach(tid);
    EXPECT_EQ(errorCode, ERROR_SYS_OK);

    errorCode = ThreadSetPriority(tid, THR_PRI_LOW);
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    errorCode = ThreadGetPriority(tid, &pri);
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    EXPECT_EQ(pri, THR_PRI_LOW);

    errorCode = ThreadSetPriority(tid, THR_PRI_MIDDLE);
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    errorCode = ThreadGetPriority(tid, &pri);
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    EXPECT_EQ(pri, THR_PRI_LOW);

    errorCode = ThreadSetPriority(tid, THR_PRI_HIGH);
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    errorCode = ThreadGetPriority(tid, &pri);
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    EXPECT_EQ(pri, THR_PRI_LOW);

    g_threadRunningConditionVariable = true;
}

/**
 * @tc.name:  ThreadKeyFunction001_Level0
 * @tc.desc:  Test the thread local key create,get,set and delete API.
 * @tc.type: FUNC
 */
#define THREAD_NUMBERS   4
TEST_F(ThreadTest, ThreadKeyFunction001_Level0)
{
    /**
    * @tc.steps: step1. Create thread key and set local key.
    * @tc.expected: step1.The thread functions are correct.
    */
    ErrorCode errorCode;
    errorCode = ThreadKeyCreate(&g_threadLocalKey);
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    int localValue = 0;
    errorCode = ThreadSetSpecific(g_threadLocalKey, &localValue);
    EXPECT_EQ(errorCode, ERROR_SYS_OK);

    /**
    * @tc.steps: step2. Create threads and each thread operates its own key.
    * @tc.expected: step2.The thread functions are correct.
    */
    Tid tid[THREAD_NUMBERS];
    int i;
    int integerValue = 0;
    for (i = 0; i < THREAD_NUMBERS; i++) {
        errorCode = ThreadCreate(&tid[i], ThreadRoutineLocalKeyOperation, &integerValue);
        EXPECT_EQ(errorCode, ERROR_SYS_OK);
    }
    int *valuePtr;
    for (i = 0; i < THREAD_NUMBERS; i++) {
        errorCode = ThreadJoin(tid[i], (void **) &valuePtr);
        EXPECT_EQ(errorCode, ERROR_SYS_OK);
    }
    /**
    * @tc.steps: step3. Obtain the key of the thread and verify that the value is not changed.
    * @tc.expected: step3.The thread functions are correct.
    */
    int *localValuePtr = (int *) ThreadGetSpecific(g_threadLocalKey);
    EXPECT_EQ(*localValuePtr, localValue);
    errorCode = ThreadKeyDelete(g_threadLocalKey);
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
}