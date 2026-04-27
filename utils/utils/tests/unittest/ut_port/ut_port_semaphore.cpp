
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
 * ut_port_semaphore.cpp
 * Developer test of semaphore.
 *
 * ---------------------------------------------------------------------------------
 */
#include <gtest/gtest.h>
#include <mockcpp/mockcpp.hpp>
#include "securec.h"
#include "port/platform_port.h"

/** ************************************************************************************************************* **/

struct SemaphoreInterruptsTest {
    bool isFirstCall;
    bool interruptOKFirstCallback;
    bool interruptOKSecondCallback;
    bool interruptCallback;
};
/* Initialize the variable member to the opposite value.*/
struct SemaphoreInterruptsTest g_semaphoreInterruptsTest = {true, false, true, false};

void SemaphoreInterruptsTestReset()
{
    g_semaphoreInterruptsTest.isFirstCall = true;
    g_semaphoreInterruptsTest.interruptOKFirstCallback = false;
    g_semaphoreInterruptsTest.interruptOKSecondCallback = true;
    g_semaphoreInterruptsTest.interruptCallback = false;
}
void EnableInterrupts(void *threadContext, bool interruptOK)
{
    struct SemaphoreInterruptsTest *semaphoreInterruptsTest = (struct SemaphoreInterruptsTest *)threadContext;
    if (semaphoreInterruptsTest->isFirstCall) {
        semaphoreInterruptsTest->interruptOKFirstCallback = interruptOK;
        semaphoreInterruptsTest->isFirstCall = false;
    } else {
        semaphoreInterruptsTest->interruptOKSecondCallback = interruptOK;
    }
}

void ProcessInterrupts(void *threadContext)
{
    struct SemaphoreInterruptsTest *semaphoreInterruptsTest = (struct SemaphoreInterruptsTest *)threadContext;
    semaphoreInterruptsTest->interruptCallback = true;
}

Semaphore g_semaphoreConcurrentLock;
bool g_semaphoreConcurrentFlag = false;

/*
 * The Concurrent lock thread function. Lock and set g_semaphoreConcurrentFlag as true.
 */
void *SemaphoreConcurrentLockThreadFunc(void *args)
{
    SemaphoreLock(&g_semaphoreConcurrentLock, true);
    g_semaphoreConcurrentFlag = true;
    SemaphoreUnlock(&g_semaphoreConcurrentLock);
    return args;
}
/** ************************************************************************************************************* **/

class SemaphoreTest : public testing::Test {
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

#define TEST_SEMAPHORE_KEY_VALUE 9432000
#define SEMAPHORE_LOCK_TEST_MAX_WAIT_TIME_MS 1000 /* 1 seconds */
#define TEST_MAX_SEMAPHORE 33
/**
 * @tc.name:  SemaphoreFunction001_Level0
 * @tc.desc:  Test the semaphore create,lock,unlock and destroy.
 * @tc.type: FUNC
 */
TEST_F(SemaphoreTest, SemaphoreFunction001_Level0)
{
    /**
     * @tc.steps: step1. Test basic semaphore operating functions
     * @tc.expected: step1.The semaphore functions are correct.
     */
    int semaphoreKey = TEST_SEMAPHORE_KEY_VALUE;
    Semaphore semaphore;
    SemaphoreCreate(semaphoreKey, NULL, &semaphore);
    SemaphoreLock(&semaphore, true);
    SemaphoreUnlock(&semaphore);
#define TEST_SEMAPHORE_LOCK_TIMEDWAIT_MILLISECONDS 10
    ErrorCode errorCode;
    int milliseconds = TEST_SEMAPHORE_LOCK_TIMEDWAIT_MILLISECONDS;
    errorCode = SemaphoreLockTimedwait(&semaphore, true, milliseconds);
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    SemaphoreUnlock(&semaphore);
    bool tryLockResult = false;
    tryLockResult = SemaphoreTryLock(&semaphore);
    ASSERT_TRUE(tryLockResult);
    SemaphoreReset(&semaphore);
    errorCode = SemaphoreLockTimedwait(&semaphore, true, milliseconds);
    EXPECT_EQ(errorCode, ERROR_UTILS_PORT_ETIMEDOUT);
    tryLockResult = SemaphoreTryLock(&semaphore);
    ASSERT_FALSE(tryLockResult);
    SemaphoreDestroy(&semaphore);
}

/**
 * @tc.name:  SemaphoreFunction002_Level0
 * @tc.desc: Test the semaphore attribute callback.
 * @tc.type: FUNC
 */
TEST_F(SemaphoreTest, SemaphoreFunction002_Level0)
{
    SemaphoreAttribute semaphoreAttribute;
    SemaphoreAttributeInit(&semaphoreAttribute);
    SemaphoreAttributeSetInterrupts(
            &semaphoreAttribute, EnableInterrupts, ProcessInterrupts, (void *)(&g_semaphoreInterruptsTest));
    int semaphoreKey = TEST_SEMAPHORE_KEY_VALUE;
    Semaphore semaphore;
    SemaphoreCreate(semaphoreKey, &semaphoreAttribute, &semaphore);
    SemaphoreLock(&semaphore, true);
    ASSERT_TRUE(g_semaphoreInterruptsTest.interruptOKFirstCallback);
    ASSERT_FALSE(g_semaphoreInterruptsTest.interruptOKSecondCallback);
    ASSERT_TRUE(g_semaphoreInterruptsTest.interruptCallback);
    SemaphoreUnlock(&semaphore);
    SemaphoreDestroy(&semaphore);
}

/**
 * @tc.name:  SemaphoreFunction003_Level0
 * @tc.desc: Test the semaphore concurrent lock.
 * @tc.type: FUNC
 */
TEST_F(SemaphoreTest, SemaphoreFunction003_Level0)
{
    int semaphoreKey = TEST_SEMAPHORE_KEY_VALUE;
    g_semaphoreConcurrentFlag = false;
    SemaphoreCreate(semaphoreKey, NULL, &g_semaphoreConcurrentLock);
    SemaphoreLock(&g_semaphoreConcurrentLock, true);
#define INITIAL_INTEGER_VALUE 1
    ErrorCode errorCode;
    Tid tid;
    int initValue = INITIAL_INTEGER_VALUE;
    errorCode = ThreadCreate(&tid, SemaphoreConcurrentLockThreadFunc, &initValue);
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    for (int i = 0; i < SEMAPHORE_LOCK_TEST_MAX_WAIT_TIME_MS; ++i) {
        Usleep(1000);
        ASSERT_FALSE(g_semaphoreConcurrentFlag);
    }
    SemaphoreUnlock(&g_semaphoreConcurrentLock);
    int *resultValue;
    errorCode = ThreadJoin(tid, (void **)&resultValue);
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    EXPECT_EQ(*resultValue, INITIAL_INTEGER_VALUE);
    ASSERT_TRUE(g_semaphoreConcurrentFlag);
    SemaphoreDestroy(&g_semaphoreConcurrentLock);
}

/**
 * @tc.name:  SemaphoreSetFunction001_Level0
 * @tc.desc: Test the semaphore set create,get and destroy.
 * @tc.type: FUNC
 */
TEST_F(SemaphoreTest, SemaphoreSetFunction001_Level0)
{
    SemaphoreSet *semaphoreSet = NULL;
    int semaphoreKey = TEST_SEMAPHORE_KEY_VALUE;
    SemaphoreAttribute semaphoreAttribute;
    SemaphoreAttributeInit(&semaphoreAttribute);
    SemaphoreAttributeSetInterrupts(
            &semaphoreAttribute, EnableInterrupts, ProcessInterrupts, (void *)(&g_semaphoreInterruptsTest));
    semaphoreSet = SemaphoreSetCreate(semaphoreKey, TEST_MAX_SEMAPHORE, &semaphoreAttribute);
    EXPECT_NE(semaphoreSet, nullptr);
    uint semaphoreCount;
    semaphoreCount = GetSemaphoreCountFromSet(semaphoreSet);
    ASSERT_EQ(semaphoreCount, TEST_MAX_SEMAPHORE);
    Semaphore *semaphore;
    semaphore = GetSemaphoreFromSet(semaphoreSet, TEST_MAX_SEMAPHORE);
    EXPECT_EQ(semaphore, nullptr);
    uint i;
    bool tryLockResult = false;
    for (i = 0; i < TEST_MAX_SEMAPHORE; i++) {
        SemaphoreInterruptsTestReset();
        semaphore = GetSemaphoreFromSet(semaphoreSet, i);
        SemaphoreLock(semaphore, true);
        ASSERT_TRUE(g_semaphoreInterruptsTest.interruptOKFirstCallback);
        ASSERT_FALSE(g_semaphoreInterruptsTest.interruptOKSecondCallback);
        ASSERT_TRUE(g_semaphoreInterruptsTest.interruptCallback);
        tryLockResult = true;
        tryLockResult = SemaphoreTryLock(semaphore);
        ASSERT_FALSE(tryLockResult);
        SemaphoreUnlock(semaphore);
    }
    SemaphoreSetDestroy(semaphoreSet);
}
