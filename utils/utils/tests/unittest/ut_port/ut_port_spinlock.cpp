
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
 * ut_port_spinlock.cpp
 * unit tests of spin lock
 *
 * ---------------------------------------------------------------------------------
 */

#include <pthread.h>
#include <gtest/gtest.h>
#include <mockcpp/mockcpp.hpp>
#include "port/platform_port.h"

/** ************************************************************************************************************* **/
#define MAX_COUNT 100000
#define COUNT_THREAD 2
#define SPIN_LOCK_TEST_MAX_WAIT_TIME_MS 10000  /* 10 seconds */

int g_number = 0;
SpinLock countSpin;
SpinLock lockSpin;

struct SpinLockGuard {
    bool isLocked = false;
    bool canUnlock = false;
};

static bool TestWaitBool(const bool *waitArgs, bool target)
{
    if (*waitArgs == target) {
        return true;
    }
    for (int i = 0; i < SPIN_LOCK_TEST_MAX_WAIT_TIME_MS; ++i) {
        Usleep(1000);
        if (*waitArgs == target) {
            return true;
        }
    }
    return false;
}

void *Counter(void *args)
{
    bool needSpin = (bool) (*((bool *) args));
    if (needSpin) {
        for (int i = 1; i <= MAX_COUNT / COUNT_THREAD; i++) {
            SpinLockAcquire(&countSpin);
            g_number++;
            SpinLockRelease(&countSpin);
        }
    } else {
        for (int i = 1; i <= MAX_COUNT / COUNT_THREAD; i++) {
            g_number++;
        }
    }
    return args;
}

/**
 * Using {COUNT_THREAD} threads to counting {MAX_COUNT}
 *
 * @param withSpin is the condition if with or not spin lock
 */
void SpinLockThreadCount(bool withSpin){
    g_number = 0;
    pthread_t th[COUNT_THREAD];
    for (auto &thread : th) {
        pthread_create(&thread, nullptr, Counter, &withSpin);
    }
    for (auto &thread : th) {
        pthread_join(thread, nullptr);
    }
    if (withSpin) {
        ASSERT_EQ(g_number, MAX_COUNT);
    } else {
        ASSERT_GT(g_number, 0);
        ASSERT_LE(g_number, MAX_COUNT);
    }
}

void *LockToDoSomething(void *args)
{
    auto *lockGuard = (SpinLockGuard *)args;
    SpinLockAcquire(&lockSpin);
    lockGuard->isLocked = true;
    EXPECT_TRUE(TestWaitBool(&(lockGuard->canUnlock), true));
    SpinLockRelease(&lockSpin);
    return args;
}

/** ************************************************************************************************************* **/

class SpinLockTest : public testing::Test {
public:
    void SetUp() override {
    };

    void TearDown() override {
        GlobalMockObject::verify();
    };
};

TEST_F(SpinLockTest, SpinInitTest)
{
    SpinLock params;
    SpinLockInit(&params);
    ASSERT_TRUE(SpinLockTryAcquire(&params));
    SpinLockRelease(&params);
    SpinLockDestroy(&params);
}

TEST_F(SpinLockTest, SpinLockTest)
{
    SpinLock params;
    SpinLockInit(&params);
    SpinLockAcquire(&params);
    ASSERT_FALSE(SpinLockTryAcquire(&params));
    SpinLockRelease(&params);
    SpinLockDestroy(&params);
}

TEST_F(SpinLockTest, SpinTryLockTest)
{
    SpinLock params;
    SpinLockInit(&params);
    ASSERT_TRUE(SpinLockTryAcquire(&params));
    ASSERT_FALSE(SpinLockTryAcquire(&params));
    SpinLockRelease(&params);
    ASSERT_TRUE(SpinLockTryAcquire(&params));
    SpinLockRelease(&params);
    SpinLockDestroy(&params);
}

TEST_F(SpinLockTest, SpinUnlockTest)
{
    SpinLock params;
    SpinLockInit(&params);
    SpinLockAcquire(&params);
    ASSERT_FALSE(SpinLockTryAcquire(&params));
    SpinLockRelease(&params);
    ASSERT_TRUE(SpinLockTryAcquire(&params));
    SpinLockRelease(&params);
    SpinLockDestroy(&params);
}

/**
 * Counting with or without lock
 */
TEST_F(SpinLockTest, SpinLockThreadCountTest)
{
    SpinLockInit(&countSpin);
    SpinLockThreadCount(false);
    SpinLockThreadCount(true);
    SpinLockDestroy(&countSpin);
}

/**
 * Use the thread to acquire the lock for a period of time. Observe the release of the lock.
 */
TEST_F(SpinLockTest, SpinLockThreadLockTest)
{
    SpinLockInit(&lockSpin);
    SpinLockAcquire(&lockSpin);
    SpinLockGuard lockGuard;
    pthread_t t;
    pthread_create(&t, nullptr, LockToDoSomething, &lockGuard);
    SpinLockRelease(&lockSpin);
    ASSERT_TRUE(TestWaitBool(&(lockGuard.isLocked), true));
    ASSERT_FALSE(SpinLockTryAcquire(&lockSpin));
    lockGuard.canUnlock = true;
    pthread_join(t, nullptr);
    ASSERT_TRUE(SpinLockTryAcquire(&lockSpin));
    SpinLockRelease(&lockSpin);
    SpinLockDestroy(&lockSpin);
}
