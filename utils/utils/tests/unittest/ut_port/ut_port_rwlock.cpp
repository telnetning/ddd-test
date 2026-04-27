
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
 * ut_port_rwlock.cpp
 * unit tests of spin lock
 *
 * ---------------------------------------------------------------------------------
 */
#include <pthread.h>
#include <gtest/gtest.h>
#include "port/platform_port.h"

/** ************************************************************************************************************* **/
#define COUNT_THREAD 2
#define RW_LOCK_TEST_MAX_WAIT_TIME_MS 10000  /* 10 seconds */
RWLock rwlock;

bool TestWaitBool(const bool *waitArgs, bool target)
{
    if (*waitArgs == target) {
        return true;
    }
    for (int i = 0; i < RW_LOCK_TEST_MAX_WAIT_TIME_MS; ++i) {
        Usleep(1000);
        if (*waitArgs == target) {
            return true;
        }
    }
    return false;
}

struct TestLockGuard {
    bool isLocked = false;
    bool canUnlock = false;
};

void *LockToRead(void *args)
{
    RWLockRdLock(&rwlock);
    auto *lockGuard = (TestLockGuard *)args;
    lockGuard->isLocked = true;
    EXPECT_TRUE(TestWaitBool(&(lockGuard->canUnlock), true));
    RWLockRdUnlock(&rwlock);
    return args;
}

void *LockToWrite(void *args)
{
    RWLockWrLock(&rwlock);
    auto *lockGuard = (TestLockGuard *)args;
    lockGuard->isLocked = true;
    EXPECT_TRUE(TestWaitBool(&(lockGuard->canUnlock), true));
    RWLockWrUnlock(&rwlock);
    return args;
}

void *LockToReadWithTimeout(void *args)
{
    uint32_t lockTimeout = *(uint32_t *)args;
    bool res = RWLockRdLockTimeout(&rwlock, lockTimeout);
    EXPECT_TRUE(res);
    if (res) {
        RWLockRdUnlock(&rwlock);
    }
    return args;
}

class RWLockTest : public testing::Test {
public:
    void SetUp() override {
    };

    void TearDown() override {
    };
};

TEST_F(RWLockTest, RWLockReadWriteSingleThreadTest)
{
    RWLock params;
    RWLockInit(&params, RWLOCK_PREFER_READER_NP);
    RWLockRdLock(&params);
    ASSERT_TRUE(RWLockTryRdLock(&params));
    ASSERT_FALSE(RWLockTryWrLock(&params));
    RWLockRdUnlock(&params);
    ASSERT_FALSE(RWLockTryWrLock(&params));
    RWLockRdUnlock(&params);
    ASSERT_TRUE(RWLockTryWrLock(&params));
    RWLockWrUnlock(&params);
    RWLockWrLock(&params);
    ASSERT_FALSE(RWLockTryRdLock(&params));
    ASSERT_FALSE(RWLockTryWrLock(&params));
    RWLockWrUnlock(&params);
    RWLockDestroy(&params);
}

TEST_F(RWLockTest, RWLockReadWriteMultiThreadTest)
{
    RWLockInit(&rwlock, RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
    pthread_t th[COUNT_THREAD + 1];
    TestLockGuard lockGuard[COUNT_THREAD + 1];
    int ret;
    for (int i = 0; i < COUNT_THREAD; i++) {
        th[i] = i;
        ret = pthread_create(&th[i], nullptr, LockToRead, &(lockGuard[i]));
        ASSERT_EQ(ret, 0);
        EXPECT_TRUE(TestWaitBool(&(lockGuard[i].isLocked), true));
    }
    ASSERT_TRUE(RWLockTryRdLock(&rwlock));
    ASSERT_FALSE(RWLockTryWrLock(&rwlock));
    for (int i = 0; i < COUNT_THREAD; ++i) {
        lockGuard[i].canUnlock = true;
        ret = pthread_join(th[i], nullptr);
        ASSERT_EQ(ret, 0);
    }
    RWLockRdUnlock(&rwlock);
    th[COUNT_THREAD] = COUNT_THREAD;
    ret = pthread_create(&th[COUNT_THREAD], nullptr, LockToWrite, &(lockGuard[COUNT_THREAD]));
    ASSERT_EQ(ret, 0);
    EXPECT_TRUE(TestWaitBool(&(lockGuard[COUNT_THREAD].isLocked), true));
    ASSERT_FALSE(RWLockTryRdLock(&rwlock));
    ASSERT_FALSE(RWLockTryWrLock(&rwlock));
    lockGuard[COUNT_THREAD].canUnlock = true;
    ret = pthread_join(th[COUNT_THREAD], nullptr);
    ASSERT_EQ(ret, 0);
    ASSERT_TRUE(RWLockTryRdLock(&rwlock));
    RWLockRdUnlock(&rwlock);
    ASSERT_TRUE(RWLockTryWrLock(&rwlock));
    RWLockWrUnlock(&rwlock);
    RWLockDestroy(&rwlock);
}

TEST_F(RWLockTest, RWLockReadWriteWithTimeoutMultiThreadTest)
{
    RWLockInit(&rwlock, RWLOCK_PREFER_WRITER_NP);
    pthread_t th[COUNT_THREAD + 1];
    TestLockGuard lockGuard[COUNT_THREAD + 1];
    int ret;
    for (int i = 0; i < COUNT_THREAD; i++) {
        th[i] = i;
        ret = pthread_create(&th[i], nullptr, LockToRead, &(lockGuard[i]));
        ASSERT_EQ(ret, 0);
        EXPECT_TRUE(TestWaitBool(&(lockGuard[i].isLocked), true));
    }
    ASSERT_FALSE(RWLockWrLockTimeout(&rwlock, 200));
    for (int i = 0; i < COUNT_THREAD; ++i) {
        lockGuard[i].canUnlock = true;
        ret = pthread_join(th[i], nullptr);
        ASSERT_EQ(ret, 0);
    }
    th[COUNT_THREAD] = COUNT_THREAD;
    ret = pthread_create(&th[COUNT_THREAD], nullptr, LockToWrite, &(lockGuard[COUNT_THREAD]));
    ASSERT_EQ(ret, 0);
    EXPECT_TRUE(TestWaitBool(&(lockGuard[COUNT_THREAD].isLocked), true));
    pthread_t waitThread;
    uint32_t waitTime = 10 * 1000 * 1000; /* Max wait 10 seconds */
    ret = pthread_create(&waitThread, nullptr, LockToReadWithTimeout, &waitTime);
    ASSERT_EQ(ret, 0);
    ASSERT_FALSE(RWLockWrLockTimeout(&rwlock, 200));
    lockGuard[COUNT_THREAD].canUnlock = true;
    ret = pthread_join(th[COUNT_THREAD], nullptr);
    ASSERT_EQ(ret, 0);
    ret = pthread_join(waitThread, nullptr);
    ASSERT_EQ(ret, 0);

    RWLockDestroy(&rwlock);
}
