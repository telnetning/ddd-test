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
#include <iostream>
#include <fstream>
#include <unistd.h>
#include <sys/syscall.h>
#include "lock/lwlock.h"
#include "lock/lwlock_internal.h"

using namespace std;

#if defined __linux__ || defined linux
pid_t GetTid(void)
{
    return syscall(__NR_gettid);
}
#endif

static void ReadModifyWrite(volatile uint64_t *ptr, time_t delayMs)
{
    auto temp = *ptr;
    usleep(delayMs * 10);
    temp += 1;
    usleep(delayMs * 10);
    *ptr = temp;
}

static void LwLockLog(SYMBOL_UNUSED LWLock *lock, SYMBOL_UNUSED int errCode,
                          SYMBOL_UNUSED const ErrInfo *info)
{
}

class LwlockTest : public testing::Test {
public:
    static void SetUpTestSuite() { }
    static void TearDownTestSuite() { }
    void SetUp() override { }
    void TearDown() override { }
};
/**
 * @tc.name: LwlockBaseTest
 * @tc.desc: Base function of lock/unlock test
 * @tc.type: Functional use case
 */
TEST_F(LwlockTest, LwlockBaseTest)
{
    LWLock lock1;
    LWLock lock2;
    LWLockCtlParam para = {LwLockLog};
    LWLockInitialize(&lock1);
    LWLockInitializeWithParam(&lock2, &para);

#define THREAD_EXCLU_TEST 5
#define THREAD_MIX_NUMS 6
#define THREAD_NUMS (THREAD_MIX_NUMS + THREAD_EXCLU_TEST)
#define COUNT_TIMES 500
    vector<thread> threads;
    LWlockContext cxt[THREAD_NUMS];
    atomic_uint mark(0);
    volatile uint64_t count = 0;
    volatile uint64_t count2 = 0;
    using Fn = function<void()>;
    Fn Lock1 = [&lock1](){
        LWLockAcquire(&lock1, LW_EXCLUSIVE);
        ASSERT_EQ(lock1.owner.id, GetTid());
    };
    Fn Unlock1 = [&lock1](){ LWLockRelease(&lock1); };
    Fn Lock2Share = [&lock2](){ LWLockAcquire(&lock2, LW_SHARED); };
    Fn Lock2 = [&lock2](){ LWLockAcquire(&lock2, LW_EXCLUSIVE); };
    Fn Unlock2 = [&lock2](){ LWLockRelease(&lock2); };
    auto threadMain = [&mark](int tno, LWlockContext *cxt, volatile uint64_t *data, Fn start, Fn end) {
        ASSERT_EQ(LwLockInitThreadCore(cxt), 0);
        mark.fetch_add(1);
        while ((mark.load() % THREAD_NUMS) != 0);
        for (int i = 0; i < COUNT_TIMES; i++) {
            start();
            ReadModifyWrite(data, 1);
            end();
            usleep(10);
        }
        ASSERT_EQ(LWLockDeInitThreadCore(), 0);
    };

    for (int i = 0; i < THREAD_EXCLU_TEST; i++) {
        threads.push_back(thread(threadMain, i, &cxt[i], &count, Lock1, Unlock1));
    }
    for (int i = THREAD_EXCLU_TEST; i < THREAD_NUMS; i++) {
        /* Mix share and exclusive mode lock, run normally is OK */
        if (i  % 2) {
            threads.push_back(thread(threadMain, i, &cxt[i], &count2, Lock2, Unlock2));
        } else {
            threads.push_back(thread(threadMain, i, &cxt[i], &count2, Lock2Share, Unlock2));
        }
    }
    for (auto &thread : threads){
        thread.join();
    }
    threads.clear();
    ASSERT_EQ(count, THREAD_EXCLU_TEST * COUNT_TIMES);
}

/**
 * @tc.name: LwlockConditionalAcquireTest
 * @tc.desc: Base function of LWLockConditionalAcquireDebug test
 * @tc.type: Functional use case
 */
TEST_F(LwlockTest, LwlockConditionalAcquireTest)
{
    LWLock lock;
    LWLockInitialize(&lock);

#define THREAD_NUMS 2
    LWlockContext cxt[THREAD_NUMS];
    ASSERT_EQ(LwLockInitThreadCore(&cxt[0]), 0);
    ASSERT_TRUE(LWLockConditionalAcquireDebug(&lock, LW_EXCLUSIVE, __FILE__, __LINE__, __func__));

    thread t1([&lock](LWlockContext *cxt) {
        ASSERT_EQ(LwLockInitThreadCore(cxt), 0);
        ASSERT_FALSE(LWLockConditionalAcquireDebug(&lock, LW_EXCLUSIVE, __FILE__, __LINE__, __func__));
        ASSERT_EQ(LWLockDeInitThreadCore(), 0);
    }, &cxt[1]);
    t1.join();

    LWLockReleaseAll();
    ASSERT_TRUE(LWLockConditionalAcquireDebug(&lock, LW_SHARED, __FILE__, __LINE__, __func__));
    thread t2([&lock](LWlockContext *cxt) {
        ASSERT_EQ(LwLockInitThreadCore(cxt), 0);
        ASSERT_TRUE(LWLockConditionalAcquireDebug(&lock, LW_SHARED, __FILE__, __LINE__, __func__));
        ASSERT_EQ(LWLockDeInitThreadCore(), 0);
    }, &cxt[1]);
    ASSERT_FALSE(LWLockConditionalAcquireDebug(&lock, LW_EXCLUSIVE, __FILE__, __LINE__, __func__));
    // no thread sleep for wait the lock, so we can not release
    t2.join();
}

/**
 * @tc.name: LWLockAcquireOrWaitTest
 * @tc.desc: Base function of LWLockAcquireOrWait test
 * @tc.type: Functional use case
 */
TEST_F(LwlockTest, LWLockAcquireOrWaitTest)
{
    LWLock lock1, lock2;
    LWLockInitialize(&lock1);
    LWLockInitialize(&lock2);

#define THREAD_NUMS 2
#define COUNT_NUMS 1000
    LWlockContext cxt[THREAD_NUMS];
    ASSERT_EQ(LwLockInitThreadCore(&cxt[0]), 0);
    ASSERT_TRUE(LWLockAcquireOrWait(&lock1, LW_EXCLUSIVE));

    thread t1([&lock1, &lock2](LWlockContext *cxt) {
        ASSERT_EQ(LwLockInitThreadCore(cxt), 0);
        /* Get lock2 */
        LWLockAcquire(&lock2, LW_EXCLUSIVE);
        /* Wait lock1 release */
        ASSERT_FALSE(LWLockAcquireOrWait(&lock1, LW_EXCLUSIVE));
        /* Release lock3 */
        LWLockRelease(&lock2);
        ASSERT_EQ(LWLockDeInitThreadCore(), 0);
    }, &cxt[1]);
    /* Test lock3 had been locked */
    while (LWLockConditionalAcquireDebug(&lock2, LW_SHARED, __FILE__, __LINE__, __func__)) {
        LWLockRelease(&lock2);
        usleep(10);
    }
    /* Test again */
    while (LWLockConditionalAcquireDebug(&lock2, LW_SHARED, __FILE__, __LINE__, __func__)) {
        LWLockRelease(&lock2);
        usleep(10);
    }
    LWLockRelease(&lock1);
        /* Get lock2 */
    LWLockAcquire(&lock2, LW_EXCLUSIVE);
    /* Get lock1, because t1 not lock it */
    ASSERT_TRUE(LWLockAcquireOrWait(&lock1, LW_EXCLUSIVE));
    LWLockReleaseAll();
    t1.join();
}

/**
 * @tc.name: LWLockGetInfoTest
 * @tc.desc: Base function of get lwlock info test, Involved 
 *           LWLockAtomicReadState\LWLockHeldByMe\LWLockHeldByMeInMode\LWLockReset\
 *           LWLockIsHeldsOverflow\GetHeldLWlocks\GetHeldLWlocksNum\GetHeldLWLockMode
 * @tc.type: Functional use case
 */
TEST_F(LwlockTest, LWLockGetInfoTest)
{
#define EXCEED_LOCKS 100
    LWlockContext *cxt = new(nothrow) LWlockContext;
    ASSERT_NE(cxt, nullptr);
    auto size = sizeof(cxt->heldLWlocks) / sizeof(cxt->heldLWlocks[0]) + EXCEED_LOCKS;
    vector<LWLock> locks(size);

    ASSERT_EQ(LwLockInitThreadCore(cxt), 0);
    ASSERT_EQ(GetHeldLWlocks(), &(cxt->heldLWlocks));
    for (auto &lock : locks) {
        LWLockInitialize(&lock);
        LWLockReset(&lock);
        ASSERT_EQ(LWLockAtomicReadState(&lock), LW_FLAG_RELEASE_OK);
        ASSERT_TRUE(LWLockConditionalAcquireDebug(&lock, LW_SHARED, __FILE__, __LINE__, __func__));
    }
    ASSERT_EQ(GetHeldLWlocksNum(), size);
    ASSERT_EQ(GetHeldLWlocksNum(), size);
    ASSERT_EQ(GetRecordHeldLWlocksNum(), size - EXCEED_LOCKS);
    for (auto i = locks.size() - 1; i > 0; --i) {
        if (LWLockIsHeldsOverflow()) {
            LWLockRelease(&locks[i]);
            continue;
        }
        ASSERT_TRUE(LWLockHeldByMe(&locks[i]));
        ASSERT_EQ(GetHeldLWLockMode(&locks[i]), LW_SHARED);
        ASSERT_FALSE(LWLockHeldByMeInMode(&locks[i], LW_EXCLUSIVE));
    }

    LWLockReleaseAll();
    ASSERT_EQ(LWLockDeInitThreadCore(), 0);
    delete cxt;
}
