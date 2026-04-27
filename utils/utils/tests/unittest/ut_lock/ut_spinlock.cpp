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
 * Description: unit test for spinlock
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
#include "lock/spinlock.h"

using namespace std;

static uint64_t GetNsTime(void)
{
    struct timespec ts {};
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (ts.tv_sec * 1000 * 1000 * 1000) + (ts.tv_nsec);
};

class SpinlockTest : public testing::Test {
public:
    static void SetUpTestSuite() { }
    static void TearDownTestSuite() { }
    void SetUp() override { }
    void TearDown() override { }
};
/**
 * @tc.name: SpinTryLockAndIsLocked
 * @tc.desc: Function test of SpinTryLock and SpinLockIsLocked
 * @tc.type: Functional use case
 */
TEST_F(SpinlockTest, SpinTryLockAndIsLocked)
{
    SpinLock lock;
    SpinLockInit(&lock);
    ASSERT_FALSE(SpinLockIsLocked(&lock));
    SpinLockAcquire(&lock);
    ASSERT_TRUE(SpinLockIsLocked(&lock));
    ASSERT_EQ(SpinLockTryAcquire(&lock), -1);
    SpinLockRelease(&lock);
    ASSERT_FALSE(SpinLockIsLocked(&lock));
    ASSERT_EQ(SpinLockTryAcquire(&lock), 0);
    ASSERT_TRUE(SpinLockIsLocked(&lock));
    SpinLockRelease(&lock);
    ASSERT_FALSE(SpinLockIsLocked(&lock));
    SpinLockDestroy(&lock);
}
/**
 * @tc.name: MultiThreadCriticalProtect
 * @tc.desc: Test pthread_spinlock_t protect critical code for multi-thread execute
 * @tc.type: Functional use case
 */
TEST_F(SpinlockTest, MultiThreadCriticalProtect)
{
    SpinLock lock;
    pthread_spinlock_t portSpin;
    vector<thread> threads;
    volatile uint64_t danger = 0;
    volatile uint64_t control = 0;
    volatile uint64_t count = 0;
    atomic_uint mark(0);

    SpinLockInit(&lock);
    pthread_spin_init(&portSpin, PTHREAD_PROCESS_PRIVATE);

#define THREAD_NUMS 10
#define COUNT_TIMES 5000
    using Fn = function<void()>;
    Fn actionNull = [](){};
    Fn actionPortLock = [&portSpin](){ pthread_spin_lock(&portSpin); };
    Fn actionPortUnlock = [&portSpin](){ pthread_spin_unlock(&portSpin); };
    Fn actionpthread_spinlock_t = [&lock](){ SpinLockAcquire(&lock); };
    Fn actionSpinUnlock = [&lock](){ SpinLockRelease(&lock); };
    auto threadMain = [&mark](int tno, volatile uint64_t *data, Fn start, Fn end) {
        mark.fetch_add(1);
        while ((mark.load() % THREAD_NUMS) != 0);
        auto time = GetNsTime();
        auto temp = 0;
        for (int i = 0; i < COUNT_TIMES; i++) {
            start();
            temp = *data;
            temp++;
            *data = temp;
            end();
        }
        time = GetNsTime() - time;
        string log = string("thread") + to_string(tno) + " end at " + to_string(time) +
                         " with data = " + to_string(temp) + "\n";
        write(fileno(stdout), log.c_str(), log.size());
    };

    for (int i = 0; i < THREAD_NUMS; i++) {
        threads.push_back(thread(threadMain, i, &danger, actionNull, actionNull));
    }
    for (auto& thread : threads){
        thread.join();
    }
    threads.clear();
    ASSERT_LT(danger, THREAD_NUMS * COUNT_TIMES);

    for (int i = 0; i < THREAD_NUMS; i++) {
        threads.push_back(thread(threadMain, i, &control, actionPortLock, actionPortUnlock));
    }
    for(auto& thread : threads){
        thread.join();
    }
    threads.clear();
    ASSERT_EQ(control, THREAD_NUMS * COUNT_TIMES);

    for (int i = 0; i < THREAD_NUMS; i++) {
        threads.push_back(thread(threadMain, i, &count, actionpthread_spinlock_t, actionSpinUnlock));
    }
    for(auto& thread : threads){
        thread.join();
    }
    threads.clear();
    ASSERT_EQ(count, THREAD_NUMS * COUNT_TIMES);

    pthread_spin_destroy(&portSpin);
    SpinLockDestroy(&lock);
}
/**
 * @tc.name: UniprocessorScene
 * @tc.desc: Test pthread_spinlock_t on uniprocessor scene
 * @tc.type: Functional use case
 */
TEST_F(SpinlockTest, UniprocessorScene)
{
    SpinLock lock;
    pthread_spinlock_t portSpin;
    vector<thread> threads;
    atomic_uint mark(0);
    atomic_ulong portLatency(0);
    atomic_ulong testLatency(0);

    SpinLockInit(&lock);
    pthread_spin_init(&portSpin, PTHREAD_PROCESS_PRIVATE);
#define THREAD_NUMS 4
#define COUNT_TIMES 3
    using Fn = function<void()>;
    Fn actionPortLock = [&portSpin](){ pthread_spin_lock(&portSpin); };
    Fn actionPortUnlock = [&portSpin](){ pthread_spin_unlock(&portSpin); };
    Fn actionpthread_spinlock_t = [&lock](){ SpinLockAcquire(&lock); };
    Fn actionSpinUnlock = [&lock](){ SpinLockRelease(&lock); };

    auto threadMain = [&mark](int tno, atomic_ulong *latency, Fn start, Fn end) {
        cpu_set_t set;
        CPU_ZERO(&set);
        CPU_SET(0, &set);
        sched_setaffinity(0, sizeof(set), &set); /* calling thread*/
        ifstream istrm(string("/proc/") + to_string(getpid()) + "/task/" + to_string(syscall(SYS_gettid)) + "/schedstat");
        mark.fetch_add(1);
        while ((mark.load() % THREAD_NUMS) != 0);
        auto time = GetNsTime();
        time_t curlat;
        istrm >> curlat;
        for (int i = 0; i < COUNT_TIMES; i++) {
            start();
            auto lat = curlat;
#define NS2MS 1000 * 1000
        /* spinlock protect critical code run 70ms, will be schedule multi-times, other threads spin useless, when the spinlock is from port(posix) */
            while((curlat - lat) < 70 * NS2MS) {
                istrm.seekg(0, ios_base::beg);
                istrm >> curlat;
            }
            end();
        }
        time = GetNsTime() - time;
        latency->fetch_add(time);
    };

    for (int i = 0; i < THREAD_NUMS; i++) {
        threads.push_back(thread(threadMain, i, &portLatency, actionPortLock, actionPortUnlock));
    }
    for(auto& thread : threads){
        thread.join();
    }
    threads.clear();
    for (int i = 0; i < THREAD_NUMS; i++) {
        threads.push_back(thread(threadMain, i, &testLatency, actionpthread_spinlock_t, actionSpinUnlock));
    }
    for(auto& thread : threads){
        thread.join();
    }
    ASSERT_LT(testLatency, portLatency) << setprecision (6) << fixed << (double)testLatency / (1000 * NS2MS)<< "s VS " <<
        (double)portLatency / (1000 * NS2MS) << "s";
    pthread_spin_destroy(&portSpin);
    SpinLockDestroy(&lock);
}
