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
 */
#include <gtest/gtest.h>
#include "lock/dstore_lwlock.h"
#include "ut_utilities/ut_dstore_framework.h"
#include "framework/dstore_instance.h"
#include "fault_injection/fault_injection.h"
#include "securec.h"
#include <thread>
#include <mutex>
#include <atomic>
#include <vector>
#include <string>

using namespace DSTORE;

const static int UT_LWLOCK_THREAD_COUNT = 10;
const static int UT_LWLOCK_LOOP_COUNT = 10000;
static LWLock g_lwlock;
static int g_count = 0;
static pthread_barrier_t g_barrier;
static std::mutex g_mutex;


static void memFree(void *addr)
{
    if (addr != nullptr) {
        free(addr);
    }
}

class LWLockTest : public DSTORETEST {
protected:

    void SetUp() override
    {
        DSTORETEST::SetUp();
        LWLockInitialize(&g_lwlock, 0);
        pthread_barrier_init(&g_barrier, nullptr, UT_LWLOCK_THREAD_COUNT);
        UtInstance *instance = DstoreNew(m_ut_memory_context)UtInstance();
        instance->Install(&DSTORETEST::m_guc, m_ut_memory_context);
        instance->Startup(&DSTORETEST::m_guc);
        g_count = 0;
    }
    void TearDown() override
    {
        pthread_barrier_destroy(&g_barrier);
        UtInstance *instance = (UtInstance *)g_storageInstance;
        instance->Shutdown();
        delete instance;
        DSTORETEST::TearDown();
    }
};

static void UtLWLockThreadFunInit()
{
    ((UtInstance *)g_storageInstance)->ThreadSetupAndRegister();
}

static void UtLWLockThreadFunUinit()
{
    ((UtInstance *)g_storageInstance)->ThreadUnregisterAndExit();
}

void UtLWLockAcquireThreadFun()
{
    UtLWLockThreadFunInit();

    pthread_barrier_wait(&g_barrier);

    for (int i = 0; i < UT_LWLOCK_LOOP_COUNT; i++) {
        DstoreLWLockAcquire(&g_lwlock, LW_EXCLUSIVE);

        EXPECT_TRUE(LWLockHeldByMe(&g_lwlock));
        EXPECT_TRUE(LWLockHeldByMeInMode(&g_lwlock, LW_EXCLUSIVE));

        g_count++;

        LWLockRelease(&g_lwlock);
    }

    UtLWLockThreadFunUinit();
}

TEST_F(LWLockTest, PerfTest_level2)
{
    DSTORE::LWLock lock;
    uint32 *array = nullptr;
    LWLockInitialize(&lock);
    uint64_t count = 0;

    array = (uint32 *)DstorePalloc(sizeof(uint32) * 192);
    for (uint32 i = 0; i < 192; i++) {
        array[i] = i;
    }

    auto func = [](LWLock *lock, uint32 *array, uint64_t *count) {
        pthread_setname_np(pthread_self(), "PerfTest");
        create_thread_and_register();
        auto binarySearch = [](uint32 *arr, uint32 length, uint32 target) -> int {
            uint32 left = 0;
            uint32 right = length - 1;
            while (left <= right) {
                uint32 mid = (left + right) / 2;
                if (target > arr[mid]) {
                    left = mid + 1;
                } else {
                    right = mid - 1;
                }
            }
            return left;
        };

        while (__atomic_load_n(count, __ATOMIC_RELAXED) != 1);
        for (uint32 i = 0; i < 1000000; i++) {
            DstoreLWLockAcquire(lock, LW_SHARED);
            uint32 idx = binarySearch(array, 192, 96);
            LWLockRelease(lock);
        }
        unregister_thread();
    };

    std::thread thread_list[50];
    for (uint32 i = 0; i < 50; i++) {
        thread_list[i] = std::thread(func, &lock, array, &count);
    }

    std::chrono::time_point<std::chrono::high_resolution_clock> startTime = std::chrono::high_resolution_clock::now();
    __atomic_store_n(&count, 1, __ATOMIC_RELAXED);

    for (uint32 i = 0; i < 50; i++) {
        thread_list[i].join();
    }

    std::chrono::time_point<std::chrono::high_resolution_clock> endTime = std::chrono::high_resolution_clock::now();
    std::chrono::microseconds duration = std::chrono::duration_cast<std::chrono::microseconds>(endTime - startTime);
    printf("cost = %.3lfs\n", (double)duration.count() / 1000 / 1000);
}

TEST_F(LWLockTest, PerfTest2_level2)
{
    DSTORE::LWLock lock;
    LWLockInitialize(&lock);
    auto func = [](LWLock *lock, int cpu) {
        cpu_set_t cpuset;
        CPU_SET(cpu, &cpuset); // bind to cpu
        sched_setaffinity(0, sizeof(cpu_set_t), &cpuset);
        pthread_setname_np(pthread_self(), "PerfTest");
        create_thread_and_register();

        auto startTime = std::chrono::high_resolution_clock::now();
        for (uint32 i = 0; i < 50000; i++) {
            DstoreLWLockAcquire(lock, LW_EXCLUSIVE);
            for (std::atomic_int j(0); j < 1000; j++);
            LWLockRelease(lock);
        }
        auto endTime = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(endTime - startTime);
        printf("cost = %.3lfs\n", (double)duration.count() / 1000 / 1000);
        unregister_thread();
    };

    std::thread t[10];
    for (int i = 0; i < 10; i++)
        t[i] = std::thread(func, &lock, i);
    for (int i = 0; i < 10; i++)
        t[i].join();
}

TEST_F(LWLockTest, ConcurrencyTest_level1)
{
    DSTORE::LWLock lock;
    LWLockInitialize(&lock);
    volatile uint64_t testCount = 1;
    std::atomic_long lastRead(testCount);
    std::atomic_long printCnt(0);
    std::atomic_bool sync(true);
#define TEST_COUNT 100000
#define THREAD_COUNT 30

    auto func = [&lastRead, &printCnt, &sync](LWLock *lock, volatile uint64_t *testCount, bool read) {
        pthread_setname_np(pthread_self(), "ConcurrencyTest");
        create_thread_and_register();

        while (sync);  /* sync */
        for (uint32 i = 0; i < TEST_COUNT; i++) {
            if (read) {
                static std::string msg("[ConcurrencyTest]read thread, read value ");
                usleep(1); // reduce shared mode lock/unlock continuity
                DstoreLWLockAcquire(lock, LW_SHARED);
                /* testCount init to 1, and increasing monotonically */
                auto temp = *testCount;
                auto last = lastRead.load(std::memory_order_relaxed);
                lastRead.store(temp, std::memory_order_relaxed);
                EXPECT_GE(temp, last);
                if ((printCnt.load(std::memory_order_relaxed) <= 100) && (last != temp)) {
                    printCnt.fetch_add(1, std::memory_order_relaxed);
                    auto str = msg + std::to_string(*testCount) + "\n";
                    write(fileno(stdout), str.c_str(), str.length());
                }
                EXPECT_EQ(temp, *testCount); /* in shared locked, no exclusive write critical section write the data */
            } else {
                DstoreLWLockAcquire(lock, LW_EXCLUSIVE);
                auto temp = *testCount;
                temp++;
                *testCount = temp;
            }
            LWLockRelease(lock);
        }
        unregister_thread();
    };

    std::vector<std::thread> threadVec(THREAD_COUNT);
    for (uint32 i = 0; i < THREAD_COUNT; i++) {
        /* i=0,5,10,... is write thread, other is read */
        threadVec[i] = std::thread(func, &lock, &testCount, (i % 5) != 0);
    }

    std::chrono::time_point<std::chrono::high_resolution_clock> startTime = std::chrono::high_resolution_clock::now();
    sync = false; // start test
    for (auto &thread : threadVec) {
        thread.join();
    }

    std::chrono::time_point<std::chrono::high_resolution_clock> endTime = std::chrono::high_resolution_clock::now();
    std::chrono::microseconds duration = std::chrono::duration_cast<std::chrono::microseconds>(endTime - startTime);
    printf("cost = %.3lfs testCount %lu\n", (double)duration.count() / 1000 / 1000, testCount);
    ASSERT_EQ(testCount, (uint64_t)(THREAD_COUNT / 5) * TEST_COUNT + 1); // testCount init to 1
}

TEST_F(LWLockTest, DstoreLWLockConditionalAcquireAnyModeTest_level0)
{
    DSTORE::LWLock lock;
    LWLockInitialize(&lock);
    volatile uint64_t testCount = 1;
    std::atomic_long lastRead(testCount);
    std::atomic_long printCnt(0);
    std::atomic_long wirteCnt(1);
    std::atomic_long readCnt(0);
    std::atomic_bool sync(true);
#undef TEST_COUNT
#undef THREAD_COUNT
#define TEST_COUNT 50000
#define THREAD_COUNT 50

    auto func = [&lastRead, &printCnt, &sync, &wirteCnt, &readCnt](LWLock *lock, volatile uint64_t *testCount) {
        pthread_setname_np(pthread_self(), "AnyModeTest");
        create_thread_and_register();

        while (sync);  /* sync */
        for (uint32 i = 0; i < TEST_COUNT; i++) {
            LWLockMode mode = LW_WAIT_UNTIL_FREE;
            while (!DstoreLWLockConditionalAcquireAnyMode(lock, &mode));
            if (mode == LW_SHARED) {
                static std::string msg("[AnyModeTest]read thread, read value ");
                /* testCount init to 1, and increasing monotonically */
                auto temp = *testCount;
                readCnt.fetch_add(1, std::memory_order_relaxed);
                auto last = lastRead.load(std::memory_order_relaxed);
                lastRead.store(temp, std::memory_order_relaxed);
                EXPECT_GE(temp, last);
                if ((printCnt.load(std::memory_order_relaxed) <= 100) && (last != temp)) {
                    printCnt.fetch_add(1, std::memory_order_relaxed);
                    auto str = msg + std::to_string(*testCount) + "\n";
                    write(fileno(stdout), str.c_str(), str.length());
                }
                EXPECT_EQ(temp, *testCount); /* in shared locked, no exclusive write critical section write the data */
            } else if (mode == LW_EXCLUSIVE){
                auto temp = *testCount;
                temp++;
                /* using to check no two more exclusive write in critical at same time */
                wirteCnt.fetch_add(1, std::memory_order_relaxed);
                *testCount = temp;
            } else {
                ASSERT_TRUE(false);
            }
            LWLockRelease(lock);
        }
        unregister_thread();
    };

    std::vector<std::thread> threadVec(THREAD_COUNT);
    for (uint32 i = 0; i < THREAD_COUNT; i++) {
        threadVec[i] = std::thread(func, &lock, &testCount);
    }

    /* 10 threads to race lock, may have a chance to take shared lock? */
    for (int i = 0; i < 10; i++) {
        threadVec.push_back(std::thread([&sync](LWLock *lock){
            pthread_setname_np(pthread_self(), "SharedModeInst");
            create_thread_and_register();
            while (sync);  /* sync */
            for (int i = 0; i < 10; i++) {
                DstoreLWLockAcquire(lock, LW_SHARED);
                usleep(1000);
                LWLockRelease(lock);
                usleep(10000);
            }
            unregister_thread();
        }, &lock));
    }
    /* In actually, the 10 threads above, It does allow the "AnyModeTest" test threads to have the opportunity
     * to include some shared locking moments. */

    std::chrono::time_point<std::chrono::high_resolution_clock> startTime = std::chrono::high_resolution_clock::now();
    sync = false; // start test
    for (auto &thread : threadVec) {
        thread.join();
    }

    std::chrono::time_point<std::chrono::high_resolution_clock> endTime = std::chrono::high_resolution_clock::now();
    std::chrono::microseconds duration = std::chrono::duration_cast<std::chrono::microseconds>(endTime - startTime);
    printf("cost = %.3lfs testCount %lu\n", (double)duration.count() / 1000 / 1000, testCount);
    EXPECT_EQ(testCount, wirteCnt);
    EXPECT_EQ(readCnt, TEST_COUNT * THREAD_COUNT + 1 - wirteCnt);
}

TEST_F(LWLockTest, DISABLED_DstoreLWLockStuckScene)
{
    std::vector<std::thread> thVec;
    static std::atomic_uint testing;
    static std::atomic_bool runFault;
#define TESTING_BEGIN           0x0
#define EXCLUSIVEMODE_1_LOCKED  0x1
#define SHAREDMODE_2_LOCKED     0x2
#define TESTING_END             0x3
    testing = TESTING_BEGIN;
    runFault = false;

    DSTORE::LWLock lock;
    LWLockInitialize(&lock);

    void(*faultInjectFunc)(const FaultInjectionEntry *, void *) = [](const FaultInjectionEntry *, void *) -> void {
        FAULT_INJECTION_INACTIVE(LWLOCK_STUCK_SCENE_FAULT_INJECT, FI_GLOBAL);
        runFault = true;
        while (testing != TESTING_END);
    };
    FaultInjectionEntry entries[] = {
        FAULT_INJECTION_ENTRY(LWLOCK_STUCK_SCENE_FAULT_INJECT, false, faultInjectFunc),
    };
    ASSERT_EQ(RegisterFaultInjection(entries, sizeof(entries) / sizeof(entries[0]), FI_GLOBAL), ERROR_SYS_OK);
    FAULT_INJECTION_ACTIVE(LWLOCK_STUCK_SCENE_FAULT_INJECT, FI_GLOBAL);

    // first get exclusive lwlock
    thVec.push_back(std::thread([](DSTORE::LWLock *lock) {
        pthread_setname_np(pthread_self(), "ExclusiveMode1");
        create_thread_and_register();
        DstoreLWLockAcquire(lock, LW_EXCLUSIVE);
        testing = EXCLUSIVEMODE_1_LOCKED;
        while (!runFault);  /* wait fault inject callback ready */
        LWLockRelease(lock);
        unregister_thread();
    }, &lock));
    // second get shared lwlock
    thVec.push_back(std::thread([](DSTORE::LWLock *lock) {
        pthread_setname_np(pthread_self(), "SharedMode1");
        create_thread_and_register();
        while (testing != EXCLUSIVEMODE_1_LOCKED);
        if (DstoreLWLockConditionalAcquire(lock, LW_SHARED)) {
            LWLockRelease(lock);
        }
        unregister_thread();
    }, &lock));
    while (!runFault);  /* wait fault inject callback ready */
    // third get shared lwlock
    thVec.push_back(std::thread([](DSTORE::LWLock *lock) {
        pthread_setname_np(pthread_self(), "SharedMode2");
        create_thread_and_register();
        DstoreLWLockAcquire(lock, LW_SHARED);
        testing = SHAREDMODE_2_LOCKED;
        LWLockRelease(lock);
        unregister_thread();
    }, &lock));
    // fourth get exclusive lwlock
    thVec.push_back(std::thread([](DSTORE::LWLock *lock) {
        pthread_setname_np(pthread_self(), "ExclusiveMode2");
        create_thread_and_register();
        while (testing != SHAREDMODE_2_LOCKED);
        testing = TESTING_END;
        DstoreLWLockAcquire(lock, LW_EXCLUSIVE);
        LWLockRelease(lock);
        unregister_thread();
    }, &lock));

    for (auto &t : thVec) {
        t.join();
    }
}

TEST_F(LWLockTest, LWLockAcquireAndRelease_level0)
{
    std::thread t[UT_LWLOCK_THREAD_COUNT];

    for (int i = 0; i < UT_LWLOCK_THREAD_COUNT; i++) {
        t[i] = std::thread(UtLWLockAcquireThreadFun);
    }

    for(int i = 0; i < UT_LWLOCK_THREAD_COUNT; i++) {
        t[i].join();
    }

    ASSERT_EQ(g_count, UT_LWLOCK_THREAD_COUNT * UT_LWLOCK_LOOP_COUNT);
}

void UtLWLockConditionalAcquireThreadFun()
{
    UtLWLockThreadFunInit();

    pthread_barrier_wait(&g_barrier);

    for (int i = 0; i < UT_LWLOCK_LOOP_COUNT; i++) {
        while (!DstoreLWLockConditionalAcquire(&g_lwlock, LW_EXCLUSIVE))
        {
            EXPECT_FALSE(LWLockHeldByMe(&g_lwlock));
            EXPECT_FALSE(LWLockHeldByMeInMode(&g_lwlock, LW_EXCLUSIVE));
        }

        EXPECT_TRUE(LWLockHeldByMe(&g_lwlock));

        EXPECT_TRUE(LWLockHeldByMeInMode(&g_lwlock, LW_EXCLUSIVE));

        EXPECT_FALSE(LWLockHeldByMeInMode(&g_lwlock, LW_SHARED));

        g_count++;

        LWLockRelease(&g_lwlock);
    }

    UtLWLockThreadFunUinit();
}

TEST_F(LWLockTest, LWLockConditionalAcquireAndRelease_level0)
{
    std::thread t[UT_LWLOCK_THREAD_COUNT];

    for (int i = 0; i < UT_LWLOCK_THREAD_COUNT; i++) {
        t[i] = std::thread(UtLWLockConditionalAcquireThreadFun);
    }

    for(int i = 0; i < UT_LWLOCK_THREAD_COUNT; i++) {
        t[i].join();
    }

    ASSERT_EQ(g_count, UT_LWLOCK_THREAD_COUNT * UT_LWLOCK_LOOP_COUNT);
}


void UtLWLockAcquireOrWaitThreadFun()
{
    UtLWLockThreadFunInit();

    pthread_barrier_wait(&g_barrier);

    for (int i = 0; i < UT_LWLOCK_LOOP_COUNT; i++) {
        while (!LWLockAcquireOrWait(&g_lwlock, LW_EXCLUSIVE))
        {
            EXPECT_FALSE(LWLockHeldByMe(&g_lwlock));
        }

        EXPECT_TRUE(LWLockHeldByMe(&g_lwlock));

        EXPECT_TRUE(LWLockHeldByMeInMode(&g_lwlock, LW_EXCLUSIVE));

        g_count++;

        LWLockRelease(&g_lwlock);
    }

    UtLWLockThreadFunUinit();
}

TEST_F(LWLockTest, UtLWLockAcquireOrWaitThreadFunAndRelease_level0)
{
    std::thread t[UT_LWLOCK_THREAD_COUNT];

    for (int i = 0; i < UT_LWLOCK_THREAD_COUNT; i++) {
        t[i] = std::thread(UtLWLockAcquireOrWaitThreadFun);
    }

    for(int i = 0; i < UT_LWLOCK_THREAD_COUNT; i++) {
        t[i].join();
    }

    ASSERT_EQ(g_count, UT_LWLOCK_THREAD_COUNT * UT_LWLOCK_LOOP_COUNT);
}

void UtLWLockSharedReadThreadFun()
{
    UtLWLockThreadFunInit();

    pthread_barrier_wait(&g_barrier);

    for (int i = 0; i < UT_LWLOCK_LOOP_COUNT; i++) {
        DstoreLWLockAcquire(&g_lwlock, LW_SHARED);

        EXPECT_TRUE(LWLockHeldByMe(&g_lwlock));

        EXPECT_TRUE(LWLockHeldByMeInMode(&g_lwlock, LW_SHARED));

        int count = g_count;

        LWLockRelease(&g_lwlock);
    }

    UtLWLockThreadFunUinit();
}

TEST_F(LWLockTest, LWLockAcquireReleaseSummaryTest_level0)
{
    int j = 0;
    std::thread t[UT_LWLOCK_THREAD_COUNT];

    for (int i = 0; i < UT_LWLOCK_THREAD_COUNT / 3; i++, j++) {
        t[j] = std::thread(UtLWLockAcquireThreadFun);
    }

    for (int i = 0; i < UT_LWLOCK_THREAD_COUNT / 3; i++, j++) {
        t[j] = std::thread(UtLWLockConditionalAcquireThreadFun);
    }

    for (int i = 0; i < UT_LWLOCK_THREAD_COUNT / 3; i++, j++) {
        t[j] = std::thread(UtLWLockAcquireOrWaitThreadFun);
    }

    for (int i = 0; i < UT_LWLOCK_THREAD_COUNT % 3; i++, j++) {
        t[j] = std::thread(UtLWLockSharedReadThreadFun);
    }

    for(int i = 0; i < UT_LWLOCK_THREAD_COUNT; i++) {
        t[i].join();
    }

    ASSERT_EQ(g_count, (UT_LWLOCK_THREAD_COUNT - (UT_LWLOCK_THREAD_COUNT % 3)) * UT_LWLOCK_LOOP_COUNT);
}

TEST_F(LWLockTest, DstoreLWLockMultiCrossThreadReleaseSharedLockTest_level0)
{
    DSTORE::LWLock lock;
    LWLockInitialize(&lock);
    std::atomic_bool sync(true);
    std::atomic_long hasReachSyncThreads(0);

    auto AddSharedLockAndReleaseRecordThread = [](DSTORE::LWLock *lock) {
        UtLWLockThreadFunInit();
        DstoreLWLockAcquire(lock, LW_SHARED);
        LWLockDisown(lock);
        UtLWLockThreadFunUinit();
    };

    auto OwnSharedLockAndReleaseThread = [&sync, &hasReachSyncThreads](DSTORE::LWLock *lock) {
        UtLWLockThreadFunInit();
        DstoreLWLockOwn(lock, LW_SHARED);
        hasReachSyncThreads.fetch_add(1, std::memory_order_relaxed);
        while (sync);
        LWLockRelease(lock);
        UtLWLockThreadFunUinit();
    };

    auto AddSharedLockAndReleaseThread = [&sync, &hasReachSyncThreads](DSTORE::LWLock *lock) {
        UtLWLockThreadFunInit();
        DstoreLWLockAcquire(lock, LW_SHARED);
        hasReachSyncThreads.fetch_add(1, std::memory_order_relaxed);
        while (sync);
        LWLockRelease(lock);
        UtLWLockThreadFunUinit();
    };
    /* 1.acquire a no Owner LWlock */
    auto thread1 = std::thread(AddSharedLockAndReleaseRecordThread, &lock);
    thread1.join();

    auto thread2 = std::thread(OwnSharedLockAndReleaseThread, &lock);
    auto thread3 = std::thread(AddSharedLockAndReleaseThread, &lock);
    while (true) {
        /* 2. wait thread2 thread3 reach locking syncpoint */
        if (hasReachSyncThreads.load(std::memory_order_relaxed) >= 2) {
            /* race to release */
            sync = false;
            break;
        }
        GaussUsleep(1000);
    }
    thread2.join();
    thread3.join();
    uint64 curState = GsAtomicReadU64(&lock.state);
    bool isLocked = (curState & (1 << 24 | 1 << 25));
    ASSERT_EQ(isLocked, false);
}

TEST_F(LWLockTest, LWLockDisallowPreemptStressTest_level2)
{
    DSTORE::LWLock lock;
    LWLockInitialize(&lock);
    volatile uint64_t testCount = 0;
    std::atomic_bool sync(true);
    static const uint32 loopTimesBase = 100000; /* Change to 1000000 under release compile. */
    static const uint32 threadNum = 30;
    std::chrono::microseconds exclusiveWaitTime{};
    static const uint32 dataLength = 100000;
    uint32 *array = (uint32 *)DstorePalloc(sizeof(uint32) * dataLength);
    for (uint32 i = 0; i < dataLength; i++) {
        array[i] = i;
    }

    auto func = [&sync, &exclusiveWaitTime, &array](LWLock *lock, volatile uint64_t *testCount, bool read) {
        pthread_setname_np(pthread_self(), "ConcurrencyTest");
        create_thread_and_register();
        uint32 loopTimes;
        while (sync) {};  /* sync */
        if (read) {
            loopTimes = loopTimesBase;
        } else {
            loopTimes = 3;
            usleep(1000);
        }

        for (uint32 i = 0; i < loopTimes; i++) {
            if (read) {
                DstoreLWLockAcquire(lock, LW_SHARED);
                /* testCount init to 1, and increasing monotonically */
                auto loopSearch = [](uint32 *arr, uint32 length, uint32 target) -> int {
                    uint id = 0;
                    for (id = 0; id < length; id++) {
                        if (arr[id] == target) {
                            return id;
                        }
                    }
                    return id;
                };

                uint32 idx = loopSearch(array, dataLength, dataLength - 1);
                EXPECT_TRUE(idx == (dataLength - 1));
            } else {
                std::chrono::time_point<std::chrono::high_resolution_clock> startAcquireTime = std::chrono::high_resolution_clock::now();
                DstoreLWLockAcquire(lock, LW_EXCLUSIVE);
                std::chrono::time_point<std::chrono::high_resolution_clock> endAcquireTime = std::chrono::high_resolution_clock::now();
                exclusiveWaitTime += std::chrono::duration_cast<std::chrono::microseconds>(endAcquireTime - startAcquireTime);
                auto temp = *testCount;
                temp++;
                *testCount = temp;
            }
            LWLockRelease(lock);
        }
        unregister_thread();
    };

    std::vector<std::thread> threadVec(threadNum);
    for (uint32 i = 0; i < threadNum; i++) {
        /* i=0,5,10,... is write thread, other is read */
        threadVec[i] = std::thread(func, &lock, &testCount, (i % 5) != 0);
    }

    std::chrono::time_point<std::chrono::high_resolution_clock> startTime = std::chrono::high_resolution_clock::now();
    sync = false;
    for (auto &thread : threadVec) {
        thread.join();
    }

    std::chrono::time_point<std::chrono::high_resolution_clock> endTime = std::chrono::high_resolution_clock::now();
    std::chrono::microseconds duration = std::chrono::duration_cast<std::chrono::microseconds>(endTime - startTime);
    printf("total cost = %.3lfs, exclusive lock count %lu\n", (double)duration.count() / 1000 / 1000, testCount);
    printf("exclusive lock total wait time %.3lfs\n", (double)exclusiveWaitTime.count() / 1000 / 1000);
}