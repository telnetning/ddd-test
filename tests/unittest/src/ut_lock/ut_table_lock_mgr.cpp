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

#include "ut_utilities/ut_dstore_framework.h"
#include "common/memory/dstore_mctx.h"
#include "framework/dstore_instance.h"
#include "lock/dstore_table_lock_mgr.h"
#include "lock/dstore_lock_interface.h"
#include "lock/dstore_lock_entry.h"
#include "transaction/dstore_transaction_interface.h"
#include <thread>
#include <mutex>
#include <stdlib.h>
#include <time.h>

using namespace DSTORE;

class UTTableLockMgr : public DSTORETEST {
protected:
    void SetUp() override
    {
        DSTORETEST::SetUp();
        UtInstance *instance = DstoreNew(m_ut_memory_context)UtInstance();

        DSTORETEST::m_guc.lockHashTableSize = 256;
        DSTORETEST::m_guc.lockTablePartitionNum = 256;
        instance->Install(&DSTORETEST::m_guc, m_ut_memory_context);
        instance->Startup(&DSTORETEST::m_guc);
    }

    void TearDown() override
    {
        UtInstance *instance = (UtInstance *)g_storageInstance;
        instance->Shutdown();
        delete instance;
        DSTORETEST::TearDown();
    }

    static void ThreadInit()
    {
        UtInstance *instance = (UtInstance *)g_storageInstance;
        instance->ThreadSetupAndRegister();
    }

    static void ThreadDestroy()
    {
        UtInstance *instance = (UtInstance *)g_storageInstance;
        instance->ThreadUnregisterAndExit();
    }

    static void LockCompeteTestThread(uint32 *counter, uint32 cycle);
    static void LockConflictTestThread(int holderMode);
    static void LockRandomConflictTestThread(uint32 cycle, LockMask *mask, uint32 lockModeCnts[]);
    static void LockWeakLockCompeteTestThread(uint32 *counter, uint32 cycle);
    static void LockStrongLockCompeteTestThread(uint32 *counter, uint32 cycle);
    static void TestTryLockTableThread(LockInterface::TableLockContext *context);
    static void PerformanceTestThread(int relationNum, int loopTimes);
    static void RunWaitConflictLockThread(const LockTag &tag, pthread_barrier_t *b1, pthread_barrier_t *b2);
    static void RunWaitConflictLockWithoutTimeout(const LockTag &tag, pthread_barrier_t *b, bool *needWait);
    static void RunTableLockCancelThread1(LockInterface::TableLockContext *context,
        pthread_barrier_t *b1, pthread_barrier_t *b2);
    static void RunTableLockCancelThread2(LockInterface::TableLockContext *context,
        pthread_barrier_t *b1, pthread_barrier_t *b2, ThreadContext **currThrd, uint64 *releaseTime);

    static std::mutex dataLock;
};

std::mutex UTTableLockMgr::dataLock;

/*
 * Basic lock test:
 *  1. lock() returns SUCC in an empty TableLockMgr.
 *  2. unlock() causes no memory leakage.
 */
TEST_F(UTTableLockMgr, LockAndUnlockTest_level0)
{
    ASSERT_FALSE(g_storageInstance->GetTableLockMgr() == NULL);

    LockTag tag;
    tag.SetTableLockTag(0, 0);
    LockMode mode = DSTORE_EXCLUSIVE_LOCK;

    LockErrorInfo info = {0};
    RetStatus ret = g_storageInstance->GetTableLockMgr()->Lock(&tag, mode, false, &info);
    EXPECT_TRUE(ret == DSTORE_SUCC);

    g_storageInstance->GetTableLockMgr()->Unlock(&tag, mode);
}

void UTTableLockMgr::LockCompeteTestThread(uint32 *counter, uint32 cycle)
{
    ThreadInit();

    LockTag tag;
    LockMode mode = DSTORE_EXCLUSIVE_LOCK;
    tag.SetTableLockTag(0, 0);

    for (int i = 0; i < cycle; i++) {
        RetStatus ret = g_storageInstance->GetTableLockMgr()->Lock(&tag, mode, false, nullptr);
        EXPECT_TRUE(ret == DSTORE_SUCC);
        (*counter)++;
        g_storageInstance->GetTableLockMgr()->Unlock(&tag, mode);
    }

    ThreadDestroy();
}

/*
 * Make sure critical section is protected by lock.
 */
TEST_F(UTTableLockMgr, LockCompeteTest_level0)
{
    ASSERT_FALSE(g_storageInstance->GetTableLockMgr() == NULL);

    uint32 counter = 0;
    uint32 cycle = 16;
    uint32 threadsTotal = 16;
    std::thread t[threadsTotal];

    for (int i = 0; i < threadsTotal; i++) {
        t[i] = std::thread(LockCompeteTestThread, &counter, cycle);
    }

    for(int i = 0; i < threadsTotal; i++) {
        t[i].join();
    }

    EXPECT_TRUE(counter == threadsTotal * cycle);
}

void UTTableLockMgr::LockConflictTestThread(int holderMode)
{
    ThreadInit();

    LockTag tag;
    RetStatus ret;
    tag.SetTableLockTag(0, 0);

    for (int mode = DSTORE_ACCESS_SHARE_LOCK; mode < DSTORE_LOCK_MODE_MAX; mode++) {
        ret = g_storageInstance->GetTableLockMgr()->Lock(&tag, static_cast<LockMode>(mode),
            true, nullptr);
        if (HasConflictWithMode(static_cast<LockMode>(holderMode), static_cast<LockMode>(mode)) == false) {
            EXPECT_TRUE(ret == DSTORE_SUCC);
            g_storageInstance->GetTableLockMgr()->Unlock(&tag, static_cast<LockMode>(mode));
        } else {
            EXPECT_FALSE(ret == DSTORE_SUCC);
            EXPECT_TRUE(StorageGetErrorCode() == LOCK_INFO_NOT_AVAIL);
        }
    }

    ThreadDestroy();
}

/*
 * Make sure the overall conflict behavior of TableLockMgr(fast path + lock table) is correct,
 * by simulating 2 threads requesting for all kinds of lockmodes.
 */
TEST_F(UTTableLockMgr, LockConflictTest_level0)
{
    ASSERT_FALSE(g_storageInstance->GetTableLockMgr() == NULL);

    LockTag tag;
    RetStatus ret;
    std::thread t;
    tag.SetTableLockTag(0, 0);

    for (int mode = DSTORE_ACCESS_SHARE_LOCK; mode < DSTORE_LOCK_MODE_MAX; mode++) {
        ret = g_storageInstance->GetTableLockMgr()->Lock(&tag, static_cast<LockMode>(mode),
            true, nullptr);
        ASSERT_TRUE(ret == DSTORE_SUCC);

        t = std::thread(LockConflictTestThread, mode);
        t.join();

        g_storageInstance->GetTableLockMgr()->Unlock(&tag, static_cast<LockMode>(mode));
    }
}

void UTTableLockMgr::LockRandomConflictTestThread(uint32 cycle, LockMask *mask, uint32 lockModeCnts[])
{
    ThreadInit();

    LockTag tag;
    tag.SetTableLockTag(0, 0);
    LockMask tmpMask;
    RetStatus success;
    uint32 conflictMap[DSTORE_LOCK_MODE_MAX] = {0, 0x100, 0x180, 0x1E0, 0x1F0, 0x1D8, 0x1F8, 0x1FC, 0x1FE};

    for (int i = 0; i < cycle; i++) {
        uint32 mode = (rand() % (DSTORE_LOCK_MODE_MAX - 1)) + 1;

        dataLock.lock();
        success = g_storageInstance->GetTableLockMgr()->Lock(&tag,
            static_cast<LockMode>(mode), true, nullptr);
        tmpMask = *mask;
        if (success == DSTORE_SUCC) {
            (*mask) |= (1 << mode);
            lockModeCnts[mode]++;
        }
        dataLock.unlock();

        if ((conflictMap[mode] & tmpMask) != 0) {
            EXPECT_FALSE(success == DSTORE_SUCC);
            EXPECT_TRUE(StorageGetErrorCode() == LOCK_INFO_NOT_AVAIL);
            if (success == DSTORE_SUCC) {
                printf("LockRandomConflictTest failed%d. holdmask: 0x%x, request %d\n", success, tmpMask, mode);
            }
        } else {
            EXPECT_TRUE(success == DSTORE_SUCC);
            if (success != DSTORE_SUCC) {
                printf("LockRandomConflictTest failed%d. holdmask: 0x%x, request %d\n", success, tmpMask, mode);
            }
        }

        if (success != DSTORE_SUCC) {
            continue;
        }

        dataLock.lock();
        g_storageInstance->GetTableLockMgr()->Unlock(&tag, static_cast<LockMode>(mode));
        lockModeCnts[mode]--;
        if (lockModeCnts[mode] == 0) {
            (*mask) &= ~(1 << mode);
        }
        dataLock.unlock();
    }

    ThreadDestroy();
}

/*
 * Make sure the overall conflict behavior of TableLockMgr(fast path + lock table) is correct,
 * by simulating 16 threads requesting for random lockmodes.
 *
 *   This test case is to cover: Multiple weak locks for different backends should all be transfered
 * into lock table when one thread ask for a strong lock, which is not covered by previous test cases.
 * Since full permutation is too big(8^16) for 16 threads, this is a random test.
 */
TEST_F(UTTableLockMgr, LockRandomConflictTest_level0)
{
    ASSERT_FALSE(g_storageInstance->GetTableLockMgr() == NULL);

    uint32 cycle = 16;
    uint32 threadsTotal = 16;
    LockMask mask = 0;
    uint32 lockModeCnts[DSTORE_LOCK_MODE_MAX] = {0};
    std::thread t[threadsTotal];

    srand((unsigned int)time(NULL));

    for (int i = 0; i < threadsTotal; i++) {
        t[i] = std::thread(LockRandomConflictTestThread, cycle, &mask, lockModeCnts);
    }

    for(int i = 0; i < threadsTotal; i++) {
        t[i].join();
    }
}

void UTTableLockMgr::LockWeakLockCompeteTestThread(uint32 *counter, uint32 cycle)
{
    ThreadInit();

    LockTag tag;
    tag.SetTableLockTag(0, 0);
    LockMode mode = DSTORE_ROW_SHARE_LOCK;

    for (int i = 0; i < cycle; i++) {
        RetStatus ret = g_storageInstance->GetTableLockMgr()->Lock(&tag, mode, false, nullptr);
        EXPECT_TRUE(ret == DSTORE_SUCC);
        TsAnnotateBenignRaceSized(counter, sizeof(uint32));
        (*counter)++;
        g_storageInstance->GetTableLockMgr()->Unlock(&tag, mode);
    }

    ThreadDestroy();
}

void UTTableLockMgr::LockStrongLockCompeteTestThread(uint32 *counter, uint32 cycle)
{
    ThreadInit();

    LockTag tag;
    tag.SetTableLockTag(0, 0);
    LockMode mode = DSTORE_EXCLUSIVE_LOCK;

    for (int i = 0; i < cycle; i++) {
        RetStatus ret = g_storageInstance->GetTableLockMgr()->Lock(&tag, mode, false, nullptr);
        EXPECT_TRUE(ret == DSTORE_SUCC);
        (*counter)++;
        g_storageInstance->GetTableLockMgr()->Unlock(&tag, mode);
    }

    ThreadDestroy();
}

/*
 * Make sure critical section is protected between weak and strong lock competition.
 */
TEST_F(UTTableLockMgr, LockWeakStrongLockCompeteTest_level0)
{
    ASSERT_FALSE(g_storageInstance->GetTableLockMgr() == NULL);

    uint32 counter = 0;
    uint32 cycle = 256;
    uint32 threadsTotal = 2;
    std::thread t[2];

    t[0] = std::thread(LockWeakLockCompeteTestThread, &counter, cycle);
    t[1] = std::thread(LockStrongLockCompeteTestThread, &counter, cycle);

    for(int i = 0; i < 2; i++) {
        t[i].join();
    }

    EXPECT_TRUE(counter == threadsTotal * cycle);
}

/*
 * Make sure LockInterface returns LOCKACQUIRE_ALREADY_HELD when table lock
 * is already held by the thread/transaction. This is tested by acquiring same
 * table lock twice by the same thread.
 * Tests both TABLE and PARTITION locks.
 */
TEST_F(UTTableLockMgr, TestLockAlreadyHeldForTableLockInterface_level0)
{
    /* Test for table lock first, then for the partition lock. */
    for (int i = 0; i < 2; i++) {
        bool isPartition = (i == 0) ? false : true;
        /* Step 1. Prepare a TableLockContext */
        LockInterface::TableLockContext context;
        context.dbId = 0;
        context.relId = 0;
        context.isSessionLock = false;
        context.isPartition = isPartition;
        context.mode = DSTORE_EXCLUSIVE_LOCK;
        context.dontWait = LOCK_WAIT;

        /* Step 2. Acquire lock for the first time will success with LOCKACQUIRE_OK. */
        RetStatus res = LockInterface::LockTable(&context);
        EXPECT_TRUE(res == DSTORE_SUCC);
        EXPECT_TRUE(context.result == LockInterface::LockAcquireResult::LOCKACQUIRE_OK);

        /* Step 3. Acquire same lock again and detects table lock is already held. */
        RetStatus res2 = LockInterface::LockTable(&context);
        EXPECT_TRUE(res2 == DSTORE_SUCC);
        EXPECT_TRUE(context.result == LockInterface::LockAcquireResult::LOCKACQUIRE_ALREADY_HELD);

        /* Step 4. Unlock the table lock twice. */
        LockInterface::UnlockTable(&context);
        LockInterface::UnlockTable(&context);

        /*
         * Check the success of Unlock by locking the same lock again and not
         * getting LOCKACQUIRE_ALREADY_HELD.
         */
        res = LockInterface::LockTable(&context);
        EXPECT_TRUE(res == DSTORE_SUCC);
        EXPECT_TRUE(context.result == LockInterface::LockAcquireResult::LOCKACQUIRE_OK);
        LockInterface::UnlockTable(&context);
    }
}

/*
 * Thread function that acquires table lock with dontWait = true.
 * Used to test that table lock will not hang and will return the correct error
 * when table lock is already held by other backend.
 */
void UTTableLockMgr::TestTryLockTableThread(LockInterface::TableLockContext *context)
{
    ThreadInit();
    RetStatus res = LockInterface::LockTable(context);
    EXPECT_TRUE(res == DSTORE_FAIL);
    EXPECT_TRUE(context->result == LockInterface::LockAcquireResult::LOCKACQUIRE_NOT_AVAIL);
    ThreadDestroy();
}

/*
 * Test non-blocking LockTable from the LockInterface API. It returns LOCKACQUIRE_OK when table
 * lock is available, and returns LOCKACQUIRE_NOT_AVAIL when lock is already taken.
 * This is tested by acquiring table lock in main thread and trying to acquire
 * the same table lock from another thread.
 * Tests both TABLE and PARTITION locks.
 */
TEST_F(UTTableLockMgr, TestTryLockForTableLockInterface_level0)
{
    /* Test for table lock first, then for the partition lock. */
    for (int i = 0; i < 2; i++) {
        bool isPartition = (i == 0) ? false : true;
        /* Step 1. Prepare a TableLockContext */
        LockInterface::TableLockContext context;
        context.dbId = 0;
        context.relId = 0;
        context.isSessionLock = false;
        context.isPartition = isPartition;
        context.mode = DSTORE_EXCLUSIVE_LOCK;
        context.dontWait = LOCK_DONT_WAIT;

        /*
         * Step 2. Try to lock table with dontWait = true.
         * Acquire lock for the first time will success with LOCKACQUIRE_OK.
         */
        RetStatus res = LockInterface::LockTable(&context);
        EXPECT_TRUE(res == DSTORE_SUCC);
        EXPECT_TRUE(context.result == LockInterface::LockAcquireResult::LOCKACQUIRE_OK);

        /* Step 3. Simulates another thread to acquire table lock. */
        std::thread thread;
        thread = std::thread(TestTryLockTableThread, &context);
        thread.join();

        /* Step 4. Unlock the table lock. */
        UnlockTable(&context);

        /* Check the success of Unlock by locking the same lock again. */
        res = LockInterface::LockTable(&context);
        EXPECT_TRUE(res == DSTORE_SUCC);
        EXPECT_TRUE(context.result == LockInterface::LockAcquireResult::LOCKACQUIRE_OK);
        LockInterface::UnlockTable(&context);
    }
}

void UTTableLockMgr::PerformanceTestThread(int relationNum, int loopTimes)
{
    ThreadInit();

    LockInterface::TableLockContext context;
    context.dbId = 0;
    context.relId = 0;
    context.isPartition = false;
    context.isSessionLock = false;
    context.mode = DSTORE_ROW_EXCLUSIVE_LOCK;
    context.dontWait = LOCK_WAIT;
    RetStatus res = DSTORE_SUCC;
    static const int COMMAND_NUM = 8;

    for (int i = 0; i < loopTimes; i++) {
        res = TransactionInterface::StartTrxCommand();
        EXPECT_TRUE(res == DSTORE_SUCC);
        res = TransactionInterface::BeginTrxBlock();
        EXPECT_TRUE(res == DSTORE_SUCC);
        res = TransactionInterface::CommitTrxCommand();
        EXPECT_TRUE(res == DSTORE_SUCC);

        for (int j = 0; j < COMMAND_NUM; j++) {
            res = TransactionInterface::StartTrxCommand();
            EXPECT_TRUE(res == DSTORE_SUCC);

            for (int k = 0; k < relationNum; k++) {
                context.relId = k;
                res = LockInterface::LockTable(&context);
                EXPECT_TRUE(res == DSTORE_SUCC);
            }

            res = TransactionInterface::CommitTrxCommand();
            EXPECT_TRUE(res == DSTORE_SUCC);
        }

        res = TransactionInterface::StartTrxCommand();
        EXPECT_TRUE(res == DSTORE_SUCC);
        res = TransactionInterface::EndTrxBlock();
        EXPECT_TRUE(res == DSTORE_SUCC);
        res = TransactionInterface::CommitTrxCommand();
        EXPECT_TRUE(res == DSTORE_SUCC);
    }

    ThreadDestroy();
}

/* The test case only make sense under release mode. */
TEST_F(UTTableLockMgr, DISABLED_InterfacePerformanceTest_TIER1)
{
    ASSERT_FALSE(g_storageInstance->GetTableLockMgr() == NULL);

    static const int TEST_REL_NUM = 200;
    static const int TEST_LOOP_TIMES = 20000;
    static const int TEST_THREAD_NUM = 100;

    std::thread threads[TEST_THREAD_NUM];
    for (int i = 0; i < TEST_THREAD_NUM; i++) {
        threads[i] = std::thread(PerformanceTestThread, TEST_REL_NUM, TEST_LOOP_TIMES);
    }

    std::chrono::time_point<std::chrono::high_resolution_clock> startTime = std::chrono::high_resolution_clock::now();

    for (int i = 0; i < TEST_THREAD_NUM; i++) {
        threads[i].join();
    }

    std::chrono::time_point<std::chrono::high_resolution_clock> endTime = std::chrono::high_resolution_clock::now();
    std::chrono::microseconds duration = std::chrono::duration_cast<std::chrono::microseconds>(endTime - startTime);
    printf("cost = %.3lfs\n", (double)duration.count() / 1000 / 1000);
}

int MockGetSessionLockWaitTimeout2()
{
    return 6000;
}

void UTTableLockMgr::RunWaitConflictLockThread(const LockTag &tag, pthread_barrier_t *b1, pthread_barrier_t *b2)
{
    ThreadInit();
    LockErrorInfo info = {0};
    static constexpr uint64 MICROSEC_PER_MILLISEC = 1000;
    RetStatus ret = g_storageInstance->GetTableLockMgr()->Lock(&tag, DSTORE_SHARE_LOCK, LOCK_WAIT, &info);
    ASSERT_EQ(ret, DSTORE_SUCC);
    g_storageInstance->GetTableLockMgr()->Unlock(&tag, DSTORE_SHARE_LOCK);

    pthread_barrier_wait(b1);
    uint64 start_wait = GetSystemTimeInMicrosecond();
    ret = g_storageInstance->GetTableLockMgr()->Lock(&tag, DSTORE_EXCLUSIVE_LOCK, LOCK_WAIT, &info);

    if (ret != DSTORE_SUCC) {
        uint64 current_time = GetSystemTimeInMicrosecond();
        ASSERT_TRUE(current_time >= start_wait + MockGetSessionLockWaitTimeout2() * MICROSEC_PER_MILLISEC);
        ASSERT_TRUE(StorageGetErrorCode() == LOCK_ERROR_WAIT_TIMEOUT);
    }

    pthread_barrier_wait(b2);

    if (ret == DSTORE_SUCC) {
        g_storageInstance->GetTableLockMgr()->Unlock(&tag, DSTORE_EXCLUSIVE_LOCK);
    }
    ThreadDestroy();
}

/* Test two threads to see if they can both acquire non-mutual locks, but one cannot acquire the conflicting lock. */
TEST_F(UTTableLockMgr, TestTableLockMgrSupportLockWaitTimeout_level0)
{
    ASSERT_FALSE(g_storageInstance->GetTableLockMgr() == NULL);

    LockTag tag;
    tag.SetTableLockTag(0, 0);

    const int numThreads = 2;
    std::thread threads[numThreads];
    pthread_barrier_t b1;
    pthread_barrier_t b2;

    pthread_barrier_init(&b1, NULL, numThreads);
    pthread_barrier_init(&b2, NULL, numThreads);

    g_storageInstance->m_lockWaitTimeoutCallBack = MockGetSessionLockWaitTimeout2;

    threads[0] = std::thread(RunWaitConflictLockThread, tag, &b1, &b2);
    threads[1] = std::thread(RunWaitConflictLockThread, tag, &b1, &b2);

    for (ThreadId i = 0; i < numThreads; i++) {
        threads[i].join();
    }
    pthread_barrier_destroy(&b1);
    pthread_barrier_destroy(&b2);
}

int MockGetSessionLockWaitTimeoutZero()
{
    return 0;
}

void UTTableLockMgr::RunWaitConflictLockWithoutTimeout(const LockTag &tag, pthread_barrier_t *b, bool *needWait)
{
    ThreadInit();
    LockErrorInfo info = {0};

    pthread_barrier_wait(b);
    RetStatus ret = g_storageInstance->GetTableLockMgr()->Lock(&tag, DSTORE_EXCLUSIVE_LOCK, LOCK_WAIT, &info);
    ASSERT_EQ(ret, DSTORE_SUCC);
    uint64 currentTime = GetSystemTimeInMicrosecond();
    std::cout << "start hold lock : " << currentTime << std::endl;
    /* Hold lock for 5s before releasing it, ensuring that waiting thread waiting time exceeds
     * lockwait_timeout and finally obtains the lock. */
    if (*needWait) {
        sleep(5);
        *needWait = false;
    }
    g_storageInstance->GetTableLockMgr()->Unlock(&tag, DSTORE_EXCLUSIVE_LOCK);

    ThreadDestroy();
}

/* Test lockwait_timeout=0: Lock wait timeout detection is not supported. */
TEST_F(UTTableLockMgr, TestTableLockMgrLockWaitTimeoutZero_level0)
{
    ASSERT_FALSE(g_storageInstance->GetTableLockMgr() == NULL);

    LockTag tag;
    tag.SetTableLockTag(0, 0);

    const int numThreads = 2;
    std::thread threads[numThreads];
    pthread_barrier_t b;
    bool needWait = true;

    pthread_barrier_init(&b, NULL, numThreads);

    g_storageInstance->m_lockWaitTimeoutCallBack = MockGetSessionLockWaitTimeoutZero;

    threads[0] = std::thread(RunWaitConflictLockWithoutTimeout, tag, &b, &needWait);
    threads[1] = std::thread(RunWaitConflictLockWithoutTimeout, tag, &b, &needWait);

    for (ThreadId i = 0; i < numThreads; i++) {
        threads[i].join();
    }
    pthread_barrier_destroy(&b);
}

void UTTableLockMgr::RunTableLockCancelThread1(LockInterface::TableLockContext *context, pthread_barrier_t *b1,
    pthread_barrier_t *b2)
{
    ThreadInit();

    RetStatus ret = LockInterface::LockTable(context);
    EXPECT_EQ(ret, DSTORE_SUCC);
    EXPECT_EQ(context->result, LockInterface::LockAcquireResult::LOCKACQUIRE_OK);

    /* thread1 has hold lock. */
    pthread_barrier_wait(b1);

    /* wait until thread2 released lock. */
    pthread_barrier_wait(b2);

    LockInterface::UnlockTable(context);
    ThreadDestroy();
}

void UTTableLockMgr::RunTableLockCancelThread2(LockInterface::TableLockContext *context,
    pthread_barrier_t *b1, pthread_barrier_t *b2, ThreadContext **currThrd, uint64 *releaseTime)
{
    ThreadInit();

    /* thrd is used to send interrupt signal in main thread later. */
    *currThrd = thrd;

    /* wait until thread1 hold lock. */
    pthread_barrier_wait(b1);

    RetStatus ret = LockInterface::LockTable(context);
    *releaseTime = GetSystemTimeInMicrosecond();
    EXPECT_EQ(ret, DSTORE_FAIL);
    EXPECT_EQ(StorageGetErrorCode(), LOCK_ERROR_WAIT_CANCELED);

    /* thread2 has released lock. */
    pthread_barrier_wait(b2);

    ThreadDestroy();
}
