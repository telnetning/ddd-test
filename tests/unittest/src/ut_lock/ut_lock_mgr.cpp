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
#include "framework/dstore_instance.h"
#include "common/memory/dstore_mctx.h"
#include "lock/dstore_lock_mgr.h"
#include <thread>
#include <mutex>

using namespace DSTORE;

class UTLockMgr : public DSTORETEST {
protected:
    void SetUp() override
    {
        DSTORETEST::SetUp();
        UtInstance *instance = DstoreNew(m_ut_memory_context)UtInstance();
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

    static void RunConcurrentLockUpgradeThread(const LockTag &tag, pthread_barrier_t *b1, pthread_barrier_t *b2);

    static void RunWaitConflictLockThread(const LockTag &tag, pthread_barrier_t *b, bool *need_wait);

    static void RunLockMgrCancelThread1(const LockTag &tag, pthread_barrier_t *b1, pthread_barrier_t *b2,
        uint64 *releaseTime);

    static void RunLockMgrCancelThread2(const LockTag &tag, pthread_barrier_t *b1,
        ThreadContext **currThrd, uint64 *holdLockTime);
};

/*
 * Basic lock test:
 *  1. Lock() returns SUCC in an empty LockMgr.
 *  2. Unlock() causes no memory leakage.
 */
TEST_F(UTLockMgr, LockAndUnlockTest_level0)
{
    ASSERT_FALSE(g_storageInstance->GetLockMgr() == NULL);

    LockTag tag;
    tag.SetTableLockTag(0, 0);
    LockMode mode = DSTORE_EXCLUSIVE_LOCK;
    LockErrorInfo info = {0};
    RetStatus ret = g_storageInstance->GetLockMgr()->Lock(&tag, mode, LOCK_WAIT, &info);
    EXPECT_TRUE(ret == DSTORE_SUCC);

    g_storageInstance->GetLockMgr()->Unlock(&tag, mode);
}

void UTLockMgr::LockCompeteTestThread(uint32 *counter, uint32 cycle)
{
    ThreadInit();

    LockTag tag;
    tag.SetTableLockTag(0, 0);
    LockMode mode = DSTORE_EXCLUSIVE_LOCK;
    LockErrorInfo info = {0};

    for (int i = 0; i < cycle; i++) {
        RetStatus ret = g_storageInstance->GetLockMgr()->Lock(&tag, mode, LOCK_WAIT, &info);
        EXPECT_TRUE(ret == DSTORE_SUCC);
        (*counter)++;
        g_storageInstance->GetLockMgr()->Unlock(&tag, mode);
    }

    ThreadDestroy();
}

/*
 * Make sure critical section is protected by lock.
 */
TEST_F(UTLockMgr, LockCompeteTest_level0)
{
    ASSERT_FALSE(g_storageInstance->GetLockMgr() == NULL);

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

/*
 * Lock request for a previously granted lock could be granted by local lock directly.
 */
TEST_F(UTLockMgr, RecursiveLockTest_level0)
{
    ASSERT_FALSE(g_storageInstance->GetLockMgr() == NULL);

    LockTag tag;
    tag.SetTableLockTag(0, 0);
    LockMode mode = DSTORE_EXCLUSIVE_LOCK;
    LockErrorInfo info = {0};
    RetStatus ret = g_storageInstance->GetLockMgr()->Lock(&tag, mode, LOCK_WAIT, &info);
    EXPECT_TRUE(ret == DSTORE_SUCC);

    ret = g_storageInstance->GetLockMgr()->Lock(&tag, mode, LOCK_WAIT, &info);
    EXPECT_TRUE(ret == DSTORE_SUCC);

    g_storageInstance->GetLockMgr()->Unlock(&tag, mode);
    g_storageInstance->GetLockMgr()->Unlock(&tag, mode);
}

/*
 * Thread function that first acquires SHARE lock and then tries to acquire EXCLUSIVE
 * lock. Two different threads call this function to simulate the scenario where
 * deadlock with soft edge occurs.
 */
void UTLockMgr::RunConcurrentLockUpgradeThread(const LockTag &tag, pthread_barrier_t *b1, pthread_barrier_t *b2)
{
    ThreadInit();
    LockErrorInfo info = {0};
    RetStatus ret = g_storageInstance->GetLockMgr()->Lock(&tag, DSTORE_SHARE_LOCK, LOCK_WAIT, &info);
    pthread_barrier_wait(b1);
    ret = g_storageInstance->GetLockMgr()->Lock(&tag, DSTORE_EXCLUSIVE_LOCK, LOCK_WAIT, &info);
    /*
     * If I failed to get the EXCLUSIVE lock, then it means I should detect the
     * soft edge deadlock when being inserted to the waiting queue. I will release
     * my SHARE lock for the other thread to return from its EXCLUSIVE lock require.
     */
    if (ret != DSTORE_SUCC) {
        ASSERT_TRUE(StorageGetErrorCode() == LOCK_ERROR_DEADLOCK);
        g_storageInstance->GetLockMgr()->Unlock(&tag, DSTORE_SHARE_LOCK);
    }
    pthread_barrier_wait(b2);
    /*
     * If I succeeded to get the EXCLUSIVE lock, then it means I wake up and get my
     * EXCLUSIVE since the other thread found the deadlock and released its SHARE lock,
     * which was originally preventing me from getting my EXCLUSIVE lock.
     */
    if (ret == DSTORE_SUCC) {
        g_storageInstance->GetLockMgr()->Unlock(&tag, DSTORE_SHARE_LOCK);
        g_storageInstance->GetLockMgr()->Unlock(&tag, DSTORE_EXCLUSIVE_LOCK);
    }
    ThreadDestroy();
}

/*
 * Test early deadlock detection specific to the lock upgrade scenario.
 * This is the testcase that simulates the scenario where we detect soft edge deadlock
 * when trying to insert the lock request into the waiting queue. This is
 * the case where a waiter from the waiting queue conflicts locks owned by
 * me and my request lock conflicts locks owned by the waiter thread.
 * Two threads, concurrently, acquires the SHARE lock and then EXCLUSIVE lock.
 * The first thread starts requiring EXCLUSIVE lock will start sleeping and wait.
 * The second thread that starts requiring EXCLUSIVE lock will detect the
 * early deadlock and release the request so that the first thread will get its
 * EXCLUSIVE lock and return from the Lock() function.
 */
TEST_F(UTLockMgr, TestLockUpgradeEarlyDeadlockDetection_level0)
{
    ASSERT_FALSE(g_storageInstance->GetLockMgr() == NULL);

    LockTag tag;
    tag.SetTableLockTag(0, 0);

    const int numThreads = 2;
    std::thread threads[numThreads];
    pthread_barrier_t b1;
    pthread_barrier_t b2;

    pthread_barrier_init(&b1, NULL, numThreads);
    pthread_barrier_init(&b2, NULL, numThreads);
    /*
     * Both threads would first acquire SHARED lock, and only then
     * would both try to upgrade to EXCLUSIVE. That would trigger the
     * deadlock.
     */
    threads[0] = std::thread(RunConcurrentLockUpgradeThread, tag, &b1, &b2);
    threads[1] = std::thread(RunConcurrentLockUpgradeThread, tag, &b1, &b2);

    for (ThreadId i = 0; i < numThreads; i++) {
        threads[i].join();
    }
    pthread_barrier_destroy(&b1);
    pthread_barrier_destroy(&b2);
}

int MockGetSessionLockWaitTimeout()
{
    return 3000;
}

void UTLockMgr::RunWaitConflictLockThread(const LockTag &tag, pthread_barrier_t *b, bool *need_wait)
{
    ThreadInit();
    LockErrorInfo info = {0};
    RetStatus ret = g_storageInstance->GetLockMgr()->Lock(&tag, DSTORE_SHARE_LOCK, LOCK_WAIT, &info);
    ASSERT_EQ(ret, DSTORE_SUCC);
    g_storageInstance->GetLockMgr()->Unlock(&tag, DSTORE_SHARE_LOCK);

    pthread_barrier_wait(b);
    ret = g_storageInstance->GetLockMgr()->Lock(&tag, DSTORE_EXCLUSIVE_LOCK, LOCK_WAIT, &info);
    ASSERT_EQ(ret, DSTORE_SUCC);
    uint64 currentTime = GetSystemTimeInMicrosecond();
    std::cout << "start hold lock : " << currentTime << std::endl;
    /* Hold lock for 5s before releasing it, ensuring that waiting thread waiting time exceeds
     * lockwait_timeout and finally obtains the lock. */
    if (*need_wait) {
        sleep(5);
        *need_wait = false;
    }
    g_storageInstance->GetLockMgr()->Unlock(&tag, DSTORE_EXCLUSIVE_LOCK);

    ThreadDestroy();
}

/* Two threads requesting conflicting locks can only acquire the lock after the thread that
 * acquired the lock first releases it. If the waiting time for the lock exceeds lockwait_timeout,
 * the thread will continue to wait for the lock.
 */
TEST_F(UTLockMgr, TestLockMgrNotSupportLockWaitTimeout_level0)
{
    ASSERT_FALSE(g_storageInstance->GetLockMgr() == NULL);

    LockTag tag;
    tag.SetTableLockTag(0, 0);

    const int numThreads = 2;
    std::thread threads[numThreads];
    pthread_barrier_t b;
    bool need_wait = true;

    pthread_barrier_init(&b, NULL, numThreads);

    g_storageInstance->m_lockWaitTimeoutCallBack = MockGetSessionLockWaitTimeout;

    threads[0] = std::thread(RunWaitConflictLockThread, tag, &b, &need_wait);
    threads[1] = std::thread(RunWaitConflictLockThread, tag, &b, &need_wait);

    for (ThreadId i = 0; i < numThreads; i++) {
        threads[i].join();
    }
    pthread_barrier_destroy(&b);
}

/* thread1 holds lock, waits until thread2 has been canceled for 1s, and then release lock.*/
void UTLockMgr::RunLockMgrCancelThread1(const LockTag &tag, pthread_barrier_t *b1, pthread_barrier_t *b2,
    uint64 *releaseTime)
{
    ThreadInit();
    LockErrorInfo info = {0};
    RetStatus ret = g_storageInstance->GetLockMgr()->Lock(&tag, DSTORE_EXCLUSIVE_LOCK, LOCK_WAIT, &info);
    EXPECT_TRUE(ret == DSTORE_SUCC);

    /* thread1 has hold lock. */
    pthread_barrier_wait(b1);

    /* wait until main thread has sent cancel signal to thread2 for 1s. */
    pthread_barrier_wait(b2);

    *releaseTime = GetSystemTimeInMicrosecond();
    g_storageInstance->GetLockMgr()->Unlock(&tag, DSTORE_EXCLUSIVE_LOCK);

    ThreadDestroy();
}

/* thread2 wait until thread1 hold lock, and then try to lock */
void UTLockMgr::RunLockMgrCancelThread2(const LockTag &tag, pthread_barrier_t *b1,
    ThreadContext **currThrd, uint64 *holdLockTime)
{
    ThreadInit();

    /* thrd is used to send interrupt signal in main thread later. */
    *currThrd = thrd;

    /* wait until thread1 hold lock */
    pthread_barrier_wait(b1);

    LockErrorInfo info = {0};
    RetStatus ret = g_storageInstance->GetLockMgr()->Lock(&tag, DSTORE_EXCLUSIVE_LOCK, LOCK_WAIT, &info);
    *holdLockTime = GetSystemTimeInMicrosecond();
    EXPECT_EQ(ret, DSTORE_SUCC);

    g_storageInstance->GetLockMgr()->Unlock(&tag, DSTORE_EXCLUSIVE_LOCK);

    ThreadDestroy();
}
