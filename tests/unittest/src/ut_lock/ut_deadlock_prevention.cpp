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
#include "port/dstore_port.h"
#include <thread>
#include <mutex>

using namespace DSTORE;

class UTDeadlockPrevention : public DSTORETEST {
protected:
    void SetUp() override
    {
        DSTORETEST::SetUp();
        m_guc.deadlockTimeInterval = 20000;
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
        semaphores_lock.lock();
        UtInstance *instance = (UtInstance *)g_storageInstance;
        instance->ThreadSetupAndRegister();
        semaphores_lock.unlock();
    }

    static void ThreadDestroy()
    {
        UtInstance *instance = (UtInstance *)g_storageInstance;
        instance->ThreadUnregisterAndExit();
    }

    static void ThreadOne(pthread_barrier_t *barrier1, pthread_barrier_t *barrier2);
    static void ThreadTwo(pthread_barrier_t *barrier1, pthread_barrier_t *barrier2);
    static void ThreadThree(pthread_barrier_t *barrier1, pthread_barrier_t *barrier2);

    static std::mutex semaphores_lock;
};

std::mutex UTDeadlockPrevention::semaphores_lock;

void UTDeadlockPrevention::ThreadOne(pthread_barrier_t *barrier1, pthread_barrier_t *barrier2)
{
    ThreadInit();

    /* t1 obtains the DSTORE_ROW_EXCLUSIVE_LOCK */
    LockTag tag;
    tag.SetTableLockTag(0, 0);
    LockMode mode1 = DSTORE_ROW_EXCLUSIVE_LOCK;
    LockErrorInfo info = {0};
    RetStatus ret = g_storageInstance->GetLockMgr()->Lock(&tag, mode1, false, &info);
    ASSERT_TRUE(ret == DSTORE_SUCC);

    pthread_barrier_wait(barrier1);
    /*
     * Wait 2 seconds to make sure t2 has added
     * its request to the waiting queue before
     * t1 tries to lock DSTORE_SHARE_UPDATE_EXCLUSIVE_LOCK.
     * Note: sleep is needed here since we need
     * to rearrange the waiting queue in a particular
     * order and we don't have an API way to check that
     * some other thread is already waiting for a lock we need.
     */
    GaussUsleep(2000000);
    /*
     * Sync with t3 as well.
     */
    pthread_barrier_wait(barrier2);
    /*
     * t1 tries the DSTORE_ROW_EXCLUSIVE_LOCK but wait
     * t3 will wait 2 seconds for t1 to wait on
     * DSTORE_SHARE_UPDATE_EXCLUSIVE_LOCK.
     *
     * (1) If deadlock prevention is implemented,
     * then t1's request will be the
     * first in the wait queue before t2's request.
     * Then when t3 releases DSTORE_SHARE_UPDATE_EXCLUSIVE_LOCK,
     * t1's request will be picked up, stops waiting,
     * and return to manager->lock. After t1 returns from
     * its wait in manager->lock, it proceeds to release
     * both its owned locks so that t2's request will
     * be picked up and lock will be granted.
     *
     * (2) If deadlock prevention is not implemented,
     * then t1's request will be after t2's request
     * in the wait queue. Then when t3 releases
     * DSTORE_SHARE_UPDATE_EXCLUSIVE_LOCK, t1's reuqest
     * will not be picked as we only try to grant
     * the first request in the wait queue. We try
     * to grant lock to t2's request, but it has
     * conflicting mode with already granted requests.
     * So, t2 keeps sleeping while t1 is also sleeping.
     * t1's second manager->lock will never return
     * and hence the test will hang
     */
    LockMode mode2 = DSTORE_SHARE_UPDATE_EXCLUSIVE_LOCK;
    ret = g_storageInstance->GetLockMgr()->Lock(&tag, mode2, false, &info);
    ASSERT_TRUE(ret == DSTORE_SUCC);

    g_storageInstance->GetLockMgr()->Unlock(&tag, mode1);
    g_storageInstance->GetLockMgr()->Unlock(&tag, mode2);

    ThreadDestroy();
}

void UTDeadlockPrevention::ThreadTwo(pthread_barrier_t *barrier1, pthread_barrier_t *barrier2)
{
    ThreadInit();

    pthread_barrier_wait(barrier1);
    /* t2 tries to obtain the Share Lock */
    LockTag tag;
    tag.SetTableLockTag(0, 0);
    LockMode mode = DSTORE_SHARE_LOCK;
    LockErrorInfo info = {0};
    /* t2 will wait after calling lock() since it is blocked by locks granted
     * to t1's first request and t3's request (i.e. DSTORE_SHARE_LOCK conflicts
     * DSTORE_ROW_EXCLUSIVE_LOCK and DSTORE_SHARE_UPDATE_EXCLUSIVE_LOCK).
     */
    RetStatus ret = g_storageInstance->GetLockMgr()->Lock(&tag, mode, false, &info);

    g_storageInstance->GetLockMgr()->Unlock(&tag, mode);

    ThreadDestroy();
}

void UTDeadlockPrevention::ThreadThree(pthread_barrier_t *barrier1, pthread_barrier_t *barrier2)
{
    ThreadInit();

    /* t3 obtains the DSTORE_SHARE_UPDATE_EXCLUSIVE_LOCK */
    LockTag tag;
    tag.SetTableLockTag(0, 0);
    LockMode mode = DSTORE_SHARE_UPDATE_EXCLUSIVE_LOCK;
    LockErrorInfo info = {0};
    RetStatus ret = g_storageInstance->GetLockMgr()->Lock(&tag, mode, false, &info);
    ASSERT_TRUE(ret == DSTORE_SUCC);

    pthread_barrier_wait(barrier1);
    pthread_barrier_wait(barrier2);
    /*
     * At this point, t1 and t3 own the lock and t2 waits for it
     * Wait 2 seconds for t1 to start waiting for
     * DSTORE_SHARE_UPDATE_EXCLUSIVE_LOCK before releasing
     * Note: sleep is needed here since we want to
     * release the lock only when there is a specific waiting queue
     * and we don't have an API way to check that some
     * other thread is already waiting for a lock we will release.
     */
    GaussUsleep(2000000);
    g_storageInstance->GetLockMgr()->Unlock(&tag, mode);

    ThreadDestroy();
}

/*
 * This test case creates three threads, we call them
 * t1, t2, and t3. t1 first acquires DSTORE_ROW_EXCLUSIVE_LOCK
 * and it is successfully granted. Similar for t3, it
 * acquires DSTORE_SHARE_UPDATE_EXCLUSIVE_LOCK and successfully
 * get it. Then t2 tries to acquire DSTORE_SHARE_LOCK, but this
 * mode conflicts the previously granted lock mode. So,
 * t2 starts waiting and its request is added to the waiting
 * queue, which currently contains only t2's request.
 * After t2 starts waiting and its request being inserted
 * to the waiting queue, t1 tries to acquire DSTORE_SHARE_UPDATE_EXCLUSIVE_LOCK
 * but this will cause t1 to wait since this mode is owned
 * by a different thread t3. t1's second request will be
 * added to the waiting queue.
 * 1) If without deadlock prevention, t1's second request
 * will be inserted after t2's request. When t3 releases
 * its lock, we would try the first request in the waiting queue
 * which is t2's request in such case but find that
 * t2's request has mode conflicting t1's owned lock mode.
 * So t2 is back to waiting again. Notice that t1 is waiting
 * on its second request and a thread cannot release an
 * owned lock and try to acquire a waiting request at
 * the same time. Hence, both t1 and t2 will keep waiting,
 * resulting in deadlock (i.e. test case will hang).
 * 2) If with deadlock prevention, t1's second request will
 * be inserted before t2's request. So once t3 releases its
 * owned lock, we would try the first request in the waiting
 * queue which is t1's second request in such case. t1's second
 * request then will be successfully granted since it has no
 * conflicts with all granted locks. Then t2 will be the only
 * thread waiting. Hence, deadlock is prevented.
 */
TEST_F(UTDeadlockPrevention, UTDeadlockPreventionTest_TIER1)
{
    ASSERT_FALSE(g_storageInstance->GetLockMgr() == NULL);

    uint32 threads_total = 3;
    pthread_barrier_t barrier1;
    pthread_barrier_t barrier2;
    std::thread t[threads_total];
    pthread_barrier_init(&barrier1, NULL, threads_total);
    pthread_barrier_init(&barrier2, NULL, threads_total - 1);

    t[0] = std::thread(ThreadOne, &barrier1, &barrier2);
    t[1] = std::thread(ThreadTwo, &barrier1, &barrier2);
    t[2] = std::thread(ThreadThree, &barrier1, &barrier2);

    t[0].join();
    t[1].join();
    t[2].join();
}
