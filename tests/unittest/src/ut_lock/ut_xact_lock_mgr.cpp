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
#include "lock/dstore_xact_lock_mgr.h"
#include <thread>
#include <chrono>

using namespace DSTORE;
using namespace std::chrono;

class UTXactLockMgr : public DSTORETEST {
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

    static void WaitForTransactionThread(const Xid &xid, uint64 *timeAfterWaitOnXid);
    static void TransferLockThrd(Xid xid, pthread_barrier_t *barrier, bool isAcquiredTwice);
    static void RunWaitConflictLockThread(const LockTag &tag, const Xid &xid, pthread_barrier_t *b1,
        pthread_barrier_t *b2);
    static void RunXactLockCancelThread1(const Xid &xid, pthread_barrier_t *b1, pthread_barrier_t *b2);
    static void RunXactLockCancelThread2(const Xid &xid, pthread_barrier_t *b1, pthread_barrier_t *b2,
        ThreadContext **currThrd, uint64 *releaseTime);
};

void UTXactLockMgr::WaitForTransactionThread(const Xid &xid, uint64 *timeAfterWaitOnXid)
{
    UtInstance *instance = (UtInstance *)g_storageInstance;
    instance->ThreadSetupAndRegister();
    ASSERT_FALSE(g_storageInstance->GetXactLockMgr() == NULL);

    RetStatus ret = g_storageInstance->GetXactLockMgr()->Wait(g_defaultPdbId, xid);
    *timeAfterWaitOnXid = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    EXPECT_TRUE(ret == DSTORE_SUCC);

    instance->ThreadUnregisterAndExit();
}

TEST_F(UTXactLockMgr, WaitForTransactionTest_level0)
{
    ASSERT_FALSE(g_storageInstance->GetXactLockMgr() == NULL);

    Xid xid = INVALID_XID;
    RetStatus ret = g_storageInstance->GetXactLockMgr()->Lock(g_defaultPdbId, xid);
    EXPECT_TRUE(ret == DSTORE_SUCC);

    uint64 timeAfterWaitOnXid = 0;
    std::thread waitThread = std::thread(WaitForTransactionThread, xid, &timeAfterWaitOnXid);

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    uint64 timeBeforeReleaseXid = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    g_storageInstance->GetXactLockMgr()->Unlock(g_defaultPdbId, xid);

    waitThread.join();

    /* Wait() should only return after the transaction is complete. */
    EXPECT_TRUE(timeBeforeReleaseXid <= timeAfterWaitOnXid);
}

/*
 * Thread function used as the thread to be transferred out xact lock.
 */
void UTXactLockMgr::TransferLockThrd(Xid xid, pthread_barrier_t *barrier, bool isAcquiredTwice)
{
    UtInstance *instance = (UtInstance *)g_storageInstance;
    instance->ThreadSetupAndRegister();
    XactLockMgr *xactLockMgr = g_storageInstance->GetXactLockMgr();
    RetStatus ret = xactLockMgr->Lock(g_defaultPdbId, xid);
    ASSERT_TRUE(STORAGE_FUNC_SUCC(ret));
    if (isAcquiredTwice) {
        RetStatus ret = xactLockMgr->Lock(g_defaultPdbId, xid);
        ASSERT_TRUE(STORAGE_FUNC_SUCC(ret));
    }
    pthread_barrier_wait(barrier);
    /* Wait for the other thread to transfer the lock. */
    pthread_barrier_wait(barrier);
    /*
     * Check my thread local locks to see if the lock was removed correctly.
     */
    /*
     * TODO: Once TAC adds code to skip cleaning up the old thread's local locks, we add this check.
     *
     * ASSERT_TRUE(thrd->GetLockCtx()->GetLocalLock()->IsEmpty());
     */
    pthread_barrier_wait(barrier);
    pthread_barrier_wait(barrier);
    /* See if we can now obtain the lock. */
    ret = xactLockMgr->Lock(g_defaultPdbId, xid);
    ASSERT_TRUE(STORAGE_FUNC_SUCC(ret));
    xactLockMgr->Unlock(g_defaultPdbId, xid);
    instance->ThreadUnregisterAndExit();
}

/*
 * This test case checks the scenario where a transaction is re-connected with a different thread
 */
TEST_F(UTXactLockMgr, TestTransferLockDifferentThread_level0)
{
    Xid xid = INVALID_XID;
    pthread_barrier_t barrier;
    XactLockMgr *xactLockMgr = g_storageInstance->GetXactLockMgr();

    /* Here we test the expected scenario where t1 != t2 */
    pthread_barrier_init(&barrier, NULL, 2);
    std::thread thread = std::thread(TransferLockThrd, xid, &barrier, false);
    pthread_barrier_wait(&barrier);

    /* t1 has locked the xact lock. */
    RetStatus ret = xactLockMgr->TransferXactLockHolder(g_defaultPdbId, xid);
    ASSERT_TRUE(STORAGE_FUNC_SUCC(ret));
    pthread_barrier_wait(&barrier);

    /* Check the main lock table to see if I now hold the lock*/
    LockHashTable *lockTable = static_cast<LockHashTable *>(xactLockMgr->UTGetLockTable());
    LockTag tag;
    tag.SetTransactionLockTag(g_defaultPdbId, xid);
    LockRequest request(DSTORE_EXCLUSIVE_LOCK, thrd);
    ASSERT_TRUE(lockTable->IsHeldByRequester(&tag, &request));

    pthread_barrier_wait(&barrier);
    xactLockMgr->Unlock(g_defaultPdbId, xid);
    /* Verify the unlock worked by locking and unlocking. */
    ret = xactLockMgr->Lock(g_defaultPdbId, xid);
    ASSERT_TRUE(STORAGE_FUNC_SUCC(ret));
    xactLockMgr->Unlock(g_defaultPdbId, xid);
    pthread_barrier_wait(&barrier);
    /* Wait for thread to join. */
    thread.join();
    pthread_barrier_destroy(&barrier);
}

/*
 * This test case checks the scenario where the transaction lock doesn't have a holder
 */
TEST_F(UTXactLockMgr, TestTransferLockNoHolder_level0)
{
    Xid xid = INVALID_XID;
    pthread_barrier_t barrier;
    XactLockMgr *xactLockMgr = g_storageInstance->GetXactLockMgr();

    /* Test the case where we incorrectly try to transfer the lock holder. (No one is holding lock) */
    RetStatus ret = xactLockMgr->TransferXactLockHolder(g_defaultPdbId, xid);
    ASSERT_TRUE(STORAGE_FUNC_FAIL(ret));
    ASSERT_EQ(StorageGetErrorCode(), XACTLOCK_ERROR_TRANSFER_LOCK_NOT_HELD);
    ASSERT_TRUE(thrd->GetLockCtx()->GetLocalLock()->IsEmpty());
    ret = xactLockMgr->Lock(g_defaultPdbId, xid);
    ASSERT_TRUE(STORAGE_FUNC_SUCC(ret));
    xactLockMgr->Unlock(g_defaultPdbId, xid);
}

int MockGetLockWaitTimeoutXact()
{
    return 5000;
}

void UTXactLockMgr::RunWaitConflictLockThread(const LockTag &tag, const Xid &xid, pthread_barrier_t *b1,
    pthread_barrier_t *b2)
{
    UtInstance *instance = (UtInstance *)g_storageInstance;
    instance->ThreadSetupAndRegister();
    LockErrorInfo info = {0};
    static constexpr uint64 MICROSEC_PER_MILLISEC = 1000;
    RetStatus ret = g_storageInstance->GetXactLockMgr()->Lock(&tag, DSTORE_SHARE_LOCK, LOCK_WAIT, &info);
    ASSERT_EQ(ret, DSTORE_SUCC);
    g_storageInstance->GetXactLockMgr()->Unlock(&tag, DSTORE_SHARE_LOCK);

    pthread_barrier_wait(b1);
    uint64 start_wait = GetSystemTimeInMicrosecond();
    ret = g_storageInstance->GetXactLockMgr()->Lock(&tag, DSTORE_EXCLUSIVE_LOCK, LOCK_WAIT, &info);

    if (ret != DSTORE_SUCC) {
        uint64 current_time = GetSystemTimeInMicrosecond();
        ASSERT_TRUE(current_time >= start_wait + MockGetLockWaitTimeoutXact() * MICROSEC_PER_MILLISEC);
        ASSERT_TRUE(StorageGetErrorCode() == LOCK_ERROR_WAIT_TIMEOUT);
    }

    pthread_barrier_wait(b2);

    if (ret == DSTORE_SUCC) {
        g_storageInstance->GetXactLockMgr()->Unlock(&tag, DSTORE_EXCLUSIVE_LOCK);
    }
    instance->ThreadUnregisterAndExit();
}

/* Test two threads to see if they can both acquire non-mutual locks, but one cannot acquire the conflicting lock. */
TEST_F(UTXactLockMgr, TestXactLockMgrSupportLockWaitTimeout_level0)
{
    ASSERT_FALSE(g_storageInstance->GetXactLockMgr() == NULL);

    LockTag tag;
    tag.SetTableLockTag(0, 0);

    const int numThreads = 2;
    Xid xid = INVALID_XID;
    std::thread threads[numThreads];
    pthread_barrier_t b1;
    pthread_barrier_t b2;

    pthread_barrier_init(&b1, NULL, numThreads);
    pthread_barrier_init(&b2, NULL, numThreads);

    g_storageInstance->m_lockWaitTimeoutCallBack = MockGetLockWaitTimeoutXact;

    threads[0] = std::thread(RunWaitConflictLockThread, tag, xid, &b1, &b2);
    threads[1] = std::thread(RunWaitConflictLockThread, tag, xid, &b1, &b2);

    for (ThreadId i = 0; i < numThreads; i++) {
        threads[i].join();
    }
    pthread_barrier_destroy(&b1);
    pthread_barrier_destroy(&b2);
}

void UTXactLockMgr::RunXactLockCancelThread1(const Xid &xid, pthread_barrier_t *b1, pthread_barrier_t *b2)
{
    UtInstance *instance = (UtInstance *)g_storageInstance;
    instance->ThreadSetupAndRegister();
    ASSERT_FALSE(g_storageInstance->GetXactLockMgr() == NULL);

    RetStatus ret = g_storageInstance->GetXactLockMgr()->Lock(g_defaultPdbId, xid);
    EXPECT_EQ(ret, DSTORE_SUCC);

    /* thread1 has hold lock. */
    pthread_barrier_wait(b1);

    /* thread2 has released lock. */
    pthread_barrier_wait(b2);
    g_storageInstance->GetXactLockMgr()->Unlock(g_defaultPdbId, xid);

    instance->ThreadUnregisterAndExit();
}

void UTXactLockMgr::RunXactLockCancelThread2(const Xid &xid, pthread_barrier_t *b1, pthread_barrier_t *b2,
    ThreadContext **currThrd, uint64 *releaseTime)
{
    UtInstance *instance = (UtInstance *)g_storageInstance;
    instance->ThreadSetupAndRegister();
    ASSERT_FALSE(g_storageInstance->GetXactLockMgr() == NULL);

    /* thrd is used to send interrupt signal in main thread later. */
    *currThrd = thrd;

    /* after thread1 hold lock, lock thread2 */
    pthread_barrier_wait(b1);

    RetStatus ret = g_storageInstance->GetXactLockMgr()->Wait(g_defaultPdbId, xid);
    *releaseTime = GetSystemTimeInMicrosecond();
    EXPECT_EQ(ret, DSTORE_FAIL);
    EXPECT_EQ(StorageGetErrorCode(), LOCK_ERROR_WAIT_CANCELED);

    /* thread2 has released lock. */
    pthread_barrier_wait(b2);

    instance->ThreadUnregisterAndExit();
}
