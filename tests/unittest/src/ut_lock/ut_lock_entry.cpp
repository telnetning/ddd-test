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
#include "framework/dstore_thread.h"
#include "lock/dstore_lock_thrd_local.h"
#include "lock/dstore_lock_entry.h"
#include <mutex>

using namespace DSTORE;

class UTLockMgrEntry : public DSTORETEST {
protected:
    void SetUp() override
    {
        DSTORETEST::SetUp();
        UtInstance *instance = DstoreNew(m_ut_memory_context)UtInstance();
        instance->Install(&DSTORETEST::m_guc, m_ut_memory_context);
        instance->Startup(&DSTORETEST::m_guc);

        LockTag tag;
        tag.lockTagType = LOCKTAG_TABLE;
        m_entry.Initialize(&tag);
        m_freeList = DstoreNew(m_ut_memory_context) LockRequestFreeList(1024, m_ut_memory_context);
    }

    void TearDown() override
    {
        m_freeList->FreeAllMemory();
        delete m_freeList;
        m_freeList = nullptr;

        UtInstance *instance = (UtInstance *)g_storageInstance;
        instance->Shutdown();
        delete instance;
        DSTORETEST::TearDown();
    }
    /* Initialize and destroy the local lock's thread. */
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

    static void GrantAndWaitTestThrd(LockEntry *entry, LockRequestFreeList *freeList);
    static void WakeupTestThrd(LockEntry *entry, LockRequestFreeList *freeList, pthread_barrier_t *barrier);
    static void DeadlockPreventionTestThrd2(LockEntry *entry, LockRequestFreeList *freeList,
                                            pthread_barrier_t *barrier);
    static void DeadlockPreventionTestThrd3(LockEntry *entry, LockRequestFreeList *freeList,
                                            pthread_barrier_t *barrier, ThreadId *thread3Id);
    static void SkipListTestThread(LockRequestSkipList *skipList, LockRequestFreeList *freeList,
                                   std::mutex *listLock, uint32 *cnt);

    LockEntry m_entry;
    LockRequestFreeList *m_freeList;
};

/*
 * Thread function for thread2 in GrantAndWaitTest.
 * Since thread1 is holding DSTORE_EXCLUSIVE_LOCK, thread2 should wait.
 * After thread1 releases the lock, thread2 should get the lock.
 */
void UTLockMgrEntry::GrantAndWaitTestThrd(LockEntry *entry, LockRequestFreeList *freeList)
{
    ThreadInit();
    LockRequest request2(DSTORE_ACCESS_EXCLUSIVE_LOCK, thrd);
    LockErrorInfo info = {0};
    RetStatus ret = entry->EnqueueLockRequest(&request2, freeList, &info);
    EXPECT_FALSE(ret == DSTORE_SUCC);
    EXPECT_TRUE(StorageGetErrorCode() == LOCK_INFO_WAITING);
    EXPECT_FALSE(entry->IsNoHolderAndNoWaiter());
    EXPECT_FALSE(entry->grantedQueue.IsSkipListEmpty());
    EXPECT_TRUE(entry->lockEntryCore.m_grantedTotal == 1);
    EXPECT_TRUE(entry->lockEntryCore.m_grantMask == (1 << DSTORE_EXCLUSIVE_LOCK));
    EXPECT_FALSE(DListIsEmpty(&(entry->waitingQueue)));
    EXPECT_TRUE(entry->lockEntryCore.m_waitingTotal == 1);
    EXPECT_TRUE(entry->lockEntryCore.m_waitMask == (1 << DSTORE_ACCESS_EXCLUSIVE_LOCK));

    /* When release the lock request from wait queue, wait queue should be empty, and grant queue stays the same. */
    entry->DequeueLockRequest(&request2, freeList);
    EXPECT_FALSE(entry->IsNoHolderAndNoWaiter());
    EXPECT_FALSE(entry->grantedQueue.IsSkipListEmpty());
    EXPECT_TRUE(entry->lockEntryCore.m_grantedTotal == 1);
    EXPECT_TRUE(entry->lockEntryCore.m_grantMask == (1 << DSTORE_EXCLUSIVE_LOCK));
    EXPECT_TRUE(DListIsEmpty(&(entry->waitingQueue)));
    EXPECT_TRUE(entry->lockEntryCore.m_waitingTotal == 0);
    EXPECT_TRUE(entry->lockEntryCore.m_waitMask == 0);
    ThreadDestroy();
}

/*
 * Basic test for lock entry grant and wait status.
 */
TEST_F(UTLockMgrEntry, GrantAndWaitTest_level0)
{
    LockErrorInfo info = {0};

    /* thread1 asks for lock, grant queue should not be empty, while wait queue is empty. */
    LockRequest request1(DSTORE_EXCLUSIVE_LOCK, thrd);
    RetStatus ret = m_entry.EnqueueLockRequest(&request1, m_freeList, &info);
    EXPECT_TRUE(ret == DSTORE_SUCC);
    EXPECT_FALSE(m_entry.IsNoHolderAndNoWaiter());
    EXPECT_FALSE(m_entry.grantedQueue.IsSkipListEmpty());
    EXPECT_TRUE(m_entry.lockEntryCore.m_grantedTotal == 1);
    EXPECT_TRUE(m_entry.lockEntryCore.m_grantMask == (1 << DSTORE_EXCLUSIVE_LOCK));
    EXPECT_TRUE(DListIsEmpty(&m_entry.waitingQueue));
    EXPECT_TRUE(m_entry.lockEntryCore.m_waitingTotal == 0);

    /*
     * thread2 asks for lock, will be waiting, both grant queue and wait queue should not be empty.
     * Then thread2 will release the lock immediately.
     */
    std::thread thread2 = std::thread(GrantAndWaitTestThrd, &m_entry, m_freeList);
    thread2.join();

    /* When release the only lock request from grant queue, lock entry should be clear by now. */
    m_entry.DequeueLockRequest(&request1, m_freeList);
    EXPECT_TRUE(m_entry.IsNoHolderAndNoWaiter());
    EXPECT_TRUE(m_entry.grantedQueue.IsSkipListEmpty());
    EXPECT_TRUE(DListIsEmpty(&m_entry.waitingQueue));
    EXPECT_TRUE(m_entry.lockEntryCore.m_grantMask == 0);
    EXPECT_TRUE(m_entry.lockEntryCore.m_waitMask == 0);
}

/*
 * Thread function for thread2 in WakeupTest.
 * thread2 will be waiting for the lock since thread1 is holding it.
 * Once thread1 releases the lock, thread2 should be woken up.
 */
void UTLockMgrEntry::WakeupTestThrd(LockEntry *entry, LockRequestFreeList *freeList, pthread_barrier_t *barrier)
{
    ThreadInit();
    LockErrorInfo info = {0};
    LockRequest request2(DSTORE_ACCESS_EXCLUSIVE_LOCK, thrd);

    /* The actual setting isWaiting to true is in LockMgr::Lock(), since we are testing lock entry only, set it here.*/
    thrd->GetLockCtx()->isWaiting = true;
    RetStatus ret = entry->EnqueueLockRequest(&request2, freeList, &info);
    EXPECT_FALSE(ret == DSTORE_SUCC);
    EXPECT_TRUE(StorageGetErrorCode() == LOCK_INFO_WAITING);
    EXPECT_TRUE(thrd->GetLockCtx()->isWaiting);
    pthread_barrier_wait(barrier);
    pthread_barrier_wait(barrier);
    /* thread2 releases the lock. */
    entry->DequeueLockRequest(&request2, freeList);
    EXPECT_FALSE(thrd->GetLockCtx()->isWaiting);
    EXPECT_TRUE(entry->IsNoHolderAndNoWaiter());
    ThreadDestroy();
}

/*
 * Basic test for lock wakeup.
 */
TEST_F(UTLockMgrEntry, WakeupTest_level0)
{
    LockErrorInfo info = {0};

    /* thread1 asks for DSTORE_EXCLUSIVE_LOCK lock, and is granted. */
    LockRequest request1(DSTORE_EXCLUSIVE_LOCK, thrd);
    RetStatus ret = m_entry.EnqueueLockRequest(&request1, m_freeList, &info);
    EXPECT_TRUE(ret == DSTORE_SUCC);

    pthread_barrier_t barrier;
    pthread_barrier_init(&barrier, NULL, 2);

    /* thread2 asks for DSTORE_ACCESS_EXCLUSIVE_LOCK lock, and is waiting. */
    std::thread thread2 = std::thread(WakeupTestThrd, &m_entry, m_freeList, &barrier);
    pthread_barrier_wait(&barrier);

    /* Thread1 releases the lock, this should grant lock to Thread2 and wake it up. */
    m_entry.DequeueLockRequest(&request1, m_freeList);
    EXPECT_FALSE(thrd->GetLockCtx()->isWaiting);
    EXPECT_FALSE(m_entry.grantedQueue.IsSkipListEmpty());
    EXPECT_TRUE(m_entry.lockEntryCore.m_grantedTotal == 1);
    EXPECT_TRUE(m_entry.lockEntryCore.m_grantMask == (1 << DSTORE_ACCESS_EXCLUSIVE_LOCK));
    EXPECT_TRUE(DListIsEmpty(&m_entry.waitingQueue));
    EXPECT_TRUE(m_entry.lockEntryCore.m_waitingTotal == 0);

    /* Wait for thread2 to release the granted lock. */
    pthread_barrier_wait(&barrier);
    thread2.join();
    pthread_barrier_destroy(&barrier);
}

/*
 * For testing whether lock mode conflict pattern follows designed matrix.
 */
TEST_F(UTLockMgrEntry, LockConflictTest_level0)
{
    RetStatus ret;
    ThreadId id1 = 1;
    ThreadId id2 = 2;
    uint32 coreIndex1 = 1;
    uint32 coreIndex2 = 2;
    LockErrorInfo info = {0};

    /*
     * Pick one lock mode, and grant it in a clear lock entry.
     */
    for (int mode1 = DSTORE_ACCESS_SHARE_LOCK; mode1 < DSTORE_LOCK_MODE_MAX; mode1++) {
        LockRequest request1(static_cast<LockMode>(mode1), id1, coreIndex1, LockEnqueueMethod::HOLD_BUT_DONT_WAIT);
        ret = m_entry.EnqueueLockRequest(&request1, m_freeList, &info);
        ASSERT_TRUE(ret == DSTORE_SUCC);

        /*
         * Check if other lock modes conflict with this one.
         */
        for (int mode2 = DSTORE_ACCESS_SHARE_LOCK; mode2 < DSTORE_LOCK_MODE_MAX; mode2++) {
            LockRequest request2(static_cast<LockMode>(mode2), id2, coreIndex2, LockEnqueueMethod::HOLD_BUT_DONT_WAIT);
            ret = m_entry.EnqueueLockRequest(&request2, m_freeList, &info);
            if (HasConflictWithMode(static_cast<LockMode>(mode1), static_cast<LockMode>(mode2)) == false) {
                EXPECT_TRUE(ret == DSTORE_SUCC);
                m_entry.DequeueLockRequest(&request2, m_freeList);
            } else {
                EXPECT_FALSE(ret == DSTORE_SUCC);
                EXPECT_TRUE(StorageGetErrorCode() == LOCK_INFO_NOT_AVAIL);
            }
        }

        m_entry.DequeueLockRequest(&request1, m_freeList);
        EXPECT_TRUE(m_entry.IsNoHolderAndNoWaiter());
    }
}

/*
 * This test is meant to verify the correctness of the function HasConflictWithMode()
 * between any of the the two given LockModes. It tests it by comparing the result of
 * the function HasConflictWithMode() with the expected results for all possible
 * combinations of LockModes.
 */
TEST_F(UTLockMgrEntry, LockConflictMapTest_level0)
{
    int conflictResult[DSTORE_LOCK_MODE_MAX][DSTORE_LOCK_MODE_MAX] = {
        {1, 1, 1, 1, 1, 1, 1, 1, 1},
        {1, 1, 1, 1, 1, 1, 1, 1, 0},
        {1, 1, 1, 1, 1, 1, 1, 0, 0},
        {1, 1, 1, 1, 1, 0, 0, 0, 0},
        {1, 1, 1, 1, 0, 0, 0, 0, 0},
        {1, 1, 1, 0, 0, 1, 0, 0, 0},
        {1, 1, 1, 0, 0, 0, 0, 0, 0},
        {1, 1, 0, 0, 0, 0, 0, 0, 0},
        {1, 0, 0, 0, 0, 0, 0, 0, 0}
    };

    for (int i = DSTORE_NO_LOCK; i < DSTORE_LOCK_MODE_MAX; i ++) {
        for (int j = DSTORE_NO_LOCK; j < DSTORE_LOCK_MODE_MAX; j ++) {
            bool hasConflict = HasConflictWithMode(static_cast<LockMode>(i),
                static_cast<LockMode>(j));
            ASSERT_TRUE(conflictResult[i][j] != static_cast<int>(hasConflict));
        }
    }
}

/*
 * Lock requests from the same backend thread should not block each other.
 */
TEST_F(UTLockMgrEntry, SameRequesterThreadTest_level0)
{
    LockErrorInfo info = {0};

    LockRequest request1(DSTORE_EXCLUSIVE_LOCK, thrd);
    RetStatus ret = m_entry.EnqueueLockRequest(&request1, m_freeList, &info);
    EXPECT_TRUE(ret == DSTORE_SUCC);

    EXPECT_TRUE(m_entry.IsHeldByRequester(&request1));

    LockRequest request2(DSTORE_ACCESS_EXCLUSIVE_LOCK, thrd);
    ret = m_entry.EnqueueLockRequest(&request2, m_freeList, &info);
    EXPECT_TRUE(ret == DSTORE_SUCC);

    m_entry.DequeueLockRequest(&request1, m_freeList);
    m_entry.DequeueLockRequest(&request2, m_freeList);

    EXPECT_TRUE(m_entry.IsNoHolderAndNoWaiter());
}

/*
 * This function is used for DeadlockPreventionTest as thread2.
 * This function asks for DSTORE_SHARE_UPDATE_EXCLUSIVE_LOCK lock, and is granted.
 */
void UTLockMgrEntry::DeadlockPreventionTestThrd2(LockEntry *entry, LockRequestFreeList *freeList,
                                                 pthread_barrier_t *barrier)
{
    ThreadInit();
    LockErrorInfo info = {0};
    LockRequest request2(DSTORE_SHARE_UPDATE_EXCLUSIVE_LOCK, thrd);
    RetStatus ret = entry->EnqueueLockRequest(&request2, freeList, &info);
    EXPECT_TRUE(ret == DSTORE_SUCC);
    EXPECT_TRUE(entry->lockEntryCore.m_grantMask == ((1 << DSTORE_ROW_EXCLUSIVE_LOCK) |
        (1 << DSTORE_SHARE_UPDATE_EXCLUSIVE_LOCK)));
    pthread_barrier_wait(barrier);
    /* Wait for thread3 to enqueue lockRequest with DSTORE_SHARE_LOCK. */
    pthread_barrier_wait(barrier);
    /* Wait for main thread to enqueue lockRequest with DSTORE_SHARE_UPDATE_EXCLUSIVE_LOCK. */
    pthread_barrier_wait(barrier);
    /*
     * thread2 releases the lock, this should grant lock to thread1 and wake it up if
     * rearrangement happened during wait function of lock entry.
     */
    entry->DequeueLockRequest(&request2, freeList);
    EXPECT_TRUE(entry->lockEntryCore.m_grantedTotal == 2);
    EXPECT_TRUE(entry->lockEntryCore.m_grantMask == (( 1 << DSTORE_ROW_EXCLUSIVE_LOCK) |
        ( 1 << DSTORE_SHARE_UPDATE_EXCLUSIVE_LOCK)));
    EXPECT_TRUE(entry->lockEntryCore.m_waitingTotal == 1);
    EXPECT_TRUE(entry->lockEntryCore.m_waitMask == (( 1 << DSTORE_SHARE_LOCK)));
    pthread_barrier_wait(barrier);
    pthread_barrier_wait(barrier);
    ThreadDestroy();
}

/*
 * This function is used for DeadlockPreventionTest as thread3.
 * This function asks for DSTORE_SHARE_LOCK lock, and is waiting.
 */
void UTLockMgrEntry::DeadlockPreventionTestThrd3(LockEntry *entry, LockRequestFreeList *freeList,
                                                 pthread_barrier_t *barrier, ThreadId *thread3Id)
{
    ThreadInit();
    *thread3Id = thrd->GetThreadId();
    thrd->GetLockCtx()->isWaiting = true;
    LockErrorInfo info = {0};
    LockRequest request3(DSTORE_SHARE_LOCK, thrd);
    pthread_barrier_wait(barrier);
    RetStatus ret = entry->EnqueueLockRequest(&request3, freeList, &info);
    EXPECT_FALSE(ret == DSTORE_SUCC);
    EXPECT_TRUE(StorageGetErrorCode() == LOCK_INFO_WAITING);
    EXPECT_TRUE(thrd->GetLockCtx()->isWaiting);
    pthread_barrier_wait(barrier);
    /* Wait for main thread to enqueue lockRequest with DSTORE_SHARE_UPDATE_EXCLUSIVE_LOCK. */
    pthread_barrier_wait(barrier);
    /* Wait for thread2 to dequeue lockRequest with DSTORE_SHARE_UPDATE_EXCLUSIVE_LOCK. */
    pthread_barrier_wait(barrier);
    /* thread3 releases the lock. */
    entry->DequeueLockRequest(&request3, freeList);
    pthread_barrier_wait(barrier);
    ThreadDestroy();
}

/*
 * Verify deadlock prevention by creating four requests
 * to create scenarios where deadlock prevention will be
 * triggered. We check the order of the waiting queue
 * to verify the functionality of the deadlock prevention.
 */
TEST_F(UTLockMgrEntry, DeadlockPreventionTest_level0)
{
    LockErrorInfo info = {0};
    thrd->GetLockCtx()->isWaiting = true;

    /* thread1 asks for DSTORE_ROW_EXCLUSIVE_LOCK lock, and is granted. */
    LockRequest request1(DSTORE_ROW_EXCLUSIVE_LOCK, thrd);
    RetStatus ret = m_entry.EnqueueLockRequest(&request1, m_freeList, &info);
    EXPECT_TRUE(ret == DSTORE_SUCC);
    EXPECT_TRUE(m_entry.lockEntryCore.m_grantMask == (1 << DSTORE_ROW_EXCLUSIVE_LOCK));

    pthread_barrier_t barrier;
    pthread_barrier_init(&barrier, NULL, 3);

    /* thread2 asks for DSTORE_SHARE_UPDATE_EXCLUSIVE_LOCK lock, and is granted. */
    std::thread thread2 = std::thread(DeadlockPreventionTestThrd2, &m_entry, m_freeList, &barrier);

    /* thread3 asks for DSTORE_SHARE_LOCK lock, and will be waiting. */
    ThreadId thread3Id = INVALID_THREAD_ID;
    std::thread thread3 = std::thread(DeadlockPreventionTestThrd3, &m_entry, m_freeList, &barrier, &thread3Id);

    /*
     * Wait for thread2 to enqueue lockRequest with DSTORE_SHARE_UPDATE_EXCLUSIVE_LOCK
     * and thread3 to enqueue lockRequest with DSTORE_SHARE_LOCK.
     */
    pthread_barrier_wait(&barrier);
    pthread_barrier_wait(&barrier);
    /* thread1 asks for DSTORE_SHARE_UPDATE_EXCLUSIVE_LOCK lock, and will be waiting. */
    LockRequest request4(DSTORE_SHARE_UPDATE_EXCLUSIVE_LOCK, thrd);
    ret = m_entry.EnqueueLockRequest(&request4, m_freeList, &info);
    EXPECT_FALSE(ret == DSTORE_SUCC);
    EXPECT_TRUE(StorageGetErrorCode() == LOCK_INFO_WAITING);
    EXPECT_TRUE(thrd->GetLockCtx()->isWaiting);

    /*
     * We traverse the wait queue to make sure rearrangement happened.
     * With rearrangement, request4 of thread1 will be inserted before
     * request3 of thread3.
     */
    dlist_iter iter;
    int counter = 0;
    ThreadId threadIds[] = {thrd->GetThreadId(), thread3Id};
    dlist_foreach(iter, &m_entry.waitingQueue) {
        LockRequest *waiter = (LockRequest *)LockRequestInterface::GetLockRequestFromNode(iter.cur);
        EXPECT_TRUE(waiter->threadId == threadIds[counter]);
        counter++;
    }

    printf("waiting queue check is done\n");

    pthread_barrier_wait(&barrier);
    /* Wait for thread2 to dequeue lockRequest with DSTORE_SHARE_UPDATE_EXCLUSIVE_LOCK. */
    pthread_barrier_wait(&barrier);

    /* Wait for thread3 to dequeue lockRequest with DSTORE_SHARE_LOCK. */
    pthread_barrier_wait(&barrier);

    m_entry.DequeueLockRequest(&request1, m_freeList);
    thread2.join();
    thread3.join();
    m_entry.DequeueLockRequest(&request4, m_freeList);
    EXPECT_TRUE(m_entry.IsNoHolderAndNoWaiter());
    pthread_barrier_destroy(&barrier);
}

TEST_F(UTLockMgrEntry, SkipListTest_level0)
{
    LockRequestSkipList skipList;

    LockRequest request1(DSTORE_ACCESS_SHARE_LOCK, 1, 1);
    LockRequestInterface *insertRequest1 = m_freeList->PopACopyOf(&request1);
    RetStatus ret = skipList.InsertLockRequest(insertRequest1);
    EXPECT_TRUE(ret == DSTORE_SUCC);

    LockRequestInterface *searchRequest1 = skipList.SearchLockRequest(&request1);
    EXPECT_TRUE(searchRequest1 != nullptr);

    EXPECT_TRUE(skipList.GetLevelRequestCnt(0) == 1);
    LockRequestSkipList::ListIterator iter(&skipList, 0);
    for (LockRequestInterface *request = iter.GetNextRequest(); request != nullptr; request = iter.GetNextRequest()) {
        EXPECT_TRUE(request1.Compare(request) == 0);
    }

    LockRequestInterface *deleteRequest1 = skipList.RemoveLockRequest(&request1);
    EXPECT_TRUE(deleteRequest1 != nullptr);
    EXPECT_TRUE(deleteRequest1 == insertRequest1);
    EXPECT_TRUE(skipList.IsSkipListEmpty());
    m_freeList->Push(deleteRequest1);
}

static void DumpSkipList(LockRequestSkipList *skipList, std::mutex *listLock)
{
    listLock->lock();
    StringInfoData str;
    str.init();
    skipList->DumpSkipList(&str);
    printf("%s", str.data);
    DstorePfreeExt(str.data);
    listLock->unlock();
}

void UTLockMgrEntry::SkipListTestThread(LockRequestSkipList *skipList, LockRequestFreeList *freeList,
                                        std::mutex *listLock, uint32 *cnt)
{
    ThreadInit();
    static const int MAX_LOOP_TIME = 400;
    std::vector<uint32_t> requestRecord(MAX_LOOP_TIME);

    for (int i = 0; i < MAX_LOOP_TIME; i++) {
        listLock->lock();
        uint32 id = (*cnt)++;
        requestRecord[i] = id;

        LockRequest request1(DSTORE_ACCESS_SHARE_LOCK, id, id);
        LockRequestInterface *insertRequest1 = freeList->PopACopyOf(&request1);
        RetStatus ret = skipList->InsertLockRequest(insertRequest1);
        EXPECT_TRUE(ret == DSTORE_SUCC);

        listLock->unlock();
    }

    for (int i = 0; i < MAX_LOOP_TIME; i++) {
        listLock->lock();
        uint32 id = requestRecord[i];

        LockRequest request1(DSTORE_ACCESS_SHARE_LOCK, id, id);
        LockRequestInterface *searchRequest1 = skipList->SearchLockRequest(&request1);
        EXPECT_TRUE(searchRequest1 != nullptr);
        EXPECT_TRUE(searchRequest1->IsSameRequester(&request1));

        listLock->unlock();
    }

    for (int i = 0; i < MAX_LOOP_TIME; i++) {
        listLock->lock();
        uint32 id = requestRecord[i];

        LockRequest request1(DSTORE_ACCESS_SHARE_LOCK, id, id);
        LockRequestInterface *deleteRequest1 = skipList->RemoveLockRequest(&request1);
        EXPECT_TRUE(deleteRequest1 != nullptr);
        EXPECT_TRUE(deleteRequest1->IsSameRequester(&request1));
        freeList->Push(deleteRequest1);

        listLock->unlock();
    }

    ThreadDestroy();
}

TEST_F(UTLockMgrEntry, SkipListMultiThreadTest_level2)
{
    LockRequestSkipList skipList;
    std::mutex listLock;
    uint32 cnt = 0;

    static const int MAX_THREAD_CNT = 300;
    std::thread threads[MAX_THREAD_CNT];
    for (int i = 0; i < MAX_THREAD_CNT; i++) {
        threads[i] = std::thread(SkipListTestThread, &skipList, m_freeList, &listLock, &cnt);
    }

    for (int i = 0; i < MAX_THREAD_CNT; i++) {
        threads[i].join();
    }

    EXPECT_TRUE(skipList.IsSkipListEmpty());
}
