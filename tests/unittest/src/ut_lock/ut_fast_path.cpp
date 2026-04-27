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
#include "ut_lock/ut_fast_path.h"
#include <thread>
#include <mutex>
#include <time.h>

using namespace DSTORE;

class UTLockMgrFastPath : public DSTORETEST {
protected:
    void SetUp() override
    {
        DSTORETEST::SetUp();
        UtInstance *instance = DstoreNew(m_ut_memory_context)UtInstance();
        instance->Install(&DSTORETEST::m_guc, m_ut_memory_context);
        instance->Startup(&DSTORETEST::m_guc);
        uint32 hashTableSize = 16;
        uint32 partitionNum = 16;
        fastPath = DstoreNew(m_ut_memory_context) TableLockMgrForFastPathTest();
        ASSERT_TRUE(fastPath != NULL);
        RetStatus ret = fastPath->Initialize(hashTableSize, partitionNum);
        ASSERT_TRUE(ret == DSTORE_SUCC);
    }

    void TearDown() override
    {
        fastPath->Destroy();
        delete fastPath;
        fastPath = NULL;

        UtInstance *instance = (UtInstance *)g_storageInstance;
        instance->Shutdown();
        delete instance;
        DSTORETEST::TearDown();
        ThreadLocalLock::CheckStrongLocksInFastPathLeak();
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

    TableLockMgrForFastPathTest *fastPath;
};

/*
 * Fast path component test.
 * All threads can get weak locks from fast path directly, without access to main lock table.
 */
TEST_F(UTLockMgrFastPath, GrantAndReleaseByFastPathTest_level0)
{
    uint32 threadsTotal = 16;
    std::thread t[threadsTotal];
    for (int i = 0; i < threadsTotal; i++) {
        t[i] = std::thread([] {
            ThreadInit();

            ThreadLocalLock threadLocalLock;
            threadLocalLock.Initialize(thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LOCK));
            EXPECT_TRUE(threadLocalLock.IsEmpty());

            RetStatus ret;
            LockTag tag;
            tag.SetTableLockTag(0, 0);
            LockTagCache tagCache(&tag);
            bool alreadyHeld = false;
            /* No background thread should request for strong lock in this test case. */
            ASSERT_EQ(ThreadLocalLock::GetStrongLockCntInPartition(tagCache.hashCode), 0);

            /* Thread local locks are clean so attempts to grant via the fast path should succeed. */
            ret = threadLocalLock.TryGrantByFastPath(tagCache, DSTORE_ACCESS_SHARE_LOCK, TABLE_LOCK_MGR, alreadyHeld);
            EXPECT_EQ(ret, DSTORE_SUCC);
            EXPECT_FALSE(alreadyHeld);
            EXPECT_EQ(threadLocalLock.GetNumEntries(), 1);
            ret = threadLocalLock.TryGrantByFastPath(tagCache, DSTORE_ROW_SHARE_LOCK, TABLE_LOCK_MGR, alreadyHeld);
            EXPECT_EQ(ret, DSTORE_SUCC);
            EXPECT_FALSE(alreadyHeld);
            EXPECT_EQ(threadLocalLock.GetNumEntries(), 1);
            ret = threadLocalLock.TryGrantByFastPath(tagCache, DSTORE_ROW_EXCLUSIVE_LOCK, TABLE_LOCK_MGR, alreadyHeld);
            EXPECT_EQ(ret, DSTORE_SUCC);
            EXPECT_FALSE(alreadyHeld);
            EXPECT_EQ(threadLocalLock.GetNumEntries(), 1);

            /* Since the lock was granted via the fast path, the release should also succeed. */
            ret = threadLocalLock.TryReleaseByFastPath(tagCache, DSTORE_ACCESS_SHARE_LOCK);
            EXPECT_EQ(ret, DSTORE_SUCC);
            EXPECT_EQ(threadLocalLock.GetNumEntries(), 1);
            ret = threadLocalLock.TryReleaseByFastPath(tagCache, DSTORE_ROW_SHARE_LOCK);
            EXPECT_EQ(ret, DSTORE_SUCC);
            EXPECT_EQ(threadLocalLock.GetNumEntries(), 1);
            ret = threadLocalLock.TryReleaseByFastPath(tagCache, DSTORE_ROW_EXCLUSIVE_LOCK);
            EXPECT_EQ(ret, DSTORE_SUCC);
            EXPECT_EQ(threadLocalLock.GetNumEntries(), 0);
            EXPECT_TRUE(threadLocalLock.IsEmpty());

            threadLocalLock.Destroy();

            ThreadDestroy();
        });
    }

    for(int i = 0; i < threadsTotal; i++) {
        t[i].join();
    }
}

/*
 * Fast path component test.
 * Batch release interface should work when the weak lock is granted by fast path.
 */
TEST_F(UTLockMgrFastPath, GrantAndBatchReleaseByFastPathTest_level0)
{
    uint32 threadsTotal = 16;
    std::thread t[threadsTotal];
    for (int i = 0; i < threadsTotal; i++) {
        t[i] = std::thread([] {
            ThreadInit();

            ThreadLocalLock threadLocalLock;
            threadLocalLock.Initialize(thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LOCK));
            EXPECT_TRUE(threadLocalLock.IsEmpty());

            RetStatus ret;
            LockTag tag;
            tag.SetTableLockTag(0, 0);
            LockTagCache tagCache(&tag);
            bool alreadyHeld = false;
            ASSERT_EQ(ThreadLocalLock::GetStrongLockCntInPartition(tagCache.hashCode), 0);

            /* Thread local locks are clean so attempts to grant via the fast path should succeed. */
            ret = threadLocalLock.TryGrantByFastPath(tagCache, DSTORE_ACCESS_SHARE_LOCK, TABLE_LOCK_MGR, alreadyHeld);
            EXPECT_EQ(ret, DSTORE_SUCC);
            EXPECT_FALSE(alreadyHeld);
            EXPECT_EQ(threadLocalLock.GetNumEntries(), 1);
            ret = threadLocalLock.TryGrantByFastPath(tagCache, DSTORE_ROW_SHARE_LOCK, TABLE_LOCK_MGR, alreadyHeld);
            EXPECT_EQ(ret, DSTORE_SUCC);
            EXPECT_FALSE(alreadyHeld);
            EXPECT_EQ(threadLocalLock.GetNumEntries(), 1);
            ret = threadLocalLock.TryGrantByFastPath(tagCache, DSTORE_ROW_EXCLUSIVE_LOCK, TABLE_LOCK_MGR, alreadyHeld);
            EXPECT_EQ(ret, DSTORE_SUCC);
            EXPECT_FALSE(alreadyHeld);
            EXPECT_EQ(threadLocalLock.GetNumEntries(), 1);

            /* Since the lock was granted via the fast path, the release should also succeed. */
            bool unlockFinished = false;
            ret = threadLocalLock.BatchReleaseByFastPath(tagCache, DSTORE_ACCESS_SHARE_LOCK, 1, unlockFinished);
            EXPECT_EQ(ret, DSTORE_SUCC);
            EXPECT_EQ(threadLocalLock.GetNumEntries(), 1);
            EXPECT_TRUE(unlockFinished);
            unlockFinished = false;
            ret = threadLocalLock.BatchReleaseByFastPath(tagCache, DSTORE_ROW_SHARE_LOCK, 1, unlockFinished);
            EXPECT_EQ(ret, DSTORE_SUCC);
            EXPECT_EQ(threadLocalLock.GetNumEntries(), 1);
            EXPECT_TRUE(unlockFinished);
            unlockFinished = false;
            ret = threadLocalLock.BatchReleaseByFastPath(tagCache, DSTORE_ROW_EXCLUSIVE_LOCK, 1, unlockFinished);
            EXPECT_EQ(ret, DSTORE_SUCC);
            EXPECT_EQ(threadLocalLock.GetNumEntries(), 0);
            EXPECT_TRUE(threadLocalLock.IsEmpty());
            EXPECT_TRUE(unlockFinished);

            threadLocalLock.Destroy();

            ThreadDestroy();
        });
    }

    for(int i = 0; i < threadsTotal; i++) {
        t[i].join();
    }
}

/*
 * Fast path component test.
 * A thread can't get weak locks from fast path when there is a strong lock marked.
 */
TEST_F(UTLockMgrFastPath, StrongLockBlocksGrantWeakLockTest_level0)
{
    LockTag tag;
    tag.SetTableLockTag(0, 0);
    LockTagCache tagCache(&tag);
    ThreadLocalLock::MarkStrongLockInFastPath(tagCache.hashCode);

    uint32 threadsTotal = 16;
    std::thread t[threadsTotal];
    for (int i = 0; i < threadsTotal; i++) {
        t[i] = std::thread([] {
            ThreadInit();

            ThreadLocalLock threadLocalLock;
            threadLocalLock.Initialize(thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LOCK));
            EXPECT_TRUE(threadLocalLock.IsEmpty());

            RetStatus ret;
            LockTag tag;
            tag.SetTableLockTag(0, 0);
            LockTagCache tagCache(&tag);
            bool alreadyHeld = false;

            /* There is a strong lock, so attempts to grant lock via the fast path should fail. */
            ret = threadLocalLock.TryGrantByFastPath(tagCache, DSTORE_ACCESS_SHARE_LOCK, TABLE_LOCK_MGR, alreadyHeld);
            EXPECT_EQ(ret, DSTORE_FAIL);
            EXPECT_EQ(StorageGetErrorCode(), LOCK_INFO_NOT_AVAIL);
            EXPECT_FALSE(alreadyHeld);
            EXPECT_EQ(threadLocalLock.GetNumEntries(), 1);
            /* Keep thread local locks clean by recording the lock result as failure. */
            threadLocalLock.RecordLockResult(tagCache, DSTORE_ACCESS_SHARE_LOCK, DSTORE_FAIL);
            EXPECT_TRUE(threadLocalLock.IsEmpty());

            /* Same thing should happen for DSTORE_ROW_SHARE_LOCK. */
            ret = threadLocalLock.TryGrantByFastPath(tagCache, DSTORE_ROW_SHARE_LOCK, TABLE_LOCK_MGR, alreadyHeld);
            EXPECT_EQ(ret, DSTORE_FAIL);
            EXPECT_EQ(StorageGetErrorCode(), LOCK_INFO_NOT_AVAIL);
            EXPECT_FALSE(alreadyHeld);
            EXPECT_EQ(threadLocalLock.GetNumEntries(), 1);
            threadLocalLock.RecordLockResult(tagCache, DSTORE_ROW_SHARE_LOCK, DSTORE_FAIL);
            EXPECT_TRUE(threadLocalLock.IsEmpty());

            /* And also for DSTORE_ROW_EXCLUSIVE_LOCK. */
            ret = threadLocalLock.TryGrantByFastPath(tagCache, DSTORE_ROW_EXCLUSIVE_LOCK, TABLE_LOCK_MGR, alreadyHeld);
            EXPECT_EQ(ret, DSTORE_FAIL);
            EXPECT_EQ(StorageGetErrorCode(), LOCK_INFO_NOT_AVAIL);
            EXPECT_FALSE(alreadyHeld);
            EXPECT_EQ(threadLocalLock.GetNumEntries(), 1);
            threadLocalLock.RecordLockResult(tagCache, DSTORE_ROW_EXCLUSIVE_LOCK, DSTORE_FAIL);
            EXPECT_TRUE(threadLocalLock.IsEmpty());

            threadLocalLock.Destroy();

            ThreadDestroy();
        });
    }

    for(int i = 0; i < threadsTotal; i++) {
        t[i].join();
    }

    ThreadLocalLock::UnmarkStrongLockInFastPath(tagCache.hashCode);
}

/*
 * Fast path component test.
 * If a thread didn't get a weak lock from fast path, it should fail on release the same lock by fast path.
 */
TEST_F(UTLockMgrFastPath, StrongLockBlocksReleaseWeakLockTest_level0)
{
    LockTag tag;
    tag.SetTableLockTag(0, 0);
    LockTagCache tagCache(&tag);
    ThreadLocalLock::MarkStrongLockInFastPath(tagCache.hashCode);

    uint32 threadsTotal = 16;
    std::thread t[threadsTotal];
    for (int i = 0; i < threadsTotal; i++) {
        t[i] = std::thread([] {
            ThreadInit();

            ThreadLocalLock threadLocalLock;
            threadLocalLock.Initialize(thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LOCK));
            EXPECT_TRUE(threadLocalLock.IsEmpty());

            RetStatus ret;
            LockTag tag;
            tag.SetTableLockTag(0, 0);
            LockTagCache tagCache(&tag);
            bool alreadyHeld = false;

            /* There is a strong lock, so attempts to grant lock via the fast path should fail. */
            ret = threadLocalLock.TryGrantByFastPath(tagCache, DSTORE_ACCESS_SHARE_LOCK, TABLE_LOCK_MGR, alreadyHeld);
            EXPECT_EQ(ret, DSTORE_FAIL);
            EXPECT_EQ(StorageGetErrorCode(), LOCK_INFO_NOT_AVAIL);
            EXPECT_FALSE(alreadyHeld);
            EXPECT_EQ(threadLocalLock.GetNumEntries(), 1);
            /* Record the lock result as succ, it would leave a hold lock record in thread local lock. */
            threadLocalLock.RecordLockResult(tagCache, DSTORE_ACCESS_SHARE_LOCK, DSTORE_SUCC);
            EXPECT_FALSE(threadLocalLock.IsEmpty());
            EXPECT_EQ(threadLocalLock.GetNumEntries(), 1);
            /* Attempting to release via the fast path should fail because we didn't obtain the lock from the fast path. */
            ret = threadLocalLock.TryReleaseByFastPath(tagCache, DSTORE_ACCESS_SHARE_LOCK);
            EXPECT_EQ(ret, DSTORE_FAIL);
            EXPECT_EQ(StorageGetErrorCode(), LOCK_INFO_NOT_AVAIL);
            EXPECT_EQ(threadLocalLock.GetNumEntries(), 1);
            /* Remove the lock record to keep thread local locks clean. */
            ret = threadLocalLock.RemoveLockRecord(tagCache, DSTORE_ACCESS_SHARE_LOCK);
            EXPECT_EQ(ret, DSTORE_SUCC);
            EXPECT_TRUE(threadLocalLock.IsEmpty());

            /* Same thing should happen for DSTORE_ROW_SHARE_LOCK. */
            ret = threadLocalLock.TryGrantByFastPath(tagCache, DSTORE_ROW_SHARE_LOCK, TABLE_LOCK_MGR, alreadyHeld);
            EXPECT_EQ(ret, DSTORE_FAIL);
            EXPECT_EQ(StorageGetErrorCode(), LOCK_INFO_NOT_AVAIL);
            EXPECT_FALSE(alreadyHeld);
            EXPECT_EQ(threadLocalLock.GetNumEntries(), 1);
            threadLocalLock.RecordLockResult(tagCache, DSTORE_ROW_SHARE_LOCK, DSTORE_SUCC);
            EXPECT_FALSE(threadLocalLock.IsEmpty());
            EXPECT_EQ(threadLocalLock.GetNumEntries(), 1);
            ret = threadLocalLock.TryReleaseByFastPath(tagCache, DSTORE_ROW_SHARE_LOCK);
            EXPECT_EQ(ret, DSTORE_FAIL);
            EXPECT_EQ(StorageGetErrorCode(), LOCK_INFO_NOT_AVAIL);
            ret = threadLocalLock.RemoveLockRecord(tagCache, DSTORE_ROW_SHARE_LOCK);
            EXPECT_EQ(ret, DSTORE_SUCC);
            EXPECT_TRUE(threadLocalLock.IsEmpty());
            EXPECT_EQ(threadLocalLock.GetNumEntries(), 0);

            /* And also for DSTORE_ROW_EXCLUSIVE_LOCK. */
            ret = threadLocalLock.TryGrantByFastPath(tagCache, DSTORE_ROW_EXCLUSIVE_LOCK, TABLE_LOCK_MGR, alreadyHeld);
            EXPECT_EQ(ret, DSTORE_FAIL);
            EXPECT_EQ(StorageGetErrorCode(), LOCK_INFO_NOT_AVAIL);
            EXPECT_FALSE(alreadyHeld);
            EXPECT_EQ(threadLocalLock.GetNumEntries(), 1);
            threadLocalLock.RecordLockResult(tagCache, DSTORE_ROW_EXCLUSIVE_LOCK, DSTORE_SUCC);
            EXPECT_FALSE(threadLocalLock.IsEmpty());
            EXPECT_EQ(threadLocalLock.GetNumEntries(), 1);
            ret = threadLocalLock.TryReleaseByFastPath(tagCache, DSTORE_ROW_EXCLUSIVE_LOCK);
            EXPECT_EQ(ret, DSTORE_FAIL);
            EXPECT_EQ(StorageGetErrorCode(), LOCK_INFO_NOT_AVAIL);
            EXPECT_EQ(threadLocalLock.GetNumEntries(), 1);
            ret = threadLocalLock.RemoveLockRecord(tagCache, DSTORE_ROW_EXCLUSIVE_LOCK);
            EXPECT_EQ(ret, DSTORE_SUCC);
            EXPECT_TRUE(threadLocalLock.IsEmpty());

            threadLocalLock.Destroy();

            ThreadDestroy();
        });
    }

    for(int i = 0; i < threadsTotal; i++) {
        t[i].join();
    }

    ThreadLocalLock::UnmarkStrongLockInFastPath(tagCache.hashCode);
}

/*
 * Fast path component test.
 * If a thread didn't get a weak lock from fast path, it should fail on batch release the same lock by fast path.
 */
TEST_F(UTLockMgrFastPath, StrongLockBlocksBatchReleaseWeakLockTest_level0)
{
    LockTag tag;
    tag.SetTableLockTag(0, 0);
    LockTagCache tagCache(&tag);
    ThreadLocalLock::MarkStrongLockInFastPath(tagCache.hashCode);

    uint32 threadsTotal = 16;
    std::thread t[threadsTotal];
    for (int i = 0; i < threadsTotal; i++) {
        t[i] = std::thread([] {
            ThreadInit();

            ThreadLocalLock threadLocalLock;
            threadLocalLock.Initialize(thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LOCK));

            RetStatus ret;
            LockTag tag;
            tag.SetTableLockTag(0, 0);
            LockTagCache tagCache(&tag);
            bool alreadyHeld = false;

            for (uint8 j = 1; j <= static_cast<uint8>(DSTORE_ROW_EXCLUSIVE_LOCK); j++) {
                LockMode mode = static_cast<LockMode>(j);
                /* There is a strong lock, so attempts to grant lock via the fast path should fail. */
                ret = threadLocalLock.TryGrantByFastPath(tagCache, mode, TABLE_LOCK_MGR, alreadyHeld);
                EXPECT_EQ(ret, DSTORE_FAIL);
                EXPECT_EQ(StorageGetErrorCode(), LOCK_INFO_NOT_AVAIL);
                EXPECT_FALSE(alreadyHeld);
                EXPECT_EQ(threadLocalLock.GetNumEntries(), 1);
                /* Record the lock result as succ, it would leave a hold lock record in thread local lock. */
                threadLocalLock.RecordLockResult(tagCache, mode, DSTORE_SUCC);
                EXPECT_FALSE(threadLocalLock.IsEmpty());
                EXPECT_EQ(threadLocalLock.GetNumEntries(), 1);
                /* Attempting to release via the fast path should fail because we didn't obtain the lock from the fast path. */
                bool unlockFinished = false;
                ret = threadLocalLock.BatchReleaseByFastPath(tagCache, mode, 1, unlockFinished);
                EXPECT_EQ(ret, DSTORE_SUCC);
                EXPECT_EQ(unlockFinished, false);
                EXPECT_TRUE(threadLocalLock.IsEmpty());
            }

            threadLocalLock.Destroy();

            ThreadDestroy();
        });
    }

    for(int i = 0; i < threadsTotal; i++) {
        t[i].join();
    }

    ThreadLocalLock::UnmarkStrongLockInFastPath(tagCache.hashCode);
}

/*
 * Fast path integrate test in table lock mgr.
 * Weak locks should be transfered into main lock table when a conflicting strong lock is waiting to be granted.
 */
TEST_F(UTLockMgrFastPath, WeakLockTransferInFastPathTest_level0)
{
    LockTag tag;
    tag.SetTableLockTag(0, 0);
    LockTagCache tagCache(&tag);
    uint32 threadsTotal = 16;
    pthread_barrier_t barrier;
    pthread_barrier_init(&barrier, nullptr, threadsTotal + 1);
    TableLockMgrForFastPathTest *fakeLockMgr = fastPath;

    std::thread t[threadsTotal];
    for (int i = 0; i < threadsTotal; i++) {
        t[i] = std::thread([this, &barrier, &fakeLockMgr, tagCache] {
            ThreadInit();

            /* Obtain weak locks from fast path. */
            RetStatus ret = fakeLockMgr->TryAcquireWeakLockByFastPath(tagCache, DSTORE_ACCESS_SHARE_LOCK);
            EXPECT_EQ(ret, DSTORE_SUCC);
            ret = fakeLockMgr->TryAcquireWeakLockByFastPath(tagCache, DSTORE_ROW_SHARE_LOCK);
            EXPECT_EQ(ret, DSTORE_SUCC);
            ret = fakeLockMgr->TryAcquireWeakLockByFastPath(tagCache, DSTORE_ROW_EXCLUSIVE_LOCK);
            EXPECT_EQ(ret, DSTORE_SUCC);

            /* Wait until child threads to acquire the weak lock. */
            pthread_barrier_wait(&barrier);

            /* Wait until main thread to mark strong lock. */
            pthread_barrier_wait(&barrier);

            /* Release from fast path should fail, as the weak locks whould be transfered already. */
            ret = fakeLockMgr->TryReleaseWeakLockByFastPath(tagCache, DSTORE_ACCESS_SHARE_LOCK);
            EXPECT_EQ(ret, DSTORE_FAIL);
            EXPECT_EQ(StorageGetErrorCode(), LOCK_INFO_NOT_AVAIL);
            ret = fakeLockMgr->TryReleaseWeakLockByFastPath(tagCache, DSTORE_ROW_SHARE_LOCK);
            EXPECT_EQ(ret, DSTORE_FAIL);
            EXPECT_EQ(StorageGetErrorCode(), LOCK_INFO_NOT_AVAIL);
            ret = fakeLockMgr->TryReleaseWeakLockByFastPath(tagCache, DSTORE_ROW_EXCLUSIVE_LOCK);
            EXPECT_EQ(ret, DSTORE_FAIL);
            EXPECT_EQ(StorageGetErrorCode(), LOCK_INFO_NOT_AVAIL);

            ThreadDestroy();
        });
    }

    /* Wait until child threads to acquire the weak lock. */
    pthread_barrier_wait(&barrier);
    EXPECT_EQ(fakeLockMgr->GetTransferCount(), 0);

    /* Try mark strong lock, it's supposed to trigger weak lock transfer. */
    RetStatus ret = fakeLockMgr->TryMarkStrongLockByFastPath(tagCache);
    EXPECT_EQ(ret, DSTORE_SUCC);

    /* Notify child threads that marking strong lock has finished. */
    pthread_barrier_wait(&barrier);

    /* Wait until child threads finished testing. */
    for(int i = 0; i < threadsTotal; i++) {
        t[i].join();
    }

    /* Check transfer count. */
    EXPECT_EQ(fakeLockMgr->GetTransferCount(), threadsTotal * 3);
    fakeLockMgr->UnmarkStrongLockByFastPath(tagCache.hashCode);
}

/*
 * Fast path parameter test in table lock mgr.
 * This is a test case to show what is expected to happen if wrong arguments are passed to the fast path interface.
 * Since fast path interfaces are internal functions, assertions and panics should be enough,
 * thus this test case can only run manually.
 */
TEST_F(UTLockMgrFastPath, DISABLED_FastPathInvalidParameterTest_level2)
{
    LockTag tag;
    tag.SetTableLockTag(0, 0);
    LockTagCache tagCache(&tag);

    RetStatus ret;
    bool isAlreadyHeld = false;
    ThreadLocalLock *localLock = thrd->GetLockCtx()->GetLocalLock();

    /* DSTORE_NO_LOCK should never happen in lock mgr. */
    LockMode invalidMode1 = DSTORE_NO_LOCK;
    ret = localLock->TryGrantByFastPath(tagCache, invalidMode1, TABLE_LOCK_MGR, isAlreadyHeld);
    EXPECT_EQ(ret, DSTORE_FAIL);
    
    /* Any lock mode > 3 should not go through fast path, we only pick one for example. */
    LockMode invalidMode2 = DSTORE_SHARE_UPDATE_EXCLUSIVE_LOCK;
    ret = localLock->TryGrantByFastPath(tagCache, invalidMode2, TABLE_LOCK_MGR, isAlreadyHeld);
    EXPECT_EQ(ret, DSTORE_FAIL);

    /* Invalid lock tag(not initialized) should not be passed into local lock. */
    LockTag invalidTag;
    LockTagCache invalidTagCache(&invalidTag);
    LockMode mode = DSTORE_ACCESS_SHARE_LOCK;
    ret = localLock->TryGrantByFastPath(invalidTagCache, mode, TABLE_LOCK_MGR, isAlreadyHeld);
    EXPECT_EQ(ret, DSTORE_FAIL);

    /* Release a lock that we don't own would panic. */
    ret = localLock->TryReleaseByFastPath(tagCache, mode);
    EXPECT_EQ(ret, DSTORE_FAIL);

    /* Batch release a lock that we don't own for 0 time would panic, it should not happen in code. */
    bool unlockFinished = false;
    ret = localLock->BatchReleaseByFastPath(tagCache, mode, 0, unlockFinished);
    EXPECT_EQ(ret, DSTORE_FAIL);
}
