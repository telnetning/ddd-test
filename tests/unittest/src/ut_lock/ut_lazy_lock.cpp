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
#include "lock/dstore_lock_interface.h"
#include "transaction/dstore_transaction_interface.h"
#include "transaction/dstore_resowner.h"
#include "errorcode/dstore_transaction_error_code.h"
#include "diagnose/dstore_lock_mgr_diagnose.h"
#include "transaction/dstore_transaction.h"
#include "tablespace/dstore_tablespace.h"
#include <thread>
#include <mutex>
#include <stdlib.h>

using namespace DSTORE;

class UTLazyLock : public DSTORETEST {
protected:
    void SetUp() override
    {
        DSTORETEST::SetUp();
        UtInstance *instance = DstoreNew(m_ut_memory_context)UtInstance();

        DSTORETEST::m_guc.enableLazyLock = true;
        instance->Install(&DSTORETEST::m_guc, m_ut_memory_context);
        instance->Startup(&DSTORETEST::m_guc);

        /* Alloc and add file for undo tablespace firstly, prevent lazy hints from being cleared during lock upgrade. */
        TablespaceMgr *tablespaceMgr = g_storageInstance->GetPdb(g_defaultPdbId)->GetTablespaceMgr();
        TableSpace *m_undoTbs = (TableSpace *)tablespaceMgr->OpenTablespace(
            static_cast<TablespaceId>(TBS_ID::UNDO_TABLE_SPACE_ID), DSTORE::DSTORE_ACCESS_EXCLUSIVE_LOCK);
        FileId fileId;
        (void)(m_undoTbs)->AllocAndAddDataFile(g_defaultPdbId, &fileId, EXTENT_SIZE_ARRAY[0], false);
        tablespaceMgr->CloseTablespace((TableSpace *)m_undoTbs, DSTORE::DSTORE_ACCESS_EXCLUSIVE_LOCK);
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
};

static uint32 GetNonLazyLockCnt(Oid dbId, Oid relId, LockMode mode)
{
    LockTag lockTag;
    lockTag.SetTableLockTag(dbId, relId);
    uint32 granted[DSTORE_LOCK_MODE_MAX] = {0};
    ThreadLocalLock *localLock = thrd->GetLockCtx()->GetLocalLock();
    localLock->GetHoldLockCnt(&lockTag, granted, DSTORE_LOCK_MODE_MAX);
    return granted[mode];
}

static uint32 GetLazyLockPartId(Oid dbId, Oid relId)
{
    LockTag tag;
    tag.SetTableLockTag(dbId, relId);
    LockTagCache tagCache(&tag);
    return (tagCache.hashCode % LazyLockHint::LAZY_LOCK_HINT_PART_CNT);
}

static uint32 FindAnotherTableOidHasSamePartId(Oid dbId, Oid relId1)
{
    LockTag tag1;
    tag1.SetTableLockTag(dbId, relId1);
    LockTagCache tagCache1(&tag1);
    uint32 partId1 = tagCache1.hashCode % LazyLockHint::LAZY_LOCK_HINT_PART_CNT;

    for (uint32 relId2 = relId1 + 1; relId2 < 10000; relId2++) {
        LockTag tag2;
        tag2.SetTableLockTag(dbId, relId2);
        LockTagCache tagCache2(&tag2);
        uint32 partId2 = tagCache2.hashCode % LazyLockHint::LAZY_LOCK_HINT_PART_CNT;
        if (partId2 == partId1) {
            return relId2;
        }
    }

    StorageAssert(0);
    return 0;
}

/**
 * When there is no strong lock, then all weak lock acquisitions should return success through lazy locks.
 */
TEST_F(UTLazyLock, WeakLockTest_level0)
{
    ASSERT_FALSE(g_storageInstance->GetTableLockMgr() == NULL);

    LockInterface::TableLockContext context;
    context.dbId = 0;
    context.relId = 0;
    context.isPartition = false;
    context.isSessionLock = false;
    context.mode = DSTORE_ROW_EXCLUSIVE_LOCK;
    context.dontWait = LOCK_WAIT;

    /* Acquire a weak lock. */
    RetStatus res = LockInterface::LockTable(&context);
    EXPECT_TRUE(res == DSTORE_SUCC);
    EXPECT_TRUE(context.result == LockInterface::LockAcquireResult::LOCKACQUIRE_OK);

    /* Confirm that lock is acquired by lazy lock. */
    LazyLockHint *lazyLockHint = thrd->GetLockCtx()->GetLazyLockHint(GetLazyLockPartId(0, 0));
    EXPECT_TRUE(GetNonLazyLockCnt(0, 0, DSTORE_ROW_EXCLUSIVE_LOCK) == 0);
    EXPECT_TRUE(lazyLockHint->GetLazyLockCnt() == 1);
    EXPECT_TRUE(lazyLockHint->IsLazyLockEnabled());

    /* Release it. */
    LockInterface::UnlockTable(&context);

    /* Confirm that lock is released from lazy lock. */
    EXPECT_TRUE(GetNonLazyLockCnt(0, 0, DSTORE_ROW_EXCLUSIVE_LOCK) == 0);
    EXPECT_TRUE(lazyLockHint->GetLazyLockCnt() == 0);
    EXPECT_TRUE(lazyLockHint->IsLazyLockEnabled());
}

/**
 * Weak locks acquired through lazy locks should be able to be released when the transaction commits.
 */
TEST_F(UTLazyLock, WeakLockWithTransactionTest_level0)
{
    ASSERT_FALSE(g_storageInstance->GetTableLockMgr() == NULL);

    LockInterface::TableLockContext context;
    context.dbId = 0;
    context.relId = 0;
    context.isPartition = false;
    context.isSessionLock = false;
    context.mode = DSTORE_ROW_EXCLUSIVE_LOCK;
    context.dontWait = LOCK_WAIT;

    /* Start transaction. */
    TransactionInterface::StartTrxCommand();

    /* Acquire a weak lock. */
    RetStatus res = LockInterface::LockTable(&context);
    EXPECT_TRUE(res == DSTORE_SUCC);
    EXPECT_TRUE(context.result == LockInterface::LockAcquireResult::LOCKACQUIRE_OK);

    /* Confirm that lock is acquired by lazy lock. */
    LazyLockHint *lazyLockHint = thrd->GetLockCtx()->GetLazyLockHint(GetLazyLockPartId(0, 0));
    EXPECT_TRUE(GetNonLazyLockCnt(0, 0, DSTORE_ROW_EXCLUSIVE_LOCK) == 0);
    EXPECT_TRUE(lazyLockHint->GetLazyLockCnt() == 1);
    EXPECT_TRUE(lazyLockHint->IsLazyLockEnabled());

    /* Commit transaction. */
    TransactionInterface::CommitTrxCommand();

    /* Confirm that lock is released from lazy lock. */
    EXPECT_TRUE(GetNonLazyLockCnt(0, 0, DSTORE_ROW_EXCLUSIVE_LOCK) == 0);
    EXPECT_TRUE(lazyLockHint->GetLazyLockCnt() == 0);
    EXPECT_TRUE(lazyLockHint->IsLazyLockEnabled());
}

/**
 * Lock result in lock interface should be the same if lazy lock is enabled.
 */
TEST_F(UTLazyLock, LockInterfaceResultTest_level0)
{
    ASSERT_FALSE(g_storageInstance->GetTableLockMgr() == NULL);

    LockInterface::TableLockContext context;
    context.dbId = 0;
    context.relId = 0;
    context.isPartition = false;
    context.isSessionLock = false;
    context.mode = DSTORE_ROW_EXCLUSIVE_LOCK;
    context.dontWait = LOCK_WAIT;

    /* Acquire a weak lock. */
    RetStatus res = LockInterface::LockTable(&context);
    EXPECT_TRUE(res == DSTORE_SUCC);
    EXPECT_TRUE(context.result == LockInterface::LockAcquireResult::LOCKACQUIRE_OK);

    /* Confirm that lock is acquired by lazy lock. */
    LazyLockHint *lazyLockHint = thrd->GetLockCtx()->GetLazyLockHint(GetLazyLockPartId(0, 0));
    EXPECT_TRUE(GetNonLazyLockCnt(0, 0, DSTORE_ROW_EXCLUSIVE_LOCK) == 0);
    EXPECT_TRUE(lazyLockHint->GetLazyLockCnt() == 1);
    EXPECT_TRUE(lazyLockHint->IsLazyLockEnabled());

    /* Acquire the weak lock again, result should be already held. */
    res = LockInterface::LockTable(&context);
    EXPECT_TRUE(res == DSTORE_SUCC);
    EXPECT_TRUE(context.result == LockInterface::LockAcquireResult::LOCKACQUIRE_ALREADY_HELD);

    /* Confirm that second lock is also acquired by lazy lock. */
    EXPECT_TRUE(GetNonLazyLockCnt(0, 0, DSTORE_ROW_EXCLUSIVE_LOCK) == 0);
    EXPECT_TRUE(lazyLockHint->GetLazyLockCnt() == 2);
    EXPECT_TRUE(lazyLockHint->IsLazyLockEnabled());

    /* Release locks, and confirm that lock is released from lazy lock. */
    LockInterface::UnlockTable(&context);
    EXPECT_TRUE(GetNonLazyLockCnt(0, 0, DSTORE_ROW_EXCLUSIVE_LOCK) == 0);
    EXPECT_TRUE(lazyLockHint->GetLazyLockCnt() == 1);
    EXPECT_TRUE(lazyLockHint->IsLazyLockEnabled());
    LockInterface::UnlockTable(&context);
    EXPECT_TRUE(GetNonLazyLockCnt(0, 0, DSTORE_ROW_EXCLUSIVE_LOCK) == 0);
    EXPECT_TRUE(lazyLockHint->GetLazyLockCnt() == 0);
    EXPECT_TRUE(lazyLockHint->IsLazyLockEnabled());
}

/**
 * Acquire weak lazy locks on some threads, then try to acquire one strong lock,
 * the weak locks should be transfered into main lock table.
 */
TEST_F(UTLazyLock, StrongLockTest_level0)
{
    ASSERT_FALSE(g_storageInstance->GetTableLockMgr() == NULL);
    constexpr int numThreads = 2;
    pthread_barrier_t barrier;
    pthread_barrier_init(&barrier, nullptr, numThreads);

    std::thread weakLockThread = std::thread([&barrier] {
        ThreadInit();
        LockInterface::TableLockContext context;
        context.dbId = 0;
        context.relId = 0;
        context.isPartition = false;
        context.isSessionLock = false;
        context.mode = DSTORE_ROW_EXCLUSIVE_LOCK;
        context.dontWait = LOCK_WAIT;

        /* Get a weak lock from lazy lock first. */
        RetStatus res = LockInterface::LockTable(&context);
        EXPECT_TRUE(res == DSTORE_SUCC);
        EXPECT_TRUE(context.result == LockInterface::LockAcquireResult::LOCKACQUIRE_OK);

        /* Confirm that lock is acquired by lazy lock. */
        LazyLockHint *lazyLockHint = thrd->GetLockCtx()->GetLazyLockHint(GetLazyLockPartId(0, 0));
        EXPECT_TRUE(GetNonLazyLockCnt(0, 0, DSTORE_ROW_EXCLUSIVE_LOCK) == 0);
        EXPECT_TRUE(lazyLockHint->GetLazyLockCnt() == 1);
        EXPECT_TRUE(lazyLockHint->IsLazyLockEnabled());

        /* Finish getting weak lock. */
        pthread_barrier_wait(&barrier);

        /* Get the weak lock again to trigger lazy lock transfer, make sure it happens after lazy lock is disabled. */
        sleep(2);
        ASSERT_FALSE(lazyLockHint->IsLazyLockEnabled());
        res = LockInterface::LockTable(&context);
        EXPECT_TRUE(res == DSTORE_SUCC);
        EXPECT_TRUE(context.result == LockInterface::LockAcquireResult::LOCKACQUIRE_ALREADY_HELD);
        EXPECT_TRUE(lazyLockHint->GetLazyLockCnt() == 0);
        EXPECT_FALSE(lazyLockHint->IsLazyLockEnabled());
        EXPECT_EQ(GetNonLazyLockCnt(0, 0, DSTORE_ROW_EXCLUSIVE_LOCK), 2);

        /* Release weak locks. */
        LockInterface::UnlockTable(&context);
        EXPECT_TRUE(lazyLockHint->GetLazyLockCnt() == 0);
        EXPECT_FALSE(lazyLockHint->IsLazyLockEnabled());
        EXPECT_EQ(GetNonLazyLockCnt(0, 0, DSTORE_ROW_EXCLUSIVE_LOCK), 1);
        LockInterface::UnlockTable(&context);
        EXPECT_TRUE(lazyLockHint->GetLazyLockCnt() == 0);
        EXPECT_EQ(GetNonLazyLockCnt(0, 0, DSTORE_ROW_EXCLUSIVE_LOCK), 0);

        /* Only after releasing the weak lock can other thread obtain the strong lock. */
        pthread_barrier_wait(&barrier);
        EXPECT_TRUE(lazyLockHint->IsLazyLockEnabled());

        ThreadDestroy();
    });

    LockInterface::TableLockContext context;
    context.dbId = 0;
    context.relId = 0;
    context.isPartition = false;
    context.isSessionLock = false;
    context.mode = DSTORE_ACCESS_EXCLUSIVE_LOCK;
    context.dontWait = LOCK_WAIT;

    /* Wait for the other thread to get weak lock. */
    pthread_barrier_wait(&barrier);

    /* Get a strong lock, it should disable lazy lock on all threads. */
    RetStatus res = LockInterface::LockTable(&context);
    EXPECT_TRUE(res == DSTORE_SUCC);
    EXPECT_TRUE(context.result == LockInterface::LockAcquireResult::LOCKACQUIRE_OK);
    LazyLockHint *lazyLockHint = thrd->GetLockCtx()->GetLazyLockHint(GetLazyLockPartId(0, 0));
    EXPECT_TRUE(lazyLockHint->GetLazyLockCnt() == 0);
    EXPECT_FALSE(lazyLockHint->IsLazyLockEnabled());
    EXPECT_EQ(GetNonLazyLockCnt(0, 0, DSTORE_ACCESS_EXCLUSIVE_LOCK), 1);

    /* Release strong lock. */
    LockInterface::UnlockTable(&context);
    EXPECT_TRUE(lazyLockHint->GetLazyLockCnt() == 0);
    EXPECT_TRUE(lazyLockHint->IsLazyLockEnabled());
    EXPECT_EQ(GetNonLazyLockCnt(0, 0, DSTORE_ACCESS_EXCLUSIVE_LOCK), 0);

    /* Finish checking strong lock. */
    pthread_barrier_wait(&barrier);

    weakLockThread.join();
}

/**
 * Acquire weak lazy lock on one thread, then try to acquire one strong lock on the other thread,
 * the strong lock should be waiting for weak lock to release.
 * After the weak lock is released, strong lock thread is waked up and continue to get the lock.
 */
TEST_F(UTLazyLock, WeakLockWakeupStrongLockTest_level0)
{
    ASSERT_FALSE(g_storageInstance->GetTableLockMgr() == NULL);

    LockInterface::TableLockContext context;
    context.dbId = 0;
    context.relId = 0;
    context.isPartition = false;
    context.isSessionLock = false;
    context.mode = DSTORE_ROW_EXCLUSIVE_LOCK;
    context.dontWait = LOCK_WAIT;

    /* Acquire a weak lock. */
    RetStatus res = LockInterface::LockTable(&context);
    EXPECT_TRUE(res == DSTORE_SUCC);
    EXPECT_TRUE(context.result == LockInterface::LockAcquireResult::LOCKACQUIRE_OK);

    /* Confirm that lock is acquired by lazy lock. */
    LazyLockHint *lazyLockHint = thrd->GetLockCtx()->GetLazyLockHint(GetLazyLockPartId(0, 0));
    EXPECT_TRUE(GetNonLazyLockCnt(0, 0, DSTORE_ROW_EXCLUSIVE_LOCK) == 0);
    EXPECT_TRUE(lazyLockHint->GetLazyLockCnt() == 1);
    EXPECT_TRUE(lazyLockHint->IsLazyLockEnabled());

    /* Start a new thread to wait for strong lock. */
    constexpr int numThreads = 2;
    pthread_barrier_t barrier;
    pthread_barrier_init(&barrier, nullptr, numThreads);

    std::thread strongLockThread = std::thread([&barrier] {
        ThreadInit();
        LockInterface::TableLockContext context;
        context.dbId = 0;
        context.relId = 0;
        context.isPartition = false;
        context.isSessionLock = false;
        context.mode = DSTORE_EXCLUSIVE_LOCK;
        context.dontWait = LOCK_WAIT;

        /* Make sure the 2 threads all reach this point. */
        pthread_barrier_wait(&barrier);

        /* Wait for strong lock acquisition. */
        RetStatus res = LockInterface::LockTable(&context);
        EXPECT_TRUE(res == DSTORE_SUCC);
        EXPECT_TRUE(context.result == LockInterface::LockAcquireResult::LOCKACQUIRE_OK);

        /* Release strong lock. */
        LockInterface::UnlockTable(&context);
        LazyLockHint *lazyLockHint = thrd->GetLockCtx()->GetLazyLockHint(GetLazyLockPartId(0, 0));
        EXPECT_TRUE(lazyLockHint->GetLazyLockCnt() == 0);
        EXPECT_TRUE(lazyLockHint->IsLazyLockEnabled());
        EXPECT_EQ(GetNonLazyLockCnt(0, 0, DSTORE_ROW_EXCLUSIVE_LOCK), 0);

        ThreadDestroy();
    });

    /* Make sure the 2 threads all reach this point, after sleep the other thread must start waiting. */
    pthread_barrier_wait(&barrier);
    sleep(2);

    /* Release weak lock. */
    LockInterface::UnlockTable(&context);
    EXPECT_TRUE(lazyLockHint->GetLazyLockCnt() == 0);

    strongLockThread.join();
}


/**
 * Acquire 2 strong locks, after release one of them, the weak locks should still return fail from lazy lock.
 */
TEST_F(UTLazyLock, MultiStrongLockTest_level0)
{
    ASSERT_FALSE(g_storageInstance->GetTableLockMgr() == NULL);
    constexpr int numThreads = 2;
    pthread_barrier_t barrier;
    pthread_barrier_init(&barrier, nullptr, numThreads);

    std::thread weakLockThread = std::thread([&barrier] {
        ThreadInit();
        LockInterface::TableLockContext context;
        context.dbId = 0;
        context.relId = FindAnotherTableOidHasSamePartId(0, 0);
        context.isPartition = false;
        context.isSessionLock = false;
        context.mode = DSTORE_ROW_EXCLUSIVE_LOCK;
        context.dontWait = LOCK_WAIT;

        /* Wait until strong lock is acquired by the other thread. */
        pthread_barrier_wait(&barrier);
        uint32 partId = GetLazyLockPartId(0, context.relId);
        ASSERT_EQ(partId, GetLazyLockPartId(0, 0));
        LazyLockHint *lazyLockHint = thrd->GetLockCtx()->GetLazyLockHint(partId);
        ASSERT_FALSE(lazyLockHint->IsLazyLockEnabled());

        /* Get a different weak lock, it should not go through lazy lock. */
        RetStatus res = LockInterface::LockTable(&context);
        EXPECT_TRUE(res == DSTORE_SUCC);
        EXPECT_TRUE(context.result == LockInterface::LockAcquireResult::LOCKACQUIRE_OK);
        EXPECT_EQ(GetNonLazyLockCnt(0, context.relId, DSTORE_ROW_EXCLUSIVE_LOCK), 1);
        EXPECT_TRUE(lazyLockHint->GetLazyLockCnt() == 0);
        EXPECT_FALSE(lazyLockHint->IsLazyLockEnabled());

        /* Release it. */
        LockInterface::UnlockTable(&context);
        EXPECT_TRUE(lazyLockHint->GetLazyLockCnt() == 0);
        EXPECT_EQ(GetNonLazyLockCnt(0, context.relId, DSTORE_ROW_EXCLUSIVE_LOCK), 0);
        EXPECT_TRUE(lazyLockHint->GetLazyLockCnt() == 0);
        EXPECT_FALSE(lazyLockHint->IsLazyLockEnabled());

        /* Notify strong lock thread to release strong lock. */
        pthread_barrier_wait(&barrier);
        pthread_barrier_wait(&barrier);
        EXPECT_TRUE(lazyLockHint->IsLazyLockEnabled());

        /* Now we can get weak locks by lazy lock. */
        res = LockInterface::LockTable(&context);
        EXPECT_TRUE(res == DSTORE_SUCC);
        EXPECT_TRUE(context.result == LockInterface::LockAcquireResult::LOCKACQUIRE_OK);
        EXPECT_EQ(GetNonLazyLockCnt(0, context.relId, DSTORE_ROW_EXCLUSIVE_LOCK), 0);
        EXPECT_TRUE(lazyLockHint->GetLazyLockCnt() == 1);
        EXPECT_TRUE(lazyLockHint->IsLazyLockEnabled());

        /* Release it. */
        LockInterface::UnlockTable(&context);
        EXPECT_TRUE(lazyLockHint->GetLazyLockCnt() == 0);
        EXPECT_EQ(GetNonLazyLockCnt(0, context.relId, DSTORE_ROW_EXCLUSIVE_LOCK), 0);
        EXPECT_TRUE(lazyLockHint->GetLazyLockCnt() == 0);
        EXPECT_TRUE(lazyLockHint->IsLazyLockEnabled());

        ThreadDestroy();
    });

    LockInterface::TableLockContext context;
    context.dbId = 0;
    context.relId = 0;
    context.isPartition = false;
    context.isSessionLock = false;
    context.mode = DSTORE_ACCESS_EXCLUSIVE_LOCK;
    context.dontWait = LOCK_WAIT;

    uint32 partId = GetLazyLockPartId(0, 0);
    LazyLockHint *lazyLockHint = thrd->GetLockCtx()->GetLazyLockHint(partId);
    EXPECT_TRUE(lazyLockHint->IsLazyLockEnabled());

    /* Get a strong lock, it should disable lazy lock on all threads. */
    RetStatus res = LockInterface::LockTable(&context);
    EXPECT_TRUE(res == DSTORE_SUCC);
    EXPECT_TRUE(context.result == LockInterface::LockAcquireResult::LOCKACQUIRE_OK);
    EXPECT_TRUE(lazyLockHint->GetLazyLockCnt() == 0);
    EXPECT_FALSE(lazyLockHint->IsLazyLockEnabled());
    EXPECT_EQ(GetNonLazyLockCnt(0, 0, DSTORE_ACCESS_EXCLUSIVE_LOCK), 1);

    /* Get another strong lock. */
    res = LockInterface::LockTable(&context);
    EXPECT_TRUE(res == DSTORE_SUCC);
    EXPECT_TRUE(context.result == LockInterface::LockAcquireResult::LOCKACQUIRE_ALREADY_HELD);
    EXPECT_TRUE(lazyLockHint->GetLazyLockCnt() == 0);
    EXPECT_FALSE(lazyLockHint->IsLazyLockEnabled());
    EXPECT_EQ(GetNonLazyLockCnt(0, 0, DSTORE_ACCESS_EXCLUSIVE_LOCK), 2);

    /* Release one of them. */
    LockInterface::UnlockTable(&context);
    EXPECT_TRUE(lazyLockHint->GetLazyLockCnt() == 0);
    EXPECT_FALSE(lazyLockHint->IsLazyLockEnabled());
    EXPECT_EQ(GetNonLazyLockCnt(0, 0, DSTORE_ACCESS_EXCLUSIVE_LOCK), 1);

    /* Wait for weak lock thread. */
    pthread_barrier_wait(&barrier);
    pthread_barrier_wait(&barrier);

    /* Clean up. */
    LockInterface::UnlockTable(&context);
    EXPECT_TRUE(lazyLockHint->GetLazyLockCnt() == 0);
    EXPECT_TRUE(lazyLockHint->IsLazyLockEnabled());
    EXPECT_EQ(GetNonLazyLockCnt(0, 0, DSTORE_ACCESS_EXCLUSIVE_LOCK), 0);

    /* Wait for weak lock thread. */
    pthread_barrier_wait(&barrier);

    weakLockThread.join();
}

/**
 * Acquire weak lazy lock on one thread, then try to acquire one strong lock with a different
 * lazy lock partition id on another thread, since lazy lock is partitioned, it should
 * not block the original thread.
 */
TEST_F(UTLazyLock, StrongLockAtDiffPartIdDoesntBlockWeakLock_level0)
{
    ASSERT_FALSE(g_storageInstance->GetTableLockMgr() == NULL);

    LockInterface::TableLockContext context;
    context.dbId = 0;
    context.relId = 0;
    context.isPartition = false;
    context.isSessionLock = false;
    context.mode = DSTORE_ROW_EXCLUSIVE_LOCK;
    context.dontWait = LOCK_WAIT;

    /* Acquire a weak lock. */
    RetStatus res = LockInterface::LockTable(&context);
    EXPECT_TRUE(res == DSTORE_SUCC);
    EXPECT_TRUE(context.result == LockInterface::LockAcquireResult::LOCKACQUIRE_OK);

    /* Confirm that lock is acquired by lazy lock. */
    LazyLockHint *lazyLockHint = thrd->GetLockCtx()->GetLazyLockHint(GetLazyLockPartId(0, 0));
    EXPECT_TRUE(GetNonLazyLockCnt(0, 0, DSTORE_ROW_EXCLUSIVE_LOCK) == 0);
    EXPECT_TRUE(lazyLockHint->GetLazyLockCnt() == 1);
    EXPECT_TRUE(lazyLockHint->IsLazyLockEnabled());

    /* Start a new thread to wait for strong lock. */
    constexpr int numThreads = 2;
    pthread_barrier_t barrier;
    pthread_barrier_init(&barrier, nullptr, numThreads);

    std::thread strongLockThread = std::thread([&barrier] {
        ThreadInit();
        LockInterface::TableLockContext context;
        context.dbId = 0;
        context.relId = 1;
        context.isPartition = false;
        context.isSessionLock = false;
        context.mode = DSTORE_EXCLUSIVE_LOCK;
        context.dontWait = LOCK_WAIT;

        /* Make sure the 2 threads all reach this point. */
        pthread_barrier_wait(&barrier);

        /* Acquire a strong lock with different partId. */
        uint32 partId = GetLazyLockPartId(0, 1);
        ASSERT_NE(partId, GetLazyLockPartId(0, 0));
        RetStatus res = LockInterface::LockTable(&context);
        EXPECT_TRUE(res == DSTORE_SUCC);
        EXPECT_TRUE(context.result == LockInterface::LockAcquireResult::LOCKACQUIRE_OK);

        /* Wait for weak lock thread finish checking. */
        pthread_barrier_wait(&barrier);

        /* Release strong lock. */
        LockInterface::UnlockTable(&context);
        LazyLockHint *lazyLockHint = thrd->GetLockCtx()->GetLazyLockHint(GetLazyLockPartId(0, 0));
        EXPECT_TRUE(lazyLockHint->GetLazyLockCnt() == 0);
        EXPECT_TRUE(lazyLockHint->IsLazyLockEnabled());
        EXPECT_EQ(GetNonLazyLockCnt(0, 0, DSTORE_ROW_EXCLUSIVE_LOCK), 0);

        ThreadDestroy();
    });

    /* Make sure the 2 threads all reach this point. */
    pthread_barrier_wait(&barrier);

    /* Check that lazy lock is still enabled on current partId. */
    EXPECT_TRUE(GetNonLazyLockCnt(0, 0, DSTORE_ROW_EXCLUSIVE_LOCK) == 0);
    EXPECT_TRUE(lazyLockHint->GetLazyLockCnt() == 1);
    EXPECT_TRUE(lazyLockHint->IsLazyLockEnabled());

    /* Get another weak lock from lazy lock and check. */
    res = LockInterface::LockTable(&context);
    EXPECT_TRUE(res == DSTORE_SUCC);
    EXPECT_TRUE(context.result == LockInterface::LockAcquireResult::LOCKACQUIRE_ALREADY_HELD);
    EXPECT_TRUE(GetNonLazyLockCnt(0, 0, DSTORE_ROW_EXCLUSIVE_LOCK) == 0);
    EXPECT_TRUE(lazyLockHint->GetLazyLockCnt() == 2);
    EXPECT_TRUE(lazyLockHint->IsLazyLockEnabled());

    /* Finish checking, wake up strong lock thread. */
    pthread_barrier_wait(&barrier);

    /* Release the 2 weak locks. */
    LockInterface::UnlockTable(&context);
    EXPECT_TRUE(lazyLockHint->GetLazyLockCnt() == 1);
    LockInterface::UnlockTable(&context);
    EXPECT_TRUE(lazyLockHint->GetLazyLockCnt() == 0);

    strongLockThread.join();
}

/**
 * Deadlock detector should still work when lazy lock is enabled.
 */
TEST_F(UTLazyLock, DISABLED_DeadlockDetectTest_level0)
{
    ASSERT_FALSE(g_storageInstance->GetTableLockMgr() == NULL);
    constexpr int numThreads = 2;
    pthread_barrier_t barrier;
    pthread_barrier_init(&barrier, nullptr, numThreads);

    LockInterface::TableLockContext context1;
    context1.dbId = 0;
    context1.relId = 0;
    context1.isPartition = false;
    context1.isSessionLock = false;
    context1.mode = DSTORE_ROW_SHARE_LOCK;
    context1.dontWait = LOCK_WAIT;

    LockInterface::TableLockContext context2;
    context2.dbId = 0;
    context2.relId = FindAnotherTableOidHasSamePartId(0, 0);
    context2.isPartition = false;
    context2.isSessionLock = false;
    context2.mode = DSTORE_ACCESS_SHARE_LOCK;
    context2.dontWait = LOCK_WAIT;

    std::thread deadlockThread = std::thread([&barrier, &context1, &context2] {
        ThreadInit();

        /* Start transaction. */
        TransactionInterface::StartTrxCommand();
        uint32 partId1 = GetLazyLockPartId(0, 0);
        uint32 partId2 = GetLazyLockPartId(0, context2.relId);
        ASSERT_EQ(partId1, partId2);
        LazyLockHint *lazyLockHint = thrd->GetLockCtx()->GetLazyLockHint(partId1);
        EXPECT_TRUE(lazyLockHint->IsLazyLockEnabled());

        /* Hold weak lock1 first. */
        RetStatus res = LockInterface::LockTable(&context1);
        EXPECT_TRUE(res == DSTORE_SUCC);
        EXPECT_TRUE(context1.result == LockInterface::LockAcquireResult::LOCKACQUIRE_OK);
        EXPECT_TRUE(lazyLockHint->GetLazyLockCnt() == 1);
        EXPECT_TRUE(lazyLockHint->IsLazyLockEnabled());
        EXPECT_EQ(GetNonLazyLockCnt(0, 0, DSTORE_ROW_SHARE_LOCK), 0);

        /* Wait until the other thread hold weak lock2. */
        pthread_barrier_wait(&barrier);
        EXPECT_TRUE(lazyLockHint->IsLazyLockEnabled());
        pthread_barrier_wait(&barrier);

        /* Wait for strong lock2, it should success. */
        context2.mode = DSTORE_ACCESS_EXCLUSIVE_LOCK;
        res = LockInterface::LockTable(&context2);
        EXPECT_TRUE(res == DSTORE_SUCC);
        EXPECT_TRUE(context2.result == LockInterface::LockAcquireResult::LOCKACQUIRE_OK);
        EXPECT_TRUE(lazyLockHint->GetLazyLockCnt() == 0);
        EXPECT_FALSE(lazyLockHint->IsLazyLockEnabled());
        EXPECT_EQ(GetNonLazyLockCnt(0, 0, DSTORE_ROW_SHARE_LOCK), 1);
        EXPECT_EQ(GetNonLazyLockCnt(0, context2.relId, DSTORE_ACCESS_EXCLUSIVE_LOCK), 1);

        /* Clean up. */
        TransactionInterface::CommitTrxCommand();
        pthread_barrier_wait(&barrier);
        EXPECT_EQ(GetNonLazyLockCnt(0, 0, DSTORE_ROW_SHARE_LOCK), 0);
        EXPECT_EQ(GetNonLazyLockCnt(0, context2.relId, DSTORE_ACCESS_EXCLUSIVE_LOCK), 0);
        EXPECT_TRUE(lazyLockHint->IsLazyLockEnabled());
        EXPECT_TRUE(lazyLockHint->GetLazyLockCnt() == 0);

        ThreadDestroy();
    });

    /* Wait until the other thread get weak lock1. */
    pthread_barrier_wait(&barrier);
    TransactionInterface::StartTrxCommand();
    uint32 partId1 = GetLazyLockPartId(0, 0);
    uint32 partId2 = GetLazyLockPartId(0, context2.relId);
    ASSERT_EQ(partId1, partId2);
    LazyLockHint *lazyLockHint = thrd->GetLockCtx()->GetLazyLockHint(partId1);
    EXPECT_TRUE(lazyLockHint->IsLazyLockEnabled());

    /* Hold weak lock2. */
    RetStatus res = LockInterface::LockTable(&context2);
    EXPECT_TRUE(res == DSTORE_SUCC);
    EXPECT_TRUE(context2.result == LockInterface::LockAcquireResult::LOCKACQUIRE_OK);
    EXPECT_TRUE(lazyLockHint->GetLazyLockCnt() == 1);
    EXPECT_TRUE(lazyLockHint->IsLazyLockEnabled());
    EXPECT_EQ(GetNonLazyLockCnt(0, context2.relId, DSTORE_ACCESS_SHARE_LOCK), 0);

    /* Notify to start strong lock acquisition. */
    pthread_barrier_wait(&barrier);

    /* Wait for strong lock1, it should fail because of deadlock. */
    context1.mode = DSTORE_EXCLUSIVE_LOCK;
    res = LockInterface::LockTable(&context1);
    EXPECT_FALSE(res == DSTORE_SUCC);
    EXPECT_TRUE(context1.result == LockInterface::LockAcquireResult::LOCKACQUIRE_OTHER_ERROR);
    EXPECT_TRUE(StorageGetErrorCode() == LOCK_ERROR_DEADLOCK);

    /* Comfirm lazy lock is transfered in to main lock table when deadlock detect. */
    EXPECT_TRUE(lazyLockHint->GetLazyLockCnt() == 0);
    EXPECT_FALSE(lazyLockHint->IsLazyLockEnabled());
    EXPECT_EQ(GetNonLazyLockCnt(0, context2.relId, DSTORE_ACCESS_SHARE_LOCK), 1);

    /* Clean up. */
    TransactionInterface::AbortTrx();
    pthread_barrier_wait(&barrier);
    EXPECT_EQ(GetNonLazyLockCnt(0, context2.relId, DSTORE_ACCESS_SHARE_LOCK), 0);
    EXPECT_TRUE(lazyLockHint->IsLazyLockEnabled());
    EXPECT_TRUE(lazyLockHint->GetLazyLockCnt() == 0);

    deadlockThread.join();
}

/**
 * Same thread deadlock detect for autonomous transaction should still work when lazy lock is enabled.
 */
TEST_F(UTLazyLock, AutonomousTrxSameThreadTableLockDeadlock_level0)
{
    ASSERT_FALSE(g_storageInstance->GetTableLockMgr() == NULL);

    LockInterface::TableLockContext context;
    context.dbId = 0;
    context.relId = 0;
    context.isPartition = false;
    context.isSessionLock = false;
    context.mode = DSTORE_ROW_EXCLUSIVE_LOCK;
    context.dontWait = LOCK_WAIT;

    /* Start transaction. */
    TransactionInterface::StartTrxCommand();
    LazyLockHint *lazyLockHint = thrd->GetLockCtx()->GetLazyLockHint(GetLazyLockPartId(0, 0));
    EXPECT_TRUE(lazyLockHint->IsLazyLockEnabled());

    /* Acquire a weak lock. */
    RetStatus res = LockInterface::LockTable(&context);
    EXPECT_TRUE(res == DSTORE_SUCC);
    EXPECT_TRUE(context.result == LockInterface::LockAcquireResult::LOCKACQUIRE_OK);

    /* Confirm that lock is acquired by lazy lock. */
    EXPECT_TRUE(GetNonLazyLockCnt(0, 0, DSTORE_ROW_EXCLUSIVE_LOCK) == 0);
    EXPECT_TRUE(lazyLockHint->GetLazyLockCnt() == 1);
    EXPECT_TRUE(lazyLockHint->IsLazyLockEnabled());

    /* Create autonomous transaction. */
    TransactionInterface::CreateAutonomousTransaction();
    thrd->GetActiveTransaction()->Start();
    thrd->GetActiveTransaction()->BeginTransactionBlock();
    thrd->GetActiveTransaction()->Commit();

    /* Try take a not conflict lock in autonomous transaction, it should success. */
    context.mode = DSTORE_ROW_SHARE_LOCK;
    res = LockInterface::LockTable(&context);
    EXPECT_TRUE(res == DSTORE_SUCC);
    EXPECT_TRUE(context.result == LockInterface::LockAcquireResult::LOCKACQUIRE_OK);

    /* Confirm that second lock is also acquired by lazy lock. */
    EXPECT_TRUE(GetNonLazyLockCnt(0, 0, DSTORE_ROW_EXCLUSIVE_LOCK) == 0);
    EXPECT_TRUE(GetNonLazyLockCnt(0, 0, DSTORE_ROW_SHARE_LOCK) == 0);
    EXPECT_TRUE(lazyLockHint->GetLazyLockCnt() == 2);
    EXPECT_TRUE(lazyLockHint->IsLazyLockEnabled());

    /* Try take a conflict lock in autonomous transaction, it should return fail. */
    context.mode = DSTORE_EXCLUSIVE_LOCK;
    res = LockInterface::LockTable(&context);
    EXPECT_TRUE(res == DSTORE_FAIL);
    EXPECT_TRUE(context.result == LockInterface::LockAcquireResult::LOCKACQUIRE_OTHER_ERROR);
    EXPECT_EQ(StorageGetErrorCode(), TRANSACTION_INFO_SAME_THREAD_DEADLOCK);
    EXPECT_TRUE(GetNonLazyLockCnt(0, 0, DSTORE_ROW_EXCLUSIVE_LOCK) == 0);
    EXPECT_TRUE(GetNonLazyLockCnt(0, 0, DSTORE_ROW_SHARE_LOCK) == 0);
    EXPECT_TRUE(GetNonLazyLockCnt(0, 0, DSTORE_EXCLUSIVE_LOCK) == 0);
    EXPECT_TRUE(lazyLockHint->GetLazyLockCnt() == 2);
    EXPECT_TRUE(lazyLockHint->IsLazyLockEnabled());

    /* Commit autonomous transaction. */
    thrd->GetActiveTransaction()->EndTransactionBlock();
    thrd->GetActiveTransaction()->Commit();
    TransactionInterface::DestroyAutonomousTransaction();

    EXPECT_TRUE(lazyLockHint->GetLazyLockCnt() == 1);
    EXPECT_TRUE(lazyLockHint->IsLazyLockEnabled());

    /* Commit the primary transaction. */
    TransactionInterface::CommitTrxCommand();
    EXPECT_TRUE(lazyLockHint->GetLazyLockCnt() == 0);
    EXPECT_TRUE(lazyLockHint->IsLazyLockEnabled());
}

/**
 * Savepoint should still work when lazy lock is enabled.
 */
TEST_F(UTLazyLock, SavepointTest_level0)
{
    ASSERT_FALSE(g_storageInstance->GetTableLockMgr() == NULL);

    LockInterface::TableLockContext context;
    context.dbId = 0;
    context.relId = 0;
    context.isPartition = false;
    context.isSessionLock = false;
    context.mode = DSTORE_ROW_EXCLUSIVE_LOCK;
    context.dontWait = LOCK_WAIT;

    /* Start transaction. */
    TransactionInterface::StartTrxCommand();
    uint32 partId1 = GetLazyLockPartId(0, 0);
    LazyLockHint *lazyLockHint1 = thrd->GetLockCtx()->GetLazyLockHint(partId1);
    EXPECT_TRUE(lazyLockHint1->IsLazyLockEnabled());

    /* Acquire a weak lock. */
    RetStatus res = LockInterface::LockTable(&context);
    EXPECT_TRUE(res == DSTORE_SUCC);
    EXPECT_TRUE(context.result == LockInterface::LockAcquireResult::LOCKACQUIRE_OK);

    /* Confirm that lock is acquired by lazy lock. */
    EXPECT_TRUE(GetNonLazyLockCnt(0, 0, DSTORE_ROW_EXCLUSIVE_LOCK) == 0);
    EXPECT_TRUE(lazyLockHint1->GetLazyLockCnt() == 1);
    EXPECT_TRUE(lazyLockHint1->IsLazyLockEnabled());

    /* Create savepoint1. */
    res = TransactionInterface::CreateSavepoint("savepoint1");
    EXPECT_TRUE(res == DSTORE_SUCC);

    /* Acquire the weak lock again. */
    res = LockInterface::LockTable(&context);
    EXPECT_TRUE(res == DSTORE_SUCC);
    EXPECT_TRUE(context.result == LockInterface::LockAcquireResult::LOCKACQUIRE_ALREADY_HELD);

    /* Confirm that second lock is also acquired by lazy lock. */
    EXPECT_TRUE(GetNonLazyLockCnt(0, 0, DSTORE_ROW_EXCLUSIVE_LOCK) == 0);
    EXPECT_TRUE(lazyLockHint1->GetLazyLockCnt() == 2);
    EXPECT_TRUE(lazyLockHint1->IsLazyLockEnabled());

    /* Try take another weak lock. */
    context.relId = 1;
    uint32 partId2 = GetLazyLockPartId(0, 1);
    ASSERT_NE(partId1, partId2);
    LazyLockHint *lazyLockHint2 = thrd->GetLockCtx()->GetLazyLockHint(partId2);
    res = LockInterface::LockTable(&context);
    EXPECT_TRUE(res == DSTORE_SUCC);
    EXPECT_TRUE(context.result == LockInterface::LockAcquireResult::LOCKACQUIRE_OK);
    EXPECT_TRUE(GetNonLazyLockCnt(0, 0, DSTORE_ROW_EXCLUSIVE_LOCK) == 0);
    EXPECT_TRUE(GetNonLazyLockCnt(0, 1, DSTORE_ROW_EXCLUSIVE_LOCK) == 0);
    EXPECT_TRUE(lazyLockHint1->GetLazyLockCnt() == 2);
    EXPECT_TRUE(lazyLockHint2->GetLazyLockCnt() == 1);
    EXPECT_TRUE(lazyLockHint2->IsLazyLockEnabled());

    /* Rollback to savepoint1. */
    res = TransactionInterface::RollbackToSavepoint("savepoint1");
    EXPECT_TRUE(res == DSTORE_SUCC);
    EXPECT_TRUE(lazyLockHint1->GetLazyLockCnt() == 1);
    EXPECT_TRUE(lazyLockHint1->IsLazyLockEnabled());
    EXPECT_TRUE(lazyLockHint2->GetLazyLockCnt() == 0);
    EXPECT_TRUE(lazyLockHint2->IsLazyLockEnabled());

    /* Acquire a strong lock. */
    context.mode = DSTORE_EXCLUSIVE_LOCK;
    res = LockInterface::LockTable(&context);
    EXPECT_TRUE(res == DSTORE_SUCC);
    EXPECT_TRUE(context.result == LockInterface::LockAcquireResult::LOCKACQUIRE_OK);
    EXPECT_TRUE(GetNonLazyLockCnt(0, 0, DSTORE_ROW_EXCLUSIVE_LOCK) == 1);
    EXPECT_TRUE(GetNonLazyLockCnt(0, 1, DSTORE_EXCLUSIVE_LOCK) == 1);
    EXPECT_TRUE(lazyLockHint1->GetLazyLockCnt() == 0);
    EXPECT_TRUE(lazyLockHint1->IsLazyLockEnabled());
    EXPECT_TRUE(lazyLockHint2->GetLazyLockCnt() == 0);
    EXPECT_FALSE(lazyLockHint2->IsLazyLockEnabled());

    /* Commit the primary transaction. */
    TransactionInterface::CommitTrxCommand();
    EXPECT_TRUE(lazyLockHint1->GetLazyLockCnt() == 0);
    EXPECT_TRUE(lazyLockHint1->IsLazyLockEnabled());
    EXPECT_TRUE(lazyLockHint2->GetLazyLockCnt() == 0);
    EXPECT_TRUE(lazyLockHint2->IsLazyLockEnabled());
}

TEST_F(UTLazyLock, LockRecordMergeTest_level0)
{
    LockTag lockTag;
    lockTag.SetTableLockTag(0, 0);
    DstoreMemoryContext memCtx = thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_TRANSACTION);
    RetStatus ret;

    LockResource::LockResourceRecord record1;
    LockResource::LockResourceRecord record2;

    record1.InitRecord(lockTag, DSTORE_ACCESS_SHARE_LOCK, TABLE_LOCK_MGR);
    record2.InitRecord(lockTag, DSTORE_ACCESS_SHARE_LOCK, TABLE_LOCK_MGR);

    LockResource::SubLockResourceID lockSequence1[] = {
        2, 2, 5, 7, 7, 7, 8, 8, 10, 11, 100
    };
    LockResource::SubLockResourceID lockSequence2[] = {
        3, 3, 3, 6, 7, 8, 9, 9, 12, 12, 13, 14, 100, 101, 101, 105, 106, 107
    };
    LockResource::SubLockResourceID lockSequence3[] = {
        2, 2, 3, 3, 3, 5, 6, 7, 7, 7, 7, 8, 8, 8, 9, 9, 10, 11, 12, 12, 13, 14, 100, 100, 101, 101, 105, 106, 107
    };
    LockResource::SubLockResourceID checkResId1[] = {
        2, 5, 7, 8, 10, 11, 100
    };
    LockResource::SubLockResourceID checkResId2[] = {
        3, 6, 7, 8, 9, 12, 13, 14, 100, 101, 105, 106, 107
    };
    LockResource::SubLockResourceID checkResId3[] = {
        2, 3, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 100, 101, 105, 106, 107
    };
    LockResource::SubLockResourceID checkResId4[] = {
        2, 3, 5, 6, 7, 8
    };
    uint32 checkCnt1[] = {
        2, 1, 3, 2, 1, 1, 1
    };
    uint32 checkCnt2[] = {
        3, 1, 1, 1, 2, 2, 1, 1, 1, 2, 1, 1, 1
    };
    uint32 checkCnt3[] = {
        2, 3, 1, 1, 4, 3, 2, 1, 1, 2, 1, 1, 2, 2, 1, 1, 1
    };
    uint32 checkCnt4[] = {
        2, 3, 1, 1, 4, 2
    };

    /* Add lock to record1 by lock sequence1. */
    for (uint32 i = 0; i < sizeof(lockSequence1) / sizeof(LockResource::SubLockResourceID); i++) {
        ret = record1.AddLock(memCtx, lockSequence1[i]);
        EXPECT_TRUE(ret == DSTORE_SUCC);
    }

    /* Check the lock is correctly recorded. */
    uint32 checkIndex = 0;
    LockResource::LockResourceRecord::SubLockResourceIter iter1(&record1);
    while (!iter1.IsEnd()) {
        EXPECT_EQ(iter1.GetSubResourceID(), checkResId1[checkIndex]);
        EXPECT_EQ(iter1.GetSubResourceCnt(), checkCnt1[checkIndex]);
        iter1.Next();
        checkIndex++;
    }
    EXPECT_EQ(checkIndex, sizeof(checkResId1) / sizeof(LockResource::SubLockResourceID));
    EXPECT_EQ(checkIndex, sizeof(checkCnt1) / sizeof(uint32));
    EXPECT_EQ(record1.GetTotalCnt(), 11);

    /* Add lock to record2 by lock sequence2. */
    for (uint32 i = 0; i < sizeof(lockSequence2) / sizeof(LockResource::SubLockResourceID); i++) {
        ret = record2.AddLock(memCtx, lockSequence2[i]);
        EXPECT_TRUE(ret == DSTORE_SUCC);
    }

    /* Check the lock is correctly recorded. */
    checkIndex = 0;
    LockResource::LockResourceRecord::SubLockResourceIter iter2(&record2);
    while (!iter2.IsEnd()) {
        EXPECT_EQ(iter2.GetSubResourceID(), checkResId2[checkIndex]);
        EXPECT_EQ(iter2.GetSubResourceCnt(), checkCnt2[checkIndex]);
        iter2.Next();
        checkIndex++;
    }
    EXPECT_EQ(checkIndex, sizeof(checkResId2) / sizeof(LockResource::SubLockResourceID));
    EXPECT_EQ(checkIndex, sizeof(checkCnt2) / sizeof(uint32));
    EXPECT_EQ(record2.GetTotalCnt(), 18);

    /* Merge record2 to record1. */
    ret = record1.MergeRecord(memCtx, &record2);
    EXPECT_TRUE(ret == DSTORE_SUCC);

    /* Check that merge result is correct. */
    checkIndex = 0;
    LockResource::LockResourceRecord::SubLockResourceIter iter3(&record1);
    while (!iter3.IsEnd()) {
        EXPECT_EQ(iter3.GetSubResourceID(), checkResId3[checkIndex]);
        EXPECT_EQ(iter3.GetSubResourceCnt(), checkCnt3[checkIndex]);
        iter3.Next();
        checkIndex++;
    }
    EXPECT_EQ(checkIndex, sizeof(checkResId3) / sizeof(LockResource::SubLockResourceID));
    EXPECT_EQ(checkIndex, sizeof(checkCnt3) / sizeof(uint32));
    EXPECT_EQ(record1.GetTotalCnt(), 29);

    /* Check remove from 101. */
    uint32 removed = record1.RemoveLocksAfter(101);
    EXPECT_EQ(removed, 5);
    EXPECT_EQ(record1.GetTotalCnt(), 24);

    /* Check remove from 9. */
    removed = record1.RemoveLocksAfter(9);
    EXPECT_EQ(removed, 10);
    EXPECT_EQ(record1.GetTotalCnt(), 14);

    /* Check remove latest. */
    record1.RemoveLatestLock();
    EXPECT_EQ(record1.GetTotalCnt(), 13);
    checkIndex = 0;
    LockResource::LockResourceRecord::SubLockResourceIter iter4(&record1);
    while (!iter4.IsEnd()) {
        EXPECT_EQ(iter4.GetSubResourceID(), checkResId4[checkIndex]);
        EXPECT_EQ(iter4.GetSubResourceCnt(), checkCnt4[checkIndex]);
        iter4.Next();
        checkIndex++;
    }
    EXPECT_EQ(checkIndex, sizeof(checkResId4) / sizeof(LockResource::SubLockResourceID));
    EXPECT_EQ(checkIndex, sizeof(checkCnt4) / sizeof(uint32));

    /* Clean up. */
    record1.CleanUp();
    record2.CleanUp();
}

TEST_F(UTLazyLock, RandomTest_level2)
{
    ASSERT_FALSE(g_storageInstance->GetTableLockMgr() == NULL);

    /* Start new threads for lock competition. */
    constexpr int numThreads = 8;
    std::thread lockTestThreads[numThreads];

    for (int i = 0; i < numThreads; i++) {
        lockTestThreads[i] = std::thread([i] {
            ThreadInit();

            for (int loop = 0; loop < 10000; loop++) {
                LockInterface::TableLockContext context;
                context.dbId = 0;
                context.relId = 0;
                context.isPartition = false;
                context.isSessionLock = false;
                context.mode = static_cast<LockMode>(DSTORE_ACCESS_SHARE_LOCK + loop % 8);
                context.dontWait = LOCK_WAIT;

                RetStatus res = LockInterface::LockTable(&context);
                EXPECT_TRUE(res == DSTORE_SUCC);
                EXPECT_TRUE(context.result == LockInterface::LockAcquireResult::LOCKACQUIRE_OK);

                LockInterface::UnlockTable(&context);
                LazyLockHint *lazyLockHint = thrd->GetLockCtx()->GetLazyLockHint(GetLazyLockPartId(0, 0));
                EXPECT_TRUE(lazyLockHint->GetLazyLockCnt() == 0);
            }

            ThreadDestroy();
        });
    }

    for (int i = 0; i < numThreads; i++) {
        lockTestThreads[i].join();
    }
}

TEST_F(UTLazyLock, RandomTest2_level2)
{
    ASSERT_FALSE(g_storageInstance->GetTableLockMgr() == NULL);

    /* Start new threads for lock competition. */
    constexpr int numThreads = 4;
    std::thread lockTestThreads[numThreads];

    for (int i = 0; i < numThreads; i++) {
        lockTestThreads[i] = std::thread([i] {
            ThreadInit();

            for (int loop = 0; loop < 10000; loop++) {
                LockInterface::TableLockContext context;
                context.dbId = 0;
                context.relId = 0;
                context.isPartition = false;
                context.isSessionLock = false;
                int randNum = rand();
                context.mode = static_cast<LockMode>(DSTORE_ACCESS_SHARE_LOCK + (((uint32)(loop + randNum)) % 5));
                context.dontWait = LOCK_WAIT;

                RetStatus res = LockInterface::LockTable(&context);
                EXPECT_TRUE(res == DSTORE_SUCC);
                EXPECT_TRUE(context.result == LockInterface::LockAcquireResult::LOCKACQUIRE_OK);

                LockInterface::TableLockContext context2;
                context2.dbId = 0;
                context2.relId = 1;
                context2.isPartition = false;
                context2.isSessionLock = false;
                context2.mode = static_cast<LockMode>(DSTORE_ACCESS_SHARE_LOCK + (((uint32)(loop + randNum)) % 8));
                context2.dontWait = LOCK_WAIT;

                res = LockInterface::LockTable(&context2);
                EXPECT_TRUE(res == DSTORE_SUCC);
                EXPECT_TRUE(context2.result == LockInterface::LockAcquireResult::LOCKACQUIRE_OK);

                LockInterface::UnlockTable(&context2);
                LockInterface::UnlockTable(&context);
                LazyLockHint *lazyLockHint = thrd->GetLockCtx()->GetLazyLockHint(GetLazyLockPartId(0, 0));
                EXPECT_TRUE(lazyLockHint->GetLazyLockCnt() == 0);
                lazyLockHint = thrd->GetLockCtx()->GetLazyLockHint(GetLazyLockPartId(1, 0));
                EXPECT_TRUE(lazyLockHint->GetLazyLockCnt() == 0);
            }

            ThreadDestroy();
        });
    }

    for (int i = 0; i < numThreads; i++) {
        lockTestThreads[i].join();
    }
}

TEST_F(UTLazyLock, GetTableLockByLockTagTest_level2)
{
    ASSERT_FALSE(g_storageInstance->GetTableLockMgr() == NULL);

    uint32 holdThreadCount = 5;
    uint32 waitThreadCount = 1;
    pthread_barrier_t barrier;
    pthread_barrier_init(&barrier, nullptr, holdThreadCount + waitThreadCount + 1);
    std::atomic<bool> isPrintFinished(false);

    auto HoldLockThread = [&barrier, &isPrintFinished]() {
        UtInstance *instance = (UtInstance *)g_storageInstance;
        instance->ThreadSetupAndRegister();

        LockInterface::TableLockContext context;
        context.dbId = 0;
        context.relId = 0;
        context.isPartition = false;
        context.isSessionLock = false;
        context.mode = DSTORE_ROW_EXCLUSIVE_LOCK;
        context.dontWait = LOCK_WAIT;
        RetStatus res = LockInterface::LockTable(&context);
        EXPECT_TRUE(res == DSTORE_SUCC);
        EXPECT_TRUE(context.result == LockInterface::LockAcquireResult::LOCKACQUIRE_OK);

        pthread_barrier_wait(&barrier);
        while (!isPrintFinished) {
            usleep(1000);
        }

        LockInterface::UnlockTable(&context);

        instance->ThreadUnregisterAndExit();
    };

    auto WaitLockThread = [&barrier, &isPrintFinished]() {
        UtInstance *instance = (UtInstance *)g_storageInstance;
        instance->ThreadSetupAndRegister();

        /* Wait until hold threads all finished acquiring lock. */
        pthread_barrier_wait(&barrier);
        LockInterface::TableLockContext context;
        context.dbId = 0;
        context.relId = 0;
        context.isPartition = false;
        context.isSessionLock = false;
        context.mode = DSTORE_SHARE_UPDATE_EXCLUSIVE_LOCK;
        context.dontWait = LOCK_WAIT;
        RetStatus res = LockInterface::LockTable(&context);
        EXPECT_TRUE(res == DSTORE_SUCC);
        EXPECT_TRUE(context.result == LockInterface::LockAcquireResult::LOCKACQUIRE_OK);
        while (!isPrintFinished) {
            usleep(1000);
        }

        LockInterface::UnlockTable(&context);

        instance->ThreadUnregisterAndExit();
    };

    std::thread holdLockThreads[holdThreadCount];
    for (uint32 i = 0; i < holdThreadCount; i++) {
        holdLockThreads[i] = std::thread(HoldLockThread);
    }

    std::thread waitLockThreads[waitThreadCount];
    for (uint32 i = 0; i < waitThreadCount; i++) {
        waitLockThreads[i] = std::thread(WaitLockThread);
    }

    LockTagContext context = {0};
    context.lockTagType = LOCKTAG_TABLE;
    pthread_barrier_wait(&barrier);
    usleep(1000 * 100);

    /* Now hold threads all get locks, and wait threads is likely to be already waiting. */
    char *lockDump = LockMgrDiagnose::GetTableLockByLockTag(context);
    EXPECT_TRUE(lockDump != nullptr);
    printf("%s\n", lockDump);
    DstorePfreeExt(lockDump);
    isPrintFinished = true;

    for (uint32 i = 0; i < holdThreadCount; i++) {
        holdLockThreads[i].join();
    }

    for (uint32 i = 0; i < waitThreadCount; i++) {
        waitLockThreads[i].join();
    }
}
