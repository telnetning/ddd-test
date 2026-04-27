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
#include "lock/dstore_lock_hash_table.h"
#include "ut_lock/ut_lock_hash_table.h"
#include <mutex>

using namespace DSTORE;

class UTLockMgrHashTable : public DSTORETEST {
protected:
    void SetUp() override
    {
        DSTORETEST::SetUp();
        UtInstance *instance = DstoreNew(m_ut_memory_context)UtInstance();
        instance->Install(&DSTORETEST::m_guc, m_ut_memory_context);
        instance->Startup(&DSTORETEST::m_guc);

        m_lockTable = DstoreNew(m_ut_memory_context) LockHashTableForUT();
        ASSERT_TRUE(m_lockTable != NULL);
        uint32 hashTableSize = 16;
        uint32 partitionNum = 16;
        RetStatus ret = m_lockTable->Initialize(hashTableSize, partitionNum, m_ut_memory_context);
        EXPECT_TRUE(ret == DSTORE_SUCC);
    }

    void TearDown() override
    {
        m_lockTable->Destroy();
        delete m_lockTable;
        m_lockTable = NULL;

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

    static void AcquireAndReleaseTestThrd(LockHashTableForUT *m_lockTable, pthread_barrier_t *barrier,
        LockRequest *request, LockTagCache *tagCache);

    LockHashTableForUT *m_lockTable;
};

/*
 * Thread function for thread2 in AcquireAndReleaseTest.
 * Since thread1 is holding lock with tag1, thread2 should be waiting.
 * After thread1 releases the lock, thread2 should get the lock.
 */
void UTLockMgrHashTable::AcquireAndReleaseTestThrd(LockHashTableForUT *m_lockTable, pthread_barrier_t *barrier,
    LockRequest *request, LockTagCache *tagCache)
{
    ThreadInit();
    LockErrorInfo info = {0};
    request->threadId = thrd->GetThreadId();
    request->threadCoreIdx = thrd->GetCore()->selfIdx;
    RetStatus ret = m_lockTable->LockRequestEnqueue(*tagCache, request, &info);
    EXPECT_FALSE(ret == DSTORE_SUCC);
    EXPECT_TRUE(m_lockTable->GetEntryCount() == 1);
    EXPECT_FALSE(m_lockTable->IsHeldByRequester(tagCache->GetLockTag(), request));
    pthread_barrier_wait(barrier);
    /* thread2 releases the lock. */
    pthread_barrier_wait(barrier);
    m_lockTable->LockRequestDequeue(*tagCache, request);
    EXPECT_TRUE(m_lockTable->GetEntryCount() == 0);
    EXPECT_FALSE(m_lockTable->IsHeldByRequester(tagCache->GetLockTag(), request));
    ThreadDestroy();
}

/*
 * Basic test for lock table acquire and release operation.
 */
TEST_F(UTLockMgrHashTable, AcquireAndReleaseTest_level0)
{
    LockErrorInfo info = {0};
    LockTag tag1;
    LockTag tag2;
    tag1.field1 = 0;
    tag2.field1 = 1;
    LockTagCache tagCache1(&tag1);
    LockTagCache tagCache2(&tag2);
    EXPECT_TRUE(m_lockTable->GetEntryCount() == 0);

    /* thread1 ask for lock with tag1. */
    LockRequest request1(DSTORE_EXCLUSIVE_LOCK, thrd);
    RetStatus ret = m_lockTable->LockRequestEnqueue(tagCache1, &request1, &info);
    EXPECT_TRUE(ret == DSTORE_SUCC);
    EXPECT_TRUE(m_lockTable->GetEntryCount() == 1);
    EXPECT_TRUE(m_lockTable->IsHeldByRequester(&tag1, &request1));

    LockRequest request2(DSTORE_ACCESS_EXCLUSIVE_LOCK, INVALID_THREAD_ID, INVALID_THREAD_CORE_ID);
    pthread_barrier_t barrier;
    pthread_barrier_init(&barrier, NULL, 2);

    /* thread2 asks for lock with tag1, should fail and wait. */
    std::thread thread2 = std::thread(AcquireAndReleaseTestThrd, m_lockTable, &barrier,
        &request2, &tagCache1);

    pthread_barrier_wait(&barrier);
    /* thread1 asks for lock with tag2. */
    ret = m_lockTable->LockRequestEnqueue(tagCache2, &request1, &info);
    EXPECT_TRUE(ret == DSTORE_SUCC);
    EXPECT_TRUE(m_lockTable->GetEntryCount() == 2);
    EXPECT_TRUE(m_lockTable->IsHeldByRequester(&tag2, &request1));

    /* thread1 releases lock with tag1. */
    m_lockTable->LockRequestDequeue(tagCache1, &request1);
    EXPECT_TRUE(m_lockTable->GetEntryCount() == 2);
    EXPECT_FALSE(m_lockTable->IsHeldByRequester(&tag1, &request1));
    EXPECT_TRUE(m_lockTable->IsHeldByRequester(&tag1, &request2));

    /* thread1 releases lock with tag2. */
    m_lockTable->LockRequestDequeue(tagCache2, &request1);
    EXPECT_TRUE(m_lockTable->GetEntryCount() == 1);
    EXPECT_FALSE(m_lockTable->IsHeldByRequester(&tag2, &request1));

    pthread_barrier_wait(&barrier);
    thread2.join();
    pthread_barrier_destroy(&barrier);
}
