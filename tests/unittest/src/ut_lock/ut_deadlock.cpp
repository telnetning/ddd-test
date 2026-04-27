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
#include "port/dstore_port.h"
#include "lock/dstore_lock_mgr.h"
#include "lock/dstore_xact_lock_mgr.h"
#include "lock/dstore_table_lock_mgr.h"
#include "lock/dstore_lock_thrd_local.h"
#include "transaction/dstore_transaction_interface.h"
#include "transaction/dstore_transaction_mgr.h"
#include <iostream>
#include <thread>
#include <mutex>

using namespace DSTORE;

class LockMgrDeadlockTest : public DSTORETEST {
protected:
    void SetUp() override
    {
        DSTORETEST::SetUp();
        UtInstance *instance = DstoreNew(m_ut_memory_context)UtInstance();
        instance->Install(&DSTORETEST::m_guc, m_ut_memory_context);
        instance->Startup(&DSTORETEST::m_guc);
        instance->GetGuc()->deadlockTimeInterval = 500;
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
        semaphoresLock.lock();
        UtInstance *instance = (UtInstance *)g_storageInstance;
        instance->ThreadSetupAndRegister();
        semaphoresLock.unlock();
    }
    static void ThreadDestroy()
    {
        UtInstance *instance = (UtInstance *)g_storageInstance;
        instance->ThreadUnregisterAndExit();
    }

    enum LockAction {
        ACT_LOCK,
        ACT_UNLOCK
    };

    struct LockTestSeqItem {
        unsigned int threadId;
        unsigned int lockId;
        LockAction action;
        LockMode mode;
        bool needBarrier;
        int needOtherWaitSeqId;

        LockTestSeqItem(unsigned int threadId, unsigned int lockId, LockAction action, LockMode mode,
                        bool needBarrier = false, int needOtherWaitSeqId = -1)
            : threadId(threadId),
              lockId(lockId),
              action(action),
              mode(mode),
              needBarrier(needBarrier),
              needOtherWaitSeqId(needOtherWaitSeqId)
        {}
    };

    struct LockTestThreadSeqItemRes {
        RetStatus realRes;
        ErrorCode realErrorCode;
        LockTestThreadSeqItemRes(): realRes(DSTORE_SUCC), realErrorCode(STORAGE_OK) {}
    };

    struct LockTestObservationPoint {
        std::vector<unsigned int> seqIds;   /* Or */
        int deadlockVictimNum;
    };

    LockTag CreateTableLockTag(Oid pdbId, Oid relId)
    {
        LockTag tag;
        tag.SetTableLockTag(pdbId, relId);
        return tag;
    }

    static std::mutex semaphoresLock;

    static LockMgr *GetLockMgr(LockTagType type, LockMgrType &mgrType);

    static RetStatus RunThread(int tid, const LockTag lockTags[], const int lockNums, const LockTestSeqItem lockSeq[],
                        const int lockSeqLength, pthread_barrier_t *barrier, LockTestThreadSeqItemRes runRes[],
                        DstoreMemoryContext parentCtx, pthread_barrier_t *initBarrier = nullptr,
                        ThreadCore **thrdCores = nullptr);

    static bool ThreadHasBeenWaitingForLock(ThreadCore *core, const LockTag &tag, LockMode mode);
};

std::mutex LockMgrDeadlockTest::semaphoresLock;

LockMgr *LockMgrDeadlockTest::GetLockMgr(LockTagType type, LockMgrType& mgrType)
{
    LockMgr* lockMgr = nullptr;
    switch (type) {
        case LOCKTAG_TABLE:
            lockMgr = g_storageInstance->GetTableLockMgr();
            mgrType = TABLE_LOCK_MGR;
            break;
        default:
            break;
    }
    return lockMgr;
}

bool LockMgrDeadlockTest::ThreadHasBeenWaitingForLock(ThreadCore *core, const LockTag &tag, LockMode mode)
{
    ThreadLocalLock *localLock = core->regularLockCtx->GetLocalLock();
    LockTag waitTags[ThreadLocalLock::m_waitLockMaxCount];
    LockMode waitMode = DSTORE_NO_LOCK;
    uint32 waitLocksLen = 0;
    bool isWaiting =
        localLock->GetWaitingLocks(waitTags, &waitMode, ThreadLocalLock::m_waitLockMaxCount, &waitLocksLen);
    if (!isWaiting) {
        return false;
    } else {
        for (uint32 i = 0; i < waitLocksLen; i++) {
            if (waitTags[i] == tag && mode == waitMode) {
                return true;
            }
        }
        return false;
    }
}


RetStatus LockMgrDeadlockTest::RunThread(int tid, const LockTag lockTags[], const int lockNums,
                                         const LockTestSeqItem lockSeq[], const int lockSeqLength,
                                         pthread_barrier_t *barrier, LockTestThreadSeqItemRes runRes[],
                                         DstoreMemoryContext parentCtx, pthread_barrier_t *initBarrier, ThreadCore** thrdCores)
{
    ThreadInit();
    DstoreMemoryContext ctx = DstoreAllocSetContextCreate(parentCtx, "UTDynaHashMemoryContext", ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_INITSIZE, MemoryContextType::THREAD_CONTEXT);
    LockResource lockResource;
    lockResource.Initialize(ctx);
    RetStatus ret = DSTORE_SUCC;

    if (initBarrier != nullptr) {
        thrdCores[tid] = thrd->GetCore();
        pthread_barrier_wait(initBarrier);
    }

    for (int seqIndex = 0; seqIndex < lockSeqLength; ++seqIndex) {
        const LockTestSeqItem &item = lockSeq[seqIndex];
        LockErrorInfo info{0};
        if (item.threadId == tid) {
            const LockTag &tag = lockTags[item.lockId];
            LockMgrType mgrType = LOCK_MGR;
            LockMgr *mgr = GetLockMgr(tag.lockTagType, mgrType);
            if (item.action == ACT_LOCK) {
                if (thrdCores != nullptr && item.needOtherWaitSeqId != -1) {
                    const LockTestSeqItem& waitItem = lockSeq[item.needOtherWaitSeqId];
                    while (!ThreadHasBeenWaitingForLock(thrdCores[waitItem.threadId], lockTags[waitItem.lockId], waitItem.mode)) {
                        GaussUsleep(1000);
                    }
                }
                ret = mgr->Lock(&tag, item.mode, false, &info);
                lockResource.RememberLock(tag, item.mode, mgrType);
                runRes[seqIndex].realRes = ret;
                if (STORAGE_FUNC_FAIL(ret)) {
                    lockResource.ForgetLock(tag, item.mode, mgrType);
                    runRes[seqIndex].realErrorCode = StorageGetErrorCode();
                    lockResource.ReleaseAllLocks();
                }
            } else { /* UNLOCK */
                mgr->Unlock(&tag, item.mode);
                lockResource.ForgetLock(tag, item.mode, mgrType);
                runRes[seqIndex].realRes = DSTORE_SUCC;
            }
        }
        if (item.needBarrier) {
            pthread_barrier_wait(barrier);
        }
    }
    lockResource.ReleaseAllLocks();
    lockResource.Destroy();
    DstoreMemoryContextDelete(ctx);
    ThreadDestroy();
    return DSTORE_SUCC;
}

TEST_F(LockMgrDeadlockTest, DeadLockDetectionTestTwoThread_level0)
{
    // input
    constexpr int threadNums = 2;
    LockTag lockTags[] = {
        CreateTableLockTag(g_defaultPdbId, 1),
        CreateTableLockTag(g_defaultPdbId, 2),
    };
    LockTestSeqItem lockSeq[] = {
        LockTestSeqItem{0, 0, ACT_LOCK, DSTORE_ACCESS_EXCLUSIVE_LOCK},
        LockTestSeqItem{1, 1, ACT_LOCK, DSTORE_ACCESS_EXCLUSIVE_LOCK, true},
        LockTestSeqItem{0, 1, ACT_LOCK, DSTORE_ACCESS_EXCLUSIVE_LOCK},
        LockTestSeqItem{1, 0, ACT_LOCK, DSTORE_ACCESS_EXCLUSIVE_LOCK},
    };
    LockTestObservationPoint observations[] = {
        LockTestObservationPoint{{0}, 0},
        LockTestObservationPoint{{1}, 0},
        LockTestObservationPoint{{2, 3}, 1},
    };
    constexpr int lockNums = sizeof(lockTags) / sizeof(LockTag);
    constexpr int lockSeqLength = sizeof(lockSeq) / sizeof(LockTestSeqItem);
    constexpr int observationSize = sizeof(observations) / sizeof(LockTestObservationPoint);

    // run
    pthread_barrier_t barrier;
    pthread_barrier_init(&barrier, nullptr, threadNums);
    std::thread threads[threadNums];
    LockTestThreadSeqItemRes runRes[lockSeqLength];
    for (int i = 0; i < threadNums; ++i) {
        threads[i] = std::thread(RunThread, i, lockTags, lockNums, lockSeq, lockSeqLength, &barrier, runRes, m_ut_memory_context, nullptr, nullptr);
    }
    for (int i = 0; i < threadNums; ++i) {
        threads[i].join();
    }

    // clean
    pthread_barrier_destroy(&barrier);

    // compare
    for (int i = 0; i < observationSize; ++i) {
        auto &vec = observations[i].seqIds;
        int expectedVictimNum = observations[i].deadlockVictimNum;
        int realVictimNum = 0;
        for (auto seqId : vec) {
            auto& res = runRes[seqId];
            if (STORAGE_FUNC_FAIL(res.realRes)) {
                EXPECT_EQ(res.realErrorCode, LOCK_ERROR_DEADLOCK);
                realVictimNum++;
            } else {
                EXPECT_TRUE(STORAGE_FUNC_SUCC(res.realRes));
                EXPECT_EQ(res.realErrorCode, STORAGE_OK);
            }
        }
        EXPECT_EQ(realVictimNum, expectedVictimNum);
    }
}

TEST_F(LockMgrDeadlockTest, DeadLockDetectionTestMultiThread_level0)
{
    // input
    constexpr int threadNums = 6;
    LockTag lockTags[] = {
        CreateTableLockTag(g_defaultPdbId, 1),
        CreateTableLockTag(g_defaultPdbId, 2),
        CreateTableLockTag(g_defaultPdbId, 3),
    };
    LockTestSeqItem lockSeq[] = {
        LockTestSeqItem{0, 0, ACT_LOCK, DSTORE_ACCESS_EXCLUSIVE_LOCK},
        LockTestSeqItem{1, 1, ACT_LOCK, DSTORE_ACCESS_EXCLUSIVE_LOCK},
        LockTestSeqItem{2, 2, ACT_LOCK, DSTORE_ACCESS_EXCLUSIVE_LOCK, true},
        LockTestSeqItem{3, 0, ACT_LOCK, DSTORE_ACCESS_EXCLUSIVE_LOCK},
        LockTestSeqItem{4, 1, ACT_LOCK, DSTORE_ACCESS_EXCLUSIVE_LOCK},
        LockTestSeqItem{5, 2, ACT_LOCK, DSTORE_ACCESS_EXCLUSIVE_LOCK},
        LockTestSeqItem{2, 0, ACT_LOCK, DSTORE_ACCESS_EXCLUSIVE_LOCK},
        LockTestSeqItem{0, 1, ACT_LOCK, DSTORE_ACCESS_EXCLUSIVE_LOCK},
        LockTestSeqItem{1, 2, ACT_LOCK, DSTORE_ACCESS_EXCLUSIVE_LOCK},
    };
    LockTestObservationPoint observations[] = {
        LockTestObservationPoint{{0}, 0},
        LockTestObservationPoint{{1}, 0},
        LockTestObservationPoint{{2}, 0},
        LockTestObservationPoint{{3, 4, 5}, 0},
        LockTestObservationPoint{{6, 7, 8}, 1},
    };
    constexpr int lockNums = sizeof(lockTags) / sizeof(LockTag);
    constexpr int lockSeqLength = sizeof(lockSeq) / sizeof(LockTestSeqItem);
    constexpr int observationSize = sizeof(observations) / sizeof(LockTestObservationPoint);

    // run
    pthread_barrier_t barrier;
    pthread_barrier_init(&barrier, nullptr, threadNums);
    std::thread threads[threadNums];
    LockTestThreadSeqItemRes runRes[lockSeqLength];
    for (int i = 0; i < threadNums; ++i) {
        threads[i] = std::thread(RunThread, i, lockTags, lockNums, lockSeq, lockSeqLength, &barrier, runRes, m_ut_memory_context, nullptr, nullptr);
    }
    for (int i = 0; i < threadNums; ++i) {
        threads[i].join();
    }

    // clean
    pthread_barrier_destroy(&barrier);

    // compare
    for (int i = 0; i < observationSize; ++i) {
        auto &vec = observations[i].seqIds;
        int expectedVictimNum = observations[i].deadlockVictimNum;
        int realVictimNum = 0;
        for (auto seqId : vec) {
            auto& res = runRes[seqId];
            if (STORAGE_FUNC_FAIL(res.realRes)) {
                EXPECT_EQ(res.realErrorCode, LOCK_ERROR_DEADLOCK);
                realVictimNum++;
            } else {
                EXPECT_TRUE(STORAGE_FUNC_SUCC(res.realRes));
                EXPECT_EQ(res.realErrorCode, STORAGE_OK);
            }
        }
        EXPECT_EQ(realVictimNum, expectedVictimNum);
    }
}

TEST_F(LockMgrDeadlockTest, DeadLockDetectionTestWithSoftEdge_level0)
{
    // input
    constexpr int threadNums = 3;
    LockTag lockTags[] = {
        CreateTableLockTag(g_defaultPdbId, 1),
        CreateTableLockTag(g_defaultPdbId, 2),
    };
    LockTestSeqItem lockSeq[] = {
        LockTestSeqItem{0, 0, ACT_LOCK, DSTORE_ACCESS_SHARE_LOCK},
        LockTestSeqItem{1, 1, ACT_LOCK, DSTORE_ACCESS_SHARE_LOCK, true},
        LockTestSeqItem{2, 0, ACT_LOCK, DSTORE_ACCESS_EXCLUSIVE_LOCK},
        LockTestSeqItem{0, 1, ACT_LOCK, DSTORE_ACCESS_EXCLUSIVE_LOCK},
        LockTestSeqItem{1, 0, ACT_LOCK, DSTORE_ACCESS_SHARE_LOCK, false, 2},
    };
    LockTestObservationPoint observations[] = {
        LockTestObservationPoint{{0}, 0},
        LockTestObservationPoint{{1}, 0},
        LockTestObservationPoint{{2, 3, 4}, 1},
    };
    constexpr int lockNums = sizeof(lockTags) / sizeof(LockTag);
    constexpr int lockSeqLength = sizeof(lockSeq) / sizeof(LockTestSeqItem);
    constexpr int observationSize = sizeof(observations) / sizeof(LockTestObservationPoint);

    // run
    pthread_barrier_t barrier;
    pthread_barrier_init(&barrier, nullptr, threadNums);
    pthread_barrier_t initbBarrier;
    pthread_barrier_init(&initbBarrier, nullptr, threadNums + 1);
    std::thread threads[threadNums];
    LockTestThreadSeqItemRes runRes[lockSeqLength];
    ThreadCore* thrdCores[threadNums];
    for (int i = 0; i < threadNums; ++i) {
        threads[i] = std::thread(RunThread, i, lockTags, lockNums, lockSeq, lockSeqLength, &barrier, runRes, m_ut_memory_context, &initbBarrier, thrdCores);
    }
    pthread_barrier_wait(&initbBarrier);
    for (int i = 0; i < threadNums; ++i) {
        threads[i].join();
    }

    // clean
    pthread_barrier_destroy(&barrier);

    // compare
    for (int i = 0; i < observationSize; ++i) {
        auto &vec = observations[i].seqIds;
        int expectedVictimNum = observations[i].deadlockVictimNum;
        int realVictimNum = 0;
        for (auto seqId : vec) {
            auto& res = runRes[seqId];
            if (STORAGE_FUNC_FAIL(res.realRes)) {
                EXPECT_EQ(res.realErrorCode, LOCK_ERROR_DEADLOCK);
                realVictimNum++;
            } else {
                EXPECT_TRUE(STORAGE_FUNC_SUCC(res.realRes));
                EXPECT_EQ(res.realErrorCode, STORAGE_OK);
            }
        }
        EXPECT_TRUE(realVictimNum == expectedVictimNum || realVictimNum == 0);
    }
}

TEST_F(LockMgrDeadlockTest, DeadLockDetectionTestMultiCycle_level0)
{
    // input
    constexpr int threadNums = 5;
    LockTag lockTags[] = {
        CreateTableLockTag(g_defaultPdbId, 1),
        CreateTableLockTag(g_defaultPdbId, 2),
        CreateTableLockTag(g_defaultPdbId, 3),
        CreateTableLockTag(g_defaultPdbId, 4),
        CreateTableLockTag(g_defaultPdbId, 5),
    };
    LockTestSeqItem lockSeq[] = {
        LockTestSeqItem{0, 0, ACT_LOCK, DSTORE_ACCESS_EXCLUSIVE_LOCK},
        LockTestSeqItem{1, 1, ACT_LOCK, DSTORE_ACCESS_EXCLUSIVE_LOCK},
        LockTestSeqItem{2, 2, ACT_LOCK, DSTORE_ACCESS_EXCLUSIVE_LOCK},
        LockTestSeqItem{3, 3, ACT_LOCK, DSTORE_ACCESS_EXCLUSIVE_LOCK},
        LockTestSeqItem{4, 4, ACT_LOCK, DSTORE_ACCESS_EXCLUSIVE_LOCK, true},
        LockTestSeqItem{0, 1, ACT_LOCK, DSTORE_ACCESS_EXCLUSIVE_LOCK},
        LockTestSeqItem{1, 2, ACT_LOCK, DSTORE_ACCESS_EXCLUSIVE_LOCK},
        LockTestSeqItem{2, 0, ACT_LOCK, DSTORE_ACCESS_EXCLUSIVE_LOCK},
        LockTestSeqItem{3, 4, ACT_LOCK, DSTORE_ACCESS_EXCLUSIVE_LOCK},
        LockTestSeqItem{4, 3, ACT_LOCK, DSTORE_ACCESS_EXCLUSIVE_LOCK},
    };
    LockTestObservationPoint observations[] = {
        LockTestObservationPoint{{0}, 0},
        LockTestObservationPoint{{1}, 0},
        LockTestObservationPoint{{2}, 0},
        LockTestObservationPoint{{3}, 0},
        LockTestObservationPoint{{4}, 0},
        LockTestObservationPoint{{5, 6, 7}, 1},
        LockTestObservationPoint{{8, 9}, 1},
    };
    constexpr int lockNums = sizeof(lockTags) / sizeof(LockTag);
    constexpr int lockSeqLength = sizeof(lockSeq) / sizeof(LockTestSeqItem);
    constexpr int observationSize = sizeof(observations) / sizeof(LockTestObservationPoint);

    // run
    pthread_barrier_t barrier;
    pthread_barrier_init(&barrier, nullptr, threadNums);
    std::thread threads[threadNums];
    LockTestThreadSeqItemRes runRes[lockSeqLength];
    for (int i = 0; i < threadNums; ++i) {
        threads[i] = std::thread(RunThread, i, lockTags, lockNums, lockSeq, lockSeqLength, &barrier, runRes, m_ut_memory_context, nullptr, nullptr);
    }
    for (int i = 0; i < threadNums; ++i) {
        threads[i].join();
    }

    // clean
    pthread_barrier_destroy(&barrier);

    // compare
    for (int i = 0; i < observationSize; ++i) {
        auto &vec = observations[i].seqIds;
        int expectedVictimNum = observations[i].deadlockVictimNum;
        int realVictimNum = 0;
        for (auto seqId : vec) {
            auto& res = runRes[seqId];
            if (STORAGE_FUNC_FAIL(res.realRes)) {
                EXPECT_EQ(res.realErrorCode, LOCK_ERROR_DEADLOCK);
                realVictimNum++;
            } else {
                EXPECT_TRUE(STORAGE_FUNC_SUCC(res.realRes));
                EXPECT_EQ(res.realErrorCode, STORAGE_OK);
            }
        }
        EXPECT_EQ(realVictimNum, expectedVictimNum);
    }
}


TEST_F(LockMgrDeadlockTest, DeadLockDetectionTestNoCycle_level0)
{
    // input
    constexpr int threadNums = 5;
    LockTag lockTags[] = {
        CreateTableLockTag(g_defaultPdbId, 1),
        CreateTableLockTag(g_defaultPdbId, 2),
        CreateTableLockTag(g_defaultPdbId, 3),
    };
    LockTestSeqItem lockSeq[] = {
        LockTestSeqItem{0, 0, ACT_LOCK, DSTORE_ACCESS_EXCLUSIVE_LOCK},
        LockTestSeqItem{1, 1, ACT_LOCK, DSTORE_ACCESS_EXCLUSIVE_LOCK},
        LockTestSeqItem{2, 2, ACT_LOCK, DSTORE_ACCESS_EXCLUSIVE_LOCK, true},
        LockTestSeqItem{1, 0, ACT_LOCK, DSTORE_ACCESS_EXCLUSIVE_LOCK},
        LockTestSeqItem{2, 1, ACT_LOCK, DSTORE_ACCESS_EXCLUSIVE_LOCK},
        LockTestSeqItem{3, 2, ACT_LOCK, DSTORE_ACCESS_EXCLUSIVE_LOCK},
        LockTestSeqItem{4, 1, ACT_LOCK, DSTORE_ACCESS_EXCLUSIVE_LOCK},
        LockTestSeqItem{0, 0, ACT_UNLOCK, DSTORE_ACCESS_EXCLUSIVE_LOCK},
    };
    LockTestObservationPoint observations[] = {
        LockTestObservationPoint{{0, 1, 2, 3, 4, 5, 6, 7}, 0},
    };
    constexpr int lockNums = sizeof(lockTags) / sizeof(LockTag);
    constexpr int lockSeqLength = sizeof(lockSeq) / sizeof(LockTestSeqItem);
    constexpr int observationSize = sizeof(observations) / sizeof(LockTestObservationPoint);

    // run
    pthread_barrier_t barrier;
    pthread_barrier_init(&barrier, nullptr, threadNums);
    std::thread threads[threadNums];
    LockTestThreadSeqItemRes runRes[lockSeqLength];
    for (int i = 0; i < threadNums; ++i) {
        threads[i] = std::thread(RunThread, i, lockTags, lockNums, lockSeq, lockSeqLength, &barrier, runRes, m_ut_memory_context, nullptr, nullptr);
    }
    for (int i = 0; i < threadNums; ++i) {
        threads[i].join();
    }

    // clean
    pthread_barrier_destroy(&barrier);

    // compare
    for (int i = 0; i < observationSize; ++i) {
        auto &vec = observations[i].seqIds;
        int expectedVictimNum = observations[i].deadlockVictimNum;
        int realVictimNum = 0;
        for (auto seqId : vec) {
            auto& res = runRes[seqId];
            if (STORAGE_FUNC_FAIL(res.realRes)) {
                EXPECT_EQ(res.realErrorCode, LOCK_ERROR_DEADLOCK);
                realVictimNum++;
            } else {
                EXPECT_TRUE(STORAGE_FUNC_SUCC(res.realRes));
                EXPECT_EQ(res.realErrorCode, STORAGE_OK);
            }
        }
        EXPECT_EQ(realVictimNum, expectedVictimNum);
    }
}