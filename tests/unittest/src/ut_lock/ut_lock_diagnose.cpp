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
#include "diagnose/dstore_lock_mgr_diagnose.h"
#include "lock/dstore_lock_mgr.h"
#include "lock/dstore_table_lock_mgr.h"
#include "lock/dstore_xact_lock_mgr.h"
#include <thread>
#include <mutex>

using namespace DSTORE;

class UTLockMgrDiagnose : public DSTORETEST {
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

    static void LockWaitThread(const LockTag *tag, LockMode mode);
};

void UTLockMgrDiagnose::LockWaitThread(const LockTag *tag, LockMode mode)
{
    UtInstance *instance = (UtInstance *)g_storageInstance;
    instance->ThreadSetupAndRegister();

    RetStatus ret = g_storageInstance->GetLockMgr()->Lock(tag, mode, LOCK_WAIT, nullptr);
    EXPECT_TRUE(ret == DSTORE_SUCC);

    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    g_storageInstance->GetLockMgr()->Unlock(tag, mode);

    instance->ThreadUnregisterAndExit();
}

TEST_F(UTLockMgrDiagnose, GetLockStatusTest_level0)
{
    ASSERT_FALSE(g_storageInstance->GetLockMgr() == NULL);

    LockTag tags[LOCKTAG_MAX_NUM];
    LockMode modes[LOCKTAG_MAX_NUM];
    std::thread threads[LOCKTAG_MAX_NUM];

    for (int i = 0; i < static_cast<int>(LOCKTAG_MAX_NUM); i++) {
        tags[i].lockTagType = static_cast<LockTagType>(i);
        modes[i] = static_cast<LockMode>(i % DSTORE_LOCK_MODE_MAX);
    }

    LockErrorInfo info = {0};
    RetStatus ret;
    g_traceSwitch = TABLELOCK_STATS_TRACE_SWITCH;

    for (int i = 0; i < static_cast<int>(LOCKTAG_MAX_NUM); i++) {
        ret = g_storageInstance->GetLockMgr()->Lock(&tags[i], modes[i], LOCK_WAIT, &info);
        EXPECT_TRUE(ret == DSTORE_SUCC);
    }

    for (int i = 0; i < static_cast<int>(LOCKTAG_MAX_NUM); i++) {
        threads[i] = std::thread(LockWaitThread, &tags[i], modes[i]);
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    int lockNum = 0;
    LockStatus **lockStatus = LockMgrDiagnose::GetLocksByThread(lockNum);
    EXPECT_TRUE(lockStatus != nullptr);
    LockMgrDiagnose::FreeLockStatusArr(lockStatus, lockNum);

    for (int i = 0; i < static_cast<int>(LOCKTAG_MAX_NUM); i++) {
        g_storageInstance->GetLockMgr()->Unlock(&tags[i], modes[i]);
    }

    for (int i = 0; i < static_cast<int>(LOCKTAG_MAX_NUM); i++) {
        threads[i].join();
    }

    g_traceSwitch = 0;
}

TEST_F(UTLockMgrDiagnose, GetLockByLockTagTest_level2)
{
    ASSERT_FALSE(g_storageInstance->GetLockMgr() == NULL);

    LockTag tag;
    tag.SetControlFileLockTag(0);
    uint32 holdThreadCount = 5;
    uint32 waitThreadCount = 5;
    pthread_barrier_t barrier;
    pthread_barrier_init(&barrier, nullptr, holdThreadCount + waitThreadCount + 1);
    std::atomic<bool> isPrintFinished(false);

    auto HoldLockThread = [&tag, &barrier, &isPrintFinished]() {
        UtInstance *instance = (UtInstance *)g_storageInstance;
        instance->ThreadSetupAndRegister();

        RetStatus ret = g_storageInstance->GetLockMgr()->Lock(&tag, DSTORE_SHARE_LOCK, LOCK_WAIT, nullptr);
        EXPECT_TRUE(ret == DSTORE_SUCC);

        pthread_barrier_wait(&barrier);
        while (!isPrintFinished) {
            usleep(1000);
        }

        g_storageInstance->GetLockMgr()->Unlock(&tag, DSTORE_SHARE_LOCK);

        instance->ThreadUnregisterAndExit();
    };

    auto WaitLockThread = [&tag, &barrier]() {
        UtInstance *instance = (UtInstance *)g_storageInstance;
        instance->ThreadSetupAndRegister();

        /* Wait until hold threads all finished acquiring lock. */
        pthread_barrier_wait(&barrier);
        RetStatus ret = g_storageInstance->GetLockMgr()->Lock(&tag, DSTORE_EXCLUSIVE_LOCK, LOCK_WAIT, nullptr);
        EXPECT_TRUE(ret == DSTORE_SUCC);

        g_storageInstance->GetLockMgr()->Unlock(&tag, DSTORE_EXCLUSIVE_LOCK);

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
    context.lockTagType = LOCKTAG_CONTROL_FILE;
    pthread_barrier_wait(&barrier);
    usleep(1000 * 100);

    /* Now hold threads all get locks, and wait threads is likely to be already waiting. */
    char *lockDump = LockMgrDiagnose::GetLockByLockTag(context);
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

TEST_F(UTLockMgrDiagnose, GetTableLockByLockTagTest_level2)
{
    ASSERT_FALSE(g_storageInstance->GetTableLockMgr() == NULL);

    LockTag tag;
    tag.SetTableLockTag(0, 0);
    uint32 holdThreadCount = 5;
    uint32 waitThreadCount = 1;
    pthread_barrier_t barrier;
    pthread_barrier_init(&barrier, nullptr, holdThreadCount + waitThreadCount + 1);
    std::atomic<bool> isPrintFinished(false);

    auto HoldLockThread = [&tag, &barrier, &isPrintFinished]() {
        UtInstance *instance = (UtInstance *)g_storageInstance;
        instance->ThreadSetupAndRegister();

        RetStatus ret = g_storageInstance->GetTableLockMgr()->Lock(&tag, DSTORE_ROW_EXCLUSIVE_LOCK, LOCK_WAIT, nullptr);
        EXPECT_TRUE(ret == DSTORE_SUCC);

        pthread_barrier_wait(&barrier);
        while (!isPrintFinished) {
            usleep(1000);
        }

        g_storageInstance->GetTableLockMgr()->Unlock(&tag, DSTORE_ROW_EXCLUSIVE_LOCK);

        instance->ThreadUnregisterAndExit();
    };

    auto WaitLockThread = [&tag, &barrier, &isPrintFinished]() {
        UtInstance *instance = (UtInstance *)g_storageInstance;
        instance->ThreadSetupAndRegister();

        /* Wait until hold threads all finished acquiring lock. */
        pthread_barrier_wait(&barrier);
        RetStatus ret = g_storageInstance->GetTableLockMgr()->Lock(&tag, DSTORE_SHARE_UPDATE_EXCLUSIVE_LOCK, LOCK_WAIT, nullptr);
        EXPECT_TRUE(ret == DSTORE_SUCC);
        while (!isPrintFinished) {
            usleep(1000);
        }

        g_storageInstance->GetTableLockMgr()->Unlock(&tag, DSTORE_SHARE_UPDATE_EXCLUSIVE_LOCK);

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

TEST_F(UTLockMgrDiagnose, GetTrxInfoTest_level2)
{
    ASSERT_FALSE(g_storageInstance->GetXactLockMgr() == NULL);

    Xid xid(100, 1000);
    pthread_barrier_t barrier;
    pthread_barrier_init(&barrier, nullptr, 3);
    std::atomic<bool> isPrintFinished(false);

    auto transaction1 = [&xid, &barrier, &isPrintFinished]() {
        UtInstance *instance = (UtInstance *)g_storageInstance;
        instance->ThreadSetupAndRegister();

        RetStatus ret = g_storageInstance->GetXactLockMgr()->Lock(0, xid);
        EXPECT_TRUE(ret == DSTORE_SUCC);

        pthread_barrier_wait(&barrier);
        while (!isPrintFinished) {
            usleep(1000);
        }

        g_storageInstance->GetXactLockMgr()->Unlock(0, xid);

        instance->ThreadUnregisterAndExit();
    };

    auto transaction2 = [&xid, &barrier, &isPrintFinished]() {
        UtInstance *instance = (UtInstance *)g_storageInstance;
        instance->ThreadSetupAndRegister();

        /* Wait until transaction1 starts. */
        pthread_barrier_wait(&barrier);
        RetStatus ret = g_storageInstance->GetXactLockMgr()->Wait(0, xid);
        EXPECT_TRUE(ret == DSTORE_SUCC);
        while (!isPrintFinished) {
            usleep(1000);
        }

        instance->ThreadUnregisterAndExit();
    };

    std::thread transaction1Thread = std::thread(transaction1);
    std::thread transaction2Thread = std::thread(transaction2);

    pthread_barrier_wait(&barrier);
    usleep(1000 * 100);

    char *trxInfo = LockMgrDiagnose::GetTrxInfoFromLockMgr(0, xid.m_placeHolder);
    EXPECT_TRUE(trxInfo != nullptr);
    printf("%s\n", trxInfo);
    DstorePfreeExt(trxInfo);
    isPrintFinished = true;

    transaction1Thread.join();
    transaction2Thread.join();
}