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
#include <gtest/gtest.h>
#include <sys/time.h>
#include <thread>
#include <mutex>
#include <random>

#include "framework/dstore_instance.h"
#include "wal/dstore_wal_utils.h"
#include "ut_utilities/ut_dstore_framework.h"
#include "ut_wal/ut_wal_buffer.h"
#include "ut_buffer/ut_buffer_fake_instance.h"
#include "ut_wal/ut_wal_basic.h"
#include "ut_mock/ut_wal_stream_mock.h"

/* these must be the same as that on the storage_wal_buffer.h */
#define BLOCK_NUM_IN_BUFFER 4
#define INSERT_STATUS_ENTRY_POW (16)
#define INSERT_STATUS_ENTRY_CNT (1 << INSERT_STATUS_ENTRY_POW)
#define RESERVE_NUM 100
constexpr uint32 PARALLEL_TEST_CNT_NUM = 3;
using namespace DSTORE;
using namespace std;

std::mutex mtx;

class WalBufferTest : public WALBASICTEST {
protected:
    void SetUp() override
    {
        SetDefaultPdbId(PDB_TEMPLATE1_ID);
        WALBASICTEST::SetUp();
        mWalBuffer = DstoreNew(g_dstoreCurrentMemoryContext)
            MockWalStreamBuffer(g_dstoreCurrentMemoryContext, BLOCK_NUM_IN_BUFFER);
        mWalBuffer->m_walFileSize = 16 * 1024 * 1024U;
        m_walFileMgr = DstoreNew(g_dstoreCurrentMemoryContext)WalFileManager(g_dstoreCurrentMemoryContext);
    }
    void TearDown() override
    {
        delete mWalBuffer;
        delete m_walFileMgr;
        WALBASICTEST::TearDown();
    }

    void ReserveAndMarkFinish(uint32 size, uint32 count, uint64 &startPlsn, uint64 &endPlsn)
    {
        for (int i = 0; i < count; i++) {
            mWalBuffer->ReserveInsertLocation(size, startPlsn, endPlsn);
            uint64 cursorPlsn = mWalBuffer->WalBytePosToPlsn((size)*i, false);
            uint64 startPlsnWithOffset = cursorPlsn % m_walFileSize == 0 ? cursorPlsn + WAL_FILE_HDR_SIZE : cursorPlsn;
            ASSERT_EQ(startPlsn, startPlsnWithOffset);
            ASSERT_EQ(endPlsn, mWalBuffer->WalBytePosToPlsn((size) * (i + 1), true));
            /* before invoking this, should be a step of converting the logical address to the physical address. */
            mWalBuffer->MarkInsertFinish(startPlsn, endPlsn);
        }
    }

    void WaitTimeAndAdvanceBuffer(uint64 endPlsn) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10000));
    }

    void CalculateGetBufferTime(uint64 endPlsn, timeval start) {
        mWalBuffer->GetBufferBlock(endPlsn);
        struct timeval end;
        gettimeofday(&end,NULL);
        double timeUse = (end.tv_sec - start.tv_sec) + (double)(end.tv_usec - start.tv_usec)/1000000.0;
        ASSERT_GT(timeUse, 10);
    }

    void WaitTimeAndFlush() {
        std::this_thread::sleep_for(std::chrono::milliseconds(10000));
        uint64 startPlsn = 0;
        uint64 endPlsn = 0;
        uint8 *res;
        bool reachBound;
        mWalBuffer->GetNextFlushData(startPlsn, endPlsn, res, reachBound);
    }

   void CalculateMarkTime(timeval start) {
        uint64 startPlsn;
        uint64 endPlsn;
        ReserveAndMarkFinish(100, INSERT_STATUS_ENTRY_CNT + 100, startPlsn, endPlsn);
        struct timeval end;
        gettimeofday(&end,NULL);
        double timeUse = (end.tv_sec - start.tv_sec) + (double)(end.tv_usec - start.tv_usec)/1000000.0;
        ASSERT_GT(timeUse, 10);
    }

    WalStreamBuffer *mWalBuffer;
    WalFileManager *m_walFileMgr;
    uint64 m_walFileSize = 16 * 1024 * 1024U;
    sem_t m_beginSem1;
    sem_t m_beginSem2;
    sem_t m_beginSem3;
    sem_t m_endSem;
};

void GetBufferMulti(WalStreamBuffer *walBuffer, int32 startPlsn) {
    ((UtInstance *)g_storageInstance)->ThreadSetupAndRegister();
    for (int32 i = startPlsn; i< startPlsn + 100; i++) {
        uint8* point = walBuffer->GetBufferBlock(i);
        ASSERT_NE(point, nullptr);
    }
    ((UtInstance *)g_storageInstance)->ThreadUnregisterAndExit();
}

/* nomal init */
TEST_F(WalBufferTest, BufferInit001)
{
    RetStatus ret = mWalBuffer->Init(0, m_walFileSize);
    ASSERT_EQ(ret, DSTORE_SUCC);

    delete mWalBuffer;
    mWalBuffer = DstoreNew(g_dstoreCurrentMemoryContext) WalStreamBuffer(g_dstoreCurrentMemoryContext, BLOCK_NUM_IN_BUFFER);
    ret = mWalBuffer->Init(WAL_FILE_HDR_SIZE, m_walFileSize);
    ASSERT_EQ(ret, DSTORE_SUCC);
}

/* nomal init */
TEST_F(WalBufferTest, BufferInit002)
{
    RetStatus ret = mWalBuffer->Init(10000, m_walFileSize);
    ASSERT_EQ(ret, DSTORE_SUCC);
}

TEST_F(WalBufferTest, BufferInit003)
{
    RetStatus ret = mWalBuffer->Init(0, m_walFileSize);
    ASSERT_EQ(ret, DSTORE_SUCC);

    uint64 startPlsn;
    uint64 endPlsn;
    uint64 newStart;
    int size = 100;
    /* the loop temporarily ignores the impact of the segment header. */
    int count = 100;
    ReserveAndMarkFinish(size, count, startPlsn, endPlsn);
    uint8 *res;
    uint64 startFlushLsn;
    uint64 endFlushLsn;
    bool reachBound;
    mWalBuffer->GetNextFlushData(startFlushLsn, endFlushLsn, res, reachBound);
    ASSERT_EQ(startFlushLsn, 0);
    newStart = mWalBuffer->WalPlsnToBytePos(endFlushLsn);

    /* if you want to test init again with no delete, need open StorageAssert */
    delete mWalBuffer;
    mWalBuffer = DstoreNew(g_dstoreCurrentMemoryContext) WalStreamBuffer(g_dstoreCurrentMemoryContext, BLOCK_NUM_IN_BUFFER);
    ret = mWalBuffer->Init(endFlushLsn, m_walFileSize);
    ASSERT_EQ(ret, DSTORE_SUCC);

    for (int i = 0;i < count; i++) {
        mWalBuffer->ReserveInsertLocation(size, startPlsn, endPlsn);
        ASSERT_EQ(startPlsn, mWalBuffer->WalBytePosToPlsn(newStart + (size) * i, false));
        ASSERT_EQ(endPlsn, mWalBuffer->WalBytePosToPlsn(newStart + (size) * (i + 1), true));

        /* actual, before invoking this, should be a step of converting the logical address to the physical address. */
        mWalBuffer->MarkInsertFinish(i, endPlsn);
    }

    mWalBuffer->GetNextFlushData(startFlushLsn, endFlushLsn, res, reachBound);
    ASSERT_EQ(startFlushLsn, mWalBuffer->WalBytePosToPlsn(newStart, false));
}

/* multiple reservation in a single thread */
TEST_F(WalBufferTest, BufferReserve001)
{
    RetStatus ret = mWalBuffer->Init(0, m_walFileSize);
    ASSERT_EQ(ret, DSTORE_SUCC);

    uint64 startPlsn;
    uint64 endPlsn;
    ReserveAndMarkFinish(100, 600, startPlsn, endPlsn);
}

/* reserve if endplsn at the end of the segment */
TEST_F(WalBufferTest, BufferReserve003)
{
    RetStatus ret = mWalBuffer->Init(0, m_walFileSize);
    ASSERT_EQ(ret, DSTORE_SUCC);

    uint32 size = WAL_BLCKSZ - WAL_FILE_HDR_SIZE;
    uint64 startPlsn;
    uint64 endPlsn;
    mWalBuffer->ReserveInsertLocation(size, startPlsn, endPlsn);
    ASSERT_EQ(startPlsn, WAL_FILE_HDR_SIZE);
    ASSERT_EQ(endPlsn, WAL_BLCKSZ);

    size += WAL_FILE_HDR_SIZE;
    for (int i = 0; i < m_walFileSize / WAL_BLCKSZ - 1; i++) {
        mWalBuffer->ReserveInsertLocation(size, startPlsn, endPlsn);
        ASSERT_EQ(startPlsn, WAL_BLCKSZ * (i + 1));
        ASSERT_EQ(endPlsn, WAL_BLCKSZ * (i + 2));
    }

    size = size - WAL_FILE_HDR_SIZE;
    mWalBuffer->ReserveInsertLocation(size, startPlsn, endPlsn);
    ASSERT_EQ(startPlsn, m_walFileSize + WAL_FILE_HDR_SIZE);
    ASSERT_EQ(endPlsn, m_walFileSize + WAL_BLCKSZ);
}

void AsyncMarkFlushFinish(WalStreamBuffer *walBuffer) {
    ((UtInstance *)g_storageInstance)->ThreadSetupAndRegister();
    std::this_thread::sleep_for(std::chrono::milliseconds(2000));
    ((UtInstance *)g_storageInstance)->ThreadUnregisterAndExit();
}

/* input lsn is right, direct return */
TEST_F(WalBufferTest, BufferGetBuffer002)
{
    RetStatus ret = mWalBuffer->Init(0, m_walFileSize);
    ASSERT_EQ(ret, DSTORE_SUCC);
    mWalBuffer->GetBufferBlock(100);
    mWalBuffer->GetBufferBlock(200);
    mWalBuffer->GetBufferBlock(300);
}

/* multi thread GetBuffer right */
TEST_F(WalBufferTest, BufferGetBuffer003)
{
    RetStatus ret = mWalBuffer->Init(0, m_walFileSize);
    ASSERT_EQ(ret, DSTORE_SUCC);

    std::thread t1(GetBufferMulti, mWalBuffer, 0);
    std::thread t2(GetBufferMulti, mWalBuffer, 100);
    std::thread t3(GetBufferMulti, mWalBuffer, 200);
    std::thread t4(GetBufferMulti, mWalBuffer, 300);
    std::thread t5(GetBufferMulti, mWalBuffer, 400);
    std::thread t6(GetBufferMulti, mWalBuffer, 500);
    t1.join();
    t2.join();
    t3.join();
    t4.join();
    t5.join();
    t6.join();
}

/* normal mark */
TEST_F(WalBufferTest, BufferMarkInsertFinish001)
{
    RetStatus ret = mWalBuffer->Init(0, m_walFileSize);
    ASSERT_EQ(ret, DSTORE_SUCC);

    mWalBuffer->MarkInsertFinish(1, 1000);
}

/* active flush buffer with include data, but all have flush, no new data come */
TEST_F(WalBufferTest, BufferFlush001)
{
    RetStatus ret = mWalBuffer->Init(0, m_walFileSize);
    ASSERT_EQ(ret, DSTORE_SUCC);

    uint64 startPlsn;
    uint64 endPlsn;
    ReserveAndMarkFinish(100, 100, startPlsn, endPlsn);

    uint8 *res;
    uint64 startFlushLsn;
    uint64 endFlushLsn;
    bool reachBound;
    mWalBuffer->GetNextFlushData(startFlushLsn, endFlushLsn, res, reachBound);
    ASSERT_EQ(startFlushLsn, 0);

    mWalBuffer->GetNextFlushData(startFlushLsn, endFlushLsn, res, reachBound);
    ASSERT_EQ(startFlushLsn, endFlushLsn);
}

/* consecutive and the last entry is in copied state, entry->endPlsn before target plsn */
TEST_F(WalBufferTest, BufferFlush002)
{
    RetStatus ret = mWalBuffer->Init(0, m_walFileSize);
    ASSERT_EQ(ret, DSTORE_SUCC);

    uint64 startPlsn;
    uint64 endPlsn;
    ReserveAndMarkFinish(100, 100, startPlsn, endPlsn);

    uint8 *res;
    uint64 startFlushLsn;
    uint64 endFlushLsn;
    bool reachBound;
    mWalBuffer->GetNextFlushData(startFlushLsn, endFlushLsn, res, reachBound);
    ASSERT_EQ(startFlushLsn, 0);
}

/* consecutive and the last entry is in copied state, entry->endPlsn after target plsn, but in same block */
TEST_F(WalBufferTest, BufferFlush003)
{
    RetStatus ret = mWalBuffer->Init(0, m_walFileSize);
    ASSERT_EQ(ret, DSTORE_SUCC);

    uint64 startPlsn;
    uint64 endPlsn;
    ReserveAndMarkFinish(100, 100, startPlsn, endPlsn);

    uint8 *res;
    uint64 startFlushLsn;
    uint64 endFlushLsn;
    bool reachBound;
    mWalBuffer->GetNextFlushData(startFlushLsn, endFlushLsn, res, reachBound);
    ASSERT_EQ(startFlushLsn, 0);
    ASSERT_EQ(endFlushLsn, endPlsn);
}

/* consecutive and the last entry is in copied state, entry->endPlsn after target plsn, in different block */
TEST_F(WalBufferTest, BufferFlush004)
{
    RetStatus ret = mWalBuffer->Init(0, m_walFileSize);
    ASSERT_EQ(ret, DSTORE_SUCC);

    uint64 startPlsn;
    uint64 endPlsn;
    ReserveAndMarkFinish(100, 240, startPlsn, endPlsn);

    uint8 *res;
    uint64 startFlushLsn;
    uint64 endFlushLsn;
    bool reachBound;
    mWalBuffer->GetNextFlushData(startFlushLsn, endFlushLsn, res, reachBound);
    ASSERT_EQ(startFlushLsn, 0);
}

/* entry need active flush with part end and part start, endPlsn is buffer end */
TEST_F(WalBufferTest, BufferFlush006)
{
    RetStatus ret = mWalBuffer->Init(0, m_walFileSize);
    ASSERT_EQ(ret, DSTORE_SUCC);

    uint64 startPlsn;
    uint64 endPlsn;
    int size = 100;
    /* the loop temporarily ignores the impact of the segment header. */
    int count = 100;
    ReserveAndMarkFinish(size, count, startPlsn, endPlsn);
    uint8 *res;
    uint64 startFlushLsn;
    uint64 endFlushLsn;
    bool reachBound;
    mWalBuffer->GetNextFlushData(startFlushLsn, endFlushLsn, res, reachBound);
    ASSERT_EQ(startFlushLsn, 0);
    ASSERT_EQ(endFlushLsn, endPlsn);

    uint64 newStart = endFlushLsn;
    /* the loop temporarily ignores the impact of the segment header. */
    for (int i = 0;i < INSERT_STATUS_ENTRY_CNT; i++) {
        mWalBuffer->ReserveInsertLocation(size, startPlsn, endPlsn);
        ASSERT_EQ(startPlsn, mWalBuffer->WalBytePosToPlsn((size) * (i + count), false));
        ASSERT_EQ(endPlsn, mWalBuffer->WalBytePosToPlsn((size) * (i + count + 1), true));
        // actual, before invoking this, should be a step of converting the logical address to the physical address.
        mWalBuffer->MarkInsertFinish((i + count), endPlsn);
    }

    mWalBuffer->GetNextFlushData(startFlushLsn, endFlushLsn, res, reachBound);
    ASSERT_EQ(startFlushLsn, newStart);
    ASSERT_EQ(endFlushLsn, BLOCK_NUM_IN_BUFFER * WAL_BLCKSZ);

    mWalBuffer->GetNextFlushData(startFlushLsn, endFlushLsn, res, reachBound);
    ASSERT_EQ(startFlushLsn, BLOCK_NUM_IN_BUFFER * WAL_BLCKSZ);
    ASSERT_EQ(endFlushLsn, BLOCK_NUM_IN_BUFFER * WAL_BLCKSZ * 2);
}

/* entry that need period flush in same block, startPlsn and endPlsn are equal */
TEST_F(WalBufferTest, BufferFlush007)
{
    RetStatus ret = mWalBuffer->Init(0, m_walFileSize);
    ASSERT_EQ(ret, DSTORE_SUCC);

    uint64 startPlsn;
    uint64 endPlsn;
    ReserveAndMarkFinish(100, 5, startPlsn, endPlsn);

    uint8 *res;
    uint64 startFlushLsn;
    uint64 endFlushLsn;
    bool reachBound;
    mWalBuffer->GetNextFlushData(startFlushLsn, endFlushLsn, res, reachBound);
    ASSERT_EQ(startFlushLsn, 0);

    for (int i = 5;i < 255; i++) {
        mWalBuffer->ReserveInsertLocation(100, startPlsn, endPlsn);
        ASSERT_EQ(startPlsn, mWalBuffer->WalBytePosToPlsn((100) * i, false));
        ASSERT_EQ(endPlsn, mWalBuffer->WalBytePosToPlsn((100) * (i + 1), true));
        // actual, before invoking this, should be a step of converting the logical address to the physical address.
        mWalBuffer->MarkInsertFinish(startPlsn, endPlsn);
    }
    mWalBuffer->GetNextFlushData(startFlushLsn, endFlushLsn, res, reachBound);
}

/* period flush with buffer end and flip buffer start */
TEST_F(WalBufferTest, BufferFlush009)
{
    RetStatus ret = mWalBuffer->Init(0, m_walFileSize);
    ASSERT_EQ(ret, DSTORE_SUCC);

    uint64 startPlsn;
    uint64 endPlsn;
    int size = 100;
    /* the loop temporarily ignores the impact of the segment header. */
    int count = BLOCK_NUM_IN_BUFFER * WAL_BLCKSZ / (size) - 80;
    ReserveAndMarkFinish(size, count, startPlsn, endPlsn);
    uint8 *res;
    uint64 startFlushLsn;
    uint64 endFlushLsn;
    bool reachBound;
    mWalBuffer->GetNextFlushData(startFlushLsn, endFlushLsn, res, reachBound);
    ASSERT_EQ(startFlushLsn, 0);

    uint64 newStart = endFlushLsn;
    /* the loop temporarily ignores the impact of the segment header. */
    for (int i = 0;i < BLOCK_NUM_IN_BUFFER * WAL_BLCKSZ / (size) + 80 - count; i++) {
        mWalBuffer->ReserveInsertLocation(size, startPlsn, endPlsn);
        ASSERT_EQ(startPlsn, mWalBuffer->WalBytePosToPlsn((size) * (i + count), false));
        ASSERT_EQ(endPlsn, mWalBuffer->WalBytePosToPlsn((size) * (i + count + 1), true));
        // actual, before invoking this, should be a step of converting the logical address to the physical address.
        mWalBuffer->MarkInsertFinish(startPlsn, endPlsn);
    }
    mWalBuffer->GetNextFlushData(startFlushLsn, endFlushLsn, res, reachBound);
    ASSERT_EQ(startFlushLsn, newStart);
    ASSERT_EQ(endFlushLsn, BLOCK_NUM_IN_BUFFER * WAL_BLCKSZ);

    mWalBuffer->GetNextFlushData(startFlushLsn, endFlushLsn, res, reachBound);
    ASSERT_EQ(startFlushLsn, BLOCK_NUM_IN_BUFFER * WAL_BLCKSZ);
}

/* period flush with single wal is bigger than 2 * WAL_BLCKSZ */
TEST_F(WalBufferTest, BufferFlush010)
{
    RetStatus ret = mWalBuffer->Init(0, m_walFileSize);
    ASSERT_EQ(ret, DSTORE_SUCC);

    uint64 startPlsn;
    uint64 endPlsn;
    uint64 newStart;

    int size = 100;
    /* the loop temporarily ignores the impact of the segment header. */
    ReserveAndMarkFinish(size, 10, startPlsn, endPlsn);
    newStart = endPlsn;

    mWalBuffer->ReserveInsertLocation(WAL_BLCKSZ * 2 + 100, startPlsn, endPlsn);
    ASSERT_EQ(startPlsn, newStart);
    ASSERT_EQ(endPlsn, newStart + (WAL_BLCKSZ * 2 + 100));
    // actual, before invoking this, should be a step of converting the logical address to the physical address.
    mWalBuffer->MarkInsertFinish(startPlsn, endPlsn);

    uint8 *res;
    uint64 startFlushLsn;
    uint64 endFlushLsn;
    bool reachBound;
    mWalBuffer->GetNextFlushData(startFlushLsn, endFlushLsn, res, reachBound);
    ASSERT_EQ(startFlushLsn, 0);
}

/* period flush and active flush are used interactively */
TEST_F(WalBufferTest, BufferFlush011)
{
    RetStatus ret = mWalBuffer->Init(0, m_walFileSize);
    ASSERT_EQ(ret, DSTORE_SUCC);

    /* step 1: active flush on first page with not enough */
    uint64 startPlsn;
    uint64 endPlsn;
    int size = 100;
    /* the loop temporarily ignores the impact of the segment header. */
    int count = 40;
    ReserveAndMarkFinish(size, count, startPlsn, endPlsn);
    uint8 *res;
    uint64 startFlushLsn;
    uint64 endFlushLsn;
    uint64 newStart = 0;
    bool reachBound;
    mWalBuffer->GetNextFlushData(startFlushLsn, endFlushLsn, res, reachBound);
    ASSERT_EQ(startFlushLsn, 0);
    ASSERT_EQ(endFlushLsn, endPlsn);
    newStart = endFlushLsn;

    /* step 2: period flush with first, second and third page enough, fourth page not enough */
    for (int i = count;i < 240; i++) {
        mWalBuffer->ReserveInsertLocation(size, startPlsn, endPlsn);
        ASSERT_EQ(startPlsn, mWalBuffer->WalBytePosToPlsn((size) * i, false));
        ASSERT_EQ(endPlsn, mWalBuffer->WalBytePosToPlsn((size) * (i + 1), true));
        // actual, before invoking this, should be a step of converting the logical address to the physical address.
        mWalBuffer->MarkInsertFinish(startPlsn, endPlsn);
    }
    mWalBuffer->GetNextFlushData(startFlushLsn, endFlushLsn, res, reachBound);
    ASSERT_EQ(startFlushLsn, newStart);

    newStart = endFlushLsn;
    /* step 3: period flush with fourth page not enough */
    for (int i = 240;i < 290; i++) {
        mWalBuffer->ReserveInsertLocation(size, startPlsn, endPlsn);
        ASSERT_EQ(startPlsn, mWalBuffer->WalBytePosToPlsn((size) * i, false));
        ASSERT_EQ(endPlsn, mWalBuffer->WalBytePosToPlsn((size) * (i + 1), true));
        // actual, before invoking this, should be a step of converting the logical address to the physical address.
        mWalBuffer->MarkInsertFinish(startPlsn, endPlsn);
    }
    mWalBuffer->GetNextFlushData(startFlushLsn, endFlushLsn, res, reachBound);
    ASSERT_EQ(startFlushLsn, newStart);
    newStart = endFlushLsn;

    /* step 4: active flush with fourth page not enough */
    for (int i = 290;i < 300; i++) {
        mWalBuffer->ReserveInsertLocation(size, startPlsn, endPlsn);
        ASSERT_EQ(startPlsn, mWalBuffer->WalBytePosToPlsn((size) * i, false));
        ASSERT_EQ(endPlsn, mWalBuffer->WalBytePosToPlsn((size) * (i + 1), true));
        // actual, before invoking this, should be a step of converting the logical address to the physical address.
        mWalBuffer->MarkInsertFinish(startPlsn, endPlsn);
    }
    mWalBuffer->GetNextFlushData(startFlushLsn, endFlushLsn, res, reachBound);
    ASSERT_EQ(startFlushLsn, newStart);
    newStart = endFlushLsn;

    /* step 5: period flush with fourth page enough, and flip first page enough */
    for (int i = 300;i < 400; i++) {
        mWalBuffer->ReserveInsertLocation(size, startPlsn, endPlsn);
        ASSERT_EQ(startPlsn, mWalBuffer->WalBytePosToPlsn((size) * i, false));
        ASSERT_EQ(endPlsn, mWalBuffer->WalBytePosToPlsn((size) * (i + 1), true));
        // actual, before invoking this, should be a step of converting the logical address to the physical address.
        mWalBuffer->MarkInsertFinish(startPlsn, endPlsn);
    }
    mWalBuffer->GetNextFlushData(startFlushLsn, endFlushLsn, res, reachBound);
    ASSERT_EQ(startFlushLsn, newStart);
    ASSERT_EQ(endFlushLsn, BLOCK_NUM_IN_BUFFER * WAL_BLCKSZ);
    newStart = endFlushLsn;

    mWalBuffer->GetNextFlushData(startFlushLsn, endFlushLsn, res, reachBound);
    ASSERT_EQ(startFlushLsn, newStart);
    newStart = endFlushLsn;

    mWalBuffer->GetNextFlushData(startFlushLsn, endFlushLsn, res, reachBound);
    ASSERT_EQ(startFlushLsn, newStart);
}

void *WalBufferMarkInsertFinishThread1(WalBufferTest *para)
{
    WalUtils::SignalBlock();
    (void)g_storageInstance->CreateThreadAndRegister();
    constexpr size_t binCpuTarget = 1;
    cpu_set_t cpuSet;
    CPU_ZERO(&cpuSet);
    CPU_SET(binCpuTarget, &cpuSet);
    int rc = sched_setaffinity(0, sizeof(cpu_set_t), &cpuSet);
    if (rc == -1) {
        ErrLog(DSTORE_WARNING, MODULE_WAL,
            ErrMsg("WalBufferMarkInsertFinishThread1 bind cpu failed, ErrorCode:%d.", rc));
    }
    std::mt19937 mtRand(std::random_device{}());
    uint32 round = 0;
    uint64 startPlsn = 0;
    uint64 endPlsn = 0;

    for (uint32 i = 0; i < PARALLEL_TEST_CNT_NUM; i++) {
        sem_wait(&para->m_beginSem1);
        while (mtRand() % 8 != 0) {
        }

        startPlsn = para->mWalBuffer->WalBytePosToPlsn((2 * i) * WAL_BLCKSZ, false);
        endPlsn = para->mWalBuffer->WalBytePosToPlsn((2 * i + 1) * WAL_BLCKSZ, true);
        para->mWalBuffer->MarkInsertFinish(startPlsn, endPlsn);
        sem_post(&para->m_endSem);
    }
    g_storageInstance->UnregisterThread();
    return NULL;
}

void *WalBufferMarkInsertFinishThread2(WalBufferTest *para)
{
    WalUtils::SignalBlock();
    (void)g_storageInstance->CreateThreadAndRegister();
    constexpr size_t binCpuTarget = 2;
    cpu_set_t cpuSet;
    CPU_ZERO(&cpuSet);
    CPU_SET(binCpuTarget, &cpuSet);
    int rc = sched_setaffinity(0, sizeof(cpu_set_t), &cpuSet);
    if (rc == -1) {
        ErrLog(DSTORE_WARNING, MODULE_WAL,
            ErrMsg("WalBufferMarkInsertFinishThread2 bind cpu failed, ErrorCode:%d.", rc));
    }
    std::mt19937 mtRand(std::random_device{}());

    uint64 startPlsn = 0;
    uint64 endPlsn = 0;
    for (uint32 i = 0; i < PARALLEL_TEST_CNT_NUM; i++) {
        sem_wait(&para->m_beginSem2);
        while (mtRand() % 8 != 0) {
        }
        startPlsn = para->mWalBuffer->WalBytePosToPlsn((2 * i + 1) * WAL_BLCKSZ, false);
        endPlsn = para->mWalBuffer->WalBytePosToPlsn((2 * i + 2) * WAL_BLCKSZ, true);
        para->mWalBuffer->MarkInsertFinish(startPlsn, endPlsn);

        sem_post(&para->m_endSem);
    }
    g_storageInstance->UnregisterThread();
    return NULL;
}

void *WalBufferGetNextFlushDataThread(WalBufferTest *para)
{
    WalUtils::SignalBlock();
    (void)g_storageInstance->CreateThreadAndRegister();
    constexpr size_t binCpuTarget = 3;
    cpu_set_t cpuSet;
    CPU_ZERO(&cpuSet);
    CPU_SET(binCpuTarget, &cpuSet);
    int rc = sched_setaffinity(0, sizeof(cpu_set_t), &cpuSet);
    if (rc == -1) {
        ErrLog(DSTORE_WARNING, MODULE_WAL,
            ErrMsg("WalBufferGetNextFlushDataThread bind cpu failed, ErrorCode:%d.", rc));
    }
    std::mt19937 mtRand(std::random_device{}());

    for (uint32 i = 0; i < PARALLEL_TEST_CNT_NUM; i++) {
        sem_wait(&para->m_beginSem3);
        while (mtRand() % 8 != 0) {
        }
        uint64 startPlsn = 0;
        uint64 endPlsn = 0;
        uint8 *data = nullptr;
        bool reachBufferTail;

        while (data == nullptr || endPlsn < para->mWalBuffer->WalBytePosToPlsn((2 * i + 2) * WAL_BLCKSZ, true)) {
            para->mWalBuffer->GetNextFlushData(startPlsn, endPlsn, data, reachBufferTail);
        }

        EXPECT_EQ(endPlsn, para->mWalBuffer->WalBytePosToPlsn((2 * i + 2) * WAL_BLCKSZ, true));
        sem_post(&para->m_endSem);
    }
    g_storageInstance->UnregisterThread();
    return NULL;
}

TEST_F(WalBufferTest, ParallelWorkerTestConcurrency)
{
    RetStatus ret = mWalBuffer->Init(0, m_walFileSize);
    ASSERT_EQ(ret, DSTORE_SUCC);

    sem_init(&m_beginSem1, 0, 0);
    sem_init(&m_beginSem2, 0, 0);
    sem_init(&m_beginSem3, 0, 0);
    sem_init(&m_endSem, 0, 0);
    GS_MEMORY_BARRIER();

    std::thread t1(WalBufferMarkInsertFinishThread1, this);
    std::thread t2(WalBufferMarkInsertFinishThread2, this);
    std::thread t3(WalBufferGetNextFlushDataThread, this);
    for (uint32 i = 0; i < PARALLEL_TEST_CNT_NUM; i++) {
        sem_post(&m_beginSem1);
        sem_post(&m_beginSem2);
        sem_post(&m_beginSem3);
        sem_wait(&m_endSem);
        sem_wait(&m_endSem);
        sem_wait(&m_endSem);
    }

    t1.join();
    t2.join();
    t3.join();
}

TEST_F(WalBufferTest, BufferInitAtWalFileHeadTail)
{
    WalStreamBuffer *walBuffer;
    walBuffer = DstoreNew(g_dstoreCurrentMemoryContext)
        MockWalStreamBuffer(g_dstoreCurrentMemoryContext, BLOCK_NUM_IN_BUFFER);
    walBuffer->m_walFileSize = 16 * 1024 * 1024U;
    RetStatus ret = walBuffer->Init(WAL_FILE_HDR_SIZE, m_walFileSize);
    ASSERT_EQ(ret, DSTORE_SUCC);

    uint64 startPlsn = 0;
    uint64 endPlsn = 0;
    uint8 *data = nullptr;
    bool reachBufferTail;
    walBuffer->GetNextFlushData(startPlsn, endPlsn, data, reachBufferTail);

    ASSERT_EQ(endPlsn, WAL_FILE_HDR_SIZE);
    MockWalStream walStream(1, g_dstoreCurrentMemoryContext, m_walFileMgr, m_pdbId);
    walStream.Init(walBuffer);
    walStream.Flush();
}

TEST_F(WalBufferTest, WalBytePosToPlsnTest)
{
    ASSERT_EQ(WAL_FILE_HDR_SIZE, mWalBuffer->WalBytePosToPlsn(0, false));
    ASSERT_EQ(WAL_FILE_HDR_SIZE + m_walFileSize - 100, mWalBuffer->WalBytePosToPlsn(m_walFileSize - 100, false));

    /* same bytepos value but different position */
    ASSERT_EQ(m_walFileSize, mWalBuffer->WalBytePosToPlsn(m_walFileSize - WAL_FILE_HDR_SIZE, true));
    ASSERT_EQ(m_walFileSize + WAL_FILE_HDR_SIZE, mWalBuffer->WalBytePosToPlsn(m_walFileSize - WAL_FILE_HDR_SIZE, false));

    ASSERT_EQ(WAL_FILE_HDR_SIZE * 2 + m_walFileSize + 100, mWalBuffer->WalBytePosToPlsn(m_walFileSize + 100, false));
    ASSERT_EQ(m_walFileSize * 2, mWalBuffer->WalBytePosToPlsn((m_walFileSize - WAL_FILE_HDR_SIZE) * 2, true));
}

TEST_F(WalBufferTest, WalPlsnToBytePosTest)
{
    ASSERT_EQ(0, mWalBuffer->WalPlsnToBytePos(0));
    ASSERT_EQ(0, mWalBuffer->WalPlsnToBytePos(WAL_FILE_HDR_SIZE - 1));
    ASSERT_EQ(0, mWalBuffer->WalPlsnToBytePos(WAL_FILE_HDR_SIZE));
    ASSERT_EQ(m_walFileSize - WAL_FILE_HDR_SIZE - 100, mWalBuffer->WalPlsnToBytePos(m_walFileSize - 100));

    ASSERT_EQ(m_walFileSize - WAL_FILE_HDR_SIZE, mWalBuffer->WalPlsnToBytePos(m_walFileSize));
    ASSERT_EQ(m_walFileSize - WAL_FILE_HDR_SIZE, mWalBuffer->WalPlsnToBytePos(m_walFileSize + 1));
    ASSERT_EQ(m_walFileSize - WAL_FILE_HDR_SIZE, mWalBuffer->WalPlsnToBytePos(m_walFileSize + WAL_FILE_HDR_SIZE));

    ASSERT_EQ((m_walFileSize - WAL_FILE_HDR_SIZE) * 2 - 100, mWalBuffer->WalPlsnToBytePos(m_walFileSize * 2 - 100));
    ASSERT_EQ((m_walFileSize - WAL_FILE_HDR_SIZE) * 2, mWalBuffer->WalPlsnToBytePos(m_walFileSize * 2));
}
