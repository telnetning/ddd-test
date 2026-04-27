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
#include "common/concurrent/dstore_atomic.h"
#include "common/dstore_tsan_annotation.h"
#include <pthread.h>
#include <thread>

using namespace DSTORE;

const static int UT_THREAD_COUNT = 10;
const static int UT_THREAD_LOOP_COUNT = 100000;
static pthread_barrier_t g_barrier;
static uint32 g_atomic_u32;
static int32 g_atomic_i32;
static uint64 g_atomic_u64;
static int64 g_atomic_i64;

class AtomicTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        pthread_barrier_init(&g_barrier, nullptr, UT_THREAD_COUNT);
    }
    void TearDown() override
    {
        pthread_barrier_destroy(&g_barrier);
    }
};

#pragma pack(1)
struct Uint64Unaligned {
    char m_char[126];
    gs_atomic_uint64 m_atomic_uint64;
};
#pragma pack()
static Uint64Unaligned *m_data;

void TestThreadFunAtomicReadAndWrite()
{
    static uint64 NUM1 = 0xFFFFFFFFUL;
    static uint64 NUM2 = 0x0UL;
    pthread_barrier_wait(&g_barrier);

    for (int i = 0; i < UT_THREAD_LOOP_COUNT; i++) {
        uint64 curCsn = GsAtomicReadU64(&(m_data->m_atomic_uint64));
        if ((curCsn != NUM1) && (curCsn != NUM2)) {
            printf("error value 0x%lx\n", curCsn);
        }

        if (i % 2 == 0) {
            GsAtomicWriteU64(&(m_data->m_atomic_uint64), NUM1);
        } else {
            GsAtomicWriteU64(&(m_data->m_atomic_uint64), NUM2);
        }
    }
    
    return;
}

/*
 * This test case is to show pg_atomic_read_u64() and pg_atomic_write_u64() might fail if
 * the uint64 memory cross 2 cache line.
 */
TEST_F(AtomicTest, DISABLED_pg_atomic_read_write_unaligned)
{
    void *buffer = malloc(2 * 128 + sizeof(Uint64Unaligned));
    m_data = (Uint64Unaligned *)((unsigned long)buffer / 128 * 128 + 128 + 1);
    m_data->m_atomic_uint64 = 0;
    printf("&m_atomic_uint64 mod 8 == %d, &m_atomic_uint64 mod 128 == %d\n", 
           ((unsigned long)&(m_data->m_atomic_uint64) % 8),
           ((unsigned long)&(m_data->m_atomic_uint64) % 128));

    std::thread threadArray[UT_THREAD_COUNT];

    for (int i = 0; i < UT_THREAD_COUNT; i++) {
        threadArray[i] = std::thread(TestThreadFunAtomicReadAndWrite);
    }

    for (int i = 0; i < UT_THREAD_COUNT; i++) {
        threadArray[i].join();
    }

    free(buffer);
}

void TestThreadFunAtomicAdd()
{
    pthread_barrier_wait(&g_barrier);

    for (int i = 0; i < UT_THREAD_LOOP_COUNT; i++) {
        GsAtomicAdd32(&g_atomic_i32, 1);
        GsAtomicAdd64(&g_atomic_i64, 1);
    }
    
    return;
}

TEST_F(AtomicTest, gs_atomic_add)
{
    std::thread threadArray[UT_THREAD_COUNT];

    g_atomic_i32 = 0;
    g_atomic_i64 = 0;
    
    for (int i = 0; i < UT_THREAD_COUNT; i++) {
        threadArray[i] = std::thread(TestThreadFunAtomicAdd);
    }

    for (int i = 0; i < UT_THREAD_COUNT; i++) {
        threadArray[i].join();
    }

    EXPECT_EQ(g_atomic_i32, UT_THREAD_COUNT * UT_THREAD_LOOP_COUNT);
    EXPECT_EQ(g_atomic_i64, UT_THREAD_COUNT * UT_THREAD_LOOP_COUNT);

    return;
}

void TestThreadFunAtomicCompareAndSwap32()
{
    pthread_barrier_wait(&g_barrier);

    for (int i = 0; i < UT_THREAD_LOOP_COUNT; i++) {
        /* Test case is to show CAS operation works, it's ok to read a dirty value here. */
        TsAnnotateBenignRaceSized(&g_atomic_i32, sizeof(g_atomic_i32));
        int32 oldValue = g_atomic_i32;
        while (!GsCompareAndSwap32(&g_atomic_i32, oldValue, oldValue + 1)) {
            TsAnnotateBenignRaceSized(&g_atomic_i32, sizeof(g_atomic_i32));
            oldValue = g_atomic_i32;
        }
    }

    return;
}

void TestThreadFunAtomicCompareAndSwap64()
{
    pthread_barrier_wait(&g_barrier);

    for (int i = 0; i < UT_THREAD_LOOP_COUNT; i++) {
        /* Test case is to show CAS operation works, it's ok to read a dirty value here. */
        TsAnnotateBenignRaceSized(&g_atomic_i64, sizeof(g_atomic_i64));
        int64 oldValue = g_atomic_i64;
        while (!GsCompareAndSwap64(&g_atomic_i64, oldValue, oldValue + 1)) {
            TsAnnotateBenignRaceSized(&g_atomic_i64, sizeof(g_atomic_i64));
            oldValue = g_atomic_i64;
        }
    }

    return;
}

TEST_F(AtomicTest, gs_compare_and_swap32)
{
    std::thread threadArray[UT_THREAD_COUNT];

    g_atomic_i32 = 0;
    
    for (int i = 0; i < UT_THREAD_COUNT; i++) {
        threadArray[i] = std::thread(TestThreadFunAtomicCompareAndSwap32);
    }

    for (int i = 0; i < UT_THREAD_COUNT; i++) {
        threadArray[i].join();
    }

    EXPECT_EQ(g_atomic_i32, UT_THREAD_COUNT * UT_THREAD_LOOP_COUNT);

    return;
}

TEST_F(AtomicTest, gs_compare_and_swap64)
{
    std::thread threadArray[UT_THREAD_COUNT];

    g_atomic_i64 = 0;
    
    for (int i = 0; i < UT_THREAD_COUNT; i++) {
        threadArray[i] = std::thread(TestThreadFunAtomicCompareAndSwap64);
    }

    for (int i = 0; i < UT_THREAD_COUNT; i++) {
        threadArray[i].join();
    }

    EXPECT_EQ(g_atomic_i64, UT_THREAD_COUNT * UT_THREAD_LOOP_COUNT);

    return;
}

void TestThreadFunAtomicAddFetch()
{
    pthread_barrier_wait(&g_barrier);

    for (int i = 0; i < UT_THREAD_LOOP_COUNT; i++) {
        GsAtomicAddFetchU64(&g_atomic_u64, 1);
        GsAtomicFetchAddU64(&g_atomic_u64, 1);
        GsAtomicAddFetchU32(&g_atomic_u32, 1);
        GsAtomicFetchAddU32(&g_atomic_u32, 1);
        GsAtomicSubFetchU64(&g_atomic_u64, 1);
        GsAtomicFetchSubU64(&g_atomic_u64, 1);
        GsAtomicSubFetchU32(&g_atomic_u32, 1);
        GsAtomicFetchSubU32(&g_atomic_u32, 1);
    }

    return;
}

TEST_F(AtomicTest, pg_atomic_add_sub_fetch)
{
    std::thread threadArray[UT_THREAD_COUNT];

    g_atomic_u64 = 0;
    g_atomic_u32 = 0;
    
    for (int i = 0; i < UT_THREAD_COUNT; i++) {
        threadArray[i] = std::thread(TestThreadFunAtomicAddFetch);
    }

    for (int i = 0; i < UT_THREAD_COUNT; i++) {
        threadArray[i].join();
    }

    EXPECT_EQ(g_atomic_u64, 0);
    EXPECT_EQ(g_atomic_u32, 0);

    return;
}
