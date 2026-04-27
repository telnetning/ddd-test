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
#include "common/concurrent/dstore_barrier.h"
#include "common/dstore_datatype.h"
#include "common/dstore_tsan_annotation.h"
#include "common/concurrent/dstore_futex.h"
#include "securec.h"
#include <thread>
#include <atomic>
using namespace DSTORE;
const static int UT_BARRIER_LOOP_COUNT = 10000;
const static int UT_SEMA_CREATE_COUNT = 4;

static int g_x = 0;
static int g_y = 0;
static int g_a = 0;
static int g_b = 0;

static std::atomic<bool> g_finish(false);

static DstoreFutex g_semaPhore1;
static DstoreFutex g_semaPhore2;
static DstoreFutex g_semaPhore3;
static DstoreFutex g_semaPhore4;

static void memFree(void *addr)
{
    if (addr != nullptr) {
        free(addr);
    }
}

class BarrierTest : public ::testing::Test {
protected:
    static void SetUpTestCase()
    {
    }
    static void TearDownTestCase()
    {
    }
    void SetUp() override
    {
        g_finish = false;
        g_semaPhore1.DstoreFutexInit();
        g_semaPhore2.DstoreFutexInit();
        g_semaPhore3.DstoreFutexInit();
        g_semaPhore4.DstoreFutexInit();
    }
    void TearDown() override
    {
    }
private:
};


static void UtBarrierThread1()
{
    while (true) {
        if (!g_semaPhore1.DstoreFutexTry()) {
            if (g_finish) {
                break;
            }
            continue;
        }

        if (g_finish) {
            return;
        }

        g_x = 1;

        GS_MEMORY_BARRIER();

        /* Even if there is race condition, test case still works. */
        TsAnnotateBenignRaceSized(&g_y, sizeof(g_y));
        g_a = g_y;

        g_semaPhore3.DstoreFutexPost();
    }
}

static void UtBarrierThread2()
{
    while (true) {
        if (!g_semaPhore2.DstoreFutexTry()) {
            if (g_finish) {
                break;
            }
            continue;
        }

        g_y = 1;

        GS_MEMORY_BARRIER();

        /* Even if there is race condition, test case still works. */
        TsAnnotateBenignRaceSized(&g_x, sizeof(g_x));
        g_b = g_x;

        g_semaPhore4.DstoreFutexPost();
    }
}

TEST_F(BarrierTest, Basic)
{
    std::thread t1(UtBarrierThread1);
    std::thread t2(UtBarrierThread2);
    int loopCount = 0;
    while (true) {
        if (loopCount >= UT_BARRIER_LOOP_COUNT) {
            g_finish = true;
            break;
        }
        g_x = 0;
        g_y = 0;
        g_a = 0;
        g_b = 0;
        g_semaPhore1.DstoreFutexPost();
        g_semaPhore2.DstoreFutexPost();

        g_semaPhore3.DstoreFutexWait(true);
        g_semaPhore4.DstoreFutexWait(true);

        ASSERT_FALSE(g_a == 0 && g_b == 0);

        loopCount++;
    }

    t1.join();
    t2.join();
}
