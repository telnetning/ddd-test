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
#include "common/concurrent/dstore_futex.h"
#include "common/dstore_datatype.h"
#include "framework/dstore_thread.h"
#include "securec.h"
#include <thread>
#include <pthread.h>
#include <mutex>

using namespace DSTORE;

const static int UT_SPIN_LOCK_THREAD_COUNT = 10;
const static int UT_SPIN_LOCK_LOOP_COUNT = 100000;
static pthread_barrier_t g_barrier;
static DstoreSpinLock g_spinLock;
static int g_count = 0;
static std::mutex g_mutex;

static void memFree(void *addr)
{
    if (addr != nullptr) {
        free(addr);
    }
}

static void UtLWLockThreadFunInit()
{
    thrd = (ThreadContext *)malloc(sizeof(ThreadContext));
    StorageAssert(thrd != nullptr);
    errno_t rc = memset_s(thrd, sizeof(ThreadContext), 0, sizeof(ThreadContext));
    storage_securec_check(rc, "\0", "\0");
    thrd->threadCore.core = (ThreadCore *)malloc(sizeof(ThreadCore));
    rc = memset_s(thrd->threadCore.core, sizeof(ThreadCore), 0, sizeof(ThreadCore));
    storage_securec_check(rc, "\0", "\0");
    thrd->threadCore.xact = (ThreadXact *)malloc(sizeof(ThreadXact));
    rc = memset_s(thrd->threadCore.xact, sizeof(ThreadXact), 0, sizeof(ThreadXact));
    storage_securec_check(rc, "\0", "\0");
    thrd->lwlockContext.num_held_lwlocks = 0;
    thrd->lwlockContext.held_lwlocks = (LWLockHandle *)malloc(sizeof(LWLockHandle) * MAX_SIMUL_LWLOCKS);
    rc = memset_s(thrd->lwlockContext.held_lwlocks, sizeof(LWLockHandle) * MAX_SIMUL_LWLOCKS, 0,
                  sizeof(LWLockHandle) * MAX_SIMUL_LWLOCKS);
    storage_securec_check(rc, "\0", "\0");
    std::lock_guard<std::mutex> guard(g_mutex);
}

static void UtLWLockThreadFunUinit()
{
    free(thrd->lwlockContext.held_lwlocks);
    free(thrd->GetCore());
    free(thrd->GetXact());
    free(thrd);
}

class SpinLcokTest : public ::testing::Test {
protected:
    static void SetUpTestCase()
    {
        g_spinLock.Init();
    }
    static void TearDownTestCase()
    {}
    void SetUp() override
    {
        g_count = 0;
        pthread_barrier_init(&g_barrier, nullptr, UT_SPIN_LOCK_THREAD_COUNT);
    }
    void TearDown() override
    {
        pthread_barrier_destroy(&g_barrier);
    }
};

void UTSpinLockThreadFun()
{
    UtLWLockThreadFunInit();

    for (int i = 0; i < UT_SPIN_LOCK_LOOP_COUNT; i++) {
        g_spinLock.Acquire();

        g_count++;

        g_spinLock.Release();
    }

    UtLWLockThreadFunUinit();
}

TEST_F(SpinLcokTest, DstoreSpinLock)
{
    std::thread t[UT_SPIN_LOCK_THREAD_COUNT];

    for (int i = 0; i < UT_SPIN_LOCK_THREAD_COUNT; i++) {
        t[i] = std::thread(UTSpinLockThreadFun);
    }

    for (int i = 0; i < UT_SPIN_LOCK_THREAD_COUNT; i++) {
        t[i].join();
    }

    ASSERT_EQ(g_count, UT_SPIN_LOCK_THREAD_COUNT * UT_SPIN_LOCK_LOOP_COUNT);
}
