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
#include "common/concurrent/dstore_futex.h"
#include "common/dstore_datatype.h"
#include "securec.h"
#include <unistd.h>
#include <thread>
using namespace DSTORE;
const static int UT_SEMA_CREATE_COUNT = 1;
static DstoreFutex g_semaPhore;
const static int UT_PRODUCT_COUNT = 1000;
static uint32 g_semaCount = 0;


static void memFree(void *addr)
{
    if (addr != nullptr) {
        free(addr);
    }
}

class SemaTest : public DSTORETEST {
protected:
    static void SetUpTestCase()
    {
    }
    static void TearDownTestCase()
    {
    }
    void SetUp() override
    {
        g_semaCount = 0;
        g_semaPhore.DstoreFutexInit();
    }
    void TearDown() override
    {
    }
private:
    static Size m_memSize;
    static void *m_addr;
};

Size SemaTest::m_memSize = 0;
void* SemaTest::m_addr = nullptr;

void UTSemaProducer()
{
    for (int i = 0; i < UT_PRODUCT_COUNT; i++) {
        g_semaPhore.DstoreFutexPost();
        while (g_semaCount != i + 1) {
            /* the futex semapost can't continuous post */
            usleep(1000);
        }
    }
}

void UTSemaConsumer()
{
    while(true) {
        g_semaPhore.DstoreFutexWait(true);
        g_semaCount++;

        // 初始化为1
        if (g_semaCount == UT_PRODUCT_COUNT) {
            break;
        }
    }
}

void UTSemaConsumerTry()
{
    while(true) {
        if (g_semaPhore.DstoreFutexTry()) {
            g_semaCount++;
            if (g_semaCount == UT_PRODUCT_COUNT) {
                break;
            }
        }
    }
}

TEST_F(SemaTest, PGSemaphoreLock)
{
    std::thread t2(UTSemaConsumer);
    std::thread t1(UTSemaProducer);
    t2.join();
    t1.join();

    EXPECT_FALSE(g_semaPhore.DstoreFutexTry());
}

TEST_F(SemaTest, PGSemaphoreTryLock)
{
    std::thread t2(UTSemaConsumerTry);
    std::thread t1(UTSemaProducer);
    t2.join();
    t1.join();

    EXPECT_FALSE(g_semaPhore.DstoreFutexTry());
}
