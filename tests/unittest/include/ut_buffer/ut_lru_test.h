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
//
// Created by c00428156 on 2022/4/15.
//

#ifndef DSTORE_UT_LRU_TEST_H
#define DSTORE_UT_LRU_TEST_H
#include <gtest/gtest.h>
#include <thread>
#include <type_traits>
#include <cmath>
#include "buffer/dstore_buf_lru.h"
#include "framework/dstore_modules.h"
#include "ut_buffer/ut_buffer_fake_instance.h"

using namespace DSTORE;

struct TestEntry {
    DSTORE::LruNode node;
    int value;
    TestEntry() : node{}, value{0}
    {
        node.InitNode(this);
    };
    TestEntry(int val) : node{}, value{val}
    {
        node.InitNode(this);
    };
};

template<typename T>
struct LruTestContext {
    T* list;
    std::atomic<bool>* is_stop;
    TestEntry *entrys;
    Size testset_size;
};

template<typename T>
struct ThreadParam {
    int idx;
    uint64 start;
    uint64 stop;
    volatile Size count;

    LruTestContext<T>* context;
};

class LruTest : public DSTORETEST {
protected:
    void SetUp() override
    {
        DSTORETEST::SetUp();
        g_stop = false;
        InstallDatabase(&DSTORETEST::m_guc, m_ut_memory_context);
        BufferMgrFakeStorageInstance *bufMgrInstance = DstoreNew(m_ut_memory_context)BufferMgrFakeStorageInstance();
        bufMgrInstance->Startup(&DSTORETEST::m_guc);
    }
    void TearDown() override
    {
        BufferMgrFakeStorageInstance *bufMgrInstance = (BufferMgrFakeStorageInstance *)g_storageInstance;
        bufMgrInstance->Shutdown();
        delete bufMgrInstance;
        DSTORETEST::TearDown();
    }

    template<typename T>
    void test_context_init(LruTestContext<T> *context,
                           Size push_thread_num, Size& testset_size, T* list)
    {
        list->Initialize();
        context->list = list;

        testset_size = (testset_size / push_thread_num) * push_thread_num;

        context->testset_size = testset_size;
        context->entrys = new TestEntry[testset_size];
        for (Size i = 0; i < testset_size; i++) {
            context->entrys[i].value = i;
        }
        context->is_stop = &g_stop;
    }

    template<typename T>
    void test_context_destroy(LruTestContext<T> *context)
    {
        delete context->list;
        delete[] context->entrys;
    }

    static void wait_for_thread_all_finish(std::thread* threads, Size size)
    {
        for (int i = 0; i < size; i++) {
            threads[i].join();
        }
    }

    std::atomic<bool> g_stop;
};

#endif  // DSTORE_UT_LRU_TEST_H
