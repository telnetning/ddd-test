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
#include "ut_buffer/ut_lru_test.h"

using namespace DSTORE;

class HotListTest : public LruTest {
protected:
    static void* hot_data_pull(void* arg)
    {
        ThreadParam<LruHotList>* param = static_cast<ThreadParam<LruHotList>*>(arg);
        LruTestContext<LruHotList>* context = param->context;
        param->count = 0;

        BufferMgrFakeStorageInstance *bufMgrInstance = (BufferMgrFakeStorageInstance *)g_storageInstance;
        bufMgrInstance->ThreadSetupAndRegister();

        while (context->list->Pop() != nullptr) {
            param->count++;
        }

        bufMgrInstance->ThreadUnregisterAndExit();

        return nullptr;
    }

    static void* hot_data_pull_until_signal_stop(void* arg)
    {
        ThreadParam<LruHotList>* param = static_cast<ThreadParam<LruHotList>*>(arg);
        LruTestContext<LruHotList>* context = param->context;
        param->count = 0;

        BufferMgrFakeStorageInstance *bufMgrInstance = (BufferMgrFakeStorageInstance *)g_storageInstance;
        bufMgrInstance->ThreadSetupAndRegister();

        LruNode *node = nullptr;
        while (1) {
            if (context->is_stop->load()) {
                node = context->list->Pop();
                if (node == nullptr) {
                    break;
                }
                param->count++;
            } else {
                node = context->list->Pop();
                if (node != nullptr) {
                    param->count++;
                }
            }
        }

        bufMgrInstance->ThreadUnregisterAndExit();

        return nullptr;
    }

    static void* hot_data_push(void* arg)
    {
        ThreadParam<LruHotList>* param = static_cast<ThreadParam<LruHotList>*>(arg);
        LruTestContext<LruHotList>* context = param->context;

        BufferMgrFakeStorageInstance *bufMgrInstance = (BufferMgrFakeStorageInstance *)g_storageInstance;
        bufMgrInstance->ThreadSetupAndRegister();

        for (int i = param->start; i <= param->stop; i++) {
            TestEntry *entry = &context->entrys[i];
            while (!context->list->Push(&entry->node)) {}
            param->count++;
        }

        bufMgrInstance->ThreadUnregisterAndExit();

        return nullptr;
    }

    static void* hot_data_remove(void* arg)
    {
        ThreadParam<LruHotList>* param = static_cast<ThreadParam<LruHotList>*>(arg);
        LruTestContext<LruHotList>* context = param->context;
        param->count = 0;

        BufferMgrFakeStorageInstance *bufMgrInstance = (BufferMgrFakeStorageInstance *)g_storageInstance;
        bufMgrInstance->ThreadSetupAndRegister();

        for (int i = param->start; i <= param->stop; i++) {
            TestEntry *entry = &context->entrys[i];
            context->list->Remove(&entry->node);
            param->count++;
        }

        bufMgrInstance->ThreadUnregisterAndExit();

        return nullptr;
    }
};

TEST_F(HotListTest, PullEmptyListTest)
{
    LruHotList list;
    list.Initialize();
    Size size = list.Length();

    ASSERT_EQ(size, 0);

    LruNode* node = list.Pop();
    ASSERT_EQ(node, nullptr);
}

TEST_F(HotListTest, PushListTest)
{
    LruHotList list;
    list.Initialize();
    Size size = list.Length();
    ASSERT_EQ(size, 0);

    TestEntry entry(2);
    list.Push(&entry.node);

    size = list.Length();
    ASSERT_EQ(size, 1);

    LruNode* node = list.Pop();
    ASSERT_NE(node, nullptr);

    TestEntry* result = node->GetValue<TestEntry>();
    ASSERT_EQ(result->value, 2);
}

TEST_F(HotListTest, PushFullListTest)
{
    Size test_set_size = 8;
    LruHotList list(test_set_size);
    list.Initialize();
    Size size = list.Length();
    ASSERT_EQ(size, 0);

    TestEntry entrys[test_set_size];
    for (int i = 0; i < test_set_size; i++) {
        entrys[i].value = i;
        list.Push(&entrys[i].node);
    }

    size = list.Length();
    ASSERT_EQ(size, test_set_size);

    TestEntry one_more_entry(9);

    ASSERT_FALSE(list.Push(&one_more_entry.node));

    LruNode* node = list.Pop();
    ASSERT_NE(node, nullptr);

    TestEntry* entry = node->GetValue<TestEntry>();
    ASSERT_EQ(entry->value, 0);

    ASSERT_TRUE(list.Push(&one_more_entry.node));
    size = list.Length();
    ASSERT_EQ(size, 8);
}

TEST_F(HotListTest, RemoveTest)
{
    LruHotList list{};
    list.Initialize();
    TestEntry* entrys = new TestEntry[3]{};
    for (int i = 0; i < 3; i++) {
        entrys[i].value = i;
        list.Push(&entrys[i].node);
    }

    Size size = list.Length();
    ASSERT_EQ(size, 3);

    list.Remove(&entrys[1].node);
    size = list.Length();
    ASSERT_EQ(size, 2);

    LruNode* node = list.Pop();
    ASSERT_NE(node, nullptr);
    ASSERT_EQ(node->GetValue<TestEntry>()->value, 0);

    node = list.Pop();
    ASSERT_NE(node, nullptr);
    ASSERT_EQ(node->GetValue<TestEntry>()->value, 2);

    delete[] entrys;
}

TEST_F(HotListTest, PushThenPullEntrysInOneThreadTest)
{
    // params
    Size TestSetSize = 100;
    Size PushThreadNum = 1;
    Size PullThreadNum = 1;
    Size ThreadNum = PushThreadNum + PullThreadNum;

    TestSetSize = (TestSetSize / ThreadNum) * ThreadNum;

    LruTestContext<LruHotList> context{};
    test_context_init(&context, PushThreadNum, TestSetSize, new LruHotList(TestSetSize));
    std::thread threads[ThreadNum];
    ThreadParam<LruHotList> params[ThreadNum];

    for (int i = 0; i < PushThreadNum; i++) {
        ThreadParam<LruHotList> *param = &params[i];
        param->idx = i;
        param->start = i * (TestSetSize / PushThreadNum);
        param->stop = (i + 1) * (TestSetSize / PushThreadNum) - 1;
        param->context = &context;
        param->count = 0;
        threads[i] = std::thread(hot_data_push, (void*)param);
        pthread_setname_np(threads[i].native_handle(), "data_push");
    }

    wait_for_thread_all_finish(threads, PushThreadNum);

    Size push_count = 0;
    for (int i = 0; i < PushThreadNum; i++) {
        push_count += params[i].count;
    }
    ASSERT_EQ(push_count, TestSetSize);

    Size size = context.list->Length();
    ASSERT_EQ(size, TestSetSize);

    for (int i = PushThreadNum; i < ThreadNum; i++) {
        ThreadParam<LruHotList> *param = &params[i];
        param->idx = i;
        param->context = &context;
        param->count = 0;
        threads[i] = std::thread(hot_data_pull, (void*)param);
        pthread_setname_np(threads[i].native_handle(), "data_pull");
    }

    wait_for_thread_all_finish(threads + PushThreadNum, PullThreadNum);

    Size pull_count = 0;
    for (int i = PushThreadNum; i < ThreadNum; i++) {
        pull_count += params[i].count;
    }
    ASSERT_EQ(pull_count, TestSetSize);

    size = context.list->Length();
    ASSERT_EQ(size, 0U);

    test_context_destroy(&context);
}

TEST_F(HotListTest, PushThenPullEntrysInMultiThreadTest_TIER1)
{
    // params
    Size TestSetSize = 5000;
    Size PushThreadNum = 10;
    Size PullThreadNum = 10;
    Size ThreadNum = PushThreadNum + PullThreadNum;

    TestSetSize = (TestSetSize / ThreadNum) * ThreadNum;

    LruTestContext<LruHotList> context{};
    test_context_init(&context, PushThreadNum, TestSetSize, new LruHotList(TestSetSize));
    std::thread threads[ThreadNum];
    ThreadParam<LruHotList> params[ThreadNum];

    for (int i = 0; i < PushThreadNum; i++) {
        ThreadParam<LruHotList> *param = &params[i];
        param->idx = i;
        param->start = i * (TestSetSize / PushThreadNum);
        param->stop = (i + 1) * (TestSetSize / PushThreadNum) - 1;
        param->context = &context;
        param->count = 0;
        threads[i] = std::thread(hot_data_push, (void*)param);
        pthread_setname_np(threads[i].native_handle(), "data_push");
    }

    wait_for_thread_all_finish(threads, PushThreadNum);

    Size push_count = 0;
    for (int i = 0; i < PushThreadNum; i++) {
        push_count += params[i].count;
    }
    ASSERT_EQ(push_count, TestSetSize);

    Size size = context.list->Length();
    ASSERT_EQ(size, TestSetSize);

    for (int i = PushThreadNum; i < ThreadNum; i++) {
        ThreadParam<LruHotList> *param = &params[i];
        param->idx = i;
        param->context = &context;
        param->count = 0;
        threads[i] = std::thread(hot_data_pull, (void*)param);
        pthread_setname_np(threads[i].native_handle(), "data_pull");
    }

    wait_for_thread_all_finish(threads + PushThreadNum, PullThreadNum);

    Size pull_count = 0;
    for (int i = PushThreadNum; i < ThreadNum; i++) {
        pull_count += params[i].count;
    }
    ASSERT_EQ(pull_count, TestSetSize);

    size = context.list->Length();
    ASSERT_EQ(size, 0U);

    test_context_destroy(&context);
}

TEST_F(HotListTest, PushThenRemoveEntrysInMultiThreadTest_TIER1)
{
    // params
    Size TestSetSize = 5000;
    Size PushThreadNum = 10;
    Size RemoveVisibleThreadNum = PushThreadNum;
    Size ThreadNum = PushThreadNum + RemoveVisibleThreadNum;

    TestSetSize = (TestSetSize / ThreadNum) * ThreadNum;

    LruTestContext<LruHotList> context{};
    test_context_init(&context, PushThreadNum, TestSetSize, new LruHotList(TestSetSize));
    std::thread threads[ThreadNum];
    ThreadParam<LruHotList> params[ThreadNum];

    for (int i = 0; i < PushThreadNum; i++) {
        ThreadParam<LruHotList> *param = &params[i];
        param->idx = i;
        param->start = i * (TestSetSize / PushThreadNum);
        param->stop = (i + 1) * (TestSetSize / PushThreadNum) - 1;
        param->context = &context;
        param->count = 0;
        threads[i] = std::thread(hot_data_push, (void*)param);
        pthread_setname_np(threads[i].native_handle(), "data_push");
    }

    wait_for_thread_all_finish(threads, PushThreadNum);

    Size push_count = 0;
    for (int i = 0; i < PushThreadNum; i++) {
        push_count += params[i].count;
    }
    ASSERT_EQ(push_count, TestSetSize);

    Size size = context.list->Length();
    ASSERT_EQ(size, TestSetSize);

    for (int i = PushThreadNum; i < ThreadNum; i++) {
        ThreadParam<LruHotList> *param = &params[i];
        param->idx = i - PushThreadNum;
        param->start = (i - PushThreadNum) * (TestSetSize / PushThreadNum);
        param->stop = (i - PushThreadNum + 1) * (TestSetSize / PushThreadNum) - 1;
        param->context = &context;
        param->count = 0;
        threads[i] = std::thread(hot_data_remove, (void*)param);
        pthread_setname_np(threads[i].native_handle(), "data_remove");
    }

    wait_for_thread_all_finish(threads + PushThreadNum, RemoveVisibleThreadNum);

    Size pull_count = 0;
    for (int i = PushThreadNum; i < ThreadNum; i++) {
        pull_count += params[i].count;
    }
    ASSERT_EQ(pull_count, TestSetSize);

    size = context.list->Length();
    ASSERT_EQ(size, 0U);

    test_context_destroy(&context);
}

TEST_F(HotListTest, PushAndPullAtSameTimeInMultiThreadTest01_TIER1)
{
    Size TestSetSize = 50000;
    Size PushThreadNum = 10;
    Size PullThreadNum = 5;
    Size ThreadNum = PushThreadNum + PullThreadNum;

    TestSetSize = (TestSetSize / PushThreadNum) * PushThreadNum;

    LruTestContext<LruHotList> context{};
    test_context_init(&context, PushThreadNum, TestSetSize, new LruHotList(TestSetSize));
    ThreadParam<LruHotList> params[ThreadNum];
    std::thread threads[ThreadNum];
    for (int i = PushThreadNum; i < ThreadNum; i++) {
        ThreadParam<LruHotList>* param = &params[i];
        param->idx = i;
        param->context = &context;
        param->count = 0;
        threads[i] = std::thread(hot_data_pull_until_signal_stop, (void*)param);
        pthread_setname_np(threads[i].native_handle(), "data_always_pull");
    }

    for (int i = 0; i < PushThreadNum; i++) {
        ThreadParam<LruHotList>* param = &params[i];
        param->idx = i;
        param->start = i * (TestSetSize / PushThreadNum);
        param->stop = (i + 1) * (TestSetSize / PushThreadNum) - 1;
        param->context = &context;
        param->count = 0;
        threads[i] = std::thread(hot_data_push, (void*)param);
        pthread_setname_np(threads[i].native_handle(), "data_push");
    }

    wait_for_thread_all_finish(threads, PushThreadNum);

    g_stop.store(true);

    wait_for_thread_all_finish(threads + PushThreadNum, PullThreadNum);

    Size push_count = 0;
    Size pull_count = 0;
    for (int i = 0; i < PushThreadNum; i++) {
        push_count += params[i].count;
    }
    for (int i = PushThreadNum; i < ThreadNum; i++) {
        pull_count += params[i].count;
    }
    ASSERT_EQ(push_count, TestSetSize);
    ASSERT_EQ(pull_count, TestSetSize);

    Size size = context.list->Length();

    ASSERT_EQ(size, 0U);

    test_context_destroy(&context);
}

TEST_F(HotListTest, PushAndPullAtSameTimeInMultiThreadTest02_TIER1)
{
    Size TestSetSize = 50000;
    Size PushThreadNum = 5;
    Size PullThreadNum = 10;
    Size ThreadNum = PushThreadNum + PullThreadNum;

    TestSetSize = (TestSetSize / PushThreadNum) * PushThreadNum;

    LruTestContext<LruHotList> context{};
    test_context_init(&context, PushThreadNum, TestSetSize, new LruHotList(TestSetSize));
    ThreadParam<LruHotList> params[ThreadNum];
    std::thread threads[ThreadNum];
    for (int i = PushThreadNum; i < ThreadNum; i++) {
        ThreadParam<LruHotList>* param = &params[i];
        param->idx = i;
        param->context = &context;
        param->count = 0;
        threads[i] = std::thread(hot_data_pull_until_signal_stop, (void*)param);
        pthread_setname_np(threads[i].native_handle(), "data_always_pull");
    }

    for (int i = 0; i < PushThreadNum; i++) {
        ThreadParam<LruHotList>* param = &params[i];
        param->idx = i;
        param->start = i * (TestSetSize / PushThreadNum);
        param->stop = (i + 1) * (TestSetSize / PushThreadNum) - 1;
        param->context = &context;
        param->count = 0;
        threads[i] = std::thread(hot_data_push, (void*)param);
        pthread_setname_np(threads[i].native_handle(), "data_push");
    }

    wait_for_thread_all_finish(threads, PushThreadNum);

    g_stop.store(true);

    wait_for_thread_all_finish(threads + PushThreadNum, PullThreadNum);

    Size push_count = 0;
    Size pull_count = 0;
    for (int i = 0; i < PushThreadNum; i++) {
        push_count += params[i].count;
    }
    for (int i = PushThreadNum; i < ThreadNum; i++) {
        pull_count += params[i].count;
    }
    ASSERT_EQ(push_count, TestSetSize);
    ASSERT_EQ(pull_count, TestSetSize);

    Size size = context.list->Length();

    ASSERT_EQ(size, 0U);

    test_context_destroy(&context);
}

TEST_F(HotListTest, PushAndPullAtSameTimeInMultiThreadTest03_TIER1)
{
    Size TestSetSize = 50000;
    Size PushThreadNum = 5;
    Size PullThreadNum = 10;
    Size ThreadNum = PushThreadNum + PullThreadNum;

    TestSetSize = (TestSetSize / PushThreadNum) * PushThreadNum;

    LruTestContext<LruHotList> context{};
    test_context_init(&context, PushThreadNum, TestSetSize, new LruHotList(100));
    ThreadParam<LruHotList> params[ThreadNum];
    std::thread threads[ThreadNum];
    for (int i = PushThreadNum; i < ThreadNum; i++) {
        ThreadParam<LruHotList>* param = &params[i];
        param->idx = i;
        param->count = 0;
        param->context = &context;
        param->count = 0;
        threads[i] = std::thread(hot_data_pull_until_signal_stop, (void*)param);
        pthread_setname_np(threads[i].native_handle(), "data_always_pull");
    }

    for (int i = 0; i < PushThreadNum; i++) {
        ThreadParam<LruHotList>* param = &params[i];
        param->idx = i;
        param->start = i * (TestSetSize / PushThreadNum);
        param->stop = (i + 1) * (TestSetSize / PushThreadNum) - 1;
        param->context = &context;
        param->count = 0;
        threads[i] = std::thread(hot_data_push, (void*)param);
        pthread_setname_np(threads[i].native_handle(), "data_push");
    }

    wait_for_thread_all_finish(threads, PushThreadNum);

    g_stop.store(true);

    wait_for_thread_all_finish(threads + PushThreadNum, PullThreadNum);

    Size push_count = 0;
    Size pull_count = 0;
    for (int i = 0; i < PushThreadNum; i++) {
        push_count += params[i].count;
    }
    for (int i = PushThreadNum; i < ThreadNum; i++) {
        pull_count += params[i].count;
    }
    ASSERT_EQ(push_count, TestSetSize);
    ASSERT_EQ(pull_count, TestSetSize);

    Size size = context.list->Length();

    ASSERT_EQ(size, 0U);

    test_context_destroy(&context);
}

