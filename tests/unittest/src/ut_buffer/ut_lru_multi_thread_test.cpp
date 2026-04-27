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
/*
 * Created by c00428156 on 2022/4/19.
 */
#include "ut_buffer/ut_buffer_fake_instance.h"
#include "ut_buffer/ut_buffer_util.h"

#include "buffer/dstore_buf_lru.h"
#include "common/dstore_datatype.h"

#include <thread>
#include <random>
#include <vector>

using namespace DSTORE;

struct TestThreadContext {
    BufLruList *buf_lru;
    BufferDesc **buffers;
    Size buf_size;
    Size iterate;
};

class BufLruMultiThreadTest : public DSTOREParamTest<std::vector<Size>> {
protected:
    void SetUp() override
    {
        DSTOREParamTest<std::vector<Size>>::SetUp();

        InstallDatabase(&DSTOREParamTest<std::vector<Size>>::m_guc, m_ut_memory_context);
        VfsInterface::ModuleInitialize();
        BufferMgrFakeStorageInstance *bufMgrInstance = DstoreNew(m_ut_memory_context)BufferMgrFakeStorageInstance();
        bufMgrInstance->Startup(&DSTOREParamTest<std::vector<Size>>::m_guc);
    }

    void TearDown() override
    {
        BufferMgrFakeStorageInstance *bufMgrInstance = (BufferMgrFakeStorageInstance *)g_storageInstance;
        bufMgrInstance->Shutdown();
        delete bufMgrInstance;
        DSTOREParamTest<std::vector<Size>>::TearDown();
    }

    BufferDesc **make_buf_lru(Size buf_size, BufLruList *lru)
    {
        BufferDesc **buffers = UtBufferUtils::prepare_buffer(buf_size);

        for (Size i = 0; i < buf_size; i++) {
            lru->AddNewBuffer(buffers[i]);
        }

        return buffers;
    }

    void move_buffer_to_lru_list(BufLruList *buf_lru, Size buf_size)
    {
        for (Size i = 0; i < buf_size; i++) {
            BufferTag bufTag{g_defaultPdbId, {0, uint32(i)}};
            BufferDesc *buffer = buf_lru->GetCandidateBuffer(bufTag, false);
            buffer->bufTag = bufTag;

            buf_lru->PushBackToLru(buffer, true);

            buffer->UnlockHdr(GsAtomicReadU64(&buffer->state));
            buffer->Unpin();
        }
    }

    void wait_for_thread_all_finish(std::thread *threads, Size size)
    {
        for (int i = 0; i < size; i++) {
            threads[i].join();
        }
    }

    static void *buffer_access_thread(void *param)
    {
        TestThreadContext *thd_ctx = (TestThreadContext *)param;
        BufLruList *buf_lru = thd_ctx->buf_lru;
        Size buf_size = thd_ctx->buf_size;
        BufferDesc **buffers = thd_ctx->buffers;

        std::default_random_engine rand_engine;
        std::uniform_int_distribution<Size> distribution(0, buf_size - 1);

        BufferMgrFakeStorageInstance *bufMgrInstance = (BufferMgrFakeStorageInstance *)g_storageInstance;
        bufMgrInstance->ThreadSetupAndRegister();

        for (Size i = 0; i < thd_ctx->iterate; i++) {
            Size idx = distribution(rand_engine);
            buf_lru->BufferAccessStat(buffers[idx]);
        }

        bufMgrInstance->ThreadUnregisterAndExit();

        return nullptr;
    }

    static void *get_free_buffer_thread(void *param)
    {
        TestThreadContext *thd_ctx = (TestThreadContext *)param;
        BufLruList *buf_lru = thd_ctx->buf_lru;
        std::default_random_engine rand_engine;
        std::bernoulli_distribution distribution;
        BufferMgrFakeStorageInstance *bufMgrInstance = (BufferMgrFakeStorageInstance *)g_storageInstance;
        bufMgrInstance->ThreadSetupAndRegister();

        for (Size i = 0; i < thd_ctx->iterate; i++) {
            BufferTag bufTag{g_defaultPdbId, {0, uint32(i)}};
            BufferDesc *buffer = buf_lru->GetCandidateBuffer(bufTag, false);
            if (buffer != INVALID_BUFFER_DESC) {
                StorageAssert(BufLruMultiThreadTest::check_buffer_pop_from_candidate_list(buffer));

                buf_lru->PushBackToLru(buffer, distribution(rand_engine));
                buffer->UnlockHdr(GsAtomicReadU64(&buffer->state));
                buffer->Unpin();
            }
        }

        bufMgrInstance->ThreadUnregisterAndExit();
        return nullptr;
    }

    static void *move_buffer_to_candidate_thread(void *param)
    {
        TestThreadContext *thd_ctx = (TestThreadContext *)param;
        BufLruList *buf_lru = thd_ctx->buf_lru;
        Size buf_size = thd_ctx->buf_size;
        BufferDesc **buffers = thd_ctx->buffers;

        std::default_random_engine rand_engine;
        std::uniform_int_distribution<Size> distribution(0, buf_size - 1);

        BufferMgrFakeStorageInstance *bufMgrInstance = (BufferMgrFakeStorageInstance *)g_storageInstance;
        bufMgrInstance->ThreadSetupAndRegister();

        for (Size i = 0; i < thd_ctx->iterate; i++) {
            Size idx = distribution(rand_engine);
            buf_lru->MoveToCandidateList(buffers[idx]);
        }

        bufMgrInstance->ThreadUnregisterAndExit();

        return nullptr;
    }

    static void *remove_buffer_from_list_thread(void *param)
    {
        TestThreadContext *thd_ctx = (TestThreadContext *)param;
        BufLruList *buf_lru = thd_ctx->buf_lru;
        Size buf_size = thd_ctx->buf_size;
        BufferDesc **buffers = thd_ctx->buffers;

        std::default_random_engine rand_engine;
        std::uniform_int_distribution<Size> distribution(0, buf_size - 1);

        BufferMgrFakeStorageInstance *bufMgrInstance = (BufferMgrFakeStorageInstance *)g_storageInstance;
        bufMgrInstance->ThreadSetupAndRegister();

        for (Size i = 0; i < thd_ctx->iterate; i++) {
            Size idx = distribution(rand_engine);
            buf_lru->Remove(buffers[idx]);
            StorageAssert(check_buffer_remove_from_list(buffers[idx]));
            if (i % 2 == 0) {
                buf_lru->PushBackToCandidate(buffers[idx]);
            } else {
                buf_lru->PushBackToLru(buffers[idx], true);
            }
        }

        bufMgrInstance->ThreadUnregisterAndExit();

        return nullptr;
    }

public:
    static testing::AssertionResult check_buffer_pop_from_candidate_list(BufferDesc *buffer)
    {
        if (buffer == INVALID_BUFFER_DESC) {
            return testing::AssertionFailure() << "buffer is INVALID_BUFFER_DESC";
        }

        if (!buffer->lruNode.IsInPendingState()) {
            return testing::AssertionFailure() << "buffer is NOT in pending state";
        }

        uint64 buf_state = GsAtomicReadU64(&buffer->state);
        if (buffer->GetRefcount() != 1U) {
            return testing::AssertionFailure() << "buffer refcount is NOT 1";
        }

        if ((buf_state & Buffer::BUF_LOCKED) == 0) {
            return testing::AssertionFailure() << "do NOT held buffer header lock";
        }
        return testing::AssertionSuccess();
    }

    static testing::AssertionResult check_buffer_remove_from_list(BufferDesc *buffer)
    {
        if (buffer == INVALID_BUFFER_DESC) {
            return testing::AssertionFailure() << "buffer is INVALID_BUFFER_DESC";
        }

        if (!buffer->lruNode.IsInPendingState()) {
            return testing::AssertionFailure() << "buffer is NOT in pending state";
        }

        if (buffer->lruNode.GetUsage() != 0) {
            return testing::AssertionFailure() << "usage is not 0, actual is " << buffer->lruNode.GetUsage();
        }

        return testing::AssertionSuccess();
    }
};

INSTANTIATE_TEST_SUITE_P(ThreadNumber, BufLruMultiThreadTest,
                         testing::Values(
                             /* access_thread, get_free_buffer_thread, move_buffer_thread, remove_buffer_thread */
                             std::vector<Size>{10, 0, 0, 0}, std::vector<Size>{0, 10, 0, 0},
                             std::vector<Size>{0, 0, 10, 0}, std::vector<Size>{0, 0, 0, 10},
                             std::vector<Size>{10, 0, 0, 2}, std::vector<Size>{10, 0, 2, 0},
                             std::vector<Size>{10, 2, 0, 0}, std::vector<Size>{10, 2, 2, 2},
#ifndef ENABLE_THREAD_CHECK
                             std::vector<Size>{0, 10, 10, 10},
#endif
                             std::vector<Size>{10, 5, 0, 1},
                             std::vector<Size>{10, 5, 1, 0}));

TEST_P(BufLruMultiThreadTest, MultiThreadAccess_TIER1)
{
    Size buf_size = 80;
    Size hot_buf_size = 64;

    BufLruList lru(0, hot_buf_size);
    lru.Initialize();
    BufferDesc **buffers = make_buf_lru(buf_size, &lru);
    move_buffer_to_lru_list(&lru, buf_size);

    /* check the buffer state */
    for (Size i = 0; i < buf_size; i++) {
        ASSERT_TRUE(buffers[i]->lruNode.IsInLruList());
        ASSERT_EQ(buffers[i]->lruNode.GetUsage(), 1U);
    }

    std::vector<Size> params = GetParam();
    Size access_thread = params[0];
    Size get_free_thread = params[1];
    Size move_buffer_thread = params[2];
    Size remove_buffer_thread = params[3];
    Size thread_num = access_thread + get_free_thread + move_buffer_thread + remove_buffer_thread;
    Size iterate = 10000;

    std::thread threads[thread_num];
    TestThreadContext thd_ctx[thread_num];

    for (Size i = 0; i < thread_num; i++) {
        TestThreadContext *ctx = &thd_ctx[i];
        ctx->buf_lru = &lru;
        ctx->buf_size = buf_size;
        ctx->buffers = buffers;
        ctx->iterate = iterate;
        if (i < access_thread) {
            threads[i] = std::thread(buffer_access_thread, (void *)ctx);
            pthread_setname_np(threads[i].native_handle(), "buffer_access");
        } else if (i < (access_thread + get_free_thread)) {
            threads[i] = std::thread(get_free_buffer_thread, (void *)ctx);
            pthread_setname_np(threads[i].native_handle(), "get_candidate_buffer");
        } else if (i < (access_thread + get_free_thread + move_buffer_thread)) {
            threads[i] = std::thread(move_buffer_to_candidate_thread, (void *)ctx);
            pthread_setname_np(threads[i].native_handle(), "move_buffer_to_candidate");
        } else {
            threads[i] = std::thread(remove_buffer_from_list_thread, (void *)ctx);
            pthread_setname_np(threads[i].native_handle(), "remove_buffer_thread");
        }
    }

    wait_for_thread_all_finish(threads, thread_num);

    Size hot_buffers = 0;
    Size lru_buffers = 0;
    Size free_buffers = 0;
    Size pending_buffers = 0;
    Size unknown_buffers = 0;
    for (Size i = 0; i < buf_size; i++) {
        if ((buffers[i]->lruNode.IsInHotList())) {
            hot_buffers++;
        } else if ((buffers[i]->lruNode.IsInLruList())) {
            lru_buffers++;
        } else if ((buffers[i]->lruNode.IsInCandidateList())) {
            free_buffers++;
        } else if ((buffers[i]->lruNode.IsInPendingState())) {
            pending_buffers++;
        } else {
            unknown_buffers++;
        }
    }

    RecordProperty("access_thread", access_thread);
    RecordProperty("get_free_thread", get_free_thread);
    RecordProperty("move_buffer_thread", move_buffer_thread);
    RecordProperty("hot", hot_buffers);
    RecordProperty("lru", lru_buffers);
    RecordProperty("free", free_buffers);
    RecordProperty("pending", pending_buffers);
    RecordProperty("unknow", unknown_buffers);
    RecordProperty("sum", buf_size);

    ASSERT_EQ(unknown_buffers, 0);
    ASSERT_EQ(pending_buffers, 0);
    ASSERT_EQ(hot_buffers + lru_buffers + free_buffers, buf_size);

    UtBufferUtils::free_buffer(buffers, buf_size);
}
