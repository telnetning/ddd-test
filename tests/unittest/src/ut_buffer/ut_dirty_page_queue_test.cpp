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
#include "errorcode/dstore_buf_error_code.h"
#include "ut_buffer/ut_buffer_fake_instance.h"
#include "ut_buffer/ut_buffer_util.h"
#include "buffer/dstore_bg_page_writer_mgr.h"
#include "ut_mock/ut_wal_stream_mock.h"
#include "ut_utilities/ut_thread_pool.h"
#include "port/dstore_port.h"
#include <algorithm>
#include "errorcode/dstore_buf_error_code.h"
class DirtyPageQueueTest : public DSTORETEST {
protected:
    void SetUp() override
    {
        DSTORETEST::SetUp();
        InstallDatabase(&DSTORETEST::m_guc, m_ut_memory_context);
        BufferMgrFakeStorageInstance *bufMgrInstance = DstoreNew(m_ut_memory_context)BufferMgrFakeStorageInstance();
        bufMgrInstance->Startup(&DSTORETEST::m_guc);

        m_walStreamBuffer = DstoreNew(m_ut_memory_context)WalStreamBuffer(m_ut_memory_context, 0);
        m_walFileMgr = DstoreNew(m_ut_memory_context)WalFileManager(m_ut_memory_context);
    }

    void TearDown() override
    {
        delete m_walFileMgr;
        BufferMgrFakeStorageInstance *bufMgrInstance = (BufferMgrFakeStorageInstance *)g_storageInstance;
        bufMgrInstance->Shutdown();
        delete bufMgrInstance;
        DSTORETEST::TearDown();
    }

    void PushBufferToDirtyQueue(BufferDesc **bufferDescs, uint32 bufferSize, DirtyPageQueue *queue, WalStream* walStream)
    {
        WalGroupLsnInfo walRecordGroupPtr = walStream->Append(nullptr, 100);
        for (int i = 0; i < bufferSize; i++) {
            BufferDesc *bufferDesc = bufferDescs[i];
            uint64 state = bufferDesc->LockHdr();
            queue->Push(bufferDesc, true, DSTORE::DEFAULT_BGWRITER_SLOT_ID, 1);
            bufferDesc->UnlockHdr(state);
            walRecordGroupPtr = walStream->Append(nullptr, 100);
            walStream->WaitTargetPlsnPersist(walRecordGroupPtr.m_endPlsn);
        }

        for (int i = 0; i < bufferSize; i++) {
            PageId pageId = {UtBufferUtils::DEFAULT_FILE_ID, (BlockNumber )i};
            BufferDesc *bufferDesc = bufferDescs[i];
            EXPECT_EQ(bufferDesc->bufTag.pageId, pageId);
            EXPECT_EQ(bufferDesc->recoveryPlsn[DSTORE::DEFAULT_BGWRITER_SLOT_ID].load(std::memory_order_acquire), (i + 1) * 100);
        }
    }

    struct TestParam
    {
        DirtyPageQueue *dirtyPageQueue;
        BufDescVector tmpDirtyPageVec;
        WalStream *walStream;
        BufferDesc **bufferDescs;
        bool isForward;
        uint64 iterator;
        uint64 pushCount;
        uint64 removeCount;
        uint64 flushCount;
        bool stop;
    };

    static void PushDirtyPageThread(TestParam *params)
    {
        thrd = DstoreNew(g_storageInstance->GetMemoryMgr()->GetGroupContext(DSTORE::MEMORY_CONTEXT_LONGLIVE))
            ThreadContext();
        (void)thrd->InitializeBasic();
        (void)thrd->InitStorageContext(g_defaultPdbId);
        g_storageInstance->AddVisibleThread(thrd, g_defaultPdbId);

        DirtyPageQueue *queue = params->dirtyPageQueue;
        WalStream *walStream = params->walStream;
        uint64 i = 0;
        while (i < params->iterator) {
            BufferDesc *bufferDesc = params->bufferDescs[i];
            uint64 state = bufferDesc->LockHdr();
            queue->Push(bufferDesc, true, DSTORE::DEFAULT_BGWRITER_SLOT_ID, 1);

            params->pushCount++;
            bufferDesc->UnlockHdr(state);
            i++;
            StorageAssert(params->pushCount <= params->iterator);
            WalGroupLsnInfo walRecordGroupPtr = walStream->Append(nullptr, 100);
            walStream->WaitTargetPlsnPersist(walRecordGroupPtr.m_endPlsn);
        }

        EXPECT_EQ(params->pushCount, params->iterator);

        if (thrd) {
            g_storageInstance->RemoveVisibleThread(thrd);
            thrd->Destroy();
            DstorePfree(thrd);
            thrd = nullptr;
        }
    }

    static void RemoveAfterFlushThread(TestParam *params)
    {
        DirtyPageQueue *queue = params->dirtyPageQueue;
        uint64 i = 0;
        while (i < params->iterator) {
            uint64 idx = params->isForward ? i : (params->iterator - 1) - i;
            BufferDesc *bufferDesc = params->bufferDescs[idx];
            uint64 state = bufferDesc->LockHdr();
            queue->Remove(bufferDesc, DSTORE::DEFAULT_BGWRITER_SLOT_ID);
            params->flushCount++;
            bufferDesc->UnlockHdr(state);
            i++;
        }
    }

    static void AdvanceHeadAfterFlushThread(TestParam *params)
    {
        DirtyPageQueue *queue = params->dirtyPageQueue;
        uint64 queueSize = queue->GetPageNum();
        while (queueSize > 0) {
            BufferDesc *entry = queue->GetHead();
            BufferDesc *newEntry = nullptr;
            while (entry != nullptr) {
                uint64 state = entry->LockHdr();
                newEntry = entry->nextDirtyPagePtr[DSTORE::DEFAULT_BGWRITER_SLOT_ID];
                entry->UnlockHdr(state);
                params->flushCount++;
                entry = newEntry;
            }
            queue->AdvanceHeadAfterFlush(params->tmpDirtyPageVec, queue->GetPageNum(), DSTORE::DEFAULT_BGWRITER_SLOT_ID);
            queueSize = queue->GetPageNum();
            params->removeCount += queue->GetRemoveCnt();
        }
    }

    static void MockBgPageWriterMainThread(TestParam *params, bool* stop)
    {
        thrd = DstoreNew(g_storageInstance->GetMemoryMgr()->GetGroupContext(DSTORE::MEMORY_CONTEXT_LONGLIVE))
            ThreadContext();
        (void)thrd->InitializeBasic();
        (void)thrd->InitStorageContext(g_defaultPdbId);
        g_storageInstance->AddVisibleThread(thrd, g_defaultPdbId);

        DirtyPageQueue *queue = params->dirtyPageQueue;

        AdvanceHeadAfterFlushThread(params);

        if (thrd) {
            g_storageInstance->RemoveVisibleThread(thrd);
            thrd->Destroy();
            DstorePfree(thrd);
            thrd = nullptr;
        }
    }

    WalStreamBuffer *m_walStreamBuffer;
    WalFileManager *m_walFileMgr;
};

TEST_F(DirtyPageQueueTest, PushDirtyPageToQueueTest)
{
    uint32 bufferSize = 100;
    MockWalStream walStream(1, m_ut_memory_context, m_walFileMgr, m_pdbId);
    walStream.Init(m_walStreamBuffer);
    DirtyPageQueue dirtyPageQueue;
    dirtyPageQueue.Init(&walStream);
    BufferDesc **bufferDescs = UtBufferUtils::prepare_buffer(bufferSize);
    PushBufferToDirtyQueue(bufferDescs, bufferSize, &dirtyPageQueue, &walStream);

    uint64 queueLength = dirtyPageQueue.GetPageNum();
    EXPECT_EQ(queueLength, bufferSize);
    dirtyPageQueue.Destroy();
    UtBufferUtils::free_buffer(bufferDescs, bufferSize);
}

TEST_F(DirtyPageQueueTest, PushExistDirtyPageToQueueTest_TIER1)
{
    uint32 bufferSize = 10;
    MockWalStream walStream(1, m_ut_memory_context, m_walFileMgr, m_pdbId);
    walStream.Init(m_walStreamBuffer);
    DirtyPageQueue dirtyPageQueue;
    dirtyPageQueue.Init(&walStream);
    BufferDesc **bufferDescs = UtBufferUtils::prepare_buffer(bufferSize);
    PushBufferToDirtyQueue(bufferDescs, bufferSize, &dirtyPageQueue, &walStream);

    uint64 recoveryPlsn = bufferDescs[0]->recoveryPlsn[DSTORE::DEFAULT_BGWRITER_SLOT_ID].load(std::memory_order_acquire);
    uint64 state = bufferDescs[0]->LockHdr();
    dirtyPageQueue.Push(bufferDescs[0], true, DSTORE::DEFAULT_BGWRITER_SLOT_ID, 1);
    bufferDescs[0]->UnlockHdr(state);
    EXPECT_EQ(bufferDescs[0]->recoveryPlsn[DSTORE::DEFAULT_BGWRITER_SLOT_ID].load(std::memory_order_acquire), recoveryPlsn);

    uint64 queueLength = dirtyPageQueue.GetPageNum();
    EXPECT_EQ(queueLength, bufferSize);
    dirtyPageQueue.Destroy();
    UtBufferUtils::free_buffer(bufferDescs, bufferSize);
}

TEST_F(DirtyPageQueueTest, InvalidateDirtyPageAfterFlush_TIER1)
{
    uint32 bufferSize = 10;
    MockWalStream walStream(1, m_ut_memory_context, m_walFileMgr, m_pdbId);
    walStream.Init(m_walStreamBuffer);
    DirtyPageQueue dirtyPageQueue;
    dirtyPageQueue.Init(&walStream);
    BufferDesc **bufferDescs = UtBufferUtils::prepare_buffer(bufferSize);
    PushBufferToDirtyQueue(bufferDescs, bufferSize, &dirtyPageQueue, &walStream);

    for (int i = 0; i < bufferSize; i++) {
        PageId pageId = {UtBufferUtils::DEFAULT_FILE_ID, (BlockNumber)(i)};
        BufferDesc *bufferDesc = bufferDescs[i];
        uint64 state = bufferDesc->LockHdr();
        dirtyPageQueue.Remove(bufferDesc, DSTORE::DEFAULT_BGWRITER_SLOT_ID);
        bufferDesc->UnlockHdr(state);
        EXPECT_EQ(bufferDesc->bufTag.pageId, pageId);
    }

    dirtyPageQueue.Destroy();
    UtBufferUtils::free_buffer(bufferDescs, bufferSize);
}

TEST_F(DirtyPageQueueTest, AdvanceHeadAfterFlushTest)
{
    uint32 bufferSize = 150;
    MockWalStream walStream(1, m_ut_memory_context, m_walFileMgr, m_pdbId);
    walStream.Init(m_walStreamBuffer);
    DirtyPageQueue dirtyPageQueue;
    dirtyPageQueue.Init(&walStream);
    BufferDesc **bufferDescs = UtBufferUtils::prepare_buffer(bufferSize);
    PushBufferToDirtyQueue(bufferDescs, bufferSize, &dirtyPageQueue, &walStream);
    TestParam params;
    params.dirtyPageQueue = &dirtyPageQueue;
    params.tmpDirtyPageVec.Init(g_storageInstance->GetMemoryMgr()->GetGroupContext(DSTORE::MEMORY_CONTEXT_LONGLIVE));
    params.walStream = &walStream;
    params.isForward = true;
    params.iterator = 0;
    params.bufferDescs = nullptr;
    params.pushCount = params.removeCount = params.flushCount = 0;
    AdvanceHeadAfterFlushThread(&params);
    EXPECT_EQ(params.removeCount, bufferSize);

    params.tmpDirtyPageVec.Destroy();
    dirtyPageQueue.Destroy();
    UtBufferUtils::free_buffer(bufferDescs, bufferSize);
}

TEST_F(DirtyPageQueueTest, LockFreeAlgTest_TIER1)
{
    bool stop = false;
    uint32 bufferSize = 15000;
    uint32 numberOfThreads = 10;
    TestParam pushThreadParams[numberOfThreads];
    std::thread threads[numberOfThreads];
    MockWalStream walStream(1, m_ut_memory_context, m_walFileMgr, m_pdbId);
    walStream.Init(m_walStreamBuffer);
    DirtyPageQueue dirtyPageQueue;
    dirtyPageQueue.Init(&walStream);

    TestParam bgWriterParams;
    bgWriterParams.dirtyPageQueue = &dirtyPageQueue;
    bgWriterParams.tmpDirtyPageVec.Init(
        g_storageInstance->GetMemoryMgr()->GetGroupContext(DSTORE::MEMORY_CONTEXT_LONGLIVE));
    bgWriterParams.pushCount = numberOfThreads * bufferSize;
    bgWriterParams.flushCount = 0;
    bgWriterParams.removeCount = 0;
    std::thread BgWriter {MockBgPageWriterMainThread, &bgWriterParams, &stop};

    for (uint t = 0; t < numberOfThreads; t++) {
        BufferDesc **bufferDescs = UtBufferUtils::prepare_buffer(bufferSize);
        pushThreadParams[t].dirtyPageQueue = &dirtyPageQueue;
        pushThreadParams[t].walStream = &walStream;
        pushThreadParams[t].pushCount = 0;
        pushThreadParams[t].iterator = bufferSize;
        pushThreadParams[t].bufferDescs = bufferDescs;
        threads[t] = std::thread(PushDirtyPageThread, &pushThreadParams[t]);
    }

    for (uint t = 0; t < numberOfThreads; t++) {
        threads[t].join();
    }
    BgWriter.join();
    StorageAssert(bgWriterParams.removeCount == bgWriterParams.flushCount);

    bgWriterParams.tmpDirtyPageVec.Destroy();
    dirtyPageQueue.Destroy();
}

void UpdateNextPagePtr(BufferDesc *buf1, BufferDesc *buf2)
{
    std::this_thread::sleep_for(std::chrono::seconds(5));
    std::cout << "update next ptr" << std::endl;
    buf1->nextDirtyPagePtr[DSTORE::DEFAULT_BGWRITER_SLOT_ID] = buf2;
}

TEST_F(DirtyPageQueueTest, UpdateHeadTest)
{
    MockWalStream walStream(1, m_ut_memory_context, m_walFileMgr, m_pdbId);
    walStream.Init(m_walStreamBuffer);
    DirtyPageQueue dirtyPageQueue;
    dirtyPageQueue.Init(&walStream);

    BufferDesc buf1;
    buf1.InitBufferDesc(nullptr, nullptr);
    uint64 state1 = buf1.LockHdr();

    BufferDesc buf2;
    buf2.InitBufferDesc(nullptr, nullptr);
    uint64 state2 = buf2.LockHdr();

    dirtyPageQueue.Push(&buf1, true, DSTORE::DEFAULT_BGWRITER_SLOT_ID, 1);
    dirtyPageQueue.Push(&buf2, true, DSTORE::DEFAULT_BGWRITER_SLOT_ID, 1);

    EXPECT_EQ(buf1.nextDirtyPagePtr[DSTORE::DEFAULT_BGWRITER_SLOT_ID], &buf2);

    for (uint32 i = 0; i < DSTORE::DIRTY_PAGE_QUEUE_MAX_SIZE; i++) {
        buf1.nextDirtyPagePtr[i] = INVALID_BUFFER_DESC;
    }

    std::thread t1(UpdateNextPagePtr, &buf1, &buf2);
    dirtyPageQueue.UpdateHead(&buf1, DSTORE::DEFAULT_BGWRITER_SLOT_ID);

    t1.join();
    dirtyPageQueue.Destroy();
    buf1.UnlockHdr(state1);
    buf2.UnlockHdr(state2);
}

TEST_F(DirtyPageQueueTest, EmptyTest)
{
    MockWalStream walStream(1, m_ut_memory_context, m_walFileMgr, m_pdbId);
    walStream.Init(m_walStreamBuffer);
    DirtyPageQueue dirtyPageQueue;
    dirtyPageQueue.Init(&walStream);

    constexpr int threadNum = 8;
    constexpr int bufNum = 64;
    constexpr int times = 8;
    BufferDesc *buffers = static_cast<BufferDesc *>(DstorePalloc(bufNum * times * threadNum * sizeof(BufferDesc)));

    /* clear queue */
    std::atomic_bool start{true};
    std::thread cleaner([&dirtyPageQueue, &start, &walStream, this] {
        BufDescVector tmpDirtyPageVec;
        tmpDirtyPageVec.Init(m_ut_memory_context);
        uint64 recoveryPlsn = 0;
        while (start.load(std::memory_order_relaxed)) {
            uint64 maxAppendPlsn = walStream.GetMaxAppendedPlsn();
            /* ScanDirtyListForFlush */
            BufferDesc *current = dirtyPageQueue.GetHead();
            uint64 advanceNum = 0;
            uint64 maxDirtyPageNum = dirtyPageQueue.GetPageNum();
            while (maxDirtyPageNum-- > 0 && current != INVALID_BUFFER_DESC) {
                advanceNum++;
                current = DirtyPageQueue::GetNext(current, DSTORE::DEFAULT_BGWRITER_SLOT_ID);
            }

            uint64 plsn =
                dirtyPageQueue.AdvanceHeadAfterFlush(tmpDirtyPageVec, advanceNum, DSTORE::DEFAULT_BGWRITER_SLOT_ID);

            if (advanceNum == 0) {
                if (dirtyPageQueue.UpdateHeadWhenEmpty(maxAppendPlsn)) {
                    recoveryPlsn = maxAppendPlsn;
                }
            } else {
                ASSERT_FALSE(plsn < recoveryPlsn);
                recoveryPlsn = plsn;
            }
        }
    });

    std::thread *threads[threadNum] = {nullptr};
    for (int i = 0; i < threadNum; ++i) {
        threads[i] = new std::thread([i, &buffers, &dirtyPageQueue, &walStream] {
            for (int j = 0; j < times; ++j) {
                for (int k = 0; k < bufNum; ++k) {
                    BufferDesc *buffer = buffers + i * (bufNum * times) + j * bufNum + k;
                    buffer->InitBufferDesc(nullptr, nullptr);
                    uint64 state = buffer->LockHdr();
                    dirtyPageQueue.Push(buffer, true, DSTORE::DEFAULT_BGWRITER_SLOT_ID, 1);
                    walStream.IncreaseMaxAppendedPlsn();
                    buffer->UnlockHdr(state);
                }
                GaussUsleep(1000 * 500);
            }
        });
    }

    for (auto thread : threads) {
        if (thread->joinable()) {
            thread->join();
        }
        delete thread;
    }
    start.store(false, std::memory_order_relaxed);
    if (cleaner.joinable()) {
        cleaner.join();
    }
    DstorePfree(buffers);
    dirtyPageQueue.Destroy();
}