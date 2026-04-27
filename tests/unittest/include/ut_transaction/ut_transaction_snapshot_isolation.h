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
#ifndef DSTORE_UT_TRANSACTION_SNAPSHOT_ISOLATION_H
#define DSTORE_UT_TRANSACTION_SNAPSHOT_ISOLATION_H

#include <condition_variable>
#include <gtest/gtest.h>
#include <mutex>
#include <thread>

#include "ut_tablehandler/ut_table_handler.h"
#include "ut_utilities/ut_dstore_framework.h"
#include "ut_utilities/ut_thread_pool.h"

class UTTransactionSnapshotIsolation : public DSTORETEST {
public:
    void SetUp() override
    {
        using namespace DSTORE;
        DSTORETEST::SetUp();
        UtInstance *instance = DstoreNew(m_ut_memory_context) UtInstance();
        instance->Install(&DSTORETEST::m_guc, m_ut_memory_context);
        instance->Startup(&DSTORETEST::m_guc);

        m_utTableHandler = UTTableHandler::CreateTableHandler(g_defaultPdbId, m_ut_memory_context);
        m_pool.Start(2);
    }

    void TearDown() override
    {
        using namespace DSTORE;
        m_pool.Shutdown();
        UTTableHandler::Destroy(m_utTableHandler);
        m_utTableHandler = nullptr;
        MockStorageInstance *instance = (MockStorageInstance *)g_storageInstance;
        instance->Shutdown();
        delete instance;
        DSTORETEST::TearDown();
    }

    void BuildThreadLocalVar()
    {
        if (unlikely(ThdUtTableHandler == nullptr)) {
            StorageAssert(m_utTableHandler != nullptr);
            PageId heapSegment = m_utTableHandler->m_heapSegmentPageId;
            PageId lobSegment = m_utTableHandler->m_lobSegmentPageId;
            ThdUtTableHandler = UTTableHandler::GetTableHandler(g_defaultPdbId,
                thrd->m_memoryMgr->GetGroupContext(MemoryGroupType::MEMORY_CONTEXT_LONGLIVE),
                heapSegment,
                lobSegment,
                INVALID_PAGE_ID);
        }
        ASSERT_NE(ThdUtTableHandler, nullptr);
        ASSERT_NE(ThdUtTableHandler->GetHeapTabSmgr(), nullptr);
        ASSERT_NE(ThdUtTableHandler->GetLobTabSmgr(), nullptr);
        ASSERT_TRUE(ThdUtTableHandler->m_heapSegmentPageId.IsValid());
        ASSERT_TRUE(ThdUtTableHandler->m_lobSegmentPageId.IsValid());
    }

    static void XactThreadTask01_ThreadL(UTTransactionSnapshotIsolation *ptr)
    {
        ptr->XactThreadFunctionTest01_ThreadL();
    }
    static void XactThreadTask01_ThreadR(UTTransactionSnapshotIsolation *ptr)
    {
        ptr->XactThreadFunctionTest01_ThreadR();
    }

    static void XactThreadTask02_ThreadL(UTTransactionSnapshotIsolation *ptr)
    {
        ptr->XactThreadFunctionTest02_ThreadL();
    }
    static void XactThreadTask02_ThreadR(UTTransactionSnapshotIsolation *ptr)
    {
        ptr->XactThreadFunctionTest02_ThreadR();
    }

protected:
    UTTableHandler *m_utTableHandler;
    UTThreadPool m_pool;
    std::mutex m_mutex;
    std::condition_variable m_condition;

    ItemPointerData m_ctid;
    void InitThread();

    bool volatile m_threadLNeedWait;
    void SetThreadLNeedWait();
    void SetThreadLWillRun();
    void ThreadLWaitIfNeed(std::unique_lock<std::mutex> &lck);

    bool volatile m_threadRNeedWait;
    void SetThreadRNeedWait();
    void SetThreadRWillRun();
    void ThreadRWaitIfNeed(std::unique_lock<std::mutex> &lck);


    void XactThreadFunctionTest01_ThreadL();
    void XactThreadFunctionTest01_ThreadR();

    void XactThreadFunctionTest02_ThreadL();
    void XactThreadFunctionTest02_ThreadR();
};

#endif