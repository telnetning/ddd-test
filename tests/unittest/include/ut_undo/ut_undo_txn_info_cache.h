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
#ifndef UT_UNDO_TXN_INFO_CACHE_H
#define UT_UNDO_TXN_INFO_CACHE_H

#include <gtest/gtest.h>

#include "ut_utilities/ut_dstore_framework.h"
#include "ut_utilities/ut_thread_pool.h"
#include "ut_mock/ut_tablespace_mock.h"
#include "ut_mock/ut_buf_mgr_mock.h"
#include "ut_mock/ut_instance_mock.h"
#include "undo/dstore_undo_mgr.h"
#include "undo/dstore_undo_zone_txn_mgr.h"

namespace DSTORE {

#ifdef UT
#define private public
#endif

class UndoTxnInfoCacheTest : public DSTORETEST {
protected:
    void SetUp() override
    {
        DSTORETEST::SetUp();
        MockStorageInstance *instance = DstoreNew(m_ut_memory_context) MockStorageInstance();
        instance->Install(&DSTORETEST::m_guc, m_ut_memory_context);
        instance->Startup(&DSTORETEST::m_guc);

        m_bufferMgr = g_storageInstance->GetBufferMgr();
        m_tablespace = UtMockModule::UtGetTableSpace(UNDO_TABLE_SPACE_ID_UT, m_bufferMgr);

        m_undoMgr = DstoreNew(m_ut_memory_context) UndoMgr(m_bufferMgr, m_pdbId);
        m_undoMgr->Init(g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));
        m_undoMgr->LoadUndoMapSegment();
        m_tranMgr = DstoreNew(m_ut_memory_context) TransactionMgr(m_undoMgr, g_storageInstance->GetCsnMgr(),
            g_defaultPdbId);

        m_pool.Start(10);
    }
    void TearDown() override
    {
        m_pool.Shutdown();
        delete m_tranMgr;
        delete m_undoMgr;

        MockStorageInstance *instance = (MockStorageInstance *)g_storageInstance;
        instance->Shutdown();
        delete instance;
        DSTORETEST::TearDown();
    }

    void UndoWriteCacheTest(uint64 index);
    void UndoReadCacheTest(uint64 index);
    static void UndoWriteCacheTask(UndoTxnInfoCacheTest *ptr, uint64 index)
    {
        ptr->UndoWriteCacheTest(index);
    }

    static void UndoReadCacheTask(UndoTxnInfoCacheTest *ptr, uint64 index)
    {
        ptr->UndoReadCacheTest(index);
    }

    UndoMgr* m_undoMgr;
    TransactionMgr* m_tranMgr;
    TableSpaceInterface *m_tablespace;
    BufMgrInterface     *m_bufferMgr;

    UTThreadPool m_pool;
};

}  // namespace DSTORE
#endif //UT_UNDO_TXN_INFO_CACHE_H
