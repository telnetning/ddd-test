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
#ifndef UT_DSTORE_ROLLBACK_TRX_WORKER_H
#define UT_DSTORE_ROLLBACK_TRX_WORKER_H

#include <gtest/gtest.h>

#include "ut_mock/ut_tablespace_mock.h"
#include "ut_mock/ut_instance_mock.h"
#include "ut_utilities/ut_dstore_framework.h"
#include "undo/dstore_rollback_trx_worker.h"
#include "undo/dstore_undo_zone_txn_mgr.h"

class UTRollbackTrxWorker : public DSTORETEST {
protected:
    void SetUp() override
    {
        DSTORETEST::SetUp();
        MockStorageInstance *instance = DstoreNew(m_ut_memory_context) MockStorageInstance();
        instance->Install(&DSTORETEST::m_guc, m_ut_memory_context);
        instance->Startup(&DSTORETEST::m_guc);

        m_bufferMgr = g_storageInstance->GetBufferMgr();
        m_tablespaceId = static_cast<TablespaceId>(TBS_ID::UNDO_TABLE_SPACE_ID);
        m_segment = (Segment *)SegmentInterface::AllocUndoSegment(g_defaultPdbId, m_tablespaceId,
            DSTORE::SegmentType::UNDO_SEGMENT_TYPE, m_bufferMgr);
        ASSERT_TRUE(SegmentIsValid(m_segment));
    }

    void TearDown() override
    {
        MockStorageInstance *instance = (MockStorageInstance *)g_storageInstance;
        instance->Shutdown();
        delete instance;
        DSTORETEST::TearDown();
    }

    TablespaceId m_tablespaceId;
    BufMgrInterface     *m_bufferMgr;
    Segment    *m_segment;
};

#endif  /* UT_STORAGE_ROLLBACK_TRX_WORKER_H */