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
#include <deque>

#include "lock/dstore_xact_lock_mgr.h"
#include "ut_undo/ut_dstore_rollback_trx_worker.h"

using namespace DSTORE;


TEST_F(UTRollbackTrxWorker, WorkerTest)
{
    const uint32 zoneId = 2;
    UndoZone *uzone = DstoreNew(g_dstoreCurrentMemoryContext) UndoZone(m_segment, m_bufferMgr, zoneId, m_pdbId);
    uzone->Init(g_dstoreCurrentMemoryContext);
    Xid xid = uzone->AllocSlot();

    RollbackTrxTaskMgr *taskMgr =
            g_storageInstance->GetPdb(g_defaultPdbId)->GetTransactionMgr()->m_rollbackTrxTaskMgr;
    RollbackTrxWorker *worker = (taskMgr->m_workers)[0];

    ASSERT_EQ(worker->m_rollbackTrxTask, nullptr);

    ASSERT_EQ(taskMgr->AddRollbackTrxTask(xid, uzone), DSTORE_SUCC);

    ASSERT_EQ(worker->IsRunning(), false);
    RollbackTrxTask *task = taskMgr->GetNextRollbackTrxTask();
    ASSERT_NE(task, nullptr);
    worker->SetTask(task);
    ASSERT_EQ(worker->IsRunning(), true);

    worker->Run();
    while (worker->IsRunning()) {
        sleep(1);
    }
    ASSERT_EQ(worker->IsRunning(), false);
    ASSERT_EQ(taskMgr->IsAllTaskFinished(), true);
    ASSERT_EQ(worker->m_rollbackTrxTask, nullptr);
    ASSERT_EQ(uzone->IsAsyncRollbacking(), false);
}