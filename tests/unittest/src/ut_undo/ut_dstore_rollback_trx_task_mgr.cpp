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
#include "ut_undo/ut_dstore_rollback_trx_task_mgr.h"
#include "undo/dstore_rollback_trx_worker.h"

using namespace DSTORE;

TEST_F(UTRollbackTrxTaskMgr, TaskMgrInterfaceSimpleTest)
{
    RollbackTrxTaskMgr *taskMgr =
            g_storageInstance->GetPdb(g_defaultPdbId)->GetTransactionMgr()->m_rollbackTrxTaskMgr;

    Xid xid(1);
    ASSERT_EQ(taskMgr->IsAllTaskFinished(), true);
    ASSERT_EQ(taskMgr->AddRollbackTrxTask(xid, nullptr), DSTORE_SUCC);
    ASSERT_EQ(taskMgr->IsAllTaskFinished(), false);

    RollbackTrxTask *task = taskMgr->GetNextRollbackTrxTask();
    ASSERT_NE(task, nullptr);
    ASSERT_EQ(task->rollbackXid, xid);
    ASSERT_EQ(task->rollbackUndoZone, nullptr);
    task = taskMgr->GetNextRollbackTrxTask();
    ASSERT_EQ(task, nullptr);

    taskMgr->AddRollbackTrxTask(xid, nullptr);
    task = taskMgr->GetNextRollbackTrxTask();
    ASSERT_NE(task, nullptr);
    ASSERT_EQ(task->rollbackXid, xid);
    ASSERT_EQ(task->rollbackUndoZone, nullptr);

    ASSERT_EQ(taskMgr->IsAllTaskFinished(), true);
}

TEST_F(UTRollbackTrxTaskMgr, TaskMgrWorkerSetDoTest)
{
    RollbackTrxTaskMgr *taskMgr =
            g_storageInstance->GetPdb(g_defaultPdbId)->GetTransactionMgr()->m_rollbackTrxTaskMgr;

    UndoMgr *undoMgr = DstoreNew(m_ut_memory_context) UndoMgr(m_bufferMgr, m_pdbId);
    undoMgr->Init(g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));
    
    undoMgr->LoadUndoMapSegment();
    constexpr uint32 UNDO_ZONE_TEST_COUNT = 20;

    ZoneId *zidArr = (ZoneId *)DstorePalloc0(sizeof(ZoneId) * UNDO_ZONE_TEST_COUNT);
    for (uint32 i = 0; i < UNDO_ZONE_TEST_COUNT; i++) {
        zidArr[i] = INVALID_ZONE_ID;
    }

    for (uint32 i = 0; i < UNDO_ZONE_TEST_COUNT; i++) {
        undoMgr->AllocateZoneId(*(zidArr + i));
        ASSERT_EQ(zidArr[i], ZoneId(i));
    }

    for (uint32 i = 0; i < UNDO_ZONE_TEST_COUNT; i++) {
        ASSERT_NE(zidArr[i], INVALID_ZONE_ID);
        UndoZone *outUzone = nullptr;
        ASSERT_EQ(undoMgr->GetUndoZone(zidArr[i], &outUzone, true), DSTORE_SUCC);
        Xid xid = outUzone->AllocSlot();
        ASSERT_EQ(taskMgr->AddRollbackTrxTask(xid, outUzone), DSTORE_SUCC);
        ASSERT_EQ(outUzone->IsAsyncRollbacking(), false);
        outUzone->SetAsyncRollbackState(true);
    }

    uint32 maxWorkerNum = taskMgr->m_maxWorkerNum;

    for (uint32 i = 0; i < maxWorkerNum; ++i) {
        RollbackTrxWorker *worker = taskMgr->GetNextIdleWorker();
        ASSERT_NE(worker, nullptr);
        RollbackTrxTask *task = taskMgr->GetNextRollbackTrxTask();
        ASSERT_NE(task, nullptr);
        worker->SetTask(task);
    }
    RollbackTrxWorker *worker = taskMgr->GetNextIdleWorker();
    ASSERT_EQ(worker, nullptr);

    for (uint32 i = 0; i < maxWorkerNum; ++i) {
        worker = (taskMgr->m_workers)[i];
        worker->Run();
    }
    for (uint32 i = 0; i < maxWorkerNum; ++i) {
        worker = (taskMgr->m_workers)[i];
        while (worker->IsRunning()) {
            sleep(1);
        }
    }
    for (uint32 i = 0; i < maxWorkerNum; ++i) {
        worker = taskMgr->GetNextIdleWorker();
        ASSERT_NE(worker, nullptr);
        RollbackTrxTask *task = taskMgr->GetNextRollbackTrxTask();
        ASSERT_NE(task, nullptr);
        worker->SetTask(task);
        worker->Run();
    }

    for (uint32 i = 0; i < maxWorkerNum; ++i) {
        worker = (taskMgr->m_workers)[i];
        while (worker->IsRunning()) {
            sleep(1);
        }
    }

    ASSERT_EQ(taskMgr->IsAllTaskFinished(), true);
    for (uint32 i = 0; i < UNDO_ZONE_TEST_COUNT; i++) {
        ASSERT_NE(zidArr[i], INVALID_ZONE_ID);
        UndoZone *outUzone = nullptr;
        ASSERT_EQ(undoMgr->GetUndoZone(zidArr[i], &outUzone), DSTORE_SUCC);
        ASSERT_EQ(outUzone->IsAsyncRollbacking(), false);
    }
}

TEST_F(UTRollbackTrxTaskMgr, TaskMgrWorkerDispatchTest)
{
    RollbackTrxTaskMgr *taskMgr =
            g_storageInstance->GetPdb(g_defaultPdbId)->GetTransactionMgr()->m_rollbackTrxTaskMgr;

    UndoMgr *undoMgr = DstoreNew(m_ut_memory_context) UndoMgr(m_bufferMgr, m_pdbId);
    undoMgr->Init(g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));
    
    undoMgr->LoadUndoMapSegment();
    constexpr uint32 UNDO_ZONE_TEST_COUNT = 20;

    ZoneId *zidArr = (ZoneId *)DstorePalloc0(sizeof(ZoneId) * UNDO_ZONE_TEST_COUNT);
    for (uint32 i = 0; i < UNDO_ZONE_TEST_COUNT; i++) {
        zidArr[i] = INVALID_ZONE_ID;
    }

    for (uint32 i = 0; i < UNDO_ZONE_TEST_COUNT; i++) {
        undoMgr->AllocateZoneId(*(zidArr + i));
        ASSERT_EQ(zidArr[i], ZoneId(i));
    }

    for (uint32 i = 0; i < UNDO_ZONE_TEST_COUNT; i++) {
        ASSERT_NE(zidArr[i], INVALID_ZONE_ID);
        UndoZone *outUzone = nullptr;
        ASSERT_EQ(undoMgr->GetUndoZone(zidArr[i], &outUzone, true), DSTORE_SUCC);
        Xid xid = outUzone->AllocSlot();
        ASSERT_EQ(taskMgr->AddRollbackTrxTask(xid, outUzone), DSTORE_SUCC);
        ASSERT_EQ(outUzone->IsAsyncRollbacking(), false);
        outUzone->SetAsyncRollbackState(true);
    }

    taskMgr->StartDispatch();
    while (!taskMgr->IsAllTaskFinished()) {
        sleep(1);
    }
    taskMgr->StopDispatch();
    while (taskMgr->IsDispatching()) {
        sleep(1);
    }
    taskMgr->Destroy();
    ASSERT_EQ(taskMgr->m_workers, nullptr);
    ASSERT_EQ(taskMgr->m_dispatchThread, nullptr);

    for (uint32 i = 0; i < UNDO_ZONE_TEST_COUNT; i++) {
        ASSERT_NE(zidArr[i], INVALID_ZONE_ID);
        UndoZone *outUzone = nullptr;
        ASSERT_EQ(undoMgr->GetUndoZone(zidArr[i], &outUzone), DSTORE_SUCC);
        ASSERT_EQ(outUzone->IsAsyncRollbacking(), false);
    }
}