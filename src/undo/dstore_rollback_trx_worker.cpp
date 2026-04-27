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
 *
 * ---------------------------------------------------------------------------------------
 *
 * dstore_rollback_trx_worker.cpp
 *
 * IDENTIFICATION
 *        src/undo/dstore_rollback_trx_worker.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include <csignal>

#include "lock/dstore_xact_lock_mgr.h"
#include "transaction/dstore_transaction_mgr.h"
#include "undo/dstore_undo_zone_txn_mgr.h"
#include "undo/dstore_rollback_trx_worker.h"

namespace DSTORE {

RollbackTrxWorker::RollbackTrxWorker(DstoreMemoryContext memoryContext, RollbackTrxTaskMgr *rollbackTrxTaskMgr,
    PdbId pdbId)
    : m_pdbId(pdbId), m_memoryContext(memoryContext), m_rollbackTrxTask(nullptr),
    m_rollbackTrxTaskMgr(rollbackTrxTaskMgr), m_isRunning(false), m_rollbackResult(DSTORE_FAIL) {}

void RollbackTrxWorker::SetTask(RollbackTrxTask *rollbackTask)
{
    StorageAssert(m_rollbackTrxTask == nullptr);
    StorageAssert(!m_isRunning);

    m_isRunning.store(true, std::memory_order_release);
    m_rollbackTrxTask = rollbackTask;
}

void RollbackTrxWorker::Run()
{
    StorageReleasePanic(m_rollbackTrxTask == nullptr, MODULE_UNDO,
                        ErrMsg("Rollback task is null, pdbId[%u].", m_pdbId));
    std::thread workThread(&RollbackTrxWorker::WorkerMain, this);
    workThread.detach();
}

bool RollbackTrxWorker::IsRunning() const
{
    return m_isRunning.load(std::memory_order_acquire);
}

void RollbackTrxWorker::RollbackSubMain()
{
    InitSignalMask();
    void *sqlThrd = nullptr;
    SQLThrdInitCtx context = {m_pdbId, "RollbackSubThrd", InternalThreadType::THREAD_ROLLBACK_WORKER,
                              &sqlThrd, false};
    StorageAssert(thrd == nullptr);
    bool initResult = g_storageInstance->InitSQLThreadContext(&context);
    if (!initResult) {
        ErrLog(DSTORE_ERROR, MODULE_UNDO, ErrMsg("RollbackSubMain sql thread init fail, pdbId[%u]", m_pdbId));
        return;
    }

    if (thrd == nullptr) {
        g_storageInstance->CreateThreadAndRegister(m_pdbId, false, "RollbackSubThrd", true,
                                                   ThreadMemoryLevel::THREADMEM_MEDIUM_PRIORITY);
    }
    if (unlikely(thrd == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_UNDO, ErrMsg("RollbackSubMain thrd init fail, pdbId[%u].", m_pdbId));
        return;
    }
    thrd->SetNeedCommBuffer(true);
#ifdef __aarch64__
    thrd->SetNumaId(1);
#endif
    thrd->RefreshWorkingVersionNum();
    m_rollbackResult = DoRollback();
    g_storageInstance->ReleaseSQLThreadContext(&context);
    if (thrd != nullptr) {
        g_storageInstance->UnregisterThread();
    }
}

void RollbackTrxWorker::WorkerMain()
{
    InitSignalMask();
    StorageAssert(thrd == nullptr);
    (void)g_storageInstance->CreateThreadAndRegister(m_pdbId, false, "RollbackTrxWker", true,
                                                     ThreadMemoryLevel::THREADMEM_MEDIUM_PRIORITY);
    if (unlikely(thrd == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_UNDO,
               ErrMsg("RollbackTrxWorker create thread fail, pdbId[%u], xid(%d, %lu).", m_pdbId,
                      static_cast<int>(m_rollbackTrxTask->rollbackXid.m_zoneId),
                      m_rollbackTrxTask->rollbackXid.m_logicSlotId));
        (void)m_rollbackTrxTaskMgr->AddRollbackTrxTask(m_rollbackTrxTask->rollbackXid,
                                                       m_rollbackTrxTask->rollbackUndoZone);
        Destroy();
        m_rollbackTrxTaskMgr->WakeupDispatch();
        return;
    }
#ifdef __aarch64__
    thrd->SetNumaId(1);
#endif

    ErrLog(DSTORE_DEBUG1, MODULE_UNDO,
           ErrMsg("Async rollback undo zone, pdbId[%u], xid(%d, %lu).", m_pdbId,
                  static_cast<int>(m_rollbackTrxTask->rollbackXid.m_zoneId),
                  m_rollbackTrxTask->rollbackXid.m_logicSlotId));
    /* Index may need access SQL thrd, ERR_LEVEL_FATAL may happen when init SQL thrd, then current thread may be
     * killed, so need create sub-thrd to init SQL thrd to avoid dstore resource leaks. */
    m_rollbackResult = DSTORE_FAIL;
    std::thread rollbackSubThread(&RollbackTrxWorker::RollbackSubMain, this);
    if (rollbackSubThread.joinable()) {
        rollbackSubThread.join();
    }
    if (likely(m_rollbackResult == DSTORE_SUCC)) {
        m_rollbackTrxTask->rollbackUndoZone->SetAsyncRollbackState(false);
        m_rollbackTrxTask->rollbackUndoZone = nullptr;
    } else {
        ErrLog(DSTORE_LOG, MODULE_UNDO,
               ErrMsg("Async rollback undo zone failed, re-add rollback task, pdbId[%u], xid(%d, %lu).", m_pdbId,
                      static_cast<int>(m_rollbackTrxTask->rollbackXid.m_zoneId),
                      m_rollbackTrxTask->rollbackXid.m_logicSlotId));
        (void)m_rollbackTrxTaskMgr->AddRollbackTrxTask(m_rollbackTrxTask->rollbackXid,
                                                       m_rollbackTrxTask->rollbackUndoZone);
    }

    g_storageInstance->UnregisterThread();

    Destroy();
    m_rollbackTrxTaskMgr->WakeupDispatch();
}

RetStatus RollbackTrxWorker::DoRollback()
{
    StorageAssert(m_rollbackTrxTask != nullptr);
    StorageAssert(m_isRunning);
    if (m_rollbackTrxTask) {
        return m_rollbackTrxTask->rollbackUndoZone->RollbackUndoZone(m_rollbackTrxTask->rollbackXid, true);
    } else {
        ErrLog(DSTORE_ERROR, MODULE_UNDO, ErrMsg("Async rollback undo zone get a null task ptr."));
        return DSTORE_FAIL;
    }
}

void RollbackTrxWorker::Destroy()
{
    if (m_rollbackTrxTask) {
        delete m_rollbackTrxTask;
        m_rollbackTrxTask = nullptr;
    }

    m_isRunning.store(false, std::memory_order_release);
}

PdbId RollbackTrxWorker::GetPdbId()
{
    return m_pdbId;
}

}  // namespace DSTORE