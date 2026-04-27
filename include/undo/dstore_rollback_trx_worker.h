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
 * dstore_rollback_trx_worker.h
 *     This file defines the building blocks that bg page writer needs.
 *
 * IDENTIFICATION
 *        include/undo/dstore_rollback_trx_worker.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_ROLLBACK_TRX_WORKER_H
#define DSTORE_ROLLBACK_TRX_WORKER_H

#include <atomic>
#include <thread>

#include "transaction/dstore_transaction_types.h"
#include "undo/dstore_undo_zone.h"
#include "undo/dstore_rollback_trx_task_mgr.h"

namespace DSTORE {

#ifdef UT
#define private public
#endif

class RollbackTrxWorker : public BaseObject {
public:
    explicit RollbackTrxWorker(DstoreMemoryContext memoryContext, RollbackTrxTaskMgr *rollbackTrxTaskMgr, PdbId pdbId);
    void SetTask(RollbackTrxTask *rollbackTask);
    void Run();
    bool IsRunning() const;
    PdbId GetPdbId();
private:
    PdbId m_pdbId;
    DstoreMemoryContext m_memoryContext;
    RollbackTrxTask *m_rollbackTrxTask;
    RollbackTrxTaskMgr *m_rollbackTrxTaskMgr;

    std::atomic<bool> m_isRunning;
    RetStatus m_rollbackResult;

    void WorkerMain();
    RetStatus DoRollback();
    void Destroy();
    /* Index may need access SQL thrd, ERR_LEVEL_FATAL may happen when init SQL thrd, then current thread may be
     * killed, so need create sub-thrd to init SQL thrd to avoid dstore resource leaks. */
    void RollbackSubMain();
};

#ifdef UT
#undef private
#endif

}  /* namespace DSTORE */

#endif  /* STORAGE_ROLLBACK_TRX_WORKER_H */