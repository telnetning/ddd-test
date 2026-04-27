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
 * dstore_wal_redo_manager.h
 *
 *
 *
 * IDENTIFICATION
 *        storage/include/wal/dstore_wal_redo_manager.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_WAL_REDO_MANAGER_H
#define DSTORE_WAL_REDO_MANAGER_H
#include <atomic>
#include <mutex>
#include <condition_variable>
#include <thread>
#include "common/dstore_datatype.h"
#include "common/memory/dstore_mctx.h"
#include "common/algorithm/dstore_dynahash.h"
#include "common/algorithm/dstore_ilist.h"
#include "transaction/dstore_transaction_types.h"
#include "common/concurrent/dstore_atomic.h"
namespace DSTORE {
#define WAL_MAX_BIND_CPU_SIZE 100
#define PARSE_REDO_BIND_CPU_INFO_COUNT 2
struct RedoTask {
    uint64 term;
    uint32 walCount;
    WalId *walIds;
    std::thread *procThrd;
};

struct RedoTaskFinish {
    dlist_node dlistNode;
    uint64 term;
};

struct TransactionNeedRollbackInfo {
    uint64 csn;
    Xid xid;
};

struct TransactionsNeedRollbackListNode {
    TransactionNeedRollbackInfo info;
    dlist_node dlistNode;
};

/*
 * WalRedoManager is used to manage redo task.
 * The redo task is including:
 * - wal replay
 * - async recovery undo
 * - checkpoint after wal replay
 * - broadcast wal complete term
 *
 * When the recovery node take over the wal streams, if there are some wal streams that need to be replayed, it will
 * use 'StartRedo' to start a redo task. StartRedo will not wait redo task finish, it just record the task and create
 * a new thread to process the task and the function returns.
 *
 * The redo task is processed by the new created thread, and after it finish the task, it will notify the cleanup thread
 * to remove the redo task record.
 *
 * The CleanFinishRedoTask thread is a bankend thread, it mainly check the redoTaskCleanupList, and process redo task
 * clean up when the list is not empty in the loop.
 *
 * When the WalRedoManager needs to be destroyed, it will reject new incoming redo task and then notify
 * CleanFinishRedoTask thread to stop. The CleanFinishRedoTask thread will not exit until there is no redo task is
 * processing. After that the WalRedoManager can be destroyed safely.
 */
class WalRedoManager {
public:
    WalRedoManager();
    ~WalRedoManager() = default;

    /*
     * Initialize the WalRedoManager and start CleanFinishRedoTask backend thread
     */
    void InitWalRedoManager(PdbId pdbId);
    /*
     * Wait redo task finish, and stop the backend cleanup thread, destroy the WalRedoManager
     */
    void DestroyWalRedoManager();

    /*
     * Provide an interface to query whether redo tasks can be acceptted, epecially with close PDB operations.
     */
    bool CanAcceptRedoTask();

    /* This value should be true if takes over while closing pdb. */
    void SetTakeOverRejectRedo();

    bool IsTakeOverRejectRedo();

    uint64 GetRedoTaskNum();
    /*
     * Wait for the specified term of memberview processing finish
     */
    void WaitRedoTaskFinish(uint64 term);
    /*
     * Wait for all the redo task processing finish
     */
    void WaitAllRedoTaskFinish();

    /*
     * Push one transaction that need to rollback into rollabck list.
     */
    void PushTransactionIntoRollbackList(TransactionsNeedRollbackListNode *node);

    /*
     * Get all transactions that need to rollback.
     */
    RetStatus GetAllTransactionsNeedRollback(TransactionNeedRollbackInfo **rollbackArray, uint64 &totalRollbackNum);

    /*
     * Stop RedoTaskCleanup thread
     */
    void StopBgThread();

    static void GetBindCpus(char* redoBindCpuAttrIter, uint32 *cpus, uint32 *cpuCount);
    static void ParseRedoBindCpuInfo(bool *isBindCpu, uint32 *cpus, uint32 *cpuCount);

    static RetStatus GetNextAwailableCpu(uint32 *targetCpu);

    bool HasInited();
    uint64 GetTermRedoProgress(uint64 term);

#ifdef UT
    RetStatus UtAddRedoTask(uint64 term);
    RetStatus UtDelRedoTask(uint64 term);
    long UtGetRedoTaskNum();
    bool UtIsRedoTaskFinish(uint64 term);
    void UtRejectRedoTask();
#endif
private:
    static void CleanFinishRedoTask(WalRedoManager *redoMgr);
    void NotifyRedoTaskFinish(uint64 term);
    void CleanFinishRedoTaskMainLoop();
    void RejectRedoTask();
    void StopRedoTaskCleanupThrd();

    PdbId pdbId;
    DstoreMemoryContext redoMemoryContext;
    std::mutex redoTaskMutex;
    std::condition_variable redoFinishCv;
    HTAB *redoTaskHtab;
    bool canAcceptRedoTask;
    bool takeOverRejectRedo{
        false}; /* This varaiable is for avoding setting canAcceptRedoTask to false after taking over walstreams. */

    std::mutex redoTaskCleanupMutex;
    std::condition_variable redoTaskCleanupCv;
    dlist_head redoTaskCleanupList;
    std::atomic_bool stopCleanupRedoTask;
    std::thread *redoTaskCleanupThrd;
    bool isInited;

    /* Stores transactional information that needs to be rolled back for backup restoration. */
    dlist_head m_transactionsNeedRollbackListHead;
    uint64 m_numOfTransactionsNeedRollback;
    static bool m_isRedoBindCpu;
    static uint32 m_cpus[WAL_MAX_BIND_CPU_SIZE];
    static uint32 m_cpuCount;
    static gs_atomic_uint32 m_nextAvailableCpu;
};
}

#endif