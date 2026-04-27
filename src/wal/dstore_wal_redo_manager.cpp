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
 * dstore_wal_redo_manager.cpp
 *
 *
 *
 * IDENTIFICATION
 *        storage/src/wal/dstore_wal_redo_manager.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "wal/dstore_wal_redo_manager.h"

#include <random>
#include <cctype>
#include "port/dstore_port.h"
#include "framework/dstore_instance.h"
#include "wal/dstore_wal.h"
#include "lock/dstore_lock_interface.h"
#include "wal/dstore_wal_logstream.h"
namespace DSTORE {
const uint32 BACKUP_RESTORE_HEAD_MEMBER = 2;

bool WalRedoManager::m_isRedoBindCpu = false;
uint32 WalRedoManager::m_cpuCount = 0;
uint32 WalRedoManager::m_cpus[WAL_MAX_BIND_CPU_SIZE] = {0};
gs_atomic_uint32 WalRedoManager::m_nextAvailableCpu = 0;
WalRedoManager::WalRedoManager() : pdbId{INVALID_PDB_ID}, redoMemoryContext{nullptr},
    redoTaskMutex{}, redoFinishCv{}, redoTaskHtab{nullptr}, canAcceptRedoTask{false},
    redoTaskCleanupMutex{}, redoTaskCleanupCv{},
    redoTaskCleanupList{}, stopCleanupRedoTask{false}, redoTaskCleanupThrd{nullptr}, isInited{false},
    m_transactionsNeedRollbackListHead({nullptr, nullptr}),
    m_numOfTransactionsNeedRollback(0) {}

void WalRedoManager::InitWalRedoManager(PdbId id)
{
    if (isInited) {
        return;
    }
    this->pdbId = id;
    this->redoMemoryContext = DstoreAllocSetContextCreate(
        g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LONGLIVE), "RedoManagerMemoryContext",
        ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE,
        MemoryContextType::SHARED_CONTEXT);
    StorageReleasePanic(!redoMemoryContext, MODULE_WAL, ErrMsg("Failed to alloc RedoManagerMemoryContext."));

     /* Create shared hash table */
    HASHCTL info;
    info.keysize = sizeof(uint64);
    info.entrysize = sizeof(RedoTask);
    info.hash = tag_hash;
    info.hcxt = redoMemoryContext;

    const long hashTableInitSize = 10;
    this->redoTaskHtab = hash_create("Redo Task Hash Table", hashTableInitSize, &info,
        HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT | HASH_SHRCTX);
    this->canAcceptRedoTask = true;
    this->stopCleanupRedoTask.store(false);
    DListInit(&redoTaskCleanupList);
    this->redoTaskCleanupThrd = new std::thread(CleanFinishRedoTask, this);
    isInited = true;
}

void WalRedoManager::StopBgThread()
{
    /* reject incoming redo task */
    RejectRedoTask();
    /* stop redo task cleanup thread and wait for all redo task finish */
    StopRedoTaskCleanupThrd();
    /* wait for all redo task finish */
    WaitAllRedoTaskFinish();
    /* make sure no redo task is running */
    StorageAssert(DListIsEmpty(&redoTaskCleanupList));
}

void WalRedoManager::DestroyWalRedoManager()
{
    if (!isInited) {
        return;
    }
    /* reject incoming redo task */
    RejectRedoTask();
    /* stop redo task cleanup thread and wait for all redo task finish */
    StopRedoTaskCleanupThrd();
    ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("[Replica] pdb (%u) StopRedoTaskCleanupThrd.", pdbId));
    /* wait for all redo task finish */
    WaitAllRedoTaskFinish();
    ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("[Replica] pdb (%u) WaitAllRedoTaskFinish.", pdbId));
    /* make sure no redo task is running */
    StorageAssert(DListIsEmpty(&redoTaskCleanupList));
    hash_destroy(redoTaskHtab);
    DstoreMemoryContextDelete(redoMemoryContext);
    redoMemoryContext = nullptr;
    redoTaskHtab = nullptr;
    isInited = false;
    pdbId = INVALID_PDB_ID;
}

bool WalRedoManager::HasInited()
{
    return isInited;
}

void WalRedoManager::RejectRedoTask()
{
    std::unique_lock<std::mutex> lock{redoTaskMutex};
    canAcceptRedoTask = false;
}

void WalRedoManager::WaitRedoTaskFinish(uint64 term)
{
    bool isRedoTaskFinish = false;
    while (!isRedoTaskFinish) {
        std::unique_lock<std::mutex> lock{redoTaskMutex};
        bool isFound = false;
        /* check if the specified term of the redo task is processing */
        (void)hash_search(redoTaskHtab, &term, HASHACTION::HASH_FIND, &isFound);
        if (!isFound) {
            /* the redo task is processed finish */
            isRedoTaskFinish = true;
        } else {
            /* wait if the redo task is not processed finish */
            redoFinishCv.wait(lock, [this, term]() -> bool {
                bool hasTask = false;
                (void)hash_search(redoTaskHtab, &term, HASHACTION::HASH_FIND, &hasTask);
                return !hasTask;
            });
        }
    }
}

void WalRedoManager::WaitAllRedoTaskFinish()
{
    bool isAllRedoTaskFinish = false;
    while (!isAllRedoTaskFinish) {
        std::unique_lock<std::mutex> lock{redoTaskMutex};
        /* check if all the redo task is processed finish */
        long taskNum = hash_get_num_entries(redoTaskHtab);
        if (taskNum == 0) {
            /* all the redo task is processed finish */
            isAllRedoTaskFinish = true;
        } else {
            /* wait if there are still some redo task is processing */
            redoFinishCv.wait(lock, [this]() -> bool {
                long num = hash_get_num_entries(redoTaskHtab);
                return num == 0;
            });
        }
    }
}

bool WalRedoManager::CanAcceptRedoTask()
{
    return canAcceptRedoTask;
}

void WalRedoManager::SetTakeOverRejectRedo()
{
    takeOverRejectRedo = true;
}

bool WalRedoManager::IsTakeOverRejectRedo()
{
    return takeOverRejectRedo;
}

uint64 WalRedoManager::GetRedoTaskNum()
{
    std::unique_lock<std::mutex> lock{redoTaskMutex};
    long taskNum = hash_get_num_entries(redoTaskHtab);
    return taskNum;
}

void WalRedoManager::CleanFinishRedoTask(WalRedoManager *redoMgr)
{
    redoMgr->CleanFinishRedoTaskMainLoop();
}

void WalRedoManager::CleanFinishRedoTaskMainLoop()
{
    (void)g_storageInstance->CreateThreadAndRegister(pdbId, false, "RedoTaskCleanup", true,
                                                     ThreadMemoryLevel::THREADMEM_MEDIUM_PRIORITY);

    AutoMemCxtSwitch autoSwitch{redoMemoryContext};
    ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("WalRedoManager clean finish redo task thread start."));
    while (true) {
        /* check if it should stop CleanupRedoTask thread */
        if (stopCleanupRedoTask.load(std::memory_order_acquire)) {
            std::unique_lock<std::mutex> redoHtabLock{redoTaskMutex};
            long taskNum = hash_get_num_entries(redoTaskHtab);
            /* if there are no processing redo task, break the loop. otherwise it still in the loop and
             * wait all the redo task finish
             */
            if (taskNum == 0) {
                break;
            }
        }
        std::unique_lock<std::mutex> listLock{redoTaskCleanupMutex};
        /* check if the redoTaskCleanupList is empty, if it is not empty, process the cleanup task,
         * otherwise wait for the incoming cleanup task.
         */
        if (DListIsEmpty(&redoTaskCleanupList)) {
            redoTaskCleanupCv.wait(listLock, [this]() -> bool {
                return !DListIsEmpty(&this->redoTaskCleanupList) || stopCleanupRedoTask.load(std::memory_order_acquire);
            });
            listLock.unlock();
            continue;
        }
        dlist_node *node = DListPopTailNode(&redoTaskCleanupList);
        listLock.unlock();

        /* clean the finish redo task */
        RedoTaskFinish *redoTaskFinish = dlist_container(RedoTaskFinish, dlistNode, node);
        std::unique_lock<std::mutex> htabLock{redoTaskMutex};
        htabLock.unlock();
        DstorePfreeExt(redoTaskFinish);
    }
    ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("WalRedoManager clean finish redo task thread exit."));
    g_storageInstance->UnregisterThread(false);
}

void WalRedoManager::NotifyRedoTaskFinish(uint64 term)
{
    AutoMemCxtSwitch autoSwitch{redoMemoryContext};
    RedoTaskFinish *entry = static_cast<RedoTaskFinish *>(DstorePalloc(sizeof(RedoTaskFinish)));
    while (STORAGE_VAR_NULL(entry)) {
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("[PDB:%u]WalRecovery RedoTaskFinish alloc failed!", pdbId));
        GaussUsleep(WAL_WAIT_MEMORY_AVAILABLE_TIME);
        entry = static_cast<RedoTaskFinish *>(DstorePalloc(sizeof(RedoTaskFinish)));
    }
    DListNodeInit(&entry->dlistNode);
    entry->term = term;
    std::unique_lock<std::mutex> lock{redoTaskCleanupMutex};
    DListPushHead(&redoTaskCleanupList, &entry->dlistNode);
    redoTaskCleanupCv.notify_all();
}

void WalRedoManager::StopRedoTaskCleanupThrd()
{
    /* set stopCleanupRedoTask as true */
    this->stopCleanupRedoTask.store(true);
    /* wake up redo task clean up thread */
    {
        std::unique_lock<std::mutex> listLock{redoTaskCleanupMutex};
        redoTaskCleanupCv.notify_all();
    }

    if (this->redoTaskCleanupThrd != nullptr) {
        if (this->redoTaskCleanupThrd->joinable()) {
            /* wait for redo task clean up thread exit */
            this->redoTaskCleanupThrd->join();
        }
        ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("redoTaskCleanupThrd."));
        /* clean resource */
        delete this->redoTaskCleanupThrd;
        this->redoTaskCleanupThrd = nullptr;
    }
}

void WalRedoManager::GetBindCpus(char* redoBindCpuAttrIter, uint32 *cpus, uint32 *cpuCount)
{
    uint32 bindCpuStart;
    uint32 bindCpuEnd;
    while (*redoBindCpuAttrIter != '\0') {
        if (sscanf_s(redoBindCpuAttrIter, "%u-%u", &bindCpuStart, &bindCpuEnd) == PARSE_REDO_BIND_CPU_INFO_COUNT) {
            for (uint32 i = bindCpuStart; i <= bindCpuEnd && *cpuCount < WAL_MAX_BIND_CPU_SIZE; i++) {
                cpus[(*cpuCount)++] = i;
            }
            if (*cpuCount == WAL_MAX_BIND_CPU_SIZE) return;
            while (*redoBindCpuAttrIter != '\0' && *redoBindCpuAttrIter != ',') redoBindCpuAttrIter++;
        } else {
            redoBindCpuAttrIter++;
        }

        if (*redoBindCpuAttrIter == ',') redoBindCpuAttrIter++;
    }
}

void WalRedoManager::ParseRedoBindCpuInfo(bool *isBindCpu, uint32 *cpus, uint32 *cpuCount)
{
    char* redoBindCpuAttr = static_cast<char*>(DstorePalloc0(strlen(g_storageInstance->GetGuc()->redoBindCpuAttr) + 1));
    while (STORAGE_VAR_NULL(redoBindCpuAttr)) {
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("ParseRedoBindCpuInfo alloc redoBindCpuAttr failed!"));
        GaussUsleep(WAL_WAIT_MEMORY_AVAILABLE_TIME);
        redoBindCpuAttr = static_cast<char*>(DstorePalloc0(strlen(g_storageInstance->GetGuc()->redoBindCpuAttr) + 1));
    }
    char *redoBindCpuAttrIter = redoBindCpuAttr;
    for (char* iter = g_storageInstance->GetGuc()->redoBindCpuAttr; *iter != '\0'; iter++) {
        if (!isspace((int)*iter)) {
            *redoBindCpuAttrIter = tolower((int)*iter);
            redoBindCpuAttrIter++;
        }
    }
    *redoBindCpuAttrIter = '\0';

    if (strcmp(redoBindCpuAttr, "nobind") == 0) {
        *isBindCpu = false;
        DstorePfree(redoBindCpuAttr);
        return;
    }

    if (strncmp(redoBindCpuAttr, "cpubind", strlen("cpubind")) == 0) {
        *isBindCpu = true;
        *cpuCount = 0;
        WalRedoManager::GetBindCpus(redoBindCpuAttr + strlen("cpubind:"), cpus, cpuCount);
        if (*cpuCount == 0) {
            ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("ParseRedoBindCpuInfo %s failed.", redoBindCpuAttr));
            *isBindCpu = false;
        }
        DstorePfree(redoBindCpuAttr);
        return;
    }

    ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("ParseRedoBindCpuInfo %s failed.", redoBindCpuAttr));
    *isBindCpu = false;
    DstorePfree(redoBindCpuAttr);
    return;
}

RetStatus WalRedoManager::GetNextAwailableCpu(uint32 *targetCpu)
{
    if (m_isRedoBindCpu) {
        *targetCpu = m_cpus[GsAtomicReadU32(&m_nextAvailableCpu)];
        if (GsAtomicAddFetchU32(&m_nextAvailableCpu, 1) == m_cpuCount) {
            GsAtomicWriteU32(&m_nextAvailableCpu, 0);
        }
        return DSTORE_SUCC;
    }
    return DSTORE_FAIL;
}
#ifdef UT

long WalRedoManager::UtGetRedoTaskNum()
{
    std::unique_lock<std::mutex> redoHtabLock{redoTaskMutex};
    long taskNum = hash_get_num_entries(redoTaskHtab);
    return taskNum;
}

bool WalRedoManager::UtIsRedoTaskFinish(uint64 term)
{
    std::unique_lock<std::mutex> redoHtabLock{redoTaskMutex};
    bool isFound = false;
    (void)hash_search(redoTaskHtab, &term, HASHACTION::HASH_FIND, &isFound);
    return !isFound;
}

void WalRedoManager::UtRejectRedoTask()
{
    std::unique_lock<std::mutex> lock{redoTaskMutex};
    canAcceptRedoTask = false;
}

RetStatus WalRedoManager::UtAddRedoTask(uint64 term)
{
    AutoMemCxtSwitch autoSwitch{redoMemoryContext};
    std::unique_lock<std::mutex> lock{redoTaskMutex};
    /* check if it can accept new redo task */
    if (!canAcceptRedoTask) {
        storage_set_error(WAL_ERROR_REJECT_REDO_TASK);
        return DSTORE_FAIL;
    }
    /* record the redo task in htab */
    bool isFound = false;
    RedoTask *task = static_cast<RedoTask*>(
        hash_search(redoTaskHtab, &term, HASHACTION::HASH_ENTER, &isFound));

    task->walCount = 0;
    task->walIds = nullptr;
    task->procThrd = nullptr;

    return DSTORE_SUCC;
}

RetStatus WalRedoManager::UtDelRedoTask(uint64 term)
{
    AutoMemCxtSwitch autoSwitch{redoMemoryContext};
    std::unique_lock<std::mutex> htabLock{redoTaskMutex};
    bool isFound = false;
    RedoTask *task = static_cast<RedoTask*>(
        hash_search(redoTaskHtab, &term, HASHACTION::HASH_REMOVE, &isFound));

    if (isFound) {
        if (task->walIds != nullptr) {
            DstorePfree(task->walIds);
        }
        if (task->procThrd != nullptr) {
            task->procThrd->join();
            delete task->procThrd;
        }
    }
    return DSTORE_SUCC;
}
#endif

RetStatus WalRedoManager::GetAllTransactionsNeedRollback(
    TransactionNeedRollbackInfo **rollbackArray, uint64 &totalRollbackNum)
{
    uint64 totalNum = m_numOfTransactionsNeedRollback;
    if (totalNum == 0) {
        ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("Noneed rollback."));
        return DSTORE_SUCC;
    }
    *rollbackArray = static_cast<TransactionNeedRollbackInfo *>(
        DstorePalloc(totalNum * sizeof(TransactionNeedRollbackInfo)));
    if (unlikely(*rollbackArray == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("GetAllTransactionsNeedRollback palloc failed, size: %lu",
            totalNum * sizeof(TransactionNeedRollbackInfo)));
        return DSTORE_FAIL;
    }

    uint64 curRollbackCnt = 0;
    dlist_iter iter;
    dlist_foreach(iter, &m_transactionsNeedRollbackListHead) {
        TransactionsNeedRollbackListNode *listNode = dlist_container(TransactionsNeedRollbackListNode,
            dlistNode, iter.cur);
        (*rollbackArray)[curRollbackCnt].csn = listNode->info.csn;
        (*rollbackArray)[curRollbackCnt].xid = listNode->info.xid;
        DstorePfreeExt(listNode);
        curRollbackCnt++;
        ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("GetAllTransactionsNeedRollback curRollbackCnt:%lu",
            curRollbackCnt));
    }

    if (unlikely(curRollbackCnt != totalNum)) {
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("GetAllTransactionsNeedRollback curRollbackCnt:%lu totalNum:%lu",
            curRollbackCnt, totalNum));
    }
    totalRollbackNum = totalNum;
    return DSTORE_SUCC;
}

void WalRedoManager::PushTransactionIntoRollbackList(TransactionsNeedRollbackListNode *node)
{
    DListPushTail(&m_transactionsNeedRollbackListHead, &node->dlistNode);
    m_numOfTransactionsNeedRollback++;
}

uint64 WalRedoManager::GetTermRedoProgress(uint64 term)
{
    std::unique_lock<std::mutex> lock{redoTaskMutex};
    bool isFound = false;
    RedoTask *task = static_cast<RedoTask *>(hash_search(redoTaskHtab, &term, HASHACTION::HASH_ENTER, &isFound));
    uint64 progress = UINT64_MAX;
    if (isFound) {
        for (uint32 idx = 0; idx < task->walCount; idx++) {
            if (unlikely(task->walIds[idx] == INVALID_WAL_ID)) {
                continue;
            }
            StoragePdb *pdb = g_storageInstance->GetPdb(pdbId);
            StorageReleasePanic(pdb == nullptr, MODULE_WAL, ErrMsg("Get pdb failed, pdbId(%u).", pdbId));
            if (pdb->GetWalMgr()->GetWalStreamManager()->GetWalStream(task->walIds[idx]) == nullptr) {
                continue;
            }
            WalRedoInfo walRedoInfo;
            walRedoInfo.Init();
            pdb->GetWalMgr()->GetWalStreamManager()->GetWalStream(task->walIds[idx])->GetRedoInfo(&walRedoInfo);
            progress = DstoreMin(progress, walRedoInfo.progress);
        }
    }
    return progress;
}
}