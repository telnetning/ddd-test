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
 * dstore_wal.cpp
 *
 * IDENTIFICATION
 * src/wal/dstore_wal.cpp
 *
 * ---------------------------------------------------------------------------------------
 *
 */

#include "common/log/dstore_log.h"
#include "common/error/dstore_error.h"
#include "framework/dstore_thread.h"
#include "heap/dstore_heap_wal_struct.h"
#include "index/dstore_btree_wal.h"
#include "common/algorithm/dstore_ilist.h"
#include "wal/dstore_wal.h"
namespace DSTORE {

bool WalLogicalActive()
{
    static bool isLogicalActive =  g_storageInstance->GetGuc()->walLevel >= static_cast<int>(WalLevel::WAL_LEVEL_LOGICAL);
    return isLogicalActive;
}

WalManager::WalManager(DstoreMemoryContext memoryContext)
    : m_minLogicalRepRequired(INVALID_PLSN), m_memoryContext(memoryContext),
    m_streamManager(nullptr),
    m_redoManager(),
    m_initState(WalInitState::UNINITIALIZED),
    m_controlFile(nullptr),
    m_pdbId(INVALID_PDB_ID)
{
}

WalManager::~WalManager()
{
    if (m_streamManager != nullptr) {
        delete m_streamManager;
        m_streamManager = nullptr;
    }
    m_initState = WalInitState::UNINITIALIZED;
    m_memoryContext = nullptr;
    m_controlFile = nullptr;
    m_pdbId = INVALID_PDB_ID;
}

RetStatus WalManager::Init(ControlFile *controlFile)
{
    if (controlFile == nullptr) {
        storage_set_error(WAL_ERROR_UNREACHABLE_CODE);
        ErrLog(DSTORE_PANIC, MODULE_WAL,
            ErrMsg("WalManager Init fail cause controlFile is not exist."));
        return DSTORE_FAIL;
    }
    if (m_initState == WalInitState::INITIALIZED) {
        return DSTORE_SUCC;
    }
    m_controlFile = controlFile;
    m_pdbId = m_controlFile->GetPdbId();
    if (m_memoryContext == nullptr) {
        storage_set_error(WAL_ERROR_UNREACHABLE_CODE);
        ErrLog(DSTORE_PANIC, MODULE_WAL,
            ErrMsg("WalManager Init fail cause memory Context is not exist."));
        return DSTORE_FAIL;
    }

    /* Step 1: set init state to INITIALIZING */
    if (!ModifyInitState(WalInitState::UNINITIALIZED, WalInitState::INITIALIZING)) {
        return DSTORE_FAIL;
    }

    /* Step 2: StreamManager initialize */
    if (m_streamManager == nullptr) {
        m_streamManager = DstoreNew(m_memoryContext)WalStreamManager(m_memoryContext);
        if (unlikely(m_streamManager == nullptr)) {
            storage_set_error(WAL_ERROR_UNREACHABLE_CODE);
            ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("Allocate StreamManager memory failed"));
            return DSTORE_FAIL;
        }
    }
    RetStatus result = m_streamManager->Init(controlFile);
    if (result != DSTORE_SUCC) {
        delete m_streamManager;
        m_streamManager = nullptr;
        if (!ModifyInitState(WalInitState::INITIALIZING, WalInitState::UNINITIALIZED)) {
            ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("Wal Manager modify init state fail!"));
        }
        return DSTORE_FAIL;
    }

    /* Step 3: RedoManager initialize */
    m_redoManager.InitWalRedoManager(m_pdbId);

    /* Step 4: set init state to Init success */
    if (!ModifyInitState(WalInitState::INITIALIZING, WalInitState::INITIALIZED)) {
        ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("Wal Manager modify init state fail!"));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

void WalManager::Destroy()
{
    ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("pdb (%u) destroy WalManager start.",
        m_pdbId));
    m_redoManager.DestroyWalRedoManager();
    if (m_streamManager != nullptr) {
        delete m_streamManager;
        m_streamManager = nullptr;
    }
    ErrLog(DSTORE_LOG, MODULE_WAL, ErrMsg("pdb (%u) destroy WalManager finish.",
        m_pdbId));
    m_initState = WalInitState::UNINITIALIZED;
    m_controlFile = nullptr;
    m_pdbId = INVALID_PDB_ID;
}

bool WalManager::IsInited() const
{
    return m_initState == WalInitState::INITIALIZED;
}

bool WalManager::isUninit() const
{
    return m_initState == WalInitState::UNINITIALIZED;
}

WalStreamManager *WalManager::GetWalStreamManager()
{
    return m_streamManager;
}

WalRedoManager &WalManager::GetWalRedoManager()
{
    return m_redoManager;
}

bool WalManager::ModifyInitState(WalInitState oldState, WalInitState newState)
{
    if (m_initState != oldState) {
        storage_set_error(WAL_ERROR_INIT_ERROR);
        ErrLog(DSTORE_ERROR, MODULE_WAL,
            ErrMsg("Find oldState is not same as seted when try to modify WalManager Init state."));
        return false;
    }
    m_initState = newState;
    return true;
}

RetStatus WalManager::Recovery(WalId walId, PdbId pdbId, uint64 term, bool tryRecoveryFromDisk)
{
    WalStream *walStream = m_streamManager->GetWalStream(walId);
    if (walStream == nullptr) {
        ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("The wal stream of walId:%lu not exist.", walId));
        return DSTORE_FAIL;
    }

    RetStatus ret = walStream->Recovery(pdbId, RedoMode::RECOVERY_REDO, term, tryRecoveryFromDisk);

    return ret;
}

void WalManager::WaitRecoveryFinish(RedoTask *task)
{
    for (uint32 i = 0; i < task->walCount; i++) {
        if (likely(task->walIds[i] != INVALID_WAL_ID)) {
            WalStreamNode *walStreamNode = m_streamManager->GetWalStreamNode(task->walIds[i]);
            if (walStreamNode == nullptr) {
                ErrLog(DSTORE_WARNING, MODULE_WAL,
                       ErrMsg("The wal stream node of walId:%lu not exist.", task->walIds[i]));
                continue;
            }
            WalStream *walStream = walStreamNode->walStream;
            if (walStream == nullptr) {
                m_streamManager->DerefWalStreamNode(walStreamNode);
                ErrLog(DSTORE_WARNING, MODULE_WAL, ErrMsg("The wal stream of walId:%lu not exist.", task->walIds[i]));
                continue;
            }
            walStream->WaitRecoveryFinish();
            m_streamManager->DerefWalStreamNode(walStreamNode);
        }
    }
}

RetStatus WalManager::TakeOverStreams(WalId *walIdArray, const uint32 length)
{
    return m_streamManager->TakeOverStreams(walIdArray, length);
}

RetStatus WalManager::GetPageWalRecordInfoListAndLock(const PageId pageId, const WalRecordLsnInfo pageLsnInfo,
                                                      bool fromDiskWalRecovery, BigArray::Iter *recordIter,
                                                      bool *isRecoveryDone)
{
    WalStream *walStream = m_streamManager->GetWalStream(pageLsnInfo.walId);
    if (isRecoveryDone == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_WAL,
               ErrMsg("isRecoveryDone is nullptr, walId: %lu, buftag (%hhu, %hu, %u).", pageLsnInfo.walId, m_pdbId,
                      pageId.m_fileId, pageId.m_blockId));
        return DSTORE_FAIL;
    }
    if (walStream == nullptr || recordIter == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_WAL,
               ErrMsg("Invalid params, walId: %lu, buftag (%hhu, %hu, %u).", pageLsnInfo.walId, m_pdbId,
                      pageId.m_fileId, pageId.m_blockId));
        *isRecoveryDone = true;
        return DSTORE_SUCC;
    }
    WalRecovery *walRecovery;
    if (fromDiskWalRecovery) {
        if (!walStream->NeedRecoverWithDiskWalRecovery()) {
            *isRecoveryDone = true;
            return DSTORE_SUCC;
        }
        walRecovery = walStream->GetDiskWalRecovery();
    } else {
        walRecovery = walStream->GetWalRecovery();
    }
    walRecovery->WaitDirtyPageSetBuilt();
    *isRecoveryDone = walRecovery->GetWalRecoveryStage() >= WalRecoveryStage::RECOVERY_REDO_DONE;
    if (walRecovery->GetWalRecoveryStage() >= WalRecoveryStage::RECOVERY_GET_DIRTY_PAGE_SET_DONE &&
        walRecovery->GetWalRecoveryStage() < WalRecoveryStage::RECOVERY_REDO_DONE) {
        if (!walRecovery->TryAcquirePageWalRecordsLwlock(LW_SHARED)) {
            *isRecoveryDone = true;
            return DSTORE_SUCC;
        }
        PageWalRecordInfoListEntry *entry = nullptr;
        walRecovery->GetPageWalRecordInfoList(pageId, &entry);
        if (entry == nullptr) {
            walRecovery->ReleasePageWalRecordsLwlock();
            *isRecoveryDone = true;
            return DSTORE_SUCC;
        }
        bool gotFirstRecord = false;
        BigArray::Iter iter(entry->walRecordInfoList, 0);
        for (; !iter.IsEnd(); iter++) {
            WalRecordInfoListNode *listNode = *iter;
            if (!gotFirstRecord) {
                if (listNode->glsn < pageLsnInfo.glsn ||
                    (listNode->glsn == pageLsnInfo.glsn &&
                    listNode->redoContext.recordEndPlsn <= pageLsnInfo.endPlsn)) {
                    continue;
                }
                recordIter->Init(entry->walRecordInfoList, iter.curIndex);
            }
            gotFirstRecord = true;
        }
        if (gotFirstRecord) {
            *isRecoveryDone = false;
        } else {
            walRecovery->ReleasePageWalRecordsLwlock();
            *isRecoveryDone = true;
        }
    }
    return DSTORE_SUCC;
}

void WalManager::ReleasePageWalRecordInfoListLock(WalId walId, bool fromDiskWalRecovery)
{
    WalStream *walStream = m_streamManager->GetWalStream(walId);
    if (STORAGE_VAR_NULL(walStream)) {
        ErrLog(DSTORE_ERROR, MODULE_WAL, ErrMsg("WalStream %lu nox exist", walId));
        return;
    }
    WalRecovery *walRecovery;
    if (fromDiskWalRecovery) {
        walRecovery = walStream->GetDiskWalRecovery();
    } else {
        walRecovery = walStream->GetWalRecovery();
    }
    walStream->GetWalRecovery();
    walRecovery->ReleasePageWalRecordsLwlock();
}

RetStatus WalManager::GetAllTransactionsNeedRollback(
    TransactionNeedRollbackInfo **rollbackArray, uint64 &totalRollbackNum)
{
    return m_redoManager.GetAllTransactionsNeedRollback(rollbackArray, totalRollbackNum);
}
} /* The end of namespace DSTORE */
