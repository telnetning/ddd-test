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
 * dstore_savepoint.cpp
 *
 *
 * IDENTIFICATION
 *        storage/src/transaction/dstore_savepoint.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "transaction/dstore_savepoint.h"
#include "transaction/dstore_transaction_mgr.h"
#include "errorcode/dstore_transaction_error_code.h"
#include "framework/dstore_instance.h"
#include "common/log/dstore_log.h"

namespace DSTORE {

Savepoint::Savepoint(PdbId pdbId)
    : m_name(nullptr),
      m_lastUndoPtr(INVALID_UNDO_RECORD_PTR),
      m_lastLockPos(0),
      m_extra_res(nullptr),
      m_pdbId(pdbId)
{
    DListNodeInit(&m_nodeInList);
}

RetStatus Savepoint::Create(const char *name)
{
    AutoMemCxtSwitch memCxtSwitch(thrd->GetTransactionMemoryContext());

    /* Record the last undoptr of current transaction. */
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION, ErrMsg("Transaction is null when create savepoint."));
        return DSTORE_FAIL;
    }
    m_lastUndoPtr = transaction->GetLastUndoRecord();

    /* Record the lock held by current transaction. */
    m_lastLockPos = transaction->Generate2PLockSubResourceID();

    if (name != nullptr) {
        StringInfoData string;
        if (unlikely(!string.init())) {
            ErrLog(DSTORE_ERROR, MODULE_TRANSACTION, ErrMsg("Malloc savepoint name failed!"));
            return DSTORE_FAIL;
        }
        string.AppendString(name);
        m_name = string.data;
    }

    DListNodeInit(&m_nodeInList);
    return DSTORE_SUCC;
}

RetStatus Savepoint::Rollback()
{
    RetStatus ret = DSTORE_SUCC;
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION, ErrMsg("Transaction is null when rollback to savepoint."));
        return DSTORE_FAIL;
    }
    if (transaction->GetCurrentXid().IsValid()) {
        /* Rollback undo record from last to savepoint. */
        StoragePdb *pdb = g_storageInstance->GetPdb(m_pdbId);
        if (STORAGE_VAR_NULL(pdb)) {
            ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
                   ErrMsg("Pdb is null when rollback to savepoint, pdbId[%u].", m_pdbId));
            return DSTORE_FAIL;
        }
        TransactionMgr *transactionMgr = pdb->GetTransactionMgr();
        if (STORAGE_VAR_NULL(transactionMgr)) {
            ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
                   ErrMsg("TransactionMgr is null when rollback to savepoint, pdbId[%u].", m_pdbId));
            return DSTORE_FAIL;
        }
        ret = transactionMgr->RollbackToUndoptr(transaction->GetCurrentXid(), m_lastUndoPtr);
        StorageAssert(STORAGE_FUNC_SUCC(ret));
        if (STORAGE_FUNC_FAIL(ret)) {
            ErrLog(DSTORE_ERROR, MODULE_TRANSACTION,
                ErrMsg("Savepoint Type: %d, Savepoint Name: %s rollback undo fail.",
                    SavepointList::GetSavepointType(m_name), SavepointList::GetSavepointName(m_name)));
            return ret;
        }
    }

    /* Release locks acquired after savepoint. */
    transaction->Release2PLocksAcquiredAfter(m_lastLockPos);
    return ret;
}

void Savepoint::Release()
{
    DstorePfreeExt(m_name);
}

PdbId Savepoint::GetPdbId()
{
    return m_pdbId;
}

void Savepoint::PushSelfToListHead(dlist_head *list)
{
    DListPushHead(list, &m_nodeInList);
}

bool Savepoint::HasSameName(const char *name) const
{
    if (name != nullptr && m_name != nullptr) {
        return strcmp(name, m_name) == 0;
    }
    /* check for internal savepoint match */
    return ((m_name == nullptr) && (name == nullptr));
}

Savepoint *Savepoint::GetSavepointFromNodeInList(dlist_node *node)
{
    return static_cast<Savepoint *>(dlist_container(Savepoint, m_nodeInList, node));
}

void SavepointList::Init()
{
    m_nestLevel = 0;
    DListInit(&m_savepointList);
}

RetStatus SavepointList::AddSavepoint(const char *name)
{
    Savepoint *savepoint = DstoreNew(thrd->GetTransactionMemoryContext()) Savepoint(m_pdbId);
    if (STORAGE_VAR_NULL(savepoint)) {
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        return DSTORE_FAIL;
    }

    RetStatus ret = savepoint->Create(name);
    if (STORAGE_FUNC_FAIL(ret)) {
        delete savepoint;
        return ret;
    }

    savepoint->PushSelfToListHead(&m_savepointList);
    m_nestLevel++;
    return DSTORE_SUCC;
}

/*
 * Starts from the SavepointList head (the latest savepoint),
 * releases all savepoints until match encountered.
 * userSavepointCounter and exceptionSavepointCounter return
 * how many savepoints were deleted.
 */
RetStatus SavepointList::ReleaseSavepoint(const char *name, int16 *userSavepointCounter,
                                          int16 *exceptionSavepointCounter)
{
    StorageAssert(IsSavepointExist(name));
    dlist_mutable_iter iter;
    dlist_foreach_modify(iter, &m_savepointList) {
        Savepoint *savepoint = Savepoint::GetSavepointFromNodeInList(iter.cur);
        /* exception savepoints have name == nullptr */
        if (userSavepointCounter != nullptr && savepoint->HasSavepointName()) {
            (*userSavepointCounter)++;
        } else if (exceptionSavepointCounter != nullptr) {
            (*exceptionSavepointCounter)++;
        }

        /* released matching savepoint, can exit */
        if (savepoint->HasSameName(name)) {
            DListDelete(iter.cur);
            savepoint->Release();
            delete savepoint;
            m_nestLevel--;
            return DSTORE_SUCC;
        }

        DListDelete(iter.cur);
        savepoint->Release();
        delete savepoint;
        m_nestLevel--;
    }

    /*
     * At this point all the savepoints have been deleted and it is too late.
     * Must check if savepoint exists before
     */
    StorageReleasePanic(true, MODULE_TRANSACTION, ErrMsg("All savepoint deleted by ReleaseSavepoint."));
    return DSTORE_FAIL;
}

RetStatus SavepointList::RollbackToSavepoint(const char *name, int16 *userSavepointCounter,
                                             int16 *exceptionSavepointCounter)
{
    StorageAssert(IsSavepointExist(name));
    /*
     * Starts from the SavepointList head (the latest savepoint),
     * releases all savepoints until match encountered. Only rollbacks the matching savepoint
     */
    dlist_mutable_iter iter;
    dlist_foreach_modify(iter, &m_savepointList) {
        Savepoint *savepoint = Savepoint::GetSavepointFromNodeInList(iter.cur);
        if (savepoint->HasSameName(name)) {
            /* match found, rollback the savepoint */
            return savepoint->Rollback();
        }
        /* match not found, delete savepoint */
        /* exception savepoints have name == nullptr */
        if (userSavepointCounter != nullptr && savepoint->HasSavepointName()) {
            (*userSavepointCounter)++;
        } else if (exceptionSavepointCounter != nullptr) {
            (*exceptionSavepointCounter)++;
        }
        DListDelete(iter.cur);
        savepoint->Release();
        delete savepoint;
        m_nestLevel--;
    }

    /*
     * At this point all the savepoints have been deleted and it is too late.
     * Must check if savepoint exists before
     */
    StorageReleasePanic(true, MODULE_TRANSACTION, ErrMsg("All savepoint deleted by RollbackToSavepoint."));
    return DSTORE_FAIL;
}

RetStatus SavepointList::SaveExtraResPtrToSavepoint(const char *name, void* data)
{
    RetStatus ret = DSTORE_FAIL;
    dlist_mutable_iter iter;
    dlist_foreach_modify(iter, &m_savepointList) {
        Savepoint *savepoint = Savepoint::GetSavepointFromNodeInList(iter.cur);
        if (savepoint->HasSameName(name)) {
            savepoint->SetSavepointExtraResRtr(data);
            return DSTORE_SUCC;
        }
    }
    return ret;
}

void* SavepointList::GetExtraResPtrFromSavepoint(const char *name)
{
    dlist_mutable_iter iter;
    dlist_foreach_modify(iter, &m_savepointList) {
        Savepoint *savepoint = Savepoint::GetSavepointFromNodeInList(iter.cur);
        if (savepoint->HasSameName(name)) {
            return savepoint->GetSavepointExtraResRtr();
        }
    }
    return nullptr;
}

void* SavepointList::GetExtraResPtrFromCurrentSavepoint()
{
    StorageAssert(!IsEmpty());
    return Savepoint::GetSavepointFromNodeInList(DListHeadNode(&m_savepointList))->GetSavepointExtraResRtr();
}

bool SavepointList::IsSavepointExist(const char *name)
{
    dlist_iter iter;
    dlist_foreach(iter, &m_savepointList) {
        Savepoint *savepoint = Savepoint::GetSavepointFromNodeInList(iter.cur);
        if (savepoint->HasSameName(name)) {
            return true;
        }
    }
    return false;
}

bool SavepointList::HasCurrentSavepointName()
{
    StorageAssert(!IsEmpty());
    return Savepoint::GetSavepointFromNodeInList(
        DListHeadNode(&m_savepointList))->HasSavepointName();
}

char *SavepointList::GetCurrentSavepointName()
{
    if (unlikely(IsEmpty())) {
        return nullptr;
    }
    return Savepoint::GetSavepointFromNodeInList(DListHeadNode(&m_savepointList))->GetSavepointName();
}

bool SavepointList::IsEmpty()
{
    if (DListIsEmpty(&m_savepointList)) {
        StorageAssert(m_nestLevel == 0);
        return true;
    }
    StorageAssert(m_nestLevel > 0);
    return false;
}

void SavepointList::DeleteAll()
{
    dlist_mutable_iter iter;
    dlist_foreach_modify(iter, &m_savepointList) {
        Savepoint *savepoint = Savepoint::GetSavepointFromNodeInList(iter.cur);
        DListDelete(iter.cur);
        savepoint->Release();
        delete savepoint;
        m_nestLevel--;
    }
    StorageAssert(m_nestLevel == 0);
}

PdbId SavepointList::GetPdbId()
{
    return m_pdbId;
}

}
