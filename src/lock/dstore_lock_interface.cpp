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

#include "lock/dstore_lock_interface.h"
#include "common/log/dstore_log.h"
#include "framework/dstore_thread.h"
#include "framework/dstore_instance.h"
#include "transaction/dstore_transaction.h"
#include "lock/dstore_table_lock_mgr.h"
#include "errorcode/dstore_transaction_error_code.h"

namespace LockInterface {
using namespace DSTORE;

static inline RetStatus TransactionThrdEnvCheck()
{
    if (unlikely(thrd == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_LOCK, ErrMsg("Use thread context before initialized."));
        storage_set_error(LOCK_ERROR_INVALID_PARAMETER);
        return DSTORE_FAIL;
    }

    if (unlikely(thrd->GetActiveTransaction() == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_LOCK, ErrMsg("Use transaction context before initialized."));
        storage_set_error(LOCK_ERROR_INVALID_PARAMETER);
        return DSTORE_FAIL;
    }

    return DSTORE_SUCC;
}

static inline RetStatus SessionThrdEnvCheck()
{
    if (unlikely(thrd == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_LOCK, ErrMsg("Use thread context before initialized."));
        storage_set_error(LOCK_ERROR_INVALID_PARAMETER);
        return DSTORE_FAIL;
    }

    if (unlikely(thrd->GetSession() == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_LOCK, ErrMsg("Use session context before initialized."));
        storage_set_error(LOCK_ERROR_INVALID_PARAMETER);
        return DSTORE_FAIL;
    }

    return DSTORE_SUCC;
}

/*
 * Function used by both Lock() and LockSession() to acquire lock in table lock manager.
 */
static RetStatus DoLockTable(const LockTag *tag, LockMode mode, bool dontWait, LockAcquireResult *result,
    bool &isAlreadyHeld)
{
    LockErrorInfo info;
    TableLockMgr *tableLockMgr = g_storageInstance->GetTableLockMgr();
    LockAcquireContext lockContext = {.tagCache = LockTagCache(tag), mode, dontWait, &info};
    RetStatus ret = tableLockMgr->LockTable(lockContext, isAlreadyHeld);
    if (STORAGE_FUNC_FAIL(ret)) {
        if ((dontWait == LOCK_DONT_WAIT) && (StorageGetErrorCode() == LOCK_INFO_NOT_AVAIL)) {
            /* We've tried to acquire the lock (dontWait is true) but failed. */
            *result = LockAcquireResult::LOCKACQUIRE_NOT_AVAIL;
        } else {
            *result = LockAcquireResult::LOCKACQUIRE_OTHER_ERROR;
            /* This is unexpected error so dump it into the diag log. */
            ErrLog(DSTORE_ERROR, MODULE_LOCK,
                ErrMsg("Failed to lock %s in mode %s, %s, error %s", tag->ToString().CString(),
                    GetLockModeString(mode), (dontWait ? "no wait" : "wait"), StorageGetMessage()));
        }
    }

    return ret;
}

/*
 * Function to process the result of DoLockTable.
 * Used by both Lock() and LockSession().
 */
static void ProcessDoLockTableResult(bool isAlreadyHeld, LockAcquireResult *result)
{
    if (isAlreadyHeld) {
        /*
         * We need to know if we already held the lock
         * in the exact same mode. If that is the case, we will not receive
         * SharedInvalidation message after locking.
         */
        *result = LockAcquireResult::LOCKACQUIRE_ALREADY_HELD;
    } else {
        *result = LockAcquireResult::LOCKACQUIRE_OK;
    }

#if defined(LOCK_DEBUG) && defined(USE_ASSERT_CHECKING)
    /* ErrLog that we have successfully acquired the lock. */
    ErrLog(DSTORE_DEBUG1, MODULE_LOCK,
        ErrMsg("Locked " "[locktag: field1 = %u, field2 = %u, field3 = %u, lockTagType = %s]"
            " successfully in mode %s",
            tag->field1, tag->field2, tag->field3, tag->GetTypeName(), GetLockModeString(mode)));
#endif
}

static RetStatus Lock(const LockTag *tag, LockMode mode, bool dontWait,
    LockAcquireResult *result, bool needTransaction = true)
{
    RetStatus ret = DSTORE_SUCC;
    StorageStat *stat = g_storageInstance->GetStat();
    stat->ReportLockStat(DSTORE::StmtDetailType::LOCK_START, mode, tag);
    bool isAlreadyHeld = false;
    if (needTransaction && STORAGE_FUNC_FAIL(TransactionThrdEnvCheck())) {
        ErrLog(DSTORE_ERROR, MODULE_LOCK,
            ErrMsg("Failed to acquire lock %s in mode %s because transaction is not initialized.",
                tag->ToString().CString(), GetLockModeString(mode)));
        *result = LockAcquireResult::LOCKACQUIRE_OTHER_ERROR;
        g_storageInstance->GetStat()->ReportLockStat(DSTORE::StmtDetailType::LOCK_END, DSTORE_NO_LOCK);
        return DSTORE_FAIL;
    }

    Transaction *transaction = thrd->GetActiveTransaction();
    if (transaction && transaction->IsAutonomousTransaction() &&
        thrd->NonActiveTransactionHoldConflict2PLock(*tag, mode, TABLE_LOCK_MGR)) {
        if (dontWait) {
            storage_set_error(LOCK_INFO_NOT_AVAIL);
            *result = LockAcquireResult::LOCKACQUIRE_NOT_AVAIL;
        } else {
            storage_set_error(TRANSACTION_INFO_SAME_THREAD_DEADLOCK);
            *result = LockAcquireResult::LOCKACQUIRE_OTHER_ERROR;
            ErrLog(DSTORE_ERROR, MODULE_LOCK,
                ErrMsg("Failed to acquire lock %s in mode %s because autonomous transaction hold conflict lock.",
                    tag->ToString().CString(), GetLockModeString(mode)));
        }
        stat->ReportLockStat(DSTORE::StmtDetailType::LOCK_END, DSTORE_NO_LOCK);
        return DSTORE_FAIL;
    }

    if (transaction && g_storageInstance->GetGuc()->enableLazyLock) {
        ret = transaction->RememberLazy2PLock(*tag, mode, TABLE_LOCK_MGR, isAlreadyHeld);
        if (STORAGE_FUNC_SUCC(ret)) {
            *result = isAlreadyHeld ? LockAcquireResult::LOCKACQUIRE_ALREADY_HELD : LockAcquireResult::LOCKACQUIRE_OK;
            if (transaction->IsAutonomousTransaction() && !isAlreadyHeld &&
                thrd->AllTransactionsHold2PLockMoreThan(*tag, mode, TABLE_LOCK_MGR, 1)) {
                *result = LockAcquireResult::LOCKACQUIRE_ALREADY_HELD;
            }
            stat->ReportLockStat(DSTORE::StmtDetailType::LOCK_END, mode);
            return ret;
        }
    }

    ret = DoLockTable(tag, mode, dontWait, result, isAlreadyHeld);
    if STORAGE_FUNC_FAIL(ret) {
        stat->ReportLockStat(DSTORE::StmtDetailType::LOCK_END, DSTORE_NO_LOCK);
        return ret;
    }

    /*
     * Table lock was acquired successfully, record lock acquisition in
     * the transaction's LockResource list.
     */
    if (transaction) {
        ret = transaction->Remember2PLock(*tag, mode, TABLE_LOCK_MGR);
        if (STORAGE_FUNC_FAIL(ret)) {
            ErrLog(DSTORE_ERROR, MODULE_LOCK,
                ErrMsg("Failed to remember lock %s in mode %s, error %s",
                    tag->ToString().CString(), GetLockModeString(mode), StorageGetMessage()));
            ErrorCode err = StorageGetErrorCode();
            TableLockMgr *tableLockMgr = g_storageInstance->GetTableLockMgr();
            tableLockMgr->Unlock(tag, mode);
            storage_set_error(err);
            *result = LockAcquireResult::LOCKACQUIRE_OTHER_ERROR;
            stat->ReportLockStat(DSTORE::StmtDetailType::LOCK_END, DSTORE_NO_LOCK);
            return ret;
        }
    } else {
#ifndef UT
        PrintBackTrace();
#endif
        ErrLog(DSTORE_LOG, MODULE_LOCK,
            ErrMsg("Failed to remember lock %s in mode %s because transaction runtime is not initialized.",
                tag->ToString().CString(), GetLockModeString(mode)));
    }

    ProcessDoLockTableResult(isAlreadyHeld, result);
    StorageAssert(STORAGE_FUNC_SUCC(ret));
    stat->ReportLockStat(DSTORE::StmtDetailType::LOCK_END, mode);
    return ret;
}

static RetStatus LockSession(const LockTag *tag, LockMode mode, bool dontWait, LockAcquireResult *result)
{
    if (STORAGE_FUNC_FAIL(SessionThrdEnvCheck())) {
        ErrLog(DSTORE_ERROR, MODULE_LOCK,
            ErrMsg("Failed to acquire session lock %s in mode %s because session is not initialized.",
                tag->ToString().CString(), GetLockModeString(mode)));
        *result = LockAcquireResult::LOCKACQUIRE_OTHER_ERROR;
        return DSTORE_FAIL;
    }

    RetStatus ret = DSTORE_SUCC;
    StorageStat *stat = g_storageInstance->GetStat();
    stat->ReportLockStat(DSTORE::StmtDetailType::LOCK_START, mode, tag);
    bool isAlreadyHeld = false;

    ret = DoLockTable(tag, mode, dontWait, result, isAlreadyHeld);
    if STORAGE_FUNC_FAIL(ret) {
        stat->ReportLockStat(DSTORE::StmtDetailType::LOCK_END, DSTORE_NO_LOCK);
        return ret;
    }

    StorageSession *session = thrd->GetSession();
    ret = session->lockRes->RememberLock(*tag, mode, TABLE_LOCK_MGR);
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_LOCK,
            ErrMsg("Failed to remember session lock %s in mode %s, error %s",
                tag->ToString().CString(), GetLockModeString(mode), StorageGetMessage()));
        ErrorCode err = StorageGetErrorCode();
        TableLockMgr *tableLockMgr = g_storageInstance->GetTableLockMgr();
        tableLockMgr->Unlock(tag, mode);
        storage_set_error(err);
        *result = LockAcquireResult::LOCKACQUIRE_OTHER_ERROR;
        stat->ReportLockStat(DSTORE::StmtDetailType::LOCK_END, DSTORE_NO_LOCK);
        return ret;
    }

    ProcessDoLockTableResult(isAlreadyHeld, result);
    StorageAssert(STORAGE_FUNC_SUCC(ret));
    stat->ReportLockStat(DSTORE::StmtDetailType::LOCK_END, mode);
    return ret;
}

static void Unlock(const LockTag *tag, LockMode mode, bool needTransaction = true)
{
    if (needTransaction && STORAGE_FUNC_FAIL(TransactionThrdEnvCheck())) {
        ErrLog(DSTORE_ERROR, MODULE_LOCK,
            ErrMsg("Failed to release lock %s in mode %s because transaction is not initialized.",
                tag->ToString().CString(), GetLockModeString(mode)));
        return;
    }

    Transaction *transaction = thrd->GetActiveTransaction();
    if (transaction && g_storageInstance->GetGuc()->enableLazyLock) {
        RetStatus ret = transaction->ForgetLazy2PLock(*tag, mode, TABLE_LOCK_MGR);
        if (STORAGE_FUNC_SUCC(ret)) {
            g_storageInstance->GetStat()->ReportLockStat(DSTORE::StmtDetailType::LOCK_RELEASE, mode, tag);
            return;
        }
    }

    TableLockMgr *tableLockMgr = g_storageInstance->GetTableLockMgr();
    tableLockMgr->Unlock(tag, mode);

#if defined(LOCK_DEBUG) && defined(USE_ASSERT_CHECKING)
    ErrLog(DSTORE_DEBUG1, MODULE_LOCK,
        ErrMsg("Unlock " "[locktag: field1 = %u, field2 = %u, field3 = %u, lockTagType = %s]" " in mode %s",
            tag->field1, tag->field2, tag->field3, tag->GetTypeName(), GetLockModeString(mode)));
#endif
    g_storageInstance->GetStat()->ReportLockStat(DSTORE::StmtDetailType::LOCK_RELEASE, mode, tag);

    if (transaction) {
        /* Remove lock from the transaction's LockResource list. */
        RetStatus ret = transaction->Forget2PLock(*tag, mode, TABLE_LOCK_MGR);
        if (STORAGE_FUNC_FAIL(ret)) {
            /* Forget2PLock should never fail but we assert in case it ever does. */
            ErrLog(DSTORE_PANIC, MODULE_LOCK,
                   ErrMsg("Unlocked %s successfully but failed to forget in mode %s", tag->ToString().CString(),
                          GetLockModeString(mode)));
        }
    } else {
#ifndef UT
        PrintBackTrace();
#endif
        ErrLog(DSTORE_LOG, MODULE_LOCK,
            ErrMsg("Failed to forget lock %s in mode %s because transaction runtime is not initialized.",
                tag->ToString().CString(), GetLockModeString(mode)));
    }
}

static void UnlockSession(const LockTag *tag, LockMode mode)
{
    if (STORAGE_FUNC_FAIL(SessionThrdEnvCheck())) {
        ErrLog(DSTORE_ERROR, MODULE_LOCK,
            ErrMsg("Failed to release session lock %s in mode %s because session is not initialized.",
                tag->ToString().CString(), GetLockModeString(mode)));
        return;
    }

    StorageSession *session = thrd->GetSession();
    TableLockMgr *tableLockMgr = g_storageInstance->GetTableLockMgr();
    tableLockMgr->Unlock(tag, mode);

#if defined(LOCK_DEBUG) && defined(USE_ASSERT_CHECKING)
    ErrLog(DSTORE_DEBUG1, MODULE_LOCK,
        ErrMsg("Unlock " "[locktag: field1 = %u, field2 = %u, field3 = %u, lockTagType = %s]" " in mode %s",
            tag->field1, tag->field2, tag->field3, tag->GetTypeName(), GetLockModeString(mode)));
#endif
    session->lockRes->ForgetLock(*tag, mode, TABLE_LOCK_MGR);
}

/*
 * Sets locktag as TABLE or PARTITION based on context's locktype.
 * Keep this out of dstore_table_lock_handler.h to avoid adding header
 * dependencies to the SQL engine code.
 */
inline void SetLockTag(TableLockContext *context, LockTag &tag)
{
    /* Session locks are only table locks. */
    StorageAssert(!context->isSessionLock || !context->isPartition);
    if (context->isPartition) {
        tag.SetPartitionLockTag(context->dbId, context->relId, context->partId);
    } else {
        tag.SetTableLockTag(context->dbId, context->relId);
    }
}

static inline RetStatus LockModeParameterCheck(LockMode lockMode)
{
    if (unlikely((lockMode == DSTORE::DSTORE_NO_LOCK) || (lockMode >= DSTORE::DSTORE_LOCK_MODE_MAX))) {
        ErrLog(DSTORE_ERROR, MODULE_LOCK, ErrMsg("Input parameter mode %u is invalid.", lockMode));
        storage_set_error(LOCK_ERROR_INVALID_PARAMETER);
        return DSTORE_FAIL;
    }

    return DSTORE_SUCC;
}

static inline RetStatus TableLockContextCheck(TableLockContext *context)
{
    if (unlikely(context == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_LOCK, ErrMsg("Input parameter context is nullptr."));
        storage_set_error(LOCK_ERROR_INVALID_PARAMETER);
        return DSTORE_FAIL;
    }

    if (STORAGE_FUNC_FAIL(LockModeParameterCheck(context->mode))) {
        return DSTORE_FAIL;
    }

    return DSTORE_SUCC;
}

/*
 * Lock table inside DSTORE TableLockMgr using provided context.
 * If context->dontWait is true, acquire table lock but only if we can get the
 * lock without blocking.
 */
RetStatus LockTable(TableLockContext *context)
{
    if (STORAGE_FUNC_FAIL(TableLockContextCheck(context))) {
        if (context != nullptr) {
            context->result = LockAcquireResult::LOCKACQUIRE_OTHER_ERROR;
        }
        return DSTORE_FAIL;
    }

    LockTag tag;
    SetLockTag(context, tag);

    /* nullptr context is handled by the sanity check above. */
    if (context->isSessionLock) {
        return LockSession(&tag, context->mode, context->dontWait, &context->result);
    }
    return Lock(&tag, context->mode, context->dontWait, &context->result);
}

/*
 * Release the table lock inside DSTORE TableLockMgr.
 */
void UnlockTable(TableLockContext *context)
{
    StorageAssert(context != nullptr);
    if (STORAGE_FUNC_FAIL(TableLockContextCheck(context))) {
        ErrLog(DSTORE_ERROR, MODULE_LOCK, ErrMsg("Input unlocking table parameter is invalid"));
        return;
    }

    LockTag tag;
    SetLockTag(context, tag);

    /* nullptr context is handled by the sanity check above. */
    if (unlikely(context->isSessionLock)) {
        UnlockSession(&tag, context->mode);
    } else {
        Unlock(&tag, context->mode);
    }
}

RetStatus LockObject(ObjectLockContext *context)
{
    LockTag tag;
    tag.SetObjectLockTag(context->dbId, context->classId, context->objectId,
                         context->subObjectId1, context->subObjectId2);
    if (STORAGE_FUNC_FAIL(LockModeParameterCheck(context->mode))) {
        ErrLog(DSTORE_ERROR, MODULE_LOCK,
            ErrMsg("Failed to acquire object lock %s in mode %s.",
                tag.ToString().CString(), GetLockModeString(context->mode)));
        return DSTORE_FAIL;
    }

    if (unlikely(context->isSessionLock)) {
        return LockSession(&tag, context->mode, context->dontWait, &context->result);
    }
    return Lock(&tag, context->mode, context->dontWait, &context->result);
}

void UnlockObject(ObjectLockContext *context)
{
    LockTag tag;
    tag.SetObjectLockTag(context->dbId, context->classId, context->objectId,
                         context->subObjectId1, context->subObjectId2);
    if (STORAGE_FUNC_FAIL(LockModeParameterCheck(context->mode))) {
        ErrLog(DSTORE_ERROR, MODULE_LOCK,
            ErrMsg("Failed to release object lock %s in mode %s.",
                tag.ToString().CString(), GetLockModeString(context->mode)));
        return;
    }

    if (unlikely(context->isSessionLock)) {
        UnlockSession(&tag, context->mode);
    } else {
        Unlock(&tag, context->mode);
    }
}

RetStatus LockPackage(PackageLockContext *context)
{
    LockTag tag;
    tag.SetPackageLockTag(context->dbId, context->pkgId);
    if (STORAGE_FUNC_FAIL(LockModeParameterCheck(context->mode))) {
        ErrLog(DSTORE_ERROR, MODULE_LOCK,
            ErrMsg("Failed to acquire package lock %s in mode %s.",
                tag.ToString().CString(), GetLockModeString(context->mode)));
        return DSTORE_FAIL;
    }

    if (unlikely(context->isSessionLock)) {
        return LockSession(&tag, context->mode, context->dontWait, &context->result);
    }
    return Lock(&tag, context->mode, context->dontWait, &context->result);
}

void UnlockPackage(PackageLockContext *context)
{
    LockTag tag;
    tag.SetPackageLockTag(context->dbId, context->pkgId);
    if (STORAGE_FUNC_FAIL(LockModeParameterCheck(context->mode))) {
        ErrLog(DSTORE_ERROR, MODULE_LOCK,
            ErrMsg("Failed to release package lock %s in mode %s.",
                tag.ToString().CString(), GetLockModeString(context->mode)));
        return;
    }

    if (unlikely(context->isSessionLock)) {
        return UnlockSession(&tag, context->mode);
    }
    return Unlock(&tag, context->mode);
}

bool IsPackageLockedInCurrentTransaction(Oid dbId, Oid pkgId, LockMode mode)
{
    LockTag tag;
    tag.SetPackageLockTag(dbId, pkgId);
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        ErrLog(DSTORE_ERROR, MODULE_LOCK,
            ErrMsg("Transaction is null when check is package locked."));
        return false;
    }

    return transaction->Has2PLock(tag, mode, TABLE_LOCK_MGR);
}

RetStatus LockProcedure(ProcedureLockContext *context)
{
    LockTag tag;
    tag.SetProcedureLockTag(context->dbId, context->procId);
    if (STORAGE_FUNC_FAIL(LockModeParameterCheck(context->mode))) {
        ErrLog(DSTORE_ERROR, MODULE_LOCK,
            ErrMsg("Failed to acquire procedure lock %s in mode %s.",
                tag.ToString().CString(), GetLockModeString(context->mode)));
        return DSTORE_FAIL;
    }

    if (unlikely(context->isSessionLock)) {
        return LockSession(&tag, context->mode, context->dontWait, &context->result);
    }
    return Lock(&tag, context->mode, context->dontWait, &context->result);
}

void UnlockProcedure(ProcedureLockContext *context)
{
    LockTag tag;
    tag.SetProcedureLockTag(context->dbId, context->procId);
    if (STORAGE_FUNC_FAIL(LockModeParameterCheck(context->mode))) {
        ErrLog(DSTORE_ERROR, MODULE_LOCK,
            ErrMsg("Failed to release procedure lock %s in mode %s.",
                tag.ToString().CString(), GetLockModeString(context->mode)));
        return;
    }

    if (unlikely(context->isSessionLock)) {
        return UnlockSession(&tag, context->mode);
    }
    return Unlock(&tag, context->mode);
}

bool IsProcedureLockedInCurrentTransaction(Oid dbId, Oid procId, LockMode mode)
{
    LockTag tag;
    tag.SetProcedureLockTag(dbId, procId);
    Transaction *transaction = thrd->GetActiveTransaction();
    if (STORAGE_VAR_NULL(transaction)) {
        ErrLog(DSTORE_ERROR, MODULE_LOCK,
            ErrMsg("Transaction is null when check is procedure locked."));
        return false;
    }

    return transaction->Has2PLock(tag, mode, TABLE_LOCK_MGR);
}

RetStatus LockPdb(PdbId pdbId, LockMode mode, bool dontWait)
{
    LockTag tag;
    LockAcquireResult result;
    tag.SetObjectLockTag(0, DSTORE::DatabaseRelationId, pdbId, 0, 0);
    if (STORAGE_FUNC_FAIL(LockModeParameterCheck(mode))) {
        ErrLog(DSTORE_ERROR, MODULE_LOCK,
            ErrMsg("Failed to acquire pdb lock %s in mode %s.",
                tag.ToString().CString(), GetLockModeString(mode)));
        return DSTORE_FAIL;
    }

    ErrLog(DSTORE_LOG, MODULE_PDB, ErrMsg("[LockPDB] lock pdb %u", pdbId));
    return Lock(&tag, mode, dontWait, &result, false);
}

RetStatus LockPdbNoWaitWithRetry(DSTORE::PdbId pdbId, DSTORE::LockMode mode, uint8 maxRetryTime)
{
    if (STORAGE_FUNC_FAIL(LockModeParameterCheck(mode))) {
        ErrLog(DSTORE_ERROR, MODULE_LOCK,
            ErrMsg("Failed to lock pdb %u in mode %s.",
                pdbId, GetLockModeString(mode)));
        return DSTORE_FAIL;
    }

    ErrLog(DSTORE_LOG, MODULE_PDB, ErrMsg("[LockPDB] lock pdb %u", pdbId));
    uint8 curRetryTime = 0;
    while (STORAGE_FUNC_FAIL(LockPdb(pdbId, mode, true))) {
        ++curRetryTime;
        GaussUsleep(1000000L);
        if (curRetryTime >= maxRetryTime) {
            ErrLog(DSTORE_ERROR, MODULE_PDB, ErrMsg("[LockPDB] Fail to lockPdb more than %d times", maxRetryTime));
            return DSTORE_FAIL;
        }
    }
    return DSTORE_SUCC;
}

void UnlockPdb(PdbId pdbId, LockMode mode)
{
    LockTag tag;
    tag.SetObjectLockTag(0, DSTORE::DatabaseRelationId, pdbId, 0, 0);
    if (STORAGE_FUNC_FAIL(LockModeParameterCheck(mode))) {
        ErrLog(DSTORE_ERROR, MODULE_LOCK,
            ErrMsg("Failed to release pdb lock %s in mode %s.",
                tag.ToString().CString(), GetLockModeString(mode)));
        return;
    }

    Unlock(&tag, mode, false);
    ErrLog(DSTORE_LOG, MODULE_PDB, ErrMsg("[UnlockPDB] unlock pdb %u", pdbId));
}

RetStatus LockBackupRestore(PdbId pdbId, LockMode mode, bool dontWait)
{
    LockTag tag;
    LockAcquireResult result;
    tag.SetBackupRestoreLockTag(pdbId);
    if (STORAGE_FUNC_FAIL(LockModeParameterCheck(mode))) {
        ErrLog(DSTORE_ERROR, MODULE_LOCK,
            ErrMsg("Failed to acquire backup restore lock %s in mode %s.",
                tag.ToString().CString(), GetLockModeString(mode)));
        return DSTORE_FAIL;
    }
    ErrLog(DSTORE_LOG, MODULE_PDB, ErrMsg("[LockBackupRestore] lock pdb %u", pdbId));
    return Lock(&tag, mode, dontWait, &result);
}

void UnlockBackupRestore(PdbId pdbId, LockMode mode)
{
    LockTag tag;
    tag.SetBackupRestoreLockTag(pdbId);
    if (STORAGE_FUNC_FAIL(LockModeParameterCheck(mode))) {
        ErrLog(DSTORE_ERROR, MODULE_LOCK,
            ErrMsg("Failed to release backup restore lock %s in mode %s.",
                tag.ToString().CString(), GetLockModeString(mode)));
        return;
    }
    Unlock(&tag, mode);
    ErrLog(DSTORE_LOG, MODULE_PDB, ErrMsg("[LockBackupRestore] unlock pdb %u", pdbId));
}

RetStatus LockTablespace(TablespaceLockContext *context)
{
    if (context->mode == DSTORE_NO_LOCK) {
        return DSTORE_SUCC;
    }
 
    LockTag tag;
    tag.SetTablespaceLockTag(context->pdbId, context->tablespaceId);
    if (STORAGE_FUNC_FAIL(LockModeParameterCheck(context->mode))) {
        ErrLog(DSTORE_ERROR, MODULE_LOCK,
            ErrMsg("Failed to acquire tablespace lock %s in mode %s.",
                tag.ToString().CString(), GetLockModeString(context->mode)));
        return DSTORE_FAIL;
    }

    return Lock(&tag, context->mode, context->dontWait, &context->result, false);
}
 
void UnlockTablespace(TablespaceLockContext *context)
{
    if (context->mode == DSTORE_NO_LOCK) {
        return;
    }
 
    LockTag tag;
    tag.SetTablespaceLockTag(context->pdbId, context->tablespaceId);
    if (STORAGE_FUNC_FAIL(LockModeParameterCheck(context->mode))) {
        ErrLog(DSTORE_ERROR, MODULE_LOCK,
            ErrMsg("Failed to release tablespace lock %s in mode %s.",
                tag.ToString().CString(), GetLockModeString(context->mode)));
        return;
    }

    return Unlock(&tag, context->mode, false);
}

RetStatus LockAdvisory(AdvisoryLockContext *context)
{
    LockTag tag;
    tag.SetAdvisoryLockTag(context->dbId, context->key1, context->key2, context->type);
    if (STORAGE_FUNC_FAIL(LockModeParameterCheck(context->mode))) {
        ErrLog(DSTORE_ERROR, MODULE_LOCK,
            ErrMsg("Failed to acquire advisory lock %s in mode %s.",
                tag.ToString().CString(), GetLockModeString(context->mode)));
        return DSTORE_FAIL;
    }

    if (unlikely(context->isSessionLock)) {
        return LockSession(&tag, context->mode, context->dontWait, &context->result);
    }
    return Lock(&tag, context->mode, context->dontWait, &context->result);
}

RetStatus UnlockAdvisory(AdvisoryLockContext *context)
{
    LockTag tag;
    tag.SetAdvisoryLockTag(context->dbId, context->key1, context->key2, context->type);
    if (STORAGE_FUNC_FAIL(LockModeParameterCheck(context->mode))) {
        ErrLog(DSTORE_ERROR, MODULE_LOCK,
            ErrMsg("Failed to release advisory lock %s in mode %s.",
                tag.ToString().CString(), GetLockModeString(context->mode)));
        return DSTORE_FAIL;
    }

    if (unlikely(context->isSessionLock)) {
        if (unlikely((thrd == nullptr) || (thrd->GetSession() == nullptr))) {
            ErrLog(DSTORE_ERROR, MODULE_LOCK,
                ErrMsg("Failed to release advisory lock %s in mode %s because session is not initialized.",
                    tag.ToString().CString(), GetLockModeString(context->mode)));
            storage_set_error(LOCK_ERROR_INVALID_PARAMETER);
            return DSTORE_FAIL;
        }
        if (unlikely(thrd->GetSession()->lockRes->GetLockCnt(tag, context->mode, TABLE_LOCK_MGR) == 0)) {
            ErrLog(DSTORE_ERROR, MODULE_LOCK,
                ErrMsg("Failed to release advisory lock %s in mode %s because we don't hold such lock.",
                    tag.ToString().CString(), GetLockModeString(context->mode)));
            storage_set_error(LOCK_ERROR_ADVISORY_NOT_HELD);
            return DSTORE_FAIL;
        }
        UnlockSession(&tag, context->mode);
    } else {
        if (unlikely((thrd == nullptr) || (thrd->GetActiveTransaction() == nullptr))) {
            ErrLog(DSTORE_ERROR, MODULE_LOCK,
                ErrMsg("Failed to release advisory lock %s in mode %s because transaction is not initialized.",
                    tag.ToString().CString(), GetLockModeString(context->mode)));
            storage_set_error(LOCK_ERROR_INVALID_PARAMETER);
            return DSTORE_FAIL;
        }
        if (thrd->GetActiveTransaction()->GetHold2PLockCnt(tag, context->mode, TABLE_LOCK_MGR) == 0) {
            ErrLog(DSTORE_ERROR, MODULE_LOCK,
                ErrMsg("Failed to release advisory lock %s in mode %s because we don't hold such lock.",
                    tag.ToString().CString(), GetLockModeString(context->mode)));
            storage_set_error(LOCK_ERROR_ADVISORY_NOT_HELD);
            return DSTORE_FAIL;
        }
        Unlock(&tag, context->mode);
    }
    return DSTORE_SUCC;
}

void ReleaseAllAdvisorySessionLock()
{
    StorageSession *session = thrd->GetSession();
    if (STORAGE_VAR_NULL(session)) {
        ErrLog(DSTORE_LOG, MODULE_LOCK,
            ErrMsg("Unable to release all advisory session locks because session is nullptr."));
        return;
    }
    session->lockRes->ReleaseAllLocksByType(LOCKTAG_ADVISORY);
}

void ReleaseAllInternalSessionLock()
{
    StorageSession *session = thrd->GetSession();
    if (STORAGE_VAR_NULL(session)) {
        ErrLog(DSTORE_LOG, MODULE_LOCK,
            ErrMsg("Unable to release all internal session locks because session is nullptr."));
        return;
    }

    /*
     * This is a hack to adapt SQL layer. Package and procedure locks are special now when all locks are released,
     * the upper caller expects that the locks will not be released by any other interface except unlocking.
     */
    LockTagType exceptTypes[] = {LOCKTAG_ADVISORY, LOCKTAG_PACKAGE, LOCKTAG_PROCEDURE};
    session->lockRes->ReleaseAllLocksExceptTypes(exceptTypes, sizeof(exceptTypes) / sizeof(LockTagType));
}

void ForceReleaseAllSessionLocks()
{
    StorageSession *session = thrd->GetSession();
    if (STORAGE_VAR_NULL(session)) {
        ErrLog(DSTORE_LOG, MODULE_LOCK, ErrMsg("Unable to release all session locks because session is nullptr."));
    } else {
        session->lockRes->ReleaseAllLocks();
    }
}

/* If current thread holds a session locks, it should not switch to another session in thread pool. */
bool IsHoldingSessionLock()
{
    StorageSession *session = thrd->GetSession();
    if (STORAGE_VAR_NULL(session)) {
        return false;
    }
    return session->lockRes->HasRememberedLock();
}

RetStatus ForceAcquireLazyLocks()
{
    if (!g_storageInstance->GetGuc()->enableLazyLock) {
        return DSTORE_SUCC;
    }

    if (STORAGE_VAR_NULL(thrd)) {
        return DSTORE_SUCC;
    }

    RetStatus ret = DSTORE_FAIL;
    do {
        ret = thrd->ActuallyAcquireLazyLocksOnCurrentThread();
    } while (STORAGE_FUNC_FAIL(ret) && (StorageGetErrorCode() == LOCK_INFO_NOT_AVAIL));

    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_LOCK,
            ErrMsg("Force acquire lazy locks on current thread failed, error code=0x%llx, error message: %s.",
            static_cast<unsigned long long>(StorageGetErrorCode()), StorageGetMessage()));
    }
    return ret;
}

GetThreadSqlStatement g_getThreadSqlStatement = nullptr;

void RegisterGetThreadSqlStatementCallback(GetThreadSqlStatement callback)
{
    g_getThreadSqlStatement = callback;
}

char *GetThreadSqlStatementCallback(pthread_t tid, DSTORE::AllocMemFunc allocFunc)
{
    if (g_getThreadSqlStatement != nullptr) {
        return g_getThreadSqlStatement(tid, allocFunc);
    }
    return nullptr;
}

bool IsTableLockedInCurrentTransaction(TableLockContext *context)
{
    if (STORAGE_FUNC_FAIL(TransactionThrdEnvCheck())) {
        return false;
    }
    LockTag tag;
    SetLockTag(context, tag);
    Transaction *transaction = thrd->GetActiveTransaction();
    return transaction->Has2PLock(tag, context->mode, TABLE_LOCK_MGR);
}

bool HasAnyLockInModeInCurrentTransaction(LockMode mode)
{
    if (STORAGE_FUNC_FAIL(TransactionThrdEnvCheck())) {
        return false;
    }
    Transaction *transaction = thrd->GetActiveTransaction();
    return transaction->HasAny2PLockInMode(mode);
}

}
