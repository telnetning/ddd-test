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

#include "diagnose/dstore_lock_mgr_diagnose.h"
#include "lock/dstore_lock_thrd_local.h"
#include "framework/dstore_instance.h"
#include "framework/dstore_thread.h"
#include "lock/dstore_table_lock_mgr.h"
#include "common/dstore_common_utils.h"
#include "lock/dstore_xact_lock_mgr.h"
#include "lock/dstore_lock_mgr.h"

using namespace DSTORE;
static_assert(LOCKTAG_MAX_NUM == LOCKTAG_DIAGNOSE_MAX, "LOCKATG_MAX_NUM must be equal to LOCKTAG_DIAGNOSE_MAX");
inline void SetLockField(LockStatus *singleLockStatus, LockTag lockTag)
{
    singleLockStatus->field1 = lockTag.field1;
    singleLockStatus->field2 = lockTag.field2;
    singleLockStatus->field3 = lockTag.field3;
    singleLockStatus->field4 = lockTag.field4;
    singleLockStatus->field5 = lockTag.field5;
}

RetStatus FormLockStatus(LockStatus* occupyLockStatus, StringInfoData* string, LockTag lockTag,
                    LockStatusContext LockStatusContext)
{
    string->reset();
    occupyLockStatus->tid = LockStatusContext.tid;
    occupyLockStatus->lockTagType = lockTag.lockTagType;
    SetLockField(occupyLockStatus, lockTag);
    occupyLockStatus->lockMode = LockStatusContext.lockMode;
    occupyLockStatus->isWaiting = LockStatusContext.isWaiting;
    occupyLockStatus->grantedCnt = LockStatusContext.granted;
    RetStatus ret = lockTag.DescribeLockTag(string);
    if (STORAGE_FUNC_FAIL(ret)) {
        storage_set_error(LOCK_ERROR_OUT_OF_MEMORY);
        return DSTORE_FAIL;
    }
    errno_t rc = strcpy_s(occupyLockStatus->lockTagDescription, LOCKDESCLEN, string->data);
    storage_securec_check(rc, "\0", "\0");
    return DSTORE_SUCC;
}

LockStatus** AllocOrRepallocLockStatus(LockStatus** lockStatus, int lockNum, int addlockNum)
{
    StorageReleasePanic(addlockNum == 0, MODULE_LOCK, ErrMsg("add lock number doesn't match"));
    LockStatus **newLockStatus = (LockStatus **)DstorePalloc(sizeof(LockStatus *) * (lockNum + addlockNum));
    if (STORAGE_VAR_NULL(newLockStatus)) {
        LockMgrDiagnose::FreeLockStatusArr(lockStatus, lockNum);
        ErrLog(DSTORE_WARNING,
            MODULE_LOCK, ErrMsg("Palloc memory failure when get hold lock status."));
        return nullptr;
    }

    if (lockStatus != nullptr) {
        StorageReleasePanic(lockNum == 0, MODULE_LOCK, ErrMsg("lock number doesn't match"));
        errno_t rc = memcpy_s(newLockStatus, sizeof(LockStatus *) * lockNum,
            lockStatus, sizeof(LockStatus *) * lockNum);
        storage_securec_check(rc, "\0", "\0");
        DstorePfreeExt(lockStatus);
    }

    return newLockStatus;
}

RetStatus HoldGetLockStatus(const ThreadLocalLock::LocalLockEntry *entry, LockStatus **&lockStatus, int& lockNum,
    ThreadCore *core, StringInfoData *string)
{
    int ret = DSTORE_SUCC;
    int grantcnt = 0;
    for (int i = 0; i < static_cast<uint8>(DSTORE_LOCK_MODE_MAX); i++) {
        if (entry->granted[i] == 0) {
            continue;
        }
        grantcnt++;
    }
    if (grantcnt == 0) {
        return DSTORE_SUCC;
    }

    LockStatus **newLockStatus = AllocOrRepallocLockStatus(lockStatus, lockNum, grantcnt);
    if (newLockStatus == nullptr) {
        DstorePfreeExt(string->data);
        lockStatus = nullptr;
        lockNum = 0;
        return DSTORE_FAIL;
    }
    lockStatus = newLockStatus;
    for (int i = 0; i < static_cast<uint8>(DSTORE_LOCK_MODE_MAX); i++) {
        if (entry->granted[i] == 0) {
            continue;
        }
        LockStatus *holdLockStatus = (LockStatus*)DstorePalloc(sizeof(LockStatus));
        if (holdLockStatus == nullptr) {
            LockMgrDiagnose::FreeLockStatusArr(lockStatus, lockNum);
            DstorePfreeExt(string->data);
            lockStatus = nullptr;
            lockNum = 0;
            ErrLog(DSTORE_WARNING,
                MODULE_LOCK, ErrMsg("Palloc memory failure when get hold lock status."));
            return DSTORE_FAIL;
        }
        LockStatusContext holdLockContext = {core->pid, static_cast<LockMode>(i), entry->granted[i], false};
        ret = FormLockStatus(holdLockStatus, string, entry->tag, holdLockContext);
        if (STORAGE_FUNC_FAIL(ret)) {
            DstorePfreeExt(string->data);
            DstorePfreeExt(holdLockStatus);
            LockMgrDiagnose::FreeLockStatusArr(lockStatus, lockNum);
            lockStatus = nullptr;
            lockNum = 0;
            ErrLog(DSTORE_WARNING,
                MODULE_LOCK, ErrMsg("Out of memory when get hold lock status."));
            return DSTORE_FAIL;
        }
        lockStatus[lockNum] = holdLockStatus;
        lockNum++;
    }
    return DSTORE_SUCC;
}

RetStatus MultiWaitGetLockStatus(LockStatus **&lockStatus, int& lockNum,
    ThreadLocalLock *localLock, ThreadCore *core, StringInfoData *string)
{
    int ret = DSTORE_SUCC;
    LockTag waitTags[ThreadLocalLock::m_waitLockMaxCount];
    LockMode waitMode;
    uint32 waitLocksLen;
    bool isWaiting = localLock->GetWaitingLocks(waitTags, &waitMode,
        ThreadLocalLock::m_waitLockMaxCount, &waitLocksLen);
    /**
     * There is a concurrecy between we think it's waiting for multiple locks,
     * but when we get waiting locks the thread is no longer waiting.
     */
    if (waitLocksLen > 0) {
        LockStatus **newLockStatus = AllocOrRepallocLockStatus(lockStatus, lockNum, waitLocksLen);
        if (newLockStatus == nullptr) {
            DstorePfreeExt(string->data);
            lockStatus = nullptr;
            return DSTORE_FAIL;
        }
        lockStatus = newLockStatus;
    }
    for (uint32 i = 0; i < waitLocksLen; i++) {
        LockStatus *multiLockStatus = (LockStatus*)DstorePalloc(sizeof(LockStatus));
        if (multiLockStatus == nullptr) {
            LockMgrDiagnose::FreeLockStatusArr(lockStatus, lockNum);
            DstorePfreeExt(string->data);
            lockStatus = nullptr;
            lockNum = 0;
            ErrLog(DSTORE_WARNING,
                MODULE_LOCK, ErrMsg("Palloc memory failure when get multi-lock status."));
            return DSTORE_FAIL;
        }
        LockStatusContext multiLockContext = {core->pid, waitMode, 0, true};
        ret = FormLockStatus(multiLockStatus, string, waitTags[i], multiLockContext);
        if (STORAGE_FUNC_FAIL(ret)) {
            DstorePfreeExt(string->data);
            DstorePfreeExt(multiLockStatus);
            LockMgrDiagnose::FreeLockStatusArr(lockStatus, lockNum);
            lockStatus = nullptr;
            lockNum = 0;
            ErrLog(DSTORE_WARNING,
                MODULE_LOCK, ErrMsg("Out of memory when get multi-lock status."));
            return DSTORE_FAIL;
        }
        lockStatus[lockNum] = multiLockStatus;
        lockNum++;
    }
    UNUSED_VARIABLE(isWaiting);
    return DSTORE_SUCC;
}

LockStatus** LockMgrDiagnose::GetLocksByThread(int& lockNum, ThreadId pid)
{
    LockStatus** lockStatus = nullptr;
    AutoMemCxtSwitch autoSwitch{thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LOCK)};
    ThreadCoreMgr::ThreadIterator iter(g_storageInstance->GetThreadCoreMgr());
    ThreadCore *core = nullptr;
    lockNum = 0;
    StringInfoData string;
    if (unlikely(!string.init())) {
        lockNum = 0;
        return nullptr;
    }

    while ((core = iter.GetNextThreadCore()) != nullptr) {
        if (pid != 0 && core->pid != pid) {
            continue;
        }
        LockThreadContext *ctx = core->regularLockCtx;
        StorageAssert(ctx != nullptr);
        ThreadLocalLock *localLock = ctx->GetLocalLock();
        LockTag waitTag;
        LockMode waitMode;
        bool isWaiting = localLock->IsWaitingLock(&waitTag, &waitMode);
        if (isWaiting) {
            LockStatus *singleLockStatus = (LockStatus*)DstorePalloc(sizeof(LockStatus));
            if (singleLockStatus == nullptr) {
                FreeLockStatusArr(lockStatus, lockNum);
                DstorePfreeExt(string.data);
                lockNum = 0;
                ErrLog(DSTORE_WARNING,
                    MODULE_LOCK, ErrMsg("Palloc memory failure when get single lock status."));
                return nullptr;
            }
            lockStatus = AllocOrRepallocLockStatus(lockStatus, lockNum, 1);
            if (lockStatus == nullptr) {
                DstorePfreeExt(string.data);
                lockNum = 0;
                return nullptr;
            }
            LockStatusContext singleLockContext = {core->pid, waitMode, 0, true};
            RetStatus ret = FormLockStatus(singleLockStatus, &string, waitTag, singleLockContext);
            if (STORAGE_FUNC_FAIL(ret)) {
                DstorePfreeExt(string.data);
                DstorePfreeExt(singleLockStatus);
                FreeLockStatusArr(lockStatus, lockNum);
                lockNum = 0;
                ErrLog(DSTORE_WARNING,
                    MODULE_LOCK, ErrMsg("Out of memory when get single lock status."));
                return nullptr;
            }
            lockStatus[lockNum] = singleLockStatus;
            lockNum++;
        } else if (localLock->IsWaitingForMultipleLocks()) {
            RetStatus ret = MultiWaitGetLockStatus(lockStatus, lockNum, localLock, core, &string);
            if (STORAGE_FUNC_FAIL(ret)) {
                lockNum = 0;
                return nullptr;
            }
        }
        const ThreadLocalLock::LocalLockEntry *entry;
        ThreadLocalLock::HoldLockIterator holdLockIter(localLock);
        while ((entry = holdLockIter.GetNextLock()) != nullptr) {
            if (entry->grantedTotal == 0) {
                continue;
            }
            RetStatus ret = HoldGetLockStatus(entry, lockStatus, lockNum, core, &string);
            if (STORAGE_FUNC_FAIL(ret)) {
                lockNum = 0;
                return nullptr;
            }
        }
    }
    if (lockNum == 0) {
        StorageReleasePanic(lockStatus != nullptr, MODULE_LOCK, ErrMsg("lock number doesn't match"));
    }
    DstorePfreeExt(string.data);
    return lockStatus;
}

void LockMgrDiagnose::FreeLockStatusArr(LockStatus **lockStatus, int lockNum)
{
    if (lockStatus == nullptr) {
        return;
    }
    if (*lockStatus != nullptr) {
        for (int i = 0; i < lockNum; i++) {
            DstorePfreeExt(lockStatus[i]);
            lockStatus[i] = nullptr;
        }
    }
    DstorePfreeExt(lockStatus);
}

char *LockMgrDiagnose::GetTableLockStatistics(void)
{
    AutoMemCxtSwitch autoSwitch{thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LOCK)};
    StringInfoData string;
    if (unlikely(!string.init())) {
        return nullptr;
    }

    /* Get Table Lock's weak lock and strong lock statistics. */
    if (unlikely(static_cast<uint64>(g_traceSwitch) & TABLELOCK_STATS_TRACE_SWITCH)) {
        TableLockMgr* tableLockMgr = dynamic_cast<TableLockMgr*>(g_storageInstance->GetTableLockMgr());
        TableLockMgr::TableLockStats* tableLockStats = &(tableLockMgr->m_tableLockStats);
        if (STORAGE_FUNC_FAIL(string.append("Table Lock Statistics collected since %s:\n",
                      static_cast<char *>(tableLockStats->resetTimeStamp)))) {
            DstorePfreeExt(string.data);
            ErrLog(DSTORE_ERROR, MODULE_LOCK,
                   ErrMsg("Append resetTimeStamp failed when GetTableLockStatistics."));
            return nullptr;
        }
        if (STORAGE_FUNC_FAIL(string.append("   Number of weak locks transferred: %lu\n",
                      static_cast<uint64>(tableLockStats->numWeakLockTransfers)))) {
            DstorePfreeExt(string.data);
            ErrLog(DSTORE_ERROR, MODULE_LOCK,
                   ErrMsg("Append numWeakLockTransfers failed when GetTableLockStatistics."));
            return nullptr;
        }
        if (STORAGE_FUNC_FAIL(string.append("   Number of weak lock fastpath successes: %lu\n",
                      static_cast<uint64>(tableLockStats->numFastPathSuccesses)))) {
            DstorePfreeExt(string.data);
            ErrLog(DSTORE_ERROR, MODULE_LOCK,
                   ErrMsg("Append numFastPathSuccesses failed when GetTableLockStatistics."));
            return nullptr;
        }
        if (STORAGE_FUNC_FAIL(string.append("   Number of strong locks acquired: %lu\n",
                      static_cast<uint64>(tableLockStats->numStrongLocksAcquired)))) {
            DstorePfreeExt(string.data);
            ErrLog(DSTORE_ERROR, MODULE_LOCK,
                   ErrMsg("Append numStrongLocksAcquired failed when GetTableLockStatistics."));
            return nullptr;
        }
    }
    return string.data;
}

char *LockMgrDiagnose::ResetTableLockStatistics(void)
{
    AutoMemCxtSwitch autoSwitch{thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LOCK)};
    StringInfoData string;
    if (unlikely(!string.init())) {
        return nullptr;
    }

    TableLockMgr* tableLockMgr = dynamic_cast<TableLockMgr*>(g_storageInstance->GetTableLockMgr());
    TableLockMgr::TableLockStats* tableLockStats = &(tableLockMgr->m_tableLockStats);
    tableLockStats->Reset();
    if (STORAGE_FUNC_FAIL(string.append("Reset table lock mgr statistics at %s\n", tableLockStats->resetTimeStamp))) {
        DstorePfreeExt(string.data);
        ErrLog(DSTORE_ERROR, MODULE_LOCK, ErrMsg("Append resetTimeStamp failed when ResetTableLockStatistics."));
        return nullptr;
    }

    return string.data;
}

char *LockMgrDiagnose::GetLockByLockTag(const LockTagContext &context)
{
    AutoMemCxtSwitch autoSwitch{thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LOCK)};
    StringInfoData string;
    RetStatus ret = DSTORE_SUCC;
    if (unlikely(!string.init())) {
        storage_set_error(LOCK_ERROR_OUT_OF_MEMORY);
        return nullptr;
    }

    if (context.lockTagType >= static_cast<uint32>(LOCKTAG_MAX_NUM)) {
        ret = string.append("Lock tag type invalid.\n");
        if (STORAGE_FUNC_FAIL(ret)) {
            DstorePfreeExt(string.data);
            return nullptr;
        }
        return string.data;
    }

    LockTag tag;
    tag.SetLockTagType(static_cast<LockTagType>(context.lockTagType));
    tag.FillInData(context.field1, context.field2, context.field3, context.field4, context.field5);

    LockMgr *lockMgr = g_storageInstance->GetLockMgr();
    ret = lockMgr->DumpByLockTag(&tag, &string);
    if (STORAGE_FUNC_FAIL(ret)) {
        DstorePfreeExt(string.data);
        return nullptr;
    }
    return string.data;
}

char *LockMgrDiagnose::GetTableLockByLockTag(const LockTagContext &context)
{
    AutoMemCxtSwitch autoSwitch{thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LOCK)};
    StringInfoData string;
    RetStatus ret = DSTORE_SUCC;
    if (unlikely(!string.init())) {
        storage_set_error(LOCK_ERROR_OUT_OF_MEMORY);
        return nullptr;
    }

    if (context.lockTagType >= static_cast<uint32>(LOCKTAG_MAX_NUM)) {
        ret = string.append("Table lock tag type invalid.\n");
        if (STORAGE_FUNC_FAIL(ret)) {
            DstorePfreeExt(string.data);
            return nullptr;
        }
        return string.data;
    }

    LockTag tag;
    tag.SetLockTagType(static_cast<LockTagType>(context.lockTagType));
    tag.FillInData(context.field1, context.field2, context.field3, context.field4, context.field5);

    TableLockMgr *tableLockMgr = g_storageInstance->GetTableLockMgr();
    ret = tableLockMgr->DumpByLockTag(&tag, &string);
    if (STORAGE_FUNC_FAIL(ret)) {
        DstorePfreeExt(string.data);
        return nullptr;
    }
    return string.data;
}

char *LockMgrDiagnose::GetTrxInfoFromLockMgr(PdbId pdbId, uint64_t xid)
{
    RetStatus ret = DSTORE_SUCC;
    AutoMemCxtSwitch autoSwitch{thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LOCK)};
    StringInfoData string;
    if (unlikely(!string.init())) {
        storage_set_error(LOCK_ERROR_OUT_OF_MEMORY);
        return nullptr;
    }

    XactLockMgr *xactLockMgr = g_storageInstance->GetXactLockMgr();
    ret = xactLockMgr->DumpTrxLockInfo(pdbId, static_cast<Xid>(xid), &string);
    if (STORAGE_FUNC_FAIL(ret)) {
        return nullptr;
    }

    return string.data;
}

char *LockMgrDiagnose::GetLockMgrStatus(void)
{
    AutoMemCxtSwitch autoSwitch{thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LOCK)};
    StringInfoData string;
    RetStatus ret = DSTORE_SUCC;
    if (unlikely(!string.init())) {
        return nullptr;
    }

    ret = string.append("Lock Manager status:\n");
    if (STORAGE_FUNC_FAIL(ret)) {
        DstorePfreeExt(string.data);
        return nullptr;
    }
    LockMgr *lockMgr = g_storageInstance->GetLockMgr();
    ret = lockMgr->DescribeStatus(&string);
    if (STORAGE_FUNC_FAIL(ret)) {
        DstorePfreeExt(string.data);
        return nullptr;
    }

    ret = string.append("\nTable Lock Manager status:\n");
    if (STORAGE_FUNC_FAIL(ret)) {
        DstorePfreeExt(string.data);
        return nullptr;
    }
    TableLockMgr *tableLockMgr = g_storageInstance->GetTableLockMgr();
    ret = tableLockMgr->DescribeStatus(&string);
    if (STORAGE_FUNC_FAIL(ret)) {
        DstorePfreeExt(string.data);
        return nullptr;
    }

    ret = string.append("\nTransaction Lock Manager status:\n");
    if (STORAGE_FUNC_FAIL(ret)) {
        DstorePfreeExt(string.data);
        return nullptr;
    }
    XactLockMgr *xactLockMgr = g_storageInstance->GetXactLockMgr();
    ret = xactLockMgr->DescribeStatus(&string);
    if (STORAGE_FUNC_FAIL(ret)) {
        DstorePfreeExt(string.data);
        return nullptr;
    }

    return string.data;
}

char *LockMgrDiagnose::DecodeLockTag(const LockTagContext &context)
{
    AutoMemCxtSwitch autoSwitch{thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LOCK)};
    StringInfoData string;
    if (unlikely(!string.init())) {
        return nullptr;
    }

    RetStatus ret = DSTORE_SUCC;
    if (context.lockTagType >= static_cast<uint32>(LOCKTAG_MAX_NUM)) {
        ret = string.append("Lock tag type invalid.\n");
        if (STORAGE_FUNC_FAIL(ret)) {
            storage_set_error(LOCK_ERROR_OUT_OF_MEMORY);
            DstorePfreeExt(string.data);
            return nullptr;
        }
    }

    LockTag tag;
    tag.SetLockTagType(static_cast<LockTagType>(context.lockTagType));
    tag.FillInData(context.field1, context.field2, context.field3, context.field4, context.field5);
    ret = tag.DescribeLockTag(&string);
    if (STORAGE_FUNC_FAIL(ret)) {
        storage_set_error(LOCK_ERROR_OUT_OF_MEMORY);
        DstorePfreeExt(string.data);
        return nullptr;
    }
    return string.data;
}

const char *LockMgrDiagnose::GetLockTagName(uint16_t lock_tag)
{
    LockTag t;
    t.SetLockTagType(static_cast<LockTagType>(lock_tag));
    return t.GetTypeName();
}