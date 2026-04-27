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
#ifndef UT_FAST_PATH_H
#define UT_FAST_PATH_H

#include "lock/dstore_table_lock_mgr.h"

using namespace DSTORE;

/*
 * This class is defined to test "Fast Path" separately from TableLockMgr.
 */
class TableLockMgrForFastPathTest : public TableLockMgr {
public:
    TableLockMgrForFastPathTest() : m_counter(0), TableLockMgr() {}
    virtual ~TableLockMgrForFastPathTest() {}

    RetStatus TryAcquireWeakLockByFastPath(const LockTagCache &tag, LockMode mode)
    {
        bool isAlreadyHeld = false;
        ThreadLocalLock *localLock = thrd->GetLockCtx()->GetLocalLock();
        RetStatus ret = localLock->TryGrantByFastPath(tag, mode, GetType(), isAlreadyHeld);
        if (STORAGE_FUNC_FAIL(ret)) {
            /* We don't want to test with main lock table, so just return here. */
            localLock->RecordLockResult(tag, mode, ret);
        }
        return ret;
    }

    RetStatus TryReleaseWeakLockByFastPath(const LockTagCache &tag, LockMode mode)
    {
        ThreadLocalLock *localLock = thrd->GetLockCtx()->GetLocalLock();
        RetStatus ret = localLock->TryReleaseByFastPath(tag, mode);
        if (STORAGE_FUNC_FAIL(ret)) {
            /* We don't want to test with main lock table, so just return here. */
            localLock->RemoveLockRecord(tag, mode);
        }
        return ret;
    }

    RetStatus TryMarkStrongLockByFastPath(const LockTagCache &tag)
    {
        return TableLockMgr::TryMarkStrongLockByFastPath(tag);
    }

    void UnmarkStrongLockByFastPath(uint32 tagHashCode)
    {
        ThreadLocalLock::UnmarkStrongLockInFastPath(tagHashCode);
    }

    uint32 GetTransferCount()
    {
        return m_counter;
    }

    void ClearTransferCount()
    {
        m_counter = 0;
    }

protected:
    RetStatus TransferOneWeakLock(const LockTag *tag, LockMode mode, ThreadId id, uint32 threadCoreIdx) override
    {
        m_counter++;
        return DSTORE_SUCC;
    }

private:
    uint32 m_counter;
};

#endif