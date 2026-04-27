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

#ifndef DSTORE_XACT_LOCK_MGR_H
#define DSTORE_XACT_LOCK_MGR_H

#include "lock/dstore_lock_mgr.h"
#include "dstore_lock_thrd_local.h"
#include "transaction/dstore_transaction_types.h"

namespace DSTORE {

/*
 * We hold a DSTORE_ROW_EXCLUSIVE_LOCK for waiting on an
 * exclusive transaction lock. We use this level of
 * lock because it's the most restrictive shared lock
 * that conflicts with any types of exclusive lock.
 */
#define LOCK_XACT_SHARED_WAIT_LOCK DSTORE_ROW_EXCLUSIVE_LOCK

/*
 * Important: Lock() and Unlock() do not include mode since an external caller
 * of XactLockManager's Lock() and Unlock() function only needs to call them
 * in exclusive mode. For Shared Lock, Wait() function should be used. The Wait()
 * function will call LockManager's Lock() function, which takes in mode. The added
 * mode restriction in this class's Lock() function protects the caller from making
 * mistakes and asking for a shared lock on a transaction directly without going through
 * Wait().
 */
class XactLockMgr : public LockMgr {
public:
    XactLockMgr() = default;
    ~XactLockMgr() override = default;
    DISALLOW_COPY_AND_MOVE(XactLockMgr);

    using LockMgr::Lock;
    virtual RetStatus Lock(PdbId pdbId, Xid xid);
    using LockMgr::Unlock;
    virtual void Unlock(PdbId pdbId, Xid xid);
    virtual RetStatus Wait(PdbId pdbId, Xid xid);
    virtual RetStatus WaitForAnyTransactionEnd(PdbId *pdbIds, const Xid *xids, uint32 arrayLen);
    /*
     * Transfers the xact lock owner to the current thread.
     */
    RetStatus TransferXactLockHolder(PdbId pdbId, Xid xid);

    virtual RetStatus DumpTrxLockInfo(PdbId pdbId, Xid xid, StringInfo str);

protected:
    LockMgrType GetType() const override
    {
        return XACT_LOCK_MGR;
    }

    void LogStartLockAcquires(LockTag *tags, uint32 modeCount) const;
    void LogEndLockAcquires(LockTag *tags, uint32 modeCount, RetStatus ret) const;

private:
    void UnlockAll(const LockTagCache *tagCaches, uint32 dequeueLen) const;
    RetStatus WaitForAnyTransactionEndForMultiWaitLock(const LockTagCache *tagCaches, uint32 arrayLen,
        LockRequest *request);
};
} /* namespace DSTORE */

#endif
