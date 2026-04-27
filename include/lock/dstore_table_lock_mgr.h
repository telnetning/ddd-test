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

#ifndef DSTORE_TABLE_LOCK_MGR_H
#define DSTORE_TABLE_LOCK_MGR_H

#include <atomic>
#include "lock/dstore_s_lock.h"
#include "lock/dstore_lock_mgr.h"
#include "lock/dstore_lock_thrd_local.h"

namespace DSTORE {

class TableLockMgr : public LockMgr {
public:
    TableLockMgr() = default;
    ~TableLockMgr() override = default;
    DISALLOW_COPY_AND_MOVE(TableLockMgr);

    /**
    * Initialize and destroy TableLockMgr instance.
    *
    * @param[in] hashTableSize: estimated lock table size.
    * @param[in] partitionNum: partition number of lock table.
    */
    RetStatus Initialize(uint32 hashTableSize, uint32 partitionNum) override;
    void Destroy() override;

    /**
    * Rewrite virtual interfaces so that TableLockMgr::Lock could benefit from "fast path".
    * Fast Path accelerates weak locks but slows down strong locks.
    *
    * @param[in] tag: unique identifier for the lockable object.
    * @param[in] mode: lock mode to acquire.
    * @param[in] dontWait: if true, don't wait to acquire lock.
    * @param[out] info: return detailed error information, could be NULL if not needed.
    */
    RetStatus Lock(const LockTag *tag, LockMode mode, bool dontWait, LockErrorInfo *info) override;

    /**
    * Acquires a table lock.
    * Adds extended support for isAlreadyHeld functionality to detect whether table lock is held.
    *
    * @param[in/out] lockContext: detailed param and error information to acquire table lock, same as Lock().
    * @param[out] isAlreadyHeld: whether the lock is already held by current thread.
    */
    virtual RetStatus LockTable(LockAcquireContext lockContext, bool &isAlreadyHeld);

    /**
    * Release a lock acquired from Lock() or LockTable().
    *
    * @param[in] tag: unique identifier for the lockable object.
    * @param[in] mode: lock mode to release.
    */
    void Unlock(const LockTag *tag, LockMode mode) override;

    /**
    * Release locks acquired from Lock() or LockTable() in batch.
    *
    * @param[in] tag: unique identifier for the lockable object.
    * @param[in] mode: lock mode to release.
    * @param[in] unlockTimes: how many times to release.
    */
    virtual void BatchUnlock(const LockTag *tag, LockMode mode, uint32 unlockTimes);

    /**
    * Dump lock fastpath and queueing information by lock tag.
    *
    * @param[in] tag: unique identifier for the lockable object.
    * @param[out] str: diagnose message.
    */
    RetStatus DumpByLockTag(const LockTag *tag, StringInfo str) override;

    /**
    * Dump lock fastpath and lock table status.
    *
    * @param[out] str: diagnose message.
    */
    RetStatus DescribeStatus(StringInfo str) override;

    /*
     * Table lock counters to keep track weak lock transfers, weak lock
     * fast path failures/successes and strong lock failure/successs,
     * and dump all statistics in dstore_table_lock_statistics().
     */
    struct TableLockStats {
        void Initialize();
        /*
         * Function resets table lock statistics on weak lock and strong lock.
         */
        void Reset();
        void SetTimeStamp();
        LWLock mutex;
        std::atomic<uint64> numWeakLockTransfers;
        std::atomic<uint64> numFastPathSuccesses;
        std::atomic<uint64> numStrongLocksFailures;
        std::atomic<uint64> numStrongLocksAcquired;
        static const int timeStampMaxSize = 100;
        char resetTimeStamp[timeStampMaxSize] = {0};
    };

    friend class LockMgrDiagnose;

#ifndef UT
protected:
#endif
    /**
     * Functions for operating fast path.
     */
    RetStatus TryMarkStrongLockByFastPath(const LockTagCache &tag);
    virtual RetStatus TransferOneWeakLock(const LockTag *tag, LockMode mode, ThreadId id, uint32 threadCoreIdx);

    /**
     * To indicate lock manager type in local lock entry.
     */
    LockMgrType GetType() const override;

    /**
     * Table lock statistics.
     */
    TableLockStats m_tableLockStats;

private:
    /**
     * Functions for transfer weak locks in fast path.
     */
    RetStatus TransferWeakLocks(const LockTagCache &tag);
    RetStatus TransferAllModesForWeakLock(const ThreadCore *threadCore, const LockTag *tag,
        ThreadLocalLock::LocalLockEntry *lockEntry);
    
    /**
     * Functions for acquire and release weak and strong locks.
     */
    RetStatus WeakLockAcquire(LockAcquireContext lockContext, bool &isAlreadyHeld);
    void WeakLockRelease(const LockTag *tag, LockMode mode);
    void WeakLockBatchRelease(const LockTag *tag, LockMode mode, uint32 unlockTimes);
    RetStatus StrongLockAcquire(LockAcquireContext lockContext, bool &isAlreadyHeld);
    void StrongLockRelease(const LockTag *tag, LockMode mode);
    void StrongLockBatchRelease(const LockTag *tag, LockMode mode, uint32 unlockTimes);

    /**
     * If lazy lock is turned on, strong table locks need to wait until all lazy locks
     * no longer exist before starting to calculate lock conflicts in the main lock table.
     */
    RetStatus WaitLazyLockGoneOnAllThreads(const LockTagCache &tagCache, LockMode mode);
    void EnableLazyLockOnAllThreads(const LockTagCache &tagCache, LockMode mode);

    /**
     * Diagnose functions.
     */
    RetStatus DumpFastpathByLockTag(const LockTagCache &tagCache, StringInfo str);
    RetStatus DumpFastpathSlotModes(ThreadLocalLock::LocalLockEntry *lockEntry, ThreadCore *threadCore, StringInfo str,
                                    bool &found);
};
}

#endif
