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

#ifndef DSTORE_LOCK_MGR_H
#define DSTORE_LOCK_MGR_H

#include "common/memory/dstore_mctx.h"
#include "common/algorithm/dstore_string_info.h"
#include "lock/dstore_lwlock.h"
#include "lock/dstore_lock_datatype.h"
#include "lock/dstore_lock_hash_table.h"
#include "errorcode/dstore_lock_error_code.h"

namespace DSTORE {
class LockMgr : public BaseObject {
public:
    LockMgr() : m_lockTable(nullptr) {};
    virtual ~LockMgr()
    {
        m_lockTable = nullptr;
    }
    DISALLOW_COPY_AND_MOVE(LockMgr);

    /*
     * Initialize and destroy main lock table for LockMgr instance.
     */
    virtual RetStatus Initialize(uint32 hashTableSize, uint32 partitionNum);
    virtual void Destroy();

    /**
     * External interfaces of LockMgr and derived classes.
     * @param[in] tag: unique identifier for the lockable object.
     * @param[in] mode: lock mode to acquire.
     * @param[in] dontWait: if true, don't wait to acquire lock.
     * @param[out] info: return detailed error information, could be NULL if not needed.
     * Side Effects: An error code may be set in the function.
     */
    virtual RetStatus Lock(const LockTag *tag, LockMode mode, bool dontWait, LockErrorInfo *info);
    virtual void Unlock(const LockTag *tag, LockMode mode);

    /*
     * Diagnose Functions.
     */
    virtual RetStatus DumpByLockTag(const LockTag *tag, StringInfo str);
    virtual RetStatus DescribeStatus(StringInfo str);

#ifdef UT
    void *UTGetLockTable() { return m_lockTable; }
#endif

protected:
    /*
     * Main lock table accessing functions.
     */
    RetStatus LockInMainLockTable(const LockAcquireContext &lockContext);
    void UnlockInMainLockTable(const LockTagCache &tag, LockMode mode);

    /*
     * Virtual functions to distinguish lock manager type.
     */
    virtual LockMgrType GetType() const;

    /*
     * Main lock table.
     * LockMgr and derived classes use separated main lock table.
     */
    LockHashTable *m_lockTable;

    /*
     * Error log helper functions.
     * Log will only be printed if LOCK_DEBUG is defined. i.e., "export GS_LOCK_DEBUG=ON".
     * to turn off LOCK_DEBUG, use "unset GS_LOCK_DEBUG".
     */
    void LogStartLockAcquire(const LockTag *tag, LockMode mode) const;
    void LogEndLockAcquire(const LockTag *tag, LockMode mode, RetStatus ret) const;
    void LogLockRelease(const LockTag *tag, LockMode mode) const;
};
}

#endif
