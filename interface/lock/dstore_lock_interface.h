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
 * dstore_lock_interface.h
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/dstore/interface/lock/dstore_lock_interface.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_LOCK_INTERFACE_H
#define DSTORE_LOCK_INTERFACE_H

#include "lock/dstore_lock_struct.h"
#include "common/dstore_common_utils.h"

namespace LockInterface {
#pragma GCC visibility push(default)
/*
 * Adapters between DSTORE::TableLockMgr and the SQL Engine's LockRelation
 * APIs such as LockRelation, LockRelationId, LockRelationOid and the
 * corresponding Unlock and ConditionalLock functions.
 */
DSTORE::RetStatus LockTable(TableLockContext *context);
void UnlockTable(TableLockContext *context);
bool IsTableLockedInCurrentTransaction(TableLockContext *context);
bool HasAnyLockInModeInCurrentTransaction(DSTORE::LockMode mode);

DSTORE::RetStatus LockPdb(DSTORE::PdbId pdbId, DSTORE::LockMode mode, bool dontWait = false);
void UnlockPdb(DSTORE::PdbId pdbId, DSTORE::LockMode mode);

DSTORE::RetStatus LockPdbNoWaitWithRetry(DSTORE::PdbId pdbId, DSTORE::LockMode mode, uint8_t maxRetryTime = 120);

DSTORE::RetStatus LockBackupRestore(DSTORE::PdbId pdbId, DSTORE::LockMode mode, bool dontWait = false);
void UnlockBackupRestore(DSTORE::PdbId pdbId, DSTORE::LockMode mode);

DSTORE::RetStatus LockTablespace(TablespaceLockContext *context);
void UnlockTablespace(TablespaceLockContext *context);

DSTORE::RetStatus LockObject(ObjectLockContext *context);
void UnlockObject(ObjectLockContext *context);

DSTORE::RetStatus LockAdvisory(AdvisoryLockContext *context);
DSTORE::RetStatus UnlockAdvisory(AdvisoryLockContext *context);

DSTORE::RetStatus LockPackage(PackageLockContext *context);
void UnlockPackage(PackageLockContext *context);
bool IsPackageLockedInCurrentTransaction(DSTORE::Oid dbId, DSTORE::Oid pkgId, DSTORE::LockMode mode);

DSTORE::RetStatus LockProcedure(ProcedureLockContext *context);
void UnlockProcedure(ProcedureLockContext *context);
bool IsProcedureLockedInCurrentTransaction(DSTORE::Oid dbId, DSTORE::Oid procId, DSTORE::LockMode mode);

void ReleaseAllAdvisorySessionLock();
void ReleaseAllInternalSessionLock();
void ForceReleaseAllSessionLocks();
bool IsHoldingSessionLock();

using GetThreadSqlStatement = char *(*)(pthread_t, DSTORE::AllocMemFunc);
void RegisterGetThreadSqlStatementCallback(GetThreadSqlStatement callback);

/*
 * When a SQL thread who is holding lazy locks needs to start a new thread
 * for ddl and waits for it to end, like by PQconnectdb, call this first
 * to avoid the 2nd thread dead wait on lazy locks.
 */
DSTORE::RetStatus ForceAcquireLazyLocks();

#pragma GCC visibility pop
}
#endif
