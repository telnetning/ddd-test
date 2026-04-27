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
 * dstore_lock_struct.h
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/dstore/interface/lock/dstore_lock_struct.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_LOCK_STRUCT_H
#define DSTORE_LOCK_STRUCT_H

#include "common/dstore_common_utils.h"

namespace DSTORE {
/*
 * LockMode is an integer (1..N) indicating a lock type.
 */
enum LockMode : unsigned char {
    DSTORE_NO_LOCK,
    DSTORE_ACCESS_SHARE_LOCK,           /* SELECT */
    DSTORE_ROW_SHARE_LOCK,              /* SELECT FOR UPDATE/FOR SHARE */
    DSTORE_ROW_EXCLUSIVE_LOCK,          /* INSERT, UPDATE, DELETE */
    DSTORE_SHARE_UPDATE_EXCLUSIVE_LOCK, /* ANALYZE, CREATE INDEX CONCURRENTLY */
    DSTORE_SHARE_LOCK,                  /* CREATE INDEX (WITHOUT CONCURRENTLY) */
    DSTORE_SHARE_ROW_EXCLUSIVE_LOCK,    /* Like EXCLUSIVE MODE, but allows ROW SHARE */
    DSTORE_EXCLUSIVE_LOCK,              /* blocks SELECT...FOR UPDATE */
    DSTORE_ACCESS_EXCLUSIVE_LOCK,       /* ALTER TABLE, DROP TABLE and unqualified LOCK TABLE */
    DSTORE_LOCK_MODE_MAX
};

/* indicate advisory lock type, 1 if using an int8 key, 2 if using 2 int4 keys */
enum class AdvisoryLockType : uint32_t {
    ADVISORY_INT8 = 1,
    ADVISORY_INT4 = 2
};
}

namespace LockInterface {
/*
 * Return values to be used by lock context.
 * The LockAcquireResult enum should be an exact copy of the same enum used
 * by LockAcquire API from lock.h. This is done to avoid including runtime
 * engine headers into the DSTORE code.
 */
enum LockAcquireResult : unsigned char {
    LOCKACQUIRE_NOT_AVAIL,         /* lock not available, and dontWait=true */
    LOCKACQUIRE_OK,                /* lock successfully acquired */
    LOCKACQUIRE_ALREADY_HELD,      /* incremented count for lock already held */
    LOCKACQUIRE_OTHER_ERROR        /* lock failed for some other error */
};

struct TableLockContext {
    DSTORE::Oid dbId;
    DSTORE::Oid relId;
    DSTORE::Oid partId;
    DSTORE::LockMode mode;
    bool dontWait;
    bool isSessionLock;
    bool isPartition;
    LockAcquireResult result;
};

struct ObjectLockContext {
    DSTORE::Oid dbId;
    DSTORE::Oid classId;
    DSTORE::Oid objectId;
    DSTORE::Oid subObjectId1;
    DSTORE::Oid subObjectId2;
    DSTORE::LockMode mode;
    bool dontWait;
    bool isSessionLock;
    LockAcquireResult result;
};

struct AdvisoryLockContext {
    /* u_sess->proc_cxt.MyDatabaseId ... ensures locks are local to each database */
    DSTORE::Oid dbId;
    /* first of 2 int4 keys, or high-order half of an int8 key */
    DSTORE::Oid key1;
    /* second of 2 int4 keys, or low-order half of an int8 key */
    DSTORE::Oid key2;
    /* 1 if using an int8 key, 2 if using 2 int4 keys */
    int type;
    DSTORE::LockMode mode;
    bool dontWait;
    bool isSessionLock;
    LockAcquireResult result;
};

struct PackageLockContext {
    DSTORE::Oid dbId;
    DSTORE::Oid pkgId;
    DSTORE::LockMode mode;
    bool dontWait;
    bool isSessionLock;
    LockAcquireResult result;
};

struct ProcedureLockContext {
    DSTORE::Oid dbId;
    DSTORE::Oid procId;
    DSTORE::LockMode mode;
    bool dontWait;
    bool isSessionLock;
    LockAcquireResult result;
};

struct TablespaceLockContext {
    DSTORE::PdbId pdbId;
    DSTORE::TablespaceId tablespaceId;
    DSTORE::LockMode mode;
    bool dontWait;
    LockAcquireResult result;
};

}
#endif
