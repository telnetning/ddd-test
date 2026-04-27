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
 * ---------------------------------------------------------------------------------
 *
 * win32_rwlock.h
 *
 * Description:
 * Defines the rwlock external interfaces wrapper for windows platform rwlock.
 *
 * ---------------------------------------------------------------------------------
 */
#ifndef UTILS_WIN32_RWLOCK_H
#define UTILS_WIN32_RWLOCK_H

#include <stdlib.h>
#include <windows.h>
#include "types/data_types.h"

GSDB_BEGIN_C_CODE_DECLS

enum RWLockKind { RWLOCK_PREFER_READER_NP = 0, RWLOCK_PREFER_WRITER_NP, RWLOCK_PREFER_WRITER_NONRECURSIVE_NP };

/* rwlock wrapper. */
typedef struct RWLock RWLock;
struct RWLock {
    RTL_SRWLOCK rwlock; /* rwlock controlling the lock */
};

/**
 * Dynamic mode rwlock initialization.
 *
 * @param rwlock: RWLock
 * @param kind: enum RWLockKind
 */
void RWLockInit(RWLock *rwlock, enum RWLockKind kind);

/**
 * Destroy rwlock.
 *
 * @param rwlock: RWLock
 */
void RWLockDestroy(RWLock *rwlock);

/**
 * rwlock read lock.
 *
 * @param rwlock: RWLock
 */
void RWLockRdLock(RWLock *rwlock);

/**
 * rwlock read lock with timeout.
 *
 * @param rwlock: RWLock
 * @param timeUSec: int
 * @return true if read lock success.
 */
bool RWLockRdLockTimeout(RWLock *rwlock, uint32_t timeUSec);

/**
 * rwlock try read lock.
 *
 * @param rwlock: RWLock
 * @return true if try read lock success.
 */
bool RWLockTryRdLock(RWLock *rwlock);

/**
 * rwlock write lock.
 *
 * @param rwlock: RWLock
 */
void RWLockWrLock(RWLock *rwlock);

/**
 * rwlock write lock with timeout.
 *
 * @param rwlock: RWLock
 * @param timeUSec: int
 * @return true if write lock success.
 */
bool RWLockWrLockTimeout(RWLock *rwlock, uint32_t timeUSec);

/**
 * rwlock try write lock.
 *
 * @param rwlock: RWLock
 * @return true if try write lock success.
 */
bool RWLockTryWrLock(RWLock *rwlock);

/**
 * Unlock read rwlock.
 *
 * @param rwlock: RWLock
 */
void RWLockRdUnlock(RWLock *rwlock);

/**
 * Unlock write rwlock.
 *
 * @param rwlock: RWLock
 */
void RWLockWrUnlock(RWLock *rwlock);

GSDB_END_C_CODE_DECLS
#endif /* UTILS_WIN32_RWLOCK_H */
