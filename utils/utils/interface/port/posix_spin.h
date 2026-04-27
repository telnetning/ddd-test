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
 * posix_spin.h
 *
 * Description:
 * Defines the spin external interfaces wrapper for POSIX standard spin lock.
 *
 * ---------------------------------------------------------------------------------
 */
#ifndef UTILS_POSIX_SPIN_H
#define UTILS_POSIX_SPIN_H

#include <pthread.h>
#include "types/data_types.h"

GSDB_BEGIN_C_CODE_DECLS

/* SpinLock wrapper. */
typedef struct SpinLock SpinLock;
struct SpinLock {
    pthread_spinlock_t spinlock; /* spin controlling the lock */
};

/**
 * Dynamic mode spinlock initialization.
 *
 * @param spinLock: SpinLock
 */
void SpinLockInit(SpinLock *spinLock);

/**
 * Destroy spinlock.
 *
 * @param spinLock: SpinLock
 */
void SpinLockDestroy(SpinLock *spinLock);

/**
 * Lock spinlock.
 *
 * @param spinLock: SpinLock
 */
void SpinLockAcquire(SpinLock *spinLock);

/**
 * Try lock spinlock.
 *
 * @param spinLock: SpinLock
 * @return true if try lock success.
 */
bool SpinLockTryAcquire(SpinLock *spinLock);

/**
 * Unlock spinlock.
 *
 * @param spinLock: SpinLock
 */
void SpinLockRelease(SpinLock *spinLock);

GSDB_END_C_CODE_DECLS
#endif /* UTILS_POSIX_SPIN_H */
