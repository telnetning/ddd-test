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
 * posix_spin.c
 *
 * Description:
 * Implements for POSIX standard spin lock interface.
 *
 * ---------------------------------------------------------------------------------
 */

#include <pthread.h>
#include "defines/abort.h"
#include "port/posix_spin.h"

UTILS_EXPORT void SpinLockInit(SpinLock *spinLock)
{
    if (spinLock == NULL) {
        return;
    }
    if (pthread_spin_init(&spinLock->spinlock, PTHREAD_PROCESS_PRIVATE) != 0) {
        Abort();
    }
}

UTILS_EXPORT void SpinLockDestroy(SpinLock *spinLock)
{
    if (spinLock == NULL) {
        return;
    }
    if (pthread_spin_destroy(&spinLock->spinlock) != 0) {
        Abort();
    }
}

UTILS_EXPORT void SpinLockAcquire(SpinLock *spinLock)
{
    if (spinLock == NULL) {
        return;
    }
    if (pthread_spin_lock(&spinLock->spinlock) != 0) {
        Abort();
    }
}

UTILS_EXPORT bool SpinLockTryAcquire(SpinLock *spinLock)
{
    if (spinLock == NULL) {
        return false;
    }
    if (pthread_spin_trylock(&spinLock->spinlock) != 0) {
        return false;
    }
    return true;
}

UTILS_EXPORT void SpinLockRelease(SpinLock *spinLock)
{
    if (spinLock == NULL) {
        return;
    }
    if (pthread_spin_unlock(&spinLock->spinlock) != 0) {
        Abort();
    }
}