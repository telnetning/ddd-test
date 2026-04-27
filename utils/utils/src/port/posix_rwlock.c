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
 * posix_rwlock.c
 *
 * Description:
 * Implements for POSIX standard rwlock interface.
 *
 * ---------------------------------------------------------------------------------
 */

#include <pthread.h>
#include <time.h>
#include <sys/time.h>
#include "defines/abort.h"
#include "port/posix_rwlock.h"

#define TIME_UNIT 1000

static inline void GenerateAbsoluteTime(struct timespec *tsp, uint32_t timeUSec)
{
    struct timeval now;
    /* get the current time */
    if (unlikely(gettimeofday(&now, NULL) != 0)) {
        tsp->tv_sec = tsp->tv_nsec = 0;
        return;
    }
    tsp->tv_sec = now.tv_sec + (time_t)timeUSec / (TIME_UNIT * TIME_UNIT);
    tsp->tv_nsec = (now.tv_usec + ((time_t)timeUSec % (TIME_UNIT * TIME_UNIT))) * TIME_UNIT;
}

UTILS_EXPORT void RWLockInit(RWLock *rwlock, enum RWLockKind kind)
{
    pthread_rwlockattr_t attr;
    if (pthread_rwlockattr_init(&attr) != 0) {
        Abort();
    }
    if (pthread_rwlockattr_setkind_np(&attr, (int)kind) != 0) {
        Abort();
    }
    if (pthread_rwlock_init(&rwlock->rwlock, &attr) != 0) {
        Abort();
    }
    if (pthread_rwlockattr_destroy(&attr) != 0) {
        Abort();
    }
}

UTILS_EXPORT void RWLockDestroy(RWLock *rwlock)
{
    if (pthread_rwlock_destroy(&rwlock->rwlock) != 0) {
        Abort();
    }
}

UTILS_EXPORT void RWLockRdLock(RWLock *rwlock)
{
    if (pthread_rwlock_rdlock(&rwlock->rwlock) != 0) {
        Abort();
    }
}

UTILS_EXPORT bool RWLockRdLockTimeout(RWLock *rwlock, uint32_t timeUSec)
{
    struct timespec abstime;
    GenerateAbsoluteTime(&abstime, timeUSec);
    int ret = pthread_rwlock_timedrdlock(&rwlock->rwlock, &abstime);
    if (ret != 0) {
        return false;
    }
    return true;
}

UTILS_EXPORT bool RWLockTryRdLock(RWLock *rwlock)
{
    if (pthread_rwlock_tryrdlock(&rwlock->rwlock) == 0) {
        return true;
    }
    return false;
}

UTILS_EXPORT void RWLockWrLock(RWLock *rwlock)
{
    if (pthread_rwlock_wrlock(&rwlock->rwlock) != 0) {
        Abort();
    }
}

UTILS_EXPORT bool RWLockWrLockTimeout(RWLock *rwlock, uint32_t timeUSec)
{
    struct timespec abstime;
    GenerateAbsoluteTime(&abstime, timeUSec);
    int ret = pthread_rwlock_timedwrlock(&rwlock->rwlock, &abstime);
    if (ret != 0) {
        return false;
    }
    return true;
}

UTILS_EXPORT bool RWLockTryWrLock(RWLock *rwlock)
{
    if (pthread_rwlock_trywrlock(&rwlock->rwlock) == 0) {
        return true;
    }
    return false;
}

UTILS_EXPORT void RWLockRdUnlock(RWLock *rwlock)
{
    if (pthread_rwlock_unlock(&rwlock->rwlock) != 0) {
        Abort();
    }
}

UTILS_EXPORT void RWLockWrUnlock(RWLock *rwlock)
{
    if (pthread_rwlock_unlock(&rwlock->rwlock) != 0) {
        Abort();
    }
}