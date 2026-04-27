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
 * Description: Implement spinlock
 */

#ifndef UTILS_SPINLOCK_H
#define UTILS_SPINLOCK_H

#include <stdbool.h>
#include <stdint.h>
#include <unistd.h>
#include "defines/common.h"
#if defined(_WIN32) || defined(_WIN64) || defined(__CYGWIN__) || defined(__MINGW32__) || defined(__BORLANDC__)
#include "port/win32_time.h"
#else
#include "port/posix_time.h"
#endif

GSDB_BEGIN_C_CODE_DECLS
// NOTE: it is possible to bring in exponential back-off spin lock here
/** @typedef Spin lock type */
typedef struct {
    uint32_t lock;
} SpinLock;

typedef struct {
    int spins;
    int delays;
    int curDelay;
} SpinDelayStatus;

/** @define Value denoting lock state (i.e. locked) */
#define SPIN_UNLOCKED                0U
#define SPIN_LOCKED                  (1U << 31)
#define SPIN_DEFAULT_SPINS_PER_DELAY 100
#define SPIN_MIN_DELAY_USEC          1000L
#define SPIN_MAX_DELAY_USEC          1000000L

UTILS_INLINE static int SpinUsleep(int delay)
{
    int ret = delay;
    /* first time to delay or wrap back to minimum delay when max is exceeded */
    if ((delay <= 0) || (delay > SPIN_MAX_DELAY_USEC)) {
        ret = SPIN_MIN_DELAY_USEC;
    }
    Usleep(ret);
    return ret;
}

/*
 * Wait while spinning on a contended spinlock.
 */
UTILS_INLINE static void ContendAdjust(SpinDelayStatus *status)
{
    ASSERT(status != NULL);
    /* Block the process every DEFAULT_SPINS_PER_DELAY tries */
    if (++(status->spins) >= SPIN_DEFAULT_SPINS_PER_DELAY) {
        status->curDelay = SpinUsleep(status->curDelay);
        /* increase delay by a random fraction between 1X and 2X */
        uint64_t time = (uint64_t)(GetCurrentTimeValue().useconds) << 1;
        status->curDelay += (int)(((double)time / SPIN_MAX_DELAY_USEC + 1) * status->curDelay);
        status->spins = 0;
    }
}

UTILS_INLINE static void DelayWait(void)
{
#if defined __i386__ || defined __x86_64__
    __builtin_ia32_pause(); // equivalent to rep; nop
#elif defined __ia64__
    asm volatile("hint @pause" : : : "memory");
#elif defined __arm__ || defined __aarch64__
    asm volatile("wfe" : : : "memory");
#else
    asm volatile("" : : : "memory"); // some web says yield:::memory
#endif
}

UTILS_INLINE static bool SpinXchg(uint32_t *ptr, uint32_t newval, uint32_t oldval)
{
    ASSERT(ptr != NULL);
    // weak compare_exchange
    return __atomic_compare_exchange(ptr, &oldval, &newval, 1, __ATOMIC_ACQUIRE, __ATOMIC_ACQUIRE);
}

UTILS_INLINE static uint32_t SpinRead(uint32_t *ptr)
{
    ASSERT(ptr != NULL);
    uint32_t ret = 0;
    __atomic_load(ptr, &ret, __ATOMIC_RELAXED);
    return ret;
}

UTILS_INLINE static void SpinWrite(uint32_t *ptr, uint32_t val)
{
    ASSERT(ptr != NULL);
    /* memory barrier */
    __atomic_thread_fence(__ATOMIC_RELEASE);
    __atomic_store(ptr, &val, __ATOMIC_RELAXED);
}

/**
 * @brief Init a spin lock.
 * @param lock - The spin lock to initialize
 */
UTILS_INLINE static void SpinLockInit(SpinLock *lock)
{
    ASSERT(lock != NULL);
    lock->lock = SPIN_UNLOCKED;
}

/**
 * @brief Destory a spin lock.
 * @param lock - The spin lock to destory
 */
UTILS_INLINE static void SpinLockDestroy(SYMBOL_UNUSED SpinLock *lock)
{
    ASSERT(lock != NULL);
}

/**
 * @brief Locks a spin lock. The call imposes a full barrier.
 * @param lock - The spin lock to lock.
 */
UTILS_INLINE static void SpinLockAcquire(SpinLock *lock)
{
    ASSERT(lock != NULL);
    SpinDelayStatus delayStatus = {0};
    while (SpinXchg(&(lock->lock), SPIN_LOCKED, SPIN_UNLOCKED) == 0) {
        while (SpinRead(&(lock->lock)) != SPIN_UNLOCKED) {
            DelayWait();
            ContendAdjust(&delayStatus);
        }
    }
}

/**
 * @brief Unlocks a spin lock. <b>No barrier is imposed.</b>
 * @param lock - The spin lock to unlock.
 */
UTILS_INLINE static void SpinLockRelease(SpinLock *lock)
{
    ASSERT(lock != NULL);
    SpinWrite(&(lock->lock), SPIN_UNLOCKED);
#if defined __arm__ || defined __aarch64__
    // wake up CPU by sev instruct in ARM architecture
    asm volatile("sev" : : : "memory");
#endif
}

/**
 * @brief Attempts to lock a spin lock. The call imposes a full barrier.
 * @param lock - The spin-lock to lock
 * @return Zero if lock was acquired, -1 indicate EBUSY.
 */
UTILS_INLINE static int SpinLockTryAcquire(SpinLock *lock)
{
    ASSERT(lock != NULL);
    if ((SpinRead(&(lock->lock)) == SPIN_UNLOCKED) && SpinXchg(&(lock->lock), SPIN_LOCKED, SPIN_UNLOCKED)) {
        return 0;
    }

    return -1;
}

/**
 * @brief Queries whether a spin lock is locked. <b>No barrier is imposed.</b>
 * @param lock - The spin-lock to query its state.
 * @return Non-zero value if the spin-lock is locked, otherwise zero.
 */
UTILS_INLINE static bool SpinLockIsLocked(SpinLock *lock)
{
    ASSERT(lock != NULL);
    return SpinRead(&(lock->lock)) == SPIN_LOCKED;
}
GSDB_END_C_CODE_DECLS
#endif /* __SPINLOCK_H__ */
