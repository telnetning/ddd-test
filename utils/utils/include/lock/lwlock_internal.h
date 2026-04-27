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

#ifndef UTILS_LWLOCK_INTERNAL_H
#define UTILS_LWLOCK_INTERNAL_H

#include <stdbool.h>
#include <stdint.h>
#include "defines/common.h"

GSDB_BEGIN_C_CODE_DECLS

#ifdef ENABLE_THREAD_CHECK
#define TS_ANNOTATE_HAPPENS_BEFORE(addr)       AnnotateHappensBefore(__FILE__, __LINE__, (uintptr_t)addr)
#define TS_ANNOTATE_HAPPENS_AFTER(addr)        AnnotateHappensAfter(__FILE__, __LINE__, (uintptr_t)addr)
#define TS_ANNOTATE_RW_LOCK_CREATE(m)          AnnotateRWLockCreate(__FILE__, __LINE__, (uintptr_t)m)
#define TS_ANNOTATE_RW_LOCK_DESTROY(m)         AnnotateRWLockDestroy(__FILE__, __LINE__, (uintptr_t)m)
#define TS_ANNOTATE_RW_LOCK_ACQUIRED(m, is_w)  AnnotateRWLockAcquired(__FILE__, __LINE__, (uintptr_t)m, is_w)
#define TS_ANNOTATE_RW_LOCK_RELEASED(m, is_w)  AnnotateRWLockReleased(__FILE__, __LINE__, (uintptr_t)m, is_w)
#define TS_ANNOTATE_BENIGN_RACE_SIZED(m, size) AnnotateBenignRaceSized(__FILE__, __LINE__, (uintptr_t)m, size, NULL)
#else
#define TS_ANNOTATE_HAPPENS_BEFORE(addr)
#define TS_ANNOTATE_HAPPENS_AFTER(addr)
#define TS_ANNOTATE_RW_LOCK_CREATE(m)
#define TS_ANNOTATE_RW_LOCK_DESTROY(m)
#define TS_ANNOTATE_RW_LOCK_ACQUIRED(m, is_w)
#define TS_ANNOTATE_RW_LOCK_RELEASED(m, is_w)
#define TS_ANNOTATE_BENIGN_RACE_SIZED(m, size)
#endif /* endif ENABLE_THREAD_CHECK */

#define LW_FLAG_HAS_WAITERS ((uint32_t)1 << 30)
#define LW_FLAG_RELEASE_OK  ((uint32_t)1 << 29)
#define LW_FLAG_LOCKED      ((uint32_t)1 << 28) /* Waiters list spin lock */
#define LW_VAL_EXCLUSIVE    ((uint32_t)1 << 24)
#define LW_LOCK_MASK        ((uint32_t)((1 << 25) - 1))

#define LWLOCK_MAGIC_TAG_INIT 0xBADBEAF
#define LWLOCK_MAGIC_TAG      0xABCDA5A5
#define RESUME_INTERRUPTS     (void)0
#define HOLD_INTERRUPTS       (void)0

inline static uint32_t LWAtomicRead(uint32_t *ptr)
{
    return __atomic_fetch_or(ptr, 0, __ATOMIC_RELAXED);
}

inline static void LWAtomicWrite(uint32_t *ptr, uint32_t val)
{
    *ptr = val;
}

inline static uint32_t LWAtomicFetchSetBit(uint32_t *ptr, uint32_t val)
{
    return __atomic_fetch_or(ptr, val, __ATOMIC_ACQUIRE);
}

inline static uint32_t LWAtomicFetchClearBit(uint32_t *ptr, uint32_t val)
{
    return __atomic_fetch_and(ptr, val, __ATOMIC_RELEASE);
}

inline static uint32_t LWAtomicSetBit(uint32_t *ptr, uint32_t val)
{
    return __atomic_or_fetch(ptr, val, __ATOMIC_RELAXED);
}

inline static uint32_t LWAtomicClearBit(uint32_t *ptr, uint32_t val)
{
    return __atomic_and_fetch(ptr, val, __ATOMIC_RELAXED);
}

inline static uint32_t LWAtomicAdd(uint32_t *ptr, uint32_t val)
{
    return __atomic_fetch_add(ptr, val, __ATOMIC_RELAXED);
}

inline static uint32_t LWAtomicSub(uint32_t *ptr, uint32_t val)
{
    return __atomic_fetch_sub(ptr, val, __ATOMIC_RELAXED);
}

inline static uint32_t LWAtomicSubFetch(uint32_t *ptr, uint32_t val)
{
    return __atomic_sub_fetch(ptr, val, __ATOMIC_RELEASE);
}

inline static bool LWXchg(uint32_t *ptr, uint32_t *oldVal, uint32_t *newVal)
{
    return __atomic_compare_exchange(ptr, oldVal, newVal, 1, __ATOMIC_ACQUIRE, __ATOMIC_ACQUIRE);
}

inline static void LWBarrier(void)
{
    __atomic_thread_fence(__ATOMIC_RELEASE);
}

GSDB_END_C_CODE_DECLS
#endif /* UTILS_LWLOCK_INTERNAL_H */
