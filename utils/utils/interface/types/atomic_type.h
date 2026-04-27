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
 * atomic_type.h
 *
 * Description:
 * In the C language project, we temporarily use the GNU extension to implement atomic
 * operations of a specific width type(4 bytes). If large-scale use is needed, we will
 * consider other implementations.
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef UTILS_ATOMIC_TYPE_H
#define UTILS_ATOMIC_TYPE_H

#include "types/data_types.h"

GSDB_BEGIN_C_CODE_DECLS

typedef int32_t Atomic32;

/* If target processor supports atomic compare and swap operations on operands 4 bytes in length */
#if defined(__GCC_HAVE_SYNC_COMPARE_AND_SWAP_4)
/* We prefer the new C11-style atomic extension of GCC if available, instead of
 * '__sync_synchronize()' barriar. but clang has no __atomic_xxx_4() */
#if defined(__ATOMIC_SEQ_CST) && !defined(__clang__)

#define GSDB_ATOMIC32_GET(addr) (Atomic32) __atomic_load_4((addr), __ATOMIC_SEQ_CST)

#define GSDB_ATOMIC32_SET(addr, value) (void)__atomic_store_4((addr), (value), __ATOMIC_SEQ_CST)

#else /* defined(__ATOMIC_SEQ_CST) && !defined(__clang__) */

#define GSDB_ATOMIC32_GET(addr) (__sync_synchronize(), (Atomic32)(*(addr)))

#define GSDB_ATOMIC32_SET(addr, value) (*((Atomic32 *)(addr)) = (value), __sync_synchronize())

#endif /* defined(__ATOMIC_SEQ_CST) && !defined(__clang__) */

#define GSDB_ATOMIC32_INC(addr) __sync_fetch_and_add((addr), 1)

#define GSDB_ATOMIC32_DEC(addr) __sync_fetch_and_sub((addr), 1)

#define GSDB_ATOMIC32_ADD(addr, value) __sync_fetch_and_add((addr), (value))

#define GSDB_ATOMIC32_SUB(addr, value) __sync_fetch_and_sub((addr), (value))

#define GSDB_ATOMIC32_AND(addr, value) __sync_fetch_and_and((addr), (value))

#define GSDB_ATOMIC32_OR(addr, value) __sync_fetch_and_or((addr), (value))

#define GSDB_ATOMIC32_XOR(addr, value) __sync_fetch_and_xor((addr), (value))

#define GSDB_ATOMIC32_INC_AND_GET(addr) __sync_add_and_fetch((addr), 1)

#define GSDB_ATOMIC32_DEC_AND_GET(addr) __sync_sub_and_fetch((addr), 1)

#define GSDB_ATOMIC32_DEC_AND_TEST_ZERO(addr) ((bool)(__sync_fetch_and_sub((addr), 1) == 1))

#define GSDB_ATOMIC32_CAS(addr, oldValue, newValue) ((bool)__sync_bool_compare_and_swap((addr), (oldValue), (newValue)))

#define GSDB_ATOMIC_FULL_BARRIER() \
    do {                           \
        __sync_synchronize();      \
    } while (0)

#else /* __GCC_HAVE_SYNC_COMPARE_AND_SWAP_4 */
#error "Atomic operations are not supported currently and need to be implemented in other ways."
#endif /* __GCC_HAVE_SYNC_COMPARE_AND_SWAP_4 */

typedef int64_t Atomic64;

/* If target processor supports atomic compare and swap operations on operands 8 bytes in length  */
#if defined(__GCC_HAVE_SYNC_COMPARE_AND_SWAP_8)
/* We prefer the new C11-style atomic extension of GCC if available, instead of
 * '__sync_synchronize()' barriar. but clang has no __atomic_xxx_8() */
#if defined(__ATOMIC_SEQ_CST) && !defined(__clang__)

#define GSDB_ATOMIC64_GET(addr) (Atomic64) __atomic_load_8((addr), __ATOMIC_SEQ_CST)

#define GSDB_ATOMIC64_SET(addr, value) (void)__atomic_store_8((addr), (value), __ATOMIC_SEQ_CST)

#define GSDB_ATOMIC64_EXCHANGE(addr, value) (Atomic64) __atomic_exchange_n((addr), (value), __ATOMIC_SEQ_CST)

#else /* defined(__ATOMIC_SEQ_CST) && !defined(__clang__) */

#define GSDB_ATOMIC64_GET(addr) (__sync_synchronize(), (Atomic64)(*(addr)))

#define GSDB_ATOMIC64_SET(addr, value) (*((Atomic64 *)(addr)) = (value), __sync_synchronize())

#define GSDB_ATOMIC64_EXCHANGE(addr, value)        \
    ({                                             \
        __sync_synchronize();                      \
        Atomic64 tmpValue = ((Atomic64)(*(addr))); \
        *((Atomic64 *)(addr)) = (value);           \
        __sync_synchronize();                      \
        tmpValue;                                  \
    })

#endif /* defined(__ATOMIC_SEQ_CST) && !defined(__clang__) */

#define GSDB_ATOMIC64_INC(addr) __sync_fetch_and_add((addr), 1)

#define GSDB_ATOMIC64_DEC(addr) __sync_fetch_and_sub((addr), 1)

#define GSDB_ATOMIC64_ADD(addr, value) __sync_fetch_and_add((addr), (value))

#define GSDB_ATOMIC64_SUB(addr, value) __sync_fetch_and_sub((addr), (value))

#define GSDB_ATOMIC64_AND(addr, value) __sync_fetch_and_and((addr), (value))

#define GSDB_ATOMIC64_OR(addr, value) __sync_fetch_and_or((addr), (value))

#define GSDB_ATOMIC64_XOR(addr, value) __sync_fetch_and_xor((addr), (value))

#define GSDB_ATOMIC64_INC_AND_GET(addr) __sync_add_and_fetch((addr), 1)

#define GSDB_ATOMIC64_DEC_AND_TEST_ZERO(addr) ((bool)(__sync_fetch_and_sub((addr), 1) == 1))

#define GSDB_ATOMIC64_CAS(addr, oldValue, newValue) ((bool)__sync_bool_compare_and_swap((addr), (oldValue), (newValue)))

#ifndef GSDB_ATOMIC_FULL_BARRIER
#define GSDB_ATOMIC_FULL_BARRIER() \
    do {                           \
        __sync_synchronize();      \
    } while (0)
#endif

#else /* __GCC_HAVE_SYNC_COMPARE_AND_SWAP_8 */
#error "Atomic operations are not supported currently and need to be implemented in other ways."
#endif /* __GCC_HAVE_SYNC_COMPARE_AND_SWAP_8 */

GSDB_END_C_CODE_DECLS

#endif /* UTILS_ATOMIC_TYPE_H */
