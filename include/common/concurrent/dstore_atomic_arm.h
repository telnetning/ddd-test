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
 * IDENTIFICATION
 *        include/common/concurrent/dstore_atomic_arm.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_ATOMIC_ARM_H
#define DSTORE_ATOMIC_ARM_H
#include "common/dstore_datatype.h"

namespace DSTORE {

#ifdef __aarch64__

/*
 * Exclusive load/store 2 uint64_t variables to fullfil 128bit atomic compare and swap
 */
static inline uint128_u __excl_compare_and_swap_u128(volatile uint128_u *ptr, uint128_u oldval, uint128_u newval)
{
    uint64_t tmp, ret;
    uint128_u old;
    asm volatile("1:     ldxp    %0, %1, %4\n" "       eor     %2, %0, %5\n" "       eor     %3, %1, %6\n"
                 "       orr     %2, %3, %2\n" "       cbnz    %2, 2f\n" "       stlxp   %w2, %7, %8, %4\n"
                 "       cbnz    %w2, 1b\n" "       b 3f\n" "2:" "       stlxp   %w2, %0, %1, %4\n"
                 "       cbnz    %w2, 1b\n" "3:" "       dmb ish\n"
                 : "=&r"(old.u64[0]), "=&r"(old.u64[1]), "=&r"(ret), "=&r"(tmp), "+Q"(ptr->u128)
                 : "r"(oldval.u64[0]), "r"(oldval.u64[1]), "r"(newval.u64[0]), "r"(newval.u64[1])
                 : "memory");
    return old;
}

/*
 * using CASP instinct to atomically compare and swap 2 uint64_t variables to fullfil
 * 128bit atomic compare and swap
 */
static inline uint128_u __lse_compare_and_swap_u128(volatile uint128_u *ptr, uint128_u oldval, uint128_u newval)
{
    uint128_u old;
    register unsigned long x0 asm("x0") = oldval.u64[0];
    register unsigned long x1 asm("x1") = oldval.u64[1];
    register unsigned long x2 asm("x2") = newval.u64[0];
    register unsigned long x3 asm("x3") = newval.u64[1];

    asm volatile(".arch_extension lse\n"
                 "   caspal    %[old_low], %[old_high], %[new_low], %[new_high], %[v]\n"
                 : [ old_low ] "+&r"(x0), [ old_high ] "+&r"(x1), [ v ] "+Q"(*(ptr))
                 : [ new_low ] "r"(x2), [ new_high ] "r"(x3)
                 : "x30", "memory");

    old.u64[0] = x0;
    old.u64[1] = x1;
    return old;
}

#endif /* __aarch64__ */
}
#endif /* STORAGE_ATOMIC_ARM_H */
