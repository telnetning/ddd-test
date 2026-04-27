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
 * posix_atomic.h
 *
 * Description:
 * In the C language project, we temporarily use the GNU extension to implement atomic
 * operations of a specific width type(4 bytes). If large-scale use is needed, we will
 * consider other implementations.
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef UTILS_POSIX_ATOMIC_H
#define UTILS_POSIX_ATOMIC_H

#if defined(__GNUC__) && defined(ATOMIC_BUILTINS)
#include "port/posix_atomic_atomic.h"
#elif defined(__GNUC__) && defined(SYNC_BUILTINS)
#include "port/posix_atomic_sync.h"
#endif

GSDB_BEGIN_C_CODE_DECLS
/*
 * Atomic types syntax is as follows:
 * (1) _Atomic ( type-name )
 * (2) _Atomic type-name
 * 1) Use as a type specifier; this designates a new atomic type.
 * 2) Use as a type qualifier; this designates the atomic version of type-name. In this role,
 * it may be mixed with const, volatile, and restrict, although unlike other qualifiers, the atomic
 * version of type-name may have a different size, alignment, and object representation.
 * type-name: any type other than array or function. For (1), type-name also cannot be atomic
 * or cvr-qualified. The header <stdatomic.h> defines 37 convenience type aliases,
 * from atomic_bool to atomic_uintmax_t, which simplify the use of this keyword with built-in and
 * library types.
 * If the macro constant __STDC_NO_ATOMICS__ is defined by the compiler, the keyword _Atomic is
 * not provided.
 * Objects of atomic types are the only objects that are free from data races; that is, they may be
 * modified by two threads concurrently or modified by one and read by another.
 * Each atomic object has its own associated modification order, which is a total order of modifications
 * made to that object. If, from some thread's point of view, modification A of some atomic M happens-before
 * modification B of the same atomic M, then in the modification order of M, A occurs before B.
 * Note that although each atomic object has its own modification order, there is no single total order;
 * different threads may observe modifications to different atomic objects in different orders.
 *
 * Store Barrier:A store barrier, forces all store instructions prior to
 * the barrier to happen before the barrier and have the store buffers flushed to cache for the CPU
 * on which it is issued.  This will make the program state visible to other CPUs so they can act
 * on it if necessary.
 * Load Barrier:A load barrier,forces all load instructions after the
 * barrier to happen after the barrier and then wait on the load buffer to drain for that CPU.
 * This makes program state exposed from other CPUs visible to this CPU before making further progress.
 * Full Barrier:A full barrier, is a composite of both load and store barriers happening on a CPU.
 *
 * Atomic instructions are effectively a full barrier as they lock the memory sub-system
 * to perform an operation and have guaranteed total order, even across CPUs.
 * Software locks usually employ memory barriers, or atomic instructions,
 * to achieve visibility and preserve program order.
 *
 * Some notes are as follows:
 * Accessing a member of an atomic struct/union is undefined behavior.
 * The library type sig_atomic_t does not provide inter-thread synchronization or memory ordering,
 * only atomicity.
 * The volatile types do not provide inter-thread synchronization, memory ordering, or atomicity.
 * Implementations are recommended to ensure that the representation of _Atomic(T) in C is same as that
 * of std::atomic<T> in C++ for every possible type T. The mechanisms used to ensure atomicity and memory
 * ordering should be compatible.
 */
GSDB_END_C_CODE_DECLS

#endif /* UTILS_POSIX_ATOMIC_H */
