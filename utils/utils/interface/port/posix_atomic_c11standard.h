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
 * posix_atomic_c11stand.h
 *
 * Description:
 * Atomic operations of the c11 standard version.
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef UTILS_POSIX_ATOMIC_C11STANDARD_H
#define UTILS_POSIX_ATOMIC_C11STANDARD_H

/* The following 2 header files cannot change order */
#include "posix_atomic_c11standard_def.h"
#include "port_atomic.h"

GSDB_BEGIN_C_CODE_DECLS

#define ATOMIC_INIT(...) ATOMIC_VAR_INIT(__VA_ARGS__)

/*
 * Convert the cross-platform memory order into the c11 standard memory order.
 */
static memory_order PortToC11StandardMemoryOrder(MemoryOrder memoryOrder)
{
    switch (memoryOrder) {
        case MEMORY_ORDER_RELAXED:
            return memory_order_relaxed;
        case MEMORY_ORDER_CONSUME:
            return memory_order_consume;
        case MEMORY_ORDER_ACQUIRE:
            return memory_order_acquire;
        case MEMORY_ORDER_RELEASE:
            return memory_order_release;
        case MEMORY_ORDER_ACQ_REL:
            return memory_order_acq_rel;
        case MEMORY_ORDER_SEQ_CST:
            return memory_order_seq_cst;
        default:
            return memory_order_seq_cst;
    }
}

/*
 * This built-in function acts as a synchronization fence between threads based on the specified memory model.
 * All memory orders are valid.
 * The multithreaded memory barrier works the same as the synchronous semantics of using atomic objects, but
 * does not require atomic objects.
 */
static inline void AtomicThreadFence(MemoryOrder memoryOrder)
{
    atomic_thread_fence(PortToC11StandardMemoryOrder(memoryOrder));
}

/*
 * The four functions(get,set,exchange and compareExchange) are non-arithmetic functions.
 * The other functions are arithmetic functions.
 * When implementing patterns for these functions, the memory model parameter can be ignored
 * as long as the pattern implements the most restrictive MEMORY_ORDER_SEQ_CST model. Any of the
 * other memory models execute correctly with this memory model but they may not execute as
 * efficiently as they could with a more appropriate implementation of the relaxed requirements.
 */

/*
 * The following is an atomic operation function of type int8_t.
 */
/*
 * This function implements an atomic get operation. It returns the contents of *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, MEMORY_ORDER_ACQUIRE, and MEMORY_ORDER_CONSUME.
 */
static inline int8_t Atomic8Get(const Atomic8 *atomic8, MemoryOrder memoryOrder)
{
    Atomic8 *atomicVariable = (Atomic8 *)atomic8;
    return atomic_load_explicit(atomicVariable, PortToC11StandardMemoryOrder(memoryOrder));
}
/*
 * This function implements an atomic set operation. It writes value into *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, and MEMORY_ORDER_RELEASE.
 */
static inline void Atomic8Set(Atomic8 *atomic8, int8_t value, MemoryOrder memoryOrder)
{
    atomic_store_explicit(atomic8, value, PortToC11StandardMemoryOrder(memoryOrder));
}
/*
 * This function implements an atomic exchange operation. It writes value into *atomic, and returns
 * the previous contents of *atomic.The valid memory model variants are MEMORY_ORDER_RELAXED,
 * MEMORY_ORDER_SEQ_CST, MEMORY_ORDER_ACQUIRE, MEMORY_ORDER_RELEASE, and MEMORY_ORDER_ACQ_REL.
 */
static inline int8_t Atomic8Exchange(Atomic8 *atomic8, int8_t value, MemoryOrder memoryOrder)
{
    return atomic_exchange_explicit(atomic8, value, PortToC11StandardMemoryOrder(memoryOrder));
}
/*
 * This function implements an atomic compare and exchange operation.
 * This compares the contents of *atomic with the contents of *expected and if equal,
 * writes desired into *atomic. If they are not equal, the current contents of *atomic is written into *expected.
 * True is returned if desired is written into *atomic and the execution is considered
 * to conform to the memory model specified by successMemoryOrder. There are no restrictions
 * on what memory model can be used here.
 * False is returned otherwise, and the execution is considered to conform to failureMemoryOrder.
 * This memory model cannot be MEMORY_ORDER_RELEASE nor MEMORY_ORDER_ACQ_REL.
 * It also cannot be a stronger model than that specified by successMemoryOrder.
 * Weak allows false failures (Fail Spurious). The comparison-swap operation in a loop may
 * be *atomic != *expected, but they are actually equal.The weak version requires a loop, but the
 * strong version does not.
 */
static inline bool Atomic8CompareExchangeWeak(Atomic8 *atomic8, int8_t *expected, int8_t desired,
                                              MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder)
{
    return atomic_compare_exchange_weak_explicit(atomic8, expected, desired,
                                                 PortToC11StandardMemoryOrder(successMemoryOrder),
                                                 PortToC11StandardMemoryOrder(failureMemoryOrder));
}
static inline bool Atomic8CompareExchangeStrong(Atomic8 *atomic8, int8_t *expected, int8_t desired,
                                                MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder)
{
    return atomic_compare_exchange_strong_explicit(atomic8, expected, desired,
                                                   PortToC11StandardMemoryOrder(successMemoryOrder),
                                                   PortToC11StandardMemoryOrder(failureMemoryOrder));
}
/*
 * These functions perform the operation suggested by the name, and return the value that had previously
 * been in *atomic. That is,
 * { tmp = *atomic; *atomic op= value; return tmp; }
 * All memory models are valid.
 */
static inline int8_t Atomic8FetchAdd(Atomic8 *atomic8, int8_t value, MemoryOrder memoryOrder)
{
    return atomic_fetch_add_explicit(atomic8, value, PortToC11StandardMemoryOrder(memoryOrder));
}
static inline int8_t Atomic8FetchSub(Atomic8 *atomic8, int8_t value, MemoryOrder memoryOrder)
{
    return atomic_fetch_sub_explicit(atomic8, value, PortToC11StandardMemoryOrder(memoryOrder));
}
static inline int8_t Atomic8FetchAnd(Atomic8 *atomic8, int8_t value, MemoryOrder memoryOrder)
{
    return atomic_fetch_and_explicit(atomic8, value, PortToC11StandardMemoryOrder(memoryOrder));
}
static inline int8_t Atomic8FetchOr(Atomic8 *atomic8, int8_t value, MemoryOrder memoryOrder)
{
    return atomic_fetch_or_explicit(atomic8, value, PortToC11StandardMemoryOrder(memoryOrder));
}
static inline int8_t Atomic8FetchXor(Atomic8 *atomic8, int8_t value, MemoryOrder memoryOrder)
{
    return atomic_fetch_xor_explicit(atomic8, value, PortToC11StandardMemoryOrder(memoryOrder));
}

/*
 * The following is an atomic operation function of type uint8_t.
 */
/*
 * This function implements an atomic get operation. It returns the contents of *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, MEMORY_ORDER_ACQUIRE, and MEMORY_ORDER_CONSUME.
 */
static inline uint8_t AtomicU8Get(const AtomicU8 *atomic8, MemoryOrder memoryOrder)
{
    AtomicU8 *atomicVariable = (AtomicU8 *)atomic8;
    return atomic_load_explicit(atomicVariable, PortToC11StandardMemoryOrder(memoryOrder));
}
/*
 * This function implements an atomic set operation. It writes value into *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, and MEMORY_ORDER_RELEASE.
 */
static inline void AtomicU8Set(AtomicU8 *atomic8, uint8_t value, MemoryOrder memoryOrder)
{
    atomic_store_explicit(atomic8, value, PortToC11StandardMemoryOrder(memoryOrder));
}
static inline uint8_t AtomicU8Exchange(AtomicU8 *atomic8, uint8_t value, MemoryOrder memoryOrder)
{
    return atomic_exchange_explicit(atomic8, value, PortToC11StandardMemoryOrder(memoryOrder));
}
/*
 * This function implements an atomic compare and exchange operation.
 * This compares the contents of *atomic with the contents of *expected and if equal,
 * writes desired into *atomic. If they are not equal, the current contents of *atomic is written into *expected.
 * True is returned if desired is written into *atomic and the execution is considered
 * to conform to the memory model specified by successMemoryOrder. There are no restrictions
 * on what memory model can be used here.
 * False is returned otherwise, and the execution is considered to conform to failureMemoryOrder.
 * This memory model cannot be MEMORY_ORDER_RELEASE nor MEMORY_ORDER_ACQ_REL.
 * It also cannot be a stronger model than that specified by successMemoryOrder.
 * Weak allows false failures (Fail Spurious). The comparison-swap operation in a loop may
 * be *atomic != *expected, but they are actually equal.The weak version requires a loop, but the
 * strong version does not.
 */
static inline bool AtomicU8CompareExchangeWeak(AtomicU8 *atomic8, uint8_t *expected, uint8_t desired,
                                               MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder)
{
    return atomic_compare_exchange_weak_explicit(atomic8, expected, desired,
                                                 PortToC11StandardMemoryOrder(successMemoryOrder),
                                                 PortToC11StandardMemoryOrder(failureMemoryOrder));
}
static inline bool AtomicU8CompareExchangeStrong(AtomicU8 *atomic8, uint8_t *expected, uint8_t desired,
                                                 MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder)
{
    return atomic_compare_exchange_strong_explicit(atomic8, expected, desired,
                                                   PortToC11StandardMemoryOrder(successMemoryOrder),
                                                   PortToC11StandardMemoryOrder(failureMemoryOrder));
}
/*
 * These functions perform the operation suggested by the name, and return the value that had previously
 * been in *atomic. That is,
 * { tmp = *atomic; *atomic op= value; return tmp; }
 * All memory models are valid.
 */
static inline uint8_t AtomicU8FetchAdd(AtomicU8 *atomic8, uint8_t value, MemoryOrder memoryOrder)
{
    return atomic_fetch_add_explicit(atomic8, value, PortToC11StandardMemoryOrder(memoryOrder));
}
static inline uint8_t AtomicU8FetchSub(AtomicU8 *atomic8, uint8_t value, MemoryOrder memoryOrder)
{
    return atomic_fetch_sub_explicit(atomic8, value, PortToC11StandardMemoryOrder(memoryOrder));
}
static inline uint8_t AtomicU8FetchAnd(AtomicU8 *atomic8, uint8_t value, MemoryOrder memoryOrder)
{
    return atomic_fetch_and_explicit(atomic8, value, PortToC11StandardMemoryOrder(memoryOrder));
}
static inline uint8_t AtomicU8FetchOr(AtomicU8 *atomic8, uint8_t value, MemoryOrder memoryOrder)
{
    return atomic_fetch_or_explicit(atomic8, value, PortToC11StandardMemoryOrder(memoryOrder));
}
static inline uint8_t AtomicU8FetchXor(AtomicU8 *atomic8, uint8_t value, MemoryOrder memoryOrder)
{
    return atomic_fetch_xor_explicit(atomic8, value, PortToC11StandardMemoryOrder(memoryOrder));
}

/*
 * The following is an atomic operation function of type int16_t.
 */
/*
 * This function implements an atomic get operation. It returns the contents of *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, MEMORY_ORDER_ACQUIRE, and MEMORY_ORDER_CONSUME.
 */
static inline int16_t Atomic16Get(const Atomic16 *atomic16, MemoryOrder memoryOrder)
{
    Atomic16 *atomicVariable = (Atomic16 *)atomic16;
    return atomic_load_explicit(atomicVariable, PortToC11StandardMemoryOrder(memoryOrder));
}
/*
 * This function implements an atomic set operation. It writes value into *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, and MEMORY_ORDER_RELEASE.
 */
static inline void Atomic16Set(Atomic16 *atomic16, int16_t value, MemoryOrder memoryOrder)
{
    atomic_store_explicit(atomic16, value, PortToC11StandardMemoryOrder(memoryOrder));
}
static inline int16_t Atomic16Exchange(Atomic16 *atomic16, int16_t value, MemoryOrder memoryOrder)
{
    return atomic_exchange_explicit(atomic16, value, PortToC11StandardMemoryOrder(memoryOrder));
}
/*
 * This function implements an atomic compare and exchange operation.
 * This compares the contents of *atomic with the contents of *expected and if equal,
 * writes desired into *atomic. If they are not equal, the current contents of *atomic is written into *expected.
 * True is returned if desired is written into *atomic and the execution is considered
 * to conform to the memory model specified by successMemoryOrder. There are no restrictions
 * on what memory model can be used here.
 * False is returned otherwise, and the execution is considered to conform to failureMemoryOrder.
 * This memory model cannot be MEMORY_ORDER_RELEASE nor MEMORY_ORDER_ACQ_REL.
 * It also cannot be a stronger model than that specified by successMemoryOrder.
 * Weak allows false failures (Fail Spurious). The comparison-swap operation in a loop may
 * be *atomic != *expected, but they are actually equal.The weak version requires a loop, but the
 * strong version does not.
 */
static inline bool Atomic16CompareExchangeWeak(Atomic16 *atomic16, int16_t *expected, int16_t desired,
                                               MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder)
{
    return atomic_compare_exchange_weak_explicit(atomic16, expected, desired,
        PortToC11StandardMemoryOrder(successMemoryOrder),
        PortToC11StandardMemoryOrder(failureMemoryOrder);
}
static inline bool Atomic16CompareExchangeStrong(Atomic16 *atomic16, int16_t *expected, int16_t desired,
                                                 MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder)
{
    return atomic_compare_exchange_strong_explicit(atomic16, expected, desired,
                                                   PortToC11StandardMemoryOrder(successMemoryOrder),
                                                   PortToC11StandardMemoryOrder(failureMemoryOrder));
}
/*
 * These functions perform the operation suggested by the name, and return the value that had previously
 * been in *atomic. That is,
 * { tmp = *atomic; *atomic op= value; return tmp; }
 * All memory models are valid.
 */
static inline int16_t Atomic16FetchAdd(Atomic16 *atomic16, int16_t value, MemoryOrder memoryOrder)
{
    return atomic_fetch_add_explicit(atomic16, value, PortToC11StandardMemoryOrder(memoryOrder));
}
static inline int16_t Atomic16FetchSub(Atomic16 *atomic16, int16_t value, MemoryOrder memoryOrder)
{
    return atomic_fetch_sub_explicit(atomic16, value, PortToC11StandardMemoryOrder(memoryOrder));
}
static inline int16_t Atomic16FetchAnd(Atomic16 *atomic16, int16_t value, MemoryOrder memoryOrder)
{
    return atomic_fetch_and_explicit(atomic16, value, PortToC11StandardMemoryOrder(memoryOrder));
}
static inline int16_t Atomic16FetchOr(Atomic16 *atomic16, int16_t value, MemoryOrder memoryOrder)
{
    return atomic_fetch_or_explicit(atomic16, value, PortToC11StandardMemoryOrder(memoryOrder));
}
static inline int16_t Atomic16FetchXor(Atomic16 *atomic16, int16_t value, MemoryOrder memoryOrder)
{
    return atomic_fetch_xor_explicit(atomic16, value, PortToC11StandardMemoryOrder(memoryOrder));
}

/*
 * The following is an atomic operation function of type uint16_t.
 */
/*
 * This function implements an atomic get operation. It returns the contents of *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, MEMORY_ORDER_ACQUIRE, and MEMORY_ORDER_CONSUME.
 */
static inline uint16_t AtomicU16Get(const AtomicU16 *atomic16, MemoryOrder memoryOrder)
{
    AtomicU16 *atomicVariable = (AtomicU16 *)atomic16;
    return atomic_load_explicit(atomicVariable, PortToC11StandardMemoryOrder(memoryOrder));
}
/*
 * This function implements an atomic set operation. It writes value into *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, and MEMORY_ORDER_RELEASE.
 */
static inline void AtomicU16Set(AtomicU16 *atomic16, uint16_t value, MemoryOrder memoryOrder)
{
    atomic_store_explicit(atomic16, value, PortToC11StandardMemoryOrder(memoryOrder));
}
static inline uint16_t AtomicU16Exchange(AtomicU16 *atomic16, uint16_t value, MemoryOrder memoryOrder)
{
    return atomic_exchange_explicit(atomic16, value, PortToC11StandardMemoryOrder(memoryOrder));
}
/*
 * This function implements an atomic compare and exchange operation.
 * This compares the contents of *atomic with the contents of *expected and if equal,
 * writes desired into *atomic. If they are not equal, the current contents of *atomic is written into *expected.
 * True is returned if desired is written into *atomic and the execution is considered
 * to conform to the memory model specified by successMemoryOrder. There are no restrictions
 * on what memory model can be used here.
 * False is returned otherwise, and the execution is considered to conform to failureMemoryOrder.
 * This memory model cannot be MEMORY_ORDER_RELEASE nor MEMORY_ORDER_ACQ_REL.
 * It also cannot be a stronger model than that specified by successMemoryOrder.
 * Weak allows false failures (Fail Spurious). The comparison-swap operation in a loop may
 * be *atomic != *expected, but they are actually equal.The weak version requires a loop, but the
 * strong version does not.
 */
static inline bool AtomicU16CompareExchangeWeak(AtomicU16 *atomic16, uint16_t *expected, uint16_t desired,
                                                MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder)
{
    return atomic_compare_exchange_weak_explicit(atomic16, expected, desired,
                                                 PortToC11StandardMemoryOrder(successMemoryOrder),
                                                 PortToC11StandardMemoryOrder(failureMemoryOrder));
}
static inline bool AtomicU16CompareExchangeStrong(AtomicU16 *atomic16, uint16_t *expected, uint16_t desired,
                                                  MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder)
{
    return atomic_compare_exchange_strong_explicit(atomic16, expected, desired,
                                                   PortToC11StandardMemoryOrder(successMemoryOrder),
                                                   PortToC11StandardMemoryOrder(failureMemoryOrder));
}
/*
 * These functions perform the operation suggested by the name, and return the value that had previously
 * been in *atomic. That is,
 * { tmp = *atomic; *atomic op= value; return tmp; }
 * All memory models are valid.
 */
static inline uint16_t AtomicU16FetchAdd(AtomicU16 *atomic16, uint16_t value, MemoryOrder memoryOrder)
{
    return atomic_fetch_add_explicit(atomic16, value, PortToC11StandardMemoryOrder(memoryOrder));
}
static inline uint16_t AtomicU16FetchSub(AtomicU16 *atomic16, uint16_t value, MemoryOrder memoryOrder)
{
    return atomic_fetch_sub_explicit(atomic16, value, PortToC11StandardMemoryOrder(memoryOrder));
}
static inline uint16_t AtomicU16FetchAnd(AtomicU16 *atomic16, uint16_t value, MemoryOrder memoryOrder)
{
    return atomic_fetch_and_explicit(atomic16, value, PortToC11StandardMemoryOrder(memoryOrder));
}
static inline uint16_t AtomicU16FetchOr(AtomicU16 *atomic16, uint16_t value, MemoryOrder memoryOrder)
{
    return atomic_fetch_or_explicit(atomic16, value, PortToC11StandardMemoryOrder(memoryOrder));
}
static inline uint16_t AtomicU16FetchXor(AtomicU16 *atomic16, uint16_t value, MemoryOrder memoryOrder)
{
    return atomic_fetch_xor_explicit(atomic16, value, PortToC11StandardMemoryOrder(memoryOrder));
}

/*
 * The following is an atomic operation function of type int32_t.
 */
/*
 * This function implements an atomic get operation. It returns the contents of *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, MEMORY_ORDER_ACQUIRE, and MEMORY_ORDER_CONSUME.
 */
static inline int32_t Atomic32Get(const Atomic32 *atomic32, MemoryOrder memoryOrder)
{
    Atomic32 *atomicVariable = (Atomic32 *)atomic32;
    return atomic_load_explicit(atomicVariable, PortToC11StandardMemoryOrder(memoryOrder));
}
/*
 * This function implements an atomic set operation. It writes value into *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, and MEMORY_ORDER_RELEASE.
 */
static inline void Atomic32Set(Atomic32 *atomic32, int32_t value, MemoryOrder memoryOrder)
{
    atomic_store_explicit(atomic32, value, PortToC11StandardMemoryOrder(memoryOrder));
}
static inline int32_t Atomic32Exchange(Atomic32 *atomic32, int32_t value, MemoryOrder memoryOrder)
{
    return atomic_exchange_explicit(atomic32, value, PortToC11StandardMemoryOrder(memoryOrder));
}
/*
 * This function implements an atomic compare and exchange operation.
 * This compares the contents of *atomic with the contents of *expected and if equal,
 * writes desired into *atomic. If they are not equal, the current contents of *atomic is written into *expected.
 * True is returned if desired is written into *atomic and the execution is considered
 * to conform to the memory model specified by successMemoryOrder. There are no restrictions
 * on what memory model can be used here.
 * False is returned otherwise, and the execution is considered to conform to failureMemoryOrder.
 * This memory model cannot be MEMORY_ORDER_RELEASE nor MEMORY_ORDER_ACQ_REL.
 * It also cannot be a stronger model than that specified by successMemoryOrder.
 * Weak allows false failures (Fail Spurious). The comparison-swap operation in a loop may
 * be *atomic != *expected, but they are actually equal.The weak version requires a loop, but the
 * strong version does not.
 */
static inline bool Atomic32CompareExchangeWeak(Atomic32 *atomic32, int32_t *expected, int32_t desired,
                                               MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder)
{
    return atomic_compare_exchange_weak_explicit(atomic32, expected, desired,
                                                 PortToC11StandardMemoryOrder(successMemoryOrder),
                                                 PortToC11StandardMemoryOrder(failureMemoryOrder));
}
static inline bool Atomic32CompareExchangeStrong(Atomic32 *atomic32, int32_t *expected, int32_t desired,
                                                 MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder)
{
    return atomic_compare_exchange_strong_explicit(atomic32, expected, desired,
                                                   PortToC11StandardMemoryOrder(successMemoryOrder),
                                                   PortToC11StandardMemoryOrder(failureMemoryOrder));
}
/*
 * These functions perform the operation suggested by the name, and return the value that had previously
 * been in *atomic. That is,
 * { tmp = *atomic; *atomic op= value; return tmp; }
 * All memory models are valid.
 */
static inline int32_t Atomic32FetchAdd(Atomic32 *atomic32, int32_t value, MemoryOrder memoryOrder)
{
    return atomic_fetch_add_explicit(atomic32, value, PortToC11StandardMemoryOrder(memoryOrder));
}
static inline int32_t Atomic32FetchSub(Atomic32 *atomic32, int32_t value, MemoryOrder memoryOrder)
{
    return atomic_fetch_sub_explicit(atomic32, value, PortToC11StandardMemoryOrder(memoryOrder));
}
static inline int32_t Atomic32FetchAnd(Atomic32 *atomic32, int32_t value, MemoryOrder memoryOrder)
{
    return atomic_fetch_and_explicit(atomic32, value, PortToC11StandardMemoryOrder(memoryOrder));
}
static inline int32_t Atomic32FetchOr(Atomic32 *atomic32, int32_t value, MemoryOrder memoryOrder)
{
    return atomic_fetch_or_explicit(atomic32, value, PortToC11StandardMemoryOrder(memoryOrder));
}
static inline int32_t Atomic32FetchXor(Atomic32 *atomic32, int32_t value, MemoryOrder memoryOrder)
{
    return atomic_fetch_xor_explicit(atomic32, value, PortToC11StandardMemoryOrder(memoryOrder));
}

/*
 * The following is an atomic operation function of type uint32_t.
 */
/*
 * This function implements an atomic get operation. It returns the contents of *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, MEMORY_ORDER_ACQUIRE, and MEMORY_ORDER_CONSUME.
 */
static inline uint32_t AtomicU32Get(const AtomicU32 *atomic32, MemoryOrder memoryOrder)
{
    AtomicU32 *atomicVariable = (AtomicU32 *)atomic32;
    return atomic_load_explicit(atomicVariable, PortToC11StandardMemoryOrder(memoryOrder));
}
/*
 * This function implements an atomic set operation. It writes value into *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, and MEMORY_ORDER_RELEASE.
 */
static inline void AtomicU32Set(AtomicU32 *atomic32, uint32_t value, MemoryOrder memoryOrder)
{
    atomic_store_explicit(atomic32, value, PortToC11StandardMemoryOrder(memoryOrder));
}
static inline uint32_t AtomicU32Exchange(AtomicU32 *atomic32, uint32_t value, MemoryOrder memoryOrder)
{
    return atomic_exchange_explicit(atomic32, value, PortToC11StandardMemoryOrder(memoryOrder));
}
/*
 * This function implements an atomic compare and exchange operation.
 * This compares the contents of *atomic with the contents of *expected and if equal,
 * writes desired into *atomic. If they are not equal, the current contents of *atomic is written into *expected.
 * True is returned if desired is written into *atomic and the execution is considered
 * to conform to the memory model specified by successMemoryOrder. There are no restrictions
 * on what memory model can be used here.
 * False is returned otherwise, and the execution is considered to conform to failureMemoryOrder.
 * This memory model cannot be MEMORY_ORDER_RELEASE nor MEMORY_ORDER_ACQ_REL.
 * It also cannot be a stronger model than that specified by successMemoryOrder.
 * Weak allows false failures (Fail Spurious). The comparison-swap operation in a loop may
 * be *atomic != *expected, but they are actually equal.The weak version requires a loop, but the
 * strong version does not.
 */
static inline bool AtomicU32CompareExchangeWeak(AtomicU32 *atomic32, uint32_t *expected, uint32_t desired,
                                                MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder)
{
    return atomic_compare_exchange_weak_explicit(atomic32, expected, desired,
                                                 PortToC11StandardMemoryOrder(successMemoryOrder),
                                                 PortToC11StandardMemoryOrder(failureMemoryOrder));
}
static inline bool AtomicU32CompareExchangeStrong(AtomicU32 *atomic32, uint32_t *expected, uint32_t desired,
                                                  MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder)
{
    return atomic_compare_exchange_strong_explicit(atomic32, expected, desired,
                                                   PortToC11StandardMemoryOrder(successMemoryOrder),
                                                   PortToC11StandardMemoryOrder(failureMemoryOrder));
}
/*
 * These functions perform the operation suggested by the name, and return the value that had previously
 * been in *atomic. That is,
 * { tmp = *atomic; *atomic op= value; return tmp; }
 * All memory models are valid.
 */
static inline uint32_t AtomicU32FetchAdd(AtomicU32 *atomic32, uint32_t value, MemoryOrder memoryOrder)
{
    return atomic_fetch_add_explicit(atomic32, value, PortToC11StandardMemoryOrder(memoryOrder));
}
static inline uint32_t AtomicU32FetchSub(AtomicU32 *atomic32, uint32_t value, MemoryOrder memoryOrder)
{
    return atomic_fetch_sub_explicit(atomic32, value, PortToC11StandardMemoryOrder(memoryOrder));
}
static inline uint32_t AtomicU32FetchAnd(AtomicU32 *atomic32, uint32_t value, MemoryOrder memoryOrder)
{
    return atomic_fetch_and_explicit(atomic32, value, PortToC11StandardMemoryOrder(memoryOrder));
}
static inline uint32_t AtomicU32FetchOr(AtomicU32 *atomic32, uint32_t value, MemoryOrder memoryOrder)
{
    return atomic_fetch_or_explicit(atomic32, value, PortToC11StandardMemoryOrder(memoryOrder));
}
static inline uint32_t AtomicU32FetchXor(AtomicU32 *atomic32, uint32_t value, MemoryOrder memoryOrder)
{
    return atomic_fetch_xor_explicit(atomic32, value, PortToC11StandardMemoryOrder(memoryOrder));
}

/*
 * The following is an atomic operation function of type int64_t.
 */
/*
 * This function implements an atomic get operation. It returns the contents of *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, MEMORY_ORDER_ACQUIRE, and MEMORY_ORDER_CONSUME.
 */
static inline int64_t Atomic64Get(const Atomic64 *atomic64, MemoryOrder memoryOrder)
{
    Atomic64 *atomicVariable = (Atomic64 *)atomic64;
    return atomic_load_explicit(atomicVariable, PortToC11StandardMemoryOrder(memoryOrder));
}
/*
 * This function implements an atomic set operation. It writes value into *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, and MEMORY_ORDER_RELEASE.
 */
static inline void Atomic64Set(Atomic64 *atomic64, int64_t value, MemoryOrder memoryOrder)
{
    atomic_store_explicit(atomic64, value, PortToC11StandardMemoryOrder(memoryOrder));
}
static inline int64_t Atomic64Exchange(Atomic64 *atomic64, int64_t value, MemoryOrder memoryOrder)
{
    return atomic_exchange_explicit(atomic64, value, PortToC11StandardMemoryOrder(memoryOrder));
}
/*
 * This function implements an atomic compare and exchange operation.
 * This compares the contents of *atomic with the contents of *expected and if equal,
 * writes desired into *atomic. If they are not equal, the current contents of *atomic is written into *expected.
 * True is returned if desired is written into *atomic and the execution is considered
 * to conform to the memory model specified by successMemoryOrder. There are no restrictions
 * on what memory model can be used here.
 * False is returned otherwise, and the execution is considered to conform to failureMemoryOrder.
 * This memory model cannot be MEMORY_ORDER_RELEASE nor MEMORY_ORDER_ACQ_REL.
 * It also cannot be a stronger model than that specified by successMemoryOrder.
 * Weak allows false failures (Fail Spurious). The comparison-swap operation in a loop may
 * be *atomic != *expected, but they are actually equal.The weak version requires a loop, but the
 * strong version does not.
 */
static inline bool Atomic64CompareExchangeWeak(Atomic64 *atomic64, int64_t *expected, int64_t desired,
                                               MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder)
{
    return atomic_compare_exchange_weak_explicit(atomic64, expected, desired,
                                                 PortToC11StandardMemoryOrder(successMemoryOrder),
                                                 PortToC11StandardMemoryOrder(failureMemoryOrder));
}
static inline bool Atomic64CompareExchangeStrong(Atomic64 *atomic64, int64_t *expected, int64_t desired,
                                                 MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder)
{
    return atomic_compare_exchange_strong_explicit(atomic64, expected, desired,
                                                   PortToC11StandardMemoryOrder(successMemoryOrder),
                                                   PortToC11StandardMemoryOrder(failureMemoryOrder));
}
/*
 * These functions perform the operation suggested by the name, and return the value that had previously
 * been in *atomic. That is,
 * { tmp = *atomic; *atomic op= value; return tmp; }
 * All memory models are valid.
 */
static inline int64_t Atomic64FetchAdd(Atomic64 *atomic64, int64_t value, MemoryOrder memoryOrder)
{
    return atomic_fetch_add_explicit(atomic64, value, PortToC11StandardMemoryOrder(memoryOrder));
}
static inline int64_t Atomic64FetchSub(Atomic64 *atomic64, int64_t value, MemoryOrder memoryOrder)
{
    return atomic_fetch_sub_explicit(atomic64, value, PortToC11StandardMemoryOrder(memoryOrder));
}
static inline int64_t Atomic64FetchAnd(Atomic64 *atomic64, int64_t value, MemoryOrder memoryOrder)
{
    return atomic_fetch_and_explicit(atomic64, value, PortToC11StandardMemoryOrder(memoryOrder));
}
static inline int64_t Atomic64FetchOr(Atomic64 *atomic64, int64_t value, MemoryOrder memoryOrder)
{
    return atomic_fetch_or_explicit(atomic64, value, PortToC11StandardMemoryOrder(memoryOrder));
}
static inline int64_t Atomic64FetchXor(Atomic64 *atomic64, int64_t value, MemoryOrder memoryOrder)
{
    return atomic_fetch_xor_explicit(atomic64, value, PortToC11StandardMemoryOrder(memoryOrder));
}

/*
 * The following is an atomic operation function of type uint64_t.
 */
/*
 * This function implements an atomic get operation. It returns the contents of *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, MEMORY_ORDER_ACQUIRE, and MEMORY_ORDER_CONSUME.
 */
static inline uint64_t AtomicU64Get(const AtomicU64 *atomic64, MemoryOrder memoryOrder)
{
    AtomicU64 *atomicVariable = (AtomicU64 *)atomic64;
    return atomic_load_explicit(atomicVariable, PortToC11StandardMemoryOrder(memoryOrder));
}
/*
 * This function implements an atomic set operation. It writes value into *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, and MEMORY_ORDER_RELEASE.
 */
static inline void AtomicU64Set(AtomicU64 *atomic64, uint64_t value, MemoryOrder memoryOrder)
{
    atomic_store_explicit(atomic64, value, PortToC11StandardMemoryOrder(memoryOrder));
}
static inline uint64_t AtomicU64Exchange(AtomicU64 *atomic64, uint64_t value, MemoryOrder memoryOrder)
{
    return atomic_exchange_explicit(atomic64, value, PortToC11StandardMemoryOrder(memoryOrder));
}
/*
 * This function implements an atomic compare and exchange operation.
 * This compares the contents of *atomic with the contents of *expected and if equal,
 * writes desired into *atomic. If they are not equal, the current contents of *atomic is written into *expected.
 * True is returned if desired is written into *atomic and the execution is considered
 * to conform to the memory model specified by successMemoryOrder. There are no restrictions
 * on what memory model can be used here.
 * False is returned otherwise, and the execution is considered to conform to failureMemoryOrder.
 * This memory model cannot be MEMORY_ORDER_RELEASE nor MEMORY_ORDER_ACQ_REL.
 * It also cannot be a stronger model than that specified by successMemoryOrder.
 * Weak allows false failures (Fail Spurious). The comparison-swap operation in a loop may
 * be *atomic != *expected, but they are actually equal.The weak version requires a loop, but the
 * strong version does not.
 */
static inline bool AtomicU64CompareExchangeWeak(AtomicU64 *atomic64, uint64_t *expected, uint64_t desired,
                                                MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder)
{
    return atomic_compare_exchange_weak_explicit(atomic64, expected, desired,
                                                 PortToC11StandardMemoryOrder(successMemoryOrder),
                                                 PortToC11StandardMemoryOrder(failureMemoryOrder));
}
static inline bool AtomicU64CompareExchangeStrong(AtomicU64 *atomic64, uint64_t *expected, uint64_t desired,
                                                  MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder)
{
    return atomic_compare_exchange_strong_explicit(atomic64, expected, desired,
                                                   PortToC11StandardMemoryOrder(successMemoryOrder),
                                                   PortToC11StandardMemoryOrder(failureMemoryOrder));
}
/*
 * These functions perform the operation suggested by the name, and return the value that had previously
 * been in *atomic. That is,
 * { tmp = *atomic; *atomic op= value; return tmp; }
 * All memory models are valid.
 */
static inline uint64_t AtomicU64FetchAdd(AtomicU64 *atomic64, uint64_t value, MemoryOrder memoryOrder)
{
    return atomic_fetch_add_explicit(atomic64, value, PortToC11StandardMemoryOrder(memoryOrder));
}
static inline uint64_t AtomicU64FetchSub(AtomicU64 *atomic64, uint64_t value, MemoryOrder memoryOrder)
{
    return atomic_fetch_sub_explicit(atomic64, value, PortToC11StandardMemoryOrder(memoryOrder));
}
static inline uint64_t AtomicU64FetchAnd(AtomicU64 *atomic64, uint64_t value, MemoryOrder memoryOrder)
{
    return atomic_fetch_and_explicit(atomic64, value, PortToC11StandardMemoryOrder(memoryOrder));
}
static inline uint64_t AtomicU64FetchOr(AtomicU64 *atomic64, uint64_t value, MemoryOrder memoryOrder)
{
    return atomic_fetch_or_explicit(atomic64, value, PortToC11StandardMemoryOrder(memoryOrder));
}
static inline uint64_t AtomicU64FetchXor(AtomicU64 *atomic64, uint64_t value, MemoryOrder memoryOrder)
{
    return atomic_fetch_xor_explicit(atomic64, value, PortToC11StandardMemoryOrder(memoryOrder));
}

/*
 * The following is an atomic operation function of type ssize_t.
 */
/*
 * This function implements an atomic get operation. It returns the contents of *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, MEMORY_ORDER_ACQUIRE, and MEMORY_ORDER_CONSUME.
 */
static inline ssize_t AtomicSsizeGet(const AtomicSsize *atomicSsize, MemoryOrder memoryOrder)
{
    AtomicSsize *atomicVariable = (AtomicSsize *)atomicSsize;
    return atomic_load_explicit(atomicVariable, PortToC11StandardMemoryOrder(memoryOrder));
}
/*
 * This function implements an atomic set operation. It writes value into *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, and MEMORY_ORDER_RELEASE.
 */
static inline void AtomicSsizeSet(AtomicSsize *atomicSsize, ssize_t value, MemoryOrder memoryOrder)
{
    atomic_store_explicit(atomicSsize, value, PortToC11StandardMemoryOrder(memoryOrder));
}
static inline ssize_t AtomicSsizeExchange(AtomicSsize *atomicSsize, ssize_t value, MemoryOrder memoryOrder)
{
    return atomic_exchange_explicit(atomicSsize, value, PortToC11StandardMemoryOrder(memoryOrder));
}
/*
 * This function implements an atomic compare and exchange operation.
 * This compares the contents of *atomic with the contents of *expected and if equal,
 * writes desired into *atomic. If they are not equal, the current contents of *atomic is written into *expected.
 * True is returned if desired is written into *atomic and the execution is considered
 * to conform to the memory model specified by successMemoryOrder. There are no restrictions
 * on what memory model can be used here.
 * False is returned otherwise, and the execution is considered to conform to failureMemoryOrder.
 * This memory model cannot be MEMORY_ORDER_RELEASE nor MEMORY_ORDER_ACQ_REL.
 * It also cannot be a stronger model than that specified by successMemoryOrder.
 * Weak allows false failures (Fail Spurious). The comparison-swap operation in a loop may
 * be *atomic != *expected, but they are actually equal.The weak version requires a loop, but the
 * strong version does not.
 */
static inline bool AtomicSsizeCompareExchangeWeak(AtomicSsize *atomicSsize, ssize_t *expected, ssize_t desired,
                                                  MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder)
{
    return atomic_compare_exchange_weak_explicit(atomicSsize, expected, desired,
                                                 PortToC11StandardMemoryOrder(successMemoryOrder),
                                                 PortToC11StandardMemoryOrder(failureMemoryOrder));
}
static inline bool AtomicSSizeCompareExchangeStrong(AtomicSsize *atomicSsize, ssize_t *expected, ssize_t desired,
                                                    MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder)
{
    return atomic_compare_exchange_strong_explicit(atomicSsize, expected, desired,
                                                   PortToC11StandardMemoryOrder(successMemoryOrder),
                                                   PortToC11StandardMemoryOrder(failureMemoryOrder));
}
/*
 * These functions perform the operation suggested by the name, and return the value that had previously
 * been in *atomic. That is,
 * { tmp = *atomic; *atomic op= value; return tmp; }
 * All memory models are valid.
 */
static inline ssize_t AtomicSsizeFetchAdd(AtomicSsize *atomicSsize, ssize_t value, MemoryOrder memoryOrder)
{
    return atomic_fetch_add_explicit(atomicSsize, value, PortToC11StandardMemoryOrder(memoryOrder));
}
static inline ssize_t AtomicSsizeFetchSub(AtomicSsize *atomicSsize, ssize_t value, MemoryOrder memoryOrder)
{
    return atomic_fetch_sub_explicit(atomicSsize, value, PortToC11StandardMemoryOrder(memoryOrder));
}
static inline ssize_t AtomicSsizeFetchAnd(AtomicSsize *atomicSsize, ssize_t value, MemoryOrder memoryOrder)
{
    return atomic_fetch_and_explicit(atomicSsize, value, PortToC11StandardMemoryOrder(memoryOrder));
}
static inline ssize_t AtomicSsizeFetchOr(AtomicSsize *atomicSsize, ssize_t value, MemoryOrder memoryOrder)
{
    return atomic_fetch_or_explicit(atomicSsize, value, PortToC11StandardMemoryOrder(memoryOrder));
}
static inline ssize_t AtomicSsizeFetchXor(AtomicSsize *atomicSsize, ssize_t value, MemoryOrder memoryOrder)
{
    return atomic_fetch_xor_explicit(atomicSsize, value, PortToC11StandardMemoryOrder(memoryOrder));
}

/*
 * The following is an atomic operation function of type size_t.
 */
/*
 * This function implements an atomic get operation. It returns the contents of *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, MEMORY_ORDER_ACQUIRE, and MEMORY_ORDER_CONSUME.
 */
static inline size_t AtomicSizeGet(const AtomicSize *atomicSize, MemoryOrder memoryOrder)
{
    AtomicSize *atomicVariable = (AtomicSize *)atomicSize;
    return atomic_load_explicit(atomicVariable, PortToC11StandardMemoryOrder(memoryOrder));
}
/*
 * This function implements an atomic set operation. It writes value into *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, and MEMORY_ORDER_RELEASE.
 */
static inline void AtomicSizeSet(AtomicSize *atomicSize, size_t value, MemoryOrder memoryOrder)
{
    atomic_store_explicit(atomicSize, value, PortToC11StandardMemoryOrder(memoryOrder));
}
static inline size_t AtomicSizeExchange(AtomicSize *atomicSize, size_t value, MemoryOrder memoryOrder)
{
    return atomic_exchange_explicit(atomicSize, value, PortToC11StandardMemoryOrder(memoryOrder));
}
/*
 * This function implements an atomic compare and exchange operation.
 * This compares the contents of *atomic with the contents of *expected and if equal,
 * writes desired into *atomic. If they are not equal, the current contents of *atomic is written into *expected.
 * True is returned if desired is written into *atomic and the execution is considered
 * to conform to the memory model specified by successMemoryOrder. There are no restrictions
 * on what memory model can be used here.
 * False is returned otherwise, and the execution is considered to conform to failureMemoryOrder.
 * This memory model cannot be MEMORY_ORDER_RELEASE nor MEMORY_ORDER_ACQ_REL.
 * It also cannot be a stronger model than that specified by successMemoryOrder.
 * Weak allows false failures (Fail Spurious). The comparison-swap operation in a loop may
 * be *atomic != *expected, but they are actually equal.The weak version requires a loop, but the
 * strong version does not.
 */
static inline bool AtomicSizeCompareExchangeWeak(AtomicSize *atomicSize, size_t *expected, size_t desired,
                                                 MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder)
{
    return atomic_compare_exchange_weak_explicit(atomicSize, expected, desired,
                                                 PortToC11StandardMemoryOrder(successMemoryOrder),
                                                 PortToC11StandardMemoryOrder(failureMemoryOrder));
}
static inline bool AtomicSizeCompareExchangeStrong(AtomicSize *atomicSize, size_t *expected, size_t desired,
                                                   MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder)
{
    return atomic_compare_exchange_strong_explicit(atomicSize, expected, desired,
                                                   PortToC11StandardMemoryOrder(successMemoryOrder),
                                                   PortToC11StandardMemoryOrder(failureMemoryOrder));
}
/*
 * These functions perform the operation suggested by the name, and return the value that had previously
 * been in *atomic. That is,
 * { tmp = *atomic; *atomic op= value; return tmp; }
 * All memory models are valid.
 */
static inline size_t AtomicSizeFetchAdd(AtomicSize *atomicSize, size_t value, MemoryOrder memoryOrder)
{
    return atomic_fetch_add_explicit(atomicSize, value, PortToC11StandardMemoryOrder(memoryOrder));
}
static inline size_t AtomicSizeFetchSub(AtomicSize *atomicSize, size_t value, MemoryOrder memoryOrder)
{
    return atomic_fetch_sub_explicit(atomicSize, value, PortToC11StandardMemoryOrder(memoryOrder));
}
static inline size_t AtomicSizeFetchAnd(AtomicSize *atomicSize, size_t value, MemoryOrder memoryOrder)
{
    return atomic_fetch_and_explicit(atomicSize, value, PortToC11StandardMemoryOrder(memoryOrder));
}
static inline size_t AtomicSizeFetchOr(AtomicSize *atomicSize, size_t value, MemoryOrder memoryOrder)
{
    return atomic_fetch_or_explicit(atomicSize, value, PortToC11StandardMemoryOrder(memoryOrder));
}
static inline size_t AtomicSizeFetchXor(AtomicSize *atomicSize, size_t value, MemoryOrder memoryOrder)
{
    return atomic_fetch_xor_explicit(atomicSize, value, PortToC11StandardMemoryOrder(memoryOrder));
}

/*
 * The following is an atomic operation function of type void *.
 */
/*
 * This function implements an atomic get operation. It returns the contents of *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, MEMORY_ORDER_ACQUIRE, and MEMORY_ORDER_CONSUME.
 */
static inline void *AtomicPointerGet(const AtomicPointer *atomicPointer, MemoryOrder memoryOrder)
{
    AtomicPointer *atomicVariable = (AtomicPointer *)atomicPointer;
    return atomic_load_explicit(atomicVariable, PortToC11StandardMemoryOrder(memoryOrder));
}
/*
 * This function implements an atomic set operation. It writes value into *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, and MEMORY_ORDER_RELEASE.
 */
static inline void AtomicPointerSet(AtomicPointer *atomicPointer, void *value, MemoryOrder memoryOrder)
{
    atomic_store_explicit(atomicPointer, value, PortToC11StandardMemoryOrder(memoryOrder));
}
static inline void *AtomicPointerExchange(AtomicPointer *atomicPointer, void *value, MemoryOrder memoryOrder)
{
    return atomic_exchange_explicit(atomicPointer, value, PortToC11StandardMemoryOrder(memoryOrder));
}
/*
 * This function implements an atomic compare and exchange operation.
 * This compares the contents of *atomic with the contents of *expected and if equal,
 * writes desired into *atomic. If they are not equal, the current contents of *atomic is written into *expected.
 * True is returned if desired is written into *atomic and the execution is considered
 * to conform to the memory model specified by successMemoryOrder. There are no restrictions
 * on what memory model can be used here.
 * False is returned otherwise, and the execution is considered to conform to failureMemoryOrder.
 * This memory model cannot be MEMORY_ORDER_RELEASE nor MEMORY_ORDER_ACQ_REL.
 * It also cannot be a stronger model than that specified by successMemoryOrder.
 * Weak allows false failures (Fail Spurious). The comparison-swap operation in a loop may
 * be *atomic != *expected, but they are actually equal.The weak version requires a loop, but the
 * strong version does not.
 */
static inline bool AtomicPointerCompareExchangeWeak(AtomicPointer *atomicPointer, void **expected, void *desired,
                                                    MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder)
{
    return atomic_compare_exchange_weak_explicit(atomicPointer, expected, desired,
                                                 PortToC11StandardMemoryOrder(successMemoryOrder),
                                                 PortToC11StandardMemoryOrder(failureMemoryOrder));
}
static inline bool AtomicPointerCompareExchangeStrong(AtomicPointer *atomicPointer, void **expected, void *desired,
                                                      MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder)
{
    return atomic_compare_exchange_strong_explicit(atomicPointer, expected, desired,
                                                   PortToC11StandardMemoryOrder(successMemoryOrder),
                                                   PortToC11StandardMemoryOrder(failureMemoryOrder));
}
/*
 * The following is an atomic operation function of type bool.
 */
/*
 * This function implements an atomic get operation. It returns the contents of *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, MEMORY_ORDER_ACQUIRE, and MEMORY_ORDER_CONSUME.
 */
static inline bool AtomicBoolGet(const AtomicBool *atomicBool, MemoryOrder memoryOrder)
{
    AtomicBool *atomicVariable = (AtomicBool *)atomicBool;
    return atomic_load_explicit(atomicVariable, PortToC11StandardMemoryOrder(memoryOrder));
}
/*
 * This function implements an atomic set operation. It writes value into *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, and MEMORY_ORDER_RELEASE.
 */
static inline void AtomicBoolSet(AtomicBool *atomicBool, bool value, MemoryOrder memoryOrder)
{
    atomic_store_explicit(atomicBool, value, PortToC11StandardMemoryOrder(memoryOrder));
}
static inline bool AtomicBoolExchange(AtomicBool *atomicBool, bool value, MemoryOrder memoryOrder)
{
    return atomic_exchange_explicit(atomicBool, value, PortToC11StandardMemoryOrder(memoryOrder));
}
/*
 * This function implements an atomic compare and exchange operation.
 * This compares the contents of *atomic with the contents of *expected and if equal,
 * writes desired into *atomic. If they are not equal, the current contents of *atomic is written into *expected.
 * True is returned if desired is written into *atomic and the execution is considered
 * to conform to the memory model specified by successMemoryOrder. There are no restrictions
 * on what memory model can be used here.
 * False is returned otherwise, and the execution is considered to conform to failureMemoryOrder.
 * This memory model cannot be MEMORY_ORDER_RELEASE nor MEMORY_ORDER_ACQ_REL.
 * It also cannot be a stronger model than that specified by successMemoryOrder.
 * Weak allows false failures (Fail Spurious). The comparison-swap operation in a loop may
 * be *atomic != *expected, but they are actually equal.The weak version requires a loop, but the
 * strong version does not.
 */
static inline bool AtomicBoolCompareExchangeWeak(AtomicBool *atomicBool, bool *expected, bool desired,
                                                 MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder)
{
    return atomic_compare_exchange_weak_explicit(atomicBool, expected, desired,
                                                 PortToC11StandardMemoryOrder(successMemoryOrder),
                                                 PortToC11StandardMemoryOrder(failureMemoryOrder));
}
static inline bool AtomicBoolCompareExchangeStrong(AtomicBool *atomicBool, bool *expected, bool desired,
                                                   MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder)
{
    return atomic_compare_exchange_strong_explicit(atomicBool, expected, desired,
                                                   PortToC11StandardMemoryOrder(successMemoryOrder),
                                                   PortToC11StandardMemoryOrder(failureMemoryOrder));
}

GSDB_END_C_CODE_DECLS

#endif /* UTILS_POSIX_ATOMIC_C11STANDARD_H */
