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
 * posix_atomic_sync.h
 *
 * Description:
 * Atomic operations of the __sync version.
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef UTILS_POSIX_ATOMIC_SYNC_H
#define UTILS_POSIX_ATOMIC_SYNC_H

#include "port_atomic.h"

GSDB_BEGIN_C_CODE_DECLS

#define ATOMIC_INIT(...) \
    {                    \
        __VA_ARGS__      \
    }

/*
 * This built-in function acts as a synchronization fence between threads based on the specified memory model.
 * All memory orders are valid.
 * The multithreaded memory barrier works the same as the synchronous semantics of using atomic objects, but
 * does not require atomic objects.
 * The asm volatile("" ::: "memory"); is compiler memory barrier. This barrier prevents a compiler from
 * reordering instructions, they do not prevent reordering by CPU.
 * 1.__asm__ is used to instruct the compiler to insert an assembly statement here.
 * 2.__volatile_ is used to inform the compiler that the assembly statement here is not allowed to be recombined
 * with other statements for optimization. I.e., deal with this compilation as it was.
 * 3.The memory force gcc compiler assumes that all memory units of the RAM are modified by assembly instructions.
 * In this way, data in registers in the CPU and cached memory units will be invalidated. The CPU will have to
 * re-read data from memory when needed. This prevents the CPU from using the data in registers and cache to optimize
 * instructions and avoid accessing the memory.
 * 4. ""::: indicates that this is an empty instruction. barrier() does not need to insert a serialized assembly
 * instruction here.
 */
inline void AtomicThreadFence(MemoryOrder memoryOrder)
{
    /* No barrier and full barrier. */
    if (memoryOrder == MEMORY_ORDER_RELAXED) {
        asm volatile("" ::: "memory");
        return;
    }
    if (memoryOrder == MEMORY_ORDER_SEQ_CST) {
        asm volatile("" ::: "memory");
        __sync_synchronize();
        asm volatile("" ::: "memory");
        return;
    }
    asm volatile("" ::: "memory");
#if defined(__i386__) || defined(__x86_64__)
    /* The x86 is implicit, nothing needs to be done. */
#elif defined(__ppc64__)
    asm volatile("lwsync");
#elif defined(__ppc__)
    asm volatile("sync");
#elif defined(__sparc__) && defined(__arch64__)
    if (memoryOrder == MEMORY_ORDER_ACQUIRE) {
        /* membar #LoadLoad | #LoadStore ; is acquire-barrier. */
        asm volatile("membar #LoadLoad | #LoadStore");
    } else if (memoryOrder == MEMORY_ORDER_RELEASE) {
        /* membar #LoadStore | #StoreStore ; is release-barrier. */
        asm volatile("membar #LoadStore | #StoreStore");
    } else {
        asm volatile("membar #LoadLoad | #LoadStore | #StoreStore");
    }
#else
    __sync_synchronize();
#endif
    asm volatile("" ::: "memory");
}

static inline void AtomicBeforeSeqCstLoadFence(void)
{
#if defined(__i386__) || defined(__x86_64__) || (defined(__sparc__) && defined(__arch64__))
    AtomicThreadFence(MEMORY_ORDER_RELAXED);
#else
    AtomicThreadFence(MEMORY_ORDER_SEQ_CST);
#endif
}

static inline void AtomicAfterSeqCstStoreFence(void)
{
#if defined(__i386__) || defined(__x86_64__) || (defined(__sparc__) && defined(__arch64__))
    AtomicThreadFence(MEMORY_ORDER_SEQ_CST);
#else
    AtomicThreadFence(MEMORY_ORDER_RELAXED);
#endif
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
    if (memoryOrder == MEMORY_ORDER_SEQ_CST) {
        AtomicBeforeSeqCstLoadFence();
    }
    int8_t result = atomic8->atomicValue;
    if (memoryOrder != MEMORY_ORDER_RELAXED) {
        AtomicThreadFence(MEMORY_ORDER_ACQUIRE);
    }
    return result;
}
/*
 * This function implements an atomic set operation. It writes value into *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, and MEMORY_ORDER_RELEASE.
 */
static inline void Atomic8Set(Atomic8 *atomic8, int8_t value, MemoryOrder memoryOrder)
{
    if (memoryOrder != MEMORY_ORDER_RELAXED) {
        AtomicThreadFence(MEMORY_ORDER_RELEASE);
    }
    atomic8->atomicValue = value;
    if (memoryOrder == MEMORY_ORDER_SEQ_CST) {
        AtomicAfterSeqCstStoreFence();
    }
}
/*
 * This function implements an atomic exchange operation. It writes value into *atomic, and returns
 * the previous contents of *atomic.The valid memory model variants are MEMORY_ORDER_RELAXED,
 * MEMORY_ORDER_SEQ_CST, MEMORY_ORDER_ACQUIRE, MEMORY_ORDER_RELEASE, and MEMORY_ORDER_ACQ_REL.
 */
static inline int8_t Atomic8Exchange(Atomic8 *atomic8, int8_t value, SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    while (true) {
        int8_t oldValue = atomic8->atomicValue;
        if (__sync_bool_compare_and_swap(&(atomic8->atomicValue), oldValue, value)) {
            return oldValue;
        }
    }
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
                                              SYMBOL_UNUSED MemoryOrder successMemoryOrder,
                                              SYMBOL_UNUSED MemoryOrder failureMemoryOrder)
{
    int8_t prevValue = __sync_val_compare_and_swap(&(atomic8->atomicValue), *expected, desired);
    if (prevValue == *expected) {
        return true;
    } else {
        *expected = prevValue;
        return false;
    }
}
static inline bool Atomic8CompareExchangeStrong(Atomic8 *atomic8, int8_t *expected, int8_t desired,
                                                MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder)
{
    return Atomic8CompareExchangeWeak(atomic8, expected, desired, successMemoryOrder, failureMemoryOrder);
}
/*
 * These functions perform the operation suggested by the name, and return the value that had previously
 * been in *atomic. That is,
 * { tmp = *atomic; *atomic op= value; return tmp; }
 * All memory models are valid.
 */
static inline int8_t Atomic8FetchAdd(Atomic8 *atomic8, int8_t value, SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    return __sync_fetch_and_add(&(atomic8->atomicValue), value);
}
static inline int8_t Atomic8FetchSub(Atomic8 *atomic8, int8_t value, SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    return __sync_fetch_and_sub(&(atomic8->atomicValue), value);
}
static inline int8_t Atomic8FetchAnd(Atomic8 *atomic8, int8_t value, SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    return __sync_fetch_and_and(&(atomic8->atomicValue), value);
}
static inline int8_t Atomic8FetchOr(Atomic8 *atomic8, int8_t value, SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    return __sync_fetch_and_or(&(atomic8->atomicValue), value);
}
static inline int8_t Atomic8FetchXor(Atomic8 *atomic8, int8_t value, SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    return __sync_fetch_and_xor(&(atomic8->atomicValue), value);
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
    if (memoryOrder == MEMORY_ORDER_SEQ_CST) {
        AtomicBeforeSeqCstLoadFence();
    }
    uint8_t result = atomic8->atomicValue;
    if (memoryOrder != MEMORY_ORDER_RELAXED) {
        AtomicThreadFence(MEMORY_ORDER_ACQUIRE);
    }
    return result;
}
/*
 * This function implements an atomic set operation. It writes value into *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, and MEMORY_ORDER_RELEASE.
 */
static inline void AtomicU8Set(AtomicU8 *atomic8, uint8_t value, MemoryOrder memoryOrder)
{
    if (memoryOrder != MEMORY_ORDER_RELAXED) {
        AtomicThreadFence(MEMORY_ORDER_RELEASE);
    }
    atomic8->atomicValue = value;
    if (memoryOrder == MEMORY_ORDER_SEQ_CST) {
        AtomicAfterSeqCstStoreFence();
    }
}
static inline uint8_t AtomicU8Exchange(AtomicU8 *atomic8, uint8_t value, SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    while (true) {
        uint8_t oldValue = atomic8->atomicValue;
        if (__sync_bool_compare_and_swap(&(atomic8->atomicValue), oldValue, value)) {
            return oldValue;
        }
    }
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
                                               SYMBOL_UNUSED MemoryOrder successMemoryOrder,
                                               SYMBOL_UNUSED MemoryOrder failureMemoryOrder)
{
    uint8_t prevValue = __sync_val_compare_and_swap(&(atomic8->atomicValue), *expected, desired);
    if (prevValue == *expected) {
        return true;
    } else {
        *expected = prevValue;
        return false;
    }
}
static inline bool AtomicU8CompareExchangeStrong(AtomicU8 *atomic8, uint8_t *expected, uint8_t desired,
                                                 MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder)
{
    return AtomicU8CompareExchangeWeak(atomic8, expected, desired, successMemoryOrder, failureMemoryOrder);
}
/*
 * These functions perform the operation suggested by the name, and return the value that had previously
 * been in *atomic. That is,
 * { tmp = *atomic; *atomic op= value; return tmp; }
 * All memory models are valid.
 */
static inline uint8_t AtomicU8FetchAdd(AtomicU8 *atomic8, uint8_t value, SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    return __sync_fetch_and_add(&(atomic8->atomicValue), value);
}
static inline uint8_t AtomicU8FetchSub(AtomicU8 *atomic8, uint8_t value, SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    return __sync_fetch_and_sub(&(atomic8->atomicValue), value);
}
static inline uint8_t AtomicU8FetchAnd(AtomicU8 *atomic8, uint8_t value, SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    return __sync_fetch_and_and(&(atomic8->atomicValue), value);
}
static inline uint8_t AtomicU8FetchOr(AtomicU8 *atomic8, uint8_t value, SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    return __sync_fetch_and_or(&(atomic8->atomicValue), value);
}
static inline uint8_t AtomicU8FetchXor(AtomicU8 *atomic8, uint8_t value, SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    return __sync_fetch_and_xor(&(atomic8->atomicValue), value);
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
    if (memoryOrder == MEMORY_ORDER_SEQ_CST) {
        AtomicBeforeSeqCstLoadFence();
    }
    int16_t result = atomic16->atomicValue;
    if (memoryOrder != MEMORY_ORDER_RELAXED) {
        AtomicThreadFence(MEMORY_ORDER_ACQUIRE);
    }
    return result;
}
/*
 * This function implements an atomic set operation. It writes value into *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, and MEMORY_ORDER_RELEASE.
 */
static inline void Atomic16Set(Atomic16 *atomic16, int16_t value, MemoryOrder memoryOrder)
{
    if (memoryOrder != MEMORY_ORDER_RELAXED) {
        AtomicThreadFence(MEMORY_ORDER_RELEASE);
    }
    atomic16->atomicValue = value;
    if (memoryOrder == MEMORY_ORDER_SEQ_CST) {
        AtomicAfterSeqCstStoreFence();
    }
}
static inline int16_t Atomic16Exchange(Atomic16 *atomic16, int16_t value, SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    while (true) {
        int16_t oldValue = atomic16->atomicValue;
        if (__sync_bool_compare_and_swap(&(atomic16->atomicValue), oldValue, value)) {
            return oldValue;
        }
    }
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
                                               SYMBOL_UNUSED MemoryOrder successMemoryOrder,
                                               SYMBOL_UNUSED MemoryOrder failureMemoryOrder)
{
    int16_t prevValue = __sync_val_compare_and_swap(&(atomic16->atomicValue), *expected, desired);
    if (prevValue == *expected) {
        return true;
    } else {
        *expected = prevValue;
        return false;
    }
}
static inline bool Atomic16CompareExchangeStrong(Atomic16 *atomic16, int16_t *expected, int16_t desired,
                                                 MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder)
{
    return Atomic16CompareExchangeWeak(atomic16, expected, desired, successMemoryOrder, failureMemoryOrder);
}
/*
 * These functions perform the operation suggested by the name, and return the value that had previously
 * been in *atomic. That is,
 * { tmp = *atomic; *atomic op= value; return tmp; }
 * All memory models are valid.
 */
static inline int16_t Atomic16FetchAdd(Atomic16 *atomic16, int16_t value, SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    return __sync_fetch_and_add(&(atomic16->atomicValue), value);
}
static inline int16_t Atomic16FetchSub(Atomic16 *atomic16, int16_t value, SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    return __sync_fetch_and_sub(&(atomic16->atomicValue), value);
}
static inline int16_t Atomic16FetchAnd(Atomic16 *atomic16, int16_t value, SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    return __sync_fetch_and_and(&(atomic16->atomicValue), value);
}
static inline int16_t Atomic16FetchOr(Atomic16 *atomic16, int16_t value, SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    return __sync_fetch_and_or(&(atomic16->atomicValue), value);
}
static inline int16_t Atomic16FetchXor(Atomic16 *atomic16, int16_t value, SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    return __sync_fetch_and_xor(&(atomic16->atomicValue), value);
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
    if (memoryOrder == MEMORY_ORDER_SEQ_CST) {
        AtomicBeforeSeqCstLoadFence();
    }
    uint16_t result = atomic16->atomicValue;
    if (memoryOrder != MEMORY_ORDER_RELAXED) {
        AtomicThreadFence(MEMORY_ORDER_ACQUIRE);
    }
    return result;
}
/*
 * This function implements an atomic set operation. It writes value into *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, and MEMORY_ORDER_RELEASE.
 */
static inline void AtomicU16Set(AtomicU16 *atomic16, uint16_t value, MemoryOrder memoryOrder)
{
    if (memoryOrder != MEMORY_ORDER_RELAXED) {
        AtomicThreadFence(MEMORY_ORDER_RELEASE);
    }
    atomic16->atomicValue = value;
    if (memoryOrder == MEMORY_ORDER_SEQ_CST) {
        AtomicAfterSeqCstStoreFence();
    }
}
static inline uint16_t AtomicU16Exchange(AtomicU16 *atomic16, uint16_t value, SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    while (true) {
        uint16_t oldValue = atomic16->atomicValue;
        if (__sync_bool_compare_and_swap(&(atomic16->atomicValue), oldValue, value)) {
            return oldValue;
        }
    }
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
                                                SYMBOL_UNUSED MemoryOrder successMemoryOrder,
                                                SYMBOL_UNUSED MemoryOrder failureMemoryOrder)
{
    uint16_t prevValue = __sync_val_compare_and_swap(&(atomic16->atomicValue), *expected, desired);
    if (prevValue == *expected) {
        return true;
    } else {
        *expected = prevValue;
        return false;
    }
}
static inline bool AtomicU16CompareExchangeStrong(AtomicU16 *atomic16, uint16_t *expected, uint16_t desired,
                                                  MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder)
{
    return AtomicU16CompareExchangeWeak(atomic16, expected, desired, successMemoryOrder, failureMemoryOrder);
}
/*
 * These functions perform the operation suggested by the name, and return the value that had previously
 * been in *atomic. That is,
 * { tmp = *atomic; *atomic op= value; return tmp; }
 * All memory models are valid.
 */
static inline uint16_t AtomicU16FetchAdd(AtomicU16 *atomic16, uint16_t value, SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    return __sync_fetch_and_add(&(atomic16->atomicValue), value);
}
static inline uint16_t AtomicU16FetchSub(AtomicU16 *atomic16, uint16_t value, SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    return __sync_fetch_and_sub(&(atomic16->atomicValue), value);
}
static inline uint16_t AtomicU16FetchAnd(AtomicU16 *atomic16, uint16_t value, SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    return __sync_fetch_and_and(&(atomic16->atomicValue), value);
}
static inline uint16_t AtomicU16FetchOr(AtomicU16 *atomic16, uint16_t value, SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    return __sync_fetch_and_or(&(atomic16->atomicValue), value);
}
static inline uint16_t AtomicU16FetchXor(AtomicU16 *atomic16, uint16_t value, SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    return __sync_fetch_and_xor(&(atomic16->atomicValue), value);
}

/*
 * The following is an atomic operation function of type int32_t.
 */
/*
 * This function implements an atomic get operation. It returns the contents of *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, MEMORY_ORDER_ACQUIRE, and MEMORY_ORDER_CONSUME.
 */
static inline int32_t Atomic32Get(const AtomicInt32 *atomic32, MemoryOrder memoryOrder)
{
    if (memoryOrder == MEMORY_ORDER_SEQ_CST) {
        AtomicBeforeSeqCstLoadFence();
    }
    int32_t result = atomic32->atomicValue;
    if (memoryOrder != MEMORY_ORDER_RELAXED) {
        AtomicThreadFence(MEMORY_ORDER_ACQUIRE);
    }
    return result;
}
/*
 * This function implements an atomic set operation. It writes value into *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, and MEMORY_ORDER_RELEASE.
 */
static inline void Atomic32Set(AtomicInt32 *atomic32, int32_t value, MemoryOrder memoryOrder)
{
    if (memoryOrder != MEMORY_ORDER_RELAXED) {
        AtomicThreadFence(MEMORY_ORDER_RELEASE);
    }
    atomic32->atomicValue = value;
    if (memoryOrder == MEMORY_ORDER_SEQ_CST) {
        AtomicAfterSeqCstStoreFence();
    }
}
static inline int32_t Atomic32Exchange(AtomicInt32 *atomic32, int32_t value, SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    while (true) {
        int32_t oldValue = atomic32->atomicValue;
        if (__sync_bool_compare_and_swap(&(atomic32->atomicValue), oldValue, value)) {
            return oldValue;
        }
    }
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
static inline bool Atomic32CompareExchangeWeak(AtomicInt32 *atomic32, int32_t *expected, int32_t desired,
                                               SYMBOL_UNUSED MemoryOrder successMemoryOrder,
                                               SYMBOL_UNUSED MemoryOrder failureMemoryOrder)
{
    int32_t prevValue = __sync_val_compare_and_swap(&(atomic32->atomicValue), *expected, desired);
    if (prevValue == *expected) {
        return true;
    } else {
        *expected = prevValue;
        return false;
    }
}
static inline bool Atomic32CompareExchangeStrong(AtomicInt32 *atomic32, int32_t *expected, int32_t desired,
                                                 MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder)
{
    return Atomic32CompareExchangeWeak(atomic32, expected, desired, successMemoryOrder, failureMemoryOrder);
}
/*
 * These functions perform the operation suggested by the name, and return the value that had previously
 * been in *atomic. That is,
 * { tmp = *atomic; *atomic op= value; return tmp; }
 * All memory models are valid.
 */
static inline int32_t Atomic32FetchAdd(AtomicInt32 *atomic32, int32_t value, SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    return __sync_fetch_and_add(&(atomic32->atomicValue), value);
}
static inline int32_t Atomic32FetchSub(AtomicInt32 *atomic32, int32_t value, SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    return __sync_fetch_and_sub(&(atomic32->atomicValue), value);
}
static inline int32_t Atomic32FetchAnd(AtomicInt32 *atomic32, int32_t value, SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    return __sync_fetch_and_and(&(atomic32->atomicValue), value);
}
static inline int32_t Atomic32FetchOr(AtomicInt32 *atomic32, int32_t value, SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    return __sync_fetch_and_or(&(atomic32->atomicValue), value);
}
static inline int32_t Atomic32FetchXor(AtomicInt32 *atomic32, int32_t value, SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    return __sync_fetch_and_xor(&(atomic32->atomicValue), value);
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
    if (memoryOrder == MEMORY_ORDER_SEQ_CST) {
        AtomicBeforeSeqCstLoadFence();
    }
    uint32_t result = atomic32->atomicValue;
    if (memoryOrder != MEMORY_ORDER_RELAXED) {
        AtomicThreadFence(MEMORY_ORDER_ACQUIRE);
    }
    return result;
}
/*
 * This function implements an atomic set operation. It writes value into *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, and MEMORY_ORDER_RELEASE.
 */
static inline void AtomicU32Set(AtomicU32 *atomic32, uint32_t value, MemoryOrder memoryOrder)
{
    if (memoryOrder != MEMORY_ORDER_RELAXED) {
        AtomicThreadFence(MEMORY_ORDER_RELEASE);
    }
    atomic32->atomicValue = value;
    if (memoryOrder == MEMORY_ORDER_SEQ_CST) {
        AtomicAfterSeqCstStoreFence();
    }
}
static inline uint32_t AtomicU32Exchange(AtomicU32 *atomic32, uint32_t value, SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    while (true) {
        uint32_t oldValue = atomic32->atomicValue;
        if (__sync_bool_compare_and_swap(&(atomic32->atomicValue), oldValue, value)) {
            return oldValue;
        }
    }
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
                                                SYMBOL_UNUSED MemoryOrder successMemoryOrder,
                                                SYMBOL_UNUSED MemoryOrder failureMemoryOrder)
{
    uint32_t prevValue = __sync_val_compare_and_swap(&(atomic32->atomicValue), *expected, desired);
    if (prevValue == *expected) {
        return true;
    } else {
        *expected = prevValue;
        return false;
    }
}
static inline bool AtomicU32CompareExchangeStrong(AtomicU32 *atomic32, uint32_t *expected, uint32_t desired,
                                                  MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder)
{
    return AtomicU32CompareExchangeWeak(atomic32, expected, desired, successMemoryOrder, failureMemoryOrder);
}
/*
 * These functions perform the operation suggested by the name, and return the value that had previously
 * been in *atomic. That is,
 * { tmp = *atomic; *atomic op= value; return tmp; }
 * All memory models are valid.
 */
static inline uint32_t AtomicU32FetchAdd(AtomicU32 *atomic32, uint32_t value, SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    return __sync_fetch_and_add(&(atomic32->atomicValue), value);
}
static inline uint32_t AtomicU32FetchSub(AtomicU32 *atomic32, uint32_t value, SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    return __sync_fetch_and_sub(&(atomic32->atomicValue), value);
}
static inline uint32_t AtomicU32FetchAnd(AtomicU32 *atomic32, uint32_t value, SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    return __sync_fetch_and_and(&(atomic32->atomicValue), value);
}
static inline uint32_t AtomicU32FetchOr(AtomicU32 *atomic32, uint32_t value, SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    return __sync_fetch_and_or(&(atomic32->atomicValue), value);
}
static inline uint32_t AtomicU32FetchXor(AtomicU32 *atomic32, uint32_t value, SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    return __sync_fetch_and_xor(&(atomic32->atomicValue), value);
}

/*
 * The following is an atomic operation function of type int64_t.
 */
/*
 * This function implements an atomic get operation. It returns the contents of *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, MEMORY_ORDER_ACQUIRE, and MEMORY_ORDER_CONSUME.
 */
static inline int64_t Atomic64Get(const AtomicInt64 *atomic64, MemoryOrder memoryOrder)
{
    if (memoryOrder == MEMORY_ORDER_SEQ_CST) {
        AtomicBeforeSeqCstLoadFence();
    }
    int64_t result = atomic64->atomicValue;
    if (memoryOrder != MEMORY_ORDER_RELAXED) {
        AtomicThreadFence(MEMORY_ORDER_ACQUIRE);
    }
    return result;
}
/*
 * This function implements an atomic set operation. It writes value into *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, and MEMORY_ORDER_RELEASE.
 */
static inline void Atomic64Set(AtomicInt64 *atomic64, int64_t value, MemoryOrder memoryOrder)
{
    if (memoryOrder != MEMORY_ORDER_RELAXED) {
        AtomicThreadFence(MEMORY_ORDER_RELEASE);
    }
    atomic64->atomicValue = value;
    if (memoryOrder == MEMORY_ORDER_SEQ_CST) {
        AtomicAfterSeqCstStoreFence();
    }
}
static inline int64_t Atomic64Exchange(AtomicInt64 *atomic64, int64_t value, SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    while (true) {
        int64_t oldValue = atomic64->atomicValue;
        if (__sync_bool_compare_and_swap(&(atomic64->atomicValue), oldValue, value)) {
            return oldValue;
        }
    }
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
static inline bool Atomic64CompareExchangeWeak(AtomicInt64 *atomic64, int64_t *expected, int64_t desired,
                                               SYMBOL_UNUSED MemoryOrder successMemoryOrder,
                                               SYMBOL_UNUSED MemoryOrder failureMemoryOrder)
{
    int64_t prevValue = __sync_val_compare_and_swap(&(atomic64->atomicValue), *expected, desired);
    if (prevValue == *expected) {
        return true;
    } else {
        *expected = prevValue;
        return false;
    }
}
static inline bool Atomic64CompareExchangeStrong(AtomicInt64 *atomic64, int64_t *expected, int64_t desired,
                                                 MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder)
{
    return Atomic64CompareExchangeWeak(atomic64, expected, desired, successMemoryOrder, failureMemoryOrder);
}
/*
 * These functions perform the operation suggested by the name, and return the value that had previously
 * been in *atomic. That is,
 * { tmp = *atomic; *atomic op= value; return tmp; }
 * All memory models are valid.
 */
static inline int64_t Atomic64FetchAdd(AtomicInt64 *atomic64, int64_t value, SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    return __sync_fetch_and_add(&(atomic64->atomicValue), value);
}
static inline int64_t Atomic64FetchSub(AtomicInt64 *atomic64, int64_t value, SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    return __sync_fetch_and_sub(&(atomic64->atomicValue), value);
}
static inline int64_t Atomic64FetchAnd(AtomicInt64 *atomic64, int64_t value, SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    return __sync_fetch_and_and(&(atomic64->atomicValue), value);
}
static inline int64_t Atomic64FetchOr(AtomicInt64 *atomic64, int64_t value, SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    return __sync_fetch_and_or(&(atomic64->atomicValue), value);
}
static inline int64_t Atomic64FetchXor(AtomicInt64 *atomic64, int64_t value, SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    return __sync_fetch_and_xor(&(atomic64->atomicValue), value);
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
    if (memoryOrder == MEMORY_ORDER_SEQ_CST) {
        AtomicBeforeSeqCstLoadFence();
    }
    uint64_t result = atomic64->atomicValue;
    if (memoryOrder != MEMORY_ORDER_RELAXED) {
        AtomicThreadFence(MEMORY_ORDER_ACQUIRE);
    }
    return result;
}
/*
 * This function implements an atomic set operation. It writes value into *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, and MEMORY_ORDER_RELEASE.
 */
static inline void AtomicU64Set(AtomicU64 *atomic64, uint64_t value, MemoryOrder memoryOrder)
{
    if (memoryOrder != MEMORY_ORDER_RELAXED) {
        AtomicThreadFence(MEMORY_ORDER_RELEASE);
    }
    atomic64->atomicValue = value;
    if (memoryOrder == MEMORY_ORDER_SEQ_CST) {
        AtomicAfterSeqCstStoreFence();
    }
}
static inline uint64_t AtomicU64Exchange(AtomicU64 *atomic64, uint64_t value, SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    while (true) {
        uint64_t oldValue = atomic64->atomicValue;
        if (__sync_bool_compare_and_swap(&(atomic64->atomicValue), oldValue, value)) {
            return oldValue;
        }
    }
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
                                                SYMBOL_UNUSED MemoryOrder successMemoryOrder,
                                                SYMBOL_UNUSED MemoryOrder failureMemoryOrder)
{
    uint64_t prevValue = __sync_val_compare_and_swap(&(atomic64->atomicValue), *expected, desired);
    if (prevValue == *expected) {
        return true;
    } else {
        *expected = prevValue;
        return false;
    }
}
static inline bool AtomicU64CompareExchangeStrong(AtomicU64 *atomic64, uint64_t *expected, uint64_t desired,
                                                  MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder)
{
    return AtomicU64CompareExchangeWeak(atomic64, expected, desired, successMemoryOrder, failureMemoryOrder);
}
/*
 * These functions perform the operation suggested by the name, and return the value that had previously
 * been in *atomic. That is,
 * { tmp = *atomic; *atomic op= value; return tmp; }
 * All memory models are valid.
 */
static inline uint64_t AtomicU64FetchAdd(AtomicU64 *atomic64, uint64_t value, SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    return __sync_fetch_and_add(&(atomic64->atomicValue), value);
}
static inline uint64_t AtomicU64FetchSub(AtomicU64 *atomic64, uint64_t value, SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    return __sync_fetch_and_sub(&(atomic64->atomicValue), value);
}
static inline uint64_t AtomicU64FetchAnd(AtomicU64 *atomic64, uint64_t value, SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    return __sync_fetch_and_and(&(atomic64->atomicValue), value);
}
static inline uint64_t AtomicU64FetchOr(AtomicU64 *atomic64, uint64_t value, SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    return __sync_fetch_and_or(&(atomic64->atomicValue), value);
}
static inline uint64_t AtomicU64FetchXor(AtomicU64 *atomic64, uint64_t value, SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    return __sync_fetch_and_xor(&(atomic64->atomicValue), value);
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
    if (memoryOrder == MEMORY_ORDER_SEQ_CST) {
        AtomicBeforeSeqCstLoadFence();
    }
    ssize_t result = atomicSsize->atomicValue;
    if (memoryOrder != MEMORY_ORDER_RELAXED) {
        AtomicThreadFence(MEMORY_ORDER_ACQUIRE);
    }
    return result;
}
/*
 * This function implements an atomic set operation. It writes value into *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, and MEMORY_ORDER_RELEASE.
 */
static inline void AtomicSsizeSet(AtomicSsize *atomicSsize, ssize_t value, MemoryOrder memoryOrder)
{
    if (memoryOrder != MEMORY_ORDER_RELAXED) {
        AtomicThreadFence(MEMORY_ORDER_RELEASE);
    }
    atomicSsize->atomicValue = value;
    if (memoryOrder == MEMORY_ORDER_SEQ_CST) {
        AtomicAfterSeqCstStoreFence();
    }
}
static inline ssize_t AtomicSsizeExchange(AtomicSsize *atomicSsize, ssize_t value,
                                          SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    while (true) {
        ssize_t oldValue = atomicSsize->atomicValue;
        if (__sync_bool_compare_and_swap(&(atomicSsize->atomicValue), oldValue, value)) {
            return oldValue;
        }
    }
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
                                                  SYMBOL_UNUSED MemoryOrder successMemoryOrder,
                                                  SYMBOL_UNUSED MemoryOrder failureMemoryOrder)
{
    ssize_t prevValue = __sync_val_compare_and_swap(&(atomicSsize->atomicValue), *expected, desired);
    if (prevValue == *expected) {
        return true;
    } else {
        *expected = prevValue;
        return false;
    }
}
static inline bool AtomicSSizeCompareExchangeStrong(AtomicSsize *atomicSsize, ssize_t *expected, ssize_t desired,
                                                    MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder)
{
    return AtomicSsizeCompareExchangeWeak(atomicSsize, expected, desired, successMemoryOrder, failureMemoryOrder);
}
/*
 * These functions perform the operation suggested by the name, and return the value that had previously
 * been in *atomic. That is,
 * { tmp = *atomic; *atomic op= value; return tmp; }
 * All memory models are valid.
 */
static inline ssize_t AtomicSsizeFetchAdd(AtomicSsize *atomicSsize, ssize_t value,
                                          SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    return __sync_fetch_and_add(&(atomicSsize->atomicValue), value);
}
static inline ssize_t AtomicSsizeFetchSub(AtomicSsize *atomicSsize, ssize_t value,
                                          SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    return __sync_fetch_and_sub(&(atomicSsize->atomicValue), value);
}
static inline ssize_t AtomicSsizeFetchAnd(AtomicSsize *atomicSsize, ssize_t value,
                                          SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    return __sync_fetch_and_and(&(atomicSsize->atomicValue), value);
}
static inline ssize_t AtomicSsizeFetchOr(AtomicSsize *atomicSsize, ssize_t value, SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    return __sync_fetch_and_or(&(atomicSsize->atomicValue), value);
}
static inline ssize_t AtomicSsizeFetchXor(AtomicSsize *atomicSsize, ssize_t value,
                                          SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    return __sync_fetch_and_xor(&(atomicSsize->atomicValue), value);
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
    if (memoryOrder == MEMORY_ORDER_SEQ_CST) {
        AtomicBeforeSeqCstLoadFence();
    }
    size_t result = atomicSize->atomicValue;
    if (memoryOrder != MEMORY_ORDER_RELAXED) {
        AtomicThreadFence(MEMORY_ORDER_ACQUIRE);
    }
    return result;
}
/*
 * This function implements an atomic set operation. It writes value into *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, and MEMORY_ORDER_RELEASE.
 */
static inline void AtomicSizeSet(AtomicSize *atomicSize, size_t value, MemoryOrder memoryOrder)
{
    if (memoryOrder != MEMORY_ORDER_RELAXED) {
        AtomicThreadFence(MEMORY_ORDER_RELEASE);
    }
    atomicSize->atomicValue = value;
    if (memoryOrder == MEMORY_ORDER_SEQ_CST) {
        AtomicAfterSeqCstStoreFence();
    }
}
static inline size_t AtomicSizeExchange(AtomicSize *atomicSize, size_t value, SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    while (true) {
        size_t oldValue = atomicSize->atomicValue;
        if (__sync_bool_compare_and_swap(&(atomicSize->atomicValue), oldValue, value)) {
            return oldValue;
        }
    }
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
                                                 SYMBOL_UNUSED MemoryOrder successMemoryOrder,
                                                 SYMBOL_UNUSED MemoryOrder failureMemoryOrder)
{
    size_t prevValue = __sync_val_compare_and_swap(&(atomicSize->atomicValue), *expected, desired);
    if (prevValue == *expected) {
        return true;
    } else {
        *expected = prevValue;
        return false;
    }
}
static inline bool AtomicSizeCompareExchangeStrong(AtomicSize *atomicSize, size_t *expected, size_t desired,
                                                   MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder)
{
    return AtomicSizeCompareExchangeWeak(atomicSize, expected, desired, successMemoryOrder, failureMemoryOrder);
}
/*
 * These functions perform the operation suggested by the name, and return the value that had previously
 * been in *atomic. That is,
 * { tmp = *atomic; *atomic op= value; return tmp; }
 * All memory models are valid.
 */
static inline size_t AtomicSizeFetchAdd(AtomicSize *atomicSize, size_t value, SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    return __sync_fetch_and_add(&(atomicSize->atomicValue), value);
}
static inline size_t AtomicSizeFetchSub(AtomicSize *atomicSize, size_t value, SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    return __sync_fetch_and_sub(&(atomicSize->atomicValue), value);
}
static inline size_t AtomicSizeFetchAnd(AtomicSize *atomicSize, size_t value, SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    return __sync_fetch_and_and(&(atomicSize->atomicValue), value);
}
static inline size_t AtomicSizeFetchOr(AtomicSize *atomicSize, size_t value, SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    return __sync_fetch_and_or(&(atomicSize->atomicValue), value);
}
static inline size_t AtomicSizeFetchXor(AtomicSize *atomicSize, size_t value, SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    return __sync_fetch_and_xor(&(atomicSize->atomicValue), value);
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
    if (memoryOrder == MEMORY_ORDER_SEQ_CST) {
        AtomicBeforeSeqCstLoadFence();
    }
    void *result = atomicPointer->atomicValue;
    if (memoryOrder != MEMORY_ORDER_RELAXED) {
        AtomicThreadFence(MEMORY_ORDER_ACQUIRE);
    }
    return result;
}
/*
 * This function implements an atomic set operation. It writes value into *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, and MEMORY_ORDER_RELEASE.
 */
static inline void AtomicPointerSet(AtomicPointer *atomicPointer, void *value, MemoryOrder memoryOrder)
{
    if (memoryOrder != MEMORY_ORDER_RELAXED) {
        AtomicThreadFence(MEMORY_ORDER_RELEASE);
    }
    atomicPointer->atomicValue = value;
    if (memoryOrder == MEMORY_ORDER_SEQ_CST) {
        AtomicAfterSeqCstStoreFence();
    }
}
static inline void *AtomicPointerExchange(AtomicPointer *atomicPointer, void *value,
                                          SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    while (true) {
        void *oldValue = atomicPointer->atomicValue;
        if (__sync_bool_compare_and_swap(&(atomicPointer->atomicValue), oldValue, value)) {
            return oldValue;
        }
    }
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
                                                    SYMBOL_UNUSED MemoryOrder successMemoryOrder,
                                                    SYMBOL_UNUSED MemoryOrder failureMemoryOrder)
{
    void *prevValue = __sync_val_compare_and_swap(&(atomicPointer->atomicValue), *expected, desired);
    if (prevValue == *expected) {
        return true;
    } else {
        *expected = prevValue;
        return false;
    }
}
static inline bool AtomicPointerCompareExchangeStrong(AtomicPointer *atomicPointer, void **expected, void *desired,
                                                      MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder)
{
    return AtomicPointerCompareExchangeWeak(atomicPointer, expected, desired, successMemoryOrder, failureMemoryOrder);
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
    if (memoryOrder == MEMORY_ORDER_SEQ_CST) {
        AtomicBeforeSeqCstLoadFence();
    }
    bool result = atomicBool->atomicValue;
    if (memoryOrder != MEMORY_ORDER_RELAXED) {
        AtomicThreadFence(MEMORY_ORDER_ACQUIRE);
    }
    return result;
}
/*
 * This function implements an atomic set operation. It writes value into *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, and MEMORY_ORDER_RELEASE.
 */
static inline void AtomicBoolSet(AtomicBool *atomicBool, bool value, MemoryOrder memoryOrder)
{
    if (memoryOrder != MEMORY_ORDER_RELAXED) {
        AtomicThreadFence(MEMORY_ORDER_RELEASE);
    }
    atomicBool->atomicValue = value;
    if (memoryOrder == MEMORY_ORDER_SEQ_CST) {
        AtomicAfterSeqCstStoreFence();
    }
}
static inline bool AtomicBoolExchange(AtomicBool *atomicBool, bool value, SYMBOL_UNUSED MemoryOrder memoryOrder)
{
    while (true) {
        bool oldValue = atomicBool->atomicValue;
        if (__sync_bool_compare_and_swap(&(atomicBool->atomicValue), oldValue, value)) {
            return oldValue;
        }
    }
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
                                                 SYMBOL_UNUSED MemoryOrder successMemoryOrder,
                                                 SYMBOL_UNUSED MemoryOrder failureMemoryOrder)
{
    bool prevValue = __sync_val_compare_and_swap(&(atomicBool->atomicValue), *expected, desired);
    if (prevValue == *expected) {
        return true;
    } else {
        *expected = prevValue;
        return false;
    }
}
static inline bool AtomicBoolCompareExchangeStrong(AtomicBool *atomicBool, bool *expected, bool desired,
                                                   MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder)
{
    return AtomicBoolCompareExchangeWeak(atomicBool, expected, desired, successMemoryOrder, failureMemoryOrder);
}

GSDB_END_C_CODE_DECLS

#endif /* UTILS_POSIX_ATOMIC_SYNC_H */
