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
 * win32_atomic.h
 *
 * Description:
 * Windows platform atomic operations.
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef UTILS_WIN32_ATOMIC_H
#define UTILS_WIN32_ATOMIC_H
#include "port_atomic.h"

#if !defined(_MSC_VER)
#error "Don't support atomics implemented on this platform."
#endif

GSDB_BEGIN_C_CODE_DECLS

#define ATOMIC_INIT(...) \
    {                    \
        __VA_ARGS__      \
    }

/*
 * The __forceinline keyword overrides the cost/benefit analysis and relies on the
 * judgment of the programmer instead.
 */
#define inline static __forceinline

/*
 * Defined atomic type as a structure to avoid variable calculation by bypassing atomic operation functions.
 * The Windows platform does not provide atomic operation functions corresponding to unsigned types.
 * Unsigned atomic types are encapsulated using signed types. Forcible conversion is performed when
 * the return value is returned.
 */
typedef struct Atomic8 Atomic8;
struct Atomic8 {
    int8_t atomicValue;
};

typedef struct AtomicU8 AtomicU8;
struct AtomicU8 {
    int8_t atomicValue;
};

typedef struct Atomic16 Atomic16;
struct Atomic16 {
    int16_t atomicValue;
};

typedef struct AtomicU16 AtomicU16;
struct AtomicU16 {
    int16_t atomicValue;
};

typedef struct Atomic32 Atomic32;
struct Atomic32 {
    int32_t atomicValue;
};

typedef struct AtomicU32 AtomicU32;
struct AtomicU32 {
    int32_t atomicValue;
};

typedef struct Atomic64 Atomic64;
struct Atomic64 {
    __int64 atomicValue;
};

typedef struct AtomicU64 AtomicU64;
struct AtomicU64 {
    __int64 atomicValue;
};

typedef struct AtomicSsize AtomicSsize;
struct AtomicSsize {
    ssize_t atomicValue;
};

typedef struct AtomicSize AtomicSize;
struct AtomicSize {
    ssize_t atomicValue;
};

typedef struct AtomicPointer AtomicPointer;
struct AtomicPointer {
#if CURRENT_COMPILE_MODE == COMPILE_MODE_32_BIT
    int32_t atomicValue;
#else
    int64_t atomicValue;
#endif
};

typedef struct AtomicBool AtomicBool;
struct AtomicBool {
    int8_t atomicValue;
};

/*
 * This built-in function acts as a synchronization fence between threads based on the specified memory model.
 * All memory orders are valid.
 * The multithreaded memory barrier works the same as the synchronous semantics of using atomic objects, but
 * does not require atomic objects.
 * _ReadBarrier, _WriteBarrier, and _ReadWriteBarrier limits the compiler optimizations
 * that can reorder memory accesses across the point of the call.
 * MemoryBarrier() creates a hardware memory barrier (fence) that prevents the CPU from
 * re-ordering read and write operations. It may also prevent the compiler from re-ordering
 * read and write operations.
 */
inline void AtomicThreadFence(MemoryOrder memoryOrder)
{
    _ReadWriteBarrier();
#if defined(_M_ARM) || defined(_M_ARM64)
    /* ARM needs a barrier for everything but MEMORY_ORDER_RELAXED. */
    if (memoryOrder != MEMORY_ORDER_RELAXED) {
        MemoryBarrier();
    }
#elif defined(_M_IX86) || defined(_M_X64)
    /* x86 needs a barrier only for MEMORY_ORDER_SEQ_CST. */
    if (memoryOrder == MEMORY_ORDER_SEQ_CST) {
        MemoryBarrier();
    }
#else
#error "Don't create atomics for this platform for MSVC."
#endif
    _ReadWriteBarrier();
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
inline int8_t Atomic8Get(const Atomic8 *atomic8, MemoryOrder memoryOrder)
{
    int8_t retValue = atomic8->atomicValue;
    if (memoryOrder != MEMORY_ORDER_RELAXED) {
        AtomicThreadFence(MEMORY_ORDER_ACQUIRE);
    }
    return retValue;
}

/*
 * This function implements an atomic set operation. It writes value into *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, and MEMORY_ORDER_RELEASE.
 */
inline void Atomic8Set(Atomic8 *atomic8, int8_t value, MemoryOrder memoryOrder)
{
    if (memoryOrder != MEMORY_ORDER_RELAXED) {
        AtomicThreadFence(MEMORY_ORDER_RELEASE);
    }
    atomic8->atomicValue = value;
    if (memoryOrder == MEMORY_ORDER_SEQ_CST) {
        AtomicThreadFence(MEMORY_ORDER_SEQ_CST);
    }
}

/*
 * This function implements an atomic exchange operation. It writes value into *atomic, and returns
 * the previous contents of *atomic.The valid memory model variants are MEMORY_ORDER_RELAXED,
 * MEMORY_ORDER_SEQ_CST, MEMORY_ORDER_ACQUIRE, MEMORY_ORDER_RELEASE, and MEMORY_ORDER_ACQ_REL.
 */
inline int8_t Atomic8Exchange(Atomic8 *atomic8, int8_t value, MemoryOrder memoryOrder)
{
    return (int8_t)_InterlockedExchange8(&(atomic8->atomicValue), value);
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
inline bool Atomic8CompareExchangeWeak(Atomic8 *atomic8, int8_t *expected, int8_t desired,
                                       MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder)
{
    int8_t expectedValue = *expected;
    int8_t desiredValue = desired;
    int8_t old = _InterlockedCompareExchange8(&(atomic8->atomicValue), desiredValue, expectedValue);
    if (old == expectedValue) {
        return true;
    } else {
        *expected = old;
        return false;
    }
}

inline bool Atomic8CompareExchangeStrong(Atomic8 *atomic8, int8_t *expected, int8_t desired,
                                         MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder)
{
    /* This implement the weak version with strong semantics. */
    return Atomic8CompareExchangeWeak(atomic8, expected, desired, successMemoryOrder, failureMemoryOrder);
}

/*
 * These functions perform the operation suggested by the name, and return the value that had previously
 * been in *atomic. That is,
 * { tmp = *atomic; *atomic op= value; return tmp; }
 * All memory models are valid.
 */
inline int8_t Atomic8FetchAdd(Atomic8 *atomic8, int8_t value, MemoryOrder memoryOrder)
{
    return (int8_t)_InterlockedExchangeAdd8(&(atomic8->atomicValue), value);
}

#define MSVC_NEGATION_OPERANDS_WARN 4146
inline int8_t Atomic8FetchSub(Atomic8 *atomic8, int8_t value, MemoryOrder memoryOrder)
{
    /*
     * MSVC warns on negation of unsigned operands, but the semantics is right(MAX_TYPE + 1 - operand).
     */
    // clang-format off
    __pragma(warning(push))
    __pragma(warning(disable: MSVC_NEGATION_OPERANDS_WARN))
    return Atomic8FetchAdd(atomic8, -value, memoryOrder);
    __pragma(warning(pop))
    // clang-format on
}

inline int8_t Atomic8FetchAnd(Atomic8 *atomic8, int8_t value, MemoryOrder memoryOrder)
{
    return (int8_t)_InterlockedAnd8(&(atomic8->atomicValue), value);
}

inline int8_t Atomic8FetchOr(Atomic8 *atomic8, int8_t value, MemoryOrder memoryOrder)
{
    return (int8_t)_InterlockedOr8(&(atomic8->atomicValue), value);
}

inline int8_t Atomic8FetchXor(Atomic8 *atomic8, int8_t value, MemoryOrder memoryOrder)
{
    return (int8_t)_InterlockedXor8(&(atomic8->atomicValue), value);
}

/*
 * The following is an atomic operation function of type uint8_t.
 */
/*
 * This function implements an atomic get operation. It returns the contents of *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, MEMORY_ORDER_ACQUIRE, and MEMORY_ORDER_CONSUME.
 */
inline uint8_t AtomicU8Get(const AtomicU8 *atomic8, MemoryOrder memoryOrder)
{
    int8_t retValue = atomic8->atomicValue;
    if (memoryOrder != MEMORY_ORDER_RELAXED) {
        AtomicThreadFence(MEMORY_ORDER_ACQUIRE);
    }
    return (uint8_t)retValue;
}

/*
 * This function implements an atomic set operation. It writes value into *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, and MEMORY_ORDER_RELEASE.
 */
inline void AtomicU8Set(AtomicU8 *atomic8, uint8_t value, MemoryOrder memoryOrder)
{
    if (memoryOrder != MEMORY_ORDER_RELAXED) {
        AtomicThreadFence(MEMORY_ORDER_RELEASE);
    }
    atomic8->atomicValue = (int8_t)value;
    if (memoryOrder == MEMORY_ORDER_SEQ_CST) {
        AtomicThreadFence(MEMORY_ORDER_SEQ_CST);
    }
}

/*
 * This function implements an atomic exchange operation. It writes value into *atomic, and returns
 * the previous contents of *atomic.The valid memory model variants are MEMORY_ORDER_RELAXED,
 * MEMORY_ORDER_SEQ_CST, MEMORY_ORDER_ACQUIRE, MEMORY_ORDER_RELEASE, and MEMORY_ORDER_ACQ_REL.
 */
inline uint8_t AtomicU8Exchange(AtomicU8 *atomic8, uint8_t value, MemoryOrder memoryOrder)
{
    return (uint8_t)_InterlockedExchange8(&(atomic8->atomicValue), (int8_t)value);
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
inline bool AtomicU8CompareExchangeWeak(AtomicU8 *atomic8, uint8_t *expected, uint8_t desired,
                                        MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder)
{
    int8_t expectedValue = (int8_t)*expected;
    int8_t desiredValue = (int8_t)desired;
    int8_t old = _InterlockedCompareExchange8(&(atomic8->atomicValue), desiredValue, expectedValue);
    if (old == expectedValue) {
        return true;
    } else {
        *expected = (uint8_t)old;
        return false;
    }
}

inline bool AtomicU8CompareExchangeStrong(AtomicU8 *atomic8, uint8_t *expected, uint8_t desired,
                                          MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder)
{
    /* This implement the weak version with strong semantics. */
    return AtomicU8CompareExchangeWeak(atomic8, expected, desired, successMemoryOrder, failureMemoryOrder);
}

/*
 * These functions perform the operation suggested by the name, and return the value that had previously
 * been in *atomic. That is,
 * { tmp = *atomic; *atomic op= value; return tmp; }
 * All memory models are valid.
 */
inline uint8_t AtomicU8FetchAdd(AtomicU8 *atomic8, uint8_t value, MemoryOrder memoryOrder)
{
    return (uint8_t)_InterlockedExchangeAdd8(&(atomic8->atomicValue), (int8_t)value);
}

inline uint8_t AtomicU8FetchSub(AtomicU8 *atomic8, uint8_t value, MemoryOrder memoryOrder)
{
    /*
     * MSVC warns on negation of unsigned operands, but the semantics is right(MAX_TYPE + 1 - operand).
     */
    // clang-format off
    __pragma (warning(push))
    __pragma(warning(disable: MSVC_NEGATION_OPERANDS_WARN))
    return AtomicU8FetchAdd(atomic8, -value, memoryOrder);
    __pragma (warning(pop))
    // clang-format on
}

inline uint8_t AtomicU8FetchAnd(AtomicU8 *atomic8, uint8_t value, MemoryOrder memoryOrder)
{
    return (uint8_t)_InterlockedAnd8(&(atomic8->atomicValue), (int8_t)value);
}

inline uint8_t AtomicU8FetchOr(AtomicU8 *atomic8, uint8_t value, MemoryOrder memoryOrder)
{
    return (uint8_t)_InterlockedOr8(&(atomic8->atomicValue), (int8_t)value);
}

inline uint8_t AtomicU8FetchXor(AtomicU8 *atomic8, uint8_t value, MemoryOrder memoryOrder)
{
    return (uint8_t)_InterlockedXor8(&(atomic8->atomicValue), (int8_t)value);
}

/*
 * The following is an atomic operation function of type int16_t.
 */
/*
 * This function implements an atomic get operation. It returns the contents of *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, MEMORY_ORDER_ACQUIRE, and MEMORY_ORDER_CONSUME.
 */
inline int16_t Atomic16Get(const Atomic16 *atomic16, MemoryOrder memoryOrder)
{
    int16_t retValue = atomic16->atomicValue;
    if (memoryOrder != MEMORY_ORDER_RELAXED) {
        AtomicThreadFence(MEMORY_ORDER_ACQUIRE);
    }
    return retValue;
}

/*
 * This function implements an atomic set operation. It writes value into *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, and MEMORY_ORDER_RELEASE.
 */
inline void Atomic16Set(Atomic16 *atomic16, int16_t value, MemoryOrder memoryOrder)
{
    if (memoryOrder != MEMORY_ORDER_RELAXED) {
        AtomicThreadFence(MEMORY_ORDER_RELEASE);
    }
    atomic16->atomicValue = value;
    if (memoryOrder == MEMORY_ORDER_SEQ_CST) {
        AtomicThreadFence(MEMORY_ORDER_SEQ_CST);
    }
}

/*
 * This function implements an atomic exchange operation. It writes value into *atomic, and returns
 * the previous contents of *atomic.The valid memory model variants are MEMORY_ORDER_RELAXED,
 * MEMORY_ORDER_SEQ_CST, MEMORY_ORDER_ACQUIRE, MEMORY_ORDER_RELEASE, and MEMORY_ORDER_ACQ_REL.
 */
inline int16_t Atomic16Exchange(Atomic16 *atomic16, int16_t value, MemoryOrder memoryOrder)
{
    return (int16_t)_InterlockedExchange8(&(atomic16->atomicValue), value);
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
inline bool Atomic16CompareExchangeWeak(Atomic16 *atomic16, int16_t *expected, int16_t desired,
                                        MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder)
{
    int16_t expectedValue = *expected;
    int16_t desiredValue = desired;
    int16_t old = _InterlockedCompareExchange16(&(atomic16->atomicValue), desiredValue, expectedValue);
    if (old == expectedValue) {
        return true;
    } else {
        *expected = old;
        return false;
    }
}

inline bool Atomic16CompareExchangeStrong(Atomic16 *atomic16, int16_t *expected, int16_t desired,
                                          MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder)
{
    /* This implement the weak version with strong semantics. */
    return Atomic16CompareExchangeWeak(atomic16, expected, desired, successMemoryOrder, failureMemoryOrder);
}

/*
 * These functions perform the operation suggested by the name, and return the value that had previously
 * been in *atomic. That is,
 * { tmp = *atomic; *atomic op= value; return tmp; }
 * All memory models are valid.
 */
inline int16_t Atomic16FetchAdd(Atomic16 *atomic16, int16_t value, MemoryOrder memoryOrder)
{
    return (int16_t)_InterlockedExchangeAdd16(&(atomic16->atomicValue), value);
}

inline int16_t Atomic16FetchSub(Atomic16 *atomic16, int16_t value, MemoryOrder memoryOrder)
{
    /*
     * MSVC warns on negation of unsigned operands, but the semantics is right(MAX_TYPE + 1 - operand).
     */
    // clang-format off
    __pragma(warning(push))
    __pragma(warning(disable: MSVC_NEGATION_OPERANDS_WARN))
    return Atomic16FetchAdd(atomic16, -value, memoryOrder);
    __pragma(warning(pop))
    // clang-format on
}

inline int16_t Atomic16FetchAnd(Atomic16 *atomic16, int16_t value, MemoryOrder memoryOrder)
{
    return (int16_t)_InterlockedAnd16(&(atomic16->atomicValue), value);
}

inline int16_t Atomic16FetchOr(Atomic16 *atomic16, int16_t value, MemoryOrder memoryOrder)
{
    return (int16_t)_InterlockedOr16(&(atomic16->atomicValue), value);
}

inline int16_t Atomic16FetchXor(Atomic16 *atomic16, int16_t value, MemoryOrder memoryOrder)
{
    return (int16_t)_InterlockedXor16(&(atomic16->atomicValue), value);
}

/*
 * The following is an atomic operation function of type uint16_t.
 */
/*
 * This function implements an atomic get operation. It returns the contents of *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, MEMORY_ORDER_ACQUIRE, and MEMORY_ORDER_CONSUME.
 */
inline uint16_t AtomicU16Get(const AtomicU16 *atomic16, MemoryOrder memoryOrder)
{
    int16_t retValue = atomic16->atomicValue;
    if (memoryOrder != MEMORY_ORDER_RELAXED) {
        AtomicThreadFence(MEMORY_ORDER_ACQUIRE);
    }
    return (uint16_t)retValue;
}

/*
 * This function implements an atomic set operation. It writes value into *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, and MEMORY_ORDER_RELEASE.
 */
inline void AtomicU16Set(AtomicU16 *atomic16, uint16_t value, MemoryOrder memoryOrder)
{
    if (memoryOrder != MEMORY_ORDER_RELAXED) {
        AtomicThreadFence(MEMORY_ORDER_RELEASE);
    }
    atomic16->atomicValue = (int16_t)value;
    if (memoryOrder == MEMORY_ORDER_SEQ_CST) {
        AtomicThreadFence(MEMORY_ORDER_SEQ_CST);
    }
}

/*
 * This function implements an atomic exchange operation. It writes value into *atomic, and returns
 * the previous contents of *atomic.The valid memory model variants are MEMORY_ORDER_RELAXED,
 * MEMORY_ORDER_SEQ_CST, MEMORY_ORDER_ACQUIRE, MEMORY_ORDER_RELEASE, and MEMORY_ORDER_ACQ_REL.
 */
inline uint16_t AtomicU16Exchange(AtomicU16 *atomic16, uint16_t value, MemoryOrder memoryOrder)
{
    return (uint16_t)_InterlockedExchange16(&(atomic16->atomicValue), (int16_t)value);
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
inline bool AtomicU16CompareExchangeWeak(AtomicU16 *atomic16, uint16_t *expected, uint16_t desired,
                                         MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder)
{
    int16_t expectedValue = (int16_t)*expected;
    int16_t desiredValue = (int16_t)desired;
    int16_t old = _InterlockedCompareExchange16(&(atomic8->atomicValue), desiredValue, expectedValue);
    if (old == expectedValue) {
        return true;
    } else {
        *expected = (uint16_t)old;
        return false;
    }
}

inline bool AtomicU16CompareExchangeStrong(AtomicU16 *atomic16, uint16_t *expected, uint16_t desired,
                                           MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder)
{
    /* This implement the weak version with strong semantics. */
    return AtomicU16CompareExchangeWeak(atomic16, expected, desired, successMemoryOrder, failureMemoryOrder);
}

/*
 * These functions perform the operation suggested by the name, and return the value that had previously
 * been in *atomic. That is,
 * { tmp = *atomic; *atomic op= value; return tmp; }
 * All memory models are valid.
 */
inline uint16_t AtomicU16FetchAdd(AtomicU16 *atomic16, uint16_t value, MemoryOrder memoryOrder)
{
    return (uint16_t)_InterlockedExchangeAdd8(&(atomic16->atomicValue), (int16_t)value);
}

inline uint16_t AtomicU16FetchSub(AtomicU16 *atomic16, uint16_t value, MemoryOrder memoryOrder)
{
    /*
     * MSVC warns on negation of unsigned operands, but the semantics is right(MAX_TYPE + 1 - operand).
     */
    // clang-format off
    __pragma (warning(push))
    __pragma(warning(disable: MSVC_NEGATION_OPERANDS_WARN))
    return AtomicU16FetchAdd(atomic16, -value, memoryOrder);
    __pragma (warning(pop))
}

inline uint16_t AtomicU16FetchAnd(AtomicU16 *atomic16, uint16_t value, MemoryOrder memoryOrder)
{
    return (uint16_t)_InterlockedAnd16(&(atomic16->atomicValue), (int16_t)value);
}

inline uint16_t AtomicU16FetchOr(AtomicU16 *atomic16, uint16_t value, MemoryOrder memoryOrder)
{
    return (uint16_t)_InterlockedOr16(&(atomic16->atomicValue), (int16_t)value);
}

inline uint16_t AtomicU16FetchXor(AtomicU16 *atomic16, uint16_t value, MemoryOrder memoryOrder)
{
    return (uint16_t)_InterlockedXor16(&(atomic16->atomicValue), (int16_t)value);
}

/*
 * The following is an atomic operation function of type int32_t.
 */
/*
 * This function implements an atomic get operation. It returns the contents of *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, MEMORY_ORDER_ACQUIRE, and MEMORY_ORDER_CONSUME.
 */
inline int32_t Atomic32Get(const Atomic32 *atomic32, MemoryOrder memoryOrder)
{
    int32_t retValue = atomic32->atomicValue;
    if (memoryOrder != MEMORY_ORDER_RELAXED) {
        AtomicThreadFence(MEMORY_ORDER_ACQUIRE);
    }
    return retValue;
}

/*
 * This function implements an atomic set operation. It writes value into *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, and MEMORY_ORDER_RELEASE.
 */
inline void Atomic32Set(Atomic32 *atomic32, int32_t value, MemoryOrder memoryOrder)
{
    if (memoryOrder != MEMORY_ORDER_RELAXED) {
        AtomicThreadFence(MEMORY_ORDER_RELEASE);
    }
    atomic32->atomicValue = value;
    if (memoryOrder == MEMORY_ORDER_SEQ_CST) {
        AtomicThreadFence(MEMORY_ORDER_SEQ_CST);
    }
}

/*
 * This function implements an atomic exchange operation. It writes value into *atomic, and returns
 * the previous contents of *atomic.The valid memory model variants are MEMORY_ORDER_RELAXED,
 * MEMORY_ORDER_SEQ_CST, MEMORY_ORDER_ACQUIRE, MEMORY_ORDER_RELEASE, and MEMORY_ORDER_ACQ_REL.
 */
inline int32_t Atomic32Exchange(Atomic32 *atomic32, int32_t value, MemoryOrder memoryOrder)
{
    return (int32_t)_InterlockedExchange(&(atomic32->atomicValue), value);
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
inline bool Atomic32CompareExchangeWeak(Atomic32 *atomic32, int32_t *expected, int32_t desired,
                                        MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder)
{
    int32_t expectedValue = *expected;
    int32_t desiredValue = desired;
    int32_t old = _InterlockedCompareExchange(&(atomic32->atomicValue), desiredValue, expectedValue);
    if (old == expectedValue) {
        return true;
    } else {
        *expected = old;
        return false;
    }
}

inline bool Atomic32CompareExchangeStrong(Atomic32 *atomic32, int32_t *expected, int32_t desired,
                                          MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder)
{
    /* This implement the weak version with strong semantics. */
    return Atomic32CompareExchangeWeak(atomic32, expected, desired, successMemoryOrder, failureMemoryOrder);
}

/*
 * These functions perform the operation suggested by the name, and return the value that had previously
 * been in *atomic. That is,
 * { tmp = *atomic; *atomic op= value; return tmp; }
 * All memory models are valid.
 */
inline int32_t Atomic32FetchAdd(Atomic32 *atomic32, int32_t value, MemoryOrder memoryOrder)
{
    return (int32_t)_InterlockedExchangeAdd(&(atomic32->atomicValue), value);
}

inline int32_t Atomic32FetchSub(Atomic32 *atomic32, int32_t value, MemoryOrder memoryOrder)
{
    /*
     * MSVC warns on negation of unsigned operands, but the semantics is right(MAX_TYPE + 1 - operand).
     */
    // clang-format off
    __pragma(warning(push))
    __pragma(warning(disable: MSVC_NEGATION_OPERANDS_WARN))
    return Atomic32FetchAdd(atomic32, -value, memoryOrder);
    __pragma(warning(pop))
    // clang-format on
}

inline int32_t Atomic32FetchAnd(Atomic32 *atomic32, int32_t value, MemoryOrder memoryOrder)
{
    return (int32_t)_InterlockedAnd(&(atomic32->atomicValue), value);
}

inline int32_t Atomic32FetchOr(Atomic32 *atomic32, int32_t value, MemoryOrder memoryOrder)
{
    return (int32_t)_InterlockedOr(&(atomic32->atomicValue), value);
}

inline int32_t Atomic32FetchXor(Atomic32 *atomic32, int32_t value, MemoryOrder memoryOrder)
{
    return (int32_t)_InterlockedXor(&(atomic32->atomicValue), value);
}

/*
 * The following is an atomic operation function of type uint32_t.
 */
/*
 * This function implements an atomic get operation. It returns the contents of *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, MEMORY_ORDER_ACQUIRE, and MEMORY_ORDER_CONSUME.
 */
inline uint32_t AtomicU32Get(const AtomicU32 *atomic32, MemoryOrder memoryOrder)
{
    int32_t retValue = atomic32->atomicValue;
    if (memoryOrder != MEMORY_ORDER_RELAXED) {
        AtomicThreadFence(MEMORY_ORDER_ACQUIRE);
    }
    return (uint32_t)retValue;
}

/*
 * This function implements an atomic set operation. It writes value into *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, and MEMORY_ORDER_RELEASE.
 */
inline void AtomicU32Set(AtomicU32 *atomic32, uint32_t value, MemoryOrder memoryOrder)
{
    if (memoryOrder != MEMORY_ORDER_RELAXED) {
        AtomicThreadFence(MEMORY_ORDER_RELEASE);
    }
    atomic32->atomicValue = (int32_t)value;
    if (memoryOrder == MEMORY_ORDER_SEQ_CST) {
        AtomicThreadFence(MEMORY_ORDER_SEQ_CST);
    }
}

/*
 * This function implements an atomic exchange operation. It writes value into *atomic, and returns
 * the previous contents of *atomic.The valid memory model variants are MEMORY_ORDER_RELAXED,
 * MEMORY_ORDER_SEQ_CST, MEMORY_ORDER_ACQUIRE, MEMORY_ORDER_RELEASE, and MEMORY_ORDER_ACQ_REL.
 */
inline uint32_t AtomicU32Exchange(AtomicU32 *atomic32, uint32_t value, MemoryOrder memoryOrder)
{
    return (uint32_t)_InterlockedExchange(&(atomic32->atomicValue), (int32_t)value);
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
inline bool AtomicU32CompareExchangeWeak(AtomicU32 *atomic32, uint32_t *expected, uint32_t desired,
                                         MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder)
{
    int32_t expectedValue = (int32_t)*expected;
    int32_t desiredValue = (int32_t)desired;
    int32_t old = _InterlockedCompareExchange(&(atomic32->atomicValue), desiredValue, expectedValue);
    if (old == expectedValue) {
        return true;
    } else {
        *expected = (uint32_t)old;
        return false;
    }
}

inline bool AtomicU32CompareExchangeStrong(AtomicU32 *atomic32, uint32_t *expected, uint32_t desired,
                                           MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder)
{
    /* This implement the weak version with strong semantics. */
    return AtomicU32CompareExchangeWeak(atomic32, expected, desired, successMemoryOrder, failureMemoryOrder);
}

/*
 * These functions perform the operation suggested by the name, and return the value that had previously
 * been in *atomic. That is,
 * { tmp = *atomic; *atomic op= value; return tmp; }
 * All memory models are valid.
 */
inline uint32_t AtomicU32FetchAdd(AtomicU32 *atomic32, uint32_t value, MemoryOrder memoryOrder)
{
    return (uint32_t)_InterlockedExchangeAdd(&(atomic32->atomicValue), (int32_t)value);
}

inline uint32_t AtomicU32FetchSub(AtomicU32 *atomic32, uint32_t value, MemoryOrder memoryOrder)
{
    /*
     * MSVC warns on negation of unsigned operands, but the semantics is right(MAX_TYPE + 1 - operand).
     */
    // clang-format off
    __pragma (warning(push))
    __pragma(warning(disable: MSVC_NEGATION_OPERANDS_WARN))
    return AtomicU32FetchAdd(atomic32, -value, memoryOrder);
    __pragma (warning(pop))
}

inline uint32_t AtomicU32FetchAnd(AtomicU32 *atomic32, uint32_t value, MemoryOrder memoryOrder)
{
    return (uint32_t)_InterlockedAnd(&(atomic32->atomicValue), (int32_t)value);
}

inline uint32_t AtomicU32FetchOr(AtomicU32 *atomic32, uint32_t value, MemoryOrder memoryOrder)
{
    return (uint32_t)_InterlockedOr(&(atomic32->atomicValue), (int32_t)value);
}

inline uint32_t AtomicU32FetchXor(AtomicU32 *atomic32, uint32_t value, MemoryOrder memoryOrder)
{
    return (uint32_t)_InterlockedXor(&(atomic32->atomicValue), (int32_t)value);
}

/*
 * The following is an atomic operation function of type int64_t.
 */
/*
 * This function implements an atomic get operation. It returns the contents of *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, MEMORY_ORDER_ACQUIRE, and MEMORY_ORDER_CONSUME.
 */
inline int64_t Atomic64Get(const Atomic64 *atomic64, MemoryOrder memoryOrder)
{
    int64_t retValue = atomic64->atomicValue;
    if (memoryOrder != MEMORY_ORDER_RELAXED) {
        AtomicThreadFence(MEMORY_ORDER_ACQUIRE);
    }
    return retValue;
}

/*
 * This function implements an atomic set operation. It writes value into *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, and MEMORY_ORDER_RELEASE.
 */
inline void Atomic64Set(Atomic64 *atomic64, int64_t value, MemoryOrder memoryOrder)
{
    if (memoryOrder != MEMORY_ORDER_RELAXED) {
        AtomicThreadFence(MEMORY_ORDER_RELEASE);
    }
    atomic64->atomicValue = value;
    if (memoryOrder == MEMORY_ORDER_SEQ_CST) {
        AtomicThreadFence(MEMORY_ORDER_SEQ_CST);
    }
}

/*
 * This function implements an atomic exchange operation. It writes value into *atomic, and returns
 * the previous contents of *atomic.The valid memory model variants are MEMORY_ORDER_RELAXED,
 * MEMORY_ORDER_SEQ_CST, MEMORY_ORDER_ACQUIRE, MEMORY_ORDER_RELEASE, and MEMORY_ORDER_ACQ_REL.
 */
inline int64_t Atomic64Exchange(Atomic64 *atomic64, int64_t value, MemoryOrder memoryOrder)
{
    return (int64_t)_InterlockedExchange64(&(atomic64->atomicValue), value);
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
inline bool Atomic64CompareExchangeWeak(Atomic64 *atomic64, int64_t *expected, int64_t desired,
                                        MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder)
{
    int64_t expectedValue = *expected;
    int64_t desiredValue = desired;
    int64_t old = _InterlockedCompareExchange64(&(atomic64->atomicValue), desiredValue, expectedValue);
    if (old == expectedValue) {
        return true;
    } else {
        *expected = old;
        return false;
    }
}

inline bool Atomic64CompareExchangeStrong(Atomic64 *atomic64, int64_t *expected, int64_t desired,
                                          MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder)
{
    /* This implement the weak version with strong semantics. */
    return Atomic64CompareExchangeWeak(atomic64, expected, desired, successMemoryOrder, failureMemoryOrder);
}

/*
 * These functions perform the operation suggested by the name, and return the value that had previously
 * been in *atomic. That is,
 * { tmp = *atomic; *atomic op= value; return tmp; }
 * All memory models are valid.
 */
inline int64_t Atomic64FetchAdd(Atomic64 *atomic64, int64_t value, MemoryOrder memoryOrder)
{
    return (int64_t)_InterlockedExchangeAdd64(&(atomic64->atomicValue), value);
}

inline int64_t Atomic64FetchSub(Atomic64 *atomic64, int64_t value, MemoryOrder memoryOrder)
{
    /*
     * MSVC warns on negation of unsigned operands, but the semantics is right(MAX_TYPE + 1 - operand).
     */
    // clang-format off
    __pragma(warning(push))
    __pragma(warning(disable: MSVC_NEGATION_OPERANDS_WARN))
    return Atomic64FetchAdd(atomic64, -value, memoryOrder);
    __pragma(warning(pop))
    // clang-format on
}

inline int64_t Atomic64FetchAnd(Atomic64 *atomic64, int64_t value, MemoryOrder memoryOrder)
{
    return (int64_t)_InterlockedAnd64(&(atomic64->atomicValue), value);
}

inline int64_t Atomic64FetchOr(Atomic64 *atomic64, int64_t value, MemoryOrder memoryOrder)
{
    return (int64_t)_InterlockedOr64(&(atomic64->atomicValue), value);
}

inline int64_t Atomic64FetchXor(Atomic64 *atomic64, int64_t value, MemoryOrder memoryOrder)
{
    return (int64_t)_InterlockedXor64(&(atomic64->atomicValue), value);
}

/*
 * The following is an atomic operation function of type uint64_t.
 */
/*
 * This function implements an atomic get operation. It returns the contents of *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, MEMORY_ORDER_ACQUIRE, and MEMORY_ORDER_CONSUME.
 */
inline uint64_t AtomicU64Get(const AtomicU64 *atomic64, MemoryOrder memoryOrder)
{
    int64_t retValue = atomic64->atomicValue;
    if (memoryOrder != MEMORY_ORDER_RELAXED) {
        AtomicThreadFence(MEMORY_ORDER_ACQUIRE);
    }
    return (uint64_t)retValue;
}

/*
 * This function implements an atomic set operation. It writes value into *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, and MEMORY_ORDER_RELEASE.
 */
inline void AtomicU64Set(AtomicU64 *atomic64, uint64_t value, MemoryOrder memoryOrder)
{
    if (memoryOrder != MEMORY_ORDER_RELAXED) {
        AtomicThreadFence(MEMORY_ORDER_RELEASE);
    }
    atomic64->atomicValue = (int64_t)value;
    if (memoryOrder == MEMORY_ORDER_SEQ_CST) {
        AtomicThreadFence(MEMORY_ORDER_SEQ_CST);
    }
}

/*
 * This function implements an atomic exchange operation. It writes value into *atomic, and returns
 * the previous contents of *atomic.The valid memory model variants are MEMORY_ORDER_RELAXED,
 * MEMORY_ORDER_SEQ_CST, MEMORY_ORDER_ACQUIRE, MEMORY_ORDER_RELEASE, and MEMORY_ORDER_ACQ_REL.
 */
inline uint64_t AtomicU64Exchange(AtomicU64 *atomic64, uint64_t value, MemoryOrder memoryOrder)
{
    return (uint64_t)_InterlockedExchange64(&(atomic64->atomicValue), (int64_t)value);
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
inline bool AtomicU64CompareExchangeWeak(AtomicU64 *atomic64, uint64_t *expected, uint64_t desired,
                                         MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder)
{
    int64_t expectedValue = (int64_t)*expected;
    int64_t desiredValue = (int64_t)desired;
    int64_t old = _InterlockedCompareExchange64(&(atomic64->atomicValue), desiredValue, expectedValue);
    if (old == expectedValue) {
        return true;
    } else {
        *expected = (uint64_t)old;
        return false;
    }
}

inline bool AtomicU64CompareExchangeStrong(AtomicU64 *atomic64, uint64_t *expected, uint64_t desired,
                                           MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder)
{
    /* This implement the weak version with strong semantics. */
    return AtomicU64CompareExchangeWeak(atomic64, expected, desired, successMemoryOrder, failureMemoryOrder);
}

/*
 * These functions perform the operation suggested by the name, and return the value that had previously
 * been in *atomic. That is,
 * { tmp = *atomic; *atomic op= value; return tmp; }
 * All memory models are valid.
 */
inline uint64_t AtomicU64FetchAdd(AtomicU64 *atomic64, uint64_t value, MemoryOrder memoryOrder)
{
    return (uint64_t)_InterlockedExchangeAdd64(&(atomic64->atomicValue), (int64_t)value);
}

inline uint64_t AtomicU64FetchSub(AtomicU64 *atomic64, uint64_t value, MemoryOrder memoryOrder)
{
    /*
     * MSVC warns on negation of unsigned operands, but the semantics is right(MAX_TYPE + 1 - operand).
     */
    // clang-format off
    __pragma (warning(push))
    __pragma(warning(disable: MSVC_NEGATION_OPERANDS_WARN))
    return AtomicU64FetchAdd(atomic64, -value, memoryOrder);
    __pragma (warning(pop))
}

inline uint64_t AtomicU64FetchAnd(AtomicU64 *atomic64, uint64_t value, MemoryOrder memoryOrder)
{
    return (uint64_t)_InterlockedAnd64(&(atomic64->atomicValue), (int64_t)value);
}

inline uint64_t AtomicU64FetchOr(AtomicU64 *atomic64, uint64_t value, MemoryOrder memoryOrder)
{
    return (uint64_t)_InterlockedOr64(&(atomic64->atomicValue), (int64_t)value);
}

inline uint64_t AtomicU64FetchXor(AtomicU64 *atomic64, uint64_t value, MemoryOrder memoryOrder)
{
    return (uint64_t)_InterlockedXor64(&(atomic64->atomicValue), (int64_t)value);
}

/*
 * The following is an atomic operation function of type ssize_t.
 */
/*
 * This function implements an atomic get operation. It returns the contents of *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, MEMORY_ORDER_ACQUIRE, and MEMORY_ORDER_CONSUME.
 */
inline ssize_t AtomicSsizeGet(const AtomicSsize *atomicSsize, MemoryOrder memoryOrder)
{
    ssize_t retValue = atomicSsize->atomicValue;
    if (memoryOrder != MEMORY_ORDER_RELAXED) {
        AtomicThreadFence(MEMORY_ORDER_ACQUIRE);
    }
    return retValue;
}

/*
 * This function implements an atomic set operation. It writes value into *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, and MEMORY_ORDER_RELEASE.
 */
inline void AtomicSsizeSet(AtomicSsize *atomicSsize, ssize_t value, MemoryOrder memoryOrder)
{
    if (memoryOrder != MEMORY_ORDER_RELAXED) {
        AtomicThreadFence(MEMORY_ORDER_RELEASE);
    }
    atomicSsize->atomicValue = value;
    if (memoryOrder == MEMORY_ORDER_SEQ_CST) {
        AtomicThreadFence(MEMORY_ORDER_SEQ_CST);
    }
}

/*
 * This function implements an atomic exchange operation. It writes value into *atomic, and returns
 * the previous contents of *atomic.The valid memory model variants are MEMORY_ORDER_RELAXED,
 * MEMORY_ORDER_SEQ_CST, MEMORY_ORDER_ACQUIRE, MEMORY_ORDER_RELEASE, and MEMORY_ORDER_ACQ_REL.
 */
inline ssize_t AtomicSsizeExchange(AtomicSsize *atomicSsize, ssize_t value, MemoryOrder memoryOrder)
{
#if CURRENT_COMPILE_MODE == COMPILE_MODE_32_BIT
    return (ssize_t)_InterlockedExchange(&(atomicSsize->atomicValue), value);
#else
    return (ssize_t)_InterlockedExchange64(&(atomicSsize->atomicValue), value);
#endif
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
inline bool AtomicSsizeCompareExchangeWeak(AtomicSsize *atomicSsize, ssize_t *expected, ssize_t desired,
                                           MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder)
{
    ssize_t expectedValue = *expected;
    ssize_t desiredValue = desired;
#if CURRENT_COMPILE_MODE == COMPILE_MODE_32_BIT
    ssize_t old = _InterlockedCompareExchange(&(atomicSsize->atomicValue), desiredValue, expectedValue);
#else
    ssize_t old = _InterlockedCompareExchange64(&(atomicSsize->atomicValue), desiredValue, expectedValue);
#endif
    if (old == expectedValue) {
        return true;
    } else {
        *expected = old;
        return false;
    }
}

inline bool AtomicSSizeCompareExchangeStrong(AtomicSsize *atomicSsize, ssize_t *expected, ssize_t desired,
                                             MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder)
{
    /* This implement the weak version with strong semantics. */
    return AtomicSsizeCompareExchangeWeak(atomicSsize, expected, desired, successMemoryOrder, failureMemoryOrder);
}

/*
 * These functions perform the operation suggested by the name, and return the value that had previously
 * been in *atomic. That is,
 * { tmp = *atomic; *atomic op= value; return tmp; }
 * All memory models are valid.
 */
inline ssize_t AtomicSsizeFetchAdd(AtomicSsize *atomicSsize, ssize_t value, MemoryOrder memoryOrder)
{
#if CURRENT_COMPILE_MODE == COMPILE_MODE_32_BIT
        return (ssize_t)_InterlockedExchangeAdd(&(atomicSsize->atomicValue), value);
#else
        return (ssize_t)_InterlockedExchangeAdd64(&(atomicSsize->atomicValue), value);
#endif
}

inline ssize_t AtomicSsizeFetchSub(AtomicSsize *atomicSsize, ssize_t value, MemoryOrder memoryOrder)
{
    /*
     * MSVC warns on negation of unsigned operands, but the semantics is right(MAX_TYPE + 1 - operand).
     */
    // clang-format off
    __pragma(warning(push))
    __pragma(warning(disable: MSVC_NEGATION_OPERANDS_WARN))
    return AtomicSsizeFetchAdd(atomicSsize, -value, memoryOrder);
    __pragma(warning(pop))
    // clang-format on
}

inline ssize_t AtomicSsizeFetchAnd(AtomicSsize *atomicSsize, ssize_t value, MemoryOrder memoryOrder)
{
#if CURRENT_COMPILE_MODE == COMPILE_MODE_32_BIT
    return (ssize_t)_InterlockedAnd(&(atomicSsize->atomicValue), value);
#else
    return (ssize_t)_InterlockedAnd64(&(atomicSsize->atomicValue), value);
#endif
}

inline ssize_t AtomicSsizeFetchOr(AtomicSsize *atomicSsize, ssize_t value, MemoryOrder memoryOrder)
{
#if CURRENT_COMPILE_MODE == COMPILE_MODE_32_BIT
    return (ssize_t)_InterlockedOr(&(atomicSsize->atomicValue), value);
#else
    return (ssize_t)_InterlockedOr64(&(atomicSsize->atomicValue), value);
#endif
}

inline ssize_t AtomicSsizeFetchXor(AtomicSsize *atomicSsize, ssize_t value, MemoryOrder memoryOrder)
{
#if CURRENT_COMPILE_MODE == COMPILE_MODE_32_BIT
    return (ssize_t)_InterlockedXor(&(atomicSsize->atomicValue), value);
#else
    return (ssize_t)_InterlockedXor64(&(atomicSsize->atomicValue), value);
#endif
}

/*
 * The following is an atomic operation function of type size_t.
 */
/*
 * This function implements an atomic get operation. It returns the contents of *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, MEMORY_ORDER_ACQUIRE, and MEMORY_ORDER_CONSUME.
 */
inline size_t AtomicSizeGet(const AtomicSize *atomicSize, MemoryOrder memoryOrder)
{
    ssize_t retValue = atomicSize->atomicValue;
    if (memoryOrder != MEMORY_ORDER_RELAXED) {
        AtomicThreadFence(MEMORY_ORDER_ACQUIRE);
    }
    return (size_t)retValue;
}

/*
 * This function implements an atomic set operation. It writes value into *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, and MEMORY_ORDER_RELEASE.
 */
inline void AtomicSizeSet(AtomicSize *atomicSize, size_t value, MemoryOrder memoryOrder)
{
    if (memoryOrder != MEMORY_ORDER_RELAXED) {
        AtomicThreadFence(MEMORY_ORDER_RELEASE);
    }
    atomicSize->atomicValue = (ssize_t)value;
    if (memoryOrder == MEMORY_ORDER_SEQ_CST) {
        AtomicThreadFence(MEMORY_ORDER_SEQ_CST);
    }
}

/*
 * This function implements an atomic exchange operation. It writes value into *atomic, and returns
 * the previous contents of *atomic.The valid memory model variants are MEMORY_ORDER_RELAXED,
 * MEMORY_ORDER_SEQ_CST, MEMORY_ORDER_ACQUIRE, MEMORY_ORDER_RELEASE, and MEMORY_ORDER_ACQ_REL.
 */
inline size_t AtomicSizeExchange(AtomicSize *atomicSize, size_t value, MemoryOrder memoryOrder)
{
#if CURRENT_COMPILE_MODE == COMPILE_MODE_32_BIT
    return (size_t)_InterlockedExchange(&(atomicSize->atomicValue), (ssize_t)value);
#else
    return (size_t)_InterlockedExchange64(&(atomicSize->atomicValue), (ssize_t)value);
#endif
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
inline bool AtomicSizeCompareExchangeWeak(AtomicSize *atomicSize, size_t *expected, size_t desired,
                                          MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder)
{
    ssize_t expectedValue = (ssize_t)*expected;
    ssize_t desiredValue = (ssize_t)desired;
#if CURRENT_COMPILE_MODE == COMPILE_MODE_32_BIT
    ssize_t old = _InterlockedCompareExchange(&(atomicSize->atomicValue), desiredValue, expectedValue);
#else
    ssize_t old = _InterlockedCompareExchange64(&(atomicSize->atomicValue), desiredValue, expectedValue);
#endif
    if (old == expectedValue) {
        return true;
    } else {
        *expected = (size_t)old;
        return false;
    }
}

inline bool AtomicSizeCompareExchangeStrong(AtomicSize *atomicSize, size_t *expected, size_t desired,
                                            MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder)
{
    /* This implement the weak version with strong semantics. */
    return AtomicSizeCompareExchangeWeak(atomicSize, expected, desired, successMemoryOrder, failureMemoryOrder);
}

/*
 * These functions perform the operation suggested by the name, and return the value that had previously
 * been in *atomic. That is,
 * { tmp = *atomic; *atomic op= value; return tmp; }
 * All memory models are valid.
 */
inline size_t AtomicSizeFetchAdd(AtomicSize *atomicSize, size_t value, MemoryOrder memoryOrder)
{
#if CURRENT_COMPILE_MODE == COMPILE_MODE_32_BIT
    return (size_t)_InterlockedExchangeAdd(&(atomicSize->atomicValue), (ssize_t)value);
#else
    return (size_t)_InterlockedExchangeAdd64(&(atomicSize->atomicValue), (ssize_t)value);
#endif
}

inline size_t AtomicSizeFetchSub(AtomicSize *atomicSize, size_t value, MemoryOrder memoryOrder)
{
    /*
     * MSVC warns on negation of unsigned operands, but the semantics is right(MAX_TYPE + 1 - operand).
     */
    // clang-format off
    __pragma (warning(push))
    __pragma(warning(disable: MSVC_NEGATION_OPERANDS_WARN))
    return AtomicSizeFetchAdd(atomicSize, -value, memoryOrder);
    __pragma (warning(pop))
}

inline size_t AtomicSizeFetchAnd(AtomicSize *atomicSize, size_t value, MemoryOrder memoryOrder)
{
#if CURRENT_COMPILE_MODE == COMPILE_MODE_32_BIT
    return (size_t)_InterlockedAnd(&(atomicSize->atomicValue), (ssize_t)value);
#else
    return (size_t)_InterlockedAnd64(&(atomicSize->atomicValue), (ssize_t)value);
#endif
}

inline size_t AtomicSizeFetchOr(AtomicSize *atomicSize, size_t value, MemoryOrder memoryOrder)
{
#if CURRENT_COMPILE_MODE == COMPILE_MODE_32_BIT
    return (size_t)_InterlockedOr(&(atomicSize->atomicValue), (ssize_t)value);
#else
    return (size_t)_InterlockedOr64(&(atomicSize->atomicValue), (ssize_t)value);
#endif
}

inline size_t AtomicSizeFetchXor(AtomicSize *atomicSize, size_t value, MemoryOrder memoryOrder)
{
#if CURRENT_COMPILE_MODE == COMPILE_MODE_32_BIT
    return (size_t)_InterlockedXor(&(atomicSize->atomicValue), (ssize_t)value);
#else
    return (size_t)_InterlockedXor64(&(atomicSize->atomicValue), (ssize_t)value);
#endif
}

/*
 * The following is an atomic operation function of type void *.
 */
/*
 * This function implements an atomic get operation. It returns the contents of *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, MEMORY_ORDER_ACQUIRE, and MEMORY_ORDER_CONSUME.
 */
inline void *AtomicPointerGet(const AtomicPointer *atomicPointer, MemoryOrder memoryOrder)
{
#if CURRENT_COMPILE_MODE == COMPILE_MODE_32_BIT
    int32_t retValue = atomicPointer->atomicValue;
#else
    int64_t retValue = atomicPointer->atomicValue;
#endif
    if (memoryOrder != MEMORY_ORDER_RELAXED) {
        AtomicThreadFence(MEMORY_ORDER_ACQUIRE);
    }
    return (void *)retValue;
}

/*
 * This function implements an atomic set operation. It writes value into *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, and MEMORY_ORDER_RELEASE.
 */
inline void AtomicPointerSet(AtomicPointer *atomicPointer, void *value, MemoryOrder memoryOrder)
{
    if (memoryOrder != MEMORY_ORDER_RELAXED) {
        AtomicThreadFence(MEMORY_ORDER_RELEASE);
    }
#if CURRENT_COMPILE_MODE == COMPILE_MODE_32_BIT
    atomicPointer->atomicValue = (int32_t)value;
#else
    atomicPointer->atomicValue = (int64_t)value;
#endif
    if (memoryOrder == MEMORY_ORDER_SEQ_CST) {
        AtomicThreadFence(MEMORY_ORDER_SEQ_CST);
    }
}

/*
 * This function implements an atomic exchange operation. It writes value into *atomic, and returns
 * the previous contents of *atomic.The valid memory model variants are MEMORY_ORDER_RELAXED,
 * MEMORY_ORDER_SEQ_CST, MEMORY_ORDER_ACQUIRE, MEMORY_ORDER_RELEASE, and MEMORY_ORDER_ACQ_REL.
 */
inline void *AtomicPointerExchange(AtomicPointer *atomicPointer, void *value, MemoryOrder memoryOrder)
{
#if CURRENT_COMPILE_MODE == COMPILE_MODE_32_BIT
    return (void *)_InterlockedExchange(&(atomicPointer->atomicValue), (int32_t)value);
#else
    return (void *)_InterlockedExchange64(&(atomicPointer->atomicValue), (int64_t)value);
#endif
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
inline bool AtomicPointerCompareExchangeWeak(AtomicPointer *atomicPointer, void **expected, void *desired,
                                             MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder)
{
    void *expectedValue = *expected;
    void *desiredValue = desired;
#if CURRENT_COMPILE_MODE == COMPILE_MODE_32_BIT
    void *old = (void *)_InterlockedCompareExchange(&(atomicPointer->atomicValue),
        (int32_t)desiredValue, (int32_t)expectedValue);
#else
    void *old = (void *)_InterlockedCompareExchange64(&(atomicPointer->atomicValue),
        (int64_t)desiredValue, (int64_t)expectedValue);
#endif
    if (old == expectedValue) {
        return true;
    } else {
        *expected = old;
        return false;
    }
}

inline bool AtomicPointerCompareExchangeStrong(AtomicPointer *atomicPointer, void **expected, void *desired,
                                               MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder)
{
    /* This implement the weak version with strong semantics. */
    return AtomicPointerCompareExchangeWeak(atomicPointer, expected, desired, successMemoryOrder, failureMemoryOrder);
}

/*
 * The following is an atomic operation function of type bool.
 */
/*
 * This function implements an atomic get operation. It returns the contents of *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, MEMORY_ORDER_ACQUIRE, and MEMORY_ORDER_CONSUME.
 */
inline bool AtomicBoolGet(const AtomicBool *atomicBool, MemoryOrder memoryOrder)
{
    int8_t retValue = atomicBool->atomicValue;
    if (memoryOrder != MEMORY_ORDER_RELAXED) {
        AtomicThreadFence(MEMORY_ORDER_ACQUIRE);
    }
    return (retValue != 0 ? true : false);
}

/*
 * This function implements an atomic set operation. It writes value into *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, and MEMORY_ORDER_RELEASE.
 */
inline void AtomicBoolSet(AtomicBool *atomicBool, bool value, MemoryOrder memoryOrder)
{
    if (memoryOrder != MEMORY_ORDER_RELAXED) {
        AtomicThreadFence(MEMORY_ORDER_RELEASE);
    }
    atomicBool->atomicValue = (value ? 1 : 0);
    if (memoryOrder == MEMORY_ORDER_SEQ_CST) {
        AtomicThreadFence(MEMORY_ORDER_SEQ_CST);
    }
}

/*
 * This function implements an atomic exchange operation. It writes value into *atomic, and returns
 * the previous contents of *atomic.The valid memory model variants are MEMORY_ORDER_RELAXED,
 * MEMORY_ORDER_SEQ_CST, MEMORY_ORDER_ACQUIRE, MEMORY_ORDER_RELEASE, and MEMORY_ORDER_ACQ_REL.
 */
inline bool AtomicBoolExchange(AtomicBool *atomicBool, bool value, MemoryOrder memoryOrder)
{
    int8_t retValue = _InterlockedExchange8(&(atomicBool->atomicValue), (value ? 1 : 0));
    return (retValue != 0 ? true : false);
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
inline bool AtomicBoolCompareExchangeWeak(AtomicBool *atomicBool, bool *expected, bool desired,
                                          MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder)
{
    int8_t expectedValue = (*expected ? 1 : 0);
    int8_t desiredValue = (desired ? 1 : 0);
    int8_t old = _InterlockedCompareExchange8(&(atomicBool->atomicValue), desiredValue, expectedValue);
    if (old == expectedValue) {
        return true;
    } else {
        *expected = (old != 0 ? true : false);
        return false;
    }
}

inline bool AtomicBoolCompareExchangeStrong(AtomicBool *atomicBool, bool *expected, bool desired,
                                            MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder)
{
    /* This implement the weak version with strong semantics. */
    return AtomicBoolCompareExchangeWeak(atomicBool, expected, desired, successMemoryOrder, failureMemoryOrder);
}

GSDB_END_C_CODE_DECLS

#endif /* UTILS_WIN32_ATOMIC_H */
