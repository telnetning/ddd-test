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
 * -------------------------------------------------------------------------
 *
 * dstore_atomic.h
 *
 * IDENTIFICATION
 *    src/include/utils/atomic.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef DSTORE_ATOMIC_H
#define DSTORE_ATOMIC_H

#include "common/dstore_datatype.h"
#include "common/concurrent/dstore_barrier.h"
#include "common/concurrent/dstore_atomic_arm.h"

namespace DSTORE {

enum class MemoryOrder : int {
    /* No barriers or synchronization. */
    MEMORY_ORDER_RELAXED = __ATOMIC_RELAXED,
    /* Data dependency only for both barrier and synchronization with another thread. */
    MEMORY_ORDER_CONSUME = __ATOMIC_CONSUME,
    /* Barrier to hoisting of code and synchronizes with release (or stronger) semantic stores from another thread. */
    MEMORY_ORDER_ACQUIRE = __ATOMIC_ACQUIRE,
    /* Barrier to sinking of code and synchronizes with acquire (or stronger) semantic loads from another thread. */
    MEMORY_ORDER_RELEASE = __ATOMIC_RELEASE,
    /* Full barrier in both directions and synchronizes with acquire loads and release stores in another thread. */
    MEMORY_ORDER_ACQ_REL = __ATOMIC_ACQ_REL,
    /* Full barrier in both directions and synchronizes with acquire loads and release stores in all threads. */
    MEMORY_ORDER_SEQ_CST = __ATOMIC_SEQ_CST
};

typedef volatile uint32 gs_atomic_uint32;
typedef volatile uint64 gs_atomic_uint64;
typedef volatile uint128_u gs_atomic_uint128;

#ifdef __aarch64__

/*
 * alternative atomic operations of __sync_val_compare_and_swap for 32bits integers
 * */
static inline uint32 __lse_compare_and_swap_u32(volatile uint32 *ptr, uint32 oldval, uint32 newval)
{
    register unsigned long x0 asm ("x0") = (unsigned long)ptr;                  \
    register uint32 x1 asm ("x1") = oldval;                                     \
    register uint32 x2 asm ("x2") = newval;                                     \
                                                                                \
    asm volatile(".arch_extension lse\n"                                        \
        "       mov     w30, %w[oldval]\n"                                      \
        "       casal  w30, %w[newval], %[v]\n"                                 \
        "       mov    %w[ret], w30\n"                                          \
        : [ret] "+r" (x0), [v] "+Q" (*(ptr))                                    \
        : [oldval] "r" (x1), [newval] "r" (x2)                                  \
        : "x16", "x17", "x30", "memory");                                       \
    return x0;                                                                  \
}

/*
 * alternative atomic operations of __sync_val_compare_and_swap for 64bits integers
 * */
static inline uint64 __lse_compare_and_swap_u64(volatile uint64 *ptr, uint64 oldval, uint64 newval)
{
    register unsigned long x0 asm ("x0") = (unsigned long)ptr;                  \
    register uint64 x1 asm ("x1") = oldval;                                     \
    register uint64 x2 asm ("x2") = newval;                                     \
                                                                                \
    asm volatile(".arch_extension lse\n"                                        \
        "       mov     x30, %x[oldval]\n"                                      \
        "       casal  x30, %x[newval], %[v]\n"                                 \
        "       mov    %x[ret], x30"                                            \
        : [ret] "+r" (x0), [v] "+Q" (*ptr)                                      \
        : [oldval] "r" (x1), [newval] "r" (x2)                                  \
        : "x16", "x17", "x30", "memory");                                       \
    return x0;                                                                  \
}


/*
 * alternative atomic operations of __sync_fetch_and_and for 32bits integers
 * */
static inline uint32 __lse_atomic_fetch_and_u32(volatile uint32 *ptr, uint32 val)
{
    register uint32 w0 asm ("w0") = val;                                        \
    register uint32 *x1 asm ("x1") = (uint32 *)(unsigned long)ptr;              \
                                                                                \
    asm volatile(".arch_extension lse\n"                                        \
        "       mvn     %w[val], %w[val]\n"                                     \
        "       ldclral  %w[val], %w[val], %[v]\n"                              \
        : [val] "+&r" (w0), [v] "+Q" (*ptr)                                     \
        : "r" (x1)                                                              \
        : "x16", "x17", "x30", "memory");                                       \
    return w0;                                                                  \
}

/*
 * alternative atomic operations of __sync_fetch_and_and for 64bits integers
 * */
static inline uint64 __lse_atomic_fetch_and_u64(volatile uint64 *ptr, uint64 val)
{
    register uint64 x0 asm ("w0") = val;                                        \
    register uint64 *x1 asm ("x1") = (uint64 *)(unsigned long)ptr;              \
                                                                                \
    asm volatile(".arch_extension lse\n"                                        \
        "       mvn     %[val], %[val]\n"                                       \
        "       ldclral  %[val], %[val], %[v]\n"                                \
        : [val] "+&r" (x0), [v] "+Q" (*ptr)                                     \
        : "r" (x1)                                                              \
        : "x16", "x17", "x30", "memory");                                       \
    return x0;                                                                  \
}


/*
 * alternative atomic operations of __sync_fetch_and_add for 32bits integers
 * */
static inline uint32 __lse_atomic_fetch_add_u32(volatile uint32 *ptr, uint32 val)
{
    register uint32 w0 asm ("w0") = val;                                        \
    register uint32 *x1 asm ("x1") = (uint32 *)(unsigned long)ptr;              \
                                                                                \
    asm volatile(".arch_extension lse\n"                                        \
        "       ldaddal  %w[val], %w[val], %[v]\n"                              \
        : [val] "+r" (w0), [v] "+Q" (*ptr)                                      \
        : "r" (x1)                                                              \
        : "x16", "x17", "x30", "memory");                                       \
    return w0;                                                                  \
}


/*
 * alternative atomic operations of __sync_fetch_and_add for 64bits integers
 * */
static inline uint64 __lse_atomic_fetch_add_u64(volatile uint64 *ptr, uint64 val)
{
    register uint64 x0 asm ("w0") = val;                                        \
    register uint64 *x1 asm ("x1") = (uint64 *)(unsigned long)ptr;              \
                                                                                \
    asm volatile(".arch_extension lse\n"                                        \
        "       ldaddal  %[val], %[val], %[v]\n"                                \
        : [val] "+r" (x0), [v] "+Q" (*ptr)                                      \
        : "r" (x1)                                                              \
        : "x16", "x17", "x30", "memory");                                       \
    return x0;                                                                  \
}

/*
 * alternative atomic operations of __sync_fetch_and_or for 32bits integers
 * */
static inline uint32 __lse_atomic_fetch_or_u32(volatile uint32 *ptr, uint32 val)
{
    register uint32 w0 asm ("w0") = val;                                        \
    register uint32 *x1 asm ("x1") = (uint32 *)(unsigned long)ptr;              \
                                                                                \
    asm volatile(".arch_extension lse\n"                                        \
        "       ldsetal  %w[val], %w[val], %[v]\n"                              \
        : [val] "+r" (w0), [v] "+Q" (*ptr)                                      \
        : "r" (x1)                                                              \
        : "x16", "x17", "x30", "memory");                                       \
    return w0;                                                                  \
}

/*
 * alternative atomic operations of __sync_fetch_and_or for 64bits integers
 * */
static inline uint64 __lse_atomic_fetch_or_u64(volatile uint64 *ptr, uint64 val)
{
    register uint64 x0 asm ("w0") = val;                                        \
    register uint64 *x1 asm ("x1") = (uint64 *)(unsigned long)ptr;              \
                                                                                \
    asm volatile(".arch_extension lse\n"                                        \
        "       ldsetal  %[val], %[val], %[v]\n"                                \
        : [val] "+r" (x0), [v] "+Q" (*ptr)                                      \
        : "r" (x1)                                                              \
        : "x16", "x17", "x30", "memory");                                       \
    return x0;                                                                  \
}

/*
 * alternative atomic operations of __sync_fetch_and_sub for 32bits integers
 * */
static inline uint32 __lse_atomic_fetch_sub_u32(volatile uint32 *ptr, uint32 val)
{
    register uint32 w0 asm ("w0") = val;                                        \
    register uint32 *x1 asm ("x1") = (uint32 *)(unsigned long)ptr;              \
                                                                                \
    asm volatile(".arch_extension lse\n"                                        \
        "       neg  %w[val], %w[val]\n"                                        \
        "       ldaddal  %w[val], %w[val], %[v]\n"                              \
        : [val] "+&r" (w0), [v] "+Q" (*ptr)                                     \
        : "r" (x1)                                                              \
        : "x16", "x17", "x30", "memory");                                       \
    return w0;                                                                  \
}

/*
 * alternative atomic operations of __sync_fetch_and_sub for 64bits integers
 * */
static inline uint64 __lse_atomic_fetch_sub_u64(volatile uint64 *ptr, uint64 val)
{
    register uint64 x0 asm ("w0") = val;                                        \
    register uint64 *x1 asm ("x1") = (uint64 *)(unsigned long)ptr;              \
                                                                                \
    asm volatile(".arch_extension lse\n"                                        \
        "       neg  %[val], %[val]\n"                                          \
        "       ldaddal  %[val], %[val], %[v]\n"                                \
        : [val] "+&r" (x0), [v] "+Q" (*ptr)                                     \
        : "r" (x1)                                                              \
        : "x16", "x17", "x30", "memory");                                       \
    return x0;                                                                  \
}

#endif // __aarch64__

#ifndef WIN32

/*
 * @Description: Atomic increment in a 32-bit address, and return the incremented value.
 * @IN ptr: int32 pointer
 * @IN inc: increase value
 * @Return: new value
 */
static inline int32 GsAtomicAdd32(volatile int32* ptr, int32 inc)
{
    ASSERT_POINTER_ALIGNMENT(ptr, sizeof(int32));
    return __sync_fetch_and_add(ptr, inc) + inc;
}

/*
 * @Description: Atomic increment in a 64-bit address, and return the incremented value.
 * @IN ptr: int64 pointer
 * @IN inc: increase value
 * @Return: new value
 */
static inline int64 GsAtomicAdd64(int64* ptr, int64 inc)
{
    ASSERT_POINTER_ALIGNMENT(ptr, sizeof(int64));
    return __sync_fetch_and_add(ptr, inc) + inc;
}

/*
 * @Description: Atomic set val into *ptr in a 32-bit address, and return the previous pointed by ptr.
 * @IN ptr: int32 pointer
 * @IN val: value to set
 * @Return: old value
 */
static inline int32 GsLockTestAndSet(volatile int32* ptr, int32 val)
{
    ASSERT_POINTER_ALIGNMENT(ptr, sizeof(int32));
    return  __sync_lock_test_and_set(ptr, val);
}

/*
 * @Description: Atomic set val into *ptr in a 32-bit address, and return the previous pointed by ptr.
 * @IN ptr: int64 pointer
 * @IN val: value to set
 * @Return: old value
 */
static inline int64 GsLockTestAndSet64(volatile int64* ptr, int64 val)
{
    ASSERT_POINTER_ALIGNMENT(ptr, sizeof(int64));
    return  __sync_lock_test_and_set(ptr, val);
}

/*
 * @Description: Atomic compare and set val into *ptr in a 32-bit address, and return compare ok or not.
 * @IN ptr: int32 pointer
 * @IN oldval: value to compare
 * @IN newval: value to set
 * @Return: true: dest is equal to the old value false: not equal
 */
static inline bool GsCompareAndSwap32(int32* dest, int32 oldval, int32 newval)
{
    ASSERT_POINTER_ALIGNMENT(dest, sizeof(int32));
    if (oldval == newval) {
        return true;
    }
    volatile bool res = __sync_bool_compare_and_swap(dest, oldval, newval);

    return res;
}

/*
 * @Description: Atomic compare and set val into *ptr in a 64-bit address, and return compare ok or not.
 * @IN ptr: int64 pointer
 * @IN oldval: value to compare
 * @IN newval: value to set
 * @Return: true: dest is equal to the old value false: not equal
 */
static inline bool GsCompareAndSwap64(int64* dest, int64 oldval, int64 newval)
{
    ASSERT_POINTER_ALIGNMENT(dest, sizeof(int64));
    if (oldval == newval) {
        return true;
    }
    return __sync_bool_compare_and_swap(dest, oldval, newval);
}

static inline uint32 GsCompareAndSwapU32(volatile uint32* ptr, uint32 oldval, uint32 newval)
{
    ASSERT_POINTER_ALIGNMENT(ptr, sizeof(uint32));
    return (uint32)__sync_val_compare_and_swap(ptr, oldval, newval);
}

static inline uint64 GsCompareAndSwapU64(volatile uint64* ptr, uint64 oldval, uint64 newval)
{
    ASSERT_POINTER_ALIGNMENT(ptr, sizeof(uint64));
    return (uint64)__sync_val_compare_and_swap(ptr, oldval, newval);
}

/*
 * @Description: Atomic init in a 32-bit address.
 * @IN ptr: int32 pointer
 * @IN inc: int32 value
 * @Return: void
 *
 * WARNING: This is not an ordinary atomic variable like std::atomic in C++,
 * it has atomic semantics but no barrier semantics.
 */
static inline void GsAtomicInitU32(volatile uint32* ptr, uint32 val)
{
    ASSERT_POINTER_ALIGNMENT(ptr, sizeof(uint32));
    __atomic_store_n(ptr, val, __ATOMIC_RELEASE);
}

/*
 * @Description: Atomic increment in a 32-bit address, and return the incremented value.
 * @IN ptr: int32 pointer
 * @IN inc: increase value
 * @Return: new value
 *
 * WARNING: This is not an ordinary atomic operation like std::atomic::load in C++,
 * it has atomic semantics but no barrier semantics.
 */
static inline uint32 GsAtomicReadU32(volatile uint32* ptr)
{
    ASSERT_POINTER_ALIGNMENT(ptr, sizeof(uint32));
    return __atomic_load_n(ptr, __ATOMIC_ACQUIRE);
}

/*
 * @Description: Atomic increment in a 32-bit address, and return the old value.
 * @IN ptr: int32 pointer
 * @IN inc: increase value
 * @Return: old value
 */
static inline uint32 GsAtomicFetchOrU32(volatile uint32* ptr, uint32 inc)
{
    ASSERT_POINTER_ALIGNMENT(ptr, sizeof(uint32));
    return __sync_fetch_and_or(ptr, inc);
}

/*
 * @Description: Atomic and in a 32-bit address, and return the old value.
 * @IN ptr: int32 pointer
 * @IN inc: and value
 * @Return: oid value
 */
static inline uint32 GsAtomicFetchAndU32(volatile uint32* ptr, uint32 inc)
{
    ASSERT_POINTER_ALIGNMENT(ptr, sizeof(uint32));
    return __sync_fetch_and_and(ptr, inc);
}

/*
 * @Description: Atomic increment in a 32-bit address, and return the old value.
 * @IN ptr: int32 pointer
 * @IN inc: increase value
 * @Return: old value
 */
static inline uint32 GsAtomicFetchAddU32(volatile uint32* ptr, uint32 inc)
{
    ASSERT_POINTER_ALIGNMENT(ptr, sizeof(uint32));
    return __sync_fetch_and_add(ptr, inc);
}

/*
 * @Description: Atomic increment in a 32-bit address, and return the incremented value.
 * @IN ptr: int32 pointer
 * @IN inc: increase value
 * @Return: new value
 */
static inline uint32 GsAtomicAddFetchU32(volatile uint32* ptr, uint32 inc)
{
    ASSERT_POINTER_ALIGNMENT(ptr, sizeof(uint32));
    return __sync_fetch_and_add(ptr, inc) + inc;
}

/*
 * @Description: Atomic decrement in a 32-bit address, and return the old value.
 * @IN ptr: int32 pointer
 * @IN inc: decrease value
 * @Return: old value
 */
static inline uint32 GsAtomicFetchSubU32(volatile uint32* ptr, int32 inc)
{
    ASSERT_POINTER_ALIGNMENT(ptr, sizeof(uint32));
    return __sync_fetch_and_sub(ptr, inc);
}

/*
 * @Description: Atomic decrement in a 32-bit address, and return the decremented value.
 * @IN ptr: int32 pointer
 * @IN inc: decrease value
 * @Return: new value
 */
static inline uint32 GsAtomicSubFetchU32(volatile uint32* ptr, int32 inc)
{
    ASSERT_POINTER_ALIGNMENT(ptr, sizeof(uint32));
    return  __sync_fetch_and_sub(ptr, inc) - inc;
}

/*
 * @Description: Atomic change to given value newval in a 32-bit address.
 * if *ptr is equal to *expected.
 * @IN ptr: int32 pointer
 * @IN expected: int32 pointer, expected value
 * @IN newval: new value
 * @Return: true if *ptr is equal to *expected. otherwise return false.
 */
static inline bool GsAtomicCompareExchangeU32(volatile uint32* ptr, uint32* expected, uint32 newval)
{
    ASSERT_POINTER_ALIGNMENT(ptr, sizeof(uint32));
    ASSERT_POINTER_ALIGNMENT(expected, sizeof(uint32));
    bool ret = false;
    uint32 current = __sync_val_compare_and_swap(ptr, *expected, newval);
    ret = current == *expected;
    *expected = current;
    return ret;
}

/*
 * @Description: Atomic write in a 32-bit address.
 * @IN ptr: int32 pointer
 * @IN val: new value
 */
static inline void GsAtomicWriteU32(volatile uint32* ptr, uint32 val)
{
    ASSERT_POINTER_ALIGNMENT(ptr, sizeof(uint32));
    __atomic_store_n(ptr, val, __ATOMIC_RELEASE);
}

/*
 * @Description: Atomic write in a 32-bit address.
 * @IN ptr: int32 pointer
 * @IN val: new value
 * @IN memoryOrder: memory order
 */
inline void GsAtomicBarrierWriteU32(volatile uint32* ptr, uint32 val, MemoryOrder memoryOrder)
{
    ASSERT_POINTER_ALIGNMENT(ptr, sizeof(uint32));
    __atomic_store_n(ptr, val, static_cast<int>(memoryOrder));
}

/*
 * @Description: Atomic change to the value newval in a 32-bit address.
 * if *ptr is equal to *expected, return old value.
 * @IN ptr: int32 pointer
 * @IN newval: new value
 * @Return: old value
 */
inline uint32 GsAtomicExchangeU32(volatile uint32* ptr, uint32 newval)
{
    ASSERT_POINTER_ALIGNMENT(ptr, sizeof(uint32));
    uint32 old;
    while (true) {
        old = GsAtomicReadU32(ptr);
        if (GsAtomicCompareExchangeU32(ptr, &old, newval)) {
            break;
        }
    }
    return old;
}

/*
 * @Description:  Atomic init in a 64-bit address.
 * @IN ptr:  int64 pointer
 * @IN inc:  int64 value
 * @Return:  void
 *
 * WARNING: This is not an ordinary atomic variable like std::atomic in C++,
 * it has atomic semantics but no barrier semantics.
 */
static inline void GsAtomicInitU64(volatile uint64* ptr, uint64 val)
{
    ASSERT_POINTER_ALIGNMENT(ptr, sizeof(uint64));
    __atomic_store_n(ptr, val, __ATOMIC_RELEASE);
}

/*
 * @Description: Atomic read in a 64-bit address, and return the int64 pointer.
 * @IN ptr: int64 pointer
 * @Return: int64 pointer
 *
 * WARNING: This is not an ordinary atomic operation like std::atomic::load in C++,
 * it has atomic semantics but no barrier semantics.
 */
static inline uint64 GsAtomicReadU64(volatile uint64* ptr)
{
    ASSERT_POINTER_ALIGNMENT(ptr, sizeof(uint64));
    return __atomic_load_n(ptr, __ATOMIC_ACQUIRE);
}

/*
 * @Description: Atomic or in a 64-bit address, and return the old value.
 * @IN ptr: int64 pointer
 * @IN inc: or value
 * @Return: old value
 */
static inline uint64 GsAtomicFetchOrU64(volatile uint64* ptr, uint64 inc)
{
    ASSERT_POINTER_ALIGNMENT(ptr, sizeof(uint64));
    return __sync_fetch_and_or(ptr, inc);
}

/*
 * @Description: Atomic and in a 64-bit address, and return the old result.
 * @IN ptr: int64 pointer
 * @IN inc: and value
 * @Return: old value
 */
static inline uint64 GsAtomicFetchAndU64(volatile uint64* ptr, uint64 inc)
{
    ASSERT_POINTER_ALIGNMENT(ptr, sizeof(uint64));
    return __sync_fetch_and_and(ptr, inc);
}

/*
 * @Description: Atomic increment in a 64-bit address, and return the old value.
 * @IN ptr: int64 pointer
 * @IN inc: incremented value
 * @Return: old value
 */
static inline uint64 GsAtomicFetchAddU64(volatile uint64* ptr, uint64 inc)
{
    ASSERT_POINTER_ALIGNMENT(ptr, sizeof(uint64));
    return __sync_fetch_and_add(ptr, inc);
}

/*
 * @Description: Atomic increment in a 64-bit address, and return the incremented value.
 * @IN ptr: int64 pointer
 * @IN inc: incremented value
 * @Return: new value
 */
static inline uint64 GsAtomicAddFetchU64(volatile uint64* ptr, uint64 inc)
{
    ASSERT_POINTER_ALIGNMENT(ptr, sizeof(uint64));
    return __sync_add_and_fetch(ptr, inc);
}

/*
 * @Description: Atomic increment in a 64-bit address, and return the old value.
 * @IN ptr: int64 pointer
 * @IN inc: delcrease value
 * @Return: old value
 */
static inline uint64 GsAtomicFetchSubU64(volatile uint64* ptr, uint64 inc)
{
    ASSERT_POINTER_ALIGNMENT(ptr, sizeof(uint64));
    return __sync_fetch_and_sub(ptr, inc);
}

/*
 * @Description: Atomic decrease in a 64-bit address, and return the decremented value.
 * @IN ptr: int64 pointer
 * @IN inc: decrease value
 * @Return: new value
 */
static inline uint64 GsAtomicSubFetchU64(volatile uint64* ptr, uint64 inc)
{
    ASSERT_POINTER_ALIGNMENT(ptr, sizeof(uint64));
    return __sync_sub_and_fetch(ptr, inc);
}

/*
 * @Description:  Atomic change to the given value newval in a 64-bit address, if *ptr is equal to *expected.
 * @IN ptr:  int64 pointer
 * @IN expected:  int64 pointer, expected value
 * @IN newval:  new value
 * @Return:  true if *ptr is equal to *expected. otherwise return false.
 */
static inline bool GsAtomicCompareExchangeU64(volatile uint64* ptr, uint64* expected, uint64 newval)
{
    ASSERT_POINTER_ALIGNMENT(ptr, sizeof(uint64));
    ASSERT_POINTER_ALIGNMENT(expected, sizeof(uint64));
    bool ret = false;
    uint64 current = __sync_val_compare_and_swap(ptr, *expected, newval);
    ret = current == *expected;
    *expected = current;
    return ret;
}

/*
 * @Description: Atomic write in a 34-bit address.
 * @IN ptr: int64 pointer
 * @IN val: new value
 */
static inline void GsAtomicWriteU64(volatile uint64* ptr, uint64 val)
{
    ASSERT_POINTER_ALIGNMENT(ptr, sizeof(uint64));
    __atomic_store_n(ptr, val, __ATOMIC_RELEASE);
}

/*
 * @Description: Atomic write in a 34-bit address.
 * @IN ptr: int64 pointer
 * @IN val: new value
 * @IN memoryOrder: memory order
 */
inline void GsAtomicBarrierWriteU64(volatile uint64* ptr, uint64 val, MemoryOrder memoryOrder)
{
    ASSERT_POINTER_ALIGNMENT(ptr, sizeof(uint64));
    __atomic_store_n(ptr, val, static_cast<int>(memoryOrder));
}

/*
 * @Description: Atomic change to the value newval in a 64-bit address,
 * if *ptr is equal to *expected, return old value.
 * @IN ptr: int64 pointer
 * @IN newval: new value
 * @Return: old value
 */
static inline uint64 GsAtomicExchangeU64(volatile uint64* ptr, uint64 newval)
{
    ASSERT_POINTER_ALIGNMENT(ptr, sizeof(uint64));
    uint64 old;
    while (true) {
        old = GsAtomicReadU64(ptr);
        if (GsAtomicCompareExchangeU64(ptr, &old, newval)) {
            break;
        }
    }
    return old;
}

#ifdef __aarch64__
/*
 * This static function implements an atomic compare and exchange operation of 128bit width variable.
 * This compares the contents of *ptr with the contents of *old and if equal, writes newval into *ptr.
 * If they are not equal, the current contents of *ptr is written into *old.
 * This API is an alternative implementation of __sync_val_compare_and_swap for 128bit on ARM64 platforms.
 *
 * @IN ptr: uint128_u pointer shold be 128bit(16bytes) aligned
 * @IN oldval: old value, should be 128bit(16bytes) aligned
 * @IN newval: new value, should be 128bit(16bytes) aligned
 * @Return: old value
 */
static inline uint128_u arm_compare_and_swap_u128(volatile uint128_u* ptr, uint128_u oldval, uint128_u newval)
{
    ASSERT_POINTER_ALIGNMENT(ptr, sizeof(uint128_u));
#ifndef __ARM_LSE
    return __excl_compare_and_swap_u128(ptr, oldval, newval);
#else
    return __lse_compare_and_swap_u128(ptr, oldval, newval);
#endif
}
#endif

static inline void GsAtomicInitUintptr(volatile uintptr_t* ptr, uintptr_t val)
{
    GsAtomicInitU64(ptr, (uint64)val);
}

static inline uintptr_t GsAtomicReadUintptr(volatile uintptr_t* ptr)
{
    return (uintptr_t)GsAtomicReadU64(ptr);
}

static inline bool GsAtomicCompareExchangeUintptr(volatile uintptr_t* ptr, uintptr_t* expected, uintptr_t newval)
{
    return GsAtomicCompareExchangeU64(ptr, (uint64*)expected, (uint64)newval);
}

/*
 * @Description: Atomic write in a uintptr_t address.
 * @IN ptr: uintptr_t pointer
 * @IN inc: new value
 */
static inline void GsAtomicWriteUintptr(volatile uintptr_t* ptr, uintptr_t val)
{
    GsAtomicWriteU64(ptr, (uint64)val);
}

/*
 * @Description: Atomic change to the value newval in a uintptr_t address,
 * if *ptr is equal to *expected, return old value.
 * @IN ptr: uintptr_t pointer
 * @IN newval: new value
 * @Return: old value
 */
static inline uintptr_t GsAtomicExchangeUintptr(volatile uintptr_t* ptr, uintptr_t newval)
{
    return (uintptr_t)GsAtomicExchangeU64(ptr, (uint64)newval);
}

/*
 * @Description: Atomic load acquire the value in a uint32 address.
 * @IN ptr: uint32 pointer
 * @Return: the value of pointer
 */
inline uint32 GsAtomicBarrierReadU32(volatile uint32* ptr, MemoryOrder memoryOrder)
{
    return __atomic_load_n(ptr, static_cast<int>(memoryOrder));
}

/*
 * @Description: Atomic load acquire the value in a uint64 address.
 * @IN ptr: uint64 pointer
 * @Return: the value of pointer
 */
inline uint64 GsAtomicBarrierReadU64(volatile uint64* ptr, MemoryOrder memoryOrder)
{
    ASSERT_POINTER_ALIGNMENT(ptr, sizeof(uint64));
    return __atomic_load_n(ptr, static_cast<int>(memoryOrder));
}

/*
 * @Description  This API is an unified wrapper of __sync_val_compare_and_swap for 128bit on ARM64 platforms and X86.
 *
 * @IN ptr:  uint128_t pointer shold be 128bit(16bytes) aligned
 * @IN oldval:  old value, should be 128bit(16bytes) aligned
 * @IN newval:  new value, should be 128bit(16bytes) aligned
 * @Return:  old value
 */
static inline uint128_u atomic_compare_and_swap_u128(
    volatile uint128_u* ptr,
    uint128_u oldval = uint128_u{0},
    uint128_u newval = uint128_u{0})
{
    ASSERT_POINTER_ALIGNMENT(ptr, sizeof(uint128_u));
#ifndef __aarch64__
    uint128_u ret;
    ret.u128 = __sync_val_compare_and_swap(&ptr->u128, oldval.u128, newval.u128);
    return ret;
#else
    return arm_compare_and_swap_u128(ptr, oldval, newval);
#endif
}
#endif
}

#endif /* STORAGE_ATOMIC_H */
