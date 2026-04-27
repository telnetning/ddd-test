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
 * port_atomic.h
 *
 * Description:
 * Cross-platform atomic operations.
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef UTILS_PORT_ATOMIC_H
#define UTILS_PORT_ATOMIC_H

#include <stdint.h>

#include "defines/common.h"
#include "defines/err_code.h"
#include "defines/utils_errorcode.h"

#if defined(__GNUC__) && defined(ATOMIC_BUILTINS)
#include "port/posix_atomic_atomic_def.h"
#elif defined(__GNUC__) && defined(SYNC_BUILTINS)
#include "port/posix_atomic_sync_def.h"
#endif

GSDB_BEGIN_C_CODE_DECLS

/*
 * The current program compiled mode in 32-bit or 64-bit mode, should be specified the compilation macro
 * in CMake and use the compilation macro in code development.
 * The current CMake project does not have this macro. Therefore, here use SIZE_MAX for judgment.
 * C defines the maximum value of the basic type in the <stdint.h> header file,
 * and SIZE_MAX defines the macro constant of the maximum value of the size_t type.
 * If SIZE_MAX is 2 ^ 64 - 1, that's 64 bits. If SIZE_MAX is 2 ^ 32 - 1, that's 32 bits.
 */
#define COMPILE_MODE_32_BIT 1
#define COMPILE_MODE_64_BIT 2
#if SIZE_MAX == 18446744073709551615ull
#define CURRENT_COMPILE_MODE COMPILE_MODE_64_BIT
#elif SIZE_MAX == 4294967295
#define CURRENT_COMPILE_MODE COMPILE_MODE_32_BIT
#else
#error "the current compile mode is unknown "
#endif

/*
 * The memory model defines a set of output results of the concurrent program when it performs read and
 * write operations on shared variables.
 * Memory model involves two parts: programming language memory model and hardware memory consistency model.
 * 1. Programming language memory model: compilation optimization when converting programs to binary code,
 * reordering variable accesses.
 * 2. Hardware memory model: memory access optimization and memory access consistency (that is, CPU cache coherence)
 * during CPU execution in a specific hardware architecture.
 * 3. Atomic type operations have six memory order options: MEMORY_ORDER_RELAXED, MEMORY_ORDER_CONSUME,
 * MEMORY_ORDER_ACQUIRE, MEMORY_ORDER_RELEASE, MEMORY_ORDER_ACQ_REL,MEMORY_ORDER_SEQ_CST .
 * All atomic types default to MEMORY_ORDER_SEQ_CST unless you specify an order option for a particular operation.
 * 4. The six memory sequences can be divided into three categories:
 * Free Memory Order (MEMORY_ORDER_RELAXED).
 * Get-Release Memory Order (MEMORY_ORDER_CONSUME, MEMORY_ORDER_ACQUIRE, MEMORY_ORDER_RELEASE and MEMORY_ORDER_ACQ_REL).
 * sequential consistent memory (MEMORY_ORDER_SEQ_CST).
 * 5. MEMORY_ORDER_RELAXED is used independently and acts on atomic variables. It does not have the synchronizes-with
 * relationship.The same atomic variable has the happens-before relationship in the same thread. Different atomic
 * variables do not have the happens-before relationship and can be executed out of order.In a thread, if an expression
 * has seen the value a of an atomic variable, subsequent expressions of the expression can only see a or a value later
 * than a.
 * 6. MEMORY_ORDER_RELEASE and MEMORY_ORDER_ACQUIRE are used together.
 * or MEMORY_ORDER_RELEASE, MEMORY_ORDER_ACQUIRE, and MEMORY_ORDER_ACQ_REL are used together,
 * where MEMORY_ORDER_ACQ_REL is generally used for functions that have both read and write operations,
 * for example, compare and exchange.
 * 7.MEMORY_ORDER_CONSUME and MEMORY_ORDER_RELEASE are used together.
 * MEMORY_ORDER_CONSUME only ensures that the atomic object self is read and written synchronously.
 * 8. MEMORY_ORDER_SEQ_CST is used alone, more strictly than MEMORY_ORDER_ACQ_REL.
 * MEMORY_ORDER_SEQ_CST establishes a separate complete order for all memory operations with this tag.
 * The order you actually observe on multiple threads is consistent with the program order.
 * Sequential consistency is the simplest and most intuitive sequence, but it is also the most expensive
 * memory sequence and requires global synchronization of all threads.
 * 9. Don't use mixing memory models,especially if it involves a relaxed mode.
 * Mixing MEMORY_ORDER_SEQ_CST and MEMORY_ORDER_RELEASE/MEMORY_ORDER_ACQUIRE can be done with care,
 * but you really do need to have a thorough understanding the subtleties.
 */

/*
 * MemoryOrder.
 * MEMORY_ORDER_RELAXED：Only the atomicity of the current operation is ensured.
 * Synchronization between threads is not considered. There is no sequence restriction on the read
 * and write of other threads. Other threads may read new values or old values. Instructions may be
 * executed in different order within the same thread.
 * There are no synchronization or ordering constraints imposed on other reads or writes,
 * only this operation's atomicity is guaranteed.
 *
 * MEMORY_ORDER_CONSUME:Consume indicates the read operation (load). The MEMORY_ORDER_CONSUME
 * indicates that the read/write operations that follow this statement and depend on this atomic
 * variable cannot be rearranged before the load statement in the current thread. All other threads
 * have written the variable memory of the store operation, which is visible to this thread.
 * A load operation with this memory order performs a consume operation on the affected memory
 * location: no reads or writes in the current thread dependent on the value currently loaded
 * can be reordered before this load. Writes to data-dependent variables in other threads that
 * release the same atomic variable are visible in the current thread. On most platforms,
 * this affects compiler optimizations only.
 *
 * MEMORY_ORDER_ACQUIRE:Acquire indicates the read operation (load). The MEMORY_ORDER_ACQUIRE
 * indicates that the read and write operations after the statement cannot be rearranged to the
 * front of the statement in the current thread. Write operations of all other threads are
 * visible to this thread.
 * A load operation with this memory order performs the acquire operation on the affected memory
 * location: no reads or writes in the current thread can be reordered before this load.
 * All writes in other threads that release the same atomic variable are visible in the current thread.
 *
 * MEMORY_ORDER_RELEASE:Release indicates the write operation (store).The MEMORY_ORDER_RELEASE
 * indicates that the read/write operations before the statement cannot be rearranged after the
 * statement in the current thread. All write operations of the current thread are visible to
 * other threads, indicating that the write operations of the current thread to non-local
 * variables can be obtained by other threads.
 * A store operation with this memory order performs the release operation: no reads or writes
 * in the current thread can be reordered after this store. All writes in the current thread are
 * visible in other threads that acquire the same atomic variable and writes that carry a dependency
 * into the atomic variable become visible in other threads that consume the same atomic.
 *
 * MEMORY_ORDER_ACQ_REL:Acquire and release indicates a read-modify-write operation.The MEMORY_ORDER_ACQ_REL
 * indicating that the operation is both an acquire operation and a release operation.
 * In the current thread, the memory reads and writes before and after the write operation cannot be rearranged.
 * The memory write of all other threads for the release operation of the atomic object is visible before
 * the current thread changes the modification operation. The memory modification is visible to other threads
 * before acquiring the atomic object.
 * A read-modify-write operation with this memory order is both an acquire operation and a release operation.
 * No memory reads or writes in the current thread can be reordered before the load, nor after the store.
 * All writes in other threads that release the same atomic variable are visible before the modification
 * and the modification is visible in other threads that acquire the same atomic variable.
 *
 * MEMORY_ORDER_SEQ_CST:This is a combination of MEMORY_ORDER_RELEASE, MEMORY_ORDER_ACQUIRE, and MEMORY_ORDER_ACQ_REL.
 * Read is the acquire semantics, write is the release semantics, and read + write is the acquire-release semantics.
 * At the same time, all atomic operations using the memory order are synchronized. All threads view the memory
 * operations in the same order, just as a single thread executes the instructions of all threads.
 * A load operation with this memory order performs an acquire operation, a store performs a release operation,
 * and read-modify-write performs both an acquire operation and a release operation, plus a single total order
 * exists in which all threads observe all modifications in the same order.
 */
enum MemoryOrder {
    /* No barriers or synchronization. */
    MEMORY_ORDER_RELAXED = 1,
    /* Data dependency only for both barrier and synchronization with another thread. */
    MEMORY_ORDER_CONSUME,
    /* Barrier to hoisting of code and synchronizes with release (or stronger) semantic stores from another thread. */
    MEMORY_ORDER_ACQUIRE,
    /* Barrier to sinking of code and synchronizes with acquire (or stronger) semantic loads from another thread. */
    MEMORY_ORDER_RELEASE,
    /* Full barrier in both directions and synchronizes with acquire loads and release stores in another thread. */
    MEMORY_ORDER_ACQ_REL,
    /* Full barrier in both directions and synchronizes with acquire loads and release stores in all threads. */
    MEMORY_ORDER_SEQ_CST
};
typedef enum MemoryOrder MemoryOrder;

/*
 * The following data type atomic operations are supported:
 * int8_t,uint8_t,int16_t,uint16_t,int32_t,uint32_t,int64_t,uint64_t,ssize_t,size_t,void *,bool.
 * The corresponding atomic type is Atomic8,AtomicU8,Atomic16,AtomicU16,AtomicInt32,AtomicU32,AtomicInt64,
 * AtomicU64,AtomicSsize,AtomicSize,AtomicPointer,AtomicBool.
 */

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
static inline void AtomicThreadFence(MemoryOrder memoryOrder);
/*
 * The following is an atomic operation function of type int8_t.
 */
/*
 * This function implements an atomic get operation. It returns the contents of *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, MEMORY_ORDER_ACQUIRE, and MEMORY_ORDER_CONSUME.
 */
static inline int8_t Atomic8Get(const Atomic8 *atomic8, MemoryOrder memoryOrder);
/*
 * This function implements an atomic set operation. It writes value into *atomic. The valid memory
 * model variants are MEMORY_ORDER_RELAXED, MEMORY_ORDER_SEQ_CST, and MEMORY_ORDER_RELEASE.
 */
static inline void Atomic8Set(Atomic8 *atomic8, int8_t value, MemoryOrder memoryOrder);
/*
 * This function implements an atomic exchange operation. It writes value into *atomic, and returns
 * the previous contents of *atomic.The valid memory model variants are MEMORY_ORDER_RELAXED,
 * MEMORY_ORDER_SEQ_CST, MEMORY_ORDER_ACQUIRE, MEMORY_ORDER_RELEASE, and MEMORY_ORDER_ACQ_REL.
 */
static inline int8_t Atomic8Exchange(Atomic8 *atomic8, int8_t value, MemoryOrder memoryOrder);
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
                                              MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder);
static inline bool Atomic8CompareExchangeStrong(Atomic8 *atomic8, int8_t *expected, int8_t desired,
                                                MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder);
/*
 * These functions perform the operation suggested by the name, and return the value that had previously
 * been in *atomic. That is,
 * { tmp = *atomic; *atomic op= value; return tmp; }
 * All memory models are valid.
 */
static inline int8_t Atomic8FetchAdd(Atomic8 *atomic8, int8_t value, MemoryOrder memoryOrder);
static inline int8_t Atomic8FetchSub(Atomic8 *atomic8, int8_t value, MemoryOrder memoryOrder);
static inline int8_t Atomic8FetchAnd(Atomic8 *atomic8, int8_t value, MemoryOrder memoryOrder);
static inline int8_t Atomic8FetchOr(Atomic8 *atomic8, int8_t value, MemoryOrder memoryOrder);
static inline int8_t Atomic8FetchXor(Atomic8 *atomic8, int8_t value, MemoryOrder memoryOrder);
/* The memory model is the MEMORY_ORDER_RELAXED,only keep atomic. */
static inline void Atomic8GetAddSet(Atomic8 *atomic8, int8_t increment)
{
    int8_t oldValue = Atomic8Get(atomic8, MEMORY_ORDER_RELAXED);
    int8_t newValue = (int8_t)(oldValue + increment);
    Atomic8Set(atomic8, newValue, MEMORY_ORDER_RELAXED);
}
static inline void Atomic8GetSubSet(Atomic8 *atomic8, int8_t increment)
{
    int8_t oldValue = Atomic8Get(atomic8, MEMORY_ORDER_RELAXED);
    int8_t newValue = (int8_t)(oldValue - increment);
    Atomic8Set(atomic8, newValue, MEMORY_ORDER_RELAXED);
}

/*
 * The following is an atomic operation function of type uint8_t.
 */
static inline uint8_t AtomicU8Get(const AtomicU8 *atomic8, MemoryOrder memoryOrder);
static inline void AtomicU8Set(AtomicU8 *atomic8, uint8_t value, MemoryOrder memoryOrder);
static inline uint8_t AtomicU8Exchange(AtomicU8 *atomic8, uint8_t value, MemoryOrder memoryOrder);
static inline bool AtomicU8CompareExchangeWeak(AtomicU8 *atomic8, uint8_t *expected, uint8_t desired,
                                               MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder);
static inline bool AtomicU8CompareExchangeStrong(AtomicU8 *atomic8, uint8_t *expected, uint8_t desired,
                                                 MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder);
static inline uint8_t AtomicU8FetchAdd(AtomicU8 *atomic8, uint8_t value, MemoryOrder memoryOrder);
static inline uint8_t AtomicU8FetchSub(AtomicU8 *atomic8, uint8_t value, MemoryOrder memoryOrder);
static inline uint8_t AtomicU8FetchAnd(AtomicU8 *atomic8, uint8_t value, MemoryOrder memoryOrder);
static inline uint8_t AtomicU8FetchOr(AtomicU8 *atomic8, uint8_t value, MemoryOrder memoryOrder);
static inline uint8_t AtomicU8FetchXor(AtomicU8 *atomic8, uint8_t value, MemoryOrder memoryOrder);
/* The memory model is the MEMORY_ORDER_RELAXED,only keep atomic. */
static inline void AtomicU8GetAddSet(AtomicU8 *atomic8, uint8_t increment)
{
    uint8_t oldValue = AtomicU8Get(atomic8, MEMORY_ORDER_RELAXED);
    uint8_t newValue = (uint8_t)(oldValue + increment);
    AtomicU8Set(atomic8, newValue, MEMORY_ORDER_RELAXED);
}
static inline void AtomicU8GetSubSet(AtomicU8 *atomic8, uint8_t increment)
{
    uint8_t oldValue = AtomicU8Get(atomic8, MEMORY_ORDER_RELAXED);
    uint8_t newValue = (uint8_t)(oldValue - increment);
    AtomicU8Set(atomic8, newValue, MEMORY_ORDER_RELAXED);
}

/*
 * The following is an atomic operation function of type int16_t.
 */
static inline int16_t Atomic16Get(const Atomic16 *atomic16, MemoryOrder memoryOrder);
static inline void Atomic16Set(Atomic16 *atomic16, int16_t value, MemoryOrder memoryOrder);
static inline int16_t Atomic16Exchange(Atomic16 *atomic16, int16_t value, MemoryOrder memoryOrder);
static inline bool Atomic16CompareExchangeWeak(Atomic16 *atomic16, int16_t *expected, int16_t desired,
                                               MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder);
static inline bool Atomic16CompareExchangeStrong(Atomic16 *atomic16, int16_t *expected, int16_t desired,
                                                 MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder);
static inline int16_t Atomic16FetchAdd(Atomic16 *atomic16, int16_t value, MemoryOrder memoryOrder);
static inline int16_t Atomic16FetchSub(Atomic16 *atomic16, int16_t value, MemoryOrder memoryOrder);
static inline int16_t Atomic16FetchAnd(Atomic16 *atomic16, int16_t value, MemoryOrder memoryOrder);
static inline int16_t Atomic16FetchOr(Atomic16 *atomic16, int16_t value, MemoryOrder memoryOrder);
static inline int16_t Atomic16FetchXor(Atomic16 *atomic16, int16_t value, MemoryOrder memoryOrder);
/* The memory model is the MEMORY_ORDER_RELAXED,only keep atomic. */
static inline void Atomic16GetAddSet(Atomic16 *atomic16, int16_t increment)
{
    int16_t oldValue = Atomic16Get(atomic16, MEMORY_ORDER_RELAXED);
    int16_t newValue = (int16_t)(oldValue + increment);
    Atomic16Set(atomic16, newValue, MEMORY_ORDER_RELAXED);
}
static inline void Atomic16GetSubSet(Atomic16 *atomic16, int16_t increment)
{
    int16_t oldValue = Atomic16Get(atomic16, MEMORY_ORDER_RELAXED);
    int16_t newValue = (int16_t)(oldValue - increment);
    Atomic16Set(atomic16, newValue, MEMORY_ORDER_RELAXED);
}

/*
 * The following is an atomic operation function of type uint16_t.
 */
static inline uint16_t AtomicU16Get(const AtomicU16 *atomic16, MemoryOrder memoryOrder);
static inline void AtomicU16Set(AtomicU16 *atomic16, uint16_t value, MemoryOrder memoryOrder);
static inline uint16_t AtomicU16Exchange(AtomicU16 *atomic16, uint16_t value, MemoryOrder memoryOrder);
static inline bool AtomicU16CompareExchangeWeak(AtomicU16 *atomic16, uint16_t *expected, uint16_t desired,
                                                MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder);
static inline bool AtomicU16CompareExchangeStrong(AtomicU16 *atomic16, uint16_t *expected, uint16_t desired,
                                                  MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder);
static inline uint16_t AtomicU16FetchAdd(AtomicU16 *atomic16, uint16_t value, MemoryOrder memoryOrder);
static inline uint16_t AtomicU16FetchSub(AtomicU16 *atomic16, uint16_t value, MemoryOrder memoryOrder);
static inline uint16_t AtomicU16FetchAnd(AtomicU16 *atomic16, uint16_t value, MemoryOrder memoryOrder);
static inline uint16_t AtomicU16FetchOr(AtomicU16 *atomic16, uint16_t value, MemoryOrder memoryOrder);
static inline uint16_t AtomicU16FetchXor(AtomicU16 *atomic16, uint16_t value, MemoryOrder memoryOrder);
/* The memory model is the MEMORY_ORDER_RELAXED,only keep atomic. */
static inline void AtomicU16GetAddSet(AtomicU16 *atomic16, uint16_t increment)
{
    uint16_t oldValue = AtomicU16Get(atomic16, MEMORY_ORDER_RELAXED);
    uint16_t newValue = (uint16_t)(oldValue + increment);
    AtomicU16Set(atomic16, newValue, MEMORY_ORDER_RELAXED);
}
static inline void AtomicU16GetSubSet(AtomicU16 *atomic16, uint16_t increment)
{
    uint16_t oldValue = AtomicU16Get(atomic16, MEMORY_ORDER_RELAXED);
    uint16_t newValue = (uint16_t)(oldValue - increment);
    AtomicU16Set(atomic16, newValue, MEMORY_ORDER_RELAXED);
}

/*
 * The following is an atomic operation function of type int32_t.
 */
static inline int32_t Atomic32Get(const AtomicInt32 *atomic32, MemoryOrder memoryOrder);
static inline void Atomic32Set(AtomicInt32 *atomic32, int32_t value, MemoryOrder memoryOrder);
static inline int32_t Atomic32Exchange(AtomicInt32 *atomic32, int32_t value, MemoryOrder memoryOrder);
static inline bool Atomic32CompareExchangeWeak(AtomicInt32 *atomic32, int32_t *expected, int32_t desired,
                                               MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder);
static inline bool Atomic32CompareExchangeStrong(AtomicInt32 *atomic32, int32_t *expected, int32_t desired,
                                                 MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder);
static inline int32_t Atomic32FetchAdd(AtomicInt32 *atomic32, int32_t value, MemoryOrder memoryOrder);
static inline int32_t Atomic32FetchSub(AtomicInt32 *atomic32, int32_t value, MemoryOrder memoryOrder);
static inline int32_t Atomic32FetchAnd(AtomicInt32 *atomic32, int32_t value, MemoryOrder memoryOrder);
static inline int32_t Atomic32FetchOr(AtomicInt32 *atomic32, int32_t value, MemoryOrder memoryOrder);
static inline int32_t Atomic32FetchXor(AtomicInt32 *atomic32, int32_t value, MemoryOrder memoryOrder);
/* The memory model is the MEMORY_ORDER_RELAXED,only keep atomic. */
static inline void Atomic32GetAddSet(AtomicInt32 *atomic32, int32_t increment)
{
    int32_t oldValue = Atomic32Get(atomic32, MEMORY_ORDER_RELAXED);
    int32_t newValue = oldValue + increment;
    Atomic32Set(atomic32, newValue, MEMORY_ORDER_RELAXED);
}
static inline void Atomic32GetSubSet(AtomicInt32 *atomic32, int32_t increment)
{
    int32_t oldValue = Atomic32Get(atomic32, MEMORY_ORDER_RELAXED);
    int32_t newValue = oldValue - increment;
    Atomic32Set(atomic32, newValue, MEMORY_ORDER_RELAXED);
}

/*
 * The following is an atomic operation function of type uint32_t.
 */
static inline uint32_t AtomicU32Get(const AtomicU32 *atomic32, MemoryOrder memoryOrder);
static inline void AtomicU32Set(AtomicU32 *atomic32, uint32_t value, MemoryOrder memoryOrder);
static inline uint32_t AtomicU32Exchange(AtomicU32 *atomic32, uint32_t value, MemoryOrder memoryOrder);
static inline bool AtomicU32CompareExchangeWeak(AtomicU32 *atomic32, uint32_t *expected, uint32_t desired,
                                                MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder);
static inline bool AtomicU32CompareExchangeStrong(AtomicU32 *atomic32, uint32_t *expected, uint32_t desired,
                                                  MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder);
static inline uint32_t AtomicU32FetchAdd(AtomicU32 *atomic32, uint32_t value, MemoryOrder memoryOrder);
static inline uint32_t AtomicU32FetchSub(AtomicU32 *atomic32, uint32_t value, MemoryOrder memoryOrder);
static inline uint32_t AtomicU32FetchAnd(AtomicU32 *atomic32, uint32_t value, MemoryOrder memoryOrder);
static inline uint32_t AtomicU32FetchOr(AtomicU32 *atomic32, uint32_t value, MemoryOrder memoryOrder);
static inline uint32_t AtomicU32FetchXor(AtomicU32 *atomic32, uint32_t value, MemoryOrder memoryOrder);
/* The memory model is the MEMORY_ORDER_RELAXED,only keep atomic. */
static inline void AtomicU32GetAddSet(AtomicU32 *atomic32, uint32_t increment)
{
    uint32_t oldValue = AtomicU32Get(atomic32, MEMORY_ORDER_RELAXED);
    uint32_t newValue = oldValue + increment;
    AtomicU32Set(atomic32, newValue, MEMORY_ORDER_RELAXED);
}
static inline void AtomicU32GetSubSet(AtomicU32 *atomic32, uint32_t increment)
{
    uint32_t oldValue = AtomicU32Get(atomic32, MEMORY_ORDER_RELAXED);
    uint32_t newValue = oldValue - increment;
    AtomicU32Set(atomic32, newValue, MEMORY_ORDER_RELAXED);
}

/*
 * The following is an atomic operation function of type int64_t.
 */
static inline int64_t Atomic64Get(const AtomicInt64 *atomic64, MemoryOrder memoryOrder);
static inline void Atomic64Set(AtomicInt64 *atomic64, int64_t value, MemoryOrder memoryOrder);
static inline int64_t Atomic64Exchange(AtomicInt64 *atomic64, int64_t value, MemoryOrder memoryOrder);
static inline bool Atomic64CompareExchangeWeak(AtomicInt64 *atomic64, int64_t *expected, int64_t desired,
                                               MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder);
static inline bool Atomic64CompareExchangeStrong(AtomicInt64 *atomic64, int64_t *expected, int64_t desired,
                                                 MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder);
static inline int64_t Atomic64FetchAdd(AtomicInt64 *atomic64, int64_t value, MemoryOrder memoryOrder);
static inline int64_t Atomic64FetchSub(AtomicInt64 *atomic64, int64_t value, MemoryOrder memoryOrder);
static inline int64_t Atomic64FetchAnd(AtomicInt64 *atomic64, int64_t value, MemoryOrder memoryOrder);
static inline int64_t Atomic64FetchOr(AtomicInt64 *atomic64, int64_t value, MemoryOrder memoryOrder);
static inline int64_t Atomic64FetchXor(AtomicInt64 *atomic64, int64_t value, MemoryOrder memoryOrder);
/* The memory model is the MEMORY_ORDER_RELAXED,only keep atomic. */
static inline void Atomic64GetAddSet(AtomicInt64 *atomic64, int64_t increment)
{
    int64_t oldValue = Atomic64Get(atomic64, MEMORY_ORDER_RELAXED);
    int64_t newValue = oldValue + increment;
    Atomic64Set(atomic64, newValue, MEMORY_ORDER_RELAXED);
}
static inline void Atomic64GetSubSet(AtomicInt64 *atomic64, int64_t increment)
{
    int64_t oldValue = Atomic64Get(atomic64, MEMORY_ORDER_RELAXED);
    int64_t newValue = oldValue - increment;
    Atomic64Set(atomic64, newValue, MEMORY_ORDER_RELAXED);
}

/*
 * The following is an atomic operation function of type uint64_t.
 */
static inline uint64_t AtomicU64Get(const AtomicU64 *atomic64, MemoryOrder memoryOrder);
static inline void AtomicU64Set(AtomicU64 *atomic64, uint64_t value, MemoryOrder memoryOrder);
static inline uint64_t AtomicU64Exchange(AtomicU64 *atomic64, uint64_t value, MemoryOrder memoryOrder);
static inline bool AtomicU64CompareExchangeWeak(AtomicU64 *atomic64, uint64_t *expected, uint64_t desired,
                                                MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder);
static inline bool AtomicU64CompareExchangeStrong(AtomicU64 *atomic64, uint64_t *expected, uint64_t desired,
                                                  MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder);
static inline uint64_t AtomicU64FetchAdd(AtomicU64 *atomic64, uint64_t value, MemoryOrder memoryOrder);
static inline uint64_t AtomicU64FetchSub(AtomicU64 *atomic64, uint64_t value, MemoryOrder memoryOrder);
static inline uint64_t AtomicU64FetchAnd(AtomicU64 *atomic64, uint64_t value, MemoryOrder memoryOrder);
static inline uint64_t AtomicU64FetchOr(AtomicU64 *atomic64, uint64_t value, MemoryOrder memoryOrder);
static inline uint64_t AtomicU64FetchXor(AtomicU64 *atomic64, uint64_t value, MemoryOrder memoryOrder);
/* The memory model is the MEMORY_ORDER_RELAXED,only keep atomic. */
static inline void AtomicU64GetAddSet(AtomicU64 *atomic64, uint64_t increment)
{
    uint64_t oldValue = AtomicU64Get(atomic64, MEMORY_ORDER_RELAXED);
    uint64_t newValue = oldValue + increment;
    AtomicU64Set(atomic64, newValue, MEMORY_ORDER_RELAXED);
}
static inline void AtomicU64GetSubSet(AtomicU64 *atomic64, uint64_t increment)
{
    uint64_t oldValue = AtomicU64Get(atomic64, MEMORY_ORDER_RELAXED);
    uint64_t newValue = oldValue - increment;
    AtomicU64Set(atomic64, newValue, MEMORY_ORDER_RELAXED);
}
/*
 * The following is an atomic operation function of type ssize_t.
 */
static inline ssize_t AtomicSsizeGet(const AtomicSsize *atomicSsize, MemoryOrder memoryOrder);
static inline void AtomicSsizeSet(AtomicSsize *atomicSsize, ssize_t value, MemoryOrder memoryOrder);
static inline ssize_t AtomicSsizeExchange(AtomicSsize *atomicSsize, ssize_t value, MemoryOrder memoryOrder);
static inline bool AtomicSsizeCompareExchangeWeak(AtomicSsize *atomicSsize, ssize_t *expected, ssize_t desired,
                                                  MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder);
static inline bool AtomicSSizeCompareExchangeStrong(AtomicSsize *atomicSsize, ssize_t *expected, ssize_t desired,
                                                    MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder);
static inline ssize_t AtomicSsizeFetchAdd(AtomicSsize *atomicSsize, ssize_t value, MemoryOrder memoryOrder);
static inline ssize_t AtomicSsizeFetchSub(AtomicSsize *atomicSsize, ssize_t value, MemoryOrder memoryOrder);
static inline ssize_t AtomicSsizeFetchAnd(AtomicSsize *atomicSsize, ssize_t value, MemoryOrder memoryOrder);
static inline ssize_t AtomicSsizeFetchOr(AtomicSsize *atomicSsize, ssize_t value, MemoryOrder memoryOrder);
static inline ssize_t AtomicSsizeFetchXor(AtomicSsize *atomicSsize, ssize_t value, MemoryOrder memoryOrder);
/* The memory model is the MEMORY_ORDER_RELAXED,only keep atomic. */
static inline void AtomicSsizeGetAddSet(AtomicSsize *atomicSsize, ssize_t increment)
{
    ssize_t oldValue = AtomicSsizeGet(atomicSsize, MEMORY_ORDER_RELAXED);
    ssize_t newValue = oldValue + increment;
    AtomicSsizeSet(atomicSsize, newValue, MEMORY_ORDER_RELAXED);
}
static inline void AtomicSsizeGetSubSet(AtomicSsize *atomicSsize, ssize_t increment)
{
    ssize_t oldValue = AtomicSsizeGet(atomicSsize, MEMORY_ORDER_RELAXED);
    ssize_t newValue = oldValue - increment;
    AtomicSsizeSet(atomicSsize, newValue, MEMORY_ORDER_RELAXED);
}

/*
 * The following is an atomic operation function of type size_t.
 */
static inline size_t AtomicSizeGet(const AtomicSize *atomicSize, MemoryOrder memoryOrder);
static inline void AtomicSizeSet(AtomicSize *atomicSize, size_t value, MemoryOrder memoryOrder);
static inline size_t AtomicSizeExchange(AtomicSize *atomicSize, size_t value, MemoryOrder memoryOrder);
static inline bool AtomicSizeCompareExchangeWeak(AtomicSize *atomicSize, size_t *expected, size_t desired,
                                                 MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder);
static inline bool AtomicSizeCompareExchangeStrong(AtomicSize *atomicSize, size_t *expected, size_t desired,
                                                   MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder);
static inline size_t AtomicSizeFetchAdd(AtomicSize *atomicSize, size_t value, MemoryOrder memoryOrder);
static inline size_t AtomicSizeFetchSub(AtomicSize *atomicSize, size_t value, MemoryOrder memoryOrder);
static inline size_t AtomicSizeFetchAnd(AtomicSize *atomicSize, size_t value, MemoryOrder memoryOrder);
static inline size_t AtomicSizeFetchOr(AtomicSize *atomicSize, size_t value, MemoryOrder memoryOrder);
static inline size_t AtomicSizeFetchXor(AtomicSize *atomicSize, size_t value, MemoryOrder memoryOrder);
/* The memory model is the MEMORY_ORDER_RELAXED,only keep atomic. */
static inline void AtomicSizeGetAddSet(AtomicSize *atomicSize, size_t increment)
{
    size_t oldValue = AtomicSizeGet(atomicSize, MEMORY_ORDER_RELAXED);
    size_t newValue = oldValue + increment;
    AtomicSizeSet(atomicSize, newValue, MEMORY_ORDER_RELAXED);
}
static inline void AtomicSizeGetSubSet(AtomicSize *atomicSize, size_t increment)
{
    size_t oldValue = AtomicSizeGet(atomicSize, MEMORY_ORDER_RELAXED);
    size_t newValue = oldValue - increment;
    AtomicSizeSet(atomicSize, newValue, MEMORY_ORDER_RELAXED);
}

/*
 * The following is an atomic operation function of type void *.
 */
static inline void *AtomicPointerGet(const AtomicPointer *atomicPointer, MemoryOrder memoryOrder);
static inline void AtomicPointerSet(AtomicPointer *atomicPointer, void *value, MemoryOrder memoryOrder);
static inline void *AtomicPointerExchange(AtomicPointer *atomicPointer, void *value, MemoryOrder memoryOrder);
static inline bool AtomicPointerCompareExchangeWeak(AtomicPointer *atomicPointer, void **expected, void *desired,
                                                    MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder);
static inline bool AtomicPointerCompareExchangeStrong(AtomicPointer *atomicPointer, void **expected, void *desired,
                                                      MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder);
/*
 * The following is an atomic operation function of type bool.
 */
static inline bool AtomicBoolGet(const AtomicBool *atomicBool, MemoryOrder memoryOrder);
static inline void AtomicBoolSet(AtomicBool *atomicBool, bool value, MemoryOrder memoryOrder);
static inline bool AtomicBoolExchange(AtomicBool *atomicBool, bool value, MemoryOrder memoryOrder);
static inline bool AtomicBoolCompareExchangeWeak(AtomicBool *atomicBool, bool *expected, bool desired,
                                                 MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder);
static inline bool AtomicBoolCompareExchangeStrong(AtomicBool *atomicBool, bool *expected, bool desired,
                                                   MemoryOrder successMemoryOrder, MemoryOrder failureMemoryOrder);

#define ATOMICU8_INSTANCE_INITIAL_VALUE 0
UTILS_EXPORT ErrorCode InitializedOnlyOnce(AtomicU8 *atomic8, ErrorCode (*initializedCallback)(void *), void *arg);
GSDB_END_C_CODE_DECLS

#endif /* UTILS_PORT_ATOMIC_H */
