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
 * Lightweight lock manager
 *
 * Lightweight locks are intended primarily to provide mutual exclusion of
 * access to shared-memory data structures.  Therefore, they offer both
 * exclusive and shared lock modes (to support read/write and read-only
 * access to a shared object). There are few other frammishes.  User-level
 * locking should be done with the full lock manager --- which depends on
 * LWLocks to protect its shared state.
 *
 * NOTES:
 *
 * This used to be a pretty straight forward reader-writer lock
 * implementation, in which the internal state was protected by a
 * spinlock. Unfortunately the overhead of taking the spinlock proved to be
 * too high for workloads/locks that were taken in shared mode very
 * frequently. Often we were spinning in the (obviously exclusive) spinlock,
 * while trying to acquire a shared lock that was actually free.
 *
 * Thus a new implementation was devised that provides wait-free shared lock
 * acquisition for locks that aren't exclusively locked.
 *
 * The basic idea is to have a single atomic variable 'lockcount' instead of
 * the formerly separate shared and exclusive counters and to use atomic
 * operations to acquire the lock. That's fairly easy to do for plain
 * rw-spinlocks, but a lot harder for something like LWLocks that want to wait
 * in the OS.
 *
 * For lock acquisition we use an atomic compare-and-exchange on the lockcount
 * variable. For exclusive lock we swap in a sentinel value
 * (LW_VAL_EXCLUSIVE), for shared locks we count the number of holders.
 *
 * To release the lock we use an atomic decrement to release the lock. If the
 * new value is zero (we get that atomically), we know we can/have to release
 * waiters.
 *
 * Obviously it is important that the sentinel value for exclusive locks
 * doesn't conflict with the maximum number of possible share lockers -
 * luckily MAX_BACKENDS makes that easily possible.
 *
 *
 * The attentive reader might have noticed that naively doing the above has a
 * glaring race condition: We try to lock using the atomic operations and
 * notice that we have to wait. Unfortunately by the time we have finished
 * queuing, the former locker very well might have already finished it's
 * work. That's problematic because we're now stuck waiting inside the OS.
 *
 * To mitigate those races we use a two phased attempt at locking:
 *   Phase 1: Try to do it atomically, if we succeed, nice
 *   Phase 2: Add ourselves to the waitqueue of the lock
 *   Phase 3: Try to grab the lock again, if we succeed, remove ourselves from
 *            the queue
 *   Phase 4: Sleep till wake-up, goto Phase 1
 *
 * This protects us against the problem from above as nobody can release too
 *    quick, before we're queued, since after Phase 2 we're already queued.
 * -------------------------------------------------------------------------
 */
#include "lock/dstore_lwlock.h"
#include "common/dstore_tsan_annotation.h"
#include "common/concurrent/dstore_futex.h"
#include "common/log/dstore_log.h"
#include "common/algorithm/dstore_string_info.h"
#include "lock/dstore_s_lock.h"
#include "framework/dstore_thread.h"

#include "lock/dstore_lock_dummy.h"
#include "errorcode/dstore_lock_error_code.h"
#include "framework/dstore_instance.h"
#include "diagnose/dstore_lwlock_diagnose.h"
#include "fault_injection/fault_injection.h"

namespace DSTORE {
#define LW_FEILD_BIT_WIDTH      (32)

/* Below is high 32bit in state. It is a flag bit that records the information used internally by lwlock.
 * Record three types of information:
 * (1) HAS_WAITERS      -- has lwlock waiter in wait list, uncertainly true entering kernel state sleep
 * (2) RELEASE_OK       -- already about to go to sleep, need wake up me when someone release the lwlock
 * (3) WAIT_LIST_LOCKED -- internal wait list locked status */
#define LW_FLAG_HAS_WAITERS64       (static_cast<uint64>(1) << (10 + 32))
#define LW_FLAG_RELEASE_OK64        (static_cast<uint64>(1) << (9 + 32))
#define LW_FLAG_HAS_WAITERS32       (static_cast<uint32>(1) << 10)
#define LW_FLAG_RELEASE_OK32        (static_cast<uint32>(1) << 9)
#define LW_FLAG_WAIT_LIST_LOCKED32  (static_cast<uint32>(1) << 8) // together

/* Those low 32bit is express the perceived lock status (shared locked flag and exclusive locked flag)
 * and shared locker counter by the outsize. That is, the basic semantic identification of a lock.
 * Record three types of information:
 * (1) VAL_EXCLUSIVE    -- the lwlock already exclusively locked
 * (2) VAL_SHARED       -- already shared locked
 * (3) SHARE_COUNT      -- shard locked count if already shared locked */
#define LW_VAL_DISALLOW_PREEMPT64   (static_cast<uint64>(1) << 26)
#define LW_VAL_DISALLOW_PREEMPT32   (static_cast<uint32>(1) << 26)
#define LW_VAL_EXCLUSIVE64          (static_cast<uint64>(1) << 25)
#define LW_VAL_SHARED64             (static_cast<uint64>(1) << 24)
#define LW_LOCK_MASK64              (static_cast<uint64>(LW_VAL_EXCLUSIVE64 | LW_VAL_SHARED64))
#define LW_VAL_EXCLUSIVE32          (static_cast<uint32>(1) << 25)
#define LW_VAL_SHARED32             (static_cast<uint32>(1) << 24) // together
#define LW_LOCK_MASK32              (static_cast<uint32>(LW_VAL_SHARED32 | LW_VAL_EXCLUSIVE32))
#define LW_LOCK_SHARE_COUNT_MASK64  ((static_cast<uint64>(1) << 24) - 1)
#define LW_LOCK_SHARE_COUNT_MASK32  ((static_cast<uint32>(1) << 24) - 1)

/* Two groups of information status in 64bit state, bit used like below
 *                                                                  SHARED LOCKED    __NOT USED__
 *                                                                          \/      /            \
 * | 0| 1| 2| 3| 4| 5| 6| 7| 8| 9|10|11|12|13|14|15|16|17|18|19|20|21|22|23|24|25|26|27|28|29|30|31|
 * |<---------------------------- SHARE_COUNT ---------------------------->|   /\ /\
 *                                                                             || ||
 *                                                                             || DISALLOW PREEMPT
 *                    WAIT_LIST_LOCKED                                  EXCLUSIVE LOCKED
 *   ______NOT USED______   ||         ___________________________NOT USED_______________________
 *  /                    \  \/        /                                                          \
 * |32|33|34|35|36|37|38|39|40|41|42|43|44|45|46|47|48|49|50|51|52|53|54|55|56|57|58|59|60|61|62|63|
 *                             ^   ^
 *               RELEASE_OK____|   |______HAS_WAITER
 * Why flag need to define the positions in 32bit and 64bit respectively, It's because for the sake of performance,
 * some operations are 64-bit together, for example, When a shared locker releases the lwlock, need let share count
 * subtract 1, and need get all 64bit of state simultaneously, to decide whether need to wakeup waiter. But in some
 * cases, in order to avoid conflicts, only operate 32bit. for example, CAS operate when locking, only care lwlock
 * itself locked state, avoid distractions from internal waiting queue lock/unlock. can only use 32bit CAS to remove
 * SHARED LOCKED flag, when shared locker count from 1. because if use 64bit CAS, RELEASE_OK and HAS_WAITER will
 * disrupt release CAS, causes loss of a wakeup action. This is why bit of RELEASE_OK/HAS_WAITER and SHARED LOCKED
 * must be separated, Because they need CAS not to interfere with each other.
 */

#define LW_FLAG_EXSTATE_MASK32  (static_cast<uint64>(~static_cast<uint32>(0)))

#ifdef LOCK_DEBUG
    /* Must be greater than MAX_BACKENDS - which is 2^23-1, so we're fine. */
    #define LW_SHARED_MASK (static_cast<uint32>(1 << 23))
#endif

static inline gs_atomic_uint32 *GetStateHigh32(LWLock *lock)
{
    return ((gs_atomic_uint32 *)&lock->state) + 1;
}

static inline gs_atomic_uint32 *GetStateLow32(LWLock *lock)
{
    return ((gs_atomic_uint32 *)&lock->state);
}

static inline uint32 LWLockStateHigh32(uint64 state)
{
    return (uint32)((state >> LW_FEILD_BIT_WIDTH) & LW_FLAG_EXSTATE_MASK32);
}

static inline uint32 LWLockStateLow32(uint64 state)
{
    return (uint32)((state) & LW_FLAG_EXSTATE_MASK32);
}

bool IsLWLockExclusiveLocked(LWLock *lock)
{
    return ((GsAtomicReadU32(GetStateLow32(lock))) & LW_VAL_EXCLUSIVE32);
}

bool IsLWLockIdle(LWLock *lock)
{
    uint32 state = GsAtomicReadU32(GetStateLow32(lock));
    return !(state & LW_VAL_EXCLUSIVE32) && !(state & LW_VAL_SHARED32) && !(state & LW_FLAG_HAS_WAITERS32);
}

static const char *g_LWLockGroupNames[] = {
    "LWLOCK_GROUP_DEFAULT",
    "LWLOCK_GROUP_HASH_TABLE_PARTITION",
    "LWLOCK_GROUP_TABLE_LOCK_STAT_MUTEX",
    "LWLOCK_GROUP_BUF_LRU",
    "LWLOCK_GROUP_DISTRIBUTED_BUF_DESC_CONTROLLER_PD_REQUEST",
    "LWLOCK_GROUP_INV_TASK_QUEUE",
    "LWLOCK_GROUP_REGISTERED_THREAD",
    "LWLOCK_GROUP_MEMBER_VIEW_ON_CHANGE",
    "LWLOCK_GROUP_MEMBER_VIEW_NODE_CRASH_TERM_HTAB",
    "LWLOCK_GROUP_DEADLOCK_DETECTOR_WAITER_INFO",
    "LWLOCK_GROUP_DISTRIBUTED_TABLE_CLUSTER_INFO",
    "LWLOCK_GROUP_THREAD_LIST",
    "LWLOCK_GROUP_CSN_NODELIST_UPDATE",
    "LWLOCK_GROUP_PLSN_WAIT",
    "LWLOCK_GROUP_BUF_DESC_IO_IN_PROGRESS",
    "LWLOCK_GROUP_BUF_DESC_CR_ASSIGN",
    "LWLOCK_GROUP_BUF_DESC_PAGE_WRITE_WAL",
    "LWLOCK_GROUP_BUF_DESC_CONTENT",
    "LWLOCK_GROUP_BUF_LRU_EXPAND",
    "LWLOCK_GROUP_BUF_MEMCHUNK",
    "LWLOCK_GROUP_BUF_MEMCHUNK_LIST",
    "LWLOCK_GROUP_BUF_MGR_SIZE",
    "LWLOCK_GROUP_BUF_MAPPING",
    "LWLOCK_GROUP_CHECK_POINT",
    "LWLOCK_GROUP_MEM_CHECK_POINT",
    "LWLOCK_GROUP_BUFFER_POOL_SCAN_MEM_CHUNK",
    "LWLOCK_GROUP_PD_HASH_PARTITION",
    "LWLOCK_GROUP_CONSISTENT_HASH_LIST_LATCH",
    "LWLOCK_GROUP_HASH_META_LATCH",
    "LWLOCK_GROUP_HASH_RING_LATCH",
    "LWLOCK_GROUP_RESCUE_MEM",
    "LWLOCK_GROUP_VFS_LIB_HANDLE",
    "LWLOCK_GROUP_PDB_INDEX",
    "LWLOCK_GROUP_INIT_PDB",
    "LWLOCK_GROUP_THREAD_CORE",
    "LWLOCK_GROUP_THREAD_XACT",
    "LWLOCK_GROUP_AID_CLUSTER",
    "LWLOCK_GROUP_DEADLOCK_DETECT",
    "LWLOCK_GROUP_DEADLOCK_THRD_STAT",
    "LWLOCK_GROUP_DEADLOCK_DETECT_DATA",
    "LWLOCK_GROUP_LOCAL_LOCK",
    "LWLOCK_GROUP_LOCK_NODE_LOCAL_PARTITION",
    "LWLOCK_GROUP_DECODE_DICT_BUF",
    "LWLOCK_GROUP_LOGICAL_REPLICA_MGR_SLOT_CTL",
    "LWLOCK_GROUP_LOGICAL_REPLICA_MGR_SLOT_ALLOC",
    "LWLOCK_GROUP_DISTRIBUTE_LOGICAL_REPLICA_MGR_NODE_LIST",
    "LWLOCK_GROUP_PDB_COMM_STATUS",
    "LWLOCK_GROUP_ALLOC_STANDBY_PDB_SLOT",
    "LWLOCK_GROUP_PDB_REPLICA_MGR_NODE_LIST",
    "LWLOCK_GROUP_PERF_DUMP",
    "LWLOCK_GROUP_DIST_PDB_RECOVERY_STATE",
    "LWLOCK_GROUP_MULTI_THREAD_TASK_HTAB",
    "LWLOCK_GROUP_REDO_QUEUE",
    "LWLOCK_GROUP_REDO_QUEUE_HTAB_PARTITION",
    "LWLOCK_GROUP_OBJ_SPACE_MGR_TASK_QUEUE",
    "LWLOCK_GROUP_TBS_PAGE",
    "LWLOCK_GROUP_TBS_PAGE_MAPPING",
    "LWLOCK_GROUP_MAX_RESERVED_CSN",
    "LWLOCK_GROUP_UPDATE_LOCAL_CSN_MIN",
    "LWLOCK_GROUP_CONFLICT_HINT",
    "LWLOCK_GROUP_TAC_HEAP",
    "LWLOCK_GROUP_UNDO_MAP",
    "LWLOCK_GROUP_GET_PAGE_WAL_RECORDS",
    "LWLOCK_GROUP_BUF_TRANSFER_PAGE_CACHE",
    "LWLOCK_GROUP_BUF_ANTI_CACHE",
    "LWLOCK_GROUP_TABLESPACE",
    "LWLOCK_GROUP_DATAFILE",
    "LWLOCK_GROUP_MAX"
};
static_assert(sizeof(g_LWLockGroupNames) / sizeof(const char *) == static_cast<unsigned long>(LWLOCK_GROUP_MAX) + 1);

const char *GetLWLockGroupName(int groupId)
{
    StorageAssert(groupId < static_cast<int>(LWLOCK_GROUP_MAX));
    return g_LWLockGroupNames[groupId];
}
#define DSTORE_T_NAME(lock) GetLWLockGroupName((lock)->groupId)

#ifdef DSTORE_LWLOCK_LOG
bool Trace_lwlocks = false;

inline static void PRINT_LWDEBUG(const char *where, LWLock *lock, LWLockMode mode)
{
    /* hide statement & context here, otherwise the log is just too verbose */
    if (Trace_lwlocks) {
        uint64 state = GsAtomicReadU64(&lock->state);
        ErrLog(DSTORE_LOG, MODULE_LOCK,
            ErrMsg("tid %d: %s(%s): lock state %lu, num waiters %u.",
                t_thrd.proc_cxt.MyProcPid, where, DSTORE_T_NAME(lock), state, GsAtomicReadU32(&lock->nwaiters)));
    }
}

inline static void LOG_LWDEBUG(const char *where, LWLock *lock, const char *msg)
{
    /* hide statement & context here, otherwise the log is just too verbose */
    if (Trace_lwlocks) {
        ErrLog(DSTORE_LOG, MODULE_LOCK, ErrMsg("%s(%s): %s.", where, DSTORE_T_NAME(lock), msg));
    }
}

#else /* not DSTORE_LWLOCK_LOG */
#define PRINT_LWDEBUG(a, b, c) ((void)0)
#define LOG_LWDEBUG(a, b, c) ((void)0)
#endif /* DSTORE_LWLOCK_LOG */

/*
 * LWLockInitialize - initialize a new lwlock; it's initially unlocked
 */
void LWLockInitialize(LWLock *lock, uint16 groupId)
{
    StorageAssert(groupId < static_cast<uint16>(LWLOCK_GROUP_MAX));
    GsAtomicInitU64(&lock->state, LW_FLAG_RELEASE_OK64);
    DListInit(&lock->waiters);
    lock->groupId = static_cast<uint16>(groupId);
    lock->spinsPerDelay = DEFAULT_SPINS_PER_DELAY;

    /* ENABLE_THREAD_CHECK only, Register RWLock in Tsan */
    TsAnnotateRWLockCreate(&lock->rwlock);
    TsAnnotateRWLockCreate(&lock->listlock);

#ifdef LOCK_DEBUG
    GsAtomicInitU32(&lock->nwaiters, 0);
#endif
}

HOTFUNCTION static inline uint32 LWLockGetDesiredStateExclusive(uint32 oldState, bool needAllowPreempt,
    bool wakedFromQueue, bool &noLockFlagBefore)
{
    uint32 desiredState = oldState;

    if (needAllowPreempt) {
        StorageAssert((oldState & LW_VAL_DISALLOW_PREEMPT32) != 0);
        noLockFlagBefore = ((oldState & LW_LOCK_MASK32) == 0);
        if (noLockFlagBefore) {
            desiredState |= LW_VAL_EXCLUSIVE32;
            desiredState &= ~LW_VAL_DISALLOW_PREEMPT32;
        }
    } else {
        if (wakedFromQueue) {
            noLockFlagBefore = ((oldState & LW_LOCK_MASK32) == 0);
        } else {
            noLockFlagBefore = ((oldState & (LW_LOCK_MASK32 | LW_VAL_DISALLOW_PREEMPT32)) == 0);
        }
        if (noLockFlagBefore) {
            desiredState |= LW_VAL_EXCLUSIVE32;
        }
    }

    return desiredState;
}

HOTFUNCTION static inline uint32 LWLockGetDesiredStateShare(uint32 oldState,
    UNUSE_PARAM bool needAllowPreempt, bool wakedFromQueue, bool &noLockFlagBefore)
{
    uint32 desiredState = oldState;
    StorageAssert(!needAllowPreempt);

    /* Only test LW_VAL_EXCLUSIVE32 for share mode. */
    if (wakedFromQueue) {
        noLockFlagBefore = ((oldState & LW_VAL_EXCLUSIVE32) == 0);
    } else {
        noLockFlagBefore = ((oldState & (LW_VAL_EXCLUSIVE32 | LW_VAL_DISALLOW_PREEMPT32)) == 0);
    }
    if (noLockFlagBefore) {
        /* If it is no locked, let's set shared lock */
        desiredState |= LW_VAL_SHARED32;
    } else {
        /* If it is locked, the following CAS has two purposes
         * 1. determines whether old_state has changed (whether it has been unlocked this moment)
         * 2. decrease the shared count that had beed increased */
        desiredState -= 1;
        if (((oldState & LW_VAL_DISALLOW_PREEMPT32) != 0) && ((oldState & LW_VAL_SHARED32) != 0) &&
            ((oldState & LW_LOCK_SHARE_COUNT_MASK32) == 1)) {
            desiredState &= ~LW_VAL_SHARED32;
        }
    }

    return desiredState;
}

static void LWLockWakeup(LWLock *lock);
static inline void LWLockTryWakeupWhenAttemptLock(LWLock *lock, LWLockMode mode, uint32 oldState)
{
    if ((mode == LW_SHARED) && ((oldState & LW_VAL_SHARED32) != 0)) {
        StorageAssert((oldState & LW_VAL_DISALLOW_PREEMPT32) != 0);
        uint64 newstate = GsAtomicReadU64(&lock->state);
        bool checkWaiters =
            ((newstate & (LW_FLAG_HAS_WAITERS64 | LW_FLAG_RELEASE_OK64)) ==
            (LW_FLAG_HAS_WAITERS64 | LW_FLAG_RELEASE_OK64)) &&
            ((newstate & LW_LOCK_MASK64) == 0);
        if (checkWaiters) {
            LWLockWakeup(lock);
        }
    }
}

/*
 * Internal function that tries to atomically acquire the lwlock in the passed
 * in mode. This function will not block waiting for a lock to become free - that's the
 * callers job.
 * Returns true if the lock isn't free and we need to wait.
 */

template <LWLockMode mode>
HOTFUNCTION static bool LWLockAttemptLock(LWLock *lock, bool needAllowPreempt, bool wakedFromQueue)
{
    uint32 old_state;
    gs_atomic_uint32 *state = GetStateLow32(lock);
    bool directLock = true;

    StorageAssert(mode == LW_EXCLUSIVE || mode == LW_SHARED);

    /*
     * Read once outside the loop, later iterations will get the newer value
     * via compare & exchange.
     */
    if (mode == LW_SHARED) {
        /* add 1 unconditionally, two effects
         * 1. exclusive cas to be more times, but exclusive lock cas, most likely to sleep
         * 2. more easier preempted by a shared lock
         * it seems not to bad */
        old_state = LWLockStateLow32(__atomic_add_fetch(&lock->state, 1, __ATOMIC_RELAXED));
        /* Why we say add_fetch is old state, becase of add state must not affected the bits of state
         * except SHARE_COUNT, the bits of of state will all keep old value except SHARE_COUNT, so can say
         * __atomic_add_fetch is get oldState */
    } else {
        old_state = GsAtomicReadU32(state); // low 32 bit
    }

    /* loop until we've determined whether we could acquire the lock or not */
    while (true) {
        uint32 desiredState;
        bool noLockFlagBefore = false;

        if (mode == LW_SHARED) {
            if (((old_state & LW_VAL_SHARED32) != 0) &&
                (((old_state & LW_VAL_DISALLOW_PREEMPT32) == 0) || wakedFromQueue)) {
                /* the lwlock had locked in shared mode, so got shared lock directly */
                if (directLock) {
                    /* direct lock success, not pass cas, need read barrier. here add a bidirectional read barrier */
                    __atomic_thread_fence(__ATOMIC_ACQUIRE);
                }
                return false;
            }
            /* unfortunatily, lost shared, maybe either free or exclusive lock, have to cas */
            directLock = false;
            desiredState = LWLockGetDesiredStateShare(old_state, needAllowPreempt,
                                                      wakedFromQueue, noLockFlagBefore);
        } else {
            desiredState = LWLockGetDesiredStateExclusive(old_state, needAllowPreempt,
                                                          wakedFromQueue, noLockFlagBefore);
        }

        /*
         * Attempt to swap in the state we are expecting. If we see the lwlock already locked before,
         * desiredState == old_state, aim to look once the old_state whether changed this moment. Retry if the value
         * changed since we last looked at it. If it freed by other one, we will be happy to try to acquire the lock
         * again, don't need go to kernel space to sleep myself.
         * If we saw it as no locked before, we'll attempt to mark(lock) it acquired. The reason that we always swap
         * in the value is that this doubles as a memory barrier.
         */
        if (GsAtomicCompareExchangeU32(state, &old_state, desiredState)) {
            if (noLockFlagBefore) {
                /* ENABLE_THREAD_CHECK only, Must acquire vector clock info from other
                 * thread after got the lock */
                if (desiredState & LW_VAL_EXCLUSIVE32) {
                    TsAnnotateRWLockAcquired(&lock->rwlock, 1);
                } else {
                    TsAnnotateRWLockAcquired(&lock->rwlock, 0);
                }

                /* Great! Got the lock. */
#ifdef LOCK_DEBUG
                if (mode == LW_EXCLUSIVE) {
                    lock->owner = thrd->GetCore();
                }
#endif
                return false;
            } else {
                LWLockTryWakeupWhenAttemptLock(lock, mode, old_state);
                /* fault inject let the execution flow become very very not atomic */
                FAULT_INJECTION_CALL(LWLOCK_STUCK_SCENE_FAULT_INJECT, NULL);
                /* CAS success, and locked before. This case implies that the exclusivily locker did not release lock.
                 * sadly, we not got the lock. */
                return true; /* someobdy else has the lock */
            }
        }
    }
}
template bool LWLockAttemptLock<LW_EXCLUSIVE>(LWLock *lock, bool needAllowPreempt, bool wakedFromQueue);
template bool LWLockAttemptLock<LW_SHARED>(LWLock *lock, bool needAllowPreempt, bool wakedFromQueue);
/*
 * Lock the LWLock's wait list against concurrent activity.
 *
 * NB: even though the wait list is locked, non-conflicting lock operations
 * may still happen concurrently.
 *
 * Time spent holding mutex should be short!
 */
static void LWLockWaitListLock(LWLock *lock)
{
    uint32 old_state;
    gs_atomic_uint32 *state = GetStateHigh32(lock);

    while (true) {
        /* always try once to acquire lock directly */
        old_state = GsAtomicFetchOrU32(state, LW_FLAG_WAIT_LIST_LOCKED32);
        if (!(old_state & LW_FLAG_WAIT_LIST_LOCKED32)) {
            break; /* got lock */
        }

        /* and then spin without atomic operations until lock is released */
        {
#ifndef ENABLE_THREAD_CHECK
            SpinDelayStatus delayStatus = InitSpinDelay(__FILE__, __LINE__);
#endif

            while (old_state & LW_FLAG_WAIT_LIST_LOCKED32) {
#ifndef ENABLE_THREAD_CHECK
                PerformSpinDelay(&delayStatus, lock->spinsPerDelay);
#endif
                old_state = GsAtomicReadU32(state);
            }

#ifndef ENABLE_THREAD_CHECK
            AdjustSpinsPerDelay(&delayStatus, lock->spinsPerDelay);
#endif
        }

        /*
         * Retry. The lock might obviously already be re-acquired by the time
         * we're attempting to get it again.
         */
    }

    /* ENABLE_THREAD_CHECK only, Must acquire vector clock info from other
     * thread after got the lock */
    TsAnnotateRWLockAcquired(&lock->listlock, 1);
}

/*
 * Unlock the LWLock's wait list.
 *
 * Note that it can be more efficient to manipulate flags and release the
 * locks in a single atomic operation.
 */
static void LWLockWaitListUnlock(LWLock *lock)
{
    uint32 old_state DSTORE_PG_USED_FOR_ASSERTS_ONLY;

    /* ENABLE_THREAD_CHECK only, Must release vector clock info to other
     * threads before unlock */
    TsAnnotateRWLockReleased(&lock->listlock, 1);

    old_state = GsAtomicFetchAndU32(GetStateHigh32(lock), ~LW_FLAG_WAIT_LIST_LOCKED32);
    StorageAssert(old_state & LW_FLAG_WAIT_LIST_LOCKED32);
}

/*
 * Wakeup all the lockers that currently have a chance to acquire the lock.
 */
HOTFUNCTION static void LWLockWakeup(LWLock *lock)
{
    bool newReleaseOk = true;
    bool wokeupSomebody = false;
    dlist_head wakeup;
    dlist_mutable_iter iter;

    DListInit(&wakeup);

    /* lock wait list while collecting backends to wake up */
    LWLockWaitListLock(lock);

    dlist_foreach_modify(iter, &lock->waiters)
    {
        LWLockWaiter *waiter = dlist_container(LWLockWaiter, next_waiter, iter.cur);
        if (wokeupSomebody && waiter->mode == static_cast<int>(LW_EXCLUSIVE)) {
            continue;
        }

        DListDelete(&waiter->next_waiter);
        DListPushTail(&wakeup, &waiter->next_waiter);
        if (waiter->mode != static_cast<int>(LW_WAIT_UNTIL_FREE)) {
            /* Prevent additional wakeups until retryer gets to run. Backends
             * that are just waiting for the lock to become free don't retry
             * automatically. */
            newReleaseOk = false;
            /* Don't wakeup (further) exclusive locks. */
            wokeupSomebody = true;
        }

        /*
         * Once we've woken up an exclusive lock, there's no point in waking
         * up anybody else.
         */
        if (waiter->mode == static_cast<int>(LW_EXCLUSIVE)) {
            break;
        }
    }

    StorageAssert(DListIsEmpty(&wakeup) || (GsAtomicReadU64(&lock->state) & LW_FLAG_HAS_WAITERS64));

    /* unset required flags, and release lock, in one fell swoop */
    {
        uint32 old_state;
        uint32 desired_state;

        /* ENABLE_THREAD_CHECK only, Must release vector clock info to other
         * threads before unlock */
        TsAnnotateRWLockReleased(&lock->listlock, 1);

        gs_atomic_uint32 *state = GetStateHigh32(lock);
        old_state = GsAtomicReadU32(state);
        while (true) {
            desired_state = old_state;

            /* compute desired flags */
            if (newReleaseOk) {
                desired_state |= LW_FLAG_RELEASE_OK32;
            } else {
                desired_state &= ~LW_FLAG_RELEASE_OK32;
            }

            if (DListIsEmpty(&wakeup)) {
                desired_state &= ~LW_FLAG_HAS_WAITERS32;
            }

            desired_state &= ~LW_FLAG_WAIT_LIST_LOCKED32;  // release lock
            if (GsAtomicCompareExchangeU32(state, &old_state, desired_state)) {
                break;
            }
        }
    }

    /* Awaken any waiters I removed from the queue. */
    dlist_foreach_modify(iter, &wakeup)
    {
        LWLockWaiter *waiter = dlist_container(LWLockWaiter, next_waiter, iter.cur);
        ThreadCore* core = CONTAINER_OF(ThreadCore, lockWaiter, waiter);
        LOG_LWDEBUG("LWLockRelease", lock, "release waiter");
        DListDelete(&waiter->next_waiter);

        /* ENABLE_THREAD_CHECK only, waiter->lwWaiting should not be reported race  */
        TsAnnotateBenignRaceSized(waiter, sizeof(LWLockWaiter));
        waiter->CleanUpWaitLock();

        /*
         * Guarantee that lwWaiting being unset only becomes visible once the
         * unlink from the link has completed. Otherwise the target backend
         * could be woken up for other reason and enqueue for a new lock - if
         * that happens before the list unlink happens, the list would end up
         * being corrupted.
         *
         * The barrier pairs with the LWLockWaitListLock() when enqueing for
         * another lock.
         */
        GS_WRITE_BARRIER();
        waiter->StopWaiting();

        core->Wakeup();
    }
}

/*
 * Add ourselves to the end of the queue.
 *
 * NB: Mode can be LW_WAIT_UNTIL_FREE here!
 */
static void LWLockQueueSelf(LWLock *lock, LWLockMode mode)
{
    /*
     * If we don't have a PGPROC structure, there's no way to wait. This
     * should never occur, since t_thrd.proc should only be null during shared
     * memory initialization.
     */
    StorageReleasePanic(thrd->GetCore() == nullptr, MODULE_LOCK, ErrMsg("cannot wait without a PGPROC structure."));

    LWLockWaiter* waiter = &thrd->GetCore()->lockWaiter;
    StorageReleasePanic(waiter->waiting, MODULE_LOCK, ErrMsg("queueing for lock while waiting on another one."));

    LWLockWaitListLock(lock);

    /* setting the flag is protected by the spinlock */
    (void)GsAtomicFetchOrU32(GetStateHigh32(lock), LW_FLAG_HAS_WAITERS32);

    waiter->RecordWaitLock(lock, static_cast<uint8>(mode));
    waiter->StartWaiting();

    /* LW_WAIT_UNTIL_FREE waiters are always at the front of the queue */
    if (mode == LW_WAIT_UNTIL_FREE) {
        DListPushHead(&lock->waiters, &waiter->next_waiter);
    } else {
        DListPushTail(&lock->waiters, &waiter->next_waiter);
    }

    /* Can release the mutex now */
    LWLockWaitListUnlock(lock);

#ifdef LOCK_DEBUG
    GsAtomicFetchAddU32(&lock->nwaiters, 1);
#endif
}

/*
 * Remove ourselves from the waitlist.
 *
 * This is used if we queued ourselves because we thought we needed to sleep
 * but, after further checking, we discovered that we don't actually need to
 * do so. Returns false if somebody else already has woken us up, otherwise
 * returns true.
 */
static void LWLockDequeueSelf(LWLock *lock, __attribute__((__unused__))LWLockMode mode)
{
    bool found = false;
    dlist_mutable_iter iter;

    LWLockWaitListLock(lock);

    /*
     * Can't just remove ourselves from the list, but we need to iterate over
     * all entries as somebody else could have unqueued us.
     */
    dlist_foreach_modify(iter, &lock->waiters)
    {
        LWLockWaiter *waiter = dlist_container(LWLockWaiter, next_waiter, iter.cur);
        ThreadCore* core = CONTAINER_OF(ThreadCore, lockWaiter, waiter);
        if (core == thrd->GetCore()) {
            found = true;
            DListDelete(&waiter->next_waiter);
            break;
        }
    }

    gs_atomic_uint32 *state = GetStateHigh32(lock);
    if (DListIsEmpty(&lock->waiters) && (GsAtomicReadU32(state) & LW_FLAG_HAS_WAITERS32) != 0) {
        (void)GsAtomicFetchAndU32(state, ~LW_FLAG_HAS_WAITERS32);
    }

    /* XXX: combine with fetch_and above? */
    LWLockWaitListUnlock(lock);

    /* clear waiting state again, nice for debugging */
    if (found) {
        thrd->GetCore()->lockWaiter.StopWaiting();
    } else {
        /*
         * Somebody else dequeued us and has or will wake us up. Deal with the
         * superflous absorption of a wakeup.
         *
         * Reset releaseOk if somebody woke us before we removed ourselves -
         * they'll have set it to false. */
        (void)GsAtomicFetchOrU32(state, LW_FLAG_RELEASE_OK32);

        /*
         * Now wait for the scheduled wakeup, otherwise our ->lwWaiting would
         * get reset at some inconvenient point later. Most of the time this
         * will immediately return. */
        for (;;) {
            /* "false" means cannot accept cancel/die interrupt here. */
            thrd->Sleep();
            if (thrd->IsWaitLwlock() == false) {
                break;
            }
        }
    }

#ifdef LOCK_DEBUG
    {
        /* not waiting anymore */
        StorageAssert(GsAtomicFetchSubU32(&lock->nwaiters, 1) < MAX_BACKENDS);
    }
#endif
}

static void LWLockLogHoldLocks()
{
    StringInfoData string;
    if (unlikely(!string.init())) {
        ErrLog(DSTORE_WARNING, MODULE_LOCK, ErrMsg("Out of memory when log wait and hold locks."));
        return;
    }

    RetStatus ret = string.append("Wait lock: Lock addr:%p, mode:%d\n",
        thrd->GetCore()->lockWaiter.waitLock,
        thrd->GetCore()->lockWaiter.mode);
    if (STORAGE_FUNC_FAIL(string.append("Wait lock: Lock addr:%p, mode:%d\n",
            thrd->GetCore()->lockWaiter.waitLock,
            thrd->GetCore()->lockWaiter.mode)) ||
        STORAGE_FUNC_FAIL(string.append("Hold locks:\n"))) {
        DstorePfreeExt(string.data);
        ErrLog(DSTORE_WARNING, MODULE_LOCK, ErrMsg("Out of memory when log wait and hold locks."));
        return;
    }

    for (int i = 0; i < thrd->lwlockContext.num_held_lwlocks; i++) {
        ret = string.append("Lock addr:%p, mode:%d, ",
            thrd->lwlockContext.held_lwlocks[i].lock,
            static_cast<int>(thrd->lwlockContext.held_lwlocks[i].mode));
        if (STORAGE_FUNC_FAIL(ret)) {
            DstorePfreeExt(string.data);
            ErrLog(DSTORE_WARNING, MODULE_LOCK, ErrMsg("Out of memory when log wait and hold locks."));
            return;
        }
    }

    ret = string.append("\n");
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_WARNING, MODULE_LOCK, ErrMsg("Out of memory when log wait and hold locks."));
    } else {
        ErrLog(DSTORE_WARNING, MODULE_LOCK, ErrMsg("%s", string.data));
    }
    DstorePfreeExt(string.data);
}

const float NEED_UPDATE_LOCKID_QUEUE_SLOT = 0.6;
const long int MICRO_SECS_PER_SEC = 1000 * 1000;
const long int MAX_PREEMPT_WAIT_TIME = 5 * MICRO_SECS_PER_SEC;
const long int DUMP_LOCK_WAIT_TIME = 10 * 60 * MICRO_SECS_PER_SEC;

static bool LWLockWaitWithDisallowPreempt(LWLock *lock)
{
    bool needAllowPreempt = false;
    uint64 startWaitTime = GetSystemTimeInMicrosecond();
    long int nextWaitTimeout = MAX_PREEMPT_WAIT_TIME;
    bool needTimeoutDump = true;
    for (;;) {
        struct timespec timeout = {
            .tv_sec = static_cast<__time_t>(nextWaitTimeout / MICRO_SECS_PER_SEC),
            .tv_nsec = 0
        };

        thrd->Sleep(&timeout);
        if (!thrd->IsWaitLwlock()) {
            break;
        }
        uint64 wakeupTime = GetSystemTimeInMicrosecond();
        if (wakeupTime - startWaitTime > MAX_PREEMPT_WAIT_TIME) {
            uint32 oldState = GsAtomicFetchOrU32(GetStateLow32(lock), LW_VAL_DISALLOW_PREEMPT32);
            if ((oldState & LW_VAL_DISALLOW_PREEMPT32) == 0) {
                needAllowPreempt = true;
            }
        }
        if ((wakeupTime - startWaitTime > DUMP_LOCK_WAIT_TIME) && needTimeoutDump) {
            int groupId = lock->groupId;
            ErrLog(DSTORE_LOG, MODULE_LOCK,
                ErrMsg("Current thread wait for lwlock(%s) too long, lock state: 0x%lx.",
                    groupId < static_cast<int>(LWLOCK_GROUP_MAX) ? GetLWLockGroupName(groupId) : "unknown",
                    GsAtomicReadU64(&lock->state)));
            needTimeoutDump = false;
        }

        nextWaitTimeout = MAX_PREEMPT_WAIT_TIME -
            (static_cast<long int>(wakeupTime - startWaitTime) % MAX_PREEMPT_WAIT_TIME);
    }
    return needAllowPreempt;
}

static void CheckSelfThreadDeadlock(LWLock *lock, LWLockMode mode)
{
    /* Check if there is a self-deadlock. */
    if ((mode == LW_EXCLUSIVE) && LWLockHeldByMe(lock)) {
        LWLockLogHoldLocks();
        StorageReleasePanic(true, MODULE_LOCK, ErrMsg("self thread lwlock deadlock."));
    } else if ((mode == LW_SHARED) && LWLockHeldByMeInMode(lock, LW_EXCLUSIVE)) {
        LWLockLogHoldLocks();
        StorageReleasePanic(true, MODULE_LOCK, ErrMsg("self thread lwlock deadlock."));
    }
}

/*
 * LWLockAcquire - acquire a lightweight lock in the specified mode
 *
 * If the lock is not available, sleep until it is.
 *
 * Side effect: cancel/die interrupts are held off until lock release.
 */
template <LWLockMode mode>
HOTFUNCTION void LWLockAcquire(LWLock *lock, UNUSE_PARAM const char *fileName, UNUSE_PARAM int lineNumber,
                        UNUSE_PARAM const char *functionName)
{
    bool needAllowPreempt = false;
    bool wakedFromQueue = false;

    StorageAssert(mode == LW_SHARED || mode == LW_EXCLUSIVE);

    PRINT_LWDEBUG("LWLockAcquire", lock, mode);

    /* Ensure we will have room to remember the lock */
    if (unlikely(thrd->lwlockContext.num_held_lwlocks >= MAX_SIMUL_LWLOCKS)) {
        StorageReleasePanic(
            true, MODULE_LOCK,
            ErrMsg("Unable to remember the lock due to the num of current locks reaching the maximum."));
    }

    /*
     * Lock out cancel/die interrupts until we exit the code section protected
     * by the LWLock.  This ensures that interrupts will not interfere with
     * manipulations of data structures in shared memory.
     */
    HOLD_INTERRUPTS();

    /*
     * Loop here to try to acquire lock after each time we are signaled by
     * LWLockRelease.
     *
     * NOTE: it might seem better to have LWLockRelease actually grant us the
     * lock, rather than retrying and possibly having to go back to sleep. But
     * in practice that is no good because it means a process swap for every
     * lock acquisition when two or more processes are contending for the same
     * lock.  Since LWLocks are normally used to protect not-very-long
     * sections of computation, a process needs to be able to acquire and
     * release the same lock many times during a single CPU time slice, even
     * in the presence of contention.  The efficiency of being able to do that
     * outweighs the inefficiency of sometimes wasting a process dispatch
     * cycle because the lock is not free when a released waiter finally gets
     * to run.	See pgsql-hackers archives for 29-Dec-01.
     */
    StorageStat *stat = g_storageInstance->GetStat();
    for (;;) {
        bool mustwait = false;

        /*
         * Try to grab the lock the first time, we're not in the waitqueue
         * yet/anymore.
         */
        mustwait = LWLockAttemptLock<mode>(lock, needAllowPreempt, wakedFromQueue);
        if (!mustwait) {
            /* XXX: remove before commit? */
            LOG_LWDEBUG("LWLockAcquire", lock, "immediately acquired lock");
            break; /* got the lock */
        }

        /*
         * Ok, at this point we couldn't grab the lock on the first try. We
         * cannot simply queue ourselves to the end of the list and wait to be
         * woken up because by now the lock could long have been released.
         * Instead add us to the queue and try to grab the lock again. If we
         * succeed we need to revert the queuing and be happy, otherwise we
         * recheck the lock. If we still couldn't grab it, we know that the
         * other lock will see our queue entries when releasing since they
         * existed before we checked for the lock.
         */
        /* add to the queue */
        LWLockQueueSelf(lock, mode);

        mustwait = LWLockAttemptLock<mode>(lock, needAllowPreempt, wakedFromQueue);
        /* ok, grabbed the lock the second time round, need to undo queueing */
        if (!mustwait) {
            LOG_LWDEBUG("LWLockAcquire", lock, "acquired, undoing queue");

            LWLockDequeueSelf(lock, mode);
            break;
        }

        /*
         * Wait until awakened.
         *
         * Since we share the process wait semaphore with the regular lock
         * manager and ProcWaitForSignal, and we may need to acquire an LWLock
         * while one of those is pending, it is possible that we get awakened
         * for a reason other than being signaled by LWLockRelease. If so,
         * loop back and wait again.  Once we've gotten the LWLock,
         * re-increment the sema by the number of additional signals received,
         * so that the lock manager or signal manager will see the received
         * signal when it next waits.
         */
        LOG_LWDEBUG("LWLockAcquire", lock, "waiting");

        TRACE_POSTGRESQL_LWLOCK_WAIT_START(DSTORE_T_NAME(lock), mode);
        stat->ReportLWLockStat(DSTORE::StmtDetailType::LWLOCK_WAIT_START, mode, lock->groupId);
        stat->m_reportWaitEvent(static_cast<uint32_t>(lock->groupId |
            OPTUTIL_GSSTAT_WAIT_LWLOCK));

        /* Check for self thread deadlock before waiting. */
        if (!wakedFromQueue) {
            CheckSelfThreadDeadlock(lock, mode);
        }

        if (mode == LW_EXCLUSIVE) {
            needAllowPreempt |= LWLockWaitWithDisallowPreempt(lock);
        } else {
            for (;;) {
                /* "false" means cannot accept cancel/die interrupt here. */
                thrd->Sleep();
                if (thrd->IsWaitLwlock() == false) {
                    break;
                }
            }
        }

        wakedFromQueue = true;
        /* Retrying, allow LWLockRelease to release waiters again. */
        (void)GsAtomicFetchOrU32(GetStateHigh32(lock), LW_FLAG_RELEASE_OK32);

#ifdef LOCK_DEBUG
        {
            /* not waiting anymore */
            StorageAssert(GsAtomicFetchSubU32(&lock->nwaiters, 1) < MAX_BACKENDS);
        }
#endif
        TRACE_POSTGRESQL_LWLOCK_WAIT_DONE(DSTORE_T_NAME(lock), mode);
        stat->m_reportWaitEvent(OPTUTIL_GSSTAT_WAIT_EVENT_END);
        stat->ReportLWLockStat(DSTORE::StmtDetailType::LWLOCK_WAIT_END, mode, lock->groupId);
        LOG_LWDEBUG("LWLockAcquire", lock, "awakened");

        /* Now loop back and try to acquire lock again. */
    }
    TRACE_POSTGRESQL_LWLOCK_ACQUIRE(DSTORE_T_NAME(lock), mode);

    AddToThreadHeldLockList(lock, mode, fileName, lineNumber, functionName);
}
template void LWLockAcquire<LW_EXCLUSIVE>(LWLock *lock, UNUSE_PARAM const char *fileName,
                                               UNUSE_PARAM int lineNumber, UNUSE_PARAM const char *functionName);
template void LWLockAcquire<LW_SHARED>(LWLock *lock, UNUSE_PARAM const char *fileName, UNUSE_PARAM int lineNumber,
                                            UNUSE_PARAM const char *functionName);
/*
 * LWLockConditionalAcquire - acquire a lightweight lock in the specified mode
 *
 * If the lock is not available, return FALSE with no side-effects.
 *
 * If successful, cancel/die interrupts are held off until lock release.
 */
bool LWLockConditionalAcquireDebug(LWLock *lock, LWLockMode mode, UNUSE_PARAM const char *fileName,
                                   UNUSE_PARAM int lineNumber, UNUSE_PARAM const char *functionName)
{
    bool mustwait = false;

    StorageAssert(mode == LW_SHARED || mode == LW_EXCLUSIVE);

    PRINT_LWDEBUG("LWLockConditionalAcquire", lock, mode);

    /* Ensure we will have room to remember the lock */
    if (unlikely(thrd->lwlockContext.num_held_lwlocks >= MAX_SIMUL_LWLOCKS)) {
        StorageReleasePanic(
            true, MODULE_LOCK,
            ErrMsg("Unable to remember the lock due to the num of current locks reaching the maximum."));
    }

    /*
     * Lock out cancel/die interrupts until we exit the code section protected
     * by the LWLock.  This ensures that interrupts will not interfere with
     * manipulations of data structures in shared memory.
     */
    HOLD_INTERRUPTS();

    /* Check for the lock */
    if (mode == LW_EXCLUSIVE) {
        mustwait = LWLockAttemptLock<LW_EXCLUSIVE>(lock, false, false);
    } else {
        mustwait = LWLockAttemptLock<LW_SHARED>(lock, false, false);
    }
    if (mustwait) {
        /* Failed to get lock, so release interrupt holdoff */
        RESUME_INTERRUPTS();
        LOG_LWDEBUG("LWLockConditionalAcquire", lock, "failed");
        TRACE_POSTGRESQL_LWLOCK_CONDACQUIRE_FAIL(DSTORE_T_NAME(lock), mode);
    } else {
        AddToThreadHeldLockList(lock, mode, fileName, lineNumber, functionName);
        TRACE_POSTGRESQL_LWLOCK_CONDACQUIRE(DSTORE_T_NAME(lock), mode);
    }
    return !mustwait;
}

/*
 * LWLockAcquireNoQueue - try a lightweight lock in exclusive lock first. If failed,
 * get shared lock. This call returns true if either exclusive or shared lock is
 * available and acquiredLockMode returns the lock mode acquired. False is returned
 * if no lock is available.
 *
 * This function is similar to LWLockConditionalAcquire in that both functions don't
 * put itself into queue when the desired lock is not immediately available but this
 * function has no lock mode requirement. It first tries the exclusive lock mode then
 * the shared mode.
 */
bool LWLockConditionalAcquireAnyModeDebug(LWLock *lock, LWLockMode *acquiredLockMode,
        UNUSE_PARAM const char *fileName, UNUSE_PARAM int lineNumber, UNUSE_PARAM const char *functionName)
{
    StorageAssert(acquiredLockMode != nullptr);
    bool lockFree = false;
    bool exclusiveLocked = false;
    bool sharedLocked = false;
    bool noDoCas = true;
    uint32 oldState;

    /* Ensure we will have room to remember the lock */
    if (unlikely(thrd->lwlockContext.num_held_lwlocks >= MAX_SIMUL_LWLOCKS)) {
        StorageReleasePanic(
            true, MODULE_LOCK,
            ErrMsg("Unable to remember the lock due to the num of current locks reaching the maximum."));
    }

    /*
     * Lock out cancel/die interrupts until we exit the code section protected
     * by the LWLock.  This ensures that interrupts will not interfere with
     * manipulations of data structures in shared memory.
     */
    HOLD_INTERRUPTS();

    /*
     * Read once outside the loop, later iterations will get the newer value
     * via compare & exchange.
     */
    /* lock exclusivily take precedence, also here direct +1 at first, at the same time get the oldState */
    oldState = LWLockStateLow32(__atomic_add_fetch(&lock->state, 1, __ATOMIC_RELAXED));

    /* loop till we determine whether we could acquire the lock */
    for (;;) {
        uint32 desiredState = oldState;

        /* the lwlock two bit's LW_VAL_XXX flag only have three state, that is
         * 00 (not lockd),
         * 01 (shared locked)
         * 10 (exclusive locked)
         * 11 (fatal error, should not exist) */
        lockFree = ((oldState & LW_LOCK_MASK32) == 0);
        sharedLocked = ((oldState & LW_VAL_SHARED32) != 0);
        exclusiveLocked = ((oldState & LW_VAL_EXCLUSIVE32) != 0);

        if (lockFree) {
            desiredState += LW_VAL_EXCLUSIVE32;
        } else if (sharedLocked) {
            /* luckly, got shared lock */
            break;
        } else if (exclusiveLocked) {
            /* no lock is available, decrease shared count by CAS */
            desiredState -= 1;
        } else {
            RESUME_INTERRUPTS();
            /* never happened, or already have a fatal error */
            storage_set_error(LWLOCK_ERROR_NOT_AVAIL_NAME, DSTORE_T_NAME(lock));
            return false;
        }

        noDoCas = false;
        /*
         * Attempt to swap in the state we are expecting. If we didn't see
         * lock to be free, that's just the old value. If we saw it as free,
         * we'll attempt to mark it acquired. The reason that we always swap
         * in the value is that this doubles as a memory barrier. We could try
         * to be smarter and only swap in values if we saw the lock as free,
         * but benchmark haven't shown it as beneficial so far.
         *
         * Retry if the value changed since we last looked at it.
         */
        if (GsAtomicCompareExchangeU32(GetStateLow32(lock), &oldState, desiredState)) {
            if (exclusiveLocked) {
                RESUME_INTERRUPTS();
                LOG_LWDEBUG("LWLockAcquireNoQueueDebug", lock, "no lock");
                /* no lock is available */
                return false; /* someobdy else has the lock */
            }
            /* ok, got lock. because the CAS means we want to lock the lwlock in any mode.
             * this is different with LWLockAttemptLock */
            break;
        }
    }
    if (lockFree) {
        *acquiredLockMode = LW_EXCLUSIVE;
        /* we got the lwlock with exclusive mode, so can guarantee decrease shared count is safety */
        (void)__atomic_sub_fetch(&lock->state, 1, __ATOMIC_RELAXED);
    } else {
        *acquiredLockMode = LW_SHARED;
    }

    thrd->lwlockContext.held_lwlocks[thrd->lwlockContext.num_held_lwlocks].lock = lock;
    thrd->lwlockContext.held_lwlocks[thrd->lwlockContext.num_held_lwlocks++].mode = *acquiredLockMode;

    if (noDoCas) {
        __atomic_thread_fence(__ATOMIC_ACQUIRE);
    }
    return true;
}

/*
 * LWLockAcquireOrWait - Acquire lock, or wait until it's free
 *
 * The semantics of this function are a bit funky.	If the lock is currently
 * free, it is acquired in the given mode, and the function returns true.  If
 * the lock isn't immediately free, the function waits until it is released
 * and returns false, but does not acquire the lock.
 *
 * This is currently used for WALWriteLock: when a backend flushes the WAL,
 * holding WALWriteLock, it can flush the commit records of many other
 * backends as a side-effect.  Those other backends need to wait until the
 * flush finishes, but don't need to acquire the lock anymore.  They can just
 * wake up, observe that their records have already been flushed, and return.
 */
bool LWLockAcquireOrWait(LWLock *lock, LWLockMode mode)
{
    bool mustwait = false;

    StorageAssert(mode == LW_SHARED || mode == LW_EXCLUSIVE);

    PRINT_LWDEBUG("LWLockAcquireOrWait", lock, mode);

    /* Ensure we will have room to remember the lock */
    if (unlikely(thrd->lwlockContext.num_held_lwlocks >= MAX_SIMUL_LWLOCKS)) {
        StorageReleasePanic(
            true, MODULE_LOCK,
            ErrMsg("Unable to remember the lock due to the num of current locks reaching the maximum."));
    }

    /*
     * Lock out cancel/die interrupts until we exit the code section protected
     * by the LWLock.  This ensures that interrupts will not interfere with
     * manipulations of data structures in shared memory.
     */
    HOLD_INTERRUPTS();

    if (mode == LW_EXCLUSIVE) {
        mustwait = LWLockAttemptLock<LW_EXCLUSIVE>(lock, false, false);
    } else {
        mustwait = LWLockAttemptLock<LW_SHARED>(lock, false, false);
    }
    if (mustwait) {
        g_storageInstance->GetStat()->m_reportWaitEvent(static_cast<uint32_t>(lock->groupId |
            OPTUTIL_GSSTAT_WAIT_LWLOCK));
        TRACE_POSTGRESQL_LWLOCK_WAIT_START(DSTORE_T_NAME(lock), mode);

        LWLockQueueSelf(lock, LW_WAIT_UNTIL_FREE);

        if (mode == LW_EXCLUSIVE) {
            mustwait = LWLockAttemptLock<LW_EXCLUSIVE>(lock, false, false);
        } else {
            mustwait = LWLockAttemptLock<LW_SHARED>(lock, false, false);
        }
        if (mustwait) {
            /*
             * Wait until awakened.  Like in LWLockAcquire, be prepared for bogus
             * wakups, because we share the semaphore with ProcWaitForSignal.
             */
            LOG_LWDEBUG("LWLockAcquireOrWait", lock, "waiting");

            for (;;) {
                /* "false" means cannot accept cancel/die interrupt here. */
                thrd->Sleep();
                if (!thrd->IsWaitLwlock()) {
                    break;
                }
            }

#ifdef LOCK_DEBUG
            {
                /* not waiting anymore */
                StorageAssert(GsAtomicFetchSubU32(&lock->nwaiters, 1) < MAX_BACKENDS);
            }
#endif
            LOG_LWDEBUG("LWLockAcquireOrWait", lock, "awakened");
        } else {
            LOG_LWDEBUG("LWLockAcquireOrWait", lock, "acquired, undoing queue");

            /*
             * Got lock in the second attempt, undo queueing. We need to
             * treat this as having successfully acquired the lock, otherwise
             * we'd not necessarily wake up people we've prevented from
             * acquiring the lock.
             */
            LWLockDequeueSelf(lock, mode);
        }
        TRACE_POSTGRESQL_LWLOCK_WAIT_DONE(DSTORE_T_NAME(lock), mode);
        if (mustwait) {
            g_storageInstance->GetStat()->m_reportWaitEventFailed(static_cast<uint32_t>(lock->groupId |
                OPTUTIL_GSSTAT_WAIT_LWLOCK));
        }
        g_storageInstance->GetStat()->m_reportWaitEvent(OPTUTIL_GSSTAT_WAIT_EVENT_END);
    }

    if (mustwait) {
        /* Failed to get lock, so release interrupt holdoff */
        RESUME_INTERRUPTS();
        LOG_LWDEBUG("LWLockAcquireOrWait", lock, "failed");
        TRACE_POSTGRESQL_LWLOCK_WAIT_UNTIL_FREE_FAIL(DSTORE_T_NAME(lock), mode);
    } else {
        LOG_LWDEBUG("LWLockAcquireOrWait", lock, "succeeded");
        /* Add lock to list of locks held by this backend */
        thrd->lwlockContext.held_lwlocks[thrd->lwlockContext.num_held_lwlocks].lock = lock;
        thrd->lwlockContext.held_lwlocks[thrd->lwlockContext.num_held_lwlocks].mode = mode;
        thrd->lwlockContext.num_held_lwlocks++;
        TRACE_POSTGRESQL_LWLOCK_WAIT_UNTIL_FREE(DSTORE_T_NAME(lock), mode);
    }

    return !mustwait;
}

template <LWLockMode mode>
HOTFUNCTION bool UpDateNewStat(uint64 &newstate, LWLock *lock)
{
    if (mode == LW_EXCLUSIVE) {
        /* ENABLE_THREAD_CHECK only, Must release vector clock info to other
         * threads before unlock */
        TsAnnotateRWLockReleased(&lock->rwlock, 1);
        newstate = GsAtomicSubFetchU64(&lock->state, LW_VAL_EXCLUSIVE64);
        /* nobody else can have that kind of lock */
        StorageAssert(!(newstate & LW_VAL_EXCLUSIVE64));

    } else {
        /* ENABLE_THREAD_CHECK only, Must release vector clock info to other
         * threads before unlock */
        TsAnnotateRWLockReleased(&lock->rwlock, 0);
        newstate = GsAtomicSubFetchU64(&lock->state, 1);
        /* If the shared locked flag is already cleared, ok, we are not the one really release the shared lwlock.
         * why not test shared count is 1->0? That when locking the lwlock, shared count will +1 unconditionally.
         * the shared lwlock maybe released by other one, that say LW_VAL_SHARED bit already is zero, nothing to do */
        if ((newstate & LW_VAL_SHARED64) == 0) {
            return true;
        }

        /* Consider one case, someone acquire shared at my shared lock release moment, that guy will +1 shared count,
         * make trouble we test shared count ->0, But please attention!!! This moment, the flag of LW_VAL_SHARED is
         * not cleared, so that guy should be locked lwlock with shared mode, eh..., CLEAR LW_VAL_SHARED action hand
         * over to that guy. I can actually leave safely, but I still doing a checkWaiters, have no side effects */

        /* If the shared count is zero (1->0 by me). It's time to clear the shared locked flag by CAS. If not, still
         * doing a checkWaiters, have no side effects */
        if ((newstate & LW_LOCK_SHARE_COUNT_MASK64) == 0) {
            /* want bit at LW_VAL_SHARED32 from 1 to 0, if success, ok, really release shared lwlock and wake others,
             * if fail, also ok, other one re-locked the shared lwlock */
            uint32 oldstate32 = LWLockStateLow32(newstate);
            gs_atomic_uint32 *state32Ptr = GetStateLow32(lock);
            bool unsetSucc = GsAtomicCompareExchangeU32(state32Ptr, &oldstate32, oldstate32 - LW_VAL_SHARED32);
            if (!unsetSucc && ((oldstate32 & LW_VAL_DISALLOW_PREEMPT32) != 0) &&
                ((oldstate32 & LW_LOCK_SHARE_COUNT_MASK32) == 0) && ((oldstate32 & LW_VAL_SHARED32) != 0)) {
                StorageAssert((newstate & LW_VAL_DISALLOW_PREEMPT64) == 0);
                StorageAssert((oldstate32 & LW_VAL_EXCLUSIVE32) == 0);
                unsetSucc = GsAtomicCompareExchangeU32(state32Ptr, &oldstate32, oldstate32 - LW_VAL_SHARED32);
            }
            if (unsetSucc) {
                /* really release the lwlock, so that others can lock it,
                 * nobody else can have that kind of lock at meantime */
                LOG_LWDEBUG("LWLockRelease", lock, "Shared locker unlock the lwlock");
                StorageAssert(!(oldstate32 & LW_VAL_EXCLUSIVE32));
                newstate = GsAtomicReadU64(&lock->state);
            } else {
                /* ok, one other exchange success, successfully removed SHARED LOCKED status, leave wakeup action to
                 * that one, we can go to exit directly. That is to say, that one is who really unlock shared lock */
                return true;
            }
        }
    }
    return false;
}
template bool UpDateNewStat<LW_EXCLUSIVE>(uint64 &newstate, LWLock *lock);
template bool UpDateNewStat<LW_SHARED>(uint64 &newstate, LWLock *lock);

/*
 * LWLockRelease - release a previously acquired lock
 */
HOTFUNCTION void LWLockRelease(LWLock *lock) noexcept
{
    LWLockMode mode = LW_EXCLUSIVE;
    uint64 newstate;
    bool checkWaiters = false;
    int i;

    /* Remove lock from list of locks held.  Usually, but not always, it will
     * be the latest-acquired lock; so search array backwards. */
    LWLockHandle* handle = thrd->lwlockContext.held_lwlocks;
    for (i = thrd->lwlockContext.num_held_lwlocks; --i >= 0;) {
        if (lock == handle[i].lock) {
            mode = handle[i].mode;
            break;
        }
    }
    if (unlikely(i < 0)) {
        /* Try to release a lock doesn't belong to me. */
        ErrLog(DSTORE_PANIC, MODULE_LOCK,
            ErrMsg("Tried to release a lock not belonging to me, error \"%s\"", StorageGetMessage()));
        return;
    }
    thrd->lwlockContext.num_held_lwlocks--;
    for (; i < thrd->lwlockContext.num_held_lwlocks; i++) {
        handle[i] = handle[i + 1];
    }

    PRINT_LWDEBUG("LWLockRelease", lock, mode);

    bool needExit = false;
    if (mode == LW_EXCLUSIVE) {
        needExit = UpDateNewStat<LW_EXCLUSIVE>(newstate, lock);
    } else {
        needExit = UpDateNewStat<LW_SHARED>(newstate, lock);
    }
    if (!needExit) {
        /* We're still waiting for backends to get scheduled, don't wake them up again. */
        checkWaiters =
            ((newstate & (LW_FLAG_HAS_WAITERS64 | LW_FLAG_RELEASE_OK64)) ==
            (LW_FLAG_HAS_WAITERS64 | LW_FLAG_RELEASE_OK64)) && ((newstate & LW_LOCK_MASK64) == 0);

        /* As waking up waiters requires the spinlock to be acquired, only do so if necessary. */
        if (checkWaiters) {
            LOG_LWDEBUG("LWLockRelease", lock, "releasing waiters");
            LWLockWakeup(lock);
        }
    }

    TRACE_POSTGRESQL_LWLOCK_RELEASE(DSTORE_T_NAME(lock));

    /* Now okay to allow cancel/die interrupts. */
    RESUME_INTERRUPTS();
}

/*
 * LWLockReleaseAll - release all currently-held locks
 *
 * Used to clean up after ereport(ERROR). An important difference between this
 * function and retail LWLockRelease calls is that t_thrd.int_cxt.InterruptHoldoffCount is
 * unchanged by this operation.  This is necessary since t_thrd.int_cxt.InterruptHoldoffCount
 * has been set to an appropriate level earlier in error recovery. We could
 * decrement it below zero if we allow it to drop for each released lock!
 */
void LWLockReleaseAll(void)
{
    while (thrd->lwlockContext.num_held_lwlocks > 0) {
        HOLD_INTERRUPTS(); /* match the upcoming RESUME_INTERRUPTS */
        LWLockRelease(thrd->lwlockContext.held_lwlocks[thrd->lwlockContext.num_held_lwlocks - 1].lock);
    }
}

/*
 * LWLockHeldByMe - test whether my process currently holds a lock
 *
 * This is meant as debug support only.  We currently do not distinguish
 * whether the lock is held shared or exclusive.
 */
HOTFUNCTION bool LWLockHeldByMe(LWLock *lock)
{
    for (int i = 0; i < thrd->lwlockContext.num_held_lwlocks; i++) {
        if (thrd->lwlockContext.held_lwlocks[i].lock == lock) {
            return true;
        }
    }
    return false;
}

/*
 * LWLockHeldByMeInMode - test whether my process holds a lock in given mode
 *
 * This is meant as debug support only.
 */
bool LWLockHeldByMeInMode(LWLock *lock, LWLockMode mode)
{
    for (int i = 0; i < thrd->lwlockContext.num_held_lwlocks; i++) {
        if (thrd->lwlockContext.held_lwlocks[i].lock == lock &&
            thrd->lwlockContext.held_lwlocks[i].mode == mode) {
            return true;
        }
    }
    return false;
}

/* reset a lwlock */
void LWLockReset(LWLock *lock)
{
    GsAtomicInitU64(&lock->state, LW_FLAG_RELEASE_OK64);

    /* ENABLE_THREAD_CHECK only */
    TsAnnotateRWLockDestroy(&lock->listlock);
    TsAnnotateRWLockCreate(&lock->listlock);
    TsAnnotateRWLockDestroy(&lock->rwlock);
    TsAnnotateRWLockCreate(&lock->rwlock);

#ifdef LOCK_DEBUG
    GsAtomicInitU32(&lock->nwaiters, 0);
    lock->owner = nullptr;
#endif
    DListInit(&lock->waiters);
}

/* get the address of num_held_lwlocks */
int *GetHeldLwlocksNum(void)
{
    return &thrd->lwlockContext.num_held_lwlocks;
}

/* get the max number of held lwlocks */
uint32 GetHeldLwlocksMaxnum(void)
{
    return static_cast<uint32>(MAX_SIMUL_LWLOCKS);
}

/* get lwlocks now held */
void *GetHeldLwlocks(void)
{
    return static_cast<void *>(thrd->lwlockContext.held_lwlocks);
}

void CheckLwLockLeak()
{
    /* LWLock must be released when transaction end */
    bool hasLeak = false;
    for (int i = 0; i < thrd->lwlockContext.num_held_lwlocks; i++) {
        ErrLog(DSTORE_ERROR, MODULE_LOCK,
            ErrMsg("lwlock leak found for %s.", DSTORE_T_NAME(thrd->lwlockContext.held_lwlocks[i].lock)));
        hasLeak = true;
    }
    StorageReleasePanic(hasLeak, MODULE_LOCK, ErrMsg("lwlock leak found."));
}

LWLockMode GetHeldLWLockMode(LWLock *lock)
{
    for (int i = 0; i < thrd->lwlockContext.num_held_lwlocks; i++) {
        if (thrd->lwlockContext.held_lwlocks[i].lock == lock) {
            return thrd->lwlockContext.held_lwlocks[i].mode;
        }
    }
    storage_set_error(LWLOCK_ERROR_NOT_AVAIL_NAME, DSTORE_T_NAME(lock));
    return static_cast<LWLockMode>(0); /* keep compiler silence */
}

char *LWLockDiagnose::GetLWLockStatus(void)
{
    StringInfoData string;
    if (unlikely(!string.init())) {
        ErrLog(DSTORE_WARNING, MODULE_LOCK, ErrMsg("Out of memory when log lwlock status"));
        return nullptr;
    }
    ThreadCoreMgr::ThreadIterator iter(g_storageInstance->GetThreadCoreMgr());
    ThreadCore *core = nullptr;

    RetStatus ret = string.append("%-17s %17s  %-60s\n", "Thread id", "Wait lock", "Hold locks");
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_WARNING, MODULE_LOCK, ErrMsg("Out of memory when log lwlock status"));
        DstorePfreeExt(string.data);
        return nullptr;
    }

    while ((core = iter.GetNextThreadCore()) != nullptr) {
        StorageAssert(core->threadContext != nullptr);
        LWlockContext *lwlockContext = &core->threadContext->lwlockContext;
        LWLockWaiter *waiter = &core->lockWaiter;
        ret = string.append("%-17lu %15p:%1d  ", core->pid, waiter->waitLock, waiter->mode);
        if (STORAGE_FUNC_FAIL(ret)) {
            ErrLog(DSTORE_WARNING, MODULE_LOCK, ErrMsg("Out of memory when log lwlock status"));
            DstorePfreeExt(string.data);
            return nullptr;
        }
        for (int i = 0; i < lwlockContext->num_held_lwlocks; i++) {
            ret = string.append("%p:%d ", lwlockContext->held_lwlocks[i].lock,
                static_cast<int>(lwlockContext->held_lwlocks[i].mode));
            if (STORAGE_FUNC_FAIL(ret)) {
                ErrLog(DSTORE_WARNING, MODULE_LOCK, ErrMsg("Out of memory when log lwlock status"));
                DstorePfreeExt(string.data);
                return nullptr;
            }
        }
        ret = string.append("\n");
        if (STORAGE_FUNC_FAIL(ret)) {
            ErrLog(DSTORE_WARNING, MODULE_LOCK, ErrMsg("Out of memory when log lwlock status"));
            DstorePfreeExt(string.data);
            return nullptr;
        }
    }

    return string.data;
}
const char *LWLockDiagnose::GetGroupName(uint32_t group_id)
{
    return GetLWLockGroupName(static_cast<int>(group_id));
}

/*
 * LWLockOwn - obtain ownership of a lock acquired by another thread
 * This function should only be used when a lock is acquired in one thread
 * context and released within another.
 * reference Postgresql.
 */
void LWLockOwn(LWLock *lock, LWLockMode mode, UNUSE_PARAM const char *fileName,
               UNUSE_PARAM int lineNumber, UNUSE_PARAM const char *functionName)
{
    uint64 expectedState;

    /* Ensure we will have room to remember the lock */
    if (unlikely(thrd->lwlockContext.num_held_lwlocks >= MAX_SIMUL_LWLOCKS)) {
        ErrLog(DSTORE_PANIC, MODULE_LOCK, ErrMsg("LWLockOwn too many LWLocks taken"));
    }

    /* Ensure that lock is held */
    expectedState = GsAtomicReadU64(&lock->state);
    if (!(expectedState & LW_LOCK_MASK64)) {
        ErrLog(DSTORE_PANIC, MODULE_LOCK,
            ErrMsg("LWLockOwn lock %s is not held", DSTORE_T_NAME(lock)));
    }

    AddToThreadHeldLockList(lock, mode, fileName, lineNumber, functionName);
    HOLD_INTERRUPTS();
}

/*
 * LWLockDisown - make this thread release the lock record,
 * so that the thread can acquire the lock again.
 * note: the thread of caller must initialize thread context.
 * reference Postgresql.
 */
void LWLockDisown(LWLock *lock)
{
    uint64 expectedState;
    int i;

    /* Ensure that lock is held */
    expectedState = GsAtomicReadU64(&lock->state);
    if (!(expectedState & LW_LOCK_MASK64)) {
        ErrLog(DSTORE_PANIC, MODULE_LOCK,
            ErrMsg("LWLockDisown lock %s is not held", DSTORE_T_NAME(lock)));
    }

    for (i = thrd->lwlockContext.num_held_lwlocks; --i >= 0;) {
        if (lock == thrd->lwlockContext.held_lwlocks[i].lock) {
            break;
        }
    }

    if (i < 0) {
        ErrLog(DSTORE_PANIC, MODULE_LOCK,
            ErrMsg("LWLockDisown lock %s is not held", DSTORE_T_NAME(lock)));
    }

    thrd->lwlockContext.num_held_lwlocks--;
    for (; i < thrd->lwlockContext.num_held_lwlocks; i++) {
        thrd->lwlockContext.held_lwlocks[i] = thrd->lwlockContext.held_lwlocks[i + 1];
    }

    RESUME_INTERRUPTS();
}

/*
 * Add lock to list of locks held by this backend.
 */
void AddToThreadHeldLockList(LWLock *lock, LWLockMode mode, UNUSE_PARAM const char *fileName,
    UNUSE_PARAM int lineNumber, UNUSE_PARAM const char *functionName)
{
    thrd->lwlockContext.held_lwlocks[thrd->lwlockContext.num_held_lwlocks].lock = lock;
    thrd->lwlockContext.held_lwlocks[thrd->lwlockContext.num_held_lwlocks].mode = mode;
    thrd->lwlockContext.num_held_lwlocks++;
}
}
