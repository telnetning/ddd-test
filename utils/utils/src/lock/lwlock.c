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
 * Description: Implement for lwlock
 */
#include <stddef.h>
#if defined POSIX_SEMA
#include <semaphore.h>
#else
// sysv semphore
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/sem.h>
#include <fcntl.h>
#endif
#include <unistd.h>
#include <sys/syscall.h>
#include "lock/lwlock_internal.h"
#include "container/linked_list.h"
#include "defines/common.h"
#include "defines/abort.h"
#include "lock/spinlock.h"
#include "lock/lwlock.h"

typedef struct {
#if defined POSIX_SEMA
    sem_t semId;
#else
    int semId;
#endif
} LWSemaphore;

/* thread info to distinguish different threads */
struct LWThreadCore {
    DListNode listNode; /* next waiter for same LW lock */
    LWLockMode mode;    /* lwlock mode being waited for */
    LWSemaphore sema;   /* OS layer semaphore for block thread which can't obtain lock */
    bool waiting;       /* true if waiting for an LW lock */
    LWlockContext *cxt;
    ThreadInfo info;
    int64_t magic; /* thread local magic, will use to check thread alive or not */
};

inline static void LWLogDebug(SYMBOL_UNUSED const char *func, SYMBOL_UNUSED LWLock *lock, SYMBOL_UNUSED const char *msg)
{
    (void)0;
}

inline static void LWPrintDebug(SYMBOL_UNUSED const char *func, SYMBOL_UNUSED LWLock *lock,
                                SYMBOL_UNUSED LWLockMode mode)
{
    (void)0;
}

static __thread LWThreadCore g_lwCtl = {
    .listNode = {NULL, NULL},
    .mode = LW_MODE_INVAILD,
#if defined POSIX_SEMA
    .sema = {{}},
#else
    .sema = {-1},
#endif
    .waiting = false,
    .cxt = NULL,
    .info = {0},
    .magic = LWLOCK_MAGIC_TAG_INIT,
};

static void LwLockLog(SYMBOL_UNUSED LWLock *lock, SYMBOL_UNUSED int errCode, SYMBOL_UNUSED const ErrInfo *info)
{
    char outStr[512]; // 512: out string max 512 byte
    int ret = snprintf_truncated_s(outStr, sizeof(outStr),
                                   "[lwlock]%s(%zd) "
                                   "state %zx\n",
                                   info->msg, errCode, LWLockAtomicReadState(lock));
    if (ret != -1) {
        (void)write(fileno(stdout), outStr, (size_t)(uint32_t)ret);
    }
}

UTILS_EXPORT
int32_t LwLockInitThreadCore(LWlockContext *cxt)
{
    ASSERT(cxt != NULL);
#if defined POSIX_SEMA
    if (sem_init(&g_lwCtl.sema.semId, 0, 0) == -1) {
#else
    g_lwCtl.sema.semId = semget(IPC_PRIVATE, 1, S_IRUSR | S_IWUSR);
    if (g_lwCtl.sema.semId == -1) {
#endif
        return errno;
    }
    g_lwCtl.cxt = cxt;
    (void)memset_s(cxt, sizeof(LWlockContext), 0, sizeof(LWlockContext));
    g_lwCtl.magic = LWLOCK_MAGIC_TAG;
#if defined __linux__ || defined linux
    g_lwCtl.info.id = syscall(SYS_gettid);
#endif
    g_lwCtl.info.task = &g_lwCtl;
    return 0;
}

UTILS_EXPORT
int32_t LWLockDeInitThreadCore(void)
{
#if defined POSIX_SEMA
    if (sem_destroy(&g_lwCtl.sema.semId) == -1) {
#else
    if (semctl(g_lwCtl.sema.semId, 0, IPC_RMID) == -1) {
#endif
        return errno;
    }
    return 0;
}

UTILS_EXPORT
uint32_t LWLockAtomicReadState(LWLock *lock)
{
    ASSERT(lock != NULL);
    return LWAtomicRead(&lock->state);
}

/*
 * LWLockInitialize - initialize a new lwlock; it's initially unlocked
 */
UTILS_EXPORT
void LWLockInitialize(LWLock *lock)
{
    ASSERT(lock != NULL);
    (void)memset_s(lock, sizeof(LWLock), 0, sizeof(LWLock));
    lock->para.log = LwLockLog;
    LWAtomicWrite(&lock->state, LW_FLAG_RELEASE_OK);
    DListInit(&lock->waiters);
    /* ENABLE_THREAD_CHECK only, Register RWLock in Tsan */
    TS_ANNOTATE_RW_LOCK_CREATE(&lock->rwlock);
    TS_ANNOTATE_RW_LOCK_CREATE(&lock->listlock);
}

/*
 * LWLockInitialize - initialize a new lwlock; it's initially unlocked
 */
UTILS_EXPORT
void LWLockInitializeWithParam(LWLock *lock, LWLockCtlParam *para)
{
    ASSERT(para != NULL);
    LWLockInitialize(lock);
    if (para->log != NULL) {
        lock->para.log = para->log;
    }
}

/*
 * LwLockWait
 */
static void LwLockWait(LWSemaphore *sema)
{
    int errStatus;
    ASSERT(sema != NULL);
#if defined POSIX_SEMA
    do {
        errStatus = sem_wait(&sema->semId);
#else
    struct sembuf sops = {
        .sem_num = 0,
        .sem_op = -1,
        .sem_flg = 0,
    };
    do {
        errStatus = semop(sema->semId, &sops, 1);
#endif
    } while ((errStatus < 0) && (errno == EINTR))

        /* ENABLE_THREAD_CHECK only, Must acquire vector clock info from other
         * thread after got the lock */
        TS_ANNOTATE_HAPPENS_AFTER(sema);

    if (errStatus < 0) {
        Abort();
    }
}

/*
 * LwLockWake
 */
static void LwLockWake(LWSemaphore *sema)
{
    ASSERT(sema != NULL);
    /* ENABLE_THREAD_CHECK only, Must release vector clock info to other
     * threads before unlock */
    TS_ANNOTATE_HAPPENS_BEFORE(sema);

#if defined POSIX_SEMA
    if (sem_post(&sema->semId) == -1) {
#else
    struct sembuf sops = {
        .sem_num = 0,
        .sem_op = 1,
        .sem_flg = 0,
    };
    if (semop(sema->semId, &sops, 1) == -1) {
#endif
        Abort();
    }
}

/*
 * Internal function that tries to atomically acquire the lwlock in the passed
 * in mode. This function will not block waiting for a lock to become free - that's the
 * callers job.
 * Returns true if the lock isn't free and we need to wait.
 */
static bool LWLockAttemptLock(LWLock *lock, LWLockMode mode)
{
    ASSERT(lock != NULL);
    /*
     * Read once outside the loop, later iterations will get the newer value
     * via compare & exchange.
     */
    uint32_t oldState = LWAtomicRead(&lock->state);

    /* loop until we've determined whether we could acquire the lock or not */
    for (;;) {
        uint32_t desiredState;
        bool lockFree = false;

        desiredState = oldState;

        if (mode == LW_EXCLUSIVE) {
            lockFree = ((oldState & LW_LOCK_MASK) == 0);
            if (lockFree) {
                desiredState += LW_VAL_EXCLUSIVE;
            }
        } else {
            lockFree = ((oldState & LW_VAL_EXCLUSIVE) == 0);
            if (lockFree) {
                desiredState += 1;
            }
        }

        /* weak compare_exchange, if failed we will retry, so it's ok.
         *  if failed the lock->state value update to oldState
         */
        if (LWXchg(&lock->state, &oldState, &desiredState)) {
            if (lockFree) {
                /* ENABLE_THREAD_CHECK only, Must acquire vector clock info from other
                 * thread after got the lock */
                if (desiredState & LW_VAL_EXCLUSIVE) {
                    TS_ANNOTATE_RW_LOCK_ACQUIRED(&lock->rwlock, 1);
                } else {
                    TS_ANNOTATE_RW_LOCK_ACQUIRED(&lock->rwlock, 0);
                }

                /* Great! Got the lock. */
                return false;
            } else {
                return true; /* someobdy else has the lock */
            }
        }
    }
    return false;
}

/*
 * Lock the LWLock's wait list against concurrent activity.
 *
 * NB: even though the wait list is locked, non-conflicting lock operations
 * may still happen concurrently.
 *
 * Time spent holding mutex should be short!
 */
static void LwLockWaitListLock(LWLock *lock)
{
    uint32_t oldState;
    ASSERT(lock != NULL);

    for (;;) {
        /* always try once to acquire lock directly */
        oldState = LWAtomicFetchSetBit(&lock->state, LW_FLAG_LOCKED);
        if (!(oldState & LW_FLAG_LOCKED)) {
            /* Got lock */
            break;
        }

        /* and then spin without atomic operations until lock is released */
#ifndef ENABLE_THREAD_CHECK
        SpinDelayStatus delayStatus = {0};
#endif

        while (oldState & LW_FLAG_LOCKED) {
#ifndef ENABLE_THREAD_CHECK
            ContendAdjust(&delayStatus);
#endif
            oldState = LWAtomicRead(&lock->state);
        }
        /*
         * Retry. The lock might obviously already be re-acquired by the time
         * we're attempting to get it again.
         */
    }

    /* ENABLE_THREAD_CHECK only, Must acquire vector clock info from other
     * thread after got the lock */
    TS_ANNOTATE_RW_LOCK_ACQUIRED(&lock->listlock, 1);
}

/*
 * Unlock the LWLock's wait list.
 *
 * Note that it can be more efficient to manipulate flags and release the
 * locks in a single atomic operation.
 */
static void LwLockWaitListUnlock(LWLock *lock)
{
    ASSERT(lock != NULL);
    /* ENABLE_THREAD_CHECK only, Must release vector clock info to other
     * threads before unlock */
    TS_ANNOTATE_RW_LOCK_RELEASED(&lock->listlock, 1);

    SYMBOL_UNUSED uint32_t oldState = LWAtomicFetchClearBit(&lock->state, ~LW_FLAG_LOCKED);
    ASSERT(oldState & LW_FLAG_LOCKED);
}

/*
 * Wakeup all the lockers that currently have a chance to acquire the lock.
 */
static void LwLockWakeup(LWLock *lock)
{
    DListHead wakeup;
    DListMutableIter iter;
    uint32_t desiredState;
    bool newReleaseOk = true;
    bool wokeupSomebody = false;

    ASSERT(lock != NULL);

    DListInit(&wakeup);

    /* lock wait list while collecting backends to wake up */
    LwLockWaitListLock(lock);

    DLIST_MODIFY_FOR_EACH(iter, &lock->waiters)
    {
        LWThreadCore *waiter = DLIST_CONTAINER(LWThreadCore, listNode, iter.cur);
        if (wokeupSomebody && waiter->mode == LW_EXCLUSIVE) {
            continue;
        }

        DListDelete(iter.cur);
        DListPushTail(&wakeup, iter.cur);

        if (waiter->mode == LW_WAIT_UNTIL_FREE) {
            /* A waiter only want the lock state is free, so can wake it */
            continue;
        }

        /* Prevent additional wakeups until retryer gets to run. Backends
         * that are just waiting for the lock to become free don't retry
         * automatically. */
        newReleaseOk = false;
        /* Don't wakeup (further) exclusive locks. */
        wokeupSomebody = true;

        /*
         * Once we've woken up an exclusive lock, there's no point in waking
         * up anybody else.
         */
        if (waiter->mode == LW_EXCLUSIVE) {
            break;
        }
    }

    ASSERT(DListIsEmpty(&wakeup) || (LWAtomicRead(&lock->state) & LW_FLAG_HAS_WAITERS));

    /* ENABLE_THREAD_CHECK only, Must release vector clock info to other
     * threads before unlock */
    TS_ANNOTATE_RW_LOCK_RELEASED(&lock->listlock, 1);

    /* unset required flags, and release lock, in one fell swoop */
    uint32_t oldState = LWAtomicRead(&lock->state);
    for (;;) {
        desiredState = oldState;
        /* compute desired flags */
        if (newReleaseOk) {
            desiredState |= LW_FLAG_RELEASE_OK;
        } else {
            desiredState &= ~LW_FLAG_RELEASE_OK;
        }

        if (DListIsEmpty(&wakeup)) {
            desiredState &= ~LW_FLAG_HAS_WAITERS;
        }

        desiredState &= ~LW_FLAG_LOCKED; // release lock
        /* Here LWXchg has a release semantic, so need barrier with __ATOMIC_RELEASE, But LWXchg more used
         * as acquire semantic, so here add a LWBarrier to prevent reorder memory access above after lock release */
        LWBarrier();
        if (LWXchg(&lock->state, &oldState, &desiredState)) {
            break;
        }
    }

    /* Awaken any waiters I removed from the queue. */
    DLIST_MODIFY_FOR_EACH(iter, &wakeup)
    {
        LWThreadCore *waiter = DLIST_CONTAINER(LWThreadCore, listNode, iter.cur);
        LWLogDebug("LWLockRelease", lock, "release waiter");
        DListDelete(iter.cur);
        /*
         * Guarantee that lwWaiting being unset only becomes visible once the
         * unlink from the link has completed. Otherwise the target backend
         * could be woken up for other reason and enqueue for a new lock - if
         * that happens before the list unlink happens, the list would end up
         * being corrupted.
         *
         * The barrier pairs with the LwLockWaitListLock() when enqueing for
         * another lock.
         */
        LWBarrier();

        /* ENABLE_THREAD_CHECK only, waiter->lwWaiting should not be reported race  */
        TS_ANNOTATE_BENIGN_RACE_SIZED(&waiter->waiting, sizeof(waiter->waiting));

        waiter->waiting = false;
        LwLockWake(&waiter->sema);
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
     * If we don't have init the thread local control structure, there's no way to wait.
     * This should never occur
     */
    ASSERT(lock != NULL);
    ASSERT(!g_lwCtl.waiting);
    ASSERT(g_lwCtl.magic == LWLOCK_MAGIC_TAG);

    LwLockWaitListLock(lock);

    /* setting the flag is protected by the spinlock */
    SYMBOL_UNUSED uint32_t newState = LWAtomicSetBit(&lock->state, LW_FLAG_HAS_WAITERS);
    ASSERT(newState & LW_FLAG_HAS_WAITERS);

    g_lwCtl.mode = mode;
    g_lwCtl.waiting = true;

    /* LW_WAIT_UNTIL_FREE waiters are always at the front of the queue */
    if (mode == LW_WAIT_UNTIL_FREE) {
        DListPushHead(&lock->waiters, &g_lwCtl.listNode);
    } else {
        DListPushTail(&lock->waiters, &g_lwCtl.listNode);
    }

    /* Can release the mutex now */
    LwLockWaitListUnlock(lock);
}

/*
 * Remove ourselves from the waitlist.
 *
 * This is used if we queued ourselves because we thought we needed to sleep
 * but, after further checking, we discovered that we don't actually need to
 * do so. Returns false if somebody else already has woken us up, otherwise
 * returns true.
 */
static void LWLockDequeueSelf(LWLock *lock)
{
    ASSERT(lock != NULL);
    bool found = false;
    DListMutableIter iter;

    LwLockWaitListLock(lock);

    /*
     * Can't just remove ourselves from the list, but we need to iterate over
     * all entries as somebody else could have unqueued us.
     */
    DLIST_MODIFY_FOR_EACH(iter, &lock->waiters)
    {
        LWThreadCore *waiter = DLIST_CONTAINER(LWThreadCore, listNode, iter.cur);
        if (waiter == &g_lwCtl) {
            found = true;
            DListDelete(iter.cur);
            break;
        }
    }

    if (DListIsEmpty(&lock->waiters) && (lock->state & LW_FLAG_HAS_WAITERS) != 0) {
        (void)LWAtomicClearBit(&lock->state, ~LW_FLAG_HAS_WAITERS);
    }

    /* XXX: combine with fetch_and above? */
    LwLockWaitListUnlock(lock);

    /* clear waiting state again, nice for debugging */
    if (found) {
        g_lwCtl.waiting = false;
    } else {
        uint32_t extraWaits = 0;
        /*
         * Somebody else dequeued us and has or will wake us up. Deal with the
         * superflous absorption of a wakeup.
         *
         * Reset releaseOk if somebody woke us before we removed ourselves -
         * they'll have set it to false. */
        (void)LWAtomicSetBit(&lock->state, LW_FLAG_RELEASE_OK);

        /*
         * Now wait for the scheduled wakeup, otherwise our ->lwWaiting would
         * get reset at some inconvenient point later. Most of the time this
         * will immediately return. */
        for (;;) {
            /* "false" means cannot accept cancel/die interrupt here. */
            LwLockWait(&g_lwCtl.sema);
            if (g_lwCtl.waiting == false) {
                break;
            }
            extraWaits++;
        }
        /*
         * Fix the process wait semaphore's count for any absorbed wakeups.
         */
        while (extraWaits--) {
            LwLockWake(&g_lwCtl.sema);
        }
    }
}

static inline void LWLockRecordLock(LWlockContext *cxt, LWLock *lock, LWLockMode mode)
{
    ASSERT(cxt != NULL);
    cxt->heldLWlocks[cxt->recordHeldLWlocks].lock = lock;
    cxt->heldLWlocks[cxt->recordHeldLWlocks].mode = mode;
    cxt->recordHeldLWlocks++;
}

/*
 * LWLockAcquire - acquire a lightweight lock in the specified mode
 * If the lock is not available, sleep until it is.
 * Side effect: cancel/die interrupts are held off until lock release.
 */
UTILS_EXPORT
bool LWLockAcquire(LWLock *lock, LWLockMode mode)
{
    ASSERT(lock != NULL);
    ASSERT(mode == LW_EXCLUSIVE || mode == LW_SHARED);
    bool result = true;
    int extraWaits = 0;

    LWPrintDebug("LWLockAcquire", lock, mode);

    /*
     * Lock out cancel/die interrupts until we exit the code section protected
     * by the LWLock.  This ensures that interrupts will not interfere with
     * manipulations of data structures in shared memory.
     */
    HOLD_INTERRUPTS;

    /*
     * Loop here to try to acquire lock after each time we are wakeuped by
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
    for (;;) {
        bool mustwait = false;

        /*
         * Try to grab the lock the first time, we're not in the waitqueue
         * yet/anymore.
         */
        mustwait = LWLockAttemptLock(lock, mode);
        if (!mustwait) {
            LWLogDebug("LWLockAcquire", lock, "immediately acquired lock");
            break; // get the lock
        }

        /*
         * Ok, at this point we couldn't grab the lock on the first try.
         * add to the queue.
         */
        LWLockQueueSelf(lock, mode);
        /*
         * After we queued self, maybe the lock could have been released. try
         * to grab the lock again. If we succeed we need to revert the queuing
         * and be happy, otherwise we recheck the lock. If we still couldn't
         * grab it, we know that the other lock will see our queue entries when
         * releasing since they existed before we checked for the lock.
         */
        mustwait = LWLockAttemptLock(lock, mode);
        /* ok, grabbed the lock the second time round, need to undo queueing */
        if (!mustwait) {
            LWLogDebug("LWLockAcquire", lock, "acquired, undoing queue");

            LWLockDequeueSelf(lock);
            break;
        }
        /*
         * Wait until awakened.
         *
         * it is possible that we get awakened by other reason, such as signal.
         * If so, loop back and wait again.  Once we've gotten the LWLock,
         * re-increment the sema by the number of additional signals received,
         * so that the lock manager or signal manager will see the received
         * signal when it next waits.
         */
        LWLogDebug("LWLockAcquire", lock, "waiting");

        for (;;) {
            /* "false" means cannot accept cancel/die interrupt here. */
            LwLockWait(&g_lwCtl.sema);
            if (g_lwCtl.waiting == false) {
                break;
            }
            /* this time is interrupt by other case or no intent to wake up me */
            extraWaits++;
        }

        /* Retrying, allow LWLockRelease to release waiters again. */
        (void)LWAtomicSetBit(&lock->state, LW_FLAG_RELEASE_OK);
        LWLogDebug("LWLockAcquire", lock, "awakened");

        /* Now loop back and try to acquire lock again. */
        result = false;
    }
    /*
     * Fix the process wait semaphore's count for any absorbed wakeups.
     */
    while (extraWaits--) {
        LwLockWake(&g_lwCtl.sema);
    }

    g_lwCtl.cxt->numHeldLWlocks++;
    lock->owner = g_lwCtl.info;
    if (g_lwCtl.cxt->numHeldLWlocks > MAX_SIMUL_LWLOCKS) {
        return result;
    }
    /* Add lock to list of locks held by this backend */
    LWLockRecordLock(g_lwCtl.cxt, lock, mode);
    return result;
}

UTILS_EXPORT
bool LWLockAcquireDebug(LWLock *lock, LWLockMode mode, SYMBOL_UNUSED const char *fileName, SYMBOL_UNUSED int lineNumber,
                        SYMBOL_UNUSED const char *functionName)
{
    bool ret = LWLockAcquire(lock, mode);
    return ret;
}

/*
 * LWLockConditionalAcquire - acquire a lightweight lock in the specified mode
 * If the lock is not available, return FALSE with no side-effects.
 * If successful, cancel/die interrupts are held off until lock release.
 */
UTILS_EXPORT
bool LWLockConditionalAcquireDebug(LWLock *lock, LWLockMode mode, SYMBOL_UNUSED const char *fileName,
                                   SYMBOL_UNUSED int lineNumber, SYMBOL_UNUSED const char *functionName)
{
    ASSERT(lock != NULL);
    ASSERT(mode == LW_SHARED || mode == LW_EXCLUSIVE);

    bool mustwait = false;

    LWPrintDebug("LWLockConditionalAcquire", lock, mode);
    /*
     * Lock out cancel/die interrupts until we exit the code section protected
     * by the LWLock.  This ensures that interrupts will not interfere with
     * manipulations of data structures in shared memory.
     */
    HOLD_INTERRUPTS;

    /* Check for the lock */
    mustwait = LWLockAttemptLock(lock, mode);
    if (mustwait) {
        /* Failed to get lock, so release interrupt holdoff */
        RESUME_INTERRUPTS;
        LWLogDebug("LWLockConditionalAcquire", lock, "failed");
    } else {
        /* Got lock */
        lock->owner = g_lwCtl.info;
        g_lwCtl.cxt->numHeldLWlocks++;
        if (g_lwCtl.cxt->numHeldLWlocks > MAX_SIMUL_LWLOCKS) {
            return !mustwait;
        }
        /* Add lock to list of locks held by this backend */
        LWLockRecordLock(g_lwCtl.cxt, lock, mode);
    }
    return !mustwait;
}

/*
 * LWLockAcquireOrWait - Acquire lock, or wait until it's free
 *
 * The semantics of this function are a bit funky.	If the lock is currently
 * free, it is acquired in the given mode, and the function returns true.
 * If the lock isn't immediately free, the function waits until it is
 * released and returns false, but does not acquire the lock.
 */
UTILS_EXPORT
bool LWLockAcquireOrWait(LWLock *lock, LWLockMode mode)
{
    ASSERT(lock != NULL);
    ASSERT(mode == LW_SHARED || mode == LW_EXCLUSIVE);
    bool mustwait = false;
    int extraWaits = 0;

    LWPrintDebug("LWLockAcquireOrWait", lock, mode);

    /*
     * Lock out cancel/die interrupts until we exit the code section protected
     * by the LWLock.  This ensures that interrupts will not interfere with
     * manipulations of data structures in shared memory.
     */
    HOLD_INTERRUPTS;

    mustwait = LWLockAttemptLock(lock, mode);
    if (mustwait) {
        LWLockQueueSelf(lock, LW_WAIT_UNTIL_FREE);
        mustwait = LWLockAttemptLock(lock, mode);
        if (mustwait) {
            /*
             * Wait until awakened.
             */
            LWLogDebug("LWLockAcquireOrWait", lock, "waiting");
            for (;;) {
                /* "false" means cannot accept cancel/die interrupt here. */
                LwLockWait(&g_lwCtl.sema);
                if (g_lwCtl.waiting == false) {
                    break;
                }
                /* this time is interrupt by other case or no intent to wake up me */
                extraWaits++;
            }
            LWLogDebug("LWLockAcquireOrWait", lock, "awakened");
        } else {
            LWLogDebug("LWLockAcquireOrWait", lock, "acquired, undoing queue");

            /*
             * Got lock in the second attempt, undo queueing. We need to
             * treat this as having successfully acquired the lock, otherwise
             * we'd not necessarily wake up people we've prevented from
             * acquiring the lock.
             */
            LWLockDequeueSelf(lock);
        }
    }

    /*
     * Fix the process wait semaphore's count for any absorbed wakeups.
     */
    while (extraWaits--) {
        LwLockWake(&g_lwCtl.sema);
    }

    if (mustwait) {
        /* Failed to get lock, so release interrupt holdoff */
        RESUME_INTERRUPTS;
        LWLogDebug("LWLockAcquireOrWait", lock, "failed");
    } else {
        LWLogDebug("LWLockAcquireOrWait", lock, "succeeded");
        g_lwCtl.cxt->numHeldLWlocks++;
        lock->owner = g_lwCtl.info;
        if (g_lwCtl.cxt->numHeldLWlocks > MAX_SIMUL_LWLOCKS) {
            return !mustwait;
        }
        /* record lock into LWlockContext */
        LWLockRecordLock(g_lwCtl.cxt, lock, mode);
    }

    return !mustwait;
}

/*
 * LWLockRelease - release a previously acquired lock
 */
UTILS_EXPORT
void LWLockRelease(LWLock *lock)
{
    int i;
    uint32_t oldstate;
    bool checkWaiters = false;
    ASSERT(lock != NULL);
    LWLockMode mode = lock->state & LW_VAL_EXCLUSIVE ? LW_EXCLUSIVE : LW_SHARED;

    /* Remove lock from list of locks held.  Usually, but not always, it will
     * be the latest-acquired lock; so search array backwards. */
    for (i = g_lwCtl.cxt->recordHeldLWlocks; --i >= 0;) {
        if (lock == g_lwCtl.cxt->heldLWlocks[i].lock) {
            mode = g_lwCtl.cxt->heldLWlocks[i].mode;
            break;
        }
    }
    if ((i < 0) && !LWLockIsHeldsOverflow()) {
        /* Try to release a lock doesn't belong to me. */
        return;
    }
    if (i >= 0) {
        g_lwCtl.cxt->recordHeldLWlocks--;
        for (; i < g_lwCtl.cxt->recordHeldLWlocks; i++) {
            g_lwCtl.cxt->heldLWlocks[i] = g_lwCtl.cxt->heldLWlocks[i + 1];
        }
    }
    g_lwCtl.cxt->numHeldLWlocks--;

    LWPrintDebug("LWLockRelease", lock, mode);

    /*
     * Release my hold on lock, after that it can immediately be acquired by
     * others, even if we still have to wakeup other waiters. */
    if (mode == LW_EXCLUSIVE) {
        /* ENABLE_THREAD_CHECK only, Must release vector clock info to other
         * threads before unlock */
        TS_ANNOTATE_RW_LOCK_RELEASED(&lock->rwlock, 1);
        oldstate = LWAtomicSubFetch(&lock->state, LW_VAL_EXCLUSIVE);
    } else {
        /* ENABLE_THREAD_CHECK only, Must release vector clock info to other
         * threads before unlock */
        TS_ANNOTATE_RW_LOCK_RELEASED(&lock->rwlock, 0);
        lock->owner = g_lwCtl.info;
        oldstate = LWAtomicSubFetch(&lock->state, 1);
    }

    /* nobody else can have that kind of lock */
    ASSERT(!(oldstate & LW_VAL_EXCLUSIVE));

    /* We're still waiting for backends to get scheduled, don't wake them up again. */
    checkWaiters =
        ((oldstate & (LW_FLAG_HAS_WAITERS | LW_FLAG_RELEASE_OK)) == (LW_FLAG_HAS_WAITERS | LW_FLAG_RELEASE_OK)) &&
        ((oldstate & LW_LOCK_MASK) == 0);
    /* As waking up waiters requires the spinlock to be acquired, only do so
     * if necessary. */
    if (checkWaiters) {
        LWLogDebug("LWLockRelease", lock, "releasing waiters");
        LwLockWakeup(lock);
    }
    /* Now okay to allow cancel/die interrupts. */
    RESUME_INTERRUPTS;
}

/*
 * LWLockReleaseAll - release all currently-held locks
 *
 * Used to clean up after ereport(ERROR). An important difference between this
 * function and retail LWLockRelease calls is that INTERRUPT state is unchanged
 * by this operation.  This is necessary since INTERRUPT state has been set to
 * an appropriate level earlier in error recovery. We could decrement it
 * below zero if we allow it to drop for each released lock!
 */
void LWLockReleaseAll(void)
{
    if (LWLockIsHeldsOverflow()) {
        Abort();
    }
    while (g_lwCtl.cxt->recordHeldLWlocks > 0) {
        HOLD_INTERRUPTS; /* match the upcoming ResumeInterrupts */
        LWLockRelease(g_lwCtl.cxt->heldLWlocks[g_lwCtl.cxt->recordHeldLWlocks - 1].lock);
    }
}

/*
 * LWLockHeldByMe - test whether my process currently holds a lock
 *
 * This is meant as debug support only.  We currently do not distinguish
 * whether the lock is held shared or exclusive.
 */
UTILS_EXPORT
bool LWLockHeldByMe(LWLock *lock)
{
    ASSERT(lock != NULL);
    for (int i = 0; i < g_lwCtl.cxt->recordHeldLWlocks; i++) {
        if (g_lwCtl.cxt->heldLWlocks[i].lock == lock) {
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
UTILS_EXPORT
bool LWLockHeldByMeInMode(LWLock *lock, LWLockMode mode)
{
    ASSERT(lock != NULL);
    for (int i = 0; i < g_lwCtl.cxt->recordHeldLWlocks; i++) {
        if (g_lwCtl.cxt->heldLWlocks[i].lock == lock && g_lwCtl.cxt->heldLWlocks[i].mode == mode) {
            return true;
        }
    }
    return false;
}

/* reset a lwlock */
UTILS_EXPORT
void LWLockReset(LWLock *lock)
{
    ASSERT(lock != NULL);
    LWAtomicWrite(&lock->state, LW_FLAG_RELEASE_OK);

    /* ENABLE_THREAD_CHECK only */
    TS_ANNOTATE_RW_LOCK_DESTROY(&lock->listlock);
    TS_ANNOTATE_RW_LOCK_CREATE(&lock->listlock);
    TS_ANNOTATE_RW_LOCK_DESTROY(&lock->rwlock);
    TS_ANNOTATE_RW_LOCK_CREATE(&lock->rwlock);

    DListInit(&lock->waiters);
}

UTILS_EXPORT
bool LWLockIsHeldsOverflow(void)
{
    return g_lwCtl.cxt->numHeldLWlocks != g_lwCtl.cxt->recordHeldLWlocks;
}

/* get the address of numHeldLWlocks */
UTILS_EXPORT
int32_t GetHeldLWlocksNum(void)
{
    return g_lwCtl.cxt->numHeldLWlocks;
}

UTILS_EXPORT
int32_t GetRecordHeldLWlocksNum(void)
{
    return g_lwCtl.cxt->recordHeldLWlocks;
}

UTILS_EXPORT
/* get lwlocks now held */
void *GetHeldLWlocks(void)
{
    return (void *)g_lwCtl.cxt->heldLWlocks;
}

UTILS_EXPORT
LWLockMode GetHeldLWLockMode(LWLock *lock)
{
    ASSERT(lock != NULL);
    for (int i = 0; i < g_lwCtl.cxt->numHeldLWlocks; i++) {
        if (g_lwCtl.cxt->heldLWlocks[i].lock == lock) {
            return g_lwCtl.cxt->heldLWlocks[i].mode;
        }
    }
    Abort();
    return LW_MODE_INVAILD; /* silence compiler warnning */
}
