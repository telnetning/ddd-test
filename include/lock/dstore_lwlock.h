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
 */

#ifndef DSTORE_LWLOCK_H
#define DSTORE_LWLOCK_H

#include <atomic>
#include "lock/dstore_s_lock.h"
#include "common/algorithm/dstore_ilist.h"
#include "common/concurrent/dstore_atomic.h"
#include "diagnose/dstore_lwlock_diagnose.h"

namespace DSTORE {

enum LWLockMode : uint8 {
    LW_EXCLUSIVE,
    LW_SHARED,
    LW_WAIT_UNTIL_FREE /* A special mode used in PGPROC->lwlockMode,
                        * when waiting for lock to become free. Not
                        * to be used as LWLockAcquire argument */
};

#ifdef DSTORE_USE_ASSERT_CHECKING
#define DSTORE_LWLOCK_DEBUG
#endif


struct LWLockWaiter {
    bool waiting;           /* true if waiting for an LW lock */
    uint8 mode;             /* lwlock mode being waited for */
    dlist_node next_waiter; /* next waiter for same LW lock */
    struct LWLock *waitLock;

    void init()
    {
        waiting = false;
        mode = 0;
        waitLock = nullptr;
    }

    void RecordWaitLock(UNUSE_PARAM struct LWLock *lock, uint8 newMode)
    {
        mode = newMode;
        waitLock = lock;
    }

    void CleanUpWaitLock()
    {
        mode = static_cast<uint8>(LW_EXCLUSIVE);
        waitLock = nullptr;
    }

    void StartWaiting()
    {
        waiting = true;
    }

    void StopWaiting()
    {
        waiting = false;
    }
};

typedef struct LWLock {
    int spinsPerDelay;            /* add spins per delay */
    uint16           groupId;     /* group ID */
    gs_atomic_uint64 state;       /* 64bit state of exlusive/nonexclusive lockers */
    dlist_head       waiters;     /* list of waiting PGPROCs */

#ifdef LOCK_DEBUG
    gs_atomic_uint32 nwaiters; /* number of waiters */
    struct ThreadCore *owner;      /* last exlusive owner of the lock */
#endif
#ifdef ENABLE_THREAD_CHECK
    gs_atomic_uint32 rwlock;
    gs_atomic_uint32 listlock;
#endif
} LWLock;
const uint LWLOCK_PADDED_SIZE = DSTORE_CACHELINE_SIZE;

typedef union LWLockPadded {
    LWLock lock;
    char pad[LWLOCK_PADDED_SIZE];
} LWLockPadded;

typedef struct LWLockHandle {
    LWLock* lock;
    LWLockMode mode;
} LWLockHandle;

typedef struct LWlockContext {
    int num_held_lwlocks;
    LWLockHandle* held_lwlocks;
}LWlockContext;

/*
 * We use this structure to keep track of locked LWLocks for release
 * during error recovery.  The maximum size could be determined at runtime
 * if necessary, but it seems unlikely that more than a few locks could
 * ever be held simultaneously.
 */
#define MAX_SIMUL_LWLOCKS 4224

extern void LWLockInitialize(LWLock* lock, uint16 groupId = static_cast<uint16>(LWLOCK_GROUP_DEFAULT));
template <LWLockMode mode>
extern void LWLockAcquire(LWLock *lock, const char *fileName, int lineNumber, const char *functionName);
extern bool LWLockConditionalAcquireDebug(LWLock* lock, LWLockMode mode,
                                          const char *fileName, int lineNumber, const char *functionName);
extern bool LWLockConditionalAcquireAnyModeDebug(LWLock* lock, LWLockMode* acquiredLockMode,
                                                 const char *fileName, int lineNumber, const char *functionName);
extern bool LWLockAcquireOrWait(LWLock* lock, LWLockMode mode);
extern void LWLockRelease(LWLock* lock) noexcept;
extern void LWLockReleaseAll(void);
extern bool LWLockHeldByMe(LWLock* lock);
extern bool LWLockHeldByMeInMode(LWLock* lock, LWLockMode mode);
extern void LWLockReset(LWLock* lock);
extern bool IsLWLockExclusiveLocked(LWLock *lock);
extern bool IsLWLockIdle(LWLock *lock);
extern int *GetHeldLwlocksNum(void);
extern uint32 GetHeldLwlocksMaxnum(void);
extern void* GetHeldLwlocks(void);
extern LWLockMode GetHeldLWLockMode(LWLock* lock);

extern const char *GetLWLockGroupName(int groupId);

extern void LWLockOwn(LWLock *lock, LWLockMode mode,
    const char *fileName, int lineNumber, const char *functionName);
extern void LWLockDisown(LWLock *lock);

#define DstoreLWLockAcquireByMode(lock, mode) \
    if (unlikely((mode) == LW_EXCLUSIVE)) { \
        DstoreLWLockAcquire(lock, LW_EXCLUSIVE); \
    } else { \
        DstoreLWLockAcquire(lock, LW_SHARED); \
    }

#define DstoreLWLockAcquire(lock, mode) \
    LWLockAcquire<mode>(lock, __FILE__, __LINE__, __FUNCTION__)

#define DstoreLWLockConditionalAcquire(lock, mode) \
    LWLockConditionalAcquireDebug(lock, mode, __FILE__, __LINE__, __FUNCTION__)

#define DstoreLWLockOwn(lock, mode) \
    LWLockOwn(lock, mode, __FILE__, __LINE__, __FUNCTION__)

extern inline bool DstoreLWLockConditionalAcquireAnyMode(LWLock* lock, LWLockMode* mode)
{
    return LWLockConditionalAcquireAnyModeDebug(lock, mode, __FILE__, __LINE__, __FUNCTION__);
}

extern void AddToThreadHeldLockList(LWLock *lock, LWLockMode mode, const char *fileName,
    int lineNumber, const char *functionName);

extern void CheckLwLockLeak();

}

#endif   /* STORAGE_LWLOCK_H */
