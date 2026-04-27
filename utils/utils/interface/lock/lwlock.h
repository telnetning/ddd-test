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
 * Description: Header file for lwlock
 */
#ifndef UTILS_LWLOCK_H
#define UTILS_LWLOCK_H

#include <stdbool.h>
#include <stdint.h>
#include "defines/common.h"
#include "container/linked_list.h"

GSDB_BEGIN_C_CODE_DECLS

typedef struct LWLock LWLock;
typedef struct LWThreadCore LWThreadCore;
typedef struct ThreadInfo ThreadInfo;

typedef enum {
    LW_EXCLUSIVE,
    LW_SHARED,
    LW_MODE_INVAILD,
    LW_WAIT_UNTIL_FREE, /* A special mode for lwlock internal
                         * when waiting for lock to become free. Not
                         * to be used as LWLockAcquire argument */
} LWLockMode;

typedef struct {
    const char *msg;
} ErrInfo;

typedef struct {
    LWLock *lock;
    LWLockMode mode;
} LWLockHandle;

typedef void (*LwLockLogFunc)(LWLock *lock, int errCode, const ErrInfo *errInfo);

typedef struct {
    LwLockLogFunc log; /* log print function */
} LWLockCtlParam;

struct ThreadInfo {
    int64_t id;         /* OS thread id */
    LWThreadCore *task; /* point to task of id */
};

/*
 * We use this structure to keep track of locked LWLocks for release
 * during error recovery.  The maximum size could be determined at runtime
 * if necessary, but it seems unlikely that more than a few locks could
 * ever be held simultaneously.
 */
#define MAX_SIMUL_LWLOCKS 4224

typedef struct {
    int32_t numHeldLWlocks;
    int32_t recordHeldLWlocks;
    LWLockHandle heldLWlocks[MAX_SIMUL_LWLOCKS];
} LWlockContext;

#ifdef LWLOCK_DEBUG
#if defined __AARCH64EB__
typedef struct {
    uint32_t wakeupOk : 1;
    uint32_t hasWaiters : 1;
    uint32_t releaseOk : 1;
    uint32_t iListsLocked : 1;
    uint32_t reserved : 3;
    uint32_t exclusive : 1;
    uint32_t sharedCount : 24;
} LWLockVerbose;
#elif defined(__AARCH64EL__) || (__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__)
typedef struct {
    uint32_t sharedCount : 24;
    uint32_t exclusive : 1;
    uint32_t reserved : 3;
    uint32_t iListsLocked : 1;
    uint32_t releaseOk : 1;
    uint32_t hasWaiters : 1;
    uint32_t wakeupOk : 1;
} LWLockVerbose;
#endif
#endif

struct LWLock {
    union {
        uint32_t state; /* state of exlusive/nonexclusive lockers */
#ifdef LWLOCK_DEBUG
        LWLockVerbose stateVerbose; /* verbose of state, for gdb print */
#endif
    };
    DListHead waiters;      /* list of waiting threads */
    LWThreadCore *exWakeup; /* last wake up exclusive thread */
    LWLockCtlParam para;    /* caller custom lwlock parameters */
    ThreadInfo owner;       /* owner thread of held the lock in exlusive mode,
                             * or last release the lock in shared mode */
#ifdef ENABLE_THREAD_CHECK
    uint32_t rwlock;
    uint32_t listlock;
#endif
};

/* LWLockPadded LWLockAligned */
typedef struct {
    LWLock lock;
} LWLockPadded __attribute__((aligned(GS_CACHE_LINE_SIZE)));

/**
 * @brief Init lwlock thread local info
 * @Constraint this API need call by all thread once before use lwlock
 * @param  cxt - lwlock context use to save all the hold lwlock of one thread
 * @return   0 - If initialize success
 *      others - others init semaphore fail
 */
int32_t LwLockInitThreadCore(LWlockContext *cxt);

/**
 * @brief DeInit lwlock thread local info
 * @Constraint this API need call before thread exit
 * @return   0 - If initialize success
 *      others - others init semaphore fail
 */
int32_t LWLockDeInitThreadCore(void);

void LWLockInitialize(LWLock *lock);
void LWLockInitializeWithParam(LWLock *lock, LWLockCtlParam *para);

/**
 * @brief Acquire a lightweight lock in the specified mode
 * @Constraint cancel/die interrupts are held off until lock release.
 * @param lock - The lwlock to lock
 * @param mode - The lock mode, share or exclusive
 * @return true  - If the lock was available immediately.
 *         false - If the lock is not available, sleep until it is, will not return.
 */
bool LWLockAcquire(LWLock *lock, LWLockMode mode);
bool LWLockAcquireDebug(LWLock *lock, LWLockMode mode, const char *fileName, int lineNumber, const char *functionName);
bool LWLockConditionalAcquireDebug(LWLock *lock, LWLockMode mode, const char *fileName, int lineNumber,
                                   const char *functionName);
bool LWLockAcquireOrWait(LWLock *lock, LWLockMode mode);
void LWLockRelease(LWLock *lock);
void LWLockReleaseAll(void);
bool LWLockHeldByMe(LWLock *lock);
bool LWLockHeldByMeInMode(LWLock *lock, LWLockMode mode);
void LWLockReset(LWLock *lock);

uint32_t LWLockAtomicReadState(LWLock *lock);
bool LWLockIsHeldsOverflow(void);
void *GetHeldLWlocks(void);
int32_t GetHeldLWlocksNum(void);
int32_t GetRecordHeldLWlocksNum(void);
LWLockMode GetHeldLWLockMode(LWLock *lock);

GSDB_END_C_CODE_DECLS
#endif /* UTILS_LWLOCK_H */
