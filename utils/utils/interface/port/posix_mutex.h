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
 * ---------------------------------------------------------------------------------
 *
 * posix_mutex.h
 *
 * Description:Defines the mutex external interfaces wrapper for POSIX standard mutex.
 *
 * ---------------------------------------------------------------------------------
 */
#ifndef UTILS_POSIX_MUTEX_H
#define UTILS_POSIX_MUTEX_H

#include <sys/time.h>
#include <pthread.h>
#include <stdbool.h>
#include "securec.h"
#include "defines/common.h"
#include "defines/abort.h"
#include "port/posix_errcode.h"

GSDB_BEGIN_C_CODE_DECLS

/* Mutex  wrapper. */
typedef struct {
    pthread_mutex_t mutex; /* Mutex controlling the lock */
} Mutex;

/*
 * Static mode initialization is not supported on the windows platform. Therefore, The following static mode
 * initialization is not provided,avoid cross-platform migration issues.
 * #define MUTEX_INITIALIZER { PTHREAD_MUTEX_INITIALIZER }
 * Mutex initialization using the PTHREAD_MUTEX_INITIALIZER does not immediately initialize the mutex. Instead,
 * on first use,pthread_mutex_lock() or pthread_mutex_trylock() branches into a slow path and causes the
 * initialization of the mutex.Because a mutex is not just a simple memory object and requires that some resources
 * be allocated by the system, an attempt to call pthread_mutex_destroy() or pthread_mutex_unlock() on a mutex
 * that has statically initialized using PTHREAD_MUTEX_INITIALER and was not yet locked causes an EINVAL error.
 */

/*
 * On the Linux platform, mutex-related functions may return an error, but the encapsulation interface of mutex
 * does not return an error.First, these exceptions rarely occur. Second,the internal mutex function checks these
 * exceptions.If an exception occurs,referring to the GLIB process, the process is forced to abort. The mutex
 * initialization is generally performed during process startup,if the mutex fails, the restart has little impact.
 * If the system is locked or unlocked abnormally,the system cannot run properly.In this case, the system can only
 * be recovered by restarting the system.
 */

/**
 * Dynamic mode mutex initialization.
 * @param mutex: Mutex
 * @return
 */
void MutexInit(Mutex *mutex);
/**
 * Destroy Mutex.
 * @param mutex :Mutex
 * @return
 */
void MutexDestroy(Mutex *mutex);
/**
 * Lock Mutex.
 * @param mutex :Mutex
 * @return
 */
void MutexLock(Mutex *mutex);
/**
 * Try lock Mutex.
 * @param mutex :Mutex
 * @return
 */
bool MutexTrylock(Mutex *mutex);
/**
 * Unlock Mutex.
 * @param mutex :Mutex
 * @return
 */
void MutexUnlock(Mutex *mutex);

/* The pthread_cond_t wrapper. */
typedef struct {
    pthread_cond_t cond;
} ConditionVariable;

/*
 * The cond static mode initialization is not supported on the windows platform. Therefore, The following static mode
 * initialization is not provided,avoid cross-platform migration issues.
 * #define COND_INITIALIZER { PTHREAD_COND_INITIALIZER }
 */

/**
 * Dynamic mode condition variable initialization.
 * @param cond :ConditionVariable
 * @return
 */
void ConditionVariableInit(ConditionVariable *cond);
/**
 * Destroy condition variable.
 * @param cond :ConditionVariable
 * @return
 */
void ConditionVariableDestroy(ConditionVariable *cond);
/**
 * Wait condition variable used to block on a condition variable.The function shall be called with mutex locked
 * by the calling thread or undefined behavior will result.The functions release mutex and cause the calling thread
 * to block on the condition variable cond.
 * @param cond:ConditionVariable
 * @param mutex:Mutex
 * @return
 */
void ConditionVariableWait(ConditionVariable *cond, Mutex *mutex);
/**
 * The ConditionVariableTimedWait function is the same as the ConditionVariableWait function except that
 * an error is returned if the time specified by timeout passes before the condition cond is signaled.
 * When such timeouts occur,ConditionVariableTimedWait() shall nonetheless release and reacquire the mutex
 * referenced by mutex.
 * @param cond:ConditionVariable
 * @param mutex:Mutex
 * @param timeout:time_t  unit:milliseconds.
 * @return
 */
bool ConditionVariableTimedWait(ConditionVariable *cond, Mutex *mutex, time_t timeout);
/**
 * The functions are used to unblock threads blocked on a condition variable.The ConditionVariableSignal
 * call unblocks at least one of the threads that are blocked on the specified condition variable cond
 * (if any threads are blocked on cond).
 * @param cond:ConditionVariable
 * @return
 */
void ConditionVariableSignal(ConditionVariable *cond);
/**
 * The functions are used to unblock threads blocked on a condition variable.The ConditionVariableBroadcast
 * call unblocks at least one of the threads that are blocked on the specified condition variable cond
 * (if any threads are blocked on cond).
 * @param cond:ConditionVariable
 * @return
 */
void ConditionVariableBroadcast(ConditionVariable *cond);

GSDB_END_C_CODE_DECLS
#endif /* UTILS_POSIX_MUTEX_H */
