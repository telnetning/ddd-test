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
 * posix_mutex.c
 *
 * Description:
 * 1. Implementation of the POSIX mutex interface wrapper.
 *
 * ---------------------------------------------------------------------------------
 */
#include "port/posix_time.h"
#include "port/posix_mutex.h"

/**
 * Dynamic mode mutex initialization.
 * @param mutex: Mutex
 * @return
 */
static ErrorCode MutexInitEx(Mutex *mutex)
{
    int rc;
    ErrorCode errCode = ERROR_SYS_OK;
    pthread_mutexattr_t attr;
    rc = pthread_mutexattr_init(&attr);
    if (rc != 0) {
        PosixErrorCode2PortErrorCode(rc, &errCode);
        return errCode;
    }
#if defined(_POSIX_THREAD_PROCESS_SHARED)
    rc = pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_PRIVATE);
    if (rc != 0) {
        PosixErrorCode2PortErrorCode(rc, &errCode);
        return errCode;
    }
#endif
    /* PTHREAD_MUTEX_RECURSIVE is what posix says should be supported, but some versions of glibc have only
     * PTHREAD_MUTEX_RECURSIVE_NP (the np stands for non-portable), when they have the same meaning. */
    rc = pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE_NP);
    if (rc != 0) {
        PosixErrorCode2PortErrorCode(rc, &errCode);
        return errCode;
    }
    rc = pthread_mutex_init(&(mutex->mutex), &attr);
    if (rc != 0) {
        PosixErrorCode2PortErrorCode(rc, &errCode);
        return errCode;
    }
    /* The parameter type of pthread_mutexattr_init is actually opaque and contains a system-assigned attribute object.
     * During the execution of this function, the required memory is allocated to the attribute object. Therefore,
     * if the mutex attribute object is not destroyed by pthread_mutexattr_destroy, a memory leak occurs. */
    rc = pthread_mutexattr_destroy(&attr);
    if (rc != 0) {
        PosixErrorCode2PortErrorCode(rc, &errCode);
        return errCode;
    }
    return errCode;
}

/**
 * Dynamic mode mutex initialization.
 * @param mutex: Mutex
 * @return
 */
UTILS_EXPORT void MutexInit(Mutex *mutex)
{
    ErrorCode errCode = ERROR_SYS_OK;
    errCode = MutexInitEx(mutex);
    if (errCode != ERROR_SYS_OK) {
        Abort();
    }
}

/**
 * Destroy Mutex.
 * @param mutex :Mutex
 * @return
 */
static ErrorCode MutexDestroyEx(Mutex *mutex)
{
    int rc;
    ErrorCode errCode = ERROR_SYS_OK;
    rc = pthread_mutex_destroy(&(mutex->mutex));
    if (rc != 0) {
        PosixErrorCode2PortErrorCode(rc, &errCode);
        return errCode;
    }
    return errCode;
}

/**
 * Destroy GaussMutex.
 * @param mutex :GaussMutex
 * @return
 */
UTILS_EXPORT void MutexDestroy(Mutex *mutex)
{
    ErrorCode errCode = ERROR_SYS_OK;
    errCode = MutexDestroyEx(mutex);
    if (errCode != ERROR_SYS_OK) {
        Abort();
    }
}

/**
 * Lock Mutex.
 * @param mutex :Mutex
 * @return
 */
static ErrorCode MutexLockEx(Mutex *mutex)
{
    int rc;
    ErrorCode errCode = ERROR_SYS_OK;
    rc = pthread_mutex_lock(&(mutex->mutex));
    if (rc != 0) {
        PosixErrorCode2PortErrorCode(rc, &errCode);
        return errCode;
    }
    return errCode;
}

/**
 * Lock Mutex.
 * @param mutex :Mutex
 * @return
 */
UTILS_EXPORT void MutexLock(Mutex *mutex)
{
    ErrorCode errCode = ERROR_SYS_OK;
    errCode = MutexLockEx(mutex);
    if (errCode != ERROR_SYS_OK) {
        Abort();
    }
}

/**
 * Try lock Mutex.
 * @param mutex :Mutex
 * @return
 */
static ErrorCode MutexTrylockEx(Mutex *mutex)
{
    int rc;
    ErrorCode errCode = ERROR_SYS_OK;
    rc = pthread_mutex_trylock(&(mutex->mutex));
    if (rc != 0) {
        PosixErrorCode2PortErrorCode(rc, &errCode);
        return errCode;
    }
    return errCode;
}
/**
 * Try lock Mutex.
 * @param mutex :Mutex
 * @return
 */
UTILS_EXPORT bool MutexTrylock(Mutex *mutex)
{
    ErrorCode errCode = ERROR_SYS_OK;
    errCode = MutexTrylockEx(mutex);
    if (errCode != ERROR_SYS_OK) {
        return false;
    }
    return true;
}

/**
 * Unlock Mutex.
 * @param mutex :Mutex
 * @return
 */
static ErrorCode MutexUnlockEx(Mutex *mutex)
{
    int rc;
    ErrorCode errCode = ERROR_SYS_OK;
    rc = pthread_mutex_unlock(&(mutex->mutex));
    if (rc != 0) {
        PosixErrorCode2PortErrorCode(rc, &errCode);
        return errCode;
    }
    return errCode;
}

/**
 * Unlock Mutex.
 * @param mutex :Mutex
 * @return
 */
UTILS_EXPORT void MutexUnlock(Mutex *mutex)
{
    ErrorCode errCode = ERROR_SYS_OK;
    errCode = MutexUnlockEx(mutex);
    if (errCode != ERROR_SYS_OK) {
        Abort();
    }
}

/**
 * Dynamic mode condition variable initialization.
 * @param cond :ConditionVariable
 * @return
 */
static ErrorCode ConditionVariableInitEx(ConditionVariable *cond)
{
    int rc;
    ErrorCode errCode = ERROR_SYS_OK;
    pthread_condattr_t attr;
    rc = pthread_condattr_init(&attr);
    if (rc != 0) {
        PosixErrorCode2PortErrorCode(rc, &errCode);
        return errCode;
    }
#if defined(_POSIX_THREAD_PROCESS_SHARED)
    rc = pthread_condattr_setpshared(&attr, PTHREAD_PROCESS_PRIVATE);
    if (rc != 0) {
        PosixErrorCode2PortErrorCode(rc, &errCode);
        return errCode;
    }
#endif
    rc = pthread_cond_init(&(cond->cond), &attr);
    if (rc != 0) {
        PosixErrorCode2PortErrorCode(rc, &errCode);
        return errCode;
    }
    rc = pthread_condattr_destroy(&attr);
    if (rc != 0) {
        PosixErrorCode2PortErrorCode(rc, &errCode);
        return errCode;
    }
    return errCode;
}

/**
 * Dynamic mode condition variable initialization.
 * @param cond :ConditionVariable
 * @return
 */
UTILS_EXPORT void ConditionVariableInit(ConditionVariable *cond)
{
    ErrorCode errCode = ERROR_SYS_OK;
    errCode = ConditionVariableInitEx(cond);
    if (errCode != ERROR_SYS_OK) {
        Abort();
    }
}

/**
 * Destroy condition variable.
 * @param cond :ConditionVariable
 * @return
 */
static ErrorCode ConditionVariableDestroyEx(ConditionVariable *cond)
{
    int rc;
    ErrorCode errCode = ERROR_SYS_OK;
    rc = pthread_cond_destroy(&(cond->cond));
    if (rc != 0) {
        PosixErrorCode2PortErrorCode(rc, &errCode);
        return errCode;
    }
    return errCode;
}

/**
 * Destroy condition variable.
 * @param cond :ConditionVariable
 * @return
 */
UTILS_EXPORT void ConditionVariableDestroy(ConditionVariable *cond)
{
    ErrorCode errCode = ERROR_SYS_OK;
    errCode = ConditionVariableDestroyEx(cond);
    if (errCode != ERROR_SYS_OK) {
        Abort();
    }
}

/**
 * Wait condition variable used to block on a condition variable.The function shall be called with mutex locked
 * by the calling thread or undefined behavior will result.The functions release mutex and cause the calling thread
 * to block on the condition variable cond.
 * @param cond:ConditionVariable
 * @param mutex:Mutex
 * @return
 */
static ErrorCode ConditionVariableWaitEx(ConditionVariable *cond, Mutex *mutex)
{
    int rc;
    ErrorCode errCode = ERROR_SYS_OK;
    rc = pthread_cond_wait(&(cond->cond), &(mutex->mutex));
    if (rc != 0) {
        PosixErrorCode2PortErrorCode(rc, &errCode);
        return errCode;
    }
    return errCode;
}

/**
 * Wait condition variable used to block on a condition variable.The function shall be called with mutex locked
 * by the calling thread or undefined behavior will result.The functions release mutex and cause the calling thread
 * to block on the condition variable cond.
 * @param cond:ConditionVariable
 * @param mutex:Mutex
 * @return
 */
UTILS_EXPORT void ConditionVariableWait(ConditionVariable *cond, Mutex *mutex)
{
    ErrorCode errCode = ERROR_SYS_OK;
    errCode = ConditionVariableWaitEx(cond, mutex);
    if (errCode != ERROR_SYS_OK) {
        Abort();
    }
}

/**
 * The ConditionVariableTimedWait function is the same as the ConditionVariableWait function except that
 * an error is returned if the time specified by timeout passes before the condition cond is signaled. When such
 * timeouts occur,ConditionVariableTimedWait() shall nonetheless release and reacquire the mutex referenced by mutex.
 * @param cond:ConditionVariable
 * @param mutex:Mutex
 * @param timeout:time_t  unit:milliseconds.
 * @return
 */

static ErrorCode ConditionVariableTimedWaitEx(ConditionVariable *cond, Mutex *mutex, time_t timeout)
{
    int rc;
    ErrorCode errCode = ERROR_SYS_OK;
    struct timespec abstime;
    ConvertMillisecondsToAbsoluteTime(timeout, &abstime);
    rc = pthread_cond_timedwait(&(cond->cond), &(mutex->mutex), &abstime);
    if (rc != 0) {
        PosixErrorCode2PortErrorCode(rc, &errCode);
        return errCode;
    }
    return errCode;
}
/**
 * The ConditionVariableTimedWait function is the same as the ConditionVariableWait function except that
 * an error is returned if the time specified by timeout passes before the condition cond is signaled. When such
 * timeouts occur,ConditionVariableTimedWait() shall nonetheless release and reacquire the mutex referenced by mutex.
 * @param cond:ConditionVariable
 * @param mutex:Mutex
 * @param timeout:time_t  unit:milliseconds.
 * @return
 */
UTILS_EXPORT bool ConditionVariableTimedWait(ConditionVariable *cond, Mutex *mutex, time_t timeout)
{
    ErrorCode errCode = ERROR_SYS_OK;
    errCode = ConditionVariableTimedWaitEx(cond, mutex, timeout);
    if (errCode == ERROR_SYS_OK) {
        return true;
    } else if (errCode == ERROR_UTILS_PORT_ETIMEDOUT) {
        return false;
    } else {
        Abort();
        return false;
    }
}

/**
 * The functions are used to unblock threads blocked on a condition variable.The ConditionVariableSignal call
 * unblocks at least one of the threads that are blocked on the specified condition variable cond (if any threads
 * are blocked on cond).
 * @param cond:ConditionVariable
 * @return
 */
static ErrorCode ConditionVariableSignalEx(ConditionVariable *cond)
{
    int rc;
    ErrorCode errCode = ERROR_SYS_OK;
    rc = pthread_cond_signal(&(cond->cond));
    if (rc != 0) {
        PosixErrorCode2PortErrorCode(rc, &errCode);
        return errCode;
    }
    return errCode;
}

/**
 * The functions are used to unblock threads blocked on a condition variable.The ConditionVariableBroadcast call
 * unblocks at least one of the threads that are blocked on the specified condition variable cond (if any threads
 * are blocked on cond).
 * @param cond:ConditionVariable
 * @return
 */
static ErrorCode ConditionVariableBroadcastEx(ConditionVariable *cond)
{
    int rc;
    ErrorCode errCode = ERROR_SYS_OK;
    rc = pthread_cond_broadcast(&(cond->cond));
    if (rc != 0) {
        PosixErrorCode2PortErrorCode(rc, &errCode);
        return errCode;
    }
    return errCode;
}

/**
 * The functions are used to unblock threads blocked on a condition variable.The ConditionVariableSignal call
 * unblocks at least one of the threads that are blocked on the specified condition variable cond (if any
 * threads are blocked on cond).
 * The pthread_cond_broadcast() or pthread_cond_signal() functions may be called by a thread whether or not
 * it currently owns the mutex that threads calling pthread_cond_wait() or  pthread_cond_timedwait() have
 * associated with the condition variable during their waits; however, if predictable scheduling behavior is
 * required, then that mutex shall be locked by the thread calling  pthread_cond_broadcast() or
 * pthread_cond_signal().
 * @param cond:ConditionVariable
 * @return
 */
UTILS_EXPORT void ConditionVariableSignal(ConditionVariable *cond)
{
    ErrorCode errCode = ERROR_SYS_OK;
    errCode = ConditionVariableSignalEx(cond);
    if (errCode != ERROR_SYS_OK) {
        Abort();
    }
}

/**
 * The functions are used to unblock threads blocked on a condition variable.The ConditionVariableBroadcast call
 * unblocks at least one of the threads that are blocked on the specified condition variable cond (if any
 * threads are blocked on cond).
 * The pthread_cond_broadcast() or pthread_cond_signal() functions may be called by a thread whether or not
 * it currently owns the mutex that threads calling pthread_cond_wait() or  pthread_cond_timedwait() have
 * associated with the condition variable during their waits; however, if predictable scheduling behavior is
 * required, then that mutex shall be locked by the thread calling  pthread_cond_broadcast() or
 * pthread_cond_signal().
 * @param cond:ConditionVariable
 * @return
 */
UTILS_EXPORT void ConditionVariableBroadcast(ConditionVariable *cond)
{
    ErrorCode errCode = ERROR_SYS_OK;
    errCode = ConditionVariableBroadcastEx(cond);
    if (errCode != ERROR_SYS_OK) {
        Abort();
    }
}
