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
 * win32_mutex.c
 *
 * Description:
 * 1.  Implementation of the windows mutex interface wrapper
 *
 * ---------------------------------------------------------------------------------
 */
#include "port/win32_mutex.h"

/* On the Windows platform, the functions of CRITICAL_SECTION and conditional variables are void, so there is no errors
 * need check and encapsulate. */

/**
 * Abort:A wrapper for the windows abort() function.
 * On windows it is a function that makes extra effort (including a call
 * to abort()) to ensure that a debugger-catchable exception is thrown
 * before the program terminates.
 */
#define COMMAND_NOT_FOUND 127

void Abort(void)
{
    /* Break the debugger. */
    DebugBreak();
    abort();
    /* Terminate the program for sure */
    ExitProcess(COMMAND_NOT_FOUND);
}

/**
 * Dynamic mode mutex initialization.
 * @param mutex: Mutex
 */
void MutexInit(Mutex *mutex)
{
    InitializeCriticalSection(&(mutex->mutex));
}

/**
 * Destroy Mutex
 * @param mutex :Mutex
 */
void MutexDestroy(Mutex *mutex)
{
    DeleteCriticalSection(&(mutex->mutex));
}

/**
 * Lock Mutex
 * @param mutex :Mutex
 */
void MutexLock(Mutex *mutex)
{
    EnterCriticalSection(&(mutex->mutex));
}

/**
 * Try lock Mutex
 * @param mutex :Mutex
 * @return : true or false.
 */
bool MutexTrylock(Mutex *mutex)
{
    BOOL rc;
    rc = TryEnterCriticalSection(&(mutex->mutex));
    if (!rc) {
        false;
    }
    return true;
}

/**
 * Unlock Mutex
 * @param mutex :Mutex
 */
void MutexUnlock(Mutex *mutex)
{
    LeaveCriticalSection(&(mutex->mutex));
}

/**
 * Dynamic mode condition variable initialization.
 * @param cond :ConditionVariable
 */
void ConditionVariableInit(ConditionVariable *cond)
{
    InitializeConditionVariable(&(cond->cond));
}

/**
 * Destroy condition variable. Windows support for Condition Variables today (as provided by Microsoft for Windows
 * Vista and later).A conditional variable is a very light-weight object that is internally based on a single global
 * kernel keyed event object that is always available through every process's entire lifetime. The conditional variable
 * simply contains a pointer to that object. So there is nothing that needs to be freed explicitly,
 * thus no delete function is needed. This interface is intended to be compatible with Linux interfaces.
 * @param cond :ConditionVariable
 */
void ConditionVariableDestroy(ConditionVariable *cond)
{
    return;
}

/**
 * Wait condition variable,used to block on a condition variable.
 * @param cond:ConditionVariable
 * @param mutex:Mutex.
 */
void ConditionVariableWait(ConditionVariable *cond, Mutex *mutex)
{
    BOOL rc;
    rc = SleepConditionVariableCS(&(cond->cond), &(mutex->mutex), INFINITE);
    if (!rc) {
        Abort();
    }
}

/**
 * The ConditionVariableTimedWait function is the same as the ConditionVariableWait function except that an
 * error is returned if the time specified by timeout passes before the condition variable is signaled.
 * @param cond:ConditionVariable
 * @param mutex:Mutex.This parameter is not used on the windows platform.
 * @param timeout:time_t  unit:milliseconds.
 * @return:true or false.
 */
bool ConditionVariableTimedWait(ConditionVariable *cond, Mutex *mutex, time_t timeout)
{
    BOOL rc;
    DWORD lastError;
    rc = SleepConditionVariableCS(&(cond->cond), &(mutex->mutex), timeout);
    if (rc) {
        return true;
    }
    lastError = GetLastError();
    if (lastError == ERROR_TIMEOUT) {
        return false;
    } else {
        Abort();
        return false;
    }
}

/**
 * The functions are used to unblock threads blocked on a condition variable.the ConditionVariableSignal call
 * unblocks at least one of the threads that are blocked on the specified condition variable (if any threads are
 * blocked on condition variable).
 * @param cond:ConditionVariable
 */
void ConditionVariableSignal(ConditionVariable *cond)
{
    WakeConditionVariable(&(cond->cond));
}

/**
 * The functions are used to unblock threads blocked on a condition variable.the ConditionVariableBroadcast call
 * unblocks at least one of the threads that are blocked on the specified condition variable (if any threads are
 * blocked on condition variable).
 * @param cond:ConditionVariable
 */
void ConditionVariableBroadcast(ConditionVariable *cond)
{
    WakeAllConditionVariable(&(cond->cond));
}