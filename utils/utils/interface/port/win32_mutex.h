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
 * win32_mutex.h
 *
 * Description:Defines the mutex external interfaces wrapper for windows platform mutex.
 *
 * ---------------------------------------------------------------------------------
 */
#ifndef UTILS_WIN32_MUTEX_H
#define UTILS_WIN32_MUTEX_H

#include <stdlib.h>
#include <windows.h>
#include <stdarg.h>
#include <stdio.h>
#include "win32_errcode.h"

GSDB_BEGIN_C_CODE_DECLS

/* Mutex  wrapper. */
typedef struct Mutex Mutex;
struct Mutex {
    CRITICAL_SECTION mutex; /* Mutex controlling the lock. */
};
/* The mutex static initialization mode is not supported on the windows platform.Windows platform cannot statically
 * initialize CRITICAL_SECTION as pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER.
 * If the dynamic library is used, you can call the dynamic initialization API when DLL_PROCESS_ATTACH is used to
 * initialize the library. Or use the following methods.
 * volatile static CRITICAL_SECTION* cs = NULL;
 *  static CRITICAL_SECTION *get() {
        CRITICAL_SECTION *inst = cs;
        if (!inst) {
            CRITICAL_SECTION* inst = new CRITICAL_SECTION();
            InitializeCriticalSection(inst);
            CRITICAL_SECTION* cur = InterlockedCompareExchange(&cs, inst, 0);
            if (cur) {
              DeleteCriticalSection(inst);
              delete inst;
              return *cur;
            }
        }
        return *inst;
    }
    Or use the following methods.
    INIT_ONCE g_initOnce = INIT_ONCE_STATIC_INIT; Invoke InitOnceExecuteOnce for initialization.
*/

/**
 * Dynamic mode mutex initialization.
 * @param mutex: Mutex
 * @return
 */
void MutexInit(Mutex *mutex);
/**
 * Destroy Mutex
 * @param mutex :Mutex
 * @return
 */
void MutexDestroy(Mutex *mutex);
/**
 * Lock Mutex
 * @param mutex :Mutex
 * @return
 */
void MutexLock(Mutex *mutex);
/**
 * Try lock Mutex
 * @param mutex :Mutex
 * @return
 */
bool MutexTrylock(Mutex *mutex);
/**
 * Unlock Mutex
 * @param mutex :Mutex
 * @return
 */
void MutexUnlock(Mutex *mutex);

/* Condition variable wrapper. */
typedef struct ConditionVariable ConditionVariable;
struct ConditionVariable {
    CONDITION_VARIABLE cond;
};
/* The condition variable static initialization mode is not supported on the windows platform. Windows platform
 * cannot statically initialize condition variable as pthread_cond_t cond = PTHREAD_COND_INITIALIZER.
 * If the dynamic library is used, you can refer the previous PTHREAD_MUTEX_INITIALIZER method. */

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
 * Wait condition variable,used to block on a condition variable.
 * @param cond:ConditionVariable
 * @param mutex:Mutex.
 * @return
 */
void ConditionVariableWait(ConditionVariable *cond, Mutex *mutex);
/**
 * The ConditionVariableTimedWait function is the same as the ConditionVariableWait function except
 * that an error is returned if the time specified by timeout passes before the condition variable is signaled.
 * @param cond:ConditionVariable
 * @param mutex:Mutex.
 * @param timeout:time_t  unit:milliseconds.
 * @return
 */
bool ConditionVariableTimedWait(ConditionVariable *cond, Mutex *mutex, time_t timeout);
/**
 * The functions are used to unblock threads blocked on a condition variable.the ConditionVariableSignal
 * call unblocks at least one of the threads that are blocked on the specified condition variable (if any threads
 * are blocked on condition variable).
 * @param cond:ConditionVariable
 * @return
 */
void ConditionVariableSignal(ConditionVariable *cond);

GSDB_END_C_CODE_DECLS
#endif /* UTILS_WIN32_MUTEX_H */
