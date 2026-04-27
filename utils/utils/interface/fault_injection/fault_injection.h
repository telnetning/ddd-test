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
 * fault_injection.h
 * These APIs are suitable for failure points name which are defined using enumerated types
 * ---------------------------------------------------------------------------------
 */
#ifndef UTILS_FAULT_INJECTION_H
#define UTILS_FAULT_INJECTION_H

#include <limits.h>
#include "fault_injection_base.h"

GSDB_BEGIN_C_CODE_DECLS

#ifdef ENABLE_FAULT_INJECTION

// These are the interface required by source code
/*
 * The CALL REPLACE will first replace the function of matched FaultInjection(FI)
 * point in the global FI hash table. If it does not find a match in the global
 * FI hash table, it will execute the same code on thread local hash table.
 */
#define FAULT_INJECTION_CALL_REPLACE(point, ...)                                                     \
    do {                                                                                             \
        FaultInjectionEntry *faultInjectionEntry = FindFaultInjectionEntry(#point, FI_GLOBAL);       \
        if (faultInjectionEntry == NULL) {                                                           \
            faultInjectionEntry = FindFaultInjectionEntry(#point, FI_THREAD);                        \
        }                                                                                            \
        if (faultInjectionEntry != NULL && faultInjectionEntry->isActive) {                          \
            bool isNeedDoAction = false;                                                             \
            MutexLock(&faultInjectionEntry->countMutex);                                             \
            if (faultInjectionEntry->skipCount != 0) {                                               \
                faultInjectionEntry->skipCount--;                                                    \
            } else if (faultInjectionEntry->calledCount < faultInjectionEntry->expectTriggerCount) { \
                faultInjectionEntry->calledCount++;                                                  \
                isNeedDoAction = true;                                                               \
            }                                                                                        \
            MutexUnlock(&faultInjectionEntry->countMutex);                                           \
            if (isNeedDoAction && faultInjectionEntry->func != NULL) {                               \
                faultInjectionEntry->func(faultInjectionEntry, __VA_ARGS__);                         \
            }                                                                                        \
        } else {

#define FAULT_INJECTION_CALL_REPLACE_END \
    }                                    \
    }                                    \
    while (0)

#define FAULT_INJECTION_CALL(point, ...)             \
    FAULT_INJECTION_CALL_REPLACE(point, __VA_ARGS__) \
    FAULT_INJECTION_CALL_REPLACE_END

/*
 * If the global or thread-level fault injection matches, then this *action* will be executed
 */
#define FAULT_INJECTION_ACTION(point, action)                                                        \
    do {                                                                                             \
        FaultInjectionEntry *faultInjectionEntry = FindFaultInjectionEntry(#point, FI_GLOBAL);       \
        if (faultInjectionEntry == NULL) {                                                           \
            faultInjectionEntry = FindFaultInjectionEntry(#point, FI_THREAD);                        \
        }                                                                                            \
        if (faultInjectionEntry != NULL && faultInjectionEntry->isActive) {                          \
            MutexLock(&faultInjectionEntry->countMutex);                                             \
            bool isNeedDoAction = false;                                                             \
            if (faultInjectionEntry->skipCount != 0) {                                               \
                faultInjectionEntry->skipCount--;                                                    \
            } else if (faultInjectionEntry->calledCount < faultInjectionEntry->expectTriggerCount) { \
                faultInjectionEntry->calledCount++;                                                  \
                isNeedDoAction = true;                                                               \
            }                                                                                        \
            MutexUnlock(&faultInjectionEntry->countMutex);                                           \
            if (isNeedDoAction) {                                                                    \
                action;                                                                              \
            }                                                                                        \
        }                                                                                            \
    } while (0)

#define FAULT_INJECTION_RETURN(point, val) FAULT_INJECTION_ACTION(point, return (val))

// These are the interfaces required by the test code
// clang-format off
#define FAULT_INJECTION_ENTRY(point, active, callback)                             \
    {                                                                              \
        #point, active, 0, 0, 0, INT_MAX, NULL, (FaultInjectionCallback)(callback) \
    }
// clang-format on

#define FAULT_INJECTION_ACTIVE_MODE_LEVEL(point, mode, level, skip, expect) \
    do {                                                                    \
        ActiveFaultInjection(#point, level, mode, skip, expect);            \
    } while (0)

#define FAULT_INJECTION_ACTIVE(point, level) FAULT_INJECTION_ACTIVE_MODE_LEVEL(point, 0, level, 0, INT_MAX)

#define FAULT_INJECTION_INACTIVE(point, level) \
    do {                                       \
        DeactiveFaultInjection(#point, level); \
    } while (0)

/**
 * Wait on a global level fault injection point.
 *
 * @param point: fault injection point
 */
#define FAULT_INJECTION_WAIT(point)                      \
    do {                                                 \
        Tid waitId = {0};                                \
        WaitOnFaultInjection(#point, FI_GLOBAL, waitId); \
    } while (0)

/**
 * Wait on a thread level fault injection point.
 *
 * @param tid: thread id which get from `GetCurrentTid` method
 * @param point: fault injection point
 */
#define FAULT_INJECTION_THREAD_LEVEL_WAIT(tid, point) \
    do {                                              \
        WaitOnFaultInjection(#point, FI_THREAD, tid); \
    } while (0)

/**
 * Notify the threads which are waitting on the specify fault injection point to wake up.
 *
 * @param point: fault injection point
 */
#define FAULT_INJECTION_NOTIFY(point)                      \
    do {                                                   \
        Tid notifyId = {0};                                \
        NotifyFaultInjection(#point, FI_GLOBAL, notifyId); \
    } while (0)

/**
 * Notify the threads which are waitting on the specify thread level fault injection point to wake up.
 *
 * @param tid: thread id which get from `GetCurrentTid` method
 * @param point: fault injection point
 */
#define FAULT_INJECTION_THREAD_LEVEL_NOTIFY(tid, point) \
    do {                                                \
        NotifyFaultInjection(#point, FI_THREAD, tid);   \
    } while (0)

#else

#define FAULT_INJECTION_ACTIVE_MODE_LEVEL(point, mode, level, skip, expect)
#define FAULT_INJECTION_ACTIVE(point, level)
#define FAULT_INJECTION_INACTIVE(point, level)
#define FAULT_INJECTION_CALL_REPLACE(point, ...)
#define FAULT_INJECTION_CALL_REPLACE_END
#define FAULT_INJECTION_CALL(point, ...)
#define FAULT_INJECTION_ACTION(point, action)
#define FAULT_INJECTION_RETURN(point, val)
#define FAULT_INJECTION_WAIT(point)
#define FAULT_INJECTION_THREAD_LEVEL_WAIT(tid, point)
#define FAULT_INJECTION_NOTIFY(point)
#define FAULT_INJECTION_THREAD_LEVEL_NOTIFY(tid, point)

#endif

GSDB_END_C_CODE_DECLS

#endif // UTILS_FAULT_INJECTION_H
