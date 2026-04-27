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
 * fault_injection_base.h
 *
 * ---------------------------------------------------------------------------------
 */
#ifndef UTILS_FAULT_INJECTION_BASE_H
#define UTILS_FAULT_INJECTION_BASE_H

#include <semaphore.h>
#include <stdbool.h>

#include "defines/utils_errorcode.h"
#include "port/posix_thread.h"
#include "port/posix_mutex.h"

GSDB_BEGIN_C_CODE_DECLS

#define MAX_POINT_NAME_SIZE 64

enum FaultInjectionLevel {
    FI_GLOBAL = 0,
    FI_THREAD,
};

typedef struct FaultInjectionEntry FaultInjectionEntry;

typedef void (*FaultInjectionCallback)(const FaultInjectionEntry *entry, ...);

/*
 * FaultInjectinonEntry which contains the information of a injection point.
 */
struct FaultInjectionEntry {
    /* point injection name */
    char pointName[MAX_POINT_NAME_SIZE];
    /* indicate if the point is active, if the point is active, the fault action will be triggered */
    bool isActive;
    /* indicate the exception mode which can trigger different fault */
    int exceptionMode;
    /* the invoke count of this point */
    int calledCount;
    /* the point will skip `skipCount` times before it is triggered */
    int skipCount;
    /* when the point trigger count reach the `expectTriggerCount` the point will not be triggered */
    int expectTriggerCount;
    /* the semaphore which use to corrdinate between the threads */
    sem_t *semForWaitNotify;
    /* callback function which can replace the original logic to trigger fault */
    FaultInjectionCallback func;
    /* mutex used to ensue skipCount and calledCount thread safe */
    Mutex countMutex;
};

/* Error code of fault injection */
#define ERROR_UTILS_FI_OUT_OF_MEMORY   MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_FAULT_INJECTION_MODULE_ID, 0x00000001)
#define ERROR_UTILS_FI_SEM_CREATE_FAIL MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_FAULT_INJECTION_MODULE_ID, 0x00000002)

/*
 * Register a global or thread local FaultInjection(FI) hash table.
 *
 * e.g.: If a global FI hash table is registered for all threads,
 * one thread activate a FI point, the FI points of other threads
 * will be activated as well.
 * e.g.: If a thread register its own FI hash table, it will be able
 * to activate its own FI point without the interference from other threads.
 *
 * The FaultInjectionEntry with the same name will be overwritten. To avoid duplication,
 * it is recommended that the fault point name contains the module name.
 *
 * RegisterFaultInjection may encounter some error, it should call `DestroyFaultInjectionHash` destroy the hash table
 * before register again.
 *
 * WARNNING: this function is not thread safe.
 */
ErrorCode RegisterFaultInjection(FaultInjectionEntry entryArray[], int len, enum FaultInjectionLevel level);

/*
 * Destroy the global or thread local FaultInjection hash table.
 * Each thread need to destroy its own FI hash table.
 *
 * WARNNING: this function is not thread safe.
 */
void DestroyFaultInjectionHash(enum FaultInjectionLevel level);

/*
 * Find an FaultInjectionEntry from global or thread local FaultInjection hash table.
 * When level is FI_GLOBAL, the hash table is initialized before concurrency starts, so there is no concurrency problem
 * when calling this function.
 * When level is FI_THREAD, the hash table is monopolized by thread, there is no concurrency problem.
 *
 * WARNNING: this function is not thread safe.
 */
FaultInjectionEntry *FindFaultInjectionEntry(const char *pointName, enum FaultInjectionLevel level);

/*
 * The two functions below are used to activate or deactivate FaultInjection. The caller must ensure that these
 * functions are called only once for a specified FaultInjection.
 *
 * WARNNING: this function is not thread safe.
 */
void ActiveFaultInjection(const char *pointName, enum FaultInjectionLevel level, int mode, int skip, int expect);
void DeactiveFaultInjection(const char *pointName, enum FaultInjectionLevel level);

/*
 * The two functions below are thread safe.
 */
void WaitOnFaultInjection(const char *pointName, enum FaultInjectionLevel level, Tid tid);
void NotifyFaultInjection(const char *pointName, enum FaultInjectionLevel level, Tid tid);

GSDB_END_C_CODE_DECLS

#endif // UTILS_FAULT_INJECTION_BASE_H
