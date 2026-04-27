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
 * fault_injection.c
 *
 * ---------------------------------------------------------------------------------
 */
#include <fcntl.h>
#include <limits.h>

#include "container/hash_table.h"
#include "syslog/err_log.h"

#include "fault_injection/fault_injection_base.h"

#ifdef LOCAL_MODULE_NAME
#undef LOCAL_MODULE_NAME
#endif
#define LOCAL_MODULE_NAME "fault_injection"

static const Size EXPECT_FI_HASH_SIZE = 64;

#define SEM_NAME_MAX_SIZE (256)

static MemoryContext g_fiGlobalMemoryContext = NULL;

static HashTab *g_fiGlobalHashTab = NULL;

static THR_LOCAL MemoryContext g_fiThreadMemoryContext = NULL;

static THR_LOCAL HashTab *g_fiThreadHashTab = NULL;

static HashTab *CreateFiHash(const char *tableName, MemoryContext memCtx)
{
    if (memCtx == NULL) {
        ErrLog(ERROR, ErrMsg("CreateFiHash, memory context is NULL."));
        return NULL;
    }
    HashCTL hctl;
    hctl.keySize = MAX_POINT_NAME_SIZE;
    hctl.entrySize = sizeof(FaultInjectionEntry);
    hctl.hash = StringHash;
    hctl.hcxt = memCtx;

    return HashCreate(tableName, EXPECT_FI_HASH_SIZE, &hctl,
                      HASH_CONTEXT | HASH_EXTERN_CONTEXT | HASH_ELEM | HASH_FUNCTION);
}

static HashTab *GetFiHashTable(enum FaultInjectionLevel level)
{
    ASSERT(level == FI_GLOBAL || level == FI_THREAD);

    if (level == FI_GLOBAL && g_fiGlobalHashTab != NULL) {
        return g_fiGlobalHashTab;
    }
    if (level == FI_THREAD && g_fiThreadHashTab != NULL) {
        return g_fiThreadHashTab;
    }

    if (level == FI_GLOBAL) {
        MemoryContext memCtx = MemoryContextCreate(NULL, MEM_CXT_TYPE_SHARE, "fiHashContext", MCTX_UNUSED, MCTX_UNUSED,
                                                   DEFAULT_UNLIMITED_SIZE);
        g_fiGlobalHashTab = CreateFiHash("global fault injection hash", memCtx);
        g_fiGlobalMemoryContext = memCtx;
        return g_fiGlobalHashTab;
    } else {
        MemoryContext memCtx = MemoryContextCreate(NULL, MEM_CXT_TYPE_GENERIC, "fiThreadHashContext", 0,
                                                   DEFAULT_UNLIMITED_SIZE, DEFAULT_UNLIMITED_SIZE);
        g_fiThreadHashTab = CreateFiHash("thread fault injection hash", memCtx);
        g_fiThreadMemoryContext = memCtx;
        return g_fiThreadHashTab;
    }
}

static void GetSemName(const char *pointName, enum FaultInjectionLevel level, Tid tid, char *semName,
                       Size semNameMaxSize)
{
    int ret;
    char *user = getenv("USER");
    if (user == NULL) {
        ErrLog(FATAL, ErrMsg("Can not get env \'USER\'."));
    }
    if (level == FI_GLOBAL) {
        ret = sprintf_s(semName, semNameMaxSize, "/%s_%s", pointName, user);
    } else {
        ret = sprintf_s(semName, semNameMaxSize, "/%s_%s_%lu", pointName, user, tid.tid);
    }

    if (ret < 0) {
        ErrLog(FATAL, ErrMsg("Format sem name failed."));
    }
}

/*
 * Look up every Fault Injection Entry in the Hash Table, creating a new entry if not present
 */
static ErrorCode InitFiEntryHash(HashTab *fiHashTab, enum FaultInjectionLevel level, FaultInjectionEntry entryArray[],
                                 int len)
{
    for (int i = 0; i < len; i++) {
        FaultInjectionEntry *entry =
            (FaultInjectionEntry *)HashSearch(fiHashTab, entryArray[i].pointName, HASH_ENTER_NULL, NULL);
        if (unlikely(entry == NULL)) {
            ErrLog(ERROR, ErrMsg("fiHashTab out of memory, pointName:%s.", entryArray[i].pointName));
            return ERROR_UTILS_FI_OUT_OF_MEMORY;
        }

        (void)strcpy_s(entry->pointName, MAX_POINT_NAME_SIZE, entryArray[i].pointName);
        entry->calledCount = 0;
        entry->isActive = entryArray[i].isActive;
        entry->func = entryArray[i].func;
        entry->skipCount = 0;
        entry->expectTriggerCount = INT_MAX;
        MutexInit(&entry->countMutex);

        char semName[SEM_NAME_MAX_SIZE] = {0};
        GetSemName(entry->pointName, level, GetCurrentTid(), semName, sizeof(semName));
        int ret = sem_unlink(semName);
        if (ret < 0 && errno != ENOENT) {
            ErrLog(ERROR, ErrMsg("unlink sem %s fail error:%d", semName, errno));
        }

        entry->semForWaitNotify = sem_open(semName, O_CREAT, S_IRWXU, 0);
        if (entry->semForWaitNotify == NULL) {
            ErrLog(ERROR, ErrMsg("sem_open %s fail error:%d(%s)", semName, errno, strerror(errno)));
            return ERROR_UTILS_FI_SEM_CREATE_FAIL;
        }
        ret = sem_init(entry->semForWaitNotify, 1, 0);
        if (ret < 0) {
            ErrLog(ERROR, ErrMsg("sem_init %s fail error:%d(%s)", semName, errno, strerror(errno)));
            return ERROR_UTILS_FI_SEM_CREATE_FAIL;
        }
    }

    return ERROR_SYS_OK;
}

/*
 * Register a global or thread local Fault Injection Entry hash table
 */
ErrorCode RegisterFaultInjection(FaultInjectionEntry entryArray[], int len, enum FaultInjectionLevel level)
{
    ASSERT(entryArray != NULL);

    HashTab *fiHashTab = GetFiHashTable(level);
    if (fiHashTab == NULL) {
        ErrLog(ERROR, ErrMsg("hash table is NULL"));
        return ERROR_UTILS_FI_OUT_OF_MEMORY;
    }

    return InitFiEntryHash(fiHashTab, level, entryArray, len);
}

static void RemoveSem(FaultInjectionEntry *node, const char *semName)
{
    int ret = sem_close(node->semForWaitNotify);
    if (ret < 0) {
        /* the sem is invalid, no need to unlink it. */
        ErrLog(ERROR, ErrMsg("sem_close %s fail error:%d(%s)", semName, errno, strerror(errno)));
        return;
    }
    ret = sem_unlink(semName);
    if (ret < 0 && errno != ENOENT) {
        ErrLog(ERROR, ErrMsg("sem_unlink %s fail error:%d(%s)", semName, errno, strerror(errno)));
    }
}

static void UnlinkAllSem(HashTab *tab, enum FaultInjectionLevel level)
{
    HashSeqStatus seqStatus;
    HashSeqInit(&seqStatus, tab);
    FaultInjectionEntry *node = (FaultInjectionEntry *)HashSeqSearch(&seqStatus);
    char semName[SEM_NAME_MAX_SIZE] = {0};
    while (node != NULL) {
        GetSemName(node->pointName, level, GetCurrentTid(), semName, sizeof(semName));
        RemoveSem(node, semName);
        node = HashSeqSearch(&seqStatus);
    }
    HashSeqTerm(&seqStatus);
}

static void DestoryAllMutex(HashTab *tab)
{
    HashSeqStatus seqStatus;
    HashSeqInit(&seqStatus, tab);
    FaultInjectionEntry *entry = (FaultInjectionEntry *)HashSeqSearch(&seqStatus);
    while (entry != NULL) {
        MutexDestroy(&entry->countMutex);
        entry = HashSeqSearch(&seqStatus);
    }
    HashSeqTerm(&seqStatus);
}

/*
 * Destroy the global or thread local Fault Injection hash table
 */
UTILS_EXPORT void DestroyFaultInjectionHash(enum FaultInjectionLevel level)
{
    if (level == FI_GLOBAL) {
        if (g_fiGlobalHashTab != NULL) {
            UnlinkAllSem(g_fiGlobalHashTab, FI_GLOBAL);
            DestoryAllMutex(g_fiGlobalHashTab);
            HashDestroy(g_fiGlobalHashTab);
            g_fiGlobalHashTab = NULL;
        }
        if (g_fiGlobalMemoryContext != NULL) {
            MemoryContextDelete(g_fiGlobalMemoryContext);
            g_fiGlobalMemoryContext = NULL;
        }
    } else {
        if (g_fiThreadHashTab != NULL) {
            UnlinkAllSem(g_fiThreadHashTab, FI_THREAD);
            DestoryAllMutex(g_fiThreadHashTab);
            HashDestroy(g_fiThreadHashTab);
            g_fiThreadHashTab = NULL;
        }
        if (g_fiThreadMemoryContext != NULL) {
            MemoryContextDelete(g_fiThreadMemoryContext);
            g_fiThreadMemoryContext = NULL;
        }
    }
}

UTILS_EXPORT FaultInjectionEntry *FindFaultInjectionEntry(const char *pointName, enum FaultInjectionLevel level)
{
    ASSERT(pointName != NULL);

    HashTab *hashTable = NULL;
    if (level == FI_GLOBAL && g_fiGlobalHashTab != NULL) {
        hashTable = g_fiGlobalHashTab;
    } else if (level == FI_THREAD && g_fiThreadHashTab != NULL) {
        hashTable = g_fiThreadHashTab;
    } else {
        return NULL;
    }
    return (FaultInjectionEntry *)HashSearch(hashTable, pointName, HASH_FIND, NULL);
}

UTILS_EXPORT void ActiveFaultInjection(const char *pointName, enum FaultInjectionLevel level, int mode, int skip,
                                       int expect)
{
    ASSERT(pointName != NULL);

    FaultInjectionEntry *entry = FindFaultInjectionEntry(pointName, level);
    if (entry != NULL) {
        entry->isActive = true;
        entry->exceptionMode = mode;
        entry->skipCount = skip;
        entry->expectTriggerCount = expect;
    }
}

UTILS_EXPORT void DeactiveFaultInjection(const char *pointName, enum FaultInjectionLevel level)
{
    ASSERT(pointName != NULL);

    FaultInjectionEntry *entry = FindFaultInjectionEntry(pointName, level);
    if (entry != NULL) {
        entry->isActive = false;
    }
}

static void WaitOnGlobalFaultInjection(const char *pointName)
{
    FaultInjectionEntry *entry = FindFaultInjectionEntry(pointName, FI_GLOBAL);
    if (entry == NULL || !entry->isActive) {
        return;
    }

    MutexLock(&entry->countMutex);
    if (entry->skipCount != 0) {
        entry->skipCount--;
        MutexUnlock(&entry->countMutex);
        return;
    }
    if (entry->calledCount >= entry->expectTriggerCount) {
        MutexUnlock(&entry->countMutex);
        return;
    }
    entry->calledCount++;
    MutexUnlock(&entry->countMutex);

    int ret = sem_wait(entry->semForWaitNotify);
    if (ret < 0) {
        ErrLog(FATAL, ErrMsg("sem_wait %s fail error:%d(%s)", entry->pointName, errno, strerror(errno)));
    }
}

static void WaitOnThreadFaultInjection(const char *pointName, Tid tid)
{
    char semName[SEM_NAME_MAX_SIZE];
    GetSemName(pointName, FI_THREAD, tid, semName, sizeof(semName));
    sem_t *sem = sem_open(semName, O_RDWR, S_IRWXU, 0);
    if (sem == NULL) {
        ErrLog(FATAL, ErrMsg("sem_open fail when WaitOnThreadLevelFaultInjection, %d(%s)", errno, strerror(errno)));
    }
    int ret = sem_wait(sem);
    if (ret < 0) {
        ErrLog(FATAL, ErrMsg("sem_wait fail, %d(%s)", errno, strerror(errno)));
    }
    ret = sem_close(sem);
    if (ret < 0) {
        ErrLog(FATAL, ErrMsg("sem_close fail, %d(%s)", errno, strerror(errno)));
    }
}

static void NotifyGlobalFaultInjection(const char *pointName)
{
    FaultInjectionEntry *entry = FindFaultInjectionEntry(pointName, FI_GLOBAL);
    if (entry == NULL || !entry->isActive) {
        return;
    }

    int ret = sem_post(entry->semForWaitNotify);
    if (ret < 0) {
        ErrLog(FATAL, ErrMsg("sem_post fail, %d(%s)", errno, strerror(errno)));
    }
}

static void NotifyThreadFaultInjection(const char *pointName, Tid tid)
{
    char semName[SEM_NAME_MAX_SIZE] = {0};
    GetSemName(pointName, FI_THREAD, tid, semName, sizeof(semName));
    sem_t *sem = sem_open(semName, O_RDWR, S_IRWXU, 0);
    if (sem == NULL) {
        ErrLog(FATAL, ErrMsg("sem_open fail when NotifyThreadLevelFaultInjection, %d(%s)", errno, strerror(errno)));
    }
    int ret = sem_post(sem);
    if (ret < 0) {
        ErrLog(FATAL, ErrMsg("sem_post fail, %d(%s)", errno, strerror(errno)));
    }
    ret = sem_close(sem);
    if (ret < 0) {
        ErrLog(FATAL, ErrMsg("sem_close fail, %d(%s)", errno, strerror(errno)));
    }
}

UTILS_EXPORT void WaitOnFaultInjection(const char *pointName, enum FaultInjectionLevel level, Tid tid)
{
    ASSERT(pointName != NULL);

    if (level == FI_GLOBAL) {
        WaitOnGlobalFaultInjection(pointName);
    } else {
        WaitOnThreadFaultInjection(pointName, tid);
    }
}

UTILS_EXPORT void NotifyFaultInjection(const char *pointName, enum FaultInjectionLevel level, Tid tid)
{
    ASSERT(pointName != NULL);

    if (level == FI_GLOBAL) {
        NotifyGlobalFaultInjection(pointName);
    } else {
        NotifyThreadFaultInjection(pointName, tid);
    }
}
