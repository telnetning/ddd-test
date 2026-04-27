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
 * win32_semaphore.c
 *
 * Description:
 * 1. Implementation of the Windows semaphore interface wrapper.
 *
 * ---------------------------------------------------------------------------------
 */
#include "port/win32_semaphore.h"

/*
 * Create Semaphore.
 * If emergency signal processing is not required during semaphore waiting,
 * the semaphoreAttribute can be set to NULL.
 * semaphoreKey is not used on the Windows platform.
 * */
void SemaphoreCreate(int semaphoreKey, SemaphoreAttribute *semaphoreAttribute, Semaphore *semaphore)
{
    HANDLE currentHandle = NULL;
    SECURITY_ATTRIBUTES securityAttributes;
    ZeroMemory(&securityAttributes, sizeof(securityAttributes));
    securityAttributes.nLength = sizeof(securityAttributes);
    securityAttributes.lpSecurityDescriptor = NULL;
    securityAttributes.bInheritHandle = TRUE;
#define SEMAPHORE_INITIAL_COUNT 1
#define SEMAPHORE_MAXIMUM_COUNT 32767
    /* Create an anonymous semaphore */
    currentHandle = CreateSemaphore(&securityAttributes, SEMAPHORE_INITIAL_COUNT, SEMAPHORE_MAXIMUM_COUNT, NULL);
    if (currentHandle != NULL) {
        /* Successfully done. */
        semaphore->semaphore = currentHandle;
    } else {
        semaphore->semaphore = NULL;
        Abort();
    }
    if (semaphoreAttribute != NULL) {
        semaphore->semaphoreAttribute.threadContext = semaphoreAttribute->threadContext;
        semaphore->semaphoreAttribute.enableInterruptsCallBack = semaphoreAttribute->enableInterrupts;
        semaphore->semaphoreAttribute.processInterruptsCallBack = semaphoreAttribute->processInterrupts;
    } else {
        semaphore->semaphoreAttribute.threadContext = NULL;
        semaphore->semaphoreAttribute.enableInterruptsCallBack = NULL;
        semaphore->semaphoreAttribute.processInterruptsCallBack = NULL;
    }
}

/* Destroy semaphore related resources. */
void SemaphoreDestroy(Semaphore *semaphore)
{
    BOOL rc;
    if (semaphore != NULL) {
        rc = CloseHandle(semaphore->semaphore);
        if (!rc) {
            Abort();
        }
        semaphore->semaphore = NULL;
        semaphore->semaphoreAttribute = NULL;
    }
    return;
}

/* Try semaphore lock. */
bool SemaphoreTryLock(Semaphore *semaphore)
{
    DWORD returnCode;
    returnCode = WaitForSingleObject(semaphore->semaphore, 0);
    if (returnCode == WAIT_OBJECT_0) {
        /* Wait succeed! */
        return true;
    } else if (returnCode == WAIT_TIMEOUT) {
        /* Time out fail! */
        errno = EAGAIN;
        return false;
    }
    /* Otherwise we are in trouble and abort. */
    Abort();
    /* keep compiler quiet. */
    return false;
}

/* Reset a previously-initialized Semaphore to count 0. */
void SemaphoreReset(Semaphore *semaphore)
{
    /*
     * There's no direct API for this on Win32, so we have to use SemaphoreTryLock to ratchet down
     * the semaphore to 0.
     */
    while (SemaphoreTryLock(semaphore)) {
    }
}

/* Semaphore lock timed wait. */
ErrorCode SemaphoreLockTimedwait(Semaphore *semaphore, bool interruptOK, int milliseconds)
{
    DWORD retCode;
#define SEMAPHORE_WAIT_HANDLE_COUNT 1
    HANDLE waitHandle[SEMAPHORE_WAIT_HANDLE_COUNT];
    waitHandle[0] = semaphore->semaphore;
    /*
     * While waiting for the semaphore, check for cancel/die interrupts each time through the loop.
     */
    do {
        if (semaphore->semaphoreAttribute.enableInterruptsCallBack != NULL) {
            semaphore->semaphoreAttribute.enableInterruptsCallBack(semaphore->semaphoreAttribute.threadContext,
                                                                   interruptOK);
        }
        if (semaphore->semaphoreAttribute.processInterruptsCallBack != NULL) {
            semaphore->semaphoreAttribute.processInterruptsCallBack(semaphore->semaphoreAttribute.threadContext);
        }
        if (milliseconds < 0) {
            retCode = WaitForMultipleObjectsEx(SEMAPHORE_WAIT_HANDLE_COUNT, waitHandle, FALSE, INFINITE, TRUE);
        } else {
            retCode = WaitForMultipleObjectsEx(SEMAPHORE_WAIT_HANDLE_COUNT, waitHandle, FALSE, milliseconds, TRUE);
        }
        if (retCode == WAIT_OBJECT_0) {
            /* Succeed. */
            errno = 0;
            return ERROR_SYS_OK;
        } else if (retCode == WAIT_TIMEOUT) {
            return ERROR_UTILS_PORT_ETIMEDOUT;
        } else {
            /* Otherwise we are in trouble */
            errno = EIDRM;
        }
        if (semaphore->semaphoreAttribute.enableInterruptsCallBack != NULL) {
            semaphore->semaphoreAttribute.enableInterruptsCallBack(semaphore->semaphoreAttribute.threadContext, false);
        }
    } while (errno == EINTR);
    if (errno != 0) {
        Abort();
    }
}

/* Semaphore unlock. */
void SemaphoreUnlock(Semaphore *semaphore)
{
    if (!ReleaseSemaphore(semaphore->semaphore, 1, NULL)) {
        Abort();
    }
    return;
}

/*
 * Create semaphore set.
 * If emergency signal processing is not required during semaphore waiting,
 * the interrupt handling callback function can be set to NULL.
 * semaphoreStartKey is not used on the Windows platform.
 * */
SemaphoreSet *SemaphoreSetCreate(int semaphoreStartKey, int maxSemaphore, SemaphoreAttribute *semaphoreAttribute)
{
    SemaphoreSet *semaphoreSet = NULL;
    size_t setSize = maxSemaphore * sizeof(Semaphore) + sizeof(SemaphoreSet);
    semaphoreSet = (SemaphoreSet *)MemPagesAlloc(setSize);
    if (semaphoreSet == NULL) {
        Abort();
    }
    semaphoreSet->setSize = setSize;
    semaphoreSet->semaphore = (Semaphore *)(semaphoreSet + 1);
    int i;
    int semaphoreKey;
    for (i = 0; i < maxSemaphore; i++) {
        semaphoreKey = semaphoreKey + i;
        SemaphoreCreate(semaphoreKey, semaphoreAttribute, (semaphoreSet->semaphore + i));
    }
    semaphoreSet->maxSemaphore = maxSemaphore;
    return semaphoreSet;
}

/* Destroy semaphore set related resources. */
void SemaphoreSetDestroy(SemaphoreSet *semaphoreSet)
{
    if (semaphoreSet != NULL) {
        int i;
        for (i = 0; i < semaphoreSet->maxSemaphore; i++) {
            CloseHandle(semaphoreSet->semaphore[i].semaphore);
        }
        MemPagesFree((void *)semaphoreSet, semaphoreSet->setSize);
    }
}
