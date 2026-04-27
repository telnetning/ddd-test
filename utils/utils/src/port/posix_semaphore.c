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
 * posix_semaphore.c
 *
 * Description:
 * 1. Implementation of the POSIX semaphore interface wrapper.
 *
 * ---------------------------------------------------------------------------------
 */

#if defined(USE_POSIX_SEMAPHORES)
#include <stdbool.h>
#include <errno.h>
#include "port/posix_errcode.h"
#include "defines/abort.h"
#include "port/posix_time.h"
#include "port/posix_page.h"
#include "port/posix_semaphore.h"

/*
 * Create Semaphore.
 * If emergency signal processing is not required during semaphore waiting,
 * the semaphoreAttribute can be set to NULL.
 * POSIX semaphore have two kind,one is unnamed style,another is named style.
 * We prefer the unnamed style of POSIX semaphore which is sem_init.
 * semaphoreKey is not used on unnamed style posix semaphore.
 * */
void SemaphoreCreate(int semaphoreKey, SemaphoreAttribute *semaphoreAttribute, Semaphore *semaphore)
{
    int rc;
    (void)semaphoreKey;
#define SEMAPHORE_THREAD_SHARE 0
#define SEMAPHORE_INIT_VALUE   1
    rc = sem_init(&(semaphore->semaphore), SEMAPHORE_THREAD_SHARE, SEMAPHORE_INIT_VALUE);
    if (rc < 0) {
        Abort();
    }
    if (semaphoreAttribute != NULL) {
        semaphore->semaphoreAttribute.threadContext = semaphoreAttribute->threadContext;
        semaphore->semaphoreAttribute.enableInterruptsCallBack = semaphoreAttribute->enableInterruptsCallBack;
        semaphore->semaphoreAttribute.processInterruptsCallBack = semaphoreAttribute->processInterruptsCallBack;
    } else {
        semaphore->semaphoreAttribute.threadContext = NULL;
        semaphore->semaphoreAttribute.enableInterruptsCallBack = NULL;
        semaphore->semaphoreAttribute.processInterruptsCallBack = NULL;
    }
}

/* Destroy semaphore related resources. */
void SemaphoreDestroy(Semaphore *semaphore)
{
    int rc;
    if (semaphore != NULL) {
        rc = sem_destroy(&(semaphore->semaphore));
        if (rc < 0) {
            Abort();
        }
        semaphore->semaphoreAttribute.threadContext = NULL;
        semaphore->semaphoreAttribute.enableInterruptsCallBack = NULL;
        semaphore->semaphoreAttribute.processInterruptsCallBack = NULL;
    }
    return;
}

/* Try semaphore lock. */
bool SemaphoreTryLock(Semaphore *semaphore)
{
    int rc;
    /*
     * If rc is -1 and errno == EINTR then it means we returned
     * from the operation prematurely because we were sent a signal.  So we
     * try and lock the semaphore again.
     */
    do {
        rc = sem_trywait(&(semaphore->semaphore));
    } while (rc < 0 && errno == EINTR);

    if (rc < 0) {
        if (errno == EAGAIN || errno == EDEADLK) {
            /* Failed to lock it. */
            return false;
        } else {
            /* Otherwise we got trouble. */
            Abort();
        }
    }
    return true;
}

/* Reset a previously-initialized Semaphore to count 0. */
void SemaphoreReset(Semaphore *semaphore)
{
    int rc;
    /*
     * There's no direct API for this in POSIX, so we have to use sem_trywait to ratchet the
     * semaphore down to 0.
     */
    for (;;) {
        rc = sem_trywait(&(semaphore->semaphore));
        if (rc < 0) {
            if (errno == EAGAIN || errno == EDEADLK) {
                break; /* got it down to 0 */
            } else if (errno == EINTR) {
                continue;
            } else {
                /* Otherwise we got trouble. */
                Abort();
            }
        }
    }
}

/* Semaphore lock timed wait. */
ErrorCode SemaphoreLockTimedwait(Semaphore *semaphore, bool interruptOK, int milliseconds)
{
    int rc;
    struct timespec abstime;
    /*
     * We handle the interrupt case where sem_wait()
     * returns errno == EINTR after a signal, and the case where it just keeps waiting.
     */
    if (milliseconds > 0) {
        ConvertMillisecondsToAbsoluteTime(milliseconds, &abstime);
    }
    do {
        if (semaphore->semaphoreAttribute.enableInterruptsCallBack != NULL) {
            semaphore->semaphoreAttribute.enableInterruptsCallBack(semaphore->semaphoreAttribute.threadContext,
                                                                   interruptOK);
        }
        if (semaphore->semaphoreAttribute.processInterruptsCallBack != NULL) {
            semaphore->semaphoreAttribute.processInterruptsCallBack(semaphore->semaphoreAttribute.threadContext);
        }
        if (milliseconds < 0) {
            rc = sem_wait(&(semaphore->semaphore));
        } else {
            rc = sem_timedwait(&(semaphore->semaphore), &abstime);
        }
        if (semaphore->semaphoreAttribute.enableInterruptsCallBack != NULL) {
            semaphore->semaphoreAttribute.enableInterruptsCallBack(semaphore->semaphoreAttribute.threadContext, false);
        }
    } while (rc < 0 && errno == EINTR);

    if (rc == 0) {
        return ERROR_SYS_OK;
    } else if (rc < 0 && errno == ETIMEDOUT) {
        return ERROR_UTILS_PORT_ETIMEDOUT;
    } else {
        /* Otherwise we got trouble. */
        Abort();
        return ERROR_UTILS_PORT_UNKNOWN; /* Couldn't reach here, only keep compiler quiet. */
    }
}

/* Semaphore unlock. */
void SemaphoreUnlock(Semaphore *semaphore)
{
    int rc;
    /*
     * If rc is -1 and errno == EINTR then it means we returned
     * from the operation prematurely because we were sent a signal.  So we
     * try and unlock the semaphore again.
     */
    do {
        rc = sem_post(&(semaphore->semaphore));
    } while (rc < 0 && errno == EINTR);

    if (rc < 0) {
        Abort();
    }
}

/*
 * Create semaphore set.
 * If emergency signal processing is not required during semaphore waiting,
 * the semaphoreAttribute can be set to NULL.
 * */
SemaphoreSet *SemaphoreSetCreate(int semaphoreStartKey, uint maxSemaphore, SemaphoreAttribute *semaphoreAttribute)
{
    SemaphoreSet *semaphoreSet = NULL;
    size_t setSize = (size_t)(maxSemaphore * sizeof(Semaphore) + sizeof(SemaphoreSet));
    semaphoreSet = (SemaphoreSet *)MemPagesAlloc(setSize);
    if (semaphoreSet == NULL) {
        Abort();
    }
    semaphoreSet->setSize = setSize;
    semaphoreSet->semaphore = (Semaphore *)(semaphoreSet + 1);
    uint i;
    int semaphoreKey;
    for (i = 0; i < maxSemaphore; i++) {
        semaphoreKey = semaphoreStartKey + (int)(i);
        SemaphoreCreate(semaphoreKey, semaphoreAttribute, (semaphoreSet->semaphore + i));
    }
    semaphoreSet->maxSemaphore = maxSemaphore;
    return semaphoreSet;
}

/* Destroy semaphore set related resources. */
void SemaphoreSetDestroy(SemaphoreSet *semaphoreSet)
{
    if (semaphoreSet != NULL) {
        uint i;
        for (i = 0; i < semaphoreSet->maxSemaphore; i++) {
            SemaphoreDestroy(semaphoreSet->semaphore + i);
        }
        MemPagesFree((void *)semaphoreSet, semaphoreSet->setSize);
    }
}

#endif
