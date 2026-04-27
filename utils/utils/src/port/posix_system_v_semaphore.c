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
 * posix_system_v_semaphore.c
 *
 * Description:
 * 1. Implementation of the system v semaphore interface wrapper.
 *
 * ---------------------------------------------------------------------------------
 */

#ifndef USE_POSIX_SEMAPHORES
#include <stdbool.h>
#include <errno.h>
#include <unistd.h>
#include <sys/types.h>
#include <signal.h>
#include <sys/ipc.h>
#include <sys/sem.h>
#include "port/posix_errcode.h"
#include "defines/abort.h"
#include "port/posix_time.h"
#include "port/posix_page.h"
#include "port/posix_system_v_semaphore.h"

/* Magic is used to detect whether the conflicting semaphore is created by the same service process. */
#define SEMAPHORE_DEFAULT_MAGIC (538)  /* Must be less than SEMVMX. */
#define SEMAPHORE_PROTECTION    (0600) /* Access/modify by user only. */
/*
 * SEMAPHORES_PER_SET is the number of useful semaphores in each semaphore set we allocate.
 * It must be less than your kernel's SEMMSL parameter,which is often around 25.
 * Actually, we'll allocate one extra sema in each set for identification purposes.
 */
#define SEMAPHORES_PER_SET (16)
/*
 * The following structure is defined based on OS requirements.
 * union semun are identified by __attribute__((unused)) to keep codecheck quiet.
 * OS requirements the calling program must define the union semun by ourselves as follows:
 */
union semun {
    int val;                    /* Value for SETVAL. */
    struct semid_ds *buf;       /* Buffer for IPC_STAT, IPC_SET. */
    unsigned short *array;      /* Array for GETALL, SETALL. */
    struct seminfo *ipcInfoBuf; /* Buffer for IPC_INFO(Linux-specific). */
} __attribute__((unused));

/*
 * Set semaphore number per set attribute.
 * Currently, The api is not open to external modules because the current semaphore operation
 * interface does not support simultaneous operations on multiple semaphores in the same semaphore set.
 * Only one semaphore can be operated at a time.
 */
static inline void SemaphoreAttributeSetSemaphoresPerSet(SemaphoreAttribute *semaphoreAttribute,
                                                         unsigned short semaphoresPerSet)
{
    semaphoreAttribute->semaphoresPerSet = semaphoresPerSet;
}

/*
 * Copy semaphore from source attribute to target attribute.
 */
static void SemaphoreAttributeCopy(SemaphoreAttribute *targetSemaphoreAttribute,
                                   SemaphoreAttribute *sourceSemaphoreAttribute)
{
    if (sourceSemaphoreAttribute != NULL) {
        targetSemaphoreAttribute->threadContext = sourceSemaphoreAttribute->threadContext;
        targetSemaphoreAttribute->enableInterruptsCallBack = sourceSemaphoreAttribute->enableInterruptsCallBack;
        targetSemaphoreAttribute->processInterruptsCallBack = sourceSemaphoreAttribute->processInterruptsCallBack;
        targetSemaphoreAttribute->semaphoresPerSet = sourceSemaphoreAttribute->semaphoresPerSet;
        targetSemaphoreAttribute->semaphoreMagicPerSet = sourceSemaphoreAttribute->semaphoreMagicPerSet;
    }
}
/*
 * Internal create semaphore.
 */
static void InternalSemaphoreCreate(int semaphoreKey, unsigned short semaphoresPerSet, int semaphoreMagicPerSet,
                                    int *semaphoreSetId)
{
    int semaphoreId;
    union semun semaphoreUnion;
    int semaphoreMagic;
    pid_t creatorPID;
    int nextSemaphoreKey;
    unsigned short semaphoreNum;
    /* Loop till we find a free semaphore key. */
    for (nextSemaphoreKey = semaphoreKey;; nextSemaphoreKey++) {
        /* +1 for semaphore magic. */
        semaphoreId = semget(nextSemaphoreKey, semaphoresPerSet + 1, IPC_CREAT | IPC_EXCL | SEMAPHORE_PROTECTION);
        if (semaphoreId >= 0) {
            break; /* Successful create. */
        } else {
            /*
             * If error not indicates a collision with existing set and not a permission violation
             * and not an old set is slated for destruction but not gone yet.
             * We are in trouble.
             */
            if (errno != EEXIST && errno != EACCES
#ifdef EIDRM
                && errno != EIDRM
#endif
            ) {
                Abort();
            }
        }
        /* See if it looks to be leftover from a dead process. */
        semaphoreId = semget(nextSemaphoreKey, semaphoresPerSet + 1, 0);
        if (semaphoreId < 0) {
            continue; /* Failed: must be some other app's. */
        }
        semaphoreUnion.val = 0; /* Unused. */
        /* The semaphores in a set are numbered starting at 0. Magic location is last. */
        semaphoreNum = semaphoresPerSet;
        semaphoreMagic = semctl(semaphoreId, semaphoreNum, GETVAL, semaphoreUnion);
        if (semaphoreMagic != semaphoreMagicPerSet) {
            continue; /* Semaphore belongs to other app. */
        }
        /*
         * If the creator PID is my own PID or does not belong to any extant
         * process, it's safe to remove it.
         * PID location use magic location, see the following notes.
         */
        semaphoreUnion.val = 0; /* Unused. */
        creatorPID = semctl(semaphoreId, semaphoreNum, GETPID, semaphoreUnion);
        if (creatorPID <= 0) {
            continue; /* GETPID failed. */
        }
        if (creatorPID != getpid()) {
            if (kill(creatorPID, 0) == 0 || errno != ESRCH) {
                continue; /* Semaphore belongs to a live process. */
            }
        }
        /*
         * The semaphore set appears to be from a dead process, or from a
         * previous cycle of life in this same process.  Remove it, if possible.
         */
        semaphoreUnion.val = 0; /* Unused, but keep compiler quiet */
        if (semctl(semaphoreId, 0, IPC_RMID, semaphoreUnion) < 0) {
            continue;
        }
        /*
         * Now try again to create the semaphore set.
         */
        semaphoreId = semget(nextSemaphoreKey, semaphoresPerSet + 1, IPC_CREAT | IPC_EXCL | SEMAPHORE_PROTECTION);
        if (semaphoreId >= 0) {
            break; /* Successful create. */
        }
        /*
         * Can only get here if some other process create the same semaphore key before we did.
         * Let him have that one, loop around to try next key.
         */
    }
    /*
     * OK, we created a new semaphore set. Mark it as created by this process. We do this by setting
     * the spare semaphore to semaphoreMagicPerSet-1 and then incrementing it with semop().
     * That leaves it with value semaphoreMagicPerSet and sempid referencing this process.
     */
    semaphoreUnion.val = semaphoreMagicPerSet - 1;
    semaphoreNum = semaphoresPerSet;
    if (semctl(semaphoreId, semaphoreNum, SETVAL, semaphoreUnion) < 0) {
        Abort();
    }
    int rc;
    struct sembuf semaphoreOps;
    semaphoreOps.sem_op = 1; /* Increment. */
    semaphoreOps.sem_flg = 0;
    semaphoreOps.sem_num = semaphoreNum;
    /*
     *  If rc is -1 and errno == EINTR then it means we returned
     *  from the operation prematurely because we were sent a signal.  So we
     *  try and unlock the semaphore again.
     */
    do {
        rc = semop(semaphoreId, &semaphoreOps, 1);
    } while (rc < 0 && errno == EINTR);

    if (rc < 0) {
        Abort();
    }
    *semaphoreSetId = semaphoreId;
    return;
}

/*
 * Internal initialize semaphore,initialize a semaphore to the specified value.
 */
static void InternalSemaphoreInitialize(Semaphore *semaphore, int value)
{
    int rc;
    union semun semaphoreUnion;
    semaphoreUnion.val = value;
    rc = semctl(semaphore->semaphoreId, semaphore->semaphoreNum, SETVAL, semaphoreUnion);
    if (rc < 0) {
        Abort();
    }
    return;
}
/*
 * Create Semaphore.
 * If emergency signal processing is not required during semaphore waiting,
 * and use the default number of semaphores and default magic in each semaphore set,
 * the semaphoreAttribute can be set to NULL.
 * semaphoreKey is passed for possible use as a key. For system v semaphore, we use
 * it to generate the starting semaphore key.
 * */
void SemaphoreCreate(int semaphoreKey, SemaphoreAttribute *semaphoreAttribute, Semaphore *semaphore)
{
    unsigned short semaphoresPerSet;
    int semaphoreMagicPerSet;
    int semaphoreSetId;
    semaphoresPerSet = 1;
    semaphoreMagicPerSet = SEMAPHORE_DEFAULT_MAGIC;
    if (semaphoreAttribute != NULL && semaphoreAttribute->semaphoresPerSet > 0) {
        semaphoresPerSet = semaphoreAttribute->semaphoresPerSet;
    }
    if (semaphoreAttribute != NULL && semaphoreAttribute->semaphoreMagicPerSet > 0) {
        semaphoreMagicPerSet = semaphoreAttribute->semaphoreMagicPerSet;
    }
    InternalSemaphoreCreate(semaphoreKey, semaphoresPerSet, semaphoreMagicPerSet, &semaphoreSetId);
    semaphore->semaphoreAttribute.semaphoresPerSet = semaphoresPerSet;
    semaphore->semaphoreAttribute.semaphoreMagicPerSet = semaphoreMagicPerSet;
    if (semaphoreAttribute != NULL) {
        semaphore->semaphoreAttribute.threadContext = semaphoreAttribute->threadContext;
        semaphore->semaphoreAttribute.enableInterruptsCallBack = semaphoreAttribute->enableInterruptsCallBack;
        semaphore->semaphoreAttribute.processInterruptsCallBack = semaphoreAttribute->processInterruptsCallBack;
    } else {
        semaphore->semaphoreAttribute.threadContext = NULL;
        semaphore->semaphoreAttribute.enableInterruptsCallBack = NULL;
        semaphore->semaphoreAttribute.processInterruptsCallBack = NULL;
    }
    /*
     * Currently, one semaphore has only one member. A semaphore cannot contain multiple members.
     * The semaphore ID of the semaphore set returned by the OS is used as the semaphore ID.
     * The number of the semaphore is the set cout minus 1. The actual value is 0.
     * The semaphores in a set are numbered starting at 0.
     * */
    semaphore->semaphoreId = semaphoreSetId;
    semaphore->semaphoreNum = (unsigned short)(semaphoresPerSet - 1);
    InternalSemaphoreInitialize(semaphore, 1);
}

/* Destroy semaphore related resources. */
void SemaphoreDestroy(Semaphore *semaphore)
{
    int rc;
    union semun semaphoreUnion;
    semaphoreUnion.val = 0; /* Unused, but keep compiler quiet. */
    if (semaphore != NULL) {
        rc = semctl(semaphore->semaphoreId, 0, IPC_RMID, semaphoreUnion);
        if (rc != 0) {
            Abort();
        }
        semaphore->semaphoreId = -1;
        semaphore->semaphoreNum = 0;
        semaphore->semaphoreAttribute.threadContext = NULL;
        semaphore->semaphoreAttribute.enableInterruptsCallBack = NULL;
        semaphore->semaphoreAttribute.processInterruptsCallBack = NULL;
    }
    return;
}

/* Try semaphore lock. */
bool SemaphoreTryLock(Semaphore *semaphore)
{
    int errorCode;
    struct sembuf semaphoreOps;

    semaphoreOps.sem_op = -1;          /* Decrement. */
    semaphoreOps.sem_flg = IPC_NOWAIT; /* Don't block. */
    semaphoreOps.sem_num = semaphore->semaphoreNum;
    /*
     * If errorCode is -1 and errno == EINTR then it means we returned
     * from the operation prematurely because we were sent a signal.  So we
     * try and lock the semaphore again.
     */
    do {
        errorCode = semop(semaphore->semaphoreId, &semaphoreOps, 1);
    } while (errorCode < 0 && errno == EINTR);

    if (errorCode < 0) {
        /* Expect EAGAIN or EWOULDBLOCK (platform-dependent) */
#ifdef EAGAIN
        if (errno == EAGAIN) {
            return false; /* Failed to lock it. */
        }

#endif
#if defined(EWOULDBLOCK) && (!defined(EAGAIN) || (EWOULDBLOCK != EAGAIN))
        if (errno == EWOULDBLOCK) {
            return false; /* Failed to lock it. */
        }
#endif
        /* Otherwise we got trouble. */
        Abort();
    }
    return true;
}

/* Reset a previously-initialized Semaphore to count 0. */
void SemaphoreReset(Semaphore *semaphore)
{
    int rc;
    union semun semaphoreUnion;
    semaphoreUnion.val = 0;
    rc = semctl(semaphore->semaphoreId, semaphore->semaphoreNum, SETVAL, semaphoreUnion);
    if (rc < 0) {
        /* Otherwise we got trouble. */
        Abort();
    }
}

/* Semaphore lock timed wait. */
ErrorCode SemaphoreLockTimedwait(Semaphore *semaphore, bool interruptOK, int milliseconds)
{
    int rc;
    struct timespec timeout;
    struct sembuf semaphoreOps;

    semaphoreOps.sem_op = -1; /* Decrement. */
    semaphoreOps.sem_flg = 0; /* Will block. */
    semaphoreOps.sem_num = semaphore->semaphoreNum;
    /*
     * We handle the interrupt case where semop()
     * returns errno == EINTR after a signal, and the case where it just keeps waiting.
     */
    if (milliseconds > 0) {
        timeout.tv_sec = (milliseconds / SECONDS_TO_MILLISECONDS);
        timeout.tv_nsec = (milliseconds - timeout.tv_sec * SECONDS_TO_MILLISECONDS) * MILLISECONDS_TO_NANOSECONDS;
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
            rc = semop(semaphore->semaphoreId, &semaphoreOps, 1);
        } else {
            rc = semtimedop(semaphore->semaphoreId, &semaphoreOps, 1, &timeout);
        }
        if (semaphore->semaphoreAttribute.enableInterruptsCallBack != NULL) {
            semaphore->semaphoreAttribute.enableInterruptsCallBack(semaphore->semaphoreAttribute.threadContext, false);
        }
    } while (rc < 0 && errno == EINTR);

    if (rc == 0) {
        return ERROR_SYS_OK;
    } else if (rc < 0 && errno == EAGAIN) {
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
    struct sembuf semaphoreOps;

    semaphoreOps.sem_op = 1; /* Increment. */
    semaphoreOps.sem_flg = 0;
    semaphoreOps.sem_num = semaphore->semaphoreNum;
    /*
     *  If rc is -1 and errno == EINTR then it means we returned
     *  from the operation prematurely because we were sent a signal.  So we
     *  try and unlock the semaphore again.
     */
    do {
        rc = semop(semaphore->semaphoreId, &semaphoreOps, 1);
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
    uint numSemaphoreIdSet;
    SemaphoreSet *semaphoreSet = NULL;
    numSemaphoreIdSet = (maxSemaphore + SEMAPHORES_PER_SET - 1) / SEMAPHORES_PER_SET;
    size_t setSize =
        (size_t)(sizeof(SemaphoreSet) + maxSemaphore * sizeof(Semaphore) + numSemaphoreIdSet * sizeof(int));
    semaphoreSet = (SemaphoreSet *)MemPagesAlloc(setSize);
    if (semaphoreSet == NULL) {
        return NULL;
    }
    semaphoreSet->setSize = setSize;
    semaphoreSet->semaphore = (Semaphore *)(semaphoreSet + 1);
    semaphoreSet->semaphoreIdSet = (int *)(semaphoreSet->semaphore + maxSemaphore);
    semaphoreSet->numSemaphoreIdSet = numSemaphoreIdSet;
    uint i;
    int semaphoreKey;
    int semaphoreSetId;
    int semaphoreMagicPerSet = SEMAPHORE_DEFAULT_MAGIC;
    if (semaphoreAttribute != NULL && semaphoreAttribute->semaphoreMagicPerSet > 0) {
        semaphoreMagicPerSet = semaphoreAttribute->semaphoreMagicPerSet;
    }
    for (i = 0; i < numSemaphoreIdSet; i++) {
        semaphoreKey = semaphoreStartKey + (int)(i);
        InternalSemaphoreCreate(semaphoreKey, SEMAPHORES_PER_SET, semaphoreMagicPerSet, &semaphoreSetId);
        semaphoreSet->semaphoreIdSet[i] = semaphoreSetId;
    }
    /*
     * InternalSemaphoreCreate create a semaphore set, each semaphore set contains 16(SEMAPHORES_PER_SET) members.
     * The semaphore returned to the upper app layer contains only one member.
     * Here, assign the semaphore set to each semaphore.
     */
    for (i = 0; i < maxSemaphore; i++) {
        semaphoreSet->semaphore[i].semaphoreId = semaphoreSet->semaphoreIdSet[i / SEMAPHORES_PER_SET];
        semaphoreSet->semaphore[i].semaphoreNum = i % SEMAPHORES_PER_SET;
        InternalSemaphoreInitialize(semaphoreSet->semaphore + i, 1);
        SemaphoreAttributeCopy(&(semaphoreSet->semaphore[i].semaphoreAttribute), semaphoreAttribute);
        SemaphoreAttributeSetSemaphoreMagicPerSet(&(semaphoreSet->semaphore[i].semaphoreAttribute),
                                                  semaphoreMagicPerSet);
        SemaphoreAttributeSetSemaphoresPerSet(&(semaphoreSet->semaphore[i].semaphoreAttribute), 1);
    }
    semaphoreSet->maxSemaphore = maxSemaphore;
    return semaphoreSet;
}

/* Destroy semaphore set related resources. */
void SemaphoreSetDestroy(SemaphoreSet *semaphoreSet)
{
    uint i;
    int rc;
    union semun semaphoreUnion;
    semaphoreUnion.val = 0; /* Unused, but keep compiler quiet. */
    if (semaphoreSet != NULL) {
        for (i = 0; i < semaphoreSet->numSemaphoreIdSet; i++) {
            rc = semctl(semaphoreSet->semaphoreIdSet[i], 0, IPC_RMID, semaphoreUnion);
            if (rc != 0) {
                Abort();
            }
        }
        MemPagesFree((void *)semaphoreSet, semaphoreSet->setSize);
    }
}
#endif
