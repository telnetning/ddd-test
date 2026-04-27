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
 * port_semaphore.h
 *
 * Description:Defines the semaphore for the platform portable interface.
 *
 * ---------------------------------------------------------------------------------
 */
#ifndef UTILS_PORT_SEMAPHORE_H
#define UTILS_PORT_SEMAPHORE_H

#include "defines/common.h"
#include "defines/err_code.h"

GSDB_BEGIN_C_CODE_DECLS

/* Semaphore attribute wrapper. */
typedef struct SemaphoreAttribute SemaphoreAttribute;
struct SemaphoreAttribute {
    void *threadContext; /* Signal interrupt processing thread context. */
    /* Whether to allow signal interrupt processing. */
    void (*enableInterruptsCallBack)(void *threadContext, bool interruptOK);
    void (*processInterruptsCallBack)(void *threadContext); /* Signal interrupt processing. */
    unsigned short semaphoresPerSet; /* Semaphores in each semaphore set,only useful for system v semaphore. */
    int semaphoreMagicPerSet;        /* Semaphores magic in each semaphore set,only useful for system v semaphore. */
};

/*
 * Initialize semaphore attribute.
 */
static inline void SemaphoreAttributeInit(SemaphoreAttribute *semaphoreAttribute)
{
    semaphoreAttribute->threadContext = NULL;
    semaphoreAttribute->enableInterruptsCallBack = NULL;
    semaphoreAttribute->processInterruptsCallBack = NULL;
    semaphoreAttribute->semaphoresPerSet = 0;
    semaphoreAttribute->semaphoreMagicPerSet = 0;
}
/*
 * Set semaphore interrupts attribute.
 */
static inline void SemaphoreAttributeSetInterrupts(SemaphoreAttribute *semaphoreAttribute,
                                                   void (*enableInterrupts)(void *threadContext, bool interruptOK),
                                                   void (*processInterrupts)(void *threadContext), void *threadContext)
{
    semaphoreAttribute->threadContext = threadContext;
    semaphoreAttribute->enableInterruptsCallBack = enableInterrupts;
    semaphoreAttribute->processInterruptsCallBack = processInterrupts;
}

/*
 * Set semaphore magic per set attribute.
 * Magic is used to detect whether the conflicting semaphore is created by the same service process.
 */
static inline void SemaphoreAttributeSetSemaphoreMagicPerSet(SemaphoreAttribute *semaphoreAttribute,
                                                             int semaphoreMagicPerSet)
{
    semaphoreAttribute->semaphoreMagicPerSet = semaphoreMagicPerSet;
}
/* When semaphore timeout wait, a negative value indicates infinite wait. */
#define SEMAPHORE_INFINITE_TIMED_WAIT (-1)

/* Semaphore wrapper. */
typedef struct Semaphore Semaphore;

/* Semaphore set wrapper. */
typedef struct SemaphoreSet SemaphoreSet;
/*
 * Create Semaphore.
 * If emergency signal processing is not required during semaphore waiting,
 * and use the default number of semaphores and default magic in each semaphore set,
 * the semaphoreAttribute can be set to NULL.
 * semaphoreKey is passed for possible use as a key. For system v semaphore, we use
 * it to generate the starting semaphore key.
 * */
void SemaphoreCreate(int semaphoreKey, SemaphoreAttribute *semaphoreAttribute, Semaphore *semaphore);

/* Destroy semaphore related resources. */
void SemaphoreDestroy(Semaphore *semaphore);

/* Try semaphore lock. */
bool SemaphoreTryLock(Semaphore *semaphore);

/* Reset a previously-initialized Semaphore to count 0. */
void SemaphoreReset(Semaphore *semaphore);

/* Semaphore lock timed wait. */
ErrorCode SemaphoreLockTimedwait(Semaphore *semaphore, bool interruptOK, int milliseconds);

/* Semaphore lock. */
void SemaphoreLock(Semaphore *semaphore, bool interruptOK);

/* Semaphore unlock. */
void SemaphoreUnlock(Semaphore *semaphore);

/*
 * Create semaphore set.
 * If emergency signal processing is not required during semaphore waiting,
 * the semaphoreAttribute can be set to NULL.
 * */
SemaphoreSet *SemaphoreSetCreate(int semaphoreStartKey, uint maxSemaphore, SemaphoreAttribute *semaphoreAttribute);

/* Destroy semaphore set related resources. */
void SemaphoreSetDestroy(SemaphoreSet *semaphoreSet);

/*
 * Get an semaphore element from semaphore set.
 */
Semaphore *GetSemaphoreFromSet(SemaphoreSet *semaphoreSet, uint index);

/*
 * Get the number of semaphores set.
 */
uint GetSemaphoreCountFromSet(SemaphoreSet *semaphoreSet);

GSDB_END_C_CODE_DECLS
#endif /* UTILS_PORT_SEMAPHORE_H */
