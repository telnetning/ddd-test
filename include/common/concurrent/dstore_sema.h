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
 * Description:
 * Platform-independent API for semaphores.
 */
#ifndef DSTORE_PG_SEMA_H
#define DSTORE_PG_SEMA_H

/*
 * PGSemaphoreData and pointer type PGSemaphore are the data structure
 * representing an individual semaphore.  The content of PGSemaphoreData
 * varies from implementation to implementation and must never be touched by
 * platform-independent code.  PGSemaphoreData structures are always allocated
 * in shared memory (to support implementations where the data changes during
 * lock/unlock).
 */
// TODO: 放在makefile中
#define DSTORE_USE_SYSV_SEMAPHORES

#ifdef USE_NAMED_POSIX_SEMAPHORES

#include <semaphore.h>
namespace DSTORE {
typedef sem_t* PGSemaphoreData;
}
#endif

#ifdef USE_UNNAMED_POSIX_SEMAPHORES

#include <semaphore.h>
namespace DSTORE {
typedef sem_t PGSemaphoreData;
}
#endif

#ifdef DSTORE_USE_SYSV_SEMAPHORES
namespace DSTORE {
struct PGSemaphoreData {
    int semId;  /* semaphore set identifier */
    int semNum; /* semaphore number within set */
};
}
#endif

#ifdef USE_WIN32_SEMAPHORES
namespace DSTORE {
typedef HANDLE PGSemaphoreData;
}
#endif

#include "common/dstore_datatype.h"

namespace DSTORE {
using PGSemaphore = PGSemaphoreData*;

using PGSemaphoreMemFreeCallback = void(*)(void *ptr);

/* concurrent control for locale */
extern PGSemaphoreData g_localeSem;

/* Module initialization (called during postmaster start or shmem reinit) */
extern void PGReserveSemaphores(void *ptr, int port, PGSemaphoreMemFreeCallback freeFunc);

/* Initialize a PGSemaphore structure to represent a sema with count 1 */
extern void PGSemaphoreCreate(PGSemaphore sem);

/* PGSemaphore structure has been allocated,
 * and Re-Initialize a PGSemaphore structure to represent a sema with count 1
 */
void PGSemaphoreReInit(PGSemaphore sem);

/* Reset a previously-initialized PGSemaphore to have count 0 */
extern void PGSemaphoreReset(PGSemaphore sem);

/* Lock a semaphore (decrement count), blocking if count would be < 0 */
extern void PGSemaphoreLock(PGSemaphore sem, bool interruptOK, const struct timespec *timeout = nullptr);

/* Unlock a semaphore (increment count) */
extern void PGSemaphoreUnlock(PGSemaphore sema);

/* Lock a semaphore only if able to do so without blocking */
extern bool PGSemaphoreTryLock(PGSemaphore sema);

extern void CancelSemphoreRelease(void);

extern Size PGSemaphoresMemSize(int maxSemas);

extern void PGReleaseSemaphores();
}
#endif /* STORAGE_PG_SEMA_H */
