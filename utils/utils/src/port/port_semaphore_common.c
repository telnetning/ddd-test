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
 * Description: semaphore API common implement for all the platfrom
 */

#include "port/platform_port.h"

/* Semaphore lock. */
void SemaphoreLock(Semaphore *semaphore, bool interruptOK)
{
    ErrorCode errorCode = SemaphoreLockTimedwait(semaphore, interruptOK, SEMAPHORE_INFINITE_TIMED_WAIT);
    if (errorCode != ERROR_SYS_OK) {
        Abort();
    }
}

/*
 * Get an semaphore element from semaphore set.
 */
Semaphore *GetSemaphoreFromSet(SemaphoreSet *semaphoreSet, uint index)
{
    if ((semaphoreSet != NULL) && (index < semaphoreSet->maxSemaphore)) {
        return semaphoreSet->semaphore + index;
    } else {
        return NULL;
    }
}
/*
 * Get the number of semaphores set.
 */
uint GetSemaphoreCountFromSet(SemaphoreSet *semaphoreSet)
{
    if (semaphoreSet != NULL) {
        return semaphoreSet->maxSemaphore;
    } else {
        return 0;
    }
}
