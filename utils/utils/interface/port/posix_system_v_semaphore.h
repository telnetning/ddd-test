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
 * posix_system_v_semaphore.h
 *
 * Description:Defines the system v semaphore for the platform portable interface.
 *
 * ---------------------------------------------------------------------------------
 */
#ifndef UTILS_POSIX_SYSTEM_V_SEMAPHORE_H
#define UTILS_POSIX_SYSTEM_V_SEMAPHORE_H

#include "defines/common.h"
#include "port/port_semaphore.h"

GSDB_BEGIN_C_CODE_DECLS

struct Semaphore {
    int semaphoreId;                       /* Semaphore set identifier. */
    unsigned short semaphoreNum;           /* Semaphore number within set. */
    SemaphoreAttribute semaphoreAttribute; /* Semaphore attribute. */
};

struct SemaphoreSet {
    Semaphore *semaphore;   /* Semaphore. */
    uint maxSemaphore;      /* Maximum number of semaphores. */
    int *semaphoreIdSet;    /* Semaphores id set. */
    uint numSemaphoreIdSet; /* The number of semaphores id set. */
    size_t setSize;         /* Semaphores set memory size. */
};

GSDB_END_C_CODE_DECLS
#endif /* UTILS_POSIX_SYSTEM_V_SEMAPHORE_H */
