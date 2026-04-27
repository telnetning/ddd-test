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
#ifndef UTILS_POSIX_SEMAPHORE_H
#define UTILS_POSIX_SEMAPHORE_H
#include <semaphore.h>
#include "defines/common.h"
#include "port/port_semaphore.h"

GSDB_BEGIN_C_CODE_DECLS

struct Semaphore {
    sem_t semaphore;                       /* Semaphore handle. */
    SemaphoreAttribute semaphoreAttribute; /* Semaphore attribute. */
};

struct SemaphoreSet {
    Semaphore *semaphore; /* Semaphore. */
    uint maxSemaphore;    /* Maximum number of semaphores. */
    size_t setSize;       /* Semaphores set memory size. */
};

GSDB_END_C_CODE_DECLS
#endif /* UTILS_POSIX_SEMAPHORE_H */
