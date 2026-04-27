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
 */

#ifndef UTILS_COMMON_POSIX_ATOMIC_ATOMIC_DEF_H
#define UTILS_COMMON_POSIX_ATOMIC_ATOMIC_DEF_H

#include <stdio.h>
#include <stdint.h>
#include <stdbool.h>
#include <unistd.h>
#include <sys/types.h>

/*
 * Defined atomic type as a structure to avoid variable calculation by bypassing atomic operation functions.
 */
typedef struct Atomic8 Atomic8;
struct Atomic8 {
    int8_t atomicValue;
};

typedef struct AtomicU8 AtomicU8;
struct AtomicU8 {
    uint8_t atomicValue;
};

typedef struct Atomic16 Atomic16;
struct Atomic16 {
    int16_t atomicValue;
};

typedef struct AtomicU16 AtomicU16;
struct AtomicU16 {
    uint16_t atomicValue;
};

typedef struct AtomicInt32 AtomicInt32;
struct AtomicInt32 {
    int32_t atomicValue;
};

typedef struct AtomicU32 AtomicU32;
struct AtomicU32 {
    uint32_t atomicValue;
};

typedef struct AtomicInt64 AtomicInt64;
struct AtomicInt64 {
    int64_t atomicValue;
};

typedef struct AtomicU64 AtomicU64;
struct AtomicU64 {
    uint64_t atomicValue;
};

typedef struct AtomicSsize AtomicSsize;
struct AtomicSsize {
    ssize_t atomicValue;
};

typedef struct AtomicSize AtomicSize;
struct AtomicSize {
    size_t atomicValue;
};

typedef struct AtomicPointer AtomicPointer;
struct AtomicPointer {
    void *atomicValue;
};

typedef struct AtomicBool AtomicBool;
struct AtomicBool {
    bool atomicValue;
};

#endif // UTILS_COMMON_POSIX_ATOMIC_ATOMIC_DEF_H
