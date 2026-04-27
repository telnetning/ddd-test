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

#ifndef UTILS_COMMON_POSIX_ATOMIC_C11STANDARD_DEF_H
#define UTILS_COMMON_POSIX_ATOMIC_C11STANDARD_DEF_H

/*
 * Defined atomic type.
 */
typedef _Atomic(int8_t) Atomic8;
typedef _Atomic(uint8_t) AtomicU8;
typedef _Atomic(int16_t) Atomic16;
typedef _Atomic(uint16_t) AtomicU16;
typedef _Atomic(int32_t) Atomic32;
typedef _Atomic(uint32_t) AtomicU32;
typedef _Atomic(int64_t) Atomic64;
typedef _Atomic(uint64_t) AtomicU64;
typedef _Atomic(ssize_t) AtomicSsize;
typedef _Atomic(size_t) AtomicSize;
typedef _Atomic(void *) AtomicPointer;
typedef _Atomic(bool) AtomicBool;

#endif // UTILS_COMMON_POSIX_ATOMIC_C11STANDARD_DEF_H
