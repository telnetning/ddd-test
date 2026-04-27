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
 * data_types.h
 *
 * Description:
 * This file defines some common data types, such as C99 data type alias and bool data
 * type, and provides some simple data operation macros, such as MAX(), MIN() etc.
 *
 * ---------------------------------------------------------------------------------
 */
#ifndef UTILS_DATA_TYPES_H
#define UTILS_DATA_TYPES_H

#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <stdbool.h>

#include "defines/common.h"

GSDB_BEGIN_C_CODE_DECLS

typedef int8_t int8;
typedef int16_t int16;
typedef int32_t int32;
typedef int64_t int64;
typedef uint8_t uint8;
typedef uint16_t uint16;
typedef uint32_t uint32;
typedef uint64_t uint64;
typedef size_t Size;
typedef char *Pointer;

#define FLEXIBLE_ARRAY_MEMBER 1
#define GS_NOINLINE           __attribute__((noinline))

typedef uint32_t NodeId;
typedef uint32_t Oid;
typedef uint32_t BlockNumber;
typedef uint16_t FileId;

typedef uintptr_t Datum;

#define SIZEOF_VOID_P 8 /* hard code */
#define SIZEOF_DATUM  SIZEOF_VOID_P

#define FOUR_BYTE_SHIFT 32

#define GET_1_BYTE(datum)  (((Datum)(datum)) & 0x000000ff)
#define GET_2_BYTES(datum) (((Datum)(datum)) & 0x0000ffff)
#define GET_4_BYTES(datum) (((Datum)(datum)) & 0xffffffff)
#if SIZEOF_DATUM == 8
#define GET_8_BYTES(datum) ((Datum)(datum))
#endif
#define SET_1_BYTE(value)  (((Datum)(value)) & 0x000000ff)
#define SET_2_BYTES(value) (((Datum)(value)) & 0x0000ffff)
#define SET_4_BYTES(value) (((Datum)(value)) & 0xffffffff)
#if SIZEOF_DATUM == 8
#define SET_8_BYTES(value) ((Datum)(value))
#endif

#ifndef likely
#define likely(x) (__builtin_expect((x) ? 1 : 0, 1) != 0)
#endif
#ifndef unlikely
#define unlikely(x) (__builtin_expect((x) ? 1 : 0, 0) != 0)
#endif

/*
 * DatumGetUInt32
 *      Returns 32-bit unsigned integer value of a datum.
 */
#ifndef DatumGetUInt32
#define DatumGetUInt32(X) ((uint32_t)GET_4_BYTES(X))
#endif
/*
 * UInt32GetDatum
 *      Returns datum representation for a 32-bit unsigned integer.
 */

#define UInt32GetDatum(X) ((Datum)SET_4_BYTES(X))

#ifndef TYPEALIGN
#define MAXIMUM_ALIGNOF                 8
#define TYPEALIGN(ALIGNVAL, LEN)        (((uintptr_t)(LEN) + ((ALIGNVAL) - (1))) & ~((uintptr_t)((ALIGNVAL) - (1))))
#define IS_TYPE_ALIGINED(ALIGNVAL, LEN) ((((uintptr_t)(LEN)) & ((uintptr_t)((ALIGNVAL) - (1)))) == 0)
#define MAXALIGN(LEN)                   TYPEALIGN(MAXIMUM_ALIGNOF, (LEN))
#endif

/*
 * Max
 *      Return the maximum of two numbers.
 */
#define Max(x, y) ((x) > (y) ? (x) : (y))

/*
 * Min
 *      Return the minimum of two numbers.
 */
#define Min(x, y) ((x) < (y) ? (x) : (y))

/*
 * Abs
 *      Return the absolute value of the argument.
 */
#define Abs(x) ((x) >= 0 ? (x) : -(x))

/* PClint */
#ifdef PC_LINT
#define THR_LOCAL
#endif

#ifndef THR_LOCAL
#ifndef WIN32
#define THR_LOCAL __thread
#else
#define THR_LOCAL __declspec(thread)
#endif
#endif

GSDB_END_C_CODE_DECLS

#endif /* UTILS_DATA_TYPES_H */
