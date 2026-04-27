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
 * -------------------------------------------------------------------------
 *
 * dstore_instr_time.h
 *	  portable high-precision interval timing
 *
 * This file provides an abstraction layer to hide portability issues in
 * interval timing.  On Unix we use gettimeofday(), but on Windows that
 * gives a low-precision result so we must use QueryPerformanceCounter()
 * instead.  These macros also give some breathing room to use other
 * high-precision-timing APIs on yet other platforms.
 *
 * The basic data type is instr_time, which all callers should treat as an
 * opaque typedef.	instr_time can store either an absolute time (of
 * unspecified reference time) or an interval.	The operations provided
 * for it are:
 *
 * INSTR_TIME_IS_ZERO(t)			is t equal to zero?
 *
 * INSTR_TIME_SET_ZERO(t)			set t to zero (memset is acceptable too)
 *
 * INSTR_TIME_SET_CURRENT(t)		set t to current time
 *
 * INSTR_TIME_ADD(x, y)				x += y
 *
 * INSTR_TIME_SUBTRACT(x, y)		x -= y
 *
 * INSTR_TIME_ACCUM_DIFF(x, y, z)	x += (y - z)
 *
 * INSTR_TIME_GET_DOUBLE(t)			convert t to double (in seconds)
 *
 * INSTR_TIME_GET_MILLISEC(t)		convert t to double (in milliseconds)
 *
 * INSTR_TIME_GET_MICROSEC(t)		convert t to uint64 (in microseconds)
 *
 * Note that INSTR_TIME_SUBTRACT and INSTR_TIME_ACCUM_DIFF convert
 * absolute times to intervals.  The INSTR_TIME_GET_xxx operations are
 * only useful on intervals.
 *
 * When summing multiple measurements, it's recommended to leave the
 * running sum in instr_time form (ie, use INSTR_TIME_ADD or
 * INSTR_TIME_ACCUM_DIFF) and convert to a result format only at the end.
 *
 * Beware of multiple evaluations of the macro arguments.
 *
 *
 * Copyright (c) 2001-2012, PostgreSQL Global Development Group
 *
 * src/include/portability/instr_time.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef INSTR_TIME_H
#define INSTR_TIME_H

#ifndef WIN32

#include <sys/time.h>

typedef struct timeval instr_time;
#define INSTR_TIME_SET_CURRENT(t) gettimeofday(&(t), NULL)
#define INSTR_TIME_SUBTRACT(x, y)   \
    do {                            \
        (x).tv_sec -= (y).tv_sec;   \
        (x).tv_usec -= (y).tv_usec; \
        /* Normalize */             \
        while ((x).tv_usec < 0) {   \
            (x).tv_usec += 1000000; \
            (x).tv_sec--;           \
        }                           \
    } while (0)
#define INSTR_TIME_GET_MICROSEC(t) (((uint64)(t).tv_sec * (uint64)1000000) + (uint64)(t).tv_usec)
#else /* WIN32 */

typedef LARGE_INTEGER instr_time;
#define INSTR_TIME_SUBTRACT(x, y) ((x).QuadPart -= (y).QuadPart)
#define INSTR_TIME_SET_CURRENT(t) QueryPerformanceCounter(&(t))
#define INSTR_TIME_GET_MICROSEC(t) (((uint64)(t).tv_sec * (uint64)1000000) + (uint64)(t).tv_usec)
#endif

#endif /* INSTR_TIME_H */
