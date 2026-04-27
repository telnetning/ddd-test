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
 * common.h
 *
 * Description:
 * This file defines the basic configurations, common macro definitions, and other
 * basic tools required by the project.
 *
 * ---------------------------------------------------------------------------------
 */
#ifndef UTILS_COMMON_H
#define UTILS_COMMON_H

#include <stdlib.h>
#include <stdio.h>

#ifdef GSDB_DEBUG
#include <execinfo.h>
#include <pthread.h>
#include <unistd.h>
#endif

#ifdef __cplusplus
#define GSDB_BEGIN_C_CODE_DECLS extern "C" {
#define GSDB_END_C_CODE_DECLS   }
#else
#define GSDB_BEGIN_C_CODE_DECLS
#define GSDB_END_C_CODE_DECLS
#endif /* __cplusplus */

#define UTILS_EXPORT __attribute__((visibility("default")))

#ifdef WIN32
#define SYMBOL_UNUSED
#else
#define SYMBOL_UNUSED __attribute__((unused))
#endif

#define UTILS_INLINE   inline __attribute__((always_inline))
#define UTILS_NOINLINE __attribute__((noinline))

/** Count the element of a compile-time array */
#ifndef ELEMENT_COUNT
#define ELEMENT_COUNT(arr) ((uint32_t)(sizeof(arr) / sizeof((arr)[0])))
#endif /* ELEMENT_COUNT */

/*
 * If the condition is true, the expanded macro is equal to (void)sizeof(char[1]),no error is reported
 * during compilation.If the condition is false, the expanded macro is equal to (void)sizeof(char[-1]),
 * an error is reported during compilation.
 */
#define COMPILE_ASSERT_CONDITION(condition) ((void)sizeof(char[1 - 2 * !(condition)]))

GSDB_BEGIN_C_CODE_DECLS

/*
 * Assumed cache line size. This doesn't affect correctness, but can be used
 * for low-level optimizations. Currently, this is used to pad some data
 * structures in xlog.c, to ensure that highly-contended fields are on
 * different cache lines. Too small a value can hurt performance due to false
 * sharing, while the only downside of too large a value is a few bytes of
 * wasted memory. The default is 128, which should be large enough for all
 * supported platforms.
 */
#define GS_CACHE_LINE_SIZE 128U

/*
 * Debug
 */
#ifdef GSDB_DEBUG

#define MAX_SYMBOL_NUM 64
static inline void CommonPrintBacktrace(void)
{
    void *btSymbolArray[MAX_SYMBOL_NUM];
    int traceNum = backtrace(btSymbolArray, MAX_SYMBOL_NUM);
    backtrace_symbols_fd(btSymbolArray, traceNum, STDOUT_FILENO);
}

#define TRAP(condition, errorType)                                                                   \
    do {                                                                                             \
        if ((condition)) {                                                                           \
            (void)fprintf(stderr, "[%d][%lu]TRAP: %s(\"%s\", File: \"%s\", Line: %d)\n", getpid(),   \
                          (unsigned long)pthread_self(), #condition, errorType, __FILE__, __LINE__); \
            CommonPrintBacktrace();                                                                  \
            abort();                                                                                 \
        }                                                                                            \
    } while (0)
#ifndef ASSERT
#define ASSERT(condition) TRAP(!(condition), "FailedAssertion")
#endif

#else
#ifndef ASSERT
#define ASSERT(condition)
#endif

#endif /* GSDB_DEBUG */

#define ASSERT_VAR_IS_OF_TYPE_MACRO(varName, typeName) ASSERT(sizeof(varName) == sizeof(typeName))

/**
 * This extension allows gcc to check the format string for consistency with
 * the supplied arguments.
 */
#if defined(__GNUC__)
#define GsAttributePrintf(f, n) __attribute__((format(printf, f, n)))
#else
#define GsAttributePrintf(f, n)
#endif /* defined(__GNUC__) */

GSDB_END_C_CODE_DECLS

#endif /* UTILS_COMMON_H */
