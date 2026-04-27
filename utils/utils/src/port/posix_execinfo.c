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
 * posix_execinfo.c
 *
 * Description:
 * 1. Implementation of the linux backtrace interface wrapper.
 *
 * ---------------------------------------------------------------------------------
 */

#include <execinfo.h>
#include "port/posix_execinfo.h"

/**
 * The backtrace function obtains a backtrace for the current thread, as a list of pointers, and places the
 * information into buffer.
 * @param buffer : Store the backtrace.
 * @param size : Buffer size.
 * @param framesToSkip :The number of frames to skip from the start of the back trace.
 * @param traceContext: Backtrace context.
 * @return
 */
UTILS_EXPORT int Backtrace(void **buffer, int size, unsigned framesToSkip, BacktraceContext *traceContext)
{
    if (traceContext == NULL) {
        return -1;
    }
    traceContext->frames = NULL;
    traceContext->framesToSkip = framesToSkip;
    return (backtrace(buffer, size) - (int)framesToSkip);
}

/**
 *  Translates the information obtained from the backtrace function into an array of strings.
 * @param buffer : A pointer to an array of addresses obtained via the backtrace function.
 * @param size :  Size is the number of entries in that array (the return value of backtrace).
 * @param frameNo : The number of frames to be returned.
 * @param traceContext : Backtrace context.
 * @return
 */
UTILS_EXPORT char *BacktraceSymbols(void **buffer, int size, unsigned frameNo, BacktraceContext *traceContext)
{
    if (traceContext == NULL) {
        return NULL;
    }
    if (traceContext->frames == NULL) {
        traceContext->frames = backtrace_symbols(buffer, (int)traceContext->framesToSkip + size);
    }
    return traceContext->frames[traceContext->framesToSkip + frameNo];
}

/**
 * Free the backtrace context
 * @param traceContext
 */
UTILS_EXPORT void FreeBacktraceSymbols(BacktraceContext *traceContext)
{
    if (traceContext == NULL) {
        return;
    }
    if (traceContext->frames != NULL) {
        free(traceContext->frames);
        traceContext->frames = NULL;
    }
}