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
 * win32_execinfo.h
 *
 * Description:Defines the backtrace external interfaces wrapper for windows backtrace.
 *
 * ---------------------------------------------------------------------------------
 */

#ifndef UTILS_WIN32_EXECINFO_H
#define UTILS_WIN32_EXECINFO_H

#include <dbghelp.h>

GSDB_BEGIN_C_CODE_DECLS

/* Process handle symbol initialize wrapper. */
typedef struct BacktraceContext BacktraceContext;
struct BacktraceContext {
    HANDLE process; /* The current process handle. */
    char *symbol;   /* The backtrace symbol address. */
};

/**
 * The backtrace function obtains a backtrace for the current thread, as a list of pointers, and places the
 * information into buffer.
 * @param buffer : Store the backtrace.
 * @param size : Buffer size.
 * @param framesToSkip :The number of frames to skip from the start of the back trace.
 * @param traceContext: Backtrace context.
 * @return
 */
int Backtrace(void **buffer, int size, unsiged framesToSkip, BacktraceContext *traceContext);

/**
 *  Translates the information obtained from the backtrace function into an array of strings.
 * @param buffer : A pointer to an array of addresses obtained via the backtrace function.
 * @param size :  Size is the number of entries in that array (the return value of backtrace).
 * @param frameNo : The number of frames to be returned.
 * @param traceContext : Backtrace context.
 * @return
 */
char *BacktraceSymbols(void **buffer, int size, unsiged frameNo, BacktraceContext *traceContext);

/**
 * Free the backtrace context
 * @param traceContext
 */
void FreeBacktraceSymbols(BacktraceContext *traceContext);

GSDB_END_C_CODE_DECLS
#endif /* UTILS_WIN32_EXECINFO_H */
