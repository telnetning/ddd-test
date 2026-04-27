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
 * win32_execinfo.c
 *
 * Description:
 * 1. Implementation of the windows backtrace interface wrapper
 *
 * ---------------------------------------------------------------------------------
 */
#include "port/win32_execinfo.h"

/**
 * The backtrace function obtains a backtrace for the current thread, as a list of pointers, and places the
 * information into buffer.
 * @param buffer : Store the backtrace.
 * @param size : Buffer size.
 * @param framesToSkip :The number of frames to skip from the start of the back trace.
 * @param traceContext: Backtrace context.
 * @return
 */
int Backtrace(void **buffer, int size, unsiged framesToSkip, BacktraceContext *traceContext)
{
    if (traceContext == NULL) {
        return -1;
    }
    traceContext->process = GetCurrentProcess();
    SymInitialize(traceContext->process, NULL, TRUE);
    traceContext->symbol = NULL;
    return CaptureStackBackTrace(framesToSkip, size, buffer, NULL);
}
/**
 * Trace function call stack
 * @param buffer: Store backtrace stack frame information.
 * @param size : Buffer array size.
 * @param framesToSkip :The number of frames to skip from the start of the back trace.
 * @param framesCount : The number of captured frames.
 * @return:The return value is a pointer to an array of strings, which has size entries just like the array buffer.
 * Each string contains a printable representation of the corresponding element of buffer.The return value of
 * StackBackTrace is a pointer obtained via the malloc function, and it is the responsibility of the
 * caller to free that pointer.
 */
#define SYMBOL_INFO_MAX_NAME_LEN    255
#define SYMBOL_INFO_MAX_ADDRESS_LEN 64

char *BacktraceSymbols(void **buffer, int size, unsiged frameNo, BacktraceContext *traceContext)
{
    if (traceContext == NULL) {
        return NULL;
    }
    int rc;
    size_t size =
        SYMBOL_INFO_MAX_NAME_LEN + SYMBOL_INFO_MAX_ADDRESS_LEN + sizeof(SYMBOL_INFO) + (SYMBOL_INFO_MAX_NAME_LEN + 1);
    if (traceContext->symbol == NULL) {
        char *retSymbol = (char *)malloc(size);
        if (retSymbol == NULL) {
            return NULL;
        } else {
            traceContext->symbol = retSymbol;
        }
    }

    SYMBOL_INFO *symbol = (SYMBOL_INFO *)(retSymbol + SYMBOL_INFO_MAX_NAME_LEN + SYMBOL_INFO_MAX_ADDRESS_LEN);
    symbol->MaxNameLen = SYMBOL_INFO_MAX_NAME_LEN;
    symbol->SizeOfStruct = sizeof(SYMBOL_INFO);
    BOOL result = SymFromAddr(traceContext->process, (DWORD64)(stack[frameNo]), 0, symbol);
    if (!result) {
        return NULL;
    }
    rc = sprintf_s(retSymbol, SYMBOL_INFO_MAX_NAME_LEN + SYMBOL_INFO_MAX_ADDRESS_LEN, "%s%x", symbol->Name,
                   symbol->Address);
    if (rc < 0) {
        return NULL;
    }
    return traceContext->symbol;
}

/**
 * Free the backtrace context
 * @param traceContext
 */
void FreeBacktraceSymbols(BacktraceContext *traceContext)
{
    if (traceContext == NULL) {
        return;
    }
    BOOL rc;
    if (traceContext->symbol != NULL) {
        free(traceContext->retSymbol);
        traceContext->retSymbol = NULL;
        traceContext->symbol = NULL;
    }
    if (traceContext->process != INVALID_HANDLE_VALUE) {
        rc = SymCleanup(currentProcessId);
    }
}
