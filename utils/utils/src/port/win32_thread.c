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
 * win32_thread.c
 *
 * Description:
 * 1. Implementation of the windows thread interface wrapper.
 *
 * ---------------------------------------------------------------------------------
 */

#include "port/win32_thread.h"

/**
 * Get the current process id.
 * @return pid : Pid
 */
Pid GetCurrentPid(Pid *pid)
{
    Pid pid;
    pid.pid = GetCurrentProcessId();
    return pid;
}

/**
 * Whether the two processes id are equal.
 * @param pid1 : Pid
 * @param pid2 : Pid
 * @return
 */
bool PidIsEqual(const Pid *pid1, const Pid *pid2)
{
    return (pid1->pid == pid2->pid) ? true : false;
}

/**
 * Converts the process PID value to a character string.
 * @param pid : Pid.
 * @param pidStr : Output parameter, PID conversion string.
 * @param size : Pid string size.
 */
void Pid2String(const Pid *pid, char *pidStr, size_t size)
{
    int rc = sprintf_s(pidStr, size, "%lu", pid->pid);
    if (rc < 0) {
        unsigned long i;
        for (i = 0; i < size - 1; i++) {
            pidStr[i] = '?';
        }
        pidStr[size - 1] = '\0';
    }
    return;
}

/**
 * Get the current thread id.
 * @return tid : Tid
 */
Tid GetCurrentTid(void)
{
    Tid tid;
    tid.tid = GetCurrentThreadId();
    return tid;
}

/**
 * Whether the two thread id are equal.
 * @param tid1 : Tid
 * @param tid2 : Tid
 * @return
 */
bool TidIsEqual(Tid *tid1, Tid *tid2)
{
    return (tid1->tid == tid2->tid) ? true : false;
}

/**
 * Converts the thread id value to a character string.
 * @param tid : Tid.
 * @param tidStr : Output parameter, TID conversion string.
 * @param size : Tid string size.
 */
void Tid2String(const Tid *tid, char *tidStr, size_t size)
{
    int rc = sprintf_s(tidStr, size, "%lu", tid->tid);
    if (rc < 0) {
        unsigned long i;
        for (i = 0; i < size - 1; i++) {
            tidStr[i] = '?';
        }
        tidStr[size - 1] = '\0';
    }
    return;
}

uint64_t Tid2Integer(const Tid *tid)
{
    return tid->tid;
}

/* Thread proxy function of the windows platform. */
unsigned int __stdcall ThreadFunctionProxy(void *arg)
{
    Tid *tid = (Tid *)arg;
    tid->result = tid->startRoutine(tid->arg);
    return 0;
}
/**
 * Create thread.There are two interfaces for creating threads in Windows. One is CreateThread. The corresponding
 * callback function has two formats. The standard format is as follows:
 * DWORD WINAPI ThreadFunctionName (LPVOID lpParam)；CreateThread(NULL, 0, ThreadFunctionName, 0, 0, 0);
 * The other format is as follows. The lpStartAddress needs to be converted into LPTHREAD_START_ROUTINE.
 * void ThreadFunctionName(void); CreateThread(NULL, 0, (LPTHREAD_START_ROUTINE)ThreadFunctionName, 0, 0, 0);
 * The other interface is _beginthreadex. The corresponding callback function format is as follows:
 * unsigned int __stdcall ThreadFunctionName(void *); _beginthreadex(NULL, 0, ThreadFunctionName, NULL, 0, NULL);
 * The return value of ThreadFunctionName indicates the thread exit error code. You can obtain the error code
 * by calling GetExitCodeThread.
 * The difference between CreateThread and _beginthreadex is that when using global variables from the standard
 * C runtime,each thread created by _beginthreadex will have its own dedicated memory area for all the required
 * functions in the standard C runtime.the threads created by CreateThread share these global variables.
 * CreateThread is a windows API, _beginthread(ex) is a function of the Visual C++ runtime, and quit using _endthreadex.
 * Here use the _beginthreadex.
 * @param tid : Tid.
 * @param startRoutine: ThreadStartRoutine.
 * @param arg: startRoutine input parameter.
 * @return
 */
#define THREAD_STACK_SIZE_BYTES (10 * 1024 * 1024)

ErrorCode ThreadCreate(Tid *tid, ThreadStartRoutine startRoutine, void *arg)
{
    ErrorCode errCode = ERROR_SYS_OK;
    tid->startRoutine = startRoutine;
    tid->arg = arg;
    /* Set initflag to 0 to run immediately, or to CREATE_SUSPENDED to create the thread in a suspended state;
     * use ResumeThread to execute the thread. Here set initflag to 0. */
    tid->handle =
        (HANDLE)_beginthreadex(NULL, THREAD_STACK_SIZE_BYTES, ThreadFunctionProxy, (void *)tid, 0, &(tid->tid));
    if (tid->handle == 0) {
        WindowsErrorCode2PortErrorCode(errno, &errCode);
        return errCode;
    }
    return errCode;
}

/**
 * Waiting for the thread execution to end.
 * @param tid: Tid.
 * @param valuePtr: ThreadStartRoutine return value.
 * @return
 */
ErrorCode ThreadJoin(Tid tid, void **valuePtr)
{
    DWORD rc;
    rc = WaitForSingleObject(tid.handle, INFINITE);
    if (rc != WAIT_OBJECT_0) {
        return ERROR_UTILS_PORT_UNKNOWN;
    }
    *valuePtr = tid->result;
    return ERROR_SYS_OK;
}

/**
 * Indicate to the implementation that storage for the thread tid can be reclaimed when that thread terminates.
 * @param tid: GaussTid.
 * @return
 */
ErrorCode ThreadDetach(Tid tid)
{
    BOOL rc;
    rc = CloseHandle(tid.handle);
    if (!rc) {
        return ERROR_UTILS_PORT_UNKNOWN;
    }
    return ERROR_SYS_OK;
}

/**
 * The thread exit.
 * @param valuePtr
 */
void ThreadExit(void *valuePtr)
{
    tid->result = valuePtr;
    _endthreadex(0);
}

void ThreadYield(void)
{
    SleepEx(0, FALSE);
}

/**
 * Set the thread priority.
 * @param tid : Tid.
 * @param pri : ThreadPriority.
 */
#define NUMERIC_CONSTANT_TWO 2
ErrorCode ThreadSetPriority(Tid tid, ThreadPriority pri)
{
    BOOL rc;
    int priority;
    switch (pri) {
        case THR_PRI_LOW:
            priority = THREAD_PRIORITY_IDLE;
            break;
        case THR_PRI_MIDDLE:
            priority = (THREAD_PRIORITY_IDLE + THREAD_PRIORITY_HIGHEST) / NUMERIC_CONSTANT_TWO;
            break;
        case THR_PRI_HIGH:
            priority = THREAD_PRIORITY_HIGHEST;
            break;
        default:
            priority = THREAD_PRIORITY_IDLE;
            break;
    }
    rc = SetThreadPriority(tid.handle, priority);
    if (!rc) {
        return ERROR_UTILS_PORT_UNKNOWN;
    }
    return ERROR_SYS_OK;
}

/**
 * Get the thread priority.
 * @param tid : Tid.
 * @param pri : ThreadPriority.
 */
ErrorCode ThreadGetPriority(Tid tid, ThreadPriority *pri)
{
    int priority = GetThreadPriority(tid.handle);
    if (priority == THREAD_PRIORITY_IDLE) {
        *pri = THR_PRI_LOW;
    } else if (priority == THREAD_PRIORITY_HIGHEST) {
        *pri = THR_PRI_HIGH;
    } else {
        *pri = THR_PRI_MIDDLE;
    }
    return ERROR_SYS_OK;
}

/**
 * Set thread local variables.THR_LOCAL is preferentially used to define thread local variables.
 * The efficiency of THR_LOCAL is higher than that of ThreadSetSpecific.
 * @param key : ThreadKey
 * @param value : Thread local variables.
 * @return
 */
ErrorCode ThreadSetSpecific(ThreadKey key, const void *value)
{
    BOOL rc;
    rc = TlsSetValue(key.key, value);
    if (!rc) {
        return ERROR_UTILS_PORT_UNKNOWN;
    }
    return ERROR_SYS_OK;
}

/**
 * Get thread local variables.
 * @param key : ThreadKey
 * @return :  Thread local variables.If the function fails, the return value is zero.
 */
void *ThreadGetSpecific(ThreadKey key)
{
    return TlsGetValue(key.key);
}

/**
 * Create the key of a thread local variable.
 * @param key : ThreadKey, output parameter.
 * @return
 */
ErrorCode ThreadKeyCreate(ThreadKey *key)
{
    key->key = TlsAlloc();
    if (key->key != TLS_OUT_OF_INDEXES) {
        return ERROR_SYS_OK;
    }
    return ERROR_UTILS_PORT_UNKNOWN;
}

/**
 * Delete the key of a thread local variable.
 * @param key : ThreadKey.
 * @return
 */
ErrorCode ThreadKeyDelete(ThreadKey key)
{
    BOOL rc;
    rc = TlsFree(key.key);
    if (!rc) {
        return ERROR_UTILS_PORT_UNKNOWN;
    }
    return ERROR_SYS_OK;
}

/**
 * Set a unique name for a thread. The handle must have THREAD_SET_LIMITED_INFORMATION access.
 * @param name : The thread name is a meaningful C language string, whose length is restricted to 16 characters,
 * including the terminating null byte ('\0').
 * @return
 */
ErrorCode ThreadSetName(const char *name)
{
    HRESULT result;
    DWORD tid = GetCurrentThreadId();
    result = SetThreadDescription(tid, PCWSTR(name));
    if (SUCCEEDED(result)) {
        return ERROR_SYS_OK;
    } else {
        return ERROR_UTILS_PORT_UNKNOWN;
    }
}

/**
 * Retrieve the name of the thread.The handle must have THREAD_QUERY_LIMITED_INFORMATION access.
 * @param name : Used to return the thread name.
 * @param len :  Specifies the number of bytes available in name. The buffer specified by name should be at least 16
 * characters in length. The returned thread name in the output buffer will be null terminated.
 * @return
 */
ErrorCode ThreadGetName(char *name, size_t len)
{
    int rc;
    HRESULT result;
    PWSTR pszThreadDescription;
    DWORD tid = GetCurrentThreadId();
    result = GetThreadDescription(tid, &pszThreadDescription);
    if (SUCCEEDED(result)) {
        rc = sprintf_s(name, len, "%s", pszThreadDescription);
        LocalFree(pszThreadDescription);
        if (rc < 0) {
            int i;
            for (i = 0; i < len - 1; i++) {
                name[i] = '?';
            }
            name[len - 1] = '\0';
            return ERROR_UTILS_PORT_UNKNOWN;
        }
        return ERROR_SYS_OK;
    } else {
        return ERROR_UTILS_PORT_UNKNOWN;
    }
}
