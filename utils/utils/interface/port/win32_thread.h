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
 * win32_thread.h
 *
 * Description:Defines the thread external interfaces wrapper for windows platform thread.
 *
 * ---------------------------------------------------------------------------------
 */
#ifndef UTILS_WIN32_THREAD_H
#define UTILS_WIN32_THREAD_H

#include <stdlib.h>
#include <windows.h>
#include <stdarg.h>
#include <stdio.h>
#include "port/win32_errcode.h"

GSDB_BEGIN_C_CODE_DECLS

/* ProcessId  wrapper. DWORD is same as unsigned long. */
typedef struct Pid Pid;
struct Pid {
    DWORD pid;
};

/**
 * Get the current process id.
 * @return pid : Pid
 */
Pid GetCurrentPid(void);

void SetPid(Pid *pid, int value);
/**
 * Whether the two processes id are equal.
 * @param pid1 : Pid
 * @param pid2 : Pid
 * @return
 */
bool PidIsEqual(const Pid *pid1, const Pid *pid2);

/**
 * Converts the process PID value to a character string.
 * @param pid : Pid.
 * @param pidStr : Output parameter, PID conversion string.
 * @param size : Pid string size.
 */
void Pid2String(const Pid *pid, char *pidStr, size_t size);

/* ThreadId  wrapper. */
typedef struct Tid Tid;
struct Tid {
    DWORD tid;                       /* The thread identifier. */
    HANDLE handle;                   /* The thread handle. */
    ThreadStartRoutine startRoutine; /* The routine to run in the thread.  */
    void *arg;                       /* The routine input parameter. */
    void *result;                    /* The routine returned result. */
};

/**
 * Get the current thread id.
 * @return tid : Tid
 */
Tid GetCurrentTid(void);

void SetTid(Pid *tid, unsigned long value);
/**
 * Whether the two thread id are equal.
 * @param tid1 : Tid
 * @param tid2 : Tid
 * @return
 */
bool TidIsEqual(Tid *tid1, Tid *tid2);

/**
 * Converts the thread id value to a character string.
 * @param tid : Tid.
 * @param tidStr : Output parameter, TID conversion string.
 * @param size : Tid string size.
 */
void Tid2String(const Tid *tid, char *tidStr, size_t size);

/**
 * Converts the thread id value to uint64_t integer
 * @param tid : Tid
 */
uint64_t Tid2Integer(const Tid *tid);

/* Thread running function */
typedef void *(*ThreadStartRoutine)(void *);
/**
 * Create thread
 * @param tid : Tid.
 * @param startRoutine: ThreadStartRoutine.
 * @param arg: startRoutine input parameter.
 * @return
 */
ErrorCode ThreadCreate(Tid *tid, ThreadStartRoutine startRoutine, void *arg);

/**
 * Waiting for the thread execution to end.
 * @param tid: Tid.
 * @param valuePtr: ThreadStartRoutine return value.
 * @return
 */
ErrorCode ThreadJoin(Tid tid, void **valuePtr);

/**
 * Indicate to the implementation that storage for the thread tid can be reclaimed when that thread terminates.
 * @param tid: Tid.
 * @return
 */
ErrorCode ThreadDetach(Tid tid);

/**
 * The thread exit.
 * @param valuePtr
 */
void ThreadExit(void *valuePtr);

/**
 * Thread relinquishes the CPU
 */
void ThreadYield(void);

/* Thread priority */
typedef enum ThreadPriority { THR_PRI_LOW, THR_PRI_HIGH };
/**
 * Set the thread priority.
 * @param tid : Tid.
 * @param pri : ThreadPriority.
 */
ErrorCode ThreadSetPriority(Tid tid, ThreadPriority pri);

/**
 * Get the thread priority.
 * @param tid : Tid.
 * @param pri : ThreadPriority.
 */
ErrorCode ThreadGetPriority(Tid tid, ThreadPriority *pri);

/* The key wrapper. */
typedef struct ThreadKey ThreadKey;
struct ThreadKey {
    DWORD key;
};
/**
 * Set thread local variables.THR_LOCAL is preferentially used to define thread local variables.
 * The efficiency of THR_LOCAL is higher than that of ThreadSetSpecific.
 * @param key : ThreadKey
 * @param value : Thread local variables.
 * @return
 */
ErrorCode ThreadSetSpecific(ThreadKey key, const void *value);

/**
 * Get thread local variables.
 * @param key : ThreadKey
 * @return :  Thread local variables.
 */
void *ThreadGetSpecific(ThreadKey key);

/**
 * Create the key of a thread local variable.
 * @param key : ThreadKey, output parameter.
 * @return
 */
ErrorCode ThreadKeyCreate(ThreadKey *key);

/**
 * Delete the key of a thread local variable.
 * @param key : ThreadKey.
 * @return
 */
ErrorCode ThreadKeyDelete(ThreadKey key);

/**
 * Set a unique name for a thread.
 * @param name : The thread name is a meaningful C language string, whose length is restricted to 16 characters,
 * including the terminating null byte ('\0').
 * @return
 */
ErrorCode ThreadSetName(const char *name);
/**
 * Retrieve the name of the thread.
 * @param name : Used to return the thread name.
 * @param len :  Specifies the number of bytes available in name. The buffer specified by name should be at least 16
 * characters in length. The returned thread name in the output buffer will be null terminated.
 * @return
 */
ErrorCode ThreadGetName(char *name, size_t len);

GSDB_END_C_CODE_DECLS
#endif /* UTILS_WIN32_THREAD_H */
