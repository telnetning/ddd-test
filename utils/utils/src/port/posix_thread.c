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
 * posix_thread.c
 *
 * Description:
 * 1.Implementation of the POSIX thread interface wrapper.
 *
 * ---------------------------------------------------------------------------------
 */
#include <errno.h>
#include "port/posix_thread.h"

/**
 * Get the current process id.
 * @return : Pid
 */
UTILS_EXPORT Pid GetCurrentPid(void)
{
    Pid pid;
    pid.pid = getpid();
    return pid;
}

UTILS_EXPORT void SetPid(Pid *pid, int value)
{
    pid->pid = value;
}
/**
 * Whether the two processes id are equal.
 * @param pid1 : Pid. Refer to <sys/types.h>,the pid_t type is the same as the int type.
 * @param pid2 : Pid
 * @return : true or false.
 */
UTILS_EXPORT bool PidIsEqual(const Pid *pid1, const Pid *pid2)
{
    return (pid1->pid == pid2->pid) ? true : false;
}

/**
 * Converts the process PID value to a character string.
 * @param pid : Pid.
 * @param pidStr : Output parameter, PID conversion string.
 * @param size : Pid string size.
 */
UTILS_EXPORT void Pid2String(const Pid *pid, char *pidStr, size_t size)
{
    int rc = sprintf_s(pidStr, size, "%d", pid->pid);
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
 * @return : Tid
 */
UTILS_EXPORT Tid GetCurrentTid(void)
{
    Tid tid;
    tid.tid = pthread_self();
    return tid;
}

UTILS_EXPORT void SetTid(Tid *tid, unsigned long value)
{
    tid->tid = value;
}
/**
 * Whether the two thread id are equal.
 * @param tid1 : Tid
 * @param tid2 : Tid
 * @return : true or false.
 */
UTILS_EXPORT bool TidIsEqual(Tid *tid1, Tid *tid2)
{
    int rc;
    rc = pthread_equal(tid1->tid, tid2->tid);
    return (rc != 0) ? true : false;
}

/**
 * Converts the thread id value to a character string.In the Linux implementation, pthread_t is defined as
 * "unsigned long int". The print thread pthread_t uses %lu.
 * @param tid : Tid.
 * @param tidStr : Output parameter, TID conversion string.
 * @param size : Tid string size.
 */
UTILS_EXPORT void Tid2String(const Tid *tid, char *tidStr, size_t size)
{
    int rc = sprintf_s(tidStr, size, "%lu", tid->tid);
    if (rc < 0) {
        for (unsigned long i = 0; i < size - 1; i++) {
            tidStr[i] = '?';
        }
        tidStr[size - 1] = '\0';
    }
}

UTILS_EXPORT uint64_t Tid2Integer(const Tid *tid)
{
    ASSERT(tid != NULL);
    return tid->tid;
}

/* The Linux default stack size is 8M (8388608) and the minimum stack size is 16K(16384). Here use default 10M. */
#define THREAD_STACK_SIZE_BYTES (10 * 1024 * 1024)

/**
 * Create thread
 * @param tid : Tid.
 * @param startRoutine: ThreadStartRoutine.
 * @param arg: startRoutine input parameter.
 * @return : ErrorCode.
 */
UTILS_EXPORT ErrorCode ThreadCreate(Tid *tid, ThreadStartRoutine startRoutine, void *arg)
{
    int rc;
    ErrorCode errCode = ERROR_SYS_OK;
    pthread_attr_t attr;
    rc = pthread_attr_init(&attr);
    if (rc != 0) {
        PosixErrorCode2PortErrorCode(rc, &errCode);
        return errCode;
    }
#if defined(_POSIX_THREAD_ATTR_STACKSIZE)
    /* The unit of stacksize is byte. */
    rc = pthread_attr_setstacksize(&attr, THREAD_STACK_SIZE_BYTES);
    if (rc != 0) {
        PosixErrorCode2PortErrorCode(rc, &errCode);
        return errCode;
    }
#endif
    rc = pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
    if (rc != 0) {
        PosixErrorCode2PortErrorCode(rc, &errCode);
        return errCode;
    }
    rc = pthread_create(&(tid->tid), &attr, startRoutine, arg);
    if (rc != 0) {
        PosixErrorCode2PortErrorCode(rc, &errCode);
        return errCode;
    }
    rc = pthread_attr_destroy(&attr);
    if (rc != 0) {
        PosixErrorCode2PortErrorCode(rc, &errCode);
        return errCode;
    }
    return errCode;
}

/**
 * Waiting for the thread execution to end.
 * @param tid: Tid.
 * @param valuePtr: ThreadStartRoutine return value.
 * @return : ErrorCode.
 */
UTILS_EXPORT ErrorCode ThreadJoin(Tid tid, void **valuePtr)
{
    int rc;
    ErrorCode errCode = ERROR_SYS_OK;
    rc = pthread_join(tid.tid, valuePtr);
    if (rc != 0) {
        PosixErrorCode2PortErrorCode(rc, &errCode);
        return errCode;
    }
    return errCode;
}

/**
 * Indicate to the implementation that storage for the thread tid can be reclaimed when that thread terminates.
 * @param tid: Tid.
 * @return : ErrorCode.
 */
UTILS_EXPORT ErrorCode ThreadDetach(Tid tid)
{
    int rc;
    ErrorCode errCode = ERROR_SYS_OK;
    rc = pthread_detach(tid.tid);
    if (rc != 0) {
        PosixErrorCode2PortErrorCode(rc, &errCode);
        return errCode;
    }
    return errCode;
}

/**
 * The thread exit.
 * @param valuePtr
 */
UTILS_EXPORT void ThreadExit(void *valuePtr)
{
    pthread_exit(valuePtr);
}

UTILS_EXPORT void ThreadYield(void)
{
    (void)sched_yield();
}

/**
 * Set the thread priority. Total three scheduling policies: SCHED_OTHER, SCHED_FIFO, and SCHED_RR.
 * SCHED_FIFO and SCHED_RR can be set only for the super user.
 * @param tid : Tid.
 * @param pri : ThreadPriority.
 * @return : ErrorCode.
 */
UTILS_EXPORT ErrorCode ThreadSetPriority(Tid tid, ThreadPriority pri)
{
#define NUMERIC_CONSTANT_TWO 2
    int rc;
    ErrorCode errCode = ERROR_SYS_OK;
    struct sched_param schedParam;
    int minPriority = sched_get_priority_min(SCHED_OTHER);
    int maxPriority = sched_get_priority_max(SCHED_OTHER);
    switch (pri) {
        case THR_PRI_LOW:
            schedParam.sched_priority = minPriority;
            break;
        case THR_PRI_MIDDLE:
            schedParam.sched_priority = (minPriority + maxPriority) / NUMERIC_CONSTANT_TWO;
            break;
        case THR_PRI_HIGH:
            schedParam.sched_priority = maxPriority;
            break;
        default:
            schedParam.sched_priority = minPriority;
            break;
    }
    rc = pthread_setschedparam(tid.tid, SCHED_OTHER, &schedParam);
    if (rc != 0) {
        PosixErrorCode2PortErrorCode(rc, &errCode);
        return errCode;
    }
    return errCode;
}

/**
 * Get the thread priority.
 * @param tid : Tid.
 * @param pri : ThreadPriority.
 * @return : ErrorCode.
 */
UTILS_EXPORT ErrorCode ThreadGetPriority(Tid tid, ThreadPriority *pri)
{
    int rc;
    ErrorCode errCode = ERROR_SYS_OK;
    struct sched_param schedParam;
    int policy;
    rc = pthread_getschedparam(tid.tid, &policy, &schedParam);
    if (rc != 0) {
        PosixErrorCode2PortErrorCode(rc, &errCode);
        return errCode;
    }
    int minPriority = sched_get_priority_min(SCHED_OTHER);
    int maxPriority = sched_get_priority_max(SCHED_OTHER);
    if (schedParam.sched_priority == minPriority) {
        *pri = THR_PRI_LOW;
    } else if (schedParam.sched_priority == maxPriority) {
        *pri = THR_PRI_HIGH;
    } else {
        *pri = THR_PRI_MIDDLE;
    }
    return errCode;
}

/**
 * Set thread local variables.THR_LOCAL is preferentially used to define thread local variables.
 * The efficiency of THR_LOCAL is higher than that of ThreadSetSpecific.
 * THR_LOCAL is supported by the GCC compiler.ThreadKey is supported by OS.
 * @param key : ThreadKey
 * @param value : Thread local variables.
 * @return : ErrorCode.
 */
UTILS_EXPORT ErrorCode ThreadSetSpecific(ThreadKey key, const void *value)
{
    int rc;
    ErrorCode errCode = ERROR_SYS_OK;
    rc = pthread_setspecific(key.key, value);
    if (rc != 0) {
        PosixErrorCode2PortErrorCode(rc, &errCode);
        return errCode;
    }
    return errCode;
}

/**
 * Get thread local variables.If no thread specific data value is associated with key, then the value NULL is returned.
 * @param key : ThreadKey
 * @return :  Thread local variables or NULL.
 */
UTILS_EXPORT void *ThreadGetSpecific(ThreadKey key)
{
    return pthread_getspecific(key.key);
}

/**
 * Create the key of a thread local variable.
 * @param key : ThreadKey, output parameter.
 * @return : ErrorCode.
 */
UTILS_EXPORT ErrorCode ThreadKeyCreate(ThreadKey *key)
{
    int rc;
    ErrorCode errCode = ERROR_SYS_OK;
    rc = pthread_key_create(&(key->key), NULL);
    if (rc != 0) {
        PosixErrorCode2PortErrorCode(rc, &errCode);
        return errCode;
    }
    return errCode;
}

/**
 * Delete the key of a thread local variable.
 * @param key : ThreadKey.
 * @return : ErrorCode.
 */
UTILS_EXPORT ErrorCode ThreadKeyDelete(ThreadKey key)
{
    int rc;
    ErrorCode errCode = ERROR_SYS_OK;
    rc = pthread_key_delete(key.key);
    if (rc != 0) {
        PosixErrorCode2PortErrorCode(rc, &errCode);
        return errCode;
    }
    return errCode;
}

/**
 * Set a unique name for a thread.
 * @param name : The thread name is a meaningful C language string, whose length is restricted to 16 characters,
 * including the terminating null byte ('\0').
 * @return : ErrorCode.
 */
UTILS_EXPORT ErrorCode ThreadSetName(const char *name)
{
    ErrorCode errCode = ERROR_SYS_OK;
    int rc;
#ifdef _GNU_SOURCE
    pthread_t tid = pthread_self();
    rc = pthread_setname_np(tid, name);
    if (rc != 0) {
        PosixErrorCode2PortErrorCode(rc, &errCode);
    }
#else
    rc = prctl(PR_SET_NAME, (unsigned long)(uintptr_t)name);
    if (rc != 0) {
        PosixErrorCode2PortErrorCode(errno, &errCode);
    }
#endif
    return errCode;
}
/**
 * Retrieve the name of the thread.
 * @param name : Used to return the thread name.
 * @param len :  Specifies the number of bytes available in name. The buffer specified by name should be at least 16
 * characters in length. The returned thread name in the output buffer will be null terminated.
 * @return : ErrorCode.
 */
UTILS_EXPORT ErrorCode ThreadGetName(char *name, size_t len)
{
    ErrorCode errCode = ERROR_SYS_OK;
    int rc;
#ifdef _GNU_SOURCE
    pthread_t tid = pthread_self();
    rc = pthread_getname_np(tid, name, len);
    if (rc != 0) {
        PosixErrorCode2PortErrorCode(rc, &errCode);
    }
#else
    char threadName[THREAD_NAME_LEN] = {0};
    rc = prctl(PR_GET_NAME, (unsigned long)threadName);
    if (rc != 0) {
        PosixErrorCode2PortErrorCode(errno, &errCode);
        return errCode;
    }
    rc = sprintf_s(name, len, "%s", threadName);
    if (rc < 0) {
        unsigned int i;
        for (i = 0; i < len - 1; i++) {
            name[i] = '?';
        }
        name[len - 1] = '\0';
    }
#endif
    return errCode;
}
