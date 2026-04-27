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
 * posix_signal.c
 *
 * Description:
 * 1. Implementation of the POSIX signal interface wrapper.
 *
 * ---------------------------------------------------------------------------------
 */
#include <signal.h>
#include <sys/file.h>
#include "port/posix_atomic.h"
#include "port/port_page.h"
#include "port/posix_time.h"
#include "port/posix_signal.h"

/* The atomic variable protects the process signal context that is initialized only once. */
AtomicU8 g_processSignalContextInitialAtomicU8 = ATOMIC_INIT(ATOMICU8_INSTANCE_INITIAL_VALUE);
ProcessSignalContext g_processSignalContext;
THR_LOCAL ThreadSignalContext *g_threadSignalContext = NULL;
// clang-format off
/* Map table for converting cross-platform signal values to posix signal values. */
int g_portSignalToPosixSignalMap[MAX_SIG_NUM] = {INVALID_SIG_NUM,
                                                 SIGHUP,
                                                 SIGINT,
                                                 SIGQUIT,
                                                 SIGILL,
                                                 SIGTRAP,
                                                 SIGABRT,
                                                 SIGBUS,
                                                 SIGFPE,
                                                 SIGKILL,
                                                 SIGUSR1,
                                                 SIGSEGV,
                                                 SIGUSR2,
                                                 SIGPIPE,
                                                 SIGALRM,
                                                 SIGTERM,
                                                 SIGSTKFLT,
                                                 SIGCHLD,
                                                 SIGCONT,
                                                 SIGSTOP,
                                                 SIGTSTP,
                                                 SIGTTIN,
                                                 SIGTTOU,
                                                 SIGURG,
                                                 SIGXCPU,
                                                 SIGXFSZ,
                                                 SIGVTALRM,
                                                 SIGPROF,
                                                 SIGWINCH,
                                                 SIGIO,
                                                 SIGPWR,
                                                 SIGSYS};
/* Map table for converting posix signal values to cross-platform signal values. */
int g_posixSignalToPortSignalMap[MAX_SIG_NUM] = {INVALID_SIG_NUM,
                                                 SIG_HUP,
                                                 SIG_INT,
                                                 SIG_QUIT,
                                                 SIG_ILL,
                                                 SIG_TRAP,
                                                 SIG_ABRT,
                                                 SIG_BUS,
                                                 SIG_FPE,
                                                 SIG_KILL,
                                                 SIG_USR1,
                                                 SIG_SEGV,
                                                 SIG_USR2,
                                                 SIG_PIPE,
                                                 SIG_ALRM,
                                                 SIG_TERM,
                                                 SIG_STKFLT,
                                                 SIG_CHLD,
                                                 SIG_CONT,
                                                 SIG_STOP,
                                                 SIG_TSTP,
                                                 SIG_TTIN,
                                                 SIG_TTOU,
                                                 SIG_URG,
                                                 SIG_XCPU,
                                                 SIG_XFSZ,
                                                 SIG_VTALRM,
                                                 SIG_PROF,
                                                 SIG_WINCH,
                                                 SIG_IO,
                                                 SIG_PWR,
                                                 SIG_SYS};
// clang-format on
static ErrorCode ThreadSignalContextInitialize(void);
/* Converting cross-platform signal values to posix signal values. */
static void PortSignalToPosixSignal(int portSignalNo, int *posixSignalNo)
{
    if (portSignalNo <= 0 || portSignalNo >= MAX_SIG_NUM) {
        *posixSignalNo = INVALID_SIG_NUM;
    } else {
        *posixSignalNo = g_portSignalToPosixSignalMap[portSignalNo];
    }
}
/* Converting posix signal values to cross-platform signal values. */
static void PosixSignalToPortSignal(int posixSignalNo, int *portSignalNo)
{
    if (posixSignalNo <= 0 || posixSignalNo >= MAX_SIG_NUM) {
        *portSignalNo = INVALID_SIG_NUM;
    } else {
        *portSignalNo = g_posixSignalToPortSignalMap[posixSignalNo];
    }
}

/* Clear the signal set. */
UTILS_EXPORT ErrorCode SignalEmptySet(SignalSet *signalSet)
{
    ErrorCode errCode = ERROR_SYS_OK;
    int rc = sigemptyset(&(signalSet->signalSet));
    if (rc != 0) {
        PosixErrorCode2PortErrorCode(errno, &errCode);
    }
    return errCode;
}

/* Fill all signals into the signal set. */
UTILS_EXPORT ErrorCode SignalFillSet(SignalSet *signalSet)
{
    ErrorCode errCode = ERROR_SYS_OK;
    int rc = sigfillset(&(signalSet->signalSet));
    if (rc != 0) {
        PosixErrorCode2PortErrorCode(errno, &errCode);
    }
    return errCode;
}

/* Add a signal to the signal set. */
UTILS_EXPORT ErrorCode SignalAddSet(SignalSet *signalSet, int portSignalNo)
{
    int posixSignalNo;
    PortSignalToPosixSignal(portSignalNo, &posixSignalNo);
    if (posixSignalNo == INVALID_SIG_NUM) {
        return ERROR_UTILS_COMMON_INVALID_PARAMETER;
    }
    ErrorCode errCode = ERROR_SYS_OK;
    int rc = sigaddset(&(signalSet->signalSet), posixSignalNo);
    if (rc != 0) {
        PosixErrorCode2PortErrorCode(errno, &errCode);
    }
    return errCode;
}

/* Remove a signal from the signal set. */
UTILS_EXPORT ErrorCode SignalDelSet(SignalSet *signalSet, int portSignalNo)
{
    int posixSignalNo;
    PortSignalToPosixSignal(portSignalNo, &posixSignalNo);
    if (posixSignalNo == INVALID_SIG_NUM) {
        return ERROR_UTILS_COMMON_INVALID_PARAMETER;
    }
    ErrorCode errCode = ERROR_SYS_OK;
    int rc = sigdelset(&(signalSet->signalSet), posixSignalNo);
    if (rc != 0) {
        PosixErrorCode2PortErrorCode(errno, &errCode);
    }
    return errCode;
}

/* Whether a signal is a member of a signal set. */
UTILS_EXPORT ErrorCode SignalIsMember(const SignalSet *signalSet, int portSignalNo)
{
    int posixSignalNo;
    PortSignalToPosixSignal(portSignalNo, &posixSignalNo);
    if (posixSignalNo == INVALID_SIG_NUM) {
        return ERROR_UTILS_COMMON_INVALID_PARAMETER;
    }
    ErrorCode errCode = ERROR_SYS_OK;
    int rc = sigismember(&(signalSet->signalSet), posixSignalNo);
    if (rc == 1) {
        errCode = ERROR_UTILS_PORT_ETRUE;
    } else if (rc == 0) {
        errCode = ERROR_UTILS_PORT_EFALSE;
    } else {
        PosixErrorCode2PortErrorCode(errno, &errCode);
    }
    return errCode;
}

/* Set signal mask. */
UTILS_EXPORT ErrorCode SignalSetMask(SignalSet *signalSet)
{
    if (g_threadSignalContext == NULL) {
        return ERROR_UTILS_COMMON_FAILED;
    }
    errno_t result = memcpy_s(&(g_threadSignalContext->signalSet.signalSet), sizeof(SignalSet), &(signalSet->signalSet),
                              sizeof(SignalSet));
    if (result != 0) {
        return ERROR_UTILS_COMMON_SECURE_C_FUNCTION_FAILED;
    }
    sigset_t outSet;
    ErrorCode errCode = ERROR_SYS_OK;
    int rc = pthread_sigmask(SIG_SETMASK, &(signalSet->signalSet), &outSet);
    if (rc != 0) {
        PosixErrorCode2PortErrorCode(errno, &errCode);
    }
    return errCode;
}
/* Opens a named pipe for signal communication. */
static ErrorCode OpenSignalPipe(Pid pid, Pipe *pipe, bool init)
{
    ErrorCode errCode;
#define SIGNAL_PIPE_NAME_LEN 1024
#define SIGNAL_PID_STR_LEN   16
#define SIGNAL_TMP_PATH      "/tmp"
    if (access(SIGNAL_TMP_PATH, F_OK) != 0) {
        (void)mkdir(SIGNAL_TMP_PATH, S_IRWXU);
    }
    char pipePathName[SIGNAL_PIPE_NAME_LEN] = {0};
    char pidString[SIGNAL_PID_STR_LEN] = {0};
    Pid2String(&pid, pidString, SIGNAL_PID_STR_LEN);

    int rc = sprintf_s(pipePathName, SIGNAL_PIPE_NAME_LEN, "%s/%s%u_%s", SIGNAL_TMP_PATH, SIGNAL_FILE_PREFIX, getuid(),
                       pidString);
    if (unlikely(rc == -1)) {
        PosixErrorCode2PortErrorCode(errno, &errCode);
        return errCode;
    }
    errCode = MakeNamedPipe(pipePathName, PIPE_RDWR, pipe);
    if (errCode != ERROR_SYS_OK && errCode != ERROR_UTILS_PORT_EEXIST) {
        return errCode;
    }
    errCode = OpenNamedPipe(pipe, PIPE_RDWR);
    if (errCode == ERROR_SYS_OK) {
        errCode = SetNamedPipeNoBlock(pipe);
    }
    if ((errCode != ERROR_SYS_OK) && init) {
        (void)unlink(pipePathName);
    }
    return errCode;
}

/*  Read message of signals from pipelines. */
static ErrorCode ReadSignalInfoFromPipe(Pipe *pipe, int *portSignalNo, void *signalMessage, size_t *messageSize)
{
    ssize_t readCount = 0;
    ErrorCode errCode = ERROR_SYS_OK;
    size_t receivedMessageSize;
#define SIGNAL_PIPE_READ_STATE_NO      1
#define SIGNAL_PIPE_READ_STATE_SIZE    2
#define SIGNAL_PIPE_READ_STATE_MESSAGE 3
#define SIGNAL_PIPE_READ_STATE_END     4
    int state = SIGNAL_PIPE_READ_STATE_NO;
    while (state != SIGNAL_PIPE_READ_STATE_END) {
        switch (state) {
            case SIGNAL_PIPE_READ_STATE_NO:
                errCode = ReadFromPipe(pipe, portSignalNo, sizeof(*portSignalNo), &readCount);
                if (errCode != ERROR_SYS_OK) {
                    return errCode;
                }
                if (*portSignalNo <= 0 || *portSignalNo >= MAX_SIG_NUM) {
                    return ERROR_UTILS_COMMON_DATA_CORRUPTED;
                }
                state = SIGNAL_PIPE_READ_STATE_SIZE;
                break;
            case SIGNAL_PIPE_READ_STATE_SIZE:
                receivedMessageSize = SIGNAL_QUEUE_MAX_MESSAGE_SIZE;
                errCode = ReadFromPipe(pipe, &receivedMessageSize, sizeof(receivedMessageSize), &readCount);
                if (errCode != ERROR_SYS_OK) {
                    break;
                }
                if (receivedMessageSize > SIGNAL_QUEUE_MAX_MESSAGE_SIZE) {
                    return ERROR_UTILS_COMMON_DATA_CORRUPTED;
                }
                *messageSize = receivedMessageSize;
                if (receivedMessageSize == 0) {
                    return ERROR_SYS_OK;
                }
                state = SIGNAL_PIPE_READ_STATE_MESSAGE;
                break;
            case SIGNAL_PIPE_READ_STATE_MESSAGE:
                errCode = ReadFromPipe(pipe, signalMessage, receivedMessageSize, &readCount);
                if (errCode != ERROR_SYS_OK || receivedMessageSize != (size_t)readCount) {
                    return ERROR_UTILS_COMMON_DATA_CORRUPTED;
                }
                return ERROR_SYS_OK;
            default:
                break;
        }
    }
    return errCode;
}

/* Open the pipe, read the message, and then close the pipe. */
static ErrorCode ReadProcessSignalFromPipe(Pipe *pipe, int *portSignalNo, void *signalMessage, size_t *messageSize)
{
    ErrorCode errCode = ERROR_SYS_OK;
    errCode = ReadSignalInfoFromPipe(pipe, portSignalNo, signalMessage, messageSize);
    return errCode;
}

static void CopySignalInfoToThreadSignalQueues(int portSignalNo, void *signalMessage, size_t messageSize,
                                               ThreadSignalContext *threadSignalContext)
{
    threadSignalContext->signalQueues[portSignalNo].portSignalNo = portSignalNo;
    if (messageSize != 0) {
        errno_t rc = memcpy_s(threadSignalContext->signalQueues[portSignalNo].message, SIGNAL_QUEUE_MAX_MESSAGE_SIZE,
                              signalMessage, messageSize);
        if (rc != EOK) {
            Abort();
        }
        threadSignalContext->signalQueues[portSignalNo].actualSize = messageSize;
    } else {
        threadSignalContext->signalQueues[portSignalNo].actualSize = 0;
    }
}

static void SignalCallback(int portSignalNo, ThreadSignalContext *threadSignalContext)
{
    SignalQueues *signalQueues = &(threadSignalContext->signalQueues[portSignalNo]);
    if (threadSignalContext->signalCallbackFunctionType[portSignalNo] == SIGNAL_NO_CALLBACK_TYPE) {
        if (threadSignalContext->signalFunction[portSignalNo] == SIG_FUNC_DFL) {
            return;
        } else {
            threadSignalContext->signalFunction[portSignalNo](portSignalNo);
        }
    }
    if (threadSignalContext->signalCallbackFunctionType[portSignalNo] == SIGNAL_MESSAGE_CALLBACK_TYPE) {
        if (threadSignalContext->signalMessageFunction[portSignalNo] == SIG_MESSAGE_FUNC_DFL) {
            return;
        } else {
            if (signalQueues->actualSize != 0) {
                threadSignalContext->signalMessageFunction[portSignalNo](portSignalNo, signalQueues->message,
                                                                         signalQueues->actualSize);
            } else {
                threadSignalContext->signalMessageFunction[portSignalNo](portSignalNo, NULL, signalQueues->actualSize);
            }
        }
    }
}

static void DispatchSignalToThread(int portSignalNo, void *signalMessage, size_t messageSize)
{
    ErrorCode errCode;
    DListIter dListIter;
    ThreadSignalContext *threadSignalContext = NULL;
    ThreadSignalContext *threadSignalContextFound = NULL;
    MutexLock(&(g_processSignalContext.mutex));
    DLIST_FOR_EACH(dListIter, &(g_processSignalContext.head))
    {
        threadSignalContext = (ThreadSignalContext *)(uintptr_t)dListIter.cur;
        MutexLock(&(threadSignalContext->mutex));
        errCode = SignalIsMember(&(threadSignalContext->signalSet), portSignalNo);
        if (errCode == ERROR_UTILS_PORT_EFALSE) {
            threadSignalContextFound = threadSignalContext;
            break;
        }
        MutexUnlock(&(threadSignalContext->mutex));
    }
    MutexUnlock(&(g_processSignalContext.mutex));
    if (threadSignalContextFound == NULL) {
        return;
    }
    CopySignalInfoToThreadSignalQueues(portSignalNo, signalMessage, messageSize, threadSignalContextFound);
    SignalCallback(portSignalNo, threadSignalContextFound);
    MutexUnlock(&(threadSignalContext->mutex));
}

void *ProcessHandlerThread(void *arg)
{
    SignalQueues signalQueues;
    ErrorCode errCode;
    SignalSet signalSet;
    errCode = ThreadSignalContextInitialize();
    if (errCode != ERROR_SYS_OK) {
        return arg;
    }
    errCode = SignalFillSet(&signalSet);
    if (errCode != ERROR_SYS_OK) {
        return arg;
    }
    errCode = SignalSetMask(&signalSet);
    if (errCode != ERROR_SYS_OK) {
        return arg;
    }
    Pipe *pipe = (Pipe *)arg;
    errCode = SetNamedPipeBlock(pipe);
    if (errCode != ERROR_SYS_OK) {
        return arg;
    }
    while (!g_processSignalContext.signalPipeHandleThreadExit) {
        errCode = ReadProcessSignalFromPipe(pipe, &(signalQueues.portSignalNo), signalQueues.message,
                                            &(signalQueues.actualSize));
        if (errCode != ERROR_SYS_OK) {
            continue;
        }
        if (signalQueues.portSignalNo <= 0 || signalQueues.portSignalNo >= MAX_SIG_NUM) {
            continue;
        }
        DispatchSignalToThread(signalQueues.portSignalNo, signalQueues.message, signalQueues.actualSize);
    }
    return arg;
}

static bool IsFileOpened(char *fileName)
{
    char resolvedFilename[PATH_MAX];
    if (!CanonicalizePath(fileName, resolvedFilename)) {
        ErrLog(ERROR, ErrMsg("file name : %s canonicalize path failed, errno = %d ", fileName, errno));
        return false;
    }
    int file = open(resolvedFilename, O_RDWR);
    if (file != -1) {
        /* lock file */
        int ret = flock(file, LOCK_EX | LOCK_NB);
        if (ret == -1) {
            /* file has been occupied by other process */
            (void)close(file);
            return true;
        }
        /* unlock file */
        (void)flock(file, LOCK_UN);
        (void)close(file);
        return false;
    }
    return false;
}

static ErrorCode ClearInvalidSignalFile(void)
{
    /* Traversing each file in the Diectory */
    Directory dir;
    if (access(SIGNAL_TMP_PATH, F_OK) != 0) {
        return ERROR_SYS_OK;
    }
    ErrorCode errCode = OpenDirectory(SIGNAL_TMP_PATH, &dir);
    if (errCode != ERROR_SYS_OK) {
        return errCode;
    }

    char signalFilePrefix[SIGNAL_PIPE_NAME_LEN] = {0};
    int rc = sprintf_s(signalFilePrefix, sizeof(signalFilePrefix), "%s%u", SIGNAL_FILE_PREFIX, getuid());
    if (unlikely(rc == -1)) {
        PosixErrorCode2PortErrorCode(errno, &errCode);
        return errCode;
    }

    DirectoryEntry dirEntry;
    while (ReadDirectory(&dir, &dirEntry)) {
        /* is file */
        if (!strcmp(dirEntry.name, ".") || !strcmp(dirEntry.name, "..")) {
            continue;
        }

        if (strstr(dirEntry.name, signalFilePrefix) == NULL) {
            continue;
        }

        char filePath[SIGNAL_PIPE_NAME_LEN] = {0};
        rc = sprintf_s(filePath, sizeof(filePath), "%s/%s", SIGNAL_TMP_PATH, dirEntry.name);
        if (unlikely(rc < 0)) {
            CloseDirectory(&dir);
            PosixErrorCode2PortErrorCode(errno, &errCode);
            return errCode;
        }
        if (!IsFileOpened(filePath)) {
            (void)unlink(filePath);
        }
    }
    CloseDirectory(&dir);
    return ERROR_SYS_OK;
}

static ErrorCode CreateProcessSignalHandlerThread(void)
{
    ErrorCode errCode;
    errCode = ClearInvalidSignalFile();
    if (errCode != ERROR_SYS_OK) {
        return errCode;
    }

    Pid pid = GetCurrentPid();
    MutexLock(&(g_processSignalContext.mutex));
    errCode = OpenSignalPipe(pid, &(g_processSignalContext.pipe), true);
    if (errCode != ERROR_SYS_OK) {
        MutexUnlock(&(g_processSignalContext.mutex));
        return errCode;
    }
    /* lock file */
    int ret = flock(g_processSignalContext.pipe.pipe, LOCK_EX | LOCK_NB);
    if (ret == -1) {
        MutexUnlock(&(g_processSignalContext.mutex));
        PosixErrorCode2PortErrorCode(errno, &errCode);
        return errCode;
    }

    Tid tid;
    errCode = ThreadCreate(&tid, ProcessHandlerThread, &(g_processSignalContext.pipe));
    MutexUnlock(&(g_processSignalContext.mutex));
    return errCode;
}

/*
 * Signal processing wrapper function, in which the signal processing function
 * of the application is called.
 */
static void SignalHandler(int posixSignalNo)
{
    int portSignalNo;
    PosixSignalToPortSignal(posixSignalNo, &portSignalNo);
    if (portSignalNo <= 0 || portSignalNo >= MAX_SIG_NUM) {
        return;
    }
    if (g_threadSignalContext != NULL && g_threadSignalContext->signalFunction[portSignalNo] != SIG_FUNC_DFL) {
        g_threadSignalContext->signalFunction[portSignalNo](portSignalNo);
    }
}
/*
 * Registers the wrapper signal processing function to the OS.
 */
static SignalFunction RegisterSignalHandler(int portSignalNo)
{
    struct sigaction action;
    action.sa_handler = SignalHandler;
    if (sigemptyset(&action.sa_mask) < 0) {
        return SIG_FUNC_ERR;
    }
    action.sa_flags = SA_RESTART;

    int posixSignalNo;
    PortSignalToPosixSignal(portSignalNo, &posixSignalNo);
#ifdef SA_NOCLDSTOP
    if (posixSignalNo == SIGCHLD) {
        action.sa_flags = SA_RESTART | SA_NOCLDSTOP;
    }
#endif
    if (sigaction(posixSignalNo, &action, NULL) < 0) {
        return SIG_FUNC_ERR;
    }
    return SIG_FUNC_DFL;
}

/* Register signal no processing functions for application. */
UTILS_EXPORT SignalFunction Signal(int portSignalNo, SignalFunction handler)
{
    if (g_threadSignalContext == NULL) {
        return SIG_FUNC_ERR;
    }

    SignalFunction oldSignalFunction = NULL;
    if (portSignalNo <= 0 || portSignalNo >= MAX_SIG_NUM) {
        return SIG_FUNC_ERR;
    }
    MutexLock(&(g_threadSignalContext->mutex));
    oldSignalFunction = g_threadSignalContext->signalFunction[portSignalNo];
    g_threadSignalContext->signalCallbackFunctionType[portSignalNo] = SIGNAL_NO_CALLBACK_TYPE;
    g_threadSignalContext->signalFunction[portSignalNo] = handler;
    MutexUnlock(&(g_threadSignalContext->mutex));
    SignalFunction signalFunction = NULL;
    signalFunction = RegisterSignalHandler(portSignalNo);
    if (signalFunction == SIG_FUNC_ERR) {
        return SIG_FUNC_ERR;
    }
    return oldSignalFunction;
}

/* Register signal message processing functions for application. */
UTILS_EXPORT SignalMessageFunction SignalMessage(int portSignalNo, SignalMessageFunction handler)
{
    if (g_threadSignalContext == NULL) {
        return SIG_MESSAGE_FUNC_ERR;
    }

    SignalMessageFunction oldSignalMessageFunction = NULL;
    if (portSignalNo <= 0 || portSignalNo >= MAX_SIG_NUM) {
        return SIG_MESSAGE_FUNC_ERR;
    }
    MutexLock(&(g_threadSignalContext->mutex));
    oldSignalMessageFunction = g_threadSignalContext->signalMessageFunction[portSignalNo];
    g_threadSignalContext->signalCallbackFunctionType[portSignalNo] = SIGNAL_MESSAGE_CALLBACK_TYPE;
    g_threadSignalContext->signalMessageFunction[portSignalNo] = handler;
    MutexUnlock(&(g_threadSignalContext->mutex));
    return oldSignalMessageFunction;
}
/* Place the signal message to the queue of the corresponding thread signal. */
static ErrorCode SendThreadSignalToSignalQueues(Tid tid, int portSignalNo, void *signalMessage, size_t messageSize)
{
    DListIter dListIter;
    ThreadSignalContext *threadSignalContext = NULL;
    ThreadSignalContext *threadSignalContextFound = NULL;
    MutexLock(&(g_processSignalContext.mutex));
    DLIST_FOR_EACH(dListIter, &(g_processSignalContext.head))
    {
        threadSignalContext = (ThreadSignalContext *)(uintptr_t)dListIter.cur;
        MutexLock(&(threadSignalContext->mutex));
        if (TidIsEqual(&tid, &(threadSignalContext->tid))) {
            threadSignalContextFound = threadSignalContext;
            break;
        }
        MutexUnlock(&(threadSignalContext->mutex));
    }
    MutexUnlock(&(g_processSignalContext.mutex));
    if (threadSignalContextFound == NULL) {
        return ERROR_UTILS_COMMON_INVALID_PARAMETER;
    }
    CopySignalInfoToThreadSignalQueues(portSignalNo, signalMessage, messageSize, threadSignalContextFound);
    SignalCallback(portSignalNo, threadSignalContextFound);
    MutexUnlock(&(threadSignalContext->mutex));
    return ERROR_SYS_OK;
}

UTILS_EXPORT ErrorCode ThreadKill(Tid tid, int portSignalNo)
{
    ErrorCode errCode = ERROR_SYS_OK;
    int posixSignalNo;
    PortSignalToPosixSignal(portSignalNo, &posixSignalNo);
    int rc;
    rc = pthread_kill(tid.tid, posixSignalNo);
    if (rc != 0) {
        PosixErrorCode2PortErrorCode(rc, &errCode);
    }
    return errCode;
}

/* Sending a signal to a thread. */
UTILS_EXPORT ErrorCode SendThreadSignal(Tid tid, int portSignalNo, void *signalMessage, size_t messageSize)
{
    if (portSignalNo <= 0 || portSignalNo >= MAX_SIG_NUM) {
        return ERROR_UTILS_COMMON_INVALID_PARAMETER;
    }
    if (signalMessage != NULL && messageSize > SIGNAL_QUEUE_MAX_MESSAGE_SIZE) {
        return ERROR_UTILS_COMMON_INVALID_PARAMETER;
    }
    ErrorCode errCode;
    errCode = SendThreadSignalToSignalQueues(tid, portSignalNo, signalMessage, messageSize);
    return errCode;
}
/* Write the signal message to the pipe. */
static ErrorCode WriteSignalInfoToPipe(Pipe *pipe, int portSignalNo, void *signalMessage, size_t messageSize)
{
    ErrorCode errCode;
    errCode = WriteToPipe(pipe, &portSignalNo, sizeof(portSignalNo));
    if (errCode != ERROR_SYS_OK) {
        return errCode;
    }
    if (signalMessage != NULL && messageSize != 0) {
        errCode = WriteToPipe(pipe, &messageSize, sizeof(messageSize));
        if (errCode != ERROR_SYS_OK) {
            return errCode;
        }
        errCode = WriteToPipe(pipe, signalMessage, messageSize);
    } else {
        size_t zeroMessageSize = 0;
        errCode = WriteToPipe(pipe, &zeroMessageSize, sizeof(zeroMessageSize));
        if (errCode != ERROR_SYS_OK) {
            return errCode;
        }
    }
    return errCode;
}
/* Open the pipe, write the signal message to the pipe, and close the pipe. */
static ErrorCode WriteProcessSignalToPipeWithoutThreadSafe(Pid pid, int portSignalNo, void *signalMessage,
                                                           size_t messageSize)
{
    ErrorCode errCode;
    Pipe pipe;
    errCode = OpenSignalPipe(pid, &pipe, false);
    if (errCode != ERROR_SYS_OK) {
        return errCode;
    }
    errCode = WriteSignalInfoToPipe(&pipe, portSignalNo, signalMessage, messageSize);
    CloseNamedPipe(&pipe);
    return errCode;
}
/* Open the pipe, write the signal message to the pipe, and close the pipe. */
static ErrorCode WriteProcessSignalToPipe(Pid pid, int portSignalNo, void *signalMessage, size_t messageSize)
{
    ErrorCode errCode = ERROR_SYS_OK;
    MutexLock(&(g_processSignalContext.mutex));
    errCode = WriteProcessSignalToPipeWithoutThreadSafe(pid, portSignalNo, signalMessage, messageSize);
    MutexUnlock(&(g_processSignalContext.mutex));
    return errCode;
}

UTILS_EXPORT ErrorCode ProcessKill(Pid pid, int portSignalNo)
{
    ErrorCode errCode = ERROR_SYS_OK;
    int posixSignalNo;
    PortSignalToPosixSignal(portSignalNo, &posixSignalNo);
    int rc;
    rc = kill(pid.pid, posixSignalNo);
    if (rc != 0) {
        PosixErrorCode2PortErrorCode(errno, &errCode);
    }
    return errCode;
}
/* Sending a signals to a process. */
UTILS_EXPORT ErrorCode SendProcessSignal(Pid pid, int portSignalNo, void *signalMessage, size_t messageSize)
{
    ErrorCode errCode = ERROR_SYS_OK;
    if (portSignalNo <= 0 || portSignalNo >= MAX_SIG_NUM) {
        return ERROR_UTILS_COMMON_INVALID_PARAMETER;
    }
    if (signalMessage != NULL && messageSize > SIGNAL_QUEUE_MAX_MESSAGE_SIZE) {
        return ERROR_UTILS_COMMON_INVALID_PARAMETER;
    }
    errCode = WriteProcessSignalToPipe(pid, portSignalNo, signalMessage, messageSize);
    return errCode;
}

/* Hold all locks before process fork. */
void SignalMutexPrefork(void)
{
    /* Check child process is being executed, and will not be locked again */
    if (AtomicU8Get(&g_processSignalContextInitialAtomicU8, MEMORY_ORDER_SEQ_CST) == ATOMICU8_INSTANCE_INITIAL_VALUE) {
        return;
    }
    DListIter dListIter;
    ThreadSignalContext *threadSignalContext = NULL;
    MutexLock(&(g_processSignalContext.mutex));
    DLIST_FOR_EACH(dListIter, &(g_processSignalContext.head))
    {
        threadSignalContext = (ThreadSignalContext *)(uintptr_t)dListIter.cur;
        MutexLock(&(threadSignalContext->mutex));
    }
}

static void SignalMutexUnlock(void)
{
    DListIter dListIter;
    ThreadSignalContext *threadSignalContext = NULL;
    DLIST_FOR_EACH(dListIter, &(g_processSignalContext.head))
    {
        threadSignalContext = (ThreadSignalContext *)(uintptr_t)dListIter.cur;
        MutexUnlock(&(threadSignalContext->mutex));
    }
    MutexUnlock(&(g_processSignalContext.mutex));
}

/* The parent process releases all locks after fork. */
void SignalMutexPostforkParent(void)
{
    /* Check child process is being executed, and will not be unlocked again */
    if (AtomicU8Get(&g_processSignalContextInitialAtomicU8, MEMORY_ORDER_SEQ_CST) == ATOMICU8_INSTANCE_INITIAL_VALUE) {
        return;
    }
    SignalMutexUnlock();
}

/* The child process releases all locks after fork. */
void SignalMutexPostforkChild(void)
{
    AtomicU8 initialAtomicU8 = ATOMIC_INIT(ATOMICU8_INSTANCE_INITIAL_VALUE);
    errno_t result =
        memcpy_s(&g_processSignalContextInitialAtomicU8, sizeof(AtomicU8), &initialAtomicU8, sizeof(AtomicU8));
    if (result != 0) {
        Abort();
    }
}

/* Process signal context initialize. */
static ErrorCode ProcessSignalContextInitialize(void *processSignalContextArg)
{
    ErrorCode errorCode;
    ProcessSignalContext *processSignalContext = (ProcessSignalContext *)processSignalContextArg;
    MutexInit(&(processSignalContext->mutex));
    DListInit(&(processSignalContext->head));
    processSignalContext->signalPipeHandleThreadExit = false;
    errorCode = CreateProcessSignalHandlerThread();
    if (errorCode != ERROR_SYS_OK) {
        return errorCode;
    }
    return ERROR_SYS_OK;
}
/* Process signal context initialize. */
static ErrorCode ProcessSignalContextInitializeOnlyOnce(void *processSignalContextArg)
{
    ErrorCode errorCode;
    errorCode = ProcessSignalContextInitialize(processSignalContextArg);
    if (errorCode != ERROR_SYS_OK) {
        return errorCode;
    }
    int result = pthread_atfork(SignalMutexPrefork, SignalMutexPostforkParent, SignalMutexPostforkChild);
    if (result != 0) {
        PosixErrorCode2PortErrorCode(result, &errorCode);
        return errorCode;
    }
    return ERROR_SYS_OK;
}
/* Thread signal context initialize. */
static ErrorCode ThreadSignalContextInitialize(void)
{
    ErrorCode errorCode;
    ThreadSignalContext *threadSignalContext = NULL;
    threadSignalContext = (ThreadSignalContext *)MemPagesAlloc(sizeof(ThreadSignalContext));
    if (threadSignalContext == NULL) {
        return ERROR_UTILS_COMMON_NOENOUGH_MEMORY;
    }
    DListNodeInit(&(threadSignalContext->threadSignalContextNode));
    MutexInit(&(threadSignalContext->mutex));
    threadSignalContext->tid = GetCurrentTid();
    errorCode = SignalEmptySet(&(threadSignalContext->signalSet));
    if (errorCode != ERROR_SYS_OK) {
        MemPagesFree((void *)threadSignalContext, sizeof(ThreadSignalContext));
        return errorCode;
    }
    uint32_t i;
    for (i = 0; i < MAX_SIG_NUM; i++) {
        threadSignalContext->signalCallbackFunctionType[i] = SIGNAL_NO_CALLBACK_TYPE;
        threadSignalContext->signalFunction[i] = SIG_FUNC_DFL;
        threadSignalContext->signalMessageFunction[i] = SIG_MESSAGE_FUNC_DFL;
        threadSignalContext->signalQueues[i].portSignalNo = INVALID_SIG_NUM;
        threadSignalContext->signalQueues[i].actualSize = 0;
    }
    g_threadSignalContext = threadSignalContext;
    MutexLock(&(g_processSignalContext.mutex));
    DListPushHead(&(g_processSignalContext.head), &(threadSignalContext->threadSignalContextNode));
    MutexUnlock(&(g_processSignalContext.mutex));
    return errorCode;
}
/* Signal initialize. */
UTILS_EXPORT ErrorCode SignalInitialize(void)
{
    ErrorCode errorCode;
    errorCode = InitializedOnlyOnce(&g_processSignalContextInitialAtomicU8, ProcessSignalContextInitializeOnlyOnce,
                                    &g_processSignalContext);
    if (errorCode != ERROR_SYS_OK) {
        return errorCode;
    }
    errorCode = ThreadSignalContextInitialize();
    return errorCode;
}

/* Destroy signal related resources. */
UTILS_EXPORT void SignalDestroy()
{
    MutexLock(&(g_processSignalContext.mutex));
    MutexLock(&(g_threadSignalContext->mutex));
    DListDelete(&(g_threadSignalContext->threadSignalContextNode));
    MutexUnlock(&(g_threadSignalContext->mutex));
    MutexUnlock(&(g_processSignalContext.mutex));

    MutexDestroy(&(g_threadSignalContext->mutex));
    MemPagesFree((void *)g_threadSignalContext, sizeof(ThreadSignalContext));
    g_threadSignalContext = NULL;
    (void)flock(g_processSignalContext.pipe.pipe, LOCK_UN);
    (void)close(g_processSignalContext.pipe.pipe);
    (void)unlink(g_processSignalContext.pipe.pathname);
    AtomicU8 initialAtomicU8 = ATOMIC_INIT(ATOMICU8_INSTANCE_INITIAL_VALUE);
    g_processSignalContextInitialAtomicU8 = initialAtomicU8;
    return;
}
