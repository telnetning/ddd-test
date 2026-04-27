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
 * err_log.c
 *
 * Description:
 * 1. Error log implementation.
 *
 * ---------------------------------------------------------------------------------
 */
#include <stdarg.h>
#include <stdio.h>
#include <unistd.h>
#include <ctype.h>

#include "securec.h"
#include "container/string_info.h"
#include "fault_injection/fault_injection.h"
#include "syslog/err_log_internal.h"
#include "syslog/err_log_fold.h"
#include "syslog/trace.h"
#include "port/posix_time.h"

#ifdef LOCAL_MODULE_NAME
#undef LOCAL_MODULE_NAME
#endif
#define LOCAL_MODULE_NAME "syslog"

/* Error log connection. */
typedef struct ErrLogConnection ErrLogConnection;
struct ErrLogConnection {
    char *srcIP;  /* Source ip address. */
    int srcPort;  /* Source port. */
    char *destIP; /* Destination ip address. */
    int destPort; /* Destination port. */
};

/* Error log session. */
typedef struct ErrLogSession ErrLogSession;
struct ErrLogSession {
    char *applicationName; /* Application program name. */
    char *databaseName;    /* Database name. */
    char *userName;        /* User name. */
    int sessionID;         /* Session identification. */
};

/* Error log running context. */
typedef struct ErrLogRunningContext ErrLogRunningContext;
struct ErrLogRunningContext {
    Pid pid;          /* Current pid. */
    Tid tid;          /* Current tid. */
    char *threadName; /* Current thread name. */
};

/* Error log position context. */
typedef struct ErrLogPositionContext ErrLogPositionContext;
struct ErrLogPositionContext {
    char *fileName; /* File name. */
    int lineNo;     /* Line number. */
    char *funcName; /* Function name. */
};

/*
 * ErrorData holds the message data accumulated during anyone errLog cycle.
 */
typedef struct ErrorData ErrorData;
struct ErrorData {
    int errorLevel;                        /* Error level. */
    bool outputToServer;                   /* Will report to server log ? */
    bool outputToClient;                   /* Will report to client ? */
    ErrLogPositionContext positionContext; /* Invoke the ErrLog position information. */
    ErrLogModuleContext moduleContext;     /* Invoke the ErrLog module information. */
    char *backtrace;                       /* The buffer for backtrace. */
    char *context;                         /* Context message. */
    const char *domain;                    /* Message domain. */
    int sqlErrorCode;                      /* Sql error code ERRSTATE. */
    char *message;                         /* Primary error message. */
    char *cause;                           /* Error cause. */
    char *action;                          /* Error action. */
    uint8_t messageType;                  /* Error message type. */
};

typedef struct SyslogOutputConfig SyslogOutputConfig;
struct SyslogOutputConfig {
    /* Whether each message is prefixed with an increasing sequence number (e.g., [2]). */
    bool syslogSequenceNumbers;
    /* Whether the message will be divided into lines so that it can be placed in 1024 bytes. */
    bool syslogSplitMessages;
};

/* Used to determine whether a valid transaction ID is set. */
#define INVALID_TRANSACTION_ID 0

/* Provide a small stack of ErrorData records for re-entrant cases. */
#define ERRORDATA_STACK_SIZE   5
#define MAX_ERROR_FILTER_COUNT 5
typedef struct ThreadErrorLogContextData ThreadErrorLogContextData;
struct ThreadErrorLogContextData {
    ErrLogConnection connectionInfo;           /* The current connection information. */
    ErrLogSession sessionInfo;                 /* The current session information. */
    ErrLogRunningContext runningContextInfo;   /* The current running context information. */
    int transactionID;                         /* The currently executed transaction. */
    char *queryString;                         /* The currently executed query string. */
    SListHead errorLogContextCallbackHead;     /* The error log context callback head. */
    int (*threadExitCallback)(void *valuePtr); /* The fatal error thread exit callback. */
    void *valuePtr;                            /* The fatal error thread exit return value. */
    void (*processInterruptsCallBack)(void);   /* The process interrupt call back. */
    bool isLoggerThread;                       /* Indicates whether the thread is a log thread. */
    int traceCategoryFlag;                     /* Error log trace category information. */
    int sendClientFlag;                        /* Whether to allow sending messages to clients. */
    /* Send the error message to the client frontend call back. */
    void (*sendToFrontendCallBack)(int level, int sqlErrorCode, char *message, char *cause, char *action);
    int connectionFilterCount;
    ErrLogConnection connectionFilter[MAX_ERROR_FILTER_COUNT]; /* Error log connection filter. */
    int sessionFilterCount;
    ErrLogSession sessionFilter[MAX_ERROR_FILTER_COUNT]; /* Error log session filter. */
    int runningContextFilterCount;
    ErrLogRunningContext runningContextFilter[MAX_ERROR_FILTER_COUNT]; /* Error log running context filter. */
    int positionContextFilterCount;
    ErrLogPositionContext positionContextFilter[MAX_ERROR_FILTER_COUNT]; /* Error log position context filter. */
    SyslogOutputConfig sysLogOutputConfig;                               /* Syslog output config. */
    MemoryContext errorContext;            /* Error log message memory context, save msg before
                                            * send to errlog thread message queue. */
    ErrLogModuleContext currentModule;     /* The current log module information. */
    ErrLogPositionContext currentPosition; /* The current log position information. */
    int recursionDepth;                    /* To detect actual recursion. */
    int errorDataStackDepth;               /* Index of topmost active frame for errorData. */
    ErrorData errorData[ERRORDATA_STACK_SIZE];
    LogIdentifier logIdentifier;
    bool isPipeEnlargeActive;
};

THR_LOCAL ThreadErrorLogContextData g_threadErrorLogContextData;

typedef struct {
    const char *(*getLogLinePrefix)(void);
    const char *(*getApplicationName)(void);
    long (*getMyStartTime)(void);
    const char *(*getDisp)(void);
    const char *(*getUserName)(void);
    const char *(*getDatabaseName)(void);
    const char *(*getRemoteHost)(void);
    const char *(*getRemotePort)(void);
    int (*getThreadId)(void);
    int (*getSessionId)(void);
    int (*getGlobalSessionIdNodeId)(void);
    int (*getGlobalSessionIdSessionId)(void);
    int (*getGlobalSessionIdSeq)(void);
    uint64 (*getDebugQueryId)(void);
    void (*updateLogLineNumber)(void);
    long (*getLogLineNumber)(void);
    int (*getLogicTid)(void);
    char *(*getFormattedStartTime)(void);
    int (*getBackendId)(void);
    uint64 (*getLxid)(void);
    uint64 (*getCurrentTopXid)(void);
    const char *(*getSqlState)(int);
    char *(*getPGXCNodeName)(void);
    char *(*getTraceId)(void);
    bool (*isProcPortEmpty)(void);
    bool (*isLogVerbose)(void);
} ErrorLogContextDataCallbacks;

static const char *GetLogLinePrefix(void)
{
    return "%m %u %d %h %p %S ";
}

static const char *GetApplicationName(void)
{
    return "[unknown]";
}

static long GetMyStartTime(void)
{
    return 0;
}

static const char *GetDisp(void)
{
    return "[unknown]";
}

static const char *GetUserName(void)
{
    return "[unknown]";
}

static const char *GetDatabaseName(void)
{
    return "[unknown]";
}

static const char *GetRemoteHost(void)
{
    return NULL;
}

static const char *GetRemotePort(void)
{
    return NULL;
}

static int GetThreadId(void)
{
    return 0;
}

static int GetSessionId(void)
{
    return -1;
}

static int GetGlobalSessionIdNodeId(void)
{
    return -1;
}

static int GetGlobalSessionIdSessionId(void)
{
    return -1;
}

static int GetGlobalSessionIdSeq(void)
{
    return -1;
}

static uint64 GetDebugQueryId(void)
{
    return (uint64)-1;
}

static void UpdateLogLineNumber(void)
{
    return;
}

static long GetLogLineNumber(void)
{
    return 0;
}

static int GetLogicTid(void)
{
    return 0;
}

static char *GetFormattedStartTime(void)
{
    return "";
}

static int GetBackendId(void)
{
    return -1;
}

static uint64 GetLxid(void)
{
    return (uint64)-1;
}

static uint64 GetCurrentTopXid(void)
{
    return 0;
}

static const char *GetSqlState(int sqlerrcode)
{
    (void)sqlerrcode;
    return "";
}

static char *GetPGXCNodeName(void)
{
    return "";
}

static char *GetTraceId(void)
{
    return "";
}

static bool IsProcPortEmpty(void)
{
    return true;
}

static bool IsLogVerbose(void)
{
    return false;
}

static ErrorLogContextDataCallbacks g_errorLogContextDataCallbacks = {
    .getLogLinePrefix = GetLogLinePrefix,
    .getApplicationName = GetApplicationName,
    .getMyStartTime = GetMyStartTime,
    .getDisp = GetDisp,
    .getUserName = GetUserName,
    .getDatabaseName = GetDatabaseName,
    .getRemoteHost = GetRemoteHost,
    .getRemotePort = GetRemotePort,
    .getThreadId = GetThreadId,
    .getSessionId = GetSessionId,
    .getGlobalSessionIdNodeId = GetGlobalSessionIdNodeId,
    .getGlobalSessionIdSessionId = GetGlobalSessionIdSessionId,
    .getGlobalSessionIdSeq = GetGlobalSessionIdSeq,
    .getDebugQueryId = GetDebugQueryId,
    .updateLogLineNumber = UpdateLogLineNumber,
    .getLogLineNumber = GetLogLineNumber,
    .getLogicTid = GetLogicTid,
    .getFormattedStartTime = GetFormattedStartTime,
    .getBackendId = GetBackendId,
    .getLxid = GetLxid,
    .getCurrentTopXid = GetCurrentTopXid,
    .getSqlState = GetSqlState,
    .getPGXCNodeName = GetPGXCNodeName,
    .getTraceId = GetTraceId,
    .isProcPortEmpty = IsProcPortEmpty,
    .isLogVerbose = IsLogVerbose};

UTILS_EXPORT void InitGetLogLinePrefixFunc(const char *(*getLogLinePrefix)(void))
{
    g_errorLogContextDataCallbacks.getLogLinePrefix = getLogLinePrefix;
}

UTILS_EXPORT void InitGetApplicationNameFunc(const char *(*getApplicationName)(void))
{
    g_errorLogContextDataCallbacks.getApplicationName = getApplicationName;
}

UTILS_EXPORT void InitGetMyStartTimeFunc(long (*getMyStartTime)(void))
{
    g_errorLogContextDataCallbacks.getMyStartTime = getMyStartTime;
}

UTILS_EXPORT void InitGetDispFunc(const char *(*getDisp)(void))
{
    g_errorLogContextDataCallbacks.getDisp = getDisp;
}

UTILS_EXPORT void InitGetUserNameFunc(const char *(*getUserName)(void))
{
    g_errorLogContextDataCallbacks.getUserName = getUserName;
}

UTILS_EXPORT void InitGetDatabaseNameFunc(const char *(*getDatabaseName)(void))
{
    g_errorLogContextDataCallbacks.getDatabaseName = getDatabaseName;
}

UTILS_EXPORT void InitGetRemoteHostFunc(const char *(*getRemoteHost)(void))
{
    g_errorLogContextDataCallbacks.getRemoteHost = getRemoteHost;
}

UTILS_EXPORT void InitGetRemotePortFunc(const char *(*getRemotePort)(void))
{
    g_errorLogContextDataCallbacks.getRemotePort = getRemotePort;
}

UTILS_EXPORT void InitGetThreadIdFunc(int (*getThreadId)(void))
{
    g_errorLogContextDataCallbacks.getThreadId = getThreadId;
}

UTILS_EXPORT void InitGetSessionIdFunc(int (*getSessionId)(void))
{
    g_errorLogContextDataCallbacks.getSessionId = getSessionId;
}

UTILS_EXPORT void InitGetGlobalSessionIdNodeIdFunc(int (*getGlobalSessionIdNodeId)(void))
{
    g_errorLogContextDataCallbacks.getGlobalSessionIdNodeId = getGlobalSessionIdNodeId;
}

UTILS_EXPORT void InitGetGlobalSessionIdSessionIdFunc(int (*getGlobalSessionIdSessionId)(void))
{
    g_errorLogContextDataCallbacks.getGlobalSessionIdSessionId = getGlobalSessionIdSessionId;
}

UTILS_EXPORT void InitGetGlobalSessionIdSeqFunc(int (*getGlobalSessionIdSeq)(void))
{
    g_errorLogContextDataCallbacks.getGlobalSessionIdSeq = getGlobalSessionIdSeq;
}

UTILS_EXPORT void InitGetDebugQueryIdFunc(uint64 (*getDebugQueryId)(void))
{
    g_errorLogContextDataCallbacks.getDebugQueryId = getDebugQueryId;
}

UTILS_EXPORT void InitUpdateLogLineNumberFunc(void (*updateLogLineNumber)(void))
{
    g_errorLogContextDataCallbacks.updateLogLineNumber = updateLogLineNumber;
}

UTILS_EXPORT void InitGetLogLineNumberFunc(long (*getLogLineNumber)(void))
{
    g_errorLogContextDataCallbacks.getLogLineNumber = getLogLineNumber;
}

UTILS_EXPORT void InitGetLogicTidFunc(int (*getLogicTid)(void))
{
    g_errorLogContextDataCallbacks.getLogicTid = getLogicTid;
}

UTILS_EXPORT void InitGetFormattedStartTimeFunc(char *(*getFormattedStartTime)(void))
{
    g_errorLogContextDataCallbacks.getFormattedStartTime = getFormattedStartTime;
}

UTILS_EXPORT void InitGetBackendIdFunc(int (*getBackendId)(void))
{
    g_errorLogContextDataCallbacks.getBackendId = getBackendId;
}

UTILS_EXPORT void InitGetLxidFunc(uint64 (*getLxid)(void))
{
    g_errorLogContextDataCallbacks.getLxid = getLxid;
}

UTILS_EXPORT void InitGetCurrentTopXidFunc(uint64 (*getCurrentTopXid)(void))
{
    g_errorLogContextDataCallbacks.getCurrentTopXid = getCurrentTopXid;
}

UTILS_EXPORT void InitGetSqlStateFunc(const char *(*getSqlState)(int))
{
    g_errorLogContextDataCallbacks.getSqlState = getSqlState;
}

UTILS_EXPORT void InitGetPGXCNodeNameFunc(char *(*getPGXCNodeName)(void))
{
    g_errorLogContextDataCallbacks.getPGXCNodeName = getPGXCNodeName;
}

UTILS_EXPORT void InitGetTraceIdFunc(char *(*getTraceId)(void))
{
    g_errorLogContextDataCallbacks.getTraceId = getTraceId;
}

UTILS_EXPORT void InitIsProcPortEmptyFunc(bool (*isProcPortEmpty)(void))
{
    g_errorLogContextDataCallbacks.isProcPortEmpty = isProcPortEmpty;
}

UTILS_EXPORT void InitIsLogVerboseFunc(bool (*isLogVerbose)(void))
{
    g_errorLogContextDataCallbacks.isLogVerbose = isLogVerbose;
}

typedef struct ThreadSyslogContext ThreadSyslogContext;
struct ThreadSyslogContext {
    bool isReady;
    char *syslogIdent;
    Syslog syslogContext;
    uint64_t syslogSeq;
};

THR_LOCAL ThreadSyslogContext g_threadSyslogContext = {
    .isReady = false,
    .syslogIdent = NULL,
    .syslogContext = SYSLOG_INITIALIZER,
    .syslogSeq = 0,
};

void ResetThreadSyslogSeq(void)
{
    g_threadSyslogContext.syslogSeq = 0;
}

static void OpenThreadSyslog(void)
{
    if (g_threadSyslogContext.isReady) {
        return;
    }
    char processName[MAX_PATH] = {0};
    ErrorCode errorCode = GetCurrentProcessName(processName, MAX_PATH);
    if (errorCode == ERROR_SYS_OK) {
        g_threadSyslogContext.syslogIdent = MemoryContextStrdup(g_threadErrorLogContextData.errorContext, processName);
    } else {
        g_threadSyslogContext.syslogIdent = MemoryContextStrdup(g_threadErrorLogContextData.errorContext, "Unknown");
    }
#define THREAD_SYSLOG_FACILITY "LOCAL0"
    OpenSyslog(g_threadSyslogContext.syslogIdent, THREAD_SYSLOG_FACILITY, &(g_threadSyslogContext.syslogContext));
    g_threadSyslogContext.isReady = true;
}

static void CloseThreadSyslog(void)
{
    if (g_threadSyslogContext.isReady) {
        if (g_threadSyslogContext.syslogIdent != NULL) {
            MemFreeAndSetNull((void **)&g_threadSyslogContext.syslogIdent);
        }
        CloseSyslog(&(g_threadSyslogContext.syslogContext));
        g_threadSyslogContext.isReady = false;
    }
}

/* Checking errorDataStackDepth is reasonable. */
static void CheckErrorLogStackDepth(void)
{
    if (g_threadErrorLogContextData.errorDataStackDepth < 0) {
        g_threadErrorLogContextData.errorDataStackDepth = -1;
        ErrLog(ERROR, ErrMsg("ErrStart was not called"));
    }
}

/*
 * IsLogConnectionOutput -- is error data connection equals filter connection ?
 */
static bool IsLogConnectionOutput(ErrLogConnection *errData, ErrLogConnection *filter, int count)
{
    int i;
    if (count == 0) {
        return true;
    }
    for (i = 0; i < count; i++) {
        if (strcmp(filter[i].srcIP, LOG_STRING_WILDCARD) != 0 && strcmp(filter[i].srcIP, errData->srcIP) != 0) {
            continue;
        }
        if ((filter[i].srcPort != LOG_INT_WILDCARD) && (filter[i].srcPort != errData->srcPort)) {
            continue;
        }
        if (strcmp(filter[i].destIP, LOG_STRING_WILDCARD) != 0 && strcmp(filter[i].destIP, errData->destIP) != 0) {
            continue;
        }
        if ((filter[i].destPort != LOG_INT_WILDCARD) && (filter[i].destPort != errData->destPort)) {
            continue;
        }
        return true;
    }
    return false;
}

/*
 * IsLogConnectionOutput -- is error data session equals filter session ?
 */
static bool IsLogSessionOutput(ErrLogSession *errData, ErrLogSession *filter, int count)
{
    int i;
    if (count == 0) {
        return true;
    }
    for (i = 0; i < count; i++) {
        if (strcmp(filter[i].applicationName, LOG_STRING_WILDCARD) != 0 &&
            strcmp(filter[i].applicationName, errData->applicationName) != 0) {
            continue;
        }
        if (strcmp(filter[i].databaseName, LOG_STRING_WILDCARD) != 0 &&
            strcmp(filter[i].databaseName, errData->databaseName) != 0) {
            continue;
        }
        if (strcmp(filter[i].userName, LOG_STRING_WILDCARD) != 0 &&
            strcmp(filter[i].userName, errData->userName) != 0) {
            continue;
        }
        if ((filter[i].sessionID != LOG_INT_WILDCARD) && (filter[i].sessionID != errData->sessionID)) {
            continue;
        }
        return true;
    }
    return false;
}

/*
 * IsLogRunningContextOutput -- is error data RunningContext equals filter RunningContext ?
 */
static bool IsLogRunningContextOutput(ErrLogRunningContext *errData, ErrLogRunningContext *filter, int count)
{
    int i;
    if (count == 0) {
        return true;
    }
    Pid pid;
    Tid tid;
    SetPid(&pid, LOG_INT_WILDCARD);
    SetTid(&tid, LOG_INT_WILDCARD);
    for (i = 0; i < count; i++) {
        if (!PidIsEqual(&(filter[i].pid), &pid) && !PidIsEqual(&(filter[i].pid), &(errData->pid))) {
            continue;
        }
        if (!TidIsEqual(&(filter[i].tid), &tid) && !TidIsEqual(&(filter[i].tid), &(errData->tid))) {
            continue;
        }
        if (strcmp(filter[i].threadName, LOG_STRING_WILDCARD) != 0 &&
            strcmp(filter[i].threadName, errData->threadName) != 0) {
            continue;
        }
        return true;
    }
    return false;
}

/*
 * IsLogPositionContextOutput -- is current position ErrLogPositionContext equals
 * filter ErrLogPositionContext ?
 */
static bool IsLogPositionContextOutput(ErrLogPositionContext *currentPosition, ErrLogPositionContext *filter, int count)
{
    int i;
    if (count == 0) {
        return true;
    }
    for (i = 0; i < count; i++) {
        if (strcmp(filter[i].fileName, LOG_STRING_WILDCARD) != 0 &&
            strcmp(filter[i].fileName, currentPosition->fileName) != 0) {
            continue;
        }
        if ((filter[i].lineNo != LOG_INT_WILDCARD) && (filter[i].lineNo != currentPosition->lineNo)) {
            continue;
        }
        if (strcmp(filter[i].funcName, LOG_STRING_WILDCARD) != 0 &&
            strcmp(filter[i].funcName, currentPosition->funcName) != 0) {
            continue;
        }
        return true;
    }
    return false;
}

/*
 * ShouldOutputToServer --- should message of given elevel go to the log ?
 */
static bool ShouldOutputToServer(int elevel)
{
    ThreadErrorLogContextData *context = &g_threadErrorLogContextData;
    if (!IS_LOG_LEVEL_OUTPUT(elevel, GET_ERR_LOG_SERVER_LEVEL)) {
        return false;
    }
    bool output =
        IsLogConnectionOutput(&(context->connectionInfo), context->connectionFilter, context->connectionFilterCount);
    if (!output) {
        return false;
    }
    output = IsLogSessionOutput(&(context->sessionInfo), context->sessionFilter, context->sessionFilterCount);
    if (!output) {
        return false;
    }
    output = IsLogRunningContextOutput(&(context->runningContextInfo), context->runningContextFilter,
                                       context->runningContextFilterCount);
    if (!output) {
        return false;
    }
    return true;
}

/*
 * Copy module and position context.
 * Here we use shallow copies, pointers are assigned directly, because they are always valid in the context in
 * which they are used.
 */
static void ErrLogDataContextCopy(ErrLogModuleContext *moduleContext, ErrLogPositionContext *positionContext)
{
    moduleContext->componentName = g_threadErrorLogContextData.currentModule.componentName;
    moduleContext->moduleName = g_threadErrorLogContextData.currentModule.moduleName;
    positionContext->fileName = g_threadErrorLogContextData.currentPosition.fileName;
    positionContext->lineNo = g_threadErrorLogContextData.currentPosition.lineNo;
    positionContext->funcName = g_threadErrorLogContextData.currentPosition.funcName;
}

/*
 * ShouldOutputToClient --- should message of given elevel go to the client ?
 */
bool ShouldOutputToClient(int elevel)
{
    if (g_threadErrorLogContextData.sendClientFlag == DISABLE_ERROR_LOG_SEND_CLIENT_ALL) {
        return false;
    } else if (g_threadErrorLogContextData.sendClientFlag == ENABLE_ERROR_LOG_SEND_CLIENT_ALL) {
        return (elevel >= GET_ERR_LOG_SERVER_LEVEL || elevel == INFO);
    } else if (g_threadErrorLogContextData.sendClientFlag == ENABLE_ERROR_LOG_SEND_CLIENT_ERROR) {
        return (elevel >= ERROR);
    } else {
        return false;
    }
}

/*
 * Set send message to client flag.
 */
void SetSendToClientFlag(int flag)
{
    g_threadErrorLogContextData.sendClientFlag = flag;
}

#define ERROR_LOG_STDERR_BUFFER_SIZE 2048

/**
 * Write errors to stderr. Used before ErrLog can be used safely (memory context, GUC load etc)
 */
void WriteStderr(const char *fmt, ...)
{
    va_list args;
    char errBuffer[ERROR_LOG_STDERR_BUFFER_SIZE];
    ssize_t retLen;
    va_start(args, fmt);
    retLen = vsprintf_s(errBuffer, ERROR_LOG_STDERR_BUFFER_SIZE, fmt, args);
    if ((retLen > 0) && (retLen < ERROR_LOG_STDERR_BUFFER_SIZE)) {
        (void)fwrite(errBuffer, 1, (size_t)retLen, stderr);
        (void)fflush(stderr);
    }
    va_end(args);
}

void WriteErrMsg(const char *fmt, ...)
{
    va_list args;
    char errBuffer[ERROR_LOG_STDERR_BUFFER_SIZE];
    ssize_t retLen;
    va_start(args, fmt);
    retLen = vsprintf_s(errBuffer, ERROR_LOG_STDERR_BUFFER_SIZE, fmt, args);
    if ((retLen > 0) && (retLen < ERROR_LOG_STDERR_BUFFER_SIZE)) {
        if (!IsLogThreadErrSyslogWrite()) {
            (void)fwrite(errBuffer, 1, (size_t)retLen, stderr);
            (void)fflush(stderr);
        } else {
            FAULT_INJECTION_CALL_REPLACE(MOCK_WRITE_SYSLOG, errBuffer)
            WriteSyslog(EVENT_LOG_ERROR, errBuffer, (size_t)retLen);
            FAULT_INJECTION_CALL_REPLACE_END;
        }
    }
    va_end(args);
}

#define SYSLOG_MESSAGE_LEN_LIMIT 900

static void SyslogSplitMessagesWordBoundary(const char *lineStr, char *buf, SYMBOL_UNUSED size_t maxLen, size_t *msgLen)
{
    ASSERT(msgLen != NULL);
    ASSERT(*msgLen >= 1);
    ASSERT(*msgLen <= maxLen);
    /* already word boundary? */
    if (lineStr[*msgLen] != '\0' && !isspace((unsigned char)lineStr[*msgLen])) {
        /* try to divide at word boundary */
        size_t i = *msgLen - 1;
        while (i > 0 && !isspace((unsigned char)buf[i])) {
            i--;
        }

        /* else couldn't divide word boundary */
        if (i > 0) {
            *msgLen = i;
            buf[i] = '\0';
        }
    }
}

/*
 * Write a message line to syslog.
 */
void WriteSyslog(int syslogLevel, const char *line, size_t count)
{
    /* Open syslog connection if not done yet */
    OpenThreadSyslog();
    Syslog *syslog = &(g_threadSyslogContext.syslogContext);

    /*
     * Our problem here is that many syslog implementations don't handle long
     * messages in an acceptable manner. While this function doesn't help that
     * fact, it does work around by splitting up messages into smaller pieces.
     * We divide into multiple syslog() calls if message is too long or if the
     * message contains embedded newline(s).
     */
    size_t len = count;
    const char *newlinePos = NULL;
    const char *lineStr = line;
    g_threadSyslogContext.syslogSeq++;

    newlinePos = strchr(lineStr, '\n');
    if (len > SYSLOG_MESSAGE_LEN_LIMIT || newlinePos != NULL) {
        int chunkNo = 0;
        while (len > 0) {
            char buf[SYSLOG_MESSAGE_LEN_LIMIT + 1];
            size_t bufLen;
            /* If we start at a newline, move ahead one char. */
            if (lineStr[0] == '\n') {
                lineStr++;
                len--;
                /* We need to recompute the next newline's position, too. */
                newlinePos = strchr(lineStr, '\n');
                continue;
            }
            /* Copy one line, or as much as will fit, to buf. */
            if (newlinePos != NULL) {
                bufLen = (size_t)(newlinePos - lineStr);
            } else {
                bufLen = len;
            }
            bufLen = Min(bufLen, SYSLOG_MESSAGE_LEN_LIMIT);
            errno_t ret = memcpy_s(buf, sizeof(buf), lineStr, bufLen);
            if (ret != EOK) {
                return;
            }
            buf[bufLen] = '\0';

            SyslogSplitMessagesWordBoundary(lineStr, buf, sizeof(buf), &bufLen);

            chunkNo++;
            /* Using injection to write format buffer to stderr for unittest */
            FAULT_INJECTION_ACTION(COPY_SYSLOG_OUTPUT_TO_STDERR,
                                   WriteStderr("[%lu-%d] %s\n", g_threadSyslogContext.syslogSeq, chunkNo, buf));
            ReportSyslog(syslog, syslogLevel, "[%lu-%d] %s", g_threadSyslogContext.syslogSeq, chunkNo, buf);
            lineStr += bufLen;
            len -= bufLen;
        }
    } else {
        /* Message short enough. */
        ReportSyslog(syslog, syslogLevel, "[%lu] %s", g_threadSyslogContext.syslogSeq, lineStr);
    }
}

/*
 * InErrorRecursionTrouble --- are we at risk of infinite error recursion?
 * This function exists to provide common control of various fallback steps
 * that we take if we think we are facing infinite error recursion.  See the
 * callers for details.
 */
static bool InErrorRecursionTrouble(void)
{
#define ERROR_LOG_RECURSION_MAX_DEPTH 2
    /* Pull the plug if recurse more than once. */
    return (g_threadErrorLogContextData.recursionDepth > ERROR_LOG_RECURSION_MAX_DEPTH);
}

/* Support for attaching context information to error reports. */
typedef struct ErrorLogContextCallback ErrorLogContextCallback;

struct ErrorLogContextCallback {
    SListNode node;

    void (*callback)(void *);

    void *arg;
};

/**
 * Initialize the linked list header of the callback function.
 */
static void InitErrorLogContextCallback(void)
{
    SListInit(&(g_threadErrorLogContextData.errorLogContextCallbackHead));
}

/**
 * Push error log callback function.
 * @param contextCallback : ErrorLogContextCallback
 */
UTILS_EXPORT void PushErrorLogContextCallback(void (*callback)(void *), void *arg)
{
    MemoryContext errorContext = g_threadErrorLogContextData.errorContext;
    ErrorLogContextCallback *callBack =
        (ErrorLogContextCallback *)MemoryContextAlloc(errorContext, sizeof(ErrorLogContextCallback));
    if (unlikely(callBack == NULL)) {
        WriteErrMsg("MemoryContextAlloc failed in PushErrorLogContextCallback!\n");
        return;
    }
    callBack->callback = callback;
    callBack->arg = arg;
    SListPushHead(&(g_threadErrorLogContextData.errorLogContextCallbackHead), &(callBack->node));
}

/**
 * Pop error log callback function.
 */
UTILS_EXPORT void PopErrorLogContextCallback(void)
{
    SListNode *node = SListPopHeadNode(&(g_threadErrorLogContextData.errorLogContextCallbackHead));
    MemFree((void *)node);
}

/*
 * Register fatal error thread exit callback.
 * @param threadExitCallback: Thread exit callback.
 * @param valuePtr: The thread exit value.
 */
UTILS_EXPORT void RegisterErrLogThreadExitCallBack(int (*threadExitCallback)(void *valuePtr), void *valuePtr)
{
    g_threadErrorLogContextData.threadExitCallback = threadExitCallback;
    g_threadErrorLogContextData.valuePtr = valuePtr;
}

/**
 * Register the callback function for sending error log messages to the client.
 * @param sendToFrontendCallBack: Send to frontend callback.
 */
UTILS_EXPORT void RegisterErrLogSendToFrontendCallBack(void (*sendToFrontendCallBack)(int level, int sqlErrorCode,
                                                                                      char *message, char *cause,
                                                                                      char *action))
{
    g_threadErrorLogContextData.sendToFrontendCallBack = sendToFrontendCallBack;
}

/*
 * Register process interrupts callback.
 * @param processInterruptsCallBack: Process interrupts callback.
 */
UTILS_EXPORT void RegisterErrLogProcessInterruptsCallBack(void (*processInterruptsCallBack)(void))
{
    g_threadErrorLogContextData.processInterruptsCallBack = processInterruptsCallBack;
}

/**
 * Pre-process error log information.
 * @param componentName: Component name.
 * @param moduleName: Module name.
 * @param componentId: Component id.
 * @param moduleId: Module id.
 * @param errorLevel: error level.
 * @return
 */
UTILS_EXPORT bool ErrStartModule(const char *componentName, const char *moduleName, int componentId, int moduleId,
                                 int errorLevel)
{
    bool output = false;
    ThreadErrorLogContextData *context = &g_threadErrorLogContextData;
    context->currentModule.componentName = (char *)(uintptr_t)componentName;
    context->currentModule.moduleName = (char *)(uintptr_t)moduleName;
    context->currentModule.componentId = componentId;
    context->currentModule.moduleId = moduleId;
    context->currentModule.errorLevel = errorLevel;
    output = IsLogModuleContextOutput(&context->currentModule);
    return output;
}

/**
 * Pre-process error log information.
 * @param fileName : Source file name of log position.
 * @param lineNo : Line number of log position.
 * @param funcName : Function name of log position.
 * @return
 */
UTILS_EXPORT bool ErrStartPosition(const char *fileName, int lineNo, const char *funcName)
{
    bool output = false;
    ThreadErrorLogContextData *context = &g_threadErrorLogContextData;
    char *fullPathFileName = (char *)(uintptr_t)fileName;
    context->currentPosition.fileName = Basename(fullPathFileName);
    context->currentPosition.lineNo = lineNo;
    context->currentPosition.funcName = (char *)(uintptr_t)funcName;
    output = IsLogPositionContextOutput(&(context->currentPosition), context->positionContextFilter,
                                        context->positionContextFilterCount);
    /* Record full path and line number in fold info */
    context->logIdentifier.lineNum = lineNo;
    (void)strncpy_s(context->logIdentifier.fileName, sizeof(context->logIdentifier.fileName), fileName,
                    sizeof(context->logIdentifier.fileName) - 1);
    return output;
}

UTILS_EXPORT void SetLogPipeEnlarge(bool isPipeEnlargeActive)
{
    g_threadErrorLogContextData.isPipeEnlargeActive = isPipeEnlargeActive;
}

bool isPipeEnlargeActive(void)
{
    return g_threadErrorLogContextData.isPipeEnlargeActive;
}

#define ERRCODE_SUCCESSFUL_COMPLETION 0
#define ERRCODE_WARNING               1
#define ERRCODE_INTERNAL_ERROR        2
/* Set the error data sql error code. */
static void SetErrorDataSqlErrorCode(int errorLevel, ErrorData *errorData)
{
    if (errorLevel >= ERROR) {
        errorData->sqlErrorCode = ERRCODE_INTERNAL_ERROR;
    } else if (errorLevel >= WARNING) {
        errorData->sqlErrorCode = ERRCODE_WARNING;
    } else {
        errorData->sqlErrorCode = ERRCODE_SUCCESSFUL_COMPLETION;
    }
}

UTILS_EXPORT bool IsErrLevelPassed(int errorLevel)
{
    if (!IS_ERR_LOG_OPEN()) {
        (void)OpenErrorLog();
    }
    /* Indicates whether the thread has enabled the log function. */
    if (IS_ERR_LOG_OPEN() &&
        (IsLogLevelOutput(errorLevel, GetErrLogServerLevel()) || ShouldOutputToClient(errorLevel))) {
        return true;
    }
    return false;
}

/**
 * Pre-process error log information.
 * @param errorLevel: Error log level.
 * @param positionResult : Log position output result.
 * @param moduleResult : Log module output result.
 * @return:true or false.
 */
UTILS_EXPORT bool ErrStart(int errorLevel, bool positionResult, bool moduleResult)
{
    /* Indicates whether the thread has enabled the log function. */
    if (!IS_ERR_LOG_OPEN()) {
        ErrorCode errorCode = OpenErrorLog();
        if (errorCode != ERROR_SYS_OK) {
            WriteErrMsg("Open error log failed,the logger thread not started !\n");
            return false;
        }
    }

    ErrorData *errorData = NULL;
    bool outputToServer = false;
    bool outputToClient = false;
    int i;
    int maxErrorLevel = errorLevel;
    if (maxErrorLevel >= FATAL) {
        /*
         * If the error level is FATAL or more, ErrFinish is not going to return to caller; therefore, if there is any
         * stacked error already in progress it will be lost.  This is more or less okay, except we do not want to have
         * a PANIC error downgraded because the reporting process was interrupted by a lower-grade error.  So check the
         * stack and make sure we panic if panic is warranted.
         */
        for (i = 0; i <= g_threadErrorLogContextData.errorDataStackDepth; i++) {
            maxErrorLevel = Max(maxErrorLevel, g_threadErrorLogContextData.errorData[i].errorLevel);
        }
    }
    /*
     * Now decide whether we need to process this error log at all; if it's warning or less and not enabled for logging,
     * just return false without starting up any error logging machinery.
     * */
    outputToServer = ShouldOutputToServer(maxErrorLevel) && positionResult && moduleResult;

    outputToClient = ShouldOutputToClient(maxErrorLevel);
    if (maxErrorLevel < ERROR && !outputToServer && !outputToClient) {
        return false;
    }

    /*
     * Okay, crank up a stack entry to store the info in.
     */
    if (g_threadErrorLogContextData.recursionDepth++ > 0 && maxErrorLevel >= FATAL) {
        /*
         * Oops, error during error processing.  Clear ErrorContext as discussed at top of file.  We will not return
         * to the original error's reporter or handler, so we don't need it.
         */
        MemoryContextReset(g_threadErrorLogContextData.errorContext);

        /*
         * Infinite error recursion might be due to something broken in a context traceback routine.Abandon them too.
         * We also abandon attempting to print the error statement (which, if long, could itself be the source of the
         * recursive failure).
         */
        if (InErrorRecursionTrouble()) {
            InitErrorLogContextCallback();
            g_threadErrorLogContextData.queryString = NULL;
        }
    }
    if (++g_threadErrorLogContextData.errorDataStackDepth >= ERRORDATA_STACK_SIZE) {
        /*
         * Wups, stack not big enough. We treat this as a PANIC condition  because it suggests an infinite
         * loop of errors during error recovery.
         */
        /* Make room on stack. */
        g_threadErrorLogContextData.errorDataStackDepth = -1;
        ErrLog(FATAL, ErrMsg("ERRORDATA_STACK_SIZE exceeded"));
    }
    /* Initialize data for this error frame. */
    errorData = &(g_threadErrorLogContextData.errorData[g_threadErrorLogContextData.errorDataStackDepth]);
    errno_t rc = memset_s(errorData, sizeof(ErrorData), 0, sizeof(ErrorData));
    SecurecCheck(rc, false);
    errorData->errorLevel = maxErrorLevel;
    errorData->outputToServer = outputToServer;
    errorData->outputToClient = outputToClient;
    ErrLogDataContextCopy(&(errorData->moduleContext), &(errorData->positionContext));
    /* The default text domain is the backend's. */
    errorData->domain = FindModuleTextDomain(g_threadErrorLogContextData.currentModule.componentName,
                                             g_threadErrorLogContextData.currentModule.moduleName);
    /* Select default errcode based on elevel */
    SetErrorDataSqlErrorCode(maxErrorLevel, errorData);
    g_threadErrorLogContextData.recursionDepth--;
    return true;
}

#define INTERNAL_BACKTRACE_SUPPORT_FUNC_FRAME 2
#define MAX_BACKTRACE_FRAMES                  100

/*
 * Compute backtrace data and add it to the supplied ErrorData.  numSkip
 * specifies how many inner frames to skip.  Use this to avoid showing the
 * internal backtrace support functions in the backtrace.  This requires that
 * this and related functions are not inlined.
 */
static void SetBacktrace(ErrorData *errorData, uint32_t numSkip)
{
    void *buf[MAX_BACKTRACE_FRAMES];
    int frames;
    char *strFrame = NULL;
    BacktraceContext traceContext;
    frames = Backtrace(buf, MAX_BACKTRACE_FRAMES, numSkip, &traceContext);
    int i;
    StringInfoData errTrace;
    bool ret = InitString(g_threadErrorLogContextData.errorContext, &errTrace);
    /* If string initial failed, return directly to avoid accessing null pointer */
    if (unlikely(!ret)) {
        return;
    }

    for (i = 0; i < frames; i++) {
        strFrame = BacktraceSymbols(buf, frames, (unsigned)i, &traceContext);
        if (strFrame != NULL) {
            (void)AppendString(&errTrace, "\n%d:%s", i, strFrame);
        }
    }
    FreeBacktraceSymbols(&traceContext);
    TransferString(&(errorData->backtrace), &errTrace);
}

/**
 * Set the ErrorData backtrace.
 * @return
 */
int ErrBacktrace(void)
{
    ErrorData *errorData = &(g_threadErrorLogContextData.errorData[g_threadErrorLogContextData.errorDataStackDepth]);
    g_threadErrorLogContextData.recursionDepth++;
    CheckErrorLogStackDepth();
    SetBacktrace(errorData, INTERNAL_BACKTRACE_SUPPORT_FUNC_FRAME);
    g_threadErrorLogContextData.recursionDepth--;
    return 0;
}

/**
 * Get the current error log time.
 * @param formattedTime :The current formatted time.
 * @param maxSize :The formattedTime size.
 */
static void FormatCurrentLogTime(char *formattedTime, size_t maxSize, TimesSecondsSinceEpoch *curTime)
{
    ASSERT(curTime != NULL);
    TimeFormatStructure formatTime;
    LocalTime(curTime, &formatTime);
    (void)FormatTime(formattedTime, maxSize, "%Y-%m-%d %H:%M:%S", &formatTime);
}

/**
 * Get the current error log time.
 * @param formattedTime :The current formatted time.
 * @param maxSize :The formattedTime size.
 */
static void FormatCurrentLogHighPrecisionTime(char *formattedTime, size_t maxSize, TimeValue *curTime)
{
    ASSERT(curTime != NULL);
    TimeFormatStructure formatTime;
    LocalHighPrecisionTime(curTime, &formatTime);
    (void)FormatTime(formattedTime, maxSize, "%Y-%m-%d %H:%M:%S", &formatTime);

/* 1000 microseconds equals 1 millisecond */
#define ONE_MILLISECOND      1000
#define MILLI_SECOND_MAX_LEN 8
    char milliSecondStr[MILLI_SECOND_MAX_LEN];

    if (sprintf_s(milliSecondStr, sizeof(milliSecondStr), ".%03ld", curTime->useconds / ONE_MILLISECOND) < 0) {
        WriteErrMsg("Failed to format time string in error log\n");
        return;
    }
    if (strcat_s(formattedTime, maxSize, milliSecondStr) != EOK) {
        WriteErrMsg("Failed to concatenate string in error log\n");
        return;
    }
}

static void SetupFormattedLogTime(char *formattedTime, size_t maxSize)
{
    struct timeval tv;
    time_t stampTime;
    char msbuf[8];
    errno_t rc = EOK;
    struct tm* localTime = NULL;
    gettimeofday(&tv, NULL);
    stampTime = (time_t)tv.tv_sec;

    log_timezone = GetErrLogLocalTime();
    if (log_timezone == NULL) {
       WriteErrMsg("log_timezone must not be null!");
        return;
    }
    
    localTime = GetLocaltime(&stampTime, log_timezone);
    if (localTime == NULL) {
        WriteErrMsg("localTime must not be null!");
        return;
    }
    (void)strftime(formattedTime, maxSize,
        /* leave room for milliseconds... */
        "%Y-%m-%d %H:%M:%S     %Z", localTime);

    /* 'paste' milliseconds into place... */
    rc = sprintf_s(msbuf, sizeof(msbuf), ".%03d", (int)(tv.tv_usec / ONE_MILLISECOND));
    if (unlikely(rc < 0)) {
        WriteErrMsg("Format msbuf string failed.");
        return;
    }
#define FORMART_TIME_LEN 19
#define USEC_LEN 4
    rc = strncpy_s(formattedTime + FORMART_TIME_LEN, maxSize - FORMART_TIME_LEN, msbuf, USEC_LEN);
    if (unlikely(rc < 0)) {
        WriteErrMsg("Format formatted_log_time string failed.");
        return;
    }
}

static void AppendTraceInfo(StringInfo errBuffer)
{
    TraceId id = TraceChainGetId();
    if (TraceChainIsTraceIdValid(&id)) {
        (void)AppendString(errBuffer, "  TraceId:0x%lx ", id.chainId);
    }
}

static void LogLinePrefix(StringInfo errBuffer, ErrorData *errData, LogIdentifier *logMetaInfo)
{
    const char *logLinePrefix = g_errorLogContextDataCallbacks.getLogLinePrefix();
    if (logLinePrefix == NULL) {
        return;
    }
    g_errorLogContextDataCallbacks.updateLogLineNumber();

    size_t formatLen = strlen(logLinePrefix);
    for (size_t i = 0; i < formatLen; i++) {
        if (logLinePrefix[i] != '%') {
            /* literal char, just copy */
            (void)AppendStringChar(errBuffer, logLinePrefix[i]);
            continue;
        }
        /* go to char after '%' */
        i++;
        if (i >= formatLen) {
            break; /* format error - ignore it */
        }

        /* process the option */
        switch (logLinePrefix[i]) {
            case 'a': {
                const char *appName = g_errorLogContextDataCallbacks.getApplicationName();
                (void)AppendString(errBuffer, "%s", appName);
                break;
            }
            case 'u': {
                const char *userName = g_errorLogContextDataCallbacks.getUserName();
                (void)AppendString(errBuffer, "%s", userName);
                break;
            }
            case 'd': {
                const char *dbName = g_errorLogContextDataCallbacks.getDatabaseName();
                (void)AppendString(errBuffer, "%s", dbName);
                break;
            }
            case 'c':
                (void)AppendString(errBuffer, "%lx.%d", g_errorLogContextDataCallbacks.getMyStartTime(),
                                   g_errorLogContextDataCallbacks.getLogicTid());
                break;
            case 'p':
                (void)AppendString(errBuffer, "%lu", pthread_self());
                break;
            case 'l':
                (void)AppendString(errBuffer, "%ld", g_errorLogContextDataCallbacks.getLogLineNumber());
                break;
            case 'm': {
                /* Error log time, high precision will print microsecond time format */
                char formattedTime[FORMATTED_TIME_MAX_LEN] = {0};
                /* Check the usage scenario */
                if (!IsErrLogNeedRedirect()) {
                    TimeValue curTime = GetCurrentTimeValue();
                    logMetaInfo->timeSecond = curTime.seconds;
                    FormatCurrentLogHighPrecisionTime(formattedTime, FORMATTED_TIME_MAX_LEN, &curTime);
                } else {
                    SetupFormattedLogTime(formattedTime, FORMATTED_TIME_MAX_LEN);
                }
                (void)AppendString(errBuffer, "%s", formattedTime);
                break;
            }
            case 't': {
                TimesSecondsSinceEpoch curTime;
                Time(&curTime);
                logMetaInfo->timeSecond = curTime.timeSeconds;
                char formattedTime[FORMATTED_TIME_MAX_LEN] = {0};
                FormatCurrentLogTime(formattedTime, FORMATTED_TIME_MAX_LEN, &curTime);
                (void)AppendString(errBuffer, "%s ", formattedTime);
                break;
            }
            case 's':
                (void)AppendString(errBuffer, "%s", g_errorLogContextDataCallbacks.getFormattedStartTime());
                break;
            case 'i': {
                const char *disp = g_errorLogContextDataCallbacks.getDisp();
                (void)AppendString(errBuffer, "%s", disp);
                break;
            }
            case 'r': {
                const char *remoteHost = g_errorLogContextDataCallbacks.getRemoteHost();
                if (remoteHost == NULL) {
                    (void)AppendString(errBuffer, "localhost");
                    break;
                }

                (void)AppendString(errBuffer, "%s", remoteHost);
                const char *remotePort = g_errorLogContextDataCallbacks.getRemotePort();
                if (remotePort != NULL) {
                    (void)AppendString(errBuffer, "(%s)", remotePort);
                }
                break;
            }
            case 'h': {
                const char *remoteHost = g_errorLogContextDataCallbacks.getRemoteHost();
                if (remoteHost == NULL) {
                    (void)AppendString(errBuffer, "localhost");
                } else {
                    (void)AppendString(errBuffer, "%s", remoteHost);
                }
                break;
            }
            case 'q':
                if (g_errorLogContextDataCallbacks.isProcPortEmpty()) {
                    i = formatLen;
                }
                break;
            case 'v':
                if (g_errorLogContextDataCallbacks.getBackendId() != -1) {
                    (void)AppendString(errBuffer, "%d/%lu", g_errorLogContextDataCallbacks.getBackendId(),
                                       g_errorLogContextDataCallbacks.getLxid());
                } else {
                    (void)AppendString(errBuffer, "0/0");
                }
                break;
            case 'x':
                (void)AppendString(errBuffer, "%lu", g_errorLogContextDataCallbacks.getCurrentTopXid());
                break;
            case 'e':
                (void)AppendString(errBuffer, "%s", g_errorLogContextDataCallbacks.getSqlState(errData->sqlErrorCode));
                break;
            case 'n':
                (void)AppendString(errBuffer, "%s", g_errorLogContextDataCallbacks.getPGXCNodeName());
                break;
            case 'S':
                (void)AppendString(errBuffer, "%d[%d:%d#%d]", g_errorLogContextDataCallbacks.getSessionId(),
                                   (int)g_errorLogContextDataCallbacks.getGlobalSessionIdNodeId(),
                                   g_errorLogContextDataCallbacks.getGlobalSessionIdSessionId(),
                                   g_errorLogContextDataCallbacks.getGlobalSessionIdSeq());
                break;
            case 'T':
                (void)AppendString(errBuffer, "%s", g_errorLogContextDataCallbacks.getTraceId());
                break;
            case '%':
                (void)AppendStringChar(errBuffer, '%');
                break;
            default:
                /* format error - ignore it */
                break;
        }
    }

    uint64 debugQueryId = g_errorLogContextDataCallbacks.getDebugQueryId();
    if (debugQueryId != (uint64)-1) {
        (void)AppendString(errBuffer, " %lu", debugQueryId);
    }

    /* module name information */
    (void)AppendString(errBuffer, " [%s] ", g_threadErrorLogContextData.currentModule.moduleName);
}

static void AppendWithTabs(StringInfo errBuffer, const char *p)
{
    char ch = 0;
    while ((ch = *p++) != '\0') {
        (void)AppendStringChar(errBuffer, ch);
        if (ch == '\n') {
            (void)AppendStringChar(errBuffer, '\t');
        }
    }
}

/*
 * Format the error log message.
 */
static void FormatErrorLogMessage(ErrorData *errData, StringInfo errBuffer, LogIdentifier *logMetaInfo)
{
    LogLinePrefix(errBuffer, errData, logMetaInfo);
    logMetaInfo->logLevel = errData->errorLevel;
    (void)AppendString(errBuffer, "%s:  ", ErrorLevel2String(errData->errorLevel));

    AppendTraceInfo(errBuffer);

    if (errData->message != NULL) {
        AppendWithTabs(errBuffer, errData->message);
    } else {
        (void)AppendString(errBuffer, "missing error text");
    }

    (void)AppendStringChar(errBuffer, '\n');

    if (errData->cause != NULL) {
        LogLinePrefix(errBuffer, errData, logMetaInfo);
        (void)AppendString(errBuffer, "CAUSE:  ");
        AppendWithTabs(errBuffer, errData->cause);
        (void)AppendStringChar(errBuffer, '\n');
    }
    if (errData->action != NULL) {
        LogLinePrefix(errBuffer, errData, logMetaInfo);
        (void)AppendString(errBuffer, "ACTION:  ");
        AppendWithTabs(errBuffer, errData->action);
        (void)AppendStringChar(errBuffer, '\n');
    }
    if (errData->context != NULL) {
        LogLinePrefix(errBuffer, errData, logMetaInfo);
        (void)AppendString(errBuffer, "CONTEXT:  ");
        AppendWithTabs(errBuffer, errData->context);
        (void)AppendStringChar(errBuffer, '\n');
    }

    if (g_errorLogContextDataCallbacks.isLogVerbose()) {
        /* assume no newlines in funcname or filename... */
        if (errData->positionContext.funcName && errData->positionContext.fileName) {
            LogLinePrefix(errBuffer, errData, logMetaInfo);
            (void)AppendString(errBuffer, "LOCATION:  %s, %s:%d\n", errData->positionContext.funcName,
                               errData->positionContext.fileName, errData->positionContext.lineNo);
        } else if (errData->positionContext.fileName) {
            LogLinePrefix(errBuffer, errData, logMetaInfo);
            (void)AppendString(errBuffer, "LOCATION:  %s:%d\n", errData->positionContext.fileName,
                               errData->positionContext.lineNo);
        }
    }

    if (errData->backtrace != NULL) {
        LogLinePrefix(errBuffer, errData, logMetaInfo);
        (void)AppendString(errBuffer, "BACKTRACELOG:  ");
        AppendWithTabs(errBuffer, errData->backtrace);
        (void)AppendStringChar(errBuffer, '\n');
    }
}

static inline LogIdentifier *CopyConstructLogIdentifier(const LogIdentifier *src)
{
    ASSERT(src != NULL);
    LogIdentifier *logId = (LogIdentifier *)malloc(sizeof(LogIdentifier));
    if (logId != NULL) {
        *logId = *src;
    }
    return logId;
}

static inline void DestoryCopyLogIdentifier(void *msgContext)
{
    free(msgContext);
    WriteErrMsg("Allocate thread message failed.\n");
}

/*
 * Send the error log message to the server.
 */
static void SendErrorLogMessageToServer(const char *buffer, size_t count, uint8_t messageType)
{
    /* Using message pipe */
    ThreadMessagePipeChunk p;
    int fd;
    if (GetErrorLogRedirectionDone()) {
        fd = fileno(stderr);
    } else {
        fd = GetMessagePipe();
    }
    errno_t err;

    ASSERT(count > 0);

    p.pipeHeader.nuls[0] = p.pipeHeader.nuls[1] = '\0';
    p.pipeHeader.tid = GetCurrentTid().tid;
    p.pipeHeader.msgType = messageType;
    p.pipeHeader.magic = PIPE_HEADER_MAGICNUM;
    p.pipeHeader.msgContext = g_threadErrorLogContextData.logIdentifier;

    while (count > THREAD_MESSAGE_PIPE_MAX_PAYLOAD) {
        p.pipeHeader.isLast = 'f';
        p.pipeHeader.chunkLen = THREAD_MESSAGE_PIPE_MAX_PAYLOAD;
        err = memcpy_s(p.pipeHeader.data, THREAD_MESSAGE_PIPE_MAX_PAYLOAD, buffer, THREAD_MESSAGE_PIPE_MAX_PAYLOAD);
        if (err != EOK) {
            ErrLog(FATAL, ErrMsg("memcpy_s failed, errno = %d", err));
        }
        (void)write(fd, &p, THREAD_MESSAGE_PIPE_HEADER_LEN + THREAD_MESSAGE_PIPE_MAX_PAYLOAD);
        buffer += THREAD_MESSAGE_PIPE_MAX_PAYLOAD;
        count -= THREAD_MESSAGE_PIPE_MAX_PAYLOAD;
    }

    p.pipeHeader.isLast = 't';
    p.pipeHeader.chunkLen = (uint16_t)count;
    err = memcpy_s(p.pipeHeader.data, THREAD_MESSAGE_PIPE_MAX_PAYLOAD, buffer, count);
    if (err != EOK) {
        ErrLog(FATAL, ErrMsg("memcpy_s failed, errno = %d", err));
    }

    (void)write(fd, &p, THREAD_MESSAGE_PIPE_HEADER_LEN + count);
}

/**
 * Convert the error log level to the system log level.
 * @param level
 * @param syslogLevel
 */
static void ErrorLogLevel2SyslogLevel(int level, int *syslogLevel)
{
    switch (level) {
        case DEBUG:
            *syslogLevel = EVENT_LOG_DEBUG;
            break;
        case LOG:
        case INFO:
            *syslogLevel = EVENT_LOG_INFO;
            break;
        case NOTICE:
            *syslogLevel = EVENT_LOG_NOTICE;
            break;
        case WARNING:
            *syslogLevel = EVENT_LOG_WARNING;
            break;
        case ERROR:
            *syslogLevel = EVENT_LOG_ERROR;
            break;
        case FATAL:
        case PANIC:
            *syslogLevel = EVENT_LOG_CRIT;
            break;
        default:
            *syslogLevel = EVENT_LOG_ERROR;
            break;
    }
}

static inline bool NeedOutputToServer(uint32_t logDestination)
{
    return (logDestination & LOG_DESTINATION_LOCAL_FILE) != 0 || (logDestination & LOG_DESTINATION_REMOTE_FILE) != 0;
}

/*
 * Actual output of the top-of-stack error message.
 */
static void EmitErrorReport(void)
{
    if (unlikely(g_threadErrorLogContextData.errorDataStackDepth < 0)) {
        ErrLog(ERROR, ErrMsg("ErrStart was not called"));
        return;
    }
    ErrorData *errorData = &(g_threadErrorLogContextData.errorData[g_threadErrorLogContextData.errorDataStackDepth]);
    g_threadErrorLogContextData.recursionDepth++;
    CheckErrorLogStackDepth();
    if (errorData->outputToServer) {
        StringInfoData errBuffer;
        bool ret = InitString(g_threadErrorLogContextData.errorContext, &errBuffer);
        /* If string initial failed, return directly to avoid accessing null pointer */
        if (unlikely(!ret)) {
            return;
        }

        FormatErrorLogMessage(errorData, &errBuffer, &g_threadErrorLogContextData.logIdentifier);
        char *buffer = GetCStrOfString(&errBuffer);
        size_t len = GetLengthOfString(&errBuffer);
        uint32_t logDestination = GetErrLogDestination();
        if ((logDestination & LOG_DESTINATION_LOCAL_STDERR) != 0) {
            (void)fwrite(buffer, 1, len, stderr);
            (void)fflush(stderr);
        }
        if ((logDestination & LOG_DESTINATION_LOCAL_SYSLOG) != 0) {
            int syslogLevel;
            ErrorLogLevel2SyslogLevel(errorData->errorLevel, &syslogLevel);
            WriteSyslog(syslogLevel, buffer, len);
        } else {
            CloseThreadSyslog();
        }
        if (IsLoggerStarted() && NeedOutputToServer(logDestination)) {
            /* If in the syslogger thread, try to write messages direct to file. */
            if (g_threadErrorLogContextData.isLoggerThread) {
                WriteLocalLogDataFile(buffer, len);
            } else {
                if (IsErrLogNeedRedirect()) {
                    if (GetErrorLogRedirectionDone()) {
                        SendErrorLogMessageToServer(buffer, len, errorData->messageType);
                    } else {
                        int syslogLevel;
                        ErrorLogLevel2SyslogLevel(errorData->errorLevel, &syslogLevel);
                        WriteSyslog(syslogLevel, buffer, len);
                    }
                } else {
                    SendErrorLogMessageToServer(buffer, len, errorData->messageType);
                }
            }
        }
        FreeString(&errBuffer);
    }
    if ((errorData->outputToClient) && (g_threadErrorLogContextData.sendToFrontendCallBack != NULL)) {
        g_threadErrorLogContextData.sendToFrontendCallBack(errorData->errorLevel, errorData->sqlErrorCode,
                                                           errorData->message, errorData->cause, errorData->action);
    }
    g_threadErrorLogContextData.recursionDepth--;
}

/**
 * Releases the memory of the errorData-related member.
 * @param errorData: ErrorData.
 */
static void FreeErrorData(ErrorData *errorData)
{
    if (errorData->backtrace != NULL) {
        FreeStringData(errorData->backtrace);
        errorData->backtrace = NULL;
    }
    if (errorData->context != NULL) {
        FreeStringData(errorData->context);
        errorData->context = NULL;
    }
    if (errorData->message != NULL) {
        FreeStringData(errorData->message);
        errorData->message = NULL;
    }
    if (errorData->cause != NULL) {
        FreeStringData(errorData->cause);
        errorData->cause = NULL;
    }
    if (errorData->action != NULL) {
        FreeStringData(errorData->action);
        errorData->action = NULL;
    }
}

/**
 * Process error log to output.
 */
UTILS_EXPORT void ErrFinish(void)
{
    ErrorData *errorData = NULL;
    int errorLevel;
    g_threadErrorLogContextData.recursionDepth++;
    CheckErrorLogStackDepth();
    if (unlikely(g_threadErrorLogContextData.errorDataStackDepth < 0)) {
        ErrLog(ERROR, ErrMsg("ErrStart was not called"));
        return;
    }
    errorData = &(g_threadErrorLogContextData.errorData[g_threadErrorLogContextData.errorDataStackDepth]);
    if ((errorData->backtrace == NULL) && (errorData->positionContext.funcName != NULL) &&
        (g_threadErrorLogContextData.traceCategoryFlag == ENABLE_ERROR_LOG_BACKTRACE)) {
        SetBacktrace(errorData, INTERNAL_BACKTRACE_SUPPORT_FUNC_FRAME);
    }
    /*
     * Call any context callback functions.  Errors occurring in callback functions will be treated as recursive
     * errors --- this ensures we will avoid infinite recursion (see ErrStart).
     */
    SListIter sListIter;
    ErrorLogContextCallback *contextCallback;
    SLIST_FOR_EACH(sListIter, &(g_threadErrorLogContextData.errorLogContextCallbackHead))
    {
        contextCallback = (ErrorLogContextCallback *)(uintptr_t)sListIter.cur;
        contextCallback->callback(contextCallback->arg);
    }

    /* Emit the message to the right places. */
    EmitErrorReport();
    /* Now free up subsidiary data attached to stack entry, and release it. */
    FreeErrorData(errorData);
    g_threadErrorLogContextData.errorDataStackDepth--;
    g_threadErrorLogContextData.recursionDepth--;
    errorLevel = errorData->errorLevel;
    if (errorLevel == FATAL) {
        /*
         * The ErrLog does not know the running phase of the upper-layer module. The upper-layer module invoke
         * SetErrLogToClient() sets the corresponding flag based on the running phase of its own code to determine
         * whether to output logs to the client during the startup phase and normal running phase.
         * Therefore, SetErrLogToClient(DISABLE_ERROR_LOG_SEND_CLIENT_ALL) is not invoked here to disable log output
         * to the client.
         */

        /*
         * fflush here is just to improve the odds that we get to see the error message, in case things are so hosed
         * that threadExitCallback crashes.Any other code you might be tempted to add here should probably be in the
         * threadExitCallback instead.
         */
        (void)fflush(stdout);
        (void)fflush(stderr);
        /*
         * Do normal thread-exit cleanup, then return exit code 1 to indicate FATAL termination.  The master thread
         * may or may not consider this worthy of panic, depending on which sub thread returns it.
         */
        if (g_threadErrorLogContextData.threadExitCallback != NULL) {
            g_threadErrorLogContextData.threadExitCallback(g_threadErrorLogContextData.valuePtr);
        }
        ErrLog(DEBUG, ErrMsg("Thread exit, code is 1"));
        ThreadExit(g_threadErrorLogContextData.valuePtr);
    }
    if (errorLevel >= PANIC) {
        /* Serious crash time,the process exit. */
        (void)fflush(stdout);
        (void)fflush(stderr);
        FlushLogger();
        Abort();
    }
    if (g_threadErrorLogContextData.processInterruptsCallBack != NULL) {
        g_threadErrorLogContextData.processInterruptsCallBack();
    }
    return;
}

/* Below auxiliary functions is used for fill 'ErrorData' */
/*
 *  Add SQLSTATE error code to the current error.
 *  The code is expected to be represented as per MAKE_SQLSTATE().
 */
UTILS_EXPORT void ErrCode(int sqlErrorCode)
{
    int errorDataStackDepth = g_threadErrorLogContextData.errorDataStackDepth;
    if (unlikely(errorDataStackDepth < 0)) {
        ErrLog(ERROR, ErrMsg("ErrStart was not called"));
        return;
    }
    ErrorData *errorData = &(g_threadErrorLogContextData.errorData[errorDataStackDepth]);
    /* we don't bother incrementing recursionDepth */
    CheckErrorLogStackDepth();
    errorData->sqlErrorCode = sqlErrorCode;
    /* return value does not matter */
    return;
}

UTILS_EXPORT void ErrComponentId(int componentId)
{
    int errorDataStackDepth = g_threadErrorLogContextData.errorDataStackDepth;
    if (unlikely(errorDataStackDepth < 0)) {
        ErrLog(ERROR, ErrMsg("ErrStart was not called"));
        return;
    }
    ErrorData *errorData = &(g_threadErrorLogContextData.errorData[errorDataStackDepth]);
    errorData->moduleContext.componentId = componentId;
}

UTILS_EXPORT void ErrModuleId(int moduleId)
{
    int errorDataStackDepth = g_threadErrorLogContextData.errorDataStackDepth;
    if (unlikely(errorDataStackDepth < 0)) {
        ErrLog(ERROR, ErrMsg("ErrStart was not called"));
        return;
    }
    ErrorData *errorData = &(g_threadErrorLogContextData.errorData[errorDataStackDepth]);
    errorData->moduleContext.moduleId = moduleId;
}

UTILS_EXPORT void ErrMsgType(uint8_t messageType)
{
    int errorDataStackDepth = g_threadErrorLogContextData.errorDataStackDepth;
    if (unlikely(errorDataStackDepth < 0)) {
        ErrLog(ERROR, ErrMsg("ErrStart was not called"));
        return;
    }
    ErrorData *errorData = &(g_threadErrorLogContextData.errorData[errorDataStackDepth]);
    errorData->messageType = messageType;
}

#define MakeMessage(targetField, fmt)                                                           \
    do {                                                                                        \
        int errorDataStackDepth = g_threadErrorLogContextData.errorDataStackDepth;              \
        if (unlikely(errorDataStackDepth < 0)) {                                                \
            ErrLog(ERROR, ErrMsg("ErrStart was not called"));                                   \
            break;                                                                              \
        }                                                                                       \
        ASSERT(errorDataStackDepth >= 0);                                                       \
        ErrorData *errorData = &(g_threadErrorLogContextData.errorData[errorDataStackDepth]);   \
        g_threadErrorLogContextData.recursionDepth++;                                           \
        CheckErrorLogStackDepth();                                                              \
        const char *domainFormat = fmt;                                                         \
        if (!InErrorRecursionTrouble()) {                                                       \
            domainFormat = DomainGetText((errorData->domain), domainFormat);                    \
        }                                                                                       \
        StringInfoData buf;                                                                     \
        MemoryContext errorContext = g_threadErrorLogContextData.errorContext;                  \
        bool ret = InitString(errorContext, &buf);                                              \
        if (unlikely(!ret)) {                                                                   \
            break;                                                                              \
        }                                                                                       \
        int32_t needed;                                                                         \
        for (;;) {                                                                              \
            va_list args;                                                                       \
            va_start(args, fmt);                                                                \
            needed = AppendStringVA(&buf, domainFormat, args);                                  \
            va_end(args);                                                                       \
            if (needed == 0) {                                                                  \
                break;                                                                          \
            }                                                                                   \
            if (needed == -1) {                                                                 \
                break;                                                                          \
            }                                                                                   \
            ret = EnlargeString(&buf, (size_t)(long long)needed);                               \
            if (!ret) {                                                                         \
                break;                                                                          \
            }                                                                                   \
        }                                                                                       \
        TransferString(&(errorData->targetField), &buf);                                        \
        g_threadErrorLogContextData.recursionDepth--;                                           \
    } while (0)

/* Fill the 'ErrorData' message field. */
UTILS_EXPORT void ErrMsg(const char *fmt, ...)
{
    MakeMessage(message, fmt);
    return;
}

/* Fill the 'ErrorData' cause field. */
UTILS_EXPORT void ErrCause(const char *fmt, ...)
{
    MakeMessage(cause, fmt);
    return;
}

/* Fill the 'ErrorData' action field. */
UTILS_EXPORT void ErrAction(const char *fmt, ...)
{
    MakeMessage(action, fmt);
    return;
}

/*
 * ErrContext add a context error message text to the current error. Unlike other cases, multiple calls
 * are allowed to build up a stack of context information.  We assume earlier calls represent more-closely-nested
 * states.This function can only be called in the callback function of PushErrorLogContextCallback.
 */
UTILS_EXPORT int ErrContext(const char *fmt, ...)
{
    MakeMessage(context, fmt);
    return 0;
}

/* Below functions is used for set the log config */

/*
 * Set the log output syslog sequence numbers.
 * @param syslogSequenceNumbers : Error log syslog sequence numbers.
 */
void SetErrLogSyslogSequenceNumbers(bool syslogSequenceNumbers)
{
    g_threadErrorLogContextData.sysLogOutputConfig.syslogSequenceNumbers = syslogSequenceNumbers;
}

/*
 * Set the log output syslog split messages.
 * @param syslogSplitMessages : Error log syslog split messages.
 */
void SetErrLogSyslogSplitMessages(bool syslogSplitMessages)
{
    g_threadErrorLogContextData.sysLogOutputConfig.syslogSplitMessages = syslogSplitMessages;
}

/**
 * Set the error log print backtrace
 * @param backtraceFlag  :ENABLE_ERROR_LOG_BACKTRACE indicates allowed.
 * DISABLE_ERROR_LOG_BACKTRACE indicates forbidden.
 */
void SetErrLogBacktrace(int backtraceFlag)
{
    g_threadErrorLogContextData.traceCategoryFlag = backtraceFlag;
}

void ClearErrLogConnectionFilter(void)
{
    int i;
    int count = g_threadErrorLogContextData.connectionFilterCount;
    ErrLogConnection *filter = g_threadErrorLogContextData.connectionFilter;
    for (i = 0; i < count; i++) {
        MemFree(filter[i].srcIP);
        filter[i].srcIP = NULL;
        filter[i].srcPort = 0;
        MemFree(filter[i].destIP);
        filter[i].destIP = NULL;
        filter[i].destPort = 0;
    }
    g_threadErrorLogContextData.connectionFilterCount = 0;
}

/**
 * Set the error log connection filter.
 * @param srcIP    : Source IP address.
 * @param srcPort  : Source port.
 * @param destIP   : Destination IP address.
 * @param destPort : Destination port
 */
void SetErrLogConnectionFilter(char **srcIP, int *srcPort, char **destIP, int *destPort, int count)
{
    ClearErrLogConnectionFilter();
    int i;
    int filterCount = count;
    if (filterCount > MAX_ERROR_FILTER_COUNT) {
        filterCount = MAX_ERROR_FILTER_COUNT;
    }
    MemoryContext errorContext = g_threadErrorLogContextData.errorContext;
    ErrLogConnection *filter = g_threadErrorLogContextData.connectionFilter;
    for (i = 0; i < filterCount; i++) {
        filter[i].srcIP = MemoryContextStrdup(errorContext, srcIP[i]);
        filter[i].srcPort = srcPort[i];
        filter[i].destIP = MemoryContextStrdup(errorContext, destIP[i]);
        filter[i].destPort = destPort[i];
    }
    g_threadErrorLogContextData.connectionFilterCount = filterCount;
}

/*
 * Set error log output connection information.
 */
UTILS_EXPORT void SetErrLogConnectionInfo(char *srcIP, int srcPort, char *destIP, int destPort)
{
    ClearErrLogConnectionInfo();
    MemoryContext errorContext = g_threadErrorLogContextData.errorContext;
    if (srcIP != NULL) {
        g_threadErrorLogContextData.connectionInfo.srcIP = MemoryContextStrdup(errorContext, srcIP);
    } else {
        g_threadErrorLogContextData.connectionInfo.srcIP = MemoryContextStrdup(errorContext, ERROR_LOG_UNKNOWN_NAME);
    }
    g_threadErrorLogContextData.connectionInfo.srcPort = srcPort;
    if (destIP != NULL) {
        g_threadErrorLogContextData.connectionInfo.destIP = MemoryContextStrdup(errorContext, destIP);
    } else {
        g_threadErrorLogContextData.connectionInfo.destIP = MemoryContextStrdup(errorContext, ERROR_LOG_UNKNOWN_NAME);
    }
    g_threadErrorLogContextData.connectionInfo.destPort = destPort;
}

/*
 * Clear error log output connection information.
 */
UTILS_EXPORT void ClearErrLogConnectionInfo(void)
{
    if (g_threadErrorLogContextData.connectionInfo.srcIP != NULL) {
        MemFree(g_threadErrorLogContextData.connectionInfo.srcIP);
        g_threadErrorLogContextData.connectionInfo.srcIP = NULL;
    }
    g_threadErrorLogContextData.connectionInfo.srcPort = 0;
    if (g_threadErrorLogContextData.connectionInfo.destIP != NULL) {
        MemFree(g_threadErrorLogContextData.connectionInfo.destIP);
        g_threadErrorLogContextData.connectionInfo.destIP = NULL;
    }
    g_threadErrorLogContextData.connectionInfo.destPort = 0;
}

/*
 * Clear error log output session information.
 */
void ClearErrLogSessionFilter(void)
{
    int i;
    int count = g_threadErrorLogContextData.sessionFilterCount;
    ErrLogSession *filter = g_threadErrorLogContextData.sessionFilter;
    for (i = 0; i < count; i++) {
        MemFree(filter[i].applicationName);
        filter[i].applicationName = NULL;
        MemFree(filter[i].databaseName);
        filter[i].databaseName = NULL;
        MemFree(filter[i].userName);
        filter[i].userName = NULL;
        filter[i].sessionID = 0;
    }
    g_threadErrorLogContextData.sessionFilterCount = 0;
}

/*
 * Set the error log session filter
 * @param applicationName    : Application name.
 * @param databaseName       : Database name.
 * @param userName           : User name.
 * @param sessionID          : Session ID.
 */
void SetErrLogSessionFilter(char **applicationName, char **databaseName, char **userName, int *sessionID, int count)
{
    ClearErrLogSessionFilter();
    int i;
    int filterCount = count;
    if (filterCount > MAX_ERROR_FILTER_COUNT) {
        filterCount = MAX_ERROR_FILTER_COUNT;
    }
    MemoryContext errorContext = g_threadErrorLogContextData.errorContext;
    ErrLogSession *filter = g_threadErrorLogContextData.sessionFilter;
    for (i = 0; i < filterCount; i++) {
        filter[i].applicationName = MemoryContextStrdup(errorContext, applicationName[i]);
        filter[i].databaseName = MemoryContextStrdup(errorContext, databaseName[i]);
        filter[i].userName = MemoryContextStrdup(errorContext, userName[i]);
        filter[i].sessionID = sessionID[i];
    }
    g_threadErrorLogContextData.sessionFilterCount = filterCount;
}

/*
 * Set error log output session information.
 */
UTILS_EXPORT void SetErrLogSessionInfo(char *applicationName, char *databaseName, char *userName, int sessionID)
{
    ClearErrLogSessionInfo();
    MemoryContext errorContext = g_threadErrorLogContextData.errorContext;
    ErrLogSession *sessionInfo = &(g_threadErrorLogContextData.sessionInfo);
    if (applicationName != NULL) {
        sessionInfo->applicationName = MemoryContextStrdup(errorContext, applicationName);
    } else {
        sessionInfo->applicationName = MemoryContextStrdup(errorContext, ERROR_LOG_UNKNOWN_NAME);
    }
    if (databaseName != NULL) {
        sessionInfo->databaseName = MemoryContextStrdup(errorContext, databaseName);
    } else {
        sessionInfo->databaseName = MemoryContextStrdup(errorContext, ERROR_LOG_UNKNOWN_NAME);
    }
    if (userName != NULL) {
        sessionInfo->userName = MemoryContextStrdup(errorContext, userName);
    } else {
        sessionInfo->userName = MemoryContextStrdup(errorContext, ERROR_LOG_UNKNOWN_NAME);
    }
    sessionInfo->sessionID = sessionID;
}

/*
 * Clear error log output session information.
 */
UTILS_EXPORT void ClearErrLogSessionInfo(void)
{
    if (g_threadErrorLogContextData.sessionInfo.applicationName != NULL) {
        MemFree(g_threadErrorLogContextData.sessionInfo.applicationName);
        g_threadErrorLogContextData.sessionInfo.applicationName = NULL;
    }
    if (g_threadErrorLogContextData.sessionInfo.databaseName != NULL) {
        MemFree(g_threadErrorLogContextData.sessionInfo.databaseName);
        g_threadErrorLogContextData.sessionInfo.databaseName = NULL;
    }
    if (g_threadErrorLogContextData.sessionInfo.userName != NULL) {
        MemFree(g_threadErrorLogContextData.sessionInfo.userName);
        g_threadErrorLogContextData.sessionInfo.userName = NULL;
    }
    g_threadErrorLogContextData.sessionInfo.sessionID = 0;
}

/*
 * Clear error log running context filter.
 */
void ClearErrLogRunningContextFilter(void)
{
    int i;
    int count = g_threadErrorLogContextData.runningContextFilterCount;
    ErrLogRunningContext *filter = g_threadErrorLogContextData.runningContextFilter;
    for (i = 0; i < count; i++) {
        MemFree(filter[i].threadName);
        filter[i].threadName = NULL;
        SetPid(&(filter[i].pid), 0);
        SetTid(&(filter[i].tid), 0);
    }
    g_threadErrorLogContextData.runningContextFilterCount = 0;
}

/*
 * Set the error log running context filter
 * @param pid          : PID
 * @param tid          : TID
 * @param threadName   : thread name
 */
void SetErrLogRunningContextFilter(int *pid, uint32_t *tid, char **threadName, int count)
{
    ClearErrLogRunningContextFilter();

    int i;
    int filterCount = count;
    if (filterCount > MAX_ERROR_FILTER_COUNT) {
        filterCount = MAX_ERROR_FILTER_COUNT;
    }
    MemoryContext errorContext = g_threadErrorLogContextData.errorContext;
    ErrLogRunningContext *filter = g_threadErrorLogContextData.runningContextFilter;
    for (i = 0; i < filterCount; i++) {
        SetPid(&(filter[i].pid), pid[i]);
        SetTid(&(filter[i].tid), tid[i]);
        filter[i].threadName = MemoryContextStrdup(errorContext, threadName[i]);
    }
    g_threadErrorLogContextData.runningContextFilterCount = filterCount;
}

/*
 * Set error log output running context information.
 */
void SetErrLogRunningContextInfo(const char *threadName)
{
    ClearErrLogRunningContextInfo();
    MemoryContext errorContext = g_threadErrorLogContextData.errorContext;
    ErrLogRunningContext *runningContextInfo = &(g_threadErrorLogContextData.runningContextInfo);
    if (threadName != NULL) {
        runningContextInfo->threadName = MemoryContextStrdup(errorContext, threadName);
    } else {
        runningContextInfo->threadName = MemoryContextStrdup(errorContext, ERROR_LOG_UNKNOWN_NAME);
    }
    runningContextInfo->pid = GetCurrentPid();
    runningContextInfo->tid = GetCurrentTid();
}

/*
 * Clear error log output running context information.
 */
UTILS_EXPORT void ClearErrLogRunningContextInfo(void)
{
    if (g_threadErrorLogContextData.runningContextInfo.threadName != NULL) {
        MemFree(g_threadErrorLogContextData.runningContextInfo.threadName);
        g_threadErrorLogContextData.runningContextInfo.threadName = NULL;
    }
    SetPid(&(g_threadErrorLogContextData.runningContextInfo.pid), 0);
    SetTid(&(g_threadErrorLogContextData.runningContextInfo.tid), 0);
}

/*
 * Clear error log position context filter.
 */
void ClearErrLogPositionContextFilter(void)
{
    int i;
    int count = g_threadErrorLogContextData.positionContextFilterCount;
    ErrLogPositionContext *filter = g_threadErrorLogContextData.positionContextFilter;
    for (i = 0; i < count; i++) {
        MemFree(filter[i].fileName);
        filter[i].fileName = NULL;
        filter[i].lineNo = 0;
        MemFree(filter[i].funcName);
        filter[i].funcName = NULL;
    }
    g_threadErrorLogContextData.positionContextFilterCount = 0;
}

/*
 * Set the error log position context filter.
 * @param fileName       : File name.
 * @param lineNo         : Line number.
 * @param funcName       : Function name.
 */
void SetErrLogPositionContextFilter(char **fileName, int *lineNo, char **functionName, int count)
{
    ClearErrLogPositionContextFilter();

    int i;
    int filterCount = count;
    if (filterCount > MAX_ERROR_FILTER_COUNT) {
        filterCount = MAX_ERROR_FILTER_COUNT;
    }
    MemoryContext errorContext = g_threadErrorLogContextData.errorContext;
    ErrLogPositionContext *filter = g_threadErrorLogContextData.positionContextFilter;
    for (i = 0; i < filterCount; i++) {
        filter[i].fileName = MemoryContextStrdup(errorContext, fileName[i]);
        filter[i].lineNo = lineNo[i];
        filter[i].funcName = MemoryContextStrdup(errorContext, functionName[i]);
    }
    g_threadErrorLogContextData.positionContextFilterCount = filterCount;
}
/*
 * Set error log output transaction ID information.
 */
UTILS_EXPORT void SetErrLogTransactionIDInfo(int transactionID)
{
    g_threadErrorLogContextData.transactionID = transactionID;
}

/*
 * Clear error log output transaction ID information.
 */
UTILS_EXPORT void ClearErrLogTransactionIDInfo(void)
{
    g_threadErrorLogContextData.transactionID = INVALID_TRANSACTION_ID;
}

/*
 * Set error log output query string information.
 */
UTILS_EXPORT void SetErrLogQueryStringInfo(const char *queryString)
{
    ClearErrLogQueryStringInfo();
    g_threadErrorLogContextData.queryString =
        MemoryContextStrdup(g_threadErrorLogContextData.errorContext, queryString);
}

/*
 * Clear error log output query string information.
 */
UTILS_EXPORT void ClearErrLogQueryStringInfo(void)
{
    if (g_threadErrorLogContextData.queryString != NULL) {
        MemFree(g_threadErrorLogContextData.queryString);
        g_threadErrorLogContextData.queryString = NULL;
    }
}

/*
 * Initialize error log configuration parameters.
 */
static void InitThreadErrlogConfigParameters(void)
{
    SetErrLogSyslogSplitMessages(1);
    SetErrLogSyslogSequenceNumbers(1);
}

THR_LOCAL int g_errorLogOpenState = ERROR_LOG_CLOSED;

/*
 * Open error log. Error Log Client Initialization.
 * @return ErrorCode
 */
ErrorCode OpenErrorLog(void)
{
    if (g_errorLogOpenState == ERROR_LOG_OPENED) {
        return ERROR_SYS_OK;
    }

    errno_t rc = memset_s(&g_threadErrorLogContextData, sizeof(g_threadErrorLogContextData), 0,
                          sizeof(g_threadErrorLogContextData));
    if (rc != EOK) {
        return ERROR_UTILS_COMMON_SECURE_C_FUNCTION_FAILED;
    }
    /* need limited the memory size before tr5, and remove abort when memory not enough */
    g_threadErrorLogContextData.errorContext =
        MemoryContextCreate(NULL, MEM_CXT_TYPE_SHARE, "ThreadErrorLogContext", MCTX_UNUSED, MCTX_UNUSED,
                            ERRMSG_BEFORE_SENDTO_QUEUE_MAXSIZE);
    MemoryContextSetSilent(g_threadErrorLogContextData.errorContext, true); // avoid circular dependencies
    InitThreadErrlogConfigParameters();
    InitErrorLogContextCallback();
    g_threadErrorLogContextData.recursionDepth = 0;
    g_threadErrorLogContextData.errorDataStackDepth = -1;
    g_threadErrorLogContextData.isLoggerThread = false;
#define THREAD_NAME_MAX_LEN 16
    char threadName[THREAD_NAME_MAX_LEN];
    if (ThreadGetName(threadName, THREAD_NAME_MAX_LEN) == ERROR_SYS_OK) {
        const char *threadNameStr = threadName;
        SetErrLogRunningContextInfo(threadNameStr);
    } else {
        SetErrLogRunningContextInfo(ERROR_LOG_UNKNOWN_NAME);
    }
    Tid currentTid = GetCurrentTid();
    Tid logTid = GetLoggerThreadTid();
    if (TidIsEqual(&currentTid, &logTid)) {
        g_threadErrorLogContextData.isLoggerThread = true;
    }
    /* Record thread id in fold info */
    g_threadErrorLogContextData.logIdentifier.threadId = Tid2Integer(&currentTid);
    g_errorLogOpenState = ERROR_LOG_OPENED;
    return ERROR_SYS_OK;
}

static void FreeThreadErrorLogContextData(void)
{
    MemoryContextDelete(g_threadErrorLogContextData.errorContext);
    g_threadErrorLogContextData.errorContext = NULL;
}

/*
 * Close error log.
 */
void CloseErrorLog(void)
{
    CloseThreadSyslog();
    if (g_threadErrorLogContextData.errorContext != NULL) {
        FreeThreadErrorLogContextData();
    }
    g_errorLogOpenState = ERROR_LOG_CLOSED;
}

/*
 *	Use this to mark string constants as needing translation at some later
 *	time, rather than immediately.  This is useful for cases where you need
 *	access to the original string and translated string, and for cases where
 *	immediate translation is not possible, like when initializing global
 *	variables.
 *
 */
#define GetTextNoOperation(x) (x)

/*
 * Get the localized string representing elevel.
 * Spaces are used to align to the seven bytes of warning.
 */
UTILS_EXPORT const char *ErrorLevel2String(int elevel)
{
    const char *prefix = NULL;
    switch (elevel) {
        case DEBUG:
            prefix = GetTextNoOperation("DEBUG");
            break;
        case LOG:
            prefix = GetTextNoOperation("LOG");
            break;
        case INFO:
            prefix = GetTextNoOperation("INFO");
            break;
        case NOTICE:
            prefix = GetTextNoOperation("NOTICE");
            break;
        case WARNING:
            prefix = GetTextNoOperation("WARNING");
            break;
        case ERROR:
            prefix = GetTextNoOperation("ERROR");
            break;
        case FATAL:
            prefix = GetTextNoOperation("FATAL");
            break;
        case PANIC:
            prefix = GetTextNoOperation("PANIC");
            break;
        default:
            prefix = ERROR_LOG_UNKNOWN_NAME;
            break;
    }
    return prefix;
}

/**
 * Get error string information corresponding to security function error codes.
 * @param errCode : errno_t.
 * @return : Error string.
 */
UTILS_EXPORT const char *GetSecurecErrorInfo(errno_t errCode)
{
    if (errCode != EOK) {
        switch (errCode) {
            case EINVAL:
                return "The destination buffer is NULL or not terminated."
                       "The second case only occures in function strcat_s/strncat_s.";
            case EINVAL_AND_RESET:
                return "The source buffer is NULL.";
            case ERANGE:
                return "The parameter destMax is equal to zero or larger than the macro : "
                       "SECUREC_STRING_MAX_LEN.";
            case ERANGE_AND_RESET:
                return "The parameter destMax is too small or parameter count is larger than macro parameter "
                       "SECUREC_STRING_MAX_LEN. The second case only occures in functions strncat_s/strncpy_s.";
            case EOVERLAP_AND_RESET:
                return "The destination buffer and source buffer are overlapped.";
            default:
                return "Unrecognized return type.";
        }
    } else {
        return ERROR_LOG_UNKNOWN_NAME;
    }
}
/**
 * Checking the error codes returned by security functions.
 * @param errCode :errno_t
 * @param printfFamily :Indicates whether to print logs.
 * @return:true or false.
 */
UTILS_EXPORT bool CheckSecurecRetCode(errno_t errCode, bool printfFamily)
{
    bool flag = (bool)((printfFamily && errCode == -1) || (!printfFamily && errCode != EOK));
    return flag;
}

/**
 * Create error log directories.
 * @param directoryName
 * @param mode
 * @return
 */
ErrorCode MakeErrLogDirectories(const char *directoryName, unsigned int mode)
{
    if (directoryName == NULL) {
        return ERROR_UTILS_ERRORLOG_DIRECTORY_IS_NULL;
    }

    if (strlen(directoryName) > (PATH_MAX - 1)) {
        return ERROR_UTILS_ERRORLOG_DIRECTORY_IS_OUT_OF_RANGE;
    }

    ErrorCode errCode = ERROR_SYS_OK;
    char tempDir[PATH_MAX] = {0};
    (void)strncpy_s(tempDir, sizeof(tempDir), directoryName, sizeof(tempDir) - 1);
    size_t strLen = strlen(tempDir);
    for (size_t i = 0; i < strLen; i++) {
        if (tempDir[i] == '/') {
            tempDir[i] = '\0';
            if (access(tempDir, 0) != 0) {
                mkdir(tempDir, mode);
            }
            tempDir[i] = '/';
        }
    }

    if (strLen > 0 && access(tempDir, 0) != 0) {
        int rc = mkdir(tempDir, mode);
        if (rc != 0) {
            PosixErrorCode2PortErrorCode(errno, &errCode);
        }
    }
    return errCode;
}
