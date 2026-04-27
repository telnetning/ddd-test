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
 * syslogger.c
 *
 * Description:
 * 1. Log daemon thread processing for error logs, audit logs, etc.
 *
 * ---------------------------------------------------------------------------------
 */
#include <ctype.h>
#include <libintl.h>
#include <sys/poll.h>
#include <signal.h>
#include <zlib.h>
#include "securec.h"
#include "port/platform_port.h"
#include "port/posix_pipe.h"
#include "container/string_info.h"
#include "vfs/vfs_interface.h"
#include "vfs/vfs_linux_common.h"
#include "fault_injection/fault_injection.h"
#include "defines/common_defs.h"
#include "syslog/err_log_internal.h"
#include "syslog/err_log_fold.h"
#include "syslog/err_log_flow_control.h"

#ifdef LOCAL_MODULE_NAME
#undef LOCAL_MODULE_NAME
#endif
#define LOCAL_MODULE_NAME     "syslog"
#define FFIC_FILE_NAME        "ffic_gaussdb"
#define PLSQL_FFIC_FILE_NAME  "plsqlffic_gaussdb"

typedef struct ErrLogModuleFilterContext ErrLogModuleFilterContext;
typedef struct ErrLogComponentFilterContext ErrLogComponentFilterContext;

struct ErrLogComponentFilterContext {
    bool isFilterStarted;
    struct ErrLogModuleFilterContext {
        int componentId;
        int moduleNum;
        struct ErrLogModuleLevelPair {
            int moduleId;
            int errorLevel;
        } moduleLevelPair[MAX_MODULE_FILTER_COUNT];
    } moduleContextFilter[MAX_COMPONENT_FILTER_COUNT];
};

/* Log file config. */
typedef struct LogFileConfig LogFileConfig;
struct LogFileConfig {
    char *logDirectory;             /* Log file directory. */
    char *pLogDirectory;            /* Plog file directory. */
    char *slowQueryLogDirectory;    /* Slow query log file directory. */
    char *aspLogDirectory;          /* Asp log file directory. */
    char *FFICLogDirectory;          /* ffic log file directory. */
    uint32_t logDirectorySpaceSize; /* Maximum size of log directory in kilobytes. */
    char *logFileName;              /* Log file name. */
    char *slowQueryFileName;        /* Slow query file name. */
    char *aspFileName;              /* Asp file name. */
    int logFileMode;                /* Log file mode,default 0600. */
    uint32_t logFileRotationSize;   /* Maximum size of an individual log file in kilobytes. */
    uint32_t logFileRotationAge;    /* Maximum lifetime of an individual log file in minutes. */
    uint32_t maxSequenceNo;         /* Log data file max sequence number. */
    int fileType;                   /* File type,such as error log file,audit file,and SQL file. */
};

typedef struct RemoteLogFileContext RemoteLogFileContext;
struct RemoteLogFileContext {
    ErrorLogRemoteFileConfigure remoteFileCfg;
    uint32_t currentSequenceNo;   /* Current sequence number. */
    uint32_t logFileRotationSize; /* Maximum size of an individual log file in kilobytes. */
    uint32_t logFileRotationAge;  /* Maximum lifetime of an individual log file in minutes. */
    FileDescriptor *metaFd;       /* Metafile handle. */
    FileDescriptor *dataFd;       /* Log data file handle. */
    int64_t currentDataFileSize;  /* Log data file size to check if needed to switch file */
};

#define LOCAL_VIRTUAL_FILE_SYSTEM  0
#define REMOTE_VIRTUAL_FILE_SYSTEM 1

typedef struct LogOutputFile LogOutputFile;
struct LogOutputFile {
    int vfsType;                             /* VFS type,local vfs or remote vfs. */
    FileDescriptor *dataFd;                  /* Log data file handle. */
    FileDescriptor *profileFd;               /* PLog file handle. */
    FileDescriptor *sqlFd;                   /* Slow query log data file handle. */
    FileDescriptor *aspFd;                   /* Asp log data file handle. */
    FileDescriptor *csvFd;                   /* Csv log file handle. */
    uint32_t currentSequenceNo;              /* Current data file sequence number. */
    uint32_t currentProfileSequenceNo;       /* Current data file sequence number. */
    uint32_t currentSqlSequenceNo;           /* Current data file sequence number. */
    uint32_t currentAspSequenceNo;           /* Current data file sequence number. */
    uint32_t currentCsvSequenceNo;           /* Current data file sequence number. */
    uint32_t currentCompressedSequenceNo;    /* Current compressed log file sequence number. */
    TimesSecondsSinceEpoch nextRotationTime; /* Next rotate time. */
    TimesSecondsSinceEpoch lastLogTime;
    TimesSecondsSinceEpoch lastLogRotateTime;
    TimesSecondsSinceEpoch lastPLogTime;
    TimesSecondsSinceEpoch lastPLogRotateTime;
    TimesSecondsSinceEpoch lastSlowQueryLogTime;
    TimesSecondsSinceEpoch lastSlowQueryLogRotateTime;
    TimesSecondsSinceEpoch lastAspLogTime;
    TimesSecondsSinceEpoch lastAspLogRotateTime;
    TimesSecondsSinceEpoch lastCsvLogTime;
    TimesSecondsSinceEpoch lastCsvLogRotateTime;
};
typedef struct ModuleTextDomain ModuleTextDomain;
struct ModuleTextDomain {
    char *componentName;
    char *moduleName;
    char *domainName;
    char *currentDirName;
};
typedef enum LogThreadState {
    LOG_THREAD_STATE_READY = 1,
    LOG_THREAD_STATE_RUNNING,
    LOG_THREAD_STATE_FAILED,
} LogThreadState;
#define MAX_MODULE_TEXT_DOMAIN 5
#define BUFFER_LISTS_NUMBER    256
#define LOG_PIPE_NAME_LEN      1024
#define COMPRESSD_FFIC_COUNT   100

typedef struct Latch Latch;
struct Latch {
    sig_atomic_t isSet;
    bool is_shared;
    Tid ownerPid;
};

typedef struct LoggerThreadContext LoggerThreadContext;
struct LoggerThreadContext {
    volatile bool rotationRequested;   /* The log file rotate request signal received ? */
    volatile bool configModifyPending; /* Configuration file modification signal received ? */
    volatile bool stopPending;         /* Stop the logger thread ? */
    int messagePipe[2];                /* messagePipe[0] for read, messagePipe[1] for write */
    Tid tid;                           /* The logger thread tid. */
    bool enableLogFileVerification;    /* The log thread will automatically remove unrecognized log files */
    bool localVfsInitByLoggerThread;   /* The local file system is initialized by the logger thread ? */
    VirtualFileSystem *localVfs;       /* Local virtual file system. */
    VirtualFileSystem *remoteVfs;      /* Remote virtual file system. */
    MemoryContext logContext;          /* The logger thread memory context. */
    char *processName;                 /* The current process name. */
    /* The remote file system call back function to get the file id. */
    ErrorCode (*getLogFileIdFun)(char *fileName, const char *storeSpaceName);
    /* The remote file system call back function to get the log file name. */
    char **(*getLogFileFun)(int *count);
    /* The remote file system call back function to free the log file name. */
    void (*freeLogFileFun)(char **fileName);
    /* The remote file system call back function to remove the log file id. */
    void (*removeLogFileIdFun)(char *fileName);
    uint32_t domainCount;                                      /* The module domain count. */
    ModuleTextDomain moduleTextDomain[MAX_MODULE_TEXT_DOMAIN]; /* The module bind text domain. */

    MemoryContext errorLogContext;    /* The error log context. */
    uint32_t errorLogFileDestination; /* The error log destination. */
    LogFileConfig errorLogFileConfig; /* The error log file configure. */
    uint64_t errorLogLocalFileCfgSeq;
    LogOutputFile errorLogOutputFile; /* The error log output file. */
    LogFoldContext *logFoldContext;
    LogFlowContext *logFlowContext;
    volatile bool remoteLogIsReady;
    volatile bool remoteLogIsStopPending;
    RemoteLogFileContext remoteLogContext;
    MsgBatchData msgBatchData;
    LogThreadState logThreadState;
    ConditionVariable cond;
    Mutex mutex;
    ConditionVariable flushCond;
    Mutex flushMutex;
    volatile bool isNeedFlush; /* Need flush the log to log file? */
    /* When there are some problems happened in logger thread, write error message to syslog(true) or stderr(false) */
    volatile bool isLogThreadErrSyslogWrite;
    DListHead bufferLists[BUFFER_LISTS_NUMBER];
    bool redirection_done;
    Latch errorLogLatch;
    volatile sig_atomic_t wait;
    volatile uint32_t latchTimes;
    int selfPipeReadFd;
    int selfPipeWriteFd;
    MemoryContext pipeErrorMsgContext; /* The pipe error message context. */
    volatile bool pipeEofSeen;
    volatile bool isFinalChunk;
    bool isDiskFull;
    bool isUseSignal;
};

#define LOG_COMPRESSION_THREAD_NOT_STARTED 0
#define LOG_COMPRESSION_THREAD_STARTING    1
#define LOG_COMPRESSION_THREAD_STARTED     2
int g_logCompressionThreadState = LOG_COMPRESSION_THREAD_NOT_STARTED;
typedef struct LogCompressionThreadContext LogCompressionThreadContext;
struct LogCompressionThreadContext {
    Tid tid;
    bool logCompressionExitFlag;
    bool compressEnableFlag;
    Mutex mutex;
    ConditionVariable cond;
};

typedef struct FullPathName FullPathName;
struct FullPathName {
    char *directoryName; /* Directory name. */
    char *fileName;      /* File name. */
    char *appendName;    /* Append suffix name. */
    char *extendName;    /* File extend name. */
};

LoggerThreadContext g_loggerThreadContext;
LogCompressionThreadContext g_logCompressionThreadContext;

/**
 * check file size need rotation, file size unit = B, file rotation size = KB
 */
static inline bool CheckFileSizeNeedRotation(int64_t fileSize, uint32_t fileRotationSize)
{
    if (fileSize > (1024L * fileRotationSize)) {
        return true;
    }
    return false;
}

/* Define inline function for mock vfs operation */
static inline ErrorCode LogFileOpen(VirtualFileSystem *vfs, SYMBOL_UNUSED uint16_t fileId, const char *pathName,
                                    int flags, FileDescriptor **fd)
{
    ErrorCode errorCode = -1;
    FAULT_INJECTION_CALL_REPLACE(MOCK_REMOTE_LOG_OPEN, &errorCode, vfs, fileId, pathName, flags, fd)
    errorCode = Open(vfs, pathName, flags, fd);
    FAULT_INJECTION_CALL_REPLACE_END;
    return errorCode;
}

static inline ErrorCode LogFileCreate(VirtualFileSystem *vfs, SYMBOL_UNUSED uint16_t fileId, const char *pathName,
                                      FileParameter fileParameter, FileDescriptor **fd)
{
    ErrorCode errorCode = -1;
    FAULT_INJECTION_CALL_REPLACE(MOCK_REMOTE_LOG_CREATE, &errorCode, vfs, fileId, pathName, fileParameter, fd)
    errorCode = Create(vfs, pathName, fileParameter, fd);
    FAULT_INJECTION_CALL_REPLACE_END;
    return errorCode;
}

static inline ErrorCode LogFileIsExist(VirtualFileSystem *vfs, SYMBOL_UNUSED uint16_t fileId, const char *pathName,
                                       bool *out)
{
    ErrorCode errorCode = -1;
    FAULT_INJECTION_CALL_REPLACE(MOCK_REMOTE_LOG_FILE_IS_EXIST, &errorCode, vfs, fileId, pathName, out)
    errorCode = FileIsExist(vfs, pathName, out);
    FAULT_INJECTION_CALL_REPLACE_END;
    return errorCode;
}

static inline ThreadStartRoutine GetThreadFunc(void)
{
    ThreadStartRoutine routineFunc = NULL;
    FAULT_INJECTION_CALL_REPLACE(MOCK_LOGGER_THREAD_ROUTINE, &routineFunc)
    routineFunc = LoggerMainThread;
    FAULT_INJECTION_CALL_REPLACE_END;
    return routineFunc;
}

static inline ThreadStartRoutine GetLogCompressionThreadFunc(void)
{
    ThreadStartRoutine routineFunc = NULL;
    routineFunc = LogCompressionMainThread;
    return routineFunc;
}

/* Log output to stderr and syslog in default without logger thread, switch to local file when logger thread ready */
#define DEFAULT_LOG_DESTINATION (LOG_DESTINATION_LOCAL_STDERR | LOG_DESTINATION_LOCAL_SYSLOG)

ErrorLogConfigure g_errorLogConfigure = {
    .errLogDestination = DEFAULT_LOG_DESTINATION,
    .errLogLinePrefixSuffix = ERROR_LOG_DEFAULT_PREFIX,
    .clientLevel = NOTICE,
    .serverLevel = WARNING,
    .foldLevel = WARNING,
    .foldThreshold = 0,
    .foldPeriod = 0,
    .maxErrLogFlow = 0,
    .isBatchWrite = true,
    .flowControlThreshold = 0,
    .filterLevel = 0,
    .logStatPeriod = 0,
    .isNeedRedirect = false,
    .logLocalTime = NULL,
    .enableCompress = true,
};

UTILS_EXPORT void ResetErrorLogConfigure(void)
{
    g_errorLogConfigure.errLogDestination = DEFAULT_LOG_DESTINATION;
    g_errorLogConfigure.errLogLinePrefixSuffix = ERROR_LOG_DEFAULT_PREFIX;
    g_errorLogConfigure.clientLevel = NOTICE;
    g_errorLogConfigure.serverLevel = WARNING;
    g_errorLogConfigure.maxErrLogFlow = 0;
    g_errorLogConfigure.isBatchWrite = true;
    g_errorLogConfigure.isNeedRedirect = false;
    g_errorLogConfigure.logLocalTime = NULL;
    g_errorLogConfigure.enableCompress = true;
    ResetErrLogFoldConfig();
    ResetErrLogFlowConfig();
}

/*
 * Set the error log line prefix and suffix.
 * @param errLogLinePrefixSuffix : Error log line prefix and suffix. This parameter is a binary bit string.
 * Each bit indicates a log prefix or suffix.The value 1 indicates that the log prefix or suffix is enabled
 * and the value 0 indicates that the log prefix or suffix is disabled.The log prefixes and suffixes
 * corresponding to bits from left to right are as follows:source IP,source port,destination IP,
 * destination port,application name,database name, user name,sessionID,PID,TID,thread name,component name,
 * module name,file name,line number,function name,normal timestamp,high precision timestamp,epoch timestamp,
 * level,transaction ID,query string,context,backtrace.total 24 bit.
 * The expansion is performed on the right, and the left remains unchanged and total keeps forward compatible.
 */
UTILS_EXPORT void SetErrLogLinePrefixSuffix(uint32_t errLogLinePrefixSuffix)
{
    g_errorLogConfigure.errLogLinePrefixSuffix = errLogLinePrefixSuffix;
}

UTILS_EXPORT uint32_t GetErrLogLinePrefixSuffix(void)
{
    return g_errorLogConfigure.errLogLinePrefixSuffix;
}

UTILS_EXPORT void ResetErrLogLinePrefixSuffix(void)
{
    g_errorLogConfigure.errLogLinePrefixSuffix = ERROR_LOG_DEFAULT_PREFIX;
}

UTILS_EXPORT void SetErrLogDestination(uint32_t logDestination)
{
    g_errorLogConfigure.errLogDestination = logDestination;
}

UTILS_EXPORT uint32_t GetErrLogDestination(void)
{
    return g_errorLogConfigure.errLogDestination;
}

UTILS_EXPORT void SetErrLogServerLevel(int serverLevel)
{
    g_errorLogConfigure.serverLevel = serverLevel;
}

UTILS_EXPORT int GetErrLogServerLevel(void)
{
    return g_errorLogConfigure.serverLevel;
}

UTILS_EXPORT void SetErrLogClientLevel(int clientLevel)
{
    g_errorLogConfigure.clientLevel = clientLevel;
}

UTILS_EXPORT int GetErrLogClientLevel(void)
{
    return g_errorLogConfigure.clientLevel;
}

UTILS_EXPORT bool SetErrLogFoldConfig(uint32_t foldPeriod, uint32_t foldThreshold, int foldLevel)
{
    if (g_loggerThreadContext.logFoldContext != NULL &&
        !LogFoldSetRule(g_loggerThreadContext.logFoldContext, foldLevel, foldThreshold, foldPeriod)) {
        return false;
    }
    g_errorLogConfigure.foldPeriod = foldPeriod;
    g_errorLogConfigure.foldThreshold = foldThreshold;
    g_errorLogConfigure.foldLevel = foldLevel;
    return true;
}

UTILS_EXPORT void ResetErrLogFoldConfig(void)
{
    g_errorLogConfigure.foldPeriod = 0;
    g_errorLogConfigure.foldThreshold = 0;
    g_errorLogConfigure.foldLevel = WARNING;
    (void)LogFoldSetRule(g_loggerThreadContext.logFoldContext, g_errorLogConfigure.foldLevel,
                         g_errorLogConfigure.foldThreshold, g_errorLogConfigure.foldPeriod);
}

UTILS_EXPORT void SetErrLogWriteMode(bool isBatchWrite)
{
    g_errorLogConfigure.isBatchWrite = isBatchWrite;
}

UTILS_EXPORT bool IsErrLogBatchWrite(void)
{
    return g_errorLogConfigure.isBatchWrite;
}

UTILS_EXPORT bool SetErrLogFlowConfig(uint64_t flowControlThreshold, int filterLevel, uint64_t logStatPeriod)
{
    if (g_loggerThreadContext.logFlowContext != NULL &&
        !LogFlowSetRule(g_loggerThreadContext.logFlowContext, flowControlThreshold, filterLevel, logStatPeriod)) {
        return false;
    }
    g_errorLogConfigure.flowControlThreshold = flowControlThreshold;
    g_errorLogConfigure.filterLevel = filterLevel;
    g_errorLogConfigure.logStatPeriod = logStatPeriod;
    return true;
}

UTILS_EXPORT void SetErrLogRedirect(void)
{
    g_errorLogConfigure.isNeedRedirect = true;
}

UTILS_EXPORT bool IsErrLogNeedRedirect(void)
{
    return g_errorLogConfigure.isNeedRedirect;
}

UTILS_EXPORT void SetErrorLogCompress(bool enableCompress)
{
    g_errorLogConfigure.enableCompress = enableCompress;
}

UTILS_EXPORT bool GetErrorLogCompress(void)
{
    return g_errorLogConfigure.enableCompress;
}

UTILS_EXPORT void SetErrLogLocalTime(struct pg_tz* logLocalTime)
{
    g_errorLogConfigure.logLocalTime = logLocalTime;
}

UTILS_EXPORT struct pg_tz* GetErrLogLocalTime(void)
{
    return g_errorLogConfigure.logLocalTime;
}

UTILS_EXPORT void ResetErrLogFlowConfig(void)
{
    g_errorLogConfigure.filterLevel = 0;
    g_errorLogConfigure.flowControlThreshold = 0;
    g_errorLogConfigure.logStatPeriod = 0;
    (void)LogFlowSetRule(g_loggerThreadContext.logFlowContext, g_errorLogConfigure.flowControlThreshold,
                         g_errorLogConfigure.filterLevel, g_errorLogConfigure.logStatPeriod);
}

#define ERROR_LOG_FILE_ROTATION_AGE            (24 * 60)
#define ERROR_LOG_DEFAULT_FILE_ROTATION_SIZE   (64 * 1024)
#define ERROR_LOG_DEFAULT_DIRECTORY_SPACE_SIZE (ERROR_LOG_DEFAULT_FILE_ROTATION_SIZE * 20)

typedef struct ErrorLogLocalFileConfigure ErrorLogLocalFileConfigure;
struct ErrorLogLocalFileConfigure {
    uint64_t errorLogLocalFileCfgSeq; /* Use to update local file configure during running time */
    char localErrorLogDirectory[MAX_PATH];
    char localPLogDirectory[MAX_PATH];
    char localSlowQueryLogDirectory[MAX_PATH];
    char localAspLogDirectory[MAX_PATH];
    char localFFICLogDirectory[MAX_PATH];
    uint32_t errorLogTotalSpace;       /* Total error log space in KB */
    uint32_t errorLogFileRotationSize; /* Max error log file size in KB */
    uint32_t errorLogFileRotationTime; /* Max error log file lifetime in minutes */
    int errorLogFileMode;
    ErrLogComponentFilterContext componentContextFilter;
    int defaultComponentModuleFilterLevel;
};

static ErrorLogLocalFileConfigure g_errorLogLocalFileCfg = {
    .errorLogLocalFileCfgSeq = 0,
    .localErrorLogDirectory = ERROR_LOG_DEFAULT_DIRECTORY,
    .localPLogDirectory = PROFILE_LOG_DEFAULT_DIRECTORY,
    .localAspLogDirectory = ASP_LOG_DEFAULT_DIRECTORY,
    .localFFICLogDirectory = FFIC_LOG_DEFAULT_DIRECTORY,
    .errorLogTotalSpace = ERROR_LOG_DEFAULT_DIRECTORY_SPACE_SIZE,
    .errorLogFileRotationSize = ERROR_LOG_DEFAULT_FILE_ROTATION_SIZE,
    .errorLogFileRotationTime = ERROR_LOG_FILE_ROTATION_AGE,
    .errorLogFileMode = FILE_READ_AND_WRITE_MODE,
    .componentContextFilter = {0},
    .defaultComponentModuleFilterLevel = WARNING,
};

/*
 * Reset error log module context filter.
 */
UTILS_EXPORT void ResetErrLogModuleContextFilter(void)
{
    g_errorLogLocalFileCfg.componentContextFilter.isFilterStarted = false;
    for (int i = 0; i < MAX_COMPONENT_FILTER_COUNT; i++) {
        g_errorLogLocalFileCfg.componentContextFilter.moduleContextFilter[i].moduleNum = 0;
    }
}

/*
 * Set the error log module context filter.
 * WARNING: The moduleId and errorLevel are the list of module id and error level,
 * there is a one-to-one correlation between them. In addition, the number of
 * moduleId and errorLevel need to be equal to the value of count.
 * @param componentId  : Component id.
 * @param moduleId     : Module id.
 * @param errorLevel   : Error level.
 * @param count        : the number of module id and error level pair.
 */
UTILS_EXPORT void SetErrLogModuleContextFilter(int componentId, const int *moduleId, const int *errorLevel, int count)
{
    if (errorLevel == NULL) {
        WriteErrMsg("The input error level list is NULL.\n");
        return;
    }

    if (componentId >= MAX_COMPONENT_FILTER_COUNT || componentId < 0) {
        WriteErrMsg("The value of component id is invalid.\n");
        return;
    }
    int preModuleFilterCount = g_errorLogLocalFileCfg.componentContextFilter.moduleContextFilter[componentId].moduleNum;

    int curModuleFilterCount = preModuleFilterCount + count;
    if (curModuleFilterCount > MAX_MODULE_FILTER_COUNT) {
        WriteErrMsg("The number of module filter exceeds the maximum.\n");
        return;
    }

    ErrLogComponentFilterContext *componentFilter = &g_errorLogLocalFileCfg.componentContextFilter;
    ErrLogModuleFilterContext *moduleContextFilter = &componentFilter->moduleContextFilter[componentId];

    for (int i = moduleContextFilter->moduleNum; i < moduleContextFilter->moduleNum + count; i++) {
        if (moduleId == NULL) {
            moduleContextFilter->moduleLevelPair[0].moduleId = DEFAULT_MODULE_ID;
            moduleContextFilter->moduleLevelPair[0].errorLevel = errorLevel[0];
            ASSERT(count == 1);
        } else {
            moduleContextFilter->moduleLevelPair[i].moduleId = moduleId[i];
            moduleContextFilter->moduleLevelPair[i].errorLevel = errorLevel[i];
        }
    }
    moduleContextFilter->componentId = componentId;
    moduleContextFilter->moduleNum += count;
    componentFilter->isFilterStarted = true;
}

static bool InitLoggerContext(void)
{
    /* Initial log thread context. */
    g_loggerThreadContext.logContext = MemoryContextCreate(NULL, MEM_CXT_TYPE_SHARE, "LoggerThreadContext", MCTX_UNUSED,
                                                           MCTX_UNUSED, LOGGERTHREADCONTEXT_DATA_STRUCT_MAXSIZE);
    /* Initial error log context. */
    g_loggerThreadContext.errorLogContext = MemoryContextCreate(NULL, MEM_CXT_TYPE_SHARE, "LoggerThreadErrorLogContext",
                                                                MCTX_UNUSED, MCTX_UNUSED, LOG_FOLD_MAXSIZE);
    /* Initial pipe error message context. */
    g_loggerThreadContext.pipeErrorMsgContext =
        MemoryContextCreate(NULL, MEM_CXT_TYPE_SHARE, "LoggerThreadPipeErrorMsgContext", MCTX_UNUSED, MCTX_UNUSED,
                            ERRLOG_THREAD_MESSAGE_MAX_SIZE_UNLIMIT);
    if ((g_loggerThreadContext.logContext == NULL) || (g_loggerThreadContext.errorLogContext == NULL)) {
        return false;
    }
    MemoryContextSetSilent(g_loggerThreadContext.logContext, true);
    MemoryContextSetSilent(g_loggerThreadContext.errorLogContext, true);
    return true;
}

static void FreeLoggerThreadContext(LoggerThreadContext *context)
{
    if (context->pipeErrorMsgContext != NULL) {
        MemoryContextDelete(context->pipeErrorMsgContext);
        context->pipeErrorMsgContext = NULL;
    }
    if (context->errorLogContext != NULL) {
        MemoryContextDelete(context->errorLogContext);
        context->errorLogContext = NULL;
    }
    if (context->logContext != NULL) {
        MemoryContextDelete(context->logContext);
        context->logContext = NULL;
    }
}

/* Set the local file system for log. */
static void SetLogLocalVfs(VirtualFileSystem *vfs)
{
    g_loggerThreadContext.localVfs = vfs;
}

/**
 * Get the error log file destination.
 * @return
 */
uint32_t GetErrorLogFileDestination(void)
{
    return g_loggerThreadContext.errorLogFileDestination;
}

/**
 * Set the error log destination.
 */
void SetErrorLogFileDestination(uint32_t errorLogDestination)
{
    g_loggerThreadContext.errorLogFileDestination = errorLogDestination;
}

/**
 * Get the error log error message destination.
 * @return
 */
bool IsLogThreadErrSyslogWrite(void)
{
    return g_loggerThreadContext.isLogThreadErrSyslogWrite;
}

static void SetErrorLogSignalUsage(bool isUseSignal)
{
    g_loggerThreadContext.isUseSignal = isUseSignal;
}

bool GetErrorLogSignalUsage(void)
{
    return g_loggerThreadContext.isUseSignal;
}

/* Set the remote file system for log. */
UTILS_EXPORT void SetLogRemoteVfs(VirtualFileSystem *remoteVfs,
                                  ErrorCode (*getLogFileIdFun)(char *fileName, const char *storeSpaceName),
                                  char **(*getLogFileFun)(int *count), void (*freeLogFileFun)(char **fileName),
                                  void (*removeLogFileIdFun)(char *fileName))
{
    g_loggerThreadContext.remoteVfs = remoteVfs;
    g_loggerThreadContext.getLogFileIdFun = getLogFileIdFun;
    g_loggerThreadContext.getLogFileFun = getLogFileFun;
    g_loggerThreadContext.freeLogFileFun = freeLogFileFun;
    g_loggerThreadContext.removeLogFileIdFun = removeLogFileIdFun;
}

#define PGINVALID_SOCKET (-1)

#define WL_LATCH_SET        (1U << 0)
#define WL_SOCKET_READABLE  (1U << 1)
#define WL_SOCKET_WRITEABLE (1U << 2)
#define WL_TIMEOUT          (1U << 3)

static void InstrTimeSubtract(TimeValue curTime, TimeValue startTime)
{
#define USEC_PER_SEC 1000000
    curTime.seconds -= startTime.seconds;
    curTime.useconds -= startTime.useconds;
    /* Normalize */
    while (curTime.useconds < 0) {
        curTime.useconds += USEC_PER_SEC;
        curTime.seconds--;
    }
}

/* Send one byte to the self-pipe, to wake up WaitLatch */
static void SendSelfPipeByte(void)
{
    int rc;
    char dummy = 0;

    while (true) {
        rc = (int)write(g_loggerThreadContext.selfPipeWriteFd, &dummy, 1);
        if (rc < 0) {
            /* If interrupted by signal, just retry */
            if (errno == EINTR) {
                continue;
            }
        }
        return;
    }
}

static void ClearSelfPipe(void)
{
    /*
     * There shouldn't normally be more than one byte in the pipe, or maybe a
     * few bytes if multiple processes run SetLatch at the same instant.
     */
    char buf[16];
    ssize_t rc;

    for (;;) {
        rc = read(g_loggerThreadContext.selfPipeReadFd, buf, sizeof(buf));
        if (rc < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            } else if (errno == EINTR) {
                continue;
            } else {
                g_loggerThreadContext.wait = false;
                ErrLog(ERROR, ErrMsg("read() on self-pipe failed.\n"));
            }
        } else if (rc == 0) {
            g_loggerThreadContext.wait = false;
            ErrLog(ERROR, ErrMsg("Unexpected EOF on self-pipe.\n"));
        } else if (rc < (int)sizeof(buf)) {
            /* we successfully drained the pipe; no need to read() again */
            break;
        }
        /* else buffer wasn't big enough, so read again */
    }
}

/*
 * Initialize the process-local latch infrastructure.
 *
 * This must be called once during startup of any process that can wait on
 * latches, before it issues any InitLatch() or OwnLatch() calls.
 */
void InitializeLatchSupport(void)
{
    int pipefd[2];

    ASSERT(g_loggerThreadContext.selfPipeReadFd == -1);

    if (pipe(pipefd) < 0) {
        ErrLog(FATAL, ErrMsg("pipe() failed.\n"));
    }

    if (fcntl(pipefd[0], F_SETFL, O_NONBLOCK) < 0) {
        ErrLog(FATAL, ErrMsg("fcntl() failed on read-end of self-pipe.\n"));
    }

    if (fcntl(pipefd[1], F_SETFL, O_NONBLOCK) < 0) {
        ErrLog(FATAL, ErrMsg("fcntl() failed on write-end of self-pipe.\n"));
    }

    g_loggerThreadContext.selfPipeReadFd = pipefd[0];
    g_loggerThreadContext.selfPipeWriteFd = pipefd[1];
}

/*
 * Initialize a backend-local latch.
 */
void InitLatch(volatile Latch *latch)
{
    /* Assert InitializeLatchSupport has been called in this process */
    ASSERT(g_loggerThreadContext.selfPipeReadFd >= 0);

    latch->isSet = false;
    latch->ownerPid = GetCurrentTid();
    latch->is_shared = false;
}

void SetLatch(volatile Latch *latch)
{
    Tid ownerPid;

    /* Quick exit if already set */
    if (latch->isSet) {
        return;
    }

    latch->isSet = true;

    ownerPid = latch->ownerPid;
    if (ownerPid.tid == 0) {
        return;
    } else if (ownerPid.tid == GetCurrentTid().tid) {
        if (g_loggerThreadContext.wait) {
            SendSelfPipeByte();
        }
    } else {
        if (GetErrorLogSignalUsage()) {
            (void)pthread_kill(ownerPid.tid, SIG_USR1);
        }
    }
}

/*
 * Clear the latch. Calling WaitLatch after this will sleep, unless
 * the latch is set again before the WaitLatch call.
 */
void ResetLatch(volatile Latch *latch)
{
    /* Only the owner should reset the latch */
    ASSERT(latch->ownerPid.tid == GetCurrentTid().tid);

    latch->isSet = false;
}

static uint32_t PollAndGetResult(struct pollfd *pollFd, nfds_t nfdIndex, long curTimeout, uint32_t curWakeEventsStatus)
{
    uint32_t result = 0;
    /* Sleep */
    int rc = poll(pollFd, nfdIndex, (int)curTimeout);
    /* Check return code */
    if (rc < 0) {
        /* EINTR is okay, otherwise complain */
        if (errno != EINTR) {
            g_loggerThreadContext.wait = false;
            ErrLog(ERROR, ErrMsg("poll() failed.\n"));
        }
    } else if (rc == 0) {
        /* timeout exceeded */
        if ((unsigned int)curWakeEventsStatus & WL_TIMEOUT) {
            result |= WL_TIMEOUT;
        }
    } else {
        /* at least one event occurred, so check revents values */
        if (((unsigned int)curWakeEventsStatus & WL_SOCKET_READABLE) && ((unsigned short)pollFd[0].revents & POLLIN)) {
            /* data available in socket, or EOF/error condition */
            result |= WL_SOCKET_READABLE;
        }
        if (((unsigned int)curWakeEventsStatus & WL_SOCKET_WRITEABLE) &&
            ((unsigned short)pollFd[0].revents & POLLOUT)) {
            /* socket is writable */
            result |= WL_SOCKET_WRITEABLE;
        }
        if ((unsigned short)pollFd[0].revents & (POLLHUP | POLLERR | POLLNVAL)) {
            /* EOF/error condition */
            if ((unsigned int)curWakeEventsStatus & WL_SOCKET_READABLE) {
                result |= WL_SOCKET_READABLE;
            }
            if ((unsigned int)curWakeEventsStatus & WL_SOCKET_WRITEABLE) {
                result |= WL_SOCKET_WRITEABLE;
            }
        }
    }
    return result;
}

/*
 * Like WaitLatch, but with an extra socket argument for WL_SOCKET_*
 * conditions.
 *
 * When wait on a socket, EOF and error conditions are reported by
 * returning the socket as readable/writable or both, depending on
 * WL_SOCKET_READABLE/WL_SOCKET_WRITEABLE being specified.
 */
uint32_t WaitLatchOrSocket(volatile Latch *latch, uint32_t wakeEventsStatus, int sock, long timeout)
{
    uint32_t result = 0;
    TimeValue startTime;
    TimeValue curTime;
    long curTimeout;

    struct pollfd pfdList[3];
    nfds_t nfdIndex;
    uint32_t curWakeEventsStatus = wakeEventsStatus;

    /* Ignore WL_SOCKET_* events if no valid socket is given */
    if (sock == PGINVALID_SOCKET) {
        curWakeEventsStatus &= ~(WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE);
    }

    ASSERT(curWakeEventsStatus != 0); /* must have at least one wake event */

    if ((curWakeEventsStatus & WL_LATCH_SET) && latch->ownerPid.tid != GetCurrentTid().tid) {
        ErrLog(PANIC, ErrMsg("Cannot wait on a latch owned by another process.\n"));
    }

    /*
     * Initialize timeout if requested.  We must record the current time so
     * that we can determine the remaining timeout if the poll() or select()
     * is interrupted.	(On some platforms, select() will update the contents
     * of "tv" for us, but unfortunately we can't rely on that.)
     */
    if (curWakeEventsStatus & WL_TIMEOUT) {
        startTime = GetCurrentTimeValue();
        ASSERT(timeout >= 0);
        curTimeout = timeout;
    } else {
        curTimeout = -1;
    }

    g_loggerThreadContext.wait = true;
    do {
        ClearSelfPipe();

        if ((curWakeEventsStatus & WL_LATCH_SET) && latch->isSet) {
            result |= WL_LATCH_SET;

            /*
             * Leave loop immediately, avoid blocking again. We don't attempt
             * to report any other events that might also be satisfied.
             */
            break;
        }

        /*
         * Must wait ... we use poll(2) if available, otherwise select(2).
         */
        nfdIndex = 0;
        if (curWakeEventsStatus & (WL_SOCKET_READABLE | WL_SOCKET_WRITEABLE)) {
            /* socket, if used, is always in pfdList[0] */
            pfdList[0].fd = sock;
            uint16_t events = 0;
            if (curWakeEventsStatus & WL_SOCKET_READABLE) {
                events |= POLLIN;
            }
            if (curWakeEventsStatus & WL_SOCKET_WRITEABLE) {
                events |= POLLOUT;
            }
            pfdList[0].events = (short)events;
            pfdList[0].revents = 0;
            nfdIndex++;
        }

        pfdList[nfdIndex].fd = g_loggerThreadContext.selfPipeReadFd;
        pfdList[nfdIndex].events = POLLIN;
        pfdList[nfdIndex].revents = 0;
        nfdIndex++;

        /* If we're not done, update curTimeout for next iteration */
        result |= PollAndGetResult(pfdList, nfdIndex, curTimeout, curWakeEventsStatus);
        if (result == 0 && curTimeout >= 0) {
            curTime = GetCurrentTimeValue();
            InstrTimeSubtract(curTime, startTime);
#define TIME_TRANS_GAP 1000
            curTimeout = timeout - (long)(((double)curTime.seconds * TIME_TRANS_GAP) +
                                          ((double)curTime.useconds) / TIME_TRANS_GAP);
            if (curTimeout < 0) {
                curTimeout = 0;
            } else if (curTimeout > timeout) {
                curTimeout = timeout;
            }
        }
    } while (result == 0);
    g_loggerThreadContext.wait = false;

    return result;
}

/*
 * Get the error log output directory.
 */
UTILS_EXPORT void GetErrLogDirectory(char *logDirectory, Size len)
{
    errno_t rc = strcpy_s(logDirectory, len, g_loggerThreadContext.errorLogFileConfig.logDirectory);
    SecurecCheck(rc, false);
}

/*
 * Get the profile log output directory.
 */
UTILS_EXPORT void GetPLogDirectory(char *logDirectory, Size len)
{
    errno_t rc = strcpy_s(logDirectory, len, g_loggerThreadContext.errorLogFileConfig.pLogDirectory);
    SecurecCheck(rc, false);
}

/*
 * Get the slow query log output directory.
 */
UTILS_EXPORT void GetSlowQueryLogDirectory(char *logDirectory, Size len)
{
    errno_t rc = strcpy_s(logDirectory, len, g_loggerThreadContext.errorLogFileConfig.slowQueryLogDirectory);
    SecurecCheck(rc, false);
}

/*
 * Get the asp log output directory.
 */
UTILS_EXPORT void GetAspLogDirectory(char *logDirectory, Size len)
{
    errno_t rc = strcpy_s(logDirectory, len, g_loggerThreadContext.errorLogFileConfig.aspLogDirectory);
    SecurecCheck(rc, false);
}

/*
 * Set the log output directory.
 * @param logDirectory : Error log directory.
 */
UTILS_EXPORT void SetErrLogDirectory(const char *logDirectory)
{
    if (logDirectory == NULL) {
        ErrLog(ERROR, ErrMsg("The error log directory pointer is NULL"));
        return;
    }
    if (strlen(logDirectory) > MAX_PATH - 1) {
        ErrLog(ERROR, ErrMsg("The length of error log directory is too long"));
        return;
    }
    if (strcmp(g_errorLogLocalFileCfg.localErrorLogDirectory, logDirectory) == 0) {
        return;
    }
    errno_t ret = strcpy_s(g_errorLogLocalFileCfg.localErrorLogDirectory,
                           sizeof(g_errorLogLocalFileCfg.localErrorLogDirectory), logDirectory);
    if (ret != EOK) {
        ErrLog(ERROR, ErrMsg("Set error log directory [%s] failed\n", logDirectory));
        return;
    }
    g_errorLogLocalFileCfg.errorLogLocalFileCfgSeq += 1;
    SetLatch(&(g_loggerThreadContext.errorLogLatch));
}

/*
 * Set the Plog output directory.
 * @param logDirectory : Plog directory.
 */
UTILS_EXPORT void SetPLogDirectory(const char *logDirectory)
{
    if (logDirectory == NULL) {
        ErrLog(ERROR, ErrMsg("The plog directory pointer is NULL"));
        return;
    }
    if (strlen(logDirectory) > MAX_PATH - 1) {
        ErrLog(ERROR, ErrMsg("The length of plog directory is too long"));
        return;
    }
    if (strcmp(g_errorLogLocalFileCfg.localPLogDirectory, logDirectory) == 0) {
        return;
    }
    errno_t ret = strcpy_s(g_errorLogLocalFileCfg.localPLogDirectory, sizeof(g_errorLogLocalFileCfg.localPLogDirectory),
                           logDirectory);
    if (ret != EOK) {
        ErrLog(ERROR, ErrMsg("Set error log directory [%s] failed\n", logDirectory));
        return;
    }
    g_errorLogLocalFileCfg.errorLogLocalFileCfgSeq += 1;
    SetLatch(&(g_loggerThreadContext.errorLogLatch));
}

/*
 * Set the slow query log output directory.
 * @param logDirectory : Slow query log directory.
 */
UTILS_EXPORT void SetSlowQueryLogDirectory(const char *logDirectory)
{
    if (logDirectory == NULL) {
        ErrLog(ERROR, ErrMsg("The slow query log directory pointer is NULL"));
        return;
    }
    if (strlen(logDirectory) > MAX_PATH - 1) {
        ErrLog(ERROR, ErrMsg("The length of slow query log directory is too long"));
        return;
    }
    if (strcmp(g_errorLogLocalFileCfg.localSlowQueryLogDirectory, logDirectory) == 0) {
        return;
    }
    errno_t ret = strcpy_s(g_errorLogLocalFileCfg.localSlowQueryLogDirectory,
                           sizeof(g_errorLogLocalFileCfg.localSlowQueryLogDirectory), logDirectory);
    if (ret != EOK) {
        ErrLog(ERROR, ErrMsg("Set error log directory [%s] failed\n", logDirectory));
        return;
    }
    g_errorLogLocalFileCfg.errorLogLocalFileCfgSeq += 1;
    SetLatch(&(g_loggerThreadContext.errorLogLatch));
}

/*
 * Set the asp log output directory.
 * @param logDirectory : asp log directory.
 */
UTILS_EXPORT void SetAspLogDirectory(const char *logDirectory)
{
    if (logDirectory == NULL) {
        ErrLog(ERROR, ErrMsg("The asp log directory pointer is NULL"));
        return;
    }
    if (strlen(logDirectory) > MAX_PATH - 1) {
        ErrLog(ERROR, ErrMsg("The length of asp log directory is too long"));
        return;
    }
    if (strcmp(g_errorLogLocalFileCfg.localAspLogDirectory, logDirectory) == 0) {
        return;
    }
    errno_t ret = strcpy_s(g_errorLogLocalFileCfg.localAspLogDirectory,
                           sizeof(g_errorLogLocalFileCfg.localAspLogDirectory), logDirectory);
    if (ret != EOK) {
        ErrLog(ERROR, ErrMsg("Set error log directory [%s] failed\n", logDirectory));
        return;
    }
    g_errorLogLocalFileCfg.errorLogLocalFileCfgSeq += 1;
    SetLatch(&(g_loggerThreadContext.errorLogLatch));
}

/*
 * Set the ffic log output directory.
 * @param logDirectory : ffic log directory.
 */
UTILS_EXPORT void SetFFICLogDirectory(const char *logDirectory)
{
    if (logDirectory == NULL) {
        ErrLog(ERROR, ErrMsg("The ffic log directory pointer is NULL"));
        return;
    }
    if (strlen(logDirectory) > MAX_PATH - 1) {
        ErrLog(ERROR, ErrMsg("The length of ffic log directory is too long"));
        return;
    }
    if (strcmp(g_errorLogLocalFileCfg.localFFICLogDirectory, logDirectory) == 0) {
        return;
    }
    errno_t ret = strcpy_s(g_errorLogLocalFileCfg.localFFICLogDirectory,
                           sizeof(g_errorLogLocalFileCfg.localFFICLogDirectory), logDirectory);
    if (ret != EOK) {
        ErrLog(ERROR, ErrMsg("Set error log directory [%s] failed\n", logDirectory));
        return;
    }
    g_errorLogLocalFileCfg.errorLogLocalFileCfgSeq += 1;
    SetLatch(&(g_loggerThreadContext.errorLogLatch));
}

UTILS_EXPORT void ResetErrLogDirectory(void)
{
    SetErrLogDirectory(ERROR_LOG_DEFAULT_DIRECTORY);
}

UTILS_EXPORT void ResetPLogDirectory(void)
{
    SetPLogDirectory(PROFILE_LOG_DEFAULT_DIRECTORY);
}

UTILS_EXPORT void ResetAspLogDirectory(void)
{
    SetAspLogDirectory(ASP_LOG_DEFAULT_DIRECTORY);
}

static inline bool CheckErrLogSizeParameters(uint32_t totalErrLogSpaceSize, uint32_t spacePerErrLogFile)
{
    if (totalErrLogSpaceSize == 0 || spacePerErrLogFile == 0 || totalErrLogSpaceSize < spacePerErrLogFile) {
        return false;
    }
    return true;
}

UTILS_EXPORT void SetErrLogSpaceSize(uint32_t totalErrLogSpaceSize, uint32_t spacePerErrLogFile)
{
    if (unlikely(!CheckErrLogSizeParameters(totalErrLogSpaceSize, spacePerErrLogFile))) {
        WriteErrMsg("Set error log space {%u, %u} failed\n", totalErrLogSpaceSize, spacePerErrLogFile);
        return;
    }
    if (g_errorLogLocalFileCfg.errorLogTotalSpace == totalErrLogSpaceSize &&
        g_errorLogLocalFileCfg.errorLogFileRotationSize == spacePerErrLogFile) {
        return;
    }
    g_errorLogLocalFileCfg.errorLogTotalSpace = totalErrLogSpaceSize;
    g_errorLogLocalFileCfg.errorLogFileRotationSize = spacePerErrLogFile;
    g_errorLogLocalFileCfg.errorLogLocalFileCfgSeq += 1;
    SetLatch(&(g_loggerThreadContext.errorLogLatch));
}

UTILS_EXPORT void ResetErrLogSpaceSize(void)
{
    SetErrLogSpaceSize(ERROR_LOG_DEFAULT_DIRECTORY_SPACE_SIZE, ERROR_LOG_DEFAULT_FILE_ROTATION_SIZE);
}

UTILS_EXPORT void SetErrLogLocalFileMode(int mode)
{
    if (unlikely(!CheckFileModeValidation(mode))) {
        WriteErrMsg("Error log file mode %d is invalid\n", mode);
        return;
    }
    g_errorLogLocalFileCfg.errorLogFileMode = mode;
    g_errorLogLocalFileCfg.errorLogLocalFileCfgSeq += 1;
    SetLatch(&(g_loggerThreadContext.errorLogLatch));
}

UTILS_EXPORT bool EnableRemoteErrorLog(const ErrorLogRemoteFileConfigure *cfg)
{
    if (cfg == NULL || cfg->errorLogFileCount == 0 || cfg->errorLogFileCount > REMOTE_LOG_FILE_MAX_COUNT ||
        cfg->totalSpace / cfg->errorLogFileCount == 0 || cfg->remoteVfs == NULL) {
        return false;
    }
    if (!IsLoggerStarted()) {
        return false;
    }
    if (g_loggerThreadContext.remoteLogIsReady) {
        WriteErrMsg("Remote logger is already enabled\n");
        return false;
    }
    if (g_loggerThreadContext.remoteLogIsStopPending) {
        WriteErrMsg("Remote logger previous stopping is not completed\n");
        return false;
    }
    if (!PrepareRemoteLogResource(cfg)) {
        return false;
    }
    uint32_t logDestination = GetErrLogDestination();
    SetErrLogDestination(logDestination | LOG_DESTINATION_REMOTE_FILE);
    return true;
}

UTILS_EXPORT bool GetRemoteErrorLogConfigure(ErrorLogRemoteFileConfigure *cfg)
{
    if (cfg == NULL) {
        return false;
    }
    if (!IsLoggerStarted() || !g_loggerThreadContext.remoteLogIsReady || g_loggerThreadContext.remoteLogIsStopPending) {
        return false;
    }
    *cfg = g_loggerThreadContext.remoteLogContext.remoteFileCfg;
    return true;
}

UTILS_EXPORT void DisableRemoteErrorLog(void)
{
    if (!IsLoggerStarted()) {
        return;
    }
    if (!g_loggerThreadContext.remoteLogIsReady || g_loggerThreadContext.remoteLogIsStopPending) {
        return;
    }
    uint32_t logDestination = GetErrLogDestination();
    SetErrLogDestination(logDestination & (~LOG_DESTINATION_REMOTE_FILE));
    g_loggerThreadContext.remoteLogIsStopPending = true;
    SetLatch(&(g_loggerThreadContext.errorLogLatch));
}

static void SetLoggerThreadLocalFileCfgSeq(uint64_t seq)
{
    g_loggerThreadContext.errorLogLocalFileCfgSeq = seq;
}

#define ERROR_LOG_MIN_SEQUENCE_NO 2

static void SetErrLogMaxSeqNo(LogFileConfig *config)
{
    ASSERT(config != NULL);
    if (config->logFileRotationSize > 0) {
        uint32_t maxSequenceNo = config->logDirectorySpaceSize / config->logFileRotationSize;
        if (maxSequenceNo < ERROR_LOG_MIN_SEQUENCE_NO) {
            maxSequenceNo = ERROR_LOG_MIN_SEQUENCE_NO;
        }
        config->maxSequenceNo = maxSequenceNo;
    } else {
        config->maxSequenceNo = ERROR_LOG_MIN_SEQUENCE_NO;
    }
}

/*
 * Set the log output directory space size.
 * @param logDirectorySpaceSize : Error log directory space size.
 */
void SetErrLogDirectorySpaceSize(uint32_t logDirectorySpaceSize)
{
    g_loggerThreadContext.errorLogFileConfig.logDirectorySpaceSize = logDirectorySpaceSize;
    SetErrLogMaxSeqNo(&g_loggerThreadContext.errorLogFileConfig);
}

/*
 * Set some name in the errlog module, such as file name, process name.
 * @param context : memory context, it can't be a simple share context, this context had't release all
 *                  when delete context.
 */
static ErrorCode SetErrLogXXName(MemoryContext context, const char *name, char **dest)
{
    ASSERT(dest != NULL);
    ASSERT(name != NULL);
    size_t len = strlen(name) + 1;
    *dest = NULL;
    FAULT_INJECTION_ACTION(SET_ERRLOG_XXNAME_FAULT_INJECTION1, return ERROR_UTILS_COMMON_NOENOUGH_MEMORY);
    *dest = (char *)MemoryContextAlloc(context, len);
    if (*dest == NULL) {
        return ERROR_UTILS_COMMON_NOENOUGH_MEMORY;
    }
    /* according to securec specs, this situation will not return error */
    (void)memcpy_s(*dest, len, name, len);
    return ERROR_SYS_OK;
}

/*
 * Set the log output file mode.
 * @param logFileMode : Error log file mode.
 */
void SetErrLogFileMode(int logFileMode)
{
    g_loggerThreadContext.errorLogFileConfig.logFileMode = logFileMode;
}

/*
 * Get the log output file mode.
 */
static inline int GetErrLogFileMode(void)
{
    return g_loggerThreadContext.errorLogFileConfig.logFileMode;
}
/*
 * Set the log file rotation size.
 * @param logRotationSize : Error log file rotation size.
 */
void SetErrLogFileRotationSize(uint32_t logRotationSize)
{
    g_loggerThreadContext.errorLogFileConfig.logFileRotationSize = logRotationSize;
    SetErrLogMaxSeqNo(&g_loggerThreadContext.errorLogFileConfig);
}

/*
 * Get the error log max sequence no.
 */
uint32_t GetErrLogFileMaxSequenceNo(void)
{
    return g_loggerThreadContext.errorLogFileConfig.maxSequenceNo;
}

/*
 * Set the log file rotation age.
 * @param logRotationAge : Error log file rotation age.
 */
void SetErrLogFileRotationAge(uint32_t logRotationAge)
{
    g_loggerThreadContext.errorLogFileConfig.logFileRotationAge = logRotationAge;
}

/*
 * Set the error log output file.
 */
static void SetErrorLogOutputFile(void)
{
    uint32_t logFileDestination = GetErrorLogFileDestination();
    if ((logFileDestination & LOG_DESTINATION_LOCAL_FILE) != 0) {
        g_loggerThreadContext.errorLogOutputFile.vfsType = LOCAL_VIRTUAL_FILE_SYSTEM;
    }
    if ((logFileDestination & LOG_DESTINATION_REMOTE_FILE) != 0) {
        g_loggerThreadContext.errorLogOutputFile.vfsType = REMOTE_VIRTUAL_FILE_SYSTEM;
    }
    g_loggerThreadContext.errorLogFileConfig.fileType = ERR_LOG_FILE_TYPE;
}

static void SetErrorLogRedirectionDone(bool isRedirectionDone)
{
    g_loggerThreadContext.redirection_done = isRedirectionDone;
}

/*
 * IsLogLevelOutput -- is elevel logically >= logMinLevel?
 * We use this for tests that should consider LOG to sort out-of-order, between ERROR and FATAL.
 * Generally this is the right thing for testing whether a message should go to the server log,
 * whereas a simple >= test is correct for testing whether the message should go to the client.
 */
bool IsLogLevelOutput(int elevel, int logMinLevel)
{
    if (g_errorLogLocalFileCfg.componentContextFilter.isFilterStarted) {
        return true;
    }
    if ((elevel == LOG) && (logMinLevel <= ERROR)) {
        return true;
    }
    if (elevel >= logMinLevel) { /* Neither is LOG. */
        return true;
    }
    return false;
}

bool IsFilterStarted()
{
    return g_errorLogLocalFileCfg.componentContextFilter.isFilterStarted;
}

/*
 * IsLogModuleContextOutput -- is current module ErrLogModuleContext equals
 * filter ErrLogModuleContext ?
 */
bool IsLogModuleContextOutput(ErrLogModuleContext *currentModule)
{
    ErrorLogLocalFileConfigure *filterConfig = &g_errorLogLocalFileCfg;
    /* Component and module filter function is not open */
    if (!filterConfig->componentContextFilter.isFilterStarted) {
        return true;
    }

    int componentIndex = currentModule->componentId;
    /* Check the validation of component id */
    if (componentIndex >= MAX_COMPONENT_FILTER_COUNT || componentIndex < 0) {
        WriteErrMsg("The value of component id is invalid.\n");
        return true;
    }
    ErrLogModuleFilterContext *moduleContextFilter =
        &filterConfig->componentContextFilter.moduleContextFilter[componentIndex];

    for (int i = 0; i < moduleContextFilter->moduleNum; i++) {
        /* Not target component */
        if (moduleContextFilter->componentId != currentModule->componentId) {
            continue;
        }
        /* Only filter component */
        if (moduleContextFilter->moduleLevelPair[0].moduleId == DEFAULT_MODULE_ID &&
            moduleContextFilter->moduleLevelPair[0].errorLevel <= currentModule->errorLevel) {
            return true;
        }
        /* Filtering every matched module in targeted component */
        if (moduleContextFilter->moduleLevelPair[i].moduleId == currentModule->moduleId &&
            moduleContextFilter->moduleLevelPair[i].errorLevel <= currentModule->errorLevel) {
            return true;
        }
    }
    /* Not matched component and module filtered by default error level */
    return currentModule->errorLevel >= g_errorLogLocalFileCfg.defaultComponentModuleFilterLevel;
}

/**!
 * Initialize the error log configuration parameters.
 */
ErrorCode InitErrlogConfigParameters(void)
{
    /* Error log local file configure */
    ErrorCode errorCode = SetErrLogXXName(g_loggerThreadContext.errorLogContext, g_loggerThreadContext.processName,
                                          &g_loggerThreadContext.errorLogFileConfig.logFileName);
    uint64_t tmp = (uint64_t)errorCode;
    tmp |= (uint64_t)SetErrLogXXName(g_loggerThreadContext.errorLogContext, "slow_query_log",
                                     &g_loggerThreadContext.errorLogFileConfig.slowQueryFileName);
    tmp |= (uint64_t)SetErrLogXXName(g_loggerThreadContext.errorLogContext, "asp",
                                     &g_loggerThreadContext.errorLogFileConfig.aspFileName);
    errorCode = (ErrorCode)tmp;
    if (errorCode != ERROR_SYS_OK) {
        return errorCode;
    }
    FAULT_INJECTION_ACTION(SET_ERRLOG_XXNAME_FAULT_INJECTION2, return ERROR_UTILS_COMMON_NOENOUGH_MEMORY);
    errorCode = SetErrLogXXName(g_loggerThreadContext.errorLogContext, g_errorLogLocalFileCfg.localErrorLogDirectory,
                                &g_loggerThreadContext.errorLogFileConfig.logDirectory);
    tmp = (uint64_t)errorCode;
    tmp |= (uint64_t)SetErrLogXXName(g_loggerThreadContext.errorLogContext, g_errorLogLocalFileCfg.localPLogDirectory,
                                     &g_loggerThreadContext.errorLogFileConfig.pLogDirectory);

    tmp |= (uint64_t)SetErrLogXXName(g_loggerThreadContext.errorLogContext,
                                     g_errorLogLocalFileCfg.localSlowQueryLogDirectory,
                                     &g_loggerThreadContext.errorLogFileConfig.slowQueryLogDirectory);

    tmp |= (uint64_t)SetErrLogXXName(g_loggerThreadContext.errorLogContext, g_errorLogLocalFileCfg.localAspLogDirectory,
                                     &g_loggerThreadContext.errorLogFileConfig.aspLogDirectory);

    tmp |= (uint64_t)SetErrLogXXName(g_loggerThreadContext.errorLogContext,
                                     g_errorLogLocalFileCfg.localFFICLogDirectory,
                                     &g_loggerThreadContext.errorLogFileConfig.FFICLogDirectory);
    errorCode = (ErrorCode)tmp;
    if (errorCode != ERROR_SYS_OK) {
        return errorCode;
    }
    SetLoggerThreadLocalFileCfgSeq(g_errorLogLocalFileCfg.errorLogLocalFileCfgSeq);
    SetErrLogDirectorySpaceSize(g_errorLogLocalFileCfg.errorLogTotalSpace);
    SetErrLogFileMode(g_errorLogLocalFileCfg.errorLogFileMode);
    SetErrLogFileRotationAge(g_errorLogLocalFileCfg.errorLogFileRotationTime);
    SetErrLogFileRotationSize(g_errorLogLocalFileCfg.errorLogFileRotationSize);
    SetErrorLogFileDestination(LOG_DESTINATION_LOCAL_FILE);
    g_loggerThreadContext.isLogThreadErrSyslogWrite = true;
    SetErrorLogRedirectionDone(false);
    g_loggerThreadContext.wait = false;
    g_loggerThreadContext.selfPipeReadFd = -1;
    g_loggerThreadContext.selfPipeWriteFd = -1;
    SetErrorLogOutputFile();
    (void)memset_s(&g_errorLogLocalFileCfg.componentContextFilter, sizeof(ErrLogComponentFilterContext), 0,
                   sizeof(ErrLogComponentFilterContext));
    g_loggerThreadContext.isDiskFull = false;
    return ERROR_SYS_OK;
}

/* Initial the logger thread configure parameters. */
static inline ErrorCode InitLoggerThreadConfigParameters(void)
{
    char processName[MAX_PATH] = {0};
    ErrorCode errorCode = GetCurrentProcessName(processName, MAX_PATH);
    if (errorCode != ERROR_SYS_OK) {
        return errorCode;
    }
    errorCode = SetErrLogXXName(g_loggerThreadContext.logContext, processName, &g_loggerThreadContext.processName);
    if (errorCode != ERROR_SYS_OK) {
        return errorCode;
    }
    return InitErrlogConfigParameters();
}

/**
 * Obtain the file system corresponding to the vfs type.
 * @param vfsType
 * @return: VirtualFileSystem.
 */
static VirtualFileSystem *GetVfsByVfsType(int vfsType)
{
    VirtualFileSystem *vfs = NULL;
    switch (vfsType) {
        case LOCAL_VIRTUAL_FILE_SYSTEM:
            vfs = g_loggerThreadContext.localVfs;
            break;
        case REMOTE_VIRTUAL_FILE_SYSTEM:
            vfs = g_loggerThreadContext.remoteVfs;
            break;
        default:
            vfs = g_loggerThreadContext.localVfs;
            break;
    }
    return vfs;
}

/**
 * Obtain the file extend name corresponding to the file type.
 * @param fileType
 * @return:Extend file name.
 */
static char *GetExtendNameByFileType(int fileType)
{
    char *extendName = NULL;
    switch (fileType) {
        case ERR_LOG_FILE_TYPE:
            extendName = ERR_LOG_EXTEND;
            break;
        case AUDIT_LOG_FILE_TYPE:
            extendName = AUDIT_LOG_EXTEND;
            break;
        case SQL_LOG_FILE_TYPE:
            extendName = ERR_LOG_EXTEND;
            break;
        case ASP_LOG_FILE_TYPE:
            extendName = ERR_LOG_EXTEND;
            break;
        case PLOG_LOG_FILE_TYPE:
            extendName = PROFILE_LOG_EXTEND;
            break;
        case CSV_LOG_FILE_TYPE:
            extendName = CSV_LOG_EXTEND;
            break;
        default:
            extendName = ERROR_LOG_UNKNOWN_NAME;
            break;
    }
    return extendName;
}

#define MAX_LOG_FILE_SIZE (1024 * 1024 * 1024)

static bool GetFileFullPath(const FullPathName *fullPathName, char *str, size_t len)
{
    ssize_t rc;
    ssize_t count = 0;
    rc = sprintf_s(str, len, "%s/%s", fullPathName->directoryName, fullPathName->fileName);
    if (rc == -1) {
        ErrLog(ERROR, ErrMsg("sprintf_s failed, err: %ld\n", rc));
        return false;
    }
    count += rc;

    if (fullPathName->appendName != NULL && strlen(fullPathName->appendName) != 0) {
        rc = (ssize_t)sprintf_s(str + count, len - (size_t)count, "-%s", fullPathName->appendName);
        if (rc == -1) {
            ErrLog(ERROR, ErrMsg("sprintf_s failed, err: %ld\n", rc));
            return false;
        }
        count += rc;
    }

    rc = (ssize_t)sprintf_s(str + count, len - (size_t)count, ".%s", fullPathName->extendName);
    if (rc == -1) {
        ErrLog(ERROR, ErrMsg("sprintf_s failed, err: %ld\n", rc));
        return false;
    }

    return true;
}

static ErrorCode GetCurLogFileCreateTime(char *pathName, int fileType)
{
    struct stat file_stat;
    if (unlikely(stat(pathName, &file_stat) != 0)) {
        ErrorCode errorCode = ConvertLinuxSysErrCode(errno);
        ErrLog(ERROR, ErrMsg("Get file stat information failed, err: %lld\n", errorCode));
        return errorCode;
    }

    switch (fileType) {
        case ERR_LOG_FILE_TYPE:
            if (g_loggerThreadContext.errorLogOutputFile.lastLogTime.timeSeconds == 0) {
                g_loggerThreadContext.errorLogOutputFile.lastLogTime.timeSeconds = file_stat.st_ctime;
            }
            break;
        case PLOG_LOG_FILE_TYPE:
            if (g_loggerThreadContext.errorLogOutputFile.lastPLogTime.timeSeconds == 0) {
                g_loggerThreadContext.errorLogOutputFile.lastPLogTime.timeSeconds = file_stat.st_ctime;
            }
            break;
        case SQL_LOG_FILE_TYPE:
            if (g_loggerThreadContext.errorLogOutputFile.lastSlowQueryLogTime.timeSeconds == 0) {
                g_loggerThreadContext.errorLogOutputFile.lastSlowQueryLogTime.timeSeconds = file_stat.st_ctime;
            }
            break;
        case ASP_LOG_FILE_TYPE:
            if (g_loggerThreadContext.errorLogOutputFile.lastAspLogTime.timeSeconds == 0) {
                g_loggerThreadContext.errorLogOutputFile.lastAspLogTime.timeSeconds = file_stat.st_ctime;
            }
            break;
        case CSV_LOG_FILE_TYPE:
            if (g_loggerThreadContext.errorLogOutputFile.lastCsvLogTime.timeSeconds == 0) {
                g_loggerThreadContext.errorLogOutputFile.lastCsvLogTime.timeSeconds = file_stat.st_ctime;
            }
            break;
        default:
            break;
    }

    return ERROR_SYS_OK;
}

/* Open the log file. */
static ErrorCode OpenLogFile(int vfsType, VirtualFileSystem *vfs, const FullPathName *fullPathName, FileDescriptor **fd,
                             int fileType)
{
    char pathName[MAX_PATH] = {0};
    if (!GetFileFullPath(fullPathName, pathName, sizeof(pathName))) {
        return ERROR_UTILS_COMMON_SECURE_C_FUNCTION_FAILED;
    }

    bool exist;
    int flags = FILE_READ_AND_WRITE_FLAG | FILE_APPEND_FLAG;
    FileParameter fileParameter;
    ErrorLogRemoteFileConfigure *remoteFileCfg = &g_loggerThreadContext.remoteLogContext.remoteFileCfg;
    ErrorCode errorCode = ERROR_SYS_OK;
    int ret =
        strcpy_s(fileParameter.storeSpaceName, sizeof(fileParameter.storeSpaceName), remoteFileCfg->storeSpaceName);
    if (ret != EOK) {
        errorCode = VFS_ERROR_SECURE_FUNCTION_FAIL;
        return errorCode;
    }
    fileParameter.streamId = VFS_DEFAULT_FILE_STREAM_ID;
    fileParameter.flag = APPEND_WRITE_FILE;
    fileParameter.fileSubType = ERR_LOG_FILE_TYPE;
    fileParameter.rangeSize = DEFAULT_RANGE_SIZE;
    fileParameter.maxSize = MAX_LOG_FILE_SIZE;
    fileParameter.recycleTtl = 0;
    fileParameter.mode = GetErrLogFileMode();
    fileParameter.isReplayWrite = false;
    switch (vfsType) {
        /*
         * In the local file system, there is no mapping between file names and storeSpaceId,
         * and the upper layer does not register callback functions.
         */
        case LOCAL_VIRTUAL_FILE_SYSTEM:
            break;
        case REMOTE_VIRTUAL_FILE_SYSTEM:
            g_loggerThreadContext.getLogFileIdFun(pathName, fileParameter.storeSpaceName);
            break;
        default:
            break;
    }
    errorCode = FileIsExist(vfs, pathName, &exist);
    if (errorCode != ERROR_SYS_OK) {
        return errorCode;
    }
    if (!exist) {
        switch (fileType) {
            case ERR_LOG_FILE_TYPE:
                Time(&(g_loggerThreadContext.errorLogOutputFile.lastLogTime));
                break;
            case PLOG_LOG_FILE_TYPE:
                Time(&(g_loggerThreadContext.errorLogOutputFile.lastPLogTime));
                break;
            case SQL_LOG_FILE_TYPE:
                Time(&(g_loggerThreadContext.errorLogOutputFile.lastSlowQueryLogTime));
                break;
            case ASP_LOG_FILE_TYPE:
                Time(&(g_loggerThreadContext.errorLogOutputFile.lastAspLogTime));
                break;
            case CSV_LOG_FILE_TYPE:
                Time(&(g_loggerThreadContext.errorLogOutputFile.lastCsvLogTime));
                break;
            default:
                break;
        }
        FAULT_INJECTION_CALL_REPLACE(MOCK_OPENED_FILE_REACH_MAX, &errorCode);
        errorCode = Create(vfs, pathName, fileParameter, fd);
        FAULT_INJECTION_CALL_REPLACE_END;
        if (errorCode == VFS_ERROR_OPENED_FILE_REACH_MAX) {
            WriteErrMsg("%s, error code is %lld", GetVfsErrMsg(VFS_ERROR_OPENED_FILE_REACH_MAX), errorCode);
            ErrLog(PANIC, ErrMsg("Create file failed, error code is: %lld\n", errorCode));
            return errorCode;
        }
    } else {
        errorCode = GetCurLogFileCreateTime(pathName, fileType);
        if (errorCode != ERROR_SYS_OK) {
            ErrLog(ERROR, ErrMsg("Get current log file create time failed, err: %lld\n", errorCode));
            return errorCode;
        }
        errorCode = Open(vfs, pathName, flags, fd);
        if (errorCode == VFS_ERROR_OPENED_FILE_REACH_MAX) {
            WriteErrMsg("%s, error code is %lld", GetVfsErrMsg(VFS_ERROR_OPENED_FILE_REACH_MAX), errorCode);
            ErrLog(PANIC, ErrMsg("Open file failed, error code is: %lld\n", errorCode));
            return errorCode;
        }
    }
    return errorCode;
}

#define CURRENT_LOG_SUFFIX "current"

static ErrorCode RenameLogFileWhenDiskFull(void)
{
    ErrorCode errCode = ERROR_SYS_OK;
    char sourceErrLogFileName[MAX_PATH] = {0};
    char targetErrLogFileName[MAX_PATH] = {0};
    char *dirName = g_loggerThreadContext.errorLogFileConfig.logDirectory;
    char *fileName = g_loggerThreadContext.errorLogFileConfig.logFileName;
    char *appendName = CURRENT_LOG_SUFFIX;
    char *extendName = GetExtendNameByFileType(ERR_LOG_FILE_TYPE);
    int ret = sprintf_s(sourceErrLogFileName, sizeof(sourceErrLogFileName), "%s/%s-%s.%s", dirName,
                        fileName, appendName, extendName);
    if (unlikely(ret < 0)) {
        PosixErrorCode2PortErrorCode(ret, &errCode);
        WriteErrMsg("Fail to generate source error log file name, err: %lld.\n", errCode);

        return errCode;
    }

    ret = sprintf_s(targetErrLogFileName, sizeof(targetErrLogFileName), "%s/%s-%s.%s.%s", dirName,
                    fileName, appendName, extendName, "disk_full");
    if (unlikely(ret < 0)) {
        PosixErrorCode2PortErrorCode(ret, &errCode);
        WriteErrMsg("Fail to generate target error log file name, err: %lld.\n", errCode);

        return errCode;
    }

    if (!g_loggerThreadContext.isDiskFull) {
        ret = rename(sourceErrLogFileName, targetErrLogFileName);
        if (unlikely(ret < 0)) {
            PosixErrorCode2PortErrorCode(ret, &errCode);
            WriteErrMsg("Fail to rename error log file to no space log file, err: %lld.\n", errCode);

            return errCode;
        }
        g_loggerThreadContext.isDiskFull = true;
    } else {
        ret = rename(targetErrLogFileName, sourceErrLogFileName);
        if (unlikely(ret < 0)) {
            PosixErrorCode2PortErrorCode(ret, &errCode);
            WriteErrMsg("Fail to rename no space log file back to error log file, err: %lld.\n", errCode);

            return errCode;
        }
        g_loggerThreadContext.isDiskFull = false;
    }

    return errCode;
}

/**
 * Open the log data file.
 * @param logFileConfig: LogFileConfig.
 * @param logOutputFile: LogOutputFile.
 * @return: ErrorCode.
 */
static ErrorCode OpenLogDataFile(LogFileConfig *logFileConfig, LogOutputFile *logOutputFile)
{
    FullPathName fullPathName;
    fullPathName.directoryName = logFileConfig->logDirectory;
    fullPathName.fileName = logFileConfig->logFileName;
    fullPathName.appendName = CURRENT_LOG_SUFFIX;
    fullPathName.extendName = GetExtendNameByFileType(ERR_LOG_FILE_TYPE);
    ErrorCode errorCode = ERROR_SYS_OK;
    FAULT_INJECTION_CALL_REPLACE(MOCK_LOG_DISK_FULL, &errorCode);
    errorCode = OpenLogFile(logOutputFile->vfsType, GetVfsByVfsType(logOutputFile->vfsType), &fullPathName,
                            &(logOutputFile->dataFd), ERR_LOG_FILE_TYPE);
    FAULT_INJECTION_CALL_REPLACE_END;
    if unlikely(((errorCode == VFS_ERROR_DISK_HAS_NO_SPACE && !g_loggerThreadContext.isDiskFull) ||
                 (errorCode == ERROR_SYS_OK && g_loggerThreadContext.isDiskFull))) {
        errorCode = RenameLogFileWhenDiskFull();
    }

    return errorCode;
}

/**
 * Open the profile log file.
 * @param logFileConfig: LogFileConfig.
 * @param logOutputFile: LogOutputFile.
 * @return: ErrorCode.
 */
static ErrorCode OpenPLogFile(LogFileConfig *logFileConfig, LogOutputFile *logOutputFile)
{
    FullPathName fullPathName;
    fullPathName.directoryName = logFileConfig->pLogDirectory;
    fullPathName.fileName = logFileConfig->logFileName;
    fullPathName.appendName = CURRENT_LOG_SUFFIX;
    fullPathName.extendName = GetExtendNameByFileType(PLOG_LOG_FILE_TYPE);
    return OpenLogFile(logOutputFile->vfsType, GetVfsByVfsType(logOutputFile->vfsType), &fullPathName,
                       &(logOutputFile->profileFd), PLOG_LOG_FILE_TYPE);
}

/**
 * Open the slow query log file.
 * @param logFileConfig: LogFileConfig.
 * @param logOutputFile: LogOutputFile.
 * @return: ErrorCode.
 */
static ErrorCode OpenSlowQueryLogFile(LogFileConfig *logFileConfig, LogOutputFile *logOutputFile)
{
    FullPathName fullPathName;
    fullPathName.directoryName = logFileConfig->slowQueryLogDirectory;
    fullPathName.fileName = logFileConfig->slowQueryFileName;
    fullPathName.appendName = CURRENT_LOG_SUFFIX;
    fullPathName.extendName = GetExtendNameByFileType(SQL_LOG_FILE_TYPE);
    return OpenLogFile(logOutputFile->vfsType, GetVfsByVfsType(logOutputFile->vfsType), &fullPathName,
                       &(logOutputFile->sqlFd), SQL_LOG_FILE_TYPE);
}

/**
 * Open the asp log file.
 * @param logFileConfig: LogFileConfig.
 * @param logOutputFile: LogOutputFile.
 * @return: ErrorCode.
 */
static ErrorCode OpenAspLogFile(LogFileConfig *logFileConfig, LogOutputFile *logOutputFile)
{
    FullPathName fullPathName;
    fullPathName.directoryName = logFileConfig->aspLogDirectory;
    fullPathName.fileName = logFileConfig->aspFileName;
    fullPathName.appendName = CURRENT_LOG_SUFFIX;
    fullPathName.extendName = GetExtendNameByFileType(ASP_LOG_FILE_TYPE);
    return OpenLogFile(logOutputFile->vfsType, GetVfsByVfsType(logOutputFile->vfsType), &fullPathName,
                       &(logOutputFile->aspFd), ASP_LOG_FILE_TYPE);
}

/**
 * Open the csv log file.
 * @param logFileConfig: LogFileConfig.
 * @param logOutputFile: LogOutputFile.
 * @return: ErrorCode.
 */
static ErrorCode OpenCsvLogFile(LogFileConfig *logFileConfig, LogOutputFile *logOutputFile)
{
    FullPathName fullPathName;
    fullPathName.directoryName = logFileConfig->logDirectory;
    fullPathName.fileName = logFileConfig->logFileName;
    fullPathName.appendName = CURRENT_LOG_SUFFIX;
    fullPathName.extendName = GetExtendNameByFileType(CSV_LOG_FILE_TYPE);
    return OpenLogFile(logOutputFile->vfsType, GetVfsByVfsType(logOutputFile->vfsType), &fullPathName,
                       &(logOutputFile->csvFd), CSV_LOG_FILE_TYPE);
}

static inline bool RemoteLogIsReady(void)
{
    if ((GetErrLogDestination() & LOG_DESTINATION_REMOTE_FILE) == 0) {
        return false;
    }
    if (!g_loggerThreadContext.remoteLogIsReady) {
        return false;
    }
    return true;
}

static bool WriteRemoteLogDataFile(FileDescriptor *fd, const void *buf, size_t len)
{
    int64_t writeSize = 0;
    int64_t filePos;
    ErrorCode errorCode = FileSeek(fd, 0, FILE_SEEK_END, &filePos);
    if (errorCode != ERROR_SYS_OK) {
        return false;
    }
    /* WARNING: Only synchronous interface is allowed, due to the current log dead loop check method and solution. */
    FAULT_INJECTION_CALL_REPLACE(MOCK_REMOTE_LOG_DATA_FILE, &errorCode)
    errorCode = WriteSync(fd, buf, len, &writeSize);
    FAULT_INJECTION_CALL_REPLACE_END;
    if (errorCode != 0) {
        return false;
    }
    g_loggerThreadContext.remoteLogContext.currentDataFileSize += writeSize;
    return true;
}

static FileDescriptor *GetRemoteLogDataFileFd(VirtualFileSystem *remoteVfs, const char *remoteStoreSpaceName,
                                              FileId fileId)
{
    bool isExist = false;
    FileDescriptor *fd = NULL;
    ErrorCode errorCode = LogFileIsExist(remoteVfs, fileId, "", &isExist);
    if (errorCode != ERROR_SYS_OK) {
        goto ERR_EXIT;
    }
    if (isExist) {
        errorCode = LogFileOpen(remoteVfs, fileId, "", FILE_READ_AND_WRITE_FLAG, &fd);
        if (errorCode != ERROR_SYS_OK) {
            goto ERR_EXIT;
        }
    } else {
        FileParameter fileParameter;
        int ret = strcpy_s(fileParameter.storeSpaceName, sizeof(fileParameter.storeSpaceName), remoteStoreSpaceName);
        if (ret != EOK) {
            goto ERR_EXIT;
        }
        fileParameter.streamId = VFS_DEFAULT_FILE_STREAM_ID;
        fileParameter.flag = APPEND_WRITE_FILE;
        fileParameter.fileSubType = ERR_LOG_FILE_TYPE;
        fileParameter.rangeSize = BLCKSZ;
        fileParameter.maxSize = UINT32_MAX;
        fileParameter.recycleTtl = 0;
        fileParameter.mode = GetErrLogFileMode();
        fileParameter.isReplayWrite = false;
        errorCode = LogFileCreate(remoteVfs, fileId, "", fileParameter, &fd);
        if (errorCode != ERROR_SYS_OK) {
            goto ERR_EXIT;
        }
    }
    return fd;

ERR_EXIT:
    if (fd != NULL) {
        (void)Close(fd);
    }
    return NULL;
}

static ErrorCode UpdateRemoteLogCurSeqNum(FileDescriptor *metaFd, uint32_t seq)
{
    int64_t curPos = 0;

    /* Initail all buf members to space */
    char buf[LOG_SEQUENCE_INFO_SIZE];
    for (int i = 0; i < LOG_SEQUENCE_INFO_SIZE; i++) {
        buf[i] = ' ';
    }

    /* Add sequence information to buf */
    (void)sprintf_s(buf, sizeof(buf), "%s%u", LOG_SEQUENCE_NUM_PREFIX, seq);
    /* Replace the line terminator string with space */
    buf[strlen(buf)] = ' ';
    /* Add new line character to the end of current sequence info */
    buf[LOG_SEQUENCE_INFO_SIZE - 1] = '\n';
    ErrorCode errorCode = PwriteSync(metaFd, buf, sizeof(buf), 0, &curPos);
    if (errorCode != ERROR_SYS_OK) {
        ErrLog(ERROR, ErrMsg("Failed to write current sequence info to remote log file.\n"));
        return errorCode;
    }

    return ERROR_SYS_OK;
}

#define REMOTE_META_FILE_SEQ 0

static void SwitchToNewRemoteLogDataFile(void)
{
    RemoteLogFileContext *ctx = &g_loggerThreadContext.remoteLogContext;
    FileDescriptor *oldDataFd = ctx->dataFd;
    FileDescriptor *dataFd = NULL;
    uint32_t seq = ctx->currentSequenceNo + 1 >= ctx->remoteFileCfg.errorLogFileCount ? 0 : ctx->currentSequenceNo + 1;
    // pc-lint: 661 potential out of bounds pointer access: excess of integer byte(s)
    if ((seq < sizeof(ctx->remoteFileCfg.errorLogFileIds) / sizeof(ctx->remoteFileCfg.errorLogFileIds[0])) &&
        (seq != REMOTE_META_FILE_SEQ)) {
        dataFd = GetRemoteLogDataFileFd(ctx->remoteFileCfg.remoteVfs, ctx->remoteFileCfg.storeSpaceName,
                                        ctx->remoteFileCfg.errorLogFileIds[seq]);
        if (dataFd == NULL) {
            ErrLog(ERROR, ErrMsg("Remote log file rotation failed, using old file\n"));
            goto RET1; // next skip the Seq
        }
    }

    ErrorCode errorCode;
    FAULT_INJECTION_CALL_REPLACE(MOCK_UPDATE_REMOTE_LOG_SEQ_NUM_FAILED, &errorCode)
    errorCode = UpdateRemoteLogCurSeqNum(ctx->metaFd, seq);
    FAULT_INJECTION_CALL_REPLACE_END;
    if (errorCode != ERROR_SYS_OK) {
        ErrLog(ERROR,
               ErrMsg("Update remote error log sequence number %d failed, old is %d\n", seq, ctx->currentSequenceNo));
        /* Update remote log sequence number failed, rollback to the previous sequence number */
        goto RET2;
    }
    // OK, got the dataFd and assign oldDataFd to dataFd (will close it soon later)
    ctx->dataFd = seq == REMOTE_META_FILE_SEQ ? ctx->metaFd : dataFd;
    ASSERT(ctx->dataFd != NULL);
    // last seq is REMOTE_META_FILE_SEQ, don't close, because it same as meta fd
    dataFd = ctx->currentSequenceNo == REMOTE_META_FILE_SEQ ? NULL : oldDataFd;
RET1:
    ctx->currentSequenceNo = seq;
RET2:
    if ((dataFd != NULL) && (Close(dataFd) != ERROR_SYS_OK)) {
        ErrLog(ERROR, ErrMsg("Close remote log file failed\n"));
    }
}

static void LogRemoteFileSizeRotate(void)
{
    if (!CheckFileSizeNeedRotation(g_loggerThreadContext.remoteLogContext.currentDataFileSize,
                                   g_loggerThreadContext.remoteLogContext.logFileRotationSize)) {
        /* Current file size does not exceed file rotation size */
        return;
    }
    /* Switch to new log data file if file count > 1 */
    if (g_loggerThreadContext.remoteLogContext.remoteFileCfg.errorLogFileCount > 1) {
        SwitchToNewRemoteLogDataFile();
    }

    bool isFirstRemoteFile = (g_loggerThreadContext.remoteLogContext.currentSequenceNo == 0);

    /* Truncate log data file to 0 size */
    ErrorCode errorCode =
        Truncate(g_loggerThreadContext.remoteLogContext.dataFd, isFirstRemoteFile ? LOG_SEQUENCE_INFO_SIZE : 0);
    if (errorCode != ERROR_SYS_OK) {
        ErrLog(ERROR, ErrMsg("Truncate remote log data file failed\n"));
        goto ERR_EXIT;
    }
    g_loggerThreadContext.remoteLogContext.currentDataFileSize = isFirstRemoteFile ? LOG_SEQUENCE_INFO_SIZE : 0;
    return;

ERR_EXIT:
    SetErrLogDestination(GetErrLogDestination() & (~LOG_DESTINATION_REMOTE_FILE));
    ReleaseRemoteLogResource();
}

#define NUMBER_BASE 10

static int64_t GetRemoteLogSeqNum(FileDescriptor *metaFd)
{
    ASSERT(metaFd != NULL);

#ifdef ENABLE_FAULT_INJECTION
#define INVALID_SEQ_NUM 1000
#define VALID_SEQ_NUM   1
#endif
    FAULT_INJECTION_RETURN(MOCK_REMOTE_LOG_INVALID_SEQ_NUM, INVALID_SEQ_NUM);
    FAULT_INJECTION_RETURN(MOCK_REMOTE_LOG_VALID_SEQ_NUM, VALID_SEQ_NUM);

    int64_t fileSize = 0;
    if (GetSize(metaFd, &fileSize) != ERROR_SYS_OK) {
        return -1;
    }

    /* If file size is smaller than the size of log sequence buffer, which means there is no log sequence info */
    if (fileSize < LOG_SEQUENCE_INFO_SIZE) {
        return -1;
    }

    int64_t readSize = 0;
    char metaBuf[LOG_SEQUENCE_INFO_SIZE] = {0};
    if (Pread(metaFd, metaBuf, sizeof(metaBuf), 0, &readSize) != ERROR_SYS_OK) {
        return -1;
    }

    /* Check the format of sequence info is valid or not */
    if (strncmp(LOG_SEQUENCE_NUM_PREFIX, metaBuf, strlen(LOG_SEQUENCE_NUM_PREFIX)) != 0) {
        WriteErrMsg("Check remote log current sequence number failed.\n");
        /* Check remote log meta file failed */
        return -1;
    }

    /* Check the value of sequence number is valid or not */
    errno = 0;
    int64_t seq = strtol(metaBuf + strlen(LOG_SEQUENCE_NUM_PREFIX), NULL, NUMBER_BASE);
    if ((seq == 0) && (errno == EINVAL)) {
        seq = -1;
    }

    return seq;
}

bool PrepareRemoteLogResource(const ErrorLogRemoteFileConfigure *remoteLogCfg)
{
    FileDescriptor *metaFd = NULL, *dataFd = NULL;
    VirtualFileSystem *remoteVfs = remoteLogCfg->remoteVfs;
    metaFd = GetRemoteLogDataFileFd(remoteVfs, remoteLogCfg->storeSpaceName,
                                    remoteLogCfg->errorLogFileIds[REMOTE_META_FILE_SEQ]);
    if (metaFd == NULL) {
        return false;
    }
    int64_t remoteLogSeq = GetRemoteLogSeqNum(metaFd);
    if ((remoteLogSeq < 0) || (remoteLogSeq >= remoteLogCfg->errorLogFileCount)) {
        WriteErrMsg("Invalid sequence number %lld, reset sequence number to %d\n", remoteLogSeq, REMOTE_META_FILE_SEQ);
        /* Check remote log meta file failed, reset sequence number to REMOTE_META_FILE_SEQ */
        if (UpdateRemoteLogCurSeqNum(metaFd, REMOTE_META_FILE_SEQ) != ERROR_SYS_OK) {
            (void)Close(metaFd);
            return false;
        }
        remoteLogSeq = REMOTE_META_FILE_SEQ;
    }
    if (remoteLogSeq == REMOTE_META_FILE_SEQ) {
        dataFd = metaFd;
    } else {
        dataFd = GetRemoteLogDataFileFd(remoteVfs, remoteLogCfg->storeSpaceName,
                                        remoteLogCfg->errorLogFileIds[remoteLogSeq]);
        if (dataFd == NULL) {
            (void)Close(metaFd);
            return false;
        }
    }
    int64_t dataFileSize = 0;
    if (GetSize(dataFd, &dataFileSize) != ERROR_SYS_OK) {
        (void)Close(metaFd);
        if (metaFd != dataFd) {
            (void)Close(dataFd);
        }
        return false;
    }
    g_loggerThreadContext.remoteLogContext.remoteFileCfg = *remoteLogCfg;
    g_loggerThreadContext.remoteLogContext.currentSequenceNo = (uint32_t)remoteLogSeq;
    g_loggerThreadContext.remoteLogContext.logFileRotationSize =
        remoteLogCfg->totalSpace / remoteLogCfg->errorLogFileCount;
    g_loggerThreadContext.remoteLogContext.logFileRotationAge = ERROR_LOG_FILE_ROTATION_AGE;
    g_loggerThreadContext.remoteLogContext.metaFd = metaFd;
    g_loggerThreadContext.remoteLogContext.dataFd = dataFd;
    g_loggerThreadContext.remoteLogContext.currentDataFileSize = dataFileSize;
    g_loggerThreadContext.remoteLogIsReady = true;
    return true;
}

void ReleaseRemoteLogResource(void)
{
    if (!g_loggerThreadContext.remoteLogIsReady) {
        return;
    }
    g_loggerThreadContext.remoteLogIsReady = false;
    if (Close(g_loggerThreadContext.remoteLogContext.metaFd) != ERROR_SYS_OK) {
        ErrLog(ERROR, ErrMsg("Close remote log meta file failed\n"));
    }
    if (g_loggerThreadContext.remoteLogContext.metaFd != g_loggerThreadContext.remoteLogContext.dataFd) {
        if (Close(g_loggerThreadContext.remoteLogContext.dataFd) != ERROR_SYS_OK) {
            ErrLog(ERROR, ErrMsg("Close remote log data file failed\n"));
        }
    }
}

/* Close the current error log data file. */
static void CloseLogDataFile(LogOutputFile *logOutputFile)
{
    if (logOutputFile->dataFd != NULL) {
        if (Close(logOutputFile->dataFd) != ERROR_SYS_OK) {
            ErrLog(WARNING, ErrMsg("Failed to close logger data file.\n"));
        }
        logOutputFile->dataFd = NULL;
    }
}

/* Close the current error log data file. */
static void ClosePLogFile(LogOutputFile *logOutputFile)
{
    if (logOutputFile->profileFd != NULL) {
        if (Close(logOutputFile->profileFd) != ERROR_SYS_OK) {
            ErrLog(WARNING, ErrMsg("Failed to close profile log file.\n"));
        }
        logOutputFile->profileFd = NULL;
    }
}

/* Close the current error log data file. */
static void CloseSlowQueryLogFile(LogOutputFile *logOutputFile)
{
    if (logOutputFile->sqlFd != NULL) {
        if (Close(logOutputFile->sqlFd) != ERROR_SYS_OK) {
            ErrLog(WARNING, ErrMsg("Failed to close slow query log file.\n"));
        }
        logOutputFile->sqlFd = NULL;
    }
}

/* Close the current error log data file. */
static void CloseAspLogFile(LogOutputFile *logOutputFile)
{
    if (logOutputFile->aspFd != NULL) {
        if (Close(logOutputFile->aspFd) != ERROR_SYS_OK) {
            ErrLog(WARNING, ErrMsg("Failed to close asp log file.\n"));
        }
        logOutputFile->aspFd = NULL;
    }
}

/* Close the current error log data file. */
static void CloseCsvLogFile(LogOutputFile *logOutputFile)
{
    if (logOutputFile->csvFd != NULL) {
        if (Close(logOutputFile->csvFd) != ERROR_SYS_OK) {
            ErrLog(WARNING, ErrMsg("Failed to close csv log file.\n"));
        }
        logOutputFile->csvFd = NULL;
    }
}

/**
 * Writes buffer data to error log files.
 * @param buffer:Error log data.
 * @param count: Data size.
 */
void WriteLocalLogDataFile(const char *buffer, size_t count)
{
    ErrorCode errorCode = ERROR_SYS_OK;
    FAULT_INJECTION_CALL_REPLACE(MOCK_LOG_DISK_FULL, &errorCode)
    errorCode = WriteAsync(g_loggerThreadContext.errorLogOutputFile.dataFd, buffer, count, NULL);
    FAULT_INJECTION_CALL_REPLACE_END;
    if unlikely(((errorCode == VFS_ERROR_DISK_HAS_NO_SPACE && !g_loggerThreadContext.isDiskFull) ||
                 (errorCode == ERROR_SYS_OK && g_loggerThreadContext.isDiskFull))) {
        RenameLogFileWhenDiskFull();
    }
}

/**
 * Writes buffer data to profile log files.
 * @param buffer:Error log data.
 * @param count: Data size.
 */
static void WriteLocalPLogFile(const char *buffer, size_t count)
{
    (void)WriteAsync(g_loggerThreadContext.errorLogOutputFile.profileFd, buffer, count, NULL);
}

/**
 * Writes buffer data to asp log files.
 * @param buffer:Error log data.
 * @param count: Data size.
 */
static void WriteLocalAspLogFile(const char *buffer, size_t count)
{
    (void)WriteAsync(g_loggerThreadContext.errorLogOutputFile.aspFd, buffer, count, NULL);
}

/**
 * Writes buffer data to csv log files.
 * @param buffer:Error log data.
 * @param count: Data size.
 */
static void WriteLocalCsvLogFile(const char *buffer, size_t count)
{
    (void)WriteAsync(g_loggerThreadContext.errorLogOutputFile.csvFd, buffer, count, NULL);
}

static void WriteErrorLogDataFile(const char *buffer, size_t count, bool tryWriteRemote)
{
    if (tryWriteRemote) {
        bool res = WriteRemoteLogDataFile(g_loggerThreadContext.remoteLogContext.dataFd, buffer, count);
        if (res) {
            return;
        }
    }
    WriteLocalLogDataFile(buffer, count);
}

static void WritePLogFile(const char *buffer, size_t count, bool tryWriteRemote)
{
    if (tryWriteRemote) {
        return;
    }
    WriteLocalPLogFile(buffer, count);
}

static void WriteAspLogFile(const char *buffer, size_t count, bool tryWriteRemote)
{
    if (tryWriteRemote) {
        return;
    }
    WriteLocalAspLogFile(buffer, count);
}

static void WriteCsvLogFile(const char *buffer, size_t count, bool tryWriteRemote)
{
    if (tryWriteRemote) {
        return;
    }
    WriteLocalCsvLogFile(buffer, count);
}

static void FlushBatchLog(MsgBatchData *msgBatchData, const bool isWriteRmote)
{
    if (msgBatchData->msgBatchLen != 0) {
        WriteErrorLogDataFile(msgBatchData->msgBatch, msgBatchData->msgBatchLen, isWriteRmote);
        (void)Fsync(g_loggerThreadContext.errorLogOutputFile.dataFd);
        msgBatchData->msgBatchLen = 0;
    }
}

/**
 * Add log to log batch
 */
static bool AddLogToBatch(char *msgBatch, size_t offset, const char *msgPtr, size_t msgLen)
{
    errno_t errCode = memcpy_s(msgBatch + offset, LOG_BATCH_BUF_SIZE - offset, msgPtr, msgLen);
    if (errCode != EOK) {
        ErrLog(ERROR, ErrMsg("fail to copy data by memcpy_s, error code %d.\n", errCode));
    }
    return (errCode == EOK);
}

/**
 * Write logs in a batch way.
 */
static void BatchWriteLog(MsgBatchData *msgBatchData, const char *msgPtr, size_t msgLen, size_t bufSize,
                          bool tryWriteRemote)
{
    /**
     * When the size of log in batch reach the length of the batch size, write the existing log to log file,
     * and add this log the next batch.
     */
    if (msgBatchData->msgBatchLen + msgLen > bufSize) {
        WriteErrorLogDataFile(msgBatchData->msgBatch, msgBatchData->msgBatchLen, tryWriteRemote);
        msgBatchData->msgBatchLen = 0;
        /* If the length of message greater than the batch size, writing to log file directly. */
        if (msgLen > bufSize) {
            WriteErrorLogDataFile(msgPtr, msgLen, tryWriteRemote);
            return;
        }
    }
    /* If the log size not reach the remain batch size, add log into batch */
    bool isAddLogSucceed = AddLogToBatch(msgBatchData->msgBatch, msgBatchData->msgBatchLen, msgPtr, msgLen);
    msgBatchData->msgBatchLen += isAddLogSucceed ? msgLen : 0;
}

static int RenameLocalLogFile(FullPathName *fullPathName, TimesSecondsSinceEpoch *timePtr, char *oldPathName,
                              char *newPathName, size_t pathNameSize)
{
    if (unlikely(fullPathName == NULL)) {
        ErrLog(ERROR, ErrMsg("Fail to rename local log file, the fullPathName is NULL.\n"));
        return -1;
    }

    if (!GetFileFullPath(fullPathName, oldPathName, pathNameSize)) {
        return -1;
    }

    TimeFormatStructure formatTime;
    LocalTime(timePtr, &formatTime);

    (void)FormatTime(fullPathName->appendName, FORMATTED_TIME_MAX_LEN, "%Y-%m-%d_%H%M%S", &formatTime);
    if (!GetFileFullPath(fullPathName, newPathName, pathNameSize)) {
        return -1;
    }

    ErrorCode errCode = RenameFile(g_loggerThreadContext.localVfs, oldPathName, newPathName);
    if (errCode != ERROR_SYS_OK) {
        ErrLog(ERROR, ErrMsg("fail to rename oldPathName to newPathName, error code %lld.\n", errCode));
        return -1;
    }

    return 0;
}

static void DeleteRedundantLogFile(uint32_t *curSeqNo, uint64_t maxSeqNo, char *fileSuffixStr)
{
    while ((uint64_t)*curSeqNo > maxSeqNo) {
        char filePath[MAX_PATH] = {0};
        (void)GetFileInfoFromLocalDirectory(g_loggerThreadContext.errorLogFileConfig.logDirectory, filePath, MAX_PATH,
                                            fileSuffixStr, g_loggerThreadContext.errorLogFileConfig.logFileName);
        int rc;
        FAULT_INJECTION_CALL_REPLACE(MOCK_DELETE_LOG_FILE_FAILED, &rc);
        rc = unlink(filePath);
        FAULT_INJECTION_CALL_REPLACE_END;
        if (unlikely(rc == -1)) {
            ErrLog(ERROR, ErrMsg("Delete oldest log file %s failed.\n", filePath));
        } else {
            *curSeqNo -= 1;
        }
    }
}

static void DeleteRedundantCompressedLogFile(const char* compressdLogName, LogOutputFile *logOutputFile)
{
    ErrorCode errCode = ERROR_SYS_OK;
    char filePath[MAX_PATH] = {0};
    uint64_t maxCompressedLogNum = 0;
    uint32_t compressedFileCount = GetFileInfoFromLocalDirectory(g_loggerThreadContext.errorLogFileConfig.logDirectory,
                                                                 filePath, MAX_PATH, ZIP_LOG_EXTEND,
                                                                 g_loggerThreadContext.errorLogFileConfig.logFileName);
    /* Check the actual compressed log file number */
    if (logOutputFile->currentCompressedSequenceNo != compressedFileCount) {
        logOutputFile->currentCompressedSequenceNo = compressedFileCount;
    }

    /* Compute the max compressed log file number */
    uint64_t compressedFileSize = 0;
    struct stat st;
    if (stat(compressdLogName, &st) == 0) {
        compressedFileSize = (uint64_t)st.st_size;
    } else {
        PosixErrorCode2PortErrorCode(errno, &errCode);
        return;
    }

#define BYTES_PER_KB 1024
    if (compressedFileSize != 0) {
            maxCompressedLogNum = ((uint64_t)g_loggerThreadContext.errorLogFileConfig.logDirectorySpaceSize *
                                   BYTES_PER_KB) / compressedFileSize;
    }

    if (maxCompressedLogNum != 0) {
        DeleteRedundantLogFile(&(logOutputFile->currentCompressedSequenceNo), maxCompressedLogNum, ZIP_LOG_EXTEND);
    }
    
    return;
}

static ErrorCode CloseOpenedFile(FILE *in, gzFile *out, const char *filename, const char *compressdLogName)
{
    ErrorCode errCode = ERROR_SYS_OK;

    int ret = fclose(in);
    if (unlikely(ret != 0)) {
        PosixErrorCode2PortErrorCode(errno, &errCode);
        ErrLog(ERROR, ErrMsg("fail to close the uncompressed log file %s, error code %lld.\n", filename, errCode));
        return errCode;
    }

    ret = gzclose(*out);
    if (unlikely(ret != 0)) {
        PosixErrorCode2PortErrorCode(errno, &errCode);
        ErrLog(ERROR, ErrMsg("fail to close the compressed log file %s, error code %lld.\n", compressdLogName,
                             errCode));
        return errCode;
    }
    
    return errCode;
}

static ErrorCode CompressLogFile(const char *filename, LogOutputFile *logOutputFile, char *compressdLogName)
{
#define CHUNK_SIZE 8192
    ErrorCode errCode = ERROR_SYS_OK;
    char resolvedFilename[PATH_MAX] = {0x00};
    if (!CanonicalizePath(filename, resolvedFilename)) {
        PosixErrorCode2PortErrorCode(errno, &errCode);
        ErrLog(ERROR, ErrMsg("File name : %s canonicalize path failed, error code %lld ", filename, errCode));
        return errCode;
    }

    FILE *in = fopen(resolvedFilename, "rb");
    if (unlikely(in == NULL)) {
        PosixErrorCode2PortErrorCode(errno, &errCode);
        ErrLog(ERROR, ErrMsg("fail to open the uncompressed log file %s, error code %lld.\n", filename, errCode));
        return errCode;
    }

    int ret = sprintf_s(compressdLogName, MAX_PATH, "%s.gz", filename);
    if (unlikely(ret < 0)) {
        PosixErrorCode2PortErrorCode(errno, &errCode);
        ErrLog(ERROR, ErrMsg("Format compressed log name failed, error code %lld.\n", errCode));
        return errCode;
    }

    /* If compressed log file already exist, delete it */
    struct stat path_stat;
    FAULT_INJECTION_ACTION(MOCK_CREATE_LOG_SAME_NAME_DIR, (mkdir(compressdLogName, S_IRWXU)));
    if (stat(compressdLogName, &path_stat) == 0) {
        if (S_ISREG(path_stat.st_mode)) {
            ret = unlink(compressdLogName);
        } else if (S_ISDIR(path_stat.st_mode)) {
            ret = ForceRemoveDir(compressdLogName);
        }
        
        if (unlikely(ret == -1)) {
            PosixErrorCode2PortErrorCode(errno, &errCode);
            ErrLog(ERROR, ErrMsg("Delete compressed log file %s failed, errcode is :%lld.\n", compressdLogName,
                                 errCode));
        } else {
            logOutputFile->currentCompressedSequenceNo--;
        }
    }

    gzFile out = gzopen(compressdLogName, "wb");
    if (unlikely(out == NULL)) {
        PosixErrorCode2PortErrorCode(errno, &errCode);
        ErrLog(ERROR, ErrMsg("fail to open the compressed log file %s, error code %lld.\n", compressdLogName, errCode));
        ret = fclose(in);
        if (unlikely(ret != 0)) {
            PosixErrorCode2PortErrorCode(errno, &errCode);
            ErrLog(ERROR, ErrMsg("fail to close the uncompressed log file %s, error code %lld.\n", filename, errCode));
            return errCode;
        }
        return errCode;
    }
    logOutputFile->currentCompressedSequenceNo++;

    char buffer[CHUNK_SIZE];
    size_t bytes_read;

    while ((bytes_read = fread(buffer, 1, sizeof(buffer), in)) > 0) {
        if (gzwrite(out, buffer, (unsigned int)bytes_read) != (int)bytes_read) {
            PosixErrorCode2PortErrorCode(errno, &errCode);
            ErrLog(ERROR, ErrMsg("Failed to write to compressed file %s, error code %lld.\n", compressdLogName,
                                 errCode));
            return CloseOpenedFile(in, &out, filename, compressdLogName);
        }
    }

    return CloseOpenedFile(in, &out, filename, compressdLogName);
}

static bool IsErrLogFileNameValid(const char *sourceErrLogFileName, const char *baseFileName, uint64_t *fileTimeStamp,
                                  char *fileSuffixStr, char *fileName)
{
/* These two macros are used to check the length of time stamp, not represent for a real time */
#define MIN_FILE_TIME_STAMP     9999999999999
#define MAX_FILE_TIME_STAMP     100000000000000
#define NEW_FILE_TIME_STAMP_LEN 15 /* The length of new file time stamp %Y%m%d%H%M%S */
    /* Check whether it is the error log file before rotation */
    if (strncmp(sourceErrLogFileName, baseFileName, strlen(baseFileName)) == 0 &&
        (strlen(sourceErrLogFileName) == strlen(baseFileName))) {
        if (strcmp(fileSuffixStr, ZIP_LOG_EXTEND) == 0) {
            return false;
        }
        return true;
    }

    /* Check whether the link symbol of file name is correct */
    if (baseFileName[strlen(fileName)] != '-') {
        return false;
    }

    /* Check the validation of file timestamp and file name suffix */
    char *endPtr;
    const char *fileTimeStampStr = baseFileName + strlen(fileName) + 1;
    uint32_t year, mounth, day, minSec;
#define DATE_PAIR_NUM 4
    if (sscanf_s(fileTimeStampStr, "%u-%u-%u_%u", &year, &mounth, &day, &minSec) != DATE_PAIR_NUM) {
        if (GetErrorLogRedirectionDone()) {
            ErrLog(ERROR, ErrMsg("The file timestamp is invalid."));
        }
        return false;
    }
    char newFileTimeStampStr[NEW_FILE_TIME_STAMP_LEN] = {0};
    int ret = sprintf_s(newFileTimeStampStr, sizeof(newFileTimeStampStr), "%u%02u%02u%06u", year, mounth, day, minSec);
    if (ret < 0) {
        if (GetErrorLogRedirectionDone()) {
            ErrLog(ERROR, ErrMsg("IsErrLogFileNameValid sprintf_s failed, err:%d", ret));
        }
        return false;
    }

    *fileTimeStamp = (uint64_t)strtol(newFileTimeStampStr, &endPtr, NUMBER_BASE);

    const char *fileNameSuffix = fileTimeStampStr + (strlen(fileTimeStampStr) - strlen(fileSuffixStr));

    bool condition = (strcmp(fileNameSuffix, fileSuffixStr) == 0) && (*fileTimeStamp > MIN_FILE_TIME_STAMP) &&
                     (*fileTimeStamp < MAX_FILE_TIME_STAMP);
    return condition;
}

static inline void GenerateErrorLogFilePath(char *errorLogFilePath, Size len, const char *dirName,
                                            const char *baseFileName)
{
    ssize_t rc = sprintf_s(errorLogFilePath, len, "%s/%s", dirName, baseFileName);
    if (rc == -1) {
        ErrLog(ERROR, ErrMsg("sprintf_s failed, err: %ld\n", rc));
    }
}

/*
 * Get the error log file information from local directory.
 */
uint32_t GetFileInfoFromLocalDirectory(const char *dirName, char *errorLogFilePath, Size len, char *fileSuffixStr,
                                       char *fileName)
{
    uint32_t count = 0;
    Directory dir;
    ErrorCode errorCode = OpenDirectory(dirName, &dir);
    if (errorCode != ERROR_SYS_OK) {
        return count;
    }
    DirectoryEntry dirEntry;
    uint64_t oldestErrLogFileTimeStamp = UINT64_MAX;

    char sourceErrLogFileName[MAX_PATH] = {0};
    int ret = 0;
    if (strcmp(fileSuffixStr, ZIP_LOG_EXTEND) == 0) {
        ret = sprintf_s(sourceErrLogFileName, sizeof(sourceErrLogFileName), "%s-current.%s", fileName, ERR_LOG_EXTEND);
    } else {
        ret = sprintf_s(sourceErrLogFileName, sizeof(sourceErrLogFileName), "%s-current.%s", fileName, fileSuffixStr);
    }

    if (unlikely(ret < 0)) {
        WriteErrMsg("Fail to generate source error log file name.\n");
        return count;
    }

    while (ReadDirectory(&dir, &dirEntry)) {
        if (dirEntry.type != DIR_TYPE_REGULAR_FILE) {
            continue;
        }
        if (!strcmp(dirEntry.name, ".") || !strcmp(dirEntry.name, "..")) {
            continue;
        }
        char *baseFileName = Basename(dirEntry.name);

        uint64_t fileTimeStamp = 0;
        /* Check the validation of file name or file time stamp. */
        if (IsErrLogFileNameValid(sourceErrLogFileName, baseFileName, &fileTimeStamp, fileSuffixStr, fileName)) {
            count++;
            /* Find the oldest formatted error log file */
            if (fileTimeStamp < oldestErrLogFileTimeStamp && fileTimeStamp != 0) {
                oldestErrLogFileTimeStamp = fileTimeStamp;
                GenerateErrorLogFilePath(errorLogFilePath, len, dirName, baseFileName);
                /* If no formatted error log file, add source error log file to errorLogFilePath */
            } else if (fileTimeStamp == 0 && strlen(errorLogFilePath) == 0) {
                GenerateErrorLogFilePath(errorLogFilePath, len, dirName, baseFileName);
            }
        }
    }

    CloseDirectory(&dir);
    return count;
}

static void DeleteRedundantPLogFile(uint32_t *curSeqNo, uint32_t maxSeqNo)
{
    while (*curSeqNo > maxSeqNo) {
        char filePath[MAX_PATH] = {0};
        (void)GetFileInfoFromLocalDirectory(g_loggerThreadContext.errorLogFileConfig.pLogDirectory, filePath, MAX_PATH,
                                            GetExtendNameByFileType(PLOG_LOG_FILE_TYPE),
                                            g_loggerThreadContext.errorLogFileConfig.logFileName);
        int rc;
        FAULT_INJECTION_CALL_REPLACE(MOCK_DELETE_LOG_FILE_FAILED, &rc);
        rc = unlink(filePath);
        FAULT_INJECTION_CALL_REPLACE_END;
        if (rc == -1) {
            ErrLog(ERROR, ErrMsg("Delete oldest profile log file %s failed.\n", filePath));
        } else {
            *curSeqNo -= 1;
        }
    }
}

static void DeleteRedundantSlowQueryLogFile(uint32_t *curSeqNo, uint32_t maxSeqNo)
{
    while (*curSeqNo > maxSeqNo) {
        char filePath[MAX_PATH] = {0};
        (void)GetFileInfoFromLocalDirectory(g_loggerThreadContext.errorLogFileConfig.slowQueryLogDirectory, filePath,
                                            MAX_PATH, GetExtendNameByFileType(SQL_LOG_FILE_TYPE),
                                            g_loggerThreadContext.errorLogFileConfig.slowQueryFileName);
        int rc;
        FAULT_INJECTION_CALL_REPLACE(MOCK_DELETE_LOG_FILE_FAILED, &rc);
        rc = unlink(filePath);
        FAULT_INJECTION_CALL_REPLACE_END;
        if (rc == -1) {
            ErrLog(ERROR, ErrMsg("Delete oldest slow query log file %s failed.\n", filePath));
        } else {
            *curSeqNo -= 1;
        }
    }
}

static void DeleteRedundantAspLogFile(uint32_t *curSeqNo, uint32_t maxSeqNo)
{
    while (*curSeqNo > maxSeqNo) {
        char filePath[MAX_PATH] = {0};
        (void)GetFileInfoFromLocalDirectory(g_loggerThreadContext.errorLogFileConfig.aspLogDirectory, filePath,
                                            MAX_PATH, GetExtendNameByFileType(ASP_LOG_FILE_TYPE),
                                            g_loggerThreadContext.errorLogFileConfig.aspFileName);
        int rc;
        FAULT_INJECTION_CALL_REPLACE(MOCK_DELETE_LOG_FILE_FAILED, &rc);
        rc = unlink(filePath);
        FAULT_INJECTION_CALL_REPLACE_END;
        if (rc == -1) {
            ErrLog(ERROR, ErrMsg("Delete oldest asp log file %s failed.\n", filePath));
        } else {
            *curSeqNo -= 1;
        }
    }
}

static void DeleteRedundantCsvLogFile(uint32_t *curSeqNo, uint32_t maxSeqNo)
{
    while (*curSeqNo > maxSeqNo) {
        char filePath[MAX_PATH] = {0};
        (void)GetFileInfoFromLocalDirectory(g_loggerThreadContext.errorLogFileConfig.logDirectory, filePath, MAX_PATH,
                                            GetExtendNameByFileType(CSV_LOG_FILE_TYPE),
                                            g_loggerThreadContext.errorLogFileConfig.logFileName);
        int rc;
        FAULT_INJECTION_CALL_REPLACE(MOCK_DELETE_LOG_FILE_FAILED, &rc);
        rc = unlink(filePath);
        FAULT_INJECTION_CALL_REPLACE_END;
        if (rc == -1) {
            ErrLog(ERROR, ErrMsg("Delete oldest csv file %s failed.\n", filePath));
        } else {
            *curSeqNo -= 1;
        }
    }
}

static void TraverseDirAndCompressExistLogFile(char *filePath, char *compressdLogName, LogOutputFile *logOutputFile)
{
    char sourceErrLogFileName[MAX_PATH] = {0};
    int ret = sprintf_s(sourceErrLogFileName, sizeof(sourceErrLogFileName), "%s-current.%s",
                        g_loggerThreadContext.errorLogFileConfig.logFileName, ERR_LOG_EXTEND);
    if (unlikely(ret < 0)) {
        WriteErrMsg("Fail to generate source error log file name.\n");
        return;
    }

    Directory dir;
    ErrorCode errCode = OpenDirectory(g_loggerThreadContext.errorLogFileConfig.logDirectory, &dir);
    if (errCode != ERROR_SYS_OK) {
        return;
    }
    DirectoryEntry dirEntry;
    while (ReadDirectory(&dir, &dirEntry)) {
        if (dirEntry.type != DIR_TYPE_REGULAR_FILE) {
            continue;
        }
        if (!strcmp(dirEntry.name, ".") || !strcmp(dirEntry.name, "..")) {
            continue;
        }
        char *baseFileName = Basename(dirEntry.name);

        uint64_t fileTimeStamp = 0;
        /* Check the validation of file name or file time stamp. */
        if (IsErrLogFileNameValid(sourceErrLogFileName, baseFileName, &fileTimeStamp,
            ERR_LOG_EXTEND, g_loggerThreadContext.errorLogFileConfig.logFileName)) {
            /* Compress the existing error log file */
            if (strncmp(sourceErrLogFileName, baseFileName, strlen(baseFileName)) != 0) {
                GenerateErrorLogFilePath(filePath, MAX_PATH, g_loggerThreadContext.errorLogFileConfig.logDirectory,
                                         baseFileName);

                (void)memset_s(compressdLogName, MAX_PATH, 0, MAX_PATH);
                errCode = CompressLogFile(filePath, logOutputFile, compressdLogName);
                if (errCode != ERROR_SYS_OK) {
                    ErrLog(ERROR, ErrMsg("fail to compress log file, errcode is :%lld.\n", errCode));
                    continue;
                } else {
                    /* delete log file after successfully compressed log file */
                    ret = unlink(filePath);
                    if (ret == -1) {
                        PosixErrorCode2PortErrorCode(errno, &errCode);
                        continue;
                    } else {
                        logOutputFile->currentSequenceNo -= 1;
                    }
                    ret = chmod(compressdLogName, S_IRUSR);
                    if (ret == -1) {
                        ErrLog(LOG, ErrMsg("fail to chmod compress log file."));
                    }
                }
            }
        }
    }

    CloseDirectory(&dir);
}

static void LogCompressionWorkNotify(void)
{
    MutexLock(&g_logCompressionThreadContext.mutex);
    g_logCompressionThreadContext.compressEnableFlag = true;
    ConditionVariableSignal(&g_logCompressionThreadContext.cond);
    MutexUnlock(&g_logCompressionThreadContext.mutex);
}

static void LogCompressionThreadExit(void)
{
    /* Check if the log compress thread has started. */
    if (g_logCompressionThreadState != LOG_COMPRESSION_THREAD_STARTED) {
        return;
    }

    ErrorCode errorCode = ERROR_SYS_OK;
    Tid log_thread_tid = GetLogCompressionThreadTid();

    MutexLock(&g_logCompressionThreadContext.mutex);
    g_logCompressionThreadContext.logCompressionExitFlag = true;
    g_logCompressionThreadState = LOG_COMPRESSION_THREAD_NOT_STARTED;
    ConditionVariableSignal(&g_logCompressionThreadContext.cond);
    MutexUnlock(&g_logCompressionThreadContext.mutex);

    /* The main log thread waits for log compression thread to exit. */
    errorCode = ThreadJoin(log_thread_tid, NULL);
    if (errorCode != ERROR_SYS_OK) {
        WriteErrMsg("Failed to wait log compression thread exit.\n");
    }
}

/**
 * Rotate the log file to the next file.
 */
static void LogFileRotate(LogFileConfig *logFileConfig, LogOutputFile *logOutputFile)
{
    Time(&(logOutputFile->lastLogTime));
    if (logOutputFile->lastLogTime.timeSeconds == logOutputFile->lastLogRotateTime.timeSeconds) {
        return;
    }
    logOutputFile->lastLogRotateTime.timeSeconds = logOutputFile->lastLogTime.timeSeconds;
    FileDescriptor *oldFd = logOutputFile->dataFd;
    FullPathName fullPathName;
    fullPathName.directoryName = logFileConfig->logDirectory;
    fullPathName.fileName = logFileConfig->logFileName;
    char timePostfix[FORMATTED_TIME_MAX_LEN] = {0};
    (void)sprintf_s(timePostfix, sizeof(timePostfix), CURRENT_LOG_SUFFIX);
    fullPathName.appendName = timePostfix;
    fullPathName.extendName = GetExtendNameByFileType(ERR_LOG_FILE_TYPE);

    /* old name is current.log */
    char oldPathName[MAX_PATH] = {0};
    /* new name is timestamp.log */
    char newPathName[MAX_PATH] = {0};
    int ret = RenameLocalLogFile(&fullPathName, &(logOutputFile->lastLogTime), oldPathName, newPathName, MAX_PATH);
    if (ret == -1) {
        WriteErrMsg("fail to rename log file %s/%s-%s.%s.\n", fullPathName.directoryName, fullPathName.fileName,
                    fullPathName.appendName, fullPathName.extendName);
    }
    ErrorCode errCode = OpenLogDataFile(logFileConfig, logOutputFile);
    if (errCode != ERROR_SYS_OK) {
        ResetErrLogDirectory();
        g_errorLogLocalFileCfg.errorLogLocalFileCfgSeq += 1;
        SetLatch(&(g_loggerThreadContext.errorLogLatch));
        return;
    }

    errCode = Close(oldFd);
    if (errCode != ERROR_SYS_OK) {
        return;
    }
    ret = chmod(newPathName, S_IRUSR);
    if (ret == -1) {
        ErrLog(LOG, ErrMsg("fail to chmod log file."));
    }

    errCode = Truncate(logOutputFile->dataFd, 0);
    if (errCode != ERROR_SYS_OK) {
        return;
    }

    logOutputFile->currentSequenceNo++;
    char filePath[MAX_PATH] = {0};
    (void)memset_s(filePath, sizeof(filePath), 0, sizeof(filePath));
    uint32_t fileCount = GetFileInfoFromLocalDirectory(g_loggerThreadContext.errorLogFileConfig.logDirectory, filePath,
                                                       MAX_PATH, GetExtendNameByFileType(ERR_LOG_FILE_TYPE),
                                                       g_loggerThreadContext.errorLogFileConfig.logFileName);

    /* Notify log compression thread to compress logs */
    if (g_logCompressionThreadState == LOG_COMPRESSION_THREAD_STARTED) {
        LogCompressionWorkNotify();
    }

    /* Check the actual log file number */
    if (logOutputFile->currentSequenceNo != fileCount) {
        logOutputFile->currentSequenceNo = fileCount;
    }
    DeleteRedundantLogFile(&(logOutputFile->currentSequenceNo), (uint64_t)logFileConfig->maxSequenceNo,
                           GetExtendNameByFileType(ERR_LOG_FILE_TYPE));
}

/**
 * Rotate the log file to the next file.
 */
static void PLogFileRotate(LogFileConfig *logFileConfig, LogOutputFile *logOutputFile)
{
    Time(&(logOutputFile->lastPLogTime));
    if (logOutputFile->lastPLogTime.timeSeconds == logOutputFile->lastPLogRotateTime.timeSeconds) {
        return;
    }
    logOutputFile->lastPLogRotateTime.timeSeconds = logOutputFile->lastPLogTime.timeSeconds;
    FileDescriptor *oldFd = logOutputFile->profileFd;
    FullPathName fullPathName;
    fullPathName.directoryName = logFileConfig->pLogDirectory;
    fullPathName.fileName = logFileConfig->logFileName;
    char timePostfix[FORMATTED_TIME_MAX_LEN] = {0};
    (void)sprintf_s(timePostfix, sizeof(timePostfix), CURRENT_LOG_SUFFIX);
    fullPathName.appendName = timePostfix;
    fullPathName.extendName = GetExtendNameByFileType(PLOG_LOG_FILE_TYPE);
    char oldPathName[MAX_PATH] = {0};
    char newPathName[MAX_PATH] = {0};
    int ret = RenameLocalLogFile(&fullPathName, &(logOutputFile->lastPLogTime), oldPathName, newPathName, MAX_PATH);
    if (ret == -1) {
        WriteErrMsg("fail to rename plog file %s/%s-%s.%s.\n", fullPathName.directoryName, fullPathName.fileName,
                    fullPathName.appendName, fullPathName.extendName);
    }
    ErrorCode errCode = OpenPLogFile(logFileConfig, logOutputFile);
    if (errCode != ERROR_SYS_OK) {
        ResetPLogDirectory();
        g_errorLogLocalFileCfg.errorLogLocalFileCfgSeq += 1;
        SetLatch(&(g_loggerThreadContext.errorLogLatch));
        return;
    }

    errCode = Close(oldFd);
    if (errCode != ERROR_SYS_OK) {
        return;
    }
    ret = chmod(newPathName, S_IRUSR);
    if (ret == -1) {
        ErrLog(LOG, ErrMsg("fail to chmod log file."));
    }
    logOutputFile->currentProfileSequenceNo++;
    char filePath[MAX_PATH] = {0};
    logOutputFile->currentProfileSequenceNo = GetFileInfoFromLocalDirectory(
        g_loggerThreadContext.errorLogFileConfig.pLogDirectory, filePath, MAX_PATH,
        GetExtendNameByFileType(PLOG_LOG_FILE_TYPE), g_loggerThreadContext.errorLogFileConfig.logFileName);
    DeleteRedundantPLogFile(&(logOutputFile->currentProfileSequenceNo), logFileConfig->maxSequenceNo);

    errCode = Truncate(logOutputFile->profileFd, 0);
    if (errCode != ERROR_SYS_OK) {
        return;
    }
}

/**
 * Rotate the log file to the next file.
 */
static void AspLogFileRotate(LogFileConfig *logFileConfig, LogOutputFile *logOutputFile)
{
    Time(&(logOutputFile->lastAspLogTime));
    if (logOutputFile->lastAspLogTime.timeSeconds == logOutputFile->lastAspLogRotateTime.timeSeconds) {
        return;
    }
    logOutputFile->lastAspLogRotateTime.timeSeconds = logOutputFile->lastAspLogTime.timeSeconds;
    FileDescriptor *oldFd = logOutputFile->aspFd;
    FullPathName fullPathName;
    fullPathName.directoryName = logFileConfig->aspLogDirectory;
    fullPathName.fileName = logFileConfig->aspFileName;
    char timePostfix[FORMATTED_TIME_MAX_LEN] = {0};
    (void)sprintf_s(timePostfix, sizeof(timePostfix), CURRENT_LOG_SUFFIX);
    fullPathName.appendName = timePostfix;
    fullPathName.extendName = GetExtendNameByFileType(ASP_LOG_FILE_TYPE);
    char oldPathName[MAX_PATH] = {0};
    char newPathName[MAX_PATH] = {0};
    int ret = RenameLocalLogFile(&fullPathName, &(logOutputFile->lastAspLogTime), oldPathName, newPathName, MAX_PATH);
    if (ret == -1) {
        WriteErrMsg("fail to rename asp log file %s/%s-%s.%s.\n", fullPathName.directoryName, fullPathName.fileName,
                    fullPathName.appendName, fullPathName.extendName);
    }
    ErrorCode errCode = OpenAspLogFile(logFileConfig, logOutputFile);
    if (errCode != ERROR_SYS_OK) {
        ResetAspLogDirectory();
        g_errorLogLocalFileCfg.errorLogLocalFileCfgSeq += 1;
        SetLatch(&(g_loggerThreadContext.errorLogLatch));
        return;
    }

    errCode = Close(oldFd);
    if (errCode != ERROR_SYS_OK) {
        return;
    }
    ret = chmod(newPathName, S_IRUSR);
    if (ret == -1) {
        ErrLog(LOG, ErrMsg("fail to chmod log file."));
    }
    logOutputFile->currentAspSequenceNo++;
    char filePath[MAX_PATH] = {0};
    uint32_t fileCount = GetFileInfoFromLocalDirectory(g_loggerThreadContext.errorLogFileConfig.aspLogDirectory,
                                                       filePath, MAX_PATH, GetExtendNameByFileType(ASP_LOG_FILE_TYPE),
                                                       g_loggerThreadContext.errorLogFileConfig.aspFileName);
    /* Notify log compression thread to compress logs */
    if (g_logCompressionThreadState == LOG_COMPRESSION_THREAD_STARTED) {
        LogCompressionWorkNotify();
    }
    /* Check the actual log file number */
    if (logOutputFile->currentAspSequenceNo != fileCount) {
        logOutputFile->currentAspSequenceNo = fileCount;
    }
    DeleteRedundantAspLogFile(&(logOutputFile->currentAspSequenceNo), logFileConfig->maxSequenceNo);

    errCode = Truncate(logOutputFile->aspFd, 0);
    if (errCode != ERROR_SYS_OK) {
        return;
    }
}

/**
 * Rotate the log file to the next file.
 */
static void CsvLogFileRotate(LogFileConfig *logFileConfig, LogOutputFile *logOutputFile)
{
    Time(&(logOutputFile->lastCsvLogTime));
    if (logOutputFile->lastCsvLogTime.timeSeconds == logOutputFile->lastCsvLogRotateTime.timeSeconds) {
        return;
    }
    logOutputFile->lastCsvLogRotateTime.timeSeconds = logOutputFile->lastCsvLogTime.timeSeconds;
    FileDescriptor *oldFd = logOutputFile->csvFd;
    FullPathName fullPathName;
    fullPathName.directoryName = logFileConfig->logDirectory;
    fullPathName.fileName = logFileConfig->logFileName;
    char timePostfix[FORMATTED_TIME_MAX_LEN] = {0};
    (void)sprintf_s(timePostfix, sizeof(timePostfix), CURRENT_LOG_SUFFIX);
    fullPathName.appendName = timePostfix;
    fullPathName.extendName = GetExtendNameByFileType(CSV_LOG_FILE_TYPE);
    char oldPathName[MAX_PATH] = {0};
    char newPathName[MAX_PATH] = {0};
    int ret = RenameLocalLogFile(&fullPathName, &(logOutputFile->lastCsvLogTime), oldPathName, newPathName, MAX_PATH);
    if (ret == -1) {
        WriteErrMsg("fail to rename csv log file %s/%s-%s.%s.\n", fullPathName.directoryName, fullPathName.fileName,
                    fullPathName.appendName, fullPathName.extendName);
    }
    ErrorCode errCode = OpenCsvLogFile(logFileConfig, logOutputFile);
    if (errCode != ERROR_SYS_OK) {
        ResetErrLogDirectory();
        g_errorLogLocalFileCfg.errorLogLocalFileCfgSeq++;
        SetLatch(&(g_loggerThreadContext.errorLogLatch));
        return;
    }

    errCode = Close(oldFd);
    if (errCode != ERROR_SYS_OK) {
        return;
    }
    ret = chmod(newPathName, S_IRUSR);
    if (ret == -1) {
        ErrLog(LOG, ErrMsg("fail to chmod log file."));
    }
    logOutputFile->currentCsvSequenceNo++;
    char filePath[MAX_PATH] = {0};
    uint32_t fileCount = GetFileInfoFromLocalDirectory(g_loggerThreadContext.errorLogFileConfig.logDirectory, filePath,
                                                       MAX_PATH, GetExtendNameByFileType(CSV_LOG_FILE_TYPE),
                                                       g_loggerThreadContext.errorLogFileConfig.logFileName);
    /* Check the actual log file number */
    if (logOutputFile->currentCsvSequenceNo != fileCount) {
        logOutputFile->currentCsvSequenceNo = fileCount;
    }
    DeleteRedundantCsvLogFile(&(logOutputFile->currentCsvSequenceNo), logFileConfig->maxSequenceNo);

    errCode = Truncate(logOutputFile->csvFd, 0);
    if (errCode != ERROR_SYS_OK) {
        return;
    }
}

/**
 * Rotate the log file based on the size of the log file.
 */
static void LogLocalFileSizeRotate(LogFileConfig *logFileConfig, LogOutputFile *logOutputFile)
{
    ErrorCode errCode;
    int64_t fileSize;
    if (logFileConfig->logFileRotationSize == 0) {
        return;
    }
    errCode = GetSize(logOutputFile->dataFd, &fileSize);
    if (errCode != ERROR_SYS_OK) {
        return;
    }
    if (fileSize < logFileConfig->logFileRotationSize * 1024L) {
        return;
    }
    LogFileRotate(logFileConfig, logOutputFile);
}

/**
 * Rotate the log file based on the size of the log file.
 */
static void PLogLocalFileSizeRotate(LogFileConfig *logFileConfig, LogOutputFile *logOutputFile)
{
    ErrorCode errCode;
    int64_t fileSize;
    if (logFileConfig->logFileRotationSize == 0) {
        return;
    }
    errCode = GetSize(logOutputFile->profileFd, &fileSize);
    if (errCode != ERROR_SYS_OK) {
        return;
    }
    if (fileSize < logFileConfig->logFileRotationSize * 1024L) {
        return;
    }
    PLogFileRotate(logFileConfig, logOutputFile);
}

/**
 * Rotate the log file based on the size of the log file.
 */
static void AspLogLocalFileSizeRotate(LogFileConfig *logFileConfig, LogOutputFile *logOutputFile)
{
    ErrorCode errCode;
    int64_t fileSize;
    if (logFileConfig->logFileRotationSize == 0) {
        return;
    }
    errCode = GetSize(logOutputFile->aspFd, &fileSize);
    if (errCode != ERROR_SYS_OK) {
        return;
    }
    if (fileSize < logFileConfig->logFileRotationSize * 1024L) {
        return;
    }
    AspLogFileRotate(logFileConfig, logOutputFile);
}

/**
 * Rotate the log file based on the size of the log file.
 */
static void CsvLogLocalFileSizeRotate(LogFileConfig *logFileConfig, LogOutputFile *logOutputFile)
{
    ErrorCode errCode;
    int64_t fileSize;
    if (logFileConfig->logFileRotationSize == 0) {
        return;
    }
    errCode = GetSize(logOutputFile->csvFd, &fileSize);
    if (errCode != ERROR_SYS_OK) {
        return;
    }
    if (fileSize < logFileConfig->logFileRotationSize * 1024L) {
        return;
    }
    CsvLogFileRotate(logFileConfig, logOutputFile);
}

/**
 * Rotate the error log file based on the size of the log file.
 */
static void ErrorLogFileSizeRotate(void)
{
    LogLocalFileSizeRotate(&(g_loggerThreadContext.errorLogFileConfig), &(g_loggerThreadContext.errorLogOutputFile));
    if (g_loggerThreadContext.remoteLogIsReady) {
        LogRemoteFileSizeRotate();
    }
}

#define SECONDS_PER_MINUTE 60

/*
 * Determine the next planned rotation time, and store in nextRotationTime.
 */
static void SetLogNextRotationTime(const LogFileConfig *logFileConfig, LogOutputFile *logOutputFile)
{
    if (logFileConfig->logFileRotationAge == 0) {
        return;
    }
    uint32_t rotateInterval = logFileConfig->logFileRotationAge * SECONDS_PER_MINUTE;
    Time(&(logOutputFile->nextRotationTime));
    logOutputFile->nextRotationTime.timeSeconds += (time_t)rotateInterval;
}

/**
 * Rotate the log file based on the time of the log file.
 */
static void LogFileTimeRotate(LogFileConfig *logFileConfig, LogOutputFile *logOutputFile)
{
    if (logFileConfig->logFileRotationAge == 0) {
        return;
    }
    TimesSecondsSinceEpoch time;
    FAULT_INJECTION_CALL_REPLACE(MOCK_LOG_ROTATION_TIME, &time)
    Time(&time);
    FAULT_INJECTION_CALL_REPLACE_END;
    if (time.timeSeconds < logOutputFile->nextRotationTime.timeSeconds) {
        return;
    }
    if (logOutputFile->dataFd != NULL) {
        LogFileRotate(logFileConfig, logOutputFile);
    }
    if (logOutputFile->profileFd != NULL) {
        PLogFileRotate(logFileConfig, logOutputFile);
    }
    if (logOutputFile->aspFd != NULL) {
        AspLogFileRotate(logFileConfig, logOutputFile);
    }
    if (logOutputFile->csvFd != NULL) {
        CsvLogFileRotate(logFileConfig, logOutputFile);
    }
    SetLogNextRotationTime(logFileConfig, logOutputFile);
}

/**
 * Rotate the log file based on the time of the log file.
 */
static void ErrorLogFileTimeRotate(void)
{
    LogFileTimeRotate(&(g_loggerThreadContext.errorLogFileConfig), &(g_loggerThreadContext.errorLogOutputFile));
}

/*
 * Determine the error log next planned rotation time, and store in nextRotationTime.
 */
static void SetErrorLogNextRotationTime(void)
{
    SetLogNextRotationTime(&(g_loggerThreadContext.errorLogFileConfig), &(g_loggerThreadContext.errorLogOutputFile));
}

static void WriteFoldLogToFile(const char *msgBuf, size_t msgLen, uint64_t foldCount)
{
#define FOLD_MSG_LEN 64
    char foldMsgBuf[FOLD_MSG_LEN] = {0};
    int ret = sprintf_s(foldMsgBuf, sizeof(foldMsgBuf), "[Fold %lu times] ", foldCount);
    if (ret >= 0) {
        WriteLocalLogDataFile(foldMsgBuf, strlen(foldMsgBuf));
    }
    WriteLocalLogDataFile(msgBuf, msgLen);
}

static void WriteFoldLogAndFreeMessage(const LogFoldContent *logFoldContent)
{
    if (logFoldContent->foldCount > 0) {
        ASSERT(logFoldContent->msgBuf != NULL);
        WriteFoldLogToFile(logFoldContent->msgBuf, logFoldContent->msgLen, logFoldContent->foldCount);
        ErrorLogFileSizeRotate();
    }
    MemFree(logFoldContent->msgBuf);
}

static void GetFoldLogAndWrite(LogFoldContext *context, bool checkTime, time_t curTime)
{
#define FOLD_LOG_ARRAY_LEN 64
    LogFoldContent contentArray[FOLD_LOG_ARRAY_LEN];
    for (;;) {
        uint64_t foldLogCount = FoldLogGetContent(context, checkTime, curTime, contentArray, FOLD_LOG_ARRAY_LEN);
        ASSERT(foldLogCount <= FOLD_LOG_ARRAY_LEN);
        if (foldLogCount == 0) {
            break;
        }
        for (uint64_t i = 0; i < foldLogCount; ++i) {
            WriteFoldLogAndFreeMessage(&(contentArray[i]));
        }
    }
}

/* Extend name is log file. */
static bool ExtendNameIsLogFileName(char *extendName, int fileType)
{
    switch (fileType) {
        case ERR_LOG_FILE_TYPE:
            if (strcmp(extendName, ERR_LOG_EXTEND) != 0) {
                return false;
            }
            break;
        case AUDIT_LOG_FILE_TYPE:
            if (strcmp(extendName, AUDIT_LOG_EXTEND) != 0) {
                return false;
            }
            break;
        case SQL_LOG_FILE_TYPE:
            if (strcmp(extendName, ERR_LOG_EXTEND) != 0) {
                return false;
            }
            break;
        case ASP_LOG_FILE_TYPE:
            if (strcmp(extendName, ERR_LOG_EXTEND) != 0) {
                return false;
            }
            break;
        case PLOG_LOG_FILE_TYPE:
            if (strcmp(extendName, PROFILE_LOG_EXTEND) != 0) {
                return false;
            }
            break;
        case CSV_LOG_FILE_TYPE:
            if (strcmp(extendName, CSV_LOG_EXTEND) != 0) {
                return false;
            }
            break;
        default:
            return false;
    }
    return true;
}
/**
 * Check whether a file is a log file.
 * @param fileName: Name of the file to be checked.
 * @param logFileConfig: LogFileConfig.
 * @return:true or false.
 */
static bool LogFileNameCheck(char *fileName, LogFileConfig *logFileConfig)
{
    /*
     * The following processing will modify the character string.
     * Therefore, copy the character string first.
     */
    char fullFileName[MAX_PATH];
    errno_t rc = strcpy_s(fullFileName, MAX_PATH, fileName);
    if (rc != EOK) {
        return false;
    }
    char *basename = Basename(fullFileName);

    /* Process extend name. */
    char *str = basename;
    while ((*str != '\0') && (*str != '.')) {
        str++;
    }
    if (*str != '.') {
        return false;
    }
    char *extendName = str + 1;
    int fileType = logFileConfig->fileType;
    if (!ExtendNameIsLogFileName(extendName, fileType)) {
        return false;
    }
    *str = '\0';

    /* Process append name. */
    while ((str > basename) && (*str != '_')) {
        str--;
    }
    char *appendName = str + 1;
    if (isalpha(*appendName)) {
        if (strcmp(appendName, "meta") != 0) {
            return false;
        }
    } else if (isdigit(*appendName)) {
        char *strEnd;
        uint32_t sequenceNo = (uint32_t)strtol(appendName, &strEnd, NUMBER_BASE);
        if (sequenceNo > logFileConfig->maxSequenceNo) {
            return false;
        }
    } else {
        return false;
    }
    *str = '\0';

    /* Process file name. */
    if (strcmp(basename, logFileConfig->logFileName) != 0) {
        return false;
    }

    return true;
}

/**
 * Check whether a file is a current configuration error log file.
 * @param fileName:Name of the file to be checked.
 * @return:true or false.
 */
static bool FileIsCurrentErrorLogFile(char *fileName)
{
    return LogFileNameCheck(fileName, &(g_loggerThreadContext.errorLogFileConfig));
}

/**
 * Check whether a file is a current configuration log file.
 * @param fileName: Name of the file to be checked.
 * @return: true or false.
 */
static bool FileIsCurrentLogFile(char *fileName)
{
    bool rc = false;
    rc = FileIsCurrentErrorLogFile(fileName);
    if (rc) {
        return true;
    }
    return false;
}

/**
 * Check whether a file is a log file.
 * @param fileName: Name of the file to be checked.
 * @return: true or false.
 */
static bool FileIsLogFile(char *fileName)
{
    char *errlogPos = strstr(fileName, ERR_LOG_EXTEND);
    char *auditlogPos = strstr(fileName, AUDIT_LOG_EXTEND);
    char *plogPos = strstr(fileName, PROFILE_LOG_EXTEND);
    char *csvlogPos = strstr(fileName, CSV_LOG_EXTEND);
    if ((errlogPos != NULL) || (auditlogPos != NULL) || (plogPos != NULL) || (csvlogPos != NULL)) {
        return true;
    } else {
        return false;
    }
}

/**
 * Remote file system log file verification.
 * If these log files are no longer used, delete the log files from the storage and delete the assigned file ID.
 */
static void RemoteLogFileVerification(void)
{
    if ((g_loggerThreadContext.getLogFileFun == NULL) || (g_loggerThreadContext.freeLogFileFun == NULL) ||
        (g_loggerThreadContext.removeLogFileIdFun == NULL)) {
        return;
    }
    char **fileName = NULL;
    int count;
    fileName = g_loggerThreadContext.getLogFileFun(&count);
    if ((fileName == NULL) || (count == 0)) {
        return;
    }
    for (int i = 0; i < count; i++) {
        if (!FileIsLogFile(fileName[i])) {
            continue;
        }
        if (FileIsCurrentLogFile(fileName[i])) {
            continue;
        }
        const char *storeSpaceNames = "storeSpaceName1";
        ErrorCode errorCode = g_loggerThreadContext.getLogFileIdFun(fileName[i], storeSpaceNames);
        if (errorCode != ERROR_SYS_OK) {
            continue;
        }
        errorCode = Remove(g_loggerThreadContext.remoteVfs, fileName[i]);
        if (errorCode != ERROR_SYS_OK) {
            continue;
        }
        g_loggerThreadContext.removeLogFileIdFun(fileName[i]);
    }
    g_loggerThreadContext.freeLogFileFun(fileName);
}

/**
 * Check whether the files in the path are log files.
 * @param dirName: Directory name.
 */
static void LocalDirectoryCheck(char *dirName)
{
    Directory dir;
    ErrorCode errorCode = OpenDirectory(dirName, &dir);
    if (errorCode != ERROR_SYS_OK) {
        return;
    }
    bool result = true;
    DirectoryEntry dirEntry;
    errno_t rc;
    char fullPathFileName[MAX_PATH];
    while (result) {
        result = ReadDirectory(&dir, &dirEntry);
        if (!result) {
            break;
        }
        if (dirEntry.type != DIR_TYPE_REGULAR_FILE) {
            continue;
        }
        if ((strcmp(dirEntry.name, ".") == 0) || (strcmp(dirEntry.name, "..") == 0)) {
            continue;
        }
        if (!FileIsLogFile(dirEntry.name)) {
            continue;
        }
        if (FileIsCurrentLogFile(dirEntry.name)) {
            continue;
        }

        rc = memset_s(fullPathFileName, MAX_PATH, 0, MAX_PATH);
        SecurecCheck(rc, false);
        rc = sprintf_s(fullPathFileName, MAX_PATH, "%s/%s", dirName, dirEntry.name);
        SecurecCheck(rc, true);
        errorCode = Remove(g_loggerThreadContext.localVfs, fullPathFileName);
        if (errorCode != ERROR_SYS_OK) {
            ErrLog(ERROR, ErrMsg("Failed to remove file = %s.\n", fullPathFileName));
        }
    }
    CloseDirectory(&dir);
}

/**
 * Check whether the files in the error log path are error log files.
 */
static void LocalErrorLogDirectoryCheck(void)
{
    LocalDirectoryCheck(g_loggerThreadContext.errorLogFileConfig.logDirectory);
}

/**
 * Local File System Log File Verification.
 * If these log files are no longer used, delete the log files from the storage.
 */
void LocalLogFileVerification(void)
{
    LocalErrorLogDirectoryCheck();
}

/**
 * Log File Verification.Check whether the information in the actually used log file is
 * consistent with that in other files.
 */
#define LOG_FILE_VERIFICATION_PERIOD (SECONDS_PER_MINUTE * 5) /* 5 minutes. */

static void LogFileVerification(void)
{
    static TimesSecondsSinceEpoch nextCheckTime = {0}; /* Next rotate time. */
    if (nextCheckTime.timeSeconds == 0) {
        Time(&nextCheckTime);
        nextCheckTime.timeSeconds += LOG_FILE_VERIFICATION_PERIOD;
        return;
    }
    TimesSecondsSinceEpoch currentTime;
    Time(&currentTime);
    if (currentTime.timeSeconds < nextCheckTime.timeSeconds) {
        return;
    }
    uint32_t logFileDestination = GetErrorLogFileDestination();
    if ((logFileDestination & LOG_DESTINATION_LOCAL_FILE) != 0) {
        LocalLogFileVerification();
    }
    if ((logFileDestination & LOG_DESTINATION_REMOTE_FILE) != 0) {
        RemoteLogFileVerification();
    }
    nextCheckTime.timeSeconds += LOG_FILE_VERIFICATION_PERIOD;
}

/**
 * Get the logger thread tid.
 * @return:Tid.
 */
Tid GetLoggerThreadTid(void)
{
    return g_loggerThreadContext.tid;
}

/**
 * Get the log compression thread tid.
 * @return:Tid.
 */
Tid GetLogCompressionThreadTid(void)
{
    return g_logCompressionThreadContext.tid;
}

/**
 * Get the logger thread id.
 * @return:tid.
 */
UTILS_EXPORT pthread_t GetLoggerThreadId(void)
{
    return g_loggerThreadContext.tid.tid;
}

/**
 * Get the pipe read fd.
 * @return:fd.
 */
int GetMessagePipe(void)
{
    return g_loggerThreadContext.messagePipe[1];
}

/**
 * Get the logger thread error log memory context.
 * @return:MemoryContext.
 */
MemoryContext GetLoggerThreadErrorLogContext(void)
{
    return g_loggerThreadContext.errorLogContext;
}

/*
 * Bind module text domain. Set the path for a module domain.
 * If not set module text domain,the module use the process text domain.
 * @param compName   : Component name.
 * @param modName    : Module name.
 * @param domainName : Text domain name.
 * @return :ErrorCode.
 */
UTILS_EXPORT ErrorCode BindModuleTextDomain(const char *componentName, const char *moduleName, const char *domainName,
                                            const char *dirName)
{
    uint32_t count = g_loggerThreadContext.domainCount;
    if (count >= MAX_MODULE_TEXT_DOMAIN) {
        return ERROR_UTILS_ERRORLOG_OUT_OF_RANGE;
    }
    ErrorCode errorCode = SetErrLogXXName(g_loggerThreadContext.logContext, componentName,
                                          &g_loggerThreadContext.moduleTextDomain[count].componentName);
    if (errorCode != ERROR_SYS_OK) {
        return errorCode;
    }
    errorCode = SetErrLogXXName(g_loggerThreadContext.logContext, moduleName,
                                &g_loggerThreadContext.moduleTextDomain[count].moduleName);
    if (errorCode != ERROR_SYS_OK) {
        return errorCode;
    }
    errorCode = SetErrLogXXName(g_loggerThreadContext.logContext, domainName,
                                &g_loggerThreadContext.moduleTextDomain[count].domainName);
    if (errorCode != ERROR_SYS_OK) {
        return errorCode;
    }
    char *currentDirName = bindtextdomain(domainName, dirName);
    errorCode = SetErrLogXXName(g_loggerThreadContext.logContext, currentDirName,
                                &g_loggerThreadContext.moduleTextDomain[count].currentDirName);
    if (errorCode != ERROR_SYS_OK) {
        return errorCode;
    }
    g_loggerThreadContext.domainCount++;
    return ERROR_SYS_OK;
}

/*
 * Find module text domain. if not set module text domain,the module use the process text domain.
 * @param compName   : Component name.
 * @param modName    : Module name.
 * @return :Text domain name.
 */
const char *FindModuleTextDomain(const char *componentName, const char *moduleName)
{
    uint32_t i;
    for (i = 0; i < g_loggerThreadContext.domainCount; i++) {
        if (strcmp(g_loggerThreadContext.moduleTextDomain[i].componentName, LOG_STRING_WILDCARD) != 0 &&
            strcmp(g_loggerThreadContext.moduleTextDomain[i].componentName, componentName) != 0) {
            continue;
        }
        if (strcmp(g_loggerThreadContext.moduleTextDomain[i].moduleName, LOG_STRING_WILDCARD) != 0 &&
            strcmp(g_loggerThreadContext.moduleTextDomain[i].moduleName, moduleName) != 0) {
            continue;
        }
        return g_loggerThreadContext.moduleTextDomain[i].domainName;
    }
    return NULL;
}

/**
 * Attempt to translate a text string into the user's native language,
 * by looking up the translation in a message catalog.
 * @param domainName:Domain name.
 * @param msgId: Message identification.
 * @return:Translated message.
 */
const char *DomainGetText(const char *domainName, const char *msgId)
{
    if (domainName == NULL) {
        return msgId;
    } else {
        return dgettext(domainName, msgId);
    }
}

/*
 * Stop error log.
 */
static void StopErrorLog(void)
{
    CloseLogDataFile(&(g_loggerThreadContext.errorLogOutputFile));
    ClosePLogFile(&(g_loggerThreadContext.errorLogOutputFile));
    CloseSlowQueryLogFile(&(g_loggerThreadContext.errorLogOutputFile));
    CloseAspLogFile(&(g_loggerThreadContext.errorLogOutputFile));
    CloseCsvLogFile(&(g_loggerThreadContext.errorLogOutputFile));
    ReleaseRemoteLogResource();
    g_loggerThreadContext.remoteLogIsStopPending = false;
}

#define GAUSSDB "gaussdb"
#define UT_NAME "utils_unittest"
void MakeLogDir(void)
{
    if (strcmp(g_loggerThreadContext.processName, GAUSSDB) == 0 ||
        strcmp(g_loggerThreadContext.processName, UT_NAME) == 0) {
        (void)MakeErrLogDirectories(g_loggerThreadContext.errorLogFileConfig.logDirectory,
                                    DIRECTORY_MODE_OWNER);
        if (strcmp(g_loggerThreadContext.errorLogFileConfig.pLogDirectory, "") != 0) {
            (void)MakeErrLogDirectories(g_loggerThreadContext.errorLogFileConfig.pLogDirectory, DIRECTORY_MODE_OWNER);
        }
        (void)MakeErrLogDirectories(g_loggerThreadContext.errorLogFileConfig.slowQueryLogDirectory,
                                    DIRECTORY_MODE_OWNER);
        (void)MakeErrLogDirectories(g_loggerThreadContext.errorLogFileConfig.aspLogDirectory,
                                    DIRECTORY_MODE_OWNER);
        (void)MakeErrLogDirectories(g_loggerThreadContext.errorLogFileConfig.FFICLogDirectory,
                                    DIRECTORY_MODE_OWNER);
    } else {
        (void)MakeErrLogDirectories(g_loggerThreadContext.errorLogFileConfig.logDirectory,
                                    DIRECTORY_MODE_OWNER);
    }
}

/*
 * Start error log.
 */
static ErrorCode StartErrorLog(void)
{
    if (g_loggerThreadContext.errorLogFileConfig.logDirectory == NULL) {
        return ERROR_UTILS_ERRORLOG_DIRECTORY_IS_NULL;
    }
    uint32_t logFileDestination = GetErrorLogFileDestination();
    if ((logFileDestination & LOG_DESTINATION_LOCAL_FILE) != 0) {
        /* Do not check the return value. If the directory already exists, ignore it. */
        MakeLogDir();
    }
    char filePath[MAX_PATH] = {0};
    uint32_t logFileCount = GetFileInfoFromLocalDirectory(
        g_loggerThreadContext.errorLogFileConfig.logDirectory, filePath, MAX_PATH,
        GetExtendNameByFileType(ERR_LOG_FILE_TYPE), g_loggerThreadContext.errorLogFileConfig.logFileName);
    if (logFileCount == UINT32_MAX) {
        return ERROR_UTILS_COMMON_SECURE_C_FUNCTION_FAILED;
    }

    uint32_t pLogFileCount = GetFileInfoFromLocalDirectory(
        g_loggerThreadContext.errorLogFileConfig.pLogDirectory, filePath, MAX_PATH,
        GetExtendNameByFileType(PLOG_LOG_FILE_TYPE), g_loggerThreadContext.errorLogFileConfig.logFileName);
    if (pLogFileCount == UINT32_MAX) {
        return ERROR_UTILS_COMMON_SECURE_C_FUNCTION_FAILED;
    }

    uint32_t sqlLogFileCount = GetFileInfoFromLocalDirectory(
        g_loggerThreadContext.errorLogFileConfig.slowQueryLogDirectory, filePath, MAX_PATH,
        GetExtendNameByFileType(SQL_LOG_FILE_TYPE), g_loggerThreadContext.errorLogFileConfig.slowQueryFileName);
    if (sqlLogFileCount == UINT32_MAX) {
        return ERROR_UTILS_COMMON_SECURE_C_FUNCTION_FAILED;
    }

    uint32_t aspLogFileCount = GetFileInfoFromLocalDirectory(
        g_loggerThreadContext.errorLogFileConfig.aspLogDirectory, filePath, MAX_PATH,
        GetExtendNameByFileType(ASP_LOG_FILE_TYPE), g_loggerThreadContext.errorLogFileConfig.aspFileName);
    if (aspLogFileCount == UINT32_MAX) {
        return ERROR_UTILS_COMMON_SECURE_C_FUNCTION_FAILED;
    }

    uint32_t csvLogFileCount = GetFileInfoFromLocalDirectory(
        g_loggerThreadContext.errorLogFileConfig.logDirectory, filePath, MAX_PATH,
        GetExtendNameByFileType(CSV_LOG_FILE_TYPE), g_loggerThreadContext.errorLogFileConfig.logFileName);
    if (csvLogFileCount == UINT32_MAX) {
        return ERROR_UTILS_COMMON_SECURE_C_FUNCTION_FAILED;
    }

    g_loggerThreadContext.errorLogOutputFile.currentSequenceNo = logFileCount;
    g_loggerThreadContext.errorLogOutputFile.currentProfileSequenceNo = pLogFileCount;
    g_loggerThreadContext.errorLogOutputFile.currentSqlSequenceNo = sqlLogFileCount;
    g_loggerThreadContext.errorLogOutputFile.currentAspSequenceNo = aspLogFileCount;
    g_loggerThreadContext.errorLogOutputFile.currentCsvSequenceNo = csvLogFileCount;

    DeleteRedundantLogFile(&(g_loggerThreadContext.errorLogOutputFile.currentSequenceNo),
                           (uint64_t)g_loggerThreadContext.errorLogFileConfig.maxSequenceNo,
                           GetExtendNameByFileType(ERR_LOG_FILE_TYPE));
    DeleteRedundantPLogFile(&(g_loggerThreadContext.errorLogOutputFile.currentProfileSequenceNo),
                            g_loggerThreadContext.errorLogFileConfig.maxSequenceNo);
    DeleteRedundantSlowQueryLogFile(&(g_loggerThreadContext.errorLogOutputFile.currentSqlSequenceNo),
                                    g_loggerThreadContext.errorLogFileConfig.maxSequenceNo);
    DeleteRedundantAspLogFile(&(g_loggerThreadContext.errorLogOutputFile.currentAspSequenceNo),
                              g_loggerThreadContext.errorLogFileConfig.maxSequenceNo);
    DeleteRedundantCsvLogFile(&(g_loggerThreadContext.errorLogOutputFile.currentCsvSequenceNo),
                              g_loggerThreadContext.errorLogFileConfig.maxSequenceNo);
    g_loggerThreadContext.errorLogOutputFile.lastLogRotateTime.timeSeconds = 0;
    g_loggerThreadContext.errorLogOutputFile.lastPLogRotateTime.timeSeconds = 0;
    g_loggerThreadContext.errorLogOutputFile.lastSlowQueryLogRotateTime.timeSeconds = 0;
    g_loggerThreadContext.errorLogOutputFile.lastAspLogRotateTime.timeSeconds = 0;
    g_loggerThreadContext.errorLogOutputFile.lastCsvLogRotateTime.timeSeconds = 0;
    SetErrorLogNextRotationTime();
    return ERROR_SYS_OK;
}

/*
 * Simple signal handler for triggering a configuration modify.Normally, this handler would be used for SIGHUP.
 * The idea is that code which uses it would arrange to check the configModifyPending flag at convenient places
 * inside main loops.
 */
static void SignalHandlerForConfigModify(SYMBOL_UNUSED int sigNo)
{
    g_loggerThreadContext.configModifyPending = true;
}

static ErrorCode SwitchErrLogLocalFileCfg(const ErrorLogLocalFileConfigure *localFileConfigure)
{
    LogFileConfig oldCfg = g_loggerThreadContext.errorLogFileConfig;
    /* Switch error log local file configure seq regardless switch result */
    ErrorCode errorCode =
        SetErrLogXXName(g_loggerThreadContext.errorLogContext, localFileConfigure->localErrorLogDirectory,
                        &g_loggerThreadContext.errorLogFileConfig.logDirectory);

    uint64_t tmp = (uint64_t)errorCode;
    tmp |= (uint64_t)SetErrLogXXName(g_loggerThreadContext.errorLogContext, localFileConfigure->localPLogDirectory,
                                     &g_loggerThreadContext.errorLogFileConfig.pLogDirectory);

    tmp |=
        (uint64_t)SetErrLogXXName(g_loggerThreadContext.errorLogContext, localFileConfigure->localSlowQueryLogDirectory,
                                  &g_loggerThreadContext.errorLogFileConfig.slowQueryLogDirectory);

    tmp |= (uint64_t)SetErrLogXXName(g_loggerThreadContext.errorLogContext, localFileConfigure->localAspLogDirectory,
                                     &g_loggerThreadContext.errorLogFileConfig.aspLogDirectory);
    errorCode = (ErrorCode)tmp;

    if (errorCode != ERROR_SYS_OK) {
        /* if SetErrLogXXName not ok, only when the memory not alloc ok, no need release memory */
        return errorCode;
    }
    SetLoggerThreadLocalFileCfgSeq(localFileConfigure->errorLogLocalFileCfgSeq);
    SetErrLogDirectorySpaceSize(localFileConfigure->errorLogTotalSpace);
    SetErrLogFileRotationSize(localFileConfigure->errorLogFileRotationSize);
    /* Do not check the return value. If the directory already exists, ignore it. */
    MakeLogDir();
    /* Close old log file descriptor */
    CloseLogDataFile(&(g_loggerThreadContext.errorLogOutputFile));
    ClosePLogFile(&(g_loggerThreadContext.errorLogOutputFile));
    CloseSlowQueryLogFile(&(g_loggerThreadContext.errorLogOutputFile));
    CloseAspLogFile(&(g_loggerThreadContext.errorLogOutputFile));
    CloseCsvLogFile(&(g_loggerThreadContext.errorLogOutputFile));
    /* Free old directory memory */
    MemFree(oldCfg.logDirectory);
    MemFree(oldCfg.pLogDirectory);
    MemFree(oldCfg.slowQueryLogDirectory);
    MemFree(oldCfg.aspLogDirectory);
    return ERROR_SYS_OK;
}

#define LOG_RECEIVE_MESSAGE_WAITTIMEOUT 10   /* unit:milliseconds. */
#define SLEEP_GAP_US                    1000 /* 1 millisecond */
#define SLEEP_TWO_SECOND                2    /* 2 second */

static void ErrLogMessageProcess(char *msgPtr, size_t msgLen, struct LogStatTime *logStatTime, uint32_t *count,
                                 ThreadMessagePipeHeader *pipeHeader)
{
    char *tmpMsg = (char *)MemoryContextAlloc(g_loggerThreadContext.errorLogContext, msgLen + 1);
    if (tmpMsg == NULL) {
        WriteErrMsg("Failed to malloc in ErrLogMessageProcess\n");
        return;
    }
    errno_t ret = strncpy_s(tmpMsg, msgLen + 1, msgPtr, msgLen);
    if (unlikely(ret != EOK)) {
        MemFree(tmpMsg);
        WriteErrMsg("Failed to copy msg in ErrLogMessageProcess\n");
        return;
    }
    logStatTime->curStatTime =
        (uint64_t)(GetCurrentTimeValue().seconds * MSEC_PRE_SEC + GetCurrentTimeValue().useconds / MSEC_PRE_SEC);
    if (*count == 0) {
        logStatTime->preStatTime = logStatTime->curStatTime;
    }
    LogIdentifier *logIdentifier = &pipeHeader->msgContext;
    if (IsNeededLogFlowControl(g_loggerThreadContext.logFlowContext, logIdentifier, logStatTime, count)) {
        if (!IsNeedLogFilter(g_loggerThreadContext.logFlowContext, logIdentifier)) {
            (void)sleep(SLEEP_TWO_SECOND);
        } else {
            (void)sleep(SLEEP_TWO_SECOND);
            MemFree(tmpMsg);
            return;
        }
    }
    LogFoldContent logFoldContent = {0};
    bool needFold =
        LogFoldIsNeedFold(g_loggerThreadContext.logFoldContext, logIdentifier, tmpMsg, msgLen, &logFoldContent);

    /* Write fold log first if needed */
    if (logFoldContent.msgBuf != NULL) {
        WriteFoldLogAndFreeMessage(&logFoldContent);
    }

    /* Write origin log based on fold log result */
    if (!needFold) {
        if (g_errorLogConfigure.isBatchWrite) {
            BatchWriteLog(&g_loggerThreadContext.msgBatchData, tmpMsg, msgLen, LOG_BATCH_BUF_SIZE, RemoteLogIsReady());
        } else {
            WriteErrorLogDataFile(tmpMsg, msgLen, RemoteLogIsReady());
        }
        MemFree(tmpMsg);
        ErrorLogFileSizeRotate();
    }
}

static void NonErrLogMessageProcess(char *msgPtr, size_t msgLen, uint8_t msgType)
{
    switch (msgType) {
        case PROFILE_LOG_MESSAGE_TYPE:
            WritePLogFile(msgPtr, msgLen, RemoteLogIsReady());
            PLogLocalFileSizeRotate(&(g_loggerThreadContext.errorLogFileConfig),
                                    &(g_loggerThreadContext.errorLogOutputFile));
            break;
        case ASP_LOG_MESSAGE_TYPE:
            WriteAspLogFile(msgPtr, msgLen, RemoteLogIsReady());
            AspLogLocalFileSizeRotate(&(g_loggerThreadContext.errorLogFileConfig),
                                      &(g_loggerThreadContext.errorLogOutputFile));
            break;
        case CSV_LOG_MESSAGE_TYPE:
            WriteCsvLogFile(msgPtr, msgLen, RemoteLogIsReady());
            CsvLogLocalFileSizeRotate(&(g_loggerThreadContext.errorLogFileConfig),
                                      &(g_loggerThreadContext.errorLogOutputFile));
            break;
        default:
            break;
    }
}

static inline void MigrateLoggerState(LogThreadState logThreadState)
{
    g_loggerThreadContext.logThreadState = logThreadState;
    MutexLock(&(g_loggerThreadContext.mutex));
    ConditionVariableSignal(&(g_loggerThreadContext.cond));
    MutexUnlock(&(g_loggerThreadContext.mutex));
}

static inline void SendLoggerFlushFinishSignal(void)
{
    g_loggerThreadContext.isNeedFlush = false;
    MutexLock(&(g_loggerThreadContext.flushMutex));
    ConditionVariableSignal(&(g_loggerThreadContext.flushCond));
    MutexUnlock(&(g_loggerThreadContext.flushMutex));
}

static bool CheckPipeHeader(ThreadMessagePipeHeader p)
{
    if (p.nuls[0] == '\0' && p.nuls[1] == '\0' && p.chunkLen > 0 && p.chunkLen <= THREAD_MESSAGE_PIPE_MAX_PAYLOAD &&
        p.tid != 0 && (p.isLast == 't' || p.isLast == 'T' || p.isLast == 'f' || p.isLast == 'F') &&
        p.msgType <= CSV_LOG_MESSAGE_TYPE && p.magic == PIPE_HEADER_MAGICNUM)
        return true;
    return false;
}

typedef struct SaveBuffer SaveBuffer;
struct SaveBuffer {
    pthread_t tid;
    StringInfoData data;
};

typedef struct SaveBufDListNode SaveBufDListNode;
struct SaveBufDListNode {
    DListNode node;
    SaveBuffer buf;
};

typedef struct ErrMsgInfo ErrMsgInfo;
struct ErrMsgInfo {
    char *msgPtr;
    size_t msgLen;
};

static void WriteErrorLog(ErrMsgInfo *errMsgInfo, uint8_t msgType, struct LogStatTime *logStatTime, uint32_t *count,
                          ThreadMessagePipeHeader *pipeHeader)
{
    if (errMsgInfo->msgLen != 0) {
        switch (msgType) {
            case ERROR_LOG_MESSAGE_TYPE: {
                if (g_loggerThreadContext.errorLogOutputFile.dataFd == NULL) {
                    ErrorCode errorCode = OpenLogDataFile(&(g_loggerThreadContext.errorLogFileConfig),
                                                          &(g_loggerThreadContext.errorLogOutputFile));
                    if (errorCode != ERROR_SYS_OK) {
                        WriteErrMsg("Failed to OpenLogDataFile.\n");
                        return;
                    }
                }
                ErrLogMessageProcess(errMsgInfo->msgPtr, errMsgInfo->msgLen, logStatTime, count, pipeHeader);
                break;
            }
            case PROFILE_LOG_MESSAGE_TYPE: {
                if (g_loggerThreadContext.errorLogOutputFile.profileFd == NULL) {
                    Time(&(g_loggerThreadContext.errorLogOutputFile.lastPLogTime));
                    ErrorCode errorCode = OpenPLogFile(&(g_loggerThreadContext.errorLogFileConfig),
                                                       &(g_loggerThreadContext.errorLogOutputFile));
                    if (errorCode != ERROR_SYS_OK) {
                        StopErrorLog();
                        WriteErrMsg("Failed to OpenPLogFile.\n");
                        return;
                    }
                }
                NonErrLogMessageProcess(errMsgInfo->msgPtr, errMsgInfo->msgLen, msgType);
                break;
            }
            case SQL_LOG_MESSAGE_TYPE: {
                if (g_loggerThreadContext.errorLogOutputFile.sqlFd == NULL) {
                    ErrorCode errorCode = OpenSlowQueryLogFile(&(g_loggerThreadContext.errorLogFileConfig),
                                                               &(g_loggerThreadContext.errorLogOutputFile));
                    if (errorCode != ERROR_SYS_OK) {
                        StopErrorLog();
                        WriteErrMsg("Failed to OpenSlowQueryLogFile.\n");
                        return;
                    }
                }
                NonErrLogMessageProcess(errMsgInfo->msgPtr, errMsgInfo->msgLen, msgType);
                break;
            }
            case ASP_LOG_MESSAGE_TYPE: {
                if (g_loggerThreadContext.errorLogOutputFile.aspFd == NULL) {
                    ErrorCode errorCode = OpenAspLogFile(&(g_loggerThreadContext.errorLogFileConfig),
                                                         &(g_loggerThreadContext.errorLogOutputFile));
                    if (errorCode != ERROR_SYS_OK) {
                        StopErrorLog();
                        WriteErrMsg("Failed to OpenAspLogFile.\n");
                        return;
                    }
                }
                NonErrLogMessageProcess(errMsgInfo->msgPtr, errMsgInfo->msgLen, msgType);
                break;
            }
            case CSV_LOG_MESSAGE_TYPE: {
                if (g_loggerThreadContext.errorLogOutputFile.csvFd == NULL) {
                    ErrorCode errorCode = OpenCsvLogFile(&(g_loggerThreadContext.errorLogFileConfig),
                                                         &(g_loggerThreadContext.errorLogOutputFile));
                    if (errorCode != ERROR_SYS_OK) {
                        StopErrorLog();
                        WriteErrMsg("Failed to OpenCsvLogFile.\n");
                        return;
                    }
                }
                NonErrLogMessageProcess(errMsgInfo->msgPtr, errMsgInfo->msgLen, msgType);
                break;
            }
            default:
                break;
        }
    }
}

static void ProcessLastChunkOfMessage(SaveBuffer *existingBuf, SaveBuffer *freeBuf, DListHead *bufferList,
                                      char *chunkStart, ThreadMessagePipeHeader *msgHeader)
{
    StringInfo str;
    /*
     * Save a complete non-final chunk in a per-pid buffer
     */
    if (existingBuf != NULL) {
        /* Add chunk to data from preceding chunks */
        str = &(existingBuf->data);
        (void)AppendBinaryString(str, chunkStart + THREAD_MESSAGE_PIPE_HEADER_LEN, (size_t)msgHeader->chunkLen);
    } else {
        /* First chunk of message, save in a new buffer */
        if (freeBuf == NULL) {
            /*
             * Need a free slot, but there isn't one in the list,
             * so create a new one and extend the list with it.
             */
            SaveBufDListNode *freeSlot = (SaveBufDListNode *)MemoryContextAlloc(
                g_loggerThreadContext.pipeErrorMsgContext, sizeof(SaveBufDListNode));
            if (freeSlot == NULL) {
                ErrLog(ERROR, ErrMsg("Can not allocate memory to freeSlot"));
                return;
            }
            DListPushTail(bufferList, &freeSlot->node);
            g_loggerThreadContext.bufferLists[msgHeader->tid % BUFFER_LISTS_NUMBER] = *bufferList;
            freeBuf = &(freeSlot->buf);
        }
        freeBuf->tid = msgHeader->tid;
        str = &(freeBuf->data);

        InitString(g_loggerThreadContext.pipeErrorMsgContext, str);
        (void)AppendBinaryString(str, chunkStart + THREAD_MESSAGE_PIPE_HEADER_LEN, (size_t)msgHeader->chunkLen);
    }
    g_loggerThreadContext.isFinalChunk = false;
}

static void ProcessPipeInput(char *logBuffer, ssize_t *bytesInLogbuffer, struct LogStatTime *logStatTime,
                             uint32_t *count)
{
    char *cursor = logBuffer;
    ssize_t bytesCount = *bytesInLogbuffer;

    /* While we have enough for a header, process data... */
    while (bytesCount >= (ssize_t)sizeof(ThreadMessagePipeHeader)) {
        ThreadMessagePipeHeader p;
        ErrMsgInfo errMsgInfo;
        size_t chunkLen;

        errno_t err = memcpy_s(&p, sizeof(ThreadMessagePipeHeader), cursor, sizeof(ThreadMessagePipeHeader));
        if (err != EOK) {
            return;
        }

        if (CheckPipeHeader(p)) {
            DListHead bufferList;
            DListMutableIter cell;
            SaveBuffer *freeBuf = NULL;
            SaveBuffer *existingBuf = NULL;
            StringInfo str;

            chunkLen = THREAD_MESSAGE_PIPE_HEADER_LEN + p.chunkLen;

            /* Fall out of loop if we don't have the whole chunk yet */
            if (bytesCount < (ssize_t)chunkLen) {
                break;
            }

            /* Locate any existing buffer for this source pid */
            bufferList = g_loggerThreadContext.bufferLists[p.tid % BUFFER_LISTS_NUMBER];
            DLIST_FOR_EACH(cell, &bufferList)
            {
                SaveBufDListNode *curNode = (SaveBufDListNode *)DLIST_CONTAINER(SaveBufDListNode, node, cell.cur);
                if (curNode->buf.tid == p.tid) {
                    existingBuf = &curNode->buf;
                    break;
                }
                bool flag = curNode->buf.tid == 0 && freeBuf == NULL;
                if (flag) {
                    freeBuf = &curNode->buf;
                }
            }

            bool flag = p.isLast == 'f' || p.isLast == 'F';
            if (flag) {
                ProcessLastChunkOfMessage(existingBuf, freeBuf, &bufferList, cursor, &p);
            } else {
                if (p.msgType == 0) {
                    p.msgType = (p.isLast == 'T' ? CSV_LOG_MESSAGE_TYPE : ERROR_LOG_MESSAGE_TYPE);
                }
                /*
                 * Final chunk --- add it to anything saved for that pid, and
                 * either way write the whole thing out.
                 */
                if (existingBuf != NULL) {
                    str = &(existingBuf->data);
                    (void)AppendBinaryString(str, cursor + THREAD_MESSAGE_PIPE_HEADER_LEN, (size_t)p.chunkLen);
                    errMsgInfo.msgPtr = str->data;
                    errMsgInfo.msgLen = str->len;
                    WriteErrorLog(&errMsgInfo, p.msgType, logStatTime, count, &p);

                    /* Mark the buffer unused, and reclaim string storage */
                    existingBuf->tid = 0;
                    MemFree(str->data);
                } else {
                    /* The whole message was one chunk, evidently. */
                    errMsgInfo.msgPtr = cursor + THREAD_MESSAGE_PIPE_HEADER_LEN;
                    errMsgInfo.msgLen = p.chunkLen;
                    WriteErrorLog(&errMsgInfo, p.msgType, logStatTime, count, &p);
                }
                g_loggerThreadContext.isFinalChunk = true;
            }

            /* Finished processing this chunk */
            cursor += chunkLen;
            bytesCount -= (ssize_t)chunkLen;
        } else {
            for (chunkLen = 1; (ssize_t)chunkLen < bytesCount; chunkLen++) {
                if (cursor[chunkLen] == '\0') {
                    break;
                }
            }
            /* Finished processing this chunk */
            errMsgInfo.msgPtr = cursor;
            errMsgInfo.msgLen = chunkLen;
            WriteErrorLog(&errMsgInfo, ERROR_LOG_MESSAGE_TYPE, logStatTime, count, &p);
            cursor += chunkLen;
            bytesCount -= (ssize_t)chunkLen;
        }
    }

    /* We don't have a full chunk, so left-align what remains in the buffer */
    if (bytesCount > 0 && cursor != logBuffer) {
        errno_t rc = memmove_s(logBuffer, (size_t)bytesCount, cursor, (size_t)bytesCount);
        ASSERT(rc == EOK);
        if (rc != EOK) {
            return;
        }
    }
    *bytesInLogbuffer = bytesCount;
}

static void SignalHandlerForLatch(SYMBOL_UNUSED int sigNo)
{
    g_loggerThreadContext.isNeedFlush = true;
    if (g_loggerThreadContext.wait) {
        SendSelfPipeByte();
    }
}

static void SetPollTimeout(long *curTimeout, unsigned int *curFlags)
{
#define DEFAULT_TIMEOUT 1000 /* msec */
    *curTimeout = DEFAULT_TIMEOUT;
    *curFlags = WL_TIMEOUT;
}

static void MainWorkerLoop(void)
{
    ErrorCode errorCode;
    ssize_t bytesInLogbuffer = 0;
    LogStatTime statTime;
    uint32_t count = 0;
    TimesSecondsSinceEpoch lastTime, curTime;
    Time(&lastTime);
    for (;;) {
        ResetLatch(&(g_loggerThreadContext.errorLogLatch));
        /* Check configure seq, update error log file configure if seq changed */
        if (g_loggerThreadContext.errorLogLocalFileCfgSeq != g_errorLogLocalFileCfg.errorLogLocalFileCfgSeq) {
            errorCode = SwitchErrLogLocalFileCfg(&g_errorLogLocalFileCfg);
            if (errorCode != ERROR_SYS_OK) {
                ErrLog(WARNING,
                       ErrMsg("Switch error log local file configure failed, using old configure, code = %lld\n",
                              errorCode));
            }
        }

        ErrorCode errCode = ERROR_SYS_OK;
#define READ_BUF_SIZE (THREAD_MESSAGE_PIPE_CHUNK_SIZE * 2)
        char logBuffer[READ_BUF_SIZE];
        long curTimeout = 0;
        unsigned int curFlags = 0;
        SetPollTimeout(&curTimeout, &curFlags);

        uint32_t rc =
            WaitLatchOrSocket(&(g_loggerThreadContext.errorLogLatch), WL_LATCH_SET | WL_SOCKET_READABLE | curFlags,
                              g_loggerThreadContext.messagePipe[0], curTimeout);
        if (rc & WL_SOCKET_READABLE) {
            ssize_t bytesRead = 0;

            bytesRead = (int)read(g_loggerThreadContext.messagePipe[0], logBuffer + bytesInLogbuffer,
                                  sizeof(logBuffer) - (size_t)bytesInLogbuffer);
            if (bytesRead < 0) {
                if (errno != EINTR && errno != EAGAIN) {
                    PosixErrorCode2PortErrorCode(errno, &errCode);
                    ErrLog(ERROR, ErrMsg("could not read from logger pipe, errCode = %lld\n", errCode));
                }
            } else if (bytesRead > 0) {
                bytesInLogbuffer += bytesRead;
                ProcessPipeInput(logBuffer, &bytesInLogbuffer, &statTime, &count);
                g_loggerThreadContext.pipeEofSeen = false;
                if (!g_loggerThreadContext.isFinalChunk) {
                    continue;
                }
            } else {
                /* EOF detected */
                g_loggerThreadContext.pipeEofSeen = true;
                FlushBatchLog(&g_loggerThreadContext.msgBatchData, false);
            }
        } else if (rc & WL_LATCH_SET) {
            g_loggerThreadContext.isNeedFlush = true;
        }

        ErrorLogFileTimeRotate();
        if (g_loggerThreadContext.enableLogFileVerification) {
            LogFileVerification();
        }

        if (g_loggerThreadContext.stopPending) {
            /* Print all fold log before exit logger thread */
            if (g_loggerThreadContext.logFoldContext != NULL) {
                GetFoldLogAndWrite(g_loggerThreadContext.logFoldContext, false, 0);
            }
            FlushBatchLog(&g_loggerThreadContext.msgBatchData, RemoteLogIsReady());
            break;
        }

        if (g_loggerThreadContext.isNeedFlush && (rc & WL_TIMEOUT)) {
            /* Flush all error log in thread message queue */
            FlushBatchLog(&g_loggerThreadContext.msgBatchData, RemoteLogIsReady());
            SendLoggerFlushFinishSignal();
        }

        if (g_loggerThreadContext.remoteLogIsStopPending) {
            /* Write buffer logs to log file before releasing remote log resource */
            FlushBatchLog(&g_loggerThreadContext.msgBatchData, true);
            ReleaseRemoteLogResource();
            g_loggerThreadContext.remoteLogIsStopPending = false;
        }

        Time(&curTime);
        if (g_loggerThreadContext.logFoldContext != NULL) {
            /* Check fold log per second */
            if (curTime.timeSeconds != lastTime.timeSeconds) {
                GetFoldLogAndWrite(g_loggerThreadContext.logFoldContext, true, curTime.timeSeconds);
            }
        }
        lastTime = curTime;
    }
}

/*
 * When the postmaster thread initiates the log thread, the postmaster itself has not yet blocked signals,
 * so the log thread needs to block the relevant signals on its own.
 *
 * Based on the signal blocking set referenced from the postmaster, add an additional blocking for the SIGUSR2 signal,
 * as this signal is currently not needed by the logging thread.
 */
static void LoggerMaskSignal(void)
{
    sigset_t mask;
    (void)sigfillset(&mask);
    (void)sigdelset(&mask, SIG_PROF);
    (void)sigdelset(&mask, SIG_SEGV);
    (void)sigdelset(&mask, SIG_BUS);
    (void)sigdelset(&mask, SIG_FPE);
    (void)sigdelset(&mask, SIG_ILL);
    (void)sigdelset(&mask, SIG_SYS);
    (void)pthread_sigmask(SIG_SETMASK, &mask, NULL);
}

/* Main log processing thread, invokes corresponding processing functions based on the received message type. */
void *LoggerMainThread(SYMBOL_UNUSED void *arg)
{
    ErrorCode errorCode;
    (void)ThreadSetName("Logger thread");
    SetFileIOMode(stderr, FILE_IO_MODE_TEXT);
    if (GetErrorLogSignalUsage()) {
        (void)signal(SIG_HUP, SignalHandlerForConfigModify);
        (void)signal(SIG_USR1, SignalHandlerForLatch);
    } else {
        LoggerMaskSignal();
    }
    /* Opens the error log of the logger thread self. */
    const int checkOpenLoggerRepeat = 1000;
    FAULT_INJECTION_CALL_REPLACE(MOCK_OPEN_LOG_FAILED, &errorCode)
    for (int i = 0; i < checkOpenLoggerRepeat; ++i) {
        errorCode = OpenLogger();
        if (errorCode == ERROR_SYS_OK) {
            MigrateLoggerState(LOG_THREAD_STATE_READY);
            break;
        }
        Usleep(SLEEP_GAP_US);
    }
    FAULT_INJECTION_CALL_REPLACE_END;

    if (errorCode != ERROR_SYS_OK) {
        MigrateLoggerState(LOG_THREAD_STATE_FAILED);
        return (void *)&g_loggerThreadContext;
    }

    g_loggerThreadContext.isNeedFlush = false;
    ConditionVariableInit(&(g_loggerThreadContext.flushCond));
    MutexInit(&(g_loggerThreadContext.flushMutex));
    InitializeLatchSupport();
    InitLatch(&(g_loggerThreadContext.errorLogLatch));
    MainWorkerLoop();

    /* Write buffer logs to log file before exiting logger thread */
    FlushBatchLog(&g_loggerThreadContext.msgBatchData, false);

    /* Exit log compression thread */
    LogCompressionThreadExit();

    CloseLogger();
    StopErrorLog();
    return (void *)&g_loggerThreadContext;
}

/*
 * Open log,initialize log client.
 * @return ErrorCode
 */
UTILS_EXPORT ErrorCode OpenLogger(void)
{
    return OpenErrorLog();
}

/*
 * Close log,destroy log client.
 */
UTILS_EXPORT void CloseLogger(void)
{
    CloseErrorLog();
}

#define LOGGER_THREAD_NOT_STARTED 0
#define LOGGER_THREAD_STARTING    1
#define LOGGER_THREAD_STARTED     2

int g_loggerThreadState = LOGGER_THREAD_NOT_STARTED;

int g_stdoutCopyFd;
int g_stderrCopyFd;

void StdRedirectToLogFile(void)
{
    if (!GetErrorLogRedirectionDone() && IsErrLogNeedRedirect()) {
        g_stdoutCopyFd = dup(fileno(stdout));
        g_stderrCopyFd = dup(fileno(stderr));
        fflush(stdout);
        if (dup2(g_loggerThreadContext.messagePipe[1], fileno(stdout)) < 0) {
            ErrLog(FATAL, ErrMsg("Failed to redirect stdout.\n"));
        }
        fflush(stderr);
        if (dup2(g_loggerThreadContext.messagePipe[1], fileno(stderr)) < 0) {
            ErrLog(FATAL, ErrMsg("Failed to redirect stderr.\n"));
        }
        /* Now we are done with the write end of the pipe. */
        close(g_loggerThreadContext.messagePipe[1]);
        g_loggerThreadContext.messagePipe[1] = -1;
        SetErrorLogRedirectionDone(true);
    }
}

static size_t GetPipeMaxSize(void)
{
#define PIPE_MAX_SIZE_PATH "/proc/sys/fs/pipe-max-size"
    FILE *fp = fopen(PIPE_MAX_SIZE_PATH, "r");
    if (!fp) {
        fprintf(stderr, "fopen /proc/sys/fs/pipe-max-size failed, errno is %d\n", errno);
        return 0;
    }

    size_t max_size;
    if (fscanf_s(fp, "%zu", &max_size) != 1) {
        fprintf(stderr, "Failed to read pipe-max-size, errno is %d\n", errno);
        (void)fclose(fp);
        return 0;
    }

    (void)fclose(fp);
    return max_size;
}

static ErrorCode StartLoggerInternal(void)
{
    if (g_loggerThreadState == LOGGER_THREAD_STARTED) {
        return ERROR_SYS_OK;
    } else if (g_loggerThreadState == LOGGER_THREAD_STARTING) {
        return ERROR_UTILS_ERRORLOG_THREAD_STARTING;
    } else {
        g_loggerThreadState = LOGGER_THREAD_STARTING;
    }
    (void)memset_s(&g_loggerThreadContext, sizeof(g_loggerThreadContext), 0, sizeof(g_loggerThreadContext));
    ErrorCode errorCode;
    if (!InitLoggerContext()) {
        char *buf = "Failed to init logger context.\n";
        WriteSyslog(EVENT_LOG_ERROR, buf, strlen(buf));
        errorCode = ERROR_UTILS_ERRORLOG_CONTEXT_INITL_FAIL;
        goto START_LOGGER_FAIL;
    }
    errorCode = InitLoggerThreadConfigParameters();
    FAULT_INJECTION_ACTION(MOCK_INIT_LOG_CFG_PARAM_FAILED, (errorCode = ERROR_UTILS_COMMON_INVALID_PARAMETER));
    if (errorCode != ERROR_SYS_OK) {
        char errBuf[LOG_ERROR_BUF_SIZE] = {0};
        (void)sprintf_s(errBuf, sizeof(errBuf), "Failed to init logger config, code = %lld.\n", errorCode);
        FAULT_INJECTION_CALL_REPLACE(MOCK_WRITE_SYSLOG, errBuf)
        WriteSyslog(EVENT_LOG_ERROR, errBuf, strlen(errBuf));
        FAULT_INJECTION_CALL_REPLACE_END;
        goto START_LOGGER_FAIL;
    }
    errorCode = InitVfsModule(NULL);
    FAULT_INJECTION_ACTION(MOCK_INIT_VFS_FAILED, (errorCode = VFS_ERROR_PARAMETERS_INVALID));
    if ((errorCode != ERROR_SYS_OK) && (errorCode != VFS_ERROR_VFS_MODULE_ALREADY_INIT)) {
        ErrLog(ERROR, ErrMsg("Failed to initialize the local file system.\n"));
        goto START_LOGGER_FAIL;
    }
    g_loggerThreadContext.enableLogFileVerification = false;
    if (errorCode != VFS_ERROR_VFS_MODULE_ALREADY_INIT) {
        g_loggerThreadContext.localVfsInitByLoggerThread = true;
    }
    VirtualFileSystem *vfs;
    errorCode = GetStaticLocalVfsInstance(&vfs);
    if (errorCode != ERROR_SYS_OK) {
        WriteErrMsg("Failed to get static vfs instance.\n");
        goto START_LOGGER_FAIL;
    }
    SetLogLocalVfs(vfs);
    errorCode = StartErrorLog();
    if (errorCode != ERROR_SYS_OK) {
        WriteErrMsg("Failed to start the error log module.\n");
        goto START_LOGGER_FAIL;
    }
    /* Create log fold context */
    g_loggerThreadContext.logFoldContext =
        LogFoldAllocContext(g_loggerThreadContext.errorLogContext, g_errorLogConfigure.foldLevel,
                            g_errorLogConfigure.foldThreshold, g_errorLogConfigure.foldPeriod);
    if (g_loggerThreadContext.logFoldContext == NULL) {
        WriteErrMsg("Create fold log context failed, fold log ability disabled\n");
    }

    /* Create log filter context */
    g_loggerThreadContext.logFlowContext = LogFlowInitContext(
        g_errorLogConfigure.flowControlThreshold, g_errorLogConfigure.filterLevel, g_errorLogConfigure.logStatPeriod);
    if (g_loggerThreadContext.logFoldContext == NULL) {
        WriteErrMsg("Create filter log context failed, flow control ability disabled\n");
    }

    /*
     * The initialization of communication between all threads must be put into the StartLogger function.
     * Otherwise, a random time window exists. After the StartLogger is successful, some communication
     * variables between threads are not initialized. As a result, the OpenLogger thread uses uninitialized
     * variables, causing exceptions.
     */
    /* Create message pipe */
    if (pipe(g_loggerThreadContext.messagePipe) < 0) {
        WriteErrMsg("Failed to create the message pipe.\n");
    }

    if (isPipeEnlargeActive()) {
        /* Expand syslog pipe buffer size to 1M */
        int curBufSize = fcntl(g_loggerThreadContext.messagePipe[1], F_GETPIPE_SZ);
        if (curBufSize < 0) {
            (void)fprintf(stderr, "fcntl() failed to get pipe size, errno is %d\n", errno);
        }

#define BUFSIZE 1048576
        if (curBufSize > 0 && curBufSize < BUFSIZE) {
            size_t pipeMaxSize = GetPipeMaxSize();
            if (pipeMaxSize <= 0) {
                (void)fprintf(stderr, "GetPipeMaxSize failed, use default value BUFSIZE\n");
            }

            size_t setSize = ((pipeMaxSize > 0 && pipeMaxSize < BUFSIZE) ? pipeMaxSize : BUFSIZE);
            if (curBufSize < (int)setSize) {
                int ret = fcntl(g_loggerThreadContext.messagePipe[1], F_SETPIPE_SZ, setSize);
                if (ret < 0) {
                    (void)fprintf(stderr, "fcntl() failed to set pipe size, errno is %d\n", errno);
                }
            }
        }
#undef BUFSIZE
    }

    g_loggerThreadContext.msgBatchData.msgBatchLen = 0;
    ThreadStartRoutine routineFunc = GetThreadFunc();
    g_loggerThreadContext.logThreadState = LOG_THREAD_STATE_RUNNING;

    ConditionVariableInit(&(g_loggerThreadContext.cond));
    MutexInit(&(g_loggerThreadContext.mutex));
    MutexLock(&(g_loggerThreadContext.mutex));
    errorCode = ThreadCreate(&(g_loggerThreadContext.tid), routineFunc, NULL);
    if (errorCode != ERROR_SYS_OK) {
        WriteErrMsg("Failed to create the log thread.\n");
        MutexUnlock(&(g_loggerThreadContext.mutex));
        goto START_LOGGER_FAIL1;
    }

    StdRedirectToLogFile();
    if (g_loggerThreadContext.logThreadState == LOG_THREAD_STATE_RUNNING) {
        (void)ConditionVariableTimedWait(&(g_loggerThreadContext.cond), &(g_loggerThreadContext.mutex), SLEEP_GAP_US);
    }

    MutexUnlock(&(g_loggerThreadContext.mutex));

    if (g_loggerThreadContext.logThreadState == LOG_THREAD_STATE_FAILED) {
        errorCode = ERROR_UTILS_ERRORLOG_THREAD_NOT_STARTED;
        WriteErrMsg("Failed to start log.\n");
        goto START_LOGGER_FAIL1;
    }

    g_loggerThreadState = LOGGER_THREAD_STARTED;
    /* Logger thread is ready, switch to log destination to local file */
    SetErrLogDestination(LOG_DESTINATION_LOCAL_FILE);
    if (GetErrorLogCompress()) {
        StartLogCompression();
    }

    return ERROR_SYS_OK;

START_LOGGER_FAIL1:
    /* null context is ok to delete */
    StopErrorLog();
    LogFoldFreeContext(g_loggerThreadContext.logFoldContext);
    g_loggerThreadContext.logFoldContext = NULL;
    LogFlowFreeContext(g_loggerThreadContext.logFlowContext);
    g_loggerThreadContext.logFlowContext = NULL;

START_LOGGER_FAIL:
    /* null context is ok to delete */
    FreeLoggerThreadContext(&g_loggerThreadContext);
    g_loggerThreadState = LOGGER_THREAD_NOT_STARTED;
    MutexDestroy(&(g_loggerThreadContext.mutex));
    ConditionVariableDestroy(&(g_loggerThreadContext.cond));
    return errorCode;
}

/*
 * Start logger. all parameters except previously set parameters are set to default values.
 * if you need change those config params, you call the previous series of SetXXX().
 * @return ErrorCode
 */
UTILS_EXPORT ErrorCode StartLogger(void)
{
    SetErrorLogSignalUsage(true);
    return StartLoggerInternal();
}

/*
 * Start logger only for gaussdb process. all parameters except previously set parameters are set to default values.
 * if you need change those config params, you call the previous series of SetXXX().
 * @return ErrorCode
 */
UTILS_EXPORT ErrorCode StartLoggerNoSignal(void)
{
    SetErrorLogSignalUsage(false);
    return StartLoggerInternal();
}

/*
 * Stop logger,destroy and close related resources.
 */
UTILS_EXPORT void StopLogger(void)
{
    if (g_loggerThreadState != LOGGER_THREAD_STARTED) {
        if (GetErrorLogRedirectionDone()) {
            if (dup2(g_stdoutCopyFd, fileno(stdout)) < 0) {
                ErrLog(FATAL, ErrMsg("Failed to reset stdout.\n"));
            }
            if (dup2(g_stderrCopyFd, fileno(stderr)) < 0) {
                ErrLog(FATAL, ErrMsg("Failed to reset stderr.\n"));
            }
            SetErrorLogRedirectionDone(false);
        }
        /* Logger thread is not active, return directly */
        return;
    }
    SetErrLogDestination(DEFAULT_LOG_DESTINATION);
    g_loggerThreadContext.stopPending = true;
    FAULT_INJECTION_CALL_REPLACE(MOCK_SET_LATCH_FAILED, NULL)
    SetLatch(&(g_loggerThreadContext.errorLogLatch));
    FAULT_INJECTION_CALL_REPLACE_END;
    ErrorCode errorCode;

    Tid tid1 = GetLoggerThreadTid();
    Tid tid2;
    SetTid(&tid2, 0);
    void *valuePtr;
    if (!TidIsEqual(&tid1, &tid2)) {
        /* reset stdout and stderr */
        if (GetErrorLogRedirectionDone()) {
            if (dup2(g_stdoutCopyFd, fileno(stdout)) < 0) {
                ErrLog(FATAL, ErrMsg("Failed to reset stdout.\n"));
            }
            if (dup2(g_stderrCopyFd, fileno(stderr)) < 0) {
                ErrLog(FATAL, ErrMsg("Failed to reset stderr.\n"));
            }
            SetErrorLogRedirectionDone(false);
        }

        errorCode = ThreadJoin(g_loggerThreadContext.tid, (void **)&valuePtr);
        if (errorCode != ERROR_SYS_OK) {
            WriteErrMsg("Failed to wait logger thread exit.\n");
        }
    }

    int tmpFd;
    if (g_loggerThreadContext.selfPipeReadFd >= 0) {
        ASSERT(g_loggerThreadContext.selfPipeReadFd >= 0);
        tmpFd = g_loggerThreadContext.selfPipeReadFd;
        g_loggerThreadContext.selfPipeReadFd = -1;
        close(tmpFd);
    }

    if (g_loggerThreadContext.selfPipeWriteFd >= 0) {
        ASSERT(g_loggerThreadContext.selfPipeWriteFd >= 0);
        tmpFd = g_loggerThreadContext.selfPipeWriteFd;
        g_loggerThreadContext.selfPipeWriteFd = -1;
        close(tmpFd);
    }

    if (g_loggerThreadContext.logFoldContext != NULL) {
        LogFoldFreeContext(g_loggerThreadContext.logFoldContext);
        g_loggerThreadContext.logFoldContext = NULL;
    }

    if (g_loggerThreadContext.logFlowContext != NULL) {
        LogFlowFreeContext(g_loggerThreadContext.logFlowContext);
        g_loggerThreadContext.logFlowContext = NULL;
    }

    /*
     * If the logger thread initializes the local file system, the logger thread call ExitVfsModule to exit vfs.
     * This processing is tricky. In addition, an exception occurs if the local file system is initialized by
     * the logger module and other modules access the file system after the logger stops.However, this is the
     * best method when file system initialization is not handled according to reference count.
     */
    if (g_loggerThreadContext.localVfsInitByLoggerThread) {
        if (g_loggerThreadContext.localVfs != NULL) {
            errorCode = ExitVfsModule();
            if (errorCode != ERROR_SYS_OK) {
                WriteErrMsg("Failed to exit vfs module.\n");
            }
        }
    }

    if (g_loggerThreadContext.logContext != NULL) {
        FreeLoggerThreadContext(&g_loggerThreadContext);
    }

    MutexDestroy(&(g_loggerThreadContext.mutex));
    ConditionVariableDestroy(&(g_loggerThreadContext.cond));

    g_loggerThreadState = LOGGER_THREAD_NOT_STARTED;
}

/*
 * Whether the thread log have opened.
 */
UTILS_EXPORT bool IsLoggerStarted(void)
{
    return g_loggerThreadState == LOGGER_THREAD_STARTED ? true : false;
}

uint64_t GetErrorLogLocalFileCfgSeq(void)
{
    return g_loggerThreadContext.errorLogLocalFileCfgSeq;
}

bool GetRemoteLogStoppingState(void)
{
    return g_loggerThreadContext.remoteLogIsStopPending;
}

uint32_t GetRemoteLogCurSeqNum(void)
{
    return g_loggerThreadContext.remoteLogContext.currentSequenceNo;
}

int64_t GetRemoteLogCurFileSize(void)
{
    return g_loggerThreadContext.remoteLogContext.currentDataFileSize;
}

UTILS_EXPORT void FlushLogger(void)
{
    time_t waitTime = 2000; // milliseconds
    g_loggerThreadContext.isNeedFlush = true;
    SetLatch(&(g_loggerThreadContext.errorLogLatch));

    MutexLock(&(g_loggerThreadContext.flushMutex));
    while (g_loggerThreadContext.isNeedFlush) {
        bool rc = ConditionVariableTimedWait(&(g_loggerThreadContext.flushCond), &(g_loggerThreadContext.flushMutex),
                                             waitTime);
        if (!rc) {
            g_loggerThreadContext.isNeedFlush = false;
            ErrLog(ERROR, ErrMsg("Waiting the flush state changing to finish timeout."));
            break;
        }
    }
    MutexUnlock(&(g_loggerThreadContext.flushMutex));
}

bool GetErrorLogRedirectionDone(void)
{
    return g_loggerThreadContext.redirection_done;
}

/*
 * Init log compression thread context
 */
static void InitLogCompressionContext(void)
{
    g_logCompressionThreadContext.logCompressionExitFlag = false;
    g_logCompressionThreadContext.compressEnableFlag = false;
    ConditionVariableInit(&(g_logCompressionThreadContext.cond));
    MutexInit(&(g_logCompressionThreadContext.mutex));
}

static void TraverseDirAndCompressExistFFICLogFile(char *filePath, char *compressdLogName, LogOutputFile *logOutputFile)
{
    char sourceErrLogFileName[MAX_PATH] = {0};
    int ret;
    Directory dir;
    char resolvedLogDir[MAX_PATH] = {0};
    if (realpath(g_loggerThreadContext.errorLogFileConfig.FFICLogDirectory, resolvedLogDir) == NULL) {
        ErrLog(ERROR, ErrMsg("Invalid log directory path"));
        return;
    }
    ErrorCode errCode = OpenDirectory(resolvedLogDir, &dir);
    if (errCode != ERROR_SYS_OK) {
        return;
    }
    DirectoryEntry dirEntry;
    while (ReadDirectory(&dir, &dirEntry)) {
        if (dirEntry.type != DIR_TYPE_REGULAR_FILE) {
            continue;
        }
        if (!strcmp(dirEntry.name, ".") || !strcmp(dirEntry.name, "..")) {
            continue;
        }
        char *baseFileName = Basename(dirEntry.name);
        uint64_t fileTimeStamp = 0;
        /* Check the validation of file name or file time stamp. */
        if (IsErrLogFileNameValid(sourceErrLogFileName, baseFileName, &fileTimeStamp,
            ERR_LOG_EXTEND, FFIC_FILE_NAME)
            || IsErrLogFileNameValid(sourceErrLogFileName, baseFileName, &fileTimeStamp,
            ERR_LOG_EXTEND, PLSQL_FFIC_FILE_NAME)) {
            /* Compress the existing error log file */
            GenerateErrorLogFilePath(filePath, MAX_PATH, resolvedLogDir, baseFileName);
            (void)memset_s(compressdLogName, sizeof(compressdLogName), 0, sizeof(compressdLogName));
            errCode = CompressLogFile(filePath, logOutputFile, compressdLogName);
            if (errCode != ERROR_SYS_OK) {
                ErrLog(ERROR, ErrMsg("fail to compress log file, errcode is :%lld.\n", errCode));
                continue;
            } else {
                /* delete log file after successfully compressed log file */
                ret = unlink(filePath);
                if (ret == -1) {
                    PosixErrorCode2PortErrorCode(errno, &errCode);
                    continue;
                } else {
                    logOutputFile->currentSequenceNo -= 1;
                }
            }
        }
    }
    CloseDirectory(&dir);
}

static void DeleteRedundantFFICFile(uint32_t curSeqNo, uint32_t maxSeqNo, char *fileSuffixStr)
{
    while (curSeqNo > maxSeqNo) {
        char filePath[MAX_PATH] = {0};
        (void)GetFileInfoFromLocalDirectory(g_loggerThreadContext.errorLogFileConfig.FFICLogDirectory,
                                            filePath, MAX_PATH, fileSuffixStr, FFIC_FILE_NAME);
        int rc;
        FAULT_INJECTION_CALL_REPLACE(MOCK_DELETE_LOG_FILE_FAILED, &rc);
        rc = unlink(filePath);
        FAULT_INJECTION_CALL_REPLACE_END;
        if (unlikely(rc == -1)) {
            ErrLog(ERROR, ErrMsg("Delete oldest ffic log file %s failed.\n", filePath));
        } else {
            curSeqNo -= 1;
        }
        (void)GetFileInfoFromLocalDirectory(g_loggerThreadContext.errorLogFileConfig.FFICLogDirectory,
                                            filePath, MAX_PATH, fileSuffixStr, PLSQL_FFIC_FILE_NAME);
        FAULT_INJECTION_CALL_REPLACE(MOCK_DELETE_LOG_FILE_FAILED, &rc);
        rc = unlink(filePath);
        FAULT_INJECTION_CALL_REPLACE_END;
        if (unlikely(rc == -1)) {
            ErrLog(ERROR, ErrMsg("Delete oldest ffic log file %s failed.\n", filePath));
        } else {
            curSeqNo -= 1;
        }
    }
}

static void DeleteRedundantCompressedFFICLogFile(void)
{
    char filePath[MAX_PATH] = {0};
    uint32_t compressedFileCount = GetFileInfoFromLocalDirectory(
        g_loggerThreadContext.errorLogFileConfig.FFICLogDirectory,
        filePath, MAX_PATH, ZIP_LOG_EXTEND,
        FFIC_FILE_NAME) +
        GetFileInfoFromLocalDirectory(
        g_loggerThreadContext.errorLogFileConfig.FFICLogDirectory,
        filePath, MAX_PATH, ZIP_LOG_EXTEND,
        PLSQL_FFIC_FILE_NAME);
    if (compressedFileCount > COMPRESSD_FFIC_COUNT) {
        DeleteRedundantFFICFile(compressedFileCount, COMPRESSD_FFIC_COUNT, ZIP_LOG_EXTEND);
    }
    
    return;
}

static void LogCompressionFFICDoCompress(void)
{
    char compressdLogName[MAX_PATH] = {0};
    char filePath[MAX_PATH] = {0};
    (void)memset_s(filePath, sizeof(filePath), 0, sizeof(filePath));
    uint32_t fileCount = GetFileInfoFromLocalDirectory(g_loggerThreadContext.errorLogFileConfig.FFICLogDirectory,
                                                        filePath, MAX_PATH,
                                                        GetExtendNameByFileType(ERR_LOG_FILE_TYPE),
                                                        FFIC_FILE_NAME);
    fileCount += GetFileInfoFromLocalDirectory(g_loggerThreadContext.errorLogFileConfig.FFICLogDirectory,
                                                        filePath, MAX_PATH,
                                                        GetExtendNameByFileType(ERR_LOG_FILE_TYPE),
                                                        PLSQL_FFIC_FILE_NAME);
    if (fileCount >= 1) {
        TraverseDirAndCompressExistFFICLogFile(filePath, compressdLogName, &(g_loggerThreadContext.errorLogOutputFile));
        DeleteRedundantCompressedFFICLogFile();
    }
}

UTILS_EXPORT ErrorCode StartLogCompression(void)
{
    if (g_logCompressionThreadState == LOG_COMPRESSION_THREAD_STARTED) {
        return ERROR_SYS_OK;
    } else if (g_logCompressionThreadState == LOG_COMPRESSION_THREAD_STARTING) {
        return ERROR_UTILS_ERRORLOG_THREAD_STARTING;
    } else {
        g_logCompressionThreadState = LOG_COMPRESSION_THREAD_STARTING;
    }

    InitLogCompressionContext();

    ThreadStartRoutine routineFunc = GetLogCompressionThreadFunc();
    LogCompressionFFICDoCompress();
    ErrorCode errorCode = ERROR_SYS_OK;
    errorCode = ThreadCreate(&(g_logCompressionThreadContext.tid), routineFunc, NULL);
    if (errorCode != ERROR_SYS_OK) {
        WriteErrMsg("Failed to create the log thread.\n");
        g_logCompressionThreadState = LOG_COMPRESSION_THREAD_NOT_STARTED;
        return errorCode;
    }

    g_logCompressionThreadState = LOG_COMPRESSION_THREAD_STARTED;
    return errorCode;
}

static void LogCompressionDoCompress(void)
{
    char compressdLogName[MAX_PATH] = {0};

    char filePath[MAX_PATH] = {0};
    (void)memset_s(filePath, sizeof(filePath), 0, sizeof(filePath));
    uint32_t fileCount = GetFileInfoFromLocalDirectory(g_loggerThreadContext.errorLogFileConfig.logDirectory,
                                                        filePath, MAX_PATH,
                                                        GetExtendNameByFileType(ERR_LOG_FILE_TYPE),
                                                        g_loggerThreadContext.errorLogFileConfig.logFileName);
    if (fileCount > 1) {
        /* Traverse the log directory and compress all existing log files with timestamp */
        TraverseDirAndCompressExistLogFile(filePath, compressdLogName, &(g_loggerThreadContext.errorLogOutputFile));

        /* Delete redundant compressed log file */
        DeleteRedundantCompressedLogFile(compressdLogName, &(g_loggerThreadContext.errorLogOutputFile));
    }
}

static void TraverseDirAndCompressExistAspLogFile(char *filePath, char *compressdLogName, LogOutputFile *logOutputFile)
{
    char sourceErrLogFileName[MAX_PATH] = {0};
    int ret = sprintf_s(sourceErrLogFileName, sizeof(sourceErrLogFileName), "%s-current.%s",
                        g_loggerThreadContext.errorLogFileConfig.aspFileName, ERR_LOG_EXTEND);
    if (unlikely(ret < 0)) {
        WriteErrMsg("Fail to generate source asp log file name.\n");
        return;
    }

    Directory dir;
    ErrorCode errCode = OpenDirectory(g_loggerThreadContext.errorLogFileConfig.aspLogDirectory, &dir);
    if (errCode != ERROR_SYS_OK) {
        WriteErrMsg("Fail to open asp log.\n");
        return;
    }
    DirectoryEntry dirEntry;
    while (ReadDirectory(&dir, &dirEntry)) {
        if (dirEntry.type != DIR_TYPE_REGULAR_FILE) {
            continue;
        }
        if (!strcmp(dirEntry.name, ".") || !strcmp(dirEntry.name, "..")) {
            continue;
        }
        char *baseFileName = Basename(dirEntry.name);

        uint64_t fileTimeStamp = 0;
        /* Check the validation of file name or file time stamp. */
        if (IsErrLogFileNameValid(sourceErrLogFileName, baseFileName, &fileTimeStamp,
            ERR_LOG_EXTEND, g_loggerThreadContext.errorLogFileConfig.aspFileName)) {
            /* Compress the existing error log file */
            if (strncmp(sourceErrLogFileName, baseFileName, strlen(baseFileName)) != 0) {
                GenerateErrorLogFilePath(filePath, MAX_PATH, g_loggerThreadContext.errorLogFileConfig.aspLogDirectory,
                                         baseFileName);

                (void)memset_s(compressdLogName, MAX_PATH, 0, MAX_PATH);
                errCode = CompressLogFile(filePath, logOutputFile, compressdLogName);
                if (errCode != ERROR_SYS_OK) {
                    ErrLog(ERROR, ErrMsg("Failed to compress asp log file, errcode is :%lld.\n", errCode));
                    continue;
                } else {
                    /* delete log file after successfully compressed log file */
                    ret = unlink(filePath);
                    if (ret == -1) {
                        PosixErrorCode2PortErrorCode(errno, &errCode);
                        continue;
                    } else {
                        logOutputFile->currentSequenceNo -= 1;
                    }
                }
            }
        }
    }

    CloseDirectory(&dir);
}

static void DeleteRedundantCompressedAspLogFile(const char* compressdLogName, LogOutputFile *logOutputFile)
{
    ErrorCode errCode = ERROR_SYS_OK;
    char filePath[MAX_PATH] = {0};
    uint64_t maxCompressedLogNum = 0;
    uint32_t compressedFileCount = GetFileInfoFromLocalDirectory(
        g_loggerThreadContext.errorLogFileConfig.aspLogDirectory,
        filePath, MAX_PATH, ZIP_LOG_EXTEND,
        g_loggerThreadContext.errorLogFileConfig.aspFileName);
    /* Check the actual compressed log file number */
    if (logOutputFile->currentCompressedSequenceNo != compressedFileCount) {
        logOutputFile->currentCompressedSequenceNo = compressedFileCount;
    }

    /* Compute the max compressed log file number */
    uint64_t compressedFileSize = 0;
    struct stat st;
    if (stat(compressdLogName, &st) == 0) {
        compressedFileSize = (uint64_t)st.st_size;
    } else {
        PosixErrorCode2PortErrorCode(errno, &errCode);
        return;
    }

#define BYTES_PER_KB 1024
    if (compressedFileSize != 0) {
            maxCompressedLogNum = ((uint64_t)g_loggerThreadContext.errorLogFileConfig.logDirectorySpaceSize *
                                   BYTES_PER_KB) / compressedFileSize;
    }

    if (maxCompressedLogNum != 0) {
        DeleteRedundantLogFile(&(logOutputFile->currentCompressedSequenceNo), maxCompressedLogNum, ZIP_LOG_EXTEND);
    }
    
    return;
}

static void LogCompressionAspDoCompress(void)
{
    char compressdLogName[MAX_PATH] = {0};
    char filePath[MAX_PATH] = {0};
    (void)memset_s(filePath, sizeof(filePath), 0, sizeof(filePath));
    uint32_t fileCount = GetFileInfoFromLocalDirectory(g_loggerThreadContext.errorLogFileConfig.aspLogDirectory,
                                                        filePath, MAX_PATH,
                                                        GetExtendNameByFileType(ERR_LOG_FILE_TYPE),
                                                        g_loggerThreadContext.errorLogFileConfig.aspFileName);
    if (fileCount > 1) {
        /* Traverse the log directory and compress all existing log files with timestamp */
        TraverseDirAndCompressExistAspLogFile(filePath, compressdLogName, &(g_loggerThreadContext.errorLogOutputFile));

        /* Delete redundant compressed log file */
        DeleteRedundantCompressedAspLogFile(compressdLogName, &(g_loggerThreadContext.errorLogOutputFile));
    }
}

void *LogCompressionMainThread(SYMBOL_UNUSED void *arg)
{
    (void)ThreadSetName("LogCompress");
    LoggerMaskSignal();
    OpenLogger();

    MutexLock(&g_logCompressionThreadContext.mutex);

    while (!g_logCompressionThreadContext.logCompressionExitFlag) {
        while (!g_logCompressionThreadContext.compressEnableFlag &&
               !g_logCompressionThreadContext.logCompressionExitFlag) {
#define WAIT_TIME 1000 /* milliseconds */
            (void)ConditionVariableTimedWait(&g_logCompressionThreadContext.cond, &g_logCompressionThreadContext.mutex,
                                             WAIT_TIME);
        }
        if (g_logCompressionThreadContext.logCompressionExitFlag) {
            break;
        }
        g_logCompressionThreadContext.compressEnableFlag = false;
        MutexUnlock(&g_logCompressionThreadContext.mutex);
        LogCompressionDoCompress();
	    LogCompressionAspDoCompress();
        MutexLock(&g_logCompressionThreadContext.mutex);
    }

    MutexUnlock(&g_logCompressionThreadContext.mutex);
    CloseLogger();
    return (void *)&g_logCompressionThreadContext;
}
