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
 * err_log_internal.h
 *
 * Description:
 * 1. Error log internal header file.
 *
 * ---------------------------------------------------------------------------------
 */
#ifndef UTILS_ERR_LOG_INTERNAL_H
#define UTILS_ERR_LOG_INTERNAL_H

#include "syslog/err_log.h"
#include "syslog/err_log_fold.h"

GSDB_BEGIN_C_CODE_DECLS

/**
 * The log file management needs to be considered as follows:
 * 1.In the case of writing remote log files, the file ID is allocated by the storage engine.
 * It is a resource that needs to be reused and cannot be frequently created or deleted.
 * 2.To know the current log file, you need to add a log metafile to record the information.
 * The log metafile stores the current log file name, maximum serial number, and current serial number.
 * Format: metaVersion fileName maxSequenceNo currentSequenceNo.
 * 3.Log files include real log files, log metafiles, and control files. (The control file stores the
 * relationship between the file name and the file ID.) Data consistency needs to be ensured in these three areas.
 * The information in the log metadata file is subject to the configuration information, and the information
 * in the control file is subject to the actual log file information. You need to verify the information with
 * the actual log file.
 * 4.Log file management must be universal and support the use of audit logs, slow logs, and SQL recording logs.
 * The entire interface is divided into two levels: general function and specific log type.
 * 5.The format of a log file name is as follows: Log file name + _ three-digit sequence number +. + File name suffix.
 * The suffix is used to distinguish different file types, such as log and auditlog.For example, if the file name
 * is GaussDB, the error log file name is GaussDB_001.log. The default log file name is the current process name.
 * The error log meta file name is GaussDB_meta.log.
 * 6.Both online and offline(modify the parameter file and restart the system.) of log parameters modification
 * must be considered.
 */

/* Error Log Message Type. */
#define ERROR_LOG_MESSAGE_TYPE   0
#define PROFILE_LOG_MESSAGE_TYPE 1
#define SQL_LOG_MESSAGE_TYPE     2
#define ASP_LOG_MESSAGE_TYPE     3
#define CSV_LOG_MESSAGE_TYPE     4
/* the suitable size need discuss */
/* The log context max size. */
#define ERRLOG_THREAD_MESSAGE_MAX_SIZE_UNLIMIT  ((size_t)-1)                 /* max */
#define ERRMSG_BEFORE_SENDTO_QUEUE_MAXSIZE      ((size_t)512 * 1024 * 1024)  /* 512M */
#define LOGGERTHREADCONTEXT_DATA_STRUCT_MAXSIZE ((size_t)32 * 1024 * 1024)   /* 32M */
#define LOG_FOLD_MAXSIZE                        ((size_t)1024 * 1024 * 1024) /* 1G */

/* Log destination type. */
#define LOG_DESTINATION_REMOTE_FILE (1U << 4) /* Output to remote file. */

/* the size of log buffer in a batch */
#define LOG_BATCH_BUF_SIZE 8192

/* the size of error buffer */
#define LOG_ERROR_BUF_SIZE 1024

/* the size of sequence info in log file */
#define LOG_SEQUENCE_INFO_SIZE 128

/* the size of sequence number string in sequence info */
#define LOG_SEQUENCE_NUM_STRING_SIZE 8

/* the prefix of sequence info */
#define LOG_SEQUENCE_NUM_PREFIX "CurrentFileSeqNum: "

/* the max length of formatted time string */
#define FORMATTED_TIME_MAX_LEN 128

/* Write message to stderr. */
void WriteStderr(const char *fmt, ...);

/* Open the error log.Initialize the error log thread local variables. */
ErrorCode OpenErrorLog(void);

/* Close the error log. Destroy the thread local variable memory context. */
void CloseErrorLog(void);

/*
 * Main log processing thread, invokes corresponding processing functions based on the received message type.
 */
void *LoggerMainThread(void *arg);

void *LogCompressionMainThread(void *arg);

/* Write error message to log file. */
void WriteLocalLogDataFile(const char *buffer, size_t count);

/* Get the logger thread error log memory context. */
MemoryContext GetLoggerThreadErrorLogContext(void);

/* Find the module bind text domain. */
const char *FindModuleTextDomain(const char *componentName, const char *moduleName);

/* Translate a text string into the user's native language. */
const char *DomainGetText(const char *domainName, const char *msgId);

/*
 * Set the log output syslog sequence numbers.
 * @param syslogSequenceNumbers : Error log syslog sequence numbers.
 * @return void
 */
void SetErrLogSyslogSequenceNumbers(bool syslogSequenceNumbers);
/*
 * Set the log output syslog split messages.
 * @param syslogSplitMessages : Error log syslog split messages.
 * @return void
 */
void SetErrLogSyslogSplitMessages(bool syslogSplitMessages);

#define DISABLE_ERROR_LOG_BACKTRACE 0
#define ENABLE_ERROR_LOG_BACKTRACE  1
/**
 * Set the error log print backtrace
 * @param backtraceFlag  :ENABLE_ERROR_LOG_BACKTRACE indicates allowed.
 * DISABLE_ERROR_LOG_BACKTRACE indicates forbidden.
 * @return void
 */
void SetErrLogBacktrace(int backtraceFlag);

/**
 * Set the ErrorData backtrace.
 * @return
 */
int ErrBacktrace(void);

/*
 * Set send message to client flag.
 */
void SetSendToClientFlag(int flag);

void ClearErrLogConnectionFilter(void);
/**
 * Set the error log connection filter.
 * @param srcIP    : Source IP address.
 * @param srcPort  : Source port.
 * @param destIP   : Destination IP address.
 * @param destPort : Destination port
 */
void SetErrLogConnectionFilter(char **srcIP, int *srcPort, char **destIP, int *destPort, int count);
/*
 * Clear error log output session information.
 */
void ClearErrLogSessionFilter(void);
/*
 * Set the error log session filter
 * @param applicationName    : Application name.
 * @param databaseName       : Database name.
 * @param userName           : User name.
 * @param sessionID          : Session ID.
 */
void SetErrLogSessionFilter(char **applicationName, char **databaseName, char **userName, int *sessionID, int count);

/*
 * Clear error log running context filter.
 */
void ClearErrLogRunningContextFilter(void);
/*
 * Set the error log running context filter
 * @param pid          : PID
 * @param tid          : TID
 * @param threadName   : thread name
 */
void SetErrLogRunningContextFilter(int *pid, uint32_t *tid, char **threadName, int count);

/*
 * Clear error log position context filter.
 */
void ClearErrLogPositionContextFilter(void);
/*
 * Set the error log position context filter.
 * @param fileName       : File name.
 * @param lineNo         : Line number.
 * @param funcName       : Function name.
 */
void SetErrLogPositionContextFilter(char **fileName, int *lineNo, char **functionName, int count);
/**
 * Get the error log file destination.
 * @return
 */
uint32_t GetErrorLogFileDestination(void);
/**
 * Set the error log destination.
 */
void SetErrorLogFileDestination(uint32_t errorLogDestination);
bool IsLogThreadErrSyslogWrite(void);
/*
 * Set the log output directory space size.
 * @param logDirectorySpaceSize : Error log directory space size.
 */
void SetErrLogDirectorySpaceSize(uint32_t logDirectorySpaceSize);
/*
 * Set the log output file name.
 * @param logFileName : Error log file name.
 */
void SetErrLogFileName(char *logFileName);
/*
 * Set the log output file mode.
 * @param logFileMode : Error log file mode.
 */
void SetErrLogFileMode(int logFileMode);
/*
 * Set the log file rotation size.
 * @param logRotationSize : Error log file rotation size.
 */
void SetErrLogFileRotationSize(uint32_t logRotationSize);

/*
 * Get the error log max sequence no.
 */
uint32_t GetErrLogFileMaxSequenceNo(void);

/*
 * Set the log file rotation age.
 * @param logRotationAge : Error log file rotation age.
 */
void SetErrLogFileRotationAge(uint32_t logRotationAge);

/**
 * Local File System Log File Verification.
 * If these log files are no longer used, delete the log files from the storage.
 */
void LocalLogFileVerification(void);

/*
 * Set error log output running context information.
 */
void SetErrLogRunningContextInfo(const char *threadName);

typedef struct MsgBatchData MsgBatchData;
struct MsgBatchData {
    size_t msgBatchLen;
    char msgBatch[LOG_BATCH_BUF_SIZE];
};

uint64_t GetErrorLogLocalFileCfgSeq(void);

uint32_t GetRemoteLogCurSeqNum(void);

int64_t GetRemoteLogCurFileSize(void);

void ResetThreadSyslogSeq(void);

bool PrepareRemoteLogResource(const ErrorLogRemoteFileConfigure *remoteLogCfg);

void ReleaseRemoteLogResource(void);

bool GetRemoteLogStoppingState(void);

bool isPipeEnlargeActive(void);

/* Get the logger thread tid. */
Tid GetLoggerThreadTid(void);

/* Get the log compression thread tid. */
Tid GetLogCompressionThreadTid(void);

void WriteSyslog(int syslogLevel, const char *line, size_t count);

void WriteErrMsg(const char *fmt, ...);

#ifdef ENABLE_UT
UTILS_EXPORT
#endif
uint32_t GetFileInfoFromLocalDirectory(const char *dirName, char *errorLogFilePath, Size len, char *fileSuffixStr,
                                       char *fileName);

bool GetErrorLogRedirectionDone(void);

int GetMessagePipe(void);

#ifdef ENABLE_FAULT_INJECTION

/* Fault injection related definitions */
enum FaultInjectionErrLogPoint {
    COPY_SYSLOG_OUTPUT_TO_STDERR, /* For test syslog output content */
    /* For mock remote file interface to local file */
    MOCK_REMOTE_LOG_OPEN,
    MOCK_REMOTE_LOG_CREATE,
    MOCK_REMOTE_LOG_FILE_IS_EXIST,
    MOCK_LOGGER_THREAD_ROUTINE,
    MOCK_LOG_ROTATION_TIME,
    MOCK_REMOTE_LOG_DATA_FILE,
    MOCK_UPDATE_REMOTE_LOG_SEQ_NUM_FAILED,
    MOCK_REMOTE_LOG_INVALID_SEQ_NUM,
    MOCK_REMOTE_LOG_VALID_SEQ_NUM,
    MOCK_GET_REMOTE_LOG_SEQ_NUM_SECCEED,
    MOCK_OPEN_LOG_FAILED,
    MOCK_ALLOCATE_MESSAGE_FAILED,
    MOCK_COPY_LOG_IDENTIFIER_FAILED,
    MOCK_DELETE_LOG_FILE_FAILED,
    MOCK_INIT_LOG_CFG_PARAM_FAILED,
    MOCK_INIT_VFS_FAILED,
    MOCK_WRITE_SYSLOG,
    MOCK_STRING_INIT_MALLOC_FAILED,
    MOCK_SET_LATCH_FAILED,
    MOCK_LOG_DISK_FULL,
    MOCK_OPENED_FILE_REACH_MAX,
    MOCK_CREATE_LOG_SAME_NAME_DIR,
};

#endif /* ENABLE_FAULT_INJECTION */

/* Error log module context. */
typedef struct ErrLogModuleContext ErrLogModuleContext;
struct ErrLogModuleContext {
    char *componentName; /* Component name. */
    char *moduleName;    /* Module name. */
    int componentId;     /* Component id. */
    int moduleId;        /* Module id. */
    int errorLevel;      /* Error level. */
};

#define DEFAULT_COMPONENT_ID       (-1)
#define DEFAULT_MODULE_ID          (-1)
#define MAX_COMPONENT_FILTER_COUNT 30
#define MAX_MODULE_FILTER_COUNT    50

bool IsLogLevelOutput(int elevel, int logMinLevel);
bool IsLogModuleContextOutput(ErrLogModuleContext *currentModule);

/**
 * Create error log directories.
 * @param directoryName
 * @param mode
 * @return
 */
ErrorCode MakeErrLogDirectories(const char *directoryName, unsigned int mode);

#define PIPE_HEADER_MAGICNUM 0x123456789ABCDEF0
typedef struct ThreadMessagePipeHeader ThreadMessagePipeHeader;
struct ThreadMessagePipeHeader {
    char nuls[2];     /* always \0\0 */
    uint16_t chunkLen;  /* size of this chunk (counts data only) */
    uint8_t msgType; /* The message type. */
    char isLast;      /* last chunk of message? 't' or 'f' */
    pthread_t tid;
    uint64_t magic; /* magic number to check the proto header */
    LogIdentifier msgContext;
    char data[]; /* The message data. */
};

/* POSIX says the value of PIPE_BUF must be at least 512 */
#define THREAD_MESSAGE_PIPE_CHUNK_SIZE 4096
typedef union ThreadMessagePipeChunk ThreadMessagePipeChunk;
union ThreadMessagePipeChunk {
    ThreadMessagePipeHeader pipeHeader;
    char filler[THREAD_MESSAGE_PIPE_CHUNK_SIZE];
};
#define THREAD_MESSAGE_PIPE_HEADER_LEN  offsetof(ThreadMessagePipeHeader, data)
#define THREAD_MESSAGE_PIPE_MAX_PAYLOAD (THREAD_MESSAGE_PIPE_CHUNK_SIZE - THREAD_MESSAGE_PIPE_HEADER_LEN)

GSDB_END_C_CODE_DECLS
#endif // UTILS_ERR_LOG_INTERNAL_H
