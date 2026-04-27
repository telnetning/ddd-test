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
 * err_log.h
 *
 * Description:
 * Defines the error output api for error log.
 *
 * ---------------------------------------------------------------------------------
 */
#ifndef UTILS_ERR_LOG_H
#define UTILS_ERR_LOG_H

#include <stdlib.h>
#include "securec.h"
#include "defines/common.h"
#include "defines/err_code.h"
#include "types/data_types.h"
#include "vfs/vfs_interface.h"

GSDB_BEGIN_C_CODE_DECLS

/*
 * __FUNCTION__ is not standard macro.  __FUNCTION__ is defined on both MSVC & GCC;
 * GCC support __func__,__FUNCTION__ and __PRETTY_FUNCTION__,
 * better use __PRETTY_FUNCTION__. C99 has standard __func__.
 * The identifier __func__ shall be implicitly declared by the translator as if,
 * immediately following the opening brace of each function definition, the declaration
 * static const char __func__[] = "function-name";appeared,where function-name is the name of the
 * lexically-enclosing function.Here defined FUNCNAME_MACRO macro according to the compiler used,
 * not use the C99 standard __func__.
 */
#if defined(__GNUC__)
#define FUNCNAME_MACRO __PRETTY_FUNCTION__
#elif defined(_MSC_VER)
#define FUNCNAME_MACRO __FUNCTION__
#else
#define FUNCNAME_MACRO "???"
#endif

/*
 * Each component should define the component name macro in its own component header file, and each module
 * should define the module name macro in its own module header file.
 * When, you want to redefine the macro, first of all, undefined the macro by using #undef preprocessor directive.
 * And, then define the macro again by using #define preprocessor directive.Refer the following format:
xxx_component.h
#undef  LOCAL_COMPONENT_NAME
#define LOCAL_COMPONENT_NAME  XXXXXX
#undef  LOCAL_COMPONENT_ID
#define LOCAL_COMPONENT_ID  XXXXXX
xxx_module.h
#undef  LOCAL_MODULE_NAME
#define LOCAL_MODULE_NAME   XXXXXX
#undef  LOCAL_MODULE_ID
#define LOCAL_MODULE_ID   XXXXXX
* The own component header and module header files must be at the end of all included header files in the C file.
* Ensure that the component name macro and module name macro use your own macros.
* If a module has only one file, as an alternative, you can define both macros in the C file.
*/
/* A few characters have an alternative representation, called a trigraph sequence.
 * A trigraph is a three-character sequence that represents a single character.
 * For example, the characters ???- represent the two characters ?~.
 * To avoid the trigraph interpretation, separate the string literal or escape the question marks.
 */
#define ERROR_LOG_UNKNOWN_NAME "?\?\?"
#define LOCAL_COMPONENT_NAME   ERROR_LOG_UNKNOWN_NAME
#define LOCAL_MODULE_NAME      ERROR_LOG_UNKNOWN_NAME
#define LOCAL_COMPONENT_ID     (-1)
#define LOCAL_MODULE_ID        (-1)

// clang-format off
/*
 * Error level codes,values in order of increasing detail.
 */
#define DEBUG      1   /* Debugging messages, used by developers to trace system running tracks,
                          such as function entry and exit or invoking other functions. */
#define LOG        2   /* Server operational messages; sent only to server log by default.
                          Used to output important data or status information when the system is running properly. */
#define INFO       3   /* Messages specifically requested by user,always sent to client regardless of clientLevel,
                          but by default not sent to server log. */
#define NOTICE     4   /* Helpful messages to users about query operation;sent to client and server log by default. */
#define WARNING    5   /* NOTICE is for expected messages like implicit sequence creation by SERIAL.
                          WARNING is for unexpected messages. */
#define ERROR      6   /* User error - abort transaction; return to known state. */
#define FATAL      7   /* Fatal error - current thread exit. */
#define PANIC      8   /* Panic error - abort whole process. */

/* Log line prefix and suffix bit. */
#define    LOG_LINE_PREFIX_CONNECTION_SRC_IP                      (1 << 30)   /* Output connection source ip. */
#define    LOG_LINE_PREFIX_CONNECTION_SRC_PORT                    (1 << 29)   /* Output connection source port. */
#define    LOG_LINE_PREFIX_CONNECTION_DEST_IP                     (1 << 28)   /* Output connection destination ip. */
#define    LOG_LINE_PREFIX_CONNECTION_DEST_PORT                   (1 << 27)   /* Output connection destination port. */
#define    LOG_LINE_PREFIX_SESSION_APPLICATION_NAME               (1 << 26)   /* Output session application name. */
#define    LOG_LINE_PREFIX_SESSION_DATABASE_NAME                  (1 << 25)   /* Output session database name. */
#define    LOG_LINE_PREFIX_SESSION_USER_NAME                      (1 << 24)   /* Output session user name. */
#define    LOG_LINE_PREFIX_SESSION_ID                             (1 << 23)   /* Output session identification. */
#define    LOG_LINE_PREFIX_RUNNING_CONTEXT_PID                    (1 << 22)   /* Output running context pid. */
#define    LOG_LINE_PREFIX_RUNNING_CONTEXT_TID                    (1 << 21)   /* Output running context tid. */
#define    LOG_LINE_PREFIX_RUNNING_CONTEXT_THREAD_NAME            (1 << 20)   /* Output running context thread name. */
/* Output compilation context component name. */
#define    LOG_LINE_PREFIX_COMPILATION_CONTEXT_COMPONENT_NAME     (1 << 19)
/* Output compilation context module name. */
#define    LOG_LINE_PREFIX_COMPILATION_CONTEXT_MODULE_NAME        (1 << 18)
/* Output compilation context file name. */
#define    LOG_LINE_PREFIX_COMPILATION_CONTEXT_FILE_NAME          (1 << 17)
/* Output compilation context line number. */
#define    LOG_LINE_PREFIX_COMPILATION_CONTEXT_LINENO             (1 << 16)
/* Output compilation context function name. */
#define    LOG_LINE_PREFIX_COMPILATION_CONTEXT_FUNCTION_NAME      (1 << 15)
/* Output timestamp.The time precision is second. */
#define    LOG_LINE_PREFIX_TIMESTAMP                              (1 << 14)
/* Output timestamp.The time precision is milliseconds. */
#define    LOG_LINE_PREFIX_HIGH_PRECISION_TIMESTAMP               (1 << 13)
#define    LOG_LINE_PREFIX_EPOCH_TIMESTAMP                        (1 << 12)   /* Output timestamp as a unix epoch. */
#define    LOG_LINE_PREFIX_SEVERITY                               (1 << 11)   /* Output severity. */
#define    LOG_LINE_PREFIX_TRANSACTION_ID                         (1 << 10)   /* Output transaction ID. */
#define    LOG_LINE_PREFIX_QUERY_STRING                           (1 << 9)    /* Output query string. */
#define    LOG_LINE_SUFFIX_CONTEXT                                (1 << 8)    /* Output context. */
#define    LOG_LINE_SUFFIX_BACKTRACE                              (1 << 7)    /* Output backtrace. */
// clang-format on

#define ERROR_LOG_DEFAULT_PREFIX                                                                  \
    (LOG_LINE_PREFIX_TIMESTAMP | LOG_LINE_PREFIX_SEVERITY | LOG_LINE_PREFIX_RUNNING_CONTEXT_TID | \
     LOG_LINE_PREFIX_COMPILATION_CONTEXT_FILE_NAME | LOG_LINE_PREFIX_COMPILATION_CONTEXT_LINENO)

/* Log destination type. */
#define LOG_DESTINATION_LOCAL_STDERR (1 << 1) /* Output to local stderr. */
#define LOG_DESTINATION_LOCAL_FILE   (1 << 2) /* Output to local file. */
#define LOG_DESTINATION_LOCAL_SYSLOG (1 << 3) /* Output to local syslog. */

extern THR_LOCAL int g_errorLogOpenState;

#define ERROR_LOG_CLOSED 0
#define ERROR_LOG_OPENED 1

/*
 * Whether the thread log have opened.
 */
#define IS_ERR_LOG_OPEN() (g_errorLogOpenState == ERROR_LOG_OPENED)
#define GET_ERR_LOG_SERVER_LEVEL (g_errorLogConfigure.serverLevel)
#define GET_ERR_LOG_CLIENT_LEVEL (g_errorLogConfigure.clientLevel)

/* Whether to allow error logs to be sent to the client. */
#define DISABLE_ERROR_LOG_SEND_CLIENT_ALL  0
#define ENABLE_ERROR_LOG_SEND_CLIENT_ALL   1
#define ENABLE_ERROR_LOG_SEND_CLIENT_ERROR 2

#define SHOULD_OUTPUT_TO_CLIENT(elevel, flag) \
    ((flag) == DISABLE_ERROR_LOG_SEND_CLIENT_ALL ? false : \
     (flag) == ENABLE_ERROR_LOG_SEND_CLIENT_ALL ? ((elevel) >= GET_ERR_LOG_SERVER_LEVEL || (elevel) == INFO) : \
     (flag) == ENABLE_ERROR_LOG_SEND_CLIENT_ERROR ? ((elevel) >= ERROR) : false)

bool ShouldOutputToClient(int elevel);
int GetErrLogServerLevel(void);
/* Open the error log.Initialize the error log thread local variables. */
ErrorCode OpenErrorLog(void);
#define IS_ERR_LEVEL_PASSED(errorLevel) ((IS_LOG_LEVEL_OUTPUT(errorLevel, GET_ERR_LOG_SERVER_LEVEL) || \
                                          SHOULD_OUTPUT_TO_CLIENT(errorLevel, GET_ERR_LOG_CLIENT_LEVEL)))
/*
 * Error log API: to be used in this way:
 *		ErrLog(ERROR,
 *				ErrCode(ERRCODE_UNDEFINED_CURSOR),
 *				ErrMsg("portal \"%s\" not found", stmt->portalName),
 *				... other ErrXxx() fields as needed ...);
 *
 * The error level is required, and so is a primary error message (ErrMsg).All else is optional.
 * ErrCode() defaults to ERRCODE_INTERNAL_ERROR if elevel is ERROR or more,
 * ERRCODE_WARNING if elevel is WARNING, or ERRCODE_SUCCESSFUL_COMPLETION if elevel is NOTICE or below.
 * Extra parentheses were not required around the list of auxiliary function calls; that's optional.
 */

#define ErrLog(elevel, ...)                                                                         \
    do {                                                                                            \
        if (!IS_ERR_LOG_OPEN())                                                                   \
            (void)OpenErrorLog();                                                                   \
                                                                                                    \
        if (IS_ERR_LEVEL_PASSED(elevel))                                                            \
            if (ErrStart(elevel, ErrStartPosition(__FILE__, __LINE__, FUNCNAME_MACRO),              \
                        ErrStartModule(LOCAL_COMPONENT_NAME, LOCAL_MODULE_NAME, LOCAL_COMPONENT_ID, \
                                        LOCAL_MODULE_ID, elevel)))                                  \
                __VA_ARGS__, ErrFinish();                                                           \
    } while (0)

void InitGetLogLinePrefixFunc(const char *(*getLogLinePrefix)(void));
void InitGetApplicationNameFunc(const char *(*getApplicationName)(void));
void InitGetMyStartTimeFunc(long (*getMyStartTime)(void));
void InitGetDispFunc(const char *(*getDisp)(void));
void InitGetUserNameFunc(const char *(*getUserName)(void));
void InitGetDatabaseNameFunc(const char *(*getDatabaseName)(void));
void InitGetRemoteHostFunc(const char *(*getRemoteHost)(void));
void InitGetRemotePortFunc(const char *(*getRemotePort)(void));
void InitGetThreadIdFunc(int (*getThreadId)(void));
void InitGetSessionIdFunc(int (*getSessionId)(void));
void InitGetGlobalSessionIdNodeIdFunc(int (*getGlobalSessionIdNodeId)(void));
void InitGetGlobalSessionIdSessionIdFunc(int (*getGlobalSessionIdSessionId)(void));
void InitGetGlobalSessionIdSeqFunc(int (*getGlobalSessionIdSeq)(void));
void InitGetDebugQueryIdFunc(uint64 (*getDebugQueryId)(void));
void InitUpdateLogLineNumberFunc(void (*updateLogLineNumber)(void));
void InitGetLogLineNumberFunc(long (*getLogLineNumber)(void));
void InitGetLogicTidFunc(int (*getLogicTid)(void));
void InitGetFormattedStartTimeFunc(char *(*getFormattedStartTime)(void));
void InitGetBackendIdFunc(int (*getBackendId)(void));
void InitGetLxidFunc(uint64 (*getLxid)(void));
void InitGetCurrentTopXidFunc(uint64 (*getCurrentTopXid)(void));
void InitGetSqlStateFunc(const char *(*getSqlState)(int));
void InitGetPGXCNodeNameFunc(char *(*getPGXCNodeName)(void));
void InitGetTraceIdFunc(char *(*getTraceId)(void));
void InitIsProcPortEmptyFunc(bool (*isProcPortEmpty)(void));
void InitIsLogVerboseFunc(bool (*isLogVerbose)(void));

/**
 * Check whether error log level should be output
 * @param errorLevel
 * @return
 */
bool IsErrLevelPassed(int errorLevel);

/**
 * Pre-process error log information.
 * @param errorLevel: Log level.
 * @param positionResult : Log position output result.
 * @param moduleResult : Log module result.
 * @return
 */
bool ErrStart(int errorLevel, bool positionResult, bool moduleResult);

/**
 * Pre-process error log information.
 * @param componentName: Component name.
 * @param moduleName: Module name.
 * @param componentId: Component id.
 * @param moduleId: Module id.
 * @param errorLevel: error level.
 * @return
 */
bool ErrStartModule(const char *componentName, const char *moduleName, int componentId, int moduleId, int errorLevel);

/**
 * Pre-process error log information.
 * @param fileName : Source file name of log position.
 * @param lineNo : Line number of log position.
 * @param funcName : Function name of log position.
 * @return
 */
bool ErrStartPosition(const char *fileName, int lineNo, const char *funcName);

/**
 * Process error log to output.
 */
void ErrFinish(void);

/* Below auxiliary functions is used for fill 'ErrorData'. */
void ErrCode(int sqlErrorCode);
void ErrComponentId(int componentId);
void ErrModuleId(int moduleId);
void ErrMsgType(uint8_t messageType);
void ErrMsg(const char *fmt, ...) GsAttributePrintf(1, 2);
void ErrCause(const char *fmt, ...) GsAttributePrintf(1, 2);
void ErrAction(const char *fmt, ...) GsAttributePrintf(1, 2);

/*
 * Error log common configure
 */
typedef struct ErrorLogConfigure ErrorLogConfigure;
struct ErrorLogConfigure {
    uint32_t errLogDestination;
    uint32_t errLogLinePrefixSuffix;
    int clientLevel;        /* Currently does not support send error log to client. */
    int serverLevel;        /* Error log >= serverLevel will output to server */
    int foldLevel;          /* Error log >= foldLevel will not be folded */
    uint32_t foldThreshold; /* Error log > foldThreshold will be folded in 1 foldPeriod, 0 means fold log disabled */
    uint32_t foldPeriod;    /* Unit: second, 0 means fold log disabled */
    uint64_t maxErrLogFlow; /* Max error log per second output to server, 0 means flow control disabled */
    bool isBatchWrite;      /* If the value is true, which means write logs in batch way, otherwise in single way */
    uint64_t flowControlThreshold; /* The speed of received log > flowControlThreshold will be controlled */
    int filterLevel; /* Error log level <= filterLevel will not output to server when in the flow control process */
    uint64_t logStatPeriod; /* Every logStatPeriod time, checking the speed of error logs once time */
    bool isNeedRedirect;
    struct pg_tz* logLocalTime;
    bool enableCompress;
};
extern ErrorLogConfigure g_errorLogConfigure;

void ResetErrorLogConfigure(void);

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
void SetErrLogLinePrefixSuffix(uint32_t errLogLinePrefixSuffix);
/*
 * Get the error log line prefix and suffix.
 */
uint32_t GetErrLogLinePrefixSuffix(void);

void ResetErrLogLinePrefixSuffix(void);

/**
 * Set error log output destination, could be combination of [stderr, local file, syslog]
 * @param[in] logDestination error log output destination
 */
void SetErrLogDestination(uint32_t logDestination);

uint32_t GetErrLogDestination(void);

/**
 * Set minimal server log output level
 * @param[in] serverLevel Server error log level
 */
void SetErrLogServerLevel(int serverLevel);

void SetErrLogClientLevel(int clientLevel);

int GetErrLogClientLevel(void);

bool SetErrLogFoldConfig(uint32_t foldPeriod, uint32_t foldThreshold, int foldLevel);

void ResetErrLogFoldConfig(void);

void SetErrLogWriteMode(bool isBatchWrite);

bool IsErrLogBatchWrite(void);

bool SetErrLogFlowConfig(uint64_t flowControlThreshold, int filterLevel, uint64_t logStatPeriod);

void ResetErrLogFlowConfig(void);

void SetErrLogRedirect(void);

bool IsErrLogNeedRedirect(void);

void SetErrorLogCompress(bool enableCompress);

void SetLogPipeEnlarge(bool isPipeEnlargeActive);

bool GetErrorLogCompress(void);

void SetErrLogLocalTime(struct pg_tz* logLocalTime);

struct pg_tz* GetErrLogLocalTime(void);

/* Default directory of error log */
#define ERROR_LOG_DEFAULT_DIRECTORY      "./error_log"
#define PROFILE_LOG_DEFAULT_DIRECTORY    ""
#define ASP_LOG_DEFAULT_DIRECTORY        "./asp_data"
#define FFIC_LOG_DEFAULT_DIRECTORY        "./ffic_log"

/* Different kind of error log suffix */
#define ERR_LOG_EXTEND     "log"
#define AUDIT_LOG_EXTEND   "auditlog"
#define PROFILE_LOG_EXTEND "prf"
#define CSV_LOG_EXTEND     "csv"
#define ZIP_LOG_EXTEND     "log.gz"

/**
 * Set the log output directory, call before StartLogger()
 * if not called, error log will use ERROR_LOG_DEFAULT_DIRECTORY as log directory
 * @param[in] logDirectory
 */
void SetErrLogDirectory(const char *logDirectory);

/**
 * Set the Plog output directory, call before StartLogger()
 * if not called, Plog directory will not be created
 * @param[in] logDirectory
 */
void SetPLogDirectory(const char *logDirectory);

/**
 * Set the slow query log output directory, call before StartLogger()
 * if not called, slow query log directory will not be created
 * @param[in] logDirectory
 */
void SetSlowQueryLogDirectory(const char *logDirectory);

/**
 * Set the asp log output directory, call before StartLogger()
 * if not called, asp log directory will not be created
 * @param[in] logDirectory
 */
void SetAspLogDirectory(const char *logDirectory);

/**
 * Set the ffic log output directory, call before StartLogger()
 * if not called, ffic log directory will not be created
 * @param[in] logDirectory
 */
void SetFFICLogDirectory(const char *logDirectory);

/*
 * Get the error log output directory.
 */
void GetErrLogDirectory(char *logDirectory, Size len);

/*
 * Get the profile log output directory.
 */
void GetPLogDirectory(char *logDirectory, Size len);

/*
 * Get the slow query log output directory.
 */
void GetSlowQueryLogDirectory(char *logDirectory, Size len);

/*
 * Get the asp log output directory.
 */
void GetAspLogDirectory(char *logDirectory, Size len);

void ResetErrLogDirectory(void);

void ResetPLogDirectory(void);

void ResetAspLogDirectory(void);

/**
 * Specify error log space parameters
 * @param[in] totalErrLogSpaceSize Total error log space in KB
 * @param[in] spacePerErrLogFile error log file size in KB
 */
void SetErrLogSpaceSize(uint32_t totalErrLogSpaceSize, uint32_t spacePerErrLogFile);

void ResetErrLogSpaceSize(void);

/**
 * Set error log local file mode
 * There are several modes could be choosen, and all of them are combinable.
 * If do not set file mode, FILE_READ_AND_WRITE_MODE in default.
 * @param[in] mode file permission
 *                 FILE_READ_AND_WRITE_MODE: Read and write by owner
 *                 FILE_GROUP_READ_MODE: Read by group
 *                 FILE_GROUP_WRITE_MODE: Write by group
 *                 FILE_OTHER_READ_MODE: Read by others
 *                 FILE_OTHER_WRITE_MODE: Write by others
 */
void SetErrLogLocalFileMode(int mode);

#define REMOTE_LOG_FILE_MAX_COUNT 64

typedef struct ErrorLogRemoteFileConfigure ErrorLogRemoteFileConfigure;
struct ErrorLogRemoteFileConfigure {
    VirtualFileSystem *remoteVfs; /* Remote vfs handle */
    uint32_t totalSpace;          /* Total error log space in KB */
    char storeSpaceName[STORESPACE_NAME_MAX_LEN];
    uint32_t errorLogFileCount; /* Append file count of log data */
    FileId errorLogFileIds[REMOTE_LOG_FILE_MAX_COUNT];
};

/**
 * Enable write error log to remote file, must call after StartLogger()
 * @param[in] cfg remote log related configure
 * @return
 */
bool EnableRemoteErrorLog(const ErrorLogRemoteFileConfigure *cfg);

/**
 * Get current running remote error log configure
 * @param[out] cfg remote configure information
 * @return true if get remote error log configure success, or false if something wrong
 */
bool GetRemoteErrorLogConfigure(ErrorLogRemoteFileConfigure *cfg);

/**
 * Disable remote error log, automatically call in StopLogger()
 */
void DisableRemoteErrorLog(void);

/* Is log have opened. */
bool IsLoggerStarted(void);

/*
 * Start log Server. all parameters except previously set parameters are set to default values.
 * if you need change those config params, you call the series of SetXXX().
 * @return ErrorCode
 */
ErrorCode StartLogger(void);
/*
 * Start logger only for gaussdb process. all parameters except previously set parameters are set to default values.
 * if you need change those config params, you call the previous series of SetXXX().
 * @return ErrorCode
 */
ErrorCode StartLoggerNoSignal(void);
/*
 * Stop log server.
 */
void StopLogger(void);

/*
 * Open log,initialize log client.
 * Each thread does not have to invoke the OpenLogger. The OpenLogger is automatically initialized during error log.
 * However, if the thread proactively exits, the CloseLogger must be invoked to release thread-related resources.
 * To ensure interface symmetry, the OpenLogger interfaces are provided for external systems. If a thread needs
 * to register additional callback functions during error log, OpenLogger must be called first.
 * @return ErrorCode
 */
ErrorCode OpenLogger(void);

/*
 * Close log,destroy log client.
 */
void CloseLogger(void);

void FlushLogger(void);
ErrorCode StartLogCompression(void);

// clang-format off
/* The error log path is empty. */
#define ERROR_UTILS_ERRORLOG_DIRECTORY_IS_NULL \
    MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_ERRORLOG_MODULE_ID, 0x00000001)

/* The value out of the range. */
#define ERROR_UTILS_ERRORLOG_OUT_OF_RANGE \
    MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_ERRORLOG_MODULE_ID, 0x00000002)

/* The context initialization failed. */
#define ERROR_UTILS_ERRORLOG_CONTEXT_INITL_FAIL \
    MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_ERRORLOG_MODULE_ID, 0x00000003)

/* The logger thread not stated. */
#define ERROR_UTILS_ERRORLOG_THREAD_NOT_STARTED \
    MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_ERRORLOG_MODULE_ID, 0x00000004)
/* The logger thread starting. */
#define ERROR_UTILS_ERRORLOG_THREAD_STARTING \
    MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_ERRORLOG_MODULE_ID, 0x00000005)

/* The logger thread open log file failed. */
#define ERROR_UTILS_ERRORLOG_FILE_OPEN \
    MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_ERRORLOG_MODULE_ID, 0x00000006)

/* The error log directory is not exist. */
#define ERROR_UTILS_ERRORLOG_DIRECTORY_IS_NOT_EXIT \
    MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_ERRORLOG_MODULE_ID, 0x00000007)

/* The length of error log directory is out of range. */
#define ERROR_UTILS_ERRORLOG_DIRECTORY_IS_OUT_OF_RANGE \
    MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_ERRORLOG_MODULE_ID, 0x00000008)
// clang-format on

/* Set the remote file system for log. */
void SetLogRemoteVfs(VirtualFileSystem *remoteVfs,
                     ErrorCode (*getLogFileIdFun)(char *fileName, const char *storeSpaceName),
                     char **(*getLogFileFun)(int *count), void (*freeLogFileFun)(char **fileName),
                     void (*removeLogFileIdFun)(char *fileName));

/* The following auxiliary function is used to set the log output prefix. */
/*
 * Set error log output connection information.
 */
void SetErrLogConnectionInfo(char *srcIP, int srcPort, char *destIP, int destPort);
/*
 * Clear error log output connection information.
 */
void ClearErrLogConnectionInfo(void);

/*
 * Set error log output session information.
 */
void SetErrLogSessionInfo(char *applicationName, char *databaseName, char *userName, int sessionID);
/*
 * Clear error log output session information.
 */
void ClearErrLogSessionInfo(void);

/*
 * Clear error log output running context information.
 */
void ClearErrLogRunningContextInfo(void);

/*
 * Set error log output transaction ID information.
 */
void SetErrLogTransactionIDInfo(int transactionID);
/*
 * Clear error log output transaction ID information.
 */
void ClearErrLogTransactionIDInfo(void);

/*
 * Set error log output query string information.
 */
void SetErrLogQueryStringInfo(const char *queryString);
/*
 * Clear error log output query string information.
 */
void ClearErrLogQueryStringInfo(void);

/*
 * Defines the wildcard for setting log comparison conditions.
 * "*" is the string type wildcard.
 * -1 is the int type wildcard.
 */
#define LOG_STRING_WILDCARD ("*")
#define LOG_INT_WILDCARD    (0x0FFFFFFF)

/*
 * Bind module text domain. if not set module text domain,the module use the process text domain.
 * This function must be invoked after StartLogger succeeds.
 * @param compName   : Component name.
 * @param modName    : Module name.
 * @param domainName : Text domain name.
 * @return ErrorCode.
 */
ErrorCode BindModuleTextDomain(const char *componentName, const char *moduleName, const char *domainName,
                               const char *dirName);

/**
 * Push error log callback function.
 * @param contextCallback : ErrorLogContextCallback.
 */
void PushErrorLogContextCallback(void (*callback)(void *), void *arg);
/**
 * Pop error log callback function.
 */
void PopErrorLogContextCallback(void);
/*
 * ErrContext add a context error message text to the current error. Unlike other cases, multiple calls
 * are allowed to build up a stack of context information.  We assume earlier calls represent more-closely-nested
 * states.This function can only be called in the callback function of PushErrorLogContextCallback.
 */
int ErrContext(const char *fmt, ...);

/*
 * Register fatal error thread exit callback.
 * @param threadExitCallback: Thread exit callback.
 * @param valuePtr: The thread exit value.
 */
void RegisterErrLogThreadExitCallBack(int (*threadExitCallback)(void *valuePtr), void *valuePtr);

/*
 * Convert the elevel to the corresponding character string.
 * The ErrLogSendToFrontendCallBackFun invokes this function to send the error level string to the client.
 */
const char *ErrorLevel2String(int elevel);

/* Register send the error log message to client callback. */
void RegisterErrLogSendToFrontendCallBack(void (*sendToFrontendCallBack)(int level, int sqlErrorCode, char *message,
                                                                         char *cause, char *action));

/*
 * Register process interrupts callback.
 * @param processInterruptsCallBack: Process interrupts callback.
 */
void RegisterErrLogProcessInterruptsCallBack(void (*processInterruptsCallBack)(void));

/*
 * Below macro is encapsulates the checking of return value of secure function,
 * simplify processing of invoking functions.
 */
const char *GetSecurecErrorInfo(errno_t errCode);
bool CheckSecurecRetCode(errno_t errCode, bool printfFamily);

/*
 * Reset error log module context filter.
 */
void ResetErrLogModuleContextFilter(void);

/*
 * Set the error log module context filter.
 * WARNING: The moduleId and errorLevel are the list of module id and error level,
 * there is a one-to-one correlation between them. In addition, the number of
 * moduleId and errorLevel need to be equal to the value of count. Calling this interface
 * would reset existing filter condition.
 * @param componentId  : Component id.
 * @param moduleId     : Module id.
 * @param errorLevel   : Error level.
 * @param count        : the number of module id and error level pair.
 */
void SetErrLogModuleContextFilter(int componentId, const int *moduleId, const int *errorLevel, int count);

pthread_t GetLoggerThreadId(void);

/*
 * bPrintfFamily is used to represent a printf type function.
 * Its return value is -1, which is different from EOK.
 * var args is used to free resource, such as buffer.
 * for example:
 * ret = snprintf_s(buff, buffSize, buffSize - 1, "%s/%s", "aaaaa", "bbbb");
 * SecurecCheckReturn(ret, true, ERROR_XXXX, MemFree(buff));
 */
#define SecurecCheckReturn(syserr, bPrintfFamily, retCode, ...)         \
    do {                                                                \
        errno_t tmpErrNo = (errno_t)syserr; /* for pclint */            \
        if (CheckSecurecRetCode(tmpErrNo, bPrintfFamily)) {             \
            ErrLog(ERROR, ErrMsg("%s", GetSecurecErrorInfo(tmpErrNo))); \
            __VA_ARGS__;                                                \
            return retCode;                                             \
        }                                                               \
    } while (0)

#define SecurecCheck(syserr, bPrintfFamily, ...)                        \
    do {                                                                \
        errno_t tmpErrNo = (errno_t)syserr; /* for pclint */            \
        if (CheckSecurecRetCode(tmpErrNo, bPrintfFamily)) {             \
            ErrLog(ERROR, ErrMsg("%s", GetSecurecErrorInfo(tmpErrNo))); \
            __VA_ARGS__;                                                \
        }                                                               \
    } while (0)

typedef struct LogStatTime LogStatTime; /* use for recording error log statistics time */
struct LogStatTime {
    uint64_t curStatTime; /* current log statistics time */
    uint64_t preStatTime; /* previous log statistics time */
};

bool IsFilterStarted(void);

#define IS_LOG_LEVEL_OUTPUT(elevel, logMinLevel) \
((IsFilterStarted()) || (((elevel) == LOG) && ((logMinLevel) <= ERROR)) || ((elevel) >= (logMinLevel)))

GSDB_END_C_CODE_DECLS
#endif /* UTILS_ERR_LOG_H */
