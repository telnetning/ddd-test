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
 * ut_err_log.cpp
 *
 * Description:
 * 1. test error log
 *
 * ---------------------------------------------------------------------------------
 */
#include <thread>
#include <gtest/gtest.h>
#include <mockcpp/mokc.h>
#include <iostream>
#include <fstream>
#include <string>
#include "securec.h"
#include "syslog/err_log.h"
#include "syslog/err_log_internal.h"
#include "fault_injection/fault_injection.h"
#include "syslog/err_log_fold.h"
#include "syslog/trace.h"
#include "container/string_info.h"
#include "ut_err_log_common.h"

using namespace std;
/** ************************************************************************************************************* **/
VirtualFileSystem *g_localVfs;
#define TEST_CURRENT_DIRECTORY "."
#define TEST_STDERR_REDIRECT_FILE  "test_stderr.data"
#define TEST_PROFILE_LOG_DEFAULT_DIRECTORY    "./gs_profile"

#define ERROR_LOG_WAIT_GAP_US 1000 /* 1 millisecond */
#define ERROR_LOG_MAX_WAIT_US (60 * 1000 * 1000) /* 1 minute */

bool WaitErrLogLocalFileCfgTerm(uint64_t targetTerm)
{
    for (int i = 0; i < ERROR_LOG_MAX_WAIT_US / ERROR_LOG_WAIT_GAP_US; i++) {
        uint64_t currentTerm = GetErrorLogLocalFileCfgSeq();
        if (currentTerm == targetTerm) {
            return true;
        }
        Usleep(ERROR_LOG_WAIT_GAP_US);
    }
    return false;
}

bool CheckDirectoryExist(const char *directory)
{
    Directory testDir;
    ErrorCode errorCode = OpenDirectory(directory, &testDir);
    if (errorCode != ERROR_SYS_OK) {
        return false;
    }
    CloseDirectory(&testDir);
    return true;
}

char stdErrPath[MAX_PATH];

void StartStdErrRedirect(const char *fileName)
{
    (void) memset_s(stdErrPath, MAX_PATH, 0, MAX_PATH);
    int errFd = fileno(stderr);
    char buf[128] = {0};
    (void) snprintf(buf, sizeof(buf), "/proc/self/fd/%d", errFd);
    (void) readlink(buf, stdErrPath, sizeof(stdErrPath));
    freopen(fileName, "w", stderr);
}

void StopStdErrRedirect()
{
    fclose(stderr);
    freopen(stdErrPath, "w", stderr);
}

#define MAX_ERROR_LOG_FILE_NAME_COUNT     4096

/*
 * Remove all error log files in the specified directory.
 */
void RemoveErrorLogFileFromLocalDirectory(VirtualFileSystem *vfs, const char *dirName)
{
    ErrorCode errorCode;
    Directory dir;
    errorCode = OpenDirectory(dirName, &dir);
    if (errorCode != ERROR_SYS_OK) {
        fprintf(stderr, "RemoveErrorLogFileFromLocalDirectory =%s\n", dirName);
        return;
    }
    bool result = false;
    DirectoryEntry dirEntry;
    char *firstErrLogFileName = NULL;
    char *firstPLogFileName = NULL;
    char *firstCsvLogFileName = NULL;
    char *firstCompressedLogFileName = NULL;
    int count = 0;
    char *fileName[MAX_ERROR_LOG_FILE_NAME_COUNT] = {NULL};
    while ((result = ReadDirectory(&dir, &dirEntry))) {
        if (dirEntry.type != DIR_TYPE_REGULAR_FILE) {
            continue;
        }
        if (!strcmp(dirEntry.name, ".") || !strcmp(dirEntry.name, "..")) {
            continue;
        }
        firstErrLogFileName = strstr(dirEntry.name, ERR_LOG_EXTEND);
        firstPLogFileName = strstr(dirEntry.name, PROFILE_LOG_EXTEND);
        firstCsvLogFileName = strstr(dirEntry.name, CSV_LOG_EXTEND);
        firstCompressedLogFileName =  strstr(dirEntry.name, ZIP_LOG_EXTEND);
        if (firstErrLogFileName != NULL || firstPLogFileName != NULL ||
            firstCsvLogFileName != NULL ||  firstCompressedLogFileName != NULL) {
            fileName[count] = (char *) malloc(MAX_PATH);
            errno_t rc;
            rc = memset_s(fileName[count], MAX_PATH, 0, MAX_PATH);
            SecurecCheck(rc, true);
            rc = sprintf_s(fileName[count], MAX_PATH, "%s/%s", dirName, dirEntry.name);
            SecurecCheck(rc, true);
            count++;
            if (count >= MAX_ERROR_LOG_FILE_NAME_COUNT) {
                break;
            }
        }
    }
    CloseDirectory(&dir);

    int i;
    for (i = 0; i < count; i++) {
        errorCode = FileIsExist(vfs, fileName[i], &result);
        if (errorCode != ERROR_SYS_OK) {
            fprintf(stderr, "FileIsExist fileName =%s failed!\n", fileName[i]);
        }
        if (!result) {
            fprintf(stderr, "FileIsExist not exit fileName =%s failed!\n", fileName[i]);
        }

        errorCode = Remove(vfs, fileName[i]);
        if (errorCode != ERROR_SYS_OK) {
            fprintf(stderr, "Remove fileName =%s failed!\n", fileName[i]);
        }
        free(fileName[i]);
    }
    return;
}

#define ERROR_LOG_CONTENT_LEN    (32 * 1024)

/*
 * Reads the contents of the specified error log file.
 */
ErrorCode ReadErrorLogFileContent(VirtualFileSystem *vfs, const char *filePath, char *buffer, size_t len, bool isRemoteErrLog)
{
#define MAX_LOG_FILE_SIZE    (16 * 1024 * 1024)
    int fileId = 0;
    ErrorCode errorCode;
    int flags = FILE_READ_AND_WRITE_FLAG;
    FileDescriptor *errorLogFile = NULL;

    errorCode = Open(vfs, filePath, flags, &errorLogFile);
    if (errorCode != ERROR_SYS_OK) {
        return errorCode;
    }
    errno_t rc;
    rc = memset_s(buffer, len, 0, len);
    SecurecCheck(rc, true);
    int64_t readSize = 0;
    if (isRemoteErrLog) {
        errorCode = Pread(errorLogFile, buffer, len, LOG_SEQUENCE_INFO_SIZE, &readSize);
    } else {
        errorCode = Pread(errorLogFile, buffer, len, 0, &readSize);
    }

    if (errorCode != ERROR_SYS_OK) {
        return errorCode;
    }
    errorCode = Close(errorLogFile);
    if (errorCode != ERROR_SYS_OK) {
        return errorCode;
    }
    return ERROR_SYS_OK;
}

/*
 * Check whether the specified character string is contained.
 */
bool IsSubstringContained(char *str, const char *substr)
{
    char *pos = strstr(str, substr);
    if (pos != NULL) {
        return true;
    } else {
        return false;
    }
}

char g_errorLogFileFullPath[MAX_PATH] = {0};
char g_pLogFileFullPath[MAX_PATH] = {0};
char g_aspLogFileFullPath[MAX_PATH] = {0};
char g_csvLogFileFullPath[MAX_PATH] = {0};

#define UT_NAME "utils_unittest"
bool GetErrorLogFileInDirectory(const char *directory, char *fileFullPath, size_t len)
{
    char filePath[MAX_PATH] = {0};
    int count = GetFileInfoFromLocalDirectory(directory, filePath, MAX_PATH, ERR_LOG_EXTEND, UT_NAME);
    if (count <= 0) {
        return false;
    }
    if (sprintf_s(fileFullPath, len, "%s", filePath) < 0) {
        return false;
    }
    return true;
}

/*
 * Get the full path of the error log file name.
 */
ErrorCode GetErrorLogFileFullPath(char *fileFullPath, Size len, void (*getLogDirectory)(char *logDirectory, Size len), char *fileSuffixStr, char *fileName)
{
    char dir[MAX_PATH] = {0};
    if (getLogDirectory == NULL) {
        return ERROR_UTILS_ERRORLOG_DIRECTORY_IS_NOT_EXIT;
    }
    getLogDirectory(dir, MAX_PATH);
    int count = 0;
    char filePath[MAX_PATH] = {0};
    count = GetFileInfoFromLocalDirectory(dir, filePath, MAX_PATH, fileSuffixStr, fileName);
    if (count <= 0) {
        return ERROR_UTILS_ERRORLOG_DIRECTORY_IS_NULL;
    }

    errno_t rc;
    rc = memset_s(fileFullPath, len, 0, len);
    SecurecCheck(rc, true);
    rc = sprintf_s(fileFullPath, len, "%s", filePath);
    SecurecCheck(rc, true);
    return ERROR_SYS_OK;
}

/*
 * Whether the file contains the specified character string.
 */
bool IsFileContainedString(VirtualFileSystem *vfs, const char *fileFullPath, const char *substr, bool isRemoteErrLog)
{
    char buffer[ERROR_LOG_CONTENT_LEN] = {0};
    ErrorCode errorCode = ReadErrorLogFileContent(vfs, fileFullPath, buffer, ERROR_LOG_CONTENT_LEN, isRemoteErrLog);
    if (errorCode != ERROR_SYS_OK) {
        return false;
    }
    return IsSubstringContained(buffer, substr);
}

void ErrorLogContextCallback(void *parameter)
{
    char *string = (char *) parameter;
    ErrContext("%s", string);
}

#define PROCESS_INTERRUPT_STRING  "interrupt string"

bool g_interruptPending = false;

void ProcessInterruptsCallBack()
{
    if (g_interruptPending) {
        /*
        * This variable must be set to false here, if put it at the end,which will
        * cause an infinite loop.
        */
        g_interruptPending = false;
        ErrLog(ERROR, ErrMsg("%s", PROCESS_INTERRUPT_STRING));
    }

}

bool g_sendToFrontend = false;
#define SEND_MESSAGE_TO_FRONTEND  "send message to fronted"

void ErrLogSendToFrontendCallBack(int level, int sqlErrorCode, char *message, char *cause, char *action)
{
    if (g_sendToFrontend) {
        /*
        * This variable must be set to false here, if put it at the end,which will
        * cause an infinite loop.
        */
        g_sendToFrontend = false;
        if ((cause != NULL) && (action != NULL)) {
            ErrLog(ERROR,
                   ErrMsg("%s %d %d %s %s %s", SEND_MESSAGE_TO_FRONTEND, level, sqlErrorCode, message, cause, action));
        } else if (cause != NULL) {
            ErrLog(ERROR, ErrMsg("%s %d %d %s %s", SEND_MESSAGE_TO_FRONTEND, level, sqlErrorCode, message, cause));
        } else if (action != NULL) {
            ErrLog(ERROR, ErrMsg("%s %d %d %s %s", SEND_MESSAGE_TO_FRONTEND, level, sqlErrorCode, message, action));
        } else {
            ErrLog(ERROR, ErrMsg("%s %d %d %s", SEND_MESSAGE_TO_FRONTEND, level, sqlErrorCode, message));
        }
    }
}

/*
 * The thread test function. Increases the value of the input parameter by 1,
 * and then returns the input parameter.
 */
void *ThreadRoutineConcurrentErrLog(void *arg)
{
    int *intPtr = (int *)arg;
    /* A new thread does not open error log in beginning */
    if (IS_ERR_LOG_OPEN()) {
        fprintf(stderr, "error log state invalid!\n");
        *intPtr = -1;
        return arg;
    }
    /* Error log thread context will automatically open in first ErrLog */
    ErrLog(ERROR, ErrMsg("First error log message"));
    if (!IS_ERR_LOG_OPEN()) {
        fprintf(stderr, "open logger failed!\n");
        *intPtr = -1;
        return arg;
    }
    int i;
    for (i = 0; i < *intPtr; i++) {
        ErrLog(ERROR, ErrMsg("%s", "Concurrent ErrLog test,error message"),
               ErrCause("%s", "Concurrent ErrLog test,cause message"),
               ErrAction("%s", "Concurrent ErrLog test,action message"));
    }
    CloseLogger();
    return arg;
}

void StubGetLogRotationTime(const FaultInjectionEntry *entry, TimesSecondsSinceEpoch *time)
{
#define MAX_TIME_STAMP 253402271999 /* 9999-12-31 23:59:59 */
    time->timeSeconds = MAX_TIME_STAMP;
}

void MockOpenLogFailed(const FaultInjectionEntry *entry, ErrorCode *errorCode)
{
    *errorCode = ERROR_UTILS_COMMON_SECURE_C_FUNCTION_FAILED;
}

void MockAllocThreadMsgFailed(const FaultInjectionEntry *entry, char *msgPtr)
{
    msgPtr = NULL;
}

void MockCopyLogIdentifierFailed(const FaultInjectionEntry *entry, LogIdentifier *logId)
{
    logId = NULL;
}

void MockDeleteLogFileFailed(const FaultInjectionEntry *entry, int *rc)
{
    *rc = -1;
    FAULT_INJECTION_INACTIVE(MOCK_DELETE_LOG_FILE_FAILED, FI_GLOBAL);
}

void MockWriteSyslog(const FaultInjectionEntry *entry, char *errBuf)
{
    FileDescriptor *fd = NULL;
    FileParameter fileParameter;
    (void)strcpy_s(fileParameter.storeSpaceName, sizeof(fileParameter.storeSpaceName), "storeSpaceName1");
    fileParameter.streamId = VFS_DEFAULT_FILE_STREAM_ID;
    fileParameter.flag = APPEND_WRITE_FILE;
    fileParameter.fileSubType = ERR_LOG_FILE_TYPE;
    fileParameter.rangeSize = DEFAULT_RANGE_SIZE;
    fileParameter.maxSize = MAX_LOG_FILE_SIZE;
    fileParameter.recycleTtl = 0;
    fileParameter.mode = FILE_READ_AND_WRITE_MODE;
    fileParameter.isReplayWrite = false;
    Create(g_localVfs, "./syslog.txt", fileParameter, &fd);
    (void)WriteAsync(fd, errBuf, strlen(errBuf), NULL);
    Close(fd);
}

void MockStringInitMallocFailed(const FaultInjectionEntry *entry, char **data)
{
    *data = NULL;
}

void MockOpenedFileReachMax(const FaultInjectionEntry *entry, ErrorCode *errorCode)
{
    *errorCode = VFS_ERROR_OPENED_FILE_REACH_MAX;
}

void MockSetLatchFailed(const FaultInjectionEntry *entry)
{
    return;
}

void MockDiskFull(const FaultInjectionEntry *entry, ErrorCode *errorCode)
{
    *errorCode = VFS_ERROR_DISK_HAS_NO_SPACE;
}

/** ************************************************************************************************************* **/

class ErrorLogTest : public testing::Test {
public:

    static void SetUpTestCase()
    {
        ErrorCode errCode = ERROR_SYS_OK;
        errCode = InitVfsModule(NULL);
        if (errCode != ERROR_SYS_OK) {
            fprintf(stderr, "InitVfsModule failed!\n");
        }
        errCode = GetStaticLocalVfsInstance(&g_localVfs);
        if (errCode != ERROR_SYS_OK) {
            fprintf(stderr, "GetStaticLocalVfsInstance failed!\n");
        }
#define TEST_ERROR_LOG_FILE_SIZE (8 * 1024)
#define TEST_TOTAL_ERROR_LOG_SPACE (TEST_ERROR_LOG_FILE_SIZE * 6)
        SetErrLogSpaceSize(TEST_TOTAL_ERROR_LOG_SPACE, TEST_ERROR_LOG_FILE_SIZE);
        FaultInjectionEntry entries[] = {
            FAULT_INJECTION_ENTRY(COPY_SYSLOG_OUTPUT_TO_STDERR, false, nullptr),
            FAULT_INJECTION_ENTRY(MOCK_LOG_ROTATION_TIME, false, StubGetLogRotationTime),
            FAULT_INJECTION_ENTRY(MOCK_OPEN_LOG_FAILED, false, MockOpenLogFailed),
            FAULT_INJECTION_ENTRY(MOCK_ALLOCATE_MESSAGE_FAILED, false, MockAllocThreadMsgFailed),
            FAULT_INJECTION_ENTRY(MOCK_COPY_LOG_IDENTIFIER_FAILED, false, MockCopyLogIdentifierFailed),
            FAULT_INJECTION_ENTRY(MOCK_DELETE_LOG_FILE_FAILED, false, MockDeleteLogFileFailed),
            FAULT_INJECTION_ENTRY(MOCK_INIT_LOG_CFG_PARAM_FAILED, false, nullptr),
            FAULT_INJECTION_ENTRY(MOCK_INIT_VFS_FAILED, false, nullptr),
            FAULT_INJECTION_ENTRY(MOCK_WRITE_SYSLOG, false, MockWriteSyslog),
            FAULT_INJECTION_ENTRY(MOCK_STRING_INIT_MALLOC_FAILED, false, MockStringInitMallocFailed),
            FAULT_INJECTION_ENTRY(MOCK_SET_LATCH_FAILED, false, MockSetLatchFailed),
            FAULT_INJECTION_ENTRY(MOCK_LOG_DISK_FULL, false, MockDiskFull),
            FAULT_INJECTION_ENTRY(MOCK_OPENED_FILE_REACH_MAX, false, MockOpenedFileReachMax),
            FAULT_INJECTION_ENTRY(SET_ERRLOG_XXNAME_FAULT_INJECTION1, false, nullptr),
            FAULT_INJECTION_ENTRY(SET_ERRLOG_XXNAME_FAULT_INJECTION2, false, nullptr),
        };
        ASSERT_EQ(RegisterFaultInjection(entries, sizeof(entries) / sizeof(entries[0]), FI_GLOBAL), ERROR_SYS_OK);
    }

    static void TearDownTestCase()
    {
        /* default init  */
        ErrorCode errCode = ERROR_SYS_OK;
        errCode = ExitVfsModule();
        if (errCode != ERROR_SYS_OK) {
            fprintf(stderr, "ExitVfsModule failed!\n");
        }
        ResetErrLogSpaceSize();
        DestroyFaultInjectionHash(FI_GLOBAL);
    }

    void SetUp() override
    {
        ResetErrorLogConfigure();
    };

    void TearDown() override
    {
        ResetErrorLogConfigure();
        ResetErrLogDirectory();
        ResetPLogDirectory();
        ResetAspLogDirectory();
    };
};

/**
 * @tc.name:  StartLoggerFunction001_Level0
 * @tc.desc:  Test the start logger thread and stop logger thread.
 * @tc.type: FUNC
 */

TEST_F(ErrorLogTest, StartLoggerFunction001_Level0)
{
    /**
    * @tc.steps: step1. Test start logger thread and stop logger thread.
    * @tc.expected: step1.The logger thread functions are correct.
    */
    ErrorCode errorCode = StartLogger();
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    bool started = IsLoggerStarted();
    ASSERT_TRUE(started);
    StopLogger();
    started = IsLoggerStarted();
    ASSERT_FALSE(started);
    /* StopLogger() would not block if called twice */
    StopLogger();
}

TEST_F(ErrorLogTest, WriteLoggerAndExit)
{
    /**
    * @tc.steps: step1. Test start logger thread and stop logger thread.
    * @tc.expected: step1.The logger thread functions are correct.
    */
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, ERROR_LOG_DEFAULT_DIRECTORY);
    ErrorCode errorCode = StartLogger();
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    bool started = IsLoggerStarted();
    ASSERT_TRUE(started);
    ErrLog(ERROR, ErrMsg("WriteLoggerAndExit"));
    StopLogger();
    started = IsLoggerStarted();
    ASSERT_FALSE(started);
    /* StopLogger() would not block if called twice */
    StopLogger();
}

#define LOG_FILE_SIZE 1024 * 11
#define LOG_SPACE_SIZE (LOG_FILE_SIZE * 5)
TEST_F(ErrorLogTest, RestartLoggerAndSizeRotate)
{
    /**
    * @tc.steps: step1. Test start logger thread and stop logger thread.
    * @tc.expected: step1.The logger thread functions are correct.
    */
    SetErrLogSpaceSize(LOG_SPACE_SIZE, LOG_FILE_SIZE);
    StartLogger();

    /* write an error log */
    char *testErrLogStr = "This is an error log test.";

    for (int i = 0; i < 100000; i++) {
        ErrLog(ERROR, ErrMsg("%s", testErrLogStr));
    }
    
    FlushLogger();
    CloseLogger();
    StopLogger();

#define INVALID_LOG_NAME "utils_unittest-1970-01-01_080000.log"
    char filePath[MAX_PATH] = {0};
    EXPECT_EQ(GetFileInfoFromLocalDirectory(ERROR_LOG_DEFAULT_DIRECTORY, filePath, MAX_PATH, ERR_LOG_EXTEND, UT_NAME), 1);

    /* Check the validation of the error log file name */
    Directory dir;
    OpenDirectory(ERROR_LOG_DEFAULT_DIRECTORY, &dir);
    DirectoryEntry dirEntry;
    while (ReadDirectory(&dir, &dirEntry)) {
        if (dirEntry.type != DIR_TYPE_REGULAR_FILE) {
            continue;
        }
        if (!strcmp(dirEntry.name, ".") || !strcmp(dirEntry.name, "..")) {
            continue;
        }
        char *baseFileName = Basename(dirEntry.name);

        EXPECT_NE(strcmp(baseFileName, INVALID_LOG_NAME), 0);
    }

    CloseDirectory(&dir);
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, ERROR_LOG_DEFAULT_DIRECTORY);
}

TEST_F(ErrorLogTest, StartLoggerWithFaultInject)
{
    // active inject1
    FAULT_INJECTION_ACTIVE(SET_ERRLOG_XXNAME_FAULT_INJECTION1, FI_GLOBAL);
    // test
    EXPECT_NE(StartLogger(), ERROR_SYS_OK);
    ASSERT_FALSE(IsLoggerStarted());
    // disable inject1
    FAULT_INJECTION_INACTIVE(SET_ERRLOG_XXNAME_FAULT_INJECTION1, FI_GLOBAL);

    // active inject2
    FAULT_INJECTION_ACTIVE(SET_ERRLOG_XXNAME_FAULT_INJECTION2, FI_GLOBAL);
    // test
    EXPECT_NE(StartLogger(), ERROR_SYS_OK);
    ASSERT_FALSE(IsLoggerStarted());
    // disable inject2
    FAULT_INJECTION_INACTIVE(SET_ERRLOG_XXNAME_FAULT_INJECTION2, FI_GLOBAL);
}

/**
 * @tc.name:  OpenLoggerFunction001_Level0
 * @tc.desc:  Test the open logger and close logger.
 * @tc.type: FUNC
 */

TEST_F(ErrorLogTest, OpenLoggerFunction001_Level0)
{
    /**
    * @tc.steps: step1. Test start logger thread.
    * @tc.expected: step1.The logger thread functions are correct.
    */
    ErrorCode errorCode = StartLogger();
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    bool started = IsLoggerStarted();
    ASSERT_TRUE(started);

    /**
    * @tc.steps: step2. Test open logger and close logger.
    * @tc.expected: step2.The logger open functions are correct.
    */
    errorCode = OpenLogger();
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    bool opened = IS_ERR_LOG_OPEN();
    ASSERT_TRUE(opened);
    CloseLogger();
    opened = IS_ERR_LOG_OPEN();
    ASSERT_FALSE(opened);

    /**
    * @tc.steps: step3. Test stop logger thread.
    * @tc.expected: step3.The logger thread functions are correct.
    */
    StopLogger();
    started = IsLoggerStarted();
    ASSERT_FALSE(started);
}


/**
 * @tc.name:  ErrLogLevelFunction001_Level0
 * @tc.desc:  Test the error log level function.
 * @tc.type: FUNC
 */

TEST_F(ErrorLogTest, ErrLogLevelFunction001_Level0)
{
    /**
    * @tc.steps: step1. Remove all the error log file.
     * GetErrLogDirectory depends on the logger thread startup.
     * Therefore, the logger thread must be started first and then stop.
    * @tc.expected: step1.Remove all log files are correct.
    */
    SetErrLogWriteMode(false);
    ErrorCode errorCode = StartLogger();
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    bool started = IsLoggerStarted();
    ASSERT_TRUE(started);
    char logDirectory[MAX_PATH] = {0};
    GetErrLogDirectory(logDirectory, MAX_PATH);
    StopLogger();
    started = IsLoggerStarted();
    ASSERT_FALSE(started);

    RemoveErrorLogFileFromLocalDirectory(g_localVfs, logDirectory);

    /**
    * @tc.steps: step2. Test start logger thread and open logger.
    * @tc.expected: step2.The logger thread and logger open functions are correct.
    */
    SetErrLogServerLevel(INFO);
    errorCode = StartLogger();
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    started = IsLoggerStarted();
    ASSERT_TRUE(started);

    errorCode = OpenLogger();
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    bool opened = IS_ERR_LOG_OPEN();
    ASSERT_TRUE(opened);

    bool result = false;
    SetErrLogLinePrefixSuffix(0);

    /**
    * @tc.steps: step4. Test error log level filter.
    * @tc.expected: step4.The error log level filter functions are correct.
    */
#define ERROR_LOG_LEVEL_INFO_TEST_STRING  "Hello world, INFO MESSAGE"
    ErrLog(INFO, ErrMsg(ERROR_LOG_LEVEL_INFO_TEST_STRING));
    FlushLogger();
    errorCode = GetErrorLogFileFullPath(g_errorLogFileFullPath, MAX_PATH, GetErrLogDirectory, ERR_LOG_EXTEND, UT_NAME);
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath, ERROR_LOG_LEVEL_INFO_TEST_STRING, false);
    ASSERT_TRUE(result);

#define ERROR_LOG_LEVEL_TEST_WARNING_STRING  "Hello world, WARNING MESSAGE"
    SetErrLogServerLevel(ERROR);
    ErrLog(WARNING, ErrMsg(ERROR_LOG_LEVEL_TEST_WARNING_STRING));
    FlushLogger();
    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath,
                                   ERROR_LOG_LEVEL_TEST_WARNING_STRING, false);
    ASSERT_FALSE(result);

    /**
    * @tc.steps: step5. Test error log level prefix control.
    * @tc.expected: step5.The error log level prefix control functions are correct.
    */
#define ERROR_LOG_LEVEL_TEST_ERROR_STRING  "Hello world, ERROR MESSAGE"
    SetErrLogServerLevel(ERROR);
    ErrLog(ERROR, ErrMsg(ERROR_LOG_LEVEL_TEST_ERROR_STRING));
    FlushLogger();
    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath, ERROR_LOG_LEVEL_TEST_ERROR_STRING, false);
    ASSERT_TRUE(result);

    /**
    * @tc.steps: step6. Test close logger and  stop logger thread.
    * @tc.expected: step6.The logger thread functions are correct.
    */
    CloseLogger();
    opened = IS_ERR_LOG_OPEN();
    ASSERT_FALSE(opened);

    StopLogger();
    started = IsLoggerStarted();
    ASSERT_FALSE(started);
}

/**
 * @tc.name:  ErrLogConnectionFunction001_Level0
 * @tc.desc:  Test the error log connection function.
 * @tc.type: FUNC
 */
TEST_F(ErrorLogTest, DISABLED_ErrLogConnectionFunction001_Level0)
{
    /**
    * @tc.steps: step1. Remove all the error log file.
     * GetErrLogDirectory depends on the logger thread startup.
     * Therefore, the logger thread must be started first and then stop.
    * @tc.expected: step1.Remove all log files are correct.
    */
    SetErrLogWriteMode(false);
    ErrorCode errorCode = StartLogger();
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    bool started = IsLoggerStarted();
    ASSERT_TRUE(started);
    char logDirectory[MAX_PATH] = {0};
    GetErrLogDirectory(logDirectory, MAX_PATH);
    StopLogger();
    started = IsLoggerStarted();
    ASSERT_FALSE(started);

    RemoveErrorLogFileFromLocalDirectory(g_localVfs, logDirectory);

    /**
    * @tc.steps: step2. Test start logger thread and open logger.
    * @tc.expected: step2.The logger thread and logger open functions are correct.
    */
    errorCode = StartLogger();
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    started = IsLoggerStarted();
    ASSERT_TRUE(started);

    errorCode = OpenLogger();
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    bool opened = IS_ERR_LOG_OPEN();
    ASSERT_TRUE(opened);

    bool result = false;
    SetErrLogLinePrefixSuffix(0);

#define CONNECTION_FILTER_COUNT  1
#define CONNECT_FILTER_TEST_PORT 80
    SetErrLogServerLevel(ERROR);
    //fprintf(stderr,"%s\n","ErrLogConnectionFunction001_Level0 1");
    /**
     * @tc.steps: step4. Test error log connection no filter.
     * @tc.expected: step4.The error log connection no filter functions are correct.
     */
#define CONNECTION_INFO_NO_FILTER_TEST_STRING  "Hello world, No filter Connection Message"
    ClearErrLogConnectionFilter();
    ErrLog(ERROR, ErrMsg(CONNECTION_INFO_NO_FILTER_TEST_STRING));
    FlushLogger();
    errorCode = GetErrorLogFileFullPath(g_errorLogFileFullPath, MAX_PATH, GetErrLogDirectory, ERR_LOG_EXTEND, UT_NAME);
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath, CONNECTION_INFO_NO_FILTER_TEST_STRING, false);
    ASSERT_TRUE(result);
    /**
     * @tc.steps: step5. Test error log connection normal filter.
     * @tc.expected: step5.The error log connection normal filter functions are correct.
     */
#define CONNECTION_INFO_FILTER_NORMAL_MATCH_TEST_STRING  "Hello world, Filter Normal Match Connection Message"
    const char *srcIPFilter[CONNECTION_FILTER_COUNT] = {"10.168.0.1"};
    int srcPortFilter[CONNECTION_FILTER_COUNT] = {CONNECT_FILTER_TEST_PORT};
    const char *destIPFilter[CONNECTION_FILTER_COUNT] = {"10.168.0.1"};
    int destPortFilter[CONNECTION_FILTER_COUNT] = {CONNECT_FILTER_TEST_PORT};
    SetErrLogConnectionFilter((char **) srcIPFilter, srcPortFilter, (char **) destIPFilter, destPortFilter,
                              CONNECTION_FILTER_COUNT);
    const char *srcIP = "10.168.0.1";
    const char *destIP = "10.168.0.1";
    SetErrLogConnectionInfo((char *) srcIP, CONNECT_FILTER_TEST_PORT, (char *) destIP, CONNECT_FILTER_TEST_PORT);
    ErrLog(ERROR, ErrMsg(CONNECTION_INFO_FILTER_NORMAL_MATCH_TEST_STRING));
    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath,
                                   CONNECTION_INFO_FILTER_NORMAL_MATCH_TEST_STRING, false);
    ASSERT_TRUE(result);
    //fprintf(stderr,"%s\n","ErrLogConnectionFunction001_Level0 2");
#define CONNECTION_INFO_FILTER_NORMAL_NO_MATCH_TEST_STRING  "Hello world, Filter Normal No Match Connection Message"
    ClearErrLogConnectionInfo();
    const char *srcIP2 = "10.168.0.2";
    SetErrLogConnectionInfo((char *) srcIP2, CONNECT_FILTER_TEST_PORT, (char *) destIP, CONNECT_FILTER_TEST_PORT);
    ErrLog(ERROR, ErrMsg(CONNECTION_INFO_FILTER_NORMAL_NO_MATCH_TEST_STRING));
    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath,
                                   CONNECTION_INFO_FILTER_NORMAL_NO_MATCH_TEST_STRING, false);
    ASSERT_FALSE(result);
    /**
     * @tc.steps: step6. Test error log connection wild filter.
     * @tc.expected: step6.The error log connection wild filter functions are correct.
     */
    //fprintf(stderr,"%s\n","ErrLogConnectionFunction001_Level0 3");
#define CONNECTION_INFO_FILTER_WILD_MATCH_TEST_STRING  "Hello world, Filter Wild Match Connection Message"
    ClearErrLogConnectionFilter();
    const char *srcIPFilterWild[CONNECTION_FILTER_COUNT] = {LOG_STRING_WILDCARD};
    int srcPortFilterWild[CONNECTION_FILTER_COUNT] = {LOG_INT_WILDCARD};
    const char *destIPFilterWild[CONNECTION_FILTER_COUNT] = {LOG_STRING_WILDCARD};
    int destPortFilterWild[CONNECTION_FILTER_COUNT] = {LOG_INT_WILDCARD};
    SetErrLogConnectionFilter((char **) srcIPFilterWild, srcPortFilterWild, (char **) destIPFilterWild,
                              destPortFilterWild, CONNECTION_FILTER_COUNT);
    SetErrLogConnectionInfo((char *) srcIP, CONNECT_FILTER_TEST_PORT, (char *) destIP, CONNECT_FILTER_TEST_PORT);
    ErrLog(ERROR, ErrMsg(CONNECTION_INFO_FILTER_WILD_MATCH_TEST_STRING));
    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath,
                                   CONNECTION_INFO_FILTER_WILD_MATCH_TEST_STRING, false);
    ASSERT_TRUE(result);
    //fprintf(stderr,"%s\n","ErrLogConnectionFunction001_Level0 4");
    /**
     * @tc.steps: step7. Test error log connection prefix control.
     * @tc.expected: step7.The error log connection prefix control functions are correct.
     */
    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath, srcIP, false);
    ASSERT_FALSE(result);
    uint32_t linePrefixSuffix = GetErrLogLinePrefixSuffix();
    linePrefixSuffix = linePrefixSuffix | LOG_LINE_PREFIX_CONNECTION_SRC_IP;
    SetErrLogLinePrefixSuffix(linePrefixSuffix);
    ErrLog(ERROR, ErrMsg(CONNECTION_INFO_FILTER_WILD_MATCH_TEST_STRING));
    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath, srcIP, false);
    ASSERT_TRUE(result);

    /**
    * @tc.steps: step8. Test close logger and  stop logger thread.
    * @tc.expected: step8.The logger thread functions are correct.
    */
    CloseLogger();
    opened = IS_ERR_LOG_OPEN();
    ASSERT_FALSE(opened);

    StopLogger();
    started = IsLoggerStarted();
    ASSERT_FALSE(started);
}

/**
 * @tc.name:  ErrLogSessionFunction001_Level0
 * @tc.desc:  Test the error log Session function.
 * @tc.type: FUNC
 */
TEST_F(ErrorLogTest, DISABLED_ErrLogSessionFunction001_Level0)
{
    /**
    * @tc.steps: step1. Remove all the error log file.
     * GetErrLogDirectory depends on the logger thread startup.
     * Therefore, the logger thread must be started first and then stop.
    * @tc.expected: step1.Remove all log files are correct.
    */
    SetErrLogWriteMode(false);
    ErrorCode errorCode = StartLogger();
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    bool started = IsLoggerStarted();
    ASSERT_TRUE(started);
    char logDirectory[MAX_PATH] = {0};
    GetErrLogDirectory(logDirectory, MAX_PATH);
    StopLogger();
    started = IsLoggerStarted();
    ASSERT_FALSE(started);

    RemoveErrorLogFileFromLocalDirectory(g_localVfs, logDirectory);

    /**
    * @tc.steps: step2. Test start logger thread and open logger.
    * @tc.expected: step2.The logger thread and logger open functions are correct.
    */
    errorCode = StartLogger();
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    started = IsLoggerStarted();
    ASSERT_TRUE(started);

    errorCode = OpenLogger();
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    bool opened = IS_ERR_LOG_OPEN();
    ASSERT_TRUE(opened);

    bool result = false;
    SetErrLogLinePrefixSuffix(0);

#define SESSION_FILTER_COUNT  1
#define SESSION_FILTER_TEST_ID 80
    SetErrLogServerLevel(ERROR);

    /**
     * @tc.steps: step4. Test error log session no filter.
     * @tc.expected: step4.The error log session no filter functions are correct.
     */
#define SESSION_INFO_NO_FILTER_TEST_STRING  "Hello world, No filter Session Message"
    ClearErrLogSessionFilter();
    //fprintf(stderr,"%s\n","ErrLogSessionFunction001_Level0 1");
    ErrLog(ERROR, ErrMsg(SESSION_INFO_NO_FILTER_TEST_STRING));
    FlushLogger();
    errorCode = GetErrorLogFileFullPath(g_errorLogFileFullPath, MAX_PATH, GetErrLogDirectory, ERR_LOG_EXTEND, UT_NAME);
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath, SESSION_INFO_NO_FILTER_TEST_STRING, false);
    ASSERT_TRUE(result);
    /**
     * @tc.steps: step5. Test error log session normal filter.
     * @tc.expected: step5.The error log session normal filter functions are correct.
     */
#define SESSION_INFO_FILTER_NORMAL_MATCH_TEST_STRING  "Hello world, Filter Normal Match Session Message"
    const char *applicationNameFilter[SESSION_FILTER_COUNT] = {"appName"};
    const char *databaseNameFilter[SESSION_FILTER_COUNT] = {"databaseName"};
    const char *userNameFilter[SESSION_FILTER_COUNT] = {"userName"};
    int sessionID[SESSION_FILTER_COUNT] = {SESSION_FILTER_TEST_ID};
    SetErrLogSessionFilter((char **) applicationNameFilter, (char **) databaseNameFilter, (char **) userNameFilter,
                           sessionID, SESSION_FILTER_COUNT);
    const char *applicationName = "appName";
    const char *databaseName = "databaseName";
    const char *userName = "userName";
    SetErrLogSessionInfo((char *) applicationName, (char *) databaseName, (char *) userName, SESSION_FILTER_TEST_ID);
    //fprintf(stderr,"%s\n","ErrLogSessionFunction001_Level0 2");
    ErrLog(ERROR, ErrMsg(SESSION_INFO_FILTER_NORMAL_MATCH_TEST_STRING));
    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath, SESSION_INFO_FILTER_NORMAL_MATCH_TEST_STRING, false);
    ASSERT_TRUE(result);

#define SESSION_INFO_FILTER_NORMAL_NO_MATCH_TEST_STRING  "Hello world, Filter Normal No Match Session Message"
    ClearErrLogConnectionInfo();
    const char *applicationName2 = "appName2";
    SetErrLogSessionInfo((char *) applicationName2, (char *) databaseName, (char *) userName, SESSION_FILTER_TEST_ID);
    //fprintf(stderr,"%s\n","ErrLogSessionFunction001_Level0 3");
    ErrLog(ERROR, ErrMsg(SESSION_INFO_FILTER_NORMAL_NO_MATCH_TEST_STRING));
    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath, SESSION_INFO_FILTER_NORMAL_NO_MATCH_TEST_STRING, false);
    ASSERT_FALSE(result);
    /**
     * @tc.steps: step6. Test error log session wild filter.
     * @tc.expected: step6.The error log session wild filter functions are correct.
     */
#define SESSION_INFO_FILTER_WILD_MATCH_TEST_STRING  "Hello world, Filter Wild Match Session Message"
    ClearErrLogSessionFilter();
    const char *applicationNameFilterWild[SESSION_FILTER_COUNT] = {LOG_STRING_WILDCARD};
    const char *databaseNameFilterWild[SESSION_FILTER_COUNT] = {LOG_STRING_WILDCARD};
    const char *userNameFilterWild[SESSION_FILTER_COUNT] = {LOG_STRING_WILDCARD};
    int sessionIDFilterWild[SESSION_FILTER_COUNT] = {LOG_INT_WILDCARD};
    SetErrLogSessionFilter((char **) applicationNameFilterWild, (char **) databaseNameFilterWild,
                           (char **) userNameFilterWild, sessionIDFilterWild, SESSION_FILTER_COUNT);
    SetErrLogSessionInfo((char *) applicationName, (char *) databaseName, (char *) userName, SESSION_FILTER_TEST_ID);
    //fprintf(stderr,"%s\n","ErrLogSessionFunction001_Level0 4");
    ErrLog(ERROR, ErrMsg(SESSION_INFO_FILTER_WILD_MATCH_TEST_STRING));
    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath, SESSION_INFO_FILTER_WILD_MATCH_TEST_STRING, false);
    ASSERT_TRUE(result);
    /**
     * @tc.steps: step7. Test error log session prefix control.
     * @tc.expected: step7.The error log session prefix control functions are correct.
     */
    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath, applicationName, false);
    ASSERT_FALSE(result);
    uint32_t linePrefixSuffix = GetErrLogLinePrefixSuffix();
    linePrefixSuffix = linePrefixSuffix | LOG_LINE_PREFIX_SESSION_APPLICATION_NAME;
    SetErrLogLinePrefixSuffix(linePrefixSuffix);
    //fprintf(stderr,"%s\n","ErrLogSessionFunction001_Level0 5");
    ErrLog(ERROR, ErrMsg(SESSION_INFO_FILTER_WILD_MATCH_TEST_STRING));
    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath, applicationName, false);
    ASSERT_TRUE(result);

    /**
    * @tc.steps: step8. Test close logger and  stop logger thread.
    * @tc.expected: step8.The logger thread functions are correct.
    */
    CloseLogger();
    opened = IS_ERR_LOG_OPEN();
    ASSERT_FALSE(opened);

    StopLogger();
    started = IsLoggerStarted();
    ASSERT_FALSE(started);
}

/**
 * @tc.name:  ErrLogRunningContextFunction001_Level0
 * @tc.desc:  Test the error log running context function.
 * @tc.type: FUNC
 */
TEST_F(ErrorLogTest, DISABLED_ErrLogRunningContextFunction001_Level0)
{
    /**
    * @tc.steps: step1. Remove all the error log file.
     * GetErrLogDirectory depends on the logger thread startup.
     * Therefore, the logger thread must be started first and then stop.
    * @tc.expected: step1.Remove all log files are correct.
    */
    SetErrLogWriteMode(false);
    ErrorCode errorCode = StartLogger();
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    bool started = IsLoggerStarted();
    ASSERT_TRUE(started);
    char logDirectory[MAX_PATH] = {0};
    GetErrLogDirectory(logDirectory, MAX_PATH);
    StopLogger();
    started = IsLoggerStarted();
    ASSERT_FALSE(started);

    RemoveErrorLogFileFromLocalDirectory(g_localVfs, logDirectory);

    /**
    * @tc.steps: step2. Test start logger thread and open logger.
    * @tc.expected: step2.The logger thread and logger open functions are correct.
    */
    errorCode = StartLogger();
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    started = IsLoggerStarted();
    ASSERT_TRUE(started);

    /* Set thread name before open log */
    const char *threadName = "threadName";
    ASSERT_EQ(ThreadSetName(threadName), ERROR_SYS_OK);

    errorCode = OpenLogger();
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    bool opened = IS_ERR_LOG_OPEN();
    ASSERT_TRUE(opened);

    bool result = false;
    SetErrLogLinePrefixSuffix(0);

#define RUNNING_CONTEXT_FILTER_COUNT  1
#define RUNNING_CONTEXT_FILTER_TEST_ID 80
    SetErrLogServerLevel(ERROR);

    /**
     * @tc.steps: step4. Test error log running context no filter.
     * @tc.expected: step4.The error log running context no filter functions are correct.
     */
#define RUNNING_CONTEXT_INFO_NO_FILTER_TEST_STRING  "Hello world, No filter Running Context Message"
    ClearErrLogRunningContextFilter();
    ErrLog(ERROR, ErrMsg(RUNNING_CONTEXT_INFO_NO_FILTER_TEST_STRING));
    FlushLogger();
    errorCode = GetErrorLogFileFullPath(g_errorLogFileFullPath, MAX_PATH, GetErrLogDirectory, ERR_LOG_EXTEND, UT_NAME);
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath, RUNNING_CONTEXT_INFO_NO_FILTER_TEST_STRING, false);
    ASSERT_TRUE(result);
    /**
     * @tc.steps: step5. Test error log running context normal filter.
     * @tc.expected: step5.The error log running context  normal filter functions are correct.
     */
#define RUNNING_CONTEXT_INFO_FILTER_NORMAL_MATCH_TEST_STRING  "Hello world, Filter Normal Match Running Context Message"
    int pidFilter[RUNNING_CONTEXT_FILTER_COUNT] = {LOG_INT_WILDCARD};
    uint32_t tidFilter[RUNNING_CONTEXT_FILTER_COUNT] = {LOG_INT_WILDCARD};
    const char *threadNameFilter[RUNNING_CONTEXT_FILTER_COUNT] = {"threadName"};
    SetErrLogRunningContextFilter(pidFilter, tidFilter, (char **) threadNameFilter, RUNNING_CONTEXT_FILTER_COUNT);
    ErrLog(ERROR, ErrMsg(RUNNING_CONTEXT_INFO_FILTER_NORMAL_MATCH_TEST_STRING));
    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath,
                                   RUNNING_CONTEXT_INFO_FILTER_NORMAL_MATCH_TEST_STRING, false);
    ASSERT_TRUE(result);

#define RUNNING_CONTEXT_INFO_FILTER_NORMAL_NO_MATCH_TEST_STRING  "Hello world, Filter Normal No Match Running Context Message"
    ClearErrLogRunningContextInfo();
    const char *threadName2 = "threadName2";
    SetErrLogRunningContextInfo((char *) threadName2);
    ErrLog(ERROR, ErrMsg(RUNNING_CONTEXT_INFO_FILTER_NORMAL_NO_MATCH_TEST_STRING));
    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath,
                                   RUNNING_CONTEXT_INFO_FILTER_NORMAL_NO_MATCH_TEST_STRING, false);
    ASSERT_FALSE(result);
    /**
     * @tc.steps: step6. Test error log running context wild filter.
     * @tc.expected: step6.The error log running context wild filter functions are correct.
     */
#define RUNNING_CONTEXT_INFO_FILTER_WILD_MATCH_TEST_STRING  "Hello world, Filter Wild Match Running Context Message"
    ClearErrLogRunningContextFilter();
    int pidFilterWild[RUNNING_CONTEXT_FILTER_COUNT] = {LOG_INT_WILDCARD};
    uint32_t tidFilterWild[RUNNING_CONTEXT_FILTER_COUNT] = {LOG_INT_WILDCARD};
    const char *threadNameFilterWild[RUNNING_CONTEXT_FILTER_COUNT] = {"*"};
    SetErrLogRunningContextFilter(pidFilterWild, tidFilterWild, (char **) threadNameFilterWild,
                                  RUNNING_CONTEXT_FILTER_COUNT);
    SetErrLogRunningContextInfo((char *) threadName);
    ErrLog(ERROR, ErrMsg(RUNNING_CONTEXT_INFO_FILTER_WILD_MATCH_TEST_STRING));
    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath,
                                   RUNNING_CONTEXT_INFO_FILTER_WILD_MATCH_TEST_STRING, false);
    ASSERT_TRUE(result);
    /**
     * @tc.steps: step7. Test error log running context  prefix control.
     * @tc.expected: step7.The error log running context prefix control functions are correct.
     */
    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath, threadName, false);
    ASSERT_FALSE(result);
    uint32_t linePrefixSuffix = GetErrLogLinePrefixSuffix();
    linePrefixSuffix = linePrefixSuffix | LOG_LINE_PREFIX_RUNNING_CONTEXT_THREAD_NAME;
    SetErrLogLinePrefixSuffix(linePrefixSuffix);
    ErrLog(ERROR, ErrMsg(RUNNING_CONTEXT_INFO_FILTER_WILD_MATCH_TEST_STRING));
    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath, threadName, false);
    ASSERT_TRUE(result);

    /**
    * @tc.steps: step8. Test close logger and  stop logger thread.
    * @tc.expected: step8.The logger thread functions are correct.
    */
    CloseLogger();
    opened = IS_ERR_LOG_OPEN();
    ASSERT_FALSE(opened);

    StopLogger();
    started = IsLoggerStarted();
    ASSERT_FALSE(started);
}

/**
 * @tc.name:  ErrLogCompileContextFunction001_Level0
 * @tc.desc:  Test the error log compile context (include position and module) function.
 * @tc.type: FUNC
 */
TEST_F(ErrorLogTest, DISABLED_ErrLogCompileContextFunction001_Level0)
{
    /**
    * @tc.steps: step1. Remove all the error log file.
     * GetErrLogDirectory depends on the logger thread startup.
     * Therefore, the logger thread must be started first and then stop.
    * @tc.expected: step1.Remove all log files are correct.
    */
    SetErrLogWriteMode(false);
    ErrorCode errorCode = StartLogger();
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    bool started = IsLoggerStarted();
    ASSERT_TRUE(started);
    char logDirectory[MAX_PATH] = {0};
    GetErrLogDirectory(logDirectory, MAX_PATH);
    StopLogger();
    started = IsLoggerStarted();
    ASSERT_FALSE(started);

    RemoveErrorLogFileFromLocalDirectory(g_localVfs, logDirectory);

    /**
    * @tc.steps: step2. Test start logger thread and open logger.
    * @tc.expected: step2.The logger thread and logger open functions are correct.
    */
    errorCode = StartLogger();
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    started = IsLoggerStarted();
    ASSERT_TRUE(started);

    errorCode = OpenLogger();
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    bool opened = IS_ERR_LOG_OPEN();
    ASSERT_TRUE(opened);

    bool result = false;
    SetErrLogLinePrefixSuffix(0);

#define COMPILE_CONTEXT_FILTER_COUNT  1
    SetErrLogServerLevel(ERROR);

    /**
     * @tc.steps: step4. Test error log compile context no filter.
     * @tc.expected: step4.The error log compile context no filter functions are correct.
     */
#define COMPILE_CONTEXT_INFO_NO_FILTER_TEST_STRING  "Hello world, No filter Compile Context Message"
    ResetErrLogModuleContextFilter();
    ClearErrLogPositionContextFilter();
    ErrLog(ERROR, ErrMsg(COMPILE_CONTEXT_INFO_NO_FILTER_TEST_STRING));
    FlushLogger();
    errorCode = GetErrorLogFileFullPath(g_errorLogFileFullPath, MAX_PATH, GetErrLogDirectory, ERR_LOG_EXTEND, UT_NAME);
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath, COMPILE_CONTEXT_INFO_NO_FILTER_TEST_STRING, false);
    ASSERT_TRUE(result);
    /**
     * @tc.steps: step5. Test error log compile context normal filter.
     * @tc.expected: step5.The error log compile context  normal filter functions are correct.
     */
#define COMPILE_CONTEXT_INFO_FILTER_MODULE_MATCH_TEST_STRING  "Hello world, Filter Normal Match Compile Context Message"
    int componentIdFilter = 1;
    int moduleIdFilter[COMPILE_CONTEXT_FILTER_COUNT] = {1};
    int errorLevelFilter[COMPILE_CONTEXT_FILTER_COUNT] = {ERROR};
    const char *fileNameFilter[COMPILE_CONTEXT_FILTER_COUNT] = {LOG_STRING_WILDCARD};
    int line[COMPILE_CONTEXT_FILTER_COUNT] = {LOG_INT_WILDCARD};
    const char *functionNameFilter[COMPILE_CONTEXT_FILTER_COUNT] = {LOG_STRING_WILDCARD};
    SetErrLogModuleContextFilter(componentIdFilter, (const int *) moduleIdFilter,
                                 (const int *) errorLevelFilter, COMPILE_CONTEXT_FILTER_COUNT);
    SetErrLogPositionContextFilter((char **) fileNameFilter, line, (char **) functionNameFilter,
                                   COMPILE_CONTEXT_FILTER_COUNT);

#undef LOCAL_COMPONENT_NAME
#define LOCAL_COMPONENT_NAME      "component name"
#undef LOCAL_COMPONENT_ID
#define LOCAL_COMPONENT_ID        1
#undef LOCAL_MODULE_NAME
#define LOCAL_MODULE_NAME         "module name"
#undef LOCAL_MODULE_ID
#define LOCAL_MODULE_ID           1
    ErrLog(ERROR, ErrMsg(COMPILE_CONTEXT_INFO_FILTER_MODULE_MATCH_TEST_STRING));
    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath,
                                   COMPILE_CONTEXT_INFO_FILTER_MODULE_MATCH_TEST_STRING, false);
    ASSERT_TRUE(result);

#define COMPILE_CONTEXT_INFO_FILTER_MODULE_NO_MATCH_TEST_STRING  "Hello world, Filter Normal No Match Compile Context Message"
#undef LOCAL_MODULE_ID
#define LOCAL_MODULE_ID           2
    //fprintf(stderr,"ErrLogCompileContextFunction001_Level0  -1 \n");
    ErrLog(NOTICE, ErrMsg(COMPILE_CONTEXT_INFO_FILTER_MODULE_NO_MATCH_TEST_STRING));
    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath,
                                   COMPILE_CONTEXT_INFO_FILTER_MODULE_NO_MATCH_TEST_STRING, false);
    ASSERT_FALSE(result);
    //fprintf(stderr,"ErrLogCompileContextFunction001_Level0  0 \n");
    /**
     * @tc.steps: step6. Test error log compile context wild filter.
     * @tc.expected: step6.The error log compile context wild filter functions are correct.
     */
#define COMPILE_CONTEXT_INFO_FILTER_COMPONENT_MATCH_TEST_STRING  "Hello world, Filter Wild Match Compile Context Message"
#undef LOCAL_MODULE_ID
#define LOCAL_MODULE_ID           1
    ResetErrLogModuleContextFilter();
    ClearErrLogPositionContextFilter();
    SetErrLogModuleContextFilter(componentIdFilter, NULL, (const int *) errorLevelFilter,
                                 COMPILE_CONTEXT_FILTER_COUNT);
    SetErrLogPositionContextFilter((char **) fileNameFilter, line, (char **) functionNameFilter,
                                   COMPILE_CONTEXT_FILTER_COUNT);

    //fprintf(stderr,"ErrLogCompileContextFunction001_Level0  1 \n");
    ErrLog(ERROR, ErrMsg(COMPILE_CONTEXT_INFO_FILTER_COMPONENT_MATCH_TEST_STRING));
    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath,
                                   COMPILE_CONTEXT_INFO_FILTER_COMPONENT_MATCH_TEST_STRING, false);
    ASSERT_TRUE(result);
    /**
     * @tc.steps: step7. Test error log compile context  prefix control.
     * @tc.expected: step7.The error log compile context prefix control functions are correct.
     */
    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath, LOCAL_COMPONENT_NAME, false);
    ASSERT_FALSE(result);
    uint32_t linePrefixSuffix = GetErrLogLinePrefixSuffix();
    linePrefixSuffix = linePrefixSuffix | LOG_LINE_PREFIX_COMPILATION_CONTEXT_COMPONENT_NAME;
    SetErrLogLinePrefixSuffix(linePrefixSuffix);
    //fprintf(stderr,"errlog before 2 \n");
    ErrLog(ERROR, ErrMsg(COMPILE_CONTEXT_INFO_FILTER_COMPONENT_MATCH_TEST_STRING));
    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath, LOCAL_COMPONENT_NAME, false);
    ASSERT_TRUE(result);

    /**
    * @tc.steps: step8. Test close logger and  stop logger thread.
    * @tc.expected: step8.The logger thread functions are correct.
    */
    ResetErrLogModuleContextFilter();
    CloseLogger();
    opened = IS_ERR_LOG_OPEN();
    ASSERT_FALSE(opened);

    StopLogger();
    started = IsLoggerStarted();
    ASSERT_FALSE(started);
}


TEST_F(ErrorLogTest, DISABLED_ErrLogModuleFilterTest)
{
    /* Clear existing log file */
    StartLogger();
    char logDirectory[MAX_PATH] = {0};
    GetErrLogDirectory(logDirectory, MAX_PATH);
    StopLogger();
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, logDirectory);

    /* Start logger */
    StartLogger();
    OpenLogger();

    /* Set log prefix */
    uint32_t linePrefixSuffix = GetErrLogLinePrefixSuffix();
    linePrefixSuffix = linePrefixSuffix | LOG_LINE_PREFIX_COMPILATION_CONTEXT_COMPONENT_NAME |
                       LOG_LINE_PREFIX_COMPILATION_CONTEXT_MODULE_NAME;
    SetErrLogLinePrefixSuffix(linePrefixSuffix);
    SetErrLogServerLevel(WARNING);

    /* Set module filter condition */
#define COMPONENT_ID 1
#define MODULE_ID_1 1
#define MODULE_ID_2 2
#define MODULE_ID_3 3
#undef COMPILE_CONTEXT_FILTER_COUNT
#define COMPILE_CONTEXT_FILTER_COUNT 3
    int componentIdFilter = COMPONENT_ID;
    int moduleIdFilter[COMPILE_CONTEXT_FILTER_COUNT] = {MODULE_ID_1, MODULE_ID_2, MODULE_ID_3};
    int errorLevelFilter[COMPILE_CONTEXT_FILTER_COUNT] = {DEBUG, WARNING, ERROR};
    ResetErrLogModuleContextFilter();
    SetErrLogModuleContextFilter(componentIdFilter, moduleIdFilter, (const int *) errorLevelFilter,
                                 COMPILE_CONTEXT_FILTER_COUNT);

    /* Print error log */
#define MODULE_1_INFO_FILTER_MATCH_TEST_STRING "This is module 1 INFO log."
#define MODULE_1_WARNING_FILTER_MATCH_TEST_STRING "This is module 1 WARNING log."
#define MODULE_2_NOTICE_FILTER_NO_MATCH_TEST_STRING "This is module 2 NOTICE log."
#define MODULE_2_WARNING_FILTER_MATCH_TEST_STRING "This is module 2 WARNING log."
#define MODULE_3_DEBUG_FILTER_NO_MATCH_TEST_STRING "This is module 3 WARNING log."
#define MODULE_3_ERROR_FILTER_MATCH_TEST_STRING "This is module 3 ERROR log."
    /* Module 1 errlog */
#undef LOCAL_COMPONENT_NAME
#define LOCAL_COMPONENT_NAME      "component name"
#undef LOCAL_COMPONENT_ID
#define LOCAL_COMPONENT_ID        COMPONENT_ID
#undef LOCAL_MODULE_NAME
#define LOCAL_MODULE_NAME         "module1"
#undef LOCAL_MODULE_ID
#define LOCAL_MODULE_ID           MODULE_ID_1
    ErrLog(INFO, ErrMsg(MODULE_1_INFO_FILTER_MATCH_TEST_STRING));
    ErrLog(WARNING, ErrMsg(MODULE_1_WARNING_FILTER_MATCH_TEST_STRING));
    /* Module 2 errlog */
#undef LOCAL_MODULE_NAME
#define LOCAL_MODULE_NAME         "module2"
#undef LOCAL_MODULE_ID
#define LOCAL_MODULE_ID           MODULE_ID_2
    ErrLog(NOTICE, ErrMsg(MODULE_2_NOTICE_FILTER_NO_MATCH_TEST_STRING));
    ErrLog(WARNING, ErrMsg(MODULE_2_WARNING_FILTER_MATCH_TEST_STRING));
    /* Module 3 errlog */
#undef LOCAL_MODULE_NAME
#define LOCAL_MODULE_NAME         "module3"
#undef LOCAL_MODULE_ID
#define LOCAL_MODULE_ID           MODULE_ID_3
    ErrLog(DEBUG, ErrMsg(MODULE_3_DEBUG_FILTER_NO_MATCH_TEST_STRING));
    ErrLog(ERROR, ErrMsg(MODULE_3_ERROR_FILTER_MATCH_TEST_STRING));
    FlushLogger();
    GetErrorLogFileFullPath(g_errorLogFileFullPath, MAX_PATH, GetErrLogDirectory, ERR_LOG_EXTEND, UT_NAME);

    /* Check result */
    bool result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath, MODULE_1_INFO_FILTER_MATCH_TEST_STRING, false);
    ASSERT_TRUE(result);
    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath, MODULE_1_WARNING_FILTER_MATCH_TEST_STRING, false);
    ASSERT_TRUE(result);
    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath, MODULE_2_NOTICE_FILTER_NO_MATCH_TEST_STRING, false);
    ASSERT_FALSE(result);
    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath, MODULE_2_WARNING_FILTER_MATCH_TEST_STRING, false);
    ASSERT_TRUE(result);
    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath, MODULE_3_DEBUG_FILTER_NO_MATCH_TEST_STRING, false);
    ASSERT_FALSE(result);
    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath, MODULE_3_ERROR_FILTER_MATCH_TEST_STRING, false);
    ASSERT_TRUE(result);

    ResetErrLogModuleContextFilter();
    /* Exit Logger */
    CloseLogger();
    StopLogger();
}

TEST_F(ErrorLogTest, DISABLED_ErrLogModuleFilterDefaultValueTest)
{
    /* Clear existing log file */
    StartLogger();
    char logDirectory[MAX_PATH] = {0};
    GetErrLogDirectory(logDirectory, MAX_PATH);
    StopLogger();
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, logDirectory);

    /* Start logger */
    StartLogger();
    OpenLogger();

    /* Set log prefix */
    uint32_t linePrefixSuffix = GetErrLogLinePrefixSuffix();
    linePrefixSuffix = linePrefixSuffix | LOG_LINE_PREFIX_COMPILATION_CONTEXT_COMPONENT_NAME |
                       LOG_LINE_PREFIX_COMPILATION_CONTEXT_MODULE_NAME;
    SetErrLogLinePrefixSuffix(linePrefixSuffix);
    SetErrLogServerLevel(WARNING);

    /* Set module filter condition */
#define COMPONENT_ID 1
#define MODULE_ID_1 1
#define MODULE_ID_2 2
#define MODULE_ID_3 3
#define MODULE_ID_4 4
#undef COMPILE_CONTEXT_FILTER_COUNT
#define COMPILE_CONTEXT_FILTER_COUNT 3
    int componentIdFilter = COMPONENT_ID;
    int moduleIdFilter[COMPILE_CONTEXT_FILTER_COUNT] = {MODULE_ID_1, MODULE_ID_2, MODULE_ID_3};
    int errorLevelFilter[COMPILE_CONTEXT_FILTER_COUNT] = {DEBUG, WARNING, ERROR};
    ResetErrLogModuleContextFilter();
    SetErrLogModuleContextFilter(componentIdFilter, moduleIdFilter, (const int *) errorLevelFilter,
                                 COMPILE_CONTEXT_FILTER_COUNT);

    /* Print error log */
#define MODULE_4_DEBUG_FILTER_NO_MATCH_DEFAULT_TEST_STRING "This is module 4 DEBUG log."
#define MODULE_4_LOG_FILTER_NO_MATCH_DEFAULT_TEST_STRING "This is module 4 LOG log."
#define MODULE_4_INFO_FILTER_NO_MATCH_DEFAULT_TEST_STRING "This is module 4 INFO log."
#define MODULE_4_NOTICE_FILTER_NO_MATCH_DEFAULT_TEST_STRING "This is module 4 NOTICE log."
#define MODULE_4_WARNING_FILTER_MATCH_DEFAULT_TEST_STRING "This is module 4 WARNING log."
#define MODULE_4_ERROR_FILTER_MATCH_DEFAULT_TEST_STRING "This is module 4 ERROR log."
    /* Module 4 errlog */
#undef LOCAL_COMPONENT_NAME
#define LOCAL_COMPONENT_NAME      "component name"
#undef LOCAL_COMPONENT_ID
#define LOCAL_COMPONENT_ID        COMPONENT_ID
#undef LOCAL_MODULE_NAME
#define LOCAL_MODULE_NAME         "module4"
#undef LOCAL_MODULE_ID
#define LOCAL_MODULE_ID           MODULE_ID_4
    ErrLog(DEBUG, ErrMsg(MODULE_4_DEBUG_FILTER_NO_MATCH_DEFAULT_TEST_STRING));
    ErrLog(LOG, ErrMsg(MODULE_4_LOG_FILTER_NO_MATCH_DEFAULT_TEST_STRING));
    ErrLog(INFO, ErrMsg(MODULE_4_INFO_FILTER_NO_MATCH_DEFAULT_TEST_STRING));
    ErrLog(NOTICE, ErrMsg(MODULE_4_NOTICE_FILTER_NO_MATCH_DEFAULT_TEST_STRING));
    ErrLog(WARNING, ErrMsg(MODULE_4_WARNING_FILTER_MATCH_DEFAULT_TEST_STRING));
    ErrLog(ERROR, ErrMsg(MODULE_4_ERROR_FILTER_MATCH_DEFAULT_TEST_STRING));
    FlushLogger();
    GetErrorLogFileFullPath(g_errorLogFileFullPath, MAX_PATH, GetErrLogDirectory, ERR_LOG_EXTEND, UT_NAME);

    /* Check result */
    bool result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath, MODULE_4_DEBUG_FILTER_NO_MATCH_DEFAULT_TEST_STRING, false);
    ASSERT_FALSE(result);
    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath, MODULE_4_LOG_FILTER_NO_MATCH_DEFAULT_TEST_STRING, false);
    ASSERT_FALSE(result);
    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath, MODULE_4_INFO_FILTER_NO_MATCH_DEFAULT_TEST_STRING, false);
    ASSERT_FALSE(result);
    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath, MODULE_4_NOTICE_FILTER_NO_MATCH_DEFAULT_TEST_STRING, false);
    ASSERT_FALSE(result);
    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath, MODULE_4_WARNING_FILTER_MATCH_DEFAULT_TEST_STRING, false);
    ASSERT_TRUE(result);
    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath, MODULE_4_ERROR_FILTER_MATCH_DEFAULT_TEST_STRING, false);
    ASSERT_TRUE(result);

    ResetErrLogModuleContextFilter();
    /* Exit Logger */
    usleep(100);
    CloseLogger();
    StopLogger();
}

TEST_F(ErrorLogTest, DISABLED_ErrLogComponentFilterTest)
{
    /* Clear existing log file */
    StartLogger();
    char logDirectory[MAX_PATH] = {0};
    GetErrLogDirectory(logDirectory, MAX_PATH);
    usleep(100);
    StopLogger();
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, logDirectory);

    /* Start logger */
    StartLogger();
    OpenLogger();

    /* Set log prefix */
    uint32_t linePrefixSuffix = GetErrLogLinePrefixSuffix();
    linePrefixSuffix = linePrefixSuffix | LOG_LINE_PREFIX_COMPILATION_CONTEXT_COMPONENT_NAME |
                       LOG_LINE_PREFIX_COMPILATION_CONTEXT_MODULE_NAME;
    SetErrLogLinePrefixSuffix(linePrefixSuffix);
    SetErrLogServerLevel(WARNING);

    /* Set module filter condition */
    int componentIdFilter = COMPONENT_ID;
#undef COMPILE_CONTEXT_FILTER_COUNT
#define COMPILE_CONTEXT_FILTER_COUNT 1
    int errorLevelFilter[COMPILE_CONTEXT_FILTER_COUNT] = {WARNING};
    ResetErrLogModuleContextFilter();
    SetErrLogModuleContextFilter(componentIdFilter, NULL, (const int *) errorLevelFilter,
                                 COMPILE_CONTEXT_FILTER_COUNT);

    /* Print error log */
    /* Module 1 errlog */
#undef LOCAL_MODULE_NAME
#define LOCAL_MODULE_NAME         "module1"
#undef LOCAL_MODULE_ID
#define LOCAL_MODULE_ID           MODULE_ID_1
    ErrLog(INFO, ErrMsg(MODULE_1_INFO_FILTER_MATCH_TEST_STRING));
    ErrLog(WARNING, ErrMsg(MODULE_1_WARNING_FILTER_MATCH_TEST_STRING));
    /* Module 2 errlog */
#undef LOCAL_MODULE_NAME
#define LOCAL_MODULE_NAME         "module2"
#undef LOCAL_MODULE_ID
#define LOCAL_MODULE_ID           MODULE_ID_2
    ErrLog(NOTICE, ErrMsg(MODULE_2_NOTICE_FILTER_NO_MATCH_TEST_STRING));
    ErrLog(WARNING, ErrMsg(MODULE_2_WARNING_FILTER_MATCH_TEST_STRING));
    /* Module 3 errlog */
#undef LOCAL_MODULE_NAME
#define LOCAL_MODULE_NAME         "module3"
#undef LOCAL_MODULE_ID
#define LOCAL_MODULE_ID           MODULE_ID_3
    ErrLog(DEBUG, ErrMsg(MODULE_3_DEBUG_FILTER_NO_MATCH_TEST_STRING));
    ErrLog(ERROR, ErrMsg(MODULE_3_ERROR_FILTER_MATCH_TEST_STRING));
    FlushLogger();
    GetErrorLogFileFullPath(g_errorLogFileFullPath, MAX_PATH, GetErrLogDirectory, ERR_LOG_EXTEND, UT_NAME);
    /* Check result */
    bool result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath, MODULE_1_INFO_FILTER_MATCH_TEST_STRING, false);
    ASSERT_FALSE(result);
    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath, MODULE_1_WARNING_FILTER_MATCH_TEST_STRING, false);
    ASSERT_TRUE(result);
    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath, MODULE_2_NOTICE_FILTER_NO_MATCH_TEST_STRING, false);
    ASSERT_FALSE(result);
    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath, MODULE_2_WARNING_FILTER_MATCH_TEST_STRING, false);
    ASSERT_TRUE(result);
    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath, MODULE_3_DEBUG_FILTER_NO_MATCH_TEST_STRING, false);
    ASSERT_FALSE(result);
    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath, MODULE_3_ERROR_FILTER_MATCH_TEST_STRING, false);
    ASSERT_TRUE(result);

    ResetErrLogModuleContextFilter();
    /* Exit Logger */
    usleep(100);
    CloseLogger();
    StopLogger();
}

void PrintFilterErrorLogFunc(int logLevel)
{
    OpenLogger();
    ErrLog(logLevel, ErrMsg("This is %d log", logLevel));
    CloseLogger();
}

TEST_F(ErrorLogTest, ErrLogComponentFilterMultiThreadsTest)
{
    /* Clear existing log file */
    StartLogger();
    char logDirectory[MAX_PATH] = {0};
    GetErrLogDirectory(logDirectory, MAX_PATH);
    usleep(100);
    StopLogger();
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, logDirectory);
    SetErrLogServerLevel(DEBUG);

    int componentIdFilter = COMPONENT_ID;
    int errorLevelFilter[] = {WARNING};

    int testConditions[] = {ERROR, LOG, DEBUG};

    for (auto &testCondition : testConditions) {
        /* Start logger */
        StartLogger();
        OpenLogger();
        SetErrLogModuleContextFilter(componentIdFilter, NULL, (const int *) errorLevelFilter,
                                     COMPILE_CONTEXT_FILTER_COUNT);
        std::thread thread = std::thread(PrintFilterErrorLogFunc, testCondition);
        thread.join();
        /* Exit Logger */
        usleep(100);
        GetErrorLogFileFullPath(g_errorLogFileFullPath, MAX_PATH, GetErrLogDirectory, ERR_LOG_EXTEND, UT_NAME);
        CloseLogger();
        StopLogger();
    }
    
    /* Check result */
    bool result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath, "This is 6 log", false);
    ASSERT_TRUE(result);
    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath, "This is 2 log", false);
    ASSERT_FALSE(result);
    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath, "This is 1 log", false);
    ASSERT_FALSE(result);

    RemoveErrorLogFileFromLocalDirectory(g_localVfs, logDirectory);
}

/**
 * @tc.name:  ErrLogPrefixControlFunction001_Level0
 * @tc.desc:  Test the error log output prefix control function.
 * @tc.type: FUNC
 */
TEST_F(ErrorLogTest, DISABLED_ErrLogPrefixControlFunction001_Level0)
{
    /**
    * @tc.steps: step1. Remove all the error log file.
     * GetErrLogDirectory depends on the logger thread startup.
     * Therefore, the logger thread must be started first and then stop.
    * @tc.expected: step1.Remove all log files are correct.
    */
    SetErrLogWriteMode(false);
    ErrorCode errorCode = StartLogger();
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    bool started = IsLoggerStarted();
    ASSERT_TRUE(started);
    char logDirectory[MAX_PATH] = {0};
    GetErrLogDirectory(logDirectory, MAX_PATH);
    usleep(100);
    StopLogger();
    started = IsLoggerStarted();
    ASSERT_FALSE(started);

    RemoveErrorLogFileFromLocalDirectory(g_localVfs, logDirectory);

    /**
    * @tc.steps: step2. Test start logger thread and open logger.
    * @tc.expected: step2.The logger thread and logger open functions are correct.
    */
    errorCode = StartLogger();
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    started = IsLoggerStarted();
    ASSERT_TRUE(started);

    errorCode = OpenLogger();
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    bool opened = IS_ERR_LOG_OPEN();
    ASSERT_TRUE(opened);

    bool result = false;
    SetErrLogLinePrefixSuffix(0);

#define OUTPUT_PREFIX_TRANSACTION_ID    9999
#define OUTPUT_PREFIX_TRANSACTION_ID_STRING    "9999"
#define OUTPUT_PREFIX_QUERY_STRING   "querey string"
#define OUTPUT_PREFIX_CONTEXT   "callback context"
#define OUTPUT_PREFIX_BACKTRACE   "Backtrace"
#define OUTPUT_PREFIX_TEST_STRING   "Hello world, Prefix Message"
    SetErrLogServerLevel(ERROR);

    ErrLog(ERROR, ErrMsg(OUTPUT_PREFIX_TEST_STRING));
    FlushLogger();
    errorCode = GetErrorLogFileFullPath(g_errorLogFileFullPath, MAX_PATH, GetErrLogDirectory, ERR_LOG_EXTEND, UT_NAME);
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath, OUTPUT_PREFIX_TEST_STRING, false);
    ASSERT_TRUE(result);

    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath, OUTPUT_PREFIX_BACKTRACE, false);
    ASSERT_FALSE(result);

    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath, OUTPUT_PREFIX_CONTEXT, false);
    ASSERT_FALSE(result);

    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath, OUTPUT_PREFIX_QUERY_STRING, false);
    ASSERT_FALSE(result);

    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath, OUTPUT_PREFIX_TRANSACTION_ID_STRING, false);
    ASSERT_FALSE(result);

    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath, PROCESS_INTERRUPT_STRING, false);
    ASSERT_FALSE(result);

    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath, SEND_MESSAGE_TO_FRONTEND, false);
    ASSERT_FALSE(result);

    uint32_t linePrefixSuffix = GetErrLogLinePrefixSuffix();
    linePrefixSuffix = linePrefixSuffix | LOG_LINE_PREFIX_TIMESTAMP |
                       LOG_LINE_PREFIX_TRANSACTION_ID | LOG_LINE_PREFIX_QUERY_STRING |
                       LOG_LINE_SUFFIX_CONTEXT | LOG_LINE_SUFFIX_BACKTRACE;
    SetErrLogLinePrefixSuffix(linePrefixSuffix);
    ClearErrLogQueryStringInfo();
    SetErrLogQueryStringInfo(OUTPUT_PREFIX_QUERY_STRING);
    ClearErrLogTransactionIDInfo();
    SetErrLogTransactionIDInfo(OUTPUT_PREFIX_TRANSACTION_ID);
    SetErrLogBacktrace(ENABLE_ERROR_LOG_BACKTRACE);
    g_interruptPending = true;
    RegisterErrLogProcessInterruptsCallBack(ProcessInterruptsCallBack);
    g_sendToFrontend = true;
    SetSendToClientFlag(ENABLE_ERROR_LOG_SEND_CLIENT_ALL);
    RegisterErrLogSendToFrontendCallBack(ErrLogSendToFrontendCallBack);

    const char *contextString = "push context callback";
    PushErrorLogContextCallback(ErrorLogContextCallback, (void *) contextString);
    ErrLog(ERROR, ErrMsg(COMPILE_CONTEXT_INFO_FILTER_COMPONENT_MATCH_TEST_STRING));

    PopErrorLogContextCallback();
    SetErrLogBacktrace(DISABLE_ERROR_LOG_BACKTRACE);

    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath, OUTPUT_PREFIX_BACKTRACE, false);
    ASSERT_TRUE(result);

    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath, (char *) contextString, false);
    ASSERT_TRUE(result);

    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath, OUTPUT_PREFIX_QUERY_STRING, false);
    ASSERT_TRUE(result);
    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath, OUTPUT_PREFIX_TRANSACTION_ID_STRING, false);
    ASSERT_TRUE(result);

    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath, PROCESS_INTERRUPT_STRING, false);
    ASSERT_TRUE(result);

    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath, SEND_MESSAGE_TO_FRONTEND, false);
    ASSERT_TRUE(result);

    /**
    * @tc.steps: step8. Test close logger and  stop logger thread.
    * @tc.expected: step8.The logger thread functions are correct.
    */
    CloseLogger();
    opened = IS_ERR_LOG_OPEN();
    ASSERT_FALSE(opened);

    usleep(100);
    StopLogger();
    started = IsLoggerStarted();
    ASSERT_FALSE(started);
}

static uint32_t CountDirectoryFileLine(const char *dirName)
{
    uint32_t count = 0;
    Directory dir;
    ErrorCode errorCode = OpenDirectory(dirName, &dir);
    if (errorCode != ERROR_SYS_OK) {
        ErrLog(ERROR, ErrMsg("fail to open directory, error code %lld.\n", errorCode));
        return count;
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
        char temp[1024] = {0};
        std::sprintf(temp, "%s/%s", dirName, baseFileName);
        count += GetLogFileLineNum(temp);
    }
    CloseDirectory(&dir);
    return count;
}

#define THREAD_NUMBERS   4
/**
 * @tc.name:  ErrLogConcurrentFunction001_Level0
 * @tc.desc:  Test the error log concurrent output function.
 * @tc.type: FUNC
 */
TEST_F(ErrorLogTest, ErrLogConcurrentFunction001_Level0)
{
    /**
    * @tc.steps: step1. Remove all the error log file.
     * GetErrLogDirectory depends on the logger thread startup.
     * Therefore, the logger thread must be started first and then stop.
    * @tc.expected: step1.Remove all log files are correct.
    */
    ErrorCode errorCode = StartLogger();
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    bool started = IsLoggerStarted();
    ASSERT_TRUE(started);
    char logDirectory[MAX_PATH] = {0};
    GetErrLogDirectory(logDirectory, MAX_PATH);
    usleep(100);
    StopLogger();
    started = IsLoggerStarted();
    ASSERT_FALSE(started);

    RemoveErrorLogFileFromLocalDirectory(g_localVfs, logDirectory);

    /**
    * @tc.steps: step2. Test start logger thread and open logger.
    * @tc.expected: step2.The logger thread and logger open functions are correct.
    */
    errorCode = StartLogger();
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    started = IsLoggerStarted();
    ASSERT_TRUE(started);

    errorCode = OpenLogger();
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    bool opened = IS_ERR_LOG_OPEN();
    ASSERT_TRUE(opened);

#define ERROR_LOG_CONCURRENT_RECORD_COUNT  10
    /**
    * @tc.steps: step3. Test concurrent thread call error log.
    * @tc.expected: step3.The concurrent error log function are correct.
    */
    Tid tid[THREAD_NUMBERS];
    int i;
    int integerVec[THREAD_NUMBERS];
    for (i = 0; i < THREAD_NUMBERS; i++) {
        integerVec[i] = ERROR_LOG_CONCURRENT_RECORD_COUNT;
        errorCode = ThreadCreate(&tid[i], ThreadRoutineConcurrentErrLog, &integerVec[i]);
        EXPECT_EQ(errorCode, ERROR_SYS_OK);
    }
    int *valuePtr;
    for (i = 0; i < THREAD_NUMBERS; i++) {
        errorCode = ThreadJoin(tid[i], (void **) &valuePtr);
        EXPECT_EQ(errorCode, ERROR_SYS_OK);
        ASSERT_NE(*valuePtr, -1);
    }

    /**
    * @tc.steps: step4. Test close logger and  stop logger thread.
    * @tc.expected: step4.The logger thread functions are correct.
    */
    CloseLogger();
    opened = IS_ERR_LOG_OPEN();
    ASSERT_FALSE(opened);

    sleep(1);
    StopLogger();
    started = IsLoggerStarted();
    ASSERT_FALSE(started);
}

/**
 * @tc.name:  ErrLogFileRecycleFunction001_Level2
 * @tc.desc:  Test the error log file recycle function.
 * @tc.type: FUNC
 */
TEST_F(ErrorLogTest, DISABLED_ErrLogFileRecycleFunction001_Level2)
{
    /**
    * @tc.steps: step1. Remove all the error log file.
     * GetErrLogDirectory depends on the logger thread startup.
     * Therefore, the logger thread must be started first and then stop.
    * @tc.expected: step1.Remove all log files are correct.
    */
    ErrorCode errorCode = StartLogger();
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    bool started = IsLoggerStarted();
    ASSERT_TRUE(started);
    char logDirectory[MAX_PATH] = {0};
    GetErrLogDirectory(logDirectory, MAX_PATH);
    usleep(100);
    StopLogger();
    started = IsLoggerStarted();
    ASSERT_FALSE(started);

    RemoveErrorLogFileFromLocalDirectory(g_localVfs, logDirectory);

    /**
    * @tc.steps: step2. Test start logger thread and open logger.
    * @tc.expected: step2.The logger thread and logger open functions are correct.
    */
    errorCode = StartLogger();
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    started = IsLoggerStarted();
    ASSERT_TRUE(started);

    errorCode = OpenLogger();
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    bool opened = IS_ERR_LOG_OPEN();
    ASSERT_TRUE(opened);
    /*
     * The maximum value of THREAD_MESSAGE_MAX_NUMBER is 1000000.
     * There are 4 threads in total. Each thread cannot exceed 25000.
     * ERROR_LOG_CONCURRENT_RECORD_COUNT max value is 25000.
     */
#undef ERROR_LOG_CONCURRENT_RECORD_COUNT
#define ERROR_LOG_CONCURRENT_RECORD_COUNT  240000
    /**
    * @tc.steps: step3. Test the error log file recycle.
    * @tc.expected: step3.The error log file recycle function are correct.
    */
    Tid tid[THREAD_NUMBERS];
    int i;
    int integerVec[THREAD_NUMBERS];
    for (i = 0; i < THREAD_NUMBERS; i++) {
        integerVec[i] = ERROR_LOG_CONCURRENT_RECORD_COUNT;
        errorCode = ThreadCreate(&tid[i], ThreadRoutineConcurrentErrLog, &integerVec[i]);
        EXPECT_EQ(errorCode, ERROR_SYS_OK);
    }
    int *valuePtr;
    for (i = 0; i < THREAD_NUMBERS; i++) {
        errorCode = ThreadJoin(tid[i], (void **) &valuePtr);
        EXPECT_EQ(errorCode, ERROR_SYS_OK);
        ASSERT_NE(*valuePtr, -1);
    }
    char errorLogFilePath[MAX_PATH];
    int count = 0;
    count = GetFileInfoFromLocalDirectory(logDirectory, errorLogFilePath, MAX_PATH, ERR_LOG_EXTEND, UT_NAME);
    uint32_t maxSequenceNo = GetErrLogFileMaxSequenceNo();
    EXPECT_EQ(count, maxSequenceNo);

    /**
    * @tc.steps: step4. Test close logger and  stop logger thread.
    * @tc.expected: step4.The logger thread functions are correct.
    */
    CloseLogger();
    opened = IS_ERR_LOG_OPEN();
    ASSERT_FALSE(opened);

    usleep(100);
    StopLogger();
    started = IsLoggerStarted();
    ASSERT_FALSE(started);
}

/**
 * @tc.name:  ErrLogTextDomainFunction001_Level0
 * @tc.desc:  Test the error log text domain function.
 * @tc.type: FUNC
 */
TEST_F(ErrorLogTest, ErrLogTextDomainFunction001_Level0)
{
    /**
    * @tc.steps: step1. Start logger thread.
    * @tc.expected: step1.Start logger thread succeed.
    */
    ErrorCode errorCode = StartLogger();
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    bool started = IsLoggerStarted();
    ASSERT_TRUE(started);

    /**
    * @tc.steps: step2. Domain text test.
    * @tc.expected: step2.Domain text function is correct.
    */
#undef  LOCAL_COMPONENT_NAME
#define LOCAL_COMPONENT_NAME      "component name"
#undef  LOCAL_MODULE_NAME
#define LOCAL_MODULE_NAME         "module name"
#define DOMAIN_NAME               "domain_name"
#define DOMAIN_TEXT_TEST_STRING   "This is test string"
    errorCode = BindModuleTextDomain(LOCAL_COMPONENT_NAME, LOCAL_MODULE_NAME,
                                     DOMAIN_NAME, ERROR_LOG_DEFAULT_DIRECTORY);
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    const char *domainName = FindModuleTextDomain(LOCAL_COMPONENT_NAME, LOCAL_MODULE_NAME);
    EXPECT_STREQ(domainName, DOMAIN_NAME);
    const char *testString = DomainGetText(domainName, DOMAIN_TEXT_TEST_STRING);
    EXPECT_STREQ(testString, DOMAIN_TEXT_TEST_STRING);

    /**
    * @tc.steps: step3. Stop logger thread.
    * @tc.expected: step3.Stop logger thread succeed.
    */
    usleep(100);
    StopLogger();
    started = IsLoggerStarted();
    ASSERT_FALSE(started);
}


/**
 * @tc.name:  ErrLogFileCheckFunction001_Level0
 * @tc.desc:  Test the error log file check function.
 * @tc.type: FUNC
 */
TEST_F(ErrorLogTest, ErrLogFileCheckFunction001_Level0)
{
    /**
    * @tc.steps: step1. Start the logger thread and get the log directory.
    * @tc.expected: step1.Get the logger directory are correct.
    */
    ErrorCode errorCode = StartLogger();
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    bool started = IsLoggerStarted();
    ASSERT_TRUE(started);
    char logDirectory[MAX_PATH] = {0};
    GetErrLogDirectory(logDirectory, MAX_PATH);

    /**
    * @tc.steps: step2. Create and verify a test file.
    * @tc.expected: step2.The check file are correct.
    */
    int fileId = 0;
    bool result;
    FileParameter fileParameter;
    (void)strcpy_s(fileParameter.storeSpaceName, sizeof(fileParameter.storeSpaceName), "storeSpaceName1");
    fileParameter.streamId = VFS_DEFAULT_FILE_STREAM_ID;
    fileParameter.flag = APPEND_WRITE_FILE;
    fileParameter.fileSubType = ERR_LOG_FILE_TYPE;
    fileParameter.rangeSize = DEFAULT_RANGE_SIZE;
    fileParameter.maxSize = MAX_LOG_FILE_SIZE;
    fileParameter.recycleTtl = 0;
    fileParameter.mode = FILE_READ_AND_WRITE_MODE;
    fileParameter.isReplayWrite = false;
    FileDescriptor *fd = NULL;
    char fileFullPath[MAX_PATH] = {0};
    errno_t rc;
    rc = memset_s(fileFullPath, MAX_PATH, 0, MAX_PATH);
    SecurecCheck(rc, true);
    rc = sprintf_s(fileFullPath, MAX_PATH, "%s/%s", logDirectory, "logtest.log");
    SecurecCheck(rc, true);
    errorCode = FileIsExist(g_localVfs, fileFullPath, &result);
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    if (!result) {
        errorCode = Create(g_localVfs, fileFullPath, fileParameter, &fd);
        EXPECT_EQ(errorCode, ERROR_SYS_OK);
        errorCode = Close(fd);
        EXPECT_EQ(errorCode, ERROR_SYS_OK);
        errorCode = FileIsExist(g_localVfs, fileFullPath, &result);
        EXPECT_EQ(errorCode, ERROR_SYS_OK);
        ASSERT_TRUE(result);
    }
    LocalLogFileVerification();
    errorCode = FileIsExist(g_localVfs, fileFullPath, &result);
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    ASSERT_FALSE(result);

    /**
    * @tc.steps: step3. Stop logger thread.
    * @tc.expected: step3.Stop logger thread succeed.
    */
    usleep(100);
    StopLogger();
    started = IsLoggerStarted();
    ASSERT_FALSE(started);
}

/**
 * @tc.name:  ErrLogSyslogFunction001_Level0
 * @tc.desc:  Test the error log sys report function.
 * @tc.type: FUNC
 */
TEST_F(ErrorLogTest, ErrLogSyslogFunction001_Level0)
{
    /**
    * @tc.steps: step1. Start logger thread.
    * @tc.expected: step1.Start logger thread succeed.
    */
    ErrorCode errorCode = StartLogger();
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    bool started = IsLoggerStarted();
    ASSERT_TRUE(started);

    errorCode = OpenLogger();
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    bool opened = IS_ERR_LOG_OPEN();
    ASSERT_TRUE(opened);

    /**
    * @tc.steps: step2. Set the logger destination to syslog and call ErrLog.
    * @tc.expected: step2.The ErrLog succeed.
    */
#define OUTPUT_TO_SYSLOG_TEST_STRING  "This is test string to syslog"
    SetErrLogDestination(LOG_DESTINATION_LOCAL_SYSLOG);
    ErrLog(ERROR, ErrMsg(OUTPUT_TO_SYSLOG_TEST_STRING));
#define SYSLOG_OUTPUT_LARGE_MESSAGE_LEN  2048
    char syslogMessage[SYSLOG_OUTPUT_LARGE_MESSAGE_LEN];
    errno_t rc;
    rc = memset_s(syslogMessage, SYSLOG_OUTPUT_LARGE_MESSAGE_LEN, 0, SYSLOG_OUTPUT_LARGE_MESSAGE_LEN);
    SecurecCheck(rc, true);
    int i;
    for (i = 0; i < SYSLOG_OUTPUT_LARGE_MESSAGE_LEN; i++) {
        syslogMessage[i] = 'a';
    }
    syslogMessage[SYSLOG_OUTPUT_LARGE_MESSAGE_LEN - 1] = '\0';
    ErrLog(ERROR, ErrMsg("%s", syslogMessage));

    /**
    * @tc.steps: step3. Stop logger thread.
    * @tc.expected: step3.Stop logger thread succeed.
    */
    CloseLogger();
    opened = IS_ERR_LOG_OPEN();
    ASSERT_FALSE(opened);
    usleep(100);
    StopLogger();
    started = IsLoggerStarted();
    ASSERT_FALSE(started);
    ResetErrorLogConfigure();
}

/**
 * @tc.name:  ErrLogStderrFunction001_Level0
 * @tc.desc:  Test the error log stderr function.
 * @tc.type: FUNC
 */
TEST_F(ErrorLogTest, ErrLogStderrFunction001_Level0)
{
    /**
    * @tc.steps: step1. Start logger thread.
    * @tc.expected: step1.Start logger thread succeed.
    */
    ErrorCode errorCode = StartLogger();
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    bool started = IsLoggerStarted();
    ASSERT_TRUE(started);

    errorCode = OpenLogger();
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    bool opened = IS_ERR_LOG_OPEN();
    ASSERT_TRUE(opened);

    /**
    * @tc.steps: step2. Set the logger destination to stderr and call ErrLog.
    * @tc.expected: step2.The ErrLog succeed.
    */
#define OUTPUT_TO_STDERR_TEST_STRING  "This is test string to stderr"
    SetErrLogDestination(LOG_DESTINATION_LOCAL_STDERR);
    ErrLog(ERROR, ErrMsg(OUTPUT_TO_STDERR_TEST_STRING));

    /**
    * @tc.steps: step3. Stop logger thread.
    * @tc.expected: step3.Stop logger thread succeed.
    */
    CloseLogger();
    opened = IS_ERR_LOG_OPEN();
    ASSERT_FALSE(opened);
    usleep(100);
    StopLogger();
    started = IsLoggerStarted();
    ASSERT_FALSE(started);
    ResetErrorLogConfigure();
}

#define ERROR_LOG_COMPONENT_NAME "ComponentName"

TEST_F(ErrorLogTest, SetLogLogLinePrefixSuffix001_Level0)
{
    SetErrLogWriteMode(false);
    uint32_t errLogSuffix = GetErrLogLinePrefixSuffix();
    errLogSuffix &= ~LOG_LINE_PREFIX_RUNNING_CONTEXT_THREAD_NAME;
    errLogSuffix &= ~LOG_LINE_PREFIX_COMPILATION_CONTEXT_COMPONENT_NAME;
    errLogSuffix &= ~LOG_LINE_PREFIX_COMPILATION_CONTEXT_MODULE_NAME;
    SetErrLogLinePrefixSuffix(errLogSuffix);

    const char *testLogDirectory = ".";
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, testLogDirectory);
    SetErrLogDirectory(testLogDirectory);
    EXPECT_EQ(StartLogger(), ERROR_SYS_OK);
    ASSERT_TRUE(IsLoggerStarted());
    EXPECT_EQ(OpenLogger(), ERROR_SYS_OK);
    ASSERT_TRUE(IS_ERR_LOG_OPEN());

#define ERROR_LOG_MESSAGE "Hello World"
    ErrLog(ERROR, ErrMsg(ERROR_LOG_MESSAGE));
    bool result = false;
    FlushLogger();
    EXPECT_EQ(GetErrorLogFileFullPath(g_errorLogFileFullPath, MAX_PATH, GetErrLogDirectory, ERR_LOG_EXTEND, UT_NAME), ERROR_SYS_OK);
    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath, ERROR_LOG_MESSAGE, false);
    ASSERT_TRUE(result);

    /* Error log will not print component name */
    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath, ERROR_LOG_COMPONENT_NAME, false);
    ASSERT_FALSE(result);

    CloseLogger();
    ASSERT_FALSE(IS_ERR_LOG_OPEN());
    usleep(100);
    StopLogger();
    ASSERT_FALSE(IsLoggerStarted());
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, testLogDirectory);
    ResetErrLogDirectory();
    ResetErrLogLinePrefixSuffix();
}

TEST_F(ErrorLogTest, SetLogOutputDirectory001_Level0)
{
    SetErrLogWriteMode(false);
    const char *testLogDirectory = ".";
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, testLogDirectory);

    SetErrLogDirectory(testLogDirectory);
    EXPECT_EQ(StartLogger(), ERROR_SYS_OK);
    ASSERT_TRUE(IsLoggerStarted());
    EXPECT_EQ(OpenLogger(), ERROR_SYS_OK);
    ASSERT_TRUE(IS_ERR_LOG_OPEN());

#define TEST_LOG_MESSAGE "Specify log directory"
    ErrLog(ERROR, ErrMsg(TEST_LOG_MESSAGE));
    FlushLogger();
    EXPECT_EQ(GetErrorLogFileFullPath(g_errorLogFileFullPath, MAX_PATH, GetErrLogDirectory, ERR_LOG_EXTEND, UT_NAME), ERROR_SYS_OK);
    bool result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath, TEST_LOG_MESSAGE, false);
    ASSERT_TRUE(result);

    /* Error log will print severity level default */
    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath, ErrorLevel2String(ERROR), false);
    ASSERT_TRUE(result);

    CloseLogger();
    ASSERT_FALSE(IS_ERR_LOG_OPEN());
    usleep(100);
    StopLogger();
    ASSERT_FALSE(IsLoggerStarted());
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, testLogDirectory);
    ResetErrLogDirectory();
}

void PrintErrorLogInLoop(uint32_t loop, long sleepEachLoop, int logLevel)
{
    for (uint32_t i = 0; i < loop; ++i) {
        ErrLog(logLevel, ErrMsg("Loop %d log", logLevel));
        Usleep(sleepEachLoop);
    }
}

void PrintErrorLogFunc(uint32_t loop, long sleepEachLoop, int logLevel)
{
    ASSERT_EQ(OpenLogger(), 0);
    PrintErrorLogInLoop(loop, sleepEachLoop, logLevel);
    CloseLogger();
}

struct TestFoldCondition {
    int logLevel;
    bool foldRes;
};

TEST_F(ErrorLogTest, ErrorLogFoldTest001_Level0)
{
    constexpr uint32_t logCount = 20;
    constexpr uint32_t logFoldThreshold = 10;
    ASSERT_TRUE(SetErrLogFoldConfig(10, logFoldThreshold, WARNING));
    SetErrLogDirectory(TEST_CURRENT_DIRECTORY);
    SetErrLogServerLevel(DEBUG);

    TestFoldCondition testConditions[] = {
        {ERROR, false},
        {LOG, false}, /* LOG level will never be folded */
        {DEBUG, true},
    };

    for (auto &testCondition : testConditions) {
        RemoveErrorLogFileFromLocalDirectory(g_localVfs, TEST_CURRENT_DIRECTORY);
        EXPECT_EQ(StartLogger(), ERROR_SYS_OK);
        ASSERT_TRUE(IsLoggerStarted());
        EXPECT_EQ(OpenLogger(), ERROR_SYS_OK);
        ASSERT_TRUE(IS_ERR_LOG_OPEN());

        std::thread thread = std::thread(PrintErrorLogFunc, logCount, 10 * 1000, testCondition.logLevel);
        thread.join();

        CloseLogger();
        ASSERT_FALSE(IS_ERR_LOG_OPEN());
        usleep(100);
        EXPECT_EQ(GetErrorLogFileFullPath(g_errorLogFileFullPath, MAX_PATH, GetErrLogDirectory, ERR_LOG_EXTEND, UT_NAME), ERROR_SYS_OK);
        StopLogger();
        ASSERT_FALSE(IsLoggerStarted());

        char targetFoldMsg[100];
        sprintf_s(targetFoldMsg, sizeof(targetFoldMsg), "[Fold %lu times]", logCount - logFoldThreshold);
        bool result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath, targetFoldMsg, false);
        ASSERT_EQ(result, testCondition.foldRes);
    }

    RemoveErrorLogFileFromLocalDirectory(g_localVfs, TEST_CURRENT_DIRECTORY);
    ResetErrLogFoldConfig();
    ResetErrLogDirectory();
}

enum FoldLogTestThreadType {
    FOLD_LOG_NORMAL_SPEED, /* Steady normal speed to print log bug not cause fold log */
    FOLD_LOG_HIGH_SPEED, /* Steady high speed to print log and cause fold log */
    FOLD_LOG_RANDOM_SPEED, /* Non-steady speed to print log */
};

void PrintErrorLogWithTypeFunc(FoldLogTestThreadType type, long totalTimeSecond)
{
    ASSERT_EQ(OpenLogger(), 0);
    long totalTimeUs = totalTimeSecond * 1000 * 1000;
    switch (type) {
        case FOLD_LOG_HIGH_SPEED: {
            ErrLog(DEBUG, ErrMsg("High speed fold log test thread"));
            /* Print 1 log / ms */
            while (totalTimeUs > 0) {
                ErrLog(DEBUG, ErrMsg("High speed log"));
                Usleep(1000);
                totalTimeUs -= 1000;
            }
            break;
        }
        case FOLD_LOG_RANDOM_SPEED: {
            ErrLog(INFO, ErrMsg("Random speed fold log test thread"));
            srand(0);
            /* Print log gap range [1ms, 250ms] */
            while (totalTimeUs > 0) {
                ErrLog(INFO, ErrMsg("Random speed log"));
                long sleepTime = (rand() % 250 + 1) * 1000;
                Usleep(sleepTime);
                totalTimeUs -= sleepTime;
            }
            break;
        }
        case FOLD_LOG_NORMAL_SPEED:
        default: {
            ErrLog(LOG, ErrMsg("Normal speed fold log test thread"));
            /* Print 1 log / s */
            while (totalTimeUs > 0) {
                ErrLog(LOG, ErrMsg("Normal speed log"));
                Usleep(1000 * 1000);
                totalTimeUs -= 1000 * 1000;
            }
            break;
        }
    }

    CloseLogger();
}

static bool CheckErrLogFilePermission(char *filepath, int result)
{
    unsigned int mask = 0777;
	struct stat buff;

    if (stat(filepath, &buff) != -1) {
        return ((buff.st_mode&mask) == result);
    } else {
        printf("stat %s failed\n", filepath);
        exit(0);
    }
}

static mode_t ComputeActualUmask(mode_t expectUmask)
{
    mode_t sysUmask = umask(expectUmask);

    /* revert umask */
    (void)umask(sysUmask);

    mode_t actualUmask = expectUmask & (~sysUmask);
    return actualUmask;
}

TEST_F(ErrorLogTest, ErrorLogFoldTest002_Level2)
{
    ASSERT_TRUE(SetErrLogFoldConfig(2, 20, WARNING));
    SetErrLogDirectory(TEST_CURRENT_DIRECTORY);
    SetErrLogServerLevel(DEBUG);
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, TEST_CURRENT_DIRECTORY);

    EXPECT_EQ(StartLogger(), ERROR_SYS_OK);
    ASSERT_TRUE(IsLoggerStarted());
    EXPECT_EQ(OpenLogger(), ERROR_SYS_OK);
    ASSERT_TRUE(IS_ERR_LOG_OPEN());

#define FOLD_LOG_TEST_THREAD_COUNT  3
    constexpr long totalTestTime = 10;
    FoldLogTestThreadType typeArray[] = {FOLD_LOG_NORMAL_SPEED, FOLD_LOG_HIGH_SPEED, FOLD_LOG_RANDOM_SPEED};
    std::thread threads[FOLD_LOG_TEST_THREAD_COUNT];
    for (int i = 0; i < FOLD_LOG_TEST_THREAD_COUNT; ++i) {
        threads[i] = std::thread(PrintErrorLogWithTypeFunc, typeArray[i], totalTestTime);
    }
    for (auto &thread : threads) {
        thread.join();
    }

    CloseLogger();
    ASSERT_FALSE(IS_ERR_LOG_OPEN());
    usleep(100);
    StopLogger();
    ASSERT_FALSE(IsLoggerStarted());

    RemoveErrorLogFileFromLocalDirectory(g_localVfs, TEST_CURRENT_DIRECTORY);
    ResetErrLogFoldConfig();
    ResetErrLogDirectory();
}

TEST_F(ErrorLogTest, ErrorLogFoldTest003_Level0)
{
    SetErrLogDirectory(TEST_CURRENT_DIRECTORY);
    SetErrLogServerLevel(DEBUG);
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, TEST_CURRENT_DIRECTORY);

    EXPECT_EQ(StartLogger(), ERROR_SYS_OK);
    ASSERT_TRUE(IsLoggerStarted());
    EXPECT_EQ(OpenLogger(), ERROR_SYS_OK);
    ASSERT_TRUE(IS_ERR_LOG_OPEN());

    /* Fold log disabled */
    ASSERT_TRUE(SetErrLogFoldConfig(0, 0, WARNING));
    PrintErrorLogInLoop(2, 0, WARNING);
    FlushLogger();
    EXPECT_EQ(GetErrorLogFileFullPath(g_errorLogFileFullPath, MAX_PATH, GetErrLogDirectory, ERR_LOG_EXTEND, UT_NAME), ERROR_SYS_OK);
    bool result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath, "[Fold 1 times]", false);
    ASSERT_FALSE(result);

    /* Fold log enabled */
    ASSERT_TRUE(SetErrLogFoldConfig(2, 1, WARNING));
    PrintErrorLogInLoop(2, 0, WARNING);

    CloseLogger();
    ASSERT_FALSE(IS_ERR_LOG_OPEN());
    usleep(100);
    StopLogger();
    ASSERT_FALSE(IsLoggerStarted());

    result = IsFileContainedString(g_localVfs, g_errorLogFileFullPath, "[Fold 1 times]", false);
    ASSERT_TRUE(result);

    RemoveErrorLogFileFromLocalDirectory(g_localVfs, TEST_CURRENT_DIRECTORY);
    ResetErrLogFoldConfig();
    ResetErrLogDirectory();
}

void *PrintErrorLogFuncNew(void *arg)
{
    int *level = (int *)arg;
    PrintErrorLogInLoop(5, 10 * 1000, *level);
    CloseLogger();
    return arg;
}

TEST_F(ErrorLogTest, ErrorLogFoldTest004_Level0)
{
    constexpr uint32_t logCount = 5;
    constexpr uint32_t logFoldThreshold = 3;
    ASSERT_TRUE(SetErrLogFoldConfig(10, logFoldThreshold, NOTICE));
    SetErrLogDirectory(TEST_CURRENT_DIRECTORY);
    SetErrLogServerLevel(DEBUG);
    EXPECT_EQ(StartLogger(), ERROR_SYS_OK);
    ASSERT_TRUE(IsLoggerStarted());
    usleep(100);
    EXPECT_EQ(OpenLogger(), ERROR_SYS_OK);
    ASSERT_TRUE(IS_ERR_LOG_OPEN());

    TestFoldCondition testConditions[] = {
        {ERROR, false},
        {DEBUG, true},
        {LOG, false}, /* LOG level will never be folded */
        {NOTICE, true},
        {WARNING, false},
    };

#undef THREAD_NUMBERS
#define THREAD_NUMBERS 5
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, TEST_CURRENT_DIRECTORY);
    Tid tid[THREAD_NUMBERS];
    int i;
    int integerVec[THREAD_NUMBERS];
    ErrorCode errorCode;
    for (i = 0; i < THREAD_NUMBERS; i++) {
        integerVec[i] = ERROR_LOG_CONCURRENT_RECORD_COUNT;
        errorCode = ThreadCreate(&tid[i], PrintErrorLogFuncNew, &testConditions[i].logLevel);
        EXPECT_EQ(errorCode, ERROR_SYS_OK);
    }
    int *valuePtr;
    for (i = 0; i < THREAD_NUMBERS; i++) {
        errorCode = ThreadJoin(tid[i], (void **) &valuePtr);
        EXPECT_EQ(errorCode, ERROR_SYS_OK);
        ASSERT_NE(*valuePtr, -1);
    }

    CloseLogger();
    ASSERT_FALSE(IS_ERR_LOG_OPEN());
    usleep(100);
    EXPECT_EQ(GetErrorLogFileFullPath(g_errorLogFileFullPath, MAX_PATH, GetErrLogDirectory, ERR_LOG_EXTEND, UT_NAME), ERROR_SYS_OK);
    StopLogger();
    ASSERT_FALSE(IsLoggerStarted());
    ResetErrLogFoldConfig();
    ResetErrLogDirectory();
}

/**
 * Test change log directory during logger thread running
 */
TEST_F(ErrorLogTest, SwitchLogDirectoryTest001_Level0)
{
    SetErrLogWriteMode(false);
    /* Prepare directory */
    const char *testDir1 = "./test1";
    const char *testDir2 = "./test2";
    char testLogFullName1[MAX_PATH] = {0};
    char testLogFullName2[MAX_PATH] = {0};
    if (CheckDirectoryExist(testDir1)) {
        RemoveErrorLogFileFromLocalDirectory(g_localVfs, testDir1);
        DestroyDirectory(testDir1);
    }
    if (CheckDirectoryExist(testDir2)) {
        RemoveErrorLogFileFromLocalDirectory(g_localVfs, testDir2);
        DestroyDirectory(testDir2);
    }

    EXPECT_EQ(StartLogger(), ERROR_SYS_OK);
    ASSERT_TRUE(IsLoggerStarted());
    EXPECT_EQ(OpenLogger(), ERROR_SYS_OK);
    ASSERT_TRUE(IS_ERR_LOG_OPEN());
    uint64_t currSeq = GetErrorLogLocalFileCfgSeq();

    /* Change to testDir1 and write 1 log */
    SetErrLogDirectory(testDir1);
    ASSERT_TRUE(WaitErrLogLocalFileCfgTerm(++currSeq));
    ErrLog(ERROR, ErrMsg("test1"));
    /* File create by logger thread, using a loop to get current error log file name */
    for (int i = 0; i < ERROR_LOG_MAX_WAIT_US / ERROR_LOG_WAIT_GAP_US; i++) {
        if (GetErrorLogFileInDirectory(testDir1, testLogFullName1, MAX_PATH)) {
            break;
        }
        Usleep(ERROR_LOG_WAIT_GAP_US);
    }
    ASSERT_GT(strlen(testLogFullName1), 0);

    /* Change to testDir2 and write 1 log */
    SetErrLogDirectory(testDir2);
    ASSERT_TRUE(WaitErrLogLocalFileCfgTerm(++currSeq));
    ErrLog(ERROR, ErrMsg("test2"));
    for (int i = 0; i < ERROR_LOG_MAX_WAIT_US / ERROR_LOG_WAIT_GAP_US; i++) {
        if (GetErrorLogFileInDirectory(testDir2, testLogFullName2, MAX_PATH)) {
            break;
        }
        Usleep(ERROR_LOG_WAIT_GAP_US);
    }
    ASSERT_GT(strlen(testLogFullName2), 0);

    usleep(1000);
    CloseLogger();
    StopLogger();

    /* Check error message has been writen to correct log file */
    EXPECT_TRUE(IsFileContainedString(g_localVfs, testLogFullName1, "test1", false));
    EXPECT_FALSE(IsFileContainedString(g_localVfs, testLogFullName1, "test2", false));
    EXPECT_FALSE(IsFileContainedString(g_localVfs, testLogFullName2, "test1", false));
    EXPECT_TRUE(IsFileContainedString(g_localVfs, testLogFullName2, "test2", false));

    /* Clean */
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, testDir1);
    DestroyDirectory(testDir1);
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, testDir2);
    DestroyDirectory(testDir2);
}

/**
 * Test change log configure (line prefix, error log level, log destination) during logger thread running
 */
TEST_F(ErrorLogTest, DISABLED_ChangeConfigureDynamicTest001_Level0)
{
    SetErrLogWriteMode(false);
    Remove(g_localVfs, TEST_STDERR_REDIRECT_FILE);
    /* Redirect stderr to file for test */
    StartStdErrRedirect(TEST_STDERR_REDIRECT_FILE);

    SetErrLogDirectory(TEST_CURRENT_DIRECTORY);
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, TEST_CURRENT_DIRECTORY);

    EXPECT_EQ(StartLogger(), ERROR_SYS_OK);
    ASSERT_TRUE(IsLoggerStarted());
    EXPECT_EQ(OpenLogger(), ERROR_SYS_OK);
    ASSERT_TRUE(IS_ERR_LOG_OPEN());

    /* Switch error log prefix suffix configure, check error log file content */
    uint32_t errLogPrefixSuffixWithouThreadID = LOG_LINE_PREFIX_TIMESTAMP | LOG_LINE_PREFIX_SEVERITY |
        LOG_LINE_PREFIX_COMPILATION_CONTEXT_FILE_NAME | LOG_LINE_PREFIX_COMPILATION_CONTEXT_LINENO;
    SetErrLogLinePrefixSuffix(errLogPrefixSuffixWithouThreadID);
    ErrLog(ERROR, ErrMsg("No thread id"));
    FlushLogger();
    EXPECT_EQ(GetErrorLogFileFullPath(g_errorLogFileFullPath, MAX_PATH, GetErrLogDirectory, ERR_LOG_EXTEND, UT_NAME), ERROR_SYS_OK);
    EXPECT_FALSE(IsFileContainedString(g_localVfs, g_errorLogFileFullPath, "Tid:", false));
    /* Set default prefix suffix, check error log file content */
    SetErrLogLinePrefixSuffix(ERROR_LOG_DEFAULT_PREFIX);
    ErrLog(ERROR, ErrMsg("Contain thread id"));
    EXPECT_TRUE(IsFileContainedString(g_localVfs, g_errorLogFileFullPath, "Tid:", false));

    /* Switch error log level configure */
    SetErrLogServerLevel(ERROR);
    /* Log level = ERROR, WARNING message will not be writen */
    ErrLog(WARNING, ErrMsg("WarningMessage"));
    EXPECT_FALSE(IsFileContainedString(g_localVfs, g_errorLogFileFullPath, "WarningMessage", false));
    SetErrLogServerLevel(WARNING);
    /* Log level = WARNING, WARNING message will be writen */
    ErrLog(WARNING, ErrMsg("WarningMessage"));
    EXPECT_TRUE(IsFileContainedString(g_localVfs, g_errorLogFileFullPath, "WarningMessage", false));

    /* Switch error log destination configure */
    SetErrLogDestination(LOG_DESTINATION_LOCAL_STDERR);
    /* Log destination switch to stderr, log will be writen to stderr but not local file */
    ErrLog(ERROR, ErrMsg("Destination1"));
    EXPECT_FALSE(IsFileContainedString(g_localVfs, g_errorLogFileFullPath, "Destination1", false));
    EXPECT_TRUE(IsFileContainedString(g_localVfs, TEST_STDERR_REDIRECT_FILE, "Destination1", false));
    /* Log destination switch to stderr and local file, log will be writen to both stderr and local file */
    SetErrLogDestination(LOG_DESTINATION_LOCAL_STDERR | LOG_DESTINATION_LOCAL_FILE);
    ErrLog(ERROR, ErrMsg("Destination2"));
    EXPECT_TRUE(IsFileContainedString(g_localVfs, g_errorLogFileFullPath, "Destination2", false));
    EXPECT_TRUE(IsFileContainedString(g_localVfs, TEST_STDERR_REDIRECT_FILE, "Destination2", false));

    usleep(100);
    CloseLogger();
    StopLogger();

    RemoveErrorLogFileFromLocalDirectory(g_localVfs, TEST_CURRENT_DIRECTORY);
    ResetErrLogDirectory();
    StopStdErrRedirect();
    Remove(g_localVfs, TEST_STDERR_REDIRECT_FILE);
}

/**
 * Test open error log automatically when logger thread not ready, output to stderr and syslog
 */
TEST_F(ErrorLogTest, WriteLogWithoutLoggerThreadTest001_Level0)
{
    SetErrLogWriteMode(false);
    Remove(g_localVfs, TEST_STDERR_REDIRECT_FILE);
    /* Redirect stderr to file for test */
    StartStdErrRedirect(TEST_STDERR_REDIRECT_FILE);

    ASSERT_FALSE(IS_ERR_LOG_OPEN());
    /* First call ErrLog will automatically OpenLogger for thread local resource initialize,
     * log will be writen to stderr and syslog without logger thread */
    ErrLog(ERROR, ErrMsg("LogMessage1"));
    ASSERT_TRUE(IS_ERR_LOG_OPEN());

    SetErrLogDirectory(TEST_CURRENT_DIRECTORY);
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, TEST_CURRENT_DIRECTORY);
    EXPECT_EQ(StartLogger(), ERROR_SYS_OK);
    ASSERT_TRUE(IsLoggerStarted());
    ASSERT_TRUE(IS_ERR_LOG_OPEN());
    /* Logger thread ready, log will be writen to local file */
    ErrLog(ERROR, ErrMsg("LogMessage2"));

    usleep(100);
    EXPECT_EQ(GetErrorLogFileFullPath(g_errorLogFileFullPath, MAX_PATH, GetErrLogDirectory, ERR_LOG_EXTEND, UT_NAME), ERROR_SYS_OK);
    StopLogger();
    ASSERT_FALSE(IsLoggerStarted());
    ASSERT_TRUE(IS_ERR_LOG_OPEN());
    /* Logger thread stopped, log will be writen to stderr and syslog */
    ErrLog(ERROR, ErrMsg("LogMessage3"));
    CloseLogger();
    ASSERT_FALSE(IS_ERR_LOG_OPEN());

    EXPECT_FALSE(IsFileContainedString(g_localVfs, g_errorLogFileFullPath, "LogMessage1", false));
    EXPECT_TRUE(IsFileContainedString(g_localVfs, g_errorLogFileFullPath, "LogMessage2", false));
    EXPECT_FALSE(IsFileContainedString(g_localVfs, g_errorLogFileFullPath, "LogMessage3", false));
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, TEST_CURRENT_DIRECTORY);
    ResetErrLogDirectory();
    EXPECT_TRUE(IsFileContainedString(g_localVfs, TEST_STDERR_REDIRECT_FILE, "LogMessage1", false));
    EXPECT_FALSE(IsFileContainedString(g_localVfs, TEST_STDERR_REDIRECT_FILE, "LogMessage2", false));
    EXPECT_TRUE(IsFileContainedString(g_localVfs, TEST_STDERR_REDIRECT_FILE, "LogMessage3", false));

    StopStdErrRedirect();
    Remove(g_localVfs, TEST_STDERR_REDIRECT_FILE);
}

/**
 * Write error log to syslog to test log message split
 */
TEST_F(ErrorLogTest, WriteLongSyslogTest001_Level0)
{
    SetErrLogWriteMode(false);
    Remove(g_localVfs, TEST_STDERR_REDIRECT_FILE);
    /* Redirect stderr to file for test */
    StartStdErrRedirect(TEST_STDERR_REDIRECT_FILE);

    SetErrLogDestination(LOG_DESTINATION_LOCAL_SYSLOG);
    ResetThreadSyslogSeq();

    /* Prepare 2 log message, one with space, another without space */
    constexpr size_t msgLen = 4096;
    char logMessage1[msgLen] = {0}, logMessage2[msgLen] = {0};
    char *linePos = logMessage1;
    char msgWord[] = "HelloWorld";
    size_t wordLen = strlen(msgWord);
    size_t curPos = 0;
    while (curPos + strlen(msgWord) + 1 < msgLen) {
        (void) strcpy_s(linePos + curPos, msgLen - curPos, msgWord);
        curPos += wordLen;
        linePos[curPos] = ' ';
        curPos += 1;
    }
    size_t expectMessageSplitCount1 = msgLen / 900 + 1; /* SYSLOG_MESSAGE_LEN_LIMIT = 900 */
    for (auto &c : logMessage2) {
        c = 'a';
    }
    logMessage2[msgLen - 1] = '\0';
    size_t expectMessageSplitCount2 = msgLen / 900 + 1 + 1; /* Log header will be split into extra line */
    FAULT_INJECTION_ACTIVE(COPY_SYSLOG_OUTPUT_TO_STDERR, FI_GLOBAL);
    ErrLog(ERROR, ErrMsg("%s", logMessage1));
    ErrLog(ERROR, ErrMsg("%s", "Line1\nLine2\nLine3"));
    ErrLog(ERROR, ErrMsg("%s", logMessage2));
    FAULT_INJECTION_INACTIVE(COPY_SYSLOG_OUTPUT_TO_STDERR, FI_GLOBAL);

    CloseLogger();

#ifdef ENABLE_FAULT_INJECTION
    char checkMessage[1024] = {0};
    for (int i = 1; i <= expectMessageSplitCount1; ++i) {
        sprintf_s(checkMessage, sizeof(checkMessage), "[%lu-%d]", 1, i);
        EXPECT_TRUE(IsFileContainedString(g_localVfs, TEST_STDERR_REDIRECT_FILE, checkMessage, false));
    }
    sprintf_s(checkMessage, sizeof(checkMessage), "[%lu-%d]", 1, expectMessageSplitCount1 + 1);
    EXPECT_FALSE(IsFileContainedString(g_localVfs, TEST_STDERR_REDIRECT_FILE, checkMessage, false));
    for (int i = 1; i <= 3; ++i) {
        sprintf_s(checkMessage, sizeof(checkMessage), "[%lu-%d]", 2, i);
        EXPECT_TRUE(IsFileContainedString(g_localVfs, TEST_STDERR_REDIRECT_FILE, checkMessage, false));
    }
    sprintf_s(checkMessage, sizeof(checkMessage), "[%lu-%d]", 2, 4);
    EXPECT_FALSE(IsFileContainedString(g_localVfs, TEST_STDERR_REDIRECT_FILE, checkMessage, false));
    for (int i = 1; i <= expectMessageSplitCount2; ++i) {
        sprintf_s(checkMessage, sizeof(checkMessage), "[%lu-%d]", 3, i);
        EXPECT_TRUE(IsFileContainedString(g_localVfs, TEST_STDERR_REDIRECT_FILE, checkMessage, false));
    }
    sprintf_s(checkMessage, sizeof(checkMessage), "[%lu-%d]", 3, expectMessageSplitCount2 + 1);
    EXPECT_FALSE(IsFileContainedString(g_localVfs, TEST_STDERR_REDIRECT_FILE, checkMessage, false));
#endif /* ENABLE_FAULT_INJECTION */

    StopStdErrRedirect();
    Remove(g_localVfs, TEST_STDERR_REDIRECT_FILE);
}

TEST_F(ErrorLogTest, DeleteErrLogDir001_Level0)
{
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, ERROR_LOG_DEFAULT_DIRECTORY);
    SetErrLogWriteMode(false);
    SetErrLogDirectory(ERROR_LOG_DEFAULT_DIRECTORY);
    ErrorCode errorCode = StartLogger();
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    bool started = IsLoggerStarted();
    ASSERT_TRUE(started);
    ErrLog(ERROR, ErrMsg("test1"));
    FAULT_INJECTION_ACTIVE(MOCK_LOG_ROTATION_TIME, FI_GLOBAL);
    ErrLog(ERROR, ErrMsg("test2"));
    FAULT_INJECTION_INACTIVE(MOCK_LOG_ROTATION_TIME, FI_GLOBAL);
    char cmd[50] = "rm -rf ";
    strcat(cmd, ERROR_LOG_DEFAULT_DIRECTORY);
    int ret = system(cmd);
    ASSERT_EQ(WEXITSTATUS(ret), 0);
    ErrLog(ERROR, ErrMsg("test3"));
    ErrLog(ERROR, ErrMsg("test4"));
    usleep(100);
    StopLogger();
    started = IsLoggerStarted();
    ASSERT_FALSE(started);
}

TEST_F(ErrorLogTest, SwitchErrLogLocalFileCfgWithFaultInject)
{
    EXPECT_EQ(StartLogger(), ERROR_SYS_OK);
    ASSERT_TRUE(IsLoggerStarted());
    ErrLog(ERROR, ErrMsg("test1"));

    FAULT_INJECTION_ACTIVE(SET_ERRLOG_XXNAME_FAULT_INJECTION1, FI_GLOBAL);

    FAULT_INJECTION_ACTIVE(MOCK_LOG_ROTATION_TIME, FI_GLOBAL);
    ErrLog(ERROR, ErrMsg("test2"));
    FAULT_INJECTION_INACTIVE(MOCK_LOG_ROTATION_TIME, FI_GLOBAL);
    ErrLog(ERROR, ErrMsg("test3"));

    usleep(100);
    StopLogger();
    ASSERT_FALSE(IsLoggerStarted());

    FAULT_INJECTION_INACTIVE(SET_ERRLOG_XXNAME_FAULT_INJECTION1, FI_GLOBAL);
}

/**
 * Test if the error log default path is ./error_log 
 */
TEST_F(ErrorLogTest, CheckLoggerDefaultPath001)
{
    ASSERT_EQ(StartLogger(), 0);
    char dir[MAX_PATH] = {0};
    GetErrLogDirectory(dir, sizeof(dir));
    ASSERT_EQ(strcmp(dir, ERROR_LOG_DEFAULT_DIRECTORY), 0);
    usleep(100);
    CloseLogger();
    StopLogger();
}

TEST_F(ErrorLogTest, LocalLogFileTimeRotationTest)
{
    SetErrLogWriteMode(false);
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, ERROR_LOG_DEFAULT_DIRECTORY);
    StartLogger();
    ErrLog(ERROR, ErrMsg("test1"));

    /* Mock local log file time rotation */
    FAULT_INJECTION_ACTIVE(MOCK_LOG_ROTATION_TIME, FI_GLOBAL);
    ErrLog(ERROR, ErrMsg("test2"));
    ErrLog(ERROR, ErrMsg("test3"));
    ErrLog(ERROR, ErrMsg("test4"));
    StopLogger();
    FAULT_INJECTION_INACTIVE(MOCK_LOG_ROTATION_TIME, FI_GLOBAL);

    /* Check the number of log file, there should be 1 log files after the file rotation */
    char filePath[PATH_MAX];
    EXPECT_LE(GetFileInfoFromLocalDirectory(ERROR_LOG_DEFAULT_DIRECTORY, filePath, MAX_PATH, ERR_LOG_EXTEND, UT_NAME), 2);
    EXPECT_GE(GetFileInfoFromLocalDirectory(ERROR_LOG_DEFAULT_DIRECTORY, filePath, MAX_PATH, ZIP_LOG_EXTEND, UT_NAME), 0);
}

TEST_F(ErrorLogTest, AllocThreadMsgFailedTest)
{
    /**
    * @tc.steps: Test error messages will be not sent to server message queue when allocate thread message failed.
    * @tc.expected: The number of error message in message queue is still 0 after allocating thread message failed.
    */
    StartLogger();
    FAULT_INJECTION_ACTIVE(MOCK_ALLOCATE_MESSAGE_FAILED, FI_GLOBAL);
    ErrLog(ERROR, ErrMsg("Allocate thread message failed."));
    FAULT_INJECTION_INACTIVE(MOCK_ALLOCATE_MESSAGE_FAILED, FI_GLOBAL);
    usleep(100);
    StopLogger();
}

TEST_F(ErrorLogTest, OpenLoggerFailedTest)
{
    /**
    * @tc.steps: Test StartLogger can receive the signal when open error log failed.
    * @tc.expected: Test StartLogger will return an error code after open error log failed.
    */
    FAULT_INJECTION_ACTIVE(MOCK_OPEN_LOG_FAILED, FI_GLOBAL);
    ASSERT_EQ(StartLogger(), ERROR_UTILS_ERRORLOG_THREAD_NOT_STARTED);
    FAULT_INJECTION_INACTIVE(MOCK_OPEN_LOG_FAILED, FI_GLOBAL);
    usleep(100);
    StopLogger();
}

TEST_F(ErrorLogTest, CopyLogIdentifierFailedTest)
{
    /**
    * @tc.steps: Test error messages will be not sent to server message queue when copy log identifier failed.
    * @tc.expected: The number of error message in message queue is still 0 after allocating thread message failed.
    */
    StartLogger();
    FAULT_INJECTION_ACTIVE(MOCK_COPY_LOG_IDENTIFIER_FAILED, FI_GLOBAL);
    ErrLog(ERROR, ErrMsg("Copy log identifier failed."));
    FAULT_INJECTION_INACTIVE(MOCK_COPY_LOG_IDENTIFIER_FAILED, FI_GLOBAL);
    usleep(100);
    StopLogger();
}

TEST_F(ErrorLogTest, LocalLogFileRotationWhenLogFileExceedMaxNumber)
{
    SetErrLogWriteMode(false);
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, ERROR_LOG_DEFAULT_DIRECTORY);

    /* Generating 20 log file before starting to write error log */
    for (int i=1; i<=20; i++) {
        char shellCmd[MAX_PATH] = {0x00};
        sprintf(shellCmd, "> ./error_log/utils_unittest-%d-%d-%d_%06d.log", 1111, 11, 11, i);
        system(shellCmd);
        Usleep(1000);
    }

    /* Set log total space 10 KB, log file rotation size 1 KB */
    SetErrLogSpaceSize(10, 1);
    StartLogger();

    /* Write 180 logs to test the file rotation, every 12 logs will cause an rotation */
    PrintErrorLogInLoop(180, 1000, ERROR);
    FlushLogger();

    /* Check the number of log file will not equal to the max log file number that user set */
    char filePath[PATH_MAX];
    ASSERT_EQ(GetFileInfoFromLocalDirectory(ERROR_LOG_DEFAULT_DIRECTORY, filePath, PATH_MAX, ERR_LOG_EXTEND, UT_NAME), 1);

    usleep(100);
    StopLogger();
}

TEST_F(ErrorLogTest, LocalLogFileRotationWhenLogFileBeDeleted)
{
    SetErrLogWriteMode(false);
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, ERROR_LOG_DEFAULT_DIRECTORY);
 
    /* create 9 log archives */
    for (int i = 0; i < 9; i++) {
        std::string createLogFileCmd;
        createLogFileCmd += "touch ";
        createLogFileCmd += "./error_log/utils_unittest-2024-08-14_09121";
        createLogFileCmd += std::to_string(i);
        createLogFileCmd += ".log";
        system(createLogFileCmd.c_str());
    }
 
    /* Set log total space 10 KB, log file rotation size 1 KB */
    SetErrLogSpaceSize(10, 1);
    StartLogger();

    /* Delete all achieved error log files in error_log folder */
    system("rm -rf ./error_log/utils_unittest-*.log");

    /* Write an error log will activate file rotation */
    PrintErrorLogInLoop(1, 1000, ERROR);
    FlushLogger();

    /* Check the number of log file will equal to 1 */
    char filePath[PATH_MAX];
    uint32_t fileCount =
        GetFileInfoFromLocalDirectory(ERROR_LOG_DEFAULT_DIRECTORY, filePath, PATH_MAX, ERR_LOG_EXTEND, UT_NAME);
    ASSERT_EQ(fileCount, 1);

    usleep(100);
    StopLogger();
}

TEST_F(ErrorLogTest, DISABLED_LocalLogFileRotationWhenLogFileDeleteFailed)
{
    SetErrLogWriteMode(false);
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, ERROR_LOG_DEFAULT_DIRECTORY);

    /* Set log total space 10 KB, log file rotation size 1 KB */
    SetErrLogSpaceSize(10, 1);
    StartLogger();

    FAULT_INJECTION_ACTIVE(MOCK_DELETE_LOG_FILE_FAILED, FI_GLOBAL);
    /* Write 120 logs per millisecond to test the file rotation, every 12 logs will cause an rotation */
    PrintErrorLogInLoop(120, 83334, ERROR);
    FlushLogger();
    FAULT_INJECTION_INACTIVE(MOCK_DELETE_LOG_FILE_FAILED, FI_GLOBAL);

    /* Check the number of log file will equal to 1 */
    char filePath[PATH_MAX];
    ASSERT_GE(GetFileInfoFromLocalDirectory(ERROR_LOG_DEFAULT_DIRECTORY, filePath, PATH_MAX, ERR_LOG_EXTEND, UT_NAME), 1);

    usleep(100);
    StopLogger();
}

TEST_F(ErrorLogTest, DISABLED_RecordSyslogWhenInitLoggerFailed)
{
    system("rm -rf ./syslog.txt");

    FAULT_INJECTION_ACTIVE(MOCK_INIT_LOG_CFG_PARAM_FAILED, FI_GLOBAL);
    FAULT_INJECTION_ACTIVE(MOCK_WRITE_SYSLOG, FI_GLOBAL);
    StartLogger();
    FAULT_INJECTION_INACTIVE(MOCK_INIT_LOG_CFG_PARAM_FAILED, FI_GLOBAL);
    FAULT_INJECTION_INACTIVE(MOCK_WRITE_SYSLOG, FI_GLOBAL);

    /* Check the error message in syslog */
    bool result = IsFileContainedString(g_localVfs, "./syslog.txt", "Failed to init logger config", false);
    ASSERT_TRUE(result);
    usleep(100);
    CloseLogger();
    StopLogger();
}

TEST_F(ErrorLogTest, NullPointerAccessTest)
{
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, ERROR_LOG_DEFAULT_DIRECTORY);
    StartLogger();
    (void)GetErrorLogFileFullPath(g_errorLogFileFullPath, MAX_PATH, GetErrLogDirectory, ERR_LOG_EXTEND, UT_NAME);

    char *testStr = "This is a test.";
    FAULT_INJECTION_ACTIVE(MOCK_STRING_INIT_MALLOC_FAILED, FI_GLOBAL);
    ErrLog(ERROR, ErrMsg("%s", testStr));
    FAULT_INJECTION_INACTIVE(MOCK_STRING_INIT_MALLOC_FAILED, FI_GLOBAL);
    FlushLogger();

    EXPECT_FALSE(IsFileContainedString(g_localVfs, g_errorLogFileFullPath, testStr, false));
    usleep(100);
    CloseLogger();
    StopLogger();
}

TEST_F(ErrorLogTest, OpenedFileReachMaxTest)
{
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, ERROR_LOG_DEFAULT_DIRECTORY);

    pid_t testPid;
    int status;
    testPid = fork();
    if (testPid < 0) {
        ASSERT_TRUE(false);
    } else if (testPid == 0) {
        StartLogger();
        FAULT_INJECTION_ACTIVE(MOCK_OPENED_FILE_REACH_MAX, FI_GLOBAL);
        char *testStr = "This is a test.";
        ErrLog(ERROR, ErrMsg("%s", testStr));
        FlushLogger();

        CloseLogger();
        StopLogger();
        FAULT_INJECTION_INACTIVE(MOCK_OPENED_FILE_REACH_MAX, FI_GLOBAL);
        exit(0);
    }
    if (wait(&status) != testPid) {
        ASSERT_TRUE(false);
    }
    char filePath[MAX_PATH] = {0};
    EXPECT_EQ(GetFileInfoFromLocalDirectory(ERROR_LOG_DEFAULT_DIRECTORY, filePath, MAX_PATH, ERR_LOG_EXTEND, UT_NAME), 0);
}

TEST_F(ErrorLogTest, LocalLogFileRotationWhenExistingOtherFileTest)
{
    SetErrLogWriteMode(false);
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, ERROR_LOG_DEFAULT_DIRECTORY);

    /* Set log total space 10 KB, log file rotation size 1 KB */
    SetErrLogSpaceSize(10, 1);
    StartLogger();
    usleep(100);
    ASSERT_TRUE(IsLoggerStarted());

    /**
     * Generating 20 non-log file before starting to write error log,
     * the length of errlog timestamp is 17. Here, creating 20 errlog
     * file which satisfy the validation of errlog timestamp.
     */
    for (long i=100000; i<100020; i++) {
        char shellCmd[MAX_PATH] = {0x00};
        sprintf(shellCmd, "> ./error_log/utils_unittest.%ld", i);
        system(shellCmd);
    }

    /**
     * Generating 20 log file before starting to write error log,
     * Creating 20 error log file which belong to other process.
     */
    for (long i=100000; i<100020; i++) {
        char shellCmd[MAX_PATH] = {0x00};
        sprintf(shellCmd, "> ./error_log/tenant-%d-%d-%d_%06d.log", 1111, 11, 11, i);
        system(shellCmd);
    }

    /**
     * Generating 20 log file before starting to write error log,
     * Creating 20 error log file with invalid link symbol.
     */
    for (long i=100000; i<100020; i++) {
        char shellCmd[MAX_PATH] = {0x00};
        sprintf(shellCmd, "> ./error_log/utils_unittest_%d-%d-%d_%06d.log", 1111, 11, 11, i);
        system(shellCmd);
    }

    /**
     * Generating 20 log file before starting to write error log,
     * Creating 20 error log file with invalid suffix.
     */
    for (long i=100000; i<100020; i++) {
        char shellCmd[MAX_PATH] = {0x00};
        sprintf(shellCmd, "> ./error_log/utils_unittest-%d-%d-%d_%06d.log1", 1111, 11, 11, i);
        system(shellCmd);
    }

    /* Generating one file with only process name */
    char shellCmd[MAX_PATH] = {0x00};
    sprintf(shellCmd, "> ./error_log/utils_unittest");
    system(shellCmd);

    /* Write 240 logs to test the file rotation, every 12 logs will cause an rotation */
    PrintErrorLogInLoop(240, 83334, ERROR);
    FlushLogger();

    /* Check the number of log file will not equal to the max log file number that user set */
    char filePath[PATH_MAX];
    ASSERT_EQ(GetFileInfoFromLocalDirectory(ERROR_LOG_DEFAULT_DIRECTORY, filePath, PATH_MAX, ERR_LOG_EXTEND, UT_NAME), 1);

    usleep(100);
    StopLogger();
    /* Remove log file */
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, ERROR_LOG_DEFAULT_DIRECTORY);
    /* Remove non-log file */
    for (long i=100000; i<100020; i++) {
        char shellCmd[MAX_PATH] = {0x00};
        sprintf(shellCmd, "rm ./error_log/utils_unittest.%ld", i);
        system(shellCmd);
    }
    /* Remove one file with only process name */
    sprintf(shellCmd, "rm ./error_log/utils_unittest");
    system(shellCmd);
}

TEST_F(ErrorLogTest, ErrorLogFileUserPermissionTest)
{
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, ERROR_LOG_DEFAULT_DIRECTORY);
    SetErrLogLocalFileMode(FILE_READ_AND_WRITE_MODE);
    StartLogger();

    ErrLog(ERROR, ErrMsg("This is a test message."));
    FlushLogger();
    (void)GetErrorLogFileFullPath(g_errorLogFileFullPath, MAX_PATH, GetErrLogDirectory, ERR_LOG_EXTEND, UT_NAME);
    ASSERT_TRUE(CheckErrLogFilePermission(g_errorLogFileFullPath, ComputeActualUmask(0600)));

    usleep(100);
    CloseLogger();
    StopLogger();
    SetErrLogLocalFileMode(FILE_READ_AND_WRITE_MODE);
}

TEST_F(ErrorLogTest, ErrorLogFileGroupPermissionTest)
{
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, ERROR_LOG_DEFAULT_DIRECTORY);
    SetErrLogLocalFileMode(FILE_READ_AND_WRITE_MODE | FILE_GROUP_READ_MODE | FILE_GROUP_WRITE_MODE);
    StartLogger();

    ErrLog(ERROR, ErrMsg("This is a test message."));
    FlushLogger();
    (void)GetErrorLogFileFullPath(g_errorLogFileFullPath, MAX_PATH, GetErrLogDirectory, ERR_LOG_EXTEND, UT_NAME);
    ASSERT_TRUE(CheckErrLogFilePermission(g_errorLogFileFullPath, ComputeActualUmask(0660)));

    usleep(100);
    CloseLogger();
    StopLogger();
    SetErrLogLocalFileMode(FILE_READ_AND_WRITE_MODE);
}

TEST_F(ErrorLogTest, ErrorLogFileOtherPermissionTest)
{
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, ERROR_LOG_DEFAULT_DIRECTORY);
    SetErrLogLocalFileMode(FILE_READ_AND_WRITE_MODE | FILE_OTHER_READ_MODE | FILE_OTHER_WRITE_MODE);
    StartLogger();

    ErrLog(ERROR, ErrMsg("This is a test message."));
    FlushLogger();
    (void)GetErrorLogFileFullPath(g_errorLogFileFullPath, MAX_PATH, GetErrLogDirectory, ERR_LOG_EXTEND, UT_NAME);
    ASSERT_TRUE(CheckErrLogFilePermission(g_errorLogFileFullPath, ComputeActualUmask(0606)));

    usleep(100);
    CloseLogger();
    StopLogger();
    SetErrLogLocalFileMode(FILE_READ_AND_WRITE_MODE);
}

TEST_F(ErrorLogTest, ErrorLogFileDefaultPermissionTest)
{
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, ERROR_LOG_DEFAULT_DIRECTORY);
    StartLogger();

    ErrLog(ERROR, ErrMsg("This is a test message."));
    FlushLogger();
    (void)GetErrorLogFileFullPath(g_errorLogFileFullPath, MAX_PATH, GetErrLogDirectory, ERR_LOG_EXTEND, UT_NAME);
    ASSERT_TRUE(CheckErrLogFilePermission(g_errorLogFileFullPath, ComputeActualUmask(0600)));

    usleep(100);
    CloseLogger();
    StopLogger();
    SetErrLogLocalFileMode(FILE_READ_AND_WRITE_MODE);
}

TEST_F(ErrorLogTest, ErrorLogFileDirectoryCreateTest)
{
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, "./gs_log");
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, "./gs_profile/dataname");
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, "./asp_data/dataname");
    SetErrLogDirectory("./gs_log");
    SetPLogDirectory("./gs_profile/dataname");
    SetAspLogDirectory("./asp_data/dataname");
    StartLogger();
    OpenLogger();

    usleep(100);
    CloseLogger();
    StopLogger();

    ASSERT_EQ(access("./gs_log", F_OK), 0);
    ASSERT_EQ(access("./gs_profile/dataname", F_OK), 0);
    ASSERT_EQ(access("./asp_data/dataname", F_OK), 0);

    /* clear directory */
    ASSERT_EQ(rmdir("./gs_log"), 0);
    ASSERT_EQ(rmdir("./gs_profile/dataname"), 0);
    ASSERT_EQ(rmdir("./asp_data/dataname"), 0);
}

TEST_F(ErrorLogTest, WriteDifferentTypeLogsTest)
{
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, ERROR_LOG_DEFAULT_DIRECTORY);
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, TEST_PROFILE_LOG_DEFAULT_DIRECTORY);
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, ASP_LOG_DEFAULT_DIRECTORY);
    SetPLogDirectory(TEST_PROFILE_LOG_DEFAULT_DIRECTORY);
    StartLogger();

    /* write an error log */
    char *testErrLogStr = "This is an error log test.";
    ErrLog(ERROR, ErrMsg("%s", testErrLogStr));

    /* write a profile error log */
    char *testPLogStr = "This is a profile log test.";
    ErrLog(ERROR, ErrMsg("%s", testPLogStr), ErrMsgType(PROFILE_LOG_MESSAGE_TYPE));

    /* write an asp error log */
    char *testAspLogStr = "This is an asp log test.";
    ErrLog(ERROR, ErrMsg("%s", testAspLogStr), ErrMsgType(ASP_LOG_MESSAGE_TYPE));

    /* write a csv error log */
    char *testCsvLogStr = "This is a csv log test.";
    ErrLog(ERROR, ErrMsg("%s", testCsvLogStr), ErrMsgType(CSV_LOG_MESSAGE_TYPE));

    FlushLogger();
    usleep(1000);
    (void)GetErrorLogFileFullPath(g_errorLogFileFullPath, MAX_PATH, GetErrLogDirectory, ERR_LOG_EXTEND, UT_NAME);
    (void)GetErrorLogFileFullPath(g_pLogFileFullPath, MAX_PATH, GetPLogDirectory, PROFILE_LOG_EXTEND, UT_NAME);
    (void)GetErrorLogFileFullPath(g_aspLogFileFullPath, MAX_PATH, GetAspLogDirectory, ERR_LOG_EXTEND, "asp");
    (void)GetErrorLogFileFullPath(g_csvLogFileFullPath, MAX_PATH, GetErrLogDirectory, CSV_LOG_EXTEND, UT_NAME);

    CloseLogger();
    StopLogger();

    EXPECT_TRUE(IsFileContainedString(g_localVfs, g_errorLogFileFullPath, testErrLogStr, false));
    EXPECT_TRUE(IsFileContainedString(g_localVfs, g_pLogFileFullPath, testPLogStr, false));
    EXPECT_TRUE(IsFileContainedString(g_localVfs, g_aspLogFileFullPath, testAspLogStr, false));
    EXPECT_TRUE(IsFileContainedString(g_localVfs, g_csvLogFileFullPath, testCsvLogStr, false));
}

TEST_F(ErrorLogTest, WriteDifferentTypeLogsSizeRotationTest)
{
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, ERROR_LOG_DEFAULT_DIRECTORY);
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, TEST_PROFILE_LOG_DEFAULT_DIRECTORY);
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, ASP_LOG_DEFAULT_DIRECTORY);

    SetErrLogSpaceSize(LOG_SPACE_SIZE, LOG_FILE_SIZE);
    SetPLogDirectory(TEST_PROFILE_LOG_DEFAULT_DIRECTORY);
    StartLogger();

    /* write an error log */
    char *testErrLogStr = "This is an error log test.";
    char *testPLogStr = "This is a profile log test.";
    char *testAspLogStr = "This is an asp log test.";
    char *testCsvLogStr = "This is a csv log test.";

    for (int i = 0; i < 100000; i++) {
        ErrLog(ERROR, ErrMsg("%s", testErrLogStr));
        ErrLog(ERROR, ErrMsg("%s", testPLogStr), ErrMsgType(PROFILE_LOG_MESSAGE_TYPE));
        ErrLog(ERROR, ErrMsg("%s", testAspLogStr), ErrMsgType(ASP_LOG_MESSAGE_TYPE));
        ErrLog(ERROR, ErrMsg("%s", testCsvLogStr), ErrMsgType(CSV_LOG_MESSAGE_TYPE));
    }
    
    FlushLogger();
    CloseLogger();
    StopLogger();

    char filePath[MAX_PATH] = {0};
    EXPECT_EQ(GetFileInfoFromLocalDirectory(ERROR_LOG_DEFAULT_DIRECTORY, filePath, MAX_PATH, ERR_LOG_EXTEND, UT_NAME), 1);
    EXPECT_EQ(GetFileInfoFromLocalDirectory(TEST_PROFILE_LOG_DEFAULT_DIRECTORY, filePath, MAX_PATH, PROFILE_LOG_EXTEND, UT_NAME), 2);
    EXPECT_LE(GetFileInfoFromLocalDirectory(ASP_LOG_DEFAULT_DIRECTORY, filePath, MAX_PATH, ERR_LOG_EXTEND, "asp"), 2);
    EXPECT_EQ(GetFileInfoFromLocalDirectory(ERROR_LOG_DEFAULT_DIRECTORY, filePath, MAX_PATH, CSV_LOG_EXTEND, UT_NAME), 2);
}

TEST_F(ErrorLogTest, WriteDifferentTypeLogsTimeRotationTest)
{
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, ERROR_LOG_DEFAULT_DIRECTORY);
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, TEST_PROFILE_LOG_DEFAULT_DIRECTORY);
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, ASP_LOG_DEFAULT_DIRECTORY);

    SetErrLogSpaceSize(LOG_SPACE_SIZE, LOG_FILE_SIZE);
    SetErrLogWriteMode(false);
    SetPLogDirectory(TEST_PROFILE_LOG_DEFAULT_DIRECTORY);
    StartLogger();

    for (int i = 0; i < 2; i++) {
        ErrLog(ERROR, ErrMsg("%s", "This is an error log test."));
        ErrLog(ERROR, ErrMsg("%s", "This is a profile log test."), ErrMsgType(PROFILE_LOG_MESSAGE_TYPE));
        ErrLog(ERROR, ErrMsg("%s", "This is an asp log test."), ErrMsgType(ASP_LOG_MESSAGE_TYPE));
        ErrLog(ERROR, ErrMsg("%s", "This is a csv log test."), ErrMsgType(CSV_LOG_MESSAGE_TYPE));
        if (i == 0) {
            FlushLogger();
            FAULT_INJECTION_ACTIVE(MOCK_LOG_ROTATION_TIME, FI_GLOBAL);
        }
    }
    CloseLogger();
    StopLogger();
    FAULT_INJECTION_INACTIVE(MOCK_LOG_ROTATION_TIME, FI_GLOBAL);

    char filePath[MAX_PATH] = {0};
    EXPECT_LE(GetFileInfoFromLocalDirectory(ERROR_LOG_DEFAULT_DIRECTORY, filePath, MAX_PATH, ERR_LOG_EXTEND, UT_NAME), 2);
    EXPECT_EQ(GetFileInfoFromLocalDirectory(TEST_PROFILE_LOG_DEFAULT_DIRECTORY, filePath, MAX_PATH, PROFILE_LOG_EXTEND, UT_NAME), 2);
    EXPECT_EQ(GetFileInfoFromLocalDirectory(ASP_LOG_DEFAULT_DIRECTORY, filePath, MAX_PATH, ERR_LOG_EXTEND, "asp"), 1);
    EXPECT_EQ(GetFileInfoFromLocalDirectory(ERROR_LOG_DEFAULT_DIRECTORY, filePath, MAX_PATH, CSV_LOG_EXTEND, UT_NAME), 2);
}

TEST_F(ErrorLogTest, WriteDifferentTypeLogsDeletionTest)
{
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, ERROR_LOG_DEFAULT_DIRECTORY);
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, TEST_PROFILE_LOG_DEFAULT_DIRECTORY);
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, ASP_LOG_DEFAULT_DIRECTORY);
#undef  LOG_SPACE_SIZE
#define LOG_SPACE_SIZE (LOG_FILE_SIZE * 2)
    SetErrLogSpaceSize(LOG_SPACE_SIZE, LOG_FILE_SIZE);
    SetPLogDirectory(TEST_PROFILE_LOG_DEFAULT_DIRECTORY);
    StartLogger();

    FAULT_INJECTION_ACTIVE(MOCK_LOG_ROTATION_TIME, FI_GLOBAL);
    for (int i = 0; i < 3; i++) {
        ErrLog(ERROR, ErrMsg("%s%d.", "This is an error log test", i));
        ErrLog(ERROR, ErrMsg("%s%d.", "This is a profile log test", i), ErrMsgType(PROFILE_LOG_MESSAGE_TYPE));
        ErrLog(ERROR, ErrMsg("%s%d.", "This is an asp log test", i), ErrMsgType(ASP_LOG_MESSAGE_TYPE));
        ErrLog(ERROR, ErrMsg("%s%d.", "This is a csv log test", i), ErrMsgType(CSV_LOG_MESSAGE_TYPE));
        FlushLogger();
        if (i == 0) {
            FAULT_INJECTION_ACTIVE(MOCK_LOG_ROTATION_TIME, FI_GLOBAL);
        }
    }
    FAULT_INJECTION_INACTIVE(MOCK_LOG_ROTATION_TIME, FI_GLOBAL);

    char filePath[MAX_PATH] = {0};
    EXPECT_LE(GetFileInfoFromLocalDirectory(ERROR_LOG_DEFAULT_DIRECTORY, filePath, MAX_PATH, ERR_LOG_EXTEND, UT_NAME), 2);
    EXPECT_EQ(GetFileInfoFromLocalDirectory(TEST_PROFILE_LOG_DEFAULT_DIRECTORY, filePath, MAX_PATH, PROFILE_LOG_EXTEND, UT_NAME), 2);
    EXPECT_LE(GetFileInfoFromLocalDirectory(ASP_LOG_DEFAULT_DIRECTORY, filePath, MAX_PATH, ERR_LOG_EXTEND, "asp"), 2);
    EXPECT_EQ(GetFileInfoFromLocalDirectory(ERROR_LOG_DEFAULT_DIRECTORY, filePath, MAX_PATH, CSV_LOG_EXTEND, UT_NAME), 2);

    CloseLogger();
    StopLogger();
}

static char *ConstructLogMsg(char cha, int msgLen)
{
    char *msgPtr = (char *)malloc(msgLen + 1);
    errno_t err = memset_s(msgPtr, msgLen + 1, cha, msgLen + 1);
    EXPECT_EQ(err, EOK);
    return msgPtr;
}
 
static void DestroyLogMsg(char *msgPtr)
{
    free(msgPtr);
    return;
}
 
void *ThreadWriteAMessage(void *arg)
{
    int *loop = (int *)arg;
    char *msg = ConstructLogMsg('a', 3000);
 
    for (int i=0; i<*loop; i++) {
        TraceChainBegin(NULL);
        ErrLog(ERROR, ErrMsg(msg));
        TraceChainEnd();
    }
 
    CloseLogger();
    DestroyLogMsg(msg);
    return arg;
}
 
void *ThreadWriteBMessage(void *arg)
{
    int *loop = (int *)arg;
    char *msg = ConstructLogMsg('b', 2050);
 
    for (int i=0; i<*loop; i++) {
        TraceChainBegin(NULL);
        ErrLog(ERROR, ErrMsg(msg));
        TraceChainEnd();
    }
 
    CloseLogger();
    DestroyLogMsg(msg);
    return arg;
}
 
void *ThreadWriteCMessage(void *arg)
{
    int *loop = (int *)arg;
    char *msg = ConstructLogMsg('c', 2050);
 
    for (int i=0; i<*loop; i++) {
        TraceChainBegin(NULL);
        ErrLog(ERROR, ErrMsg(msg));
        TraceChainEnd();
    }
 
    CloseLogger();
    DestroyLogMsg(msg);
    return arg;
}
 
void *ThreadWriteDMessage(void *arg)
{
    int *loop = (int *)arg;
    char *msg = ConstructLogMsg('d', 150);
 
    for (int i=0; i<*loop; i++) {
        TraceChainBegin(NULL);
        ErrLog(ERROR, ErrMsg(msg));
        TraceChainEnd();
    }
 
    CloseLogger();
    DestroyLogMsg(msg);
    return arg;
}
 
void *ThreadWriteEMessage(void *arg)
{
    int *loop = (int *)arg;
    char *msg = ConstructLogMsg('e', 3000);
 
    for (int i=0; i<*loop; i++) {
        TraceChainBegin(NULL);
        ErrLog(ERROR, ErrMsg(msg));
        TraceChainEnd();
    }
 
    CloseLogger();
    DestroyLogMsg(msg);
    return arg;
}

void *ThreadWriteError1Message(void *arg)
{
    int *loop = (int *)arg;
 
    for (int i=0; i<*loop; i++) {
        ErrLog(ERROR, ErrMsg("This is an error message1 :%d.", i));
    }
 
    CloseLogger();
    return arg;
}

void *ThreadWriteError2Message(void *arg)
{
    int *loop = (int *)arg;
 
    for (int i=0; i<*loop; i++) {
        ErrLog(ERROR, ErrMsg("This is an error message2 :%d.", i));
    }
 
    CloseLogger();
    return arg;
}

void *ThreadWriteError3Message(void *arg)
{
    int *loop = (int *)arg;
 
    for (int i=0; i<*loop; i++) {
        ErrLog(ERROR, ErrMsg("This is an error message3 :%d.", i));
    }
 
    CloseLogger();
    return arg;
}

void *ThreadWriteError4Message(void *arg)
{
    int *loop = (int *)arg;
 
    for (int i=0; i<*loop; i++) {
        ErrLog(ERROR, ErrMsg("This is an error message4 :%d.", i));
    }
 
    CloseLogger();
    return arg;
}

void *ThreadWritePanicMessage(void *arg)
{
    int *loop = (int *)arg;
 
    for (int i=0; i<*loop; i++) {
        ErrLog(ERROR, ErrMsg("This is an error message5 :%d.", i));
    }
    ErrLog(PANIC, ErrMsg("This is a panic message."));
 
    CloseLogger();
    return arg;
}
 
TEST_F(ErrorLogTest, DISABLED_DifferentLogWriteTest)
{
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, ERROR_LOG_DEFAULT_DIRECTORY);
    StartLogger();
    Tid tid1;
    Tid tid2;
    Tid tid3;
    Tid tid4;
    Tid tid5;
    int loop = 100;
    ErrorCode errCode;
 
    errCode = ThreadCreate(&tid1, ThreadWriteAMessage, &loop);
    EXPECT_EQ(errCode, ERROR_SYS_OK);
    errCode = ThreadCreate(&tid2, ThreadWriteBMessage, &loop);
    EXPECT_EQ(errCode, ERROR_SYS_OK);
    errCode = ThreadCreate(&tid3, ThreadWriteCMessage, &loop);
    EXPECT_EQ(errCode, ERROR_SYS_OK);
    errCode = ThreadCreate(&tid4, ThreadWriteDMessage, &loop);
    EXPECT_EQ(errCode, ERROR_SYS_OK);
    errCode = ThreadCreate(&tid5, ThreadWriteEMessage, &loop);
    EXPECT_EQ(errCode, ERROR_SYS_OK);
 
    errCode = ThreadJoin(tid1, NULL);
    EXPECT_EQ(errCode, ERROR_SYS_OK);
    errCode = ThreadJoin(tid2, NULL);
    EXPECT_EQ(errCode, ERROR_SYS_OK);
    errCode = ThreadJoin(tid3, NULL);
    EXPECT_EQ(errCode, ERROR_SYS_OK);
    errCode = ThreadJoin(tid4, NULL);
    EXPECT_EQ(errCode, ERROR_SYS_OK);
    errCode = ThreadJoin(tid5, NULL);
    EXPECT_EQ(errCode, ERROR_SYS_OK);
 
    StopLogger();

    char filePath[MAX_PATH] = {0};
    EXPECT_EQ(GetFileInfoFromLocalDirectory(ERROR_LOG_DEFAULT_DIRECTORY, filePath, MAX_PATH, ERR_LOG_EXTEND, UT_NAME), 1);
    EXPECT_EQ(GetLogFileLineNum(filePath), 500);
}

TEST_F(ErrorLogTest, WriteLogSizeRotationTest)
{
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, ERROR_LOG_DEFAULT_DIRECTORY);

    SetErrLogSpaceSize(LOG_SPACE_SIZE, LOG_FILE_SIZE);
    StartLogger();

    /* write an error log */
    char *testErrLogStr = "This is an error log test.";

    for (int i = 0; i < 1000; i++) {
        SetErrLogSpaceSize(LOG_SPACE_SIZE, LOG_FILE_SIZE);
        ErrLog(ERROR, ErrMsg("%s", testErrLogStr));
    }
    
    FlushLogger();
    CloseLogger();
    StopLogger();

    char filePath[MAX_PATH] = {0};
    EXPECT_EQ(GetFileInfoFromLocalDirectory(ERROR_LOG_DEFAULT_DIRECTORY, filePath, MAX_PATH, ERR_LOG_EXTEND, UT_NAME), 1);
    EXPECT_EQ(GetLogFileLineNum(filePath), 1000);
}

TEST_F(ErrorLogTest, WriteCompressedLogTest)
{
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, ERROR_LOG_DEFAULT_DIRECTORY);

    SetErrLogSpaceSize(100, 100);
    StartLogger();

    /* write an error log */
    char *testErrLogStr = "This is an error log test.";

    for (int i = 0; i < 1000; i++) {
        ErrLog(ERROR, ErrMsg("%s:%d", testErrLogStr, i));
    }
    
    FlushLogger();
    CloseLogger();
    StopLogger();

    char filePath[MAX_PATH] = {0};
    EXPECT_EQ(GetFileInfoFromLocalDirectory(ERROR_LOG_DEFAULT_DIRECTORY, filePath, MAX_PATH, ERR_LOG_EXTEND, UT_NAME), 1);
    EXPECT_EQ(GetFileInfoFromLocalDirectory(ERROR_LOG_DEFAULT_DIRECTORY, filePath, MAX_PATH, ZIP_LOG_EXTEND, UT_NAME), 1);
}

TEST_F(ErrorLogTest, DisableCompressedLogTest)
{
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, ERROR_LOG_DEFAULT_DIRECTORY);

    SetErrLogSpaceSize(100, 100);
    SetErrorLogCompress(false);
    StartLogger();

    /* write an error log */
    char *testErrLogStr = "This is an error log test.";

    for (int i = 0; i < 1000; i++) {
        ErrLog(ERROR, ErrMsg("%s:%d", testErrLogStr, i));
    }
    
    FlushLogger();
    CloseLogger();
    StopLogger();

    char filePath[MAX_PATH] = {0};
    EXPECT_EQ(GetFileInfoFromLocalDirectory(ERROR_LOG_DEFAULT_DIRECTORY, filePath, MAX_PATH, ERR_LOG_EXTEND, UT_NAME), 2);
    EXPECT_EQ(GetFileInfoFromLocalDirectory(ERROR_LOG_DEFAULT_DIRECTORY, filePath, MAX_PATH, ZIP_LOG_EXTEND, UT_NAME), 0);
}

TEST_F(ErrorLogTest, WriteCompressedLogInBigLogSpaceTest)
{
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, ERROR_LOG_DEFAULT_DIRECTORY);

    SetErrLogSpaceSize(536870912, 100);
    StartLogger();

    /* write an error log */
    char *testErrLogStr = "This is an error log test.";

    for (int i = 0; i < 1000; i++) {
        ErrLog(ERROR, ErrMsg("%s:%d", testErrLogStr, i));
    }
    
    FlushLogger();
    CloseLogger();
    StopLogger();

    char filePath[MAX_PATH] = {0};
    EXPECT_EQ(GetFileInfoFromLocalDirectory(ERROR_LOG_DEFAULT_DIRECTORY, filePath, MAX_PATH, ERR_LOG_EXTEND, UT_NAME), 1);
    EXPECT_EQ(GetFileInfoFromLocalDirectory(ERROR_LOG_DEFAULT_DIRECTORY, filePath, MAX_PATH, ZIP_LOG_EXTEND, UT_NAME), 1);
}

TEST_F(ErrorLogTest, ResetSizeAndWriteCompressedLogTest)
{
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, ERROR_LOG_DEFAULT_DIRECTORY);

    SetErrLogSpaceSize(100, 100);
    StartLogger();

    /* write an error log */
    char *testErrLogStr = "This is an error log test.";

    for (int i = 0; i < 2000; i++) {
        ErrLog(ERROR, ErrMsg("%s:%d", testErrLogStr, i));
    }
    FlushLogger();
    char filePath[MAX_PATH] = {0};
    EXPECT_EQ(GetFileInfoFromLocalDirectory(ERROR_LOG_DEFAULT_DIRECTORY, filePath, MAX_PATH, ERR_LOG_EXTEND, UT_NAME), 1);
    EXPECT_LE(GetFileInfoFromLocalDirectory(ERROR_LOG_DEFAULT_DIRECTORY, filePath, MAX_PATH, ZIP_LOG_EXTEND, UT_NAME), 2);

    SetErrLogSpaceSize(100, 50);
    for (int i = 0; i < 1000; i++) {
        ErrLog(ERROR, ErrMsg("%s:%d", testErrLogStr, i));
    }
    
    FlushLogger();
    CloseLogger();
    StopLogger();
    EXPECT_EQ(GetFileInfoFromLocalDirectory(ERROR_LOG_DEFAULT_DIRECTORY, filePath, MAX_PATH, ERR_LOG_EXTEND, UT_NAME), 1);
    EXPECT_GT(GetFileInfoFromLocalDirectory(ERROR_LOG_DEFAULT_DIRECTORY, filePath, MAX_PATH, ZIP_LOG_EXTEND, UT_NAME), 0);
}

TEST_F(ErrorLogTest, WriteCompressedLogAfterMakeSameNameDirTest)
{
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, ERROR_LOG_DEFAULT_DIRECTORY);

    SetErrLogSpaceSize(100, 100);
    StartLogger();

    /* write an error log */
    char *testErrLogStr = "This is an error log test.";

    FAULT_INJECTION_ACTIVE(MOCK_CREATE_LOG_SAME_NAME_DIR, FI_GLOBAL);
    for (int i = 0; i < 1000; i++) {
        ErrLog(ERROR, ErrMsg("%s:%d", testErrLogStr, i));
    }

    FlushLogger();
    CloseLogger();
    StopLogger();
    FAULT_INJECTION_INACTIVE(MOCK_CREATE_LOG_SAME_NAME_DIR, FI_GLOBAL);

    char filePath[MAX_PATH] = {0};
    EXPECT_EQ(GetFileInfoFromLocalDirectory(ERROR_LOG_DEFAULT_DIRECTORY, filePath, MAX_PATH, ERR_LOG_EXTEND, UT_NAME), 1);
    EXPECT_EQ(GetFileInfoFromLocalDirectory(ERROR_LOG_DEFAULT_DIRECTORY, filePath, MAX_PATH, ZIP_LOG_EXTEND, UT_NAME), 1);
}

static int32_t GetFileSize(const char * filePath) {
    struct stat st;
    if (stat(filePath, &st) == 0) {
        return st.st_size;
    }

    return -1;
}

TEST_F(ErrorLogTest, TheRatioOfCompressedLogSizeTest)
{
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, ERROR_LOG_DEFAULT_DIRECTORY);

#define LOG_ROTATION_SIZE (100 * 1024)
    SetErrLogSpaceSize(100, LOG_ROTATION_SIZE / 1024);
    StartLogger();

    /* write an error log */
    char *testErrLogStr = "This is an error log test.";

    for (int i = 0; i < 1000; i++) {
        ErrLog(ERROR, ErrMsg("%s:%d", testErrLogStr, i));
    }
    
    FlushLogger();
    CloseLogger();
    StopLogger();

    char filePath[MAX_PATH] = {0};
    EXPECT_EQ(GetFileInfoFromLocalDirectory(ERROR_LOG_DEFAULT_DIRECTORY, filePath, MAX_PATH, ERR_LOG_EXTEND, UT_NAME), 1);
    EXPECT_EQ(GetFileInfoFromLocalDirectory(ERROR_LOG_DEFAULT_DIRECTORY, filePath, MAX_PATH, ZIP_LOG_EXTEND, UT_NAME), 1);

    int32_t compressedFileSize = GetFileSize(filePath);
    ASSERT_NE(compressedFileSize, -1);
    /* Make sure that the size of compressed log file is  */
    ASSERT_LT(compressedFileSize, LOG_ROTATION_SIZE * 0.1);
}

TEST_F(ErrorLogTest, CompressedLogFileDeletionTest)
{
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, ERROR_LOG_DEFAULT_DIRECTORY);

    SetErrLogSpaceSize(20000, 20000);
    StartLogger();

    /* write an error log */
    char *testErrLogStr = "This is an error log test.";

    /* Create 50 number of compressed log file */
    for (long i = 0; i < 50; i++) {
        char shellCmd[MAX_PATH] = {0x00};
        sprintf(shellCmd, "> ./error_log/utils_unittest-1111-01-01_0101%0.2d.log.gz", i);
        system(shellCmd);
    }

    /* Continue writing log */
    for (int i = 0; i < 199999; i++) {
        ErrLog(ERROR, ErrMsg("%s:%d", testErrLogStr, i));
    }
    
    FlushLogger();
    CloseLogger();
    StopLogger();

    char filePath[MAX_PATH] = {0};
    EXPECT_LE(GetFileInfoFromLocalDirectory(ERROR_LOG_DEFAULT_DIRECTORY, filePath, MAX_PATH, ZIP_LOG_EXTEND, UT_NAME), 51);
}

TEST_F(ErrorLogTest, DecompressCompressedLogTest)
{
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, ERROR_LOG_DEFAULT_DIRECTORY);

    SetErrLogSpaceSize(300, 100);
    StartLogger();

    /* Write an error log */
    char *testErrLogStr = "This is an error log test.";

    /* Writing log to active log rotation and log compression */
    for (int i = 0; i < 1000; i++) {
        ErrLog(ERROR, ErrMsg("%s:%d", testErrLogStr, i));
        usleep(1200);
    }

    FlushLogger();

    /* Get the compressed log file name */
    char filePath[MAX_PATH] = {0};
    EXPECT_EQ(GetFileInfoFromLocalDirectory(ERROR_LOG_DEFAULT_DIRECTORY, filePath, MAX_PATH, ZIP_LOG_EXTEND, UT_NAME), 1);

    /* Decompress the compressed log file */
    char shellCmd[MAX_PATH] = {0x00};
    sprintf(shellCmd, "gzip -d %s", filePath);
    system(shellCmd);

    /* Check the number of log file and compressed log file */
    EXPECT_LE(GetFileInfoFromLocalDirectory(ERROR_LOG_DEFAULT_DIRECTORY, filePath, MAX_PATH, ERR_LOG_EXTEND, UT_NAME), 2);
    EXPECT_GE(GetFileInfoFromLocalDirectory(ERROR_LOG_DEFAULT_DIRECTORY, filePath, MAX_PATH, ZIP_LOG_EXTEND, UT_NAME), 0);

    /* Writing log to active log rotation and log compression */
    for (int i = 0; i < 1000; i++) {
        ErrLog(ERROR, ErrMsg("%s:%d", testErrLogStr, i));
        usleep(1200);
    }

    FlushLogger();
    CloseLogger();
    StopLogger();

    /* Check the number of log file and compressed log file */
    EXPECT_LE(GetFileInfoFromLocalDirectory(ERROR_LOG_DEFAULT_DIRECTORY, filePath, MAX_PATH, ERR_LOG_EXTEND, UT_NAME), 2);
    EXPECT_GE(GetFileInfoFromLocalDirectory(ERROR_LOG_DEFAULT_DIRECTORY, filePath, MAX_PATH, ZIP_LOG_EXTEND, UT_NAME), 0);
}

TEST_F(ErrorLogTest, LoggerThreadExitTest)
{
    ErrorCode errorCode = StartLogger();
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    bool started = IsLoggerStarted();
    ASSERT_TRUE(started);
    FAULT_INJECTION_ACTIVE(MOCK_SET_LATCH_FAILED, FI_GLOBAL);
    StopLogger();
    FAULT_INJECTION_INACTIVE(MOCK_SET_LATCH_FAILED, FI_GLOBAL);
    started = IsLoggerStarted();
    ASSERT_FALSE(started);
}

TEST_F(ErrorLogTest, DISABLED_LoggerRedirectTest)
{
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, ERROR_LOG_DEFAULT_DIRECTORY);
    /**
    * @tc.steps: step1. Test start logger thread and stop logger thread.
    * @tc.expected: step1.The logger thread functions are correct.
    */
    SetErrLogRedirect();
    ErrorCode errorCode = StartLogger();
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    bool started = IsLoggerStarted();
    ASSERT_TRUE(started);
    ErrLog(ERROR, ErrMsg("This is a test string"));
    StopLogger();
    started = IsLoggerStarted();
    ASSERT_FALSE(started);
    char filePath[MAX_PATH] = {0};
    EXPECT_EQ(GetFileInfoFromLocalDirectory(ERROR_LOG_DEFAULT_DIRECTORY, filePath, MAX_PATH, ERR_LOG_EXTEND, UT_NAME), 1);
    EXPECT_EQ(GetLogFileLineNum(filePath), 1);
}

static bool FindSubString(const char *dirName, const char *subString)
{
    ifstream file(dirName); // 打开文件
    string line;

    while (getline(file, line)) { // 逐行读取文件内容
        if (line.find(subString) != string::npos) { // 查找特定字段
            return true;
        }
    }

    return false;
}

TEST_F(ErrorLogTest, LoggerPanicLogTest)
{
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, ERROR_LOG_DEFAULT_DIRECTORY);
    /**
    * @tc.steps: step1. Test start logger thread and stop logger thread.
    * @tc.expected: step1.The logger thread functions are correct.
    */
    pid_t testPid;
    int status;
    testPid = fork();
    if (testPid < 0) {
        ASSERT_TRUE(false);
    } else if (testPid == 0) {
        ErrorCode errorCode = StartLogger();
        EXPECT_EQ(errorCode, ERROR_SYS_OK);
        bool started = IsLoggerStarted();
        ASSERT_TRUE(started);

        Tid tid1;
        Tid tid2;
        Tid tid3;
        Tid tid4;
        Tid tid5;
        int loop = 100;
        ErrorCode errCode;
    
        errCode = ThreadCreate(&tid1, ThreadWriteError1Message, &loop);
        EXPECT_EQ(errCode, ERROR_SYS_OK);
        errCode = ThreadCreate(&tid2, ThreadWriteError2Message, &loop);
        EXPECT_EQ(errCode, ERROR_SYS_OK);
        errCode = ThreadCreate(&tid3, ThreadWritePanicMessage, &loop);
        EXPECT_EQ(errCode, ERROR_SYS_OK);
        errCode = ThreadCreate(&tid4, ThreadWriteError3Message, &loop);
        EXPECT_EQ(errCode, ERROR_SYS_OK);
        errCode = ThreadCreate(&tid5, ThreadWriteError4Message, &loop);
        EXPECT_EQ(errCode, ERROR_SYS_OK);
    
        errCode = ThreadJoin(tid1, NULL);
        EXPECT_EQ(errCode, ERROR_SYS_OK);
        errCode = ThreadJoin(tid2, NULL);
        EXPECT_EQ(errCode, ERROR_SYS_OK);
        errCode = ThreadJoin(tid3, NULL);
        EXPECT_EQ(errCode, ERROR_SYS_OK);
        errCode = ThreadJoin(tid4, NULL);
        EXPECT_EQ(errCode, ERROR_SYS_OK);
        errCode = ThreadJoin(tid5, NULL);
        EXPECT_EQ(errCode, ERROR_SYS_OK);

        StopLogger();
        started = IsLoggerStarted();
        ASSERT_FALSE(started);
        exit(0);
    }
    if (wait(&status) != testPid) {
        ASSERT_TRUE(false);
    }
    char filePath[MAX_PATH] = {0};
    EXPECT_EQ(GetFileInfoFromLocalDirectory(ERROR_LOG_DEFAULT_DIRECTORY, filePath, MAX_PATH, ERR_LOG_EXTEND, UT_NAME), 1);
#define ERROR_LOG_DEFAULT_PATH "./error_log/utils_unittest-current.log"
    EXPECT_TRUE(FindSubString(ERROR_LOG_DEFAULT_PATH, "PANIC"));
}
TEST_F(ErrorLogTest, LoggerErrMsgOutOfBoundTest)
{
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, ERROR_LOG_DEFAULT_DIRECTORY);
    /**
    * @tc.steps: step1. Test start logger thread and stop logger thread.
    * @tc.expected: step1.The logger thread functions are correct.
    */
    ErrorCode errorCode = StartLogger();
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    bool started = IsLoggerStarted();
    ASSERT_TRUE(started);
    OpenLogger();
    ErrMsg("This is a test message.");
    StopLogger();
    started = IsLoggerStarted();
    ASSERT_FALSE(started);
    char filePath[MAX_PATH] = {0};
    EXPECT_EQ(GetFileInfoFromLocalDirectory(ERROR_LOG_DEFAULT_DIRECTORY, filePath, MAX_PATH, ERR_LOG_EXTEND, UT_NAME), 1);
    EXPECT_EQ(GetLogFileLineNum(filePath), 1);
}

TEST_F(ErrorLogTest, LoggerDiskFullAndRecoverTest)
{
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, ERROR_LOG_DEFAULT_DIRECTORY);
    StartLogger();
    /* Disk not Full */
    ErrLog(ERROR, ErrMsg("This is a test string when disk not full."));
    FlushLogger();
    ASSERT_EQ(access("./error_log/utils_unittest-current.log", F_OK), 0);

    /* Mock disk full */
    FAULT_INJECTION_ACTIVE(MOCK_LOG_DISK_FULL, FI_GLOBAL);
    ErrLog(ERROR, ErrMsg("This is a test string when disk full."));
    FlushLogger();
    ASSERT_EQ(access("./error_log/utils_unittest-current.log.disk_full", F_OK), 0);
    FAULT_INJECTION_INACTIVE(MOCK_LOG_DISK_FULL, FI_GLOBAL);

    /* Mock disk full recover*/
    ErrLog(ERROR, ErrMsg("This is a test string when disk recover."));
    StopLogger();
    ASSERT_EQ(access("./error_log/utils_unittest-current.log", F_OK), 0);
}

TEST_F(ErrorLogTest, LoggerFlushTest)
{
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, ERROR_LOG_DEFAULT_DIRECTORY);
    StartLogger();
    ErrLog(ERROR, ErrMsg("This is a test string."));
    FlushLogger();
    char filePath[MAX_PATH] = {0};
    EXPECT_EQ(GetFileInfoFromLocalDirectory(ERROR_LOG_DEFAULT_DIRECTORY, filePath, MAX_PATH, ERR_LOG_EXTEND, UT_NAME), 1);
    EXPECT_EQ(GetLogFileLineNum(filePath), 1);
    CloseLogger();
    StopLogger();
}

/**
 * @tc.name:  ErrLogRotateInterval_Level0
 * @tc.desc:  Test the error log count.
 * @tc.type: FUNC
 */
TEST_F(ErrorLogTest, ErrLogRotateInterval_Level0)
{
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, ERROR_LOG_DEFAULT_DIRECTORY);
    SetErrorLogCompress(false);
    ErrorCode errorCode = StartLogger();
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    bool started = IsLoggerStarted();
    ASSERT_TRUE(started);
    char logDirectory[MAX_PATH] = {0};
    GetErrLogDirectory(logDirectory, MAX_PATH);
    errorCode = OpenLogger();
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    bool opened = IS_ERR_LOG_OPEN();
    ASSERT_TRUE(opened);

#define ERROR_LOG_CONCURRENT_RECORD_COUNT  10000
    Tid tid[THREAD_NUMBERS];
    int i;
    int integerVec[THREAD_NUMBERS];
    for (i = 0; i < THREAD_NUMBERS; i++) {
        integerVec[i] = ERROR_LOG_CONCURRENT_RECORD_COUNT;
        errorCode = ThreadCreate(&tid[i], ThreadRoutineConcurrentErrLog, &integerVec[i]);
        EXPECT_EQ(errorCode, ERROR_SYS_OK);
    }
    int *valuePtr;
    for (i = 0; i < THREAD_NUMBERS; i++) {
        errorCode = ThreadJoin(tid[i], (void **) &valuePtr);
        EXPECT_EQ(errorCode, ERROR_SYS_OK);
        ASSERT_NE(*valuePtr, -1);
    }
    sleep(3);
    CloseLogger();
    opened = IS_ERR_LOG_OPEN();
    ASSERT_FALSE(opened);

    sleep(1);
    StopLogger();
    started = IsLoggerStarted();
    ASSERT_FALSE(started);

    int ExpectdLineCount = THREAD_NUMBERS * (ERROR_LOG_CONCURRENT_RECORD_COUNT * 3 + 1);
    int ResultCount = CountDirectoryFileLine(ERROR_LOG_DEFAULT_DIRECTORY);
    EXPECT_EQ(ResultCount, ExpectdLineCount);
    SetErrorLogCompress(true);
#undef ERROR_LOG_CONCURRENT_RECORD_COUNT
}

TEST_F(ErrorLogTest, CompressionLogThreadMemoryLeakTest)
{
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, ERROR_LOG_DEFAULT_DIRECTORY);

    pid_t testPid;
    int status;
    StartLogger();
    char *testStr = "This is a test.";
    ErrLog(ERROR, ErrMsg("%s", testStr));
    StopLogger();
    testPid = fork();
    if (testPid < 0) {
        ASSERT_TRUE(false);
    } else if (testPid == 0) {
        exit(0);
    }
    if (wait(&status) != testPid) {
        ASSERT_TRUE(false);
    }
    char filePath[MAX_PATH] = {0};
    EXPECT_EQ(GetFileInfoFromLocalDirectory(ERROR_LOG_DEFAULT_DIRECTORY, filePath, MAX_PATH, ERR_LOG_EXTEND, UT_NAME), 1);
}

void *ThreadWriteMessage(void *arg)
{
    int *loop = (int *)arg;
    for (int i=0; i<*loop; i++) {
        ErrLog(ERROR, ErrMsg("This is a test log."));
        sleep(1);
    }

    CloseLogger();
}

static time_t timeLogGap;
void *ThreadCountWriteMessageTime(void *arg)
{
    int *loop = (int *)arg;
    time_t startLogTime = time(NULL);
    for (int i=0; i<*loop; i++) {
        ErrLog(ERROR, ErrMsg("This is a test log."));
    }
    time_t endlogTime = time(NULL);
    timeLogGap = endlogTime - startLogTime;

    CloseLogger();
}

TEST_F(ErrorLogTest, LogPrintTimeCostTest)
{
    RemoveErrorLogFileFromLocalDirectory(g_localVfs, ERROR_LOG_DEFAULT_DIRECTORY);
    StartLogger();
#define NUM_THREADS 3000
    int loop = 10;
    int testLogLoop = 1;
    Tid threads[NUM_THREADS];
    Tid tid;
    ErrorCode errCode;

    for (int i=0; i<NUM_THREADS; i++) {
        errCode = ThreadCreate(&threads[i], ThreadWriteMessage, &loop);
        EXPECT_EQ(errCode, ERROR_SYS_OK);
    }
    
    errCode = ThreadCreate(&tid, ThreadCountWriteMessageTime, &testLogLoop);
    EXPECT_EQ(errCode, ERROR_SYS_OK);
    ASSERT_LE(timeLogGap, 3);

    for (int i=0; i<NUM_THREADS; i++) {
        errCode = ThreadJoin(threads[i], NULL);
        EXPECT_EQ(errCode, ERROR_SYS_OK);
    }

    errCode = ThreadJoin(tid, NULL);
    EXPECT_EQ(errCode, ERROR_SYS_OK);
 
    StopLogger();
#undef NUM_THREADS
}