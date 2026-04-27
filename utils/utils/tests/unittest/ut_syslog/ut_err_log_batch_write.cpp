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
 */

#include <gtest/gtest.h>
#include <fstream>
#include <iostream>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <thread>
#include "syslog/err_log.h"
#include "syslog/err_log_fold.h"
#include "syslog/err_log_internal.h"
#include "fault_injection/fault_injection.h"
#include "ut_err_log_common.h"

using namespace std;
 
VirtualFileSystem *g_localVfsBatchWrite;

#define TEST_LOG_UNDER_BATCH 3
#define TEST_LOG_OVER_BATCH 100

#define ERROR_LOG_LEVEL_INFO_TEST_STRING  "Hello world, INFO MESSAGE, see you!"
 
char g_logFileFullPath[MAX_PATH] = {0};
 
static void RepeatWriteLog(int loopCount, char *msg)
{
    for (int i = 1; i <= loopCount; ++i) {
        ErrLog(INFO, ErrMsg(msg));
    }
}
 
static void PrepareLoggerResource()
{
    SetErrLogServerLevel(INFO);
    SetErrLogDirectory(ERROR_LOG_DEFAULT_DIRECTORY);
    RemoveErrorLogFileFromLocalDirectory(g_localVfsBatchWrite, ERROR_LOG_DEFAULT_DIRECTORY);
    ErrorCode errorCode = StartLogger();
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    bool started = IsLoggerStarted();
    ASSERT_TRUE(started);
 
    errorCode = OpenLogger();
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    bool opened = IS_ERR_LOG_OPEN();
    ASSERT_TRUE(opened);
}
 
static void ReleaseLoggerResource()
{
    sleep(1);
    CloseLogger();
    bool opened = IS_ERR_LOG_OPEN();
    ASSERT_FALSE(opened);
 
    StopLogger();
    bool started = IsLoggerStarted();
    ASSERT_FALSE(started);
}
 
int GetLogFileLineNum(const char *fileName)
{
    std::ifstream fp; 
    char curChar, prevChar = '\n';
    int lineNum = 0;
    fp.open(fileName, ios::in); /* do not ignore '\n' */
    fp.unsetf(ios::skipws);
    while(fp.peek() != EOF) /* read til the end of file */
    {
        fp>>curChar;
        if(curChar == '\n') {
            lineNum ++;
        }
        prevChar = curChar;
    }
    if(prevChar != '\n') {
        lineNum ++;
    }
    return lineNum;
}

class ErrorLogBatchWriteTest : public testing::Test {
protected:
    static void SetUpTestCase()
    {
        FaultInjectionEntry entries[] = {
            FAULT_INJECTION_ENTRY(COPY_SYSLOG_OUTPUT_TO_STDERR, false, nullptr),
        };
        ASSERT_EQ(RegisterFaultInjection(entries, sizeof(entries) / sizeof(entries[0]), FI_GLOBAL), ERROR_SYS_OK);
 
        ErrorCode errCode = ERROR_SYS_OK;
        errCode = InitVfsModule(NULL);
        if (errCode != ERROR_SYS_OK) {
            fprintf(stderr, "InitVfsModule failed!\n");
        }
        errCode = GetStaticLocalVfsInstance(&g_localVfsBatchWrite);
        if (errCode != ERROR_SYS_OK) {
            fprintf(stderr, "GetStaticLocalVfsInstance failed!\n");
        }
#define TEST_ERROR_LOG_FILE_SIZE (64 * 1024)
#define TEST_TOTAL_ERROR_LOG_SPACE (TEST_ERROR_LOG_FILE_SIZE * 20)
        SetErrLogSpaceSize(TEST_TOTAL_ERROR_LOG_SPACE, TEST_ERROR_LOG_FILE_SIZE);
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
        PrepareLoggerResource();
    };
 
    void TearDown() override
    {
        ResetErrorLogConfigure();
        ReleaseLoggerResource();
    };
    LogIdentifier logIdentifier;
};
 
/**
 * @tc.name:  BatchWriteTest001
 * @tc.desc:  Test batch write function is not activated when the total error log is not exceed 8k.
 * @tc.type: FUNC
 */
#define UT_NAME "utils_unittest"
TEST_F(ErrorLogBatchWriteTest, BatchWriteTest001)
{
    char filePath[PATH_MAX];
    RepeatWriteLog(TEST_LOG_UNDER_BATCH, ERROR_LOG_LEVEL_INFO_TEST_STRING);
    /* Total log size do not exceed the batch size, no log is written to log file */
    (void)memset_s(filePath, sizeof(filePath), 0, sizeof(filePath));
    (void)GetFileInfoFromLocalDirectory(ERROR_LOG_DEFAULT_DIRECTORY, filePath, PATH_MAX, ERR_LOG_EXTEND, UT_NAME);
    int lineNum = GetLogFileLineNum(filePath);
    ASSERT_EQ(lineNum, 0);
    FlushLogger();
    (void)memset_s(filePath, sizeof(filePath), 0, sizeof(filePath));
    (void)GetFileInfoFromLocalDirectory(ERROR_LOG_DEFAULT_DIRECTORY, filePath, PATH_MAX, ERR_LOG_EXTEND, UT_NAME);
    /* after exiting logger thread, the remaining logs in buffer will be flushed into log file */
    lineNum = GetLogFileLineNum(filePath);
    ASSERT_TRUE(lineNum > 0);
}

/**
 * @tc.name:  BatchWriteTest003
 * @tc.desc:  Test batch write function is activated when the total error log is exceed 8k.
 * @tc.type: FUNC
 */

TEST_F(ErrorLogBatchWriteTest, BatchWriteTest003)
{
    char filePath[PATH_MAX];
    RepeatWriteLog(TEST_LOG_OVER_BATCH, ERROR_LOG_LEVEL_INFO_TEST_STRING);
    (void)memset_s(filePath, sizeof(filePath), 0, sizeof(filePath));
    (void)GetFileInfoFromLocalDirectory(ERROR_LOG_DEFAULT_DIRECTORY, filePath, PATH_MAX, ERR_LOG_EXTEND, UT_NAME);
    int lineNum = GetLogFileLineNum(filePath);
    /* Total log size exceed the batch size, the existing logs in curent batch are written to log file */
    FlushLogger();
    (void)memset_s(filePath, sizeof(filePath), 0, sizeof(filePath));
    (void)GetFileInfoFromLocalDirectory(ERROR_LOG_DEFAULT_DIRECTORY, filePath, PATH_MAX, ERR_LOG_EXTEND, UT_NAME);
    /* after exiting logger thread, the remaining logs in buffer will be flushed into log file */
    ASSERT_TRUE(GetLogFileLineNum(filePath) > lineNum);
}

/**
 * @tc.name:  BatchWriteTest004
 * @tc.desc:  Test batch write function is not activated when the single error log is exceed 8k.
 * @tc.type: FUNC
 */

TEST_F(ErrorLogBatchWriteTest, BatchWriteTest004)
{
    char filePath[PATH_MAX];
    std::string longMsg(8192, 'a');
    ErrLog(INFO, ErrMsg(longMsg.c_str()));
    usleep(1000);
    (void)memset_s(filePath, sizeof(filePath), 0, sizeof(filePath));
    (void)GetFileInfoFromLocalDirectory(ERROR_LOG_DEFAULT_DIRECTORY, filePath, PATH_MAX, ERR_LOG_EXTEND, UT_NAME);
    int lineNum = GetLogFileLineNum(filePath);
    ASSERT_EQ(lineNum, 1);
    printf("%d\n", lineNum);
}

TEST_F(ErrorLogBatchWriteTest, ErrLogSyncWriteTest)
{
    char filePath[PATH_MAX];
    (void)memset_s(filePath, sizeof(filePath), 0, sizeof(filePath));
    (void)GetFileInfoFromLocalDirectory(ERROR_LOG_DEFAULT_DIRECTORY, filePath, PATH_MAX, ERR_LOG_EXTEND, UT_NAME);
    ErrLog(INFO, ErrMsg(ERROR_LOG_LEVEL_INFO_TEST_STRING));
    /* There should be no log in log file before FlushLogger */
    ASSERT_EQ(GetLogFileLineNum(filePath), 0);
    FlushLogger();
    (void)memset_s(filePath, sizeof(filePath), 0, sizeof(filePath));
    (void)GetFileInfoFromLocalDirectory(ERROR_LOG_DEFAULT_DIRECTORY, filePath, PATH_MAX, ERR_LOG_EXTEND, UT_NAME);
    GetErrorLogFileFullPath(g_logFileFullPath, MAX_PATH, GetErrLogDirectory, ERR_LOG_EXTEND, UT_NAME);
    bool result = IsFileContainedString(g_localVfsBatchWrite, g_logFileFullPath, ERROR_LOG_LEVEL_INFO_TEST_STRING, false);
    ASSERT_TRUE(result);
    /* One error log per thread, the number of logs should be equal to thread number. */
    ASSERT_EQ(GetLogFileLineNum(filePath), 1);
}