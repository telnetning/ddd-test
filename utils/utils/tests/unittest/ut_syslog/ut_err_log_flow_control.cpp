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
#include <mockcpp/mokc.h>
#include "syslog/err_log.h"
#include "syslog/err_log_flow_control.h"
#include "syslog/err_log_internal.h"
#include "fault_injection/fault_injection.h"
#include "ut_err_log_common.h"
 
VirtualFileSystem *g_localVfsFlowCtl;
 
uint64_t g_curStatTime; /* current log time */
uint64_t g_preStatTime; /* start time this log period time */
uint32_t g_count; /* the number of received logs in this period */

static void SetLogStatData(uint64_t curStatTime, uint64_t preStatTime, uint32_t count)
{
    g_curStatTime = curStatTime;
    g_preStatTime = preStatTime;
    g_count = count;
}

static void RepeatWriteLogPreTime(int loopCount, char *msg)
{
    for (int i = 1; i <= loopCount; ++i) {
        ErrLog(INFO, ErrMsg(msg));
        Usleep(90 * 1000); /* 1 second */
    }
    ErrLog(INFO, ErrMsg(msg));
}
 
class ErrorLogFlowControlTest : public testing::Test {
protected:
    static void SetUpTestCase()
    {
        ErrorCode errCode = ERROR_SYS_OK;
        errCode = InitVfsModule(NULL);
        if (errCode != ERROR_SYS_OK) {
            fprintf(stderr, "InitVfsModule failed!\n");
        }
        errCode = GetStaticLocalVfsInstance(&g_localVfsFlowCtl);
        if (errCode != ERROR_SYS_OK) {
            fprintf(stderr, "GetStaticLocalVfsInstance failed!\n");
        }
#define TEST_ERROR_LOG_FILE_SIZE (8 * 1024)
#define TEST_TOTAL_ERROR_LOG_SPACE (TEST_ERROR_LOG_FILE_SIZE * 6)
        SetErrLogSpaceSize(TEST_TOTAL_ERROR_LOG_SPACE, TEST_ERROR_LOG_FILE_SIZE);
        RemoveErrorLogFileFromLocalDirectory(g_localVfsFlowCtl, ERROR_LOG_DEFAULT_DIRECTORY);
        g_localVfsFlowCtl = NULL;
    }
 
    static void TearDownTestCase()
    {
        ErrorCode errCode = ExitVfsModule();
        if (errCode != ERROR_SYS_OK) {
            fprintf(stderr, "ExitVfsModule failed!\n");
        }
        ResetErrLogSpaceSize();
        RemoveErrorLogFileFromLocalDirectory(g_localVfsFlowCtl, ERROR_LOG_DEFAULT_DIRECTORY);
        g_localVfsFlowCtl = NULL;
    }
 
    void SetUp() override
    {
        logIdentifier.threadId = 1;
        logIdentifier.lineNum = 1;
        logIdentifier.logLevel = INFO;
        logIdentifier.timeSecond = 1;
        (void)memset_s(logIdentifier.fileName, sizeof(logIdentifier.fileName), 0, sizeof(logIdentifier.fileName));
        (void)strncpy_s(logIdentifier.fileName, sizeof(logIdentifier.fileName), __FILE__, sizeof(logIdentifier.fileName) - 1);

        g_curStatTime = 0;
        g_preStatTime = 0;
        g_count = 0;
        ResetErrorLogConfigure();
    };
 
    void TearDown() override
    {
        ResetErrorLogConfigure();
    };
    LogIdentifier logIdentifier;
};
 
/**
 * @tc.name:  TestFlowControlWithValidParametersThatInitSuccess_DEBUG
 * @tc.desc:  Test if the user config is valid.
 * @tc.type: FUNC
 */
 
TEST_F(ErrorLogFlowControlTest, TestFlowControlWithValidParametersThatInitSuccess_DEBUG)
{   
    /**
    * @tc.steps: step1. Set the user config using valid parameters.
    * @tc.expected: step1.The user config is valid.
    */
#define TEST_ERRLOG_FLOW_CONTROL_THRESHOLD 10
#define TEST_ERRLOG_STAT_PERIOD 1
    LogFlowContext *context = LogFlowInitContext(TEST_ERRLOG_FLOW_CONTROL_THRESHOLD, DEBUG, TEST_ERRLOG_STAT_PERIOD);
    ASSERT(context != NULL);
    LogFlowFreeContext(context);
    context = NULL;
}

/**
 * @tc.name:  TestFlowControlWithValidParametersThatInitSuccess_LOG
 * @tc.desc:  Test if the user config is valid.
 * @tc.type: FUNC
 */
 
TEST_F(ErrorLogFlowControlTest, TestFlowControlWithValidParametersThatInitSuccess)
{   
    /**
    * @tc.steps: step1. Set the user config using valid parameters.
    * @tc.expected: step1.The user config is valid.
    */
#define TEST_ERRLOG_FLOW_CONTROL_THRESHOLD 10

    LogFlowContext *context = LogFlowInitContext(TEST_ERRLOG_FLOW_CONTROL_THRESHOLD, LOG, TEST_ERRLOG_STAT_PERIOD);
    ASSERT(context != NULL);
    LogFlowFreeContext(context);

    context = LogFlowInitContext(TEST_ERRLOG_FLOW_CONTROL_THRESHOLD, INFO, TEST_ERRLOG_STAT_PERIOD);
    ASSERT(context != NULL);
    LogFlowFreeContext(context);

    context = LogFlowInitContext(TEST_ERRLOG_FLOW_CONTROL_THRESHOLD, NOTICE, TEST_ERRLOG_STAT_PERIOD);
    ASSERT(context != NULL);
    LogFlowFreeContext(context);

    context = LogFlowInitContext(TEST_ERRLOG_FLOW_CONTROL_THRESHOLD, WARNING, TEST_ERRLOG_STAT_PERIOD);
    ASSERT(context != NULL);
    LogFlowFreeContext(context);

    context = NULL;
}

TEST_F(ErrorLogFlowControlTest, TestFlowControlWithInValidParametersThatInitFail_FATAL)
{   
    /**
    * @tc.steps: step1. Set the user config using invalid parameters.
    * @tc.expected: step1.The user config is invalid.
    */
    LogFlowContext *context = LogFlowInitContext(TEST_ERRLOG_FLOW_CONTROL_THRESHOLD, FATAL, TEST_ERRLOG_STAT_PERIOD);

    context = LogFlowInitContext(TEST_ERRLOG_FLOW_CONTROL_THRESHOLD, PANIC, TEST_ERRLOG_STAT_PERIOD);

    ASSERT(context == NULL);
}
 
/**
 * @tc.name:  TestFlowControlWithDefaultParametersThatInitFail
 * @tc.desc:  Test if the flow control function is need.
 * @tc.type: FUNC
 */
 
TEST_F(ErrorLogFlowControlTest, TestFlowControlWithDefaultParametersThatInitFail)
{   
    /**
    * @tc.steps: step1. Do not set the user config.
    * @tc.expected: step1.The flow control function is not need.
    */
#define DEFAULT_ERRLOG_FLOW_CONTROL_THRESHOLD 0
#define DEFAULT_ERRLOG_LEVEL 0
#define DEFAULT_ERRLOG_STAT_PERIOD 0
#define TEST_CURRENT_TIME 1
#define TEST_PREVIOUS_STAT_TIME 0
#define TEST_PERIOD_LOG_COUNT 3
    LogFlowContext *context = LogFlowInitContext(DEFAULT_ERRLOG_FLOW_CONTROL_THRESHOLD, DEFAULT_ERRLOG_LEVEL,
                                                 DEFAULT_ERRLOG_STAT_PERIOD);
    ASSERT(context == NULL);
}
 
/**
 * @tc.name:  TestFlowControlIsNeedWhenLogSpeedExceedThreshold
 * @tc.desc:  Test if the flow control function is need.
 * @tc.type: FUNC
 */
 
TEST_F(ErrorLogFlowControlTest, TestFlowControlIsNeedWhenLogSpeedExceedThreshold)
{   
    /**
    * @tc.steps: step1. flow control threshold exceed the speed of received logs.
    * @tc.expected: step1.The flow control function is need.
    */
    LogFlowContext *context = LogFlowInitContext(TEST_ERRLOG_FLOW_CONTROL_THRESHOLD, INFO, TEST_ERRLOG_STAT_PERIOD);
    ASSERT(context != NULL);
#define TEST_CURRENT_TIME 1000
#define TEST_PREVIOUS_STAT_TIME 0
#define TEST_PERIOD_LOG_COUNT 10
    SetLogStatData(TEST_CURRENT_TIME, TEST_PREVIOUS_STAT_TIME, TEST_PERIOD_LOG_COUNT);
    LogStatTime logStatTime;
    logStatTime.curStatTime = g_curStatTime;
    logStatTime.preStatTime = g_preStatTime;
    bool result = IsNeededLogFlowControl(context, &logIdentifier, &logStatTime, &g_count);
    ASSERT_TRUE(result);
    LogFlowFreeContext(context);
    context = NULL;
}
 
/**
 * @tc.name:  TestFlowControlIsNotNeedWhenLogSpeedNotExceedThreshold
 * @tc.desc:  Test if the flow control function is need.
 * @tc.type: FUNC
 */
 
TEST_F(ErrorLogFlowControlTest, TestFlowControlIsNotNeedWhenLogSpeedNotExceedThreshold)
{   
    /**
    * @tc.steps: step1. flow control threshold do not exceed the speed of received logs.
    * @tc.expected: step1.The flow control function is not need.
    */
    LogFlowContext *context = LogFlowInitContext(TEST_ERRLOG_FLOW_CONTROL_THRESHOLD, INFO, TEST_ERRLOG_STAT_PERIOD);
    ASSERT(context != NULL);
#undef TEST_PERIOD_LOG_COUNT
#define TEST_PERIOD_LOG_COUNT 9
    SetLogStatData(TEST_CURRENT_TIME, TEST_PREVIOUS_STAT_TIME, TEST_PERIOD_LOG_COUNT);
    LogStatTime logStatTime;
    logStatTime.curStatTime = g_curStatTime;
    logStatTime.preStatTime = g_preStatTime;
    bool result = IsNeededLogFlowControl(context, &logIdentifier, &logStatTime, &g_count);
    ASSERT_FALSE(result);
    LogFlowFreeContext(context);
    context = NULL;
}
 
/**
 * @tc.name:  TestLogFilterIsNeedWhenLogLevelNotExceedFilterLevel
 * @tc.desc:  Test if the log filter function is need.
 * @tc.type: FUNC
 */
 
TEST_F(ErrorLogFlowControlTest, TestLogFilterIsNeedWhenLogLevelNotExceedFilterLevel)
{   
    /**
    * @tc.steps: step1. set log level greater than the filter level.
    * @tc.expected: step1.The log filter function is need.
    */
    LogFlowContext *context = LogFlowInitContext(TEST_ERRLOG_FLOW_CONTROL_THRESHOLD, WARNING, TEST_ERRLOG_STAT_PERIOD);
    ASSERT(context != NULL);
    bool result = IsNeedLogFilter(context, &logIdentifier);
    ASSERT_TRUE(result);
    LogFlowFreeContext(context);
    context = NULL;
}
 
/**
 * @tc.name:  TestLogFilterIsNotNeedWhenLogLevelExceedFilterLevel
 * @tc.desc:  Test if the log filter function is need.
 * @tc.type: FUNC
 */
 
TEST_F(ErrorLogFlowControlTest, TestLogFilterIsNotNeedWhenLogLevelExceedFilterLevel)
{   
    /**
    * @tc.steps: step1. set log level less or equal than the filter level.
    * @tc.expected: step1.The log filter function is not need.
    */
    LogFlowContext *context = LogFlowInitContext(TEST_ERRLOG_FLOW_CONTROL_THRESHOLD, LOG, TEST_ERRLOG_STAT_PERIOD);
    ASSERT(context != NULL);
    bool result = IsNeedLogFilter(context, &logIdentifier);
    ASSERT_FALSE(result);
    LogFlowFreeContext(context);
    context = NULL;
}

/**
 * @tc.name:  TestFlowControlTimeWhenFlowControl
 * @tc.desc:  Test if the log filter function is need.
 * @tc.type: FUNC
 */
 
TEST_F(ErrorLogFlowControlTest, TestFlowControlProcessWhenLogSpeedExceedThresholdThatFlowControlIsNeed)
{   
    /**
    * @tc.steps: step1. set log level less or equal than the filter level.
    * @tc.expected: step1.The log filter function is not need.
    */
#define TEST_ERRLOG_PRROCESS_THRESHOLD 10
#define TEST_ERRLOG_FLOW_CONTROL_MSG "Flow control test."
#define TEST_ERRLOG_MSG_COUNT 12
#define TEST_ERRLOG_PRROCESS_STAT_PERIOD 1000
 
    uint64_t startTime, endTime;
    SetErrLogWriteMode(false);
    SetErrLogServerLevel(INFO);
    SetErrLogDirectory(ERROR_LOG_DEFAULT_DIRECTORY);

    /* Do not activate flow control */
    ASSERT_EQ(StartLogger(), ERROR_SYS_OK);
    ASSERT_EQ(OpenLogger(), ERROR_SYS_OK);
    startTime = (uint64_t)(GetCurrentTimeValue().seconds * 1000 + GetCurrentTimeValue().useconds / 1000);
    RepeatWriteLogPreTime(TEST_ERRLOG_MSG_COUNT, TEST_ERRLOG_FLOW_CONTROL_MSG);
    FlushLogger();
    endTime = (uint64_t)(GetCurrentTimeValue().seconds * 1000 + GetCurrentTimeValue().useconds / 1000);
    CloseLogger();
    StopLogger();
    uint32_t timeSpan = (uint32_t)(endTime - startTime);

    /* Activate flow control */
    bool ret = SetErrLogFlowConfig(TEST_ERRLOG_PRROCESS_THRESHOLD, LOG, TEST_ERRLOG_PRROCESS_STAT_PERIOD);
    ASSERT_TRUE(ret);
    ASSERT_EQ(StartLogger(), ERROR_SYS_OK);
    ASSERT_EQ(OpenLogger(), ERROR_SYS_OK);
    startTime = (uint64_t)(GetCurrentTimeValue().seconds * 1000 + GetCurrentTimeValue().useconds / 1000);
    RepeatWriteLogPreTime(TEST_ERRLOG_MSG_COUNT, TEST_ERRLOG_FLOW_CONTROL_MSG);
    FlushLogger();
    endTime = (uint64_t)(GetCurrentTimeValue().seconds * 1000 + GetCurrentTimeValue().useconds / 1000);
    CloseLogger();
    StopLogger();
    uint32_t flowControlTimeSpan = (uint32_t)(endTime - startTime);

    ASSERT_GE(flowControlTimeSpan / 1000, timeSpan / 1000);
}

/**
 * @tc.name:  TestFlowControlProcessWhenFlowControlIsNeedThatFilter
 * @tc.desc:  Test if the log filter function is need.
 * @tc.type: FUNC
 */
 
TEST_F(ErrorLogFlowControlTest, TestFlowControlProcessWhenFlowControlIsNeedThatLogFilterIsActivated)
{   
    /**
    * @tc.steps: step1. set log level less or equal than the filter level.
    * @tc.expected: step1.The log filter function is not need.
    */
#define TEST_ERRLOG_PRROCESS_THRESHOLD 10
#define TEST_ERRLOG_FLOW_CONTROL_MSG "Flow control test."
#define TEST_ERRLOG_MSG_COUNT 12
 
    uint64_t startTime, endTime;
    SetErrLogWriteMode(false);
    SetErrLogServerLevel(INFO);
    SetErrLogDirectory(ERROR_LOG_DEFAULT_DIRECTORY);

    /* Do not activate flow control */
    ASSERT_EQ(StartLogger(), ERROR_SYS_OK);
    ASSERT_EQ(OpenLogger(), ERROR_SYS_OK);
    startTime = (uint64_t)(GetCurrentTimeValue().seconds * 1000 + GetCurrentTimeValue().useconds / 1000);
    RepeatWriteLogPreTime(TEST_ERRLOG_MSG_COUNT, TEST_ERRLOG_FLOW_CONTROL_MSG);
    FlushLogger();
    endTime = (uint64_t)(GetCurrentTimeValue().seconds * 1000 + GetCurrentTimeValue().useconds / 1000);
    CloseLogger();
    StopLogger();
    uint32_t timeSpan = (uint32_t)(endTime - startTime);

    /* Activate flow control */
    bool ret = SetErrLogFlowConfig(TEST_ERRLOG_PRROCESS_THRESHOLD, INFO, TEST_ERRLOG_PRROCESS_STAT_PERIOD);
    ASSERT_TRUE(ret);
    ASSERT_EQ(StartLogger(), ERROR_SYS_OK);
    ASSERT_EQ(OpenLogger(), ERROR_SYS_OK);
    startTime = (uint64_t)(GetCurrentTimeValue().seconds * 1000 + GetCurrentTimeValue().useconds / 1000);
    RepeatWriteLogPreTime(TEST_ERRLOG_MSG_COUNT, TEST_ERRLOG_FLOW_CONTROL_MSG);
    FlushLogger();
    endTime = (uint64_t)(GetCurrentTimeValue().seconds * 1000 + GetCurrentTimeValue().useconds / 1000);
    CloseLogger();
    StopLogger();
    uint32_t flowControlTimeSpan = (uint32_t)(endTime - startTime);

    ASSERT_GE(flowControlTimeSpan / 1000, timeSpan / 1000);

}