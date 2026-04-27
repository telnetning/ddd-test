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
#include "syslog/err_log.h"
#include "syslog/err_log_fold.h"

class ErrorLogFoldTest : public testing::Test {
protected:
    void SetUp() override
    {
        foldContext = LogFoldAllocContext(nullptr, WARNING, 1, 1);
        ASSERT_NE(foldContext, nullptr);
    }

    void TearDown() override
    {
        LogFoldFreeContext(foldContext);
    }

    LogFoldContext *foldContext = nullptr;
};

/**
 * Fold log configure parameters test
 */
TEST_F(ErrorLogFoldTest, CheckConfigureTest001)
{
    EXPECT_TRUE(LogFoldSetRule(foldContext, DEBUG, 0, 0));
    EXPECT_TRUE(LogFoldSetRule(foldContext, WARNING, 0, 0));
    EXPECT_FALSE(LogFoldSetRule(foldContext, ERROR, 0, 0));
    EXPECT_FALSE(LogFoldSetRule(foldContext, PANIC, 0, 0));
    EXPECT_FALSE(LogFoldSetRule(foldContext, 0, 0, 0));
    EXPECT_FALSE(LogFoldSetRule(foldContext, -1, 0, 0));
    EXPECT_FALSE(LogFoldSetRule(foldContext, PANIC + 1, 0, 0));
    constexpr uint32_t maxFoldPeriod = 60 * 60;
    EXPECT_TRUE(LogFoldSetRule(foldContext, DEBUG, 0, maxFoldPeriod));
    EXPECT_FALSE(LogFoldSetRule(foldContext, DEBUG, 0, maxFoldPeriod + 1));
}

/**
 * Add 3 log to fold log context in 1 fold period, with fold threshold = 1
 * First log will not be folded, others will be folded
 * Add 1 log to fold log context in another fold period
 * This log will not be folded, output previous fold log with fold count = 2
 */
TEST_F(ErrorLogFoldTest, AddFoldLogTest001)
{
    char msg1[] = "Test1", msg2[] = "Test2", msg3[] = "Test3", msg4[] = "Test4";
    LogIdentifier logIdentifier;
    logIdentifier.threadId = 1;
    logIdentifier.lineNum = 1;
    logIdentifier.logLevel = DEBUG;
    logIdentifier.timeSecond = 1;
    (void)memset_s(logIdentifier.fileName, sizeof(logIdentifier.fileName), 0, sizeof(logIdentifier.fileName));
    (void)strncpy_s(logIdentifier.fileName, sizeof(logIdentifier.fileName), __FILE__, sizeof(logIdentifier.fileName) - 1);
    LogFoldContent logFoldContent;
    bool res = LogFoldIsNeedFold(foldContext, &logIdentifier, msg1, strlen(msg1), &logFoldContent);
    ASSERT_FALSE(res);
    ASSERT_EQ(logFoldContent.foldCount, 0);
    ASSERT_EQ(logFoldContent.msgBuf, nullptr);
    /* Fold threshold = 1, second log will be folded */
    res = LogFoldIsNeedFold(foldContext, &logIdentifier, msg2, strlen(msg2), &logFoldContent);
    ASSERT_TRUE(res);
    ASSERT_EQ(logFoldContent.foldCount, 0);
    ASSERT_EQ(logFoldContent.msgBuf, nullptr);
    /* third log will be folded and return second log in output parameters */
    res = LogFoldIsNeedFold(foldContext, &logIdentifier, msg3, strlen(msg3), &logFoldContent);
    ASSERT_TRUE(res);
    ASSERT_EQ(logFoldContent.foldCount, 0);
    ASSERT_EQ(logFoldContent.msgBuf, msg2);
    /* Change log time to next fold period, return third log in output parameters and do not fold */
    logIdentifier.timeSecond = 3;
    res = LogFoldIsNeedFold(foldContext, &logIdentifier, msg4, strlen(msg4), &logFoldContent);
    ASSERT_FALSE(res);
    ASSERT_EQ(logFoldContent.msgBuf, msg3);
    ASSERT_EQ(logFoldContent.foldCount, 2);
    ASSERT_EQ(logFoldContent.msgLen, strlen(msg3));
}

/**
 * Test update fold log configure in running time
 * Add 2 log to fold log context in 1 fold period, with fold threshold = 1, second log will be folded
 * Change fold threshold = 0, add 1 log in same fold period, this log will not be folded
 */
TEST_F(ErrorLogFoldTest, UpdateLogFoldConfigureTest001)
{
    char msg[] = "Test";
    LogIdentifier logIdentifier;
    logIdentifier.threadId = 1;
    logIdentifier.lineNum = 1;
    logIdentifier.logLevel = DEBUG;
    logIdentifier.timeSecond = 1;
    (void)memset_s(logIdentifier.fileName, sizeof(logIdentifier.fileName), 0, sizeof(logIdentifier.fileName));
    (void)strncpy_s(logIdentifier.fileName, sizeof(logIdentifier.fileName), __FILE__, sizeof(logIdentifier.fileName) - 1);
    LogFoldContent logFoldContent;
    ASSERT_FALSE(LogFoldIsNeedFold(foldContext, &logIdentifier, msg, strlen(msg), &logFoldContent));
    /* second log will be folded */
    ASSERT_TRUE(LogFoldIsNeedFold(foldContext, &logIdentifier, msg, strlen(msg), &logFoldContent));
    /* Disable fold log */
    LogFoldSetRule(foldContext, WARNING, 0, 1);
    ASSERT_FALSE(LogFoldIsNeedFold(foldContext, &logIdentifier, msg, strlen(msg), &logFoldContent));
    /* Enable fold log */
    LogFoldSetRule(foldContext, WARNING, 1, 1);
    ASSERT_TRUE(LogFoldIsNeedFold(foldContext, &logIdentifier, msg, strlen(msg), &logFoldContent));
}

/**
 * Get fold log exceed fold period base on time
 * Add 2 different log to fold log in same fold period
 * Get fold log in same fold period, return 0 fold log content
 * Get fold log in next fold period, return 2 fold log contents
 * Get fold log in next fold period, return 0 fold log content
 */
TEST_F(ErrorLogFoldTest, GetFoldLogContentTest001)
{
    char msg[] = "Test";
    LogIdentifier logIdentifier1;
    logIdentifier1.threadId = 1;
    logIdentifier1.lineNum = 1;
    logIdentifier1.logLevel = DEBUG;
    logIdentifier1.timeSecond = 1;
    (void)memset_s(logIdentifier1.fileName, sizeof(logIdentifier1.fileName), 0, sizeof(logIdentifier1.fileName));
    (void)strncpy_s(logIdentifier1.fileName, sizeof(logIdentifier1.fileName), __FILE__, sizeof(logIdentifier1.fileName) - 1);
    LogIdentifier logIdentifier2;
    logIdentifier2.threadId = 2;
    logIdentifier2.lineNum = 1;
    logIdentifier2.logLevel = DEBUG;
    logIdentifier2.timeSecond = 1;
    (void)memset_s(logIdentifier2.fileName, sizeof(logIdentifier2.fileName), 0, sizeof(logIdentifier2.fileName));
    (void)strncpy_s(logIdentifier2.fileName, sizeof(logIdentifier2.fileName), __FILE__, sizeof(logIdentifier2.fileName) - 1);
    LogFoldContent logFoldContent;
    ASSERT_FALSE(LogFoldIsNeedFold(foldContext, &logIdentifier1, msg, strlen(msg), &logFoldContent));
    ASSERT_FALSE(LogFoldIsNeedFold(foldContext, &logIdentifier2, msg, strlen(msg), &logFoldContent));
    ASSERT_TRUE(LogFoldIsNeedFold(foldContext, &logIdentifier1, msg, strlen(msg), &logFoldContent));
    ASSERT_TRUE(LogFoldIsNeedFold(foldContext, &logIdentifier2, msg, strlen(msg), &logFoldContent));
    LogFoldContent foldContentArray[2];
    uint64_t foldLogCount = FoldLogGetContent(foldContext, true, 2, foldContentArray, 2);
    ASSERT_EQ(foldLogCount, 0);
    foldLogCount = FoldLogGetContent(foldContext, true, 3, foldContentArray, 2);
    ASSERT_EQ(foldLogCount, 2);
    for (uint64_t i = 0; i < foldLogCount; ++i) {
        ASSERT_EQ(foldContentArray[i].msgBuf, msg);
        ASSERT_EQ(foldContentArray[i].msgLen, strlen(msg));
        ASSERT_EQ(foldContentArray[i].foldCount, 1);
    }
    foldLogCount = FoldLogGetContent(foldContext, true, 3, foldContentArray, 2);
    ASSERT_EQ(foldLogCount, 0);
}

/**
 * Get fold log exceed fold threshold regardless of time
 * Add 1 fold log to fold context with fold count = 2
 * Get 1 fold content of this log with fold count = 2
 * Get 0 fold content in next call
 */
TEST_F(ErrorLogFoldTest, GetFoldLogContentTest002)
{
    char msg[] = "Test";
    LogIdentifier logIdentifier;
    logIdentifier.threadId = 1;
    logIdentifier.lineNum = 1;
    logIdentifier.logLevel = DEBUG;
    logIdentifier.timeSecond = 1;
    (void)memset_s(logIdentifier.fileName, sizeof(logIdentifier.fileName), 0, sizeof(logIdentifier.fileName));
    (void)strncpy_s(logIdentifier.fileName, sizeof(logIdentifier.fileName), __FILE__, sizeof(logIdentifier.fileName) - 1);
    LogFoldContent logFoldContent;
    ASSERT_FALSE(LogFoldIsNeedFold(foldContext, &logIdentifier, msg, strlen(msg), &logFoldContent));
    ASSERT_TRUE(LogFoldIsNeedFold(foldContext, &logIdentifier, msg, strlen(msg), &logFoldContent));
    ASSERT_TRUE(LogFoldIsNeedFold(foldContext, &logIdentifier, msg, strlen(msg), &logFoldContent));
    uint64_t foldLogCount = FoldLogGetContent(foldContext, false, 0, &logFoldContent, 1);
    ASSERT_EQ(foldLogCount, 1);
    ASSERT_EQ(logFoldContent.msgBuf, msg);
    ASSERT_EQ(logFoldContent.msgLen, strlen(msg));
    ASSERT_EQ(logFoldContent.foldCount, 2);
    foldLogCount = FoldLogGetContent(foldContext, false, 0, &logFoldContent, 1);
    ASSERT_EQ(foldLogCount, 0);
}

/**
 * Get fold log exceed fold threshold regardless of time
 * Add 1 fold log to fold context with fold count = 2
 * Get 0 fold content of this log, ERROR log do not need be folded.
 */
TEST_F(ErrorLogFoldTest, IsLogNeedFoldTest001)
{
    char msg[] = "Test";
    LogIdentifier logIdentifier;
    logIdentifier.threadId = 1;
    logIdentifier.lineNum = 1;
    logIdentifier.logLevel = PANIC;
    logIdentifier.timeSecond = 1;
    (void)memset_s(logIdentifier.fileName, sizeof(logIdentifier.fileName), 0, sizeof(logIdentifier.fileName));
    (void)strncpy_s(logIdentifier.fileName, sizeof(logIdentifier.fileName), __FILE__, sizeof(logIdentifier.fileName) - 1);
    LogFoldContent logFoldContent;
    /* The first log */
    ASSERT_FALSE(LogFoldIsNeedFold(foldContext, &logIdentifier, msg, strlen(msg), &logFoldContent));
    /* The second identical log */
    ASSERT_FALSE(LogFoldIsNeedFold(foldContext, &logIdentifier, msg, strlen(msg), &logFoldContent));
    /* The third identical log */
    ASSERT_FALSE(LogFoldIsNeedFold(foldContext, &logIdentifier, msg, strlen(msg), &logFoldContent));
    uint64_t foldLogCount = FoldLogGetContent(foldContext, false, 0, &logFoldContent, 1);
    ASSERT_EQ(foldLogCount, 0);
}

/**
 * Get fold log exceed fold threshold regardless of time
 * Add 1 fold log to fold context with fold count = 2
 * Get 1 fold content of this log with fold count = 2, WARNING log need be folded.
 */
TEST_F(ErrorLogFoldTest, IsLogNeedFoldTest002)
{
    char msg[] = "Test";
    LogIdentifier logIdentifier;
    logIdentifier.threadId = 1;
    logIdentifier.lineNum = 1;
    logIdentifier.logLevel = WARNING;
    logIdentifier.timeSecond = 1;
    (void)memset_s(logIdentifier.fileName, sizeof(logIdentifier.fileName), 0, sizeof(logIdentifier.fileName));
    (void)strncpy_s(logIdentifier.fileName, sizeof(logIdentifier.fileName), __FILE__, sizeof(logIdentifier.fileName) - 1);
    LogFoldContent logFoldContent;
    /* The first log */
    ASSERT_FALSE(LogFoldIsNeedFold(foldContext, &logIdentifier, msg, strlen(msg), &logFoldContent));
    /* The second identical log */
    ASSERT_TRUE(LogFoldIsNeedFold(foldContext, &logIdentifier, msg, strlen(msg), &logFoldContent));
    /* The third identical log */
    ASSERT_TRUE(LogFoldIsNeedFold(foldContext, &logIdentifier, msg, strlen(msg), &logFoldContent));
    uint64_t foldLogCount = FoldLogGetContent(foldContext, false, 0, &logFoldContent, 1);
    ASSERT_EQ(foldLogCount, 1);
    ASSERT_EQ(logFoldContent.msgBuf, msg);
    ASSERT_EQ(logFoldContent.msgLen, strlen(msg));
    ASSERT_EQ(logFoldContent.foldCount, 2);
}