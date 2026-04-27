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
#include <string>
#include <thread>
#include <regex>

#include "syslog/trace.h"
#include "syslog/err_log.h"
#include "syslog/err_log_internal.h"
#include "defines/time.h"
#include "ut_err_log_common.h"

#define ARRAY_SIZE(a) sizeof(a)/sizeof(a[0])

#undef TRACE_ENABLE_PRINT

using namespace std;
using namespace chrono;

class TraceTest : public testing::Test {
public:
    static void SetUpTestSuite()
    {
        ErrorCode err = StartLogger();
        ASSERT_EQ(err, ERROR_SYS_OK);

        err = OpenLogger();
        ASSERT_EQ(err, ERROR_SYS_OK);
    }

    static void TearDownTestSuite()
    {
        FlushLogger();
        usleep(100);
        CloseLogger();
        StopLogger();
    }

    void SetUp() override
    {
    }

    void TearDown() override
    {
    }
};

TEST_F(TraceTest, TestBeginAndEnd)
{
    TraceChainBegin(NULL);
    TraceId id = TraceChainGetId();
    ASSERT_TRUE(TraceChainIsTraceIdValid(&id));

    TraceChainEnd();
}

TEST_F(TraceTest, TestBeginWithTraceId)
{
    TraceId id = {.chainId = 0x1122334455667788};

    TraceChainBegin(&id);
    TraceId idGet = TraceChainGetId();
    ASSERT_TRUE(TraceChainIsTraceIdValid(&idGet));

    ASSERT_EQ(0, memcmp(&id, &idGet, sizeof(TraceId)));

    TraceChainEnd();
}

TEST_F(TraceTest, TestBeginReentered)
{
    TraceChainBegin(NULL);
    TraceId id1 = TraceChainGetId();
    ASSERT_TRUE(TraceChainIsTraceIdValid(&id1));

    TraceChainBegin(NULL);
    TraceId id2 = TraceChainGetId();
    ASSERT_TRUE(TraceChainIsTraceIdValid(&id2));

    ASSERT_NE(0, memcmp(&id1, &id2, sizeof(TraceId)));

    TraceChainEnd();
}

TEST_F(TraceTest, TestGetAndSet)
{
    TraceChainBegin(NULL);

    TraceId id1 = TraceChainGetId();
    ASSERT_TRUE(TraceChainIsTraceIdValid(&id1));

    TraceChainSetId(&id1);
    TraceId id2 = TraceChainGetId();

    ASSERT_EQ(0, memcmp(&id1, &id2, sizeof(TraceId)));

    TraceChainEnd();
}

TEST_F(TraceTest, TestSetAndClear)
{
    TraceChainBegin(NULL);

    TraceId idValid = TraceChainGetId();
    ASSERT_TRUE(TraceChainIsTraceIdValid(&idValid));

    TraceChainSetId(&idValid);

    idValid = TraceChainGetId();
    ASSERT_TRUE(TraceChainIsTraceIdValid(&idValid));

    TraceChainClearId();

    TraceId idInvalid = TraceChainGetId();
    ASSERT_FALSE(TraceChainIsTraceIdValid(&idInvalid));

    TraceChainEnd();
}

// performance test
TEST_F(TraceTest, TestLatencyOfTime)
{
    auto start = system_clock::now();
    time_t tt;
    tt = time(NULL);
    auto end  = system_clock::now();
    auto duration = duration_cast<nanoseconds>(end - start);

#ifdef TRACE_ENABLE_PRINT
    cout << "get time by time(), elapsed time:" << double(duration.count()) << " nanoseconds" << endl;
#endif

    start = system_clock::now();
    struct timeval tv;
    (void)gettimeofday(&tv, NULL);
    end  = system_clock::now();
    duration = duration_cast<nanoseconds>(end - start);

#ifdef TRACE_ENABLE_PRINT
    cout << "get time by gettimeofday(), elapsed time:" << double(duration.count()) << " nanoseconds" << endl;
#endif

    start = system_clock::now();
    struct timespec tp;
    uint64_t ms = GetTimeMsec(EPOCH_TIME, NULL);
    end  = system_clock::now();
    duration = duration_cast<nanoseconds>(end - start);

#ifdef TRACE_ENABLE_PRINT
    cout << "get time by GetTimeMsec(), elapsed time:" << double(duration.count()) << " nanoseconds" << endl;
#endif

#ifdef TRACE_ENABLE_PRINT
    cout << tt << " sec, by time()" << endl;
    cout << tv.tv_sec << " sec,"
        << tv.tv_usec << " usec, by gettimeofday()"
        << endl;
    cout << ms << " msec, by GetTimeMsec()" << endl;
#endif
}

TEST_F(TraceTest, TestPerfOfGenerateUID)
{
    for (int i = 0; i < 20; i++) {
        auto start = system_clock::now();
        uint64_t uid = TraceChainGenerateUID();
        auto end  = system_clock::now();
        auto duration = duration_cast<nanoseconds>(end - start);
#ifdef TRACE_ENABLE_PRINT
        cout << "No:" << i
            << ",TraceChainGenerateUID:0x"
            << hex << uid  << dec
            << ",elapsed time:" << double(duration.count()) << " nanoseconds"
            << endl;
#endif
    }
}

TEST_F(TraceTest, TestPerfOfBegin)
{
    for (int i = 0; i < 10; i++) {
        auto start = system_clock::now();
        TraceChainBegin(NULL);
        auto end  = system_clock::now();
        auto duration = duration_cast<nanoseconds>(end - start);

        TraceId id = TraceChainGetId();
#ifdef TRACE_ENABLE_PRINT
        cout << "No:" << i
            << ",TraceChainBegin:0x"
            << hex << id.chainId  << dec
            << ",elapsed time:" << double(duration.count()) << " nanoseconds"
            << endl;
#endif
        ASSERT_TRUE(TraceChainIsTraceIdValid(&id));
        TraceChainEnd();
    }
}

TEST_F(TraceTest, TestPerfOfGetId)
{
    for (int i = 0; i < 10; i++) {
        TraceChainBegin(NULL);

        auto start = system_clock::now();
        TraceId id = TraceChainGetId();
        auto end  = system_clock::now();
        auto duration = duration_cast<nanoseconds>(end - start);

#ifdef TRACE_ENABLE_PRINT
        cout << "No:" << i
            << ",TraceChainGetId:0x"
            << hex << id.chainId  << dec
            << ",elapsed time:" << double(duration.count()) << " nanoseconds"
            << endl;
#endif
        ASSERT_TRUE(TraceChainIsTraceIdValid(&id));
        TraceChainEnd();
    }
}

TEST_F(TraceTest, TestTraceLog)
{
    SetErrLogDestination(LOG_DESTINATION_LOCAL_STDERR);
    testing::internal::CaptureStderr();

    TraceChainBegin(NULL);
#define TRACE_LOG_INFO "the log contains some trace info"
    ErrLog(ERROR, ErrMsg(TRACE_LOG_INFO));
    TraceChainEnd();

    string strErrLog = testing::internal::GetCapturedStderr();
#ifdef TRACE_ENABLE_PRINT
    cout << strErrLog << endl;
#endif
    string strTraceKey = "TraceId:";
    string strReg = ".*" + strTraceKey + ".*" + TRACE_LOG_INFO + "\n";
    regex reg(strReg.c_str());
    ASSERT_TRUE(std::regex_match(strErrLog, reg));

    SetErrLogDestination(LOG_DESTINATION_LOCAL_FILE);
}

// test user scenarios
static void serviceA()
{
    TraceChainBegin(NULL);
    TraceId id = TraceChainGetId();
    ASSERT_TRUE(TraceChainIsTraceIdValid(&id));

    ErrLog(ERROR, ErrMsg("Service A, begin trace 1st."));

    TraceChainEnd();

    CloseLogger(); // release log resource in thread
}

static void serviceB()
{
    TraceChainBegin(NULL);
    TraceId id = TraceChainGetId();
    ASSERT_TRUE(TraceChainIsTraceIdValid(&id));

    ErrLog(ERROR, ErrMsg("Service B, begin trace 1st."));

    TraceChainEnd();

    TraceChainBegin(NULL);
    id = TraceChainGetId();
    ASSERT_TRUE(TraceChainIsTraceIdValid(&id));
    ErrLog(ERROR, ErrMsg("Service B, begin trace 2nd."));
    TraceChainEnd();

    CloseLogger(); // release log resource in thread
}

TEST_F(TraceTest, TestTraceInMultipleService)
{
    thread threads[2];
    threads[0] = thread(serviceA);
    threads[1] = thread(serviceB);

    threads[0].join();
    threads[1].join();
}

