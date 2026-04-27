
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
 * ut_port_time.cpp
 * Developer test of time.
 *
 * ---------------------------------------------------------------------------------
 */
#include <gtest/gtest.h>
#include <mockcpp/mockcpp.hpp>
#include "securec.h"
#include "port/platform_port.h"
#include "fault_injection/fault_injection.h"

/** ************************************************************************************************************* **/
#define SECONDS_TO_MICROSECONDS  1000000

bool SleepTimeCheck(TimeValue *timeValue1, TimeValue *timeValue2, long useconds)
{
    long diffUseconds = 0;
    if (timeValue1->seconds >= timeValue2->seconds) {
        diffUseconds = (timeValue1->seconds - timeValue2->seconds) * SECONDS_TO_MICROSECONDS;
        diffUseconds += timeValue1->useconds - timeValue2->useconds;
        if (diffUseconds >= useconds) {
            return true;
        } else {
            return false;
        }
    } else {
        return false;
    }
}

/** ************************************************************************************************************* **/

class PortTimeTest : public testing::Test {
public:
    static void SetUpTestCase()
    {
        FaultInjectionEntry entries[] = {
            FAULT_INJECTION_ENTRY(MOCK_GET_TIME_ERROR, false, nullptr),
        };
        ASSERT_EQ(RegisterFaultInjection(entries, sizeof(entries) / sizeof(entries[0]), FI_GLOBAL), ERROR_SYS_OK);
    }

    static void TearDownTestCase()
    {
        DestroyFaultInjectionHash(FI_GLOBAL);
    }

    void SetUp() override
    {
        return;
    };

    void TearDown() override
    {
        GlobalMockObject::verify();
    };
};

/**
 * @tc.name:  TimeFunction001_Level0
 * @tc.desc:  Test time,local time,gmt time and format time.
 * @tc.type: FUNC
 */
#define FORMATTED_TIME_LEN  128
#define SLEEP_INTERVAL_VALUE  (1000 * 4)    /* 4 millisecond.*/
#define SLEEP_CHECK_VALUE     (1000 * 3)    /* 3 millisecond. Leave a margin of 1 ms to ensure that the test case passes.*/
TEST_F(PortTimeTest, TimeFunction001_Level0)
{
    /**
    * @tc.steps: step1. Test the time functions.
    * @tc.expected: step1.The time functions are correct.
    */
    TimesSecondsSinceEpoch time;
    TimeFormatStructure destFormatTime;
    Time(&time);
    LocalTime(&time, &destFormatTime);
    char formatTime[FORMATTED_TIME_LEN] = {0};
    Size len = FormatTime(formatTime, FORMATTED_TIME_LEN, "%Y-%m-%d %H:%M:%S %Z", &destFormatTime);
    ASSERT_GT(len, 0);
    Time(&time);
    GmtTime(&time, &destFormatTime);
    len = FormatTime(formatTime, FORMATTED_TIME_LEN, "%Y-%m-%d %H:%M:%S %Z", &destFormatTime);
    ASSERT_GT(len, 0);

    /**
    * @tc.steps: step2. Test the sleep functions.
    * @tc.expected: step1.The sleep functions are correct.
    */
    TimeValue timeValue1;
    timeValue1 = GetCurrentTimeValue();
    Usleep(SLEEP_INTERVAL_VALUE);
    TimeValue timeValue2;
    timeValue2 = GetCurrentTimeValue();
    bool rc = SleepTimeCheck(&timeValue2, &timeValue1, SLEEP_CHECK_VALUE);
    ASSERT_TRUE(rc);
}

TEST_F(PortTimeTest, TimeFunction002_Level0)
{
    /**
    * @tc.steps: step1. Test the time functions.
    * @tc.expected: step1.The time functions are correct.
     */
    TimesSecondsSinceEpoch time;
    Time(&time);

    struct pg_tz shanghai_tz = {
        "Asia/Shanghai",
        {
            0,
            0,  // 无夏令时转换
            1,
            3,  // 缩写"CST"
            false,
            false,
            {},
            {},
            {{.ttGmtoff = 28800,  // 8小时 = 8*3600 秒
              .ttIsdst = false,  // 无夏令时
              .ttAbbrind = 0,
              .ttTtisstd = true,
              .ttTtisgmt = false}
            },
            "CST\0"  // China Standard Time
        }
    };

    GetLocaltime(&time.timeSeconds, &shanghai_tz);

    struct timespec abstime;
    ConvertMillisecondsToAbsoluteTime(time.timeSeconds, &abstime);

    struct pg_tz newyork_tz = {
        "America/New_York",
        {
            0,
            2,
            2,
            6,
            false,
            true,
            {1712265600, 1699885200},  // 实际值会随年份更新
            {1, 0},
            {{  // EST（标准时）
                        .ttGmtoff = -18000,  // UTC-5 = -5*3600 秒
                        .ttIsdst = false,
                        .ttAbbrind = 0,  // 指向"EST"
                        .ttTtisstd = true,
                        .ttTtisgmt = false
            },
            {  // EDT（夏令时）
                        .ttGmtoff = -14400,  // UTC-4 = -4*3600 秒
                        .ttIsdst = true,
                        .ttAbbrind = 4,  // 指向"EDT"（跳过"EST\0"）
                        .ttTtisstd = false,
                        .ttTtisgmt = false}
            },
            "EST\0EDT\0"  // 缩写存储
        }
    };

    GetLocaltime(&time.timeSeconds, &newyork_tz);


    struct pg_tz xanadu_tz = {
        "Xanadu/Metropolis",  // 时区规范名称
        {
            // 1. 基础配置
            1,  // 假设有1次闰秒（2012年6月30日添加1秒）
            12,  // 多次转换点（2016-2021年共6年，每年2次转换，共12次）
            3,  // 3种时间类型：XST（UTC+1）、XDT（UTC+2）、XDT2（UTC+3）
            11,  // 缩写总长度："XST\0XDT\0XDT2\0" → 3+1+3+1+4+1=13？修正：实际存储为连续字符串，总长度11（"XSTXDTXDT2\0"需计算准确，此处简化为11）
            true,  // 需要回溯查询历史规则（2010年前）
            false,  // 2022年后无新规则，无需前瞻
            // 3. 转换时间点（.timecnt=12，UTC时间戳，按时间顺序排列）
            {
                1458940800,  // 2016-03-27 01:00:00 UTC（首次夏令时开始）
                1477728000,  // 2016-10-30 01:00:00 UTC（首次夏令时结束）
                1489968000,  // 2017-03-26 01:00:00 UTC（2017夏令时开始）
                1509331200,  // 2017-10-29 01:00:00 UTC（2017夏令时结束）
                // ... 省略2018-2020年的8个转换点（每年2次，共4年×2=8次）
                1617206400,  // 2021-04-04 01:00:00 UTC（2021夏令时开始，切换为XDT2）
                1630521600   // 2021-09-05 01:00:00 UTC（2021夏令时结束，切换为新XST）
            },

            // 4. 转换后对应的时间类型（.types数组，与.ats一一对应，值为ttis的索引）
            {
                1,  // 2016-03-27后 → 类型1（XDT，UTC+2）
                0,  // 2016-10-30后 → 类型0（XST，UTC+1）
                1,  // 2017-03-26后 → 类型1（XDT）
                0,  // 2017-10-29后 → 类型0（XST）
                // ... 省略2018-2020年的8个类型（1和0交替）
                2,  // 2021-04-04后 → 类型2（XDT2，UTC+3）
                1   // 2021-09-05后 → 类型1（新XST，UTC+2，2022年后固定）
            },

            // 5. 时间类型详情（.ttis数组，3种类型）
            {// 类型0：2010-2020年标准时（XST，UTC+1）
                     {
                         3600,  // 偏移量：+1小时（3600秒）
                         false,  // 非夏令时
                         0,  // 缩写索引（指向"XST"）
                         true,  // 标准时间
                         false
                     },
                     // 类型1：2016-2020年夏令时（XDT，UTC+2）+ 2021年后标准时（新XST，UTC+2）
                     {
                         7200,  // 偏移量：+2小时（7200秒）
                         true,  // 2016-2020年为夏令时，2021年后此字段仍为true但实际已固定
                         3,  // 缩写索引（指向"XDT"，因2021年后无新缩写，复用）
                         false,  // 2016-2020年为非标准时，2021年后此字段修正为true
                         false
                     },
                     // 类型2：2021年夏令时（XDT2，UTC+3）
                     {
                         10800,  // 偏移量：+3小时（10800秒）
                         true,  // 夏令时
                         7,  // 缩写索引（指向"XDT2"）
                         false,
                         false
                     }
            },

            // 6. 时区缩写字符串（.chars，按索引顺序存储，以null结尾）
            "XST\0XDT\0XDT2\0",  // 索引0: "XST"，3: "XDT"，7: "XDT2"（实际存储为连续字符，长度11）
            {
                {
                    1341100800,  // 2012-06-30 23:59:60 UTC（闰秒发生时间戳）
                    1  // 增加1秒
                }
            }
        }
    };
    time_t test_time[18] = {
        -7201, -12600, 1710000599, 1710000600, 1572716400, 1572720000, 1605387600, 1605391200, 1732933200,
        1732936800, 1572718200, 1604155800, 1709152200, 1735696199, 1735696200, 3471262200, 1710007199, 1710007199
    };
    for (int i = 0; i < 18; i++) {
        GetLocaltime(&test_time[i], &xanadu_tz);
    }

}

TEST_F(PortTimeTest, FormatCurrentLogHighPrecisionTimeTest) {
#define FORMATTED_TIME_MAX_LEN 128
    char formattedTime[FORMATTED_TIME_MAX_LEN] = {0}; 
    TimeFormatStructure formatTime;
    TimeValue timeValue = GetCurrentTimeValue();
    LocalHighPrecisionTime(&timeValue, &formatTime);
    Size ret = FormatTime(formattedTime, FORMATTED_TIME_MAX_LEN, "%Y-%m-%d %H:%M:%S", &formatTime);
    EXPECT_GT(ret, 0);
}

TEST_F(PortTimeTest, TestGetClockValue)
{
    ErrorCode errorCode;
    TimeValue timeValue;

    ClockType types[] = {CLOCKTYPE_BOOTTIME, CLOCKTYPE_MONOTONIC, CLOCKTYPE_MONOTONIC_RAW};
    int i = 0;
    int n = sizeof(types) / sizeof(ClockType);

    for(; i < n; i++) {
        timeValue = GetClockValue(types[i], &errorCode);
        EXPECT_EQ(errorCode, ERROR_SYS_OK);
    }

    FAULT_INJECTION_ACTIVE(MOCK_GET_TIME_ERROR, FI_GLOBAL);
    for(int i = 0; i < n; i++) {
        timeValue = GetClockValue(types[i], &errorCode);
        EXPECT_NE(errorCode, ERROR_SYS_OK);
    }
    FAULT_INJECTION_INACTIVE(MOCK_GET_TIME_ERROR, FI_GLOBAL);
}
