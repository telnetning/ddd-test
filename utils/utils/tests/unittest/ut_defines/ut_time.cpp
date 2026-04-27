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
 * Description:
 * 1. unit tests of memory_allocator.c
 *
 * ---------------------------------------------------------------------------------
 */

#include <gtest/gtest.h>
#include "defines/time.h"

class TimeTest : public testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}
};

TEST_F(TimeTest, TestGetTimeMsecWhenTypeValidThenSucceed)
{
    ErrorCode errorCode;
    uint64_t retTime;

    retTime = GetTimeMsec(EPOCH_TIME, &errorCode);
    EXPECT_EQ(errorCode, ERROR_SYS_OK);

    retTime = GetTimeMsec(BOOT_TIME, &errorCode);
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
}

TEST_F(TimeTest, TestGetTimeMsecWhenErrCodeIsNULLThenSucceed)
{
    uint64_t retTime = GetTimeMsec(BOOT_TIME, NULL);
    ASSERT_NE(retTime, INVALID_TIME_VALUE);
}

TEST_F(TimeTest, TestGetTimeMsecWhenTypeInvalidThenFail)
{
    ErrorCode errorCode;
    uint64_t retTime = GetTimeMsec(TimeType(BOOT_TIME+1), &errorCode);

    ASSERT_EQ(errorCode, UTILS_ERROR_TIME_UNKNOWN_TIMETYPE);
    ASSERT_EQ(retTime, INVALID_TIME_VALUE);
}
