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
#include <gmock/gmock.h>
#include <unordered_map>
#include "defines/err_code.h"

/* at .h file */
#define UT_TEST_COMPONENT_ID          0x15
#define UT_TEST_MODULE_ID             0x11
/* make a error code and error code descript infomation */
#define UT_TEST_ERROR_CODE1           MAKE_ERROR_CODE(UT_TEST_COMPONENT_ID, UT_TEST_MODULE_ID, 0x0)
#define UT_TEST_ERROR_CODE1_DESC_INFO "[ut] ut test error code1!"
#define UT_TEST_ERROR_CODE2           MAKE_ERROR_CODE(UT_TEST_COMPONENT_ID, UT_TEST_MODULE_ID, 0x1)
#define UT_TEST_ERROR_CODE2_DESC_INFO "[ut] ut test error code2!"
#define UT_TEST_ERROR_CODE3           MAKE_ERROR_CODE(UT_TEST_COMPONENT_ID, UT_TEST_MODULE_ID, 0x2)
#define UT_TEST_ERROR_CODE3_DESC_INFO "[ut] ut test error code3!"

/* at .c or .cpp file */
DECLEAR_ERROR_CODE_DESC_BEGIN(UT_TEST_COMPONENT_ID, UT_TEST_MODULE_ID) /* begin */
DECLEAR_ERROR_CODE_DESC(UT_TEST_ERROR_CODE1, UT_TEST_ERROR_CODE1_DESC_INFO)
DECLEAR_ERROR_CODE_DESC(UT_TEST_ERROR_CODE2, UT_TEST_ERROR_CODE2_DESC_INFO)
/* UT_TEST_ERROR_CODE3 not establish map relationship */
DECLEAR_ERROR_CODE_DESC_END(UT_TEST_COMPONENT_ID, UT_TEST_MODULE_ID) /* end */

#define UT_TEST_MODULE2_ID            0x12
#define UT_TEST_ERROR_CODE4           MAKE_ERROR_CODE(UT_TEST_COMPONENT_ID, UT_TEST_MODULE2_ID, 0x1000)
#define UT_TEST_ERROR_CODE4_DESC_INFO "[ut] ut test error code4!"
#define UT_TEST_ERROR_CODE5           MAKE_ERROR_CODE(UT_TEST_COMPONENT_ID, UT_TEST_MODULE2_ID, 0x1001)
#define UT_TEST_ERROR_CODE5_DESC_INFO "[ut] ut test error code5!"

class ErrorCodeTransferTest : public testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}
};

static const char *MyCallback(uint64_t errorCode)
{
    static std::unordered_map<ErrorCode, std::string> codeMap = {
        {UT_TEST_ERROR_CODE4, UT_TEST_ERROR_CODE4_DESC_INFO},
        {UT_TEST_ERROR_CODE5, UT_TEST_ERROR_CODE5_DESC_INFO},
    };
    return codeMap[errorCode].c_str();
}

using std::string;
using ::testing::HasSubstr;
using ::testing::Not;
TEST_F(ErrorCodeTransferTest, GetErrorCodeInfo)
{
    EXPECT_THAT(string(GetErrorCodeInfo(UT_TEST_ERROR_CODE1)), HasSubstr(UT_TEST_ERROR_CODE1_DESC_INFO));
    EXPECT_THAT(string(GetErrorCodeInfo(UT_TEST_ERROR_CODE2)), HasSubstr(UT_TEST_ERROR_CODE2_DESC_INFO));
    EXPECT_THAT(string(GetErrorCodeInfo(UT_TEST_ERROR_CODE3)), Not(UT_TEST_ERROR_CODE3_DESC_INFO));
}

TEST_F(ErrorCodeTransferTest, RegisterErrorDescInfoCallback)
{
    RegisterErrorDescInfoCallback(UT_TEST_COMPONENT_ID, UT_TEST_MODULE2_ID, MyCallback);
    EXPECT_THAT(string(GetErrorCodeInfo(UT_TEST_ERROR_CODE4)), HasSubstr(UT_TEST_ERROR_CODE4_DESC_INFO));
    EXPECT_THAT(string(GetErrorCodeInfo(UT_TEST_ERROR_CODE5)), HasSubstr(UT_TEST_ERROR_CODE5_DESC_INFO));
}
