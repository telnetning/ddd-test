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
 * ut_string_info.cpp
 * 
 * Description:
 * 1. Developer test of string info.
 *
 * ---------------------------------------------------------------------------------
 */
#include "securec.h"
#include "gtest/gtest.h"
#include <mockcpp/mockcpp.hpp>
#include "container/string_info.h"
#include "types/data_types.h"

/** ************************************************************************************************************* **/
bool FormatString(StringInfo str, const char *fmt, ...)
{
    va_list args;
    va_start(args, fmt);
    int32_t ret = AppendStringVA(str, fmt, args);
    va_end(args);
    return (ret == 0);
}

/** ************************************************************************************************************* **/

class StringInfoTest : public testing::Test {
public:
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
 * @tc.name:  StringInfoFunction001_Level0
 * @tc.desc:  Test the string info make,destroy,append and enlarge.
 * @tc.type: FUNC
 */
/* The string info context max size. */
#define STRINGINFO_CONTEXT_MAX_SIZE   (16 * 1024 * 1024)
TEST_F(StringInfoTest, StringInfoFunction001_Level0)
{
    /**
    * @tc.steps: step1. The string info make and destroy.
    * @tc.expected: step1.The functions are correct.
    */
    MemoryContext context = MemoryContextCreate(NULL, MEM_CXT_TYPE_SHARE, "StringInfoContext",
                                                MCTX_UNUSED, MCTX_UNUSED, DEFAULT_UNLIMITED_SIZE);
    StringInfo stringInfo = MakeString(context);
    size_t maxLen = GetCapacityOfString(stringInfo);
    ASSERT_GT(maxLen, 0);
    char *data = GetCStrOfString(stringInfo);
    ASSERT_TRUE(data != NULL);
    DestroyString(stringInfo);

    /**
    * @tc.steps: step2. The string info initial and free.
    * @tc.expected: step2.The functions are correct.
    */
    StringInfoData stringInfoData;
    stringInfo = &stringInfoData;
    ASSERT_TRUE(InitString(context, stringInfo));
    maxLen = GetCapacityOfString(stringInfo);
    ASSERT_GT(maxLen, 0);
    data = GetCStrOfString(stringInfo);
    ASSERT_TRUE(data != NULL);
    FreeString(stringInfo);

    /**
    * @tc.steps: step3. The string info append, reset and reinitialize.
    * @tc.expected: step3.The functions are correct.
    */
#define STRINGINFO_TEST_STR      "This is a test string"
    ASSERT_TRUE(InitString(context, stringInfo));
    ASSERT_TRUE(AppendString(stringInfo, "%s", STRINGINFO_TEST_STR));
    data = GetCStrOfString(stringInfo);
    ASSERT_STREQ(data, STRINGINFO_TEST_STR);

    ResetString(stringInfo);
    ASSERT_TRUE(AppendStringString(stringInfo, STRINGINFO_TEST_STR));
    data = GetCStrOfString(stringInfo);
    ASSERT_STREQ(data, STRINGINFO_TEST_STR);

    ResetString(stringInfo);
    ASSERT_TRUE(AppendBinaryString(stringInfo, STRINGINFO_TEST_STR, strlen(STRINGINFO_TEST_STR)));
    data = GetCStrOfString(stringInfo);
    ASSERT_STREQ(data, STRINGINFO_TEST_STR);

    ResetString(stringInfo);
    ASSERT_TRUE(AppendBinaryStringNotTrailNull(stringInfo, STRINGINFO_TEST_STR, strlen(STRINGINFO_TEST_STR)));
    size_t len = GetLengthOfString(stringInfo);
    ASSERT_EQ(strlen(STRINGINFO_TEST_STR), len);

    ResetString(stringInfo);
    bool success = FormatString(stringInfo, "%s", STRINGINFO_TEST_STR);
    data = GetCStrOfString(stringInfo);
    ASSERT_TRUE(success);
    ASSERT_STREQ(data, STRINGINFO_TEST_STR);

#define STRINGINFO_TEST_CHAR    'A'
#define STRINGINFO_TEST_CHAR_RESULT "AAA"
    ResetString(stringInfo);
    ASSERT_TRUE(AppendStringChar(stringInfo, STRINGINFO_TEST_CHAR));
    ASSERT_TRUE(AppendStringChar(stringInfo, STRINGINFO_TEST_CHAR));
    ASSERT_TRUE(AppendStringChar(stringInfo, STRINGINFO_TEST_CHAR));
    data = GetCStrOfString(stringInfo);
    ASSERT_STREQ(data, STRINGINFO_TEST_CHAR_RESULT);

    ASSERT_TRUE(ReInitString(stringInfo));
    ASSERT_TRUE(AppendStringCharFast(stringInfo, STRINGINFO_TEST_CHAR));
    ASSERT_TRUE(AppendStringCharFast(stringInfo, STRINGINFO_TEST_CHAR));
    ASSERT_TRUE(AppendStringCharFast(stringInfo, STRINGINFO_TEST_CHAR));
    data = GetCStrOfString(stringInfo);
    ASSERT_STREQ(data, STRINGINFO_TEST_CHAR_RESULT);

#define STRINGINFO_TEST_SPACES_COUNT   3
#define STRINGINFO_TEST_SPACES_RESULT  "   "
    ASSERT_TRUE(ReInitString(stringInfo));
    ASSERT_TRUE(AppendStringSpaces(stringInfo, STRINGINFO_TEST_SPACES_COUNT));
    data = GetCStrOfString(stringInfo);
    ASSERT_STREQ(data, STRINGINFO_TEST_SPACES_RESULT);

    /**
    * @tc.steps: step4. The string info copy and transfer.
    * @tc.expected: step4.The initial functions are correct.
    */
    ResetString(stringInfo);
    ASSERT_TRUE(AppendStringString(stringInfo, STRINGINFO_TEST_STR));
    TransferString(&data, stringInfo);
    ASSERT_STREQ(data, STRINGINFO_TEST_STR);
    char *currentData = GetCStrOfString(stringInfo);
    ASSERT_TRUE(currentData == NULL);
    FreeStringData(data);

    ASSERT_TRUE(InitString(context, stringInfo));
    ASSERT_TRUE(AppendStringString(stringInfo, STRINGINFO_TEST_STR));
    StringInfoData toStringData;
    StringInfo toString = &toStringData;
    ASSERT_TRUE(InitString(context, toString));
    ASSERT_TRUE(CopyString(toString, stringInfo));
    char *fromData = GetCStrOfString(stringInfo);
    char *toData = GetCStrOfString(toString);
    ASSERT_STREQ(fromData, toData);
    FreeString(toString);
    FreeString(stringInfo);

    /**
    * @tc.steps: step5. The string enlarge.
    * @tc.expected: step5.The functions are correct.
    */
#define STRINGINFO_ENLARGE_MIN_LEN  1
    ASSERT_TRUE(InitString(context, stringInfo));
    size_t beforeLen = GetCapacityOfString(stringInfo);
    ASSERT_TRUE(EnlargeString(stringInfo, beforeLen + STRINGINFO_ENLARGE_MIN_LEN));
    size_t afterLen = GetCapacityOfString(stringInfo);
    ASSERT_GT(afterLen, beforeLen);
    FreeString(stringInfo);

    /**
    * @tc.steps: step6. The string info left trim and right trim.
    * @tc.expected: step6.The functions are correct.
    */
#define STRINGINFO_TEST_STR_LEFT_TRIM      "    This is a test string"
#define STRINGINFO_TEST_STR_RIGHT_TRIM      "This is a test string    "
    ASSERT_TRUE(InitString(context, stringInfo));
    ASSERT_TRUE(AppendString(stringInfo, "%s", STRINGINFO_TEST_STR_LEFT_TRIM));
    LeftTrimString(stringInfo);
    data = GetCStrOfString(stringInfo);
    ASSERT_STREQ(data, STRINGINFO_TEST_STR);
    FreeString(stringInfo);

    ASSERT_TRUE(InitString(context, stringInfo));
    ASSERT_TRUE(AppendString(stringInfo, "%s", STRINGINFO_TEST_STR_RIGHT_TRIM));
    RightTrimString(stringInfo);
    data = GetCStrOfString(stringInfo);
    ASSERT_STREQ(data, STRINGINFO_TEST_STR);
    FreeString(stringInfo);

#define C_STRING_TRIM_TEST_LEN   100
    char leftTrimString[C_STRING_TRIM_TEST_LEN] = STRINGINFO_TEST_STR_LEFT_TRIM;
    LeftTrim(leftTrimString);
    ASSERT_STREQ(leftTrimString, STRINGINFO_TEST_STR);

    char rightTrimString[C_STRING_TRIM_TEST_LEN] = STRINGINFO_TEST_STR_RIGHT_TRIM;
    RightTrim(rightTrimString);
    ASSERT_STREQ(rightTrimString, STRINGINFO_TEST_STR);

    MemoryContextDelete(context);
}
