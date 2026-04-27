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
#include "ut_error/ut_error.h"
#include "common/error/dstore_error.h"
#include "errorcode/dstore_common_error_code.h"
#include "errorcode/dstore_undo_error_code.h"
#include "ut_error/ut_error_code.h"
#include "ut_error/ut_error_code_map.h"
#include "framework/dstore_instance.h"

using namespace DSTORE;

#define storage_utest_error_common(_expected_output_errmsg)   \
    {                                                       \
        EXPECT_STREQ(StorageGetMessage(), (_expected_output_errmsg)); \
        const char *actual_file_name = nullptr; \
        int actual_line_number = 0; \
        thrd->error->GetLocation(actual_file_name, actual_line_number); \
        EXPECT_STREQ(actual_file_name, __FILE__); \
        EXPECT_EQ(actual_line_number, __LINE__); \
        const char *actual_function_name = nullptr; \
        StorageGetFunctionName(actual_function_name); \
        EXPECT_STREQ(actual_function_name, __FUNCTION__); \
    }

#define storage_utest_error_local_node(_expected_output_errmsg, ...) \
    {                                                                   \
        storage_set_error(__VA_ARGS__); \
        storage_utest_error_common(_expected_output_errmsg); \
        EXPECT_EQ((StorageGetErrorNodeId()), g_storageInstance->GetGuc()->selfNodeId); \
    }

#define storage_utest_error_remote_node(nodeId, _expected_output_errmsg, ...) \
    {                                                                           \
        storage_set_error_with_nodeId(nodeId, __VA_ARGS__); \
        storage_utest_error_common(_expected_output_errmsg); \
        EXPECT_EQ((StorageGetErrorNodeId()), nodeId); \
    }

#define storage_utest_error(_expected_output_errmsg, ...) \
    {                                                       \
        storage_utest_error_local_node(_expected_output_errmsg, __VA_ARGS__); \
        storage_utest_error_remote_node(57873474 /* Any random nodeId */, _expected_output_errmsg, __VA_ARGS__); \
    }

static void overwrite_error_table()
{
    /* overwrite the error table */
    delete thrd->error;
    thrd->error = DstoreNew(g_dstoreCurrentMemoryContext) Error(g_ut_error_code_map, sizeof(g_ut_error_code_map)/sizeof(const char *));
}

static void restore_error_table()
{
    /* overwrite the error table */
    delete thrd->error;
    thrd->error = DstoreNew(g_dstoreCurrentMemoryContext) Error();
}

TEST_F(ErrorTest, ErrorTestBasic)
{
    storage_utest_error("Invalid zone id -1", UNDO_ERROR_INVALID_ZONE_ID, -1);
}

TEST_F(ErrorTest, ErrorTestUndefined)
{
    /* Set an error that is not defined */
    storage_set_error(UNDO_ERROR_OUT_OF_MEMORY | ERROR_MODULE_MASK);

    EXPECT_EQ(StorageGetErrorCode(), COMMON_ERROR_UNDEFINED_ERROR);
    EXPECT_STREQ(StorageGetMessage(), "Undefined error");
    EXPECT_STREQ(StorageGetErrorName(), "COMMON_ERROR_UNDEFINED_ERROR");
}

/* The following tests use the overwritten error table specific for unit tests in ut_error_code_map.h  */
TEST_F(ErrorTest, DISABLED_ErrorTestUTTable)
{
    overwrite_error_table();
    storage_utest_error("Testing Error without tokens", UT_ERROR_BASIC);
    storage_utest_error("Testing Error ends with %", UT_ERROR_PERCENT);
    storage_utest_error("Testing Error including % in the middle", UT_ERROR_PERCENT2);
    storage_utest_error("token1 is my token", UT_ERROR_WITH_TOKEN, "token1");
    storage_utest_error("My token is in the end: token1", UT_ERROR_TOKEN_IN_THE_END, "token1");
    storage_utest_error("The first token: token1 and the second token: token2 and the third token: token3",
                         UT_ERROR_MULTIPLE_TOKENS, "token1", "token2", "token3");
    storage_utest_error("token1 is my token", UT_ERROR_WITH_TOKEN, "token1", "token2");
    storage_utest_error("Testing %d token i1 = -1, i2 = -2", UT_ERROR_TOKEN_PERCENTD, -1, -2);

    const char expected_error_msg[] =
      "This is a really long message. Just repeat the message until it exceeds 512 characters.\
       This is a really long message. Just repeat the message until it exceeds 512 characters.\
       This is a really long message. Just repeat the message until it exceeds 512 characters.\
       This is a really long message. Just repeat the message until it exceeds 512 characters.\
       This is a really long message. Just repeat the message until it exceeds 512 characters.\
       This is a really long message. Just repeat"; /* cuts off at 512 characters */

    storage_utest_error(expected_error_msg, UT_ERROR_LONG_MESSAGE);
    /* "A single percent % in the end middle" % i outputs as %i with an additional space in front */
    storage_utest_error("A single percent  67n the middle", UT_ERROR_TOKEN_SINGLE_PERCENT, 67);
    restore_error_table();
}
