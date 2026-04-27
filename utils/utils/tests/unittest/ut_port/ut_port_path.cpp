
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
 * ut_port_path.cpp
 * Developer test of path.
 *
 * ---------------------------------------------------------------------------------
 */
#include <gtest/gtest.h>
#include <mockcpp/mockcpp.hpp>
#include "securec.h"
#include "port/platform_port.h"

/** ************************************************************************************************************* **/


/** ************************************************************************************************************* **/

class PathTest : public testing::Test {
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
 * @tc.name:  PathFunction001_Level0
 * @tc.desc:  Test the mutex create,lock,unlock and destroy.
 * @tc.type: FUNC
 */
#define FULL_PATH_TEST_STRING     "/home/usr/database/dt_test"
#define DIR_NAME_RESULT           "/home/usr/database"
#define BASE_NAME_RESULT          "dt_test"
#define CURRENT_PROGRAM_NAME      "utils_unittest"
#define TEST_DIRECTORY_NAME       "path_test"

TEST_F(PathTest, PathFunction001_Level0)
{
    /**
    * @tc.steps: step1. Test basic path operating functions
    * @tc.expected: step1.The path functions are correct.
    */
    char fullPath[MAX_PATH] = FULL_PATH_TEST_STRING;
    char *basename = Basename(fullPath);
    ASSERT_STREQ(basename, BASE_NAME_RESULT);
    char *dirname = Dirname(fullPath);
    ASSERT_STREQ(dirname, DIR_NAME_RESULT);

    /**
    * @tc.steps: step2. Test get,create,open,read,close and destroy api.
    * @tc.expected: step2.The path functions are correct.
    */
    ErrorCode errorCode;
    errorCode = GetCurrentProcessName(fullPath, MAX_PATH);
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    ASSERT_STREQ(fullPath, CURRENT_PROGRAM_NAME);
    errorCode = GetCurrentWorkingDirectory(fullPath, MAX_PATH);
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    char dirPath[MAX_PATH] = {0};
    int rc = sprintf_s(dirPath, MAX_PATH, "%s/%s", fullPath, TEST_DIRECTORY_NAME);
    ASSERT_GT(rc, 0);
    errorCode = MakeDirectory(dirPath, S_IRWXU);
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    Directory dir;
    errorCode = OpenDirectory(fullPath, &dir);
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
    DirectoryEntry dirEntry;
    bool result = ReadDirectory(&dir, &dirEntry);
    ASSERT_TRUE(result);
    CloseDirectory(&dir);
    errorCode = DestroyDirectory(dirPath);
    EXPECT_EQ(errorCode, ERROR_SYS_OK);
}

/**
 * @tc.name:  PathFunction002_Level0
 * @tc.desc:  Test canonicalize path.
 * @tc.type: FUNC
 */
TEST_F(PathTest, PathFunction002_Level0)
{
    char path[MAX_PATH];
    EXPECT_EQ(GetCurrentWorkingDirectory(path, MAX_PATH), ERROR_SYS_OK);
    char resolvedPath[PATH_MAX];
    ASSERT_TRUE (CanonicalizePath(path, resolvedPath)) ;
    ASSERT_STREQ(path, resolvedPath);
    sprintf_s(path, MAX_PATH, "%s/%s", path, "../");
    ASSERT_TRUE (CanonicalizePath(path, resolvedPath)) ;
    ASSERT_STRNE(path, resolvedPath);
}
