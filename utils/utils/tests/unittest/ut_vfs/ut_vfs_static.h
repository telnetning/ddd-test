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

#ifndef UTILS_COMMON_UT_VFS_STATIC_H
#define UTILS_COMMON_UT_VFS_STATIC_H

#include "ut_vfs_common.h"
#include "vfs/vfs_utils.h"

#define TEST_STORE_SPACE_ID 1

class VfsStaticLinuxTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        PrepareTestDataDirectory();
        ASSERT_EQ(InitVfsModule(nullptr), 0);
        ASSERT_EQ(GetStaticLocalVfsInstance(&testVfs), 0);
        for (uint32_t i = 0; i < SIZE_8K; ++i) {
            lowerCaseData[i] = (char)('a' + (i % LETTER_COUNT));
            upperCaseData[i] = (char)('A' + (i % LETTER_COUNT));
            digitData[i] = (char)('0' + (i % DIGIT_COUNT));
        }
        defaultTestFilePath = GenerateTestFilePath(utDataDir, VFS_UT_FILE_ID);
        for (uint32_t i = 0; i < FILE_PATH_MAX_LEN; ++i) {
            invalidFilePath[i] = 'a';
        }
        invalidFilePath[FILE_PATH_MAX_LEN] = '\0';
    }

    void TearDown() override
    {
        ASSERT_EQ(ExitVfsModule(), 0);
        (void) rmdir(utDataDir.c_str());
    }

    void PrepareTestDataDirectory()
    {
        if (access(VFS_UT_DATA_DIR, F_OK) == 0) {
            /* ut data dir already exist, need to remove */
            std::string rmCmd = "rm -rf ";
            std::string vfsDataDir = VFS_UT_DATA_DIR;
            std::string rmDirCmd = rmCmd + vfsDataDir;
            system(rmDirCmd.c_str());
        }
        ASSERT_EQ(mkdir(VFS_UT_DATA_DIR, S_IRWXU), 0);
        utDataDir = VFS_UT_DATA_DIR;
    }

    const char *GetTestFilePath() const
    {
        return defaultTestFilePath.c_str();
    }

    void CreateTestFile()
    {
        ASSERT_EQ(Create(testVfs, GetTestFilePath(), testFilePara, &testFd), 0);
    }

    void CloseAndRemoveTestFile()
    {
        ASSERT_EQ(Close(testFd), 0);
        ASSERT_EQ(Remove(testVfs, GetTestFilePath()), 0);
    }

    VirtualFileSystem *GetInvalidVfsHandle()
    {
        invalidVfsHandle = *testVfs;
        invalidVfsHandle.vfsHandle = nullptr;
        return &invalidVfsHandle;
    }

    FileDescriptor *GetInvalidFd()
    {
        invalidFd = *testFd;
        invalidFd.fileHandle = nullptr;
        return &invalidFd;
    }

    VirtualFileSystem *testVfs;
    FileDescriptor *testFd;
    char lowerCaseData[SIZE_8K];
    char upperCaseData[SIZE_8K];
    char digitData[SIZE_8K];
    FileParameter testFilePara = {
        "storeSpaceName1",
        VFS_DEFAULT_FILE_STREAM_ID,
        IN_PLACE_WRITE_FILE,
        DATA_FILE_TYPE,
        DEFAULT_RANGE_SIZE,
        0xFFFFFFFFU,
        0,
        FILE_READ_AND_WRITE_MODE,
        false
    };
    std::string utDataDir;
    std::string defaultTestFilePath;
    const char *emptyStr = "";
    char invalidFilePath[FILE_PATH_MAX_LEN + 1];
    StoreSpaceAttr storeSpaceAttr = {
        .attrFlags = MEDIUM_DISK_PERF_FLAG,
        .maxSize = 1,
        .reserved = 0
    };
    const char *storeSpaceNames[STORESPACE_NAME_MAX_LEN] = {"storeSpaceName1"};

private:
    VirtualFileSystem invalidVfsHandle;
    FileDescriptor invalidFd;
};

#endif /* UTILS_COMMON_UT_VFS_STATIC_H */
