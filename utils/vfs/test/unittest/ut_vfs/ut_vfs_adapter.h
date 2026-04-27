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
 * ut_vfs_adapter.h
 *
 * Description:
 * linux vfs dynamic library ut header
 *
 * ---------------------------------------------------------------------------------
 */
#ifndef UTILS_COMMON_UT_VFS_ADAPTER_H
#define UTILS_COMMON_UT_VFS_ADAPTER_H

#include <cstdlib>
#include <ctime>
#include <string>
#include <gtest/gtest.h>
#include "securec.h"
#include "vfs/vfs_interface.h"

#define TEST_PAGE_SIZE 8192
#define LETTER_COUNT 26
#define TEST_VFS_DATA_DIR "./ut_vfs_data_dir/"
#define TEST_TENANT_NAME "TenantName"
#define TEST_VFS_NAME_1 "UtVfsName-1"
#define TEST_VFS_NAME_2 "UtVfsName-2"
#define TEST_SNAPSHOT_NAME "SnapshotName"

constexpr const char * const VFS_TENANT_DATA_DIR_1 = "./ut_tenant_data_dir_1/";
constexpr const char * const VFS_TENANT_DATA_DIR_2 = "./ut_tenant_data_dir_2/";

class UtSubProcessService : public ::testing::Test {
protected:
    void CreateSubProcess(int processNum);
    static void EndSubProcess();
    bool IsMainProcess() const
    {
        return this->processIndex == PARENT_PROCESS_ID;
    }
private:
    static constexpr int PARENT_PROCESS_ID = -1;
    int processIndex;
    int processCount;
    bool CheckAllSubProcess() const
    {
        for (int i = 0; i < this->processCount; ++i) {
            int status;
            wait(&status);
            if (status != 0) {
                return false;
            }
        }
        return true;
    }
};

class VfsAdapterLinuxTest : public UtSubProcessService {
protected:
    static void TearDownTestCase()
    {
        if (access(TEST_VFS_DATA_DIR, F_OK) == 0) {
            std::string rmCmd = "rm -rf ";
            std::string vfsDataDir = TEST_VFS_DATA_DIR;
            std::string rmDirCmd = rmCmd + vfsDataDir;
            system(rmDirCmd.c_str());
        }
    }

    void SetUp() override
    {
        /* Create 1 sub process to run unittest to avoid chroot() effect */
        CreateSubProcess(1);
        if (IsMainProcess()) {
            return;
        }
        for (uint32_t i = 0; i < TEST_PAGE_SIZE; ++i) {
            lowerCaseData[i] = (char)('a' + (i % LETTER_COUNT));
            upperCaseData[i] = (char)('A' + (i % LETTER_COUNT));
        }
        if (access(TEST_VFS_DATA_DIR, F_OK) == 0) {
            std::string rmCmd = "rm -rf ";
            std::string vfsDataDir = TEST_VFS_DATA_DIR;
            std::string rmDirCmd = rmCmd + vfsDataDir;
            system(rmDirCmd.c_str());
        }
        ASSERT_EQ(mkdir(TEST_VFS_DATA_DIR, S_IRWXU), 0);
        ASSERT_EQ(InitVfsModule(nullptr), 0);
        PrepareAdapterVfsLib();
    }

    void TearDown() override
    {
        if (IsMainProcess()) {
            return;
        }
        ASSERT_EQ(OffloadVfsLib(libHandle), 0);
        ASSERT_EQ(ExitVfsModule(), 0);
        EndSubProcess();
    }

    static void PrepareAdapterVfsLib()
    {
#ifdef LINUX_LIB_PATH
        ASSERT_EQ(LoadVfsLib(LINUX_LIB_PATH, nullptr, &libHandle), 0);
#else
        ASSERT_TRUE(false);
#endif
        VfsLibParameter testLibPara;
        testLibPara.pageSize = TEST_PAGE_SIZE;
        testLibPara.dbType = 1;
        errno_t ret = strncpy_s(testLibPara.storageServerAddr, VFS_LIB_ATTR_LEN,
                                TEST_VFS_DATA_DIR, strlen(TEST_VFS_DATA_DIR));
        ASSERT_EQ(ret, 0);
        ASSERT_EQ(InitVfsLib(libHandle, &testLibPara), 0);
    }

    void PrepareTwoVfs()
    {
        ASSERT_EQ(CreateVfs(nullptr, TEST_TENANT_NAME, TEST_VFS_NAME_1, VFS_DEFAULT_ATTR_FLAG), 0);
        ASSERT_EQ(CreateVfs(nullptr, TEST_TENANT_NAME, TEST_VFS_NAME_2, VFS_DEFAULT_ATTR_FLAG), 0);
        ASSERT_EQ(MountVfs(nullptr, TEST_TENANT_NAME, TEST_VFS_NAME_1, &vfsHandle1), 0);
        ASSERT_EQ(MountVfs(nullptr, TEST_TENANT_NAME, TEST_VFS_NAME_2, &vfsHandle2), 0);
    }

    void DestroyTwoVfs()
    {
        ASSERT_EQ(UnmountVfs(vfsHandle1), 0);
        ASSERT_EQ(UnmountVfs(vfsHandle2), 0);
        ASSERT_EQ(DropVfs(nullptr, TEST_TENANT_NAME, TEST_VFS_NAME_1), 0);
        ASSERT_EQ(DropVfs(nullptr, TEST_TENANT_NAME, TEST_VFS_NAME_2), 0);
    }

    static VfsLibHandle *libHandle;
    VirtualFileSystem *vfsHandle1;
    VirtualFileSystem *vfsHandle2;
    FileParameter fileParam = {
        "storeSpaceName1",
        VFS_DEFAULT_FILE_STREAM_ID,
        IN_PLACE_WRITE_FILE,
        DATA_FILE_TYPE,
        DEFAULT_RANGE_SIZE,
        UPDATE_FILE_MAX_SIZE,
        0,
        FILE_READ_AND_WRITE_MODE,
        false
    };
    char lowerCaseData[TEST_PAGE_SIZE];
    char upperCaseData[TEST_PAGE_SIZE];
};

struct FilePathInfo {
    const char *dataDir;
    const char *filePath;
};

typedef void (*UnitTestFunc)(void *context);

class VfsMultiProcessTest : public ::testing::Test {
protected:
    static void TearDownTestCase()
    {
        if (access(VFS_TENANT_DATA_DIR_1, F_OK) == 0) {
            std::string rmCmd = "rm -rf ";
            std::string vfsDataDir = VFS_TENANT_DATA_DIR_1;
            std::string rmDirCmd = rmCmd + vfsDataDir;
            system(rmDirCmd.c_str());
        }
        if (access(VFS_TENANT_DATA_DIR_2, F_OK) == 0) {
            std::string rmCmd = "rm -rf ";
            std::string vfsDataDir = VFS_TENANT_DATA_DIR_2;
            std::string rmDirCmd = rmCmd + vfsDataDir;
            system(rmDirCmd.c_str());
        }
    }

    void SetUp() override
    {
        ASSERT_EQ(InitVfsModule(nullptr), 0);
    }

    void TearDown() override
    {
        ASSERT_EQ(ExitVfsModule(), 0);
    }

    static void ClearAndMakeDirectory(const char *dataDir)
    {
        if (access(dataDir, F_OK) == 0) {
            std::string rmCmd = "rm -rf ";
            std::string dataDirStr = dataDir;
            std::string rmDirCmd = rmCmd + dataDirStr;
            system(rmDirCmd.c_str());
        }
        ASSERT_EQ(mkdir(dataDir, S_IRWXU), 0);
    }

    static void PrepareVfsLibHandle(const char *dataDir, VfsLibHandle **libHandle)
    {
        VfsLibHandle *vfsLibHandle;
#ifdef LINUX_LIB_PATH
        ASSERT_EQ(LoadVfsLib(LINUX_LIB_PATH, nullptr, &vfsLibHandle), 0);
#else
        ASSERT_TRUE(false);
#endif
        VfsLibParameter testLibPara;
        errno_t ret = strncpy_s(testLibPara.storageServerAddr, VFS_LIB_ATTR_LEN,
                                dataDir, strlen(dataDir));
        ASSERT_EQ(ret, 0);
        ASSERT_EQ(InitVfsLib(vfsLibHandle, &testLibPara), 0);
        *libHandle = vfsLibHandle;
    }

    static void OffloadVfsLibHandle(VfsLibHandle *libHandle)
    {
        ASSERT_EQ(OffloadVfsLib(libHandle), 0);
        ASSERT_EQ(ExitVfsModule(), 0);
    }

public:
    static void ForkSubProcessAndTest(UnitTestFunc func, void *context);
    static void CreateFileWithAbsolutePathTest(const char *dataDir, const char *filePath);
};

#endif /* UTILS_COMMON_UT_VFS_ADAPTER_H */
