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
 * ut_vfs_adapter.cpp
 *
 * Description:
 * linux vfs dynamic library ut
 *
 * ---------------------------------------------------------------------------------
 */
#include "ut_vfs_adapter.h"
#include <sys/types.h>

VfsLibHandle *VfsAdapterLinuxTest::libHandle = nullptr;

/* Invalid vfs name with 257 length (including terminal '\0') */
char INVALID_VFS_NAME[] = "01234567890123456789012345678901234567890123456789012345678901234567890123456789"
                          "01234567890123456789012345678901234567890123456789012345678901234567890123456789"
                          "01234567890123456789012345678901234567890123456789012345678901234567890123456789"
                          "0123456789012345";
/* Valid vfs name with 256 length (including terminal '\0') */
char MAX_LEN_VALID_VFS_NAME[] = "01234567890123456789012345678901234567890123456789012345678901234567890123456789"
                                "01234567890123456789012345678901234567890123456789012345678901234567890123456789"
                                "01234567890123456789012345678901234567890123456789012345678901234567890123456789"
                                "012345678901234";

/* Invalid snapshot name with 257 length (including terminal '\0') */
char INVALID_SNAPSHOT_NAME[] = "01234567890123456789012345678901234567890123456789012345678901234567890123456789"
                               "01234567890123456789012345678901234567890123456789012345678901234567890123456789"
                               "01234567890123456789012345678901234567890123456789012345678901234567890123456789"
                               "0123456789012345";
/* Valid snapshot name with 256 length (including terminal '\0') */
char MAX_LEN_VALID_SNAPSHOT_NAME[] = "01234567890123456789012345678901234567890123456789012345678901234567890123456789"
                                     "01234567890123456789012345678901234567890123456789012345678901234567890123456789"
                                     "01234567890123456789012345678901234567890123456789012345678901234567890123456789"
                                     "012345678901234";

void UtSubProcessService::CreateSubProcess(int processNum)
{
    ASSERT_TRUE(processNum > 0);
    this->processCount = processNum;
    for (int i = 0; i < this->processCount; ++i) {
        pid_t pid = fork();
        ASSERT_TRUE(pid >= 0);
        if (pid == 0) {
            /* Sub process */
            this->processIndex = i;
        } else {
            /* Parent process */
            this->processIndex = PARENT_PROCESS_ID;
        }
    }
    /* Parent process check all sub processes status */
    if (IsMainProcess()) {
        ASSERT_TRUE(CheckAllSubProcess());
    }
}

void UtSubProcessService::EndSubProcess()
{
    exit(0);
}

TEST_F(VfsAdapterLinuxTest, CreateVfsTest001)
{
    if (IsMainProcess()) {
        return;
    }
    ASSERT_EQ(CreateVfs(nullptr, TEST_TENANT_NAME, nullptr, VFS_DEFAULT_ATTR_FLAG), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(CreateVfs(nullptr, TEST_TENANT_NAME, TEST_VFS_NAME_1, VFS_DEFAULT_ATTR_FLAG), 0);
    ASSERT_EQ(CreateVfs(nullptr, TEST_TENANT_NAME, TEST_VFS_NAME_1, VFS_DEFAULT_ATTR_FLAG),
        VFS_ERROR_CREATE_VFS_NAME_EXIST);
    ASSERT_EQ(DropVfs(nullptr, TEST_TENANT_NAME, TEST_VFS_NAME_1), 0);
}

TEST_F(VfsAdapterLinuxTest, CreateVfsTest002)
{
    if (IsMainProcess()) {
        return;
    }
    ASSERT_EQ(CreateVfs(nullptr, TEST_TENANT_NAME, INVALID_VFS_NAME, VFS_DEFAULT_ATTR_FLAG),
        VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(CreateVfs(nullptr, TEST_TENANT_NAME, MAX_LEN_VALID_VFS_NAME, VFS_DEFAULT_ATTR_FLAG), 0);
    ASSERT_EQ(DropVfs(nullptr, TEST_TENANT_NAME, MAX_LEN_VALID_VFS_NAME), 0);
}

TEST_F(VfsAdapterLinuxTest, DropVfsTest001)
{
    if (IsMainProcess()) {
        return;
    }
    ASSERT_EQ(DropVfs(nullptr, TEST_TENANT_NAME, TEST_VFS_NAME_1), VFS_ERROR_VFS_NAME_NOT_EXIST);
    ASSERT_EQ(CreateVfs(nullptr, TEST_TENANT_NAME, TEST_VFS_NAME_1, VFS_DEFAULT_ATTR_FLAG), 0);
    ASSERT_EQ(DropVfs(nullptr, TEST_TENANT_NAME, nullptr), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(DropVfs(nullptr, TEST_TENANT_NAME, TEST_VFS_NAME_1), 0);
    ASSERT_EQ(DropVfs(nullptr, TEST_TENANT_NAME, TEST_VFS_NAME_1), VFS_ERROR_VFS_NAME_NOT_EXIST);
}

TEST_F(VfsAdapterLinuxTest, MountVfsTest001)
{
    if (IsMainProcess()) {
        return;
    }
    ASSERT_EQ(MountVfs(nullptr, TEST_TENANT_NAME, TEST_VFS_NAME_1, &vfsHandle1), VFS_ERROR_VFS_NAME_NOT_EXIST);
    ASSERT_EQ(CreateVfs(nullptr, TEST_TENANT_NAME, TEST_VFS_NAME_1, VFS_DEFAULT_ATTR_FLAG), 0);
    ASSERT_EQ(MountVfs(nullptr, TEST_TENANT_NAME, nullptr, &vfsHandle1), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(MountVfs(nullptr, TEST_TENANT_NAME, TEST_VFS_NAME_1, &vfsHandle1), 0);
    ASSERT_EQ(UnmountVfs(vfsHandle1), 0);
    ASSERT_EQ(DropVfs(nullptr, TEST_TENANT_NAME, TEST_VFS_NAME_1), 0);
    ASSERT_EQ(MountVfs(nullptr, TEST_TENANT_NAME, TEST_VFS_NAME_1, &vfsHandle1), VFS_ERROR_VFS_NAME_NOT_EXIST);
}

TEST_F(VfsAdapterLinuxTest, UnmountVfsTest001)
{
    if (IsMainProcess()) {
        return;
    }
    ASSERT_EQ(CreateVfs(nullptr, TEST_TENANT_NAME, TEST_VFS_NAME_1, VFS_DEFAULT_ATTR_FLAG), 0);
    ASSERT_EQ(MountVfs(nullptr, TEST_TENANT_NAME, TEST_VFS_NAME_1, &vfsHandle1), 0);
    ASSERT_EQ(UnmountVfs(nullptr), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(UnmountVfs(vfsHandle1), 0);
    ASSERT_EQ(DropVfs(nullptr, TEST_TENANT_NAME, TEST_VFS_NAME_1), 0);
}

TEST_F(VfsAdapterLinuxTest, CreateFileTest001)
{
    if (IsMainProcess()) {
        return;
    }
    const char *testFileFullPath = "/test_vfs_linux_adapter.data";
    unlink(testFileFullPath);
    PrepareTwoVfs();
    FileDescriptor *fd1, *fd2;
    ASSERT_EQ(Create(vfsHandle1, testFileFullPath, fileParam, &fd1), 0);
    ASSERT_EQ(Close(fd1), 0);
    ASSERT_EQ(Create(vfsHandle2, testFileFullPath, fileParam, &fd2), VFS_ERROR_CREATE_FILE_EXIST);
    ASSERT_EQ(Remove(vfsHandle1, testFileFullPath), 0);
    ASSERT_EQ(Create(vfsHandle2, testFileFullPath, fileParam, &fd2), 0);
    ASSERT_EQ(Close(fd2), 0);
    ASSERT_EQ(Remove(vfsHandle2, testFileFullPath), 0);
    DestroyTwoVfs();
}

TEST_F(VfsAdapterLinuxTest, CreateFileTest002)
{
    if (IsMainProcess()) {
        return;
    }
    const char *testFilePath = "test_vfs_linux_adapter.data";
    PrepareTwoVfs();
    FileDescriptor *fd1, *fd2;
    ASSERT_EQ(Create(vfsHandle1, testFilePath, fileParam, &fd1), 0);
    ASSERT_EQ(Create(vfsHandle2, testFilePath, fileParam, &fd2), 0);
    ASSERT_EQ(Close(fd1), 0);
    ASSERT_EQ(Close(fd2), 0);
    ASSERT_EQ(Remove(vfsHandle1, testFilePath), 0);
    ASSERT_EQ(Remove(vfsHandle2, testFilePath), 0);
    DestroyTwoVfs();
}

TEST_F(VfsAdapterLinuxTest, CreateFileTest003)
{
    if (IsMainProcess()) {
        return;
    }
    const char *outRootFilePath01 = ".//.//./../abnormal_.data";
    const char *outRootFilePath02 = "////././//./../abnormal.data";
    const char *outRootFilePath03 = "..///xxx/././//./../abnormal.data";
    const char *outRootFilePath04 = "./././//./abnormal.data/..";
    PrepareTwoVfs();
    FileDescriptor *fd1, *fd2;
    ASSERT_EQ(Create(vfsHandle1, outRootFilePath01, fileParam, &fd1), VFS_ERROR_OPERATION_NOT_SUPPORT);
    ASSERT_EQ(Create(vfsHandle2, outRootFilePath02, fileParam, &fd2), VFS_ERROR_OPERATION_NOT_SUPPORT);
    ASSERT_EQ(Create(vfsHandle2, outRootFilePath03, fileParam, &fd2), VFS_ERROR_OPERATION_NOT_SUPPORT);
    ASSERT_EQ(Create(vfsHandle2, outRootFilePath04, fileParam, &fd2), VFS_ERROR_OPERATION_NOT_SUPPORT);
    DestroyTwoVfs();
}

TEST_F(VfsAdapterLinuxTest, ReadWriteFileTest001)
{
    if (IsMainProcess()) {
        return;
    }
    const char *testFilePath = "test_vfs_linux_adapter.data";
    PrepareTwoVfs();
    FileDescriptor *fd1;
    ASSERT_EQ(Create(vfsHandle1, testFilePath, fileParam, &fd1), 0);
    ASSERT_EQ(Extend(fd1, TEST_PAGE_SIZE), 0);
    ASSERT_EQ(PwriteAsync(fd1, lowerCaseData, TEST_PAGE_SIZE, 0, nullptr), 0);
    ASSERT_EQ(FDataSync(fd1), 0);
    char readData[TEST_PAGE_SIZE];
    int64_t readSize = 0;
    ASSERT_EQ(Pread(fd1, readData, TEST_PAGE_SIZE, 0, &readSize), 0);
    ASSERT_EQ(readSize, TEST_PAGE_SIZE);
    ASSERT_EQ(strncmp(readData, lowerCaseData, TEST_PAGE_SIZE), 0);
    ASSERT_EQ(WriteAsync(fd1, upperCaseData, TEST_PAGE_SIZE, nullptr), 0);
    ASSERT_EQ(Fsync(fd1), 0);
    ASSERT_EQ(Rewind(fd1), 0);
    readSize = 0;
    ASSERT_EQ(Read(fd1, readData, TEST_PAGE_SIZE, &readSize), 0);
    ASSERT_EQ(readSize, TEST_PAGE_SIZE);
    ASSERT_EQ(strncmp(readData, upperCaseData, TEST_PAGE_SIZE), 0);
    /* Reach file end, read 0 size from file */
    ASSERT_EQ(Read(fd1, readData, TEST_PAGE_SIZE, &readSize), 0);
    ASSERT_EQ(readSize, 0);
    ASSERT_EQ(Close(fd1), 0);
    ASSERT_EQ(Remove(vfsHandle1, testFilePath), 0);
    DestroyTwoVfs();
}

TEST_F(VfsAdapterLinuxTest, PreadPwriteFileTest001)
{
    if (IsMainProcess()) {
        return;
    }
    const char *testFilePath = "test_vfs_linux_adapter.data";
    PrepareTwoVfs();
    FileDescriptor *fd1, *fd2;
    ASSERT_EQ(Create(vfsHandle1, testFilePath, fileParam, &fd1), 0);
    ASSERT_EQ(Create(vfsHandle2, testFilePath, fileParam, &fd2), 0);
    ASSERT_EQ(Extend(fd1, TEST_PAGE_SIZE), 0);
    ASSERT_EQ(Extend(fd2, TEST_PAGE_SIZE), 0);
    int64_t writeSize = 0;
    ASSERT_EQ(PwriteSync(fd1, lowerCaseData, TEST_PAGE_SIZE, 0, &writeSize), 0);
    ASSERT_EQ(writeSize, TEST_PAGE_SIZE);
    writeSize = 0;
    ASSERT_EQ(PwriteSync(fd2, upperCaseData, TEST_PAGE_SIZE, 0, &writeSize), 0);
    ASSERT_EQ(writeSize, TEST_PAGE_SIZE);
    char readData[TEST_PAGE_SIZE];
    int64_t readSize = 0;
    ASSERT_EQ(Pread(fd1, readData, TEST_PAGE_SIZE, 0, &readSize), 0);
    ASSERT_EQ(readSize, TEST_PAGE_SIZE);
    ASSERT_EQ(strncmp(readData, lowerCaseData, TEST_PAGE_SIZE), 0);
    readSize = 0;
    ASSERT_EQ(Pread(fd2, readData, TEST_PAGE_SIZE, 0, &readSize), 0);
    ASSERT_EQ(readSize, TEST_PAGE_SIZE);
    ASSERT_EQ(strncmp(readData, upperCaseData, TEST_PAGE_SIZE), 0);
    ASSERT_EQ(Close(fd1), 0);
    ASSERT_EQ(Close(fd2), 0);
    ASSERT_EQ(Remove(vfsHandle1, testFilePath), 0);
    ASSERT_EQ(Remove(vfsHandle2, testFilePath), 0);
    DestroyTwoVfs();
}

TEST_F(VfsAdapterLinuxTest, OpreationSnapshotTest)
{
    if (IsMainProcess()) {
        return;
    }
    ASSERT_EQ(CreateSnapshot(nullptr, TEST_TENANT_NAME, nullptr, TEST_SNAPSHOT_NAME, SNAPSHOT_ALL_FILE_FLAG),
              VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(CreateSnapshot(nullptr, TEST_TENANT_NAME, TEST_VFS_NAME_1, TEST_SNAPSHOT_NAME, -1),
              VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(CreateSnapshot(nullptr, TEST_TENANT_NAME, INVALID_VFS_NAME, TEST_SNAPSHOT_NAME,
        SNAPSHOT_ALL_FILE_FLAG), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(CreateSnapshot(nullptr, TEST_TENANT_NAME, TEST_VFS_NAME_1, INVALID_SNAPSHOT_NAME,
        SNAPSHOT_ALL_FILE_FLAG), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(CreateSnapshot(nullptr, TEST_TENANT_NAME, TEST_VFS_NAME_1, TEST_SNAPSHOT_NAME, VFS_DEFAULT_ATTR_FLAG),
              0);
    ASSERT_EQ(CreateSnapshot(nullptr, TEST_TENANT_NAME, TEST_VFS_NAME_1, MAX_LEN_VALID_SNAPSHOT_NAME,
        VFS_DEFAULT_ATTR_FLAG), 0);

    ASSERT_EQ(RollbackSnapshot(nullptr, TEST_TENANT_NAME, nullptr, TEST_SNAPSHOT_NAME),
              VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(RollbackSnapshot(nullptr, TEST_TENANT_NAME, INVALID_VFS_NAME, TEST_SNAPSHOT_NAME),
              VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(RollbackSnapshot(nullptr, TEST_TENANT_NAME, TEST_VFS_NAME_1, INVALID_SNAPSHOT_NAME),
              VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(RollbackSnapshot(nullptr, TEST_TENANT_NAME, TEST_VFS_NAME_1, TEST_SNAPSHOT_NAME), 0);

    ASSERT_EQ(DropSnapshot(nullptr, TEST_TENANT_NAME, nullptr, TEST_SNAPSHOT_NAME), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(DropSnapshot(nullptr, TEST_TENANT_NAME, INVALID_VFS_NAME, TEST_SNAPSHOT_NAME),
              VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(DropSnapshot(nullptr, TEST_TENANT_NAME, TEST_VFS_NAME_1, INVALID_SNAPSHOT_NAME),
              VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(DropSnapshot(nullptr, TEST_TENANT_NAME, TEST_VFS_NAME_1, TEST_SNAPSHOT_NAME), 0);
    ASSERT_EQ(DropSnapshot(nullptr, TEST_TENANT_NAME, TEST_VFS_NAME_1, MAX_LEN_VALID_SNAPSHOT_NAME), 0);
}

TEST_F(VfsAdapterLinuxTest, OpenFileSnapshotTest)
{
    if (IsMainProcess()) {
        return;
    }
    const char *testFilePath = "test_vfs_linux_adapter.data";
    PrepareTwoVfs();
    ASSERT_EQ(CreateSnapshot(nullptr, TEST_TENANT_NAME, TEST_VFS_NAME_1, TEST_SNAPSHOT_NAME, VFS_DEFAULT_ATTR_FLAG),
              0);
    FileDescriptor *fd1, *fd2;
    ASSERT_EQ(Create(vfsHandle1, testFilePath, fileParam, &fd1), 0);
    ASSERT_EQ(Close(fd1), 0);
    FileOpenParam openPara = {.flags = FILE_READ_ONLY_FLAG, .filePath = testFilePath,
                              .snapshotName = nullptr};
    ASSERT_EQ(OpenSnapshot(vfsHandle1, openPara, &fd2), VFS_ERROR_PARAMETERS_INVALID);
    openPara.snapshotName = TEST_SNAPSHOT_NAME;
    ASSERT_EQ(OpenSnapshot(vfsHandle1, openPara, &fd2), 0);
    ASSERT_EQ(Close(fd2), 0);
    ASSERT_EQ(Remove(vfsHandle1, testFilePath), 0);
    ASSERT_EQ(DropSnapshot(nullptr, TEST_TENANT_NAME, TEST_VFS_NAME_1, TEST_SNAPSHOT_NAME), 0);
    DestroyTwoVfs();
}

TEST_F(VfsAdapterLinuxTest, SnapshotPreadTest)
{
    if (IsMainProcess()) {
        return;
    }
    const char *testFilePath = "test_vfs_linux_adapter.data";
    PrepareTwoVfs();
    ASSERT_EQ(CreateSnapshot(nullptr, TEST_TENANT_NAME, TEST_VFS_NAME_1, TEST_SNAPSHOT_NAME, VFS_DEFAULT_ATTR_FLAG),
              0);
    FileDescriptor *fd1, *fd2;
    ASSERT_EQ(Create(vfsHandle1, testFilePath, fileParam, &fd1), 0);
    ASSERT_EQ(Close(fd1), 0);
    FileOpenParam openPara = {.flags = FILE_READ_ONLY_FLAG, .filePath = testFilePath,
                              .snapshotName = nullptr};
    ASSERT_EQ(OpenSnapshot(vfsHandle1, openPara, &fd2), VFS_ERROR_PARAMETERS_INVALID);
    openPara.snapshotName = TEST_SNAPSHOT_NAME;
    openPara.preSnapshotName = TEST_SNAPSHOT_NAME;
    ASSERT_EQ(OpenSnapshot(vfsHandle1, openPara, &fd2), 0);
    char readData[TEST_PAGE_SIZE];
    DiffContents diffContents;
    ASSERT_EQ(SnapshotPread(fd2, readData, TEST_PAGE_SIZE, 0, &diffContents), 0);
    ASSERT_EQ(Close(fd2), 0);
    ASSERT_EQ(Remove(vfsHandle1, testFilePath), 0);
    ASSERT_EQ(DropSnapshot(nullptr, TEST_TENANT_NAME, TEST_VFS_NAME_1, TEST_SNAPSHOT_NAME), 0);
    DestroyTwoVfs();
}

TEST_F(VfsAdapterLinuxTest, RenameFileTest)
{
    if (IsMainProcess()) {
        return;
    }
    PrepareTwoVfs();
    FileDescriptor *fd1, *fd2;
#define SOURCE_FILE_NAME "srcFile"
#define DESTINATION_FILE_NAME "destFile"
    /* Create source file */
    ASSERT_EQ(Create(vfsHandle1, SOURCE_FILE_NAME, fileParam, &fd1), ERROR_SYS_OK);
    ASSERT_EQ(Close(fd1), ERROR_SYS_OK);

    /* Create destination file */
    ASSERT_EQ(Create(vfsHandle1, DESTINATION_FILE_NAME, fileParam, &fd2), ERROR_SYS_OK);
    ASSERT_EQ(Close(fd2), ERROR_SYS_OK);

    /* Rename source file */
    ASSERT_EQ(RenameFile(vfsHandle1, SOURCE_FILE_NAME, DESTINATION_FILE_NAME), ERROR_SYS_OK);

    /* Check source file */
    bool isFileExist;
    ASSERT_EQ(FileIsExist(vfsHandle1, SOURCE_FILE_NAME, &isFileExist), ERROR_SYS_OK);
    ASSERT_FALSE(isFileExist);

    /* Check destination file */
    ASSERT_EQ(FileIsExist(vfsHandle1, DESTINATION_FILE_NAME, &isFileExist), ERROR_SYS_OK);
    ASSERT_TRUE(isFileExist);

    /* Clean resource */
    ASSERT_EQ(Remove(vfsHandle1, SOURCE_FILE_NAME), VFS_ERROR_REMOVE_FILE_NOT_EXIST);
    ASSERT_EQ(Remove(vfsHandle1, DESTINATION_FILE_NAME), ERROR_SYS_OK);
    DestroyTwoVfs();
}

TEST_F(VfsAdapterLinuxTest, RenameFileWhenNoSrcTest)
{
    if (IsMainProcess()) {
        return;
    }
    PrepareTwoVfs();
    /* Rename non-exist source file */
    ASSERT_EQ(RenameFile(vfsHandle1, SOURCE_FILE_NAME, DESTINATION_FILE_NAME), VFS_ERROR_OPEN_FILE_NOT_EXIST);

    /* Clean resource */
    ASSERT_EQ(Remove(vfsHandle1, SOURCE_FILE_NAME), VFS_ERROR_REMOVE_FILE_NOT_EXIST);
    DestroyTwoVfs();
}

TEST_F(VfsAdapterLinuxTest, RenameFileWhenNoDestTest)
{
    if (IsMainProcess()) {
        return;
    }
    PrepareTwoVfs();
    FileDescriptor *fd1;
    /* Create source file */
    ASSERT_EQ(Create(vfsHandle1, SOURCE_FILE_NAME, fileParam, &fd1), ERROR_SYS_OK);
    ASSERT_EQ(Close(fd1), ERROR_SYS_OK);

    /* Rename source file */
    ASSERT_EQ(RenameFile(vfsHandle1, SOURCE_FILE_NAME, DESTINATION_FILE_NAME), ERROR_SYS_OK);

    /* Check source file */
    bool isFileExist;
    ASSERT_EQ(FileIsExist(vfsHandle1, SOURCE_FILE_NAME, &isFileExist), ERROR_SYS_OK);
    ASSERT_FALSE(isFileExist);

    /* Check destination file */
    ASSERT_EQ(FileIsExist(vfsHandle1, DESTINATION_FILE_NAME, &isFileExist), ERROR_SYS_OK);
    ASSERT_TRUE(isFileExist);

    /* Clean resource */
    ASSERT_EQ(Remove(vfsHandle1, SOURCE_FILE_NAME), VFS_ERROR_REMOVE_FILE_NOT_EXIST);
    ASSERT_EQ(Remove(vfsHandle1, DESTINATION_FILE_NAME), ERROR_SYS_OK);
    DestroyTwoVfs();
}

void VfsMultiProcessTest::ForkSubProcessAndTest(UnitTestFunc func, void *context)
{
    pid_t testPid = fork();
    if (testPid < 0) {
        ASSERT_TRUE(false);
    } else if (testPid == 0) {
        /* Sub process running unittest test instead of main process */
        func(context);
        exit(0);
    }
    int status;
    if (wait(&status) != testPid) {
        ASSERT_TRUE(false);
    }
    ASSERT_TRUE(WIFEXITED(status));
}

void VfsMultiProcessTest::CreateFileWithAbsolutePathTest(const char *dataDir, const char *filePath)
{
    VfsLibHandle *vfsLibHandle;
    PrepareVfsLibHandle(dataDir, &vfsLibHandle);
    ASSERT_EQ(CreateVfs(nullptr, "ClusterName", "VfsName", VFS_DEFAULT_ATTR_FLAG), 0);
    VirtualFileSystem *vfs;
    ASSERT_EQ(MountVfs(nullptr, "ClusterName", "VfsName", &vfs), 0);
    FileDescriptor *fd;
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
    ASSERT_EQ(Create(vfs, filePath, fileParam, &fd), 0);
    ASSERT_EQ(Close(fd), 0);
    ASSERT_EQ(UnmountVfs(vfs), 0);
    OffloadVfsLibHandle(vfsLibHandle);
}

static void CreateFileWithAbsolutePathByTenant1(void *context)
{
    auto *info = (FilePathInfo *)context;
    VfsMultiProcessTest::CreateFileWithAbsolutePathTest(info->dataDir, info->filePath);
}

static void CreateFileWithAbsolutePathByTenant2(void *context)
{
    auto *info = (FilePathInfo *)context;
    VfsMultiProcessTest::CreateFileWithAbsolutePathTest(info->dataDir, info->filePath);
}

TEST_F(VfsMultiProcessTest, MultiProcessCreateFileTest001)
{
    /* Prepare 2 directory for 2 tenants */
    ClearAndMakeDirectory(VFS_TENANT_DATA_DIR_1);
    ClearAndMakeDirectory(VFS_TENANT_DATA_DIR_2);

    /* Tenant 1 process create file with absolute path */
    FilePathInfo info = {VFS_TENANT_DATA_DIR_1, "/test.data"};
    ForkSubProcessAndTest(CreateFileWithAbsolutePathByTenant1, &info);
    /* Tenant 2 process create file with same absolute path */
    info = {VFS_TENANT_DATA_DIR_2, "/test.data"};
    ForkSubProcessAndTest(CreateFileWithAbsolutePathByTenant2, &info);
}

TEST_F(VfsMultiProcessTest, MultiProcessCreateFileTest002)
{
    /* Prepare 2 directory for 2 tenants */
    ClearAndMakeDirectory(VFS_TENANT_DATA_DIR_1);
    ClearAndMakeDirectory(VFS_TENANT_DATA_DIR_2);

    /* Tenant 1 process create file with absolute path */
    FilePathInfo info = {VFS_TENANT_DATA_DIR_1, "test.data"};
    ForkSubProcessAndTest(CreateFileWithAbsolutePathByTenant1, &info);
    /* Tenant 2 process create file with same absolute path */
    info = {VFS_TENANT_DATA_DIR_2, "test.data"};
    ForkSubProcessAndTest(CreateFileWithAbsolutePathByTenant2, &info);
}
