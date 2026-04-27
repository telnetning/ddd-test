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

#include "ut_vfs_module.h"

/**
 * Test init vfs module twice error and init vfs module after exit
 */
TEST_F(VfsModuleTest, InitVfsModule001)
{
    ASSERT_EQ(InitVfsModule(NULL_MEM_ALLOCATOR), 0);
    /* Cannot init vfs module twice */
    ASSERT_NE(InitVfsModule(NULL_MEM_ALLOCATOR), 0);
    ASSERT_EQ(ExitVfsModule(), 0);
    /* Can init vfs module after previous exit */
    ASSERT_EQ(InitVfsModule(NULL_MEM_ALLOCATOR), 0);
    ASSERT_EQ(ExitVfsModule(), 0);
}

/**
 * Check all vfs interface return error code if vfs module is not init
 */
TEST_F(VfsModuleTest, InitVfsModule002)
{
    ErrorCode expectErrCode = VFS_ERROR_VFS_MODULE_NOT_INIT;
    VfsLibHandle *testLibHandle;
    VfsControlInfo controlInfo;
    VirtualFileSystem *testVfs;
    FileDescriptor *testFd;
    int64_t testOffset;
    bool output;
    char testBuf[SIZE_8K];
    FileControlInfo testControlInfo;
    const char *storeSpaceNames[STORESPACE_NAME_MAX_LEN] = {"storeSpaceName1", "storeSpaceName2", "storeSpaceName3"};
    StoreSpaceAttr storeSpaceAttr = {
        .attrFlags = MEDIUM_DISK_PERF_FLAG,
        .maxSize = 3,
        .reserved = 0
    };
    uint32 count = sizeof(storeSpaceNames)/sizeof(storeSpaceNames[0]);
    FileParameter param = {
        "storeSpaceName",
        0,
        APPEND_WRITE_FILE,
        DATA_FILE_TYPE,
        0,
        0,
        0,
        FILE_READ_AND_WRITE_MODE,
        false
    };
    int sourceFileSize = 0; 
    ASSERT_EQ(GetStaticLocalVfsInstance(&testVfs), expectErrCode);
    ASSERT_EQ(LoadVfsLib("", NULL_MEM_ALLOCATOR, &testLibHandle), expectErrCode);
    ASSERT_EQ(OffloadVfsLib(testLibHandle), expectErrCode);
    ASSERT_EQ(SetVfsLibConfig(testLibHandle, "para", "value"), expectErrCode);
    ASSERT_EQ(GetVfsLibConfig(testLibHandle, "para", testBuf, SIZE_8K), expectErrCode);
    ASSERT_EQ(InitVfsLib(testLibHandle, &testLibPara), expectErrCode);
    ASSERT_EQ(UpdateVfsLibParameter(nullptr, &testLibPara), expectErrCode);
    ASSERT_EQ(CreateVfs(nullptr, TEST_TENANT_NAME, testVfsName, VFS_DEFAULT_ATTR_FLAG), expectErrCode);
    ASSERT_EQ(DropVfs(nullptr, TEST_TENANT_NAME, testVfsName), expectErrCode);
    ASSERT_EQ(MountVfs(nullptr, TEST_TENANT_NAME, testVfsName, &testVfs), expectErrCode);
    ASSERT_EQ(UnmountVfs(testVfs), expectErrCode);
    ASSERT_EQ(VfsControl(nullptr, TEST_TENANT_NAME, SET_VFS_IO_FENCE_WHITELIST, &controlInfo, 1), expectErrCode);
    ASSERT_EQ(CreateSnapshot(nullptr, testClusterName, testVfsName, testSnapshotName, SNAPSHOT_ALL_FILE_FLAG),
              expectErrCode);
    ASSERT_EQ(DropSnapshot(nullptr, testClusterName, testVfsName, testSnapshotName), expectErrCode);
    ASSERT_EQ(RollbackSnapshot(nullptr, testClusterName, testVfsName, testSnapshotName), expectErrCode);
    ASSERT_EQ(CreateStoreSpace(nullptr, TEST_TENANT_NAME, storeSpaceNames, &storeSpaceAttr, count), expectErrCode);
    ASSERT_EQ(DeleteStoreSpace(nullptr, TEST_TENANT_NAME, storeSpaceNames, count), expectErrCode);
    ASSERT_EQ(Create(testVfs, "file", testFilePara, &testFd), expectErrCode);
    ASSERT_EQ(Remove(testVfs, "file"), expectErrCode);
    ASSERT_EQ(FileIsExist(testVfs, "file", &output), expectErrCode);
    ASSERT_EQ(Open(testVfs, "file", FILE_READ_AND_WRITE_FLAG, &testFd), expectErrCode);
    ASSERT_EQ(OpenSnapshot(testVfs, testFileOpenPara, &testFd), expectErrCode);
    ASSERT_EQ(RenameFile(testVfs, "srcFilePath", "destFilePath"), expectErrCode);
    ASSERT_EQ(Close(testFd), expectErrCode);
    ASSERT_EQ(Fsync(testFd), expectErrCode);
    ASSERT_EQ(FileSeek(testFd, 0, FILE_SEEK_SET, &testOffset), expectErrCode);
    ASSERT_EQ(Rewind(testFd), expectErrCode);
    ASSERT_EQ(Read(testFd, testBuf, SIZE_8K, &testOffset), expectErrCode);
    ASSERT_EQ(Pread(testFd, testBuf, SIZE_8K, 0, &testOffset), expectErrCode);
    ASSERT_EQ(SnapshotPread(testFd, testBuf, SIZE_8K, 0, nullptr), expectErrCode);
    ASSERT_EQ(WriteSync(testFd, testBuf, SIZE_8K, &testOffset), expectErrCode);
    ASSERT_EQ(PwriteSync(testFd, testBuf, SIZE_8K, 0, &testOffset), expectErrCode);
    ASSERT_EQ(PwriteAsync(testFd, testBuf, SIZE_8K, 0, nullptr), expectErrCode);
    ASSERT_EQ(SnapshotPwriteSync(testFd, testBuf, SIZE_8K, 0, &testOffset), expectErrCode);
    ASSERT_EQ(SnapshotPwriteAsync(testFd, testBuf, SIZE_8K, 0, nullptr), expectErrCode);
    ASSERT_EQ(WriteAsync(testFd, testBuf, SIZE_8K, nullptr), expectErrCode);
    ASSERT_EQ(Extend(testFd, 0), expectErrCode);
    ASSERT_EQ(Truncate(testFd, 0), expectErrCode);
    ASSERT_EQ(GetSize(testFd, &testOffset), expectErrCode);
    ASSERT_EQ(FileControl(testFd, SET_FILE_FLUSH_CALLBACK, &testControlInfo), expectErrCode);
    ASSERT_EQ(LockFile(testFd, 0, 1, FILE_EXCLUSIVE_LOCK, 0), expectErrCode);
    ASSERT_EQ(TryLockFile(testFd, 0, 1, FILE_EXCLUSIVE_LOCK), expectErrCode);
    ASSERT_EQ(UnlockFile(testFd, 0, 1), expectErrCode);
    ASSERT_EQ(InitFileLock(testVfs, "file", "storespace", &testFd), expectErrCode);
    ASSERT_EQ(FileLock(testFd), expectErrCode);
    ASSERT_EQ(FileUnLock(testFd), expectErrCode);
    ASSERT_EQ(DestoryFileLock(testVfs, "file", testFd), expectErrCode);
    ASSERT_EQ(CopyFile(testVfs, "SourceFile", testVfs, "TargetFile", "storespace"), expectErrCode);
    ASSERT_EQ(FDataSync(testFd), expectErrCode);
    EXPECT_EQ(strcmp(GetVfsErrMsg(expectErrCode), "vfs module does not init"), 0);
}

TEST_F(VfsModuleTest, InvalidParameterTest001)
{
    InitVfsModule(nullptr);
    VirtualFileSystem *vfs = nullptr;
    ASSERT_EQ(GetStaticLocalVfsInstance(nullptr), VFS_ERROR_PARAMETERS_INVALID);
    EXPECT_EQ(strcmp(GetVfsErrMsg(VFS_ERROR_PARAMETERS_INVALID), "invalid argument"), 0);
    ASSERT_EQ(GetStaticLocalVfsInstance(&vfs), 0);
    ExitVfsModule();
}

TEST_F(VfsModuleTest, InvalidParameterTest002)
{
    ErrorCode expectErrCode = VFS_ERROR_INVALID_ARGUMENT;
    char testBuf[SIZE_8K];

    InitVfsModule(nullptr);
    const char *storeSpaceNames[STORESPACE_NAME_MAX_LEN] = {"storeSpaceName1"};
    StoreSpaceAttr storeSpaceAttr = {
        .attrFlags = MEDIUM_DISK_PERF_FLAG,
        .maxSize = 1,
        .reserved = 0
    };
    uint32 count = sizeof(storeSpaceNames)/sizeof(storeSpaceNames[0]);

    EXPECT_EQ(LoadVfsLib("", nullptr, nullptr), expectErrCode);
    EXPECT_EQ(OffloadVfsLib(nullptr), expectErrCode);
    EXPECT_EQ(SetVfsLibConfig(nullptr, "para", "value"), expectErrCode);
    EXPECT_EQ(GetVfsLibConfig(nullptr, "para", testBuf, SIZE_8K), expectErrCode);
    EXPECT_EQ(InitVfsLib(nullptr, nullptr), expectErrCode);
    EXPECT_EQ(StopVfsLib(nullptr), expectErrCode);
    EXPECT_EQ(UpdateVfsLibParameter(nullptr, nullptr), expectErrCode);
    EXPECT_EQ(CreateVfs(nullptr, nullptr, testVfsName, VFS_DEFAULT_ATTR_FLAG), expectErrCode);
    EXPECT_EQ(DropVfs(nullptr, nullptr, testVfsName), expectErrCode);
    EXPECT_EQ(MountVfs(nullptr, TEST_TENANT_NAME, testVfsName, nullptr), expectErrCode);
    EXPECT_EQ(UnmountVfs(nullptr), expectErrCode);
    EXPECT_EQ(VfsControl(nullptr, TEST_TENANT_NAME, SET_VFS_IO_FENCE_WHITELIST, nullptr, 0), expectErrCode);
    EXPECT_EQ(CreateSnapshot(nullptr, testClusterName, nullptr, testSnapshotName, SNAPSHOT_ALL_FILE_FLAG),
              expectErrCode);
    EXPECT_EQ(DropSnapshot(nullptr, testClusterName, nullptr, testSnapshotName), expectErrCode);
    EXPECT_EQ(RollbackSnapshot(nullptr, testClusterName, nullptr, testSnapshotName), expectErrCode);
    EXPECT_EQ(CreateStoreSpace(nullptr, TEST_TENANT_NAME, nullptr, &storeSpaceAttr, count), expectErrCode);
    EXPECT_EQ(DeleteStoreSpace(nullptr, TEST_TENANT_NAME, nullptr, count), expectErrCode);
    EXPECT_EQ(CreateVfsClientHandle(nullptr, TEST_TENANT_NAME, 0, nullptr, nullptr), expectErrCode);
    EXPECT_EQ(DeleteVfsClientHandle(nullptr), expectErrCode);
    EXPECT_EQ(SetVfsClientConfig(nullptr, nullptr, nullptr), expectErrCode);
    EXPECT_EQ(GetVfsClientConfig(nullptr, nullptr, nullptr, 0), expectErrCode);
    EXPECT_EQ(GetVfsLibClientId(nullptr, nullptr), expectErrCode);
    VfsLibHandlerRemoteMsg(nullptr, nullptr, nullptr);
    EXPECT_EQ(CreateStoreTenant(nullptr, nullptr, nullptr), expectErrCode);
    EXPECT_EQ(DeleteStoreTenant(nullptr, nullptr, nullptr, 0), expectErrCode);
    EXPECT_EQ(UpdateStoreSpace(nullptr, TEST_TENANT_NAME, nullptr, nullptr, 0), expectErrCode);
    EXPECT_EQ(QueryStoreSpaceAttr(nullptr, TEST_TENANT_NAME, nullptr, nullptr, 0), expectErrCode);
    EXPECT_EQ(QueryStoreSpaceUsedSize(nullptr, TEST_TENANT_NAME, nullptr, nullptr, 0), expectErrCode);

    ExitVfsModule();
}

TEST_F(VfsModuleTest, GetVfsErrMsgTest)
{
    EXPECT_EQ(strcmp(GetVfsErrMsg(ERROR_SYS_OK), "sys ok"), 0);

#define VFS_ERROR_TEST_NOT_EXIST_ERROR \
    MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_VFS_MODULE_ID, 0xAAAA)
    EXPECT_EQ(strcmp(GetVfsErrMsg(VFS_ERROR_TEST_NOT_EXIST_ERROR), "unknown error"), 0);

#define VFS_ERROR_TEST_MODULE_NOT_EXIST_ERROR \
    MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_EVENT_MODULE_ID, 0x0001)
    EXPECT_EQ(strcmp(GetVfsErrMsg(VFS_ERROR_TEST_MODULE_NOT_EXIST_ERROR), "not vfs module error"), 0);

#define VFS_ERROR_TEST_REMOTE_ERROR \
    MAKE_ERROR_CODE(UTILS_COMPONENT_ID, UTILS_VFS_MODULE_ID, 0x00010001)
    EXPECT_EQ(strcmp(GetVfsErrMsg(VFS_ERROR_TEST_REMOTE_ERROR), "remote vfs error"), 0);

    EXPECT_EQ(strcmp(GetVfsErrMsg(VFS_ERROR_INVALID_ARGUMENT), "invalid argument"), 0);

#undef VFS_ERROR_TEST_REMOTE_ERROR
#undef VFS_ERROR_TEST_MODULE_NOT_EXIST_ERROR
#undef VFS_ERROR_TEST_NOT_EXIST_ERROR
}