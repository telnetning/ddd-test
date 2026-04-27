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

#include <string>
#include "gtest/gtest.h"
#include "securec.h"
#include "fault_injection/fault_injection.h"
#include "vfs/vfs_interface.h"
#include "vfs/vfs_linux_common.h"
#include "vfs/vfs_utils.h"

using std::string;

namespace {

const char * const TEST_TENANT_NAME = "Tenant";
const char * const TEST_VFS_NAME = "MockVfs";
const string TEST_VFS_DIRECTORY = "./" + string(TEST_VFS_NAME);

#ifdef COMPILE_LIBRARY_DIR
#define TEST_LIBRARY_DIRECTORY  COMPILE_LIBRARY_DIR
#else
#define TEST_LIBRARY_DIRECTORY  "../lib"
#endif

#define DYNAMIC_VFS_LIBRARY_NAME  "libvfslinuxadapter.so"
#define TEST_SNAPSHOT_NAME "SnapshotName"

}

#ifdef ENABLE_FAULT_INJECTION

static void MockDynamicVfsInit(const FaultInjectionEntry *entry, const VfsAdapterParam *param)
{
    printf("Init dynamic vfs without chroot() for mock test\n");
}

static void MockGenerateVfsDirectory(const FaultInjectionEntry *entry, char *fullPath, size_t pathMaxLen,
    const char *vfsName)
{
    /* Generate vfs directory in current working directory for mock test */
    sprintf_s(fullPath, pathMaxLen, "./%s/", vfsName);
}

class VfsMockDynamicTest : public ::testing::Test {
protected:
    static void SetUpTestCase()
    {
        FaultInjectionEntry entries[] = {
            FAULT_INJECTION_ENTRY(MOCK_DYNAMIC_VFS_INIT, true, MockDynamicVfsInit),
            FAULT_INJECTION_ENTRY(MOCK_GENERATE_VFS_DIRECTORY, true, MockGenerateVfsDirectory),
        };
        ASSERT_EQ(RegisterFaultInjection(entries, sizeof(entries) / sizeof(entries[0]), FI_GLOBAL), ERROR_SYS_OK);
    }

    static void TearDownTestCase()
    {
        DestroyFaultInjectionHash(FI_GLOBAL);
    }

    void SetUp() override
    {
        RemoveTestDirectory();
        dynamicLibPath = string(TEST_LIBRARY_DIRECTORY) + string("/") + string(DYNAMIC_VFS_LIBRARY_NAME);
        ASSERT_EQ(InitVfsModule(nullptr), 0);
        LoadEmptyVfsLib(&vfsEmptyLibHandle);
        LoadEmptyVfsClient(&vfsEmptyClientHandle);
        PrepareFakeVfs(&vfs);
        PrepareFakeFd(&fd);
    }

    void TearDown() override
    {
        ASSERT_EQ(ExitVfsModule(), 0);
        RemoveTestDirectory();
        DeleteVfsClientHandle(vfsEmptyClientHandle);
        OffloadEmptyVfsLib(vfsEmptyLibHandle);
        ReleaseFakeVfs(vfs);
        ReleaseFakeFd(fd);
    }

    void InitDynamicLib()
    {
        ASSERT_EQ(LoadVfsLib(dynamicLibPath.c_str(), nullptr, &vfsLibHandle), 0);
        VfsLibParameter vfsLibParameter = {
            .pageSize = 8192,
            .dbType = 1,
            .storageServerAddr = {0},
            .localIp = {0},
            .clusterName = {0},
            .localServiceType = 0,
            .clientId = 0,
            .clientTimeLineId = 0,
        };
        strcpy_s(vfsLibParameter.storageServerAddr, sizeof(vfsLibParameter.storageServerAddr), "./");
        ASSERT_EQ(InitVfsLib(vfsLibHandle, &vfsLibParameter), 0);
    }

    void ExitDynamicLib()
    {
        ASSERT_EQ(StopVfsLib(vfsLibHandle), VFS_ERROR_OPERATION_NOT_SUPPORT);
        ASSERT_EQ(OffloadVfsLib(vfsLibHandle), 0);
    }

    void LoadEmptyVfsLib(VfsLibHandle **vfsEmptyLibHandle)
    {
        *vfsEmptyLibHandle = (VfsLibHandle*) VfsMemAlloc(nullptr, sizeof(VfsLibHandle));
        (*vfsEmptyLibHandle)->ops = (VfsAdapterInterface*) VfsMemAlloc(nullptr, sizeof(VfsAdapterInterface));
        (void)memset_s((*vfsEmptyLibHandle)->ops, sizeof(VfsAdapterInterface), 0, sizeof(VfsAdapterInterface));
    }

    void OffloadEmptyVfsLib(VfsLibHandle *vfsEmptyLibHandle)
    {
        VfsMemFree(nullptr, vfsEmptyLibHandle->ops);
        VfsMemFree(nullptr, vfsEmptyLibHandle);
    }

    void LoadEmptyVfsClient(VfsClientHandle **vfsEmptyClientHandle)
    {
        *vfsEmptyClientHandle = (VfsClientHandle*) VfsMemAlloc(nullptr, sizeof(VfsClientHandle));
        (*vfsEmptyClientHandle)->ops = (VfsAdapterInterface*) VfsMemAlloc(nullptr, sizeof(VfsAdapterInterface));
        (void)memset_s((*vfsEmptyClientHandle)->ops, sizeof(VfsAdapterInterface), 0, sizeof(VfsAdapterInterface));
    }

    void OffloadEmptyVfsClient(VfsClientHandle *vfsEmptyClientHandle)
    {
        VfsMemFree(nullptr, vfsEmptyClientHandle->ops);
        VfsMemFree(nullptr, vfsEmptyClientHandle);
    }

    void PrepareFakeVfs(VirtualFileSystem **vfs)
    {
        *vfs = (VirtualFileSystem*) VfsMemAlloc(nullptr, sizeof(VirtualFileSystem));
        (*vfs)->ops = (VfsAdapterInterface*) VfsMemAlloc(nullptr, sizeof(VfsAdapterInterface));
        (void)memset_s((*vfs)->ops, sizeof(VfsAdapterInterface), 0, sizeof(VfsAdapterInterface));
        (*vfs)->isStaticVfs = false;
    }

    void ReleaseFakeVfs(VirtualFileSystem *vfs)
    {
        VfsMemFree(nullptr, vfs->ops);
        VfsMemFree(nullptr, vfs);
    }

    void PrepareFakeFd(FileDescriptor **fd)
    {
        *fd = (FileDescriptor*) VfsMemAlloc(nullptr, sizeof(FileDescriptor));
        (*fd)->ops = (VfsAdapterInterface*) VfsMemAlloc(nullptr, sizeof(VfsAdapterInterface));
        (void)memset_s((*fd)->ops, sizeof(VfsAdapterInterface), 0, sizeof(VfsAdapterInterface));
    }

    void ReleaseFakeFd(FileDescriptor *fd)
    {
        VfsMemFree(nullptr, fd->ops);
        VfsMemFree(nullptr, fd);
    }

    string dynamicLibPath;
    VfsLibHandle *vfsLibHandle = nullptr;
    VfsLibParameter vfsLibParameter = {
        .pageSize = 8192,
        .dbType = 1,
        .storageServerAddr = {0},
        .localIp = {0},
        .clusterName = {0},
        .localServiceType = 0,
        .clientId = 0,
        .clientTimeLineId = 0,
    };
    FileParameter filePara = {
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
    StoreSpaceAttr storeSpaceAttr = {
        .attrFlags = MEDIUM_DISK_PERF_FLAG,
        .maxSize = 0xFFFFFFFFU,
        .reserved = 0,
    };
    char storeSpaceNames[1][STORESPACE_NAME_MAX_LEN] = {"StoreSpaceName"};
    VfsLibHandle *vfsEmptyLibHandle;
    VfsClientHandle *vfsEmptyClientHandle;
    VirtualFileSystem *vfs;
    FileDescriptor *fd;

private:
    static void RemoveTestDirectory()
    {
        string cmd = "rm -rf " + TEST_VFS_DIRECTORY;
        system(cmd.c_str());
    }
};

TEST_F(VfsMockDynamicTest, InitVfsLibTest)
{
    ASSERT_EQ(LoadVfsLib(dynamicLibPath.c_str(), nullptr, &vfsLibHandle), 0);
    strcpy_s(vfsLibParameter.storageServerAddr, sizeof(vfsLibParameter.storageServerAddr), "./");
    char testBuf[10];
    ASSERT_EQ(SetVfsLibConfig(vfsLibHandle, "para", "value"), 0);
    ASSERT_EQ(GetVfsLibConfig(vfsLibHandle, "para", testBuf, sizeof(testBuf)), 0);
    ASSERT_EQ(InitVfsLib(vfsLibHandle, &vfsLibParameter), 0);
    ASSERT_EQ(UpdateVfsLibParameter(nullptr, &vfsLibParameter), 0);
    ASSERT_EQ(OffloadVfsLib(vfsLibHandle), 0);
}

TEST_F(VfsMockDynamicTest, CreateVfsTest)
{
    InitDynamicLib();
    ASSERT_EQ(CreateVfs(nullptr, TEST_TENANT_NAME, TEST_VFS_NAME, VFS_DEFAULT_ATTR_FLAG), 0);
    /* Create vfs with same name, return VFS_IS_EXIST error code */
    ASSERT_EQ(CreateVfs(nullptr, TEST_TENANT_NAME, TEST_VFS_NAME, VFS_DEFAULT_ATTR_FLAG), VFS_ERROR_VFS_IS_EXIST);
    ASSERT_EQ(DropVfs(nullptr, TEST_TENANT_NAME, TEST_VFS_NAME), 0);
    ASSERT_EQ(DropVfs(nullptr, TEST_TENANT_NAME, TEST_VFS_NAME), VFS_ERROR_VFS_NAME_NOT_EXIST);
    ExitDynamicLib();
}

TEST_F(VfsMockDynamicTest, MountVfsTest)
{
    InitDynamicLib();
    VirtualFileSystem *vfs = nullptr;
    ASSERT_EQ(MountVfs(nullptr, TEST_TENANT_NAME, TEST_VFS_NAME, &vfs), VFS_ERROR_VFS_NOT_EXIST);
    ASSERT_EQ(vfs, nullptr);
    ASSERT_EQ(CreateVfs(nullptr, TEST_TENANT_NAME, TEST_VFS_NAME, VFS_DEFAULT_ATTR_FLAG), 0);
    VfsControlInfo controlInfo;
    strcpy_s(controlInfo.vfsName, sizeof(controlInfo.vfsName), TEST_VFS_NAME);
    ASSERT_EQ(VfsControl(nullptr, TEST_TENANT_NAME, SET_VFS_IO_FENCE_WHITELIST, &controlInfo, 1), 0);
    ASSERT_EQ(MountVfs(nullptr, TEST_TENANT_NAME, TEST_VFS_NAME, &vfs), 0);
    ASSERT_EQ(UnmountVfs(vfs), 0);
    ASSERT_EQ(DropVfs(nullptr, TEST_TENANT_NAME, TEST_VFS_NAME), 0);
    ExitDynamicLib();
}

TEST_F(VfsMockDynamicTest, CreateVfsClientHandleTest)
{
    InitDynamicLib();
    ASSERT_EQ(CreateVfsClientHandle(vfsLibHandle, vfsLibParameter.clusterName, 0, vfsLibParameter.storageServerAddr,
                                    &vfsEmptyClientHandle), 0);
    ASSERT_EQ(DeleteVfsClientHandle(vfsEmptyClientHandle), 0);
}

TEST_F(VfsMockDynamicTest, TestOffloadVfsLibIsNull)
{
    ASSERT_EQ(OffloadVfsLib(vfsEmptyLibHandle), VFS_ERROR_OPERATION_NOT_SUPPORT);
}

TEST_F(VfsMockDynamicTest, TestSetVfsLibConfigIsNull)
{
    ASSERT_EQ(SetVfsLibConfig(vfsEmptyLibHandle, "para", "value"), VFS_ERROR_OPERATION_NOT_SUPPORT);
}

TEST_F(VfsMockDynamicTest, TestGetVfsLibConfigIsNull)
{
    char testBuf[10];
    ASSERT_EQ(GetVfsLibConfig(vfsEmptyLibHandle, "para", testBuf, sizeof(testBuf)), VFS_ERROR_OPERATION_NOT_SUPPORT);
}

TEST_F(VfsMockDynamicTest, TestInitVfsLibIsNull)
{
    ASSERT_EQ(InitVfsLib(vfsEmptyLibHandle, &vfsLibParameter), VFS_ERROR_OPERATION_NOT_SUPPORT);
}

TEST_F(VfsMockDynamicTest, TestUpdateVfsLibParameterIsNull)
{
    ASSERT_EQ(UpdateVfsLibParameter(vfsEmptyClientHandle, &vfsLibParameter), VFS_ERROR_OPERATION_NOT_SUPPORT);
}

TEST_F(VfsMockDynamicTest, TestCreateVfsIsNull)
{
    ASSERT_EQ(CreateVfs(vfsEmptyClientHandle, TEST_TENANT_NAME, TEST_VFS_NAME, VFS_DEFAULT_ATTR_FLAG), VFS_ERROR_OPERATION_NOT_SUPPORT);
}

TEST_F(VfsMockDynamicTest, TestDropVfsIsNull)
{
    ASSERT_EQ(DropVfs(vfsEmptyClientHandle, TEST_TENANT_NAME, TEST_VFS_NAME), VFS_ERROR_OPERATION_NOT_SUPPORT);
}

TEST_F(VfsMockDynamicTest, TestMountVfsIsNull)
{
    ASSERT_EQ(MountVfs(vfsEmptyClientHandle, TEST_TENANT_NAME, TEST_VFS_NAME, &vfs), VFS_ERROR_OPERATION_NOT_SUPPORT);
}

TEST_F(VfsMockDynamicTest, TestUnmountVfsIsNull)
{
    ASSERT_EQ(UnmountVfs(vfs), VFS_ERROR_OPERATION_NOT_SUPPORT);
}

TEST_F(VfsMockDynamicTest, TestVfsControlIsNull)
{
    VfsControlInfo controlInfo;
    strcpy_s(controlInfo.vfsName, sizeof(controlInfo.vfsName), TEST_VFS_NAME);
    ASSERT_EQ(VfsControl(vfsEmptyClientHandle, TEST_TENANT_NAME, SET_VFS_IO_FENCE_WHITELIST, &controlInfo, 1),
        VFS_ERROR_OPERATION_NOT_SUPPORT);
}

TEST_F(VfsMockDynamicTest, TestCreateSnapshotIsNull)
{
    ASSERT_EQ(CreateSnapshot(vfsEmptyClientHandle, TEST_TENANT_NAME, TEST_VFS_NAME, TEST_SNAPSHOT_NAME,
                             VFS_DEFAULT_ATTR_FLAG), VFS_ERROR_OPERATION_NOT_SUPPORT);
}

TEST_F(VfsMockDynamicTest, TestDropSnapshotIsNull)
{
    ASSERT_EQ(DropSnapshot(vfsEmptyClientHandle, TEST_TENANT_NAME, TEST_VFS_NAME, TEST_SNAPSHOT_NAME),
        VFS_ERROR_OPERATION_NOT_SUPPORT);
}

TEST_F(VfsMockDynamicTest, TestRollbackSnapshotIsNull)
{
    ASSERT_EQ(RollbackSnapshot(vfsEmptyClientHandle, TEST_TENANT_NAME, TEST_VFS_NAME, TEST_SNAPSHOT_NAME),
        VFS_ERROR_OPERATION_NOT_SUPPORT);
}

TEST_F(VfsMockDynamicTest, TestCreateStoreSpaceIsNull)
{

    ASSERT_EQ(
        CreateStoreSpace(vfsEmptyClientHandle, TEST_TENANT_NAME, (const char **)storeSpaceNames, &storeSpaceAttr, 1),
        VFS_ERROR_OPERATION_NOT_SUPPORT);
}

TEST_F(VfsMockDynamicTest, TestDeleteStoreSpaceIsNull)
{
    ASSERT_EQ(DeleteStoreSpace(vfsEmptyClientHandle, TEST_TENANT_NAME, (const char **)storeSpaceNames, 1),
        VFS_ERROR_OPERATION_NOT_SUPPORT);
}

TEST_F(VfsMockDynamicTest, TestCreateIsNull)
{
    FileDescriptor *fd;
    ASSERT_EQ(Create(vfs, "filename", filePara, &fd), VFS_ERROR_OPERATION_NOT_SUPPORT);
}

TEST_F(VfsMockDynamicTest, TestRemoveIsNull)
{
    ASSERT_EQ(Remove(vfs, "filename"), VFS_ERROR_OPERATION_NOT_SUPPORT);
}

TEST_F(VfsMockDynamicTest, TestFileIsExist)
{
    bool fileIsExist = false;
    ASSERT_EQ(FileIsExist(vfs, "filename", &fileIsExist), VFS_ERROR_OPERATION_NOT_SUPPORT);
}

TEST_F(VfsMockDynamicTest, TestOpenIsNull)
{
    ASSERT_EQ(Open(vfs, "filename", FILE_READ_AND_WRITE_FLAG, &fd), VFS_ERROR_OPERATION_NOT_SUPPORT);
}

TEST_F(VfsMockDynamicTest, TestOpenSnapshotIsNull)
{
    const char *testFilePath = "test_vfs_linux_adapter.data";
    FileOpenParam openPara = {.flags = FILE_READ_ONLY_FLAG, .filePath = testFilePath,
                              .snapshotName = TEST_SNAPSHOT_NAME};
    ASSERT_EQ(OpenSnapshot(vfs, openPara, &fd), VFS_ERROR_OPERATION_NOT_SUPPORT);
}

TEST_F(VfsMockDynamicTest, TestRenameFileIsNull)
{
    const char *srcFilePath = "srcFileName";
    const char *destFilePath = "destFileName";
    ASSERT_EQ(RenameFile(vfs, srcFilePath, destFilePath), VFS_ERROR_OPERATION_NOT_SUPPORT);
}

TEST_F(VfsMockDynamicTest, TestCloseIsNull)
{
    ASSERT_EQ(Close(fd), VFS_ERROR_OPERATION_NOT_SUPPORT);
}

TEST_F(VfsMockDynamicTest, TestFsyncIsNull)
{
    ASSERT_EQ(Fsync(fd), VFS_ERROR_OPERATION_NOT_SUPPORT);
}

TEST_F(VfsMockDynamicTest, TestFileSeekIsNull)
{
    int64_t fileCurPos = -1;
    ASSERT_EQ(FileSeek(fd, 0, FILE_SEEK_CUR, &fileCurPos), VFS_ERROR_OPERATION_NOT_SUPPORT);
}

TEST_F(VfsMockDynamicTest, TestRewindIsNull)
{
    ASSERT_EQ(Rewind(fd), VFS_ERROR_OPERATION_NOT_SUPPORT);
}

TEST_F(VfsMockDynamicTest, TestReadIsNull)
{
    char readData[10];
    int64_t readSize = 0;
    ASSERT_EQ(Read(fd, readData, 10, &readSize), VFS_ERROR_OPERATION_NOT_SUPPORT);
}

TEST_F(VfsMockDynamicTest, TestPreadIsNull)
{
    char readData[10];
    int64_t readSize = 0;
    ASSERT_EQ(Pread(fd, readData, 10, 0, &readSize), VFS_ERROR_OPERATION_NOT_SUPPORT);
}

TEST_F(VfsMockDynamicTest, TestSnapshotPreadIsNull)
{
    char readData[10];
    int64_t readSize = 0;
    DiffContents diffContents;
    ASSERT_EQ(SnapshotPread(fd, readData, 10, 0, &diffContents), VFS_ERROR_OPERATION_NOT_SUPPORT);
}

TEST_F(VfsMockDynamicTest, TestPreadAsyncIsNull)
{
    char readData[10];
    ASSERT_EQ(PreadAsync(fd, readData, 10, 0, nullptr), VFS_ERROR_OPERATION_NOT_SUPPORT);
}

TEST_F(VfsMockDynamicTest, TestWriteSyncIsNull)
{
    char writeData[10];
    int64_t writeSize = 0;
    ASSERT_EQ(WriteSync(fd, writeData, 10, &writeSize), VFS_ERROR_OPERATION_NOT_SUPPORT);
}

TEST_F(VfsMockDynamicTest, TestPwriteSyncIsNull)
{
    char writeData[10];
    int64_t writeSize = 0;
    ASSERT_EQ(PwriteSync(fd, writeData, 10, 0, &writeSize), VFS_ERROR_OPERATION_NOT_SUPPORT);
}

TEST_F(VfsMockDynamicTest, TestWriteAsyncIsNull)
{
    char writeData[10];
    ASSERT_EQ(WriteAsync(fd, writeData, 10, nullptr), VFS_ERROR_OPERATION_NOT_SUPPORT);
}

TEST_F(VfsMockDynamicTest, TestPwriteAsyncIsNull)
{
    char writeData[10];
    ASSERT_EQ(PwriteAsync(fd, writeData, 10, 0, nullptr), VFS_ERROR_OPERATION_NOT_SUPPORT);
}

TEST_F(VfsMockDynamicTest, TestSnapshotPwriteSyncIsNull)
{
    char writeData[10];
    int64_t writeSize = 0;
    ASSERT_EQ(SnapshotPwriteSync(fd, writeData, 10, 0, &writeSize), VFS_ERROR_OPERATION_NOT_SUPPORT);
}

TEST_F(VfsMockDynamicTest, TestSnapshotPwriteAsyncIsNull)
{
    char writeData[10];
    ASSERT_EQ(SnapshotPwriteAsync(fd, writeData, 10, 0, nullptr), VFS_ERROR_OPERATION_NOT_SUPPORT);
}

TEST_F(VfsMockDynamicTest, TestExtendIsNull)
{
    ASSERT_EQ(Extend(fd, 10), VFS_ERROR_OPERATION_NOT_SUPPORT);
}

TEST_F(VfsMockDynamicTest, TestTruncateIsNull)
{
    ASSERT_EQ(Truncate(fd, 0), VFS_ERROR_OPERATION_NOT_SUPPORT);
}

TEST_F(VfsMockDynamicTest, TestGetSizeIsNull)
{
    int64_t fileSize = 0;
    EXPECT_EQ(GetSize(fd, &fileSize), VFS_ERROR_OPERATION_NOT_SUPPORT);
}

TEST_F(VfsMockDynamicTest, TestFileControlIsNull)
{
    FileControlInfo controlInfo = {0};
    ASSERT_EQ(FileControl(fd, SET_FILE_FLUSH_CALLBACK, &controlInfo), VFS_ERROR_OPERATION_NOT_SUPPORT);
}

TEST_F(VfsMockDynamicTest, TestLockFileIsNull)
{
    ASSERT_EQ(LockFile(fd, 0, DEFAULT_RANGE_SIZE, FILE_EXCLUSIVE_LOCK, 0), VFS_ERROR_OPERATION_NOT_SUPPORT);
}

TEST_F(VfsMockDynamicTest, TestTryLockFileIsNull)
{
    ASSERT_EQ(TryLockFile(fd, 0, DEFAULT_RANGE_SIZE, FILE_EXCLUSIVE_LOCK), VFS_ERROR_OPERATION_NOT_SUPPORT);
}

TEST_F(VfsMockDynamicTest, TestUnlockFileIsNull)
{
    ASSERT_EQ(UnlockFile(fd, 0, DEFAULT_RANGE_SIZE), VFS_ERROR_OPERATION_NOT_SUPPORT);
}

TEST_F(VfsMockDynamicTest, TestFDataSyncIsNull)
{
    ASSERT_EQ(FDataSync(fd), VFS_ERROR_OPERATION_NOT_SUPPORT);
}

#endif /* ENABLE_FAULT_INJECTION */
