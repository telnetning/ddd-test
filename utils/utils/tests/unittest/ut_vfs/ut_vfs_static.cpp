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

#include <fstream>
#include <iostream>
#include "ut_vfs_static.h"
#include "vfs/vfs_linux_static.h"
#include "vfs/vfs_utils.h"
#include "vfs/vfs_linux_aio.h"

/**
 * Create a single file with invalid arguments, return VFS_ERROR_PARAMETERS_INVALID
 * Create a single file with valid arguments, then attempt to create another file with same name
 */
TEST_F(VfsStaticLinuxTest, CreateFile001)
{
    ASSERT_EQ(Create(nullptr, GetTestFilePath(), testFilePara, &testFd), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(Create(testVfs, nullptr, testFilePara, &testFd), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(Create(testVfs, emptyStr, testFilePara, &testFd), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(Create(testVfs, invalidFilePath, testFilePara, &testFd), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(Create(testVfs, GetTestFilePath(), testFilePara, nullptr), VFS_ERROR_PARAMETERS_INVALID);
    FileParameter invalidParam = testFilePara;
    invalidParam.mode = 0;
    ASSERT_EQ(Create(testVfs, GetTestFilePath(), invalidParam, &testFd), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(Create(GetInvalidVfsHandle(), GetTestFilePath(), testFilePara, &testFd),
        VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(Create(testVfs, GetTestFilePath(), testFilePara, &testFd), 0);
    ASSERT_EQ(Create(testVfs, GetTestFilePath(), testFilePara, &testFd), VFS_ERROR_CREATE_FILE_EXIST);
    EXPECT_EQ(strcmp(GetVfsErrMsg(VFS_ERROR_FILE_IS_EXIST), "file already exist"), 0);
    ASSERT_EQ(Close(testFd), 0);
    ASSERT_EQ(Remove(testVfs, GetTestFilePath()), 0);
}

/**
 * Create 65536 different files sequentially without close any fds
 */
TEST_F(VfsStaticLinuxTest, CreateFile002)
{
    FileDescriptor *fdList[MAX_FILE_ID + 1];
    for (int fileId = 0; fileId <= MAX_FILE_ID; ++fileId) {
        std::string filePath = GenerateTestFilePath(utDataDir, fileId);
        ASSERT_EQ(Create(testVfs, filePath.c_str(), testFilePara, &(fdList[fileId])), 0);
    }
    for (int fileId = 0; fileId <= MAX_FILE_ID; ++fileId) {
        ASSERT_EQ(Close(fdList[fileId]), 0);
        std::string filePath = GenerateTestFilePath(utDataDir, fileId);
        ASSERT_EQ(Remove(testVfs, filePath.c_str()), 0);
    }
}

/**
 * Create file failure test, pathName contains non-exist directory
 */
TEST_F(VfsStaticLinuxTest, CreateFile003)
{
    std::string testDir = "./vfs_test_dir";
    std::string rmCmd = "rm -rf " + testDir;
    system(rmCmd.c_str());
    std::string testDataFilePath = testDir + "/test.data";
    FileDescriptor *fd;
    ASSERT_NE(Create(testVfs, testDataFilePath.c_str(), testFilePara, &fd), 0);
}

/**
 * Create file failure test, using relative path
 */
TEST_F(VfsStaticLinuxTest, CreateFileWithUpperDir)
{
#define TEST_UPPER_DIRCTORY "../log"
#define TEST_UPPER_DIRCTORY_FILE_PATH "../log/utils_unitest.log"
    std::string testDir = TEST_UPPER_DIRCTORY;
    std::string rmCmd = "rm -rf " + testDir;
    system(rmCmd.c_str());
    (void)mkdir(TEST_UPPER_DIRCTORY, S_IRWXU);
    FileDescriptor *fd;
    ASSERT_EQ(Create(testVfs, TEST_UPPER_DIRCTORY_FILE_PATH, testFilePara, &fd), ERROR_SYS_OK);
    (void)Close(fd);
    (void)Remove(testVfs, TEST_UPPER_DIRCTORY_FILE_PATH);
    (void)rmdir(TEST_UPPER_DIRCTORY);
}

/**
 * Remove file with invalid arguments, return VFS_ERROR_PARAMETERS_INVALID
 * Remove file successfully after create, remove non-exist file return VFS_ERROR_REMOVE_FILE_NOT_EXIST
 */
TEST_F(VfsStaticLinuxTest, RemoveFile001)
{
    ASSERT_EQ(Remove(testVfs, emptyStr), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(Remove(testVfs, invalidFilePath), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(Create(testVfs, GetTestFilePath(), testFilePara, &testFd), 0);
    ASSERT_EQ(Close(testFd), 0);
    ASSERT_EQ(Remove(nullptr, GetTestFilePath()), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(Remove(testVfs, nullptr), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(Remove(GetInvalidVfsHandle(), GetTestFilePath()), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(Remove(testVfs, GetTestFilePath()), 0);
    ASSERT_EQ(Remove(testVfs, GetTestFilePath()), VFS_ERROR_REMOVE_FILE_NOT_EXIST);
}

/**
 * Check whether file is exist with invalid arguments, return VFS_ERROR_PARAMETERS_INVALID
 * If file exist, check file existence return true, otherwise return false
 */
TEST_F(VfsStaticLinuxTest, FileIsExist001)
{
    bool output = true;
    ASSERT_EQ(FileIsExist(testVfs, emptyStr, &output), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(FileIsExist(testVfs, invalidFilePath, &output), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(FileIsExist(testVfs, GetTestFilePath(), &output), 0);
    ASSERT_FALSE(output);
    ASSERT_EQ(Create(testVfs, GetTestFilePath(), testFilePara, &testFd), 0);
    ASSERT_EQ(Close(testFd), 0);
    ASSERT_EQ(FileIsExist(nullptr, GetTestFilePath(), &output), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(FileIsExist(testVfs, nullptr, &output), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(FileIsExist(testVfs, GetTestFilePath(), nullptr), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(FileIsExist(GetInvalidVfsHandle(), GetTestFilePath(), &output),
        VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(FileIsExist(testVfs, GetTestFilePath(), &output), 0);
    ASSERT_TRUE(output);
    ASSERT_EQ(Remove(testVfs, GetTestFilePath()), 0);
    ASSERT_EQ(FileIsExist(testVfs, GetTestFilePath(), &output), 0);
    ASSERT_FALSE(output);
}

/**
 * Open non-exist file, return VFS_ERROR_OPEN_FILE_NOT_EXIST
 * Open file with invalid arguments, return VFS_ERROR_PARAMETERS_INVALID
 * After open file with valid arguments successfully, check static vfs open file count
 */
TEST_F(VfsStaticLinuxTest, OpenFile001)
{
    ASSERT_EQ(GetStaticVfsOpenFileCount(), 0);
    ASSERT_EQ(Open(testVfs, GetTestFilePath(), FILE_READ_AND_WRITE_FLAG, &testFd),
              VFS_ERROR_OPEN_FILE_NOT_EXIST);
    ASSERT_EQ(Create(testVfs, GetTestFilePath(), testFilePara, &testFd), 0);
    ASSERT_EQ(GetStaticVfsOpenFileCount(), 1);
    ASSERT_EQ(Close(testFd), 0);
    int invalidFlag = 0;
    ASSERT_EQ(Open(nullptr, GetTestFilePath(), FILE_READ_AND_WRITE_FLAG, &testFd),
              VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(Open(testVfs, nullptr, FILE_READ_AND_WRITE_FLAG, &testFd),
              VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(Open(testVfs, emptyStr, FILE_READ_AND_WRITE_FLAG, &testFd),
              VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(Open(testVfs, invalidFilePath, FILE_READ_AND_WRITE_FLAG, &testFd),
              VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(Open(testVfs, GetTestFilePath(), FILE_READ_AND_WRITE_FLAG, nullptr),
              VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(Open(testVfs, GetTestFilePath(), invalidFlag, &testFd),
              VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(Open(GetInvalidVfsHandle(), GetTestFilePath(), FILE_READ_AND_WRITE_FLAG, &testFd),
              VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(Open(testVfs, GetTestFilePath(), FILE_READ_AND_WRITE_FLAG, &testFd), 0);
    ASSERT_EQ(GetStaticVfsOpenFileCount(), 1);
    ASSERT_EQ(Close(testFd), 0);
    ASSERT_EQ(GetStaticVfsOpenFileCount(), 0);
    ASSERT_EQ(Remove(testVfs, GetTestFilePath()), 0);
}

/**
 * Open 65536 files sequentially without close any fds, all open operation return success
 */
TEST_F(VfsStaticLinuxTest, OpenFile002)
{
    FileDescriptor *fdList[MAX_FILE_ID + 1];
    for (int fileId = 0; fileId <= MAX_FILE_ID; ++fileId) {
        std::string filePath = GenerateTestFilePath(utDataDir, fileId);
        ASSERT_EQ(Create(testVfs, filePath.c_str(), testFilePara, &(fdList[fileId])), 0);
    }
    ASSERT_EQ(GetStaticVfsOpenFileCount(), MAX_FILE_ID + 1);
    for (int fileId = 0; fileId <= MAX_FILE_ID; ++fileId) {
        ASSERT_EQ(Close(fdList[fileId]), 0);
    }
    ASSERT_EQ(GetStaticVfsOpenFileCount(), 0);
    for (int fileId = MAX_FILE_ID; fileId >= 0; --fileId) {
        std::string filePath = GenerateTestFilePath(utDataDir, fileId);
        ASSERT_EQ(Open(testVfs, filePath.c_str(), FILE_READ_AND_WRITE_FLAG, &(fdList[fileId])), 0);
    }
    ASSERT_EQ(GetStaticVfsOpenFileCount(), MAX_FILE_ID + 1);
    for (int fileId = 0; fileId <= MAX_FILE_ID; ++fileId) {
        ASSERT_EQ(Close(fdList[fileId]), 0);
        std::string filePath = GenerateTestFilePath(utDataDir, fileId);
        ASSERT_EQ(Remove(testVfs, filePath.c_str()), 0);
    }
}

/**
 * Close file with invalid arguments, return VFS_ERROR_PARAMETERS_INVALID
 * Close file with valid arguments, return success
 */
TEST_F(VfsStaticLinuxTest, CloseFile001)
{
    ASSERT_EQ(Close(nullptr), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(Create(testVfs, GetTestFilePath(), testFilePara, &testFd), 0);
    ASSERT_EQ(Close(GetInvalidFd()), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(Close(testFd), 0);
    ASSERT_EQ(Open(testVfs, GetTestFilePath(), FILE_READ_AND_WRITE_FLAG, &testFd), 0);
    ASSERT_EQ(Close(testFd), 0);
    ASSERT_EQ(Remove(testVfs, GetTestFilePath()), 0);
}

/**
 * Fsync file with invalid arguments, return VFS_ERROR_PARAMETERS_INVALID
 */
TEST_F(VfsStaticLinuxTest, FsyncTest001)
{
    ASSERT_EQ(Fsync(nullptr), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(Create(testVfs, GetTestFilePath(), testFilePara, &testFd), 0);
    ASSERT_EQ(Fsync(GetInvalidFd()), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(Close(testFd), 0);
    ASSERT_EQ(Remove(testVfs, GetTestFilePath()), 0);
}

/**
 * FDataSync file with invalid arguments, return VFS_ERROR_PARAMETERS_INVALID
 */
TEST_F(VfsStaticLinuxTest, FDataSyncTest001)
{
    ASSERT_EQ(FDataSync(nullptr), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(Create(testVfs, GetTestFilePath(), testFilePara, &testFd), 0);
    ASSERT_EQ(FDataSync(GetInvalidFd()), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(Close(testFd), 0);
    ASSERT_EQ(Remove(testVfs, GetTestFilePath()), 0);
}

/**
 * Seek file with invalid arguments, return VFS_ERROR_PARAMETERS_INVALID
 * Seek file with FILE_SEEK_CUR, FILE_SEEK_SET, FILE_SEEK_END, return success and get current fd position correct
 */
TEST_F(VfsStaticLinuxTest, FileSeekTest001)
{
    CreateTestFile();
    ASSERT_EQ(Extend(testFd, SIZE_8K), 0);
    int invalidSeekFlag = -1;
    int64_t fileCurPos = -1;
    ASSERT_EQ(FileSeek(nullptr, 0, FILE_SEEK_CUR, &fileCurPos), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(FileSeek(testFd, 0, invalidSeekFlag, &fileCurPos), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(FileSeek(GetInvalidFd(), 0, FILE_SEEK_CUR, &fileCurPos), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(FileSeek(testFd, 0, FILE_SEEK_CUR, &fileCurPos), 0);
    ASSERT_EQ(fileCurPos, 0);
    ASSERT_EQ(FileSeek(testFd, 20, FILE_SEEK_CUR, &fileCurPos), 0);
    ASSERT_EQ(fileCurPos, 20);
    ASSERT_EQ(FileSeek(testFd, 30, FILE_SEEK_CUR, &fileCurPos), 0);
    ASSERT_EQ(fileCurPos, 50);
    ASSERT_EQ(FileSeek(testFd, 30, FILE_SEEK_SET, &fileCurPos), 0);
    ASSERT_EQ(fileCurPos, 30);
    ASSERT_EQ(FileSeek(testFd, 0, FILE_SEEK_END, &fileCurPos), 0);
    ASSERT_EQ(fileCurPos, SIZE_8K);
    CloseAndRemoveTestFile();
}

/**
 * Rewind file with invalid arguments, return VFS_ERROR_PARAMETERS_INVALID
 * After rewind file successfully, current fd position will be 0
 */
TEST_F(VfsStaticLinuxTest, RewindTest001)
{
    CreateTestFile();
    ASSERT_EQ(Rewind(nullptr), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(Extend(testFd, SIZE_8K), 0);
    int64_t fileCurPos = -1;
    ASSERT_EQ(FileSeek(testFd, 20, FILE_SEEK_SET, &fileCurPos), 0);
    ASSERT_EQ(fileCurPos, 20);
    ASSERT_EQ(Rewind(testFd), 0);
    ASSERT_EQ(FileSeek(testFd, 0, FILE_SEEK_CUR, &fileCurPos), 0);
    ASSERT_EQ(fileCurPos, 0);
    ASSERT_EQ(FileSeek(testFd, 0, FILE_SEEK_END, &fileCurPos), 0);
    ASSERT_EQ(fileCurPos, SIZE_8K);
    ASSERT_EQ(Rewind(testFd), 0);
    ASSERT_EQ(FileSeek(testFd, 0, FILE_SEEK_CUR, &fileCurPos), 0);
    ASSERT_EQ(fileCurPos, 0);
    ASSERT_EQ(Rewind(GetInvalidFd()), VFS_ERROR_PARAMETERS_INVALID);
    CloseAndRemoveTestFile();
}

/**
 * Open a file, read and write file with invalid arguments, return VFS_ERROR_PARAMETERS_INVALID
 * Write file with valid arguments successfully, read file and get same data as write data
 */
TEST_F(VfsStaticLinuxTest, ReadWriteFile001)
{
    CreateTestFile();
    ASSERT_EQ(Extend(testFd, 2 * SIZE_8K), 0);
    int64_t writeSize = -1;
    ASSERT_EQ(WriteSync(nullptr, lowerCaseData, SIZE_8K, &writeSize), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(WriteSync(testFd, nullptr, SIZE_8K, &writeSize), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(WriteSync(testFd, lowerCaseData, SIZE_8K, nullptr), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(WriteSync(GetInvalidFd(), lowerCaseData, SIZE_8K, &writeSize), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(WriteSync(testFd, lowerCaseData, SIZE_8K, &writeSize), 0);
    ASSERT_EQ(writeSize, SIZE_8K);
    writeSize = -1;
    ASSERT_EQ(WriteSync(testFd, upperCaseData, SIZE_8K, &writeSize), 0);
    ASSERT_EQ(writeSize, SIZE_8K);
    ASSERT_EQ(Rewind(testFd), 0);
    char readData[SIZE_8K];
    int64_t readSize = -1;
    ASSERT_EQ(Read(nullptr, readData, SIZE_8K, &readSize), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(Read(testFd, nullptr, SIZE_8K, &readSize), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(Read(testFd, readData, SIZE_8K, nullptr), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(Read(GetInvalidFd(), readData, SIZE_8K, &readSize), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(Read(testFd, readData, SIZE_8K, &readSize), 0);
    ASSERT_EQ(readSize, SIZE_8K);
    ASSERT_EQ(strncmp(readData, lowerCaseData, SIZE_8K), 0);
    readSize = -1;
    ASSERT_EQ(Read(testFd, readData, SIZE_8K, &readSize), 0);
    ASSERT_EQ(readSize, SIZE_8K);
    ASSERT_EQ(strncmp(readData, upperCaseData, SIZE_8K), 0);
    readSize = -1;
    ASSERT_EQ(Read(testFd, readData, SIZE_8K, &readSize), 0);
    ASSERT_EQ(readSize, 0);
    CloseAndRemoveTestFile();
}

/**
 * Multiple threads write and read different files simultaneously, each thread get correct result
 */
TEST_F(VfsStaticLinuxTest, ReadWriteFile002)
{
    uint16_t fileIdList[MULTI_THREAD_TEST_COUNT] = {11111, 33333, 55555};
    std::string filePathList[MULTI_THREAD_TEST_COUNT];
    FileInfo fileInfo[MULTI_THREAD_TEST_COUNT];
    for (int i = 0; i < MULTI_THREAD_TEST_COUNT; ++i) {
        filePathList[i] = GenerateTestFilePath(utDataDir, fileIdList[i]);
        fileInfo[i] = {&filePathList[i]};
    }
    DataInfo dataInfo[MULTI_THREAD_TEST_COUNT] = {{digitData, SIZE_8K}, {lowerCaseData, SIZE_8K},
                                                  {upperCaseData, SIZE_8K}};
    ReadWriteConcurrency(testVfs, {&filePathList[0]}, dataInfo[0], testFilePara);

    std::thread threads[MULTI_THREAD_TEST_COUNT];
    for (int i = 0; i < MULTI_THREAD_TEST_COUNT; ++i) {
        threads[i] = std::thread(ReadWriteConcurrency, testVfs, fileInfo[i], dataInfo[i], testFilePara);
    }
    for (auto &thread : threads) {
        thread.join();
    }
}

/**
 * Open file in write only mode, can only write data but not read data
 * Open file in read only mode, can only read data but not write data
 */
TEST_F(VfsStaticLinuxTest, ReadWriteFile003)
{
    CreateTestFile();
    ASSERT_EQ(Extend(testFd, SIZE_8K), 0);
    ASSERT_EQ(Close(testFd), 0);
    /* Open file in write only mode */
    ASSERT_EQ(Open(testVfs, GetTestFilePath(), FILE_WRITE_ONLY_FLAG, &testFd), 0);
    int64_t writeSize = -1;
    ASSERT_EQ(WriteSync(testFd, upperCaseData, SIZE_8K, &writeSize), 0);
    ASSERT_EQ(Rewind(testFd), 0);
    char readData[SIZE_8K];
    int64_t readSize = -1;
    ASSERT_NE(Read(testFd, readData, SIZE_8K, &readSize), 0);
    ASSERT_EQ(Close(testFd), 0);
    /* Open file in read only mode */
    ASSERT_EQ(Open(testVfs, GetTestFilePath(), FILE_READ_ONLY_FLAG, &testFd), 0);
    ASSERT_EQ(Read(testFd, readData, SIZE_8K, &readSize), 0);
    ASSERT_EQ(readSize, SIZE_8K);
    ASSERT_EQ(strncmp(readData, upperCaseData, SIZE_8K), 0);
    ASSERT_EQ(Rewind(testFd), 0);
    ASSERT_NE(WriteSync(testFd, lowerCaseData, SIZE_8K, &writeSize), 0);
    ASSERT_EQ(Close(testFd), 0);
    ASSERT_EQ(Remove(testVfs, GetTestFilePath()), 0);
}

/**
 * Write data async multiple times, after fsync, read correct data from file
 */
TEST_F(VfsStaticLinuxTest, WriteAsyncFile001)
{
    CreateTestFile();
    ASSERT_EQ(Extend(testFd, 2 * SIZE_8K), 0);
    ASSERT_EQ(WriteAsync(nullptr, lowerCaseData, SIZE_8K, nullptr), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(WriteAsync(testFd, nullptr, SIZE_8K, nullptr), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(WriteAsync(GetInvalidFd(), lowerCaseData, SIZE_8K, nullptr), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(WriteAsync(testFd, lowerCaseData, SIZE_8K, nullptr), 0);
    ASSERT_EQ(WriteAsync(testFd, upperCaseData, SIZE_8K, nullptr), 0);
    ASSERT_EQ(Fsync(testFd), 0);
    char readData[SIZE_8K];
    int64_t readSize = -1;
    ASSERT_EQ(Pread(testFd, readData, SIZE_8K, 0, &readSize), 0);
    ASSERT_EQ(readSize, SIZE_8K);
    ASSERT_EQ(strncmp(readData, lowerCaseData, SIZE_8K), 0);
    readSize = -1;
    ASSERT_EQ(Pread(testFd, readData, SIZE_8K, SIZE_8K, &readSize), 0);
    ASSERT_EQ(readSize, SIZE_8K);
    ASSERT_EQ(strncmp(readData, upperCaseData, SIZE_8K), 0);
    CloseAndRemoveTestFile();
}

/**
 * Pread and pwrite file with invalid arguments, return VFS_ERROR_PARAMETERS_INVALID
 * Pwrite file with valid arguments success, and pread correct data from file
 */
TEST_F(VfsStaticLinuxTest, PreadPwriteFile001)
{
    CreateTestFile();
    ASSERT_EQ(Extend(testFd, 2 * SIZE_8K), 0);
    int64_t writeSize = -1;
    ASSERT_EQ(PwriteSync(nullptr, digitData, SIZE_8K, 0, &writeSize), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(PwriteSync(testFd, nullptr, SIZE_8K, 0, &writeSize), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(PwriteSync(testFd, digitData, SIZE_8K, 0, nullptr), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(PwriteSync(GetInvalidFd(), digitData, SIZE_8K, 0, &writeSize), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(PwriteSync(testFd, digitData, SIZE_8K, 0, &writeSize), 0);
    ASSERT_EQ(writeSize, SIZE_8K);
    writeSize = -1;
    ASSERT_EQ(PwriteSync(testFd, lowerCaseData, SIZE_8K, SIZE_8K, &writeSize), 0);
    ASSERT_EQ(writeSize, SIZE_8K);
    char readData[SIZE_8K];
    int64_t readSize = -1;
    ASSERT_EQ(Pread(nullptr, readData, SIZE_8K, 0, &readSize), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(Pread(testFd, nullptr, SIZE_8K, 0, &readSize), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(Pread(testFd, readData, SIZE_8K, 0, nullptr), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(Pread(GetInvalidFd(), readData, SIZE_8K, 0, &readSize), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(Pread(testFd, readData, SIZE_8K, 0, &readSize), 0);
    ASSERT_EQ(readSize, SIZE_8K);
    ASSERT_EQ(strncmp(readData, digitData, SIZE_8K), 0);
    readSize = -1;
    ASSERT_EQ(Pread(testFd, readData, SIZE_8K, SIZE_8K, &readSize), 0);
    ASSERT_EQ(readSize, SIZE_8K);
    ASSERT_EQ(strncmp(readData, lowerCaseData, SIZE_8K), 0);
    writeSize = -1;
    ASSERT_EQ(PwriteSync(testFd, upperCaseData, SIZE_8K, SIZE_8K, &writeSize), 0);
    ASSERT_EQ(writeSize, SIZE_8K);
    readSize = -1;
    ASSERT_EQ(Pread(testFd, readData, SIZE_8K, SIZE_8K, &readSize), 0);
    ASSERT_EQ(readSize, SIZE_8K);
    ASSERT_EQ(strncmp(readData, upperCaseData, SIZE_8K), 0);
    CloseAndRemoveTestFile();
}

/**
 * Multiple threads pwrite and pread different files simultaneously, each thread get correct result
 */
TEST_F(VfsStaticLinuxTest, PreadPwriteFile002)
{
    uint16_t fileIdList[MULTI_THREAD_TEST_COUNT] = {11111, 33333, 55555};
    std::string filePathList[MULTI_THREAD_TEST_COUNT];
    FileInfo fileInfo[MULTI_THREAD_TEST_COUNT];
    for (int i = 0; i < MULTI_THREAD_TEST_COUNT; ++i) {
        filePathList[i] = GenerateTestFilePath(utDataDir, fileIdList[i]);
        fileInfo[i] = {&filePathList[i]};
    }
    DataInfo dataInfo[MULTI_THREAD_TEST_COUNT] = {{digitData, SIZE_8K}, {lowerCaseData, SIZE_8K},
                                                  {upperCaseData, SIZE_8K}};
    PreadPwriteConcurrency(testVfs, {&filePathList[0]}, dataInfo[0], testFilePara);

    std::thread threads[MULTI_THREAD_TEST_COUNT];
    for (int i = 0; i < MULTI_THREAD_TEST_COUNT; ++i) {
        threads[i] = std::thread(PreadPwriteConcurrency, testVfs, fileInfo[i], dataInfo[i], testFilePara);
    }
    for (auto &thread : threads) {
        thread.join();
    }
}

/**
 * Multiple threads pwrite different data to single file in different block, main thread pread all data correct
 */
TEST_F(VfsStaticLinuxTest, PreadPwriteFile003)
{
    CreateTestFile();
    FileInfo fileInfo = {&defaultTestFilePath};
    DataInfo dataInfo[MULTI_THREAD_TEST_COUNT] = {{digitData, SIZE_8K}, {lowerCaseData, SIZE_8K},
                                                  {upperCaseData, SIZE_8K}};
    uint32_t writeTime = 10;
    uint32_t blockArrays[MULTI_THREAD_TEST_COUNT][writeTime];
    for (uint32_t i = 0; i < MULTI_THREAD_TEST_COUNT; ++i) {
        for (uint32_t j = 0; j < writeTime; ++j) {
            blockArrays[i][j] = i * writeTime + j;
        }
    }
    ASSERT_EQ(Extend(testFd, (int64_t)writeTime * MULTI_THREAD_TEST_COUNT * SIZE_8K), 0);

    std::thread threads[MULTI_THREAD_TEST_COUNT];
    for (int i = 0; i < MULTI_THREAD_TEST_COUNT; ++i) {
        BlockList blockList = {blockArrays[i], writeTime, SIZE_8K};
        threads[i] = std::thread(PreadPwriteOneFile, testVfs, fileInfo, dataInfo[i], blockList);
    }
    for (auto &thread : threads) {
        thread.join();
    }
    char readData[SIZE_8K];
    int64_t readSize;
    for (uint32_t i = 0; i < MULTI_THREAD_TEST_COUNT; ++i) {
        for (uint32_t j = 0; j < writeTime; ++j) {
            uint32_t blockNum = blockArrays[i][j];
            int64_t offset = (int64_t)blockNum * SIZE_8K;
            readSize = -1;
            ASSERT_EQ(memset_s(readData, SIZE_8K, 0, SIZE_8K), 0);
            ASSERT_EQ(Pread(testFd, readData, SIZE_8K, offset, &readSize), 0);
            ASSERT_EQ(readSize, SIZE_8K);
            ASSERT_EQ(strncmp(readData, dataInfo[i].data, SIZE_8K), 0);
        }
    }
    CloseAndRemoveTestFile();
}

/**
 * Open 10000 files, pwrite and pread different fds sequentially and all operations success
 */
TEST_F(VfsStaticLinuxTest, PreadPwriteFile004)
{
    int testMaxFile = MAX_OPERATION_FILE_ID;
    FileDescriptor *fdList[testMaxFile];
    for (int fileId = 0; fileId < testMaxFile; ++fileId) {
        std::string filePath = GenerateTestFilePath(utDataDir, fileId);
        ASSERT_EQ(Create(testVfs, filePath.c_str(), testFilePara, &(fdList[fileId])), 0);
        ASSERT_EQ(Extend(fdList[fileId], SIZE_8K), 0);
    }
    const char *writeData[] = {digitData, lowerCaseData, upperCaseData};
    int dataSize = sizeof(writeData) / sizeof(const char *);
    int64_t writeSize;
    for (int fileId = 0; fileId < testMaxFile; ++fileId) {
        writeSize = 0;
        int dataIndex = fileId % dataSize;
        ASSERT_EQ(PwriteSync(fdList[fileId], writeData[dataIndex], SIZE_8K, 0, &writeSize), 0);
        ASSERT_EQ(writeSize, SIZE_8K);
    }
    char readData[SIZE_8K];
    int64_t readSize;
    for (int fileId = 0; fileId < testMaxFile; ++fileId) {
        readSize = 0;
        int dataIndex = fileId % dataSize;
        ASSERT_EQ(Pread(fdList[fileId], readData, SIZE_8K, 0, &readSize), 0);
        ASSERT_EQ(readSize, SIZE_8K);
        ASSERT_EQ(strncmp(readData, writeData[dataIndex], SIZE_8K), 0);
    }
    for (int fileId = 0; fileId < testMaxFile; ++fileId) {
        ASSERT_EQ(Close(fdList[fileId]), 0);
        std::string filePath = GenerateTestFilePath(utDataDir, fileId);
        ASSERT_EQ(Remove(testVfs, filePath.c_str()), 0);
    }
}

/**
 * Pwrite data async multiple times, after fsync, read correct data from file
 */
TEST_F(VfsStaticLinuxTest, PwriteAsyncFile001)
{
    CreateTestFile();
    ASSERT_EQ(Extend(testFd, 2 * SIZE_8K), 0);
    ASSERT_EQ(EnableAIO(testVfs, 256, 1, NULL, NULL), 0);
    ASSERT_EQ(PwriteAsync(nullptr, digitData, SIZE_8K, SIZE_8K, nullptr), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(PwriteAsync(testFd, nullptr, SIZE_8K, SIZE_8K, nullptr), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(PwriteAsync(GetInvalidFd(), digitData, SIZE_8K, SIZE_8K, nullptr), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(PwriteAsync(testFd, digitData, SIZE_8K, SIZE_8K, nullptr), 0);
    ASSERT_EQ(PwriteAsync(testFd, lowerCaseData, SIZE_8K, 0, nullptr), 0);
    ASSERT_EQ(Fsync(testFd), 0);
    char readData[SIZE_8K];
    int64_t readSize = -1;
    ASSERT_EQ(Pread(testFd, readData, SIZE_8K, 0, &readSize), 0);
    ASSERT_EQ(readSize, SIZE_8K);
    ASSERT_EQ(strncmp(readData, lowerCaseData, SIZE_8K), 0);
    readSize = -1;
    ASSERT_EQ(Pread(testFd, readData, SIZE_8K, SIZE_8K, &readSize), 0);
    ASSERT_EQ(readSize, SIZE_8K);
    ASSERT_EQ(strncmp(readData, digitData, SIZE_8K), 0);
    ASSERT_EQ(PwriteAsync(testFd, upperCaseData, SIZE_8K, 0, nullptr), 0);
    ASSERT_EQ(FDataSync(testFd), 0);
    readSize = -1;
    ASSERT_EQ(Pread(testFd, readData, SIZE_8K, 0, &readSize), 0);
    ASSERT_EQ(readSize, SIZE_8K);
    ASSERT_EQ(strncmp(readData, upperCaseData, SIZE_8K), 0);
    CloseAndRemoveTestFile();
}

/**
 * Extend or Truncate file with invalid arguments, return VFS_ERROR_PARAMETERS_INVALID
 * Extend a file but targetSize < currentFileSize, return VFS_ERROR_PARAMETERS_INVALID
 * Truncate a file but targetSize > currentFileSize, return VFS_ERROR_PARAMETERS_INVALID
 */
TEST_F(VfsStaticLinuxTest, ExtendTruncateFile001)
{
    CreateTestFile();
    ASSERT_EQ(Extend(nullptr, 0), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(Truncate(nullptr, 0), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(Extend(testFd, -1), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(Truncate(testFd, -1), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(Extend(GetInvalidFd(), 0), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(Truncate(GetInvalidFd(), 0), VFS_ERROR_PARAMETERS_INVALID);
    int64_t targetSize = (int64_t)128 * MB_SIZE;
    ASSERT_EQ(Extend(testFd, targetSize), 0);
    ASSERT_EQ(Extend(testFd, targetSize), 0);
    ASSERT_EQ(Truncate(testFd, targetSize), 0);
    targetSize = (int64_t)512 * MB_SIZE;
    ASSERT_EQ(Truncate(testFd, targetSize), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(Extend(testFd, targetSize), 0);
    targetSize = (int64_t)256 * MB_SIZE;
    ASSERT_EQ(Extend(testFd, targetSize), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(Truncate(testFd, targetSize), 0);
    CloseAndRemoveTestFile();
}

/**
 * GetSize with invalid arguments, return VFS_ERROR_PARAMETERS_INVALID
 * Get current file size after several Extend and Truncate calls
 */
TEST_F(VfsStaticLinuxTest, GetSizeTest001)
{
    CreateTestFile();
    int64_t returnSize = -1;
    ASSERT_EQ(GetSize(nullptr, &returnSize), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(GetSize(testFd, nullptr), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(GetSize(GetInvalidFd(), &returnSize), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(GetSize(testFd, &returnSize), 0);
    ASSERT_EQ(returnSize, 0);
    int64_t targetSize = (int64_t)128 * MB_SIZE;
    ASSERT_EQ(Extend(testFd, targetSize), 0);
    ASSERT_EQ(GetSize(testFd, &returnSize), 0);
    ASSERT_EQ(returnSize, targetSize);
    targetSize = (int64_t)256 * MB_SIZE;
    ASSERT_EQ(Extend(testFd, targetSize), 0);
    ASSERT_EQ(GetSize(testFd, &returnSize), 0);
    ASSERT_EQ(returnSize, targetSize);
    targetSize = (int64_t)32 * MB_SIZE;
    ASSERT_EQ(Truncate(testFd, targetSize), 0);
    ASSERT_EQ(GetSize(testFd, &returnSize), 0);
    ASSERT_EQ(returnSize, targetSize);
    ASSERT_EQ(Truncate(testFd, 0), 0);
    ASSERT_EQ(GetSize(testFd, &returnSize), 0);
    ASSERT_EQ(returnSize, 0);
    CloseAndRemoveTestFile();
}

/**
 * Set flush callback function using FileControl interface
 * After Fsync call, get correct context and read correct data from file
 */
TEST_F(VfsStaticLinuxTest, FileControlFlushCallback001)
{
    CreateTestFile();
    ASSERT_EQ(Extend(testFd, 2 * SIZE_8K), 0);
    UtFlushContext flushContext;
    FileControlInfo controlInfo;
    controlInfo.flushCallbackInfo.callback = UtFlushCallback;
    controlInfo.flushCallbackInfo.asyncContext = &flushContext;
    int invalidCmd = -1;
    ASSERT_EQ(FileControl(nullptr, SET_FILE_FLUSH_CALLBACK, &controlInfo), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(FileControl(testFd, SET_FILE_FLUSH_CALLBACK, nullptr), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(FileControl(testFd, invalidCmd, &controlInfo), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(FileControl(GetInvalidFd(), SET_FILE_FLUSH_CALLBACK, &controlInfo), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(FileControl(testFd, SET_FILE_FLUSH_CALLBACK, &controlInfo), 0);
    ASSERT_EQ(EnableAIO(testVfs, 256, 1, NULL, NULL), 0);
    ASSERT_EQ(PwriteAsync(testFd, lowerCaseData, SIZE_8K, 0, nullptr), 0);
    ASSERT_EQ(Fsync(testFd), 0);
    ASSERT_EQ(flushContext.errCode, 0);
    ASSERT_EQ(flushContext.flushOffset, SIZE_8K);
    char readData[SIZE_8K];
    int64_t readSize = -1;
    ASSERT_EQ(Pread(testFd, readData, SIZE_8K, 0, &readSize), 0);
    ASSERT_EQ(readSize, SIZE_8K);
    ASSERT_EQ(strncmp(readData, lowerCaseData, SIZE_8K), 0);
    ASSERT_EQ(PwriteAsync(testFd, upperCaseData, SIZE_8K, SIZE_8K, nullptr), 0);
    ASSERT_EQ(Fsync(testFd), 0);
    ASSERT_EQ(flushContext.errCode, 0);
    ASSERT_EQ(flushContext.flushOffset, 2 * SIZE_8K);
    readSize = -1;
    ASSERT_EQ(Pread(testFd, readData, SIZE_8K, SIZE_8K, &readSize), 0);
    ASSERT_EQ(readSize, SIZE_8K);
    ASSERT_EQ(strncmp(readData, upperCaseData, SIZE_8K), 0);
    CloseAndRemoveTestFile();
}

/**
 * Flush callback will return max write offset in this fd
 * Reset max write offset if Rewind or Truncate call
 * Rewind will reset to 0, Truncate will reset to min(fileSize, maxWriteOffset)
 */
TEST_F(VfsStaticLinuxTest, FileControlFlushCallback002)
{
    CreateTestFile();
    ASSERT_EQ(Extend(testFd, 2 * SIZE_8K), 0);
    UtFlushContext flushContext;
    FileControlInfo controlInfo;
    controlInfo.flushCallbackInfo.callback = UtFlushCallback;
    controlInfo.flushCallbackInfo.asyncContext = &flushContext;
    ASSERT_EQ(FileControl(testFd, SET_FILE_FLUSH_CALLBACK, &controlInfo), 0);
    ASSERT_EQ(EnableAIO(testVfs, 256, 1, NULL, NULL), 0);
    ASSERT_EQ(WriteAsync(testFd, lowerCaseData, SIZE_8K, nullptr), 0);
    ASSERT_EQ(Fsync(testFd), 0);
    ASSERT_EQ(flushContext.errCode, 0);
    ASSERT_EQ(flushContext.flushOffset, SIZE_8K);
    char readData[SIZE_8K];
    int64_t readSize = -1;
    ASSERT_EQ(Pread(testFd, readData, SIZE_8K, 0, &readSize), 0);
    ASSERT_EQ(readSize, SIZE_8K);
    ASSERT_EQ(strncmp(readData, lowerCaseData, SIZE_8K), 0);
    ASSERT_EQ(WriteAsync(testFd, upperCaseData, SIZE_8K, nullptr), 0);
    ASSERT_EQ(Fsync(testFd), 0);
    ASSERT_EQ(flushContext.errCode, 0);
    ASSERT_EQ(flushContext.flushOffset, 2 * SIZE_8K);
    readSize = -1;
    ASSERT_EQ(Pread(testFd, readData, SIZE_8K, SIZE_8K, &readSize), 0);
    ASSERT_EQ(readSize, SIZE_8K);
    ASSERT_EQ(strncmp(readData, upperCaseData, SIZE_8K), 0);
    ASSERT_EQ(Rewind(testFd), 0);
    ASSERT_EQ(WriteAsync(testFd, digitData, SIZE_8K, nullptr), 0);
    ASSERT_EQ(Fsync(testFd), 0);
    ASSERT_EQ(flushContext.errCode, 0);
    ASSERT_EQ(flushContext.flushOffset, SIZE_8K);
    readSize = -1;
    ASSERT_EQ(Pread(testFd, readData, SIZE_8K, 0, &readSize), 0);
    ASSERT_EQ(readSize, SIZE_8K);
    ASSERT_EQ(strncmp(readData, digitData, SIZE_8K), 0);
    ASSERT_EQ(Truncate(testFd, 0), 0);
    ASSERT_EQ(PwriteAsync(testFd, lowerCaseData, SIZE_8K, 0, nullptr), 0);
    ASSERT_EQ(FDataSync(testFd), 0);
    ASSERT_EQ(flushContext.errCode, 0);
    ASSERT_EQ(flushContext.flushOffset, SIZE_8K);
    readSize = -1;
    ASSERT_EQ(Pread(testFd, readData, SIZE_8K, 0, &readSize), 0);
    ASSERT_EQ(readSize, SIZE_8K);
    ASSERT_EQ(strncmp(readData, lowerCaseData, SIZE_8K), 0);
    CloseAndRemoveTestFile();
}

/**
 * Using FileControl interface to set zero copy memory key
 */
TEST_F(VfsStaticLinuxTest, FileControlMemKey001)
{
    CreateTestFile();
    FileControlInfo controlInfo;
    controlInfo.zeroCopyMemKey.memKey = 0;
    ASSERT_EQ(FileControl(testFd, SET_FILE_ZCOPY_MEMORY_KEY, &controlInfo), 0);
    CloseAndRemoveTestFile();
}

/**
 * Lock file with invalid arguments, return VFS_ERROR_PARAMETERS_INVALID
 * Main process lock file in section [0, a), another process cannot lock same file in section [0, a)
 * Main process unlock file in section [0, a), another process can lock same file in section [0, a)
 */
TEST_F(VfsStaticLinuxTest, LockFileTest001)
{
    CreateTestFile();
    int64_t testFileSize = 100;
    ASSERT_EQ(Extend(testFd, testFileSize), 0);
    int invalidLockMode = -1;
    ASSERT_EQ(LockFile(nullptr, 0, testFileSize, FILE_EXCLUSIVE_LOCK, 0), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(LockFile(testFd, 0, testFileSize, invalidLockMode, 0), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(LockFile(testFd, -1, testFileSize, FILE_EXCLUSIVE_LOCK, 0), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(LockFile(GetInvalidFd(), 0, testFileSize, FILE_EXCLUSIVE_LOCK, 0), VFS_ERROR_PARAMETERS_INVALID);
    /* Lock section [0,testFileSize) */
    ASSERT_EQ(LockFile(testFd, 0, testFileSize, FILE_EXCLUSIVE_LOCK, 0), 0);
    FileInfo fileInfo;
    LockInfo lockInfo;
    bool result;

    pid_t testPid;
    int status;
    testPid = fork();
    if (testPid < 0) {
        ASSERT_TRUE(false);
    } else if (testPid == 0) {
        /* Another process cannot lock section [0,testFileSize) */
        fileInfo = {&defaultTestFilePath};
        lockInfo = {0, testFileSize};
        result = true;
        GetLockFileResult(testVfs, fileInfo, lockInfo, &result);
        ASSERT_FALSE(result);
        exit(0);
    }
    if (wait(&status) != testPid) {
        ASSERT_TRUE(false);
    }
    ASSERT_TRUE(WIFEXITED(status));
    ASSERT_EQ(UnlockFile(nullptr, 0, testFileSize), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(UnlockFile(testFd, -1, testFileSize), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(UnlockFile(GetInvalidFd(), 0, testFileSize), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(UnlockFile(testFd, 0, testFileSize), 0);

    testPid = fork();
    if (testPid < 0) {
        ASSERT_TRUE(false);
    } else if (testPid == 0) {
        /* Main process unlock section [0,testFileSize), another process can lock section [0,testFileSize) */
        fileInfo = {&defaultTestFilePath};
        lockInfo = {0, testFileSize};
        result = false;
        GetLockFileResult(testVfs, fileInfo, lockInfo, &result);
        ASSERT_TRUE(result);
        exit(0);
    }
    if (wait(&status) != testPid) {
        ASSERT_TRUE(false);
    }
    ASSERT_TRUE(WIFEXITED(status));

    CloseAndRemoveTestFile();
}

/**
 * Main process lock file in section [0, a/2), another process can lock section [a/2, a) but not [0, a/2)
 */
TEST_F(VfsStaticLinuxTest, LockFileTest002)
{
    CreateTestFile();
    int64_t testFileSize = 100;
    ASSERT_EQ(Extend(testFd, testFileSize), 0);
    ASSERT_EQ(TryLockFile(nullptr, 0, testFileSize / 2, FILE_EXCLUSIVE_LOCK), VFS_ERROR_INVALID_ARGUMENT);
    ASSERT_EQ(TryLockFile(testFd, -1, testFileSize / 2, FILE_EXCLUSIVE_LOCK), VFS_ERROR_INVALID_ARGUMENT);
    ASSERT_EQ(TryLockFile(testFd, 0, testFileSize / 2, -1), VFS_ERROR_INVALID_ARGUMENT);
    ASSERT_EQ(TryLockFile(GetInvalidFd(), 0, testFileSize / 2, FILE_EXCLUSIVE_LOCK), VFS_ERROR_INVALID_ARGUMENT);
    /* Lock section [0,testFileSize/2) */
    ASSERT_EQ(TryLockFile(testFd, 0, testFileSize / 2, FILE_EXCLUSIVE_LOCK), 0);
    FileInfo fileInfo;
    LockInfo lockInfo;

    pid_t testPid;
    int status;
    testPid = fork();
    if (testPid < 0) {
        ASSERT_TRUE(false);
    } else if (testPid == 0) {
        /* Another process cannot lock section [0,testFileSize) */
        fileInfo = {&defaultTestFilePath};
        lockInfo = {0, testFileSize};
        bool result = true;
        GetLockFileResult(testVfs, fileInfo, lockInfo, &result);
        ASSERT_FALSE(result);
        /* Another process can lock section [testFileSize/2,testFileSize) */
        lockInfo = {testFileSize / 2, testFileSize / 2};
        GetLockFileResult(testVfs, fileInfo, lockInfo, &result);
        ASSERT_TRUE(result);
        exit(0);
    }
    if (wait(&status) != testPid) {
        ASSERT_TRUE(false);
    }
    ASSERT_TRUE(WIFEXITED(status));
    ASSERT_EQ(UnlockFile(testFd, 0, testFileSize / 2), 0);
    CloseAndRemoveTestFile();
}

/**
 * Open 10000 different files, lock file0 in section [0, a), and get file0's fd is curStorageFd
 * Sequentially write data to 10000 files in section [0, a), file0 cannot be temporarily closed
 * Get file0's fd equals to curStorageFd after 10000 writes to 10000 files
 * Another process cannot lock file0 in section [0, a) because main process hold file0 lock in section [0, a)
 */
TEST_F(VfsStaticLinuxTest, LockFileTest003)
{
    int testMaxFile = MAX_OPERATION_FILE_ID;
    FileDescriptor *fdList[testMaxFile];
    for (int fileId = 0; fileId < testMaxFile; ++fileId) {
        std::string filePath = GenerateTestFilePath(utDataDir, fileId);
        ASSERT_EQ(Create(testVfs, filePath.c_str(), testFilePara, &(fdList[fileId])), 0);
        ASSERT_EQ(Extend(fdList[fileId], SIZE_8K), 0);
    }
    ASSERT_EQ(LockFile(fdList[0], 0, SIZE_8K, FILE_EXCLUSIVE_LOCK, 0), 0);
    LinuxFileHandle *internalFd = (LinuxFileHandle *)fdList[0]->fileHandle;
    int curStorageFd = internalFd->storageFd;
    ASSERT_GT(internalFd->lockCount, 0);
    int64_t writeSize;
    for (int fileId = 0; fileId < testMaxFile; ++fileId) {
        writeSize = 0;
        ASSERT_EQ(PwriteSync(fdList[fileId], digitData, SIZE_8K, 0, &writeSize), 0);
        ASSERT_EQ(writeSize, SIZE_8K);
    }
    internalFd = (LinuxFileHandle *)fdList[0]->fileHandle;
    ASSERT_EQ(internalFd->storageFd, curStorageFd); /* Fd has file lock cannot close temporarily */

    FileInfo fileInfo;
    LockInfo lockInfo;
    pid_t testPid;
    int status;
    testPid = fork();
    if (testPid < 0) {
        ASSERT_TRUE(false);
    } else if (testPid == 0) {
        /* Another process cannot lock section [0, SIZE_8K) */
        std::string filePath = GenerateTestFilePath(utDataDir, 0);
        fileInfo = {&filePath};
        lockInfo = {0, SIZE_8K};
        bool result = true;
        GetLockFileResult(testVfs, fileInfo, lockInfo, &result);
        ASSERT_FALSE(result);
        exit(0);
    }
    if (wait(&status) != testPid) {
        ASSERT_TRUE(false);
    }

    for (int fileId = 0; fileId < testMaxFile; ++fileId) {
        ASSERT_EQ(Close(fdList[fileId]), 0);
        std::string filePath = GenerateTestFilePath(utDataDir, fileId);
        ASSERT_EQ(Remove(testVfs, filePath.c_str()), 0);
    }
}

TEST_F(VfsStaticLinuxTest, PreadAsyncPwriteFile)
{
    CreateTestFile();
    ASSERT_EQ(Extend(testFd, 2 * SIZE_8K), 0);
    int64_t writeSize = -1;
    ASSERT_EQ(PwriteSync(testFd, digitData, SIZE_8K, 0, &writeSize), 0);
    ASSERT_EQ(writeSize, SIZE_8K);
    char readData[SIZE_8K];

    ASSERT_EQ(PreadAsync(testFd, readData, SIZE_8K, 0, nullptr), 0);
    CloseAndRemoveTestFile();
}

char *prepareWriteContent(void)
{
    int64_t pagesize = getpagesize();
    int64_t Size = pagesize * 10;
    char *buffer;
    posix_memalign((void**)&buffer, pagesize, Size);
    memset_s(buffer, Size, 0, Size);
#define TEST_CONTENT "This is a text.\n"
    strcpy_s(buffer, Size, TEST_CONTENT);
    return buffer;
}

char *prepareReadBuffer(void)
{
    int64_t pagesize = getpagesize();
    int64_t Size = pagesize * 20;
    char *readData;
    posix_memalign((void**)&readData, pagesize, Size);
    return readData;
}

TEST_F(VfsStaticLinuxTest, DirectIOOpenTest)
{
    /* Open an non-exist file */
    ASSERT_EQ(Open(testVfs, GetTestFilePath(),
                   FILE_READ_AND_WRITE_FLAG | FILE_DIRECT_IO_FLAG, &testFd),
              VFS_ERROR_OPEN_FILE_NOT_EXIST);
}

TEST_F(VfsStaticLinuxTest, DirectIOWriteTest)
{
    char *buffer = prepareWriteContent();

    /* Create file */
    ASSERT_EQ(Create(testVfs, GetTestFilePath(), testFilePara, &testFd), 0);
    ASSERT_EQ(Close(testFd), 0);

    /* DirectIO */
    ASSERT_EQ(Open(testVfs, GetTestFilePath(),
                   FILE_READ_AND_WRITE_FLAG | FILE_DIRECT_IO_FLAG, &testFd),
              0);

    /* Write file */
    int64_t writeSize = -1;
    int64_t Size = getpagesize() * 10;
    ASSERT_EQ(WriteSync(testFd, buffer, Size, &writeSize), 0);
    ASSERT_EQ(writeSize, Size);

    /* Read file */
    char *readData = prepareReadBuffer();
    int64_t readSize = -1;
    ASSERT_EQ(Pread(testFd, readData, Size, 0, &readSize), 0);
    ASSERT_EQ(readSize, Size);
    ASSERT_EQ(strcmp(readData, TEST_CONTENT), 0);
    ASSERT_EQ(Close(testFd), 0);

    /* Release resource */
    free(buffer);
    free(readData);
}

TEST_F(VfsStaticLinuxTest, DirectIOSynchronizedWriteTest)
{
    char *buffer = prepareWriteContent();

    /* Create file */
    ASSERT_EQ(Create(testVfs, GetTestFilePath(), testFilePara, &testFd), 0);
    ASSERT_EQ(Close(testFd), 0);

    /* DirectIO */
    ASSERT_EQ(Open(testVfs, GetTestFilePath(),
                   FILE_READ_AND_WRITE_FLAG | FILE_DIRECT_IO_FLAG | FILE_SYNC_FLAG, &testFd),
              0);
    /* Write file */
    int64_t writeSize = -1;
    int64_t Size = getpagesize() * 10;
    ASSERT_EQ(WriteSync(testFd, buffer, Size, &writeSize), 0);
    ASSERT_EQ(writeSize, Size);

    /* Read file */
    char *readData = prepareReadBuffer();
    int64_t readSize = -1;
    ASSERT_EQ(Pread(testFd, readData, Size, 0, &readSize), 0);
    ASSERT_EQ(readSize, Size);
    ASSERT_EQ(strcmp(readData, TEST_CONTENT), 0);
    ASSERT_EQ(Close(testFd), 0);

    /* Release resource */
    free(buffer);
    free(readData);
}

TEST_F(VfsStaticLinuxTest, DirectIOAppendTest)
{
    char *buffer = prepareWriteContent();

    /* Create file */
    ASSERT_EQ(Create(testVfs, GetTestFilePath(), testFilePara, &testFd), 0);

    /* Write file first time */
    int64_t writeSize = -1;
    int64_t Size = getpagesize() * 10;
    ASSERT_EQ(WriteSync(testFd, buffer, Size, &writeSize), 0);
    ASSERT_EQ(writeSize, Size);
    ASSERT_EQ(Close(testFd), 0);

    /* Open a file in append mode */
    ASSERT_EQ(Open(testVfs, GetTestFilePath(),
                   FILE_READ_AND_WRITE_FLAG | FILE_APPEND_FLAG, &testFd),
              0);

    /* Write file second time */
    ASSERT_EQ(WriteSync(testFd, buffer, Size, &writeSize), 0);
    ASSERT_EQ(writeSize, Size);

    /* Read file */
    char *readData = prepareReadBuffer();
    int64_t readSize = -1;
    ASSERT_EQ(Pread(testFd, readData, Size * 2, 0, &readSize), 0);
    ASSERT_EQ(readSize, Size * 2);
    ASSERT_EQ(strcmp(readData, TEST_CONTENT), 0);
    ASSERT_EQ(Close(testFd), 0);

    /* Release resource */
    free(buffer);
    free(readData);
}

TEST_F(VfsStaticLinuxTest, DirectIOTruncateTest)
{
    /* Create file */
    ASSERT_EQ(Create(testVfs, GetTestFilePath(), testFilePara, &testFd), 0);
    ASSERT_EQ(Close(testFd), 0);

    ASSERT_EQ(Open(testVfs, GetTestFilePath(),
                   FILE_READ_AND_WRITE_FLAG | FILE_TRUNC_FLAG, &testFd),
              0);

    int64_t returnSize = -1;
    ASSERT_EQ(GetSize(testFd, &returnSize), 0);
    ASSERT_EQ(returnSize, 0);

    ASSERT_EQ(Close(testFd), 0);
}

TEST_F(VfsStaticLinuxTest, DirectIOUnalignedTest)
{
    char *buffer;
    size_t alignment = 4096;
    int64_t Size = alignment - 1;
    posix_memalign((void **)&buffer, alignment, Size);
    memset_s(buffer, Size, 0, Size);
#define TEST_CONTENT "This is a text.\n"
    strcpy_s(buffer, Size, TEST_CONTENT);

    /* Create file */
    ASSERT_EQ(Create(testVfs, GetTestFilePath(), testFilePara, &testFd), 0);
    ASSERT_EQ(Close(testFd), 0);

    /* DirectIO */
    ASSERT_EQ(Open(testVfs, GetTestFilePath(),
                   FILE_READ_AND_WRITE_FLAG | FILE_DIRECT_IO_FLAG, &testFd),
              0);
    int64_t writeSize = -1;
    ASSERT_EQ(WriteSync(testFd, buffer, Size, &writeSize), VFS_ERROR_PARAMETERS_INVALID);
    ASSERT_EQ(writeSize, -1);
    ASSERT_EQ(Close(testFd), 0);

    /* Release resource */
    free(buffer);
}

#define SOURCE_FILE_NAME "./srcFile"
#define DESTINATION_FILE_NAME "./destFile"
TEST_F(VfsStaticLinuxTest, RenameFileTest)
{
    /* Create source file */
    ASSERT_EQ(Create(testVfs, SOURCE_FILE_NAME, testFilePara, &testFd), ERROR_SYS_OK);
    ASSERT_EQ(Close(testFd), ERROR_SYS_OK);

    /* Create destination file */
    ASSERT_EQ(Create(testVfs, DESTINATION_FILE_NAME, testFilePara, &testFd), ERROR_SYS_OK);
    ASSERT_EQ(Close(testFd), ERROR_SYS_OK);

    /* Rename source file */
    ASSERT_EQ(RenameFile(testVfs, SOURCE_FILE_NAME, DESTINATION_FILE_NAME), ERROR_SYS_OK);

    /* Check source file */
    bool isFileExist;
    ASSERT_EQ(FileIsExist(testVfs, SOURCE_FILE_NAME, &isFileExist), ERROR_SYS_OK);
    ASSERT_FALSE(isFileExist);

    /* Check destination file */
    ASSERT_EQ(FileIsExist(testVfs, DESTINATION_FILE_NAME, &isFileExist), ERROR_SYS_OK);
    ASSERT_TRUE(isFileExist);

    /* Clean resource */
    ASSERT_EQ(Remove(testVfs, SOURCE_FILE_NAME), VFS_ERROR_REMOVE_FILE_NOT_EXIST);
    ASSERT_EQ(Remove(testVfs, DESTINATION_FILE_NAME), ERROR_SYS_OK);
}

TEST_F(VfsStaticLinuxTest, RenameFileWhenNoSrcTest)
{
    /* Rename non-exist source file */
    ASSERT_EQ(RenameFile(testVfs, SOURCE_FILE_NAME, DESTINATION_FILE_NAME), VFS_ERROR_OPEN_FILE_NOT_EXIST);

    /* Clean resource */
    ASSERT_EQ(Remove(testVfs, SOURCE_FILE_NAME), VFS_ERROR_REMOVE_FILE_NOT_EXIST);
}

TEST_F(VfsStaticLinuxTest, RenameFileWhenNoDestTest)
{
    /* Create source file */
    ASSERT_EQ(Create(testVfs, SOURCE_FILE_NAME, testFilePara, &testFd), ERROR_SYS_OK);
    ASSERT_EQ(Close(testFd), ERROR_SYS_OK);

    /* Rename source file */
    ASSERT_EQ(RenameFile(testVfs, SOURCE_FILE_NAME, DESTINATION_FILE_NAME), ERROR_SYS_OK);

    /* Check source file */
    bool isFileExist;
    ASSERT_EQ(FileIsExist(testVfs, SOURCE_FILE_NAME, &isFileExist), ERROR_SYS_OK);
    ASSERT_FALSE(isFileExist);

    /* Check destination file */
    ASSERT_EQ(FileIsExist(testVfs, DESTINATION_FILE_NAME, &isFileExist), ERROR_SYS_OK);
    ASSERT_TRUE(isFileExist);

    /* Clean resource */
    ASSERT_EQ(Remove(testVfs, SOURCE_FILE_NAME), VFS_ERROR_REMOVE_FILE_NOT_EXIST);
    ASSERT_EQ(Remove(testVfs, DESTINATION_FILE_NAME), ERROR_SYS_OK);
}

static int GetAioFileLineNum(const char *filePath)
{
    std::ifstream fp;
    char curChar, prevChar = '\n';
    int lineNum = 0;
    fp.open(filePath, std::ios::in); /* do not ignore '\n' */
    fp.unsetf(std::ios::skipws);
    while(fp.peek() != EOF) /* read til the end of file */
    {
        fp>>curChar;
        if(curChar == '\n') {
            lineNum ++;
        }
        prevChar = curChar;
    }
    return lineNum;
}

Atomic32 callbackCount;
static void CallbackFunc1(ErrorCode errorCode, int64_t successSize, SYMBOL_UNUSED void *asyncContext)
{
    GSDB_ATOMIC32_INC(&callbackCount);
}

#define TEST_STRING "This is a text string.\n"
TEST_F(VfsStaticLinuxTest, PwriteAsyncFileAio)
{
    CreateTestFile();
    ASSERT_EQ(Close(testFd), 0);
    callbackCount = 0;

    /* Open in O_DIRECT flag */
    ASSERT_EQ(Open(testVfs, GetTestFilePath(),
                   FILE_READ_AND_WRITE_FLAG | FILE_ASYNC_IO_FLAG, &testFd), 0);

    /* Event number equal the max event number */
    ASSERT_EQ(EnableAIO(testVfs, MAX_AIO_EVENT_NUM, 1, NULL, NULL), 0);
    /* Thread number exceed the max thread number */
    ASSERT_EQ(EnableAIO(testVfs, 256, MAX_AIO_THREAD_NUM + 1, NULL, NULL), VFS_ERROR_INVALID_ARGUMENT);
    /* Create io context queue with 128 events, and create 4 threads to get events */
    ASSERT_EQ(EnableAIO(testVfs, 256, 4, NULL, NULL), 0);

    /* Prepare write buffer */
    char *buf;
    posix_memalign((void**)&buf, 512, 512);
    memset_s(buf, 512, 0, 512);
    strcpy_s(buf, 512, TEST_STRING);

    /* Write in async way */
    AsyncIoContext asyncContext1 = {CallbackFunc1, NULL};
    for(int i = 0; i < 128; i++) {
        ASSERT_EQ(PwriteAsync(testFd, buf, 512, i * 512, &asyncContext1), 0);
    }

    /* Prepare read buffer */
    char *readBuf;
    posix_memalign((void**)&readBuf, 512, 512);
    memset_s(readBuf, 512, 0, 512);
    strcpy_s(readBuf, 512, TEST_STRING);

    /* Read in async way */
    for(int i = 0; i < 128; i++) {
        ASSERT_EQ(PreadAsync(testFd, readBuf, 512, i * 512, &asyncContext1), 0);
    }
    ASSERT_EQ(Close(testFd), 0);
    /* The number of write string */
    ASSERT_EQ(GetAioFileLineNum(GetTestFilePath()), 128);
    /* The calling times of callback function, both write and read */
    ASSERT_EQ(callbackCount, 256);

    free(buf);
    free(readBuf);
}

TEST_F(VfsStaticLinuxTest, PwriteAsyncFileAioWhenNoAsyncContext)
{
    CreateTestFile();
    ASSERT_EQ(Close(testFd), 0);
    callbackCount = 0;

    /* Open in O_DIRECT flag */
    ASSERT_EQ(Open(testVfs, GetTestFilePath(),
                   FILE_READ_AND_WRITE_FLAG | FILE_ASYNC_IO_FLAG, &testFd), 0);

    /* Create io context queue with 128 events, and create 1 threads to get events */
    ASSERT_EQ(EnableAIO(testVfs, 256, 1, NULL, NULL), 0);

    /* Prepare write buffer */
    char *buf;
    posix_memalign((void**)&buf, 512, 512);
    memset_s(buf, 512, 0, 512);
    strcpy_s(buf, 512, TEST_STRING);

    /* Write without async context */
    for(int i = 0; i < 1; i++) {
        ASSERT_EQ(PwriteAsync(testFd, buf, 512, i * 512, NULL), 0);
    }

    /* Prepare read buffer */
    char *readBuf;
    posix_memalign((void**)&readBuf, 512, 512);
    memset_s(readBuf, 512, 0, 512);
    strcpy_s(readBuf, 512, TEST_STRING);

    /* Read in async way */
    for(int i = 0; i < 1; i++) {
        ASSERT_EQ(PreadAsync(testFd, readBuf, 512, i * 512, NULL), 0);
    }

    ASSERT_EQ(Close(testFd), 0);
    /* The number of write string */
    ASSERT_EQ(GetAioFileLineNum(GetTestFilePath()), 1);
    /* The calling times of callback function, both write and read */
    ASSERT_EQ(callbackCount, 0);

    free(buf);
    free(readBuf);
}

TEST_F(VfsStaticLinuxTest, PwriteAsyncFileAioWhenNoAlignedBuffer)
{
    CreateTestFile();
    ASSERT_EQ(Close(testFd), 0);
    callbackCount = 0;

    /* Open in O_DIRECT flag */
    ASSERT_EQ(Open(testVfs, GetTestFilePath(),
                   FILE_READ_AND_WRITE_FLAG | FILE_ASYNC_IO_FLAG, &testFd), 0);

    /* Create io context queue with 128 events, and create 4 threads to get events */
    ASSERT_EQ(EnableAIO(testVfs, 256, 1, NULL, NULL), 0);

    /* Write in async way */
    AsyncIoContext asyncContext1 = {CallbackFunc1, NULL};
    for(int i = 0; i < 1; i++) {
        ASSERT_EQ(PwriteAsync(testFd, TEST_STRING, sizeof(TEST_STRING), i * sizeof(TEST_STRING), &asyncContext1), 0);
    }

    ASSERT_EQ(Close(testFd), 0);
    /* The number of write string */
    ASSERT_EQ(GetAioFileLineNum(GetTestFilePath()), 0);
    /* The calling times of callback function, both write and read */
    ASSERT_EQ(callbackCount, 1);
}

TEST_F(VfsStaticLinuxTest, PwriteAsyncFileAioWhenNoUseAsyncFlag)
{
    CreateTestFile();
    ASSERT_EQ(Close(testFd), 0);
    callbackCount = 0;

    /* Open in O_DIRECT flag */
    ASSERT_EQ(Open(testVfs, GetTestFilePath(),
                   FILE_READ_AND_WRITE_FLAG, &testFd), 0);

    /* Create io context queue with 128 events, and create 4 threads to get events */
    ASSERT_EQ(EnableAIO(testVfs, 256, 1, NULL, NULL), 0);

    /* Prepare write buffer */
    char *buf;
    posix_memalign((void**)&buf, 512, 512);
    memset_s(buf, 512, 0, 512);
    strcpy_s(buf, 512, TEST_STRING);

    /* Write in async way */
    AsyncIoContext asyncContext1 = {CallbackFunc1, NULL};
    for(int i = 0; i < 1; i++) {
        ASSERT_EQ(PwriteAsync(testFd, buf, 512, i * 512, &asyncContext1), VFS_ERROR_PARAMETERS_INVALID);
    }

    /* Prepare read buffer */
    char *readBuf;
    posix_memalign((void**)&readBuf, 512, 512);
    memset_s(readBuf, 512, 0, 512);
    strcpy_s(readBuf, 512, TEST_STRING);

    /* Read in async way */
    for(int i = 0; i < 128; i++) {
        ASSERT_EQ(PreadAsync(testFd, readBuf, 512, i * 512, &asyncContext1), VFS_ERROR_PARAMETERS_INVALID);
    }

    ASSERT_EQ(Close(testFd), 0);
    /* The number of write string */
    ASSERT_EQ(GetAioFileLineNum(GetTestFilePath()), 0);
    /* The calling times of callback function, both write and read */
    ASSERT_EQ(callbackCount, 0);

    free(buf);
    free(readBuf);
}

static uint16_t enterNum = 0;
static uint16_t exitNum = 0;

static void AIOThreadEnterCallbackFunc(void) {
    enterNum++;
}

static void AIOThreadExitCallbackFunc(void) {
    exitNum++;
}

TEST_F(VfsStaticLinuxTest, EnableAIOCallbackTest) {
    ASSERT_EQ(EnableAIO(testVfs, 256, 1, AIOThreadEnterCallbackFunc, AIOThreadExitCallbackFunc), 0);
    
    /* sleep to wait aio thread start */
    usleep(100);

    /* aio thread start */
    ASSERT_GE(enterNum, 1);

    /* aio thread not exit */
    ASSERT_GE(exitNum, 0);

    ASSERT_EQ(ExitVfsModule(), 0);
    /* aio thread exit */
    ASSERT_GE(exitNum, 1);
}