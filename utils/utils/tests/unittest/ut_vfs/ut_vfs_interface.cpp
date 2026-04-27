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

#include <cstring>

#include "ut_vfs_interface.h"

void UtFlushCallbackFunc(FileDescriptor *fd, int64_t offset, ErrorCode errorCode, void *asyncContext)
{
    (void)fd;
    auto *context = (FlushContext*)asyncContext;
    if (errorCode != 0) {
        context->errCode = -1;
    } else {
        context->flushOffset = offset;
    }
}

TEST_F(VfsInterfaceTest, CreateFileTest)
{
    FileDescriptor *fd800, *fd900;
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
    ASSERT_EQ(Create(m_testStaticVfs, UT_FILENAME_1, filePara, &fd800), 0);
    ASSERT_EQ(Create(m_testStaticVfs, UT_FILENAME_2, filePara, &fd900), 0);
    bool fileIsExist;
    ASSERT_EQ(FileIsExist(m_testStaticVfs, UT_FILENAME_1, &fileIsExist), 0);
    ASSERT_TRUE(fileIsExist);
    ASSERT_EQ(FileIsExist(m_testStaticVfs, UT_FILENAME_2, &fileIsExist), 0);
    ASSERT_TRUE(fileIsExist);
    ASSERT_EQ(Close(fd800), 0);
    ASSERT_EQ(Close(fd900), 0);

    /* Cannot create a file that is existed */
    ASSERT_NE(Create(m_testStaticVfs, UT_FILENAME_1, filePara, &fd800), 0);

    ASSERT_EQ(Open(m_testStaticVfs, UT_FILENAME_1, FILE_READ_AND_WRITE_FLAG, &fd800), 0);
    ASSERT_EQ(Open(m_testStaticVfs, UT_FILENAME_2, FILE_READ_AND_WRITE_FLAG, &fd900), 0);
    ASSERT_EQ(Close(fd800), 0);
    ASSERT_EQ(Close(fd900), 0);

    ASSERT_EQ(Remove(m_testStaticVfs, UT_FILENAME_1), 0);
    ASSERT_EQ(Remove(m_testStaticVfs, UT_FILENAME_2), 0);
    ASSERT_EQ(FileIsExist(m_testStaticVfs, UT_FILENAME_2, &fileIsExist), 0);
    ASSERT_FALSE(fileIsExist);
    ASSERT_NE(Remove(m_testStaticVfs, UT_FILENAME_1), 0);

    /* Cannot open a file that is not existed */
    ASSERT_NE(Open(m_testStaticVfs, UT_FILENAME_2, FILE_READ_AND_WRITE_FLAG, &fd900), 0);
}

TEST_F(VfsInterfaceTest, ReadWriteFileTest)
{
    const char writeText[] = "HelloWorld";
    const char digitText[] = "0123456789";
    char readText[100];
    const int writeTextLen = 10;
    int64_t writeSize = 0;
    int64_t readSize = 0;

    FileDescriptor *fd800;
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
    ASSERT_EQ(Create(m_testStaticVfs, UT_FILENAME_1, filePara, &fd800), 0);

    int writeTime = 2;
    for (int i = 0; i < writeTime; ++i) {
        ASSERT_EQ(WriteAsync(fd800, writeText, writeTextLen, nullptr), 0);
    }
    ASSERT_EQ(Fsync(fd800), 0);
    int64_t currentFileSize;
    ASSERT_EQ(GetSize(fd800, &currentFileSize), 0);
    ASSERT_EQ(currentFileSize, writeTextLen * writeTime);
    for (int i = 0; i < writeTime; ++i) {
        ASSERT_EQ(WriteSync(fd800, digitText, writeTextLen, &writeSize), 0);
        ASSERT_EQ(writeSize, writeTextLen);
    }
    ASSERT_EQ(GetSize(fd800, &currentFileSize), 0);
    ASSERT_EQ(currentFileSize, writeTextLen * writeTime * 2);

    ASSERT_EQ(Pread(fd800, readText, writeTextLen * writeTime * 2, 0, &readSize), 0);
    ASSERT_EQ(readSize, writeTextLen * writeTime * 2);
    ASSERT_EQ(strncmp("HelloWorldHelloWorld01234567890123456789", readText, writeTextLen * writeTime * 2), 0);

    ASSERT_EQ(Rewind(fd800), 0);
    ASSERT_EQ(WriteSync(fd800, digitText, writeTextLen, &writeSize), 0);
    ASSERT_EQ(writeSize, writeTextLen);
    int64_t fileOffset = -1;
    ASSERT_EQ(FileSeek(fd800, 0, FILE_SEEK_SET, &fileOffset), 0);
    ASSERT_EQ(fileOffset, 0);
    readSize = 0;
    ASSERT_EQ(Read(fd800, readText, writeTextLen * writeTime * 2, &readSize), 0);
    ASSERT_EQ(readSize, writeTextLen * writeTime * 2);
    ASSERT_EQ(strncmp("0123456789HelloWorld01234567890123456789", readText, writeTextLen * writeTime * 2), 0);
    ASSERT_EQ(FileSeek(fd800, 0, FILE_SEEK_CUR, &fileOffset), 0);
    ASSERT_EQ(fileOffset, writeTextLen * writeTime * 2);

    ASSERT_EQ(Close(fd800), 0);
}

TEST_F(VfsInterfaceTest, PreadPwriteFileTest)
{
    const char writeText[] = "HelloWorld";
    char readText[100];
    const int writeTextLen = 10;
    ssize_t writeSize;
    ssize_t readSize;

    FileDescriptor *fd800;
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
    ASSERT_EQ(Create(m_testStaticVfs, UT_FILENAME_1, filePara, &fd800), 0);

    ASSERT_EQ(PwriteAsync(fd800, writeText, writeTextLen, 0, nullptr), 0);
    ASSERT_EQ(Fsync(fd800), 0);
    off_t currentFileSize;
    ASSERT_EQ(GetSize(fd800, &currentFileSize), 0);
    ASSERT_EQ(currentFileSize, writeTextLen);
    ASSERT_EQ(PwriteSync(fd800, writeText, writeTextLen, currentFileSize, &writeSize), 0);
    ASSERT_EQ(writeSize, writeTextLen);

    ASSERT_EQ(Pread(fd800, readText, writeTextLen * 2, 0, &readSize), 0);
    ASSERT_EQ(readSize, writeTextLen * 2);
    ASSERT_EQ(strncmp("HelloWorldHelloWorld", readText, writeTextLen * 2), 0);

    ASSERT_EQ(PwriteSync(fd800, writeText, writeTextLen, 5, &writeSize), 0);
    ASSERT_EQ(writeSize, writeTextLen);
    ASSERT_EQ(Pread(fd800, readText, writeTextLen * 2, 0, &readSize), 0);
    ASSERT_EQ(readSize, writeTextLen * 2);
    ASSERT_EQ(strncmp("HelloHelloWorldWorld", readText, writeTextLen * 2), 0);

    ASSERT_EQ(Close(fd800), 0);
}

TEST_F(VfsInterfaceTest, ExtendTruncateFileTest)
{
    FileDescriptor *fd800;
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
    ASSERT_EQ(Create(m_testStaticVfs, UT_FILENAME_1, filePara, &fd800), 0);

    off_t currentFileSize;
    off_t targetSize = (off_t)128 * 1024 * 1024; /* 128M */
    ASSERT_EQ(Extend(fd800, targetSize), 0);
    ASSERT_EQ(GetSize(fd800, &currentFileSize), 0);
    ASSERT_EQ(currentFileSize, targetSize);

    targetSize = (off_t)512 * 1024 * 1024; /* 512M */
    ASSERT_EQ(Extend(fd800, targetSize), 0);
    ASSERT_EQ(GetSize(fd800, &currentFileSize), 0);
    ASSERT_EQ(currentFileSize, targetSize);

    targetSize = (off_t)256 * 1024 * 1024; /* 256M */
    ASSERT_EQ(Truncate(fd800, targetSize), 0);
    ASSERT_EQ(GetSize(fd800, &currentFileSize), 0);
    ASSERT_EQ(currentFileSize, targetSize);

    ASSERT_EQ(Truncate(fd800, 0), 0);
    ASSERT_EQ(GetSize(fd800, &currentFileSize), 0);
    ASSERT_EQ(currentFileSize, 0);

    ASSERT_EQ(Close(fd800), 0);
}

TEST_F(VfsInterfaceTest, FlushCallbackOffsetTest)
{
    const char writeText[] = "HelloWorld";
    const char digitText[] = "0123456789";
    char readText[100];
    const int writeTextLen = 10;
    int64_t readSize;
    int writeTime = 4;

    FileDescriptor *fd800;
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
    ASSERT_EQ(Create(m_testStaticVfs, UT_FILENAME_1, filePara, &fd800), 0);
    FlushContext flushContext;
    FileControlInfo controlInfo;
    controlInfo.flushCallbackInfo.callback = UtFlushCallbackFunc;
    controlInfo.flushCallbackInfo.asyncContext = &flushContext;
    ASSERT_EQ(FileControl(fd800, SET_FILE_FLUSH_CALLBACK, &controlInfo), 0);
    ASSERT_EQ(Extend(fd800, writeTime * writeTextLen), 0);

    for (int i = 0; i < writeTime; ++i) {
        ASSERT_EQ(PwriteAsync(fd800, writeText, writeTextLen, i * writeTextLen, nullptr), 0);
        ASSERT_EQ(Fsync(fd800), 0);
        ASSERT_EQ(flushContext.errCode, 0);
        ASSERT_EQ(flushContext.flushOffset, (i + 1) * writeTextLen);
    }
    ASSERT_EQ(Pread(fd800, readText, writeTime * writeTextLen, 0, &readSize), 0);
    ASSERT_EQ(readSize, writeTime * writeTextLen);
    ASSERT_EQ(strncmp(readText, "HelloWorldHelloWorldHelloWorldHelloWorld", writeTime * writeTextLen), 0);

    ASSERT_EQ(Rewind(fd800), 0);
    for (int i = 0; i < writeTime / 2; ++i) {
        ASSERT_EQ(PwriteAsync(fd800, digitText, writeTextLen, i * writeTextLen, nullptr), 0);
        ASSERT_EQ(Fsync(fd800), 0);
        ASSERT_EQ(flushContext.errCode, 0);
        ASSERT_EQ(flushContext.flushOffset, (i + 1) * writeTextLen);
    }
    ASSERT_EQ(Pread(fd800, readText, writeTime * writeTextLen, 0, &readSize), 0);
    ASSERT_EQ(readSize, writeTime * writeTextLen);
    ASSERT_EQ(strncmp(readText, "01234567890123456789HelloWorldHelloWorld", writeTime * writeTextLen), 0);

    ASSERT_EQ(Close(fd800), 0);
}

TEST_F(VfsInterfaceTest, TestExitModule)
{
    FileDescriptor *fd800;
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
    ASSERT_EQ(Create(m_testStaticVfs, UT_FILENAME_1, filePara, &fd800), 0);
    ASSERT_EQ(ExitVfsModule(), VFS_ERROR_VFS_RESOURCE_NOT_RELEASE);

    ASSERT_EQ(Close(fd800), 0);
    ASSERT_EQ(ExitVfsModule(), 0);
}