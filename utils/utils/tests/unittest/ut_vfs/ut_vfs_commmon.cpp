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

#include "ut_vfs_common.h"

void UtFlushCallback(FileDescriptor *fd, int64_t offset, ErrorCode errorCode, void *asyncContext)
{
    (void)fd;
    auto *context = (UtFlushContext*)asyncContext;
    if (errorCode != 0) {
        context->errCode = -1;
    } else {
        context->flushOffset = offset;
    }
}

std::string GenerateTestVfsPath(std::string defaultTestDataDir, int index)
{
    if (index == 0) {
        return defaultTestDataDir;
    }
    return defaultTestDataDir + "_" + std::to_string(index);
}

std::string GenerateTestFilePath(const std::string &dataDir, int fileId)
{
    return dataDir + "/test." + std::to_string(fileId) + ".data";
}

void ReadWriteConcurrency(VirtualFileSystem *vfs, FileInfo fileInfo, DataInfo dataInfo, FileParameter param)
{
    int writeTime = 10;
    FileDescriptor *fd;
    ASSERT_EQ(Create(vfs, fileInfo.pathName->c_str(), param, &fd), 0);
    ASSERT_EQ(Extend(fd, writeTime * dataInfo.dataLen), 0);
    int64_t writeSize, readSize;
    for (int i = 0; i < writeTime; ++i) {
        writeSize = -1;
        ASSERT_EQ(WriteSync(fd, dataInfo.data, dataInfo.dataLen, &writeSize), 0);
        ASSERT_EQ(writeSize, dataInfo.dataLen);
    }
    char *readData = (char *)malloc(dataInfo.dataLen);
    ASSERT_NE(readData, nullptr);
    ASSERT_EQ(Rewind(fd), 0);
    for (int i = 0; i < writeTime; ++i) {
        readSize = -1;
        ASSERT_EQ(memset_s(readData, dataInfo.dataLen, 0, dataInfo.dataLen), 0);
        ASSERT_EQ(Read(fd, readData, dataInfo.dataLen, &readSize), 0);
        ASSERT_EQ(readSize, dataInfo.dataLen);
        ASSERT_EQ(strncmp(readData, dataInfo.data, dataInfo.dataLen), 0);
    }
    free(readData);
    ASSERT_EQ(Close(fd), 0);
    ASSERT_EQ(Remove(vfs, fileInfo.pathName->c_str()), 0);
}

void PreadPwriteConcurrency(VirtualFileSystem *vfs, FileInfo fileInfo, DataInfo dataInfo, FileParameter param)
{
    int writeTime = 10;
    FileDescriptor *fd;
    ASSERT_EQ(Create(vfs, fileInfo.pathName->c_str(), param, &fd), 0);
    ASSERT_EQ(Extend(fd, writeTime * dataInfo.dataLen), 0);
    int64_t writeSize, readSize;
    for (int i = writeTime - 1; i >= 0; --i) {
        writeSize = -1;
        ASSERT_EQ(PwriteSync(fd, dataInfo.data, dataInfo.dataLen, dataInfo.dataLen * i, &writeSize), 0);
        ASSERT_EQ(writeSize, dataInfo.dataLen);
    }
    char *readData = (char *)malloc(dataInfo.dataLen);
    ASSERT_NE(readData, nullptr);
    for (int i = 0; i < writeTime; ++i) {
        readSize = -1;
        ASSERT_EQ(memset_s(readData, dataInfo.dataLen, 0, dataInfo.dataLen), 0);
        ASSERT_EQ(Pread(fd, readData, dataInfo.dataLen, dataInfo.dataLen * i, &readSize), 0);
        ASSERT_EQ(readSize, dataInfo.dataLen);
        ASSERT_EQ(strncmp(readData, dataInfo.data, dataInfo.dataLen), 0);
    }
    free(readData);
    ASSERT_EQ(Close(fd), 0);
    ASSERT_EQ(Remove(vfs, fileInfo.pathName->c_str()), 0);
}

void PreadPwriteOneFile(VirtualFileSystem *vfs, FileInfo fileInfo, DataInfo dataInfo, BlockList blockList)
{
    FileDescriptor *fd;
    ASSERT_EQ(Open(vfs, fileInfo.pathName->c_str(), FILE_READ_AND_WRITE_FLAG, &fd), 0);
    int64_t writeSize;
    for (uint32_t i = 0; i < blockList.listLen; ++i) {
        int64_t offset = (int64_t)(blockList.block[i]) * blockList.blockSize;
        ASSERT_EQ(PwriteSync(fd, dataInfo.data, dataInfo.dataLen, offset, &writeSize), 0);
        ASSERT_EQ(writeSize, dataInfo.dataLen);
    }
    ASSERT_EQ(Close(fd), 0);
}

void GetLockFileResult(VirtualFileSystem *vfs, FileInfo fileInfo, LockInfo lockInfo, bool *result)
{
    FileDescriptor *fd;
    ASSERT_EQ(Open(vfs, fileInfo.pathName->c_str(), FILE_READ_AND_WRITE_FLAG, &fd), 0);
    ErrorCode ret = LockFile(fd, lockInfo.startPos, lockInfo.len, FILE_EXCLUSIVE_LOCK, 1);
    ASSERT_EQ(Close(fd), 0);
    if (ret == 0) {
        *result = true;
    } else if (ret == VFS_ERROR_LOCK_FILE_FAIL) {
        *result = false;
    } else {
        ASSERT_TRUE(false);
    }
}
