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

#ifndef UTILS_COMMON_UT_VFS_COMMON_H
#define UTILS_COMMON_UT_VFS_COMMON_H

#include <string>
#include <sys/stat.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>
#include "gtest/gtest.h"
#include "securec.h"
#include "vfs/vfs_interface.h"

#define MULTI_THREAD_TEST_COUNT 3
#define LETTER_COUNT 26
#define DIGIT_COUNT 10
#define SIZE_8K 8192
#ifdef GSDB_DEBUG
/* To avoid print lots of log in console, reduce test file count */
#define MAX_FILE_ID 1024
#define MAX_OPERATION_FILE_ID 1024
#else
/* Vfs log print is disabled in release version, need not to care about log print in console */
#define MAX_FILE_ID 0xFFFF
#define MAX_OPERATION_FILE_ID 10000
#endif
#define VFS_UT_DATA_DIR "vfs_ut_data"
#define VFS_UT_FILE_ID 50689

#define TEST_CLUSTER_NAME "ClusterName"
#define TEST_TENANT_NAME "TenantName"
#define TEST_STORESPACE_NAME "StoreSpaceName"

constexpr int64_t MB_SIZE = 1024 * 1024;
constexpr MemAllocator *NULL_MEM_ALLOCATOR = nullptr;

struct UtFlushContext {
    off_t flushOffset = 0;
    ErrorCode errCode = 0;
};

struct DataInfo {
    const char *data;
    int64_t dataLen;
};

struct FileInfo {
    std::string *pathName;
};

struct BlockList {
    uint32_t *block;
    uint32_t listLen;
    uint32_t blockSize;
};

struct LockInfo {
    int64_t startPos;
    int64_t len;
};

void UtFlushCallback(FileDescriptor *fd, int64_t offset, ErrorCode errorCode, void *asyncContext);

std::string GenerateTestVfsPath(std::string defaultTestDataDir, int index);

std::string GenerateTestFilePath(const std::string &dataDir, int fileId);

void ReadWriteConcurrency(VirtualFileSystem *vfs, FileInfo fileInfo, DataInfo dataInfo, FileParameter param);

void PreadPwriteConcurrency(VirtualFileSystem *vfs, FileInfo fileInfo, DataInfo dataInfo, FileParameter param);

void PreadPwriteOneFile(VirtualFileSystem *vfs, FileInfo fileInfo, DataInfo dataInfo, BlockList blockList);

void GetLockFileResult(VirtualFileSystem *vfs, FileInfo fileInfo, LockInfo lockInfo, bool *result);

bool CheckDirectoryExist(const char *directory);

#endif /* UTILS_COMMON_UT_VFS_COMMON_H */
