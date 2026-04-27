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
 * vfs_linux_common.h
 *
 * Description:
 * This file defines linux file common definitions and operations
 *
 * ---------------------------------------------------------------------------------
 */
#ifndef UTILS_VFS_LINUX_COMMON_H
#define UTILS_VFS_LINUX_COMMON_H

#include <fcntl.h>
#include <sys/types.h>
#include <stdio.h>
#include <libaio.h>
#include "types/data_types.h"
#include "types/atomic_type.h"
#include "container/linked_list.h"
#include "port/platform_port.h"
#include "vfs/vfs_linux_aio.h"
#include "vfs/vfs_adapter_interface.h"

GSDB_BEGIN_C_CODE_DECLS

#define LINUX_PATH_MAX_LEN     4096
#define LINUX_FILE_CREATE_FLAG (O_CREAT | O_RDWR | O_TRUNC)
#define VFS_MAX_OPEN_FILE      100000000

/* Invalid linux arguments definitions */
#define INVALID_LINUX_FILE_FLAGS 0xFFFFFFFFU
#define INVALID_LINUX_FILE_MODE  0xFFFFFFFFU
#define INVALID_LINUX_SEEK_FLAG  (-1)
#define INVALID_LINUX_LOCK_TYPE  (-1)

static inline void VfsPrintLog(SYMBOL_UNUSED const char *fmt, ...)
{
#ifdef GSDB_DEBUG
    va_list args;
    va_start(args, fmt);
#define MAX_BUFFER_LEN 1024
    char buffer[MAX_BUFFER_LEN];
    int writeLen = vsprintf_s(buffer, sizeof(buffer), fmt, args);
    if (writeLen <= 0) {
        return;
    }
    (void)printf("[VFS]%s\n", buffer);
#endif
}

static inline void VfsPrintReleaseLog(SYMBOL_UNUSED const char *fmt, ...)
{
    va_list args;
    va_start(args, fmt);
#define MAX_BUFFER_LEN 1024
    char buffer[MAX_BUFFER_LEN];
    int writeLen = vsprintf_s(buffer, sizeof(buffer), fmt, args);
    if (writeLen <= 0) {
        return;
    }
    (void)printf("[VFS]%s\n", buffer);
}

typedef struct LinuxFileHandle LinuxFileHandle;
typedef struct LinuxVfsHandle LinuxVfsHandle;
typedef struct LinuxClientHandle LinuxClientHandle;

#define VFS_LINUX_AIO_CALLBACK_NUM     2
#define VFS_LINUX_READ_AIO_IOCB_INDEX  0
#define VFS_LINUX_WRITE_AIO_IOCB_INDEX 1

typedef struct UtilsAioIocb UtilsAioIocb;
struct UtilsAioIocb {
    struct iocb iocb;
    LinuxFileHandle *fd;
    AsyncIoCallback asyncCallback;
};

struct LinuxFileHandle {
    int storageFd;
    LinuxVfsHandle *vfsHandle;
    uint64_t accessCount;
    int64_t currentMaxWriteOffset;
    FileDescriptor *upperFd;
    AdapterFlushCallback flushCallback;
    void *flushAsyncContext;
    char *fileName;
    uint32_t openFlag;
    int64_t lastFilePos;
    Atomic32 lockCount;
    DListNode fileHandleNode;
    UtilsAioIocb *aioIocb;
    Atomic32 asyncReqCount;
};

struct LinuxClientHandle {
    Atomic32 mountVfsCount;
};

struct LinuxVfsHandle {
    Atomic32 openFileCount;
    bool isStaticVfs;
    char vfsDir[LINUX_PATH_MAX_LEN];
    const char *rootDataDir;
    SpinLock fdListLock;
    DListHead sysOpenFileHandleList;
    AioThreadContext *aioThreadContext;
    LinuxClientHandle *vfsClientHandle;
};

void InitVfsHandle(LinuxVfsHandle *vfsHandle, bool isStatic);
void ReleaseVfsHandle(LinuxVfsHandle *vfsHandle);

/* Linux file utils common functions */
void *VfsMemAlloc(MemAllocator *memAllocator, uint64_t size);
void VfsMemFree(MemAllocator *memAllocator, void *ptr);
ErrorCode ConvertLinuxSysErrCode(int sysErrorCode);
bool CheckTenantClusterNameLen(const char *tenantName, const char *clusterName);
bool CheckTenantVfsNameLen(const char *tenantName, const char *vfsName);
void *LinuxDlOpenLibraryWithPathCheck(const char *libPath);
bool CheckSnapshotNameLen(const char *snapshotName);
bool CheckSnapshotFlag(int64_t flags);

/* Linux file common function definitions */
ErrorCode LinuxCommonCreate(VfsHandlePtr vfs, const char *pathName, const FileParam *param, FileHandlePtr *fileHandle,
                            MemAllocator *memAllocator);
ErrorCode LinuxCommonRemove(VfsHandlePtr vfs, const char *pathName);
ErrorCode LinuxCommonFileIsExist(VfsHandlePtr vfs, const char *pathName, bool *out);
ErrorCode LinuxCommonOpen(VfsHandlePtr vfs, const char *pathName, int flags, FileHandlePtr *fileHandle,
                          MemAllocator *memAllocator);
ErrorCode LinuxCommonRenameFile(VfsHandlePtr vfs, const char *srcPathName, const char *destPathName);
ErrorCode LinuxCommonClose(FileHandlePtr fileHandle, MemAllocator *memAllocator);
ErrorCode LinuxCommonFsync(FileHandlePtr fileHandle);
ErrorCode LinuxCommonFileSeek(FileHandlePtr fileHandle, int64_t offset, int seekFlag, int64_t *newPos);
ErrorCode LinuxCommonRewind(FileHandlePtr fileHandle);
ErrorCode LinuxCommonRead(FileHandlePtr fileHandle, void *buf, uint64_t count, int64_t *readSize);
ErrorCode LinuxCommonPread(FileHandlePtr fileHandle, void *buf, uint64_t count, int64_t offset, int64_t *readSize);
ErrorCode LinuxCommonPreadAsync(FileHandlePtr fileHandle, void *buf, uint64_t count, int64_t offset,
                                const AsyncIoContext *aioContext);
ErrorCode LinuxCommonWriteSync(FileHandlePtr fileHandle, const void *buf, uint64_t count, int64_t *writeSize);
ErrorCode LinuxCommonPwriteSync(FileHandlePtr fileHandle, const void *buf, uint64_t count, int64_t offset,
                                int64_t *writeSize);
ErrorCode LinuxCommonWriteAsync(FileHandlePtr fileHandle, const void *buf, uint64_t count,
                                const AsyncIoContext *aioContext);
ErrorCode LinuxCommonPwriteAsync(FileHandlePtr fileHandle, const void *buf, uint64_t count, int64_t offset,
                                 const AsyncIoContext *aioContext);
ErrorCode LinuxCommonExtend(FileHandlePtr fileHandle, int64_t length);
ErrorCode LinuxCommonTruncate(FileHandlePtr fileHandle, int64_t length);
ErrorCode LinuxCommonGetSize(FileHandlePtr fileHandle, int64_t *fileSize);
ErrorCode LinuxCommonFileControl(FileHandlePtr fileHandle, int cmd, const AdapterFileControlInfo *controlInfo);
ErrorCode LinuxCommonLockFile(FileHandlePtr fileHandle, int64_t startPos, int64_t len, int lockMode, uint32_t timeout);
ErrorCode LinuxCommonTryLockFile(FileHandlePtr fileHandle, int64_t startPos, int64_t len, int lockMode);
ErrorCode LinuxCommonUnlockFile(FileHandlePtr fileHandle, int64_t startPos, int64_t len);
ErrorCode LinuxCommonFDataSync(FileHandlePtr fileHandle);
ErrorCode LinuxCommonStartAIO(VfsHandlePtr vfs, uint16_t maxEvents, uint16_t threadCount,
                              void (*threadEnterCallback)(void), void (*threadExitCallback)(void));

#ifdef ENABLE_FAULT_INJECTION

/* Fault injection related definitions */
enum FaultInjectionVfsPoint {
    MOCK_DYNAMIC_VFS_INIT,
    MOCK_GENERATE_VFS_DIRECTORY,
};

#endif /* ENABLE_FAULT_INJECTION */

#define MODE_COMBINATION_VALUE_MIN 1
#define MODE_COMBINATION_VALUE_MAX ((1 << 6) - 1) /* Related to the number of mode */
static inline bool CheckFileModeValidation(int mode)
{
    if (mode >= MODE_COMBINATION_VALUE_MIN && mode <= MODE_COMBINATION_VALUE_MAX) {
        return true;
    }
    return false;
}

int ForceRemoveDir(const char *path);

GSDB_END_C_CODE_DECLS

#endif /* UTILS_VFS_LINUX_COMMON_H */
