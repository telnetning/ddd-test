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
 * vfs_linux_common.c
 *
 * Description:
 * This file defines linux file common function implementation
 *
 * ---------------------------------------------------------------------------------
 */
#include <dlfcn.h>
#include <sys/stat.h>
#include "vfs/vfs_linux_aio.h"
#include "vfs/vfs_linux_common.h"

#define LINUX_OPEN_FILE_EXTRA_FLAG (O_CLOEXEC)

#define FILE_OPS_RETRY_TIME 3
#define US_PER_MS           1000

static inline bool IsAbsolutePath(const char *pathName)
{
    return pathName[0] == '/';
}

static inline bool CheckFileIsExist(const char *pathName)
{
    return access(pathName, F_OK) == 0;
}

static inline mode_t CreateFileMode(uint32_t flag)
{
    mode_t mode = 0;
    if (flag & O_CREAT) {
        mode |= (S_IRUSR | S_IWUSR);
    }
    return mode;
}

static inline int OpenFileWithPathCheck(const char *pathName, uint32_t flag)
{
    char fileRealPath[PATH_MAX] = {0};
    if (realpath(pathName, fileRealPath) == NULL) {
        VfsPrintReleaseLog("readpath() return NULL by input path");
        return -1;
    }
    return open(fileRealPath, (int)flag, CreateFileMode(flag));
}

static int CreateFileWithPathCheck(const char *pathName, uint32_t flag, mode_t mode, ErrorCode *errorCode)
{
    ASSERT(errorCode != NULL);
    char tempPathName[PATH_MAX] = {0};
    if (strcpy_s(tempPathName, sizeof(tempPathName), pathName) != EOK) {
        *errorCode = VFS_ERROR_SECURE_FUNCTION_FAIL;
        return -1;
    }
    /* Check upper directory */
    char *pathDirectory = Dirname(tempPathName);
    char fileRealPath[PATH_MAX] = {0};
    char *dirPath = realpath(pathDirectory, fileRealPath);
    int sysErrNo;
    if (dirPath == NULL) {
        sysErrNo = errno;
        *errorCode = ConvertLinuxSysErrCode(sysErrNo);
        VfsPrintReleaseLog("Create file (pathName:%s) check upper directory failed with errno (%d)",
                           pathName, sysErrNo);
        return -1;
    }
    /* Check input file */
    char *rpath = realpath(pathName, fileRealPath);
    if (rpath != NULL) {
        *errorCode = VFS_ERROR_FILE_IS_EXIST;
        VfsPrintReleaseLog("Create file (pathName:%s) already exist", pathName);
        return -1;
    }
    /* WARNING: The expected mode may be different from the actual file mode,
     * because the actual mode of the created file is (mode & ~umask).
     */
    int fd = open(fileRealPath, (int)flag, mode);
    if (fd >= 0) {
        *errorCode = ERROR_SYS_OK;
        return fd;
    }
    sysErrNo = errno;
    *errorCode = ConvertLinuxSysErrCode(sysErrNo);
    VfsPrintReleaseLog("Create file (pathName:%s) failed with errno (%d)", pathName, sysErrNo);
    return -1;
}

static inline ErrorCode RemoveOneFile(const char *pathName)
{
    if (access(pathName, F_OK) != 0) {
        return VFS_ERROR_REMOVE_FILE_NOT_EXIST;
    }
    if (unlink(pathName) != 0) {
        return ConvertLinuxSysErrCode(errno);
    }
    return ERROR_SYS_OK;
}

static ErrorCode CloseOneFileTemporary(LinuxVfsHandle *vfsHandle)
{
    ASSERT(vfsHandle != NULL);

    if (DListIsEmpty(&(vfsHandle->sysOpenFileHandleList))) {
        return VFS_ERROR_OPERATION_DENIED;
    }

    DListNode *lastFileNode;
    LinuxFileHandle *lastFileHandle;

    bool getFileUnlock = false;
    for (int i = 0; i < FILE_OPS_RETRY_TIME; ++i) {
        lastFileNode = DListTailNode(&(vfsHandle->sysOpenFileHandleList));
        lastFileHandle = DLIST_CONTAINER(LinuxFileHandle, fileHandleNode, lastFileNode);
        if (GSDB_ATOMIC32_GET(&(lastFileHandle->lockCount)) != 0) {
            /* Current fd has file lock, cannot close this fd */
            SpinLockAcquire(&(vfsHandle->fdListLock));
            DListMoveHead(&(vfsHandle->sysOpenFileHandleList), lastFileNode);
            SpinLockRelease(&(vfsHandle->fdListLock));
            continue;
        }
        getFileUnlock = true;
        break;
    }
    if (!getFileUnlock) {
        return VFS_ERROR_OPERATION_DENIED;
    }

    lastFileHandle->lastFilePos = lseek(lastFileHandle->storageFd, 0, SEEK_CUR);
    (void)close(lastFileHandle->storageFd);
    lastFileHandle->storageFd = -1;
    SpinLockAcquire(&(vfsHandle->fdListLock));
    DListDelete(lastFileNode);
    SpinLockRelease(&(vfsHandle->fdListLock));
    return ERROR_SYS_OK;
}

static ErrorCode ReopenFile(LinuxFileHandle *fileHandle)
{
    ASSERT(fileHandle != NULL);

    if (fileHandle->storageFd > 0) {
        return ERROR_SYS_OK;
    }
    int fd = OpenFileWithPathCheck(fileHandle->fileName, fileHandle->openFlag | LINUX_OPEN_FILE_EXTRA_FLAG);
    if (fd < 0) {
        return VFS_ERROR_REOPEN_FILE_FAIL;
    }
    fileHandle->storageFd = fd;
    if (fileHandle->lastFilePos > 0) {
        (void)lseek(fileHandle->storageFd, fileHandle->lastFilePos, SEEK_SET);
    }
    SpinLockAcquire(&(fileHandle->vfsHandle->fdListLock));
    DListPushHead(&(fileHandle->vfsHandle->sysOpenFileHandleList), &(fileHandle->fileHandleNode));
    SpinLockRelease(&(fileHandle->vfsHandle->fdListLock));
    return ERROR_SYS_OK;
}

static ErrorCode TryReopenFileWithHandle(LinuxFileHandle *fileHandle)
{
    ASSERT(fileHandle != NULL);

    if (ReopenFile(fileHandle) == ERROR_SYS_OK) {
        return ERROR_SYS_OK;
    }
    for (int i = 0; i < FILE_OPS_RETRY_TIME; ++i) {
        if (CloseOneFileTemporary(fileHandle->vfsHandle) != 0) {
            continue;
        }
        if (ReopenFile(fileHandle) == ERROR_SYS_OK) {
            return ERROR_SYS_OK;
        }
    }
    return VFS_ERROR_REOPEN_FILE_FAIL;
}

static ErrorCode TryCloseAndCreateFile(LinuxVfsHandle *vfsHandle, const char *pathName, uint32_t flags, mode_t mode,
                                       int *fd)
{
    for (int i = 0; i < FILE_OPS_RETRY_TIME; ++i) {
        if (CloseOneFileTemporary(vfsHandle) != ERROR_SYS_OK) {
            continue;
        }
        ErrorCode errorCode = ERROR_SYS_OK;
        int newSysFd = CreateFileWithPathCheck(pathName, flags | LINUX_OPEN_FILE_EXTRA_FLAG, mode, &errorCode);
        if (newSysFd >= 0) {
            *fd = newSysFd;
            return ERROR_SYS_OK;
        }
        if (errorCode != VFS_ERROR_OPENED_FILE_REACH_MAX) {
            return errorCode;
        }
    }
    return VFS_ERROR_OPENED_FILE_REACH_MAX;
}

static ErrorCode TryCloseAndOpenFile(LinuxVfsHandle *vfsHandle, const char *pathName, uint32_t flags, int *fd)
{
    for (int i = 0; i < FILE_OPS_RETRY_TIME; ++i) {
        if (CloseOneFileTemporary(vfsHandle) != ERROR_SYS_OK) {
            continue;
        }
        int newSysFd = OpenFileWithPathCheck(pathName, flags | LINUX_OPEN_FILE_EXTRA_FLAG);
        if (newSysFd >= 0) {
            *fd = newSysFd;
            return ERROR_SYS_OK;
        }
    }
    return VFS_ERROR_OPENED_FILE_REACH_MAX;
}

static void FillFileHandle(LinuxFileHandle *fileHandle, int storageFd, uint32_t openFlag, LinuxVfsHandle *vfsHandle)
{
    fileHandle->storageFd = storageFd;
    fileHandle->vfsHandle = vfsHandle;
    fileHandle->accessCount = 0;
    fileHandle->currentMaxWriteOffset = 0;
    fileHandle->upperFd = NULL;
    fileHandle->flushCallback = NULL;
    fileHandle->flushAsyncContext = NULL;
    fileHandle->openFlag = openFlag;
    fileHandle->lastFilePos = 0;
    GSDB_ATOMIC32_SET(&(fileHandle->lockCount), 0);
    if (fileHandle->aioIocb != NULL) {
        (void)memset_s(fileHandle->aioIocb, sizeof(struct UtilsAioIocb) * VFS_LINUX_AIO_CALLBACK_NUM, 0,
                       sizeof(struct UtilsAioIocb) * VFS_LINUX_AIO_CALLBACK_NUM);
    }

    GSDB_ATOMIC32_SET(&(fileHandle->asyncReqCount), 0);
}

static inline void ReleaseFileHandle(LinuxFileHandle *fileHandle, MemAllocator *memAllocator)
{
    if (fileHandle) {
        if (fileHandle->fileName) {
            VfsMemFree(memAllocator, fileHandle->fileName);
        }
        VfsMemFree(memAllocator, fileHandle);
    }
}

void InitVfsHandle(LinuxVfsHandle *vfsHandle, bool isStatic)
{
    GSDB_ATOMIC32_SET(&(vfsHandle->openFileCount), 0);
    vfsHandle->isStaticVfs = isStatic;
    vfsHandle->aioThreadContext = NULL;
    SpinLockInit(&(vfsHandle->fdListLock));
    DListInit(&(vfsHandle->sysOpenFileHandleList));
}

void ReleaseVfsHandle(LinuxVfsHandle *vfsHandle)
{
    SpinLockDestroy(&(vfsHandle->fdListLock));
}

static inline void AddFdToVfsHandle(LinuxVfsHandle *vfsHandle, LinuxFileHandle *fileHandle)
{
    ASSERT(vfsHandle != NULL);
    ASSERT(fileHandle != NULL);
    SpinLockAcquire(&(vfsHandle->fdListLock));
    DListPushHead(&(vfsHandle->sysOpenFileHandleList), &(fileHandle->fileHandleNode));
    SpinLockRelease(&(vfsHandle->fdListLock));
}

static inline void RemoveFdFromVfsHandle(LinuxVfsHandle *vfsHandle, LinuxFileHandle *fileHandle)
{
    ASSERT(vfsHandle != NULL);
    ASSERT(fileHandle != NULL);
    SpinLockAcquire(&(vfsHandle->fdListLock));
    DListDelete(&(fileHandle->fileHandleNode));
    SpinLockRelease(&(vfsHandle->fdListLock));
}

/* Common functions */
void *VfsMemAlloc(MemAllocator *memAllocator, uint64_t size)
{
    if (memAllocator) {
        return memAllocator->alloc(memAllocator, size);
    } else {
        return malloc(size);
    }
}

void VfsMemFree(MemAllocator *memAllocator, void *ptr)
{
    if (memAllocator) {
        memAllocator->free(memAllocator, ptr);
    } else {
        free(ptr);
    }
}

ErrorCode ConvertLinuxSysErrCode(int sysErrorCode)
{
    switch (sysErrorCode) {
        case EAGAIN:
            return VFS_ERROR_NEED_RETRY_AGAIN;
        case EACCES:
            return VFS_ERROR_FILE_ACCESS_DENIED;
        case EDQUOT:
        case ENOSPC:
            return VFS_ERROR_DISK_HAS_NO_SPACE;
        case EBADF:
        case EFAULT:
        case EFBIG:
        case EINVAL:
        case EISDIR:
        case ENAMETOOLONG:
        case EOVERFLOW:
        case EPERM:
        case EROFS:
            return VFS_ERROR_PARAMETERS_INVALID;
        case EMFILE:
            return VFS_ERROR_OPENED_FILE_REACH_MAX;
        case ENOMEM:
            return VFS_ERROR_OUT_OF_MEMORY;
        default:
            return VFS_ERROR_LOCAL_FILE_OPERATION_FAILED;
    }
}

bool CheckTenantClusterNameLen(const char *tenantName, const char *clusterName)
{
    size_t tenantNameLen = strlen(tenantName);
    size_t clusterNameLen = strlen(clusterName);
    if (tenantNameLen == 0 || tenantNameLen > TENANT_NAME_MAX_LEN - 1) {
        VfsPrintReleaseLog("The length of tenant name is %d, and the max length of tenant name is %d, tenant name: %s",
                    tenantNameLen, TENANT_NAME_MAX_LEN - 1, tenantName);
        return false;
    }
    if (clusterNameLen == 0 || clusterNameLen > CLUSTER_NAME_MAX_LEN - 1) {
        VfsPrintReleaseLog(
            "The length of cluster name is %d, and the max length of cluster name is %d, cluster name: %s",
            clusterNameLen, CLUSTER_NAME_MAX_LEN - 1, clusterName);
        return false;
    }

    return true;
}

bool CheckTenantVfsNameLen(const char *tenantName, const char *vfsName)
{
    size_t tenantNameLen = strlen(tenantName);
    size_t vfsNameLen = strlen(vfsName);
    if (tenantNameLen == 0 || tenantNameLen > TENANT_NAME_MAX_LEN - 1) {
        VfsPrintReleaseLog("The length of tenant name is %d, and the max length of tenant name is %d, tenant name: %s",
                    tenantNameLen, TENANT_NAME_MAX_LEN - 1, tenantName);
        return false;
    }
    if (vfsNameLen == 0 || vfsNameLen > VFS_NAME_MAX_LEN - 1) {
        VfsPrintReleaseLog("The length of vfs name is %d, and the max length of vfs name is %d, vfs name: %s",
                           vfsNameLen, VFS_NAME_MAX_LEN - 1, vfsName);
        return false;
    }

    return true;
}

static inline bool CheckFilePathNameLen(const char *pathName)
{
    size_t pathnameLen = strlen(pathName);
    if (pathnameLen > 0 && pathnameLen < FILE_PATH_MAX_LEN) {
        return true;
    }
    VfsPrintReleaseLog("File path len (%lu) is invalid", pathnameLen);
    return false;
}

bool CheckSnapshotNameLen(const char *snapshotName)
{
    size_t snapshotNameLen = strlen(snapshotName);
    if (snapshotNameLen > 0 && snapshotNameLen < SNAPSHOT_NAME_MAX_LEN) {
        return true;
    }
    VfsPrintReleaseLog("SnapshotName len (%lu) is invalid", snapshotNameLen);
    return false;
}

bool CheckSnapshotFlag(int64_t flags)
{
    switch (flags) {
        case SNAPSHOT_ALL_FILE_FLAG:
        case SNAPSHOT_IN_PLACE_UPDATE_FILE_FLAG:
        case SNAPSHOT_APPEND_FILE_FLAG:
            return true;
        default:
            return false;
    }
}

static inline bool CheckFileType(uint16_t fileType)
{
    switch (fileType) {
        case IN_PLACE_WRITE_FILE:
        case APPEND_WRITE_FILE:
            return true;
        default:
            return false;
    }
}

static uint32_t GetLinuxFileFlags(int flags)
{
#define MANDATORY_FLAGS_MASK 0x3
    uint32_t realFlags;
    uint32_t iflags = (uint32_t)flags;
    /* Mandatory flag */
    switch (iflags & MANDATORY_FLAGS_MASK) {
        case FILE_READ_AND_WRITE_FLAG:
            realFlags = O_RDWR;
            break;

        case FILE_READ_ONLY_FLAG:
            realFlags = O_RDONLY;
            break;

        case FILE_WRITE_ONLY_FLAG:
            realFlags = O_WRONLY;
            break;

        default:
            return INVALID_LINUX_FILE_FLAGS;
    }

    /* Optional flag */
    if (iflags & FILE_TRUNC_FLAG) {
        realFlags |= O_TRUNC;
    }

    if (iflags & FILE_APPEND_FLAG) {
        realFlags |= O_APPEND;
    }

    if (iflags & FILE_DIRECT_IO_FLAG) {
        realFlags |= O_DIRECT;
    }

    if (iflags & FILE_SYNC_FLAG) {
        realFlags |= O_SYNC;
    }

    if (iflags & FILE_ASYNC_IO_FLAG) {
        realFlags |= O_DIRECT;
    }

    return realFlags;
}

static mode_t GetLinuxFileMode(int mode)
{
    if (unlikely(!CheckFileModeValidation(mode))) {
        VfsPrintReleaseLog("Error log file mode %d is invalid\n", mode);
        return INVALID_LINUX_FILE_MODE;
    }

    mode_t filePermission = 0;
    if ((unsigned int)mode & FILE_READ_AND_WRITE_MODE) {
        filePermission |= (S_IRUSR | S_IWUSR);
    }
    if ((unsigned int)mode & FILE_GROUP_READ_MODE) {
        filePermission |= S_IRGRP;
    }
    if ((unsigned int)mode & FILE_GROUP_WRITE_MODE) {
        filePermission |= S_IWGRP;
    }
    if ((unsigned int)mode & FILE_OTHER_READ_MODE) {
        filePermission |= S_IROTH;
    }
    if ((unsigned int)mode & FILE_OTHER_WRITE_MODE) {
        filePermission |= S_IWOTH;
    }

    return filePermission;
}

static inline int GetLinuxSeekWhence(int seekFlag)
{
    switch (seekFlag) {
        case FILE_SEEK_SET:
            return SEEK_SET;
        case FILE_SEEK_CUR:
            return SEEK_CUR;
        case FILE_SEEK_END:
            return SEEK_END;
        default:
            return INVALID_LINUX_SEEK_FLAG;
    }
}

static inline short GetLinuxLockType(int lockMode)
{
    if (lockMode == FILE_EXCLUSIVE_LOCK) {
        return F_WRLCK;
    }
    return INVALID_LINUX_LOCK_TYPE;
}

static inline int64_t GetWriteEndOffset(int64_t writeStartOffset, int64_t writeSize)
{
    /* Do not consider overflow because current system cannot alloc 2^63 memory */
    return writeStartOffset + writeSize;
}

void *LinuxDlOpenLibraryWithPathCheck(const char *libPath)
{
    char libRealPathStr[PATH_MAX] = {0};
    char *libRealPath = libRealPathStr;
    if (realpath(libPath, libRealPath) == NULL) {
        VfsPrintReleaseLog("Dynamic library path %s does not exist", libPath);
        return NULL;
    }
    void *libHandle = dlopen(libRealPath, RTLD_LAZY);
    if (libHandle == NULL) {
        VfsPrintReleaseLog("Dynamic library %s dlopen error: %s", libPath, dlerror());
    }
    return libHandle;
}

static bool IsOutOfDirectory(const char *path)
{
    /* forbidden ..\0, ../, /../, backtrack path */
    uint32_t pos = 0;
    for (;;) {
        /* ignore repeat '/', './' and '.\0' path sign */
        while ((path[pos] == '/') || ((path[pos] == '.') && (path[pos + 1] == '/')) ||
               ((path[pos] == '.') && (path[pos + 1] == '\0'))) {
            pos++;
        }
        if (path[pos] == '\0') {
            break;
        }
        if ((path[pos] == '.') && (path[pos + 1] == '.') &&
            ((path[pos + 2] == '/') || (path[pos + 2] == '\0'))) { /* 2 :get '../' or '..\0' chars */
            return true;                                           /* '..' backtrack to lash path level */
        }
        /* until to next level */
        while ((path[pos] != '/') && (path[pos] != '\0')) {
            pos++;
        }
    }
    return false;
}

static ErrorCode GetFullPath(LinuxVfsHandle *vfsHandle, const char *pathName, MemAllocator *memAllocator, void *out,
                             size_t outLen)
{
    ASSERT(vfsHandle != NULL);
    ASSERT(out != NULL);
    if (!vfsHandle->isStaticVfs && IsOutOfDirectory(pathName)) {
        VfsPrintReleaseLog("The path backtrack is not permit!");
        return VFS_ERROR_OPERATION_NOT_SUPPORT;
    }
    char *buf = (char *)out;
    size_t pathBufLen = outLen;
    const char *prefix = ((vfsHandle->rootDataDir != NULL) ? vfsHandle->rootDataDir : "");
    const char *vfsDir = ((vfsHandle->isStaticVfs || IsAbsolutePath(pathName)) ? "" : vfsHandle->vfsDir);
    if (pathBufLen == 0) {
        pathBufLen = strlen(prefix) + strlen(vfsDir) + strlen(pathName) + 1;
        buf = (char *)VfsMemAlloc(memAllocator, pathBufLen);
        if (buf == NULL) {
            return VFS_ERROR_OUT_OF_MEMORY;
        }
        *(char **)out = buf;
    }
    if (sprintf_s(buf, pathBufLen, "%s%s%s", prefix, vfsDir, pathName) < 0) {
        if (outLen == 0) {
            VfsMemFree(memAllocator, buf);
        }
        return VFS_ERROR_SECURE_FUNCTION_FAIL;
    }
    return ERROR_SYS_OK;
}

static ErrorCode FillPathNameToFileHandle(LinuxFileHandle *fileHandle, LinuxVfsHandle *vfsHandle, const char *pathName,
                                          MemAllocator *memAllocator)
{
    ASSERT(fileHandle != NULL);
    ASSERT(vfsHandle != NULL);

    fileHandle->fileName = NULL;
    return GetFullPath(vfsHandle, pathName, memAllocator, &fileHandle->fileName, 0);
}

ErrorCode LinuxCommonCreate(VfsHandlePtr vfs, const char *pathName, const FileParam *param, FileHandlePtr *fileHandle,
                            MemAllocator *memAllocator)
{
    LinuxVfsHandle *curVfsHandle = (LinuxVfsHandle *)vfs;
    if (unlikely(curVfsHandle == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (unlikely(!CheckFilePathNameLen(pathName))) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (!CheckFileType(param->flag)) {
        VfsPrintLog("Create file type invalid");
    }
    mode_t linuxFileMode = GetLinuxFileMode(param->mode);
    if (unlikely(linuxFileMode == INVALID_LINUX_FILE_MODE)) {
        VfsPrintReleaseLog("Create file mode invalid");
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (unlikely(GSDB_ATOMIC32_GET(&(curVfsHandle->openFileCount)) >= VFS_MAX_OPEN_FILE)) {
        return VFS_ERROR_OPENED_FILE_REACH_MAX;
    }
    LinuxFileHandle *newFd = VfsMemAlloc(memAllocator, sizeof(LinuxFileHandle));
    ErrorCode ret = VFS_ERROR_OUT_OF_MEMORY;
    if (unlikely(newFd == NULL)) {
        return ret;
    }
    newFd->aioIocb = NULL;
    ret = FillPathNameToFileHandle(newFd, curVfsHandle, pathName, memAllocator);
    if (unlikely(ret != ERROR_SYS_OK)) {
        ReleaseFileHandle(newFd, memAllocator);
        return ret;
    }
    if (unlikely(access(newFd->fileName, F_OK) == 0)) {
        VfsPrintReleaseLog("Fail to Create file (pathName:%s), it already exist", pathName);
        ReleaseFileHandle(newFd, memAllocator);
        return VFS_ERROR_CREATE_FILE_EXIST; /* File already exists */
    }
#define LINUX_CREATE_FILE_FLAG (LINUX_FILE_CREATE_FLAG | LINUX_OPEN_FILE_EXTRA_FLAG)
    int fd = CreateFileWithPathCheck(newFd->fileName, LINUX_CREATE_FILE_FLAG, linuxFileMode, &ret);
    if (unlikely(fd < 0)) {
        if (ret != VFS_ERROR_OPENED_FILE_REACH_MAX) {
            ReleaseFileHandle(newFd, memAllocator);
            return ret;
        }
        ret = TryCloseAndCreateFile(curVfsHandle, newFd->fileName, LINUX_FILE_CREATE_FLAG, linuxFileMode, &fd);
        if (ret != ERROR_SYS_OK) {
            ReleaseFileHandle(newFd, memAllocator);
            return ret;
        }
    }
    FillFileHandle(newFd, fd, O_RDWR, curVfsHandle);
    GSDB_ATOMIC32_INC(&(curVfsHandle->openFileCount));
    AddFdToVfsHandle(curVfsHandle, newFd);
    *fileHandle = newFd;
    return ERROR_SYS_OK;
}

ErrorCode LinuxCommonRemove(VfsHandlePtr vfs, const char *pathName)
{
    if (unlikely(vfs == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (unlikely(!CheckFilePathNameLen(pathName))) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    LinuxVfsHandle *vfsHandle = (LinuxVfsHandle *)vfs;
    char fullPathName[LINUX_PATH_MAX_LEN];
    ErrorCode ret = GetFullPath(vfsHandle, pathName, NULL, fullPathName, sizeof(fullPathName));
    if (ret != ERROR_SYS_OK) {
        return ret;
    }
    return RemoveOneFile(fullPathName);
}

ErrorCode LinuxCommonFileIsExist(VfsHandlePtr vfs, const char *pathName, bool *out)
{
    if (unlikely(vfs == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    LinuxVfsHandle *vfsHandle = (LinuxVfsHandle *)vfs;
    if (unlikely(!CheckFilePathNameLen(pathName))) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    char fullPathName[LINUX_PATH_MAX_LEN];
    ErrorCode ret = GetFullPath(vfsHandle, pathName, NULL, fullPathName, sizeof(fullPathName));
    if (ret != ERROR_SYS_OK) {
        return ret;
    }
    *out = CheckFileIsExist(fullPathName);
    return ERROR_SYS_OK;
}

ErrorCode LinuxCommonOpen(VfsHandlePtr vfs, const char *pathName, int flags, FileHandlePtr *fileHandle,
                          MemAllocator *memAllocator)
{
    LinuxVfsHandle *curVfsHandle = (LinuxVfsHandle *)vfs;
    if (unlikely(curVfsHandle == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (unlikely(!CheckFilePathNameLen(pathName))) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    uint32_t linuxFileFlags = GetLinuxFileFlags(flags);
    if (unlikely(linuxFileFlags == INVALID_LINUX_FILE_FLAGS)) {
        VfsPrintReleaseLog("Open file flags does not support");
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (unlikely(GSDB_ATOMIC32_GET(&(curVfsHandle->openFileCount)) >= VFS_MAX_OPEN_FILE)) {
        return VFS_ERROR_OPENED_FILE_REACH_MAX;
    }
    size_t fdSize = sizeof(LinuxFileHandle);
    if (((unsigned int)flags & FILE_ASYNC_IO_FLAG) != 0) {
        fdSize += VFS_LINUX_AIO_CALLBACK_NUM * sizeof(UtilsAioIocb);
    }
    LinuxFileHandle *newFd = VfsMemAlloc(memAllocator, fdSize);
    if (unlikely(newFd == NULL)) {
        return VFS_ERROR_OUT_OF_MEMORY;
    }
    if (((unsigned int)flags & FILE_ASYNC_IO_FLAG) != 0) {
        newFd->aioIocb = (UtilsAioIocb *)(newFd + 1);
    } else {
        newFd->aioIocb = NULL;
    }
    ErrorCode ret = FillPathNameToFileHandle(newFd, curVfsHandle, pathName, memAllocator);
    if (unlikely(ret != ERROR_SYS_OK)) {
        ReleaseFileHandle(newFd, memAllocator);
        return ret;
    }
    if (unlikely(access(newFd->fileName, F_OK) != 0)) {
        VfsPrintReleaseLog("Open file (pathName:%s) does not exist", pathName);
        ReleaseFileHandle(newFd, memAllocator);
        return VFS_ERROR_OPEN_FILE_NOT_EXIST; /* Do not allow open non-exist file */
    }
    int fd = OpenFileWithPathCheck(newFd->fileName, linuxFileFlags | LINUX_OPEN_FILE_EXTRA_FLAG);
    if (unlikely(fd < 0)) {
        int sysErrorCode = errno;
        if (sysErrorCode != EMFILE) {
            VfsPrintReleaseLog("Open file (pathName:%s) failed with errno (%d)", pathName, sysErrorCode);
            ReleaseFileHandle(newFd, memAllocator);
            return ConvertLinuxSysErrCode(sysErrorCode);
        }
        ret = TryCloseAndOpenFile(curVfsHandle, newFd->fileName, linuxFileFlags, &fd);
        if (ret != ERROR_SYS_OK) {
            ReleaseFileHandle(newFd, memAllocator);
            return ret;
        }
    }
    FillFileHandle(newFd, fd, linuxFileFlags & (uint32_t)(~O_TRUNC), curVfsHandle);
    GSDB_ATOMIC32_INC(&(curVfsHandle->openFileCount));
    AddFdToVfsHandle(curVfsHandle, newFd);
    *fileHandle = newFd;
    return ERROR_SYS_OK;
}

ErrorCode LinuxCommonRenameFile(VfsHandlePtr vfs, const char *srcPathName, const char *destPathName)
{
    LinuxVfsHandle *curVfsHandle = (LinuxVfsHandle *)vfs;
    if (unlikely(curVfsHandle == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (unlikely(!CheckFilePathNameLen(srcPathName) && !CheckFilePathNameLen(destPathName))) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }

    /* check source file path */
    LinuxFileHandle srcFd = {0};
    ErrorCode ret = FillPathNameToFileHandle(&srcFd, curVfsHandle, srcPathName, NULL);
    if (unlikely(ret != ERROR_SYS_OK)) {
        return ret;
    }
    if (unlikely(access(srcFd.fileName, F_OK) != 0)) {
        VfsPrintReleaseLog("Source file (pathName:%s) does not exist", srcFd.fileName);
        free(srcFd.fileName);
        return VFS_ERROR_OPEN_FILE_NOT_EXIST; /* Do not allow open non-exist file */
    }

    /* check destination file path */
    LinuxFileHandle destFd = {0};
    ret = FillPathNameToFileHandle(&destFd, curVfsHandle, destPathName, NULL);
    if (unlikely(ret != ERROR_SYS_OK)) {
        free(srcFd.fileName);
        return ret;
    }

    if (rename(srcFd.fileName, destFd.fileName) != 0) {
        ret = ConvertLinuxSysErrCode(errno);
    }

    free(srcFd.fileName);
    free(destFd.fileName);
    return ret;
}

ErrorCode LinuxCommonClose(FileHandlePtr fileHandle, MemAllocator *memAllocator)
{
    LinuxFileHandle *curFd = (LinuxFileHandle *)fileHandle;
    if (unlikely(curFd == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }

    uint16_t tryTimes = 0;
#define MAX_TRY_TIMES     2000
#define AIO_WAIT_TIME_GAP 1000
    while (GSDB_ATOMIC32_GET(&(curFd->asyncReqCount)) != 0) {
        if (tryTimes > MAX_TRY_TIMES) {
            VfsPrintLog("Try time reach the max.");
            break;
        }
        (void)usleep(AIO_WAIT_TIME_GAP);
        tryTimes++;
    }

    if (curFd->storageFd >= 0) {
        if (close(curFd->storageFd) != 0) {
            return ConvertLinuxSysErrCode(errno);
        }
        RemoveFdFromVfsHandle(curFd->vfsHandle, curFd);
    }
    GSDB_ATOMIC32_DEC(&(curFd->vfsHandle->openFileCount));
    ReleaseFileHandle(curFd, memAllocator);
    return ERROR_SYS_OK;
}

ErrorCode LinuxCommonFsync(FileHandlePtr fileHandle)
{
    LinuxFileHandle *curFd = (LinuxFileHandle *)fileHandle;
    if (unlikely(curFd == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }

    curFd->accessCount += 1;
    if (unlikely(curFd->storageFd < 0)) {
        ErrorCode ret = TryReopenFileWithHandle(curFd);
        if (ret != ERROR_SYS_OK) {
            VfsPrintReleaseLog("Fsync failed because file cannot reopen");
            return ret;
        }
    }
    if (unlikely(fsync(curFd->storageFd) != 0)) {
        return ConvertLinuxSysErrCode(errno);
    }
    /* Write callback function */
    if (curFd->flushCallback != NULL) {
        /* Get current write max offset */
        curFd->flushCallback(curFd->upperFd, curFd->currentMaxWriteOffset, 0, curFd->flushAsyncContext);
    }
    return ERROR_SYS_OK;
}

ErrorCode LinuxCommonFileSeek(FileHandlePtr fileHandle, int64_t offset, int seekFlag, int64_t *newPos)
{
    LinuxFileHandle *curFd = (LinuxFileHandle *)fileHandle;
    if (unlikely(curFd == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }

    int linuxWhence = GetLinuxSeekWhence(seekFlag);
    if (unlikely(linuxWhence == INVALID_LINUX_SEEK_FLAG)) {
        VfsPrintReleaseLog("File seek flag invalid");
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    curFd->accessCount += 1;
    if (unlikely(curFd->storageFd < 0)) {
        ErrorCode ret = TryReopenFileWithHandle(curFd);
        if (ret != ERROR_SYS_OK) {
            return ret;
        }
    }
    int64_t resPos = lseek(curFd->storageFd, offset, linuxWhence);
    if (unlikely(resPos < 0)) {
        return ConvertLinuxSysErrCode(errno);
    }
    *newPos = resPos;
    return ERROR_SYS_OK;
}

ErrorCode LinuxCommonRewind(FileHandlePtr fileHandle)
{
    LinuxFileHandle *curFd = (LinuxFileHandle *)fileHandle;
    if (unlikely(curFd == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }

    curFd->accessCount += 1;
    if (unlikely(curFd->storageFd < 0)) {
        ErrorCode ret = TryReopenFileWithHandle(curFd);
        if (ret != 0) {
            return ret;
        }
    }
    int64_t resPos = lseek(curFd->storageFd, 0, SEEK_SET);
    if (unlikely(resPos < 0)) {
        return ConvertLinuxSysErrCode(errno);
    }
    curFd->currentMaxWriteOffset = 0;
    return ERROR_SYS_OK;
}

ErrorCode LinuxCommonRead(FileHandlePtr fileHandle, void *buf, uint64_t count, int64_t *readSize)
{
    LinuxFileHandle *curFd = (LinuxFileHandle *)fileHandle;
    if (unlikely(curFd == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }

    curFd->accessCount += 1;
    if (unlikely(curFd->storageFd < 0)) {
        ErrorCode ret = TryReopenFileWithHandle(curFd);
        if (ret != ERROR_SYS_OK) {
            VfsPrintReleaseLog("Read file failed because file cannot reopen");
            return ret;
        }
    }
    int64_t returnReadSize = read(curFd->storageFd, buf, count);
    if (unlikely(returnReadSize < 0)) {
        return ConvertLinuxSysErrCode(errno);
    }
    *readSize = returnReadSize;
    return ERROR_SYS_OK;
}

ErrorCode LinuxCommonPread(FileHandlePtr fileHandle, void *buf, uint64_t count, int64_t offset, int64_t *readSize)
{
    LinuxFileHandle *curFd = (LinuxFileHandle *)fileHandle;
    if (unlikely(curFd == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }

    curFd->accessCount += 1;
    if (unlikely(curFd->storageFd < 0)) {
        ErrorCode ret = TryReopenFileWithHandle(curFd);
        if (ret != ERROR_SYS_OK) {
            VfsPrintReleaseLog("Pread file failed because file cannot reopen");
            return ret;
        }
    }
    int64_t returnReadSize = pread(curFd->storageFd, buf, count, offset);
    if (unlikely(returnReadSize < 0)) {
        return ConvertLinuxSysErrCode(errno);
    }
    *readSize = returnReadSize;
    return ERROR_SYS_OK;
}

static int AioSubmitCommon(AioThreadContext *aioThreadContext, struct iocb **iocbs)
{
    uint16_t tryTimes = 0;
    int rc = 0;
    while (rc != 1) {
        if (tryTimes > MAX_TRY_TIMES) {
            VfsPrintLog("Try time reach the max.");
            break;
        }
        rc = io_submit(aioThreadContext->ioctx, 1, iocbs);
        tryTimes++;
    }

    return rc;
}

static ErrorCode AioSubmitRead(LinuxFileHandle *fileHandle, void **buf, uint64_t count, int64_t offset,
                               const AsyncIoContext *aioContext)
{
    AioThreadContext *aioThreadContext = fileHandle->vfsHandle->aioThreadContext;
    struct iocb *iocbs = &fileHandle->aioIocb[VFS_LINUX_READ_AIO_IOCB_INDEX].iocb;

    /* There should be only one callback function a file */
    fileHandle->aioIocb[VFS_LINUX_READ_AIO_IOCB_INDEX].asyncCallback = aioContext->callback;
    fileHandle->aioIocb[VFS_LINUX_READ_AIO_IOCB_INDEX].fd = fileHandle;

    io_prep_pread(iocbs, fileHandle->storageFd, *buf, count, offset);
    fileHandle->aioIocb[VFS_LINUX_READ_AIO_IOCB_INDEX].iocb.data = aioContext->asyncContext;

    int rc = AioSubmitCommon(aioThreadContext, &iocbs);
    if (rc != 1) {
        return VFS_ERROR_AIO_READ_SUBMIT_FAIL;
    }

    return ERROR_SYS_OK;
}

ErrorCode LinuxCommonPreadAsync(FileHandlePtr fileHandle, void *buf, uint64_t count, int64_t offset,
                                const AsyncIoContext *aioContext)
{
    LinuxFileHandle *curFd = (LinuxFileHandle *)fileHandle;
    if (unlikely(curFd == NULL || ((curFd->openFlag & O_DIRECT) == 0 && aioContext != NULL))) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }

    curFd->accessCount += 1;
    if (unlikely(curFd->storageFd < 0)) {
        ErrorCode ret = TryReopenFileWithHandle(curFd);
        if (ret != ERROR_SYS_OK) {
            VfsPrintReleaseLog("Pread file failed because file cannot reopen");
            return ret;
        }
    }

    if (aioContext != NULL) {
        GSDB_ATOMIC32_INC(&(curFd->asyncReqCount));
        ErrorCode errCode = AioSubmitRead(curFd, (void **)(void *)&buf, count, offset, aioContext);
        if (errCode != ERROR_SYS_OK) {
            return errCode;
        }
    } else {
        int64_t returnReadSize = pread(curFd->storageFd, buf, count, offset);
        if (unlikely(returnReadSize < 0)) {
            return ConvertLinuxSysErrCode(errno);
        }
    }

    return ERROR_SYS_OK;
}

ErrorCode LinuxCommonWriteSync(FileHandlePtr fileHandle, const void *buf, uint64_t count, int64_t *writeSize)
{
    LinuxFileHandle *curFd = (LinuxFileHandle *)fileHandle;
    if (unlikely(curFd == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }

    curFd->accessCount += 1;
    if (unlikely(curFd->storageFd < 0)) {
        ErrorCode ret = TryReopenFileWithHandle(curFd);
        if (ret != ERROR_SYS_OK) {
            VfsPrintReleaseLog("Write file failed because file cannot reopen");
            return ret;
        }
    }
    int64_t writeStartOffset = lseek(curFd->storageFd, 0, SEEK_CUR);
    int64_t returnWriteSize = write(curFd->storageFd, buf, count);
    if (unlikely(returnWriteSize < 0)) {
        return ConvertLinuxSysErrCode(errno);
    }
    if (unlikely(fsync(curFd->storageFd) != 0)) {
        return ConvertLinuxSysErrCode(errno);
    }
    int64_t writeEndOffset = GetWriteEndOffset(writeStartOffset, returnWriteSize);
    curFd->currentMaxWriteOffset = Max(curFd->currentMaxWriteOffset, writeEndOffset);
    *writeSize = returnWriteSize;
    return ERROR_SYS_OK;
}

ErrorCode LinuxCommonPwriteSync(FileHandlePtr fileHandle, const void *buf, uint64_t count, int64_t offset,
                                int64_t *writeSize)
{
    LinuxFileHandle *curFd = (LinuxFileHandle *)fileHandle;
    if (unlikely(curFd == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }

    curFd->accessCount += 1;
    if (unlikely(curFd->storageFd < 0)) {
        ErrorCode ret = TryReopenFileWithHandle(curFd);
        if (ret != ERROR_SYS_OK) {
            VfsPrintReleaseLog("Pwrite file failed because file cannot reopen");
            return ret;
        }
    }
    int64_t returnWriteSize = pwrite(curFd->storageFd, buf, count, offset);
    if (unlikely(returnWriteSize < 0)) {
        return ConvertLinuxSysErrCode(errno);
    }
    if (unlikely(fsync(curFd->storageFd) != 0)) {
        return ConvertLinuxSysErrCode(errno);
    }
    int64_t writeEndOffset = GetWriteEndOffset(offset, returnWriteSize);
    curFd->currentMaxWriteOffset = Max(curFd->currentMaxWriteOffset, writeEndOffset);
    *writeSize = returnWriteSize;
    return ERROR_SYS_OK;
}

ErrorCode LinuxCommonWriteAsync(FileHandlePtr fileHandle, const void *buf, uint64_t count,
                                const AsyncIoContext *aioContext)
{
    if (aioContext != NULL) {
        return VFS_ERROR_OPERATION_NOT_SUPPORT;
    }
    LinuxFileHandle *curFd = (LinuxFileHandle *)fileHandle;
    if (unlikely(curFd == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    curFd->accessCount += 1;
    if (unlikely(curFd->storageFd < 0)) {
        if (TryReopenFileWithHandle(curFd) != 0) {
            return VFS_ERROR_REOPEN_FILE_FAIL;
        }
    }
    int64_t writeStartOffset = lseek(curFd->storageFd, 0, SEEK_CUR);
    int64_t writeSize = write(curFd->storageFd, buf, count);
    if (unlikely(writeSize < 0)) {
        return ConvertLinuxSysErrCode(errno); /* Cannot push forward max write offset */
    }
    int64_t writeEndOffset = GetWriteEndOffset(writeStartOffset, writeSize);
    curFd->currentMaxWriteOffset = Max(curFd->currentMaxWriteOffset, writeEndOffset);
    return ERROR_SYS_OK;
}

static ErrorCode AioSubmitWrite(LinuxFileHandle *fileHandle, void **buf, uint64_t count, int64_t offset,
                                const AsyncIoContext *aioContext)
{
    AioThreadContext *aioThreadContext = fileHandle->vfsHandle->aioThreadContext;
    struct iocb *iocbs = &fileHandle->aioIocb[VFS_LINUX_WRITE_AIO_IOCB_INDEX].iocb;

    /* There should be only one callback function a file */
    fileHandle->aioIocb[VFS_LINUX_WRITE_AIO_IOCB_INDEX].asyncCallback = aioContext->callback;
    fileHandle->aioIocb[VFS_LINUX_WRITE_AIO_IOCB_INDEX].fd = fileHandle;

    io_prep_pwrite(iocbs, fileHandle->storageFd, *buf, count, offset);
    fileHandle->aioIocb[VFS_LINUX_WRITE_AIO_IOCB_INDEX].iocb.data = aioContext->asyncContext;

    int rc = AioSubmitCommon(aioThreadContext, &iocbs);
    if (rc != 1) {
        return VFS_ERROR_AIO_WRITE_SUBMIT_FAIL;
    }

    return ERROR_SYS_OK;
}

ErrorCode LinuxCommonPwriteAsync(FileHandlePtr fileHandle, const void *buf, uint64_t count, int64_t offset,
                                 const AsyncIoContext *aioContext)
{
    LinuxFileHandle *curFd = (LinuxFileHandle *)fileHandle;
    if (unlikely(curFd == NULL || ((curFd->openFlag & O_DIRECT) == 0 && aioContext != NULL))) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }

    curFd->accessCount += 1;
    if (unlikely(curFd->storageFd < 0)) {
        if (TryReopenFileWithHandle(curFd) != 0) {
            return VFS_ERROR_REOPEN_FILE_FAIL;
        }
    }

    int64_t writeSize = 0;
    if (aioContext != NULL) {
        GSDB_ATOMIC32_INC(&(curFd->asyncReqCount));
        ErrorCode errCode = AioSubmitWrite(curFd, (void **)(void *)&buf, count, offset, aioContext);
        if (errCode != ERROR_SYS_OK) {
            return errCode;
        }
        writeSize = (int64_t)count;
    } else {
        writeSize = pwrite(curFd->storageFd, buf, count, offset);
        if (unlikely(writeSize < 0)) {
            return ConvertLinuxSysErrCode(errno); /* Cannot push forward max write offset */
        }
    }

    int64_t writeEndOffset = GetWriteEndOffset(offset, writeSize);
    curFd->currentMaxWriteOffset = Max(curFd->currentMaxWriteOffset, writeEndOffset);

    return ERROR_SYS_OK;
}

ErrorCode LinuxCommonExtend(FileHandlePtr fileHandle, int64_t length)
{
    LinuxFileHandle *curFd = (LinuxFileHandle *)fileHandle;
    if (unlikely(curFd == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }

    curFd->accessCount += 1;
    struct stat fileStat;
    if (unlikely(stat(curFd->fileName, &fileStat) != 0)) {
        return ConvertLinuxSysErrCode(errno);
    }
    int64_t currentFileSize = fileStat.st_size;
    if (unlikely(length < currentFileSize)) {
        VfsPrintReleaseLog("Extend file failed, current size (%ld), expected size (%ld)", currentFileSize, length);
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (unlikely(truncate(curFd->fileName, length) != 0)) {
        return ConvertLinuxSysErrCode(errno);
    }
    return ERROR_SYS_OK;
}

ErrorCode LinuxCommonTruncate(FileHandlePtr fileHandle, int64_t length)
{
    LinuxFileHandle *curFd = (LinuxFileHandle *)fileHandle;
    if (unlikely(curFd == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }

    curFd->accessCount += 1;
    struct stat fileStat;
    if (unlikely(stat(curFd->fileName, &fileStat) != 0)) {
        return ConvertLinuxSysErrCode(errno);
    }
    int64_t currentFileSize = fileStat.st_size;
    if (unlikely(length > currentFileSize)) {
        VfsPrintReleaseLog("Truncate file failed, current size (%ld), expected size (%ld)", currentFileSize, length);
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (unlikely(truncate(curFd->fileName, length) != 0)) {
        return ConvertLinuxSysErrCode(errno);
    }
    curFd->currentMaxWriteOffset = Min(curFd->currentMaxWriteOffset, length);
    return ERROR_SYS_OK;
}

ErrorCode LinuxCommonGetSize(FileHandlePtr fileHandle, int64_t *fileSize)
{
    LinuxFileHandle *curFd = (LinuxFileHandle *)fileHandle;
    if (unlikely(curFd == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }

    curFd->accessCount += 1;
    struct stat fileStat;
    if (unlikely(stat(curFd->fileName, &fileStat) != 0)) {
        return ConvertLinuxSysErrCode(errno);
    }
    *fileSize = fileStat.st_size;
    return ERROR_SYS_OK;
}

ErrorCode LinuxCommonFileControl(FileHandlePtr fileHandle, int cmd, const AdapterFileControlInfo *controlInfo)
{
    LinuxFileHandle *curFd = (LinuxFileHandle *)fileHandle;
    if (unlikely(curFd == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }

    curFd->accessCount += 1;
    switch (cmd) {
        case SET_FILE_FLUSH_CALLBACK: {
            curFd->flushCallback = controlInfo->flushCallbackInfo.callback;
            curFd->flushAsyncContext = controlInfo->flushCallbackInfo.asyncContext;
            curFd->upperFd = controlInfo->flushCallbackInfo.fd;
            return ERROR_SYS_OK;
        }
        case SET_FILE_ZCOPY_MEMORY_KEY: {
            /* Local file system does not need zero copy memory key */
            (void)controlInfo->zeroCopyMemKey.memKey;
            return ERROR_SYS_OK;
        }
        default:
            VfsPrintReleaseLog("File control with invalid cmd");
            return VFS_ERROR_PARAMETERS_INVALID;
    }
}

static int TryLockFileWithTimeout(int fd, const struct flock *lockInfo, uint32_t timeout)
{
    int res = fcntl(fd, F_SETLK, lockInfo);
    if (res != 0) {
        /* repeat several times according to timeout */
        for (uint32_t i = 0; i < timeout; ++i) {
            (void)usleep(US_PER_MS);
            res = fcntl(fd, F_SETLK, lockInfo);
            if (res == 0) {
                break;
            }
        }
    }
    return res;
}

ErrorCode LinuxCommonLockFile(FileHandlePtr fileHandle, int64_t startPos, int64_t len, int lockMode, uint32_t timeout)
{
    LinuxFileHandle *curFd = (LinuxFileHandle *)fileHandle;
    if (unlikely(curFd == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }

    curFd->accessCount += 1;
    short lockType = GetLinuxLockType(lockMode);
    if (unlikely(lockType == INVALID_LINUX_LOCK_TYPE)) {
        VfsPrintReleaseLog("Lock file failed, lock mode does not support");
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (unlikely(curFd->storageFd < 0)) {
        ErrorCode ret = TryReopenFileWithHandle(curFd);
        if (ret != ERROR_SYS_OK) {
            VfsPrintReleaseLog("Lock file failed because file cannot reopen");
            return ret;
        }
    }
    struct flock fileLockInfo;
    fileLockInfo.l_type = lockType;
    fileLockInfo.l_whence = SEEK_SET;
    fileLockInfo.l_start = startPos;
    fileLockInfo.l_len = len;
    int res;
    if (timeout > 0) {
        res = TryLockFileWithTimeout(curFd->storageFd, &fileLockInfo, timeout);
    } else {
        res = fcntl(curFd->storageFd, F_SETLKW, &fileLockInfo);
    }
    if (res != 0) {
        return VFS_ERROR_LOCK_FILE_FAIL;
    }
    GSDB_ATOMIC32_INC(&(curFd->lockCount));
    return ERROR_SYS_OK;
}

ErrorCode LinuxCommonTryLockFile(FileHandlePtr fileHandle, int64_t startPos, int64_t len, int lockMode)
{
    LinuxFileHandle *curFd = (LinuxFileHandle *)fileHandle;
    if (unlikely(curFd == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }

    curFd->accessCount += 1;
    short lockType = GetLinuxLockType(lockMode);
    if (unlikely(lockType == INVALID_LINUX_LOCK_TYPE)) {
        VfsPrintReleaseLog("Try lock file failed, lock mode does not support");
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (unlikely(curFd->storageFd < 0)) {
        ErrorCode ret = TryReopenFileWithHandle(curFd);
        if (ret != ERROR_SYS_OK) {
            VfsPrintReleaseLog("Try lock file failed because file cannot reopen");
            return ret;
        }
    }
    struct flock fileLockInfo;
    fileLockInfo.l_type = lockType;
    fileLockInfo.l_whence = SEEK_SET;
    fileLockInfo.l_start = startPos;
    fileLockInfo.l_len = len;
    int res = fcntl(curFd->storageFd, F_SETLK, &fileLockInfo);
    if (res != 0) {
        return VFS_ERROR_LOCK_FILE_FAIL;
    }
    GSDB_ATOMIC32_INC(&(curFd->lockCount));
    return ERROR_SYS_OK;
}

ErrorCode LinuxCommonUnlockFile(FileHandlePtr fileHandle, int64_t startPos, int64_t len)
{
    LinuxFileHandle *curFd = (LinuxFileHandle *)fileHandle;
    if (unlikely(curFd == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }

    curFd->accessCount += 1;
    struct flock fileLockInfo;
    fileLockInfo.l_type = F_UNLCK;
    fileLockInfo.l_whence = SEEK_SET;
    fileLockInfo.l_start = startPos;
    fileLockInfo.l_len = len;
    if (unlikely(fcntl(curFd->storageFd, F_SETLK, &fileLockInfo) != 0)) {
        return VFS_ERROR_UNLOCK_FILE_FAIL;
    }
    return ERROR_SYS_OK;
}

ErrorCode LinuxCommonFDataSync(FileHandlePtr fileHandle)
{
    LinuxFileHandle *curFd = (LinuxFileHandle *)fileHandle;
    if (unlikely(curFd == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }

    curFd->accessCount += 1;
    if (unlikely(curFd->storageFd < 0)) {
        ErrorCode ret = TryReopenFileWithHandle(curFd);
        if (ret != ERROR_SYS_OK) {
            VfsPrintReleaseLog("FDataSync failed because file cannot reopen");
            return ret;
        }
    }
    if (unlikely(fdatasync(curFd->storageFd) != 0)) {
        return ConvertLinuxSysErrCode(errno);
    }
    /* Write callback function */
    if (curFd->flushCallback != NULL) {
        /* Get current write max offset */
        curFd->flushCallback(curFd->upperFd, curFd->currentMaxWriteOffset, 0, curFd->flushAsyncContext);
    }
    return ERROR_SYS_OK;
}

ErrorCode LinuxCommonStartAIO(VfsHandlePtr vfs, uint16_t maxEvents, uint16_t threadCount,
                              void (*threadEnterCallback)(void), void (*threadExitCallback)(void))
{
    if (maxEvents < MIN_AIO_EVENT_NUM || threadCount > MAX_AIO_THREAD_NUM || threadCount < 1) {
        return VFS_ERROR_INVALID_ARGUMENT;
    }

    LinuxVfsHandle *curVfsHandle = (LinuxVfsHandle *)vfs;
    if (unlikely(curVfsHandle == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }

    return StartAIO(maxEvents, threadCount, &curVfsHandle->aioThreadContext, threadEnterCallback, threadExitCallback);
}

int ForceRemoveDir(const char *path)
{
    DIR *dir = opendir(path);
    if (dir == NULL) {
        return -1;
    }

    struct dirent *p;
    while ((p = readdir(dir)) != NULL) {
        if (!strcmp(p->d_name, ".") || !strcmp(p->d_name, "..")) {
            continue;
        }

#define SUBPATH_PADDING_LEN 2
        size_t subpathsize = strlen(path) + strlen(p->d_name) + SUBPATH_PADDING_LEN;
        char *subpath = malloc(subpathsize);
        if (unlikely(subpath == NULL)) {
            continue;
        }

        if (sprintf_s(subpath, subpathsize, "%s/%s", path, p->d_name) == -1) {
            free(subpath);
            continue;
        }

        struct stat stbuf;
        if (stat(subpath, &stbuf) == -1) {
            free(subpath);
            continue;
        }

        if (S_ISDIR(stbuf.st_mode)) {
            ForceRemoveDir(subpath);
        } else {
            unlink(subpath);
        }
        free(subpath);
    }
    closedir(dir);

    return rmdir(path);
}