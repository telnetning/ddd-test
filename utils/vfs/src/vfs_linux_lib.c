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
 * vfs_linux_lib.c
 *
 * Description:
 * linux vfs adapter dynamic library source
 *
 * ---------------------------------------------------------------------------------
 */
#include "fault_injection/fault_injection.h"
#include "vfs/vfs_linux_common.h"
#include "vfs_linux_lib.h"

#define LINUX_VFS_LIB_MAGIC   0x5fa9acb914dc5e45
#define LINUX_VFS_LIB_MODE    0x01
#define LINUX_VFS_LIB_VERSION 0x01

static MemAllocator g_linuxAdapterMemAllocator;
static MemAllocator *g_linuxAdapterMemAllocatorPtr = NULL;
static LinuxVfsAdapter g_adapterInstance;
static LinuxVfsAdapter *g_adapterPtr = NULL;

static ErrorCode GenerateVfsDirectory(const char *rootDataDir, const char *vfsName, char *fullPath, size_t pathMaxLen)
{
    if (unlikely(sprintf_s(fullPath, pathMaxLen, "%s/%s/", rootDataDir, vfsName) < 0)) {
        return VFS_ERROR_SECURE_FUNCTION_FAIL;
    }
    return ERROR_SYS_OK;
}

static bool CheckRootDataDirValid(const char *rootDataDir)
{
    if (unlikely(access(rootDataDir, F_OK) != 0)) {
        return false;
    }
    struct stat dirStat;
    if (unlikely(stat(rootDataDir, &dirStat) != 0)) {
        return false;
    }
    if (S_ISDIR(dirStat.st_mode)) {
        return true;
    }
    return false;
}

/* Adapter interface implementation */
ErrorCode LinuxAdapterSetConfig(const char *para, const char *value)
{
    (void)para;
    (void)value;
    return ERROR_SYS_OK;
}

ErrorCode LinuxAdapterGetConfig(const char *para, char *value, uint64_t len)
{
    (void)para;
    (void)value;
    (void)len;
    return ERROR_SYS_OK;
}

ErrorCode LinuxAdapterInit(const VfsAdapterParam *param)
{
    if (unlikely(g_adapterPtr != NULL)) {
        return VFS_ERROR_LOCAL_ADAPTER_ALREADY_INIT;
    }
    if (unlikely(!CheckRootDataDirValid(param->storageServerAddr))) {
        VfsPrintReleaseLog("Root data directory (%s) is not valid", param->storageServerAddr);
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    size_t rootDataDirLen = strlen(param->storageServerAddr) + 1;
    g_adapterInstance.rootDataDir = (char *)VfsMemAlloc(g_linuxAdapterMemAllocatorPtr, rootDataDirLen);
    if (g_adapterInstance.rootDataDir == NULL) {
        VfsPrintReleaseLog("Linux adapter Init out of memory!");
        return VFS_ERROR_OUT_OF_MEMORY;
    }
    if (strcpy_s(g_adapterInstance.rootDataDir, rootDataDirLen, param->storageServerAddr) != EOK) {
        VfsPrintReleaseLog("Linux adapter Init strcpy_s error!");
        VfsMemFree(g_linuxAdapterMemAllocatorPtr, g_adapterInstance.rootDataDir);
        g_adapterInstance.rootDataDir = NULL;
        return VFS_ERROR_SECURE_FUNCTION_FAIL;
    }
    g_adapterInstance.pageSize = param->pageSize;
    g_adapterInstance.dbType = param->dbType;
    GSDB_ATOMIC32_SET(&(g_adapterInstance.mountVfsCount), 0);
    g_adapterPtr = &g_adapterInstance;
    return ERROR_SYS_OK;
}

ErrorCode LinuxAdapterUpdateParam(VfsClientHandlePtr vfsClientHandle, const VfsAdapterParam *param)
{
    if (unlikely(g_adapterPtr == NULL)) {
        return VFS_ERROR_LOCAL_ADAPTER_NOT_INIT;
    }
    (void)vfsClientHandle;
    g_adapterPtr->pageSize = param->pageSize;
    g_adapterPtr->dbType = param->dbType;
    return ERROR_SYS_OK;
}

ErrorCode LinuxAdapterGetClientId(VfsClientHandlePtr vfsClientHandle, uint64_t *adapterClientId)
{
    (void)adapterClientId;
    (void)vfsClientHandle;
    return ERROR_SYS_OK;
}

void LinuxAdapterRemoteMsgHandler(VfsClientHandlePtr vfsClientHandle, void *msgRequest, void *userContext)
{
    (void)vfsClientHandle;
    (void)msgRequest;
    (void)userContext;
}

ErrorCode LinuxAdapterExit(void)
{
    if (g_adapterPtr != NULL) {
        if (GSDB_ATOMIC32_GET(&(g_adapterPtr->mountVfsCount)) > 0) {
            return VFS_ERROR_VFS_RESOURCE_NOT_RELEASE;
        }
        VfsMemFree(g_linuxAdapterMemAllocatorPtr, g_adapterPtr->rootDataDir);
        g_adapterPtr->rootDataDir = NULL;
        g_adapterPtr = NULL;
        g_linuxAdapterMemAllocatorPtr = NULL;
    }
    return ERROR_SYS_OK;
}

ErrorCode LinuxAdapterCreateVfsClientHandle(const char *clusterName, uint32_t authType, const char *storageServerAddr,
    VfsClientHandlePtr *vfsClientHandle)
{
    if (unlikely(g_adapterPtr == NULL)) {
        return VFS_ERROR_LOCAL_ADAPTER_NOT_INIT;
    }

    if (unlikely(clusterName == NULL || storageServerAddr == NULL || vfsClientHandle == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }

    (void)authType;
    LinuxClientHandle *newVfsClientHandle =
        (LinuxClientHandle *)VfsMemAlloc(g_linuxAdapterMemAllocatorPtr, sizeof(LinuxClientHandle));
    if (unlikely(newVfsClientHandle == NULL)) {
        return VFS_ERROR_OUT_OF_MEMORY;
    }

    GSDB_ATOMIC32_SET(&(newVfsClientHandle->mountVfsCount), 0);
    *vfsClientHandle = newVfsClientHandle;
    return ERROR_SYS_OK;
}

ErrorCode LinuxAdapterDeleteVfsClientHandle(VfsClientHandlePtr vfsClientHandle)
{
    if (unlikely(g_adapterPtr == NULL)) {
        return VFS_ERROR_LOCAL_ADAPTER_NOT_INIT;
    }

    if (unlikely(vfsClientHandle == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }

    LinuxClientHandle *curVfsClientHandle = (LinuxClientHandle *)vfsClientHandle;
    if (unlikely(GSDB_ATOMIC32_GET(&(curVfsClientHandle->mountVfsCount)) > 0)) {
        return VFS_ERROR_VFS_RESOURCE_NOT_RELEASE;
    }

    VfsMemFree(g_linuxAdapterMemAllocatorPtr, curVfsClientHandle);
    return ERROR_SYS_OK;
}

ErrorCode LinuxAdapterSetClientInstanceConfig(VfsClientHandlePtr vfsClientHandle, const char *para,
    const char *value)
{
    if (unlikely(g_adapterPtr == NULL)) {
        return VFS_ERROR_LOCAL_ADAPTER_NOT_INIT;
    }

    if (unlikely(vfsClientHandle == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }

    (void)para;
    (void)value;
    return ERROR_SYS_OK;
}

ErrorCode LinuxAdapterGetClientInstanceConfig(VfsClientHandlePtr vfsClientHandle, const char *para,
    char *value, uint64_t len)
{
    if (unlikely(g_adapterPtr == NULL)) {
        return VFS_ERROR_LOCAL_ADAPTER_NOT_INIT;
    }

    if (unlikely(vfsClientHandle == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }

    (void)para;
    (void)value;
    (void)len;
    return ERROR_SYS_OK;
}

ErrorCode LinuxAdapterCreateTenant(VfsClientHandlePtr vfsClientHandle, const char *clusterName,
                                   const char *tenantName)
{
    if (unlikely(g_adapterPtr == NULL)) {
        return VFS_ERROR_LOCAL_ADAPTER_NOT_INIT;
    }
    if (unlikely(!CheckTenantClusterNameLen(tenantName, clusterName))) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    (void)vfsClientHandle;
    return ERROR_SYS_OK;
}

ErrorCode LinuxAdapterDeleteTenant(VfsClientHandlePtr vfsClientHandle, const char *clusterName,
                                   const char *tenantName, SYMBOL_UNUSED uint64_t attr)
{
    if (unlikely(g_adapterPtr == NULL)) {
        return VFS_ERROR_LOCAL_ADAPTER_NOT_INIT;
    }
    if (unlikely(!CheckTenantClusterNameLen(tenantName, clusterName))) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    (void)vfsClientHandle;
    return ERROR_SYS_OK;
}

ErrorCode LinuxAdapterCreateVfs(VfsClientHandlePtr vfsClientHandle, const char *tenantName,
                                const char *vfsName, SYMBOL_UNUSED uint64_t attr)
{
    if (unlikely(g_adapterPtr == NULL)) {
        return VFS_ERROR_LOCAL_ADAPTER_NOT_INIT;
    }
    if (unlikely(!CheckTenantVfsNameLen(tenantName, vfsName))) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    char vfsDataDir[LINUX_PATH_MAX_LEN];
    ErrorCode ret = GenerateVfsDirectory(g_adapterPtr->rootDataDir, vfsName, vfsDataDir, sizeof(vfsDataDir));
    if (unlikely(ret != ERROR_SYS_OK)) {
        return ret;
    }
    const char *vfsDataDirStr = vfsDataDir;
    if (unlikely(access(vfsDataDirStr, F_OK) == 0)) {
        /* directory related to this vfs name already exist */
        return VFS_ERROR_CREATE_VFS_NAME_EXIST;
    }
    if (unlikely(mkdir(vfsDataDirStr, S_IRWXU) != 0)) {
        return VFS_ERROR_LOCAL_FILE_OPERATION_FAILED;
    }
    (void)vfsClientHandle;
    return ERROR_SYS_OK;
}

ErrorCode LinuxAdapterDropVfs(VfsClientHandlePtr vfsClientHandle, const char *tenantName, const char *vfsName,
                              SYMBOL_UNUSED uint64_t attr)
{
    if (unlikely(g_adapterPtr == NULL)) {
        return VFS_ERROR_LOCAL_ADAPTER_NOT_INIT;
    }
    if (unlikely(!CheckTenantVfsNameLen(tenantName, vfsName))) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    /* Check directory related to "vfsName" */
    char vfsDataDir[LINUX_PATH_MAX_LEN];
    ErrorCode ret = GenerateVfsDirectory(g_adapterPtr->rootDataDir, vfsName, vfsDataDir, sizeof(vfsDataDir));
    if (unlikely(ret != ERROR_SYS_OK)) {
        return ret;
    }
    const char *vfsDataDirStr = vfsDataDir;
    if (unlikely(access(vfsDataDirStr, F_OK) != 0)) {
        return VFS_ERROR_VFS_NAME_NOT_EXIST;
    }
    int rc = 0;
    if (attr == VFS_FORCE_DELETE_FLAG) {
        rc = ForceRemoveDir(vfsDataDirStr);
    } else {
        rc = rmdir(vfsDataDirStr);
    }
    if (unlikely(rc != 0)) {
        return VFS_ERROR_LOCAL_FILE_OPERATION_FAILED;
    }
    (void)vfsClientHandle;
    return ERROR_SYS_OK;
}

ErrorCode LinuxAdapterMountVfs(VfsClientHandlePtr vfsClientHandle, const char *tenantName,
                               const char *vfsName, VfsHandlePtr *vfs)
{
    if (unlikely(g_adapterPtr == NULL)) {
        return VFS_ERROR_LOCAL_ADAPTER_NOT_INIT;
    }
    if (unlikely(!CheckTenantVfsNameLen(tenantName, vfsName))) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }

    LinuxVfsHandle *newVfs = (LinuxVfsHandle *)VfsMemAlloc(g_linuxAdapterMemAllocatorPtr, sizeof(LinuxVfsHandle));
    if (unlikely(newVfs == NULL)) {
        return VFS_ERROR_OUT_OF_MEMORY;
    }
    InitVfsHandle(newVfs, false);
    newVfs->rootDataDir = g_adapterPtr->rootDataDir;
    ErrorCode ret = GenerateVfsDirectory(g_adapterPtr->rootDataDir, vfsName, newVfs->vfsDir, sizeof(newVfs->vfsDir));
    if (unlikely(ret != ERROR_SYS_OK)) {
        VfsMemFree(g_linuxAdapterMemAllocatorPtr, newVfs);
        return ret;
    }
    if (unlikely(access(newVfs->vfsDir, F_OK) != 0)) {
        VfsMemFree(g_linuxAdapterMemAllocatorPtr, newVfs);
        return VFS_ERROR_VFS_NAME_NOT_EXIST;
    }
    /* vfsDir only save vfsName as vfs logic relative path directory,
     * need splice rootDataDir to absolute path before pass to OS API */
    ret = GenerateVfsDirectory("", vfsName, newVfs->vfsDir, sizeof(newVfs->vfsDir));
    if (unlikely(ret != ERROR_SYS_OK)) {
        VfsMemFree(g_linuxAdapterMemAllocatorPtr, newVfs);
        return ret;
    }
    GSDB_ATOMIC32_INC(&(g_adapterPtr->mountVfsCount));

    LinuxClientHandle *curVfsClientHandle = (LinuxClientHandle *)vfsClientHandle;
    GSDB_ATOMIC32_INC(&(curVfsClientHandle->mountVfsCount));
    newVfs->vfsClientHandle = curVfsClientHandle;
    *vfs = newVfs;
    return ERROR_SYS_OK;
}

ErrorCode LinuxAdapterUnmountVfs(VfsHandlePtr vfs)
{
    if (unlikely(g_adapterPtr == NULL)) {
        return VFS_ERROR_LOCAL_ADAPTER_NOT_INIT;
    }
    if (unlikely(vfs == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    LinuxVfsHandle *curVfsHandle = (LinuxVfsHandle *)vfs;
    if (unlikely(GSDB_ATOMIC32_GET(&(curVfsHandle->openFileCount)) > 0)) {
        return VFS_ERROR_VFS_RESOURCE_NOT_RELEASE;
    }
    if (curVfsHandle->aioThreadContext != NULL) {
        StopAIO(curVfsHandle->aioThreadContext);
        curVfsHandle->aioThreadContext = NULL;
    }

    LinuxClientHandle *curVfsClientHandle = (LinuxClientHandle *)curVfsHandle->vfsClientHandle;
    GSDB_ATOMIC32_DEC(&(curVfsClientHandle->mountVfsCount));
    VfsMemFree(g_linuxAdapterMemAllocatorPtr, vfs);
    GSDB_ATOMIC32_DEC(&(g_adapterPtr->mountVfsCount));
    return ERROR_SYS_OK;
}

ErrorCode LinuxAdapterVfsControl(VfsClientHandlePtr vfsClientHandle, SYMBOL_UNUSED const char *tenantName,
                                 SYMBOL_UNUSED int cmd, SYMBOL_UNUSED VfsControlInfo *vfsControlInfo,
                                 SYMBOL_UNUSED uint32_t count)
{
    if (unlikely(g_adapterPtr == NULL)) {
        return VFS_ERROR_LOCAL_ADAPTER_NOT_INIT;
    }
    (void)vfsClientHandle;
    return ERROR_SYS_OK;
}

ErrorCode LinuxAdapterCreateStoreSpace(VfsClientHandlePtr vfsClientHandle, const char *tenantName,
                                       const char **storeSpaceNames, const StoreSpaceAttr *attrs, uint32 count)
{
    if (unlikely(g_adapterPtr == NULL)) {
        return VFS_ERROR_LOCAL_ADAPTER_NOT_INIT;
    }
    (void)vfsClientHandle;
    (void)tenantName;
    (void)storeSpaceNames;
    (void)attrs;
    (void)count;

    return ERROR_SYS_OK;
}

ErrorCode LinuxAdapterDeleteStoreSpace(VfsClientHandlePtr vfsClientHandle, const char *tenantName,
                                       const char **storeSpaceNames, uint32 count)
{
    if (unlikely(g_adapterPtr == NULL)) {
        return VFS_ERROR_LOCAL_ADAPTER_NOT_INIT;
    }
    (void)vfsClientHandle;
    (void)tenantName;
    (void)storeSpaceNames;
    (void)count;
    return ERROR_SYS_OK;
}

ErrorCode LinuxAdapterUpdateStoreSpace(VfsClientHandlePtr vfsClientHandle, const char *tenantName,
                                       const char **storeSpaceNames, const StoreSpaceAttr *attrs, uint32 count)
{
    if (unlikely(g_adapterPtr == NULL)) {
        return VFS_ERROR_LOCAL_ADAPTER_NOT_INIT;
    }
    (void)vfsClientHandle;
    (void)tenantName;
    (void)storeSpaceNames;
    (void)attrs;
    (void)count;
    return ERROR_SYS_OK;
}

ErrorCode LinuxAdapterQueryStoreSpaceAttr(VfsClientHandlePtr vfsClientHandle, const char *tenantName,
                                          const char **storeSpaceNames, StoreSpaceAttr *storeSpaceAttrs, uint32 count)
{
    if (unlikely(g_adapterPtr == NULL)) {
        return VFS_ERROR_LOCAL_ADAPTER_NOT_INIT;
    }
    (void)vfsClientHandle;
    (void)tenantName;
    (void)storeSpaceNames;
    (void)storeSpaceAttrs;
    (void)count;
    return ERROR_SYS_OK;
}

ErrorCode LinuxAdapterQueryStoreSpaceUsedSize(VfsClientHandlePtr vfsClientHandle, const char *tenantName,
                                              const char **storeSpaceNames, uint64 *usedSizes, uint32 count)
{
    if (unlikely(g_adapterPtr == NULL)) {
        return VFS_ERROR_LOCAL_ADAPTER_NOT_INIT;
    }
    (void)vfsClientHandle;
    (void)tenantName;
    (void)storeSpaceNames;
    (void)usedSizes;
    (void)count;
    return ERROR_SYS_OK;
}

ErrorCode LinuxAdapterCreateSnapshot(VfsClientHandlePtr vfsClientHandle, const char *tenantName,
                                     const char *vfsName, const char *snapshotName, int64_t flags)
{
    if (unlikely(g_adapterPtr == NULL)) {
        return VFS_ERROR_LOCAL_ADAPTER_NOT_INIT;
    }
    if (unlikely(!CheckTenantVfsNameLen(tenantName, vfsName))) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (unlikely(!CheckSnapshotNameLen(snapshotName))) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (unlikely(!CheckSnapshotFlag(flags))) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    (void)vfsClientHandle;
    return ERROR_SYS_OK;
}

ErrorCode LinuxAdapterDropSnapshot(VfsClientHandlePtr vfsClientHandle, const char *tenantName,
                                   const char *vfsName, const char *snapshotName)
{
    if (unlikely(g_adapterPtr == NULL)) {
        return VFS_ERROR_LOCAL_ADAPTER_NOT_INIT;
    }
    if (unlikely(!CheckTenantVfsNameLen(tenantName, vfsName))) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (unlikely(!CheckSnapshotNameLen(snapshotName))) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    (void)vfsClientHandle;
    return ERROR_SYS_OK;
}

ErrorCode LinuxAdapterRollbackSnapshot(VfsClientHandlePtr vfsClientHandle, const char *tenantName,
                                       const char *vfsName, const char *snapshotName)
{
    if (unlikely(g_adapterPtr == NULL)) {
        return VFS_ERROR_LOCAL_ADAPTER_NOT_INIT;
    }
    if (unlikely(!CheckTenantVfsNameLen(tenantName, vfsName))) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (unlikely(!CheckSnapshotNameLen(snapshotName))) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    (void)vfsClientHandle;
    return ERROR_SYS_OK;
}

ErrorCode LinuxAdapterCreateFile(VfsHandlePtr vfs, const char *pathName, const FileParam *para,
                                 FileHandlePtr *fileHandle)
{
    if (unlikely(g_adapterPtr == NULL)) {
        return VFS_ERROR_LOCAL_ADAPTER_NOT_INIT;
    }
    return LinuxCommonCreate(vfs, pathName, para, fileHandle, g_linuxAdapterMemAllocatorPtr);
}

ErrorCode LinuxAdapterRemoveFile(VfsHandlePtr vfs, const char *pathName)
{
    if (unlikely(g_adapterPtr == NULL)) {
        return VFS_ERROR_LOCAL_ADAPTER_NOT_INIT;
    }
    return LinuxCommonRemove(vfs, pathName);
}

ErrorCode LinuxAdapterFileIsExist(VfsHandlePtr vfs, const char *pathName, bool *out)
{
    if (unlikely(g_adapterPtr == NULL)) {
        return VFS_ERROR_LOCAL_ADAPTER_NOT_INIT;
    }
    return LinuxCommonFileIsExist(vfs, pathName, out);
}

ErrorCode LinuxAdapterOpenFile(VfsHandlePtr vfs, const char *pathName, int flags, FileHandlePtr *fileHandle)
{
    if (unlikely(g_adapterPtr == NULL)) {
        return VFS_ERROR_LOCAL_ADAPTER_NOT_INIT;
    }
    return LinuxCommonOpen(vfs, pathName, flags, fileHandle, g_linuxAdapterMemAllocatorPtr);
}

ErrorCode LinuxAdapterOpenFileSnapshot(VfsHandlePtr vfs, FileOpenParam *openPara, FileHandlePtr *fileHandle)
{
    if (unlikely(g_adapterPtr == NULL)) {
        return VFS_ERROR_LOCAL_ADAPTER_NOT_INIT;
    }
    return LinuxCommonOpen(vfs, openPara->filePath, openPara->flags, fileHandle, g_linuxAdapterMemAllocatorPtr);
}

ErrorCode LinuxAdapterRenameFile(VfsHandlePtr vfs, const char *srcPathName, const char *destPathName)
{
    if (unlikely(g_adapterPtr == NULL)) {
        return VFS_ERROR_LOCAL_ADAPTER_NOT_INIT;
    }
    return LinuxCommonRenameFile(vfs, srcPathName, destPathName);
}

ErrorCode LinuxAdapterCloseFile(FileHandlePtr fileHandle)
{
    if (unlikely(g_adapterPtr == NULL)) {
        return VFS_ERROR_LOCAL_ADAPTER_NOT_INIT;
    }
    return LinuxCommonClose(fileHandle, g_linuxAdapterMemAllocatorPtr);
}

ErrorCode LinuxAdapterFsync(FileHandlePtr fileHandle)
{
    if (unlikely(g_adapterPtr == NULL)) {
        return VFS_ERROR_LOCAL_ADAPTER_NOT_INIT;
    }
    return LinuxCommonFsync(fileHandle);
}

ErrorCode LinuxAdapterFileSeek(FileHandlePtr fileHandle, int64_t offset, int seekFlag, int64_t *newPos)
{
    if (unlikely(g_adapterPtr == NULL)) {
        return VFS_ERROR_LOCAL_ADAPTER_NOT_INIT;
    }
    return LinuxCommonFileSeek(fileHandle, offset, seekFlag, newPos);
}

ErrorCode LinuxAdapterRewind(FileHandlePtr fileHandle)
{
    if (unlikely(g_adapterPtr == NULL)) {
        return VFS_ERROR_LOCAL_ADAPTER_NOT_INIT;
    }
    return LinuxCommonRewind(fileHandle);
}

ErrorCode LinuxAdapterRead(FileHandlePtr fileHandle, void *buf, uint64_t count, int64_t *readSize)
{
    if (unlikely(g_adapterPtr == NULL)) {
        return VFS_ERROR_LOCAL_ADAPTER_NOT_INIT;
    }
    return LinuxCommonRead(fileHandle, buf, count, readSize);
}

ErrorCode LinuxAdapterSnapshotPread(FileHandlePtr fileHandle, void *buf, uint64_t count, int64_t offset,
                                    DiffContents *diffContents)
{
    if (unlikely(g_adapterPtr == NULL)) {
        return VFS_ERROR_LOCAL_ADAPTER_NOT_INIT;
    }
    if (unlikely(fileHandle == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (unlikely(buf == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (unlikely(diffContents == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    VfsPrintLog("The count is %lu, and the offset is %ld.", count, offset);
    return ERROR_SYS_OK;
}

ErrorCode LinuxAdapterPread(FileHandlePtr fileHandle, void *buf, uint64_t count, int64_t offset, int64_t *readSize)
{
    if (unlikely(g_adapterPtr == NULL)) {
        return VFS_ERROR_LOCAL_ADAPTER_NOT_INIT;
    }
    return LinuxCommonPread(fileHandle, buf, count, offset, readSize);
}

ErrorCode LinuxAdapterPreadAsync(FileHandlePtr fileHandle, void *buf, uint64_t count, int64_t offset,
                                 const AsyncIoContext *aioContext)
{
    if (unlikely(g_adapterPtr == NULL)) {
        return VFS_ERROR_LOCAL_ADAPTER_NOT_INIT;
    }
    return LinuxCommonPreadAsync(fileHandle, buf, count, offset, aioContext);
}

ErrorCode LinuxAdapterWriteSync(FileHandlePtr fileHandle, const void *buf, uint64_t count, int64_t *writeSize)
{
    if (unlikely(g_adapterPtr == NULL)) {
        return VFS_ERROR_LOCAL_ADAPTER_NOT_INIT;
    }
    return LinuxCommonWriteSync(fileHandle, buf, count, writeSize);
}

ErrorCode LinuxAdapterPwriteSync(FileHandlePtr fileHandle, const void *buf, uint64_t count, int64_t offset,
                                 int64_t *writeSize)
{
    if (unlikely(g_adapterPtr == NULL)) {
        return VFS_ERROR_LOCAL_ADAPTER_NOT_INIT;
    }
    return LinuxCommonPwriteSync(fileHandle, buf, count, offset, writeSize);
}

ErrorCode LinuxAdapterSnapshotPwriteSync(FileHandlePtr fileHandle, const void *buf, uint64_t count, int64_t offset,
                                         int64_t *writeSize)
{
    if (unlikely(g_adapterPtr == NULL)) {
        return VFS_ERROR_LOCAL_ADAPTER_NOT_INIT;
    }
    if (unlikely(fileHandle == NULL || buf == NULL || writeSize == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    VfsPrintLog("The count is %lu, and the offset is %ld.", count, offset);
    return ERROR_SYS_OK;
}

ErrorCode LinuxAdapterWriteAsync(FileHandlePtr fileHandle, const void *buf, uint64_t count,
                                 const AsyncIoContext *aioContext)
{
    if (unlikely(g_adapterPtr == NULL)) {
        return VFS_ERROR_LOCAL_ADAPTER_NOT_INIT;
    }
    return LinuxCommonWriteAsync(fileHandle, buf, count, aioContext);
}

ErrorCode LinuxAdapterPwriteAsync(FileHandlePtr fileHandle, const void *buf, uint64_t count, int64_t offset,
                                  const AsyncIoContext *aioContext)
{
    if (unlikely(g_adapterPtr == NULL)) {
        return VFS_ERROR_LOCAL_ADAPTER_NOT_INIT;
    }
    return LinuxCommonPwriteAsync(fileHandle, buf, count, offset, aioContext);
}

ErrorCode LinuxAdapterSnapshotPwriteAsync(FileHandlePtr fileHandle, const void *buf, uint64_t count, int64_t offset,
                                          const AsyncIoContext *aioContext)
{
    if (unlikely(g_adapterPtr == NULL)) {
        return VFS_ERROR_LOCAL_ADAPTER_NOT_INIT;
    }
    if (unlikely(fileHandle == NULL || buf == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (aioContext->callback == NULL) {
        VfsPrintLog("The count is %lu, the offset is %ld, the async callback is null.", count, offset);
    } else {
        VfsPrintLog("The count is %lu, and the offset is %ld.", count, offset);
    }
    return ERROR_SYS_OK;
}

ErrorCode LinuxAdapterExtend(FileHandlePtr fileHandle, int64_t length)
{
    if (unlikely(g_adapterPtr == NULL)) {
        return VFS_ERROR_LOCAL_ADAPTER_NOT_INIT;
    }
    return LinuxCommonExtend(fileHandle, length);
}

ErrorCode LinuxAdapterTruncate(FileHandlePtr fileHandle, int64_t length)
{
    if (unlikely(g_adapterPtr == NULL)) {
        return VFS_ERROR_LOCAL_ADAPTER_NOT_INIT;
    }
    return LinuxCommonTruncate(fileHandle, length);
}

ErrorCode LinuxAdapterGetSize(FileHandlePtr fileHandle, int64_t *fileSize)
{
    if (unlikely(g_adapterPtr == NULL)) {
        return VFS_ERROR_LOCAL_ADAPTER_NOT_INIT;
    }
    return LinuxCommonGetSize(fileHandle, fileSize);
}

ErrorCode LinuxAdapterFileControl(FileHandlePtr fileHandle, int cmd, const AdapterFileControlInfo *controlInfo)
{
    if (unlikely(g_adapterPtr == NULL)) {
        return VFS_ERROR_LOCAL_ADAPTER_NOT_INIT;
    }
    return LinuxCommonFileControl(fileHandle, cmd, controlInfo);
}

ErrorCode LinuxAdapterLockFile(FileHandlePtr fileHandle, int64_t startPos, int64_t len, int lockMode, uint32_t timeout)
{
    if (unlikely(g_adapterPtr == NULL)) {
        return VFS_ERROR_LOCAL_ADAPTER_NOT_INIT;
    }
    return LinuxCommonLockFile(fileHandle, startPos, len, lockMode, timeout);
}

ErrorCode LinuxAdapterTryLockFile(FileHandlePtr fileHandle, int64_t startPos, int64_t len, int lockMode)
{
    if (unlikely(g_adapterPtr == NULL)) {
        return VFS_ERROR_LOCAL_ADAPTER_NOT_INIT;
    }
    return LinuxCommonTryLockFile(fileHandle, startPos, len, lockMode);
}

ErrorCode LinuxAdapterUnlockFile(FileHandlePtr fileHandle, int64_t startPos, int64_t len)
{
    if (unlikely(g_adapterPtr == NULL)) {
        return VFS_ERROR_LOCAL_ADAPTER_NOT_INIT;
    }
    return LinuxCommonUnlockFile(fileHandle, startPos, len);
}

ErrorCode LinuxAdapterFDataSync(FileHandlePtr fileHandle)
{
    if (unlikely(g_adapterPtr == NULL)) {
        return VFS_ERROR_LOCAL_ADAPTER_NOT_INIT;
    }
    return LinuxCommonFDataSync(fileHandle);
}

ErrorCode LinuxAdapterStartAIO(VfsHandlePtr vfs, uint16_t maxEvents, uint16_t threadCount,
                               void (*threadEnterCallback)(void), void (*threadExitCallback)(void))
{
    if (unlikely(g_adapterPtr == NULL)) {
        return VFS_ERROR_LOCAL_ADAPTER_NOT_INIT;
    }
    return LinuxCommonStartAIO(vfs, maxEvents, threadCount, threadEnterCallback, threadExitCallback);
}

/* vfs linux adapter dynamic lib function */
UTILS_EXPORT VfsAdapterInterface *GetVfsAdapterInterface(void)
{
    VfsAdapterInterface *ops =
        (VfsAdapterInterface *)VfsMemAlloc(g_linuxAdapterMemAllocatorPtr, sizeof(VfsAdapterInterface));
    if (unlikely(ops == NULL)) {
        return NULL; /* out of memory */
    }
    ops->setAdapterConfig = LinuxAdapterSetConfig;
    ops->getAdapterConfig = LinuxAdapterGetConfig;
    ops->initAdapter = LinuxAdapterInit;
    ops->stopAdapter = NULL;
    ops->updateAdapterParam = LinuxAdapterUpdateParam;
    ops->getAdapterClientId = LinuxAdapterGetClientId;
    ops->handlerAdapterRemoteMsg = LinuxAdapterRemoteMsgHandler;
    ops->exitAdapter = LinuxAdapterExit;
    ops->createTenant = LinuxAdapterCreateTenant;
    ops->deleteTenant = LinuxAdapterDeleteTenant;
    ops->createVfs = LinuxAdapterCreateVfs;
    ops->dropVfs = LinuxAdapterDropVfs;
    ops->mountVfs = LinuxAdapterMountVfs;
    ops->unmountVfs = LinuxAdapterUnmountVfs;
    ops->vfsControl = LinuxAdapterVfsControl;
    ops->createSnapshot = LinuxAdapterCreateSnapshot;
    ops->dropSnapshot = LinuxAdapterDropSnapshot;
    ops->rollbackSnapshot = LinuxAdapterRollbackSnapshot;
    ops->createStoreSpace = LinuxAdapterCreateStoreSpace;
    ops->deleteStoreSpace = LinuxAdapterDeleteStoreSpace;
    ops->updateStoreSpace = LinuxAdapterUpdateStoreSpace;
    ops->queryStoreSpaceAttr = LinuxAdapterQueryStoreSpaceAttr;
    ops->queryStoreSpaceUsedSize = LinuxAdapterQueryStoreSpaceUsedSize;
    ops->createFile = LinuxAdapterCreateFile;
    ops->removeFile = LinuxAdapterRemoveFile;
    ops->fileIsExist = LinuxAdapterFileIsExist;
    ops->openFile = LinuxAdapterOpenFile;
    ops->openFileSnapshot = LinuxAdapterOpenFileSnapshot;
    ops->close = LinuxAdapterCloseFile;
    ops->renameFile = LinuxAdapterRenameFile;
    ops->fsync = LinuxAdapterFsync;
    ops->fileSeek = LinuxAdapterFileSeek;
    ops->rewind = LinuxAdapterRewind;
    ops->read = LinuxAdapterRead;
    ops->pread = LinuxAdapterPread;
    ops->snapshotPread = LinuxAdapterSnapshotPread;
    ops->preadAsync = LinuxAdapterPreadAsync;
    ops->writeSync = LinuxAdapterWriteSync;
    ops->pwriteSync = LinuxAdapterPwriteSync;
    ops->writeAsync = LinuxAdapterWriteAsync;
    ops->pwriteAsync = LinuxAdapterPwriteAsync;
    ops->extend = LinuxAdapterExtend;
    ops->truncate = LinuxAdapterTruncate;
    ops->getSize = LinuxAdapterGetSize;
    ops->fileControl = LinuxAdapterFileControl;
    ops->lockFile = LinuxAdapterLockFile;
    ops->tryLockFile = LinuxAdapterTryLockFile;
    ops->unlockFile = LinuxAdapterUnlockFile;
    ops->fDataSync = LinuxAdapterFDataSync;
    ops->enableAIO = LinuxAdapterStartAIO;
    ops->createClientHandle = LinuxAdapterCreateVfsClientHandle;
    ops->deleteClientHandle = LinuxAdapterDeleteVfsClientHandle;
    ops->setClientConfig = LinuxAdapterSetClientInstanceConfig;
    ops->getClientConfig = LinuxAdapterGetClientInstanceConfig;
    return ops;
}

UTILS_EXPORT ErrorCode SetVfsAdapterMemAllocator(const MemAllocator *memAllocator)
{
    if (!memAllocator) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    g_linuxAdapterMemAllocator = *memAllocator;
    g_linuxAdapterMemAllocatorPtr = &g_linuxAdapterMemAllocator;
    return ERROR_SYS_OK;
}

UTILS_EXPORT void DeleteVfsAdapterInterface(VfsAdapterInterface *ops)
{
    if (ops != NULL) {
        VfsMemFree(g_linuxAdapterMemAllocatorPtr, ops);
    }
}

UTILS_EXPORT AdapterInfo GetVfsAdapterInfo(void)
{
    AdapterInfo info = {LINUX_VFS_LIB_MAGIC, LINUX_VFS_LIB_MODE, LINUX_VFS_LIB_VERSION};
    return info;
}
