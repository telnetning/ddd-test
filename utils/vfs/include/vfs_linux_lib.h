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
 * vfs_linux_lib.h
 *
 * Description:
 * linux vfs adapter dynamic library header
 *
 * ---------------------------------------------------------------------------------
 */
#ifndef UTILS_VFS_LINUX_LIB_H
#define UTILS_VFS_LINUX_LIB_H

#include "types/atomic_type.h"
#include "vfs/vfs_adapter_interface.h"

typedef struct LinuxVfsAdapter LinuxVfsAdapter;
struct LinuxVfsAdapter {
    uint16_t pageSize;
    uint16_t dbType;
    Atomic32 mountVfsCount;
    char *rootDataDir;
};

/* Adapter interface definition */
ErrorCode LinuxAdapterSetConfig(const char *para, const char *value);
ErrorCode LinuxAdapterGetConfig(const char *para, char *value, uint64_t len);
ErrorCode LinuxAdapterInit(const VfsAdapterParam *param);
ErrorCode LinuxAdapterUpdateParam(VfsClientHandlePtr vfsClientHandle, const VfsAdapterParam *param);
ErrorCode LinuxAdapterGetClientId(VfsClientHandlePtr vfsClientHandle, uint64_t *adapterClientId);
void LinuxAdapterRemoteMsgHandler(VfsClientHandlePtr vfsClientHandle, void *msgRequest, void *userContext);
ErrorCode LinuxAdapterExit(void);
ErrorCode LinuxAdapterCreateVfsClientHandle(const char *clusterName, uint32_t authType, const char *storageServerAddr,
    VfsClientHandlePtr *vfsClientHandle);
ErrorCode LinuxAdapterDeleteVfsClientHandle(VfsClientHandlePtr vfsClientHandle);
ErrorCode LinuxAdapterSetClientInstanceConfig(VfsClientHandlePtr vfsClientHandle, const char *para,
    const char *value);
ErrorCode LinuxAdapterGetClientInstanceConfig(VfsClientHandlePtr vfsClientHandle, const char *para,
    char *value, uint64_t len);
ErrorCode LinuxAdapterCreateTenant(VfsClientHandlePtr vfsClientHandle, const char *clusterName,
                                   const char *tenantName);
ErrorCode LinuxAdapterDeleteTenant(VfsClientHandlePtr vfsClientHandle, const char *clusterName,
                                   const char *tenantName, uint64_t attr);
ErrorCode LinuxAdapterCreateVfs(VfsClientHandlePtr vfsClientHandle, const char *tenantName,
                                const char *vfsName, uint64_t attr);
ErrorCode LinuxAdapterDropVfs(VfsClientHandlePtr vfsClientHandle, const char *tenantName, const char *vfsName,
                              uint64_t attr);
ErrorCode LinuxAdapterMountVfs(VfsClientHandlePtr vfsClientHandle, const char *tenantName,
                               const char *vfsName, VfsHandlePtr *vfs);
ErrorCode LinuxAdapterUnmountVfs(VfsHandlePtr vfs);
ErrorCode LinuxAdapterVfsControl(VfsClientHandlePtr vfsClientHandle, const char *tenantName, int cmd,
                                 VfsControlInfo *vfsControlInfo, uint32_t count);
ErrorCode LinuxAdapterCreateSnapshot(VfsClientHandlePtr vfsClientHandle, const char *tenantName,
                                     const char *vfsName, const char *snapshotName, int64_t flags);
ErrorCode LinuxAdapterDropSnapshot(VfsClientHandlePtr vfsClientHandle, const char *tenantName,
                                   const char *vfsName, const char *snapshotName);
ErrorCode LinuxAdapterRollbackSnapshot(VfsClientHandlePtr vfsClientHandle, const char *tenantName,
                                       const char *vfsName, const char *snapshotName);
ErrorCode LinuxAdapterCreateStoreSpace(VfsClientHandlePtr vfsClientHandle, const char *tenantName,
                                       const char **storeSpaceNames, const StoreSpaceAttr *attrs, uint32 count);
ErrorCode LinuxAdapterDeleteStoreSpace(VfsClientHandlePtr vfsClientHandle, const char *tenantName,
                                       const char **storeSpaceNames, uint32 count);
ErrorCode LinuxAdapterUpdateStoreSpace(VfsClientHandlePtr vfsClientHandle, const char *tenantName,
                                       const char **storeSpaceNames, const StoreSpaceAttr *attrs, uint32 count);
ErrorCode LinuxAdapterQueryStoreSpaceAttr(VfsClientHandlePtr vfsClientHandle, const char *tenantName,
                                          const char **storeSpaceNames, StoreSpaceAttr *storeSpaceAttrs, uint32 count);
ErrorCode LinuxAdapterQueryStoreSpaceUsedSize(VfsClientHandlePtr vfsClientHandle, const char *tenantName,
                                              const char **storeSpaceNames, uint64 *usedSizes, uint32 count);
ErrorCode LinuxAdapterCreateFile(VfsHandlePtr vfs, const char *pathName, const FileParam *para,
                                 FileHandlePtr *fileHandle);
ErrorCode LinuxAdapterRemoveFile(VfsHandlePtr vfs, const char *pathName);
ErrorCode LinuxAdapterFileIsExist(VfsHandlePtr vfs, const char *pathName, bool *out);
ErrorCode LinuxAdapterOpenFile(VfsHandlePtr vfs, const char *pathName, int flags, FileHandlePtr *fileHandle);
ErrorCode LinuxAdapterOpenFileSnapshot(VfsHandlePtr vfs, FileOpenParam *openPara, FileHandlePtr *fileHandle);
ErrorCode LinuxAdapterCloseFile(FileHandlePtr fileHandle);
ErrorCode LinuxAdapterFsync(FileHandlePtr fileHandle);
ErrorCode LinuxAdapterFileSeek(FileHandlePtr fileHandle, int64_t offset, int seekFlag, int64_t *newPos);
ErrorCode LinuxAdapterRewind(FileHandlePtr fileHandle);
ErrorCode LinuxAdapterRead(FileHandlePtr fileHandle, void *buf, uint64_t count, int64_t *readSize);
ErrorCode LinuxAdapterSnapshotPread(FileHandlePtr fileHandle, void *buf, uint64_t count, int64_t offset,
                                    DiffContents *diffContents);
ErrorCode LinuxAdapterPread(FileHandlePtr fileHandle, void *buf, uint64_t count, int64_t offset, int64_t *readSize);
ErrorCode LinuxAdapterPreadAsync(FileHandlePtr fileHandle, void *buf, uint64_t count, int64_t offset,
                                 const AsyncIoContext *aioContext);
ErrorCode LinuxAdapterWriteSync(FileHandlePtr fileHandle, const void *buf, uint64_t count, int64_t *writeSize);
ErrorCode LinuxAdapterPwriteSync(FileHandlePtr fileHandle, const void *buf, uint64_t count, int64_t offset,
                                 int64_t *writeSize);
ErrorCode LinuxAdapterSnapshotPwriteSync(FileHandlePtr fileHandle, const void *buf, uint64_t count, int64_t offset,
                                         int64_t *writeSize);
ErrorCode LinuxAdapterWriteAsync(FileHandlePtr fileHandle, const void *buf, uint64_t count,
                                 const AsyncIoContext *aioContext);
ErrorCode LinuxAdapterPwriteAsync(FileHandlePtr fileHandle, const void *buf, uint64_t count, int64_t offset,
                                  const AsyncIoContext *aioContext);
ErrorCode LinuxAdapterSnapshotPwriteAsync(FileHandlePtr fileHandle, const void *buf, uint64_t count, int64_t offset,
                                          const AsyncIoContext *aioContext);
ErrorCode LinuxAdapterExtend(FileHandlePtr fileHandle, int64_t length);
ErrorCode LinuxAdapterTruncate(FileHandlePtr fileHandle, int64_t length);
ErrorCode LinuxAdapterGetSize(FileHandlePtr fileHandle, int64_t *fileSize);
ErrorCode LinuxAdapterFileControl(FileHandlePtr fileHandle, int cmd, const AdapterFileControlInfo *controlInfo);
ErrorCode LinuxAdapterLockFile(FileHandlePtr fileHandle, int64_t startPos, int64_t len, int lockMode, uint32_t timeout);
ErrorCode LinuxAdapterTryLockFile(FileHandlePtr fileHandle, int64_t startPos, int64_t len, int lockMode);
ErrorCode LinuxAdapterUnlockFile(FileHandlePtr fileHandle, int64_t startPos, int64_t len);
ErrorCode LinuxAdapterFDataSync(FileHandlePtr fileHandle);

#endif /* UTILS_VFS_LINUX_LIB_H */
