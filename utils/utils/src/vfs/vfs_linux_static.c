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
 * vfs_linux_static.c
 *
 * Description:
 * Local vfs implementation
 *
 * ---------------------------------------------------------------------------------
 */
#include "vfs/vfs_linux_static.h"

static MemAllocator g_vfsLinuxStaticMemAllocator;
static MemAllocator *g_vfsLinuxStaticMemAllocatorPtr = NULL;
static LinuxVfsHandle g_staticLocalVfsHandle;
static bool g_staticLocalVfsHandleIsReady = false;

static ErrorCode LocalCreateFile(VfsHandlePtr vfs, const char *pathName, const FileParam *param,
                                 FileHandlePtr *fileHandle)
{
    return LinuxCommonCreate(vfs, pathName, param, fileHandle, g_vfsLinuxStaticMemAllocatorPtr);
}

static ErrorCode LocalOpenFile(VfsHandlePtr vfs, const char *pathName, int flags, FileHandlePtr *fileHandle)
{
    return LinuxCommonOpen(vfs, pathName, flags, fileHandle, g_vfsLinuxStaticMemAllocatorPtr);
}

static ErrorCode LocalClose(FileHandlePtr fileHandle)
{
    return LinuxCommonClose(fileHandle, g_vfsLinuxStaticMemAllocatorPtr);
}

VfsHandlePtr GetStaticLocalVfs(const MemAllocator *memAllocator)
{
    if (!g_staticLocalVfsHandleIsReady) {
        InitVfsHandle(&g_staticLocalVfsHandle, true);
        if (memAllocator != NULL) {
            g_vfsLinuxStaticMemAllocator = *memAllocator;
            g_vfsLinuxStaticMemAllocatorPtr = &g_vfsLinuxStaticMemAllocator;
        } else {
            g_vfsLinuxStaticMemAllocatorPtr = NULL;
        }
        g_staticLocalVfsHandleIsReady = true;
    }
    return &g_staticLocalVfsHandle;
}

void ReleaseStaticLocalVfs(void)
{
    if (g_staticLocalVfsHandleIsReady) {
        if (g_staticLocalVfsHandle.aioThreadContext != NULL) {
            StopAIO(g_staticLocalVfsHandle.aioThreadContext);
        }
        g_staticLocalVfsHandle.aioThreadContext = NULL;
        ReleaseVfsHandle(&g_staticLocalVfsHandle);
    }
    g_staticLocalVfsHandleIsReady = false;
}

bool StaticVfsHasOpenFile(void)
{
    if (!g_staticLocalVfsHandleIsReady) {
        return false;
    }
    return GSDB_ATOMIC32_GET(&(g_staticLocalVfsHandle.openFileCount)) > 0;
}

VfsAdapterInterface *GetStaticVfsOps(void)
{
    static VfsAdapterInterface ops;
    /* static local vfs interface do not need to call adapter and vfs related interface, just call file operations */
    ops.setAdapterConfig = NULL;
    ops.getAdapterConfig = NULL;
    ops.initAdapter = NULL;
    ops.stopAdapter = NULL;
    ops.updateAdapterParam = NULL;
    ops.getAdapterClientId = NULL;
    ops.handlerAdapterRemoteMsg = NULL;
    ops.exitAdapter = NULL;
    ops.createTenant = NULL;
    ops.deleteTenant = NULL;
    ops.createVfs = NULL;
    ops.dropVfs = NULL;
    ops.mountVfs = NULL;
    ops.unmountVfs = NULL;
    ops.vfsControl = NULL;
    ops.createSnapshot = NULL;
    ops.dropSnapshot = NULL;
    ops.rollbackSnapshot = NULL;
    ops.snapshotPread = NULL;
    ops.createStoreSpace = NULL;
    ops.deleteStoreSpace = NULL;
    ops.updateStoreSpace = NULL;
    ops.queryStoreSpaceAttr = NULL;
    ops.queryStoreSpaceUsedSize = NULL;
    ops.createFile = LocalCreateFile;
    ops.removeFile = LinuxCommonRemove;
    ops.fileIsExist = LinuxCommonFileIsExist;
    ops.openFile = LocalOpenFile;
    ops.openFileSnapshot = NULL;
    ops.close = LocalClose;
    ops.renameFile = LinuxCommonRenameFile;
    ops.fsync = LinuxCommonFsync;
    ops.fileSeek = LinuxCommonFileSeek;
    ops.rewind = LinuxCommonRewind;
    ops.read = LinuxCommonRead;
    ops.pread = LinuxCommonPread;
    ops.preadAsync = LinuxCommonPreadAsync;
    ops.writeSync = LinuxCommonWriteSync;
    ops.pwriteSync = LinuxCommonPwriteSync;
    ops.writeAsync = LinuxCommonWriteAsync;
    ops.pwriteAsync = LinuxCommonPwriteAsync;
    ops.extend = LinuxCommonExtend;
    ops.truncate = LinuxCommonTruncate;
    ops.getSize = LinuxCommonGetSize;
    ops.fileControl = LinuxCommonFileControl;
    ops.lockFile = LinuxCommonLockFile;
    ops.tryLockFile = LinuxCommonTryLockFile;
    ops.unlockFile = LinuxCommonUnlockFile;
    ops.fDataSync = LinuxCommonFDataSync;
    ops.enableAIO = LinuxCommonStartAIO;
    return &ops;
}

#ifdef ENABLE_UT
int32_t GetStaticVfsOpenFileCount(void)
{
    if (!g_staticLocalVfsHandleIsReady) {
        return 0;
    }
    return GSDB_ATOMIC32_GET(&(g_staticLocalVfsHandle.openFileCount));
}
#endif
