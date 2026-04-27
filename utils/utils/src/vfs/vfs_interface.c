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
 * vfs_interface.c
 *
 * Description:
 * Vfs interface implementation
 *
 * ---------------------------------------------------------------------------------
 */
#include <dlfcn.h>

#include "securec.h"
#include "defines/common_defs.h"
#include "vfs/vfs_utils.h"
#include "vfs/vfs_linux_common.h"
#include "vfs/vfs_linux_static.h"
#include "vfs/vfs_linux_aio.h"
#include "vfs/vfs_interface.h"

static bool g_vfsModuleIsInit = false;
static MemAllocator g_vfsMemAllocator;
static MemAllocator *g_vfsMemAllocatorPtr = NULL;
static VfsClientHandle *g_staticLocalVfsClientHandle = NULL;
static VirtualFileSystem g_staticLocalVfs;
static bool g_staticLocalVfsIsReady = false;

UTILS_EXPORT ErrorCode InitVfsModule(const MemAllocator *memAllocator)
{
    if (g_vfsModuleIsInit) {
        return VFS_ERROR_VFS_MODULE_ALREADY_INIT;
    }
    g_vfsModuleIsInit = true;
    if (memAllocator != NULL) {
        g_vfsMemAllocator = *memAllocator;
        g_vfsMemAllocatorPtr = &g_vfsMemAllocator;
    }

    return ERROR_SYS_OK;
}

UTILS_EXPORT ErrorCode ExitVfsModule(void)
{
    if (g_vfsModuleIsInit) {
        if (g_staticLocalVfsIsReady) {
            if (StaticVfsHasOpenFile()) {
                /* Static local vfs has files not close */
                VfsPrintReleaseLog("static local vfs has files not close");
                return VFS_ERROR_VFS_RESOURCE_NOT_RELEASE;
            }
            ReleaseStaticLocalVfs();
        }
        g_vfsModuleIsInit = false;
        g_vfsMemAllocatorPtr = NULL;
        g_staticLocalVfsIsReady = false;
    }
    return ERROR_SYS_OK;
}

UTILS_EXPORT ErrorCode GetStaticLocalVfsInstance(VirtualFileSystem **vfs)
{
    if (unlikely(!g_vfsModuleIsInit)) {
        return VFS_ERROR_VFS_MODULE_NOT_INIT;
    }
    if (unlikely(vfs == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (!g_staticLocalVfsIsReady) {
        VfsAdapterInterface *ops = GetStaticVfsOps();
        if (ops == NULL) {
            return VFS_ERROR_OUT_OF_MEMORY;
        }
        if (g_vfsMemAllocatorPtr) {
            g_staticLocalVfs.vfsHandle = GetStaticLocalVfs(g_vfsMemAllocatorPtr);
        } else {
            g_staticLocalVfs.vfsHandle = GetStaticLocalVfs(NULL);
        }
        g_staticLocalVfs.ops = ops;
        g_staticLocalVfs.isStaticVfs = true;
        g_staticLocalVfsIsReady = true;
    }
    *vfs = &g_staticLocalVfs;
    return ERROR_SYS_OK;
}

UTILS_EXPORT ErrorCode LoadVfsLib(const char *vfsLibPath, const MemAllocator *memAllocator, VfsLibHandle **vfsLibHandle)
{
    if (unlikely(!g_vfsModuleIsInit)) {
        return VFS_ERROR_VFS_MODULE_NOT_INIT;
    }
    if (unlikely(vfsLibPath == NULL || vfsLibHandle == NULL)) {
        VfsPrintReleaseLog("The vfsLibPath or vfsLibHandle is invalid.");
        return VFS_ERROR_PARAMETERS_INVALID;
    }

    /* Step 1: Call dlopen() to get lib handle */
    void *libHandle = LinuxDlOpenLibraryWithPathCheck(vfsLibPath);
    ASSERT(libHandle != NULL);
    if (unlikely(libHandle == NULL)) {
        return VFS_ERROR_LOAD_ADAPTER_LIB_FAIL;
    }

    /* Step 2: Call dlsym() to get lib info and functions */
    GetVfsAdapterInterfaceFunc getAdapterInterfaceFunc =
        (GetVfsAdapterInterfaceFunc)(uintptr_t)dlsym(libHandle, GET_ADAPTER_INTERFACE_FUNC);
    DeleteVfsAdapterInterfaceFunc deleteAdapterInterfaceFunc =
        (DeleteVfsAdapterInterfaceFunc)(uintptr_t)dlsym(libHandle, DELETE_ADAPTER_INTERFACE_FUNC);
    if (getAdapterInterfaceFunc == NULL || deleteAdapterInterfaceFunc == NULL) {
        VfsPrintReleaseLog("Function loading error in dlsym");
        (void)dlclose(libHandle);
        return VFS_ERROR_LOAD_ADAPTER_FUNC_FAIL;
    }
    GetVfsAdapterInfoFunc getVfsAdapterInfoFunc =
        (GetVfsAdapterInfoFunc)(uintptr_t)dlsym(libHandle, GET_ADAPTER_INFO_FUNC);
    if (getVfsAdapterInfoFunc != NULL) {
        SYMBOL_UNUSED AdapterInfo info = getVfsAdapterInfoFunc();
        VfsPrintLog("Loading adapter info: magic[%lu], mode[%u], version[%u]", info.magic, info.mode, info.version);
    } else {
        VfsPrintLog("Unknown adapter info");
    }

    /* Step 3: if memAllocator is not NULL, call SetVfsAdapterMemAllocator() */
    if (memAllocator != NULL) {
        SetVfsAdapterMemAllocatorFunc setVfsAdapterMemAllocatorFunc =
            (SetVfsAdapterMemAllocatorFunc)(uintptr_t)dlsym(libHandle, SET_ADAPTER_MEM_ALLOCATOR_FUNC);
        if (setVfsAdapterMemAllocatorFunc != NULL) {
            (void)setVfsAdapterMemAllocatorFunc(memAllocator);
        }
    }

    /* Step 4: Create VfsLibHandle object and assign values */
    VfsLibHandle *curLibHandle = (VfsLibHandle *)VfsMemAlloc(g_vfsMemAllocatorPtr, sizeof(VfsLibHandle));
    if (unlikely(curLibHandle == NULL)) {
        (void)dlclose(libHandle);
        return VFS_ERROR_OUT_OF_MEMORY;
    }
    curLibHandle->libHandle = libHandle;
    curLibHandle->deleteAdapterInterfaceFunc = deleteAdapterInterfaceFunc;
    curLibHandle->ops = getAdapterInterfaceFunc();

    *vfsLibHandle = curLibHandle;
    return ERROR_SYS_OK;
}

UTILS_EXPORT ErrorCode OffloadVfsLib(VfsLibHandle *vfsLibHandle)
{
    if (unlikely(!g_vfsModuleIsInit)) {
        return VFS_ERROR_VFS_MODULE_NOT_INIT;
    }
    if (unlikely(vfsLibHandle == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (unlikely(vfsLibHandle->ops == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (vfsLibHandle->ops->exitAdapter == NULL) {
        return VFS_ERROR_OPERATION_NOT_SUPPORT;
    }
    if (vfsLibHandle->ops->deleteClientHandle == NULL) {
        return VFS_ERROR_OPERATION_NOT_SUPPORT;
    }

    /* Step 1: Call deleteClientHandle() to free g_staticLocalVfsClientHandle */
    ErrorCode rc = ERROR_SYS_OK;
    if (g_staticLocalVfsClientHandle != NULL) {
        void *clientHandle = g_staticLocalVfsClientHandle->clientHandle;
        rc = vfsLibHandle->ops->deleteClientHandle(clientHandle);
        if (unlikely(rc != ERROR_SYS_OK)) {
            return rc;
        }
        VfsMemFree(g_vfsMemAllocatorPtr, g_staticLocalVfsClientHandle);
        g_staticLocalVfsClientHandle = NULL;
    }

    /* Step 2: Call ExitAdapter() to free adapter resource */
    rc = vfsLibHandle->ops->exitAdapter();
    if (unlikely(rc != ERROR_SYS_OK)) {
        return rc;
    }

    /* Step 3: Delete VfsAdapterInterface object */
    vfsLibHandle->deleteAdapterInterfaceFunc(vfsLibHandle->ops);

    /* Step 4: Call dlclose() to offload lib handle */
    if (dlclose(vfsLibHandle->libHandle) != 0) {
        return VFS_ERROR_OFFLOAD_ADAPTER_LIB_FAIL;
    }

    /* Step 5: Delete VfsLibHandle object */
    VfsMemFree(g_vfsMemAllocatorPtr, vfsLibHandle);

    return ERROR_SYS_OK;
}

UTILS_EXPORT ErrorCode SetVfsLibConfig(VfsLibHandle *vfsLibHandle, const char *para, const char *value)
{
    if (unlikely(!g_vfsModuleIsInit)) {
        return VFS_ERROR_VFS_MODULE_NOT_INIT;
    }
    if (unlikely(vfsLibHandle == NULL || para == NULL || value == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (vfsLibHandle->ops->setAdapterConfig == NULL) {
        return VFS_ERROR_OPERATION_NOT_SUPPORT;
    }
    return vfsLibHandle->ops->setAdapterConfig(para, value);
}

UTILS_EXPORT ErrorCode GetVfsLibConfig(VfsLibHandle *vfsLibHandle, const char *para, char *value, uint64_t len)
{
    if (unlikely(!g_vfsModuleIsInit)) {
        return VFS_ERROR_VFS_MODULE_NOT_INIT;
    }
    if (unlikely(vfsLibHandle == NULL || para == NULL || value == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (vfsLibHandle->ops->getAdapterConfig == NULL) {
        return VFS_ERROR_OPERATION_NOT_SUPPORT;
    }
    return vfsLibHandle->ops->getAdapterConfig(para, value, len);
}

UTILS_EXPORT ErrorCode InitVfsLib(VfsLibHandle *vfsLibHandle, const VfsLibParameter *param)
{
    if (unlikely(!g_vfsModuleIsInit)) {
        return VFS_ERROR_VFS_MODULE_NOT_INIT;
    }
    if (unlikely(vfsLibHandle == NULL || param == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (vfsLibHandle->ops->initAdapter == NULL) {
        return VFS_ERROR_OPERATION_NOT_SUPPORT;
    }
    if (vfsLibHandle->ops->createClientHandle == NULL) {
        return VFS_ERROR_OPERATION_NOT_SUPPORT;
    }
    ErrorCode rc = vfsLibHandle->ops->initAdapter(param);
    if (unlikely(rc != ERROR_SYS_OK)) {
        return rc;
    }
    if (g_staticLocalVfsClientHandle != NULL) {
        return ERROR_SYS_OK;
    }

    VfsClientHandle *curClientHandle = (VfsClientHandle *)VfsMemAlloc(g_vfsMemAllocatorPtr, sizeof(VfsClientHandle));
    if (unlikely(curClientHandle == NULL)) {
        return VFS_ERROR_OUT_OF_MEMORY;
    }
    rc = vfsLibHandle->ops->createClientHandle(
        param->clusterName, (uint32)-1, param->storageServerAddr, &(curClientHandle->clientHandle));
    if (unlikely(rc != ERROR_SYS_OK)) {
        VfsMemFree(g_vfsMemAllocatorPtr, curClientHandle);
        return rc;
    }

    curClientHandle->ops = vfsLibHandle->ops;
    g_staticLocalVfsClientHandle = curClientHandle;
    return ERROR_SYS_OK;
}

UTILS_EXPORT ErrorCode StopVfsLib(VfsLibHandle *vfsLibHandle)
{
    if (unlikely(!g_vfsModuleIsInit)) {
        return VFS_ERROR_VFS_MODULE_NOT_INIT;
    }
    if (unlikely(vfsLibHandle == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (vfsLibHandle->ops->stopAdapter == NULL) {
        return VFS_ERROR_OPERATION_NOT_SUPPORT;
    }
    return vfsLibHandle->ops->stopAdapter();
}

UTILS_EXPORT ErrorCode CreateVfsClientHandle(VfsLibHandle *vfsLibHandle, const char *clusterName,
    uint32_t authType, const char *storageServerAddr, VfsClientHandle **vfsClientHandle)
{
    if (unlikely(!g_vfsModuleIsInit)) {
        return VFS_ERROR_VFS_MODULE_NOT_INIT;
    }
    if (unlikely(vfsLibHandle == NULL || storageServerAddr == NULL || vfsClientHandle == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (vfsLibHandle->ops->createClientHandle == NULL) {
       return VFS_ERROR_OPERATION_NOT_SUPPORT;
    }

    VfsClientHandle *curClientHandle = (VfsClientHandle *)VfsMemAlloc(g_vfsMemAllocatorPtr, sizeof(VfsClientHandle));
    if (unlikely(curClientHandle == NULL)) {
        return VFS_ERROR_OUT_OF_MEMORY;
    }
   
    ErrorCode rc = vfsLibHandle->ops->createClientHandle(
        clusterName, authType, storageServerAddr, &(curClientHandle->clientHandle));
    if (unlikely(rc != ERROR_SYS_OK)) {
        VfsMemFree(g_vfsMemAllocatorPtr, curClientHandle);
        return rc;
    }

    curClientHandle->ops = vfsLibHandle->ops;
    *vfsClientHandle = curClientHandle;
    return ERROR_SYS_OK;
}

UTILS_EXPORT ErrorCode DeleteVfsClientHandle(VfsClientHandle *vfsClientHandle)
{
    if (unlikely(!g_vfsModuleIsInit)) {
        return VFS_ERROR_VFS_MODULE_NOT_INIT;
    }
    if (unlikely(vfsClientHandle == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (vfsClientHandle->ops->deleteClientHandle == NULL) {
        return VFS_ERROR_OPERATION_NOT_SUPPORT;
    }
    ErrorCode rc = vfsClientHandle->ops->deleteClientHandle(vfsClientHandle->clientHandle);
    if (unlikely(rc != ERROR_SYS_OK)) {
        return rc;
    }

    VfsMemFree(g_vfsMemAllocatorPtr, vfsClientHandle);
    return ERROR_SYS_OK;
}

ErrorCode SetVfsClientConfig(VfsClientHandle *vfsClientHandle, const char *para, const char *value)
{
    if (unlikely(!g_vfsModuleIsInit)) {
        return VFS_ERROR_VFS_MODULE_NOT_INIT;
    }
    if (vfsClientHandle == NULL) {
        vfsClientHandle = g_staticLocalVfsClientHandle;
    }
    if (unlikely(vfsClientHandle == NULL || para == NULL || value == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (vfsClientHandle->ops->setClientConfig == NULL) {
        return VFS_ERROR_OPERATION_NOT_SUPPORT;
    }
    return vfsClientHandle->ops->setClientConfig(vfsClientHandle->clientHandle, para, value);
}

ErrorCode GetVfsClientConfig(VfsClientHandle *vfsClientHandle, const char *para, char *value, uint64_t len)
{
    if (unlikely(!g_vfsModuleIsInit)) {
        return VFS_ERROR_VFS_MODULE_NOT_INIT;
    }
    if (vfsClientHandle == NULL) {
        vfsClientHandle = g_staticLocalVfsClientHandle;
    }
    if (unlikely(vfsClientHandle == NULL || para == NULL || value == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (vfsClientHandle->ops->getClientConfig == NULL) {
        return VFS_ERROR_OPERATION_NOT_SUPPORT;
    }
    return vfsClientHandle->ops->getClientConfig(vfsClientHandle->clientHandle, para, value, len);
}

UTILS_EXPORT ErrorCode UpdateVfsLibParameter(VfsClientHandle *vfsClientHandle, const VfsLibParameter *param)
{
    if (unlikely(!g_vfsModuleIsInit)) {
        return VFS_ERROR_VFS_MODULE_NOT_INIT;
    }
    if (vfsClientHandle == NULL) {
        vfsClientHandle = g_staticLocalVfsClientHandle;
    }
    if (unlikely(vfsClientHandle == NULL || param == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (vfsClientHandle->ops->updateAdapterParam == NULL) {
        return VFS_ERROR_OPERATION_NOT_SUPPORT;
    }
    return vfsClientHandle->ops->updateAdapterParam(vfsClientHandle->clientHandle, param);
}

UTILS_EXPORT ErrorCode GetVfsLibClientId(const VfsClientHandle *vfsClientHandle, uint64_t *clientId)
{
    if (unlikely(!g_vfsModuleIsInit)) {
        return VFS_ERROR_VFS_MODULE_NOT_INIT;
    }
    if (vfsClientHandle == NULL) {
        vfsClientHandle = g_staticLocalVfsClientHandle;
    }
    if (unlikely(vfsClientHandle == NULL || clientId == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (unlikely(vfsClientHandle->ops == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (unlikely(vfsClientHandle->ops->getAdapterClientId == NULL)) {
        return VFS_ERROR_OPERATION_NOT_SUPPORT;
    }
    return vfsClientHandle->ops->getAdapterClientId(vfsClientHandle->clientHandle, clientId);
}

UTILS_EXPORT void VfsLibHandlerRemoteMsg(VfsClientHandle *vfsClientHandle, void *msgRequest, void *handleMsgContext)
{
    if (vfsClientHandle == NULL) {
        vfsClientHandle = g_staticLocalVfsClientHandle;
    }
    if (unlikely(!g_vfsModuleIsInit || vfsClientHandle == NULL)) {
        return;
    }
    if (vfsClientHandle->ops->handlerAdapterRemoteMsg == NULL) {
        return;
    }
    vfsClientHandle->ops->handlerAdapterRemoteMsg(vfsClientHandle->clientHandle, msgRequest, handleMsgContext);
}

UTILS_EXPORT ErrorCode CreateStoreTenant(VfsClientHandle *vfsClientHandle, const char *clusterName,
                                           const char *tenantName)
{
    if (vfsClientHandle == NULL) {
        vfsClientHandle = g_staticLocalVfsClientHandle;
    }
    if (unlikely(vfsClientHandle == NULL || clusterName == NULL || tenantName == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (vfsClientHandle->ops->createTenant == NULL) {
        return VFS_ERROR_OPERATION_NOT_SUPPORT;
    }
    return vfsClientHandle->ops->createTenant(vfsClientHandle->clientHandle, clusterName, tenantName);
}

UTILS_EXPORT ErrorCode DeleteStoreTenant(VfsClientHandle *vfsClientHandle, const char *clusterName,
                                           const char *tenantName, uint64_t attr)
{
    if (vfsClientHandle == NULL) {
        vfsClientHandle = g_staticLocalVfsClientHandle;
    }
    if (unlikely(vfsClientHandle == NULL || clusterName == NULL || tenantName == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (vfsClientHandle->ops->deleteTenant == NULL) {
        return VFS_ERROR_OPERATION_NOT_SUPPORT;
    }
    return vfsClientHandle->ops->deleteTenant(vfsClientHandle->clientHandle, clusterName, tenantName, attr);
}

UTILS_EXPORT ErrorCode CreateVfs(VfsClientHandle *vfsClientHandle, const char *tenantName, const char *vfsName,
                                 uint64_t attrFlags)
{
    if (unlikely(!g_vfsModuleIsInit)) {
        return VFS_ERROR_VFS_MODULE_NOT_INIT;
    }
    if (vfsClientHandle == NULL) {
        vfsClientHandle = g_staticLocalVfsClientHandle;
    }
    if (unlikely(vfsClientHandle == NULL || tenantName == NULL || vfsName == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (vfsClientHandle->ops->createVfs == NULL) {
        return VFS_ERROR_OPERATION_NOT_SUPPORT;
    }
    return vfsClientHandle->ops->createVfs(vfsClientHandle->clientHandle, tenantName, vfsName, attrFlags);
}

UTILS_EXPORT ErrorCode DropVfs(VfsClientHandle *vfsClientHandle, const char *tenantName, const char *vfsName)
{
    if (unlikely(!g_vfsModuleIsInit)) {
        return VFS_ERROR_VFS_MODULE_NOT_INIT;
    }
    if (vfsClientHandle == NULL) {
        vfsClientHandle = g_staticLocalVfsClientHandle;
    }
    if (unlikely(vfsClientHandle == NULL || tenantName == NULL || vfsName == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (vfsClientHandle->ops->dropVfs == NULL) {
        return VFS_ERROR_OPERATION_NOT_SUPPORT;
    }
    return vfsClientHandle->ops->dropVfs(vfsClientHandle->clientHandle, tenantName, vfsName, VFS_DEFAULT_DELETE_FLAG);
}

UTILS_EXPORT ErrorCode MountVfs(VfsClientHandle *vfsClientHandle, const char *tenantName, const char *vfsName,
                                VirtualFileSystem **vfs)
{
    if (unlikely(!g_vfsModuleIsInit)) {
        return VFS_ERROR_VFS_MODULE_NOT_INIT;
    }
    if (vfsClientHandle == NULL) {
        vfsClientHandle = g_staticLocalVfsClientHandle;
    }
    if (unlikely(vfsClientHandle == NULL || tenantName == NULL || vfsName == NULL || vfs == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }

    VirtualFileSystem *curVfs = (VirtualFileSystem *)VfsMemAlloc(g_vfsMemAllocatorPtr, sizeof(VirtualFileSystem));
    if (unlikely(curVfs == NULL)) {
        return VFS_ERROR_OUT_OF_MEMORY;
    }
    if (vfsClientHandle->ops->mountVfs == NULL) {
        VfsMemFree(g_vfsMemAllocatorPtr, curVfs);
        return VFS_ERROR_OPERATION_NOT_SUPPORT;
    }

    ErrorCode rc = vfsClientHandle->ops->mountVfs(vfsClientHandle->clientHandle, tenantName, vfsName,
        &(curVfs->vfsHandle));
    if (unlikely(rc != ERROR_SYS_OK)) {
        VfsMemFree(g_vfsMemAllocatorPtr, curVfs);
        return rc;
    }

    curVfs->ops = vfsClientHandle->ops;
    curVfs->isStaticVfs = false;
    *vfs = curVfs;
    return ERROR_SYS_OK;
}

UTILS_EXPORT ErrorCode UnmountVfs(VirtualFileSystem *vfs)
{
    if (unlikely(!g_vfsModuleIsInit)) {
        return VFS_ERROR_VFS_MODULE_NOT_INIT;
    }
    if (unlikely(vfs == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }

    if (unlikely(vfs->isStaticVfs)) {
        return VFS_ERROR_UNMOUNT_STATIC_VFS_FAIL;
    }

    if (vfs->ops->unmountVfs == NULL) {
        return VFS_ERROR_OPERATION_NOT_SUPPORT;
    }

    ErrorCode rc = vfs->ops->unmountVfs(vfs->vfsHandle);
    if (unlikely(rc != ERROR_SYS_OK)) {
        return rc;
    }

    VfsMemFree(g_vfsMemAllocatorPtr, vfs);
    return ERROR_SYS_OK;
}

UTILS_EXPORT ErrorCode VfsControl(VfsClientHandle *vfsClientHandle, const char *tenantName, int cmd,
                                  VfsControlInfo *vfsControlInfo, uint32_t count)
{
    if (unlikely(!g_vfsModuleIsInit)) {
        return VFS_ERROR_VFS_MODULE_NOT_INIT;
    }
    if (vfsClientHandle == NULL) {
        vfsClientHandle = g_staticLocalVfsClientHandle;
    }
    if (unlikely(vfsClientHandle == NULL || tenantName == NULL || vfsControlInfo == NULL || count == 0)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (vfsClientHandle->ops->vfsControl == NULL) {
        return VFS_ERROR_OPERATION_NOT_SUPPORT;
    }
    return vfsClientHandle->ops->vfsControl(vfsClientHandle->clientHandle, tenantName, cmd, vfsControlInfo, count);
}

UTILS_EXPORT ErrorCode CreateSnapshot(VfsClientHandle *vfsClientHandle, const char *tenantName, const char *vfsName,
                                        const char *snapshotName, int64_t flags)
{
    if (unlikely(!g_vfsModuleIsInit)) {
        return VFS_ERROR_VFS_MODULE_NOT_INIT;
    }
    if (vfsClientHandle == NULL) {
        vfsClientHandle = g_staticLocalVfsClientHandle;
    }
    if (unlikely(vfsClientHandle == NULL || tenantName == NULL || vfsName == NULL || snapshotName == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (vfsClientHandle->ops->createSnapshot == NULL) {
        return VFS_ERROR_OPERATION_NOT_SUPPORT;
    }
    return vfsClientHandle->ops->createSnapshot(vfsClientHandle->clientHandle, tenantName, vfsName, snapshotName,
        flags);
}

UTILS_EXPORT ErrorCode DropSnapshot(VfsClientHandle *vfsClientHandle, const char *tenantName, const char *vfsName,
                                        const char *snapshotName)
{
    if (unlikely(!g_vfsModuleIsInit)) {
        return VFS_ERROR_VFS_MODULE_NOT_INIT;
    }
    if (vfsClientHandle == NULL) {
        vfsClientHandle = g_staticLocalVfsClientHandle;
    }
    if (unlikely(vfsClientHandle == NULL || tenantName == NULL || vfsName == NULL || snapshotName == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (vfsClientHandle->ops->dropSnapshot == NULL) {
        return VFS_ERROR_OPERATION_NOT_SUPPORT;
    }
    return vfsClientHandle->ops->dropSnapshot(vfsClientHandle->clientHandle, tenantName, vfsName, snapshotName);
}

UTILS_EXPORT ErrorCode RollbackSnapshot(VfsClientHandle *vfsClientHandle, const char *tenantName, const char *vfsName,
                                        const char *snapshotName)
{
    if (unlikely(!g_vfsModuleIsInit)) {
        return VFS_ERROR_VFS_MODULE_NOT_INIT;
    }
    if (vfsClientHandle == NULL) {
        vfsClientHandle = g_staticLocalVfsClientHandle;
    }
    if (unlikely(vfsClientHandle == NULL || tenantName == NULL || vfsName == NULL || snapshotName == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (vfsClientHandle->ops->rollbackSnapshot == NULL) {
        return VFS_ERROR_OPERATION_NOT_SUPPORT;
    }
    return vfsClientHandle->ops->rollbackSnapshot(vfsClientHandle->clientHandle, tenantName, vfsName, snapshotName);
}

UTILS_EXPORT ErrorCode CreateStoreSpace(VfsClientHandle *vfsClientHandle, const char *tenantName,
                                        const char **storeSpaceNames, const StoreSpaceAttr *attrs, uint32 count)
{
    if (unlikely(!g_vfsModuleIsInit)) {
        return VFS_ERROR_VFS_MODULE_NOT_INIT;
    }
    if (vfsClientHandle == NULL) {
        vfsClientHandle = g_staticLocalVfsClientHandle;
    }
    if (unlikely(vfsClientHandle == NULL || tenantName == NULL || storeSpaceNames == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (vfsClientHandle->ops->createStoreSpace == NULL) {
        return VFS_ERROR_OPERATION_NOT_SUPPORT;
    }
    return vfsClientHandle->ops->createStoreSpace(vfsClientHandle->clientHandle, tenantName, storeSpaceNames, attrs,
        count);
}

UTILS_EXPORT ErrorCode DeleteStoreSpace(VfsClientHandle *vfsClientHandle, const char *tenantName,
                                        const char **storeSpaceNames, uint32 count)
{
    if (unlikely(!g_vfsModuleIsInit)) {
        return VFS_ERROR_VFS_MODULE_NOT_INIT;
    }
    if (vfsClientHandle == NULL) {
        vfsClientHandle = g_staticLocalVfsClientHandle;
    }
    if (unlikely(vfsClientHandle == NULL || tenantName == NULL || storeSpaceNames == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (vfsClientHandle->ops->deleteStoreSpace == NULL) {
        return VFS_ERROR_OPERATION_NOT_SUPPORT;
    }
    return vfsClientHandle->ops->deleteStoreSpace(vfsClientHandle->clientHandle, tenantName, storeSpaceNames, count);
}

UTILS_EXPORT ErrorCode UpdateStoreSpace(VfsClientHandle *vfsClientHandle, const char *tenantName,
                                           const char **storeSpaceNames, const StoreSpaceAttr *attrs, uint32 count)
{
    if (unlikely(!g_vfsModuleIsInit)) {
        return VFS_ERROR_VFS_MODULE_NOT_INIT;
    }
    if (vfsClientHandle == NULL) {
        vfsClientHandle = g_staticLocalVfsClientHandle;
    }
    if (unlikely(vfsClientHandle == NULL || tenantName == NULL || storeSpaceNames == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (vfsClientHandle->ops->updateStoreSpace == NULL) {
        return VFS_ERROR_OPERATION_NOT_SUPPORT;
    }
    return vfsClientHandle->ops->updateStoreSpace(vfsClientHandle->clientHandle, tenantName, storeSpaceNames, attrs,
        count);
}

UTILS_EXPORT ErrorCode QueryStoreSpaceAttr(VfsClientHandle *vfsClientHandle, const char *tenantName,
                                           const char **storeSpaceNames, StoreSpaceAttr *attrs, uint32 count)
{
    if (unlikely(!g_vfsModuleIsInit)) {
        return VFS_ERROR_VFS_MODULE_NOT_INIT;
    }
    if (vfsClientHandle == NULL) {
        vfsClientHandle = g_staticLocalVfsClientHandle;
    }
    if (unlikely(vfsClientHandle == NULL || tenantName == NULL || storeSpaceNames == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (vfsClientHandle->ops->queryStoreSpaceAttr == NULL) {
        return VFS_ERROR_OPERATION_NOT_SUPPORT;
    }
    return vfsClientHandle->ops->queryStoreSpaceAttr(vfsClientHandle->clientHandle, tenantName, storeSpaceNames, attrs,
        count);
}

UTILS_EXPORT ErrorCode QueryStoreSpaceUsedSize(VfsClientHandle *vfsClientHandle, const char *tenantName,
                                               const char **storeSpaceNames, uint64 *usedSizes, uint32 count)
{
    if (unlikely(!g_vfsModuleIsInit)) {
        return VFS_ERROR_VFS_MODULE_NOT_INIT;
    }
    if (vfsClientHandle == NULL) {
        vfsClientHandle = g_staticLocalVfsClientHandle;
    }
    if (unlikely(vfsClientHandle == NULL || tenantName == NULL || storeSpaceNames == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (vfsClientHandle->ops->queryStoreSpaceUsedSize == NULL) {
        return VFS_ERROR_OPERATION_NOT_SUPPORT;
    }
    return vfsClientHandle->ops->queryStoreSpaceUsedSize(vfsClientHandle->clientHandle, tenantName, storeSpaceNames,
        usedSizes, count);
}

UTILS_EXPORT ErrorCode Create(VirtualFileSystem *vfs, const char *fileName, FileParameter fileParameter,
                              FileDescriptor **fd)
{
    if (unlikely(!g_vfsModuleIsInit)) {
        return VFS_ERROR_VFS_MODULE_NOT_INIT;
    }
    if (unlikely(vfs == NULL || fd == NULL || fileName == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }

    FileDescriptor *curFd = (FileDescriptor *)VfsMemAlloc(g_vfsMemAllocatorPtr, sizeof(FileDescriptor));
    if (unlikely(curFd == NULL)) {
        return VFS_ERROR_OUT_OF_MEMORY;
    }
    if (vfs->ops->createFile == NULL) {
        VfsMemFree(g_vfsMemAllocatorPtr, curFd);
        return VFS_ERROR_OPERATION_NOT_SUPPORT;
    }
    ErrorCode rc = vfs->ops->createFile(vfs->vfsHandle, fileName, &fileParameter, &(curFd->fileHandle));
    if (unlikely(rc != ERROR_SYS_OK)) {
        VfsMemFree(g_vfsMemAllocatorPtr, curFd);
        return rc;
    }
    curFd->ops = vfs->ops;
    *fd = curFd;
    return ERROR_SYS_OK;
}

UTILS_EXPORT ErrorCode Remove(VirtualFileSystem *vfs, const char *fileName)
{
    if (unlikely(!g_vfsModuleIsInit)) {
        return VFS_ERROR_VFS_MODULE_NOT_INIT;
    }
    if (unlikely(vfs == NULL || fileName == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (vfs->ops->removeFile == NULL) {
        return VFS_ERROR_OPERATION_NOT_SUPPORT;
    }
    return vfs->ops->removeFile(vfs->vfsHandle, fileName);
}

UTILS_EXPORT ErrorCode FileIsExist(VirtualFileSystem *vfs, const char *fileName, bool *out)
{
    if (unlikely(!g_vfsModuleIsInit)) {
        return VFS_ERROR_VFS_MODULE_NOT_INIT;
    }
    if (unlikely(vfs == NULL || fileName == NULL || out == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (vfs->ops->fileIsExist == NULL) {
        return VFS_ERROR_OPERATION_NOT_SUPPORT;
    }
    return vfs->ops->fileIsExist(vfs->vfsHandle, fileName, out);
}

UTILS_EXPORT ErrorCode Open(VirtualFileSystem *vfs, const char *fileName, int flags, FileDescriptor **fd)
{
    if (unlikely(!g_vfsModuleIsInit)) {
        return VFS_ERROR_VFS_MODULE_NOT_INIT;
    }
    if (unlikely(vfs == NULL || fileName == NULL || fd == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }

    FileDescriptor *curFd = (FileDescriptor *)VfsMemAlloc(g_vfsMemAllocatorPtr, sizeof(FileDescriptor));
    if (unlikely(curFd == NULL)) {
        return VFS_ERROR_OUT_OF_MEMORY;
    }
    if (vfs->ops->openFile == NULL) {
        VfsMemFree(g_vfsMemAllocatorPtr, curFd);
        return VFS_ERROR_OPERATION_NOT_SUPPORT;
    }
    ErrorCode rc = vfs->ops->openFile(vfs->vfsHandle, fileName, flags, &(curFd->fileHandle));
    if (unlikely(rc != ERROR_SYS_OK)) {
        VfsMemFree(g_vfsMemAllocatorPtr, curFd);
        return rc;
    }
    curFd->ops = vfs->ops;
    *fd = curFd;
    return ERROR_SYS_OK;
}

UTILS_EXPORT ErrorCode OpenSnapshot(VirtualFileSystem *vfs, FileOpenParam openPara, FileDescriptor **fd)
{
    if (unlikely(!g_vfsModuleIsInit)) {
        return VFS_ERROR_VFS_MODULE_NOT_INIT;
    }
    if (unlikely(vfs == NULL || openPara.filePath == NULL || openPara.snapshotName == NULL || fd == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    FileDescriptor *curFd = (FileDescriptor *)VfsMemAlloc(g_vfsMemAllocatorPtr, sizeof(FileDescriptor));
    if (unlikely(curFd == NULL)) {
        return VFS_ERROR_OUT_OF_MEMORY;
    }
    if (vfs->ops->openFileSnapshot == NULL) {
        VfsMemFree(g_vfsMemAllocatorPtr, curFd);
        return VFS_ERROR_OPERATION_NOT_SUPPORT;
    }
    ErrorCode rc = vfs->ops->openFileSnapshot(vfs->vfsHandle, &openPara, &(curFd->fileHandle));
    if (unlikely(rc != ERROR_SYS_OK)) {
        VfsMemFree(g_vfsMemAllocatorPtr, curFd);
        return rc;
    }
    curFd->ops = vfs->ops;
    *fd = curFd;
    return ERROR_SYS_OK;
}

UTILS_EXPORT ErrorCode RenameFile(VirtualFileSystem *vfs, const char *srcFileName, const char *destFileName)
{
    if (unlikely(!g_vfsModuleIsInit)) {
        return VFS_ERROR_VFS_MODULE_NOT_INIT;
    }
    if (unlikely(vfs == NULL || srcFileName == NULL || destFileName == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (vfs->ops->renameFile == NULL) {
        return VFS_ERROR_OPERATION_NOT_SUPPORT;
    }

    return vfs->ops->renameFile(vfs->vfsHandle, srcFileName, destFileName);
}

UTILS_EXPORT ErrorCode Close(FileDescriptor *fd)
{
    if (unlikely(!g_vfsModuleIsInit)) {
        return VFS_ERROR_VFS_MODULE_NOT_INIT;
    }
    if (unlikely(fd == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (unlikely(fd->ops == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }

    if (fd->ops->close == NULL) {
        return VFS_ERROR_OPERATION_NOT_SUPPORT;
    }

    ErrorCode rc = fd->ops->close(fd->fileHandle);
    if (unlikely(rc != ERROR_SYS_OK)) {
        return rc;
    }

    VfsMemFree(g_vfsMemAllocatorPtr, fd);
    return ERROR_SYS_OK;
}

UTILS_EXPORT ErrorCode Fsync(FileDescriptor *fd)
{
    if (unlikely(!g_vfsModuleIsInit)) {
        return VFS_ERROR_VFS_MODULE_NOT_INIT;
    }
    if (unlikely(fd == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (fd->ops->fsync == NULL) {
        return VFS_ERROR_OPERATION_NOT_SUPPORT;
    }
    return fd->ops->fsync(fd->fileHandle);
}

UTILS_EXPORT ErrorCode FileSeek(FileDescriptor *fd, int64_t offset, int seekFlag, int64_t *newPos)
{
    if (unlikely(!g_vfsModuleIsInit)) {
        return VFS_ERROR_VFS_MODULE_NOT_INIT;
    }
    if (unlikely(fd == NULL || newPos == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (fd->ops->fileSeek == NULL) {
        return VFS_ERROR_OPERATION_NOT_SUPPORT;
    }
    return fd->ops->fileSeek(fd->fileHandle, offset, seekFlag, newPos);
}

UTILS_EXPORT ErrorCode Rewind(FileDescriptor *fd)
{
    if (unlikely(!g_vfsModuleIsInit)) {
        return VFS_ERROR_VFS_MODULE_NOT_INIT;
    }
    if (unlikely(fd == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (fd->ops->rewind == NULL) {
        return VFS_ERROR_OPERATION_NOT_SUPPORT;
    }
    return fd->ops->rewind(fd->fileHandle);
}

UTILS_EXPORT ErrorCode Read(FileDescriptor *fd, void *buf, uint64_t count, int64_t *readSize)
{
    if (unlikely(!g_vfsModuleIsInit)) {
        return VFS_ERROR_VFS_MODULE_NOT_INIT;
    }
    if (unlikely(fd == NULL || buf == NULL || readSize == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (fd->ops->read == NULL) {
        return VFS_ERROR_OPERATION_NOT_SUPPORT;
    }
    return fd->ops->read(fd->fileHandle, buf, count, readSize);
}

UTILS_EXPORT ErrorCode Pread(FileDescriptor *fd, void *buf, uint64_t count, int64_t offset, int64_t *readSize)
{
    if (unlikely(!g_vfsModuleIsInit)) {
        return VFS_ERROR_VFS_MODULE_NOT_INIT;
    }
    if (unlikely(fd == NULL || buf == NULL || readSize == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (fd->ops->pread == NULL) {
        return VFS_ERROR_OPERATION_NOT_SUPPORT;
    }
    return fd->ops->pread(fd->fileHandle, buf, count, offset, readSize);
}

UTILS_EXPORT ErrorCode PreadAsync(FileDescriptor *fd, void *buf, uint64_t count, int64_t offset,
                                  const AsyncIoContext *aioContext)
{
    if (unlikely(!g_vfsModuleIsInit)) {
        return VFS_ERROR_VFS_MODULE_NOT_INIT;
    }
    if (unlikely(fd == NULL || buf == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (fd->ops->preadAsync == NULL) {
        return VFS_ERROR_OPERATION_NOT_SUPPORT;
    }
    return fd->ops->preadAsync(fd->fileHandle, buf, count, offset, aioContext);
}

UTILS_EXPORT ErrorCode SnapshotPread(FileDescriptor *fd, void *buf, uint64_t count, int64_t offset,
                                     DiffContents *diffContents)
{
    if (unlikely(!g_vfsModuleIsInit)) {
        return VFS_ERROR_VFS_MODULE_NOT_INIT;
    }
    if (unlikely(fd == NULL || buf == NULL || diffContents == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (fd->ops->snapshotPread == NULL) {
        return VFS_ERROR_OPERATION_NOT_SUPPORT;
    }
    return fd->ops->snapshotPread(fd->fileHandle, buf, count, offset, diffContents);
}

UTILS_EXPORT ErrorCode WriteSync(FileDescriptor *fd, const void *buf, uint64_t count, int64_t *writeSize)
{
    if (unlikely(!g_vfsModuleIsInit)) {
        return VFS_ERROR_VFS_MODULE_NOT_INIT;
    }
    if (unlikely(fd == NULL || buf == NULL || writeSize == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (fd->ops->writeSync == NULL) {
        return VFS_ERROR_OPERATION_NOT_SUPPORT;
    }
    return fd->ops->writeSync(fd->fileHandle, buf, count, writeSize);
}

UTILS_EXPORT ErrorCode PwriteSync(FileDescriptor *fd, const void *buf, uint64_t count, int64_t offset,
                                  int64_t *writeSize)
{
    if (unlikely(!g_vfsModuleIsInit)) {
        return VFS_ERROR_VFS_MODULE_NOT_INIT;
    }
    if (unlikely(fd == NULL || buf == NULL || writeSize == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (fd->ops->pwriteSync == NULL) {
        return VFS_ERROR_OPERATION_NOT_SUPPORT;
    }
    return fd->ops->pwriteSync(fd->fileHandle, buf, count, offset, writeSize);
}

UTILS_EXPORT ErrorCode SnapshotPwriteSync(FileDescriptor *fd, const void *buf, uint64_t count, int64_t offset,
                                          int64_t *writeSize)
{
    if (unlikely(!g_vfsModuleIsInit)) {
        return VFS_ERROR_VFS_MODULE_NOT_INIT;
    }
    if (unlikely(fd == NULL || buf == NULL || writeSize == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (fd->ops->pwriteSync == NULL) {
        return VFS_ERROR_OPERATION_NOT_SUPPORT;
    }
    return fd->ops->snapshotPwriteSync(fd->fileHandle, buf, count, offset, writeSize);
}

UTILS_EXPORT ErrorCode WriteAsync(FileDescriptor *fd, const void *buf, uint64_t count, const AsyncIoContext *aioContext)
{
    if (unlikely(!g_vfsModuleIsInit)) {
        return VFS_ERROR_VFS_MODULE_NOT_INIT;
    }
    if (unlikely(fd == NULL || buf == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (fd->ops->writeAsync == NULL) {
        return VFS_ERROR_OPERATION_NOT_SUPPORT;
    }
    return fd->ops->writeAsync(fd->fileHandle, buf, count, aioContext);
}

UTILS_EXPORT ErrorCode PwriteAsync(FileDescriptor *fd, const void *buf, uint64_t count, int64_t offset,
                                   const AsyncIoContext *aioContext)
{
    if (unlikely(!g_vfsModuleIsInit)) {
        return VFS_ERROR_VFS_MODULE_NOT_INIT;
    }
    if (unlikely(fd == NULL || buf == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (fd->ops->pwriteAsync == NULL) {
        return VFS_ERROR_OPERATION_NOT_SUPPORT;
    }
    return fd->ops->pwriteAsync(fd->fileHandle, buf, count, offset, aioContext);
}

UTILS_EXPORT ErrorCode SnapshotPwriteAsync(FileDescriptor *fd, const void *buf, uint64_t count, int64_t offset,
                                           const AsyncIoContext *aioContext)
{
    if (unlikely(!g_vfsModuleIsInit)) {
        return VFS_ERROR_VFS_MODULE_NOT_INIT;
    }
    if (unlikely(fd == NULL || buf == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (fd->ops->pwriteAsync == NULL) {
        return VFS_ERROR_OPERATION_NOT_SUPPORT;
    }
    return fd->ops->snapshotPwriteAsync(fd->fileHandle, buf, count, offset, aioContext);
}

UTILS_EXPORT ErrorCode Extend(FileDescriptor *fd, int64_t length)
{
    if (unlikely(!g_vfsModuleIsInit)) {
        return VFS_ERROR_VFS_MODULE_NOT_INIT;
    }
    if (unlikely(fd == NULL || length < 0)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (fd->ops->extend == NULL) {
        return VFS_ERROR_OPERATION_NOT_SUPPORT;
    }
    return fd->ops->extend(fd->fileHandle, length);
}

UTILS_EXPORT ErrorCode Truncate(FileDescriptor *fd, int64_t length)
{
    if (unlikely(!g_vfsModuleIsInit)) {
        return VFS_ERROR_VFS_MODULE_NOT_INIT;
    }
    if (unlikely(fd == NULL || length < 0)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (fd->ops == NULL) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (fd->ops->truncate == NULL) {
        return VFS_ERROR_OPERATION_NOT_SUPPORT;
    }
    return fd->ops->truncate(fd->fileHandle, length);
}

UTILS_EXPORT ErrorCode GetSize(FileDescriptor *fd, int64_t *fileSize)
{
    if (unlikely(!g_vfsModuleIsInit)) {
        return VFS_ERROR_VFS_MODULE_NOT_INIT;
    }
    if (unlikely(fd == NULL || fileSize == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (fd->ops->getSize == NULL) {
        return VFS_ERROR_OPERATION_NOT_SUPPORT;
    }
    return fd->ops->getSize(fd->fileHandle, fileSize);
}

UTILS_EXPORT ErrorCode FileControl(FileDescriptor *fd, int cmd, const FileControlInfo *fileControlInfo)
{
    if (unlikely(!g_vfsModuleIsInit)) {
        return VFS_ERROR_VFS_MODULE_NOT_INIT;
    }
    if (unlikely(fd == NULL || fileControlInfo == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    AdapterFileControlInfo controlInfo;
    if (memcpy_s(&controlInfo, sizeof(AdapterFileControlInfo), fileControlInfo, sizeof(FileControlInfo)) != 0) {
        return VFS_ERROR_SECURE_FUNCTION_FAIL;
    }
    if (cmd == SET_FILE_FLUSH_CALLBACK) {
        controlInfo.flushCallbackInfo.fd = (void *)fd;
    }
    if (fd->ops->fileControl == NULL) {
        return VFS_ERROR_OPERATION_NOT_SUPPORT;
    }
    return fd->ops->fileControl(fd->fileHandle, cmd, &controlInfo);
}

UTILS_EXPORT ErrorCode LockFile(FileDescriptor *fd, int64_t startPos, int64_t len, int lockMode, uint32_t timeout)
{
    if (unlikely(!g_vfsModuleIsInit)) {
        return VFS_ERROR_VFS_MODULE_NOT_INIT;
    }
    if (unlikely(fd == NULL || startPos < 0)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (fd->ops->lockFile == NULL) {
        return VFS_ERROR_OPERATION_NOT_SUPPORT;
    }
    return fd->ops->lockFile(fd->fileHandle, startPos, len, lockMode, timeout);
}

UTILS_EXPORT ErrorCode TryLockFile(FileDescriptor *fd, int64_t startPos, int64_t len, int lockMode)
{
    if (unlikely(!g_vfsModuleIsInit)) {
        return VFS_ERROR_VFS_MODULE_NOT_INIT;
    }
    if (unlikely(fd == NULL || startPos < 0)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (fd->ops->tryLockFile == NULL) {
        return VFS_ERROR_OPERATION_NOT_SUPPORT;
    }
    return fd->ops->tryLockFile(fd->fileHandle, startPos, len, lockMode);
}

UTILS_EXPORT ErrorCode UnlockFile(FileDescriptor *fd, int64_t startPos, int64_t len)
{
    if (unlikely(!g_vfsModuleIsInit)) {
        return VFS_ERROR_VFS_MODULE_NOT_INIT;
    }
    if (unlikely(fd == NULL || startPos < 0)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (fd->ops->unlockFile == NULL) {
        return VFS_ERROR_OPERATION_NOT_SUPPORT;
    }
    return fd->ops->unlockFile(fd->fileHandle, startPos, len);
}

UTILS_EXPORT ErrorCode FDataSync(FileDescriptor *fd)
{
    if (unlikely(!g_vfsModuleIsInit)) {
        return VFS_ERROR_VFS_MODULE_NOT_INIT;
    }
    if (unlikely(fd == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    if (fd->ops->fDataSync == NULL) {
        return VFS_ERROR_OPERATION_NOT_SUPPORT;
    }
    return fd->ops->fDataSync(fd->fileHandle);
}

ErrorCode DestoryFileLock(VirtualFileSystem *vfs, const char *fileName, FileDescriptor *lockFd)
{
    ErrorCode errCode = ERROR_SYS_OK;

    if (unlikely(!g_vfsModuleIsInit)) {
        return VFS_ERROR_VFS_MODULE_NOT_INIT;
    }

    if (lockFd == NULL) {
        return VFS_ERROR_FILE_LOCK_INVALID;
    }

    errCode = Close(lockFd);
    if (errCode != ERROR_SYS_OK) {
        return errCode;
    }

    errCode = Remove(vfs, fileName);
    if (errCode == VFS_ERROR_REMOVE_FILE_NOT_EXIST) {
        return ERROR_SYS_OK;
    }

    return errCode;
}

ErrorCode InitFileLock(VirtualFileSystem *vfs, const char *fileName, const char *storeSpaceName,
                       FileDescriptor **lockFd)
{
    if (unlikely(!g_vfsModuleIsInit)) {
        return VFS_ERROR_VFS_MODULE_NOT_INIT;
    }

    if (unlikely(storeSpaceName == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }

    FileParameter fileParameter;
    int ret = strcpy_s(fileParameter.storeSpaceName, sizeof(fileParameter.storeSpaceName), storeSpaceName);
    if (ret != EOK) {
        return VFS_ERROR_SECURE_FUNCTION_FAIL;
    }
    fileParameter.streamId = VFS_DEFAULT_FILE_STREAM_ID;
    fileParameter.flag = IN_PLACE_WRITE_FILE;
    fileParameter.fileSubType = TEMP_FILE_TYPE;
    fileParameter.rangeSize = BLCKSZ;
    fileParameter.maxSize = BLCKSZ;
    fileParameter.recycleTtl = 0;
    fileParameter.mode = FILE_READ_AND_WRITE_MODE;
    fileParameter.isReplayWrite = false;

    ErrorCode errCode = Open(vfs, fileName, FILE_READ_ONLY_FLAG, lockFd);
    if (errCode == VFS_ERROR_OPEN_FILE_NOT_EXIST) {
        errCode = Create(vfs, fileName, fileParameter, lockFd);
        if (errCode != ERROR_SYS_OK && errCode != VFS_ERROR_FILE_IS_EXIST) {
            return errCode;
        }

        errCode = Extend(*lockFd, (int64_t)fileParameter.maxSize);
        if (errCode != ERROR_SYS_OK) {
            (void)DestoryFileLock(vfs, fileName, *lockFd);
            return errCode;
        }
    }

    return errCode;
}

ErrorCode FileLock(FileDescriptor *lockFd)
{
    return TryLockFile(lockFd, 0, BLCKSZ, FILE_EXCLUSIVE_LOCK);
}

ErrorCode FileUnLock(FileDescriptor *lockFd)
{
    return UnlockFile(lockFd, 0, BLCKSZ);
}

static ErrorCode OpenSourceFile(VirtualFileSystem *vfs, const char *path, FileDescriptor **sourceFd)
{
    ErrorCode errCode = ERROR_SYS_OK;
    /* Open source file in read mode */
    errCode = Open(vfs, path, FILE_READ_ONLY_FLAG, sourceFd);
    if (errCode != ERROR_SYS_OK) {
        return errCode;
    }

    return ERROR_SYS_OK;
}

static ErrorCode CheckSourceFileValidity(FileDescriptor *sourceFd, FileParameter fileParameter, int64_t *sourceFileSize)
{
    ErrorCode errCode = GetSize(sourceFd, sourceFileSize);
    if (errCode != ERROR_SYS_OK) {
        return errCode;
    }

    /* Source file is empty or invalid */
    if (*sourceFileSize <= 0 || *sourceFileSize > (int64_t)fileParameter.maxSize) {
        errCode = VFS_ERROR_PARAMETERS_INVALID;
        return errCode;
    }

    return ERROR_SYS_OK;
}

static ErrorCode OpenTargetFile(VirtualFileSystem *vfs, const char *path, FileParameter fileParameter,
                                FileDescriptor **targetFd)
{
    ErrorCode errCode = ERROR_SYS_OK;
    errCode = Open(vfs, path, FILE_READ_AND_WRITE_FLAG, targetFd);
    if (errCode == VFS_ERROR_OPEN_FILE_NOT_EXIST) {
        /* Create a new target file */
        errCode = Create(vfs, path, fileParameter, targetFd);
        if (errCode != ERROR_SYS_OK) {
            return errCode;
        }
    } else if (errCode != ERROR_SYS_OK) {
        return errCode;
    }

    return errCode;
}

static ErrorCode ReadFromSourceFile(FileDescriptor *sourceFd, int64_t sourceFileSize, char *buffer)
{
    ErrorCode errCode = ERROR_SYS_OK;
    int64_t readPos = 0;
    int64_t readSize;
    while (readPos < sourceFileSize) {
        errCode = Pread(sourceFd, buffer + readPos, (uint64_t)sourceFileSize, readPos, &readSize);
        if (errCode != ERROR_SYS_OK) {
            return errCode;
        }
        readPos += readSize;
    }

    return errCode;
}

static void WriteToTargetFile(FileDescriptor *targetFd, int64_t sourceFileSize, char *buffer)
{
    (void)Rewind(targetFd);

    /* Write source file content to target file */
    ErrorCode errCode = ERROR_SYS_OK;
    int64_t writePos = 0;
    int64_t writeSize;
    while (writePos < sourceFileSize) {
        errCode = PwriteSync(targetFd, buffer + writePos, (uint64_t)sourceFileSize, writePos, &writeSize);
        if (errCode != ERROR_SYS_OK) {
            return;
        }
        writePos += writeSize;
    }
}

UTILS_EXPORT ErrorCode CopyFile(VirtualFileSystem *sourceVfs, const char *sourceFileName, VirtualFileSystem *targetVfs,
                                const char *targetFileName, const char *storeSpaceName)
{
    if (unlikely(!g_vfsModuleIsInit)) {
        return VFS_ERROR_VFS_MODULE_NOT_INIT;
    }

    if (unlikely(sourceVfs == NULL || targetVfs == NULL || storeSpaceName == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }

    FileDescriptor *sourceFd = NULL;
    FileDescriptor *targetFd = NULL;
    char *buffer = NULL;
    ErrorCode errCode = ERROR_SYS_OK;

    FileParameter fileParameter;
    int ret = strcpy_s(fileParameter.storeSpaceName, sizeof(fileParameter.storeSpaceName), storeSpaceName);
    if (ret != EOK) {
        errCode = VFS_ERROR_SECURE_FUNCTION_FAIL;
        return errCode;
    }
    fileParameter.streamId = VFS_DEFAULT_FILE_STREAM_ID;
    fileParameter.flag = APPEND_WRITE_FILE;
    fileParameter.fileSubType = CONFIG_FILE_TYPE;
    fileParameter.rangeSize = RANGE_UNIT;
    fileParameter.maxSize = (uint64_t)64U * BLCKSZ;
    fileParameter.recycleTtl = 0;
    fileParameter.mode = FILE_READ_AND_WRITE_MODE;
    fileParameter.isReplayWrite = false;

    /* Open source config file */
    errCode = OpenSourceFile(sourceVfs, sourceFileName, &sourceFd);
    if (errCode != ERROR_SYS_OK) {
        goto RELEASE_RESOURCE;
    }

    int64_t sourceFileSize = 0;
    errCode = CheckSourceFileValidity(sourceFd, fileParameter, &sourceFileSize);
    if (errCode != ERROR_SYS_OK) {
        goto RELEASE_RESOURCE;
    }

    /* Open target config file */
    errCode = OpenTargetFile(targetVfs, targetFileName, fileParameter, &targetFd);
    if (errCode != ERROR_SYS_OK) {
        goto RELEASE_RESOURCE;
    }

    /* Malloc buffer for copying */
    buffer = (char *)malloc((size_t)sourceFileSize);
    if (buffer == NULL) {
        VfsPrintReleaseLog("Cannot malloc buffer for copy!");
        errCode = ERROR_UTILS_COMMON_NOENOUGH_MEMORY;
        goto RELEASE_RESOURCE;
    }

    errCode = ReadFromSourceFile(sourceFd, sourceFileSize, buffer);
    if (errCode != ERROR_SYS_OK) {
        goto RELEASE_RESOURCE;
    }

    WriteToTargetFile(targetFd, sourceFileSize, buffer);

RELEASE_RESOURCE:
    if (buffer != NULL) {
        free(buffer);
    }

    if (targetFd != NULL) {
        (void)Close(targetFd);
    }

    if (sourceFd != NULL) {
        (void)Close(sourceFd);
    }

    return errCode;
}

UTILS_EXPORT ErrorCode EnableAIO(VirtualFileSystem *vfs, uint16_t maxEvents, uint16_t threadCount,
                                 void (*threadEnterCallback)(void), void (*threadExitCallback)(void))
{
    if (unlikely(vfs == NULL)) {
        return VFS_ERROR_PARAMETERS_INVALID;
    }

    if (vfs->ops->enableAIO == NULL) {
        return ERROR_SYS_OK;
    }

    return vfs->ops->enableAIO(vfs->vfsHandle, maxEvents, threadCount, threadEnterCallback, threadExitCallback);
}
