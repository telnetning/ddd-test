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
 * ---------------------------------------------------------------------------------------
 *
 * dstore_vfs_config.cpp
 *     This file defines the building blocks that bg page writer needs.
 *
 * IDENTIFICATION
 *        src/config/dstore_vfs_config.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "config/dstore_vfs_config.h"
#include <libgen.h>
#include "common/memory/dstore_mctx.h"
#include "common/log/dstore_log.h"
#include "framework/dstore_vfs_adapter.h"
#include "framework/dstore_vfs_interface.h"
#include "errorcode/dstore_framework_error_code.h"
#include "lock/dstore_lwlock.h"
#include "framework/dstore_thread.h"
#include "common/error/dstore_error.h"
#include "framework/dstore_instance.h"

#include <cerrno>
#include "securec.h"

namespace DSTORE {
static char g_vfsClusterName[MAXLIBPATH];
static char g_vfsTenantName[DSTORE_TENANT_NAME_MAX_LEN];

/* Dynamic VFS lib handles. */
static ::VfsLibHandle *g_vfsLibHandle{nullptr};
static std::atomic_uint32_t g_adapterCounter{0};
static LWLock g_vfsLibHandleLock;
UploadVfsClientIdCallback g_uploadVfsClientIdCallback{nullptr};

/* VFS client handles. */
static VfsClientHandleDesc g_vfsClientHandles[MAX_CLUSTER_COUNT] = {
    {nullptr, INVALID_CLUSTER_ID, {0}},
    {nullptr, INVALID_CLUSTER_ID, {0}},
    {nullptr, INVALID_CLUSTER_ID, {0}},
    {nullptr, INVALID_CLUSTER_ID, {0}},
    {nullptr, INVALID_CLUSTER_ID, {0}}};
static Mutex g_vfsClientHandleLock;
VfsInterface::GsscmsCommInfoQueryCallback g_gsscmsCommInfoQueryCallback{nullptr};

// read from GUC.
int g_gssFlushThreadNum = 0;
int g_gssAsyncBatchPageNum = 0;
int g_gssAsyncFlushInterval = 1000;
int g_gssOlcAlgorithm = 0;
int g_gssFileLockTtl = 10000; /* 10s */
static bool g_cipherLoadWorkKey = false;

static void SetPageStoreIntConfigPara(VfsLibHandle *vfsLibHandle, const char *para, int val)
{
    StorageAssert(vfsLibHandle != nullptr);
    char tempBuf[MAX_GUC_INT_LEN];
    int rc = sprintf_s(tempBuf, MAX_GUC_INT_LEN, "%d", val);
    storage_securec_check_ss(rc);

    ::ErrorCode err = SetVfsLibConfig(vfsLibHandle, para, static_cast<const char *>(tempBuf));
    StorageReleasePanic(err != 0, MODULE_FRAMEWORK,
                        ErrMsg("Failed to set %s "
                               "for VFS library, error code = %lld",
                               para, err));
}

static void SetPageStoreStringConfigPara(VfsLibHandle *vfsLibHandle, const char *para, char *val)
{
    StorageAssert(vfsLibHandle != nullptr);

    ::ErrorCode err = SetVfsLibConfig(vfsLibHandle, para, (const char *)val);
    StorageReleasePanic(err != 0, MODULE_FRAMEWORK, ErrMsg("Failed to set String %s "
        "for VFS library, error code = %lld", para, err));
}

void InitVfsClientHandles()
{
    for (uint32 idx = 0; idx < MAX_CLUSTER_COUNT; idx++) {
        g_vfsClientHandles[idx].clusterId = INVALID_CLUSTER_ID;
        g_vfsClientHandles[idx].clientHandle = nullptr;
        errno_t rc = memset_s(g_vfsClientHandles[idx].clusterName, CLUSTER_NAME_MAX_LEN, 0, CLUSTER_NAME_MAX_LEN);
        storage_securec_check(rc, "\0", "\0");
    }
}

void LoadGUCFlexConfig4VFS()
{
    /* set async Flush Dirty Page thread nums in Pagestore */
    SetPageStoreIntConfigPara(g_vfsLibHandle, GSS_FLUSH_THREAD_NUM, g_gssFlushThreadNum);

    SetPageStoreIntConfigPara(g_vfsLibHandle, GSS_ASYNC_BATCH_PAGE_NUM, g_gssAsyncBatchPageNum);

    SetPageStoreIntConfigPara(g_vfsLibHandle, GSS_ASYNC_FLUSH_INTERVAL, g_gssAsyncFlushInterval);

    SetPageStoreIntConfigPara(g_vfsLibHandle, GSS_OLC_ALGORITHM, g_gssOlcAlgorithm);
}

bool IsLoadCipherWorkKey()
{
    return g_cipherLoadWorkKey;
}

VfsLibHandle *SetupTenantIsolation(const TenantConfig *config, const char *datadir)
{
    ::ErrorCode err = ERROR_SYS_OK;
    StorageAssert(config);
    const char *libPath = config->storageConfig.clientLibPath;
    err = LoadVfsLib(libPath, nullptr, &g_vfsLibHandle);
    StorageReleasePanic(err, MODULE_FRAMEWORK, ErrMsg("Failed to load VFS library, error code = %lld", err));

    ::VfsLibParameter testLibPara;
    testLibPara.pageSize = BLCKSZ;
    testLibPara.dbType = 1;
    testLibPara.clientTimeLineId = NODE_TIMELINE_ID_DEFAULT;

    errno_t rc = snprintf_s(testLibPara.storageServerAddr, VFS_LIB_ATTR_LEN, VFS_LIB_ATTR_LEN - 1, "%s", datadir);
    storage_securec_check_ss(rc);
    rc = strcpy_s(testLibPara.clusterName, CLUSTER_NAME_MAX_LEN, config->clusterName);
    storage_securec_check(rc, "\0", "\0");
    err = InitVfsLib(g_vfsLibHandle, &testLibPara);
    if (err != 0 && err != VFS_ERROR_LOCAL_ADAPTER_ALREADY_INIT) {
        StorageReleasePanic(err, MODULE_FRAMEWORK, ErrMsg("Failed to initialize VFS library, error code = %lld", err));
    }
    return g_vfsLibHandle;
}


/**
 * 1) g_adapterCounter+1 when CreateVfs and OpenVFs(Create PDB by user);
 * 2) in TENANT_ISOLATION mode: GaussDB process does not LoadVfsLib when startup, LoadVfsLib happens when
 *  user call CreateVfs; when adapterCounter == 1, it means that pdb created by user remains only 1,
 *  it need to OffLoadVFSLib;
 * 3) in PAGESTORE mode: GaussDB process does LoadVfsLib when startup, as g_adapterCounter == 0,
 * it need to OffLoadVFSLib when process exit.
 * @param storageType
 * @return
 */
bool NeedOffLoadVFSLib(StorageType storageType, uint32_t adapterCounter)
{
    if (storageType == StorageType::TENANT_ISOLATION) {
        return (adapterCounter == 1);
    } else {
        return false;
    }
}

void DynamicUnlinkVFS(::VirtualFileSystem *vfs, const char *vfsName, bool dropData)
{
    StorageAssert(g_storageInstance->GetGuc()->tenantConfig);
    /* Unmount vfs handle. */
    ::ErrorCode retError = UnmountVfs(vfs);
    StorageReleasePanic(retError, MODULE_FRAMEWORK, ErrMsg("Failed to unmount VFS, error code = %lld", retError));

    TenantConfig *tenantCfg = g_storageInstance->GetGuc()->tenantConfig;
    if (tenantCfg == nullptr) {
        ErrLog(DSTORE_PANIC, MODULE_FRAMEWORK, ErrMsg("Guc tenantConfig is nullptr!"));
        return;
    }
    StorageType storageType = tenantCfg->storageConfig.type;

    if (!g_storageInstance->IsBootstrapping() && storageType == StorageType::PAGESTORE) {
        RetStatus ret = IoFencingVFSCollection::GetSingleton()->RemoveActiveVFS(vfsName);
        StorageReleasePanic(ret != DSTORE_SUCC, MODULE_FRAMEWORK, ErrMsg("Failed to remove active vfs: %s", vfsName));
    }

    if (dropData) {
        char *vfsTenantName = tenantCfg->tenantName;
        if (vfsName == nullptr) {
            vfsName = tenantCfg->storageConfig.rootpdbVfsName;
        }

        /* Drop vfs with name */
        retError = DropVfs(GetDefaultVfsClientHandle(), vfsTenantName, vfsName);
        StorageReleasePanic(retError, MODULE_FRAMEWORK, ErrMsg("Failed to drop VFS, error code = %lld", retError));
        ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("[VFS]DropVfs: %s in DynamicUnlinkVFS", vfsName));
    }
    uint32_t adapterCounter = g_adapterCounter.fetch_sub(1);
    if (NeedOffLoadVFSLib(static_cast<StorageType>(storageType), adapterCounter)) {
        RetStatus ret = ReleaseAllStandbyVfsClientHandle();
        StorageReleasePanic(ret == DSTORE_FAIL, MODULE_FRAMEWORK, ErrMsg("Failed to delete standby client handle"));
        /* Offload the sal vfs lib. */
        retError = OffloadVfsLib(g_vfsLibHandle);
        StorageReleasePanic(retError, MODULE_FRAMEWORK,
                            ErrMsg("Failed to offload VFS lib, error code = %lld", retError));
        g_vfsLibHandle = nullptr;

        ErrLog(DSTORE_LOG, MODULE_FRAMEWORK,
               ErrMsg("[VFS]Offload vfs lib when unlink vfs: %s, load count: %d", vfsName, g_adapterCounter.load()));
    }

    ErrLog(DSTORE_LOG, MODULE_FRAMEWORK,
           ErrMsg("[VFS]Dynamic unlink vfs: %s, load count: %d", vfsName, g_adapterCounter.load()));
}

RetStatus DropVfsDataForce(const char *vfsName, PdbId pdbId)
{
    TenantConfig *tenantCfg = g_storageInstance->GetGuc()->tenantConfig;
    if (tenantCfg == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Guc tenantConfig is nullptr!"));
        return DSTORE_FAIL;
    }
    char *vfsTenantName = tenantCfg->tenantName;

    /* Drop vfs with name */
    ::ErrorCode retError = DropVfs(GetDefaultVfsClientHandle(), vfsTenantName, vfsName);
    ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("[DropVfsDataForce]DropVfs: %s in DropVfsDataForce", vfsName));
    if (retError == VFS_ERROR_VFS_NAME_NOT_EXIST) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("[DropVfsDataForce]Drop vfs empty, vfs name: %s", vfsName));
        return DSTORE_SUCC;
    } else if (retError == VFS_ERROR_IO_FENCING_REFUSE) {
        /* In the IO_FENCING scenario, the trustlist needs to be reloaded. */
        VFSAdapter *vfs =
            DstoreNew(g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LONGLIVE)) VFSAdapter(pdbId);
        if (vfs == nullptr) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
                   ErrMsg("[DropVfsDataForce]Alloc vfs adapter failed, vfs name: %s", vfsName));
            return DSTORE_FAIL;
        }
        if (STORAGE_FUNC_FAIL(vfs->Initialize(vfsName))) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
                   ErrMsg("[DropVfsDataForce]vfs initialize failed. vfsName = %s.", vfsName));
            delete vfs;
            return DSTORE_FAIL;
        }
        if (STORAGE_FUNC_FAIL(vfs->Destroy(vfsName))) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
                   ErrMsg("vfs Destroy failed. vfsName = %s.", vfsName));
            delete vfs;
            return DSTORE_FAIL;
        }
        delete vfs;
        retError = DropVfs(GetDefaultVfsClientHandle(), vfsTenantName, vfsName);
        ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("[VFS]DropVfs: %s in DropVfsDataForce2", vfsName));
        if (retError == VFS_ERROR_VFS_NAME_NOT_EXIST) {
            ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK, ErrMsg("[VFS]Drop vfs empty, vfs name: %s", vfsName));
            return DSTORE_SUCC;
        }
    }

    if (retError) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to drop VFS, error code = %lld", retError));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus CreateVFS(::VirtualFileSystem **vfs, const char *vfsName, const uint64 ioFenceFlag)
{
    StorageAssert(vfsName != nullptr);
    StorageAssert(g_storageInstance->GetGuc()->tenantConfig);
    char *vfsTenantName = g_storageInstance->GetGuc()->tenantConfig->tenantName;
    ::ErrorCode retError = CreateVfs(GetDefaultVfsClientHandle(), vfsTenantName, vfsName, ioFenceFlag);
    if (retError) {
        storage_set_error(VFS_ERROR_FAILED_TO_CREATE_VFS);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to create VFS, error code = %lld", retError));
        return DSTORE_FAIL;
    }

    /* Mount vfs with name and get vfs handle */
    retError = MountVfs(GetDefaultVfsClientHandle(), vfsTenantName, vfsName, vfs);
    if (retError) {
        storage_set_error(VFS_ERROR_FAILED_TO_MOUNT_VFS);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to mount VFS, error code = %lld", retError));
        return DSTORE_FAIL;
    }

    g_adapterCounter.fetch_add(1);
    ErrLog(DSTORE_LOG, MODULE_FRAMEWORK,
           ErrMsg("[VFS]Create vfs: %s, load count: %d", vfsName, g_adapterCounter.load()));
    return DSTORE_SUCC;
}


::VfsLibHandle *LoadVfsLibrary(const TenantConfig *config)
{
    if (STORAGE_VAR_NULL(config)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Parameter config is nullptr."));
        return nullptr;
    }
    ::ErrorCode err = LoadVfsLib(config->storageConfig.clientLibPath, nullptr, &g_vfsLibHandle);
    if (err != ERROR_SYS_OK) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to load VFS library, error code = %lld", err));
        return nullptr;
    }
    ::VfsLibParameter testLibPara;
    testLibPara.pageSize = BLCKSZ;
    testLibPara.dbType = 1;
    testLibPara.clientId = config->nodeId;
    testLibPara.localServiceType = PAGESTORE_CM_SERVICE_TYPE;
    testLibPara.clientTimeLineId = NODE_TIMELINE_ID_DEFAULT;

    if (unlikely(config->storageConfig.serverAddresses == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("ServerAddresses is nullptr."));
        return nullptr;
    }
    errno_t rc = strcpy_s(testLibPara.storageServerAddr, VFS_LIB_ATTR_LEN, config->storageConfig.serverAddresses);
    storage_securec_check(rc, "\0", "\0");
    const char *localIp = config->communicationConfig.localConfig.localIp;
    if (unlikely(localIp == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("LocalIp is nullptr."));
        return nullptr;
    }
    rc = strcpy_s(testLibPara.localIp, sizeof(testLibPara.localIp), localIp);
    storage_securec_check(rc, "\0", "\0");
    rc = strcpy_s(testLibPara.clusterName, CLUSTER_NAME_MAX_LEN, config->clusterName);
    storage_securec_check(rc, "\0", "\0");
    LoadGUCFlexConfig4VFS();

    /* set comm auth_type in Pagestore */
    SetPageStoreIntConfigPara(g_vfsLibHandle, GSS_COMM_AUTH_TYPE,
                              static_cast<int>(config->communicationConfig.authType));
    /* set pagestore_file_lock_ttl in Pagestore */
    SetPageStoreIntConfigPara(g_vfsLibHandle, GSS_FILE_LOCK_TTL, g_gssFileLockTtl);

    /* set ssl_path in Pagestore */
    char certFileCopy[DSTORE_MAX_TLS_NAME_LEN]{0};
    errno_t er = strncpy_s(certFileCopy, DSTORE_MAX_TLS_NAME_LEN, config->securityConfig.rpcSsl.certFile,
    strlen(config->securityConfig.rpcSsl.certFile));
    storage_securec_check(er, "\0", "\0");
    SetPageStoreStringConfigPara(g_vfsLibHandle, GSS_SSL_PATH, dirname(certFileCopy));

    err = InitVfsLib(g_vfsLibHandle, &testLibPara);
    if (err != ERROR_SYS_OK) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to initialize VFS library, error code = %lld", err));
        return nullptr;
    }
    /* It is not really elegant, but we need it before dstore's guc inited,
     * so we keep a copy of cluster name in local. */
    rc = strcpy_s(g_vfsClusterName, MAXLIBPATH, config->clusterName);
    storage_securec_check(rc, "\0", "\0");
    rc = strcpy_s(g_vfsTenantName, DSTORE_TENANT_NAME_MAX_LEN, config->tenantName);
    storage_securec_check(rc, "\0", "\0");
    InitVfsClientHandles();
    return g_vfsLibHandle;
}

char *GetVfsClusterName()
{
    return g_vfsClusterName;
}

char *GetVfsTenantName()
{
    return g_vfsTenantName;
}

/* Using in initdb progress main and dump tools.
 * Only set up communication thread pool in initdb to avoid memory leak.
 */
RetStatus DynamicLinkVFS(void *tenantConfig, StorageInstanceType instanceType, uint32 clientId, const char *datadir,
    bool isInitDb)
{
    (void)instanceType;
    (void)clientId;
    (void)isInitDb;
    TenantConfig *tenantCfg = static_cast<TenantConfig *>(tenantConfig);
    if (unlikely(tenantCfg == nullptr)) {
        /* Only support PageStore for now. */
        StorageAssert(g_storageInstance->GetGuc()->tenantConfig);
        tenantCfg = g_storageInstance->GetGuc()->tenantConfig;
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Dynamic link VFS tenant cfg ptr is invalid!"));
    }
    if (tenantCfg == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Guc tenantConfig is nullptr!"));
        return DSTORE_FAIL;
    }
    StorageType storageType = tenantCfg->storageConfig.type;
    if (storageType == StorageType::PAGESTORE) {
        if (instanceType == StorageInstanceType::SINGLE && !tenantCfg->isEmbeddedShareMode) {
        }
        g_vfsLibHandle = LoadVfsLibrary(tenantCfg);
        if (g_vfsLibHandle == nullptr) {
            return DSTORE_FAIL;
        }
    } else if (storageType == StorageType::TENANT_ISOLATION) {
        g_vfsLibHandle = SetupTenantIsolation(tenantCfg, datadir);
    } else {
        /* Only support PageStore for now. */
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
               ErrMsg("Dynamic link VFS type (%d) is invalid!", static_cast<int>(storageType)));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus OpenVFS(const char *tenantName, const char *vfsName, ::VirtualFileSystem **vfs, const uint64 ioFenceFlag)
{
    bool enableIoFencing = (ioFenceFlag == VFS_ENABLE_IO_FENCE_FLAG);
    if (STORAGE_FUNC_FAIL(IoFencingVFSCollection::GetSingleton()->AddActiveVFS(vfsName, enableIoFencing))) {
        ::ErrorCode errCode = StorageGetErrorCode();
        if (errCode != VFS_WARNING_DUPLICATE_ADD_ACTIVE) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
                   ErrMsg("[DR_VFS_ADAPTER] add active vfs failed, tenantName:%s, vfsname:%s.", tenantName, vfsName));
            return DSTORE_FAIL;
        }
    }
#ifndef UT
    /* Just for initDB */
    if (IsTemplate(g_defaultPdbId) || g_storageInstance->GetType() == StorageInstanceType::SINGLE) {
        if (STORAGE_FUNC_FAIL(IoFencingVFSCollection::GetSingleton()->SetIoFencingWhiteList(vfsName))) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
                   ErrMsg("[DR_VFS_ADAPTER] set ioFencingWhiteList failed, tenantName:%s, vfsname:%s.", tenantName,
                          vfsName));
            return DSTORE_FAIL;
        }
    } else {
        if (STORAGE_FUNC_FAIL(IoFencingVFSCollection::GetSingleton()->TriggerIoFencing())) {
            ErrLog(
                DSTORE_ERROR, MODULE_FRAMEWORK,
                ErrMsg("[DR_VFS_ADAPTER] trigger ioFencing failed, tenantName:%s, vfsname:%s.", tenantName, vfsName));
            return DSTORE_FAIL;
        }
    }
#else
    if (STORAGE_FUNC_FAIL(IoFencingVFSCollection::GetSingleton()->TriggerIoFencing())) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
               ErrMsg("[DR_VFS_ADAPTER] trigger ioFencing failed, tenantName:%s, vfsname:%s.", tenantName, vfsName));
        return DSTORE_FAIL;
    }
#endif
    /* Mount vfs with name and get vfs handle */
    ::ErrorCode retError = MountVfs(GetDefaultVfsClientHandle(), tenantName, vfsName, vfs);
    if (unlikely(retError)) {
        storage_set_error(VFS_ERROR_FAILED_TO_MOUNT_VFS);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
               ErrMsg("[VFS]MountVfs tenantName:%s, vfsname:%s, failed %lld\n", tenantName, vfsName, retError));
        return DSTORE_FAIL;
    }
    g_adapterCounter.fetch_add(1);
    ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("[VFS]Open vfs: %s, load count: %d", vfsName, g_adapterCounter.load()));
    return DSTORE_SUCC;
}

RetStatus CloseVFS(const char *vfsName, ::VirtualFileSystem *vfs)
{
    if (STORAGE_FUNC_FAIL(IoFencingVFSCollection::GetSingleton()->RemoveActiveVFS(vfsName))) {
        return DSTORE_FAIL;
    }

    ::ErrorCode retError = UnmountVfs(vfs);
    if (unlikely(retError != ERROR_SYS_OK)) {
        (void)printf("[VFS]UnmountVfs %s failed %lld\n", vfsName, retError);
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

::VfsLibHandle *GetVfsLibHandle()
{
    return g_vfsLibHandle;
}

LWLock *GetVfsHandleLock()
{
    return &g_vfsLibHandleLock;
}

void InitVfsHandleLock()
{
    LWLockInitialize(&g_vfsLibHandleLock, LWLOCK_GROUP_VFS_LIB_HANDLE);
}

/* VFS create a default client handle, nullptr means using the default client handle */
VfsClientHandle *GetDefaultVfsClientHandle()
{
    return nullptr;
}

RetStatus GetClusterServerAddr(uint32 clusterId, AidClusterGsscmsCommInfo *gsscmsCommInfo)
{
    if (g_gsscmsCommInfoQueryCallback == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("GsscmsCommInfoQueryCallback is null."));
        return DSTORE_FAIL;
    }

    ErrLog(DSTORE_LOG, MODULE_PDBREPLICA, ErrMsg("GetGsscmsCommInfo, clsuterId %u", clusterId));

    ErrorCode result = 0;
    uint32 maxRetryTimes = 30;
    uint32 curRetryTime = 0;
    const int32 retrySleepTime = 1000000; /* 1s */
    do {
        result = g_gsscmsCommInfoQueryCallback(clusterId, gsscmsCommInfo);
        if (result != 0) {
            ErrLog(
                DSTORE_WARNING, MODULE_PDBREPLICA,
                ErrMsg("Failed to get gsscms comm info, error code %lld, cur retry times %u.", result, curRetryTime));
            GaussUsleep(retrySleepTime);
        }
        curRetryTime++;
    } while (result != 0 && curRetryTime < maxRetryTimes);
    if (result != 0) {
        ErrLog(DSTORE_ERROR, MODULE_PDBREPLICA,
               ErrMsg("Get gsscms comm info failed with %u retry, error code %lld.", maxRetryTimes, result));
        return DSTORE_FAIL;
    }

    ErrLog(DSTORE_LOG, MODULE_PDBREPLICA,
           ErrMsg("GetGsscmsCommInfo success, clusterId %u, protocolType %u, authMethod %u, gsscmsAddr %s", clusterId,
                  gsscmsCommInfo->protocolType, gsscmsCommInfo->authMethod, gsscmsCommInfo->gsscmsAddr));
    return DSTORE_SUCC;
}

VfsClientHandle *GetVfsClientHandle(uint32 clusterId, bool needCreate, bool needUploadClientId)
{
    if (clusterId == INVALID_CLUSTER_ID) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg(
            "[VFS_CLIENT_HANLDE] Invalid cluster id when get vfs client handle."));
        return nullptr;
    }

    /* 1: Find available cluster slot. */
    ::VfsClientHandle *newClientHandle = nullptr;
    int32 avail_slot_idx = -1;
    MutexLock(&g_vfsClientHandleLock);
    for (uint32 idx = 0; idx < MAX_CLUSTER_COUNT; idx++) {
        if (g_vfsClientHandles[idx].clusterId == INVALID_CLUSTER_ID) {
            if (avail_slot_idx == -1) {
                avail_slot_idx = static_cast<int32>(idx);
            }
        } else if (g_vfsClientHandles[idx].clusterId == clusterId) {
            newClientHandle = g_vfsClientHandles[idx].clientHandle;
            MutexUnlock(&g_vfsClientHandleLock);
            ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg(
                "[VFS_CLIENT_HANLDE] Client instance handle for cluster %u already exists.", clusterId));
            return newClientHandle;
        }
    }

    if (avail_slot_idx == -1) {
        MutexUnlock(&g_vfsClientHandleLock);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg(
            "[VFS_CLIENT_HANLDE] Failed to find available slot for cluster %u when create vfs client handle.",
            clusterId));
        return nullptr;
    }

    if (!needCreate) {
        MutexUnlock(&g_vfsClientHandleLock);
        ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg(
            "[VFS_CLIENT_HANLDE] cluster name is null, get existed client failed, "
            "can not create vfs client handle for cluster %u.", clusterId));
        return nullptr;
    }

    /* 2: Get server address for aid cluster */
    AidClusterGsscmsCommInfo gsscmsCommInfo = {"", "", 0, 0};
    RetStatus rc = GetClusterServerAddr(clusterId, &gsscmsCommInfo);
    if (STORAGE_FUNC_FAIL(rc) || gsscmsCommInfo.gsscmsAddr[0] == '\0' || gsscmsCommInfo.clusterName[0] == '\0') {
        MutexUnlock(&g_vfsClientHandleLock);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
               ErrMsg("[VFS_CLIENT_HANLDE] Failed to get server address for clusterId = %u, gsscmsAddr = %s, "
                      "clusterName = %s.",
                      clusterId, gsscmsCommInfo.gsscmsAddr, gsscmsCommInfo.clusterName));
        return nullptr;
    } else {
        ErrLog(DSTORE_LOG, MODULE_FRAMEWORK,
               ErrMsg("[VFS_CLIENT_HANLDE] Get server address success for clusterId = %u, clusterName = %s, "
                      "protocolType = %d, authType = %d, addr = %s.",
                      clusterId, gsscmsCommInfo.clusterName, gsscmsCommInfo.protocolType, gsscmsCommInfo.authMethod,
                      gsscmsCommInfo.gsscmsAddr));
    }

    /* 3: Create vfs client handle. */
    uint32 authType = 0;
    if (gsscmsCommInfo.authMethod > 0) {
        /* authMethod > 0 means using psk or tls, dr only support tls (COMM_AUTH_TYPE_TLS_MUTUAL_AUTH_ENABLE) */
        authType = 3;
    }
    ::ErrorCode errcode = ::CreateVfsClientHandle(g_vfsLibHandle, gsscmsCommInfo.clusterName, authType,
                                                  gsscmsCommInfo.gsscmsAddr, &newClientHandle);
    if (errcode != ERROR_SYS_OK) {
        MutexUnlock(&g_vfsClientHandleLock);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg(
            "[VFS_CLIENT_HANLDE] Failed to create client instance handle for cluster %u, errCode = %lld",
            clusterId, errcode));
        return nullptr;
    }

    g_vfsClientHandles[avail_slot_idx].clusterId = clusterId;
    g_vfsClientHandles[avail_slot_idx].clientHandle = newClientHandle;
    errno_t nRet =
        strcpy_s(g_vfsClientHandles[avail_slot_idx].clusterName, CLUSTER_NAME_MAX_LEN, gsscmsCommInfo.clusterName);
    storage_securec_check(nRet, "\0", "\0");

    /* 4: Invoke callback to update client id in cmc memberview. */
    uint64_t clientId = 0;
    errcode = ::GetVfsLibClientId(newClientHandle, &clientId);
    if (errcode != ERROR_SYS_OK) {
        ErrLog(
            DSTORE_ERROR, MODULE_FRAMEWORK,
            ErrMsg("[VFS_CLIENT_HANLDE] Failed to get client id for cluster %u, errCode = %lld", clusterId, errcode));
        MutexUnlock(&g_vfsClientHandleLock);
        ReleaseVfsClientHandle(clusterId);
        return nullptr;
    }
    MutexUnlock(&g_vfsClientHandleLock);

    if (needUploadClientId) {
        StorageReleasePanic(g_uploadVfsClientIdCallback == nullptr, MODULE_FRAMEWORK,
                            ErrMsg("[VFS_CLIENT_HANLDE] UpsertVfsClientIdCallback is nullptr."));
        errcode = g_uploadVfsClientIdCallback(clusterId, clientId);
        if (errcode != ERROR_SYS_OK) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
                   ErrMsg("[VFS_CLIENT_HANLDE] Failed to update client id for cluster %u, errCode = %lld", clusterId,
                          errcode));
            ReleaseVfsClientHandle(clusterId);
            return nullptr;
        } else {
            ErrLog(DSTORE_LOG, MODULE_FRAMEWORK,
                   ErrMsg("[VFS_CLIENT_HANLDE] Update client id %lu for cluster %u success.", clientId, clusterId));
        }
    } else {
        ErrLog(DSTORE_LOG, MODULE_FRAMEWORK,
               ErrMsg("[VFS_CLIENT_HANLDE] Not need to update client id for cluster %u.", clusterId));
    }

    ErrLog(DSTORE_LOG, MODULE_FRAMEWORK,
           ErrMsg("[VFS_CLIENT_HANLDE] Create vfs client handle success, clusterId = %u, clientId = %lu.", clusterId,
                  clientId));
    return newClientHandle;
}

RetStatus ReleaseVfsClientHandle(uint32 clusterId)
{
    if (clusterId == INVALID_CLUSTER_ID) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg(
            "[VFS_CLIENT_HANLDE] Invalid cluster id when release vfs client handle."));
        return DSTORE_FAIL;
    }
    MutexLock(&g_vfsClientHandleLock);
    for (uint32 idx = 0; idx < MAX_CLUSTER_COUNT; idx++) {
        if (g_vfsClientHandles[idx].clusterId == clusterId) {
            ::ErrorCode errcode = ::DeleteVfsClientHandle(g_vfsClientHandles[idx].clientHandle);
            if (errcode != ERROR_SYS_OK) {
                MutexUnlock(&g_vfsClientHandleLock);
                ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg(
                    "[VFS_CLIENT_HANLDE] Failed to delete vfs client handle for cluster %u, error code = %lld",
                    clusterId, errcode));
                return DSTORE_FAIL;
            }
            g_vfsClientHandles[idx].clusterId = INVALID_CLUSTER_ID;
            g_vfsClientHandles[idx].clientHandle = nullptr;
            errno_t rc = memset_s(g_vfsClientHandles[idx].clusterName, CLUSTER_NAME_MAX_LEN, 0,
                CLUSTER_NAME_MAX_LEN);
            storage_securec_check(rc, "\0", "\0");
            MutexUnlock(&g_vfsClientHandleLock);
            ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg(
                "[VFS_CLIENT_HANLDE] Delete vfs client handle for cluster %u success when release.",
                clusterId));
            return DSTORE_SUCC;
        }
    }
    MutexUnlock(&g_vfsClientHandleLock);
    ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg(
        "[VFS_CLIENT_HANLDE] Nonexistent cluster id when release vfs client handle."));
    return DSTORE_FAIL;
}

RetStatus ReleaseAllStandbyVfsClientHandle()
{
#ifdef UT
    return DSTORE_SUCC;
#endif
    MutexLock(&g_vfsClientHandleLock);
    for (uint32 idx = 0; idx < MAX_CLUSTER_COUNT; idx++) {
        if (g_vfsClientHandles[idx].clusterId != INVALID_CLUSTER_ID) {
            uint32 clusterId = g_vfsClientHandles[idx].clusterId;
            ::ErrorCode errcode = ::DeleteVfsClientHandle(g_vfsClientHandles[idx].clientHandle);
            if (errcode != ERROR_SYS_OK) {
                MutexUnlock(&g_vfsClientHandleLock);
                ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg(
                    "[VFS_CLIENT_HANLDE] Failed to delete vfs client handle for cluster %u, error code = %lld",
                    clusterId, errcode));
                return DSTORE_FAIL;
            }
            g_vfsClientHandles[idx].clusterId = INVALID_CLUSTER_ID;
            g_vfsClientHandles[idx].clientHandle = nullptr;
            errno_t rc = memset_s(g_vfsClientHandles[idx].clusterName, CLUSTER_NAME_MAX_LEN, 0,
                CLUSTER_NAME_MAX_LEN);
            storage_securec_check(rc, "\0", "\0");
            ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg(
                "[VFS_CLIENT_HANLDE] Delete vfs client handle for cluster %u success when release.",
                clusterId));
        }
    }
    MutexUnlock(&g_vfsClientHandleLock);
    ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg(
        "[VFS_CLIENT_HANLDE] Nonexistent cluster id when release vfs client handle."));
    return DSTORE_SUCC;
}

void InitVfsClientHandleLock()
{
    MutexInit(&g_vfsClientHandleLock);
}

void RegisterUploadVfsClientIdCallback(UploadVfsClientIdCallback callback)
{
    g_uploadVfsClientIdCallback = callback;
}

void RegisterGsscmsCommInfoQueryCallback(GsscmsCommInfoQueryCallback callback)
{
    g_gsscmsCommInfoQueryCallback = callback;
}

RetStatus OpenVFS(VfsClientHandle *vfsClientHandle, const char *tenantName, const char *vfsName, uint32 clusterId,
    ::VirtualFileSystem **vfs, bool &needClearDrRelation)
{
    if (STORAGE_VAR_NULL(vfsClientHandle)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg(
            "[VFS_CLIENT_HANLDE] vfs client handle is nullptr when open aid vfs."));
        return DSTORE_FAIL;
    }
    if (STORAGE_VAR_NULL(tenantName)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg(
            "[VFS_CLIENT_HANLDE] tenant name is nullptr when open aid vfs."));
        return DSTORE_FAIL;
    }
    if (STORAGE_VAR_NULL(vfsName)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg(
            "[VFS_CLIENT_HANLDE] vfs name is nullptr when open aid vfs."));
        return DSTORE_FAIL;
    }
    if (STORAGE_VAR_NULL(vfs)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg(
            "[VFS_CLIENT_HANLDE] given vfs pointer is nullptr when open aid vfs."));
        return DSTORE_FAIL;
    }

    /* 1: Add active vfs */
    if (STORAGE_FUNC_FAIL(IoFencingVFSCollection::GetSingleton()->AddActiveVFS(vfsName, true, clusterId))) {
        ::ErrorCode errCode = StorageGetErrorCode();
        if (errCode != VFS_WARNING_DUPLICATE_ADD_ACTIVE) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg(
                "[DR_VFS_ADAPTER] add active vfs failed, aidClusterId(%u), vsfName(%s).",
                clusterId, vfsName));
            return DSTORE_FAIL;
        }
    }
    /* 1.1: triger io fencing */
    if (STORAGE_FUNC_FAIL(IoFencingVFSCollection::GetSingleton()->TriggerIoFencing())) {
        (void)IoFencingVFSCollection::GetSingleton()->RemoveActiveVFS(vfsName, clusterId);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg(
            "[DR_VFS_ADAPTER] trigger io fencing failed, aidClusterId(%u), vsfName(%s).",
            clusterId, vfsName));
        return DSTORE_FAIL;
    }
    ErrorCode ioFenceErr = ERROR_SYS_OK;
    if (STORAGE_FUNC_FAIL(IoFencingVFSCollection::GetSingleton()->GetActiveDrVfsIoFenceErr(vfsName, clusterId,
                                                                                           ioFenceErr))) {
        (void)IoFencingVFSCollection::GetSingleton()->RemoveActiveVFS(vfsName, clusterId);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg(
            "[DR_VFS_ADAPTER] GetActiveDrVfsIoFenceErr failed, aidClusterId(%u), vsfName(%s).",
            clusterId, vfsName));
        return DSTORE_FAIL;
    }
    if (unlikely(ioFenceErr != ERROR_SYS_OK)) {
        if (ioFenceErr == VFS_ERROR_IO_FENCING_REFUSE_BY_OWNER || ioFenceErr == VFS_ERROR_VFS_NOT_EXIST) {
            needClearDrRelation = true;
        }
        (void)IoFencingVFSCollection::GetSingleton()->RemoveActiveVFS(vfsName, clusterId);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg(
            "[DR_VFS_ADAPTER] trigger dr io fencing failed, aidClusterId(%u), vsfName(%s).",
            clusterId, vfsName));
        return DSTORE_FAIL;
    }

    /* 2: Mount vfs with name for vfs client handle. */
    ::ErrorCode retError = MountVfs(vfsClientHandle, tenantName, vfsName, vfs);
    if (unlikely(retError)) {
        storage_set_error(VFS_ERROR_FAILED_TO_MOUNT_VFS);
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg(
            "[VFS_CLIENT_HANLDE] Mount Vfs tenant name:%s, vfs name:%s, cluster id %u, failed %lld.",
            tenantName, vfsName, clusterId, retError));
        (void)IoFencingVFSCollection::GetSingleton()->RemoveActiveVFS(vfsName, clusterId);
        return DSTORE_FAIL;
    }
    ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg(
        "[VFS_CLIENT_HANLDE] Tenant %s open vfs success: %s, cluster id %u.",
        tenantName, vfsName, clusterId));
    return DSTORE_SUCC;
}

using VfsInterface::DISK_PERF_DESC_HIGH;
using VfsInterface::DISK_PERF_DESC_LOW;
using VfsInterface::DISK_PERF_DESC_MEDIUM;
using VfsInterface::DISK_PERF_TYPE;
StoreSpaceConfig *ChooseTheBestStoreSpaceConfig(StoreSpaceConfig *storeSpaceConfig, int cnt, const char *type)
{
    int arr[static_cast<int>(DISK_PERF_TYPE::COUNT)];
    for (int k = 0; k < static_cast<int>(DISK_PERF_TYPE::COUNT); k++) {
        arr[k] = -1;
    }
    for (int i = 0; i < cnt; i++) {
        StoreSpaceConfig *config = storeSpaceConfig + i;
        if (strcmp(config->type, DISK_PERF_DESC_HIGH) == 0 &&
            arr[static_cast<int>(DISK_PERF_TYPE::DISK_PERF_HIGH)] == -1) {
            arr[static_cast<int>(DISK_PERF_TYPE::DISK_PERF_HIGH)] = i;
        }
        if (strcmp(config->type, DISK_PERF_DESC_MEDIUM) == 0 &&
            arr[static_cast<int>(DISK_PERF_TYPE::DISK_PERF_MEDIUM)] == -1) {
            arr[static_cast<int>(DISK_PERF_TYPE::DISK_PERF_MEDIUM)] = i;
        }
        if (strcmp(config->type, DISK_PERF_DESC_LOW) == 0 &&
            arr[static_cast<int>(DISK_PERF_TYPE::DISK_PERF_LOW)] == -1) {
            arr[static_cast<int>(DISK_PERF_TYPE::DISK_PERF_LOW)] = i;
        }
    }

    int startIdx = static_cast<int>(DISK_PERF_TYPE::COUNT);
    if (strcmp(type, DISK_PERF_DESC_LOW) == 0) {
        startIdx = static_cast<int>(DISK_PERF_TYPE::DISK_PERF_LOW);
    } else if (strcmp(type, DISK_PERF_DESC_MEDIUM) == 0) {
        startIdx = static_cast<int>(DISK_PERF_TYPE::DISK_PERF_MEDIUM);
    } else if (strcmp(type, DISK_PERF_DESC_HIGH) == 0) {
        startIdx = static_cast<int>(DISK_PERF_TYPE::DISK_PERF_HIGH);
    }

    StoreSpaceConfig *theBestConfig = nullptr;
    for (int j = startIdx; j < static_cast<int>(DISK_PERF_TYPE::COUNT); j++) {
        if (arr[j] != -1) {
            theBestConfig = storeSpaceConfig + arr[j];
            break;
        }
    }

    return theBestConfig;
}
} /* namespace DSTORE */
