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
 * dstore_vfs_adapter.h
 *
 * Description: this file defineds the behaviors how does storage engine communicate
 * with the external VFS library.
 *
 * ---------------------------------------------------------------------------------------
 *
 */
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cstring>
#include <fcntl.h>
#include <cstdio>
#include "config/dstore_vfs_config.h"
#include "vfs/vfs_interface.h"
#include "port/dstore_port.h"
#include "framework/dstore_vfs_file_interface.h"
#include "framework/dstore_vfs_adapter.h"
#include "common/log/dstore_log.h"
#include "framework/dstore_vfs_interface.h"

namespace VfsInterface {
using namespace DSTORE;

constexpr int LOG_INTERVAL = 500;

bool ModuleInitialize()
{
    RetStatus ret = VFSAdapter::ModuleInitialize();
    if (ret == DSTORE_FAIL) {
        return false;
    }
    return true;
}

RetStatus LibInitialize(void *tenantConfig, StorageInstanceType type, uint32_t clientId)
{
    return VFSAdapter::LibInitialize(tenantConfig, type, clientId);
}

VfsLibHandle *GetVfsModuleLib()
{
    return VFSAdapter::GetVfsLib();
}

RetStatus OpenVfs(const char *tenantName, const char *vfsName, ::VirtualFileSystem **vfs)
{
    ErrorCode retError = MountVfs(GetDefaultVfsClientHandle(), tenantName, vfsName, vfs);
    if (unlikely(retError)) {
        (void)printf("[VFS]MountVfs tenantName:%s, vfsname:%s, failed %lld\n", tenantName, vfsName, retError);
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus CloseVfs(const char *vfsName, ::VirtualFileSystem *vfs)
{
    ErrorCode retError = UnmountVfs(vfs);
    if (unlikely(retError != ERROR_SYS_OK)) {
        (void)printf("[VFS]UnmountVfs %s failed %lld\n", vfsName, retError);
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}


bool AddDrClusterId(uint32 *clusterIdArr, uint32 clusterId, uint32 arrSize, uint32 *realSize)
{
    if (*realSize >= arrSize) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
               ErrMsg("[VFS] The number of clusters exceeds the maximum value %u.", arrSize));
        return false;
    }
    bool found = false;
    for (uint32 idx = 0; idx < *realSize; idx++) {
        if (clusterIdArr[idx] == clusterId) {
            found = true;
            break;
        }
    }
    if (!found) {
        clusterIdArr[*realSize] = clusterId;
        (*realSize)++;
    }
    return true;
}

void SetIoFencingCallback(CmcIoFencingCallback callback)
{
    IoFencingVFSCollection::GetSingleton()->SetTriggerIoFencingCallback(callback);
}

void SetUploadVfsClientIdCallback(UploadVfsClientIdCallback callback)
{
    RegisterUploadVfsClientIdCallback(callback);
}

void SetGsscmsCommInfoQueryCallback(GsscmsCommInfoQueryCallback callback)
{
    RegisterGsscmsCommInfoQueryCallback(callback);
}

VfsClientHandle *CreateDisasterVfsClientHandle(uint32 clusterId)
{
    /* create a client but not upload client id to cmc memberview module */
    return GetVfsClientHandle(clusterId, true, false);
}

DSTORE::StoreSpaceConfig *ChooseTheBestStoreSpaceConfig(DSTORE::StoreSpaceConfig *storeSpaceConfig, int cnt,
                                                        const char *type)
{
    return DSTORE::ChooseTheBestStoreSpaceConfig(storeSpaceConfig, cnt, type);
}

::VfsLibHandle *SetupPageStore(const DSTORE::TenantConfig *config)
{
    InitSignalMask();
    if (unlikely(config == nullptr)) {
        return nullptr;
    }
    if (config->storageConfig.type != DSTORE::StorageType::PAGESTORE) {
        return nullptr;
    }

    return LoadVfsLibrary(config);
}

ErrorCode CopyFileFromRemoteToLocal(VirtualFileSystem *remoteVfs, VirtualFileSystem *localVfs, const char *path,
                                    uint16_t fileId, bool needRemoveExistFile)
{
    /* Find whether file exists in local vfs */
    if (needRemoveExistFile) {
        bool fileExists = false;
        ErrorCode errCode = FileIsExist(localVfs, path, &fileExists);
        if (unlikely(errCode != ERROR_SYS_OK)) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
                   ErrMsg("Failed to query file exist or not, error code is %lld, fileid is %hu.", errCode, fileId));
            return errCode;
        }
        /* Remove file in local vfs */
        if (fileExists) {
            errCode = Remove(localVfs, path);
            if (unlikely(errCode != ERROR_SYS_OK)) {
                ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
                       ErrMsg("Failed to remove local config file for reload, error code is %lld, fileid is %hu.",
                              errCode, fileId));
                return errCode;
            }
        }
    }
    /* Copy file in remote vfs to local vfs */
    char remoteFilePath[MAXPGPATH];
    int rc = snprintf_s(remoteFilePath, MAXPGPATH, MAXPGPATH - 1, "%hu", fileId);
    storage_securec_check_ss(rc);
    ErrorCode errCode = CopyFile(remoteVfs, static_cast<const char *>(remoteFilePath), localVfs, path, "");
    if (unlikely(errCode != ERROR_SYS_OK)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
               ErrMsg("Failed to copy file from remote to local, error code is %lld, fileid is %hu.", errCode, fileId));
    }
    return errCode;
}

ErrorCode GetVfsFileLockAndCopy(VirtualFileSystem *remoteVfs, VirtualFileSystem *localVfs, uint16_t fileId,
                                const char *path, char *storeSpaceName, bool needRemoveExistFile)
{
    FileDescriptor *lockFd = nullptr;
    ErrorCode vfsRet = ERROR_SYS_OK;
    ErrorCode copyRet = ERROR_SYS_OK;
    uint16_t configFileId = 0;

    /* Init file lock in remote vfs */
    char fileName[MAXPGPATH];
    int rc = snprintf_s(fileName, MAXPGPATH, MAXPGPATH - 1, "%u", fileId);
    storage_securec_check_ss(rc);
    vfsRet = InitFileLock(remoteVfs, static_cast<const char *>(fileName), storeSpaceName, &lockFd);
    if (unlikely(vfsRet != ERROR_SYS_OK)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to init file lock, error code is %lld.", vfsRet));
        return vfsRet;
    }

    if (fileId == DbConfigFileId::GS_HBA_LOCK_FILEID) {
        configFileId = DbConfigFileId::GS_HBA_FILEID;
    } else if (fileId == DbConfigFileId::GAUSSDB_CONF_LOCK_FILEID) {
        configFileId = DbConfigFileId::GAUSSDB_CONF_FILEID;
    } else {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
               ErrMsg("Error input parameter fileId %hu.", fileId));
        return VFS_ERROR_PARAMETERS_INVALID;
    }
    int tryFileLockTime = 0;
    while (true) {
        tryFileLockTime++;
        vfsRet = FileLock(lockFd);
        if (vfsRet == ERROR_SYS_OK) {
            ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("Get file lock finally at %d times.", tryFileLockTime));
            copyRet = CopyFileFromRemoteToLocal(remoteVfs, localVfs, path, configFileId, needRemoveExistFile);
            if (copyRet != ERROR_SYS_OK) {
                ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
                       ErrMsg("Failed to copy file from remote to local and will retry using its backup file, "
                              "error code is %lld, fileid is %hu.", copyRet, fileId));
                if (configFileId == GS_HBA_FILEID) {
                    configFileId = DbConfigFileId::GS_HBA_BK_FILEID;
                    copyRet = CopyFileFromRemoteToLocal(remoteVfs, localVfs, path, configFileId, needRemoveExistFile);
                } else if (configFileId == GAUSSDB_CONF_FILEID) {
                    configFileId = DbConfigFileId::GAUSSDB_CONF_BK_FILEID;
                    copyRet = CopyFileFromRemoteToLocal(remoteVfs, localVfs, path, configFileId, needRemoveExistFile);
                }
            }
            if (copyRet != ERROR_SYS_OK) {
                ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
                   ErrMsg("Failed to copy config file from remote to local, "
                          " error code is %lld, fileid is %hu.", copyRet, fileId));
            } else {
                ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("Copy file from remote to local success."));
            }
            vfsRet = copyRet;
            break;
        }
        GaussUsleep(TRY_LOCK_DELAY);
        if (tryFileLockTime % LOG_INTERVAL == 0) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
                   ErrMsg("Failed to get file lock %d times, fileid is %hu.", tryFileLockTime, fileId));
            break;
        }
    }

    // If lock file failed, skip unlock file operation.
    if (tryFileLockTime % LOG_INTERVAL != 0) {
        ErrorCode ecUnlock = FileUnLock(lockFd);
        if (unlikely(ecUnlock != ERROR_SYS_OK)) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
                ErrMsg("Failed to unlock file, error code is %lld, fileid is %hu.", ecUnlock, fileId));
            vfsRet = ecUnlock;
        }
    }
    
    ErrorCode ecCloseLock = Close(lockFd);
    if (unlikely(ecCloseLock != ERROR_SYS_OK)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
               ErrMsg("Failed to close lock file, error code is %lld, fileid is %hu.", ecCloseLock, fileId));
        vfsRet = ecCloseLock;
    }
    return vfsRet;
}

::VfsLibHandle *SetupTenantIsoland(const DSTORE::TenantConfig *config, const char* basePath)
{
    InitSignalMask();
    if (unlikely(config == nullptr)) {
        return nullptr;
    }
    if (config->storageConfig.type != DSTORE::StorageType::TENANT_ISOLATION) {
        return nullptr;
    }
    return SetupTenantIsolation(config, basePath);
}

RetStatus CopyAllPssConfigFileToLocal(VirtualFileSystem *remoteVfs, const char *pgData, char *storeSpaceName)
{
    VirtualFileSystem *localVfs = nullptr;
    ErrorCode errCode = GetStaticLocalVfsInstance(&localVfs);
    if (unlikely(errCode != 0)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
               ErrMsg("Failed to get local vfs instance! errcode code is %lld.", errCode));
        return DSTORE_FAIL;
    }
    for (uint i = 0; i < sizeof(DB_CONFIG_MAP) / sizeof(DB_CONFIG_MAP[0]); i++) {
        if (DB_CONFIG_MAP[i].fileId == DbConfigFileId::GAUSSDB_CONF_BK_FILEID ||
            DB_CONFIG_MAP[i].fileId == DbConfigFileId::GS_HBA_BK_FILEID) {
            continue;
        }
        char *configFilePath = (char *)malloc(strlen(pgData) + MAX_CONF_NAME_LEN);
        if (unlikely(configFilePath == nullptr)) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to malloc memory for configfilename!"));
            return DSTORE_FAIL;
        }
        int nRet =
            sprintf_s(configFilePath, (strlen(pgData) + MAX_CONF_NAME_LEN), "%s/%s", pgData, DB_CONFIG_MAP[i].filename);
        storage_securec_check_ss(nRet);
        char remoteConfigFilePath[MAXPGPATH];
        nRet = snprintf_s(remoteConfigFilePath, MAXPGPATH, MAXPGPATH - 1, "%hu", DB_CONFIG_MAP[i].fileId);
        storage_securec_check_ss(nRet);
        if ((DB_CONFIG_MAP[i].fileId == DbConfigFileId::GS_HBA_FILEID) ||
            (DB_CONFIG_MAP[i].fileId == DbConfigFileId::GAUSSDB_CONF_FILEID)) {
            errCode = GetVfsFileLockAndCopy(remoteVfs, localVfs, DB_CONFIG_MAP[i].fileLockId,
                                            static_cast<const char *>(configFilePath), storeSpaceName, true);
        } else {
            errCode = CopyFile(remoteVfs, static_cast<const char *>(remoteConfigFilePath), localVfs,
                               static_cast<const char *>(configFilePath), "");
        }
        free(configFilePath);
        configFilePath = nullptr;
        if (errCode != 0) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
                   ErrMsg("Failed to copy file from pss to local! errcode code is %lld.", errCode));
            return DSTORE_FAIL;
        }
    }
    return DSTORE_SUCC;
}

static ErrorCode CreateLockFileToPss(VirtualFileSystem *remoteVfs, uint16_t lockFileId, char *storeSpaceName)
{
    ErrorCode vfsRet = ERROR_SYS_OK;
    FileDescriptor *lockFd = nullptr;
    char fileName[MAXPGPATH];
    int rc = snprintf_s(fileName, MAXPGPATH, MAXPGPATH - 1, "%u", lockFileId);
    storage_securec_check_ss(rc);
    vfsRet = InitFileLock(remoteVfs, static_cast<const char *>(fileName), storeSpaceName, &lockFd);
    if (unlikely(vfsRet != ERROR_SYS_OK)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
               ErrMsg("Failed to create lock file, error code is %lld, fileid is %hu.", vfsRet, lockFileId));
        return vfsRet;
    }
    vfsRet = Close(lockFd);
    if (unlikely(vfsRet != ERROR_SYS_OK)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
               ErrMsg("Failed to close lock file, error code is %lld, fileid is %hu.", vfsRet, lockFileId));
        return vfsRet;
    }
    ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("Create lock file to remote success! fileid is %hu.", lockFileId));
    return vfsRet;
}

RetStatus CopyAllLocalConfigFileToPss(VirtualFileSystem *remoteVfs, const char *pgData, char *storeSpaceName)
{
    /* Step 1. copy config file */
    VirtualFileSystem *localVfs = nullptr;
    ErrorCode errCode = GetStaticLocalVfsInstance(&localVfs);
    if (unlikely(errCode != 0)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
               ErrMsg("Failed to get local vfs instance! errcode code is %lld.", errCode));
        return DSTORE_FAIL;
    }
    for (uint i = 0; i < sizeof(DB_CONFIG_MAP) / sizeof(DB_CONFIG_MAP[0]); i++) {
        char *configFilePath = (char *)malloc(strlen(pgData) + MAX_CONF_NAME_LEN);
        if (unlikely(configFilePath == nullptr)) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed to malloc memory for configfilename!"));
            return DSTORE_FAIL;
        }
        const char *postgreFileName = "gaussdb.conf";
        const char *postgreFileRunName = "gaussdb_run.conf";
        if (strncmp(DB_CONFIG_MAP[i].filename, postgreFileName, strlen(postgreFileName)) == 0) {
            int nRet =
                sprintf_s(configFilePath, (strlen(pgData) + MAX_CONF_NAME_LEN), "%s/%s", pgData, postgreFileRunName);
            storage_securec_check_ss(nRet);
        } else {
            int nRet = sprintf_s(configFilePath, (strlen(pgData) + MAX_CONF_NAME_LEN), "%s/%s", pgData,
                                 DB_CONFIG_MAP[i].filename);
            storage_securec_check_ss(nRet);
        }
        char remoteConfigFilePath[MAXPGPATH];
        int rc = snprintf_s(remoteConfigFilePath, MAXPGPATH, MAXPGPATH - 1, "%u", DB_CONFIG_MAP[i].fileId);
        storage_securec_check_ss(rc);
        errCode = CopyFile(localVfs, static_cast<const char *>(configFilePath), remoteVfs,
            static_cast<const char *>(remoteConfigFilePath), storeSpaceName);
        free(configFilePath);
        configFilePath = nullptr;
        if (unlikely(errCode != 0)) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
                   ErrMsg("Failed to copy file from local to remote! errcode code is %lld, fileid is %hu.", errCode,
                          DB_CONFIG_MAP[i].fileId));
            return DSTORE_FAIL;
        }
        ErrLog(DSTORE_LOG, MODULE_FRAMEWORK,
               ErrMsg("Copy file from local to remote success! fileid is %hu.", DB_CONFIG_MAP[i].fileId));
    }
    /* Step 2. create postgresql.lock and pg_hba.lock file */
    if (CreateLockFileToPss(remoteVfs, DbConfigFileId::GAUSSDB_CONF_LOCK_FILEID, storeSpaceName) != ERROR_SYS_OK) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
               ErrMsg("Failed to create lock file, fileid is %hu.", DbConfigFileId::GAUSSDB_CONF_LOCK_FILEID));
        return DSTORE_FAIL;
    }
    if (CreateLockFileToPss(remoteVfs, DbConfigFileId::GS_HBA_LOCK_FILEID, storeSpaceName) != ERROR_SYS_OK) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
               ErrMsg("Failed to create lock file, fileid is %hu.", DbConfigFileId::GS_HBA_LOCK_FILEID));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

static RetStatus CreateSpecVfs(DSTORE::TenantConfig *tenantConfig, const char *vfsName, uint64 *nodeId,
                               unsigned long long vfsIoFenceFlag, uint64 term)
{
    ::ErrorCode retError = CreateVfs(GetDefaultVfsClientHandle(), tenantConfig->tenantName, vfsName, vfsIoFenceFlag);
    if (unlikely(retError)) {
        (void)fprintf(stderr, "Failed to create root pdb vfs, error code is %lld.\n", retError);
        return DSTORE_FAIL;
    }

    if (vfsIoFenceFlag == VFS_ENABLE_IO_FENCE_FLAG && tenantConfig->storageConfig.type == StorageType::PAGESTORE) {
        RetStatus ret = IoFencingVFSCollection::GetSingleton()->AddActiveVFS(vfsName);
        if (STORAGE_FUNC_FAIL(ret)) {
            return DSTORE_FAIL;
        }
        uint64 nodeTimelineId = NODE_TIMELINE_ID_DEFAULT;  // default value
        ret = IoFencingVFSCollection::GetSingleton()->SetIoFencingWhiteList(nodeId, &nodeTimelineId, 1, term);
        if (STORAGE_FUNC_FAIL(ret)) {
            return DSTORE_FAIL;
        }
    }

    return DSTORE_SUCC;
}

static uint64_t GetStoreSpaceType(const char* type)
{
    if (strcmp(type, DISK_PERF_DESC_HIGH) == 0) {
        return HIGH_DISK_PERF_FLAG;
    }
    if (strcmp(type, DISK_PERF_DESC_MEDIUM) == 0) {
        return MEDIUM_DISK_PERF_FLAG;
    }
    if (strcmp(type, DISK_PERF_DESC_LOW) == 0) {
        return LOW_DISK_PERF_FLAG;
    }
    return 0;      /* invalid type */
}

static RetStatus CreateTenantDefaultStoreSpace(VfsLibHandle *vfsLibHandle, DSTORE::TenantConfig *config)
{
    ErrorCode retError;
    (void)vfsLibHandle;
    for (int i = 0; i < config->storeSpaceCnt; i++) {
        StoreSpaceAttr attrs[1];  /* mock vfs create one storespace */
        const char *storeSpaceNames[] = {config->storeSpaces[i].storeSpaceName};
        attrs[0].attrFlags = GetStoreSpaceType(config->storeSpaces[i].type);
        attrs[0].maxSize = static_cast<uint32_t>(config->storeSpaces[i].maxSpaceSize);
        attrs[0].reserved = 0;
        retError = CreateStoreSpace(GetDefaultVfsClientHandle(), config->tenantName,
                                    static_cast<const char **>(&storeSpaceNames[0]), attrs,
                                    1);  /* mock vfs create one storespace */
        if (unlikely(retError != 0)) {
            (void)fprintf(stderr,
                "Failed to CreateStoreSpace %s, error code is %lld.\n",
                config->storeSpaces[i].storeSpaceName,
                retError);
            return DSTORE_FAIL;
        }
        (void)printf("[VFS]Create storespace %s success\n", storeSpaceNames[0]);
    }
    return DSTORE_SUCC;
}

RetStatus CreateTenantDefaultVfs(DSTORE::TenantConfig *tenantConfig)
{
    uint64 nodeId = 0;
    ::ErrorCode retError;
#ifndef UT
    retError = GetVfsLibClientId(GetDefaultVfsClientHandle(), &nodeId);
    if (unlikely(retError)) {
        (void)fprintf(stderr, "Failed to get vfs lib client id, error code is %lld.\n", retError);
        return DSTORE_FAIL;
    }
#endif
    if (tenantConfig->storageConfig.type == StorageType::PAGESTORE) {
        retError = CreateStoreTenant(GetDefaultVfsClientHandle(), tenantConfig->clusterName,
            tenantConfig->tenantName);
        if (unlikely(retError)) {
            (void)fprintf(stderr, "Failed to create tenant %s, error code is %lld.\n",
                tenantConfig->tenantName, retError);
            return DSTORE_FAIL;
        }
        RetStatus ret = CreateTenantDefaultStoreSpace(GetVfsLibHandle(), tenantConfig);
        if (STORAGE_FUNC_FAIL(ret)) {
            return DSTORE_FAIL;
        }
    }

    if (CreateSpecVfs(tenantConfig, tenantConfig->storageConfig.template0VfsName, &nodeId,
                      VFS_ENABLE_IO_FENCE_FLAG, 0) == DSTORE_FAIL ||
        CreateSpecVfs(tenantConfig, tenantConfig->storageConfig.template1VfsName, &nodeId,
                      VFS_ENABLE_IO_FENCE_FLAG, 0) == DSTORE_FAIL ||
        CreateSpecVfs(tenantConfig, tenantConfig->storageConfig.rootpdbVfsName, &nodeId,
                      VFS_ENABLE_IO_FENCE_FLAG, 0) == DSTORE_FAIL ||
        CreateSpecVfs(tenantConfig, tenantConfig->storageConfig.votingVfsName, &nodeId,
                      VFS_DISABLE_IO_FENCE_FLAG, 0) == DSTORE_FAIL ||
        CreateSpecVfs(tenantConfig, tenantConfig->storageConfig.runlogVfsName, &nodeId,
                      VFS_DISABLE_IO_FENCE_FLAG, 0) == DSTORE_FAIL) {
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}
RetStatus CreateTemplateVfs(DSTORE::TenantConfig *tenantConfig, const char *vfsName)
{
    /* The log module is not initialized. Use printf first. */
    if (unlikely(tenantConfig == nullptr || vfsName == nullptr)) {
        (void)fprintf(stderr, "Invalid parameter for creating a VFS.\n");
        return DSTORE_FAIL;
    }
    uint64 term = 0;
    if (unlikely(IoFencingVFSCollection::GetSingleton()->GetIoFencingWhiteList(
        tenantConfig->storageConfig.template1VfsName, nullptr, nullptr, &term) != DSTORE_SUCC)) {
        (void)fprintf(stderr, "Invalid parameter for creating a VFS.\n");
        return DSTORE_FAIL;
    }

    uint64 nodeId = 0;
#ifndef UT
    ::ErrorCode retError = GetVfsLibClientId(GetDefaultVfsClientHandle(), &nodeId);
    if (unlikely(retError)) {
        (void)fprintf(stderr, "Failed to get vfs lib client id, error code is %lld.\n", retError);
        return DSTORE_FAIL;
    }
#endif
    return CreateSpecVfs(tenantConfig, vfsName, &nodeId, VFS_ENABLE_IO_FENCE_FLAG, term);
}

RetStatus GetVfsClientId(uint64 *adapterClientId)
{
    ::ErrorCode retError = GetVfsLibClientId(GetDefaultVfsClientHandle(), adapterClientId);
    if (unlikely(retError)) {
        (void)fprintf(stderr, "Failed to get vfs lib client id, error code is %lld.\n", retError);
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

/*
 * PS guc params has set its default boot value before InitVfs when loading PS related libs
 * then SelectConfigFiles load config value to mem, so we need re-set the params after it
 */
void ReloadGssGUCParams()
{
    LoadGUCFlexConfig4VFS();
}

/*
 * Generally, update the parameter clientTimelineId in SAL once the SAL has been initialized, which is required
 * by the IOfencing.
 */
void UpdateNodeTimelineIdParameter(uint64 nodeTimelineId, const char* clusterName)
{
    /* Struct VfsLibParameter is initialed by zero, its member variables all will be invalid except for those
       that are assigned explicitly. */
    ::VfsLibParameter vfsLibPara = {};
    vfsLibPara.clientTimeLineId = nodeTimelineId;
    if (clusterName != nullptr) {
        ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg(
            "Update NodeTimelineIdParameter, clusterName %s, nodeTimelineId %lu.",
            clusterName, nodeTimelineId));
        errno_t rc = strcpy_s(vfsLibPara.clusterName, CLUSTER_NAME_MAX_LEN, clusterName);
        storage_securec_check(rc, "\0", "\0");
    } else {
        ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg(
            "Update NodeTimelineIdParameter, clusterName is null, nodeTimelineId %lu.",
            nodeTimelineId));
    }

    ErrorCode err = UpdateVfsLibParameter(GetDefaultVfsClientHandle(), &vfsLibPara);
    StorageReleasePanic(err != 0, MODULE_FRAMEWORK,
                        ErrMsg("Failed to update clientTimelineId via UpdateVfsLibParameter, error code = %lld", err));
}

}  // namespace VfsInterface
