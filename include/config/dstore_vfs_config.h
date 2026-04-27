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
 * dstore_vfs_config.h
 *
 * IDENTIFICATION
 *        include/config/dstore_vfs_config.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_DSTORE_VFS_CONFIG_H
#define DSTORE_DSTORE_VFS_CONFIG_H

#include "common/dstore_datatype.h"
#include "framework/dstore_instance_interface.h"
#include "framework/dstore_vfs_interface.h"
#include "vfs/vfs_interface.h"
#include "lock/dstore_lwlock.h"

namespace DSTORE {

constexpr uint32 MAX_VFS_TYPE_NAME_LENGTH{32};
constexpr uint32 MAX_CONFIG_NAME_LENGTH{256};
constexpr uint32 MAX_STORESPACE_NAME_LENGTH{256};
constexpr uint32 MAX_CONFIG_IP_LENGTH{16};
constexpr uint32 MAX_CONFIG_RDMA_DEVICE_NAME{16};

const char DEFAULT_VFS_CLUSTER_NAME[] = "cluster";
constexpr const char *GSS_FLUSH_THREAD_NUM = "pagestore_flush_thread_num";
constexpr const char *GSS_ASYNC_BATCH_PAGE_NUM = "pagestore_async_batch_page_num";
constexpr const char *GSS_ASYNC_FLUSH_INTERVAL = "pagestore_async_flush_interval";
constexpr const char *GSS_COMM_AUTH_TYPE = "pagestore_comm_auth_type";
constexpr const char *GSS_OLC_ALGORITHM = "pagestore_sal_olc_algorithm";
constexpr const char *GSS_SSL_PATH = "gaussstor_ssl_path";
constexpr const char *GSS_FILE_LOCK_TTL = "pagestore_file_lock_ttl";

constexpr uint16_t GAUSSDB_PROC_ROLE_BASE{0xE1F};
constexpr uint16_t PAGESTORE_CM_SERVICE_TYPE{0x0007};

constexpr int MAX_SSL_CIPHER_KEY_LEN = 1024;
/*
 * SYS_TABLE_STORESPACE
 * UNDO_STORESPACE
 * DATA_FILE_STORESPACE
 * REDO_STORESPACE
 * RESERVED_STORESPACE
 */
constexpr const char DEFAULT_STORESPACE[MAX_STORESPACE_NAME_LENGTH] = "storeSpaceName1";

enum VFSType : uint8 { VFS_TYPE_LOCAL_FS = 0, VFS_TYPE_PAGE_STORE };

/* DO NOT USE THE STRUCTURE DIRECTLY. */
struct VFSConfigCore {
    char vfsLibPath[MAXPGPATH];
    char vfsName[MAX_CONFIG_NAME_LENGTH];
    char vfsClusterName[MAX_CONFIG_NAME_LENGTH];
};

static_assert(std::is_pod<VFSConfigCore>::value);

struct VFSLocalFSConfig {
    VFSConfigCore core;
};

static_assert(std::is_pod<VFSLocalFSConfig>::value);

struct VFSPageStoreConfig {
    VFSConfigCore core;

    uint32 pageStoreClusterId;
    uint32 pageStoreCMNodeId;
    char pageStoreCMIp[MAX_CONFIG_IP_LENGTH];
    uint16 pageStoreCMPort;

    uint32 localNodeId;
    char localIp[MAX_CONFIG_IP_LENGTH];
    uint16 localPort;

    int commProtocolType;
    uint8 localRdmaGidIndex;
    uint8 localRdmaIbPort;
    char localRdmaDeviceName[MAX_CONFIG_RDMA_DEVICE_NAME];
};

static_assert(std::is_pod<VFSPageStoreConfig>::value);

struct VfsClientHandleDesc {
    ::VfsClientHandle* clientHandle;
    uint32 clusterId; /* aid cluster id */
    char clusterName[CLUSTER_NAME_MAX_LEN]; /* aid cluster name. */
};

extern void InitVfsClientHandles();
/**
 * Dynamically link the VFS library based on the given config file.
 *
 * @param configFilePath the path to the configuration file
 * @param instanceType the type of the storage instance
 * @param[out] vfsConfig parsed VFS config, forcible type casting will be needed to adapt to different VFS
 * configurations.
 *
 * @return DSTORE_SUCC means success, DSTORE_FAIL means failure
 */
extern RetStatus DynamicLinkVFS(void *tenantConfig, StorageInstanceType instanceType, uint32 clientId,
                                const char *datadir = nullptr, bool isInitDb = true);
/**
 * Create a VFS based on vfs name
 *
 * @param vfsName vfs name to be created
 * @param[out] vfs the VFS object to be created
 * @param ioFenceFlag io fence flag
 *
 * @return DSTORE_SUCC means success, DSTORE_FAIL means failure
 */
extern RetStatus CreateVFS(::VirtualFileSystem **vfs, const char *vfsName,
    const uint64 ioFenceFlag = VFS_ENABLE_IO_FENCE_FLAG);
/**
 * Open an existing VFS based on the given config
 *
 * @param tenantName tenant name of vfs to be created
 * @param vfsName name of vfs to be created
 * @param[out] vfs the VFS object to be opened
 * @param ioFenceFlag io fence flag
 *
 * @return DSTORE_SUCC means success, DSTORE_FAIL means failure
 */
extern RetStatus OpenVFS(const char *tenantName, const char *vfsName, ::VirtualFileSystem **vfs,
    const uint64 ioFenceFlag = VFS_ENABLE_IO_FENCE_FLAG);
/**
 * Close an existing VFS based on the given config
 *
 * @param vfsName the VFS name to be closed
 * @param vfs the VFS object to be closed
 *
 * @return DSTORE_SUCC means success, DSTORE_FAIL means failure
 */
extern RetStatus CloseVFS(const char *vfsName, ::VirtualFileSystem *vfs);
/**
 * Unlink the dynamic VFS library.
 *
 * @param vfs the VFS object to be uninitialized
 * @param vfsName uniquely identifies a VFS.
 * @param dropData if true, the whole data in the vfs system will be droped, otherwise it will be kept for use next time
 * the vfs is opened.
 */
extern void DynamicUnlinkVFS(::VirtualFileSystem *vfs, const char *vfsName, bool dropData);
/**
 * The whole data in the vfs system will be droped.
 *
 * @param vfsName uniquely identifies a VFS.
 */
extern RetStatus DropVfsDataForce(const char *vfsName, PdbId pdbId);
/**
 * Get the dynamic VFS library handle.
 *
 * @return handle pointer
 */
extern ::VfsLibHandle *GetVfsLibHandle();
extern LWLock *GetVfsHandleLock();
extern void InitVfsHandleLock();

/**
 * Vfs client handle interface.
 * Local vfs client: Each vfs lib has a default client handle, which is used to access the local gaussstor.
 *     GetDefaultVfsClientHandle
 * Disaster vfs client: Diaster vfs client is used to access the remote gaussstor, should be created when needed.
 *     GetVfsClientHandle
 *     ReleaseVfsClientHandle
 *     InitVfsClientHandleLock
 */
extern VfsClientHandle *GetDefaultVfsClientHandle();
/**
 * Get vfs client handle of aid cluster, create if not exist.
 *
 * @param clusterId id of aid cluster.
 * @param clusterName name of aid cluster, nullptr means trying to get client already exist.
 * @param notFound try to get client already exist, but not found.
 *
 * @return nullptr if failed, otherwise return vfs client handle.
 */
extern VfsClientHandle *GetVfsClientHandle(uint32 clusterId, bool needCreate = true, bool needUploadClientId = true);

using VfsInterface::UploadVfsClientIdCallback;
void RegisterUploadVfsClientIdCallback(UploadVfsClientIdCallback callback);
using VfsInterface::GsscmsCommInfoQueryCallback;
void RegisterGsscmsCommInfoQueryCallback(GsscmsCommInfoQueryCallback callback);

/**
 * Release an existing vfs client handle of aid cluster.
 *
 * @param clusterId id of aid cluster.
 *
 * @return DSTORE_SUCC means success, DSTORE_FAIL means failure
 */
extern RetStatus ReleaseVfsClientHandle(uint32 clusterId);
extern RetStatus ReleaseAllStandbyVfsClientHandle();
extern void InitVfsClientHandleLock();
/**
 * Open an existing VFS of aid cluster based on the given config.
 *
 * @param vfsClientHandle vfs client handle of aid cluster
 * @param tenantName tenant name of vfs to be created
 * @param vfsName name of vfs to be created
 * @param clusterId id of aid cluster
 * @param[out] vfs the VFS object to be opened
 * @param[out] needClearDrRelation the VFS is fenced or not exist
 *
 * @return DSTORE_SUCC means success, DSTORE_FAIL means failure
 */
extern RetStatus OpenVFS(VfsClientHandle *vfsClientHandle, const char *tenantName, const char *vfsName,
    uint32 clusterId, ::VirtualFileSystem **vfs, bool &needClearDrRelation);

extern StoreSpaceConfig *ChooseTheBestStoreSpaceConfig(StoreSpaceConfig *storeSpaceConfig, int cnt, const char *type);
extern ::VfsLibHandle* LoadVfsLibrary(const TenantConfig *config);
extern VfsLibHandle *SetupTenantIsolation(const TenantConfig *config, const char *datadir);
extern char *GetVfsClusterName();
extern char *GetVfsTenantName();
extern void LoadGUCFlexConfig4VFS();
extern bool IsLoadCipherWorkKey();

} /* namespace DSTORE */

#endif
