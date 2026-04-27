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
 * dstore_vfs_interface.h
 *
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/dstore/interface/table/dstore_vfs_interface.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_VFS_A_INTERFACE_H
#define DSTORE_VFS_A_INTERFACE_H

#include "common/dstore_common_utils.h"
#include "framework/dstore_vfs_file_interface.h"
#include "framework/dstore_instance_interface.h"
#include "vfs/vfs_interface.h"
#include "config_parser/config_parser.h"

namespace VfsInterface {
#pragma GCC visibility push(default)

constexpr const char *DISK_PERF_DESC_HIGH = "H";
constexpr const char *DISK_PERF_DESC_MEDIUM = "M";
constexpr const char *DISK_PERF_DESC_LOW = "L";
enum class DISK_PERF_TYPE {
    DISK_PERF_HIGH = 0,
    DISK_PERF_MEDIUM = 1,
    DISK_PERF_LOW = 2,
    COUNT
};

bool ModuleInitialize();
DSTORE::RetStatus LibInitialize(void *tenantConfig, DSTORE::StorageInstanceType type, uint32_t clientId);

VfsLibHandle *GetVfsModuleLib();

DSTORE::RetStatus OpenVfs(const char *tenantName, const char *vfsName, ::VirtualFileSystem **vfs);
DSTORE::RetStatus CloseVfs(const char *vfsName, ::VirtualFileSystem *vfs);

using CmcIoFencingCallback = long long (*) (void);
void SetIoFencingCallback(CmcIoFencingCallback callback);

using UploadVfsClientIdCallback = ErrorCode (*)(uint32_t, uint64_t);
void SetUploadVfsClientIdCallback(UploadVfsClientIdCallback callback);
using GsscmsCommInfoQueryCallback = ErrorCode (*)(uint32_t, DSTORE::AidClusterGsscmsCommInfo *);
void SetGsscmsCommInfoQueryCallback(GsscmsCommInfoQueryCallback callback);
VfsClientHandle *CreateDisasterVfsClientHandle(uint32 clusterId);

DSTORE::StoreSpaceConfig *ChooseTheBestStoreSpaceConfig(DSTORE::StoreSpaceConfig *storeSpaceConfig, int cnt,
                                                        const char *type);
::VfsLibHandle *SetupPageStore(const DSTORE::TenantConfig *config);
::VfsLibHandle *SetupTenantIsoland(const DSTORE::TenantConfig *config, const char* basePath);

DSTORE::RetStatus CreateTenantDefaultVfs(DSTORE::TenantConfig *tenantConfig);
DSTORE::RetStatus CreateTemplateVfs(DSTORE::TenantConfig *tenantConfig, const char *vfsName);

DSTORE::RetStatus GetVfsClientId(uint64 *adapterClientId);
bool AddDrClusterId(uint32 *clusterIdArr, uint32 clusterId, uint32 arrSize, uint32 *realSize);

void ReloadGssGUCParams();
void UpdateNodeTimelineIdParameter(uint64 nodeTimelineId, const char* clusterName);
#pragma GCC visibility pop


}  // namespace VfsInterface

#endif
