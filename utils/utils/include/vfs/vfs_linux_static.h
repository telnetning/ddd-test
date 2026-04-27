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
 * vfs_linux_static.h
 *
 * Description:
 * This file defines linux static vfs functions
 *
 * ---------------------------------------------------------------------------------
 */
#ifndef UTILS_VFS_LINUX_STATIC_H
#define UTILS_VFS_LINUX_STATIC_H

#include "vfs/vfs_linux_common.h"

GSDB_BEGIN_C_CODE_DECLS

/**
 * Get static vfs for non-tenant related module with no vfs name
 * @param[in] memAllocator Memory allocator instance
 * @return
 */
VfsHandlePtr GetStaticLocalVfs(const MemAllocator *memAllocator);

/**
 * Release static vfs resource
 */
void ReleaseStaticLocalVfs(void);

/**
 * Check if static local vfs has fds not close
 * @return false if open file count is zero, otherwise true
 */
bool StaticVfsHasOpenFile(void);

/**
 * Get static vfs ops for non-tenant related module with no vfs name
 * @return
 */
VfsAdapterInterface *GetStaticVfsOps(void);

#ifdef ENABLE_UT
/**
 * Get open files count in static local vfs
 * @return
 */
int32_t GetStaticVfsOpenFileCount(void);
#endif

GSDB_END_C_CODE_DECLS

#endif /* UTILS_VFS_LINUX_STATIC_H */
