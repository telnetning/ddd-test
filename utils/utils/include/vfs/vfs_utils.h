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
 * vfs_utils.h
 *
 * Description:
 * This file defines structure for vfs interface
 *
 * ---------------------------------------------------------------------------------
 */
#ifndef UTILS_VFS_UTILS_H
#define UTILS_VFS_UTILS_H

#include "vfs/vfs_interface.h"
#include "vfs/vfs_adapter_interface.h"

GSDB_BEGIN_C_CODE_DECLS

struct VfsLibHandle {
    void *libHandle;
    DeleteVfsAdapterInterfaceFunc deleteAdapterInterfaceFunc;
    AdapterInfo adapterInfo;
    VfsAdapterInterface *ops;
};

struct VfsClientHandle {
    void *clientHandle;
    VfsAdapterInterface *ops;
};

struct VirtualFileSystem {
    void *vfsHandle;
    VfsAdapterInterface *ops;
    bool isStaticVfs;
};

struct FileDescriptor {
    void *fileHandle;
    VfsAdapterInterface *ops;
};

GSDB_END_C_CODE_DECLS

#endif /* UTILS_VFS_UTILS_H */
