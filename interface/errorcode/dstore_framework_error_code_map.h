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
 */

#ifndef DSTORE_FRAMEWORK_ERROR_CODE_MAP_H
#define DSTORE_FRAMEWORK_ERROR_CODE_MAP_H

#include "errorcode/dstore_framework_error_code.h"

namespace DSTORE {

static ErrorDetails g_framework_error_code_map[] = {
    [STORAGE_OK] = {"STORAGE_OK", ""},
    [ERROR_GET_CODE(VFS_ERROR_FAILED_TO_CREATE_STORE_SPACE)] =
        {"VFS_ERROR_FAILED_TO_CREATE_STORE_SPACE", "VFS failed to create store space"},
    [ERROR_GET_CODE(VFS_ERROR_FAILED_TO_GET_LIBRARY)] =
        {"VFS_ERROR_FAILED_TO_GET_LIBRARY", "VFS failed to get library"},
    [ERROR_GET_CODE(VFS_ERROR_FILE_ALREADY_CREATED_OR_OPEN)] =
        {"VFS_ERROR_FILE_ALREADY_CREATED_OR_OPEN", "VFS file already created or open"},
    [ERROR_GET_CODE(VFS_ERROR_FAILED_TO_RENAME)] =
        {"VFS_ERROR_FAILED_TO_RENAME", "VFS failed to rename file"},
    [ERROR_GET_CODE(VFS_WARNING_FAILED_TO_CREATE_FILE)] =
        {"VFS_WARNING_FAILED_TO_CREATE_FILE", "VFS failed to create file"},
    [ERROR_GET_CODE(VFS_WARNING_FILE_NOT_CLOSED)] =
        {"VFS_WARNING_FILE_NOT_CLOSED", "VFS file not closed"},
    [ERROR_GET_CODE(VFS_WARNING_FAILED_TO_REMOVE_FILE)] =
        {"VFS_WARNING_FAILED_TO_REMOVE_FILE", "VFS failed to remove file"},
    [ERROR_GET_CODE(VFS_WARNING_FILE_ALREADY_EXISTS)] =
        {"VFS_WARNING_FILE_ALREADY_EXISTS", "VFS file already exists"},
    [ERROR_GET_CODE(VFS_WARNING_FILE_NOT_EXISTS)] =
        {"VFS_WARNING_FILE_NOT_EXISTS", "VFS file does not exist"},
    [ERROR_GET_CODE(VFS_FATAL_FAILED_TO_OPEN_FILE)] =
        {"VFS_FATAL_FAILED_TO_OPEN_FILE", "VFS failed to open file"},
    [ERROR_GET_CODE(VFS_WARNING_FAILED_TO_CLOSE_FILE)] =
        {"VFS_WARNING_FAILED_TO_CLOSE_FILE", "VFS failed to close file"},
    [ERROR_GET_CODE(VFS_WARNING_FILE_NOT_OPENED)] =
        {"VFS_WARNING_FILE_NOT_OPENED", "VFS file not opened"},
    [ERROR_GET_CODE(VFS_WARNING_FAILED_TO_PREAD_FILE)] =
        {"VFS_WARNING_FAILED_TO_PREAD_FILE", "VFS failed to pread file"},
    [ERROR_GET_CODE(VFS_WARNING_FSYNC_FILE_NOT_OPENED)] =
        {"VFS_WARNING_FSYNC_FILE_NOT_OPENED", "VFS failed to fsync opened file"},
    [ERROR_GET_CODE(VFS_WARNING_FAILED_TO_FSYNC)] =
        {"VFS_WARNING_FAILED_TO_FSYNC", "VFS failed to fsync"},
    [ERROR_GET_CODE(VFS_WARNING_FAILED_TO_GET_SIZE)] =
        {"VFS_WARNING_FAILED_TO_GET_SIZE", "VFS failed to get size"},
    [ERROR_GET_CODE(VFS_WARNING_FAILED_TO_EXTEND)] =
        {"VFS_WARNING_FAILED_TO_EXTEND", "VFS failed to extend"},
    [ERROR_GET_CODE(VFS_WARNING_FAILED_TO_TRUNCATE)] =
        {"VFS_WARNING_FAILED_TO_TRUNCATE", "VFS failed to truncate"},
    [ERROR_GET_CODE(VFS_WARNING_FAILED_TO_IO_FENCING)] =
        {"VFS_WARNING_FAILED_TO_IO_FENCING", "VFS failed to io fencing"},
    [ERROR_GET_CODE(VFS_WARNING_DUPLICATE_ADD_ACTIVE)] =
        {"VFS_WARNING_DUPLICATE_ADD_ACTIVE", "VFS failed to add activate again"},
    [ERROR_GET_CODE(VFS_WARNING_NO_AVAILABLE_SLOT)] =
        {"VFS_WARNING_NO_AVAILABLE_SLOT", "VFS failed to add activate due to no available cluster slot"},
    [ERROR_GET_CODE(VFS_WARNING_NULL_CLUSTER_NAME)] =
        {"VFS_WARNING_NULL_CLUSTER_NAME", "VFS failed to add activate due to cluster name is null"},
    [ERROR_GET_CODE(VFS_WARNING_FILE_DESCRIPTOR_EMPTY)] =
        {"VFS_WARNING_FILE_DESCRIPTOR_EMPTY", "VFS file descriptor is empty"},
    [ERROR_GET_CODE(VFS_ERROR_HASH_ENTRY_INSERT_FAILED)] =
        {"VFS_ERROR_HASH_ENTRY_INSERT_FAILED", "VFS adapter insert entry into hash failed"},
    [ERROR_GET_CODE(VFS_WARNING_HASH_ENTRY_FIND_FAILED)] =
        {"VFS_WARNING_HASH_ENTRY_FIND_FAILED", "VFS adapter find entry in hash failed"},
    [ERROR_GET_CODE(VFS_ERROR_HASH_ENTRY_REMOVE_FAILED)] =
        {"VFS_ERROR_HASH_ENTRY_REMOVE_FAILED", "VFS adapter remove entry in hash failed"},
    [ERROR_GET_CODE(VFS_ERROR_FAILED_TO_INIT_VFS_MODULE)] =
        {"VFS_ERROR_FAILED_TO_INIT_VFS_MODULE", "VFS failed to init vfs module"},
    [ERROR_GET_CODE(VFS_ERROR_FAILED_TO_CREATE_VFS)] =
        {"VFS_ERROR_FAILED_TO_CREATE_VFS", "VFS failed to create vfs"},
    [ERROR_GET_CODE(VFS_ERROR_FAILED_TO_OPEN_VFS)] =
        {"VFS_ERROR_FAILED_TO_OPEN_VFS", "VFS failed to open vfs"},
    [ERROR_GET_CODE(VFS_ERROR_FAILED_TO_MOUNT_VFS)] =
        {"VFS_ERROR_FAILED_TO_MOUNT_VFS", "VFS failed to mount vfs"},
    [ERROR_GET_CODE(VFS_ERROR_FAILED_TO_CREATE_SNAPSHOT)] =
        {"VFS_ERROR_FAILED_TO_CREATE_SNAPSHOT", "VFS failed to create snapshot"},
    [ERROR_GET_CODE(VFS_ERROR_FAILED_TO_OPEN_SNAPSHOT)] =
        {"VFS_ERROR_FAILED_TO_OPEN_SNAPSHOT", "VFS failed to open snapshot"},
    [ERROR_GET_CODE(VFS_ERROR_FAILED_TO_DROP_SNAPSHOT)] =
        {"VFS_ERROR_FAILED_TO_DROP_SNAPSHOT", "VFS failed to drop snapshot"},
    [ERROR_GET_CODE(VFS_ERROR_FAILED_TO_ROLLBACK_SNAPSHOT)] =
        {"VFS_ERROR_FAILED_TO_ROLLBACK_SNAPSHOT", "VFS failed to rollback snapshot"},
    [ERROR_GET_CODE(VFS_ERROR_FAILED_TO_GET_FILENAME)] =
        {"VFS_ERROR_FAILED_TO_GET_FILENAME", "VFS failed to get filename"},
    [ERROR_GET_CODE(VFS_ERROR_INVALID_PARAMETER)] =
        {"VFS_ERROR_INVALID_PARAMETER", "VFS adapter invalid parameter"},
    [ERROR_GET_CODE(VFS_ERROR_FAILED_TO_PREAD_ASYNC)] =
        {"VFS_ERROR_FAILED_TO_PREAD_ASYNC", "VFS failed to pread async"},
    [ERROR_GET_CODE(VFS_ERROR_FAILED_TO_PWRITE_SYNC)] =
        {"VFS_ERROR_FAILED_TO_PWRITE_SYNC", "VFS failed to pwrite sync"},
    [ERROR_GET_CODE(VFS_ERROR_FAILED_TO_PWRITE_ASYNC)] =
        {"VFS_ERROR_FAILED_TO_PWRITE_ASYNC", "VFS failed to pwrite async"},
    [ERROR_GET_CODE(VFS_ERROR_FAILED_TO_GET_LOCK)] =
        {"VFS_ERROR_FAILED_TO_GET_LOCK", "VFS failed to get lock"},
    [ERROR_GET_CODE(FRAMEWORK_ERROR_OUT_OF_MEMORY)] =
        {"FRAMEWORK_ERROR_OUT_OF_MEMORY", "Framework module out of memory"}
};

static_assert(sizeof(g_framework_error_code_map) != FRAMEWORK_ERROR_MAX, "Error code map size doesn't match");

} /* namespace DSTORE */

#endif
