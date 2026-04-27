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

#include "vfs/vfs_error_code.h"
#include "vfs/vfs_adapter_interface.h"

#define REMOTE_VFS_ERROR_CODE_BEGIN 0x00010000

static GetVfsErrMsgFunc g_getRemoteVfsErrMsg = NULL;

typedef struct ErrCodeMsg ErrCodeMsg;
struct ErrCodeMsg {
    ErrorCode errorCode;
    const char *errorMsg;
};

static ErrCodeMsg g_errCodeMsgArray[] = {
    {VFS_ERROR_UNKNOWN_ERROR, "unknown error"},
    {VFS_ERROR_INVALID_ARGUMENT, "invalid argument"},
    {VFS_ERROR_RESOURCE_IS_EXIST, "resource already exist"},
    {VFS_ERROR_RESOURCE_NOT_EXIST, "resource does not exist"},
    {VFS_ERROR_RESOURCE_NOT_ENOUGH, "resource does not enough"},
    {VFS_ERROR_RESOURCE_NOT_RELEASE, "resource does not release"},
    {VFS_ERROR_NEED_RETRY_AGAIN, "need retry again"},
    {VFS_ERROR_COMMUNICATION_FAILED, "communication failed"},
    {VFS_ERROR_OPERATION_NOT_SUPPORT, "operation not support"},
    {VFS_ERROR_VFS_IS_EXIST, "vfs already exist"},
    {VFS_ERROR_VFS_NOT_EXIST, "vfs does not exist"},
    {VFS_ERROR_STORESPACE_IS_EXIST, "storespace already exist"},
    {VFS_ERROR_STORESPACE_NOT_EXIST, "storespace not exist"},
    {VFS_ERROR_STORESPACE_IN_USE, "storespace is using"},
    {VFS_ERROR_STORESPACE_SPACE_NOT_ENOUGH, "storespace space is not enough"},
    {VFS_ERROR_STORESPACE_UPDATE_BELOW_LIMIT, "storespace update is below limit"},
    {VFS_ERROR_FILE_IS_EXIST, "file already exist"},
    {VFS_ERROR_FILE_NOT_EXIST, "file does not exist"},
    {VFS_ERROR_FILE_NOT_CLOSE, "file does not close"},
    {VFS_ERROR_REQUEST_OVERLOAD, "requests too much or too frequent"},
    {VFS_ERROR_PAGESTORE_INNER_ERROR, "pagestore inner error"},
    {VFS_ERROR_PAGESTORE_POOL_SPACE_NOT_ENOUGH, "pagestore pool space is not enough"},
    {VFS_ERROR_VFS_IN_USE, "vfs is still in use"},
    {VFS_ERROR_FILE_READ_PADDING_BUFFER, "read padding buffer"},
    {VFS_ERROR_FILE_PADDING_NOT_CONTINUE, "padding not continue"},
    {VFS_ERROR_FILE_PADDING_CONFLICT, "padding conflict"},
    {VFS_ERROR_FILE_ALREADY_OPEN, "file already open"},
    {VFS_ERROR_FILE_WRITE_SEAL, "write file seal"},
    {VFS_ERROR_FILE_LOCK_INVALID, "filelock invalid"},
    {VFS_ERROR_FILE_WRITE_CONFLICT, "write conflict"},
    {VFS_ERROR_LOCAL_ADAPTER_NOT_INIT, "local vfs adapter does not init"},
    {VFS_ERROR_LOCAL_ADAPTER_ALREADY_INIT, "local vfs adapter already init"},
    {VFS_ERROR_LOCAL_FILE_OPERATION_FAILED, "local file operation failed"},
    {VFS_ERROR_OUT_OF_MEMORY, "out of memory"},
    {VFS_ERROR_VFS_MODULE_ALREADY_INIT, "vfs module already init"},
    {VFS_ERROR_VFS_MODULE_NOT_INIT, "vfs module does not init"},
    {VFS_ERROR_DISK_HAS_NO_SPACE, "not enough disk space"},
    {VFS_ERROR_OPENED_FILE_REACH_MAX, "open file descriptor reach max"},
    {VFS_ERROR_LOAD_ADAPTER_LIB_FAIL, "dlopen vfs dynamic library failed"},
    {VFS_ERROR_LOAD_ADAPTER_FUNC_FAIL, "dlsym vfs dynamic library function failed"},
    {VFS_ERROR_OFFLOAD_ADAPTER_LIB_FAIL, "dlclose vfs library handle failed"},
    {VFS_ERROR_FILE_ACCESS_DENIED, "local file access denied"},
    {VFS_ERROR_UNMOUNT_STATIC_VFS_FAIL, "cannot unmount static vfs handle"},
    {VFS_ERROR_MEM_ALLOCATOR_IS_SET, "memory allocator already set"},
    {VFS_ERROR_SECURE_FUNCTION_FAIL, "secure c function failed"},
    {VFS_ERROR_OPERATION_DENIED, "local file operation denied"},
    {VFS_ERROR_REOPEN_FILE_FAIL, "reopen file failed"},
    {VFS_ERROR_REMOVE_FILE_NOT_EXIST, "remove file not exist"},
    {VFS_ERROR_LOCK_FILE_FAIL, "local file lock failed"},
    {VFS_ERROR_UNLOCK_FILE_FAIL, "local file unlock failed"},
    {VFS_ERROR_AIO_CONTEXT_INITL_FAIL, "async io conetx"},
    {VFS_ERROR_AIO_WRITE_SUBMIT_FAIL, "async io write submit failed"},
    {VFS_ERROR_AIO_READ_SUBMIT_FAIL, "async io read submit failed"},
    {VFS_ERROR_AIO_GET_EVENTS_FAIL, "async io get events failed"},
    {VFS_ERROR_IO_FENCING_REFUSE, "io fencing refuse"},
    {VFS_ERROR_SAL_IS_STOPPED, "sal is stopped"},
    {VFS_ERROR_SNAPSHOT_IS_EXIST, "snapshot is exist"},
    {VFS_ERROR_SNAPSHOT_NOT_EXIST, "snapshot is not exist"},
    {VFS_ERROR_FILE_SNAPSHOT_NOT_EXIST, "file snapshot not exist"},
    {VFS_ERROR_SNAPSHOT_REACH_LIMIT, "file snapshot reach limit"},
    {VFS_ERROR_TENANT_NOT_EXIST, "tenant not exist"},
    {VFS_ERROR_TENANT_IS_EXIST, "tenant is exist"},
    {VFS_ERROR_TENANT_IN_USE, "tenant is in use"},
    {VFS_ERROR_IO_FENCING_REFUSE_BY_OWNER, "io fencing refuse by owner"},
    {VFS_ERROR_VFS_ERROR_IO_FENCE_TERMID_LOWER, "io fence termid lower"},
};

static const char *GetVfsErrMsgInternal(ErrorCode errorCode)
{
    for (uint32_t i = 0; i < sizeof(g_errCodeMsgArray) / sizeof(ErrCodeMsg); ++i) {
        if (g_errCodeMsgArray[i].errorCode == errorCode) {
            return g_errCodeMsgArray[i].errorMsg;
        }
    }
    return "unknown error";
}

UTILS_EXPORT const char *GetVfsErrMsg(ErrorCode errorCode)
{
    if (errorCode == ERROR_SYS_OK) {
        return "sys ok";
    }
    if (ERROR_GET_COMPONENT(errorCode) != UTILS_COMPONENT_ID || ERROR_GET_MODULE(errorCode) != UTILS_VFS_MODULE_ID) {
        return "not vfs module error";
    }
    uint32_t errCodeLow32 = (uint32_t)(ERROR_GET_CODE(errorCode));
    if (errCodeLow32 >= REMOTE_VFS_ERROR_CODE_BEGIN) {
        if (g_getRemoteVfsErrMsg != NULL) {
            return g_getRemoteVfsErrMsg(errorCode);
        } else {
            return "remote vfs error";
        }
    } else {
        return GetVfsErrMsgInternal(errorCode);
    }
}

void SetRemoteVfsErrMsgFunc(GetVfsErrMsgFunc func)
{
    g_getRemoteVfsErrMsg = func;
}
