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
 * vfs_interface.h
 *
 * Description:
 * This file defines vfs interface for file operations
 *
 * ---------------------------------------------------------------------------------
 */
#ifndef UTILS_VFS_INTERFACE_H
#define UTILS_VFS_INTERFACE_H

#include "defines/common.h"
#include "types/data_types.h"
#include "memory/memory_allocator.h"
#include "vfs/vfs_error_code.h"

GSDB_BEGIN_C_CODE_DECLS

#define CLUSTER_NAME_MAX_LEN  256
#define OWNER_NAME_MAX_LEN      (CLUSTER_NAME_MAX_LEN)
#define TENANT_NAME_MAX_LEN   256
#define VFS_NAME_MAX_LEN      256
#define FILE_PATH_MAX_LEN     1024
#define VFS_LIB_ATTR_LEN      4096
#define VFS_MAX_IP_LEN        46
#define MAX_CLIENT_ID_COUNT   1024
#define MAX_OWNER_COUNT       16
#define SNAPSHOT_NAME_MAX_LEN 256
#define RANGE_UNIT            (64 << 10) /* 64K */

/* Vfs module initialize interfaces */
/**
 * Init vfs module with memory allocator, if memory allocator is NULL, vfs module will use system memory function
 * @param memAllocator Memory allocator instance
 * @return
 */
ErrorCode InitVfsModule(const MemAllocator *memAllocator);

/**
 * Close vfs module
 * @return
 */
ErrorCode ExitVfsModule(void);

/**
 * Get error message by error code
 * @param[in] errorCode error code to get error message
 * @return error message
 */
const char *GetVfsErrMsg(ErrorCode errorCode);

/* VFS interfaces */
typedef struct VfsLibHandle VfsLibHandle;
typedef struct VfsClientHandle VfsClientHandle;
typedef struct VirtualFileSystem VirtualFileSystem;
typedef struct VfsLibParameter VfsLibParameter;
struct VfsLibParameter {
    uint16_t pageSize;
    uint16_t dbType;
    char storageServerAddr[VFS_LIB_ATTR_LEN]; /* storage cluster manager Address List */
    char localIp[VFS_MAX_IP_LEN];             /* local ip address */
    char clusterName[CLUSTER_NAME_MAX_LEN];
    uint16_t localServiceType;
    uint32_t clientId; /* Specify current node id */
    uint64_t clientTimeLineId;
};

/* VFS attribute flags */
#define VFS_DISABLE_IO_FENCE_FLAG (0ULL)
#define VFS_ENABLE_IO_FENCE_FLAG  (1ULL)
/* VFS default flags will disable all attributes */
#define VFS_DEFAULT_ATTR_FLAG (VFS_DISABLE_IO_FENCE_FLAG)

/* vfs drop attribute flags */
#define VFS_FORCE_DELETE_FLAG     (0ULL)
#define VFS_NOT_FORCE_DELETE_FLAG (1ULL)
#define VFS_DEFAULT_DELETE_FLAG   (VFS_FORCE_DELETE_FLAG)

/* VFS control related definitions */
#define SET_VFS_IO_FENCE_WHITELIST 1
#define GET_VFS_IO_FENCE_WHITELIST 2
#define ADD_VFS_IO_FENCE_OWNER 3
#define DEL_VFS_IO_FENCE_OWNER 4
#define GET_VFS_IO_FENCE_OWNER 5

/* Special client id */
#define VFS_IO_FENCE_NOT_FILTER_CLIENTID (4294967295U)

/* StoreSpace tenant attribute flags */
#define STORE_TENANT_NOT_FORCE_DELETE_FLAG (0ULL)
#define STORE_TENANT_FORCE_DELETE_FLAG     (1ULL)
#define STORE_TENANT_DEFAULT_DELETE_FLAG   (STORE_TENANT_NOT_FORCE_DELETE_FLAG)

typedef struct VfsControlInfo VfsControlInfo;
struct VfsControlInfo {
    char vfsName[VFS_NAME_MAX_LEN];
    ErrorCode retCode;
    union {
        struct {
            uint64_t clientIds[MAX_CLIENT_ID_COUNT]; /* client id array */
            uint64_t clientTimeLineIds[MAX_CLIENT_ID_COUNT];
            uint32_t clientIdCount;                  /* total client count */
            uint64_t term;
            char ownerName[OWNER_NAME_MAX_LEN];
        } ioFence;
        struct {
             char ownerName[MAX_OWNER_COUNT][OWNER_NAME_MAX_LEN];
             uint32 ownerCount;
             uint64 term;
        } ioFenceOwner;
    } info;
};

/**
 * Get a local vfs instance for non-tenant related module with no vfs name
 * @param[out] vfs VirtualFileSystem instance, used to access files
 * @return
 */
ErrorCode GetStaticLocalVfsInstance(VirtualFileSystem **vfs);

/**
 * Load VfsHandle lib
 * @param[in] vfsLibPath VFS lib full path
 * @param[out] vfsLibHandle vfs lib instance
 * @return
 */
ErrorCode LoadVfsLib(const char *vfsLibPath, const MemAllocator *memAllocator, VfsLibHandle **vfsLibHandle);

/**
 * Offload VfsHandle lib
 * @param[in] vfsLibHandle vfs lib instance
 * @return
 */
ErrorCode OffloadVfsLib(VfsLibHandle *vfsLibHandle);

/**
 * Set Vfs adapter configure, only PageStore library has settable parameters in current version
 *
 * Following is PageStore settable parameters:
 * all vfs adapter configure parameters and their valid value ranges, value string length is less than 64 characters
 * EXAMPLE: pagestore_xxx_xxx: [min,max]
 * pagestore_max_file_handle_size:          [64,UINT64_MAX]  Default: 65536
 * pagestore_rpc_default_timeout:           [0,INT32_MAX] (ms)  Default: 10000
 * pagestore_rpc_print_interval:            [1,UINT32_MAX]  Default: 60
 * pagestore_max_rpc_retry_times:           [0,UINT32_MAX]  Default: 1000
 * pagestore_sal_memcontext_size:           [16777216,UINT64_MAX] (B)  Default: 256*1024*1024
 * pagestore_async_write_memcontext_size:   [16777216,UINT64_MAX] (B)  Default: 128*1024*1024
 * pagestore_sal_page_size:                 [8192,8192] (B)  Default: 8192
 * pagestore_sal_max_thread_num:            [2,8192]  Default: 1024
 * pagestore_alloc_wait_interval:           [1,UINT32_MAX]  Default: 10
 * pagestore_max_alloc_retry_times:         [0,UINT32_MAX]  Default: 300
 * pagestore_retry_wait_interval:           [1,UINT32_MAX]  Default: 10
 * pagestore_retry_wait_time:               [0,UINT32_MAX] (us)  Default: 2000
 * pagestore_flush_thread_num:              [0,8192]  Default: 0
 * pagestore_retry_thread_num:              [1,8192]  Default: 2
 * pagestore_completion_thread_num:         [0,8192]  Default: 0
 * pagestore_perf_scheduler_interval:       [1,UINT32_MAX]  Default: 60
 * pagestore_file_lock_ttl:                 [1,UINT64_MAX] (ms)  Default: 30000
 * pagestore_enable_multi_write:            0(false), 1(true)  Default: 0
 * pagestore_perf_level:                    0(CLOSE), 1(DEBUG), 2(RELEASE), 255(OFF)  Default: 2
 * pagestore_local_ip:                      valid IP address (string length is less than 15 characters)
 *
 * @param[in] vfsLibHandle vfs lib instance
 * @param[in] para para parameter name
 * @param[in] value value parameter value
 * @return
 */
ErrorCode SetVfsLibConfig(VfsLibHandle *vfsLibHandle, const char *para, const char *value);

/**
 * Get Vfs adapter configure
 * @param[in] vfsLibHandle vfs lib instance
 * @param[in] para para parameter name
 * @param[out] value value parameter value
 * @param[in] len length of value
 * @return
 */
ErrorCode GetVfsLibConfig(VfsLibHandle *vfsLibHandle, const char *para, char *value, uint64_t len);

/**
 * Init VfsHandle lib with parameters
 * @param[in] vfsLibHandle vfs lib instance
 * @param[in] param vfs lib parameters, including ip list...
 * @return
 */
ErrorCode InitVfsLib(VfsLibHandle *vfsLibHandle, const VfsLibParameter *param);

/**
 * Stop VfsHandle lib
 * @param[in] vfsLibHandle vfs lib instance
 * @return
 */
ErrorCode StopVfsLib(VfsLibHandle *vfsLibHandle);

/**
 * create vfs client instance handle
 * @param[in] vfsLibHandle vfs lib instance
 * @param[in] clusterName vfs cluster name
 * @param[in] authType: encryption authentication mode for connecting to the vfs.
 * @param[in] storageServerAddr: ip list of the storage server node
 * @param[out] vfsClientHandle: vfs ClientHandle instance
 * @return 0 means success, other means failure
 */
ErrorCode CreateVfsClientHandle(VfsLibHandle *vfsLibHandle, const char *clusterName, uint32_t authType,
    const char *storageServerAddr, VfsClientHandle **vfsClientHandle);

/**
 * delete vfs client instance handle
 * @param[in] vfsClientHandle: vfs ClientHandle instance
 * @return 0 means success, other means failure
 */
ErrorCode DeleteVfsClientHandle(VfsClientHandle *vfsClientHandle);

/**
 * Set vfs adapter configure
 * @param[in] vfsClientHandle: vfs ClientHandle instance
 * @param[in] para parameter name
 * @param[in] value parameter value
 * @return
 */
ErrorCode SetVfsClientConfig(VfsClientHandle *vfsClientHandle, const char *para, const char *value);

/**
 * Get vfs adapter configure
 * @param[in] vfsClientHandle: vfs ClientHandle instance
 * @param[in] para parameter name
 * @param[out] value parameter value
 * @param[in] len length of value
 * @return
 */
ErrorCode GetVfsClientConfig(VfsClientHandle *vfsClientHandle, const char *para, char *value, uint64_t len);

/**
 * Update VfsHandle lib parameter info
 * @param[in] vfsClientHandle: vfs ClientHandle instance
 * @param[in] param vfs lib parameters, including ip list...
 * @return
 */
ErrorCode UpdateVfsLibParameter(VfsClientHandle *vfsClientHandle, const VfsLibParameter *param);

/**
 * Get VfsHandle client id
 * @param[in] vfsClientHandle: vfs ClientHandle instance
 * @param[out] clientId client id
 * @return
 */
ErrorCode GetVfsLibClientId(const VfsClientHandle *vfsClientHandle, uint64_t *clientId);

/**
 * Vfs adapter remote message handler for communication lib
 * @param[in] vfsClientHandle: vfs ClientHandle instance
 * @param[in] msgRequest remote message instance
 * @param[in] handleMsgContext remote message handler context
 */
void VfsLibHandlerRemoteMsg(VfsClientHandle *vfsClientHandle, void *msgRequest, void *handleMsgContext);

/**
 * Create tenant
 * @param[in] vfsClientHandle: vfs ClientHandle instance
 * @param[in] clusterName cluster name with max length 256
 * @param[in] tenantName tenant name with max length 256
 * @return 0 means success, other means failure
 */
ErrorCode CreateStoreTenant(VfsClientHandle *vfsClientHandle, const char *clusterName, const char *tenantName);

/**
 * Delete tenant
 * @param[in] vfsClientHandle: vfs ClientHandle instance
 * @param[in] clusterName cluster name with max length 256
 * @param[in] tenantName tenant name with max length 256
 * @param[in] attr attribute flags
 * @return 0 means success, other means failure
 */
ErrorCode DeleteStoreTenant(VfsClientHandle *vfsClientHandle, const char *clusterName, const char *tenantName,
                            uint64_t attr);

/**
 * Create VFS in storage with special attribute flags, does not mount when create vfs successful
 * @param[in] vfsClientHandle: vfs ClientHandle instance
 * @param[in] tenantName tenant name with max length 256
 * @param[in] vfsName vfs name with max length 256
 * @param[in] attrFlags attr vfs attribute flags, current support flags:
 *            1. VFS io fence ability: {VFS_DISABLE_IO_FENCE_FLAG, VFS_ENABLE_IO_FENCE_FLAG}
 * @return
 */
ErrorCode CreateVfs(VfsClientHandle *vfsClientHandle, const char *tenantName, const char *vfsName, uint64_t attrFlags);

/**
 * Drop VFS in storage system
 * @param[in] vfsClientHandle: vfs ClientHandle instance
 * @param[in] tenantName tenant name
 * @param[in] vfsName VFS name
 * @return 0 means success, other means failure
 */
ErrorCode DropVfs(VfsClientHandle *vfsClientHandle, const char *tenantName, const char *vfsName);

/**
 * Mount VFS in VfsMgr, file operations can be used after calling MountVfs successfully
 * @param[in] vfsClientHandle: vfs ClientHandle instance
 * @param[in] tenantName tenant name
 * @param[in] vfsName VFS name
 * @param[out] vfs VirtualFileSystem instance, used to access files
 * @return 0 means success, other means failure
 */
ErrorCode MountVfs(VfsClientHandle *vfsClientHandle, const char *tenantName, const char *vfsName,
                   VirtualFileSystem **vfs);

/**
 * Unmount VFS in VfsMgr
 * @param[in] vfs VirtualFileSystem instance
 * @return 0 means success, other means failure
 */
ErrorCode UnmountVfs(VirtualFileSystem *vfs);

/**
 * VFS control interface
 * @param[in] vfsClientHandle: vfs ClientHandle instance
 * @param[in] tenantName tenant name
 * @param[in] cmd vfs control operation cmd id
 * @param[in,out] vfsControlInfo vfs control information
 * @param[in] count array count of vfsControlInfo
 * @return
 */
ErrorCode VfsControl(VfsClientHandle *vfsClientHandle, const char *tenantName, int cmd, VfsControlInfo *vfsControlInfo,
                     uint32_t count);

/* Snapshot interfaces */

/* Snapshot flags */
#define SNAPSHOT_ALL_FILE_FLAG 0x00
#define SNAPSHOT_IN_PLACE_UPDATE_FILE_FLAG 0x01
#define SNAPSHOT_APPEND_FILE_FLAG 0x02

/**
 * Creat a snapshot in VFS with special flags
 * @param[in] vfsClientHandle: vfs ClientHandle instance
 * @param[in] tenantName tenant name
 * @param[in] vfsName VFS name
 * @param[in] snapshotName snapshot name
 * @param[in] flags snapshot flags
 * @return
 */
ErrorCode CreateSnapshot(VfsClientHandle *vfsClientHandle, const char *tenantName, const char *vfsName,
                         const char *snapshotName, int64_t flags);

/**
 * Drop a snapshot in VFSs
 * @param[in] vfsClientHandle: vfs ClientHandle instance
 * @param[in] tenantName tenant name
 * @param[in] vfsName VFS name
 * @param[in] snapshotName snapshot name
 * @return
 */
ErrorCode DropSnapshot(VfsClientHandle *vfsClientHandle, const char *tenantName, const char *vfsName,
                       const char *snapshotName);

/**
 * Rollback a snapshot in VFS
 * @param[in] vfsClientHandle: vfs ClientHandle instance
 * @param[in] tenantName tenant name
 * @param[in] vfsName VFS name
 * @param[in] snapshotName snapshot name
 * @return
 */
ErrorCode RollbackSnapshot(VfsClientHandle *vfsClientHandle, const char *tenantName, const char *vfsName,
                           const char *snapshotName);

/* StoreSpace interfaces */

/* Store space pool disk perf flags */
#define HIGH_DISK_PERF_FLAG 0x01
#define MEDIUM_DISK_PERF_FLAG 0x02
#define LOW_DISK_PERF_FLAG 0x04

/* The max length of the storespace name is 128 bytes */
#define STORESPACE_NAME_MAX_LEN 128

/* Storespace size is unlimited */
#define UNLIMITED_STORESPACE_SIZE UINT64_MAX

typedef struct StoreSpaceAttr {
    /**
     * pool disk perf flags which is an unchangeable parameter,
     * Disk perf flag: {HIGH_DISK_PERF_FLAG, MEDIUM_DISK_PERF_FLAG, LOW_DISK_PERF_FLAG}
     */
    uint64 attrFlags;
    /**
     * The unit is MB. when the maxSize is UNLIMITED_STORESPACE_SIZE,
     * pagestore will set store space to unlimited.
     */
    uint64 maxSize;
    uint64 reserved;
} StoreSpaceAttr;

/**
 * Create a number of store spaces
 * @param[in] vfsClientHandle: vfs ClientHandle instance
 * @param[in] tenantName tenant name
 * @param[in] storeSpaceNames store space name list
 * @param[in] attrs store space pool attribute
 * @param[in] count the number of store space
 * @return
 */
ErrorCode CreateStoreSpace(VfsClientHandle *vfsClientHandle, const char *tenantName, const char **storeSpaceNames,
                           const StoreSpaceAttr *attrs, uint32 count);

/**
 * Delete a number of store spaces
 * @param[in] vfsClientHandle: vfs ClientHandle instance
 * @param[in] tenantName tenant name
 * @param[in] storeSpaceNames store space name list
 * @param[in] count the number of store space
 * @return
 */
ErrorCode DeleteStoreSpace(VfsClientHandle *vfsClientHandle, const char *tenantName, const char **storeSpaceNames,
                           uint32 count);

/**
 * Update a number of store spaces
 * @param[in] vfsClientHandle: vfs ClientHandle instance
 * @param[in] tenantName tenant name
 * @param[in] storeSpaceNames store space name list
 * @param[in] attrs store space pool attribute
 * @param[in] count the number of store space
 * @return
 */
ErrorCode UpdateStoreSpace(VfsClientHandle *vfsClientHandle, const char *tenantName, const char **storeSpaceNames,
                           const StoreSpaceAttr *attrs, uint32 count);

/**
 * Query the attributes of the list of store spaces
 * @param[in] vfsClientHandle: vfs ClientHandle instance
 * @param[in] tenantName tenant name
 * @param[in] storeSpaceNames store space name list
 * @param[in] attrs store space pool attribute
 * @param[in] count the number of store space
 * @return
 */
ErrorCode QueryStoreSpaceAttr(VfsClientHandle *vfsClientHandle, const char *tenantName, const char **storeSpaceNames,
                              StoreSpaceAttr *attrs, uint32 count);

/**
 * Query the used size of the list of store spaces
 * @param[in] vfsClientHandle: vfs ClientHandle instance
 * @param[in] tenantName tenant name
 * @param[in] storeSpaceNames store space name list
 * @param[in] usedSizes store space used sizes
 * @param[in] count the number of store space
 * @return
 */
ErrorCode QueryStoreSpaceUsedSize(VfsClientHandle *vfsClientHandle, const char *tenantName,
                                  const char **storeSpaceNames, uint64 *usedSizes, uint32 count);

/* File interfaces */

/* File write type flags in FileParameter.flag */
#define IN_PLACE_WRITE_FILE 0x01
#define APPEND_WRITE_FILE   0x02
/* File sub flags in FileParameter.fileSubType */
/* File sub flags of IN_PLACE_WRITE_FILE */
#define DATA_FILE_TYPE 0x00
#define TEMP_FILE_TYPE 0x01
#define VOTE_DISK_FILE_TYPE 0x02

/* File sub flags of APPEND_WRITE_FILE */
#define AUDIT_LOG_FILE_TYPE         0x01
#define ERR_LOG_FILE_TYPE           0x02
#define CSV_LOG_FILE_TYPE           0x03
#define CONFIG_FILE_TYPE            0x04
#define PLOG_LOG_FILE_TYPE          0x05
#define SQL_LOG_FILE_TYPE           0x06
#define ASP_LOG_FILE_TYPE           0x07
#define WAL_LOG_FILE_TYPE           0x08
#define IMMUTABLE_WAL_LOG_FILE_TYPE 0x10

/* File open flags */
/* Those flags below are mutex and mandatory */
#define FILE_READ_AND_WRITE_FLAG 0x01
#define FILE_READ_ONLY_FLAG      0x02
#define FILE_WRITE_ONLY_FLAG     0x03
/* Those flags below are combinable */
#define FILE_TRUNC_FLAG  (1 << 3)
#define FILE_APPEND_FLAG (1 << 4)
/**
 * WARNING: When use directIO, all IO operations need to be
 * done in block-size multiples of page size.
 */
#define FILE_DIRECT_IO_FLAG (1 << 5)
#define FILE_SYNC_FLAG      (1 << 6)
#define FILE_ASYNC_IO_FLAG  (1 << 7)
/* File create mode */
#define FILE_READ_AND_WRITE_MODE 0x01
#define FILE_GROUP_READ_MODE     (1 << 1)
#define FILE_GROUP_WRITE_MODE    (1 << 2)
#define FILE_OTHER_READ_MODE     (1 << 3)
#define FILE_OTHER_WRITE_MODE    (1 << 4)
/* File seek flags in FileSeek() */
#define FILE_SEEK_SET 0x00
#define FILE_SEEK_CUR 0x01
#define FILE_SEEK_END 0x02

/* File max size definitions */
/* Append file offset is 32-bit(limited by PageStore), max append file size is 2^32 Bytes */
#define APPEND_FILE_MAX_SIZE 0xFFFFFFFFU
/* Update file offset is 64-bit, max update file size is 2^64 Bytes */
#define UPDATE_FILE_MAX_SIZE 0xFFFFFFFFFFFFFFFFU

/* Default range size (64M) used in PageStore */
#define DEFAULT_RANGE_SIZE (64 * 1024 * 1024)

/* Default file stream id */
#define VFS_INVALID_FILE_STREAM_ID 0xFFFFFFFF
#define VFS_DEFAULT_FILE_STREAM_ID 0xFFFFFFFE

typedef struct FileDescriptor FileDescriptor;

typedef struct FileParameter FileParameter;
struct FileParameter {
    char storeSpaceName[STORESPACE_NAME_MAX_LEN];
    uint32_t streamId;
    uint16_t flag;        /* File write type (append or inplace-update) */
    uint16_t fileSubType; /* file sub-type (append: Wal Append file /runnning Log File) */
    uint32_t rangeSize;   /* At least multiple of 8K */
    uint64_t maxSize;
    uint64_t recycleTtl; /* minute, delete the file according to the last record's TTL timestamp */
    int mode;
    bool isReplayWrite;
};

typedef struct FileOpenParam FileOpenParam;
struct FileOpenParam {
    int32_t flags;            /* file open flags, refer to POSIX definitions */
    const char *filePath;     /* file path, used in local file system */
    const char *snapshotName; /* snapshot name */
    const char *preSnapshotName; /* previous snapshot name */
};

/* File control related definitions */
typedef void (*FlushCallback)(FileDescriptor *fd, int64_t offset, ErrorCode errorCode, void *asyncContext);

#define SET_FILE_FLUSH_CALLBACK   1 /* Setting write flush callback function */
#define SET_FILE_ZCOPY_MEMORY_KEY 2 /* Setting zero copy memory key */

typedef union FileControlInfo FileControlInfo;
union FileControlInfo {
    struct {
        FlushCallback callback;
        void *asyncContext;
    } flushCallbackInfo;
    struct {
        uint64_t memKey;
    } zeroCopyMemKey;
    struct {
        int64_t offset;
        int64_t length;
    } paddingFileInfo;
};

/* File lock flags */
/* Exclusive lock on the specified section of the file, return error if file is already locked by others */
#define FILE_EXCLUSIVE_LOCK 2

/* Async write related */
typedef void (*AsyncIoCallback)(ErrorCode errorCode, int64_t successSize, void *asyncContext);
typedef struct AsyncIoContext AsyncIoContext;
struct AsyncIoContext {
    AsyncIoCallback callback;
    void *asyncContext;
};

/* Snapshot read related */
/* The maximum number of data blocks returned by a snapshot read */
#define SNAPSHOT_READ_MAX_DATA_COUNT 64
typedef struct DiffBlock DiffBlock;
struct DiffBlock {
    int64_t startOffset;
    int64_t len;
    void *buff;
};
typedef struct DiffContents DiffContents;
struct DiffContents {
    int64_t comparedSize; /* the number of bytes compare */
    uint64_t diffBlockNums;
    DiffBlock diffBlocks[SNAPSHOT_READ_MAX_DATA_COUNT];
};

/**
 * Create a file in VFS
 * @constraint fileName with .. to backtrack to upper level directory is not permit
 * @param[in] vfs VirtualFileSystem instance
 * @param[in] fileName local store file name or PageStore file name
 * @param[in] fileParameter file type, store space id, etc
 * @param[out] fd FileDescriptor instance
 * @return
 */
ErrorCode Create(VirtualFileSystem *vfs, const char *fileName, FileParameter fileParameter, FileDescriptor **fd);

/**
 * Remove a file in VFS
 * @constraint fileName with .. to backtrack to upper level directory is not permit
 * @param[in] vfs VirtualFileSystem instance
 * @param[in] fileName local store file name or PageStore file name
 * @return
 */
ErrorCode Remove(VirtualFileSystem *vfs, const char *fileName);

/**
 * Check a file is exist or not
 * @constraint fileName with .. to backtrack to upper level directory is not permit
 * @param[in] vfs VirtualFileSystem instance
 * @param[in] fileName local store file name or PageStore file name
 * @param[out] out file existence result
 * @return
 */
ErrorCode FileIsExist(VirtualFileSystem *vfs, const char *fileName, bool *out);

/**
 * Open a file in VFS
 * @constraint fileName with .. to backtrack to upper level directory is not permit
 * @param[in] vfs VirtualFileSystem instance
 * @param[in] fileName local store file name or PageStore file name
 * @param[in] flags file open flags, refer to POSIX definitions
 * @param[out] fd FileDescriptor instance
 * @return
 */
ErrorCode Open(VirtualFileSystem *vfs, const char *fileName, int flags, FileDescriptor **fd);

/**
 * Open a file with snapshot in VFS
 * @constraint fileName with .. to backtrack to upper level directory is not permit
 * @param[in] vfs VirtualFileSystem instance
 * @param[in] openPara flags, fileName and snapshotName
 *                     flags file open flags, refer to POSIX definitions
 *                     fileName local store file name or PageStore file name
 *                     snapshotName snapshot name
 * @param[out] fd FileDescriptor instance
 * @return
 */
ErrorCode OpenSnapshot(VirtualFileSystem *vfs, FileOpenParam openPara, FileDescriptor **fd);

/**
 * Rename a file in VFS
 * @param[in] vfs VirtualFileSystem instance
 * @param[in] srcFileName source file name
 * @param[in] destFileName destination file name
 * @return
 */
ErrorCode RenameFile(VirtualFileSystem *vfs, const char *srcFileName, const char *destFileName);

/**
 * Close a file in VFS
 * @param[in] fd FileDescriptor instance
 * @return
 */
ErrorCode Close(FileDescriptor *fd);

/**
 * Sync write data to storage
 * @param[in] fd FileDescriptor instance
 * @return
 */
ErrorCode Fsync(FileDescriptor *fd);

/**
 * Repositions the offset of the open file base on file seek flag
 * FILE_SEEK_SET: The offset is set to offset bytes
 * FILE_SEEK_CUR: The offset is set to its current location plus offset bytes
 * FILE_SEEK_END: The offset is set to the size of the file plus offset bytes
 * @param[in] fd FileDescriptor instance
 * @param[in] offset Gap size between file seek flag and target offset
 * @param[in] seekFlag file seek flag
 * @param[out] newPos result position of current fd
 * @return the resulting offset of current fd as measured in bytes from the beginning of the file
 */
ErrorCode FileSeek(FileDescriptor *fd, int64_t offset, int seekFlag, int64_t *newPos);

/**
 * Set the file position to 0 of current fd
 * @param[in] fd FileDescriptor instance
 * @return
 */
ErrorCode Rewind(FileDescriptor *fd);

/**
 * Reads up to {count} bytes at current position of {fd} into the buffer starting at {buf}
 * @param[in] fd FileDescriptor instance
 * @param[in,out] buf buffer pointer
 * @param[in] count read bytes
 * @param[out] readSize the number of bytes read
 * @return
 */
ErrorCode Read(FileDescriptor *fd, void *buf, uint64_t count, int64_t *readSize);

/**
 * Reads up to {count} bytes from file at {offset} (from the start of the file) into the buffer starting at {buf}
 * @param[in] fd FileDescriptor instance
 * @param[in,out] buf buffer pointer
 * @param[in] count read bytes
 * @param[in] offset read start position from start of the file
 * @param[out] readSize the number of bytes read
 * @return
 */
ErrorCode Pread(FileDescriptor *fd, void *buf, uint64_t count, int64_t offset, int64_t *readSize);

/**
 * Reads file asynchronously, read synchronously if aioContext is NULL
 * @param[in] fd FileDescriptor instance
 * @param[in,out] buf buffer pointer
 * @param[in] count read bytes
 * @param[in] offset read start position from start of the file
 * @param[in] aioContext async callback and user context, could be NULL if do not care about current read result
 * @return
 */
ErrorCode PreadAsync(FileDescriptor *fd, void *buf, uint64_t count, int64_t offset, const AsyncIoContext *aioContext);

/**
 * Snapshot reads up to {count} bytes from {fileId} at {offset} (from the start of the file)
 * into the buffer starting at {buf}
 * @param[in] fileHandle FileHandle instance
 * @param[in,out] buf buffer pointer
 * @param[in] count read bytes
 * @param[in] offset read start position from start of the file
 * @param[out] diffContents the contents of read
 * @return 0 means success, other means failure
 */
ErrorCode SnapshotPread(FileDescriptor *fd, void *buf, uint64_t count, int64_t offset, DiffContents *diffContents);

/**
 * Writes up to {count} bytes from the buffer starting at {buf} to current position of {fd}
 * @param[in] fd FileDescriptor instance
 * @param[in] buf buffer pointer
 * @param[in] count write bytes
 * @param[out] writeSize the number of bytes written
 * @return
 */
ErrorCode WriteSync(FileDescriptor *fd, const void *buf, uint64_t count, int64_t *writeSize);

/**
 * Writes up to {count} bytes from the buffer starting at {buf} to the file at {offset}
 * @param[in] fd FileDescriptor instance
 * @param[in] buf buffer pointer
 * @param[in] count write bytes
 * @param[in] offset write start position from start of the file
 * @param[out] writeSize the number of bytes written
 * @return
 */
ErrorCode PwriteSync(FileDescriptor *fd, const void *buf, uint64_t count, int64_t offset, int64_t *writeSize);

/**
 * Snapshot write up to {count} bytes from the buffer starting at {buf} to the file at {offset}
 * @param[in] fd FileDescriptor instance
 * @param[in] buf buffer pointer
 * @param[in] count write bytes
 * @param[in] offset write start position from start of the file
 * @param[out] writeSize the number of bytes written
 * @return
 */
ErrorCode SnapshotPwriteSync(FileDescriptor *fd, const void *buf, uint64_t count, int64_t offset, int64_t *writeSize);

/**
 * Write data to file asynchronously
 * @param[in] fd FileDescriptor instance
 * @param[in] buf buffer pointer
 * @param[in] count write bytes
 * @param[in] aioContext async callback and user context, could be NULL if do not care about current write result
 * @return 0 means success, other means failure
 */
ErrorCode WriteAsync(FileDescriptor *fd, const void *buf, uint64_t count, const AsyncIoContext *aioContext);

/**
 * Write data to file asynchronously, write synchronously if aioContext is NULL
 * @param[in] fd FileDescriptor instance
 * @param[in] buf buffer pointer
 * @param[in] count write bytes
 * @param[in] offset write start position from start of the file
 * @param[in] aioContext async callback and user context, could be NULL if do not care about current write result
 * @return 0 means success, other means failure
 */
ErrorCode PwriteAsync(FileDescriptor *fd, const void *buf, uint64_t count, int64_t offset,
                      const AsyncIoContext *aioContext);

/**
 * Snapshot write data to file asynchronously, write synchronously if aioContext is NULL
 * @param[in] fd FileDescriptor instance
 * @param[in] buf buffer pointer
 * @param[in] count write bytes
 * @param[in] offset write start position from start of the file
 * @param[in] aioContext async callback and user context, could be NULL if do not care about current write result
 * @return 0 means success, other means failure
 */
ErrorCode SnapshotPwriteAsync(FileDescriptor *fd, const void *buf, uint64_t count, int64_t offset,
                              const AsyncIoContext *aioContext);

/**
 * Extend a file to {length} size
 * @param[in] fd FileDescriptor instance
 * @param[in] length target file size
 * @return
 */
ErrorCode Extend(FileDescriptor *fd, int64_t length);

/**
 * Truncate a file to {length} size
 * @param[in] fd FileDescriptor instance
 * @param[in] length target file size
 * @return
 */
ErrorCode Truncate(FileDescriptor *fd, int64_t length);

/**
 * Get file size
 * @param[in] fd FileDescriptor instance
 * @param[out] fileSize file size in bytes
 * @return
 */
ErrorCode GetSize(FileDescriptor *fd, int64_t *fileSize);

/**
 * File control interface, use to set fd information(flush callback, zero copy memory key, etc.)
 * @param[in] fd FileDescriptor instance
 * @param[in] cmd File control operation cmd id
 * @param[in] fileControlInfo File control info
 * @return
 */
ErrorCode FileControl(FileDescriptor *fd, int cmd, const FileControlInfo *fileControlInfo);

/**
 * Lock file in section [startPos, startPos + len)
 * @param[in] fd FileDescriptor instance
 * @param[in] startPos start offset of lock section
 * @param[in] len lock section length
 * @param[in] lockMode file lock mode, current only support FILE_EXCLUSIVE_LOCK
 * @param[in] timeout allow <= {timeout} ms to acquire file lock, if timeout=0, will wait until acquire lock success
 * @return
 */
ErrorCode LockFile(FileDescriptor *fd, int64_t startPos, int64_t len, int lockMode, uint32_t timeout);

/**
 * Try lock file in section [startPos, startPos + len)
 * @param[in] fd FileDescriptor instance
 * @param[in] startPos start offset of lock section
 * @param[in] len lock section length
 * @param[in] lockMode file lock mode, current only support FILE_EXCLUSIVE_LOCK
 * @return
 */
ErrorCode TryLockFile(FileDescriptor *fd, int64_t startPos, int64_t len, int lockMode);

/**
 * Unlock file in section [startPos, startPos + len)
 * @param[in] fd FileDescriptor instance
 * @param[in] startPos start offset of lock section
 * @param[in] len lock section length
 * @return
 */
ErrorCode UnlockFile(FileDescriptor *fd, int64_t startPos, int64_t len);

/**
 * Sync file data block cache to storage
 * @param[in] fd FileDescriptor instance
 * @return
 */
ErrorCode FDataSync(FileDescriptor *fd);

/**
 * Initial file lock
 * @param[in] vfs VirtualFileSystem instance
 * @param[in] fileName local store file name or PageStore file name
 * @param[in] lockFd FileDescriptor instance
 * @return error code
 */
ErrorCode InitFileLock(VirtualFileSystem *vfs, const char *fileName, const char *storeSpaceName,
                       FileDescriptor **lockFd);

/**
 * Destory file lock
 * @param[in] vfs VirtualFileSystem instance
 * @param[in] fileName local store file name or PageStore file name
 * @param[in] storeSpaceId 16-bit store space id
 * @param[in] lockFd FileDescriptor instance
 * @return error code
 */
ErrorCode DestoryFileLock(VirtualFileSystem *vfs, const char *fileName, FileDescriptor *lockFd);

/**
 * Lock file
 * @param[in] lockFd FileDescriptor instance
 * @return error code
 */
ErrorCode FileLock(FileDescriptor *lockFd);

/**
 * Unlock file
 * @param[in] lockFd FileDescriptor instance
 * @return error code
 */
ErrorCode FileUnLock(FileDescriptor *lockFd);

/**
 * Copy config file from source file to target file
 * @param[in] sourceVfs VirtualFileSystem instance
 * @param[in] sourceFileName local file name or PageStore file name
 * @param[in] targetVfs VirtualFileSystem instance
 * @param[in] targetFileName local file name or PageStore file name
 * @param[in] storeSpaceId 16-bit store space id
 * @return error code
 */
ErrorCode CopyFile(VirtualFileSystem *sourceVfs, const char *sourceFileName, VirtualFileSystem *targetVfs,
                   const char *targetFileName, const char *storeSpaceName);

/**
 * Enable the aio function for async read and async write
 * @param[in] localVfs VirtualFileSystem instance
 * @param[in] maxEvents max events number
 * @param[in] threadCount threads number
 * @param[in] threadEnterCallback callback function
 * @param[in] threadExitCallback callback function
 * @return error code
 */
ErrorCode EnableAIO(VirtualFileSystem *vfs, uint16_t maxEvents, uint16_t threadCount, void (*threadEnterCallback)(void),
                    void (*threadExitCallback)(void));

GSDB_END_C_CODE_DECLS

#endif /* UTILS_VFS_INTERFACE_H */
