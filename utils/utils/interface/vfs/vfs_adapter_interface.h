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
 * vfs_adapter_interface.h
 *
 * Description:
 * This file defines adapter interface for storage system
 *
 * ---------------------------------------------------------------------------------
 */
#ifndef UTILS_VFS_ADAPTER_INTERFACE_H
#define UTILS_VFS_ADAPTER_INTERFACE_H

#include "vfs/vfs_interface.h"

GSDB_BEGIN_C_CODE_DECLS

/* All vfs dynamic lib must implement following function using C-api symbol */
#define GET_ADAPTER_INTERFACE_FUNC     "GetVfsAdapterInterface"
#define SET_ADAPTER_MEM_ALLOCATOR_FUNC "SetVfsAdapterMemAllocator"
#define DELETE_ADAPTER_INTERFACE_FUNC  "DeleteVfsAdapterInterface"
#define GET_ADAPTER_INFO_FUNC          "GetVfsAdapterInfo"

typedef struct VfsLibParameter VfsAdapterParam;
typedef struct FileParameter FileParam;

typedef void *VfsClientHandlePtr;
typedef void *VfsHandlePtr;
typedef void *FileHandlePtr;

typedef struct AdapterInfo AdapterInfo;
struct AdapterInfo {
    uint64_t magic;
    uint32_t mode;
    uint32_t version;
};

typedef void (*AdapterFlushCallback)(void *fd, int64_t offset, ErrorCode errorCode, void *asyncContext);

typedef union AdapterFileControlInfo AdapterFileControlInfo;
union AdapterFileControlInfo {
    struct {
        AdapterFlushCallback callback;
        void *asyncContext;
        void *fd;
    } flushCallbackInfo;
    struct {
        uint64_t memKey;
    } zeroCopyMemKey;
    struct {
        uint64_t offset;
    } readSeekInfo;
};

typedef struct VfsAdapterInterface VfsAdapterInterface;
struct VfsAdapterInterface {
    /* adapter instance operation interface */

    /**
     * Get error message by error code
     * @param[in] errorCode error code to get error message
     * @return error message
     */
    const char *(*getErrMsg)(ErrorCode errorCode);

    /**
     * Set vfs adapter configure
     * @param[in] para parameter name
     * @param[in] value parameter value
     * @return
     */
    ErrorCode (*setAdapterConfig)(const char *para, const char *value);

    /**
     * Get vfs adapter configure
     * @param[in] para parameter name
     * @param[out] value parameter value
     * @param[in] len length of value
     * @return
     */
    ErrorCode (*getAdapterConfig)(const char *para, char *value, uint64_t len);

    /**
     * Init VfsHandle adapter with parameters
     * @param[in] param vfs adapter parameters, including ip list...
     * @return 0 means success, other means failure
     */
    ErrorCode (*initAdapter)(const VfsAdapterParam *param);

    /**
     * Stop VfsHandle adapter
     * @return 0 means success, other means failure
     */
    ErrorCode (*stopAdapter)(void);

    /**
     * Update VfsHandle adapter parameter info
     * @param[in] vfsClientHandle: vfs ClientHandle instance
     * @param[in] param vfs adapter parameters, including ip list...
     * @return 0 means success, other means failure
     */
    ErrorCode (*updateAdapterParam)(VfsClientHandlePtr vfsClientHandle, const VfsAdapterParam *param);

    /**
     * Get vfs adapter client id
     * @param[in] vfsClientHandle: vfs ClientHandle instance
     * @param[out] adapterClientId client id value
     * @return
     */
    ErrorCode (*getAdapterClientId)(VfsClientHandlePtr vfsClientHandle, uint64_t *adapterClientId);

    /**
     * Vfs adapter remote message handler for communication lib
     * @param[in] vfsClientHandle: vfs ClientHandle instance
     * @param[in] msgRequest remote message instance
     * @param[in] handleMsgContext remote message handler context
     */
    void (*handlerAdapterRemoteMsg)(VfsClientHandlePtr vfsClientHandle, void *msgRequest,
                                    void *handleMsgContext);

    /**
     * exit VfsHandle adapter with parameters
     * @return 0 means success, other means failure
     */
    ErrorCode (*exitAdapter)(void);

    /* vfs operation interface */

    /**
     * Create tenant
     * @param[in] vfsClientHandle: vfs ClientHandle instance
     * @param[in] clusterName cluster name with max length 256
     * @param[in] tenantName tenant name with max length 256
     * @return 0 means success, other means failure
     */
    ErrorCode (*createTenant)(VfsClientHandlePtr vfsClientHandle, const char *clusterName,
                              const char *tenantName);

    /**
     * Delete tenant
     * @param[in] vfsClientHandle: vfs ClientHandle instance
     * @param[in] clusterName cluster name with max length 256
     * @param[in] tenantName tenant name with max length 256
     * @param[in] attr attribute flags
     * @return 0 means success, other means failure
     */
    ErrorCode (*deleteTenant)(VfsClientHandlePtr vfsClientHandle, const char *clusterName,
                              const char *tenantName, uint64_t attr);

    /**
     * Create VFS in storage, does not mount when create vfs successful
     * @param[in] vfsClientHandle: vfs ClientHandle instance
     * @param[in] tenantName tenant name with max length 256
     * @param[in] vfsName VFS name with max length 256
     * @param[in] attr vfs attribute flags
     * @return 0 means success, other means failure
     */
    ErrorCode (*createVfs)(VfsClientHandlePtr vfsClientHandle, const char *tenantName, const char *vfsName,
                           uint64_t attr);

    /**
     * Drop VFS in storage system
     * @param[in] vfsClientHandle: vfs ClientHandle instance
     * @param[in] tenantName tenant name
     * @param[in] vfsName VFS name
     * @param[in] attr vfs drop attribute flags
     * @return 0 means success, other means failure
     */
    ErrorCode (*dropVfs)(VfsClientHandlePtr vfsClientHandle, const char *tenantName, const char *vfsName,
                         uint64_t attr);

    /**
     * Mount VFS in VfsMgr, file operations can be used after calling MountVfs successfully
     * @param[in] vfsClientHandle: vfs ClientHandle instance
     * @param[in] tenantName tenant name
     * @param[in] vfsName VFS name
     * @param[out] vfs VfsHandle *instance, used to access files
     * @return 0 means success, other means failure
     */
    ErrorCode (*mountVfs)(VfsClientHandlePtr vfsClientHandle, const char *tenantName, const char *vfsName,
                          VfsHandlePtr *vfs);

    /**
     * Unmount VFS in VfsMgr
     * @param[in] vfs VfsHandle instance
     * @return 0 means success, other means failure
     */
    ErrorCode (*unmountVfs)(VfsHandlePtr vfs);

    /**
     * VFS control interface
     * @param[in] vfsClientHandle: vfs ClientHandle instance
     * @param[in] tenantName tenant name
     * @param[in] cmd vfs control operation cmd id
     * @param[in,out] vfsControlInfo vfs control information
     * @param[in] count array count of vfsControlInfo
     * @return 0 means success, other means failure
     */
    ErrorCode (*vfsControl)(VfsClientHandlePtr vfsClientHandle, const char *tenantName, int cmd,
                            VfsControlInfo *vfsControlInfo, uint32_t count);

    /* snapshot operater interface */

    /**
     * Creat a snapshot in VFS
     * @param[in] vfsClientHandle: vfs ClientHandle instance
     * @param[in] tenantName tenant name
     * @param[in] vfsName VFS name
     * @param[in] snapshotName snapshot name
     * @param[in] flags snapshot flags
     * @return 0 means success, other means failure
     */
    ErrorCode (*createSnapshot)(VfsClientHandlePtr vfsClientHandle, const char *tenantName,
                                const char *vfsName, const char *snapshotName, int64 flags);

    /**
     * Drop a snapshot in VFS
     * @param[in] vfsClientHandle: vfs ClientHandle instance
     * @param[in] tenantName tenant name
     * @param[in] vfsName VFS name
     * @param[in] snapshotName snapshot name
     * @return 0 means success, other means failure
     */
    ErrorCode (*dropSnapshot)(VfsClientHandlePtr vfsClientHandle, const char *tenantName, const char *vfsName,
                              const char *snapshotName);

    /**
     * Rollback a snapshot in VFS
     * @param[in] vfsClientHandle: vfs ClientHandle instance
     * @param[in] tenantName tenant name
     * @param[in] vfsName VFS name
     * @param[in] snapshotName snapshot name
     * @return 0 means success, other means failure
     */
    ErrorCode (*rollbackSnapshot)(VfsClientHandlePtr vfsClientHandle, const char *tenantName,
                                  const char *vfsName, const char *snapshotName);

    /* store space operation interface */

    /**
     * Create a store space in VFS
     * @param[in] vfsClientHandle: vfs ClientHandle instance
     * @param[in] tenantName tenant name
     * @param[in] storeSpaceNames storeSpace name list
     * @param[in] attrs store space attribute
     * @param[in] count the number of store space
     * @return 0 means success, other means failure
     */
    ErrorCode (*createStoreSpace)(VfsClientHandlePtr vfsClientHandle, const char *tenantName,
                                  const char **storeSpaceNames, const StoreSpaceAttr *attrs, uint32_t count);

    /**
     * Delete a store space in VFS
     * @param[in] vfsClientHandle: vfs ClientHandle instance
     * @param[in] tenantName tenant name
     * @param[in] storeSpaceNames storeSpace name list
     * @param[in] count the number of store space
     * @return 0 means success, other means failure
     */
    ErrorCode (*deleteStoreSpace)(VfsClientHandlePtr vfsClientHandle, const char *tenantName,
                                  const char **storeSpaceNames, uint32_t count);

    /**
     * Update the store space in VFS
     * @param[in] vfsClientHandle: vfs ClientHandle instance
     * @param[in] tenantName tenant name
     * @param[in] storeSpaceNames storeSpace name list
     * @param[in] attrs store space attribute
     * @param[in] count the number of store space
     * @return 0 means success, other means failure
     */
    ErrorCode (*updateStoreSpace)(VfsClientHandlePtr vfsClientHandle, const char *tenantName,
                                  const char **storeSpaceNames, const StoreSpaceAttr *attrs, uint32_t count);

    /**
     * Query the attributes of the store space in VFS
     * @param[in] vfsClientHandle: vfs ClientHandle instance
     * @param[in] tenantName tenant name
     * @param[in] storeSpaceNames storeSpace name list
     * @param[in] attrs store space attribute
     * @param[in] count the number of store space
     * @return 0 means success, other means failure
     */
    ErrorCode (*queryStoreSpaceAttr)(VfsClientHandlePtr vfsClientHandle, const char *tenantName,
                                     const char **storeSpaceNames, StoreSpaceAttr *attrs, uint32_t count);

    /**
     * Query the used size of the store space in VFS
     * @param[in] vfsClientHandle: vfs ClientHandle instance
     * @param[in] tenantName tenant name
     * @param[in] storeSpaceNames storeSpace name list
     * @param[in] usedSizes store space used sizes
     * @param[in] count the number of store space
     * @return 0 means success, other means failure
     */
    ErrorCode (*queryStoreSpaceUsedSize)(VfsClientHandlePtr vfsClientHandle, const char *tenantName,
                                         const char **storeSpaceNames, uint64 *usedSizes, uint32_t count);

    /* file operation interface */

    /**
     * Create a file in VFS
     * @param[in] vfs VfsHandle instance
     * @param[in] fileId file_id, 16-bit
     * @param[in] pathName file path, used in local file system
     * @param[in] param file type, store space id, etc
     * @param[out] fileHandle FileHandle instance
     * @return 0 means success, other means failure
     */
    ErrorCode (*createFile)(VfsHandlePtr vfs, const char *pathName, const FileParam *param, FileHandlePtr *fileHandle);

    /**
     * Remove a file in VFS
     * @param[in] vfs VfsHandle instance
     * @param[in] fileId file_id, 16-bit
     * @param[in] pathName file path, used in local file system
     * @return 0 means success, other means failure
     */
    ErrorCode (*removeFile)(VfsHandlePtr vfs, const char *pathName);

    /**
     * Check whether the file exists
     * @param[in] vfs VfsHandle instance
     * @param[in] fileId file_id, 16-bit
     * @param[in] pathName file path, used in local file system
     * @param[out] out file existence result
     * @return 0 means success, other means failure
     */
    ErrorCode (*fileIsExist)(VfsHandlePtr vfs, const char *pathName, bool *out);

    /**
     * Open a file in VFS
     * @param[in] vfs VfsHandle instance
     * @param[in] fileId file_id, 16-bit
     * @param[in] pathName file path, used in local file system
     * @param[in] flags file open flags, refer to POSIX definitions
     * @param[out] fileHandle FileHandle instance
     * @return 0 means success, other means failure
     */
    ErrorCode (*openFile)(VfsHandlePtr vfs, const char *pathName, int flags, FileHandlePtr *fileHandle);

    /**
     * Open a file in VFS with snapshotName
     * @param[in] vfs Vfs instance
     * @param[in] openPara fileId, flags, filePath and snapshotName
     *                     fileId file_id, 16-bit
     *                     flags file open flags, refer to POSIX definitions
     *                     filePath file path, used in local file system
     *                     snapshotName snapshot name
     * @param[out] fileHandle FileHandle instance
     * @return 0 means success, other means failure
     */
    ErrorCode (*openFileSnapshot)(VfsHandlePtr vfs, FileOpenParam *openPara, FileHandlePtr *fileHandle);

    /**
     * Rename a file in VFS
     * @param[in] vfs VfsHandle instance
     * @param[in] srcFileId source file_id, 16-bit
     * @param[in] destFileId destination file_id, 16-bit
     * @param[in] srcPathName source file path, used in local file system
     * @param[in] destPathName destination file path, used in local file system
     * @return 0 means success, other means failure
     */
    ErrorCode (*renameFile)(VfsHandlePtr vfs, const char *srcPathName, const char *destPathName);

    /**
     * Close a file in VFS
     * @param[in] fileHandle FileHandle instance
     * @return 0 means success, other means failure
     */
    ErrorCode (*close)(FileHandlePtr fileHandle);

    /**
     * Sync write data to storage
     * @param[in] fileHandle FileHandle instance
     * @return 0 means success, other means failure
     */
    ErrorCode (*fsync)(FileHandlePtr fileHandle);

    /**
     * Repositions the offset of the open file base on file seek flag
     * FILE_SEEK_SET: The offset is set to offset bytes
     * FILE_SEEK_CUR: The offset is set to its current location plus offset bytes
     * FILE_SEEK_END: The offset is set to the size of the file plus offset bytes
     * @param[in] fileHandle FileHandle instance
     * @param[in] offset Gap size between file seek flag and target offset
     * @param[in] seekFlag file seek flag
     * @param[out] newPos result position of current fd
     * @return
     */
    ErrorCode (*fileSeek)(FileHandlePtr fileHandle, int64_t offset, int seekFlag, int64_t *newPos);

    /**
     * Set the file position to 0 of current fd
     * @param[in] fileHandle FileHandle instance
     * @return
     */
    ErrorCode (*rewind)(FileHandlePtr fileHandle);

    /**
     * Reads up to {count} bytes at current position of {fd} into the buffer starting at {buf}
     * @param[in] fileHandle FileHandle instance
     * @param[in,out] buf buffer pointer
     * @param[in] count read bytes
     * @param[out] readSize the number of bytes read
     * @return
     */
    ErrorCode (*read)(FileHandlePtr fileHandle, void *buf, uint64_t count, int64_t *readSize);

    /**
     * Reads up to {count} bytes from {fileId} at {offset} (from the start of the file)
     * into the buffer starting at {buf}
     * @param[in] fileHandle FileHandle instance
     * @param[in,out] buf buffer pointer
     * @param[in] count read bytes
     * @param[in] offset read start position from start of the file
     * @param[out] readSize the number of bytes read
     * @return 0 means success, other means failure
     */
    ErrorCode (*pread)(FileHandlePtr fileHandle, void *buf, uint64_t count, int64_t offset, int64_t *readSize);

    /**
     * Snapshot reads up to {count} bytes from {fileId} at {offset} (from the start of the file)
     * into the buffer starting at {buf}
     * @param[in] fileHandle FileHandle instance
     * @param[in,out] buf buffer pointer
     * @param[in] count read bytes
     * @param[in] offset read start position from start of the file
     * @param[out] readSize the number of bytes read
     * @return 0 means success, other means failure
     */
    ErrorCode (*snapshotPread)(FileHandlePtr fileHandle, void *buf, uint64_t count, int64_t offset,
                               DiffContents *diffContents);

    /**
     * Reads file asynchronously
     * into the buffer starting at {buf}
     * @param[in] fileHandle FileHandle instance
     * @param[in,out] buf buffer pointer
     * @param[in] count read bytes
     * @param[in] offset read start position from start of the file
     * @param[in] aioContext async callback and user context, could be NULL if do not care about current read result
     * @return 0 means success, other means failure
     */
    ErrorCode (*preadAsync)(FileHandlePtr fileHandle, void *buf, uint64_t count, int64_t offset,
                            const AsyncIoContext *aioContext);

    /**
     * Writes up to {count} bytes from the buffer starting at {buf} to current position of {fd}
     * @param[in] fileHandle FileHandle instance
     * @param[in] buf buffer pointer
     * @param[in] count write bytes
     * @param[out] writeSize the number of bytes written
     * @return
     */
    ErrorCode (*writeSync)(FileHandlePtr fileHandle, const void *buf, uint64_t count, int64_t *writeSize);

    /**
     * Writes up to {count} bytes from the buffer starting at {buf} to the {fileId} at {offset}
     * @param[in] fileHandle FileHandle instance
     * @param[in] buf buffer pointer
     * @param[in] count write bytes
     * @param[in] offset write start position from start of the file
     * @param[out] writeSize the number of bytes written
     * @return 0 means success, other means failure
     */
    ErrorCode (*pwriteSync)(FileHandlePtr fileHandle, const void *buf, uint64_t count, int64_t offset,
                            int64_t *writeSize);

    /**
     * Snapshot write up to {count} bytes from the buffer starting at {buf} to the {fileId} at {offset}
     * @param[in] fileHandle FileHandle instance
     * @param[in] buf buffer pointer
     * @param[in] count write bytes
     * @param[in] offset write start position from start of the file
     * @param[out] writeSize the number of bytes written
     * @return 0 means success, other means failure
     */
    ErrorCode (*snapshotPwriteSync)(FileHandlePtr fileHandle, const void *buf, uint64_t count, int64_t offset,
                                    int64_t *writeSize);

    /**
     * Write data to file asynchronously
     * @param[in] fileHandle FileHandle instance
     * @param[in] buf buffer pointer
     * @param[in] count write bytes
     * @param[in] aioContext async callback and user context, could be NULL if do not care about current write result
     * @return 0 means success, other means failure
     */
    ErrorCode (*writeAsync)(FileHandlePtr fileHandle, const void *buf, uint64_t count,
                            const AsyncIoContext *aioContext);

    /**
     * Write data to file asynchronously
     * @param[in] fileHandle FileHandle instance
     * @param[in] buf buffer pointer
     * @param[in] count write bytes
     * @param[in] offset write start position from start of the file
     * @param[in] aioContext async callback and user context, could be NULL if do not care about current write result
     * @return 0 means success, other means failure
     */
    ErrorCode (*pwriteAsync)(FileHandlePtr fileHandle, const void *buf, uint64_t count, int64_t offset,
                             const AsyncIoContext *aioContext);

    /**
     * Snapshot write data to file asynchronously
     * @param[in] fileHandle FileHandle instance
     * @param[in] buf buffer pointer
     * @param[in] count write bytes
     * @param[in] offset write start position from start of the file
     * @param[in] aioContext async callback and user context, could be NULL if do not care about current write result
     * @return 0 means success, other means failure
     */
    ErrorCode (*snapshotPwriteAsync)(FileHandlePtr fileHandle, const void *buf, uint64_t count, int64_t offset,
                                     const AsyncIoContext *aioContext);

    /**
     * Extend a file to {length} size
     * @param[in] fileHandle FileHandle instance
     * @param[in] length target file size
     * @return 0 means success, other means failure
     */
    ErrorCode (*extend)(FileHandlePtr fileHandle, int64_t length);

    /**
     * Truncate a file to {length} size
     * @param[in] fileHandle FileHandle instance
     * @param[in] length target file size
     * @return 0 means success, other means failure
     */
    ErrorCode (*truncate)(FileHandlePtr fileHandle, int64_t length);

    /**
     * Get file size
     * @param[in] fileHandle FileHandle instance
     * @param[out] fileSize file size in bytes
     * @return 0 means success, other means failure
     */
    ErrorCode (*getSize)(FileHandlePtr fileHandle, int64_t *fileSize);

    /**
     * File control interface, use to set fd information(flush callback, zero copy memory key, etc.)
     * @param[in] fileHandle FileHandle instance
     * @param[in] cmd File control operation cmd id
     * @param[in] controlInfo File control info
     */
    ErrorCode (*fileControl)(FileHandlePtr fileHandle, int cmd, const AdapterFileControlInfo *controlInfo);

    /**
     * Lock file in section [startPos, startPos + len)
     * @param[in] fileHandle FileHandle instance
     * @param[in] startPos start offset of lock section
     * @param[in] len lock section length
     * @param[in] lockMode file lock mode, current only support FILE_EXCLUSIVE_LOCK
     * @param[in] timeout allow <= {timeout} ms to acquire file lock, if timeout=0, will wait until acquire lock success
     * @return
     */
    ErrorCode (*lockFile)(FileHandlePtr fileHandle, int64_t startPos, int64_t len, int lockMode, uint32_t timeout);

    /**
     * Try lock file in section [startPos, startPos + len)
     * @param[in] fileHandle FileHandle instance
     * @param[in] startPos start offset of lock section
     * @param[in] len lock section length
     * @param[in] lockMode file lock mode, current only support FILE_EXCLUSIVE_LOCK
     * @return
     */
    ErrorCode (*tryLockFile)(FileHandlePtr fileHandle, int64_t startPos, int64_t len, int lockMode);

    /**
     * Unlock file in section [startPos, startPos + len)
     * @param[in] fileHandle FileHandle instance
     * @param[in] startPos start offset of lock section
     * @param[in] len lock section length
     * @return
     */
    ErrorCode (*unlockFile)(FileHandlePtr fileHandle, int64_t startPos, int64_t len);

    /**
     * Sync file data block cache to storage
     * @param[in] fileHandle FileHandle instance
     * @return 0 means success, other means failure
     */
    ErrorCode (*fDataSync)(FileHandlePtr fileHandle);

    /**
     * Enable the aio function for async read and async write
     * @param[in] vfs VfsHandle instance
     * @param[in] maxEvents max events number
     * @param[in] threadCount threads number
     * @param[in] threadExitCallback callback function
     * @return 0 means success, other means failure
     */
    ErrorCode (*enableAIO)(VfsHandlePtr vfs, uint16_t maxEvents, uint16_t threadCount,
                           void (*threadEnterCallback)(void), void (*threadExitCallback)(void));

    /**
     * create vfs client instance handle
     * @param[in] clusterName vfs cluster name
     * @param[in] storageServerAddr: ip list of the storage server node
     * @param[out] vfsClientHandle: vfs ClientHandle instance
     * @return 0 means success, other means failure
     */
    ErrorCode (*createClientHandle)(const char *clusterName, uint32_t authType, const char *storageServerAddr,
        VfsClientHandlePtr *vfsClientHandle);

    /**
     * delete vfs client instance handle
     * @param[in] vfsClientHandle: vfs ClientHandle instance
     * @return 0 means success, other means failure
     */
    ErrorCode (*deleteClientHandle)(VfsClientHandlePtr vfsClientHandle);

    /**
     * Set vfs adapter configure
     * @param[in] vfsClientHandle: vfs ClientHandle instance
     * @param[in] para parameter name
     * @param[in] value parameter value
     * @return
     */
    ErrorCode (*setClientConfig)(VfsClientHandlePtr vfsClientHandle, const char *para, const char *value);

    /**
     * Get vfs adapter configure
     * @param[in] vfsClientHandle: vfs ClientHandle instance
     * @param[in] para parameter name
     * @param[out] value parameter value
     * @param[in] len length of value
     * @return
     */
    ErrorCode (*getClientConfig)(VfsClientHandlePtr vfsClientHandle, const char *para, char *value, uint64_t len);
};

VfsAdapterInterface *GetVfsAdapterInterface(void);
ErrorCode SetVfsAdapterMemAllocator(const MemAllocator *memAllocator);
void DeleteVfsAdapterInterface(VfsAdapterInterface *ops);
AdapterInfo GetVfsAdapterInfo(void);

typedef VfsAdapterInterface *(*GetVfsAdapterInterfaceFunc)(void);
typedef ErrorCode (*SetVfsAdapterMemAllocatorFunc)(const MemAllocator *memAllocator);
typedef void (*DeleteVfsAdapterInterfaceFunc)(VfsAdapterInterface *ops);
typedef AdapterInfo (*GetVfsAdapterInfoFunc)(void);

typedef const char *(*GetVfsErrMsgFunc)(ErrorCode errorCode);
void SetRemoteVfsErrMsgFunc(GetVfsErrMsgFunc func);

GSDB_END_C_CODE_DECLS

#endif /* UTILS_VFS_ADAPTER_INTERFACE_H */
