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
 * Description: this file provides the facilities to adapt to the external VFS library.
 *
 * ---------------------------------------------------------------------------------------
 *
 */

#ifndef DSTORE_DSTORE_VFS_ADAPTER_H
#define DSTORE_DSTORE_VFS_ADAPTER_H
#include <atomic>
#include "defines/cluster_defs.h"
#include "common/memory/dstore_mctx.h"
#include "common/dstore_datatype.h"
#include "framework/dstore_instance_interface.h"
#include "framework/dstore_vfs_interface.h"
#include "page/dstore_page_struct.h"
#include "framework/dstore_vfs_interface.h"
#include "port/platform_port.h"

struct FileDescriptor;
struct FileParameter;
struct VirtualFileSystem;
struct AsyncIoContext;
namespace DSTORE {

constexpr int DSTORE_FILE_OPEN_FLAG = FILE_READ_AND_WRITE_FLAG | FILE_DIRECT_IO_FLAG | FILE_SYNC_FLAG;
constexpr int DSTORE_FILE_ADIO_FLAG = FILE_READ_AND_WRITE_FLAG | FILE_ASYNC_IO_FLAG;
constexpr uint16 REGULAR_FILE_STORE_SPACE_ID = 10;
constexpr uint16 WAL_STORE_SPACE_ID = 20;

constexpr uint32 CONTROL_FILE_RANGE_SIZE{1 << 20}; /* 1M */
constexpr uint32 DECODE_DICT_FILE_RANGE_SIZE{64 << 10}; /* 64 KB */
constexpr uint32 WAL_STREAM_INFO_FILE_RANGE_SIZE{24 << 10}; /* 24 KB */
constexpr uint32 WAL_STREAM_RANGE_FILE_RANGE_SIZE{8 << 10}; /* 8 KB */
#ifdef UT
constexpr uint32 TBS_FILE_RANGE_SIZE{8 << 20};    /* 8 MB */
#else
constexpr uint32 TBS_FILE_RANGE_SIZE{128 << 20};    /* 128 MB */
#endif
constexpr uint32 TEMPLATE_TBS_FILE_RANGE_SIZE{8 << 20}; /* 8MB */
constexpr uint32 WAL_FILE_RANGE_SIZE{64 << 10};    /* 64 KB */

constexpr int NUM_FD_HASH_INIT_ENTRY = 4096;
constexpr int NUM_FD_HASH_PARTITIONS = 256;

constexpr int NUM_FD_ARRAY_MUTEX = 256;

constexpr int AIO_MAX_REQNUM = 512;
constexpr int NODE_TIMELINE_ID_DEFAULT = 0;
constexpr uint64 NUM_INIT_DB_FENCING_USE_TERM = 1000;
constexpr uint64 DR_FENCING_CLEAR_TERM = 0;

typedef struct VFSAdapterFdHashEntry {
    char pathName[MAXPGPATH];
    ::FileDescriptor *fileDesc;
    uint16 refCount;
} VFSAdapterFdHashEntry;

inline int64 GetOffsetByBlockNo(BlockNumber blockNo)
{
    StorageReleasePanic(blockNo > DSTORE_MAX_BLOCK_NUMBER, MODULE_FRAMEWORK, ErrMsg("The block number is abnormal."));
    uint64 uOffset = static_cast<uint64>(blockNo) * BLCKSZ;
    StorageReleasePanic(uOffset > INT64_MAX, MODULE_FRAMEWORK,
                        ErrMsg("The offset which VFS get by block number is abnormal."));
    return static_cast<int64>(uOffset);
}

inline BlockNumber GetBlockNoByOffset(int64 offset)
{
    StorageReleasePanic(offset <= 0, MODULE_FRAMEWORK, ErrMsg("The offset is abnormal."));
    uint64 blockNo = static_cast<uint64>(offset) / BLCKSZ;
    StorageReleasePanic(blockNo > DSTORE_MAX_BLOCK_NUMBER, MODULE_FRAMEWORK,
                        ErrMsg("The block number which VFS get by offset is abnormal."));
    return static_cast<BlockNumber>(blockNo);
}

/* As the name shows, the passed-in fileSize must be a multiple of BLCKSZ. */
inline BlockNumber GetBlockNoByPageAlignedFileSize(int64 fileSize)
{
    StorageReleasePanic(fileSize <= 0 || fileSize % BLCKSZ != 0, MODULE_FRAMEWORK, ErrMsg("The filesize is abnormal."));
    return GetBlockNoByOffset(fileSize);
}

class VFSAdapter : public BaseObject {
public:
    explicit VFSAdapter(PdbId pdbId);
    virtual ~VFSAdapter();

    DISALLOW_COPY_AND_MOVE(VFSAdapter);

    RetStatus Bootstrap();
    /* in local tenant isolation mode, bootstrap need a datadir as home directory;
     * this param will move to interface Bootstrap later. */
    RetStatus CreateVfs(const char *datadir, const char *vfsName, const uint64 ioFenceFlag = VFS_ENABLE_IO_FENCE_FLAG);
    RetStatus Initialize(const char *vfsName, uint32 clientId = 0);
    RetStatus Open(const char *vfsName);

    bool HasMounted() const;

    static RetStatus ModuleInitialize();
    static RetStatus LibInitialize(void *tenantConfig, StorageInstanceType type, uint32 clientId);
    static RetStatus DestroyDataForce(const char *vfsName, PdbId pdbId);
    RetStatus Destroy(const char *vfsName = nullptr, bool dropData = false);
    static struct VfsLibHandle *GetVfsLib();

    /**
     * Create a file in the VFS, and the file is open by default.
     *
     * @param fileId the file id in this VFS
     * @param pathName the path to the file
     * @param filePara the parameter used to create the file
     * @return DSTORE_SUCC means success, DSTORE_FAIL means failure
     */
    RetStatus CreateFile(FileId fileId, const char *pathName, ::FileParameter const filePara);

    /**
     * Remove a file in the VFS. If the file is not closed, the function will close it before it removes the file.
     *
     * @param fileId the file id in this VFS
     * @param pathName the path to the file
     * @return DSTORE_SUCC means success, DSTORE_FAIL means failure
     */
    RetStatus RemoveFile(FileId fileId, const char *pathName);

    /**
     * Check if the designated file exists in the VFS. For any VFS internal errors occurred inside, the method will
     * complain.
     *
     * @param fileId the file id in this VFS
     * @param pathName the path to the file
     * @return true means the file exists, or false otherwise
     */
    bool FileExists(FileId fileId, const char *pathName);

    /**
     * Check if the designated file exists in the VFS. For any VFS internal errors occurred inside, the method will
     * complain.
     *
     * @param pathName the path to the file
     * @return true means the file exists, or false otherwise
     */
    bool FileExists(const char *pathName);

    /**
     * Open a file in the VFS. Note that we cannot open a non-existing file.
     *
     * @param fileId the file id in this VFS
     * @param pathName the path to the file
     * @param flags specifies the file access mode, r, w, or r/w. The flags are defined in VFS's header file:
     *              FILE_READ_AND_WRITE_FLAG, FILE_READ_ONLY_FLAG, FILE_WRITE_ONLY_FLAG
     */
    RetStatus OpenFile(FileId fileId, const char *pathName, int flags);

    /**
     * Close the designated file in the VFS.
     *
     * @param fileId the file id in this VFS
     */
    RetStatus Close(FileId fileId);

    /**
     * Close the all files opened in the VFS.
     */
    RetStatus CloseAllFiles();

    /**
     * Read one block from the file.
     *
     * @param pageId the page ID in the VFS
     * @param[out] outBuffer the buffer where the file content will be output to
     * @return DSTORE_SUCC means success, DSTORE_FAIL means failure
     */
    RetStatus ReadPageSync(const PageId pageId, void *outBuffer);

    /**
     * Read one block from the file.
     *
     * @param fileDesc the file descriptor to read in the VFS
     * @param blockNumber the block number to read
     * @param[out] outBuffer the buffer where the file content will be output to
     * @return DSTORE_SUCC means success, DSTORE_FAIL means failure
     */
    RetStatus ReadPageSync(FileDescriptor *fileDesc, BlockNumber blockNumber, void *outBuffer);

    /**
     * Read bytes from the file.
     *
     * @param[in] fileId the file ID in the VFS
     * @param[out] buf the buffer where the file content will be output to
     * @param[in] count read bytes
     * @param[in] offset read start position from start of the file
     * @param[out] readSize the number of bytes read
     * @return DSTORE_SUCC means success, DSTORE_FAIL means failure
     */
    RetStatus Pread(FileId fileId, void *buf, uint64 count, int64 offset, int64 *readSize);

    /**
     * Asynchronously write one page to the VFS. You will need Fsync() to dump all the written data to the storage
     * media.
     *
     * @param pageId the page ID in the VFS
     * @param inBuffer the buffer whose content will be dumped to the storage
     * @return DSTORE_SUCC means success, DSTORE_FAIL means failure
     */
    RetStatus WritePageSync(const PageId pageId, const void *inBuffer);
    RetStatus WritePageSync(const FileDescriptor *fileDesc,  BlockNumber blockNumber, const void *inBuffer);

    RetStatus WritePageAsync(const PageId pageId, const void *inBuffer, const ::AsyncIoContext *aioContext);

    /**
     * Write data to file asynchronously
     * @param[in] fileId the file ID in the VFS
     * @param[in] buf buffer pointer
     * @param[in] count write bytes
     * @param[in] offset write start position from start of the file
     * @param[in] aioContext async callback and user context, could be NULL if do not care about current write result
     * @return 0 means success, other means failure
     */
    virtual RetStatus PwriteAsync(FileId fileId, const void *buf, uint64 count, int64 offset,
                                 const AsyncIoContext *aioContext);

    RetStatus PwriteSync(FileId fileId, const void *inBuffer, uint64 count, int64 offset);

    /**
     * Sync written data to storage media
     * @param fileId the file Id in this VFS
     * @return DSTORE_SUCC means success, DSTORE_FAIL means failure
     */
    virtual RetStatus Fsync(FileId fileId);

    /**
     * Get the current file size.
     *
     * @param fileId the file Id in this VFS
     * @return -1 if if failed, or a non-negative number which means the file size.
     */
    int64 GetSize(FileId fileId);

    /**
     * Extend a file to {length} size.
     * @param fileId the file Id in this VFS
     * @param length target file size in byte, which can not be less than the current file size
     * @return DSTORE_SUCC means success, DSTORE_FAIL means failure
     */
    RetStatus Extend(FileId fileId, int64 length);

    /**
     * Truncate a file to {length} size
     * @param fileId the file Id in this VFS
     * @param length target file size in byte, which can not be greater than the current file size
     * @return DSTORE_SUCC means success, DSTORE_FAIL means failure
     */
    RetStatus Truncate(FileId fileId, int64 length);
    RetStatus Rewind(FileId fileId);

    RetStatus RenameFile(FileId srcFileId, FileId destFileId, const char *srcPathName, const char *destPathName);

    /**
     * Get FileDescriptor of a file
     * @param fileId the file Id in this VFS
     * @return a pointer of FileDescriptor, nullptr means file is not open.
     */
    ::FileDescriptor *GetFileDescriptor(FileId fileId);

    /**
    * Create snapshot in the vfs
    * @param vfsName vfs name
    * @param snapshotName snapshot name
    * @param flags "SNAPSHOT_ALL_FILE_FLAG" means all files,
    *              "SNAPSHOT_IN_PLACE_UPDATE_FILE_FLAG" means data files,
    *              "SNAPSHOT_APPEND_FILE_FLAG" means log files
    * @return DSTORE_SUCC means success, otherwise means failure
    */
    ErrorCode CreateSnapshotInVfs(const char *vfsName, const char *snapshotName, int64_t flags) const;

    /**
    * Drop snapshot in the vfs
    * @param vfsName vfs name
    * @param snapshotName snapshot name
    * @return DSTORE_SUCC means success, otherwise means failure
    */
    ErrorCode DropSnapshotInVfs(const char *vfsName, const char *snapshotName) const;

    /**
    * Rollback snapshot in the vfs
    * @param vfsName vfs name
    * @param snapshotName snapshot name
    * @return DSTORE_SUCC means success, otherwise means failure
    */
    static ErrorCode RollbackSnapshotInVfs(const char *vfsName, const char *snapshotName);

    /**
    * Open snapshot in the vfs
    * @param openPara fileId, flags, filePath and snapshotName
    *                 fileId file_id, 16-bit
    *                 flags file open flags, refer to POSIX definitions
    *                 filePath file path, used in local file system
    *                 snapshotName snapshot name， used in pagestore file system
    * @param[out] fd FileDescriptor instance
    * @return DSTORE_SUCC means success, DSTORE_FAIL means failure
    */
    RetStatus OpenSnapshotFile(const FileOpenParam &openPara, FileDescriptor **fd);

    /**
     * This set of interfaces are provided for files without file id.
     */
    RetStatus CreateFile(const char *pathName, ::FileParameter const filePara, FileDescriptor **fd);
    RetStatus OpenFile(const char *pathName, int flags, FileDescriptor **fd);
    RetStatus RemoveFile(const char *pathName);
    RetStatus RenameFile(const char *srcPathName, const char *destPathName);
    RetStatus CloseFile(FileDescriptor *fd);

    int64 GetSize(FileDescriptor *fd);
    RetStatus Rewind(FileDescriptor *fd);
    RetStatus Pread(FileDescriptor *fd, void *buf, uint64 count, int64 offset, int64 *readSize);
    RetStatus Fsync(FileDescriptor *fd);
    RetStatus Truncate(FileDescriptor *fd, int64 length);
    RetStatus PreadAsync(FileDescriptor *fd, void *buf, uint64_t count, int64_t offset,
                         const AsyncIoContext *aioContext);
    RetStatus PwriteSync(FileDescriptor *fd, const void *inBuffer, uint64 count, int64 offset);
    RetStatus PwriteAsync(FileDescriptor *fd, const void *buf, uint64 count, int64 offset,
                         const AsyncIoContext *aioContext);

    RetStatus Extend(FileDescriptor *fd, int64 length);
    void OpenLocalAioIfNeed();
    RetStatus GetFileNameFromFileId(FileId fileId, char* fileName);
    ::VirtualFileSystem *GetStaticVfs() { return m_staticVfs; }
private:
    PdbId m_pdbId;
    ::FileDescriptor *m_fileDescArray[MAX_VFS_FILE_NUMBER];
    struct HTAB *m_fileDescHash;
    ::VirtualFileSystem *m_staticVfs;
    bool m_hasDestroyed;
    StorageType m_storeType;
    pthread_rwlock_t m_fdMutexArray[NUM_FD_ARRAY_MUTEX];
    Mutex *m_fdMappingMutexlock;
    DstoreMemoryContext m_mcxt;
    bool m_hasOpenLocalAio;

    void InitializeFdHash();
    void DestroyFdHash();
    VFSAdapterFdHashEntry *GetFileHashEntry(const char *pathName);
    void LockHashPartition(const char *pathName);
    void UnLockHashPartition(const char *pathName);
    void LockAllHashPartition();
    void UnLockAllHashPartition();
    RetStatus GetFileIdFromFileName(const char *fileName, FileId &fileId);
};

class IoFencingVFSCollection : public BaseObject {
public:
    IoFencingVFSCollection() noexcept;
    virtual ~IoFencingVFSCollection();
    DISALLOW_COPY_AND_MOVE(IoFencingVFSCollection);

    static IoFencingVFSCollection *GetSingleton() { return &m_self; }

    using IoFencingCallback = long long (*) (void);
    void SetTriggerIoFencingCallback(IoFencingCallback callback);
#ifdef UT
    inline void ClearIoFencingCallback() { m_ioFencingCallback = nullptr; }
#endif

    RetStatus AddActiveVFS(const char *vfsName, bool enableIoFencing = true, uint32 clusterId = INVALID_CLUSTER_ID);
    RetStatus RemoveActiveVFS(const char *vfsName, uint32 clusterId = INVALID_CLUSTER_ID);
    RetStatus GetActiveDrVfsIoFenceErr(const char *vfsName, uint32 clusterId, ErrorCode &err);

    RetStatus SetIoFencingWhiteList(const char *vfsName);
    RetStatus SetIoFencingWhiteList(const uint64 *nodeIds, const uint64 *nodeTimelineIds, uint32 nodeCnt, uint64 term);
    RetStatus SetDrIoFencingWhiteList(const uint64 *nodeIds, const uint64 *nodeTimelineIds, uint32 nodeCnt, uint64 term,
        uint32 clusterId);
    RetStatus GetIoFencingWhiteList(const char *vfsName, uint64 *nodeIds, uint32 *nodeCnt, uint64 *term,
        uint32 clusterId = INVALID_CLUSTER_ID) const;
    RetStatus GetIoFencingWhiteList(const char *vfsName, uint64 *nodeIds, uint32 *nodeCnt, uint64 *term,
        const char* ownerName) const;
    RetStatus TriggerIoFencing();
    /**
    * Clear IoFencing list for a vfs.
    * NOTE: this function can only be called by self cluster.
    * @param vfsName name of the vfs
    * @param ownerName ower name, e.g. cluster name
    * @return DSTORE_SUCC means success, DSTORE_FAIL means failure
    */
    RetStatus ClearIoFencingWhiteList(const char *vfsName, const char *ownerName);
    /**
    * Add IoFencing owner for a vfs. Add one owner at a time.
    * NOTE: this function can only be called by self cluster.
    * @param vfsName name of the vfs
    * @param ownerName ower name, e.g. cluster name
    * @return DSTORE_SUCC means success, DSTORE_FAIL means failure
    */
    RetStatus AddIoFenceOwner(const char *vfsName, const char *ownerName);
    /**
    * Del IoFencing owner for a vfs. Del one owner at a time.
    * NOTE: this function can only be called by self cluster.
    * @param vfsName name of the vfs
    * @param ownerName ower name, e.g. cluster name
    * @return DSTORE_SUCC means success, DSTORE_FAIL means failure
    */
    RetStatus DelIoFenceOwner(const char *vfsName, const char *ownerName);
    /**
    * Get all IoFencing owners for a vfs.
    * NOTE: this function can only be called by self cluster.
    * @param vfsName name of the vfs
    * @param[out] ownerNames ower name list, size: [MAX_CLIENT_ID_COUNT][OWNER_NAME_MAX_LEN]
    * @param[out] ownerCnt count of owners
    * @param[out] term term
    * @return DSTORE_SUCC means success, DSTORE_FAIL means failure
    */
    RetStatus GetIoFenceOwners(const char *vfsName, char (*ownerNames)[CLUSTER_NAME_MAX_LEN], uint32 *ownerCnt,
        uint64 *term);

private:
    RetStatus SetIoFencingWhiteList(const char *vfsName, const uint64 *nodeIds, const uint64 *nodeTimelineIds,
                                                        uint32 nodeCnt, uint64 term);
    void PrintIoFencingResult(const uint64 *nodeIds, uint32 nodeCnt, uint64 term, ErrorCode err,
        const VfsControlInfo *controlInfo, const int activeVFSCnt, uint32 clusterId = INVALID_CLUSTER_ID) const;
    static constexpr int maxVfsNameLen = 256;
    static constexpr int maxVfsNumPerInstance = 20;

    struct ActiveVFS {
        char vfsName[maxVfsNameLen];
        bool isValid;
        bool enableIoFencing;
        std::atomic<ErrorCode> drIoFenceTriggerErr{ERROR_SYS_OK};
    };

    struct ClusterActiveVFS {
        ActiveVFS activeVFS[maxVfsNumPerInstance];
        int activeVFSCnt;
        uint32 clusterId;
        bool isValid;
    };

    IoFencingCallback m_ioFencingCallback;

    pthread_rwlock_t m_mutex;
    ClusterActiveVFS m_clusterActiveVFS[MAX_CLUSTER_COUNT];
    int m_totalActiveVFSCnt;

    static IoFencingVFSCollection m_self;
};


} /* namespace DSTORE */

#endif
