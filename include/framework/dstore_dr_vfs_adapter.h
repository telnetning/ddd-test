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
 * dstore_dr_vfs_adapter.h
 *
 * Description: this file provides the facilities to adapt to the external VFS library of aid cluster.
 *
 * ---------------------------------------------------------------------------------------
 *
 */

#ifndef DSTORE_DSTORE_DR_VFS_ADAPTER_H
#define DSTORE_DSTORE_DR_VFS_ADAPTER_H
#include <atomic>
#include "common/dstore_common_utils.h"
#include "common/dstore_datatype.h"
#include "config/dstore_vfs_config.h"
#include "common/memory/dstore_mctx.h"
#include "framework/dstore_instance_interface.h"
#include "port/platform_port.h"
#include "framework/dstore_vfs_adapter.h"
#include "wal/dstore_wal_logstream.h"

namespace DSTORE {

/* max read/write size each time: 1M */
constexpr uint32 ONCE_RW_BLOCK_NUM = 64;
constexpr uint32 ONCE_RW_DATA_SIZE = ONCE_RW_BLOCK_NUM * BLCKSZ;

class DrVfsAdapter : public BaseObject {
public:
    DrVfsAdapter() = delete;
    DrVfsAdapter(PdbId primaryPdbId, uint32 aidClusterId, const char* vfsName, const char* aidClusterName,
        const char* storeSpaceName);
    virtual ~DrVfsAdapter();

    DISALLOW_COPY_AND_MOVE(DrVfsAdapter);

    void InitializeFdhash();
    void DestroyFdhash();
    /**
     * Lock the partition of fdhash table.
     *
     * @param pathName the path to the file
     * @return the mutex of the partition
     */
    Mutex* LockFdhashPartition(const char* pathName);
    /**
     * Unlock the partition of fdhash table.
     *
     * @param pathName the path to the file
     * @return void
     */
    void UnLockFdhashPartition(const char* pathName);
    /**
     * Unlock the partition of fdhash table.
     *
     * @param mutex the mutex of the partition
     * @return void
     */
    void UnLockFdhashPartition(Mutex* mutex);
    void LockAllFdhashPartition();
    void UnLockAllFdhashPartition();
    /**
     * Search the file descriptor of the file in fdhash table.
     * should hold the partition lock of fdhash table before calling this function.
     *
     * @param pathName the path to the file
     * @return entry of the file descriptor
     */
    VFSAdapterFdHashEntry* GetFileHashEntry(const char *pathName);

    RetStatus InitializeVfsClientHandle();
    void DestroyVfsClientHandle();
    RetStatus MountVfs(bool &needClearDrRelation);
    void UnmountVfs();
    bool HasMounted() const;
    /**
     * Create a file in the VFS of aid cluster, and the file is open by default.
     *
     * @param pathName the path to the file
     * @param filePara the parameter used to create the file
     * @param[out] fileDesc the file descriptor of the file
     * @return DSTORE_SUCC means success, DSTORE_FAIL means failure
     */
    RetStatus CreateFile(const char *pathName, const FileParameter filePara, FileDescriptor **fileDesc,
        ErrLevel errLevel = ErrLevel::DSTORE_ERROR);
    /**
     * Check if the designated file exists in the VFS of aid cluster.
     *
     * @param pathName the path to the file
     * @return true means exsit, false means not exist
     */
    bool FileExists(const char *pathName);
    /**
     * Open file in the VFS of aid cluster.
     * @param pathName the path to the file
     * @param[out] fileDesc the file descriptor of the file
     * @return DSTORE_SUCC means success, DSTORE_FAIL means failure
     */
    RetStatus OpenFile(const char *pathName, int flags, FileDescriptor **fd);
    /**
     * Remove file in the VFS of aid cluster
     * @param pathName the path to the file
     * @return DSTORE_SUCC means success, DSTORE_FAIL means failure
     */
    RetStatus RemoveFile(const char *pathName);

    int64 GetSize(FileDescriptor *fd);

    RetStatus Extend(FileDescriptor *fd, int64 length);

    RetStatus CloseFile(const char* pathName);

    void CloseAllFiles();
    
    RetStatus PwriteSync(FileDescriptor *fd, const void *inBuffer, uint64 count, int64 offset);

    RetStatus Pread(FileDescriptor *fd, void *buf, uint64 count, int64 offset, int64 *readSize);

    inline const char *GetStoreSpaceName()
    {
        return m_storeSpaceName;
    }
    ::VirtualFileSystem *GetSyandbyVfs()
    {
        return m_standbyVfs;
    }

private:
    PdbId m_primaryPdbId;
    uint32 m_standbyClusterId;
    char m_standbyVfsName[MAX_CONFIG_NAME_LENGTH];
    char m_standbyClusterName[DSTORE_CLUSTER_NAME_MAX_LEN];
    char m_storeSpaceName[STORESPACE_NAME_MAX_LEN];
    VfsClientHandle *m_standbyVfsClientHandle;
    ::VirtualFileSystem *m_standbyVfs;
    StorageType m_storagetype;
    struct HTAB *m_fileDescHash;
    DstoreMemoryContext m_mcxt;
    Mutex *m_fdMappingMutexlock;
};

} /* namespace DSTORE */

#endif
