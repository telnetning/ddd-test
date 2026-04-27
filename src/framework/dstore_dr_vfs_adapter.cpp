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
 * dstore_dr_vfs_adapter.cpp
 *
 * Description: this file defineds the behaviors how does storage engine communicate
 * with the external VFS library of aid cluster.
 *
 * ---------------------------------------------------------------------------------------
 *
 */

#include "framework/dstore_dr_vfs_adapter.h"

#include <csignal>
#include "framework/dstore_instance.h"
#include "errorcode/dstore_framework_error_code.h"

namespace DSTORE {

constexpr int VFS_WRITE_RETRY_TIME_US = 100;
constexpr int WARNING_LOG_INTEVAL_TIMES = 1000;

DrVfsAdapter::DrVfsAdapter(PdbId primaryPdbId, uint32 aidClusterId, const char* vfsName, const char* aidClusterName,
    const char* storeSpaceName)
    : m_primaryPdbId(primaryPdbId),
      m_standbyClusterId(aidClusterId),
      m_standbyVfsClientHandle(nullptr),
      m_standbyVfs(nullptr),
      m_storagetype(StorageType::PAGESTORE),
      m_fileDescHash(nullptr),
      m_mcxt(nullptr),
      m_fdMappingMutexlock(nullptr)
{
    /* 1: Sets vfs name of standby pdb */
    errno_t rc = strncpy_s(m_standbyVfsName, MAX_CONFIG_NAME_LENGTH, vfsName, MAX_CONFIG_NAME_LENGTH - 1);
    storage_securec_check(rc, "\0", "\0");
    /* 2: Sets aid cluster name */
    rc = strncpy_s(m_standbyClusterName, DSTORE_CLUSTER_NAME_MAX_LEN, aidClusterName, DSTORE_CLUSTER_NAME_MAX_LEN - 1);
    storage_securec_check(rc, "\0", "\0");
    rc = strncpy_s(m_storeSpaceName, STORESPACE_NAME_MAX_LEN, storeSpaceName, STORESPACE_NAME_MAX_LEN - 1);
    storage_securec_check(rc, "\0", "\0");
}

DrVfsAdapter::~DrVfsAdapter()
{}

void DrVfsAdapter::InitializeFdhash()
{
    /* 1: Create a shared DstoreMemoryContext for hash table */
    if (m_mcxt == nullptr) {
        m_mcxt = DstoreAllocSetContextCreate(
            g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LONGLIVE),
            "DrVfsAdaptorFdHashMemoryContext", ALLOCSET_DEFAULT_MINSIZE,
            ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_INITSIZE,
            MemoryContextType::SHARED_CONTEXT);

        StorageReleasePanic(STORAGE_VAR_NULL(m_mcxt), MODULE_FRAMEWORK, ErrMsg(
            "[DR_VFS_ADAPTER] Failed to create memory context for DrVfsAdaptorFdHashMemoryContext, pdbId(%u).",
            m_primaryPdbId));
    }
    /* 2: Initialize partitionally mapping locks */
    if (m_fdMappingMutexlock == nullptr) {
        Size hashMappingLockSize = NUM_FD_HASH_PARTITIONS * sizeof(Mutex);
        m_fdMappingMutexlock = (Mutex *)DstorePallocAligned(hashMappingLockSize, DSTORE_CACHELINE_SIZE, m_mcxt);

        StorageReleasePanic(m_fdMappingMutexlock == nullptr, MODULE_FRAMEWORK, ErrMsg(
            "[DR_VFS_ADAPTER] alloc memory for fdMappingMutexlock fail, pdbId(%u).", m_primaryPdbId));
        for (int i = 0; i < NUM_FD_HASH_PARTITIONS; i++) {
            MutexInit(&m_fdMappingMutexlock[i]);
        }
    }

    /* 3: Create shared hash table */
    if (m_fileDescHash == nullptr) {
        HASHCTL ctl;
        ctl.keysize = MAXPGPATH;                       /* Hash key is file pathName */
        ctl.entrysize = sizeof(VFSAdapterFdHashEntry); /* Hash value fd hash entry */
        ctl.hash = string_hash;
        ctl.num_partitions = NUM_FD_HASH_PARTITIONS;
        ctl.dsize = hash_select_dirsize(NUM_FD_HASH_INIT_ENTRY);
        ctl.hcxt = m_mcxt;

        m_fileDescHash =
            hash_create("dr vfs adaptor hash for file descriptor", NUM_FD_HASH_INIT_ENTRY, &ctl,
                        HASH_ELEM | HASH_FUNCTION | HASH_PARTITION | HASH_DIRSIZE | HASH_CONTEXT | HASH_SHRCTX);
        StorageReleasePanic(STORAGE_VAR_NULL(m_fileDescHash), MODULE_FRAMEWORK, ErrMsg(
            "[DR_VFS_ADAPTER] Failed to create fileDesc hash, pdbI(%u).", m_primaryPdbId));
    }
}

void DrVfsAdapter::DestroyFdhash()
{
    if (m_fileDescHash != nullptr) {
        hash_destroy(m_fileDescHash);
        m_fileDescHash = nullptr;
        ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg(
            "[DR_VFS_ADAPTER] fileDesc hash destroyed, pdbId(%u).", m_primaryPdbId));
    }
    if (m_fdMappingMutexlock != nullptr) {
        for (int i = 0; i < NUM_FD_HASH_PARTITIONS; i++) {
            MutexDestroy(&m_fdMappingMutexlock[i]);
        }
        DstorePfreeAligned(m_fdMappingMutexlock);
        m_fdMappingMutexlock = nullptr;
    }
    if (m_mcxt != nullptr) {
        DstoreMemoryContextDelete(m_mcxt);
        m_mcxt = nullptr;
    }
}

Mutex* DrVfsAdapter::LockFdhashPartition(const char* pathName)
{
    StorageAssert(pathName != nullptr);
    uint32 hashCode = get_hash_value(m_fileDescHash, static_cast<const void *>(pathName));
    Mutex *mutex = m_fdMappingMutexlock + (hashCode % NUM_FD_HASH_PARTITIONS);
    MutexLock(mutex);
    return mutex;
}

void DrVfsAdapter::UnLockFdhashPartition(const char* pathName)
{
    StorageAssert(pathName != nullptr);
    uint32 hashCode = get_hash_value(m_fileDescHash, static_cast<const void *>(pathName));
    Mutex *mutex = m_fdMappingMutexlock + (hashCode % NUM_FD_HASH_PARTITIONS);
    MutexUnlock(mutex);
}

void DrVfsAdapter::UnLockFdhashPartition(Mutex* mutex)
{
    StorageAssert(mutex != nullptr);
    MutexUnlock(mutex);
}

void DrVfsAdapter::LockAllFdhashPartition()
{
    for (int i = 0; i < NUM_FD_HASH_PARTITIONS; i++) {
        Mutex *mutex = &m_fdMappingMutexlock[i];
        MutexLock(mutex);
    }
}

void DrVfsAdapter::UnLockAllFdhashPartition()
{
    for (int i = 0; i < NUM_FD_HASH_PARTITIONS; i++) {
        Mutex *mutex = &m_fdMappingMutexlock[i];
        MutexUnlock(mutex);
    }
}

VFSAdapterFdHashEntry* DrVfsAdapter::GetFileHashEntry(const char *pathName)
{
    StorageAssert(m_standbyVfs);
    if (STORAGE_VAR_NULL(pathName)) {
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK, ErrMsg(
            "[DR_VFS_ADAPTER] GetHashFileDescriptor, pathName is empty, pdbId(%u).", m_primaryPdbId));
        return nullptr;
    }

    bool found = false;
    VFSAdapterFdHashEntry *entry = static_cast<VFSAdapterFdHashEntry *>(
        hash_search(m_fileDescHash, static_cast<const void *>(pathName), HASH_FIND, &found));
    if (unlikely(!found || entry == nullptr)) {
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK, ErrMsg(
            "[DR_VFS_ADAPTER] Find no entry in fileDesc hash, pdbId(%u), pathname: %s.",
            m_primaryPdbId, pathName));
        return nullptr;
    }
    StorageAssert(entry->refCount > 0 && entry->fileDesc != nullptr);

    return entry;
}

RetStatus DrVfsAdapter::InitializeVfsClientHandle()
{
    if (m_standbyVfsClientHandle != nullptr) {
        ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg(
            "[DR_VFS_ADAPTER] vfsClientHandle already exixts, no need to initialize again, aidClusterId(%u).",
            m_standbyClusterId));
        return DSTORE_SUCC;
    }
    /* get vfs client handle */
    m_standbyVfsClientHandle = GetVfsClientHandle(m_standbyClusterId);
    if (STORAGE_VAR_NULL(m_standbyVfsClientHandle)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg(
            "[DR_VFS_ADAPTER] Failed to init, unable to get vfsClientHandle, aidClusterId(%u).", m_standbyClusterId));
        return DSTORE_FAIL;
    }

    ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg(
        "[DR_VFS_ADAPTER] Init dr vfs adapter success, aidClusterId(%u).", m_standbyClusterId));
    return DSTORE_SUCC;
}

void DrVfsAdapter::DestroyVfsClientHandle()
{
    if (STORAGE_VAR_NULL(m_standbyVfsClientHandle)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg(
            "[DR_VFS_ADAPTER] vfsClientHandle already released, aidClusterId(%u).", m_standbyClusterId));
        return;
    }
    CloseAllFiles();
    RetStatus ret = ReleaseVfsClientHandle(m_standbyClusterId);
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_PANIC, MODULE_FRAMEWORK, ErrMsg(
            "[DR_VFS_ADAPTER] vfsClientHandle release failed when destroy dr vfs adapter, aidClusterId(%u).",
            m_standbyClusterId));
        return;
    }
}

RetStatus DrVfsAdapter::MountVfs(bool &needClearDrRelation)
{
    if (STORAGE_VAR_NULL(m_standbyVfsName)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("[DR_VFS_ADAPTER] failed to mount vfs, null m_standbyVfsName."));
        return DSTORE_FAIL;
    }
    if (STORAGE_VAR_NULL(m_standbyClusterName)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg(
            "[DR_VFS_ADAPTER] failed to mount vfs, null m_standbyClusterName."));
        return DSTORE_FAIL;
    }
    if (HasMounted()) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg(
            "[DR_VFS_ADAPTER] vfs already mounted, aidClusterId(%u), aidClusterName(%s), vsfName(%s).",
            m_standbyClusterId, m_standbyClusterName, m_standbyVfsName));
        return DSTORE_FAIL;
    }
    const char *tenantName = g_storageInstance->GetGuc()->tenantConfig->tenantName;
    needClearDrRelation = false;
    RetStatus ret = OpenVFS(m_standbyVfsClientHandle, tenantName, m_standbyVfsName, m_standbyClusterId,
        &m_standbyVfs, needClearDrRelation);
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg(
            "[DR_VFS_ADAPTER] mount vfs failed, aidClusterId(%u), aidClusterName(%s), vsfName(%s).",
            m_standbyClusterId, m_standbyClusterName, m_standbyVfsName));
        return DSTORE_FAIL;
    }
    ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg(
        "[DR_VFS_ADAPTER] mount vfs success, aidClusterId(%u), aidClusterName(%s), vsfName(%s).",
        m_standbyClusterId, m_standbyClusterName, m_standbyVfsName));
    return DSTORE_SUCC;
}

void DrVfsAdapter::UnmountVfs()
{
    if (!HasMounted()) {
        ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg(
            "[DR_VFS_ADAPTER] vfs not mounted, aidClusterId(%u), aidClusterName(%s), vsfName(%s).",
            m_standbyClusterId, m_standbyClusterName, m_standbyVfsName));
        return;
    }
    StorageReleasePanic(STORAGE_VAR_NULL(m_standbyVfsName), MODULE_FRAMEWORK, ErrMsg(
        "[DR_VFS_ADAPTER] failed to unmount vfs, null m_standbyVfsName."));

    ::ErrorCode retError = ::UnmountVfs(GetSyandbyVfs());
    StorageReleasePanic(retError, MODULE_FRAMEWORK,
        ErrMsg("[DR_VFS_ADAPTER] Failed to unmount standby VFS, error code = %lld", retError));
    m_standbyVfs = nullptr;
    RetStatus ret = IoFencingVFSCollection::GetSingleton()->RemoveActiveVFS(m_standbyVfsName, m_standbyClusterId);
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_PDBREPLICA, ErrMsg(
            "[DR_VFS_ADAPTER] Failed to remove active standby vfs, vfsName: %s, clusterId: %u",
            m_standbyVfsName, m_standbyClusterId));
    } else {
        ErrLog(DSTORE_LOG, MODULE_PDBREPLICA, ErrMsg(
            "[DR_VFS_ADAPTER] Success to remove active standby vfs, vfsName: %s, clusterId: %u",
            m_standbyVfsName, m_standbyClusterId));
    }
}

bool DrVfsAdapter::HasMounted() const
{
    return m_standbyVfs != nullptr;
}

RetStatus DrVfsAdapter::CreateFile(const char *pathName, const FileParameter filePara, FileDescriptor **fileDesc,
    ErrLevel errLevel)
{
    if (STORAGE_VAR_NULL(pathName) || STORAGE_VAR_NULL(fileDesc)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg(
            "[DR_VFS_ADAPTER] failed to create file, invalid pathName or fd."));
        return DSTORE_FAIL;
    }

    Mutex* mutex = LockFdhashPartition(pathName);
    /* 1: Make sure the file does not physically exist in vfs. */
    if (FileExists(pathName)) {
        storage_set_error(VFS_ERROR_FILE_ALREADY_CREATED_OR_OPEN);
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK, ErrMsg(
            "[DR_VFS_ADAPTER] Creating file failed, pdbId(%u), filePath(%s) already exists.",
            m_primaryPdbId, pathName));
        UnLockFdhashPartition(mutex);
        return DSTORE_FAIL;
    }

    /* 2: Find file in fd hash. */
    if (GetFileHashEntry(pathName) != nullptr) {
        storage_set_error(VFS_ERROR_FILE_ALREADY_CREATED_OR_OPEN);
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK, ErrMsg(
            "[DR_VFS_ADAPTER] Creating file failed, pdbId(%u), File(%s) has already been created or opened.",
            m_primaryPdbId, pathName));
        UnLockFdhashPartition(mutex);
        return DSTORE_FAIL;
    }

    /* 3: Create file in vfs. */
    bool retry = false;
    uint32 retryCnt = 0;
    do {
        retry = false;
        ErrorCode errcode = Create(m_standbyVfs, pathName, filePara, fileDesc);
        if (unlikely(errcode == VFS_ERROR_NEED_RETRY_AGAIN || errcode == VFS_ERROR_REQUEST_OVERLOAD)) {
            retryCnt++;
            if ((retryCnt % WARNING_LOG_INTEVAL_TIMES) == 0) {
                ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
                    ErrMsg("[DR_VFS_ADAPTER] FileIsExist get retry error %lld and will retry, now already retryCnt %u",
                    errcode, retryCnt));
            }
            GaussUsleep(VFS_WRITE_RETRY_TIME_US);
            retry = true;
        } else if (unlikely(errcode == VFS_ERROR_IO_FENCING_REFUSE)) {
            STORAGE_PROCESS_FORCE_EXIT(
                true, "[DR_VFS_ADAPTER] FileIsExist meets %lld(IO fencing refuse) so we will exit gaussdb process.",
                errcode);
        } else if (unlikely(errcode != ERROR_SYS_OK)) {
            storage_set_error(VFS_WARNING_FAILED_TO_CREATE_FILE);
            ErrLog(errLevel, MODULE_FRAMEWORK, ErrMsg(
                "[DR_VFS_ADAPTER] failed to create file, pdbId %u, error code: %lld, filepath: %s.",
                m_primaryPdbId, errcode, pathName));
            UnLockFdhashPartition(mutex);
            return DSTORE_FAIL;
        }
    } while (retry);

    /* 4: Insert file into fd hash. */
    bool found = false;
    VFSAdapterFdHashEntry *entry = static_cast<VFSAdapterFdHashEntry *>(
        hash_search(m_fileDescHash, static_cast<const char *>(pathName), HASH_ENTER, &found));
    StorageReleasePanic(
        (found || entry == nullptr), MODULE_FRAMEWORK, ErrMsg(
            "[DR_VFS_ADAPTER] pdbId(%u), File(%s) enter fd hash failed after file created.",
            m_primaryPdbId, pathName));
    errno_t rc = strcpy_s(entry->pathName, MAXPGPATH, pathName);
    storage_securec_check(rc, "\0", "\0");
    entry->refCount = 1;
    entry->fileDesc = *fileDesc;

    UnLockFdhashPartition(mutex);
    return DSTORE_SUCC;
}

RetStatus DrVfsAdapter::RemoveFile(const char *pathName)
{
    if (STORAGE_VAR_NULL(pathName)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("[DR_VFS_ADAPTER] Failed to remove file, invalid pathName."));
        return DSTORE_FAIL;
    }
    Mutex *mutex = LockFdhashPartition(pathName);
    /* Make sure the file has been created. */
    if (!FileExists(pathName)) {
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK, ErrMsg("[DR_VFS_ADAPTER] The file(%s) to be removed does not exist in "
            "vfs, pdbId: %hhu, clusterId: %u.", pathName, m_primaryPdbId, m_standbyClusterId));
        UnLockFdhashPartition(mutex);
        return DSTORE_SUCC;
    }
    VFSAdapterFdHashEntry *entry = GetFileHashEntry(pathName);
    if (entry != nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_PDBREPLICA, ErrMsg("[DR_VFS_ADAPTER] The file(%s) to be removed hasn't been "
            "closed, pdbId: %hhu, standbyClusterId: %u, refcount: %u.",
            pathName, m_primaryPdbId, m_standbyClusterId, entry->refCount));
        UnLockFdhashPartition(mutex);
        return DSTORE_FAIL;
    }
    ErrorCode errCode = ::Remove(m_standbyVfs, pathName);
    if (errCode != ERROR_SYS_OK) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("[DR_VFS_ADAPTER] Failed to remove file(%s), vfsRet(%lld), "
            "pdbId: %hhu, clusterId: %u", pathName, errCode, m_primaryPdbId, m_standbyClusterId));
        UnLockFdhashPartition(mutex);
        return DSTORE_FAIL;
    }
    UnLockFdhashPartition(mutex);
    return DSTORE_SUCC;
}

bool DrVfsAdapter::FileExists(const char *pathName)
{
    if (STORAGE_VAR_NULL(pathName)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg(
            "[DR_VFS_ADAPTER] failed to check file existence, null pathName."));
        return false;
    }
    bool fileExists = false;
    bool retry = false;
    uint32 retryCnt = 0;
    do {
        retry = false;
        ErrorCode errcode = ::FileIsExist(m_standbyVfs, pathName, &fileExists);
        if (likely(errcode == ERROR_SYS_OK)) {
            return fileExists;
        } else if (unlikely(errcode == VFS_ERROR_NEED_RETRY_AGAIN || errcode == VFS_ERROR_REQUEST_OVERLOAD)) {
            retryCnt++;
            if ((retryCnt % WARNING_LOG_INTEVAL_TIMES) == 0) {
                ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
                    ErrMsg("[DR_VFS_ADAPTER] FileIsExist get retry error %lld and will retry, now already retryCnt %u",
                    errcode, retryCnt));
            }
            GaussUsleep(VFS_WRITE_RETRY_TIME_US);
            retry = true;
        } else if (unlikely(errcode == VFS_ERROR_IO_FENCING_REFUSE)) {
            STORAGE_PROCESS_FORCE_EXIT(
                true, "[DR_VFS_ADAPTER] FileIsExist meets %lld(IO fencing refuse) so we will exit gaussdb process.",
                errcode);
        } else {
            if (errcode != ERROR_SYS_OK) {
                ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg(
                    "[DR_VFS_ADAPTER] Checking file existence in vfs failed, pdbId(%u), filePath(%s), errcode(%lld).",
                    m_primaryPdbId, pathName, errcode));
                return false;
            }
        }
    } while (retry);
    return fileExists;
}

RetStatus DrVfsAdapter::OpenFile(const char *pathName, int flags, FileDescriptor **fd)
{
    if (STORAGE_VAR_NULL(pathName) || STORAGE_VAR_NULL(fd)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg(
            "[DR_VFS_ADAPTER] failed to open file, invalid pathName or fd."));
        return DSTORE_FAIL;
    }

    Mutex *mutex = LockFdhashPartition(pathName);
    /* 1: if exists in hash table, return it */
    VFSAdapterFdHashEntry *entry = nullptr;
    if ((entry = GetFileHashEntry(pathName)) != nullptr) {
        entry->refCount++;
        *fd = entry->fileDesc;
        UnLockFdhashPartition(mutex);
        return DSTORE_SUCC;
    }

    /* 2: check file existence */
    if (!FileExists(pathName)) {
        storage_set_error(VFS_WARNING_FILE_NOT_EXISTS);
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK, ErrMsg(
            "[DR_VFS_ADAPTER] Open file failed, pdbId(%u), filePath(%s) not exists.", m_primaryPdbId, pathName));
        UnLockFdhashPartition(mutex);
        return DSTORE_FAIL;
    }

    /* 3: open file */
    bool retry = false;
    uint32 retryCnt = 0;
    do {
        retry = false;
        ErrorCode errcode = ::Open(m_standbyVfs, pathName, flags, fd);
        if (unlikely(errcode == VFS_ERROR_NEED_RETRY_AGAIN || errcode == VFS_ERROR_REQUEST_OVERLOAD)) {
            retryCnt++;
            if ((retryCnt % WARNING_LOG_INTEVAL_TIMES) == 0) {
                ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
                    ErrMsg("[DR_VFS_ADAPTER] FileIsExist get retry error %lld and will retry, now already retryCnt %u",
                    errcode, retryCnt));
            }
            GaussUsleep(VFS_WRITE_RETRY_TIME_US);
            retry = true;
        } else if (unlikely(errcode == VFS_ERROR_IO_FENCING_REFUSE)) {
            STORAGE_PROCESS_FORCE_EXIT(
                true, "[DR_VFS_ADAPTER] FileIsExist meets %lld(IO fencing refuse) so we will exit gaussdb process.",
                errcode);
        } else if (unlikely(errcode != ERROR_SYS_OK)) {
            storage_set_error(VFS_FATAL_FAILED_TO_OPEN_FILE);
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
                ErrMsg("[DR_VFS_ADAPTER] Failed to open file, error code: %lld, filepath: %s, pdbId: %u, "
                    "destClusterId: %u.", errcode, pathName, m_primaryPdbId, m_standbyClusterId));
            UnLockFdhashPartition(mutex);
            return DSTORE_FAIL;
        }
    } while (retry);

    /* 4: insert into fd hash */
    bool found = false;
    entry = static_cast<VFSAdapterFdHashEntry *>(
        hash_search(m_fileDescHash, static_cast<const char *>(pathName), HASH_ENTER, &found));
    StorageReleasePanic(
        (found || entry == nullptr), MODULE_FRAMEWORK, ErrMsg(
            "[DR_VFS_ADAPTER] pdbId(%u), File(%s) enter fd hash failed after file created.",
            m_primaryPdbId, pathName));
    errno_t rc = strcpy_s(entry->pathName, MAXPGPATH, pathName);
    storage_securec_check(rc, "\0", "\0");
    entry->refCount = 1;
    entry->fileDesc = *fd;
    UnLockFdhashPartition(mutex);
    return DSTORE_SUCC;
}

int64 DrVfsAdapter::GetSize(FileDescriptor *fd)
{
    if (STORAGE_VAR_NULL(fd)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("[DR_VFS_ADAPTER] Failed to get file size, null fd."));
        return -1;
    }
    int64 fileSize = 0;
    bool retry = false;
    uint32 retryCnt = 0;
    do {
        retry = false;
        ErrorCode errcode = ::GetSize(fd, &fileSize);
        if (unlikely(errcode == VFS_ERROR_NEED_RETRY_AGAIN || errcode == VFS_ERROR_REQUEST_OVERLOAD)) {
            retryCnt++;
            if ((retryCnt % WARNING_LOG_INTEVAL_TIMES) == 0) {
                ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
                    ErrMsg("[DR_VFS_ADAPTER] Pread get retry error %lld and will retry, now already retryCnt %u",
                    errcode, retryCnt));
            }
            GaussUsleep(VFS_WRITE_RETRY_TIME_US);
            retry = true;
        } else if (unlikely(errcode == VFS_ERROR_IO_FENCING_REFUSE)) {
            STORAGE_PROCESS_FORCE_EXIT(
                true, "[DR_VFS_ADAPTER] Pread meets %lld(IO fencing refuse) so we will exit gaussdb process.",
                errcode);
        } else if (unlikely(errcode != ERROR_SYS_OK)) {
            storage_set_error(VFS_WARNING_FAILED_TO_GET_SIZE);
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
                ErrMsg("[DR_VFS_ADAPTER] Failed to get file size, vfsRet(%lld).", errcode));
            return -1;
        }
    } while (retry);
    return fileSize;
}

RetStatus DrVfsAdapter::Extend(FileDescriptor *fd, int64 length)
{
    if (STORAGE_VAR_NULL(fd)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("[DR_VFS_ADAPTER] Failed to extend file, null fd."));
        return DSTORE_FAIL;
    }
    bool retry = false;
    uint32 retryCnt = 0;
    do {
        retry = false;
        ErrorCode errcode = ::Extend(fd, length);
        if (unlikely(errcode == VFS_ERROR_NEED_RETRY_AGAIN || errcode == VFS_ERROR_REQUEST_OVERLOAD)) {
            retryCnt++;
            if ((retryCnt % WARNING_LOG_INTEVAL_TIMES) == 0) {
                ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
                    ErrMsg("[DR_VFS_ADAPTER] Pread get retry error %lld and will retry, now already retryCnt %u",
                    errcode, retryCnt));
            }
            GaussUsleep(VFS_WRITE_RETRY_TIME_US);
            retry = true;
        } else if (unlikely(errcode == VFS_ERROR_IO_FENCING_REFUSE)) {
            STORAGE_PROCESS_FORCE_EXIT(
                true, "[DR_VFS_ADAPTER] Pread meets %lld(IO fencing refuse) so we will exit gaussdb process.",
                errcode);
        } else if (unlikely(errcode != ERROR_SYS_OK)) {
            storage_set_error(VFS_WARNING_FAILED_TO_EXTEND);
            int64 fileSize = GetSize(fd);
            ErrLog(fileSize >= length ? DSTORE_WARNING : DSTORE_ERROR, MODULE_FRAMEWORK,
                ErrMsg("[DR_VFS_ADAPTER] Failed to extend file, vfsRet(%lld).", errcode));
            return DSTORE_FAIL;
        }
    } while (retry);
    return DSTORE_SUCC;
}

RetStatus DrVfsAdapter::CloseFile(const char *pathName)
{
    if (STORAGE_VAR_NULL(pathName)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("[DR_VFS_ADAPTER] Failed to close file, null pathName."));
        return DSTORE_FAIL;
    }
    Mutex* mutex = LockFdhashPartition(pathName);
    /* 1: Make sure the file is opened. */
    VFSAdapterFdHashEntry *entry = GetFileHashEntry(pathName);
    if (entry == nullptr) {
        storage_set_error(VFS_WARNING_FILE_NOT_OPENED);
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK, ErrMsg(
            "[DR_VFS_ADAPTER] Closing file failed, pdbId(%u), filePath(%s) not opened.",
            m_primaryPdbId, pathName));
        UnLockFdhashPartition(mutex);
        return DSTORE_FAIL;
    }

    if (entry->refCount == 1) {
        bool retry = false;
        uint32 retryCnt = 0;
        do {
            retry = false;
            ErrorCode errcode = ::Close(entry->fileDesc);
            if (unlikely(errcode == VFS_ERROR_NEED_RETRY_AGAIN || errcode == VFS_ERROR_REQUEST_OVERLOAD)) {
                retryCnt++;
                if ((retryCnt % WARNING_LOG_INTEVAL_TIMES) == 0) {
                    ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
                        ErrMsg("[DR_VFS_ADAPTER] Pread get retry error %lld and will retry, now already retryCnt %u",
                        errcode, retryCnt));
                }
                GaussUsleep(VFS_WRITE_RETRY_TIME_US);
                retry = true;
            } else if (unlikely(errcode == VFS_ERROR_IO_FENCING_REFUSE)) {
                STORAGE_PROCESS_FORCE_EXIT(
                    true, "[DR_VFS_ADAPTER] Pread meets %lld(IO fencing refuse) so we will exit gaussdb process.",
                    errcode);
            } else if (unlikely(errcode != ERROR_SYS_OK)) {
                storage_set_error(VFS_WARNING_FAILED_TO_CLOSE_FILE);
                ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
                    ErrMsg("[DR_VFS_ADAPTER] Failed to close file, vfsRet(%lld), pathName: %s.", errcode, pathName));
                UnLockFdhashPartition(mutex);
                return DSTORE_FAIL;
            }
        } while (retry);
        bool found = false;
        (void)hash_search(m_fileDescHash, static_cast<const char *>(pathName), HASH_REMOVE, &found);
        StorageAssert(found);
    }
    entry->refCount--;
    UnLockFdhashPartition(mutex);
    return DSTORE_SUCC;
}

void DrVfsAdapter::CloseAllFiles()
{
    if (STORAGE_VAR_NULL(m_fileDescHash)) {
        ErrLog(DSTORE_LOG, MODULE_PDBREPLICA,
            ErrMsg("[STANDBY_BUILD] m_fileDescHash is nullptr, no need to close all files."));
        return;
    }
    HASH_SEQ_STATUS scan;
    hash_seq_init(&scan, m_fileDescHash);
    VFSAdapterFdHashEntry *entry;
    while ((entry = static_cast<VFSAdapterFdHashEntry *>(hash_seq_search(&scan))) != nullptr) {
        ErrLog(DSTORE_WARNING, MODULE_PDBREPLICA, ErrMsg("[STANDBY_BUILD] Unclosed file in drVfsAdapter, "
            "pathName: %s, refCount: %u", entry->pathName, entry->refCount));
        bool retry = false;
        uint32 retryCnt = 0;
        do {
            retry = false;
            ErrorCode errcode = ::Close(entry->fileDesc);
            if (unlikely(errcode == VFS_ERROR_NEED_RETRY_AGAIN || errcode == VFS_ERROR_REQUEST_OVERLOAD)) {
                retryCnt++;
                if ((retryCnt % WARNING_LOG_INTEVAL_TIMES) == 0) {
                    ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
                        ErrMsg("[DR_VFS_ADAPTER] Pread get retry error %lld and will retry, now already retryCnt %u",
                        errcode, retryCnt));
                }
                GaussUsleep(VFS_WRITE_RETRY_TIME_US);
                retry = true;
            } else if (unlikely(errcode == VFS_ERROR_IO_FENCING_REFUSE)) {
                STORAGE_PROCESS_FORCE_EXIT(
                    true, "[DR_VFS_ADAPTER] Pread meets %lld(IO fencing refuse) so we will exit gaussdb process.",
                    errcode);
            } else if (unlikely(errcode != ERROR_SYS_OK)) {
                ErrLog(DSTORE_ERROR, MODULE_PDBREPLICA,
                    ErrMsg("[STANDBY_BUILD] Failed to close file %s.", entry->pathName));
                continue;
            }
        } while (retry);
        bool found = false;
        (void)hash_search(m_fileDescHash, static_cast<const char *>(entry->pathName), HASH_REMOVE, &found);
    }
}
 
RetStatus DrVfsAdapter::PwriteSync(FileDescriptor *fd, const void *inBuffer, uint64 count, int64 offset)
{
    if (STORAGE_VAR_NULL(fd) || STORAGE_VAR_NULL(inBuffer)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
            ErrMsg("[DR_VFS_ADAPTER] Failed to write file, invalid fd or inBuffer."));
        return DSTORE_FAIL;
    }
 
    bool retry = false;
    uint32 retryCnt = 0;
    do {
        retry = false;
        int64 writtenSize = 0;
        ErrorCode errcode = ::PwriteSync(fd, inBuffer, count, offset, &writtenSize);
        if (likely(errcode == ERROR_SYS_OK)) {
            if (likely(writtenSize == static_cast<int64>(count))) {
                return DSTORE_SUCC;
            } else {
                ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK, ErrMsg(
                    "[DR_VFS_ADAPTER] PwriteSync partially success with writeSize %ld (expected size %lu) "
                    "at write offset %ld", writtenSize, count, offset));
                /* update write information and retry unwritten data */
                count -= static_cast<uint64>(writtenSize);
                offset += writtenSize;
                char *newBuffer = static_cast<char *>(const_cast<void *>(inBuffer)) + writtenSize;
                inBuffer = const_cast<const void*>(static_cast<void *>(newBuffer));
                retry = true;
            }
        } else if (unlikely(errcode == VFS_ERROR_NEED_RETRY_AGAIN || errcode == VFS_ERROR_REQUEST_OVERLOAD)) {
            retryCnt++;
            if ((retryCnt % WARNING_LOG_INTEVAL_TIMES) == 0) {
                ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
                    ErrMsg("[DR_VFS_ADAPTER] PwriteSync get retry error %lld and will retry, now already retryCnt %u",
                    errcode, retryCnt));
            }
            GaussUsleep(VFS_WRITE_RETRY_TIME_US);
            retry = true;
        } else if (unlikely(errcode == VFS_ERROR_IO_FENCING_REFUSE)) {
            STORAGE_PROCESS_FORCE_EXIT(
                true, "[DR_VFS_ADAPTER] PwriteSync meets %lld(IO fencing refuse) so we will exit gaussdb process.",
                errcode);
        } else {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("[DR_VFS_ADAPTER] PwriteSync get vfs error %lld, "
                    "already writeSize %ld at offset %ld, expected write size %lu.",
                    errcode, writtenSize, offset, count));
            return DSTORE_FAIL;
        }
    } while (retry);
    ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("[DR_VFS_ADAPTER] PWriteSync: unexpected error, shouldn't be here."));
    return DSTORE_FAIL;
}

RetStatus DrVfsAdapter::Pread(FileDescriptor *fd, void *buf, uint64 count, int64 offset, int64 *readSize)
{
    if (fd == nullptr || buf == nullptr) {
        storage_set_error(VFS_WARNING_FILE_DESCRIPTOR_EMPTY);
        ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
               ErrMsg("Pread file failed, fd is empty."));
        return DSTORE_FAIL;
    }

    bool retry = false;
    uint32 retryCnt = 0;
    do {
        retry = false;
        ErrorCode errcode = ::Pread(fd, buf, count, offset, readSize);
        if (likely(errcode == ERROR_SYS_OK)) {
            if (likely(*readSize == static_cast<int64>(count))) {
                return DSTORE_SUCC;
            } else {
                ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
                    ErrMsg("[DR_VFS_ADAPTER] Pread success with readSize %ld (expected size %lu) at read offset %ld",
                    *readSize, count, offset));
                retry = true;
            }
        } else if (unlikely(errcode == VFS_ERROR_NEED_RETRY_AGAIN || errcode == VFS_ERROR_REQUEST_OVERLOAD)) {
            retryCnt++;
            if ((retryCnt % WARNING_LOG_INTEVAL_TIMES) == 0) {
                ErrLog(DSTORE_WARNING, MODULE_FRAMEWORK,
                    ErrMsg("[DR_VFS_ADAPTER] Pread get retry error %lld and will retry, now already retryCnt %u",
                    errcode, retryCnt));
            }
            GaussUsleep(VFS_WRITE_RETRY_TIME_US);
            retry = true;
        } else if (unlikely(errcode == VFS_ERROR_IO_FENCING_REFUSE)) {
            STORAGE_PROCESS_FORCE_EXIT(
                true, "[DR_VFS_ADAPTER] Pread meets %lld(IO fencing refuse) so we will exit gaussdb process.",
                errcode);
        } else {
            if (errcode != ERROR_SYS_OK) {
                ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
                    ErrMsg("[DR_VFS_ADAPTER] Pread error for getting fatal vfs error %lld,already readSize %ld at "
                    "offset %ld, expected read size %lu.", errcode, *readSize, offset, count));
                return DSTORE_FAIL;
            }
        }
    }  while (retry);
    return DSTORE_SUCC;
}

} /* namespace DSTORE */
