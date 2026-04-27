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
/* included headers are sorted alphabetically unless they have dependency. */
#include <atomic>
#include <thread>
#include <fcntl.h>
#include <ftw.h>         /* for nftw() */
#include <securectype.h> /* for snprintf_s() */
#include <sys/file.h>    /* for flock() */
#include "ut_mock/ut_comm_api_mock.h"
#include "port/dstore_port.h"

namespace DSTORE {

/* Initialize global thread-local flag. */
thread_local bool g_hasRPCBeenCalled = false;

UTRpcFile::UTRpcFile() : m_fileDesc(INVALID_FILE_DESC)
{
    if (pthread_mutex_init(&m_fileMutex, NULL) != 0) {
        printf("Mutex Init failed\n");
        StorageAssert(false);
    }
}

UTRpcFile::UTRpcFile(bool isRequestFile, NodeId destNodeId)
    : m_selfNodeId(destNodeId), m_fileDesc(INVALID_FILE_DESC), m_isRequestFile(isRequestFile)
{
    int rc = memset_s(m_name, RPC_FILE_NAME_MAX_LEN, 0, RPC_FILE_NAME_MAX_LEN);
    storage_securec_check(rc, "\0", "\0");
    rc = snprintf_s(m_name, RPC_FILE_NAME_MAX_LEN, RPC_FILE_NAME_MAX_LEN - 1, "%s/%s_to_%d", RPC_DIRECTORY,
                    m_isRequestFile ? "request" : "response", m_selfNodeId);
    storage_securec_check_ss(rc);

    if (pthread_mutex_init(&m_fileMutex, NULL) != 0) {
        printf("\n mutex Init failed\n");
        StorageAssert(false);
    }
}

void UTRpcFile::ClearContent()
{
    /* The file must've been opened. */
    StorageAssert(m_fileDesc != INVALID_FILE_DESC);
    int ret = ftruncate(m_fileDesc, 0);
    if (ret != 0) {
        fprintf(stderr, "Failed to clear the file content, file name: %s, %s\n", m_name, strerror(errno));
        StorageAssert(false);
    }
}

bool UTRpcFile::Open()
{
    if (m_fileDesc != INVALID_FILE_DESC) {
        return false;
    }

    /*
     * O_CREAT: create it if can't find;
     * O_RDWR: assign read/write permissions
     * S_IRWXU: mode == 0777, accessible for everyone
     */
    m_fileDesc = open(m_name, O_CREAT | O_RDWR, S_IRWXU | S_IRWXG | S_IRWXO);
    if (m_fileDesc == -1) {
        return false;
    }
    StorageAssert(m_fileDesc != -1);
    return true;
}

void UTRpcFile::Lock()
{
    /* We need a mutex to prevent multi-thread contentions. */
    pthread_mutex_lock(&m_fileMutex);
    /* The file must've been opened. */
    StorageAssert(m_fileDesc != INVALID_FILE_DESC);
    if (flock(m_fileDesc, LOCK_EX) == -1) {
        fprintf(stderr, "Failed to lock the file %s, %s\n", m_name, strerror(errno));
        StorageAssert(false);
    }
}

void UTRpcFile::Unlock()
{
    /* The file must've been opened. */
    StorageAssert(m_fileDesc != INVALID_FILE_DESC);
    if (flock(m_fileDesc, LOCK_UN) == -1) {
        fprintf(stderr, "Failed to unlock the file %s, %s\n", m_name, strerror(errno));
        StorageAssert(false);
    }
    pthread_mutex_unlock(&m_fileMutex);
}

void UTRpcFile::Close()
{
    /* The file must've been opened. */
    StorageAssert(m_fileDesc != INVALID_FILE_DESC);
    /* Flush all the contents in file system's buffer down to the storage media. Someone might ask why do we only put
     * fsync() here. Shouldn't it be needed every time we modify the file? In reality, fsync() of a file descriptor is
     * not the same as fflush() of a file stream. When we call read/write, in fact, we get/modify the data from/in OS's
     * file system cache and it's consistent across different processes. What fsync() does is to make sure the file
     * content is consistent between the OS cache and the storage media. However, for a file stream, every process has
     * got a userspace buffer for it. Therefore, we need to do fflush() to push the userspace buffer all the way to the
     * kernel when we modify the file, in order to keep it consistent for all the processes who are interested in it. */
    int rc = fsync(m_fileDesc);
    if (rc == -1) {
        fprintf(stderr, "File %s's fsync failed, %s\n", m_name, strerror(errno));
        StorageAssert(false);
    }
    /* The clsoe syscall has got unlocking semantics. */
    rc = close(m_fileDesc);
    if (rc == -1) {
        fprintf(stderr, "Closing file %s's failed, %s\n", m_name, strerror(errno));
        StorageAssert(false);
    }
    m_fileDesc = INVALID_FILE_DESC;
}

void UTRpcFile::Write(const void *buf, size_t count)
{
    int writtenBytes = write(m_fileDesc, buf, count);
    if (writtenBytes != count) {
        fprintf(stderr, "Writing contents to file %s's failed, %s\n", m_name, strerror(errno));
        StorageAssert(false);
    }
}

} /* namespace DSTORE */
