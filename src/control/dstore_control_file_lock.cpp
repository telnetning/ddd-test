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
 * dstore_control_file_lock.cpp
 *
 * IDENTIFICATION
 *        src/control/dstore_control_file_lock.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include <pthread.h>
#include "securec.h"

#include "framework/dstore_instance.h"
#include "framework/dstore_thread.h"
#include "lock/dstore_lock_mgr.h"
#ifdef UT
#include <fcntl.h>
#endif
#include "control/dstore_control_file_lock.h"

namespace DSTORE {


#ifdef UT
PThreadRWLock::PThreadRWLock(PdbId pdbId, char *path, int groupType)
#else
PThreadRWLock::PThreadRWLock(PdbId pdbId)
#endif
{
#ifndef UT
    (void)pthread_rwlock_init(&m_mutex, nullptr);
    m_pdbId = pdbId;
#else
    if (path != nullptr) {
        errno_t rc = sprintf_s(m_path, MAXPGPATH, "%s_utgrouplock%d", path, groupType);
        storage_securec_check_ss(rc);
    } else {
        m_path[0] = '\0';
        StorageAssert(0);
    }
    m_fd = open(m_path, O_RDWR | O_CREAT, 0644);
#endif
}

PThreadRWLock::~PThreadRWLock()
{
#ifdef UT
    if (m_fd != -1) {
        close(m_fd);
    }
#else
    (void)pthread_rwlock_destroy(&m_mutex);
#endif
}

RetStatus PThreadRWLock::Lock(CFLockMode mode)
{
#ifdef UT
    if (m_fd == -1) {
        StorageAssert(0);
        return DSTORE_FAIL;
    }
    struct flock fl;
    fl.l_whence = SEEK_SET;
    fl.l_start = 0;
    fl.l_len = 0;
    if (mode == CFLockMode::CF_SHARE) {
        fl.l_type = F_RDLCK;
    } else {
        fl.l_type = F_WRLCK;
    }
    if (fcntl(m_fd, F_SETLKW, &fl) == -1) {
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
#else
    int rt = -1;
    if (mode == CFLockMode::CF_SHARE) {
        rt = pthread_rwlock_rdlock(&m_mutex);
    } else {
        rt = pthread_rwlock_wrlock(&m_mutex);
    }

    return rt == EOK ? DSTORE_SUCC : DSTORE_FAIL;
#endif
}

void PThreadRWLock::Unlock(CFLockMode /* mode */)
{
#ifdef UT
    struct flock fl;
    fl.l_whence = SEEK_SET;
    fl.l_start = 0;
    fl.l_len = 0;
    fl.l_type = F_UNLCK;
    fcntl(m_fd, F_SETLK, &fl);
    return;
#endif
    (void)pthread_rwlock_unlock(&m_mutex);
}

}