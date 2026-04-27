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
 * dstore_control_file.h
 *  ControlFiles records tablespace metadata (files contained in a tablespace), and logStream information
 *  (storage location and recovery point for each LogStream)
 *
 *
 * IDENTIFICATION
 *        dstore/include/control/dstore_control_file_lock.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_CONTROL_FILE_LOCK_H
#define DSTORE_CONTROL_FILE_LOCK_H

#include "lock/dstore_lock_datatype.h"

namespace DSTORE {

enum class CFLockMode {
    CF_SHARE,
    CF_EXCLUSIVE
};

class ControlFileGlobalLock : public BaseObject {
public:
    virtual RetStatus Lock(CFLockMode mode) = 0;
    virtual void Unlock(CFLockMode mode) = 0;
    virtual ~ControlFileGlobalLock() {}
};

class ControlFileLockAutoUnlock {
public:
    explicit ControlFileLockAutoUnlock(ControlFileGlobalLock *fileLock, CFLockMode mode)
        : m_fileLock(fileLock), m_lockMode(mode)
    {}
    ~ControlFileLockAutoUnlock()
    {
        UnLock();
        m_fileLock = nullptr;
    }
private:
    void UnLock() noexcept
    {
        m_fileLock->Unlock(m_lockMode);
    }
    ControlFileGlobalLock *m_fileLock;
    CFLockMode m_lockMode;
};

class PThreadRWLock final : public ControlFileGlobalLock {
public:
#ifdef UT
    explicit PThreadRWLock(PdbId pdbId, char *path, int groupType);
#else
    explicit PThreadRWLock(PdbId pdbId);
#endif

    ~PThreadRWLock() final;

    DISALLOW_COPY_AND_MOVE(PThreadRWLock);

    RetStatus Lock(CFLockMode mode) final;

    void Unlock(CFLockMode /* mode */) final;

private:
    pthread_rwlock_t m_mutex;
    PdbId m_pdbId;
#ifdef UT
    int m_fd;
    char m_path[MAXPGPATH];
#endif
};

}

#endif /* DSTORE_CONTROL_FILE_LOCK_H */
