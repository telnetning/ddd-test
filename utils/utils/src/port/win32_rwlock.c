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
 * win32_rwlock.c
 *
 * Description:
 * Implements for WIN32 standard rwlock interface.
 *
 * ---------------------------------------------------------------------------------
 */

#include <windows.h>
#include "port/win32_rwlock.h"

void RWLockInit(RWLock *rwlock, enum RWLockKind kind)
{
    // writer prefer only
    InitializeSRWLock(&rwlock->rwlock);
}

void RWLockDestroy(RWLock *rwlock)
{
    // nothing need to do.
    return;
}

void RWLockRdLock(RWLock *rwlock)
{
    AcquireSRWLockShared(&rwlock->rwlock);
}

bool RWLockRdLockTimeout(RWLock *rwlock, uint32_t timeUSec)
{
    // windows don't support timeout interface
    AcquireSRWLockShared(&rwlock->rwlock);
    return true;
}

bool RWLockTryRdLock(RWLock *rwlock)
{
    if (TryAcquireSRWLockShared(&rwlock->rwlock) == 0) {
        return false;
    }
    return true;
}

void RWLockWrLock(RWLock *rwlock)
{
    AcquireSRWLockExclusive(&rwlock->rwlock);
}

bool RWLockWrLockTimeout(RWLock *rwlock, uint32_t timeUSec)
{
    // windows don't support timeout interface
    AcquireSRWLockExclusive(&rwlock->rwlock);
    return true;
}

bool RWLockTryWrLock(RWLock *rwlock)
{
    if (TryAcquireSRWLockExclusive(&rwlock->rwlock) == 0) {
        return false;
    }
    return true;
}

void RWLockRdUnlock(RWLock *rwlock)
{
    ReleaseSRWLockShared(&rwlock->rwlock);
}

void RWLockWrUnlock(RWLock *rwlock)
{
    ReleaseSRWLockExclusive(&rwlock->rwlock);
}