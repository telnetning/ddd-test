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
 * win32_spin.c
 *
 * Description:
 * Implements for WIN32 standard spin lock interface.
 *
 * ---------------------------------------------------------------------------------
 */

#include <windows.h>
#include "port/win32_spin.h"

#define DEFAULT_SPIN_COUNT (-1)

void SpinLockInit(SpinLock *spinLock)
{
    InitializeCriticalSectionAndSpinCount(&spinLock->spinlock, DEFAULT_SPIN_COUNT);
}

void SpinLockDestroy(SpinLock *spinLock)
{
    DeleteCriticalSection(&spinLock->spinlock);
}

void SpinLockAcquire(SpinLock *spinLock)
{
    EnterCriticalSection(&spinLock->spinlock);
}

bool SpinLockTryAcquire(SpinLock *spinLock)
{
    if (TryEnterCriticalSection(&spinLock->spinlock) == 0) {
        return false;
    }
    return true;
}

void SpinLockRelease(SpinLock *spinLock)
{
    LeaveCriticalSection(&spinLock->spinlock);
}