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
 * IDENTIFICATION
 *        src/port/dstore_futex.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include <sys/syscall.h>
#include <unistd.h>
#include <linux/futex.h>
#include "framework/dstore_thread.h"
#include "lock/dstore_lock_dummy.h"
#include "common/concurrent/dstore_atomic.h"
#include "common/error/dstore_error.h"
#include "common/dstore_tsan_annotation.h"
#include "errorcode/dstore_common_error_code.h"
#include "common/concurrent/dstore_futex.h"

constexpr uint32 DSTORE_FUTEX_INIT = 0;
constexpr uint32 DSTORE_FUTEX_POSTED = 1;
static inline int Futex(int *uaddr, int futexOp, int val, const struct timespec *timeout, int val3)
{
    return static_cast<int>(syscall(SYS_futex, uaddr, futexOp | FUTEX_PRIVATE_FLAG, val, timeout, nullptr, val3));
}

namespace DSTORE {
void DstoreFutex::DstoreFutexInit()
{
    m_futex = DSTORE_FUTEX_INIT; /* futex is 0, indicates that futex has not been posted */
}

bool DstoreFutex::DstoreFutexTry()
{
    if (GsCompareAndSwap32(&m_futex, DSTORE_FUTEX_POSTED, DSTORE_FUTEX_INIT)) {
        /* success */
        return true;
    }
    return false;
}

/* the implementation is the simplest futex application implemented with reference to
   the man page futex() example, and already fully meets the current usage scenario */
void DstoreFutex::DstoreFutexWait(SYMBOL_UNUSED bool interruptOK, const struct timespec *timeout)
{
    for (;;) {
        if (GsCompareAndSwap32(&m_futex, DSTORE_FUTEX_POSTED, DSTORE_FUTEX_INIT)) {
            /* success */
            break;
        }
        CHECK_FOR_INTERRUPTS();
        if (Futex(&m_futex, FUTEX_WAIT, DSTORE_FUTEX_INIT, timeout, 0) == 0) {
            continue;
        }
        /* EINTR (operation was interrupted by a signal) */
        /* If the futex value does not match val, then the call fails immediately with the error EAGAIN. */
        if ((errno == EINTR) || (errno == EAGAIN)) {
            /* if timeout not nullptr, caller need handle and adjust the timeout value ? */
            continue;
        }
        /* ETIMEDOUT occur, return to caller handle it */
        if (errno == ETIMEDOUT) {
            /* here can't storage_set_error, becase timeout only using to implement sleep, timeout is normal case */
            break;
        }

        ErrLog(DSTORE_ERROR, MODULE_LOCK, ErrMsg("System call Futex() wait return unhandled errno %d.", errno));
        break;
    }

    /* ENABLE_THREAD_CHECK only, Must acquire vector clock info from other
     * thread after got the lock */
    TsAnnotateHappensAfter(&m_futex);
}

void DstoreFutex::DstoreFutexPost()
{
    if (GsCompareAndSwap32(&m_futex, DSTORE_FUTEX_INIT, DSTORE_FUTEX_POSTED)) {
        int ret = Futex(&m_futex, FUTEX_WAKE, 1, nullptr, 0);
        if (unlikely(ret == -1)) {
            ErrLog(DSTORE_ERROR, MODULE_LOCK, ErrMsg("System call Futex() wake return unhandled errno %d.", errno));
        }
    }
    /* ENABLE_THREAD_CHECK only, Must release vector clock info to other
     * threads before unlock */
    TsAnnotateHappensBefore(&m_futex);
}
}
