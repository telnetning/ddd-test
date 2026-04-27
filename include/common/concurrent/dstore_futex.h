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
 *        include/common/concurrent/dstore_futex.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef STORAGE_FUTEX_H
#define STORAGE_FUTEX_H
#include "defines/common.h"

namespace DSTORE {

class DstoreFutex {
public:
    void DstoreFutexInit();
    bool DstoreFutexTry();
    /* timeout default is nullptr, so it is simple wait and notify mechanism.
       timeout not nullptr currently only used to implement sleep(x) */
    void DstoreFutexWait(SYMBOL_UNUSED bool interruptOK, const struct timespec *timeout = nullptr);
    void DstoreFutexPost();

private:
    int m_futex;
};

}
#endif /* STORAGE_FUTEX_H */
