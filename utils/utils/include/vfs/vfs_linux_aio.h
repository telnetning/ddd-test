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

#ifndef UTILS_VFS_LINUX_AIO_H
#define UTILS_VFS_LINUX_AIO_H

#include <libaio.h>
#include "port/posix_thread.h"
#include "vfs/vfs_interface.h"

GSDB_BEGIN_C_CODE_DECLS

#define MAX_AIO_THREAD_NUM 16
#define MIN_AIO_EVENT_NUM  256
#define MAX_AIO_EVENT_NUM  65535

typedef struct AioThreadContext AioThreadContext;
struct AioThreadContext {
    bool isAioStop;
    Tid aioCompleterThreadId[MAX_AIO_THREAD_NUM];
    io_context_t ioctx;
    uint16_t maxEvents;
    uint16_t threadCount;
    void (*threadEnterCallback)(void);
    void (*threadExitCallback)(void);
};

void StopAIO(AioThreadContext *aioThreadContext);

ErrorCode StartAIO(uint16_t maxEvents, uint16_t threadCount, AioThreadContext **aioThreadContext,
                   void (*threadEnterCallback)(void), void (*threadExitCallback)(void));

GSDB_END_C_CODE_DECLS

#endif /* UTILS_VFS_LINUX_AIO_H */