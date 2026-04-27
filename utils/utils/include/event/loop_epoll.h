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
 * -----------------------------------------------------------------------------------
 *
 * loop_epoll.h
 *
 * Description:
 * On the Linux platform, the main loop is implemented using epoll.
 *
 * -----------------------------------------------------------------------------------
 */

#ifndef GSDB_LOOP_EPOLL_H
#define GSDB_LOOP_EPOLL_H

#include <sys/epoll.h>
#include "event/loop_base.h"

GSDB_BEGIN_C_CODE_DECLS

#define EPOLL_INIT_REVENTS   32
#define MAX_EPOLL_FD_COUNT   8192
#define MAX_EPOLL_SLEEP_MSEC 1000

typedef struct EpollLoop EpollLoop;
struct EpollLoop {
    EventLoop super;
    EventFd epollFd;
    int pipeFd[2];
    unsigned pollFdCount;
    unsigned reventsCapacity;
    struct epoll_event *revents;
};

typedef struct EpollLoopOps EpollLoopOps;
struct EpollLoopOps {
    EventLoopOps super;
};

DECLARE_NEW_TYPED_CLASS(EpollLoop)

GSDB_END_C_CODE_DECLS

#endif /* GSDB_LOOP_EPOLL_H */
