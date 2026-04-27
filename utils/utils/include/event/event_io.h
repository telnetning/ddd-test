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
 * event_io.h
 *
 * Description:
 * 1. Event object used to listen for IO events, such as a socket.
 *
 * -----------------------------------------------------------------------------------
 */

#ifndef GSDB_EVENT_IO_H
#define GSDB_EVENT_IO_H

#include "event/event_base.h"

GSDB_BEGIN_C_CODE_DECLS

typedef struct IOEvent IOEvent;
struct IOEvent {
    Event super;
    EventFd fd;
    unsigned eventType;
    unsigned reventType;
};

typedef struct IOEventOps IOEventOps;
struct IOEventOps {
    EventOps super;
};

DECLARE_NEW_TYPED_CLASS(IOEvent)

GSDB_END_C_CODE_DECLS

#endif /* GSDB_EVENT_IO_H */
