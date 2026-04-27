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
 * event_io.c
 *
 * Description:
 * 1. implementation of event for io, sush as for socket.
 *
 * -----------------------------------------------------------------------------------
 */

#include "event/event_io.h"
#include "event/loop_base.h"

static ErrorCode IOEventInit(IOEvent *self, TypeInitParams *initData)
{
    ASSERT(self != NULL);
    EventCfgParameter *cfg = DOWN_TYPE_CAST(initData, EventCfgParameter);
    self->fd = cfg->eventFd;
    self->eventType = cfg->eventType;
    self->reventType = 0;
    return ERROR_SYS_OK;
}

static void IOEventFinalize(IOEvent *self)
{
    ASSERT(self != NULL);
    CallEventCallBackFinalize(UP_TYPE_CAST(self, Event));
}

static bool IOEventNeedPolling(Event *super)
{
    SYMBOL_UNUSED IOEvent *self = DOWN_TYPE_CAST(super, IOEvent);
    ASSERT(self != NULL);
    return true;
}

static EventFd IOEventGetEventFd(Event *super)
{
    IOEvent *self = DOWN_TYPE_CAST(super, IOEvent);
    ASSERT(self != NULL);
    return self->fd;
}

static void IOEventSetEventType(Event *super, unsigned eventType)
{
    IOEvent *self = DOWN_TYPE_CAST(super, IOEvent);
    ASSERT(self != NULL);
    self->eventType = eventType;
}

static unsigned IOEventGetEventType(Event *super)
{
    IOEvent *self = DOWN_TYPE_CAST(super, IOEvent);
    ASSERT(self != NULL);
    return self->eventType;
}

static void IOEventSetReventType(Event *super, unsigned reventType)
{
    IOEvent *self = DOWN_TYPE_CAST(super, IOEvent);
    ASSERT(self != NULL);
    self->reventType = reventType;
}

static unsigned IOEventGetReventType(Event *super)
{
    IOEvent *self = DOWN_TYPE_CAST(super, IOEvent);
    ASSERT(self != NULL);
    return self->reventType;
}

static void IOEventOpsInit(IOEventOps *self)
{
    GET_FOPS(Event)->needPolling = IOEventNeedPolling;
    GET_FOPS(Event)->getEventFd = IOEventGetEventFd;
    GET_FOPS(Event)->setEventType = IOEventSetEventType;
    GET_FOPS(Event)->getEventType = IOEventGetEventType;
    GET_FOPS(Event)->setReventType = IOEventSetReventType;
    GET_FOPS(Event)->getReventType = IOEventGetReventType;
}

DEFINE_NEW_TYPED_CLASS(IOEvent, Event)
