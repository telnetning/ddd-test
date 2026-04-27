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
 * event_base.h
 *
 * Description:
 * 1. Defines the super class of event objects, including I/O and timer event objects.
 *
 * -----------------------------------------------------------------------------------
 */

#ifndef GSDB_EVENT_BASE_H
#define GSDB_EVENT_BASE_H

#include "types/ref_object.h"
#include "types/data_types.h"
#include "port/platform_port.h"
#include "defines/utils_errorcode.h"
#include "container/linked_list.h"
#include "event/event_loop.h"
#include "event/timer_wheel.h"

GSDB_BEGIN_C_CODE_DECLS

#define EVENT_MASK    (EVENT_READ | EVENT_WRITE)
#define MSEC_PER_SEC  1000
#define USEC_PER_MSEC 1000

typedef int32_t EventFd;
typedef uint64_t EventTime;

/* User-registered callback function, which is invoked when an event
 * occurs or when the event object is destroyed.
 */
typedef struct EventCallbacks EventCallbacks;
struct EventCallbacks {
    void *context;
    OnEventRunFunc run;
    OnEventFinalizeFunc finalize;
};

/*
 * This class describes the event objects that are detected, monitored,
 * and dispatched by the event loop, including I/O event objects and
 * timer objects.
 */
struct Event {
    RefObject super;
    unsigned flags;
    unsigned timeout;  /* in msec, 0 means never timeout */
    Atomic32 loopRefs; /* in action queue & attached */
    EventTime timeoutPoint;
    uint32_t timeoutJiffy;                        /* Timer wheel related variables, equal to current timeout / jiffy */
    uint32_t addWheelIndex[WHEEL_MAX_LIST_COUNT]; /* Timer wheel related variables */
    DListNode attachedNode;
    DListNode activeNode;
    DListNode timeoutNode;
    EventLoop *bornFrom;
    EventLoop *attached;
    void *userData;
    EventCallbacks callbacks;
};

typedef struct EventOps EventOps;
struct EventOps {
    RefObjectOps super;
    bool (*needPolling)(Event *self);
    EventFd (*getEventFd)(Event *self);
    void (*setEventType)(Event *self, unsigned eventType);
    unsigned (*getEventType)(Event *self);
    void (*setReventType)(Event *self, unsigned reventType);
    unsigned (*getReventType)(Event *self);
};

DECLARE_NEW_TYPED_CLASS(Event)

/* Internal functions */
void CallEventCallBackFinalize(Event *self);
bool TestEventNeedPolling(Event *self);
void SetEventType(Event *self, unsigned eventType);
unsigned GetEventType(Event *self);
void SetReventType(Event *self, unsigned reventType);
unsigned GetReventType(Event *self);

#ifdef ENABLE_UT
int GetTotalEventObjectCount(void);
void ResetTotalEventObjectCount(void);
#endif

GSDB_END_C_CODE_DECLS

#endif /* GSDB_EVENT_BASE_H */
