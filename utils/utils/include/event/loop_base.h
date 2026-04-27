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
 * loop_base.h
 *
 * Description:
 * 1. Defining a Public Facility for an Event-Driven Model.
 *
 * -----------------------------------------------------------------------------------
 */

#ifndef GSDB_LOOP_BASE_H
#define GSDB_LOOP_BASE_H

#include "event/event_base.h"

GSDB_BEGIN_C_CODE_DECLS

#define EV_FLAGS_DISPATCHED     0x0100U
#define EV_FLAGS_DISABLED       0x0200U
#define EV_FLAGS_REMOVING       0x0400U
#define DEFAULT_MAX_EVENT_COUNT 8192
#define TIMEPOINT_NEVER_REACHED ((EventTime)-1)
#define DEFAULT_SLEEP_INTERVAL  1000 /* 1s */
#define TIMER_WHEEL_JIFFY       10   /* 10ms for 1 jiffy */

typedef struct EventCfgParameter EventCfgParameter;
struct EventCfgParameter {
    TypeInitParams super;
    EventLoop *creator;
    EventFd eventFd;
    unsigned eventType;
    unsigned timeoutInMsec;
};

typedef struct LoopCfgParameter LoopCfgParameter;
struct LoopCfgParameter {
    TypeInitParams super;
    unsigned maxEventCountLimit;
};

typedef enum {
    ACT_ADD_EVENT_OBJECT,
    ACT_REMOVE_EVENT_OBJECT,
    ACT_ADD_EVENT_TYPE,
    ACT_REMOVE_EVENT_TYPE,
    ACT_RESET_EVENT_TIMEOUT,
} ActionType;

typedef struct LoopAction LoopAction;
struct LoopAction {
    DListNode node;
    ActionType actionType;
    Event *evObj;
    unsigned eventType;
    unsigned timeoutInMsec;
};

typedef enum {
    READY = 0, /* The loop thread has never been started. */
    RUNNING,   /* The loop thread is running */
    QUITED,    /* The loop thread is stopped, but can be restarted */
    ZOMBIES,   /* The loop thread is stopped and cannot be started. */
} LoopState;

/*
 * Defining a Main Loop for an Event-Driven Model.
 */
struct EventLoop {
    RefObject super;
    SpinLock loopLock;
    DListHead actionQueue;
    DListHead attachedQueue;
    TimerWheel timerWheel;
    EventTime lastPushTimerWheelTime;
    DListHead activeEvents;
    DListHead timeoutEvents;
    LoopState state;
    unsigned actionCount;
    unsigned attachedCount;
    unsigned activeCount;
    unsigned timeoutCount;
    unsigned maxEventCount;
    bool canRestart;
    uint32_t quitCount;
    EventTime loopClock;
    Tid loopThread;
    Mutex quitedMutex;
    ConditionVariable quitedCondtion;
    bool hasWakeUpSignal;
};

typedef struct EventLoopOps EventLoopOps;
struct EventLoopOps {
    RefObjectOps super;
    ErrorCode (*createEventObject)(EventLoop *self, EventCfgParameter *cfg, Event **evObj);
    ErrorCode (*addPolling)(EventLoop *self, Event *evObj, unsigned eventType);
    void (*removePolling)(EventLoop *self, Event *evObj);
    void (*addPollingType)(EventLoop *self, Event *evObj, unsigned eventType);
    void (*removePollingType)(EventLoop *self, Event *evObj, unsigned eventType);
    ErrorCode (*prepare)(EventLoop *self, EventTime *interval);
    ErrorCode (*polling)(EventLoop *self, EventTime interval);
    void (*sendWakeupSignal)(EventLoop *self);
    void (*clearWakeupSignal)(EventLoop *self);
};

DECLARE_NEW_TYPED_CLASS(EventLoop)

/* Internal functions */
ErrorCode CreateEventObject(EventLoop *self, EventCfgParameter *cfg, Event **evObj);
ErrorCode QueueActionToLoop(EventLoop *self, LoopAction *action);
void SetEventObjectToActiveState(EventLoop *self, Event *evObj);

#ifdef ENABLE_UT
int GetTotalLoopCount(void);
void ResetTotalLoopCount(void);
#endif

GSDB_END_C_CODE_DECLS

#endif /* GSDB_LOOP_BASE_H */
