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
 * event_base.c
 *
 * Description:
 * 1. implementation of event base api
 *
 * -----------------------------------------------------------------------------------
 */

#include <pthread.h>
#include "memory/memory_allocator.h"
#include "event/loop_base.h"
#include "fault_injection/fault_injection.h"

#ifdef LOCAL_MODULE_NAME
#undef LOCAL_MODULE_NAME
#endif
#define LOCAL_MODULE_NAME "event"

#ifdef ENABLE_UT
Atomic32 g_totalEventObjectCount = 0;

int GetTotalEventObjectCount(void)
{
    return (int)GSDB_ATOMIC32_GET(&g_totalEventObjectCount);
}

void ResetTotalEventObjectCount(void)
{
    GSDB_ATOMIC32_SET(&g_totalEventObjectCount, 0);
}
#endif

static inline void InitEventCallBacks(EventCallbacks *callbacks)
{
    ASSERT(callbacks != NULL);
    callbacks->context = NULL;
    callbacks->run = NULL;
    callbacks->finalize = NULL;
}

void CallEventCallBackFinalize(Event *self)
{
    ASSERT(self != NULL);
    EventCallbacks *callbacks = &self->callbacks;
    if (callbacks->finalize) {
        (*callbacks->finalize)(self, callbacks->context);
    }
}

static ErrorCode EventInit(Event *self, TypeInitParams *initData)
{
    ASSERT(self != NULL);
    EventCfgParameter *cfg = DOWN_TYPE_CAST(initData, EventCfgParameter);

    if (cfg == NULL || cfg->creator == NULL) {
        ErrLog(WARNING, ErrMsg("Init event object error, null cfg args."));
        return ERROR_UTILS_EVENT_INVALID_ARGS;
    }

    self->flags = 0;
    self->timeout = cfg->timeoutInMsec;
    self->timeoutPoint = TIMEPOINT_NEVER_REACHED;
    GSDB_ATOMIC32_SET(&self->loopRefs, 0);
    DListNodeInit(&self->attachedNode);
    DListNodeInit(&self->activeNode);
    DListNodeInit(&self->timeoutNode);
    self->bornFrom = cfg->creator;
    self->attached = NULL;
    InitEventCallBacks(&self->callbacks);
    RefObjectIncRef(UP_TYPE_CAST(self->bornFrom, RefObject));

#ifdef ENABLE_UT
    GSDB_ATOMIC32_INC(&g_totalEventObjectCount);
#endif

    return ERROR_SYS_OK;
}

static void EventFinalize(Event *self)
{
    ASSERT(self != NULL);
    ASSERT(self->attached == NULL);
    ASSERT(GSDB_ATOMIC32_GET(&self->loopRefs) == 0);
    if (self->bornFrom != NULL) {
        RefObjectDecRef(UP_TYPE_CAST(self->bornFrom, RefObject));
        self->bornFrom = NULL;
    }

#ifdef ENABLE_UT
    GSDB_ATOMIC32_DEC(&g_totalEventObjectCount);
#endif
}

static void EventOnKill(RefObject *super)
{
    ASSERT(super != NULL);
    Event *self = DOWN_TYPE_CAST(super, Event);
    (void)RemoveEventFromLoop(self);
}

static bool EventNeedPolling(SYMBOL_UNUSED Event *self)
{
    ASSERT(self != NULL);
    return false;
}

static EventFd EventGetEventFd(SYMBOL_UNUSED Event *self)
{
    ASSERT(self != NULL);
    return -1;
}

static void EventSetEventType(SYMBOL_UNUSED Event *self, SYMBOL_UNUSED unsigned eventType)
{
    ASSERT(self != NULL);
}

static unsigned EventGetEventType(SYMBOL_UNUSED Event *self)
{
    ASSERT(self != NULL);
    return 0;
}

static void EventSetReventType(SYMBOL_UNUSED Event *self, SYMBOL_UNUSED unsigned reventType)
{
    ASSERT(self != NULL);
}

static unsigned EventgetReventType(SYMBOL_UNUSED Event *self)
{
    ASSERT(self != NULL);
    return 0;
}

static void EventOpsInit(EventOps *self)
{
    GET_FOPS(RefObject)->kill = EventOnKill;
    GET_FOPS(Event)->needPolling = EventNeedPolling;
    GET_FOPS(Event)->getEventFd = EventGetEventFd;
    GET_FOPS(Event)->setEventType = EventSetEventType;
    GET_FOPS(Event)->getEventType = EventGetEventType;
    GET_FOPS(Event)->setReventType = EventSetReventType;
    GET_FOPS(Event)->getReventType = EventgetReventType;
}

DEFINE_NEW_TYPED_CLASS(Event, RefObject)

UTILS_EXPORT void SetEventCallbacks(Event *self, OnEventRunFunc run, OnEventFinalizeFunc finalize, void *context)
{
    if (self == NULL) {
        ErrLog(WARNING, ErrMsg("Set event callbacks failed, null ev pointer."));
        return;
    }
    EventCallbacks *cb = &self->callbacks;
    cb->context = context;
    cb->run = run;
    cb->finalize = finalize;
}

static inline bool TestIfEventAddedToLoop(const Event *self)
{
    ASSERT(self != NULL);
    if (GSDB_ATOMIC32_GET(&self->loopRefs) != 0) {
        return true;
    }
    return false;
}

static inline bool AddEventTypeDirectly(Event *self, unsigned eventType)
{
    ASSERT(self != NULL);
    if (!TestIfEventAddedToLoop(self)) {
        SetEventType(self, eventType);
        return true;
    }
    return false;
}

static inline bool RemoveEventTypeDirectly(Event *self, unsigned eventType)
{
    ASSERT(self != NULL);
    if (!TestIfEventAddedToLoop(self)) {
        unsigned type = GetEventType(self);
        type &= ~eventType;
        SetEventType(self, type);
        return true;
    }
    return false;
}

static inline bool ResetEventTimeoutDirectly(Event *self, unsigned timeoutInMsec)
{
    ASSERT(self != NULL);
    if (!TestIfEventAddedToLoop(self)) {
        self->timeout = timeoutInMsec;
        return true;
    }
    return false;
}

static inline bool RemoveEventObjectDirectly(Event *self)
{
    ASSERT(self != NULL);
    if (!TestIfEventAddedToLoop(self)) {
        return true;
    }
    return false;
}

static ErrorCode SendActionToLoop(EventLoop *loop, Event *evObj, ActionType actionType, unsigned eventsType,
                                  unsigned timeoutMsec)
{
    ASSERT(loop != NULL);
    ASSERT(evObj != NULL);
    LoopAction *act = malloc(sizeof(LoopAction));
    if (act == NULL) {
        ErrLog(WARNING, ErrMsg("Send action to loop failed, system out of memory."));
        return ERROR_UTILS_EVENT_OUT_OF_MEMORY;
    }
    DListNodeInit(&act->node);
    act->actionType = actionType;
    act->evObj = evObj;
    act->eventType = eventsType;
    act->timeoutInMsec = timeoutMsec;
    ErrorCode errCode = QueueActionToLoop(loop, act);
    if (errCode != ERROR_SYS_OK) {
        free(act);
        ErrLog(WARNING, ErrMsg("Queue action failed when send action to loop."));
        return errCode;
    }
    return ERROR_SYS_OK;
}

UTILS_EXPORT ErrorCode AddEventType(Event *self, unsigned eventType)
{
    if (self == NULL) {
        ErrLog(WARNING, ErrMsg("Add event type failed, null ev pointer."));
        return ERROR_UTILS_EVENT_INVALID_ARGS;
    }
    if ((eventType & ~EVENT_MASK) != 0) {
        ErrLog(WARNING, ErrMsg("Add event type failed, unknown type value."));
        return ERROR_UTILS_EVENT_INVALID_ARGS;
    }
    if (AddEventTypeDirectly(self, eventType)) {
        return ERROR_SYS_OK;
    }
    return SendActionToLoop(self->bornFrom, self, ACT_ADD_EVENT_TYPE, eventType, 0);
}

UTILS_EXPORT ErrorCode RemoveEventType(Event *self, unsigned eventType)
{
    if (self == NULL) {
        ErrLog(WARNING, ErrMsg("Remove event type failed, null ev pointer."));
        return ERROR_UTILS_EVENT_INVALID_ARGS;
    }
    if ((eventType & ~EVENT_MASK) != 0) {
        ErrLog(WARNING, ErrMsg("Remove event type failed, unknown type value."));
        return ERROR_UTILS_EVENT_INVALID_ARGS;
    }
    if (RemoveEventTypeDirectly(self, eventType)) {
        return ERROR_SYS_OK;
    }
    FAULT_INJECTION_CALL(UT_EVENT_REMOVE_TYPE_DELAY_2, NULL);
    return SendActionToLoop(self->bornFrom, self, ACT_REMOVE_EVENT_TYPE, eventType, 0);
}

UTILS_EXPORT void ResetEventTimeout(Event *self, unsigned timeoutInMsec)
{
    if (self == NULL) {
        ErrLog(WARNING, ErrMsg("Reset event timeout failed, null ev pointer."));
        return;
    }
    if (ResetEventTimeoutDirectly(self, timeoutInMsec)) {
        return;
    }
    (void)SendActionToLoop(self->bornFrom, self, ACT_RESET_EVENT_TIMEOUT, 0, timeoutInMsec);
}

UTILS_EXPORT EventFd GetEventFd(Event *self)
{
    if (self == NULL) {
        ErrLog(WARNING, ErrMsg("Get event fd failed, null ev pointer."));
        return INVALID_EVENT_FD;
    }
    return GET_FAP(Event)->getEventFd(self);
}

UTILS_EXPORT ErrorCode AddEventToLoop(Event *self)
{
    if (self == NULL) {
        ErrLog(WARNING, ErrMsg("Add event to loop failed, null ev pointer."));
        return ERROR_UTILS_EVENT_INVALID_ARGS;
    }
    return SendActionToLoop(self->bornFrom, self, ACT_ADD_EVENT_OBJECT, 0, 0);
}

UTILS_EXPORT ErrorCode RemoveEventFromLoop(Event *self)
{
    if (self == NULL) {
        ErrLog(WARNING, ErrMsg("Remove event from loop failed, null ev pointer."));
        return ERROR_UTILS_EVENT_INVALID_ARGS;
    }
    if (RemoveEventObjectDirectly(self)) {
        return ERROR_SYS_OK;
    }
    return SendActionToLoop(self->bornFrom, self, ACT_REMOVE_EVENT_OBJECT, 0, 0);
}

UTILS_EXPORT void DestroyEvent(Event *self)
{
    if (self == NULL) {
        ErrLog(WARNING, ErrMsg("Destroy event failed, null ev pointer."));
        return;
    }
    RefObjectKillAndDecRef(UP_TYPE_CAST(self, RefObject));
}

UTILS_EXPORT bool IsEventFinish(Event *self)
{
    if (self == NULL) {
        return true;
    }
    return !TestIfEventAddedToLoop(self);
}

/* Internal functions */

bool TestEventNeedPolling(Event *self)
{
    ASSERT(self != NULL);
    return GET_FAP(Event)->needPolling(self);
}

void SetEventType(Event *self, unsigned eventType)
{
    ASSERT(self != NULL);
    GET_FAP(Event)->setEventType(self, eventType);
}

unsigned GetEventType(Event *self)
{
    ASSERT(self != NULL);
    return GET_FAP(Event)->getEventType(self);
}

void SetReventType(Event *self, unsigned reventType)
{
    ASSERT(self != NULL);
    GET_FAP(Event)->setReventType(self, reventType);
}

unsigned GetReventType(Event *self)
{
    ASSERT(self != NULL);
    return GET_FAP(Event)->getReventType(self);
}