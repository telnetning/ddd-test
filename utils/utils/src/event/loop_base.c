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
 * loop_base.c
 *
 * Description:
 * 1. Basic framework of the loop mechanism.
 *
 * -----------------------------------------------------------------------------------
 */

#include <time.h>
#include "event/loop_base.h"

#ifdef LOCAL_MODULE_NAME
#undef LOCAL_MODULE_NAME
#endif
#define LOCAL_MODULE_NAME "event"

#define LOOP_CLEAR_SIGNAL_THRESHOLD 1000

#ifdef ENABLE_UT
Atomic32 g_totalLoopCount = 0;

int GetTotalLoopCount(void)
{
    return (int)GSDB_ATOMIC32_GET(&g_totalLoopCount);
}

void ResetTotalLoopCount(void)
{
    GSDB_ATOMIC32_SET(&g_totalLoopCount, 0);
}
#endif

static ErrorCode QuitEventLoopState(EventLoop *self, bool canRestart);

static ErrorCode EventLoopInit(EventLoop *self, TypeInitParams *initData)
{
    ASSERT(self != NULL);
    LoopCfgParameter *cfg = DOWN_TYPE_CAST(initData, LoopCfgParameter);
    if (cfg != NULL && cfg->maxEventCountLimit > 0) {
        self->maxEventCount = cfg->maxEventCountLimit;
    } else {
        self->maxEventCount = DEFAULT_MAX_EVENT_COUNT;
    }

    SpinLockInit(&self->loopLock);
    DListInit(&self->actionQueue);
    DListInit(&self->attachedQueue);
    InitTimerWheel(&self->timerWheel, TIMER_WHEEL_JIFFY);
    self->lastPushTimerWheelTime = 0;
    DListInit(&self->activeEvents);
    DListInit(&self->timeoutEvents);
    self->state = READY;
    self->actionCount = 0;
    self->attachedCount = 0;
    self->activeCount = 0;
    self->timeoutCount = 0;
    self->canRestart = true;
    self->quitCount = 0;
    self->loopClock = 0;
    self->loopThread = GetCurrentTid();
    MutexInit(&self->quitedMutex);
    ConditionVariableInit(&self->quitedCondtion);
    self->hasWakeUpSignal = false;

#ifdef ENABLE_UT
    GSDB_ATOMIC32_INC(&g_totalLoopCount);
#endif

    return ERROR_SYS_OK;
}

static void EventLoopFinalize(EventLoop *self)
{
    ASSERT(self != NULL);
    ConditionVariableDestroy(&self->quitedCondtion);
    MutexDestroy(&self->quitedMutex);
    ASSERT(self->timeoutCount == 0);
    ASSERT(self->activeCount == 0);
    ASSERT(self->attachedCount == 0);
    ASSERT(self->state == ZOMBIES || self->state == READY); /* Derived class init() failed will get READY state */
    ASSERT(DListIsEmpty(&self->timeoutEvents));
    ASSERT(DListIsEmpty(&self->activeEvents));
    ASSERT(DListIsEmpty(&self->attachedQueue));
    ASSERT(DListIsEmpty(&self->actionQueue));
    SpinLockDestroy(&self->loopLock);

#ifdef ENABLE_UT
    GSDB_ATOMIC32_DEC(&g_totalLoopCount);
#endif
}

static void EventLoopOnKill(RefObject *super)
{
    EventLoop *self = DOWN_TYPE_CAST(super, EventLoop);
    ASSERT(self != NULL);
    (void)QuitEventLoopState(self, false);
}

static ErrorCode CreateEventObjectDefault(SYMBOL_UNUSED EventLoop *self, SYMBOL_UNUSED EventCfgParameter *cfg,
                                          SYMBOL_UNUSED Event **evObj)
{
    /* DEFAULT EMPTY */
    return ERROR_UTILS_EVENT_VFUNC_NO_IMPL;
}

static ErrorCode AddEventObjectPollingDefault(SYMBOL_UNUSED EventLoop *self, SYMBOL_UNUSED Event *evObj,
                                              SYMBOL_UNUSED unsigned eventType)
{
    /* DEFAULT EMPTY */
    return ERROR_UTILS_EVENT_VFUNC_NO_IMPL;
}

static void RemoveEventObjectPollingDefault(SYMBOL_UNUSED EventLoop *self, SYMBOL_UNUSED Event *evObj)
{
    /* DEFAULT EMPTY */
}

static void AddEventPollingTypeDefault(SYMBOL_UNUSED EventLoop *self, SYMBOL_UNUSED Event *evObj,
                                       SYMBOL_UNUSED unsigned eventType)
{
    /* DEFAULT EMPTY */
}

static void RemoveEventPollingTypeDefault(SYMBOL_UNUSED EventLoop *self, SYMBOL_UNUSED Event *evObj,
                                          SYMBOL_UNUSED unsigned eventType)
{
    /* DEFAULT EMPTY */
}

static ErrorCode PrepareDefault(SYMBOL_UNUSED EventLoop *self, SYMBOL_UNUSED EventTime *interval)
{
    /* DEFAULT EMPTY */
    return ERROR_UTILS_EVENT_VFUNC_NO_IMPL;
}

static ErrorCode PollingDefault(SYMBOL_UNUSED EventLoop *self, SYMBOL_UNUSED EventTime interval)
{
    /* DEFAULT EMPTY */
    return ERROR_UTILS_EVENT_VFUNC_NO_IMPL;
}

static void SendWakeupSignalDefault(SYMBOL_UNUSED EventLoop *self)
{
    /* DEFAULT EMPTY */
}

static void ClearWakeupSignalDefault(SYMBOL_UNUSED EventLoop *self)
{
    /* DEFAULT EMPTY */
}

static void EventLoopOpsInit(EventLoopOps *self)
{
    GET_FOPS(RefObject)->kill = EventLoopOnKill;
    GET_FOPS(EventLoop)->createEventObject = CreateEventObjectDefault;
    GET_FOPS(EventLoop)->addPolling = AddEventObjectPollingDefault;
    GET_FOPS(EventLoop)->removePolling = RemoveEventObjectPollingDefault;
    GET_FOPS(EventLoop)->addPollingType = AddEventPollingTypeDefault;
    GET_FOPS(EventLoop)->removePollingType = RemoveEventPollingTypeDefault;
    GET_FOPS(EventLoop)->prepare = PrepareDefault;
    GET_FOPS(EventLoop)->polling = PollingDefault;
    GET_FOPS(EventLoop)->sendWakeupSignal = SendWakeupSignalDefault;
    GET_FOPS(EventLoop)->clearWakeupSignal = ClearWakeupSignalDefault;
}

DEFINE_NEW_TYPED_CLASS(EventLoop, RefObject)

static inline void LoopRefEventObject(Event *self)
{
    GSDB_ATOMIC32_INC(&self->loopRefs);
}

static inline void LoopUnRefEventObject(Event *self)
{
    GSDB_ATOMIC32_DEC(&self->loopRefs);
}

static bool SetEventObjectAttached(Event *self, EventLoop *loop)
{
    if (self->attached != NULL && loop != NULL) {
        return false;
    }
    if (loop != NULL) {
        LoopRefEventObject(self);
    } else {
        if (self->attached != NULL) {
            LoopUnRefEventObject(self);
        }
    }
    self->attached = loop;
    return true;
}

static inline bool TestEventObjectAttached(const Event *self, const EventLoop *loop)
{
    bool isAttached = self->attached == loop;
    return isAttached;
}

static inline void LockEventLoop(EventLoop *self)
{
    SpinLockAcquire(&self->loopLock);
}

static inline void UnLockEventLoop(EventLoop *self)
{
    SpinLockRelease(&self->loopLock);
}

static inline bool LoopIsToQuit(EventLoop *self)
{
    LockEventLoop(self);
    bool quit = self->quitCount > 0;
    UnLockEventLoop(self);
    return quit;
}

ErrorCode CreateEventObject(EventLoop *self, EventCfgParameter *cfg, Event **evObj)
{
    ASSERT(self != NULL);
    ASSERT(cfg != NULL);
    ASSERT(evObj != NULL);
    return GET_FAP(EventLoop)->createEventObject(self, cfg, evObj);
}

static inline bool IsRunningInLoopThread(EventLoop *self)
{
    LockEventLoop(self);
    if (self->state != RUNNING) {
        UnLockEventLoop(self);
        return false;
    }
    Tid curTid = GetCurrentTid();
    bool isLoopThread = TidIsEqual(&curTid, &self->loopThread);
    UnLockEventLoop(self);
    return isLoopThread;
}

static ErrorCode QueueActionInner(EventLoop *self, LoopAction *action)
{
    LockEventLoop(self);
    if (self->state == ZOMBIES) {
        UnLockEventLoop(self);
        ErrLog(WARNING, ErrMsg("Add event object action %d to a dead loop.", (int)action->actionType));
        return ERROR_UTILS_EVENT_INVALID_LOOP_STATE;
    }

    LoopRefEventObject(action->evObj);
    DListPushTail(&self->actionQueue, &action->node);
    ++self->actionCount;
    UnLockEventLoop(self);
    return ERROR_SYS_OK;
}

static inline ErrorCode QueueAction(EventLoop *self, LoopAction *action)
{
    RefObjectIncRef(UP_TYPE_CAST(action->evObj, RefObject));
    ErrorCode errCode = QueueActionInner(self, action);
    if (errCode != ERROR_SYS_OK) {
        RefObjectDecRef(UP_TYPE_CAST(action->evObj, RefObject));
    }
    return errCode;
}

static inline void GraftActionQueue(EventLoop *self, DListHead *to)
{
    LockEventLoop(self);
    DListPushHead(&self->actionQueue, &to->head);
    DListDelete(&self->actionQueue.head);
    DListInit(&self->actionQueue);
    self->actionCount = 0;
    UnLockEventLoop(self);
}

static void ClearLoopClockCache(EventLoop *self)
{
    ASSERT(self != NULL);
    self->loopClock = 0;
}

static EventTime GetSystemMonotonicMsec(void)
{
    static bool useRealTime = false;

    if (!useRealTime) {
        ErrorCode errCode = ERROR_SYS_OK;
        TimeValue timeNow = GetClockValue(CLOCKTYPE_MONOTONIC_RAW, &errCode);
        if (errCode == ERROR_SYS_OK) {
            return (EventTime)timeNow.seconds * MSEC_PER_SEC + (EventTime)timeNow.useconds / USEC_PER_MSEC;
        }
        ErrLog(WARNING, ErrMsg("Event loop GetClockValue() failed, choose to realtime API."));
        useRealTime = true;
    }
    TimeValue tv = GetCurrentTimeValue();
    return (EventTime)tv.seconds * MSEC_PER_SEC + (EventTime)tv.useconds / USEC_PER_MSEC;
}

static inline EventTime GetLoopClockNow(EventLoop *self)
{
    if (self->loopClock == 0) {
        self->loopClock = GetSystemMonotonicMsec();
    }
    return self->loopClock;
}

static void AddEventToTimerWheel(EventLoop *self, Event *evObj, bool flying)
{
    if (!flying) {
        ++self->attachedCount;
    }
    RefObjectIncRef(UP_TYPE_CAST(evObj, RefObject));
    AddEvent(&self->timerWheel, evObj);
}

static void PopEventFromTimerWheel(EventLoop *self, Event *evObj, bool flying)
{
    if (!flying) {
        --self->attachedCount;
    }
    PopEvent(&self->timerWheel, evObj);
    DListNodeInit(&evObj->attachedNode);
    RefObjectDecRef(UP_TYPE_CAST(evObj, RefObject));
}

static void ClearAllEventFromTimerWheel(EventLoop *self)
{
    DListHead eventList;
    DListInit(&eventList);
    GetAndClearAllEvent(&self->timerWheel, &eventList);
    while (!DListIsEmpty(&eventList)) {
        DListNode *curNode = DListPopHeadNode(&eventList);
        Event *evObj = DLIST_CONTAINER(Event, attachedNode, curNode);
        if (TestEventNeedPolling(evObj)) {
            GET_FAP(EventLoop)->removePolling(self, evObj);
        }
        (void)SetEventObjectAttached(evObj, NULL);
        RefObjectDecRef(UP_TYPE_CAST(evObj, RefObject));
    }
    self->attachedCount = 0;
}

static EventTime GetMinPollingInterval(EventLoop *self, bool needRealTime)
{
    if (needRealTime) {
        ClearLoopClockCache(self);
    }
    EventTime curTime = GetLoopClockNow(self);
    ASSERT(curTime >= self->lastPushTimerWheelTime);
    EventTime timeGap = curTime - self->lastPushTimerWheelTime;
    if (timeGap <= TIMER_WHEEL_JIFFY) {
        return TIMER_WHEEL_JIFFY - timeGap;
    }
    return 0;
}

static void ProcessLoopActionsAEO(EventLoop *self, Event *evObj)
{
    ASSERT(evObj != NULL);
    if (!SetEventObjectAttached(evObj, self)) {
        ErrLog(WARNING, ErrMsg("Add event object to loop twice"));
        return;
    }
    if (TestEventNeedPolling(evObj)) {
        ErrorCode errCode = GET_FAP(EventLoop)->addPolling(self, evObj, GetEventType(evObj));
        if (errCode != ERROR_SYS_OK) {
            (void)SetEventObjectAttached(evObj, NULL);
            ErrLog(WARNING, ErrMsg("Add event object to loop polling facility failed."));
            return;
        }
    }
    evObj->flags &= ~EV_FLAGS_DISABLED;
    AddEventToTimerWheel(self, evObj, false);
}

static void ProcessLoopActionsREO(EventLoop *self, Event *evObj)
{
    ASSERT(evObj != NULL);
    if (!TestEventObjectAttached(evObj, self)) {
        return;
    }
    if (TestEventNeedPolling(evObj)) {
        GET_FAP(EventLoop)->removePolling(self, evObj);
    }
    /* We cannot touch the activeEvents and timeoutEvents queues now, because this 'REO'
     * may occur when DispatchLoopActiveEvents() is running, that is, when other event
     * objects removed from the callback of an event object. We can only set a flag to
     * indicate this scenario, to tell DispatchLoopActiveEvents skip the removed event object.
     */
    evObj->flags |= EV_FLAGS_DISABLED;
    (void)SetEventObjectAttached(evObj, NULL);
    PopEventFromTimerWheel(self, evObj, false);
}

static void ProcessLoopActionsAET(EventLoop *self, Event *evObj, LoopAction *action)
{
    ASSERT(evObj != NULL);
    if (!TestEventObjectAttached(evObj, self)) {
        ErrLog(WARNING, ErrMsg("Add event type failed, it doesn't exist in loop."));
        return;
    }
    if (TestEventNeedPolling(evObj)) {
        GET_FAP(EventLoop)->addPollingType(self, evObj, action->eventType);
    }
    evObj->flags &= ~(action->eventType & EVENT_MASK);
}

static void ProcessLoopActionsRET(EventLoop *self, Event *evObj, LoopAction *action)
{
    ASSERT(evObj != NULL);
    if (!TestEventObjectAttached(evObj, self)) {
        ErrLog(WARNING, ErrMsg("Remove event type failed, it doesn't exist in loop."));
        return;
    }
    if (TestEventNeedPolling(evObj)) {
        GET_FAP(EventLoop)->removePollingType(self, evObj, action->eventType);
    }
    evObj->flags |= (action->eventType & EVENT_MASK);
}

static void ProcessLoopActionsETO(EventLoop *self, Event *evObj, LoopAction *action)
{
    ASSERT(evObj != NULL);
    if (!TestEventObjectAttached(evObj, self)) {
        ErrLog(WARNING, ErrMsg("Reset event timeout failed, it doesn't exist in loop."));
        return;
    }
    PopEventFromTimerWheel(self, evObj, false);
    evObj->timeout = action->timeoutInMsec;
    AddEventToTimerWheel(self, evObj, false);
}

static void ProcessLoopActions(EventLoop *self)
{
    DListHead grafted;
    DListInit(&grafted);
    GraftActionQueue(self, &grafted);
    ClearLoopClockCache(self);

    DListMutableIter iter;
    DLIST_MODIFY_FOR_EACH(iter, &grafted)
    {
        LoopAction *action = DLIST_CONTAINER(LoopAction, node, iter.cur);
        Event *evObj = action->evObj;
        switch (action->actionType) {
            case ACT_ADD_EVENT_OBJECT:
                ProcessLoopActionsAEO(self, evObj);
                break;
            case ACT_REMOVE_EVENT_OBJECT:
                ProcessLoopActionsREO(self, evObj);
                break;
            case ACT_ADD_EVENT_TYPE:
                ProcessLoopActionsAET(self, evObj, action);
                break;
            case ACT_REMOVE_EVENT_TYPE:
                ProcessLoopActionsRET(self, evObj, action);
                break;
            case ACT_RESET_EVENT_TIMEOUT:
                ProcessLoopActionsETO(self, evObj, action);
                break;
            default:
                break;
        }

        LoopUnRefEventObject(evObj);
        RefObjectDecRef(UP_TYPE_CAST(evObj, RefObject));
        DListDelete(iter.cur);
        free(action);
        self->hasWakeUpSignal = true;
    }
}

static void SendWakeupSignal(EventLoop *self)
{
    LockEventLoop(self);
    /* Only threads in the RUNNING state can be hibernated on the select/epoll/kqueue. */
    bool needToSignal = self->state == RUNNING;
    UnLockEventLoop(self);

    if (needToSignal) {
        GET_FAP(EventLoop)->sendWakeupSignal(self);
    }
}

static void ClearWakeupSignal(EventLoop *self)
{
    ASSERT(self != NULL);
    GET_FAP(EventLoop)->clearWakeupSignal(self);
    self->hasWakeUpSignal = false;
}

ErrorCode QueueActionToLoop(EventLoop *self, LoopAction *action)
{
    ASSERT(self != NULL);
    ASSERT(action != NULL);
    ASSERT(action->evObj != NULL);
    ErrorCode errCode = QueueAction(self, action);
    if (errCode != ERROR_SYS_OK) {
        return errCode;
    }
    if (IsRunningInLoopThread(self)) {
        ProcessLoopActions(self);
    } else {
        SendWakeupSignal(self);
    }
    return ERROR_SYS_OK;
}

void SetEventObjectToActiveState(EventLoop *self, Event *evObj)
{
    ASSERT(self != NULL);
    ASSERT(evObj != NULL);
    evObj->flags &= ~EV_FLAGS_DISPATCHED;
    DListPushTail(&self->activeEvents, &evObj->activeNode);
    ++self->activeCount;
    RefObjectIncRef(UP_TYPE_CAST(evObj, RefObject));
}

static inline void RemoveFromActiveQueue(EventLoop *self, Event *evObj)
{
    DListDelete(&evObj->activeNode);
    DListNodeInit(&evObj->activeNode);
    --self->activeCount;
    RefObjectDecRef(UP_TYPE_CAST(evObj, RefObject));
}

static inline void AddToTimeoutQueue(EventLoop *self, Event *evObj)
{
    evObj->flags &= ~EV_FLAGS_DISPATCHED;
    DListPushTail(&self->timeoutEvents, &evObj->timeoutNode);
    ++self->timeoutCount;
    RefObjectIncRef(UP_TYPE_CAST(evObj, RefObject));
}

static inline void RemoveFromTimeoutQueue(EventLoop *self, Event *evObj)
{
    DListDelete(&evObj->timeoutNode);
    DListNodeInit(&evObj->timeoutNode);
    --self->timeoutCount;
    RefObjectDecRef(UP_TYPE_CAST(evObj, RefObject));
}

static uint64_t GetJiffyCountFromLastPushWheel(EventLoop *self)
{
    EventTime curTime = GetLoopClockNow(self);
    ASSERT(curTime >= self->lastPushTimerWheelTime);
    EventTime timeGap = curTime - self->lastPushTimerWheelTime;
    if (timeGap < TIMER_WHEEL_JIFFY) {
        return 0;
    }
    uint64_t jiffyCount = timeGap / TIMER_WHEEL_JIFFY;
    self->lastPushTimerWheelTime = self->lastPushTimerWheelTime + jiffyCount * TIMER_WHEEL_JIFFY;
    return jiffyCount;
}

static void GatherTimeoutEvents(EventLoop *self)
{
    ClearLoopClockCache(self);
    uint64_t jiffyCount = GetJiffyCountFromLastPushWheel(self);
    if (jiffyCount > 0) {
        DListHead curEventList;
        DListInit(&curEventList);
        GetMultipleJiffyEventList(&self->timerWheel, jiffyCount, &curEventList);
        while (!DListIsEmpty(&curEventList)) {
            DListNode *curNode = DListPopHeadNode(&curEventList);
            Event *evObj = DLIST_CONTAINER(Event, attachedNode, curNode);
            AddToTimeoutQueue(self, evObj);
            RefObjectDecRef(UP_TYPE_CAST(evObj, RefObject));
        }
    }

    DListIter iter;
    DLIST_FOR_EACH(iter, &self->timeoutEvents)
    {
        Event *evObj = DLIST_CONTAINER(Event, timeoutNode, iter.cur);
        AddEventToTimerWheel(self, evObj, true);
    }
}

static inline void InvokeCallbackRun(const EventLoop *self, Event *evObj, unsigned revents)
{
    EventCallbacks *cb = &evObj->callbacks;
    if (self->canRestart && cb->run) {
        EventCbType ret = (*cb->run)(evObj, revents, cb->context);
        if (ret == EV_CB_EXIT) {
            (void)RemoveEventFromLoop(evObj);
        }
    }
}

static void DispatchLoopActiveEvents(EventLoop *self)
{
    if (DListIsEmpty(&self->activeEvents)) {
        return;
    }

    DListMutableIter iter;
    DLIST_MODIFY_FOR_EACH(iter, &self->activeEvents)
    {
        Event *evObj = DLIST_CONTAINER(Event, activeNode, iter.cur);
        if ((evObj->flags & EV_FLAGS_DISABLED) == 0) {
            unsigned revents = GetReventType(evObj);
            revents &= ~(evObj->flags & EVENT_MASK);
            if (revents != 0) {
                evObj->flags |= EV_FLAGS_DISPATCHED;
                InvokeCallbackRun(self, evObj, revents);
                SetReventType(evObj, 0);
            }
        }
        RemoveFromActiveQueue(self, evObj);
    }
}

static void DispatchLoopTimeoutEvents(EventLoop *self)
{
    if (DListIsEmpty(&self->timeoutEvents)) {
        return;
    }

    DListMutableIter iter;
    DLIST_MODIFY_FOR_EACH(iter, &self->timeoutEvents)
    {
        Event *evObj = DLIST_CONTAINER(Event, timeoutNode, iter.cur);
        if ((evObj->flags & (EV_FLAGS_DISABLED | EV_FLAGS_DISPATCHED)) == 0) {
            InvokeCallbackRun(self, evObj, EVENT_TIMEOUT);
        }
        RemoveFromTimeoutQueue(self, evObj);
    }
}

static inline void DispatchReadyEvents(EventLoop *self)
{
    DispatchLoopActiveEvents(self);
    DispatchLoopTimeoutEvents(self);
}

static ErrorCode BeginToRunLoopObject(EventLoop *self, bool *needQuit)
{
    LockEventLoop(self);
    if (self->state != READY && self->state != QUITED) {
        UnLockEventLoop(self);
        ErrLog(WARNING, ErrMsg("Run loop failed because it's not int ready/quited state."));
        return ERROR_UTILS_EVENT_INVALID_LOOP_STATE;
    }
    if (self->quitCount > 0) {
        self->quitCount -= 1;
        *needQuit = true;
        UnLockEventLoop(self);
        return ERROR_SYS_OK;
    }
    *needQuit = false;
    self->state = RUNNING;
    self->loopThread = GetCurrentTid();
    ClearLoopClockCache(self);
    self->lastPushTimerWheelTime = GetLoopClockNow(self);
    RefObjectIncRef(UP_TYPE_CAST(self, RefObject));
    UnLockEventLoop(self);
    return ERROR_SYS_OK;
}

static inline void WaitEventLoopToQuit(EventLoop *self)
{
    MutexLock(&self->quitedMutex);
    while (self->state == RUNNING) {
        GET_FAP(EventLoop)->sendWakeupSignal(self);
        ConditionVariableWait(&self->quitedCondtion, &self->quitedMutex);
    }
    MutexUnlock(&self->quitedMutex);
}

static inline void NotifyLoopQuitingWaiter(EventLoop *self)
{
    MutexLock(&self->quitedMutex);
    if (self->state == QUITED || self->state == ZOMBIES) {
        ConditionVariableSignal(&self->quitedCondtion);
    }
    MutexUnlock(&self->quitedMutex);
}

static void EndToRunLoopObject(EventLoop *self, ErrorCode errCode)
{
    ASSERT(DListIsEmpty(&self->activeEvents));
    ASSERT(DListIsEmpty(&self->timeoutEvents));

    bool needClearResources = false;
    LockEventLoop(self); /* already in RUNNING state */
    if (errCode != ERROR_SYS_OK) {
        self->canRestart = false;
    }
    if (self->canRestart) {
        self->state = QUITED;
    } else {
        self->state = ZOMBIES;
        needClearResources = true;
    }
    self->quitCount = 0;
    UnLockEventLoop(self);
    if (needClearResources) {
        ProcessLoopActions(self);
        ClearAllEventFromTimerWheel(self);
    }
    NotifyLoopQuitingWaiter(self);
    RefObjectDecRef(UP_TYPE_CAST(self, RefObject));
}

UTILS_EXPORT ErrorCode RunEventLoop(EventLoop *self)
{
    if (self == NULL) {
        ErrLog(WARNING, ErrMsg("Try to run a null loop."));
        return ERROR_UTILS_EVENT_INVALID_ARGS;
    }
    bool needQuit = false;
    ErrorCode errCode = BeginToRunLoopObject(self, &needQuit);
    if (errCode != ERROR_SYS_OK) {
        return errCode;
    }
    if (needQuit) {
        return ERROR_SYS_OK;
    }
    bool needRealTime = true;
    uint32_t loopRunCount = 0;
    while (!LoopIsToQuit(self)) {
        ++loopRunCount;
        ProcessLoopActions(self);
        EventTime interval = GetMinPollingInterval(self, needRealTime);
        /* After first call, using time in GatherTimeoutEvents() to calculate polling time */

        interval = interval > 5 ? (interval >> 1) : interval; // 5, implent the best case for precision is 5ms

        needRealTime = false;
        errCode = GET_FAP(EventLoop)->prepare(self, &interval);
        if (errCode != ERROR_SYS_OK) {
            ErrLog(ERROR, ErrMsg("Loop prepare failed."));
            break;
        }
        errCode = GET_FAP(EventLoop)->polling(self, interval);
        ProcessLoopActions(self);
        GatherTimeoutEvents(self);
        DispatchReadyEvents(self);
        if (errCode != ERROR_SYS_OK) {
            ErrLog(ERROR, ErrMsg("Loop polling failed."));
            break;
        }
        if (self->hasWakeUpSignal || loopRunCount % LOOP_CLEAR_SIGNAL_THRESHOLD == 0) {
            ClearWakeupSignal(self);
        }
    }
    EndToRunLoopObject(self, errCode);
    return errCode;
}

static ErrorCode QuitEventLoopState(EventLoop *self, bool canRestart)
{
    bool inRunningThread = IsRunningInLoopThread(self);
    LockEventLoop(self);
    self->quitCount += 1;
    switch (self->state) {
        case READY:
            if (!canRestart) {
                self->state = ZOMBIES;
                self->canRestart = false;
                UnLockEventLoop(self);
                ProcessLoopActions(self);
                ClearAllEventFromTimerWheel(self);
            } else {
                self->state = QUITED;
                UnLockEventLoop(self);
            }
            return ERROR_SYS_OK;

        case RUNNING:
            if (self->canRestart) {
                self->canRestart = canRestart;
            }
            UnLockEventLoop(self);
            if (!inRunningThread) {
                WaitEventLoopToQuit(self);
            }
            return ERROR_SYS_OK;

        case QUITED:
            if (self->canRestart && !canRestart) {
                self->state = ZOMBIES;
                self->canRestart = false;
                UnLockEventLoop(self);
                ProcessLoopActions(self);
                ClearAllEventFromTimerWheel(self);
                return ERROR_SYS_OK;
            }
            UnLockEventLoop(self);
            return ERROR_SYS_OK;

        case ZOMBIES:
            UnLockEventLoop(self);
            return ERROR_SYS_OK;

        default:
            UnLockEventLoop(self);
            return ERROR_SYS_OK;
    }
}

UTILS_EXPORT ErrorCode QuitEventLoop(EventLoop *self)
{
    if (self == NULL) {
        ErrLog(WARNING, ErrMsg("Try to quit a null loop."));
        return ERROR_UTILS_EVENT_INVALID_ARGS;
    }
    return QuitEventLoopState(self, true);
}

UTILS_EXPORT void DestroyEventLoop(EventLoop *self)
{
    if (self != NULL) {
        RefObjectKillAndDecRef(UP_TYPE_CAST(self, RefObject));
    }
}

UTILS_EXPORT void IncEventLoopRef(EventLoop *self)
{
    if (self != NULL) {
        RefObjectIncRef(UP_TYPE_CAST(self, RefObject));
    }
}

UTILS_EXPORT void DecEventLoopRef(EventLoop *self)
{
    if (self != NULL) {
        RefObjectDecRef(UP_TYPE_CAST(self, RefObject));
    }
}
