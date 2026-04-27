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
 * event_loop.h
 *
 * Description:
 * 1. event manager
 * 2. execute the callback function after the request is responded.
 * 3. resource Count Management Release
 *
 * -----------------------------------------------------------------------------------
 */

#ifndef GSDB_EVENT_LOOP_H
#define GSDB_EVENT_LOOP_H

#include "defines/common.h"
#include "defines/utils_errorcode.h"
#include "types/data_types.h"

GSDB_BEGIN_C_CODE_DECLS

typedef struct Event Event;
typedef struct EventLoop EventLoop;

#define EVENT_READ    0x01U
#define EVENT_WRITE   0x02U
#define EVENT_TIMEOUT 0x04U

#define INVALID_EVENT_FD (-2) /* -1 is timeout event */

typedef enum EventCbType {
    EV_CB_EXIT,     /* remove event object */
    EV_CB_CONTINUE, /* re-add event object */
} EventCbType;

/**
 * User event running callback function,determine whether to add the event again based the result.
 *
 * @param[in] self: current Event
 * @param[in] eventType: event type
 * @param[in] context: context required for a response is received
 */
typedef EventCbType (*OnEventRunFunc)(Event *self, unsigned eventType, void *context);

/**
 * Callback function for releasing and destroying user resources
 *
 * @param[in] self: current Event
 * @param[in] context: context required for a response is received
 */
typedef void (*OnEventFinalizeFunc)(Event *self, void *context);

/**
 * Return a new created EventLoop, a thread can be created only once until the loop is destroyed.
 *
 * @param[out] errorCode
 * @return EventLoop struct data
 */
EventLoop *CreateEventLoop(ErrorCode *errorCode);

/**
 * Start the loop and start processing the event.It can only be invoked by the thread that creates the loop.
 *
 * @param[in] self: EventLoop struct data
 * @return SUCCESS if the EventLoop is started up successfully, or an ErrorCode else.
 */
ErrorCode RunEventLoop(EventLoop *self);

/**
 * Quit the loop.The loop thread can be invoked both inside and outside the loop thread.
 *
 * @param[in] self: EventLoop struct data
 * @return SUCCESS if quit the loop successfully, or an ErrorCode else.
 */
ErrorCode QuitEventLoop(EventLoop *self);

/**
 * Destroy the loop.The loop thread can be invoked both inside and outside the loop thread.
 *
 * @param[in] self: EventLoop struct data
 */
void DestroyEventLoop(EventLoop *self);

/**
 * Increase refcount on EventLoop, usually used before create loop thread on target loop
 *
 * @param self:EventLoop struct data
 */
void IncEventLoopRef(EventLoop *self);

/**
 * Decrease refcount on EventLoop, usually used after loop thread exit(invoked in loop thread)
 *
 * @param self:EventLoop struct data
 */
void DecEventLoopRef(EventLoop *self);

/**
 * Create a listening Event.The loop thread can be invoked both inside and outside the loop thread.
 *
 * @param[in] self:current EventLoop
 * @param[in] fd: socket file descriptor
 * @param[in] eventType: event type
 * @param[in] timeoutInMsec: set the timeout, 0 means no timeout.
 * @param[out] errorCode:
 * @return a Event need to monitored
 */
Event *CreateEvent(EventLoop *self, int32_t fd, unsigned eventType, unsigned timeoutInMsec, ErrorCode *errorCode);

/**
 * Set timeout triggering event.The loop thread can be invoked both inside and outside the loop thread.
 *
 * @param[in] self: current EventLoop
 * @param[in] timeoutInMsec: set the timeout, 0 means no timeout.
 * @param[out] errorCode
 * @return a timeout event need to monitored
 */
Event *CreateTimerEvent(EventLoop *self, unsigned timeoutInMsec, ErrorCode *errorCode);

/**
 * User callback function executed after a request response is received or waiting timeout occurs
 *
 * @param[in] self: current event
 * @param[in] run: event callback for user callback function
 * @param[in] finalize: event callback for reclaim event resources
 * @param[in] context: context required for a response is received
 */
void SetEventCallbacks(Event *self, OnEventRunFunc run, OnEventFinalizeFunc finalize, void *context);

/**
 * Add Event type, The loop thread can be invoked both inside and outside the loop thread.
 *
 * @param[in] self: current event
 * @param[in] eventType: event type
 * @return SUCCESS if modify the event successfully, or an ErrorCode else.
 */
ErrorCode AddEventType(Event *self, unsigned eventType);

/**
 * Remove Event type, The loop thread can be invoked both inside and outside the loop thread.
 *
 * @param[in] self: current event
 * @param[in] eventType: event type
 * @return SUCCESS if modify the event successfully, or an ErrorCode else.
 */
ErrorCode RemoveEventType(Event *self, unsigned eventType);

/**
 * Reset event timeout, The loop thread can be invoked both inside and outside the loop thread.
 *
 * @param[in] self: current event
 * @param[in] timeoutInMsec: set the timeout, 0 means no timeout.
 */
void ResetEventTimeout(Event *self, unsigned timeoutInMsec);

/**
 * Get the event fd, if the event object is a timer object, -1 will be returned.
 *
 * @param[in] self: current event
 * @return fd, or -1 if self is a timer.
 */
int32_t GetEventFd(Event *self);

/**
 * Add the Event to the EventLoop, The loop thread can be invoked both inside and outside the loop thread.
 *
 * @param[in] self: the event which is need to add
 * @return  SUCCESS if add this event to the loop, or other result code if fail.
 */
ErrorCode AddEventToLoop(Event *self);

/**
 * Remove the Event from the EventLoop, The loop thread can be invoked both inside and outside the loop thread.
 *
 * @param[in] self: the Event which is need to remove
 * @return SUCCESS if remove this event from the loop successfully, or an ErrorCode else.
 */
ErrorCode RemoveEventFromLoop(Event *self);

/**
 * Destroy the event, The loop thread can be invoked both inside and outside the loop thread.
 *
 * @param[in] self: current EventLoop
 * @param[in] event: the Event which is need to destroy
 */
void DestroyEvent(Event *self);

/**
 * Check the event whether finish, that is to say remove from the event loop
 *
 * @param[in] event: the Event which is need to check
 */
bool IsEventFinish(Event *self);

GSDB_END_C_CODE_DECLS
#endif /* GSDB_EVENT_LOOP_H */
