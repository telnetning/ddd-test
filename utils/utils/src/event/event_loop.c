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
 * event_loop.c
 *
 * Description:
 * Implements some external interfaces.
 *
 * -----------------------------------------------------------------------------------
 */

#include "event/loop_epoll.h"

#ifdef LOCAL_MODULE_NAME
#undef LOCAL_MODULE_NAME
#endif
#define LOCAL_MODULE_NAME "event"

/**
 * Return a new created EventLoop, a thread can be created only once until the loop is destroyed.
 *
 * @param[out] errorCode
 * @return EventLoop struct data
 */
UTILS_EXPORT EventLoop *CreateEventLoop(ErrorCode *errorCode)
{
    LoopCfgParameter cfg;
    cfg.maxEventCountLimit = 0;
    EpollLoop *epollLoop = NewEpollLoop(CONSTRUCTOR_PARAM(&cfg), errorCode);
    if (epollLoop == NULL) {
        ErrLog(WARNING, ErrMsg("Create event loop failed."));
    }
    return UP_TYPE_CAST(epollLoop, EventLoop);
}

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
UTILS_EXPORT Event *CreateEvent(EventLoop *self, int32_t fd, unsigned eventType, unsigned timeoutInMsec,
                                ErrorCode *errorCode)
{
    if (self == NULL) {
        ErrLog(WARNING, ErrMsg("Create io event failed, no parent loop assigned."));
        if (errorCode) {
            *errorCode = ERROR_UTILS_EVENT_INVALID_ARGS;
        }
        return NULL;
    }

    EventCfgParameter cfg;
    cfg.creator = self;
    cfg.eventFd = (EventFd)fd;
    cfg.eventType = eventType;
    cfg.timeoutInMsec = timeoutInMsec;

    Event *evObj = NULL;
    ErrorCode errCode = CreateEventObject(self, &cfg, &evObj);
    if (errorCode) {
        *errorCode = errCode;
    }
    if (errCode == ERROR_SYS_OK) {
        return evObj;
    } else {
        ErrLog(WARNING, ErrMsg("Create io event failed."));
        return NULL;
    }
}

/**
 * Set timeout triggering event.The loop thread can be invoked both inside and outside the loop thread.
 *
 * @param[in] self: current EventLoop
 * @param[in] timeoutInMsec: set the timeout, 0 means no timeout.
 * @param[out] errorCode
 * @return a timeout event need to monitored
 */
UTILS_EXPORT Event *CreateTimerEvent(EventLoop *self, unsigned timeoutInMsec, ErrorCode *errorCode)
{
    if (self == NULL) {
        ErrLog(WARNING, ErrMsg("Create timer event failed, no parent loop assigned."));
        if (errorCode) {
            *errorCode = ERROR_UTILS_EVENT_INVALID_ARGS;
        }
        return NULL;
    }

    EventCfgParameter cfg;
    cfg.creator = self;
    cfg.eventFd = (EventFd)-1;
    cfg.eventType = 0;
    cfg.timeoutInMsec = timeoutInMsec;

    Event *evObj = NULL;
    ErrorCode errCode = CreateEventObject(self, &cfg, &evObj);
    if (errorCode) {
        *errorCode = errCode;
    }
    if (errCode == ERROR_SYS_OK) {
        return evObj;
    } else {
        ErrLog(WARNING, ErrMsg("Create timer event failed."));
        return NULL;
    }
}
