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
 * loop_epoll.c
 *
 * Description:
 * Use epoll to implement a loop on the Linux platform.
 *
 * -----------------------------------------------------------------------------------
 */

#include <fcntl.h>
#include <unistd.h>
#include <sys/eventfd.h>
#include "event/event_io.h"
#include "event/event_timer.h"
#include "event/loop_epoll.h"

#ifdef LOCAL_MODULE_NAME
#undef LOCAL_MODULE_NAME
#endif
#define LOCAL_MODULE_NAME "event"

const int READ_PIPE = 0;
const int WRITE_PIPE = 1;

static inline void CheckClose(SYMBOL_UNUSED const EpollLoop *self, int fd)
{
    int ret = close(fd);
    if (ret == 0) {
        ErrLog(INFO, ErrMsg("Epoll loop close fd: %d.", fd));
    } else {
        ErrLog(ERROR, ErrMsg("Epoll loop close fd: %d failed, err(%s).", fd, strerror(errno)));
    }
}

static ErrorCode EpollLoopInit(EpollLoop *self, SYMBOL_UNUSED TypeInitParams *initData)
{
    ASSERT(self != NULL);
    struct epoll_event event;

    self->epollFd = epoll_create1(EPOLL_CLOEXEC);
    if (self->epollFd < 0) {
        ErrLog(ERROR, ErrMsg("Epoll loop invoke epoll_create1() failed, err(%s).", strerror(errno)));
        return ERROR_UTILS_EVENT_CREATE_EPOLL_FAILED;
    }

    if (pipe2(self->pipeFd, O_CLOEXEC | O_NONBLOCK) != 0) {
        ErrLog(ERROR, ErrMsg("Epoll loop Create pipe failed, err(%s)", strerror(errno)));
        CheckClose(self, self->epollFd);
        return ERROR_UTILS_EVENT_POLL_CREATE_PIPE_FAILED;
    }

    event.events = EPOLLIN;
    event.data.ptr = self;
    int ret = epoll_ctl(self->epollFd, EPOLL_CTL_ADD, self->pipeFd[READ_PIPE], &event);
    if (ret < 0) {
        ErrLog(ERROR, ErrMsg("Epoll loop add wakeup fd to polling failed, err(%s).", strerror(errno)));
        CheckClose(self, self->epollFd);
        CheckClose(self, self->pipeFd[READ_PIPE]);
        CheckClose(self, self->pipeFd[WRITE_PIPE]);
        return ERROR_UTILS_EVENT_ADD_WAKUPFD_FAILED;
    }

    self->pollFdCount = 1;
    self->reventsCapacity = EPOLL_INIT_REVENTS;
    self->revents = malloc((Size)sizeof(struct epoll_event) * self->reventsCapacity);
    if (self->revents == NULL) {
        ErrLog(ERROR, ErrMsg("Epoll loop alloc revents array failed, system out of memory."));
        CheckClose(self, self->epollFd);
        CheckClose(self, self->pipeFd[READ_PIPE]);
        CheckClose(self, self->pipeFd[WRITE_PIPE]);
        return ERROR_UTILS_EVENT_OUT_OF_MEMORY;
    }

    ErrLog(INFO, ErrMsg("Epoll loop initialized ok."));
    return ERROR_SYS_OK;
}

static void EpollLoopFinalize(EpollLoop *self)
{
    ASSERT(self != NULL);
    free(self->revents);
    ASSERT(self->pollFdCount == 1);
    self->revents = NULL;
    CheckClose(self, self->pipeFd[READ_PIPE]);
    CheckClose(self, self->pipeFd[WRITE_PIPE]);
    CheckClose(self, self->epollFd);
    ErrLog(INFO, ErrMsg("Epoll loop finalized."));
}

static ErrorCode EpollLoopCreateEventObject(EventLoop *super, EventCfgParameter *cfg, Event **evObj)
{
    ASSERT(super != NULL);
    ASSERT(cfg != NULL);
    ASSERT(evObj != NULL);

    ErrorCode errCode = ERROR_SYS_OK;
    cfg->creator = super;
    if (cfg->eventFd < 0) {
        TimerEvent *timerEvent = NewTimerEvent(CONSTRUCTOR_PARAM(cfg), &errCode);
        if (timerEvent == NULL) {
            return errCode;
        }
        *evObj = UP_TYPE_CAST(timerEvent, Event);
        return ERROR_SYS_OK;
    } else {
        IOEvent *ioEvent = NewIOEvent(CONSTRUCTOR_PARAM(cfg), &errCode);
        if (ioEvent == NULL) {
            return errCode;
        }
        *evObj = UP_TYPE_CAST(ioEvent, Event);
        return ERROR_SYS_OK;
    }
}

static ErrorCode ReserveReventsSpace(EpollLoop *super)
{
    ASSERT(super != NULL);
    EpollLoop *self = DOWN_TYPE_CAST(super, EpollLoop);

    if (self->reventsCapacity > self->pollFdCount) {
        return ERROR_SYS_OK;
    }

    unsigned newCapacity = self->reventsCapacity * 2;
    if (newCapacity > MAX_EPOLL_FD_COUNT) {
        newCapacity = MAX_EPOLL_FD_COUNT;
    }
    if (self->pollFdCount + 1 > newCapacity) {
        ErrLog(ERROR, ErrMsg("Epoll loop fd count reach max limit: %d.", MAX_EPOLL_FD_COUNT));
        return ERROR_UTILS_EVENT_POLL_FDS_REACH_MAX_LIMIT;
    }

    struct epoll_event *newArr = malloc((Size)newCapacity * sizeof(struct epoll_event));
    if (newArr == NULL) {
        ErrLog(ERROR, ErrMsg("Epoll loop revents array[%u] failed, system out of memory.", newCapacity));
        return ERROR_UTILS_EVENT_OUT_OF_MEMORY;
    }

    free(self->revents);
    self->revents = newArr;
    self->reventsCapacity = newCapacity;
    return ERROR_SYS_OK;
}

static inline uint32_t MapEvTypesToEpollTypeValue(unsigned eventType)
{
    uint32_t events = 0;
    if ((eventType & EVENT_READ) != 0) {
        events |= EPOLLIN;
    }
    if ((eventType & EVENT_WRITE) != 0) {
        events |= EPOLLOUT;
    }
    return events;
}

static inline unsigned MapEpollTypeValueToEvTypes(uint32_t events)
{
    unsigned eventType = 0;
    if ((events & EPOLLIN) != 0) {
        eventType |= EVENT_READ;
    }
    if ((events & EPOLLOUT) != 0) {
        eventType |= EVENT_WRITE;
    }
    return eventType;
}

static ErrorCode EpollLoopAddPolling(EventLoop *super, Event *evObj, unsigned eventTypes)
{
    ASSERT(super != NULL);
    ASSERT(evObj != NULL);
    EpollLoop *self = DOWN_TYPE_CAST(super, EpollLoop);
    struct epoll_event epollEvent;

    ErrorCode errCode = ReserveReventsSpace(self);
    if (errCode != ERROR_SYS_OK) {
        ErrLog(ERROR, ErrMsg("Epoll loop add fd failed, no space for revents."));
        return errCode;
    }

    unsigned eventType = eventTypes & EVENT_MASK;
    epollEvent.events = MapEvTypesToEpollTypeValue(eventType);
    epollEvent.data.ptr = evObj;
    int ret = epoll_ctl(self->epollFd, EPOLL_CTL_ADD, GetEventFd(evObj), &epollEvent);
    if (ret < 0) {
        ErrLog(ERROR, ErrMsg("Epoll Loop epoll_ctl() add fd %d failed, err(%s).", GetEventFd(evObj), strerror(errno)));
        return ERROR_UTILS_EVENT_POLL_CTL_FAILED;
    }
    SetEventType(evObj, eventType);
    ++self->pollFdCount;
    return ERROR_SYS_OK;
}

static void EpollLoopRemovePolling(EventLoop *super, Event *evObj)
{
    ASSERT(super != NULL);
    ASSERT(evObj != NULL);
    EpollLoop *self = DOWN_TYPE_CAST(super, EpollLoop);

    int ret = epoll_ctl(self->epollFd, EPOLL_CTL_DEL, GetEventFd(evObj), NULL);
    if (ret < 0) {
        ErrLog(ERROR, ErrMsg("Del fd %d from Epoll failed, err(%s).", GetEventFd(evObj), strerror(errno)));
        return;
    }
    --self->pollFdCount;
    return;
}

static void EpollLoopAddPollingType(EventLoop *super, Event *evObj, unsigned eventTypes)
{
    ASSERT(super != NULL);
    ASSERT(evObj != NULL);
    EpollLoop *self = DOWN_TYPE_CAST(super, EpollLoop);
    struct epoll_event epollEvent;

    unsigned eventType = eventTypes & EVENT_MASK;
    if (eventType == 0 || GetEventType(evObj) == eventType) {
        return;
    }
    unsigned events = GetEventType(evObj) | eventType;
    epollEvent.events = MapEvTypesToEpollTypeValue(events);
    epollEvent.data.ptr = evObj;
    int ret = epoll_ctl(self->epollFd, EPOLL_CTL_MOD, GetEventFd(evObj), &epollEvent);
    if (ret < 0) {
        ErrLog(ERROR,
               ErrMsg("Add evtype %x to Epoll failed, fd:%d, err(%s).", eventType, GetEventFd(evObj), strerror(errno)));
        return;
    }
    SetEventType(evObj, events);
    return;
}

static void EpollLoopRemovePollingType(EventLoop *super, Event *evObj, unsigned eventTypes)
{
    ASSERT(super != NULL);
    ASSERT(evObj != NULL);
    EpollLoop *self = DOWN_TYPE_CAST(super, EpollLoop);
    struct epoll_event epollEvent;

    unsigned eventType = eventTypes & EVENT_MASK;
    if (eventType == 0 || (eventType & GetEventType(evObj)) == 0) {
        return;
    }
    unsigned events = GetEventType(evObj) & (~eventType);
    epollEvent.events = MapEvTypesToEpollTypeValue(events);
    epollEvent.data.ptr = evObj;
    int ret = epoll_ctl(self->epollFd, EPOLL_CTL_MOD, GetEventFd(evObj), &epollEvent);
    if (ret < 0) {
        ErrLog(ERROR, ErrMsg("Remove evtype %x from Epoll failed, fd:%d, err(%s).", eventType, GetEventFd(evObj),
                             strerror(errno)));
        return;
    }
    SetEventType(evObj, events);
    return;
}

static ErrorCode EpollLoopPrepare(SYMBOL_UNUSED EventLoop *self, EventTime *interval)
{
    ASSERT(self != NULL);
    ASSERT(interval != NULL);

    if (*interval > MAX_EPOLL_SLEEP_MSEC) {
        *interval = MAX_EPOLL_SLEEP_MSEC;
    }
    return ERROR_SYS_OK;
}

static ErrorCode EpollLoopPolling(EventLoop *super, EventTime interval)
{
    ASSERT(super != NULL);
    EpollLoop *self = DOWN_TYPE_CAST(super, EpollLoop);

    int nReady = epoll_wait(self->epollFd, self->revents, (int)self->pollFdCount, (int)interval);
    if (nReady < 0) {
        if (errno != EINTR) {
            ErrLog(ERROR, ErrMsg("Polling call epoll_wait() failed, err(%s).", strerror(errno)));
            return ERROR_UTILS_EVENT_POLL_POLLING_FAILED;
        }
    }

    for (int index = 0; index < nReady; ++index) {
        struct epoll_event *revent = &self->revents[index];
        if (revent->data.ptr == self) {
            continue;
        }
        Event *evObj = (Event *)revent->data.ptr;

        uint32_t mask = EPOLLIN | EPOLLOUT;
        if ((revent->events & (~mask)) != 0) {
            ErrLog(WARNING, ErrMsg("Epoll loop encounter events:%d", revent->events));
        }

        unsigned revents = MapEpollTypeValueToEvTypes(revent->events);
        SetReventType(evObj, revents);
        SetEventObjectToActiveState(super, evObj);
    }
    return ERROR_SYS_OK;
}

static void EpollLoopSendWakeupSignal(EventLoop *super)
{
    ASSERT(super != NULL);
    EpollLoop *self = DOWN_TYPE_CAST(super, EpollLoop);

    char pipeData = 'a';
    while (true) {
        ssize_t nWrite = write(self->pipeFd[WRITE_PIPE], &pipeData, sizeof(pipeData));
        if (nWrite == (ssize_t)sizeof(pipeData)) {
            break;
        }
        int errNo = errno;
        if (errNo == EINTR) {
            continue;
        }
        if (errNo == EAGAIN) {
            /* Already signalled the loop, reach Max. */
            break;
        }
        ErrLog(ERROR, ErrMsg("Write data to wakup fd failed, err(%d-%s).", errNo, strerror(errNo)));
        return;
    }
}

static void EpollLoopClearWakeupSignal(EventLoop *super)
{
    ASSERT(super != NULL);
    EpollLoop *self = DOWN_TYPE_CAST(super, EpollLoop);

    while (true) {
        char pipeData;
        ssize_t nRead = read(self->pipeFd[READ_PIPE], &pipeData, sizeof(pipeData));
        if (nRead < 0) {
            int errNo = errno;
            if (errNo == EINTR) {
                continue;
            }
            if (errNo == EAGAIN) {
                break;
            }
            ErrLog(ERROR, ErrMsg("Read data from wakup fd failed, err(%d-%s).", errNo, strerror(errNo)));
            break;
        }
    }
}

static void EpollLoopOpsInit(EpollLoopOps *self)
{
    ASSERT(self != NULL);
    GET_FOPS(EventLoop)->createEventObject = EpollLoopCreateEventObject;
    GET_FOPS(EventLoop)->addPolling = EpollLoopAddPolling;
    GET_FOPS(EventLoop)->removePolling = EpollLoopRemovePolling;
    GET_FOPS(EventLoop)->addPollingType = EpollLoopAddPollingType;
    GET_FOPS(EventLoop)->removePollingType = EpollLoopRemovePollingType;
    GET_FOPS(EventLoop)->prepare = EpollLoopPrepare;
    GET_FOPS(EventLoop)->polling = EpollLoopPolling;
    GET_FOPS(EventLoop)->sendWakeupSignal = EpollLoopSendWakeupSignal;
    GET_FOPS(EventLoop)->clearWakeupSignal = EpollLoopClearWakeupSignal;
}

DEFINE_NEW_TYPED_CLASS(EpollLoop, EventLoop)
