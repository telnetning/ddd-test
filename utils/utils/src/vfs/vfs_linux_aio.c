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
 */

#include "vfs/vfs_linux_common.h"

#define AIO_GETEVENTS_WAIT_TIME_NANO_SECOND 100000000
ErrorCode AIOCtxInit(AioThreadContext *aioThreadContext, uint16_t eventNum)
{
    aioThreadContext->isAioStop = true;
    aioThreadContext->maxEvents = eventNum;

    int rc = io_setup(eventNum, &aioThreadContext->ioctx);
    if (rc != 0) {
        VfsPrintReleaseLog("io setup failed. errno is %d:%s.", errno, strerror(errno));
        return VFS_ERROR_AIO_CONTEXT_INITL_FAIL;
    }

    return ERROR_SYS_OK;
}

ErrorCode AIOHandleEvent(AioThreadContext *aioThreadContext, struct io_event *events)
{
    struct timespec timeout;
    timeout.tv_sec = 0;
    timeout.tv_nsec = AIO_GETEVENTS_WAIT_TIME_NANO_SECOND;

    /* numEvents will return 0 when timeout */
    int16_t numEvents =
        (int16_t)io_getevents(aioThreadContext->ioctx, MIN_AIO_EVENT_NUM, (long)MAX_AIO_EVENT_NUM, events, &timeout);
    if (numEvents < 0) {
        /* There are EINT, EFAULT, EINVAL and ENOSYS */
        VfsPrintReleaseLog("io get events failed. errno is %d:%s.", errno, strerror(errno));
        return ERROR_SYS_OK;
    }

    for (uint16_t i = 0; i < (uint16_t)numEvents; i++) {
        void *asyncContext = (void *)events[i].data;
        /* The address of iocb and aioIocb is the same, so here could do type conversion */
        struct UtilsAioIocb *aioIocb = (struct UtilsAioIocb *)events[i].obj;
        if (aioIocb->asyncCallback != NULL) {
            if ((long int)events[i].res < 0) {
                aioIocb->asyncCallback(ConvertLinuxSysErrCode(errno), 0, asyncContext);
            } else {
                aioIocb->asyncCallback(ERROR_SYS_OK, (int64_t)events[i].res, asyncContext);
            }
        }
        GSDB_ATOMIC32_DEC(&(aioIocb->fd->asyncReqCount));
    }

    return ERROR_SYS_OK;
}

void *AIOMainThread(void *arg)
{
    AioThreadContext *aioThreadContext = (AioThreadContext *)arg;
    if (aioThreadContext->threadEnterCallback != NULL) {
        aioThreadContext->threadEnterCallback();
    }

    struct io_event *events = (struct io_event *)VfsMemAlloc(NULL, sizeof(struct io_event) * MAX_AIO_EVENT_NUM);
    if (events == NULL) {
        VfsPrintLog("VfsMemAlloc fails in AIOMainThread().");
    } else {
        while (!aioThreadContext->isAioStop) {
            errno_t re = memset_s(events, sizeof(struct io_event) * MAX_AIO_EVENT_NUM, 0,
                                  sizeof(struct io_event) * MAX_AIO_EVENT_NUM);
            if (unlikely(re != EOK)) {
                VfsPrintLog("memset_s fails in AIOMainThread().");
                break;
            }
            /* CPU will not cost all the time to get events because of timeout in io_getevent  */
            ErrorCode errCode = AIOHandleEvent(aioThreadContext, events);
            if (errCode != ERROR_SYS_OK) {
                break;
            }
        }
        VfsMemFree(NULL, events);
    }

    VfsPrintLog("Thread exit.");
    if (aioThreadContext->threadExitCallback != NULL) {
        aioThreadContext->threadExitCallback();
    }
    return aioThreadContext;
}

void StopAIO(AioThreadContext *aioThreadContext)
{
    if (aioThreadContext == NULL || aioThreadContext->isAioStop == true) {
        return;
    }

    aioThreadContext->isAioStop = true;

    /* Wait all threads exit */
    for (uint16_t i = 0; i < aioThreadContext->threadCount; i++) {
        (void)ThreadJoin(aioThreadContext->aioCompleterThreadId[i], NULL);
    }

    (void)io_destroy(aioThreadContext->ioctx);

    VfsMemFree(NULL, aioThreadContext);
}

ErrorCode StartAIO(uint16_t maxEvents, uint16_t threadCount, AioThreadContext **aioThreadContext,
                   void (*threadEnterCallback)(void), void (*threadExitCallback)(void))
{
    if (*aioThreadContext != NULL) {
        VfsPrintLog("Aio thread context is already being initialized!");
        return ERROR_SYS_OK;
    }

    *aioThreadContext = (AioThreadContext *)VfsMemAlloc(NULL, sizeof(AioThreadContext));
    if (*aioThreadContext == NULL) {
        return VFS_ERROR_RESOURCE_NOT_ENOUGH;
    }

    (void)memset_s(*aioThreadContext, sizeof(AioThreadContext), 0, sizeof(AioThreadContext));
    ErrorCode errCode = ERROR_SYS_OK;

    errCode = AIOCtxInit(*aioThreadContext, maxEvents);
    if (errCode != ERROR_SYS_OK) {
        goto START_AIO_FAIL;
    }

    (*aioThreadContext)->threadEnterCallback = threadEnterCallback;
    (*aioThreadContext)->threadExitCallback = threadExitCallback;

    ThreadStartRoutine routineFunc = AIOMainThread;

    (*aioThreadContext)->isAioStop = false;
    (*aioThreadContext)->threadCount = threadCount;
    for (uint16_t i = 0; i < threadCount; i++) {
        errCode = ThreadCreate(&(*aioThreadContext)->aioCompleterThreadId[i], routineFunc, *aioThreadContext);
        if (errCode != ERROR_SYS_OK) {
            goto START_AIO_FAIL;
        }
    }

    return ERROR_SYS_OK;

START_AIO_FAIL:
    StopAIO(*aioThreadContext);
    *aioThreadContext = NULL;
    return errCode;
}