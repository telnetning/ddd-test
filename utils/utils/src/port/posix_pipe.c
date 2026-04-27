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
 * ---------------------------------------------------------------------------------
 *
 * posix_pipe.c
 *
 * Description:
 * 1. Implementation of the POSIX pipe interface wrapper.
 *
 * ---------------------------------------------------------------------------------
 */
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include "securec.h"
#include "defines/abort.h"
#include "port/posix_pipe.h"

#define PIPE_ACCESS_MODE_RDONLY 0400
#define PIPE_ACCESS_MODE_WRONLY 0200
#define PIPE_ACCESS_MODE_RDWR   0600
/* Create named pipe. */
UTILS_EXPORT ErrorCode MakeNamedPipe(const char *pathname, uint32_t mode, Pipe *pipe)
{
    ErrorCode errCode = ERROR_SYS_OK;
    errno_t rc = strcpy_s(pipe->pathname, MAX_PATH, pathname);
    if (rc != EOK) {
        return ERROR_UTILS_COMMON_SECURE_C_FUNCTION_FAILED;
    }
    mode_t accessMode;
    switch (mode) {
        case PIPE_RDONLY:
            accessMode = PIPE_ACCESS_MODE_RDONLY;
            break;
        case PIPE_WRONLY:
            accessMode = PIPE_ACCESS_MODE_WRONLY;
            break;
        case PIPE_RDWR:
            accessMode = PIPE_ACCESS_MODE_RDWR;
            break;
        default:
            accessMode = PIPE_ACCESS_MODE_RDWR;
            break;
    }
    rc = mkfifo(pathname, accessMode);
    if (rc != 0) {
        PosixErrorCode2PortErrorCode(errno, &errCode);
    }
    return errCode;
}

/* Open a pipe. Read and write access can be performed only after the pipe is opened. */
UTILS_EXPORT ErrorCode OpenNamedPipe(Pipe *pipe, PipeAccessMode accessMode)
{
    int flags;
    switch (accessMode) {
        case PIPE_RDONLY:
            flags = O_RDONLY;
            break;
        case PIPE_WRONLY:
            flags = O_WRONLY;
            break;
        case PIPE_RDWR:
            flags = O_RDWR;
            break;
        default:
            flags = O_RDWR;
            break;
    }
    ErrorCode errCode = ERROR_SYS_OK;
    char resolvedFilename[PATH_MAX];
    if (!CanonicalizePath(pipe->pathname, resolvedFilename)) {
        PosixErrorCode2PortErrorCode(errno, &errCode);
        return errCode;
    }
    int file = open(resolvedFilename, flags);
    if (file < 0) {
        PosixErrorCode2PortErrorCode(errno, &errCode);
        return errCode;
    }
    pipe->pipe = file;
    return errCode;
}

/* Write data to the pipe. */
UTILS_EXPORT ErrorCode WriteToPipe(Pipe *pipe, const void *buf, size_t count)
{
    ErrorCode errCode = ERROR_SYS_OK;
    ssize_t writedCount = write(pipe->pipe, buf, count);
    if (writedCount < 0 || writedCount != (ssize_t)count) {
        PosixErrorCode2PortErrorCode(errno, &errCode);
        return errCode;
    }
    return errCode;
}

/* Read data from the pipe. */
UTILS_EXPORT ErrorCode ReadFromPipe(Pipe *pipe, void *buf, size_t count, ssize_t *readCount)
{
    ErrorCode errCode = ERROR_SYS_OK;
    *readCount = read(pipe->pipe, buf, count);
    if (*readCount < 0) {
        PosixErrorCode2PortErrorCode(errno, &errCode);
        return errCode;
    }
    return errCode;
}

/* Close the pipe. After the pipe is closed, the pipe cannot be accessed. */
UTILS_EXPORT ErrorCode CloseNamedPipeEx(Pipe *pipe)
{
    ErrorCode errCode = ERROR_SYS_OK;
    int rc = close(pipe->pipe);
    if (rc < 0) {
        PosixErrorCode2PortErrorCode(errno, &errCode);
        return errCode;
    }
    pipe->pipe = -1;
    return errCode;
}

/* Close the pipe. After the pipe is closed, the pipe cannot be accessed. */
UTILS_EXPORT void CloseNamedPipe(Pipe *pipe)
{
    ErrorCode errCode;
    errCode = CloseNamedPipeEx(pipe);
    if (errCode != ERROR_SYS_OK) {
        Abort();
    }
}
/* Set the pipe no block mode. */
UTILS_EXPORT ErrorCode SetNamedPipeNoBlock(Pipe *pipe)
{
    ErrorCode errCode = ERROR_SYS_OK;
    int rc;
    int flags;
    flags = fcntl(pipe->pipe, F_GETFL);
    if (unlikely(flags < 0)) {
        PosixErrorCode2PortErrorCode(errno, &errCode);
        return errCode;
    }
    flags = (int)((unsigned int)flags | O_NONBLOCK);
    rc = fcntl(pipe->pipe, F_SETFL, flags);
    if (rc < 0) {
        PosixErrorCode2PortErrorCode(errno, &errCode);
        return errCode;
    }
    return errCode;
}

/* Set the pipe block mode. */
UTILS_EXPORT ErrorCode SetNamedPipeBlock(Pipe *pipe)
{
    ErrorCode errCode = ERROR_SYS_OK;
    int rc;
    uint32_t flags;
    rc = fcntl(pipe->pipe, F_GETFL);
    if (rc < 0) {
        PosixErrorCode2PortErrorCode(errno, &errCode);
        return errCode;
    }
    flags = (uint32_t)rc;
    flags = flags & ~(uint32_t)O_NONBLOCK;
    rc = fcntl(pipe->pipe, F_SETFL, (int)flags);
    if (rc < 0) {
        PosixErrorCode2PortErrorCode(errno, &errCode);
        return errCode;
    }
    return errCode;
}
