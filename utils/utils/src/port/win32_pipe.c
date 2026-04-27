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
 * win32_pipe.c
 *
 * Description:
 * 1. Implementation of the windows pipe interface wrapper.
 *
 * ---------------------------------------------------------------------------------
 */
#include "port/win32_pipe.h"

/*
 * Create named pipe.
 * On the Windows platform, the pipe name must be in the following format.
 * \\.\pipe\pipename
 * */
ErrorCode MakeNamedPipe(const char *pathname, uint32_t mode, Pipe *pipe)
{
#define PIPE_OUT_BUFFER_SIZE 1000
#define PIPE_IN_BUFFER_SIZE  1000
#define PIPE_WAIT_TIME_OUT   16 /* Milliseconds. */
    ErrorCode errCode = ERROR_SYS_OK;
    DWORD openMode;
    switch (mode) {
        case PIPE_RDONLY:
            openMode = PIPE_ACCESS_INBOUND;
            break;
        case PIPE_WRONLY:
            openMode = PIPE_ACCESS_OUTBOUND;
            break;
        case PIPE_RDWR:
            openMode = PIPE_ACCESS_DUPLEX;
            break;
        default:
            openMode = PIPE_ACCESS_DUPLEX;
            break;
    }
    pipe->pipe =
        CreateNamedPipe(pathname, openMode, PIPE_TYPE_MESSAGE | PIPE_READMODE_MESSAGE | PIPE_WAIT,
                        PIPE_UNLIMITED_INSTANCES, PIPE_OUT_BUFFER_SIZE, PIPE_IN_BUFFER_SIZE, PIPE_WAIT_TIME_OUT, NULL);
    if (pipe->pipe == INVALID_HANDLE_VALUE) {
        WindowsErrorCode2PortErrorCode(GetLastError(), &errCode);
    }
    return errCode;
}

/* Open a pipe. Read and write access can be performed only after the pipe is opened. */
ErrorCode OpenNamedPipe(Pipe *pipe, PipeAccessMode accessMode)
{
    BOOL connected = FALSE;
    ErrorCode errCode = ERROR_SYS_OK;
    connected = ConnectNamedPipe(pipe->pipe, NULL);
    if (connected || (GetLastError() == ERROR_PIPE_CONNECTED)) {
        return ERROR_SYS_OK;
    } else {
        WindowsErrorCode2PortErrorCode(GetLastError(), &errCode);
        return errCode;
    }
}

/* Write data to the pipe. */
ErrorCode WriteToPipe(Pipe *pipe, const void *buf, size_t count)
{
    ErrorCode errCode = ERROR_SYS_OK;
    BOOL writeSuccess = FALSE;
    ssize_t writedCount;
    writeSuccess = WriteFile(pipe->pipe, buf, count, &writedCount, NULL);
    if (!writeSuccess || writedCount != count) {
        WindowsErrorCode2PortErrorCode(GetLastError(), &errCode);
    }
    return errCode;
}

/* Read data from the pipe. */
ErrorCode ReadFromPipe(Pipe *pipe, const void *buf, size_t count, ssize_t *readCount)
{
    ErrorCode errCode = ERROR_SYS_OK;
    BOOL readSuccess = FALSE;
    readSuccess = ReadFile(pipe->pipe, buf, count, readCount, NULL);
    if (!readSuccess || *readCount < 0) {
        WindowsErrorCode2PortErrorCode(GetLastError(), &errCode);
    }
    return errCode;
}

/* Close the pipe. After the pipe is closed, the pipe cannot be accessed. */
void CloseNamedPipe(Pipe *pipe)
{
    BOOL success = DisconnectNamedPipe(pipe->pipe);
    if (!success) {
        Abort();
    }
    success = CloseHandle(pipe->pipe);
    if (!success) {
        Abort();
    }
    pipe->pipe = INVALID_FILE_HANDLE;
}

/* Set the pipe no block mode. */
ErrorCode SetNamedPipeNoBlock(Pipe *pipe)
{
    ErrorCode errCode = ERROR_SYS_OK;
    BOOL succeed = SetNamedPipeHandleState(pipe->pipe, PIPE_NOWAIT, NULL, NULL);
    if (!succeed) {
        WindowsErrorCode2PortErrorCode(GetLastError(), &errCode);
        return errCode;
    }
    return errCode;
}
