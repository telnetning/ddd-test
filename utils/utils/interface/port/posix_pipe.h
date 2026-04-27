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
 * port_pipe.h
 *
 * Description:Defines the pipe for the platform portable interface.
 *
 * ---------------------------------------------------------------------------------
 */
#ifndef UTILS_PORT_PIPE_H
#define UTILS_PORT_PIPE_H

#include "defines/common.h"
#include "port/posix_path.h"

GSDB_BEGIN_C_CODE_DECLS

typedef enum {
    PIPE_RDONLY, /* Pipe is read-only access. */
    PIPE_WRONLY, /* Pipe is write-only access. */
    PIPE_RDWR    /* Pipe is read-write access. */
} PipeAccessMode;

/* Define the pipe object across platforms. */
typedef struct Pipe Pipe;
struct Pipe {
    int pipe;
    char pathname[MAX_PATH];
};

/* Create named pipe. */
UTILS_EXPORT ErrorCode MakeNamedPipe(const char *pathname, uint32_t mode, Pipe *pipe);
/* Open a pipe. Read and write access can be performed only after the pipe is opened. */
UTILS_EXPORT ErrorCode OpenNamedPipe(Pipe *pipe, PipeAccessMode accessMode);
/* Write data to the pipe. */
UTILS_EXPORT ErrorCode WriteToPipe(Pipe *pipe, const void *buf, size_t count);
/* Read data from the pipe. */
UTILS_EXPORT ErrorCode ReadFromPipe(Pipe *pipe, void *buf, size_t count, ssize_t *readCount);
/* Close the pipe. After the pipe is closed, the pipe cannot be accessed. */
UTILS_EXPORT void CloseNamedPipe(Pipe *pipe);
/* Set the pipe no block mode. */
UTILS_EXPORT ErrorCode SetNamedPipeNoBlock(Pipe *pipe);
/* Set the pipe block mode. */
UTILS_EXPORT ErrorCode SetNamedPipeBlock(Pipe *pipe);

GSDB_END_C_CODE_DECLS
#endif /* UTILS_PORT_PIPE_H */
