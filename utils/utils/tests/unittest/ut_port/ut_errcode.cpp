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
 * Description:
 * 1. unit tests of memory_allocator.c
 *
 * ---------------------------------------------------------------------------------
 */

#include <gtest/gtest.h>
#include <errno.h>
#include "port/posix_errcode.h"

class ErrorCodeTest : public testing::Test {
public:
    void SetUp() override {}
    void TearDown() override {}
};

struct ErrorCodeEntry {
    int rc;
    ErrorCode errCode;
};

TEST_F(ErrorCodeTest, TestPosixErrorCode2PortErrorCode)
{
    ErrorCode errCode;
    struct ErrorCodeEntry entries[] = {
        {
            .rc = ENOMEM,
            .errCode = ERROR_UTILS_PORT_ENOMEM
        },
        {
            .rc = EINVAL,
            .errCode = ERROR_UTILS_PORT_EINVAL
        },
        {
            .rc = EAGAIN, 
            .errCode = ERROR_UTILS_PORT_EAGAIN
        }, 
        {
            .rc = EPERM,
            .errCode = ERROR_UTILS_PORT_EPERM
        },
        {
            .rc = EBUSY,
            .errCode = ERROR_UTILS_PORT_EBUSY
        },
        {
            .rc = EDEADLK,
            .errCode = ERROR_UTILS_PORT_EDEADLK
        },
        {
            .rc = ETIMEDOUT,
            .errCode = ERROR_UTILS_PORT_ETIMEDOUT
        },
        {
            .rc = ENOSYS,
            .errCode = ERROR_UTILS_PORT_ENOSYS
        },
        {
            .rc = ESRCH,
            .errCode = ERROR_UTILS_PORT_ESRCH
        },
        {
            .rc = ENOTSUP,
            .errCode = ERROR_UTILS_PORT_ENOTSUP
        },
        {
            .rc = ERANGE,
            .errCode = ERROR_UTILS_PORT_ERANGE
        },
        {
            .rc = ENOTDIR,
            .errCode = ERROR_UTILS_PORT_ENOTDIR
        },
        {
            .rc = ENOENT,
            .errCode = ERROR_UTILS_PORT_ENOENT
        },
        {
            .rc = EFAULT,
            .errCode = ERROR_UTILS_PORT_EFAULT
        },
        {
            .rc = ENODEV,
            .errCode = ERROR_UTILS_PORT_ENODEV
        },
        {
            .rc = EEXIST,
            .errCode = ERROR_UTILS_PORT_EEXIST
        },
        {
            .rc = EINTR,
            .errCode = ERROR_UTILS_PORT_EINTR
        },
        {
            .rc = EIO,
            .errCode = ERROR_UTILS_PORT_EIO
        },
        {
            .rc = ENXIO,
            .errCode = ERROR_UTILS_PORT_ENXIO
        },
        {
            .rc = E2BIG,
            .errCode = ERROR_UTILS_PORT_E2BIG
        },
        {
            .rc = ENOEXEC,
            .errCode = ERROR_UTILS_PORT_ENOEXEC
        },
        {
            .rc = EBADF,
            .errCode = ERROR_UTILS_PORT_EBADF
        },
        {
            .rc = ECHILD,
            .errCode = ERROR_UTILS_PORT_ECHILD
        },
        {
            .rc = EACCES,
            .errCode = ERROR_UTILS_PORT_EACCES
        },
        {
            .rc = ENOTBLK,
            .errCode = ERROR_UTILS_PORT_ENOTBLK
        },
        {
            .rc = EXDEV,
            .errCode = ERROR_UTILS_PORT_EXDEV
        },
        {
            .rc = EISDIR,
            .errCode = ERROR_UTILS_PORT_EISDIR
        },
        {
            .rc = ENFILE,
            .errCode = ERROR_UTILS_PORT_ENFILE
        },
        {
            .rc = EMFILE,
            .errCode = ERROR_UTILS_PORT_EMFILE
        },
        {
            .rc = ENOTTY,
            .errCode = ERROR_UTILS_PORT_ENOTTY
        },
        {
            .rc = ETXTBSY,
            .errCode = ERROR_UTILS_PORT_ETXTBSY
        },
        {
            .rc = EFBIG,
            .errCode = ERROR_UTILS_PORT_EFBIG
        },
        {
            .rc = ENOSPC,
            .errCode = ERROR_UTILS_PORT_ENOSPC
        },
        {
            .rc = ESPIPE,
            .errCode = ERROR_UTILS_PORT_ESPIPE
        },
        {
            .rc = EROFS,
            .errCode = ERROR_UTILS_PORT_EROFS
        },
        {
            .rc = EMLINK,
            .errCode = ERROR_UTILS_PORT_EMLINK
        },
        {
            .rc = EPIPE,
            .errCode = ERROR_UTILS_PORT_EPIPE
        },
        {
            .rc = EDOM,
            .errCode = ERROR_UTILS_PORT_EDOM
        },
        {
            .rc = 0,
            .errCode = ERROR_UTILS_PORT_UNKNOWN
        }
    };

    int i = 0;
    int n = sizeof(entries) / sizeof(struct ErrorCodeEntry);
    for (; i < n; i++) {
        PosixErrorCode2PortErrorCode(entries[i].rc, &errCode);
        EXPECT_EQ(errCode, entries[i].errCode);
    }
}