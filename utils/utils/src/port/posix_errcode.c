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
 * posix_errcode.c
 *
 * Description:
 * 1. Defines POSIX error code conversion function
 *
 * ---------------------------------------------------------------------------------
 */
#include <errno.h>
#include "port/posix_errcode.h"

/**
 * Converting POSIX error codes to portable error codes.
 * @param rc : POSIX error code.
 * @param errCode : portable error code.
 */
UTILS_EXPORT void PosixErrorCode2PortErrorCode(int rc, ErrorCode *errCode)
{
    switch (rc) {
        case ENOMEM:
            *errCode = ERROR_UTILS_PORT_ENOMEM;
            break;
        case EINVAL:
            *errCode = ERROR_UTILS_PORT_EINVAL;
            break;
        case EAGAIN:
            *errCode = ERROR_UTILS_PORT_EAGAIN;
            break;
        case EPERM:
            *errCode = ERROR_UTILS_PORT_EPERM;
            break;
        case EBUSY:
            *errCode = ERROR_UTILS_PORT_EBUSY;
            break;
        case EDEADLK:
            *errCode = ERROR_UTILS_PORT_EDEADLK;
            break;
        case ETIMEDOUT:
            *errCode = ERROR_UTILS_PORT_ETIMEDOUT;
            break;
        case ENOSYS:
            *errCode = ERROR_UTILS_PORT_ENOSYS;
            break;
        case ESRCH:
            *errCode = ERROR_UTILS_PORT_ESRCH;
            break;
        case ENOTSUP:
            *errCode = ERROR_UTILS_PORT_ENOTSUP;
            break;
        case ERANGE:
            *errCode = ERROR_UTILS_PORT_ERANGE;
            break;
        case ENOTDIR:
            *errCode = ERROR_UTILS_PORT_ENOTDIR;
            break;
        case ENOENT:
            *errCode = ERROR_UTILS_PORT_ENOENT;
            break;
        case EFAULT:
            *errCode = ERROR_UTILS_PORT_EFAULT;
            break;
        case ENODEV:
            *errCode = ERROR_UTILS_PORT_ENODEV;
            break;
        case EEXIST:
            *errCode = ERROR_UTILS_PORT_EEXIST;
            break;
        case EINTR:
            *errCode = ERROR_UTILS_PORT_EINTR;
            break;
        case EIO:
            *errCode = ERROR_UTILS_PORT_EIO;
            break;
        case ENXIO:
            *errCode = ERROR_UTILS_PORT_ENXIO;
            break;
        case E2BIG:
            *errCode = ERROR_UTILS_PORT_E2BIG;
            break;
        case ENOEXEC:
            *errCode = ERROR_UTILS_PORT_ENOEXEC;
            break;
        case EBADF:
            *errCode = ERROR_UTILS_PORT_EBADF;
            break;
        case ECHILD:
            *errCode = ERROR_UTILS_PORT_ECHILD;
            break;
        case EACCES:
            *errCode = ERROR_UTILS_PORT_EACCES;
            break;
        case ENOTBLK:
            *errCode = ERROR_UTILS_PORT_ENOTBLK;
            break;
        case EXDEV:
            *errCode = ERROR_UTILS_PORT_EXDEV;
            break;
        case EISDIR:
            *errCode = ERROR_UTILS_PORT_EISDIR;
            break;
        case ENFILE:
            *errCode = ERROR_UTILS_PORT_ENFILE;
            break;
        case EMFILE:
            *errCode = ERROR_UTILS_PORT_EMFILE;
            break;
        case ENOTTY:
            *errCode = ERROR_UTILS_PORT_ENOTTY;
            break;
        case ETXTBSY:
            *errCode = ERROR_UTILS_PORT_ETXTBSY;
            break;
        case EFBIG:
            *errCode = ERROR_UTILS_PORT_EFBIG;
            break;
        case ENOSPC:
            *errCode = ERROR_UTILS_PORT_ENOSPC;
            break;
        case ESPIPE:
            *errCode = ERROR_UTILS_PORT_ESPIPE;
            break;
        case EROFS:
            *errCode = ERROR_UTILS_PORT_EROFS;
            break;
        case EMLINK:
            *errCode = ERROR_UTILS_PORT_EMLINK;
            break;
        case EPIPE:
            *errCode = ERROR_UTILS_PORT_EPIPE;
            break;
        case EDOM:
            *errCode = ERROR_UTILS_PORT_EDOM;
            break;
        default:
            *errCode = ERROR_UTILS_PORT_UNKNOWN;
            break;
    }
}
