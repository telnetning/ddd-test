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
 * win32_errcode.c
 *
 * Description:
 * 1. Defines Windows error code conversion function
 *
 * ---------------------------------------------------------------------------------
 */
#include "port/win32_errcode.h"

/**
 * Converting windows error codes to portable error codes.
 * @param errno : Windows error code.
 * @param errCode : Portable error code.
 */
void WindowsErrorCode2PortErrorCode(int errno, ErrorCode *errCode)
{
    switch (errno) {
        case EACCES:
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
        default:
            *errCode = ERROR_UTILS_PORT_UNKNOWN;
            break;
    }
}