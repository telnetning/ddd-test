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
 * win32_errcode.h
 *
 * Description:Defines Windows error code conversion function.
 *
 * ---------------------------------------------------------------------------------
 */
#ifndef UTILS_WIN32_ERRCODE_H
#define UTILS_WIN32_ERRCODE_H

#include <stdlib.h>
#include <windows.h>
#include <stdarg.h>
#include <stdio.h>
#include "port/port_errorcode.h"

GSDB_BEGIN_C_CODE_DECLS
/**
 * Converting windows error codes to portable error codes.
 * @param errno : Windows error code.
 * @param errCode : Portable error code.
 */
void WindowsErrorCode2PortErrorCode(int errno, ErrorCode *errCode);

GSDB_END_C_CODE_DECLS
#endif /* UTILS_WIN32_ERRCODE_H */
