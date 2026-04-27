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
 * posix_errcode.h
 *
 * Description:Defines POSIX error code conversion function.
 *
 * ---------------------------------------------------------------------------------
 */
#ifndef UTILS_POSIX_ERRCODE_H
#define UTILS_POSIX_ERRCODE_H

#include "defines/common.h"
#include "defines/err_code.h"
#include "port/port_errorcode.h"

GSDB_BEGIN_C_CODE_DECLS

/* Converting POSIX error codes to portable error codes. */
void PosixErrorCode2PortErrorCode(int rc, ErrorCode *errCode);

GSDB_END_C_CODE_DECLS
#endif /* UTILS_POSIX_ERRCODE_H */
