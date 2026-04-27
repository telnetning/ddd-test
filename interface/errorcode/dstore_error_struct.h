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
 * ---------------------------------------------------------------------------------------
 *
 * IDENTIFICATION
 *        src/include/common/error/dstore_error.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_ERROR_STRUCT_H
#define DSTORE_ERROR_STRUCT_H

#include "defines/err_code.h"

namespace DSTORE {
constexpr long long ERROR_SEVERITY_MASK = 0x00000F0000000000;
constexpr long long ERROR_SEVERITY_INFO = 0x0000000000000000;
constexpr long long ERROR_SEVERITY_WARNING = 0x0000010000000000;
constexpr long long ERROR_SEVERITY_ERROR = 0x0000020000000000;
constexpr long long ERROR_SEVERITY_SEVERE = 0x0000030000000000;
constexpr long long ERROR_SEVERITY_FATAL = 0x0000040000000000;

#define ERROR_MODULE_BUFFER ((ErrorCode)MODULE_BUFFER << ERROR_MODULE_SHIFT)
#define ERROR_MODULE_CATALOG ((ErrorCode)MODULE_CATALOG << ERROR_MODULE_SHIFT)
#define ERROR_MODULE_COMMON ((ErrorCode)MODULE_COMMON << ERROR_MODULE_SHIFT)
#define ERROR_MODULE_FRAMEWORK ((ErrorCode)MODULE_FRAMEWORK << ERROR_MODULE_SHIFT)
#define ERROR_MODULE_HEAP ((ErrorCode)MODULE_HEAP << ERROR_MODULE_SHIFT)
#define ERROR_MODULE_INDEX ((ErrorCode)MODULE_INDEX << ERROR_MODULE_SHIFT)
#define ERROR_MODULE_LOCK ((ErrorCode)MODULE_LOCK << ERROR_MODULE_SHIFT)
#define ERROR_MODULE_PAGE ((ErrorCode)MODULE_PAGE << ERROR_MODULE_SHIFT)
#define ERROR_MODULE_PORT ((ErrorCode)MODULE_PORT << ERROR_MODULE_SHIFT)
#define ERROR_MODULE_WAL ((ErrorCode)MODULE_WAL << ERROR_MODULE_SHIFT)
#define ERROR_MODULE_TABLESPACE ((ErrorCode)MODULE_TABLESPACE << ERROR_MODULE_SHIFT)
#define ERROR_MODULE_TRANSACTION ((ErrorCode)MODULE_TRANSACTION << ERROR_MODULE_SHIFT)
#define ERROR_MODULE_TUPLE ((ErrorCode)MODULE_TUPLE << ERROR_MODULE_SHIFT)
#define ERROR_MODULE_UNDO ((ErrorCode)MODULE_UNDO << ERROR_MODULE_SHIFT)
#define ERROR_MODULE_RPC ((ErrorCode)MODULE_RPC << ERROR_MODULE_SHIFT)
#define ERROR_MODULE_CONTROL ((ErrorCode)MODULE_CONTROL << ERROR_MODULE_SHIFT)
#define ERROR_MODULE_SYSTABLE ((ErrorCode)MODULE_SYSTABLE << ERROR_MODULE_SHIFT)
#define ERROR_MODULE_EMBEDDED ((ErrorCode)MODULE_EMBEDDED << ERROR_MODULE_SHIFT)
#define ERROR_MODULE_RECOVERY ((ErrorCode)MODULE_RECOVERY << ERROR_MODULE_SHIFT)
#define ERROR_MODULE_LOGICAL_REPLICATION ((ErrorCode)MODULE_LOGICAL_REPLICATION << ERROR_MODULE_SHIFT)
#define ERROR_MODULE_BACKUP_RESTORE ((ErrorCode)MODULE_BACKUP_RESTORE << ERROR_MODULE_SHIFT)
#define ERROR_MODULE_PDB ((ErrorCode)MODULE_PDB << ERROR_MODULE_SHIFT)

#ifndef ERROR_GET_SEVERITY
#define ERROR_GET_SEVERITY(_errorCode) ((_errorCode)&ERROR_SEVERITY_MASK)
#endif

#define STORAGE_OK 0
const ErrorCode ERRORCODE_OK = STORAGE_OK;

struct ErrorDetails {
    const char *name;
    const char *message;
};
}  /* namespace DSTORE */
#endif
