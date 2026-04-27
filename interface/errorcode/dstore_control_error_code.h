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

#ifndef DSTORE_CONTROL_ERROR_CODE_H
#define DSTORE_CONTROL_ERROR_CODE_H

#include "common/dstore_common_utils.h"
#include "framework/dstore_modules.h"
#include "errorcode/dstore_error_struct.h"

namespace DSTORE {

enum ControlErrorReason : uint32_t {
    INVALID_CONTROL_PARAMETER = 0x1,
    FILE_NOT_OPEN = 0x2,
    NOT_INITIALIZED = 0x3,
    SECURE_FUNCTION_FAILED = 0x4,
    HEADER_CHECK_FAILED = 0x5,
    INVALID_BLOCK_NUMBER = 0x6,
    MEMORY_NOT_ENOUGH = 0x7,
    ITEM_NOT_FIND = 0x8,
    PDB_NOT_FOUND = 0x9,
    PDB_ALLOC_ID_FAILED = 0xA,
    CONTROL_ERROR_MAX
};

const ErrorCode CONTROL_ERROR_INVALID_PARAMETER =
    (ERROR_MODULE_CONTROL | ERROR_SEVERITY_ERROR | INVALID_CONTROL_PARAMETER);
const ErrorCode CONTROL_ERROR_FILE_NOT_OPEN =
    (ERROR_MODULE_CONTROL | ERROR_SEVERITY_ERROR | FILE_NOT_OPEN);
const ErrorCode CONTROL_ERROR_NOT_INITIALIZED =
    (ERROR_MODULE_CONTROL | ERROR_SEVERITY_ERROR | NOT_INITIALIZED);
const ErrorCode CONTROL_ERROR_SECURE_FUNCTION_FAILED =
    (ERROR_MODULE_CONTROL | ERROR_SEVERITY_ERROR | SECURE_FUNCTION_FAILED);
const ErrorCode CONTROL_ERROR_HEADER_CHECK_FAILED =
    (ERROR_MODULE_CONTROL | ERROR_SEVERITY_ERROR | HEADER_CHECK_FAILED);
const ErrorCode CONTROL_ERROR_INVALID_BLOCK_NUMBER =
    (ERROR_MODULE_CONTROL | ERROR_SEVERITY_ERROR | INVALID_BLOCK_NUMBER);
const ErrorCode CONTROL_ERROR_MEMORY_NOT_ENOUGH =
    (ERROR_MODULE_CONTROL | ERROR_SEVERITY_ERROR | MEMORY_NOT_ENOUGH);
const ErrorCode CONTROL_ERROR_ITEM_NOT_FIND =
    (ERROR_MODULE_CONTROL | ERROR_SEVERITY_ERROR | ITEM_NOT_FIND);
const ErrorCode CONTROL_ERROR_PDB_NOT_FOUND =
    (ERROR_MODULE_CONTROL | ERROR_SEVERITY_ERROR | PDB_NOT_FOUND);
const ErrorCode CONTROL_ERROR_PDB_ALLOC_ID_FAILED =
    (ERROR_MODULE_CONTROL | ERROR_SEVERITY_ERROR | PDB_ALLOC_ID_FAILED);

} /* namespace DSTORE */

#endif
