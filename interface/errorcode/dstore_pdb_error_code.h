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

#ifndef DSTORE_PDB_ERROR_CODE_H
#define DSTORE_PDB_ERROR_CODE_H

#include "common/dstore_common_utils.h"
#include "framework/dstore_modules.h"
#include "errorcode/dstore_error_struct.h"

namespace DSTORE {

enum PdbErrorReason : uint32_t {
    STOP_SQL_THREAD_FAIL = 0x1,
    STOP_BACKGROUND_THREADS_FAIL = 0x2,
    PDB_RESET_FAIL = 0x3,
    PDB_UPDATE_STATUS_FAIL = 0x4,
    IN_DR_RELATION = 0x5,
    PDB_INVALIDATION_BUFFER_FAIL = 0x6,
    PDB_RESET_TIME_FAIL = 0x7,
    PDB_NOT_INITIALIZED = 0x8,
    PDB_ACQUIRE_DIST_LOCK_FAIL = 0x9,
    PDB_INVALID_MESSAGE_ORDER = 0xA,
    PDB_SWITCH_CONTEXT_FAIL = 0xB,
    PDB_ERROR_MAX
};

const ErrorCode PDB_ERROR_STOP_SQL_THREAD_FAIL =
    (ERROR_MODULE_PDB | ERROR_SEVERITY_ERROR | STOP_SQL_THREAD_FAIL);
const ErrorCode PDB_ERROR_STOP_BACKGROUND_THREADS_FAIL =
    (ERROR_MODULE_PDB | ERROR_SEVERITY_ERROR | STOP_BACKGROUND_THREADS_FAIL);
const ErrorCode PDB_ERROR_RESET_FAIL =
    (ERROR_MODULE_PDB | ERROR_SEVERITY_ERROR | PDB_RESET_FAIL);
const ErrorCode PDB_ERROR_UPDATE_STATUS_FAIL =
    (ERROR_MODULE_PDB | ERROR_SEVERITY_ERROR | PDB_UPDATE_STATUS_FAIL);
const ErrorCode PDB_ERROR_IN_DR_RELATION =
    (ERROR_MODULE_PDB | ERROR_SEVERITY_ERROR | IN_DR_RELATION);
const ErrorCode PDB_ERROR_INVALIDATION_BUFFER_FAIL =
    (ERROR_MODULE_PDB | ERROR_SEVERITY_ERROR | PDB_INVALIDATION_BUFFER_FAIL);
const ErrorCode PDB_ERROR_RESET_TIME_FAIL =
    (ERROR_MODULE_PDB | ERROR_SEVERITY_ERROR | PDB_RESET_TIME_FAIL);
const ErrorCode PDB_ERROR_NOT_INITIALIZED =
    (ERROR_MODULE_PDB | ERROR_SEVERITY_ERROR | PDB_NOT_INITIALIZED);
const ErrorCode PDB_ERROR_ACQUIRE_DIST_LOCK_FAIL =
    (ERROR_MODULE_PDB | ERROR_SEVERITY_ERROR | PDB_ACQUIRE_DIST_LOCK_FAIL);
const ErrorCode PDB_ERROR_INVALID_MESSAGE_ORDER =
    (ERROR_MODULE_PDB | ERROR_SEVERITY_ERROR | PDB_INVALID_MESSAGE_ORDER);
const ErrorCode PDB_ERROR_SWITCH_CONTEXT_FAIL =
    (ERROR_MODULE_PDB | ERROR_SEVERITY_ERROR | PDB_SWITCH_CONTEXT_FAIL);

} /* namespace DSTORE */

#endif
