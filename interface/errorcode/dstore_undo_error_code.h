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

#ifndef DSTORE_UNDO_ERROR_CODE_H
#define DSTORE_UNDO_ERROR_CODE_H

#include "common/dstore_common_utils.h"
#include "framework/dstore_modules.h"
#include "errorcode/dstore_error_struct.h"

namespace DSTORE {

enum UndoErrorReason : uint32_t {
    UNDO_OUT_OF_MEMORY = 0x1,
    UNDO_CREATE_FILE = 0x2,
    UNDO_INVALID_ZONE_ID = 0x3,
    UNDO_INVALID_UNDO_TYPE = 0x4,
    UNDO_CORRPUT_RECORD = 0x5,
    UNDO_INVALID_UNDO_RECORD_PTR = 0x6,
    UNDO_INVALID_DATA = 0x7,
    UNDO_RECORD_RECYCLED = 0x8,
    UNDO_NO_FREE_ZONE = 0x9,
    UNDO_ZONE_NOT_FREE = 0xA,
    TOO_MANY_UNDO_ZONES = 0xB,
    UNDO_NOT_FULLY_INITED = 0xC,
    UNDO_NO_ZONE_OWNER = 0xD,
    FAIL_TO_UNLOCK_ZONE = 0xE,
    ZONE_NOT_OWNED_BY_THIS_NODE = 0xF,
    UNDO_ERROR_MAX
};

const ErrorCode UNDO_ERROR_OUT_OF_MEMORY =
    (ERROR_MODULE_UNDO | ERROR_SEVERITY_ERROR | UNDO_OUT_OF_MEMORY);
const ErrorCode UNDO_ERROR_CREATE_FILE =
    (ERROR_MODULE_UNDO | ERROR_SEVERITY_ERROR | UNDO_CREATE_FILE);
const ErrorCode UNDO_ERROR_INVALID_ZONE_ID =
    (ERROR_MODULE_UNDO | ERROR_SEVERITY_ERROR | UNDO_INVALID_ZONE_ID);
const ErrorCode UNDO_ERROR_INVALID_UNDO_TYPE =
    (ERROR_MODULE_UNDO | ERROR_SEVERITY_ERROR | UNDO_INVALID_UNDO_TYPE);
const ErrorCode UNDO_ERROR_CORRPUT_RECORD =
    (ERROR_MODULE_UNDO | ERROR_SEVERITY_ERROR | UNDO_CORRPUT_RECORD);
const ErrorCode UNDO_ERROR_INVALID_UNDO_RECORD_PTR =
    (ERROR_MODULE_UNDO | ERROR_SEVERITY_ERROR | UNDO_INVALID_UNDO_RECORD_PTR);
const ErrorCode UNDO_ERROR_INVALID_DATA =
    (ERROR_MODULE_UNDO | ERROR_SEVERITY_ERROR | UNDO_INVALID_DATA);
const ErrorCode UNDO_ERROR_RECORD_RECYCLED =
    (ERROR_MODULE_UNDO | ERROR_SEVERITY_ERROR | UNDO_RECORD_RECYCLED);
const ErrorCode UNDO_ERROR_NO_FREE_ZONE =
    (ERROR_MODULE_UNDO | ERROR_SEVERITY_ERROR | UNDO_NO_FREE_ZONE);
const ErrorCode UNDO_ERROR_ZONE_NOT_FREE =
    (ERROR_MODULE_UNDO | ERROR_SEVERITY_ERROR | UNDO_ZONE_NOT_FREE);
const ErrorCode UNDO_ERROR_TOO_MANY_UNDO_ZONES =
    (ERROR_MODULE_UNDO | ERROR_SEVERITY_ERROR | TOO_MANY_UNDO_ZONES);
const ErrorCode UNDO_ERROR_NOT_FULLY_INITED =
    (ERROR_MODULE_UNDO | ERROR_SEVERITY_ERROR | UNDO_NOT_FULLY_INITED);
const ErrorCode UNDO_ERROR_NO_ZONE_OWNER =
    (ERROR_MODULE_UNDO | ERROR_SEVERITY_ERROR | UNDO_NO_ZONE_OWNER);
const ErrorCode UNDO_ERROR_FAIL_TO_UNLOCK_ZONE =
    (ERROR_MODULE_UNDO | ERROR_SEVERITY_ERROR | FAIL_TO_UNLOCK_ZONE);
const ErrorCode UNDO_ERROR_ZONE_NOT_OWNED_BY_THIS_NODE =
    (ERROR_MODULE_UNDO | ERROR_SEVERITY_ERROR | ZONE_NOT_OWNED_BY_THIS_NODE);

} /* namespace DSTORE */

#endif
