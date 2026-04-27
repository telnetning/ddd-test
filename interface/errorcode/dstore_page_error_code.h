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

#ifndef DSTORE_PAGE_ERROR_CODE_H
#define DSTORE_PAGE_ERROR_CODE_H

#include "common/dstore_common_utils.h"
#include "framework/dstore_modules.h"
#include "errorcode/dstore_error_struct.h"

namespace DSTORE {

enum PageErrorReason : uint32_t {
    CORRUPT_PAGE = 0x1,
    SNAPSHOT_TOO_OLD = 0x2,
    TD_COUNT_LIMIT = 0x3,
    NO_SPACE_FOR_TD = 0x4,
    UNDO_TYPE_NOT_FOUND = 0x5,
    INSERT_UNDO_NOT_FOUND = 0x6,
    DELETE_UNDO_NOT_FOUND = 0x7,
    INVALID_INDEX_OFFNUM = 0x8,
    PAGE_ERROR_MAX
};

const ErrorCode PAGE_ERROR_CORRUPT_PAGE =
    (ERROR_MODULE_PAGE | ERROR_SEVERITY_ERROR | CORRUPT_PAGE);
const ErrorCode PAGE_ERROR_SNAPSHOT_TOO_OLD =
    (ERROR_MODULE_PAGE | ERROR_SEVERITY_ERROR | SNAPSHOT_TOO_OLD);
const ErrorCode PAGE_ERROR_TD_COUNT_LIMIT =
    (ERROR_MODULE_PAGE | ERROR_SEVERITY_ERROR | TD_COUNT_LIMIT);
const ErrorCode PAGE_ERROR_NO_SPACE_FOR_TD =
    (ERROR_MODULE_PAGE | ERROR_SEVERITY_ERROR | NO_SPACE_FOR_TD);
const ErrorCode PAGE_ERROR_UNDO_TYPE_NOT_FOUND =
    (ERROR_MODULE_PAGE | ERROR_SEVERITY_ERROR | UNDO_TYPE_NOT_FOUND);
const ErrorCode PAGE_ERROR_INSERT_UNDO_NOT_FOUND =
    (ERROR_MODULE_PAGE | ERROR_SEVERITY_ERROR | INSERT_UNDO_NOT_FOUND);
const ErrorCode PAGE_ERROR_DELETE_UNDO_NOT_FOUND =
    (ERROR_MODULE_PAGE | ERROR_SEVERITY_ERROR | DELETE_UNDO_NOT_FOUND);
const ErrorCode PAGE_ERROR_INVALID_INDEX_OFFNUM =
    (ERROR_MODULE_PAGE | ERROR_SEVERITY_ERROR | INVALID_INDEX_OFFNUM);

} /* namespace DSTORE */

#endif
