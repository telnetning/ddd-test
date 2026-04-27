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

#ifndef DSTORE_TUPLE_ERROR_CODE_H
#define DSTORE_TUPLE_ERROR_CODE_H

#include "common/dstore_common_utils.h"
#include "framework/dstore_modules.h"
#include "errorcode/dstore_error_struct.h"

namespace DSTORE {

enum TupleErrorReason : uint32_t {
    NATTRS_EXCEEDS_LIMIT = 0x1,
    UNDEFINED_COLUMN = 0x2,
    TOO_MANY_COLUMNS = 0x3,
    CORRUPTED_PAGE_POINTERS = 0x4,
    CORRUPTED_LINE_POINTER = 0x5,
    TUPLE_TOO_BIG = 0x6,
    INSERT_EXCEED_FILLFACTOR = 0x7,
    TUPLE_ERROR_MAX
};

const ErrorCode TUPLE_ERROR_NATTRS_EXCEEDS_LIMIT =
    (ERROR_MODULE_TUPLE | ERROR_SEVERITY_ERROR | NATTRS_EXCEEDS_LIMIT);
const ErrorCode TUPLE_ERROR_UNDEFINED_COLUMN =
    (ERROR_MODULE_TUPLE | ERROR_SEVERITY_ERROR | UNDEFINED_COLUMN);
const ErrorCode TUPLE_ERROR_TOO_MANY_COLUMNS =
    (ERROR_MODULE_TUPLE | ERROR_SEVERITY_ERROR | TOO_MANY_COLUMNS);
const ErrorCode TUPLE_ERROR_CORRUPTED_PAGE_POINTERS =
    (ERROR_MODULE_TUPLE | ERROR_SEVERITY_ERROR | CORRUPTED_PAGE_POINTERS);
const ErrorCode TUPLE_ERROR_CORRUPTED_LINE_POINTER =
    (ERROR_MODULE_TUPLE | ERROR_SEVERITY_ERROR | CORRUPTED_LINE_POINTER);
const ErrorCode TUPLE_ERROR_TUPLE_TOO_BIG =
    (ERROR_MODULE_TUPLE | ERROR_SEVERITY_ERROR | TUPLE_TOO_BIG);
const ErrorCode TUPLE_WARNING_INSERT_EXCEED_FILLFACTOR =
    (ERROR_MODULE_TUPLE | ERROR_SEVERITY_WARNING | INSERT_EXCEED_FILLFACTOR);

} /* namespace DSTORE */

#endif
