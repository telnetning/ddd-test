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

#ifndef DSTORE_HEAP_ERROR_CODE_H
#define DSTORE_HEAP_ERROR_CODE_H

#include "common/dstore_common_utils.h"
#include "framework/dstore_modules.h"
#include "errorcode/dstore_error_struct.h"

namespace DSTORE {

enum HeapErrorReason : uint32_t {
    TUPLE_IS_CHANGED = 0x1,
    GET_BUFFER_FAILED = 0x2,
    HEAP_INVALID_SEGMENT = 0x3,
    FETCH_UNDO_RECORD_FAILED = 0x4,
    ALLOC_TD_FAILED = 0x5,
    PAGE_NOT_ENOUGH_SPACE = 0x6,
    DO_LOCK_ALLOC_TD_FAILED = 0x7,
    HEAP_INPUT_PARAM_WRONG = 0x8,
    TUPLE_IS_DELETED = 0x9,
    FAIL_CREATE_TABLE_SMGR = 0xA,
    GET_PAGE_FREESPACE_INVALID_PAGEID = 0xB,
    ROLLBACK_BIG_TUPLE_FAILED = 0xC,
    LOCK_TUPLE_NO_WAIT = 0xD,
    LOCK_TUPLE_SKIP_LOCKED = 0xE,
    HEAP_ERROR_MAX
};

const ErrorCode HEAP_ERROR_TUPLE_IS_CHANGED =
    (ERROR_MODULE_HEAP | ERROR_SEVERITY_ERROR | TUPLE_IS_CHANGED);
const ErrorCode HEAP_ERROR_GET_BUFFER_FAILED =
    (ERROR_MODULE_HEAP | ERROR_SEVERITY_ERROR | GET_BUFFER_FAILED);
const ErrorCode HEAP_ERROR_INVALID_SEGMENT =
    (ERROR_MODULE_HEAP | ERROR_SEVERITY_ERROR | HEAP_INVALID_SEGMENT);
const ErrorCode HEAP_ERROR_FETCH_UNDO_RECORD_FAILED =
    (ERROR_MODULE_HEAP | ERROR_SEVERITY_ERROR | FETCH_UNDO_RECORD_FAILED);
const ErrorCode HEAP_INFO_ALLOC_TD_FAILED =
    (ERROR_MODULE_HEAP | ERROR_SEVERITY_INFO | ALLOC_TD_FAILED);
const ErrorCode HEAP_INFO_PAGE_NOT_ENOUGH_SPACE =
    (ERROR_MODULE_HEAP | ERROR_SEVERITY_INFO | PAGE_NOT_ENOUGH_SPACE);
const ErrorCode HEAP_INFO_DO_LOCK_ALLOC_TD_FAILED =
    (ERROR_MODULE_HEAP | ERROR_SEVERITY_INFO | DO_LOCK_ALLOC_TD_FAILED);
const ErrorCode HEAP_ERROR_INPUT_PARAM_WRONG =
    (ERROR_MODULE_HEAP | ERROR_SEVERITY_ERROR | HEAP_INPUT_PARAM_WRONG);
const ErrorCode HEAP_ERROR_TUPLE_IS_DELETED =
    (ERROR_MODULE_HEAP | ERROR_SEVERITY_ERROR | TUPLE_IS_DELETED);
const ErrorCode HEAP_ERROR_FAIL_CREATE_TABLE_SMGR =
    (ERROR_MODULE_HEAP | ERROR_SEVERITY_ERROR | FAIL_CREATE_TABLE_SMGR);
const ErrorCode HEAP_ERROR_GET_PAGE_FREESPACE_INVALID_PAGEID =
    (ERROR_MODULE_HEAP | ERROR_SEVERITY_ERROR | GET_PAGE_FREESPACE_INVALID_PAGEID);
const ErrorCode HEAP_ERROR_ROLLBACK_BIG_TUPLE_FAILED =
    (ERROR_MODULE_HEAP | ERROR_SEVERITY_ERROR | ROLLBACK_BIG_TUPLE_FAILED);
const ErrorCode HEAP_ERROR_LOCK_TUPLE_NO_WAIT =
    (ERROR_MODULE_HEAP | ERROR_SEVERITY_ERROR | LOCK_TUPLE_NO_WAIT);
const ErrorCode HEAP_INFO_LOCK_TUPLE_SKIP_LOCKED =
    (ERROR_MODULE_HEAP | ERROR_SEVERITY_INFO | LOCK_TUPLE_SKIP_LOCKED);

} /* namespace DSTORE */

#endif
