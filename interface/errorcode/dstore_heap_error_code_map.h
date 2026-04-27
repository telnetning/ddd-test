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

#ifndef DSTORE_HEAP_ERROR_CODE_MAP_H
#define DSTORE_HEAP_ERROR_CODE_MAP_H

#include "errorcode/dstore_heap_error_code.h"

namespace DSTORE {

static ErrorDetails g_heap_error_code_map[] = {
    [STORAGE_OK] = {"STORAGE_OK", ""},
    [ERROR_GET_CODE(HEAP_ERROR_TUPLE_IS_CHANGED)] =
        {"HEAP_ERROR_TUPLE_IS_CHANGED", "The tuple has been changed."},
    [ERROR_GET_CODE(HEAP_ERROR_GET_BUFFER_FAILED)] =
        {"HEAP_ERROR_GET_BUFFER_FAILED", "Failed to get buffer."},
    [ERROR_GET_CODE(HEAP_ERROR_INVALID_SEGMENT)] =
        {"HEAP_ERROR_INVALID_SEGMENT", "The segment is invalid."},
    [ERROR_GET_CODE(HEAP_ERROR_FETCH_UNDO_RECORD_FAILED)] =
        {"HEAP_ERROR_FETCH_UNDO_RECORD_FAILED", "Failed to fetch undo record."},
    [ERROR_GET_CODE(HEAP_INFO_ALLOC_TD_FAILED)] =
        {"HEAP_INFO_ALLOC_TD_FAILED", "Failed to alloc td."},
    [ERROR_GET_CODE(HEAP_INFO_PAGE_NOT_ENOUGH_SPACE)] =
        {"HEAP_INFO_PAGE_NOT_ENOUGH_SPACE", "Page doesn't have enough space."},
    [ERROR_GET_CODE(HEAP_INFO_DO_LOCK_ALLOC_TD_FAILED)] =
        {"HEAP_INFO_DO_LOCK_ALLOC_TD_FAILED", "Failed to alloc td while locking a tuple."},
    [ERROR_GET_CODE(HEAP_ERROR_INPUT_PARAM_WRONG)] =
        {"HEAP_ERROR_INPUT_PARAM_WRONG", "Heap input param is wrong."},
    [ERROR_GET_CODE(HEAP_ERROR_TUPLE_IS_DELETED)] =
        {"HEAP_ERROR_TUPLE_IS_DELETED", "Heap tuple is deleted."},
    [ERROR_GET_CODE(HEAP_ERROR_FAIL_CREATE_TABLE_SMGR)] =
        {"HEAP_ERROR_FAIL_CREATE_TABLE_SMGR", "failed to create/init table segment. segment id is {%d, %u}."},
    [ERROR_GET_CODE(HEAP_ERROR_GET_PAGE_FREESPACE_INVALID_PAGEID)] =
        {"HEAP_ERROR_GET_PAGE_FREESPACE_INVALID_PAGEID", "Parameter is invalid."},
    [ERROR_GET_CODE(HEAP_ERROR_ROLLBACK_BIG_TUPLE_FAILED)] =
        {"HEAP_ERROR_ROLLBACK_BIG_TUPLE_FAILED", "failed to rollback big tuple"},
    [ERROR_GET_CODE(HEAP_ERROR_LOCK_TUPLE_NO_WAIT)] =
        {"HEAP_ERROR_LOCK_TUPLE_NO_WAIT", "failed to lock tuple using no wait method"},
    [ERROR_GET_CODE(HEAP_INFO_LOCK_TUPLE_SKIP_LOCKED)] =
        {"HEAP_INFO_LOCK_TUPLE_SKIP_LOCKED", "failed to lock tuple using skip locked method"}
};

static_assert(sizeof(g_heap_error_code_map) != HEAP_ERROR_MAX, "Error code map size doesn't match");

} /* namespace DSTORE */

#endif
