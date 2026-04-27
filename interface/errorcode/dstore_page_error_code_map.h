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

#ifndef DSTORE_PAGE_ERROR_CODE_MAP_H
#define DSTORE_PAGE_ERROR_CODE_MAP_H

#include "errorcode/dstore_page_error_code.h"

namespace DSTORE {

static ErrorDetails g_page_error_code_map[] = {
    [STORAGE_OK] = {"STORAGE_OK", ""},
    [ERROR_GET_CODE(PAGE_ERROR_CORRUPT_PAGE)] =
        {"PAGE_ERROR_CORRUPT_PAGE", "Corrupted Page"},
    [ERROR_GET_CODE(PAGE_ERROR_SNAPSHOT_TOO_OLD)] =
        {"PAGE_ERROR_SNAPSHOT_TOO_OLD", "Snapshot too old! The undo record has been discard."},
    [ERROR_GET_CODE(PAGE_ERROR_TD_COUNT_LIMIT)] =
        {"PAGE_ERROR_TD_COUNT_LIMIT", "Number of tds exceeds the limitation."},
    [ERROR_GET_CODE(PAGE_ERROR_NO_SPACE_FOR_TD)] =
        {"PAGE_ERROR_NO_SPACE_FOR_TD", "No space for new td."},
    [ERROR_GET_CODE(PAGE_ERROR_UNDO_TYPE_NOT_FOUND)] =
        {"PAGE_ERROR_UNDO_TYPE_NOT_FOUND", "This undo type is not found."},
    [ERROR_GET_CODE(PAGE_ERROR_INSERT_UNDO_NOT_FOUND)] =
        {"PAGE_ERROR_INSERT_UNDO_NOT_FOUND", "Can't find correct tuple offset when rollback insert."},
    [ERROR_GET_CODE(PAGE_ERROR_DELETE_UNDO_NOT_FOUND)] =
        {"PAGE_ERROR_DELETE_UNDO_NOT_FOUND", "Can't find correct tuple offset when rollback delete."},
    [ERROR_GET_CODE(PAGE_ERROR_INVALID_INDEX_OFFNUM)] =
        {"PAGE_ERROR_INVALID_INDEX_OFFNUM", "invalid index offnum: %u"},
};

static_assert(sizeof(g_page_error_code_map) != PAGE_ERROR_MAX, "Error code map size doesn't match");

} /* namespace DSTORE */

#endif
