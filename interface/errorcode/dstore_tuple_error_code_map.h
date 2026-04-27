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

#ifndef DSTORE_TUPLE_ERROR_CODE_MAP_H
#define DSTORE_TUPLE_ERROR_CODE_MAP_H

#include "errorcode/dstore_tuple_error_code.h"

namespace DSTORE {

static ErrorDetails g_tuple_error_code_map[] = {
    [STORAGE_OK] = {"STORAGE_OK", ""},
    [ERROR_GET_CODE(TUPLE_ERROR_NATTRS_EXCEEDS_LIMIT)] =
        {"TUPLE_ERROR_NATTRS_EXCEEDS_LIMIT", "Number of attributes exceeds the limit of table column number."},
    [ERROR_GET_CODE(TUPLE_ERROR_UNDEFINED_COLUMN)] =
        {"TUPLE_ERROR_UNDEFINED_COLUMN", "invalid attnum: %d"},
    [ERROR_GET_CODE(TUPLE_ERROR_TOO_MANY_COLUMNS)] =
        {"TUPLE_ERROR_TOO_MANY_COLUMNS", "number of index columns (%d) exceeds limit (%d)"},
    [ERROR_GET_CODE(TUPLE_ERROR_CORRUPTED_PAGE_POINTERS)] =
        {"TUPLE_ERROR_CORRUPTED_PAGE_POINTERS", "corrupted page pointers: lower = %u, upper = %u, special = %u"},
    [ERROR_GET_CODE(TUPLE_ERROR_CORRUPTED_LINE_POINTER)] =
        {"TUPLE_ERROR_CORRUPTED_LINE_POINTER", "corrupted line pointer: offset = %u, size = %u"},
    [ERROR_GET_CODE(TUPLE_ERROR_TUPLE_TOO_BIG)] =
        {"TUPLE_ERROR_TUPLE_TOO_BIG", "The tuple is too big: size %zu, maximum size %zu"},
    [ERROR_GET_CODE(TUPLE_WARNING_INSERT_EXCEED_FILLFACTOR)] =
        {"TUPLE_WARNING_INSERT_EXCEED_FILLFACTOR", "The tuple inserted exceeding the fillfactor threshold: size %zu"},
};

static_assert(sizeof(g_tuple_error_code_map) != TUPLE_ERROR_MAX, "Error code map size doesn't match");

} /* namespace DSTORE */

#endif
