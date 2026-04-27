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

#ifndef DSTORE_CONTROL_ERROR_CODE_MAP_H
#define DSTORE_CONTROL_ERROR_CODE_MAP_H

#include "errorcode/dstore_control_error_code.h"

namespace DSTORE {

static ErrorDetails g_control_error_code_map[] = {
    [STORAGE_OK] = {"STORAGE_OK", ""},
    [ERROR_GET_CODE(CONTROL_ERROR_INVALID_PARAMETER)] =
        {"CONTROL_ERROR_INVALID_PARAMETER", "The input parameter is invalid"},
    [ERROR_GET_CODE(CONTROL_ERROR_FILE_NOT_OPEN)] =
        {"CONTROL_ERROR_FILE_NOT_OPEN", "The file is not open"},
    [ERROR_GET_CODE(CONTROL_ERROR_NOT_INITIALIZED)] =
        {"CONTROL_ERROR_NOT_INITIALIZED", "The file is not initialized"},
    [ERROR_GET_CODE(CONTROL_ERROR_SECURE_FUNCTION_FAILED)] =
        {"CONTROL_ERROR_SECURE_FUNCTION_FAILED", "Call secure function failed"},
    [ERROR_GET_CODE(CONTROL_ERROR_HEADER_CHECK_FAILED)] =
        {"CONTROL_ERROR_HEADER_CHECK_FAILED", "Check the control file header failed"},
    [ERROR_GET_CODE(CONTROL_ERROR_INVALID_BLOCK_NUMBER)] =
        {"CONTROL_ERROR_INVALID_BLOCK_NUMBER", "The block number is invalid"},
    [ERROR_GET_CODE(CONTROL_ERROR_MEMORY_NOT_ENOUGH)] =
        {"CONTROL_ERROR_MEMORY_NOT_ENOUGH", "The memory is not enough"},
    [ERROR_GET_CODE(CONTROL_ERROR_ITEM_NOT_FIND)] =
        {"CONTROL_ERROR_ITEM_NOT_FIND", "The item is not found"},
    [ERROR_GET_CODE(CONTROL_ERROR_PDB_NOT_FOUND)] =
        {"CONTROL_ERROR_PDB_NOT_FOUND", "PDB not found"},
    [ERROR_GET_CODE(CONTROL_ERROR_PDB_ALLOC_ID_FAILED)] =
        {"CONTROL_ERROR_PDB_ALLOC_ID_FAILED", "Alloc PdbId failed"}
};

static_assert(sizeof(g_control_error_code_map) != CONTROL_ERROR_MAX, "Error code map size doesn't match");

} /* namespace DSTORE */

#endif
