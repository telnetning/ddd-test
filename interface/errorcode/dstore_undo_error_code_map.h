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

#ifndef DSTORE_UNDO_ERROR_CODE_MAP_H
#define DSTORE_UNDO_ERROR_CODE_MAP_H

#include "errorcode/dstore_undo_error_code.h"

namespace DSTORE {

static ErrorDetails g_undo_error_code_map[] = {
    [STORAGE_OK] = {"STORAGE_OK", ""},
    [ERROR_GET_CODE(UNDO_ERROR_OUT_OF_MEMORY)] =
        {"UNDO_ERROR_OUT_OF_MEMORY", "Out of memory"},
    [ERROR_GET_CODE(UNDO_ERROR_CREATE_FILE)] =
        {"UNDO_ERROR_CREATE_FILE", "%s cannot be created"},
    [ERROR_GET_CODE(UNDO_ERROR_INVALID_ZONE_ID)] =
        {"UNDO_ERROR_INVALID_ZONE_ID", "Invalid zone id %d"},
    [ERROR_GET_CODE(UNDO_ERROR_INVALID_UNDO_TYPE)] =
        {"UNDO_ERROR_INVALID_UNDO_TYPE", "Invalid undo type %d"},
    [ERROR_GET_CODE(UNDO_ERROR_CORRPUT_RECORD)] =
        {"UNDO_ERROR_CORRPUT_RECORD", "Corrupt undo record"},
    [ERROR_GET_CODE(UNDO_ERROR_INVALID_UNDO_RECORD_PTR)] =
        {"UNDO_ERROR_INVALID_UNDO_RECORD_PTR", "Invalid undo record pointer 0x%016lx"},
    [ERROR_GET_CODE(UNDO_ERROR_INVALID_DATA)] =
        {"UNDO_ERROR_INVALID_DATA", "Data provided is invalid."},
    [ERROR_GET_CODE(UNDO_ERROR_RECORD_RECYCLED)] =
        {"UNDO_ERROR_RECORD_RECYCLED", "Undo record (0x%016lx) has been recycled"},
    [ERROR_GET_CODE(UNDO_ERROR_NO_FREE_ZONE)] =
        {"UNDO_ERROR_NO_FREE_ZONE", "There is no free zone available."},
    [ERROR_GET_CODE(UNDO_ERROR_ZONE_NOT_FREE)] =
        {"UNDO_ERROR_ZONE_NOT_FREE", "Zone (zid=%d) is not free"},
    [ERROR_GET_CODE(UNDO_ERROR_TOO_MANY_UNDO_ZONES)] =
        {"UNDO_ERROR_TOO_MANY_UNDO_ZONES", "Too many undo zones are requested, max count is %d, now is %d"},
    [ERROR_GET_CODE(UNDO_ERROR_NOT_FULLY_INITED)] =
        {"UNDO_ERROR_NOT_FULLY_INITED", "UndoMgr is not fully inited."},
    [ERROR_GET_CODE(UNDO_ERROR_NO_ZONE_OWNER)] =
        {"UNDO_ERROR_NO_ZONE_OWNER", "There is no zone owner (zid=0x%05x)"},
    [ERROR_GET_CODE(UNDO_ERROR_FAIL_TO_UNLOCK_ZONE)] =
        {"UNDO_ERROR_FAIL_TO_UNLOCK_ZONE", "Failed to unlock zone (zid=0x%05x)"},
    [ERROR_GET_CODE(UNDO_ERROR_ZONE_NOT_OWNED_BY_THIS_NODE)] =
        {"UNDO_ERROR_ZONE_NOT_OWNED_BY_THIS_NODE", "This zone (zid=0x%05x) is not owned by this node"},
};

static_assert(sizeof(g_undo_error_code_map) != UNDO_ERROR_MAX, "Error code map size doesn't match");

} /* namespace DSTORE */

#endif
