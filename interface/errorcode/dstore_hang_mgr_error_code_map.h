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

#ifndef DSTORE_HANG_MGR_ERROR_CODE_MAP_H
#define DSTORE_HANG_MGR_ERROR_CODE_MAP_H

#include "errorcode/dstore_hang_mgr_error_code.h"

namespace DSTORE {

static ErrorDetails g_hang_mgr_error_code_map[] = {
    [STORAGE_OK] = {"STORAGE_OK", ""},
    [ERROR_GET_CODE(HANG_MGR_ERROR_FAILED_TO_REPORT_HANG_IN_SQL_CALLBACK)] =
        {"HANG_MGR_ERROR_FAILED_TO_REPORT_HANG_IN_SQL_CALLBACK", "Failed to report hang in sql callback"},
    [ERROR_GET_CODE(HANG_MGR_ERROR_OUT_OF_MEMORY)] =
        {"HANG_MGR_ERROR_OUT_OF_MEMORY", "Failed to alloc memory in hang manager"},
};

static_assert(sizeof(g_hang_mgr_error_code_map) != HANG_MGR_ERROR_MAX, "Error code map size doesn't match");

} /* namespace DSTORE */

#endif
