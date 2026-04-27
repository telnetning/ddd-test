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

#ifndef DSTORE_SYSTABLE_ERROR_CODE_MAP_H
#define DSTORE_SYSTABLE_ERROR_CODE_MAP_H

#include "errorcode/dstore_systable_error_code.h"

namespace DSTORE {

static ErrorDetails g_systable_error_code_map[] = {
    [STORAGE_OK] = {"STORAGE_OK", ""},
    [ERROR_GET_CODE(SYSTABLE_ERROR_BOOTSTRAP_TALBE_EXISTS)] =
        {"SYSTABLE_ERROR_BOOTSTRAP_TALBE_EXISTS", "Bootstrap table with oid[%d] is already exists."}
};

static_assert(sizeof(g_systable_error_code_map) != SYSTABLE_ERROR_MAX, "Error code map size doesn't match");

} /* namespace DSTORE */

#endif
