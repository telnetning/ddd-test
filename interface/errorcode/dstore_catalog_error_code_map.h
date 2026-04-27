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

#ifndef DSTORE_CATALOG_ERROR_CODE_MAP_H
#define DSTORE_CATALOG_ERROR_CODE_MAP_H

#include "errorcode/dstore_catalog_error_code.h"

namespace DSTORE {

static ErrorDetails g_catalog_error_code_map[] = {
    [STORAGE_OK] = {"STORAGE_OK", ""},
    [ERROR_GET_CODE(CATALOG_ERROR_UNKOWN_ATT_ALIGN)] =
        {"CATALOG_ERROR_UNKOWN_ATT_ALIGN", "unkown attribute align type."},
    [ERROR_GET_CODE(CATALOG_ERROR_CLEAN_PDB_CONN_FAIL)] =
        {"CATALOG_ERROR_CLEAN_PDB_CONN_FAIL", "failed to clean pdb connection."},
    [ERROR_GET_CODE(CATALOG_ERROR_DESTROY_PDB_CACHE_FAIL)] =
        {"CATALOG_ERROR_DESTROY_PDB_CACHE_FAIL", "failed to destroy pdb cache."},
};

static_assert(sizeof(g_catalog_error_code_map) != CATALOG_ERROR_MAX, "Error code map size doesn't match");

} /* namespace DSTORE */

#endif
