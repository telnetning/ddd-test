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

#ifndef DSTORE_CATALOG_ERROR_CODE_H
#define DSTORE_CATALOG_ERROR_CODE_H

#include "common/dstore_common_utils.h"
#include "framework/dstore_modules.h"
#include "errorcode/dstore_error_struct.h"

namespace DSTORE {

enum CatalogErrorReason : uint32_t {
    UNKOWN_ATT_ALIGN = 0x1,
    CLEAN_PDB_CONN_FAIL = 0x2,
    DESTROY_PDB_CACHE_FAIL = 0x3,
    CATALOG_ERROR_MAX
};

const ErrorCode CATALOG_ERROR_UNKOWN_ATT_ALIGN =
    (ERROR_MODULE_CATALOG | ERROR_SEVERITY_ERROR | UNKOWN_ATT_ALIGN);
const ErrorCode CATALOG_ERROR_CLEAN_PDB_CONN_FAIL =
    (ERROR_MODULE_CATALOG | ERROR_SEVERITY_ERROR | CLEAN_PDB_CONN_FAIL);
const ErrorCode CATALOG_ERROR_DESTROY_PDB_CACHE_FAIL =
    (ERROR_MODULE_CATALOG | ERROR_SEVERITY_ERROR | DESTROY_PDB_CACHE_FAIL);

} /* namespace DSTORE */

#endif
