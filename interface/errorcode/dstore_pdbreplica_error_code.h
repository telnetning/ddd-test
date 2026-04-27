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

#ifndef DSTORE_PDBREPLICA_ERROR_CODE_H
#define DSTORE_PDBREPLICA_ERROR_CODE_H

#include "common/dstore_common_utils.h"
#include "framework/dstore_modules.h"
#include "errorcode/dstore_error_struct.h"

namespace DSTORE {

enum PdbReplicaErrorReason : uint32_t {
    PDB_REPLICA_NO_FOUND_PRIMARY_PDB = 0x1,
    PDB_REPLICA_ERROR_MAX
};

const ErrorCode PDB_REPLICA_ERROR_NO_FOUND_PRIMARY_PDB =
    (ERROR_MODULE_PDBREPLICA | ERROR_SEVERITY_ERROR | PDB_REPLICA_NO_FOUND_PRIMARY_PDB);

} /* namespace DSTORE */

#endif
