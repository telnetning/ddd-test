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
 *
 * ---------------------------------------------------------------------------------------
 *
 * dstore_pdb_interface.h
 *     This file defines the building blocks that bg page writer needs.
 *
 * IDENTIFICATION
 *        interface/pdb/dstore_pdb_interface.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_PDB_INTERFACE_H
#define DSTORE_PDB_INTERFACE_H

#include "common/dstore_common_utils.h"
#include "page/dstore_page_struct.h"
namespace DSTORE {
#pragma pack(push)
#pragma pack(1)
constexpr uint16_t DB_NAME_LEN = 256;
enum class RelMapType {
    RELMAP_SHARED = 0,
    RELMAP_LOCAL,
    RELMAP_MAX
};
struct RelMapNode {
    Oid relid;
    PageId segid;
};
#pragma pack(pop)
}  // namespace DSTORE
namespace StoragePdbInterface {
#pragma GCC visibility push(default)

DSTORE::RetStatus SwitchContextToTargetPdb(DSTORE::PdbId pdbId);
void GetPdbPath(DSTORE::PdbId pdbId, char *pdbPath);
DSTORE::RetStatus Checkpoint(DSTORE::PdbId pdbId);
void FlushAllDirtyPages(DSTORE::PdbId pdbId = DSTORE::INVALID_PDB_ID);
/**
 * This function is used to obtain the built-in relation-segment mapping of the PDB.
 *
 * @param type  Relation-segment mapping type
 * @param pdbid pdb id
 * @param nodes Relation-segment mapping nodes.
 * @param count Node count
 * @return Return status
 */
DSTORE::RetStatus GetBuiltinRelMap(DSTORE::RelMapType type, DSTORE::PdbId pdbid, DSTORE::RelMapNode *nodes, int *count);

/**
* Write the built-in relation mapping
* @param type  Relation-segment mapping type
* @param pdbid pdb id
* @param nodes Array of relation-segment mapping nodes
* @param count Number of nodes
* @return Writing status, success or failure
*
* This function is used to write the built-in relation-segment mapping into the control file.
*/
DSTORE::RetStatus WriteBuiltinRelMap(DSTORE::RelMapType type, DSTORE::PdbId pdbid, DSTORE::RelMapNode *nodes,
                                     int count);

bool waitAllRollbackTaskFinished(DSTORE::PdbId pdbid);
#pragma GCC visibility pop
} /* namespace StoragePdbInterface */
#endif
