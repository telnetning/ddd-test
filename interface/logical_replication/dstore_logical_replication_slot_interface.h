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
 * logical_replication_slot_interface.h
 *
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/dstore/interface/logical_replication/logical_replication_slot_interface.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef LOGICAL_REPLICATION_SLOT_INTERFACE_H
#define LOGICAL_REPLICATION_SLOT_INTERFACE_H

#include <functional>
#include "common/dstore_common_utils.h"

namespace DSTORE {
class LogicalReplicationSlot;
}


#ifdef UT
#ifndef ENABLE_LOGICAL_REPL
/* LOGICAL REPL is not supported for now. */
#define ENABLE_LOGICAL_REPL
#endif
#endif

namespace LogicalReplicationSlotInterface {
#pragma GCC visibility push(default)
#ifdef ENABLE_LOGICAL_REPL
DSTORE::RetStatus CreateLogicalReplicationSlot(char *name, char *plugin,
    std::function<DSTORE::RetStatus(DSTORE::CommitSeqNo)> syncCatalogCallBack, DSTORE::PdbId pdbId);

DSTORE::RetStatus DropLogicalReplicationSlot(char *name, DSTORE::PdbId pdbId);

DSTORE::LogicalReplicationSlot* AcquireLogicalReplicationSlot(char *name, DSTORE::PdbId pdbId);

void ReleaseLogicalReplicationSlot(DSTORE::LogicalReplicationSlot *slot);

DSTORE::RetStatus AdvanceLogicalReplicationSlot(DSTORE::LogicalReplicationSlot* slot, DSTORE::CommitSeqNo uptoCSN);

char* ReportLogicalReplicationSlot(char *name, DSTORE::PdbId pdbId);

char* ReportLogicalReplicationSlot(DSTORE::PdbId pdbId);

#endif
#pragma GCC visibility pop
}
#endif