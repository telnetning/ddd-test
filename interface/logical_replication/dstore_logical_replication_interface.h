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
 * logical_replication_interface.h
 *
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/dstore/interface/logical_replication/logical_replication_interface.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef LOGICAL_REPLICATION_INTERFACE_H
#define LOGICAL_REPLICATION_INTERFACE_H

#include "common/dstore_common_utils.h"
#include "dstore_logical_replication_slot_interface.h"
#include "dstore_logical_replication_struct.h"

namespace DSTORE {
    class LogicalReplicationSlot;
    class LogicalDecodeHandler;
}

namespace LogicalReplicationInterface {
#pragma GCC visibility push(default)

#ifdef ENABLE_LOGICAL_REPL
DSTORE::RetStatus SynchronizeCatalog(DSTORE::CatalogInfo *rawInfo);

DSTORE::LogicalDecodeHandler *CreateLogicalDecodeHandler(DSTORE::LogicalReplicationSlot* logicalSlot,
                                                         DSTORE::DecodeOptions *opts);

void DeleteLogicalDecodeHandler(DSTORE::LogicalDecodeHandler *decodeContext);

void StartupLogicalReplication(DSTORE::LogicalDecodeHandler *decodeContext);

void StopLogicalReplication(DSTORE::LogicalDecodeHandler *decodeContext);

DSTORE::TrxLogicalLog* GetNextTrxLogicalLog(DSTORE::LogicalDecodeHandler *decodeContext);

void ConfirmTrxLogicalLog(DSTORE::LogicalDecodeHandler *decodeContext, DSTORE::TrxLogicalLog *trxLog);
#endif

#pragma GCC visibility pop

}
#endif