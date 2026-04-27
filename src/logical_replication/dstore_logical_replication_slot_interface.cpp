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
 * dstore_logical_replication_slot_interface.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "logical_replication/dstore_logical_replication_slot_interface.h"
#include "logical_replication/dstore_logical_replication_mgr.h"

namespace LogicalReplicationSlotInterface {
using namespace DSTORE;

#ifdef ENABLE_LOGICAL_REPL
RetStatus CreateLogicalReplicationSlot(char *name, char *plugin,
    const std::function<RetStatus(const CommitSeqNo)> syncCatalogCallBack, PdbId pdbId)
{
    StoragePdb *pdb = g_storageInstance->GetPdb(pdbId);
    StorageAssert(pdb != nullptr && pdb->IsInit());
    LogicalReplicaMgr *logicalRepMgr = pdb->GetLogicalReplicaMgr();
    return logicalRepMgr->CreateLogicalReplicationSlot(name, plugin, syncCatalogCallBack);
}

RetStatus DropLogicalReplicationSlot(char* name, PdbId pdbId)
{
    StoragePdb *pdb = g_storageInstance->GetPdb(pdbId);
    StorageAssert(pdb != nullptr && pdb->IsInit());
    return pdb->GetLogicalReplicaMgr()->DropLogicalReplicationSlot(name);
}

LogicalReplicationSlot* AcquireLogicalReplicationSlot(char* name, PdbId pdbId)
{
    StoragePdb *pdb = g_storageInstance->GetPdb(pdbId);
    StorageAssert(pdb != nullptr && pdb->IsInit());
    return pdb->GetLogicalReplicaMgr()->AcquireLogicalReplicationSlot(name);
}

void ReleaseLogicalReplicationSlot(LogicalReplicationSlot* slot)
{
    StoragePdb *pdb = g_storageInstance->GetPdb(slot->GetPdbId());
    StorageAssert(pdb != nullptr && pdb->IsInit());
    pdb->GetLogicalReplicaMgr()->ReleaseLogicalReplicationSlot(slot);
}

RetStatus AdvanceLogicalReplicationSlot(LogicalReplicationSlot* slot, CommitSeqNo uptoCSN)
{
    StoragePdb *pdb = g_storageInstance->GetPdb(slot->GetPdbId());
    StorageAssert(pdb != nullptr && pdb->IsInit());
    LogicalReplicaMgr *logicalRepMgr = pdb->GetLogicalReplicaMgr();
    return logicalRepMgr->AdvanceLogicalReplicationSlot(slot, uptoCSN);
}

char* ReportLogicalReplicationSlot(char *name, PdbId pdbId)
{
    StoragePdb *pdb = g_storageInstance->GetPdb(pdbId);
    StorageAssert(pdb != nullptr && pdb->IsInit());
    LogicalReplicaMgr *logicalRepMgr = pdb->GetLogicalReplicaMgr();
    StringInfoData slotInfo;
    if (unlikely(!slotInfo.init())) {
        ErrLog(DSTORE_ERROR, MODULE_LOGICAL_REPLICATION, ErrMsg("StringInfoData alloc memory failed."));
        return nullptr;
    }
    logicalRepMgr->ReportLogicalReplicationSlot(name, &slotInfo);
    return slotInfo.data;
}

char* ReportLogicalReplicationSlot(PdbId pdbId)
{
    StoragePdb *pdb = g_storageInstance->GetPdb(pdbId);
    StorageAssert(pdb != nullptr && pdb->IsInit());
    LogicalReplicaMgr *logicalRepMgr = pdb->GetLogicalReplicaMgr();
    StringInfoData slotInfo;
    if (unlikely(!slotInfo.init())) {
        ErrLog(DSTORE_ERROR, MODULE_LOGICAL_REPLICATION, ErrMsg("StringInfoData alloc memory failed."));
        return nullptr;
    }
    logicalRepMgr->ReportLogicalReplicationSlot(nullptr, &slotInfo);
    return slotInfo.data;
}
#endif

}