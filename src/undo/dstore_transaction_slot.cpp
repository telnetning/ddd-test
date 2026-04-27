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
 * dstore_transaction_slot.cpp
 *
 *
 * IDENTIFICATION
 *        src/undo/dstore_transaction_slot.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "undo/dstore_transaction_slot.h"
#include "framework/dstore_instance.h"
#include "transaction/dstore_csn_mgr.h"
#include "wal/dstore_wal.h"
#include "wal/dstore_wal_struct.h"

namespace DSTORE {

void TransactionSlot::Init(uint64 tmpLogicSlotId)
{
    curTailUndoPtr = INVALID_UNDO_RECORD_PTR.m_placeHolder;
    spaceTailUndoPtr = INVALID_UNDO_RECORD_PTR.m_placeHolder;
    status = TXN_STATUS_IN_PROGRESS;
    logicSlotId = tmpLogicSlotId;
    csn = INVALID_CSN;
    walId = INVALID_WAL_ID;
    commitEndPlsn = INVALID_PLSN;
}

bool TransactionSlot::IsCommitWalPersist(PdbId pdbId)
{
    StoragePdb* pdb = g_storageInstance->GetPdb(pdbId);
    StorageReleasePanic(pdb == nullptr, MODULE_UNDO, ErrMsg("Pdb[%u] is nullptr!", pdbId));
    if (pdb->IsInit()) {
        WalStreamManager *walStreamMgr =
            g_storageInstance->GetPdb(pdbId)->GetWalMgr()->GetWalStreamManager();
        if (g_storageInstance->GetPdb(pdbId)->GetPdbRoleMode() == PdbRoleMode::PDB_PRIMARY &&
            walStreamMgr->IsSelfWritingWalStream(walId) && walStreamMgr->GetWritingWalStream() != nullptr &&
            commitEndPlsn > walStreamMgr->GetWritingWalStream()->GetMaxFlushedPlsn()) {
            return false;
        }
    }
#ifndef UT
    if (pdb->GetPdbRoleMode() == PdbRoleMode::PDB_PRIMARY) {
        CsnMgr *csnMgr = g_storageInstance->GetCsnMgr();
        csnMgr->WaitUpperBoundSatisfy(csn);
    }
#endif
    return true;
}

void TransactionSlot::Dump(StringInfo str)
{
    str->append("slotId is %lu\n", logicSlotId);
    str->append("status is %d, csn is %lu\n", static_cast<int>(status), csn);
    str->append("curTailUndoPtr(%hu, %u, %hu), spaceTailUndoPtr(%hu, %u, %hu).\n",
                static_cast<UndoRecPtr>(curTailUndoPtr).GetPageId().m_fileId,
                static_cast<UndoRecPtr>(curTailUndoPtr).GetPageId().m_blockId,
                static_cast<UndoRecPtr>(curTailUndoPtr).GetOffset(),
                static_cast<UndoRecPtr>(spaceTailUndoPtr).GetPageId().m_fileId,
                static_cast<UndoRecPtr>(spaceTailUndoPtr).GetPageId().m_blockId,
                static_cast<UndoRecPtr>(spaceTailUndoPtr).GetOffset());
}

}

