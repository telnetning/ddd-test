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
#include "diagnose/dstore_checkpointer_diagnose.h"
#include "common/algorithm/dstore_string_info.h"
#include "framework/dstore_pdb.h"
#include "framework/dstore_instance.h"
#include "buffer/dstore_checkpointer.h"

namespace DSTORE {

CheckpointerDiagnose::CheckpointerDiagnose(uint32 pdbId)
{
    m_pdbId = pdbId;
    StoragePdb *storagePdb = g_storageInstance->GetPdb(pdbId);
    if (STORAGE_VAR_NULL(storagePdb)) {
        ErrLog(DSTORE_ERROR, MODULE_BUFFER, ErrMsg("StoragePdb is nullptr."));
        return;
    }
    m_checkpointMgr = storagePdb->GetCheckpointMgr();
}

char *CheckpointerDiagnose::GetLocalCheckpointStatInfo()
{
    StringInfoData dumpInfo;
    if (unlikely(!dumpInfo.init())) {
        return nullptr;
    }
    if (!m_checkpointMgr) {
        dumpInfo.append("PDB:%u checkpointer has not inited\n", m_pdbId);
        return dumpInfo.data;
    }

    CheckpointStatInfo ckptInfo;
    RetStatus rc = m_checkpointMgr->GetCheckpointStatInfo(ckptInfo);
    if (rc == DSTORE_FAIL && ckptInfo.pdbId == INVALID_PDB_ID) {
        dumpInfo.append("PDB:%u checkpointer has not inited\n", m_pdbId);
    } else if (rc == DSTORE_FAIL) {
        dumpInfo.append("PDB:%u checkpointPid:%lu lastRequestTime:%ld requestedCheckpoints:%u\n",
            ckptInfo.pdbId, ckptInfo.checkpointPid, ckptInfo.lastRequestTime, ckptInfo.requestedCheckpoints);
        dumpInfo.append("PDB:%u failed to alloc memory for wal ckpt stat info\n", m_pdbId);
    } else {
        dumpInfo.append("PDB:%u checkpointPid:%lu lastRequestTime:%ld requestedCheckpoints:%u\n",
            ckptInfo.pdbId, ckptInfo.checkpointPid, ckptInfo.lastRequestTime, ckptInfo.requestedCheckpoints);
        DumpAllWalSteamCkptInfo(dumpInfo, ckptInfo.walStreamNum, ckptInfo.walCkptStatInfo);
    }
    return dumpInfo.data;
}

char *CheckpointerDiagnose::GetGlobalCheckpointStatInfo()
{
    StringInfoData dumpInfo;
    if (unlikely(!dumpInfo.init())) {
        return nullptr;
    }
    if (g_storageInstance->GetType() != StorageInstanceType::DISTRIBUTE_COMPUTE) {
        dumpInfo.append("The function is not applicable to single mode\n");
        return dumpInfo.data;
    }
    if (!m_checkpointMgr) {
        dumpInfo.append("PDB:%u checkpointer has not inited\n", m_pdbId);
        return dumpInfo.data;
    }
    if (unlikely(g_storageInstance->GetPdb(m_pdbId) == nullptr)) {
        dumpInfo.append("PDB:%u pdb is nullptr or recoveryRpcMgr has not inited\n", m_pdbId);
        return dumpInfo.data;
    }

    return dumpInfo.data;
}

}  // namespace DSTORE
