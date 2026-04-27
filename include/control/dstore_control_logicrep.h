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
 * dstore_control_logicrep.h
 *  Record  in the control file..
 *
 *
 * IDENTIFICATION
 *        dstore/include/control/dstore_control_logicrep.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_CONTROL_LOGICREP_H
#define DSTORE_CONTROL_LOGICREP_H
#include "control/dstore_control_struct.h"
#include "common/algorithm/dstore_string_info.h"
#include "control/dstore_control_file_mgr.h"
#include "common/dstore_common_utils.h"
#include "control/dstore_control_group.h"
namespace DSTORE {
struct ControlLogicalReplicationSlotPageItemData {
    DstoreNameData name;
    DstoreNameData plugin;
    WalId walId;
    WalPlsn restartPlsn;
    CommitSeqNo catalogCsnMin;
    CommitSeqNo decodeDictCsnMin;
    CommitSeqNo confirmedCsn;
    bool isPersistent;
    bool isConsistent;

    static void Dump(void* item, StringInfoData& dumpInfo)
    {
        auto data = static_cast<ControlLogicalReplicationSlotPageItemData *>(item);
        dumpInfo.append("slotName %s, walId %lu, catalogCsnMin %lu, restartPlsn %lu, confirmedCsn %lu, "
                        "plugin name %s, isPersistent %s, consistent %s.\n", data->name.data,
                        data->walId, data->catalogCsnMin, data->restartPlsn, data->confirmedCsn,
                        data->plugin.data, data->isPersistent ? "PERSISTENT" : "NOT PERSISTENT",
                        data->isConsistent ? "CONSISTENT" : "NOT CONSISTENT");
    }
};

class ControlLogicRep : public ControlGroup {
public:
    ControlLogicRep(ControlFileMgr *controlFileMgr, DstoreMemoryContext memCtx, PdbId pdbId)
        : ControlGroup(controlFileMgr, memCtx, CONTROL_GROUP_TYPE_LOGICALREP, CONTROLFILE_PAGEMAP_LOGICALREP_META,
                       pdbId)
    {}
    ~ControlLogicRep() {}
    DISALLOW_COPY_AND_MOVE(ControlLogicRep);
    RetStatus Init(UNUSE_PARAM DeployType deployType)
    {
        if (unlikely(ControlGroup::Init(deployType) != DSTORE_SUCC)) {
            return DSTORE_FAIL;
        }
        m_isInitialized = true;
        return DSTORE_SUCC;
    }
    RetStatus Create();
    void Reload()
    {}
    RetStatus AddLogicalReplicationSlot(ControlLogicalReplicationSlotPageItemData *data);
    RetStatus DeleteLogicalReplicationSlot(char *slotName, WalId walId, bool &isExist);
    RetStatus UpdateLogicalReplicationSlot(ControlLogicalReplicationSlotPageItemData *data);
    RetStatus GetAllLogicalReplicationSlotBaseOnWalId(WalId targetWalId,
        ControlLogicalReplicationSlotPageItemData **repSlotinfo, int &slotCountNum);
};
} // namespace DSTORE
#endif // DSTORE_CONTROL_LOGICREP_H