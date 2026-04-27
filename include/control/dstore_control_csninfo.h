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
 * dstore_control_csninfo.h
 *  Record MaxReservedCSN and undozone map segmentid in the control file..
 *
 *
 * IDENTIFICATION
 *        dstore/include/control/dstore_control_csninfo.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_CONTROL_CSNINFO_H
#define DSTORE_CONTROL_CSNINFO_H
#include "control/dstore_control_struct.h"
#include "common/algorithm/dstore_string_info.h"
#include "page/dstore_page_struct.h"
#include "control/dstore_control_file_mgr.h"
#include "common/dstore_common_utils.h"
#include "control/dstore_control_group.h"
namespace DSTORE {
struct ControlCsnPageData {
    uint32 m_version;
    CommitSeqNo m_csn;
    /* ID of the segment where undo-zone-id-to-segment-id map stored. */
    PageId m_segmentId;
    static void Dump(void* item, StringInfoData& dumpInfo)
    {
        ControlCsnPageData* data = static_cast<ControlCsnPageData*>(item);
        dumpInfo.append("CSN %lu, segmentId.m_fileId %d, segmentId.m_blockId %u.\n",
            data->m_csn, data->m_segmentId.m_fileId, data->m_segmentId.m_blockId);
    }
} PACKED;

class ControlCsnInfo : public ControlGroup {
public:
    ControlCsnInfo(ControlFileMgr *controlFileMgr, DstoreMemoryContext memCtx, PdbId pdbId)
        : ControlGroup(controlFileMgr, memCtx, CONTROL_GROUP_TYPE_CSN, CONTROLFILE_PAGEMAP_CSN_META, pdbId)
    {}
    ~ControlCsnInfo() {}
    DISALLOW_COPY_AND_MOVE(ControlCsnInfo);
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
    RetStatus SetMaxReservedCSN(CommitSeqNo csn, CommitSeqNo &newMaxReservedCsn);
    RetStatus GetMaxReservedCSN(CommitSeqNo &csn);
    RetStatus SetUndoZoneMapSegmentId(const PageId segmentId);
    RetStatus GetUndoZoneMapSegmentId(PageId &segmentId);
};
} // namespace DSTORE
#endif // DSTORE_CONTROL_CSNINFO_H