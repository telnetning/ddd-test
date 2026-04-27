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
 * dstore_control_relmap.h
 *  Record relation map in the control file..
 *
 *
 * IDENTIFICATION
 *        dstore/include/control/dstore_control_relmap.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_CONTROL_RELMAP_H
#define DSTORE_CONTROL_RELMAP_H
#include "control/dstore_control_struct.h"
#include "control/dstore_control_group.h"
#include "page/dstore_page_struct.h"
#include "common/algorithm/dstore_string_info.h"
namespace DSTORE {
enum class SysTableMapType {
    SYSTABLE_SHARED = 0,
    SYSTABLE_LOCAL,
};
#pragma pack(push)
#pragma pack(1)
struct ControlSysTableItemData {
    Oid sysTableOid;
    PageId segmentId;
    static void Dump(void* item, StringInfoData& dumpInfo)
    {
        ControlSysTableItemData* data = static_cast<ControlSysTableItemData*>(item);
        dumpInfo.append("sysTableOid %u, segmentId.m_fileId %d, segmentId.m_blockId %u.\n",
            data->sysTableOid, data->segmentId.m_fileId, data->segmentId.m_blockId);
    }
};

struct RelmapInvalidateData {
    int type;
    PdbId pdbId;
};

#pragma pack(pop)
struct ControlRelMapMeta {
    uint32 m_version;
    BlockNumber m_relMapSharedPageNumber;
    BlockNumber m_relMapLocalPageNumber;
    static void Dump(void* item, StringInfoData& dumpInfo)
    {
        ControlRelMapMeta* data = static_cast<ControlRelMapMeta*>(item);
        dumpInfo.append("version %u, sharedPageNumber %u, sharedPageNumber %u.\n",
            data->m_version, data->m_relMapSharedPageNumber, data->m_relMapLocalPageNumber);
    }
} PACKED;

class ControlRelmap : public ControlGroup {
public:
    ControlRelmap(ControlFileMgr *controlFileMgr, DstoreMemoryContext memCtx, PdbId pdbId)
        : ControlGroup(controlFileMgr, memCtx, CONTROL_GROUP_TYPE_RELMAP, CONTROLFILE_PAGEMAP_RELMAP_META, pdbId)
    {}
    ~ControlRelmap()
    {}
    DISALLOW_COPY_AND_MOVE(ControlRelmap);
    RetStatus Init(DeployType deployType)
    {
        if (unlikely(ControlGroup::Init(deployType) == DSTORE_SUCC)) {
            m_isInitialized = true;
            return DSTORE_SUCC;
        }
        return DSTORE_FAIL;
    }
    RetStatus Create();
    void Reload()
    {}
    RetStatus AddSysTableItem(const Oid sysTableOid, const PageId &segmentId);
    RetStatus GetSysTableItem(Oid sysTableOid, PageId &segmentId);
    RetStatus GetAllSysTableItem(int type, ControlSysTableItemData *systbItems, int &count);
    RetStatus WriteAllSysTableItem(int type, ControlSysTableItemData *systbItems, int count);
    RetStatus InvalidateRelmapInfo(const void* data, uint32 dataLen);

private:
    BlockNumber GetSystableItemPageNumber(int type);
};
} // namespace DSTORE
#endif // DSTORE_CONTROL_RELMAP_H