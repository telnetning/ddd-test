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
 * dstore_csn_mgr_diagnose.h
 *
 * IDENTIFICATION
 *        interface/diagnose/dstore_csn_mgr_diagnose.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_CSN_MGR_DIAGNOSE
#define DSTORE_CSN_MGR_DIAGNOSE
#include <cstdint>
#include "common/dstore_common_utils.h"
namespace DSTORE {

#pragma GCC visibility push(default)
struct CsnInfo {
    NodeId selfNodeId;
    CommitSeqNo nextCsn;
    CommitSeqNo localCsnMin;
    CommitSeqNo globalCsnMin;
    bool isGlobalCsnMinReady;
    bool isCsnOwner;
    uint32_t nodeCnt;
    NodeId *nodeList;
};
class CsnMgrDiagnose {
public:
    static CsnInfo *GetCsnInfo(uint32_t &csnInfoCnt);
    static void ReleaseCsnInfo(CsnInfo *csnInfo, uint32_t csnInfoCnt);
    static char *DumpCsnMgr();
};
#pragma GCC visibility pop
}  // namespace DSTORE

#endif
