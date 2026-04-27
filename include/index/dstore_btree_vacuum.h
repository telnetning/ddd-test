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
 * dstore_btree_vacuum.h
 *
 * IDENTIFICATION
 *        dstore/include/index/dstore_btree_vacuum.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef SRC_GAUSSKERNEL_INCLUDE_INDEX_DSTORE_BTREE_VACUUM_H
#define SRC_GAUSSKERNEL_INCLUDE_INDEX_DSTORE_BTREE_VACUUM_H

#include "tablespace/dstore_data_segment_context.h"
#include "common/memory/dstore_mctx.h"

namespace DSTORE {
class BtreeVacuum : public BaseObject {
public:
    explicit BtreeVacuum(StorageRelation indexRel, IndexInfo *indexInfo);
    DISALLOW_COPY_AND_MOVE(BtreeVacuum);
    ~BtreeVacuum();

    RetStatus BtreeLazyVacuum();
    RetStatus BtreeVacuumGPI(GPIPartOidCheckInfo *gpiCheckInfo);
    void Init();
    inline BtreeStorageMgr *GetBtreeSmgr() const
    {
        return m_indexRel->btreeSmgr;
    }
    inline PdbId GetPdbId()
    {
        return m_indexRel->m_pdbId;
    }

private:
    StorageRelation m_indexRel;
    IndexInfo *m_indexInfo;
    DstoreMemoryContext m_memContext;
    DataSegmentScanContext *m_segScanContext;
    BufMgrInterface *m_bufMgr;
};
} /* namespace DSTORE */

#endif