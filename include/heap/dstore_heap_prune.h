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
 * dstore_heap_prune.h
 *
 *
 *
 * IDENTIFICATION
 *        storage/include/heap/dstore_heap_prune.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef HEAP_DSTORE_HEAP_PRUNE_H
#define HEAP_DSTORE_HEAP_PRUNE_H

#include "framework/dstore_instance.h"
#include "common/memory/dstore_mctx.h"
#include "buffer/dstore_buf.h"
#include "transaction/dstore_csn_mgr.h"
#include "page/dstore_heap_page.h"
#include "heap/dstore_heap_handler.h"


namespace DSTORE {

class HeapPruneHandler : public BaseObject {
public:
    HeapPruneHandler(StorageInstance *instance, StorageRelation heapRel, ThreadContext *thread,
                     BufferDesc *bufferDesc, bool isLobOperation = false)
        : m_instance(instance),
          m_heapRel(heapRel),
          m_thrd(thread),
          m_bufferDesc(bufferDesc),
          m_recycleMinCsn(m_instance->GetCsnMgr()->GetRecycleCsnMin(likely(bufferDesc != nullptr) ?
            bufferDesc->GetPdbId() : (likely(heapRel != nullptr) ? heapRel->m_pdbId : INVALID_PDB_ID))),
          m_isLob(isLobOperation)
    {
        TableStorageMgr* selectedSmgr = (isLobOperation && heapRel->lobTableSmgr != nullptr) ?
                    heapRel->lobTableSmgr :
                    heapRel->tableSmgr;
 
        m_bufMgr = (selectedSmgr != nullptr && selectedSmgr->IsGlobalTempTable()) ?
                    m_thrd->GetTmpLocalBufMgr() :
                    m_instance->GetBufferMgr();
    }

    ~HeapPruneHandler()
    {
        m_instance = nullptr;
        m_heapRel = nullptr;
        m_thrd = nullptr;
        m_bufferDesc = nullptr;
        m_bufMgr = nullptr;
    }

    TableStorageMgr *GetTableSmgr() const;
    void TryPrunePage(uint16 needSize = 0, bool fullPrune = false);

private:
    StorageInstance *m_instance;
    StorageRelation m_heapRel;
    ThreadContext *m_thrd;
    BufferDesc *m_bufferDesc;
    CommitSeqNo m_recycleMinCsn;
    bool m_isLob;
    BufMgrInterface *m_bufMgr;

    static constexpr float m_potentialFreeSpaceFactor = 0.6; /* The percentage of potential free space more than
                                                                this factor, need prune. */
    static constexpr float m_freeSpaceFactor = 0.2; /* The percentage of free space on the page less than
                                                       this factor, need prune. */

    bool CheckPagePrunable(HeapPage *page, uint16 needSize);
    uint16 ScanPrunableItems(HeapPage *page, ItemIdDiff *itemIdDiff, uint16 nItems, CommitSeqNo &minCsn);
    uint16 TryPruneItems(HeapPage *page, ItemIdDiff *itemIdDiff, uint16 maxDiffSize, CommitSeqNo &recentDeadMinCsn);
    void GeneratePruneWal(ItemIdDiff *itemIdDiff, uint16 diffNum, CommitSeqNo recentDeadMinCsn);
    bool NeedWal()
    {
        return (!GetTableSmgr()->IsGlobalTempTable());
    }
};

} /* namespace DSTORE */

#endif