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
 * dstore_flashback_table.h
 *
 * IDENTIFICATION
 *        include/flashback/dstore_flashback_table.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_FLASHBACK_TABLE_H
#define DSTORE_FLASHBACK_TABLE_H

#include "framework/dstore_instance.h"
#include "heap/dstore_heap_handler.h"
#include "heap/dstore_heap_scan.h"

namespace DSTORE {

/*
 * Flashback table handler behaves like a sequential scan handler. So the two pass (delta and lost) should be scanned
 * through two FlashbackTableHandlers
 */
class FlashbackTableHandler : public BaseObject {
public:
    FlashbackTableHandler(PdbId pdbId, StorageInstance *instance, ThreadContext *thrdContext, StorageRelation heapRel,
                          CommitSeqNo snapshotCsn, Snapshot snapshot, bool isLobOperation = false)
        : m_pdbId(pdbId),
          m_instance(instance),
          m_thrd(thrdContext),
          m_heapRel(heapRel),
          m_flashbackCsn(snapshotCsn),
          m_isLob(isLobOperation)
    {
        m_heapScanHandler = DstoreNew(thrd->GetTransactionMemoryContext()) HeapScanHandler(g_storageInstance,
            thrdContext, heapRel, isLobOperation);
        StorageAssert(m_heapScanHandler != nullptr);
        if (m_heapScanHandler != nullptr) {
            m_heapScanHandler->Begin(snapshot);
        }
    }

    ~FlashbackTableHandler()
    {
        StorageAssert(m_heapScanHandler != nullptr);
        if (m_heapScanHandler != nullptr) {
            m_heapScanHandler->End();
        }
        delete m_heapScanHandler;
        m_instance = nullptr;
        m_thrd = nullptr;
        m_heapRel = nullptr;
    }

    DISALLOW_COPY_AND_MOVE(FlashbackTableHandler)

    HeapTuple *GetDeltaTuple();
    HeapTuple *GetLostTuple();

private:
    bool IsTupleVisibleFlashbackCsn(HeapTuple *tuple);
    PdbId m_pdbId;
    StorageInstance *m_instance;
    ThreadContext *m_thrd;
    StorageRelation m_heapRel;
    CommitSeqNo m_flashbackCsn;
    bool m_isLob;
    HeapScanHandler *m_heapScanHandler;
};

} /* namespace DSTORE */
#endif
