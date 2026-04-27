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
 * dstore_tac_snapshot.h
 *     This file defines the building blocks that bg page writer needs.
 *
 * IDENTIFICATION
 *        include/transaction/dstore_tac_snapshot.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_TAC_SNAPSHOT_H
#define DSTORE_TAC_SNAPSHOT_H

#include "common/algorithm/dstore_binaryheap.h"
#include "common/memory/dstore_mctx.h"
#include "common/dstore_datatype.h"
#include "lock/dstore_lwlock.h"
 
namespace DSTORE {

#ifdef UT
#define private public
#endif

class TacOrphanTrxTracker : public BaseObject {
public:
    TacOrphanTrxTracker() : m_csnHeap(nullptr),
                            m_allocator(nullptr),
                            m_heapLock{}
    {}
    
    void Init();
    void Destroy();
    void AddTacOrphanTrx(CommitSeqNo csn);
    CommitSeqNo GetSmallestOrphanCsn();
    void RefreshOrphanTrxExpiryTime();

private:
    binaryheap* m_csnHeap; /* min heap on csn */
    DstoreMemoryContext m_allocator;
    LWLock m_heapLock;

    bool TryRemoveExpiredEntry();
};
 
#ifdef UT
#undef private
#endif

}

#endif  // TAC_SNAPSHOT_H