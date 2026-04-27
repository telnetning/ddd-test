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
 * dstore_flashback_interface.cpp
 *
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/dstore/src/flashback/dstore_flashback_interface.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "framework/dstore_instance.h"
#include "flashback/dstore_flashback_table.h"
#include "flashback/dstore_flashback_interface.h"

namespace FlashbackInterface {
using namespace DSTORE;

FlashbackTableHandler *CreateFlashbackTableHandler(StorageRelation heapRel, CommitSeqNo snapshotCsn, Snapshot snapshot,
                                                   bool isLob)
{
    return DstoreNew(thrd->GetTransactionMemoryContext())
        FlashbackTableHandler(heapRel->m_pdbId, g_storageInstance, thrd, heapRel, snapshotCsn, snapshot, isLob);
}

HeapTuple *GetDeltaTuple(FlashbackTableHandler *flashbackTableHandler)
{
    return flashbackTableHandler->GetDeltaTuple();
}

HeapTuple *GetLostTuple(FlashbackTableHandler *flashbackTableHandler)
{
    return flashbackTableHandler->GetLostTuple();
}

void DestroyFlashbackHandler(FlashbackTableHandler *flashbackTableHandler)
{
    delete flashbackTableHandler;
}

}  // namespace FlashbackInterface
