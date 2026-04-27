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
 * dstore_heap_delete.h
 *     This file defines the building blocks that bg page writer needs.
 *
 * IDENTIFICATION
 *        include/heap/dstore_heap_delete.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_HEAP_DELETE_H
#define DSTORE_HEAP_DELETE_H

#include "heap/dstore_heap_lock_tuple.h"

namespace DSTORE {

class HeapDeleteHandler : public HeapLockTupHandler {
public:
    HeapDeleteHandler(StorageInstance *instance, ThreadContext *thread, StorageRelation heapRel,
                        bool isLobOperation = false);
    ~HeapDeleteHandler() override = default;

    HeapDeleteHandler()                                     = delete;
    HeapDeleteHandler(const HeapDeleteHandler &)            = delete;
    HeapDeleteHandler &operator=(const HeapDeleteHandler &) = delete;

    RetStatus Delete(HeapDeleteContext *delContext);

protected:
    RetStatus DeleteSmallTuple(HeapDeleteContext *delContext);
    void GenerateHeapDeleteWal(BufferDesc *bufferDesc, OffsetNumber offset, UndoRecPtr undoPtr, uint8 tdId,
                               CommandId cid, HeapTuple *oldTuple, Bitmapset *replicaKeyAttrs);

private:
    RetStatus       BeginDelete(HeapDeleteContext *delContext);
    RetStatus       DoDelete(HeapDeleteContext *delContext);
    RetStatus       DeleteBigTuple(HeapDeleteContext *delContext);
    RetStatus       EndDelete(HeapDeleteContext *delContext) const;
    ItemPointerData GetNextChunk(BufferDesc *bufferDesc, OffsetNumber offset);
    RetStatus       DeleteDiskTuple(HeapDeleteContext *delContext, OffsetNumber offset);
    void            UpdateFsmForPrune(OffsetNumber offset);

    bool m_isLinkTuple;
};
}  // namespace DSTORE

#endif
