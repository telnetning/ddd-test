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
 * dstore_heap_insert.h
 *
 * IDENTIFICATION
 *        include/heap/dstore_heap_insert.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_HEAP_INSERT_H
#define DSTORE_HEAP_INSERT_H

#include "framework/dstore_instance.h"
#include "heap/dstore_heap_handler.h"
#include "heap/dstore_heap_struct.h"
#include "page/dstore_heap_page.h"

namespace DSTORE {

struct HeapPage;

struct HeapFreeOffsetRanges {
    OffsetNumber startOffset[MAX_ITEM_OFFSET_NUMBER];
    OffsetNumber endOffset[MAX_ITEM_OFFSET_NUMBER];
    int nranges;
};

class HeapInsertHandler : virtual public HeapHandler {
public:
    HeapInsertHandler(StorageInstance *instance, ThreadContext *thread, StorageRelation heapRel,
                        bool isLobOperation = false, bool isUseRingBuf = false);
    ~HeapInsertHandler() override;
    
    RetStatus Insert(HeapInsertContext *insertContext);
    RetStatus BatchInsert(HeapBacthInsertContext *batchContext);
    void RollbackLastRecordWhenConflict(ItemPointerData ctid);
    void SetHeapRelAndBufMgr(StorageRelation heapRel);

protected:
    BufferDesc *GetBuffer(TdId &newTdId, uint32 size, const PageId &excludePageId = INVALID_PAGE_ID);
    HeapPageSpaceStatus CheckPageHasEnoughSpace(BufferDesc *bufferDesc, uint32 needFsmSize, TdId tdId);
    uint32 TupleNeedPageFreeSize(uint32 freeSpaceNeeded) const;
    RetStatus InsertSmallDiskTup(HeapInsertContext *insertContext, HeapDiskTuple *diskTup, uint16 diskTupSize);
    RetStatus SplitTupIntoChunks(HeapTuple *tuple);
    void GenerateHeapInsertWal(HeapDiskTuple *diskTup, OffsetNumber offset, UndoRecPtr undoPtr, CommandId cid);
    void GenerateHeapBatchInsertWal(
        HeapInsertContext **insertContexts, uint32 size, uint16 nthispage, UndoRecPtr undoPtr);

    HeapTupChunks m_tupChunks;
    bool m_tupIsSplited;

private:
    /* Initialize the tuple to be inserted. */
    RetStatus PrepareTuple(HeapTuple *tuple);
    RetStatus BeginInsert(HeapInsertContext *insertContext);
    RetStatus InsertBigTuple(HeapInsertContext *insertContext);
    RetStatus InsertSmallTuple(HeapInsertContext *insertContext);
    void EndInsert(HeapInsertContext *insertContext);
    void ClearTupChunks(HeapTuple *tuple);

    RetStatus PrepareUndoForBatchInsert(UndoRecord &undoRec, HeapFreeOffsetRanges *rangeInfo, uint16 nrangesInUse);

    RetStatus BatchInsertSmallTuples(HeapInsertContext **insertContexts, uint16 ntuples, uint32 totalSmallDiskSize);
    RetStatus BatchInsertBigTuple(HeapInsertContext *insertContext);

    HeapFreeOffsetRanges *GetUsableOffsetRanges(HeapInsertContext **insertContexts, HeapPage *page, uint16 ntuples,
                                                uint16 ndone, uint32 saveFreeSpace);
    HeapFreeOffsetRanges *LocateUsableItemIds(HeapInsertContext **insertContexts, HeapPage *page, uint16 ntuples,
                                              uint16 *nthispage, uint32 *usedSpace);
    uint32 BatchInsertInOneRangeOffset(
        HeapInsertContext **insertContexts, OffsetNumber &offnum, OffsetNumber endOffnum, TdId tdId);
};

}  // namespace DSTORE

#endif  /* STORAGE_HEAP_INSERT_H */
