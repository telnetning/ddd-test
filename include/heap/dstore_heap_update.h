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
 * dstore_heap_update.h
 *
 * IDENTIFICATION
 *        include/heap/dstore_heap_update.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_HEAP_UPDATE_H
#define DSTORE_HEAP_UPDATE_H

#include "framework/dstore_instance.h"
#include "heap/dstore_heap_delete.h"
#include "heap/dstore_heap_insert.h"

namespace DSTORE {

constexpr uint16 MAX_DIFF_POS_NUM = 10;
constexpr int NUM_DIFF_STEP = 2;

class HeapUpdateHandler final : public HeapDeleteHandler, public HeapInsertHandler {
public:
    HeapUpdateHandler(StorageInstance *instance, ThreadContext *thread, StorageRelation heapRel,
                        bool isLobOperation = false);
    ~HeapUpdateHandler() override = default;

    RetStatus Update(HeapUpdateContext *updateContext);
    RetStatus ForceUpdateTupleDataNoTrx(HeapUpdateContext *updateContext, bool wait_flush);

#ifdef UT
#define private public
#endif

private:
    RetStatus Init(ItemPointerData ctid);
    RetStatus BeginUpdate(HeapUpdateContext *updateContext);
    RetStatus DoUpdate(HeapUpdateContext *updateContext);
    RetStatus EndUpdate(HeapUpdateContext *updateContext) const;
    RetStatus DoForceUpdateTupleDataNoTrx(HeapUpdateContext *updateContext, bool wait_flush);

    RetStatus UpdateSmallTuple(HeapUpdateContext *updateContext);
    RetStatus UpdateSmallTupleInplace(HeapUpdateContext *updateContext, HeapPage *page, TdId tdId);
    RetStatus UpdateSmallTupleSamePage(HeapUpdateContext *updateContext, HeapPage *page, TdId tdId);
    RetStatus UpdateSmallTupleAnotherPage(HeapUpdateContext *updateContext, TdId oldPageTdId);
    RetStatus UpdateSmallTupleOldPage(ItemPointerData otid, ItemPointerData newCtid, TdId tdId, CommandId cid);
    ItemPointerData UpdateSmallTupleNewPage(HeapUpdateContext *updateContext);

    RetStatus UpdateBigTuple(HeapUpdateContext *updateContext);
    RetStatus UpdateBigTupSizeSmaller(HeapUpdateContext *updateContext, HeapTuple **oldTupChunks);
    RetStatus UpdateBigTupSizeBigger(HeapUpdateContext *updateContext, HeapTuple **oldTupChunks);
    RetStatus UpdateOldTupChunks(HeapUpdateContext *updateContext, ItemPointerData newTupNextChunkCtid,
                                 uint32 oldUpdateChunkNum, HeapTuple **oldTupChunks);
    RetStatus GetBigTupleChunkCtidsInterval(ItemPointerData *oldTupNextChunkCtids, uint32 oldChunkNum);

    void GetDiffBetweenTuples(HeapTuple *oldTup, HeapTuple *newTup);
    uint16 GetTupleDiffSize(uint16 tupDataSize);

    void GenerateInplaceUpdateWal(BufferDesc *bufferDesc, HeapDiskTuple *newTuple, HeapDiskTuple *oldTuple,
                                  const struct WalRecordHeapInplaceUpdateHeaderContext &inplaceUpdateHeader,
                                  CommandId cid);
    void GenerateAppendUpdateWal(BufferDesc *bufferDesc, HeapDiskTuple *newTuple, OffsetNumber offset,
                                 UndoRecPtr undoPtr, CommandId cid, HeapTuple *oldTuple, Bitmapset *replicaKeyAttrs);
    void GenerateAnotherUpdateOldPageWal(BufferDesc *bufferDesc, OffsetNumber offset, UndoRecPtr undoPtr, uint8 tdId);
    void GenerateAnotherUpdateNewPageWal(BufferDesc *bufferDesc, HeapDiskTuple *newTuple, OffsetNumber offset,
                                         UndoRecPtr undoPtr, CommandId cid, HeapTuple *oldTuple,
                                         Bitmapset *replicaKeyAttrs);
    void GenerateHeapUpdateNextCtidWal(BufferDesc *bufferDesc, OffsetNumber offset, ItemPointerData nextCtid);

    /* The diff data position of old tuple and new tuple */
    uint16 m_diffPos[MAX_DIFF_POS_NUM];
    uint16 m_numDiffPos;
    uint16 m_tupDiffPos[MAX_DIFF_POS_NUM]; /* new/old tuple diff info */
    uint16 m_tupDiffNum;
    bool m_isBigTuple; /* indicate that oldTuple or newTuple whether is bigTuple */

#ifdef UT
#undef private
#endif
};

} /* namespace DSTORE */

#endif
