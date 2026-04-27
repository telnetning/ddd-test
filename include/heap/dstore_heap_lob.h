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
 * dstore_heap_lob.h
 *
 * IDENTIFICATION
 *        include/heap/dstore_heap_lob.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_HEAP_LOB_H
#define DSTORE_HEAP_LOB_H

#include "systable/dstore_relation.h"
#include "heap/dstore_heap_handler.h"
#include "heap/dstore_heap_delete.h"
#include "heap/dstore_heap_update.h"
#include "tuple/dstore_memheap_tuple.h"
#include "common/dstore_common_utils.h"

namespace DSTORE {

struct TupleWithLobContext {
    bool needReturn = true;
    Size lobTotalSize = 0;
    TupleDesc tupDesc = nullptr;
    bool *lobIsValids = nullptr;
    HeapTuple **lobTuples = nullptr;
    FailureInfo *failureInfo = nullptr;
    SnapshotData snapshot;
    CommandId cid;

    RetStatus Init(TupleDesc tupleDesc, bool isNeedReturn = true, FailureInfo *fInfo = nullptr)
    {
        needReturn = isNeedReturn;
        tupDesc = tupleDesc;
        failureInfo = fInfo;

        uint32 natts = static_cast<uint32>(tupDesc->natts);

        lobIsValids = static_cast<bool *>(DstorePalloc0(natts * sizeof(bool)));
        if (unlikely(lobIsValids == nullptr)) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("lobIsValids is nullptr."));
            return DSTORE_FAIL;
        }

        if (needReturn) {
            lobTuples = static_cast<HeapTuple **>(DstorePalloc0(natts * sizeof(HeapTuple *)));
            if (unlikely(lobTuples == nullptr)) {
                ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("lobTuples is nullptr."));
                return DSTORE_FAIL;
            }
        }

        return DSTORE_SUCC;
    }

    void Clean()
    {
        if (needReturn && likely(lobTuples != nullptr)) {
            for (int i = 0; i < tupDesc->natts; i++) {
                DstorePfreeExt(lobTuples[i]);
            }
            DstorePfreeExt(lobTuples);
        }

        DstorePfreeExt(lobIsValids);
    }
};

struct DoUpdateContext {
    VarattLobLocator *oldLobLocator = nullptr;
    VarattLobLocator *newLobLocator = nullptr;
    TableStorageMgr *lobTableSmgr = nullptr;
    char *newLobValues = nullptr;
    bool newLobIsNull = false;
    int i = 0;
};

class HeapLobHandler : public BaseObject {
public:
    explicit HeapLobHandler(StorageRelation relation);
    ~HeapLobHandler() = default;

    RetStatus Insert(HeapTuple *tuple, CommandId cid);
    RetStatus BatchInsert(HeapTuple **tuples, uint16_t nTuples, CommandId cid);
    HeapTuple *Fetch(HeapTuple *tuple, Snapshot snapshot, AllocMemFunc allocMem = nullptr);
    RetStatus Delete(HeapDeleteContext *context);
    RetStatus Update(HeapUpdateContext *context);

    static varlena *FetchLobValueFromTuple(HeapTuple *tuple, AllocMemFunc allocMem = nullptr);

private:
    bool m_isLob = true;
    StorageRelation relation;

    RetStatus DoInsert(HeapTuple *tuple, HeapInsertHandler *heapInsert, CommandId cid);
    RetStatus DoFetch(HeapTuple *tuple, HeapScanHandler *heapScan, TupleWithLobContext &lobCtx);
    RetStatus DoDelete(HeapTuple *tuple, HeapDeleteHandler *heapDelete,
                       HeapDeleteContext &delCtx, TupleWithLobContext &lobCtx);
    RetStatus BeginUpdate(HeapUpdateContext *context);
    RetStatus DoUpdate(HeapTuple *newTuple, HeapTuple *oldTuple,
                       TupleWithLobContext &lobCtx);
    RetStatus DoInsertUpdate(DoUpdateContext &doUpdateCtx, CommandId cid);
    RetStatus DoDeleteUpdate(const DoUpdateContext &doUpdateCtx, TupleWithLobContext &lobCtx);
    RetStatus MakeTupleWithLob(HeapTuple *tuple, HeapTuple *&tupleWithLob, TupleWithLobContext &lobCtx,
                               AllocMemFunc allocMem = nullptr);

    static varlena *Compress(varlena *value);
    static varlena *Decompress(varlena *value, AllocMemFunc allocMem = nullptr);
};
}  // namespace DSTORE

#endif
