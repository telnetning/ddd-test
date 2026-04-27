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
 * dstore_heap_lock_tup.cpp
 *
 *
 *
 * IDENTIFICATION
 *        include/heap/dstore_heap_lock_tuple.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_HEAP_LOCK_TUPLE_H
#define DSTORE_HEAP_LOCK_TUPLE_H

#include "framework/dstore_instance.h"
#include "heap/dstore_heap_handler.h"
#include "heap/dstore_heap_struct.h"

namespace DSTORE {

class HeapLockTupHandler : virtual public HeapHandler {
public:
    HeapLockTupHandler(StorageInstance *instance, ThreadContext *thread, StorageRelation heapRel,
                        bool isLobOperation = false);
    ~HeapLockTupHandler() override = default;

    /* If tuple is not changed, lock it. Otherwise, return the new ctid in FailureInfo */
    RetStatus LockUnchangedTuple(HeapLockTupleContext *lockContext);

    /* tuple is changed, lock the newest tuple. */
    RetStatus LockNewestTuple(HeapLockTupleContext *lockContext);

protected:
    RetStatus DoLock(HeapLockTupleContext *lockContext, TdId &tdId);
    RetStatus InitBufferDesc(ItemPointerData ctid);
    bool CanRetry(HeapHandlerFailureReason &failReason);

    RetStatus CheckTupleChanged(ItemPointerData ctid, bool &isChanged, CommandId &tupleCid, Snapshot snapshot,
        bool executedEpq = false);
    RetStatus CheckTupleChangedByCid(ItemPointerData ctid, bool &isChanged, CommandId &tupleCid, Snapshot snapshot);
    RetStatus WaitTupleIfNeed(ItemPointerData ctid);
    RetStatus WaitTxn(Xid xid, LWLockMode lockMode);

    template <bool needExeclusiveLock>
    RetStatus GetNewestVersionCtid(const ItemPointerData &curCtid, ItemPointerData &newestCtid);
    RetStatus GetNewCtidByTupleLiveMode(ItemPointerData &newCtid, LWLockMode lockMode);
    RetStatus GetNewCtidAnotherPage(const ItemPointerData &curCtid, LWLockMode lockMode, ItemPointerData &newCtid);
    RetStatus SetFailureInfo(FailureInfo &failureInfo, ItemPointerData ctid, CommandId tupleCid, Snapshot snapshot);
    RetStatus RollbackBigTuple(ItemPointerData ctid);
private:
    RetStatus BeginLock(HeapLockTupleContext *lockContext);
};


} /* End of namespace DSTORE */
#endif
