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
 * dstore_heap_interface.h
 *
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/dstore/interface/table/dstore_heap_interface.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_HEAP_INTERFACE_H
#define DSTORE_HEAP_INTERFACE_H

#include "systable/dstore_relation.h"
#include "heap/dstore_heap_struct.h"
#include "tuple/dstore_tuple_struct.h"
#include "index/dstore_scankey.h"
#include "framework/dstore_parallel_interface.h"
#include "transaction/dstore_transaction_struct.h"
#include "common/dstore_common_utils.h"

namespace DSTORE {
struct varlena;
class HeapScanHandler;
class HeapInsertHandler;
struct SnapshotData;
}  // namespace DSTORE

namespace HeapInterface {
#pragma GCC visibility push(default)

DSTORE::HeapInsertHandler *CreateHeapInsertHandler(bool isUseRingBuf = false);
DSTORE::RetStatus Insert(DSTORE::StorageRelation relation, DSTORE::HeapTuple *tuple, DSTORE::CommandId cid);
DSTORE::RetStatus Insert(DSTORE::StorageRelation relation, DSTORE::HeapTuple *tuple, DSTORE::ItemPointerData &ctid,
                         DSTORE::CommandId cid);
DSTORE::RetStatus BatchInsert(DSTORE::HeapInsertHandler *heapInsertHandler, DSTORE::StorageRelation relation,
                              DSTORE::HeapTuple **tuples, uint16_t nTuples, DSTORE::CommandId cid);
DSTORE::RetStatus Update(DSTORE::StorageRelation relation, DSTORE::HeapUpdateContext *context,
                         bool isSwappingPartition = false);
DSTORE::RetStatus ForceUpdateTupleDataNoTrx(DSTORE::StorageRelation relation, DSTORE::HeapUpdateContext *context,
    bool wait_flush);
DSTORE::RetStatus Delete(DSTORE::StorageRelation relation, DSTORE::HeapDeleteContext *context);
DSTORE::RetStatus LazyVacuum(DSTORE::StorageRelation relation);
DSTORE::RetStatus LockUnchangedTuple(DSTORE::StorageRelation relation, DSTORE::HeapLockTupleContext *context);
DSTORE::RetStatus LockNewestTuple(DSTORE::StorageRelation relation, DSTORE::HeapLockTupleContext *context);
void DestroyHeapInsertHandler(DSTORE::HeapInsertHandler *heapInsertHandler);

DSTORE::HeapScanHandler *CreateHeapScanHandler(DSTORE::StorageRelation relation, bool isUseRingBuf = false);
DSTORE::RetStatus BeginScan(DSTORE::HeapScanHandler *heapScanHandler, DSTORE::SnapshotData *snapshot,
               bool showAnyTupForDebug = false);
void ReScan(DSTORE::HeapScanHandler *heapScanHandler);
void ResetController(DSTORE::HeapScanHandler *heapScanHandler);
DSTORE::RetStatus SetParallelController(DSTORE::HeapScanHandler *heapScanHandler,
    DSTORE::ParallelWorkController *controller, int smpId);
DSTORE::ScanKey CreateScanKey(int scanKeyNum);
void SetScanKey(DSTORE::HeapScanHandler *heapScanHandler, DSTORE::TupleDesc desc, int nkeys, DSTORE::ScanKey key);
void EndScan(DSTORE::HeapScanHandler *heapScanHandler);
DSTORE::HeapTuple *SeqScan(DSTORE::HeapScanHandler *heapScanHandler, DSTORE::FailureInfo *failureInfo = nullptr);
DSTORE::RetStatus SampleScan(DSTORE::HeapScanHandler *heapScanHandler, DSTORE::HeapSampleScanContext *context);
DSTORE::HeapTuple *FetchTuple(DSTORE::HeapScanHandler *heapScanHandler, DSTORE::ItemPointerData &ctid,
                              bool needCheckVisibility = true);
void DestroyHeapScanHandler(DSTORE::HeapScanHandler *heapScanHandler);
DSTORE::varlena *FetchLobValue(DSTORE::StorageRelation relation, DSTORE::ItemPointerData &ctid,
                               DSTORE::Snapshot snapshot, DSTORE::AllocMemFunc allocMem = nullptr);
DSTORE::HeapTuple *FetchTupleWithLob(DSTORE::StorageRelation relation, DSTORE::HeapTuple *tuple,
                                     DSTORE::Snapshot snapshot, DSTORE::AllocMemFunc allocMem = nullptr);

void RollbackLastRecordWhenConflict(DSTORE::StorageRelation relation, DSTORE::HeapTuple *tuple);

void DumpScanPage(DSTORE::HeapScanHandler *heapScanHandler, DSTORE::Datum &fileId, DSTORE::Datum &blockId,
                  DSTORE::Datum &data);
bool IsUsingMvccSnapshot(DSTORE::HeapScanHandler *heapScanHandler);
DSTORE::RetStatus FetchAllVisiableTupleInPage(DSTORE::HeapScanHandler *heapScanHandler, DSTORE::PageId pageId,
                                              DSTORE::OffsetNumber *offsetArray, uint16_t arrayLen, uint16_t &validLen);
#pragma GCC visibility pop
}  // namespace HeapInterface
#endif
