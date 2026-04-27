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
 */
#ifndef UT_BTREE_BUILD_CCINDEX_H
#define UT_BTREE_BUILD_CCINDEX_H

#include <gtest/gtest.h>
#include "ut_btree/ut_btree.h"
#include "index/concurrent/dstore_ccindex_btree_build_handler.h"

class UTBtreeCcindexTest : virtual public UTBtree {
public:
    void TearDown() override
    {
        DstorePfreeExt(m_deltaIndexCols);
        UTTableHandler::Destroy(m_deltaTableHandler);
        m_deltaTableHandler = nullptr;
        delete m_ccindexBtree;
        m_ccindexBtree = nullptr;
        UTBtree::TearDown();
    }

    void FakePhase1(bool targetIndexInique = false);
    int InsertInPhase1(ItemPointer heapCtid, int startVal, int numOper, int rollbackStep = 0, int valStep = 1);

    ItemPointerData InsertOneInPhase2(int keyVal, bool needRollback = false);
    int InsertBatchInPhase2(ItemPointer heapCtid, int startVal, int numOper, int rollbackStep = 0, int valStep = 1);
    void DeleteOneInPhase2(int keyVal, ItemPointer heapCtid, bool needRollback = false);
    ItemPointerData UpdateOneInPhase2(int oldVal, int newVal, ItemPointerData oldHeapCtid, bool needRollback = false);

    ItemPointerData InsertOneInPhase3(int keyVal, bool isUnique = true, bool needRollback = false);
    void DeleteOneInPhase3(int keyVal, ItemPointer heapCtid, bool needRollback = false);
    ItemPointerData UpdateOneInPhase3(int oldVal, int newVal, ItemPointerData oldHeapCtid, bool needRollback = false);

    void UpdateLocalCsnMin();

    ItemPointerData InsertHeapTupleIntoTargetTable(int keyVal);
    void DeleteHeapTupleFromTargetTable(ItemPointer targetHeapCtid);
    ItemPointerData UpdateHeapTupleFromTargetTable(int newVal, ItemPointerData targetHeapCtid);

    ItemPointerData InsertRecordIntoDeltaTable(int keyVal, ItemPointer targetHeapCtid,
                                               DmlOperationTypeForCcindex operType);
    void DeleteOldDeltaRecord(ItemPointer oldHeapCtid);
    RetStatus UpdateDeltaIndex(int keyVal, ItemPointer targetHeapCtid, ItemPointer deltaHeapCtid,
                               DmlOperationTypeForCcindex operType);

    void GetTargetIndexDatum(Datum *values, bool *isnulls, int keyVal);
    void GetDeltaDatum(Datum *values, bool *isnulls, int keyVal, ItemPointer heapCtid,
                       DmlOperationTypeForCcindex operType);
    void GetDeltaIndexDatumFromHeapTuple(Datum *values, bool *isnulls, HeapTuple *heapTuple);

    int GetNumOfDataFromIndex(int keyNum = 0, ScanKey skey = nullptr);
    int GetNumOfDataFromTable();

    /* Delta dml table for ccindex */
    UTTableHandler* m_deltaTableHandler;
    int m_deltaAttrNum;
    int *m_deltaIndexCols = nullptr;

    CcindexBtrBuildHandler *m_ccindexBtree;
    Xid m_metaBuildXid;
    Xid m_btreeBuildXid;
};

#endif // UT_BTREE_BUILD_CCINDEX_H
