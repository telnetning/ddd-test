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
#include <gtest/gtest.h>
#include "index/dstore_btree_build.h"
#include "index/dstore_btree_delete.h"
#include "ut_btree/ut_btree.h"

using namespace DSTORE;

TEST_F(UTBtree, BtreeDeleteTest_level0)
{
    /* Fill table with specific data */
    int rowNum = 350;
    DefaultRowDef *tableDef = (DefaultRowDef *)DstorePalloc(rowNum * sizeof(DefaultRowDef));
    for (int i = 0; i < rowNum; i++) {
        int16 r1 = (int16)i + 1;
        int32 r2 = (int32)i + 1;
        DefaultRowDef rowDef = m_utTableHandler->GetDefaultRowDef();
        rowDef.column_int16 = r1;
        rowDef.column_int32 = r2;
        tableDef[i] = rowDef;
    }
    m_utTableHandler->FillTableWithSpecificData(tableDef, rowNum);
    DstorePfree(tableDef);

    /* Build btree index. */
    int indexCols[] = {1, 2};
    bool isUnique = false;
    int numAttrs = sizeof(indexCols) / sizeof(int);
    m_utTableHandler->CreateIndex(indexCols, numAttrs, isUnique);
    TupleDesc indexTupleDesc = m_utTableHandler->GetIndexTupleDesc();

    /* Test btree meta page info */
    PageId btrRootPageId = TestBtreeMetaPage(m_utTableHandler->GetBtreeSmgr()->GetBtrMetaPageId());
    TestBtreeRootPage(btrRootPageId, 1);

    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetSnapshotCsn();
    SnapshotData snapshot = *txn->GetSnapshotData();
    txn->Commit();

    /* Insert and delete until all tds are shared */
    DefaultRowDef insertRow = m_utTableHandler->GetDefaultRowDef();
    insertRow.column_int16 = -1;
    insertRow.column_int32 = 1;

    IndexTuple *indexTuple0 = InsertSpecificIndexTuple(&insertRow);
    EXPECT_EQ(DeleteIndexTuple(indexTuple0), DSTORE_SUCC);

    insertRow.column_int16 = 0;
    insertRow.column_int32 = 1;

    IndexTuple *indexTuple1 = InsertSpecificIndexTuple(&insertRow);
    IndexTuple *indexTuple2 = InsertSpecificIndexTuple(&insertRow);
    EXPECT_EQ(DeleteIndexTuple(indexTuple1), DSTORE_SUCC);
    EXPECT_EQ(DeleteIndexTuple(indexTuple2), DSTORE_SUCC);

    /* delete visibility check */
    IndexTuple *indexTuple = InsertSpecificIndexTuple(&insertRow);

    Transaction *txnA = thrd->GetActiveTransaction();
    txnA->Start();
    txnA->SetSnapshotCsn();
    EXPECT_EQ(DeleteIndexTuple(indexTuple, true), DSTORE_SUCC);

    Transaction *txnB = thrd->GetActiveTransaction();
    txnB->Start();
    txnB->SetSnapshotCsnForFlashback(snapshot.GetCsn());
    EXPECT_EQ(DeleteIndexTuple(indexTuple, true), DSTORE_FAIL);

    txnB->Commit();
    txnA->Commit();

    std::queue<PageId> pivotPageIds, leafPageIds;
    pivotPageIds.push(btrRootPageId);
    leafPageIds = TestPivotTuples(indexTupleDesc, pivotPageIds, 1);
    EXPECT_TRUE(IsTupleDeleted(indexTupleDesc, leafPageIds, 1, indexTuple0));
}

TEST_F(UTBtree, BtreeDeleteAfterPruneTest_level0)
{
    /* Fill table with specific data */
    int rowNum = 3;
    /* insert (2, 2), (4, 4), (6, 6) */
    DefaultRowDef *tableDef = (DefaultRowDef *)DstorePalloc(rowNum * sizeof(DefaultRowDef));
    for (int i = 1; i <= rowNum; i++) {
        int16 r1 = (int16)i * 2;
        int32 r2 = (int32)i * 2;
        DefaultRowDef rowDef = m_utTableHandler->GetDefaultRowDef();
        rowDef.column_int16 = r1;
        rowDef.column_int32 = r2;
        tableDef[i - 1] = rowDef;
    }
    m_utTableHandler->FillTableWithSpecificData(tableDef, rowNum);
    DstorePfree(tableDef);

    /* Build btree index. */
    int indexCols[] = {1, 2};
    bool isUnique = false;
    int numAttrs = sizeof(indexCols) / sizeof(int);
    m_utTableHandler->CreateIndex(indexCols, numAttrs, isUnique);

    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetSnapshotCsn();
    BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
    PageId leafPageId = m_utTableHandler->GetBtreeRootPageId();
    BufferDesc *leafBuf = bufMgr->Read(m_pdbId, leafPageId, DSTORE::LW_EXCLUSIVE);
    BtrPage *leafPage = static_cast<BtrPage *>(leafBuf->GetPage());
    BtrPageLinkAndStatus *leafPageLink = leafPage->GetLinkAndStatus();

    /* Quick delete All tuples */
    for (OffsetNumber off = leafPageLink->GetFirstDataOffset(); off <= leafPage->GetMaxOffset(); off++) {
        IndexTuple *itup = leafPage->GetIndexTuple(off);
        /* Note: we do deletion in this way only in UT case! */
        itup->SetDeleted();
    }
    leafPage->SetTuplePrunable(true);
    EXPECT_EQ(leafPage->GetNonDeletedTupleNum(), 0);
    BtrPage basePage;
    memcpy_s(&basePage, BLCKSZ, leafPage, BLCKSZ);

    /* Extend TD to trigger td prune later */
    TDAllocContext tdContext;
    EXPECT_EQ(leafPage->ExtendTd(tdContext), DSTORE_SUCC);
    bufMgr->MarkDirty(leafBuf);
    bufMgr->UnlockAndRelease(leafBuf);
    txn->Commit();

    /** Case 1. Delete the first */
    /* Insert and record a tuple */
    DefaultRowDef insertRow = m_utTableHandler->GetDefaultRowDef();
    insertRow.column_int16 = 1;
    insertRow.column_int32 = 10;
    IndexTuple *indexTuple = InsertSpecificIndexTuple(&insertRow);
    g_storageInstance->GetCsnMgr()->UpdateLocalCsnMin();

    /* Then delete it */
    /* Make alloc td fail to force prune page */
    FAULT_INJECTION_ACTIVE(DstoreTransactionFI::ALLOC_TD_FAIL, FI_GLOBAL);
    EXPECT_EQ(DeleteIndexTuple(indexTuple), DSTORE_SUCC);
    FAULT_INJECTION_INACTIVE(DstoreTransactionFI::ALLOC_TD_FAIL, FI_GLOBAL);

    /* The deleted tuple should be exactly the same with inserting one */
    txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetSnapshotCsn();
    leafBuf = bufMgr->Read(m_pdbId, leafPageId, DSTORE::LW_EXCLUSIVE);
    leafPage = static_cast<BtrPage *>(leafBuf->GetPage());
    EXPECT_EQ(leafPage->GetNonDeletedTupleNum(), 0);
    bool isEqual = false;
    OffsetNumber delOff = leafPage->BinarySearch(indexTuple, m_utTableHandler->GetIndexInfo(), &isEqual);
    EXPECT_EQ(delOff, 1);
    DstorePfree(indexTuple);

    /* Restore page */
    memcpy_s(leafPage, BLCKSZ, &basePage, BLCKSZ);
    bufMgr->MarkDirty(leafBuf, false);
    bufMgr->UnlockAndRelease(leafBuf, BufferPoolUnlockContentFlag::DontCheckCrc());
    txn->Commit();

    /** Case 2. Delete the in middle */
    /* Insert and abort a tuple to make tuple space recyclable */
    txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetSnapshotCsn();
    IndexTuple *abortTuple = InsertSpecificIndexTuple(&insertRow, DefaultNullBitMap, true);
    txn->Abort();
    DstorePfreeExt(abortTuple);

    /* Insert a tuple and commit */
    insertRow.column_int16 = 5; // shoud insert at offset 3
    insertRow.column_int32 = 1;
    indexTuple = InsertSpecificIndexTuple(&insertRow);
    /* And abort another one with same key */
    txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetSnapshotCsn();
    abortTuple = InsertSpecificIndexTuple(&insertRow, DefaultNullBitMap, true);
    txn->Abort();
    DstorePfreeExt(abortTuple);
    g_storageInstance->GetCsnMgr()->UpdateLocalCsnMin();

    /* Then delete it */
    /* Make alloc td fail to force prune page */
    FAULT_INJECTION_ACTIVE(DstoreTransactionFI::ALLOC_TD_FAIL, FI_GLOBAL);
    EXPECT_EQ(DeleteIndexTuple(indexTuple), DSTORE_SUCC);
    FAULT_INJECTION_INACTIVE(DstoreTransactionFI::ALLOC_TD_FAIL, FI_THREAD);

    /* The deleted tuple should be exactly the same with inserting one */
    txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetSnapshotCsn();
    leafBuf = bufMgr->Read(m_pdbId, leafPageId, DSTORE::LW_SHARED);
    leafPage = static_cast<BtrPage *>(leafBuf->GetPage());
    delOff = leafPage->BinarySearch(indexTuple, m_utTableHandler->GetIndexInfo(), &isEqual);
    EXPECT_EQ(leafPage->GetNonDeletedTupleNum(), 0);
    EXPECT_EQ(delOff, 2);
    DstorePfree(indexTuple);

    bufMgr->UnlockAndRelease(leafBuf, BufferPoolUnlockContentFlag::DontCheckCrc());
    txn->Commit();
}