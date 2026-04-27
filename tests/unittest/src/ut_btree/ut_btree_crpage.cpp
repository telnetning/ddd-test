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
#include "heap/dstore_heap_scan.h"
#include "ut_undo/ut_undo_zone.h"
#include "ut_btree/ut_btree.h"
#include "ut_btree/ut_btree_prune.h"
#include "ut_tabledatagenerator/ut_table_data_generator.h"

TEST_F(UTBtree, BtreeExtendCrPageTest_level0)
{
    HeapScanHandler *heapScan = nullptr;
    Transaction *txn = thrd->GetActiveTransaction();

    /* Build btree index using empty table */
    int indexCols[] = {1, 2};
    bool isUnique = false;
    int numAttrs = sizeof(indexCols) / sizeof(int);
    m_utTableHandler->CreateIndex(indexCols, numAttrs, isUnique);
    int delNum = 2;
    IndexTuple *deleteTuple[delNum];
    DefaultRowDef insertRow = m_utTableHandler->GetDefaultRowDef();

    /* Step 1. Insert shuffled values into btree. */
    int insertRowCount = 305;
    std::vector<int> values;
    for (int i = 1; i <= insertRowCount; i++) {
        values.push_back(i);
    }
    std::random_shuffle(values.begin(), values.end());
    IndexTuple *indexTuple = nullptr;
    bool *nullbitmap = DefaultNullBitMap;
    for (int i = 1; i <= insertRowCount; ++i) {
        DefaultRowDef insertRowTemp = insertRow;
        insertRowTemp.column_int16 = (int16)(values[i - 1]);
        insertRowTemp.column_int32 = (int32)(values[i - 1]);
        txn->Start();
        indexTuple = InsertSpecificIndexTuple(&insertRowTemp, nullbitmap, true);
        EXPECT_NE(indexTuple, nullptr);
        if (i <= 2) {
            deleteTuple[i -1] = indexTuple;
        }
        txn->Commit();
    }

    BufferDesc *btrMetaBuf;
    BtrMeta *btrMeta = m_utTableHandler->GetBtreeSmgr()->GetBtrMeta(LW_SHARED, &btrMetaBuf);
    EXPECT_EQ(btrMeta->GetRootLevel(), 0);
    PageId pageId = btrMeta->GetRootPageId();
    EXPECT_TRUE(pageId != INVALID_PAGE_ID);
    BufferDesc *bufferDesc = g_storageInstance->GetBufferMgr()->Read(g_defaultPdbId, pageId, LW_SHARED);
    EXPECT_LT(static_cast<BtrPage *>(bufferDesc->GetPage())->GetFreeSpaceForInsert(), indexTuple->GetSize() + sizeof(ItemId));
    g_storageInstance->GetBufferMgr()->UnlockAndRelease(btrMetaBuf);
    g_storageInstance->GetBufferMgr()->UnlockAndRelease(bufferDesc);

    /* Step 2. Delete tuple */
    GsAtomicFetchAddU64(&(g_storageInstance->GetCsnMgr()->m_nextCsn), 1);
    CommitSeqNo deleteCsn = INVALID_CSN;
    txn->Start();
    txn->SetSnapshotCsn();
    deleteCsn = txn->GetSnapshotCsn();
    for (int i = 0; i < delNum; ++i) {
        EXPECT_EQ(DeleteIndexTuple(deleteTuple[i], true), DSTORE_SUCC);
    }
    txn->Commit();

    /* Step 3. Btree page prune. */
    bufferDesc = g_storageInstance->GetBufferMgr()->Read(g_defaultPdbId, pageId, LW_EXCLUSIVE);
    BtreePagePrune *btreePagePrune =
        DstoreNew(thrd->m_memoryMgr->GetGroupContext(MemoryGroupType::MEMORY_CONTEXT_LONGLIVE))
        UTBtreePrune(m_utTableHandler->GetIndexRel(), m_utTableHandler->GetIndexInfo(),
                     m_utTableHandler->GetIndexScanKey(), bufferDesc);
    RetStatus ret = btreePagePrune->Prune();
    EXPECT_TRUE(ret == DSTORE_SUCC);
    btreePagePrune->GetPagePayload()->Drop(g_storageInstance->GetBufferMgr());
    delete btreePagePrune;
    btreePagePrune = nullptr;

    /* Step 4. Insert again to call PruneNonItemIdTuples when rollback. */
    int insertRowCountAfterPrune = 2;
    for (int i = 1 + insertRowCount; i <= insertRowCountAfterPrune + insertRowCount; ++i) {
        DefaultRowDef insertRowTemp = insertRow;
        insertRowTemp.column_int16 = (int16)(i);
        insertRowTemp.column_int32 = (int32)(i);
        txn->Start();
        indexTuple = InsertSpecificIndexTuple(&insertRowTemp, nullbitmap, true);
        EXPECT_NE(indexTuple, nullptr);
        if (i == insertRowCountAfterPrune + insertRowCount) {
            txn->IncreaseCommandCounter();
            /* Step 5. Flashback scan every tuple, deleted tuple should be visible. */
            txn->SetSnapshotCsnForFlashback(deleteCsn - 1);
            heapScan = DstoreNew(g_dstoreCurrentMemoryContext) HeapScanHandler(g_storageInstance, thrd,
                                                                               m_utTableHandler->GetTableRel());
            int nkeys = m_utTableHandler->GetIndexInfo()->indexKeyAttrsNum;
            IndexScanHandler indexScan;
            indexScan.InitIndexScanHandler(m_utTableHandler->GetIndexRel(), m_utTableHandler->GetIndexInfo(), nkeys, 0);
            indexScan.InitSnapshot(txn->GetSnapshot());
            indexScan.BeginScan();
            heapScan->Begin(txn->GetSnapshot());
            int foundNum = 0;
            ScanKeyData keyInfos[nkeys];
            for (int j = 1; j <= insertRowCount; j++) {
                InitScanKeyInt24(keyInfos, values[j - 1], SCAN_ORDER_EQUAL, values[j - 1], SCAN_ORDER_EQUAL);
                indexScan.ReScan(keyInfos);
                foundNum = GetSatisfiedTupleNum(indexScan, *heapScan);
                EXPECT_EQ(foundNum, 1);
                bool keysConflictFlag;
                int numberOfKeys;
                int numArrayKeys;
                ScanKey skey;
                bool isCrExtend;
                indexScan.GetScanStatus(keysConflictFlag, numberOfKeys, numArrayKeys, skey, isCrExtend);
                EXPECT_TRUE(isCrExtend);
            }
            indexScan.EndScan();
            heapScan->EndFetch();
            delete heapScan;
        }
        txn->Commit();
    }
    txn->Start();
    txn->IncreaseCommandCounter();
    /* Step 6. Flashback scan every tuple, deleted tuple should be visible. */
    txn->SetSnapshotCsnForFlashback(deleteCsn - 1);
    heapScan = DstoreNew(g_dstoreCurrentMemoryContext) HeapScanHandler(g_storageInstance, thrd,
                                                                       m_utTableHandler->GetTableRel());
    int nkeys = m_utTableHandler->GetIndexInfo()->indexKeyAttrsNum;
    IndexScanHandler indexScan;
    indexScan.InitIndexScanHandler(m_utTableHandler->GetIndexRel(), m_utTableHandler->GetIndexInfo(), nkeys, 0);
    indexScan.InitSnapshot(txn->GetSnapshot());
    indexScan.BeginScan();
    heapScan->Begin(txn->GetSnapshot());
    int foundNum = 0;
    ScanKeyData keyInfos[nkeys];
    for (int j = 1; j <= insertRowCount; j++) {
        InitScanKeyInt24(keyInfos, values[j - 1], SCAN_ORDER_EQUAL, values[j - 1], SCAN_ORDER_EQUAL);
        indexScan.ReScan(keyInfos);
        foundNum = GetSatisfiedTupleNum(indexScan, *heapScan);
        EXPECT_EQ(foundNum, 1);
    }
    indexScan.EndScan();
    heapScan->EndFetch();
    delete heapScan;
    txn->Commit();
}

TEST_F(UTBtree, BtreeReusedTdRollbackTest_level0)
{
    /* Build btree index using empty table */
    int indexCols[] = {1, 2};
    bool isUnique = false;
    int numAttrs = sizeof(indexCols) / sizeof(int);
    m_utTableHandler->CreateIndex(indexCols, numAttrs, isUnique);
    DefaultRowDef insertRow = m_utTableHandler->GetDefaultRowDef();

    HeapScanHandler *heapScan = nullptr;
    Transaction *txn = thrd->GetActiveTransaction();
    /* save csn before insert */
    CommitSeqNo readCsn = thrd->GetSnapShotCsn();
    g_storageInstance->GetCsnMgr()->GetNextCsn(readCsn, false);

    /* Step 1. Insert shuffled values into btree. */
    int insertRowCount = 5;
    std::vector<int> values;
    for (int i = 1; i <= insertRowCount; i++) {
        values.push_back(i);
    }
    std::random_shuffle(values.begin(), values.end());
    IndexTuple *indexTuple = nullptr;
    bool *nullbitmap = DefaultNullBitMap;
    /* all td used */
    for (int i = 1; i <= insertRowCount - 1; ++i) {
        DefaultRowDef insertRowTemp = insertRow;
        insertRowTemp.column_int16 = (int16)(values[i - 1]);
        insertRowTemp.column_int32 = (int32)(values[i - 1]);
        txn->Start();
        txn->SetSnapshotCsn();
        indexTuple = InsertSpecificIndexTuple(&insertRowTemp, nullbitmap, true);
        EXPECT_NE(indexTuple, nullptr);
        txn->Commit();
    }

    /* td 0 reused */
    DefaultRowDef insertRowTemp = insertRow;
    insertRowTemp.column_int16 = (int16)(values[insertRowCount - 1]);
    insertRowTemp.column_int32 = (int32)(values[insertRowCount - 1]);
    txn->Start();
    txn->SetSnapshotCsn();
    CommandId oriCid = txn->GetSnapshotCid();
    indexTuple = InsertSpecificIndexTuple(&insertRowTemp, nullbitmap, true);
    EXPECT_NE(indexTuple, nullptr);

    /* reused td need to rollback, no records visible */
    txn->SetSnapshotCsnForFlashback(readCsn);
    txn->SetTransactionSnapshotCid(oriCid);
    heapScan = DstoreNew(g_dstoreCurrentMemoryContext) HeapScanHandler(g_storageInstance, thrd,
                                                                       m_utTableHandler->GetTableRel());
    int nkeys = m_utTableHandler->GetIndexInfo()->indexKeyAttrsNum;
    IndexScanHandler indexScan;
    indexScan.InitIndexScanHandler(m_utTableHandler->GetIndexRel(), m_utTableHandler->GetIndexInfo(), nkeys, 0);
    indexScan.InitSnapshot(txn->GetSnapshot());
    indexScan.BeginScan();
    heapScan->Begin(txn->GetSnapshot());
    int foundNum = 0;
    ScanKeyData keyInfos[nkeys];
    for (int j = 1; j <= insertRowCount; j++) {
        InitScanKeyInt24(keyInfos, values[j - 1], SCAN_ORDER_EQUAL, values[j - 1], SCAN_ORDER_EQUAL);
        indexScan.ReScan(keyInfos);
        foundNum = GetSatisfiedTupleNum(indexScan, *heapScan);
        EXPECT_EQ(foundNum, 0);
    }
    indexScan.EndScan();
    heapScan->EndFetch();
    delete heapScan;
    txn->Commit();
}