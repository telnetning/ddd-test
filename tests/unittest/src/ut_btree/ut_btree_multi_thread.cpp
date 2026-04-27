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
#include "heap/dstore_heap_scan.h"
#include "ut_btree/ut_btree_multi_thread.h"
#include "index/dstore_index_interface.h"

void UTBtreeMultiThread::BuildThreadLocalVar()
{
    if (unlikely(ThdUtTableHandler == nullptr)) {
        ThdUtTableHandler = UTTableHandler::GetTableHandler(g_defaultPdbId,
            thrd->m_memoryMgr->GetGroupContext(MemoryGroupType::MEMORY_CONTEXT_LONGLIVE),
            m_heapSegment,
            m_lobSegment,
            m_indexSegment);
        /* Also need to init btree meta */
        ThdUtTableHandler->CreateBtreeContext(m_indexCols, m_keyAttrNum, true);
    }
    ASSERT_NE(ThdUtTableHandler, nullptr);
    ASSERT_NE(ThdUtTableHandler->GetBtreeSmgr(), nullptr);
    ASSERT_TRUE(ThdUtTableHandler->m_heapSegmentPageId.IsValid());
    ASSERT_TRUE(ThdUtTableHandler->m_indexSegmentPageId.IsValid());
}

void UTBtreeMultiThread::BtreeInsertTuple(DefaultRowDef *insertRow, bool isUnique)
{
    BuildThreadLocalVar();
    AutoMemCxtSwitch autoSwitch(g_storageInstance->GetMemoryMgr()->GetGroupContext(MemoryGroupType::MEMORY_CONTEXT_LONGLIVE));

    /* Insert one tuple */
    ThdUtTableHandler->SetIndexUnique(isUnique);
    IndexTuple *indexTuple = InsertSpecificIndexTuple(insertRow, DefaultNullBitMap, false, ThdUtTableHandler);
    if (indexTuple == nullptr) {
        return;
    }

    /* Construct snapshot for future index scan. */
    Transaction *txn = thrd->GetActiveTransaction();
    CommitSeqNo csn;
    EXPECT_EQ(g_storageInstance->GetCsnMgr()->GetNextCsn(csn, false), DSTORE_SUCC);
    m_itupCounter.fetch_add(1);
    ErrLog(DSTORE_LOG, MODULE_INDEX,
           ErrMsg("Inserted index tuple data (%u, %u)", insertRow->column_int16, insertRow->column_int32));
    Lock();
    m_snapshots[csn + 1] = {indexTuple->GetHeapCtid(), {insertRow->column_int16, insertRow->column_int32}};
    m_itups.push(indexTuple);
    UnLock();
}

void UTBtreeMultiThread::BtreeScanAndCheckWithSnapshot(bool isUnique)
{
    BuildThreadLocalVar();
    ThdUtTableHandler->SetIndexUnique(isUnique);
    /* Get a snapshot. */
    Lock();
    std::map<CommitSeqNo, std::pair<ItemPointerData, std::pair<int16, int32>>>::iterator iter = m_snapshots.begin();
    if (iter == m_snapshots.end()) {
        UnLock();
        return;
    }
    CommitSeqNo csn = iter->first;
    ItemPointerData heapCtid = iter->second.first;
    std::pair<int16, int32> data = iter->second.second;
    m_snapshots.erase(iter);
    UnLock();

    /* Start a transaction for scan */
    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetSnapshotCsnForFlashback(csn);

    /* Init ScanKey */
    ScanKeyData keyInfos[m_keyAttrNum];
    InitScanKeyInt24(keyInfos, data.first, SCAN_ORDER_EQUAL, data.second, SCAN_ORDER_EQUAL);

    /* initialize index scan handler */
    IndexScanHandler indexScan;
    indexScan.InitIndexScanHandler(ThdUtTableHandler->GetIndexRel(), ThdUtTableHandler->GetIndexInfo(), m_keyAttrNum, 0);
    IndexInterface::ScanSetWantItup(&indexScan, true);
    indexScan.InitSnapshot(txn->GetSnapshot());
    indexScan.BeginScan();
    /* Set scankey */
    indexScan.ReScan(keyInfos);

    /* initialize heap scan handler */
    HeapScanHandler *heapScan = DstoreNew(thrd->GetTransactionMemoryContext())
        HeapScanHandler(g_storageInstance, thrd, ThdUtTableHandler->GetTableRel());

    /* Start scan */
    heapScan->Begin(txn->GetSnapshot());
    bool foundTarget = false;
    HeapTuple *heapTuple = nullptr;
    IndexScanDesc indexScanDesc;
    bool found;
    while (STORAGE_FUNC_SUCC(indexScan.GetNextTuple(ScanDirection::FORWARD_SCAN_DIRECTION, &found)) && found) {
        indexScanDesc = indexScan.GetScanDesc();
        /* May find more than one index tuple with the same scankey. Need to check heap ctid */
        if (heapCtid == indexScanDesc->heapCtid) {
            foundTarget = true;
            EXPECT_EQ(*(int16*)indexScanDesc->itup->GetValues(), data.first);
            EXPECT_EQ(*(int32*)(indexScanDesc->itup->GetValues() + sizeof(int16)), data.second);
        }
        heapTuple = heapScan->FetchTuple(indexScanDesc->heapCtid);
        EXPECT_NE(heapTuple, nullptr);
        DstorePfreeExt(heapTuple);
    }
    EXPECT_TRUE(foundTarget);
    heapScan->EndFetch();
    delete heapScan;
    indexScan.EndScan();
    txn->Commit();
}

void UTBtreeMultiThread::BtreeDeleteTuple()
{
    Lock();
    if (m_itups.empty()) {
        UnLock();
        return;
    }
    IndexTuple *itup = m_itups.front();
    m_itups.pop();
    UnLock();

    DeleteIndexTuple(itup, false, ThdUtTableHandler);
}

TEST_F(UTBtreeMultiThread, BtreeMultiThreadInsertScanTest_level0)
{
    BuildIndexForUTTable();
    const int TUPLE_NUM = 10;
    int start = 100;
    DefaultRowDef insertRow = UTTableHandler::GetDefaultRowDef();
    DefaultRowDef insertRows[TUPLE_NUM];

    /*
     * Insert (100,100)(101,101)...(100+TUPLE_NUM-1,100+TUPLE_NUM-1)
     * If open unique check, only half of TUPLE_NUM can be inserted successfully.
     * If not, all can be inserted successfully.
     */
    for (int i = 0; i < TUPLE_NUM; i++) {
        insertRows[i] = insertRow;
        insertRows[i].column_int16 = (int16)(start + i);
        insertRows[i].column_int32 = (int32)(start + i);
        m_pool.AddTask(BtreeInsertTask, this, &insertRows[i], true);
    }
    m_pool.WaitAllTaskFinish();
    EXPECT_EQ(m_itupCounter.load(), TUPLE_NUM / 2);

    for (int i = 0; i < TUPLE_NUM; i++) {
        insertRows[i] = insertRow;
        insertRows[i].column_int16 = (int16)(start + i);
        insertRows[i].column_int32 = (int32)(start + i);
        m_pool.AddTask(BtreeInsertTask, this, &insertRows[i], false);
    }
    m_pool.WaitAllTaskFinish();
    EXPECT_EQ(m_itupCounter.load(), TUPLE_NUM + TUPLE_NUM / 2);

    /* Muti-thread scan */
    for (int i = 0; i < TUPLE_NUM * 2; i++) {
        m_pool.AddTask(BtreeScanTask, this, false);
    }
    m_pool.WaitAllTaskFinish();
}

TEST_F(UTBtreeMultiThread, BtreeMultiThreadMixTest_level0)
{
    BuildIndexForUTTable();
    const int TUPLE_NUM = 1000;
    DefaultRowDef insertRow = m_utTableHandler->GetDefaultRowDef();
    DefaultRowDef insertRows[TUPLE_NUM];

    /* Muti-thread insert random data */
    for (int i = 0; i < TUPLE_NUM; i++) {
        int16 random = (int16)rand();
        insertRows[i] = insertRow;
        insertRows[i].column_int16 = Int16GetDatum(random);
        insertRows[i].column_int32 = Int32GetDatum(random);
        m_pool.AddTask(BtreeInsertTask, this, &insertRows[i], false);
    }
    m_pool.WaitAllTaskFinish();

    /* Muti-thread delete */
    for (int i = 0; i < TUPLE_NUM; i++) {
        m_pool.AddTask(BtreeDeleteTask, this);
        m_pool.AddTask(BtreeScanTask, this, false);
    }
    m_pool.WaitAllTaskFinish();
}
