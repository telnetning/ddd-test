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
#include "heap/dstore_heap_update.h"
#include "transaction/dstore_transaction_mgr.h"
#include "ut_heap/ut_heap_multi_thread.h"
#include "errorcode/dstore_heap_error_code.h"
#include "lock/dstore_table_lock_mgr.h"
#include "lock/dstore_xact_lock_mgr.h"
#include "transaction/dstore_transaction_interface.h"

void UTHeapMultiThread::BuildThreadLocalVar()
{
    if (unlikely(ThdUtTableHandler == nullptr)) {
        ThdUtTableHandler = UTTableHandler::GetTableHandler(g_defaultPdbId,
            thrd->m_memoryMgr->GetGroupContext(MemoryGroupType::MEMORY_CONTEXT_LONGLIVE),
            m_heapSegment,
            m_lobSegment,
            INVALID_PAGE_ID);
    }
    ASSERT_NE(ThdUtTableHandler, nullptr);
    ASSERT_NE(ThdUtTableHandler->GetHeapTabSmgr(), nullptr);
    ASSERT_NE(ThdUtTableHandler->GetLobTabSmgr(), nullptr);
    ASSERT_TRUE(ThdUtTableHandler->m_heapSegmentPageId.IsValid());
    ASSERT_TRUE(ThdUtTableHandler->m_lobSegmentPageId.IsValid());
}

void UTHeapMultiThread::HeapInsertFixedLengthTuple(int tupleLen)
{
    BuildThreadLocalVar();
    Transaction *txn = thrd->GetActiveTransaction();
    TransactionMgr *transactionMgr= g_storageInstance->GetPdb(g_defaultPdbId)->GetTransactionMgr();

    /* Step 1: Insert one tuple. */
    txn->Start();
    txn->SetSnapshotCsn();
    std::string data = GenerateRandomString(tupleLen);
    HeapTuple *heapTuple = UTTableHandler::GenerateSpecificHeapTuple(data);
    ItemPointerData ctid = ThdUtTableHandler->InsertHeapTupAndCheckResult(heapTuple, true, txn->GetSnapshotData());
    Xid xid = txn->GetCurrentXid();
    txn->Commit();
    DstorePfree(heapTuple);

    /* Step 2: Construct snapshot for future fetch, add ctid to queue for update/delete. */
    CommitSeqNo csn = transactionMgr->GetCsnFromXid(xid);
    m_mutex.lock();
    m_csnCtidMap.push_back(std::tuple<CommitSeqNo, ItemPointerData, std::string>(csn + 1,ctid, data));
    m_ctids.push(ctid);
    m_mutex.unlock();

    thrd->m_memoryMgr->reset_group_context(DSTORE::MEMORY_CONTEXT_QUERY);
}

void UTHeapMultiThread::HeapBatchInsertFixedLengthTuple(int tupleNum)
{
    BuildThreadLocalVar();
    Transaction *txn = thrd->GetActiveTransaction();
    TransactionMgr *transactionMgr= g_storageInstance->GetPdb(g_defaultPdbId)->GetTransactionMgr();

    /* Step 1: Insert one tuple. */
    DstoreMemoryContext oldMem = 
        DstoreMemoryContextSwitchTo(thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_TRANSACTION));
    txn->Start();
    txn->SetSnapshotCsn();
    HeapTuple *newHeapTuples[tupleNum];
    uint32 newDiskTupleLens[tupleNum];
    std::string data[tupleNum];
    for (int i = 0; i < tupleNum; i++) {
        data[i] = GenerateRandomString(100);
        HeapTuple *heapTuple = UTTableHandler::GenerateSpecificHeapTuple(data[i]);
        newHeapTuples[i] = heapTuple;
        newDiskTupleLens[i] = heapTuple->GetDiskTupleSize();
    }
    ItemPointerData ctids[tupleNum];
    ThdUtTableHandler->BatchInsertHeapTupsAndCheckResult(newHeapTuples, tupleNum, ctids, txn->GetSnapshotData(), true);
    Xid xid = txn->GetCurrentXid();
    txn->Commit();
    DstoreMemoryContextSwitchTo(oldMem);

    /* Step 2: Construct snapshot for future fetch, add ctid to queue for update/delete. */
    CommitSeqNo csn = transactionMgr->GetCsnFromXid(xid);
    m_mutex.lock();
    for (int i = 0; i < tupleNum; i++) {
        m_csnCtidMap.push_back(std::tuple<CommitSeqNo, ItemPointerData, std::string>(csn + 1, ctids[i], data[i]));
        m_ctids.push(ctids[i]);
    }
    m_mutex.unlock();

    thrd->m_memoryMgr->reset_group_context(DSTORE::MEMORY_CONTEXT_QUERY);
}

void UTHeapMultiThread::HeapLockTupDelayCommit()
{
    BuildThreadLocalVar();
    Transaction *txn = thrd->GetActiveTransaction();

    HeapLockTupleContext context;
    context.ctid = m_ctids.front();

    AutoMemCxtSwitch autoSwitch(thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_TRANSACTION));
    txn->Start();
    txn->SetSnapshotCsn();
    std::unique_ptr<StorageRelationData> heapRel(new StorageRelationData());
    heapRel->tableSmgr = ThdUtTableHandler->GetHeapTabSmgr();
    heapRel->m_pdbId = g_defaultPdbId;
    {
        context.snapshot = *txn->GetSnapshotData();
        HeapLockTupHandler lockTuple(g_storageInstance, thrd, heapRel.get());
        int result = lockTuple.LockUnchangedTuple(&context);
        ASSERT_EQ(result, DSTORE_SUCC);
    }
    m_counter.store(1);
    /* wait for another transaction */
    usleep(100000);
    m_counter.store(2);
    txn->Commit();

    thrd->m_memoryMgr->reset_group_context(DSTORE::MEMORY_CONTEXT_QUERY);
}

void UTHeapMultiThread::HeapFetchAndCheckWithSnapshot()
{
    BuildThreadLocalVar();
    /* Step 1: Get a snapshot. */
    m_mutex.lock();
    std::list<std::tuple<CommitSeqNo, ItemPointerData, std::string>>::iterator iter = m_csnCtidMap.begin();
    if (iter == m_csnCtidMap.end()) {
        m_mutex.unlock();
        return;
    }
    CommitSeqNo csn = std::get<0>(*iter);
    ItemPointerData ctid = std::get<1>(*iter);
    std::string data = std::get<2>(*iter);
    m_csnCtidMap.erase(iter);
    m_mutex.unlock();

    /* Step 2: Fetch tuple using snapshot and compare. */
    SnapshotData snapshot = {DSTORE::SnapshotType::SNAPSHOT_MVCC, csn, INVALID_CID};
    HeapTuple *resTuple = ThdUtTableHandler->FetchHeapTuple(&ctid, &snapshot);
    EXPECT_TRUE(resTuple->GetDiskTupleSize() >= data.length());
    StorageAssert(memcmp((void*)resTuple->GetDiskTuple()->GetData(), data.c_str(), data.length()) == 0);
    EXPECT_EQ(memcmp((void*)resTuple->GetDiskTuple()->GetData(), data.c_str(), data.length()), 0);
    DstorePfreeExt(resTuple);

    thrd->m_memoryMgr->reset_group_context(DSTORE::MEMORY_CONTEXT_QUERY);
}

void UTHeapMultiThread::HeapUpdateFixedLengthTuple(int tupleLen)
{
    BuildThreadLocalVar();
    Transaction *txn = thrd->GetActiveTransaction();
    TransactionMgr *transactionMgr= g_storageInstance->GetPdb(g_defaultPdbId)->GetTransactionMgr();

    /* Step 1: Get a ctid. */
    m_mutex.lock();
    if (m_ctids.empty()) {
        m_mutex.unlock();
        return;
    }
    ItemPointerData ctid = m_ctids.front();
    m_ctids.pop();
    m_mutex.unlock();

    /* Step 2: Update tuple of given ctid. */
    txn->Start();
    txn->SetSnapshotCsn();
    std::string data = GenerateRandomString(tupleLen);
    HeapTuple *heapTuple = UTTableHandler::GenerateSpecificHeapTuple(data);
    ItemPointerData newCtid = UpdateTupAndCheckResult(&ctid, heapTuple, txn->GetSnapshotData(), true, ThdUtTableHandler);
    Xid xid = txn->GetCurrentXid();
    txn->Commit();
    DstorePfree(heapTuple);

    if (newCtid != INVALID_ITEM_POINTER) {
        /* Step 3: Construct snapshot for future fetch, add ctid to queue for update/delete. */
        CommitSeqNo csn = transactionMgr->GetCsnFromXid(xid);
        m_mutex.lock();
        m_csnCtidMap.push_back(std::tuple<CommitSeqNo, ItemPointerData, std::string>(csn + 1, newCtid, data));
        m_ctids.push(newCtid);
        m_mutex.unlock();
    }

    thrd->m_memoryMgr->reset_group_context(DSTORE::MEMORY_CONTEXT_QUERY);
}

void UTHeapMultiThread::HeapUpdateConcurrently(int tupleLen)
{
    BuildThreadLocalVar();
    Transaction *txn = thrd->GetActiveTransaction();

    /* Step 1: Get a ctid, but keep it in map so that all transactions update the same tuple. */
    m_mutex.lock();
    std::list<std::tuple<CommitSeqNo, ItemPointerData, std::string>>::iterator iter = m_csnCtidMap.begin();
    if (iter == m_csnCtidMap.end()) {
        m_mutex.unlock();
        return;
    }
    CommitSeqNo csn = std::get<0>(*iter);
    ItemPointerData ctid = std::get<1>(*iter);
    m_mutex.unlock();

    /* Step 2: Update tuple of given snapshot. */
    txn->Start();
    txn->SetSnapshotCsnForFlashback(csn);
    std::string data = GenerateRandomString(tupleLen);
    HeapTuple *heapTuple = UTTableHandler::GenerateSpecificHeapTuple(data);
    HeapUpdateContext updateContext;
    updateContext.oldCtid = ctid;
    updateContext.newCtid = INVALID_ITEM_POINTER;
    updateContext.newTuple  = heapTuple;
    updateContext.snapshot = *txn->GetSnapshotData();
    updateContext.cid = txn->GetCurCid();
    RetStatus ret;
    std::unique_ptr<StorageRelationData> heapRel(new StorageRelationData());
    heapRel->tableSmgr = ThdUtTableHandler->GetHeapTabSmgr();
    heapRel->m_pdbId = g_defaultPdbId;
    {
        HeapUpdateHandler heapUpdate(g_storageInstance, thrd, heapRel.get());
        ret = heapUpdate.Update(&updateContext);
    }
    DstorePfree(heapTuple);

    /* Step 3: Record result. */
    if (ret == DSTORE_SUCC) {
        txn->Commit();
        m_counter++;
    } else {
        EXPECT_TRUE(StorageGetErrorCode() == HEAP_ERROR_TUPLE_IS_CHANGED);
        txn->Abort();
    }

    thrd->m_memoryMgr->reset_group_context(DSTORE::MEMORY_CONTEXT_QUERY);
}

void UTHeapMultiThread::HeapScanAndCheckWithSnapshot()
{
    BuildThreadLocalVar();

    /* Step 1: Get a snapshot. */
    m_mutex.lock();
    std::list<std::tuple<CommitSeqNo, ItemPointerData, std::string>>::iterator iter = m_csnCtidMap.begin();
    if (iter == m_csnCtidMap.end()) {
        m_mutex.unlock();
        return;
    }
    CommitSeqNo csn = std::get<0>(*iter);
    std::string data = std::get<2>(*iter);
    m_csnCtidMap.erase(iter);
    m_mutex.unlock();

    /* Step 2: Scan all to find the tuple. */
    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    bool found = false;
    std::unique_ptr<StorageRelationData> heapRel(new StorageRelationData());
    heapRel->tableSmgr = ThdUtTableHandler->GetHeapTabSmgr();
    heapRel->m_pdbId = g_defaultPdbId;
    {
        HeapScanHandler heap_scan(g_storageInstance, thrd, heapRel.get());
        txn->SetSnapshotCsnForFlashback(csn);
        HeapTuple *tuple;
        heap_scan.Begin(txn->GetSnapshot());
        while ((tuple = heap_scan.SeqScan()) != nullptr) {
            if (memcmp(tuple->GetDiskTuple()->GetData(), data.c_str(), data.length()) == 0) {
                found = true;
                break;
            }
        }
        heap_scan.End();
    }
    txn->Commit();
    EXPECT_TRUE(found);

    thrd->m_memoryMgr->reset_group_context(DSTORE::MEMORY_CONTEXT_QUERY);
}

void UTHeapMultiThread::HeapDeleteTuple()
{
    BuildThreadLocalVar();

    /* Step 1: Get a ctid. */
    m_mutex.lock();
    if (m_ctids.empty()) {
        m_mutex.unlock();
        return;
    }
    ItemPointerData ctid = m_ctids.front();
    m_ctids.pop();
    m_mutex.unlock();

    /* Step 2: Delete it. */
    DeleteTuple(&ctid, INVALID_SNAPSHOT, false, ThdUtTableHandler);
}

void UTHeapMultiThread::HeapDeleteConcurrently()
{
    BuildThreadLocalVar();
    Transaction *txn = thrd->GetActiveTransaction();

    /* Step 1: Get a ctid, but keep it in map so that all transactions update the same tuple. */
    m_mutex.lock();
    std::list<std::tuple<CommitSeqNo, ItemPointerData, std::string>>::iterator iter = m_csnCtidMap.begin();
    if (iter == m_csnCtidMap.end()) {
        m_mutex.unlock();
        return;
    }
    CommitSeqNo csn = std::get<0>(*iter);
    ItemPointerData ctid = std::get<1>(*iter);
    m_mutex.unlock();

    /* Step 2: Update tuple of given snapshot. */
    txn->Start();
    txn->SetSnapshotCsnForFlashback(csn);
    HeapDeleteContext deleteContext;
    deleteContext.ctid = ctid;
    deleteContext.snapshot = *txn->GetSnapshotData();
    deleteContext.cid = txn->GetCurCid();
    int ret;
    std::unique_ptr<StorageRelationData> heapRel(new StorageRelationData());
    heapRel->tableSmgr = ThdUtTableHandler->GetHeapTabSmgr();
    heapRel->m_pdbId = g_defaultPdbId;
    {
        HeapDeleteHandler heapDelete(g_storageInstance, thrd, heapRel.get());
        ret = heapDelete.Delete(&deleteContext);
    }

    /* Step 3: Record result. */
    if (ret == DSTORE_SUCC) {
        txn->Commit();
        m_counter++;
    } else {
        EXPECT_TRUE(StorageGetErrorCode() == HEAP_ERROR_TUPLE_IS_CHANGED);
        txn->Abort();
    }
}

void UTHeapMultiThread::LobScan()
{
    BuildThreadLocalVar();
    Transaction *txn = thrd->GetActiveTransaction();

    /* Step 1: Get a snapshot. */
    m_mutex.lock();
    std::list<std::tuple<CommitSeqNo, ItemPointerData, std::string>>::iterator iter = m_csnCtidMap.begin();
    if (iter == m_csnCtidMap.end()) {
        m_mutex.unlock();
        return;
    }
    CommitSeqNo csn = std::get<0>(*iter);
    std::string data = std::get<2>(*iter);
    m_csnCtidMap.erase(iter);
    m_mutex.unlock();

    /* Step 2: Scan all to find the tuple. */
    txn->Start();
    txn->SetSnapshotCsnForFlashback(csn);
    bool found = false;
    {
        StorageRelationData relation;
        relation.tableSmgr = ThdUtTableHandler->GetHeapTabSmgr();
        relation.lobTableSmgr = ThdUtTableHandler->GetLobTabSmgr();
        relation.m_pdbId = g_defaultPdbId;
        HeapScanHandler *heapScan = HeapInterface::CreateHeapScanHandler(&relation);
        HeapInterface::BeginScan(heapScan, txn->GetSnapshot());
        StorageAssert(heapScan != nullptr);
        HeapTuple *tuple;
        while ((tuple = HeapInterface::SeqScan(heapScan)) != nullptr) {
            bool isNull;
            void *tp = DatumGetPointer(
                tuple->GetAttr(CLOB_IDX + 1, ThdUtTableHandler->GetHeapTabSmgr()->GetTupleDesc(), &isNull));
            EXPECT_EQ(isNull, false);
            EXPECT_EQ(DstoreVarAttIsExternalDlob(tp), true);
            VarattLobLocator *lobLocator = STATIC_CAST_PTR_TYPE(VarData1BE(tp), VarattLobLocator *);
            ItemPointerData ctid(lobLocator->ctid);
            varlena *lobValue = HeapInterface::FetchLobValue(&relation, ctid, txn->GetSnapshot());
            if (memcmp(VarData4B(lobValue), data.c_str(), data.length()) == 0) {
                found = true;
                DstorePfreeExt(lobValue);
                break;
            }
            DstorePfreeExt(lobValue);
        }
        HeapInterface::EndScan(heapScan);
        HeapInterface::DestroyHeapScanHandler(heapScan);
    }
    txn->Commit();
    EXPECT_TRUE(found);

    thrd->m_memoryMgr->reset_group_context(DSTORE::MEMORY_CONTEXT_QUERY);
}

void UTHeapMultiThread::LobFetch()
{
    BuildThreadLocalVar();
    Transaction *txn = thrd->GetActiveTransaction();

    /* Step 1: Get a snapshot. */
    m_mutex.lock();
    std::list<std::tuple<CommitSeqNo, ItemPointerData, std::string>>::iterator iter = m_csnCtidMap.begin();
    if (iter == m_csnCtidMap.end()) {
        m_mutex.unlock();
        return;
    }
    CommitSeqNo csn = std::get<0>(*iter);
    ItemPointerData ctid = std::get<1>(*iter);
    std::string data = std::get<2>(*iter);
    m_csnCtidMap.erase(iter);
    m_mutex.unlock();

    /* Step 2: Fetch tuple using snapshot and compare. */
    txn->Start();
    txn->SetSnapshotCsnForFlashback(csn);
    StorageRelationData relation;
    relation.tableSmgr = ThdUtTableHandler->GetHeapTabSmgr();
    relation.lobTableSmgr = ThdUtTableHandler->GetLobTabSmgr();
    relation.m_pdbId = g_defaultPdbId;
    HeapScanHandler *heapScan = HeapInterface::CreateHeapScanHandler(&relation);
    HeapInterface::BeginScan(heapScan, txn->GetSnapshot());
    StorageAssert(heapScan != nullptr);
    HeapTuple *resTuple = HeapInterface::FetchTuple(heapScan, ctid);
    HeapInterface::EndScan(heapScan);
    HeapInterface::DestroyHeapScanHandler(heapScan);
    bool isNull;
    void *tp = DatumGetPointer(
        resTuple->GetAttr(CLOB_IDX + 1, ThdUtTableHandler->GetHeapTabSmgr()->GetTupleDesc(), &isNull));
    EXPECT_EQ(isNull, false);
    EXPECT_EQ(DstoreVarAttIsExternalDlob(tp), true);
    VarattLobLocator *lobLocator = STATIC_CAST_PTR_TYPE(VarData1BE(tp), VarattLobLocator *);
    ItemPointerData lobCtid(lobLocator->ctid);
    varlena *lobValue = HeapInterface::FetchLobValue(&relation, lobCtid, txn->GetSnapshot());
    txn->Commit();
    EXPECT_EQ(memcmp(VarData4B(lobValue), data.c_str(), data.length()), 0);
    DstorePfreeExt(lobValue);
    DstorePfreeExt(resTuple);

    thrd->m_memoryMgr->reset_group_context(DSTORE::MEMORY_CONTEXT_QUERY);
}

void UTHeapMultiThread::LobInsert(int tupleLen)
{
    BuildThreadLocalVar();
    Transaction *txn = thrd->GetActiveTransaction();
    TransactionMgr *transactionMgr= g_storageInstance->GetPdb(g_defaultPdbId)->GetTransactionMgr();

    /* Step 1: Insert one tuple. */
    txn->Start();
    txn->SetSnapshotCsn();
    txn->SetSnapshotCsnForFlashback(txn->GetSnapshotData()->GetCsn());
    std::string data = GenerateRandomString(tupleLen);
    HeapTuple *heapTuple = GenerateTupleWithLob(data, tupleLen);
    ItemPointerData ctid = INVALID_ITEM_POINTER;
    StorageRelationData relation;
    relation.tableSmgr = ThdUtTableHandler->GetHeapTabSmgr();
    relation.lobTableSmgr = ThdUtTableHandler->GetLobTabSmgr();
    relation.m_pdbId = g_defaultPdbId;
    HeapInterface::Insert(&relation, heapTuple, ctid, txn->GetCurCid());
    Xid xid = txn->GetCurrentXid();
    txn->Commit();
    DstorePfree(heapTuple);

    /* Step 2: Construct snapshot for future fetch, add ctid to queue for update/delete. */
    CommitSeqNo csn = transactionMgr->GetCsnFromXid(xid);
    m_mutex.lock();
    m_csnCtidMap.push_back(std::tuple<CommitSeqNo, ItemPointerData, std::string>(csn + 1, ctid, data));
    m_ctids.push(ctid);
    m_mutex.unlock();

    thrd->m_memoryMgr->reset_group_context(DSTORE::MEMORY_CONTEXT_QUERY);
}

void UTHeapMultiThread::LobDelete()
{
    BuildThreadLocalVar();
    Transaction *txn = thrd->GetActiveTransaction();

    /* Step 1: Get a ctid. */
    m_mutex.lock();
    if (m_ctids.empty()) {
        m_mutex.unlock();
        return;
    }
    ItemPointerData ctid = m_ctids.front();
    m_ctids.pop();
    m_mutex.unlock();

    /* Step 2: Delete it. */
    txn->Start();
    txn->SetSnapshotCsn();
    HeapDeleteContext deleteContext;
    deleteContext.ctid = ctid;
    deleteContext.needDeleteLob = true;
    deleteContext.needReturnTup = false;
    deleteContext.returnTup = nullptr;
    deleteContext.snapshot = *txn->GetSnapshotData();
    deleteContext.cid = txn->GetCurCid();
    StorageRelationData relation;
    relation.tableSmgr = ThdUtTableHandler->GetHeapTabSmgr();
    relation.lobTableSmgr = ThdUtTableHandler->GetLobTabSmgr();
    relation.m_pdbId = g_defaultPdbId;
    HeapInterface::Delete(&relation, &deleteContext);
    txn->Commit();
}

void UTHeapMultiThread::LobUpdate(int tupleLen)
{
    BuildThreadLocalVar();
    Transaction *txn = thrd->GetActiveTransaction();
    TransactionMgr *transactionMgr = g_storageInstance->GetPdb(g_defaultPdbId)->GetTransactionMgr();

    /* Step 1: Get a ctid. */
    m_mutex.lock();
    if (m_ctids.empty()) {
        m_mutex.unlock();
        return;
    }
    ItemPointerData ctid = m_ctids.front();
    m_ctids.pop();
    m_mutex.unlock();

    /* Step 2: Update tuple of given ctid. */
    txn->Start();
    txn->SetSnapshotCsn();
    txn->SetSnapshotCsnForFlashback(txn->GetSnapshotData()->GetCsn());
    std::string data = GenerateRandomString(tupleLen);
    HeapTuple *heapTuple = GenerateTupleWithLob(data, tupleLen);
    HeapUpdateContext updateContext;
    updateContext.oldCtid = ctid;
    updateContext.newCtid = INVALID_ITEM_POINTER;
    updateContext.newTuple = heapTuple;
    updateContext.needUpdateLob = true;
    updateContext.needReturnOldTup = true;
    updateContext.snapshot = *txn->GetSnapshotData();
    updateContext.cid = txn->GetCurCid();
    StorageRelationData relation;
    relation.tableSmgr = ThdUtTableHandler->GetHeapTabSmgr();
    relation.lobTableSmgr = ThdUtTableHandler->GetLobTabSmgr();
    relation.m_pdbId = g_defaultPdbId;
    HeapInterface::Update(&relation, &updateContext);
    ItemPointerData newCtid = updateContext.newCtid;
    Xid xid = txn->GetCurrentXid();
    txn->Commit();
    DstorePfree(heapTuple);

    if (newCtid != INVALID_ITEM_POINTER) {
        /* Step 3: Construct snapshot for future fetch, add ctid to queue for update/delete. */
        CommitSeqNo csn = transactionMgr->GetCsnFromXid(xid);
        m_mutex.lock();
        m_csnCtidMap.push_back(std::tuple<CommitSeqNo, ItemPointerData, std::string>(csn + 1, newCtid, data));
        m_ctids.push(newCtid);
        m_mutex.unlock();
    }

    thrd->m_memoryMgr->reset_group_context(DSTORE::MEMORY_CONTEXT_QUERY);
}

void UTHeapMultiThread::LobDeleteConcurrently()
{
    BuildThreadLocalVar();
    Transaction *txn = thrd->GetActiveTransaction();

    /* Step 1: Get a ctid, but keep it in map so that all transactions update the same tuple. */
    m_mutex.lock();
    std::list<std::tuple<CommitSeqNo, ItemPointerData, std::string>>::iterator iter = m_csnCtidMap.begin();
    if (iter == m_csnCtidMap.end()) {
        m_mutex.unlock();
        return;
    }
    CommitSeqNo csn = std::get<0>(*iter);
    ItemPointerData ctid = std::get<1>(*iter);
    m_mutex.unlock();

    /* Step 2: Update tuple of given snapshot. */
    txn->Start();
    txn->SetSnapshotCsnForFlashback(csn);
    HeapDeleteContext deleteContext;
    deleteContext.ctid = ctid;
    deleteContext.needDeleteLob = true;
    deleteContext.needReturnTup = false;
    deleteContext.returnTup = nullptr;
    deleteContext.snapshot = *txn->GetSnapshotData();
    deleteContext.cid = txn->GetCurCid();
    StorageRelationData relation;
    relation.tableSmgr = ThdUtTableHandler->GetHeapTabSmgr();
    relation.lobTableSmgr = ThdUtTableHandler->GetLobTabSmgr();
    relation.m_pdbId = g_defaultPdbId;
    RetStatus ret = HeapInterface::Delete(&relation, &deleteContext);

    /* Step 3: Record result. */
    if (ret == DSTORE_SUCC) {
        txn->Commit();
        m_counter++;
    } else {
        EXPECT_TRUE(StorageGetErrorCode() == HEAP_ERROR_TUPLE_IS_CHANGED);
        txn->Abort();
    }
}

void UTHeapMultiThread::LobUpdateConcurrently(int tupleLen)
{
    BuildThreadLocalVar();
    Transaction *txn = thrd->GetActiveTransaction();

    /* Step 1: Get a ctid, but keep it in map so that all transactions update the same tuple. */
    m_mutex.lock();
    std::list<std::tuple<CommitSeqNo, ItemPointerData, std::string>>::iterator iter = m_csnCtidMap.begin();
    if (iter == m_csnCtidMap.end()) {
        m_mutex.unlock();
        return;
    }
    CommitSeqNo csn = std::get<0>(*iter);
    ItemPointerData ctid = std::get<1>(*iter);
    m_mutex.unlock();

    /* Step 2: Update tuple of given snapshot. */
    txn->Start();
    txn->SetSnapshotCsnForFlashback(csn);
    std::string data = GenerateRandomString(tupleLen);
    HeapTuple *heapTuple = GenerateTupleWithLob(data, tupleLen);
    HeapUpdateContext updateContext;
    updateContext.oldCtid = ctid;
    updateContext.newCtid = INVALID_ITEM_POINTER;
    updateContext.newTuple = heapTuple;
    updateContext.needUpdateLob = true;
    updateContext.needReturnOldTup = true;
    updateContext.snapshot = *txn->GetSnapshotData();
    updateContext.cid = txn->GetCurCid();
    StorageRelationData relation;
    relation.tableSmgr = ThdUtTableHandler->GetHeapTabSmgr();
    relation.lobTableSmgr = ThdUtTableHandler->GetLobTabSmgr();
    relation.m_pdbId = g_defaultPdbId;
    RetStatus ret = HeapInterface::Update(&relation, &updateContext);
    DstorePfree(heapTuple);

    /* Step 3: Record result. */
    if (ret == DSTORE_SUCC) {
        txn->Commit();
        m_counter++;
    } else {
        EXPECT_TRUE(StorageGetErrorCode() == HEAP_ERROR_TUPLE_IS_CHANGED);
        txn->Abort();
    }

    thrd->m_memoryMgr->reset_group_context(DSTORE::MEMORY_CONTEXT_QUERY);
}

TEST_F(UTHeapMultiThread, HeapMultiThreadInsertFetchTest_level0)
{
    const int TUPLE_NUM = 1000;
    const int TUPLE_LEN = 33;

    for (int i = 0; i < TUPLE_NUM; i++) {
        m_pool.AddTask(HeapInsertTask, this, TUPLE_LEN);
    }

    for (int i = 0; i < TUPLE_NUM; i++) {
        m_pool.AddTask(HeapFetchTask, this);
    }

    m_pool.WaitAllTaskFinish();
}

TEST_F(UTHeapMultiThread, HeapMultiThreadBatchInsertFetchTest_level0)
{
    const int TUPLE_NUM = 100;
    const int THREAD_NUM = 10;

    for (int i = 0; i < THREAD_NUM; i++) {
        m_pool.AddTask(HeapBatchInsertTask, this, TUPLE_NUM);
    }

    for (int i = 0; i < THREAD_NUM; i++) {
        m_pool.AddTask(HeapFetchTask, this);
    }

    m_pool.WaitAllTaskFinish();
}

TEST_F(UTHeapMultiThread, HeapMultiThreadInsertScanTest_level0)
{
    const int TUPLE_NUM = 1000;
    const int TUPLE_LEN = 33;

    for (int i = 0; i < TUPLE_NUM; i++) {
        m_pool.AddTask(HeapInsertTask, this, TUPLE_LEN);
    }

    for (int i = 0; i < TUPLE_NUM; i++) {
        m_pool.AddTask(HeapScanTask, this);
    }

    m_pool.WaitAllTaskFinish();
}

TEST_F(UTHeapMultiThread, HeapMultiThreadInplaceUpdateTest_level0)
{
    const int TUPLE_NUM = 1000;
    const int TUPLE_LEN = 3000;

    for (int i = 0; i < TUPLE_NUM; i++) {
        m_pool.AddTask(HeapInsertTask, this, TUPLE_LEN);
    }

    for (int i = 0; i < TUPLE_NUM; i++) {
        m_pool.AddTask(HeapUpdateTask, this, TUPLE_LEN);
    }

    int updateLen = TUPLE_LEN;
    for (int i = 0; i < TUPLE_NUM; i++) {
        updateLen -= 1;
        m_pool.AddTask(HeapUpdateTask, this, updateLen);
    }

    for (int i = 0; i < TUPLE_NUM; i++) {
        m_pool.AddTask(HeapFetchTask, this);
    }

    m_pool.WaitAllTaskFinish();
}

TEST_F(UTHeapMultiThread, HeapMultiThreadNotInplaceUpdateTest_level0)
{
    const int TUPLE_NUM = 100;
    const int TUPLE_LEN = 3000;

    for (int i = 0; i < TUPLE_NUM; i++) {
        m_pool.AddTask(HeapInsertTask, this, TUPLE_LEN);
    }

    int updateLen = TUPLE_LEN;
    for (int i = 0; i < TUPLE_NUM; i++) {
        updateLen += 10;
        m_pool.AddTask(HeapUpdateTask, this, updateLen);
    }

    for (int i = 0; i < TUPLE_NUM; i++) {
        m_pool.AddTask(HeapFetchTask, this);
    }

    m_pool.WaitAllTaskFinish();
}

TEST_F(UTHeapMultiThread, HeapMultiThreadConcurrentUpdateTest_level0)
{
    const int TUPLE_LEN = 3000;
    m_counter = 0;

    m_pool.AddTask(HeapInsertTask, this, TUPLE_LEN);

    /* After finish insert, and then do update */
    m_pool.WaitAllTaskFinish();
    for (int i = 0; i < 100; i++) {
        m_pool.AddTask(HeapConcurrentUpdateTask, this, TUPLE_LEN);
    }
    m_pool.WaitAllTaskFinish();
    m_pool.AddTask(HeapFetchTask, this);
    m_pool.WaitAllTaskFinish();
    EXPECT_TRUE(m_counter.load() == 1);
}

TEST_F(UTHeapMultiThread, HeapMultiThreadDeleteTest_level0)
{
    const int TUPLE_NUM = 1000;
    const int TUPLE_LEN = 33;

    for (int i = 0; i < TUPLE_NUM; i++) {
        m_pool.AddTask(HeapInsertTask, this, TUPLE_LEN);
    }

    for (int i = 0; i < TUPLE_NUM; i++) {
        m_pool.AddTask(HeapDeleteTask, this);
    }

    for (int i = 0; i < TUPLE_NUM; i++) {
        m_pool.AddTask(HeapFetchTask, this);
    }

    m_pool.WaitAllTaskFinish();
}

TEST_F(UTHeapMultiThread, HeapMultiThreadMixedTest_level0)
{
    const int TUPLE_NUM = 500;
    const int TUPLE_LEN = 33;

    for (int i = 0; i < TUPLE_NUM; i++) {
        m_pool.AddTask(HeapInsertTask, this, TUPLE_LEN);
    }
    m_pool.WaitAllTaskFinish();

    for (int i = 0; i < TUPLE_NUM; i++) {
        m_pool.AddTask(HeapUpdateTask, this, TUPLE_LEN);
    }

    for (int i = 0; i < TUPLE_NUM; i++) {
        m_pool.AddTask(HeapUpdateTask, this, TUPLE_LEN + (i % 2 == 0) ? 8 : (-8));
    }
    for (int i = 0; i < TUPLE_NUM; i++) {
        m_pool.AddTask(HeapFetchTask, this);
    }

    for (int i = 0; i < TUPLE_NUM; i++) {
        m_pool.AddTask(HeapDeleteTask, this);
    }
    for (int i = 0; i < TUPLE_NUM; i++) {
        m_pool.AddTask(HeapScanTask, this);
    }
    for (int i = 0; i < TUPLE_NUM; i++) {
        m_pool.AddTask(HeapFetchTask, this);
    }
    m_pool.WaitAllTaskFinish();
}

TEST_F(UTHeapMultiThread, HeapMultiThreadConcurrentDeleteTest_level0)
{
    const int TUPLE_LEN = 3000;
    m_counter = 0;

    m_pool.AddTask(HeapInsertTask, this, TUPLE_LEN);
    m_pool.WaitAllTaskFinish();

    for (int i = 0; i < 100; i++) {
        m_pool.AddTask(HeapConcurrentDeleteTask, this);
    }

    m_pool.WaitAllTaskFinish();
    EXPECT_TRUE(m_counter.load() == 1);
}

TEST_F(UTHeapMultiThread, HeapMultiThreadConcurrentUpdateDeleteTest_level0)
{
    const int TUPLE_LEN = 3000;
    m_counter = 0;

    for (int i = 0; i < 20; i++) {
        m_pool.AddTask(HeapInsertTask, this, TUPLE_LEN);
    }
    m_pool.WaitAllTaskFinish();

    for (int i = 0; i < 100; i++) {
        m_pool.AddTask(HeapConcurrentUpdateTask, this, TUPLE_LEN);
        m_pool.AddTask(HeapConcurrentDeleteTask, this);
    }

    m_pool.WaitAllTaskFinish();
    EXPECT_TRUE(m_counter.load() == 1);
}

TEST_F(UTHeapMultiThread, HeapConcurrentMixedBigTupleTest_level0)
{
    const int TUPLE_NUM = 2;
    const int TUPLE_LEN = 9000;

    for (int i = 0; i < TUPLE_NUM; i++) {
        m_pool.AddTask(HeapInsertTask, this, TUPLE_LEN);
    }

    for (int i = 0; i < TUPLE_NUM; i++) {
        m_pool.AddTask(HeapUpdateTask, this, TUPLE_LEN);
    }

    int updateLen = TUPLE_LEN;
    for (int i = 0; i < TUPLE_NUM; i++) {
        updateLen += 1000;
        m_pool.AddTask(HeapUpdateTask, this, updateLen);
    }

    for (int i = 0; i < TUPLE_NUM; i++) {
        m_pool.AddTask(HeapDeleteTask, this);
    }

    for (int i = 0; i < TUPLE_NUM; i++) {
        m_pool.AddTask(HeapFetchTask, this);
    }
    for (int i = 0; i < TUPLE_NUM; i++) {
        m_pool.AddTask(HeapScanTask, this);
    }

    m_pool.WaitAllTaskFinish();
}

TEST_F(UTHeapMultiThread, HeapConcurrentUpdateBigTupTest_level0)
{
    const int TUPLE_LEN = 9000;
    m_counter = 0;

    for (int i = 0; i < 20; i++) {
        m_pool.AddTask(HeapInsertTask, this, TUPLE_LEN);
    }

    /* After finish insert, and then do update */
    m_pool.WaitAllTaskFinish();
    for (int i = 0; i < 100; i++) {
        m_pool.AddTask(HeapConcurrentUpdateTask, this, TUPLE_LEN);
    }
    m_pool.WaitAllTaskFinish();
    for (int i = 0; i < 20; i++) {
        m_pool.AddTask(HeapFetchTask, this);
    }
    m_pool.WaitAllTaskFinish();
    EXPECT_TRUE(m_counter.load() == 1);
}

TEST_F(UTHeapMultiThread, HeapConcurrentDeleteBigTupTest_level0)
{
    const int TUPLE_LEN = 9000;
    m_counter = 0;
    m_pool.AddTask(HeapInsertTask, this, TUPLE_LEN);

    /* After finish insert, and then do delete */
    m_pool.WaitAllTaskFinish();
    for (int i = 0; i < 100; i++) {
        m_pool.AddTask(HeapConcurrentDeleteTask, this);
    }
    m_pool.WaitAllTaskFinish();
    for (int i = 0; i < 20; i++) {
        m_pool.AddTask(HeapFetchTask, this);
    }
    m_pool.WaitAllTaskFinish();
    EXPECT_TRUE(m_counter.load() == 1);
}

TEST_F(UTHeapMultiThread, HeapSelectForUpdateTest_level0)
{
    const int TUPLE_LEN = 1024;
    m_counter = 0;
    m_pool.AddTask(HeapInsertTask, this, TUPLE_LEN);
    m_pool.WaitAllTaskFinish();

    m_pool.AddTask(HeapLockTupDelayCommitTask, this);
    ItemPointerData ctid = m_ctids.front();
    /* wait for locking tuple */
    while (m_counter.load() == 0) {
        usleep(1000);
    }
    int result = DeleteTuple(&ctid);
    ASSERT_EQ(result, DSTORE_SUCC);
    ASSERT_EQ(m_counter.load(), 2);

    m_pool.WaitAllTaskFinish();
}

void UTHeapMultiThread::HeapInsertFixedLengthTupleWithFixedTabSmgr(int tupleLen)
{
    Transaction *txn = thrd->GetActiveTransaction();
    TransactionMgr *transactionMgr= g_storageInstance->GetPdb(g_defaultPdbId)->GetTransactionMgr();

    /* Step 1: Insert one tuple. */
    txn->Start();
    txn->SetSnapshotCsn();
    std::string data = GenerateRandomString(tupleLen);
    HeapTuple *heapTuple = UTTableHandler::GenerateSpecificHeapTuple(data);
    ItemPointerData ctid = m_utTableHandler->InsertHeapTupAndCheckResult(heapTuple, true, txn->GetSnapshotData());
    Xid xid = txn->GetCurrentXid();
    txn->Commit();
    DstorePfree(heapTuple);

    /* Step 2: Construct snapshot for future fetch, add ctid to queue for update/delete. */
    m_mutex.lock();
    m_ctids.push(ctid);
    m_mutex.unlock();

    thrd->m_memoryMgr->reset_group_context(DSTORE::MEMORY_CONTEXT_QUERY);
}

void UTHeapMultiThread::HeapUpdateTupleAndWait(ItemPointerData ctid, int tupleLen,
    SyncPointGroup *syncPointGroup, int syncPointId)
{
    BuildThreadLocalVar();
    Transaction *txn = thrd->GetActiveTransaction();

    /* Update tuple of given ctid. */
    txn->Start();
    txn->SetSnapshotCsn();
    std::string data = GenerateRandomString(tupleLen);
    HeapTuple *heapTuple = UTTableHandler::GenerateSpecificHeapTuple(data);
    ItemPointerData newCtid = UpdateTupAndCheckResult(&ctid, heapTuple, txn->GetSnapshotData(), true, ThdUtTableHandler);
    Xid xid = txn->GetCurrentXid();

    /* Just wait until syncpoint to commit. */
    syncPointGroup->SyncPoint(syncPointId);
    syncPointGroup->SyncPoint(syncPointId);

    txn->Commit();
    DstorePfree(heapTuple);

    EXPECT_TRUE(newCtid != INVALID_ITEM_POINTER);
}

void UTHeapMultiThread::HeapUpdateTupleAndWaitTask(UTHeapMultiThread *ptr, ItemPointerData ctid, int tupleLen,
    SyncPointGroup *syncPointGroup, int syncPointId)
{
    ptr->HeapUpdateTupleAndWait(ctid, tupleLen, syncPointGroup, syncPointId);
}

void UTHeapMultiThread::HeapUpdateTupleGivenCtid(ItemPointerData ctid, int tupleLen)
{
    BuildThreadLocalVar();
    std::string data = GenerateRandomString(tupleLen);
    HeapTuple *newTuple = UTTableHandler::GenerateSpecificHeapTuple(data);
    ItemPointerData newCtid = UpdateTuple(&ctid, newTuple, INVALID_SNAPSHOT, false, ThdUtTableHandler);
    EXPECT_EQ(newCtid, ctid);
    DstorePfreeExt(newTuple);
}

void UTHeapMultiThread::HeapUpdateTupleGivenCtidTask(UTHeapMultiThread *ptr, ItemPointerData ctid, int tupleLen)
{
    ptr->HeapUpdateTupleGivenCtid(ctid, tupleLen);
}

/**
 * This test case show how a update transaction would wait for an active transaction on page
 * to finish when the page doesn't have enough space for td allocation.
 */
TEST_F(UTHeapMultiThread, HeapUpdateAllocTdFailAndWaitTest_level1)
{
    std::list<ItemPointerData> ctids;
    int tupleLen = 1;

    /* If there is no space for a new tuple, there is no space for alloc new td. */
    HeapTuple *heapTuple = UTTableHandler::GenerateSpecificHeapTuple("a");
    EXPECT_TRUE(sizeof(TD) > heapTuple->GetDiskTupleSize());
    DstorePfree(heapTuple);

    /* Insert one tuple first and get page id. */
    HeapInsertFixedLengthTupleWithFixedTabSmgr(tupleLen);
    ItemPointerData ctid = m_ctids.front();
    m_ctids.pop();
    PageId pageId = ctid.GetPageId();
    ctids.push_back(ctid);

    /* Keep inserting to the same page until the page is full. */
    while (true) {
        HeapInsertFixedLengthTupleWithFixedTabSmgr(tupleLen);
        ctid = m_ctids.front();
        if (ctid.GetPageId() != pageId) {
            break;
        }
        m_ctids.pop();
        ctids.push_back(ctid);
    }

    /* Start 4 threads to occupy all tds, make sure they finished update but didn't commit. */
    SyncPointGroup syncPointGroup{DEFAULT_TD_COUNT + 1};
    std::list<ItemPointerData>::iterator iter = ctids.begin();
    for (int i = 0; i < DEFAULT_TD_COUNT; i++) {
        ItemPointerData updateCtid = *iter;
        m_pool.AddTask(HeapUpdateTupleAndWaitTask, this, updateCtid, tupleLen, &syncPointGroup, i + 2);
        iter++;
    }
    syncPointGroup.SyncPoint(1);

    /* Try update on 4 other threads, the page should be running out of td space now. */
    for (int i = 0; i < 4; i++) {
        ItemPointerData updateCtid = *iter;
        m_pool.AddTask(HeapUpdateTupleGivenCtidTask, this, updateCtid, tupleLen);
        iter++;
    }

    /* Just wait, the new update should not return fail. */
    sleep(10);

    /* Wake up the first 4 threads to commit transaction. */
    syncPointGroup.SyncPoint(1);

    /* Suppose all thread update successfully. */
    m_pool.WaitAllTaskFinish();
}

void UTHeapMultiThread::HeapUpdateTupleAndWaitOnLock(ItemPointerData ctid, int tupleLen,
    SyncPointGroup *syncPointGroup, int syncPointId)
{
    BuildThreadLocalVar();
    /* Start transaction first to avoid being rollbacked. */
    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    syncPointGroup->SyncPoint(syncPointId);

    /* Wait until other thread hold table lock. */
    syncPointGroup->SyncPoint(syncPointId);

    /* Update tuple of given ctid. */
    txn->SetSnapshotCsn();
    std::string data = GenerateRandomString(tupleLen);
    HeapTuple *heapTuple = UTTableHandler::GenerateSpecificHeapTuple(data);
    ItemPointerData newCtid = UpdateTupAndCheckResult(&ctid, heapTuple, txn->GetSnapshotData(), true, ThdUtTableHandler);
    EXPECT_TRUE(newCtid != INVALID_ITEM_POINTER);

    /* Wait until syncpoint to start requesting table lock. */
    syncPointGroup->SyncPoint(syncPointId);

    LockTag locktag;
    locktag.SetTableLockTag(0, 0);
    RetStatus ret = g_storageInstance->GetTableLockMgr()->Lock(&locktag, DSTORE_ACCESS_SHARE_LOCK,
        false, nullptr);
    if (STORAGE_FUNC_FAIL(ret)) {
        EXPECT_TRUE(false);
        txn->Abort();
    } else {
        g_storageInstance->GetTableLockMgr()->Unlock(&locktag, DSTORE_ACCESS_SHARE_LOCK);
        txn->Commit();
    }

    DstorePfree(heapTuple);
}

void UTHeapMultiThread::HeapUpdateTupleAndWaitOnLockTask(UTHeapMultiThread *ptr, ItemPointerData ctid, int tupleLen,
    SyncPointGroup *syncPointGroup, int syncPointId)
{
    ptr->HeapUpdateTupleAndWaitOnLock(ctid, tupleLen, syncPointGroup, syncPointId);
}

void UTHeapMultiThread::AddUpdateTaskForTdDeadlock(ItemPointerData *outCtid, SyncPointGroup *syncPointGroup)
{
    std::list<ItemPointerData> ctids;
    int tupleLen = 1;

    /* If there is no space for a new tuple, there is no space for alloc new td. */
    HeapTuple *heapTuple = UTTableHandler::GenerateSpecificHeapTuple("a");
    EXPECT_TRUE(sizeof(TD) > heapTuple->GetDiskTupleSize());
    DstorePfree(heapTuple);

    /* Insert one tuple first and get page id. */
    HeapInsertFixedLengthTupleWithFixedTabSmgr(tupleLen);
    ItemPointerData ctid = m_ctids.front();
    m_ctids.pop();
    PageId pageId = ctid.GetPageId();
    ctids.push_back(ctid);

    /* Keep inserting to the same page until the page is full. */
    while (true) {
        HeapInsertFixedLengthTupleWithFixedTabSmgr(tupleLen);
        ctid = m_ctids.front();
        if (ctid.GetPageId() != pageId) {
            break;
        }
        m_ctids.pop();
        ctids.push_back(ctid);
    }

    /* Start 4 threads to occupy all tds, they do update on the page and then wait on table lock. */
    std::list<ItemPointerData>::iterator iter = ctids.begin();
    for (int i = 0; i < DEFAULT_TD_COUNT; i++) {
        ItemPointerData updateCtid = *iter;
        m_pool.AddTask(HeapUpdateTupleAndWaitOnLockTask, this, updateCtid, tupleLen, syncPointGroup, i + 2);
        iter++;
    }

    *outCtid = *iter;
    EXPECT_TRUE(*outCtid != INVALID_ITEM_POINTER);
}

void UTHeapMultiThread::HeapUpdateTupleGivenCtidWhileHoldingLock(ItemPointerData ctid, int tupleLen,
    SyncPointGroup *syncPointGroup, int syncPointId)
{
    BuildThreadLocalVar();
    Transaction *txn = thrd->GetActiveTransaction();
    syncPointGroup->SyncPoint(syncPointId);

    txn->Start();
    txn->SetSnapshotCsn();

    LockTag locktag;
    locktag.SetTableLockTag(0, 0);
    RetStatus ret = g_storageInstance->GetTableLockMgr()->Lock(&locktag,
        DSTORE_ACCESS_EXCLUSIVE_LOCK, false, nullptr);
    EXPECT_TRUE(ret == DSTORE_SUCC);

    syncPointGroup->SyncPoint(syncPointId);
    syncPointGroup->SyncPoint(syncPointId);

    std::string data = GenerateRandomString(tupleLen);
    HeapTuple *newTuple = UTTableHandler::GenerateSpecificHeapTuple(data);

    HeapUpdateContext updateContext;
    updateContext.oldCtid = ctid;
    updateContext.newTuple = newTuple;
    updateContext.needReturnOldTup = true;
    updateContext.snapshot = *txn->GetSnapshotData();
    updateContext.cid = txn->GetCurCid();
    std::unique_ptr<StorageRelationData> heapRel(new StorageRelationData());
    heapRel->tableSmgr = ThdUtTableHandler->GetHeapTabSmgr();
    heapRel->m_pdbId = g_defaultPdbId;
    {
        HeapUpdateHandler heapUpdate(g_storageInstance, thrd, heapRel.get());
        ret = heapUpdate.Update(&updateContext);
    }
    EXPECT_TRUE(STORAGE_FUNC_FAIL(ret));

    g_storageInstance->GetTableLockMgr()->Unlock(&locktag, DSTORE_ACCESS_EXCLUSIVE_LOCK);

    txn->Abort();
    DstorePfreeExt(newTuple);
}

void UTHeapMultiThread::HeapUpdateTupleGivenCtidWhileHoldingLockTask(UTHeapMultiThread *ptr, ItemPointerData ctid,
    int tupleLen, SyncPointGroup *syncPointGroup, int syncPointId)
{
    ptr->HeapUpdateTupleGivenCtidWhileHoldingLock(ctid, tupleLen, syncPointGroup, syncPointId);
}

/**
 * This test case shows when a page doesn't have enough space for td allocation,
 * and there is a deadlock exist between those transaction, any one of them would abort.
 */
TEST_F(UTHeapMultiThread, DISABLED_HeapUpdateAllocTdFailAndDeadlockTest_level0)
{
    SyncPointGroup syncPointGroup(DEFAULT_TD_COUNT + 1);
    ItemPointerData testCtid = INVALID_ITEM_POINTER;
    AddUpdateTaskForTdDeadlock(&testCtid, &syncPointGroup);

    /* The page should be running out of td space now, try update. */
    m_pool.AddTask(HeapUpdateTupleGivenCtidWhileHoldingLockTask, this, testCtid, 1, &syncPointGroup, 1);

    m_pool.WaitAllTaskFinish();
}

void UTHeapMultiThread::HeapLockTupleGivenCtidWhileHoldingLock(ItemPointerData ctid, SyncPointGroup *syncPointGroup,
    int syncPointId)
{
    BuildThreadLocalVar();
    Transaction *txn = thrd->GetActiveTransaction();
    syncPointGroup->SyncPoint(syncPointId);

    txn->Start();
    txn->SetSnapshotCsn();

    LockTag locktag;
    locktag.SetTableLockTag(0, 0);
    RetStatus ret = g_storageInstance->GetTableLockMgr()->Lock(&locktag,
        DSTORE_ACCESS_EXCLUSIVE_LOCK, false, nullptr);
    EXPECT_TRUE(ret == DSTORE_SUCC);

    syncPointGroup->SyncPoint(syncPointId);
    syncPointGroup->SyncPoint(syncPointId);

    HeapLockTupleContext context;
    context.ctid = ctid;
    std::unique_ptr<StorageRelationData> heapRel(new StorageRelationData());
    heapRel->tableSmgr = ThdUtTableHandler->GetHeapTabSmgr();
    heapRel->m_pdbId = g_defaultPdbId;
    {
        HeapLockTupHandler lockTuple(g_storageInstance, thrd, heapRel.get());
        int result = lockTuple.LockUnchangedTuple(&context);
        EXPECT_TRUE(STORAGE_FUNC_FAIL(result));
    }

    g_storageInstance->GetTableLockMgr()->Unlock(&locktag, DSTORE_ACCESS_EXCLUSIVE_LOCK);

    txn->Abort();
}

void UTHeapMultiThread::HeapLockTupleGivenCtidWhileHoldingLockTask(UTHeapMultiThread *ptr, ItemPointerData ctid,
    SyncPointGroup *syncPointGroup, int syncPointId)
{
    ptr->HeapLockTupleGivenCtidWhileHoldingLock(ctid, syncPointGroup, syncPointId);
}

TEST_F(UTHeapMultiThread, DISABLED_HeapLockAllocTdFailAndDeadlockTest_level0)
{
    SyncPointGroup syncPointGroup(DEFAULT_TD_COUNT + 1);
    ItemPointerData testCtid = INVALID_ITEM_POINTER;
    AddUpdateTaskForTdDeadlock(&testCtid, &syncPointGroup);

    /* The page should be running out of td space now, try lock. */
    m_pool.AddTask(HeapLockTupleGivenCtidWhileHoldingLockTask, this, testCtid, &syncPointGroup, 1);

    m_pool.WaitAllTaskFinish();
}

void UTHeapMultiThread::HeapDeleteTupleGivenCtidWhileHoldingLock(ItemPointerData ctid, SyncPointGroup *syncPointGroup,
    int syncPointId)
{
    BuildThreadLocalVar();
    Transaction *txn = thrd->GetActiveTransaction();
    syncPointGroup->SyncPoint(syncPointId);

    txn->Start();
    txn->SetSnapshotCsn();

    LockTag locktag;
    locktag.SetTableLockTag(0, 0);
    RetStatus ret = g_storageInstance->GetTableLockMgr()->Lock(&locktag,
        DSTORE_ACCESS_EXCLUSIVE_LOCK, false, nullptr);
    EXPECT_TRUE(ret == DSTORE_SUCC);

    syncPointGroup->SyncPoint(syncPointId);
    syncPointGroup->SyncPoint(syncPointId);

    HeapDeleteContext deleteContext;
    deleteContext.ctid = ctid;
    deleteContext.needReturnTup = false;
    deleteContext.snapshot = *txn->GetSnapshotData();
    deleteContext.cid = txn->GetCurCid();

    std::unique_ptr<StorageRelationData> heapRel(new StorageRelationData());
    heapRel->tableSmgr = ThdUtTableHandler->GetHeapTabSmgr();
    heapRel->m_pdbId = g_defaultPdbId;
    {
        HeapDeleteHandler heapDelete(g_storageInstance, thrd, heapRel.get());
        ret = heapDelete.Delete(&deleteContext);
    }
    EXPECT_TRUE(STORAGE_FUNC_FAIL(ret));

    g_storageInstance->GetTableLockMgr()->Unlock(&locktag, DSTORE_ACCESS_EXCLUSIVE_LOCK);

    txn->Abort();
}

void UTHeapMultiThread::HeapDeleteTupleGivenCtidWhileHoldingLockTask(UTHeapMultiThread *ptr, ItemPointerData ctid,
    SyncPointGroup *syncPointGroup, int syncPointId)
{
    ptr->HeapDeleteTupleGivenCtidWhileHoldingLock(ctid, syncPointGroup, syncPointId);
}

TEST_F(UTHeapMultiThread, DISABLED_HeapDeleteAllocTdFailAndDeadlockTest_level0)
{
    SyncPointGroup syncPointGroup(DEFAULT_TD_COUNT + 1);
    ItemPointerData testCtid = INVALID_ITEM_POINTER;
    AddUpdateTaskForTdDeadlock(&testCtid, &syncPointGroup);

    /* The page should be running out of td space now, try delete. */
    m_pool.AddTask(HeapDeleteTupleGivenCtidWhileHoldingLockTask, this, testCtid, &syncPointGroup, 1);

    m_pool.WaitAllTaskFinish();
}

TEST_F(UTHeapMultiThread, LobConcurrentMixedTest_level0)
{
    const int TUPLE_NUM = 2;
    const int TUPLE_LEN = 9000;

    for (int i = 0; i < TUPLE_NUM; i++) {
        m_pool.AddTask(LobInsertTask, this, TUPLE_LEN);
    }

    for (int i = 0; i < TUPLE_NUM; i++) {
        m_pool.AddTask(LobUpdateTask, this, TUPLE_LEN);
    }

    int updateLen = TUPLE_LEN;
    for (int i = 0; i < TUPLE_NUM; i++) {
        updateLen += 1000;
        m_pool.AddTask(LobUpdateTask, this, updateLen);
    }

    for (int i = 0; i < TUPLE_NUM; i++) {
        m_pool.AddTask(LobDeleteTask, this);
    }

    for (int i = 0; i < TUPLE_NUM; i++) {
        m_pool.AddTask(LobFetchTask, this);
    }
    for (int i = 0; i < TUPLE_NUM; i++) {
        m_pool.AddTask(LobScanTask, this);
    }

    m_pool.WaitAllTaskFinish();
}

TEST_F(UTHeapMultiThread, LobConcurrentUpdateTest_level0)
{
    const int TUPLE_LEN = 9000;
    m_counter = 0;

    for (int i = 0; i < 20; i++) {
        m_pool.AddTask(LobInsertTask, this, TUPLE_LEN);
    }

    /* After finish insert, and then do update */
    m_pool.WaitAllTaskFinish();
    for (int i = 0; i < 100; i++) {
        m_pool.AddTask(LobConcurrentUpdateTask, this, TUPLE_LEN);
    }
    m_pool.WaitAllTaskFinish();
    for (int i = 0; i < 20; i++) {
        m_pool.AddTask(LobFetchTask, this);
    }
    m_pool.WaitAllTaskFinish();
    EXPECT_TRUE(m_counter.load() == 1);
}

TEST_F(UTHeapMultiThread, LobConcurrentDeleteTest_level0)
{
    const int TUPLE_LEN = 9000;
    m_counter = 0;
    m_pool.AddTask(LobInsertTask, this, TUPLE_LEN);

    /* After finish insert, and then do delete */
    m_pool.WaitAllTaskFinish();
    for (int i = 0; i < 100; i++) {
        m_pool.AddTask(LobConcurrentDeleteTask, this);
    }
    m_pool.WaitAllTaskFinish();
    for (int i = 0; i < 20; i++) {
        m_pool.AddTask(LobFetchTask, this);
    }
    m_pool.WaitAllTaskFinish();
    EXPECT_TRUE(m_counter.load() == 1);
}

void UTHeapMultiThread::HeapShowAnyTupleUncommitTuple(int updateDataLen)
{
    BuildThreadLocalVar();

    m_mutex.lock();
    EXPECT_EQ(m_csnCtidMap.size(), 1);
    std::list<std::tuple<CommitSeqNo, ItemPointerData, std::string>>::iterator iter = m_csnCtidMap.begin();
    std::string data = std::get<2>(*iter);
    m_mutex.unlock();

    /* Step 1: prepare scan. */
    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    bool found = false;
    std::unique_ptr<StorageRelationData> heapRel(new StorageRelationData());
    heapRel->tableSmgr = ThdUtTableHandler->GetHeapTabSmgr();
    heapRel->m_pdbId = g_defaultPdbId;

    /* Scan all to find the tuple. */
    int tupleCount = 0;
    HeapTuple *tuple;
    HeapScanHandler heap_scan(g_storageInstance, thrd, heapRel.get());
    heap_scan.Begin(txn->GetSnapshot(), true);
    while ((tuple = heap_scan.SeqScan()) != nullptr) {
        if (memcmp(tuple->GetDiskTuple()->GetData(), data.c_str(), data.length()) == 0) {
            found = true;
        } else {
            uint32 tupSize = tuple->GetDiskTupleSize();
            EXPECT_EQ(tupSize, HEAP_DISK_TUP_HEADER_SIZE + updateDataLen);
        }
        tupleCount++;
    }
    heap_scan.End();

    txn->Commit();
    EXPECT_TRUE(found && tupleCount == 2);

    thrd->m_memoryMgr->reset_group_context(DSTORE::MEMORY_CONTEXT_QUERY);
}

TEST_F(UTHeapMultiThread, HeapShowAnyTupleUncommitTest_level0)
{
    const int TUPLE_LEN = 33;

    /* insert one tuple */
    m_pool.AddTask(HeapInsertTask, this, TUPLE_LEN);
    m_pool.WaitAllTaskFinish();

    /* update the tuple but without commit */
    SyncPointGroup syncPointGroup{1 + 1};
    const int TUPLE_UPDATE_LEN = 11;
    ItemPointerData updateCtid = m_ctids.front();
    m_pool.AddTask(HeapUpdateTupleAndWaitTask, this, updateCtid, TUPLE_UPDATE_LEN, &syncPointGroup, 2);

    syncPointGroup.SyncPoint(1);
    /* check show any tuple fetch the uncommitted update tuple */
    m_pool.AddTask(HeapShowAnyTupleTask, this, TUPLE_UPDATE_LEN);
    syncPointGroup.SyncPoint(1);
    m_pool.AddTask(HeapShowAnyTupleTask, this, TUPLE_UPDATE_LEN);

    m_pool.WaitAllTaskFinish();
}

/* Helper func for HeapSpaceMaxAndAllocTdLockWaitCancel Test.
 * Intert tuples to fill the entire page and return ctids.
 */
void UTHeapMultiThread::FillHeapSpaceForTdAlloc(std::vector<ItemPointerData> &ctids)
{
    /* If there is no space for a new tuple, there is no space for alloc new td. */
    int tupleLen = 1;
    HeapTuple *heapTuple = UTTableHandler::GenerateSpecificHeapTuple("a");
    EXPECT_TRUE(sizeof(TD) > heapTuple->GetDiskTupleSize());
    DstorePfree(heapTuple);

    /* Insert one tuple first and get page id. */
    HeapInsertFixedLengthTupleWithFixedTabSmgr(tupleLen);
    ItemPointerData ctid = m_ctids.front();
    m_ctids.pop();
    PageId pageId = ctid.GetPageId();
    ctids.emplace_back(ctid);

    /* Keep inserting to the same page until the page is full. */
    ctids.clear();
    while (true) {
        HeapInsertFixedLengthTupleWithFixedTabSmgr(tupleLen);
        ctid = m_ctids.front();
        if (ctid.GetPageId() != pageId) {
            pageId = ctid.GetPageId();
            break;
        }
        m_ctids.pop();
        ctids.emplace_back(ctid);
    }
}

void UTHeapMultiThread::HeapUpdateTupleToHoldTd(ItemPointerData ctid, int tupleLen,
    SyncPointGroup *syncPointGroup, int syncPointId, Xid *xid)
{
    BuildThreadLocalVar();
    /* Start transaction first to avoid being rollbacked. */
    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();

    /* Update tuple of given ctid. */
    txn->SetSnapshotCsn();
    std::string data = GenerateRandomString(tupleLen);
    HeapTuple *heapTuple = UTTableHandler::GenerateSpecificHeapTuple(data);
    ItemPointerData newCtid = UpdateTupAndCheckResult(&ctid, heapTuple, txn->GetSnapshotData(), true, ThdUtTableHandler);
    EXPECT_NE(newCtid, INVALID_ITEM_POINTER);

    /* get xid */
    *xid = txn->GetCurrentXid();

    /* Wait until all threads hold xact lock. */
    syncPointGroup->SyncPoint(syncPointId);

    /* Wait until test thread failed lock. */
    syncPointGroup->SyncPoint(syncPointId);

    DstorePfree(heapTuple);
    txn->Commit();
}


void UTHeapMultiThread::HeapUpdateTupleToHoldTdTask(UTHeapMultiThread *ptr, ItemPointerData ctid, int tupleLen,
    SyncPointGroup *syncPointGroup, int syncPointId, Xid *xid)
{
    ptr->HeapUpdateTupleToHoldTd(ctid, tupleLen, syncPointGroup, syncPointId, xid);
}

void UTHeapMultiThread::HeapDeleteTupleForCancel(ItemPointerData ctid, SyncPointGroup *syncPointGroup,
    int syncPointId, ThreadContext **testThrd, uint64 *releaseTime)
{
    BuildThreadLocalVar();
    RetStatus ret;
    *testThrd = thrd;
    Transaction *txn = thrd->GetActiveTransaction();

    txn->Start();
    txn->SetSnapshotCsn();

    /* Wait until all threads hold xact lock. */
    syncPointGroup->SyncPoint(syncPointId);

    HeapDeleteContext deleteContext;
    deleteContext.ctid = ctid;
    deleteContext.needReturnTup = false;
    deleteContext.snapshot = *txn->GetSnapshotData();
    deleteContext.cid = txn->GetCurCid();

    std::unique_ptr<StorageRelationData> heapRel(new StorageRelationData());
    heapRel->tableSmgr = ThdUtTableHandler->GetHeapTabSmgr();
    heapRel->m_pdbId = g_defaultPdbId;
    {
        HeapDeleteHandler heapDelete(g_storageInstance, thrd, heapRel.get());
        ret = heapDelete.Delete(&deleteContext);
    }
    *releaseTime = GetSystemTimeInMicrosecond();
    EXPECT_TRUE(STORAGE_FUNC_FAIL(ret));
    EXPECT_EQ(StorageGetErrorCode(), LOCK_ERROR_WAIT_CANCELED);

    /* Test thread failed lock now. */
    syncPointGroup->SyncPoint(syncPointId);

    txn->Abort();
}

void UTHeapMultiThread::HeapDeleteTupleForCancelTask(UTHeapMultiThread *ptr, ItemPointerData ctid,
    SyncPointGroup *syncPointGroup, int syncPointId, ThreadContext **testThrd, uint64 *releaseTime)
{
    ptr->HeapDeleteTupleForCancel(ctid, syncPointGroup, syncPointId, testThrd, releaseTime);
}

/* Helper func for HeapTdMaxAndAllocTdLockWaitCancel Test.
 * Insert given num tuples and return ctids.
 * In this unit test, tuples will be inerted into the same page.
 */
void UTHeapMultiThread::FillGivenHeapNumForTdAlloc(std::vector<ItemPointerData> &ctids, int num)
{
    ctids.clear();
    int tupleLen = 1;
    while (ctids.size() < num) {
        HeapInsertFixedLengthTupleWithFixedTabSmgr(tupleLen);
        ItemPointerData ctid = m_ctids.front();
        m_ctids.pop();
        ctids.emplace_back(ctid);
    }
}

void UTHeapMultiThread::HeapDeleteTupleToHoldTd(ItemPointerData ctid, SyncPointGroup *syncPointGroup,
    int syncPointId, Xid *xid)
{
    BuildThreadLocalVar();
    RetStatus ret;
    Transaction *txn = thrd->GetActiveTransaction();

    txn->Start();
    txn->SetSnapshotCsn();

    HeapDeleteContext deleteContext;
    deleteContext.ctid = ctid;
    deleteContext.needReturnTup = false;
    deleteContext.snapshot = *txn->GetSnapshotData();
    deleteContext.cid = txn->GetCurCid();

    std::unique_ptr<StorageRelationData> heapRel(new StorageRelationData());
    heapRel->tableSmgr = ThdUtTableHandler->GetHeapTabSmgr();
    heapRel->m_pdbId = g_defaultPdbId;
    {
        HeapDeleteHandler heapDelete(g_storageInstance, thrd, heapRel.get());
        ret = heapDelete.Delete(&deleteContext);
    }
    EXPECT_TRUE(STORAGE_FUNC_SUCC(ret));

    /* get xid */
    *xid = txn->GetCurrentXid();

    /* Wait until all threads hold xact lock. */
    syncPointGroup->SyncPoint(syncPointId);

    /* Wait until test thread failed lock. */
    syncPointGroup->SyncPoint(syncPointId);

    txn->Commit();
}

void UTHeapMultiThread::HeapDeleteTupleToHoldTdTask(UTHeapMultiThread *ptr, ItemPointerData ctid,
    SyncPointGroup *syncPointGroup, int syncPointId, Xid *xid)
{
    ptr->HeapDeleteTupleToHoldTd(ctid, syncPointGroup, syncPointId, xid);
}

void UTHeapMultiThread::HeapUpdateTupleForCancel(ItemPointerData ctid, int tupleLen, SyncPointGroup *syncPointGroup,
    int syncPointId, ThreadContext **testThrd, uint64 *releaseTime)
{
    BuildThreadLocalVar();
    RetStatus ret;
    *testThrd = thrd;
    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetSnapshotCsn();

    /* Wait until all threads hold xact lock. */
    syncPointGroup->SyncPoint(syncPointId);

    std::string data = GenerateRandomString(tupleLen);
    HeapTuple *newTuple = UTTableHandler::GenerateSpecificHeapTuple(data);
    HeapUpdateContext updateContext;
    updateContext.oldCtid = ctid;
    updateContext.newTuple = newTuple;
    updateContext.needReturnOldTup = true;
    updateContext.snapshot = *txn->GetSnapshotData();
    updateContext.cid = txn->GetCurCid();

    std::unique_ptr<StorageRelationData> heapRel(new StorageRelationData());
    heapRel->tableSmgr = ThdUtTableHandler->GetHeapTabSmgr();
    heapRel->m_pdbId = g_defaultPdbId;
    {
        HeapUpdateHandler heapUpdate(g_storageInstance, thrd, heapRel.get());
        ret = heapUpdate.Update(&updateContext);
    }
    *releaseTime = GetSystemTimeInMicrosecond();
    EXPECT_TRUE(STORAGE_FUNC_FAIL(ret));
    EXPECT_EQ(StorageGetErrorCode(), LOCK_ERROR_WAIT_CANCELED);

    /* Test thread failed lock now. */
    syncPointGroup->SyncPoint(syncPointId);

    txn->Abort();
}

void UTHeapMultiThread::HeapUpdateTupleForCancelTask(UTHeapMultiThread *ptr, ItemPointerData ctid, int tupleLen,
    SyncPointGroup *syncPointGroup, int syncPointId, ThreadContext **testThrd, uint64 *releaseTime)
{
    ptr->HeapUpdateTupleForCancel(ctid, tupleLen, syncPointGroup, syncPointId, testThrd, releaseTime);
}