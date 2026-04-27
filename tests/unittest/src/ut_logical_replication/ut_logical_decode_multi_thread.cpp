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
#include "ut_logical_replication/ut_logical_decode.h"
#include "ut_logical_replication/ut_logical_decode_multi_thread.h"
#include "ut_tablehandler/ut_table_handler.h"
#include "wal/dstore_wal.h"

using namespace DSTORE;

void LOGICALDECODEMULTI::SetUp()
{
    LOGICALDECODE::SetUp();
    int threadNum = m_config.ReadInteger("UTHeapMultiThread-ThreadNum");
    if (threadNum < 0) {
        threadNum = 64;
    }
    m_pool.Start(threadNum);
    m_heapSegment = m_utTableHandler->m_heapSegmentPageId;
    m_lobSegment = m_utTableHandler->m_lobSegmentPageId;
}

void LOGICALDECODEMULTI::TearDown()
{
    m_pool.Shutdown();
    LOGICALDECODE::TearDown();
}

void LOGICALDECODEMULTI::BuildThreadLocalTableHandler()
{
    if (unlikely(ThdUtTableHandler == nullptr)) {
        ThdUtTableHandler = UTTableHandler::GetTableHandler(g_defaultPdbId,
            thrd->m_memoryMgr->GetGroupContext(MemoryGroupType::MEMORY_CONTEXT_LONGLIVE),
            m_heapSegment,
            m_lobSegment,
            INVALID_PAGE_ID);
        ThdUtTableHandler->GetHeapTabSmgr()->SetTableOid(LOGICAL_DECODE_TEST_TABLE_OID);
        ThdUtTableHandler->GetHeapTabSmgr()->SetTupleDesc(m_fakeDesc);
    }
    ASSERT_NE(ThdUtTableHandler, nullptr);
    ASSERT_NE(ThdUtTableHandler->GetHeapTabSmgr(), nullptr);
    ASSERT_NE(ThdUtTableHandler->GetLobTabSmgr(), nullptr);
    ASSERT_TRUE(ThdUtTableHandler->m_heapSegmentPageId.IsValid());
    ASSERT_TRUE(ThdUtTableHandler->m_lobSegmentPageId.IsValid());
}

void LOGICALDECODEMULTI::HeapInsertTaskWithWritingLogicalWal()
{
    BuildThreadLocalTableHandler();
    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetSnapshotCsn();
    HeapTuple *heapTuple = ThdUtTableHandler->GenerateRandomHeapTuple(m_fakeDesc);
    ItemPointerData ctid;
    ctid = ThdUtTableHandler->InsertHeapTupAndCheckResult(heapTuple, true, txn->GetSnapshotData());
    Xid insertXid = txn->GetCurrentXid();
    txn->Commit();
    TransactionMgr *transactionMgr= g_storageInstance->GetPdb(g_defaultPdbId)->GetTransactionMgr();
    CommitSeqNo commitCsn = transactionMgr->GetCsnFromXid(insertXid);
    ASSERT_NE(commitCsn, INVALID_CSN);
    m_mutex.lock();
    m_csnXidMap[commitCsn] = insertXid;
    m_ctids.push(ctid);
    m_commitCounter++;
    m_mutex.unlock();
    thrd->m_memoryMgr->reset_group_context(DSTORE::MEMORY_CONTEXT_QUERY);
}

void LOGICALDECODEMULTI::HeapUpdateTaskWithWritingLogicalWal()
{
    BuildThreadLocalTableHandler();
    /* Step 1: Get a ctid. */
    m_mutex.lock();
    if (m_ctids.empty()) {
        m_mutex.unlock();
        return;
    }
    ItemPointerData oldCtid = m_ctids.front();
    m_ctids.pop();
    m_mutex.unlock();

    HeapTuple *newTuple = ThdUtTableHandler->GenerateRandomHeapTuple(m_fakeDesc);
    CommitSeqNo commitCsn = INVALID_CSN;
    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetSnapshotCsn();
    ItemPointerData newCtid = UpdateTuple(&oldCtid, newTuple, INVALID_SNAPSHOT, true, ThdUtTableHandler);
    if (newCtid == INVALID_ITEM_POINTER) {
        txn->Abort();   /* abort */
        m_abortCounter++;
        return;
    }
    Xid updateXid= txn->GetCurrentXid();
    txn->Commit();
    TransactionMgr *transactionMgr= g_storageInstance->GetPdb(g_defaultPdbId)->GetTransactionMgr();
    commitCsn = transactionMgr->GetCsnFromXid(updateXid);

    if (newCtid != INVALID_ITEM_POINTER) {
        ASSERT_NE(commitCsn, INVALID_CSN);
        m_mutex.lock();
        m_csnXidMap[commitCsn] = updateXid;
        m_ctids.push(newCtid);
        m_commitCounter++;
        m_mutex.unlock();
    }
    thrd->m_memoryMgr->reset_group_context(DSTORE::MEMORY_CONTEXT_QUERY);
}

void LOGICALDECODEMULTI::HeapUpdateTaskConflictWithWritingLogicalWal()
{
    BuildThreadLocalTableHandler();
    /* Step 1: Get a ctid, but keep it in map so that all transactions update the same tuple. */
    m_mutex.lock();
    if (m_ctids.empty()) {
        m_mutex.unlock();
        return;
    }
    ItemPointerData oldCtid = m_ctids.front();
    m_mutex.unlock();

    HeapTuple *newTuple = ThdUtTableHandler->GenerateRandomHeapTuple(m_fakeDesc);
    CommitSeqNo commitCsn = INVALID_CSN;
    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetSnapshotCsn();
    ItemPointerData newCtid = UpdateTuple(&oldCtid, newTuple, INVALID_SNAPSHOT, true, ThdUtTableHandler);
    if (newCtid == INVALID_ITEM_POINTER) {
        txn->Abort();   /* abort */
        m_abortCounter++;
        return;
    }
    Xid updateXid= txn->GetCurrentXid();
    txn->Commit();
    TransactionMgr *transactionMgr= g_storageInstance->GetPdb(g_defaultPdbId)->GetTransactionMgr();
    commitCsn = transactionMgr->GetCsnFromXid(updateXid);

    if (newCtid != INVALID_ITEM_POINTER) {
        ASSERT_NE(commitCsn, INVALID_CSN);
        m_mutex.lock();
        m_csnXidMap[commitCsn] = updateXid;
        m_ctids.push(newCtid);
        m_commitCounter++;
        m_mutex.unlock();
    }
    thrd->m_memoryMgr->reset_group_context(DSTORE::MEMORY_CONTEXT_QUERY);
}

void LOGICALDECODEMULTI::HeapDeleteTaskWithWritingLogicalWal()
{
    BuildThreadLocalTableHandler();
    /* Step 1: Get a ctid. */
    m_mutex.lock();
    if (m_ctids.empty()) {
        m_mutex.unlock();
        return;
    }
    ItemPointerData oldCtid = m_ctids.front();
    m_ctids.pop();
    m_mutex.unlock();

    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetSnapshotCsn();
    RetStatus rt = DeleteTuple(&oldCtid, INVALID_SNAPSHOT, true, ThdUtTableHandler);
    Xid deleteXid = txn->GetCurrentXid();
    txn->Commit();
    TransactionMgr *transactionMgr= g_storageInstance->GetPdb(g_defaultPdbId)->GetTransactionMgr();
    CommitSeqNo commitCsn = transactionMgr->GetCsnFromXid(deleteXid);

    if (rt == DSTORE_SUCC) {
        ASSERT_NE(commitCsn, INVALID_CSN);
        m_mutex.lock();
        m_csnXidMap[commitCsn] = deleteXid;
        m_commitCounter++;
        m_mutex.unlock();
    }
    thrd->m_memoryMgr->reset_group_context(DSTORE::MEMORY_CONTEXT_QUERY);
}

void LOGICALDECODEMULTI::HeapDeleteTaskConflictWithWritingLogicalWal()
{
    BuildThreadLocalTableHandler();
    /* Step 1: Get a ctid. */
    m_mutex.lock();
    if (m_ctids.empty()) {
        m_mutex.unlock();
        return;
    }
    ItemPointerData oldCtid = m_ctids.front();
    m_mutex.unlock();

    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetSnapshotCsn();
    RetStatus rt = DeleteTuple(&oldCtid, INVALID_SNAPSHOT, true, ThdUtTableHandler);
    Xid deleteXid = txn->GetCurrentXid();

    if (rt == DSTORE_SUCC) {
        txn->Commit();
        TransactionMgr *transactionMgr= g_storageInstance->GetPdb(g_defaultPdbId)->GetTransactionMgr();
        CommitSeqNo commitCsn = transactionMgr->GetCsnFromXid(deleteXid);
        ASSERT_NE(commitCsn, INVALID_CSN);
        m_mutex.lock();
        m_csnXidMap[commitCsn] = deleteXid;
        m_commitCounter++;
        m_mutex.unlock();
    } else {
        txn->Abort();
        m_abortCounter++;
    }
    thrd->m_memoryMgr->reset_group_context(DSTORE::MEMORY_CONTEXT_QUERY);
}


TEST_F(LOGICALDECODEMULTI, DISABLED_DecodeHeapMultiThreadInsert)
{
    uint64 beforePlsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), beforePlsn);
    m_logicalSlot->AdvanceRestartPlsn(beforePlsn);
    
    CommitSeqNo curCsn;
    g_storageInstance->GetCsnMgr()->GetNextCsn(curCsn, true);
    m_logicalSlot->AdvanceConfirmCsn(curCsn);

    const int insertCnt = 200;
    for (int i = 0; i < insertCnt; i++) {
        m_pool.AddTask(HeapInsertTask, this);
    }
    m_pool.WaitAllTaskFinish();

    uint64 afterPlsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), afterPlsn);
    // std::cout << "afterPlsn:" << afterPlsn << std::endl;

    DecodeOptions *opts = CreateDecodeOptions(thrd->GetNextCsn() - 1, INVALID_NCHANGES, 1);
    LogicalDecodeHandler *decodeContext = CreateDecodeHandler(opts);
    decodeContext->StartUp();
    int logCnt = 0;
    TrxLogicalLog *trxLog = nullptr;
    for (auto it = m_csnXidMap.begin(); it != m_csnXidMap.end(); it++) {
        trxLog = decodeContext->GetNextTrxLogicalLog();
        ASSERT_NE(trxLog, nullptr);
        ASSERT_EQ(trxLog->commitCsn, it->first); /* decoded ones should be sorted by commitCsn */
        ASSERT_EQ(trxLog->xid, it->second.m_placeHolder);
        logCnt++;
    }
    ASSERT_EQ(m_commitCounter, insertCnt);
    ASSERT_EQ(insertCnt, logCnt);
    ASSERT_EQ(decodeContext->GetNextTrxLogicalLog(), nullptr);
    decodeContext->Stop();
    decodeContext->Destroy();
    delete decodeContext;
    decodeContext = nullptr;
    DstorePfree(opts);
}

TEST_F(LOGICALDECODEMULTI, DISABLED_DecodeHeapMultiThreadUpdate)
{
    uint64 beforePlsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), beforePlsn);
    m_logicalSlot->AdvanceRestartPlsn(beforePlsn);
    
    CommitSeqNo curCsn;
    g_storageInstance->GetCsnMgr()->GetNextCsn(curCsn, true);
    m_logicalSlot->AdvanceConfirmCsn(curCsn);

    /* step1. concurrently insert */
    const int insertCnt = 100;
    for (int i = 0; i < insertCnt; i++) {
        m_pool.AddTask(HeapInsertTask, this);
    }
    m_pool.WaitAllTaskFinish();

    /* step2. concurrently update */
    const int updateCnt = 100;
    for (int i = 0; i < updateCnt; i++) {
        m_pool.AddTask(HeapUpdateTask, this);
    }
    m_pool.WaitAllTaskFinish();

    uint64 afterPlsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), afterPlsn);

    DecodeOptions *opts = CreateDecodeOptions(thrd->GetNextCsn() - 1, INVALID_NCHANGES, 1);
    LogicalDecodeHandler *decodeContext = CreateDecodeHandler(opts);
    decodeContext->StartUp();
    int logCnt = 0;
    TrxLogicalLog *trxLog = nullptr;
    for (auto it = m_csnXidMap.begin(); it != m_csnXidMap.end(); it++) {
        trxLog = decodeContext->GetNextTrxLogicalLog();
        ASSERT_NE(trxLog, nullptr);
        ASSERT_EQ(trxLog->commitCsn, it->first); /* decoded ones should be sorted by commitCsn */
        ASSERT_EQ(trxLog->xid, it->second.m_placeHolder);
        logCnt++;
    }
    ASSERT_EQ(m_commitCounter, insertCnt + updateCnt);
    ASSERT_EQ(insertCnt + updateCnt, logCnt);
    ASSERT_EQ(decodeContext->GetNextTrxLogicalLog(), nullptr);
    decodeContext->Stop();
    decodeContext->Destroy();
    delete decodeContext;
    decodeContext = nullptr;
    DstorePfree(opts);
}

TEST_F(LOGICALDECODEMULTI, DISABLED_DecodeHeapMultiThreadUpdateConflict)
{
    uint64 beforePlsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), beforePlsn);
    m_logicalSlot->AdvanceRestartPlsn(beforePlsn);
    
    CommitSeqNo curCsn;
    g_storageInstance->GetCsnMgr()->GetNextCsn(curCsn, true);
    m_logicalSlot->AdvanceConfirmCsn(curCsn);

    /* step1. generate 1 tuple */
    const int insertCnt = 1;
    for (int i = 0; i < insertCnt; i++) {
        m_pool.AddTask(HeapInsertTask, this);
    }
    m_pool.WaitAllTaskFinish();

    /* step2. concurrently update of the same tuple */
    const int updateCnt = 100;
    for (int i = 0; i < updateCnt; i++) {
        m_pool.AddTask(HeapUpdateTaskConflict, this);
    }
    m_pool.WaitAllTaskFinish();

    uint64 afterPlsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), afterPlsn);
    // std::cout << "afterPlsn:" << afterPlsn << std::endl;

    DecodeOptions *opts = CreateDecodeOptions(thrd->GetNextCsn() - 1, INVALID_NCHANGES, 1);
    LogicalDecodeHandler *decodeContext = CreateDecodeHandler(opts);
    decodeContext->StartUp();
    int logCnt = 0;
    TrxLogicalLog *trxLog = nullptr;
    for (auto it = m_csnXidMap.begin(); it != m_csnXidMap.end(); it++) {
        trxLog = decodeContext->GetNextTrxLogicalLog();
        ASSERT_NE(trxLog, nullptr);
        ASSERT_EQ(trxLog->commitCsn, it->first); /* decoded ones should be sorted by commitCsn */
        ASSERT_EQ(trxLog->xid, it->second.m_placeHolder);
        logCnt++;
    }
    ASSERT_EQ(m_commitCounter + m_abortCounter, insertCnt + updateCnt);
    ASSERT_EQ(m_commitCounter, logCnt);
    ASSERT_EQ(decodeContext->GetNextTrxLogicalLog(), nullptr);
    decodeContext->Stop();
    decodeContext->Destroy();
    delete decodeContext;
    decodeContext = nullptr;
    DstorePfree(opts);
}

TEST_F(LOGICALDECODEMULTI, DISABLED_DecodeHeapMultiThreadDelete)
{
    uint64 beforePlsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), beforePlsn);
    m_logicalSlot->AdvanceRestartPlsn(beforePlsn);
    
    CommitSeqNo curCsn;
    g_storageInstance->GetCsnMgr()->GetNextCsn(curCsn, true);
    m_logicalSlot->AdvanceConfirmCsn(curCsn);

    /* step1. generate 1 tuple */
    const int insertCnt = 100;
    for (int i = 0; i < insertCnt; i++) {
        m_pool.AddTask(HeapInsertTask, this);
    }
    m_pool.WaitAllTaskFinish();

    /* step2. concurrently update of the same tuple */
    const int deleteCnt = 100;
    for (int i = 0; i < deleteCnt; i++) {
        m_pool.AddTask(HeapDeleteTask, this);
    }
    m_pool.WaitAllTaskFinish();

    uint64 afterPlsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), afterPlsn);
    // std::cout << "afterPlsn:" << afterPlsn << std::endl;

    DecodeOptions *opts = CreateDecodeOptions(thrd->GetNextCsn() - 1, INVALID_NCHANGES, 1);
    LogicalDecodeHandler *decodeContext = CreateDecodeHandler(opts);
    decodeContext->StartUp();
    int logCnt = 0;
    TrxLogicalLog *trxLog = nullptr;
    for (auto it = m_csnXidMap.begin(); it != m_csnXidMap.end(); it++) {
        trxLog = decodeContext->GetNextTrxLogicalLog();
        ASSERT_NE(trxLog, nullptr);
        ASSERT_EQ(trxLog->commitCsn, it->first); /* decoded ones should be sorted by commitCsn */
        ASSERT_EQ(trxLog->xid, it->second.m_placeHolder);
        logCnt++;
    }
    ASSERT_EQ(m_commitCounter, insertCnt + deleteCnt);
    ASSERT_EQ(m_commitCounter, logCnt);
    ASSERT_TRUE(m_ctids.empty());
    ASSERT_EQ(decodeContext->GetNextTrxLogicalLog(), nullptr);
    decodeContext->Stop();
    decodeContext->Destroy();
    delete decodeContext;
    decodeContext = nullptr;
    DstorePfree(opts);
}

TEST_F(LOGICALDECODEMULTI, DISABLED_DecodeHeapMultiThreadDeleteConflict)
{
    uint64 beforePlsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), beforePlsn);
    m_logicalSlot->AdvanceRestartPlsn(beforePlsn);
    
    CommitSeqNo curCsn;
    g_storageInstance->GetCsnMgr()->GetNextCsn(curCsn, true);
    m_logicalSlot->AdvanceConfirmCsn(curCsn);

    /* step1. generate 1 tuple */
    const int insertCnt = 1;
    for (int i = 0; i < insertCnt; i++) {
        m_pool.AddTask(HeapInsertTask, this);
    }
    m_pool.WaitAllTaskFinish();

    /* step2. concurrently update of the same tuple */
    const int deleteCnt = 100;
    for (int i = 0; i < deleteCnt; i++) {
        m_pool.AddTask(HeapDeleteTaskConflict, this);
    }
    m_pool.WaitAllTaskFinish();

    uint64 afterPlsn = m_walStream->GetMaxAppendedPlsn();
    m_walWriter->WaitTargetPlsnPersist(m_walWriter->GetWalId(), afterPlsn);
    // std::cout << "afterPlsn:" << afterPlsn << std::endl;

    DecodeOptions *opts = CreateDecodeOptions(thrd->GetNextCsn() - 1, INVALID_NCHANGES, 1);
    LogicalDecodeHandler *decodeContext = CreateDecodeHandler(opts);
    decodeContext->StartUp();
    int logCnt = 0;
    TrxLogicalLog *trxLog = nullptr;
    for (auto it = m_csnXidMap.begin(); it != m_csnXidMap.end(); it++) {
        trxLog = decodeContext->GetNextTrxLogicalLog();
        ASSERT_NE(trxLog, nullptr);
        ASSERT_EQ(trxLog->commitCsn, it->first); /* decoded ones should be sorted by commitCsn */
        ASSERT_EQ(trxLog->xid, it->second.m_placeHolder);
        logCnt++;
    }
    ASSERT_EQ(m_commitCounter + m_abortCounter, insertCnt + deleteCnt);
    ASSERT_EQ(m_commitCounter, logCnt);
    ASSERT_EQ(decodeContext->GetNextTrxLogicalLog(), nullptr);
    decodeContext->Stop();
    decodeContext->Destroy();
    delete decodeContext;
    decodeContext = nullptr;
    DstorePfree(opts);
}
