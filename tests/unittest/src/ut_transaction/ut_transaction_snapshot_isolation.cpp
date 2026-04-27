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
#include "transaction/dstore_transaction_interface.h"
#include "ut_transaction/ut_transaction_snapshot_isolation.h"

using namespace DSTORE;

void UTTransactionSnapshotIsolation::InitThread()
{
    SetThreadLNeedWait();
    SetThreadRNeedWait();
}

void UTTransactionSnapshotIsolation::SetThreadLNeedWait()
{
    m_threadLNeedWait = true;
}

void UTTransactionSnapshotIsolation::SetThreadLWillRun()
{
    m_threadLNeedWait = false;
    m_condition.notify_all();
}

void UTTransactionSnapshotIsolation::ThreadLWaitIfNeed(std::unique_lock<std::mutex> &lck)
{
    while (m_threadLNeedWait) {
        m_condition.wait(lck);
    }
}

void UTTransactionSnapshotIsolation::SetThreadRNeedWait()
{
    m_threadRNeedWait = true;
}

void UTTransactionSnapshotIsolation::SetThreadRWillRun()
{
    m_threadRNeedWait = false;
    m_condition.notify_all();
}

void UTTransactionSnapshotIsolation::ThreadRWaitIfNeed(std::unique_lock<std::mutex> &lck)
{
    while (m_threadRNeedWait) {
        m_condition.wait(lck);
    }
}

void UTTransactionSnapshotIsolation::XactThreadFunctionTest01_ThreadL()
{
    BuildThreadLocalVar();
    std::unique_lock<std::mutex> lck(m_mutex);
    SetThreadLWillRun();
    SetThreadRNeedWait();

    Transaction *txn = thrd->GetActiveTransaction();

    /* BEGIN */
    TransactionInterface::StartTrxCommand();
    TransactionInterface::BeginTrxBlock();
    TransactionInterface::CommitTrxCommand();

    /* set transaction isolation level read committed */
    TransactionInterface::StartTrxCommand();
    TransactionInterface::SetSnapShot();
    TransactionInterface::SetIsolationLevel(static_cast<int>(TrxIsolationType::XACT_READ_COMMITTED));
    TransactionInterface::CommitTrxCommand();
    EXPECT_EQ(TransactionInterface::GetIsolationLevel(), static_cast<int>(TrxIsolationType::XACT_READ_COMMITTED));

    /* only get snapshot, do nothing */
    TransactionInterface::StartTrxCommand();
    TransactionInterface::SetSnapShot();
    CommitSeqNo firstSnapshotCsn = txn->GetSnapshotCsn();
    SetThreadLNeedWait();
    SetThreadRWillRun();
    ThreadLWaitIfNeed(lck);
    TransactionInterface::CommitTrxCommand();

    /* get new snapshot to scan, data is visible */
    TransactionInterface::StartTrxCommand();
    std::string data = "data";
    HeapTuple *heapTuple = UTTableHandler::GenerateSpecificHeapTuple(data);
    HeapTuple *findTuple = ThdUtTableHandler->FetchHeapTuple(&m_ctid, INVALID_SNAPSHOT, true);
    EXPECT_TRUE(findTuple->GetDiskTupleSize() == heapTuple->GetDiskTupleSize());
    int32 dataSize = heapTuple->GetDiskTupleSize() - HEAP_DISK_TUP_HEADER_SIZE;
    EXPECT_EQ(memcmp((void*)findTuple->GetDiskTuple()->GetData(), heapTuple->GetDiskTuple()->GetData(), dataSize), 0);
    CommitSeqNo secondSnapshotCsn = txn->GetSnapshotCsn();
    EXPECT_EQ(secondSnapshotCsn, firstSnapshotCsn + 1);
    TransactionInterface::CommitTrxCommand();

    /* COMMIT */
    TransactionInterface::StartTrxCommand();
    TransactionInterface::EndTrxBlock();
    TransactionInterface::CommitTrxCommand();
}

void UTTransactionSnapshotIsolation::XactThreadFunctionTest01_ThreadR()
{
    BuildThreadLocalVar();
    std::unique_lock<std::mutex> lck(m_mutex);
    ThreadRWaitIfNeed(lck);

    Transaction *txn = thrd->GetActiveTransaction();
    EXPECT_EQ(TransactionInterface::GetCurrentXid(), -1);

    TransactionInterface::StartTrxCommand();
    TransactionInterface::SetSnapShot();

    std::string data = "data";
    HeapTuple *heapTuple = UTTableHandler::GenerateSpecificHeapTuple(data);
    m_ctid = ThdUtTableHandler->InsertHeapTupAndCheckResult(heapTuple, true, txn->GetSnapshotData());

    EXPECT_NE(TransactionInterface::GetCurrentXid(), -1);
    TransactionInterface::CommitTrxCommand();

    SetThreadLWillRun();
}

void UTTransactionSnapshotIsolation::XactThreadFunctionTest02_ThreadL()
{
    BuildThreadLocalVar();
    std::unique_lock<std::mutex> lck(m_mutex);
    SetThreadLWillRun();
    SetThreadRNeedWait();

    Transaction *txn = thrd->GetActiveTransaction();

    /* BEGIN */
    TransactionInterface::StartTrxCommand();
    TransactionInterface::BeginTrxBlock();
    TransactionInterface::CommitTrxCommand();

    /* set transaction isolation level  repeatable read */
    TransactionInterface::StartTrxCommand();
    TransactionInterface::SetSnapShot();
    TransactionInterface::SetIsolationLevel(static_cast<int>(TrxIsolationType::XACT_TRANSACTION_SNAPSHOT));
    TransactionInterface::CommitTrxCommand();
    EXPECT_EQ(TransactionInterface::GetIsolationLevel(), static_cast<int>(TrxIsolationType::XACT_TRANSACTION_SNAPSHOT));

    /* only get snapshot, do nothing */
    TransactionInterface::StartTrxCommand();
    TransactionInterface::SetSnapShot();
    CommitSeqNo firstSnapshotCsn = txn->GetSnapshotCsn();
    SetThreadLNeedWait();
    SetThreadRWillRun();
    ThreadLWaitIfNeed(lck);
    TransactionInterface::CommitTrxCommand();

    /* use SNAPSHOT_NOW to scan, data is visible */
    TransactionInterface::StartTrxCommand();
    SnapshotData snapshot = {DSTORE::SnapshotType::SNAPSHOT_NOW, MAX_COMMITSEQNO, INVALID_CID};
    HeapTuple *findTuple = ThdUtTableHandler->FetchHeapTuple(&m_ctid, &snapshot, true);
    EXPECT_FALSE(findTuple == nullptr);

    /* get same snapshot to scan, data is invisible */
    TransactionInterface::SetSnapShot();
    findTuple = ThdUtTableHandler->FetchHeapTuple(&m_ctid, INVALID_SNAPSHOT, true);
    EXPECT_TRUE(findTuple == nullptr);
    CommitSeqNo thirdSnapshotCsn = txn->GetSnapshotCsn();
    EXPECT_EQ(thirdSnapshotCsn, firstSnapshotCsn);
    TransactionInterface::CommitTrxCommand();

    /* COMMIT */
    TransactionInterface::StartTrxCommand();
    TransactionInterface::EndTrxBlock();
    TransactionInterface::CommitTrxCommand();
}

void UTTransactionSnapshotIsolation::XactThreadFunctionTest02_ThreadR()
{
    BuildThreadLocalVar();
    std::unique_lock<std::mutex> lck(m_mutex);
    ThreadRWaitIfNeed(lck);

    Transaction *txn = thrd->GetActiveTransaction();

    TransactionInterface::StartTrxCommand();
    TransactionInterface::SetSnapShot();

    std::string data = "data";
    HeapTuple *heapTuple = UTTableHandler::GenerateSpecificHeapTuple(data);
    m_ctid = ThdUtTableHandler->InsertHeapTupAndCheckResult(heapTuple, true, txn->GetSnapshotData());

    TransactionInterface::CommitTrxCommand();

    SetThreadLWillRun();
}

TEST_F(UTTransactionSnapshotIsolation, ReadCommittedIsolationTest_level0)
{
    /*
     *   Isolation: XACT_READ_COMMITTED
     *
     *   XactL: |------------------------------->|
     *   XactR:       |------------>|
     *
     *   The XactL will see the XactR's effect in the later of XactL.
     */
    InitThread();
    m_pool.AddTask(XactThreadTask01_ThreadL, this);
    m_pool.AddTask(XactThreadTask01_ThreadR, this);
    m_pool.WaitAllTaskFinish();
}

TEST_F(UTTransactionSnapshotIsolation, TransactionSnapshotIsolationTest_level0)
{
    /*
     *   Isolation: XACT_TRANSACTION_SNAPSHOT
     *
     *   XactL: |------------------------------->|
     *   XactR:       |------------>|
     *
     *   The XactL will not see the XactR's effect in the later of XactL.
     */

    InitThread();
    m_pool.AddTask(XactThreadTask02_ThreadL, this);
    m_pool.AddTask(XactThreadTask02_ThreadR, this);
    m_pool.WaitAllTaskFinish();
}
