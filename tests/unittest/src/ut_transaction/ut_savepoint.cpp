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
#include "ut_transaction/ut_savepoint.h"
#include "heap/dstore_heap_scan.h"
#include "transaction/dstore_transaction.h"
#include "errorcode/dstore_transaction_error_code.h"
#include "index/dstore_index_handler.h"
#include "lock/dstore_lock_mgr.h"
#include "lock/dstore_table_lock_mgr.h"
#include "lock/dstore_lock_thrd_local.h"
#include <thread>
#include <vector>
#include <algorithm>

using namespace DSTORE;

void SavepointTest::InitData()
{
    m_utTableHandler = UTTableHandler::CreateTableHandler(g_defaultPdbId, m_ut_memory_context);

    int rowNum = 10;
    m_utTableHandler->FillTableWithRandomData(rowNum);

    /* Build a index on table. */
    int indexCols[] = {1, 2};
    bool isUnique = false;
    int numAttrs = sizeof(indexCols) / sizeof(int);
    m_utTableHandler->CreateIndex(indexCols, numAttrs, isUnique);
}

void SavepointTest::DestroyData()
{
    UTTableHandler::Destroy(m_utTableHandler);
    m_utTableHandler = nullptr;
}

void SavepointTest::CopyTuplesToMap(std::map<uint64, std::string> *map)
{
    HeapScanHandler scan(g_storageInstance, thrd, m_utTableHandler->GetTableRel());
    HeapTuple *heapTuple = nullptr;
    scan.Begin(thrd->GetActiveTransaction()->GetSnapshotData());
    while ((heapTuple = scan.SeqScan()) != nullptr) {
        uint64 ctid = heapTuple->GetCtid()->m_placeHolder;
        uint32 dataSize = heapTuple->GetDiskTuple()->GetTupleSize() - heapTuple->GetDiskTuple()->GetHeaderSize();
        std::string str(heapTuple->GetDiskTuple()->GetData(), dataSize);
        map->insert(std::pair<uint64, std::string>(ctid, str));
    }
    scan.End();
    CheckTuplesAreTheSameWith(map);
}

void SavepointTest::CheckTuplesAreTheSameWith(std::map<uint64, std::string> *compare)
{
    int tupleNum = compare->size();

    /* Check with heap scan. */
    int heapTupleNum = 0;
    HeapTuple *heapTuple = nullptr;
    HeapScanHandler heapScan(g_storageInstance, thrd, m_utTableHandler->GetTableRel());
    heapScan.Begin(thrd->GetActiveTransaction()->GetSnapshot());
    while ((heapTuple = heapScan.SeqScan()) != nullptr) {
        uint64 ctid = heapTuple->GetCtid()->m_placeHolder;
        uint32 dataSize = heapTuple->GetDiskTuple()->GetTupleSize() - heapTuple->GetDiskTuple()->GetHeaderSize();
        std::string str(heapTuple->GetDiskTuple()->GetData(), dataSize);
        std::map<uint64, std::string>::iterator iter;
        iter = compare->find(ctid);
        EXPECT_TRUE(iter != compare->end());
        EXPECT_TRUE(str.compare(iter->second) == 0);
        heapTupleNum++;
    }
    heapScan.End();
    EXPECT_EQ(tupleNum, heapTupleNum);

    /* Check again with index scan. */
    int indexTupleNum = 0;
    IndexScanHandler indexScan;
    indexScan.InitIndexScanHandler(m_utTableHandler->GetIndexRel(), m_utTableHandler->GetIndexInfo(), 0, 0);
    indexScan.InitSnapshot(thrd->GetActiveTransaction()->GetSnapshot());
    indexScan.BeginScan(); /* Scan all tuples. */

    bool found;
    IndexScanDesc indexScanDesc = indexScan.GetScanDesc();
    while (STORAGE_FUNC_SUCC(indexScan.GetNextTuple(ScanDirection::FORWARD_SCAN_DIRECTION, &found)) && found) {
        heapTuple = heapScan.FetchTuple(indexScanDesc->heapCtid);
        EXPECT_NE(heapTuple, nullptr);
        uint64 ctid = heapTuple->GetCtid()->m_placeHolder;
        EXPECT_EQ(ctid, indexScanDesc->heapCtid.m_placeHolder);
        uint32 dataSize = heapTuple->GetDiskTuple()->GetTupleSize() - heapTuple->GetDiskTuple()->GetHeaderSize();
        std::string str(heapTuple->GetDiskTuple()->GetData(), dataSize);
        std::map<uint64, std::string>::iterator iter;
        iter = compare->find(ctid);
        EXPECT_TRUE(iter != compare->end());
        EXPECT_TRUE(str.compare(iter->second) == 0);
        DstorePfreeExt(heapTuple);
        indexTupleNum++;
    }
    indexScan.EndScan();
    heapScan.EndFetch();
    EXPECT_EQ(tupleNum, indexTupleNum);
}

TEST_F(SavepointTest, RollbackInsertTupleTest_level0)
{
    RetStatus ret;
    Transaction *transaction = thrd->GetActiveTransaction();
    EXPECT_TRUE(transaction->IsSavepointListEmpty());
    std::map<uint64, std::string> savepoint1;

    /* Start transaction. */
    transaction->Start();
    transaction->SetSnapshotCsn();

    /* Insert one tuple. */
    IndexTuple *indexTuple = m_utTableHandler->InsertRandomIndexTuple(true);
    DstorePfreeExt(indexTuple);

    /* Create savepoint1, and save tuples for comparison. */
    ret = transaction->CreateSavepoint("savepoint1");
    EXPECT_TRUE(STORAGE_FUNC_SUCC(ret));
    CopyTuplesToMap(&savepoint1);

    /* Insert more tuples. */
    indexTuple = m_utTableHandler->InsertRandomIndexTuple(true);
    DstorePfreeExt(indexTuple);
    indexTuple = m_utTableHandler->InsertRandomIndexTuple(true);
    DstorePfreeExt(indexTuple);
    indexTuple = m_utTableHandler->InsertRandomIndexTuple(true);
    DstorePfreeExt(indexTuple);

    /* Rollback to savepoint1 and commit. */
    ret = transaction->RollbackToSavepoint("savepoint1");
    EXPECT_TRUE(STORAGE_FUNC_SUCC(ret));
    transaction->Commit();

    /* Current tuples should be the same with tuple map. */
    transaction->Start();
    transaction->SetSnapshotCsn();
    CheckTuplesAreTheSameWith(&savepoint1);
    transaction->Commit();
    EXPECT_TRUE(transaction->IsSavepointListEmpty());
}

TEST_F(SavepointTest, RollbackUpdateTupleTest_level0)
{
    RetStatus ret;
    Transaction *transaction = thrd->GetActiveTransaction();
    EXPECT_TRUE(transaction->IsSavepointListEmpty());
    std::map<uint64, std::string> savepoint1;

    /* Start transaction. */
    transaction->Start();
    transaction->SetSnapshotCsn();

    /* Insert one tuple. */
    IndexTuple *indexTuple1 = m_utTableHandler->InsertRandomIndexTuple(true);

    /* Create savepoint1, and save tuples for comparision. */
    ret = transaction->CreateSavepoint("savepoint1");
    EXPECT_TRUE(STORAGE_FUNC_SUCC(ret));
    CopyTuplesToMap(&savepoint1);

    /* Update this tuple. */
    IndexTuple *indexTuple2 = m_utTableHandler->UpdateWithIndexTuple(indexTuple1, true);
    DstorePfreeExt(indexTuple1);
    DstorePfreeExt(indexTuple2);

    /* Rollback to savepoint1 and commit. */
    ret = transaction->RollbackToSavepoint("savepoint1");
    EXPECT_TRUE(STORAGE_FUNC_SUCC(ret));
    transaction->Commit();

    /* Current tuples should be the same with tuple map. */
    transaction->Start();
    transaction->SetSnapshotCsn();
    CheckTuplesAreTheSameWith(&savepoint1);
    transaction->Commit();
    EXPECT_TRUE(transaction->IsSavepointListEmpty());
}

TEST_F(SavepointTest, RollbackDeleteTupleTest_level0)
{
    RetStatus ret;
    Transaction *transaction = thrd->GetActiveTransaction();
    EXPECT_TRUE(transaction->IsSavepointListEmpty());
    std::map<uint64, std::string> savepoint1;

    /* Start transaction. */
    transaction->Start();
    transaction->SetSnapshotCsn();

    /* Insert two tuple. */
    IndexTuple *indexTuple1 = m_utTableHandler->InsertRandomIndexTuple(true);
    IndexTuple *indexTuple2 = m_utTableHandler->InsertRandomIndexTuple(true);
    DstorePfreeExt(indexTuple2);

    /* Create savepoint1, and save tuples for comparision. */
    ret = transaction->CreateSavepoint("savepoint1");
    EXPECT_TRUE(STORAGE_FUNC_SUCC(ret));
    CopyTuplesToMap(&savepoint1);

    /* Delete one tuple. */
    m_utTableHandler->DeleteWithIndexTuple(indexTuple1, true);
    DstorePfreeExt(indexTuple1);

    /* Rollback to savepoint1 and commit. */
    ret = transaction->RollbackToSavepoint("savepoint1");
    EXPECT_TRUE(STORAGE_FUNC_SUCC(ret));
    transaction->Commit();

    /* Current tuples should be the same with tuple map. */
    transaction->Start();
    transaction->SetSnapshotCsn();
    CheckTuplesAreTheSameWith(&savepoint1);
    transaction->Commit();
    EXPECT_TRUE(transaction->IsSavepointListEmpty());
}

TEST_F(SavepointTest, RollbackMultipleSavepointsTest_level0)
{
    RetStatus ret;
    Transaction *transaction = thrd->GetActiveTransaction();
    EXPECT_TRUE(transaction->IsSavepointListEmpty());
    std::map<uint64, std::string> savepoint0;
    std::map<uint64, std::string> savepoint1;
    std::map<uint64, std::string> savepoint2;

    /* Start transaction. */
    transaction->Start();
    transaction->SetSnapshotCsn();

    /* Create and rollback to savepoint0 should succeed in read-only transaction. */
    ret = transaction->CreateSavepoint("savepoint0");
    EXPECT_TRUE(STORAGE_FUNC_SUCC(ret));
    CopyTuplesToMap(&savepoint0);
    ret = transaction->RollbackToSavepoint("savepoint0");
    EXPECT_TRUE(STORAGE_FUNC_SUCC(ret));
    CheckTuplesAreTheSameWith(&savepoint0);

    /* Insert one tuple. */
    IndexTuple *indexTuple1 = m_utTableHandler->InsertRandomIndexTuple(true);

    /* Create savepoint1. */
    ret = transaction->CreateSavepoint("savepoint1");
    EXPECT_TRUE(STORAGE_FUNC_SUCC(ret));
    CopyTuplesToMap(&savepoint1);

    /* Delete tuple. */
    m_utTableHandler->DeleteWithIndexTuple(indexTuple1, true);
    DstorePfreeExt(indexTuple1);

    /* Create savepoint2. */
    ret = transaction->CreateSavepoint("savepoint2");
    EXPECT_TRUE(STORAGE_FUNC_SUCC(ret));
    CopyTuplesToMap(&savepoint2);

    /* Insert another one tuple. */
    indexTuple1 = m_utTableHandler->InsertRandomIndexTuple(true);
    DstorePfreeExt(indexTuple1);

    /* Rollback to savepoint2, and check for result. */
    ret = transaction->RollbackToSavepoint("savepoint2");
    EXPECT_TRUE(STORAGE_FUNC_SUCC(ret));
    CheckTuplesAreTheSameWith(&savepoint2);

    /* Insert another one tuple. */
    IndexTuple *indexTuple2 = m_utTableHandler->InsertRandomIndexTuple(true);

    /* Create savepoint3. */
    ret = transaction->CreateSavepoint("savepoint3");
    EXPECT_TRUE(STORAGE_FUNC_SUCC(ret));

    /* Update tuple. */
    indexTuple1 = m_utTableHandler->UpdateWithIndexTuple(indexTuple2, true);
    DstorePfreeExt(indexTuple1);
    DstorePfreeExt(indexTuple2);

    /* Rollback to savepoint1, and check for result. */
    ret = transaction->RollbackToSavepoint("savepoint1");
    EXPECT_TRUE(STORAGE_FUNC_SUCC(ret));
    CheckTuplesAreTheSameWith(&savepoint1);

    /* Rollback to savepoint0. */
    ret = transaction->RollbackToSavepoint("savepoint0");
    EXPECT_TRUE(STORAGE_FUNC_SUCC(ret));

    /* Rollback to savepoint3, this should fail because it is not a valid savepoint any more. */
    ret = transaction->RollbackToSavepoint("savepoint3");
    EXPECT_TRUE(STORAGE_FUNC_FAIL(ret));
    EXPECT_TRUE(StorageGetErrorCode() == TRANSACTION_ERROR_SAVEPOINT_NOT_FOUND);
    transaction->Commit();

    /* Check again after commit. */
    transaction->Start();
    transaction->SetSnapshotCsn();
    CheckTuplesAreTheSameWith(&savepoint0);
    transaction->Commit();

    EXPECT_TRUE(transaction->IsSavepointListEmpty());
}

TEST_F(SavepointTest, DuplicateSavepointNameTest_level0)
{
    RetStatus ret;
    Transaction *transaction = thrd->GetActiveTransaction();
    EXPECT_TRUE(transaction->IsSavepointListEmpty());
    std::map<uint64, std::string> savepoint1;

    /* Start transaction. */
    transaction->Start();
    transaction->SetSnapshotCsn();

    /* Insert one tuple. */
    IndexTuple *indexTuple1 = m_utTableHandler->InsertRandomIndexTuple(true);

    /* Create savepoint0. */
    ret = transaction->CreateSavepoint("savepoint0");
    EXPECT_TRUE(STORAGE_FUNC_SUCC(ret));

    /* Insert another one tuple. */
    IndexTuple *indexTuple2 = m_utTableHandler->InsertRandomIndexTuple(true);
    DstorePfreeExt(indexTuple2);

    /* Create savepoint1. */
    ret = transaction->CreateSavepoint("savepoint1");
    EXPECT_TRUE(STORAGE_FUNC_SUCC(ret));

    /* Update tuple. */
    indexTuple2 = m_utTableHandler->UpdateWithIndexTuple(indexTuple1, true);
    DstorePfreeExt(indexTuple1);

    /* Create savepoint1 again, this should overwrite the old one. */
    ret = transaction->CreateSavepoint("savepoint1");
    EXPECT_TRUE(STORAGE_FUNC_SUCC(ret));
    CopyTuplesToMap(&savepoint1);

    /* Delete tuple. */
    m_utTableHandler->DeleteWithIndexTuple(indexTuple2, true);
    DstorePfreeExt(indexTuple2);

    /* Rollback to savepoint1, and check for result. */
    ret = transaction->RollbackToSavepoint("savepoint1");
    EXPECT_TRUE(STORAGE_FUNC_SUCC(ret));
    CheckTuplesAreTheSameWith(&savepoint1);
    transaction->Commit();

    /* Check again after commit. */
    transaction->Start();
    transaction->SetSnapshotCsn();
    CheckTuplesAreTheSameWith(&savepoint1);
    transaction->Commit();

    /* Savepoint0 and 1 should not exist after commit. */
    EXPECT_TRUE(transaction->IsSavepointListEmpty());
}

TEST_F(SavepointTest, ReleaseSavepointTest_level0)
{
    RetStatus ret;
    Transaction *transaction = thrd->GetActiveTransaction();
    EXPECT_TRUE(transaction->IsSavepointListEmpty());

    /* Start transaction. */
    transaction->Start();
    transaction->SetSnapshotCsn();

    /* Insert one tuple. */
    IndexTuple *indexTuple1 = m_utTableHandler->InsertRandomIndexTuple(true);
    DstorePfreeExt(indexTuple1);

    /* Create savepoint0. */
    ret = transaction->CreateSavepoint("savepoint0");
    EXPECT_TRUE(STORAGE_FUNC_SUCC(ret));
    EXPECT_FALSE(transaction->IsSavepointListEmpty());

    /* Delete savepoint0. */
    ret = transaction->ReleaseSavepoint("savepoint0");
    EXPECT_TRUE(STORAGE_FUNC_SUCC(ret));
    EXPECT_TRUE(transaction->IsSavepointListEmpty());

    /* Delete savepoint0 again, it should fail. */
    ret = transaction->ReleaseSavepoint("savepoint0");
    EXPECT_TRUE(STORAGE_FUNC_FAIL(ret));
    EXPECT_TRUE(StorageGetErrorCode() == TRANSACTION_ERROR_SAVEPOINT_NOT_FOUND);

    /* Create savepoint1. */
    ret = transaction->CreateSavepoint("savepoint1");
    EXPECT_TRUE(STORAGE_FUNC_SUCC(ret));
    EXPECT_FALSE(transaction->IsSavepointListEmpty());

    /* Rollback to savepoint1. */
    ret = transaction->RollbackToSavepoint("savepoint1");
    EXPECT_TRUE(STORAGE_FUNC_SUCC(ret));
    EXPECT_FALSE(transaction->IsSavepointListEmpty());

    transaction->Commit();
    EXPECT_TRUE(transaction->IsSavepointListEmpty());
}

TEST_F(SavepointTest, RollbackLockTest_level0)
{
    RetStatus ret;
    Transaction *transaction = thrd->GetActiveTransaction();
    transaction->Start();

    /* Insert one tuple to create xid. */
    IndexTuple *indexTuple1 = m_utTableHandler->InsertRandomIndexTuple(true);
    DstorePfreeExt(indexTuple1);

    /* Add a lock before create savepoint. */
    LockTag tag0;
    tag0.SetTableLockTag(0, 0);
    ret = g_storageInstance->GetLockMgr()->Lock(&tag0, DSTORE_EXCLUSIVE_LOCK, LOCK_WAIT, nullptr);
    EXPECT_TRUE(ret == DSTORE_SUCC);
    transaction->Remember2PLock(tag0, DSTORE_EXCLUSIVE_LOCK, LOCK_MGR);

    /* Create savepoint1. */
    ret = transaction->CreateSavepoint("savepoint1");
    EXPECT_TRUE(STORAGE_FUNC_SUCC(ret));

    /* Get some locks. */
    LockTag tag1;
    tag1.SetTableExtensionLockTag(g_defaultPdbId, INVALID_PAGE_ID);
    ret = g_storageInstance->GetLockMgr()->Lock(&tag1, DSTORE_EXCLUSIVE_LOCK, LOCK_WAIT, nullptr);
    EXPECT_TRUE(ret == DSTORE_SUCC);
    transaction->Remember2PLock(tag1, DSTORE_EXCLUSIVE_LOCK, LOCK_MGR);
    ret = g_storageInstance->GetLockMgr()->Lock(&tag1, DSTORE_SHARE_LOCK, LOCK_WAIT, nullptr);
    EXPECT_TRUE(ret == DSTORE_SUCC);
    transaction->Remember2PLock(tag1, DSTORE_SHARE_LOCK, LOCK_MGR);
    ret = g_storageInstance->GetLockMgr()->Lock(&tag1, DSTORE_SHARE_LOCK, LOCK_WAIT, nullptr);
    EXPECT_TRUE(ret == DSTORE_SUCC);
    transaction->Remember2PLock(tag1, DSTORE_SHARE_LOCK, LOCK_MGR);
    ret = g_storageInstance->GetLockMgr()->Lock(&tag1, DSTORE_ROW_EXCLUSIVE_LOCK, LOCK_WAIT, nullptr);
    EXPECT_TRUE(ret == DSTORE_SUCC);
    transaction->Remember2PLock(tag1, DSTORE_ROW_EXCLUSIVE_LOCK, LOCK_MGR);

    LockTag tag2;
    tag2.SetTableLockTag(0, 1);
    ret = g_storageInstance->GetTableLockMgr()->Lock(&tag2, DSTORE_ACCESS_SHARE_LOCK, LOCK_WAIT, nullptr);
    EXPECT_TRUE(ret == DSTORE_SUCC);
    transaction->Remember2PLock(tag2, DSTORE_ACCESS_SHARE_LOCK, TABLE_LOCK_MGR);
    ret = g_storageInstance->GetTableLockMgr()->Lock(&tag2, DSTORE_SHARE_UPDATE_EXCLUSIVE_LOCK, LOCK_WAIT, nullptr);
    EXPECT_TRUE(ret == DSTORE_SUCC);
    transaction->Remember2PLock(tag2, DSTORE_SHARE_UPDATE_EXCLUSIVE_LOCK, TABLE_LOCK_MGR);
    ret = g_storageInstance->GetTableLockMgr()->Lock(&tag2, DSTORE_ACCESS_EXCLUSIVE_LOCK, LOCK_WAIT, nullptr);
    EXPECT_TRUE(ret == DSTORE_SUCC);
    transaction->Remember2PLock(tag2, DSTORE_ACCESS_EXCLUSIVE_LOCK, TABLE_LOCK_MGR);

    /* Rollback to savepoint1. */
    ret = transaction->RollbackToSavepoint("savepoint1");
    EXPECT_TRUE(STORAGE_FUNC_SUCC(ret));

    /* Release the lock added before savepoint1. */
    g_storageInstance->GetLockMgr()->Unlock(&tag0, DSTORE_EXCLUSIVE_LOCK);
    transaction->Forget2PLock(tag0, DSTORE_EXCLUSIVE_LOCK, LOCK_MGR);

    /* Only one transaction lock exist at this point. */
    {
        const ThreadLocalLock::LocalLockEntry *entry = nullptr;
        int cnt = 0;
        ThreadLocalLock::HoldLockIterator iter(thrd->GetLockCtx()->GetLocalLock());
        while ((entry = iter.GetNextLock()) != nullptr) {
            EXPECT_TRUE(entry->tag.lockTagType == LOCKTAG_TRANSACTION);
            EXPECT_TRUE(entry->grantedTotal == 1);
            cnt++;
        }
        EXPECT_TRUE(cnt == 1);
    }

    /* Get lock again. */
    tag1.SetTableExtensionLockTag(g_defaultPdbId, INVALID_PAGE_ID);
    g_storageInstance->GetLockMgr()->Lock(&tag1, DSTORE_EXCLUSIVE_LOCK, LOCK_WAIT, nullptr);
    transaction->Remember2PLock(tag1, DSTORE_EXCLUSIVE_LOCK, LOCK_MGR);

    /* After commit, all locks should be released. */
    transaction->Commit();
    EXPECT_TRUE(transaction->IsSavepointListEmpty());
    EXPECT_TRUE(thrd->GetLockCtx()->GetLocalLock()->IsEmpty());
}

TEST_F(SavepointTest, RollbackDuplicateLockTest_level0)
{
    RetStatus ret;
    Transaction *transaction = thrd->GetActiveTransaction();
    transaction->Start();

    /* Insert one tuple to create xid. */
    IndexTuple *indexTuple1 = m_utTableHandler->InsertRandomIndexTuple(true);
    DstorePfreeExt(indexTuple1);

    /* Add a lock before create savepoint. */
    LockTag tag0;
    tag0.SetTableLockTag(0, 0);
    ret = g_storageInstance->GetLockMgr()->Lock(&tag0, DSTORE_EXCLUSIVE_LOCK, LOCK_WAIT, nullptr);
    EXPECT_TRUE(ret == DSTORE_SUCC);
    transaction->Remember2PLock(tag0, DSTORE_EXCLUSIVE_LOCK, LOCK_MGR);

    /* Create savepoint1. */
    ret = transaction->CreateSavepoint("savepoint1");
    EXPECT_TRUE(STORAGE_FUNC_SUCC(ret));

    /* Get the lock again. */
    ret = g_storageInstance->GetLockMgr()->Lock(&tag0, DSTORE_EXCLUSIVE_LOCK, LOCK_WAIT, nullptr);
    EXPECT_TRUE(ret == DSTORE_SUCC);
    transaction->Remember2PLock(tag0, DSTORE_EXCLUSIVE_LOCK, LOCK_MGR);

    /* Create savepoint2. */
    ret = transaction->CreateSavepoint("savepoint2");
    EXPECT_TRUE(STORAGE_FUNC_SUCC(ret));

    /* Get the lock again. */
    ret = g_storageInstance->GetLockMgr()->Lock(&tag0, DSTORE_EXCLUSIVE_LOCK, LOCK_WAIT, nullptr);
    EXPECT_TRUE(ret == DSTORE_SUCC);
    transaction->Remember2PLock(tag0, DSTORE_EXCLUSIVE_LOCK, LOCK_MGR);

    /* Rollback to savepoint1. */
    ret = transaction->RollbackToSavepoint("savepoint1");
    EXPECT_TRUE(STORAGE_FUNC_SUCC(ret));

    /* The 2 locks added after savepoint1 are released. */
    {
        const ThreadLocalLock::LocalLockEntry *entry = nullptr;
        int cnt = 0;
        ThreadLocalLock::HoldLockIterator iter(thrd->GetLockCtx()->GetLocalLock());
        while ((entry = iter.GetNextLock()) != nullptr) {
            EXPECT_TRUE(entry->grantedTotal == 1);
            cnt++;
        }
        /* Including transaction lock. */
        EXPECT_TRUE(cnt == 2);
    }

    /* Release the lock added before savepoint1. */
    g_storageInstance->GetLockMgr()->Unlock(&tag0, DSTORE_EXCLUSIVE_LOCK);
    transaction->Forget2PLock(tag0, DSTORE_EXCLUSIVE_LOCK, LOCK_MGR);

    /* Only one transaction lock exist at this point. */
    {
        const ThreadLocalLock::LocalLockEntry *entry = nullptr;
        int cnt = 0;
        ThreadLocalLock::HoldLockIterator iter(thrd->GetLockCtx()->GetLocalLock());
        while ((entry = iter.GetNextLock()) != nullptr) {
            EXPECT_TRUE(entry->tag.lockTagType == LOCKTAG_TRANSACTION);
            EXPECT_TRUE(entry->grantedTotal == 1);
            cnt++;
        }
        EXPECT_TRUE(cnt == 1);
    }

    /* We don't hold any lock anymore after commit. */
    transaction->Commit();
    EXPECT_TRUE(transaction->IsSavepointListEmpty());
    EXPECT_TRUE(thrd->GetLockCtx()->GetLocalLock()->IsEmpty());
}

