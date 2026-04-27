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
#include "lock/dstore_xact_lock_mgr.h"
#include "tuple/dstore_data_tuple.h"
#include "undo/dstore_undo_zone.h"
#include "ut_mock/ut_mock.h"
#include "common/fault_injection/undo_fault_injection.h"
#include "heap/dstore_heap_wal_struct.h"
#include "page/dstore_heap_page.h"
#include "ut_wal/ut_wal_basic.h"
#include "ut_heap/ut_heap.h"
#include "ut_heap/ut_heap_wal.h"
#include "ut_heap/ut_heap_multi_thread.h"


/**
 * CCT::This case is same with UTHeapMultiThread.HeapPageRollbackTest (Page rollback case)
 * CCT::thread-1: {t1:f1, t2:ni}
 * CCT::thread-2: {t1:ni, t2:f2}
 */
TEST_F(UTHeapMultiThread, HeapPageRollbackTest2_level0)
{

    // CCT::b_f1
    {
        // implementation from ThreadUnlockXid(syncPointGroup)
        BuildThreadLocalVar();
        Transaction *txn = thrd->GetActiveTransaction();
        TransactionMgr *transactionMgr = g_storageInstance->GetPdb(g_defaultPdbId)->GetTransactionMgr();
        UndoMgr *undoMgr = g_storageInstance->GetPdb(g_defaultPdbId)->GetUndoMgr();

        std::string data = GenerateRandomString(2000);
        HeapTuple *heapTuple = UTTableHandler::GenerateSpecificHeapTuple(data);
        ItemPointerData ctid = ThdUtTableHandler->InsertHeapTupAndCheckResult(heapTuple);

        txn->Start();
        txn->SetSnapshotCsn();
        std::string newData = GenerateRandomString(6000); /* make sure another-page-update */
        HeapTuple *newTuple = UTTableHandler::GenerateSpecificHeapTuple(newData);
        ItemPointerData newCtid =
            UpdateTupAndCheckResult(&ctid, newTuple, txn->GetSnapshotData(), true, ThdUtTableHandler);
        ASSERT_NE(newCtid, INVALID_ITEM_POINTER);
        Xid xid = txn->GetCurrentXid();
        g_storageInstance->GetXactLockMgr()->Unlock(g_defaultPdbId, xid);

        Lock();
        m_ctids.push(ctid);
        UnLock();
    }
    // CCT::e_f1

    // CCT::b_f2
    {
        // implementation from ThreadLockUnchangedTuple(syncPointGroup)
        BuildThreadLocalVar();
        Transaction *txn = thrd->GetActiveTransaction();
        TransactionMgr *transactionMgr = g_storageInstance->GetPdb(g_defaultPdbId)->GetTransactionMgr();
        UndoMgr *undoMgr = g_storageInstance->GetPdb(g_defaultPdbId)->GetUndoMgr();

        txn->Start();
        txn->SetSnapshotCsn();
        Lock();
        ASSERT_TRUE(!m_ctids.empty());
        ItemPointerData ctid = m_ctids.front();
        m_ctids.pop();
        UnLock();

        HeapLockTupleContext lockTupContext;
        lockTupContext.ctid = ctid;
        lockTupContext.needRetTup = true;
        StorageRelation heapRel;
        heapRel->tableSmgr = ThdUtTableHandler->GetHeapTabSmgr();
        {
            HeapLockTupHandler heapLockTup(g_storageInstance, thrd, heapRel);
            RetStatus ret = heapLockTup.LockUnchangedTuple(&lockTupContext);
            ASSERT_TRUE(STORAGE_FUNC_SUCC(ret));
        }
        HeapTuple *retTuple = lockTupContext.retTup->Copy();
        txn->Commit();

        HeapTuple *tuple = ThdUtTableHandler->FetchHeapTuple(&ctid);

        int actual_val = memcmp(retTuple->GetDiskTuple()->GetData(), tuple->GetDiskTuple()->GetData(),
                         tuple->GetDiskTupleSize() - HEAP_DISK_TUP_HEADER_SIZE);
        
        EXPECT_EQ(0, actual_val);
        
        DstorePfreeExt(retTuple);
        DstorePfreeExt(tuple);
    }
    // CCT::e_f2
}
