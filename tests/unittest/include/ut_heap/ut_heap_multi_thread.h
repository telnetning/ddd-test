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

#ifndef DSTORE_UT_HEAP_MULTI_THREAD_H
#define DSTORE_UT_HEAP_MULTI_THREAD_H

#include <gtest/gtest.h>
#include "heap/dstore_heap_update.h"
#include "transaction/dstore_transaction_mgr.h"
#include "ut_heap/ut_heap.h"
#include "ut_utilities/ut_thread_pool.h"
#include "ut_utilities/ut_sync_point_group.h"
#include <list>
#include <tuple>

class UTHeapMultiThread : virtual public UTHeap {
public:
    void SetUp() override
    {
#ifdef ENABLE_FAULT_INJECTION

    FaultInjectionEntry entries[] = {
        FAULT_INJECTION_ENTRY(DstoreHeapFI::CONSTRUCT_CR_BEFORE_READ_BASE_PAGE, false, nullptr),
        FAULT_INJECTION_ENTRY(DstoreHeapFI::CONSTRUCT_CR_AFTER_READ_BASE_PAGE, false, nullptr),
        FAULT_INJECTION_ENTRY(DstoreHeapFI::CONSTRUCT_CR_PAGE, false, nullptr),
        FAULT_INJECTION_ENTRY(DstoreHeapFI::READY_TO_WAIT_TRX_END, false, nullptr),
        FAULT_INJECTION_ENTRY(DstoreUndoFI::GET_UNDO_ZONE, false, nullptr),
        FAULT_INJECTION_ENTRY(DstoreUndoFI::RECOVER_UNDO_ZONE, false, nullptr),
        };
    ASSERT_EQ(RegisterFaultInjection(entries, sizeof(entries) / sizeof(entries[0]), FI_GLOBAL), ERROR_SYS_OK);

#endif
        UTHeap::SetUp();
        m_heapSegment = m_utTableHandler->m_heapSegmentPageId;
        m_lobSegment = m_utTableHandler->m_lobSegmentPageId;
        int threadNum = m_config.ReadInteger("UTHeapMultiThread-ThreadNum");
        if (threadNum < 0) {
            threadNum = 64;
        }
        m_pool.Start(threadNum);
    }

    void TearDown() override
    {
        m_pool.Shutdown();
        UTHeap::TearDown();
        ResetTestData();
#ifdef ENABLE_FAULT_INJECTION

    DestroyFaultInjectionHash(FI_GLOBAL);

#endif
    }

    virtual void BuildThreadLocalVar();

    static void HeapInsertTask(UTHeapMultiThread *ptr, int tupleLen)
    {
        ptr->HeapInsertFixedLengthTuple(tupleLen);
    }

    static void HeapBatchInsertTask(UTHeapMultiThread *ptr, int tupleNum)
    {
        ptr->HeapBatchInsertFixedLengthTuple(tupleNum);
    }

    static void HeapLockTupDelayCommitTask(UTHeapMultiThread *ptr)
    {
        ptr->HeapLockTupDelayCommit();
    }

    static void HeapFetchTask(UTHeapMultiThread *ptr)
    {
        ptr->HeapFetchAndCheckWithSnapshot();
    }

    static void HeapUpdateTask(UTHeapMultiThread *ptr, int tupleLen)
    {
        ptr->HeapUpdateFixedLengthTuple(tupleLen);
    }

    static void HeapConcurrentUpdateTask(UTHeapMultiThread *ptr, int tupleLen)
    {
        ptr->HeapUpdateConcurrently(tupleLen);
    }

    static void HeapDeleteTask(UTHeapMultiThread *ptr)
    {
        ptr->HeapDeleteTuple();
    }

    static void HeapConcurrentDeleteTask(UTHeapMultiThread *ptr)
    {
        ptr->HeapDeleteConcurrently();
    }

    static void HeapScanTask(UTHeapMultiThread *ptr)
    {
        ptr->HeapScanAndCheckWithSnapshot();
    }

    static void LobScanTask(UTHeapMultiThread *ptr)
    {
        ptr->LobScan();
    }

    static void LobFetchTask(UTHeapMultiThread *ptr)
    {
        ptr->LobFetch();
    }

    static void LobInsertTask(UTHeapMultiThread *ptr, int tupleLen)
    {
        ptr->LobInsert(tupleLen);
    }

    static void LobDeleteTask(UTHeapMultiThread *ptr)
    {
        ptr->LobDelete();
    }

    static void LobUpdateTask(UTHeapMultiThread *ptr, int tupleLen)
    {
        ptr->LobUpdate(tupleLen);
    }

    static void LobConcurrentDeleteTask(UTHeapMultiThread *ptr)
    {
        ptr->LobDeleteConcurrently();
    }

    static void LobConcurrentUpdateTask(UTHeapMultiThread *ptr, int tupleLen)
    {
        ptr->LobUpdateConcurrently(tupleLen);
    }

    virtual void HeapInsertFixedLengthTuple(int tupleLen);
    void HeapBatchInsertFixedLengthTuple(int tupleNum);
    void HeapLockTupDelayCommit();
    void HeapFetchAndCheckWithSnapshot();
    void HeapScanAndCheckWithSnapshot();
    void HeapUpdateFixedLengthTuple(int tupleLen);
    void HeapUpdateConcurrently(int tupleLen);
    void HeapDeleteTuple();
    void HeapDeleteConcurrently();

    void LobScan();
    void LobFetch();
    void LobInsert(int tupleLen);
    void LobDelete();
    void LobUpdate(int tupleLen);
    void LobDeleteConcurrently();
    void LobUpdateConcurrently(int tupleLen);

    void HeapShowAnyTupleUncommitTuple(int updateDataLen);
    static void HeapShowAnyTupleTask(UTHeapMultiThread *ptr, int updateDataLen)
    {
        ptr->HeapShowAnyTupleUncommitTuple(updateDataLen);
    }

    void ResetTestData()
    {
        m_mutex.lock();
        m_csnCtidMap.clear();
        while (!m_ctids.empty()) {
            m_ctids.pop();
        }
        m_mutex.unlock();
    }

    void Lock()
    {
        m_mutex.lock();
    }

    void UnLock()
    {
        m_mutex.unlock();
    }

    void UpdateWithTrx1Commit(SyncPointGroup *syncPointGroup, ItemPointerData specCtid);
    void UpdateWithTrx2Abort(SyncPointGroup *syncPointGroup, ItemPointerData specCtid);

    static void UpdateThread1(UTHeapMultiThread *ptr, SyncPointGroup *syncPointGroup, ItemPointerData specCtid);
    static void UpdateThread2(UTHeapMultiThread *ptr, SyncPointGroup *syncPointGroup, ItemPointerData specCtid);

    void RollbackThread(SyncPointGroup *syncPointGroup);
    void HeapScanThread(SyncPointGroup *syncPointGroup);

    static void ThreadRollback(UTHeapMultiThread *ptr, SyncPointGroup *syncPointGroup);
    static void ThreadHeapScan(UTHeapMultiThread *ptr, SyncPointGroup *syncPointGroup);

    void UnLockXidThread(SyncPointGroup *syncPointGroup);
    void LockUnchangedTupleThread(SyncPointGroup *syncPointGroup);
    void LockTupleUnLockXidThread(SyncPointGroup *syncPointGroup);

    static void ThreadUnlockXid(UTHeapMultiThread *ptr, SyncPointGroup *syncPointGroup);
    static void ThreadLockUnchangedTuple(UTHeapMultiThread *ptr, SyncPointGroup *syncPointGroup);
    static void ThreadLockTupleUnlockXid(UTHeapMultiThread *ptr, SyncPointGroup *syncPointGroup);

    void GetUndoZoneThread(SyncPointGroup *syncPointGroup);
    void AsynRollbackThread(SyncPointGroup *syncPointGroup);

    static void ThreadGetUndoZone(UTHeapMultiThread *ptr, SyncPointGroup *syncPointGroup);
    static void ThreadAsynRollback(UTHeapMultiThread *ptr, SyncPointGroup *syncPointGroup);

    void HeapInsertFixedLengthTupleWithFixedTabSmgr(int tupleLen);

    void HeapUpdateTupleAndWait(ItemPointerData ctid, int tupleLen, SyncPointGroup *syncPointGroup, int syncPointId);
    static void HeapUpdateTupleAndWaitTask(UTHeapMultiThread *ptr, ItemPointerData ctid, int tupleLen,
        SyncPointGroup *syncPointGroup, int syncPointId);
    
    void HeapUpdateTupleGivenCtid(ItemPointerData ctid, int tupleLen);
    static void HeapUpdateTupleGivenCtidTask(UTHeapMultiThread *ptr, ItemPointerData ctid, int tupleLen);

    void HeapUpdateTupleAndWaitOnLock(ItemPointerData ctid, int tupleLen,
        SyncPointGroup *syncPointGroup, int syncPointId);
    static void HeapUpdateTupleAndWaitOnLockTask(UTHeapMultiThread *ptr, ItemPointerData ctid, int tupleLen,
        SyncPointGroup *syncPointGroup, int syncPointId);
    
    void AddUpdateTaskForTdDeadlock(ItemPointerData *outCtid, SyncPointGroup *syncPointGroup);

    void HeapUpdateTupleGivenCtidWhileHoldingLock(ItemPointerData ctid, int tupleLen, SyncPointGroup *syncPointGroup,
        int syncPointId);
    static void HeapUpdateTupleGivenCtidWhileHoldingLockTask(UTHeapMultiThread *ptr, ItemPointerData ctid,
        int tupleLen, SyncPointGroup *syncPointGroup, int syncPointId);
    
    void HeapLockTupleGivenCtidWhileHoldingLock(ItemPointerData ctid, SyncPointGroup *syncPointGroup,
        int syncPointId);
    static void HeapLockTupleGivenCtidWhileHoldingLockTask(UTHeapMultiThread *ptr, ItemPointerData ctid,
        SyncPointGroup *syncPointGroup, int syncPointId);

    void HeapDeleteTupleGivenCtidWhileHoldingLock(ItemPointerData ctid, SyncPointGroup *syncPointGroup,
        int syncPointId);
    static void HeapDeleteTupleGivenCtidWhileHoldingLockTask(UTHeapMultiThread *ptr, ItemPointerData ctid,
        SyncPointGroup *syncPointGroup, int syncPointId);

    void FillHeapSpaceForTdAlloc(std::vector<ItemPointerData> &ctids);

    void HeapUpdateTupleToHoldTd(ItemPointerData ctid, int tupleLen,
        SyncPointGroup *syncPointGroup, int syncPointId, Xid *xid);
    static void HeapUpdateTupleToHoldTdTask(UTHeapMultiThread *ptr, ItemPointerData ctid, int tupleLen,
        SyncPointGroup *syncPointGroup, int syncPointId, Xid *xid);

    void HeapDeleteTupleForCancel(ItemPointerData ctid, SyncPointGroup *syncPointGroup,
        int syncPointId, ThreadContext **testThrd, uint64 *releaseTime);
    static void HeapDeleteTupleForCancelTask(UTHeapMultiThread *ptr, ItemPointerData ctid,
        SyncPointGroup *syncPointGroup, int syncPointId, ThreadContext **testThrd, uint64 *releaseTime);

    void FillGivenHeapNumForTdAlloc(std::vector<ItemPointerData> &ctids, int num);

    void HeapDeleteTupleToHoldTd(ItemPointerData ctid, SyncPointGroup *syncPointGroup,
        int syncPointId, Xid *xid);
    static void HeapDeleteTupleToHoldTdTask(UTHeapMultiThread *ptr, ItemPointerData ctid,
        SyncPointGroup *syncPointGroup, int syncPointId, Xid *xid);

    void HeapUpdateTupleForCancel(ItemPointerData ctid, int tupleLen, SyncPointGroup *syncPointGroup,
    int syncPointId, ThreadContext **testThrd, uint64 *releaseTime);
    static void HeapUpdateTupleForCancelTask(UTHeapMultiThread *ptr, ItemPointerData ctid, int tupleLen,
    SyncPointGroup *syncPointGroup, int syncPointId, ThreadContext **testThrd, uint64 *releaseTime);

protected:
    UTThreadPool m_pool;
    std::atomic<int> m_counter{0};
    std::list<std::tuple<CommitSeqNo, ItemPointerData, std::string>> m_csnCtidMap;
    std::queue<ItemPointerData> m_ctids;
    std::queue<UndoZone*> m_undozones;
    /* Data segment info */
    PageId m_heapSegment = INVALID_PAGE_ID;
    PageId m_lobSegment = INVALID_PAGE_ID;

private:
    std::mutex m_mutex;
};

#endif
