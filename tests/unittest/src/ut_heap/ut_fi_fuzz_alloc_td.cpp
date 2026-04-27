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
#include "tuple/dstore_data_tuple.h"
#include "undo/dstore_undo_zone.h"
#include "ut_mock/ut_mock.h"
#include "page/dstore_heap_page.h"
#include "ut_heap/ut_heap.h"

const int TdStatusNum = static_cast<int>(TDStatus::OCCUPY_TRX_END) + 1;
/* State machine for td->m_status.
 * TdStatusTransition[x][y] replace whether td->m_status can transition from x to y.
 * Only transtions list below are allowed:
 * UNOCCUPY_AND_PRUNEABLE --> UNOCCUPY_AND_PRUNEABLE
 * TXN_STATUS_IN_PROGRESS --> UNOCCUPY_AND_PRUNEABLE
 * TXN_STATUS_IN_PROGRESS --> TXN_STATUS_IN_PROGRESS
 * TXN_STATUS_IN_PROGRESS --> OCCUPY_TRX_END
 * OCCUPY_TRX_END --> UNOCCUPY_AND_PRUNEABLE
 * OCCUPY_TRX_END --> OCCUPY_TRX_END
 */
static bool TdStatusTransition[TdStatusNum][TdStatusNum] = {
    true, false, false,
    true, true, true,
    true, false, true
};

const int TupleTdStatusNum = static_cast<int>(TupleTdStatus::DETACH_TD) + 1;
/* State machine for tupleTdStatus.
 * TupleTdStatusTransition[x][y] replace whether tupleTdStatus can transition from x to y.
 * Only transtions list below are allowed:
 * ATTACH_TD_AS_NEW_OWNER --> ATTACH_TD_AS_NEW_OWNER
 * ATTACH_TD_AS_NEW_OWNER --> ATTACH_TD_AS_HISTORY_OWNER
 * ATTACH_TD_AS_NEW_OWNER --> DETACH_TD
 * ATTACH_TD_AS_HISTORY_OWNER --> ATTACH_TD_AS_HISTORY_OWNER
 * ATTACH_TD_AS_HISTORY_OWNER --> DETACH_TD
 * DETACH_TD --> DETACH_TD
 */
static bool TupleTdStatusTransition[TupleTdStatusNum][TupleTdStatusNum] = {
    true, true, true,
    false, true, true,
    false, false, true
};

void UTHeap::DfsFuzzTestTdStatus(FuzzInput* testcase, TdId tdId, HeapPage* oldPage, HeapPage* newPage)
{
    /* Generate test case finish, start test */
    if (tdId == DEFAULT_TD_COUNT) {
        errno_t rc = memcpy_s(newPage, BLCKSZ, oldPage, BLCKSZ);
        storage_securec_check(rc, "\0", "\0");
        /* step1: prepare page, according the page */
        for (uint8 i = 0; i < DEFAULT_TD_COUNT; i++) {
            TD* td = newPage->GetTd(i);
            td->SetStatus(static_cast<TDStatus>(testcase[i].tdStatus));
            Xid xid = static_cast<Xid>(td->m_xid);
            SetTransactionSlotStatus(xid, static_cast<TrxSlotStatus>(testcase[i].slotStatus));
            HeapDiskTuple* heapTuple = newPage->GetDiskTuple(i + 1);
            heapTuple->SetTdStatus(static_cast<TupleTdStatus>(testcase[i].tupleTdStatus));
        }

        /* step2: do alloctd */
        TDAllocContext tdContext;
        tdContext.Init(g_defaultPdbId, true);
        tdContext.Begin(0);
        newPage->AllocTd(tdContext);

        /* step3: check result */
        for (uint8 i = 0; i < DEFAULT_TD_COUNT; i++) {
            /* check td status */
            int oldTdStatus = static_cast<int>(oldPage->GetTd(i)->GetStatus());
            int newTdStatus = static_cast<int>(newPage->GetTd(i)->GetStatus());
            StorageAssert(TdStatusTransition[oldTdStatus][oldTdStatus]);
            StorageAssert(newPage->GetTd(i)->CheckSanity());
            /* check tuple td status */
            int oldTupleTdStatus = static_cast<int>(oldPage->GetDiskTuple(i + 1)->GetTdStatus());
            int newTupleTdStatus = static_cast<int>(newPage->GetDiskTuple(i + 1)->GetTdStatus());
            StorageAssert(TupleTdStatusTransition[oldTupleTdStatus][newTupleTdStatus]);
        }
        return;
    }

    /* Generate Case */
    for (int tdStatus = static_cast<int>(TDStatus::UNOCCUPY_AND_PRUNEABLE);
        tdStatus <= static_cast<int>(TDStatus::OCCUPY_TRX_END); tdStatus++) {
        /* because td xid and csn is valid, so tdstatus can not be UNOCCUPY_AND_PRUNEABLE */
        if (tdStatus == static_cast<int>(TDStatus::UNOCCUPY_AND_PRUNEABLE)) {
            continue;
        }
        for (int slotStatus = static_cast<int>(TrxSlotStatus::TXN_STATUS_FROZEN);
            slotStatus <= static_cast<int>(TrxSlotStatus::TXN_STATUS_ABORTED); slotStatus++) {
            /* if txn is TXN_STATUS_ABORTED, page must has donwn td rollback, can not be TXN_STATUS_ABORTED.
             * if txn is TXN_STATUS_PENDING_COMMIT, will wait txn end, can not be TXN_STATUS_ABORTED.
             */
            if (slotStatus == static_cast<int>(TrxSlotStatus::TXN_STATUS_ABORTED) ||
                slotStatus == static_cast<int>(TrxSlotStatus::TXN_STATUS_PENDING_COMMIT)) {
                continue;
            }
            /* if txn is TXN_STATUS_IN_PROGRESS, td status must be OCCUPY_TRX_IN_PROGRESS. */
            if (slotStatus == static_cast<int>(TrxSlotStatus::TXN_STATUS_IN_PROGRESS) &&
                tdStatus != static_cast<int>(TDStatus::OCCUPY_TRX_IN_PROGRESS)) {
                continue;
            }
            for (int tupleTdStatus = static_cast<int>(TupleTdStatus::ATTACH_TD_AS_NEW_OWNER);
                tupleTdStatus <= static_cast<int>(TupleTdStatus::DETACH_TD); tupleTdStatus++) {
                testcase[tdId].tdStatus = tdStatus;
                testcase[tdId].slotStatus = slotStatus;
                testcase[tdId].tupleTdStatus = tupleTdStatus;
                DfsFuzzTestTdStatus(testcase, tdId + 1, oldPage, newPage);
            }
        }
    }
}

/* this case to ensure, any td->m_status or any xid txn slot status, proc td->m_status correctly */
TEST_F(UTHeap, HeapAllocTdFuzzTest_level0)
{
    Transaction *txn = thrd->GetActiveTransaction();

    /* Step 1: Insert four tuple, will use tdId(0, 1, 2, 3) */
    const int tupleNum = 4;
    ItemPointerData ctids[tupleNum];
    Xid xids[tupleNum];
    for (int i = 0; i < tupleNum; i++) {
        txn->Start();
        txn->SetSnapshotCsn();
        HeapTuple* heapTuple = m_utTableHandler->GenerateSpecificHeapTuple("test_tuple");
        ctids[i] = m_utTableHandler->InsertHeapTupAndCheckResult(heapTuple, true, txn->GetSnapshotData());
        xids[i] = txn->GetCurrentXid();
        CheckTupleTdId(ctids[i], i % DEFAULT_TD_COUNT);
        CheckTupleLockerTdId(ctids[i], INVALID_TD_SLOT);
        txn->Commit();
    }
    /* Step2: prepare page, start fuzz test. */
    char oldPage[BLCKSZ];
    char newPage[BLCKSZ];
    txn->Start();
    txn->SetSnapshotCsn();
    txn->AllocTransactionSlot();
    BufferDesc *bufferDesc = g_storageInstance->GetBufferMgr()->Read(g_defaultPdbId,
        ctids[0].GetPageId(), LW_SHARED);
    HeapPage *heapPage = static_cast<HeapPage *>(bufferDesc->GetPage());
    errno_t rc = memcpy_s(oldPage, BLCKSZ, heapPage, BLCKSZ);
    storage_securec_check(rc, "\0", "\0");
    rc = memcpy_s(newPage, BLCKSZ, heapPage, BLCKSZ);
    storage_securec_check(rc, "\0", "\0");
    g_storageInstance->GetBufferMgr()->UnlockAndRelease(bufferDesc);

    FuzzInput testcase[DEFAULT_TD_COUNT];
    DfsFuzzTestTdStatus(testcase, 0, static_cast<HeapPage *>(static_cast<void *>(oldPage)),
        static_cast<HeapPage *>(static_cast<void *>(newPage)));
    txn->Commit();
}