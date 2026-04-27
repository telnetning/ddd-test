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
#ifndef DSTORE_UT_HEAP_H
#define DSTORE_UT_HEAP_H

#include "ut_tablehandler/ut_table_handler.h"
#include "heap/dstore_heap_update.h"
#include "heap/dstore_heap_interface.h"

using namespace DSTORE;

class UTHeap : public virtual DSTORETEST {
public:
typedef struct {
    int tdStatus;
    int slotStatus;
    int tupleTdStatus;
} FuzzInput;
    void MultiUpdateTest(std::vector<HeapTuple *> heapTupsForInsert, std::vector<HeapTuple *> heapTupsForUpdate,
                         std::vector<HeapTuple *> HeapTupsForUpdate2);
    void RollbackUpdateTest(std::vector<HeapTuple *> heapTupsForInsert, std::vector<HeapTuple *> heapTupsForUpdate,
                            std::vector<HeapTuple *> HeapTupsForUpdate2);
    void SetTransactionSlotStatus(Xid xid, TrxSlotStatus status);
    void CheckTupleTdId(ItemPointerData ctid, TdId expectTdId);
    void CheckTupleLockerTdId(ItemPointerData ctid, TdId expectTdId);
    TD* GetTd(PageId pageId, TdId tdId);
    void DfsFuzzTestTdStatus(FuzzInput* testcase, TdId tdId, HeapPage* oldPage, HeapPage* newPage);

protected:
    Bitmapset *FormReplicaIdentityKeys(UTTableHandler *utTableHandler = nullptr);
    ItemPointerData InsertSpecificHeapTuple(std::string rawData, SnapshotData *snapshot = INVALID_SNAPSHOT,
                                            bool alreadyStartXact = false);
    HeapTuple *InsertRandomHeapTuple();
    ItemPointerData UpdateTupAndCheckResult(ItemPointerData *ctid, HeapTuple *tuple,
                                            SnapshotData *snapshot = INVALID_SNAPSHOT, bool alreadyStartXact = false,
                                            UTTableHandler *utTableHandler = nullptr);
    ItemPointerData UpdateTuple(ItemPointerData* ctid, HeapTuple *tuple,
                                SnapshotData *snapshot = INVALID_SNAPSHOT, bool alreadyStartXact = false,
                                UTTableHandler *utTableHandler = nullptr, bool addTxnCid = true);
    RetStatus DeleteTuple(ItemPointerData *ctid, SnapshotData *snapshot = INVALID_SNAPSHOT,
                          bool alreadyStartXact = false, UTTableHandler *utTableHandler = nullptr);
    int LockUnchangedTuple(ItemPointerData ctid, SnapshotData *snap = INVALID_SNAPSHOT, bool alreadyStartXact = false,
                           UTTableHandler *utTableHandler = nullptr);
    ItemPointerData PrepareTupleForUndo(std::string &data, SnapshotData &snapshot, ItemId &originItemId, TD &originTd,
                                        int tupleNums);
    void CheckCrPage(ItemPointerData ctid, HeapTuple *originTup = nullptr, ItemId *originItemId = nullptr,
                     TD *originTd = nullptr, CommitSeqNo originCsn = INVALID_CSN);
    void CheckCrTuple(ItemPointerData ctid, HeapTuple *originTup = nullptr);
    SnapshotData ConstructCurSnapshot();
    static void ConstructCurSnapshotThrd(std::atomic<bool> *isSnapshotThreadNeedStop);
    void UtPrepareOneDataPage(HeapNormalSegment *heapSegment);
    void SetTupleXidStatus(ItemPointerData ctid, TrxSlotStatus status, PdbId pdbId = 0);
    void InitScanKeyInt24(ScanKey keyInfos, Datum arg1, uint16 strategy1, Datum arg2, uint16 strategy2);
    void DestroySnapshotThrdIfNeed();
    std::string GenerateRandomString(int len);

    HeapTuple *GenerateTupleWithLob(std::string &value, int len);
    varlena *FetchLobValue(varlena *value);
    RetStatus InsertTupleWithLob(HeapTuple *tuple, ItemPointerData &ctid);
    RetStatus DeleteTupleWithLob(HeapDeleteContext &deleteContext, ItemPointerData &ctid, bool needReturnTup);
    RetStatus UpdateTupleWithLob(HeapUpdateContext &updateContext, ItemPointerData &oldCtid, HeapTuple *newTuple,
                                 bool needReturnTup);
    void ForceUpdateTupleNoTrx(ItemPointerData *ctid, HeapTuple *tuple);

    void SetUp() override
    {
        DSTORETEST::SetUp();
        MockStorageInstance *instance = DstoreNew(m_ut_memory_context) MockStorageInstance();
        instance->Install(&DSTORETEST::m_guc, m_ut_memory_context);
        instance->Startup(&DSTORETEST::m_guc);
        m_utTableHandler = UTTableHandler::CreateTableHandler(g_defaultPdbId, m_ut_memory_context, true);
        m_snapshotThread = nullptr;
        m_ssMemCtx = nullptr;
    }

    void TearDown() override
    {
        DestroySnapshotThrdIfNeed();
        UTTableHandler::Destroy(m_utTableHandler);
        m_utTableHandler = nullptr;
        MockStorageInstance *instance = (MockStorageInstance *)g_storageInstance;
        instance->Shutdown();
        delete instance;
        DSTORETEST::TearDown();
    }

    UTTableHandler *m_utTableHandler;

    class std::thread     *m_snapshotThread; /* transaction is in progress to prevent undo recycle */
    DstoreMemoryContext m_ssMemCtx;
    std::atomic<bool> m_isSnapshotThreadNeedStop;
};

#endif /* DSTORE_UT_HEAP_H */

