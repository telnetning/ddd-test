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
#ifndef DSTORE_UT_BTREE_H
#define DSTORE_UT_BTREE_H

#include "ut_tablehandler/ut_table_handler.h"
#include "heap/dstore_heap_scan.h"
#include "index/dstore_btree_insert.h"
#include "index/dstore_btree_delete.h"
#include "index/dstore_index_handler.h"
#include "index/dstore_btree_prune.h"
#include "index/dstore_btree_page_recycle.h"
#include "common/fault_injection/dstore_transaction_fault_injection.h"
#include "common/fault_injection/dstore_index_fault_injection.h"
#include <queue>

using namespace DSTORE;

class UTRecyclableBtree;
enum class FakeTupleStatus {
    FAKE_NORMAL = 0,
    FAKE_NO_PRUNEABLE,
    FAKE_IN_PROGRESS
};

class UTBtree : virtual public DSTORETEST {
public:
    void CreateIndexBySpecificData(int keyAttrNum, int **indexCols = nullptr);
    IndexTuple *InsertSpecificIndexTuple(DefaultRowDef *insertRow,
                                         bool *nullbitmap = DefaultNullBitMap,
                                         bool alreadyXactStart = false,
                                         UTTableHandler *utTableHandler = nullptr);
    virtual RetStatus DeleteIndexTuple(IndexTuple *indexTuple,
                                       bool alreadyXactStart = false,
                                       UTTableHandler *utTableHandler = nullptr);
    IndexTuple *InsertIndexTupleOnly(DefaultRowDef *insertRow, bool *nullbitmap = DefaultNullBitMap,
                                     ItemPointerData fakeHeapCtid = INVALID_ITEM_POINTER);
    RetStatus DeleteIndexTupleOnly(IndexTuple *indexTuple);

    PageId GetLeftmostLeaf(UTTableHandler *utTableHandler = nullptr);

    static void InitScanKeyInt24(ScanKey keyInfos, Datum arg1, uint16 strategy1, Datum arg2, uint16 strategy2);
    static int GetSatisfiedTupleNum(IndexScanHandler &indexScan, HeapScanHandler &heapScan,
                                    ScanDirection dir = ScanDirection::FORWARD_SCAN_DIRECTION,
                                    bool checkIndexOnly = false);

protected:
    void SetUp() override
    {
        DSTORETEST::SetUp();
        MockStorageInstance *instance = DstoreNew(m_ut_memory_context) MockStorageInstance();
        instance->Install(&DSTORETEST::m_guc, m_ut_memory_context);
        instance->Startup(&DSTORETEST::m_guc);
        m_utTableHandler = UTTableHandler::CreateTableHandler(g_defaultPdbId, m_ut_memory_context, false);

#ifdef ENABLE_FAULT_INJECTION
    FaultInjectionEntry entries[] = {
        FAULT_INJECTION_ENTRY(DstoreTransactionFI::READ_PAGE_FROM_CR_BUFFER, false, ConstructCrPageFromCrBuffer),
        FAULT_INJECTION_ENTRY(DstoreTransactionFI::ALLOC_TD_FAIL, false, nullptr),
    }; 
    ASSERT_EQ(RegisterFaultInjection(entries, sizeof(entries) / sizeof(entries[0]), FI_GLOBAL), ERROR_SYS_OK);

#endif
    }

    void TearDown() override
    {
        UTTableHandler::Destroy(m_utTableHandler);
        m_utTableHandler = nullptr;
        MockStorageInstance *instance = (MockStorageInstance *)g_storageInstance;
        instance->Shutdown();
        delete instance;
        DSTORETEST::TearDown();
    }

    UTTableHandler *m_utTableHandler;
    static constexpr int NUM_PRESET_ROWS = 200;

    int CompareIndexTuple(IndexTuple *tuple1, IndexTuple *tuple2, TupleDesc indexTupleDesc);
    void TestTupleSort();
    bool IsTupleSorted(TuplesortMgr *tuplesortMgr, uint16 expectedTuplesNum);

    PageId TestBtreeMetaPage(PageId metaPageId);
    void TestBtreeRootPage(PageId rootPageId, uint32 rootLevel);
    std::queue<PageId> TestPivotTuples(TupleDesc indexTupleDesc, std::queue<PageId> &pivotPageIds, int level);
    void TestLeafTuples(TupleDesc indexTupleDesc, std::queue<PageId> &leafPageIds, int rowNum);
    void TestNullLeafTuplesInSpecificOrder(TupleDesc indexTupleDesc, std::queue<PageId> &leafPageIds, int rowNum);

    bool IsTupleInRightPosition(TupleDesc indexTupleDesc, std::queue<PageId> &leafPageIds, int position, IndexTuple *target);
    bool IsTupleDeleted(TupleDesc indexTupleDesc, std::queue<PageId> &leafPageIds, int offset, IndexTuple *target);

    void CheckLiveItemsInfo(BtreePagePrune *btreePagePrune, BtrPage *page, int numLiveTuples);
    void CheckTuplesOnPage(BtrPage *page, OffsetNumber minoff, OffsetNumber maxoff, uint16 oldUpper, int deltaTupleNum);
    void GenerateFakeTDStates(BtrPage *page, int numTds);
    void GenerateFakeTDStatesNoNeedPrune(BtrPage *page, int numTds);
    void GenerateFakeTDInProgress(BtrPage *page, int numTds);
    void GenerateFakeTupleStates(BtrPage *page, bool *isTupleLive, int numItems, int *numLiveItemIds);
    void GenerateFakeNoTuplePrunable(int numItems, int *numLiveItemIds);
    void GenerateFakeTupleInprogress(BtrPage *page, bool *isTupleLive, int numItems, int *numLiveItemIds);
    void GenerateFakeTuples(BtrPage *page, OffsetNumber minOff, OffsetNumber maxOff, int numTds);
    void GenerateFakeCallback(IndexBuildInfo *indexBuildInfo);
    BtreePagePrune *CreateFakeBtreePrune(int tdCount);
    BtreePagePrune *GenerateEmptyPrunablePage(int numTuples);
    BtreePagePrune *PrepareBtreePrune(int numTuples, int *numLiveItemIds, bool *isTupleLive, bool needCompactTD,
                                      FakeTupleStatus stat = FakeTupleStatus::FAKE_NORMAL);

    UTRecyclableBtree *GenerateRecyclableSubtree(int numLeaves, int rootLevel);
    void CheckRecyclableSubtree(int numLeaves, int rootLevel, UTRecyclableBtree *recyclableBtree);
};

class UTRecyclableBtree : public BtreeSplit {
public:
    UTRecyclableBtree(UTTableHandler *utTableHandler) : BtreeSplit(utTableHandler->GetIndexRel(),
        utTableHandler->GetIndexInfo(), utTableHandler->GetIndexScanKey(), true), m_utTableHandler(utTableHandler)
    {}

    ~UTRecyclableBtree()
    {
        DstorePfree(m_leafPageIds);
        for (int i = 0; i < m_numLeaves; i++) {
            DstorePfree(m_leafTuples[i]);
        }
        DstorePfree(m_leafTuples);
    }

    StorageRelation GetIndexRel()
    {
        return m_indexRel;
    }

    BtreeStorageMgr *GetBtrStorageMgr()
    {
        return m_indexRel->btreeSmgr;
    }
    
    IndexInfo *GetIndexInfo()
    {
        return m_indexInfo;
    }
    ScanKey GetScanKey()
    {
        return m_scanKeyValues.scankeys;
    }

    PageId GetLeaf(int leafNo)
    {
        if (leafNo < m_numLeaves) {
            return m_leafPageIds[leafNo];
        }
        return INVALID_PAGE_ID;
    }

    void SetBtrPageUnlinkStack(BtreePageUnlink *btrUnlink)
    {
        btrUnlink->SetStack(m_leafStack);
    }


    BufferDesc *SearchForLeaf(int leafNo);
    PageId MakePageEmpty(int leafNo, CommitSeqNo snapshotCsn = INVALID_CSN);
    void RecyclePageAndCheck(int leafNo);

    IndexTuple **CreateLeaves(int numLeaves);
    void CreateInternalPages(int &numChildren, IndexTuple **upperDownlinkTuples, int currLevel, bool isRoot);

    UTTableHandler *m_utTableHandler;

private:
    void CreateRoot(int &numChildren, IndexTuple **upperDownlinkTuples, int rootLevel);
    int m_numLeaves;
    PageId *m_leafPageIds;
    IndexTuple **m_leafTuples;
};

#endif
