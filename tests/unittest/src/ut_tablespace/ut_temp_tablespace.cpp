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
#include "index/dstore_btree_build.h"
#include "heap/dstore_heap_delete.h"
#include "common/datatype/dstore_compare_utils.h"
#include "index/dstore_btree_build.h"
#include "diagnose/dstore_index_diagnose.h"
#include "errorcode/dstore_index_error_code.h"
#include "errorcode/dstore_page_error_code.h"
#include "index/dstore_index_interface.h"
#include "systable/dstore_relation.h"
#include "tablespace/dstore_index_temp_segment.h"
#include "ut_heap/ut_heap.h"
#include "ut_btree/ut_btree.h"
#include "ut_tablespace/ut_segment.h"
#include "ut_tablespace/ut_temp_tablespace.h"

void UTTempTablespace::DestroySnapshotThrd()
{
    if (m_snapshotThread == nullptr) {
        return;
    }

    ASSERT_FALSE(m_isSnapshotThreadNeedStop);
    m_isSnapshotThreadNeedStop = true;
    ErrLog(DSTORE::DSTORE_DEBUG1, DSTORE::MODULE_HEAP, ErrMsg("m_isSnapshotThreadNeedStop is set to true"));
    m_snapshotThread->join();
    delete m_snapshotThread;
    m_snapshotThread = nullptr;
}

/*
 * If tuple1 < tuple2, return -1.
 * If tuple1 > tuple2, return 1.
 * If tuple1 = tuple2, return 0.
 * Note: for same level in btree, two equaled tuples(key + ctid) are not exist.
 */
int UTTempTablespace::CompareIndexTuples(IndexTuple *tuple1, IndexTuple *tuple2, TupleDesc indexTupleDesc)
{
    StorageAssert(tuple1 != nullptr);
    StorageAssert(tuple2 != nullptr);
    int cmp;
    Datum datum1, datum2;
    bool isnull1, isnull2;
    int keyNum1 = tuple1->GetKeyNum(indexTupleDesc->natts);
    int keyNum2 = tuple2->GetKeyNum(indexTupleDesc->natts);
    int minKeyNum = DstoreMin(keyNum1, keyNum2);
    for (int i = 0; i < minKeyNum; ++i) {
        datum1 = tuple1->GetAttr(i + 1, indexTupleDesc, &isnull1);
        datum2 = tuple2->GetAttr(i + 1, indexTupleDesc, &isnull2);
        if (isnull1 && isnull2) {
            continue;
        } else if (isnull1) {
            /* nulls last default */
            return 1;
        } else if (isnull2) {
            return -1;
        }
        cmp = m_utTableHandler->CompareDatum(datum1, datum2, indexTupleDesc->attrs[i]->atttypid);
        if (cmp != 0) {
            return cmp;
        }
    }

    if (keyNum1 < keyNum2) {
        return -1;
    } else if (keyNum1 > keyNum2) {
        return 1;
    }

    /* If tuple1 and tuple2 have same key, compare heap ctid */
    ItemPointerData ctid1 = tuple1->GetHeapCtid();
    ItemPointerData ctid2 = tuple2->GetHeapCtid();

    if (!tuple1->IsPivot() && tuple2->IsPivot()) {
        if (ctid2 == INVALID_ITEM_POINTER) {
            cmp = 0;
        } else {
            cmp = -1;
        }
    } else if (tuple1->IsPivot() && !tuple2->IsPivot()) {
        if (ctid1 == INVALID_ITEM_POINTER) {
            cmp = 0;
        } else {
            cmp = 1;
        }
    } else {
        /* if pivot and truncated */
        if (ctid1 == INVALID_ITEM_POINTER && ctid2 != INVALID_ITEM_POINTER)
            cmp = -1;
        else if (ctid1 != INVALID_ITEM_POINTER && ctid2 == INVALID_ITEM_POINTER)
            cmp = 1;
        else if (ctid1 == INVALID_ITEM_POINTER && ctid2 == INVALID_ITEM_POINTER)
            cmp = 0;
        else
            cmp = ItemPointerData::Compare(&ctid1, &ctid2);
    }

    return cmp;
}

/*
 * Test page in level 0, all tuples should be ordered ascendingly.
 */
void UTTempTablespace::TestLeafTuple(TupleDesc indexTupleDesc, std::queue<PageId> &leafPageIds, int rowNum)
{
    int tupleNumInLevel0 = 0;
    PageId lastPageId = INVALID_PAGE_ID;
    PageId nextPageId = INVALID_PAGE_ID;
    bool isRootPage = leafPageIds.size() == 1;
    bool firsTupleInLevel = true;
    OffsetNumber startOffset = BTREE_PAGE_FIRSTKEY;
    IndexTuple *otuple;
    IndexTuple *prevPageLastTuple = nullptr;
    IndexTuple *highKeyTuple;
    AutoMemCxtSwitch autoSwitch{thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LONGLIVE)};

    while (!leafPageIds.empty()) {
        PageId curPageId = leafPageIds.front();
        leafPageIds.pop();
        BufferDesc *buf = thrd->GetTmpLocalBufMgr()->
            Read(g_defaultPdbId, curPageId, DSTORE::LW_SHARED);
        BtrPage *leafPage = (BtrPage*)buf->GetPage();
        BtrPageLinkAndStatus *leafPageMeta = leafPage->GetLinkAndStatus();

        if (!leafPageIds.empty()) {
            nextPageId = leafPageIds.front();
            highKeyTuple = leafPage->GetIndexTuple(BTREE_PAGE_HIKEY);
        } else {
            nextPageId = INVALID_PAGE_ID;
            startOffset = BTREE_PAGE_HIKEY;
        }
        EXPECT_EQ(leafPageMeta->prev, lastPageId);
        EXPECT_EQ(leafPageMeta->next, nextPageId);
        EXPECT_EQ(leafPageMeta->level, 0);
        if (isRootPage) {
            EXPECT_TRUE(leafPageMeta->TestType(DSTORE::BtrPageType::LEAF_PAGE));
            EXPECT_TRUE(leafPageMeta->IsRoot());
        } else {
            EXPECT_TRUE(leafPageMeta->TestType(DSTORE::BtrPageType::LEAF_PAGE));
            EXPECT_FALSE(leafPageMeta->IsRoot());
        }

        OffsetNumber limit = leafPage->GetMaxOffset();
        for (OffsetNumber n = startOffset; n <= limit; ++n) {
            IndexTuple *tuple = leafPage->GetIndexTuple(n);
            if (firsTupleInLevel) {
                firsTupleInLevel = false;
            } else {
                int cmp = CompareIndexTuples(otuple, tuple, indexTupleDesc);
                EXPECT_EQ(cmp, -1);
            }
            if (n == limit) {
                DstorePfreeExt(prevPageLastTuple);
                prevPageLastTuple = otuple = tuple->Copy();
            } else {
                otuple = tuple;
            }
        }
        if (!leafPageIds.empty()) {
            int cmp = CompareIndexTuples(otuple, highKeyTuple, indexTupleDesc);
            EXPECT_EQ(cmp, -1);
        }

        thrd->GetTmpLocalBufMgr()->UnlockAndRelease(buf);
        tupleNumInLevel0 += (limit - startOffset + 1);
        lastPageId = curPageId;
    }
    EXPECT_EQ(tupleNumInLevel0, rowNum);
    DstorePfreeExt(prevPageLastTuple);
}

IndexTuple *UTTempTablespace::InsertSpecificIndexTuples(DefaultRowDef *insertRow, bool *nullbitmap, bool alreadyXactStart,
                                                       UTTableHandler *utTableHandler)
{
    Transaction *txn = thrd->GetActiveTransaction();
    if (!alreadyXactStart) {
        txn->Start();
    }

    UTTableHandler *tableHandler = (utTableHandler == nullptr) ? m_utTableHandler : utTableHandler;

    /* Get heap tuple */
    DstoreMemoryContext oldMem = DstoreMemoryContextSwitchTo(
        thrd->m_memoryMgr->GetGroupContext(MemoryGroupType::MEMORY_CONTEXT_LONGLIVE));
    TupleDesc heapTupleDesc = tableHandler->GetHeapTupDesc();
    Datum *heapValues = (Datum*)DstorePalloc(heapTupleDesc->natts * sizeof(Datum));
    bool *heapIsnulls = (bool*)DstorePalloc(heapTupleDesc->natts * sizeof(bool));
    HeapTuple *heapTuple = tableHandler->GetSpecificHeapTuple(insertRow, heapValues, heapIsnulls, nullbitmap);

    /* Heap insert */
    std::unique_ptr<StorageRelationData> heapRel(new StorageRelationData());
    heapRel->tableSmgr = tableHandler->GetHeapTabSmgr();
    heapRel->m_pdbId = g_defaultPdbId;
    HeapInsertHandler heapInsert(g_storageInstance, thrd, heapRel.get());
    HeapInsertContext heapInsertContext;
    heapInsertContext.heapTuple = heapTuple;
    heapInsertContext.cid = txn->GetCurCid();
    RetStatus status = heapInsert.Insert(&heapInsertContext);
    DstorePfreeExt(heapTuple);
    if (STORAGE_FUNC_FAIL(status)) {
        /* We must assert error code before abort(), or error code maybe overwritten. */
        StorageAssert(StorageGetErrorCode() == PAGE_ERROR_NO_SPACE_FOR_TD);
        if (!alreadyXactStart) {
            txn->Abort();
        }
        return nullptr;
    }

    /* Get index value */
    TupleDesc indexTupleDesc = tableHandler->GetIndexTupleDesc();
    int16 *indexAttrOffsets = (int16 *)(&tableHandler->GetIndexBuildInfo()->indexAttrOffset);
    Datum *indexValues = (Datum *)DstorePalloc(indexTupleDesc->natts * sizeof(Datum));
    bool *indexIsnulls = (bool *)DstorePalloc(indexTupleDesc->natts * sizeof(bool));
    for (int16 i = 0; i < indexTupleDesc->natts; i++) {
        int keyIdx = indexAttrOffsets[i] - 1;
        indexValues[i] = heapValues[keyIdx];
        indexIsnulls[i] = heapIsnulls[keyIdx];
    }
    DstorePfree(heapValues);
    DstorePfree(heapIsnulls);
    DstoreMemoryContextSwitchTo(oldMem);

    /* Index insert */
    BtreeInsert btreeInsert(tableHandler->GetIndexRel(), tableHandler->GetIndexInfo(), tableHandler->GetIndexScanKey());
    status = btreeInsert.InsertTuple(indexValues, indexIsnulls, &heapInsertContext.ctid);
    if (!alreadyXactStart) {
        if (STORAGE_FUNC_FAIL(status)) {
            txn->Abort();
        } else {
            txn->Commit();
        }
    } else {
        txn->IncreaseCommandCounter();
    }

    /* Get index tuple and return */
    IndexTuple *indexTuple = nullptr;
    if (STORAGE_FUNC_SUCC(status)) {
        indexTuple = IndexTuple::FormTuple(indexTupleDesc, indexValues, indexIsnulls);
        indexTuple->SetHeapCtid(&heapInsertContext.ctid);
    }
    DstorePfree(indexValues);
    DstorePfree(indexIsnulls);

    return indexTuple;
}

std::queue<PageId> UTTempTablespace::TestPivotTuple(TupleDesc indexTupleDesc, std::queue<PageId> &pivotPageIds, int level)
{
    StorageAssert(level > 0);
    std::queue<PageId> pageIds;
    PageId lastPageId = INVALID_PAGE_ID;
    PageId nextPageId = INVALID_PAGE_ID;
    bool firstTupleInOnePage = true;
    bool isRootPage = pivotPageIds.size() == 1;
    OffsetNumber startOffset = BTREE_PAGE_FIRSTKEY;
    IndexTuple *otuple;
    IndexTuple *highKeyTuple;

    /* Test page in level > 0 */
    while (!pivotPageIds.empty()) {
        PageId curPageId = pivotPageIds.front();
        pivotPageIds.pop();
        BufferDesc *buf = thrd->GetTmpLocalBufMgr()->
            Read(g_defaultPdbId, curPageId, DSTORE::LW_SHARED);
        BtrPage *pivotPage = (BtrPage*)buf->GetPage();
        BtrPageLinkAndStatus *pivotPageMeta = pivotPage->GetLinkAndStatus();

        if (!pivotPageIds.empty()) {
            nextPageId = pivotPageIds.front();
            highKeyTuple = (IndexTuple*)pivotPage->GetRowData(BTREE_PAGE_HIKEY);
        } else {
            nextPageId = INVALID_PAGE_ID;
            startOffset = BTREE_PAGE_HIKEY;
        }
        EXPECT_EQ(pivotPageMeta->prev, lastPageId);
        EXPECT_EQ(pivotPageMeta->next, nextPageId);
        EXPECT_EQ(pivotPageMeta->level, level);
        if (isRootPage) {
            EXPECT_TRUE(pivotPageMeta->IsRoot());
        } else {
            EXPECT_FALSE(pivotPageMeta->TestType(DSTORE::BtrPageType::LEAF_PAGE));
        }

        OffsetNumber limit = pivotPage->GetMaxOffset();
        for (OffsetNumber n = startOffset; n <= limit; ++n) {
            IndexTuple *tuple = (IndexTuple*)pivotPage->GetRowData(n);
            pageIds.push(tuple->GetLowlevelIndexpageLink());
            if (firstTupleInOnePage) {
                /* The first pivot key in every pivot page is minus infinity */
                EXPECT_EQ(tuple->m_link.val.num, 0);
                firstTupleInOnePage = false;
            } else {
                int cmp = CompareIndexTuples(otuple, tuple, indexTupleDesc);
                EXPECT_EQ(cmp, -1);
            }
            otuple = tuple;
        }
        if (!pivotPageIds.empty()) {
            int cmp = CompareIndexTuples(otuple, highKeyTuple, indexTupleDesc);
            EXPECT_EQ(cmp, -1);
        }

        thrd->GetTmpLocalBufMgr()->UnlockAndRelease(buf);
        lastPageId = curPageId;
        firstTupleInOnePage = true;
    }

    return pageIds;
}

/*
 * Test whether the specific tuple located in right position.
 * position means the offset of all leaf pages other than one page.
 */
bool UTTempTablespace::IsTupleInExpectPosition(TupleDesc indexTupleDesc, std::queue<PageId> &leafPageIds,
                                     int position, IndexTuple *target)
{
    int num = 0;
    OffsetNumber startOffset = BTREE_PAGE_FIRSTKEY;
    while (!leafPageIds.empty()) {
        PageId curPageId = leafPageIds.front();
        leafPageIds.pop();
        if (leafPageIds.empty()) {
            startOffset = BTREE_PAGE_HIKEY;
        }
        BufferDesc *buf = thrd->GetTmpLocalBufMgr()->
            Read(g_defaultPdbId, curPageId, DSTORE::LW_SHARED);
        BtrPage *leafPage = (BtrPage*)buf->GetPage();
        OffsetNumber limit = leafPage->GetMaxOffset();
        if (num + limit - startOffset + 1 < position) {
            /* If the position isn't located in this page, skip it quickly */
            num += (limit - startOffset + 1);
            thrd->GetTmpLocalBufMgr()->UnlockAndRelease(buf);
            continue;
        }
        for (OffsetNumber n = startOffset; n <= limit; ++n) {
            IndexTuple *tuple = (IndexTuple*)leafPage->GetRowData(n);
            num++;
            if (num == position) {
                int cmp = CompareIndexTuples(target, tuple, indexTupleDesc);
                EXPECT_EQ(cmp, 0);
                thrd->GetTmpLocalBufMgr()->UnlockAndRelease(buf);
                return true;
            }
        }
    }
    /* not found */
    return false;
}
/* delete index tuple from btree and delete corresponding heap tuple from heap. */
RetStatus UTTempTablespace::DeleteIndexTuples(IndexTuple *indexTuple, bool alreadyXactStart,
                                    UTTableHandler *utTableHandler)
{
    if (utTableHandler != nullptr) {
        return utTableHandler->DeleteWithIndexTuple(indexTuple, alreadyXactStart);
    }
    return m_utTableHandler->DeleteWithIndexTuple(indexTuple, alreadyXactStart);
}

void UTTempTablespace::TestBtreeRootPages(PageId rootPageId, uint32 rootLevel)
{
    BufferDesc *rootBuf = thrd->GetTmpLocalBufMgr()->
        Read(g_defaultPdbId, rootPageId, DSTORE::LW_SHARED);
    BtrPage *rootPage = (BtrPage*)rootBuf->GetPage();
    BtrPageLinkAndStatus *rootPageMeta = rootPage->GetLinkAndStatus();
    EXPECT_EQ(rootPageMeta->level, rootLevel);
    EXPECT_EQ(rootPageMeta->prev, INVALID_PAGE_ID);
    EXPECT_EQ(rootPageMeta->next, INVALID_PAGE_ID);
    if (rootLevel > 0) {
        EXPECT_TRUE(rootPageMeta->IsRoot());
    } else {
        EXPECT_TRUE(rootPageMeta->IsRoot());
        EXPECT_TRUE(rootPageMeta->TestType(DSTORE::BtrPageType::LEAF_PAGE));
    }
    thrd->GetTmpLocalBufMgr()->UnlockAndRelease(rootBuf);
}

bool UTTempTablespace::CheckTupleDeleted(TupleDesc indexTupleDesc, std::queue<PageId> &leafPageIds,
                             int offset, IndexTuple *target)
{
    int num = 0;
    OffsetNumber startOffset = BTREE_PAGE_FIRSTKEY;
    while (!leafPageIds.empty()) {
        PageId curPageId = leafPageIds.front();
        leafPageIds.pop();
        if (leafPageIds.empty()) {
            startOffset = BTREE_PAGE_HIKEY;
        }
        BufferDesc *buf = thrd->GetTmpLocalBufMgr()->
            Read(g_defaultPdbId, curPageId, DSTORE::LW_SHARED);
        BtrPage *leafPage = (BtrPage*)buf->GetPage();
        OffsetNumber limit = leafPage->GetMaxOffset();
        for (OffsetNumber n = startOffset; n <= limit; ++n) {
            num++;
            if (num == offset) {
                IndexTuple *tuple = (IndexTuple*)leafPage->GetRowData(n);
                int cmp = CompareIndexTuples(target, tuple, indexTupleDesc);
                EXPECT_EQ(cmp, 0);
                bool isDeleted = tuple->IsDeleted();
                thrd->GetTmpLocalBufMgr()->UnlockAndRelease(buf);
                return isDeleted;
            }
        }
        thrd->GetTmpLocalBufMgr()->UnlockAndRelease(buf);
    }
    /* not found */
    return false;
}

ItemPointerData UTTempTablespace::InsertSpecificHeapTuples(std::string data, SnapshotData *snapshot, bool alreadyStartXact)
{
    HeapTuple *heapTuple = m_utTableHandler->GenerateSpecificHeapTuple(data);
    ItemPointerData ctid = m_utTableHandler->InsertHeapTupAndCheckResult(heapTuple, alreadyStartXact, snapshot);
    DstorePfreeExt(heapTuple);
    return ctid;
}

PageId UTTempTablespace::TestBtreeMetaPages(PageId metaPageId)
{
    PageId segmentMetaPageId = m_utTableHandler->GetBtreeSmgr()->GetSegment()->GetSegmentMetaPageId();
    PageId FistDataPageId = {segmentMetaPageId.m_fileId, segmentMetaPageId.m_blockId + 1};
    EXPECT_EQ(metaPageId, FistDataPageId);
    BufferDesc *btrMetaBuf = thrd->GetTmpLocalBufMgr()->
        Read(g_defaultPdbId, metaPageId, DSTORE::LW_SHARED);
    BtrPage *btrMetaPage = (BtrPage*)btrMetaBuf->GetPage();
    PageId btrRootPageId = ((BtrMeta *)btrMetaPage->GetData())->GetLowestSinglePage();
    EXPECT_NE(btrRootPageId, INVALID_PAGE_ID);
    thrd->GetTmpLocalBufMgr()->UnlockAndRelease(btrMetaBuf);
    return btrRootPageId;
}

TEST_F(UTTempTablespace, HeapInsertTest)
{
    Transaction *txn = thrd->GetActiveTransaction();

    /* Step 1: Insert one tuple */
    txn->Start();
    txn->SetSnapshotCsn();
    HeapTuple *heapTuple = m_utTableHandler->GenerateRandomHeapTuple();
    int diskTupleLen = heapTuple->GetDiskTupleSize();
    ItemPointerData ctid = m_utTableHandler->InsertHeapTupAndCheckResult(heapTuple, true, txn->GetSnapshotData());
    Xid xid = txn->GetCurrentXid();
    txn->Commit();

    /* Step 2: Check TD */
    FAULT_INJECTION_ACTIVE(DstoreBufMgrFI::READ_BLOCK_FAULT, FI_GLOBAL);
    BufferDesc *bufferDesc1 = thrd->GetTmpLocalBufMgr()->
        Read(g_defaultPdbId, {1, 3}, DSTORE::LW_SHARED);
    ASSERT_EQ(bufferDesc1, INVALID_BUFFER_DESC);
    FAULT_INJECTION_INACTIVE(DstoreBufMgrFI::READ_BLOCK_FAULT, FI_GLOBAL);

    BufferDesc *bufferDesc = thrd->GetTmpLocalBufMgr()->
        Read(g_defaultPdbId, ctid.GetPageId(), DSTORE::LW_SHARED);
    auto page = (HeapPage*)bufferDesc->GetPage();
    HeapDiskTuple *diskTuple = page->GetDiskTuple(ctid.GetOffset());
    TD *td = page->GetTd(diskTuple->GetTdId());
    ASSERT_EQ(td->GetXid(), xid);
    UndoRecord undoRec;
    TransactionMgr *transactionMgr = g_storageInstance->GetPdb(g_defaultPdbId)->GetTransactionMgr();
    if (STORAGE_FUNC_SUCC(transactionMgr->FetchUndoRecord(xid, &undoRec, td->GetUndoRecPtr()))){
        ASSERT_EQ(ctid, undoRec.GetCtid());
    }
    thrd->GetTmpLocalBufMgr()->UnlockAndRelease(bufferDesc);

    /* Step 3: Check tuple */
    HeapTuple* tuple = m_utTableHandler->FetchHeapTuple(&ctid);
    ASSERT_EQ(memcmp((void*)tuple->GetDiskTuple(), (void*)heapTuple->GetDiskTuple(), diskTupleLen), 0);

    DstorePfreeExt(tuple);
    DstorePfreeExt(heapTuple);
}

TEST_F(UTTempTablespace, HeapBatchInsertTest)
{
    /* Construct big tuple. */
    std::string bigString;
    int bigTupleLen = 18000;
    for (int i = 0; i < bigTupleLen; i++) {
        bigString.append("a");
    }
    const int STEP = 5;

    Transaction *txn = thrd->GetActiveTransaction();

    /* Step 1: Insert some tuples */
    txn->Start();
    txn->SetSnapshotCsn();
    uint16 nTuples = 60;
    HeapTuple *heapTuples[nTuples];
    int diskTupleLens[nTuples];
    for (int i = 0; i < nTuples; i++) {
        HeapTuple *heapTuple = nullptr;
        int diskTupleLen = 0;
        /* generate some big tuples */
        if (i % STEP == 0) {
            heapTuple = m_utTableHandler->GenerateSpecificHeapTuple(bigString);
            diskTupleLen = heapTuple->GetDiskTupleSize();
        } else {
            heapTuple = m_utTableHandler->GenerateRandomHeapTuple();
            diskTupleLen = heapTuple->GetDiskTupleSize();
        }
        heapTuples[i] = heapTuple;
        diskTupleLens[i] = diskTupleLen;
    }
    ItemPointerData ctids[nTuples];
    m_utTableHandler->BatchInsertHeapTupsAndCheckResult(heapTuples, nTuples, ctids, txn->GetSnapshotData(), true);
    Xid xid = txn->GetCurrentXid();
    txn->Commit();

    /* Step 2: Fecth the UndoRecord and check it */
    UndoRecord undoRec;
    UndoRecPtr undoRecPtr = INVALID_UNDO_RECORD_PTR;
    bool fetchUndoRprFromTd = true;
    TransactionMgr *transactionMgr = g_storageInstance->GetPdb(g_defaultPdbId)->GetTransactionMgr();
    for (int i = nTuples - 1; i >= 0 ; i--) {
        HeapTuple *heapTuple = heapTuples[i];
        ItemPointerData ctid = ctids[i];
        int diskTupleLen = diskTupleLens[i];

        if (i % STEP == 0) {
            /* Check big tuple */
            HeapTuple *tuple = m_utTableHandler->FetchHeapTuple(&ctid);
            EXPECT_TRUE(tuple->GetDiskTupleSize() == diskTupleLen);
            EXPECT_EQ(memcmp((void*)tuple->GetDiskTuple()->GetData(), (void*)bigString.c_str(), bigTupleLen), 0);
            DstorePfreeExt(tuple);
            continue;
        }

        /* Check small tuple */
        BufferDesc *bufferDesc = thrd->GetTmpLocalBufMgr()->Read(
            g_defaultPdbId, ctid.GetPageId(), DSTORE::LW_SHARED);
        auto page = (HeapPage *)bufferDesc->GetPage();
        HeapDiskTuple *diskTuple = page->GetDiskTuple(ctid.GetOffset());
        TD *td = page->GetTd(diskTuple->GetTdId());
        ASSERT_EQ(td->GetXid(), xid);
        if (undoRecPtr == INVALID_UNDO_RECORD_PTR) {
            undoRecPtr = td->GetUndoRecPtr();
        }
        if (STORAGE_FUNC_SUCC(transactionMgr->FetchUndoRecord(xid, &undoRec, undoRecPtr))) {
            if (!undoRec.IsMatchedCtid(ctid)) {
                undoRecPtr = undoRec.GetTxnPreUndoPtr();
                if (STORAGE_FUNC_SUCC(transactionMgr->FetchUndoRecord(xid, &undoRec, undoRecPtr))) {
                    ASSERT_EQ(true, undoRec.IsMatchedCtid(ctid));
                }
            }
        }
        thrd->GetTmpLocalBufMgr()->UnlockAndRelease(bufferDesc);

        /* Check tuple */
        HeapTuple *tuple = m_utTableHandler->FetchHeapTuple(&ctid);
        ASSERT_EQ(memcmp((void *)tuple->GetDiskTuple(), (void *)heapTuple->GetDiskTuple(), diskTupleLen), 0);
        DstorePfreeExt(tuple);
    }
}

/* this test case batch insert, the page has enough space to insert the third tuple, but not the MAXALIGN third tuple */
TEST_F(UTTempTablespace, HeapBatchInsertTestPageNotEnough)
{
    TransactionMgr *transactionMgr = g_storageInstance->GetPdb(g_defaultPdbId)->GetTransactionMgr();
    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetSnapshotCsn();

    /* Insert three tuple, the first two tuple batch insert one page, third tuple another page */
    const int newTuples = 3;
    HeapTuple *newHeapTuples[newTuples];
    uint32 newDiskTupleLens[newTuples];
    HeapTuple *heapTuple = m_utTableHandler->GenerateSpecificHeapTuple(std::string(5000, 'a'));
    newHeapTuples[0] = heapTuple;
    newDiskTupleLens[0] = heapTuple->GetDiskTupleSize();
    heapTuple = m_utTableHandler->GenerateSpecificHeapTuple(std::string(2584, 'a'));
    newHeapTuples[1] = heapTuple;
    newDiskTupleLens[1] = heapTuple->GetDiskTupleSize();
    heapTuple = m_utTableHandler->GenerateSpecificHeapTuple(std::string(350, 'a'));
    newHeapTuples[2] = heapTuple;
    newDiskTupleLens[2] = heapTuple->GetDiskTupleSize();

    ItemPointerData newCtids[newTuples];
    m_utTableHandler->BatchInsertHeapTupsAndCheckResult(
        newHeapTuples, newTuples, newCtids, txn->GetSnapshotData(), true);
    Xid xid = txn->GetCurrentXid();
    txn->Commit();

    /* check pageid and undorec */
    ASSERT_TRUE(newCtids[0].GetPageId() == newCtids[1].GetPageId());
    ASSERT_TRUE(newCtids[0].GetPageId() != newCtids[2].GetPageId());
    UndoRecord newUndoRec;
    UndoRecPtr newUndoRecPtr = INVALID_UNDO_RECORD_PTR;
    for (int i = newTuples - 1; i >= 0 ; i--) {
        ItemPointerData ctid = newCtids[i];
        BufferDesc *bufferDesc = thrd->GetTmpLocalBufMgr()->Read(
            g_defaultPdbId, ctid.GetPageId(), DSTORE::LW_SHARED);
        HeapPage* page = static_cast<HeapPage *>(static_cast<void*>(bufferDesc->GetPage()));
        HeapDiskTuple *diskTuple = page->GetDiskTuple(ctid.GetOffset());
        TD *td = page->GetTd(diskTuple->GetTdId());
        ASSERT_EQ(td->GetXid(), xid);
        if (newUndoRecPtr == INVALID_UNDO_RECORD_PTR) {
            newUndoRecPtr = td->GetUndoRecPtr();
        }
        ASSERT_TRUE(STORAGE_FUNC_SUCC(transactionMgr->FetchUndoRecord(xid, &newUndoRec, newUndoRecPtr)));
        if (!newUndoRec.IsMatchedCtid(ctid)) {
            newUndoRecPtr = newUndoRec.GetTxnPreUndoPtr();
            ASSERT_TRUE(STORAGE_FUNC_SUCC(transactionMgr->FetchUndoRecord(xid, &newUndoRec, newUndoRecPtr)));
            ASSERT_EQ(true, newUndoRec.IsMatchedCtid(ctid));
        }
        thrd->GetTmpLocalBufMgr()->UnlockAndRelease(bufferDesc);
    }
}


TEST_F(UTTempTablespace, BtreeInsertTest_TIER1)
{
    /* Fill table with specific data */
    int rowNum = 200;
    DefaultRowDef *tableDef = (DefaultRowDef*)DstorePalloc(rowNum * sizeof(DefaultRowDef));
    /* Insert (2,2)(4,4)...(400,400) */
    for (int i = 1; i <= rowNum; ++i) {
        int16 r1 = (int16)(i * 2);
        int32 r2 = (int32)(i * 2);
        DefaultRowDef rowDef = m_utTableHandler->GetDefaultRowDef();
        rowDef.column_int16 = r1;
        rowDef.column_int32 = r2;
        tableDef[i - 1] = rowDef;
    }
    m_utTableHandler->FillTableWithSpecificData(tableDef, rowNum);
    DstorePfree(tableDef);

    /* Build btree index */
    int indexCols[] = {1, 2};
    bool isUnique = false;
    int numAttrs = sizeof(indexCols) / sizeof(int);
    m_utTableHandler->CreateIndex(indexCols, numAttrs, isUnique);
    BtreeStorageMgr *btrSmgr = m_utTableHandler->GetBtreeSmgr();
    TupleDesc indexTupleDesc = m_utTableHandler->GetIndexTupleDesc();

    /* Test btree meta page info */
    PageId metaPageId = btrSmgr->GetBtrMetaPageId();
    PageId btrRootPageId;

    /*
     * Insert (1,1)(3,3)...(399,399)(401,401)(401,402)(402,403)(402,404)(403,405)(403,406)...
     * until the btree has 3 level, about 71957 tuples.
     * The position means where the indexTuple should be inserted in the all leaf level.
     * The test includes leaf page split, pivot page split, create new root, add downlink to parent.
     */
    PageId lastRootPageId = INVALID_PAGE_ID;
    bool isThreeLevel = false;
    int remainNum = 10;
    uint32 rootLevel = 0;
    int insertNum = 1;
    int16 r1;
    int32 r2;
    bool reverseFlag = true;
    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    while (true) {
        std::queue<PageId> leafPageIds;
        std::queue<PageId> pivotPageIds;
        r1 = insertNum <= rowNum + 1 ? (int16)(insertNum * 2 - 1) : (int16)(r1 + (!reverseFlag));
        r2 = insertNum <= rowNum + 1 ? (int32)(insertNum * 2 - 1) : (int32)(r2 + 1);
        int position = r2;
        /* Insert specific tuple and check its position */
        DefaultRowDef insertRow = m_utTableHandler->GetDefaultRowDef();
        insertRow.column_int16 = r1;
        insertRow.column_int32 = r2;
        IndexTuple *indexTuple = InsertSpecificIndexTuples(&insertRow, DefaultNullBitMap, true);

        /* check every 10 times for saving ut time */
        btrRootPageId = btrSmgr->GetRootPageIdFromMetaCache();
        if (btrRootPageId != lastRootPageId) {
            rootLevel = btrSmgr->GetRootLevelFromMetaCache();
            if (rootLevel == 0) {
                leafPageIds.push(btrRootPageId);
            } else if (rootLevel == 1) {
                pivotPageIds.push(btrRootPageId);
                leafPageIds = TestPivotTuple(indexTupleDesc, pivotPageIds, 1);
            } else if (rootLevel == 2) {
                /* When btree has 3 levels, insert remainNum tuples finally */
                pivotPageIds.push(btrRootPageId);
                pivotPageIds = TestPivotTuple(indexTupleDesc, pivotPageIds, 2);
                leafPageIds = TestPivotTuple(indexTupleDesc, pivotPageIds, 1);
                isThreeLevel = true;
            } else {
                StorageAssert(0);
            }
            /* Test the inserted tuple position */
            bool found = IsTupleInExpectPosition(indexTupleDesc, leafPageIds, position, indexTuple);
            EXPECT_TRUE(found);
            lastRootPageId = btrRootPageId;
        }

        if (isThreeLevel && remainNum-- == 0) {
            break;
        }
        insertNum++;
        DstorePfreeExt(indexTuple);
    }
    txn->Commit();

    /* Insert some random tuples and check order */
    int randomNum = 10;
    for (int i = 0; i < randomNum; ++i) {
        m_utTableHandler->InsertRandomIndexTuple();
    }
    std::queue<PageId> pivotPageIds;
    pivotPageIds.push(btrRootPageId);
    pivotPageIds = TestPivotTuple(indexTupleDesc, pivotPageIds, 2);
    std::queue<PageId> leafPageIds = TestPivotTuple(indexTupleDesc, pivotPageIds, 1);
    TestLeafTuple(indexTupleDesc, leafPageIds, rowNum + insertNum + randomNum);
}

TEST_F(UTTempTablespace, HeapScanBigTupleTest)
{
    HeapTuple *resTuple;
    Transaction *transaction = thrd->GetActiveTransaction();
    std::unique_ptr<StorageRelationData> heapRel(new StorageRelationData());
    heapRel->tableSmgr = m_utTableHandler->GetHeapTabSmgr();
    heapRel->m_pdbId = g_defaultPdbId;
    HeapScanHandler *heapScan = DstoreNew(thrd->GetTransactionMemoryContext()) HeapScanHandler(
        g_storageInstance, thrd, heapRel.get());
    /* Construct big tup. */
    int BIG_TUP_LEN = 18000;
    std::string bigString(BIG_TUP_LEN, 'b');

    /* Case 1: insert and scan */
    transaction->Start();
    transaction->SetSnapshotCsn();
    HeapTuple *tup = m_utTableHandler->GenerateSpecificHeapTuple(bigString);
    m_utTableHandler->InsertHeapTupAndCheckResult(tup, true, transaction->GetSnapshotData());
    InsertSpecificHeapTuples("hello", transaction->GetSnapshotData(), true);
    int cnts[2] = {0};
    /* Construct once used cr page and read it */
    heapScan->Begin(transaction->GetSnapshotData());
    while ((resTuple = heapScan->SeqScan()) != nullptr) {
        if (strncmp(resTuple->m_diskTuple->GetData(), bigString.c_str(), BIG_TUP_LEN) == 0) {
            cnts[0]++;
        } else if (strncmp(resTuple->m_diskTuple->GetData(), "hello", strlen("hello")) == 0) {
            cnts[1]++;
        }
    }
    EXPECT_EQ(cnts[0], 1);
    EXPECT_EQ(cnts[1], 1);
    heapScan->End();
    delete heapScan;
    transaction->Commit();
}

TEST_F(UTTempTablespace, BtreeCreateThreeLevelTest)
{
    int rowNum = 10000;
    m_utTableHandler->FillTableWithRandomData(rowNum);

    int indexCols[] = {1, 2, 3, 4};
    bool isUnique = false;
    int numAttrs = sizeof(indexCols) / sizeof(int);
    m_utTableHandler->CreateIndex(indexCols, numAttrs, isUnique, BTREE_MIN_FILLFACTOR);

    TupleDesc indexTupleDesc = m_utTableHandler->GetIndexTupleDesc();

    /* Test btree meta page info */
    PageId btrRootPageId = TestBtreeMetaPages(m_utTableHandler->GetBtreeSmgr()->GetBtrMetaPageId());

    /* Test btree root page info */
    TestBtreeRootPages(btrRootPageId, 2);

    /* Test tuples in root page */
    std::queue<PageId> rootPageId;
    rootPageId.push(btrRootPageId);
    std::queue<PageId> pivotPageIds = TestPivotTuple(indexTupleDesc, rootPageId, 2);

    /* Test all pivot pages */
    std::queue<PageId> leafPageIds = TestPivotTuple(indexTupleDesc, pivotPageIds, 1);

    /* Test all leaf pages */
    TestLeafTuple(indexTupleDesc, leafPageIds, rowNum);
}

TEST_F(UTTempTablespace, BtreeDeleteTest)
{
    /* Fill table with specific data */
    int rowNum = 350;
    DefaultRowDef *tableDef = (DefaultRowDef *)DstorePalloc(rowNum * sizeof(DefaultRowDef));
    for (int i = 0; i < rowNum; i++) {
        int16 r1 = (int16)i + 1;
        int32 r2 = (int32)i + 1;
        DefaultRowDef rowDef = m_utTableHandler->GetDefaultRowDef();
        rowDef.column_int16 = r1;
        rowDef.column_int32 = r2;
        tableDef[i] = rowDef;
    }
    m_utTableHandler->FillTableWithSpecificData(tableDef, rowNum);
    DstorePfree(tableDef);

    /* Build btree index. */
    int indexCols[] = {1, 2};
    bool isUnique = false;
    int numAttrs = sizeof(indexCols) / sizeof(int);
    m_utTableHandler->CreateIndex(indexCols, numAttrs, isUnique);
    TupleDesc indexTupleDesc = m_utTableHandler->GetIndexTupleDesc();

    /* Test btree meta page info */
    PageId btrRootPageId = TestBtreeMetaPages(m_utTableHandler->GetBtreeSmgr()->GetBtrMetaPageId());
    TestBtreeRootPages(btrRootPageId, 1);

    Transaction *txn = thrd->GetActiveTransaction();
    txn->Start();
    txn->SetSnapshotCsn();
    SnapshotData snapshot = *txn->GetSnapshotData();
    txn->Commit();

    /* Insert and delete until all tds are shared */
    DefaultRowDef insertRow = m_utTableHandler->GetDefaultRowDef();
    insertRow.column_int16 = -1;
    insertRow.column_int32 = 1;

    IndexTuple *indexTuple0 = InsertSpecificIndexTuples(&insertRow);
    EXPECT_EQ(DeleteIndexTuples(indexTuple0), DSTORE_SUCC);

    insertRow.column_int16 = 0;
    insertRow.column_int32 = 1;

    IndexTuple *indexTuple1 = InsertSpecificIndexTuples(&insertRow);
    IndexTuple *indexTuple2 = InsertSpecificIndexTuples(&insertRow);
    EXPECT_EQ(DeleteIndexTuples(indexTuple1), DSTORE_SUCC);
    EXPECT_EQ(DeleteIndexTuples(indexTuple2), DSTORE_SUCC);

    /* delete visibility check */
    IndexTuple *indexTuple = InsertSpecificIndexTuples(&insertRow);

    Transaction *txnA = thrd->GetActiveTransaction();
    txnA->Start();
    txnA->SetSnapshotCsn();
    EXPECT_EQ(DeleteIndexTuples(indexTuple, true), DSTORE_SUCC);

    Transaction *txnB = thrd->GetActiveTransaction();
    txnB->Start();
    txnB->SetSnapshotCsnForFlashback(snapshot.GetCsn());
    EXPECT_EQ(DeleteIndexTuples(indexTuple, true), DSTORE_FAIL);

    txnB->Commit();
    txnA->Commit();

    std::queue<PageId> pivotPageIds, leafPageIds;
    pivotPageIds.push(btrRootPageId);
    leafPageIds = TestPivotTuple(indexTupleDesc, pivotPageIds, 1);
    EXPECT_TRUE(CheckTupleDeleted(indexTupleDesc, leafPageIds, 1, indexTuple0));
}

TEST_F(UTTempTablespace, DropSegmentAndReuseTest1)
{
    IndexTempSegment *indexSegment =
        (IndexTempSegment*)SegmentTest::UTAllocSegment(g_defaultPdbId, m_utTableHandler->m_tablespaceId, thrd->GetTmpLocalBufMgr(), SegmentType::INDEX_TEMP_SEGMENT_TYPE);
    ASSERT_NE(indexSegment, nullptr);
    indexSegment->DropSegment();
    delete indexSegment;

    HeapTempSegment *heapSegment =
        (HeapTempSegment*)SegmentTest::UTAllocSegment(g_defaultPdbId, m_utTableHandler->m_tablespaceId, thrd->GetTmpLocalBufMgr(), SegmentType::HEAP_TEMP_SEGMENT_TYPE);
    ASSERT_NE(heapSegment, nullptr);
    PageId newPage1 = heapSegment->GetNewPage();
    ASSERT_NE(newPage1, INVALID_PAGE_ID);
    heapSegment->DropSegment();
    delete heapSegment;
}

/* loop do {AllocSegment getNewpage DropSegment} */
TEST_F(UTTempTablespace, DropSegmentAndReuseTest2)
{
    IndexTempSegment *indexSegment =
        (IndexTempSegment*)SegmentTest::UTAllocSegment(g_defaultPdbId, m_utTableHandler->m_tablespaceId, thrd->GetTmpLocalBufMgr(), SegmentType::INDEX_TEMP_SEGMENT_TYPE);
    ASSERT_NE(indexSegment, nullptr);
    indexSegment->DropSegment();
    delete indexSegment;

    indexSegment =
        (IndexTempSegment*)SegmentTest::UTAllocSegment(g_defaultPdbId, m_utTableHandler->m_tablespaceId, thrd->GetTmpLocalBufMgr(), SegmentType::INDEX_TEMP_SEGMENT_TYPE);
    ASSERT_NE(indexSegment, nullptr);
    PageId newPage3 = indexSegment->GetNewPage();
    ASSERT_NE(newPage3, INVALID_PAGE_ID);
    indexSegment->DropSegment();
    delete indexSegment;

    indexSegment =
        (IndexTempSegment*)SegmentTest::UTAllocSegment(g_defaultPdbId, m_utTableHandler->m_tablespaceId, thrd->GetTmpLocalBufMgr(), SegmentType::INDEX_TEMP_SEGMENT_TYPE);
    ASSERT_NE(indexSegment, nullptr);
    PageId newPage4 = indexSegment->GetNewPage();
    ASSERT_NE(newPage4, INVALID_PAGE_ID);
    indexSegment->DropSegment();
    delete indexSegment;

    HeapTempSegment *heapSegment =
        (HeapTempSegment*)SegmentTest::UTAllocSegment(g_defaultPdbId, m_utTableHandler->m_tablespaceId, thrd->GetTmpLocalBufMgr(), SegmentType::HEAP_TEMP_SEGMENT_TYPE);
    ASSERT_NE(heapSegment, nullptr);
    PageId newPage1 = heapSegment->GetNewPage();
    ASSERT_NE(newPage1, INVALID_PAGE_ID);
    heapSegment->DropSegment();
    delete heapSegment;

    heapSegment =
        (HeapTempSegment*)SegmentTest::UTAllocSegment(g_defaultPdbId, m_utTableHandler->m_tablespaceId, thrd->GetTmpLocalBufMgr(), SegmentType::HEAP_TEMP_SEGMENT_TYPE);
    ASSERT_NE(heapSegment, nullptr);
    heapSegment->DropSegment();
    delete heapSegment;

    heapSegment =
        (HeapTempSegment*)SegmentTest::UTAllocSegment(g_defaultPdbId, m_utTableHandler->m_tablespaceId, thrd->GetTmpLocalBufMgr(), SegmentType::HEAP_TEMP_SEGMENT_TYPE);
    ASSERT_NE(heapSegment, nullptr);
    PageId newPage2 = heapSegment->GetNewPage();
    ASSERT_NE(newPage2, INVALID_PAGE_ID);
    heapSegment->DropSegment();
    delete heapSegment;
}

