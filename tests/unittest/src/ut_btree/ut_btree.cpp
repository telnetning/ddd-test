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
#include "ut_btree/ut_btree.h"
#include "heap/dstore_heap_delete.h"
#include "common/datatype/dstore_compare_utils.h"
#include "index/dstore_btree_build.h"
#include "diagnose/dstore_index_diagnose.h"
#include "errorcode/dstore_index_error_code.h"
#include "errorcode/dstore_page_error_code.h"
#include "index/dstore_index_interface.h"
#include "systable/dstore_relation.h"

/*
 * If tuple1 < tuple2, return -1.
 * If tuple1 > tuple2, return 1.
 * If tuple1 = tuple2, return 0.
 * Note: for same level in btree, two equaled tuples(key + ctid) are not exist.
 */
int UTBtree::CompareIndexTuple(IndexTuple *tuple1, IndexTuple *tuple2, TupleDesc indexTupleDesc)
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

void UTBtree::CreateIndexBySpecificData(int keyAttrNum, int **indexCols)
{
    /* Fill table with specific data */
    DefaultRowDef *tableDef = (DefaultRowDef*)DstorePalloc(NUM_PRESET_ROWS * sizeof(DefaultRowDef));

    /* Insert (2,2)(4,4)...(1998,1998)(2000,2000) */
    for (int i = 1; i <= NUM_PRESET_ROWS; ++i) {
        int16 r1 = (int16)(i * 2);
        int32 r2 = (int32)(i * 2);
        DefaultRowDef insertRow = m_utTableHandler->GetDefaultRowDef();
        insertRow.column_int16 = r1;
        insertRow.column_int32 = r2;
        tableDef[i - 1] = insertRow;
    }
    m_utTableHandler->FillTableWithSpecificData(tableDef, NUM_PRESET_ROWS);
    DstorePfree(tableDef);

    /* Set btree index meta */
    int *cols = (int *)DstoreMemoryContextAlloc(g_storageInstance->GetMemoryMgr()->
        GetGroupContext(MemoryGroupType::MEMORY_CONTEXT_LONGLIVE), sizeof(int) * keyAttrNum);
    for (int i = 0; i < keyAttrNum; i++) {
        cols[i] = i + 1;
    }

    /* Build btree index */
    bool isUnique = true;
    m_utTableHandler->CreateIndex(cols, keyAttrNum, isUnique);
    if (indexCols != nullptr) {
        *indexCols = cols;
    }
}

IndexTuple *UTBtree::InsertSpecificIndexTuple(DefaultRowDef *insertRow, bool *nullbitmap, bool alreadyXactStart,
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
    HeapInsertHandler heapInsert(g_storageInstance, thrd, tableHandler->GetTableRel());
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
    tableHandler->m_btreeTestContext->lastWorkingPageId = btreeInsert.m_curPageId;
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

/* delete index tuple from btree and delete corresponding heap tuple from heap. */
RetStatus UTBtree::DeleteIndexTuple(IndexTuple *indexTuple, bool alreadyXactStart,
                                    UTTableHandler *utTableHandler)
{
    if (utTableHandler != nullptr) {
        return utTableHandler->DeleteWithIndexTuple(indexTuple, alreadyXactStart);
    }
    return m_utTableHandler->DeleteWithIndexTuple(indexTuple, alreadyXactStart);
}

IndexTuple *UTBtree::InsertIndexTupleOnly(DefaultRowDef *insertRow, bool *nullbitmap, ItemPointerData fakeHeapCtid)
{
    /* Get heap tuple */
    TupleDesc heapTupleDesc = m_utTableHandler->GetHeapTupDesc();
    Datum heapValues[heapTupleDesc->natts];
    bool heapIsnulls[heapTupleDesc->natts];
    HeapTuple *heapTuple = m_utTableHandler->GetSpecificHeapTuple(insertRow, heapValues, heapIsnulls, nullbitmap);

    /* Get index value */
    TupleDesc indexTupleDesc = m_utTableHandler->GetIndexTupleDesc();
    int16 *indexAttrOffsets = (int16 *)(&m_utTableHandler->GetIndexBuildInfo()->indexAttrOffset);
    Datum indexValues[indexTupleDesc->natts];
    bool indexIsnulls[indexTupleDesc->natts];
    for (int16 i = 0; i < indexTupleDesc->natts; i++) {
        int keyIdx = indexAttrOffsets[i] - 1;
        indexValues[i] = heapValues[keyIdx];
        indexIsnulls[i] = heapIsnulls[keyIdx];
    }
    if (fakeHeapCtid == INVALID_ITEM_POINTER) {
        fakeHeapCtid = {{1, 1}, 1};
    }

    /* Index insert */
    BtreeInsert btreeInsert(m_utTableHandler->GetIndexRel(), m_utTableHandler->GetIndexInfo(),
                            m_utTableHandler->GetIndexScanKey());
    RetStatus status = btreeInsert.InsertTuple(indexValues, indexIsnulls, &fakeHeapCtid);
    m_utTableHandler->m_btreeTestContext->lastWorkingPageId = btreeInsert.m_curPageId;
    thrd->GetActiveTransaction()->IncreaseCommandCounter();

    /* Get index tuple and return */
    IndexTuple *indexTuple = nullptr;
    if (STORAGE_FUNC_SUCC(status)) {
        indexTuple = IndexTuple::FormTuple(indexTupleDesc, indexValues, indexIsnulls);
        indexTuple->SetHeapCtid(&fakeHeapCtid);
    }

    return indexTuple;
}

RetStatus UTBtree::DeleteIndexTupleOnly(IndexTuple *indexTuple)
{
    IndexInfo *indexInfo = m_utTableHandler->GetIndexInfo();
    Datum values[indexInfo->indexKeyAttrsNum];
    bool isNulls[indexInfo->indexKeyAttrsNum];
    indexTuple->DeformTuple(indexInfo->attributes, values, isNulls);
    ItemPointerData heapCtid = indexTuple->GetHeapCtid();

    /* Index delete */
    BtreeDelete btreeDelete(m_utTableHandler->GetIndexRel(), m_utTableHandler->GetIndexInfo(),
                            m_utTableHandler->GetIndexScanKey());
    RetStatus status = btreeDelete.DeleteTuple(values, isNulls, &heapCtid);
    m_utTableHandler->m_btreeTestContext->lastWorkingPageId = btreeDelete.GetCurPageId();
    thrd->GetActiveTransaction()->IncreaseCommandCounter();
    return status;
}

PageId UTBtree::GetLeftmostLeaf(UTTableHandler *utTableHandler)
{
    BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
    UTTableHandler *tableHandler = utTableHandler == nullptr ? m_utTableHandler : utTableHandler;
    BufferDesc *pageBuf = INVALID_BUFFER_DESC;
    BtrMeta *meta = tableHandler->GetBtreeSmgr()->GetBtrMeta(DSTORE::LW_SHARED, &pageBuf);
    PageId pageId = meta->GetLowestSinglePage();
    uint32 level = meta->GetLowestSinglePageLevel();
    bufMgr->UnlockAndRelease(pageBuf);

    while (level != 0U) {
        pageBuf = bufMgr->Read(g_defaultPdbId, pageId, LW_SHARED);
        StorageAssert(pageBuf != INVALID_BUFFER_DESC);
        BtrPage *page = (BtrPage *)pageBuf->GetPage();
        BtrPageLinkAndStatus *linkStat = page->GetLinkAndStatus();
        StorageAssert(linkStat->GetLevel() == level);
        pageId = page->GetIndexTuple(linkStat->GetFirstDataOffset())->GetLowlevelIndexpageLink();
        level--;
        bufMgr->UnlockAndRelease(pageBuf);
    }

    return pageId;
}

std::queue<PageId> UTBtree::TestPivotTuples(TupleDesc indexTupleDesc, std::queue<PageId> &pivotPageIds, int level)
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
        BufferDesc *buf = g_storageInstance->GetBufferMgr()->
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
                int cmp = CompareIndexTuple(otuple, tuple, indexTupleDesc);
                EXPECT_EQ(cmp, -1);
            }
            otuple = tuple;
        }
        if (!pivotPageIds.empty()) {
            int cmp = CompareIndexTuple(otuple, highKeyTuple, indexTupleDesc);
            EXPECT_EQ(cmp, -1);
        }

        g_storageInstance->GetBufferMgr()->UnlockAndRelease(buf);
        lastPageId = curPageId;
        firstTupleInOnePage = true;
    }

    return pageIds;
}

/*
 * Test page in level 0, all tuples should be ordered ascendingly.
 */
void UTBtree::TestLeafTuples(TupleDesc indexTupleDesc, std::queue<PageId> &leafPageIds, int rowNum)
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
        BufferDesc *buf = g_storageInstance->GetBufferMgr()->Read(g_defaultPdbId, curPageId, DSTORE::LW_SHARED);
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
                int cmp = CompareIndexTuple(otuple, tuple, indexTupleDesc);
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
            int cmp = CompareIndexTuple(otuple, highKeyTuple, indexTupleDesc);
            EXPECT_EQ(cmp, -1);
        }

        g_storageInstance->GetBufferMgr()->UnlockAndRelease(buf);
        tupleNumInLevel0 += (limit - startOffset + 1);
        lastPageId = curPageId;
    }
    EXPECT_EQ(tupleNumInLevel0, rowNum);
    DstorePfreeExt(prevPageLastTuple);
}

/* Test every tuple which is null or not null in specific order,
 * only used in BtreeCreateNullKeyTest and BtreeInsertNullKeyTest */
void UTBtree::TestNullLeafTuplesInSpecificOrder(TupleDesc indexTupleDesc, std::queue<PageId> &leafPageIds, int rowNum)
{
    int tupleNumInLevel0 = 0;
    OffsetNumber startOffset = BTREE_PAGE_FIRSTKEY;
    Datum datum1, datum2;
    bool isnull1, isnull2;
    while (!leafPageIds.empty()) {
        PageId curPageId = leafPageIds.front();
        leafPageIds.pop();
        BufferDesc *buf = g_storageInstance->GetBufferMgr()->Read(g_defaultPdbId, curPageId, DSTORE::LW_SHARED);
        BtrPage *leafPage = (BtrPage *)buf->GetPage();
        if (leafPageIds.empty()) {
            startOffset = BTREE_PAGE_HIKEY;
        }
        OffsetNumber limit = leafPage->GetMaxOffset();
        for (OffsetNumber n = startOffset; n <= limit; ++n) {
            tupleNumInLevel0++;
            IndexTuple *tuple = (IndexTuple *)leafPage->GetRowData(n);
            datum1 = tuple->GetAttr(1, indexTupleDesc, &isnull1);
            datum2 = tuple->GetAttr(2, indexTupleDesc, &isnull2);
            if (tupleNumInLevel0 <= rowNum / 2) {
                /* (i,i)(i,null) */
                EXPECT_EQ(datum1, (tupleNumInLevel0 + 1) / 2);
                EXPECT_FALSE(isnull1);
                if (tupleNumInLevel0 % 2) {
                    EXPECT_EQ(datum2, (tupleNumInLevel0 + 1) / 2);
                    EXPECT_FALSE(isnull2);
                } else {
                    EXPECT_EQ(datum2, 0);
                    EXPECT_TRUE(isnull2);
                }
            } else if (tupleNumInLevel0 <= rowNum / 2 + rowNum / 4) {
                /* (null,i) */
                EXPECT_EQ(datum1, 0);
                EXPECT_TRUE(isnull1);
                EXPECT_EQ(datum2, tupleNumInLevel0 - rowNum / 2);
                EXPECT_FALSE(isnull2);
            } else {
                /* (null,null) */
                EXPECT_EQ(datum1, 0);
                EXPECT_TRUE(isnull1);
                EXPECT_EQ(datum2, 0);
                EXPECT_TRUE(isnull2);
            }
        }
        g_storageInstance->GetBufferMgr()->UnlockAndRelease(buf);
    }
}

/*
 * Test whether the specific tuple located in right position.
 * position means the offset of all leaf pages other than one page.
 */
bool UTBtree::IsTupleInRightPosition(TupleDesc indexTupleDesc, std::queue<PageId> &leafPageIds,
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
        BufferDesc *buf = g_storageInstance->GetBufferMgr()->Read(g_defaultPdbId, curPageId, DSTORE::LW_SHARED);
        BtrPage *leafPage = (BtrPage*)buf->GetPage();
        OffsetNumber limit = leafPage->GetMaxOffset();
        if (num + limit - startOffset + 1 < position) {
            /* If the position isn't located in this page, skip it quickly */
            num += (limit - startOffset + 1);
            g_storageInstance->GetBufferMgr()->UnlockAndRelease(buf);
            continue;
        }
        for (OffsetNumber n = startOffset; n <= limit; ++n) {
            IndexTuple *tuple = (IndexTuple*)leafPage->GetRowData(n);
            num++;
            if (num == position) {
                int cmp = CompareIndexTuple(target, tuple, indexTupleDesc);
                EXPECT_EQ(cmp, 0);
                g_storageInstance->GetBufferMgr()->UnlockAndRelease(buf);
                return true;
            }
        }
    }
    /* not found */
    return false;
}

/* Test whether the specific tuple is deleted. */
bool UTBtree::IsTupleDeleted(TupleDesc indexTupleDesc, std::queue<PageId> &leafPageIds,
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
        BufferDesc *buf = g_storageInstance->GetBufferMgr()->Read(g_defaultPdbId, curPageId, DSTORE::LW_SHARED);
        BtrPage *leafPage = (BtrPage*)buf->GetPage();
        OffsetNumber limit = leafPage->GetMaxOffset();
        for (OffsetNumber n = startOffset; n <= limit; ++n) {
            num++;
            if (num == offset) {
                IndexTuple *tuple = (IndexTuple*)leafPage->GetRowData(n);
                int cmp = CompareIndexTuple(target, tuple, indexTupleDesc);
                EXPECT_EQ(cmp, 0);
                bool isDeleted = tuple->IsDeleted();
                g_storageInstance->GetBufferMgr()->UnlockAndRelease(buf);
                return isDeleted;
            }
        }
        g_storageInstance->GetBufferMgr()->UnlockAndRelease(buf);
    }
    /* not found */
    return false;
}

PageId UTBtree::TestBtreeMetaPage(PageId metaPageId)
{
    PageId segmentMetaPageId = m_utTableHandler->GetBtreeSmgr()->GetSegment()->GetSegmentMetaPageId();
    PageId FistDataPageId = {segmentMetaPageId.m_fileId, segmentMetaPageId.m_blockId + 1};
    EXPECT_EQ(metaPageId, FistDataPageId);
    BufferDesc *btrMetaBuf = g_storageInstance->GetBufferMgr()->Read(g_defaultPdbId, metaPageId, DSTORE::LW_SHARED);
    BtrPage *btrMetaPage = (BtrPage*)btrMetaBuf->GetPage();
    BtrMeta *btrMeta = static_cast<BtrMeta *>(static_cast<void *>(btrMetaPage->GetData()));
    PageId btrRootPageId = btrMeta->GetLowestSinglePage();
    EXPECT_NE(btrRootPageId, INVALID_PAGE_ID);
    if (g_traceSwitch == BTREE_STATISTIC_INFO_MIN_TRACE_LEVEL) {
        EXPECT_EQ(btrMeta->initializedtMagicNum, BTREE_META_STAT_INIT_MAGIC_NUM);
    }
    g_storageInstance->GetBufferMgr()->UnlockAndRelease(btrMetaBuf);
    return btrRootPageId;
}

void UTBtree::TestBtreeRootPage(PageId rootPageId, uint32 rootLevel)
{
    BufferDesc *rootBuf = g_storageInstance->GetBufferMgr()->Read(g_defaultPdbId, rootPageId, DSTORE::LW_SHARED);
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
    g_storageInstance->GetBufferMgr()->UnlockAndRelease(rootBuf);
}

TEST_F(UTBtree, BtreePageTest_level0)
{
    int rowNum = 500;
    m_utTableHandler->FillTableWithRandomData(rowNum);

    int indexCols[] = {1, 2, 3, 4};
    bool isUnique = false;
    int numAttrs = sizeof(indexCols) / sizeof(int);
    m_utTableHandler->CreateIndex(indexCols, numAttrs, isUnique);

    BufferDesc *metaBuf = INVALID_BUFFER_DESC;
    BtrMeta *btrMeta = m_utTableHandler->GetBtreeSmgr()->GetBtrMeta(DSTORE::LW_SHARED, &metaBuf);
    BtrPage *metaPage = static_cast<BtrPage *>(metaBuf->GetPage());

    char *meta = metaPage->Dump(metaPage);
    EXPECT_NE(meta, nullptr);

    BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
    PageId rootPageId = btrMeta->GetRootPageId();
    BufferDesc *rootBuf = bufMgr->Read(g_defaultPdbId, rootPageId, LW_SHARED);
    BtrPage *rootPage = static_cast<BtrPage *>(rootBuf->GetPage());

    char *root = rootPage->Dump(metaPage);
    EXPECT_NE(root, nullptr);

    PageId leafPageId = rootPage->GetIndexTuple(rootPage->GetMaxOffset())->GetLowlevelIndexpageLink();
    BufferDesc *leafBuf = bufMgr->Read(g_defaultPdbId, leafPageId, LW_SHARED);
    BtrPage *leafPage = static_cast<BtrPage *>(leafBuf->GetPage());

    char *leaf = leafPage->Dump(metaPage);
    EXPECT_NE(leaf, nullptr);

    bufMgr->UnlockAndRelease(leafBuf);
    bufMgr->UnlockAndRelease(rootBuf);
    bufMgr->UnlockAndRelease(metaBuf);
}

TEST_F(UTBtree, BtreeInterfaceTest_level0)
{
    RetStatus ret;
    int rowNum = 500;
    m_utTableHandler->FillTableWithRandomData(rowNum);

    int indexCols[] = {1, 2};
    bool isUnique = false;
    int numKeyAttrs = sizeof(indexCols) / sizeof(int);
    Transaction *transaction = thrd->GetActiveTransaction();
    transaction->Start();
    transaction->GetSnapshot();
    m_utTableHandler->CreateBtreeContext(indexCols, numKeyAttrs, isUnique);

    /* Test Build */
    StorageRelation indexRel = m_utTableHandler->GetIndexRel();
    ret = IndexInterface::Build(indexRel, m_utTableHandler->GetIndexScanKey(), m_utTableHandler->GetIndexBuildInfo());
    EXPECT_EQ(ret, DSTORE::DSTORE_SUCC);
    transaction->Commit();

    /* Test Insert */
    int heapAttrNum = m_utTableHandler->GetHeapTupDesc()->natts;
    int indexKeyAttrNum = m_utTableHandler->GetIndexInfo()->indexKeyAttrsNum;
    Datum values[heapAttrNum];
    bool isNulls[heapAttrNum];
    HeapTuple *heapTuple = m_utTableHandler->GenerateRandomHeapTuple(values, isNulls);
    ItemPointerData heapCtid =
        m_utTableHandler->InsertHeapTupAndCheckResult(heapTuple);

    Datum indexValues[indexKeyAttrNum];
    bool indexIsNulls[indexKeyAttrNum];
    for (int i = 0; i < indexKeyAttrNum; i++) {
        int keyIdx = m_utTableHandler->GetIndexBuildInfo()->indexAttrOffset[i] - 1;
        indexValues[i] = values[keyIdx];
        indexIsNulls[i] = isNulls[keyIdx];
    }

    transaction->Start();
    transaction->SetSnapshotCsn();
    ret = IndexInterface::Insert({indexRel, m_utTableHandler->GetIndexInfo(),
                                  m_utTableHandler->GetIndexScanKey(), indexValues, indexIsNulls, &heapCtid}, false);
    EXPECT_EQ(ret, DSTORE::DSTORE_SUCC);
    transaction->Commit();

    /* Insert duplicate and check unique */
    m_utTableHandler->SetIndexUnique(true);
    transaction->Start();
    transaction->SetSnapshotCsn();
    ret = IndexInterface::Insert({indexRel, m_utTableHandler->GetIndexInfo(),
                                  m_utTableHandler->GetIndexScanKey(), indexValues, indexIsNulls, &heapCtid}, false, true);
    EXPECT_EQ(ret, DSTORE::DSTORE_FAIL);
    transaction->Abort();

    bool satisfiedUniq = true;
    ItemPointerData invalidCtid = INVALID_ITEM_POINTER;
    transaction->Start();
    transaction->SetSnapshotCsn();
    ret = IndexInterface::Insert({indexRel, m_utTableHandler->GetIndexInfo(),
                                  m_utTableHandler->GetIndexScanKey(), indexValues, indexIsNulls, &invalidCtid}, false, true, &satisfiedUniq);
    EXPECT_EQ(ret, DSTORE::DSTORE_FAIL);
    EXPECT_FALSE(satisfiedUniq);
    transaction->Abort();

    /* Test Scan */
    transaction->Start();
    transaction->SetSnapshotCsn();
    IndexScanHandler *indexScanHandler = IndexInterface::ScanBegin(indexRel, m_utTableHandler->GetIndexInfo(),
                                                                   indexKeyAttrNum, 0);
    EXPECT_NE(indexScanHandler, nullptr);
    IndexInterface::IndexScanSetSnapshot(indexScanHandler, transaction->GetSnapshot());
    ScanKeyData scanKey[numKeyAttrs] = {0};
    for (int i = 0; i < indexKeyAttrNum; i++) {
        scanKey[i].skFlags = 0;
        scanKey[i].skAttno = i + 1;
        scanKey[i].skStrategy = SCAN_ORDER_EQUAL;
        scanKey[i].skArgument = indexValues[i];
    }
    m_utTableHandler->GetScanFuncByFnOid(&scanKey[0], 63);
    m_utTableHandler->GetScanFuncByFnOid(&scanKey[1], 65);

    IndexInterface::ScanRescan(indexScanHandler, scanKey);
    bool found;
    bool recheck = false;
    IndexInterface::ScanNext(indexScanHandler, ScanDirection::FORWARD_SCAN_DIRECTION, &found, &recheck);
    ItemPointer fetchedCtid = indexScanHandler->GetResultHeapCtid();
    EXPECT_EQ(heapCtid, *fetchedCtid);
    IndexInterface::ScanEnd(indexScanHandler);

    /* Test IndexOnlyScan */
    indexScanHandler = IndexInterface::ScanBegin(indexRel, m_utTableHandler->GetIndexInfo(),
                                                 indexKeyAttrNum, 0);
    IndexInterface::ScanSetWantItup(indexScanHandler, true);
    EXPECT_NE(indexScanHandler, nullptr);
    IndexInterface::IndexScanSetSnapshot(indexScanHandler, transaction->GetSnapshot());
    IndexInterface::ScanRescan(indexScanHandler, scanKey);
    TupleDesc indexTupDesc = m_utTableHandler->GetIndexTupleDesc();
    IndexTuple *fetchedTuple = IndexInterface::OnlyScanNext(indexScanHandler, ScanDirection::FORWARD_SCAN_DIRECTION,
                                                            &indexTupDesc, &recheck);
    EXPECT_NE(fetchedTuple, nullptr);
    IndexInterface::ScanEnd(indexScanHandler);
    transaction->Commit();

    /* Test Delete */
    transaction->Start();
    transaction->SetSnapshotCsn();
    ret = IndexInterface::Delete({indexRel, m_utTableHandler->GetIndexInfo(),
                                  m_utTableHandler->GetIndexScanKey(), indexValues, indexIsNulls, &heapCtid});
    EXPECT_EQ(ret, DSTORE::DSTORE_SUCC);
    transaction->Commit();

    /* delete non-exist tuple */
    transaction->Start();
    transaction->SetSnapshotCsn();
    ret = IndexInterface::Delete({indexRel, m_utTableHandler->GetIndexInfo(),
                                  m_utTableHandler->GetIndexScanKey(), indexValues, indexIsNulls, &invalidCtid});
    EXPECT_EQ(ret, DSTORE::DSTORE_FAIL);
    transaction->Abort();
}

void UTBtree::GenerateFakeCallback(IndexBuildInfo *info)
{
    info->baseInfo.exprCallback = [info](FunctionCallInfo fcinfo) {
        Datum *values = (Datum *)(DatumGetPointer(fcinfo->prealloc_arg[1]));
        bool *isNulls = (bool *)(DatumGetPointer(fcinfo->prealloc_arg[2]));
        info->baseInfo.indexAttrsNum = 1;
        info->indexAttrOffset[0] = 0;
        for (uint16 i = 0; i < info->baseInfo.indexAttrsNum; i++) {
            values[i] = Int32GetDatum(static_cast<int32>(rand()));
            isNulls[i] = false;
        }
        return DSTORE::DSTORE_SUCC;
    };
}

TEST_F(UTBtree, BtreeExpressionIndexTest_level0)
{
    TupleDesc heapTupleDesc = m_utTableHandler->GetHeapTupDesc();
    /* Fill table with specific data */
    int rowNum = 100;
    HeapTuple *heapTuple;
    bool *nullbitmap = DefaultNullBitMap;
    Datum *heapValues = (Datum*)DstorePalloc(heapTupleDesc->natts * sizeof(Datum));
    bool *heapIsnulls = (bool*)DstorePalloc(heapTupleDesc->natts * sizeof(bool));
    for (int i = 1; i <= rowNum; ++i) {
        DefaultRowDef insertRow = m_utTableHandler->GetDefaultRowDef();
        insertRow.column_int16 = i;
        insertRow.column_int32 = i;
        heapTuple =
            m_utTableHandler->GetSpecificHeapTuple(&insertRow, heapValues, heapIsnulls, nullbitmap);
        m_utTableHandler->InsertHeapTupAndCheckResult(heapTuple);
        DstorePfreeExt(heapTuple);
    }
    DstorePfree(heapValues);
    DstorePfree(heapIsnulls);
    
    int indexCols[] = {1, 2};
    int numKeyAttrs = sizeof(indexCols) / sizeof(int);
    Transaction *transaction = thrd->GetActiveTransaction();
    transaction->Start();
    transaction->SetSnapshotCsn();
    m_utTableHandler->CreateBtreeContext(indexCols, numKeyAttrs, false);
    GenerateFakeCallback(m_utTableHandler->GetIndexBuildInfo());
    /* Test Build */
    StorageRelation indexRel = m_utTableHandler->GetIndexRel();
    RetStatus ret = IndexInterface::Build(indexRel, m_utTableHandler->GetIndexScanKey(),
                                          m_utTableHandler->GetIndexBuildInfo());
    EXPECT_EQ(ret, DSTORE::DSTORE_SUCC);
    transaction->Commit();
}