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
 *
 * ---------------------------------------------------------------------------------------
 *
 * dstore_btree_build.h
 *
 * IDENTIFICATION
 *        dstore/include/index/dstore_btree_build.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_GAUSSKERNEL_INCLUDE_INDEX_DSTORE_BTREE_BUILD_H
#define SRC_GAUSSKERNEL_INCLUDE_INDEX_DSTORE_BTREE_BUILD_H

#include "tuple/dstore_index_tuple.h"
#include "common/algorithm/dstore_tuplesort.h"
#include "index/dstore_btree.h"
#include "page/dstore_index_page.h"
#include "index/dstore_btree_build_parallel.h"


namespace DSTORE {

struct BtreePageLevelBuilder {
    uint32       level;                        /* Level in btree */
    uint32       pageFreeSpaceLimit;           /* Limitation of the minimal free space a page should reserve */
    OffsetNumber lastItemOffset;               /* Position on current writing page that where last item added to */
    BufferDesc  *currPageBuf;                  /* Current writing page's buffer descriptor of level */
    IndexTuple  *lastPageHikey;                /* A copy last written page's High key in memory */
    BtreePageLevelBuilder *higherLevelBuilder; /* A linker to parent level */

    inline RetStatus Init(uint32 currLevel, int fillFactor)
    {
        level = currLevel;
        pageFreeSpaceLimit = static_cast<uint32>(BLCKSZ - (BLCKSZ * fillFactor / PERCENTAGE_DIVIDER));
        lastItemOffset = BTREE_PAGE_HIKEY;
        currPageBuf = INVALID_BUFFER_DESC;
        lastPageHikey = IndexTuple::CreateMinusinfPivotTuple();
        if (STORAGE_VAR_NULL(lastPageHikey)) {
            return DSTORE_FAIL;
        }
        higherLevelBuilder = nullptr;
        return DSTORE_SUCC;
    }
};

class BtreeBuild : public Btree {
public:
    BtreeBuild(StorageRelation indexRel, IndexBuildInfo *indexBuildInfo, ScanKey scanKey);
    /*
     * BuildIndex
     *
     * Interface for outer caller.
     * Build a B-Tree for index using given table and index options in storage.
     */
    RetStatus BuildIndex();
    /**
     * Build Index in Parallel Mode
    */
    RetStatus BuildIndexParallel(int parallelWorkers);

    inline IndexBuildInfo* GetIndexBuildInfo()
    {
        return m_indexBuildInfo;
    }

    PdbId GetPdbId()
    {
        return m_indexRel->m_pdbId;
    }

    static void* ExprInit(CallbackFunc exprCallback);
    static RetStatus GetExprValue(CallbackFunc exprCallback, HeapTuple *heapTuple, Datum *values, bool *isNulls,
                                  void *exprCxt);

protected:
    RetStatus ScanTableForGlobalIndexBuild();
    RetStatus ScanTableForLocalOrNonPartitionIndexBuild();

    RetStatus ScanTableForGlobalIndexBuildParallel(ParallelBtreeBuild *parallelBuilder);
    RetStatus ScanTableForLocalOrNonPartitionIndexBuildParallel(ParallelBtreeBuild *parallelBuilder);
    RetStatus BuildLpiParallelCrossPartition(ParallelBtreeBuild *parallelBuilder);
    /*
     * CollectTuplesFromTable
     *
     * 1. Scan table and collect heap tuples.
     * 2. Form index tuple by heap tuples according to index information
     * 3. Add index tuple to TuplesortMgr for lator sorting.
     */
    RetStatus CollectTuplesFromTable(StorageRelation heapRel, Oid tableOid, Datum *values,
                                     bool *isNulls, double &numHeapTuples);
    /*
     * WriteSortedTuples
     *
     * Read all sorted index tuples from TuplesortMgr one by one and write them onto index pages in order.
     */
    RetStatus WriteSortedTuples();
    /**
     * WriteSortedTuplesParallel
     *
     * Read sorted tuples from worker thread's TuplesortMgr and use internal heap
     * to write them into index pages in order
    */
    RetStatus WriteSortedTuplesParallel();
    /*
     * AddTupleToPage
     *
     * Add a single index tuple to index page.
     */
    RetStatus AddTupleToPage(IndexTuple *indexTuple, BtreePageLevelBuilder *currLevel);

    /*
     * CreatePageLevelBuilder
     *
     * We use BtreePageLevelBuilder to help building the structure of B-Tree. Create a new BtreePageLevelBuilder
     * for pages in the new level when higheit of btree grows.
     */
    RetStatus CreatePageLevelBuilder(BtreePageLevelBuilder *childLevel, BtreePageLevelBuilder **newLevel);
    /*
     * AddNewPageToLevelBuilder
     *
     * Add a new page to level when current writing page in BtreePageLevelBuilder is full, or when a new
     * BtreePageLevelBuilder is created.
     */
    RetStatus AddNewPageToLevelBuilder(BtreePageLevelBuilder *currLevel);

    RetStatus SplitPage(BtreePageLevelBuilder *currLevel);
    RetStatus AddPageDownlinkToParent(BtreePageLevelBuilder *currLevel);
    RetStatus CreateAndWriteHikey(BtreePageLevelBuilder *currLevel);

    void WritePage(BufferDesc *pageBuf);

    /*
     * CompleteRightmostPages
     *
     * Finish B-Tree index building by completing rightmost pages writing for each level.
     */
    RetStatus CompleteRightmostPages(PageId &rootPageId, uint32 &rootLevel);
    /*
     * RemoveHikeySpaceOnRightmostPage
     *
     * During building process, we always reserve the first ItemId for high key, which would be the last ItemId
     * to be written on the page, avoiding to move ItemIds for extra space of high key when finish writing a page.
     * This function is used for the reserved space removing on the rightmost page since high key is not nessassery
     * for rightmost pages.
     */
    RetStatus RemoveHikeySpaceOnRightmostPage(BtreePageLevelBuilder *currLevel);

    RetStatus CreateBtreeMeta(const PageId rootPageId, uint32 rootLevel);

    void HandleErrAndClear();

    void CopyDuplicateTupleFromTupleSortMgr();

    void GenerateWriteBtreePageWal(BufferDesc *btrPageBuf);
    void GenerateInitMetaPageWal(BufferDesc *btrMetaBuf);

    IndexBuildInfo          *m_indexBuildInfo;
    TuplesortMgr            *m_tuplesortMgr;
    BtreePageLevelBuilder   *m_leafLevel;

    uint64 m_splitCount[BTREE_HIGHEST_LEVEL];
    uint16 m_nParallelWorkers = 0;
};
}

#endif /* SRC_GAUSSKERNEL_INCLUDE_INDEX_STORAGE_BTREE_BUILD_H */
