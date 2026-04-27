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
 * IDENTIFICATION
 *        include/common/algorithm/dstore_tuplesort.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_GAUSSKERNEL_INCLUDE_COMMON_DSTORE_TUPLESORT_H
#define SRC_GAUSSKERNEL_INCLUDE_COMMON_DSTORE_TUPLESORT_H

#include "errorcode/dstore_common_error_code.h"
#include "common/dstore_datatype.h"
#include "common/memory/dstore_mctx.h"
#include "common/algorithm/dstore_logtape.h"
#include "catalog/dstore_fake_relation.h"
#include "catalog/dstore_function.h"
#include "tuple/dstore_tupledesc.h"
#include "tuple/dstore_index_tuple.h"
#include "index/dstore_btree.h"

namespace DSTORE {

struct SortShimExtra {
    FmgrInfo flinfo;                    /* lookup data for comparison function */
    FunctionCallInfoData fcinfo;        /* reusable callinfo structure */
};

struct SortSupportData;
using SortSupport = SortSupportData*;
struct SortSupportData {
    DstoreMemoryContext ssupCxt;        /* Context containing sort info */
    Oid         ssupCollation;          /* Collation to use, or DSTORE_INVALID_OID */
    bool        ssupReverse;            /* descending-order sort */
    bool        ssupNullsFirst;
    AttrNumber  ssupAttno;              /* column number to sort */
    SortShimExtra *ssupExtra;           /* Workspace for opclass functions */

    int         (*comparator) (Datum x, Datum y, SortSupport ssup);
};

struct SortTuple {
    void       *tuple;
    Datum       datum1;                 /* value of first key column */
    bool        isnull1;                /* is first key column NULL */
    int         srcTapeIndex;           /* source tape number */
};

enum class TupSortStatus : uint8 {
    SORT_IN_MEMORY = 0,
    SORT_ON_TAPE,
    SORT_FINAL_MERGE                    /* this is a special case for sort on tape.
                                         * this status is used for reducing read/write on tape. */
};

constexpr int SLAB_SLOT_SIZE = 1024;
union SlabSlot {
    union SlabSlot *nextfree;
    char buffer[SLAB_SLOT_SIZE];
};

/* gs_pdb/ is hard code now, after the Gaussstore is interconnected, we do not hardcode the 'gs_pdb'. */
#define PDB_BASE_PATH "gs_pdb/"
#define INDEX_BASE_PREFIX "idxtmp_"
#define INDEX_SORT_TMP_FILE_BASE_PREFIX PDB_BASE_PATH INDEX_BASE_PREFIX "%u_%u_"
/* 64 is enough for gs_pdb/idxtmp_{pdb_id(uint32)}_{index_oid(uint32)}_p{i(int)}_{seqnum(int)} */
constexpr int MAX_SORT_TMP_FILE_BASE_NAME_LEN = 64;

constexpr int MAX_TUPLE_SORT_ERROR_NUM = 25;
const ErrorCode TUPLE_SORT_ERROR_BLACK_LIST[MAX_TUPLE_SORT_ERROR_NUM] {
    COMMON_ERROR_FUNCTION_RETURN_NULL,
    COMMON_ERROR_MEMORY_ALLOCATION,
    INDEX_ERROR_MEMORY_ALLOC,
    DATATYPE_ERROR_INVALID_DATUM_POINTER,
    DATATYPE_ERROR_INVALID_TYPLEN,
    ARRAY_ERROR_ARRAY_SIZE_EXCEED_LIMIT,
    DATATYPE_ERROR_INDETERMINATE_COLLATION,
    INDEX_ERROR_UNEXPECTED_NULL_VALUE,
    TUPLESORT_ERROR_COULD_NOT_CREATE_UNIQUE_INDEX,
    TUPLESORT_ERROR_MISSING_SUPPORT_FUNCTION_FOR_FUNCOID,
    TUPLESORT_ERROR_MISSING_SUPPORT_FUNCTION_FOR_TYPE,
    TUPLESORT_ERROR_INSUFFICIENT_MEMORY_ALLOWED,
    TUPLESORT_ERROR_UNEXPECTED_OUT_OF_MEMORY,
    TUPLESORT_ERROR_INVALID_TUPLESORT_STATE,
    TUPLESORT_ERROR_BOGUS_TUPLE_LENGTH,
    TUPLESORT_ERROR_TOO_MANY_RUNS_FOR_EXTERNAL_SORT,
    TUPLESORT_ERROR_UNEXPECTED_END_OF_DATA,
    TUPLESORT_ERROR_UNEXPECTED_END_OF_TAPE,
    ATTRIBUTE_ERROR_UNSUPPORTED_BYVAL_LENGTH,
    BUFFILE_ERROR_FAIL_CREATE_TEMP_FILE,
    BUFFILE_ERROR_FAIL_WRITE_TEMP_FILE,
    LOGTAPE_ERROR_COULD_NOT_SEEK_BLOCK,
    LOGTAPE_ERROR_COULD_NOT_SEEK_BLOCK_READONLY,
    LOGTAPE_ERROR_INVALID_LOGTAPE_STATE,
    INDEX_ERROR_FAILED_TO_GET_SORTED_TUPLE
};

class TuplesortMgr : public BaseObject {
public:
    TuplesortMgr(const char *tmpFileNameBase, const PdbId pdbId);

    /* create tuple sort manager for index build */
    static TuplesortMgr* CreateIdxTupleSortMgr(const PdbId pdbId, uint32 indexOid);
    /* create tuple sort manager for index parallel build */
    static TuplesortMgr* CreateParallelIdxTupleSortMgr(const PdbId pdbId, uint32 indexOid, int parallelThrdNum);
    static void DeleteAllIdxSortTmpFile();

    void Destroy();
    void Clear();

    RetStatus PrepareTupleSortInfo(IndexInfo &baseInfo, int workMem, ScanKey scanKeys);
    RetStatus PerformSortTuple();
    RetStatus PutIndexTuple(IndexTuple* indexTuple);
    RetStatus GetNextIndexTuple(IndexTuple** tuple);

    RetStatus PrepareDatumSortInfo(IndexInfo *baseInfo, int workMem, ScanKey scanKey);
    RetStatus PerformSortDatum();
    void PutDatum(Datum datum, bool isNull);
    void GetNextDatum(Datum *datum);

    /* For parallel index build */
    RetStatus PrepareParallelSortWorker(const IndexInfo &baseInfo, int workMem,
                                  ScanKey scanKeys, DstoreMemoryContext cxt);
    RetStatus PrepareParallelSortMainThread(IndexInfo &baseInfo, int workMem,
                                            ScanKey scanKeys, int numWorkers);
    RetStatus Aggregate(TuplesortMgr *tuplesortMgr);
    RetStatus GetNextIndexTupleMainThread(IndexTuple **tuple);
    RetStatus ReadNextTupleFromWorkers();
    RetStatus PerformSortTupleMainThread();

    inline DstoreMemoryContext GetTupleMcxt() const
    {
        return m_tupleContext;
    }
    inline bool UniqueCheckSucc() const
    {
        return m_uniqueCheckSucc;
    }

    inline IndexTuple *GetDuplicateTuple()
    {
        return m_duplicateTuple;
    }

    inline int64 GetUsedMemory()
    {
        return m_allowedMem - m_availMem;
    }

    inline bool IsSortError(ErrorCode errCode) const
    {
        if (likely(errCode == STORAGE_OK)) {
            return false;
        }
        for (int i = 0; i < MAX_TUPLE_SORT_ERROR_NUM; i++) {
            if (errCode == TUPLE_SORT_ERROR_BLACK_LIST[i]) {
                return true;
            }
        }
        return false;
    }

    int CompareSortTuple(const SortTuple *a, const SortTuple *b);

    /* unique check result */
    bool          m_uniqueCheckSucc;
    IndexTuple   *m_duplicateTuple;
    ItemPointerData m_duplicateHeapCtid1;
    ItemPointerData m_duplicateHeapCtid2;

private:
    PdbId         m_pdbId;
    TupSortStatus m_status;
    int           m_nKeys;              /* number of columns in sort key */
    int64         m_availMem;           /* remaining memory available, in bytes */
    int64         m_allowedMem;         /* total memory allowed, in bytes */
    int           m_maxNumOfTapes;      /* max number of input tapes to merge */
    DstoreMemoryContext m_mainContext;  /* memory context for tuple sort metadata that
                                         * persists across multiple batches */
    DstoreMemoryContext m_sortContext;  /* memory context holding most sort data */
    DstoreMemoryContext m_tupleContext; /* sub-context of sortcontext for tuple data */
    LogicalTapeBlockMgr *m_tapeBlockMgr;

    SortTuple    *m_memTupArray;
    int           m_memTupArraySize;
    int           m_memTupCount;        /* number of tuples currently present */
    bool          m_memTupleArrayCanGrow;
    int           m_memTupReadCount;    /* only used if sorted in memory */

    bool          m_slabAllocatorUsed;
    char         *m_slabMemoryHead;     /* beginning of slab memory arena */
    char         *m_slabMemoryTail;     /* end of slab memory arena */
    SlabSlot     *m_slabFreeHead;       /* head of free list */

    void         *m_lastReturnedTuple;

    int           m_currentRun;

    LogicalTape **m_inputTapes;
    int           m_nInputTapes;
    int           m_nInputRuns;

    LogicalTape **m_outputTapes;
    int           m_nOutputTapes;
    int           m_nOutputRuns;

    LogicalTape  *m_destTape;           /* current output tape */

    LogicalTape  *m_resultTape;         /* actual tape of finished output */

    TupleDesc     m_tupDesc;
    SortSupport   m_sortKeys;           /* array of length nKeys */

    bool          m_enforceUnique;      /* complain if we find duplicate tuples */

    TuplesortMgr **m_workerTupleSorts;     /* tuplesortmgrs from worker threads, only used for parallel build */
    int            m_maxAggregate;
    int            m_nAggregatedWorkerTupleSorts;
    int16          m_tableOidAtt;      /* for global index, column no. of table oid */

    char           m_tmpFileNameBase[MAX_SORT_TMP_FILE_BASE_NAME_LEN]; /* base temp file name */

    RetStatus InitMemoryContext();
    RetStatus PrepareSortInfo(int workMem);
    RetStatus GetNextSortTuple(SortTuple *stup, bool &hasNext);
    RetStatus PutSortTuple(SortTuple *tuple);

    RetStatus InitLogicalTapes();
    RetStatus SelectNewTape();
    int GetNumOfTapes(int64 allowedMem) const;

    RetStatus WriteTupleOnTape(LogicalTape *tape, SortTuple *stup);
    RetStatus ReadSortTupleFromTape(SortTuple *stup, LogicalTape *tape, unsigned int len);
    RetStatus ReadNextSortTuple(LogicalTape *srcTape, SortTuple *stup, bool &hasNext);
    RetStatus MarkTapeWriteEnd(LogicalTape *tape) const;

    RetStatus MergeRuns();
    RetStatus MergeOneRun();
    RetStatus CheckSortOnFinalMerge(bool &finalMerge);
    RetStatus FillHeapWithFirstTupleOfEachTape();
    int64 MergeReadBufferSize(int64 availMem, int nInputTapes, int nInputRuns, int maxOutputTapes) const;

    void InsertTupleIntoHeap(SortTuple *tuple);
    void HeapReplaceTop(SortTuple *tuple);
    void HeapDeleteTop();

    RetStatus SortMemTuples();
    RetStatus GrowMemTupleArray();
    RetStatus DumpTuples(bool forceAllTuples);

    static int ComparisonShim(Datum x, Datum y, SortSupport ssup);
    RetStatus PrepareSortSupportComparisonShim(FmgrInfo *flinfo, SortSupport ssup) const;
    RetStatus PrepareSortSupportFromIndexInfo(const IndexInfo &baseInfo, int16 strategy, SortSupport ssup);

    RetStatus InitSlabAllocator(int numSlots);
    bool IsSlabSlot(void *tuple) const;
    RetStatus AllocSlabSlot(void **tuple, Size tuplen);
    void ReleaseSlabSlot(void *tuple);

    int ApplySortComparatorHandleNull(bool isNull1, bool isNull2, SortSupport ssup) const;
    int ApplySortComparator(Datum datum1, bool isNull1, Datum datum2, bool isNull2, SortSupport ssup) const;
    /* For parallel index build */
    RetStatus InitParallelScanCxt(DstoreMemoryContext parentCxt = nullptr);
    RetStatus FillHeapWithFirstTupleOfEachWorker(); /* Only used for aggregation in parallel build */

    inline bool LackMem() const
    {
        return m_availMem < 0 && !m_slabAllocatorUsed;
    }

    inline void UseMem(int64 amt)
    {
        m_availMem -= amt;
    }

    inline void FreeMem(int64 amt)
    {
        m_availMem += amt;
    }
};

}

#endif /* SRC_GAUSSKERNEL_INCLUDE_COMMON_STORAGE_TUPLESORT_H */
