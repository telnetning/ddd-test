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
#ifndef DSTORE_UT_TABLE_HANDLER_H
#define DSTORE_UT_TABLE_HANDLER_H

#include "ut_mock/ut_instance_mock.h"
#include "catalog/dstore_typecache.h"
#include "heap/dstore_heap_insert.h"
#include "heap/dstore_heap_delete.h"
#include "heap/dstore_heap_update.h"
#include "index/dstore_btree_build.h"
#include "index/dstore_btree_insert.h"
#include "index/dstore_btree_delete.h"

namespace DSTORE {

struct DefaultRowDef { /* MUST keep the same order with TYPE_CACHE_TABLE!!! */
    bool column_bool;               /* 0 */
    int16 column_int16;             /* 1 */
    int32 column_int32;             /* 2 */
    int64 column_int64;             /* 3 */
    char column_char;               /* 4 */
    DstoreNameData column_name;     /* 5 */
    Oid column_oid;                 /* 6 */
    Timestamp column_timestamp;     /* 7 */
    TimestampTz column_timestamptz; /* 8 */
    float32 column_float32;         /* 9 */
    float64 column_float64;         /* 10 */
    text *column_text;              /* 11 */
    VarChar *column_varchar;        /* 12 */
    char * column_cstring;          /* 13 */
    DateADT column_date;            /* 14 */
    Cash column_money;              /* 15 */
    TimeADT column_time;            /* 16 */
    MacAddr *column_macaddr;        /* 17 */
    Inet *column_inet;              /* 18 */
    varlena *column_blob;           /* 19 */
    varlena *column_clob;           /* 20 */
};

static const int TEXT_IDX = 11;
static const int VARCHAR_IDX = 12;
static const int CSTRING_IDX = 13;
static const int BLOB_IDX = 19;
static const int CLOB_IDX = 20;

static_assert(TYPE_CACHE_NUM == CLOB_IDX+1);

/* If some bit is null, mark it true */
static bool DefaultNullBitMap[TYPE_CACHE_NUM] = {false};

struct HeapTestContext {
    TupleDesc        tupleDesc;
    StorageRelation  tableRel;
    int              numHeapPartition;
};

struct BtreeTestContext {
    StorageRelation  indexRel;
    ScanKey          scanKeyInfo;
    IndexBuildInfo  *indexBuildInfo;
    PageId           lastWorkingPageId;
};

struct BtreeTestInsertContext {
    HeapTuple  *heapTuple;
    IndexTuple *indexTuple;
    Datum      *values;
    bool       *isnull;
};

class UTTableHandler : public BaseObject {
public:
    ~UTTableHandler() = default;
    DISALLOW_COPY_AND_MOVE(UTTableHandler);

    /* Create: will allocate data segment */
    static UTTableHandler *CreateTableHandler(PdbId pdbId, DstoreMemoryContext memCxt, bool needLob = true,
        bool isTempTableSpace = false, int nattsCustomized = 0, Oid *attTypePidListCustomized = nullptr);
    static UTTableHandler *CreatePartitionTableHandler(PdbId pdbId, DstoreMemoryContext memCxt, int numPartition);
    /* Destroy: will drop segment and table space */
    static void Destroy(UTTableHandler *utTableHandler);

    /* Get: get UTTableHandler with given data segment ID */
    static UTTableHandler *GetTableHandler(PdbId pdbId, DstoreMemoryContext memCxt, PageId heapSegmentId,
        PageId lobSegmentId, PageId indexSegmentId, bool isTempTableSpace = false, int nattsCustomized = 0,
        Oid *attTypePidListCustomized = nullptr);
    /* Clear: release memory but keep data segment and table space. */
    void Clear();

    void CreateIndex(int *colIndex, int numKeyAttrs, bool unique, int fillfactor = BTREE_DEFAULT_FILLFACTOR,
                     int numAttr = 0);
    void CreateIndexParallel(int *colNums, int numKeyAttrs, bool unique,
                             int fillfactor = BTREE_DEFAULT_FILLFACTOR, int parallelworkers = 4);
    void CreateBtreeContext(int *colIndex, int numKeyAttrs, bool unique, int fillfactor = BTREE_DEFAULT_FILLFACTOR,
                            int numAttr = 0);
    PageId GetBtreeRootPageId();

    static HeapTuple *GenerateRandomHeapTuple(TupleDesc tupleDesc);
    static DefaultRowDef GetDefaultRowDef();
    static HeapTuple *GenerateSpecificHeapTuple(const std::string &rawData);
    static void *GenerateSpecificDatumPtr(Oid typeOid, int seed = 1);
    static void *GenerateRandomDatumPtr(Oid typeOid, int random = 1);
    static void *GenerateTextWithFixedLen(int len = 1024);
    TupleDesc GenerateFakeLogicalDecodeTupDesc();

    void FillTableWithRandomData(int rowNum);
    void FillTableWithRandomData(int partitionNo, int rowNum);
    void GenerateRandomData(Datum *values, bool *isnulls);
    HeapTuple *GenerateRandomHeapTuple();
    HeapTuple *GenerateRandomHeapTuple(Datum *values, bool *isnulls);
    BtreeTestInsertContext *GenerateRandomIndexTuple();
    IndexTuple *InsertRandomIndexTuple(bool alreadyXactStart = false);
    RetStatus DeleteWithIndexTuple(IndexTuple *indexTuple, bool alreadyXactStart, bool isOnlyDeleteHeap = false);
    RetStatus DeleteWithIndexTuple(BtreePagePayload &pagePayload, OffsetNumber delOff, IndexTuple *indexTuple,
                                   bool alreadyXactStart);
    IndexTuple *UpdateWithIndexTuple(IndexTuple *indexTuple, bool alreadyXactStart);

    RetStatus InsertIndexTupleOnly(DefaultRowDef *rowDef, bool *nullbitmap, ItemPointer heapCtid);
    RetStatus DeleteIndexTupleOnly(DefaultRowDef *rowDef, bool *nullbitmap, ItemPointer heapCtid);

    void FillTableWithSpecificData(DefaultRowDef *tableDef, int rowNum, bool isTxnStarted = false);
    void FillPartitionTableWithSpecificData(int partitionNo, DefaultRowDef *tableDef, int rowNum,
        bool isTxnStarted = false);
    HeapTuple *GetSpecificHeapTuple(DefaultRowDef *tableDef, Datum *values, bool *isnulls,
        bool *nullbitmap = DefaultNullBitMap);

    ItemPointerData InsertHeapTupAndCheckResult(HeapTuple *heapTuple, bool alreadyXactStart = false,
        SnapshotData *snapshot = INVALID_SNAPSHOT, bool addTxnCid = true, bool checkResult = true);
    HeapTuple *FetchHeapTuple(ItemPointerData *ctid, SnapshotData *snapshot = INVALID_SNAPSHOT,
                              bool alreadyStartXact = false);
    HeapTuple *FetchHeapTupleWithCsn(ItemPointerData *ctid, bool alreadyStartXact, CommitSeqNo snapshotCsn);
    void FetchHeapTupAndCheckResult(HeapTuple *expecTup, ItemPointerData *ctid,
                                    SnapshotData *snapshot = INVALID_SNAPSHOT, bool alreadyStartXact = false);
    void BatchInsertHeapTupsAndCheckResult(HeapTuple **heapTuples, uint16 nTuples, ItemPointerData *ctids,
                                           SnapshotData *snapshot = INVALID_SNAPSHOT, bool alreadyXactStart = false);

    static IndexTuple *GenerateSpecificIndexTuple(std::string rawData, ItemPointerData tid = INVALID_ITEM_POINTER);
    static Datum GenerateRandomDataByType(Form_pg_attribute attr);

    inline StorageRelation GetTableRel() const
    {
        return m_heapTestContext->tableRel;
    }
    inline TableStorageMgr *GetHeapTabSmgr() const
    {
        return m_heapTestContext->tableRel->tableSmgr;
    }

    inline TableStorageMgr *GetLobTabSmgr() const
    {
        return m_heapTestContext->tableRel->lobTableSmgr;
    }

    inline TupleDesc GetHeapTupDesc() const
    {
        return m_heapTestContext->tupleDesc;
    }

    int CompareDatum(Datum a, Datum b, Oid typeOid);

    inline TupleDesc GetIndexTupleDesc() const
    {
        return m_btreeTestContext->indexBuildInfo->baseInfo.attributes;
    }

    inline StorageRelation GetIndexRel() const
    {
        return m_btreeTestContext->indexRel;
    }

    inline BtreeStorageMgr *GetBtreeSmgr() const
    {
        return m_btreeTestContext->indexRel->btreeSmgr;
    }

    inline ScanKey GetIndexScanKey() const
    {
        return m_btreeTestContext->scanKeyInfo;
    }

    inline IndexBuildInfo *GetIndexBuildInfo() const
    {
        return m_btreeTestContext->indexBuildInfo;
    }

    inline IndexInfo *GetIndexInfo() const
    {
        return &m_btreeTestContext->indexBuildInfo->baseInfo;
    }

    inline void SetIndexUnique(bool isUnique)
    {
        m_btreeTestContext->indexBuildInfo->baseInfo.isUnique = isUnique;
    }

    inline bool GetIndexUnique()
    {
        return m_btreeTestContext->indexBuildInfo->baseInfo.isUnique;
    }

    inline PageId GetLastWorkingBtrPageId() const
    {
        return m_btreeTestContext->lastWorkingPageId;
    }

    inline void SetNoNeedDropSegment()
    {
        m_allocated = false;
    }

    inline void SetCcindexBtrStatus(BtrCcidxStatus status)
    {
        m_btreeTestContext->indexBuildInfo->baseInfo.btrIdxStatus = status;
    }

    inline PdbId GetPdbId()
    {
        return m_pdbId;
    }

    PdbId m_pdbId;
    TablespaceId m_tablespaceId;
    PageId m_heapSegmentPageId;
    PageId m_lobSegmentPageId;
    PageId m_indexSegmentPageId;
    HeapTestContext *m_heapTestContext;
    BtreeTestContext *m_btreeTestContext;
    DstoreMemoryContext m_utTableHandlerMemCxt;
    bool m_allocated;

protected:
    void GetScanFuncByFnOid(ScanKey scanKey, Oid fnOid);
    void GetScanFuncByType(ScanKey scanKey, Oid leftType, Oid rightType);
    void GetScanFuncByType(ScanKey scanKey, Oid type);

private:
    static UTTableHandler *Create(PdbId pdbId, DstoreMemoryContext memCxt, bool allocateNew, bool isTempTableSpace);
    UTTableHandler(DstoreMemoryContext memCxt, bool allocateNew);

    static void GenerateRandomData(Datum *values, bool *isnulls, TupleDesc tupleDesc);

    void CreateTableContext(PdbId pdbId, int nattsCustomized = 0, Oid *attTypePidListCustomized = nullptr, bool needLob = false);
    void CreatePartitionedTableContext(int numPartition);

    void FillTableWithRandomData(StorageRelation tableRel, int rowNum);
    IndexBuildInfo *GenerateIndexBuildInfo(int numKeyAttrs, int numAttr, int *colNums, bool unique);
    TupleDesc GenerateEmptyTupDescTemplate(int natts);
    TupleDesc GenerateFakeHeapTupDesc(int nattsCustomized = 0, Oid *attTypePidListCustomized = nullptr);
    TupleDesc GenerateFakeLobTupDesc();
    void GenerateFakeIndexTupDesc(IndexBuildInfo *buildInfo, int numAttr, int *colIndex);
    void GetScanKeyByTypeKeyCache(ScanKey scanKey,  Pointer cachePointer);
    Datum GetSpecificDataByType(DefaultRowDef* tableDef, Oid type);
};

/* For multi-thread test case. UTTableHandler is owned by thread. NEVER share UTTableHandler between threads. */
extern thread_local UTTableHandler *ThdUtTableHandler;

} /* namespace DSTORE */

#endif /* DSTORE_UT_TABLE_HANDLER_H */
