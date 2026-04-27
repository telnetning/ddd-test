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
 * dstore_index_struct.h
 *
 * IDENTIFICATION
 *        dstore/interface/index/dstore_index_struct.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_INDEX_STRUCT_H
#define DSTORE_INDEX_STRUCT_H
#include <functional>
#include "catalog/dstore_catalog_struct.h"
#include "tuple/dstore_tuple_struct.h"

namespace DSTORE {

#pragma GCC visibility push(default)
struct StorageRelationData;

const int INVALID_INDEX_FILLFACTOR = 0;
const int NAMEDATA_LEN = 64;

constexpr int INDEX_MAX_KEY_NUM = 32;
constexpr int BTREE_SUPPORT_FUNC_NUM = 4;

constexpr int MAX_RETRY_UPDATE_DELTA_DML_INDEX_TIME = 10;

enum class BtrCcidxStatus:uint8_t {
    NOT_CCINDEX = 0U,       /* normal index */
    WRITE_ONLY_INDEX,       /* has btree structure but unfinished. Invalid for scan, write only */
    IN_BUILDING_INDEX,      /* has no btree structure, under contruction */
};

enum class DmlOperationTypeForCcindex : int8_t {
    INVALID_TYPE = -1,         /* invalid type of dml operation */
    DML_OPERATION_DELETE = 0,  /* delete */
    DML_OPERATION_INSERT,      /* insert */
};

struct IndexSupportProcInfo;
struct IndexInfo {
    char            indexRelName[NAMEDATA_LEN];
    Oid             indexRelId;       /* oid of index */
    char            relKind;
    bool            isUnique;
    Oid            *opcinType;
    uint16_t        indexAttrsNum;    /* total number of columns in index */
    uint16_t        indexKeyAttrsNum; /* number of key columns in index */
    int16_t         tableOidAtt;      /* for global index, column no. of table oid */
    int16_t        *indexOption;
    TupleDescData  *attributes;
    CallbackFunc exprInitCallback;
    std::function<void (void *expr_cxt)> exprDestroyCallback;
    CallbackFunc exprCallback;
    BtrCcidxStatus  btrIdxStatus;
    Datum           extraInfo;        /* extra info to sql engine if any */
    IndexSupportProcInfo  *m_indexSupportProcInfo;
    const IndexSupportProcInfo* getIndexSupportProcInfo() const
    {
        return m_indexSupportProcInfo;
    }
    void Free();
};

struct IndexBuildInfo {
    IndexInfo       baseInfo;
    Oid             heapRelationOid;
    AttrNumber      indexAttrOffset[INDEX_MAX_KEY_NUM];
    TupleDescData  *heapAttributes;
    StorageRelationData **heapRels;
    StorageRelationData **indexRels;
    int             heapRelNum;
    Oid            *allPartOids; /* of oids for all partition or subpartition table */
    bool            lpiParallelMethodIsPartition;       /* whether we are building LPI in partition parallel mode */
    /*
     * current build partition idx of heapRels, indexRels and allPartOids arrays
     * (valid when lpiParallelMethodIsPartition is true).
     */
    int             currentBuildPartIdx;

    /* Below is for output */
    double          heapTuples;                         /* num of tuples seen in parent table */
    double          indexTuples;                        /* num of tuples inserted into index */
    double         *allPartTuples;                      /* num of tuples for all partition or subpartition table */
    IndexTuple     *duplicateTuple;                     /* duplicate tuple for unique index */
    Datum           duplicateHeapCtid1;                 /* heapCtid of one of the duplicate tuple */
    Datum           duplicateHeapCtid2;                 /* heapCtid of another duplicate tuple */
    std::function<void(void *&ptr, int dataLength)> memAllocFunc;
};

using GetProcFuncCb = void (*)(const IndexSupportProcInfo &procInfo, AttrNumber attNum,
    uint16_t procNum, FmgrInfo &fmgrInfo);
using GetOpfamilyProcFuncCb = void (*)(IndexSupportProcInfo &opfamilyInfo, Oid leftTypeOid, Oid rightTypeOid,
    AttrNumber attNum, FmgrInfo &fmgrInfo);
using GetOpfamilyStratFuncCb = void (*)(IndexSupportProcInfo &opfamilyInfo, Oid leftTypeOid, Oid rightTypeOid,
    AttrNumber attNum, uint16_t strat, FmgrInfo &fmgrInfo);
using GetOutputFuncCb = void (*)(Oid typeOid, FmgrInfo &locinfo);
using IndexCommonCb = RetStatus (*)(CallbackFunc fnAddr, FunctionCallInfo fcinfo, Datum *result);

struct IndexSupportProcInfo {
    /* The number of support functions corresponding to each column in this table, Relation->rd_am->amsupport */
    int16_t numSupportProc = 0;
    /* Number of attributes in a relation */
    int16_t numKeyAtts = 0;
    /* The columns of this table correspond to all support procedures oid arrays, Relation->rd_support */
    Oid *supportProcs = nullptr;
    /* The opfamily values corresponding to each column in this table (a comparative strategy classification),
     * Relation->rd_opfamily */
    Oid *opfamily = nullptr;
    /* Each column of this table corresponds to a pointer array of all support procedures FmgrInfo structures
     * (supported full comparison function cache), Relation->rd_supportinfo */
    FmgrInfo *supportFmgrInfo = nullptr;
};

struct IndexGetFuncCb {
    /* Get comparison functions based on typeoid or funcoID */
    GetProcFuncCb procFuncCb = nullptr;
    /* Get the comparison function based on the left and right typeoids from AMPROCNUM */
    GetOpfamilyProcFuncCb procOpfamilyProcFuncCb = nullptr;
    /* Get the comparison function based on the left and right typeoids from AMOPSTRATEGY, using specific strategy */
    GetOpfamilyStratFuncCb procOpfamilyStratFuncCb = nullptr;
    /* Check partition visible for global partition index */
    PGFunction partitionIsVisible = nullptr;
    /* Index common callback with longjump, used to invoke other index callbacks. */
    IndexCommonCb commonCb = nullptr;
};

using CheckPartOidHook = bool (*)(Oid partOid, void *dropPartTree);

struct GPIPartOidCheckInfo {
    CheckPartOidHook hook;
    void *dropPartTree;
};

#pragma GCC visibility pop
} /* namespace DSTORE */
#endif
