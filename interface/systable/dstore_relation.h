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
 * dstore_relation.h
 *
 * IDENTIFICATION
 *        interface/systable/dstore_relation.h
 *
 * ---------------------------------------------------------------------------------------
 */
 
#ifndef DSTORE_RELATION_H
#define DSTORE_RELATION_H
#include "common/dstore_common_utils.h"
#include "tuple/dstore_tuple_struct.h"
#include "systable/systable_relation.h"
#include "systable/sys_partition.h"
#include "index/dstore_index_struct.h"
#include "systable/systable_index.h"
#include "table/dstore_table_interface.h"

namespace DSTORE {

class TableStorageMgr;
class BtreeStorageMgr;
class HeapSegment;
class IndexSegment;
#pragma GCC visibility push(default)
using StorageRelation = struct StorageRelationData*;
struct StorageRelationData {
public:
	Oid		                     	relOid;
    SysClassTupDef*                 rel;
    TupleDesc                       attr;
    IndexInfo*                      index;
    Int32Vector*                    indKey;
    SysIndexTupDef*                 indexInfo;
    TableStorageMgr*                tableSmgr;
    TableStorageMgr*                lobTableSmgr;
    BtreeStorageMgr*                btreeSmgr;
    void*                           pgstat_info;
    /*
     * A seqno for each worker/session attachement to identify if relationData needs cleanup.
     * It starts from 0, when equals to t_thrd.lsc_cxt.xact_seqno means the
     * relationData is open and used in current transaction then do nothing, otherwise we need reset relationdata.
     */
    uint64_t                          xact_seqno;
    PdbId m_pdbId = INVALID_PDB_ID;
    void SetTableSmgrSegment(HeapSegment *segment);
    void SetBtreeSmgrSegment(IndexSegment *segment);
    SegmentInterface *GetTableSmgrSegment();
    SegmentInterface *GetBtreeSmgrSegment();
    SegmentInterface *GetLobSmgrSegment();
    RetStatus Construct(Oid pdbId, Oid relid, SysClassTupDef *classTuple, TupleDesc tupDesc,
        int fillFactor, TablespaceId tablespaceId, bool enableLsc = false, bool is_nailed = false);
    RetStatus Construct(Oid pdbId, Oid partid, SysPartitionDef *partTuple, TupleDesc tupDesc,
        const int fillFactor, TablespaceId tablespaceId, bool isThrdMemCtx = false);
    RetStatus ConstructLobTableSmgr(TablespaceId tablespaceId, PageId lobSegmentId, const int fillFactor,
                                    char relpersistence, bool isThrdMemCtx = false);
    void SetTupleDesc(bool isThrdMemCtx);
    void Destroy();
    uint64_t GetSmgrSeqno();
    void SetSmgrSeqno(uint64_t newSeqno);
    void Init();
    PageId GetSegmentPageId();
};
inline bool DstoreRelationIsValid(const StorageRelation r)
{
    return r != nullptr;
}
inline bool DstoreRelationIsIndex(StorageRelation relation)
{
    RelationKind relkind = static_cast<RelationKind>(relation->rel->relkind);
    return (relkind == SYS_RELKIND_INDEX || relkind == SYS_RELKIND_GLOBAL_INDEX);
}
inline bool DstoreRelationIsComposite(StorageRelation relation)
{
    RelationKind relkind = static_cast<RelationKind>(relation->rel->relkind);
    return (relkind == SYS_RELKIND_COMPOSITE_TYPE);
}

inline bool DstoreRelationHasIndex(StorageRelation relation)
{
    if (relation != nullptr && relation->rel != nullptr) {
        return relation->rel->relhasindex;
    }
    return false;
}

inline bool DstoreRelationHasPrimaryIndex(StorageRelation relation)
{
    if (relation != nullptr && relation->rel != nullptr) {
        return relation->rel->relhaspkey;
    }
    return false;
}

inline int DstoreRelationGetAttrNum(StorageRelation relation)
{
    return relation->attr->natts;
}
inline uint16_t DstoreIndexRelationGetAttrNum(StorageRelation relation)
{
    return relation->index->indexAttrsNum;
}

inline char *DstoreGetRelationName(const StorageRelation relation)
{
    if (relation != nullptr && relation->rel != nullptr) {
        return relation->rel->relname.data;
    }
    return nullptr;
}

inline bool DstoreRelationHasOid(StorageRelation relation)
{
    return relation->rel->relhasoids;
}
inline bool DstoreRelationIsTemp(const StorageRelation r)
{
    return r->rel->relpersistence == static_cast<char>(SYS_RELPERSISTENCE_TEMP) ||
           r->rel->relpersistence == static_cast<char>(SYS_RELPERSISTENCE_GLOBAL_TEMP);
}
/*
 * RELATION_IS_OTHER_TEMP
 *		Test for a temporary relation that belongs to some other session.
 *
 * Beware of multiple eval of argument
 */
inline bool DstoreRelationIsGlobalTemp(const StorageRelation r)
{
    return r->rel->relpersistence == static_cast<char>(SYS_RELPERSISTENCE_GLOBAL_TEMP);
}

#pragma GCC visibility pop
}  // namespace DSTORE
#endif
