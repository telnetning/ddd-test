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
 * dstore_fake_relation.h
 *     This file defines the building blocks that bg page writer needs.
 *
 * IDENTIFICATION
 *        dstore/include/catalog/dstore_fake_relation.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_GAUSSKERNEL_INCLUDE_COMMON_DSTORE_FAKE_RELATION_H
#define SRC_GAUSSKERNEL_INCLUDE_COMMON_DSTORE_FAKE_RELATION_H

#include "common/dstore_datatype.h"
#include "tuple/dstore_tupledesc.h"
#include "catalog/dstore_catalog_struct.h"
#include "catalog/dstore_fake_index.h"
#include "catalog/dstore_fake_class.h"

namespace DSTORE {

struct StdRdOptions {
    int32 vlLen;           /* varlena header (do not touch directly!) */
    int fillfactor;          /* page fill factor in percent (0..100) */

    int initTd;

    /* Important:
     * for string type, data is appended at the tail of its parent struct.
     * CHAR* member of this STRUCT stores the offset of its string data.
     * offset=0 means that it's a NULL string.
     *
     * Take Care !!!
     * CHAR* member CANNOT be accessed directly.
     * StdRdOptionsGetStringData macro must be used for accessing CHAR* type member.
     */
    char* compression; /* compress or not compress */
    char* storage_type; /* table access method kind */
    char* orientation; /* row-store or column-store */
    char* indexsplit; /* page split method */
    char* ttl; /* time to live for tsdb data management */
    char* period; /* partition range for tsdb data management */
    char* partition_interval; /* partition interval for streaming contquery table */
    char* time_column; /* time column for streaming contquery table */
    char* ttl_interval; /* ttl interval for streaming contquery table */
    char* gather_interval; /* gather interval for streaming contquery table */
    char* string_optimize; /* string optimize for streaming contquery table */
    char* sw_interval; /* sliding window interval for streaming contquery table */
    char* version;
    char* wait_clean_gpi; /* pg_partition system catalog wait gpi-clean or not */
    /* item for online expand */
    char* append_mode;
    char* start_ctid_internal;
    char* end_ctid_internal;
    char* merge_list;
    char* dek_cipher;
    char* cmk_id;
    char* encrypt_algo;
    bool enable_tde;     /* switch flag for table-level TDE encryption */
    bool on_commit_delete_rows; /* global temp table */
};

/*
 * DstoreRelationGetDescr
 *      Returns tuple descriptor for a relation.
 */
inline TupleDescData *DstoreRelationGetDescr(Relation relation)
{
    return relation->attr;
}

/*
 * DstoreRelationGetRelationName
 *      Returns the rel's name.
 *
 * Note that the name is only unique within the containing namespace.
 */
inline char *DstoreRelationGetRelationName(Relation relation)
{
    return relation->rel->relname.data;
}

/*
 * DstoreIndexRelationGetNumberOfAttributes
 *      Returns the number of attributes in an index.
 */
inline int16 DstoreIndexRelationGetNumberOfAttributes(Relation relation)
{
    return relation->index->indnatts;
}

/*
 * DstoreIndexRelationGetNumberOfKeyAttributes
 *      Returns the number of key attributes in an index.
 */
inline uint16 DstoreIndexRelationGetNumberOfKeyAttributes(Relation relation)
{
    return relation->indexKeyAttsNum;
}

/*
 * DstoreRelNeedPersistWal
 *      Return true if target relation's persistence method need Wal
 */
inline bool DstoreRelNeedPersistWal(char relPersistMethod)
{
    return relPersistMethod == SYS_RELPERSISTENCE_PERMANENT;
}

inline bool DstoreRelationIsGlobalIndex(Relation relation)
{
    return relation->rel->relkind == SYS_RELKIND_GLOBAL_INDEX;
}

inline bool DstoreRelationIsIndex(Relation relation)
{
    return relation->rel->relkind == SYS_RELKIND_INDEX || DstoreRelationIsGlobalIndex(relation);
}

inline bool DstoreRelationIsSubPartitioned(Relation relation)
{
    return relation->rel->parttype == SYS_PARTTYPE_SUBPARTITIONED_RELATION;
}

inline bool DstoreRelationIsNonpartitioned(Relation relation)
{
    return relation->rel->parttype == SYS_PARTTYPE_NON_PARTITIONED_RELATION;
}

/*
 * true if the relation is construct from a partition
 */
inline bool DstoreRelationIsPartition(Relation relation)
{
    return DstoreOidIsValid(relation->parentId) && relation->rel->parttype == SYS_PARTTYPE_NON_PARTITIONED_RELATION &&
           (relation->rel->relkind == SYS_RELKIND_RELATION || relation->rel->relkind == SYS_RELKIND_INDEX);
}

}
#endif /* SRC_GAUSSKERNEL_INCLUDE_COMMON_STORAGE_FAKE_RELATION_H */
