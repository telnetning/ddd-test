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
 * dstore_catalog_struct.h
 *
 *
 *
 * IDENTIFICATION
 *        storage/include/heap/dstore_catalog_struct.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_CATALOG_STRUCT_H
#define DSTORE_CATALOG_STRUCT_H

#include "common/dstore_common_utils.h"
#include "page/dstore_page_struct.h"
#include "systable/systable_relation.h"
namespace DSTORE {

class TableStorageMgr;
class BtreeStorageMgr;

/* FakeRelationData* data only depend by storage engine. */
struct FakeRelationData {
    char persistenceLevel; /* rel need persistence : move to class data info */

    FormData_pg_class *rel;
    struct TupleDescData *attr; /* tuple descriptor */
    Oid relOid;                 /* FakeRelationData*'s object id */

    /*
     * options is set whenever rd_rel is loaded into the relcache entry.
     * Note that you can NOT look into rd_rel for this data.  NULL means "use
     * defaults".
     */
    bytea *options; /* parsed pg_class.reloptions */

    /*
     * index access support info (used only for an index FakeRelationData*)
     *
     * Note: only default support procs for each opclass are cached, namely
     * those with lefttype and righttype equal to the opclass's opcintype. The
     * arrays are indexed by support function number, which is a sufficient
     * identifier given that restriction.
     *
     */
    uint16_t indexKeyAttsNum;                              /* index FakeRelationData*'s indexkey nums */
    int16_t *indexOption;                                  /* per-column AM-specific flags */
    Oid *opFamily;                                         /* OIDs of op families for each index col */
    Oid *opcinType;                                        /* OIDs of opclass declared input data types */
    struct FormData_pg_index *index;                       /* pg_index describing this index */
    Oid partHeapOid;                                       /* partition index's partition oid */

    /* if this is construct by partitionGetRelation,this is Partition Oid,else this is DSTORE_INVALID_OID */
    Oid parentId;

    TableStorageMgr *tableSmgr = nullptr;
    TableStorageMgr *lobTableSmgr = nullptr;
    BtreeStorageMgr *btreeSmgr = nullptr;
};

using Relation = FakeRelationData *;
/* Form_pg_index corresponds to a pointer to a tuple with the format of pg_index relation. */
using Form_pg_index = struct FormData_pg_index *;

using MetaDataInvByOidCallback = void (*)(Oid oid);
} /* namespace DSTORE */
#endif
