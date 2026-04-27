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
 * Description:
 * The systable_index is one of normal system tables.
 */

#ifndef DSTORE_SYSTABLE_INDEX_H
#define DSTORE_SYSTABLE_INDEX_H

#include <cstdint>
#include "common/dstore_common_utils.h"
#include "index/dstore_scankey.h"
#include "tuple/dstore_tuple_struct.h"
#include "lock/dstore_lock_struct.h"
namespace DSTORE {
constexpr Oid SYS_INDEX_RELATION_ID = 2610;
constexpr Oid SYS_INDEX_RELATION_ROWTYPE_ID = 10003;

/* ----------------
 *        compiler constants for sys_index
 * ----------------
 */
constexpr int NATTS_SYS_INDEX = 22;
constexpr int ANUM_SYS_INDEX_INDEXRELID = 1;
constexpr int ANUM_SYS_INDEX_INDRELID = 2;
constexpr int ANUM_SYS_INDEX_NATTS = 3;
constexpr int ANUM_SYS_INDEX_ISUNIQUE = 4;
constexpr int ANUM_SYS_INDEX_ISPRIMARY = 5;
constexpr int ANUM_SYS_INDEX_ISEXCLUSION = 6;
constexpr int ANUM_SYS_INDEX_IMMEDIATE = 7;
constexpr int ANUM_SYS_INDEX_ISCLUSTERED = 8;
constexpr int ANUM_SYS_INDEX_ISUSABLE = 9;
constexpr int ANUM_SYS_INDEX_ISVALID = 10;
constexpr int ANUM_SYS_INDEX_CHECKXMIN = 11;
constexpr int ANUM_SYS_INDEX_ISREADY = 12;
constexpr int ANUM_SYS_INDEX_KEY = 13;
constexpr int ANUM_SYS_INDEX_COLLATION = 14;
constexpr int ANUM_SYS_INDEX_CLASS = 15;
constexpr int ANUM_SYS_INDEX_OPTION = 16;
constexpr int ANUM_SYS_INDEX_EXPRS = 17;
constexpr int ANUM_SYS_INDEX_PRED = 18;
constexpr int ANUM_SYS_INDEX_ISREPLIDENT = 19;
constexpr int ANUM_SYS_INDEX_NKEYATTS = 20;
constexpr int ANUM_SYS_INDEX_CCTMPID = 21;
constexpr int ANUM_SYS_INDEX_ISVISIBLE = 22;

struct SysIndexTupDef {
    Oid indexrelid;      /* OID of the index */
    Oid indrelid;        /* OID of the relation it indexes */
    uint16_t indnatts;   /* total number of columns in index */
    bool indisunique;    /* is this a unique index? */
    bool indisprimary;   /* is this index for primary key? */
    bool indisexclusion; /* is this index for exclusion constraint? */
    bool indimmediate;   /* is uniqueness enforced immediately? */
    bool indisclustered; /* is this the index last clustered by? */
    bool indisusable;    /* is this index useable for select and insert? */
    /* if yes, insert and select should ignore this index */
    bool indisvalid;   /* is this index valid for use by queries? */
    bool indcheckxmin; /* must we wait for xmin to be old? */
    bool indisready;   /* is this index ready for inserts? */

    /* variable-length fields start here, but we allow direct access to indkey */
    Int32Vector indkey; /* column numbers of indexed cols, or 0 */

#ifdef CATALOG_VARLEN
    oidvector indcollation; /* collation identifiers */
    oidvector indclass;     /* opclass identifiers */
    int2vector indoption;   /* per-column flags (AM-specific meanings) */
    pg_node_tree indexprs;  /* expression trees for index attributes that
                             * are not reference by simple column; one for
                             * each zero entry in indkey[] */
    pg_node_tree indpred;   /* expression tree for predicate, if a partial
                             * index; otherwise NULL */
    bool        indisreplident;     /* is this index the identity for replication? */
    uint16_t    indnkeyatts;        /* number of key columns in index */
    Oid         indcctmpid;         /* OID of the temporary table */
    bool        indisvisible;       /* is this a visible index? */
#endif
};
inline bool IndexIsUnique(SysIndexTupDef *index)
{
    return index->indisunique;
}

inline Oid IndexGetIndexRelOid(SysIndexTupDef *index)
{
    return index->indexrelid;
}
inline Oid IndexGetHeapRelOid(SysIndexTupDef *index)
{
    return index->indrelid;
}
}  // namespace DSTORE
#endif
