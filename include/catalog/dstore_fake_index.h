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
 * dstore_fake_index.h
 *
 * IDENTIFICATION
 *        dstore/include/catalog/dstore_fake_index.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_GAUSSKERNEL_INCLUDE_COMMON_DSTORE_FAKE_INDEX_H
#define SRC_GAUSSKERNEL_INCLUDE_COMMON_DSTORE_FAKE_INDEX_H

#include "common/dstore_datatype.h"
#include "catalog/dstore_catalog_struct.h"

namespace DSTORE {

#pragma pack (push, 1)
struct FormData_pg_index {
    Oid         indexrelid;      /* OID of the index */
    Oid         indrelid;        /* OID of the relation it indexes */
    int16        indnatts;        /* total number of columns in index */
    bool        indisunique;     /* is this a unique index? */
    bool        indisprimary;    /* is this index for primary key? */
    bool        indisexclusion;  /* is this index for exclusion constraint? */
    bool        indimmediate;    /* is uniqueness enforced immediately? */
    bool        indisclustered;  /* is this the index last clustered by? */
    bool        indisusable;     /* is this index useable for select and insert? */
    /* if yes, insert and select should ignore this index */
    bool        indisvalid;      /* is this index valid for use by queries? */
    bool        indcheckxmin;    /* must we wait for xmin to be old? */
    bool        indisready;      /* is this index ready for inserts? */

    /* variable-length fields start here, but we allow direct access to indkey */
    int2vector  indkey;          /* column numbers of indexed cols, or zero */

#ifdef CATALOG_VARLEN
    oidvector   indcollation;    /* collation identifiers */
    oidvector   indclass;        /* opclass identifiers */
    int2vector  indoption;       /* per-column flags (AM-specific meanings) */
    pg_node_tree indexprs;       /* expression trees for index attributes that
                                  * are not reference by simple column; one for
                                  * each zero entry in indkey[] */
    pg_node_tree indpred;        /* expression tree for predicate, if a partial
                                   * index; otherwise NULL */
    bool        indisreplident;  /* is this index the identity for replication? */
    int16        indnkeyatts;     /* number of key columns in index */
#endif
};
#pragma pack (pop)

}

#endif /* SRC_GAUSSKERNEL_INCLUDE_COMMON_STORAGE_FAKE_INDEX_H */
