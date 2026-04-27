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
 * dstore_fake_class.h
 *
 * IDENTIFICATION
 *        dstore/include/catalog/dstore_fake_class.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_GAUSSKERNEL_INCLUDE_COMMON_DSTORE_FAKE_CLASS_H
#define SRC_GAUSSKERNEL_INCLUDE_COMMON_DSTORE_FAKE_CLASS_H
#include "systable/systable_relation.h"
#ifdef CLOUD_NATIVE_DB1
#include "common/dstore_datatype.h"
#include "catalog/dstore_catalog_struct.h"
namespace DSTORE {

#pragma pack (push, 1)
struct FormData_pg_class {
    DstoreNameData relname; /* class name */
    Oid relnamespace; /* OID of namespace containing this class */
    Oid reltype;      /* OID of entry in pg_type for table's
                       * implicit row type */
    Oid reloftype;    /* OID of entry in pg_type for underlying
                       * composite type */
    Oid relowner;     /* class owner */
    Oid relam;        /* index access method; 0 if not an index */
    Oid relfilenode;  /* physical storage file identifier */

    /* relfilenode == 0 means it is a "mapped" relation, see relmapper.c */
    Oid reltablespace;   /* identifier of table space for relation */
    float64 relpages;     /* # of blocks (not always up-to-date) */
    float64 reltuples;    /* # of tuples (not always up-to-date) */
    int32 relallvisible;  /* # of all-visible blocks (not always
                          * up-to-date) */
    Oid reltoastrelid;   /* OID of toast table; 0 if none */
    Oid reltoastidxid;   /* if toast table, OID of chunk_id index */
    Oid reldeltarelid;   /* if ColStore table, it is not 0 */
    Oid reldeltaidx;
    Oid relcudescrelid;  /* if ColStore table, it is not 0; if TsStore, it is partition oid */
    Oid relcudescidx;
    bool relhasindex;    /* T if has (or has had) any indexes */
    bool relisshared;    /* T if shared across databases */
    char relpersistence; /* see RELPERSISTENCE_xxx constants below */
    char relkind;        /* see RELKIND_xxx constants below */
    int16 relnatts;       /* number of user attributes */

    /*
     * Class pg_attribute must contain exactly "relnatts" user attributes
     * (with attnums ranging from 1 to relnatts) for this class.  It may also
     * contain entries with negative attnums for system attributes.
     */
    int16 relchecks;                  /* # of CHECK constraints for class */
    bool relhasoids;                 /* T if we generate OIDs for rows of rel */
    bool relhaspkey;                 /* has (or has had) PRIMARY KEY index */
    bool relhasrules;                /* has (or has had) any rules */
    bool relhastriggers;             /* has (or has had) any TRIGGERs */
    bool relhassubclass;             /* has (or has had) derived classes */
    int8 relcmprs;                   /* row compression attribution */
    bool relhasclusterkey;           /* has (or has had) any PARTIAL CLUSTER KEY */
    bool relrowmovement;             /* enable or disable rowmovement */
    char parttype;                   /* 'p' for  partitioned relation, 'n' for non-partitioned relation */

    FileId      relfileid;
    BlockNumber relblknum;
    FileId      rellobfileid;        /* SegmentId of LobSegment (used to store out-of-line values) */
    BlockNumber rellobblknum;        /* SegmentId of LobSegment (used to store out-of-line values) */

    uint32 relfrozenxid;             /* all Xids < this are frozen in this rel */
#ifdef CATALOG_VARLEN                /* variable-length fields start here */
    /* NOTE: These fields are not present in the rd_rel field of relcache entry. */
    aclitem relacl[1];               /* access permissions */
    text reloptions[1];              /* access-method-specific options */
    char relreplident;               /* see REPLICA_IDENTITY_xxx constants */
    uint64 relfrozenxid64;           /* all Xids < this are frozen in this rel */
    Oid relbucket;                   /* bucket info in pg_hashbucket */
    int2vector relbucketkey;         /* Column number of hash partition */
    uint64 relminmxid;        /* all multixacts in this rel are >= this.
                                      * this is really a MultiXactId */
#endif
};
#pragma pack (pop)

}
#endif
#endif /* SRC_GAUSSKERNEL_INCLUDE_COMMON_STORAGE_FAKE_CLASS_H */
