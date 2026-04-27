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
 * The sys_relation is one of core system tables.
 * When we speak of “relation”, we mean: indexes,
 * sequences, views, materialized views, composite types.
 */
#ifndef DSTORE_SYSTABLE_RELATION_H
#define DSTORE_SYSTABLE_RELATION_H
#include <cstdint>
#include "common/dstore_common_utils.h"
#include "tuple/dstore_tuple_struct.h"
#include "index/dstore_scankey.h"
#include "lock/dstore_lock_struct.h"
namespace DSTORE {
constexpr char SYSTABLE_RELATION_NAME[] = "sys_relation";
constexpr int SYS_RELATION_NAMESPACE = 11;
constexpr int SYSTABLE_RELATION_OID = 1259;
constexpr int SYSTABLE_RELATION_ROWTYPE = 83;
constexpr int NATTS_SYS_REL = 44;
constexpr int ANUM_SYS_REL_RELNAME = 1;
constexpr int ANUM_SYS_REL_RELNAMESPACE = 2;
constexpr int ANUM_SYS_REL_RELTYPE = 3;
constexpr int ANUM_SYS_REL_RELOFTYPE = 4;
constexpr int ANUM_SYS_REL_RELOWNER = 5;
constexpr int ANUM_SYS_REL_RELAM = 6;
constexpr int ANUM_SYS_REL_RELFILENODE = 7;
constexpr int ANUM_SYS_REL_RELTABLESPACE = 8;
constexpr int ANUM_SYS_REL_RELPAGES = 9;
constexpr int ANUM_SYS_REL_RELTUPLES = 10;
constexpr int ANUM_SYS_REL_RELALLVISIBLE = 11;
constexpr int ANUM_SYS_REL_RELTOASTRELID = 12;
constexpr int ANUM_SYS_REL_RELTOASTIDXID = 13;
constexpr int ANUM_SYS_REL_RELDELTARELID = 14;
constexpr int ANUM_SYS_REL_RELDELTAIDX = 15;
constexpr int ANUM_SYS_REL_RELCUDESCRELID = 16;
constexpr int ANUM_SYS_REL_RELCUDESCIDX = 17;
constexpr int ANUM_SYS_REL_RELHASINDEX = 18;
constexpr int ANUM_SYS_REL_RELISSHARED = 19;
constexpr int ANUM_SYS_REL_RELPERSISTENCE = 20;
constexpr int ANUM_SYS_REL_RELKIND = 21;
constexpr int ANUM_SYS_REL_RELNATTS = 22;
constexpr int ANUM_SYS_REL_RELCHECKS = 23;
constexpr int ANUM_SYS_REL_RELHASOIDS = 24;
constexpr int ANUM_SYS_REL_RELHASPKEY = 25;
constexpr int ANUM_SYS_REL_RELHASRULES = 26;
constexpr int ANUM_SYS_REL_RELHASTRIGGERS = 27;
constexpr int ANUM_SYS_REL_RELHASSUBCLASS = 28;
constexpr int ANUM_SYS_REL_RELCMPRS = 29;
constexpr int ANUM_SYS_REL_RELHASCLUSTERKEY = 30;
constexpr int ANUM_SYS_REL_RELROWMOVEMENT = 31;
constexpr int ANUM_SYS_REL_PARTTYPE = 32;
constexpr int ANUM_SYS_REL_RELFILEID = 33;
constexpr int ANUM_SYS_REL_RELBLKNUM = 34;
constexpr int ANUM_SYS_REL_RELLOBFILEID = 35;
constexpr int ANUM_SYS_REL_RELLOBBLKNUM = 36;
constexpr int ANUM_SYS_REL_RELFROZENXID = 37;
constexpr int ANUM_SYS_REL_RELACL = 38;
constexpr int ANUM_SYS_REL_RELOPTIONS = 39;
constexpr int ANUM_SYS_REL_RELREPLIDENT = 40;
constexpr int ANUM_SYS_REL_RELFROZENXID64 = 41;
constexpr int ANUM_SYS_REL_RELBUCKET = 42;
constexpr int ANUM_SYS_REL_RELBUCKETKEY = 43;
constexpr int ANUM_SYS_REL_RELMINMXID = 44;
constexpr int SEQUENCE_RELATION_ID = 6160;
constexpr int SEQUENCE_INDEX_RELATION_ID = 6162;

enum RelationKind : char {
    SYS_RELKIND_RELATION = 'r',       /* ordinary table */
    SYS_RELKIND_INDEX = 'i',          /* secondary index */
    SYS_RELKIND_GLOBAL_INDEX = 'I',   /* GLOBAL partitioned index */
    SYS_RELKIND_SEQUENCE = 'S',       /* sequence object */
    SYS_RELKIND_LARGE_SEQUENCE = 'L', /* large sequence object that support 128-bit integer */
    SYS_RELKIND_VIEW = 'v',           /* view */
    SYS_RELKIND_MATVIEW = 'm',        /* materialized view */
    SYS_RELKIND_COMPOSITE_TYPE = 'c', /* composite type */
    SYS_RELKIND_FOREIGN_TABLE = 'f',  /* foreign table */
    SYS_RELKIND_STREAM = 'e',         /* stream */
    SYS_RELKIND_CONTQUERY = 'o',      /* contview */
};

enum RelationPersistence : char {
    SYS_RELPERSISTENCE_PERMANENT = 'p',   /* regular table */
    SYS_RELPERSISTENCE_UNLOGGED = 'u',    /* unlogged permanent table */
    SYS_RELPERSISTENCE_TEMP = 't',        /* temporary table */
    SYS_RELPERSISTENCE_GLOBAL_TEMP = 'g', /* global temporary table */
};

enum PartitionType : char {
    SYS_PARTTYPE_PARTITIONED_RELATION = 'p',       /* partitioned relation */
    SYS_PARTTYPE_SUBPARTITIONED_RELATION = 's',    /* subpartitioned relation */
    SYS_PARTTYPE_VALUE_PARTITIONED_RELATION = 'v', /* value partitioned relation */
    SYS_PARTTYPE_NON_PARTITIONED_RELATION = 'n',   /* non-partitioned relation */
};

enum ReplicaIdentity : char {
    /* default selection for replica identity (primary key or nothing) */
    SYS_REPLICA_IDENTITY_DEFAULT = 'd',
    /* no replica identity is logged for this relation */
    SYS_REPLICA_IDENTITY_NOTHING = 'n',
    /* all columns are loged as replica identity */
    SYS_REPLICA_IDENTITY_FULL = 'f',
};

#pragma pack (push, 1)
struct SysClassTupDef {
    DstoreNameData relname;           /* class name */
    Oid relnamespace;           /* OID of namespace containing this class */
    Oid reltype;                /* OID of entry in pg_type for table's
                                 * implicit row type */
    Oid reloftype;              /* OID of entry in pg_type for underlying
                                 * composite type */
    Oid relowner;               /* class owner */
    Oid relam;                  /* index access method; 0 if not an index */
    Oid relfilenode;            /* physical storage file identifier */

    /* relfilenode == 0 means it is a "mapped" relation, see relmapper.c */
    Oid reltablespace;  /* identifier of table space for relation */
    double relpages;    /* # of blocks (not always up-to-date) */
    double reltuples;   /* # of tuples (not always up-to-date) */
    int32_t relallvisible; /* # of all-visible blocks (not always
                         * up-to-date) */
    Oid reltoastrelid;  /* OID of toast table; 0 if none */
    Oid reltoastidxid;  /* if toast table, OID of chunk_id index */
    Oid reldeltarelid;  /* if ColStore table, it is not 0 */
    Oid reldeltaidx;
    Oid relcudescrelid; /* if ColStore table, it is not 0; if TsStore, it is partition oid */
    Oid relcudescidx;
    bool relhasindex;    /* T if has (or has had) any indexes */
    bool relisshared;    /* T if shared across databases */
    char relpersistence; /* see RELPERSISTENCE_xxx constants below */
    char relkind;        /* see RELKIND_xxx constants below */
    int16_t relnatts;       /* number of user attributes */

    /*
     * Class pg_attribute must contain exactly "relnatts" user attributes
     * (with attnums ranging from 1 to relnatts) for this class.  It may also
     * contain entries with negative attnums for system attributes.
     */
    int16_t relchecks;        /* # of CHECK constraints for class */
    bool relhasoids;       /* T if we generate OIDs for rows of rel */
    bool relhaspkey;       /* has (or has had) PRIMARY KEY index */
    bool relhasrules;      /* has (or has had) any rules */
    bool relhastriggers;   /* has (or has had) any TRIGGERs */
    bool relhassubclass;   /* has (or has had) derived classes */
    int8_t relcmprs;         /* row compression attribution */
    bool relhasclusterkey; /* has (or has had) any PARTIAL CLUSTER KEY */
    bool relrowmovement;   /* enable or disable rowmovement */
    char parttype;         /* 'p' for  partitioned relation, 'n' for non-partitioned relation */
    DSTORE::FileId      relfileid;
    DSTORE::BlockNumber relblknum;
    DSTORE::FileId      rellobfileid;
    DSTORE::BlockNumber rellobblknum;
    uint32_t relfrozenxid; /* all Xids < this are frozen in this rel */

#ifdef CATALOG_VARLEN      /* variable-length fields start here */
    /* NOTE: These fields are not present in the rd_rel field of relcache entry. */
    aclitem relacl[1];  /* access permissions */
    text reloptions[1]; /* access-method-specific options */
    char relreplident;       /* see REPLICA_IDENTITY_xxx constants */
    uint64_t relfrozenxid64; /* all Xids < this are frozen in this rel */
    Oid relbucket;           /* bucket info in pg_hashbucket */
    Int32Vector relbucketkey; /* Column number of hash partition */
    uint64_t relminmxid; /* all multixacts in this rel are >= this.
                          * this is really a MultiXactId */
#endif
};
#pragma pack (pop)

using FormData_pg_class = SysClassTupDef;
using Form_pg_class = SysClassTupDef *;
}  // namespace DSTORE
#endif
