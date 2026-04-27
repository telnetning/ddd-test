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
 * definition of the system "partition" relation (sys_partition)
 * along with the relation's initial contents.
 */
#ifndef SYS_PARTITION_H
#define SYS_PARTITION_H

#include <cstdint>
#include "common/dstore_common_utils.h"
namespace DSTORE {
/* ----------------
 *        sys_partition definition.  cpp turns this into
 *        typedef struct SysDatabaseDef
 * ----------------
 */
constexpr Oid SYS_PARTITION_RELATION_ID = 1262;
constexpr Oid SYS_PARTITION_RELATION_ROWTYPE_ID = 1248;

#pragma pack (push, 1)
struct SysPartitionDef {
    DstoreNameData      relname;
    char                parttype;
    Oid                 parentid;
    int32_t             rangenum;
    int32_t             intervalnum;
    char                partstrategy;
    Oid                 relfilenode;
    DSTORE::FileId      relfileid;
    DSTORE::BlockNumber relblknum;
    DSTORE::FileId      rellobfileid;
    DSTORE::BlockNumber rellobblknum;
    Oid                 reltablespace;
    double              relpages;
    double              reltuples;
    int32_t             relallvisible;
    Oid                 reltoastrelid;
    Oid                 reltoastidxid;
    Oid                 indextblid;     /* index partition's table partition's oid */
    bool                indisusable;   /* is this index partition usable for insert and select? */
                                           /* if yes, insert and select should ignore this index partition */
    Oid                 reldeltarelid;  /* if ColStore table, it is not 0 */
    Oid                 reldeltaidx;
    Oid                 relcudescrelid; /* if ColStore table, it is not 0 */
    Oid                 relcudescidx;
    uint32_t            relfrozenxid;
    int32_t             intspnum;
#ifdef CATALOG_VARLEN
    int2vector          partkey;
    OidVector           intervaltablespace;
    text                interval[1];
    text                boundaries[1];
    text                transit[1];
    text                reloptions[1];    /* access-method-specific options */
    uint64_t            relfrozenxid64;
    uint64_t            relminmxid;     /* all multixacts in this rel are >= this.
                                   * this is really a MultiXactId */
    int32_t partitionno;                /* An unique identifier of each partition,
                                         * see pg_partition_fn.h for more detail */
    int32_t             subpartitionno; /* An unique identifier of each subpartition */
#endif
};
#pragma pack (pop)

/* Size of fixed part of sys_partition tuples, not counting var-length fields */
constexpr int SYS_PARTITION_TUPLE_SIZE = (offsetof(SysPartitionDef, intspnum) + sizeof(int32_t));

constexpr char SYS_PART_OBJ_TYPE_PARTED_TABLE = 'r';
constexpr char SYS_PART_OBJ_TYPE_TOAST_TABLE = 't';
constexpr char SYS_PART_OBJ_TYPE_TABLE_PARTITION = 'p';
constexpr char SYS_PART_OBJ_TYPE_TABLE_SUB_PARTITION = 's';
constexpr char SYS_PART_OBJ_TYPE_INDEX_PARTITION = 'x';

/* ----------------
 *        compiler constants for sys_partition
 * ----------------
 */
constexpr int NATTS_SYS_PARTITION = 35;
constexpr int ANUM_SYS_PARTITION_RELNAME = 1;
constexpr int ANUM_SYS_PARTITION_PARTTYPE = 2;
constexpr int ANUM_SYS_PARTITION_PARENTID = 3;
constexpr int ANUM_SYS_PARTITION_RANGENUM = 4;
constexpr int ANUM_SYS_PARTITION_INTERVALNUM = 5;
constexpr int ANUM_SYS_PARTITION_PARTSTRATEGY = 6;
constexpr int ANUM_SYS_PARTITION_RELFILENODE = 7;
constexpr int ANUM_SYS_PARTITION_RELFILEID = 8;
constexpr int ANUM_SYS_PARTITION_RELBLKNUM = 9;
constexpr int ANUM_SYS_PARTITION_RELLOBFILEID = 10;
constexpr int ANUM_SYS_PARTITION_RELLOBBLKNUM = 11;
constexpr int ANUM_SYS_PARTITION_RELTABLESPACE = 12;
constexpr int ANUM_SYS_PARTITION_RELPAGES = 13;
constexpr int ANUM_SYS_PARTITION_RELTUPLES = 14;
constexpr int ANUM_SYS_PARTITION_RELALLVISIBLE = 15;
constexpr int ANUM_SYS_PARTITION_RELTOASTRELID = 16;
constexpr int ANUM_SYS_PARTITION_RELTOASTIDXID = 17;
constexpr int ANUM_SYS_PARTITION_INDEXTBLID = 18;
constexpr int ANUM_SYS_PARTITION_INDISUSABLE = 19;
constexpr int ANUM_SYS_PARTITION_DELTARELID = 20;
constexpr int ANUM_SYS_PARTITION_RELDELTAIDX = 21;
constexpr int ANUM_SYS_PARTITION_RELCUDESCRELID = 22;
constexpr int ANUM_SYS_PARTITION_RELCUDESCIDX = 23;
constexpr int ANUM_SYS_PARTITION_RELFROZENXID = 24;
constexpr int ANUM_SYS_PARTITION_INTSPNUM = 25;
constexpr int ANUM_SYS_PARTITION_PARTKEY = 26;
constexpr int ANUM_SYS_PARTITION_INTABLESPACE = 27;
constexpr int ANUM_SYS_PARTITION_INTERVAL = 28;
constexpr int ANUM_SYS_PARTITION_BOUNDARIES = 29;
constexpr int ANUM_SYS_PARTITION_TRANSIT = 30;
constexpr int ANUM_SYS_PARTITION_RELOPTIONS = 31;
constexpr int ANUM_SYS_PARTITION_RELFROZENXID64 = 32;
constexpr int ANUM_SYS_PARTITION_RELMINMXID = 33;
constexpr int ANUM_SYS_PARTITION_PARTITIONNO = 34;
constexpr int ANUM_SYS_PARTITION_SUBPARTITIONNO = 35;

}
#endif   /* SYS_PARTITION_H */

