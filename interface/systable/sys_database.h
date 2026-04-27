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
 * definition of the system "database" relation (sys_database)
 * along with the relation's initial contents.
 */
#ifndef SYS_DATABASE_H
#define SYS_DATABASE_H

#include <cstdint>
#include "common/dstore_common_utils.h"
namespace DSTORE {
/* ----------------
 *        sys_database definition.  cpp turns this into
 *        typedef struct SysDatabaseDef
 * ----------------
 */
constexpr Oid SYS_DATABASE_RELATION_ID = 1262;
constexpr Oid SYS_DATABASE_RELATION_ROWTYPE_ID = 1248;
constexpr char DEFAULT_SYS_VFS_NAME[] = "VFS";

#pragma pack(push, 1)
struct SysDatabaseDef {
    DstoreNameData    datname;              /* database name */
    Oid               datdba;               /* owner of database */
    int32_t           encoding;             /* character encoding */
    DstoreNameData    datcollate;           /* LC_COLLATE setting */
    DstoreNameData    datctype;             /* LC_CTYPE setting */
    bool              datistemplate;        /* allowed as CREATE DATABASE template? */
    bool              datallowconn;         /* new connections allowed? */
    int32_t           datconnlimit;         /* max connections allowed (-1=no limit) */
    Oid               datlastsysoid;        /* highest OID to consider a system OID */
    uint32_t          datfrozenxid;  /* all Xids < this are frozen in this DB */
    Oid               dattablespace;        /* default table space for this DB */
    DstoreNameData    datcompatibility;
    char              smartroutepolicy;
#ifdef CATALOG_VARLEN            /* variable-length fields start here */
    aclitem        datacl[1];        /* access permissions */
    uint64_t datfrozenxid64; /* all Xids < this are frozen in this DB */
    uint64_t datminmxid; /* all multixacts in the DB are >= this */
    DstoreNameData dattimezone;
    char dattype;
#endif
};
#pragma pack (pop)

/* Size of fixed part of sys_database tuples, not counting var-length fields */
constexpr int SYS_DATABASE_TUPLE_SIZE = (offsetof(SysDatabaseDef, datcompatibility) + sizeof(DstoreNameData));
     
/* ----------------
 *        compiler constants for sys_database
 * ----------------
 */
constexpr int NATTS_SYS_DATABASE = 18;
constexpr int ANUM_SYS_DATABASE_DATNAME = 1;
constexpr int ANUM_SYS_DATABASE_DATDBA = 2;
constexpr int ANUM_SYS_DATABASE_ENCODING = 3;
constexpr int ANUM_SYS_DATABASE_DATCOLLATE = 4;
constexpr int ANUM_SYS_DATABASE_DATCTYPE = 5;
constexpr int ANUM_SYS_DATABASE_DATISTEMPLATE = 6;
constexpr int ANUM_SYS_DATABASE_DATALLOWCONN = 7;
constexpr int ANUM_SYS_DATABASE_DATCONNLIMIT = 8;
constexpr int ANUM_SYS_DATABASE_DATLASTSYSOID = 9;
constexpr int ANUM_SYS_DATABASE_DATFROZENXID = 10;
constexpr int ANUM_SYS_DATABASE_DATTABLESPACE = 11;
constexpr int ANUM_SYS_DATABASE_COMPATIBILITY = 12;
constexpr int ANUM_SYS_DATABASE_SMARTROUTEPOLICY = 13;
constexpr int ANUM_SYS_DATABASE_DATACL = 14;
constexpr int ANUM_SYS_DATABASE_DATFROZENXID64 = 15;
constexpr int ANUM_SYS_DATABASE_DATMINMXID = 16;
constexpr int ANUM_SYS_DATABASE_DATTIMEZONE = 17;
constexpr int ANUM_SYS_DATABASE_DATTYPE = 18;

constexpr Oid TEMPLATE_DB_OID = 1;
constexpr Oid ROOT_DB_OID = 3;
constexpr char TEMPLATE1_SYS_DATABASE[] = "template1";
constexpr char TEMPLATE0_SYS_DATABASE[] = "template0";
constexpr char DEFAULT_SYS_DATABASE[] = "postgres";
constexpr char TEMPLATEA_SYS_DATABASE[] = "templatea";
constexpr char TEMPLATEM_SYS_DATABASE[] = "templatem";
}
#endif   /* SYS_DATABASE_H */

