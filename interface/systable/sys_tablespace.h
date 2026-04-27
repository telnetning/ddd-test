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
 * definition of the system "tablespace" relation (sys_tablespace)
 * along with the relation's initial contents.
 * sequences, views, materialized views, composite types.
 */

#ifndef SYS_TABLESPACE_H
#define SYS_TABLESPACE_H

#include <cstdint>
#include "common/dstore_common_utils.h"
namespace DSTORE {
/* ----------------
 *        sys_tablespace definition.  cpp turns this into
 *        typedef struct SysTableSpaceDef
 * ----------------
 */
constexpr Oid SYS_TABLESPACE_RELATION_ID = 1213;
constexpr Oid SYS_TABLESPACE_RELATION_ROWTYPE_ID = 11633;

struct SysTableSpaceDef {
    DstoreNameData  spcname;        /* name of tablespace */
    Oid       spcowner;       /* owner of tablespace */
 
#ifdef CATALOG_VARLEN         /* variable-length fields */
    aclitem   spcacl[1];      /* access permissions */
    text      spcoptions[1];  /* per-tablespace options */
    text      spcmaxsize;     /* max size of tablespace */
    bool      relative;       /* relative location */
#endif
};

/* ----------------
 *        compiler constants for sys_tablespace
 * ----------------
 */
constexpr int NATTS_SYS_TABLESPACE = 6;
constexpr int ANUM_SYS_TABLESPACE_SPCNAME = 1;
constexpr int ANUM_SYS_TABLESPACE_SPCOWNER = 2;
constexpr int ANUM_SYS_TABLESPACE_SPCACL = 3;
constexpr int ANUM_SYS_TABLESPACE_SPCOPTIONS = 4;
constexpr int ANUM_SYS_TABLESPACE_MAXSIZE = 5;
constexpr int ANUM_SYS_TABLESPACE_RELATIVE = 6;

constexpr char ROOT_GLOBAL_NAME[] = "root_global";
constexpr char ROOT_DEFAULT_NAME[] = "root_default";
constexpr char TEMPLATE_GLOBAL_NAME[] = "pg_global";
constexpr char TEMPLATE_DEFAULT_NAME[] = "pg_default";
}

#endif   /* SYS_TABLESPACE_H */

