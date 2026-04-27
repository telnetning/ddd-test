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
 * The systable_func is one of core system tables.
 */
#ifndef DSTORE_SYSTABLE_FUNC_H
#define DSTORE_SYSTABLE_FUNC_H
#include <cstdint>
#include "common/dstore_common_utils.h"
#include "catalog/dstore_function_struct.h"

namespace DSTORE {
#pragma GCC visibility push(default)
constexpr char SYSTABLE_FUNC_NAME[] = "sys_func";
constexpr Oid SYSTABLE_FUNC_OID = 1255;
constexpr Oid SYSTABLE_FUNC_ROWTYPE = 81;

constexpr int SYSTABLE_FUNC_ARGS = 8192;

constexpr int NATTS_SYS_FUNC = 39;
constexpr int ANUM_SYS_FUNC_PRONAME = 1;
constexpr int ANUM_SYS_FUNC_PRONAMESPACE = 2;
constexpr int ANUM_SYS_FUNC_PROOWNER = 3;
constexpr int ANUM_SYS_FUNC_PROLANG = 4;
constexpr int ANUM_SYS_FUNC_PROCOST = 5;
constexpr int ANUM_SYS_FUNC_PROROWS = 6;
constexpr int ANUM_SYS_FUNC_PROVARIADIC = 7;
constexpr int ANUM_SYS_FUNC_PROTRANSFORM = 8;
constexpr int ANUM_SYS_FUNC_PROISAGG = 9;
constexpr int ANUM_SYS_FUNC_PROISWINDOW = 10;
constexpr int ANUM_SYS_FUNC_PROSECDEF = 11;
constexpr int ANUM_SYS_FUNC_PROLEAKPROOF = 12;
constexpr int ANUM_SYS_FUNC_PROISSTRICT = 13;
constexpr int ANUM_SYS_FUNC_PRORETSET = 14;
constexpr int ANUM_SYS_FUNC_PROVOLATILE = 15;
constexpr int ANUM_SYS_FUNC_PRONARGS = 16;
constexpr int ANUM_SYS_FUNC_PRONARGDEFAULTS = 17;
constexpr int ANUM_SYS_FUNC_PRORETTYPE = 18;
constexpr int ANUM_SYS_FUNC_PROARGTYPES = 19;
constexpr int ANUM_SYS_FUNC_PROALLARGTYPES = 20;
constexpr int ANUM_SYS_FUNC_PROARGMODES = 21;
constexpr int ANUM_SYS_FUNC_PROARGNAMES = 22;
constexpr int ANUM_SYS_FUNC_PROARGDEFAULTS = 23;
constexpr int ANUM_SYS_FUNC_PROSRC = 24;
constexpr int ANUM_SYS_FUNC_PROBIN = 25;
constexpr int ANUM_SYS_FUNC_PROCONFIG = 26;
constexpr int ANUM_SYS_FUNC_PROACL = 27;
constexpr int ANUM_SYS_FUNC_PRODEFAULTARGPOS = 28;
constexpr int ANUM_SYS_FUNC_FENCED = 29;
constexpr int ANUM_SYS_FUNC_SHIPPABLE = 30;
constexpr int ANUM_SYS_FUNC_PACKAGE = 31;
constexpr int ANUM_SYS_FUNC_PROKIND = 32;
constexpr int ANUM_SYS_FUNC_PROARGSRC = 33;
constexpr int ANUM_SYS_FUNC_PACKAGEID = 34;
constexpr int ANUM_SYS_FUNC_PROISPRIVATE = 35;
constexpr int ANUM_SYS_FUNC_PROARGTYPESEXT = 36;
constexpr int ANUM_SYS_FUNC_PRODEFAULTARGPOSEXT = 37;
constexpr int ANUM_SYS_FUNC_ALLARGTYPES = 38;
constexpr int ANUM_SYS_FUNC_ALLARGTYPESEXT = 39;
/* func_oid is only for builitin
 * func view shouldn't be included in Natts_sys_func
 */
constexpr int ANUM_SYS_FUNC_OID = 40;

enum SysTPFuncArgMode : char {
    SYS_FUNCARGMODE_IN = 'i',
    SYS_FUNCARGMODE_OUT = 'o',
    SYS_FUNCARGMODE_INOUT = 'b',
    SYS_FUNCARGMODE_VARIADIC = 'v',
    SYS_FUNCARGMODE_TABLE = 't',
};


constexpr Oid  FUNC_RECORD_IN = 2290;
constexpr Oid  FUNC_RECORD_OUT = 2291;
constexpr Oid  FUNC_RECORD_RECV = 2402;
constexpr Oid  FUNC_RECORD_SEND  = 2403;
constexpr Oid  FUNC_OID_INPUT = 1798;
struct SysFuncTupDef {
    DstoreNameData    proname;         /* procedure name */
    Oid         pronamespace;    /* OID of namespace containing this proc */
    Oid         proowner;        /* procedure owner */
    Oid         prolang;         /* OID of pg_language entry */
    float       procost;         /* estimated execution cost */
    float       prorows;         /* estimated # of rows out (if proretset) */
    Oid         provariadic;     /* element type of variadic array, or 0 */
    RegFunc     protransform;    /* transforms calls to it during planning */
    bool        proisagg;        /* is an aggregate? */
    bool        proiswindow;     /* is a window function? */
    bool        prosecdef;       /* security definer */
    bool        proleakproof;    /* is a leak-proof function? */
    bool        proisstrict;     /* strict with respect to NULLs? */
    bool        proretset;       /* returns a set? */
    char        provolatile;     /* see PROVOLATILE_ categories below */
    int16_t     pronargs;        /* number of arguments */
    int16_t     pronargdefaults; /* number of arguments with defaults */
    Oid         prorettype;      /* OID of result type */

    /*
     * variable-length fields start here, but we allow direct access to
     * proargtypes
     */
    OidVector    proargtypes;    /* parameter types (excludes OUT params) */
  
#ifdef CATALOG_VARLEN
    Oid         proallargtypes[1];        /* all param types (NULL if IN only) */
    char        proargmodes[1];           /* parameter modes (NULL if IN only) */
    text        proargnames[1];           /* parameter names (NULL if no names) */
    pg_node_tree proargdefaults;          /* expression trees list for argument
                                           * defaults (NULL if none) */
    text        prosrc;                   /* procedure source text */
    text        probin;                   /* secondary procedure information (can be NULL) */
    text        proconfig[1];             /* procedure-local GUC settings */
    aclitem     proacl[1];                /* access permissions */
    int2vector  prodefaultargpos;
    bool        fencedmode;
    bool        proshippable;    /* if provolatile isn't 'i', proshippable will determine if the func can be shipped */
    bool        propackage;
    char        prokind;         /* see PROKIND_ categories below */
    text        proargsrc;    /* procedure header source text before keyword AS/IS */
    Oid         propackageid;    /* OID of package containing this procedure */
    bool        proisprivate;
    oidvector_extend proargtypesext;
    int2vector_extend prodefaultargposext;
    oidvector allargtypes;   /* all param types */

    oidvector_extend allargtypesext;
#endif
};
#pragma GCC visibility push(default)
/*
 * Register the builtin functions to the Dstore.
 * Note:
 *     Used only in BootStrap.
 */
extern RetStatus FmgrRegister(FmgrBuiltin *funcBuiltin, int num);
/*
 * Unregister the builtin functions to the Dstore.
 * Note:
 *     Used only in Embedded.
 */
extern void FmgrUnregister();
#pragma GCC visibility pop
}  // namespace DSTORE

#endif