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
 * dstore_function_struct.h
 *     This file defines the building blocks that bg page writer needs.
 *
 * IDENTIFICATION
 *        interface/catalog/dstore_function_struct.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef STRUCT_FUNCTION_STRUCT_H
#define STRUCT_FUNCTION_STRUCT_H
#include <functional>
#include "common/dstore_common_utils.h"

namespace DSTORE {
using DstoreMemoryContext = struct DstoreMemoryContextData *;
using FunctionCallInfo = struct FunctionCallInfoData *;
using PGFunction = Datum (*)(FunctionCallInfo fcinfo);
using CallbackFunc = std::function<Datum(FunctionCallInfo fcinfo)>;
#define NAMEDATALEN 64

/*
 * The structure must be identical to the structure in SQL Engine.
 */
struct FmgrInfo {
    PGFunction fnAddr;     /* pointer to function or handler to be called */
    Oid fnOid;             /* OID of function (NOT of handler, if any) */
    short fnNargs;         /* number of input args (0..FUNC_MAX_ARGS) */
    bool fnStrict;         /* function is "strict" (NULL in => NULL out) */
    bool fnRetset;         /* function returns a set */
    void *fnExtra;         /* extra space for use by handler */
    void* fnMcxt;  /* memory context to store fnExtra in */
    void *fnExpr;          /* expression parse tree for call, or NULL */
    Oid fn_rettype;           /* Oid of function return type */
    Oid fn_rettypemod;        /* Oid of the function returnt typmod */
    char fnName[NAMEDATALEN]; /* function name */
    char* fnLibPath;          /* library path for c-udf
                               * package.class.method(args) for java-udf */
    // Vector Function
    void* vec_fn_addr;
    void* vec_fn_cache;
    void* genericRuntime;
    uint32_t max_length;
    Oid fn_languageId;          /* function language id */
    unsigned char fn_stats;     /* collect stats if track_functions > this */
    bool fn_fenced;
    char fn_volatile;           /* procvolatile */
    uint8_t decimals;
};
/*
 * This table stores info about all the built-in functions (ie, functions
 * that are compiled into the Postgres executable).
 */
struct FmgrBuiltin {
    Oid         foid;           /* OID of the function */
    short       nargs;          /* 0..FUNC_MAX_ARGS, or -1 if variable count */
    bool        strict;         /* T if function is "strict" */
    bool        retset;         /* T if function returns a set */
    const char *funcName;       /* C name of the function */
    PGFunction  func;           /* pointer to compiled function */
    Oid         rettype;        // OID of result type
};
}  // namespace DSTORE
#endif /* STRUCT_FUNCTION_STRUCT_H */
