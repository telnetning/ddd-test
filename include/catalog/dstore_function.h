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
 * dstore_function.h
 *
 * IDENTIFICATION
 *        include/catalog/dstore_function.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_GAUSSKERNEL_INCLUDE_CATALOG_DSTORE_FUNCTION_H_
#define SRC_GAUSSKERNEL_INCLUDE_CATALOG_DSTORE_FUNCTION_H_


#include "common/dstore_datatype.h"
#include "common/memory/dstore_mctx.h"
#include "catalog/dstore_function_struct.h"
#include "index/dstore_index_struct.h"

namespace DSTORE {

/*
 * number of prealloced arguments to a function.
 * In deepsql (SVM and elastic_net), we cannot explicitly set all elements to false.
 */
constexpr uint16 FUNC_PREALLOCED_ARGS = 20;

typedef struct FunctionCallInfoData {
    FmgrInfo* flinfo;                            /* ptr to lookup info used for this call */
    void* context;                               /* pass info about context of call */
    void* resultinfo;                            /* pass or return extra info about result */
    Oid fncollation;                             /* collation for function to use */
    bool isnull;                                 /* function must set true if result is NULL */
    short nargs;                                 /* # arguments actually passed */
    Datum* arg;                                  /* Arguments passed to function */
    bool* argnull;                               /* T if arg[i] is actually NULL */
    Oid* argTypes;                               /* Argument type */
    Datum prealloc_arg[FUNC_PREALLOCED_ARGS];    /* prealloced arguments. */
    bool prealloc_argnull[FUNC_PREALLOCED_ARGS]; /* prealloced argument null flags. */
    Oid prealloc_argTypes[FUNC_PREALLOCED_ARGS]; /* prealloced argument type */
} FunctionCallInfoData;

#define DstoreInitFunctionCallInfoData(Fcinfo, Flinfo, Nargs, Collation, Context, Resultinfo) \
    do {                                                                                \
        (Fcinfo).flinfo = (Flinfo);                                                     \
        (Fcinfo).nargs = (Nargs);                                                       \
        (Fcinfo).fncollation = (Collation);                                             \
        (Fcinfo).context = (Context);                                                   \
        (Fcinfo).resultinfo = (Resultinfo);                                             \
        (Fcinfo).isnull = false;                                                        \
        if ((Nargs) > FUNC_PREALLOCED_ARGS) {                                           \
            (Fcinfo).arg = static_cast<Datum *>(DstorePalloc0(static_cast<uint16>(Nargs) * sizeof(Datum)));          \
            (Fcinfo).argnull = static_cast<bool *>(DstorePalloc0(static_cast<uint16>(Nargs) * sizeof(bool)));        \
            (Fcinfo).argTypes = static_cast<Oid *>(DstorePalloc0(static_cast<uint16>(Nargs) * sizeof(Oid)));         \
        } else {                                                                        \
            (Fcinfo).arg = (Fcinfo).prealloc_arg;                                       \
            (Fcinfo).argnull = (Fcinfo).prealloc_argnull;                               \
            (Fcinfo).argTypes = (Fcinfo).prealloc_argTypes;                             \
        }                                                                               \
    } while (0)

/*
 * This macro invokes a function given a filled-in FunctionCallInfoData
 * structure.  The macro result is the returned Datum --- but note that
 * the caller must still check fcinfo->isnull!  Also, if function is strict,
 * it is caller's responsibility to verify that no null arguments are present
 * before calling.
 */
#define FunctionCallInvoke(fcinfo) ((*(fcinfo)->flinfo->fnAddr)(fcinfo))

Datum FunctionCall1Coll(FmgrInfo* flinfo, Oid collation, Datum arg1);
Datum FunctionCall2Coll(FmgrInfo* flinfo, Oid collation, Datum arg1, Datum arg2);

/* Values for track_functions GUC variable --- order is significant! */
typedef enum TrackFunctionsLevel {
    TRACK_FUNC_OFF,
    TRACK_FUNC_PL,
    TRACK_FUNC_ALL
} TrackFunctionsLevel;

/* the fmgr_builtins table */
extern FmgrBuiltin *g_fmgrBuiltins;
extern uint16 *g_fmgrBuiltinOidIndex;
extern int g_fmgrNBuiltins;
#define DSTORE_INVALID_OID_BUILTIN_MAPPING (0xFFFF)

/*
 * This routine fills a FmgrInfo struct, given the OID
 * of the function to be called.
 */
void fill_fmgr_info(Oid functionId, FmgrInfo *finfo);

void fill_fmgr_info(Oid functionId, FmgrInfo *finfo, DstoreMemoryContext mcxt);

/*
 * This routine fills a FmgrInfo struct, given the OID
 * of the function to be called.
 *
 * The caller's g_dstoreCurrentMemoryContext is used as the fnMcxt of the info
 * struct; this means that any subsidiary data attached to the info struct
 * (either by fmgr_info itself, or later on by a function call handler)
 * will be allocated in that context.  The caller must ensure that this
 * context is at least as long-lived as the info struct itself.  This is
 * not a problem in typical cases where the info struct is on the stack or
 * in freshly-palloc'd space.  However, if one intends to store an info
 * struct in a long-lived table, it's better to use fmgr_info_cxt.
 */
void fmgr_info(Oid functionId, FmgrInfo *finfo);

/*
 * Fill a FmgrInfo struct, specifying a memory context in which its
 * subsidiary data should go.
 */
void fmgr_info_cxt(Oid functionId, FmgrInfo *finfo, DstoreMemoryContext mcxt);

/*
 * This one does the actual work.  ignore_security is ordinarily false
 * but is set to true when we need to avoid recursion.
 */
void fmgr_info_cxt_security(Oid functionId, FmgrInfo *finfo, DstoreMemoryContext mcxt, bool ignore_security);

extern void fmgr_info_copy(FmgrInfo *dstinfo, FmgrInfo *srcinfo, DstoreMemoryContext destcxt);
extern const FmgrBuiltin* FmgrIsbuiltin(Oid id);
extern uint16 *g_fmgrBuiltinOidIndex;
RetStatus RegisterBuiltinFuncs4CoreSystable();
extern void FillProcFmgrInfo(const IndexSupportProcInfo *procInfo, Oid typeOid, AttrNumber attNum, uint16 procNum,
    FmgrInfo &fmgrInfo);
extern void FillProcFmgrInfo(const IndexSupportProcInfo *procInfo, Oid funcOid, uint16 procNum, FmgrInfo &fmgrInfo,
    AttrNumber attNum);
extern void FillOpfamilyProcFmgrInfo(IndexSupportProcInfo *procInfo, Oid leftTypeOid, Oid rightTypeOid,
    AttrNumber attNum, FmgrInfo &fmgrInfo);
extern void FillOpfamilyStratFmgrInfo(IndexSupportProcInfo *procInfo, Oid leftTypeOid, Oid rightTypeOid,
    AttrNumber attNum, uint16 strat, FmgrInfo &fmgrInfo);
extern void FillOuputFmgrInfo(Oid typeOid, FmgrInfo &fmgrInfo, GetOutputFuncCb outputCb);
}

#endif /* SRC_GAUSSKERNEL_INCLUDE_CATALOG_STORAGE_FUNCTION_H_ */
