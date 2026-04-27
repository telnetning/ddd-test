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
 * dstore_function.cpp
 *     This file defines the building blocks that bg page writer needs.
 *
 * IDENTIFICATION
 *        src/catalog/dstore_function.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include <mutex>
#include "securec.h"

#include "catalog/dstore_typecache.h"
#include "systable/systable_type.h"
#include "systable/systable_func.h"
#include "catalog/dstore_fmgr.h"

namespace DSTORE {
/* the fmgr_builtins table */
FmgrBuiltin *g_fmgrBuiltins = nullptr;
/* number of entries in table */
int g_fmgrNBuiltins = 0;
/* highest function OID in table */
const Oid g_fmgrLastBuiltinOid = 10000;
uint16 *g_fmgrBuiltinOidIndex = nullptr;
std::mutex g_fmgrLock;

/*
 * Lookup routines for builtin-function table.  We can search by either Oid
 * or name, but search by Oid is much faster.
 */
const FmgrBuiltin *FmgrIsbuiltin(Oid id)
{
    uint16 index;

    /* fast lookup only possible if original oid still assigned */
    if (id >= g_fmgrLastBuiltinOid || !g_fmgrBuiltinOidIndex || !g_fmgrBuiltins) {
        return nullptr;
    }

    /*
     * Lookup function data. If there's a miss in that range it's likely a
     * nonexistent function, returning NULL here will trigger an ERROR later.
     */
    index = g_fmgrBuiltinOidIndex[id];
    if (index == 0) {
        return nullptr;
    }

    return &g_fmgrBuiltins[index - 1];
}

/*
 * This routine fills a FmgrInfo struct, given the OID
 * of the function to be called.
 */
void fill_fmgr_info(Oid functionId, FmgrInfo *finfo, DstoreMemoryContext mcxt)
{
    PGFunction fnAddr = g_storageInstance->GetCacheHashMgr()->GetFuncCacheFromFnOid(functionId).fnAddr;
    if (unlikely(fnAddr == nullptr)) {
        finfo->fnAddr = nullptr;
        finfo->fnOid = DSTORE_INVALID_OID;
        return;
    }
    /*
     * fnOid *must* be filled in last.  Some code assumes that if fnOid is
     * valid, the whole struct is valid.  Some FmgrInfo struct's do survive
     * elogs.
     */
    finfo->fnExtra = nullptr;
    finfo->fnMcxt = mcxt;
    finfo->fnExpr = nullptr;

    finfo->fnNargs = 2;
    finfo->fnStrict = true;
    finfo->fnRetset = false;
    finfo->fn_stats = TRACK_FUNC_ALL; /* ie, never track */
    finfo->fnAddr = fnAddr;
    finfo->fnOid = functionId;
}

void fill_fmgr_info(Oid functionId, FmgrInfo *finfo)
{
    fill_fmgr_info(functionId, finfo, nullptr);
}

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
void fmgr_info(Oid functionId, FmgrInfo *finfo)
{
    fmgr_info_cxt_security(functionId, finfo, g_dstoreCurrentMemoryContext, false);
}

/*
 * Fill a FmgrInfo struct, specifying a memory context in which its
 * subsidiary data should go.
 */
void fmgr_info_cxt(Oid functionId, FmgrInfo *finfo, DstoreMemoryContext mcxt)
{
    fmgr_info_cxt_security(functionId, finfo, mcxt, false);
}

/*
 * This one does the actual work.  ignore_security is ordinarily false
 * but is set to true when we need to avoid recursion.
 */
void fmgr_info_cxt_security(Oid functionId, FmgrInfo *finfo, DstoreMemoryContext mcxt,
                            bool ignore_security)
{
    (void)ignore_security;

    const FmgrBuiltin *fbp;

    /*
     * fnOid *must* be filled in last.  Some code assumes that if fnOid is
     * valid, the whole struct is valid.  Some FmgrInfo struct's do survive
     * elogs.
     */
    finfo->fnOid = DSTORE_INVALID_OID;
    finfo->fnExtra = nullptr;
    finfo->fnMcxt = mcxt;
    finfo->fnExpr = nullptr; /* caller may set this later */

    if ((fbp = FmgrIsbuiltin(functionId)) != nullptr) {
        /*
         * Fast path for builtin functions: don't bother consulting pg_proc
         */
        finfo->fnNargs = fbp->nargs;
        finfo->fnStrict = fbp->strict;
        finfo->fnRetset = fbp->retset;
        finfo->fn_stats = TRACK_FUNC_ALL; /* ie, never track */
        finfo->fnAddr = fbp->func;
        finfo->fnOid = functionId;
        return;
    }

    /* Otherwise we need the pg_proc entry */

    /*
     * If it has prosecdef set, non-null proconfig, or if a plugin wants to
     * hook function entry/exit, use fmgr_security_definer call handler ---
     * unless we are being called again by fmgr_security_definer or
     * fmgr_info_other_lang.
     *
     * When using fmgr_security_definer, function stats tracking is always
     * disabled at the outer level, and instead we set the flag properly in
     * fmgr_security_definer's private flinfo and implement the tracking
     * inside fmgr_security_definer.  This loses the ability to charge the
     * overhead of fmgr_security_definer to the function, but gains the
     * ability to set the track_functions GUC as a local GUC parameter of an
     * interesting function and have the right things happen.
     *
     * ignore_security
     */
}

/*
 * These are for invocation of a previously-looked-up function with a
 * directly-computed parameter list.  Note that neither arguments nor result
 * are allowed to be NULL.
 */
Datum FunctionCall1Coll(FmgrInfo* flinfo, Oid collation, Datum arg1)
{
    FunctionCallInfoData fcinfo;
    Datum result;

    DstoreInitFunctionCallInfoData(fcinfo, flinfo, 1, collation, nullptr, nullptr);

    fcinfo.arg[0] = arg1;
    fcinfo.argnull[0] = false;

    result = FunctionCallInvoke(&fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (fcinfo.isnull) {
        ErrLog(DSTORE_ERROR, MODULE_COMMON,
               ErrMsg("function[%d]\"%s\" returns null, arg1=%lu. err[%lld]\"%s\"",
                      flinfo->fnOid, flinfo->fnName, arg1, thrd->GetErrorCode(), thrd->GetErrorMessage()));
        storage_set_error(COMMON_ERROR_FUNCTION_RETURN_NULL, flinfo->fnOid);
        StorageAssert(0);
    }

    return result;
}

Datum FunctionCall2Coll(FmgrInfo* flinfo, Oid collation, Datum arg1, Datum arg2)
{
    /*
     * XXX if you change this routine, see also the inlined version in
     * utils/sort/tuplesort.c!
     */
    FunctionCallInfoData fcinfo;
    Datum result;

    DstoreInitFunctionCallInfoData(fcinfo, flinfo, 2, collation, nullptr, nullptr);

    fcinfo.arg[0] = arg1;
    fcinfo.arg[1] = arg2;
    fcinfo.argnull[0] = false;
    fcinfo.argnull[1] = false;

    result = FunctionCallInvoke(&fcinfo);

    /* Check for null result, since caller is clearly not expecting one */
    if (fcinfo.isnull) {
        ErrLog(DSTORE_ERROR, MODULE_COMMON,
               ErrMsg("function[%d]\"%s\" returns null, arg1=%lu, arg2=%lu. err[%lld]\"%s\"",
                      flinfo->fnOid, flinfo->fnName, arg1, arg2, thrd->GetErrorCode(), thrd->GetErrorMessage()));
        storage_set_error(COMMON_ERROR_FUNCTION_RETURN_NULL, flinfo->fnOid);
        StorageAssert(0);
    }

    return result;
}

RetStatus FmgrRegister(FmgrBuiltin *funcBuiltin, int num)
{
    std::lock_guard<std::mutex> lock(g_fmgrLock);

    if (!funcBuiltin || num == 0) {
        return DSTORE_SUCC;
    }

    if (g_fmgrBuiltins == nullptr) {
        g_fmgrBuiltins = static_cast<FmgrBuiltin *>(
            DstorePalloc0(static_cast<uint>(g_fmgrLastBuiltinOid) * sizeof(FmgrBuiltin)));
        if (unlikely(g_fmgrBuiltins == nullptr)) {
            return DSTORE_FAIL;
        }
    }

    if (g_fmgrBuiltinOidIndex == nullptr) {
        g_fmgrBuiltinOidIndex =
            static_cast<uint16_t *>(DstorePalloc0(static_cast<size_t>(g_fmgrLastBuiltinOid) * sizeof(uint16_t)));
        if (unlikely(g_fmgrBuiltinOidIndex == nullptr)) {
            DstorePfree(g_fmgrBuiltins);
            return DSTORE_FAIL;
        }
    }

    for (int i = 0; i < num; i++) {
        if (funcBuiltin[i].foid >= g_fmgrLastBuiltinOid) {
            continue;
        }
        int32_t idx = static_cast<int32_t>(g_fmgrBuiltinOidIndex[funcBuiltin[i].foid]) - 1;
        if (idx >= 0) {
            g_fmgrBuiltins[idx] = funcBuiltin[i];
        } else {
            idx = g_fmgrNBuiltins + i;
            g_fmgrBuiltins[idx] = funcBuiltin[i];
            uint32_t oidIndex = static_cast<uint32_t>(g_fmgrBuiltins[idx].foid);
            g_fmgrBuiltinOidIndex[oidIndex] = static_cast<uint16_t>(idx + 1);
        }
    }

    g_fmgrNBuiltins += num;

    return DSTORE_SUCC;
}

void FmgrUnregister()
{
    std::lock_guard<std::mutex> lock(g_fmgrLock);
    DstorePfreeExt(g_fmgrBuiltins);
    DstorePfreeExt(g_fmgrBuiltinOidIndex);
    g_fmgrNBuiltins = 0;
}

static FmgrBuiltin g_builtinFuncs4CoreSystable[] = {
    {1242, 1, true, false, "boolin", static_cast<PGFunction>(BoolIn), 16},
    {1245, 1, true, false, "charin", static_cast<PGFunction>(CharIn), 18},
    {34, 1, true, false, "namein", static_cast<PGFunction>(NameIn), 19},
    {38, 1, true, false, "int2in", static_cast<PGFunction>(Int2In), 21},
    {42, 1, true, false, "int4in", static_cast<PGFunction>(Int4In), 23},
    {1798, 1, true, false, "oidin", static_cast<PGFunction>(OidIn), 26},
    {50, 1, true, false, "xidin", static_cast<PGFunction>(XidIn), 28},
    {58, 1, true, false, "xidin4", static_cast<PGFunction>(XidIn4), 31},
    {214, 1, true, false, "float8in", static_cast<PGFunction>(Float8In), 701},
    {5541, 1, true, false, "int1in", static_cast<PGFunction>(Int1In), 5545},
};
constexpr int BUILTIN_FUNCS_CNT = sizeof(g_builtinFuncs4CoreSystable) / sizeof(g_builtinFuncs4CoreSystable[0]);

RetStatus RegisterBuiltinFuncs4CoreSystable()
{
    return FmgrRegister(g_builtinFuncs4CoreSystable, BUILTIN_FUNCS_CNT);
}

void FillProcFmgrInfo(const IndexSupportProcInfo *procInfo, Oid typeOid, AttrNumber attNum, uint16 procNum,
    FmgrInfo &fmgrInfo)
{
    // fill fmgrInfo from dstore cache if exists
    fmgrInfo.fnOid = g_storageInstance->GetCacheHashMgr()->GetFnOidFromArgType(typeOid, typeOid, procNum);
    if (fmgrInfo.fnOid != DSTORE_INVALID_OID) {
        fill_fmgr_info(fmgrInfo.fnOid, &fmgrInfo);
        return;
    }

    // fill fmgrInfo from procInfo if exists
    if (procInfo != nullptr && procInfo->supportFmgrInfo != nullptr) {
        FmgrInfo *locinfo = procInfo->supportFmgrInfo;
        int procindex = (procInfo->numSupportProc * (attNum - 1)) + (ConvertToSqlProcNum(procNum) - 1);
        locinfo += procindex;
        if (locinfo->fnOid != DSTORE_INVALID_OID) {
            fmgrInfo = *locinfo;
            return;
        }
    }

    if (procInfo != nullptr) {
        GetProcFuncCb funcCb = g_storageInstance->GetCacheHashMgr()->GetIndexProcFuncCb();
        if (unlikely(funcCb == nullptr)) {
            ErrLog(DSTORE_PANIC, MODULE_INDEX, ErrMsg("Index func callback is NULL!"));
            return;
        }
        funcCb(*procInfo, attNum, ConvertToSqlProcNum(procNum), fmgrInfo);
    }
}

void FillProcFmgrInfo(const IndexSupportProcInfo *procInfo, Oid funcOid, uint16 procNum,
    FmgrInfo &fmgrInfo, AttrNumber attNum)
{
    PGFunction funcAddr = g_storageInstance->GetCacheHashMgr()->GetFuncCacheFromFnOid(funcOid).fnAddr;
    if (funcAddr != nullptr) {
        fill_fmgr_info(funcOid, &fmgrInfo);
        return;
    }
    if (unlikely(g_storageInstance->IsBootstrapping())) {
        return;
    }

    // fill fmgrInfo from procInfo if exists
    if (procInfo != nullptr && procInfo->supportFmgrInfo != nullptr) {
        FmgrInfo *locinfo = procInfo->supportFmgrInfo;
        int procindex = (procInfo->numSupportProc * (attNum - 1)) + (ConvertToSqlProcNum(procNum) - 1);
        locinfo += procindex;
        if (locinfo->fnOid != DSTORE_INVALID_OID) {
            fmgrInfo = *locinfo;
            return;
        }
    }

    if (procInfo != nullptr) {
        GetProcFuncCb funcCb = g_storageInstance->GetCacheHashMgr()->GetIndexProcFuncCb();
        if (unlikely(funcCb == nullptr)) {
            ErrLog(DSTORE_PANIC, MODULE_INDEX, ErrMsg("Index func callback is NULL!"));
            return;
        }
        fmgrInfo.fnOid = funcOid;
        funcCb(*procInfo, attNum, ConvertToSqlProcNum(procNum), fmgrInfo);
    }
}

void FillOpfamilyProcFmgrInfo(IndexSupportProcInfo *opProcInfo, Oid leftTypeOid, Oid rightTypeOid,
                              AttrNumber attNum, FmgrInfo &fmgrInfo)
{
    Oid funcOid = g_storageInstance->GetCacheHashMgr()->GetFnOidFromArgType(leftTypeOid, rightTypeOid, MAINTAIN_ORDER);
    if (funcOid != DSTORE_INVALID_OID) {
        fill_fmgr_info(funcOid, &fmgrInfo);
        return;
    }
    if (unlikely(g_storageInstance->IsBootstrapping())) {
        return;
    }

    if (opProcInfo != nullptr) {
        GetOpfamilyProcFuncCb procOpfamilyFuncCb = g_storageInstance->GetCacheHashMgr()->GetIndexOpfProcCb();
        if (unlikely(procOpfamilyFuncCb == nullptr)) {
            ErrLog(DSTORE_PANIC, MODULE_INDEX, ErrMsg("Index opfamily proc callback is NULL!"));
        }
        procOpfamilyFuncCb(*opProcInfo, leftTypeOid, rightTypeOid, attNum, fmgrInfo);
    }
}

void FillOpfamilyStratFmgrInfo(IndexSupportProcInfo *procInfo, Oid leftTypeOid, Oid rightTypeOid, AttrNumber attNum,
    uint16 strat, FmgrInfo &fmgrInfo)
{
    StorageAssert(strat <= MAX_STRATEGY_NUM);
    StorageAssert(!g_storageInstance->IsBootstrapping());
    if (unlikely(strat > MAX_STRATEGY_NUM || g_storageInstance->IsBootstrapping() || procInfo == nullptr)) {
        /* Caller should handler this error. */
        return;
    }
    Oid funcOid = g_storageInstance->GetCacheHashMgr()->GetFnOidFromArgType(leftTypeOid, rightTypeOid, strat);
    if (funcOid != DSTORE_INVALID_OID) {
        fill_fmgr_info(funcOid, &fmgrInfo);
        return;
    }

    GetOpfamilyStratFuncCb opfamilyStratFuncCb = g_storageInstance->GetCacheHashMgr()->GetIndexOpfStratCb();
    if (unlikely(opfamilyStratFuncCb == nullptr)) {
        ErrLog(DSTORE_PANIC, MODULE_INDEX, ErrMsg("Index opfamily strat callback is NULL!"));
    }
    opfamilyStratFuncCb(*procInfo, leftTypeOid, rightTypeOid, attNum, strat, fmgrInfo);
}

void FillOuputFmgrInfo(Oid typeOid, FmgrInfo &fmgrInfo, GetOutputFuncCb outputCb)
{
    TypeCache typeCache = g_storageInstance->GetCacheHashMgr()->GetTypeCacheFromTypeOid(typeOid);
    if (typeCache.typoutput != DSTORE_INVALID_OID) {
        fill_fmgr_info(typeCache.typoutput, &fmgrInfo);
        return;
    }
    if (outputCb == nullptr) {
        ErrLog(DSTORE_PANIC, MODULE_INDEX, ErrMsg("outputCb is NULL!"));
        return;
    }
    outputCb(typeOid, fmgrInfo);
}

} /* namespace DSTORE */
