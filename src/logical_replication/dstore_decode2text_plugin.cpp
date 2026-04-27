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
 * text_plugin.cpp
 * The formate to decode.
 *
 * ---------------------------------------------------------------------------------------
 */
#include "logical_replication/dstore_decode2text_plugin.h"
#include "logical_replication/dstore_wal_sort_buffer.h"
#include "logical_replication/dstore_decode_dict.h"
#include "catalog/dstore_fake_type.h"
#include "catalog/dstore_function_struct.h"
#include "catalog/dstore_typecache.h"
#include "common/datatype/dstore_varlena_utils.h"
#include "common/algorithm/dstore_string_info.h"

namespace DSTORE {
#ifdef ENABLE_LOGICAL_REPL
RetStatus Decode2TextPlugin::DecodeBegin(TrxLogicalLog *trxOut, TrxChangeCtx *trx, DecodeOptions *options)
{
    StorageAssert(trxOut->nRows == 0);
    if (unlikely(trx->firstPlsn == INVALID_END_PLSN)) {
        return DSTORE_FAIL;
    }

    trxOut->startPlsn = trx->firstPlsn;
    trxOut->xid = trx->xid.m_placeHolder;
    RowLogicalLog *rowLog = static_cast<RowLogicalLog *>(DstorePalloc(sizeof(RowLogicalLog)));
    if (STORAGE_VAR_NULL(rowLog)) {
        return DSTORE_FAIL;
    }
    rowLog->out = StringInfoData::make();
    if (STORAGE_VAR_NULL(rowLog->out)) {
        DstorePfreeExt(rowLog);
        return DSTORE_FAIL;
    }
    rowLog->rowStartPlsn = trx->firstPlsn;
    if (options->includeXidsFlag) {
        rowLog->out->append("BEGIN %lu", trx->xid.m_placeHolder);
    } else {
        rowLog->out->AppendString("BEGIN");
    }
    RetStatus rt = trxOut->AddRowLogicalLog(rowLog);
    return rt;
}

RetStatus Decode2TextPlugin::DecodeChange(TrxLogicalLog *trxOut, RowChange *rowChange,
                                          DecodeTableInfo *dict, DecodeOptions *options)
{
    RetStatus rt = DSTORE_SUCC;
    if (unlikely(dict == nullptr)) {
        storage_set_error(DECODE_ERROR_DICT_NOT_FOUND, trxOut->xid, rowChange->data.tuple.snapshotCsn);
        return DSTORE_FAIL;
    }

    switch (rowChange->type) {
        case RowChangeType::INSERT:
            rt = Insert2Text(trxOut, rowChange, dict, options);
            break;
        case RowChangeType::UPDATE:
            rt = Update2Text(trxOut, rowChange, dict, options);
            break;
        case RowChangeType::DELETE:
            rt = Delete2Text(trxOut, rowChange, dict, options);
            break;
        case RowChangeType::CATALOG_INSERT:
        case RowChangeType::CATALOG_UPDATE:
        case RowChangeType::CATALOG_DELETE:
        case RowChangeType::INTERNAL_SPEC_INSERT:
        case RowChangeType::INTERNAL_SPEC_CONFIRM:
        case RowChangeType::INTERNAL_SPEC_ABORT:
        default:
            /* other RowChangeType should not be output anything */
            storage_set_error(DECODE_ERROR_UNSUPPORT_TYPE);
            return DSTORE_FAIL;
    }
    return rt;
}

RetStatus Decode2TextPlugin::DecodeCommit(TrxLogicalLog *trxOut, TrxChangeCtx *trx, DecodeOptions *options)
{
    if (unlikely(trx->commitPlsn == INVALID_END_PLSN || trx->commitCsn == INVALID_CSN)) {
        storage_set_error(DECODE_ERROR_EMPTY_TRX);
        return DSTORE_FAIL;
    }

    trxOut->endPlsn = trx->endPlsn;
    trxOut->commitPlsn = trx->commitPlsn;
    trxOut->commitCsn = trx->commitCsn;

    RowLogicalLog *rowLog = static_cast<RowLogicalLog *>(DstorePalloc(sizeof(RowLogicalLog)));
    if (STORAGE_VAR_NULL(rowLog)) {
        return DSTORE_FAIL;
    }
    rowLog->out = StringInfoData::make();
    if (STORAGE_VAR_NULL(rowLog->out)) {
        DstorePfreeExt(rowLog);
        return DSTORE_FAIL;
    }
    if (options->includeXidsFlag) {
        StorageAssert(trx->xid.m_placeHolder == trxOut->xid);
        rowLog->out->append("COMMIT %lu", trx->xid.m_placeHolder);
    } else {
        rowLog->out->AppendString("COMMIT");
    }
    RetStatus rt = trxOut->AddRowLogicalLog(rowLog);
    return rt;
}


RetStatus Decode2TextPlugin::Insert2Text(TrxLogicalLog *trxOut, RowChange *rowChange,
                                         DecodeTableInfo *dict, DecodeOptions *options)
{
    RowLogicalLog *rowLog = static_cast<RowLogicalLog *>(DstorePalloc(sizeof(RowLogicalLog)));
    if (STORAGE_VAR_NULL(rowLog)) {
        return DSTORE_FAIL;
    }
    const char *nspName = dict->GetNspName();
    const char *table = dict->GetRelName();
    rowLog->out = StringInfoData::make();
    if (STORAGE_VAR_NULL(rowLog->out)) {
        goto ERROR_EXIT;
    }
    rowLog->rowStartPlsn = rowChange->plsn;
    rowLog->type = RowLogicalLogType::DML;
    if (STORAGE_FUNC_FAIL(
        rowLog->out->append("table %s.%s", nspName, table))) {
        goto ERROR_EXIT;
    }
    if (STORAGE_FUNC_FAIL(rowLog->out->AppendString(" INSERT:"))) {
        goto ERROR_EXIT;
    }

    if (rowChange->data.tuple.newTuple == nullptr) {
        rowLog->out->AppendString(" (no-tuple-data)");
    } else {
        if (STORAGE_FUNC_FAIL(
            HeapTuple2String(rowLog->out, rowChange->data.tuple.newTuple, dict, options->skipAttrNullsFlag))) {
            goto ERROR_EXIT;
        }
    }

    if (STORAGE_FUNC_FAIL(trxOut->AddRowLogicalLog(rowLog))) {
        goto ERROR_EXIT;
    }
    return DSTORE_SUCC;

ERROR_EXIT:
    if (rowLog) {
        if (rowLog->out) {
            DstorePfreeExt(rowLog->out->data);
        }
        DstorePfreeExt(rowLog->out);
        DstorePfreeExt(rowLog);
    }
    return DSTORE_FAIL;
}


RetStatus Decode2TextPlugin::Update2Text(TrxLogicalLog *trxOut, RowChange *rowChange,
                                         DecodeTableInfo *dict, DecodeOptions *options)
{
    StorageAssert(dict != nullptr);

    RowLogicalLog *rowLog = static_cast<RowLogicalLog *>(DstorePalloc(sizeof(RowLogicalLog)));
    if (STORAGE_VAR_NULL(rowLog)) {
        return DSTORE_FAIL;
    }
    rowLog->out = StringInfoData::make();
    if (STORAGE_VAR_NULL(rowLog->out)) {
        DstorePfreeExt(rowLog);
        return DSTORE_FAIL;
    }
    rowLog->rowStartPlsn = rowChange->plsn;
    rowLog->type = RowLogicalLogType::DML;
    const char *nspName = dict->GetNspName();
    const char *table = dict->GetRelName();
    rowLog->out->append("table %s.%s", nspName, table);

    rowLog->out->AppendString(" UPDATE:");
    RetStatus rt = DSTORE_SUCC;
    if (rowChange->data.tuple.oldTuple != nullptr) {
        rowLog->out->AppendString(" old-key:");
        rt = HeapTuple2String(rowLog->out, rowChange->data.tuple.oldTuple, dict, options->skipAttrNullsFlag);
        if (unlikely(rt == DSTORE_FAIL)) {
            return DSTORE_FAIL;
        }
        rowLog->out->AppendString(" new-tuple:");
    }

    if (rowChange->data.tuple.newTuple == nullptr) {
        rowLog->out->AppendString(" (no-tuple-data)");
    } else {
        rt = HeapTuple2String(rowLog->out, rowChange->data.tuple.newTuple, dict, options->skipAttrNullsFlag);
        if (unlikely(rt == DSTORE_FAIL)) {
            return DSTORE_FAIL;
        }
    }
    rt = trxOut->AddRowLogicalLog(rowLog);
    return rt;
}


RetStatus Decode2TextPlugin::Delete2Text(TrxLogicalLog *trxOut, RowChange *rowChange,
                                         DecodeTableInfo *dict, DecodeOptions *options)
{
    StorageAssert(dict != nullptr);

    RowLogicalLog *rowLog = static_cast<RowLogicalLog *>(DstorePalloc(sizeof(RowLogicalLog)));
    if (STORAGE_VAR_NULL(rowLog)) {
        return DSTORE_FAIL;
    }
    rowLog->out = StringInfoData::make();
    if (STORAGE_VAR_NULL(rowLog->out)) {
        DstorePfreeExt(rowLog);
        return DSTORE_FAIL;
    }
    rowLog->rowStartPlsn = rowChange->plsn;
    rowLog->type = RowLogicalLogType::DML;
    const char *nspName = dict->GetNspName();
    const char *table = dict->GetRelName();
    rowLog->out->append("table %s.%s", nspName, table);

    rowLog->out->AppendString(" DELETE:");
    RetStatus rt = DSTORE_SUCC;
    if (rowChange->data.tuple.oldTuple == nullptr) {
        rowLog->out->AppendString(" (no-tuple-data)");
    } else {
        rt = HeapTuple2String(rowLog->out, rowChange->data.tuple.oldTuple, dict, options->skipAttrNullsFlag);
        if (unlikely(rt == DSTORE_FAIL)) {
            return DSTORE_FAIL;
        }
    }
    rt = trxOut->AddRowLogicalLog(rowLog);
    return rt;
}

RetStatus Decode2TextPlugin::HeapTuple2String(StringInfo out, TupleBuf *tupleBuf,
                                              DecodeTableInfo *dict, bool skipAttrNullsFlag)
{
    RetStatus rt = DSTORE_SUCC;
    HeapTuple tuple = tupleBuf->memTup;
    TupleDesc desc = dict->GetTupleDesc();
    /* decode each attr individually */
    for (int attrIdx = 0; attrIdx < dict->GetAttrNums(); attrIdx++) {
        Form_pg_attribute attr = desc->attrs[attrIdx];
        if (attr->attisdropped || attr->attnum < 0) {
            continue;
        }
        bool isNull;
        Datum datum = tuple.GetAttr(attrIdx + 1, desc, &isNull);
        if (isNull && skipAttrNullsFlag) {
            continue;
        }
        /* ready to form string of heap */
        /* attname */
        out->append_char(' ');
        out->AppendString(attr->attname.data);

        /* atttype */
        out->append_char('[');
        rt = Type2String(out, attr->atttypid);
        if (rt == DSTORE_FAIL) {
            break;
        }
        out->append_char(']');

        /* value */
        out->append_char(':');
        if (isNull) {
            out->AppendString("null");
        } else {
            Value2String(out, attr, datum);
        }
    }
    return rt;
}

RetStatus Decode2TextPlugin::Type2String(StringInfo out, Oid typeId)
{
    switch (typeId) {
        case BOOLOID:
            out->AppendString("boolean");
            break;
        case FLOAT4OID:
            out->AppendString("real");
            break;
        case FLOAT8OID:
            out->AppendString("double precision");
            break;
        case INT2OID:
            out->AppendString("smallint");
            break;
        case INT4OID:
            out->AppendString("integer");
            break;
        case INT8OID:
            out->AppendString("bigint");
            break;
        case VARCHAROID:
            out->AppendString("character varying");
            break;
        default:
            storage_set_error(DECODE_ERROR_UNSUPPORTED_ATTR_TYPE, typeId);
            return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

void Decode2TextPlugin::Value2String(StringInfo out, Form_pg_attribute attr, Datum rawValue)
{
    char *outputStr = nullptr;
    if (attr->attbyval) {
        outputStr = OutputFunctionCall(attr->atttypid, rawValue);
        PrintReadable(out, attr->atttypid, outputStr);
    } else {
        /* todo: handle extended var */
        outputStr = OutputFunctionCall(attr->atttypid, rawValue);
        PrintReadable(out, attr->atttypid, OutputFunctionCall(attr->atttypid, rawValue));
    }
    DstorePfreeExt(outputStr);
}

void Decode2TextPlugin::PrintReadable(StringInfo out, Oid typeId, char *outputStr)
{
    switch (typeId) {
        case FLOAT4OID:
        case FLOAT8OID:
        case INT2OID:
        case INT4OID:
        case INT8OID:
        case OIDOID:
            out->AppendString(outputStr);
            break;
        case BITOID:
        case VARBITOID:
            out->append("B'%s'", outputStr);
            break;
        case BOOLOID:
            if (strcmp(outputStr, "t") == 0) {
                out->AppendString("true");
            } else {
                out->AppendString("false");
            }
            break;
        default:
            out->append_char('\'');
            const char* valptr = nullptr;
            for (valptr = outputStr; *valptr; valptr++) {
                char ch = *valptr;
                if ((ch) == '\'') {
                    out->append_char(ch);
                }
                out->append_char(ch);
            }
            out->append_char('\'');
            break;
    }
    return;
}

char* Decode2TextPlugin::OutputFunctionCall(Oid typeId, Datum value)
{
    TypeCache typeCache = g_storageInstance->GetCacheHashMgr()->GetTypeCacheFromTypeOid(typeId);
    FmgrInfo flinfo;
    fill_fmgr_info(typeCache.typoutput, &flinfo, g_dstoreCurrentMemoryContext);
    return DatumGetCString(FunctionCall1Coll(&flinfo, DSTORE_INVALID_OID, value));
}
#endif

}

