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
 * dstore_decode2text_plugin.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_DECODE2TEXT_PLUGIN_H
#define DSTORE_DECODE2TEXT_PLUGIN_H

#include "dstore_decode_plugin.h"
#include "dstore_wal_sort_buffer.h"

#ifdef UT
#ifndef ENABLE_LOGICAL_REPL
/* LOGICAL REPL is not supported for now. */
#define ENABLE_LOGICAL_REPL
#endif
#endif

namespace DSTORE {

#ifdef ENABLE_LOGICAL_REPL
class Decode2TextPlugin : public DecodePlugin {
public:
    Decode2TextPlugin() {};
    ~Decode2TextPlugin() override = default;
    RetStatus DecodeBegin(TrxLogicalLog *trxOut, TrxChangeCtx *trx, DecodeOptions *options) override;
    RetStatus DecodeChange(TrxLogicalLog *trxOut, RowChange *rowChange,
                           DecodeTableInfo *dict, DecodeOptions *options) override;
    RetStatus DecodeCommit(TrxLogicalLog *trxOut, TrxChangeCtx *trx, DecodeOptions *options) override;
private:
    RetStatus Insert2Text(TrxLogicalLog *trxOut, RowChange *rowChange,
                          DecodeTableInfo *dict, DecodeOptions *options);
    RetStatus Update2Text(TrxLogicalLog *trxOut, RowChange *rowChange,
                          DecodeTableInfo *dict, DecodeOptions *options);
    RetStatus Delete2Text(TrxLogicalLog *trxOut, RowChange *rowChange,
                          DecodeTableInfo *dict, DecodeOptions *options);
    RetStatus HeapTuple2String(StringInfo out, TupleBuf *tupleBuf, DecodeTableInfo *dict, bool skipAttrNullsFlag);
    RetStatus Type2String(StringInfo out, Oid typeId);
    void Value2String(StringInfo out, Form_pg_attribute attr, Datum rawValue);
    void PrintReadable(StringInfo out, Oid typeId, char *outputStr);
    char* OutputFunctionCall(Oid typeId, Datum value);
};

#endif
}
#endif