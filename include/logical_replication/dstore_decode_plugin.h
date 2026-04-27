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
 * dstore_decode_plugin.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_DECODE_PLUGIN_H
#define DSTORE_DECODE_PLUGIN_H

#include "common/dstore_common_utils.h"
#include "logical_replication/dstore_logical_replication_struct.h"
#include "common/memory/dstore_mctx.h"

#ifdef UT
#ifndef ENABLE_LOGICAL_REPL
/* LOGICAL REPL is not supported for now. */
#define ENABLE_LOGICAL_REPL
#endif
#endif

namespace DSTORE {

#ifdef ENABLE_LOGICAL_REPL
struct TrxLogicalLog;
struct TrxChangeCtx;
struct RowChange;
struct DecodeTableInfo;

class DecodePlugin : public BaseObject {
public:
    DecodePlugin() {};
    virtual ~DecodePlugin() = default;
    virtual RetStatus DecodeBegin(TrxLogicalLog *trxOut, TrxChangeCtx *trx, DecodeOptions *options) = 0;
    virtual RetStatus DecodeChange(TrxLogicalLog *trxOut, RowChange *rowChange,
                                   DecodeTableInfo *dict, DecodeOptions *options) = 0;
    virtual RetStatus DecodeCommit(TrxLogicalLog *trxOut, TrxChangeCtx *trx, DecodeOptions *options) = 0;
};
#endif

}
#endif