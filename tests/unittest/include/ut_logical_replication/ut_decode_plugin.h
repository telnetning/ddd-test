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
 */
#ifndef DSTORE_UT_DECODE_PLUGIN_H
#define DSTORE_UT_DECODE_PLUGIN_H

#include "ut_logical_replication_base.h"
#include "logical_replication/dstore_decode2text_plugin.h"
#include "logical_replication/dstore_logical_replication_struct.h"
#include "ut_tablehandler/ut_table_handler.h"

namespace DSTORE {

class DECODEPLUGIN : public LOGICALREPBASETEST {
public:
    void SetUp() override;
    void TearDown() override;
protected:
    void Clear();
    void MockDecodeTableInfo();
    TrxChangeCtx* GenerateTrxChangeCtx(Xid xid = {10, 20}, WalPlsn firstPlsn = 1000, WalPlsn endPlsn = 1030, WalPlsn commitPlsn = 1030, CommitSeqNo commitCsn = 64);
    RowChange* GenerateRowChange(TrxChangeCtx* trx, RowChangeType type);
    TrxLogicalLog *GenerateEmptyTrxLogicalLog();

    UTTableHandler *m_tableHandler;
    DecodeTableInfo *m_dictInfo;
    std::vector<TrxChangeCtx *> m_trx;
    std::vector<RowChange *> m_rowChange;
    std::vector<TrxLogicalLog *> m_trxOut;
    DecodePlugin *m_textPlugin;
};
}
#endif
