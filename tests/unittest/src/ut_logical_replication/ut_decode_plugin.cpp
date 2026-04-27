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
 * ut_decode_plugin.cpp
 *
 * Description:
 *
 * ---------------------------------------------------------------------------------------
 *
 */

#include "ut_logical_replication/ut_decode_plugin.h"

using namespace DSTORE;

void DECODEPLUGIN::SetUp()
{
    LOGICALREPBASETEST::SetUp();
    m_textPlugin = DstoreNew(m_ut_memory_context) Decode2TextPlugin();
    m_tableHandler = UTTableHandler::CreateTableHandler(g_defaultPdbId, m_ut_memory_context);
    MockDecodeTableInfo();
}

void DECODEPLUGIN::TearDown()
{
    Clear();
    delete m_textPlugin;
    UTTableHandler::Destroy(m_tableHandler);
    m_tableHandler = nullptr;
    DstorePfreeExt(m_dictInfo);
    LOGICALREPBASETEST::TearDown();
}

void DECODEPLUGIN::Clear()
{
    for (int i = 0; i < m_trx.size(); i++) {
        DstorePfreeExt(m_trx[i]);
    }

    for (int i = 0; i < m_rowChange.size(); i++) {
        DstorePfreeExt(m_rowChange[i]->data.tuple.oldTuple);
        DstorePfreeExt(m_rowChange[i]->data.tuple.newTuple);
        DstorePfreeExt(m_rowChange[i]);
    }
    for (int i = 0; i < m_trxOut.size(); i++) {
        TrxLogicalLog *tmp = m_trxOut[i]; 
        for (int j = 0; j < tmp->nRows; j++) {
            DstorePfreeExt(tmp->rowArray[j]);
        }
        DstorePfreeExt(tmp);
    }
}

void DECODEPLUGIN::MockDecodeTableInfo()
{
    m_dictInfo = static_cast<DecodeTableInfo *>(DstorePalloc0(sizeof(DecodeTableInfo)));
    char buf1[NAME_DATA_LEN] = "public";
    m_dictInfo->SetNsp(DSTORE_INVALID_OID, buf1);
    char buf2[NAME_DATA_LEN] = "t1";
    m_dictInfo->SetRelName(buf2);
    m_dictInfo->fakeDescData = *(m_tableHandler->GenerateFakeLogicalDecodeTupDesc());
}

TrxChangeCtx* DECODEPLUGIN::GenerateTrxChangeCtx(Xid xid, WalPlsn firstPlsn, WalPlsn endPlsn, WalPlsn commitPlsn, CommitSeqNo commitCsn)
{
    TrxChangeCtx* trxChange = static_cast<TrxChangeCtx *>(DstorePalloc0(sizeof(TrxChangeCtx)));
    trxChange->xid = xid;
    trxChange->firstPlsn = firstPlsn;
    trxChange->endPlsn = endPlsn;
    trxChange->commitPlsn = commitPlsn;
    trxChange->commitCsn = commitCsn;
    /* for memory release */
    m_trx.push_back(trxChange);
    return trxChange;
}

RowChange* DECODEPLUGIN::GenerateRowChange(TrxChangeCtx* trx, RowChangeType type)
{
    RowChange* rowChange = static_cast<RowChange *>(DstorePalloc0(sizeof(RowChange)));
    rowChange->trx = trx;
    rowChange->type = type;
    switch (type) {
        case RowChangeType::INSERT: {
            HeapTuple *tuple =  m_tableHandler->GenerateRandomHeapTuple(m_dictInfo->GetTupleDesc());
            rowChange->data.tuple.newTuple = static_cast<TupleBuf *>(DstorePalloc0(sizeof(TupleBuf)));
            rowChange->data.tuple.newTuple->memTup = *tuple;
            break;
        }
        case RowChangeType::UPDATE: 
        case RowChangeType::DELETE: {
            HeapTuple *oldTuple =  m_tableHandler->GenerateRandomHeapTuple(m_dictInfo->GetTupleDesc());
            HeapTuple *newTuple =  m_tableHandler->GenerateRandomHeapTuple(m_dictInfo->GetTupleDesc());
            rowChange->data.tuple.oldTuple = static_cast<TupleBuf *>(DstorePalloc0(sizeof(TupleBuf)));
            rowChange->data.tuple.newTuple = static_cast<TupleBuf *>(DstorePalloc0(sizeof(TupleBuf)));
            rowChange->data.tuple.oldTuple->memTup = *oldTuple;
            rowChange->data.tuple.newTuple->memTup = *newTuple;
            break;
        }
        default:
            return rowChange;
    }
    /* for memory release */
    m_rowChange.push_back(rowChange);
    return rowChange;
}

TrxLogicalLog* DECODEPLUGIN::GenerateEmptyTrxLogicalLog()
{
    TrxLogicalLog* trxOut = static_cast<TrxLogicalLog *>(DstorePalloc0(sizeof(TrxLogicalLog)));
    trxOut->Init();
    /* for memory release */
    m_trxOut.push_back(trxOut);
    return trxOut;
}

TEST_F(DECODEPLUGIN, DecodeInsert2Text)
{
    TrxLogicalLog* out = GenerateEmptyTrxLogicalLog();
    TrxChangeCtx* trx = GenerateTrxChangeCtx();
    RowChange* rowChange = GenerateRowChange(trx, RowChangeType::INSERT);
    RetStatus rt;
    rt = m_textPlugin->DecodeBegin(out, trx, m_decodeOptions);
    ASSERT_EQ(rt, DSTORE_SUCC);
    rt = m_textPlugin->DecodeChange(out, rowChange, m_dictInfo, m_decodeOptions);
    ASSERT_EQ(rt, DSTORE_SUCC);
    rt = m_textPlugin->DecodeCommit(out, trx, m_decodeOptions);
    ASSERT_EQ(rt, DSTORE_SUCC);
    ASSERT_EQ(out->nRows, 3);
    StringInfoData outString;
    outString.init();
    out->Dump(&outString);
    //std::cout <<outString.data << std::endl;
}

TEST_F(DECODEPLUGIN, DecodeUpdate2Text)
{
    TrxLogicalLog* out = GenerateEmptyTrxLogicalLog();
    TrxChangeCtx* trx = GenerateTrxChangeCtx();
    RowChange* rowChange = GenerateRowChange(trx, RowChangeType::UPDATE);
    RetStatus rt;
    rt = m_textPlugin->DecodeBegin(out, trx, m_decodeOptions);
    ASSERT_EQ(rt, DSTORE_SUCC);
    rt = m_textPlugin->DecodeChange(out, rowChange, m_dictInfo, m_decodeOptions);
    ASSERT_EQ(rt, DSTORE_SUCC);
    rt = m_textPlugin->DecodeCommit(out, trx, m_decodeOptions);
    ASSERT_EQ(rt, DSTORE_SUCC);
    ASSERT_EQ(out->nRows, 3);
    StringInfoData outString;
    outString.init();
    out->Dump(&outString);
}

TEST_F(DECODEPLUGIN, DecodeDelete2Text)
{
    TrxLogicalLog* out = GenerateEmptyTrxLogicalLog();
    TrxChangeCtx* trx = GenerateTrxChangeCtx();
    RowChange* rowChange = GenerateRowChange(trx, RowChangeType::DELETE);
    RetStatus rt;
    rt = m_textPlugin->DecodeBegin(out, trx, m_decodeOptions);
    ASSERT_EQ(rt, DSTORE_SUCC);
    rt = m_textPlugin->DecodeChange(out, rowChange, m_dictInfo, m_decodeOptions);
    ASSERT_EQ(rt, DSTORE_SUCC);
    rt = m_textPlugin->DecodeCommit(out, trx, m_decodeOptions);
    ASSERT_EQ(rt, DSTORE_SUCC);
    ASSERT_EQ(out->nRows, 3);
    StringInfoData outString;
    outString.init();
    out->Dump(&outString);
}
