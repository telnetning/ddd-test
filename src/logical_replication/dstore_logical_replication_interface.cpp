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
 * dstore_logical_replication_interface.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "logical_replication/dstore_logical_replication_interface.h"
#include "logical_replication/dstore_logical_replication_struct.h"
#include "logical_replication/dstore_logical_replication_mgr.h"
#include "common/algorithm/dstore_string_info.h"
#include "wal/dstore_wal_struct.h"

namespace DSTORE {

#ifdef ENABLE_LOGICAL_REPL
DecodeTableInfo* CatalogInfo::ConvertToDecodeTableInfo()
{
    DecodeTableInfo *tableInfo = DecodeTableInfo::Create(natts);
    tableInfo->Init(tableOid, csn, DecodeTableInfoStatus::COLLECTED);
    tableInfo->InitDesc(natts, sysRel->relhasoids);
    tableInfo->SetRelName(sysRel->relname.data);
    tableInfo->SetNsp(nspOid, nspName);
    errno_t rc;
    for (int i = 0; i < natts; i++) {
        rc = memcpy_s(tableInfo->fakeDescData.attrs[i], MAXALIGN(ATTRIBUTE_FIXED_SIZE),
                      sysAttr[i], MAXALIGN(ATTRIBUTE_FIXED_SIZE));
        storage_securec_check(rc, "\0", "\0");
    }
    return tableInfo;
}

void TrxLogicalLog::Init()
{
    maxRows = INIT_ROW_PER_TRX;
    startPlsn = INVALID_END_PLSN;
    endPlsn = INVALID_END_PLSN;
    commitPlsn = INVALID_END_PLSN;
    commitCsn = INVALID_CSN;
    nRows = 0;
    rowArray = static_cast<RowLogicalLog **>(DstorePalloc(INIT_ROW_PER_TRX * sizeof(RowLogicalLog *)));
}

void TrxLogicalLog::FreeLogicalLog()
{
    for (int i = 0; i < nRows; i++) {
        DstorePfreeExt(rowArray[i]->out->data);
        DstorePfreeExt(rowArray[i]->out);
        DstorePfreeExt(rowArray[i]);
    }
    DstorePfreeExt(rowArray);
}

RetStatus TrxLogicalLog::AddRowLogicalLog(RowLogicalLog *rowLog)
{
    if (nRows >= maxRows) {
        maxRows = 2 * maxRows; /* expand self */
        rowArray = static_cast<RowLogicalLog **>(DstoreRepalloc(rowArray, sizeof(RowLogicalLog *) * maxRows));
    }
    StorageAssert(nRows >= 0);
    StorageReleasePanic(rowArray == nullptr, MODULE_LOGICAL_REPLICATION,
                        ErrMsg("Failed to allocate memory for row logical log!"));
    rowArray[nRows] = rowLog;
    nRows += 1;
    return DSTORE_SUCC;
}

void TrxLogicalLog::Send(StringInfoData *sendBuf)
{
    sendBuf->SendInt32(nodeId);
    sendBuf->SendInt64(xid);
    sendBuf->SendInt64(commitCsn);
    sendBuf->SendInt64(restartDecodingPlsn);
    sendBuf->SendInt32(nRows);
    for (int i = 0; i < nRows; i++) {
        sendBuf->SendInt8(static_cast<uint8>(rowArray[i]->type));
        sendBuf->SendInt32(rowArray[i]->out->len);
        sendBuf->SendBytes(rowArray[i]->out->data, rowArray[i]->out->len);
    }
}

void TrxLogicalLog::Dump(StringInfoData *dumpInfo)
{
    /* base info */
    dumpInfo->append("xid: %lu, startPlsn: %lu endPlsn: %lu CommitCsn: %lu\n", xid, startPlsn, endPlsn, commitCsn);
    for (int i = 0; i < nRows; i++) {
        rowArray[i]->Dump(dumpInfo);
        dumpInfo->append_char('\n');
    }
}

void RowLogicalLog::Dump(StringInfoData *dumpInfo)
{
    dumpInfo->AppendString(out->data);
}

}

namespace LogicalReplicationInterface {
using namespace DSTORE;

RetStatus SynchronizeCatalog(CatalogInfo *rawInfo)
{
    StoragePdb *storagePdb = g_storageInstance->GetPdb(rawInfo->pdbId);
    StorageAssert(storagePdb != nullptr && storagePdb->IsInit());
    LogicalReplicaMgr *logicalRepMgr = storagePdb->GetLogicalReplicaMgr();
    return logicalRepMgr->SyncCatalogToDecodeDict(rawInfo);
}

LogicalDecodeHandler *CreateLogicalDecodeHandler(LogicalReplicationSlot* logicalSlot, DecodeOptions *opts)
{
    StoragePdb *storagePdb = g_storageInstance->GetPdb(logicalSlot->GetPdbId());
    StorageAssert(storagePdb != nullptr && storagePdb->IsInit());
    LogicalReplicaMgr *logicalRepMgr = storagePdb->GetLogicalReplicaMgr();
    return logicalRepMgr->CreateLogicalDecodeHandler(logicalSlot, opts);
}

void DeleteLogicalDecodeHandler(LogicalDecodeHandler *decodeContext)
{
    StoragePdb *storagePdb = g_storageInstance->GetPdb(decodeContext->GetPdbId());
    StorageAssert(storagePdb != nullptr && storagePdb->IsInit());
    LogicalReplicaMgr *logicalRepMgr = storagePdb->GetLogicalReplicaMgr();
    logicalRepMgr->DeleteLogicalDecodeHandler(decodeContext);
}

void StartupLogicalReplication(LogicalDecodeHandler *decodeContext)
{
    StoragePdb *storagePdb = g_storageInstance->GetPdb(decodeContext->GetPdbId());
    StorageAssert(storagePdb != nullptr && storagePdb->IsInit());
    LogicalReplicaMgr *logicalRepMgr = storagePdb->GetLogicalReplicaMgr();
    logicalRepMgr->StartUpLogicalDecode(decodeContext);
}

void StopLogicalReplication(LogicalDecodeHandler *decodeContext)
{
    StoragePdb *storagePdb = g_storageInstance->GetPdb(decodeContext->GetPdbId());
    StorageAssert(storagePdb != nullptr && storagePdb->IsInit());
    LogicalReplicaMgr *logicalRepMgr = storagePdb->GetLogicalReplicaMgr();
    logicalRepMgr->StopLogicalDecode(decodeContext);
}

TrxLogicalLog* GetNextTrxLogicalLog(LogicalDecodeHandler *decodeContext)
{
    return decodeContext->GetNextTrxLogicalLog();
}

void ConfirmTrxLogicalLog(LogicalDecodeHandler *decodeContext, TrxLogicalLog *trxLog)
{
    decodeContext->ConfirmTrxLogicalLog(trxLog);
}
#endif
}