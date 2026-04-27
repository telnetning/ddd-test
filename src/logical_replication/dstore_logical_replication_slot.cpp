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
 * dstore_logical_replication_slot.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "logical_replication/dstore_logical_replication_mgr.h"
#include "port/dstore_port.h"
#include "common/dstore_datatype.h"
#include "transaction/dstore_csn_mgr.h"
#include "transaction/dstore_transaction_mgr.h"
#include "undo/dstore_undo_mgr.h"

namespace DSTORE {

#ifdef ENABLE_LOGICAL_REPL
LogicalReplicationSlot::LogicalReplicationSlot()
{
    Reset();
    m_slotLock.Init();
}

void LogicalReplicationSlot::Init(PdbId pdbId, char *slotName, char *pluginName, WalId walId, int slotId)
{
    m_pdbId = pdbId;
    SetName(slotName);
    SetPluginName(pluginName);
    m_walId = walId;
    m_slotId = slotId;
    m_state = StartPointState::DEFAULT;
}

void LogicalReplicationSlot::Destroy()
{
    Reset();
}

void LogicalReplicationSlot::Reset()
{
    errno_t rc = memset_s(m_name.data, NAME_DATA_LEN, 0, NAME_DATA_LEN);
    storage_securec_check(rc, "\0", "\0");
    rc = memset_s(m_plugin.data, NAME_DATA_LEN, 0, NAME_DATA_LEN);
    storage_securec_check(rc, "\0", "\0");
    m_pdbId = INVALID_PDB_ID;
    m_walId = INVALID_WAL_ID;
    m_catalogCsnMin = INVALID_CSN;
    m_decodeDictCsnMin = INVALID_CSN;
    m_restartPlsn = INVALID_END_PLSN;
    m_confirmedCsn = INVALID_CSN;
    m_state = StartPointState::DEFAULT;
    m_inUse = false;
    m_isActive = false;
    m_slotId = INVALID_SLOT_ID;
}

void LogicalReplicationSlot::SerializeToDisk(bool isCreate)
{
    StoragePdb *storagePdb = g_storageInstance->GetPdb(m_pdbId);
    StorageAssert(storagePdb != nullptr);
    ControlFile *controlFile = storagePdb->GetControlFile();
    ControlLogicalReplicationSlotPageItemData item;
    errno_t rc = strncpy_s(item.name.data, NAME_DATA_LEN, m_name.data, NAME_DATA_LEN);
    storage_securec_check(rc, "\0", "\0");
    rc = strncpy_s(item.plugin.data, NAME_DATA_LEN, m_plugin.data, NAME_DATA_LEN);
    storage_securec_check(rc, "\0", "\0");
    item.walId = m_walId;
    item.catalogCsnMin = m_catalogCsnMin;
    item.decodeDictCsnMin = m_decodeDictCsnMin;
    item.restartPlsn = m_restartPlsn;
    item.confirmedCsn = m_confirmedCsn;
    item.isConsistent = (m_state == StartPointState::CONSISTENT);   /* unnecessary to keep middle state */

    if (isCreate && STORAGE_FUNC_FAIL(controlFile->AddLogicalReplicationSlot(&item))) {
        ErrLog(DSTORE_PANIC, MODULE_LOGICAL_REPLICATION, ErrMsg("Failed to create serialized slot info"));
    }
    if (!isCreate && STORAGE_FUNC_FAIL(controlFile->UpdateLogicalReplicationSlot(&item))) {
        ErrLog(DSTORE_PANIC, MODULE_LOGICAL_REPLICATION, ErrMsg("Failed to update serialized slot info"));
    }
}

void LogicalReplicationSlot::RestoreFromDisk(const ControlLogicalReplicationSlotPageItemData item)
{
    SetName(item.name.data);
    SetPluginName(item.plugin.data);
    m_walId = item.walId;
    m_catalogCsnMin = item.catalogCsnMin;
    m_decodeDictCsnMin = item.decodeDictCsnMin;
    m_restartPlsn = item.restartPlsn;
    m_confirmedCsn = item.confirmedCsn;
    m_state = item.isConsistent ? StartPointState::CONSISTENT : StartPointState::DEFAULT;
    m_inUse = true;
    m_isActive = false;
}

RetStatus LogicalReplicationSlot::FindStartPoint(
    const std::function<RetStatus(const CommitSeqNo)> syncCatalogCallBack)
{
    /* only self transaction. */
    StoragePdb *storagePdb = g_storageInstance->GetPdb(m_pdbId);
    StorageAssert(storagePdb != nullptr);
    if (storagePdb->GetActiveTransactionNum() == 1) {
        m_state = StartPointState::CONSISTENT;
        ErrLog(DSTORE_LOG, MODULE_LOGICAL_REPLICATION, ErrMsg("Only self trx, DEFAULT To CONSISTENT"));
        return SyncCatalog(syncCatalogCallBack);
    }
    /* get initial active trxs by undoMgr */
    int maxThreadNum = static_cast<int>(g_storageInstance->GetGuc()->ncores);
    HASHCTL info;
    info.keysize = sizeof(Xid);
    info.entrysize = sizeof(Xid);
    info.hash = tag_hash;
    info.hcxt = g_dstoreCurrentMemoryContext;
    int hashFlags = (HASH_ELEM | HASH_CONTEXT | HASH_FUNCTION);
    HTAB *initialActiveTrxs = hash_create("InitialActiveTrxs", maxThreadNum, &info, hashFlags);
    UndoMgr *undoMgr = g_storageInstance->GetPdb(m_pdbId)->GetUndoMgr();
    undoMgr->GetLocalCurrentAllActiveTrxXid(PutAllActiveTrxXidToHTAB, static_cast<void *>(initialActiveTrxs));
    if (hash_get_num_entries(initialActiveTrxs) == 0) {
        m_state = StartPointState::CONSISTENT;
        hash_destroy(initialActiveTrxs);
        ErrLog(DSTORE_LOG, MODULE_LOGICAL_REPLICATION, ErrMsg("Only read-only trxs, DEFAULT To CONSISTENT"));
        return SyncCatalog(syncCatalogCallBack);
    }

    HTAB *currentActiveTrxs = hash_create("CurrentActiveTrxs", maxThreadNum, &info, hashFlags);
    HTAB *newBeginTrxs = hash_create("NewTrxs", maxThreadNum, &info, hashFlags);
    m_state = StartPointState::WAIT_ACTIVE_TRX_FINISH;
    ErrLog(DSTORE_LOG, MODULE_LOGICAL_REPLICATION, ErrMsg("DEFAULT To WAIT_ACTIVE_TRX_FINISH"));

    /* phase1: wait all active trxs finish stored in initialActiveTrxs */
    int checkCnt = 0;
    int checkInterval = g_storageInstance->GetGuc()->updateCsnMinInterval;
    Xid *xid = nullptr;
    HASH_SEQ_STATUS status;
    uint8 hashLogLimit = 20;
    while (hash_get_num_entries(initialActiveTrxs) != 0) {
        checkCnt++;
        GaussUsleep(checkInterval);
        undoMgr->GetLocalCurrentAllActiveTrxXid(PutAllActiveTrxXidToHTAB, static_cast<void *>(currentActiveTrxs));
        /*
         * remove all no-active trxs from initialActiveTrxs.
         */
        hash_seq_init(&status, initialActiveTrxs);
        while ((xid = static_cast<Xid *>(hash_seq_search(&status))) != nullptr) {
            bool found = false;
            hash_search(currentActiveTrxs, static_cast<void *>(xid), HASH_FIND, &found);
            if (!found) {
                hash_search(initialActiveTrxs, static_cast<void *>(xid), HASH_REMOVE, &found);
            }
        }
        /*
         * collect all new trxs starting after we get initialActiveTrxs and put them into newBeginTrxs,
         * these trxs could be influenced by initialActiveTrxs if initialActiveTrxs had ddls.
         * we will also wait them finish(In phase2).
         */
        hash_seq_init(&status, currentActiveTrxs);
        while ((xid = static_cast<Xid *>(hash_seq_search(&status))) != nullptr) {
            bool found = false;
            hash_search(initialActiveTrxs, static_cast<void *>(xid), HASH_FIND, &found);
            if (!found) {
                hash_search(newBeginTrxs, static_cast<void *>(xid), HASH_ENTER, &found);
            }
            /* clean currentActiveTrxs */
            hash_search(currentActiveTrxs, static_cast<void *>(xid), HASH_REMOVE, &found);
        }
        if (checkCnt % hashLogLimit == 0) {
            ErrLog(DSTORE_LOG, MODULE_LOGICAL_REPLICATION,
                ErrMsg("Checking %d times in WAIT_ACTIVE_TRX_FINISH", checkCnt));
        }
    }
    m_state = StartPointState::WAIT_FULL_SNAPSHOT;
    ErrLog(DSTORE_LOG, MODULE_LOGICAL_REPLICATION, ErrMsg("WAIT_ACTIVE_TRX_FINISH To WAIT_FULL_SNAPSHOT"));

    /* phase2: wait all new trxs started in phase1 finish */
    hash_seq_init(&status, newBeginTrxs);
    while ((xid = static_cast<Xid *>(hash_seq_search(&status))) != nullptr) {
        bool txnFailed;
        g_storageInstance->GetPdb(m_pdbId)->GetTransactionMgr()->WaitForTransactionEnd(*xid, txnFailed);
        UNUSED_VARIABLE(txnFailed);
    }
    m_state = StartPointState::CONSISTENT;
    ErrLog(DSTORE_LOG, MODULE_LOGICAL_REPLICATION, ErrMsg("WAIT_FULL_SNAPSHOT To CONSISTENT"));
    hash_destroy(initialActiveTrxs);
    hash_destroy(currentActiveTrxs);
    hash_destroy(newBeginTrxs);
    return SyncCatalog(syncCatalogCallBack);
}

RetStatus LogicalReplicationSlot::SyncCatalog(const std::function<RetStatus(const CommitSeqNo)> syncCatalogCallBack)
{
    StorageAssert(m_state == StartPointState::CONSISTENT);
    CommitSeqNo nextCsn;
    if (STORAGE_FUNC_FAIL(g_storageInstance->GetCsnMgr()->GetNextCsn(nextCsn, false))) {
        ErrLog(DSTORE_LOG, MODULE_LOGICAL_REPLICATION, ErrMsg("failed to get next csn."));
        return DSTORE_FAIL;
    }
    m_confirmedCsn = nextCsn - 1;
    if (syncCatalogCallBack == nullptr) {
        m_state = StartPointState::WAITING_SYNC_CATALOG;
        return DSTORE_SUCC;
    }
    /* todo: write wal when sync catalog */
    return syncCatalogCallBack(m_confirmedCsn);
}

void LogicalReplicationSlot::PutAllActiveTrxXidToHTAB(Xid xid, void *arg)
{
    bool found;
    HTAB *activeTrxs = static_cast<HTAB *>(arg);
    (void)hash_search(activeTrxs, static_cast<void *>(&xid), HASH_ENTER, &found);
    UNUSED_VARIABLE(found);
}
#endif

}
