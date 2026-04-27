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
 * dstore_wal_diagnose.cpp
 *
 * Description:
 * Implement diagnose function for Wal perf and status
 * ---------------------------------------------------------------------------------------
 *
 */
#include "diagnose/dstore_wal_diagnose.h"
#include "framework/dstore_instance.h"
#include "wal/dstore_wal.h"
#include "wal/dstore_wal_dump.h"
#include "wal/dstore_wal_logstream.h"
#include "wal/dstore_wal_recovery.h"

namespace DSTORE {

static constexpr uint16 HUNDRED_PERCENTAGE = 100;
static constexpr double DOUBLE_ZERO_RANGE = 0.0000001;
const char *g_walTypeForPrint[static_cast<int>(WAL_TYPE_BUTTOM)] = {
    /* Heap wal type */
    "WAL_HEAP_INSERT",
    "WAL_HEAP_BATCH_INSERT",
    "WAL_HEAP_DELETE",
    "WAL_HEAP_INPLACE_UPDATE",
    "WAL_HEAP_SAME_PAGE_APPEND",
    "WAL_HEAP_ANOTHER_PAGE_APPEND_UPDATE_NEW_PAGE",
    "WAL_HEAP_ANOTHER_PAGE_APPEND_UPDATE_OLD_PAGE",
    "WAL_HEAP_ALLOC_TD",
    "WAL_HEAP_PRUNE",
    "WAL_HEAP_FORCE_UPDATE_TUPLE_DATA_NO_TRX",
    /* Index wal type */
    "WAL_BTREE_BUILD",
    "WAL_BTREE_INIT_META_PAGE",
    "WAL_BTREE_UPDATE_META_ROOT",
    "WAL_BTREE_UPDATE_LOWEST_SINGLE_PAGE",
    "WAL_BTREE_NEW_INTERNAL_ROOT",
    "WAL_BTREE_NEW_LEAF_ROOT",
    "WAL_BTREE_INSERT_ON_INTERNAL",
    "WAL_BTREE_INSERT_ON_LEAF",
    "WAL_BTREE_SPLIT_INTERNAL",
    "WAL_BTREE_SPLIT_LEAF",
    "WAL_BTREE_SPLIT_INSERT_INTERNAL",
    "WAL_BTREE_SPLIT_INSERT_LEAF",
    "WAL_BTREE_NEW_INTERNAL_RIGHT",
    "WAL_BTREE_NEW_LEAF_RIGHT",
    "WAL_BTREE_DELETE_ON_INTERNAL",
    "WAL_BTREE_DELETE_ON_LEAF",
    "WAL_BTREE_PAGE_PRUNE",
    "WAL_BTREE_ALLOC_TD",
    "WAL_BTREE_UPDATE_LIVESTATUS",
    "WAL_BTREE_UPDATE_SPLITSTATUS",
    "WAL_BTREE_UPDATE_LEFT_SIB_LINK",
    "WAL_BTREE_UPDATE_RIGHT_SIB_LINK",
    "WAL_BTREE_UPDATE_DOWNLINK",
    "WAL_BTREE_ERASE_INS_FOR_DEL_FLAG",
    /* Btree recycle queue wal type */
    "WAL_BTREE_RECYCLE_PARTITION_INIT_PAGE",
    "WAL_BTREE_RECYCLE_PARTITION_PUSH",
    "WAL_BTREE_RECYCLE_PARTITION_BATCH_PUSH",
    "WAL_BTREE_RECYCLE_PARTITION_POP",
    "WAL_BTREE_RECYCLE_PARTITION_ALLOC_SLOT",
    "WAL_BTREE_RECYCLE_PARTITION_WRITE_SLOT",
    "WAL_BTREE_RECYCLE_QUEUE_PAGE_META_SET_NEXT",
    "WAL_BTREE_RECYCLE_PARTITION_META_INIT",
    "WAL_BTREE_RECYCLE_PARTITION_META_SET_HEAD",
    "WAL_BTREE_RECYCLE_PARTITION_META_TIMESTAMP_UPDATE",
    "WAL_BTREE_RECYCLE_ROOT_META_INIT",
    "WAL_BTREE_RECYCLE_ROOT_META_SET_PARTITION_META",
    /* Tablespace wal type */
    "WAL_TBS_INIT_BITMAP_META_PAGE",
    "WAL_TBS_INIT_TBS_FILE_META_PAGE",
    "WAL_TBS_INIT_TBS_SPACE_META_PAGE",
    "WAL_TBS_UPDATE_TBS_FILE_META_PAGE",
    "WAL_TBS_INIT_ONE_BITMAP_PAGE",
    "WAL_TBS_INIT_BITMAP_PAGES",
    "WAL_TBS_ADD_BITMAP_PAGES",
    "WAL_TBS_BITMAP_ALLOC_BIT_START",
    "WAL_TBS_BITMAP_ALLOC_BIT_END",
    "WAL_TBS_BITMAP_FREE_BIT_START",
    "WAL_TBS_BITMAP_FREE_BIT_END",
    "WAL_TBS_EXTEND_FILE",
    "WAL_TBS_INIT_UNDO_SEGMENT_META",
    "WAL_TBS_INIT_DATA_SEGMENT_META",
    "WAL_TBS_INIT_HEAP_SEGMENT_META",
    "WAL_TBS_INIT_FSM_META",
    "WAL_TBS_INIT_FSM_PAGE",
    "WAL_TBS_INIT_EXT_META",
    "WAL_TBS_MODIFY_EXT_META_NEXT",
    "WAL_TBS_MODIFY_FSM_INDEX",
    "WAL_TBS_ADD_FSM_SLOT",
    "WAL_TBS_MOVE_FSM_SLOT",
    "WAL_TBS_SEG_ADD_EXT",
    "WAL_TBS_DATA_SEG_ADD_EXT",
    "WAL_TBS_SEG_META_ASSIGN_DATA_PAGES",
    "WAL_TBS_SEG_UNLINK_EXT",
    "WAL_TBS_SEG_META_ADD_FSM_TREE",
    "WAL_TBS_SEG_META_RECYCLE_FSM_TREE",
    "WAL_TBS_SEG_META_ADJUST_DATA_PAGES_INFO",
    "WAL_TBS_FSM_META_UPDATE_FSM_TREE",
    "WAL_TBS_FSM_META_UPDATE_NUM_USED_PAGES",
    "WAL_TBS_FSM_META_UPDATE_EXTENSION_STAT",
    "WAL_TBS_INIT_ONE_DATA_PAGE",
    "WAL_TBS_INIT_MULTIPLE_DATA_PAGES",
    "WAL_TBS_UPDATE_FIRST_FREE_BITMAP_PAGE",
    "WAL_TBS_CREATE_TABLESPACE",
    "WAL_TBS_CREATE_DATA_FILE",
    "WAL_TBS_ADD_FILE_TO_TABLESPACE",
    "WAL_TBS_DROP_TABLESPACE",
    "WAL_TBS_DROP_DATA_FILE",
    "WAL_TBS_ALTER_TABLESPACE",
    /* Checkpoint wal type */
    "WAL_CHECKPOINT_SHUTDOWN",
    "WAL_CHECKPOINT_ONLINE",
    /* Undo wal type */
    "WAL_UNDO_INIT_MAP_SEGMENT",
    "WAL_UNDO_SET_ZONE_SEGMENT_ID",
    "WAL_UNDO_INSERT_RECORD",
    "WAL_UNDO_INIT_RECORD_SPACE",
    "WAL_UNDO_EXTEND_PAGE_RING_PREV_PAGE",
    "WAL_UNDO_EXTEND_PAGE_RING_NEXT_PAGE",
    "WAL_UNDO_EXTEND_PAGE_RING_NEW_PAGE",
    "WAL_UNDO_INIT_TXN_PAGE",
    "WAL_UNDO_UPDATE_TXN_SLOT_PTR",
    "WAL_UNDO_ALLOCATE_TXN_SLOT",
    "WAL_UNDO_SET_TXN_PAGE_INITED",
    /* The wal for undo heap DML */
    "WAL_UNDO_HEAP",
    "WAL_UNDO_BTREE",
    "WAL_UNDO_HEAP_PAGE_ROLL_BACK",
    "WAL_UNDO_BTREE_PAGE_ROLL_BACK",
    /* The wal for recycle undo */
    "WAL_UNDO_RECYCLE_TXN_SLOT",
    /* Transaction wal type */
    "WAL_TXN_COMMIT",
    "WAL_TXN_ABORT",
    /* Logical replication wal type */
    "WAL_NEXT_CSN",
    "WAL_BARRIER_CSN",
    /* Systable wal type */
    "WAL_SYSTABLE_WRITE_BUILTIN_RELMAP",
#ifdef UT
    /* XLog reserved wal type */
    "WAL_EMPTY_REDO"
    "WAL_EMPTY_DDL_REDO"
#endif
};

#ifdef UT
static_assert(static_cast<int>(WAL_TYPE_BUTTOM) == 112, "g_walTypeForPrint must be the same as WalType");
#else
static_assert(static_cast<int>(WAL_TYPE_BUTTOM) == 110, "g_walTypeForPrint must be the same as WalType");
#endif

void WalStreamStateInfo::Init()
{
    walId = INVALID_WAL_ID;
    pdbId = INVALID_PDB_ID;
    nodeId = INVALID_NODE_ID;
    errno_t nRet = memset_s(usage, MAX_STATE_INFO_LEN, 0, MAX_STATE_INFO_LEN);
    storage_securec_check(nRet, "\0", "\0");
    nRet = memset_s(state, MAX_STATE_INFO_LEN, 0, MAX_STATE_INFO_LEN);
    storage_securec_check(nRet, "\0", "\0");
    walFileCount =  0;
    nRet = memset_s(headFileName, MAX_WAL_FILE_NAME_LEN, 0, MAX_WAL_FILE_NAME_LEN);
    storage_securec_check(nRet, "\0", "\0");
    nRet = memset_s(tailFileName, MAX_WAL_FILE_NAME_LEN, 0, MAX_WAL_FILE_NAME_LEN);
    storage_securec_check(nRet, "\0", "\0");
    maxAppendedPlsn = INVALID_PLSN;
    maxWrittenToFilePlsn = INVALID_PLSN;
    maxFlushFinishPlsn = INVALID_PLSN;
    term = 0;
    nRet = memset_s(redoMode, MAX_STATE_INFO_LEN, 0, MAX_STATE_INFO_LEN);
    storage_securec_check(nRet, "\0", "\0");
    redoStartPlsn = INVALID_PLSN;
    redoFinishedPlsn = INVALID_PLSN;
    nRet = memset_s(redoStage, MAX_STATE_INFO_LEN, 0, MAX_STATE_INFO_LEN);
    storage_securec_check(nRet, "\0", "\0");
    redoDonePlsn = INVALID_PLSN;
}

void WalStreamStateInfo::Copy(const WalStreamStateInfo &info)
{
    walId = info.walId;
    pdbId = info.pdbId;
    nodeId = info.nodeId;
    errno_t nRet = memcpy_s(usage, MAX_STATE_INFO_LEN, info.usage, MAX_STATE_INFO_LEN);
    storage_securec_check(nRet, "\0", "\0");
    nRet = memcpy_s(state, MAX_STATE_INFO_LEN, info.state, MAX_STATE_INFO_LEN);
    storage_securec_check(nRet, "\0", "\0");
    walFileCount =  info.walFileCount;
    nRet = memcpy_s(headFileName, MAX_WAL_FILE_NAME_LEN, info.headFileName, MAX_WAL_FILE_NAME_LEN);
    storage_securec_check(nRet, "\0", "\0");
    nRet = memcpy_s(tailFileName, MAX_WAL_FILE_NAME_LEN, info.tailFileName, MAX_WAL_FILE_NAME_LEN);
    storage_securec_check(nRet, "\0", "\0");
    maxAppendedPlsn =  info.maxAppendedPlsn;
    maxWrittenToFilePlsn =  info.maxWrittenToFilePlsn;
    maxFlushFinishPlsn =  info.maxFlushFinishPlsn;
    term =  info.term;
    nRet = memcpy_s(redoMode, MAX_STATE_INFO_LEN, info.redoMode, MAX_STATE_INFO_LEN);
    storage_securec_check(nRet, "\0", "\0");
    redoStartPlsn =  info.redoStartPlsn;
    redoFinishedPlsn =  info.redoFinishedPlsn;
    nRet = memcpy_s(redoStage, MAX_STATE_INFO_LEN, info.redoStage, MAX_STATE_INFO_LEN);
    storage_securec_check(nRet, "\0", "\0");
    redoDonePlsn =  info.redoDonePlsn;
}

void WalRedoInfo::Init()
{
    progress = 0;
    recovery_start_plsn = 0;
    recovery_end_plsn = 0;
    curr_redo_plsn = 0;
    redo_speed = 0;
}

void WalReadIoStat::Copy(const WalReadIoStat &walReadIoStat)
{
    GsAtomicWriteU64(&readCount, walReadIoStat.readCount);
    GsAtomicWriteU64(&readLen, walReadIoStat.readLen);
    GsAtomicWriteU64(&actualReadLen, walReadIoStat.actualReadLen);
    GsAtomicWriteU64(&readTime, walReadIoStat.readTime);
    GsAtomicWriteU64(&waitCount, walReadIoStat.waitCount);
    GsAtomicWriteU64(&waitTime, walReadIoStat.waitTime);
}

void WalWriteIoStat::Copy(const WalWriteIoStat &walWriteIoStat)
{
    writeCount = walWriteIoStat.writeCount;
    writeLen = walWriteIoStat.writeLen;
    writeTime = walWriteIoStat.writeTime;
    syncCount = walWriteIoStat.syncCount;
    syncLen = walWriteIoStat.syncLen;
    syncTime = walWriteIoStat.syncTime;
    waitCount = walWriteIoStat.waitCount;
    waitTime = walWriteIoStat.waitTime;
}

char *WalDiagnose::WalDump(PdbId pdbId, uint32 dumpType, uint64 id1, uint32 id2)
{
    StringInfoData dumpInfo;
    while (!dumpInfo.init()) {
        GaussUsleep(1000);
    }

    WalDumpConfig config;
    WalDumper::InitWalDumpConfig(config);
    config.vfs = g_storageInstance->GetPdb(pdbId)->GetVFS();
    config.vfsType = g_storageInstance->GetGuc()->tenantConfig->storageConfig.type;
    config.reuseVfs = config.vfsType == StorageType::PAGESTORE;
    char *pdbPath = StorageInstance::GetPdbPath(g_storageInstance->GetGuc()->dataDir, pdbId);
    if (likely(pdbPath != nullptr)) {
        int rc = sprintf_s(config.dir, MAXPGPATH, "%s", pdbPath);
        storage_securec_check_ss(rc)
        DstorePfree(pdbPath);
    }

    if (dumpType == static_cast<uint32>(DSTORE::DumpType::DUMP_ONE_PAGE)) {
        if (id1 == 0 || id1 > UINT16_MAX) {
            dumpInfo.append("Dump failed: [Type: dump page], [Page id: file id %u, block id %u] invali args. \n",
                static_cast<uint16>(id1), id2);
            free(config.pdbVfsName);
            return dumpInfo.data;
        }
        dumpInfo.append("Dump information: [Type: dump page], [Page id: file id %u, block id %u] \n",
            static_cast<uint16>(id1), id2);
        config.pageIdFilter = {static_cast<uint16>(id1), id2};
    } else if (dumpType == static_cast<uint32>(DSTORE::DumpType::DUMP_ONE_STREAM)) {
        if (id2 != 0) {
            dumpInfo.append("Dump information: [Type: dump stream], id2 is not zero. \n");
            free(config.pdbVfsName);
            return dumpInfo.data;
        }
        if (id1 != 0) {
            dumpInfo.append("Dump information: [Type: dump stream], [Stream id: %lu] \n", id1);
            config.walId = id1;
        } else {
            dumpInfo.append("Dump information: [Type: dump all stream]] \n");
        }
    } else if (dumpType == static_cast<uint32>(DSTORE::DumpType::CHECK_ONE_PAGE)) {
        if (id1 == 0 || id1 > UINT16_MAX) {
            dumpInfo.append("Dump failed: [Type: check page], [Page id: file id %u, block id %u] invali args. \n",
                static_cast<uint16>(id1), id2);
            free(config.pdbVfsName);
            return dumpInfo.data;
        }
        dumpInfo.append("Dump information: [Type: check page], [Page id: file id %u, block id %u] \n",
            static_cast<uint16>(id1), id2);
        config.pageIdFilter = {static_cast<uint16>(id1), id2};
        config.checkPageError = true;
    } else {
        dumpInfo.append("Dump type: %u invalid \n", dumpType);
        free(config.pdbVfsName);
        return dumpInfo.data;
    }

    WalDumper::DumpToLocalFile(&config, &dumpInfo);
    free(config.pdbVfsName);
    return dumpInfo.data;
}

void WalDiagnose::FreeWalStreamStateInfo(WalStreamStateInfo *walStreamInfo)
{
    DstorePfreeExt(walStreamInfo);
}

RetStatus WalDiagnose::GetWalStreamInfoLocally(PdbId pdbId, WalStreamStateInfo **walStreamInfo,
    uint32_t *walStreamCount, char **errorMsg)
{
    StoragePdb *pdb = g_storageInstance->GetPdb(pdbId);
    StringInfoData dumpInfo;
    if (pdb == nullptr || (pdb->GetWalMgr() == nullptr) || (pdb->GetWalMgr()->GetWalStreamManager() == nullptr)) {
        while (!dumpInfo.init()) {
            GaussUsleep(1000);
        }
        dumpInfo.append("PDB:%u not init in node %u \n", pdbId, g_storageInstance->GetGuc()->selfNodeId);
        *errorMsg = dumpInfo.data;
        return DSTORE_FAIL;
    }
    if (STORAGE_FUNC_FAIL(
            pdb->GetWalMgr()->GetWalStreamManager()->GetAllWalStreamInfo(walStreamInfo, walStreamCount))) {
        dumpInfo.init();
        dumpInfo.append("GetAllWalStreamInfo failed because of out of memory \n");
        *errorMsg = dumpInfo.data;
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus WalDiagnose::GetAllWalStreamInfo(PdbId pdbId, WalStreamStateInfo **walStreamInfo, uint32_t *walStreamCount,
    char **errorMsg)
{
    WalId *walIdArray = nullptr;
    StringInfoData dumpInfo;
    *errorMsg = nullptr;
    StoragePdb *pdb = g_storageInstance->GetPdb(pdbId);
    if (STORAGE_VAR_NULL(pdb)) {
        while (!dumpInfo.init()) {
            GaussUsleep(1000);
        }
        dumpInfo.append("Get pdb failed, pdbId(%u)\n", pdbId);
        *errorMsg = dumpInfo.data;
        return DSTORE_FAIL;
    }
    ControlFile *controlFile = dynamic_cast<ControlFile *>(pdb->GetControlFile());
    uint32 walIdCount = controlFile->GetAllWalStreams(&walIdArray);
    *walStreamInfo = static_cast<WalStreamStateInfo *>(DstorePalloc0(sizeof(WalStreamStateInfo) * walIdCount));
    if (STORAGE_VAR_NULL(walStreamInfo)) {
        while (!dumpInfo.init()) {
            GaussUsleep(1000);
        }
        dumpInfo.append("Alloc memory failed for WalStreamStateInfo \n");
        *errorMsg = dumpInfo.data;
        DstorePfreeExt(walIdArray);
        return DSTORE_FAIL;
    }

    for (uint32 i = 0; i < walIdCount; i++) {
        (*walStreamInfo)[i].Init();
        (*walStreamInfo)[i].walId = walIdArray[i];
    }
    DstorePfreeExt(walIdArray);

    WalStreamStateInfo *walStreamInfoLocal = nullptr;
    uint32 walStreamCountLocal = 0;
    if (STORAGE_FUNC_FAIL(GetWalStreamInfoLocally(pdbId, &walStreamInfoLocal, &walStreamCountLocal, errorMsg))) {
        DstorePfreeExt(walStreamInfoLocal);
        return DSTORE_FAIL;
    }
    for (uint32 i = 0; i < walIdCount; i++) {
        for (uint32 j = 0; j < walStreamCountLocal; j++) {
            if ((*walStreamInfo)[i].walId ==  walStreamInfoLocal[j].walId) {
                (*walStreamInfo)[i].Copy(walStreamInfoLocal[j]);
            }
        }
    }
    DstorePfreeExt(walStreamInfoLocal);

    *walStreamCount = walIdCount;
    return DSTORE_SUCC;
}

RetStatus WalDiagnose::GetWalRedoInfoLocally(PdbId pdbId, WalId walId,
                                             WalRedoInfo *walRedoInfo, char **errorMsg, bool *walstreamExist)
{
    *walstreamExist = false;
    StoragePdb *pdb = g_storageInstance->GetPdb(pdbId);
    StringInfoData dumpInfo;
    dumpInfo.init();
    if (pdb == nullptr || (pdb->GetWalMgr() == nullptr) || (pdb->GetWalMgr()->GetWalStreamManager() == nullptr)) {
        dumpInfo.append("PDB:%u not init in node %u \n", pdbId, g_storageInstance->GetGuc()->selfNodeId);
        *errorMsg = dumpInfo.data;
        return DSTORE_FAIL;
    }
    if (pdb->GetWalMgr()->GetWalStreamManager()->GetWalStream(walId) == nullptr) {
        dumpInfo.append("not find wal stream %lu in node %u \n", walId, g_storageInstance->GetGuc()->selfNodeId);
        *errorMsg = dumpInfo.data;
        return DSTORE_SUCC;
    }
    *walstreamExist = true;
    if (STORAGE_FUNC_FAIL(pdb->GetWalMgr()->GetWalStreamManager()->GetWalStream(walId)->GetRedoInfo(walRedoInfo))) {
        dumpInfo.append("GetAllWalStreamInfo failed because of out of memory \n");
        *errorMsg = dumpInfo.data;
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus WalDiagnose::GetWalRedoInfo(PdbId pdbId, uint64 walId, WalRedoInfo *walRedoInfo, char **errorMsg)
{
    walRedoInfo->Init();
    StringInfoData dumpInfo;
    dumpInfo.init();
    bool walstreamExist;
    if (STORAGE_FUNC_FAIL(GetWalRedoInfoLocally(pdbId, walId, walRedoInfo, errorMsg, &walstreamExist))) {
        return DSTORE_FAIL;
    }
    if (!walstreamExist) {
        dumpInfo.append("Could not find wal stream %lu in PDB %u\n", walId, pdbId);
        *errorMsg = dumpInfo.data;
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

char *WalDiagnose::GetWalRedoStat(uint64_t timeVal)
{
    StringInfoData dumpInfo;
    dumpInfo.init();
    dumpInfo.append("collect_start_time = %lu, ", static_cast<Timestamp>(time(nullptr)));
    WalRecovery::StartStatRedo();
    GaussUsleep(timeVal * 1000000);
    WalRecovery::StopStatRedo();
    dumpInfo.append("collect_end_time = %lu\n", static_cast<Timestamp>(time(nullptr)));
    for (uint16 type = 0; type < static_cast<uint16>(WAL_TYPE_BUTTOM); type++) {
        if (WalRecovery::GetWalTypeRedoCount(type) == 0) {
            continue;
        }
        dumpInfo.append("wal_type = %s, num = %u, avg_redo_time = %.5f\n", g_walTypeForPrint[type],
                        WalRecovery::GetWalTypeRedoCount(type), WalRecovery::GetWalTypeRedoAvgTime(type));
    }
    WalRecovery::ClearRedoStat();
    return dumpInfo.data;
}

RetStatus WalDiagnose::StopCollectWalReadIoStat(PdbId pdbId, WalId walId, NodeId nodeId, WalReadIoStat *walReadIoStat,
    char **errorMsg)
{
    StringInfoData dumpInfo;
    *errorMsg = nullptr;
    NodeId selfNodeId = g_storageInstance->GetGuc()->selfNodeId;
    if (nodeId == selfNodeId) {
        StoragePdb *pdb = g_storageInstance->GetPdb(pdbId);
        if (pdb == nullptr || (pdb->GetWalMgr() == nullptr) || (pdb->GetWalMgr()->GetWalStreamManager() == nullptr)) {
            dumpInfo.init();
            dumpInfo.append("PDB:%u not init in node %u \n", pdbId, nodeId);
            *errorMsg = dumpInfo.data;
            return DSTORE_FAIL;
        }
        /* stop collect locally */
        bool found = false;
        if (STORAGE_FUNC_FAIL(
            pdb->GetWalMgr()->GetWalStreamManager()->StopCollectWalReadIoStat(walId, walReadIoStat, &found))) {
            dumpInfo.init();
            if (found) {
                dumpInfo.append("PDB:%u Wal:%lu not start collect\n", pdbId, walId);
            } else {
                dumpInfo.append("PDB:%u Wal:%lu not found in node:%u\n", pdbId, walId, nodeId);
            }
            *errorMsg = dumpInfo.data;
            return DSTORE_FAIL;
        }
        return DSTORE_SUCC;
    }

    dumpInfo.init();
    dumpInfo.append("Cannot found PDB:%u Wal:%lu\n", pdbId, walId);
    *errorMsg = dumpInfo.data;
    return DSTORE_FAIL;
}

RetStatus WalDiagnose::StartCollectWalWriteIoStat(PdbId pdbId, WalId walId, char **errorMsg, NodeId *nodeId)
{
    StringInfoData dumpInfo;
    *errorMsg = nullptr;
    StoragePdb *pdb = g_storageInstance->GetPdb(pdbId);
    if (pdb == nullptr || (pdb->GetWalMgr() == nullptr) || (pdb->GetWalMgr()->GetWalStreamManager() == nullptr)) {
        dumpInfo.init();
        dumpInfo.append("PDB:%u not init in node %u \n", pdbId, g_storageInstance->GetGuc()->selfNodeId);
        *errorMsg = dumpInfo.data;
        return DSTORE_FAIL;
    }

    /* start collect locally */
    NodeId selfNodeId = g_storageInstance->GetGuc()->selfNodeId;
    bool found = false;
    if (STORAGE_FUNC_FAIL(pdb->GetWalMgr()->GetWalStreamManager()->StartCollectWalWriteIoStat(walId, &found))) {
        if (found) {
            dumpInfo.init();
            dumpInfo.append("PDB:%u Wal:%lu is not use for write\n", pdbId, walId);
            *errorMsg = dumpInfo.data;
            return DSTORE_FAIL;
        }
    } else {
        *nodeId = selfNodeId;
        return DSTORE_SUCC;
    }

    dumpInfo.init();
    dumpInfo.append("Cannot found PDB:%u Wal:%lu\n", pdbId, walId);
    *errorMsg = dumpInfo.data;
    return DSTORE_FAIL;
}

RetStatus WalDiagnose::StopCollectWalWriteIoStat(PdbId pdbId, WalId walId, NodeId nodeId,
    WalWriteIoStat *walWriteIoStat, char **errorMsg)
{
    StringInfoData dumpInfo;
    *errorMsg = nullptr;
    NodeId selfNodeId = g_storageInstance->GetGuc()->selfNodeId;
    if (nodeId == selfNodeId) {
        StoragePdb *pdb = g_storageInstance->GetPdb(pdbId);
        if (pdb == nullptr || (pdb->GetWalMgr() == nullptr) || (pdb->GetWalMgr()->GetWalStreamManager() == nullptr)) {
            dumpInfo.init();
            dumpInfo.append("PDB:%u not init in node %u \n", pdbId, nodeId);
            *errorMsg = dumpInfo.data;
            return DSTORE_FAIL;
        }
        /* stop collect locally */
        bool found = false;
        if (STORAGE_FUNC_FAIL(
            pdb->GetWalMgr()->GetWalStreamManager()->StopCollectWalWriteIoStat(walId, walWriteIoStat, &found))) {
            dumpInfo.init();
            if (found) {
                dumpInfo.append("PDB:%u Wal:%lu not start collect\n", pdbId, walId);
            } else {
                dumpInfo.append("PDB:%u Wal:%lu not found in node:%u\n", pdbId, walId, nodeId);
            }
            *errorMsg = dumpInfo.data;
            return DSTORE_FAIL;
        }
        return DSTORE_SUCC;
    }

    dumpInfo.init();
    dumpInfo.append("Cannot found PDB:%u Wal:%lu\n", pdbId, walId);
    *errorMsg = dumpInfo.data;
    return DSTORE_FAIL;
}
}
