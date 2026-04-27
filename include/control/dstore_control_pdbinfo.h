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
 * dstore_control_pdbinfo.h
 *  ControlFiles records pdb metadata
 *
 *
 * IDENTIFICATION
 *        dstore/include/control/dstore_control_pdbinfo.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_CONTROL_PDBINFO_H
#define DSTORE_CONTROL_PDBINFO_H

#include "control/dstore_control_struct.h"
#include "config/dstore_vfs_config.h"
#include "defines/cluster_defs.h"
#include "common/algorithm/dstore_string_info.h"
#include "control/dstore_control_file_mgr.h"
#include "common/dstore_common_utils.h"
#include "control/dstore_control_group.h"
namespace DSTORE {
struct ControlPdbInfoMeta {
    uint32 m_version;
    uint32 m_totalPdbCount;
} PACKED;
struct PdbInfoItemPointerData {
    BlockNumber blkno;
    OffsetNumber offset;
} PACKED;
enum PdbMessageType : uint8 {
    REQUEST_PAGE_BUFFER_INVALIDATION = 1,
    REQUEST_PDB_STATUS_INVALIDATION,
    REQUEST_PDB_STATUS_RELOAD,
    REQUEST_PDB_SYNC_MODE_INVALIDATION,
    REQUEST_PDB_SYNC_MODE_RELOAD,
    REQUEST_PDB_STOP_SQL_THREAD,
    REQUEST_PDB_STOP_BG_THREADS_PHASE1,
    REQUEST_PDB_STOP_BG_THREADS_PHASE2,
    REQUEST_PDB_RESET,
    REQUEST_PDB_RESET_TIME,
};
struct PdbInfoInvalidateData {
    uint32 m_version;
    uint8 m_type;
    uint8 m_pad[3];
    uint32 m_pdbId;
    uint32 m_standbyClusterId;
    PdbInfoInvalidateData(uint8 type, uint32 pdbId, uint32 standbyClusterId = 0)
    {
        m_version = 0;
        m_type = type;
        m_pad[0] = 0;
        m_pad[1] = 0;
        m_pad[2] = 0;
        m_pdbId = pdbId;
        m_standbyClusterId = standbyClusterId;
    }
} PACKED;

constexpr uint16 PDB_NAME_LEN = 256;
constexpr uint16 USER_NAME_LEN = 128;
constexpr uint32_t INVALID_HEARTBEAT_TIMEOUT_INTERVAL = UINT32_MAX;
constexpr uint32_t INVALID_WAL_SIZE_THRESHOLD = UINT32_MAX;
constexpr uint64_t INVALID_PDB_TIMESTAMP = 0;
constexpr CommitSeqNo INVALID_PDB_CSN = 0;

struct PdbHashIndexEntry {
    PdbId pdbId;
    char pdbName[PDB_NAME_LEN];
    PdbStatus pdbStatus;
    PdbInfoItemPointerData pdbItemPointer;
} PACKED;

/* Identifies the steps of rebuilding standby pdb */
enum class PdbReplicaRebuildProgress : uint8 {
    INVALID = 0,
    NO_REPLICA_REBUILD,
    SEARCH_PRIMARY_PDB,
    FOUND_PRIMARY_PDB,
    REQUEST_TRANS_DATA_WAL,
    START_DATA_WAL_TRANS,
    FINISH_DATA_TRANS,
    FINISH_REBUILD
};

/* String representations for PdbRebuildProgress enum values */
constexpr const char* g_pdbRebuildProgressStr[] = {
    "INVALID",
    "NO_REPLICA_REBUILD",
    "SEARCH_PRIMARY_PDB",
    "FOUND_PRIMARY_PDB",
    "REQUEST_TRANS_DATA_WAL",
    "START_DATA_WAL_TRANS",
    "FINISH_DATA_TRANS",
    "FINISH_REBUILD"
};

struct StandbyPdbInfo {
    uint32 clusterId;
    char clusterName[CLUSTER_NAME_MAX_LEN];
    char pdbName[PDB_NAME_LEN];        /* DRTODO: used later. */
    char vfsName[VFS_NAME_MAX_LEN];
    char storeSpaceName[STORESPACE_NAME_MAX_LEN];
    PdbSyncMode pdbSyncMode;
    PdbSyncMode actualSyncMode;
    uint32 walSizeThreshold;
} PACKED;

/* used for rootDb */
struct ControlPdbInfoPageItemData {
    PdbId pdbId;
    char pdbUuid[FORMATTED_UUID_ARR_LEN];
    PdbStatus pdbStatus;
    char pdbName[PDB_NAME_LEN];
    char vfsName[MAX_CONFIG_NAME_LENGTH];
    PdbRoleMode pdbRoleMode;
    PdbReplicaRebuildProgress pdbReplicaStatus;
    PdbSwitchStatus pdbSwitchStatus;
    bool isFullRepair;
    CommitSeqNo pdbCsn;
    uint64 pdbTimestamp;
    StandbyPdbInfo standbyPdbInfo[MAX_CLUSTER_COUNT];
    bool isInRestoreFromBackup;
    Oid grpRespoolOid; /* group respool oid of pdb */
    Oid dbaId;         /* pdb dba user oid */
    uint8 version;
    CommitSeqNo pdbRecycleCsnMin;
    uint8 reserved[120];

    static void Dump(void *item, StringInfoData &dumpInfo)
    {
        ControlPdbInfoPageItemData *data = static_cast<ControlPdbInfoPageItemData *>(item);
        dumpInfo.append("pdbId %u, pdbUuid %s, pdbName %s, pdbStatus %hhu, vfsName %s,"
                        "pdbRoleMode %hhu, pdbReplicaStatus %hhu, pdbCsn %lu, pdbTimestamp %lu,"
                        "isInRestoreFromBackup %hhu, grpRespoolOid %u, dbaId %u, version %u, pdbRecycleCsnMin %lu.\n",
                        data->pdbId, data->pdbUuid, data->pdbName, static_cast<uint8>(data->pdbStatus), data->vfsName,
                        static_cast<uint8>(data->pdbRoleMode), static_cast<uint8>(data->pdbReplicaStatus),
                        static_cast<uint64>(data->pdbCsn), static_cast<uint64>(data->pdbTimestamp),
                        static_cast<uint8>(data->isInRestoreFromBackup), data->grpRespoolOid, data->dbaId,
                        data->version, data->pdbRecycleCsnMin);
        /* Append standby info */
        for (uint32 i = 0; i < MAX_CLUSTER_COUNT; i++) {
            StandbyPdbInfo *info = &data->standbyPdbInfo[i];
            dumpInfo.append("standby#%u: clusterId=%u, clusterName=%s, pdbName=%s, vfsName=%s, storeSpaceName=%s, "
                "syncMode=%hhu, actualSyncMode=%hhu, walSizeThreshold=%u(MB)\n",
                i, info->clusterId, info->clusterName, info->pdbName, info->vfsName, info->storeSpaceName,
                static_cast<uint8>(info->pdbSyncMode), static_cast<uint8>(info->actualSyncMode),
                info->walSizeThreshold >> BYTE_TO_MB_SHIFT);
        }
    }

    static inline RetStatus Init(ControlPdbInfoPageItemData *item, PdbId thePdbId, const char *thePdbUuid,
                                 PdbStatus pdbStatus, const char *thePdbName, const char *theVfsName)
    {
        item->pdbId = thePdbId;
        errno_t rc = memset_s(item->pdbUuid, FORMATTED_UUID_ARR_LEN, 0, FORMATTED_UUID_ARR_LEN);
        storage_securec_check(rc, "\0", "\0");
        rc = strncpy_s(item->pdbUuid, FORMATTED_UUID_ARR_LEN, thePdbUuid, strlen(thePdbUuid));
        storage_securec_check(rc, "\0", "\0");
        item->pdbStatus = pdbStatus;
        rc = strncpy_s(item->pdbName, PDB_NAME_LEN, thePdbName, strlen(thePdbName));
        storage_securec_check(rc, "\0", "\0");
        rc = strncpy_s(item->vfsName, MAX_CONFIG_NAME_LENGTH, theVfsName, strlen(theVfsName));
        storage_securec_check(rc, "\0", "\0");
        item->pdbRoleMode = PdbRoleMode::PDB_PRIMARY;
        item->pdbReplicaStatus = PdbReplicaRebuildProgress::NO_REPLICA_REBUILD;
        item->pdbSwitchStatus = PdbSwitchStatus::NORMAL;
        for (uint32 i = 0; i < MAX_CLUSTER_COUNT; i++) {
            item->standbyPdbInfo[i].clusterId = INVALID_CLUSTER_ID;
            item->standbyPdbInfo[i].pdbSyncMode = PdbSyncMode::INVALID_SYNC_MODE;
            item->standbyPdbInfo[i].actualSyncMode = PdbSyncMode::MAX_PERFORMANCE_MODE;
            item->standbyPdbInfo[i].walSizeThreshold = INVALID_WAL_SIZE_THRESHOLD;
            rc = strcpy_s(item->standbyPdbInfo[i].clusterName, CLUSTER_NAME_MAX_LEN, "");
            storage_securec_check(rc, "\0", "\0");
            rc = strcpy_s(item->standbyPdbInfo[i].vfsName, VFS_NAME_MAX_LEN, "");
            storage_securec_check(rc, "\0", "\0");
            rc = strcpy_s(item->standbyPdbInfo[i].storeSpaceName, STORESPACE_NAME_MAX_LEN, "");
            storage_securec_check(rc, "\0", "\0");
            rc = strcpy_s(item->standbyPdbInfo[i].pdbName, PDB_NAME_LEN, "");
            storage_securec_check(rc, "\0", "\0");
        }
        item->isFullRepair = false;
        item->pdbCsn = INVALID_PDB_CSN;
        item->pdbTimestamp = INVALID_PDB_TIMESTAMP;
        item->isInRestoreFromBackup = false;
        item->grpRespoolOid = 0;
        item->version = 0;
        item->pdbRecycleCsnMin = INVALID_CSN;
        return DSTORE_SUCC;
    }

    static bool IsPdbStatusChanging(ControlPdbInfoPageItemData *pdbInfo)
    {
        return (pdbInfo != nullptr && pdbInfo->pdbId >= FIRST_USER_PDB_ID &&
                (pdbInfo->pdbSwitchStatus == PdbSwitchStatus::WAIT_PRIMARY_DEMOTE ||
                 pdbInfo->pdbSwitchStatus == PdbSwitchStatus::BEGIN_DEMOTE ||
                 pdbInfo->pdbSwitchStatus == PdbSwitchStatus::BEGIN_PROMOTE));
    }

    inline bool AlreadyClosed()
    {
        return this->pdbStatus == PdbStatus::PDB_STATUS_CLOSED;
    }
};
constexpr OffsetNumber PDB_ITEM_PER_CONTROLFILE =
    (((BLCKSZ - sizeof(ControlPageHeader)) / sizeof(ControlPdbInfoPageItemData)));
class ControlPdbInfo : public ControlGroup {
public:
    ControlPdbInfo(ControlFileMgr *controlFileMgr, DstoreMemoryContext memCtx, PdbId pdbId)
        : ControlGroup(controlFileMgr, memCtx, CONTROL_GROUP_TYPE_PDBINFO, CONTROLFILE_PAGEMAP_PDBINFO_META, pdbId),
          m_pdbIdxAvailable(false),
          m_pdbHashIndex(nullptr),
          m_pdbIndexLock(),
          m_deployType(CONTROL_FILE_SINGLE_NODE)
    {}
    ~ControlPdbInfo()
    {}
    DISALLOW_COPY_AND_MOVE(ControlPdbInfo);
    RetStatus Init(DeployType deployType)
    {
        m_deployType = deployType;
        if (unlikely(ControlGroup::Init(deployType) != DSTORE_SUCC)) {
            return DSTORE_FAIL;
        }
        LWLockInitialize(&m_pdbIndexLock, LWLOCK_GROUP_PDB_INDEX);
        m_isInitialized = true;
        return DSTORE_SUCC;
    }
    RetStatus Create();
    void Reload()
    {}
    RetStatus AllocPdbId(const char *pdbName, PdbId &pdbId, bool isSysPdb = false, const char* vfsName = nullptr);
    RetStatus GetPdbRoleMode(PdbId pdbId, PdbRoleMode &pdbRoleMode);
    RetStatus GetPdbRecycleCsnMin(PdbId pdbId, CommitSeqNo &pdbRecycleCsnMin);
    RetStatus UpdatePdbStatus(PdbId pdbId, PdbStatus pdbStatus);
    RetStatus UpdatePdbReplicaStatus(PdbId pdbId, PdbReplicaRebuildProgress newPdbReplicaStatus);
    RetStatus GetPdbReplicaStatus(PdbId pdbId, PdbReplicaRebuildProgress &rebuildProgress);
    RetStatus UpdatePdbSwitchStatus(PdbId pdbId, PdbSwitchStatus newPdbSwitchStatus);
    RetStatus GetPdbSwitchStatus(PdbId pdbId, PdbSwitchStatus &pdbSwitchStatus);
    RetStatus GetStandbyPdbInfo(PdbId pdbId, uint32 standbyClusterId, StandbyPdbInfo &standbyPdbInfo);

    RetStatus RegisterNewStandby(PdbId pdbId, StandbyPdbInfo standbyPdbInfo);
    RetStatus ClearStandbyPdbSlotByClusterId(PdbId pdbId, uint32 standbyClusterId);
    RetStatus UpdateStandbyPdbClusterName(PdbId pdbId, uint32 standbyClusterId, const char *clusterName);
    RetStatus UpdateStandbyPdbVfsName(PdbId pdbId, uint32 standbyClusterId, const char *vfsName);
    RetStatus UpdateStandbyPdbStoreSpaceName(PdbId pdbId, uint32 standbyClusterId, const char *storeSpaceName);
    RetStatus UpdatePdbWalSyncMode(PdbId pdbId, uint32 standbyClusterId, PdbSyncMode syncMode, uint32 walSizeThreshold,
        bool updateActualSyncMode);
    RetStatus UpdateStandbySyncModeAndWalSize(PdbId pdbId, PdbSyncMode syncMode, uint32 walSizeThreshold);

    RetStatus SetDeleteFlag(PdbId pdbId);
    RetStatus FreePdbId(PdbId pdbId);
    RetStatus UpdateTotalPdbCount(bool isIncrease);
    RetStatus GetPdbInfoById(PdbId pdbId, PdbInfo *pdbInfo);
    void Create(uint32 initPageCount, uint32 version);
    RetStatus Init(ControlFileMgr *controlFileMgr);

    ControlPdbInfoPageItemData *GetAvailablePdbInfo(const char *pdbName);
    ControlPdbInfoPageItemData *GetAvailablePdbInfo(PdbId pdbId);
    ControlPdbInfoPageItemData *GetPdbInfoById(PdbId pdbId);
    ControlPdbInfoPageItemData *GetPdbInfo(const char *pdbName, PdbStatus pdbStatus);
    ControlPdbInfoPageItemData *GetPdbInfo(PdbId pdbId, PdbStatus pdbStatus);
    ControlPdbInfoPageItemData *GetPdbInfo(const char *pdbName);
    RetStatus UpdatePdbItemData(PdbId pdbId, ControlPdbInfoPageItemData *pdbInfo, size_t pdbInfoLen);

    RetStatus UpdatePdbRoleMode(PdbId pdbId, PdbRoleMode pdbRoleMode);
    RetStatus UpdatePdbRecycleCsnMin(PdbId pdbId, CommitSeqNo pdbRecycleCsnMin);
    RetStatus SetOpenedFlag(PdbId pdbId);
    RetStatus GetAvailablePdbIdByUuid(const char *pdbUuid, PdbId *pdbId);
    RetStatus GetPdbIdByUuid(const char *pdbUuid, PdbId *pdbId, PdbStatus pdbStatus);
    FUNCTION_DELETE_LATER
    RetStatus GetPdbIdByName(const char *pdbName, PdbId *pdbId, PdbStatus pdbStatus);
    RetStatus IsPdbInRestoreFromBackup(const char *pdbName, bool *isInRestoreFromBackup);
    RetStatus UpdatePdbInRestoreFromBackup(PdbId pdbId, bool isPdbInRestoreFromBackup);
    inline bool GetPdbIdxAvailable()
    {
        return m_pdbIdxAvailable.load(std::memory_order_acquire);
    }

    RetStatus UpdateAllPdbInfoAfterRestore(void *allPdbInfo);
    RetStatus GetAllPdbIdForBackupRestore(PdbId *pdbIdArray, uint32 &count);
    void InvalidatePdbIdxIfNeed(BlockNumber blkno);
    RetStatus InvalidatePdbInfo(const void* data, uint32 dataLen);
    ControlPdbInfoPageItemData *GetDropablePdbInfo(const char *pdbName);
    RetStatus ReadPdbCxt(PdbId pdbId);
    RetStatus GetVisiablePdbInfoAll(PdbBriefInfo *pdbIdInfos, uint32_t *pdbCount);
    RetStatus BuildPdbHashIndex();
    void AssignPdbBriefInfoByPdbInfoItem(PdbBriefInfo *pdbBriefInfo, const ControlPdbInfoPageItemData *item);
    RetStatus RectifyPdbInfoAfterL2Restore();
    ControlPdbInfoPageItemData *QueryControlPdbInfoPageItemData(PdbId pdbId, BlockNumber *matchedPage);
    RetStatus UpdatePdbFullRepairStatus(PdbId pdbId, bool isFullRepair);
    RetStatus IsPdbInFullRepair(const char *pdbName, bool *isFullRepair);

private:
    inline void SetPdbIdxAvailable(bool available)
    {
        m_pdbIdxAvailable.store(available, std::memory_order_release);
    }
    inline bool IsSingleNode()
    {
        return m_deployType == DeployType::CONTROL_FILE_SINGLE_NODE;
    }
    RetStatus ResumePdbIdxIfNeed();
    RetStatus PdbIndexGetPdbIdByName(const char *pdbName, PdbId *pdbId, PdbStatus status);
    RetStatus PdbIndexUpdateItem(const char *pdbName, PdbId pdbId, PdbStatus status);
    RetStatus PdbInfoPageAddItems();
    ControlPdbInfoPageItemData *GetPdbInfoItemByCtid(PdbInfoItemPointerData ctid);

    RetStatus DoUpdateStandbyPdbInfo(PdbId pdbId, const StandbyPdbInfo standbyPdbInfo);
    RetStatus DoClearStandbyPdbSlotByClusterId(PdbId pdbId, uint32 standbyClusterId);
    RetStatus DoUpdateStandbyPdbClusterName(PdbId pdbId, uint32 standbyClusterId, const char *clusterName);
    RetStatus DoUpdateStandbyPdbVfsName(PdbId pdbId, uint32 standbyClusterId, const char *vfsName);
    RetStatus DoUpdateStandbyPdbStoreSpaceName(PdbId pdbId, uint32 standbyClusterId, const char *storeSpaceName);
    RetStatus DoUpdatePdbWalSyncMode(PdbId pdbId, uint32 standbyClusterId, PdbSyncMode syncMode,
        uint32 walSizeThreshold, bool updateActualSyncMode);
    RetStatus DoUpdateStandbySyncModeAndWalSize(PdbId pdbId, PdbSyncMode syncMode, uint32 walSizeThreshold);

    RetStatus DoUpdatePdbRoleMode(PdbId pdbId, PdbRoleMode pdbRoleMode);
    RetStatus DoUpdatePdbRecycleCsnMin(PdbId pdbId, CommitSeqNo pdbRecycleCsnMin);
    RetStatus ProcessPdbRoleModeInvalidate(const PdbInfoInvalidateData *invaliddata);
    RetStatus ProcessPdbSyncModeInvalidate();
    RetStatus ProcessPdbRecycleCsnMinInvalidate(const PdbInfoInvalidateData *invaliddata);
    std::atomic<bool> m_pdbIdxAvailable;
    HTAB *m_pdbHashIndex;
    LWLock m_pdbIndexLock;
    DeployType m_deployType;
};
}  // namespace DSTORE
#endif  // DSTORE_CONTROL_PDBINFO_H