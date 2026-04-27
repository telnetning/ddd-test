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
 * dstore_control_pdbinfo.cpp
 *
 *
 *
 * IDENTIFICATION
 *        dstore/src/control/dstore_control_pdbinfo.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "control/dstore_control_pdbinfo.h"
#include "control/dstore_control_file_lock.h"
#include "framework/dstore_instance_interface.h"
#include "common/algorithm/dstore_hsearch.h"
#include "framework/dstore_instance.h"

namespace DSTORE {
RetStatus ControlPdbInfo::AllocPdbId(const char *pdbName, PdbId &pdbId, bool isSysPdb, const char* vfsName)
{
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_EXCLUSIVE))) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Acquire control file lock fail."));
        storage_set_error(CONTROL_ERROR_PDB_ALLOC_ID_FAILED);
        return DSTORE_FAIL;
    }
    ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_EXCLUSIVE};
    if (unlikely(LoadGroup() == DSTORE_FAIL)) {
        return DSTORE_FAIL;
    }

    /* make sure pdb name will not duplicate. */
    ControlPageIterator<ControlPdbInfoPageItemData> iterator{this, CONTROLFILE_PAGEMAP_PDBINFO_START};
    while (iterator.NextItem()) {
        ControlPdbInfoPageItemData *item = iterator.GetItem();
        if (item != nullptr && strcmp(item->pdbName, pdbName) == 0 &&
            item->pdbStatus != PdbStatus::PDB_STATUS_UNCREATED) {
            ErrLog(DSTORE_WARNING,
                MODULE_CONTROL, ErrMsg("pdbName %s already exists or created failed previously.", pdbName));
            storage_set_error(CONTROL_ERROR_PDB_ALLOC_ID_FAILED);
            return DSTORE_FAIL;
        }
    }

    ControlPdbInfoPageItemData *freePdbInfoItem = nullptr;
    ControlPageIterator<ControlPdbInfoPageItemData> iterator2{this, CONTROLFILE_PAGEMAP_PDBINFO_START};
    while (iterator2.NextItem()) {
        ControlPdbInfoPageItemData *item = iterator2.GetItem();
        /* `FIRST_NORMAL_PDB_ID` PDB slots are reserved for the system */
        if (STORAGE_VAR_NULL(item)) {
            return DSTORE_FAIL;
        }
        if (!isSysPdb && item->pdbId < FIRST_USER_PDB_ID) {
            continue;
        }
        if (item != nullptr && item->pdbStatus == PdbStatus::PDB_STATUS_UNCREATED) {
            freePdbInfoItem = item;
            break;
        }
    }

    if (freePdbInfoItem == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("cannot alloc new pdbId"));
        storage_set_error(CONTROL_ERROR_PDB_ALLOC_ID_FAILED);
        return DSTORE_FAIL;
    }

    errno_t rc = strncpy_s(freePdbInfoItem->pdbName, PDB_NAME_LEN, pdbName, strlen(pdbName));
    storage_securec_check(rc, "\0", "\0");

    freePdbInfoItem->pdbStatus = PdbStatus::PDB_STATUS_CREATING;
    if (vfsName != nullptr) {
        rc = strncpy_s(freePdbInfoItem->vfsName, MAX_CONFIG_NAME_LENGTH, vfsName, strlen(vfsName));
        storage_securec_check(rc, "\0", "\0");
    } else {
        StoragePdb::GenerateVfsName(freePdbInfoItem->pdbId, freePdbInfoItem->vfsName, MAX_CONFIG_NAME_LENGTH);
    }

    pdbId = freePdbInfoItem->pdbId;

    MarkPageDirty(iterator2.GetCurrentBlock());

    if (STORAGE_FUNC_FAIL(PostGroup())) {
        storage_set_error(CONTROL_ERROR_PDB_ALLOC_ID_FAILED);
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

ControlPdbInfoPageItemData *ControlPdbInfo::QueryControlPdbInfoPageItemData(PdbId pdbId, BlockNumber *matchedPage)
{
    ControlPdbInfoPageItemData* pdbInfoData = nullptr;
    bool found = false;
    if (unlikely(m_pdbHashIndex == nullptr)) {
        if (STORAGE_FUNC_FAIL(BuildPdbHashIndex())) {
            ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Failed to build pdb hash index."));
            return nullptr;
        }
    }
    PdbHashIndexEntry* entry = static_cast<PdbHashIndexEntry*>(hash_search(m_pdbHashIndex, static_cast<void *>(&pdbId),
        HASH_FIND, &found));
    if (found && entry != nullptr) {
        if (matchedPage != nullptr) {
            *matchedPage = entry->pdbItemPointer.blkno;
        }
        pdbInfoData = GetPdbInfoItemByCtid(entry->pdbItemPointer);
        return pdbInfoData;
    }
    return nullptr;
}

RetStatus ControlPdbInfo::GetPdbRoleMode(PdbId pdbId, PdbRoleMode &pdbRoleMode)
{
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_SHARE))) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Acquire control file lock fail."));
        return DSTORE_FAIL;
    }
    ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_SHARE};
    if (unlikely(LoadGroup() == DSTORE_FAIL)) {
        return DSTORE_FAIL;
    }
    ControlPdbInfoPageItemData *matchedPdbInfoItem = QueryControlPdbInfoPageItemData(pdbId, nullptr);
    if (STORAGE_VAR_NULL(matchedPdbInfoItem)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("PdbId %d is not found.", pdbId));
        return DSTORE_FAIL;
    }

    pdbRoleMode = matchedPdbInfoItem->pdbRoleMode;
    return DSTORE_SUCC;
}

RetStatus ControlPdbInfo::GetPdbRecycleCsnMin(PdbId pdbId, CommitSeqNo &pdbRecycleCsnMin)
{
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_SHARE))) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("[PdbRecycleCsnMin] Acquire control file lock fail."));
        return DSTORE_FAIL;
    }
    ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_SHARE};
    if (unlikely(LoadGroup() == DSTORE_FAIL)) {
        return DSTORE_FAIL;
    }
    ControlPdbInfoPageItemData *matchedPdbInfoItem = QueryControlPdbInfoPageItemData(pdbId, nullptr);
    if (STORAGE_VAR_NULL(matchedPdbInfoItem)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("[PdbRecycleCsnMin] PdbId %d is not found.", pdbId));
        return DSTORE_FAIL;
    }

    pdbRecycleCsnMin = matchedPdbInfoItem->pdbRecycleCsnMin;
    ErrLog(DSTORE_LOG, MODULE_CONTROL, ErrMsg("[PdbRecycleCsnMin] PdbId %d get recycleCsnMin %lu from control file.",
        pdbId, pdbRecycleCsnMin));
    return DSTORE_SUCC;
}

RetStatus ControlPdbInfo::UpdatePdbStatus(PdbId pdbId, PdbStatus pdbStatus)
{
    BlockNumber matchedPage;
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_EXCLUSIVE))) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Acquire control file lock fail."));
        return DSTORE_FAIL;
    }
    ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_EXCLUSIVE};
    if (unlikely(LoadGroup() == DSTORE_FAIL)) {
        return DSTORE_FAIL;
    }

    ControlPdbInfoPageItemData *matchedPdbInfoItem = QueryControlPdbInfoPageItemData(pdbId, &matchedPage);
    if (matchedPdbInfoItem == nullptr) {
        ErrLog(DSTORE_WARNING, MODULE_CONTROL, ErrMsg("PdbId %d is not found.", pdbId));
        return DSTORE_FAIL;
    }

    matchedPdbInfoItem->pdbStatus = pdbStatus;

    MarkPageDirty(matchedPage);

    if (STORAGE_FUNC_FAIL(PostGroup())) {
        return DSTORE_FAIL;
    }

    if (STORAGE_FUNC_FAIL(PdbIndexUpdateItem(matchedPdbInfoItem->pdbName, pdbId, pdbStatus))) {
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus ControlPdbInfo::UpdatePdbReplicaStatus(PdbId pdbId, PdbReplicaRebuildProgress newPdbReplicaStatus)
{
    BlockNumber matchedPage;
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_EXCLUSIVE))) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Acquire control file lock fail."));
        return DSTORE_FAIL;
    }
    ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_EXCLUSIVE};
    if (unlikely(LoadGroup() == DSTORE_FAIL)) {
        return DSTORE_FAIL;
    }
    ControlPdbInfoPageItemData *pdbInfoItem = QueryControlPdbInfoPageItemData(pdbId, &matchedPage);
    if (pdbInfoItem == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("PdbId %d is not found.", pdbId));
        return DSTORE_FAIL;
    }

    if (pdbInfoItem->pdbReplicaStatus == newPdbReplicaStatus) {
        ErrLog(DSTORE_DEBUG1, MODULE_CONTROL,
               ErrMsg("No need to update, pdbId %hhu, pdbReplicaStatus %hhu.", pdbId,
                      static_cast<uint8>(newPdbReplicaStatus)));
        return DSTORE_SUCC;
    }

    PdbReplicaRebuildProgress oldOne = pdbInfoItem->pdbReplicaStatus;
    pdbInfoItem->pdbReplicaStatus = newPdbReplicaStatus;
    MarkPageDirty(matchedPage);

    if (STORAGE_FUNC_FAIL(PostGroup())) {
        return DSTORE_FAIL;
    }

    ErrLog(DSTORE_LOG, MODULE_CONTROL, ErrMsg("Update, pdbId %hhu, pdbReplicaStatus %s to %s.", pdbId,
        g_pdbRebuildProgressStr[static_cast<uint8>(oldOne)],
        g_pdbRebuildProgressStr[static_cast<uint8>(newPdbReplicaStatus)]));
    return DSTORE_SUCC;
}

RetStatus ControlPdbInfo::UpdatePdbSwitchStatus(PdbId pdbId, PdbSwitchStatus newPdbSwitchStatus)
{
    BlockNumber matchedPage;
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_EXCLUSIVE))) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Acquire control file lock fail."));
        return DSTORE_FAIL;
    }
    ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_EXCLUSIVE};
    if (unlikely(LoadGroup() == DSTORE_FAIL)) {
        return DSTORE_FAIL;
    }
    ControlPdbInfoPageItemData *matchedPdbInfoItem = QueryControlPdbInfoPageItemData(pdbId, &matchedPage);
    if (matchedPdbInfoItem == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("PdbId %d is not found.", pdbId));
        return DSTORE_FAIL;
    }

    if (matchedPdbInfoItem->pdbSwitchStatus == newPdbSwitchStatus) {
        ErrLog(DSTORE_DEBUG1, MODULE_CONTROL,
               ErrMsg("No need to update, pdbId %hhu, pdbSwitchStatus %hhu.", pdbId,
                      static_cast<uint8>(newPdbSwitchStatus)));
        return DSTORE_SUCC;
    }

    matchedPdbInfoItem->pdbSwitchStatus = newPdbSwitchStatus;
    MarkPageDirty(matchedPage);

    if (STORAGE_FUNC_FAIL(PostGroup())) {
        return DSTORE_FAIL;
    }

    return DSTORE_SUCC;
}

RetStatus ControlPdbInfo::UpdatePdbFullRepairStatus(PdbId pdbId, bool isFullRepair)
{
    BlockNumber matchedPage;
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_EXCLUSIVE))) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Acquire control file lock fail."));
        return DSTORE_FAIL;
    }
    ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_EXCLUSIVE};
    if (unlikely(LoadGroup() == DSTORE_FAIL)) {
        return DSTORE_FAIL;
    }

    ControlPdbInfoPageItemData *matchedPdbInfoItem = QueryControlPdbInfoPageItemData(pdbId, &matchedPage);
    if (matchedPdbInfoItem == nullptr) {
        ErrLog(DSTORE_WARNING, MODULE_CONTROL, ErrMsg("PdbId %d is not found.", pdbId));
        return DSTORE_FAIL;
    }

    matchedPdbInfoItem->isFullRepair = isFullRepair;
    MarkPageDirty(matchedPage);

    if (STORAGE_FUNC_FAIL(PostGroup())) {
        return DSTORE_FAIL;
    }

    return DSTORE_SUCC;
}

RetStatus ControlPdbInfo::IsPdbInFullRepair(const char *pdbName, bool *isFullRepair)
{
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_SHARE))) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Acquire control file lock fail."));
        return DSTORE_FAIL;
    }
    ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_SHARE};
    if (unlikely(LoadGroup() == DSTORE_FAIL)) {
        return DSTORE_FAIL;
    }
    ControlPageIterator<ControlPdbInfoPageItemData> iterator{this, CONTROLFILE_PAGEMAP_PDBINFO_START};
    while (iterator.NextItem()) {
        ControlPdbInfoPageItemData *pdbInfoPageItemData = iterator.GetItem();
        if (pdbInfoPageItemData != nullptr && strcmp(pdbName, pdbInfoPageItemData->pdbName) == 0) {
            *isFullRepair = pdbInfoPageItemData->isFullRepair;
            return DSTORE_SUCC;
        }
    }
    ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Don't find pdb %s in controlFile.", pdbName));
    return DSTORE_FAIL;
}

RetStatus ControlPdbInfo::GetPdbSwitchStatus(PdbId pdbId, PdbSwitchStatus &pdbSwitchStatus)
{
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_SHARE))) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Acquire control file lock fail."));
        return DSTORE_FAIL;
    }
    ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_SHARE};
    if (unlikely(LoadGroup() == DSTORE_FAIL)) {
        return DSTORE_FAIL;
    }
    ControlPdbInfoPageItemData *matchedPdbInfoItem = QueryControlPdbInfoPageItemData(pdbId, nullptr);
    if (matchedPdbInfoItem == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("PdbId %d is not found.", pdbId));
        return DSTORE_FAIL;
    }

    pdbSwitchStatus = matchedPdbInfoItem->pdbSwitchStatus;
    return DSTORE_SUCC;
}

RetStatus ControlPdbInfo::GetPdbReplicaStatus(PdbId pdbId, PdbReplicaRebuildProgress &rebuildProgress)
{
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_SHARE))) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Acquire control file lock fail."));
        return DSTORE_FAIL;
    }
    ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_SHARE};
    if (unlikely(LoadGroup() == DSTORE_FAIL)) {
        return DSTORE_FAIL;
    }
    ControlPdbInfoPageItemData *matchedPdbInfoItem = QueryControlPdbInfoPageItemData(pdbId, nullptr);
    if (matchedPdbInfoItem == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("PdbId %d is not found.", pdbId));
        return DSTORE_FAIL;
    }

    rebuildProgress = matchedPdbInfoItem->pdbReplicaStatus;
    return DSTORE_SUCC;
}

RetStatus ControlPdbInfo::GetStandbyPdbInfo(PdbId pdbId, uint32 standbyClusterId, StandbyPdbInfo &standbyPdbInfo)
{
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_SHARE))) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Acquire control file lock fail."));
        return DSTORE_FAIL;
    }
    ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_SHARE};
    if (unlikely(LoadGroup() == DSTORE_FAIL)) {
        return DSTORE_FAIL;
    }

    ControlPdbInfoPageItemData *matchedPdbInfoItem = QueryControlPdbInfoPageItemData(pdbId, nullptr);
    if (STORAGE_VAR_NULL(matchedPdbInfoItem)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("PdbId %d is not found.", pdbId));
        return DSTORE_FAIL;
    }

    uint32 slotIdx = MAX_DR_CLUSTER_COUNT;
    for (uint32 i = 0; i < MAX_DR_CLUSTER_COUNT; i++) {
        if (matchedPdbInfoItem->standbyPdbInfo[i].clusterId == standbyClusterId) {
            slotIdx = i;
            break;
        }
    }
    if (slotIdx == MAX_DR_CLUSTER_COUNT) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Cluster standby Id %d is not found.", standbyClusterId));
        return DSTORE_FAIL;
    }
    standbyPdbInfo = matchedPdbInfoItem->standbyPdbInfo[slotIdx];
    return DSTORE_SUCC;
}

RetStatus ControlPdbInfo::UpdateStandbyPdbClusterName(PdbId pdbId, uint32 standbyClusterId, const char *clusterName)
{
    if (unlikely(IsSingleNode())) {
        return DSTORE_SUCC;
    }
    /* 1. Lock exclusive lock for control file */
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_EXCLUSIVE))) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Acquire control file lock fail."));
        return DSTORE_FAIL;
    }
    {
        ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_EXCLUSIVE};
        if (unlikely(LoadGroup() == DSTORE_FAIL)) {
            return DSTORE_FAIL;
        }

        /* 2. Modify pdb sync mode and persist it in control file, release lock */
        if (STORAGE_FUNC_FAIL(DoUpdateStandbyPdbClusterName(pdbId, standbyClusterId, clusterName))) {
            ErrLog(DSTORE_ERROR, MODULE_CONTROL,
                   ErrMsg("Failed to update PdbSyncMode in control file locally. PdbId:%d; standbyClusterId:%u", pdbId,
                          standbyClusterId));
            return DSTORE_FAIL;
        }
    }
    return DSTORE_SUCC;
}
RetStatus ControlPdbInfo::UpdateStandbyPdbVfsName(PdbId pdbId, uint32 standbyClusterId, const char *vfsName)
{
    if (unlikely(IsSingleNode())) {
        return DSTORE_SUCC;
    }
    /* 1. Lock exclusive lock for control file */
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_EXCLUSIVE))) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Acquire control file lock fail."));
        return DSTORE_FAIL;
    }
    {
        ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_EXCLUSIVE};
        if (unlikely(LoadGroup() == DSTORE_FAIL)) {
            return DSTORE_FAIL;
        }

        /* 2. Modify pdb sync mode and persist it in control file, release lock */
        if (STORAGE_FUNC_FAIL(DoUpdateStandbyPdbVfsName(pdbId, standbyClusterId, vfsName))) {
            ErrLog(DSTORE_ERROR, MODULE_CONTROL,
                   ErrMsg("Failed to update PdbSyncMode in control file locally. PdbId:%d; standbyClusterId:%u", pdbId,
                          standbyClusterId));
            return DSTORE_FAIL;
        }
    }
    return DSTORE_SUCC;
}

RetStatus ControlPdbInfo::UpdateStandbyPdbStoreSpaceName(PdbId pdbId, uint32 standbyClusterId,
    const char *storeSpaceName)
{
    if (unlikely(IsSingleNode())) {
        return DSTORE_SUCC;
    }
    /* 1. Lock exclusive lock for control file */
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_EXCLUSIVE))) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Acquire control file lock fail."));
        return DSTORE_FAIL;
    }
    {
        ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_EXCLUSIVE};
        if (unlikely(LoadGroup() == DSTORE_FAIL)) {
            return DSTORE_FAIL;
        }

        /* 2. Modify pdb sync mode and persist it in control file, release lock */
        if (STORAGE_FUNC_FAIL(DoUpdateStandbyPdbStoreSpaceName(pdbId, standbyClusterId, storeSpaceName))) {
            ErrLog(DSTORE_ERROR, MODULE_CONTROL,
                   ErrMsg("Failed to update PdbSyncMode in control file locally. PdbId:%d; standbyClusterId:%u", pdbId,
                          standbyClusterId));
            return DSTORE_FAIL;
        }
    }
    return DSTORE_SUCC;
}
RetStatus ControlPdbInfo::UpdatePdbWalSyncMode(PdbId pdbId, uint32 standbyClusterId, PdbSyncMode syncMode,
    uint32 walSizeThreshold, bool updateActualSyncMode)
{
    if (unlikely(IsSingleNode())) {
        return DSTORE_SUCC;
    }
    /* 1. Lock exclusive lock for control file */
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_EXCLUSIVE))) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Acquire control file lock fail."));
        return DSTORE_FAIL;
    }
    {
        ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_EXCLUSIVE};
        if (unlikely(LoadGroup() == DSTORE_FAIL)) {
            return DSTORE_FAIL;
        }

        /* 2. Modify pdb sync mode and persist it in control file, release lock */
        if (STORAGE_FUNC_FAIL(
            DoUpdatePdbWalSyncMode(pdbId, standbyClusterId, syncMode, walSizeThreshold, updateActualSyncMode))) {
            ErrLog(DSTORE_ERROR, MODULE_CONTROL,
                ErrMsg("Failed to update PdbSyncMode in control file locally. PdbId:%d; standbyClusterId:%u", pdbId,
                standbyClusterId));
            return DSTORE_FAIL;
        }
    }
    return DSTORE_SUCC;
}

RetStatus ControlPdbInfo::UpdateStandbySyncModeAndWalSize(PdbId pdbId, PdbSyncMode syncMode, uint32 walSizeThreshold)
{
    if (unlikely(IsSingleNode())) {
        return DSTORE_SUCC;
    }
    /* 1. Lock exclusive lock for control file */
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_EXCLUSIVE))) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Acquire control file lock fail."));
        return DSTORE_FAIL;
    }
    {
        ControlFileLockAutoUnlock autoUnlock{ m_lock, CFLockMode::CF_EXCLUSIVE };
        if (unlikely(LoadGroup() == DSTORE_FAIL)) {
            return DSTORE_FAIL;
        }

        /* 2. Modify standby pdb sync mode and walSizeThreshold in control file, release lock */
        if (STORAGE_FUNC_FAIL(DoUpdateStandbySyncModeAndWalSize(pdbId, syncMode, walSizeThreshold))) {
            ErrLog(DSTORE_ERROR, MODULE_CONTROL,
                ErrMsg("Failed to update standbyPdbSyncMode and walSize in control file locally. PdbId:%d.", pdbId));
            return DSTORE_FAIL;
        }
    }
    return DSTORE_SUCC;
}

RetStatus ControlPdbInfo::ClearStandbyPdbSlotByClusterId(PdbId pdbId, uint32 standbyClusterId)
{
    if (unlikely(IsSingleNode())) {
        return DSTORE_SUCC;
    }
    /* 1. Lock exclusive lock for control file */
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_EXCLUSIVE))) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Acquire control file lock fail."));
        return DSTORE_FAIL;
    }
    {
        ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_EXCLUSIVE};
        if (unlikely(LoadGroup() == DSTORE_FAIL)) {
            return DSTORE_FAIL;
        }

        /* 2. Clear standby slot and persist it in control file, release lock */
        if (STORAGE_FUNC_FAIL(DoClearStandbyPdbSlotByClusterId(pdbId, standbyClusterId))) {
            ErrLog(DSTORE_ERROR, MODULE_CONTROL,
                   ErrMsg("Failed to Clear standby slot in control file locally. PdbId:%d; standbyClusterId:%u", pdbId,
                          standbyClusterId));
            return DSTORE_FAIL;
        }
    }
    return DSTORE_SUCC;
}

RetStatus ControlPdbInfo::DoUpdateStandbyPdbInfo(PdbId pdbId, const StandbyPdbInfo standbyPdbInfo)
{
    BlockNumber matchedPage;

    ControlPdbInfoPageItemData *matchedPdbInfoItem = QueryControlPdbInfoPageItemData(pdbId, &matchedPage);
    if (STORAGE_VAR_NULL(matchedPdbInfoItem)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("PdbId %d is not found.", pdbId));
        return DSTORE_FAIL;
    }

    uint32 slotIdx = MAX_DR_CLUSTER_COUNT;

    /* If the clusterId is not already registered, find a new slot for the cluster */
    for (uint32 i = 0; i < MAX_DR_CLUSTER_COUNT; i++) {
        if (matchedPdbInfoItem->standbyPdbInfo[i].clusterId == standbyPdbInfo.clusterId) {
            slotIdx = i;
            break;
        }
    }

    if (slotIdx == MAX_DR_CLUSTER_COUNT) {
        for (uint32 i = 0; i < MAX_DR_CLUSTER_COUNT; i++) {
            if (matchedPdbInfoItem->standbyPdbInfo[i].clusterId == INVALID_CLUSTER_ID) {
                slotIdx = i;
                break;
            }
        }
    }

    /* Failed to find an existing or new slot for a given pdb, error out */
    if (slotIdx == MAX_DR_CLUSTER_COUNT) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Fail to register or update cluster."));
        return DSTORE_FAIL;
    }
    matchedPdbInfoItem->standbyPdbInfo[slotIdx].clusterId = standbyPdbInfo.clusterId;
    matchedPdbInfoItem->standbyPdbInfo[slotIdx].pdbSyncMode = standbyPdbInfo.pdbSyncMode;
    matchedPdbInfoItem->standbyPdbInfo[slotIdx].actualSyncMode = standbyPdbInfo.actualSyncMode;
    matchedPdbInfoItem->standbyPdbInfo[slotIdx].walSizeThreshold = standbyPdbInfo.walSizeThreshold;
    /* standbyPdbInfo[MAX_DR_CLUSTER_COUNT] used to record sync mode for switchover */
    matchedPdbInfoItem->standbyPdbInfo[MAX_DR_CLUSTER_COUNT].pdbSyncMode = standbyPdbInfo.pdbSyncMode;
    matchedPdbInfoItem->standbyPdbInfo[MAX_DR_CLUSTER_COUNT].walSizeThreshold = standbyPdbInfo.walSizeThreshold;
    error_t rc = strcpy_s(matchedPdbInfoItem->standbyPdbInfo[slotIdx].clusterName, CLUSTER_NAME_MAX_LEN,
        standbyPdbInfo.clusterName);
    storage_securec_check(rc, "\0", "\0");
    rc = strcpy_s(matchedPdbInfoItem->standbyPdbInfo[slotIdx].vfsName, VFS_NAME_MAX_LEN, standbyPdbInfo.vfsName);
    storage_securec_check(rc, "\0", "\0");
    rc = strcpy_s(matchedPdbInfoItem->standbyPdbInfo[slotIdx].storeSpaceName, STORESPACE_NAME_MAX_LEN,
        standbyPdbInfo.storeSpaceName);
    storage_securec_check(rc, "\0", "\0");

    MarkPageDirty(matchedPage);
    if (STORAGE_FUNC_FAIL(PostGroup())) {
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus ControlPdbInfo::DoUpdateStandbyPdbClusterName(PdbId pdbId, uint32 standbyClusterId, const char *clusterName)
{
    BlockNumber matchedPage;

    ControlPdbInfoPageItemData *matchedPdbInfoItem = QueryControlPdbInfoPageItemData(pdbId, &matchedPage);
    if (STORAGE_VAR_NULL(matchedPdbInfoItem)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("PdbId %d is not found.", pdbId));
        return DSTORE_FAIL;
    }

    /* Find if we have registered this standby before */
    uint32 slotIdx = MAX_DR_CLUSTER_COUNT;
    for (uint32 i = 0; i < MAX_DR_CLUSTER_COUNT; i++) {
        if (matchedPdbInfoItem->standbyPdbInfo[i].clusterId == standbyClusterId) {
            slotIdx = i;
            break;
        }
    }

    /* Failed to find an existing or new slot for a given pdb, error out */
    if (slotIdx == MAX_DR_CLUSTER_COUNT) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Fail to register or update cluster."));
        return DSTORE_FAIL;
    }
    error_t rc = strcpy_s(matchedPdbInfoItem->standbyPdbInfo[slotIdx].clusterName, CLUSTER_NAME_MAX_LEN, clusterName);
    storage_securec_check(rc, "\0", "\0");
    MarkPageDirty(matchedPage);

    if (STORAGE_FUNC_FAIL(PostGroup())) {
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus ControlPdbInfo::DoUpdateStandbyPdbVfsName(PdbId pdbId, uint32 standbyClusterId, const char *vfsName)
{
    BlockNumber matchedPage;

    ControlPdbInfoPageItemData *matchedPdbInfoItem = QueryControlPdbInfoPageItemData(pdbId, &matchedPage);
    if (STORAGE_VAR_NULL(matchedPdbInfoItem)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("PdbId %d is not found.", pdbId));
        return DSTORE_FAIL;
    }

    /* Find if we have registered this standby before */
    uint32 slotIdx = MAX_DR_CLUSTER_COUNT;
    for (uint32 i = 0; i < MAX_DR_CLUSTER_COUNT; i++) {
        if (matchedPdbInfoItem->standbyPdbInfo[i].clusterId == standbyClusterId) {
            slotIdx = i;
            break;
        }
    }

    /* Failed to find an existing or new slot for a given pdb, error out */
    if (slotIdx == MAX_DR_CLUSTER_COUNT) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL,    ErrMsg("Fail to register or update cluster."));
        return DSTORE_FAIL;
    }
    error_t rc = strcpy_s(matchedPdbInfoItem->standbyPdbInfo[slotIdx].vfsName, VFS_NAME_MAX_LEN, vfsName);
    storage_securec_check(rc, "\0", "\0");
    MarkPageDirty(matchedPage);

    if (STORAGE_FUNC_FAIL(PostGroup())) {
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}
RetStatus ControlPdbInfo::DoUpdateStandbyPdbStoreSpaceName(PdbId pdbId, uint32 standbyClusterId,
    const char *storeSpaceName)
{
    BlockNumber matchedPage;

    ControlPdbInfoPageItemData *matchedPdbInfoItem = QueryControlPdbInfoPageItemData(pdbId, &matchedPage);
    if (STORAGE_VAR_NULL(matchedPdbInfoItem)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("PdbId %d is not found.", pdbId));
        return DSTORE_FAIL;
    }

    /* Find if we have registered this standby before */
    uint32 slotIdx = MAX_DR_CLUSTER_COUNT;
    for (uint32 i = 0; i < MAX_DR_CLUSTER_COUNT; i++) {
        if (matchedPdbInfoItem->standbyPdbInfo[i].clusterId == standbyClusterId) {
            slotIdx = i;
            break;
        }
    }

    /* Failed to find an existing or new slot for a given pdb, error out */
    if (slotIdx == MAX_DR_CLUSTER_COUNT) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Fail to register or update cluster."));
        return DSTORE_FAIL;
    }
    error_t rc =
        strcpy_s(matchedPdbInfoItem->standbyPdbInfo[slotIdx].storeSpaceName, STORESPACE_NAME_MAX_LEN, storeSpaceName);
    storage_securec_check(rc, "\0", "\0");
    MarkPageDirty(matchedPage);

    if (STORAGE_FUNC_FAIL(PostGroup())) {
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}
RetStatus ControlPdbInfo::DoUpdatePdbWalSyncMode(PdbId pdbId, uint32 standbyClusterId, PdbSyncMode syncMode,
    uint32 walSizeThreshold, bool updateActualSyncMode)
{
    BlockNumber matchedPage;

    ControlPdbInfoPageItemData *matchedPdbInfoItem = QueryControlPdbInfoPageItemData(pdbId, &matchedPage);
    if (STORAGE_VAR_NULL(matchedPdbInfoItem)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("PdbId %d is not found.", pdbId));
        return DSTORE_FAIL;
    }

    /* Find if we have registered this standby before */
    uint32 slotIdx = MAX_DR_CLUSTER_COUNT;
    for (uint32 i = 0; i < MAX_DR_CLUSTER_COUNT; i++) {
        if (matchedPdbInfoItem->standbyPdbInfo[i].clusterId == standbyClusterId) {
            slotIdx = i;
            break;
        }
    }

    /* Failed to find an existing or new slot for a given pdb, error out */
    if (slotIdx == MAX_DR_CLUSTER_COUNT) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL,
               ErrMsg("Fail to register or update cluster."));
        return DSTORE_FAIL;
    }
    if (!updateActualSyncMode) {
        matchedPdbInfoItem->standbyPdbInfo[slotIdx].pdbSyncMode = syncMode;
        matchedPdbInfoItem->standbyPdbInfo[slotIdx].walSizeThreshold = walSizeThreshold;
    } else {
        matchedPdbInfoItem->standbyPdbInfo[slotIdx].actualSyncMode = syncMode;
    }
    MarkPageDirty(matchedPage);

    if (STORAGE_FUNC_FAIL(PostGroup())) {
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus ControlPdbInfo::DoUpdateStandbySyncModeAndWalSize(PdbId pdbId, PdbSyncMode syncMode, uint32 walSizeThreshold)
{
    BlockNumber matchedPage;

    ControlPdbInfoPageItemData *matchedPdbInfoItem = QueryControlPdbInfoPageItemData(pdbId, &matchedPage);
    if (STORAGE_VAR_NULL(matchedPdbInfoItem)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("PdbId %d is not found.", pdbId));
        return DSTORE_FAIL;
    }
    /* used to record sync mode for switchover */
    matchedPdbInfoItem->standbyPdbInfo[MAX_DR_CLUSTER_COUNT].pdbSyncMode = syncMode;
    matchedPdbInfoItem->standbyPdbInfo[MAX_DR_CLUSTER_COUNT].walSizeThreshold = walSizeThreshold;

    MarkPageDirty(matchedPage);

    if (STORAGE_FUNC_FAIL(PostGroup())) {
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus ControlPdbInfo::DoClearStandbyPdbSlotByClusterId(PdbId pdbId, uint32 standbyClusterId)
{
    BlockNumber matchedPage;

    ControlPdbInfoPageItemData *matchedPdbInfoItem = QueryControlPdbInfoPageItemData(pdbId, &matchedPage);
    if (STORAGE_VAR_NULL(matchedPdbInfoItem)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("PdbId %d is not found.", pdbId));
        return DSTORE_FAIL;
    }

    /* Find if we have registered this standby before */
    uint32 slotIdx = MAX_DR_CLUSTER_COUNT;
    for (uint32 i = 0; i < MAX_DR_CLUSTER_COUNT; i++) {
        if (matchedPdbInfoItem->standbyPdbInfo[i].clusterId == standbyClusterId) {
            slotIdx = i;
            break;
        }
    }

    /* Failed to find an existing or new slot for a given pdb, error out */
    if (slotIdx == MAX_DR_CLUSTER_COUNT) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("clusterId is not exist, no need to clear cluster slot"));
        return DSTORE_SUCC;
    }
    matchedPdbInfoItem->standbyPdbInfo[slotIdx].clusterId = INVALID_CLUSTER_ID;
    matchedPdbInfoItem->standbyPdbInfo[slotIdx].pdbSyncMode = PdbSyncMode::INVALID_SYNC_MODE;
    matchedPdbInfoItem->standbyPdbInfo[slotIdx].actualSyncMode = PdbSyncMode::MAX_PERFORMANCE_MODE;
    matchedPdbInfoItem->standbyPdbInfo[slotIdx].walSizeThreshold = INVALID_WAL_SIZE_THRESHOLD;
    error_t rc = strcpy_s(matchedPdbInfoItem->standbyPdbInfo[slotIdx].clusterName, CLUSTER_NAME_MAX_LEN, "");
    storage_securec_check(rc, "\0", "\0");
    rc = strcpy_s(matchedPdbInfoItem->standbyPdbInfo[slotIdx].vfsName, VFS_NAME_MAX_LEN, "");
    storage_securec_check(rc, "\0", "\0");
    rc = strcpy_s(matchedPdbInfoItem->standbyPdbInfo[slotIdx].storeSpaceName, STORESPACE_NAME_MAX_LEN, "");
    storage_securec_check(rc, "\0", "\0");
    rc = strcpy_s(matchedPdbInfoItem->standbyPdbInfo[slotIdx].pdbName, PDB_NAME_LEN, "");
    storage_securec_check(rc, "\0", "\0");
    MarkPageDirty(matchedPage);

    if (STORAGE_FUNC_FAIL(PostGroup())) {
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus ControlPdbInfo::RegisterNewStandby(PdbId pdbId, StandbyPdbInfo standbyPdbInfo)
{
    /* 1. Lock exclusive lock for control file */
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_EXCLUSIVE))) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Acquire control file lock fail."));
        return DSTORE_FAIL;
    }
    {
        ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_EXCLUSIVE};
        if (unlikely(LoadGroup() == DSTORE_FAIL)) {
            return DSTORE_FAIL;
        }

        /* 2. Persist pdb standby replica info in control file, release lock */
        if (STORAGE_FUNC_FAIL(DoUpdateStandbyPdbInfo(pdbId, standbyPdbInfo))) {
            ErrLog(DSTORE_ERROR, MODULE_CONTROL,
                   ErrMsg("Failed to update PdbSyncMode in control file locally. PdbId:%d; standbyClusterId:%u",
                   pdbId, standbyPdbInfo.clusterId));
            return DSTORE_FAIL;
        }
    }
    return DSTORE_SUCC;
}

void ControlPdbInfo::InvalidatePdbIdxIfNeed(BlockNumber blkno)
{
    ControlDataPage *page = nullptr;
    ControlPageType pageType = CONTROL_PAGE_TYPE_INVALID;

    page = GetPage(blkno);
    if (page == nullptr) {
        return;
    }

    pageType = page->GetPageType();
    if (pageType != CONTROL_PDBINFO_DATAPAGE_TYPE) {
        return;
    }

    ControlPageIterator<ControlPdbInfoPageItemData> iterator{this, blkno};
    while (iterator.NextItem()) {
        ControlPdbInfoPageItemData *item = iterator.GetItem();
        if (STORAGE_VAR_NULL(item)) {
            StorageReleasePanic(true, MODULE_PDB, ErrMsg("item should not be null."));
            return;
        }
        if (item->pdbStatus != PdbStatus::PDB_STATUS_UNCREATED) {
            (void)g_storageInstance->InvalidatePdbInfoCache(item->pdbId);
        }
    }

    this->SetPdbIdxAvailable(false);
    return;
}

RetStatus ControlPdbInfo::SetOpenedFlag(PdbId pdbId)
{
    return UpdatePdbStatus(pdbId, PdbStatus::PDB_STATUS_OPENED_READ_WRITE);
}

RetStatus ControlPdbInfo::SetDeleteFlag(PdbId pdbId)
{
    return UpdatePdbStatus(pdbId, PdbStatus::PDB_STATUS_DROPPING);
}

RetStatus ControlPdbInfo::FreePdbId(PdbId pdbId)
{
    return UpdatePdbStatus(pdbId, PdbStatus::PDB_STATUS_UNCREATED);
}

RetStatus ControlPdbInfo::UpdateTotalPdbCount(UNUSE_PARAM bool isIncrease)
{
    /* wait delete */
    return DSTORE_SUCC;
}

RetStatus ControlPdbInfo::GetPdbInfoById(PdbId pdbId, PdbInfo *pdbInfo)
{
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_SHARE))) {
        return DSTORE_FAIL;
    }

    ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_SHARE};
    if (unlikely(LoadGroup() == DSTORE_FAIL)) {
        return DSTORE_FAIL;
    }

    ControlPdbInfoPageItemData *matchedPdbInfoItem = QueryControlPdbInfoPageItemData(pdbId, nullptr);
    if (matchedPdbInfoItem == nullptr) {
        ErrLog(DSTORE_WARNING, MODULE_CONTROL, ErrMsg("PdbId %hhu is not found.", pdbId));
        return DSTORE_FAIL;
    }

    /* itemdata is not null, but status needs extra validation. */
    if (unlikely(matchedPdbInfoItem->pdbStatus != PdbStatus::PDB_STATUS_OPENED_READ_WRITE &&
                 matchedPdbInfoItem->pdbStatus != PdbStatus::PDB_STATUS_CLOSED)) {
        ErrLog(DSTORE_WARNING, MODULE_CONTROL,
               ErrMsg("pdbStatus of pdbId %hhu is not PDB_STATUS_OPENED_READ_WRITE or PDB_STATUS_CLOSED.", pdbId));
        return DSTORE_FAIL;
    }
    pdbInfo->templateId = 0;
    pdbInfo->sysDatabaseTuple = NULL;
    AssignPdbBriefInfoByPdbInfoItem(&pdbInfo->briefInfo, matchedPdbInfoItem);

    return DSTORE_SUCC;
}

ControlPdbInfoPageItemData *ControlPdbInfo::GetAvailablePdbInfo(const char *pdbName)
{
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_SHARE))) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Acquire control file lock fail."));
        return nullptr;
    }
    ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_SHARE};
    if (unlikely(LoadGroup() == DSTORE_FAIL)) {
        return nullptr;
    }

    ControlPageIterator<ControlPdbInfoPageItemData> iterator{this, CONTROLFILE_PAGEMAP_PDBINFO_START};
    while (iterator.NextItem()) {
        ControlPdbInfoPageItemData *pdbInfoPageItemData = iterator.GetItem();
        if (STORAGE_VAR_NULL(pdbInfoPageItemData)) {
            StorageReleasePanic(true, MODULE_PDB, ErrMsg("item should not be null."));
            return nullptr;
        }
        if (strcmp(pdbInfoPageItemData->pdbName, pdbName) == 0 &&
            (pdbInfoPageItemData->pdbStatus == PdbStatus::PDB_STATUS_OPENED_READ_WRITE ||
             pdbInfoPageItemData->pdbStatus == PdbStatus::PDB_STATUS_CLOSED)) {
            return pdbInfoPageItemData;
        }
    }
    return nullptr;
}

ControlPdbInfoPageItemData *ControlPdbInfo::GetAvailablePdbInfo(PdbId pdbId)
{
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_SHARE))) {
        return nullptr;
    }
    ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_SHARE};
    if (unlikely(LoadGroup() == DSTORE_FAIL)) {
        return nullptr;
    }

    ControlPageIterator<ControlPdbInfoPageItemData> iterator{this, CONTROLFILE_PAGEMAP_PDBINFO_START};
    while (iterator.NextItem()) {
        ControlPdbInfoPageItemData *pdbInfoPageItemData = iterator.GetItem();
        if (STORAGE_VAR_NULL(pdbInfoPageItemData)) {
            ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("item should not be null."));
            return nullptr;
        }
        if (pdbInfoPageItemData->pdbId == pdbId &&
            (pdbInfoPageItemData->pdbStatus == PdbStatus::PDB_STATUS_OPENED_READ_WRITE ||
             pdbInfoPageItemData->pdbStatus == PdbStatus::PDB_STATUS_CLOSED)) {
            return pdbInfoPageItemData;
        }
    }
    return nullptr;
}

ControlPdbInfoPageItemData *ControlPdbInfo::GetPdbInfoById(PdbId pdbId)
{
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_SHARE))) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Acquire control file lock fail."));
        return nullptr;
    }
    ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_SHARE};
    if (unlikely(LoadGroup() == DSTORE_FAIL)) {
        return nullptr;
    }

    ControlPdbInfoPageItemData *matchedPdbInfoItem = QueryControlPdbInfoPageItemData(pdbId, nullptr);
    if (matchedPdbInfoItem == nullptr) {
        ErrLog(DSTORE_WARNING, MODULE_CONTROL, ErrMsg("PdbId %d is not found.", pdbId));
        return nullptr;
    }

    return matchedPdbInfoItem;
}

ControlPdbInfoPageItemData *ControlPdbInfo::GetPdbInfo(const char *pdbName, PdbStatus pdbStatus)
{
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_SHARE))) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Acquire control file lock fail."));
        return nullptr;
    }
    ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_SHARE};
    if (unlikely(LoadGroup() == DSTORE_FAIL)) {
        return nullptr;
    }

    ControlPageIterator<ControlPdbInfoPageItemData> iterator{this, CONTROLFILE_PAGEMAP_PDBINFO_START};
    while (iterator.NextItem()) {
        ControlPdbInfoPageItemData *pdbInfoPageItemData = iterator.GetItem();
        if (STORAGE_VAR_NULL(pdbInfoPageItemData)) {
            ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("item should not be null."));
            return nullptr;
        }
        if (strcmp(pdbInfoPageItemData->pdbName, pdbName) == 0 && pdbInfoPageItemData->pdbStatus == pdbStatus) {
            return pdbInfoPageItemData;
        }
    }
    return nullptr;
}

ControlPdbInfoPageItemData *ControlPdbInfo::GetPdbInfo(PdbId pdbId, PdbStatus pdbStatus)
{
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_SHARE))) {
        return nullptr;
    }
    ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_SHARE};
    if (unlikely(LoadGroup() == DSTORE_FAIL)) {
        return nullptr;
    }

    ControlPageIterator<ControlPdbInfoPageItemData> iterator{this, CONTROLFILE_PAGEMAP_PDBINFO_START};
    while (iterator.NextItem()) {
        ControlPdbInfoPageItemData *pdbInfoPageItemData = iterator.GetItem();
        if (pdbInfoPageItemData && pdbInfoPageItemData->pdbId == pdbId && pdbInfoPageItemData->pdbStatus == pdbStatus) {
            return pdbInfoPageItemData;
        }
    }
    return nullptr;
}

ControlPdbInfoPageItemData *ControlPdbInfo::GetPdbInfo(const char *pdbName)
{
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_SHARE))) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Acquire control file lock fail."));
        return nullptr;
    }
    ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_SHARE};

    if (unlikely(LoadGroup() == DSTORE_FAIL)) {
        return nullptr;
    }

    ControlPageIterator<ControlPdbInfoPageItemData> iterator{this, CONTROLFILE_PAGEMAP_PDBINFO_START};

    while (iterator.NextItem()) {
        ControlPdbInfoPageItemData *pdbInfoPageItemData = iterator.GetItem();
        if (pdbInfoPageItemData && strcmp(pdbInfoPageItemData->pdbName, pdbName) == 0 &&
            (pdbInfoPageItemData->pdbStatus != PdbStatus::PDB_STATUS_UNCREATED)) {
            return pdbInfoPageItemData;
        }
    }
    return nullptr;
}

RetStatus ControlPdbInfo::UpdatePdbItemData(PdbId pdbId, ControlPdbInfoPageItemData *pdbInfo, size_t pdbInfoLen)
{
    BlockNumber matchedPage;
    if (unlikely(pdbInfo == nullptr || pdbId != pdbInfo->pdbId)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("At least one paramter is illegal."));
        return DSTORE_FAIL;
    }

    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_EXCLUSIVE))) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Acquire control file lock fail."));
        return DSTORE_FAIL;
    }
    ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_EXCLUSIVE};
    if (unlikely(LoadGroup() == DSTORE_FAIL)) {
        ErrLog(DSTORE_ERROR, MODULE_PDB, ErrMsg("Do LoadGroup failed when update pdb %u.", pdbId));
        return DSTORE_FAIL;
    }

    ControlPdbInfoPageItemData *matchedPdbInfoItem = QueryControlPdbInfoPageItemData(pdbId, &matchedPage);
    if (STORAGE_VAR_NULL(matchedPdbInfoItem)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("pdbId %u not found.", pdbId));
        return DSTORE_FAIL;
    }

    if (unlikely(matchedPdbInfoItem->pdbStatus == PdbStatus::PDB_STATUS_UNCREATED)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("pdbStatus of pdbId %u is not USED or PENDING.", pdbId));
        return DSTORE_FAIL;
    }

    errno_t rc = memcpy_s(matchedPdbInfoItem, pdbInfoLen, pdbInfo, pdbInfoLen);
    storage_securec_check(rc, "\0", "\0");
    /* ignore pdbStatus field in pdbInfo. */
    matchedPdbInfoItem->pdbStatus = pdbInfo->pdbStatus;

    MarkPageDirty(matchedPage);

    if (STORAGE_FUNC_FAIL(PostGroup())) {
        ErrLog(DSTORE_ERROR, MODULE_PDB, ErrMsg("Do PostGroup failed when update pdb %u.", pdbId));
        return DSTORE_FAIL;
    }
    /* Update pdb Index. */
    if (STORAGE_FUNC_FAIL(PdbIndexUpdateItem(pdbInfo->pdbName, pdbId, pdbInfo->pdbStatus))) {
        ErrLog(DSTORE_ERROR, MODULE_PDB, ErrMsg("Do PdbIndexUpdateItem failed when update pdb %u.", pdbId));
        return DSTORE_FAIL;
    }

    return DSTORE_SUCC;
}

void ControlPdbInfo::AssignPdbBriefInfoByPdbInfoItem(PdbBriefInfo *pdbBriefInfo,
                                                     const ControlPdbInfoPageItemData *item)
{
    pdbBriefInfo->pdbId = item->pdbId;
    errno_t rc = memcpy_s(pdbBriefInfo->pdbName, DB_NAME_LEN, item->pdbName, DB_NAME_LEN);
    storage_securec_check(rc, "\0", "\0");
    rc = memcpy_s(pdbBriefInfo->pdbUuid, FORMATTED_UUID_ARR_LEN, item->pdbUuid, strlen(item->pdbUuid));
    storage_securec_check(rc, "\0", "\0");
    pdbBriefInfo->pdbStatus = item->pdbStatus;
    pdbBriefInfo->grpRespoolOid = item->grpRespoolOid;
    pdbBriefInfo->pdbRoleMode = item->pdbRoleMode;
    pdbBriefInfo->dbaId = item->dbaId;
    pdbBriefInfo->pdbSwitchStatus = item->pdbSwitchStatus;
}

RetStatus ControlPdbInfo::UpdatePdbRoleMode(PdbId pdbId, PdbRoleMode pdbRoleMode)
{
    if (unlikely(IsSingleNode())) {
        return DSTORE_SUCC;
    }
    /* 1. Lock exclusive lock for control file */
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_EXCLUSIVE))) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Acquire control file lock fail."));
        return DSTORE_FAIL;
    }
    {
        ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_EXCLUSIVE};
        if (unlikely(LoadGroup() == DSTORE_FAIL)) {
            return DSTORE_FAIL;
        }

        /* 2. Modify pdb status and persist it in control file, release lock */
        if (STORAGE_FUNC_FAIL(DoUpdatePdbRoleMode(pdbId, pdbRoleMode))) {
            ErrLog(DSTORE_ERROR, MODULE_CONTROL,
                   ErrMsg("Failed to update PDB status in control file locally. PdbId:%d; PdbRoleMode:%d", pdbId,
                          static_cast<int>(pdbRoleMode)));
            return DSTORE_FAIL;
        }
    }
    return DSTORE_SUCC;
}

RetStatus ControlPdbInfo::DoUpdatePdbRoleMode(PdbId pdbId, PdbRoleMode pdbRoleMode)
{
    BlockNumber matchedBlockNum;

    ControlPdbInfoPageItemData *matchedPdbInfoItem =
            QueryControlPdbInfoPageItemData(pdbId, &matchedBlockNum);
    if (STORAGE_VAR_NULL(matchedPdbInfoItem)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("PdbId %d is not found.", pdbId));
        return DSTORE_FAIL;
    }

    matchedPdbInfoItem->pdbRoleMode = pdbRoleMode;
    /* PdbSwitchStatus must be NORMAL with PdbRoleMode changed. */
    matchedPdbInfoItem->pdbSwitchStatus = PdbSwitchStatus::NORMAL;

    MarkPageDirty(matchedBlockNum);

    PdbInfoInvalidateData invaliddata(CTRL_FILE_REQUEST_PDB_ROLE_MODE_INVALIDATION, pdbId);
    if (STORAGE_FUNC_FAIL(PostGroup())) {
        return DSTORE_FAIL;
    }

    return DSTORE_SUCC;
}

RetStatus ControlPdbInfo::UpdatePdbRecycleCsnMin(PdbId pdbId, CommitSeqNo pdbRecycleCsnMin)
{
    if (unlikely(IsSingleNode())) {
        return DSTORE_SUCC;
    }
    /* 1. Lock exclusive lock for control file */
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_EXCLUSIVE))) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("[PdbRecycleCsnMin] Acquire control file lock fail."));
        return DSTORE_FAIL;
    }
    {
        ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_EXCLUSIVE};
        if (unlikely(LoadGroup() == DSTORE_FAIL)) {
            return DSTORE_FAIL;
        }

        /* 2. Modify pdb recycleCsnMin and persist it in control file, release lock */
        if (STORAGE_FUNC_FAIL(DoUpdatePdbRecycleCsnMin(pdbId, pdbRecycleCsnMin))) {
            ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("[PdbRecycleCsnMin] Failed to update PDB recycleCsnMin in "
                "control file locally. PdbId:%d; pdbRecycleCsnMin:%lu", pdbId, pdbRecycleCsnMin));
            return DSTORE_FAIL;
        }
        if (g_storageInstance->GetPdb(pdbId) != nullptr) {
            g_storageInstance->GetPdb(pdbId)->SetPdbRecycleCsnMin(pdbRecycleCsnMin);
        }
    }
    return DSTORE_SUCC;
}

RetStatus ControlPdbInfo::DoUpdatePdbRecycleCsnMin(PdbId pdbId, CommitSeqNo pdbRecycleCsnMin)
{
    BlockNumber matchedBlockNum;

    ControlPdbInfoPageItemData *matchedPdbInfoItem =
            QueryControlPdbInfoPageItemData(pdbId, &matchedBlockNum);
    if (STORAGE_VAR_NULL(matchedPdbInfoItem)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("[PdbRecycleCsnMin] PdbId %d is not found.", pdbId));
        return DSTORE_FAIL;
    }

    CommitSeqNo tmpCsn = matchedPdbInfoItem->pdbRecycleCsnMin;
    matchedPdbInfoItem->pdbRecycleCsnMin = pdbRecycleCsnMin;

    MarkPageDirty(matchedBlockNum);

    PdbInfoInvalidateData invaliddata(CTRL_FILE_REQUEST_PDB_RECYCLE_CSN_MIN_INVALIDATION, pdbId);
    if (STORAGE_FUNC_FAIL(PostGroup())) {
        return DSTORE_FAIL;
    }
    ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg(
        "[PdbRecycleCsnMin] pdb %u update pdbRecycleCsnMin from %lu to %lu in control file.",
        pdbId, tmpCsn, pdbRecycleCsnMin));

    return DSTORE_SUCC;
}

RetStatus ControlPdbInfo::GetAvailablePdbIdByUuid(const char *pdbUuid, PdbId *pdbId)
{
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_SHARE))) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Acquire control file lock fail."));
        return DSTORE_FAIL;
    }
    ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_SHARE};
    if (unlikely(LoadGroup() == DSTORE_FAIL)) {
        return DSTORE_FAIL;
    }

    ControlPageIterator<ControlPdbInfoPageItemData> iterator{this, CONTROLFILE_PAGEMAP_PDBINFO_START};
    while (iterator.NextItem()) {
        ControlPdbInfoPageItemData *pdbInfoPageItemData = iterator.GetItem();
        if (STORAGE_VAR_NULL(pdbInfoPageItemData)) {
            ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("item should not be null."));
            return DSTORE_FAIL;
        }
        if (pdbInfoPageItemData != nullptr && strcmp(pdbUuid, pdbInfoPageItemData->pdbUuid) == 0 &&
            (pdbInfoPageItemData->pdbStatus == PdbStatus::PDB_STATUS_OPENED_READ_WRITE ||
             pdbInfoPageItemData->pdbStatus == PdbStatus::PDB_STATUS_CLOSED)) {
            *pdbId = pdbInfoPageItemData->pdbId;
            return DSTORE_SUCC;
        }
    }
    return DSTORE_FAIL;
}

RetStatus ControlPdbInfo::GetPdbIdByUuid(const char *pdbUuid, PdbId *pdbId, PdbStatus pdbStatus)
{
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_SHARE))) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Acquire control file lock fail."));
        return DSTORE_FAIL;
    }
    ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_SHARE};
    if (unlikely(LoadGroup() == DSTORE_FAIL)) {
        return DSTORE_FAIL;
    }

    ControlPageIterator<ControlPdbInfoPageItemData> iterator{this, CONTROLFILE_PAGEMAP_PDBINFO_START};
    while (iterator.NextItem()) {
        ControlPdbInfoPageItemData *pdbInfoPageItemData = iterator.GetItem();
        if (STORAGE_VAR_NULL(pdbInfoPageItemData)) {
            ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("item should not be null."));
            return DSTORE_FAIL;
        }
        if (pdbInfoPageItemData != nullptr && strcmp(pdbUuid, pdbInfoPageItemData->pdbUuid) == 0 &&
            pdbInfoPageItemData->pdbStatus == pdbStatus) {
            *pdbId = pdbInfoPageItemData->pdbId;
            return DSTORE_SUCC;
        }
    }
    return DSTORE_FAIL;
}

FUNCTION_DELETE_LATER
RetStatus ControlPdbInfo::GetPdbIdByName(const char *pdbName, PdbId *pdbId, PdbStatus pdbStatus)
{
    /* If the index fails, it indicates that the index is invalid
     * and the control file needs to be traversed.
     */
    if (STORAGE_FUNC_SUCC(PdbIndexGetPdbIdByName(pdbName, pdbId, pdbStatus))) {
        return DSTORE_SUCC;
    }

    ControlPageIterator<ControlPdbInfoPageItemData> iterator{this, CONTROLFILE_PAGEMAP_PDBINFO_START};
    while (iterator.NextItem()) {
        ControlPdbInfoPageItemData *pdbInfoPageItemData = iterator.GetItem();
        if (pdbInfoPageItemData != nullptr && strcmp(pdbName, pdbInfoPageItemData->pdbName) == 0 &&
            pdbInfoPageItemData->pdbStatus == pdbStatus) {
            *pdbId = pdbInfoPageItemData->pdbId;
            return DSTORE_SUCC;
        }
    }
    return DSTORE_FAIL;
}

RetStatus ControlPdbInfo::IsPdbInRestoreFromBackup(const char *pdbName, bool *isInRestoreFromBackup)
{
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_SHARE))) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Acquire control file lock fail."));
        return DSTORE_FAIL;
    }
    ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_SHARE};
    if (unlikely(LoadGroup() == DSTORE_FAIL)) {
        return DSTORE_FAIL;
    }
    ControlPageIterator<ControlPdbInfoPageItemData> iterator{this, CONTROLFILE_PAGEMAP_PDBINFO_START};
    while (iterator.NextItem()) {
        ControlPdbInfoPageItemData *pdbInfoPageItemData = iterator.GetItem();
        if (pdbInfoPageItemData != nullptr && strcmp(pdbName, pdbInfoPageItemData->pdbName) == 0) {
            *isInRestoreFromBackup = pdbInfoPageItemData->isInRestoreFromBackup;
            return DSTORE_SUCC;
        }
    }
    return DSTORE_SUCC;
}

RetStatus ControlPdbInfo::UpdatePdbInRestoreFromBackup(PdbId pdbId, bool isPdbInRestoreFromBackup)
{
    BlockNumber matchedPage;
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_EXCLUSIVE))) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Acquire control file lock fail."));
        return DSTORE_FAIL;
    }
    ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_EXCLUSIVE};
    if (unlikely(LoadGroup() == DSTORE_FAIL)) {
        return DSTORE_FAIL;
    }

    ControlPdbInfoPageItemData *matchedPdbInfoItem = QueryControlPdbInfoPageItemData(pdbId, &matchedPage);
    if (matchedPdbInfoItem == nullptr) {
        ErrLog(DSTORE_WARNING, MODULE_CONTROL, ErrMsg("PdbId %d is not found.", pdbId));
        return DSTORE_FAIL;
    }

    matchedPdbInfoItem->isInRestoreFromBackup = isPdbInRestoreFromBackup;
    MarkPageDirty(matchedPage);

    if (STORAGE_FUNC_FAIL(PostGroup())) {
        return DSTORE_FAIL;
    }

    return DSTORE_SUCC;
}

RetStatus ControlPdbInfo::UpdateAllPdbInfoAfterRestore(void *allPdbInfo)
{
    ErrLog(DSTORE_LOG, MODULE_CONTROL, ErrMsg("UpdateAllPdbInfoAfterRestore entry."));
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_EXCLUSIVE))) {
        ErrLog(DSTORE_DEBUG1, MODULE_CONTROL, ErrMsg("Acquire control file lock fail."));
        return DSTORE_FAIL;
    }
    ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_EXCLUSIVE};
    if (unlikely(LoadGroup() == DSTORE_FAIL)) {
        return DSTORE_FAIL;
    }

    uint32 len = 0;
    ControlPageIterator<ControlPdbInfoPageItemData> iterator{this, CONTROLFILE_PAGEMAP_PDBINFO_START};
    while (iterator.NextItem()) {
        ControlPdbInfoPageItemData *item = iterator.GetItem();
        if (STORAGE_VAR_NULL(item)) {
            return DSTORE_FAIL;
        }

        ControlPdbInfoPageItemData *pdbInfoItem =
            STATIC_CAST_PTR_TYPE((static_cast<char *>(allPdbInfo) + len), ControlPdbInfoPageItemData *);
        if (pdbInfoItem == nullptr) {
            ErrLog(DSTORE_DEBUG1, MODULE_CONTROL, ErrMsg("Pdb is not found. len:%u", len));
            return DSTORE_FAIL;
        }
        if (pdbInfoItem->pdbId == item->pdbId) {
            if (unlikely(memcpy_s(item, sizeof(ControlPdbInfoPageItemData), pdbInfoItem,
                sizeof(ControlPdbInfoPageItemData)) != EOK)) {
                return DSTORE_FAIL;
            }
            if (strlen(pdbInfoItem->pdbUuid) > 0 && strlen(item->pdbUuid) > 0 &&
                strcmp(pdbInfoItem->pdbUuid, item->pdbUuid) == 0) {
                ErrLog(DSTORE_LOG, MODULE_CONTROL, ErrMsg("set pdb in restore state, pdbId:%u, itemUuid:%s, uuid:%s",
                    item->pdbId, item->pdbUuid, pdbInfoItem->pdbUuid));
                item->isInRestoreFromBackup = true;
            }
            MarkPageDirty(iterator.GetCurrentBlock());
            len += sizeof(ControlPdbInfoPageItemData);
        }
    }

    if (STORAGE_FUNC_FAIL(PostGroup())) {
        return DSTORE_FAIL;
    }

    return DSTORE_SUCC;
}

/* make sure we hold the share lock of control file */
RetStatus ControlPdbInfo::BuildPdbHashIndex()
{
    if (m_pdbHashIndex != nullptr) {
        return DSTORE_SUCC;
    }

    /* Step 1: init hash table */
    HASHCTL info;
    info.keysize = sizeof(PdbId);
    info.entrysize = sizeof(PdbHashIndexEntry);
    info.hash = uint32_hash;
    info.dsize = info.max_dsize = hash_select_dirsize(PDB_MAX_ID);
    info.hcxt = m_ctx;

    m_pdbHashIndex = hash_create("Root PDB Hash Index", PDB_MAX_ID, &info,
                                 HASH_ELEM | HASH_FUNCTION | HASH_DIRSIZE | HASH_CONTEXT | HASH_SHRCTX);
    StorageReleasePanic(!m_pdbHashIndex, MODULE_CATALOG, ErrMsg("Init Root PDB Hash Index failed."));

    ControlPageIterator<ControlPdbInfoPageItemData> iterator{this, CONTROLFILE_PAGEMAP_PDBINFO_START};
    bool found = false;
    ControlPdbInfoPageItemData *pdbInfoPageItemData = nullptr;
    size_t pdbNameLength = 0;
    while (iterator.NextItem()) {
        pdbInfoPageItemData = iterator.GetItem();
        if (STORAGE_VAR_NULL(pdbInfoPageItemData)) {
            ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("item should not be null."));
            return DSTORE_FAIL;
        }
        if (pdbInfoPageItemData->pdbId > PDB_MAX_ID || pdbInfoPageItemData->pdbId < PDB_START_ID) {
            ErrLog(DSTORE_ERROR, MODULE_CONTROL,
                ErrMsg("ControlPdbInfo: pdb id (%d) is out of bounds.", pdbInfoPageItemData->pdbId));
            return DSTORE_FAIL;
        }
        pdbNameLength = sizeof(char) * (strlen(pdbInfoPageItemData->pdbName) + 1);
        PdbHashIndexEntry *entry = static_cast<PdbHashIndexEntry *>(
            hash_search(m_pdbHashIndex, static_cast<void *>(&pdbInfoPageItemData->pdbId), HASH_ENTER, &found));
        if (found) {
            ErrLog(DSTORE_ERROR, MODULE_CONTROL,
                    ErrMsg("ControlPdbInfo: this pdb already exists in m_pdbHashIndex, pdbId is (%d).",
                        pdbInfoPageItemData->pdbId));
            return DSTORE_FAIL;
        }
        if (STORAGE_VAR_NULL(entry)) {
            hash_destroy(m_pdbHashIndex);
            m_pdbHashIndex = nullptr;
            ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Out of memory when create pdb hash index entry."));
            return DSTORE_FAIL;
        }
        errno_t rc = memcpy_s(entry->pdbName, pdbNameLength, pdbInfoPageItemData->pdbName, pdbNameLength);
        storage_securec_check(rc, "\0", "\0");
        entry->pdbStatus = pdbInfoPageItemData->pdbStatus;
        entry->pdbItemPointer.blkno = iterator.GetCurrentBlock();
        if (iterator.GetCurrentOffset() % sizeof(ControlPdbInfoPageItemData) != 0 ||
            iterator.GetCurrentOffset() / sizeof(ControlPdbInfoPageItemData) > PDB_ITEM_PER_CONTROLFILE) {
            ErrLog(DSTORE_ERROR, MODULE_CONTROL,
                    ErrMsg("ControlPdbInfo: pdb info page data offset error, current offset is (%d).",
                        iterator.GetCurrentOffset()));
            return DSTORE_FAIL;
        }
        entry->pdbItemPointer.offset = iterator.GetCurrentOffset() / sizeof(ControlPdbInfoPageItemData);
    }
    SetPdbIdxAvailable(true);
    return DSTORE_SUCC;
}

static RetStatus CheckVfsIsExistForL2Restore(DSTORE::PdbId pdbId, bool &vfsIsExist)
{
    char tempVfsName[MAX_CONFIG_NAME_LENGTH] = {0};
    StoragePdb::GenerateVfsName(pdbId, tempVfsName, MAX_CONFIG_NAME_LENGTH);
    VirtualFileSystem *vfs = NULL;
    ErrorCode retError =
        MountVfs(GetDefaultVfsClientHandle(), g_storageInstance->GetGuc()->tenantConfig->tenantName, tempVfsName, &vfs);
    if (retError != DSTORE_SUCC) {
        vfsIsExist = false;
        return DSTORE_SUCC;
    }
    vfsIsExist = true;
    retError = UnmountVfs(vfs);
    if (unlikely(retError != DSTORE_SUCC)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL,
               ErrMsg("UnmountVfs failed in L2Restore, vfsName:%s, pdbId:%u.", tempVfsName, pdbId));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

/* after restore, the vfs name may be inconsistent. need to correct it through config. the pdb state  may not meet
 * expectations, need to reset pdb. */
RetStatus ControlPdbInfo::RectifyPdbInfoAfterL2Restore()
{
    errno_t rc = EOK;
    bool needWrite = false;
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_EXCLUSIVE))) {
        ErrLog(DSTORE_DEBUG1, MODULE_CONTROL, ErrMsg("Acquire control file lock fail."));
        return DSTORE_FAIL;
    }
    ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_EXCLUSIVE};
    if (unlikely(LoadGroup() == DSTORE_FAIL)) {
        return DSTORE_FAIL;
    }
    TenantConfig *tenantCfg = g_storageInstance->GetGuc()->tenantConfig;

    ControlPageIterator<ControlPdbInfoPageItemData> iterator{this, CONTROLFILE_PAGEMAP_PDBINFO_START};
    ControlPdbInfoPageItemData *pdbInfoPageItemData = nullptr;
    while (iterator.NextItem()) {
        pdbInfoPageItemData = iterator.GetItem();
        if (STORAGE_VAR_NULL(pdbInfoPageItemData)) {
            return DSTORE_FAIL;
        }

        if (pdbInfoPageItemData->pdbStatus == PdbStatus::PDB_STATUS_UNCREATED) {
            continue;
        }

        if (pdbInfoPageItemData->pdbStatus != PdbStatus::PDB_STATUS_OPENED_READ_WRITE) {
            ErrLog(DSTORE_LOG, MODULE_CONTROL,
                   ErrMsg("Reset non-open status pdb %u after L2 restore.", pdbInfoPageItemData->pdbId));
            if (ControlPdbInfoPageItemData::Init(pdbInfoPageItemData, pdbInfoPageItemData->pdbId, "",
                                                 PdbStatus::PDB_STATUS_UNCREATED, "", "") == DSTORE_FAIL) {
                ErrLog(DSTORE_ERROR, MODULE_CONTROL,
                       ErrMsg("Reset non-open status pdb %u after L2 restore failed.", pdbInfoPageItemData->pdbId));
                return DSTORE_FAIL;
            }
            needWrite = true;
            MarkPageDirty(iterator.GetCurrentBlock());
            continue;
        }

        if (pdbInfoPageItemData->pdbId == PDB_TEMPLATE1_ID) {
            if (strcasecmp(pdbInfoPageItemData->vfsName, tenantCfg->storageConfig.template1VfsName) != 0) {
                ErrLog(DSTORE_LOG, MODULE_CONTROL,
                       ErrMsg("Rectify template1VfsName %s to %s.", pdbInfoPageItemData->vfsName,
                              tenantCfg->storageConfig.template1VfsName));
                rc = memset_s(pdbInfoPageItemData->vfsName, MAX_CONFIG_NAME_LENGTH, 0, MAX_CONFIG_NAME_LENGTH);
                storage_securec_check(rc, "\0", "\0");
                rc = strcpy_s(pdbInfoPageItemData->vfsName, MAX_CONFIG_NAME_LENGTH,
                              tenantCfg->storageConfig.template1VfsName);
                storage_securec_check(rc, "\0", "\0");
                needWrite = true;
                MarkPageDirty(iterator.GetCurrentBlock());
            }
            continue;
        }

        if (pdbInfoPageItemData->pdbId == PDB_TEMPLATE0_ID) {
            if (strcasecmp(pdbInfoPageItemData->vfsName, tenantCfg->storageConfig.template0VfsName) != 0) {
                ErrLog(DSTORE_LOG, MODULE_CONTROL,
                       ErrMsg("Rectify template0VfsName %s to %s.", pdbInfoPageItemData->vfsName,
                              tenantCfg->storageConfig.template0VfsName));
                rc = memset_s(pdbInfoPageItemData->vfsName, MAX_CONFIG_NAME_LENGTH, 0, MAX_CONFIG_NAME_LENGTH);
                storage_securec_check(rc, "\0", "\0");
                rc = strcpy_s(pdbInfoPageItemData->vfsName, MAX_CONFIG_NAME_LENGTH,
                              tenantCfg->storageConfig.template0VfsName);
                storage_securec_check(rc, "\0", "\0");
                needWrite = true;
                MarkPageDirty(iterator.GetCurrentBlock());
            }
            continue;
        }

        if (pdbInfoPageItemData->pdbId == PDB_ROOT_ID) {
            if (strcasecmp(pdbInfoPageItemData->vfsName, tenantCfg->storageConfig.rootpdbVfsName) != 0) {
                ErrLog(DSTORE_LOG, MODULE_CONTROL,
                       ErrMsg("Rectify rootpdbVfsName %s to %s.", pdbInfoPageItemData->vfsName,
                              tenantCfg->storageConfig.rootpdbVfsName));
                rc = memset_s(pdbInfoPageItemData->vfsName, MAX_CONFIG_NAME_LENGTH, 0, MAX_CONFIG_NAME_LENGTH);
                storage_securec_check(rc, "\0", "\0");
                rc = strcpy_s(pdbInfoPageItemData->vfsName, MAX_CONFIG_NAME_LENGTH,
                              tenantCfg->storageConfig.rootpdbVfsName);
                storage_securec_check(rc, "\0", "\0");
                needWrite = true;
                MarkPageDirty(iterator.GetCurrentBlock());
            }
            continue;
        }

        if (pdbInfoPageItemData->pdbId != PDB_TEMPLATEA_ID) {
            bool vfsIsExist = true;
            if (CheckVfsIsExistForL2Restore(pdbInfoPageItemData->pdbId, vfsIsExist) != DSTORE_SUCC) {
                return DSTORE_FAIL;
            }
            if (!vfsIsExist) {
                ErrLog(DSTORE_LOG, MODULE_CONTROL,
                       ErrMsg("Reset not exist pdb %u after L2 restore.", pdbInfoPageItemData->pdbId));
                if (ControlPdbInfoPageItemData::Init(pdbInfoPageItemData, pdbInfoPageItemData->pdbId, "",
                                                     PdbStatus::PDB_STATUS_UNCREATED, "", "") == DSTORE_FAIL) {
                    ErrLog(DSTORE_ERROR, MODULE_CONTROL,
                           ErrMsg("Reset not exist pdb %u after L2 restore failed.", pdbInfoPageItemData->pdbId));
                    return DSTORE_FAIL;
                }
                needWrite = true;
                MarkPageDirty(iterator.GetCurrentBlock());
                continue;
            }
        }

        if (((pdbInfoPageItemData->pdbId >= FIRST_USER_PDB_ID && pdbInfoPageItemData->pdbId <= PDB_MAX_ID) ||
             pdbInfoPageItemData->pdbId == PDB_TEMPLATEA_ID)) {
            char tempVfsName[MAX_CONFIG_NAME_LENGTH] = {0};
            StoragePdb::GenerateVfsName(pdbInfoPageItemData->pdbId, tempVfsName, MAX_CONFIG_NAME_LENGTH);
            if (strcasecmp(pdbInfoPageItemData->vfsName, tempVfsName) != 0) {
                ErrLog(DSTORE_LOG, MODULE_CONTROL,
                       ErrMsg("Rectify user pdb vfs name %s to %s.", pdbInfoPageItemData->vfsName, tempVfsName));
                rc = memset_s(pdbInfoPageItemData->vfsName, MAX_CONFIG_NAME_LENGTH, 0, MAX_CONFIG_NAME_LENGTH);
                storage_securec_check(rc, "\0", "\0");
                rc = strcpy_s(pdbInfoPageItemData->vfsName, MAX_CONFIG_NAME_LENGTH, tempVfsName);
                storage_securec_check(rc, "\0", "\0");
                needWrite = true;
                MarkPageDirty(iterator.GetCurrentBlock());
            }
            continue;
        }
    }

    if (!needWrite) {
        return DSTORE_SUCC;
    }
    if (STORAGE_FUNC_FAIL(PostGroup())) {
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus ControlPdbInfo::ResumePdbIdxIfNeed()
{
    if (GetPdbIdxAvailable() && m_pdbHashIndex == nullptr) {
        ErrLog(DSTORE_PANIC, MODULE_CONTROL, ErrMsg("PDB Hash Index status error."));
        return DSTORE_FAIL;
    }
    /* case 1: just be invalidate by other node. */
    DstoreLWLockAcquire(&m_pdbIndexLock, LW_EXCLUSIVE);
    if (GetPdbIdxAvailable() == false && m_pdbHashIndex != nullptr) {
        hash_destroy(m_pdbHashIndex);
        m_pdbHashIndex = nullptr;
    }
    /* case 2: init status */
    if (STORAGE_FUNC_FAIL(BuildPdbHashIndex())) {
        LWLockRelease(&m_pdbIndexLock);
        ErrLog(DSTORE_PANIC, MODULE_CONTROL, ErrMsg("PDB Hash Index build error."));
        return DSTORE_FAIL;
    }
    SetPdbIdxAvailable(true);
    LWLockRelease(&m_pdbIndexLock);
    return DSTORE_SUCC;
}

RetStatus ControlPdbInfo::PdbIndexGetPdbIdByName(const char *pdbName, PdbId *pdbId, PdbStatus status)
{
    if (STORAGE_FUNC_FAIL(ResumePdbIdxIfNeed())) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("PDB Hash Index resume error."));
        return DSTORE_FAIL;
    }
    PdbHashIndexEntry *entry = nullptr;
    HASH_SEQ_STATUS hstat;
    hash_seq_init(&hstat, m_pdbHashIndex);

    while ((entry = (PdbHashIndexEntry *)hash_seq_search(&hstat)) != nullptr) {
        if ((strcmp(entry->pdbName, pdbName) == 0) && status == entry->pdbStatus) {
            *pdbId = entry->pdbId;
            return DSTORE_SUCC;
        }
    }
    /* There is no data in the index. The outer layer
     * needs to skip the control file traversal.
     */
    return DSTORE_FAIL;
}

RetStatus ControlPdbInfo::PdbIndexUpdateItem(const char *pdbName, PdbId pdbId, PdbStatus status)
{
    if (STORAGE_FUNC_FAIL(ResumePdbIdxIfNeed())) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("PDB Hash Index resume error."));
        return DSTORE_FAIL;
    }
    if (pdbId > PDB_MAX_ID || pdbId < PDB_START_ID) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL,
        ErrMsg("ControlPdbInfo: the pdb id is out of bounds, pdbId is (%d).", pdbId));
        return DSTORE_FAIL;
    }
    PdbHashIndexEntry *entry = nullptr;
    bool found = false;
    entry =
        static_cast<PdbHashIndexEntry *>(hash_search(m_pdbHashIndex, static_cast<void *>(&pdbId), HASH_ENTER, &found));
    if (entry != nullptr) {
        size_t size = strlen(pdbName) + 1;
        errno_t rc = memcpy_s(entry->pdbName, size, pdbName, size);
        storage_securec_check(rc, "\0", "\0");
        entry->pdbStatus = status;
    }

    return DSTORE_SUCC;
}

ControlPdbInfoPageItemData *ControlPdbInfo::GetPdbInfoItemByCtid(PdbInfoItemPointerData ctid)
{
    ControlDataPage *page = GetPage(ctid.blkno);
    OffsetNumber offset = ctid.offset;
    if (unlikely(page == nullptr)) {
        return nullptr;
    }
    if (page->GetPageType() != CONTROL_PDBINFO_DATAPAGE_TYPE) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL,
                ErrMsg("ControlPdbInfo: get page type is not PDBINFO_DATAPAGE, pageType is (%d).",
                    page->GetPageType()));
        return nullptr;
    }
    return static_cast<ControlPdbInfoPageItemData *>(
        page->GetItem(offset * sizeof(ControlPdbInfoPageItemData), sizeof(ControlPdbInfoPageItemData)));
}

RetStatus ControlPdbInfo::GetAllPdbIdForBackupRestore(PdbId *pdbIdArray, uint32 &count)
{
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_SHARE))) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Acquire control file lock fail."));
        return DSTORE_FAIL;
    }
    ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_SHARE};
    if (unlikely(LoadGroup() == DSTORE_FAIL)) {
        return DSTORE_FAIL;
    }

    ControlPageIterator<ControlPdbInfoPageItemData> iterator{this, CONTROLFILE_PAGEMAP_PDBINFO_START};
    while (iterator.NextItem()) {
        ControlPdbInfoPageItemData *item = iterator.GetItem();
        if (item != nullptr &&
            (item->pdbStatus == PdbStatus::PDB_STATUS_OPENED_READ_WRITE ||
             item->pdbStatus == PdbStatus::PDB_STATUS_CLOSED || item->pdbStatus == PdbStatus::PDB_STATUS_CLOSING) &&
            (item->pdbId >= PDB_ROOT_ID && item->pdbId <= PDB_MAX_ID && item->pdbId != PDB_TEMPLATEA_ID)) {
            pdbIdArray[count++] = item->pdbId;
        }
    }
    return DSTORE_SUCC;
}

RetStatus ControlPdbInfo::PdbInfoPageAddItems()
{
    ControlPdbInfoPageItemData pdb;
    BlockNumber blockNumber = CONTROLFILE_PAGEMAP_PDBINFO_START;
    ControlDataPage *page = GetPage(CONTROLFILE_PAGEMAP_PDBINFO_START);
    if (unlikely(page == nullptr)) {
        return DSTORE_FAIL;
    }
    page->InitDataPage(CONTROL_PDBINFO_DATAPAGE_TYPE);
    MarkPageDirty(CONTROLFILE_PAGEMAP_PDBINFO_START);
    for (PdbId j = PDB_START_ID; j <= PDB_MAX_ID; j++) {
        if (page->GetAvailableSize() < sizeof(ControlPdbInfoPageItemData)) {
            /* extend new page */
            if (ExtendNewPage(CONTROL_PDBINFO_DATAPAGE_TYPE) == DSTORE_FAIL) {
                return DSTORE_FAIL;
            }
            BlockNumber nextUsedPageBlockNumber = GetLastPageBlockNumber();
            MarkPageDirty(blockNumber);
            blockNumber = nextUsedPageBlockNumber;
            SetLastPageBlockNumber(blockNumber);
            page = GetPage(blockNumber);
        }
        if (page == nullptr) {
            return DSTORE_SUCC;
        }
        if (ControlPdbInfoPageItemData::Init(&pdb, j, "", PdbStatus::PDB_STATUS_UNCREATED, "", "") == DSTORE_FAIL ||
            page->AddItem(&pdb, sizeof(ControlPdbInfoPageItemData)) == DSTORE_FAIL) {
            return DSTORE_FAIL;
        }
        MarkPageDirty(blockNumber);
    }
    return DSTORE_SUCC;
}

RetStatus ControlPdbInfo::Create()
{
    /* step 1, init metapage */
    ControlMetaPage *metaPage = GetMetaPage();
    if (unlikely(metaPage == nullptr)) {
        return DSTORE_FAIL;
    }
    metaPage->InitMetaPage(CONTROL_PDBINFO_METAPAGE_TYPE, sizeof(ControlPdbInfoMeta));
    ControlPdbInfoMeta metaData;
    metaData.m_version = 0;
    metaData.m_totalPdbCount = 0;
    if (unlikely(metaPage->SetMetaData(static_cast<void *>(&metaData), sizeof(ControlPdbInfoMeta)) == DSTORE_FAIL)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Failed to set metadata(grouptype:%hhu).", GetGroupType()));
        return DSTORE_FAIL;
    }

    ControlMetaHeader *metaHeader = metaPage->GetControlMetaHeader();
    metaHeader->m_lastPageId = CONTROLFILE_PAGEMAP_PDBINFO_START;
    ControlPageRange *pageRange = &metaHeader->m_pageRange[0];
    pageRange[0].m_start = CONTROLFILE_PAGEMAP_PDBINFO_START;
    pageRange[0].m_end = CONTROLFILE_PAGEMAP_PDBINFO_MAX;
    for (uint32 i = 1; i < CONTROL_MAX_PAGERANGE_NUM; i++) {
        pageRange[i].m_start = DSTORE_INVALID_BLOCK_NUMBER;
        pageRange[i].m_end = DSTORE_INVALID_BLOCK_NUMBER;
    }
    MarkPageDirty(CONTROLFILE_PAGEMAP_PDBINFO_META);

    /* step 2, init pdbs */
    PdbInfoPageAddItems();

    return DSTORE_SUCC;
}

ControlPdbInfoPageItemData *ControlPdbInfo::GetDropablePdbInfo(const char *pdbName)
{
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_SHARE))) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Acquire control file lock fail."));
        return nullptr;
    }
    ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_SHARE};
    if (unlikely(LoadGroup() == DSTORE_FAIL)) {
        return nullptr;
    }

    ControlPageIterator<ControlPdbInfoPageItemData> iterator{this, CONTROLFILE_PAGEMAP_PDBINFO_START};
    while (iterator.NextItem()) {
        ControlPdbInfoPageItemData *pdbInfoPageItemData = iterator.GetItem();
        if (STORAGE_VAR_NULL(pdbInfoPageItemData)) {
            ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("item should not be null."));
            return nullptr;
        }
        if (strcmp(pdbInfoPageItemData->pdbName, pdbName) == 0 &&
            (pdbInfoPageItemData->pdbStatus == PdbStatus::PDB_STATUS_CREATING ||
             pdbInfoPageItemData->pdbStatus == PdbStatus::PDB_STATUS_DROPPING ||
             pdbInfoPageItemData->pdbStatus == PdbStatus::PDB_STATUS_CLOSED)) {
            return pdbInfoPageItemData;
        }
    }
    return nullptr;
}

RetStatus ControlPdbInfo::ReadPdbCxt(PdbId pdbId)
{
    if (unlikely(!StoragePdb::IsValidPdbId(pdbId))) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("ReadPdbCxt PdbId(%u) is invalid.", pdbId));
        return DSTORE_FAIL;
    }
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_SHARE))) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Acquire control file lock fail."));
        return DSTORE_FAIL;
    }
    ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_SHARE};
    if (unlikely(LoadGroup() == DSTORE_FAIL)) {
        return DSTORE_FAIL;
    }

    /* make sure pdb name will not duplicate. */
    ControlPageIterator<ControlPdbInfoPageItemData> iterator{this, CONTROLFILE_PAGEMAP_PDBINFO_START};
    while (iterator.NextItem()) {
        ControlPdbInfoPageItemData *item = iterator.GetItem();
        if (unlikely(item == nullptr)) {
            ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("ReadPdbCxt PdbId(%u) get item failed.", pdbId));
            return DSTORE_FAIL;
        }
        if (item->pdbId == pdbId) {
            StoragePdb *pdb = g_storageInstance->GetPdb(pdbId);
            if (unlikely(pdb == nullptr)) {
                ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("ReadPdbCxt PdbId(%u) get pdb null.", pdbId));
                return DSTORE_FAIL;
            }
            g_storageInstance->LockPdbStatus<LW_EXCLUSIVE>(pdbId);
            pdb->SetPdbInfo(item);
            g_storageInstance->UnLockPdbStatus(pdbId);
            return DSTORE_SUCC;
        }
    }
    return DSTORE_FAIL;
}
RetStatus ControlPdbInfo::GetVisiablePdbInfoAll(PdbBriefInfo *pdbIdInfos, uint32_t *pdbCount)
{
    uint32 count = 0;
    PdbBriefInfo *pdbBriefInfo = nullptr;

    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_SHARE))) {
        return DSTORE_FAIL;
    }
    ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_SHARE};

    if (unlikely(LoadGroup() == DSTORE_FAIL)) {
        return DSTORE_FAIL;
    }

    ControlPageIterator<ControlPdbInfoPageItemData> iterator{this, CONTROLFILE_PAGEMAP_PDBINFO_START};
    while (iterator.NextItem()) {
        ControlPdbInfoPageItemData *pdbInfoPageItemData = iterator.GetItem();
        if (count >= *pdbCount) {
            break;
        }
        if (STORAGE_VAR_NULL(pdbInfoPageItemData)) {
            ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("item should not be null."));
            return DSTORE_FAIL;
        }
        if (pdbInfoPageItemData->pdbStatus != PdbStatus::PDB_STATUS_UNCREATED) {
            pdbBriefInfo = static_cast<PdbBriefInfo *>(pdbIdInfos + count);
            AssignPdbBriefInfoByPdbInfoItem(pdbBriefInfo, pdbInfoPageItemData);
            count++;
        }
    }
    *pdbCount = count;
    return DSTORE_SUCC;
}

RetStatus ControlPdbInfo::ProcessPdbRoleModeInvalidate(const PdbInfoInvalidateData *invaliddata)
{
    if (STORAGE_VAR_NULL(invaliddata) || STORAGE_VAR_NULL(g_storageInstance->GetPdb(invaliddata->m_pdbId))) {
        return DSTORE_FAIL; //  invalid data or invalid pdb
    }
    g_storageInstance->GetPdb(invaliddata->m_pdbId)->SetPdbRoleMode(PdbRoleMode::PDB_INVALID);
    return DSTORE_SUCC;
}

RetStatus ControlPdbInfo::ProcessPdbRecycleCsnMinInvalidate(const PdbInfoInvalidateData *invaliddata)
{
    if (STORAGE_VAR_NULL(invaliddata) || STORAGE_VAR_NULL(g_storageInstance->GetPdb(invaliddata->m_pdbId))) {
        return DSTORE_FAIL; //  invalid data or invalid pdb
    }
    g_storageInstance->GetPdb(invaliddata->m_pdbId)->SetPdbRecycleCsnMin(MAX_COMMITSEQNO);
    ErrLog(DSTORE_LOG, MODULE_FRAMEWORK, ErrMsg("[PdbRecycleCsnMin] pdb %u invalid m_pdbRecycleCsnMin.",
        invaliddata->m_pdbId));
    return DSTORE_SUCC;
}

RetStatus ControlPdbInfo::ProcessPdbSyncModeInvalidate()
{
    return DSTORE_FAIL;
}
RetStatus ControlPdbInfo::InvalidatePdbInfo(const void* data, uint32 dataLen)
{
    if (data == nullptr || dataLen == 0) {
        return DSTORE_FAIL;
    }
    const PdbInfoInvalidateData *metaData = static_cast<const PdbInfoInvalidateData *>(data);
    switch (metaData->m_type) {
        /* Requests for ONLY root pdb */
        case CTRL_FILE_REQUEST_PDB_ROLE_MODE_INVALIDATION:
            ProcessPdbRoleModeInvalidate(metaData);
            break;
        case CTRL_FILE_REQUEST_PDB_SYNC_MODE_INVALIDATION:
            ProcessPdbSyncModeInvalidate();
            break;
        case CTRL_FILE_REQUEST_PDB_RECYCLE_CSN_MIN_INVALIDATION:
            ProcessPdbRecycleCsnMinInvalidate(metaData);
            break;
        default:
            ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Invalid msg type %hhu.", metaData->m_type));
            StorageAssert(0);
            break;
    }
    return DSTORE_SUCC;
}
}  // namespace DSTORE