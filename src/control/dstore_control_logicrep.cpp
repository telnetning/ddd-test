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
 * dstore_control_logicrep.cpp
 *
 *
 *
 * IDENTIFICATION
 *        dstore/src/control/dstore_control_logicrep.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "control/dstore_control_logicrep.h"
#include "control/dstore_control_file_lock.h"

namespace DSTORE {
RetStatus ControlLogicRep::AddLogicalReplicationSlot(ControlLogicalReplicationSlotPageItemData *data)
{
    if (unlikely(data == nullptr)) {
        return DSTORE_FAIL;
    }
    ErrLog(DSTORE_LOG, MODULE_LOGICAL_REPLICATION,
        ErrMsg("create slot on disk, slotname(%s) with walId(%lu).", data->name.data, data->walId));
    return AddItemToControlFile(reinterpret_cast<const char *>(data), sizeof(ControlLogicalReplicationSlotPageItemData),
        CONTROL_LOGICALREP_DATAPAGE_TYPE);
}

RetStatus ControlLogicRep::DeleteLogicalReplicationSlot(char *slotName, WalId walId, bool &isExist)
{
    if (unlikely(slotName == nullptr)) {
        return DSTORE_FAIL;
    }
    StorageAssert(m_isInitialized);

    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_EXCLUSIVE))) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Acquire control file lock fail."));
        return DSTORE_FAIL;
    }

    ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_EXCLUSIVE};
    isExist = false;

    if (unlikely(LoadGroup() == DSTORE_FAIL)) {
        return DSTORE_FAIL;
    }
    BlockNumber curBlockNumber = CONTROLFILE_PAGEMAP_LOGICALREP_START;
    ControlPageIterator<ControlLogicalReplicationSlotPageItemData> iterator{
        this, curBlockNumber, sizeof(ControlLogicalReplicationSlotPageItemData)};

    /* Read ControlPage one by one */
    while (iterator.NextItem()) {
        ControlLogicalReplicationSlotPageItemData *itemData = iterator.GetItem();
        if (unlikely(itemData == nullptr)) {
            return DSTORE_FAIL;
        }
        if (strcmp(itemData->name.data, slotName) == 0 && itemData->walId == walId) {
            isExist = true;
            ControlPage *controlPage = GetPage(iterator.GetCurrentBlock());
            if (unlikely(controlPage == nullptr)) {
                return DSTORE_FAIL;
            }
            RetStatus rc = controlPage->RemoveItem(static_cast<size_t>(iterator.GetCurrentOffset()),
                                                   sizeof(ControlLogicalReplicationSlotPageItemData));
            StorageAssert(rc == DSTORE_SUCC);
            UNUSED_VARIABLE(rc);
            iterator.MarkItemDelete();
            BlockNumber lastPage = GetLastPageBlockNumber();
            SetLastPageBlockNumber(DstoreMin(curBlockNumber, lastPage));
            MarkPageDirty(curBlockNumber);
        }
    }

    if (STORAGE_FUNC_FAIL(PostGroup())) {
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus ControlLogicRep::UpdateLogicalReplicationSlot(ControlLogicalReplicationSlotPageItemData *data)
{
    if (unlikely(data == nullptr)) {
        return DSTORE_FAIL;
    }
    StorageAssert(m_isInitialized);
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_EXCLUSIVE))) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Acquire control file lock fail."));
        return DSTORE_FAIL;
    }
    ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_EXCLUSIVE};
    if (unlikely(LoadGroup() == DSTORE_FAIL)) {
        return DSTORE_FAIL;
    }

    ControlPageIterator<ControlLogicalReplicationSlotPageItemData> iterator{this,
        CONTROLFILE_PAGEMAP_LOGICALREP_START, sizeof(ControlLogicalReplicationSlotPageItemData)};
    ErrLog(DSTORE_LOG, MODULE_LOGICAL_REPLICATION,
        ErrMsg("update slot on disk, slotname(%s) with walId(%lu), confirmCsn(%lu), walPlsn(%lu).",
            data->name.data, data->walId, data->confirmedCsn, data->restartPlsn));

    /* Read ControlPage one by one */
    bool isExist = false;
    while (iterator.NextItem()) {
        ControlLogicalReplicationSlotPageItemData *itemData = iterator.GetItem();
        if (STORAGE_VAR_NULL(itemData)) {
            StorageAssert(itemData != nullptr);
            return DSTORE_FAIL;
        }
        if (strcmp(itemData->name.data, data->name.data) == 0 && itemData->walId == data->walId) {
            isExist = true;
            /* update member */
            errno_t rc = memcpy_s(itemData, sizeof(ControlLogicalReplicationSlotPageItemData),
                data, sizeof(ControlLogicalReplicationSlotPageItemData));
            storage_securec_check(rc, "\0", "\0");
            MarkPageDirty(iterator.GetCurrentBlock());
        }
    }
    StorageAssert(isExist);
    UNUSED_VARIABLE(isExist);
    if (STORAGE_FUNC_FAIL(PostGroup())) {
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus ControlLogicRep::GetAllLogicalReplicationSlotBaseOnWalId(WalId targetWalId,
    ControlLogicalReplicationSlotPageItemData **repSlotinfo, int &slotCountNum)
{
    if (unlikely(repSlotinfo == nullptr)) {
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

    ControlPageIterator<ControlLogicalReplicationSlotPageItemData> iterator{this, CONTROLFILE_PAGEMAP_LOGICALREP_START};

    while (iterator.NextItem()) {
        ControlLogicalReplicationSlotPageItemData *itemData = iterator.GetItem();
        if (itemData == nullptr || itemData->walId != targetWalId) {
            continue;
        }
        repSlotinfo[slotCountNum] = itemData;
        slotCountNum++;
    }
    return DSTORE_SUCC;
}

RetStatus ControlLogicRep::Create()
{
    /* step 1, init metapage */
    ControlMetaPage *metaPage = GetMetaPage();
    if (unlikely(metaPage == nullptr)) {
        return DSTORE_FAIL;
    }
    metaPage->InitMetaPage(CONTROL_LOGICALREP_METAPAGE_TYPE, 0);

    ControlMetaHeader *metaHeader = metaPage->GetControlMetaHeader();
    metaHeader->m_lastPageId = CONTROLFILE_PAGEMAP_LOGICALREP_START;
    ControlPageRange *pageRange = &metaHeader->m_pageRange[0];
    pageRange[0].m_start = CONTROLFILE_PAGEMAP_LOGICALREP_START;
    pageRange[0].m_end = CONTROLFILE_PAGEMAP_LOGICALREP_MAX;
    for (uint32 i = 1; i < CONTROL_MAX_PAGERANGE_NUM; i++) {
        pageRange[i].m_start = DSTORE_INVALID_BLOCK_NUMBER;
        pageRange[i].m_end = DSTORE_INVALID_BLOCK_NUMBER;
    }
    MarkPageDirty(CONTROLFILE_PAGEMAP_LOGICALREP_META);

    /* step 2, init sharedpage */
    ControlDataPage *page = GetPage(CONTROLFILE_PAGEMAP_LOGICALREP_START);
    if (unlikely(page == nullptr)) {
        return DSTORE_FAIL;
    }
    page->InitDataPage(CONTROL_LOGICALREP_DATAPAGE_TYPE);
    MarkPageDirty(CONTROLFILE_PAGEMAP_LOGICALREP_START);

    return DSTORE_SUCC;
}
}