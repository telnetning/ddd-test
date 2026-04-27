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
 * dstore_control_relmap.cpp
 *
 *
 *
 * IDENTIFICATION
 *        dstore/src/control/dstore_control_relmap.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "control/dstore_control_relmap.h"
#include "control/dstore_control_file_lock.h"
#include "pdb/dstore_pdb_interface.h"

namespace DSTORE {
RetStatus ControlRelmap::AddSysTableItem(const Oid sysTableOid, const PageId &segmentId)
{
    ControlSysTableItemData sysTableItem{};
    sysTableItem.sysTableOid = sysTableOid;
    sysTableItem.segmentId.m_fileId = segmentId.m_fileId;
    sysTableItem.segmentId.m_blockId = segmentId.m_blockId;

    return AddItemToControlFile(reinterpret_cast<const char *>(&sysTableItem), sizeof(ControlSysTableItemData),
                                CONTROL_RELMAP_SHARED_DATAPAGE_TYPE);
}

RetStatus ControlRelmap::GetSysTableItem(Oid sysTableOid, PageId &segmentId)
{
    if (unlikely(m_lock->Lock(CFLockMode::CF_SHARE) == DSTORE_FAIL)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Acquire control file lock fail."));
        return DSTORE_FAIL;
    }
    ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_SHARE};
    if (unlikely(LoadGroup() == DSTORE_FAIL)) {
        return DSTORE_FAIL;
    }

    ControlMetaPage *metaPage = GetMetaPage();
    if (metaPage == nullptr) {
        storage_set_error(CONTROL_ERROR_NOT_INITIALIZED);
        return DSTORE_FAIL;
    }
    ControlPageIterator<ControlSysTableItemData> iterator{this, CONTROLFILE_PAGEMAP_RELMAP_START};
    while (iterator.NextItem()) {
        ControlSysTableItemData *systbItem = iterator.GetItem();
        if (systbItem !=nullptr && systbItem->sysTableOid == sysTableOid) {
            segmentId.m_fileId = systbItem->segmentId.m_fileId;
            segmentId.m_blockId = systbItem->segmentId.m_blockId;
            ErrLog(DSTORE_DEBUG1, MODULE_CONTROL,
                   ErrMsg("get SystableItem(%u) pageId(%hu, %u) offset(%hu).", sysTableOid, segmentId.m_fileId,
                          segmentId.m_blockId, iterator.GetCurrentOffset()));
            return DSTORE_SUCC;
        }
    }

    ControlPageIterator<ControlSysTableItemData> iterator_local{this, CONTROLFILE_PAGEMAP_RELMAP_START + 1};
    while (iterator_local.NextItem()) {
        ControlSysTableItemData *systbItem = iterator_local.GetItem();
        if (STORAGE_VAR_NULL(systbItem)) {
            StorageAssert(systbItem != nullptr);
            break;
        }
        if (systbItem->sysTableOid == sysTableOid) {
            segmentId.m_fileId = systbItem->segmentId.m_fileId;
            segmentId.m_blockId = systbItem->segmentId.m_blockId;
            ErrLog(DSTORE_DEBUG1, MODULE_CONTROL,
                   ErrMsg("get SystableItem(%u) pageId(%hu, %u) offset(%hu).", sysTableOid, segmentId.m_fileId,
                          segmentId.m_blockId, iterator_local.GetCurrentOffset()));
            return DSTORE_SUCC;
        }
    }

    segmentId.m_fileId = INVALID_PAGE_ID.m_fileId;
    segmentId.m_blockId = INVALID_PAGE_ID.m_blockId;
    return DSTORE_FAIL;
}

BlockNumber ControlRelmap::GetSystableItemPageNumber(int type)
{
    BlockNumber pageNumber;
    ControlRelMapMeta *metadata = static_cast<ControlRelMapMeta *>(GetMetaData());
    if (unlikely(metadata == nullptr)) {
        return DSTORE_INVALID_BLOCK_NUMBER;
    }

    if (static_cast<SysTableMapType>(type) == SysTableMapType::SYSTABLE_SHARED) {
        pageNumber = metadata->m_relMapSharedPageNumber;
    } else if (static_cast<SysTableMapType>(type) == SysTableMapType::SYSTABLE_LOCAL) {
        pageNumber = metadata->m_relMapLocalPageNumber;
    } else {
        return DSTORE_INVALID_BLOCK_NUMBER;
    }
    return pageNumber;
}

RetStatus ControlRelmap::GetAllSysTableItem(int type, ControlSysTableItemData *systbItem, int &count)
{
    StorageAssert(m_isInitialized);
    if (unlikely(m_lock->Lock(CFLockMode::CF_SHARE) == DSTORE_FAIL)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Acquire control file lock fail."));
        return DSTORE_FAIL;
    }
    ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_SHARE};
    if (unlikely(LoadGroup() == DSTORE_FAIL)) {
        return DSTORE_FAIL;
    }
    BlockNumber pageNumber = GetSystableItemPageNumber(type);
    if (pageNumber == DSTORE_INVALID_BLOCK_NUMBER)  {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("invalid type %d for pdb %d", type, m_pdbId));
        return DSTORE_FAIL;
    }

    ControlDataPage *controlPage =
        static_cast<ControlPage *>(static_cast<void *>(GetPage(pageNumber)));
    if (unlikely(controlPage == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("group(%hhu) get page(%u) fail.", GetGroupType(), pageNumber));
        return DSTORE_FAIL;
    }

    /* step 1, get the count of item */
    uint32 totalCount = controlPage->GetWriteOffset() / sizeof(ControlSysTableItemData);
    /* step 2, get all item */
    if (totalCount > 0 && totalCount <= (uint32)count) {
        ControlPageIterator<ControlSysTableItemData> iterator{this, pageNumber};
        int i = 0;
        while (iterator.NextItem()) {
            ControlSysTableItemData *item = iterator.GetItem();
            if (STORAGE_VAR_NULL(item)) {
                StorageAssert(0); /* should not be null. */
                return DSTORE_FAIL;
            }
            systbItem[i] = *item;
            i++;
            if ((uint32)i >= totalCount) {
                break;
            }
        }
        count = i;
    } else {
        systbItem = nullptr;
        count = 0;
    }
    return DSTORE_SUCC;
}

static ControlPageType GetSystableItemPageType(int type)
{
    ControlPageType pageType;
    if (static_cast<SysTableMapType>(type) == SysTableMapType::SYSTABLE_SHARED) {
        pageType = CONTROL_RELMAP_SHARED_DATAPAGE_TYPE;
    } else if (static_cast<SysTableMapType>(type) == SysTableMapType::SYSTABLE_LOCAL) {
        pageType = CONTROL_RELMAP_LOCAL_DATAPAGE_TYPE;
    } else {
        return CONTROL_PAGE_TYPE_INVALID;
    }
    return pageType;
}

RetStatus ControlRelmap::WriteAllSysTableItem(int type, ControlSysTableItemData *systbItems, int count)
{
    StorageAssert(m_isInitialized);

    /* acquire exclusive file lock, make sure the file will not extend or change by others */
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_EXCLUSIVE))) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Acquire control file lock fail."));
        return DSTORE_FAIL;
    }
    ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_EXCLUSIVE};
    if (unlikely(LoadGroup() == DSTORE_FAIL)) {
        return DSTORE_FAIL;
    }
    BlockNumber pageNumber = GetSystableItemPageNumber(type);
    if (pageNumber == DSTORE_INVALID_BLOCK_NUMBER)  {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("invalid type %d for pdb %d", type, m_pdbId));
        return DSTORE_FAIL;
    }

    ControlDataPage *controlPage =
        static_cast<ControlDataPage *>(static_cast<void *>(GetPage(pageNumber)));
    if (unlikely(controlPage == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("group(%hhu) get page(%u) fail.", GetGroupType(), pageNumber));
        return DSTORE_FAIL;
    }
    ControlPageType pageType = GetSystableItemPageType(type);
    controlPage->InitDataPage(pageType);
    if ((controlPage->GetAvailableSize() >= sizeof(ControlSysTableItemData) * count)) {
        if (STORAGE_FUNC_FAIL(controlPage->AddItem(systbItems, sizeof(ControlSysTableItemData) * count))) {
            return DSTORE_FAIL;
        }
    } else {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL,
               ErrMsg("group(%hhu) page(%u) space not enough.", GetGroupType(), pageNumber));
        return DSTORE_FAIL;
    }
    /* for GetSysTableItem */
    if (pageType == CONTROL_RELMAP_SHARED_DATAPAGE_TYPE) {
        controlPage->SetNextPage(pageNumber + 1);
    }

    /* mark page(blockNumber) as dirty */
    MarkPageDirty(pageNumber);
    if (STORAGE_FUNC_FAIL(PostGroup())) {
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus ControlRelmap::InvalidateRelmapInfo(const void* data, uint32 dataLen)
{
    if (unlikely(data == nullptr || dataLen != sizeof(RelmapInvalidateData))) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL,
               ErrMsg("Relmap invaldate data length(%u) is incorrect or data is nullptr.", dataLen));
        return DSTORE_FAIL;
    }
    const RelmapInvalidateData *invalidateData = static_cast<const RelmapInvalidateData *>(data);
    RelMapType relMapType = (RelMapType)(invalidateData->type);
    if (unlikely(relMapType != RelMapType::RELMAP_SHARED && relMapType != RelMapType::RELMAP_LOCAL)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Relmap invaldate type(%d) is incorrect.", invalidateData->type));
        return DSTORE_FAIL;
    }

    return DSTORE_SUCC;
}

RetStatus ControlRelmap::Create()
{
    /* step 1, init metapage */
    ControlMetaPage *metaPage = GetMetaPage();
    if (unlikely(metaPage == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("group(%hhu) get metapage fail.", GetGroupType()));
        return DSTORE_FAIL;
    }
    metaPage->InitMetaPage(CONTROL_RELMAP_METAPAGE_TYPE, sizeof(ControlRelMapMeta));
    ControlRelMapMeta metaData;
    metaData.m_version = 0;
    metaData.m_relMapSharedPageNumber = CONTROLFILE_PAGEMAP_RELMAP_START;
    metaData.m_relMapLocalPageNumber = metaData.m_relMapSharedPageNumber + 1;
    if (unlikely(metaPage->SetMetaData(static_cast<void *>(&metaData), sizeof(ControlRelMapMeta)) == DSTORE_FAIL)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Failed to set metadata(grouptype:%hhu).", GetGroupType()));
        return DSTORE_FAIL;
    }
    ControlMetaHeader *metaHeader = metaPage->GetControlMetaHeader();
    metaHeader->m_lastPageId = CONTROLFILE_PAGEMAP_RELMAP_START;
    ControlPageRange *pageRange = &metaHeader->m_pageRange[0];
    pageRange[0].m_start = CONTROLFILE_PAGEMAP_RELMAP_START;
    pageRange[0].m_end = CONTROLFILE_PAGEMAP_RELMAP_MAX;
    for (uint32 i = 1; i < CONTROL_MAX_PAGERANGE_NUM; i++) {
        pageRange[i].m_start = DSTORE_INVALID_BLOCK_NUMBER;
        pageRange[i].m_end = DSTORE_INVALID_BLOCK_NUMBER;
    }
    MarkPageDirty(CONTROLFILE_PAGEMAP_RELMAP_META);

    /* step 2, init sharedpage */
    ControlDataPage *sharedPage = GetPage(metaData.m_relMapSharedPageNumber);
    if (unlikely(sharedPage == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL,
               ErrMsg("group(%hhu) get page(%u) fail.", GetGroupType(), metaData.m_relMapSharedPageNumber));
        return DSTORE_FAIL;
    }
    sharedPage->InitDataPage(CONTROL_RELMAP_SHARED_DATAPAGE_TYPE);
    sharedPage->SetNextPage(metaData.m_relMapLocalPageNumber);
    MarkPageDirty(metaData.m_relMapSharedPageNumber);

    /* step 3, init localpage */
    ControlDataPage *localPage = GetPage(metaData.m_relMapLocalPageNumber);
    if (unlikely(localPage == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL,
               ErrMsg("group(%hhu) get page(%u) fail.", GetGroupType(), metaData.m_relMapLocalPageNumber));
        return DSTORE_FAIL;
    }
    localPage->InitDataPage(CONTROL_RELMAP_LOCAL_DATAPAGE_TYPE);
    MarkPageDirty(metaData.m_relMapLocalPageNumber);

    return DSTORE_SUCC;
}
}
