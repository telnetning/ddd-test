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
 * dstore_control_group.cpp
 *
 *
 *
 * IDENTIFICATION
 *        dstore/src/control/dstore_control_group.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "control/dstore_control_group.h"
#include "control/dstore_control_file_lock.h"

namespace DSTORE {
RetStatus ControlGroup::LoadGroup()
{
    if (unlikely(!m_isInitialized)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL,
               ErrMsg("Load controlgroup(%hhu) failed, not init.", static_cast<uint8>(m_groupType)));
        return DSTORE_FAIL;
    }

    if (unlikely(m_metaBlock == DSTORE_INVALID_BLOCK_NUMBER)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL,
               ErrMsg("Load controlgroup(%hhu) failed, metaBlock(%u).", static_cast<uint8>(m_groupType), m_metaBlock));
        return DSTORE_FAIL;
    }

    /* clean cache page */
    m_metaPage = nullptr;
    if (STORAGE_FUNC_FAIL(m_controlFileMgr->GetValidMetaPage(m_pageHandle, m_metaBlock))) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL,
               ErrMsg("Load controlgroup(%hhu) failed, metaBlock(%u).", static_cast<uint8>(m_groupType), m_metaBlock));
        return DSTORE_FAIL;
    }

    return DSTORE_SUCC;
}

RetStatus ControlGroup::PostGroup()
{
    if (unlikely(m_pageHandle == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL,
               ErrMsg("Post controlgroup(%hhu) failed, pagehandle is nullptr.", static_cast<uint8>(m_groupType)));
        return DSTORE_FAIL;
    }
    if (unlikely(m_controlFileMgr->PostPageHandle(m_pageHandle, m_metaBlock) ==
                 DSTORE_FAIL)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Post group(%hhu) fail.", GetGroupType()));
        return DSTORE_FAIL;
    }
    /* Next re-read metapage */
    m_metaPage = nullptr;
    return DSTORE_SUCC;
}

RetStatus ControlGroup::MarkPageDirty(BlockNumber blocknum)
{
    return m_controlFileMgr->MarkPageDirty(m_pageHandle, blocknum);
}

RetStatus ControlGroup::SetLastPageBlockNumber(BlockNumber blocknum)
{
    if (GetMetaPage() == nullptr) {
        return DSTORE_FAIL;
    }
    m_metaPage->GetMetaHeader()->m_lastPageId = blocknum;
    return MarkPageDirty(m_metaBlock);
}

BlockNumber ControlGroup::GetLastPageBlockNumber()
{
    if (GetMetaPage() == nullptr) {
        return DSTORE_INVALID_BLOCK_NUMBER;
    }
    return m_metaPage->GetMetaHeader()->m_lastPageId;
}

RetStatus ControlGroup::ExtendNewPage(ControlPageType controlPageType)
{
    ControlMetaPage* metaPage = GetMetaPage();
    if (unlikely(metaPage == nullptr)) {
        storage_set_error(CONTROL_ERROR_NOT_INITIALIZED);
        return DSTORE_FAIL;
    }
    BlockNumber oldLastBlockNumber = GetLastPageBlockNumber();
    BlockNumber nextUsedPageBlockNumber = oldLastBlockNumber + 1;

    /* if the next used page exceed the control group total size, return fail */
    if (nextUsedPageBlockNumber >= GetMaxPageNum()) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL,
               ErrMsg("Extend controlgroup(%hhu) failed, nextUsedPageBlockNumber(%u) >= maxPageNum(%u).",
                      static_cast<uint8>(m_groupType), nextUsedPageBlockNumber, GetMaxPageNum()));
        return DSTORE_FAIL;
    }

    /* allocate a new page and link it to the end */
    ControlPage *oldControlPage = GetPage(oldLastBlockNumber);
    if (unlikely(oldControlPage == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL,
               ErrMsg("Extend controlgroup(%hhu) failed, oldLastBlockNumber(%u) is invalid.",
                      static_cast<uint8>(m_groupType), oldLastBlockNumber));
        return DSTORE_FAIL;
    }
    oldControlPage->SetNextPage(nextUsedPageBlockNumber);
    MarkPageDirty(oldLastBlockNumber);

    /* set used page count */
    SetLastPageBlockNumber(nextUsedPageBlockNumber);
    MarkPageDirty(m_metaBlock);

    /* init the new page */
    ControlPage *nextUsedPage = GetPage(nextUsedPageBlockNumber);
    if (unlikely(nextUsedPage == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL,
               ErrMsg("Extend controlgroup(%hhu) failed, nextUsedPageBlockNumber(%u) is invalid.",
                      static_cast<uint8>(m_groupType), nextUsedPageBlockNumber));
        return DSTORE_FAIL;
    }
    nextUsedPage->InitDataPage(controlPageType);
    MarkPageDirty(nextUsedPageBlockNumber);

    return DSTORE_SUCC;
}

ControlGroup::ControlGroup(ControlFileMgr *controlFileMgr, DstoreMemoryContext memCtx, ControlGroupType groupType,
                           BlockNumber metaBlock, PdbId pdbId)
{
    m_controlFileMgr = controlFileMgr;
    m_groupType = groupType;
    m_metaBlock = metaBlock;
    m_maxBlock = DSTORE_INVALID_BLOCK_NUMBER;
    m_metaPage = nullptr;
    m_ctx = memCtx;
    m_isInitialized = false;
    m_lock = nullptr;
    m_pdbId = pdbId;
    m_pageHandle = nullptr;
}

RetStatus ControlGroup::Init(DeployType deployType)
{
    if (m_isInitialized) {
        return DSTORE_SUCC;
    }

    if (unlikely(m_controlFileMgr == nullptr || m_ctx == nullptr || m_groupType >= CONTROL_GROUP_TYPE_MAX ||
                 m_metaBlock == DSTORE_INVALID_BLOCK_NUMBER)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL,
               ErrMsg("Init controlgroup(%hhu) failed, metaBlock(%u).", static_cast<uint8>(m_groupType),
                      m_metaBlock));
        StorageAssert(0);
        return DSTORE_FAIL;
    }

    if (deployType == DeployType::CONTROL_FILE_SINGLE_NODE) {
#ifdef UT
        m_lock =
            DstoreNew(m_ctx) PThreadRWLock(m_pdbId, m_controlFileMgr->UtGetFile1Path(), static_cast<int>(m_groupType));
#else
        m_lock = DstoreNew(m_ctx) PThreadRWLock(m_pdbId);
#endif
    }

    if (STORAGE_VAR_NULL(m_lock)) {
        storage_set_error(CONTROL_ERROR_MEMORY_NOT_ENOUGH);
        ErrLog(DSTORE_ERROR, MODULE_CONTROL,
               ErrMsg("Init controlgroup(%hhu) lock failed, metaBlock(%d).",
                      static_cast<uint8>(m_groupType), m_metaBlock));
        StorageAssert(0);
        return DSTORE_FAIL;
    }

    m_pageHandle = static_cast<PageHandle *>(DstoreMemoryContextAllocZero(m_ctx, sizeof(PageHandle)));
    if (unlikely(m_pageHandle == nullptr)) {
        DstorePfreeExt(m_lock);
        storage_set_error(CONTROL_ERROR_MEMORY_NOT_ENOUGH);
        ErrLog(DSTORE_ERROR, MODULE_CONTROL,
               ErrMsg("Init controlgroup(%hhu) m_pageHandle failed, metaBlock(%d).",
                      static_cast<uint8>(m_groupType), m_metaBlock));
        StorageAssert(0);
        return DSTORE_FAIL;
    }
    m_pageHandle->groupType = m_groupType;
    return DSTORE_SUCC;
}

BlockNumber ControlGroup::GetMaxPageNum()
{
    if (unlikely(!m_isInitialized)) {
        StorageAssert(0);
        return DSTORE_INVALID_BLOCK_NUMBER;
    }
    if (unlikely(m_maxBlock != DSTORE_INVALID_BLOCK_NUMBER)) {
        return m_maxBlock;
    }

    ControlMetaPage *metaPage = GetMetaPage();
    if (unlikely(metaPage == nullptr)) {
        StorageAssert(0);
        return DSTORE_INVALID_BLOCK_NUMBER;
    }
    ControlPageRange *pageRange = static_cast<ControlPageRange *>(&metaPage->GetMetaHeader()->m_pageRange[0]);
    BlockNumber maxBlock = pageRange[0].m_end;
    
    for (uint32 i = 1; i < CONTROL_MAX_PAGERANGE_NUM; i++) {
        if (pageRange[i].m_end == DSTORE_INVALID_BLOCK_NUMBER) {
            break;
        }
        if (maxBlock < pageRange[i].m_end) {
            maxBlock = pageRange[i].m_end;
        }
    }
    m_maxBlock = maxBlock;
    return m_maxBlock;
}

RetStatus ControlGroup::InsertIntoAvailablePage(const char *dataItem, uint32 dataLen, ControlPageType controlPageType)
{
    ControlMetaPage *metaPage = GetMetaPage();
    if (unlikely(metaPage == nullptr)) {
        return DSTORE_FAIL;
    }
    BlockNumber curIndex = metaPage->GetMetaHeader()->m_lastPageId;
    BlockNumber maxPage = GetMaxPageNum();
    while (curIndex != DSTORE_INVALID_BLOCK_NUMBER && curIndex < maxPage) {
        ControlPage *controlPage = static_cast<ControlPage *>(static_cast<void *>(GetPage(curIndex)));
        if (unlikely(controlPage == nullptr)) {
            return DSTORE_FAIL;
        }
        if (unlikely(controlPage->GetPageType() != controlPageType)) {
            curIndex = controlPage->GetNextPage();
            if (curIndex != DSTORE_INVALID_BLOCK_NUMBER) {
                SetLastPageBlockNumber(curIndex);
            }
        }
        if ((controlPage->GetAvailableSize() >= dataLen)) {
            if (STORAGE_FUNC_FAIL(controlPage->AddItem(dataItem, dataLen))) {
                break;
            }
            /* mark page(blockNumber) as dirty */
            MarkPageDirty(curIndex);
            SetLastPageBlockNumber(curIndex);
            return DSTORE_SUCC;
        } else {
            curIndex = controlPage->GetNextPage();
            if (curIndex != DSTORE_INVALID_BLOCK_NUMBER) {
                SetLastPageBlockNumber(curIndex);
            }
        }
    }
    return DSTORE_FAIL;
}

RetStatus ControlGroup::AddOneItem(const char *dataItem, uint32 dataLen, ControlPageType controlPageType)
{
    /* step1: Find an available page from the current lastPage->(header->m_lastPageId)
        * and write Item. */
    if (InsertIntoAvailablePage(dataItem, dataLen, controlPageType) == DSTORE_SUCC) {
        return DSTORE_SUCC;
    }

    /* step2: try extend new page and insert item */
    if (ExtendNewPage(controlPageType) == DSTORE_SUCC) {
        if (InsertIntoAvailablePage(dataItem, dataLen, controlPageType) == DSTORE_SUCC) {
            return DSTORE_SUCC;
        }
    }

    return DSTORE_FAIL;
}

RetStatus ControlGroup::AddItemToControlFile(const char *dataItem, uint32 dataLen, ControlPageType controlPageType)
{
    StorageAssert(m_isInitialized);
    /* can not process item which the length is not fixed */
    StorageAssert(controlPageType != CONTROL_WAL_STREAM_DATAPAGE_TYPE);

    /* acquire exclusive file lock, make sure the file will not extend or change by others */
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_EXCLUSIVE))) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Acquire control file lock fail."));
        return DSTORE_FAIL;
    }
    ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_EXCLUSIVE};    // Critical-Point

    /* Step 1: Update the ControlFileMetaPage, (checksum, flag)=(0,0); and flush pageBuffer
     * Step 2: Find the last Page and put the new DataItem after hwm;
     * Step 3: Update the ControlPageHeader and flush pageBuffer
     */
    if (STORAGE_FUNC_FAIL(LoadGroup())) {
        return DSTORE_FAIL;
    }
    if (STORAGE_FUNC_FAIL(AddOneItem(dataItem, dataLen, controlPageType))) {
        return DSTORE_FAIL;
    }
    if (STORAGE_FUNC_FAIL(PostGroup())) {
        return DSTORE_FAIL;
    }

    return DSTORE_SUCC;
}

void *ControlGroup::GetMetaData()
{
    ControlMetaPage *metaPage = GetMetaPage();
    if (unlikely(metaPage == nullptr)) {
        return nullptr;
    }
    return metaPage->GetMetaData();
}

RetStatus ControlGroup::WriteMetaData(void *metadata, uint32 size)
{
    ControlMetaPage *metaPage = GetMetaPage();
    if (unlikely(metaPage == nullptr)) {
        return DSTORE_FAIL;
    }

    return metaPage->SetMetaData(metadata, size);
}

ControlMetaPage *ControlGroup::GetMetaPage()
{
    if (m_metaPage != nullptr) {
        return m_metaPage;
    }
    ControlMetaPage *metaPage = static_cast<ControlMetaPage *>(static_cast<void *>(
                                                            m_controlFileMgr->ReadOnePage(m_pageHandle, m_metaBlock)));
    if (unlikely(metaPage == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("group(%hhu) get metapage fail.", GetGroupType()));
    }
    m_metaPage = metaPage;

    return metaPage;
}

ControlDataPage *ControlGroup::GetPage(BlockNumber blockNum)
{
    ControlDataPage *page =
        static_cast<ControlDataPage *>(static_cast<void *>(m_controlFileMgr->ReadOnePage(m_pageHandle, blockNum)));
    if (unlikely(page == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("group(%hhu) get page(%u) fail.", GetGroupType(), blockNum));
    }
    return page;
}

void ControlGroup::Destroy()
{
    if (m_pageHandle != nullptr) {
        m_pageHandle->Destroy();
        DstorePfreeExt(m_pageHandle);
        m_pageHandle = nullptr;
    }
    if (m_lock != nullptr) {
        delete m_lock;
        m_lock = nullptr;
    }
}

RetStatus ControlGroup::CheckCrcAndRecovery()
{
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_EXCLUSIVE))) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Acquire control file lock fail."));
        
        return DSTORE_FAIL;
    }

    ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_EXCLUSIVE};
    return m_controlFileMgr->CheckCrcAndRecoveryForGroup(m_metaBlock, m_groupType);
}

}  // namespace DSTORE