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
 * dstore_control_csninfo.cpp
 *
 *
 *
 * IDENTIFICATION
 *        dstore/include/control/dstore_control_csninfo.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "control/dstore_control_csninfo.h"
#include "control/dstore_control_file_lock.h"

namespace DSTORE {
RetStatus ControlCsnInfo::SetMaxReservedCSN(CommitSeqNo csn, CommitSeqNo &newMaxReservedCsn)
{
    StorageAssert(m_isInitialized);
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_EXCLUSIVE))) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Acquire control file lock fail."));
        return DSTORE_FAIL;
    }
    ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_EXCLUSIVE};
    if (unlikely(LoadGroup() == DSTORE_FAIL)) {
        return DSTORE_FAIL;
    }

    /* Update the CSN and flush pageBuffer. */
    ControlCsnPageData *controlCsnPageData =
        static_cast<ControlCsnPageData *>(static_cast<void *>(GetMetaData()));
    if (controlCsnPageData == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("group(%hhu) get metadata fail.", GetGroupType()));
        return DSTORE_FAIL;
    }
#ifndef UT
    if (csn <= controlCsnPageData->m_csn) {
        ErrLog(DSTORE_LOG, MODULE_CONTROL,
            ErrMsg("maxReservedCsn has been pushed to %lu in control file, current csn is %lu, no need to push",
                controlCsnPageData->m_csn, csn));
        newMaxReservedCsn = controlCsnPageData->m_csn;
        return DSTORE_SUCC;
    }
#endif
    controlCsnPageData->m_csn = csn;
    /* mark page(blockNumber) as dirty and write it. */
    MarkPageDirty(CONTROLFILE_PAGEMAP_CSN_META);

    if (STORAGE_FUNC_FAIL(PostGroup())) {
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus ControlCsnInfo::GetMaxReservedCSN(CommitSeqNo &csn)
{
    StorageAssert(m_isInitialized);
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_SHARE))) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Acquire control file lock fail."));
        return DSTORE_FAIL;
    }
    ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_SHARE};
    if (unlikely(LoadGroup() == DSTORE_FAIL)) {
        return DSTORE_FAIL;
    }

    ControlCsnPageData *controlCsnPageData =
        static_cast<ControlCsnPageData *>(static_cast<void *>((GetMetaData())));
    if (controlCsnPageData == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("group(%hhu) get metadata fail.", GetGroupType()));
        return DSTORE_FAIL;
    }
    csn = controlCsnPageData->m_csn;
    return DSTORE_SUCC;
}

RetStatus ControlCsnInfo::SetUndoZoneMapSegmentId(const PageId segmentId)
{
    StorageAssert(m_isInitialized);
    if (STORAGE_FUNC_FAIL(m_lock->Lock(CFLockMode::CF_EXCLUSIVE))) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Acquire control file lock fail."));
        return DSTORE_FAIL;
    }
    ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_EXCLUSIVE};
    if (unlikely(LoadGroup() == DSTORE_FAIL)) {
        return DSTORE_FAIL;
    }

    /* Step 1: Get metadata. */
    ControlCsnPageData *controlCsnPageData = reinterpret_cast<ControlCsnPageData *>(GetMetaData());
    if (unlikely(controlCsnPageData == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("group(%hhu) get metadata fail.", GetGroupType()));
        return DSTORE_FAIL;
    }

    /* Step 2: Write CSN to metadata. */
    controlCsnPageData->m_segmentId = segmentId;

    /* mark page(blockNumber) as dirty and write it. */
    MarkPageDirty(CONTROLFILE_PAGEMAP_CSN_META);

    if (STORAGE_FUNC_FAIL(PostGroup())) {
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus ControlCsnInfo::GetUndoZoneMapSegmentId(PageId &segmentId)
{
    StorageAssert(m_isInitialized);

    /* acquire share file lock, make sure the file will not extend or change by others */
    if (unlikely(m_lock->Lock(CFLockMode::CF_SHARE) == DSTORE_FAIL)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Acquire control file lock fail."));
        return DSTORE_FAIL;
    }
    ControlFileLockAutoUnlock autoUnlock{m_lock, CFLockMode::CF_SHARE};
    if (unlikely(LoadGroup() == DSTORE_FAIL)) {
        return DSTORE_FAIL;
    }
    /* Read metadata */
    ControlCsnPageData *controlCsnPageData = reinterpret_cast<ControlCsnPageData *>(GetMetaData());
    if (unlikely(controlCsnPageData == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("group(%hhu) get metadata fail.", GetGroupType()));
        return DSTORE_FAIL;
    }
    segmentId = controlCsnPageData->m_segmentId;
    return DSTORE_SUCC;
}

RetStatus ControlCsnInfo::Create()
{
    /* step 1, init metapage */
    ControlMetaPage *metaPage = GetMetaPage();
    if (unlikely(metaPage == nullptr)) {
        return DSTORE_FAIL;
    }
    metaPage->InitMetaPage(CONTROL_CSN_METAPAGE_TYPE, sizeof(ControlCsnPageData));
    ControlCsnPageData metaData;
    metaData.m_version = 0;
    metaData.m_csn = INVALID_CSN;
    metaData.m_segmentId = INVALID_PAGE_ID;
    if (unlikely(metaPage->SetMetaData(static_cast<void *>(&metaData), sizeof(ControlCsnPageData)) == DSTORE_FAIL)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Failed to set metadata(grouptype:%hhu).", GetGroupType()));
        return DSTORE_FAIL;
    }
    ControlMetaHeader *metaHeader = metaPage->GetControlMetaHeader();
    metaHeader->m_lastPageId = CONTROLFILE_PAGEMAP_CSN_START;
    ControlPageRange *pageRange = &metaHeader->m_pageRange[0];
    pageRange[0].m_start = CONTROLFILE_PAGEMAP_CSN_START;
    pageRange[0].m_end = CONTROLFILE_PAGEMAP_CSN_MAX;
    for (uint32 i = 1; i < CONTROL_MAX_PAGERANGE_NUM; i++) {
        pageRange[i].m_start = DSTORE_INVALID_BLOCK_NUMBER;
        pageRange[i].m_end = DSTORE_INVALID_BLOCK_NUMBER;
    }
    MarkPageDirty(CONTROLFILE_PAGEMAP_CSN_META);

    /* step 2, init firstpage */
    ControlDataPage *dataPage = GetPage(CONTROLFILE_PAGEMAP_CSN_START);
    if (unlikely(dataPage == nullptr)) {
        return DSTORE_FAIL;
    }
    dataPage->InitDataPage(CONTROL_CSN_DATAPAGE_TYPE);
    MarkPageDirty(CONTROLFILE_PAGEMAP_CSN_START);

    return DSTORE_SUCC;
}
}
