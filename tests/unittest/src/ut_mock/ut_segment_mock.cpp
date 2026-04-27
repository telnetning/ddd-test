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
 */
#include "ut_mock/ut_segment_mock.h"
#include "page/dstore_index_page.h"
#include "page/dstore_heap_page.h"

namespace DSTORE {

RetStatus MockSegment::DropSegment()
{
    if (m_isDrop) {
        return DSTORE_SUCC;
    }
    m_isDrop = true;
    return DSTORE_SUCC;
}

RetStatus MockSegment::Extend(ExtentSize extSize, PageId *extMetaPageId)
{
    if (m_segmentId == INVALID_PAGE_ID || m_isDrop) {
        return DSTORE_FAIL;
    }
    PageId newExt{};
    TablespaceMgr *tablespaceMgr = g_storageInstance->GetPdb(g_defaultPdbId)->GetTablespaceMgr();
    TableSpace *tablespace = tablespaceMgr->OpenTablespace(m_tablespaceId, DSTORE::DSTORE_ACCESS_SHARE_LOCK);
    bool isReusedFlag = false;
    if (STORAGE_FUNC_FAIL(tablespace->AllocExtent(extSize, &newExt, &isReusedFlag))) {
        tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_ACCESS_SHARE_LOCK);
        return DSTORE_FAIL;
    }
    BufferDesc *bufDesc = m_bufMgr->Read(g_defaultPdbId, newExt, LW_EXCLUSIVE);
    auto *page = (SegExtentMetaPage *)bufDesc->GetPage();
    page->m_header.m_myself = newExt;
    page->extentMeta.extSize = extSize;
    page->extentMeta.nextExtMetaPageId = INVALID_PAGE_ID;
    m_bufMgr->UnlockAndRelease(bufDesc);
    *extMetaPageId = m_lastExtPageId = newExt;
    tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_ACCESS_SHARE_LOCK);
    return DSTORE_SUCC;
}

RetStatus MockDataSegment::DropSegment()
{
    if (m_isDrop) {
        return DSTORE_SUCC;
    }
    m_isDrop = true;
    return m_segment->DropSegment();
}

RetStatus MockDataSegment::InitSegment()
{
    return DSTORE_SUCC;
}
PageId MockDataSegment::GetPageFromFsm(uint32 size, uint16 retryTime)
{
    if (m_isDrop) {
        return INVALID_PAGE_ID;
    }

    std::lock_guard<std::mutex> lock(m_fsmMutex);

    for (auto it = m_fsm.begin(); it != m_fsm.end(); it++) {
        if (it->second >= size) {
            return it->first;
        }
    }

    return INVALID_PAGE_ID;
}

PageId MockDataSegment::GetPageFromFsm(uint32 spaceNeeded, uint16 retryTime, uint32 *spaceInFsm)
{
    (void)spaceInFsm;
    if (m_isDrop) {
        return INVALID_PAGE_ID;
    }

    std::lock_guard<std::mutex> lock(m_fsmMutex);

    for (auto it = m_fsm.begin(); it != m_fsm.end(); it++) {
        if (it->second >= spaceNeeded) {
            return it->first;
        }
    }

    return INVALID_PAGE_ID;
}

PageId MockDataSegment::GetNewPage()
{
    if (m_isDrop) {
        return INVALID_PAGE_ID;
    }

    PageId preDataPage = m_curPage;

    if (m_curPage != m_endPage) {
        m_curPage.m_blockId += 1;
    } else {
        /* Need to alloc new extent */
        PageId newExt{};
        if (STORAGE_FUNC_FAIL(m_segment->Extend(EXT_SIZE_8, &newExt))) {
            return INVALID_PAGE_ID;
        }
        m_curPage = newExt;
        m_endPage = CalculateExtEndPage(m_curPage, EXT_SIZE_8);
    }
    PageId newPage = m_curPage;

    BufferDesc *pageBuf = m_mockBufMgr->Read(g_defaultPdbId, newPage, LW_EXCLUSIVE);
    if (m_segType == SegmentType::HEAP_SEGMENT_TYPE) {
        HeapPage::InitHeapPage(pageBuf, newPage, {newPage, 0});
    } else {
        BtrPage *btreePage = (BtrPage*)pageBuf->GetPage();
        btreePage->InitBtrPageInner(newPage);
    }
    auto *dataPage = (HeapPage*)pageBuf->GetPage();
    dataPage->m_header.m_myself = newPage;
    dataPage->GetFsmIndex().page = newPage; /* Record Current PageId to use in UpdateFsm */
    m_mockBufMgr->UnlockAndRelease(pageBuf);

    std::lock_guard<std::mutex> lock(m_fsmMutex);

    if (newPage.m_blockId != INVALID_PAGE_ID.m_blockId) {
        m_fsm.emplace(newPage, BLCKSZ);
    }

    if (m_pageCount == 0) {
        m_firstDataPage = newPage;
    }
    m_pageCount += 1;
    m_pageList.push_back(newPage);
    return newPage;
}

PageId MockDataSegment::GetSegmentMetaPageId()
{
    if (m_isDrop) {
        return INVALID_PAGE_ID;
    }
    return m_segmentHead;
}

RetStatus MockDataSegment::UpdateFsm(const PageId &page_id, uint32 remain_space)
{
    if (m_isDrop) {
        return DSTORE_FAIL;
    }
    std::lock_guard<std::mutex> lock(m_fsmMutex);

    if (m_fsm.count(page_id) <= 0) {
        return DSTORE_FAIL;
    }
    m_fsm[page_id] = remain_space;
    return DSTORE_SUCC;
}

RetStatus MockDataSegment::UpdateFsm(const FsmIndex &fsmIndex, uint32 remainSpace)
{
    return UpdateFsm(fsmIndex.page, remainSpace);
}

RetStatus MockDataSegment::UpdateFsmAndSearch(const FsmIndex &fsmIndex, uint32 remainSpace, uint32 spaceNeeded,
                                              uint16 retryTimes, PageId *pageId, uint32* spaceInfsm)
{
    (void)spaceNeeded;
    (void)pageId;
    (void)retryTimes;
    return UpdateFsm(fsmIndex.page, remainSpace);
}
PageId MockDataSegment::GetFirstDataPage()
{
    return m_pageList[0];
}

PageId MockVfsFile::GetNewPage(UNUSE_PARAM bool isBgExtension)
{
    std::lock_guard<std::mutex> guard(m_mutex);

    if (m_curBlkNum < m_upperBlkNum) {
        PageId id{m_fileId, m_curBlkNum++};
        return id;
    } else {
        return INVALID_PAGE_ID;
    }
}

RetStatus MockVfsFile::Extend(int32 size, PageId *first_page)
{
    std::lock_guard<std::mutex> guard(m_mutex);

    int32 mSize = BLKSIZE_ROUND_UP(size);
    BlockNumber blkNum =  mSize / BLCKSZ;
    if (m_upperBlkNum + blkNum > MAX_BLOCK_NUM_MOCK) {
        return DSTORE_FAIL;
    }

    if (first_page != nullptr) {
        *first_page = {m_fileId, m_undo_upper_blk_num + 1};
    }
    m_undo_upper_blk_num += blkNum;

    m_upperBlkNum += blkNum;

    return DSTORE_SUCC;
}
}
