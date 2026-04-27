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
 * dstore_heap_temp_segment.cpp
 *     This file defines the building blocks that bg page writer needs.
 *
 * IDENTIFICATION
 *        src/tablespace/dstore_heap_temp_segment.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "tablespace/dstore_heap_temp_segment.h"

using namespace DSTORE;

HeapTempSegment::HeapTempSegment(PdbId pdbId, const PageId &segmentId, TablespaceId tablespaceId,
    BufMgrInterface *bufMgr, DstoreMemoryContext ctx)
    : HeapSegment(pdbId, segmentId, tablespaceId, bufMgr, ctx, SegmentType::HEAP_TEMP_SEGMENT_TYPE)
{}

HeapTempSegment::~HeapTempSegment()
{}

RetStatus HeapTempSegment::InitNewDataPageWithFsmIndex(uint16 pageCount, PageId *pageIdList,
    BufferDesc **pageBufList, FsmIndex *fsmIndexList, UNUSE_PARAM bool pagesIsReused)
{
    StorageAssert(pageCount <= PAGES_ADD_TO_FSM_PER_TIME);
    uint16 failCnt = 0;
    RetStatus retStatus = DSTORE_FAIL;

    retStatus = m_bufMgr->BatchCreateNewPage(this->GetPdbId(), pageIdList, pageCount, pageBufList);
    for (uint16 i = 0; i < pageCount; i++) {
        if (pageBufList[i] != INVALID_BUFFER_DESC) {
            Page *newPage = pageBufList[i]->GetPage();
            newPage->SetLsn(0, 0, 0, true);
            m_initDataPageCallback(pageBufList[i], pageBufList[i]->GetPageId(), fsmIndexList[i]);
            (void)m_bufMgr->MarkDirty(pageBufList[i]);
            m_bufMgr->UnlockAndRelease(pageBufList[i]);
        } else {
            failCnt = (failCnt == 0 ? i : failCnt);
            pageBufList[i] = m_bufMgr->Read(this->GetPdbId(), pageIdList[i], LW_EXCLUSIVE,
                                            BufferPoolReadFlag::CreateNewPage());
            if (unlikely(pageBufList[i] == INVALID_BUFFER_DESC)) {
                ErrLog(DSTORE_WARNING, MODULE_BUFMGR,
                    ErrMsg("temp segment:insert bufTag={pdbId=%hhu, pageId={fileId=%hu, blockId=%u}} "
                        "after failing BatchCreateNewPage with curAddPageCount=%hu. batch fail Cnt:%hu %d",
                        this->GetPdbId(), pageIdList[i].m_fileId, pageIdList[i].m_blockId, i, failCnt, retStatus));
                return DSTORE_FAIL;
            }
            Page *newPage = pageBufList[i]->GetPage();
            newPage->SetLsn(0, 0, 0, true);
            m_initDataPageCallback(pageBufList[i], pageBufList[i]->GetPageId(), fsmIndexList[i]);
            (void)m_bufMgr->MarkDirty(pageBufList[i]);
            m_bufMgr->UnlockAndRelease(pageBufList[i]);
        }
    }

    return DSTORE_SUCC;
}

PageId HeapTempSegment::AllocHeapTempSegment(PdbId pdbId, TablespaceId tablespaceId, BufMgrInterface *bufMgr,
                                                       Oid tableOid)
{
    if (unlikely(tablespaceId == INVALID_TABLESPACE_ID || bufMgr == nullptr)) {
        storage_set_error(TBS_ERROR_SEGMENT_PARAMETER_INVALID);
        return INVALID_PAGE_ID;
    }
    PageId segMetaPageId;

    /* Alloc extent from tablespace */
    bool isReuseFlag = false;
    if (STORAGE_FUNC_FAIL(
        AllocExtent(pdbId, tablespaceId, TEMP_TABLE_EXT_SIZE, &segMetaPageId, &isReuseFlag))) {
        if (StorageGetErrorCode() == TBS_ERROR_TABLESPACE_USE_UP) {
            ErrLog(DSTORE_WARNING, MODULE_SEGMENT,
                ErrMsg("Alloc new Heap tempSegment fail, pdb %u tablespace %hu has no space", pdbId, tablespaceId));
        }
        return INVALID_PAGE_ID;
    }

    /* Init segment head page */
    if (STORAGE_FUNC_FAIL(HeapSegment::InitHeapSegMetaInfo(pdbId, bufMgr, SegmentType::HEAP_TEMP_SEGMENT_TYPE,
        segMetaPageId, isReuseFlag))) {
        return INVALID_PAGE_ID;
    }

    /* Construct DataSegment object */
    HeapTempSegment tempSegment(pdbId, segMetaPageId, tablespaceId, bufMgr, g_dstoreCurrentMemoryContext);

    if (STORAGE_FUNC_FAIL(tempSegment.InitSegment())) {
        /* HeapSegment init failed */
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
            ErrMsg("Heap data tempSegment (%hu, %u) initialization failed, oid %u. pdb %u, tablespace %hu",
                segMetaPageId.m_fileId, segMetaPageId.m_blockId, tableOid, pdbId, tablespaceId));
        return INVALID_PAGE_ID;
    }

    ErrLog(DSTORE_LOG, MODULE_SEGMENT,
        ErrMsg("Alloc new Heap tempSegment for oid %u success, SegmentId(%hu, %u) %s reused. pdb %u tablespace %hu",
            tableOid,
            segMetaPageId.m_fileId, segMetaPageId.m_blockId, (isReuseFlag ? "is" : "isn't"), pdbId, tablespaceId));
    return tempSegment.GetSegmentMetaPageId();
}

RetStatus HeapTempSegment::DropSegment()
{
    if (unlikely(!m_isInitialized)) {
        storage_set_error(TBS_ERROR_SEGMENT_IS_NOT_INIT);
        return DSTORE_FAIL;
    }
    if (unlikely(m_isDrop)) {
        storage_set_error(TBS_ERROR_SEGMENT_USE_AFTER_DROP);
        return DSTORE_FAIL;
    }

    if (STORAGE_FUNC_FAIL(DropHeapSegmentInternal())) {
        return DSTORE_FAIL;
    }
    m_isDrop = true;
    return DSTORE_SUCC;
}

