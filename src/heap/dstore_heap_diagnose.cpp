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
 * dstore_heap_diagnose.cpp
 *
 * IDENTIFICATION
 *        storage/src/heap/dstore_heap_diagnose.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "diagnose/dstore_heap_diagnose.h"
#include "errorcode/dstore_heap_error_code.h"
#include "framework/dstore_instance.h"
#include "heap/dstore_heap_handler.h"
#include "page/dstore_heap_page.h"

namespace DSTORE {

PageFreespace* HeapDiagnose::GetPageFreespace(PdbId pdbId, FileId fileId, BlockNumber blockNumber)
{
    PageId pageId = {fileId, blockNumber};
    if (pageId.IsInvalid() || !StoragePdb::IsValidPdbId(pdbId)) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP,
            ErrMsg("Invalid pageId or pdbId for getting page freespace. pdbid: %u, FileId: %hu, Blkno: %u",
            pdbId, fileId, blockNumber));
        storage_set_error(HEAP_ERROR_GET_PAGE_FREESPACE_INVALID_PAGEID);
        return nullptr;
    }

    BufMgrInterface* bufMgr = g_storageInstance->GetBufferMgr();
    BufferDesc *bufDesc = bufMgr->Read(pdbId, pageId, LW_SHARED);
    if (bufDesc == INVALID_BUFFER_DESC) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("read page(%hu, %u) failed.", pageId.m_fileId, pageId.m_blockId));
        storage_set_error(HEAP_ERROR_GET_BUFFER_FAILED);
        return nullptr;
    }
    Page *page = bufDesc->GetPage();

    if (page->GetType() != PageType::HEAP_PAGE_TYPE) {
        bufMgr->UnlockAndRelease(bufDesc);
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Unsupported page type: %u.", (uint32)page->GetType()));
        storage_set_error(HEAP_ERROR_INPUT_PARAM_WRONG);
        return nullptr;
    }

    uint16 freespace = (static_cast<HeapPage *>(page))->GetFreeSpaceForInsert();
    uint16 spaceline = PartitionFreeSpaceMap::GetListId(freespace);
    bufMgr->UnlockAndRelease(bufDesc);

    PageFreespace *pfs = static_cast<PageFreespace*>(DstorePalloc0(sizeof(PageFreespace)));
    if (pfs == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Allocate memory for page freespace failed."));
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
    } else {
        pfs->SetPageId(pageId);
        pfs->SetFreespace(freespace);
        pfs->SetSpaceline(spaceline);
    }
    return pfs;
}

DiagnoseIterator* HeapDiagnose::GetHeapFreespaceByFsm(StorageRelation storageRel)
{
    return storageRel->tableSmgr->GetSegment()->HeapFsmScan();
}

HeapFreespaceDiagnose::HeapFreespaceDiagnose(BufMgrInterface *bufMgr, PdbId pdbId, uint16 numFsms)
    : m_curFsmTree(0), m_fsmDiags(nullptr), m_cachedFsmPages(nullptr)
{
    m_bufMgr = bufMgr;
    m_pdbId = pdbId;
    m_numFsmTree = numFsms;
}

/* Add a task of one fsmtree to scan handler */
bool HeapFreespaceDiagnose::AddOneFsmTreeTask(PageId fsmMetaPageId)
{
    DstoreMemoryContext mctx = g_dstoreCurrentMemoryContext;
    if (m_fsmDiags == nullptr) {
        m_fsmDiags = static_cast<FreeSpaceMapDiagnose**>(DstorePalloc0(sizeof(FreeSpaceMapDiagnose *) * m_numFsmTree));
        if (m_fsmDiags == nullptr) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Allocate memory for freespace diagnose failed."));
            storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
            return false;
        }
    }
    m_fsmDiags[m_curFsmTree] = DstoreNew(mctx) FreeSpaceMapDiagnose(m_bufMgr, m_pdbId, fsmMetaPageId);
    if (m_fsmDiags[m_curFsmTree] == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Allocate memory for FSM diagnose failed."));
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        return false;
    }
    m_curFsmTree++;
    return true;
}

bool HeapFreespaceDiagnose::Init()
{
    if (unlikely(m_numFsmTree < 1)) {
        return true;
    }

    uint m_maxFsmLevel = 0;
    for (uint i = 0; i < m_numFsmTree; i++) {
        if (!m_fsmDiags[i]->Init()) {
            return false;
        }
        m_maxFsmLevel = DstoreMax(m_maxFsmLevel, m_fsmDiags[i]->GetFsmLevel());
    }

    AutoMemCxtSwitch autoMemCxtSwitch(thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_QUERY));
    m_cachedFsmPages = static_cast<FsmPage*>(DstorePalloc0(sizeof(FsmPage) * (m_maxFsmLevel + 1)));
    if (m_cachedFsmPages == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Allocate memory for page cache failed."));
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
    }
    return (m_cachedFsmPages != nullptr);
}

bool HeapFreespaceDiagnose::Begin()
{
    StorageClearError();
    m_curFsmTree = 0;
    pfs.SetFreespace(0);
    m_fsmDiags[0]->Bind(m_cachedFsmPages, &pfs);
    return m_fsmDiags[0]->Begin();
}

bool HeapFreespaceDiagnose::HasNext()
{
    StorageClearError();
    /* no fsmtree */
    if (unlikely(m_numFsmTree < 1)) {
        return false;
    }

    /* all fsm tree had been processed. */
    if (unlikely(m_curFsmTree >= m_numFsmTree)) {
        return false;
    }

    while (!m_fsmDiags[m_curFsmTree]->HasNext()) {
        if (m_curFsmTree + 1 < m_numFsmTree) {
            /* move to the next fsm tree */
            m_curFsmTree++;
            m_fsmDiags[m_curFsmTree]->Bind(m_cachedFsmPages, &pfs);
            if (!m_fsmDiags[m_curFsmTree]->Begin()) {
                return false;
            }
        } else {
            /* all fsm tree had been processed. */
            return false;
        }
    }
    return true;
}

DiagnoseItem *HeapFreespaceDiagnose::GetNext()
{
    StorageClearError();
    if (likely(m_curFsmTree < m_numFsmTree)) {
        return m_fsmDiags[m_curFsmTree]->GetNext();
    } else {
        return nullptr;
    }
}

void HeapFreespaceDiagnose::End()
{
    if (m_fsmDiags != nullptr) {
        for (uint i = 0; i < m_numFsmTree; i++) {
            if (m_fsmDiags[i] != nullptr) {
                m_fsmDiags[i]->End();
                delete m_fsmDiags[i];
                m_fsmDiags[i] = nullptr;
            }
        }
        DstorePfreeExt(m_fsmDiags);
    }

    if (m_cachedFsmPages != nullptr) {
        DstorePfreeExt(m_cachedFsmPages);
    }
}

}