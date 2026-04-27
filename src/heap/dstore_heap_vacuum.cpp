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
 * dstore_heap_vacuum.cpp
 *
 * IDENTIFICATION
 *        src/heap/dstore_heap_vacuum.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
 
#include "common/log/dstore_log.h"
#include "framework/dstore_instance.h"
#include "framework/dstore_thread.h"
#include "common/memory/dstore_mctx.h"
#include "common/dstore_datatype.h"
#include "heap/dstore_heap_prune.h"
#include "heap/dstore_heap_vacuum.h"

namespace DSTORE {

HeapVacuumHandler::HeapVacuumHandler(StorageInstance *instance, ThreadContext *thread,
                                     StorageRelation heapRel, bool isLobOperation)
    : HeapHandler(instance, thread, heapRel, isLobOperation)
{
    m_segScanContext = DstoreNew(m_thrd->GetTransactionMemoryContext()) DataSegmentScanContext(
        m_bufMgr, m_heapRel, m_isLob ? SmgrType::LOB_SMGR : SmgrType::HEAP_SMGR);
}

HeapVacuumHandler::~HeapVacuumHandler()
{
    delete m_segScanContext;
}

RetStatus HeapVacuumHandler::LazyVacuum()
{
    StorageAssert(m_segScanContext != nullptr);
    PageId curPageId = m_segScanContext->GetFirstPageId();
    BufferDesc *bufferDesc;
    while (curPageId != INVALID_PAGE_ID) {
        bufferDesc = m_bufMgr->Read(m_heapRel->m_pdbId, curPageId, LW_EXCLUSIVE);
        if (unlikely(bufferDesc == INVALID_BUFFER_DESC)) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP,
                   ErrMsg("Read page(%hu, %u) failed when lazy vacuum.", curPageId.m_fileId, curPageId.m_blockId));
            return DSTORE_FAIL;
        }
        CheckBufferedPage(bufferDesc->GetPage(), curPageId);

        HeapPruneHandler prune(m_instance, m_heapRel, m_thrd, bufferDesc, m_isLob);
        prune.TryPrunePage(0, true);
        m_bufMgr->UnlockAndRelease(bufferDesc);
        curPageId = m_segScanContext->GetNextPageId();
    }

    return DSTORE_SUCC;
}

}