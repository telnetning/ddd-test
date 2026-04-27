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
 * dstore_bg_page_writer_base.cpp
 *
 * IDENTIFICATION
 *        src/buffer/dstore_bg_page_writer_base.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "buffer/dstore_bg_page_writer_base.h"
#include "buffer/dstore_bg_page_writer_mgr.h"

namespace DSTORE {

BgPageWriterBase::BgPageWriterBase() : m_memContext{nullptr}, m_shutdownRequest{false}, m_isReady{false}
{}

bool BgPageWriterBase::IsReady() const
{
    return m_isReady.load(std::memory_order_acquire);
}

void BgPageWriterBase::SetReady()
{
    m_isReady.store(true, std::memory_order_release);
}

void BgPageWriterBase::Stop()
{
    m_shutdownRequest = true;
}

bool BgPageWriterBase::IsStop() const
{
    return m_shutdownRequest.load(std::memory_order_acquire);
}

void BgPageWriterBase::BgPageWriterExit()
{
    ErrLog(DSTORE_LOG, MODULE_BGPAGEWRITER, ErrMsg("BgPageWriter %lu will exit, shutdown request:%d",
        thrd->GetCore()->pid, m_shutdownRequest.load()));

    (void)DstoreMemoryContextSwitchTo(thrd->m_memoryMgr->GetRoot());
    DstoreMemoryContextDelete(m_memContext);
    m_memContext = nullptr;

    m_isReady = false;
    ErrLog(DSTORE_LOG, MODULE_BGPAGEWRITER, ErrMsg("BgPageWriter %lu exit", thrd->GetCore()->pid));
}

BufferDesc *BgPageWriterBase::FindValidSlotFromDirtyPageQueue(BufferDesc *start, const int64 slotId)
{
    BufferDesc *current = start;
    while (!DirtyPageQueue::IsEnd(current)) {
        if (!current->IsPageDirty()) {
            /* the buffer in the slot has been flushed, skip it */
            current = DirtyPageQueue::GetNext(current, slotId);
            continue;
        }
        /* record the buffer need checkpoint */
        return current;
    }
    /* reach the end of the queue */
    return INVALID_BUFFER_DESC;
}

}