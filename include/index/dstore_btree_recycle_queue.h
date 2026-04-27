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
 * dstore_btree_recycle_queue.h
 *
 * IDENTIFICATION
 *        dstore/include/index/dstore_btree_recycle_queue.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef SRC_GAUSSKERNEL_INCLUDE_PAGE_DSTORE_BTREE_RECYCLE_QUEUE_H
#define SRC_GAUSSKERNEL_INCLUDE_PAGE_DSTORE_BTREE_RECYCLE_QUEUE_H

#include "buffer/dstore_buf_mgr.h"
#include "page/dstore_btr_queue_page.h"

namespace DSTORE {

struct BtreeRecycleQueue : public BaseObject {
    PdbId m_pdbId;
    PageId headPageId;
    BtrRecycleQueueType type;

    BtreeRecycleQueue(PdbId pdbId, const PageId headPage, BtrRecycleQueueType queueType, BufMgrInterface *bufferMgr)
        : m_pdbId(pdbId), headPageId(headPage), type(queueType), bufMgr(bufferMgr)
    {}
    virtual ~BtreeRecycleQueue()
    {
        bufMgr = nullptr;
    }

    template <typename T>
    inline bool IsEmpty()
    {
        BufferDesc *headBuf = bufMgr->Read(m_pdbId, headPageId, LW_SHARED);
        if (unlikely(headBuf == INVALID_BUFFER_DESC)) {
            /* We're not going to handle the bufMgr error here.
             * Just treat it as the queue is empty and skip the reclaim */
            return true;
        }
        BtrQueuePage *headPage = static_cast<BtrQueuePage *>(headBuf->GetPage());
        if (!headPage->IsTail()) {
            bufMgr->UnlockAndRelease(headBuf);
            return false;
        }

        bool isEmpty = headPage->GetQueue<T>()->IsEmpty(m_pdbId);

        bufMgr->UnlockAndRelease(headBuf);
        return isEmpty;
    }

    void AddTailPage(BufferDesc *oldTailBuf, const PageId newTail);

    BufferDesc *RemoveTailPage(BufferDesc *buf);

    void RemovePage(BufferDesc *buf, BufferDesc *nextBuf);

    BufferDesc *GetQueueTailBuf(LWLockMode access, Xid btrCreatedXid);

    BufferDesc *GetQueueHeadBuf(LWLockMode access);

    BufferDesc *StepNextPage(BufferDesc *currBuf, LWLockMode access);

private:
    BufMgrInterface *bufMgr{nullptr};

    BufMgrInterface *GetBufMgr();

    BtrQueuePage *GetBtrQueuePage(BufferDesc *buf);

    BufferDesc *ReadPage(const PageId page, LWLockMode access);
};
}
#endif /* SRC_GAUSSKERNEL_INCLUDE_PAGE_STORAGE_BTREE_RECYCLE_QUEUE_H */
