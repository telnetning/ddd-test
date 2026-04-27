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
 * dstore_btree_recycle_queue.cpp
 *
 * IDENTIFICATION
 *        dstore/src/index/dstore_btree_recycle_queue.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "framework/dstore_instance.h"
#include "index/dstore_btree_recycle_queue.h"

namespace DSTORE {

BufMgrInterface *BtreeRecycleQueue::GetBufMgr()
{
    StorageAssert(bufMgr != nullptr);
    return bufMgr;
}

BufferDesc *BtreeRecycleQueue::ReadPage(const PageId page, LWLockMode access)
{
    if (unlikely(!page.IsValid())) {
        return INVALID_BUFFER_DESC;
    }
    return GetBufMgr()->Read(m_pdbId, page, access);
}

BtrQueuePage *BtreeRecycleQueue::GetBtrQueuePage(BufferDesc *buf)
{
    if (unlikely(buf == INVALID_BUFFER_DESC)) {
        return nullptr;
    }
    return static_cast<BtrQueuePage *>(buf->GetPage());
}

BufferDesc *BtreeRecycleQueue::GetQueueHeadBuf(LWLockMode access)
{
    if (unlikely(!headPageId.IsValid())) {
        return INVALID_BUFFER_DESC;
    }
    return ReadPage(headPageId, access);
}

void BtreeRecycleQueue::AddTailPage(BufferDesc *oldTailBuf, const PageId newTail)
{
    BtrQueuePage *oldTailPage = GetBtrQueuePage(oldTailBuf);
    StorageReleasePanic(oldTailPage == nullptr, MODULE_INDEX, ErrMsg("Get page of oldTailBuf failed"));
    StorageAssert(oldTailPage->IsTail());
    oldTailPage->SetNext(newTail);
    (void)GetBufMgr()->MarkDirty(oldTailBuf);
}

BufferDesc *BtreeRecycleQueue::RemoveTailPage(BufferDesc *buf)
{
    BtrQueuePage *currPage = GetBtrQueuePage(buf);
    if (currPage) {
        BufferDesc *nextBuf = ReadPage(currPage->GetNext(), LW_EXCLUSIVE);
        currPage->SetNext(INVALID_PAGE_ID);
        (void)GetBufMgr()->MarkDirty(buf);
        return nextBuf;
    }
    return INVALID_BUFFER_DESC;
}

void BtreeRecycleQueue::RemovePage(BufferDesc *buf, BufferDesc *nextBuf)
{
    BtrQueuePage *currPage = GetBtrQueuePage(buf);
    BtrQueuePage *nextPage = GetBtrQueuePage(nextBuf);
    if (currPage && nextPage) {
        currPage->SetNext(nextPage->GetNext());
        (void)GetBufMgr()->MarkDirty(buf);
    }
}

BufferDesc *BtreeRecycleQueue::StepNextPage(BufferDesc *currBuf, LWLockMode access)
{
    BtrQueuePage *currPage = GetBtrQueuePage(currBuf);
    if (unlikely(currPage == nullptr)) {
        return INVALID_BUFFER_DESC;
    }
    BufferDesc *nextBuf = ReadPage(currPage->GetNext(), access);
    GetBufMgr()->UnlockAndRelease(currBuf);
    return nextBuf;
}

BufferDesc *BtreeRecycleQueue::GetQueueTailBuf(LWLockMode access, Xid btrCreatedXid)
{
    StorageAssert(headPageId.IsValid());
    LWLockMode currAccess = LW_SHARED;
    BufferDesc *currBuf = ReadPage(headPageId, currAccess);
    while (currBuf != INVALID_BUFFER_DESC) {
        if (unlikely(btrCreatedXid != INVALID_XID) &&
            unlikely(!BtrQueuePage::isQueuePageValid(currBuf, btrCreatedXid))) {
            GetBufMgr()->UnlockAndRelease(currBuf);
            return INVALID_BUFFER_DESC;
        }
        if (GetBtrQueuePage(currBuf)->IsTail()) {
            if (currAccess == access) {
                break;
            }
            GetBufMgr()->UnlockContent(currBuf);
            if (STORAGE_FUNC_FAIL(GetBufMgr()->LockContent(currBuf, access))) {
                ErrLog(DSTORE_ERROR, MODULE_INDEX, ErrMsg("Lock content {%d, %u} failed when execute function %s.",
                    currBuf->GetPageId().m_fileId, currBuf->GetPageId().m_blockId, __FUNCTION__));
                GetBufMgr()->Release(currBuf);
                currBuf = INVALID_BUFFER_DESC;
                return INVALID_BUFFER_DESC;
            }
            currAccess = access;
            continue;
        }
        currAccess = LW_SHARED;
        currBuf = StepNextPage(currBuf, currAccess);
    }
    return currBuf;
}
}