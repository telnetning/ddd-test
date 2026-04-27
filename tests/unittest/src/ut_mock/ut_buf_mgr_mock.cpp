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
#include "ut_mock/ut_buf_mgr_mock.h"
#include "common/log/dstore_log.h"
#include "common/memory/dstore_mctx.h"
#include "framework/dstore_instance.h"
#include "lock/dstore_lwlock.h"
#include "transaction/dstore_transaction_types.h"
#include "buffer/dstore_bg_page_writer_mgr.h"
#include "securec.h"

namespace DSTORE {

static void mock_pin(BufferDesc *buffer)
{
    buffer->Pin();
}

static void mock_unpin(BufferDesc *buffer)
{
    buffer->Unpin();
}

static uint32 mock_GetRefcount(BufferDesc *buffer)
{
    return buffer->GetRefcount();
}

BufferDesc *mock_alloc_base_buffer(const BufferTag &bufTag)
{
    BufBlock block = (BufBlock)DstorePallocAligned(BLCKSZ, ALIGNOF_BUFFER);
    errno_t rc = memset_s(block, BLCKSZ, 0, BLCKSZ);
    storage_securec_check(rc, "\0", "\0");
    BufferDescController *controller = new BufferDescController{};
    controller->InitController();
    BufferDesc *buffer = (BufferDesc *)DstoreMemoryContextAllocHugeSize(
        g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_BUFFER), sizeof(BufferDesc));
    buffer->InitBufferDesc(block, controller);
    buffer->bufTag = bufTag;
    buffer->state = (Buffer::BUF_TAG_VALID | Buffer::BUF_VALID);
    buffer->ResetAsBaseBuffer();
    return buffer;
}

BufferDesc *mock_alloc_cr(const BufferTag &bufTag)
{
    BufBlock block = (BufBlock)DstorePallocAligned(BLCKSZ, ALIGNOF_BUFFER);
    errno_t rc = memset_s(block, BLCKSZ, 0, BLCKSZ);
    storage_securec_check(rc, "\0", "\0");
    BufferDescController *controller = new BufferDescController{};
    controller->InitController();
    BufferDesc *cr_buffer = (BufferDesc *)DstoreMemoryContextAllocHugeSize(
        g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_BUFFER), sizeof(BufferDesc));
    cr_buffer->InitBufferDesc(block, controller);
    cr_buffer->bufTag = bufTag;
    cr_buffer->state = Buffer::BUF_CR_PAGE;
    cr_buffer->ResetAsCrBuffer();
    return cr_buffer;
}

void mock_destroy(BufferDesc *buffer)
{
    DstorePfreeAligned(buffer->bufBlock);
    delete buffer->controller;
    buffer->controller = nullptr;
    delete buffer;
    buffer = nullptr;
}


RetStatus MockBufMgr::Init()
{}

void MockBufMgr::Destroy()
{}

BufferDesc *MockBufMgr::Read(const PdbId &pdbId, const PageId &pageId, UNUSE_PARAM BufferPoolReadFlag flag,
                             UNUSE_PARAM BufferRing bufRing /* = nullptr */)
{
    BufferDesc *desc = INVALID_BUFFER_DESC;
    if (pageId == INVALID_PAGE_ID) {
        return INVALID_BUFFER_DESC;
    }
    m_lock.lock();
    BufferTag bufTag{pdbId, pageId};
    auto ret = m_bufferMgr.find(bufTag);
    if (ret == m_bufferMgr.end()) {
        desc = mock_alloc_base_buffer(bufTag);
        StorageAssert(desc != nullptr);
        m_bufferMgr[bufTag] = desc;
    } else {
        desc = ret->second;
    }
    mock_pin(desc);
    m_lock.unlock();
    return desc;
}

BufferDesc *MockBufMgr::Read(const PdbId &pdbId, const PageId &pageId, LWLockMode mode, BufferPoolReadFlag flag,
                             BufferRing bufRing /* = nullptr */)
{
    BufferDesc *desc = Read(pdbId, pageId, flag, bufRing);
    if (desc == INVALID_BUFFER_DESC) {
        return INVALID_BUFFER_DESC;
    }
    LockContent(desc, mode);
    return desc;
}

RetStatus MockBufMgr::BatchCreateNewPage(const PdbId &pdbId,
                                         PageId *pageId, uint64 pageCount,
                                         BufferDesc **newBuffer,
                                         BufferRing bufRing /* = nullptr */)
{
    /* NOT IMPLEMENT! */
    return DSTORE_FAIL;
}

void MockBufMgr::Release(BufferDesc *buf)
{
    mock_unpin(buf);
}

RetStatus MockBufMgr::MarkDirty(__attribute__((__unused__)) BufferDesc *buf, UNUSE_PARAM bool needUpdateRecoveryPlsn)
{
    uint64 oldState = buf->LockHdr();
    uint64 state = oldState | (Buffer::BUF_CONTENT_DIRTY | Buffer::BUF_HINT_DIRTY);
    buf->UnlockHdr(state);
    return DSTORE_SUCC;
}

RetStatus MockBufMgr::Flush(UNUSE_PARAM BufferTag &bufTag, UNUSE_PARAM void* aioCtx)
{
    return DSTORE_SUCC;
}

RetStatus MockBufMgr::Invalidate(BufferDesc *buf)
{
    std::lock_guard<std::mutex> guard(m_lock);

    auto ret = m_bufferMgr.find(buf->bufTag);
    if (ret == m_bufferMgr.end()) {
        StorageAssert(false);  // no conresponding base page
    }

    if (buf->IsCrValid()) {
        BufferDesc *base_buf = buf->GetCrBaseBuffer();
        mock_destroy(buf);
        base_buf->AcquireCrAssignLwlock(LW_EXCLUSIVE);
        base_buf->ResetAsBaseBuffer();
        base_buf->ReleaseCrAssignLwlock();
    } else {
        BufferDesc *base_buf = buf;
        BufferDesc *crBufDesc = base_buf->GetCrBuffer();
        if (crBufDesc != INVALID_BUFFER_DESC) {
            mock_destroy(crBufDesc);
        }
        m_bufferMgr.erase(buf->bufTag);
        mock_destroy(buf);
    }

    return DSTORE_SUCC;
}

RetStatus MockBufMgr::InvalidateByBufTag(BufferTag bufTag, bool needFlush)
{
    (void)bufTag;
    (void)needFlush;
    return DSTORE_SUCC;
}

RetStatus MockBufMgr::InvalidateBaseBuffer(UNUSE_PARAM BufferDesc *bufferDesc, UNUSE_PARAM BufferTag bufTag,
    UNUSE_PARAM bool needFlush)
{
    return DSTORE_SUCC;
}

RetStatus MockBufMgr::InvalidateUsingGivenPdbId(PdbId pdbId)
{
    (void)pdbId;
    return DSTORE_SUCC;
}

RetStatus MockBufMgr::InvalidateUsingGivenFileId(PdbId pdbId, FileId fileId)
{
    (void)pdbId;
    (void)fileId;
    return DSTORE_SUCC;
}

char *MockBufMgr::GetClusterBufferInfo(PdbId pdbId, FileId fileId, BlockNumber blockId)
{
    (void)pdbId;
    (void)fileId;
    (void)blockId;
    return nullptr;
}

RetStatus MockBufMgr::LockContent(BufferDesc *buffer, LWLockMode mode)
{
    uint64 state = buffer->GetState(false);
    if (state & Buffer::BUF_CR_PAGE) {
        return DSTORE_SUCC;
    }
    DstoreLWLockAcquireByMode(&buffer->contentLwLock, mode);
    if (mode == LW_EXCLUSIVE) {
        buffer->WaitIfIsWritingWal();
    }
    return DSTORE_SUCC;
}

bool MockBufMgr::TryLockContent(BufferDesc *buffer, LWLockMode mode)
{
    uint64 state = buffer->GetState(false);
    if (state & Buffer::BUF_CR_PAGE) {
        return false;
    }
    StorageAssert(buffer->IsPinnedPrivately());
    if (buffer->IsContentLocked(LW_SHARED) && (mode == LW_SHARED)) {
        ErrLog(DSTORE_WARNING, MODULE_BUFMGR, ErrMsg("buffer {%u, %u, %u} is locked multiple times "
                                                              "in share mode.", buffer->GetPdbId(),
                                                              buffer->GetPageId().m_fileId,
                                                              buffer->GetPageId().m_blockId));
    }
    bool bSuc = DstoreLWLockConditionalAcquire(&buffer->contentLwLock, mode);
    if (bSuc && mode == LW_EXCLUSIVE) {
        buffer->WaitIfIsWritingWal();
    }
    return bSuc;
}

void MockBufMgr::UnlockContent(BufferDesc *buffer, BufferPoolUnlockContentFlag flag)
{
    StorageAssert(buffer->IsPinnedPrivately());
    /*
     * We ignore the CR buffer here.
     * Because the buffer is pinned by thread locally, no one can change the BUF_CR_PAGE flag,
     * so we get the snapshot of buffer state, and check the BUF_CR_PAGE flag directly, don't care about BUF_LOCKED.
     */
    uint64 state = buffer->GetState(false);
    if (state & Buffer::BUF_CR_PAGE) {
        return;
    }
    if (flag.IsCheckCrc() && buffer->IsContentLocked(LW_EXCLUSIVE)) {
        StorageAssert(state & Buffer::BUF_CONTENT_DIRTY);
    }
    LWLockRelease(&buffer->contentLwLock);
}

void MockBufMgr::PinAndLock(BufferDesc *buffer, LWLockMode mode)
{
    mock_pin(buffer);
    LockContent(buffer, mode);
}

void MockBufMgr::UnlockAndRelease(BufferDesc *buffer, BufferPoolUnlockContentFlag flag)
{
    UnlockContent(buffer, flag);
    Release(buffer);
}

RetStatus MockBufMgr::GetPageDirectoryInfo(Size *length, char **errInfo, PageDirectoryInfo **pageDirectoryArr)
{
    (void)length;
    (void)errInfo;
    (void)pageDirectoryArr;
    return DSTORE_SUCC;;
}

RetStatus MockBufMgr::GetPDBucketInfo(Size *length, char ***chashBucketInfo, uint32 startBucket, uint32 endBucket)
{
    (void)length;
    (void)chashBucketInfo;
    (void)startBucket;
    (void)endBucket;
    return DSTORE_SUCC;
}

uint8 MockBufMgr::GetBufDescResponseType(UNUSE_PARAM BufferDesc *bufferDesc)
{
    return 0;
}

RetStatus MockBufMgr::GetBufDescPrintInfo(Size *length, char **errInfo, BufDescPrintInfo **bufferDescArr)
{
    (void)length;
    (void)errInfo;
    (void)bufferDescArr;
    return DSTORE_SUCC;
}

RetStatus MockBufMgr::DoWhenBufferpoolResize(Size bufferPoolNewSize, StringInfoData &outputMessage)
{
    /* no op, but need to do the following to keep the compiler quiet
     * or the compiler complains about unused parameters */
    (void)bufferPoolNewSize;
    (void)outputMessage;
}

char *MockBufMgr::GetPdBucketLockInfo()
{
    return nullptr;
}

MockBufMgr::~MockBufMgr()
{
    std::lock_guard<std::mutex> guardUnevict(m_unevictable_lock);
    auto unevictIter = m_unevictableBufMgr.begin();
    while (unevictIter != m_unevictableBufMgr.end()) {
        m_unevictableBufMgr.erase(unevictIter++);
    }
    std::lock_guard<std::mutex> guard(m_lock);
    auto iter = m_bufferMgr.begin();
    while (iter != m_bufferMgr.end()) {
        BufferDesc *base_buf = iter->second;
        base_buf->AcquireCrAssignLwlock(LW_SHARED);
        BufferDesc *crBuffer = base_buf->GetCrBuffer();
        if (crBuffer != INVALID_BUFFER_DESC) {
            mock_destroy(crBuffer);
        }
        base_buf->ReleaseCrAssignLwlock();
        mock_destroy(base_buf);
        m_bufferMgr.erase(iter++);
    }
}

#ifdef DSTORE_USE_ASSERT_CHECKING
void MockBufMgr::AssertHasHoldBufLock(__attribute__ ((unused)) const PdbId pdbId,
                                      __attribute__ ((unused))const PageId pageId,
                                      __attribute__ ((unused))LWLockMode lockMode)
{
#ifndef UT
    BufferDesc *buffer = INVALID_BUFFER_DESC;
    m_lock.lock();
    BufferTag bufTag{pdbId, pageId};
    auto ret = m_bufferMgr.find(bufTag);
    if (ret == m_bufferMgr.end()) {
        StorageAssert(0);
    } else {
        buffer = ret->second;
    }
    mock_pin(buffer);
    m_lock.unlock();
    StorageAssert(buffer->IsContentLocked(lockMode));
    release(buffer);
    StorageAssert(buffer->IsPinnedPrivately());
#endif
}
#endif

};  // namespace DSTORE
