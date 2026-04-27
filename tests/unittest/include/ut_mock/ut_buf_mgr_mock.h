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
#ifndef DSTORE_UT_BUF_MGR_MOCK_H
#define DSTORE_UT_BUF_MGR_MOCK_H

#include <buffer/dstore_buf_mgr.h>

#include <mutex>
#include <map>

namespace DSTORE {

struct BufferTagCmp {
    bool operator()(const BufferTag &lhs, const BufferTag &rhs) const
    {
        if (lhs.pdbId < rhs.pdbId) {
            return true;
        } else if (lhs.pdbId > rhs.pdbId) {
            return false;
        } else if (lhs.pageId.m_fileId < rhs.pageId.m_fileId) {
            return true;
        } else if (lhs.pageId.m_fileId > rhs.pageId.m_fileId) {
            return false;
        } else if (lhs.pageId.m_blockId < rhs.pageId.m_blockId) {
            return true;
        } else {
            return false;
        }
    }
};

class MockBufMgr : public BufMgrInterface {
public:
    RetStatus Init() override;
    void Destroy() override;

    BufferDesc *Read(const PdbId &pdbId, const PageId &page_id, LWLockMode mode,
                     BufferPoolReadFlag flag = BufferPoolReadFlag(), BufferRing bufRing = nullptr) override;
    BufferDesc *ReadCr(BufferDesc *baseBufDesc, Snapshot snapshot) override
    {}
    BufferDesc *ReadOrAllocCr(BufferDesc *baseBufDesc, uint64 lastPageModifyTime, BufferRing bufRing = nullptr) override
    {}
    RetStatus ConsistentRead(ConsistentReadContext &crContext, BufferRing bufRing = nullptr) override
    {
        return DSTORE_FAIL;
    }
    RetStatus BatchCreateNewPage(const PdbId &pdbId,
                                 PageId *pageId, uint64 pageCount,
                                 BufferDesc **newBuffer, BufferRing bufRing = nullptr) override;

    void Release(BufferDesc *buffer) override;
    RetStatus MarkDirty(BufferDesc *buffer, bool needUpdateRecoveryPlsn = true) override;

    RetStatus Flush(BufferTag &bufTag, void* aioCtx = nullptr) override;
    RetStatus FlushAll(bool isBootstrap, bool onlyOwnedByMe = false, PdbId pdbId = g_defaultPdbId) override
    {
        return DSTORE_SUCC;
    }
    RetStatus TryFlush(BufferDesc *bufferDesc) override
    {
        return DSTORE_SUCC;
    }
    RetStatus Invalidate(BufferDesc *buffer) override;
    RetStatus InvalidateByBufTag(BufferTag bufTag, bool needFlush) override;
    RetStatus InvalidateBaseBuffer(BufferDesc *bufferDesc, BufferTag bufTag, bool needFlush) override;
    RetStatus InvalidateUsingGivenPdbId(PdbId pdbId) override;
    RetStatus InvalidateUsingGivenFileId(PdbId pdbId, FileId fileId) override;
    char *GetClusterBufferInfo(PdbId pdbId, FileId fileId, BlockNumber blockId) override;

    RetStatus LockContent(BufferDesc *buffer, LWLockMode mode) override;
    bool TryLockContent(BufferDesc *buffer, LWLockMode mode) override;
    /* Release the content_lwlock.
     *
     * NOTE: checkPageIfMarkDirty indicate if it need to check mark dirty flag when unlock buffer
     *       If the checkPageIfMarkDirty is true, then it will trigger assert when unlock the buffer in exclusive mode
     *       and the buffer is not set BUF_CONTENT_DIRTY flag.
     *       This is used to remind callers do not forget call mark_dirty after modify the page.
     */
    void UnlockContent(BufferDesc *buffer, BufferPoolUnlockContentFlag flag = BufferPoolUnlockContentFlag()) override;
    void PinAndLock(BufferDesc *buffer, LWLockMode mode) override;
    /* Unlock and release the buffer. This is just a shorthand for a common combination.
     *
     * NOTE: checkPageIfMarkDirty indicate if it need to check mark dirty flag when unlock buffer
     *       If the checkPageIfMarkDirty is true, then it will trigger assert when unlock the buffer in exclusive mode
     *       and the buffer is not set BUF_CONTENT_DIRTY flag.
     *       This is used to remind callers do not forget call mark_dirty after modify the page.
     */
    void UnlockAndRelease(BufferDesc *buffer, BufferPoolUnlockContentFlag flag = BufferPoolUnlockContentFlag()) override;
    void FinishCrBuild(BufferDesc *cr_buffer_desc, CommitSeqNo pageMaxCsn) override
    {}

    RetStatus DoWhenBufferpoolResize(Size bufferPoolNewSize, StringInfoData &outputMessage) override;

    RetStatus GetPageDirectoryInfo(Size *length, char **errInfo, PageDirectoryInfo **pageDirectoryArr) override;
    RetStatus GetPDBucketInfo(Size *length, char ***chashBucketInfo, uint32 startBucket, uint32 endBucket) override;
    RetStatus GetBufDescPrintInfo(Size *length, char **errInfo, BufDescPrintInfo **bufferDescArr) override;
    uint8 GetBufDescResponseType(BufferDesc *bufferDesc) override;
    char *GetPdBucketLockInfo() override;

    BufferDesc *RecoveryRead(const PdbId &pdbId, const PageId &pageId) override
    {
        return Read(pdbId, pageId, LW_EXCLUSIVE, BufferPoolReadFlag::CreateFlagWithRecoveryRead());
    }

    void* GetPdHashTbl()
    {
        return nullptr;
    }

    ~MockBufMgr() override;

#ifdef DSTORE_USE_ASSERT_CHECKING
    void AssertHasHoldBufLock(const PdbId pdbId, const PageId page_id, LWLockMode lockMode) override;
#endif

private:
    std::mutex m_lock;
    std::mutex m_unevictable_lock;
    std::map<BufferTag, BufferDesc *, BufferTagCmp> m_bufferMgr;
    std::map<BufferTag, BufferDesc *, BufferTagCmp> m_unevictableBufMgr;
    int m_size;
    friend class UtMockModule;
    friend class MockBufferTest;
    explicit MockBufMgr(int size) : m_size(size){}
    BufferDesc *Read(const PdbId &pdbId, const PageId &pageId, BufferPoolReadFlag flag = BufferPoolReadFlag(),
                     BufferRing bufRing = nullptr);
};
}  // namespace DSTORE

#endif
