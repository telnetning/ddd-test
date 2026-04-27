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

#ifndef DSTORE_BUF_MGR_TEMPORARY_H
#define DSTORE_BUF_MGR_TEMPORARY_H

#include "buffer/dstore_buf_mgr.h"

namespace DSTORE {

struct InvalidBufferNode {
    BufferDesc *bufferDesc;
    slist_node node;
};

class TmpLocalBufMgr final : public BufMgrInterface {
public:
    explicit TmpLocalBufMgr(int32 bufNums);
    ~TmpLocalBufMgr() override = default;
    RetStatus Init() override;
    void Destroy() override;

    BufferDesc *Read(const PdbId &pdbId, const PageId &pageId, LWLockMode mode,
                     BufferPoolReadFlag flag = BufferPoolReadFlag(), BufferRing bufRing = nullptr) override;

    BufferDesc *ReadCr(BufferDesc *baseBufferDesc, Snapshot snapshot) override;
    BufferDesc *ReadOrAllocCr(BufferDesc *baseBufDesc, uint64 lastPageModifyTime,
                              BufferRing bufRing = nullptr) override;
    RetStatus ConsistentRead(ConsistentReadContext &crContext, BufferRing bufRing = nullptr) override;

    RetStatus BatchCreateNewPage(const PdbId &pdbId, PageId *pageId, uint64 pageCount, BufferDesc **newBuffer,
                                 BufferRing bufRing = nullptr) override;

    void Release(BufferDesc *bufferDesc) override;

    RetStatus MarkDirty(BufferDesc *bufferDesc, bool needUpdateRecoveryPlsn = true) override;

    RetStatus Flush(BufferTag &bufTag, void* aioCtx = nullptr) override;
    RetStatus FlushAll(bool isBootstrap, bool onlyOwnedByMe = false, PdbId pdbId = g_defaultPdbId) override;
    RetStatus TryFlush(BufferDesc *bufferDesc) override;

    RetStatus Invalidate(BufferDesc *bufferDesc) override;
    RetStatus InvalidateByBufTag(BufferTag bufTag, bool needFlush) override;
    RetStatus InvalidateBaseBuffer(BufferDesc *bufferDesc, BufferTag bufTag, bool needFlush) override;

    char *GetClusterBufferInfo(PdbId pdbId, FileId fileId, BlockNumber blockId) override;

    RetStatus LockContent(BufferDesc *bufferDesc, LWLockMode mode) override;
    bool TryLockContent(BufferDesc *bufferDesc, LWLockMode mode) override;
    void PinAndLock(BufferDesc *bufferDesc, LWLockMode mode) override;

    RetStatus InvalidateUsingGivenPdbId(PdbId pdbId) override;

    RetStatus InvalidateUsingGivenFileId(PdbId pdbId, FileId fileId) override;

    void UnlockContent(BufferDesc *bufferDesc,
        BufferPoolUnlockContentFlag flag = BufferPoolUnlockContentFlag()) override;
    void UnlockAndRelease(BufferDesc *bufferDesc,
        BufferPoolUnlockContentFlag flag = BufferPoolUnlockContentFlag()) override;

    void FinishCrBuild(BufferDesc *crBufferDesc, CommitSeqNo pageMaxCsn) override;

    BufferDesc *RecoveryRead(const PdbId &pdbId, const PageId &pageId) override;

    RetStatus GetPageDirectoryInfo(Size *length, char **errInfo, PageDirectoryInfo **pageDirectoryArr) override;

    RetStatus GetPDBucketInfo(Size *length, char ***chashBucketInfo, uint32 startBucket, uint32 endBucket) override;

    RetStatus GetBufDescPrintInfo(Size *length, char **errInfo, BufDescPrintInfo **bufferDescArr) override;

    uint8 GetBufDescResponseType(BufferDesc *bufferDesc) override;

    RetStatus DoWhenBufferpoolResize(Size bufferPoolNewSize, StringInfoData &outputMessage) override;

    char *GetPdBucketLockInfo() override;

#ifdef DSTORE_USE_ASSERT_CHECKING
    void AssertHasHoldBufLock(const PdbId pdbId, const PageId pageId, LWLockMode lockMode) override;
#endif

private:
    BufferDesc *GetAvailableBuffer();
    RetStatus ReadBlock(BufferDesc *bufferDesc);
    RetStatus WriteBlock(BufferDesc *bufferDesc);
    void InsertInvalidBufferList(BufferDesc *bufDesc);
    void GetFromInvalidBufferList(BufferDesc **outBufDesc);

    DstoreMemoryContext m_memoryContext;
    HTAB *m_bufHash;
    BufferDesc **m_buffers;
    int32 m_bufNums;
    int32 m_nextBufIdx;
    bool m_initialized;
    static constexpr int32 INVALID_BUF_IDX = -1;
    slist_head m_invalidBuffList;
};

}; /* namespace DSTORE */

#endif
