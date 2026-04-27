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
 * dstore_btree_split.h
 *
 * IDENTIFICATION
 *        dstore/include/index/dstore_btree_split.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_GAUSSKERNEL_INCLUDE_INDEX_DSTORE_BTREE_SPLIT_H
#define SRC_GAUSSKERNEL_INCLUDE_INDEX_DSTORE_BTREE_SPLIT_H

#include "tuple/dstore_index_tuple.h"
#include "index/dstore_btree.h"
#include "page/dstore_index_page.h"

namespace DSTORE {

const uint16 MAX_RETRY_COUNT = 10000;

class BtreeSplit : public Btree {
#if defined UT && !defined private
#define private protected
#endif

public:
    BtreeSplit() = delete;
    ~BtreeSplit() override;

    CommandId m_cid;
    bool m_needRecordUndo;
    bool m_needRetrySearchBtree;
    uint16 m_retryCount;
    BtrStack m_leafStack;

protected:
    BtreeSplit(StorageRelation indexRel, IndexInfo *indexInfo, ScanKey scanKeyInfo, bool keepStack = false);

    virtual RetStatus SplitAndAddDownlink(IndexTuple *insTuple, OffsetNumber insertOff,
                                          BtrStack stack, BufferDesc *childBuf = INVALID_BUFFER_DESC);
    virtual RetStatus SplitPage(IndexTuple *insTuple, OffsetNumber insOff, BufferDesc *childBuf);
    virtual bool CheckTupleVisibility(BtrPage *btrPage, OffsetNumber checkOff, Xid *waitXid);

    RetStatus SearchBtree(BufferDesc **pageBuf, bool strictlyGreaterThanKey, bool forceUpdate = true,
                          bool needWriteLock = true, bool needCheckCreatedXid = false) final;
    RetStatus StepRightIfNeeded(BufferDesc **pageBuf, LWLockMode access, bool strictlyGreaterThanKey,
                                bool needCheckCreatedXid = false) final;
    RetStatus CompleteSplit(BufferDesc *splitBuf, BtrStack stack, LWLockMode access);
    BufferDesc *GetParentBufDesc(const PageId childPageId, BtrStack stack, LWLockMode access);
    RetStatus AddPageDownlinkToParent(BtrStack stack, bool isRoot);
    RetStatus PrepareSplittingAndRightPage(BtrPage *leftPage, IndexTuple *insTuple, SplitContext &splitCxt,
                                           BufferDesc **oldRBuf);
    void UpdateChildSplitStatus(BufferDesc *childBuf);
    void ClearPagesAfterSplit(BufferDesc *childBuf, BufferDesc *oldRBuf, BufferPoolUnlockContentFlag flag);
    void ClearStack();

    RetStatus WaitForTxnEndIfNeeded(const PageId &pageId, Xid xid);
    RetStatus WaitTxnEndForTdRealloc();

    RetStatus FormIndexTuple(Datum *values, bool *isnull, ItemPointer heapCtid, IndexTuple **indexTuple);
    uint8 AllocAndSetTd(BtrPage *page, IndexTuple *indexTuple);
    RetStatus InsertUndoRecAndSetTd(uint8 tdID, OffsetNumber insOff, BtrPage *insPage, UndoRecord *undoRecord);
    RetStatus InsertOnlyWalForSplitPage(IndexTuple *insTuple, SplitContext &splitCxt, BufferDesc *oldRightBuf,
                                        BufferDesc *childBuf, BtrPage *newLeft);
    void GenerateWalPageRollback(BufferDesc *buffer, UndoRecordVector &undoRecVec);
    RetStatus GenerateUndoRecordForInsert(IndexTuple *insTuple, OffsetNumber insOff, BtrPage *insPage,
                                          UndoRecord **undoRecord);

    void HandleErrorWhenGetUndoBuffer(BtrPage *btrPage, UndoRecord *undoRec, IndexTuple *undoTuple);
    void SetErrorAndPutFailedPageBackToFreePageQueue(ErrorCode errCode);

    void inline InitSplittingTarget(BufferDesc *splitBuf, bool needTruncateHikey = true) noexcept
    {
        m_splitBuf = splitBuf;
        m_newRightBuf = INVALID_BUFFER_DESC;
        m_oldRightBuf = INVALID_BUFFER_DESC;
        m_needTruncateHikey = needTruncateHikey;
    }

    TDAllocContext m_tdContext;
    bool m_keepStackAfterSearch;
    bool m_needTruncateHikey;
    BufferDesc *m_splitBuf;
    BufferDesc *m_newRightBuf;
    BufferDesc *m_oldRightBuf;

private:
    uint8 RetryAllocAndSetTdWhenSplit(BtrPage *page, IndexTuple *indexTuple);
    RetStatus AddTupleToInternal(IndexTuple *insTuple, OffsetNumber insertOff, BtrStack stack, BufferDesc *targetBuf,
                                 BufferDesc *childBuf);
    RetStatus AddHikeyToTempSplitLeftPage(BtrPage *leftPage, IndexTuple *insTuple, const SplitContext splitContext,
                                          bool &isSameWithLastLeft);
    IndexTuple *CreateSplittingLeftHikey(BtrPage *splitPage, IndexTuple *insTuple, const SplitContext splitContext,
                                         bool &isSameWithLastLeft);
    RetStatus CreateSplitRightPageWithHikey(BtrPage *tempLeft);
    RetStatus LinkNewRightToOrigRightIfNeeded(BufferDesc **oldRightBuf);

    RetStatus CreateNewInternalRoot();
    BufferDesc *GetParentBufDescFromRoot(const PageId targetPageId, uint32 targetLevel, OffsetNumber &childPos);
    RetStatus UpdateBtrMetaLowestSinglePage(const PageId pageId, uint32 pageLevel);

    /* Wal records for internal INSERT */
    RetStatus InsertNewRootWal(BufferDesc *rootBuf, const PageId origRoot, BufferDesc *btrMetaBuf);

    /* Undo & Wal record for SPLIT */
    RetStatus InsertUndoAndWalForSplitLeaf(IndexTuple *insTuple, SplitContext &splitCxt, OffsetNumber insertOffOnPage,
                                           BufferDesc *oldRightBuf, BtrPage *newLeft);
    WalRecord *GenerateSplitLeafWal(const SplitContext &splitCxt, bool needSetTD, uint8 tdID, BtrPage *newLeft);
    WalRecord *GenerateSplitInternalWal(const SplitContext &splitCxt, BtrPage *newLeft);
    WalRecord *GenerateSplitInsertLeafWal(IndexTuple *insTuple, const SplitContext &splitCxt, BtrPage *newLeft);
    WalRecord *GenerateSplitInsertInternalWal(IndexTuple *insTuple, const SplitContext &splitCxt, BtrPage *newLeft);
    WalRecord *GenerateNewRightWal();
    void InsertOldRightWal(BufferDesc *oldRightBuf);
    void InsertChildStatusWal(BufferDesc *childBuf);

#ifdef UT
#undef private
#endif
};
}

#endif /* SRC_GAUSSKERNEL_INCLUDE_INDEX_STORAGE_BTREE_SPLIT_H */
