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
 * dstore_btree_insert.h
 *
 * IDENTIFICATION
 *        dstore/include/index/dstore_btree_insert.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_GAUSSKERNEL_INCLUDE_INDEX_DSTORE_BTREE_INSERT_H
#define SRC_GAUSSKERNEL_INCLUDE_INDEX_DSTORE_BTREE_INSERT_H

#include "index/dstore_btree_split.h"
namespace DSTORE {

class BtreeInsert : public BtreeSplit {
public:
    BtreeInsert(StorageRelation indexRel, IndexInfo *indexInfo, ScanKey scanKey, bool needDefferCheck = false);
    BtreeInsert() = delete;
    ~BtreeInsert() override;

    virtual RetStatus InsertTuple(Datum *values, bool *isnull, ItemPointer heapCtid, bool *satisfiesUnique = nullptr,
                                  CommandId cid = INVALID_CID);
    bool SatisfiesUnique() const
    {
        return m_satisfiedUnique;
    }

protected:
    bool CheckTupleVisibility(BtrPage *btrPage, OffsetNumber checkOff, Xid *waitXid) final;
    RetStatus CheckGPIPartVisible(Oid checkTuplePartOid, bool *isVisible);

    virtual void InitSearchingKeys();
    virtual RetStatus CheckConflict();
    virtual RetStatus CheckUnique(Xid *waitXid);
    virtual RetStatus FindInsertLoc();
    virtual RetStatus AddTupleToLeaf();

    RetStatus SearchBtreeForInsert();
    RetStatus StepRightWhenCheckUnique(BufferDesc **rightBuf, LWLockMode access);
    OffsetNumber BinarySearchOnLeaf(BtrPage *btrPage);

    void UpdateCachedInsertionPageIdIfNeeded(const PageId insertPageId);

    WalRecord *GenerateLeafInsertWal();

    void HandleErrorAndRestorePage(UndoRecord *undoRec);
    void RestorePageWithoutUndoRec();
    void Clear();

    inline void Init() noexcept
    {
        m_insertPageBuf = INVALID_BUFFER_DESC;
        m_insertTuple = nullptr;
        m_insertOff = INVALID_ITEM_OFFSET_NUMBER;
        m_needCheckUnique = m_indexInfo->isUnique;
        m_satisfiedUnique = true;
        m_readyForInsert = false;
        m_useInsertPageCache = true;
        m_prunedPage = false;
        m_duplicateErrLevel = DSTORE_ERROR;
        m_isBoundValid = false;
        m_boundLow = INVALID_ITEM_OFFSET_NUMBER;
        m_boundStrictHigh = INVALID_ITEM_OFFSET_NUMBER;
        m_duplicateTuple = nullptr;
        m_curPageId = INVALID_PAGE_ID;
    }

    BufferDesc *m_insertPageBuf;
    IndexTuple *m_insertTuple;
    OffsetNumber m_insertOff;
    bool m_needCheckUnique;
    bool m_needDefferCheck;
    bool m_satisfiedUnique;
    bool m_readyForInsert;
    bool m_useInsertPageCache;
    bool m_prunedPage;
    ErrLevel m_duplicateErrLevel;

    bool m_isBoundValid;
    OffsetNumber m_boundLow;
    OffsetNumber m_boundStrictHigh;

    IndexTuple *m_duplicateTuple;
    PageId m_curPageId;
};

}

#endif /* SRC_GAUSSKERNEL_INCLUDE_INDEX_STORAGE_BTREE_INSERT_H */
