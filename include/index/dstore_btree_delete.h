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
 * dstore_btree_delete.h
 *
 * IDENTIFICATION
 *        dstore/include/index/dstore_btree_delete.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_DSTORE_BTREE_DELETE_H
#define DSTORE_DSTORE_BTREE_DELETE_H

#include "index/dstore_btree_split.h"

namespace DSTORE {

class BtreeDelete : public BtreeSplit {
public:
    BtreeDelete(StorageRelation indexRel, IndexInfo *indexInfo, ScanKey scanKeyInfo);
    ~BtreeDelete() override;

    virtual RetStatus DeleteTuple(Datum *values, bool *isnull, ItemPointer ctid, CommandId cid = INVALID_CID);
    RetStatus DeleteFromInternal(BufferDesc *internalBuf, OffsetNumber offset);

#ifdef UT
    RetStatus DeleteTupleByOffset(BufferDesc *buffer, OffsetNumber offset);
    PageId GetCurPageId() { return m_pagePayload.pageId; }
#endif

    IndexTuple      *m_searchingTarget;    /* Index tuple formed by given values to searching for deleting target */
    IndexTuple      *m_delTuple;           /* Index tuple on page that we going to delete */
    BtreePagePayload m_pagePayload;        /* Payload of page where to-be-deleted tuple is on */
    OffsetNumber     m_delOffset;          /* Offset number of to-be-deleted tuple on page */

    bool m_needRetry;

protected:
    RetStatus DoDelete();
    RetStatus FindDeleteLoc(Xid *waitXid);
    RetStatus DeleteFromLeaf();

    RetStatus StepRightWhenFindDelLoc();
    WalRecord *GenerateWalRecordForLeaf(uint8 tdId);

private:
    void InsertWalRecordForInternal();
    RetStatus GenerateUndoRecord(uint8 currTdId, UndoRecord **undoRecord);
    RetStatus InsertUndoRecord(uint8 currTdId, UndoRecord *undoRecord);
};

}  // namespace DSTORE

#endif  // DSTORE_STORAGE_BTREE_DELETE_H
