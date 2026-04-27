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
 * dstore_transaction_slot.h
 *
 * IDENTIFICATION
 *        include/undo/dstore_transaction_slot.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_TRANSACTION_SLOT_H
#define DSTORE_TRANSACTION_SLOT_H

#include "common/algorithm/dstore_string_info.h"
#include "dstore_undo_types.h"

namespace DSTORE {

constexpr int64 INVALID_TXN_SLOT_ID = -1; /* txn slotId is InValid */

enum TailUndoPtrStatus : uint8 {
    UNKNOWN_STATUS = 0,
    NO_VALID_TAIL_UNDO_PTR,  // No valid undo ptr in usp.
    NEED_FETCH_FROM_COMMITED_SLOT, // Need fetch undo tail ptr from commited slot but not recycled
    VALID_STATUS
};

enum TrxSlotStatus : uint8 {
    TXN_STATUS_UNKNOWN = 0,
    TXN_STATUS_FROZEN,
    TXN_STATUS_IN_PROGRESS,
    TXN_STATUS_PENDING_COMMIT, /* During transaction commit, fisrt we need set transaction is in pending commit and
                                * then get next csn, commit in transaction page.
                                */
    TXN_STATUS_COMMITTED,
    TXN_STATUS_ABORTED,
    TXN_STATUS_FAILED,  /* Error detected while interacting with the node */
    TXN_STATUS_PREPARED /* Transaction is prepared (XA) */
};

struct TransactionSlot {
    /* Indicates the current undo tail pointer of a transaction.
     * when insert undo record, refresh curTailUndoPtr
     * when rollback undo record, refresh curTailUndoPtr
     */
    uint64 curTailUndoPtr;
    /* Indicates the tail pointer of the undo space of a transaction.
     * when insert undo record, refresh spaceTailUndoPtr
     */
    uint64 spaceTailUndoPtr;
    CommitSeqNo csn;
    TrxSlotStatus status : 8;
    uint64 reserve : 8;
    uint64 logicSlotId : 48;
    /*
     * record transaction commit wal info for determining whether transaction commit is visible.
     * See details in COMMIT_LOGIC_TAG.
     */
    uint64 commitEndPlsn;
    WalId walId;

    void Init(uint64 tmpLogicSlotId);
    CommitSeqNo GetCsn() const
    {
        return csn;
    }
    void SetCsn(CommitSeqNo tmpCsn)
    {
        csn = tmpCsn;
    }

    void SetWalInfo(uint16 txnCommitWalId, uint64 txnCommitEndPlsn)
    {
        walId = txnCommitWalId;
        commitEndPlsn = txnCommitEndPlsn;
    }

    bool IsCommitWalPersist(PdbId pdbId);

    UndoRecPtr GetCurTailUndoPtr() const
    {
        return static_cast<UndoRecPtr>(curTailUndoPtr);
    }
    void SetCurTailUndoPtr(UndoRecPtr ptr)
    {
        curTailUndoPtr = ptr.m_placeHolder;
    }
    UndoRecPtr GetSpaceTailUndoPtr() const
    {
        return static_cast<UndoRecPtr>(spaceTailUndoPtr);
    }
    void SetSpaceTailUndoPtr(UndoRecPtr ptr)
    {
        spaceTailUndoPtr = ptr.m_placeHolder;
    }

    TrxSlotStatus GetTxnSlotStatus() const
    {
        return status;
    }
    void SetTrxSlotStatus(TrxSlotStatus tmpStatus)
    {
        status = tmpStatus;
    }
    void UpdateRollbackProgress()
    {
        status = TXN_STATUS_FAILED;
    }
    bool NeedRollback() const
    {
        return status != TXN_STATUS_FAILED;
    }
    void Dump(StringInfo str);
} PACKED;
STATIC_ASSERT_TRIVIAL(TransactionSlot);

static_assert(sizeof(TransactionSlot) < UINT16_MAX, "");
constexpr uint16 TRX_SLOT_SIZE = static_cast<uint16>(sizeof(TransactionSlot));
}

#endif  /* STORAGE_TRANSACTION_SLOT_H */
