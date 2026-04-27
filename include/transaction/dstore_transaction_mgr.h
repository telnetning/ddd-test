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
 * This file declares the transaction manager, which handles functions related
 * to a given Xid.
 *
 * IDENTIFICATION
 *        include/transaction/dstore_transaction_mgr.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_TRANSACTION_MGR_H
#define DSTORE_TRANSACTION_MGR_H

#include "transaction/dstore_transaction_types.h"
#include "transaction/dstore_transaction.h"
#include "undo/dstore_undo_mgr.h"
#include "undo/dstore_rollback_trx_task_mgr.h"

namespace DSTORE {

#ifdef UT
#define private public
#endif

static constexpr int64 TRANSACTON_MAX_WAIT_TIME_IN_SEC = 30 * 60; /* We would wait at most 30 min */

class TransactionMgr : public BaseObject {
public:
    explicit TransactionMgr(UndoMgr *undoMgr, CsnMgr *csnMgr, PdbId pdbId);
    RetStatus StartDispatchRollbackTask();
    void StopDispatchRollbackTask();
    virtual ~TransactionMgr() = default;

    PdbId GetPdbId();

    DISALLOW_COPY_AND_MOVE(TransactionMgr);

    /* Return a new transaction object */
    Transaction *GetNewTransaction(ZoneId zid = INVALID_ZONE_ID,
                                   BufPrivateRefCount *bufferPrivateRefCount = nullptr, bool isAutonomous = false,
                                   bool isSpecialAyncCommitAutoTrx = false);

#ifdef UT
    /* Get csn from transaction slot for the given xid */
    CommitSeqNo GetCsnFromXid(Xid xid);
#endif

    /* In given PDB, Wait for all active transactions that potencially have used a snashot whose snapshot csn was equal
     * to less than the targetCsn.
     * This function guarantees that no transaction started before the time that the targetCsn related
     * transaciton committed can still be in-progress. */
    RetStatus WaitEndOfAllActiveTrxsEarlierThanSnapshot(CommitSeqNo targetCsn, TimestampTz startTime = TIMESTAMPTZ_MAX);

    /* Wait for the given xid to end */
    RetStatus WaitForTransactionEnd(Xid xid, bool &txnFailed);

    /* Allocate and Init a new transaction slot for the current transaction */
    RetStatus AllocTransactionSlot(Xid &xid);

    /* Get transaction slot for the specified xid. */
    static RetStatus GetTransactionSlotCopy(PdbId pdbId, Xid xid, TransactionSlot &outTrxSlot);

    /* Commit slot status */
    RetStatus CommitTransactionSlot(Xid xid, bool isSpecialAyncCommitAutoTrx = false);

    /* Roll back the transaction slot */
    RetStatus RollbackTransactionSlot(Xid xid);

    /* Judge tuple csn less than specific csn from undo */
    bool JudgeTupCommitBeforeSpecCsn(TD *td, ItemPointerData ctid, char *data,
                                     CommitSeqNo specCsn, CommitSeqNo *tupleCsn);

    /* Get xid from undo record */
    RetStatus GetXidFromUndo(Xid &xid, TD *td, ItemPointerData ctid);

    /*
     * Before insert undo record in undo zone, we first check if the space of undo zone
     * is enough. If not, extend undo space.
     */
    RetStatus ExtendUndoSpaceIfNeeded(Xid xid, uint32 needSize);

    /* Insert undo record and set undo record ptr; must be wrapped in redo_log atomic area. */
    UndoRecPtr InsertUndoRecord(Xid xid, UndoRecord *undoRec);

    /* Fetch undo record by xid and undo record ptr. */
    RetStatus FetchUndoRecord(Xid xid, UndoRecord *undoRec, UndoRecPtr undoRecPtr);

    /* Fetch undo record by undo record ptr. */
    void FetchUndoRecordInternal(UndoRecord *undoRec, UndoRecPtr undoRecPtr, bool needStrictCheckUndo = true,
                                 bool *isVaild = nullptr);

    /* Fetch undo record matched with ctid that correspoding tuple xid is current transaction. */
    RetStatus FetchUndoRecordByMatchedCtidInCurXact(Xid &xid, UndoRecord *undoRec, UndoRecPtr ptr,
                                                    ItemPointerData ctid);

    /* Fetch undo record matched with ctid. */
    RetStatus FetchUndoRecordByMatchedCtid(Xid &xid, UndoRecord *undoRec, UndoRecPtr ptr, ItemPointerData ctid,
                                           CommitSeqNo *matchedCsn = nullptr);

    /* Get transaction slot last undoptr by xid. */
    RetStatus GetLastUndoPtr(Xid xid, UndoRecPtr &ptr);

    /* Roll back current transaction to a specific undoptr. */
    RetStatus RollbackToUndoptr(Xid xid, UndoRecPtr ptr);

    void AsyncRollback(Xid xid, UndoZone *undoZone);

    bool IsAllTaskFinished();

    RetStatus WaitForOneTransactionEnd(const Xid *xids, uint8 xidNum) const;

    /* Get TAC transaction state associated with the given xid. */
    char GetTacTransactionState(Xid xid) const;

    /* Get transaction slot status */
    TrxSlotStatus GetTxnSlotStatus(Xid xid);

private:
    UndoMgr *m_undoMgr;
    CsnMgr *m_csnMgr;
    RollbackTrxTaskMgr *m_rollbackTrxTaskMgr;
    PdbId m_pdbId;
};

#ifdef UT
#undef private
#endif

}  // namespace DSTORE

#endif
