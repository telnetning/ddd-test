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
 * dstore_transaction_struct.h
 *     This file defines the building blocks that bg page writer needs.
 *
 * IDENTIFICATION
 *        src/gausskernel/dstore/interface/transaction/dstore_transaction_struct.h
 *
 * ---------------------------------------------------------------------------------------
 */
#include "common/dstore_common_utils.h"
#include "tablespace/dstore_tablespace_struct.h"
#include "page/dstore_page_struct.h"

#ifndef DSTORE_TRANSACTION_UTILS_H
#define DSTORE_TRANSACTION_UTILS_H

namespace DSTORE {
enum class TrxEvent {
    TRX_EVENT_START,
    TRX_EVENT_PRE_COMMIT,
    TRX_EVENT_POST_COMMIT,
    TRX_EVENT_PRE_ABORT,
    TRX_EVENT_POST_ABORT,
    TRX_EVENT_PREPARE,
    TRX_EVENT_COMMIT_PREPARED,
    TRX_EVENT_ROLLBACK_PREPARED,
    TRX_EVENT_PREROLLBACK_CLEANUP,
    TRX_EVENT_PRE_STP_TRX_STATEMENT,
    TRX_EVENT_POST_TERMINATE,
};
using TrxCallback = void (*)(TrxEvent event, void *arg);

const CommitSeqNo INVALID_CSN = 0;
const CommitSeqNo COMMITSEQNO_FIRST_NORMAL = 0x1;
const CommitSeqNo MAX_COMMITSEQNO = ~INVALID_CSN;
inline bool DstoreCsnIsValid(CommitSeqNo csn)
{
    return csn != INVALID_CSN;
}

constexpr CommandId FIRST_CID = static_cast<CommandId>(0);
constexpr CommandId INVALID_CID = (~static_cast<CommandId>(0));
enum class SnapshotType {
    /* -------------------------------------------------------------------------
     * Tuple is visible if the tuple is valid in the specified snapshot.
     *
     * Visible:
     * - all transactions committed as of the time of the specified snapshot
     * - previous commands of this transaction
     *
     * Invisible:
     * - transactions shown as in-progress by the snapshot
     * - transactions started after the snapshot was taken
     * - changes made by the current command
     * -------------------------------------------------------------------------
     */
    SNAPSHOT_MVCC = 0,

    /* -------------------------------------------------------------------------
     * A tuple is visible if heap tuple is valid "now".
     *
     * Here, we consider the effects of:
     * - all committed transactions (as of the current instant)
     * - previous commands of this transaction
     *
     * Does _not_ include:
     * - changes made by the current command.
     * -------------------------------------------------------------------------
     */
    SNAPSHOT_NOW,

    /* -------------------------------------------------------------------------
     * A tuple is visible if the tuple is valid including effects of open
     * transactions.
     *
     * Here, we consider the effects of:
     * - all committed and in-progress transactions (as of the current instant)
     * - previous commands of this transaction
     * - changes made by the current command
     *
     * It also includes the effects of other xacts still in progress.
     * -------------------------------------------------------------------------
     */
    SNAPSHOT_DIRTY,
};

struct SnapshotData {
    SnapshotType snapshotType;

    /*
     * In SNAPSHOT_MVCC, this snapshot can see the effects of all transactions
     * whose csn <= snapshotCsn.
     */
    CommitSeqNo snapshotCsn;

    /* in current transaction, tuples whose cid < currentCid are visible */
    CommandId currentCid;

    inline void Init()
    {
        snapshotType = SnapshotType::SNAPSHOT_MVCC;
        snapshotCsn = INVALID_CSN;
        currentCid = FIRST_CID;
    }

    inline void SetSnapshotType(SnapshotType type)
    {
        snapshotType = type;
    }
    inline SnapshotType GetSnapshotType() const
    {
        return snapshotType;
    }
    inline CommitSeqNo GetCsn() const
    {
        return snapshotType == SnapshotType::SNAPSHOT_MVCC ? snapshotCsn : MAX_COMMITSEQNO;
    }
    inline void SetCsn(CommitSeqNo csn)
    {
        snapshotCsn = csn;
    }
    inline CommandId GetCid() const
    {
        return currentCid;
    }
    inline void SetCid(CommandId cid)
    {
        currentCid = cid;
    }
};

using Snapshot = SnapshotData *;

const Snapshot INVALID_SNAPSHOT = nullptr;

/*
 *	transaction block states - transaction state of client queries
 *
 * Note: the subtransaction states are used only for non-topmost
 * transactions; the others appear only in the topmost transaction.
 */
enum class TBlockState {
    /* not-in-transaction-block states */
    TBLOCK_DEFAULT, /* idle */
    TBLOCK_STARTED, /* running single-query transaction */

    /* transaction block states */
    TBLOCK_BEGIN,         /* starting transaction block */
    TBLOCK_INPROGRESS,    /* live transaction */
    TBLOCK_END,           /* COMMIT received */
    TBLOCK_ABORT,         /* failed xact, awaiting ROLLBACK */
    TBLOCK_ABORT_END,     /* failed xact, ROLLBACK received */
    TBLOCK_ABORT_PENDING, /* live xact, ROLLBACK received */
};

struct DropSegPendingList {
    TablespaceId tableSpaceId;
    SegmentType type;
    PageId segmentId;
    PageId lobsegmentId;
    Oid tableoid;
    bool atCommit;
    DropSegPendingList *next;
};

}  // namespace DSTORE

#endif
