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
 * dstore_transaction_type.h
 *
 *
 * IDENTIFICATION
 *        storage/include/transaction/dstore_transaction_types.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_TRANSACTION_TYPE_H
#define DSTORE_TRANSACTION_TYPE_H

#include "common/memory/dstore_mctx.h"
#include "common/algorithm/dstore_string_info.h"
#include "transaction/dstore_transaction_struct.h"

namespace DSTORE {

enum class TrxIsolationType {
    XACT_READ_UNCOMMITTED,  // Not implemented/tested for DSTORE
    XACT_READ_COMMITTED,
    XACT_TRANSACTION_SNAPSHOT,
    XACT_SERIALIZABLE       // Not implemented/tested for DSTORE
};

/* transaction slot address; logical address
 * 20 bits: zoneID
 * 44 bits: logicSlotId
 *
 * Similar to ItemPointerData. We want to update Xid atomically sometimes. Thus use a union type to define it.
 */
union Xid {
    uint64 m_placeHolder;
    struct {
        uint64 m_zoneId : 20;
        uint64 m_logicSlotId : 44;
    };

    Xid() = default;

    explicit Xid(uint64 rawData) noexcept : m_placeHolder(rawData)
    {}

    Xid(uint64 zoneId, uint64 slotId) : m_zoneId(zoneId), m_logicSlotId(slotId)
    {}

    inline bool operator==(const Xid &xid) const
    {
        return m_placeHolder == xid.m_placeHolder;
    }
    bool operator!=(const Xid &xid) const
    {
        return !(*this == xid);
    }

    inline bool IsValid() const
    {
        return m_placeHolder != static_cast<uint64>(-1);
    }

    void Dump(StringInfo str) const;
} PACKED;

const Xid INVALID_XID = Xid(-1);
/*
 * transaction states - transaction state from server perspective
 */
enum class TransState : uint8 {
    TRANS_DEFAULT,    /* idle */
    TRANS_START,      /* transaction starting */
    TRANS_INPROGRESS, /* inside a valid transaction */
    TRANS_COMMIT,     /* commit in progress */
    TRANS_ABORT,      /* abort in progress */
};

enum class TransAbortStage : uint8 {
    AbortNotStart = 0, /* not start to abort */
    PreAbortDone,      /* preabort done */
    RecordAbortDone,   /* record abort done */
    PostAbortDone,     /* some callbacks must be executed when post abort */
    /* The postabort may use transactional resources, so it must occur before resource cleanup,
     * and if the resources are cleaned up, postabort will not re-enter.
     */
    CleanUpResourceDone,
    DecreasePdbTransCountDone, /* others depend on the number of trans int pdb, so we ensure only decrease once */
    DstoreAbortCompleted,      /* abort done in dstore */
    SqlAbortCompleted          /* abort done in sql, abort may not complete after DstoreAbortCompleted status */
    /* if there are something wrong in the do_action_after_abort_xact progress */
};

struct TransactionStateData {
    Xid xid;                             /* my XID, or Invalid if none */
    TransState state;                    /* low-level state */
    TBlockState blockState;              /* high-level state */
    bool readOnly;                       /* whether xid is read only */
    bool tacXidSent;                     /* whether xid is sent to the client for TAC support */
    /* if we get xid but lock xact failed, we should know whether xact lock is held when do abort */
    bool holdXactLock;
    TransAbortStage abortStage; /* we record abort stage here, then we can re-enter if error */
};
using TransactionState = TransactionStateData *;

/* TAC transaction state to support Transparent Application Continuity (TAC) */
enum TACTransactionState : char {
    TAC_TRX_UNKNOWN = 'u',
    TAC_TRX_IN_PROGRESS = 'i',
    TAC_TRX_FROZEN = 'f',
    TAC_TRX_COMMITTED = 'c',
    TAC_TRX_ABORTED = 'a'
};

}  // namespace DSTORE

#endif
