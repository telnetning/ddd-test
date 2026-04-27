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
 * dstore_td.h
 *
 * IDENTIFICATION
 *        include/page/dstore_td.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_TD_H
#define DSTORE_TD_H

#include "common/log/dstore_log.h"
#include "undo/dstore_undo_types.h"
#include "undo/dstore_transaction_slot.h"
#include "transaction/dstore_transaction_types.h"
#include "framework/dstore_thread.h"

namespace DSTORE {

constexpr uint8 MIN_TD_COUNT = 2;
constexpr uint8 MAX_TD_COUNT = 128;
constexpr uint8 DEFAULT_TD_COUNT = 4;
constexpr uint8 EXTEND_TD_NUM = 2;
constexpr uint8 EXTEND_TD_MIN_NUM = 1;
/* need greater than (sizeof(WalRecordBtreeAllocTd) + sizeof(WalRecordForDataPage::AllocTdRecord) +
 * sizeof(TrxSlotStatus) * MAX_TD_COUNT) */
constexpr uint32 MAX_TD_WAL_DATA = 48 + MAX_TD_COUNT;

struct XidStatus;

enum class TDStatus {
    UNOCCUPY_AND_PRUNEABLE = 0,  /* Accurate status: new allocate td or all trx used this td slot can be pruned */
    OCCUPY_TRX_IN_PROGRESS,      /* Inaccurate status: occupy by a running trx, but this status may be stale,
                                  * we will not freshen this state when trx finish
                                  * we need query undo for the accurate trx info */
    OCCUPY_TRX_END,              /* Accurate status: last trx using this td slot has been committed or aborted */
};

struct TdRecycleStatus {
    bool unused;
    uint8 id;
};

enum TdCsnStatus : uint8 {
    IS_INVALID = 0,
    IS_PREV_XID_CSN,
    IS_CUR_XID_CSN
};

struct TDAllocContext {
    struct TDAllocWALInfo {
        TrxSlotStatus xidStatus[MAX_TD_COUNT];
        uint8 tdNum;
        uint8 extendNum;
        bool isDirty;
        bool needWal;
        bool hasRollbackTd;
        bool rollbackTds[MAX_TD_COUNT];
        char walData[MAX_TD_WAL_DATA];
    };
    struct TDReuseWaitInfo {
        Xid xids[MAX_TD_COUNT + MAX_TD_COUNT];
        uint8 xidNum;
    };

    PdbId m_pdbId;
    Transaction *txn;
    CommitSeqNo recycleMinCsn;
    bool needReleaseBufThenRetry;
    bool tryLocalXidOnly; /* */
    TDAllocWALInfo allocTd; /* tmp info to record alloctd wal */
    TDReuseWaitInfo waitXids;

    void Init(PdbId pdbId, bool needWal)
    {
        m_pdbId = pdbId;
        txn = thrd->GetActiveTransaction();
        recycleMinCsn = INVALID_CSN;
        tryLocalXidOnly = false;
        DisableRetryAllocTd();
        InitTdAllocWalInfo(needWal);
        InitTdReuseWaitInfo();
    }

    void Begin(CommitSeqNo csn)
    {
        StorageReleasePanic((allocTd.needWal && allocTd.isDirty), MODULE_PAGE, ErrMsg("Forget to record alloctd wal."));
        StorageReleasePanic((allocTd.needWal && allocTd.hasRollbackTd), MODULE_PAGE, ErrMsg("Forget to record rollback wal."));
        needReleaseBufThenRetry = false;
        tryLocalXidOnly = false;
        recycleMinCsn = csn;
        InitTdAllocWalInfo(allocTd.needWal);
        InitTdReuseWaitInfo();
    }

    void Begin(PdbId pdbId, CommitSeqNo csn)
    {
        m_pdbId = pdbId;
        Begin(csn);
    }

    bool NeedRetryAllocTd() const
    {
        return needReleaseBufThenRetry;
    }

    void EnableRetryAllocTd()
    {
        needReleaseBufThenRetry = true;
    }

    void DisableRetryAllocTd()
    {
        needReleaseBufThenRetry = false;
    }

    void InitTdAllocWalInfo(bool needWal)
    {
        allocTd.tdNum = 0;
        allocTd.extendNum = 0;
        allocTd.isDirty = false;
        allocTd.needWal = needWal;
        allocTd.hasRollbackTd = false;
    }

    void InitTdReuseWaitInfo()
    {
        waitXids.xidNum = 0;
    }

    void AddTdReuseWaitXid(Xid xid)
    {
        for (uint8 i = 0; i < waitXids.xidNum; i++) {
            if (waitXids.xids[i] == xid) {
                return;
            }
        }
        waitXids.xids[waitXids.xidNum] = xid;
        waitXids.xidNum++;
    }
};

/*
 * Page Transaction Directory Item
 */
struct TD {
    uint64 m_xid;
    CommitSeqNo m_csn;
    uint64 m_undoRecPtr;
    uint64 m_lockerXid;
    CommandId m_commandId;    /* The query command id of current transaction */
    uint16 m_status : 2;
    uint16 m_csnStatus : 2;
    uint16 m_pad : 12;

    inline void Reset()
    {
        m_csn = INVALID_CSN;
        m_xid = INVALID_XID.m_placeHolder;
        m_undoRecPtr = INVALID_UNDO_RECORD_PTR.m_placeHolder;
        m_lockerXid = INVALID_XID.m_placeHolder;
        m_status = static_cast<uint16>(TDStatus::UNOCCUPY_AND_PRUNEABLE);
        m_csnStatus = static_cast<uint16>(IS_INVALID);
        m_commandId = INVALID_CID;
        m_pad = 0;
    }

    inline Xid GetXid() const
    {
        return static_cast<Xid>(m_xid);
    }

    inline Xid GetLockerXid() const
    {
        return static_cast<Xid>(m_lockerXid);
    }
    inline void SetLockerXid(Xid xid)
    {
        m_lockerXid = xid.m_placeHolder;
    }

    inline CommitSeqNo GetCsn() const
    {
        return m_csn;
    }
    inline TdCsnStatus GetCsnStatus() const
    {
        return static_cast<TdCsnStatus>(m_csnStatus);
    }
    inline bool TestCsnStatus(TdCsnStatus status) const
    {
        return m_csnStatus == static_cast<uint16>(status);
    }
    inline void SetCsnStatus(TdCsnStatus status)
    {
        m_csnStatus = static_cast<uint16>(status);
    }
    inline void SetCsn(CommitSeqNo csn)
    {
        m_csn = csn;
    }
    inline UndoRecPtr GetUndoRecPtr() const
    {
        return static_cast<UndoRecPtr>(m_undoRecPtr);
    }
    inline void SetXid(Xid xid)
    {
        m_xid = xid.m_placeHolder;
    }
    inline void SetUndoRecPtr(UndoRecPtr undoRecPtr)
    {
        m_undoRecPtr = undoRecPtr.m_placeHolder;
    }

    inline TDStatus GetStatus() const
    {
        return static_cast<TDStatus>(m_status);
    }
    inline void SetStatus(TDStatus status)
    {
        m_status = static_cast<uint16>(status);
    }
    inline bool TestStatus(TDStatus status) const
    {
        return m_status == static_cast<uint16>(status);
    }

    void SetCommandId(CommandId commandId)
    {
        m_commandId = commandId;
    }
    CommandId GetCommandId() const
    {
        return m_commandId;
    }

    CommitSeqNo FillCsn(Transaction *transaction, XidStatus *xstatus = nullptr);

    void RollbackTdInfo(class UndoRecord *undo);
    void RollbackTdToPreTxn(PdbId pdbId);
    /* Comparator for CSN-Max-Heap used in td rollback during cr page construct. */
    static int RollbackComparator(Datum a, Datum b, void *arg)
    {
        (void)arg;
        TD* tdA = static_cast<TD *>(static_cast<void *>(DatumGetPointer(a)));
        TD* tdB = static_cast<TD *>(static_cast<void *>(DatumGetPointer(b)));

        /* we want a max-heap, so return 1 when a > b. */
        if (tdA->GetCsn() > tdB->GetCsn()) {
            return 1;
        } else if (tdA->GetCsn() == tdB->GetCsn()) {
            return 0;
        } else {
            return -1;
        }
    }

#ifdef DSTORE_USE_ASSERT_CHECKING
    bool CheckSanity() const
    {
        if (TestStatus(TDStatus::UNOCCUPY_AND_PRUNEABLE)) {
            StorageAssert(m_xid == INVALID_XID.m_placeHolder);
            StorageAssert(m_csn == INVALID_CSN);
            StorageAssert(m_undoRecPtr == INVALID_UNDO_RECORD_PTR.m_placeHolder);
            StorageAssert(m_csnStatus == static_cast<uint16>(IS_INVALID));
        } else if (TestStatus(TDStatus::OCCUPY_TRX_IN_PROGRESS)) {
            StorageAssert(m_lockerXid != INVALID_XID.m_placeHolder ||
                (m_xid != INVALID_XID.m_placeHolder && m_undoRecPtr != INVALID_UNDO_RECORD_PTR.m_placeHolder));
        } else if (TestStatus(TDStatus::OCCUPY_TRX_END)) {
            StorageAssert(m_xid != INVALID_XID.m_placeHolder);
            StorageAssert(m_undoRecPtr != INVALID_UNDO_RECORD_PTR.m_placeHolder);
        } else {
            StorageAssert(0);
        }

        if (TestCsnStatus(IS_INVALID)) {
        } else if (TestCsnStatus(IS_PREV_XID_CSN)) {
            StorageAssert(m_csn != INVALID_CSN);
        } else if (TestCsnStatus(IS_CUR_XID_CSN)) {
            StorageAssert(m_csn != INVALID_CSN);
            StorageAssert(m_xid != INVALID_XID.m_placeHolder);
        } else {
            StorageAssert(0);
        }

        return true;
    }
#endif
};

} /* The end of DSTORE */
#endif  // STORAGE_TD_H
