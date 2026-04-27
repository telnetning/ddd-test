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
 * dstore_transaction.h
 *
 *
 * IDENTIFICATION
 *        storage/include/transaction/dstore_transaction.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_TRANSACTION_H
#define DSTORE_TRANSACTION_H

#include "transaction/dstore_transaction_types.h"
#include "transaction/dstore_csn_mgr.h"
#include "perfcounter/dstore_perf_item.h"
#include "common/algorithm/dstore_ilist.h"
#include "common/dstore_datatype.h"
#include "tuple/dstore_memheap_tuple.h"
#include "page/dstore_heap_page.h"
#include "index/dstore_index_struct.h"
#include "transaction/dstore_cursor.h"
#include "transaction/dstore_savepoint.h"
#include "tablespace/dstore_segment.h"

namespace DSTORE {

struct CRContext {
    PdbId pdbId;
    CommitSeqNo pageMaxCsn;      /* The max csn remained in cr page */
    DataPage *localPage;         /* Local cached page */
    BufferDesc *baseBufDesc;     /* Given base page desc, don't need to look for it again */
    BufferDesc *crBufDesc;       /* cr buffer desc, not null iff useLocalCr is false */
    bool useLocalCr;             /* Whether the cr page is local cr */
    bool isGlobalTempTable;      /* for global temp table */
    Snapshot snapshot;
    Xid currentXid;
};

struct TrxCallBckItem {
    struct TrxCallBckItem *next;
    TrxCallback callbckFun;
    void *arg;
};

#ifdef UT
#define private public
#endif

class Transaction : public BaseObject {
public:
    Transaction(StorageInstance *instance, ThreadContext *thrdContext, ZoneId zid,
                BufPrivateRefCount *bufferPrivateRefCount, PdbId pdbId, bool isAutonomous = false,
                bool isSpecialAyncCommitAutoTrx = false);
    virtual ~Transaction() = default;

    RetStatus Initialize();
    void Destroy();

    PdbId GetPdbId();
    void SetPdbIdOnlyForRPC(PdbId pdbId)
    {
        m_pdbId = pdbId;
    }
    DISALLOW_COPY_AND_MOVE(Transaction);

    RetStatus Start();
    inline RetStatus Commit()
    {
        return Commit(true);
    }
    /* Caller must use CleanUpResourceAfterCommit() to clean up resource of transaction */
    /* Dangerous interface, just hack for v5 sql engine adapt */
    inline RetStatus CommitWithoutCleanUpResource()
    {
        return Commit(false);
    }
    /* Dangerous interface, just hack for v5 sql engine adapt */
    void CleanUpResourceAfterCommit();
    void CleanUpResourceAfterAbort();
    RetStatus Abort(bool terminateCurrTrx = false, bool cleanUpResource = true);
    RetStatus CommitRollbackAndRestoreTrxState(bool isRollback);
    RetStatus SetSnapshotCsn(bool useSnapshotNow = false);
    void SetSnapshotCsnForFlashback(CommitSeqNo csn);
    RetStatus GetSnapshotCsnForFlashback(CommitSeqNo &csn);
    void IncreaseCommandCounter();
    inline CommitSeqNo GetSnapshotCsn() const
    {
        return m_snapshot.GetCsn();
    }
    inline CommandId GetSnapshotCid() const
    {
        return m_snapshot.GetCid();
    }
    inline Snapshot GetSnapshot()
    {
        return &m_snapshot;
    }

    /* Cursor Functions */
    inline void DeleteAllCursorSnapshots()
    {
        GetCursorSnapshotList()->DeleteAll();
    }
    RetStatus AddCursorSnapshot(const char *name);
    RetStatus DeleteCursorSnapshot(const char *name);

    RetStatus SetTransactionSnapshotCid(CommandId cid);
    RetStatus SetTransactionSnapshotCsn(CommitSeqNo csn);
    inline void SetThrdLocalCsn(CommitSeqNo csn)
    {
        /* ThrdLocalCsn is used for MVCC snapshot, it should be updated only when csn is larger than current value,
           the TopTransaction starts first, so its snapshot must be less than or equal to that of other transactions.
           When cursorlist is not empty, it means that the snapshot of cursor is still in use, so we can't update */
        if ((IsTopTransaction() && IsCursorSnapshotListEmpty()) ||
            m_thrd->GetLocalCsn() == INVALID_CSN || m_thrd->GetLocalCsn() == MAX_COMMITSEQNO) {
            /* We should set local csn any way if local csn is invalid or max value */
            m_thrd->SetLocalCsn(csn);
        }
    }

    UndoRecPtr InsertUndoRecord(UndoRecord *undoRecord);
    UndoRecPtr GetLastUndoRecord();

    /* Savepoint Functions */
    int32 GetSavepointNestLevel()
    {
        return GetSavepointList()->GetNestLevel();
    }

    bool HasCurrentSavepointName()
    {
        return GetSavepointList()->HasCurrentSavepointName();
    }

    char *GetCurrentSavepointName()
    {
        if (IsSavepointListEmpty()) {
            return nullptr;
        }
        return GetSavepointList()->GetCurrentSavepointName();
    }

    bool IsSavepointListEmpty()
    {
        return GetSavepointList()->IsEmpty();
    }

    RetStatus CreateSavepoint(const char *name);
    RetStatus ReleaseSavepoint(const char *name, int16 *userSavepointCounter = nullptr,
        int16 *exceptionSavepointCounter = nullptr);
    RetStatus SaveExtraResPtrToSavepoint(const char *name, void* data);
    void* GetExtraResPtrFromSavepoint(const char* name);
    void* GetExtraResPtrFromCurrentSavepoint();
    RetStatus RollbackToSavepoint(const char *name, int16 *userSavepointCounter = nullptr,
        int16 *exceptionSavepointCounter = nullptr);
    RetStatus CheckSavepointCounter(int16 *userSavepointCounter, int16 *exceptionSavepointCounter) const;

    RetStatus Remember2PLock(const LockTag &tag, LockMode mode, LockMgrType mgrType);
    RetStatus Forget2PLock(const LockTag &tag, LockMode mode, LockMgrType mgrType);
    bool Has2PLock(const LockTag &tag, LockMode mode, LockMgrType mgrType);
    bool HasAny2PLockInMode(LockMode mode);
    LockResource::SubLockResourceID Generate2PLockSubResourceID();
    void Release2PLocksAcquiredAfter(LockResource::SubLockResourceID subResId);
    uint32 GetHold2PLockCnt(const LockTag &tag, LockMode mode, LockMgrType mgrType);
    RetStatus RememberLazy2PLock(const LockTag &tag, LockMode mode, LockMgrType mgrType, bool &isAlreadyHeld);
    RetStatus ForgetLazy2PLock(const LockTag &tag, LockMode mode, LockMgrType mgrType);
    RetStatus AcquireLazyLocks();

    /* transaction block support */
    bool IsTxnBlock() const;
    RetStatus BeginTransactionBlock();
    RetStatus EndTransactionBlock();
    RetStatus RollbackLastSQLCmd();
    RetStatus UserAbortTransactionBlock();
    void QueryXidStatus(XidStatus *xidstatus);
    char StatusCode() const;
    TBlockState GetCurTxnBlockState();

    TransAbortStage GetCurAbortStage();
    void SetCurAbortStage(TransAbortStage abortStage);

    /* visible check */
    bool XidVisibleToSnapshot(Xid xid, XidStatus *xidStatus = nullptr);
    inline bool XidVisibleToSnapshot(CommitSeqNo csn) const
    {
        StorageAssert(csn != INVALID_CSN);
        return csn < m_snapshot.GetCsn();
    }
    inline bool CidVisibleToSnapshot(CommandId cid) const
    {
        /* if tupleCid == snapshotCid, the tuple is invisible. */
        return cid < m_snapshot.GetCid();
    }

    /* CR page */
    BufferDesc *GetCrPage(BufferDesc *baseBufDesc, Snapshot snapshot);
    RetStatus ConstructCrPage(CRContext *crCtx, BtreeUndoContext *btrUndoContext = nullptr);

    /* transaction slot interaction */
    RetStatus AllocTransactionSlot();
    bool IsCurrent(Xid xid);

    void RegisterCallback(TrxCallback callbck, void* arg);
    void CopyCallbackList(TrxCallBckItem *callbckList);
    void FireCallback(TrxEvent event);

    char GetTacTransactionState(Xid xid) const;
    void SetSnapshotCsnForTac(CommitSeqNo csn);

    inline bool IsTacXidSent() const
    {
        return m_currTransState.tacXidSent;
    }

    inline void SetTacXidSent(bool tacXidSent)
    {
        m_currTransState.tacXidSent = tacXidSent;
    }

    inline Xid GetCurrentXid() const
    {
        return m_currTransState.xid;
    }

    inline void SetCurrentXid(Xid xid)
    {
        m_currTransState.xid = xid;
    }

    inline bool InTransaction() const
    {
        return (m_currTransState.state == TransState::TRANS_INPROGRESS);
    }

    inline bool IsTransactionRunning() const
    {
        return (m_currTransState.state != TransState::TRANS_DEFAULT);
    }

    inline bool IsTransactionDefault() const
    {
        return m_currTransState.state == TransState::TRANS_DEFAULT;
    }

    inline bool IsTransactionStart() const
    {
        return m_currTransState.state == TransState::TRANS_START;
    }

    inline bool IsTBlockDefault() const
    {
        return (m_currTransState.blockState == TBlockState::TBLOCK_DEFAULT);
    }

    inline bool IsTBlockAborted() const
    {
        return (m_currTransState.blockState == TBlockState::TBLOCK_ABORT);
    }

    inline CommandId GetCurCid() const
    {
        return m_currentCommandId;
    }

    /* "used" must be set if the caller intends to use the command ID to mark inserted/updated/deleted tuples */
    inline void SetCurCidUsed()
    {
        m_currentCommandIdUsed = true;
    }

    inline bool IsCurCidUsed() const
    {
        return m_currentCommandIdUsed;
    }

    inline TrxIsolationType GetIsolationLevel() const
    {
        return m_isolationLevel;
    }

    inline void SetIsolationLevel(TrxIsolationType isoLevel)
    {
        m_isolationLevel = isoLevel;
    }

    inline bool ReadOnly() const
    {
        return m_currTransState.readOnly;
    }

    inline void SetReadOnly(bool readOnly)
    {
        m_currTransState.readOnly = readOnly;
    }

    inline bool HoldXactLock() const
    {
        return m_currTransState.holdXactLock;
    }

    inline void SetHoldXactLock(bool holdXactLock)
    {
        m_currTransState.holdXactLock = holdXactLock;
    }

    inline ZoneId GetCurrentZoneId() const
    {
        return m_zid;
    }

    inline void SetCurrentZoneId(ZoneId zid)
    {
        m_zid = zid;
    }

    inline uint64 GetTransStartTime() const
    {
        return m_startTime;
    }

    inline bool IsAutonomousTransaction() const
    {
        return m_isAutonomous;
    }

    inline void* GetTransactionExtraResPtr() const
    {
        return m_extraResPtr;
    }

    inline void SetTransactionExtraResPtr(void *ptr)
    {
        m_extraResPtr = ptr;
    }
    
    inline DstoreMemoryContext GetMemoryContext()
    {
        return m_memoryCtx;
    }

    inline BufPrivateRefCount *GetBufferPrivateRefCount()
    {
        return m_bufferPrivateRefCount;
    }
    
    inline bool IsMvccSnapshot() const
    {
        return m_snapshot.snapshotType == SnapshotType::SNAPSHOT_MVCC;
    }

    inline bool InTransactionBlock()
    {
        return (m_currTransState.blockState == TBlockState::TBLOCK_INPROGRESS);
    }

    inline uint16 GetGucLevel() const
    {
        return m_gucLevel;
    }

    inline void SetGucLevel(const uint16 gucLevel)
    {
        m_gucLevel = gucLevel;
    }

    static const char *BlockStateAsString(TBlockState blockState);

    void SaveCurTransactionState();
    void RestoreCurTransactionState();
    void SetCurTxnBlockState(TBlockState tBlockState);

    /* release locks, only called before thread destroy */
    void ReleaseLocksInTransRes();

#ifdef UT
    Snapshot GetSnapshotData();
    inline void SetSnapshotCid(CommandId cid)
    {
        m_snapshot.SetCid(cid);
    }
#endif

    DstoreMemoryContext  m_memoryCtx;
    /* This memory context is just used for memory in the transaction's abort process.
     * First, try you best to avoid requesting memory when rolling back a transaction.
     * If memory is requested, it should be released immediately
     * after use to avoid exhausting pre-allocated memory.
     * This is because when aborting a transaction,
     * it may not be possible to request additional memory.
     */
    DstoreMemoryContext  m_abortMemoryCtx;
    BufPrivateRefCount  *m_bufferPrivateRefCount;
    Transaction         *m_prevTransaction;
    /* callback for txn event */
    TrxCallBckItem      *m_callbckList;
    DropSegPendingList  *m_dropSegPendingList;
private:
    PdbId                m_pdbId;
    StorageInstance     *m_instance;
    ThreadContext       *m_thrd;
    CsnMgr              *m_csnMgr;
    TransactionStateData m_currTransState;
    CursorSnapshotList   m_cursorSnapshotList;
    SavepointList        m_savepoints;
    Savepoint            m_innerSavepoint;
    LockResource         m_lockResource;
    ZoneId               m_zid;
    uint64               m_startTime;
    TrxIsolationType     m_isolationLevel;
    bool                 m_isAutonomous;
    uint16               m_gucLevel;
    LatencyStat::Timer   m_trxCompleteTimer;
    SnapshotData         m_snapshot;
    bool                 m_isSpecialAyncCommitAutoTrx;

    /*
     * If the caller intends to use the command ID to mark inserted/updated/deleted tuples,
     * m_currentCommandIdUsed must be set true.
     */
    bool m_currentCommandIdUsed;
    CommandId m_currentCommandId;

    /* a cache for transaction slot tail undoptr. */
    UndoRecPtr m_lastUndoPtr;

    const char *TransStateAsString(TransState state);

    /* a void* to save resource for transaction */
    void* m_extraResPtr;

    /* start */
    void InitInfoForNewTrx();

    /* commit */
    void PreCommit();
    RetStatus RecordCommit();
    void PostCommit();

    /* abort */
    void PreAbort();
    RetStatus RecordAbort();
    void PostAbort();
    void PostTerminate();
    void PreCleanUp();

    /* internal transaction AM */
    RetStatus StartInternal();
    RetStatus CommitInternal(bool cleanUpResource);
    RetStatus Commit(bool cleanUpResource);
    RetStatus AbortInternal(bool cleanUpResource = true);
    RetStatus Cleanup();

    /* transaction-level resource clean */
    void CleanupResource();
    void ResetMemoryContext();
    bool IsTermMatched();

    /* Cursor Functions */
    inline CursorSnapshotList *GetCursorSnapshotList()
    {
        return &m_cursorSnapshotList;
    }

    inline CommitSeqNo GetCursorSnapshotMinCsn()
    {
        return GetCursorSnapshotList()->GetCursorSnapshotMinCsn();
    }

    inline bool IsCursorSnapshotListEmpty()
    {
        return GetCursorSnapshotList()->IsEmpty();
    }

    /* Savepoint Functions */
    inline SavepointList *GetSavepointList()
    {
        return &m_savepoints;
    }

    inline bool IsTopTransaction()
    {
        return m_prevTransaction == nullptr;
    }
};

#ifdef UT
#undef private
#endif

struct XidStatus : public BaseObject {
    XidStatus(Xid inxid, Transaction *intrx, struct TD *td = nullptr);

    inline TrxSlotStatus GetStatus()
    {
        InitIfNeeded();
        return status;
    }

    inline CommitSeqNo GetCsn()
    {
        InitIfNeeded();
        return csn;
    }

    inline bool IsCurrentTxn() const
    {
        return trx->IsCurrent(xid);
    }

    inline bool IsFrozen()
    {
        InitIfNeeded();
        return status == TXN_STATUS_FROZEN;
    }

    inline bool IsCommitted()
    {
        InitIfNeeded();
        return status == TXN_STATUS_COMMITTED;
    }

    inline bool IsAborted()
    {
        InitIfNeeded();
        return status == TXN_STATUS_ABORTED;
    }

    inline bool IsInProgress()
    {
        InitIfNeeded();
        return status == TXN_STATUS_IN_PROGRESS;
    }

    inline bool IsPendingCommit()
    {
        InitIfNeeded();
        bool isPendingCommit = (status == TXN_STATUS_PENDING_COMMIT);
        if (isPendingCommit) {
            isInitialized = false;
        }
        return isPendingCommit;
    }

    inline bool NeedWaitPendingTxn()
    {
        InitIfNeeded();
        return needWaitPendingTxn;
    }

    inline Xid GetXid() const
    {
        return xid;
    }

    inline void SetCsn(CommitSeqNo incsn)
    {
        csn = incsn;
    }

    inline void SetStatus(TrxSlotStatus instatus)
    {
        status = instatus;
    }

    inline void SetInitialized()
    {
        isInitialized = true;
    }

    inline void SetNeedWaitPendingTxn(bool needWait)
    {
        needWaitPendingTxn = needWait;
    }

private:
    Xid            xid;
    TrxSlotStatus  status;
    CommitSeqNo    csn;
    Transaction   *trx;
    bool           needWaitPendingTxn;
    bool           isInitialized;

    inline void InitIfNeeded()
    {
        if (!isInitialized) {
            trx->QueryXidStatus(this);
            isInitialized = true;
        }
    }
};

constexpr int TRX_MEM_CTX_NAME_LEN{16};
/*
 * Creates an identifier for transaction memory context
 * using ZoneId.
 */
inline char *GetTrxMemCtxName(ZoneId zoneId)
{
    char *name = static_cast<char *>(DstorePalloc0(TRX_MEM_CTX_NAME_LEN));
    if (STORAGE_VAR_NULL(name)) {
        ErrLog(DSTORE_ERROR, MODULE_TRANSACTION, ErrMsg("Failed to allocate memory for transaction memory context."));
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        return nullptr;
    }
    int rc = sprintf_s(name, TRX_MEM_CTX_NAME_LEN, "%d", zoneId);
    storage_securec_check_ss(rc);
    return name;
}

inline bool CidVisibleToSnapshot(Snapshot snapshot, CommandId cid)
{
    /* if tupleCid == snapshotCid, the tuple is invisible. */
    return cid < snapshot->GetCid();
}

inline bool CidVisibleToSnapshot(Transaction *txn, Snapshot snapshot, CommandId cid)
{
    /* The first OR condition just hack for v5 sql engine trigger feature adapt, further consideration is needed. */
    return (!txn->IsCurCidUsed() && txn->GetCurCid() == snapshot->GetCid()) || cid < snapshot->GetCid();
}

inline bool XidVisibleToSnapshot(Snapshot snapshot, CommitSeqNo csn)
{
    StorageAssert(csn != INVALID_CSN);
    return csn < snapshot->GetCsn();
}

extern bool XidVisibleToSnapshot(Snapshot snapshot, Xid xid, Transaction *txn, XidStatus *xidStatus = nullptr);

}  // namespace DSTORE

#endif
