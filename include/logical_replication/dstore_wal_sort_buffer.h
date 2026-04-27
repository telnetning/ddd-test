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
 * dstore_wal_sort_buffer.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_WAL_SORT_BUFFER_H
#define DSTORE_WAL_SORT_BUFFER_H
 
#include "wal/dstore_wal.h"
#include "common/dstore_datatype.h"
#include "tuple/dstore_memheap_tuple.h"
#include "common/dstore_common_utils.h"
#include "dstore_parallel_decode_worker.h"
#include "dstore_logical_replication_slot.h"

#ifdef UT
#ifndef ENABLE_LOGICAL_REPL
/* LOGICAL REPL is not supported for now. */
#define ENABLE_LOGICAL_REPL
#endif
#endif

namespace DSTORE {

#ifdef ENABLE_LOGICAL_REPL
constexpr int TXN_DEFAULT_BUCKET_NUM = 1000;

/* used for choosing decode worker to decode next transaction through polling */
struct DecodingWorkerInfo {
    int curChoosedWorkerCursor;
    /* pointer to all decode workers */
    ParallelDecodeWorker** decodeWorker;
    /* number of decode workers */
    int totalWorkerNum;

    void Init(ParallelDecodeWorker **workerArray, int parallelDecodeWorkerNum)
    {
        curChoosedWorkerCursor = 0;
        decodeWorker = workerArray;
        totalWorkerNum = parallelDecodeWorkerNum;
    }

    ParallelDecodeWorker* GetCurDecodeWorker()
    {
        StorageAssert(curChoosedWorkerCursor < totalWorkerNum);
        return decodeWorker[curChoosedWorkerCursor];
    }
};

union TrxChangeCtxFlag {
    struct {
        uint32 hasCatalogChanges : 1;
        uint32 isSerialized : 1;
        uint32 isSerializedClear : 1;
        uint32 isFake : 1;
        uint32 unused : 28;
    };
    uint32 flag;
 
    TrxChangeCtxFlag() : flag(0) {}
 
    bool HasCatalogChanges() const
    {
        return (hasCatalogChanges == 1);
    }
 
    void SetCatalogChanges(const bool hasCatalogChange)
    {
        hasCatalogChanges = (hasCatalogChange ? 1 : 0);
    }

    bool IsFakeTrxChange() const
    {
        return (isFake == 1);
    }

    void SetFakeTrxChange(const bool isTrxFake)
    {
        isFake = (isTrxFake ? 1 : 0);
    }
};

struct TrxChangeCtx {
    Xid xid;
    TrxChangeCtxFlag flags;
    WalPlsn firstPlsn;
    WalPlsn endPlsn;
    WalPlsn commitPlsn;
    TimestampTz commitTime;
    CommitSeqNo commitCsn;
    CommitSeqNo committingCsn;
    /*
	 * Size of this transaction (changes currently in memory, in bytes).
	 */
    Size size;
    /* total size. */
    Size totalSize;
 
    dlist_head changes;
    int numChanges;
    int numChangesInMemory;

    /* when generate this trxChangectx, this points the current minimal plsn of all trxs in walsortbuffer,
     * this is used for slot restartPlsn pushing */
    WalPlsn restartDecodingPlsn;
    
    /* list in walsortbuffers. */
    dlist_node node;

    /* dependedTxn count, only dependTxnCnt becomes 0 can it be decoded by plugin. */
    int dependTxnCnt;
    /* referTxns are transactions which depend on this txn. */
    dlist_head referTxns;

    /* node in committing list */
    dlist_node committingNode;
    /* node in commit list */
    dlist_node commitNode;

    /* Trx block has ddl internal */
    int ddlCounts;
    dlist_head catalogTupleChanges;
    dlist_head internalTableInfos;
    dlist_head internalNameSpaceInfos;
    /* (tableOid => DecodeTableInfo) */
    HTAB *internalDecodeTableInfoHash;
    dlist_head trxInternalDecodeTblInfos;
};

struct DependentTrx {
    TrxChangeCtx* trx;
    dlist_node dependNode;
};
struct TrxChangeCtxByIdEntry {
    Xid xid;
    TrxChangeCtx *trxChange;
};

struct TupleBuf {
    HeapTuple memTup;
    char diskTup[];
};

enum class RowChangeType : uint32 {
    INSERT = 1,
    UPDATE = 2,
    DELETE = 3,
    CATALOG_INSERT = 4,
    CATALOG_UPDATE = 5,
    CATALOG_DELETE = 6,
    INTERNAL_SPEC_INSERT = 7,
    INTERNAL_SPEC_CONFIRM = 8,
    INTERNAL_SPEC_ABORT = 9,
};

/*
 * a single 'change', can be an insert (with one tuple), an update (old, new),
 * or a delete (old).
 */
struct RowChange {
    WalPlsn plsn;
    RowChangeType type;
    TrxChangeCtx *trx;
    union {
        /* Old, new tuples when action == *_INSERT|UPDATE|DELETE */
        struct {
            Oid tableOid;
            /* valid for DELETE || UPDATE */
            TupleBuf* oldTuple;
            /* valid for INSERT || UPDATE */
            TupleBuf* newTuple;
            CommitSeqNo snapshotCsn;
        } tuple;
        struct {
            Oid tableOid;
            /* valid for DELETE || UPDATE */
            TupleBuf* oldTuple;
            /* valid for INSERT || UPDATE */
            TupleBuf* newTuple;
            CommitSeqNo snapshotCsn;
            CommandId cid;
            dlist_node catalogNode;
        } catalogTuple;
    } data;
    dlist_node node;

    inline bool IsCatalogChange() const
    {
        if (type == RowChangeType::CATALOG_INSERT ||
            type == RowChangeType::CATALOG_UPDATE ||
            type == RowChangeType::CATALOG_DELETE) {
                return true;
        }
        return false;
    }
};

class WalSortBuffer : public BaseObject {
public:
    WalSortBuffer(DstoreMemoryContext mctx, LogicalReplicationSlot *logicalSlot,
                  ParallelDecodeWorker** workerArray, int parallelDecodeWorkerNum);
    ~WalSortBuffer();
    RetStatus Init();
    void Destroy();

    TrxChangeCtx *GetTrxChangeCtx(const Xid xid, const WalPlsn plsn = INVALID_PLSN, bool addIfNotExist = false);
    void FreeTrxChangeCtx(TrxChangeCtx* trxChange);
    void FreeTrxChangeCtx(Xid xid);
    void RemoveTrxEntry(TrxChangeCtx *trxChange);
    void QueueRowChange(Xid xid, RowChange *rowChange, WalPlsn plsn);
    TrxChangeCtx* AllocTrxChangeCtx();
    RowChange *AllocRowChange();
    TupleBuf *AllocTupleBuf(Size tupleLen);

    void TrxCommitButPass(Xid xid);
    void TrxCommitting(Xid xid, CommitSeqNo committingCsn);
    void TrxCommit(Xid xid, CommitSeqNo commitCsn, WalPlsn commitPlsn);
    void TrxAbort(Xid xid);
    void QueueFakeTrxChange(CommitSeqNo nextCsn, WalPlsn trxBeginPlsn);

    CommitSeqNo GetCurrentDecodedCsn() const;
    int GetDecodedChangesNum() const;

    /* cached xid => TrxChangeCtx, Very frequently the same trx gets looked up over and over again.
     * be a public member for fixing codecheck */
    Xid m_lastXid;
    TrxChangeCtx* m_lastTrxCtx;
private:
    void UpdateCache(Xid xid, TrxChangeCtx *trxChange);
    void FreeRowChange(RowChange *rowChange);
    void FreeTupleBuf(TupleBuf *tupBuf) const;
    Size GetRowChangeSize(RowChange *rowChange) const;
    void ChooseWorkerToDecode(TrxChangeCtx *trx);
    /* used for memorry and disk swapping. */
    void CheckBufLimitAndSpillToDiskIfNeed();

    /* xid => TrxChangeCtx lookup table */
    HTAB *m_txnTable;

    DstoreMemoryContext m_memoryContext;
    LogicalReplicationSlot *m_logicalSlot;
 
    /* Memory contexts for specific types objects */
    DstoreMemoryContext m_txnContext;
    DstoreMemoryContext m_changeContext;
    DstoreMemoryContext m_tupContext;

    WalPlsn m_currentActiveTrxsMinPlsn;
    DecodingWorkerInfo m_decodingWorkerInfo;
    CommitSeqNo m_lowerBoundOfNextLogicalLogCsn;
    int m_decodeNChanges;

    /* transaction that are committing, order by committingCsn */
    dlist_head m_toplevelByCommittingCsn;
    /* transaction that are commit, order by commitCsn */
    dlist_head m_toplevelByCommitCsn;
    /* transaction that are decoding, order by first walrecord plsn */
    dlist_head m_toplevelByPlsn;
    /* transaction that are decoding, order by first walrecord snapshot csn */
    dlist_head m_toplevelBySnapshotCsn;
};
#endif

}
#endif