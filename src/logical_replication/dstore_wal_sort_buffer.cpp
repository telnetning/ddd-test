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
 * dstore_wal_sort_buffer.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "logical_replication/dstore_wal_sort_buffer.h"
#include "common/algorithm/dstore_ilist.h"

namespace DSTORE {

#ifdef ENABLE_LOGICAL_REPL
WalSortBuffer::WalSortBuffer(DstoreMemoryContext mctx, LogicalReplicationSlot *logicalSlot,
                             ParallelDecodeWorker** workerArray, int parallelDecodeWorkerNum)
    : m_lastXid(INVALID_XID),
      m_lastTrxCtx(nullptr),
      m_txnTable(nullptr),
      m_memoryContext(mctx),
      m_logicalSlot(logicalSlot),
      m_txnContext(nullptr),
      m_changeContext(nullptr),
      m_tupContext(nullptr),
      m_currentActiveTrxsMinPlsn(INVALID_END_PLSN),
      m_lowerBoundOfNextLogicalLogCsn(INVALID_CSN + 1),
      m_decodeNChanges(0)
{
    DListInit(&m_toplevelByCommitCsn);
    DListInit(&m_toplevelByCommittingCsn);
    DListInit(&m_toplevelByPlsn);
    DListInit(&m_toplevelBySnapshotCsn);
    m_decodingWorkerInfo.Init(workerArray, parallelDecodeWorkerNum);
}

WalSortBuffer::~WalSortBuffer()
{
    m_lastTrxCtx = nullptr;
    m_txnTable = nullptr;
    m_memoryContext = nullptr;
    m_logicalSlot = nullptr;
    m_txnContext = nullptr;
    m_changeContext = nullptr;
    m_tupContext = nullptr;
}

RetStatus WalSortBuffer::Init()
{
    StorageAssert(m_memoryContext != nullptr);
    HASHCTL info;
    info.keysize = sizeof(Xid);
    info.entrysize = sizeof(TrxChangeCtxByIdEntry);
    info.hcxt = m_memoryContext;
    int hashFlags = (HASH_ELEM | HASH_BLOBS | HASH_CONTEXT | HASH_SHRCTX);
    m_txnTable = hash_create("walSortByXidHash", TXN_DEFAULT_BUCKET_NUM, &info, hashFlags);

    m_txnContext = DstoreAllocSetContextCreate(
        m_memoryContext, "TrxChangeCtx", ALLOCSET_DEFAULT_SIZES, MemoryContextType::SHARED_CONTEXT);
    m_changeContext = DstoreAllocSetContextCreate(
        m_memoryContext, "Change", ALLOCSET_DEFAULT_SIZES, MemoryContextType::SHARED_CONTEXT);
    m_tupContext = DstoreAllocSetContextCreate(m_memoryContext, "Tup", ALLOCSET_DEFAULT_MAXSIZE,
        ALLOCSET_DEFAULT_MAXSIZE, ALLOCSET_DEFAULT_MAXSIZE, MemoryContextType::SHARED_CONTEXT);
    if (unlikely(m_txnTable == nullptr || m_txnContext == nullptr ||
        m_changeContext == nullptr || m_tupContext == nullptr)) {
        return DSTORE_FAIL;
    }

    return DSTORE_SUCC;
}

void WalSortBuffer::Destroy()
{
    if (m_txnTable != nullptr) {
        hash_destroy(m_txnTable);
        m_txnTable = nullptr;
    }
    if (m_txnContext != nullptr) {
        DstoreMemoryContextDelete(m_txnContext);
        m_txnContext = nullptr;
    }
    if (m_changeContext != nullptr) {
        DstoreMemoryContextDelete(m_changeContext);
        m_changeContext = nullptr;
    }
    if (m_tupContext != nullptr) {
        DstoreMemoryContextDelete(m_tupContext);
        m_tupContext = nullptr;
    }
}

void WalSortBuffer::TrxCommitting(Xid xid, CommitSeqNo committingCsn)
{
    /* add this trx into committing list and keep committing csn list in ascending order */
    TrxChangeCtx *committingTrx = GetTrxChangeCtx(xid, INVALID_END_PLSN, false);
    if (committingTrx == nullptr) {
        return;
    }
    bool insertFlag = false;
    dlist_iter iter;
    dlist_reverse_foreach(iter, &m_toplevelByCommittingCsn) {
        TrxChangeCtx* curTrx = dlist_container(TrxChangeCtx, committingNode, iter.cur);
        if (curTrx->committingCsn < committingCsn) {
            DListInsertAfter(iter.cur, &committingTrx->committingNode);
            insertFlag = true;
            break;
        }
    }
    if (!insertFlag) {
        DListPushHead(&m_toplevelByCommittingCsn, &committingTrx->committingNode);
    }
}

void WalSortBuffer::TrxCommit(Xid xid, CommitSeqNo commitCsn, WalPlsn commitPlsn)
{
    TrxChangeCtx *commitTrx = GetTrxChangeCtx(xid, INVALID_END_PLSN, false);
    if (commitTrx == nullptr) {
        return;
    }
    commitTrx->commitCsn = commitCsn;
    commitTrx->commitPlsn = commitPlsn;
    commitTrx->endPlsn = commitPlsn;
    /* step1. remove from committing list */
    DListDelete(&commitTrx->committingNode);

    /* step2. detached from all commit trxs */
    dlist_mutable_iter iterModify;
    dlist_foreach_modify(iterModify, &commitTrx->referTxns) {
        DependentTrx *trxNode = dlist_container(DependentTrx, dependNode, iterModify.cur);
        trxNode->trx->dependTxnCnt--;
        DListDelete(&trxNode->dependNode);
        DstorePfree(trxNode);
    }

    /* step3. add this commit trx to the other trx's referTrx whose committing csn < this commit csn */
    dlist_iter iter;
    dlist_foreach(iter, &m_toplevelByCommittingCsn) {
        TrxChangeCtx *curTrx = dlist_container(TrxChangeCtx, committingNode, iter.cur);
        if (curTrx->committingCsn < commitCsn) {
            DependentTrx *trxNode = static_cast<DependentTrx *>(DstorePalloc(sizeof(DependentTrx)));
            StorageReleasePanic(trxNode == nullptr, MODULE_LOGICAL_REPLICATION,
                                ErrMsg("Allocate space OOM, allocate trxNode fail."));
            trxNode->trx = commitTrx;
            DListPushTail(&curTrx->referTxns, &trxNode->dependNode);
            commitTrx->dependTxnCnt += 1;
        } else {
            break;
        }
    }

    /* step4: check if there are trxs (include this commitTrx) can be decode and chose a worker to decode */
    bool stop = false;
    bool isTrxHandle = false;
    dlist_foreach_modify(iterModify, &m_toplevelByCommitCsn) {
        if (stop && isTrxHandle) {
            break;
        }
        TrxChangeCtx *curTrx = dlist_container(TrxChangeCtx, commitNode, iterModify.cur);
        if (!stop && curTrx->dependTxnCnt == 0) {
            if (!isTrxHandle && curTrx->commitCsn > commitCsn) {
                if (commitTrx->dependTxnCnt == 0) {
                    ChooseWorkerToDecode(commitTrx);
                    DListDelete(&curTrx->commitNode);
                    ChooseWorkerToDecode(curTrx);
                    m_lowerBoundOfNextLogicalLogCsn = curTrx->commitCsn + 1;
                } else {
                    DListInsertBefore(&curTrx->commitNode, &commitTrx->commitNode);
                    stop = true;
                }
                isTrxHandle = true;
            } else {
                DListDelete(&curTrx->commitNode);
                ChooseWorkerToDecode(curTrx);
                m_lowerBoundOfNextLogicalLogCsn = curTrx->commitCsn + 1;
            }
        } else {
            stop = true;
            if (!isTrxHandle && curTrx->commitCsn > commitCsn) {
                DListInsertBefore(&curTrx->commitNode, &commitTrx->commitNode);
                isTrxHandle = true;
            }
        }
    }

    if (!isTrxHandle) {
        if (!stop && commitTrx->dependTxnCnt == 0) {
            ChooseWorkerToDecode(commitTrx);
            m_lowerBoundOfNextLogicalLogCsn = commitTrx->commitCsn + 1;
        } else {
            DListPushTail(&m_toplevelByCommitCsn, &commitTrx->commitNode);
        }
    }
}

void WalSortBuffer::TrxAbort(Xid xid)
{
    TrxChangeCtx *abortTrx = GetTrxChangeCtx(xid, INVALID_END_PLSN, false);
    if (abortTrx == nullptr) {
        return;
    }
    RemoveTrxEntry(abortTrx);
    FreeTrxChangeCtx(abortTrx);
}

/* to support filter decode condition, now just keep this interface. */
void WalSortBuffer::TrxCommitButPass(Xid xid)
{
    TrxChangeCtx *passTrx = GetTrxChangeCtx(xid, INVALID_END_PLSN, false);
    if (passTrx == nullptr) {
        return;
    }
    RemoveTrxEntry(passTrx);
    FreeTrxChangeCtx(passTrx);
}

void WalSortBuffer::ChooseWorkerToDecode(TrxChangeCtx *trx)
{
    if (trx->commitCsn <= m_logicalSlot->GetConfirmCsn()) {
        RemoveTrxEntry(trx);
        FreeTrxChangeCtx(trx);
        return;
    }
    ParallelDecodeWorker* worker = m_decodingWorkerInfo.GetCurDecodeWorker();
    RemoveTrxEntry(trx); /* just remove entry and then realse the memory after decode */
    worker->QueueTrx(trx);
    m_decodingWorkerInfo.curChoosedWorkerCursor =
        (m_decodingWorkerInfo.curChoosedWorkerCursor + 1) % m_decodingWorkerInfo.totalWorkerNum;
    m_decodeNChanges += trx->numChanges;
}

void WalSortBuffer::QueueRowChange(Xid xid, RowChange *rowChange, WalPlsn walPlsn)
{
    TrxChangeCtx *trxChanges = GetTrxChangeCtx(xid, walPlsn, true);
    StorageAssert(trxChanges != nullptr);
    StorageReleasePanic(trxChanges == nullptr, MODULE_LOGICAL_REPLICATION, ErrMsg("TrxChanges is null."));
    rowChange->trx = trxChanges;
    rowChange->plsn = walPlsn;
    DListPushTail(&trxChanges->changes, &rowChange->node);
    trxChanges->numChanges++;
    trxChanges->numChangesInMemory++;
}

Size WalSortBuffer::GetRowChangeSize(RowChange *rowChange) const
{
    Size totalSize = sizeof(RowChange);
    switch (rowChange->type) {
        case RowChangeType::INSERT:
        case RowChangeType::UPDATE:
        case RowChangeType::DELETE: {
            if (rowChange->data.tuple.oldTuple) {
                totalSize += sizeof(HeapTuple) + rowChange->data.tuple.oldTuple->memTup.GetDiskTupleSize();
            }
            if (rowChange->data.tuple.newTuple) {
                totalSize += sizeof(HeapTuple) + rowChange->data.tuple.newTuple->memTup.GetDiskTupleSize();
            }
            break;
        }
        case RowChangeType::CATALOG_INSERT:
        case RowChangeType::CATALOG_UPDATE:
        case RowChangeType::CATALOG_DELETE: {
            if (rowChange->data.tuple.oldTuple) {
                totalSize += sizeof(HeapTuple) + rowChange->data.catalogTuple.oldTuple->memTup.GetDiskTupleSize();
            }
            if (rowChange->data.tuple.newTuple) {
                totalSize += sizeof(HeapTuple) + rowChange->data.catalogTuple.newTuple->memTup.GetDiskTupleSize();
            }
            break;
        }
        case RowChangeType::INTERNAL_SPEC_INSERT:
        case RowChangeType::INTERNAL_SPEC_CONFIRM:
        case RowChangeType::INTERNAL_SPEC_ABORT:
        default:
            break;
    }
    return totalSize;
}

void WalSortBuffer::QueueFakeTrxChange(CommitSeqNo nextCsn, WalPlsn trxBeginPlsn)
{
    /* no need to warp a fake next csn logical log */
    if (!DListIsEmpty(&m_toplevelByCommittingCsn) || nextCsn < m_lowerBoundOfNextLogicalLogCsn) {
        return;
    }
    TrxChangeCtx *trxChanges = AllocTrxChangeCtx();
    trxChanges->xid = INVALID_XID;
    trxChanges->commitCsn = nextCsn;    /* fake commit */
    trxChanges->flags.SetFakeTrxChange(true);
    trxChanges->firstPlsn = trxBeginPlsn;
    if (m_currentActiveTrxsMinPlsn == INVALID_END_PLSN) {
        m_currentActiveTrxsMinPlsn = trxBeginPlsn;
    }
    trxChanges->restartDecodingPlsn = m_currentActiveTrxsMinPlsn;
    m_lowerBoundOfNextLogicalLogCsn = DstoreMax(nextCsn, m_lowerBoundOfNextLogicalLogCsn);
    ChooseWorkerToDecode(trxChanges);
}

TrxChangeCtx* WalSortBuffer::GetTrxChangeCtx(const Xid xid, const WalPlsn plsn, bool addIfNotExist)
{
    StorageAssert(xid != INVALID_XID);
    if (m_lastXid != INVALID_XID && m_lastXid == xid) {
        if (m_lastTrxCtx != nullptr || !addIfNotExist) {
            return m_lastTrxCtx;
        }
    }
    bool found;
    TrxChangeCtxByIdEntry *ent = static_cast<TrxChangeCtxByIdEntry *>(
        hash_search(m_txnTable, static_cast<const void *>(&xid), addIfNotExist ? HASH_ENTER : HASH_FIND, &found));
    if (found) {
        UpdateCache(xid, ent->trxChange);
        return ent->trxChange;
    } else if (addIfNotExist) {
        ent->trxChange = AllocTrxChangeCtx();
        ent->trxChange->xid = xid;
        ent->trxChange->firstPlsn = plsn;
        if (m_currentActiveTrxsMinPlsn == INVALID_END_PLSN) {
            m_currentActiveTrxsMinPlsn = plsn;
        }
        ent->trxChange->restartDecodingPlsn = m_currentActiveTrxsMinPlsn;
    } else {
        return nullptr;
    }
    UpdateCache(xid, ent->trxChange);
    return ent->trxChange;
}

TrxChangeCtx* WalSortBuffer::AllocTrxChangeCtx()
{
    TrxChangeCtx *trxChange = static_cast<TrxChangeCtx *>(
        DstoreMemoryContextAllocZero(m_txnContext, sizeof(TrxChangeCtx)));
    if (unlikely(trxChange == nullptr)) {
        ErrLog(DSTORE_PANIC, MODULE_LOGICAL_REPLICATION, ErrMsg("Allocate space OOM, allocate TrxChangeCtx fail."));
    }
    trxChange->dependTxnCnt = 0;
    trxChange->ddlCounts = 0;
    trxChange->internalDecodeTableInfoHash = nullptr;
    DListInit(&trxChange->changes);
    DListInit(&trxChange->referTxns);
    DListInit(&trxChange->catalogTupleChanges);
    DListInit(&trxChange->internalTableInfos);
    DListInit(&trxChange->internalNameSpaceInfos);
    DListInit(&trxChange->trxInternalDecodeTblInfos);
    DListPushTail(&m_toplevelByPlsn, &trxChange->node);
    return trxChange;
}

RowChange* WalSortBuffer::AllocRowChange()
{
    RowChange *rowChange = static_cast<RowChange *>(
        DstoreMemoryContextAllocZero(m_changeContext, sizeof(RowChange)));
    if (unlikely(rowChange == nullptr)) {
        ErrLog(DSTORE_PANIC, MODULE_LOGICAL_REPLICATION, ErrMsg("Allocate space OOM, allocate RowChange fail."));
    }
    return rowChange;
}

TupleBuf* WalSortBuffer::AllocTupleBuf(Size tupleLen)
{
    TupleBuf *tupBuf;
    Size totalSize = sizeof(TupleBuf) + tupleLen;
    tupBuf = static_cast<TupleBuf *>(DstoreMemoryContextAllocZero(m_tupContext, MAXALIGN(totalSize)));
    if (unlikely(tupBuf == nullptr)) {
        ErrLog(DSTORE_PANIC, MODULE_LOGICAL_REPLICATION, ErrMsg("Allocate space OOM, allocate TupleBuf fail."));
    }
    return tupBuf;
}

void WalSortBuffer::FreeTrxChangeCtx(Xid xid)
{
    TrxChangeCtx *trxChange = GetTrxChangeCtx(xid, INVALID_END_PLSN, false);
    StorageAssert(trxChange != nullptr);
    FreeTrxChangeCtx(trxChange);
}

void WalSortBuffer::FreeTrxChangeCtx(TrxChangeCtx* trxChange)
{
    StorageAssert(trxChange->dependTxnCnt == 0);
    /* step1. clean up cache */
    if (m_lastXid == trxChange->xid) {
        m_lastXid = INVALID_XID;
        m_lastTrxCtx = nullptr;
    }

    /* step2. clean up all rowChanges in trx */
    dlist_mutable_iter iter;
    dlist_foreach_modify(iter, &trxChange->changes) {
        RowChange *rowChange = dlist_container(RowChange, node, iter.cur);
        StorageAssert(rowChange->trx == trxChange);
        FreeRowChange(rowChange);
    }

    /* check internal tableinfos has been released. */
    StorageAssert(DListIsEmpty(&trxChange->catalogTupleChanges));
    StorageAssert(DListIsEmpty(&trxChange->internalTableInfos));
    StorageAssert(DListIsEmpty(&trxChange->internalNameSpaceInfos));
    StorageAssert(DListIsEmpty(&trxChange->trxInternalDecodeTblInfos));
    StorageAssert(trxChange->internalDecodeTableInfoHash == nullptr);

    DstorePfree(trxChange);
    trxChange = nullptr;
}

void WalSortBuffer::FreeRowChange(RowChange *rowChange)
{
    /* step1. clean up tupleBuf in this row change */
    switch (rowChange->type) {
        case RowChangeType::INSERT:
        case RowChangeType::UPDATE:
        case RowChangeType::DELETE:
        case RowChangeType::CATALOG_INSERT:
        case RowChangeType::CATALOG_UPDATE:
        case RowChangeType::CATALOG_DELETE: {
            if (rowChange->data.tuple.newTuple) {
                FreeTupleBuf(rowChange->data.tuple.newTuple);
                rowChange->data.tuple.newTuple = nullptr;
            }
            if (rowChange->data.tuple.oldTuple) {
                FreeTupleBuf(rowChange->data.tuple.oldTuple);
                rowChange->data.tuple.oldTuple = nullptr;
            }
            break;
        }
        case RowChangeType::INTERNAL_SPEC_INSERT:
        case RowChangeType::INTERNAL_SPEC_CONFIRM:
        case RowChangeType::INTERNAL_SPEC_ABORT: {
            /* to support speculative */
            break;
        }
        default:
            break;
    }
    DstorePfree(rowChange);
    rowChange = nullptr;
}

void WalSortBuffer::FreeTupleBuf(TupleBuf *tupBuf) const
{
    DstorePfree(tupBuf);
}

void WalSortBuffer::UpdateCache(Xid xid, TrxChangeCtx *trxChange)
{
    StorageAssert(trxChange != nullptr);
    StorageAssert(trxChange->xid == xid);
    m_lastXid = xid;
    m_lastTrxCtx = trxChange;
}

CommitSeqNo WalSortBuffer::GetCurrentDecodedCsn() const
{
    return m_lowerBoundOfNextLogicalLogCsn - 1;
}

int WalSortBuffer::GetDecodedChangesNum() const
{
    return m_decodeNChanges;
}

void WalSortBuffer::RemoveTrxEntry(TrxChangeCtx *trxChange)
{
    bool found;
    (void)hash_search(m_txnTable, static_cast<void *>(&trxChange->xid), HASH_REMOVE, &found);
    /* min plsn trx remove, updade m_currentActiveTrxsMinPlsn */
    if (DListHeadNode(&m_toplevelByPlsn) == &trxChange->node) {
        if (DListHasNext(&m_toplevelByPlsn, &trxChange->node)) {
            TrxChangeCtx *nextTrx =
                dlist_container(TrxChangeCtx, node, DListNextNode(&m_toplevelByPlsn, &trxChange->node));
            m_currentActiveTrxsMinPlsn = nextTrx->firstPlsn;
        } else {
            m_currentActiveTrxsMinPlsn = INVALID_END_PLSN;
        }
    }
    DListDelete(&trxChange->node);
    StorageAssert(found || trxChange->xid == INVALID_XID);
}
#endif

}