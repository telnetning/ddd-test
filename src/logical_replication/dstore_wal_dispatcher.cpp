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
 * dstore_logical_decode_worker.h
 *
 * ---------------------------------------------------------------------------------------
 */

#include <thread>
#include "framework/dstore_thread.h"
#include "undo/dstore_undo_wal.h"
#include "heap/dstore_heap_wal_struct.h"
#include "port/dstore_port.h"
#include "logical_replication/dstore_logical_replication_struct.h"
#include "logical_replication/dstore_wal_dispatcher.h"

namespace DSTORE {

#ifdef ENABLE_LOGICAL_REPL
constexpr uint32 INIT_DECOMPRESS_BUFF_SIZE = 10240;

WalDispatcher::WalDispatcher(DstoreMemoryContext mctx, LogicalReplicationSlot *logicalSlot,
    DecodeOptions *decodeOpt, PdbId pdbId)
    : m_memoryContext(mctx),
      m_logicalSlot(logicalSlot),
      m_isRunningFlag(false),
      m_needStopFlag(false),
      m_workerThrd(nullptr),
      m_walReader(nullptr),
      m_walSortBuffer(nullptr),
      m_endPlsn(INVALID_END_PLSN),
      m_decodeOptions(decodeOpt),
      m_pdbId(pdbId)
{}

RetStatus WalDispatcher::Init(ParallelDecodeWorker** workerArray, int parallelDecodeWorkerNum)
{
    /* step1. Init wal reader */
    WalId walId = static_cast<uint16>(m_logicalSlot->GetWalId());
    uint64 restartPlsn = m_logicalSlot->GetRestartPlsn();
    StoragePdb *storagePdb = g_storageInstance->GetPdb(m_pdbId);
    StorageAssert(storagePdb != nullptr);
    WalStreamManager *walStreamMgr = storagePdb->GetWalMgr()->GetWalStreamManager();
    WalStream *walStream = walStreamMgr->GetWalStream(walId);
    if (walStream == nullptr) {
        ErrLog(DSTORE_PANIC, MODULE_LOGICAL_REPLICATION,
            ErrMsg("WalDispatcher Init failed for get WalStream:%lu failed", walId));
        return DSTORE_FAIL;
    }
    WalReaderConf conf = {
        walId, restartPlsn, walStream, nullptr, walStream->GetWalFileSize(), WalReadSource::WAL_READ_FROM_DISK};
    m_walReader = DstoreNew(m_memoryContext) WalRecordReader(m_memoryContext, conf);
    if (STORAGE_VAR_NULL(m_walReader) || STORAGE_FUNC_FAIL(m_walReader->Init())) {
        return DSTORE_FAIL;
    }
    ErrLog(DSTORE_DEBUG1, MODULE_LOGICAL_REPLICATION, ErrMsg("wal begin read from %lu", restartPlsn));

    /* step2. Init wal sort Buffer */
    m_walSortBuffer = DstoreNew(m_memoryContext) WalSortBuffer(m_memoryContext, m_logicalSlot,
        workerArray, parallelDecodeWorkerNum);
    if (STORAGE_VAR_NULL(m_walSortBuffer) || STORAGE_FUNC_FAIL(m_walSortBuffer->Init())) {
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

void WalDispatcher::Destroy()
{
    /*  release resources */
    if (m_walReader != nullptr) {
        delete m_walReader;
        m_walReader = nullptr;
    }

    if (m_walSortBuffer != nullptr) {
        m_walSortBuffer->Destroy();
        delete m_walSortBuffer;
        m_walSortBuffer = nullptr;
    }

    if (m_memoryContext != nullptr) {
        DstoreMemoryContextDelete(m_memoryContext);
        m_memoryContext = nullptr;
    }
}

void WalDispatcher::Run()
{
    if (IsRunning()) {
        return;
    }
    StorageAssert(m_workerThrd == nullptr);
    m_isRunningFlag.store(true);
    m_workerThrd = new std::thread(&WalDispatcher::WorkerMain, this, m_pdbId);
}

void WalDispatcher::WorkerMain(PdbId pdbId)
{
    InitSignalMask();
    (void)g_storageInstance->CreateThreadAndRegister(pdbId, false, "WalDispatcher", true,
                                                     ThreadMemoryLevel::THREADMEM_MEDIUM_PRIORITY);
    (void)pthread_setname_np(pthread_self(), "WalDispatcher");
    ErrLog(DSTORE_LOG, MODULE_LOGICAL_REPLICATION,
        ErrMsg("wal dispatcher thread start. pid: %lu", thrd->GetCore()->pid));
    AutoMemCxtSwitch autoSwitch{m_memoryContext};
    WalRecordBuf buf{m_logicalSlot->GetRestartPlsn(), m_logicalSlot->GetRestartPlsn(), INVALID_XID, nullptr};
    const WalRecordAtomicGroup *walGroup = nullptr;
    while (NeedMoreWal()) {
        /* stop sign */
        if (m_needStopFlag.load(std::memory_order_acquire)) {
            break;
        }
        if (STORAGE_FUNC_FAIL(m_walReader->ReadNext(&walGroup))) {
            ErrLog(DSTORE_PANIC, MODULE_WAL, ErrMsg("wal record reader read next group fail."));
        }
        if (unlikely(walGroup == nullptr)) {
            /* reach the end of wal */
            ErrLog(DSTORE_LOG, MODULE_LOGICAL_REPLICATION, ErrMsg("walreader reach end."));
            m_walReader->SetReadStartPlsn(buf.groupEndPlsn);
            if (!IsNewWalFlushed(buf.groupEndPlsn)) {
                ErrLog(DSTORE_LOG, MODULE_LOGICAL_REPLICATION, ErrMsg("no wal to read, sleep."));
                (void)GaussUsleep(STORAGE_USECS_PER_MSEC);
            }
            continue;
        }
        buf.xid = walGroup->xid;
        buf.groupBeginPlsn = m_walReader->GetCurGroupStartPlsn();
        buf.groupEndPlsn = m_walReader->GetCurGroupEndPlsn();
        buf.xid = walGroup->xid;
        for (uint16 i = 0; i < walGroup->recordNum; i++) {
            const WalRecord* record = m_walReader->GetNextWalRecord();
            StorageReleasePanic(record == nullptr, MODULE_LOGICAL_REPLICATION,
                                ErrMsg("WalRecord alloc memory is not enough!"));
            buf.record = record;
            if (WalRecovery::IsSupportCompress(record->GetType())) {
                BuffForDecompress buffForDecompress;
                buffForDecompress.bufferSize = INIT_TEMP_BUFF_SIZE;
                buffForDecompress.buffer = static_cast<char *>(DstorePalloc0(INIT_TEMP_BUFF_SIZE));
                buf.record = static_cast<const WalRecord *>(WalRecovery::DecompressProc(record, &buffForDecompress));
            }
            StorageAssert(buf.record != nullptr);
            StorageReleasePanic(buf.record == nullptr, MODULE_LOGICAL_REPLICATION, ErrMsg("Buf record is null!"));
            ProcessWalRecord(&buf);
            ReleaseWalDecompressedBuf(buf.record);
        }
    }
    m_isRunningFlag.store(false);
    ErrLog(DSTORE_LOG, MODULE_LOGICAL_REPLICATION,
        ErrMsg("wal dispatcher thread end. pid: %lu", thrd->GetCore()->pid));
    g_storageInstance->UnregisterThread();
}

bool WalDispatcher::IsNewWalFlushed(const WalPlsn lastPlsn)
{
    /* Get corresponding WalStream based on given walId */
    StoragePdb *storagePdb = g_storageInstance->GetPdb(m_pdbId);
    StorageAssert(storagePdb != nullptr);
    WalStreamManager *walStreamMgr = storagePdb->GetWalMgr()->GetWalStreamManager();
    WalStream *walStream = walStreamMgr->GetWalStream(m_logicalSlot->GetWalId());
    if (STORAGE_VAR_NULL(walStream)) {
        ErrLog(DSTORE_PANIC, MODULE_LOGICAL_REPLICATION, ErrMsg(
            "Wal stream with id %lu not found in function CheckNewWalFlushed", m_logicalSlot->GetWalId()));
        return false;
    }
    return (walStream->GetMaxFlushedPlsn() > lastPlsn);
}

void WalDispatcher::Stop() noexcept
{
    if (IsRunning()) {
        m_needStopFlag.store(true, std::memory_order_release);
    }

    /* wait for thread stop, even need wait it start first if thread main not running  */
    if (m_workerThrd != nullptr) {
        m_workerThrd->join();
        delete m_workerThrd;
        m_workerThrd = nullptr;
    }
}

bool WalDispatcher::NeedMoreWal() const
{
    /* never gonna stop decode until external call stop */
    if (m_endPlsn != INVALID_END_PLSN && m_walReader->GetCurGroupEndPlsn() >= m_endPlsn) {
        ErrLog(DSTORE_LOG, MODULE_LOGICAL_REPLICATION,
            ErrMsg("wal read end, last decode plsn: %lu, terminal end plsn: %lu",
                m_walReader->GetCurGroupEndPlsn(), m_endPlsn));
        return false;
    }
    return true;
}

void WalDispatcher::ReleaseWalDecompressedBuf(const WalRecord *walrecord)
{
    if (WalRecovery::IsSupportCompress(walrecord->GetType())) {
        DstorePfree(const_cast<WalRecord *>(walrecord)); /* release decompressed buffer */
    }
}

void WalDispatcher::ProcessWalRecord(WalRecordBuf *buf)
{
    switch (buf->record->GetType()) {
        case WAL_HEAP_INSERT:
        case WAL_HEAP_DELETE:
        case WAL_HEAP_INPLACE_UPDATE:
        case WAL_HEAP_SAME_PAGE_APPEND:
        case WAL_HEAP_ANOTHER_PAGE_APPEND_UPDATE_NEW_PAGE:
        case WAL_HEAP_ANOTHER_PAGE_APPEND_UPDATE_OLD_PAGE: {
            ProcessHeapOp(buf);
            break;
        }
        case WAL_TXN_COMMIT:
        case WAL_TXN_ABORT: {
            ProcessXactOp(buf);
            break;
        }
        case WAL_NEXT_CSN: {
            break;
        }
        case WAL_BARRIER_CSN: {
            ProcessWalBarrierCsn(buf);
            break;
        }
        default:
            ProcessXid(buf);
            break;
    }
}

void WalDispatcher::ProcessWalBarrierCsn(WalRecordBuf *buf)
{
    const WalBarrierCsn *record = static_cast<const WalBarrierCsn *>(buf->record);
    m_walSortBuffer->QueueFakeTrxChange(record->GetBarrierCsn(), buf->groupBeginPlsn);
}

void WalDispatcher::ProcessXid(WalRecordBuf *buf)
{
    if (buf->xid != INVALID_XID) {
        UNUSED_VARIABLE(m_walSortBuffer->GetTrxChangeCtx(buf->xid, buf->groupBeginPlsn, true));
    }
}

void WalDispatcher::ProcessHeapOp(WalRecordBuf *buf)
{
    switch (buf->record->GetType()) {
        case WAL_HEAP_INSERT:
            ParseInsert(buf);
            break;
        case WAL_HEAP_DELETE:
            ParseDelete(buf);
            break;
        case WAL_HEAP_INPLACE_UPDATE:
        case WAL_HEAP_SAME_PAGE_APPEND:
        case WAL_HEAP_ANOTHER_PAGE_APPEND_UPDATE_NEW_PAGE:
            ParseUpdate(buf);
            break;

        default:
            break;
    }
}

void WalDispatcher::ProcessXactOp(WalRecordBuf *buf)
{
    switch (buf->record->GetType()) {
        case WAL_TXN_COMMIT:
            ParseCommit(buf);
            break;
        case WAL_TXN_ABORT:
            ParseAbort(buf);
            break;
        default: {
            ErrLog(DSTORE_LOG, MODULE_LOGICAL_REPLICATION, ErrMsg("unkown xact op"));
            break;
        }
    }
}

void WalDispatcher::ParseInsert(WalRecordBuf *buf)
{
    const WalRecordHeapInsert *record = static_cast<const WalRecordHeapInsert *>(buf->record);
    if (!record->IsContainLoigcalInfo()) {
        ErrLog(DSTORE_LOG, MODULE_LOGICAL_REPLICATION, ErrMsg("heap insert walrecord has no logical info"));
        return;
    }
    RowChange *change = m_walSortBuffer->AllocRowChange();
    const HeapDiskTuple *insertTuple = record->GetInsertTuple();
    const Oid *tableOid;
    const CommitSeqNo *snapshotCsn;
    const CommandId *tupleCid;
    record->GetLogicalDecodeInfo(tableOid, snapshotCsn, tupleCid);
    StorageAssert(*tableOid != DSTORE_INVALID_OID);
    StorageAssert(*snapshotCsn != INVALID_CSN);
    TupleBuf *tupBuf;
    if (record->IsDecodeDictChange()) {
        change->type = RowChangeType::CATALOG_INSERT;
        StorageAssert(*tupleCid != INVALID_CID);
        change->data.catalogTuple.tableOid = *tableOid;
        change->data.catalogTuple.snapshotCsn = *snapshotCsn;
        change->data.catalogTuple.cid = *tupleCid;
        change->data.catalogTuple.newTuple = m_walSortBuffer->AllocTupleBuf(insertTuple->GetTupleSize());
        tupBuf = change->data.catalogTuple.newTuple;
    } else {
        change->type = RowChangeType::INSERT;
        change->data.tuple.tableOid = *tableOid;
        change->data.tuple.snapshotCsn = *snapshotCsn;
        change->data.tuple.newTuple = m_walSortBuffer->AllocTupleBuf(insertTuple->GetTupleSize());
        tupBuf = change->data.tuple.newTuple;
    }
    errno_t rc = memcpy_s(tupBuf->diskTup, insertTuple->GetTupleSize(), insertTuple, insertTuple->GetTupleSize());
    storage_securec_check(rc, "\0", "\0");
    tupBuf->memTup.SetDiskTuple(static_cast<HeapDiskTuple *>(static_cast<void *>(tupBuf->diskTup)));
    tupBuf->memTup.SetDiskTupleSize(insertTuple->GetTupleSize());
    m_walSortBuffer->QueueRowChange(buf->xid, change, buf->groupBeginPlsn);
}

void WalDispatcher::ParseDelete(WalRecordBuf *buf)
{
    const WalRecordHeapDelete *record = static_cast<const WalRecordHeapDelete *>(buf->record);
    /* do not decode delete record which not contains identity of deleted-tuple */
    if (!record->IsContainLoigcalInfo()) {
        ErrLog(DSTORE_LOG, MODULE_LOGICAL_REPLICATION, ErrMsg("heap delete walrecord has no logical info"));
        return;
    }
    RowChange *change = m_walSortBuffer->AllocRowChange();
    const HeapDiskTuple *deleteTupKey;
    const Oid *tableOid;
    const CommitSeqNo *snapshotCsn;
    const CommandId *tupleCid;
    record->GetLogicalDecodeInfo(deleteTupKey, tableOid, snapshotCsn, tupleCid);
    StorageAssert(*tableOid != DSTORE_INVALID_OID);
    StorageAssert(*snapshotCsn != INVALID_CSN);
    TupleBuf *tupBuf;
    if (record->IsDecodeDictChange()) {
        change->type = RowChangeType::CATALOG_DELETE;
        StorageAssert(*tupleCid != INVALID_CID);
        change->data.catalogTuple.tableOid = *tableOid;
        change->data.catalogTuple.snapshotCsn = *snapshotCsn;
        change->data.catalogTuple.cid = *tupleCid;
        change->data.catalogTuple.oldTuple = m_walSortBuffer->AllocTupleBuf(deleteTupKey->GetTupleSize());
        tupBuf = change->data.catalogTuple.oldTuple;
    } else {
        change->type = RowChangeType::DELETE;
        change->data.tuple.tableOid = *tableOid;
        change->data.tuple.snapshotCsn = *snapshotCsn;
        change->data.tuple.oldTuple = m_walSortBuffer->AllocTupleBuf(deleteTupKey->GetTupleSize());
        tupBuf = change->data.tuple.oldTuple;
    }
    errno_t rc = memcpy_s(tupBuf->diskTup, deleteTupKey->GetTupleSize(), deleteTupKey, deleteTupKey->GetTupleSize());
    storage_securec_check(rc, "\0", "\0");
    tupBuf->memTup.SetDiskTuple(static_cast<HeapDiskTuple *>(static_cast<void *>(tupBuf->diskTup)));
    tupBuf->memTup.SetDiskTupleSize(deleteTupKey->GetTupleSize());
    m_walSortBuffer->QueueRowChange(buf->xid, change, buf->groupBeginPlsn);
}

void WalDispatcher::ParseUpdate(WalRecordBuf *buf)
{
    /* do not decode update record which not contains identity of old-tuple */
    const WalRecordHeap *record = static_cast<const WalRecordHeap *>(buf->record);
    if (!record->IsContainLoigcalInfo()) {
        ErrLog(DSTORE_LOG, MODULE_LOGICAL_REPLICATION, ErrMsg("heap delete walrecord has no logical info"));
        return;
    }
    RowChange *change = m_walSortBuffer->AllocRowChange();
    const Oid *tableOid;
    const CommitSeqNo *snapshotCsn;
    const CommandId *tupleCid;
    uint32 oldTupLen, newTupLen;
    TupleBuf *oldTupBuf, *newTupBuf;
    switch (buf->record->GetType()) {
        case WAL_HEAP_INPLACE_UPDATE: {
            const WalRecordHeapInplaceUpdate *inplaceUpdate = static_cast<const WalRecordHeapInplaceUpdate *>(record);
            inplaceUpdate->GetUpdateTupleLen(oldTupLen, newTupLen);
            oldTupBuf = m_walSortBuffer->AllocTupleBuf(oldTupLen);
            newTupBuf = m_walSortBuffer->AllocTupleBuf(newTupLen);
            inplaceUpdate->GetUpdateTuple(static_cast<HeapDiskTuple *>(static_cast<void *>(oldTupBuf->diskTup)),
                                          static_cast<HeapDiskTuple *>(static_cast<void *>(newTupBuf->diskTup)));
            inplaceUpdate->GetLogicalDecodeInfo(tableOid, snapshotCsn, tupleCid);
            break;
        }
        case WAL_HEAP_SAME_PAGE_APPEND: {
            const WalRecordHeapSamePageAppendUpdate *samePageUpdateRec =
                static_cast<const WalRecordHeapSamePageAppendUpdate *>(record);
            samePageUpdateRec->GetUpdateTupleLen(oldTupLen, newTupLen);
            oldTupBuf = m_walSortBuffer->AllocTupleBuf(oldTupLen);
            newTupBuf = m_walSortBuffer->AllocTupleBuf(newTupLen);
            samePageUpdateRec->GetUpdateTuple(static_cast<HeapDiskTuple *>(static_cast<void *>(oldTupBuf->diskTup)),
                                              static_cast<HeapDiskTuple *>(static_cast<void *>(newTupBuf->diskTup)));
            samePageUpdateRec->GetLogicalDecodeInfo(tableOid, snapshotCsn, tupleCid);
            break;
        }
        case WAL_HEAP_ANOTHER_PAGE_APPEND_UPDATE_NEW_PAGE: {
            const WalRecordHeapAnotherPageAppendUpdateNewPage *newPageUpdateRec =
                static_cast<const WalRecordHeapAnotherPageAppendUpdateNewPage *>(record);
            newPageUpdateRec->GetUpdateTupleLen(oldTupLen, newTupLen);
            oldTupBuf = m_walSortBuffer->AllocTupleBuf(oldTupLen);
            newTupBuf = m_walSortBuffer->AllocTupleBuf(newTupLen);
            newPageUpdateRec->GetUpdateTuple(static_cast<HeapDiskTuple *>(static_cast<void *>(oldTupBuf->diskTup)),
                                             static_cast<HeapDiskTuple *>(static_cast<void *>(newTupBuf->diskTup)));
            newPageUpdateRec->GetLogicalDecodeInfo(tableOid, snapshotCsn, tupleCid);
            break;
        }
        default: {
            ErrLog(DSTORE_LOG, MODULE_LOGICAL_REPLICATION, ErrMsg("unkown heap update"));
            return;
        }
    }
    if (unlikely(record->IsDecodeDictChange())) {
        StorageAssert(tupleCid != nullptr);
        change->type = RowChangeType::CATALOG_UPDATE;
        change->data.catalogTuple.tableOid = *tableOid;
        change->data.catalogTuple.snapshotCsn = *snapshotCsn;
        change->data.catalogTuple.cid = *tupleCid;
        change->data.catalogTuple.oldTuple = oldTupBuf;
        change->data.catalogTuple.newTuple = newTupBuf;
    } else {
        change->type = RowChangeType::UPDATE;
        change->data.tuple.tableOid = *tableOid;
        change->data.tuple.snapshotCsn = *snapshotCsn;
        change->data.tuple.oldTuple = oldTupBuf;
        change->data.tuple.newTuple = newTupBuf;
    }
    oldTupBuf->memTup.SetDiskTuple(static_cast<HeapDiskTuple *>(static_cast<void *>(oldTupBuf->diskTup)));
    oldTupBuf->memTup.SetDiskTupleSize(oldTupLen);
    newTupBuf->memTup.SetDiskTuple(static_cast<HeapDiskTuple *>(static_cast<void *>(newTupBuf->diskTup)));
    newTupBuf->memTup.SetDiskTupleSize(newTupLen);
    m_walSortBuffer->QueueRowChange(buf->xid, change, buf->groupBeginPlsn);
}

void WalDispatcher::ParseCommit(WalRecordBuf *buf)
{
    StorageAssert(buf->record->GetType() == WAL_TXN_COMMIT);
    const WalRecordTransactionCommit *record = static_cast<const WalRecordTransactionCommit *>(buf->record);
    TrxSlotStatus status = record->GetTrxSlotStatus();
    CommitSeqNo csn = record->GetCsn();
    if (status == TXN_STATUS_PENDING_COMMIT) {
        m_walSortBuffer->TrxCommitting(buf->xid, csn);
    } else {
        StorageAssert(status == TXN_STATUS_COMMITTED);
        m_walSortBuffer->TrxCommit(buf->xid, csn, buf->groupEndPlsn);
    }
}

void WalDispatcher::ParseAbort(WalRecordBuf *buf)
{
    StorageAssert(buf->record->GetType() == WAL_TXN_ABORT);
    m_walSortBuffer->TrxAbort(buf->xid);
}
#endif

}