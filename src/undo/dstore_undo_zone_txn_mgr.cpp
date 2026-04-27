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
 * dstore_undo_zone_txn_mgr.cpp
 *
 * IDENTIFICATION
 *        src/undo/dstore_undo_zone_txn_mgr.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "common/instrument/trace/dstore_undo_trace.h"
#include "common/log/dstore_log.h"
#include "transaction/dstore_csn_mgr.h"
#include "transaction/dstore_transaction.h"
#include "transaction/dstore_transaction_mgr.h"
#include "wal/dstore_wal_write_context.h"
#include "undo/dstore_undo_wal.h"
#include "undo/dstore_undo_zone_txn_mgr.h"

namespace DSTORE {

UndoZoneTrxManager::~UndoZoneTrxManager() noexcept
{
    m_segment = nullptr;
    m_bufferMgr = nullptr;
}

RetStatus UndoZoneTrxManager::Init(const PageId &startPageId, bool initPage)
{
    StorageAssert(IS_VALID_ZONE_ID(m_zoneId));
    SetStartPageId(startPageId);
    PageId tmpPageId = m_segment->GetSegmentMetaPageId();
    /* Init transaction slot pages if needed */
    if (initPage) {
        for (uint32 i = 0; i < TRX_PAGES_PER_ZONE; i++) {
            tmpPageId.m_blockId++;
            if (STORAGE_FUNC_FAIL(InitSlotPage(tmpPageId))) {
                return DSTORE_FAIL;
            }
        }
    }
    return DSTORE_SUCC;
}

UndoRecPtr UndoZoneTrxManager::Restore(TailUndoPtrStatus &tailPtrStatus)
{
    BufferDesc *txnSlotPageBuf = nullptr;
    TransactionSlotPage *txnSlotPage = nullptr;
    uint64 txnSlotPageGlsn = INVALID_WAL_GLSN;
    uint64 txnSlotPagePlsn = INVALID_END_PLSN;
    WalId txnSlotPageWalId = INVALID_WAL_ID;
    uint64 bufState = UINT64_MAX;
    bool isBreak = false;

    /* Step 1: restore m_nextFreeLogicSlotId */
    m_nextFreeLogicSlotId.store(0, std::memory_order_release);
    for (uint32 i = 0; i < TRX_PAGES_PER_ZONE; ++i) {
        PageId curId = {m_startPage.m_fileId, m_startPage.m_blockId + i};
        txnSlotPageBuf = ReadTxnSlotPageBuf(curId, LW_SHARED);
        txnSlotPage = GetTxnSlotPage(txnSlotPageBuf);
        uint64 logicSlotIdOnPage = txnSlotPage->GetNextFreeLogicSlotId();
        if (logicSlotIdOnPage <= m_nextFreeLogicSlotId.load(std::memory_order_acquire)) {
            isBreak = true;
            break;
        }
        txnSlotPageGlsn = txnSlotPage->GetGlsn();
        txnSlotPagePlsn = txnSlotPage->GetPlsn();
        txnSlotPageWalId = txnSlotPage->GetWalId();
        bufState = txnSlotPageBuf->GetState();
        UnlockAndReleaseTxnSlotPageBuf(txnSlotPageBuf);
        m_nextFreeLogicSlotId.store(logicSlotIdOnPage, std::memory_order_release);
    }
    if (isBreak) {
        UnlockAndReleaseTxnSlotPageBuf(txnSlotPageBuf);
    }

    /* Step 2: need check read authorization for slot page */
    PageId tmpId = GetTxnSlotPageId(m_nextFreeLogicSlotId.load(std::memory_order_acquire));
    txnSlotPageBuf = ReadTxnSlotPageBuf(tmpId, LW_EXCLUSIVE);
    txnSlotPage = GetTxnSlotPage(txnSlotPageBuf);
    uint16 nextFreeSlotId = TransactionSlotPage::GetSlotId(m_nextFreeLogicSlotId.load(std::memory_order_acquire));
    TransactionSlot *slot = txnSlotPage->GetTransactionSlot(nextFreeSlotId);
    if (unlikely(slot->logicSlotId != 0 &&
                 slot->logicSlotId >= m_nextFreeLogicSlotId.load(std::memory_order_acquire))) {
        /* dump page that not read authorization */
        char *dumpPage = txnSlotPage->Dump();
        TransactionSlotPage::PrevDumpPage(dumpPage);
        DstorePfreeExt(dumpPage);
        ErrLog(DSTORE_ERROR, MODULE_UNDO,
               ErrMsg("Check logic slot id failed, zondId(%lu), nextFreeSlotId(%lu), read page walId:%lu, read page "
                      "glsn:%lu, read page plsn:%lu, oldState(%lu), newState(%lu), pdbId(%u).",
                      static_cast<uint64>(m_zoneId), txnSlotPageWalId, txnSlotPageGlsn, txnSlotPagePlsn,
                      m_nextFreeLogicSlotId.load(std::memory_order_acquire), bufState, txnSlotPageBuf->GetState(),
                      m_pdbId));
        StorageReleaseBufferCheckPanic(true, MODULE_TRANSACTION, txnSlotPageBuf->GetBufferTag(),
                                       "Check logic slot id failed when restore undo zone");
    }
    UnlockAndReleaseTxnSlotPageBuf(txnSlotPageBuf);

    /* Step 3: restore m_recycleLogicSlotId */
    uint32 i;
    for (i = 0; i < TRX_PAGES_PER_ZONE; ++i) {
        txnSlotPageBuf = ReadTxnSlotPageBuf(tmpId, LW_SHARED);
        txnSlotPage = GetTxnSlotPage(txnSlotPageBuf);
        TrxSlotStatus status = txnSlotPage->GetTransactionSlot(TRX_PAGE_SLOTS_NUM - 1)->status;
        if (status != TXN_STATUS_UNKNOWN && status != TXN_STATUS_FROZEN) {
            break;
        }
        UnlockAndReleaseTxnSlotPageBuf(txnSlotPageBuf);
        AdvanceTrxSlotPageId(tmpId);
    }

    uint64 nextFreeLogicSlotId = m_nextFreeLogicSlotId.load(std::memory_order_acquire);
    uint64 recycleLogicSlotId;
    if (i == 0) {
        /* recycleSlotId and nextFreeSlotId are on the same page, and recycleSlotId is after nextFreeSlotId. */
        uint16 slotId = TransactionSlotPage::GetSlotId(nextFreeLogicSlotId);
        recycleLogicSlotId = txnSlotPage->GetTransactionSlot(slotId)->logicSlotId;
        UnlockAndReleaseTxnSlotPageBuf(txnSlotPageBuf);
    } else if (i == TRX_PAGES_PER_ZONE) {
        /* recycleSlotId and nextFreeSlotId are on the same page, and recycleSlotId is before nextFreeSlotId. */
        recycleLogicSlotId = nextFreeLogicSlotId - (nextFreeLogicSlotId % TRX_PAGE_SLOTS_NUM);
    } else {
        /* recycleSlotId and nextFreeSlotId are not on the same page. */
        recycleLogicSlotId = txnSlotPage->GetTransactionSlot(0)->logicSlotId;
        UnlockAndReleaseTxnSlotPageBuf(txnSlotPageBuf);
    }
    m_recycleLogicSlotId.store(recycleLogicSlotId, std::memory_order_relaxed);

    /* Step 4: recycle */
    CommitSeqNo recycleMinCsn = g_storageInstance->GetCsnMgr()->GetRecycleCsnMin(m_pdbId);
    UndoRecPtr restoreUndoPtr = INVALID_UNDO_RECORD_PTR;
    UndoRecPtr recycleUndoPtr = INVALID_UNDO_RECORD_PTR;
    RecycleTxnSlots(recycleMinCsn, restoreUndoPtr, recycleUndoPtr, true, tailPtrStatus);
    ErrLog(DSTORE_LOG, MODULE_UNDO,
           ErrMsg("Undo zone zid = %d, trx slot restore, recycle csn min = %lu, "
                  "restoreUndoPtr(%hu, %u, %hu), recycleUndoPtr(%hu, %u, %hu), tailPtrStatus(%hhu), pdbId(%u).",
                  m_zoneId, recycleMinCsn, restoreUndoPtr.GetFileId(), restoreUndoPtr.GetBlockNum(),
                  restoreUndoPtr.GetOffset(), recycleUndoPtr.GetFileId(), recycleUndoPtr.GetBlockNum(),
                  recycleUndoPtr.GetOffset(), tailPtrStatus, m_pdbId));
    return restoreUndoPtr;
}

Xid UndoZoneTrxManager::AllocSlot()
{
    AtomicWalWriterContext *walContext = thrd->m_walWriterContext;

    /* Step 1: Check if the zone has free slot. */
    if (!HasFreeSlot()) {
        ErrLog(DSTORE_DEBUG1, MODULE_UNDO,
               ErrMsg("No free transaction slot page to use in undo zone(zid=%d)", m_zoneId));
        return INVALID_XID;
    }

    /* Step 2: Get next free slot page. */
    uint64 nextFreeLogicSlotId = m_nextFreeLogicSlotId.load(std::memory_order_acquire);
    StorageAssert(g_storageInstance->GetPdb(m_pdbId)->GetUndoMgr()->IsZoneOwned(m_zoneId));
    PageId nextFreeSlotPageId = GetTxnSlotPageId(nextFreeLogicSlotId);
    StorageReleasePanic(GetStartPageId() == INVALID_PAGE_ID, MODULE_UNDO,
                        ErrMsg("invalid start pageid when alloc slot."));
    if (unlikely(STORAGE_FUNC_FAIL(PinAndLockTxnSlotBuf(nextFreeSlotPageId)))) {
        ErrLog(DSTORE_ERROR, MODULE_UNDO, ErrMsg("AllocSlot PinAndLockTxnSlotBuf fail. "
            "nextFreeSlotPageId is (%hu, %u)", nextFreeSlotPageId.m_fileId, nextFreeSlotPageId.m_blockId));
        return INVALID_XID;
    }
    TransactionSlotPage *txnSlotPage = GetTxnSlotPage(m_currentTxnSlotBuf);

    /* Step 3: Init Slot */
    uint32 slotId = TransactionSlotPage::GetSlotId(nextFreeLogicSlotId);
    TransactionSlot *slot = txnSlotPage->GetTransactionSlot(slotId);
    slot->Init(nextFreeLogicSlotId);

    /* Step 4: Advance next free xid. */
    nextFreeLogicSlotId = m_nextFreeLogicSlotId.fetch_add(1, std::memory_order_release);
    nextFreeLogicSlotId++;
    txnSlotPage->SetNextFreeLogicSlotId(nextFreeLogicSlotId);
    (void)m_bufferMgr->MarkDirty(m_currentTxnSlotBuf);

    /* Step 5: WAL. */
    Xid curXid = {static_cast<uint32>(m_zoneId), nextFreeLogicSlotId - 1};
    walContext->BeginAtomicWal(curXid);
    walContext->RememberPageNeedWal(m_currentTxnSlotBuf);
    uint32 allocSize = static_cast<uint32>(sizeof(WalRecordUndoTxnSlotAllocate));
    WalRecordUndoTxnSlotAllocate allocRedoData;
    bool glsnChangedFlag = (txnSlotPage->GetWalId() != walContext->GetWalId());
    allocRedoData.SetHeader({WAL_UNDO_ALLOCATE_TXN_SLOT, allocSize, m_currentTxnSlotBuf->GetPageId(),
        txnSlotPage->GetWalId(), txnSlotPage->GetPlsn(), txnSlotPage->GetGlsn(), glsnChangedFlag,
        m_currentTxnSlotBuf->GetFileVersion()});
    walContext->PutNewWalRecord(&allocRedoData);

    UnlockAndReleaseTxnSlotPageBuf(m_currentTxnSlotBuf);

    (void)walContext->EndAtomicWal();
#ifdef DSTORE_USE_ASSERT_CHECKING
    ErrLog(DSTORE_DEBUG1, MODULE_UNDO, ErrMsg("Undo zone zid = %d, alloc slot %lu success.",
        m_zoneId, curXid.m_logicSlotId));
#endif
    return curXid;
}

RetStatus UndoZoneTrxManager::InitSlotPage(const PageId &slotBlk)
{
    BufferDesc *bufferDesc = m_bufferMgr->Read(m_pdbId, slotBlk, LW_EXCLUSIVE);
    if (unlikely((bufferDesc) == INVALID_BUFFER_DESC)) {
        ErrLog(DSTORE_ERROR, MODULE_UNDO,
            ErrMsg("Buffer(%hu, %u) invalid when InitSlotPage.", slotBlk.m_fileId, slotBlk.m_blockId));
        return DSTORE_FAIL;
    }
    TransactionSlotPage *trxSlotPage = static_cast<TransactionSlotPage*>(bufferDesc->GetPage());
    trxSlotPage->InitTxnSlotPage(slotBlk);
    (void)m_bufferMgr->MarkDirty(bufferDesc);
    /* All transaction slot pages must be pinned in buffer pool. */
    AtomicWalWriterContext *walContext = thrd->m_walWriterContext;
    StorageAssert(walContext->HasAlreadyBegin());
    walContext->RememberPageNeedWal(bufferDesc);
    uint32 size = static_cast<uint32>(sizeof(WalRecordUndoInitTxnPage));
    WalRecordUndoInitTxnPage redoData;
    bool glsnChangedFlag = (trxSlotPage->GetWalId() != walContext->GetWalId());
    redoData.SetHeader({WAL_UNDO_INIT_TXN_PAGE, size, bufferDesc->GetPageId(), trxSlotPage->GetWalId(),
        trxSlotPage->GetPlsn(), trxSlotPage->GetGlsn(), glsnChangedFlag, bufferDesc->GetFileVersion()});
    walContext->PutNewWalRecord(&redoData);
    m_bufferMgr->UnlockAndRelease(bufferDesc);
    ErrLog(DSTORE_DEBUG1, MODULE_UNDO, ErrMsg("Undo zone zid = %d, init slot page(%hu, %u).",
        m_zoneId, slotBlk.m_fileId, slotBlk.m_blockId));
    return DSTORE_SUCC;
}

bool UndoZoneTrxManager::IsSlotRecyclable(uint64 logicSlotId, CommitSeqNo recycleMinCsn, UndoRecPtr &tailUndoPtr,
    TransactionSlotPage *txnPage, bool &nextToBeRecycledIsCommitted)
{
    PageId slotPageId = GetTxnSlotPageId(logicSlotId);
    BufferDesc *txnSlotPageBuf = nullptr;
    uint32 slotId = TransactionSlotPage::GetSlotId(logicSlotId);
    CommitSeqNo slotCsn = INVALID_CSN;
    TrxSlotStatus slotStatus = TXN_STATUS_UNKNOWN;
    UndoRecPtr spaceTailUndoPtr = INVALID_UNDO_RECORD_PTR;
    bool needLock = (txnPage == nullptr);

    /* Read page out if we haven't lock it yet */
    if (needLock) {
        txnSlotPageBuf = ReadTxnSlotPageBuf(slotPageId, LW_SHARED);
        txnPage = GetTxnSlotPage(txnSlotPageBuf);
    }

    /* Read the info on the page now */
     if (unlikely(txnPage == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_UNDO,
               ErrMsg("TxnPage is nullptr, pageId(%hu, %u), zid(%d), pdbId[%u]", slotPageId.m_fileId,
                      slotPageId.m_blockId, m_zoneId, m_pdbId));
        if (needLock) {
            UnlockAndReleaseTxnSlotPageBuf(txnSlotPageBuf);
        }
        return false;
    }
    slotCsn = txnPage->GetTransactionSlot(slotId)->GetCsn();
    slotStatus = txnPage->GetTransactionSlot(slotId)->GetTxnSlotStatus();
    spaceTailUndoPtr = txnPage->GetTransactionSlot(slotId)->GetSpaceTailUndoPtr();

    if (needLock) {
        UnlockAndReleaseTxnSlotPageBuf(txnSlotPageBuf);
    }

    /* if slotCsn is INVALID_CSN and slotStatus is TXN_STATUS_IN_PROGRESS, mean this slot is a null transaction slot,
     * no undo record of this transaction has insert to the undo page, so you can recycle this slot.
     */
    if (unlikely(slotStatus == TXN_STATUS_IN_PROGRESS) ||
        unlikely(slotStatus == TXN_STATUS_PENDING_COMMIT)) {
        tailUndoPtr = spaceTailUndoPtr;
        return false;
    }

    if (unlikely(slotCsn == INVALID_CSN) || slotCsn < recycleMinCsn) {
        return true;
    }

    tailUndoPtr = spaceTailUndoPtr;
    nextToBeRecycledIsCommitted = true;
    return false;
}

bool UndoZoneTrxManager::IsXidRecycled(PdbId pdbId, Xid xid, CommitSeqNo* commitCsn)
{
    bool recycled = false;
    TransactionSlot trxSlot;
    StorageAssert(g_storageInstance->GetPdb(pdbId) != nullptr);
    TransactionMgr *transactionMgr = g_storageInstance->GetPdb(pdbId)->GetTransactionMgr();
Retry:
    if (STORAGE_FUNC_FAIL(transactionMgr->GetTransactionSlotCopy(pdbId, xid, trxSlot))) {
        ErrLog(DSTORE_WARNING, MODULE_UNDO,
               ErrMsg("Get Undo zone zid %lu, trx slot copy Failed, logicslotid(%lu).",
                   static_cast<uint64>(xid.m_zoneId), xid.m_logicSlotId));
        goto Retry;
    }

    if (trxSlot.status == TXN_STATUS_FROZEN || trxSlot.logicSlotId != xid.m_logicSlotId) {
        recycled = true;
    } else if (trxSlot.status == TXN_STATUS_COMMITTED) {
        /* Because undo recycle does not generate WAL, the slot may be frozen before the node fails. */
        CommitSeqNo recycleCsn = g_storageInstance->GetCsnMgr()->GetRecycleCsnMin(pdbId);
        StoragePdb *pdb = g_storageInstance->GetPdb(pdbId);
        if (pdb != nullptr && pdb->GetNeedRollbackBarrierInFailover() &&
            pdb->GetRollbackBarrierCsnInFailover() != INVALID_CSN) {
            recycleCsn = pdb->GetRollbackBarrierCsnInFailover();
        }
        recycled = trxSlot.GetCsn() < recycleCsn;
        if (!recycled && commitCsn != nullptr) {
            *commitCsn = trxSlot.GetCsn();
        }
    }

    return recycled;
}

bool UndoZoneTrxManager::IsUndoZoneNeedRollback(Xid &rollbackXid)
{
    uint64 nextFreeLogicSlotId = m_nextFreeLogicSlotId.load(std::memory_order_relaxed);
    uint64 recycleLogicSlotId = m_recycleLogicSlotId.load(std::memory_order_relaxed);
    if (nextFreeLogicSlotId == 0 || recycleLogicSlotId == nextFreeLogicSlotId) {
        return false;
    }

    PageId lastPageId = GetTxnSlotPageId(nextFreeLogicSlotId - 1);
    BufferDesc *txnSlotPageBuf = ReadTxnSlotPageBuf(lastPageId, LW_SHARED);
    TransactionSlotPage *txnSlotPage = GetTxnSlotPage(txnSlotPageBuf);
    uint32 nextSlotId = TransactionSlotPage::GetSlotId(nextFreeLogicSlotId);
    uint32 lastSlotId = ((nextSlotId != 0) ? (nextSlotId - 1) : (TRX_PAGE_SLOTS_NUM - 1));
    bool ret = false;
    TransactionSlot *lastSlot = txnSlotPage->GetTransactionSlot(lastSlotId);
    if (lastSlot->status == TXN_STATUS_IN_PROGRESS || lastSlot->status == TXN_STATUS_PENDING_COMMIT) {
        ret = true;
        rollbackXid = {static_cast<uint32>(m_zoneId), nextFreeLogicSlotId - 1};
    }
    UnlockAndReleaseTxnSlotPageBuf(txnSlotPageBuf);
    return ret;
}

void UndoZoneTrxManager::CopySlot(Xid xid, TransactionSlot &trxSlot)
{
    storage_trace_entry(TRACE_ID_UndoZone__GetTransactionSlotCopy);
    StorageAssert(m_zoneId == static_cast<ZoneId>(xid.m_zoneId));

    PageId pageId = GetTxnSlotPageId(xid.m_logicSlotId);
    BufferDesc *targetSlotPageBuf = ReadTxnSlotPageBuf(pageId, LW_SHARED);
    TransactionSlotPage *targetSlotPage = GetTxnSlotPage(targetSlotPageBuf);
    uint32 slotId = TransactionSlotPage::GetSlotId(xid.m_logicSlotId);

    TransactionSlot *tmp = targetSlotPage->GetTransactionSlot(slotId);
    if (tmp->status == TXN_STATUS_FROZEN || tmp->logicSlotId != xid.m_logicSlotId) {
        /* Transaction slot location has been recycled */
        trxSlot.status = TXN_STATUS_FROZEN;
        trxSlot.csn = INVALID_CSN;
    } else {
        trxSlot = *tmp;
    }
    UnlockAndReleaseTxnSlotPageBuf(targetSlotPageBuf);
    /*
     * If commit wal not persis yet, we can't use TXN_STATUS_COMMITTED to judge visibility.
     * See details in COMMIT_LOGIC_TAG.
     */
    if (trxSlot.status == TXN_STATUS_COMMITTED && !trxSlot.IsCommitWalPersist(m_pdbId)) {
        trxSlot.status = TXN_STATUS_PENDING_COMMIT;
    }
    storage_trace_exit(TRACE_ID_UndoZone__GetTransactionSlotCopy);
}
constexpr int MAX_BEHIND_ROUND = 2;

RetStatus UndoZoneTrxManager::PinAndLockTxnSlotBuf(PageId pageId)
{
    if (unlikely(m_currentTxnSlotBuf == INVALID_BUFFER_DESC)) {
        m_currentTxnSlotBuf = ReadTxnSlotPageBuf(pageId, LW_EXCLUSIVE);
    } else {
        m_currentTxnSlotBuf->Pin();
        if (m_currentTxnSlotBuf->GetPageId() != pageId) {
            m_currentTxnSlotBuf->Unpin();
            m_currentTxnSlotBuf = ReadTxnSlotPageBuf(pageId, LW_EXCLUSIVE);
        } else {
            if (unlikely(STORAGE_FUNC_FAIL(m_bufferMgr->LockContent(m_currentTxnSlotBuf, LW_EXCLUSIVE)))) {
                ErrLog(DSTORE_ERROR, MODULE_UNDO,
                    ErrMsg("PinAndLockTxnSlotBuf fail when LockContent, pageId is (%hu, %u).",
                    pageId.m_fileId, pageId.m_blockId));
                m_currentTxnSlotBuf->Unpin();
                return DSTORE_FAIL;
            }
        }
    }
    return DSTORE_SUCC;
}

template void UndoZoneTrxManager::Commit<TrxSlotStatus::TXN_STATUS_PENDING_COMMIT>(Xid xid, CommitSeqNo& csn,
    bool isSpecialAyncCommitAutoTrx);
template void UndoZoneTrxManager::Commit<TrxSlotStatus::TXN_STATUS_COMMITTED>(Xid xid, CommitSeqNo& csn,
    bool isSpecialAyncCommitAutoTrx);
template<TrxSlotStatus status>
void UndoZoneTrxManager::Commit(Xid xid, CommitSeqNo& csn, bool isSpecialAyncCommitAutoTrx)
{
    StoragePdb *storagePdb = g_storageInstance->GetPdb(m_pdbId);
    if (unlikely((storagePdb == nullptr) || (storagePdb->GetUndoMgr() == nullptr))) {
        ErrLog(DSTORE_PANIC, MODULE_BUFMGR,
               ErrMsg("StoragePdb or undomgr is nullptr when commit xid(%d, %lu), pdbId(%u).",
                      static_cast<int>(xid.m_zoneId), xid.m_logicSlotId, m_pdbId));
    }
    StorageAssert(storagePdb->GetUndoMgr()->IsZoneOwned(m_zoneId));
    /* Do not use g_storageInstance->GetGuc()))->csnMode here due to CsnMode::DEFAULT */
    CsnMgr *csnMgr = g_storageInstance->GetCsnMgr();
    CsnMode csnMode = csnMgr->GetCsnMode();
    if (status == TXN_STATUS_PENDING_COMMIT) {
        if (STORAGE_FUNC_FAIL(csnMgr->GetNextCsn(csn, false))) {
            return;
        }
    }
    if (status == TXN_STATUS_COMMITTED) {
        if (STORAGE_FUNC_FAIL(csnMgr->GetNextCsn(csn, true))) {
            return;
        }
    }
    PageId pageId = GetTxnSlotPageId(xid.m_logicSlotId);
    if (unlikely(STORAGE_FUNC_FAIL(PinAndLockTxnSlotBuf(pageId)))) {
        ErrLog(DSTORE_ERROR, MODULE_UNDO, ErrMsg("UndoZoneTrxManager::Commit PinAndLockTxnSlotBuf fail. "
            "pageId is (%hu, %u)", pageId.m_fileId, pageId.m_blockId));
        return;
    }
    TransactionSlotPage *txnSlotPage = GetTxnSlotPage(m_currentTxnSlotBuf);
    uint16 slotId = TransactionSlotPage::GetSlotId(xid.m_logicSlotId);
    TransactionSlot *slot = txnSlotPage->GetTransactionSlot(slotId);
    /* When Csn Broadcast thread is sleeping and the first transaction is coming, We need to wake it up for collecting
     * csn. */
    if (status == TXN_STATUS_PENDING_COMMIT) {
        csnMgr->WakeUpCsnBroadcast();
    }
    /* For lamport clock mode, in order to optimizing the performance, we get the lower bound csn after obtaining the
     * lock to shorten the waiting time */
    if (csnMode == CsnMode::BROADCAST) {
        if (status == TXN_STATUS_PENDING_COMMIT) {
            thrd->GetXact()->toBeCommitedCsn.store(COMMITSEQNO_FIRST_NORMAL, std::memory_order_release);
            slot->SetCsn(csn);
            thrd->GetXact()->toBeCommitedCsn.store(csn, std::memory_order_release);
        }
    } else {
        slot->SetCsn(csn);
    }
    slot->SetTrxSlotStatus(status);
    (void)m_bufferMgr->MarkDirty(m_currentTxnSlotBuf);

    AtomicWalWriterContext *walContext = thrd->m_walWriterContext;
    walContext->BeginAtomicWal(xid);
    walContext->RememberPageNeedWal(m_currentTxnSlotBuf);
    uint32 size = static_cast<uint32>(sizeof(WalRecordTransactionCommit));
    WalRecordTransactionCommit redoData;
    bool glsnChangedFlag = (txnSlotPage->GetWalId() != walContext->GetWalId());
    uint64 fileVersion = m_currentTxnSlotBuf->GetFileVersion();
    redoData.SetHeader({WAL_TXN_COMMIT, size, m_currentTxnSlotBuf->GetPageId(), txnSlotPage->GetWalId(),
        txnSlotPage->GetPlsn(), txnSlotPage->GetGlsn(), glsnChangedFlag, fileVersion});
    redoData.SetSlotId(slotId);
    redoData.SetCsn(csn);
    redoData.SetTrxSlotStatus(status);
    redoData.SetCurrentCommitTime();
    walContext->PutNewWalRecord(&redoData);
    WalGroupLsnInfo walGroupPtr = walContext->EndAtomicWal();
#ifndef UT
    StorageAssert(slot->walId == INVALID_WAL_ID);
    StorageAssert(slot->commitEndPlsn == INVALID_PLSN);
#endif
    if (status == TXN_STATUS_COMMITTED) {
        slot->SetWalInfo(walGroupPtr.m_walId, walGroupPtr.m_endPlsn);
    }
    /*
     * COMMIT_LOGIC_TAG: For performance reasons, we don't wait pending commit wal persist here, rather backend check
     * whether the commit information is visible by comparing commit wal endplsn with max flushed plsn.
     * The problem solved by the modification: trx commit info is used to judge visibility before wal persist,
     * if a failure occurs at this time, recovery process will set the transaction status to aborted, which causes
     * non-consistent.
     */
    UnlockAndReleaseTxnSlotPageBuf(m_currentTxnSlotBuf);

    if (status == TXN_STATUS_COMMITTED) {
        walContext->ThrottleIfNeed();
        /* In Asynchronous commit mode, we don't need to wait plsn persist and csn updating */
        if (!isSpecialAyncCommitAutoTrx) {
            walContext->WaitTargetPlsnPersist(walGroupPtr);
            /* For lamport clock mode, in order to optimizing the performance, we get the upper bound csn as late as
             * possible to shorten the waiting time */
            csnMgr->WaitUpperBoundSatisfy(csn);
        }
        thrd->GetXact()->toBeCommitedCsn.store(INVALID_CSN, std::memory_order_release);
#ifdef DSTORE_USE_ASSERT_CHECKING
        ErrLog(DSTORE_DEBUG1, MODULE_UNDO,
               ErrMsg("Xid(%lu, %lu) commit csn %lu and persist wal info is "
                      "walId %lu startPlsn %lu endPlsn %lu)",
                      (uint64_t)xid.m_zoneId, xid.m_logicSlotId, csn, walGroupPtr.m_walId, walGroupPtr.m_startPlsn,
                      walGroupPtr.m_endPlsn));
#endif
    }
    g_storageInstance->GetPdb(m_pdbId)->GetUndoMgr()->WriteTxnInfoToCache(xid, *slot, slot->csn);
}

RetStatus UndoZoneTrxManager::SetSlotUndoPtr(Xid xid, UndoRecPtr undoRecPtr, bool insertUndoFlag)
{
    PageId pageId = GetTxnSlotPageId(xid.m_logicSlotId);
    uint16 slotId = TransactionSlotPage::GetSlotId(xid.m_logicSlotId);

    StorageAssert(g_storageInstance->GetPdb(m_pdbId)->GetUndoMgr()->IsZoneOwned(m_zoneId));
    if (unlikely(STORAGE_FUNC_FAIL(PinAndLockTxnSlotBuf(pageId)))) {
        ErrLog(DSTORE_ERROR, MODULE_UNDO, ErrMsg("SetSlotUndoPtr PinAndLockTxnSlotBuf fail. "
            "pageId is (%hu, %u)", pageId.m_fileId, pageId.m_blockId));
        return DSTORE_FAIL;
    }
    TransactionSlotPage *txnSlotPage = GetTxnSlotPage(m_currentTxnSlotBuf);
    TransactionSlot *slot = txnSlotPage->GetTransactionSlot(slotId);

    slot->SetCurTailUndoPtr(undoRecPtr);
    if (insertUndoFlag) {
        slot->SetSpaceTailUndoPtr(undoRecPtr);
    }
    (void)m_bufferMgr->MarkDirty(m_currentTxnSlotBuf);

    AtomicWalWriterContext *walContext = thrd->m_walWriterContext;
    walContext->RememberPageNeedWal(m_currentTxnSlotBuf);
    StorageAssert(walContext->HasAlreadyBegin());
    uint32 size = static_cast<uint32>(sizeof(WalRecordUndoUpdateSlotUndoPtr));
    WalRecordUndoUpdateSlotUndoPtr redoData;
    bool glsnChangedFlag = (txnSlotPage->GetWalId() != walContext->GetWalId());
    uint64 fileVersion = m_currentTxnSlotBuf->GetFileVersion();
    redoData.SetHeader({WAL_UNDO_UPDATE_TXN_SLOT_PTR, size, m_currentTxnSlotBuf->GetPageId(), txnSlotPage->GetWalId(),
        txnSlotPage->GetPlsn(), txnSlotPage->GetGlsn(), glsnChangedFlag, fileVersion});
    redoData.SetUpdateSlotUndoInfo(slotId, undoRecPtr, insertUndoFlag);
    walContext->PutNewWalRecord(STATIC_CAST_PTR_TYPE(&redoData, WalRecord *));
    UnlockAndReleaseTxnSlotPageBuf(m_currentTxnSlotBuf);
#ifdef DSTORE_USE_ASSERT_CHECKING
    ErrLog(DSTORE_DEBUG1, MODULE_UNDO,
           ErrMsg("Undo zone zid = %d, insert flag %d, undo rec ptr(%hu, %u, %hu).", m_zoneId,
                  static_cast<int>(insertUndoFlag), undoRecPtr.GetFileId(), undoRecPtr.GetBlockNum(),
                  undoRecPtr.GetOffset()));
#endif
    return DSTORE_SUCC;
}

void UndoZoneTrxManager::GetSlotCurTailUndoPtr(Xid xid, UndoRecPtr &undoRecPtr)
{
    PageId pageId = GetTxnSlotPageId(xid.m_logicSlotId);
    uint32 slotId = TransactionSlotPage::GetSlotId(xid.m_logicSlotId);

    StorageAssert(g_storageInstance->GetPdb(m_pdbId) != nullptr);
    StorageAssert(g_storageInstance->GetPdb(m_pdbId)->IsInit());
    StorageAssert(g_storageInstance->GetPdb(m_pdbId)->GetUndoMgr()->IsZoneOwned(m_zoneId));
    BufferDesc *txnSlotPageBuf = ReadTxnSlotPageBuf(pageId, LW_SHARED);
    TransactionSlotPage *txnSlotPage = GetTxnSlotPage(txnSlotPageBuf);

    TransactionSlot *slot = txnSlotPage->GetTransactionSlot(slotId);
    undoRecPtr = slot->GetCurTailUndoPtr();

    UnlockAndReleaseTxnSlotPageBuf(txnSlotPageBuf);
}

BufferDesc *UndoZoneTrxManager::ReadTxnSlotPageBuf(const PageId &id, LWLockMode mode)
{
    StorageReleasePanic(id.m_blockId - m_startPage.m_blockId >= TRX_PAGES_PER_ZONE, MODULE_UNDO,
                        ErrMsg("invalid txn pageid(%hu, %u)", id.m_fileId, id.m_blockId));

    BufferDesc *bufferDesc = m_bufferMgr->Read(m_pdbId, id, mode);
    STORAGE_CHECK_BUFFER_PANIC(bufferDesc, MODULE_UNDO, id);
    return bufferDesc;
}

void UndoZoneTrxManager::UnlockAndReleaseTxnSlotPageBuf(BufferDesc *&bufDesc)
{
    StorageReleasePanic(bufDesc == nullptr, MODULE_UNDO, ErrMsg("Try to release txnSlotPage nullptr!"));
    m_bufferMgr->UnlockAndRelease(bufDesc);
    bufDesc = nullptr;
}

UndoRecPtr UndoZoneTrxManager::GetLastSpaceUndoRecPtr(int64 firstAppendSlotId)
{
    uint64 nextFreeLogicSlotId = m_nextFreeLogicSlotId.load(std::memory_order_acquire);
    uint64 headUndoSlotId = m_recycleLogicSlotId.load(std::memory_order_acquire);
    UndoRecPtr lastPtr = INVALID_UNDO_RECORD_PTR;
    if (firstAppendSlotId != INVALID_TXN_SLOT_ID) {
        /* the slot id is 44 bits. */
        headUndoSlotId = static_cast<uint64>(firstAppendSlotId);
    }
    uint64 tailLogicSlotId = nextFreeLogicSlotId - 1;
    bool lockedPage = false;
    BufferDesc *txnSlotPageBuf = nullptr;
    TransactionSlotPage *txnSlotPage = nullptr;

    while (tailLogicSlotId >= headUndoSlotId) {
        if (!lockedPage) {
            PageId pageId = GetTxnSlotPageId(tailLogicSlotId);
            txnSlotPageBuf = ReadTxnSlotPageBuf(pageId, LW_SHARED);
            txnSlotPage = GetTxnSlotPage(txnSlotPageBuf);
            lockedPage = true;
        }
        uint32 curSlotId = TransactionSlotPage::GetSlotId(tailLogicSlotId);
        TransactionSlot *slot = txnSlotPage->GetTransactionSlot(curSlotId);
        lastPtr = slot->GetSpaceTailUndoPtr();
        if (lastPtr != INVALID_UNDO_RECORD_PTR) {
            break;
        }

        tailLogicSlotId--;

        if (tailLogicSlotId % TRX_PAGE_SLOTS_NUM == TRX_PAGE_SLOTS_NUM - 1) {
            txnSlotPage = nullptr;
            UnlockAndReleaseTxnSlotPageBuf(txnSlotPageBuf);
            lockedPage = false;
        }
    }

    if (lockedPage) {
        UnlockAndReleaseTxnSlotPageBuf(txnSlotPageBuf);
    }

    return lastPtr;
}

void UndoZoneTrxManager::GetFirstSpaceUndoRecPtr(UndoRecPtr &firstTailPtr, int64 &firstAppendUndoSlot)
{
    uint64 nextFreeLogicSlotId = m_nextFreeLogicSlotId.load(std::memory_order_acquire);
    uint64 recycleLogicSlotId = m_recycleLogicSlotId.load(std::memory_order_acquire);
    uint64 firstLogicSlotId = recycleLogicSlotId;
    bool lockedPage = false;
    BufferDesc *txnSlotPageBuf = nullptr;
    TransactionSlotPage *txnSlotPage = nullptr;

    while (firstLogicSlotId < nextFreeLogicSlotId) {
        if (!lockedPage) {
            PageId pageId = GetTxnSlotPageId(firstLogicSlotId);
            txnSlotPageBuf = ReadTxnSlotPageBuf(pageId, LW_SHARED);
            txnSlotPage = GetTxnSlotPage(txnSlotPageBuf);
            lockedPage = true;
        }
        uint32 curSlotId = TransactionSlotPage::GetSlotId(firstLogicSlotId);
        TransactionSlot *slot = txnSlotPage->GetTransactionSlot(curSlotId);
        firstTailPtr = slot->GetSpaceTailUndoPtr();
        if (firstTailPtr != INVALID_UNDO_RECORD_PTR) {
            /* 44 bits */
            firstAppendUndoSlot = static_cast<int64>(firstLogicSlotId);
            break;
        }

        firstLogicSlotId++;
        if (firstLogicSlotId % TRX_PAGE_SLOTS_NUM == 0) {
            txnSlotPage = nullptr;
            UnlockAndReleaseTxnSlotPageBuf(txnSlotPageBuf);
            lockedPage = false;
        }
    }
    if (lockedPage) {
        UnlockAndReleaseTxnSlotPageBuf(txnSlotPageBuf);
    }
}

void UndoZoneTrxManager::GenerateWalForRecycle(BufferDesc *bufferDesc, uint64 lastRecycleSlotId,
                                               uint64 nextRecycleSlotId, CommitSeqNo recycleMinCsn)
{
    AtomicWalWriterContext *walContext = thrd->m_walWriterContext;
    TransactionSlotPage *page = static_cast<TransactionSlotPage *>(bufferDesc->GetPage());
    StorageReleasePanic(page == nullptr, MODULE_UNDO,
                        ErrMsg("Invalid txn page, pageid(%hu, %u). zoneId(%d), pdbId[%u]",
                               bufferDesc->GetPageId().m_fileId, bufferDesc->GetPageId().m_blockId, m_zoneId, m_pdbId));
    StorageAssert(walContext->HasAlreadyBegin());
    StorageReleasePanic(walContext->GetPdbId() != m_pdbId, MODULE_UNDO,
                        ErrMsg("Walcontext pdbid is wrong when generate wal for recycle!"));
    walContext->RememberPageNeedWal(bufferDesc);
    uint32 size = static_cast<uint32>(sizeof(WalRecordRecycleSlot));
    WalRecordRecycleSlot redoData;
    bool glsnChangedFlag = (page->GetWalId() != walContext->GetWalId());
    redoData.SetHeader({WAL_UNDO_RECYCLE_TXN_SLOT, size, bufferDesc->GetPageId(), page->GetWalId(),
                        page->GetPlsn(), page->GetGlsn(), glsnChangedFlag, bufferDesc->GetFileVersion()});
    redoData.SetZid(m_zoneId);
    redoData.SetRecycleCsn(recycleMinCsn);
    redoData.SetLastRecycleLogicSlotId(lastRecycleSlotId);
    redoData.SetNewRecycleLogicSlotId(nextRecycleSlotId);
    walContext->PutNewWalRecord(&redoData);
}

void UndoZoneTrxManager::RecycleTxnSlots(CommitSeqNo recycleMinCsn, UndoRecPtr &restoreUndoPtr,
                                         UndoRecPtr &recycleUndoPtr, bool isInRestore,
                                         TailUndoPtrStatus &tailPtrStatus)
{
    uint64 nextFreeLogicSlotId = m_nextFreeLogicSlotId.load(std::memory_order_relaxed);
    uint64 recycleLogicSlotId = m_recycleLogicSlotId.load(std::memory_order_relaxed);
    uint64 oldRecycleLogicSlotId = recycleLogicSlotId;
    bool lockedPage = false;
    BufferDesc *txnSlotPageBuf = nullptr;
    TransactionSlotPage *txnSlotPage = nullptr;
    bool nextToBeRecycledIsCommitted = false;
    AtomicWalWriterContext *walContext = thrd->m_walWriterContext;
    uint64 firstSlotRecycleLogitSlotId = 0;
    while (recycleLogicSlotId < nextFreeLogicSlotId) {
        /* step 1: check slot recyclable */
        if (!IsSlotRecyclable(recycleLogicSlotId, recycleMinCsn, restoreUndoPtr, txnSlotPage,
                              nextToBeRecycledIsCommitted)) {
            break;
        }

        if (!lockedPage) {
            PageId slotPageId = GetTxnSlotPageId(recycleLogicSlotId);
            txnSlotPageBuf = ReadTxnSlotPageBuf(slotPageId, LW_EXCLUSIVE);
            txnSlotPage = GetTxnSlotPage(txnSlotPageBuf);
            lockedPage = true;
            firstSlotRecycleLogitSlotId = recycleLogicSlotId;
        }

        /* step 2: recycle slot and get endUndoPtr. */
        uint32 curSlotId = TransactionSlotPage::GetSlotId(recycleLogicSlotId);
        if (unlikely(txnSlotPage == nullptr)) {
            ErrLog(
                DSTORE_ERROR, MODULE_UNDO,
                ErrMsg("Invalid txn page, pageid(%hu, %u). zoneId(%d), pdbId[%u]", txnSlotPageBuf->GetPageId().m_fileId,
                       txnSlotPageBuf->GetPageId().m_blockId, m_zoneId, m_pdbId));
            UnlockAndReleaseTxnSlotPageBuf(txnSlotPageBuf);
            return;
        }

        TransactionSlot *slot = txnSlotPage->GetTransactionSlot(curSlotId);
        StorageAssert(slot->status != TXN_STATUS_UNKNOWN);
        slot->SetTrxSlotStatus(TXN_STATUS_FROZEN);
        if (slot->GetSpaceTailUndoPtr() != INVALID_UNDO_RECORD_PTR) {
            recycleUndoPtr = slot->GetSpaceTailUndoPtr();
        }

        recycleLogicSlotId++;

        /* step 3: check if we need switch next page */
        if (recycleLogicSlotId % TRX_PAGE_SLOTS_NUM == 0) {
            (void)m_bufferMgr->MarkDirty(txnSlotPageBuf);
            walContext->BeginAtomicWal(INVALID_XID);
            GenerateWalForRecycle(txnSlotPageBuf, firstSlotRecycleLogitSlotId, recycleLogicSlotId, recycleMinCsn);
            (void)walContext->EndAtomicWal();
            UnlockAndReleaseTxnSlotPageBuf(txnSlotPageBuf);
            lockedPage = false;
            txnSlotPage = nullptr;
        }
    }
    bool needWal = false;
    if (recycleLogicSlotId != oldRecycleLogicSlotId) {
        m_recycleLogicSlotId.store(recycleLogicSlotId, std::memory_order_release);
        needWal = true;
    }

    if (lockedPage) {
        if (needWal) {
            (void)m_bufferMgr->MarkDirty(txnSlotPageBuf);
            walContext->BeginAtomicWal(INVALID_XID);
            GenerateWalForRecycle(txnSlotPageBuf, firstSlotRecycleLogitSlotId, recycleLogicSlotId, recycleMinCsn);
            (void)walContext->EndAtomicWal();
        }
        UnlockAndReleaseTxnSlotPageBuf(txnSlotPageBuf);
    }
    if (isInRestore) {
        if (restoreUndoPtr != INVALID_UNDO_RECORD_PTR) {
            tailPtrStatus = VALID_STATUS;
            return;
        } else {
            if (!nextToBeRecycledIsCommitted) {
                /* There is only one in-progress or pending commit slot that no undo record of this transaction has
                 * insert to the undo page. */
                tailPtrStatus = NO_VALID_TAIL_UNDO_PTR;
                return;
            }
            tailPtrStatus = NEED_FETCH_FROM_COMMITED_SLOT;
            return;
        }
    }
}

RetStatus UndoZoneTrxManager::RollbackTxnSlot(Xid xid)
{
    /*
     * Fill current max active snapshot into aborted slot to avoid premature recycle.
     * Because active transaction may have the aborted tuples in cr page. If slot is recycled,
     * these tuples will be judged visible but these are invisible in fact. After the csn, cr
     * page won't have these aborted tuples.
     */
    CommitSeqNo csn;
    if (STORAGE_FUNC_FAIL(g_storageInstance->GetCsnMgr()->GetNextCsn(csn, false))) {
        return DSTORE_FAIL;
    }

    PageId pageId = GetTxnSlotPageId(xid.m_logicSlotId);
    uint16 slotId = TransactionSlotPage::GetSlotId(xid.m_logicSlotId);
    BufferDesc *txnSlotPageBuf = ReadTxnSlotPageBuf(pageId, LW_EXCLUSIVE);
    TransactionSlotPage *txnSlotPage = GetTxnSlotPage(txnSlotPageBuf);
    TransactionSlot *slot = txnSlotPage->GetTransactionSlot(slotId);
    slot->SetTrxSlotStatus(TXN_STATUS_ABORTED);
    slot->SetCsn(csn);

    (void)m_bufferMgr->MarkDirty(txnSlotPageBuf);

    /* wal */
    AtomicWalWriterContext *walContext = thrd->m_walWriterContext;
    walContext->BeginAtomicWal(xid);
    walContext->RememberPageNeedWal(txnSlotPageBuf);
    uint32 size = sizeof(WalRecordTransactionAbort);
    WalRecordTransactionAbort redoData;
    bool glsnChangedFlag = (txnSlotPage->GetWalId() != walContext->GetWalId());
    uint64 fileVersion = txnSlotPageBuf->GetFileVersion();
    redoData.SetHeader({WAL_TXN_ABORT, size, txnSlotPageBuf->GetPageId(), txnSlotPage->GetWalId(),
        txnSlotPage->GetPlsn(), txnSlotPage->GetGlsn(), glsnChangedFlag, fileVersion});
    redoData.SetSlotId(slotId);
    redoData.SetCsn(csn);
    redoData.SetCurrentAbortTime();
    walContext->PutNewWalRecord(&redoData);

    UnlockAndReleaseTxnSlotPageBuf(txnSlotPageBuf);

    walContext->EndAtomicWal();
#ifdef DSTORE_USE_ASSERT_CHECKING
    ErrLog(DSTORE_DEBUG1, MODULE_UNDO,
           ErrMsg("Undo zone zid %d, trx slot space rollbacked, logicslotid(%lu).", m_zoneId, xid.m_logicSlotId));
#endif
    g_storageInstance->GetPdb(m_pdbId)->GetUndoMgr()->WriteTxnInfoToCache(xid, *slot, slot->csn);
    return DSTORE_SUCC;
}

Xid UndoZoneTrxManager::GetLastActiveXid()
{
    uint64 nextFreeLogicSlotId = m_nextFreeLogicSlotId.load(std::memory_order_acquire);
    uint64 recycleLogicSlotId = m_recycleLogicSlotId.load(std::memory_order_acquire);
    if (nextFreeLogicSlotId == 0 || recycleLogicSlotId == nextFreeLogicSlotId) {
        ErrLog(DSTORE_DEBUG1, MODULE_UNDO,
            ErrMsg("zoneId [%d] no active trx slot (nextFreeLogicSlotId, nextFreeLogicSlotId):(%lu, %lu)",
                m_zoneId, nextFreeLogicSlotId, recycleLogicSlotId));
        return INVALID_XID;
    }

    Xid activeXid = INVALID_XID;
    PageId lastPageId = GetTxnSlotPageId(nextFreeLogicSlotId - 1);
    BufferDesc *txnSlotPageBuf = ReadTxnSlotPageBuf(lastPageId, LW_SHARED);
    TransactionSlotPage *txnSlotPage = GetTxnSlotPage(txnSlotPageBuf);
    uint32 nextSlotId = TransactionSlotPage::GetSlotId(nextFreeLogicSlotId);
    uint32 lastSlotId = ((nextSlotId != 0) ? (nextSlotId - 1) : (TRX_PAGE_SLOTS_NUM - 1));
    TransactionSlot *lastSlot = txnSlotPage->GetTransactionSlot(lastSlotId);
    if (lastSlot->status == TXN_STATUS_IN_PROGRESS || lastSlot->status == TXN_STATUS_PENDING_COMMIT) {
        activeXid = {static_cast<uint32>(m_zoneId), nextFreeLogicSlotId - 1};
        ErrLog(DSTORE_DEBUG1, MODULE_UNDO, ErrMsg("zoneId [%d] active trx slot (logicSlotId: %lu)",
            m_zoneId, nextFreeLogicSlotId - 1));
    }
    UnlockAndReleaseTxnSlotPageBuf(txnSlotPageBuf);
    return activeXid;
}

} // namespace DSTORE
