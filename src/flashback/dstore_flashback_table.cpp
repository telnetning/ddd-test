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
 * dstore_flashback_table.cpp
 *
 * IDENTIFICATION
 *        src/flashback/dstore_flashback_table.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "flashback/dstore_flashback_table.h"
#include "transaction/dstore_transaction_mgr.h"
#include "errorcode/dstore_undo_error_code.h"
namespace DSTORE {

bool FlashbackTableHandler::IsTupleVisibleFlashbackCsn(HeapTuple *tuple)
{
    Transaction *txn = m_thrd->GetActiveTransaction();
    StorageAssert(m_instance->GetPdb(m_pdbId) != nullptr);
    StorageAssert(m_instance->GetPdb(m_pdbId)->IsInit());
    TransactionMgr *transactionMgr = m_instance->GetPdb(m_pdbId)->GetTransactionMgr();
    HeapDiskTuple *diskTuple = tuple->m_diskTuple;
    TupleTdStatus tdStatus = diskTuple->GetTdStatus();
    if (tdStatus == DETACH_TD) {
        /*
         * DETACH_TD means tupleCSN < recycleMinCsn. With the implication that recycleMinCsn < snapshotCsn, we can say
         * that tupleCSN < snapshotCSN, so the tuple is visible
         */
        return true;
    }

    /* Need to load base page and check TD to get xid or CSN */
    PageId pageId = tuple->m_head.ctid.val.m_pageid;
    BufferDesc *baseBufDesc = m_instance->GetBufferMgr()->Read(m_pdbId, pageId, LW_SHARED);
    StorageAssert(baseBufDesc != INVALID_BUFFER_DESC);
    HeapPage *basePage = static_cast<HeapPage*>(baseBufDesc->GetPage());
    TD *td = basePage->GetTd(diskTuple->GetTdId());
    Xid xid = td->GetXid();
    CommitSeqNo csn = INVALID_CSN;
    bool invisibleAtSnapshot = false;

    if (tdStatus == ATTACH_TD_AS_NEW_OWNER) {
        /* tuple writer's xid is in current TD. Directly compare */
        if (td->TestCsnStatus(IS_CUR_XID_CSN)) {
            csn = td->GetCsn();
            StorageAssert(csn != INVALID_CSN);
            /*
             * When tuple's csn == snapshotCsn, the tuple is written by the txn which commits after the snapshotCsn
             * has been given out, so it is invisible to the snapshot.
             */
            invisibleAtSnapshot = m_flashbackCsn <= csn;
        } else {
            /*
             * TD doesn't have CSN filled, but that's okay, we can make a query from undo zone.
             * Here we alter txn CSN because we want to make use of the logic in Transaction::XidVisibleToSnapshot()
             */
            txn->SetSnapshotCsnForFlashback(m_flashbackCsn);
            invisibleAtSnapshot = !XidVisibleToSnapshot(txn->GetSnapshot(), xid, txn);
        }
    } else {
        StorageAssert(tdStatus == ATTACH_TD_AS_HISTORY_OWNER);
        UndoRecPtr undoRecPtr = td->GetUndoRecPtr();
        UndoRecord record;
        if (STORAGE_FUNC_SUCC(transactionMgr->FetchUndoRecordByMatchedCtid(xid, &record,
            undoRecPtr, tuple->m_head.ctid, &csn))) {
            if (csn != INVALID_CSN) {
                invisibleAtSnapshot = m_flashbackCsn <= csn;
            } else {
                /* the preTD in undoRec doesn't have CSN filled, but that's okay, we also get it from undo zone */
                txn->SetSnapshotCsnForFlashback(m_flashbackCsn);
                invisibleAtSnapshot = !XidVisibleToSnapshot(txn->GetSnapshot(), xid, txn);
            }
        } else {
            StorageAssert(StorageGetErrorCode() == UNDO_ERROR_RECORD_RECYCLED);
            StorageClearError();
            /*
             * UNDO_ERROR_RECORD_RECYCLED means undoRecCSN < recycleCsn. With the implication that
             * recycleCsn < snapshotCsn, we can say that undoRecCSN < snapshotCSN, so the tuple is visible
             */
            invisibleAtSnapshot = false;
        }
    }

    m_instance->GetBufferMgr()->UnlockAndRelease(baseBufDesc);
    return (!invisibleAtSnapshot);
}

/*
 * A delta tuple is visible now, but invisible to the given snapshot. They needs to be deleted during flashback table.
 * This function acts similar to SeqScan: upon each call it will return one delta tuple, until there is none and it
 * will return nullptr.
 */
HeapTuple *FlashbackTableHandler::GetDeltaTuple()
{
    Transaction *txn = m_thrd->GetActiveTransaction();
    HeapTuple *tuple = nullptr;

rescan:
    /* make sure tuples we get from seqScan satisfy SNAPSHOT_NOW */
    tuple = m_heapScanHandler->SeqScan();
    m_heapScanHandler->ReportHeapBufferReadStat(m_heapRel);
    if (unlikely(tuple == nullptr)) {
        /* no tuple left on the table */
        goto finish;
    }

    txn->SetSnapshotCsnForFlashback(m_flashbackCsn);
    if (IsTupleVisibleFlashbackCsn(tuple)) {
        goto rescan;
    } else {
        goto finish;
    }

finish:
    return tuple;
}

/*
 * A lost tuple is invisible now, but visible to the given snapshot. They needs to be inserted during flashback table.
 * This function acts similar to SeqScan: upon each call it will return one lost tuple, until there is none and it
 * will return nullptr.
 */
HeapTuple *FlashbackTableHandler::GetLostTuple()
{
    BufferDesc *baseBufDesc = nullptr;
    Transaction *txn = m_thrd->GetActiveTransaction();
    HeapTuple *tuple = nullptr;
    ItemPointerData *ctid = nullptr;
    PageId pageId = INVALID_PAGE_ID;
    HeapPage *basePage = nullptr;
    ItemId *basePageItemId = nullptr;

rescan:
    if (baseBufDesc != nullptr) {
        /* not nullptr meaning the lock on baseBufDesc has not been released yet */
        m_instance->GetBufferMgr()->UnlockAndRelease(baseBufDesc);
        baseBufDesc = nullptr;
    }
    /*
     * Step 1: Scan the tuple by flashbackCsn
     * Make sure the tuples we get from seqScan satisfy SNAPSHOT_VERSION_MVCC.
     * With snapshotCsn, SeqScan will only return tuples that are visible at the time of snapshotCsn (using CR page
     * mechanism).
     */
    txn->SetSnapshotCsnForFlashback(m_flashbackCsn);
    tuple = m_heapScanHandler->SeqScan();
    m_heapScanHandler->ReportHeapBufferReadStat(m_heapRel);
    if (unlikely(tuple == nullptr)) {
        /* no tuple left on the table */
        goto finish;
    }

    /*
     * Step 2: Now that the tuple satisfy SNAPSHOT_VERSION_MVCC, we check if the tuple is invisible now.
     */
    ctid = &tuple->m_head.ctid;
    /* Need to load base page to see the current status of the tuple */
    pageId = ctid->val.m_pageid;
    baseBufDesc = m_instance->GetBufferMgr()->Read(m_pdbId, pageId, LW_SHARED);
    StorageAssert(baseBufDesc != INVALID_BUFFER_DESC);
    basePage = static_cast<HeapPage*>(baseBufDesc->GetPage());
    basePageItemId = basePage->GetItemIdPtr(ctid->val.m_offset);
    if (basePageItemId->IsNoStorage()) {
        /* Tuple has been deleted and pruned */
        goto finish;
    } else if (basePageItemId->IsUnused()) {
        /* Tuple has been deleted and pruned with ItemId set as unused, meaning its CSN < recycleCSN */
        goto finish;
    } else {
        /* Tuple is still on the page. Still need to check live mode to see if it's been marked as deleted */
        StorageAssert(basePageItemId->IsNormal());
        HeapDiskTuple *basePageDiskTuple = basePage->GetDiskTuple(ctid->GetOffset());
        HeapDiskTupLiveMode mode = basePageDiskTuple->GetLiveMode();
        StorageReleasePanic(mode == HeapDiskTupLiveMode::OLD_TUPLE_BY_SAME_PAGE_UPDATE, MODULE_HEAP,
                            ErrMsg("Invalid heap disk tuple live mode(OLD_TUPLE_BY_SAME_PAGE_UPDATE)."));
        if (mode == HeapDiskTupLiveMode::TUPLE_BY_NORMAL_DELETE ||
            mode == HeapDiskTupLiveMode::OLD_TUPLE_BY_ANOTHER_PAGE_UPDATE) {
            /* LiveMode marks the tuple deleted, so we need to re-insert it. */
            goto finish;
        } else {
            /*
             * By here, the itemId is normal, so the basePageDiskTuple is the tuple for flashback csn.
             * it is not a lost tuple. We can't reclaim the itemid because recycle csn is min of the
             * global min csn and flashback csn.
             */
            goto rescan;
        }
    }

finish:
    if (baseBufDesc != nullptr) {
        /* not nullptr meaning the lock on baseBufDesc has not been released yet */
        m_instance->GetBufferMgr()->UnlockAndRelease(baseBufDesc);
    }

    return tuple;
}

}  // namespace DSTORE
