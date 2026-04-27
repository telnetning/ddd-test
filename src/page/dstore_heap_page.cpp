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
 * dstore_heap_page.cpp
 *
 * IDENTIFICATION
 *        src/page/dstore_heap_page.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "page/dstore_heap_page.h"
#include "common/log/dstore_log.h"
#include "errorcode/dstore_undo_error_code.h"
#include "securec.h"
#include "common/dstore_common_utils.h"
#include "tuple/dstore_memheap_tuple.h"
#include "transaction/dstore_transaction_mgr.h"
#include "transaction/dstore_transaction.h"
#include "heap/dstore_heap_undo_struct.h"

namespace DSTORE {

static const int MAX_DUMP_FILENAME = 4096;

void HeapPage::InitHeapPage(BufferDesc *bufDesc, const PageId &selfPageId, const FsmIndex &fsmIndex)
{
    HeapPage *heapPage = static_cast<HeapPage *>(bufDesc->GetPage());
    StorageAssert(heapPage);
    if (STORAGE_VAR_NULL(heapPage)) {
        ErrLog(DSTORE_PANIC, MODULE_HEAP, ErrMsg("HeadPage is nullptr."));
    }
    heapPage->Init(0, PageType::HEAP_PAGE_TYPE, selfPageId);
    heapPage->SetRecentDeadTupleMinCsn(INVALID_CSN);
    heapPage->SetPotentialDelSize(0);
    heapPage->SetDataHeaderSize(HEAP_PAGE_HEADER_SIZE);
    heapPage->m_header.m_lower = heapPage->DataHeaderSize();
    heapPage->AllocateTdSpace();

    heapPage->SetFsmIndex(fsmIndex);
    heapPage->SetIsNewPage(true);
}

OffsetNumber HeapPage::AddTuple(const HeapDiskTuple *tuple, uint16 size, OffsetNumber specifyOffset)
{
    /* 1. check page sanity before insert */
    StorageAssert(CheckHeapPageSanity());

    /* 2. find empty slot */
    OffsetNumber offset = specifyOffset;
    if (!DstoreOffsetNumberIsValid(specifyOffset)) {
        offset = GetFreeItemId();
    }
    if (unlikely(offset == INVALID_ITEM_OFFSET_NUMBER)) {
        return INVALID_ITEM_OFFSET_NUMBER;
    }

    /* 3. copy to the buffer */
    StorageAssert((size > 0 && size < UINT16_MAX));
    StorageAssert(m_header.m_upper > size);
    uint16 upper = static_cast<uint16>(m_header.m_upper - size);
    OffsetNumber limit = OffsetNumberNext(GetMaxOffset());
    uint16 lower = m_header.m_lower;
    if (offset == limit) {
        /* the ItemId is new allocated */
        StorageAssert((lower <= (UINT16_MAX - sizeof(ItemId))));
        lower += sizeof(ItemId);
    }

    if (unlikely(lower > upper)) {
        return INVALID_ITEM_OFFSET_NUMBER;
    }

    errno_t rc = memcpy_s(PageHeaderPtr() + upper, size, tuple, size);
    storage_securec_check(rc, "\0", "\0");

    /* 4. set item id */
    ItemId *itemId = GetItemIdPtr(offset);
    itemId->SetNormal(upper, size);

    /* 5. adjust page header */
    m_header.m_lower = lower;
    m_header.m_upper = upper;
    StorageAssert(CheckHeapPageSanity());

    return offset;
}

void HeapPage::DelTuple(OffsetNumber offset)
{
    HeapDiskTuple *diskTuple = GetDiskTuple(offset);
    diskTuple->SetLiveMode(HeapDiskTupLiveMode::TUPLE_BY_NORMAL_DELETE);
}

void HeapPage::GetTuple(HeapTuple *tuple, OffsetNumber offset)
{
    StorageReleasePanic(!IsValidOffset(offset), MODULE_HEAP, ErrMsg("Get tuple fail, offset(%hu).", offset));

    ItemId *itemId = GetItemIdPtr(offset);
    StorageAssert(!itemId->IsNoStorage());
    tuple->m_diskTuple = static_cast<HeapDiskTuple *>(GetRowData(itemId));
    tuple->SetDiskTupleSize(tuple->m_diskTuple->GetTupleSize());
    ItemPointerData ctid(GetSelfPageId(), offset);
    tuple->SetCtid(ctid);
}

/* ----------
 * GetVisibleLobTuple
 *
 * For lob tuple, if the tuple has been pruned out then we go to undo to fetch the latest version.
 * Otherwise, the current version is the visible one.
 * ----------
 */
HeapTuple *HeapPage::GetVisibleLobTuple(PdbId pdbId, ItemPointerData &ctid)
{
    OffsetNumber offset = ctid.GetOffset();
    ItemId *itemId = GetItemIdPtr(offset);
    HeapTuple *resTuple = nullptr;

    if (likely(itemId->IsNormal())) {
        return CopyTuple(offset);
    }

    if (likely(!itemId->IsNoStorage())) {
        return nullptr;
    }

    StoragePdb *pdb = g_storageInstance->GetPdb(pdbId);
    StorageReleasePanic(pdb == nullptr, MODULE_HEAP, ErrMsg("pdb %u is nullptr", pdbId));
    TransactionMgr *txnMgr = pdb->GetTransactionMgr();
    TdId tdId = GetTupleTdId(offset);
    TD *td = GetTd(tdId);
    Xid xid = td->GetXid();
    UndoRecPtr undoRecPtr = td->GetUndoRecPtr();
    UndoRecord record;
    if (STORAGE_FUNC_FAIL(txnMgr->FetchUndoRecordByMatchedCtid(xid, &record, undoRecPtr, ctid, nullptr))) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Fetch unod record failed when get visible lob tuple."));
        return nullptr;
    }

    ConstructCrTupleFromUndo(&record, &resTuple);
    return resTuple;
}

HeapTuple *HeapPage::GetVisibleTuple(PdbId pdbId, Transaction *txn, ItemPointerData &ctid, Snapshot snapshot, bool is_lob)
{
    if (unlikely(is_lob)) {
        return GetVisibleLobTuple(pdbId, ctid);
    }

    bool needUndo = true;
    Xid xid = INVALID_XID;
    OffsetNumber offset = ctid.GetOffset();
    ItemId *itemId = GetItemIdPtr(offset);
    HeapTuple *resTuple = nullptr;

    /* Step 1: copy the newest tuple if itemId is normal */
    if (likely(itemId->IsNormal())) {
        resTuple = CopyTuple(offset);
        if (unlikely(resTuple == nullptr)) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Allocate memory fail when get visible tuple."));
            return nullptr;
        }
    } else if (itemId->IsNoStorage()) {
        resTuple = nullptr;
    } else {
        return nullptr;
    }

    /* Step 2: determine if we need undo to find the visible tuple */
    TdId tdId = GetTupleTdId(offset);
    TD *td = GetTd(tdId);
    switch (GetTupleTdStatus<HeapDiskTuple>(offset)) {
        case DETACH_TD: {
            needUndo = false;
            break;
        }
        case ATTACH_TD_AS_HISTORY_OWNER: {
            /* In snapshot dirty, committed transactions do not need to be undoed. */
            if (snapshot->GetSnapshotType() == SnapshotType::SNAPSHOT_DIRTY) {
                needUndo = false;
            } else {
                needUndo = td->TestCsnStatus(IS_INVALID) || !XidVisibleToSnapshot(snapshot, td->GetCsn());
            }
            break;
        }
        case ATTACH_TD_AS_NEW_OWNER: {
            xid = td->GetXid();
            StorageAssert(xid != INVALID_XID);
            XidStatus xs(xid, txn, td);
            needUndo = (xs.IsCurrentTxn()) ? !CidVisibleToSnapshot(txn, snapshot, td->GetCommandId()) :
                       !XidVisibleToSnapshot(snapshot, xid, txn, &xs);
            break;
        }
        default: {
            StorageAssert(false);
        }
    }

    if (needUndo) {
        /*
         * Case 1: the mvcc check for the inserted tuple which td is reused.
         * Case 2: deleted tuple for mvcc read
         * Case 3: updated tuple for mvcc read
         */
        if (STORAGE_FUNC_FAIL(ConstructCrTuple(pdbId, txn, ctid, tdId, &resTuple, snapshot))) {
            return nullptr;
        }
    } else {
        if (resTuple == nullptr) {
            return nullptr;
        }
        HeapDiskTuple *diskTup = resTuple->GetDiskTuple();
        StorageAssert(!diskTup->TestLiveMode(HeapDiskTupLiveMode::OLD_TUPLE_BY_SAME_PAGE_UPDATE));
        /* A tuple that is being deleted or modified is visible to snapshot dirty. */
        if (snapshot->GetSnapshotType() != SnapshotType::SNAPSHOT_DIRTY &&
            (diskTup->TestLiveMode(HeapDiskTupLiveMode::TUPLE_BY_NORMAL_DELETE) ||
             diskTup->TestLiveMode(HeapDiskTupLiveMode::OLD_TUPLE_BY_ANOTHER_PAGE_UPDATE))) {
            DstorePfree(resTuple);
            resTuple = nullptr;
        }
    }

    return resTuple;
}

HeapTuple *HeapPage::CopyTuple(OffsetNumber offset)
{
    StorageReleasePanic(!IsValidOffset(offset), MODULE_HEAP, ErrMsg("Copy tuple fail, offset(%hu).", offset));
    AutoMemCxtSwitch autoMemCxtSwitch(thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_QUERY));

    ItemId *itemId = GetItemIdPtr(offset);
    HeapDiskTuple *diskTuple = static_cast<HeapDiskTuple *>(GetRowData(itemId));
    StorageAssert(diskTuple != nullptr);
    StorageAssert(itemId->GetLen() >= diskTuple->GetTupleSize());
    HeapTuple *tuple = static_cast<HeapTuple *>(DstorePalloc(sizeof(HeapTuple) + diskTuple->GetTupleSize()));
    if (unlikely(tuple == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("DstorePalloc fail size(%u).",
                static_cast<uint32>(sizeof(HeapTuple) + diskTuple->GetTupleSize())));
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        return nullptr;
    }
    tuple->Init();
    tuple->m_diskTuple = static_cast<HeapDiskTuple *>(
        static_cast<void *>(static_cast<char *>(static_cast<void *>(tuple)) + sizeof(HeapTuple)));
    errno_t rc = memcpy_s(tuple->m_diskTuple, diskTuple->GetTupleSize(), diskTuple, diskTuple->GetTupleSize());
    storage_securec_check(rc, "\0", "\0");
    tuple->SetDiskTupleSize(diskTuple->GetTupleSize());
    ItemPointerData ctid(GetSelfPageId(), offset);
    tuple->SetCtid(ctid);
    return tuple;
}

HeapTuple *HeapPage::ExpandTupleSize(HeapTuple *oldTuple, uint32 newTupleSize) const
{
    HeapTuple *tuple = static_cast<HeapTuple *>(
        DstoreRepalloc(static_cast<void *>(oldTuple), sizeof(HeapTuple) + newTupleSize));
    if (unlikely(tuple == nullptr)) {
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("DstoreRepalloc failed when ExpandTupleSize."));
        return nullptr;
    }
    tuple->m_diskTuple = static_cast<HeapDiskTuple *>(
        static_cast<void *>(static_cast<char *>(static_cast<void *>(tuple)) + sizeof(HeapTuple)));
    return tuple;
}

bool HeapPage::TupBiggerThanPage(HeapTuple *tuple)
{
    return tuple->GetDiskTupleSize() > MaxDefaultTupleSpace();
}

bool HeapPage::IsValidOffset(OffsetNumber offset)
{
    /* check if offset has exceeded the max offset on the current page */
    if (offset > GetMaxOffset()) {
        return false;
    }

    /* check if corresponding item id has a valid length */
    ItemId *itemId = GetItemIdPtr(offset);
    return itemId->GetLen() != 0;
}

void HeapPage::UpdateTuple(OffsetNumber offset, HeapDiskTuple *diskTuple, uint32 diskTupleSize)
{
    ItemId *itemId = GetItemIdPtr(offset);
    uint32 pageDiskTupleSize = itemId->GetLen();
    StorageAssert(pageDiskTupleSize >= diskTupleSize);
    HeapDiskTuple *pageDiskTuple = GetDiskTuple(offset);
    errno_t rc = memcpy_s(pageDiskTuple, pageDiskTupleSize, diskTuple, diskTupleSize);
    storage_securec_check(rc, "\0", "\0");
}

void HeapPage::ExecuteUndoForInsert(UndoRecord *record)
{
    OffsetNumber offset = record->GetCtid().GetOffset();
    ItemId *itemId = GetItemIdPtr(offset);
    AddPotentialDelItemSize(offset);
    SetTuplePrunable(true);

    itemId->SetUnused();
    SetHasFreeItemId();

    /* Undo new TD using the old xid and old undo rec ptr of new td */
    GetTd(record->GetTdId())->RollbackTdInfo(record);
}

RetStatus HeapPage::ExecuteUndoForBatchInsert(UndoRecord *record, void* undoDataRec)
{
    UndoDataHeapBatchInsert *undoData = nullptr;
    if (unlikely(record->GetUndoData() == nullptr)) {
        undoData = static_cast<UndoDataHeapBatchInsert *>(undoDataRec);
    } else {
        undoData = static_cast<UndoDataHeapBatchInsert *>(record->GetUndoData());
    }
    if (unlikely(undoData == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_UNDO, ErrMsg("Undo data is null when execute undo for batch insert!"));
        return DSTORE_FAIL;
    }
    OffsetNumber *offsetData = static_cast<OffsetNumber *>(undoData->GetRawData());
    uint16 rangeNum = undoData->GetItemIdRangeCount();
    for (uint16 i = 0; i < rangeNum; ++i) {
        OffsetNumber startOffset = offsetData[i * 2];
        OffsetNumber endOffset = offsetData[i * 2 + 1];
        for (OffsetNumber offset = startOffset; offset <= endOffset; ++offset) {
            ItemId *itemId = GetItemIdPtr(offset);
            AddPotentialDelItemSize(offset);
            itemId->SetUnused();
        }
    }
    SetTuplePrunable(true);
    SetHasFreeItemId();
    /* Undo new TD using the old xid and old undo rec ptr of new td */
    GetTd(record->GetTdId())->RollbackTdInfo(record);
    return DSTORE_SUCC;
}

RetStatus HeapPage::ExecuteUndoForDelete(UndoRecord *record, void* undoDataRec)
{
    UndoDataHeapDelete *undoData = nullptr;
    if (unlikely(record->GetUndoData() == nullptr)) {
        undoData = static_cast<UndoDataHeapDelete *>(undoDataRec);
    } else {
        undoData = static_cast<UndoDataHeapDelete *>(record->GetUndoData());
    }
    if (unlikely(undoData == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_UNDO, ErrMsg("Undo data is null when execute undo for delete!"));
        return DSTORE_FAIL;
    }
    OffsetNumber offset = record->GetCtid().GetOffset();
    HeapDiskTuple *diskTuple = undoData->GetDiskTuple();
    uint16 diskTupleSize = undoData->GetRawDataSize();

    /*
     * The TD for the old version of our tuple may have been reset, even though it was not reset when generating
     * the undo record. Therefore, if the TD status is UNOCCUPY_AND_PRUNEABLE, we set tuple td status to DETACH_TD
     * so that no old tuple brought back by undo has a tuple td status of ATTACH_TD_AS_NEW_OWNER or
     * ATTACH_TD_AS_HISTORY_OWNER while pointing to a TD that has been reset (otherwise HeapPageSanityCheck can
     * fail if the resetted TD got recycled in CompactCRPageIfFreeSpaceLessThan)
     */
    if (!IsTdValidAndOccupied(diskTuple->GetTdId())) {
        diskTuple->SetTdStatus(DETACH_TD);
    } else {
        Xid curXid = GetTd(record->GetTdId())->GetXid();
        Xid tupleXid = diskTuple->GetXid(); /* xid from distuple in undo record */
        if (curXid != tupleXid) {
            diskTuple->SetTdStatus(ATTACH_TD_AS_HISTORY_OWNER);
        }
    }

    /* We need to consider rollback current transaction and flashback query */
    if (GetItemIdPtr(offset)->IsNormal() && diskTupleSize <= GetItemIdPtr(offset)->GetLen()) {
        UpdateTuple(offset, diskTuple, diskTupleSize);
    } else {
        if (STORAGE_FUNC_FAIL(CompactCRPageIfFreeSpaceLessThan(diskTupleSize))) {
            return DSTORE_FAIL;
        }
        UNUSE_PARAM OffsetNumber newOffset = AddTuple(diskTuple, diskTupleSize, offset);
        StorageAssert(newOffset == offset);
    }

    /*
     * we must roll back td after compacting cr page to avoid recycling current td, because the caller may use the td
     * pointer to construct cr.
     */
    GetTd(record->GetTdId())->RollbackTdInfo(record);
    return DSTORE_SUCC;
}

RetStatus HeapPage::ExecuteUndoForInplaceUpdate(UndoRecord *record, void* undoDataRec)
{
    UndoDataHeapInplaceUpdate *undoData = nullptr;
    if (unlikely(record->GetUndoData() == nullptr)) {
        undoData = static_cast<UndoDataHeapInplaceUpdate *>(undoDataRec);
    } else {
        undoData = static_cast<UndoDataHeapInplaceUpdate *>(record->GetUndoData());
    }
    if (unlikely(undoData == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_UNDO, ErrMsg("Undo data is null when execute undo for inplace update!"));
        return DSTORE_FAIL;
    }

    OffsetNumber offset = record->GetCtid().GetOffset();

    HeapDiskTuple *pageDiskTuple = GetDiskTuple(offset);
    ItemId *itemId = GetItemIdPtr(offset);
    UNUSE_PARAM uint32 pageDiskTupleSize = pageDiskTuple->GetTupleSize();
    uint16 oldTupleSize = undoData->GetOldTupleSize();

    /* Step 1: Undo the data of disk tuple */
    if (!IsTdValidAndOccupied(undoData->GetOldTdId())) {
        /*
         * The TD linked to the tuple might have been pruned or reset after the original undo record wrote
         * the undo record. Thus we must recheck the TD's status, and unlink it from tuple if it has already been
         * pruned or frozen.
         */
        pageDiskTuple->SetTdStatus(DETACH_TD);
    }
    if (unlikely(itemId->GetLen() < pageDiskTupleSize)) {
        char *page = Dump(false);
        if (page == nullptr) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Allocate memory fail when execute undo for inplace update."));
        }
        PrevDumpPage(page);
        DstorePfreeExt(page);
        UndoRecPtr preTrxUndoPtr = static_cast<UndoRecPtr>(record->m_header.m_txnPreUndoPtr);
        ErrLog(DSTORE_PANIC, MODULE_UNDO,
               ErrMsg("Failed to check size for rollback inplace update:item{%hu, %u, %hu}, len:%hu, "
                      "pageDiskTupleSize:%hu, txnPreUndoPtr{%hu, %u, %hu}",
                      GetFileId(), GetBlockNum(), offset, itemId->GetLen(), pageDiskTupleSize,
                      preTrxUndoPtr.GetFileId(), preTrxUndoPtr.GetBlockNum(), preTrxUndoPtr.GetOffset()));
    }
    if (undoData->GetOldTupleSize() <= itemId->GetLen()) {
        undoData->UndoActionOnTuple(pageDiskTuple, oldTupleSize);
    } else {
        /* Copy page tuple and replay undo. */
        HeapTuple *tuple = CopyTuple(offset);
        if (unlikely(tuple == nullptr)) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Allocate memory fail when execute undo for inplace update."));
            return DSTORE_FAIL;
        }
        tuple = ExpandTupleSize(tuple, oldTupleSize);
        if (unlikely(tuple == nullptr)) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Allocate memory fail when execute undo for inplace update."));
            return DSTORE_FAIL;
        }
        undoData->UndoActionOnTuple(tuple->GetDiskTuple(), oldTupleSize);

        /* Recycle page tuple space and add replayed one. */
        itemId->SetNoStorage();
        itemId->SetTdStatus(DETACH_TD);
        if (STORAGE_FUNC_FAIL(CompactCRPageIfFreeSpaceLessThan(oldTupleSize))) {
            DstorePfree(tuple);
            return DSTORE_FAIL;
        }
        UNUSE_PARAM OffsetNumber newOffset = AddTuple(tuple->GetDiskTuple(), oldTupleSize, offset);
        StorageAssert(newOffset == offset);
        DstorePfree(tuple);
    }

    /*
     * Step 2: Undo new TD using the old xid and undo rec ptr of new td
     *     warning: we must roll back td after disk tuple to avoid recycling the td when compacting CR page.
     */
    uint8 tdId = record->GetTdId();
    TD* td = GetTd(tdId);
    Xid curXid = td->GetXid();

    td->RollbackTdInfo(record);

    /*
     * The old TD may have been reused, even though it was not reused when generating undo record.
     * So we set tdId ATTACH_TD_AS_HISTORY_OWNER for consistency even if the TD is not reused in fact.
     */
    Xid tupleXid = GetDiskTuple(offset)->GetXid();
    if (!GetDiskTuple(offset)->TestTdStatus(DETACH_TD) && (tupleXid != curXid)) {
        GetDiskTuple(offset)->SetTdStatus(ATTACH_TD_AS_HISTORY_OWNER);
    }
    return DSTORE_SUCC;
}

RetStatus HeapPage::ExecuteUndoForSamePageAppendUpdate(UndoRecord *record, void* undoDataRec)
{
    UndoDataHeapSamePageAppendUpdate *undoData = nullptr;
    if (unlikely(record->GetUndoData() == nullptr)) {
        undoData = static_cast<UndoDataHeapSamePageAppendUpdate *>(undoDataRec);
    } else {
        undoData = static_cast<UndoDataHeapSamePageAppendUpdate *>(record->GetUndoData());
    }
    if (unlikely(undoData == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_UNDO, ErrMsg("Undo data is null when execute undo for same page update!"));
        return DSTORE_FAIL;
    }

    OffsetNumber offset = record->GetCtid().GetOffset();

    /* Step 1: Undo new TD using the old xid and old undo rec ptr of new td */
    TD* td = GetTd(record->GetTdId());
    Xid curXid = td->GetXid();
    td->RollbackTdInfo(record);

    /* Step 2: Undo the data of disk tuple */
    HeapDiskTuple *diskTuple = undoData->GetDiskTuple();
    uint32 diskTupleSize = undoData->GetRawDataSize();

    /*
     * The TD linked to the tuple might have been pruned or reset after the original undo record wrote
     * the undo record. Thus we must recheck the TD's status, and unlink it from tuple if it has already been pruned
     * or frozen.
     */
    if (!IsTdValidAndOccupied(diskTuple->GetTdId())) {
        diskTuple->SetTdStatus(DETACH_TD);
    }

    UpdateTuple(offset, diskTuple, diskTupleSize);
    ItemId *itemId = GetItemIdPtr(offset);
    itemId->SetLen(diskTupleSize);

    /*
     * The old TD may have been reused, even though it was not reused when generating undo record.
     * So we set tdId ATTACH_TD_AS_HISTORY_OWNER for consistency even if the TD is not reused in fact.
     */
    Xid tupleXid = GetDiskTuple(offset)->GetXid();
    if (!GetDiskTuple(offset)->TestTdStatus(DETACH_TD) && (tupleXid != curXid)) {
        GetDiskTuple(offset)->SetTdStatus(ATTACH_TD_AS_HISTORY_OWNER);
    }
    return DSTORE_SUCC;
}

RetStatus HeapPage::ExecuteUndoForOldPageAppendUpdate(UndoRecord *record, void* undoDataRec)
{
    UndoDataHeapAnotherPageAppendUpdate *undoData = nullptr;
    if (unlikely(record->GetUndoData() == nullptr)) {
        undoData = static_cast<UndoDataHeapAnotherPageAppendUpdate *>(undoDataRec);
    } else {
        undoData = static_cast<UndoDataHeapAnotherPageAppendUpdate *>(record->GetUndoData());
    }
    if (unlikely(undoData == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_UNDO, ErrMsg("Undo data is null when execute undo for old page update!"));
        return DSTORE_FAIL;
    }
    /* Step 1: Undo the data of disk tuple */
    HeapDiskTuple *diskTuple = undoData->GetDiskTuple();
    uint16 diskTupleSize = undoData->GetRawDataSize();
    OffsetNumber offset = record->GetCtid().GetOffset();
    ItemId *itemId = GetItemIdPtr(offset);

    /*
     * The TD linked to the tuple might have been pruned or reset after the original undo record wrote
     * the undo record. Thus we must recheck the TD's status, and unlink it from tuple if it has already been
     * pruned or frozen.
     * We must change the TD ID on tuple before add tuple on page, otherwise we would fail for INVALID TD ID.
     */
    if (!IsTdValidAndOccupied(diskTuple->GetTdId())) {
        diskTuple->SetTdStatus(DETACH_TD);
    }

    /* We need to consider rollback current transaction and flashback query */
    if (itemId->IsNormal() && diskTupleSize <= itemId->GetLen()) {
        UpdateTuple(offset, diskTuple, diskTupleSize);
    } else {
        if (STORAGE_FUNC_FAIL(CompactCRPageIfFreeSpaceLessThan(diskTupleSize))) {
            return DSTORE_FAIL;
        }
        UNUSE_PARAM OffsetNumber newOffset = AddTuple(diskTuple, diskTupleSize, offset);
        StorageAssert(newOffset == offset);
        StorageReleasePanic(newOffset != offset, MODULE_HEAP, ErrMsg("Failed to ExecuteUndoForOldPageAppendUpdate"));
    }

    /*
     * Step 2: Undo new TD using the old xid and old undo rec ptr of new td
     * warning: we must roll back td after compacting cr page to avoid recycling current td, because the caller may use
     * the td pointer to construct cr.
     */
    uint8 tdId = record->GetTdId();
    TD* td = GetTd(tdId);
    Xid curXid = td->GetXid();
    td->RollbackTdInfo(record);

    /*
     * The old TD may have been reused, even though it was not reused when generating undo record.
     * So we set tdId ATTACH_TD_AS_HISTORY_OWNER for consistency even if the TD is not reused in fact.
     */
    Xid tupleXid = GetDiskTuple(offset)->GetXid();
    if (!GetDiskTuple(offset)->TestTdStatus(DETACH_TD) && (tupleXid != curXid)) {
        GetDiskTuple(offset)->SetTdStatus(ATTACH_TD_AS_HISTORY_OWNER);
    }
    return DSTORE_SUCC;
}

void HeapPage::ExecuteUndoForNewPageAppendUpdate(UndoRecord *record)
{
    /* Step 1: Undo new TD using the old xid and old undo rec ptr of new td */
    GetTd(record->GetTdId())->RollbackTdInfo(record);
    /* Step 2: Undo ItemId */
    OffsetNumber offset = record->GetCtid().GetOffset();
    ItemId *itemId = GetItemIdPtr(offset);
    AddPotentialDelItemSize(offset);

    itemId->SetUnused();
    SetTuplePrunable(true);

    SetHasFreeItemId();
}

/*
 * This function is to compact page if the page doesn't have enough freespace,
 * called only when applying undo record during constructing CR page.
 *
 * (1) Need to recycle TD and Item Id space newly allocated after last prune, or the space
 * may not be enough for all tuples before last prune. To do this, we simply recycle
 * TDs and Item Ids that can no longer be seen by the transaction from back to front.
 *
 * (2) Tuples need to be compacted as well.
 */
RetStatus HeapPage::CompactCRPageIfFreeSpaceLessThan(uint32 needSpace)
{
    AutoMemCxtSwitch autoMemCxtSwitch(thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_STACK));
    if (GetFreeSpace<FreeSpaceCondition::RAW>() >= needSpace) {
        return DSTORE_SUCC;
    }

    /*
     * Step 1: Try recycle td space backwards.
     * If xid == INVALID_XID, the td is newly allocated or frozen. Transaction is not able to
     * visited it during CR page construction, it is safe to be recycled.
     */
    uint8 recycleTdCnt = 0;
    for (int tdId = dataHeader.tdCount - 1; tdId >= 0; tdId--) {
        TD *td = GetTd(static_cast<TdId>(tdId));
        if (td->GetXid() == INVALID_XID) {
            StorageAssert(td->GetUndoRecPtr() == INVALID_UNDO_RECORD_PTR);
            recycleTdCnt++;
        } else {
            break;
        }
    }
    RecycleTd(recycleTdCnt);

    /*
     * Step 2: Try recycle items backwards.
     * If item is unused, it is either an empty item, or a deleted item which CSN < global min CSN.
     * It is also safe to be recycled.
     */
    for (OffsetNumber offset = GetMaxOffset(); offset >= FIRST_ITEM_OFFSET_NUMBER; offset--) {
        ItemId *itemId = GetItemIdPtr(offset);
        if (itemId->IsUnused()) {
            RemoveLastItem();
        } else {
            break;
        }
    }

    /* Step 3: Collect remain tuples. */
    ItemIdCompactData *compactItems =
        static_cast<ItemIdCompactData *>(DstorePalloc(sizeof(ItemIdCompactData) * GetMaxOffset()));
    if (unlikely(compactItems == nullptr)) {
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("DstorePalloc fail size(%lu).",
                sizeof(ItemIdCompactData) * GetMaxOffset()));
        return DSTORE_FAIL;
    }

    uint32 nItems = 0;
    for (OffsetNumber offset = FIRST_ITEM_OFFSET_NUMBER; offset <= GetMaxOffset(); offset++) {
        ItemId *itemId = GetItemIdPtr(offset);
        if (itemId->IsNormal() && itemId->HasStorage()) {
            compactItems[nItems].tupOffset = itemId->GetOffset();
            compactItems[nItems].itemOffnum = offset;
            compactItems[nItems].itemLen = itemId->GetLen();
            nItems++;
        }
    }

    /* Step 4: Sort items descending by offset on page. */
    qsort(compactItems, nItems, sizeof(ItemIdCompactData), ItemIdCompactData::Compare);

    /*
     * Step 5: Compact tuple space to remove gaps between tuples.
     * This is possible because undo operation does not rely on physical location of tuple.
     */
    uint16 newUpper = CompactTuples(compactItems, nItems);
    DstorePfree(compactItems);
    SetUpper(newUpper);
    StorageAssert(CheckHeapPageSanity());
    StorageAssert(GetFreeSpace<FreeSpaceCondition::RAW>() >= needSpace);
    return DSTORE_SUCC;
}

HeapTuple *HeapPage::ConstructTupleFromUndoDelete(UndoRecord *record)
{
    UNUSE_PARAM AutoMemCxtSwitch autoMemCxtSwitch(thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_QUERY));
    HeapTuple *tuple = nullptr;
    UndoDataHeapDelete *undoData = static_cast<UndoDataHeapDelete *>(record->GetUndoData());
    HeapDiskTuple *diskTuple = undoData->GetDiskTuple();
    uint32 diskTupleSize = undoData->GetRawDataSize();

    tuple = static_cast<HeapTuple *>(DstorePalloc(sizeof(HeapTuple) + diskTupleSize));
    if (unlikely(tuple == nullptr)) {
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("DstorePalloc fail size(%lu).",
                sizeof(HeapTuple) + diskTupleSize));
        return nullptr;
    }
    tuple->Init();
    tuple->m_diskTuple = static_cast<HeapDiskTuple *>(
        static_cast<void *>(static_cast<char *>(static_cast<void *>(tuple)) + sizeof(HeapTuple)));
    errno_t rc = memcpy_s(tuple->m_diskTuple, diskTupleSize, diskTuple, diskTupleSize);
    storage_securec_check(rc, "\0", "\0");
    tuple->SetDiskTupleSize(diskTupleSize);
    StorageAssert(GetSelfPageId() == record->GetPageId());
    tuple->SetCtid(record->GetCtid());

    return tuple;
}

HeapTuple *HeapPage::ConstructTupleFromInplaceUpdate(UndoRecord *record, HeapTuple *tuple)
{
    if (tuple == nullptr) {
        ErrLog(DSTORE_PANIC, MODULE_HEAP,
            ErrMsg("Heap tuple is null when constructing tuple from inplace update!"));
        return nullptr;
    }
    UndoDataHeapInplaceUpdate *undoData = static_cast<UndoDataHeapInplaceUpdate *>(record->GetUndoData());
    uint32 oldTupleSize = undoData->GetOldTupleSize();
    if (oldTupleSize > tuple->GetDiskTupleSize()) {
        tuple = ExpandTupleSize(tuple, oldTupleSize);
        if (tuple == nullptr) {
            ErrLog(DSTORE_ERROR, MODULE_HEAP,
                ErrMsg("Heap tuple is null when constructing tuple from expand tuple."));
            return nullptr;
        }
    }
    undoData->UndoActionOnTuple(tuple->m_diskTuple, static_cast<uint16>(oldTupleSize));
    tuple->SetDiskTupleSize(oldTupleSize);
    StorageAssert(GetSelfPageId() == record->GetPageId());
    tuple->SetCtid(record->GetCtid());
    return tuple;
}

HeapTuple *HeapPage::ConstructTupleFromSamePageUpdate(UndoRecord *record)
{
    UNUSE_PARAM AutoMemCxtSwitch autoMemCxtSwitch(thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_QUERY));
    UndoDataHeapSamePageAppendUpdate *undoData = static_cast<UndoDataHeapSamePageAppendUpdate *>(record->GetUndoData());
    uint16 diskTupleSize = undoData->GetRawDataSize();
    HeapTuple *resTup = static_cast<HeapTuple *>(DstorePalloc0(sizeof(HeapTuple) + diskTupleSize));
    if (unlikely(resTup == nullptr)) {
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("DstorePalloc0 failed when ConstructTupleFromSamePageUpdate."));
        return nullptr;
    }
    resTup->m_diskTuple = static_cast<HeapDiskTuple *>(
        static_cast<void *>(static_cast<char *>(static_cast<void *>(resTup)) + sizeof(HeapTuple)));
    errno_t rc = memcpy_s(resTup->GetDiskTuple(), diskTupleSize, undoData->rawData, diskTupleSize);
    storage_securec_check(rc, "\0", "\0");
    resTup->SetDiskTupleSize(diskTupleSize);
    StorageAssert(GetSelfPageId() == record->GetPageId());
    resTup->SetCtid(record->GetCtid());
    return resTup;
}

HeapTuple *HeapPage::ConstructTupleFromAnotherPageUpdateOldPage(UndoRecord *record)
{
    UNUSE_PARAM AutoMemCxtSwitch autoMemCxtSwitch(thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_QUERY));
    UndoDataHeapAnotherPageAppendUpdate *undoData =
        static_cast<UndoDataHeapAnotherPageAppendUpdate *>(record->GetUndoData());
    uint16 diskTupleSize = undoData->GetRawDataSize();
    HeapTuple *resTup = static_cast<HeapTuple *>(DstorePalloc0(sizeof(HeapTuple) + diskTupleSize));
    if (unlikely(resTup == nullptr)) {
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        ErrLog(DSTORE_ERROR, MODULE_HEAP,
               ErrMsg("DstorePalloc0 failed when ConstructTupleFromAnotherPageUpdateOldPage."));
        return nullptr;
    }
    resTup->m_diskTuple = static_cast<HeapDiskTuple *>(
        static_cast<void *>(static_cast<char *>(static_cast<void *>(resTup)) + sizeof(HeapTuple)));
    errno_t rc = memcpy_s(resTup->GetDiskTuple(), diskTupleSize, undoData->rawData, diskTupleSize);
    storage_securec_check(rc, "\0", "\0");
    resTup->SetDiskTupleSize(diskTupleSize);
    resTup->SetCtid(record->GetCtid());

    return resTup;
}

bool HeapPage::IsDiskTupleDeleted(OffsetNumber offset)
{
    if (!GetItemIdPtr(offset)->IsNormal()) {
        return true;
    }

    HeapDiskTuple *diskTup = GetDiskTuple(offset);
    if (diskTup->TestLiveMode(HeapDiskTupLiveMode::TUPLE_BY_NORMAL_DELETE) ||
        diskTup->TestLiveMode(HeapDiskTupLiveMode::OLD_TUPLE_BY_ANOTHER_PAGE_UPDATE)) {
        return true;
    }

    StorageReleasePanic(diskTup->TestLiveMode(HeapDiskTupLiveMode::OLD_TUPLE_BY_SAME_PAGE_UPDATE), MODULE_HEAP,
                        ErrMsg("Tuple live mode can not be OLD_TUPLE_BY_SAME_PAGE_UPDATE!"));

    return false;
}

void HeapPage::ConstructCrTupleFromUndo(UndoRecord *record, HeapTuple **resTuple)
{
    switch (record->GetUndoType()) {
        case UNDO_HEAP_DELETE:
        case UNDO_HEAP_DELETE_TMP: {
            if (*resTuple != nullptr) {
                DstorePfree(*resTuple);
            }
            *resTuple = ConstructTupleFromUndoDelete(record);
            break;
        }

        case UNDO_HEAP_INPLACE_UPDATE:
        case UNDO_HEAP_INPLACE_UPDATE_TMP: {
            *resTuple = ConstructTupleFromInplaceUpdate(record, *resTuple);
            break;
        }

        case UNDO_HEAP_SAME_PAGE_APPEND_UPDATE:
        case UNDO_HEAP_SAME_PAGE_APPEND_UPDATE_TMP: {
            if (*resTuple != nullptr) {
                DstorePfree(*resTuple);
            }
            *resTuple = ConstructTupleFromSamePageUpdate(record);
            break;
        }

        case UNDO_HEAP_ANOTHER_PAGE_APPEND_UPDATE_OLD_PAGE:
        case UNDO_HEAP_ANOTHER_PAGE_APPEND_UPDATE_OLD_PAGE_TMP: {
            if (*resTuple != nullptr) {
                DstorePfree(*resTuple);
            }
            *resTuple = ConstructTupleFromAnotherPageUpdateOldPage(record);
            break;
        }

        case UNDO_HEAP_INSERT:
        case UNDO_HEAP_INSERT_TMP:
        case UNDO_HEAP_BATCH_INSERT:
        case UNDO_HEAP_BATCH_INSERT_TMP:
        case UNDO_HEAP_ANOTHER_PAGE_APPEND_UPDATE_NEW_PAGE:
        case UNDO_HEAP_ANOTHER_PAGE_APPEND_UPDATE_NEW_PAGE_TMP: {
            if (*resTuple != nullptr) {
                DstorePfree(*resTuple);
                *resTuple = nullptr;
            }
            break;
        }

        default: {
            ErrLog(DSTORE_PANIC, MODULE_HEAP,
                ErrMsg("Invalid undo type: %d", static_cast<int>(record->GetUndoType())));
        }
    }
}

RetStatus HeapPage::ConstructCrTuple(
    PdbId pdbId, Transaction *txn, ItemPointerData &ctid, TdId tdId, HeapTuple **resTuple, Snapshot snapshot)
{
    AutoMemCxtSwitch autoMemCxtSwitch(thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_STACK));
    TransactionMgr *txnMgr = g_storageInstance->GetPdb(pdbId)->GetTransactionMgr();
    TD *crTd = static_cast<TD *>(DstorePalloc(sizeof(TD) * GetTdCount()));
    if (unlikely(crTd == nullptr)) {
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("DstorePalloc fail size(%lu).",
                sizeof(TD) * GetTdCount()));
        return DSTORE_FAIL;
    }
    TdId tupleTdId = tdId;
    UndoRecord record;
    RetStatus status = DSTORE_SUCC;
    ItemPointerData oldCtid DSTORE_PG_USED_FOR_ASSERTS_ONLY = ctid;

    /* Initialize tds to be the same as on the page. */
    for (uint8 i = 0; i < GetTdCount(); ++i) {
        crTd[i] = *GetTd(i);
    }

    for (;;) {
        /* Step 1: fetch matched undo record */
        Xid xid = crTd[tupleTdId].GetXid();
        CommitSeqNo csn = crTd[tupleTdId].TestCsnStatus(IS_CUR_XID_CSN) ? crTd[tupleTdId].GetCsn() : INVALID_CSN;
        UndoRecPtr undoRecPtr = crTd[tupleTdId].GetUndoRecPtr();
        bool txnVisible = false;

        if (undoRecPtr == INVALID_UNDO_RECORD_PTR) {
            StorageAssert(xid == INVALID_XID);
            txnVisible = true;
        } else if (STORAGE_FUNC_SUCC(txnMgr->FetchUndoRecordByMatchedCtid(xid, &record, undoRecPtr, ctid, &csn))) {
            crTd[tupleTdId].RollbackTdInfo(&record);
            txnVisible = txn->IsCurrent(xid) ? CidVisibleToSnapshot(txn, snapshot, record.GetCid()) :
                (csn == INVALID_CSN ? XidVisibleToSnapshot(snapshot, xid, txn) : XidVisibleToSnapshot(snapshot, csn));
            /*
             * If cur xid is visible and the undo type is another-page-update, we need to switch a ctid of new page
             * to find the visible version of tuple.
             */
            if (txnVisible && (record.GetUndoType() == UNDO_HEAP_ANOTHER_PAGE_APPEND_UPDATE_OLD_PAGE ||
                record.GetUndoType() == UNDO_HEAP_ANOTHER_PAGE_APPEND_UPDATE_OLD_PAGE_TMP)) {
                UndoDataHeapAnotherPageAppendUpdate *undoData =
                    static_cast<UndoDataHeapAnotherPageAppendUpdate *>(record.GetUndoData());
                if (ctid != undoData->GetNewCtid()) {
                    ctid = undoData->GetNewCtid();
                }
            }
        } else {
            StorageAssert(StorageGetErrorCode() == UNDO_ERROR_RECORD_RECYCLED);
            /* we only need to judge if the tuple is visible. */
            txnVisible = true;
            StorageClearError();
        }

        /*
         * Step 2: The xid is matched with undo record and resTuple. If the transaction generated the tuple is
         *         visible, determine if the tuple is visible according to the live mode.
         */
        if (txnVisible) {
            if (*resTuple == nullptr) {
                status = DSTORE_SUCC;
                break;
            }
            HeapDiskTuple *diskTup = (*resTuple)->GetDiskTuple();
            StorageAssert(!diskTup->TestLiveMode(HeapDiskTupLiveMode::OLD_TUPLE_BY_SAME_PAGE_UPDATE));
            if (diskTup->TestLiveMode(HeapDiskTupLiveMode::TUPLE_BY_NORMAL_DELETE) ||
                diskTup->TestLiveMode(HeapDiskTupLiveMode::OLD_TUPLE_BY_ANOTHER_PAGE_UPDATE)) {
                DstorePfree(*resTuple);
                *resTuple = nullptr;
            }
            status = DSTORE_SUCC;
            break;
        }

        /* Step 3: The transaction generated the tuple is invisible. Construct tuple from undo. */
        ConstructCrTupleFromUndo(&record, resTuple);
        if (*resTuple) {
            /* Need switch td, if the td is reused */
            tupleTdId = (*resTuple)->GetTdId();
            if ((*resTuple)->GetDiskTuple()->TestTdStatus(DETACH_TD)) {
                crTd[tupleTdId].Reset();
            }
            StorageAssert(*((*resTuple)->GetCtid()) == oldCtid);
        }
    }

    DstorePfree(crTd);
    return status;
}

RetStatus HeapPage::UndoHeap(UndoRecord *record, void* undoData)
{
    RetStatus result = DSTORE_SUCC;
    UndoType type = record->GetUndoType();
    switch (type) {
        case UNDO_HEAP_INSERT:
        case UNDO_HEAP_INSERT_TMP: {
            ExecuteUndoForInsert(record);
            break;
        }
        case UNDO_HEAP_BATCH_INSERT:
        case UNDO_HEAP_BATCH_INSERT_TMP: {
            ExecuteUndoForBatchInsert(record, undoData);
            break;
        }
        case UNDO_HEAP_DELETE:
        case UNDO_HEAP_DELETE_TMP: {
            result = ExecuteUndoForDelete(record, undoData);
            break;
        }
        case UNDO_HEAP_INPLACE_UPDATE:
        case UNDO_HEAP_INPLACE_UPDATE_TMP: {
            result = ExecuteUndoForInplaceUpdate(record, undoData);
            break;
        }
        case UNDO_HEAP_SAME_PAGE_APPEND_UPDATE:
        case UNDO_HEAP_SAME_PAGE_APPEND_UPDATE_TMP: {
            result = ExecuteUndoForSamePageAppendUpdate(record, undoData);
            break;
        }
        case UNDO_HEAP_ANOTHER_PAGE_APPEND_UPDATE_OLD_PAGE:
        case UNDO_HEAP_ANOTHER_PAGE_APPEND_UPDATE_OLD_PAGE_TMP: {
            result = ExecuteUndoForOldPageAppendUpdate(record, undoData);
            break;
        }
        case UNDO_HEAP_ANOTHER_PAGE_APPEND_UPDATE_NEW_PAGE:
        case UNDO_HEAP_ANOTHER_PAGE_APPEND_UPDATE_NEW_PAGE_TMP: {
            ExecuteUndoForNewPageAppendUpdate(record);
            break;
        }
        default: {
            StorageAssert(false);
            return DSTORE_FAIL;
        }
    }
    StorageAssert(CheckHeapPageSanity());
    return result;
}

RetStatus HeapPage::GetTupleXid(PdbId pdbId, Xid &xid, OffsetNumber offset)
{
    StorageAssert(g_storageInstance->GetPdb(pdbId) != nullptr);
    TransactionMgr *transactionMgr = g_storageInstance->GetPdb(pdbId)->GetTransactionMgr();
    if (TestTupleTdStatus(offset, DETACH_TD)) {
        xid = INVALID_XID;
        return DSTORE_SUCC;
    }

    TD *td = GetTd(GetTupleTdId(offset));
    if (TestTupleTdStatus(offset, ATTACH_TD_AS_NEW_OWNER)) {
        xid = td->GetXid();
        return DSTORE_SUCC;
    }

    ItemPointerData ctid(GetSelfPageId(), offset);
    return transactionMgr->GetXidFromUndo(xid, td, ctid);
}

void HeapPage::PruneItems(const ItemIdDiff *itemIdDiff, uint16 nItems)
{
    for (uint16 i = 0; i < nItems; ++i) {
        OffsetNumber offNum = itemIdDiff[i].offNum;
        ItemIdState state = itemIdDiff[i].newState;
        ItemId *itemId = GetItemIdPtr(offNum);
        if (state == ITEM_ID_UNUSED) {
            itemId->SetUnused();
            SetHasFreeItemId();
#ifdef DSTORE_USE_ASSERT_CHECKING
            ErrLog(DSTORE_DEBUG1, MODULE_HEAP,
                   ErrMsg("Prune tuple({%hu, %u}, %hu)", GetSelfPageId().m_fileId, GetSelfPageId().m_blockId, offNum));
#endif
        } else if (state == ITEM_ID_NO_STORAGE) {
            HeapDiskTuple *diskTup = GetDiskTuple(offNum);
            TdId tdId = diskTup->GetTdId();
            itemId->SetTdId(tdId);
            if (diskTup->TestTdStatus(ATTACH_TD_AS_HISTORY_OWNER)) {
                itemId->SetTdStatus(ATTACH_TD_AS_HISTORY_OWNER);
            } else {
                itemId->SetTdStatus(ATTACH_TD_AS_NEW_OWNER);
            }
            itemId->SetNoStorage();
            itemId->SetTupLiveMode(static_cast<uint32>(diskTup->GetLiveMode()));
        }
    }
}

uint16 HeapPage::ScanCompactableItems(ItemIdCompactData *compactItems, uint16 &notPrunedDelSize)
{
    uint16 nItems = 0;
    for (OffsetNumber offset = FIRST_ITEM_OFFSET_NUMBER; offset < OffsetNumberNext(GetMaxOffset()); ++offset) {
        ItemId *itemId = GetItemIdPtr(offset);
        if (itemId->IsNormal() && itemId->HasStorage()) {
            compactItems[nItems].tupOffset = itemId->GetOffset();
            compactItems[nItems].itemOffnum = offset;
            compactItems[nItems].itemLen = itemId->GetLen();
            nItems++;
            if (IsDiskTupleDeleted(offset)) {
                notPrunedDelSize += itemId->GetLen();
            }
        }
    }
    return nItems;
}

void HeapPage::TryCompactTuples()
{
    AutoMemCxtSwitch autoMemCxtSwitch(thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_STACK));
    /* Step 1: Collect tuples for compaction. */
    uint16 itemIdSize = OffsetNumberNext(GetMaxOffset());
    ItemIdCompactData *compactItems =
        static_cast<ItemIdCompactData *>(DstorePalloc(sizeof(ItemIdCompactData) * itemIdSize));
    if (unlikely(compactItems == nullptr)) {
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("DstorePalloc fail size(%u) when compact tuples.",
            static_cast<uint32>(sizeof(ItemIdCompactData) * itemIdSize)));
        return;
    }
    uint16 notPrunedDelSize = 0;
    uint16 nItems = ScanCompactableItems(compactItems, notPrunedDelSize);

    /* Step 2: Sort tuples descending by offset on page. */
    qsort(compactItems, nItems, sizeof(ItemIdCompactData), ItemIdCompactData::Compare);
    /* Step 3: Compact tuples to remove the gaps caused by the removed items. */
    uint16 newUpper = CompactTuples(compactItems, nItems);
    DstorePfree(compactItems);
    SetUpper(newUpper);
    SetPotentialDelSize(static_cast<uint16>(notPrunedDelSize));
    SetTuplePrunable(notPrunedDelSize > 0);
    StorageAssert(CheckHeapPageSanity());
}

#ifdef DSTORE_USE_ASSERT_CHECKING
bool HeapPage::CheckHeapPageSanity()
{
    StorageAssert(GetType() == PageType::HEAP_PAGE_TYPE);
    StorageAssert(DataPage::CheckSanity());

    /* Check item id point to a valid td. */
    for (OffsetNumber offset = 1; offset <= GetMaxOffset(); offset++) {
        ItemId *id = GetItemIdPtr(offset);
        if (!id->IsNormal() && !id->IsNoStorage()) {
            continue;
        }
        if ((!TestTupleTdStatus(offset, DETACH_TD)) && (GetTupleTdId(offset) >= GetTdCount())) {
            StorageAssert(false);
        }
    }

    return true;
}
#endif

/* HeapDiskTuple contain different info depending on type */
void DumpTupleType(StringInfoData* str, HeapDiskTuple* diskTup)
{
    switch (diskTup->GetLinkInfo()) {
        case static_cast<uint32>(HeapDiskTupLinkInfoType::TUP_NO_LINK_TYPE):
            str->append("HeapDiskTuple\n");
            break;
        case static_cast<uint32>(HeapDiskTupLinkInfoType::TUP_LINK_FIRST_CHUNK_TYPE):
            str->append("HeapDiskTuple (BigTuple First Linked Chunk)\n");
            break;
        case static_cast<uint32>(HeapDiskTupLinkInfoType::TUP_LINK_NOT_FIRST_CHUNK_TYPE):
            str->append("HeapDiskTuple (BigTuple Non-First Linked Chunk)\n");
            break;
        default:
            break;
    }
}

/* HeapDiskTuple fixed-size header data dump */
void DumpFixedSizeHeader(StringInfoData* str,
    HeapDiskTuple* diskTup, const char** tupLiveMode, const char** strTupleTdStatus)
{
    str->append("    m_tdId: %hhu\n", diskTup->GetTdId());
    if (diskTup->GetLockerTdId() == INVALID_TD_SLOT) {
        str->append("    m_lockerTdId: %s\n", "INVALID_TD_SLOT");
    } else {
        str->append("    m_lockerTdId: %d\n", diskTup->GetLockerTdId());
    }
    str->append("    m_size: %hu\n", diskTup->GetTupleSize());
    str->append("    m_xid: %lu, zone_id: %lu, slot_id : %lu\n", diskTup->GetXid().m_placeHolder,
        (uint64)diskTup->GetXid().m_zoneId, diskTup->GetXid().m_logicSlotId);
    /* Non-first link chunk does not contain m_info */
    if (!diskTup->IsNotFirstLinkChunk()) {
        /* m_info data dump */
        str->append("    m_info:\n");
        str->append("        m_hasNull: %u\n", static_cast<uint>(diskTup->HasNull()));
        str->append("        m_hasVarwidth: %u\n", static_cast<uint>(diskTup->HasVariable()));
        str->append("        m_hasExternal: %u\n", static_cast<uint>(diskTup->HasExternal()));
        str->append("        m_hasOid: %u\n", static_cast<uint>(diskTup->HasOid()));
        str->append("        m_tdStatus: %s\n", (strTupleTdStatus)[diskTup->GetTdStatus()]);
        str->append("        m_liveMode: %s\n", (tupLiveMode)[static_cast<int>(diskTup->GetLiveMode())]);
        str->append("        m_linkInfo: %u\n", diskTup->GetLinkInfo());
        str->append("        m_numColumn: %d\n", diskTup->GetNumColumn());
    }
}

/* Linked tuple chunk header data dump */
void DumpTupleChunk(StringInfoData* str, HeapDiskTuple* diskTup)
{
    if (diskTup->IsLinked()) {
        if (diskTup->GetNextChunkCtid() == INVALID_ITEM_POINTER) {
            str->append("    Next Linked Chunk's CTID (ItemPointerData): INVALID_ITEM_POINTER\n");
        } else {
            str->append("    Next Linked Chunk's CTID (ItemPointerData): (%d, %u, %d)\n",
                diskTup->GetNextChunkCtid().GetFileId(),
                diskTup->GetNextChunkCtid().GetBlockNum(),
                diskTup->GetNextChunkCtid().GetOffset());
        }
        if (diskTup->IsFirstLinkChunk()) {
            str->append("    NumChunks (Uint32): %u\n", diskTup->GetNumChunks());
        }
    }
}

/* Non-first linked tuple chunk header data dump.  Non-first link chunk does not contain nullbitmap nor OID. */
void DumpNonFirstChunk(StringInfoData* str,
    HeapDiskTuple* diskTup, uint8 numHexPerChunk, uint8 numChunksPerRow, int alignShift)
{
    if (!numHexPerChunk || !numChunksPerRow) {  // Avoid modulo by 0
        return;
    }

    /* Nullbitmap data dump */
    if (diskTup->HasNull()) {
        uint16 numHexPerRow = static_cast<uint16>(numHexPerChunk) * static_cast<uint16>(numChunksPerRow);
        str->append("    Nullbitmap (Hex):");
        uint16 numCol = diskTup->GetNumColumn();
        for (uint32 byteIdx = 0; byteIdx < HeapDiskTuple::GetBitmapLen(numCol); byteIdx++) {
            if ((byteIdx % numHexPerRow == 0U) &&
                (HeapDiskTuple::GetBitmapLen(numCol) > numHexPerRow)) {
                str->append("%s", "\n        ");
            } else if (byteIdx % (numHexPerChunk) == 0U) {
                str->append("%s", " ");
            }

            uint8* nullBitMapPtr = diskTup->GetNullBitmap();
            if (nullBitMapPtr == nullptr) {
                StorageAssert(false);
            } else {
                str->append("%02X", nullBitMapPtr[byteIdx] & 0xFFU);
            }
        }
        str->append("%s", "\n");
    }

    /* OID data dump */
    if (diskTup->HasOid()) {
        str->append("    OID (Uint32): %u\n",
            *static_cast<Oid*>(static_cast<void*>((diskTup->GetValues() - sizeof(Oid)) + alignShift)));
    }
}

/* Dumping just the values of the tuple, in hex. */
void DumpValues(StringInfoData* str,
    HeapDiskTuple* diskTup, uint8 numHexPerChunk, uint8 numChunksPerRow, int alignShift)
{
    if (!numHexPerChunk || !numChunksPerRow) {  // Avoid modulo by 0
        return;
    }

    uint16 numHexPerRow = static_cast<uint16>(numHexPerChunk) * static_cast<uint16>(numChunksPerRow);
    uint32 dataLen = diskTup->GetTupleSize() -
        static_cast<uint32>(static_cast<int>(diskTup->GetValuesOffset()) + alignShift);
    str->append("    Data (Length = %u Bytes):", dataLen);

    for (uint32 byteIdx = 0; byteIdx < dataLen; ++byteIdx) {
        if (byteIdx % numHexPerRow == 0U) {
            str->append("%s", "\n        ");
        } else if (byteIdx % (numHexPerChunk) == 0U) {
            str->append("%s", " ");
        }
        char* charValuePtr = (diskTup->GetValues() + alignShift) + byteIdx;
        uint uintValue = *static_cast<uint*>(static_cast<void*>(charValuePtr));
        str->append("%02X", uintValue & 0xFFU);
    }
    str->append("%s", "\n");
}

char* HeapPage::Dump(bool showTupleData)
{
#ifndef DSTORE_USE_ASSERT_CHECKING
    showTupleData = false;
#endif
    StringInfoData str;
    if (unlikely(!str.init())) {
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("DstorePalloc fail."));
        return nullptr;
    }

    /* Step 1: Print header */
    str.append("HeapPageHeader\n");
    DataPage::DumpDataPageHeader(&str);

    if (HasPrunableItem()) {
        str.append(" PD_ITEM_PRUNABLE ");
    }
    if (HasPrunableTuple()) {
        str.append(" PD_TUPLE_PRUNABLE ");
    }
    str.append("\n");

    static const char *tupLiveMode[] = {
        "TUPLE_BY_NORMAL_INSERT",
        "NEW_TUPLE_BY_INPLACE_UPDATE",
        "NEW_TUPLE_BY_SAME_PAGE_UPDATE",
        "OLD_TUPLE_BY_SAME_PAGE_UPDATE",
        "OLD_TUPLE_BY_ANOTHER_PAGE_UPDATE",
        "NEW_TUPLE_BY_ANOTHER_PAGE_UPDATE",
        "TUPLE_BY_NORMAL_DELETE"
    };

    static const char *strTupleTdStatus[3] = {
        "attached as new owner    ",
        "attached as history onwer",
        "detached                 ",
    };

    /* Step 2: Print item id */
    uint8 numHexPerChunk = 8;     /* Display options for data dump */
    uint8 numChunksPerRow = 4;
    for (OffsetNumber i = 1; i <= GetMaxOffset(); ++i) {
        ItemId *itemId = GetItemIdPtr(i);
        if (itemId->IsNormal()) {
            str.append("ItemId %hu: (%hu, %hu)\n", i, itemId->GetOffset(), itemId->GetLen());
            HeapDiskTuple *diskTup = GetDiskTuple(i);

            DumpTupleType(&str, diskTup);
            DumpFixedSizeHeader(&str, diskTup, tupLiveMode, strTupleTdStatus);

            /*
             * alignShift is needed because linked tuples have different alignment and cannot use function
             * HeapDiskTuple::GetValuesOffset().
             * Eg: Dumping the first linked chunk of a big tuple with no OID and no nulls:
             *     Written to the page, the true value offset
             *     = sizeof(HeapDiskTuple header) +  LINKED_TUP_CHUNK_EXTRA_HEADER_SIZE = 8 + 9 = 17
             *     However, GetValuesOffset() returns
             *     MAXALIGN(sizeof(HeapDiskTuple header) +  LINKED_TUP_CHUNK_EXTRA_HEADER_SIZE) = MAXALIGN(17) = 24
             *     alignShift = true value offset - GetValuesOffset() = 17 - 24 = -7
             */
            int alignShift = (diskTup->IsLinked()) ?
                static_cast<int>(HeapDiskTuple::GetValuesOffset(
                    diskTup->GetNumColumn(), diskTup->HasNull(), diskTup->HasOid(), false) +
                    LINKED_TUP_CHUNK_EXTRA_HEADER_SIZE - diskTup->GetValuesOffset()) : 0;

            if (showTupleData) {
                DumpValues(&str, diskTup, numHexPerChunk, numChunksPerRow, alignShift);
            }
            DumpTupleChunk(&str, diskTup);
            DumpNonFirstChunk(&str, diskTup, numHexPerChunk, numChunksPerRow, alignShift);
            str.append("%s", "\n");
        } else if (itemId->IsNoStorage()) {
            str.append("ItemId %hu: No Storage\n", i);
            str.append("    m_tdId: %hhu\n", itemId->GetTdId());
            str.append("    m_tdStatus: %s\n", strTupleTdStatus[itemId->GetTdStatus()]);
            str.append("    m_liveMode: %s\n", tupLiveMode[static_cast<int>(itemId->GetTupLiveMode())]);
        } else {
            str.append("ItemId %hu: Unused\n", i);
        }
    }
    return str.data;
}

void HeapPage::PrevDumpPage(char *page)
{
    if (unlikely(page == nullptr)) {
        ErrLog(DSTORE_WARNING, MODULE_HEAP, ErrMsg("Failed to dump page because page is null."));
        return;
    }
    ErrLog(DSTORE_LOG, MODULE_HEAP, ErrMsg("Prev dump heap page: %s.", page));
}

bool HeapPage::ConstructHistoricTuple(ShowAnyTupleContext *showAnyTuple, HeapTuple **resTuple)
{
    bool tupleFound = false;

    for (;;) {
        if (showAnyTuple->m_tdId == INVALID_TD_SLOT) {
            break;
        }

        UndoRecord record;
        Xid xid = showAnyTuple->m_td[showAnyTuple->m_tdId].GetXid();
        UndoRecPtr undoRecPtr = showAnyTuple->m_td[showAnyTuple->m_tdId].GetUndoRecPtr();
        TransactionMgr *txnMgr = g_storageInstance->GetPdb(showAnyTuple->m_pdbId)->GetTransactionMgr();
        if (STORAGE_FUNC_SUCC(txnMgr->FetchUndoRecordByMatchedCtid(xid, &record, undoRecPtr, showAnyTuple->m_ctid))) {
            UndoType undoType = record.GetUndoType();
            if (undoType == UNDO_HEAP_INSERT || undoType == UNDO_HEAP_INSERT_TMP ||
                undoType == UNDO_HEAP_BATCH_INSERT || undoType == UNDO_HEAP_BATCH_INSERT_TMP ||
                undoType == UNDO_HEAP_ANOTHER_PAGE_APPEND_UPDATE_NEW_PAGE ||
                undoType == UNDO_HEAP_ANOTHER_PAGE_APPEND_UPDATE_NEW_PAGE_TMP) {
                /* Tuple has no more version, stop at insert */
                showAnyTuple->m_tdId = INVALID_TD_SLOT;
                break;
            }

            /* build a mroe dead tuple */
            ConstructCrTupleFromUndo(&record, resTuple);
            StorageAssert(*resTuple != nullptr);
            (*resTuple)->SetDeleteXidForDebug(xid);
            showAnyTuple->m_lastTupCid = record.GetCid();
            StorageAssert(*((*resTuple)->GetCtid()) == showAnyTuple->m_ctid);

            /* prepare next tuple's undo tdId */
            showAnyTuple->m_td[showAnyTuple->m_tdId].RollbackTdInfo(&record);
            showAnyTuple->m_tdId = (*resTuple)->GetTdId();

            /* TD's undo chain was broken or reach its beginning. */
            if ((*resTuple)->GetDiskTuple()->TestTdStatus(DETACH_TD) ||
                showAnyTuple->m_td[showAnyTuple->m_tdId].GetUndoRecPtr() == INVALID_UNDO_RECORD_PTR) {
                showAnyTuple->m_tdId = INVALID_TD_SLOT;
            }
            tupleFound = true;
            break;
        } else {
            StorageAssert(StorageGetErrorCode() == UNDO_ERROR_RECORD_RECYCLED);
            StorageClearError();

            /* Tuple has no more version in undo chain. */
            showAnyTuple->m_tdId = INVALID_TD_SLOT;
        }
    }
    return tupleFound;
}

HeapTuple *HeapPage::ShowAnyTupleFetch(ShowAnyTupleContext *showAnyTuple)
{
    HeapTuple *resTuple = nullptr;
    bool lastTupleFetched = true;   /* whether anyTuple->m_lastTuple had been fetched or not. */

    if (!showAnyTuple->m_tupleInUndo) {
        showAnyTuple->m_tupleInUndo = true;
        OffsetNumber offset = showAnyTuple->m_ctid.GetOffset();

        ItemId *itemId = GetItemIdPtr(offset);
        if (likely(itemId->IsNormal())) {
            showAnyTuple->m_lastTuple = CopyTuple(offset);
        } else if (!itemId->IsNoStorage()) {
            /* unused or unreachable */
            return nullptr;
        }

        if (TestTupleTdStatus(offset, DETACH_TD)) {
            /* undo chain was broken, just return tuple in heap page. */
            showAnyTuple->m_tdId = INVALID_TD_SLOT;
        } else {
            /*
             * One more items could touched the same TD slot, so take a copy of crPage's TD[] to
             * build the historic tuple of this item.
             */
            showAnyTuple->m_tdId = GetTupleTdId(offset);
            showAnyTuple->m_td = static_cast<TD *>(DstorePalloc(sizeof(TD) * GetTdCount()));
            if (unlikely(showAnyTuple->m_td == nullptr)) {
                storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
                ErrLog(DSTORE_ERROR, MODULE_HEAP,
                    ErrMsg("DstorePalloc fail size(%lu). Invalid td for show any tuple context.",
                        sizeof(TD) * GetTdCount()));
                return nullptr;
            }
            for (uint8 i = 0; i < GetTdCount(); ++i) {
                showAnyTuple->m_td[i] = *GetTd(i);
            }
            if (TestTupleTdStatus(offset, ATTACH_TD_AS_NEW_OWNER)) {
                showAnyTuple->m_lastTupCid = showAnyTuple->m_td[showAnyTuple->m_tdId].GetCommandId();
            } else {
                showAnyTuple->m_lastTupCid = INVALID_CID;
            }
        }

        if (showAnyTuple->m_lastTuple != nullptr) {
            HeapDiskTuple *diskTup = GetDiskTuple(offset);
            if (diskTup->TestLiveMode(HeapDiskTupLiveMode::TUPLE_BY_NORMAL_DELETE) ||
                diskTup->TestLiveMode(HeapDiskTupLiveMode::OLD_TUPLE_BY_ANOTHER_PAGE_UPDATE)) {
                showAnyTuple->m_lastTuple->SetDeleteXidForDebug(showAnyTuple->m_lastTuple->GetXid());
                /* fill frozen xid, and continue to look for the insertXid in undo record. */
                showAnyTuple->m_lastTuple->SetXid(Xid(0));

                /* if undo chain was broken, this dead tuple should be returned. */
                resTuple = showAnyTuple->m_lastTuple;
            } else {
                /* return the alive tuple in heap page directly */
                showAnyTuple->m_lastTuple->SetDeleteXidForDebug(INVALID_XID);
                return showAnyTuple->m_lastTuple;
            }
            lastTupleFetched = false;
        }
    }

    /* build historic invisiable tuple from undo segemnt */
    if (showAnyTuple->m_tdId != INVALID_TD_SLOT) {
        // look delete or update for tuple and delete_xid
        if (!ConstructHistoricTuple(showAnyTuple, &showAnyTuple->m_lastTuple)) {
            /* no more older tuple was found. */
            if (lastTupleFetched) {
                DstorePfreeExt(showAnyTuple->m_lastTuple);
            } else {
                /* failed to find insertXid of the dead tuple from heap page. */
            }
        }
        resTuple = showAnyTuple->m_lastTuple;
    }

    return resTuple;
}

void ShowAnyTupleContext::CreateShowAnyTupleContext(PdbId pdbId, ShowAnyTupleContext *&showAnyTupContext)
{
    showAnyTupContext = static_cast<ShowAnyTupleContext*>(DstorePalloc0(sizeof(ShowAnyTupleContext)));
    StorageReleasePanic(showAnyTupContext == nullptr, MODULE_HEAP, ErrMsg("Invalid item any tuple context."));
    showAnyTupContext->m_ctid = INVALID_ITEM_POINTER;
    showAnyTupContext->m_tupleInUndo = false;
    showAnyTupContext->m_lastTuple = nullptr;
    showAnyTupContext->m_td = nullptr;
    showAnyTupContext->m_pdbId = pdbId;
}

void ShowAnyTupleContext::DestoryShowAnyTupleContext(ShowAnyTupleContext *&showAnyTupContext, bool skipLastTup)
{
    if (!skipLastTup) {
        DstorePfreeExt(showAnyTupContext->m_lastTuple);
    }
    DstorePfreeExt(showAnyTupContext->m_td);
    DstorePfree(showAnyTupContext);
    showAnyTupContext = nullptr;
}

void ShowAnyTupleContext::CheckBeforeItemFetch(ItemPointerData ctid)
{
    if (m_ctid != ctid) {
        /* cleanup the previous citd's context. */
        DstorePfreeExt(m_lastTuple);
        DstorePfreeExt(m_td);

        /* do initialization for new item. */
        m_ctid = ctid;
        m_tupleInUndo = false;
        m_tdId = INVALID_TD_SLOT;
        m_lastTupCid = INVALID_CID;
    }
}

bool ShowAnyTupleContext::IsItemFetchFinished()
{
    return (m_tupleInUndo && m_tdId == INVALID_TD_SLOT);
}

void ShowAnyTupleContext::GetLastTupleSnap(Xid &snapXid, CommandId &snapCid, bool &deleteXid)
{
    StorageAssert(m_lastTuple != nullptr);
    if (m_lastTuple->GetDeleteXidForDebug() == INVALID_XID) {
        snapXid = m_lastTuple->GetXid();
        deleteXid = false;
    } else {
        snapXid = m_lastTuple->GetDeleteXidForDebug();
        deleteXid = true;
    }
    snapCid = m_lastTupCid;
}

bool ShowAnyTupleContext::IsSnapMatchedTup(Xid snapXid, CommandId snapCid, bool deleteXid)
{
    /* no more tuole was found. */
    if (m_lastTuple == nullptr) {
        return true;
    }

    /* any found historic tuple is OK. */
    if (snapXid == INVALID_XID) {
        return true;
    }

    /* commandId doesn't match. */
    if (snapCid != m_lastTupCid) {
        return false;
    }

    /* found historic tuple with matched deletionXid and cid */
    if (deleteXid && snapXid == m_lastTuple->GetDeleteXidForDebug()) {
        return true;
    }

    /* found historic tuple with matched insertionXid and cid */
    if (!deleteXid && snapXid == m_lastTuple->GetXid()) {
        return true;
    }

    return false;
}

}  // namespace DSTORE
