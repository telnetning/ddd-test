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
 * dstore_heap_wal_struct.cpp
 *
 *
 *
 * IDENTIFICATION
 *        storage/src/heap/dstore_heap_wal_struct.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include <functional>
#include "common/log/dstore_log.h"
#include "common/dstore_datatype.h"
#include "page/dstore_heap_page.h"
#include "heap/dstore_heap_update.h"
#include "heap/dstore_heap_wal_struct.h"

namespace DSTORE {

struct HeapWalRedoItem {
    WalType type;
    std::function<void(const WalRecordHeap*, HeapPage *, Xid)> redo;

    HeapWalRedoItem(WalType walType, std::function<void(const WalRecordHeap *, HeapPage *, Xid)> redoFunc) noexcept
        : type(walType), redo(std::move(redoFunc)) {}
};

static const HeapWalRedoItem HEAP_WAL_REDO_TABLE[] {
    {WAL_HEAP_INSERT, [](const WalRecordHeap *self, HeapPage *page, Xid xid)
        { (static_cast<const WalRecordHeapInsert *>(self))->Redo(page, xid); }},
    {WAL_HEAP_BATCH_INSERT, [](const WalRecordHeap *self, HeapPage *page, Xid xid)
        { (static_cast<const WalRecordHeapBatchInsert *>(self))->Redo(page, xid); }},
    {WAL_HEAP_DELETE, [](const WalRecordHeap *self, HeapPage *page, Xid xid)
        { (static_cast<const WalRecordHeapDelete *>(self))->Redo(page, xid); }},
    {WAL_HEAP_INPLACE_UPDATE, [](const WalRecordHeap *self, HeapPage *page, Xid xid)
        { (static_cast<const WalRecordHeapInplaceUpdate *>(self))->Redo(page, xid); }},
    {WAL_HEAP_SAME_PAGE_APPEND, [](const WalRecordHeap *self, HeapPage *page, Xid xid)
        { (static_cast<const WalRecordHeapSamePageAppendUpdate *>(self))->Redo(page, xid); }},
    {WAL_HEAP_ANOTHER_PAGE_APPEND_UPDATE_OLD_PAGE, [](const WalRecordHeap *self, HeapPage *page, Xid xid)
        { (static_cast<const WalRecordHeapAnotherPageAppendUpdateOldPage *>(self))->Redo(page, xid); }},
    {WAL_HEAP_ANOTHER_PAGE_APPEND_UPDATE_NEW_PAGE, [](const WalRecordHeap *self, HeapPage *page, Xid xid)
        { (static_cast<const WalRecordHeapAnotherPageAppendUpdateNewPage *>(self))->Redo(page, xid); }},
    {WAL_HEAP_ALLOC_TD, [](const WalRecordHeap *self, HeapPage *page, Xid xid)
        { (static_cast<const WalRecordHeapAllocTd *>(self))->Redo(page, xid); }},
    {WAL_HEAP_PRUNE, [](const WalRecordHeap *self, HeapPage *page, Xid xid)
        { (static_cast<const WalRecordHeapPrune *>(self))->Redo(page, xid); }},
    {WAL_HEAP_FORCE_UPDATE_TUPLE_DATA_NO_TRX, [](const WalRecordHeap *self, HeapPage *page, Xid xid)
        { (static_cast<const WalRecordHeapForceUpdateTupleDataNoTrx *>(self))->Redo(page, xid); }}
};

struct HeapWalDescItem {
    WalType type;
    std::function<void(const WalRecordHeap*, FILE *)> desc;

    HeapWalDescItem(WalType walType, std::function<void(const WalRecordHeap *, FILE *)> descFunc) noexcept
        : type(walType), desc(std::move(descFunc)) {}
};

static const HeapWalDescItem HEAP_WAL_DESC_TABLE[] {
    {WAL_HEAP_INSERT, [](const WalRecordHeap *self, FILE *fp)
        { (static_cast<const WalRecordHeapInsert *>(self))->Dump(fp); }},
    {WAL_HEAP_BATCH_INSERT, [](const WalRecordHeap *self, FILE *fp)
        { (static_cast<const WalRecordHeapBatchInsert *>(self))->Dump(fp); }},
    {WAL_HEAP_DELETE, [](const WalRecordHeap *self, FILE *fp)
        { (static_cast<const WalRecordHeapDelete *>(self))->Dump(fp); }},
    {WAL_HEAP_INPLACE_UPDATE, [](const WalRecordHeap *self, FILE *fp)
        { (static_cast<const WalRecordHeapInplaceUpdate *>(self))->Dump(fp); }},
    {WAL_HEAP_SAME_PAGE_APPEND, [](const WalRecordHeap *self, FILE *fp)
        { (static_cast<const WalRecordHeapSamePageAppendUpdate *>(self))->Dump(fp); }},
    {WAL_HEAP_ANOTHER_PAGE_APPEND_UPDATE_OLD_PAGE, [](const WalRecordHeap *self, FILE *fp)
        { (static_cast<const WalRecordHeapAnotherPageAppendUpdateOldPage *>(self))->Dump(fp); }},
    {WAL_HEAP_ANOTHER_PAGE_APPEND_UPDATE_NEW_PAGE, [](const WalRecordHeap *self, FILE *fp)
        { (static_cast<const WalRecordHeapAnotherPageAppendUpdateNewPage *>(self))->Dump(fp); }},
    {WAL_HEAP_ALLOC_TD, [](const WalRecordHeap *self, FILE *fp)
        { (static_cast<const WalRecordHeapAllocTd *>(self))->Dump(fp); }},
    {WAL_HEAP_PRUNE, [](const WalRecordHeap *self, FILE *fp)
        { (static_cast<const WalRecordHeapPrune *>(self))->Dump(fp); }},
    {WAL_HEAP_FORCE_UPDATE_TUPLE_DATA_NO_TRX, [](const WalRecordHeap *self, FILE *fp)
        { (static_cast<const WalRecordHeapForceUpdateTupleDataNoTrx *>(self))->Dump(fp); }}
};

void WalRecordHeap::RedoHeapRecord(WalRecordRedoContext *redoCtx, const WalRecordHeap *heapRecord,
                                   BufferDesc *bufferDesc)
{
    StorageAssert(heapRecord != nullptr);
    PageId pageId = heapRecord->m_pageId;
    STORAGE_CHECK_BUFFER_PANIC(bufferDesc, MODULE_HEAP, pageId);

    HeapPage *page = static_cast<HeapPage *>(bufferDesc->GetPage());
    StorageAssert(page->GetSelfPageId() == pageId);

    WalType recordType = heapRecord->m_type;
    for (uint32 i = 0; i < sizeof(HEAP_WAL_REDO_TABLE) / sizeof(HEAP_WAL_REDO_TABLE[0]); ++i) {
        if (HEAP_WAL_REDO_TABLE[i].type == recordType) {
            HEAP_WAL_REDO_TABLE[i].redo(heapRecord, page, redoCtx->xid);
        }
    }

    const uint64 glsn = heapRecord->m_pagePreWalId != redoCtx->walId ? heapRecord->m_pagePreGlsn + 1
                                                                     : heapRecord->m_pagePreGlsn;
    page->SetLsn(redoCtx->walId, redoCtx->recordEndPlsn, glsn);
}

void WalRecordHeap::DumpHeapRecord(const WalRecordHeap *heapRecord, FILE *fp)
{
    uint32 len = sizeof(HEAP_WAL_DESC_TABLE) / sizeof(HEAP_WAL_DESC_TABLE[0]);
    for (uint32 i = 0; i < len; ++i) {
        if (HEAP_WAL_DESC_TABLE[i].type == heapRecord->m_type) {
            HEAP_WAL_DESC_TABLE[i].desc(heapRecord, fp);
        }
    }
}

void WalRecordHeapInsert::SetData(char *data, uint32 size)
{
    CopyData(rawData, static_cast<uint32>(m_size - sizeof(WalRecordHeapInsert)), data, size);
}

void WalRecordHeapInsert::Redo(HeapPage *page, Xid xid) const
{
    StorageAssert(page->GetSelfPageId() == GetPageId());

    const HeapDiskTuple *diskTup = static_cast<const HeapDiskTuple *>(static_cast<const void *>(rawData));
    uint32 allocTdSize = m_size - (sizeof(WalRecordHeapInsert) + diskTup->GetTupleSize());
    if (IsContainLoigcalInfo()) {
        allocTdSize -= (sizeof(Oid) + sizeof(CommitSeqNo));
        if (unlikely(IsDecodeDictChange())) {
            allocTdSize -= sizeof(CommandId);
        }
    }
    RedoAllocTdWal(page, PageType::HEAP_PAGE_TYPE, rawData + diskTup->GetTupleSize(), allocTdSize);

    OffsetNumber offsetNum = page->AddTuple(diskTup, static_cast<uint16>(diskTup->GetTupleSize()), GetOffset());
    StorageReleasePanic(offsetNum == INVALID_ITEM_OFFSET_NUMBER, MODULE_HEAP, ErrMsg("Add tuple failed, page(%hu, %u), "
        "offset(%hu), size(%hu).", page->GetFileId(), page->GetBlockNum(), GetOffset(), diskTup->GetTupleSize()));

    if (diskTup->GetTdId() != INVALID_TD_SLOT) {
        page->SetTd(diskTup->GetTdId(), xid, UndoRecPtr(undoRecPtr), INVALID_CID);
    }

    page->SetIsNewPage(false);
}

void WalRecordHeapBatchInsert::AppendData(OffsetNumber offset, char *data, uint32 size)
{
    CopyData(rawData + pos, sizeof(OffsetNumber), static_cast<char *>(static_cast<void *>(&offset)),
             sizeof(OffsetNumber));
    pos += sizeof(OffsetNumber);
    CopyData(rawData + pos, size, data, size);
    pos += static_cast<uint16>(size);
}

void WalRecordHeapBatchInsert::Redo(HeapPage *page, Xid xid) const
{
    RedoAllocTdWal(page, PageType::HEAP_PAGE_TYPE, rawData + pos, m_size - (sizeof(WalRecordHeapBatchInsert) + pos));

    uint8 tdId = INVALID_TD_SLOT;
    uint16 curPos = 0;
    while (curPos < pos) {
        OffsetNumber offset = *static_cast<const OffsetNumber *>(static_cast<const void *>(rawData + curPos));
        curPos += sizeof(OffsetNumber);
        const HeapDiskTuple *diskTup = static_cast<const HeapDiskTuple *>(static_cast<const void *>(rawData + curPos));
        tdId = diskTup->GetTdId();
        uint16 diskTupSize = diskTup->GetTupleSize();
        curPos += diskTupSize;
        offset = page->AddTuple(diskTup, diskTupSize, offset);
        StorageReleasePanic(offset == INVALID_ITEM_OFFSET_NUMBER, MODULE_HEAP, ErrMsg("Batch insert failed, "
            "page(%hu, %u), size(%hu).", page->GetFileId(), page->GetBlockNum(), diskTup->GetTupleSize()));
    }
    if (tdId != INVALID_TD_SLOT) {
        page->SetTd(tdId, xid, UndoRecPtr(undoRecPtr), INVALID_CID);
    }

    page->SetIsNewPage(false);
}

void WalRecordHeapDelete::Redo(HeapPage *page, Xid xid) const
{
    StorageAssert(page->GetSelfPageId() == GetPageId());
    uint32 fromPos = 0;
    if (IsContainLoigcalInfo()) {
        StorageAssert(IsDeleteContainsReplicaKey());
        const HeapDiskTuple *tuple = static_cast<const HeapDiskTuple *>(static_cast<const void *>(rawData));
        fromPos += tuple->GetTupleSize();
        fromPos = fromPos + sizeof(Oid) + sizeof(CommitSeqNo);
        if (unlikely(IsDecodeDictChange())) {
            fromPos += sizeof(CommandId);
        }
    }
    RedoAllocTdWal(page, PageType::HEAP_PAGE_TYPE, rawData + fromPos, m_size - (sizeof(WalRecordHeapDelete) + fromPos));

    HeapDiskTuple *diskTup = page->GetDiskTuple(GetOffset());
    diskTup->SetTdStatus(ATTACH_TD_AS_NEW_OWNER);
    diskTup->SetTdId(tdId);
    diskTup->SetXid(xid);

    page->DelTuple(GetOffset());
    page->SetTd(tdId, xid, UndoRecPtr(undoRecPtr), INVALID_CID);
    page->SetTuplePrunable(true);
    page->AddPotentialDelItemSize(GetOffset());
}

void WalRecordHeapInplaceUpdate::SetData(uint16 numDiffPos, uint16 *diffPos, char *newTupleData)
{
    numDiff = numDiffPos;
    uint16 dataEndOffset = static_cast<uint16>((diskTupleSize - HEAP_DISK_TUP_HEADER_SIZE) - 1);
    char *newTupleDataEnd = newTupleData + dataEndOffset;

    uint16 fromPos = 0;
    for (uint16 i = 0; i < numDiff; i += NUM_DIFF_STEP) {
        fromPos = AppendData(fromPos, static_cast<char *>(static_cast<void *>(diffPos + i)),
                             static_cast<uint16>(sizeof(diffPos[0])));
        fromPos = AppendData(fromPos, static_cast<char *>(static_cast<void *>(diffPos + i + 1)),
                             static_cast<uint16>(sizeof(diffPos[0])));
        StorageReleasePanic((*(diffPos + i + 1)) <= (*(diffPos + i)), MODULE_HEAP, ErrMsg("SetData failed."));
        uint16 endOffset = static_cast<uint16>(diffPos[i + 1] - 1);
        fromPos = AppendData(fromPos, newTupleDataEnd - endOffset, static_cast<uint16>(diffPos[i + 1] - diffPos[i]));
    }
    pos = fromPos;
}

uint16 WalRecordHeapInplaceUpdate::AppendData(uint16 fromPos, char *data, uint16 size)
{
    char *buf = rawData + fromPos;
    CopyData(buf, static_cast<uint32>(m_size - fromPos), data, size);
    return fromPos + size;
}

void WalRecordHeapInplaceUpdate::Redo(HeapPage *page, Xid xid) const
{
    uint32 fromPos = pos;
    if (IsContainLoigcalInfo()) {
        StorageAssert(IsUpdateContainsReplicaKey());
        const HeapDiskTuple *tuple = static_cast<const HeapDiskTuple *>(static_cast<const void *>(rawData + pos));
        fromPos += tuple->GetTupleSize();
        fromPos = fromPos + sizeof(Oid) + sizeof(CommitSeqNo);
        if (unlikely(IsDecodeDictChange())) {
            fromPos += sizeof(CommandId);
        }
    }
    RedoAllocTdWal(page, PageType::HEAP_PAGE_TYPE, rawData + fromPos,
                   m_size - (sizeof(WalRecordHeapInplaceUpdate) + fromPos));

    HeapDiskTuple *diskTupOnPage = page->GetDiskTuple(GetOffset());
    uint16 pageTupSize = diskTupOnPage->GetTupleSize();
    uint16 redoTupDataSize = static_cast<uint16>(diskTupleSize - HEAP_DISK_TUP_HEADER_SIZE);
    uint16 pageTupDataSize = static_cast<uint16>(pageTupSize - HEAP_DISK_TUP_HEADER_SIZE);
    char *tupleData = diskTupOnPage->GetData();
    uint16 dataOffset;
    errno_t rc;
    if (diskTupleSize > pageTupSize) {
        dataOffset = static_cast<uint16>(diskTupleSize - pageTupSize);
        rc = memmove_s(tupleData + dataOffset, pageTupDataSize, tupleData, pageTupDataSize);
        storage_securec_check(rc, "\0", "\0");
    } else if (diskTupleSize < pageTupSize) {
        dataOffset = static_cast<uint16>(pageTupSize - diskTupleSize);
        rc = memmove_s(tupleData, pageTupDataSize, tupleData + dataOffset, redoTupDataSize);
        storage_securec_check(rc, "\0", "\0");
    }

    uint16 start, end;
    const char *ptr = rawData;
    uint16 dataEndOffset = static_cast<uint16>(redoTupDataSize - 1);
    char *dataEnd = tupleData + dataEndOffset;
    for (uint16 i = 0; i < numDiff; i += NUM_DIFF_STEP) {
        start = *static_cast<const uint16 *>(static_cast<const void *>(ptr));
        end = *static_cast<const uint16 *>(static_cast<const void *>(ptr + sizeof(start)));
        uint16 len = static_cast<uint16>(end - start);
        uint16 destOffset = static_cast<uint16>(end - 1);
        rc = memcpy_s(dataEnd - destOffset, len, ptr + sizeof(start) + sizeof(end), len);
        storage_securec_check(rc, "\0", "\0");
        ptr += sizeof(start) + sizeof(end) + end - start;
    }
    StorageAssert(tdId != INVALID_TD_SLOT);
    diskTupOnPage->SetTdId(tdId);
    diskTupOnPage->SetXid(xid);
    diskTupOnPage->SetTdStatus(ATTACH_TD_AS_NEW_OWNER);
    diskTupOnPage->SetLiveMode(HeapDiskTupLiveMode::NEW_TUPLE_BY_INPLACE_UPDATE);
    diskTupOnPage->SetTupleSize(diskTupleSize);
    diskTupOnPage->m_info.m_info = tupleHeadInfo;
    page->SetTd(tdId, xid, UndoRecPtr(undoRecPtr), INVALID_CID);
    page->SetTuplePrunable(true);
}

void WalRecordHeapSamePageAppendUpdate::SetData(char *data, uint32 size)
{
    CopyData(rawData, static_cast<uint32>(m_size - sizeof(WalRecordHeapSamePageAppendUpdate)), data, size);
}

void WalRecordHeapSamePageAppendUpdate::Redo(HeapPage *page, Xid xid) const
{
    HeapDiskTuple *oldDiskTup = page->GetDiskTuple(GetOffset());
    const HeapDiskTuple *newDiskTup = static_cast<const HeapDiskTuple *>(static_cast<const void *>(rawData));
    uint32 fromPos = newDiskTup->GetTupleSize();
    if (IsContainLoigcalInfo()) {
        StorageAssert(IsUpdateContainsReplicaKey());
        const HeapDiskTuple *oldTupleKey =
            static_cast<const HeapDiskTuple *>(static_cast<const void *>(rawData + fromPos));
        fromPos += oldTupleKey->GetTupleSize();
        fromPos = fromPos + sizeof(Oid) + sizeof(CommitSeqNo);
        if (unlikely(IsDecodeDictChange())) {
            fromPos += sizeof(CommandId);
        }
    }
    RedoAllocTdWal(page, PageType::HEAP_PAGE_TYPE, rawData + fromPos,
                   m_size - (sizeof(WalRecordHeapSamePageAppendUpdate) + fromPos));

    page->AddPotentialDelItemSize(GetOffset());
    page->SetTuplePrunable(true);

    OffsetNumber offsetNum = page->AddTuple(newDiskTup, static_cast<uint16>(newDiskTup->GetTupleSize()), GetOffset());
    StorageReleasePanic(offsetNum == INVALID_ITEM_OFFSET_NUMBER, MODULE_HEAP, ErrMsg("Add tuple failed, page(%hu, %u), "
        "offset(%hu), size(%hu).", page->GetFileId(), page->GetBlockNum(), GetOffset(), newDiskTup->GetTupleSize()));
    page->SetTd(newDiskTup->GetTdId(), xid, UndoRecPtr(undoRecPtr), INVALID_CID);

    oldDiskTup->SetTdId(newDiskTup->GetTdId());
    StorageAssert(xid == newDiskTup->GetXid());
    oldDiskTup->SetXid(xid);
    oldDiskTup->SetTdStatus(ATTACH_TD_AS_NEW_OWNER);
    oldDiskTup->SetLiveMode(HeapDiskTupLiveMode::OLD_TUPLE_BY_SAME_PAGE_UPDATE);
}

void WalRecordHeapAnotherPageAppendUpdateNewPage::SetData(char *data, uint32 size)
{
    CopyData(rawData, static_cast<uint32>(m_size - sizeof(WalRecordHeapAnotherPageAppendUpdateNewPage)), data, size);
}

void WalRecordHeapAnotherPageAppendUpdateNewPage::Redo(HeapPage *page, Xid xid) const
{
    const HeapDiskTuple *diskTup = static_cast<const HeapDiskTuple *>(static_cast<const void *>(rawData));
    uint32 fromPos = diskTup->GetTupleSize();
    if (IsContainLoigcalInfo()) {
        StorageAssert(IsUpdateContainsReplicaKey());
        const HeapDiskTuple *oldTupleKey =
            static_cast<const HeapDiskTuple *>(static_cast<const void *>(rawData + fromPos));
        fromPos += oldTupleKey->GetTupleSize();
        fromPos = fromPos + sizeof(Oid) + sizeof(CommitSeqNo);
        if (unlikely(IsDecodeDictChange())) {
            fromPos += sizeof(CommandId);
        }
    }
    RedoAllocTdWal(page, PageType::HEAP_PAGE_TYPE, rawData + fromPos,
                   m_size - (sizeof(WalRecordHeapAnotherPageAppendUpdateNewPage) + fromPos));

    OffsetNumber offsetNum = page->AddTuple(diskTup, static_cast<uint16>(diskTup->GetTupleSize()), GetOffset());
    StorageReleasePanic(offsetNum == INVALID_ITEM_OFFSET_NUMBER, MODULE_HEAP, ErrMsg("Add tuple failed, page(%hu, %u), "
        "offset(%hu), size(%hu).", page->GetFileId(), page->GetBlockNum(), GetOffset(), diskTup->GetTupleSize()));
    page->SetTd(diskTup->GetTdId(), xid, UndoRecPtr(undoRecPtr), INVALID_CID);

    page->SetIsNewPage(false);
}

void WalRecordHeapForceUpdateTupleDataNoTrx::SetData(char *data, uint32 size)
{
    CopyData(rawData, static_cast<uint32>(m_size - sizeof(WalRecordHeapForceUpdateTupleDataNoTrx)), data, size);
}

void WalRecordHeapForceUpdateTupleDataNoTrx::Redo(HeapPage *page, Xid xid) const
{
    HeapTuple tuple;
    StorageAssert(xid == INVALID_XID);
    (void)xid;
    page->GetTuple(&tuple, GetOffset());
    char *tuplePtr = tuple.GetDiskTuple()->GetData();
    errno_t rc = memcpy_s(tuplePtr, GetDataLen(), rawData, GetDataLen());
    storage_securec_check(rc, "\0", "\0");
}

void WalRecordHeapAnotherPageAppendUpdateOldPage::Redo(HeapPage *page, Xid xid) const
{
    RedoAllocTdWal(page, PageType::HEAP_PAGE_TYPE, rawData,
                   m_size - sizeof(WalRecordHeapAnotherPageAppendUpdateOldPage));
    HeapDiskTuple *diskTup = page->GetDiskTuple(GetOffset());

    page->DelTuple(GetOffset());
    page->SetTd(tdId, xid, UndoRecPtr(undoRecPtr), INVALID_CID);
    page->SetTuplePrunable(true);
    page->AddPotentialDelItemSize(GetOffset());

    diskTup->SetLiveMode(HeapDiskTupLiveMode::OLD_TUPLE_BY_ANOTHER_PAGE_UPDATE);
    diskTup->SetTdId(tdId);
    diskTup->SetXid(xid);
    diskTup->SetTdStatus(ATTACH_TD_AS_NEW_OWNER);
}

void WalRecordHeapPrune::SetData(char *data, uint32 size)
{
    if (size == 0) {
        return;
    }
    CopyData(rawData, m_size - sizeof(WalRecordHeapPrune), data, size);
    StorageAssert(size + sizeof(WalRecordHeapPrune) == m_size);
}

void WalRecordHeapPrune::Redo(HeapPage *page, UNUSE_PARAM Xid xid) const
{
    if (diffNum != 0) {
        const ItemIdDiff *itemIdDiff = static_cast<const ItemIdDiff *>(static_cast<const void *>(rawData));
        page->PruneItems(itemIdDiff, diffNum);
        page->SetRecentDeadTupleMinCsn(recentDeadMinCsn);
        page->SetItemPrunable(recentDeadMinCsn != INVALID_CSN);
    }

    page->TryCompactTuples();
}

void WalRecordHeapAllocTd::Redo(HeapPage *page, UNUSE_PARAM Xid xid) const
{
    RedoAllocTdWal(page, PageType::HEAP_PAGE_TYPE, rawData, m_size - sizeof(WalRecordHeapAllocTd));
}

void WalRecordHeapUpdateNextCtid::Redo(HeapPage *page, UNUSE_PARAM Xid xid) const
{
    HeapDiskTuple *diskTupChunk = page->GetDiskTuple(offset);
    StorageReleasePanic(diskTupChunk == nullptr, MODULE_HEAP, ErrMsg("Get disktuple fail, offset(%hu).", offset));
    diskTupChunk->SetNextChunkCtid(ItemPointerData(nextCtid));
}

} /* The end of DSTORE */