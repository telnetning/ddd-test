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
 * dstore_undo_zone.cpp
 *
 * IDENTIFICATION
 *        src/undo/dstore_undo_zone.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include <securec.h>
#include "common/instrument/trace/dstore_undo_trace.h"
#include "common/log/dstore_log.h"
#include "framework/dstore_instance.h"
#include "errorcode/dstore_undo_error_code.h"
#include "errorcode/dstore_framework_error_code.h"
#include "index/dstore_btree_undo_data_struct.h"
#include "wal/dstore_wal_write_context.h"
#include "transaction/dstore_transaction_mgr.h"
#include "page/dstore_index_page.h"
#include "page/dstore_undo_page.h"
#include "page/dstore_undo_segment_meta_page.h"
#include "undo/dstore_undo_mgr.h"
#include "undo/dstore_undo_zone_txn_mgr.h"
#include "undo/dstore_undo_zone.h"
#include "common/fault_injection/dstore_undo_fault_injection.h"
#include "fault_injection/fault_injection.h"

namespace DSTORE {

UndoZone::UndoZone(Segment *segment, BufMgrInterface *bufMgr, ZoneId zoneId, PdbId pdbId)
    : m_pdbId(pdbId), m_zoneId(zoneId), m_txnSlotManager(nullptr), m_segment(segment), m_bufferMgr(bufMgr)
{
    m_nextAppendUndoPtr = {INVALID_PAGE_ID, 0};
    m_needCheckPageId = INVALID_PAGE_ID;
    m_undoRecyclePageId = INVALID_PAGE_ID;
    m_isAsyncRollbacking = false;
    m_currentInsertUndoPageBuf = nullptr;
}

UndoZone::~UndoZone()
{
    delete m_txnSlotManager;
    m_txnSlotManager = nullptr;
    m_segment = nullptr;
    m_bufferMgr = nullptr;
}

RetStatus UndoZone::Init(DstoreMemoryContext context)
{
    RetStatus status = DSTORE_SUCC;
    StorageAssert(g_storageInstance->GetPdb(m_pdbId) != nullptr);
    UndoMgr *undoMgr = g_storageInstance->GetPdb(m_pdbId)->GetUndoMgr();
    BufferDesc *metaBufDesc = ReadSegMetaPageBuf(LW_SHARED);
    if (unlikely(metaBufDesc == INVALID_BUFFER_DESC)) {
        ErrLog(DSTORE_ERROR, MODULE_UNDO, ErrMsg("Read segment meta page from buffer failed."));
        return DSTORE_FAIL;
    }
    UndoSegmentMetaPage *metaPage = GetUndoSegmentMetaPage(metaBufDesc);
    if (unlikely(metaPage == nullptr)) {
        UnlockAndReleaseSegMetaPageBuf(metaBufDesc);
        PageId segMetaPageId = m_segment->GetSegmentMetaPageId();
        ErrLog(DSTORE_ERROR, MODULE_UNDO,
               ErrMsg("Meta page is nullptr when init undo zone, zoneId(%d), pageId(%hu, %u), pdbId[%u].", m_zoneId,
                      segMetaPageId.m_fileId, segMetaPageId.m_blockId, m_pdbId));
        return DSTORE_FAIL;
    }
    bool alreadyInitTxnSlotPages = metaPage->alreadyInitTxnSlotPages;
    PageId firstUndoPageId = metaPage->firstUndoPageId;

    /* Get the first block of this undo zone's segment */
    PageId segMetaPageId = m_segment->GetSegmentMetaPageId();
    UnlockAndReleaseSegMetaPageBuf(metaBufDesc);

    PageId startPageId = {segMetaPageId.m_fileId, segMetaPageId.m_blockId + 1};
    m_txnSlotManager = DstoreNew(context) UndoZoneTrxManager(m_zoneId, m_segment, m_bufferMgr, m_pdbId);
    if (unlikely(m_txnSlotManager == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_UNDO, ErrMsg("txnSlotManager alloc failed."));
        return DSTORE_FAIL;
    }

    /* we must have done init transaction slot page and undo record page both, we can go else branch. */
    if (!alreadyInitTxnSlotPages || firstUndoPageId == INVALID_PAGE_ID) {
        FAULT_INJECTION_RETURN(DstoreUndoFI::PERSIST_INIT_TRANSACTION_SLOT_FAIL, DSTORE_SUCC);
        if (!alreadyInitTxnSlotPages) {
            status = InitTransactionSlotSpace(startPageId);
            StorageReleasePanic(STORAGE_FUNC_FAIL(status), MODULE_UNDO, ErrMsg("init transaction slot space failed!"
                " zid = %d", m_zoneId));
        }

        FAULT_INJECTION_RETURN(DstoreUndoFI::PERSIST_INIT_UNDO_RECORD_FAIL, DSTORE_SUCC);
        StorageReleasePanic(firstUndoPageId != INVALID_PAGE_ID, MODULE_UNDO, ErrMsg("has already done init undo "
            "record space! zid = %d", m_zoneId));
        status = InitUndoRecordSpace();
        if (STORAGE_FUNC_FAIL(status)) {
            ErrLog(DSTORE_ERROR, MODULE_UNDO, ErrMsg("Init undo record space failed! zid = %d", m_zoneId));
            return DSTORE_FAIL;
        }

        /* after init unpersistent content of undozone, now you can add owned, means can start service and recycle */
        undoMgr->AddZoneOwned(m_zoneId);
        /* we must set start page id here when panic occurs between initializing slot and initializing undo page. */
        m_txnSlotManager->SetStartPageId(startPageId);
        ErrLog(
            DSTORE_LOG, MODULE_UNDO,
            ErrMsg("New undo zone(zid=%d) has been inited with "
                   "m_txnSlotStartPageId(%hu, %u) and m_nextAppendUndoPtr(%hu, %u, %hu) and m_needCheckPageId(%hu, "
                   "%u), pdbId(%u).",
                   m_zoneId, m_txnSlotManager->GetStartPageId().m_fileId, m_txnSlotManager->GetStartPageId().m_blockId,
                   m_nextAppendUndoPtr.GetPageId().m_fileId, m_nextAppendUndoPtr.GetPageId().m_blockId,
                   m_nextAppendUndoPtr.GetOffset(), m_needCheckPageId.m_fileId, m_needCheckPageId.m_blockId, m_pdbId));
    } else {
        m_txnSlotManager->SetStartPageId(startPageId);
        if (!undoMgr->IsZoneOccupied(m_zoneId)) {
            return status;
        }
        if (STORAGE_FUNC_FAIL(RestoreUndoZoneFromTxnSlots())) {
            status = DSTORE_FAIL;
        }
        ErrLog(
            DSTORE_DEBUG1, MODULE_UNDO,
            ErrMsg(
                "Old undo zone(zid=%d) has been inited with "
                "m_txnSlotStartPageId(%hu, %u) and m_nextAppendUndoPtr(%hu, %u, %hu) and m_needCheckPageId(%hu, %u).",
                m_zoneId, m_txnSlotManager->GetStartPageId().m_fileId, m_txnSlotManager->GetStartPageId().m_blockId,
                m_nextAppendUndoPtr.GetPageId().m_fileId, m_nextAppendUndoPtr.GetPageId().m_blockId,
                m_nextAppendUndoPtr.GetOffset(), m_needCheckPageId.m_fileId, m_needCheckPageId.m_blockId));
    }
    return status;
}

BufferDesc *UndoZone::ReadSegMetaPageBuf(LWLockMode mode)
{
    PageId segMetaPageId = m_segment->GetSegmentMetaPageId();
    BufferDesc *segMetaPageBufDesc = m_bufferMgr->Read(m_pdbId, segMetaPageId, mode);
    StorageAssert(segMetaPageBufDesc != INVALID_BUFFER_DESC);
    return segMetaPageBufDesc;
}

UndoSegmentMetaPage* UndoZone::GetUndoSegmentMetaPage(BufferDesc *segMetaPageBuf)
{
    return static_cast<UndoSegmentMetaPage*>(segMetaPageBuf->GetPage());
}

void UndoZone::UnlockAndReleaseSegMetaPageBuf(BufferDesc *&segMetaPageBuf)
{
    m_bufferMgr->UnlockAndRelease(segMetaPageBuf);
    segMetaPageBuf = nullptr;
}

UndoRecPtr UndoZone::GetNextUndoRecPtr(UndoRecPtr curPtr, uint32 recordSize)
{
    UndoRecPtr nextPtr = curPtr;
    OffsetNumber curOffset = curPtr.GetOffset();
    if (curOffset + recordSize < BLCKSZ) {
        nextPtr.SetOffset(curOffset + static_cast<uint16>(recordSize));
        return nextPtr;
    }

    /* next undo record must be on another page */
    uint32 dataOffset = static_cast<uint32>(curPtr.GetOffset() - UNDO_RECORD_PAGE_HEADER_SIZE);
    uint32 pageCnt = (dataOffset + recordSize) / UNDO_RECORD_PAGE_MAX_FREE_SPACE;
    OffsetNumber nextOffset =
        (dataOffset + recordSize) % UNDO_RECORD_PAGE_MAX_FREE_SPACE + UNDO_RECORD_PAGE_HEADER_SIZE;

    PageId pageId = curPtr.GetPageId();
    UndoRecordPage *page;
    BufferDesc *bufferDesc;
    for (uint32 i = 0; i < pageCnt; ++i) {
        bufferDesc = m_bufferMgr->Read(m_pdbId, pageId, LW_SHARED);
        STORAGE_CHECK_BUFFER_PANIC(bufferDesc, MODULE_UNDO, pageId);
        page = static_cast<UndoRecordPage*>(bufferDesc->GetPage());
        pageId = page->GetNextPageId();
        m_bufferMgr->UnlockAndRelease(bufferDesc);
    }

    return {pageId, nextOffset};
}

void UndoZone::RecycleUndoPage(UndoRecPtr end, CommitSeqNo recycleMinCsn)
{
    PageId startPageId = m_undoRecyclePageId;
    PageId endPageId = end.GetPageId();
    /* Step 1. No pages can be recycled, we only recycle the whole page. */
    if (startPageId == endPageId) {
        return;
    }

    /* Step 2. Count recycled undo page num(end - start) */
    uint32 recyclePageNum = 0;
    PageId curPageId = startPageId;
    while (curPageId != endPageId) {
        BufferDesc *curBuf = m_bufferMgr->Read(m_pdbId, curPageId, LW_SHARED);
        StorageReleasePanic(curBuf == nullptr, MODULE_UNDO,
                            ErrMsg("Read page (%hu, %u) failed when recycle, zoneId(%d), pdbId[%u]", curPageId.m_fileId,
                                   curPageId.m_blockId, m_zoneId, m_pdbId));
        UndoRecordPage *curPage = static_cast<UndoRecordPage *>(curBuf->GetPage());
        StorageReleasePanic(curPage == nullptr, MODULE_UNDO,
                            ErrMsg("Page (%hu, %u) is nullptr when recycle, zoneId(%d), pdbId[%u]", curPageId.m_fileId,
                                   curPageId.m_blockId, m_zoneId, m_pdbId));
        curPageId = curPage->m_undoRecPageHeader.next;
        m_bufferMgr->UnlockAndRelease(curBuf);
        recyclePageNum++;
    }
    m_undoRecyclePageId = endPageId;
    uint64 recycleLogicSlotId = m_txnSlotManager->GetRecycleLogicSlotId();
    ErrLog(DSTORE_DEBUG1, MODULE_UNDO,
           ErrMsg("Recycle undo page from (%hu, %u) to (%hu, %u) in zid(%d), recycle page num is %u,"
                  "recycle csn min = %lu, recycleLogicSlotId(%lu), pdbId(%u).",
                  startPageId.m_fileId, startPageId.m_blockId, endPageId.m_fileId, endPageId.m_blockId, m_zoneId,
                  recyclePageNum, recycleMinCsn, recycleLogicSlotId, m_pdbId));
}

bool UndoZone::InsertUndoBytes(const StrParam &src, char **writePtr, const char *endPtr, int &myBytesWritten,
                               int &alreadyWritten)
{
    char *srcPtr = src.ptr;
    int srcLen = static_cast<int>(src.size);

    if (myBytesWritten >= srcLen) {
        myBytesWritten -= srcLen;
        return true;
    }

    int remaining = srcLen - myBytesWritten;
    int maxWriteOnCurPage = static_cast<int>(endPtr - *writePtr);
    int canWriteBytes = DstoreMin(remaining, maxWriteOnCurPage);
    if (canWriteBytes == 0) {
        return false;
    }

    errno_t rc = memcpy_s(*writePtr, static_cast<uint32>(maxWriteOnCurPage), srcPtr + myBytesWritten,
                          static_cast<uint32>(canWriteBytes));
    storage_securec_check(rc, "\0", "\0");

    *writePtr += canWriteBytes;
    alreadyWritten += canWriteBytes;
    myBytesWritten = 0;

    return (canWriteBytes == remaining);
}

void UndoZone::ReadUndoBytesStream(ReadUndoContext &context)
{
    BufferDesc* bufferDesc = context.bufferDesc;
    char *page = static_cast<char *>(static_cast<void *>(bufferDesc->GetPage()));
    StorageAssert(page);
    char *endPtr = page + BLCKSZ;
    int alreadyRead = 0;
    PageId curId = INVALID_PAGE_ID;
    char *readPtr = page + *(context.startingByte);
    for (;;) {
        if (ReadUndoBytes(context.dest, &readPtr, endPtr, alreadyRead)) {
            *(context.startingByte) = static_cast<int>(readPtr - page);
            break;
        }

        /* switch to next page */
        curId = static_cast<UndoRecordPage *>(static_cast<void *>(page))->m_undoRecPageHeader.next;
        context.bufferMgr->UnlockAndRelease(bufferDesc);
        StorageAssert(curId != INVALID_PAGE_ID);
        bufferDesc = (context.bufferMgr)->Read(context.pdbId, curId, LW_SHARED);
        STORAGE_CHECK_BUFFER_PANIC(bufferDesc, MODULE_UNDO, curId);
        page = static_cast<char *>(static_cast<void *>(bufferDesc->GetPage()));
        StorageAssert(page);
        context.bufferDesc = bufferDesc;
        readPtr = static_cast<char *>(page) + UNDO_RECORD_PAGE_HEADER_SIZE;
        endPtr = page + BLCKSZ;
    }
}

bool UndoZone::ReadUndoBytes(const StrParam &dest, char **readPtr, const char *endPtr, int &alreadyRead)
{
    char *destPtr = dest.ptr;
    int destLen = static_cast<int>(dest.size);

    if (alreadyRead >= destLen) {
        alreadyRead -= destLen;
        return true;
    }

    int remaining = destLen - alreadyRead;
    int maxReadOnCurPage = static_cast<int>(endPtr - *readPtr);
    int canReadBytes = DstoreMin(remaining, maxReadOnCurPage);
    if (canReadBytes == 0) {
        return false;
    }

    StorageAssert(remaining > 0);
    StorageAssert(canReadBytes > 0);
    errno_t rc =
        memcpy_s(destPtr + alreadyRead, static_cast<uint32>(remaining), *readPtr, static_cast<uint32>(canReadBytes));
    storage_securec_check(rc, "\0", "\0");

    *readPtr += canReadBytes;
    alreadyRead += canReadBytes;

    return (canReadBytes == remaining);
}

/*
 * If the remaining size of the current page is less than undo record size, the remaining bytes of undo record will be
 * written on next page, also undo records must be stored continuously.
 */
bool UndoZone::WriteUndoRecord(UndoRecord &record, char* page, int startingByte, int &alreadyWritten) const
{
    StorageAssert(page);

    char *writePtr = page + startingByte;
    char *endPtr = page + BLCKSZ;
    int myBytesWritten = alreadyWritten;

    if (!InsertUndoBytes({record.GetSerializeData(), record.GetSerializeSize()},
        &writePtr, endPtr, myBytesWritten, alreadyWritten)) {
        return false;
    }

    if (!InsertUndoBytes({static_cast<char *>(static_cast<void *>(&record.m_dataInfo.len)), sizeof(int)},
        &writePtr, endPtr, myBytesWritten, alreadyWritten)) {
        return false;
    }

    if (!InsertUndoBytes({static_cast<char *>(static_cast<void *>(record.m_dataInfo.data)), record.GetUndoDataSize()},
        &writePtr, endPtr, myBytesWritten, alreadyWritten)) {
        return false;
    }

    return true;
}

/**
 * If the remaining size of the current page is less than undo record size, the remaining bytes of undo record will be
 * read on next page.
 */
void UndoZone::ReadUndoRecord(
    PdbId pdbId, UndoRecord &record, BufferDesc *&bufferDesc, int startingByte, BufMgrInterface *bufferMgr)
{
    AutoMemCxtSwitch autoMemCxtSwitch(thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_QUERY));

    ReadUndoContext context;
    context.pdbId = pdbId;
    context.bufferMgr = bufferMgr;
    context.bufferDesc = bufferDesc;
    context.startingByte = &startingByte;

    uint8 serializeSize = 0;
    StrParam dest = {static_cast<char *>(static_cast<void *>(&serializeSize)), sizeof(record.GetSerializeSize())};
    context.dest = dest;

    ReadUndoBytesStream(context);
    record.SetSerializeSize(serializeSize);
    record.PrepareDiskData();
    dest = {static_cast<char *>(static_cast<void *>((record.GetSerializeData() + sizeof(record.GetSerializeSize())))),
            record.GetSerializeSize() - sizeof(record.GetSerializeSize())};
    context.dest = dest;
    ReadUndoBytesStream(context);
    record.Deserialize();
    record.DestroyDiskData();

    dest = {static_cast<char *>(static_cast<void *>(&record.m_dataInfo.len)), sizeof(int)};
    context.dest = dest;
    ReadUndoBytesStream(context);
    StorageAssert(record.GetUndoType() < UNDO_UNKNOWN);
    uint16 undoDataSize = record.GetUndoDataSize();
    if (undoDataSize > 0) {
        StorageAssert(thrd->GetUndoContext() != nullptr);
        record.m_dataInfo.data = thrd->GetUndoContext();
        record.m_pallocDataSize = undoDataSize;
        dest = {record.m_dataInfo.data, undoDataSize};
        context.dest = dest;
        ReadUndoBytesStream(context);
    }
    bufferDesc = context.bufferDesc;
}

void UndoZone::Recycle(CommitSeqNo recycleMinCsn)
{
    storage_trace_entry(TRACE_ID_UndoZone__Recycle);
    StorageAssert(recycleMinCsn != INVALID_CSN);
    StorageAssert(m_undoRecyclePageId.IsValid());

    /* recycle per slot */
    UndoRecPtr restoreUndoPtr = INVALID_UNDO_RECORD_PTR;
    UndoRecPtr recycleUndoPtr = INVALID_UNDO_RECORD_PTR;
    TailUndoPtrStatus tailPtrStatus = UNKNOWN_STATUS;

    m_txnSlotManager->RecycleTxnSlots(recycleMinCsn, restoreUndoPtr, recycleUndoPtr, false, tailPtrStatus);
    /* If recycleUndoPtr invalid, if could be no one slot was recycled, or just recycle one null transaction slot
     * has not be inserted undo record, so we no need to recycle undo page.
     */
    if (recycleUndoPtr == INVALID_UNDO_RECORD_PTR) {
        return;
    }

    /* fetch recycleUndoPtr undo record end position. */
    UndoRecord undoRec;
    FetchUndoRecordInternal(m_pdbId, &undoRec, recycleUndoPtr, m_bufferMgr);
    UndoRecPtr endPtr = GetNextUndoRecPtr(recycleUndoPtr, undoRec.GetRecordSize());

    /*
     * Don't need to add lock here, because it is only be modified in undo recycle thread.
     * recycle per page
     */
    RecycleUndoPage(endPtr, recycleMinCsn);
}

RetStatus UndoZone::AllocNewUndoPages(PageId &firstFreePageId, PageId &lastFreePageId)
{
    PageId firstPageId;
    RetStatus status = m_segment->Extend(UNDO_ZONE_EXTENT_SIZE, &firstPageId);
    if (STORAGE_FUNC_FAIL(status)) {
        ErrLog(DSTORE_ERROR, MODULE_UNDO, ErrMsg("Extend undo segment(%hu, %u) failed.",
            m_segment->GetSegmentMetaPageId().m_fileId, m_segment->GetSegmentMetaPageId().m_blockId));
        return DSTORE_FAIL;
    }

    ErrLog(DSTORE_DEBUG1, MODULE_UNDO,ErrMsg("Extend undo segment in zid(%d) and "
        "first page id of new expend is (%hu, %u)", m_zoneId, firstPageId.m_fileId, firstPageId.m_blockId));

    /* Note: The first page of new extend is unusable, the all remaining pages should be free pages. */
    firstFreePageId = {firstPageId.m_fileId, firstPageId.m_blockId + 1};
    lastFreePageId = {firstPageId.m_fileId, firstPageId.m_blockId + (static_cast<uint32>(UNDO_ZONE_EXTENT_SIZE) - 1)};

    return DSTORE_SUCC;
}

RetStatus UndoZone::PutNewPagesIntoRing(const PageId &prev, const PageId &firstFreePageId, const PageId &next)
{
    StorageAssert(prev != INVALID_PAGE_ID);
    StorageAssert(firstFreePageId != INVALID_PAGE_ID);
    StorageAssert(next != INVALID_PAGE_ID);

    PageId prevPageId = prev;
    /* Note: The first page of new extend is unusable */
    uint16 newPageCnts = static_cast<uint16>(UNDO_ZONE_EXTENT_SIZE) - 1;
    for (uint16 i = 0; i < newPageCnts; ++i) {
        PageId curPageId = {firstFreePageId.m_fileId, firstFreePageId.m_blockId + i};
        BufferDesc *bufferDesc =
            m_bufferMgr->Read(m_pdbId, curPageId, LW_EXCLUSIVE);
        if (unlikely((bufferDesc) == INVALID_BUFFER_DESC)) {
            ErrLog(DSTORE_ERROR, MODULE_UNDO,
                ErrMsg("Buffer(%hu, %u) invalid when PutNewPagesIntoRing.",
                    curPageId.m_fileId, curPageId.m_blockId));
            return DSTORE_FAIL;
        }
        UndoRecordPage *curPage = static_cast<UndoRecordPage *>(bufferDesc->GetPage());
        StorageAssert(curPage);
        StorageAssert(curPageId.m_blockId <= DSTORE_MAX_BLOCK_NUMBER);
        curPage->m_undoRecPageHeader = {0, curPageId, prevPageId, {curPageId.m_fileId, curPageId.m_blockId + 1}};
        if (i == newPageCnts - 1) {
            curPage->m_undoRecPageHeader.next = next;
        }
        curPage->InitUndoRecPage(curPageId);
        prevPageId = curPageId;
        (void)m_bufferMgr->MarkDirty(bufferDesc);
        GenerateWalForUndoRingNewPage(bufferDesc, curPage->m_undoRecPageHeader);
        m_bufferMgr->UnlockAndRelease(bufferDesc);
    }

    return DSTORE_SUCC;
}

RetStatus UndoZone::ExtendUndoPageRing(const PageId &firstFreePageId, const PageId &lastFreePageId)
{
    /* Step 1: update previous page */
    PageId prevPageId = m_nextAppendUndoPtr.GetPageId();
    BufferDesc *bufferDesc = m_bufferMgr->
        Read(m_pdbId, m_nextAppendUndoPtr.GetPageId(), LW_EXCLUSIVE);
    if (unlikely((bufferDesc) == INVALID_BUFFER_DESC)) {
        ErrLog(DSTORE_ERROR, MODULE_UNDO,
            ErrMsg("Buffer(%hu, %u) invalid when ExtendUndoPageRing update previous page.",
                m_nextAppendUndoPtr.GetPageId().m_fileId, m_nextAppendUndoPtr.GetPageId().m_blockId));
        return DSTORE_FAIL;
    }
    UndoRecordPage *prevPage = static_cast<UndoRecordPage *>(bufferDesc->GetPage());
    StorageAssert(prevPage);
    PageId nextPageId = prevPage->m_undoRecPageHeader.next;
    prevPage->m_undoRecPageHeader.next = firstFreePageId;
    (void)m_bufferMgr->MarkDirty(bufferDesc);
    GenerateWalForUndoRingOldPage<WAL_UNDO_EXTEND_PAGE_RING_PREV_PAGE>(bufferDesc, firstFreePageId);
    m_bufferMgr->UnlockAndRelease(bufferDesc);

    /* Step 2: update next page */
    bufferDesc = m_bufferMgr->Read(m_pdbId, nextPageId, LW_EXCLUSIVE);
    if (unlikely((bufferDesc) == INVALID_BUFFER_DESC)) {
        ErrLog(DSTORE_ERROR, MODULE_UNDO,
            ErrMsg("Buffer(%hu, %u) invalid when ExtendUndoPageRing update next page.",
                nextPageId.m_fileId, nextPageId.m_blockId));
        return DSTORE_FAIL;
    }
    UndoRecordPage *nextPage = static_cast<UndoRecordPage *>(bufferDesc->GetPage());
    StorageAssert(nextPage);
    nextPage->m_undoRecPageHeader.prev = lastFreePageId;
    (void)m_bufferMgr->MarkDirty(bufferDesc);
    GenerateWalForUndoRingOldPage<WAL_UNDO_EXTEND_PAGE_RING_NEXT_PAGE>(bufferDesc, lastFreePageId);
    m_bufferMgr->UnlockAndRelease(bufferDesc);

    /* Step 3: insert new pages */
    return PutNewPagesIntoRing(prevPageId, firstFreePageId, nextPageId);
}

RetStatus UndoZone::InitUndoRecordSpace()
{
    PageId firstFreePageId = INVALID_PAGE_ID;
    PageId lastFreePageId = INVALID_PAGE_ID;
    if (STORAGE_FUNC_FAIL(AllocNewUndoPages(firstFreePageId, lastFreePageId))) {
        return DSTORE_FAIL;
    }

    AtomicWalWriterContext *walContext = thrd->m_walWriterContext;
    walContext->BeginAtomicWal(INVALID_XID);

    if (STORAGE_FUNC_FAIL(PutNewPagesIntoRing(lastFreePageId, firstFreePageId, firstFreePageId))) {
        ErrLog(DSTORE_ERROR, MODULE_UNDO,
               ErrMsg("Put new pages into undo ring failed, zid(%d), pdbId(%u).", m_zoneId, m_pdbId));
        (void)walContext->EndAtomicWal();
        return DSTORE_FAIL;
    }

    m_nextAppendUndoPtr = {firstFreePageId, UNDO_RECORD_PAGE_HEADER_SIZE};
    m_needCheckPageId = {firstFreePageId.m_fileId, firstFreePageId.m_blockId + 1};
    /* The first page of new extend is unusable, the second page will be used by m_nextAppendUndoPtr. */
    m_undoRecyclePageId = firstFreePageId;
    BufferDesc *segMetaPageBuf = ReadSegMetaPageBuf(LW_EXCLUSIVE);
    if (unlikely(segMetaPageBuf == INVALID_BUFFER_DESC)) {
        ErrLog(DSTORE_ERROR, MODULE_UNDO,
               ErrMsg("Read segment meta page from buffer failed, zid(%d), pdbId(%u).", m_zoneId, m_pdbId));
        (void)walContext->EndAtomicWal();
        return DSTORE_FAIL;
    }
    UndoSegmentMetaPage *metaPage = GetUndoSegmentMetaPage(segMetaPageBuf);
    StorageAssert(metaPage);
    metaPage->firstUndoPageId = firstFreePageId;
    (void)m_bufferMgr->MarkDirty(segMetaPageBuf);
    GenerateWalForInitUndoRecSpace(firstFreePageId, segMetaPageBuf);
    UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);

    (void)walContext->EndAtomicWal();
    ErrLog(DSTORE_LOG, MODULE_UNDO,
           ErrMsg("Init space zid(%d), first free pageId(%hu, %u), last free pageId(%hu, %u), m_pdbId(%u).", m_zoneId,
                  firstFreePageId.m_fileId, firstFreePageId.m_blockId, lastFreePageId.m_fileId,
                  lastFreePageId.m_blockId, m_pdbId));
    return DSTORE_SUCC;
}

RetStatus UndoZone::InitTransactionSlotSpace(PageId startPageId)
{
    AtomicWalWriterContext *walContext = thrd->m_walWriterContext;
    walContext->BeginAtomicWal(INVALID_XID);

    if (STORAGE_FUNC_FAIL(m_txnSlotManager->Init(startPageId, true))) {
        (void)walContext->EndAtomicWal();
        return DSTORE_FAIL;
    }
    BufferDesc *segMetaPageBuf = ReadSegMetaPageBuf(LW_EXCLUSIVE);
    if (unlikely(segMetaPageBuf == INVALID_BUFFER_DESC)) {
        ErrLog(DSTORE_ERROR, MODULE_UNDO, ErrMsg("Read segment meta page from buffer failed."));
        (void)walContext->EndAtomicWal();
        return DSTORE_FAIL;
    }
    UndoSegmentMetaPage *metaPage = GetUndoSegmentMetaPage(segMetaPageBuf);
    StorageAssert(metaPage);
    metaPage->alreadyInitTxnSlotPages = true;
    (void)m_bufferMgr->MarkDirty(segMetaPageBuf);

    walContext->RememberPageNeedWal(segMetaPageBuf);
    uint32 size = static_cast<uint32>(sizeof(WalRecordUndoTxnSlotPageInited));
    WalRecordUndoTxnSlotPageInited redoData;
    bool glsnChangedFlag = (metaPage->GetWalId() != walContext->GetWalId());
    uint64 fileVersion = segMetaPageBuf->GetFileVersion();
    redoData.SetHeader({WAL_UNDO_SET_TXN_PAGE_INITED, size, segMetaPageBuf->GetPageId(), metaPage->GetWalId(),
        metaPage->GetPlsn(), metaPage->GetGlsn(), glsnChangedFlag, fileVersion});
    walContext->PutNewWalRecord(&redoData);

    UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);
    (void)walContext->EndAtomicWal();

    return DSTORE_SUCC;
}

/*
 * We use m_appendUndoPtr and m_undoRecyclePageId to estimate whether remaining space is enough.
 * In most cases, undo space will expend when m_appendUndoPtr next pageid is same with m_undoRecyclePageId.
 * We expect that need size is less that one page when undo record insert.
 */
RetStatus UndoZone::ExtendSpaceIfNeeded(uint32 needSize)
{
    FAULT_INJECTION_CALL_REPLACE(DstoreUndoFI::SET_FREE_PAGE_NUM_ZERO, &m_needCheckPageId, m_undoRecyclePageId);
    FAULT_INJECTION_CALL_REPLACE_END;
    StorageReleasePanic(needSize > UNDO_RECORD_PAGE_MAX_FREE_SPACE, MODULE_UNDO,
                        ErrMsg("Unexpected undo record size(%u).", needSize));
    StorageAssert(g_storageInstance->GetPdb(m_pdbId)->GetUndoMgr()->IsZoneOwned(m_zoneId));
    if (likely(m_needCheckPageId != m_undoRecyclePageId)) {
        return DSTORE_SUCC;
    }

    PageId firstFreePageId, lastFreePageId;
    AtomicWalWriterContext *walContext = thrd->m_walWriterContext;
    StorageAssert(!(walContext->HasAlreadyBegin()));
    if (STORAGE_FUNC_FAIL(AllocNewUndoPages(firstFreePageId, lastFreePageId))) {
        ErrLog(DSTORE_ERROR, MODULE_UNDO, ErrMsg("AllocNewUndoPages failed."));
        return DSTORE_FAIL;
    }

    walContext->BeginAtomicWal(thrd->GetActiveTransaction()->GetCurrentXid());
    if (STORAGE_FUNC_FAIL(ExtendUndoPageRing(firstFreePageId, lastFreePageId))) {
        walContext->ResetForAbort();
        ErrLog(DSTORE_ERROR, MODULE_UNDO, ErrMsg("AllocNewUndoPages failed."));
        return DSTORE_FAIL;
    }
    walContext->EndAtomicWal();
    m_needCheckPageId = firstFreePageId;
    ErrLog(
        DSTORE_LOG, MODULE_UNDO,
        ErrMsg("Extend space if needed zid(%d), first free pageId(%hu, %u), last free pageId(%hu, %u), m_pdbId(%u).",
               m_zoneId, firstFreePageId.m_fileId, firstFreePageId.m_blockId, lastFreePageId.m_fileId,
               lastFreePageId.m_blockId, m_pdbId));
    return DSTORE_SUCC;
}

UndoRecPtr UndoZone::InsertUndoRecord(UndoRecord *record)
{
    storage_trace_entry(TRACE_ID_UndoZone__InsertUndoRecord);
    /* Step 1. Get next undo record ptr */
    UndoRecPtr recPtr = m_nextAppendUndoPtr;
    PageId checkPageId = m_needCheckPageId;
    /* Step 2. Insert all data in the undo record */
    PageId curId = recPtr.GetPageId();
    if (unlikely(curId.m_fileId == INVALID_VFS_FILE_ID)) {
        ErrLog(DSTORE_PANIC, MODULE_UNDO,
               ErrMsg("Read page(%hu, %u) failed, zoneId(%d), pdbId(%u).", curId.m_fileId, curId.m_blockId, m_zoneId,
                      m_pdbId));
    }
    if (unlikely(m_currentInsertUndoPageBuf == INVALID_BUFFER_DESC)) {
        m_currentInsertUndoPageBuf = m_bufferMgr->Read(m_pdbId, curId, LW_EXCLUSIVE);
    } else {
        m_currentInsertUndoPageBuf->Pin();
        if (m_currentInsertUndoPageBuf->GetPageId() != curId) {
            m_currentInsertUndoPageBuf->Unpin();
            m_currentInsertUndoPageBuf = m_bufferMgr->Read(m_pdbId, curId, LW_EXCLUSIVE);
        } else {
            if (unlikely(STORAGE_FUNC_FAIL(m_bufferMgr->LockContent(m_currentInsertUndoPageBuf, LW_EXCLUSIVE)))) {
                ErrLog(DSTORE_ERROR, MODULE_UNDO, ErrMsg("Read page(%hu, %u) failed when LockContent.",
                    curId.m_fileId, curId.m_blockId));
                m_currentInsertUndoPageBuf->Unpin();
                return INVALID_UNDO_RECORD_PTR;
            }
        }
    }
    if (unlikely(m_currentInsertUndoPageBuf == INVALID_BUFFER_DESC)) {
        ErrLog(DSTORE_ERROR, MODULE_UNDO, ErrMsg("Read page(%hu, %u) failed.", curId.m_fileId, curId.m_blockId));
        return INVALID_UNDO_RECORD_PTR;
    }

    UndoRecordPage *page = static_cast<UndoRecordPage*>(m_currentInsertUndoPageBuf->GetPage());
    uint16 startingByte = recPtr.GetOffset();
    int alreadyCopyPre = 0;
    int alreadyCopy = 0;
    uint16 recordEndOffset = 0;
    uint32 nextPageCount = 0;
    bool walFinished = false;
    record->Serialize();
    for (;;) {
        /* Step 2.1. Copy to the current undo page */
        alreadyCopyPre = alreadyCopy;
        bool writeFinish = WriteUndoRecord(*record, static_cast<char *>(static_cast<void *>(page)),
                                           static_cast<int>(startingByte), alreadyCopy);

        /* Step 2.2. Write wal */
        if (unlikely(walFinished)) {
            (void)m_bufferMgr->MarkDirty(m_currentInsertUndoPageBuf, false);
        } else {
            uint32 walDataSize = static_cast<uint32>(alreadyCopy - alreadyCopyPre);
            /* Global temp talble undo wal : only need record SerializeData and m_dataInfo.len */
            /* m_dataInfo.len is used to restore undo record */
            uint8 globalTempTableUndoWalSize = record->GetCompressedSize() + sizeof(record->m_dataInfo.len);
            if (record->IsGlobalTempTableUndoRec() && alreadyCopy >= globalTempTableUndoWalSize) {
                walFinished = true;
                StorageAssert(walDataSize >= static_cast<uint32>(alreadyCopy - globalTempTableUndoWalSize));
                walDataSize -= static_cast<uint32>(alreadyCopy - globalTempTableUndoWalSize);
            }
            (void)m_bufferMgr->MarkDirty(m_currentInsertUndoPageBuf);
            StorageAssert(walDataSize != 0);
            RetStatus ret = GenerateWalForUndoRec(m_currentInsertUndoPageBuf, startingByte, walDataSize);
            if (STORAGE_FUNC_FAIL(ret)) {
                m_bufferMgr->UnlockAndRelease(m_currentInsertUndoPageBuf);
                /* The parameters need to be reset to the initial state and rewritten next time. */
                m_nextAppendUndoPtr = recPtr;
                m_needCheckPageId = checkPageId;
                ErrLog(DSTORE_ERROR, MODULE_UNDO, ErrMsg("Generate wal for insert undo record failed."));
                return INVALID_UNDO_RECORD_PTR;
            }
        }

        if (writeFinish) {
            PageId nextPageId = page->GetNextPageId();
            /* Now, we could free the buffer */
            m_bufferMgr->UnlockAndRelease(m_currentInsertUndoPageBuf);

            /*
             * It is possible that we successfully write the current undo record but
             * there are no space left on the current page. In this case, we need to
             * switch to next page without writing any data on it. It could be checked
             * by whether the offset of the end of current record on page is divided by
             * UNDO_RECORD_PAGE_MAX_FREE_SPACE.
             */
            recordEndOffset = static_cast<uint32>((recPtr.GetOffset() - UNDO_RECORD_PAGE_HEADER_SIZE) + alreadyCopy) %
                              UNDO_RECORD_PAGE_MAX_FREE_SPACE;
            if (recordEndOffset == 0) {
                nextPageCount++;
                m_nextAppendUndoPtr = UndoRecPtr(nextPageId, UNDO_RECORD_PAGE_HEADER_SIZE);
                m_currentInsertUndoPageBuf = m_bufferMgr->Read(m_pdbId, m_nextAppendUndoPtr.GetPageId(), LW_SHARED);
                STORAGE_CHECK_BUFFER_PANIC(m_currentInsertUndoPageBuf, MODULE_UNDO, m_nextAppendUndoPtr.GetPageId());
                UndoRecordPage *appendPage = static_cast<UndoRecordPage *>(m_currentInsertUndoPageBuf->GetPage());
                StorageAssert(appendPage);
                m_needCheckPageId = appendPage->GetNextPageId();
                m_bufferMgr->UnlockAndRelease(m_currentInsertUndoPageBuf);
            }
            break;
        }

        /* Step 2.3. Switch to next page */
        nextPageCount++;
        PageId nextPageId = page->GetNextPageId();
        m_bufferMgr->UnlockAndRelease(m_currentInsertUndoPageBuf);
        m_currentInsertUndoPageBuf = m_bufferMgr->Read(m_pdbId, nextPageId, LW_EXCLUSIVE);
        STORAGE_CHECK_BUFFER_PANIC(m_currentInsertUndoPageBuf, MODULE_UNDO, nextPageId);
        page = static_cast<UndoRecordPage *>(m_currentInsertUndoPageBuf->GetPage());
        startingByte = UNDO_RECORD_PAGE_HEADER_SIZE;
        m_nextAppendUndoPtr = {nextPageId, UNDO_RECORD_PAGE_HEADER_SIZE};
        m_needCheckPageId = page->GetNextPageId();
        curId = nextPageId;
    }
    record->DestroyDiskData();

    /* Step 3. Update m_fullyFreePageNum if some undo record cross more than one page. */
    if (nextPageCount > 0) {
        ErrLog(DSTORE_DEBUG1, MODULE_UNDO,
               ErrMsg("Insert undo record, zid(%d), m_nextAppendUndoPtr(%hu, %u, %hu), m_needCheckPageId(%hu, %u), "
                      "nextPageCount = %u.",
                      m_zoneId, m_nextAppendUndoPtr.GetPageId().m_fileId, m_nextAppendUndoPtr.GetPageId().m_blockId,
                      m_nextAppendUndoPtr.GetOffset(), m_needCheckPageId.m_fileId, m_needCheckPageId.m_blockId,
                      nextPageCount));
    }

    /* Step 4. Update undo record ptr in the context, to be used next time */
    OffsetNumber offset = recordEndOffset + UNDO_RECORD_PAGE_HEADER_SIZE;
    m_nextAppendUndoPtr.SetOffset(offset);
    ErrLog(DSTORE_DEBUG1, MODULE_UNDO, ErrMsg("Undozone zid %d, insert undo rect ptr(%hu, %u, %u).",
        m_zoneId, recPtr.GetFileId(), recPtr.GetBlockNum(), recPtr.GetOffset()));
    storage_trace_exit(TRACE_ID_UndoZone__InsertUndoRecord);
    return recPtr;
}

RetStatus UndoZone::GenerateWalForUndoRec(BufferDesc *bufferDesc, OffsetNumber offset, uint32 dataSize)
{
    AtomicWalWriterContext *walContext = thrd->m_walWriterContext;
    UndoRecordPage *page = static_cast<UndoRecordPage *>(bufferDesc->GetPage());
    StorageAssert(page);
    StorageAssert(walContext->HasAlreadyBegin());
    walContext->RememberPageNeedWal(bufferDesc);
    uint32 size = sizeof(WalRecordUndoRecord) + dataSize;
    WalRecordUndoRecord *redoData = static_cast<WalRecordUndoRecord *>(walContext->GetTempWalBuf());
    if (STORAGE_VAR_NULL(redoData)) {
        ErrLog(DSTORE_ERROR, MODULE_UNDO, ErrMsg("Undo wal buf is nullptr."));
        return DSTORE_FAIL;
    }
    bool glsnChangedFlag = (page->GetWalId() != walContext->GetWalId());
    redoData->SetHeader({WAL_UNDO_INSERT_RECORD, size, bufferDesc->GetPageId(), page->GetWalId(), page->GetPlsn(),
                         page->GetGlsn(), glsnChangedFlag, bufferDesc->GetFileVersion()});
    redoData->SetOffset(offset);
    WalRecordForPage::CopyData(redoData->m_data, size - sizeof(WalRecordUndoRecord),
                               static_cast<char *>(static_cast<void *>(page)) + offset, dataSize);
    walContext->PutNewWalRecord(redoData);
    return DSTORE_SUCC;
}

void UndoZone::GenerateWalForInitUndoRecSpace(const PageId &recyclePageId, BufferDesc *segMetaPageBuf)
{
    AtomicWalWriterContext *walContext = thrd->m_walWriterContext;
    UndoSegmentMetaPage *metaPage = GetUndoSegmentMetaPage(segMetaPageBuf);
    StorageAssert(walContext->HasAlreadyBegin());
    walContext->RememberPageNeedWal(segMetaPageBuf);
    uint32 size = static_cast<uint32>(sizeof(WalRecordUndoInitRecSpace));
    WalRecordUndoInitRecSpace redoData;
    bool glsnChangedFlag = (metaPage->GetWalId() != walContext->GetWalId());
    redoData.SetHeader({WAL_UNDO_INIT_RECORD_SPACE, size, segMetaPageBuf->GetPageId(), metaPage->GetWalId(),
        metaPage->GetPlsn(), metaPage->GetGlsn(), glsnChangedFlag, segMetaPageBuf->GetFileVersion()});
    redoData.SetFirstUndoPageId(recyclePageId);
    walContext->PutNewWalRecord(&redoData);
}

template<WalType type>
void UndoZone::GenerateWalForUndoRingOldPage(BufferDesc *bufferDesc, const PageId &pageId)
{
    AtomicWalWriterContext *walContext = thrd->m_walWriterContext;
    UndoRecordPage *page = static_cast<UndoRecordPage *>(bufferDesc->GetPage());
    StorageAssert(walContext->HasAlreadyBegin());
    walContext->RememberPageNeedWal(bufferDesc);
    uint32 size = sizeof(WalRecordUndoRingOldPage);
    WalRecordUndoRingOldPage redoData;
    bool glsnChangedFlag = (page->GetWalId() != walContext->GetWalId());
    uint64 fileVersion = bufferDesc->GetFileVersion();
    redoData.SetHeader({type, size, bufferDesc->GetPageId(), page->GetWalId(), page->GetPlsn(),
        page->GetGlsn(), glsnChangedFlag, fileVersion});
    redoData.SetPageId(pageId);
    walContext->PutNewWalRecord(&redoData);
}

void UndoZone::GenerateWalForUndoRingNewPage(BufferDesc *bufferDesc, const UndoRecordPageHeader &hdr)
{
    AtomicWalWriterContext *walContext = thrd->m_walWriterContext;
    UndoRecordPage *page = static_cast<UndoRecordPage *>(bufferDesc->GetPage());
    StorageAssert(walContext->HasAlreadyBegin());
    walContext->RememberPageNeedWal(bufferDesc);
    uint32 size = sizeof(WalRecordUndoRingNewPage);
    WalRecordUndoRingNewPage redoData;
    bool glsnChangedFlag = (page->GetWalId() != walContext->GetWalId());
    redoData.SetHeader({WAL_UNDO_EXTEND_PAGE_RING_NEW_PAGE, size, bufferDesc->GetPageId(), page->GetWalId(),
        page->GetPlsn(), page->GetGlsn(), glsnChangedFlag, bufferDesc->GetFileVersion()});
    redoData.SetUndoRecPageHdr(hdr);
    walContext->PutNewWalRecord(&redoData);
}

RetStatus UndoZone::FetchUndoRecord(PdbId pdbId, UndoRecord *record, UndoRecPtr undoRecPtr, Xid xid,
                                    BufMgrInterface *bufferMgr, CommitSeqNo *commitCsn)
{
    /*
     * If the slot is recycled, we mustn't fetch record from undoRecPtr,
     * because it may be reused.  Variable needSkipJudge is used in failover scenarios and backup restore scenarios to
     * determine whether a committed transaction has been recycled.
     */
    bool needSkipJudge = false;
    if (commitCsn != nullptr && *commitCsn != INVALID_CSN) {
        CommitSeqNo recycleCsn = g_storageInstance->GetCsnMgr()->GetRecycleCsnMin(pdbId);
        StoragePdb *pdb = g_storageInstance->GetPdb(pdbId);
        if (pdb != nullptr && pdb->GetNeedRollbackBarrierInFailover() &&
            pdb->GetRollbackBarrierCsnInFailover() != INVALID_CSN) {
            recycleCsn = pdb->GetRollbackBarrierCsnInFailover();
        }
        if (*commitCsn < recycleCsn) {
            storage_set_error(UNDO_ERROR_RECORD_RECYCLED);
            return DSTORE_FAIL;
        } else {
            needSkipJudge = true;
        }
    }
    if (!needSkipJudge) {
        if (UndoZoneTrxManager::IsXidRecycled(pdbId, xid, commitCsn)) {
            storage_set_error(UNDO_ERROR_RECORD_RECYCLED);
            return DSTORE_FAIL;
        }
    }

    FetchUndoRecordInternal(pdbId, record, undoRecPtr, bufferMgr);

    return DSTORE_SUCC;
}

void UndoZone::FetchUndoRecordInternal(PdbId pdbId, UndoRecord *record, UndoRecPtr undoRecPtr,
                                       BufMgrInterface *bufferMgr, bool needStrictCheckUndo, bool *isVaild)
{
    StorageAssert(undoRecPtr != INVALID_UNDO_RECORD_PTR);
    /* Read all data in the undo record */
    PageId curId{undoRecPtr.GetFileId(), undoRecPtr.GetBlockNum()};
    BufferTag bufTag = {pdbId, curId};
    int startingByte = undoRecPtr.GetOffset();
    BufferDesc *curBufferDesc = record->GetCurrentFetchBuf();
    if (unlikely(curBufferDesc == INVALID_BUFFER_DESC)) {
        curBufferDesc = bufferMgr->Read(pdbId, curId, LW_SHARED);
        STORAGE_CHECK_BUFFER_PANIC(curBufferDesc, MODULE_UNDO, curId);
    } else {
        curBufferDesc->Pin();
        if (curBufferDesc->GetPageId() != curId) {
            curBufferDesc->Unpin();
            curBufferDesc = bufferMgr->Read(pdbId, curId, LW_SHARED);
            STORAGE_CHECK_BUFFER_PANIC(curBufferDesc, MODULE_UNDO, curId);
        } else {
            (void)bufferMgr->LockContent(curBufferDesc, LW_SHARED);
        }
    }

    Page *firstUndoPage = curBufferDesc->GetPage();
    StorageAssert(firstUndoPage);
    uint64 firstGlsn = firstUndoPage->GetGlsn();
    uint64 firstPlsn = firstUndoPage->GetPlsn();
    WalId firstWalId = firstUndoPage->GetWalId();

    ReadUndoRecord(pdbId, *record, curBufferDesc, startingByte, bufferMgr);
    /* Need set fetch buf here for next fetch. */
    record->SetCurrentFetchBuf(curBufferDesc);
    /* May sitch page when read undo, we need record last undo page now for log. */
    Page *lastUndoPage = curBufferDesc->GetPage();
    uint64 lastGlsn = lastUndoPage->GetGlsn();
    uint64 lastPlsn = lastUndoPage->GetPlsn();
    WalId lastWalId = lastUndoPage->GetWalId();

    ErrLevel errorLevel = DSTORE_LOG;
    if (needStrictCheckUndo) {
        errorLevel = DSTORE_PANIC;
    }
    /* Check UndoData */
    char *recInfo = nullptr;
    if (unlikely(!record->IsUndoDataValid())) {
        char *clusterBufferInfo = g_storageInstance->GetBufferMgr()->GetClusterBufferInfo(
            bufTag.pdbId, bufTag.pageId.m_fileId, bufTag.pageId.m_blockId);
        if (likely(clusterBufferInfo != nullptr)) {
            ErrLog(DSTORE_LOG, MODULE_UNDO, ErrMsg("%s", clusterBufferInfo));
        }
        if (isVaild != nullptr) {
            *isVaild = false;
        }
        recInfo = record->Dump();
        if (likely(recInfo)) {
            ErrLog(DSTORE_LOG, MODULE_UNDO, ErrMsg("recInfo:%s", recInfo));
        }
        ErrLog(errorLevel, MODULE_UNDO,
               ErrMsg("UndoData is invalid! undoPtr = ({%hu, %u}, %hu), "
                      "first page walId:%lu, first page glsn:%lu, first page "
                      "plsn:%lu, last page walId:%lu, last page glsn:%lu, last page plsn:%lu.",
                      undoRecPtr.GetFileId(), undoRecPtr.GetBlockNum(), undoRecPtr.GetOffset(), firstWalId, firstGlsn,
                      firstPlsn, lastWalId, lastGlsn, lastPlsn));
        DstorePfreeExt(recInfo);
        bufferMgr->UnlockAndRelease(curBufferDesc);
        return;
    }

    bufferMgr->UnlockAndRelease(curBufferDesc);

    /* Check PreUndoPtr */
    UndoRecPtr preUndoPtr = UndoRecPtr(record->GetTxnPreUndoPtr());
    if (preUndoPtr != INVALID_UNDO_RECORD_PTR) {
        TablespaceId tablespaceId = static_cast<TablespaceId>(TBS_ID::UNDO_TABLE_SPACE_ID);
        TablespaceMgr *tablespaceMgr = g_storageInstance->GetTablespaceMgrByPdbId(pdbId);
        if (STORAGE_VAR_NULL(tablespaceMgr)) {
            ErrLog(DSTORE_PANIC, MODULE_UNDO, ErrMsg("Failed to get tablespaceMgr, pdbId %u.", pdbId));
            return;
        }
        TableSpace *tablespace = tablespaceMgr->OpenTablespace(tablespaceId, DSTORE::DSTORE_NO_LOCK);
        if (STORAGE_VAR_NULL(tablespace)) {
            ErrLog(DSTORE_ERROR, MODULE_UNDO,
                   ErrMsg("Failed to open tablespace %u, pdbId %u."
                          "undoPtr = ({%hu, %u}, %hu), first page walId:%lu, first page glsn:%lu, first page"
                          " plsn:%lu, last page walId:%lu, last page glsn:%lu, last page plsn:%lu.",
                          tablespaceId, pdbId, undoRecPtr.GetFileId(), undoRecPtr.GetBlockNum(), undoRecPtr.GetOffset(),
                          firstWalId, firstGlsn, firstPlsn, lastWalId, lastGlsn, lastPlsn));
            return;
        }
        TbsDataFile **dataFiles = tablespaceMgr->GetDataFiles();
        TbsDataFile *dataFile = dataFiles[preUndoPtr.GetFileId()];

        if (STORAGE_VAR_NULL(dataFile)) {
            if (isVaild != nullptr) {
                *isVaild = false;
            }
            recInfo = record->Dump();
            if (likely(recInfo)) {
                ErrLog(DSTORE_LOG, MODULE_UNDO, ErrMsg("recInfo:%s", recInfo));
            }
            ErrLog(errorLevel, MODULE_UNDO,
                   ErrMsg("PreUndoPtr is invalid! undoPtr = ({%hu, %u}, %hu), "
                          "first page walId:%lu, first page glsn:%lu, first page "
                          "plsn:%lu, last page walId:%lu, last page glsn:%lu, last page plsn:%lu, recInfo = %s.",
                          undoRecPtr.GetFileId(), undoRecPtr.GetBlockNum(), undoRecPtr.GetOffset(), firstWalId,
                          firstGlsn, firstPlsn, lastWalId, lastGlsn, lastPlsn, recInfo));
            DstorePfreeExt(recInfo);
        }
        tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_NO_LOCK);
    }
}

bool UndoZone::IsRecyclingComplete() const
{
    return m_txnSlotManager->GetNextFreeLogicSlotId() == m_txnSlotManager->GetRecycleLogicSlotId();
}

RetStatus UndoZone::RestoreUndoZoneFromTxnSlots()
{
    PageId segmentId = m_segment->GetSegmentMetaPageId();
    PageId startPageId = {segmentId.m_fileId, segmentId.m_blockId + 1};
    StorageAssert(m_txnSlotManager != nullptr);
    if (STORAGE_FUNC_FAIL(m_txnSlotManager->Init(startPageId, false))) {
        return DSTORE_FAIL;
    }

    /* Only primary pdb need to restore transaction slot and write ptr */
    StoragePdb *pdb = g_storageInstance->GetPdb(m_pdbId);
    if (unlikely(pdb == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_UNDO,
               ErrMsg("Pdb is nullptr when restore, zoneId(%d), pdbId(%u).", m_zoneId, m_pdbId));
        return DSTORE_FAIL;
    }

    PdbRoleMode pdbRoleMode = pdb->GetPdbRoleMode();
    if (pdbRoleMode != PdbRoleMode::PDB_PRIMARY) {
        RetStatus ret = DSTORE_SUCC;
        ErrLevel elevel = DSTORE_LOG;
        if (pdbRoleMode == PdbRoleMode::PDB_INVALID) {
            ret = DSTORE_FAIL;
            elevel = DSTORE_ERROR;
        }
        ErrLog(elevel, MODULE_UNDO,
           ErrMsg("RestoreUndoZoneFromTxnSlots zid %d, GetPdbRoleMode not primary:%hu, pdbId(%u).",
                  m_zoneId, static_cast<uint8>(pdbRoleMode), m_pdbId));
        return ret;
    }

    UndoMgr *undoMgr = pdb->GetUndoMgr();
    if (unlikely(undoMgr == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_UNDO,
               ErrMsg("Undo mgr is nullptr when restore, zoneId(%d), pdbId(%u).", m_zoneId, m_pdbId));
        return DSTORE_FAIL;
    }
    StorageReleasePanic(!undoMgr->IsZoneOccupied(m_zoneId), MODULE_UNDO,
                        ErrMsg("Restore zid should occupied! zoneid = %d", m_zoneId));

    int64 firstAppendUndoSlotId = INVALID_TXN_SLOT_ID;
    UndoRecPtr firstAppendSlotTailPtr =
        INVALID_UNDO_RECORD_PTR; /* The tail pointer of the first transaction slot that was not recycled */
    
    TailUndoPtrStatus tailPtrStatus = UNKNOWN_STATUS;
    UndoRecPtr tailUndoPtr = m_txnSlotManager->Restore(tailPtrStatus);
    StorageReleasePanic(tailPtrStatus == UNKNOWN_STATUS, MODULE_UNDO,
                        ErrMsg("Invalid tail undo ptr status! zoneId(%d), pdbId(%u).", m_zoneId, m_pdbId));
    int fetchTimes = 0;  // Fetch undo times
    if (tailPtrStatus == NEED_FETCH_FROM_COMMITED_SLOT) {
        /* This situation occurs when there are committed but can not be recycled transactions in the current zone, and
         * no undo records was ever inserted within the first can not be recycled transactions. */
        m_txnSlotManager->GetFirstSpaceUndoRecPtr(firstAppendSlotTailPtr, firstAppendUndoSlotId);
        tailUndoPtr = firstAppendSlotTailPtr;
        ErrLog(DSTORE_LOG, MODULE_UNDO,
               ErrMsg("RestoreUndoZoneFromTxnSlots zid %d , tail ptr status is need fetch from commited slot, "
                      "firstAppendUndoSlotId(%ld), firstAppendSlotTailPtr(%hu, %u, %hu), pdbId(%u).",
                      m_zoneId, firstAppendUndoSlotId, firstAppendSlotTailPtr.GetFileId(),
                      firstAppendSlotTailPtr.GetBlockNum(), firstAppendSlotTailPtr.GetOffset(), m_pdbId));
    }

    if (tailUndoPtr != INVALID_UNDO_RECORD_PTR) {
        /* fetch the start undo ptr of the transaction by tailUndoPtr include NEED_FETCH_FROM_COMMITED_SLOT and
         * VALID_STATUS status. */
        UndoRecord undoRec;
        UndoRecPtr curUndoPtr = tailUndoPtr;
        FetchUndoRecordInternal(m_pdbId, &undoRec, curUndoPtr, m_bufferMgr);
        while (undoRec.GetTxnPreUndoPtr() != INVALID_UNDO_RECORD_PTR) {
            fetchTimes++;
            curUndoPtr = UndoRecPtr(undoRec.GetTxnPreUndoPtr());
            FetchUndoRecordInternal(m_pdbId, &undoRec, curUndoPtr, m_bufferMgr);
        }
        m_undoRecyclePageId = curUndoPtr.GetPageId();
    } else {
        /* The zone has no valid undo records */
        BufferDesc *metaBufDesc = ReadSegMetaPageBuf(LW_SHARED);
        if (unlikely(metaBufDesc == INVALID_BUFFER_DESC)) {
            ErrLog(DSTORE_ERROR, MODULE_UNDO,
                   ErrMsg("Read segment meta page from buffer failed, zoneId(%d), pdbId(%u).", m_zoneId, m_pdbId));
            return DSTORE_FAIL;
        }
        UndoSegmentMetaPage *metaPage = GetUndoSegmentMetaPage(metaBufDesc);
        if (unlikely(metaPage == nullptr)) {
            UnlockAndReleaseSegMetaPageBuf(metaBufDesc);
            PageId segMetaPageId = m_segment->GetSegmentMetaPageId();
            ErrLog(DSTORE_PANIC, MODULE_UNDO,
                   ErrMsg("Meta page is null, zoneId(%d), segment meta pageId (%hu, %u)pdbId(%u).", m_zoneId,
                          segMetaPageId.m_fileId, segMetaPageId.m_blockId, m_pdbId));
        }
        PageId firstUndoPageId = metaPage->firstUndoPageId;
        UnlockAndReleaseSegMetaPageBuf(metaBufDesc);
        m_undoRecyclePageId = firstUndoPageId;
    }
    StorageReleasePanic(m_undoRecyclePageId.m_fileId == INVALID_VFS_FILE_ID, MODULE_UNDO,
                        ErrMsg("Restore m_undoRecyclePageId failed! zoneid = %d", m_zoneId));
    RestoreUndoWritePtr(firstAppendUndoSlotId, tailPtrStatus);
    Xid rollbackXid = INVALID_XID;
    if (m_txnSlotManager->IsUndoZoneNeedRollback(rollbackXid)) {
        SetAsyncRollbackState(true);
        g_storageInstance->GetPdb(m_pdbId)->GetTransactionMgr()->AsyncRollback(rollbackXid, this);
    }

    /* after resotre unpersistent content of undozone, now you can add owned, means can start service and recycle */
    undoMgr->AddZoneOwned(m_zoneId);
    uint64 nextFreeLogicSlotId = m_txnSlotManager->GetNextFreeLogicSlotId();
    uint64 recycleLogicSlotId = m_txnSlotManager->GetRecycleLogicSlotId();
    ErrLog(DSTORE_LOG, MODULE_UNDO,
           ErrMsg("Undozone zid %d, restore undo from slots, tailUndoPtr(%hu, %u, %hu), "
                  "undoRecyclePageid(%hu, %u), nextAppendUndoPtr(%d, %u, %d), "
                  "fetchTime(%d), nextFreeLogicSlotId(%lu), recycleLogicSlotId(%lu), pdbId(%u).",
                  m_zoneId, tailUndoPtr.GetFileId(), tailUndoPtr.GetBlockNum(), tailUndoPtr.GetOffset(),
                  m_undoRecyclePageId.m_fileId, m_undoRecyclePageId.m_blockId, m_nextAppendUndoPtr.GetFileId(),
                  m_nextAppendUndoPtr.GetBlockNum(), m_nextAppendUndoPtr.GetOffset(), fetchTimes, nextFreeLogicSlotId,
                  recycleLogicSlotId, m_pdbId));
    return DSTORE_SUCC;
}

void UndoZone::RestoreUndoWritePtr(int64 firstAppendUndoSlotId, TailUndoPtrStatus tailPtrStatus)
{
    UndoRecPtr lastPtr = INVALID_UNDO_RECORD_PTR;
    if (tailPtrStatus == NO_VALID_TAIL_UNDO_PTR) {
        m_nextAppendUndoPtr = {m_undoRecyclePageId, UNDO_RECORD_PAGE_HEADER_SIZE};
    } else if (tailPtrStatus == NEED_FETCH_FROM_COMMITED_SLOT) {
        if (firstAppendUndoSlotId == INVALID_TXN_SLOT_ID) {
            m_nextAppendUndoPtr = {m_undoRecyclePageId, UNDO_RECORD_PAGE_HEADER_SIZE};
        } else {
            lastPtr = m_txnSlotManager->GetLastSpaceUndoRecPtr(firstAppendUndoSlotId);
            UndoRecord undoRec;
            FetchUndoRecordInternal(m_pdbId, &undoRec, lastPtr, m_bufferMgr);
            m_nextAppendUndoPtr = GetNextUndoRecPtr(lastPtr, undoRec.GetRecordSize());
        }
    } else {
        lastPtr = m_txnSlotManager->GetLastSpaceUndoRecPtr();
        UndoRecord undoRec;
        FetchUndoRecordInternal(m_pdbId, &undoRec, lastPtr, m_bufferMgr);
        m_nextAppendUndoPtr = GetNextUndoRecPtr(lastPtr, undoRec.GetRecordSize());
    }
    BufferDesc *pageBuf = m_bufferMgr->Read(m_pdbId, m_nextAppendUndoPtr.GetPageId(), LW_SHARED);
    STORAGE_CHECK_BUFFER_PANIC(pageBuf, MODULE_UNDO, m_nextAppendUndoPtr.GetPageId());
    UndoRecordPage *appendPage = static_cast<UndoRecordPage *>(pageBuf->GetPage());
    StorageReleasePanic(
        appendPage == nullptr, MODULE_UNDO,
        ErrMsg("Append page (%hu, %u) when restore undo writer ptr, zoneId (%d), pdbId[%u].",
               m_nextAppendUndoPtr.GetPageId().m_fileId, m_nextAppendUndoPtr.GetPageId().m_blockId, m_zoneId, m_pdbId));
    m_needCheckPageId = appendPage->GetNextPageId();
    m_bufferMgr->UnlockAndRelease(pageBuf);

    ErrLog(DSTORE_DEBUG1, MODULE_UNDO,
           ErrMsg("Undozone zid %d, restore undo write ptr, lastPtr(%d, %u, %d, "
                  "m_nextAppendUndoPtr(%d, %u, %d), m_needCheckPageId(%d, %u).",
                  m_zoneId, lastPtr.GetFileId(), lastPtr.GetBlockNum(), lastPtr.GetOffset(),
                  m_nextAppendUndoPtr.GetFileId(), m_nextAppendUndoPtr.GetBlockNum(), m_nextAppendUndoPtr.GetOffset(),
                  m_needCheckPageId.m_blockId, m_needCheckPageId.m_fileId));

    StorageReleasePanic(m_nextAppendUndoPtr.GetPageId().m_fileId == INVALID_VFS_FILE_ID, MODULE_UNDO,
                        ErrMsg("Restore m_nextAppendUndoPtr failed! zoneid = %d", m_zoneId));
}

RetStatus UndoZone::RollbackUndoZone(Xid xid, bool isRecovery)
{
    ErrLog(DSTORE_DEBUG1, MODULE_UNDO, ErrMsg("Undozone zid %d, Rollback transaction xid(%d, %lu)",
        m_zoneId, static_cast<int32>(xid.m_zoneId), xid.m_logicSlotId));
    UndoRecPtr tailRecPtr = INVALID_UNDO_RECORD_PTR;
    m_txnSlotManager->GetSlotCurTailUndoPtr(xid, tailRecPtr);

    RetStatus status = RollbackUndoRecords(xid, INVALID_UNDO_RECORD_PTR, tailRecPtr, isRecovery);
    if (status == DSTORE_SUCC) {
        return m_txnSlotManager->RollbackTxnSlot(xid);
    }
    ErrLog(DSTORE_ERROR, MODULE_UNDO, ErrMsg("Rollback undo records failed!"
        "undoPtr = ({%hu, %u}, %hu)", tailRecPtr.GetFileId(), tailRecPtr.GetBlockNum(), tailRecPtr.GetOffset()));
    return status;
}

void UndoZone::RollbackTdForBackupRestore(BtreeUndoContext &btrUndoCxt)
{
    if (!g_storageInstance->IsInBackupRestore(m_pdbId) || btrUndoCxt.m_isDeletionPruned) {
        return;
    }
    Xid xid = btrUndoCxt.m_xid;
    BtreeUndoContext *btrUndoCxtForTdRb = DstoreNew(g_dstoreCurrentMemoryContext)
        BtreeUndoContext(btrUndoCxt, BtreeUndoContextType::BACKUP_RESTORE_ROLLBACK);
    StorageReleasePanic(btrUndoCxtForTdRb == nullptr, MODULE_UNDO,
                        ErrMsg("Failed to allocate size %lu", sizeof(BtreeUndoContext)));
    BtrPage *btrPage = static_cast<BtrPage *>(btrUndoCxtForTdRb->m_currPage);
    uint8 tdId = btrPage->GetTupleTdId(btrUndoCxtForTdRb->m_offset);
    bool find = false;
    while (true) {
        Xid tdXid = Xid(btrPage->GetTd(tdId)->m_xid);
        if (tdXid == xid) {
            find = true;
            break;
        }
        RetStatus ret = btrPage->RollbackByXid(m_pdbId, tdXid, btrUndoCxt.m_bufMgr, btrUndoCxt.m_currBuf, &btrUndoCxt);
        StorageReleasePanic(STORAGE_FUNC_FAIL(ret), MODULE_UNDO, ErrMsg("Transaction xid(%d, %lu) rollback failed",
            static_cast<int32>(tdXid.m_zoneId), tdXid.m_logicSlotId));
    }
    btrUndoCxtForTdRb->Destroy();
    delete btrUndoCxtForTdRb;
    StorageReleasePanic(!find, MODULE_UNDO, ErrMsg("Find dest xid fail"));
    ErrLog(DSTORE_DEBUG1, MODULE_UNDO, ErrMsg("Rollback td(%hhu) to dest xid(%d, %lu), page id(%hu, %u)",
        tdId, static_cast<int32>(xid.m_zoneId), xid.m_logicSlotId, btrPage->GetFileId(), btrPage->GetBlockNum()));
}

/*
 * This function rollback all undo records in range (startUndoPtr, endUndoPtr].
 */
RetStatus UndoZone::RollbackUndoRecords(Xid xid, UndoRecPtr startUndoPtr, UndoRecPtr endUndoPtr, bool isRecovery)
{
    RetStatus status = DSTORE_SUCC;
    UndoRecord undoRec;
    BufferDesc *bufferDesc = nullptr;
    DataPage *page = nullptr;
    UndoRecPtr currUndoPtr = INVALID_UNDO_RECORD_PTR;
    UndoRecPtr nextRollbackUndoPtr = endUndoPtr;
    BufMgrInterface *bufMgr = nullptr;

    if ((endUndoPtr == INVALID_UNDO_RECORD_PTR) || (startUndoPtr == endUndoPtr)) {
        ErrLog(DSTORE_LOG, MODULE_UNDO,
               ErrMsg("Undozone zid %d, rollback undo record skip. pdbId:%u, tailUndoPtr(%hu, %u, %hu), "
                      "startUndoPtr(%hu, %u, %hu).",
                      m_zoneId, m_pdbId, endUndoPtr.GetFileId(), endUndoPtr.GetBlockNum(), endUndoPtr.GetOffset(),
                      startUndoPtr.GetFileId(), startUndoPtr.GetBlockNum(), startUndoPtr.GetOffset()));
        return status;
    }
    if (g_storageInstance->GetPdb(m_pdbId) == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_UNDO,
               ErrMsg("Rollback undo records failed, GetPdb(%u) is empty.", m_pdbId));
        return DSTORE_FAIL;
    }

    if (g_storageInstance->GetPdb(m_pdbId)->GetUndoMgr() == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_UNDO,
               ErrMsg("Rollback undo records failed, GetUndoMgr() is empty, pdbId is %u.", m_pdbId));
        return DSTORE_FAIL;
    }

    StorageAssert(g_storageInstance->GetPdb(m_pdbId)->GetUndoMgr()->IsZoneOwned(m_zoneId));
    StorageAssert(m_zoneId == static_cast<ZoneId>(xid.m_zoneId));
    AtomicWalWriterContext *walContext = thrd->m_walWriterContext;

    BtreeUndoContext *btrUndoCxt = nullptr; /* for btree page only. should keep nullptr for heap page */
    CommitSeqNo commitCsn = INVALID_CSN; /* only for backup restore */
    
    while (nextRollbackUndoPtr != startUndoPtr) {
        currUndoPtr = nextRollbackUndoPtr;
        status = FetchUndoRecord(m_pdbId, &undoRec, nextRollbackUndoPtr, xid, m_bufferMgr, &commitCsn);
        if (STORAGE_FUNC_FAIL(status)) {
            CommitSeqNo recycleCsn = g_storageInstance->GetCsnMgr()->GetRecycleCsnMin(m_pdbId);
            CommitSeqNo barrierCsn = INVALID_CSN;
            StoragePdb *pdb = g_storageInstance->GetPdb(m_pdbId);
            if (pdb != nullptr && pdb->GetNeedRollbackBarrierInFailover() &&
                pdb->GetRollbackBarrierCsnInFailover() != INVALID_CSN) {
                barrierCsn = pdb->GetRollbackBarrierCsnInFailover();
            }
            UndoMgr *undoMgr = g_storageInstance->GetPdb(m_pdbId)->GetUndoMgr();
            uint64 recycleLogicSlotId = undoMgr->GetRecycleLogicSlotIdFromCache(m_zoneId);
            PageId segmentId = undoMgr->GetUndoZoneSegmentId(xid.m_zoneId, true);
            PageId trxSlotPageId;
            trxSlotPageId.m_fileId = segmentId.m_fileId;
            trxSlotPageId.m_blockId =
                segmentId.m_blockId + 1 + (xid.m_logicSlotId / TRX_PAGE_SLOTS_NUM) % TRX_PAGES_PER_ZONE;
            bufferDesc = m_bufferMgr->Read(m_pdbId, trxSlotPageId, LW_SHARED);
            if (likely(bufferDesc != nullptr)) {
                TransactionSlotPage *targetSlotPage = static_cast<TransactionSlotPage *>(bufferDesc->GetPage());
                char *dumpPage = targetSlotPage->Dump();
                TransactionSlotPage::PrevDumpPage(dumpPage);
                DstorePfreeExt(dumpPage);
                ErrLog(DSTORE_ERROR, MODULE_UNDO,
                       ErrMsg("Xid is alread been recycled when rollback! "
                              "undoPtr = ({%hu, %u}, %hu), xid(%d, %lu), pdbId(%u), recyclecsn(%lu), barrierCsn(%lu), "
                              "recycleLogicSlotId(%lu), segmentId(%d, %u).",
                              nextRollbackUndoPtr.GetFileId(), nextRollbackUndoPtr.GetBlockNum(),
                              nextRollbackUndoPtr.GetOffset(), static_cast<int32>(xid.m_zoneId), xid.m_logicSlotId,
                              m_pdbId, recycleCsn, barrierCsn, recycleLogicSlotId, segmentId.m_fileId,
                              segmentId.m_blockId));
                StorageReleaseBufferCheckPanic(true, MODULE_TRANSACTION, bufferDesc->GetBufferTag(),
                                               "Slot status is wrong when rollback!");
            } else {
                ErrLog(DSTORE_PANIC, MODULE_UNDO,
                       ErrMsg("Xid is alread been recycled when rollback! "
                              "undoPtr = ({%hu, %u}, %hu), xid(%d, %lu), pdbId(%u), csnmin(%lu), "
                              "recycleLogicSlotId(%lu), segmentId(%d, %u).",
                              nextRollbackUndoPtr.GetFileId(), nextRollbackUndoPtr.GetBlockNum(),
                              nextRollbackUndoPtr.GetOffset(), static_cast<int32>(xid.m_zoneId), xid.m_logicSlotId,
                              m_pdbId, recycleCsn, recycleLogicSlotId, segmentId.m_fileId, segmentId.m_blockId));
            }
        }

        nextRollbackUndoPtr = undoRec.GetTxnPreUndoPtr();

        if (undoRec.IsGlobalTempTableUndoRec() && isRecovery) {
            walContext->BeginAtomicWal(xid);
            if (unlikely(STORAGE_FUNC_FAIL(m_txnSlotManager->SetSlotUndoPtr(xid, nextRollbackUndoPtr, false)))) {
                ErrLog(DSTORE_ERROR, MODULE_UNDO,
                    ErrMsg("RollbackUndoRecords SetSlotUndoPtr fail. "
                    "curr undoRecPtr(%hu, %u, %hu), next undoRecPtr(%hu, %u, %hu), xid(%d, %lu).",
                    currUndoPtr.GetFileId(), currUndoPtr.GetBlockNum(), currUndoPtr.GetOffset(),
                    nextRollbackUndoPtr.GetFileId(), nextRollbackUndoPtr.GetBlockNum(), nextRollbackUndoPtr.GetOffset(),
                    static_cast<int32>(xid.m_zoneId), xid.m_logicSlotId));
                (void)walContext->EndAtomicWal();
                return DSTORE_FAIL;
            }
            (void)walContext->EndAtomicWal();
            continue;
        }
        bufMgr = undoRec.IsGlobalTempTableUndoRec() ? thrd->GetTmpLocalBufMgr() : m_bufferMgr;
        bufferDesc = bufMgr->Read(m_pdbId, undoRec.GetPageId(), LW_EXCLUSIVE);
        if (unlikely(bufferDesc == INVALID_BUFFER_DESC)) {
            if (isRecovery && thrd->GetErrorCode() == VFS_WARNING_FILE_NOT_EXISTS) {
                /* If the file is not exits, it means that the tablespace has been dropped. We can skip this record. */
                ErrLog(DSTORE_LOG, MODULE_UNDO,
                    ErrMsg("Tablespace is dropped, undoRec PageId(%hu, %u), pdbid %u, xid(%d, %lu). Skip this record."
                        "curr undoRecPtr(%hu, %u, %hu), next undoRecPtr(%hu, %u, %hu)",
                        undoRec.GetPageId().m_fileId, undoRec.GetPageId().m_blockId,
                        m_pdbId, static_cast<int32>(xid.m_zoneId), xid.m_logicSlotId,
                        currUndoPtr.GetFileId(), currUndoPtr.GetBlockNum(), currUndoPtr.GetOffset(),
                        nextRollbackUndoPtr.GetFileId(), nextRollbackUndoPtr.GetBlockNum(),
                        nextRollbackUndoPtr.GetOffset()));
                continue;
            } else {
                ErrLog(DSTORE_PANIC, MODULE_UNDO,
                    ErrMsg("Page read failed, undoRec PageId(%hu, %u), pdbid %u, xid(%d, %lu)."
                        "curr undoRecPtr(%hu, %u, %hu), next undoRecPtr(%hu, %u, %hu)",
                        undoRec.GetPageId().m_fileId, undoRec.GetPageId().m_blockId,
                        m_pdbId, static_cast<int32>(xid.m_zoneId), xid.m_logicSlotId,
                        currUndoPtr.GetFileId(), currUndoPtr.GetBlockNum(), currUndoPtr.GetOffset(),
                        nextRollbackUndoPtr.GetFileId(), nextRollbackUndoPtr.GetBlockNum(),
                        nextRollbackUndoPtr.GetOffset()));
            }
        }
        page = static_cast<DataPage*>(bufferDesc->GetPage());
        TdId tdId = undoRec.GetTdId();
        if (unlikely(!IsPageTypeMatchUndoRecordType(page, undoRec))) {
            /* DDL may drop segment, page may be reuused */
            ErrLog(DSTORE_LOG, MODULE_UNDO,
                ErrMsg("Page may be reuused, curr undoRecPtr(%hu, %u, %hu), next undoRecPtr(%hu, %u, %hu), "
                       "pdbid %u, xid(%d, %lu).",
                       currUndoPtr.GetFileId(), currUndoPtr.GetBlockNum(), currUndoPtr.GetOffset(),
                       nextRollbackUndoPtr.GetFileId(), nextRollbackUndoPtr.GetBlockNum(),
                       nextRollbackUndoPtr.GetOffset(), m_pdbId, static_cast<int32>(xid.m_zoneId), xid.m_logicSlotId));
            bufMgr->UnlockAndRelease(bufferDesc);
            continue;
        }

        if (undoRec.IsBtreeUndoRecord()) {
            /*
             * For index, we firstly check whether the undo record related index tuple is belonged to the page.
             * if not, traversal to right until find it.
             */
            bool needFreeOutside = true;
            bool isCommitTrx = (commitCsn != INVALID_CSN);
            btrUndoCxt = BtreeUndoContext::FindUndoRecRelatedPage(
                btrUndoCxt, this->GetPdbId(), bufferDesc, xid, &undoRec, bufMgr, needFreeOutside, currUndoPtr, isCommitTrx);
            if (STORAGE_VAR_NULL(btrUndoCxt)) {
                if (needFreeOutside) {
                    bufMgr->UnlockAndRelease(bufferDesc);
                }
                ErrLog(DSTORE_ERROR, MODULE_UNDO,
                       ErrMsg("Failed to get BtreeUndoContext when rollback undoRec for xid(%d, %lu), "
                              "curr undoRecPtr(%hu, %u, %hu), next undoRecPtr(%hu, %u, %hu), "
                              "indexTuple(%hu, %u, %u), undoType:%u, ErrMsg:%s",
                              static_cast<int32>(xid.m_zoneId), xid.m_logicSlotId,
                              currUndoPtr.GetFileId(), currUndoPtr.GetBlockNum(), currUndoPtr.GetOffset(),
                              nextRollbackUndoPtr.GetFileId(), nextRollbackUndoPtr.GetBlockNum(),
                              nextRollbackUndoPtr.GetOffset(), undoRec.GetPageId().m_fileId,
                              undoRec.GetPageId().m_blockId, undoRec.GetCtid().GetOffset(), undoRec.GetUndoType(),
                              thrd->GetErrorMessage()));
                return DSTORE_FAIL;
            }
            bufferDesc = btrUndoCxt->m_currBuf;
            if (bufferDesc == INVALID_BUFFER_DESC) {
                page = nullptr;
                tdId = INVALID_TD_SLOT;
            } else {
                page = static_cast<DataPage*>(bufferDesc->GetPage());
                tdId = page->GetTupleTdId<IndexTuple>(btrUndoCxt->m_offset);
                RollbackTdForBackupRestore(*btrUndoCxt);
            }
        }
        walContext->BeginAtomicWal(xid);
        /* To avoid to rollback the same page twice, check and skip if rollback of page has already been done earlier */
        if ((tdId != INVALID_TD_SLOT) && (page->GetTd(tdId)->m_xid == xid.m_placeHolder)) {
            RetStatus ret = page->RollbackByUndoRec(&undoRec, btrUndoCxt);
            StorageReleasePanic(STORAGE_FUNC_FAIL(ret), MODULE_UNDO, ErrMsg("Transaction xid(%d, %lu) rollback failed",
                static_cast<int32>(xid.m_zoneId), xid.m_logicSlotId));
            (void)bufMgr->MarkDirty(bufferDesc);

            /* For BtrPage, rollback may eventually executed on a different page with the page that we have saved in
             * the undo record because of btree splitting.
             * We would copy the undoRec into wal record, and redo the wal record with the ctid from undoRec directly
             * when recovery. Thus the ctid of undoRec must be updated to where the rollback actually happens
             */
            if (page->GetType() == PageType::INDEX_PAGE_TYPE) {
                StorageReleasePanic(btrUndoCxt == nullptr, MODULE_UNDO,
                                    ErrMsg("Btree context is null when xid(%d, %lu) rollback.",
                                           static_cast<int32>(xid.m_zoneId), xid.m_logicSlotId));
                undoRec.SetCtid({page->GetSelfPageId(), btrUndoCxt->m_offset});
                UndoZone::GenerateWalForRollback(bufferDesc, undoRec, WAL_UNDO_BTREE);
            } else {
                UndoZone::GenerateWalForRollback(bufferDesc, undoRec, WAL_UNDO_HEAP);
            }
        } else {
            Xid tdXid = (tdId != INVALID_TD_SLOT && page != nullptr) ? page->GetTd(tdId)->GetXid() : INVALID_XID;
            ErrLog(DSTORE_LOG, MODULE_UNDO,
                ErrMsg("xid(%d, %lu), rollback skip td. curr undoRecPtr(%hu, %u, %hu), "
                       "next undoRecPtr(%hu, %u, %hu), tdId(%hhu), tdxid(%d, %lu), pdbId:%u",
                       static_cast<int32>(xid.m_zoneId), xid.m_logicSlotId,
                       currUndoPtr.GetFileId(), currUndoPtr.GetBlockNum(), currUndoPtr.GetOffset(),
                       nextRollbackUndoPtr.GetFileId(), nextRollbackUndoPtr.GetBlockNum(),
                       nextRollbackUndoPtr.GetOffset(), tdId, static_cast<int32>(tdXid.m_zoneId),
                       tdXid.m_logicSlotId, m_pdbId));
            if (page != nullptr) {
                ErrLog(DSTORE_LOG, MODULE_UNDO, ErrMsg(PAGE_HEADER_FMT, PAGE_HEADER_VAL(page)));
            }
        }
        if (unlikely(STORAGE_FUNC_FAIL(m_txnSlotManager->SetSlotUndoPtr(xid, nextRollbackUndoPtr, false)))) {
            ErrLog(DSTORE_ERROR, MODULE_UNDO,
                ErrMsg("RollbackUndoRecords SetSlotUndoPtr fail. "
                       "xid(%d, %lu), curr undoRecPtr(%hu, %u, %hu), next undoRecPtr(%hu, %u, %hu)",
                       static_cast<int32>(xid.m_zoneId), xid.m_logicSlotId,
                       currUndoPtr.GetFileId(), currUndoPtr.GetBlockNum(), currUndoPtr.GetOffset(),
                       nextRollbackUndoPtr.GetFileId(), nextRollbackUndoPtr.GetBlockNum(),
                       nextRollbackUndoPtr.GetOffset()));
            status = DSTORE_FAIL;
        }
        if (likely(bufferDesc != INVALID_BUFFER_DESC)) {
            bufMgr->UnlockAndRelease(bufferDesc);
        }
        (void)walContext->EndAtomicWal();
    }
    if (btrUndoCxt != nullptr) {
        btrUndoCxt->Destroy();
        delete btrUndoCxt;
    }

    ErrLog(unlikely(isRecovery) ? DSTORE_LOG : DSTORE_DEBUG1, MODULE_UNDO,
           ErrMsg("xid(%d, %lu) rollback done, pdbId %u, tailUndoRecPtr(%hu, %u, %hu), startUndoRecPtr(%hu, %u, %hu).",
                  static_cast<int32>(xid.m_zoneId), xid.m_logicSlotId, m_pdbId, endUndoPtr.GetFileId(),
                  endUndoPtr.GetBlockNum(), endUndoPtr.GetOffset(), startUndoPtr.GetFileId(),
                  startUndoPtr.GetBlockNum(), startUndoPtr.GetOffset()));
    return status;
}

void UndoZone::GenerateWalForRollback(BufferDesc *bufferDesc, const UndoRecord &undoRec, WalType walType)
{
    AtomicWalWriterContext *walContext = thrd->m_walWriterContext;
    if (!undoRec.IsGlobalTempTableUndoRec()) {
        StorageAssert(walContext->HasAlreadyBegin());
        walContext->RememberPageNeedWal(bufferDesc);
        DataPage *page = static_cast<DataPage *>(bufferDesc->GetPage());
        bool glsnChangedFlag = (page->GetWalId() != walContext->GetWalId());
        WalRecordRollbackForData *redoData = static_cast<WalRecordRollbackForData *>(walContext->GetTempWalBuf());
        StorageReleasePanic(redoData == nullptr, MODULE_UNDO,
                            ErrMsg("Redo data is nullptr when generate wal for rollback."));
        uint32 size = static_cast<uint32>(sizeof(WalRecordRollbackForData)) + undoRec.GetUndoDataSize();
        redoData->SetHeader({walType, size, page->GetSelfPageId(), page->GetWalId(), page->GetPlsn(), page->GetGlsn(),
                             glsnChangedFlag, bufferDesc->GetFileVersion()});
        redoData->RecordUndoRecHdr(undoRec);
        if (undoRec.GetUndoDataSize() > 0) {
            WalRecordForPage::CopyData(redoData->m_data, size - sizeof(WalRecordRollbackForData),
                                       static_cast<char *>(undoRec.GetUndoData()), undoRec.GetUndoDataSize());
        }
        walContext->PutNewWalRecord(redoData);
    }
}

bool UndoZone::IsPageTypeMatchUndoRecordType(DataPage* page, const UndoRecord &undoRec)
{
    StorageReleasePanic(page == nullptr, MODULE_UNDO, ErrMsg("Invalid page. undoType:%u, undoRec:Ctid(%hu, %u, %u)",
        undoRec.GetUndoType(),
        undoRec.GetPageId().m_fileId, undoRec.GetPageId().m_blockId, undoRec.GetCtid().GetOffset()));
    if (undoRec.IsBtreeUndoRecord()) {
        if (unlikely(!page->TestType(PageType::INDEX_PAGE_TYPE) ||
                         !((BtrPage *)page)->GetLinkAndStatus()->TestType(BtrPageType::LEAF_PAGE))) {
            ErrLog(DSTORE_LOG, MODULE_UNDO,
                   ErrMsg("Rollback undo index page invalid, undoType:%u, undoRec:Ctid(%hu, %u, %u)"
                          BTR_PAGE_HEADER_FMT BTR_PAGE_LINK_AND_STATUS_FMT,
                          undoRec.GetUndoType(), undoRec.GetPageId().m_fileId, undoRec.GetPageId().m_blockId,
                          undoRec.GetCtid().GetOffset(),
                          BTR_PAGE_HEADER_VAL(page),
                          BTR_PAGE_LINK_AND_STATUS_VAL(((BtrPage *)page)->GetLinkAndStatus())));
            return false;
        }
    } else if (undoRec.IsHeapUndoRecord()) {
        if (unlikely(!page->TestType(PageType::HEAP_PAGE_TYPE))) {
            ErrLog(DSTORE_LOG, MODULE_UNDO,
                   ErrMsg("Rollback undo heap page invalid, undoType:%u, undoRec:Ctid(%hu, %u, %u)" PAGE_HEADER_FMT,
                          undoRec.GetUndoType(), undoRec.GetPageId().m_fileId, undoRec.GetPageId().m_blockId,
                          undoRec.GetCtid().GetOffset(), PAGE_HEADER_VAL(page)));
            return false;
        }
    } else {
        StorageReleasePanic(true, MODULE_UNDO, ErrMsg("Invalid undoType:%u, undoRec:Ctid(%hu, %u, %u),"
            PAGE_HEADER_FMT, undoRec.GetUndoType(), undoRec.GetPageId().m_fileId, undoRec.GetPageId().m_blockId,
            undoRec.GetCtid().GetOffset(), PAGE_HEADER_VAL(page)));
    }
    return true;
}
}  // namespace DSTORE
