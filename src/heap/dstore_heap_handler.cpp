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
 * dstore_heap_handler.cpp
 *
 * IDENTIFICATION
 *        src/heap/dstore_heap_handler.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
 
#include "heap/dstore_heap_handler.h"
#include "common/log/dstore_log.h"
#include "common/memory/dstore_mctx.h"
#include "common/dstore_datatype.h"
#include "common/dstore_common_utils.h"
#include "wal/dstore_wal_write_context.h"
#include "systable/dstore_systable_utility.h"
#include "page/dstore_td.h"
#include "transaction/dstore_transaction.h"
#include "heap/dstore_heap_undo_struct.h"
#include "heap/dstore_heap_wal_struct.h"
#include "errorcode/dstore_heap_error_code.h"
#include "common/algorithm/dstore_bitmapset.h"

namespace DSTORE {

PageId TableStorageMgr::GetNewPage()
{
    return m_segment->GetNewPage();
}

PageId TableStorageMgr::GetPageFromFSM(uint32 size, uint16 retryTime, uint32 *spaceInFsm)
{
    return m_segment->GetPageFromFsm(size, retryTime, spaceInFsm);
}

RetStatus TableStorageMgr::UpdateFSM(const PageId &pageId, uint32 size)
{
    return m_segment->UpdateFsm(pageId, size);
}

RetStatus TableStorageMgr::UpdateFSM(const FsmIndex &fsmIndex, uint32 size)
{
    return m_segment->UpdateFsm(fsmIndex, size);
}

RetStatus TableStorageMgr::UpdateFsmAndSearch(const FsmIndex &fsmIndex, uint32 remainSpace, uint32 spaceNeeded,
                                              uint16 retryTimes, PageId *pageId, uint32* spaceInfsm)
{
    return m_segment->UpdateFsmAndSearch(fsmIndex, remainSpace, spaceNeeded, retryTimes, pageId, spaceInfsm);
}

uint32 TableStorageMgr::GetSaveFreeSpace() const
{
    const double hundredPercent = 100.0;
    return static_cast<uint32>(BLCKSZ * (1 - m_fillFactor / hundredPercent));
}

TableStorageMgr::TableStorageMgr(PdbId pdbId, int fillFactor, TupleDesc tupDesc, RelationPersistence persistenceMethod)
    : m_segment(nullptr),
      m_tupDesc(nullptr),
      m_pdbId(pdbId),
      m_lastPageIdForInsert(INVALID_PAGE_ID),
      m_segMetaPageId(INVALID_PAGE_ID),
      m_persistenceMethod(persistenceMethod),
#ifdef DSTORE_USE_ASSERT_CHECKING
      m_threadPid(thrd->GetThreadId()),
#endif
      m_fillFactor(fillFactor),
      m_ctx(nullptr),
      m_tableOid(DSTORE_INVALID_OID)
{
    if (tupDesc != nullptr) {
        m_tupDesc = tupDesc->Copy();
    }
}

TableStorageMgr::~TableStorageMgr()
{
    if (m_segment != nullptr) {
        delete m_segment;
        m_segment = nullptr;
    }
    DstorePfreeExt(m_tupDesc);
    m_ctx = nullptr;
}

HeapSegment *TableStorageMgr::GetSegment()
{
    return m_segment;
}

TupleDesc TableStorageMgr::GetTupleDesc()
{
    return m_tupDesc;
}

PageId TableStorageMgr::GetSegMetaPageId()
{
    return m_segMetaPageId;
}

uint64 TableStorageMgr::GetTableBlockCount()
{
    return m_segment->GetDataBlockCount();
}

RelationPersistence TableStorageMgr::GetPersistenceMethod() const
{
    return m_persistenceMethod;
}

bool TableStorageMgr::IsGlobalTempTable() const
{
    return m_persistenceMethod == SYS_RELPERSISTENCE_GLOBAL_TEMP;
}

RetStatus TableStorageMgr::Init(const PageId &segmentId, TablespaceId tablespaceId, DstoreMemoryContext context)
{
    /*
     * Store memory for SMGR inside instance-level MEMORY_CONTEXT_SMGR memory group
     * SEG: This is workaround. Change this to be session level MEMORY_CONTEXT_SMGR after supported
     */
    m_ctx = context;
    if (!IsGlobalTempTable()) {
        m_segment = DstoreNew(m_ctx) HeapNormalSegment(m_pdbId, segmentId, tablespaceId,
            g_storageInstance->GetBufferMgr(), m_ctx);
    } else {
        m_segment = DstoreNew(m_ctx) HeapTempSegment(m_pdbId, segmentId, tablespaceId,
            thrd->GetTmpLocalBufMgr(), m_ctx);
    }
    if ((m_segment == nullptr || STORAGE_FUNC_FAIL(m_segment->InitSegment()))) {
        storage_set_error(HEAP_ERROR_FAIL_CREATE_TABLE_SMGR, segmentId.m_fileId, segmentId.m_blockId);
        ErrLog(DSTORE_ERROR, MODULE_HEAP,
               ErrMsg("Init segment failed, page id (%hu, %u).", segmentId.m_fileId, segmentId.m_blockId));
        return DSTORE_FAIL;
    }
    m_lastPageIdForInsert = INVALID_PAGE_ID;
    m_segMetaPageId = segmentId;

#ifdef DSTORE_USE_ASSERT_CHECKING
    m_threadPid = thrd->GetThreadId();
#endif

    return DSTORE_SUCC;
}

void TableStorageMgr::SetLastPageIdForInsert(const PageId &pageId)
{
#if defined(UT) || defined(DSTORE_TEST_TOOL)
    StorageAssert(m_threadPid == thrd->GetThreadId());
#endif
    m_lastPageIdForInsert = pageId;
}

PageId TableStorageMgr::GetLastPageIdForInsert()
{
#if defined(UT) || defined(TPDSTORE_TEST_TOOLCC)
    StorageAssert(m_threadPid == thrd->GetThreadId());
#endif
    return m_lastPageIdForInsert;
}

bool TableStorageMgr::NeedUpdateFsm(uint32 prevSize, uint32 curSize) const
{
    return m_segment->NeedUpdateFsm(prevSize, curSize);
}

void TableStorageMgr::SetTupleDesc(TupleDesc tupDesc)
{
    m_tupDesc = tupDesc;
}

void TableStorageMgr::SetTableOid(Oid tableOid)
{
    m_tableOid = tableOid;
}

Oid TableStorageMgr::GetTableOid() const
{
    return m_tableOid;
}

HeapHandler::HeapHandler(StorageInstance *instance, ThreadContext *thread, StorageRelation heapRel,
                         bool isLobOperation, bool isUseRingBuf /* = false */)
    : m_heapRel(heapRel),
      m_instance(instance),
      m_thrd(thread),
      m_bufferDesc(nullptr),
      m_useRingBuf(isUseRingBuf),
      m_ringBuf(nullptr),
      m_isLob(isLobOperation)
{
    bool needWal = true;
    if (m_heapRel != nullptr) {
        TableStorageMgr* selectedSmgr = (isLobOperation && heapRel->lobTableSmgr != nullptr) ?
                                        heapRel->lobTableSmgr :
                                        heapRel->tableSmgr;

        m_bufMgr = (selectedSmgr != nullptr && selectedSmgr->IsGlobalTempTable()) ?
                    m_thrd->GetTmpLocalBufMgr() :
                    m_instance->GetBufferMgr();
        needWal = !(selectedSmgr != nullptr && selectedSmgr->IsGlobalTempTable());
    }

    m_tdcontext.Init((heapRel != nullptr) ? heapRel->m_pdbId : INVALID_PDB_ID, needWal);
}

TableStorageMgr *HeapHandler::GetTableSmgr() const
{
    return m_isLob ? m_heapRel->lobTableSmgr : m_heapRel->tableSmgr;
}

HeapTuple *HeapHandler::AssembleTuples(HeapTuple **tupChunks, uint32 numTupChunks)
{
    UNUSE_PARAM AutoMemCxtSwitch autoMemCxtSwitch(m_thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_QUERY));
    uint32 i = 0, len = 0, bigDiskTupLen = 0;
    constexpr uint32 linkTupChunkHeaderSize = sizeof(HeapDiskTuple) + LINKED_TUP_CHUNK_EXTRA_HEADER_SIZE;
    /* Step 1: Calculate the diskTupSize of big tuple */
    for (i = 0; i < numTupChunks; i++) {
        StorageAssert(tupChunks[i]->GetDiskTupleSize() > linkTupChunkHeaderSize);
        /* The first tupChunk */
        if (unlikely(i == 0)) {
            bigDiskTupLen += ((tupChunks[0]->GetDiskTupleSize() - linkTupChunkHeaderSize) + sizeof(HeapDiskTuple));
        } else {
            bigDiskTupLen += tupChunks[i]->GetDiskTupleSize() - linkTupChunkHeaderSize;
        }
    }
    HeapTuple *bigTup = static_cast<HeapTuple *>(DstorePalloc0(sizeof(HeapTuple) + bigDiskTupLen));
    if (unlikely(bigTup == nullptr)) {
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("DstorePalloc0 fail size(%u) when assemble tuples.",
            static_cast<uint32>(sizeof(HeapTuple) + bigDiskTupLen)));
        return nullptr;
    }

    /* Step 2: Copy header of the first tuple. */
    bigTup->m_head = tupChunks[0]->m_head;
    bigTup->m_diskTuple = static_cast<HeapDiskTuple *>(
        static_cast<void *>((static_cast<char *>(static_cast<void *>(bigTup)) + sizeof(HeapTuple))));
    char *diskTuple = static_cast<char *>(static_cast<void *>(bigTup->m_diskTuple));
    errno_t ret = memcpy_s(diskTuple, bigDiskTupLen, tupChunks[0]->GetDiskTuple(), sizeof(HeapDiskTuple));
    storage_securec_check(ret, "\0", "\0");
    len += sizeof(HeapDiskTuple);

    /* Step 3: Copy disk tuple data. */
    for (i = 0; i < numTupChunks; i++) {
        uint32 dataLen = tupChunks[i]->GetDiskTupleSize() - linkTupChunkHeaderSize;
        ret = memcpy_s(diskTuple + len, bigDiskTupLen - len,
            static_cast<char *>(static_cast<void *>(tupChunks[i]->GetDiskTuple())) + linkTupChunkHeaderSize, dataLen);
        storage_securec_check(ret, "\0", "\0");
        len += dataLen;
    }
    StorageAssert(len == bigDiskTupLen);

    /* Step 4: Fill in bigTup information. */
    bigTup->SetDiskTupleSize(bigDiskTupLen);
    bigTup->GetDiskTuple()->SetNoLink();
    bigTup->GetDiskTuple()->SetTupleSize(0); /* Note: set to bigDiskTupLen might overflow. */
    return bigTup;
}

/* only when allocTd.isDirty is true, we need to record alloctd wal. If tdId is invalid, isDirty flag must be false */
void HeapHandler::GenerateAllocTdWal(BufferDesc *bufferDesc)
{
    if (unlikely(!NeedWal())) {
        m_tdcontext.allocTd.isDirty = false;
        return;
    }
    StorageAssert(m_thrd->GetActiveTransaction() != nullptr);
    AtomicWalWriterContext *walContext = m_thrd->m_walWriterContext;
    walContext->BeginAtomicWal(m_thrd->GetCurrentXid());
    walContext->RememberPageNeedWal(bufferDesc);
    StorageReleasePanic(!walContext->HasAlreadyBegin(), MODULE_HEAP, ErrMsg("Wal has not atomic begin."));

    /* record WalRecordHeapAllocTd */
    HeapPage *page = static_cast<HeapPage *>(bufferDesc->GetPage());
    bool glsnChangedFlag = (page->GetWalId() != walContext->GetWalId());
    uint64 fileVersion = bufferDesc->GetFileVersion();
    uint32 allocTdSize = WalRecordForDataPage::GetAllocTdSize(m_tdcontext);
    uint32 walDataSize = sizeof(WalRecordHeapAllocTd) + allocTdSize;

    WalRecordHeapAllocTd* walData = static_cast<WalRecordHeapAllocTd*>(DstorePalloc(walDataSize));
    while (STORAGE_VAR_NULL(walData)) {
        ErrLog(DSTORE_ERROR, MODULE_HEAP,
            ErrMsg("GenerateAllocTdWal alloc memory for size %u failed, retry it.", walDataSize));
        GaussUsleep(WAL_WAIT_MEMORY_AVAILABLE_TIME);
        walData = static_cast<WalRecordHeapAllocTd*>(DstorePalloc(walDataSize));
    }
    walData->SetHeader({WAL_HEAP_ALLOC_TD, walDataSize, bufferDesc->GetPageId(), page->GetWalId(), page->GetPlsn(),
        page->GetGlsn(), glsnChangedFlag, fileVersion});
    walData->SetAllocTd(m_tdcontext);

    walContext->PutNewWalRecord(walData);
    DstorePfree(walData);
    (void)walContext->EndAtomicWal();
}

UndoRecPtr HeapHandler::InsertUndoAndCheck(UndoRecord* undoRecord)
{
    AtomicWalWriterContext *walContext = m_thrd->m_walWriterContext;
    Transaction *transaction = m_thrd->GetActiveTransaction();
    UndoRecPtr undoRecPtr = transaction->InsertUndoRecord(undoRecord);
    /* if insert fail, we have done alloctd and modify page, so we need rollback page changes and record alloctd wal. */
    if (unlikely(undoRecPtr == INVALID_UNDO_RECORD_PTR)) {
        walContext->ResetForAbort();
        if (m_tdcontext.allocTd.isDirty) {
            (void)m_bufMgr->MarkDirty(m_bufferDesc);
            GenerateAllocTdWal(m_bufferDesc);
        }
        HandleErrorWhenGetUndoBuffer(m_bufferDesc, undoRecord);
        ErrLog(DSTORE_ERROR, MODULE_HEAP, ErrMsg("Insert undo record fail."));
    }
    return undoRecPtr;
}

HeapTuple* HeapHandler::FormIdentityTuple(HeapTuple *tuple, Bitmapset *replicaKeyAttrs)
{
    if (replicaKeyAttrs == nullptr || BmsIsEmpty(replicaKeyAttrs)) {
        return nullptr;
    }
    TupleDesc tupDesc = GetTableSmgr()->GetTupleDesc();
    Datum *values = static_cast<Datum *>(DstorePalloc0(MAX_HEAP_ATTR_NUMBER * sizeof(Datum)));
    bool nulls[MAX_HEAP_ATTR_NUMBER];
    errno_t rc = memset_s(nulls, sizeof(nulls), 0, sizeof(nulls));
    storage_securec_check(rc, "\0", "\0");
    tuple->DeformTuple(tupDesc, values, nulls);
    StorageReleasePanic(rc != 0, MODULE_LOGICAL_REPLICATION, ErrMsg("memset_s failed ret = %d", rc));
    for (int i = 0; i < tupDesc->natts; i++) {
        if (!BmsIsMember(i + 1 -
            static_cast<int>(HeapTupleSystemAttr::DSTORE_FIRST_LOW_INVALID_HEAP_ATTRIBUTE_NUMBER), replicaKeyAttrs)) {
            nulls[i] = true;
        }
    }
    HeapTuple *identityTuple = HeapTuple::FormTuple(tupDesc, values, nulls);
    if (tuple->HasOid() && identityTuple != nullptr) {
        identityTuple->GetDiskTuple()->SetHasOid();
        identityTuple->GetDiskTuple()->SetOid(tuple->GetOid());
    }
    DstorePfreeExt(values);
    return identityTuple;
}

void HeapHandler::TryToUnlockReleaseBufferDesc()
{
    if (m_bufferDesc != nullptr) {
        m_bufMgr->UnlockAndRelease(m_bufferDesc);
        m_bufferDesc = nullptr;
    }
}

/*
 * The caller must hold the exclusive lock on buffer and don't generate WAL when undo this modified page.
 * We must restore the upper of page for kinds of insertion in order to keep that page is same in primary
 * and standby node. By here, the heap page has been modified and an error happens when get undo buffer.
 * So we need to undo the modified heap buffer page, but it is different with transaction abort.
 */
void HeapHandler::HandleErrorWhenGetUndoBuffer(BufferDesc *bufDesc, UndoRecord *undoRec, void *undoDataRec)
{
    HeapPage *page = static_cast<HeapPage *>(bufDesc->GetPage());
    UndoType type = undoRec->GetUndoType();
    switch (type) {
        case UNDO_HEAP_INSERT:
        case UNDO_HEAP_INSERT_TMP:
        case UNDO_HEAP_SAME_PAGE_APPEND_UPDATE:
        case UNDO_HEAP_SAME_PAGE_APPEND_UPDATE_TMP:
        case UNDO_HEAP_ANOTHER_PAGE_APPEND_UPDATE_NEW_PAGE:
        case UNDO_HEAP_ANOTHER_PAGE_APPEND_UPDATE_NEW_PAGE_TMP: {
            OffsetNumber offset = undoRec->GetCtid().GetOffset();
            ItemId *itemId = page->GetItemIdPtr(offset);
            /*
             * For adding tuple, page->upper will minus tuple size. So we need fix upper of page. but we don't fix lower
             * of page due to replay wal algorithms.
             */
            page->SetUpper(page->GetUpper() + itemId->GetLen());
            break;
        }
        case UNDO_HEAP_BATCH_INSERT:
        case UNDO_HEAP_BATCH_INSERT_TMP: {
            UndoDataHeapBatchInsert *undoData = static_cast<UndoDataHeapBatchInsert *>(undoRec->GetUndoData());
            if (unlikely(undoData == nullptr)) {
                /* When undorecord append failed, we need get row data only from undodata. */
                undoData = static_cast<UndoDataHeapBatchInsert *>(undoDataRec);
                if (unlikely(undoData == nullptr)) {
                    ErrLog(DSTORE_PANIC, MODULE_HEAP, ErrMsg("undo heap failed because undo data is nullptr!"));
                }
            }
            OffsetNumber *offsetData = static_cast<OffsetNumber *>(undoData->GetRawData());
            uint16 rangeNum = undoData->GetItemIdRangeCount();
            for (uint16 i = 0; i < rangeNum; ++i) {
                OffsetNumber startOffset = offsetData[i * 2];
                OffsetNumber endOffset = offsetData[i * 2 + 1];
                for (OffsetNumber offset = startOffset; offset <= endOffset; ++offset) {
                    ItemId *itemId = page->GetItemIdPtr(offset);
                    /*
                     * For adding tuple, page->upper will minus tuple size. So we need fix upper of page. but we don't
                     * fix lower of page due to replay wal algorithms.
                     */
                    page->SetUpper(page->GetUpper() + itemId->GetLen());
                }
            }
            break;
        }
        default:
            break;
    }
    if (STORAGE_FUNC_FAIL(page->UndoHeap(undoRec, undoDataRec))) {
        ErrLog(DSTORE_PANIC, MODULE_HEAP, ErrMsg("undo heap failed"));
    }
    m_bufMgr->UnlockAndRelease(bufDesc);
    if (m_bufferDesc == bufDesc) {
        m_bufferDesc = NULL;
    }
}

} /* namespace DSTORE */
