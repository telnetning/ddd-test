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
 * dstore_transaction_mgr.cpp
 *
 *
 * IDENTIFICATION
 *        include/undo/dstore_undo_record.h
 *
 * ---------------------------------------------------------------------------------------
 */

#include "securec.h"
#include "fault_injection/fault_injection.h"
#include "common/log/dstore_log.h"
#include "common/error/dstore_error.h"
#include "common/instrument/trace/dstore_undo_trace.h"
#include "framework/dstore_thread.h"
#include "framework/dstore_instance.h"
#include "errorcode/dstore_undo_error_code.h"
#include "wal/dstore_wal_write_context.h"
#include "undo/dstore_undo_wal.h"
#include "undo/dstore_undo_mgr.h"

namespace DSTORE {

RetStatus UndoMgr::AllocateZoneId(ZoneId &retZid)
{
     /* set tablespace before call this method */
    StorageAssert(!IS_VALID_ZONE_ID(retZid));

    m_bmsLock->Acquire();
    retZid = BmsFirstMember(m_freeZids);
    m_bmsLock->Release();

    StorageReleasePanic(!IS_VALID_ZONE_ID(retZid), MODULE_UNDO, ErrMsg("Zone id invalid %d.", retZid));
    StorageAssert(!IsZoneIdFree(retZid));

    ErrLog(DSTORE_DEBUG1, MODULE_UNDO, ErrMsg("alloc undo zone, zoneId = %d", retZid));

    return DSTORE_SUCC;
}

RetStatus UndoMgr::ReleaseZoneId(ZoneId &zid)
{
    StorageAssert(IS_VALID_ZONE_ID(zid));
    if (!IS_VALID_ZONE_ID(zid)) {
        ErrLog(DSTORE_LOG, MODULE_UNDO, ErrMsg("release a invalid zoneId, zoneId = %d", zid));
        return DSTORE_SUCC;
    }

    if (IsZoneIdFree(zid)) {
        ErrLog(DSTORE_DEBUG1, MODULE_UNDO,
               ErrMsg("Try to release a free zone: zoneId = %d, pdbId = %d.", zid, m_pdbId));
        return DSTORE_FAIL;
    }

    m_bmsLock->Acquire();
    m_freeZids = BmsAddMember(m_freeZids, zid);
    m_bmsLock->Release();
    ErrLog(DSTORE_DEBUG1, MODULE_UNDO, ErrMsg("Try to release a zone: zoneId = %d, pdbId = %d", zid, m_pdbId));

    GS_WRITE_BARRIER();

    zid = INVALID_ZONE_ID;

    return DSTORE_SUCC;
}

UndoMgr::UndoMgr(BufMgrInterface *bufferMgr, PdbId pdbId)
    : m_pdbId(pdbId),
      m_txnInfoCache(nullptr),
      m_undoZones(nullptr),
      m_freeZids(nullptr),
      m_undoMemoryContext(nullptr),
      m_bmsLock(nullptr),
      m_bufferMgr(bufferMgr),
      m_mapSegment(nullptr)
{
    m_mapStartPage = INVALID_PAGE_ID;
    m_fullyInited.store(false, std::memory_order_relaxed);
    m_needStopRecover.store(false, std::memory_order_relaxed);
    errno_t rc = memset_s(m_zoneLocks, sizeof(m_zoneLocks), 0, sizeof(m_zoneLocks));
    storage_securec_check(rc, "\0", "\0");
    rc = memset_s(&m_mapLock, sizeof(m_mapLock), 0, sizeof(m_mapLock));
    storage_securec_check(rc, "\0", "\0");
}

UndoMgr::~UndoMgr() noexcept
{
    DstorePfreeExt(m_bmsLock);
    for (int i = 0; i < UNDO_ZONE_COUNT; ++i) {
        if (m_undoZones[i] != nullptr) {
            delete m_undoZones[i];
            m_undoZones[i] = nullptr;
        }
    }
    DstorePfreeExt(m_freeZids);
    DstorePfreeExt(m_undoZones);
    DstorePfreeExt(m_mapSegment);
    DstoreMemoryContextDelete(m_undoMemoryContext);
    m_undoMemoryContext = nullptr;
    m_bufferMgr = nullptr;
    m_txnInfoCache = nullptr;
}

void UndoMgr::DestroyAllUndoZone()
{
    for (int i = 0; i < UNDO_ZONE_COUNT; ++i) {
        if (m_undoZones[i] != nullptr) {
            delete m_undoZones[i];
            m_undoZones[i] = nullptr;
        }
    }
}

RetStatus UndoMgr::Init(DstoreMemoryContext parentContext)
{
    storage_trace_entry(TRACE_ID_UndoMgr__Init);
    RetStatus ret = DSTORE_SUCC;
    error_t rc;

    /* Step 1: construct memory context */
    m_undoMemoryContext =
        DstoreAllocSetContextCreate(parentContext, "Undo", ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE,
                                    ALLOCSET_UNDO_MAXSIZE, MemoryContextType::SHARED_CONTEXT);
    if (unlikely(!m_undoMemoryContext)) {
        ErrLog(DSTORE_ERROR, MODULE_UNDO, ErrMsg("[UndoMgr Init]Create undoMemoryContext failed. Out of memory"));
        storage_trace_exit(TRACE_ID_UndoMgr__Init);
        return DSTORE_FAIL;
    }
    AutoMemCxtSwitch autoMemCxtSwitch(m_undoMemoryContext);

    /* Step 2: init lock */
    LWLockInitialize(&m_mapLock, LWLOCK_GROUP_UNDO_MAP);
    m_bmsLock = static_cast<DstoreSpinLock *>(DstorePalloc(sizeof(DstoreSpinLock)));
    if (unlikely(!m_bmsLock)) {
        ErrLog(DSTORE_ERROR, MODULE_UNDO, ErrMsg("[UndoMgr Init]dstore_palloc m_bmsLock fail size(%u).",
            static_cast<uint32>(sizeof(DstoreSpinLock))));
        ret = DSTORE_FAIL;
        goto EXIT;
    }
    m_bmsLock->Init();

    /* Step 3: init bitmap */
    /*
     * Create three bitmaps for undozone with three kinds of tables(permanent, unlogged and temp).
     * Use -1 to initialize each bit of the bitmap as 1.
     */
    m_freeZids = nullptr;
    m_freeZids = BmsAddMember(m_freeZids, UNDO_ZONE_COUNT);
    if (unlikely(!m_freeZids)) {
        ErrLog(DSTORE_ERROR, MODULE_UNDO, ErrMsg("[UndoMgr Init]dstore_palloc m_freeZids failed."));
        ret = DSTORE_FAIL;
        goto EXIT;
    }
    rc = memset_s(m_freeZids->words, static_cast<uint32>(m_freeZids->nwords) * sizeof(bitmapword), 0xFF,
                          static_cast<uint32>(m_freeZids->nwords) * sizeof(bitmapword));
    storage_securec_check(rc, "\0");

    /* Step4: init txn cache */
    m_txnInfoCache = static_cast<AllUndoZoneTxnInfoCache *>(DstorePalloc(sizeof(AllUndoZoneTxnInfoCache)));
    if (unlikely(!m_txnInfoCache)) {
        ErrLog(DSTORE_ERROR, MODULE_UNDO, ErrMsg("[UndoMgr Init]dstore_palloc m_txnInfoCache fail size(%u).",
            static_cast<uint32>(sizeof(DstoreSpinLock))));
        ret = DSTORE_FAIL;
        goto EXIT;
    }
    m_txnInfoCache->InitTxnInfoCache();

    /* Step 5: init undo zone array and rwlock */
    m_undoZones = static_cast<UndoZone **>(DstorePalloc0(UNDO_ZONE_COUNT * sizeof(void *)));
    if (unlikely(!m_undoZones)) {
        ErrLog(DSTORE_ERROR, MODULE_UNDO, ErrMsg("[UndoMgr Init]dstore_palloc m_undoZones fail size(%u).",
            static_cast<uint32>(sizeof(DstoreSpinLock))));
        ret = DSTORE_FAIL;
        goto EXIT;
    }
    for (uint32 i = 0; i < MAX_THREAD_NUM; ++i) {
        (void)pthread_rwlock_init(&m_zoneLocks[i].lock, nullptr);
    }

EXIT:
    storage_trace_exit(TRACE_ID_UndoMgr__Init);
    ErrLog(DSTORE_DEBUG1, MODULE_UNDO, ErrMsg("Undo mgr init"));
    if (unlikely(ret == DSTORE_FAIL && m_undoMemoryContext)) {
        DstoreMemoryContextDelete(m_undoMemoryContext);
        m_undoMemoryContext = nullptr;
    }
    return ret;
}

bool UndoMgr::IsZoneOwned(UNUSE_PARAM ZoneId zid)
{
    return true;
}

bool UndoMgr::IsZoneOccupied(UNUSE_PARAM ZoneId zid)
{
    return true;
}

RetStatus UndoMgr::SwitchZone(UndoZone *&retZone, ZoneId &zid)
{
     /* set tablespace before call this method */
    storage_trace_entry(TRACE_ID_UndoMgr__SwitchZone);

    RetStatus status = DSTORE_SUCC;

    UndoZone *zone = nullptr;
    ZoneId retZid = INVALID_ZONE_ID;
    if (STORAGE_FUNC_FAIL(AllocateZoneId(retZid))) {
        goto ErrorExit;
    }

    status = GetUndoZone(retZid, &zone, true);
    if (STORAGE_FUNC_FAIL(status)) {
        (void)ReleaseZoneId(retZid);
        goto ErrorExit;
    }

    ErrLog(DSTORE_LOG, MODULE_UNDO, ErrMsg("Switch zone from zid(%d) to zid(%d)", zid, retZid));
    zid = retZid;
    retZone = zone;

Finish:
    storage_trace_exit(TRACE_ID_UndoMgr__SwitchZone);
    return status;

ErrorExit:
    status = DSTORE_FAIL;
    goto Finish;
}

PageId UndoMgr::GetUndoZoneSegmentId(ZoneId zoneId, bool needPanic)
{
    PageId pageId = INVALID_PAGE_ID;
    UndoZone *zone =
        reinterpret_cast<UndoZone *>(GsAtomicReadUintptr(reinterpret_cast<volatile uintptr_t *>(&m_undoZones[zoneId])));
    if (zone != nullptr) {
        pageId = zone->GetSegmentId();
        if (unlikely(pageId.IsInvalid())) {
            ErrLog(DSTORE_ERROR, MODULE_UNDO, ErrMsg("GetUndoZoneSegmentId get invalid segmentId, zoneId:%d, pdbId:%u",
                zoneId, m_pdbId));
        }
        return pageId;
    }

    ItemPointerData loc = GetZoneIdLocation(zoneId);
    BufferDesc *bufferDesc = m_bufferMgr->Read(m_pdbId, loc.GetPageId(), LW_SHARED);
    if (unlikely((bufferDesc) == INVALID_BUFFER_DESC)) {
        ErrLog(DSTORE_LOG, MODULE_UNDO,
            ErrMsg("Buffer(%hu, %u) invalid when GetUndoZoneSegmentId.",
                loc.GetPageId().m_fileId, loc.GetPageId().m_blockId));
        return INVALID_PAGE_ID;
    }
    char *rawPage = static_cast<char *>(static_cast<void *>(bufferDesc->GetPage()));
    if (unlikely((rawPage) == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_UNDO,
               ErrMsg("Rawpage(%hu, %u) invalid when GetUndoZoneSegmentId, zoneId(%d), pdbId[%u].",
                      loc.GetPageId().m_fileId, loc.GetPageId().m_blockId, zoneId, m_pdbId));
        m_bufferMgr->UnlockAndRelease(bufferDesc);
        return INVALID_PAGE_ID;
    }
    pageId = *static_cast<PageId *>(static_cast<void *>(rawPage + loc.GetOffset()));
    uint64 glsn = static_cast<Page *>(static_cast<void *>(rawPage))->GetGlsn();
    uint64 plsn = static_cast<Page *>(static_cast<void *>(rawPage))->GetPlsn();
    WalId walId = static_cast<Page *>(static_cast<void *>(rawPage))->GetWalId();

    /* The inited map data pages are zeroed. */
    if (unlikely(pageId.m_fileId == 0 && pageId.m_blockId == 0)) {
        ErrLog(DSTORE_LOG, MODULE_UNDO,
            ErrMsg("pageId is invalid. loc = ({%hu, %u}, %hu), "
                "page walId:%lu, page glsn:%lu, plsn:%lu, zoneid:%d, pdbid:%u.",
                loc.GetFileId(), loc.GetBlockNum(), loc.GetOffset(), walId, glsn, plsn, zoneId, m_pdbId));
        BufferTag bufTag = {m_pdbId, loc.GetPageId()};
        StorageReleaseBufferCheckPanic(needPanic, MODULE_UNDO, bufTag, "GetUndoZoneSegmentId panic.");
        m_bufferMgr->UnlockAndRelease(bufferDesc);
        return INVALID_PAGE_ID;
    }
    m_bufferMgr->UnlockAndRelease(bufferDesc);
    StorageReleasePanic(pageId.IsInvalid(), MODULE_UNDO,
        ErrMsg("GetUndoZoneSegmentId get invalid pageID (%hu, %u), zondId (%d).",
        pageId.m_fileId, pageId.m_blockId, zoneId));
    return pageId;
}

RetStatus UndoMgr::SetUndoZoneSegmentId(ZoneId zoneId, const PageId &segmentId)
{
    StorageAssert(segmentId != INVALID_PAGE_ID);

    ItemPointerData loc = GetZoneIdLocation(zoneId);

    BufferDesc *bufferDesc = m_bufferMgr->Read(m_pdbId, loc.GetPageId(), LW_EXCLUSIVE);
    if (unlikely((bufferDesc) == INVALID_BUFFER_DESC)) {
        ErrLog(DSTORE_ERROR, MODULE_UNDO,
            ErrMsg("Buffer(%hu, %u) invalid when SetUndoZoneSegmentId.",
                loc.GetPageId().m_fileId, loc.GetPageId().m_blockId));
        return DSTORE_FAIL;
    }
    char *rawPage = static_cast<char *>(static_cast<void *>(bufferDesc->GetPage()));

    PageId *pageId = static_cast<PageId *>(static_cast<void *>(rawPage + loc.GetOffset()));
    if (pageId != nullptr) {
        StorageAssert(pageId->m_fileId == 0);
    } else {
        ErrLog(DSTORE_PANIC, MODULE_UNDO, ErrMsg("pageId is nullptr"));
    }

    *pageId = segmentId;
    (void)m_bufferMgr->MarkDirty(bufferDesc);

    AtomicWalWriterContext *walContext = thrd->m_walWriterContext;
    walContext->BeginAtomicWal(INVALID_XID);
    walContext->RememberPageNeedWal(bufferDesc);
    uint32 size = static_cast<uint32>(sizeof(WalRecordUndoZoneSegmentId));
    WalRecordUndoZoneSegmentId redoData;
    Page *page = static_cast<Page *>(bufferDesc->GetPage());
    bool glsnChangedFlag = (page->GetWalId() != walContext->GetWalId());
    uint64 fileVersion = bufferDesc->GetFileVersion();
    redoData.SetHeader({WAL_UNDO_SET_ZONE_SEGMENT_ID, size, bufferDesc->GetPageId(), page->GetWalId(), page->GetPlsn(),
                        page->GetGlsn(), glsnChangedFlag, fileVersion});
    redoData.SetOffset(loc.GetOffset());
    redoData.SetSegmentId(segmentId);
    walContext->PutNewWalRecord(&redoData);
    (void)walContext->EndAtomicWal();

    m_bufferMgr->UnlockAndRelease(bufferDesc);

    ErrLog(DSTORE_DEBUG1, MODULE_UNDO,
           ErrMsg("Set undo zone segmentId: zoneId = %d, SegmentId = {%hu, %u}, "
                  "loc = {%hu, %u}, offset = %hu.",
                  zoneId, segmentId.m_fileId, segmentId.m_blockId, loc.GetPageId().m_fileId, loc.GetPageId().m_blockId,
                  loc.GetOffset()));
    return DSTORE_SUCC;
}

RetStatus UndoMgr::GetUndoZone(ZoneId zid, UndoZone **outUzone, bool canCreate)
{
    AutoMemCxtSwitch autoSwitch(m_undoMemoryContext);

    StorageReleasePanic(!IS_VALID_ZONE_ID(zid), MODULE_UNDO, ErrMsg("Get undo zone(%d) failed!", zid));

    StorageReleasePanic(m_undoZones == nullptr, MODULE_UNDO,
                        ErrMsg("Undo zones is null when get undo zone(%d), pdbId[%u]!", zid, m_pdbId));

    /* Step 1: if the zone has been inited, just return */
    UndoZone *uzone =
        reinterpret_cast<UndoZone *>(GsAtomicReadUintptr(reinterpret_cast<volatile uintptr_t *>(&m_undoZones[zid])));
    if (likely(uzone != nullptr)) {
        *outUzone = uzone;
        return DSTORE_SUCC;
    }

    /* Step 2: if the zone does not exist in memory, we need to make sure undo map segment loaded. */
    if (unlikely(!m_fullyInited.load(std::memory_order_acquire))) {
        *outUzone = nullptr;
        storage_set_error(UNDO_ERROR_NOT_FULLY_INITED);
        return DSTORE_FAIL;
    }

    /* Step 3: load or create the zone according to zid */
    PageId segmentId = GetUndoZoneSegmentId(zid);
    if (segmentId != INVALID_PAGE_ID) {
        if (STORAGE_FUNC_FAIL(LoadUndoZone(zid, segmentId))) {
            *outUzone = nullptr;
            ErrLog(DSTORE_ERROR, MODULE_UNDO, ErrMsg("Load zone(%d) failed!", zid));
            return DSTORE_FAIL;
        }
    } else {
        /* Only for AllocTransactionSlot and SwitchZone can we create a new zone */
        if (!canCreate) {
            *outUzone = nullptr;
            return DSTORE_FAIL;
        }
        if (STORAGE_FUNC_FAIL(CreateUndoZone(zid))) {
            *outUzone = nullptr;
            ErrLog(DSTORE_ERROR, MODULE_UNDO, ErrMsg("Create zone(%d) failed!", zid));
            return DSTORE_FAIL;
        }
    }

    *outUzone = m_undoZones[zid];
    StorageReleasePanic(*outUzone == nullptr, MODULE_UNDO,
                        ErrMsg("Out undo zone is null when get undo zone(%d), pdbId[%u]!", zid, m_pdbId));
    return DSTORE_SUCC;
}

RetStatus UndoMgr::GetUndoStatusForDiagnose(ZoneId zid, UndoZoneStatus *outStatus)
{
    AutoMemCxtSwitch autoSwitch(m_undoMemoryContext);
    if (unlikely(m_undoZones == nullptr)) {
        ErrLog(DSTORE_WARNING, MODULE_UNDO, ErrMsg("Undozones is nullptr!"));
        StorageAssert(false);
        return DSTORE_FAIL;
    }
    uint32 zidMapLockId = static_cast<uint32>(zid) % MAX_THREAD_NUM;
    (void)pthread_rwlock_rdlock(&m_zoneLocks[zidMapLockId].lock);
    UndoZone *uzone =
        reinterpret_cast<UndoZone *>(GsAtomicReadUintptr(reinterpret_cast<volatile uintptr_t *>(&m_undoZones[zid])));
    if (m_undoZones[zid] == nullptr || !IsZoneOwned(zid)) {
        (void)pthread_rwlock_unlock(&m_zoneLocks[zidMapLockId].lock);
        return DSTORE_FAIL;
    }
    PageId segmentId = uzone->GetSegmentId();
    if (unlikely(segmentId == INVALID_PAGE_ID)) {
        (void)pthread_rwlock_unlock(&m_zoneLocks[zidMapLockId].lock);
        ErrLog(DSTORE_WARNING, MODULE_UNDO, ErrMsg("SegmentId is invalid!"));
        return DSTORE_FAIL;
    }
    BufferDesc *undoMetaBufDesc = m_bufferMgr->Read(m_pdbId, segmentId, LW_SHARED);
    if (unlikely(undoMetaBufDesc == INVALID_BUFFER_DESC)) {
        (void)pthread_rwlock_unlock(&m_zoneLocks[zidMapLockId].lock);
        ErrLog(DSTORE_WARNING, MODULE_UNDO, ErrMsg("Buffer(%hu, %u) is invalid!", segmentId.m_fileId,
               segmentId.m_blockId));
        return DSTORE_FAIL;
    }
    SegmentMetaPage *page = static_cast<SegmentMetaPage *>(undoMetaBufDesc->GetPage());
    if (unlikely(page == nullptr)) {
        ErrLog(DSTORE_WARNING, MODULE_UNDO,
               ErrMsg("Meta page(%hu, %u) is invalid!", segmentId.m_fileId, segmentId.m_blockId));
        m_bufferMgr->UnlockAndRelease(undoMetaBufDesc);
        (void)pthread_rwlock_unlock(&m_zoneLocks[zidMapLockId].lock);
        return DSTORE_FAIL;
    }
    outStatus->pageNum = page->GetTotalBlockCount();
    outStatus->isAsyncRollbacking = uzone->IsAsyncRollbacking();
    outStatus->recycleLogicSlotId = uzone->GetRecycleLogicSlotId();
    outStatus->nextFreeLogicSlotId = uzone->GetNextFreeLogicSlotId();
    m_bufferMgr->UnlockAndRelease(undoMetaBufDesc);
    (void)pthread_rwlock_unlock(&m_zoneLocks[zidMapLockId].lock);
    return DSTORE_SUCC;
}

RetStatus UndoMgr::AllocateZoneMemory(ZoneId zid, Segment *segment)
{
    if (unlikely(segment == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_UNDO, ErrMsg("Segment is null when allocate zone(%d) memory, pdbId[%u].", zid,
               m_pdbId));
        return DSTORE_FAIL;
    }
    UndoZone *zone = DstoreNew(m_undoMemoryContext) UndoZone(segment, m_bufferMgr, zid, m_pdbId);
    if (zone == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_UNDO, ErrMsg("No memory for undo zone, zid(%d)", zid));
        return DSTORE_FAIL;
    }

    if (STORAGE_FUNC_FAIL(zone->Init(m_undoMemoryContext))) {
        delete zone;
        ErrLog(DSTORE_ERROR, MODULE_UNDO, ErrMsg("Zone(%d) init failed", zid));
        return DSTORE_FAIL;
    }

    GS_WRITE_BARRIER();
    GsAtomicWriteUintptr(reinterpret_cast<volatile uintptr_t *>(&m_undoZones[zid]), reinterpret_cast<uintptr_t>(zone));
    ErrLog(DSTORE_DEBUG1, MODULE_UNDO, ErrMsg("Allocate zone memory, zid %d.", zid));
    return DSTORE_SUCC;
}

RetStatus UndoMgr::CreateUndoZone(ZoneId zid)
{
    StorageAssert(IS_VALID_ZONE_ID(zid));

    UNUSE_PARAM AutoMemCxtSwitch amcs(m_undoMemoryContext);
    RetStatus status = DSTORE_SUCC;
    uint32 zidMapLockId = static_cast<uint32>(zid) % MAX_THREAD_NUM;
    PageId segmentId = INVALID_PAGE_ID;
    Segment *segment = nullptr;
    /* Step 1: acquire lock */
    FAULT_INJECTION_WAIT(DstoreUndoFI::GET_UNDO_ZONE);
    (void)pthread_rwlock_wrlock(&m_zoneLocks[zidMapLockId].lock);
    if (m_undoZones[zid] != nullptr) {
        goto Finish;
    }
    /* Step 2: create an undo segment for zone */
    StorageAssert(GetUndoZoneSegmentId(zid) == INVALID_PAGE_ID);
    segment = dynamic_cast<Segment *>(SegmentInterface::AllocUndoSegment(
        m_pdbId, static_cast<TablespaceId>(TBS_ID::UNDO_TABLE_SPACE_ID), SegmentType::UNDO_SEGMENT_TYPE, m_bufferMgr));
    if (segment == nullptr) {
        goto ErrorExit;
    }
    segmentId = segment->GetSegmentMetaPageId();
    status = SetUndoZoneSegmentId(zid, segmentId);
    if (status == DSTORE_FAIL) {
        goto ErrorExit;
    }
    StorageAssert(GetUndoZoneSegmentId(zid) == segment->GetSegmentMetaPageId());

    /* Step 3: allocate memory for zone */
    if (STORAGE_FUNC_FAIL(AllocateZoneMemory(zid, segment))) {
        goto ErrorExit;
    }

    /* Step 4: check the new zone */
    if (IsZoneIdFree(zid)) {
        ErrLog(DSTORE_PANIC, MODULE_UNDO, ErrMsg("Check zone failed! ZoneId(%d) is free", zid));
        goto ErrorExit;
    }
    ErrLog(DSTORE_DEBUG1, MODULE_UNDO,
           ErrMsg("Create a new zone(%d), segment id(%hu, %u)", zid, segmentId.m_fileId, segmentId.m_blockId));
Finish:
    (void)pthread_rwlock_unlock(&m_zoneLocks[zidMapLockId].lock);
    FAULT_INJECTION_NOTIFY(DstoreUndoFI::RECOVER_UNDO_ZONE);
    return status;

ErrorExit:
    delete m_undoZones[zid];
    m_undoZones[zid] = nullptr;
    delete segment;
    status = DSTORE_FAIL;
    goto Finish;
}

RetStatus UndoMgr::LoadUndoZone(const ZoneId &zid, const PageId &segmentId)
{
    StorageAssert(IS_VALID_ZONE_ID(zid));
    StorageAssert(segmentId != INVALID_PAGE_ID);

    /* Step 1: acquire lock */
    uint32 zidMapLockId = static_cast<uint32>(zid) % MAX_THREAD_NUM;
    FAULT_INJECTION_NOTIFY(DstoreUndoFI::GET_UNDO_ZONE);
    FAULT_INJECTION_WAIT(DstoreUndoFI::RECOVER_UNDO_ZONE);
    (void)pthread_rwlock_wrlock(&m_zoneLocks[zidMapLockId].lock);
    if (m_undoZones[zid] != nullptr) {
        (void)pthread_rwlock_unlock(&m_zoneLocks[zidMapLockId].lock);
        return DSTORE_SUCC;
    }
    /* Step 2: load undo segment for zone */
    Segment *segment = DstoreNew(m_undoMemoryContext)
        Segment(m_pdbId, segmentId, SegmentType::UNDO_SEGMENT_TYPE,
                static_cast<TablespaceId>(TBS_ID::UNDO_TABLE_SPACE_ID), g_storageInstance->GetBufferMgr());
    if (segment == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_UNDO,
               ErrMsg("No memory for undo segment(%hu, %u)", segmentId.m_fileId, segmentId.m_blockId));
        (void)pthread_rwlock_unlock(&m_zoneLocks[zidMapLockId].lock);
        return DSTORE_FAIL;
    }
    RetStatus status = segment->Init();
    if (STORAGE_FUNC_FAIL(status)) {
        delete segment;
        (void)pthread_rwlock_unlock(&m_zoneLocks[zidMapLockId].lock);
        ErrLog(DSTORE_ERROR, MODULE_UNDO,
               ErrMsg("Load undo segment(%hu, %u) failed!", segmentId.m_fileId, segmentId.m_blockId));
        return DSTORE_FAIL;
    }
    /* Step 3: allocate memory for zone */
    if (STORAGE_FUNC_FAIL(AllocateZoneMemory(zid, segment))) {
        delete segment;
        (void)pthread_rwlock_unlock(&m_zoneLocks[zidMapLockId].lock);
        ErrLog(DSTORE_ERROR, MODULE_UNDO, ErrMsg("Allocate zone memory fail, zid %d.", zid));
        return DSTORE_FAIL;
    }
    ErrLog(DSTORE_DEBUG1, MODULE_UNDO,
           ErrMsg("Load an old zone(%d), segment id(%hu, %u)", zid, segmentId.m_fileId, segmentId.m_blockId));
    (void)pthread_rwlock_unlock(&m_zoneLocks[zidMapLockId].lock);
    return DSTORE_SUCC;
}

/*
 * Traverse all undo zone to recycle pages.
 */
void UndoMgr::Recycle(CommitSeqNo recycleMinCsn)
{
    StorageAssert(m_fullyInited.load(std::memory_order_relaxed));
    UndoZone *undoZone = nullptr;
    bool needStopRecycle = false;
    StoragePdb* pdb = nullptr;
    for (ZoneId i = 0; i < UNDO_ZONE_COUNT; ++i) {
        pdb = g_storageInstance->GetPdb(m_pdbId);
        if (unlikely(pdb == nullptr)) {
            ErrLog(DSTORE_ERROR, MODULE_UNDO,
                   ErrMsg("Stop recycle undo zone because pdb is nulptr, recycle csn min = %lu, pdbId(%u).",
                          recycleMinCsn, m_pdbId));
            return;
        }
        needStopRecycle = pdb->IsNeedStopBgThread();
        if (needStopRecycle) {
            ErrLog(DSTORE_LOG, MODULE_UNDO,
                   ErrMsg("Stop recycle undo zone, recycle csn min = %lu, pdbId(%u).", recycleMinCsn, m_pdbId));
            return;
        }
        undoZone =
            reinterpret_cast<UndoZone *>(GsAtomicReadUintptr(reinterpret_cast<volatile uintptr_t *>(&m_undoZones[i])));
        if (undoZone != nullptr && undoZone->GetUndoRecyclePageId().IsValid()) {
            thrd->RefreshWorkingVersionNum();
            undoZone->Recycle(recycleMinCsn);
        }
    }
}

RetStatus UndoMgr::LoadUndoMapSegment()
{
    AutoMemCxtSwitch autoSwitch(m_undoMemoryContext);
    DstoreLWLockAcquire(&m_mapLock, LW_EXCLUSIVE);
    PageId undoMapSegmentId = INVALID_PAGE_ID;
    RetStatus ret =
        g_storageInstance->GetPdb(m_pdbId)->GetControlFile()->GetUndoZoneMapSegmentId(undoMapSegmentId);
    if (STORAGE_FUNC_FAIL(ret)) {
            ErrLog(DSTORE_ERROR, MODULE_UNDO, ErrMsg("Failed to get undo map segment id!"));
            LWLockRelease(&m_mapLock);
            return DSTORE_FAIL;
    }
    if (m_mapSegment == nullptr) {
        /* The inited page are zeroed. */
        m_mapSegment =
            DstoreNew(m_undoMemoryContext) Segment(m_pdbId, undoMapSegmentId, SegmentType::UNDO_SEGMENT_TYPE,
                                                   static_cast<TablespaceId>(TBS_ID::UNDO_TABLE_SPACE_ID), m_bufferMgr);
        if (unlikely(!m_mapSegment)) {
            ErrLog(DSTORE_ERROR, MODULE_UNDO, ErrMsg("New undo map segment failed, out of memeory!"));
            LWLockRelease(&m_mapLock);
            return DSTORE_FAIL;
        }
        ErrLog(DSTORE_LOG, MODULE_UNDO, ErrMsg("Init undoSegment Start"));
        ret = m_mapSegment->Init();
        if (STORAGE_FUNC_FAIL(ret)) {
            ErrLog(DSTORE_ERROR, MODULE_UNDO,
                   ErrMsg("Load UndoZone-SegmentId map segment(%hu, %u) "
                          "failed!",
                          undoMapSegmentId.m_fileId, undoMapSegmentId.m_blockId));
            DstorePfreeExt(m_mapSegment);
            LWLockRelease(&m_mapLock);
            return DSTORE_FAIL;
        }
        ErrLog(DSTORE_LOG, MODULE_UNDO, ErrMsg("Init undoSegment End"));
        m_mapStartPage = m_mapSegment->GetLastExtentPageId();

        ErrLog(DSTORE_DEBUG1, MODULE_UNDO,
               ErrMsg("Restore UndoZone-SegmentId map from meta segment id(%hu, %u) saved in control file",
                      undoMapSegmentId.m_fileId, undoMapSegmentId.m_blockId));

        /* Skip the extent meta page, store map data on the left pages. */
        m_mapStartPage.m_blockId++;
        m_fullyInited.store(true, std::memory_order_release);
    }
    LWLockRelease(&m_mapLock);
    return DSTORE_SUCC;
}

void UndoMgr::CreateUndoMapSegment()
{
    AutoMemCxtSwitch autoSwitch(m_undoMemoryContext);
    StorageAssert(m_mapSegment == nullptr);
    m_mapSegment = dynamic_cast<Segment *>(SegmentInterface::AllocUndoSegment(
        m_pdbId, static_cast<TablespaceId>(TBS_ID::UNDO_TABLE_SPACE_ID), SegmentType::UNDO_SEGMENT_TYPE, m_bufferMgr));
    StorageReleasePanic(!SegmentIsValid(m_mapSegment), MODULE_UNDO, ErrMsg("Allocate UndoZone-SegmentId failed!"));
    /*
     * Only two extent in this segment, segment meta extent and segment data extent.
     * Allocate 64 MB for the convenience of calculating offset while storing map data.
     */
    RetStatus status = m_mapSegment->Extend(SEGMENT_ID_EXTENT_SIZE, &m_mapStartPage);
    if (STORAGE_FUNC_FAIL(status)) {
        ErrLog(DSTORE_PANIC, MODULE_UNDO,
            ErrMsg("Extend UndoZone-SegmentId map failed!"));
    }
    PageId cursor = m_mapStartPage;
    BufferDesc *bufDesc;
    Page *page;
    for (uint32 i = 0; i < SEGMENT_ID_PAGES; i++) {
        cursor.m_blockId++;
        bufDesc = m_bufferMgr->Read(m_pdbId, cursor, LW_EXCLUSIVE);
        STORAGE_CHECK_BUFFER_PANIC(bufDesc, MODULE_UNDO, cursor);
        page = bufDesc->GetPage();
        page->Init(0, PageType::UNDO_PAGE_TYPE, cursor);
        (void)m_bufferMgr->MarkDirty(bufDesc);

        AtomicWalWriterContext *walContext = thrd->m_walWriterContext;
        walContext->BeginAtomicWal(INVALID_XID);
        walContext->RememberPageNeedWal(bufDesc);
        uint32 size = static_cast<uint32>(sizeof(WalRecordUndoZidSegmentInit));
        WalRecordUndoZidSegmentInit redoData;
        bool glsnChangedFlag = (page->GetWalId() != walContext->GetWalId());
        redoData.SetHeader({WAL_UNDO_INIT_MAP_SEGMENT, size, bufDesc->GetPageId(), page->GetWalId(), page->GetPlsn(),
                            page->GetGlsn(), glsnChangedFlag, bufDesc->GetFileVersion()});
        walContext->PutNewWalRecord(&redoData);
        (void)walContext->EndAtomicWal();

        m_bufferMgr->UnlockAndRelease(bufDesc);
    }
    PageId segmentId = m_mapSegment->GetSegmentMetaPageId();
    status = g_storageInstance->GetPdb(m_pdbId)->GetControlFile()->SetUndoZoneMapSegmentId(segmentId);
    if (STORAGE_FUNC_FAIL(status)) {
        ErrLog(DSTORE_PANIC, MODULE_UNDO,
               ErrMsg("Set undo map segmentId(%hu, %u) failed!", segmentId.m_fileId, segmentId.m_blockId));
    }

    /* Skip the extent meta page, store map data on the left pages. */
    m_mapStartPage.m_blockId++;
    m_fullyInited.store(true, std::memory_order_relaxed);
    ErrLog(DSTORE_DEBUG1, MODULE_UNDO,
           ErrMsg("Create meta segment id(%hu, %u) to store UndoZone-"
                  "SegmentId map and write segment id to control file",
                  segmentId.m_fileId, segmentId.m_blockId));
}

void UndoMgr::RecoverUndoZone()
{
    ErrLog(DSTORE_DEBUG1, MODULE_UNDO, ErrMsg("Recover undo zone."));

    if (STORAGE_FUNC_FAIL(LoadUndoMapSegment())) {
        ErrLog(DSTORE_ERROR, MODULE_UNDO, ErrMsg("LoadUndoMapSegment failed!"));
        return;
    }

    PageId cursor = GetZidToSegmentMapStartPage();
    BufferDesc *bufDesc = nullptr;
    char *rawPage = static_cast<char *>(DstorePalloc0(BLCKSZ));
    if (unlikely(rawPage == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_UNDO, ErrMsg("Stop recover undo zone because of alloc fail."));
        return;
    }
    errno_t rc = EOK;
    for (uint32 i = 0; i < SEGMENT_ID_PAGES; ++i) {
        bufDesc = m_bufferMgr->Read(m_pdbId, cursor, LW_SHARED);
        if (unlikely(bufDesc == INVALID_BUFFER_DESC)) {
            ErrLog(DSTORE_ERROR, MODULE_UNDO,
                   ErrMsg("Stop recover undo zone because of buffer(%hu, %u) invalid, pdbId[%u].", cursor.m_fileId,
                   cursor.m_blockId, m_pdbId));
            return;
        }
        rc = memcpy_s(rawPage, BLCKSZ, bufDesc->GetPage(), BLCKSZ);
        storage_securec_check(rc, "\0", "\0");
        m_bufferMgr->UnlockAndRelease(bufDesc);
        for (uint32 j = 0; j < SEGMENT_ID_PER_PAGE; ++j) {
            thrd->RefreshWorkingVersionNum();
            if (m_needStopRecover.load(std::memory_order_acquire)) {
                DstorePfreeExt(rawPage);
                ErrLog(DSTORE_LOG, MODULE_UNDO, ErrMsg("Stop recover undo zone."));
                return;
            }
            OffsetNumber offset = static_cast<OffsetNumber>(j * sizeof(PageId) + sizeof(Page));
            PageId pageId = *static_cast<PageId *>(static_cast<void *>(rawPage + offset));
            if ((pageId.m_fileId == 0 && pageId.m_blockId == 0) || !pageId.IsValid()) {
                /* we find one invalid page id, and after it all the page is invalid */
                continue;
            }
            ZoneId zid = static_cast<ZoneId>(i * SEGMENT_ID_PER_PAGE + j);
            if (!IS_VALID_ZONE_ID(zid)) {
                continue;
            }
            if (reinterpret_cast<UndoZone *>(
                    GsAtomicReadUintptr(reinterpret_cast<volatile uintptr_t *>(&m_undoZones[zid]))) != nullptr) {
                continue;
            }
            if (STORAGE_FUNC_FAIL(LoadUndoZone(zid, pageId))) {
                ErrLog(DSTORE_ERROR, MODULE_UNDO, ErrMsg("Load undo zone(%d) failed!", zid));
            }
        }
        cursor.m_blockId++;
    }
    DstorePfreeExt(rawPage);
}

void UndoMgr::GetLocalCurrentAllActiveTrxXid(GetCurrentActiveXidsCallback callBack, void *arg)
{
    for (ZoneId zid = 0; zid < UNDO_ZONE_COUNT; zid++) {
        StorageAssert(IS_VALID_ZONE_ID(zid));
        uint32 zidMapLockId = static_cast<uint32>(zid) % MAX_THREAD_NUM;
        (void)pthread_rwlock_rdlock(&m_zoneLocks[zidMapLockId].lock);
        /*
         * if undozone[zid] is null, it must not be used by any active transaction.
         * So we don't need to distinguish whether it's really invalid or hasn't been loaded into memory yet.
         */
        if (m_undoZones[zid] == nullptr || !IsZoneOwned(zid)) {
            (void)pthread_rwlock_unlock(&m_zoneLocks[zidMapLockId].lock);
            continue;
        }
        Xid curActiveXid = m_undoZones[zid]->GetLastActiveXid();
        (void)pthread_rwlock_unlock(&m_zoneLocks[zidMapLockId].lock);
        if (curActiveXid != INVALID_XID) {
            callBack(curActiveXid, arg);
        }
    }
}

void UndoMgr::StopRecoverUndoZone()
{
    m_needStopRecover.store(true, std::memory_order_release);
    ErrLog(DSTORE_LOG, MODULE_UNDO, ErrMsg("Set need stop recover thread success, pdbId(%u)!", m_pdbId));
}

void UndoMgr::ResetStopRecoverUndoZone()
{
    m_needStopRecover.store(false, std::memory_order_release);
    ErrLog(DSTORE_LOG, MODULE_UNDO, ErrMsg("Reset need stop recover thread success, pdbId(%u)!", m_pdbId));
}

}  // namespace DSTORE
