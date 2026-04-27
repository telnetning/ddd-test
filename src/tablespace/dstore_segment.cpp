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
 * dstore_segment.cpp
 *     This file defines the building blocks that bg page writer needs.
 *
 * IDENTIFICATION
 *        src/tablespace/dstore_segment.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "tablespace/dstore_segment.h"
#include "common/log/dstore_log.h"
#include "framework/dstore_instance.h"
#include "lock/dstore_lock_datatype.h"
#include "lock/dstore_table_lock_mgr.h"
#include "wal/dstore_wal_write_context.h"
#include "tablespace/dstore_tablespace_wal.h"
#include "page/dstore_heap_segment_meta_page.h"
#include "page/dstore_undo_segment_meta_page.h"
#include "page/dstore_data_segment_meta_page.h"
#include "page/dstore_heap_page.h"
#include "lock/dstore_lock_interface.h"

namespace DSTORE {

RetStatus SegmentInterface::AllocExtent(
    PdbId pdbId, TablespaceId tablespaceId, ExtentSize extentSize, PageId *newExtentPageId, bool *isReUseFlag)
{
    TablespaceMgr *tablespaceMgr = g_storageInstance->GetTablespaceMgrByPdbId(pdbId);
    if (STORAGE_VAR_NULL(tablespaceMgr)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to get tablespaceMgr when allocing extent, pdbId %u.", pdbId));
        return DSTORE_FAIL;
    }

    while (true) {
        TableSpace *tablespace = tablespaceMgr->OpenTablespace(tablespaceId, DSTORE::DSTORE_ACCESS_SHARE_LOCK);
        if (STORAGE_VAR_NULL(tablespace)) {
            ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("Failed to open tablespace %u.", tablespaceId));
            return DSTORE_FAIL;
        }
        if (tablespace->GetTbsCtrlItem().used != 1) {
            ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
                   ErrMsg("Failed to open tablespace %hu due to it is unused.", tablespaceId));
            tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_ACCESS_SHARE_LOCK);
            storage_set_error(TBS_ERROR_TABLESPACE_NOT_EXIST);
            return DSTORE_FAIL;
        }

        bool continueTryAlloc = true;
        RetStatus ret = tablespace->AllocExtent(extentSize, newExtentPageId, isReUseFlag, &continueTryAlloc);
        if (ret == DSTORE_SUCC) {
            tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_ACCESS_SHARE_LOCK);
            return DSTORE_SUCC;
        }
        if (!continueTryAlloc) {
            tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_ACCESS_SHARE_LOCK);
            return DSTORE_FAIL;
        }
        uint16 hwm = tablespace->GetExtentContextBySize(extentSize).GetHwm();
        tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_ACCESS_SHARE_LOCK);

        LockInterface::TablespaceLockContext context;
        context.pdbId = pdbId;
        context.tablespaceId = LOCK_TAG_TABLESPACE_MGR_ID;
        context.dontWait = false;
        context.mode = DSTORE::DSTORE_ACCESS_SHARE_LOCK;
        if (STORAGE_FUNC_FAIL(LockInterface::LockTablespace(&context))) {
            ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("Lock tablespaceMgr failed."));
            return DSTORE_FAIL;
        }
        /* After the background thread ObjSpaceMgr fails to add an exclusive lock to a tablespace,
         * it exits without waiting. */
        tablespace = tablespaceMgr->OpenTablespace(tablespaceId, DSTORE::DSTORE_ACCESS_EXCLUSIVE_LOCK,
                                                   (thrd && thrd->m_isObjSpaceThrd));
        if (STORAGE_VAR_NULL(tablespace)) {
            ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("Failed to open tablespace %u.", tablespaceId));
            LockInterface::UnlockTablespace(&context);
            return DSTORE_FAIL;
        }
        uint16 newHwm = tablespace->GetExtentContextBySize(extentSize).GetHwm();
        if (newHwm > hwm) {
            ErrLog(DSTORE_INFO, MODULE_TABLESPACE, ErrMsg("Hwm has changed, datafile has been added."));
            tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_ACCESS_EXCLUSIVE_LOCK);
            LockInterface::UnlockTablespace(&context);
            continue;
        }
        
        uint64 tablespaceSize = 0;
        if (STORAGE_FUNC_FAIL(tablespaceMgr->GetTablespaceSize(tablespaceId, tablespaceSize))) {
            ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("Failed to get tablespace(%u) size.", tablespaceId));
            tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_ACCESS_EXCLUSIVE_LOCK);
            LockInterface::UnlockTablespace(&context);
            return DSTORE_FAIL;
        }
        uint64 needSize = IsTemplate(pdbId) ? TEMPLATE_PDB_TBS_INIT_FILE_SIZE : INIT_FILE_SIZE;
        if (tablespaceSize + needSize > tablespace->GetTbsCtrlItem().tbsMaxSize) {
            ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
                   ErrMsg("Tabelspace(%u) add datafile failed. The sum of the current tablespaceSize(%lu) and the "
                          "needSize(%lu) exceeds tablespaceMaxsize(%lu).",
                          tablespaceId, tablespaceSize, needSize,
                          tablespace->GetTbsCtrlItem().tbsMaxSize));
            tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_ACCESS_EXCLUSIVE_LOCK);
            LockInterface::UnlockTablespace(&context);
            storage_set_error(TBS_ERROR_TABLESPACE_USE_UP);
            return DSTORE_FAIL;
        }
        FileId fileId;
        bool needWal = tablespace->IsTempTbs() ? false : true;
        ret = tablespace->AllocAndAddDataFile(pdbId, &fileId, extentSize, needWal);
        if (ret != DSTORE_SUCC) {
            tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_ACCESS_EXCLUSIVE_LOCK);
            LockInterface::UnlockTablespace(&context);
            return DSTORE_FAIL;
        }
        tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_ACCESS_EXCLUSIVE_LOCK);
        LockInterface::UnlockTablespace(&context);
    }
    return DSTORE_FAIL;
}

SegmentInterface *SegmentInterface::AllocUndoSegment(PdbId pdbId, TablespaceId tablespaceId, SegmentType type,
                                                 BufMgrInterface *bufMgr)
{
    if (unlikely(bufMgr == nullptr || type != SegmentType::UNDO_SEGMENT_TYPE)) {
        storage_set_error(TBS_ERROR_SEGMENT_PARAMETER_INVALID);
        return INVALID_SEGMENT;
    }
    PageId extentMetaPageId;

    ExtentSize firstSize = EXT_SIZE_128;

    /* Alloc extent from tablespace */
    bool isReuseFlag = false;
    if (STORAGE_FUNC_FAIL(AllocExtent(pdbId, tablespaceId, firstSize, &extentMetaPageId, &isReuseFlag))) {
        ErrLog(DSTORE_WARNING, MODULE_SEGMENT,
               ErrMsg("Alloc new segment of type %d fail, tablespace has no space", static_cast<uint8>(type)));
        return INVALID_SEGMENT;
    }

    /* Init segment meta page */
    BufferDesc *segMetaPageBuf = bufMgr->Read(pdbId, extentMetaPageId, LW_EXCLUSIVE,
        BufferPoolReadFlag::CreateNewPage());
    if (unlikely(segMetaPageBuf == INVALID_BUFFER_DESC)) {
        return INVALID_SEGMENT;
    }
    UndoSegmentMetaPage *metaPage = static_cast<UndoSegmentMetaPage *>(segMetaPageBuf->GetPage());
    StoragePdb *storagePdb = g_storageInstance->GetPdb(pdbId);
    if (storagePdb == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_PDBREPLICA, ErrMsg("fail to get pdb %hhu", pdbId));
        bufMgr->UnlockAndRelease(segMetaPageBuf);
        return INVALID_SEGMENT;
    }
    WalStreamManager *walStreamMgr = storagePdb->GetWalMgr()->GetWalStreamManager();
    WalId *walStreamIds = nullptr;
    uint32 walStreamCnt = walStreamMgr->GetOwnedStreamIds(&walStreamIds);
    if (walStreamCnt == 0) {
        ErrLog(DSTORE_ERROR, MODULE_PDBREPLICA, ErrMsg("pdb %s has 0 live wal stream", storagePdb->GetPdbName()));
        bufMgr->UnlockAndRelease(segMetaPageBuf);
        return INVALID_SEGMENT;
    }
    uint64 plsn = walStreamMgr->GetWalStream(*walStreamIds)->GetMaxAppendedPlsn();
    uint64 glsn = metaPage->GetGlsn();
    metaPage->InitUndoSegmentMetaPage(extentMetaPageId, plsn, glsn);
    /* Write wal of segment head page */
    (void)bufMgr->MarkDirty(segMetaPageBuf);
    AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
    /* Write wal of segment head page */
    bool glsnChangedFlag = (metaPage->GetWalId() != walWriterContext->GetWalId());
    walWriterContext->BeginAtomicWal(thrd->GetCurrentXid());
    WalRecordTbsInitUndoSegment walData;
    walData.SetHeader({WAL_TBS_INIT_UNDO_SEGMENT_META, sizeof(walData), extentMetaPageId, metaPage->GetWalId(),
        metaPage->GetPlsn(), metaPage->GetGlsn(), glsnChangedFlag, segMetaPageBuf->GetFileVersion()});
    walData.SetData(type, plsn, glsn);
    walWriterContext->RememberPageNeedWal(segMetaPageBuf);
    walWriterContext->PutNewWalRecord(&walData);

    bufMgr->UnlockAndRelease(segMetaPageBuf);
    (void)walWriterContext->EndAtomicWal();

    Segment *segment = DstoreNew(g_dstoreCurrentMemoryContext)
        Segment(pdbId, extentMetaPageId, type, tablespaceId, bufMgr);
    if (segment != nullptr) {
        /* Segment init failed */
        if (STORAGE_FUNC_FAIL(segment->Init())) {
            delete segment;
            return INVALID_SEGMENT;
        }
    } else {
        return INVALID_SEGMENT;
    }

    ErrLog(DSTORE_LOG, MODULE_SEGMENT,
           ErrMsg("Alloc new undo segment success, Segment Id is (%d, %u). pdb %u tablespace %hu",
               extentMetaPageId.m_fileId, extentMetaPageId.m_blockId, pdbId, tablespaceId));
    return segment;
}

Segment::Segment(PdbId pdbId, const PageId &segmentId, SegmentType type, TablespaceId tablespaceId,
    BufMgrInterface *bufMgr)
    : m_segmentId(segmentId),
      m_bufMgr(bufMgr),
      m_type(type),
      m_pdbId(pdbId)
{
    m_tablespaceId = tablespaceId;
}

RetStatus Segment::Init()
{
    if (unlikely(m_isDrop)) {
        storage_set_error(TBS_ERROR_SEGMENT_USE_AFTER_DROP);
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("Pdb %u tablespace %hu segment(%hu, %u) use after drop",
            m_pdbId, m_tablespaceId, m_segmentId.m_fileId, m_segmentId.m_blockId));
        return DSTORE_FAIL;
    }
    if (unlikely(IsInitialized())) {
        ErrLog(DSTORE_LOG, MODULE_SEGMENT, ErrMsg("Pdb %u tablespace %hu segment(%hu, %u) already initialized.",
            m_pdbId, m_tablespaceId, m_segmentId.m_fileId, m_segmentId.m_blockId));
        return DSTORE_SUCC;
    }
    if (unlikely(m_tablespaceId == 0 || m_bufMgr == nullptr || m_segmentId == INVALID_PAGE_ID)) {
        storage_set_error(TBS_ERROR_SEGMENT_PARAMETER_INVALID);
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("Pdb %u tablespace %hu segment(%hu, %u) init parametter invalid",
                m_pdbId, m_tablespaceId, m_segmentId.m_fileId, m_segmentId.m_blockId));
        return DSTORE_FAIL;
    }

    /* Read segment head page and verify segment info */
    BufferDesc *segMetaPageBuf = ReadSegMetaPageBuf(LW_SHARED);
    SegmentMetaPage *segmentMetaPage = GetSegMetaPage(segMetaPageBuf);
    if (unlikely(segmentMetaPage->segmentHeader.segmentType != m_type)) {
        UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);
        storage_set_error(TBS_ERROR_SEGMENT_PARAMETER_INVALID);
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT,  ErrMsg("Pdb %u tablespace %hu segment(%hu, %u) Init type invalid, "
            "segmentheader info: magic=0x%lX, segmentType=%d, totalBlockCount=%lu, "
            "extents.count=%lu, extents.first.m_fileId=%hu, extents.first.m_blockId=%u, "
            "extents.last.m_fileId=%hu, extents.last.m_blockId=%u, "
            "plsn=%lu, glsn=%lu; m_type=%d",
            m_pdbId, m_tablespaceId, m_segmentId.m_fileId, m_segmentId.m_blockId,
            segmentMetaPage->extentMeta.magic, static_cast<uint8_t>(segmentMetaPage->segmentHeader.segmentType),
            segmentMetaPage->segmentHeader.totalBlockCount, segmentMetaPage->segmentHeader.extents.count,
            segmentMetaPage->segmentHeader.extents.first.m_fileId,
            segmentMetaPage->segmentHeader.extents.first.m_blockId,
            segmentMetaPage->segmentHeader.extents.last.m_fileId,
            segmentMetaPage->segmentHeader.extents.last.m_blockId,
            segmentMetaPage->segmentHeader.plsn,
            segmentMetaPage->segmentHeader.glsn, static_cast<uint8_t>(m_type)));
        return DSTORE_FAIL;
    }
    UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);

    m_isInitialized = true;
    return DSTORE_SUCC;
}

Segment::~Segment()
{
    if (m_bufMgr != nullptr) {
        m_bufMgr = nullptr;
    }
}

RetStatus Segment::Extend(ExtentSize extSize, PageId *extMetaPageId)
{
    if (unlikely(extMetaPageId == nullptr)) {
        storage_set_error(TBS_ERROR_PARAMETER_INVALID);
        return DSTORE_FAIL;
    }
    *extMetaPageId = INVALID_PAGE_ID;
    if (unlikely(!IsInitialized())) {
        storage_set_error(TBS_ERROR_SEGMENT_IS_NOT_INIT);
        return DSTORE_FAIL;
    }
    if (unlikely(m_isDrop)) {
        storage_set_error(TBS_ERROR_SEGMENT_USE_AFTER_DROP);
        return DSTORE_FAIL;
    }

    BufferDesc *segMetaPageBuf = ReadSegMetaPageBuf(LW_SHARED);
    uint64 curExtentCount = GetSegMetaPage(segMetaPageBuf)->GetExtentCount();
    UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);
    if (unlikely(curExtentCount == MAX_EXTENT_COUNT)) {
        storage_set_error(TBS_ERROR_SEGMENT_EXTENT_COUNT_REACH_MAX);
        return DSTORE_FAIL;
    }

    LockTag tag;
    tag.SetTableExtensionLockTag(this->GetPdbId(), GetSegmentMetaPageId());
    LockErrorInfo errorInfo = {0};
    LockMgr *lockMgr = g_storageInstance->GetLockMgr();
    if (STORAGE_FUNC_FAIL(lockMgr->Lock(&tag, DSTORE_EXCLUSIVE_LOCK, false, &errorInfo))) {
        return DSTORE_FAIL;
    }

    if (STORAGE_FUNC_FAIL(ExtendSegmentInternal(extSize, extMetaPageId))) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
               ErrMsg("Segment (%d, %u) alloc extent size %d fail", GetSegmentMetaPageId().m_fileId,
                      GetSegmentMetaPageId().m_blockId, static_cast<uint16>(extSize)));
        lockMgr->Unlock(&tag, DSTORE_EXCLUSIVE_LOCK);
        return DSTORE_FAIL;
    }

    ErrLog(DSTORE_DEBUG1, MODULE_SEGMENT,
           ErrMsg("Segment (%d, %u) alloc extent size %d success", GetSegmentMetaPageId().m_fileId,
                  GetSegmentMetaPageId().m_blockId, static_cast<uint16>(extSize)));
    lockMgr->Unlock(&tag, DSTORE_EXCLUSIVE_LOCK);
    return DSTORE_SUCC;
}

PageId Segment::GetSegmentMetaPageId()
{
    if (unlikely(m_isDrop)) {
        storage_set_error(TBS_ERROR_SEGMENT_USE_AFTER_DROP);
        return INVALID_PAGE_ID;
    }
    return this->m_segmentId;
}

RetStatus Segment::DropSegment()
{
    if (unlikely(!IsInitialized())) {
        storage_set_error(TBS_ERROR_SEGMENT_IS_NOT_INIT);
        return DSTORE_FAIL;
    }
    LockTag tag;
    LockErrorInfo errorInfo = {0};
    TableLockMgr *lockMgr = g_storageInstance->GetTableLockMgr();
    tag.SetTableExtensionLockTag(this->GetPdbId(), GetSegmentMetaPageId());
    if (STORAGE_FUNC_FAIL(lockMgr->Lock(&tag, DSTORE_ACCESS_EXCLUSIVE_LOCK, false, &errorInfo))) {
        return DSTORE_FAIL;
    }
    if (unlikely(m_isDrop)) {
        lockMgr->Unlock(&tag, DSTORE_ACCESS_EXCLUSIVE_LOCK);
        storage_set_error(TBS_ERROR_SEGMENT_USE_AFTER_DROP);
        return DSTORE_FAIL;
    }
    if (STORAGE_FUNC_FAIL(DropSegmentInternal())) {
        lockMgr->Unlock(&tag, DSTORE_ACCESS_EXCLUSIVE_LOCK);
        return DSTORE_FAIL;
    }
    m_isDrop = true;
    lockMgr->Unlock(&tag, DSTORE_ACCESS_EXCLUSIVE_LOCK);
    return DSTORE_SUCC;
}

RetStatus Segment::ExtendSegmentInternal(ExtentSize extSize, PageId *extMetaPageId)
{
    PageId newExtMetaPageId = INVALID_PAGE_ID;

    /* Step 1: Alloc extent from tablespace */
    bool isReuseFlag = false;
    if (STORAGE_FUNC_FAIL(AllocExtent(this->GetPdbId(), m_tablespaceId, extSize, &newExtMetaPageId, &isReuseFlag))) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
            ErrMsg("Tablespace %hu in pdb %u alloc new extent failed.", m_tablespaceId, m_pdbId));
        return DSTORE_FAIL;
    }
    /* Check we have a valid PageId here after alloc */
    if (unlikely(newExtMetaPageId == INVALID_PAGE_ID)) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("Alloc extent succeed but page id invalid!"));
        return DSTORE_FAIL;
    }

    BufferDesc *segMetaPageBuf = ReadSegMetaPageBuf(LW_SHARED);
    SegmentMetaPage *metaPage = GetSegMetaPage(segMetaPageBuf);
    PageId lastExtMetaPageId = metaPage->GetLastExtent();
    UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);
    bool isSecondExtent = (lastExtMetaPageId == GetSegmentMetaPageId());
    AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;

    if (!IsTempSegment()) {
        walWriterContext->BeginAtomicWal(thrd->GetCurrentXid());
    }
    /* Step 2: Init new extent meta page */
    if (STORAGE_FUNC_FAIL(InitExtMetaPage(newExtMetaPageId, extSize))) {
        ErrLog(DSTORE_PANIC, MODULE_SEGMENT,
            ErrMsg("Extend segment(%hu, %u) with new Extent(%hu, %u) failed in atomic wal progress. pdb %u",
                metaPage->GetFileId(), metaPage->GetBlockNum(),
                newExtMetaPageId.m_fileId, newExtMetaPageId.m_blockId, m_pdbId));
        return DSTORE_FAIL;
    }

    /* Step 3: Modify next pointer of last extent meta page */
    if (likely(!isSecondExtent)) {
        if (STORAGE_FUNC_FAIL(LinkNextExtInPrevExt(lastExtMetaPageId, newExtMetaPageId))) {
            ErrLog(DSTORE_PANIC, MODULE_SEGMENT,
                ErrMsg("Extend segment(%hu, %u) with new Extent(%hu, %u) failed in atomic wal progress. pdb %u",
                    metaPage->GetFileId(), metaPage->GetBlockNum(),
                    newExtMetaPageId.m_fileId, newExtMetaPageId.m_blockId, m_pdbId));
            return DSTORE_FAIL;
        }
    }

    /* Step 4: Modify segment head page extent info (DataSegment need to add pages to unassigned array) */
    if (STORAGE_FUNC_FAIL(SegMetaLinkExtent(newExtMetaPageId, extSize, isSecondExtent))) {
        ErrLog(DSTORE_PANIC, MODULE_SEGMENT,
            ErrMsg("Extend segment(%hu, %u) with new Extent(%hu, %u) failed in atomic wal progress. pdb %u",
                metaPage->GetFileId(), metaPage->GetBlockNum(),
                newExtMetaPageId.m_fileId, newExtMetaPageId.m_blockId, m_pdbId));
        return DSTORE_FAIL;
    }
    if (!IsTempSegment()) {
        (void)walWriterContext->EndAtomicWal();
    }
    *extMetaPageId = newExtMetaPageId;
    return DSTORE_SUCC;
}

RetStatus Segment::InitExtMetaPage(const PageId &extMeta, ExtentSize extSize) const
{
    BufferDesc *buf = m_bufMgr->Read(m_pdbId, extMeta, LW_EXCLUSIVE, BufferPoolReadFlag::CreateNewPage());
    if (unlikely(buf == INVALID_BUFFER_DESC)) {
        return DSTORE_FAIL;
    }

    /* Step 1: Call Init function of ExtentMetaPage */
    SegExtentMetaPage *extMetaPage = static_cast<SegExtentMetaPage *>(buf->GetPage());
    extMetaPage->InitSegExtentMetaPage(extMeta, extSize, PageType::TBS_EXTENT_META_PAGE_TYPE);
    (void)m_bufMgr->MarkDirty(buf);
    AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
    if (!IsTempSegment()) {
        /* Step 2: Add Wal for current page */
        bool glsnChangedFlag = (extMetaPage->GetWalId() != walWriterContext->GetWalId());
        WalRecordTbsExtentMetaInit walData;
        walData.SetHeader({WAL_TBS_INIT_EXT_META, sizeof(walData), extMeta, extMetaPage->GetWalId(),
                           extMetaPage->GetPlsn(), extMetaPage->GetGlsn(), glsnChangedFlag, buf->GetFileVersion()});
        walData.SetData(extSize);
        walWriterContext->RememberPageNeedWal(buf);
        walWriterContext->PutNewWalRecord(&walData);
    }

    ErrLog(DSTORE_LOG, MODULE_SEGMENT,
        ErrMsg("Init extent meta page for pageId (%hu, %u) success.", extMeta.m_fileId, extMeta.m_blockId));

    m_bufMgr->UnlockAndRelease(buf);
    return DSTORE_SUCC;
}

RetStatus Segment::LinkNextExtInPrevExt(const PageId &prevExtMetaPageId, const PageId &nextExtMetaPageId) const
{
    BufferDesc *bufDesc = m_bufMgr->Read(m_pdbId, prevExtMetaPageId, LW_EXCLUSIVE);
    if (unlikely(bufDesc == INVALID_BUFFER_DESC)) {
        return DSTORE_FAIL;
    }
    SegExtentMetaPage *extMetaPage = static_cast<SegExtentMetaPage *>(bufDesc->GetPage());
    extMetaPage->LinkNextExtent(nextExtMetaPageId);
    (void)m_bufMgr->MarkDirty(bufDesc);
    AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;

    if (!IsTempSegment()) {
        /* Write Wal of lastExtMetaPageId */
        bool glsnChangedFlag = (extMetaPage->GetWalId() != walWriterContext->GetWalId());
        WalRecordTbsExtentMetaLinkNext walData;
        walData.SetHeader({WAL_TBS_MODIFY_EXT_META_NEXT, sizeof(walData), prevExtMetaPageId, extMetaPage->GetWalId(),
                           extMetaPage->GetPlsn(), extMetaPage->GetGlsn(), glsnChangedFlag, bufDesc->GetFileVersion()});
        walData.SetData(nextExtMetaPageId);
        walWriterContext->RememberPageNeedWal(bufDesc);
        walWriterContext->PutNewWalRecord(&walData);
    }

    m_bufMgr->UnlockAndRelease(bufDesc);
    return DSTORE_SUCC;
}

RetStatus Segment::SegMetaLinkExtent(const PageId &newExtMetaPageId, ExtentSize extSize, bool isSecondExtent)
{
    BufferDesc *segMetaPageBuf = ReadSegMetaPageBuf(LW_EXCLUSIVE);
    SegmentMetaPage *metaPage = static_cast<SegmentMetaPage *>(segMetaPageBuf->GetPage());
    metaPage->LinkExtent(newExtMetaPageId, extSize);
    if (isSecondExtent) {
        metaPage->extentMeta.nextExtMetaPageId = newExtMetaPageId;
    }

    (void)m_bufMgr->MarkDirty(segMetaPageBuf);
    AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
    if (!IsTempSegment()) {
        /* Write Wal of DataSegMetaPage */
        bool glsnChangedFlag = (metaPage->GetWalId() != walWriterContext->GetWalId());
        WalRecordTbsSegmentAddExtent walData;
        walData.SetHeader({WAL_TBS_SEG_ADD_EXT, sizeof(walData), GetSegmentMetaPageId(), metaPage->GetWalId(),
                           metaPage->GetPlsn(), metaPage->GetGlsn(), glsnChangedFlag,
                           segMetaPageBuf->GetFileVersion()});
        walData.SetData(newExtMetaPageId, extSize, EXT_DATA_PAGE_TYPE);
        walWriterContext->RememberPageNeedWal(segMetaPageBuf);
        walWriterContext->PutNewWalRecord(&walData);
    }

    UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);
    return DSTORE_SUCC;
}

RetStatus Segment::SegMetaUnlinkExtent(const PageId &unlinkExtMetaPageId, ExtentSize unlinkExtSize,
    const PageId &newNextExtMetaPageId)
{
    BufferDesc *segMetaPageBuf = ReadSegMetaPageBuf(LW_EXCLUSIVE);
    SegmentMetaPage *segMetaPage = static_cast<SegmentMetaPage *>(segMetaPageBuf->GetPage());
    segMetaPage->UnlinkExtent(newNextExtMetaPageId, unlinkExtMetaPageId, static_cast<uint16>(unlinkExtSize));

    (void)m_bufMgr->MarkDirty(segMetaPageBuf);
    AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
    if (!IsTempSegment()) {
        bool glsnChangedFlag = (segMetaPage->GetWalId() != walWriterContext->GetWalId());
        /* Write Wal of segment meta page */
        WalRecordTbsSegmentUnlinkExtent walData;
        walData.SetHeader({WAL_TBS_SEG_UNLINK_EXT, sizeof(walData), GetSegmentMetaPageId(), segMetaPage->GetWalId(),
                           segMetaPage->GetPlsn(), segMetaPage->GetGlsn(), glsnChangedFlag,
                           segMetaPageBuf->GetFileVersion()});
        ExtentUseType extUseType = m_type == SegmentType::UNDO_SEGMENT_TYPE ? EXT_UNDO_PAGE_TYPE : EXT_DATA_PAGE_TYPE;
        walData.SetData(newNextExtMetaPageId, unlinkExtMetaPageId, unlinkExtSize, extUseType);
        walWriterContext->RememberPageNeedWal(segMetaPageBuf);
        walWriterContext->PutNewWalRecord(&walData);
    }

    ErrLog(DSTORE_LOG, MODULE_SEGMENT,
        ErrMsg("Unlink extent (%hu, %u) for segment (%hu, %u) success, plsn is %lu, glsn is %lu.",
        unlinkExtMetaPageId.m_fileId, unlinkExtMetaPageId.m_blockId,
        GetSegmentMetaPageId().m_fileId, GetSegmentMetaPageId().m_blockId,
        segMetaPage->GetPlsn(), segMetaPage->GetGlsn()));

    UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);
    return DSTORE_SUCC;
}

RetStatus Segment::DropSegmentInternal()
{
    BufferDesc *segMetaPageBuf = ReadSegMetaPageBuf(LW_SHARED);
    SegmentMetaPage *segMetaPage = static_cast<SegmentMetaPage *>(segMetaPageBuf->GetPage());
    ExtentSize firstExtSize = segMetaPage->extentMeta.extSize;
    PageId curExtMetaPageId = segMetaPage->extentMeta.nextExtMetaPageId;
    UnlockAndReleaseSegMetaPageBuf(segMetaPageBuf);
    TablespaceMgr *tablespaceMgr = g_storageInstance->GetTablespaceMgrByPdbId(this->GetPdbId());
    if (STORAGE_VAR_NULL(tablespaceMgr)) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT,
               ErrMsg("Failed to get tablespaceMgr when drop segment, pdbId %u.", this->GetPdbId()));
        return DSTORE_FAIL;
    }

    /* The tablespace is already locked. See DropSegment. */
    TableSpace *tablespace = tablespaceMgr->OpenTablespace(m_tablespaceId, DSTORE::DSTORE_NO_LOCK);
    if (STORAGE_VAR_NULL(tablespace)) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("Failed to open tablespace %hu.", m_tablespaceId));
        return DSTORE_FAIL;
    }
    /* Always free second extent until second extent is INVALID_PAGE_ID */
    AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
    while (curExtMetaPageId != INVALID_PAGE_ID) {
        BufferDesc *bufDesc = m_bufMgr->Read(this->GetPdbId(), curExtMetaPageId, LW_SHARED);
        if (unlikely(bufDesc == INVALID_BUFFER_DESC)) {
            tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_NO_LOCK);
            return DSTORE_FAIL;
        }
        SegExtentMetaPage *extMetaPage = static_cast<SegExtentMetaPage *>(bufDesc->GetPage());
        PageId nextExtMetaPageId = extMetaPage->extentMeta.nextExtMetaPageId;
        ExtentSize curExtSize = extMetaPage->extentMeta.extSize;
        m_bufMgr->UnlockAndRelease(bufDesc);
        if (!IsTempSegment()) {
            walWriterContext->BeginAtomicWal(thrd->GetCurrentXid());
        }
        if (IsTempSegment() &&
            STORAGE_FUNC_FAIL(InvalidateBufferInExtent(curExtMetaPageId, curExtSize))) {
            tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_NO_LOCK);
            ErrLog(DSTORE_PANIC, MODULE_SEGMENT, ErrMsg("Invalid temp buffer Start(%hu, %u) size(%hu)fail",
                curExtMetaPageId.m_fileId, curExtMetaPageId.m_blockId, static_cast<uint16>(curExtSize)));
            return DSTORE_FAIL;
        }
        if (STORAGE_FUNC_FAIL(tablespace->FreeExtent(curExtSize, curExtMetaPageId))) {
            tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_NO_LOCK);
            ErrLog(DSTORE_PANIC, MODULE_SEGMENT, ErrMsg("Free extent(%hu, %u)fail",
                curExtMetaPageId.m_fileId, curExtMetaPageId.m_blockId));
            return DSTORE_FAIL;
        }
        if (STORAGE_FUNC_FAIL(SegMetaUnlinkExtent(curExtMetaPageId, curExtSize, nextExtMetaPageId))) {
            tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_NO_LOCK);
            ErrLog(DSTORE_PANIC, MODULE_SEGMENT, ErrMsg("Unlink extent(%hu, %u)fail,next(%hu, %u)",
                curExtMetaPageId.m_fileId, curExtMetaPageId.m_blockId,
                nextExtMetaPageId.m_fileId, nextExtMetaPageId.m_blockId));
            return DSTORE_FAIL;
        }
        if (!IsTempSegment()) {
            (void)walWriterContext->EndAtomicWal();
        }
        curExtMetaPageId = nextExtMetaPageId;
    }

    if (!IsTempSegment()) {
        walWriterContext->BeginAtomicWal(thrd->GetCurrentXid());
    }
    if (IsTempSegment() &&
        STORAGE_FUNC_FAIL(InvalidateBufferInExtent(GetSegmentMetaPageId(), firstExtSize))) {
        tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_NO_LOCK);
        ErrLog(DSTORE_PANIC, MODULE_SEGMENT, ErrMsg("Invalid temp buffer Start(%hu, %u) size(%hu)fail",
            GetSegmentMetaPageId().m_fileId, GetSegmentMetaPageId().m_blockId, static_cast<uint16>(firstExtSize)));
        return DSTORE_FAIL;
    }
    if (STORAGE_FUNC_FAIL(tablespace->FreeExtent(firstExtSize, GetSegmentMetaPageId()))) {
        tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_NO_LOCK);
        ErrLog(DSTORE_PANIC, MODULE_SEGMENT, ErrMsg("Free first extent(%hu, %u)fail",
            GetSegmentMetaPageId().m_fileId, GetSegmentMetaPageId().m_blockId));
        return DSTORE_FAIL;
    }
    if (!IsTempSegment()) {
        (void)walWriterContext->EndAtomicWal();
    }
    tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_NO_LOCK);
    return DSTORE_SUCC;
}

RetStatus Segment::InvalidateBufferInExtent(const PageId &startPageId, ExtentSize extentSize)
{
    if (!IsTempSegment()) {
        return DSTORE_SUCC;
    }

    for (uint16 i = 0; i < static_cast<uint16>(extentSize); i++) {
        PageId pageId = startPageId;
        pageId.m_blockId += i;
        BufferTag bufTag = {this->GetPdbId(), pageId};
        if (STORAGE_FUNC_FAIL(m_bufMgr->InvalidateByBufTag(bufTag, false))) {
            ErrLog(DSTORE_PANIC, MODULE_BUFMGR, ErrMsg("Invalidate buftag:(%hhu, %hu, %u)fail start(%hu, %u) %hu.",
                bufTag.pdbId, bufTag.pageId.m_fileId, bufTag.pageId.m_blockId,
                startPageId.m_fileId, startPageId.m_blockId, static_cast<uint16>(extentSize)));
            return DSTORE_FAIL;
        }
    }
    return DSTORE_SUCC;
}

bool ExtentsScanner::Next()
{
    m_extMetaPageId = (m_extMetaPageId.IsInvalid()) ? m_segmentId : m_nextExtMeta;
    if (m_extMetaPageId.IsInvalid()) {
        return false;
    }
    BufferDesc *bufDesc = m_bufMgr->Read(m_pdbId, m_extMetaPageId, LW_SHARED);
    if (bufDesc == INVALID_BUFFER_DESC) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("Read extent meta page failed. pageId:(%hu, %u), errCode:%lld",
            m_extMetaPageId.m_fileId, m_extMetaPageId.m_blockId, StorageGetErrorCode()));
        return false;
    }
    SegExtentMetaPage *metaPage = static_cast<SegExtentMetaPage *>(bufDesc->GetPage());
    PageId selfPageId = metaPage->GetSelfPageId();
    m_extSize = metaPage->extentMeta.extSize;
    m_nextExtMeta = metaPage->extentMeta.nextExtMetaPageId;
    m_bufMgr->UnlockAndRelease(bufDesc);
    return selfPageId == m_extMetaPageId && m_extSize <= EXT_SIZE_8192;
}

bool ExtentsScanner::CheckSegmentMeta()
{
    if (m_segmentId.IsInvalid()) {
        return false;
    }

    BufferDesc *bufDesc = m_bufMgr->Read(m_pdbId, m_segmentId, LW_SHARED);
    if (bufDesc == INVALID_BUFFER_DESC) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("Read segment meta page failed. pageId:(%hu, %u), errCode:%lld",
            m_segmentId.m_fileId, m_segmentId.m_blockId, StorageGetErrorCode()));
        return false;
    }
    SegmentMetaPage *metaPage = static_cast<SegmentMetaPage *>(bufDesc->GetPage());
    uint64 segMetaMagic = metaPage->extentMeta.magic;
    SegmentType type = metaPage->segmentHeader.segmentType;
    PageId pageId = metaPage->GetSelfPageId();
    m_bufMgr->UnlockAndRelease(bufDesc);
    return segMetaMagic == SEGMENT_META_MAGIC && pageId == m_segmentId &&
        type >= SegmentType::HEAP_SEGMENT_TYPE && type <= SegmentType::INDEX_TEMP_SEGMENT_TYPE;
}

} /* namespace DSTORE */
