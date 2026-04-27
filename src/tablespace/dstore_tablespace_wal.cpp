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
 * dstore_tablespace_wal.cpp
 *
 * IDENTIFICATION
 *        src/tablespace/dstore_tablespace_wal.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "tablespace/dstore_tablespace_wal.h"

#include "common/log/dstore_log.h"
#include "framework/dstore_instance.h"
#include "page/dstore_bitmap_meta_page.h"
#include "page/dstore_index_page.h"
#include "page/dstore_heap_page.h"
#include "page/dstore_bitmap_page.h"
#include "page/dstore_data_segment_meta_page.h"
#include "page/dstore_heap_segment_meta_page.h"
#include "page/dstore_undo_segment_meta_page.h"
#include "page/dstore_tbs_file_meta_page.h"
#include "page/dstore_tbs_space_meta_page.h"
#include "wal/dstore_wal_recovery.h"
#include "errorcode/dstore_lock_error_code.h"

namespace DSTORE {

struct RedoItem {
    WalType type;
    std::function<void(PdbId pdbId, const WalRecordTbs*, Page *)> redo;

    RedoItem(WalType walType, const std::function<void(PdbId pdbId, const WalRecordTbs*, Page *)> &redoFunc) noexcept
        : type(walType), redo(redoFunc)
    {}
};

static const RedoItem REDO_TABLE[] {
    { WAL_TBS_INIT_BITMAP_META_PAGE,
        [](PdbId, const WalRecordTbs* self, Page *page)
        { (static_cast<const WalRecordTbsInitBitmapMetaPage *>(self))->Redo(page); } },
    { WAL_TBS_INIT_TBS_FILE_META_PAGE,
            [](PdbId, const WalRecordTbs* self, Page *page)
            { (static_cast<const WalRecordTbsInitTbsFileMetaPage *>(self))->Redo(page); } },
    { WAL_TBS_INIT_TBS_SPACE_META_PAGE,
            [](PdbId, const WalRecordTbs* self, Page *page)
            { (static_cast<const WalRecordTbsInitTbsSpaceMetaPage *>(self))->Redo(page); } },
    { WAL_TBS_UPDATE_TBS_FILE_META_PAGE,
            [](PdbId, const WalRecordTbs* self, Page *page)
            { (static_cast<const WalRecordTbsUpdateTbsFileMetaPage *>(self))->Redo(page); } },
    { WAL_TBS_INIT_ONE_BITMAP_PAGE,
        [](PdbId, const WalRecordTbs* self, Page *page)
        { (static_cast<const WalRecordTbsInitOneBitmapPage *>(self))->Redo(page); } },
    { WAL_TBS_ADD_BITMAP_PAGES,
        [](PdbId, const WalRecordTbs* self, Page *page)
        { (static_cast<const WalRecordTbsAddBitmapPages *>(self))->Redo(page); } },
    {WAL_TBS_BITMAP_ALLOC_BIT_START,
        [](PdbId, const WalRecordTbs* self, Page *page)
        { (static_cast<const WalRecordTbsBitmapSetBit *>(self))->Redo(page); } },
    {WAL_TBS_BITMAP_FREE_BIT_START,
        [](PdbId, const WalRecordTbs* self, Page *page)
        { (static_cast<const WalRecordTbsBitmapSetBit *>(self))->Redo(page); } },
    {WAL_TBS_EXTEND_FILE,
        [](PdbId pdbId, const WalRecordTbs* self, Page *page)
        { (static_cast<const WalRecordTbsExtendFile *>(self))->Redo(pdbId, page); } },
    { WAL_TBS_INIT_UNDO_SEGMENT_META,
        [](PdbId, const WalRecordTbs* self, Page *page)
        { (static_cast<const WalRecordTbsInitUndoSegment *>(self))->Redo(page); } },
    { WAL_TBS_INIT_DATA_SEGMENT_META,
        [](PdbId, const WalRecordTbs* self, Page *page)
        { (static_cast<const WalRecordTbsInitDataSegment *>(self))->Redo(page); } },
    { WAL_TBS_INIT_HEAP_SEGMENT_META,
        [](PdbId, const WalRecordTbs* self, Page *page)
        { (static_cast<const WalRecordTbsInitHeapSegment *>(self))->Redo(page); } },
    {  WAL_TBS_INIT_FSM_META,
        [](PdbId, const WalRecordTbs* self, Page *page)
        { (static_cast<const WalRecordTbsInitFreeSpaceMap *>(self))->Redo(page); } },
    {  WAL_TBS_INIT_EXT_META,
        [](PdbId, const WalRecordTbs* self, Page *page)
        { (static_cast<const WalRecordTbsExtentMetaInit *>(self))->Redo(page); } },
    {  WAL_TBS_MODIFY_EXT_META_NEXT,
        [](PdbId, const WalRecordTbs* self, Page *page)
        { (static_cast<const WalRecordTbsExtentMetaLinkNext *>(self))->Redo(page); } },
    {  WAL_TBS_MOVE_FSM_SLOT,
        [](PdbId, const WalRecordTbs* self, Page *page)
        { (static_cast<const WalRecordTbsMoveFsmSlot *>(self))->Redo(page); } },
    {  WAL_TBS_ADD_FSM_SLOT,
        [](PdbId, const WalRecordTbs* self, Page *page)
        { (static_cast<const WalRecordTbsAddMultiplePagesToFsmSlots *>(self))->Redo(page); } },
    {  WAL_TBS_INIT_FSM_PAGE,
        [](PdbId, const WalRecordTbs* self, Page *page)
        { (static_cast<const WalRecordTbsInitFsmPage *>(self))->Redo(page); } },
    {  WAL_TBS_MODIFY_FSM_INDEX,
        [](PdbId, const WalRecordTbs* self, Page *page)
        { (static_cast<const WalRecordTbsUpdateFsmIndex *>(self))->Redo(page); } },
    {  WAL_TBS_SEG_ADD_EXT,
        [](PdbId, const WalRecordTbs* self, Page *page)
        { (static_cast<const WalRecordTbsSegmentAddExtent *>(self))->Redo(page); } },
    {  WAL_TBS_DATA_SEG_ADD_EXT,
        [](PdbId, const WalRecordTbs* self, Page *page)
        { (static_cast<const WalRecordTbsDataSegmentAddExtent *>(self))->Redo(page); } },
    {  WAL_TBS_SEG_META_ASSIGN_DATA_PAGES,
        [](PdbId, const WalRecordTbs* self, Page *page)
        { (static_cast<const WalRecordTbsDataSegmentAssignDataPages *>(self))->Redo(page); } },
    {  WAL_TBS_SEG_UNLINK_EXT,
        [](PdbId, const WalRecordTbs* self, Page *page)
        { (static_cast<const WalRecordTbsSegmentUnlinkExtent *>(self))->Redo(page); } },
    {  WAL_TBS_SEG_META_ADD_FSM_TREE,
        [](PdbId, const WalRecordTbs* self, Page *page)
        { (static_cast<const WalRecordTbsSegMetaAddFsmTree *>(self))->Redo(page); } },
    {  WAL_TBS_SEG_META_RECYCLE_FSM_TREE,
        [](PdbId, const WalRecordTbs* self, Page *page)
        { (static_cast<const WalRecordTbsSegMetaRecycleFsmTree *>(self))->Redo(page); } },
    {WAL_TBS_SEG_META_ADJUST_DATA_PAGES_INFO,
        [](PdbId, const WalRecordTbs* self, Page *page)
        { (static_cast<const WalRecordTbsSegMetaAdjustDataPagesInfo *>(self))->Redo(page); } },
    {  WAL_TBS_FSM_META_UPDATE_FSM_TREE,
        [](PdbId, const WalRecordTbs* self, Page *page)
        { (static_cast<const WalRecordTbsFsmMetaUpdateFsmTree *>(self))->Redo(page); } },
    {  WAL_TBS_FSM_META_UPDATE_NUM_USED_PAGES,
        [](PdbId, const WalRecordTbs* self, Page *page)
        { (static_cast<const WalRecordTbsFsmMetaUpdateNumUsedPages *>(self))->Redo(page); } },
    {  WAL_TBS_FSM_META_UPDATE_EXTENSION_STAT,
        [](PdbId, const WalRecordTbs* self, Page *page)
        { (static_cast<const WalRecordTbsFsmMetaUpdateExtensionStat *>(self))->Redo(page); } },
    {  WAL_TBS_UPDATE_FIRST_FREE_BITMAP_PAGE,
        [](PdbId, const WalRecordTbs* self, Page *page)
        { (static_cast<const WalRecordTbsUpdateFirstFreeBitmapPageId *>(self))->Redo(page); } }
};

void WalRecordTbs::RedoTbsRecord(WalRecordRedoContext *redoCtx, const WalRecord *tbsRecord,
    BufferDesc *bufferDesc)
{
    StorageAssert(tbsRecord != nullptr);
    /* WalRecordTbsInitDataPages and WalRecordTbsInitBitmapPages are logic wal */
    WalType recordType = tbsRecord->m_type;
#ifdef UT
    if (recordType == WAL_EMPTY_DDL_REDO) {
        return;
    }
#endif
    if (recordType == WAL_TBS_INIT_MULTIPLE_DATA_PAGES) {
        static_cast<const WalRecordTbsInitDataPages *>(tbsRecord)->Redo(redoCtx);
    } else if (recordType == WAL_TBS_INIT_BITMAP_PAGES) {
        static_cast<const WalRecordTbsInitBitmapPages *>(tbsRecord)->Redo(redoCtx);
    } else if (recordType == WAL_TBS_INIT_ONE_DATA_PAGE) {
        const WalRecordTbsInitOneDataPage *walRecordTbsInitOneDataPage =
            static_cast<const WalRecordTbsInitOneDataPage *>(tbsRecord);
        STORAGE_CHECK_BUFFER_PANIC(bufferDesc, MODULE_TABLESPACE, walRecordTbsInitOneDataPage->m_pageId);
        Page *page = bufferDesc->GetPage();
        WalRecordLsnInfo preRecordLsnInfo = {walRecordTbsInitOneDataPage->m_pagePreWalId,
            walRecordTbsInitOneDataPage->m_pagePrePlsn, walRecordTbsInitOneDataPage->m_pagePreGlsn};
        uint64 recordGlsn = walRecordTbsInitOneDataPage->m_pagePreWalId != redoCtx->walId ?
            walRecordTbsInitOneDataPage->m_pagePreGlsn + 1 : walRecordTbsInitOneDataPage->m_pagePreGlsn;
        WalRecordLsnInfo recordLsnInfo = {redoCtx->walId, redoCtx->recordEndPlsn, recordGlsn};
        WalRecordReplayType replayType =
            WalRecovery::GetWalRecordReplayType(bufferDesc->GetBufferTag(), page, preRecordLsnInfo, recordLsnInfo);
        if (replayType != WalRecordReplayType::REPLAYABLE) {
            ErrLog(DSTORE_LOG, MODULE_TABLESPACE,
                   ErrMsg("Skip WAL_TBS_INIT_ONE_DATA_PAGE wal %lu plsn %lu glsn %lu page (%d, %u)", redoCtx->walId,
                          redoCtx->recordEndPlsn, recordGlsn, bufferDesc->GetPageId().m_fileId,
                          bufferDesc->GetPageId().m_blockId));
            return;
        }
        ErrLog(DSTORE_LOG, MODULE_TABLESPACE,
               ErrMsg("WAL_TBS_INIT_ONE_DATA_PAGE wal %lu plsn %lu glsn %lu page (%d, %u)", redoCtx->walId,
                      redoCtx->recordEndPlsn, recordGlsn, bufferDesc->GetPageId().m_fileId,
                      bufferDesc->GetPageId().m_blockId));
        walRecordTbsInitOneDataPage->Redo(bufferDesc);
        page->SetLsn(redoCtx->walId, redoCtx->recordEndPlsn, recordGlsn);
    } else if (recordType == WAL_TBS_CREATE_TABLESPACE) {
        static_cast<const WalRecordTbsCreateTablespace *>(tbsRecord)->Redo(redoCtx);
    } else if (recordType == WAL_TBS_CREATE_DATA_FILE) {
        static_cast<const WalRecordTbsCreateDataFile *>(tbsRecord)->Redo(redoCtx);
    } else if (recordType == WAL_TBS_ADD_FILE_TO_TABLESPACE) {
        static_cast<const WalRecordTbsAddFileToTbs *>(tbsRecord)->Redo(redoCtx);
    } else if (recordType == WAL_TBS_DROP_TABLESPACE) {
        static_cast<const WalRecordTbsDropTablespace *>(tbsRecord)->Redo(redoCtx);
    } else if (recordType == WAL_TBS_DROP_DATA_FILE) {
        static_cast<const WalRecordTbsDropDataFile *>(tbsRecord)->Redo(redoCtx);
    } else if (recordType == WAL_TBS_ALTER_TABLESPACE) {
        static_cast<const WalRecordTbsAlterTablespace *>(tbsRecord)->Redo(redoCtx);
    } else {
        const WalRecordTbs *walRecordTbs = static_cast<const WalRecordTbs *>(tbsRecord);
        PageId pageId = walRecordTbs->m_pageId;
        STORAGE_CHECK_BUFFER_PANIC(bufferDesc, MODULE_TABLESPACE, pageId);
        Page *page = bufferDesc->GetPage();
        if (recordType == WAL_TBS_INIT_ONE_BITMAP_PAGE) {
            ErrLog(DSTORE_LOG, MODULE_TABLESPACE,
                   ErrMsg("WAL_TBS_INIT_ONE_BITMAP_PAGE wal %lu plsn %lu page (%d, %u)", redoCtx->walId,
                          redoCtx->recordEndPlsn, pageId.m_fileId, pageId.m_blockId));
        }
        for (auto &redoItem: DSTORE::REDO_TABLE) {
            if (walRecordTbs->m_type == redoItem.type) {
                redoItem.redo(redoCtx->pdbId, walRecordTbs, page);
            }
        }
        /* Write back wal id, plsn, glsn */
        const uint64 glsn = walRecordTbs->m_pagePreWalId != redoCtx->walId ? walRecordTbs->m_pagePreGlsn + 1
           : walRecordTbs->m_pagePreGlsn;
        page->SetLsn(redoCtx->walId, redoCtx->recordEndPlsn, glsn);
    }
}

#ifdef UT
void WalRecordTbs::RedoInternal(PdbId pdbId, uint64 plsn, Page *page) const
{
    if (plsn <= page->GetPlsn()) {
        /* No need to redo it for the page */
        ErrLog(DSTORE_DEBUG1, MODULE_TABLESPACE,
               ErrMsg("Skip the wal which plsn is %lu during redo, the page (%d, %u) plsn is %lu", plsn,
                      m_pageId.m_fileId, m_pageId.m_blockId, page->GetPlsn()));
    } else {
        for (auto &redoItem: DSTORE::REDO_TABLE) {
            if (m_type == redoItem.type) {
                redoItem.redo(pdbId, this, page);
            }
        }
        /* Write back wal id, plsn, glsn */
        page->SetPlsn(plsn);
        page->SetGlsn(m_pagePreGlsn + m_flags.m_flag.glsnChangeFlag);
    }
}
#endif

bool WalRecordTbs::DumpTbsRecordPart1(const WalRecordTbs *tbsRecord, FILE *fp)
{
    StorageAssert(tbsRecord != nullptr);
    PageId firstPageId = tbsRecord->m_pageId;
    uint16 pageCount = 1;
    if (tbsRecord->m_type == WAL_TBS_INIT_MULTIPLE_DATA_PAGES) {
        const WalRecordTbsInitDataPages *record = static_cast<const WalRecordTbsInitDataPages *>(
            static_cast<const void *>(tbsRecord));
        record->GetPageIdRange(firstPageId, pageCount);
        (void)fprintf(
            fp, "Type WAL_TBS_INIT_MULTIPLE_DATA_PAGES. Page type %hhu, PageId range = (%hu, %u-%u), page count:%u, "
            "prev file version %lu;", static_cast<uint8>(record->dataPageType), firstPageId.m_fileId,
            firstPageId.m_blockId, firstPageId.m_blockId + pageCount - 1, pageCount, record->filePreVersion);
        for (uint32_t i = 0; i < pageCount; ++i) {
            (void)fprintf(
                fp, "page %u: pageId=(%hu, %u), page prev walId:%lu, page prev endPlsn:%lu, page prev glsn:%lu;\n", i,
                firstPageId.m_fileId, firstPageId.m_blockId + i, record->preWalPointer[i].walId,
                record->preWalPointer[i].endPlsn, record->preWalPointer[i].glsn);
        }
    } else if (tbsRecord->m_type == WAL_TBS_INIT_BITMAP_PAGES) {
        const WalRecordTbsInitBitmapPages *record =
            static_cast<const WalRecordTbsInitBitmapPages *>(static_cast<const void *>(tbsRecord));
        record->GetPageIdRange(firstPageId, pageCount);
        (void)fprintf(
            fp, "Type WAL_TBS_INIT_BITMAP_PAGES. PageId range = (%hu, %u-%u), page count:%u, prev file version %lu;\n",
            firstPageId.m_fileId, firstPageId.m_blockId, firstPageId.m_blockId + pageCount - 1,
            pageCount, record->filePreVersion);
        for (uint32_t i = 0; i < pageCount; ++i) {
            (void)fprintf(
                fp, "page %u: pageId=(%hu, %u), page prev walId:%lu, page prev endPlsn:%lu, page prev glsn:%lu;\n", i,
                firstPageId.m_fileId, firstPageId.m_blockId + i, record->preWalPointer[i].walId,
                record->preWalPointer[i].endPlsn, record->preWalPointer[i].glsn);
        }
    } else if (tbsRecord->m_type == WAL_TBS_INIT_BITMAP_META_PAGE) {
        const WalRecordTbsInitBitmapMetaPage *record =
            static_cast<const WalRecordTbsInitBitmapMetaPage*>(static_cast<const void *>(tbsRecord));
        (void)fprintf(fp, "type:WAL_TBS_INIT_BITMAP_META_PAGE; len:%hu; pageId=(%hu, %u); "
            "page prev walId:%lu; page prev endPlsn:%lu; page prev glsn:%lu; file prev version:%lu; glsnChangeFlag:%s; "
            "fileVersionFlag:%s; totalBlockCount: %hhu; ",
            record->m_size, record->m_pageId.m_fileId, record->m_pageId.m_blockId, record->m_pagePreWalId,
            record->m_pagePrePlsn, record->m_pagePreGlsn, record->m_filePreVersion,
            record->m_flags.m_flag.glsnChangeFlag ? "true" : "false",
            record->m_flags.m_flag.containFileVersionFlag ? "true" : "false",
            record->totalBlockCount);
    } else if (tbsRecord->m_type == WAL_TBS_INIT_ONE_BITMAP_PAGE) {
        const WalRecordTbsInitOneBitmapPage *record =
            static_cast<const WalRecordTbsInitOneBitmapPage*>(static_cast<const void *>(tbsRecord));
        (void)fprintf(fp, "type:WAL_TBS_INIT_ONE_BITMAP_PAGE; len:%hu; pageId=(%hu, %u); "
            "page prev walId:%lu; page prev endPlsn:%lu; page prev glsn:%lu; file prev version:%lu; glsnChangeFlag:%s; "
            "fileVersionFlag:%s; pageType:%hhu; curDataPageId=(%hu, %u); ",
            record->m_size, record->m_pageId.m_fileId, record->m_pageId.m_blockId, record->m_pagePreWalId,
            record->m_pagePrePlsn, record->m_pagePreGlsn, record->m_filePreVersion,
            record->m_flags.m_flag.glsnChangeFlag ? "true" : "false",
            record->m_flags.m_flag.containFileVersionFlag ? "true" : "false",
            static_cast<uint8>(record->pageType), record->curDataPageId.m_fileId, record->curDataPageId.m_blockId);
    } else if (tbsRecord->m_type == WAL_TBS_ADD_BITMAP_PAGES) {
        const WalRecordTbsAddBitmapPages *record =
            static_cast<const WalRecordTbsAddBitmapPages*>(static_cast<const void *>(tbsRecord));
        (void)fprintf(fp, "type:WAL_TBS_ADD_BITMAP_PAGES; len:%hu; pageId=(%hu, %u); "
            "page prev walId:%lu; page prev endPlsn:%lu; page prev glsn:%lu; file prev version:%lu; glsnChangeFlag:%s; "
            "fileVersionFlag:%s;  groupCount:%hu; groupIndex:%hu; groupFirstPage=(%hu, %u); groupFreePage:%hhu; "
            "groupPageCount:%hhu; validOffset:%hu; ",
            record->m_size, record->m_pageId.m_fileId, record->m_pageId.m_blockId, record->m_pagePreWalId,
            record->m_pagePrePlsn, record->m_pagePreGlsn, record->m_filePreVersion,
            record->m_flags.m_flag.glsnChangeFlag ? "true" : "false",
            record->m_flags.m_flag.containFileVersionFlag ? "true" : "false",
            record->groupCount, record->groupIndex, record->groupFirstPage.m_fileId,
            record->groupFirstPage.m_blockId, record->groupFreePage, record->groupPageCount,
            record->validOffset);
    } else if (tbsRecord->m_type == WAL_TBS_INIT_TBS_FILE_META_PAGE) {
        const WalRecordTbsInitTbsFileMetaPage *record =
                static_cast<const WalRecordTbsInitTbsFileMetaPage*>(static_cast<const void *>(tbsRecord));
        (void)fprintf(fp, "type:WAL_TBS_INIT_TBS_FILE_META_PAGE; len:%hu; pageId=(%hu, %u); reuseVersion:%lu; "
                    "ddlXid:%lu; page prev walId:%lu; page prev endPlsn:%lu; page prev glsn:%lu; "
                    "file prev version:%lu; glsnChangeFlag:%s; fileVersionFlag:%s; ",
                    record->m_size, record->m_pageId.m_fileId, record->m_pageId.m_blockId, record->m_reuseVersion,
                    record->m_ddlXid.m_placeHolder, record->m_pagePreWalId,
                    record->m_pagePrePlsn, record->m_pagePreGlsn, record->m_filePreVersion,
                    record->m_flags.m_flag.glsnChangeFlag ? "true" : "false",
                    record->m_flags.m_flag.containFileVersionFlag ? "true" : "false");
    } else if (tbsRecord->m_type == WAL_TBS_INIT_TBS_SPACE_META_PAGE) {
        const WalRecordTbsInitTbsSpaceMetaPage *record =
                static_cast<const WalRecordTbsInitTbsSpaceMetaPage*>(static_cast<const void *>(tbsRecord));
        (void)fprintf(fp, "type:WAL_TBS_INIT_TBS_SPACE_META_PAGE; len:%hu; pageId=(%hu, %u); "
                    "page prev walId:%lu; page prev endPlsn:%lu; page prev glsn:%lu; "
                    "file prev version:%lu; glsnChangeFlag:%s; fileVersionFlag:%s; ",
                    record->m_size, record->m_pageId.m_fileId, record->m_pageId.m_blockId, record->m_pagePreWalId,
                    record->m_pagePrePlsn, record->m_pagePreGlsn, record->m_filePreVersion,
                    record->m_flags.m_flag.glsnChangeFlag ? "true" : "false",
                    record->m_flags.m_flag.containFileVersionFlag ? "true" : "false");
    } else if (tbsRecord->m_type == WAL_TBS_UPDATE_TBS_FILE_META_PAGE) {
        const WalRecordTbsUpdateTbsFileMetaPage *record =
                static_cast<const WalRecordTbsUpdateTbsFileMetaPage*>(static_cast<const void *>(tbsRecord));
        (void)fprintf(fp, "type:WAL_TBS_UPDATE_TBS_FILE_META_PAGE; len:%hu; pageId=(%hu, %u); "
                    "page prev walId:%lu; page prev endPlsn:%lu; page prev glsn:%lu; file prev version:%lu; "
                    "glsnChangeFlag:%s; fileVersionFlag:%s; hwm:%u; oid:%u; ",
                    record->m_size, record->m_pageId.m_fileId, record->m_pageId.m_blockId, record->m_pagePreWalId,
                    record->m_pagePrePlsn, record->m_pagePreGlsn, record->m_filePreVersion,
                    record->m_flags.m_flag.glsnChangeFlag ? "true" : "false",
                    record->m_flags.m_flag.containFileVersionFlag ? "true" : "false",
                    record->hwm, record->oid);
    } else if (tbsRecord->m_type == WAL_TBS_UPDATE_FIRST_FREE_BITMAP_PAGE) {
        const WalRecordTbsUpdateFirstFreeBitmapPageId *record =
                static_cast<const WalRecordTbsUpdateFirstFreeBitmapPageId*>(static_cast<const void *>(tbsRecord));
        (void)fprintf(fp, "type:WAL_TBS_UPDATE_FIRST_FREE_BITMAP_PAGE; len:%hu; pageId=(%hu, %u); "
                    "page prev walId:%lu; page prev endPlsn:%lu; page prev glsn:%lu; file prev version:%lu; "
                    "glsnChangeFlag:%s; fileVersionFlag:%s; groupIndex:%hu; firstFreePageNo:%hhu;",
                    record->m_size, record->m_pageId.m_fileId, record->m_pageId.m_blockId, record->m_pagePreWalId,
                    record->m_pagePrePlsn, record->m_pagePreGlsn, record->m_filePreVersion,
                    record->m_flags.m_flag.glsnChangeFlag ? "true" : "false",
                    record->m_flags.m_flag.containFileVersionFlag ? "true" : "false",
                    record->groupIndex, record->firstFreePageNo);
    } else {
        return false;
    }
    return true;
}

bool WalRecordTbs::DumpTbsRecordPart2(const WalRecordTbs *tbsRecord, FILE *fp)
{
    StorageAssert(tbsRecord != nullptr);
    if (tbsRecord->m_type == WAL_TBS_BITMAP_ALLOC_BIT_START || tbsRecord->m_type == WAL_TBS_BITMAP_FREE_BIT_START) {
        const WalRecordTbsBitmapSetBit *record =
            static_cast<const WalRecordTbsBitmapSetBit*>(static_cast<const void *>(tbsRecord));
        (void)fprintf(fp, "type:%s; len:%hu; pageId=(%hu, %u); "
            "page prev walId:%lu; page prev endPlsn:%lu; page prev glsn:%lu; file prev version:%lu; glsnChangeFlag:%s; "
            "fileVersionFlag:%s; allocatedExtentCount:%hu; startBitPos:%hu; value:%hhu; ",
            tbsRecord->m_type == WAL_TBS_BITMAP_ALLOC_BIT_START ?
                "WAL_TBS_BITMAP_ALLOC_BIT_START" : "WAL_TBS_BITMAP_FREE_BIT_START",
            record->m_size, record->m_pageId.m_fileId, record->m_pageId.m_blockId, record->m_pagePreWalId,
            record->m_pagePrePlsn, record->m_pagePreGlsn, record->m_filePreVersion,
            record->m_flags.m_flag.glsnChangeFlag ? "true" : "false",
            record->m_flags.m_flag.containFileVersionFlag ? "true" : "false",
            record->allocatedExtentCount,
            record->startBitPos, record->value);
    } else if (tbsRecord->m_type == WAL_TBS_EXTEND_FILE) {
        const WalRecordTbsExtendFile *record =
            static_cast<const WalRecordTbsExtendFile*>(static_cast<const void *>(tbsRecord));
        (void)fprintf(fp, "type:WAL_TBS_EXTEND_FILE; len:%hu; pageId=(%hu, %u); "
            "page prev walId:%lu; page prev endPlsn:%lu; page prev glsn:%lu; file prev version:%lu; glsnChangeFlag:%s; "
            "fileVersionFlag:%s; fileId:%hu; totalBlockCount:%lu; ",
            record->m_size, record->m_pageId.m_fileId, record->m_pageId.m_blockId, record->m_pagePreWalId,
            record->m_pagePrePlsn, record->m_pagePreGlsn, record->m_filePreVersion,
            record->m_flags.m_flag.glsnChangeFlag ? "true" : "false",
            record->m_flags.m_flag.containFileVersionFlag ? "true" : "false",
            record->fileId, record->totalBlockCount);
    } else if (tbsRecord->m_type == WAL_TBS_INIT_UNDO_SEGMENT_META) {
        const WalRecordTbsInitUndoSegment *record =
            static_cast<const WalRecordTbsInitUndoSegment*>(static_cast<const void *>(tbsRecord));
        (void)fprintf(fp, "type:WAL_TBS_INIT_UNDO_SEGMENT_META; len:%hu; pageId=(%hu, %u); "
            "page prev walId:%lu; page prev endPlsn:%lu; page prev glsn:%lu; file prev version:%lu; glsnChangeFlag:%s; "
            "fileVersionFlag:%s; segmentType:%hhu; ",
            record->m_size, record->m_pageId.m_fileId, record->m_pageId.m_blockId, record->m_pagePreWalId,
            record->m_pagePrePlsn, record->m_pagePreGlsn, record->m_filePreVersion,
            record->m_flags.m_flag.glsnChangeFlag ? "true" : "false",
            record->m_flags.m_flag.containFileVersionFlag ? "true" : "false",
            static_cast<uint8>(record->segmentType));
    } else {
        return false;
    }
    return true;
}

bool WalRecordTbs::DumpTbsRecordPart3(const WalRecordTbs *tbsRecord, FILE *fp)
{
    StorageAssert(tbsRecord != nullptr);
    if (tbsRecord->m_type == WAL_TBS_INIT_DATA_SEGMENT_META) {
        const WalRecordTbsInitDataSegment *record =
            static_cast<const WalRecordTbsInitDataSegment*>(static_cast<const void *>(tbsRecord));
        (void)fprintf(fp, "type:WAL_TBS_INIT_DATA_SEGMENT_META; len:%hu; pageId=(%hu, %u); "
            "page prev walId:%lu; page prev endPlsn:%lu; page prev glsn:%lu; file prev version:%lu; glsnChangeFlag:%s; "
            "fileVersionFlag:%s; segmentType:%hhu; addedPageId=(%hu, %u); isReUsedFlag=%d; ",
            record->m_size, record->m_pageId.m_fileId, record->m_pageId.m_blockId, record->m_pagePreWalId,
            record->m_pagePrePlsn, record->m_pagePreGlsn, record->m_filePreVersion,
            record->m_flags.m_flag.glsnChangeFlag ? "true" : "false",
            record->m_flags.m_flag.containFileVersionFlag ? "true" : "false",
            static_cast<uint8>(record->segmentType), record->addedPageId.m_fileId, record->addedPageId.m_blockId,
            record->isReUsedFlag);
    } else if (tbsRecord->m_type == WAL_TBS_INIT_HEAP_SEGMENT_META) {
        const WalRecordTbsInitHeapSegment *record =
            static_cast<const WalRecordTbsInitHeapSegment*>(static_cast<const void *>(tbsRecord));
        (void)fprintf(fp, "type:WAL_TBS_INIT_HEAP_SEGMENT_META; len:%hu; pageId=(%hu, %u); "
            "page prev walId:%lu; page prev endPlsn:%lu; page prev glsn:%lu; file prev version:%lu; glsnChangeFlag:%s; "
            "fileVersionFlag:%s; "
            "addedPageId=(%hu, %u); fsmMetaPageId=(%hu, %u); assignedNodeId: %u; fsmId:%hu isReUsedFlag:%d; ",
            record->m_size, record->m_pageId.m_fileId, record->m_pageId.m_blockId, record->m_pagePreWalId,
            record->m_pagePrePlsn, record->m_pagePreGlsn, record->m_filePreVersion,
            record->m_flags.m_flag.glsnChangeFlag ? "true" : "false",
            record->m_flags.m_flag.containFileVersionFlag ? "true" : "false",
            record->addedPageId.m_fileId, record->addedPageId.m_blockId,
            record->fsmMetaPageId.m_fileId, record->fsmMetaPageId.m_blockId, record->assignedNodeId,
            record->fsmId, record->isReUsedFlag);
    } else if (tbsRecord->m_type == WAL_TBS_INIT_FSM_META) {
        const WalRecordTbsInitFreeSpaceMap *record = static_cast<const WalRecordTbsInitFreeSpaceMap*>(
            static_cast<const void*>(tbsRecord));
        (void)fprintf(fp, "type:WAL_TBS_INIT_FSM_META; len:%hu; pageId=(%hu, %u); "
            "page prev walId:%lu; page prev endPlsn:%lu; page prev glsn:%lu; file prev version:%lu; glsnChangeFlag:%s; "
            "fileVersionFlag:%s; fsmRootPageId=(%hu, %u); ",
            record->m_size, record->m_pageId.m_fileId, record->m_pageId.m_blockId, record->m_pagePreWalId,
            record->m_pagePrePlsn, record->m_pagePreGlsn, record->m_filePreVersion,
            record->m_flags.m_flag.glsnChangeFlag ? "true" : "false",
            record->m_flags.m_flag.containFileVersionFlag ? "true" : "false",
            record->fsmRootPageId.m_fileId, record->fsmRootPageId.m_blockId);
    } else if (tbsRecord->m_type == WAL_TBS_INIT_FSM_PAGE) {
        const WalRecordTbsInitFsmPage *record = static_cast<const WalRecordTbsInitFsmPage*>(
            static_cast<const void*>(tbsRecord));
        (void)fprintf(fp, "type:WAL_TBS_INIT_FSM_PAGE; len:%hu; pageId=(%hu, %u); "
            "page prev walId:%lu; page prev endPlsn:%lu; page prev glsn:%lu; file prev version:%lu; glsnChangeFlag:%s; "
            "fileVersionFlag:%s; fsmMetaPageId=(%hu, %u); upperFsmIndex=(%hu, %u, %hu); initSlotCount:%hu\n",
            record->m_size, record->m_pageId.m_fileId, record->m_pageId.m_blockId, record->m_pagePreWalId,
            record->m_pagePrePlsn, record->m_pagePreGlsn, record->m_filePreVersion,
            record->m_flags.m_flag.glsnChangeFlag ? "true" : "false",
            record->m_flags.m_flag.containFileVersionFlag ? "true" : "false",
            record->fsmMetaPageId.m_fileId, record->fsmMetaPageId.m_blockId,
            record->upperFsmIndex.page.m_fileId, record->upperFsmIndex.page.m_blockId,
            record->upperFsmIndex.slot, record->initSlotCount);
        for (uint16 i = 0; i < record->initSlotCount; ++i) {
            (void)fprintf(fp, "node:%hu pageId=(%hu, %u) listId:%hu prev:%hu next:%hu\n", i,
                        record->fsmNodeData[i].page.m_fileId, record->fsmNodeData[i].page.m_blockId,
                        record->fsmNodeData[i].listId, record->fsmNodeData[i].prev, record->fsmNodeData[i].next);
        }
    } else if (tbsRecord->m_type == WAL_TBS_INIT_EXT_META) {
        const WalRecordTbsExtentMetaInit *record = static_cast<const WalRecordTbsExtentMetaInit*>(
            static_cast<const void*>(tbsRecord));
        (void)fprintf(fp, "type:WAL_TBS_INIT_EXT_META; len:%hu; pageId=(%hu, %u); "
            "page prev walId:%lu; page prev endPlsn:%lu; page prev glsn:%lu; file prev version:%lu; glsnChangeFlag:%s; "
            "fileVersionFlag:%s; curExtSize:%hu; ",
            record->m_size, record->m_pageId.m_fileId, record->m_pageId.m_blockId, record->m_pagePreWalId,
            record->m_pagePrePlsn, record->m_pagePreGlsn, record->m_filePreVersion,
            record->m_flags.m_flag.glsnChangeFlag ? "true" : "false",
            record->m_flags.m_flag.containFileVersionFlag ? "true" : "false",
            static_cast<uint16>(record->curExtSize));
    } else {
        return false;
    }
    return true;
}

bool WalRecordTbs::DumpTbsRecordPart4(const WalRecordTbs *tbsRecord, FILE *fp)
{
    StorageAssert(tbsRecord != nullptr);
    if (tbsRecord->m_type == WAL_TBS_MODIFY_EXT_META_NEXT) {
        const WalRecordTbsExtentMetaLinkNext *record = static_cast<const WalRecordTbsExtentMetaLinkNext*>(
            static_cast<const void*>(tbsRecord));
        (void)fprintf(fp, "type:WAL_TBS_MODIFY_EXT_META_NEXT; len:%hu; pageId=(%hu, %u); "
            "page prev walId:%lu; page prev endPlsn:%lu; page prev glsn:%lu; file prev version:%lu; glsnChangeFlag:%s; "
            "fileVersionFlag:%s; nextExtMetaPageId=(%hu, %u); ",
            record->m_size, record->m_pageId.m_fileId, record->m_pageId.m_blockId, record->m_pagePreWalId,
            record->m_pagePrePlsn, record->m_pagePreGlsn, record->m_filePreVersion,
            record->m_flags.m_flag.glsnChangeFlag ? "true" : "false",
            record->m_flags.m_flag.containFileVersionFlag ? "true" : "false",
            record->nextExtMetaPageId.m_fileId, record->nextExtMetaPageId.m_blockId);
    } else if (tbsRecord->m_type == WAL_TBS_MODIFY_FSM_INDEX) {
        const WalRecordTbsUpdateFsmIndex *record = static_cast<const WalRecordTbsUpdateFsmIndex*>(
            static_cast<const void*>(tbsRecord));
        (void)fprintf(fp, "type:WAL_TBS_MODIFY_FSM_INDEX; len:%hu; pageId=(%hu, %u); "
            "page prev walId:%lu; page prev endPlsn:%lu; page prev glsn:%lu; file prev version:%lu; glsnChangeFlag:%s; "
            "fileVersionFlag:%s; upperFsmIndex=(%hu, %u, %hu); ",
            record->m_size, record->m_pageId.m_fileId, record->m_pageId.m_blockId, record->m_pagePreWalId,
            record->m_pagePrePlsn, record->m_pagePreGlsn, record->m_filePreVersion,
            record->m_flags.m_flag.glsnChangeFlag ? "true" : "false",
            record->m_flags.m_flag.containFileVersionFlag ? "true" : "false",
            record->upperFsmIndex.page.m_fileId, record->upperFsmIndex.page.m_blockId,
            record->upperFsmIndex.slot);
    } else if (tbsRecord->m_type == WAL_TBS_ADD_FSM_SLOT) {
        const WalRecordTbsAddMultiplePagesToFsmSlots *record =
            static_cast<const WalRecordTbsAddMultiplePagesToFsmSlots*>(static_cast<const void*>(tbsRecord));
        (void)fprintf(fp, "type:WAL_TBS_ADD_FSM_SLOT; len:%hu; pageId=(%hu, %u); "
            "page prev walId:%lu; page prev endPlsn:%lu; page prev glsn:%lu; file prev version:%lu; glsnChangeFlag:%s; "
            "fileVersionFlag:%s; addPageCount:%hu; firstSlotId:%hu; addListId:%hu;\n",
            record->m_size, record->m_pageId.m_fileId, record->m_pageId.m_blockId, record->m_pagePreWalId,
            record->m_pagePrePlsn, record->m_pagePreGlsn, record->m_filePreVersion,
            record->m_flags.m_flag.glsnChangeFlag ? "true" : "false",
            record->m_flags.m_flag.containFileVersionFlag ? "true" : "false",
            record->addPageCount, record->firstSlotId, record->addListId);
        for (uint16 i = 0; i < record->addPageCount; ++i) {
            (void)fprintf(fp, "%hu pageId=(%hu, %u)\n", i, record->addPageIdList[i].m_fileId,
                record->addPageIdList[i].m_blockId);
        }
    } else if (tbsRecord->m_type == WAL_TBS_MOVE_FSM_SLOT) {
        const WalRecordTbsMoveFsmSlot *record =
            static_cast<const WalRecordTbsMoveFsmSlot*>(static_cast<const void*>(tbsRecord));
        (void)fprintf(fp, "type:WAL_TBS_MOVE_FSM_SLOT; len:%hu; pageId=(%hu, %u); "
            "page prev walId:%lu; page prev endPlsn:%lu; page prev glsn:%lu; file prev version:%lu; glsnChangeFlag:%s; "
            "fileVersionFlag:%s; moveSlotId:%hu; newListId:%hu; ",
            record->m_size, record->m_pageId.m_fileId, record->m_pageId.m_blockId, record->m_pagePreWalId,
            record->m_pagePrePlsn, record->m_pagePreGlsn, record->m_filePreVersion,
            record->m_flags.m_flag.glsnChangeFlag ? "true" : "false",
            record->m_flags.m_flag.containFileVersionFlag ? "true" : "false",
            record->moveSlotId, record->newListId);
    } else if (tbsRecord->m_type == WAL_TBS_SEG_ADD_EXT) {
        const WalRecordTbsSegmentAddExtent *record =
            static_cast<const WalRecordTbsSegmentAddExtent*>(static_cast<const void*>(tbsRecord));
        (void)fprintf(fp, "type:WAL_TBS_SEG_ADD_EXT; len:%hu; pageId=(%hu, %u); "
            "page prev walId:%lu; page prev endPlsn:%lu; page prev glsn:%lu; file prev version:%lu; glsnChangeFlag:%s; "
            "fileVersionFlag:%s; extMetaPageId=(%hu, %u); extSize:%hu; extUseType:%hhu; ",
            record->m_size, record->m_pageId.m_fileId, record->m_pageId.m_blockId, record->m_pagePreWalId,
            record->m_pagePrePlsn, record->m_pagePreGlsn, record->m_filePreVersion,
            record->m_flags.m_flag.glsnChangeFlag ? "true" : "false",
            record->m_flags.m_flag.containFileVersionFlag ? "true" : "false",
            record->extMetaPageId.m_fileId, record->extMetaPageId.m_blockId, static_cast<uint16>(record->extSize),
            static_cast<uint8>(record->extUseType));
    } else {
        return false;
    }
    return true;
}

bool WalRecordTbs::DumpTbsRecordPart5(const WalRecordTbs *tbsRecord, FILE *fp)
{
    StorageAssert(tbsRecord != nullptr);
    if (tbsRecord->m_type == WAL_TBS_DATA_SEG_ADD_EXT) {
        const WalRecordTbsDataSegmentAddExtent *record = static_cast<const WalRecordTbsDataSegmentAddExtent*>
            (static_cast<const void*>(tbsRecord));
        (void)fprintf(fp, "type:WAL_TBS_DATA_SEG_ADD_EXT; len:%hu; pageId=(%hu, %u); "
            "page prev walId:%lu; page prev endPlsn:%lu; page prev glsn:%lu; file prev version:%lu; glsnChangeFlag:%s; "
            "fileVersionFlag:%s; addedPageId=(%hu, %u); extSize:%hu; isReUsedFlag=%d; ",
            record->m_size, record->m_pageId.m_fileId, record->m_pageId.m_blockId, record->m_pagePreWalId,
            record->m_pagePrePlsn, record->m_pagePreGlsn, record->m_filePreVersion,
            record->m_flags.m_flag.glsnChangeFlag ? "true" : "false",
            record->m_flags.m_flag.containFileVersionFlag ? "true" : "false",
            record->addedPageId.m_fileId, record->addedPageId.m_blockId, static_cast<uint16>(record->extSize),
            record->isReUsedFlag);
    } else if (tbsRecord->m_type == WAL_TBS_SEG_META_ASSIGN_DATA_PAGES) {
        const WalRecordTbsDataSegmentAssignDataPages *record =
            static_cast<const WalRecordTbsDataSegmentAssignDataPages*>(static_cast<const void *>(tbsRecord));
        (void)fprintf(fp, "type:WAL_TBS_SEG_META_ASSIGN_DATA_PAGES; len:%hu; pageId=(%hu, %u); "
            "page prev walId:%lu; page prev endPlsn:%lu; page prev glsn:%lu; file prev version:%lu; glsnChangeFlag:%s; "
            "fileVersionFlag:%s; addedPageId=(%hu, %u); ",
            record->m_size, record->m_pageId.m_fileId, record->m_pageId.m_blockId, record->m_pagePreWalId,
            record->m_pagePrePlsn, record->m_pagePreGlsn, record->m_filePreVersion,
            record->m_flags.m_flag.glsnChangeFlag ? "true" : "false",
            record->m_flags.m_flag.containFileVersionFlag ? "true" : "false",
            record->addedPageId.m_fileId, record->addedPageId.m_blockId);
    } else if (tbsRecord->m_type == WAL_TBS_SEG_UNLINK_EXT) {
        const WalRecordTbsSegmentUnlinkExtent *record = static_cast<const WalRecordTbsSegmentUnlinkExtent*>
            (static_cast<const void*>(tbsRecord));
        (void)fprintf(fp, "type:WAL_TBS_SEG_UNLINK_EXT; len:%hu; pageId=(%hu, %u); "
            "page prev walId:%lu; page prev endPlsn:%lu; page prev glsn:%lu; file prev version:%lu; glsnChangeFlag:%s; "
            "fileVersionFlag:%s; nextExtMetaPageId=(%hu, %u); "
            "unlinkExtMetaPageId=(%hu, %u); unlinkExtSize:%hu; extUseType:%hhu; ",
            record->m_size, record->m_pageId.m_fileId, record->m_pageId.m_blockId, record->m_pagePreWalId,
            record->m_pagePrePlsn, record->m_pagePreGlsn, record->m_filePreVersion,
            record->m_flags.m_flag.glsnChangeFlag ? "true" : "false",
            record->m_flags.m_flag.containFileVersionFlag ? "true" : "false",
            record->nextExtMetaPageId.m_fileId, record->nextExtMetaPageId.m_blockId,
            record->unlinkExtMetaPageId.m_fileId, record->unlinkExtMetaPageId.m_blockId,
            static_cast<uint16>(record->unlinkExtSize), static_cast<uint8>(record->extUseType));
    } else if (tbsRecord->m_type == WAL_TBS_SEG_META_ADD_FSM_TREE) {
        const WalRecordTbsSegMetaAddFsmTree *record =
            static_cast<const WalRecordTbsSegMetaAddFsmTree*>(static_cast<const void *>(tbsRecord));
        (void)fprintf(fp, "type:WAL_TBS_SEG_META_ADD_FSM_TREE; len:%hu; pageId=(%hu, %u); "
            "page prev walId:%lu; page prev endPlsn:%lu; page prev glsn:%lu; file prev version:%lu; glsnChangeFlag:%s; "
            "fileVersionFlag:%s; fsmMetaPageId=(%hu, %u); assignedNodeId:%u; fsmId:%hu; ",
            record->m_size, record->m_pageId.m_fileId, record->m_pageId.m_blockId, record->m_pagePreWalId,
            record->m_pagePrePlsn, record->m_pagePreGlsn, record->m_filePreVersion,
            record->m_flags.m_flag.glsnChangeFlag ? "true" : "false",
            record->m_flags.m_flag.containFileVersionFlag ? "true" : "false",
            record->fsmMetaPageId.m_fileId, record->fsmMetaPageId.m_blockId, record->assignedNodeId,
            record->fsmId);
    } else {
        return false;
    }
    return true;
}

bool WalRecordTbs::DumpTbsRecordPart6(const WalRecordTbs *tbsRecord, FILE *fp)
{
    StorageAssert(tbsRecord != nullptr);
    if (tbsRecord->m_type == WAL_TBS_SEG_META_RECYCLE_FSM_TREE) {
        const WalRecordTbsSegMetaRecycleFsmTree *record =
            static_cast<const WalRecordTbsSegMetaRecycleFsmTree*>(static_cast<const void *>(tbsRecord));
        (void)fprintf(fp, "type:WAL_TBS_SEG_META_RECYCLE_FSM_TREE; len:%hu; pageId=(%hu, %u); "
            "page prev walId:%lu; page prev endPlsn:%lu; page prev glsn:%lu; file prev version:%lu; glsnChangeFlag:%s; "
            "fileVersionFlag:%s; fsmMetaPageId=(%hu, %u); assignedNodeId:%u; fsmId:%hu; ",
            record->m_size, record->m_pageId.m_fileId, record->m_pageId.m_blockId, record->m_pagePreWalId,
            record->m_pagePrePlsn, record->m_pagePreGlsn, record->m_filePreVersion,
            record->m_flags.m_flag.glsnChangeFlag ? "true" : "false",
            record->m_flags.m_flag.containFileVersionFlag ? "true" : "false",
            record->fsmMetaPageId.m_fileId, record->fsmMetaPageId.m_blockId, record->assignedNodeId,
            record->fsmId);
    } else if (tbsRecord->m_type == WAL_TBS_SEG_META_ADJUST_DATA_PAGES_INFO) {
        const WalRecordTbsSegMetaAdjustDataPagesInfo *record =
            static_cast<const WalRecordTbsSegMetaAdjustDataPagesInfo*>(static_cast<const void *>(tbsRecord));
        (void)fprintf(fp, "type:WAL_TBS_SEG_META_ADJUST_DATA_PAGES_INFO; len:%hu; pageId=(%hu, %u); "
            "page prev walId:%lu; page prev endPlsn:%lu; page prev glsn:%lu; file prev version:%lu; glsnChangeFlag:%s; "
            "fileVersionFlag:%s; "
            "firstDataPageId=(%hu, %u); lastDataPageId=(%hu, %u); addedPageId=(%hu, %u); totalDataPageCount:%lu; ",
            record->m_size, record->m_pageId.m_fileId, record->m_pageId.m_blockId, record->m_pagePreWalId,
            record->m_pagePrePlsn, record->m_pagePreGlsn, record->m_filePreVersion,
            record->m_flags.m_flag.glsnChangeFlag ? "true" : "false",
            record->m_flags.m_flag.containFileVersionFlag ? "true" : "false",
            record->firstDataPageId.m_fileId, record->firstDataPageId.m_blockId, record->lastDataPageId.m_fileId,
            record->lastDataPageId.m_blockId, record->addedPageId.m_fileId, record->addedPageId.m_blockId,
            record->totalDataPageCount);
    } else if (tbsRecord->m_type == WAL_TBS_FSM_META_UPDATE_FSM_TREE) {
        const WalRecordTbsFsmMetaUpdateFsmTree *record =
            static_cast<const WalRecordTbsFsmMetaUpdateFsmTree*>(static_cast<const void *>(tbsRecord));
        (void)fprintf(fp, "type:WAL_TBS_FSM_META_UPDATE_FSM_TREE; len:%hu; pageId=(%hu, %u); "
            "page prev walId:%lu; page prev endPlsn:%lu; page prev glsn:%lu; file prev version:%lu; glsnChangeFlag:%s; "
            "fileVersionFlag:%s; numFsmLevels:%hhu; fsmExtents=(%lu, (%hu, %u), (%hu, %u)); "
            "usedFsmPageId=(%hu, %u); lastFsmPageId=(%hu, %u); curFsmExtMetaPageId=(%hu, %u);\n",
            record->m_size, record->m_pageId.m_fileId, record->m_pageId.m_blockId, record->m_pagePreWalId,
            record->m_pagePrePlsn, record->m_pagePreGlsn, record->m_filePreVersion,
            record->m_flags.m_flag.glsnChangeFlag ? "true" : "false",
            record->m_flags.m_flag.containFileVersionFlag ? "true" : "false",
            record->numFsmLevels, record->fsmExtents.count, record->fsmExtents.first.m_fileId,
            record->fsmExtents.first.m_blockId, record->fsmExtents.last.m_fileId,
            record->fsmExtents.last.m_blockId, record->usedFsmPageId.m_fileId, record->usedFsmPageId.m_blockId,
            record->lastFsmPageId.m_fileId, record->lastFsmPageId.m_blockId, record->curFsmExtMetaPageId.m_fileId,
            record->curFsmExtMetaPageId.m_blockId);
        for (uint i = 0; i < HEAP_MAX_MAP_LEVEL; ++i) {
            (void)fprintf(fp, "%u mapCount:%lu currMap=(%hu, %u)\n",
                        i, record->mapCount[i], record->currMap[i].m_fileId, record->currMap[i].m_blockId);
        }
    } else if (tbsRecord->m_type == WAL_TBS_FSM_META_UPDATE_NUM_USED_PAGES) {
        const WalRecordTbsFsmMetaUpdateNumUsedPages *record =
            static_cast<const WalRecordTbsFsmMetaUpdateNumUsedPages*>(static_cast<const void *>(tbsRecord));
        (void)fprintf(fp, "type:WAL_TBS_FSM_META_UPDATE_NUM_USED_PAGES; len:%hu; pageId=(%hu, %u); "
            "page prev walId:%lu; page prev endPlsn:%lu; page prev glsn:%lu; file prev version:%lu; glsnChangeFlag:%s; "
            "fileVersionFlag:%s; numUsedPages:%lu; ",
            record->m_size, record->m_pageId.m_fileId, record->m_pageId.m_blockId, record->m_pagePreWalId,
            record->m_pagePrePlsn, record->m_pagePreGlsn, record->m_filePreVersion,
            record->m_flags.m_flag.glsnChangeFlag ? "true" : "false",
            record->m_flags.m_flag.containFileVersionFlag ? "true" : "false",
            record->numUsedPages);
    } else {
        return false;
    }
    return true;
}

bool WalRecordTbs::DumpTbsRecordPart7(const WalRecordTbs *tbsRecord, FILE *fp)
{
    StorageAssert(tbsRecord != nullptr);
    if (tbsRecord->m_type == WAL_TBS_FSM_META_UPDATE_EXTENSION_STAT) {
        const WalRecordTbsFsmMetaUpdateExtensionStat *record =
            static_cast<const WalRecordTbsFsmMetaUpdateExtensionStat*>(static_cast<const void *>(tbsRecord));
        (void)fprintf(fp, "type:WAL_TBS_FSM_META_UPDATE_EXTENSION_STAT; len:%hu; pageId=(%hu, %u); "
            "page prev walId:%lu; page prev endPlsn:%lu; page prev glsn:%lu; file prev version:%lu; glsnChangeFlag:%s; "
            "fileVersionFlag:%s; numTotalPages:%lu; extendCoefficient:%hu; ",
            record->m_size, record->m_pageId.m_fileId, record->m_pageId.m_blockId, record->m_pagePreWalId,
            record->m_pagePrePlsn, record->m_pagePreGlsn, record->m_filePreVersion,
            record->m_flags.m_flag.glsnChangeFlag ? "true" : "false",
            record->m_flags.m_flag.containFileVersionFlag ? "true" : "false",
            record->numTotalPages, record->extendCoefficient);
    } else if (tbsRecord->m_type == WAL_TBS_INIT_ONE_DATA_PAGE) {
        const WalRecordTbsInitOneDataPage *record =
            static_cast<const WalRecordTbsInitOneDataPage*>(static_cast<const void *>(tbsRecord));
        (void)fprintf(fp, "type:WAL_TBS_INIT_ONE_DATA_PAGE; len:%hu; pageId=(%hu, %u); "
            "page prev walId:%lu; page prev endPlsn:%lu; page prev glsn:%lu; file prev version:%lu; glsnChangeFlag:%s; "
            "fileVersionFlag:%s; dataPageType:%hhu; curFsmIndex:((%hu, %u), %hu); ",
            record->m_size, record->m_pageId.m_fileId, record->m_pageId.m_blockId, record->m_pagePreWalId,
            record->m_pagePrePlsn, record->m_pagePreGlsn, record->m_filePreVersion,
            record->m_flags.m_flag.glsnChangeFlag ? "true" : "false",
            record->m_flags.m_flag.containFileVersionFlag ? "true" : "false",
            static_cast<uint8>(record->dataPageType), record->curFsmIndex.page.m_fileId,
            record->curFsmIndex.page.m_blockId, record->curFsmIndex.slot);
    } else if (tbsRecord->m_type == WAL_TBS_CREATE_TABLESPACE) {
        const WalRecordTbsCreateTablespace *record =
            static_cast<const WalRecordTbsCreateTablespace*>(static_cast<const void *>(tbsRecord));
        (void)fprintf(fp, "type:WAL_TBS_CREATE_TABLESPACE; tablespaceId:%hu; tablespace tbsMaxSize:%lu; ZoneId:%u, "
            "slotId:%lu; prev reuse version:%lu", record->tablespaceId, record->tbsMaxSize,
            static_cast<uint32>(record->ddlXid.m_zoneId), record->ddlXid.m_logicSlotId, record->preReuseVersion);
    } else if (tbsRecord->m_type == WAL_TBS_CREATE_DATA_FILE) {
        const WalRecordTbsCreateDataFile *record =
            static_cast<const WalRecordTbsCreateDataFile*>(static_cast<const void *>(tbsRecord));
        (void)fprintf(fp, "type:WAL_TBS_CREATE_DATA_FILE; tablespaceId:%hu; fileId:%hu; ZoneId:%u, slotId:%lu;"
            "prev reuse version:%lu; extentSize:%hu",
            record->tablespaceId, record->fileId, static_cast<uint32>(record->ddlXid.m_zoneId),
            record->ddlXid.m_logicSlotId, record->preReuseVersion, record->extentSize);
    } else if (tbsRecord->m_type == WAL_TBS_ADD_FILE_TO_TABLESPACE) {
        const WalRecordTbsAddFileToTbs *record =
            static_cast<const WalRecordTbsAddFileToTbs*>(static_cast<const void *>(tbsRecord));
        (void)fprintf(fp, "type:WAL_TBS_ADD_FILE_TO_TABLESPACE; tablespaceId:%hu; fileId:%hu; ZoneId:%u, slotId:%lu;"
            "prev reuse version:%lu; hwm:%hu; slotId:%hu",
            record->tablespaceId, record->fileId, static_cast<uint32>(record->ddlXid.m_zoneId),
            record->ddlXid.m_logicSlotId, record->preReuseVersion, record->hwm, record->slotId);
    } else if (tbsRecord->m_type == WAL_TBS_DROP_TABLESPACE) {
        const WalRecordTbsDropTablespace *record =
            static_cast<const WalRecordTbsDropTablespace*>(static_cast<const void *>(tbsRecord));
        (void)fprintf(fp,
                      "type:WAL_TBS_DROP_TABLESPACE; tablespaceId:%hu; ZoneId:%u, slotId:%lu; prev reuse version:%lu;"
                      " hwm:%hu",
                      record->tablespaceId, static_cast<uint32>(record->ddlXid.m_zoneId), record->ddlXid.m_logicSlotId,
                      record->preReuseVersion, record->hwm);
    } else if (tbsRecord->m_type == WAL_TBS_DROP_DATA_FILE) {
        const WalRecordTbsDropDataFile *record =
            static_cast<const WalRecordTbsDropDataFile *>(static_cast<const void *>(tbsRecord));
        (void)fprintf(fp,
                      "type:WAL_TBS_DROP_DATA_FILE; tablespaceId:%hu; fileId:%hu; ZoneId:%u, slotId:%lu;"
                      " prev reuse version:%lu; hwm:%hu; slotId:%hu",
                      record->tablespaceId, record->fileId, static_cast<uint32>(record->ddlXid.m_zoneId),
                      record->ddlXid.m_logicSlotId, record->preReuseVersion, record->hwm, record->slotId);
    } else if (tbsRecord->m_type == WAL_TBS_ALTER_TABLESPACE) {
        const WalRecordTbsAlterTablespace *record =
            static_cast<const WalRecordTbsAlterTablespace *>(static_cast<const void *>(tbsRecord));
        (void)fprintf(fp,
                      "type:WAL_TBS_ALTER_TABLESPACE; tablespaceId:%hu; tablespace maxSize:%lu; ZoneId:%u, slotId:%lu;"
                      "prev reuse version:%lu",
                      record->tablespaceId, record->tbsMaxSize, static_cast<uint32>(record->ddlXid.m_zoneId),
                      record->ddlXid.m_logicSlotId, record->preReuseVersion);
    } else {
        return false;
    }
    return true;
}

void WalRecordTbs::DumpTbsRecord(const WalRecordTbs *tbsRecord, FILE *fp)
{
    if (DumpTbsRecordPart1(tbsRecord, fp) || DumpTbsRecordPart2(tbsRecord, fp) ||
        DumpTbsRecordPart3(tbsRecord, fp) || DumpTbsRecordPart4(tbsRecord, fp) ||
        DumpTbsRecordPart5(tbsRecord, fp) || DumpTbsRecordPart6(tbsRecord, fp) ||
        DumpTbsRecordPart7(tbsRecord, fp)) {
        return;
    }
    tbsRecord->Dump(fp);
}

void WalRecordTbsInitBitmapMetaPage::Redo(void *page) const
{
    auto *bitmapMetaPage = static_cast<TbsBitmapMetaPage*>(page);
    bitmapMetaPage->InitBitmapMetaPage(m_pageId, totalBlockCount, extentSize);
    ErrLog(DSTORE_LOG, MODULE_SEGMENT, ErrMsg("WAL_TBS_INIT_BITMAP_META_PAGE: walID %lu pageId(%hu, %u)"
        "(plsn %lu glsn %lu) totalBlockCount %hhu extentSize %hu", m_pagePreWalId, m_pageId.m_fileId,
        m_pageId.m_blockId, m_pagePrePlsn, m_pagePreGlsn, totalBlockCount, extentSize));
}

void WalRecordTbsInitTbsFileMetaPage::Redo(void *page) const
{
    auto *tbsFileMetaPage = static_cast<TbsFileMetaPage*>(page);
    tbsFileMetaPage->InitTbsFileMetaPage(m_pageId, m_reuseVersion, m_ddlXid);
    ErrLog(DSTORE_LOG, MODULE_SEGMENT, ErrMsg("WAL_TBS_INIT_TBS_FILE_META_PAGE: walID %lu pageId(%hu, %u)"
        "(plsn %lu glsn %lu) m_reuseVersion %lu m_ddlXid %lu", m_pagePreWalId, m_pageId.m_fileId, m_pageId.m_blockId,
        m_pagePrePlsn, m_pagePreGlsn, m_reuseVersion, m_ddlXid.m_placeHolder));
}

void WalRecordTbsUpdateTbsFileMetaPage::Redo(void *page) const
{
    auto *tbsFileMetaPage = static_cast<TbsFileMetaPage*>(page);
    tbsFileMetaPage->hwm = hwm;
    tbsFileMetaPage->oid = oid;
    ErrLog(DSTORE_LOG, MODULE_SEGMENT, ErrMsg("WAL_TBS_UPDATE_TBS_FILE_META_PAGE: walID %lu pageId(%hu, %u)"
        "(plsn %lu glsn %lu) hwm %u oid %u", m_pagePreWalId, m_pageId.m_fileId, m_pageId.m_blockId,
        m_pagePrePlsn, m_pagePreGlsn, hwm, oid));
}

void WalRecordTbsInitTbsSpaceMetaPage::Redo(void *page) const
{
    auto *tbsSpaceMetaPage = static_cast<TbsSpaceMetaPage*>(page);
    tbsSpaceMetaPage->InitTbsSpaceMetaPage(m_pageId);
    ErrLog(DSTORE_LOG, MODULE_SEGMENT, ErrMsg("WAL_TBS_INIT_TBS_SPACE_META_PAGE: walID %lu pageId(%hu, %u)"
        "(plsn %lu glsn %lu)", m_pagePreWalId, m_pageId.m_fileId, m_pageId.m_blockId,
        m_pagePrePlsn, m_pagePreGlsn));
}

void WalRecordTbsInitBitmapPages::Redo(WalRecordRedoContext *redoCtx) const
{
    if (pageType != PageType::TBS_BITMAP_PAGE_TYPE) {
        ErrLog(DSTORE_PANIC, MODULE_TABLESPACE, ErrMsg("Invalid page type"));
    }
    BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
    PageId curPageId = firstPageId;
    PageId curDataPageId = firstDataPageId;
    for (uint32 i = 0; i < pageCount; i++) {
        BufferDesc *bitmapPageBufferDesc = bufMgr->RecoveryRead(redoCtx->pdbId, curPageId);
        uint64 loopCount = 0;
        constexpr uint32 WAIT_COUNT_FOR_REPORT_WARNING = 1000;
        constexpr uint32 WAIT_BUFFER_SLEEP_MILLISECONDS = 10;
        while (bitmapPageBufferDesc == INVALID_BUFFER_DESC) {
            if (WalUtils::GetFileVersion(redoCtx->pdbId, curPageId.m_fileId) <= filePreVersion) {
                if (++loopCount % WAIT_COUNT_FOR_REPORT_WARNING == 0) {
                    ErrLog(DSTORE_ERROR, MODULE_WAL,
                           ErrMsg("WalRecordTbsInitBitmapPages get invalid bufferDesc, pageId(%hu, %u), lsn: %lu, %lu",
                                  curPageId.m_fileId, curPageId.m_blockId, redoCtx->walId, redoCtx->recordEndPlsn));
                }
                GaussUsleep(WAIT_BUFFER_SLEEP_MILLISECONDS);
                bitmapPageBufferDesc = bufMgr->RecoveryRead(redoCtx->pdbId, curPageId);
            } else {
                continue;
            }
        }
        TbsBitmapPage *bitmapPage = static_cast<TbsBitmapPage *>(bitmapPageBufferDesc->GetPage());

        uint64 recordGlsn = preWalPointer[i].walId != redoCtx->walId ? preWalPointer[i].glsn + 1 :
                            preWalPointer[i].glsn;
        WalRecordLsnInfo recordLsnInfo = {redoCtx->walId, redoCtx->recordEndPlsn, recordGlsn};
        WalRecordReplayType replayType = WalRecovery::GetWalRecordReplayType(
            bitmapPageBufferDesc->GetBufferTag(), bitmapPage, preWalPointer[i], recordLsnInfo);
        if (replayType == WalRecordReplayType::REPLAYABLE) {
            bitmapPage->InitBitmapPage(curPageId, curDataPageId);
            /* Write back wal id, plsn, glsn */
            bitmapPage->m_header.m_walId = redoCtx->walId;
            bitmapPage->m_header.m_glsn = recordGlsn;
            bitmapPage->m_header.m_plsn = redoCtx->recordEndPlsn;
            (void)bufMgr->MarkDirty(bitmapPageBufferDesc);
        }
        bufMgr->UnlockAndRelease(bitmapPageBufferDesc);
        curPageId.m_blockId++;
        curDataPageId.m_blockId += DF_BITMAP_BIT_CNT * static_cast<uint16>(extentSize);
    }
}

void WalRecordTbsBitmapSetBit::Redo(void *page) const
{
    auto *bitmapPage = static_cast<TbsBitmapPage *>(page);
    if (value != 0) {
        if (unlikely(!bitmapPage->TestBitZero(startBitPos)
            || bitmapPage->allocatedExtentCount + 1 != allocatedExtentCount)) {
            ErrLog(DSTORE_PANIC, MODULE_SEGMENT, ErrMsg("Bitmap page (%hu, %u) set bit %hu error! "
                "Allocated extent count %hu vs %hu."
                "Page (walID %lu plsn %lu glsn %lu) WAL (walID %lu plsn %lu glsn %lu)",
                bitmapPage->m_header.m_myself.m_fileId, bitmapPage->m_header.m_myself.m_blockId, startBitPos,
                bitmapPage->allocatedExtentCount, allocatedExtentCount, bitmapPage->m_header.m_walId,
                bitmapPage->m_header.m_plsn, bitmapPage->m_header.m_glsn, m_pagePreWalId, m_pagePrePlsn,
                m_pagePreGlsn));
        }
        bitmapPage->SetByBit(startBitPos);
    } else {
        if (unlikely(bitmapPage->TestBitZero(startBitPos)
            || bitmapPage->allocatedExtentCount != allocatedExtentCount + 1)) {
            ErrLog(DSTORE_PANIC, MODULE_SEGMENT, ErrMsg("Bitmap page (%hu, %u) unset bit %hu error! "
                "Allocated extent count %hu vs %hu. "
                "Page (walID %lu plsn %lu glsn %lu) WAL (walID %lu plsn %lu glsn %lu)",
                bitmapPage->m_header.m_myself.m_fileId, bitmapPage->m_header.m_myself.m_blockId, startBitPos,
                bitmapPage->allocatedExtentCount, allocatedExtentCount, bitmapPage->m_header.m_walId,
                bitmapPage->m_header.m_plsn, bitmapPage->m_header.m_glsn, m_pagePreWalId, m_pagePrePlsn,
                m_pagePreGlsn));
        }
        bitmapPage->UnsetByBit(startBitPos);
    }
    bitmapPage->allocatedExtentCount = allocatedExtentCount;
}

void WalRecordTbsAddBitmapPages::Redo(void *page) const
{
    auto *bitmapMetaPage = static_cast<TbsBitmapMetaPage*>(page);
    bitmapMetaPage->groupCount = groupCount;
    bitmapMetaPage->validOffset = validOffset;
    bitmapMetaPage->bitmapPagesPerGroup = groupPageCount;
    TbsBitMapGroup *group = &(bitmapMetaPage->bitmapGroups[groupIndex]);
    group->firstBitmapPageId = groupFirstPage;
    group->firstFreePageNo = groupFreePage;
}

void WalRecordTbsUpdateFirstFreeBitmapPageId::Redo(void *page) const
{
    auto *bitmapMetaPage = static_cast<TbsBitmapMetaPage*>(page);
    TbsBitMapGroup *group = &(bitmapMetaPage->bitmapGroups[groupIndex]);
    group->firstFreePageNo = firstFreePageNo;
}

void WalRecordTbsExtendFile::Redo(PdbId pdbId, void *page) const
{
    StorageAssert(g_storageInstance->GetPdb(pdbId) != nullptr);
    VFSAdapter *vfsAdapter = g_storageInstance->GetPdb(pdbId)->GetVFS();
    StorageAssert(vfsAdapter != nullptr);
    int64 fileSize = vfsAdapter->GetSize(fileId);
    if (fileSize < 0) {
        ErrLog(DSTORE_PANIC, MODULE_TABLESPACE,
               ErrMsg("Failed to get size of fileId %hu during extend file redo.", fileId));
    }
    int64 targetSize = totalBlockCount * BLCKSZ;
    if (fileSize < targetSize) {
        if (STORAGE_FUNC_FAIL(vfsAdapter->Extend(fileId, targetSize))) {
            ErrLog(DSTORE_PANIC, MODULE_TABLESPACE,
                   ErrMsg("Extend file (fileId = %hu, targetSize = %ld) failed", fileId, targetSize));
        }
    }
    auto *bitmapMetaPage = static_cast<TbsBitmapMetaPage*>(page);
    bitmapMetaPage->totalBlockCount = totalBlockCount;
    ErrLog(DSTORE_LOG, MODULE_WAL,
           ErrMsg("[PDB:%u] finish extend file replay, fileId %u, blockcount %lu", pdbId, fileId, totalBlockCount));
}

void WalRecordTbsInitUndoSegment::Redo(void *page) const
{
    /* Currently, only Undo uses Segments.An independent UndoSegment is added later. */
    (static_cast<UndoSegmentMetaPage *>(page))->InitUndoSegmentMetaPage(m_pageId, plsn, glsn);
}

void WalRecordTbsInitDataSegment::Redo(void *page) const
{
    DataSegmentMetaPage *segMetaPage =  static_cast<DataSegmentMetaPage *>(page);
    if (STORAGE_FUNC_FAIL(segMetaPage->InitDataSegmentMetaPage(segmentType, m_pageId, FIRST_EXT_SIZE, plsn, glsn))) {
        ErrLog(DSTORE_PANIC, MODULE_TABLESPACE,
            ErrMsg("Init DataSegment with invalid type %u.", static_cast<uint32>(segmentType)));
    }
    segMetaPage->InitSegmentInfo(addedPageId, isReUsedFlag);
}

void WalRecordTbsInitHeapSegment::Redo(void *page) const
{
    HeapSegmentMetaPage *segMetaPage =  static_cast<HeapSegmentMetaPage *>(page);
    segMetaPage->InitHeapSegmentMetaPage(SegmentType::HEAP_SEGMENT_TYPE, m_pageId, FIRST_EXT_SIZE, plsn, glsn);
    segMetaPage->InitSegmentInfo(addedPageId, false);
    StorageAssert(segMetaPage->numFsms == fsmId);
    segMetaPage->fsmInfos[fsmId].fsmMetaPageId = fsmMetaPageId;
    segMetaPage->fsmInfos[fsmId].assignedNodeId = assignedNodeId;
    segMetaPage->numFsms += 1;
    segMetaPage->lastExtentIsReused = isReUsedFlag;
}
void WalRecordTbsInitFreeSpaceMap::Redo(void *page) const
{
    FreeSpaceMapMetaPage *fsmMetaPage = reinterpret_cast<FreeSpaceMapMetaPage *>(page);
    fsmMetaPage->InitFreeSpaceMapMetaPage(m_pageId, fsmRootPageId, accessTimestamp);
}

void WalRecordTbsExtentMetaInit::Redo(void *page) const
{
    SegExtentMetaPage *extMetaPage = reinterpret_cast<SegExtentMetaPage *>(page);
    extMetaPage->InitSegExtentMetaPage(m_pageId, static_cast<ExtentSize>(curExtSize),
                                       PageType::TBS_EXTENT_META_PAGE_TYPE);
}

void WalRecordTbsExtentMetaLinkNext::Redo(void *page) const
{
    SegExtentMetaPage *extMetaPage = reinterpret_cast<SegExtentMetaPage *>(page);
    extMetaPage->LinkNextExtent(nextExtMetaPageId);
}

void WalRecordTbsMoveFsmSlot::Redo(void *page) const
{
    FsmPage *fsmPage = reinterpret_cast<FsmPage *>(page);
    fsmPage->MoveNode(moveSlotId, newListId);
}

void WalRecordTbsAddMultiplePagesToFsmSlots::Redo(void *page) const
{
    FsmPage *fsmPage = reinterpret_cast<FsmPage *>(page);
    StorageAssert(fsmPage->fsmPageHeader.hwm == firstSlotId);
    PageId *pageIdList = static_cast<PageId *>(static_cast<void *>(DstorePalloc(addPageCount * sizeof(PageId))));
    if (pageIdList == nullptr) {
        ErrLog(DSTORE_PANIC, MODULE_TABLESPACE, ErrMsg("Alloc memory failed."));
    }
    for (uint16 i = 0; i < addPageCount; i++) {
        pageIdList[i] = addPageIdList[i];
    }
    (void)fsmPage->AddMultiNode(pageIdList, addPageCount, addListId);
    DstorePfreeExt(pageIdList);
}

void WalRecordTbsInitFsmPage::Redo(void *page) const
{
    FsmPage *fsmPage = reinterpret_cast<FsmPage *>(page);
    fsmPage->InitFsmPage(m_pageId, fsmMetaPageId, upperFsmIndex);
    for (uint16 i = fsmPage->fsmPageHeader.hwm; i < fsmPage->fsmPageHeader.hwm + initSlotCount; ++i) {
        *(fsmPage->FsmNodePtr(i)) = fsmNodeData[i];
        uint16 listId = fsmNodeData[i].listId;
        fsmPage->FsmListPtr(listId)->first = i;
        fsmPage->FsmListPtr(listId)->count += 1;
    }
    fsmPage->fsmPageHeader.hwm += initSlotCount;
    StorageReleasePanic(fsmPage->fsmPageHeader.hwm > FSM_MAX_HWM, MODULE_SEGMENT,
        ErrMsg("Fsm page high water mark is invalid, initSlotCount = %d, hwm = %u.",
            initSlotCount, fsmPage->fsmPageHeader.hwm));
}

void WalRecordTbsUpdateFsmIndex::Redo(void *page) const
{
    FsmPage *fsmPage = reinterpret_cast<FsmPage *>(page);
    fsmPage->fsmPageHeader.upperIndex = upperFsmIndex;
}

void WalRecordTbsSegmentAddExtent::Redo(void *page) const
{
    if (extUseType == EXT_FSM_PAGE_TYPE) {
        FreeSpaceMapMetaPage *freeSpaceMapMetaPage = reinterpret_cast<FreeSpaceMapMetaPage *>(page);
        freeSpaceMapMetaPage->AddFsmExtent(extMetaPageId);
    } else {
        SegmentMetaPage *metaPage = reinterpret_cast<SegmentMetaPage *>(page);
        metaPage->LinkExtent(extMetaPageId, static_cast<ExtentSize>(extSize));
    }
}

void WalRecordTbsDataSegmentAddExtent::Redo(void *page) const
{
    DataSegmentMetaPage *metaPage = reinterpret_cast<DataSegmentMetaPage *>(page);
    metaPage->LinkExtent(addedPageId, static_cast<ExtentSize>(extSize));
    metaPage->addedPageId = addedPageId;
    metaPage->extendedPageId = {addedPageId.m_fileId, addedPageId.m_blockId + static_cast<uint16>(extSize) - 1};
    metaPage->lastExtentIsReused = isReUsedFlag;
}

void WalRecordTbsDataSegmentAssignDataPages::Redo(void *page) const
{
    DataSegmentMetaPage *metaPage = reinterpret_cast<DataSegmentMetaPage *>(page);
    metaPage->addedPageId = addedPageId;
}

void WalRecordTbsSegmentUnlinkExtent::Redo(void *page) const
{
    if (extUseType == EXT_DATA_PAGE_TYPE) {
        FreeSpaceMapMetaPage *fsmMetaPage = reinterpret_cast<FreeSpaceMapMetaPage *>(page);
        fsmMetaPage->UnlinkExtent(nextExtMetaPageId);
    } else {
        SegmentMetaPage *segMetaPage = reinterpret_cast<SegmentMetaPage *>(page);
        segMetaPage->UnlinkExtent(nextExtMetaPageId, unlinkExtMetaPageId, static_cast<uint16>(unlinkExtSize));
    }
}

void WalRecordTbsSegMetaAddFsmTree::Redo(void *page) const
{
    HeapSegmentMetaPage *segMetaPage = static_cast<HeapSegmentMetaPage *>(page);
    StorageAssert(segMetaPage->numFsms == fsmId);
    segMetaPage->fsmInfos[fsmId].fsmMetaPageId = fsmMetaPageId;
    segMetaPage->fsmInfos[fsmId].assignedNodeId = assignedNodeId;
    segMetaPage->numFsms += 1;
}

void WalRecordTbsSegMetaRecycleFsmTree::Redo(void *page) const
{
    HeapSegmentMetaPage *segMetaPage = static_cast<HeapSegmentMetaPage *>(page);
    StorageAssert(segMetaPage->numFsms >= fsmId);
    StorageAssert(segMetaPage->fsmInfos[fsmId].fsmMetaPageId == fsmMetaPageId);
    segMetaPage->fsmInfos[fsmId].assignedNodeId = assignedNodeId;
}

void WalRecordTbsSegMetaAdjustDataPagesInfo::Redo(void *page) const
{
    DataSegmentMetaPage *segMetaPage = reinterpret_cast<DataSegmentMetaPage *>(page);
    segMetaPage->dataFirst = firstDataPageId;
    segMetaPage->dataLast = lastDataPageId;
    segMetaPage->dataBlockCount = totalDataPageCount;
    segMetaPage->addedPageId = addedPageId;
}

void WalRecordTbsFsmMetaUpdateFsmTree::Redo(void *page) const
{
    FreeSpaceMapMetaPage *fsmMetaPage = reinterpret_cast<FreeSpaceMapMetaPage *>(page);
    fsmMetaPage->fsmExtents = fsmExtents;
    fsmMetaPage->numFsmLevels = numFsmLevels;
    fsmMetaPage->usedFsmPage = usedFsmPageId;
    fsmMetaPage->lastFsmPage = lastFsmPageId;
    fsmMetaPage->curFsmExtMetaPageId = curFsmExtMetaPageId;
    for (int i = 0; i < HEAP_MAX_MAP_LEVEL; i++) {
        fsmMetaPage->mapCount[i] = mapCount[i];
        fsmMetaPage->currMap[i] = currMap[i];
    }
}

void WalRecordTbsFsmMetaUpdateNumUsedPages::Redo(void *page) const
{
    FreeSpaceMapMetaPage *fsmMetaPage = reinterpret_cast<FreeSpaceMapMetaPage *>(page);
    StorageAssert(numUsedPages > fsmMetaPage->numUsedPages);
    fsmMetaPage->numUsedPages = numUsedPages;
}

void WalRecordTbsFsmMetaUpdateExtensionStat::Redo(void *page) const
{
    FreeSpaceMapMetaPage *fsmMetaPage = reinterpret_cast<FreeSpaceMapMetaPage *>(page);
    StorageAssert(numTotalPages > fsmMetaPage->numTotalPages);
    StorageAssert(extendCoefficient <= MAX_FSM_EXTEND_COEFFICIENT);
    fsmMetaPage->numTotalPages = numTotalPages;
    fsmMetaPage->extendCoefficient = extendCoefficient;
}

void WalRecordTbsInitOneDataPage::Redo(BufferDesc *bufDesc) const
{
    /* Init data page with fsm index */
    if (dataPageType == PageType::HEAP_PAGE_TYPE) {
        HeapPage::InitHeapPage(bufDesc, m_pageId, curFsmIndex);
    } else {
        BtrPage::InitBtrPage(bufDesc, m_pageId, curFsmIndex);
    }
}

void WalRecordTbsInitOneBitmapPage::Redo(void *page) const
{
    TbsBitmapPage *bitmapPage = STATIC_CAST_PTR_TYPE(page, TbsBitmapPage *);
    bitmapPage->InitBitmapPage(m_pageId, curDataPageId);
}

/**
 * Logic wal redo for multiple data pages initialization
 */
void WalRecordTbsInitDataPages::Redo(WalRecordRedoContext *redoCtx) const
{
    BufMgrInterface *bufMgr = g_storageInstance->GetBufferMgr();
    if (dataPageType != PageType::HEAP_PAGE_TYPE && dataPageType != PageType::INDEX_PAGE_TYPE) {
        ErrLog(DSTORE_PANIC, MODULE_TABLESPACE, ErrMsg("Invalid data page type"));
    }

    /* Read each data page and initialize */
    FileId fileId = firstDataPageId.m_fileId;
    BlockNumber firstBlockId = firstDataPageId.m_blockId;
    PageId fsmPageId = firstFsmIndex.page;
    uint16 firstSlotId = firstFsmIndex.slot;
    for (uint16 i = 0; i < dataPageCount; ++i) {
        PageId curDataPageId = {fileId, firstBlockId + i};
        BufferDesc *bufDesc = bufMgr->RecoveryRead(redoCtx->pdbId, curDataPageId);
        uint64 loopCount = 0;
        constexpr uint32 WAIT_COUNT_FOR_REPORT_WARNING = 1000;
        constexpr uint32 WAIT_BUFFER_SLEEP_MILLISECONDS = 10;
        while (bufDesc == INVALID_BUFFER_DESC) {
            if (WalUtils::GetFileVersion(redoCtx->pdbId, curDataPageId.m_fileId) <= filePreVersion) {
                if (++loopCount % WAIT_COUNT_FOR_REPORT_WARNING == 0) {
                    ErrLog(DSTORE_ERROR, MODULE_WAL,
                           ErrMsg("WalRecordTbsInitBitmapPages get invalid bufferDesc, pageId(%hu, %u), lsn: %lu, %lu",
                                  curDataPageId.m_fileId, curDataPageId.m_blockId, redoCtx->walId,
                                  redoCtx->recordEndPlsn));
                }
                GaussUsleep(WAIT_BUFFER_SLEEP_MILLISECONDS);
                bufDesc = bufMgr->RecoveryRead(redoCtx->pdbId, curDataPageId);
            } else {
                continue;
            }
        }
        DataPage *dataPage = static_cast<DataPage *>(bufDesc->GetPage());
        uint64 recordGlsn = preWalPointer[i].walId != redoCtx->walId ? preWalPointer[i].glsn + 1 :
                            preWalPointer[i].glsn;
        WalRecordLsnInfo recordLsnInfo = {redoCtx->walId, redoCtx->recordEndPlsn, recordGlsn};
        WalRecordReplayType replayType =
            WalRecovery::GetWalRecordReplayType(bufDesc->GetBufferTag(), dataPage, preWalPointer[i], recordLsnInfo);
        if (replayType == WalRecordReplayType::NO_NEED_TO_REPLAY) {
            bufMgr->UnlockAndRelease(bufDesc);
            continue;
        }
        if (replayType == WalRecordReplayType::NOT_REPLAYABLE) {
            bufMgr->UnlockAndRelease(bufDesc);
            continue;
        }
        /* Init data page with fsm index */
        uint16 curSlotId = firstSlotId + i;
        FsmIndex curFsmIndex = {fsmPageId, curSlotId};
        if (dataPageType == PageType::HEAP_PAGE_TYPE) {
            HeapPage::InitHeapPage(bufDesc, curDataPageId, curFsmIndex);
        } else {
            BtrPage::InitBtrPage(bufDesc, curDataPageId, curFsmIndex);
        }
        /* Write back wal id, plsn, glsn */
        dataPage->SetLsn(redoCtx->walId, redoCtx->recordEndPlsn, recordGlsn);
        (void)bufMgr->MarkDirty(bufDesc);
        bufMgr->UnlockAndRelease(bufDesc);
    }
}

void WalRecordTbsCreateTablespace::Redo(WalRecordRedoContext *redoCtx) const
{
    if (redoCtx == nullptr) {
        ErrLog(DSTORE_PANIC, MODULE_TABLESPACE, ErrMsg("WAL_TBS_CREATE_TABLESPACE: redoCtx is NULL"));
    }

    if (tablespaceId == INVALID_TABLESPACE_ID) {
        ErrLog(DSTORE_PANIC, MODULE_TABLESPACE, ErrMsg("WAL_TBS_CREATE_TABLESPACE: tablespaceId is invalid"));
    }

    ErrLog(DSTORE_LOG, MODULE_TABLESPACE, ErrMsg("WAL_TBS_CREATE_TABLESPACE REDO: tablespaceId is %hu", tablespaceId));

    StoragePdb *pdb = g_storageInstance->GetPdb(redoCtx->pdbId);
    if (pdb == nullptr) {
        ErrLog(DSTORE_PANIC, MODULE_TABLESPACE, ErrMsg("WAL_TBS_CREATE_TABLESPACE: pdb is NULL"));
    }

    TablespaceMgr *tablespaceMgr = pdb->GetTablespaceMgr();
    if (tablespaceMgr == nullptr) {
        ErrLog(DSTORE_PANIC, MODULE_TABLESPACE, ErrMsg("WAL_TBS_CREATE_TABLESPACE: tablespaceMgr is NULL"));
    }

    ControlFile *m_controlFile = pdb->GetControlFile();
    if (m_controlFile == nullptr) {
        ErrLog(DSTORE_PANIC, MODULE_TABLESPACE, ErrMsg("WAL_TBS_CREATE_TABLESPACE:"
        "controlFile is NULL. tablespaceId:%hu", tablespaceId));
    }

    if (STORAGE_FUNC_FAIL(tablespaceMgr->LockTablespace(tablespaceId, DSTORE::DSTORE_ACCESS_EXCLUSIVE_LOCK))) {
        int errorLevel = ((StorageGetErrorCode() == LOCK_WARNING_FREEZING) ? DSTORE_LOG : DSTORE_PANIC);
        ErrLog(errorLevel, MODULE_TABLESPACE, ErrMsg("WAL_TBS_CREATE_TABLESPACE:"
            "failed to lock tablespace, tablespaceId:%hu", tablespaceId));
    }

    if (STORAGE_FUNC_FAIL(m_controlFile->UpdateCreateTablespace(tablespaceId, ddlXid, tbsMaxSize, preReuseVersion))) {
        tablespaceMgr->UnlockTablespace(tablespaceId, DSTORE::DSTORE_ACCESS_EXCLUSIVE_LOCK);
        ErrLog(DSTORE_PANIC, MODULE_TABLESPACE, ErrMsg("WAL_TBS_CREATE_TABLESPACE:"
            "Update controlfile failed. tablespaceId:%hu", tablespaceId));
    }

    tablespaceMgr->UnlockTablespace(tablespaceId, DSTORE::DSTORE_ACCESS_EXCLUSIVE_LOCK);
    ErrLog(DSTORE_LOG, MODULE_TABLESPACE, ErrMsg("WAL_TBS_CREATE_TABLESPACE REDO FINISHED"));
    return;
}

RetStatus WalRecordTbsCreateDataFile::CreateFile(StoragePdb *pdb) const
{
    StorageAssert(pdb != nullptr);
    VFSAdapter *vfs = pdb->GetVFS();
    /* create a physical file name and init filePara */
    char fileName[MAXPGPATH] = {0};
    if (STORAGE_FUNC_FAIL(vfs->GetFileNameFromFileId(fileId, fileName))) {
        ErrLog(DSTORE_PANIC, MODULE_TABLESPACE, ErrMsg("WAL_TBS_CREATE_DATA_FILE: "
        "Failed to fileName , tablespaceId:%hu, fileId:%hu", tablespaceId, fileId));
    }

    TenantConfig *tenantConfig = g_storageInstance->GetGuc()->tenantConfig;
    StorageAssert(tenantConfig);

    /* init m_filePara */
    FileParameter filePara;
    filePara.streamId = VFS_DEFAULT_FILE_STREAM_ID;
    filePara.flag = IN_PLACE_WRITE_FILE;
    filePara.fileSubType = DATA_FILE_TYPE;
    filePara.rangeSize = IsTemplate(pdb->GetPdbId()) ? TEMPLATE_TBS_FILE_RANGE_SIZE : TBS_FILE_RANGE_SIZE;
    filePara.maxSize = DSTORE::MAX_FILE_SIZE;
    filePara.recycleTtl = 0;
    filePara.mode = FILE_READ_AND_WRITE_MODE;
    filePara.isReplayWrite = false;

    /* vfs add physical file */
    const char* storeSpaceName = tenantConfig->storeSpaces[0].storeSpaceName;
    if (unlikely(storeSpaceName != nullptr)) {
        errno_t rc = strcpy_s(filePara.storeSpaceName, STORESPACE_NAME_MAX_LEN, storeSpaceName);
        storage_securec_check(rc, "\0", "\0");
    }

    FileSize fileSize;
    fileSize.initialFileSize = INIT_FILE_SIZE;
    fileSize.maxFileSize = MAX_FILE_SIZE;
    uint64 initBlockCount = static_cast<uint64>(fileSize.initialFileSize / BLCKSZ);

    if (!vfs->FileExists(fileName)) {
        if (STORAGE_FUNC_FAIL(vfs->CreateFile(fileId, fileName, filePara))) {
            ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("WAL_TBS_CREATE_DATA_FILE: "
                "Failed to create file, tablespaceId:%hu, fileId:%hu", tablespaceId, fileId));
            return DSTORE_FAIL;
        }
        ErrLog(DSTORE_LOG, MODULE_TABLESPACE, ErrMsg("WAL_TBS_CREATE_DATA_FILE: "
            "Success to create file, tablespaceId:%hu, fileId:%hu", tablespaceId, fileId));
    }

    if (STORAGE_FUNC_FAIL(vfs->OpenFile(fileId, fileName,
        (USE_VFS_LOCAL_AIO ? DSTORE_FILE_ADIO_FLAG : DSTORE_FILE_OPEN_FLAG)))) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("WAL_TBS_CREATE_DATA_FILE: "
            "Failed to open file. Filename: %s", fileName));
        return DSTORE_FAIL;
    }

    int64 size = vfs->GetSize(fileId);
    if (unlikely(size < 0)) {
        ErrLog(DSTORE_ERROR, MODULE_PDB, ErrMsg("WAL_TBS_CREATE_DATA_FILE: "
            "Failed to get file size. Filename: %s", fileName));
        return DSTORE_FAIL;
    }

    ErrLog(DSTORE_LOG, MODULE_TABLESPACE, ErrMsg("WAL_TBS_CREATE_DATA_FILE: "
        "size(%ld), tablespaceId:%hu, fileId:%hu", size, tablespaceId, fileId));

    if (size < GetOffsetByBlockNo(initBlockCount)) {
        if (STORAGE_FUNC_FAIL(vfs->Extend(fileId, GetOffsetByBlockNo(initBlockCount)))) {
            ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("WAL_TBS_CREATE_DATA_FILE: "
                "Failed to extend file, tablespaceId:%hu, fileId:%hu", tablespaceId, fileId));
            return DSTORE_FAIL;
        }
        ErrLog(DSTORE_LOG, MODULE_TABLESPACE, ErrMsg("WAL_TBS_CREATE_DATA_FILE: "
            "Success to extend file, tablespaceId:%hu, fileId:%hu", tablespaceId, fileId));
    }

    return DSTORE_SUCC;
}

void WalRecordTbsCreateDataFile::Redo(WalRecordRedoContext *redoCtx) const
{
    if (redoCtx == nullptr) {
        ErrLog(DSTORE_PANIC, MODULE_TABLESPACE, ErrMsg("WAL_TBS_CREATE_DATA_FILE: redoCtx is NULL"));
    }

    /* check tablespaceId and fileId first */
    if (tablespaceId == INVALID_TABLESPACE_ID || fileId == INVALID_DATA_FILE_ID) {
        ErrLog(DSTORE_PANIC, MODULE_TABLESPACE, ErrMsg("WAL_TBS_CREATE_DATA_FILE:"
            "tablespaceId or filedId is invalid, tablespaceId:%hu, fileId:%hu", tablespaceId, fileId));
    }

    ErrLog(DSTORE_LOG, MODULE_TABLESPACE,
        ErrMsg("WAL_TBS_CREATE_DATA_FILE REDO: tablespaceId:%hu, fileId:%hu", tablespaceId, fileId));
    
    StoragePdb *pdb = g_storageInstance->GetPdb(redoCtx->pdbId);
    if (pdb == nullptr) {
        ErrLog(DSTORE_PANIC, MODULE_TABLESPACE, ErrMsg("WAL_TBS_CREATE_DATA_FILE: pdb is NULL"));
    }

    TablespaceMgr *tablespaceMgr = pdb->GetTablespaceMgr();
    if (tablespaceMgr == nullptr) {
        ErrLog(DSTORE_PANIC, MODULE_TABLESPACE, ErrMsg("WAL_TBS_CREATE_DATA_FILE: tablespaceMgr is NULL"));
    }

    ControlFile *m_controlFile = pdb->GetControlFile();
    if (m_controlFile == nullptr) {
        ErrLog(DSTORE_PANIC, MODULE_TABLESPACE, ErrMsg("WAL_TBS_CREATE_DATA_FILE: controlFile is NULL"));
    }

    if (STORAGE_FUNC_FAIL(tablespaceMgr->LockTablespace(tablespaceId, DSTORE::DSTORE_ACCESS_EXCLUSIVE_LOCK))) {
        int errorLevel = ((StorageGetErrorCode() == LOCK_WARNING_FREEZING) ? DSTORE_LOG : DSTORE_PANIC);
        ErrLog(errorLevel, MODULE_TABLESPACE, ErrMsg("WAL_TBS_CREATE_DATA_FILE:"
            "failed to lock tablespace, tablespaceId:%hu, fileId:%hu", tablespaceId, fileId));
    }

    if (STORAGE_FUNC_FAIL(CreateFile(pdb))) {
        tablespaceMgr->UnlockTablespace(tablespaceId, DSTORE::DSTORE_ACCESS_EXCLUSIVE_LOCK);
        ErrLog(DSTORE_PANIC, MODULE_TABLESPACE, ErrMsg("WAL_TBS_CREATE_DATA_FILE:"
            "create data file failed. tablespaceId:%hu, fileId:%hu", tablespaceId, fileId));
    }

    if (STORAGE_FUNC_FAIL(m_controlFile->UpdateCreateDataFile(tablespaceId, fileId, fileMaxSize, extentSize,
        preReuseVersion, ddlXid))) {
        tablespaceMgr->UnlockTablespace(tablespaceId, DSTORE::DSTORE_ACCESS_EXCLUSIVE_LOCK);
        ErrLog(DSTORE_PANIC, MODULE_TABLESPACE, ErrMsg("WAL_TBS_CREATE_DATA_FILE:"
            "Update controlfile failed. tablespaceId:%hu", tablespaceId));
    }

    tablespaceMgr->UnlockTablespace(tablespaceId, DSTORE::DSTORE_ACCESS_EXCLUSIVE_LOCK);
    ErrLog(DSTORE_LOG, MODULE_TABLESPACE, ErrMsg("WAL_TBS_CREATE_DATA_FILE REDO FINISHED"));
    return;
}

void WalRecordTbsAddFileToTbs::Redo(WalRecordRedoContext *redoCtx) const
{
    if (redoCtx == nullptr) {
        ErrLog(DSTORE_PANIC, MODULE_TABLESPACE, ErrMsg("WAL_TBS_ADD_FILE_TO_TABLESPACE: redoCtx is NULL"));
    }

    /* check tablespaceId and fileId first */
    if (tablespaceId == INVALID_TABLESPACE_ID || fileId == INVALID_VFS_FILE_ID) {
        ErrLog(DSTORE_PANIC, MODULE_TABLESPACE, ErrMsg("WAL_TBS_ADD_FILE_TO_TABLESPACE:"
            "tablespaceId or fileId is invalid, tablespaceId:%hu, fileId:%hu", tablespaceId, fileId));
    }

    if (hwm < slotId || slotId >= MAX_TBS_DATAFILE_SLOT_CNT) {
        ErrLog(DSTORE_PANIC, MODULE_TABLESPACE, ErrMsg("WAL_TBS_ADD_FILE_TO_TABLESPACE:slotId or hwm is wrong, "
            "tablespaceId:%hu, fileId:%hu, slotId:%hu, hwm:%hu", tablespaceId, fileId, slotId, hwm));
    }

    ErrLog(DSTORE_LOG, MODULE_TABLESPACE,
        ErrMsg("WAL_TBS_ADD_FILE_TO_TABLESPACE REDO: tablespaceId:%hu, fileId:%hu", tablespaceId, fileId));

    StoragePdb *pdb = g_storageInstance->GetPdb(redoCtx->pdbId);
    if (pdb == nullptr) {
        ErrLog(DSTORE_PANIC, MODULE_TABLESPACE, ErrMsg("WAL_TBS_ADD_FILE_TO_TABLESPACE: pdb is NULL"));
    }

    TablespaceMgr *tablespaceMgr = pdb->GetTablespaceMgr();
    if (tablespaceMgr == nullptr) {
        ErrLog(DSTORE_PANIC, MODULE_TABLESPACE, ErrMsg("WAL_TBS_ADD_FILE_TO_TABLESPACE: tablespaceMgr is NULL"));
    }

    ControlFile *m_controlFile = pdb->GetControlFile();
    if (m_controlFile == nullptr) {
        ErrLog(DSTORE_PANIC, MODULE_TABLESPACE, ErrMsg("WAL_TBS_ADD_FILE_TO_TABLESPACE:"
            "controlFile is NULL, tablespaceId:%hu, fileId:%hu", tablespaceId, fileId));
    }

    if (STORAGE_FUNC_FAIL(tablespaceMgr->LockTablespace(tablespaceId, DSTORE::DSTORE_ACCESS_EXCLUSIVE_LOCK))) {
        int errorLevel = ((StorageGetErrorCode() == LOCK_WARNING_FREEZING) ? DSTORE_LOG : DSTORE_PANIC);
        ErrLog(errorLevel, MODULE_TABLESPACE, ErrMsg("WAL_TBS_ADD_FILE_TO_TABLESPACE:"
            "failed to lock tablespace, tablespaceId:%hu, fileId:%hu", tablespaceId, fileId));
    }

    if (STORAGE_FUNC_FAIL(m_controlFile->UpdateAddFileToTbs(tablespaceId, fileId, slotId, hwm,
        preReuseVersion, ddlXid))) {
        tablespaceMgr->UnlockTablespace(tablespaceId, DSTORE::DSTORE_ACCESS_EXCLUSIVE_LOCK);
        ErrLog(DSTORE_PANIC, MODULE_TABLESPACE, ErrMsg("WAL_TBS_ADD_FILE_TO_TABLESPACE:"
            "Update controlfile failed. tablespaceId:%hu", tablespaceId));
    }

    tablespaceMgr->UnlockTablespace(tablespaceId, DSTORE::DSTORE_ACCESS_EXCLUSIVE_LOCK);
    ErrLog(DSTORE_LOG, MODULE_TABLESPACE, ErrMsg("WAL_TBS_ADD_FILE_TO_TABLESPACE REDO FINISHED"));
    return;
}

void WalRecordTbsDropTablespace::Redo(WalRecordRedoContext *redoCtx) const
{
    if (redoCtx == nullptr) {
        ErrLog(DSTORE_PANIC, MODULE_TABLESPACE, ErrMsg("WAL_TBS_DROP_TABLESPACE: redoCtx is NULL"));
    }

    if (tablespaceId == INVALID_TABLESPACE_ID) {
        ErrLog(DSTORE_PANIC, MODULE_TABLESPACE, ErrMsg("WAL_TBS_DROP_TABLESPACE: tablespaceId is INVALID"));
    }

    ErrLog(DSTORE_LOG, MODULE_TABLESPACE, ErrMsg("WAL_TBS_DROP_TABLESPACE REDO: tablespaceId:%hu", tablespaceId));

    StoragePdb *pdb = g_storageInstance->GetPdb(redoCtx->pdbId);
    if (pdb == nullptr) {
        ErrLog(DSTORE_PANIC, MODULE_TABLESPACE, ErrMsg("WAL_TBS_DROP_TABLESPACE: pdb is NULL"));
    }

    TablespaceMgr *tablespaceMgr = pdb->GetTablespaceMgr();
    if (tablespaceMgr == nullptr) {
        ErrLog(DSTORE_PANIC, MODULE_TABLESPACE, ErrMsg("WAL_TBS_DROP_TABLESPACE: tablespaceMgr is NULL"));
    }

    ControlFile *m_controlFile = pdb->GetControlFile();
    if (m_controlFile == nullptr) {
        ErrLog(DSTORE_PANIC, MODULE_TABLESPACE, ErrMsg("WAL_TBS_DROP_TABLESPACE:"
            "controlFile is NULL. tablespaceId: %hu", tablespaceId));
    }

    if (STORAGE_FUNC_FAIL(tablespaceMgr->LockTablespace(tablespaceId, DSTORE::DSTORE_ACCESS_EXCLUSIVE_LOCK))) {
        int errorLevel = ((StorageGetErrorCode() == LOCK_WARNING_FREEZING) ? DSTORE_LOG : DSTORE_PANIC);
        ErrLog(errorLevel, MODULE_TABLESPACE, ErrMsg("WAL_TBS_DROP_TABLESPACE:"
            "failed to lock tablespace, tablespaceId: %hu", tablespaceId));
    }

    if (STORAGE_FUNC_FAIL(m_controlFile->UpdateDropTablespace(tablespaceId, preReuseVersion, ddlXid, hwm))) {
        tablespaceMgr->UnlockTablespace(tablespaceId, DSTORE::DSTORE_ACCESS_EXCLUSIVE_LOCK);
        ErrLog(DSTORE_PANIC, MODULE_TABLESPACE, ErrMsg("WAL_TBS_CREATE_TABLESPACE:"
            "Update controlfile failed. tablespaceId:%hu", tablespaceId));
    }

    tablespaceMgr->UnlockTablespace(tablespaceId, DSTORE::DSTORE_ACCESS_EXCLUSIVE_LOCK);
    ErrLog(DSTORE_LOG, MODULE_TABLESPACE, ErrMsg("WAL_TBS_DROP_TABLESPACE REDO FINISHED"));
    return;
}

RetStatus WalRecordTbsDropDataFile::DropFile(StoragePdb *pdb) const
{
    StorageAssert(pdb != nullptr);
    VFSAdapter *vfs = pdb->GetVFS();
    /* write a physical file name */
    char fileName[MAXPGPATH] = {0};
    if (STORAGE_FUNC_FAIL(vfs->GetFileNameFromFileId(fileId, fileName))) {
        ErrLog(DSTORE_LOG, MODULE_TABLESPACE,
            ErrMsg("WAL_TBS_DROP_DATA_FILE: Failed to get FileName by fileId. fileId:%hu", fileId));
        return DSTORE_FAIL;
    }

    /* skip if file not exist */
    if (vfs->FileExists(fileName)) {
        if (STORAGE_FUNC_FAIL(vfs->Close(fileId))) {
            ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
                   ErrMsg("WAL_TBS_DROP_DATA_FILE: Failed to close file, tablespaceId:%hu, fileId:%hu",
                          tablespaceId, fileId));
            return DSTORE_FAIL;
        }
        RetStatus ret = vfs->RemoveFile(fileName);
        if (ret == DSTORE_FAIL) {
            ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("WAL_TBS_DROP_DATA_FILE: "
                "Failed to create file, tablespaceId:%hu, fileId:%hu", tablespaceId, fileId));
            return DSTORE_FAIL;
        }
        ErrLog(DSTORE_LOG, MODULE_TABLESPACE, ErrMsg("WAL_TBS_DROP_DATA_FILE: "
            "Success to drop file, tablespaceId:%hu, fileId:%hu", tablespaceId, fileId));
    }
    return DSTORE_SUCC;
}

void WalRecordTbsDropDataFile::Redo(WalRecordRedoContext *redoCtx) const
{
    if (redoCtx == nullptr) {
        ErrLog(DSTORE_PANIC, MODULE_TABLESPACE, ErrMsg("WAL_TBS_DROP_DATA_FILE: redoCtx is NULL."));
    }

    if (fileId == INVALID_DATA_FILE_ID) {
        ErrLog(DSTORE_PANIC, MODULE_TABLESPACE, ErrMsg("WAL_TBS_DROP_DATA_FILE: FileId is invalid."));
    }

    /* tablespaceId = Invalid_TABLESPACE_ID indicates that only need delete data file, disassociation is not needed. */
    bool onlydelete = (tablespaceId == INVALID_TABLESPACE_ID);
    if (!onlydelete && (hwm < slotId || slotId >= MAX_TBS_DATAFILE_SLOT_CNT)) {
        ErrLog(DSTORE_PANIC, MODULE_TABLESPACE, ErrMsg("WAL_TBS_DROP_DATA_FILE: slotId or hwm is wrong,"
            "tablespaceId:%hu, fileId:%hu, slotId:%hu, hwm:%hu", tablespaceId, fileId, slotId, hwm));
    }

    ErrLog(DSTORE_LOG, MODULE_TABLESPACE, ErrMsg("WAL_TBS_DROP_DATA_FILE REDO, tablespaceId:%hu, fileId:%hu,"
        "slotId:%hu, hwm:%hu", tablespaceId, fileId, slotId, hwm));
    
    StoragePdb *pdb = g_storageInstance->GetPdb(redoCtx->pdbId);
    if (pdb == nullptr) {
        ErrLog(DSTORE_PANIC, MODULE_TABLESPACE, ErrMsg("WAL_TBS_DROP_DATA_FILE: pdb is NULL."));
    }

    TablespaceMgr *tablespaceMgr = pdb->GetTablespaceMgr();
    if (tablespaceMgr == nullptr) {
        ErrLog(DSTORE_PANIC, MODULE_TABLESPACE, ErrMsg("WAL_TBS_DROP_DATA_FILE: tablespaceMgr is NULL"));
    }

    ControlFile *m_controlFile = pdb->GetControlFile();
    if (m_controlFile == nullptr) {
        ErrLog(DSTORE_PANIC, MODULE_TABLESPACE, ErrMsg("WAL_TBS_DROP_DATA_FILE: controlFile is NULL"));
    }

    if (!onlydelete) {
        if (STORAGE_FUNC_FAIL(tablespaceMgr->LockTablespace(tablespaceId, DSTORE::DSTORE_ACCESS_EXCLUSIVE_LOCK))) {
            int errorLevel = ((StorageGetErrorCode() == LOCK_WARNING_FREEZING) ? DSTORE_LOG : DSTORE_PANIC);
            ErrLog(errorLevel, MODULE_TABLESPACE,
                ErrMsg("WAL_TBS_DROP_DATA_FILE: failed to lock tablespace, tablespaceId:%hu.", tablespaceId));
            return;
        }
    }

    RetStatus ret = DSTORE_SUCC;
    do {
        if (m_controlFile == nullptr) {
            ErrLog(DSTORE_PANIC, MODULE_TABLESPACE, ErrMsg("WAL_TBS_DROP_DATA_FILE: controlFile is NULL"));
        }
        ret = m_controlFile->UpdateDropDataFile(tablespaceId, fileId, preReuseVersion, slotId, hwm);
        if (ret != DSTORE_SUCC) {
            ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("WAL_TBS_DROP_DATA_FILE: Update controlfile failed."
                "tablespaceId:%hu, fileId:%hu, slotId:%hu, hwm:%hu", tablespaceId, fileId, slotId, hwm));
            break;
        }
        ret = g_storageInstance->GetBufferMgr()->InvalidateUsingGivenFileId(redoCtx->pdbId, fileId);
        if (ret != DSTORE_SUCC) {
            ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
                ErrMsg("Failed to invalidate buffer by fileId %hu, tablespaceId %hu, pdbId: %hhu.",
                    fileId, tablespaceId, redoCtx->pdbId));
            break;
        }
        ret = DropFile(pdb);
        if (ret != DSTORE_SUCC) {
            ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("WAL_TBS_DROP_DATA_FILE: drop file failed."
                "tablespaceId:%hu, fileId:%hu, slotId:%hu, hwm:%hu", tablespaceId, fileId, slotId, hwm));
            break;
        }
    } while (0);

    if (!onlydelete) {
        tablespaceMgr->UnlockTablespace(tablespaceId, DSTORE::DSTORE_ACCESS_EXCLUSIVE_LOCK);
    }

    if (ret == DSTORE_SUCC) {
        ErrLog(DSTORE_LOG, MODULE_TABLESPACE, ErrMsg("WAL_TBS_DROP_DATA_FILE FINISHED"));
        return;
    }

    ErrLog(DSTORE_PANIC, MODULE_TABLESPACE, ErrMsg("WAL_TBS_DROP_DATA_FILE REDO FAILD."));
    return;
}

void WalRecordTbsAlterTablespace::Redo(WalRecordRedoContext *redoCtx) const
{
    if (redoCtx == nullptr) {
        ErrLog(DSTORE_PANIC, MODULE_TABLESPACE, ErrMsg("WAL_TBS_ALTER_TABLESPACE: redoCtx is NULL"));
    }
    
    if (tablespaceId == INVALID_TABLESPACE_ID) {
        ErrLog(DSTORE_PANIC, MODULE_TABLESPACE,
            ErrMsg("WAL_TBS_ALTER_TABLESPACE: tablespaceId is INVALID, tablespaceId is %hu", tablespaceId));
    }

    ErrLog(DSTORE_LOG, MODULE_TABLESPACE,
        ErrMsg("WAL_TBS_ALTER_TABLESPACE redo, tablespaceId is %hu", tablespaceId));

    StoragePdb *pdb = g_storageInstance->GetPdb(redoCtx->pdbId);
    if (pdb == nullptr) {
        ErrLog(DSTORE_PANIC, MODULE_TABLESPACE, ErrMsg("WAL_TBS_ALTER_TABLESPACE: pdb is NULL"));
    }

    TablespaceMgr *tablespaceMgr = pdb->GetTablespaceMgr();
    if (tablespaceMgr == nullptr) {
        ErrLog(DSTORE_PANIC, MODULE_TABLESPACE, ErrMsg("WAL_TBS_ALTER_TABLESPACE: tablespaceMgr is NULL"));
    }

    ControlFile *m_controlFile = pdb->GetControlFile();
    if (m_controlFile == nullptr) {
        ErrLog(DSTORE_PANIC, MODULE_TABLESPACE,
            ErrMsg("WAL_TBS_ALTER_TABLESPACE: controlFile is null. tablespaceId is %hu", tablespaceId));
    }

    if (STORAGE_FUNC_FAIL(tablespaceMgr->LockTablespace(tablespaceId, DSTORE::DSTORE_ACCESS_EXCLUSIVE_LOCK))) {
        int errorLevel = ((StorageGetErrorCode() == LOCK_WARNING_FREEZING) ? DSTORE_LOG : DSTORE_PANIC);
        ErrLog(errorLevel, MODULE_TABLESPACE,
            ErrMsg("WAL_TBS_ALTER_TABLESPACE: failed to lock tablespace, tablespaceId is %hu", tablespaceId));
    }

    if (STORAGE_FUNC_FAIL(m_controlFile->UpdateAlterTablespace(tablespaceId, tbsMaxSize, ddlXid, preReuseVersion))) {
        tablespaceMgr->UnlockTablespace(tablespaceId, DSTORE::DSTORE_ACCESS_EXCLUSIVE_LOCK);
        ErrLog(DSTORE_PANIC, MODULE_TABLESPACE,
            ErrMsg("WAL_TBS_ALTER_TABLESPACE: Update controlfile failed. tablespaceId is %hu", tablespaceId));
    }

    tablespaceMgr->UnlockTablespace(tablespaceId, DSTORE::DSTORE_ACCESS_EXCLUSIVE_LOCK);
    ErrLog(DSTORE_LOG, MODULE_TABLESPACE, ErrMsg("WAL_TBS_ALTER_TABLESPACE REDO FINISHED!"));
    return;
}

} /* namespace DSTORE */
