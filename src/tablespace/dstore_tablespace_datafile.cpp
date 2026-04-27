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
 * dstore_tablespace_datafile.cpp
 *
 *
 *
 * IDENTIFICATION
 *        src/tablespace/dstore_tablespace_datafile.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "common/log/dstore_log.h"
#include "page/dstore_bitmap_meta_page.h"
#include "page/dstore_tbs_file_meta_page.h"
#include "securec.h"
#include "errorcode/dstore_tablespace_error_code.h"
#include "framework/dstore_vfs_adapter.h"
#include "wal/dstore_wal_write_context.h"
#include "framework/dstore_session.h"
#include "tablespace/dstore_tablespace_wal.h"
#include "lock/dstore_lock_mgr.h"

namespace DSTORE {

TbsDataFileBitmapMgr::TbsDataFileBitmapMgr()
    : m_pageCount{0}, m_startPosMem {}, m_startPos {}
{}

TbsDataFileBitmapMgr::~TbsDataFileBitmapMgr()
{
    /* Since startPos are allocated all together, free the whole memory will do the work */
    DstorePfree(this->m_startPosMem);
    m_startPosMem = nullptr;
}

RetStatus TbsDataFileBitmapMgr::InitFreeBitsSearchPos()
{
    m_startPosMem = (FreeBitsSearchPos *)DstoreMemoryContextAlloc(
        thrd->GetGlobalSmgrMemoryCtx(),
        (sizeof(FreeBitsSearchPos) * MAX_BITMAP_PAGE_COUNT));
    if (unlikely(m_startPosMem == NULL)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("InitFreeBitsSearchPos memory alloc failed."));
        return DSTORE_FAIL;
    }

    /* Initialize all FreeBitsSearchPos and record their locations */
    for (uint32 i = 0; i < MAX_BITMAP_PAGE_COUNT; i++) {
        FreeBitsSearchPos *bitStart = m_startPosMem + i;
        bitStart->m_freeBitsSearchPos = 0;
        m_startPos[m_pageCount++] = bitStart;
    }
    return DSTORE_SUCC;
}

FreeBitsSearchPos *TbsDataFileBitmapMgr::GetFreeBitsSearchPos(uint32 pageNo)
{
    if (pageNo >= m_pageCount) {
        return nullptr;
    }
    return this->m_startPos[pageNo];
}

PageId TbsDataFileBitmapMgr::GetNewGroupStart(TbsBitmapMetaPage *bitmapMetaPage)
{
    if (this->m_pageCount == 0) {
        return INVALID_PAGE_ID;
    }

    uint32 index = static_cast<uint32>(bitmapMetaPage->groupCount - 1);
    TbsBitMapGroup lastGroup = bitmapMetaPage->bitmapGroups[index];
    PageId result = lastGroup.firstBitmapPageId;
    result.m_blockId +=
        (BITMAP_PAGES_PER_GROUP +
         static_cast<uint32>(BITMAP_PAGES_PER_GROUP) * DF_BITMAP_BIT_CNT
             * static_cast<uint16>(bitmapMetaPage->extentSize));
    return result;
}

uint32 TbsDataFileBitmapMgr::FindExtentStartPos(uint32 bitmapPageNo)
{
    FreeBitsSearchPos *pos = GetFreeBitsSearchPos(bitmapPageNo);
    if (unlikely(pos == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("Invalid bitmapPageNo %u!", bitmapPageNo));
        return DF_BITMAP_BIT_CNT;
    }
    return pos->m_freeBitsSearchPos;
}

RetStatus TbsDataFileBitmapMgr::ForwardFreeBitsSearchPos(uint32 bitNo, uint32 bitmapPageNo)
{
    FreeBitsSearchPos *pos = GetFreeBitsSearchPos(bitmapPageNo);
    if (unlikely(pos == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("Invalid bitmapPageNo %u!", bitmapPageNo));
        return DSTORE_FAIL;
    }
    pos->m_freeBitsSearchPos = bitNo + 1;
    return DSTORE_SUCC;
}

void TbsDataFileBitmapMgr::BackwardFreeBitsSearchPos(uint32 bitNo, uint32 bitmapPageNo)
{
    FreeBitsSearchPos *pos = GetFreeBitsSearchPos(bitmapPageNo);
    if (pos != nullptr && bitNo < pos->m_freeBitsSearchPos) {
        pos->m_freeBitsSearchPos = bitNo;
    }
}

RetStatus TbsDataFile::GetFileMetaPage(LWLockMode lock, BufferDesc **pageDesc, TbsFileMetaPage **page)
{
    PageId pageId = {m_fileId, TBS_FILE_META_PAGE};
    BufferDesc *fileMetaPageDesc = GetBufferMgr()->Read(m_pdbId, pageId, lock);
    if (fileMetaPageDesc == INVALID_BUFFER_DESC) {
        *pageDesc = INVALID_BUFFER_DESC;
        *page = nullptr;
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("File(%hu, %u) get FileMeta page fail.", m_fileId, TBS_FILE_META_PAGE));
        return DSTORE_FAIL;
    }
    *pageDesc = fileMetaPageDesc;
    *page = static_cast<TbsFileMetaPage *>(fileMetaPageDesc->GetPage());
    return DSTORE_SUCC;
}

RetStatus TbsDataFile::GetSpaceMetaPage(LWLockMode lock, BufferDesc **pageDesc, TbsSpaceMetaPage **page)
{
    PageId pageId = {m_fileId, TBS_SPACE_META_PAGE};
    BufferDesc *spaceMetaPageDesc = GetBufferMgr()->Read(m_pdbId, pageId, lock);
    if (spaceMetaPageDesc == INVALID_BUFFER_DESC) {
        *pageDesc = INVALID_BUFFER_DESC;
        *page = nullptr;
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("File(%hu, %u) get SpaceMeta page fail.", m_fileId, TBS_SPACE_META_PAGE));
        return DSTORE_FAIL;
    }
    *pageDesc = spaceMetaPageDesc;
    *page = static_cast<TbsSpaceMetaPage *>(spaceMetaPageDesc->GetPage());
    return DSTORE_SUCC;
}

RetStatus TbsDataFile::GetBitmapMetaPage(LWLockMode lock, BufferDesc **pageDesc, TbsBitmapMetaPage **page)
{
    PageId pageId = {m_fileId, TBS_BITMAP_META_PAGE};
    BufferDesc *bitmapMetaPageDesc = GetBufferMgr()->Read(m_pdbId, pageId, lock);
    if (bitmapMetaPageDesc == INVALID_BUFFER_DESC) {
        *pageDesc = INVALID_BUFFER_DESC;
        *page = nullptr;
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("File(%u) get bitmapMeta page failed on pdb %u.", m_fileId, m_pdbId));
        return DSTORE_FAIL;
    }
    *pageDesc = bitmapMetaPageDesc;
    *page = static_cast<TbsBitmapMetaPage *>(bitmapMetaPageDesc->GetPage());
    return DSTORE_SUCC;
}

RetStatus TbsDataFile::GetBitmapPage(const PageId &pageId, LWLockMode lock,
    BufferDesc **pageDesc, TbsBitmapPage **page)
{
    BufferDesc *bitmapPageDesc = GetBufferMgr()->Read(m_pdbId, pageId, lock);
    if (bitmapPageDesc == INVALID_BUFFER_DESC) {
        *pageDesc = INVALID_BUFFER_DESC;
        *page = nullptr;
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("normal file(%u, %u) get bitmap page fail.", pageId.m_fileId, pageId.m_blockId));
        return DSTORE_FAIL;
    }
    *pageDesc = bitmapPageDesc;
    *page = static_cast<TbsBitmapPage *>(bitmapPageDesc->GetPage());
    return DSTORE_SUCC;
}

ControlDataFilePageItemData TbsDataFile::GetDataFilePageItemData()
{
    return m_ctrlDatafileItem;
}

void TbsDataFile::SetDataFilePageItemData(ControlDataFilePageItemData &datafileItem)
{
    m_ctrlDatafileItem = datafileItem;
}

void TbsDataFile::UnlockAndReleaseFileMetaPage(BufferDesc *pageDesc)
{
    GetBufferMgr()->UnlockAndRelease(pageDesc);
}

void TbsDataFile::UnlockAndReleaseMetaPage(BufferDesc *pageDesc)
{
    GetBufferMgr()->UnlockAndRelease(pageDesc);
}

void TbsDataFile::UnlockAndReleasePage(BufferDesc *pageDesc, BufferPoolUnlockContentFlag flag)
{
    GetBufferMgr()->UnlockAndRelease(pageDesc, flag);
}

RetStatus TbsDataFile::InitBitmap()
{
    PageId metaPageId = {m_fileId, TBS_BITMAP_META_PAGE};
    BufferDesc *metaPageDesc = INVALID_BUFFER_DESC;
    TbsBitmapMetaPage *bitmapMeta = nullptr;
    if (STORAGE_FUNC_FAIL(GetBitmapMetaPage(LW_EXCLUSIVE, &metaPageDesc, &bitmapMeta))) {
        return DSTORE_FAIL;
    }

    int64 fileSize = m_vfs->GetSize(m_fileId);
    if (fileSize < 0) {
        UnlockAndReleaseMetaPage(metaPageDesc);
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to get size of fileId %hu during InitBitmapMgr.", m_fileId));
        return DSTORE_FAIL;
    }
    BlockNumber blockCount = GetBlockNoByPageAlignedFileSize(fileSize);
    bitmapMeta->InitBitmapMetaPage(metaPageId, blockCount, m_extentSize);
    metaPageDesc->SetFileVersion(m_ctrlDatafileItem.reuseVersion);
    AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
    if (!m_isTempFile) {
        (void) GetBufferMgr()->MarkDirty(metaPageDesc);
        bool glsnChangedFlag = (bitmapMeta->GetWalId() != walWriterContext->GetWalId());
        /* need to write Wal. */
        walWriterContext->BeginAtomicWal(thrd->GetCurrentXid());
        WalRecordTbsInitBitmapMetaPage walData;
        walData.extentSize = m_extentSize;
        walData.totalBlockCount = blockCount;
        walData.SetHeader({WAL_TBS_INIT_BITMAP_META_PAGE, sizeof(WalRecordTbsInitBitmapMetaPage), metaPageId,
                           bitmapMeta->GetWalId(), bitmapMeta->GetPlsn(), bitmapMeta->GetGlsn(), glsnChangedFlag,
                           m_ctrlDatafileItem.reuseVersion});
        walWriterContext->RememberPageNeedWal(metaPageDesc);
        walWriterContext->PutNewWalRecord(&walData);
        (void)walWriterContext->EndAtomicWal();
    }
    ErrLog(DSTORE_LOG, MODULE_TABLESPACE, ErrMsg("File %hu init BitmapMetaPage(%hu, %u)(plsn %lu glsn %lu) succeed. "
        "pdb %u.", m_fileId, bitmapMeta->GetSelfPageId().m_fileId, bitmapMeta->GetSelfPageId().m_blockId,
        bitmapMeta->GetPlsn(), bitmapMeta->GetGlsn(), m_pdbId));

    UnlockAndReleaseMetaPage(metaPageDesc);

    uint16 oldGroupCount = 0;
    if (STORAGE_FUNC_FAIL(AddBitmapGroup(oldGroupCount))) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("AddBitmapGroup failed, fileId id %hu.", m_fileId));
        return DSTORE_FAIL;
    }

    return DSTORE_SUCC;
}

RetStatus TbsDataFile::InitBitmapMgr()
{
    m_bitmapMgr = DstoreNew(thrd->GetGlobalSmgrMemoryCtx())
        TbsDataFileBitmapMgr();
    if (STORAGE_VAR_NULL(m_bitmapMgr)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("Failed to alloc bitmapMgr, fileId %hu.", m_fileId));
        return DSTORE_FAIL;
    }
    return m_bitmapMgr->InitFreeBitsSearchPos();
}

RetStatus TbsDataFile::InitTbsFileMeta()
{
    PageId metaPageId = {m_fileId, TBS_FILE_META_PAGE};
    BufferDesc *fileMetaPageDesc = INVALID_BUFFER_DESC;
    TbsFileMetaPage *tbsFileMetaPage = nullptr;
    if (STORAGE_FUNC_FAIL(GetFileMetaPage(LW_EXCLUSIVE, &fileMetaPageDesc, &tbsFileMetaPage))) {
        return DSTORE_FAIL;
    }
    tbsFileMetaPage->InitTbsFileMetaPage(metaPageId, m_ctrlDatafileItem.reuseVersion, m_ctrlDatafileItem.ddlXid);
    fileMetaPageDesc->SetFileVersion(m_ctrlDatafileItem.reuseVersion);

    ErrLog(DSTORE_LOG, MODULE_TABLESPACE, ErrMsg("File %hu init FileMetaPage(%hu, %u)(plsn %lu glsn %lu) succeed. "
        "pdb %u.", m_fileId, tbsFileMetaPage->GetSelfPageId().m_fileId, tbsFileMetaPage->GetSelfPageId().m_blockId,
        tbsFileMetaPage->GetPlsn(), tbsFileMetaPage->GetGlsn(), m_pdbId));

    if (m_isTempFile) {
        UnlockAndReleaseFileMetaPage(fileMetaPageDesc);
        return DSTORE_SUCC;
    }
    (void) GetBufferMgr()->MarkDirty(fileMetaPageDesc);
    AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
    bool glsnChangedFlag = (tbsFileMetaPage->GetWalId() != walWriterContext->GetWalId());
    /* need to write Wal. */
    walWriterContext->BeginAtomicWal(thrd->GetCurrentXid());
    WalRecordTbsInitTbsFileMetaPage walData;
    walData.SetHeader({WAL_TBS_INIT_TBS_FILE_META_PAGE, sizeof(WalRecordTbsInitTbsFileMetaPage), metaPageId,
                       tbsFileMetaPage->GetWalId(), tbsFileMetaPage->GetPlsn(), tbsFileMetaPage->GetGlsn(),
                       glsnChangedFlag, m_ctrlDatafileItem.reuseVersion});
    walData.m_reuseVersion = tbsFileMetaPage->m_reuseVersion;
    walData.m_ddlXid = tbsFileMetaPage->m_ddlXid;
    walWriterContext->RememberPageNeedWal(fileMetaPageDesc);
    walWriterContext->PutNewWalRecord(&walData);
    (void)walWriterContext->EndAtomicWal();
    UnlockAndReleaseFileMetaPage(fileMetaPageDesc);
    return DSTORE_SUCC;
}

RetStatus TbsDataFile::InitTbsSpaceMeta()
{
    PageId metaPageId = {m_fileId, TBS_SPACE_META_PAGE};
    BufferDesc *metaPageDesc = INVALID_BUFFER_DESC;
    TbsSpaceMetaPage *spaceMeta = nullptr;
    if (STORAGE_FUNC_FAIL(GetSpaceMetaPage(LW_EXCLUSIVE, &metaPageDesc, &spaceMeta))) {
        return DSTORE_FAIL;
    }
    spaceMeta->InitTbsSpaceMetaPage(metaPageId);
    metaPageDesc->SetFileVersion(m_ctrlDatafileItem.reuseVersion);
    AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
    if (!m_isTempFile) {
        (void) GetBufferMgr()->MarkDirty(metaPageDesc);
        bool glsnChangedFlag = (spaceMeta->GetWalId() != walWriterContext->GetWalId());
        /* need to write Wal. */
        walWriterContext->BeginAtomicWal(thrd->GetCurrentXid());
        WalRecordTbsInitTbsSpaceMetaPage walData;
        walData.SetHeader({WAL_TBS_INIT_TBS_SPACE_META_PAGE, sizeof(WalRecordTbsInitTbsSpaceMetaPage), metaPageId,
                           spaceMeta->GetWalId(), spaceMeta->GetPlsn(), spaceMeta->GetGlsn(),
                           glsnChangedFlag, m_ctrlDatafileItem.reuseVersion});
        walWriterContext->RememberPageNeedWal(metaPageDesc);
        walWriterContext->PutNewWalRecord(&walData);
        (void)walWriterContext->EndAtomicWal();
    }
    ErrLog(DSTORE_LOG, MODULE_TABLESPACE, ErrMsg("File %hu init SpaceMetaPage(%hu, %u)(plsn %lu glsn %lu) succeed. "
        "pdb %u.", m_fileId, spaceMeta->GetSelfPageId().m_fileId, spaceMeta->GetSelfPageId().m_blockId,
        spaceMeta->GetPlsn(), spaceMeta->GetGlsn(), m_pdbId));
    UnlockAndReleaseMetaPage(metaPageDesc);
    return DSTORE_SUCC;
}

RetStatus TbsDataFile::RemoveFile(const char *fullFileName)
{
    if (m_vfs->FileExists(m_fileId, fullFileName)) {
        if (STORAGE_FUNC_FAIL(m_vfs->Close(m_fileId))) {
            ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
                   ErrMsg("Close file in TbsDataFile failed, fileId %hu, fileName %s, pdbId %u.", m_fileId,
                          fullFileName, m_pdbId));
            return DSTORE_FAIL;
        }
        if (STORAGE_FUNC_FAIL(m_vfs->RemoveFile(m_fileId, fullFileName))) {
            ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
                   ErrMsg("Remove file in TbsDataFile failed, fileId %hu, fileName %s, pdbId %u.", m_fileId,
                          fullFileName, m_pdbId));
            return DSTORE_FAIL;
        }
        ErrLog(DSTORE_LOG, MODULE_TABLESPACE,
               ErrMsg("Remove file success, fileId %hu, fileName %s, pdbId %u.", m_fileId, fullFileName, m_pdbId));
    }
    return DSTORE_SUCC;
}

TbsDataFile::TbsDataFile(PdbId pdbId, VFSAdapter *vfs, FileId fileId, uint64 maxBlockCount, ExtentSize extentSize,
    bool isTemp)
    : m_fileName{},
      m_storeSpaceName{},
      m_fileId(fileId),
      m_filePara({}),
      m_maxBlockCount(maxBlockCount),
      m_isTempFile(isTemp),
      m_bitmapMgr(nullptr),
      m_extentSize(extentSize),
      m_pdbId(pdbId),
      m_fd(nullptr),
      m_vfs(vfs)
{
    int rc = snprintf_s(m_fileName, MAX_FILE_NAME_LEN, MAX_FILE_NAME_LEN - 1, "%hu", fileId);
    storage_securec_check_ss(rc);
    (void)pthread_rwlock_init(&m_mutex, nullptr);
}

TbsDataFile::~TbsDataFile()
{}

void TbsDataFile::InitFilePara(const char* storeSpaceName)
{
    m_filePara.streamId = VFS_DEFAULT_FILE_STREAM_ID;
    m_filePara.flag = IN_PLACE_WRITE_FILE;
    m_filePara.fileSubType = DATA_FILE_TYPE;
    m_filePara.rangeSize = IsTemplate(m_pdbId) ? TEMPLATE_TBS_FILE_RANGE_SIZE : TBS_FILE_RANGE_SIZE;
    m_filePara.maxSize = MAX_FILE_SIZE;
    m_filePara.recycleTtl = 0;
    m_filePara.mode = FILE_READ_AND_WRITE_MODE;
    m_filePara.isReplayWrite = false;
    if (unlikely(storeSpaceName != nullptr)) {
        errno_t rc = strcpy_s(m_filePara.storeSpaceName, STORESPACE_NAME_MAX_LEN, storeSpaceName);
        storage_securec_check(rc, "\0", "\0");
    }
}

RetStatus TbsDataFile::Create(BlockNumber initBlockCount, const char* storeSpaceName)
{
    if (g_storageInstance->GetPdb(m_pdbId) == nullptr) {
        return DSTORE_FAIL;
    }
    VFSAdapter *vfs = g_storageInstance->GetPdb(m_pdbId)->GetVFS();
    InitFilePara(storeSpaceName);

    RetStatus retStatus = vfs->CreateFile(m_fileId, m_fileName, m_filePara);
    if (STORAGE_FUNC_FAIL(retStatus)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Create File in TbsDataFile failed, fileId %hu, fileName %s, pdbId %u.", m_fileId, m_fileName,
                      m_pdbId));
        return DSTORE_FAIL;
    }
    /* Initialize file with a specified size. */
    g_storageInstance->GetStat()->m_reportWaitEvent(
        static_cast<uint32_t>(GsStatWaitEvent::WAIT_EVENT_TBSDATAFILE_EXTEND_FILE));
    retStatus = vfs->Extend(m_fileId, GetOffsetByBlockNo(initBlockCount));
    g_storageInstance->GetStat()->m_reportWaitEvent(static_cast<uint32_t>(OPTUTIL_GSSTAT_WAIT_EVENT_END));
    if (STORAGE_FUNC_FAIL(retStatus)) {
        /* Datafile deletion depends on the pendinglist mechanism. */
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Extend File in TbsDataFile failed, fileId %hu, fileName %s, pdbId %u.", m_fileId, m_fileName,
                      m_pdbId));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}
BufMgrInterface* TbsDataFile::GetBufferMgr()
{
    return m_isTempFile ? thrd->GetTmpLocalBufMgr() : g_storageInstance->GetBufferMgr();
}

RetStatus TbsDataFile::Init()
{
    if (STORAGE_FUNC_FAIL(InitTbsFileMeta())) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("Failed to InitTbsFileMeta."));
        return DSTORE_FAIL;
    }
    if (STORAGE_FUNC_FAIL(InitTbsSpaceMeta())) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("Failed to InitTbsSpaceMeta."));
        return DSTORE_FAIL;
    }
    if (STORAGE_FUNC_FAIL(InitBitmap())) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("Failed to InitBitmapMeta."));
        return DSTORE_FAIL;
    }
    ErrLog(DSTORE_LOG, MODULE_TABLESPACE, ErrMsg("Datafile %hu init succeed. pdb %u", m_fileId, m_pdbId));
    return DSTORE_SUCC;
}

void TbsDataFile::Destroy()
{
    if (m_bitmapMgr) {
        delete m_bitmapMgr;
        m_bitmapMgr = nullptr;
    }

    m_vfs = nullptr;
}

ExtentSize TbsDataFile::GetExtentSize()
{
    return m_extentSize;
}

void TbsDataFile::SetExtentSize(ExtentSize extentSize)
{
    this->m_extentSize = extentSize;
}

TbsBitmapMetaPage *TbsDataFile::ReadBitmapMetaPage()
{
    BufferDesc *bitmapMetaDesc = INVALID_BUFFER_DESC;
    TbsBitmapMetaPage *bitmapMeta = nullptr;
    (void)GetBitmapMetaPage(LW_SHARED, &bitmapMetaDesc, &bitmapMeta);
    UnlockAndReleaseMetaPage(bitmapMetaDesc);
    return bitmapMeta;
}

FreeBitsSearchPos *TbsDataFile::GetFreeBitsSearchPos(uint32 bitmapPageNo)
{
    return m_bitmapMgr->GetFreeBitsSearchPos(bitmapPageNo);
}

FileId TbsDataFile::GetFileId() const
{
    return this->m_fileId;
}

FileParameter TbsDataFile::GetFileParameter() const
{
    return this->m_filePara;
}

const char *TbsDataFile::GetFileName() const
{
    return this->m_fileName;
}

const char *TbsDataFile::GetStoreSpaceName() const
{
    return this->m_storeSpaceName;
}

void TbsDataFile::SetStoreSpaceName(const char *storeSpaceName)
{
    if (unlikely(storeSpaceName != nullptr)) {
        int rc = memcpy_s(this->m_storeSpaceName, MAXTABLESPACENAME, storeSpaceName, strlen(storeSpaceName) + 1);
        storage_securec_check(rc, "\0", "\0");
    }
}

PdbId TbsDataFile::GetPdbId() const
{
    return this->m_pdbId;
}

uint64 TbsDataFile::GetMaxBlockCount() const
{
    return this->m_maxBlockCount;
}

void TbsDataFile::SetMaxBlockCount(uint64 maxBlockCount)
{
    this->m_maxBlockCount = maxBlockCount;
}

uint64 TbsDataFile::GetTotalBlockCount()
{
    TbsBitmapMetaPage *mapMeta = ReadBitmapMetaPage();
    uint64 totalBlockCount = mapMeta->totalBlockCount;
    return totalBlockCount;
}

RetStatus TbsDataFile::AllocOid(bool isInitDb, uint32 *nextOid, uint32 prefetchCount)
{
    PageId metaPageId = {m_fileId, TBS_FILE_META_PAGE};
    BufferDesc *fileMetaPageDesc = INVALID_BUFFER_DESC;
    TbsFileMetaPage *tbsFileMetaPage = nullptr;

    if (m_isTempFile) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("Allocating oid for temporary datafile is not allowed."));
        return DSTORE_FAIL;
    }

    if (STORAGE_FUNC_FAIL(GetFileMetaPage(LW_EXCLUSIVE, &fileMetaPageDesc, &tbsFileMetaPage)) ||
        STORAGE_VAR_NULL(tbsFileMetaPage)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("Failed to read tbsFileMetaPage."));
        return DSTORE_FAIL;
    }

    if (tbsFileMetaPage->oid < (static_cast<Oid>(FIRST_NORMAL_OBJECT_ID))) {
        /*
         * Condition 1: wraparound, or first post-initdb assignment, in normal mode;
         * Condition 2: wraparound in standalone mode (unlikely but possible).
         *              we may be bootstrapping, so don't enforce the full range.
         */
        if (!isInitDb || tbsFileMetaPage->oid < (static_cast<Oid>(FIRST_BOOTSTRAP_OBJECT_ID))) {
            tbsFileMetaPage->oid = FIRST_NORMAL_OBJECT_ID;
        }
    } else if (isInitDb) {
        tbsFileMetaPage->oid = FIRST_BOOTSTRAP_OBJECT_ID;
    }

    *nextOid = tbsFileMetaPage->oid;
    tbsFileMetaPage->oid += prefetchCount;
    fileMetaPageDesc->SetFileVersion(m_ctrlDatafileItem.reuseVersion);
    (void)GetBufferMgr()->MarkDirty(fileMetaPageDesc);

    AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
    bool glsnChangedFlag = (tbsFileMetaPage->GetWalId() != walWriterContext->GetWalId());
    walWriterContext->BeginAtomicWal(thrd->GetCurrentXid());
    WalRecordTbsUpdateTbsFileMetaPage walData;
    walData.SetHeader({WAL_TBS_UPDATE_TBS_FILE_META_PAGE, sizeof(WalRecordTbsUpdateTbsFileMetaPage), metaPageId,
                       tbsFileMetaPage->GetWalId(), tbsFileMetaPage->GetPlsn(), tbsFileMetaPage->GetGlsn(),
                       glsnChangedFlag, m_ctrlDatafileItem.reuseVersion});
    walData.hwm = tbsFileMetaPage->hwm;
    walData.oid = tbsFileMetaPage->oid;
    walWriterContext->RememberPageNeedWal(fileMetaPageDesc);
    walWriterContext->PutNewWalRecord(&walData);
    (void)walWriterContext->EndAtomicWal();
    UnlockAndReleaseFileMetaPage(fileMetaPageDesc);

    ErrLog(DSTORE_LOG, MODULE_TABLESPACE,
           ErrMsg("Allocate next batch oid [%u, %u) success, fileId %hu, pdbId %u, isInitDb %d.", *nextOid,
                  walData.oid, m_fileId, m_pdbId, isInitDb));
    return DSTORE_SUCC;
}

void TbsDataFile::UpdateHwmIfNeed(const PageId &allocatedPageId, uint32 beforeAllocHwm, bool *isReUseFlag)
{
    AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
    /* 1.check with before hwm. extent must be reused if it's in front of before hwm. */
    if (allocatedPageId.m_blockId + static_cast<uint16>(m_extentSize) <= beforeAllocHwm) {
        *isReUseFlag = true;
        ErrLog(DSTORE_LOG, MODULE_TABLESPACE, ErrMsg("allocatedPageId(%hu, %u) is reused, before hwm(%u).",
            allocatedPageId.m_fileId, allocatedPageId.m_blockId, beforeAllocHwm));
        return;
    }
    PageId metaPageId = {m_fileId, TBS_FILE_META_PAGE};
    BufferDesc *fileMetaPageDesc = INVALID_BUFFER_DESC;
    TbsFileMetaPage *tbsFileMetaPage = nullptr;
    if (STORAGE_FUNC_FAIL(GetFileMetaPage(LW_EXCLUSIVE, &fileMetaPageDesc, &tbsFileMetaPage))) {
        ErrLog(DSTORE_PANIC, MODULE_TABLESPACE, ErrMsg("Failed to read tbsFileMetaPage."));
    }
    /* 2.check with current hwm. */
    if (allocatedPageId.m_blockId + static_cast<uint16>(m_extentSize) <= tbsFileMetaPage->hwm) {
        *isReUseFlag = true;
        UnlockAndReleaseFileMetaPage(fileMetaPageDesc);
        ErrLog(DSTORE_LOG, MODULE_TABLESPACE, ErrMsg("allocatedPageId(%hu, %u) is reused, real hwm(%u).",
            allocatedPageId.m_fileId, allocatedPageId.m_blockId, tbsFileMetaPage->hwm));
        return;
    }

    /* 3.now it's clear that this extent is not reused. need to update hwm */
    *isReUseFlag = false;
    tbsFileMetaPage->hwm = allocatedPageId.m_blockId + static_cast<uint16>(m_extentSize);
    if (!m_isTempFile) {
        fileMetaPageDesc->SetFileVersion(m_ctrlDatafileItem.reuseVersion);
        (void) GetBufferMgr()->MarkDirty(fileMetaPageDesc);
        bool glsnChangedFlag = (tbsFileMetaPage->GetWalId() != walWriterContext->GetWalId());
        walWriterContext->BeginAtomicWal(thrd->GetCurrentXid());
        WalRecordTbsUpdateTbsFileMetaPage walData;
        walData.SetHeader({WAL_TBS_UPDATE_TBS_FILE_META_PAGE, sizeof(WalRecordTbsUpdateTbsFileMetaPage), metaPageId,
           tbsFileMetaPage->GetWalId(), tbsFileMetaPage->GetPlsn(), tbsFileMetaPage->GetGlsn(),
           glsnChangedFlag, m_ctrlDatafileItem.reuseVersion});
        walData.hwm = tbsFileMetaPage->hwm;
        walData.oid = tbsFileMetaPage->oid;
        walWriterContext->RememberPageNeedWal(fileMetaPageDesc);
        walWriterContext->PutNewWalRecord(&walData);
        (void)walWriterContext->EndAtomicWal();
    }
    UnlockAndReleaseFileMetaPage(fileMetaPageDesc);
}

RetStatus TbsDataFile::UpdateFirstFreePageInGroup(uint16 groupIndex, uint8 firstFreePageNo, bool afterAlloc,
    bool needBeginAtomicWal)
{
    BufferDesc *bitmapMetaDesc = INVALID_BUFFER_DESC;
    TbsBitmapMetaPage *bitmapMeta = nullptr;
    if (STORAGE_FUNC_FAIL(GetBitmapMetaPage(LW_EXCLUSIVE, &bitmapMetaDesc, &bitmapMeta))) {
        return DSTORE_FAIL;
    }
    TbsBitMapGroup *bitmapGroup = &(bitmapMeta->bitmapGroups[groupIndex]);
    if ((afterAlloc && firstFreePageNo > bitmapGroup->firstFreePageNo) ||
        (!afterAlloc && firstFreePageNo < bitmapGroup->firstFreePageNo)) {
        bitmapGroup->firstFreePageNo = firstFreePageNo;
    } else {
        UnlockAndReleaseMetaPage(bitmapMetaDesc);
        ErrLog(DSTORE_LOG, MODULE_TABLESPACE,
            ErrMsg("firstFreePageNo has been updated, groupIndex = %hu, firstFreePageNo = %hhu.",
            groupIndex, firstFreePageNo));
        return DSTORE_SUCC;
    }

    if (!m_isTempFile) {
        bitmapMetaDesc->SetFileVersion(m_ctrlDatafileItem.reuseVersion);
        (void) GetBufferMgr()->MarkDirty(bitmapMetaDesc);
        /* write WAL record of WalRecordTbsUpdateFirstFreeBitmapPageId */
        bool glsnChangedFlag = (bitmapMeta->GetWalId() != thrd->m_walWriterContext->GetWalId());
        if (needBeginAtomicWal) {
            thrd->m_walWriterContext->BeginAtomicWal(thrd->GetCurrentXid());
        }
        WalRecordTbsUpdateFirstFreeBitmapPageId walRecord;
        walRecord.SetHeader({WAL_TBS_UPDATE_FIRST_FREE_BITMAP_PAGE, sizeof(WalRecordTbsUpdateFirstFreeBitmapPageId),
                             bitmapMetaDesc->GetPageId(), bitmapMeta->GetWalId(), bitmapMeta->GetPlsn(),
                             bitmapMeta->GetGlsn(), glsnChangedFlag, m_ctrlDatafileItem.reuseVersion});
        walRecord.SetData(groupIndex, firstFreePageNo);
        thrd->m_walWriterContext->RememberPageNeedWal(bitmapMetaDesc);
        thrd->m_walWriterContext->PutNewWalRecord(&walRecord);
        if (needBeginAtomicWal) {
            (void)thrd->m_walWriterContext->EndAtomicWal();
        }
    }
    UnlockAndReleaseMetaPage(bitmapMetaDesc);
    return DSTORE_SUCC;
}

RetStatus TbsDataFile::AllocExtentFromExistGroups(int64 *targetBlockCount,
                                                  PageId *newExtentPageId, uint16 *oldGroupCount, bool *isReUseFlag)
{
    BufferDesc *bitmapMetaDesc = INVALID_BUFFER_DESC;
    TbsBitmapMetaPage *bitmapMeta = nullptr;
    if (STORAGE_FUNC_FAIL(GetBitmapMetaPage(LW_SHARED, &bitmapMetaDesc, &bitmapMeta))) {  // Critical-Point
        return DSTORE_FAIL;
    }
    if (unlikely(bitmapMeta->groupCount == 0)) {
        ErrLog(DSTORE_PANIC, MODULE_TABLESPACE, ErrMsg("BitmapMetaPage(%hu, %u)(plsn %lu glsn %lu) not initialized! "
            "walID %lu extent_size %u idleGroupHints %hu. file %hu pdb %u.", bitmapMeta->GetSelfPageId().m_fileId,
            bitmapMeta->GetSelfPageId().m_blockId, bitmapMeta->GetPlsn(), bitmapMeta->GetGlsn(),
            bitmapMeta->GetWalId(), bitmapMeta->extentSize, bitmapMeta->idleGroupHints, m_fileId, m_pdbId));
    }

    TbsBitMapGroup *bitmapGroup = nullptr;
    uint32 i = bitmapMeta->idleGroupHints;
    *oldGroupCount = bitmapMeta->groupCount;
    while (i < bitmapMeta->groupCount) {
        bitmapGroup = &(bitmapMeta->bitmapGroups[i]);
        PageId currMapPage = bitmapGroup->firstBitmapPageId;
        currMapPage.m_blockId += bitmapGroup->firstFreePageNo;

        BufferDesc *fileMetaPageDesc = INVALID_BUFFER_DESC;
        TbsFileMetaPage *tbsFileMetaPage = nullptr;
        if (STORAGE_FUNC_FAIL(GetFileMetaPage(LW_SHARED, &fileMetaPageDesc, &tbsFileMetaPage))) {
            ErrLog(DSTORE_PANIC, MODULE_TABLESPACE, ErrMsg("Failed to read tbsFileMetaPage in pdb %u.", m_pdbId));
        }
        uint32 curHwm = tbsFileMetaPage->hwm;
        UnlockAndReleaseFileMetaPage(fileMetaPageDesc);

        uint8 firstFreePageNo = bitmapGroup->firstFreePageNo;
        for (uint16 j = bitmapGroup->firstFreePageNo; j < BITMAP_PAGES_PER_GROUP; j++) {
            PageId result = AllocExtentFromBitmapPage(currMapPage, targetBlockCount,
                                                      i, j, bitmapMeta);
            if (result != INVALID_PAGE_ID) {
                *newExtentPageId = result;
                UnlockAndReleaseMetaPage(bitmapMetaDesc);
                UpdateHwmIfNeed(result, curHwm, isReUseFlag);
                return DSTORE_SUCC;
            }

            if (*targetBlockCount >= (int64)m_maxBlockCount) {
                /* datafile size has exceed 2^32 * 8K */
                UnlockAndReleaseMetaPage(bitmapMetaDesc);
                storage_set_error(TBS_ERROR_FILE_SIZE_EXCEED_MAX_LIMIT);
                return DSTORE_FAIL;
            } else if (*targetBlockCount > 0) {
                ErrLog(DSTORE_WARNING, MODULE_TABLESPACE,
                       ErrMsg("The allocated block count %ld exceed File %hu's totalBlockCount(%lu). pdb %u",
                              *targetBlockCount, m_fileId, bitmapMeta->totalBlockCount, m_pdbId));
                /* we need extend data file */
                UnlockAndReleaseMetaPage(bitmapMetaDesc);
                storage_set_error(TBS_ERROR_FILE_SIZE_EXCEED_LIMIT);
                return DSTORE_FAIL;
            } else {
                /* current bitmapPage has used up */
                firstFreePageNo++;
                currMapPage.m_blockId++;
            }
        }
        /* after Alloc, if firstFreePageNo > bitmapGroup->firstFreePageNo,
           it need to call UpdateFirstFreePageInGroup to modify */
        if (firstFreePageNo > bitmapGroup->firstFreePageNo) {
            UnlockAndReleaseMetaPage(bitmapMetaDesc);
            if (STORAGE_FUNC_FAIL(UpdateFirstFreePageInGroup(i, firstFreePageNo, true, true))) {
                return DSTORE_FAIL;
            }
            if (STORAGE_FUNC_FAIL(GetBitmapMetaPage(LW_SHARED, &bitmapMetaDesc, &bitmapMeta))) {
                return DSTORE_FAIL;
            }
        }
        i++;
    }
    ErrLog(DSTORE_LOG, MODULE_TABLESPACE, ErrMsg("Datafile %hu's existing BitmapGroups has used up in pdb %u.",
        m_fileId, m_pdbId));
    UnlockAndReleaseMetaPage(bitmapMetaDesc);
    storage_set_error(TBS_ERROR_FILE_BITMAP_GROUP_USE_UP);
    return DSTORE_FAIL;
}

RetStatus TbsDataFile::ProcessFileSizeExceedLimit(int64 targetBlockCount)
{
    StorageAssert(targetBlockCount > 0);
    PageId bitmapMetaPageId = {m_fileId, TBS_BITMAP_META_PAGE};
    BufferDesc *bitmapMetaDesc = INVALID_BUFFER_DESC;
    TbsBitmapMetaPage *bitmapMetaPage = nullptr;
    AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
    if (STORAGE_FUNC_FAIL(GetBitmapMetaPage(LW_SHARED, &bitmapMetaDesc, &bitmapMetaPage))) {
        return DSTORE_FAIL;
    }

    if (bitmapMetaPage->totalBlockCount >= static_cast<uint64>(targetBlockCount)) {
        UnlockAndReleaseMetaPage(bitmapMetaDesc);
        return DSTORE_SUCC;
    }
    UnlockAndReleaseMetaPage(bitmapMetaDesc);
    LockTag tag;
    LockMgr *lockMgr = g_storageInstance->GetLockMgr();
    if (!m_isTempFile) {
        tag.SetTbsExtensionLockTag(m_pdbId, bitmapMetaPageId);
        LockErrorInfo errorInfo = {0};
        if (STORAGE_FUNC_FAIL(lockMgr->Lock(&tag, DSTORE_EXCLUSIVE_LOCK, false, &errorInfo))) {
            return DSTORE_FAIL;
        }
    }

    if (STORAGE_FUNC_FAIL(GetBitmapMetaPage(LW_SHARED, &bitmapMetaDesc, &bitmapMetaPage))) {
        if (!m_isTempFile) {
            lockMgr->Unlock(&tag, DSTORE_EXCLUSIVE_LOCK);
        }
        return DSTORE_FAIL;
    }

    if (bitmapMetaPage->totalBlockCount >= static_cast<uint64>(targetBlockCount)) {
        UnlockAndReleaseMetaPage(bitmapMetaDesc);
        if (!m_isTempFile) {
            lockMgr->Unlock(&tag, DSTORE_EXCLUSIVE_LOCK);
        }
        return DSTORE_SUCC;
    }

    uint64 realBlockCount = static_cast<uint64>(DSTORE_INVALID_BLOCK_NUMBER);
    if (STORAGE_FUNC_FAIL(ExtendDataFile(static_cast<uint64>(targetBlockCount), bitmapMetaPage, &realBlockCount))) {
        /* if extend data file failed, we need to handle errorCode */
        ErrorCode errCode = StorageGetErrorCode();
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("ExtendDataFile failed, pdbId %u, fileId %hu.", m_pdbId, m_fileId));

        UnlockAndReleaseMetaPage(bitmapMetaDesc);
        if (!m_isTempFile) {
            lockMgr->Unlock(&tag, DSTORE_EXCLUSIVE_LOCK);
            storage_set_error(errCode);
        }
        if (errCode == TBS_ERROR_FILE_SIZE_EXCEED_MAX_LIMIT) {
            storage_set_error(errCode);
        }
        return DSTORE_FAIL;
    }
    UnlockAndReleaseMetaPage(bitmapMetaDesc);
    if (STORAGE_FUNC_FAIL(GetBitmapMetaPage(LW_EXCLUSIVE, &bitmapMetaDesc, &bitmapMetaPage))) {
        if (!m_isTempFile) {
            lockMgr->Unlock(&tag, DSTORE_EXCLUSIVE_LOCK);
        }
        return DSTORE_FAIL;
    }

    if (realBlockCount != DSTORE_INVALID_BLOCK_NUMBER) {
        bitmapMetaPage->totalBlockCount = realBlockCount;
    }
    ErrLog(DSTORE_LOG, MODULE_TABLESPACE,
        ErrMsg("Successfully extend dataFile %hu to %lu blocks. pdb %u", m_fileId, realBlockCount, m_pdbId));
    if (m_isTempFile) {
        UnlockAndReleaseMetaPage(bitmapMetaDesc);
        return DSTORE_SUCC;
    }

    bitmapMetaDesc->SetFileVersion(m_ctrlDatafileItem.reuseVersion);
    (void) GetBufferMgr()->MarkDirty(bitmapMetaDesc);
    bool glsnChangedFlag = (bitmapMetaPage->GetWalId() != walWriterContext->GetWalId());
    walWriterContext->BeginAtomicWal(thrd->GetCurrentXid());
    WalRecordTbsExtendFile walDataTbsExtendFile;
    walDataTbsExtendFile.SetHeader({WAL_TBS_EXTEND_FILE, sizeof(WalRecordTbsExtendFile), bitmapMetaPageId,
                                    bitmapMetaPage->GetWalId(), bitmapMetaPage->GetPlsn(), bitmapMetaPage->GetGlsn(),
                                    glsnChangedFlag, m_ctrlDatafileItem.reuseVersion});
    walDataTbsExtendFile.totalBlockCount = bitmapMetaPage->totalBlockCount;
    walDataTbsExtendFile.fileId = m_fileId;
    walWriterContext->RememberPageNeedWal(bitmapMetaDesc);
    walWriterContext->PutNewWalRecord(&walDataTbsExtendFile);
    (void)walWriterContext->EndAtomicWal();
    UnlockAndReleaseMetaPage(bitmapMetaDesc);
    lockMgr->Unlock(&tag, DSTORE_EXCLUSIVE_LOCK);
    return DSTORE_SUCC;
}

RetStatus TbsDataFile::AllocExtent(PageId *newExtentPageId, bool *isReUseFlag)
{
    int64 targetBlockCount = -1;
    uint16 oldGroupCount = 0;
    if (STORAGE_FUNC_SUCC(AllocExtentFromExistGroups(&targetBlockCount, newExtentPageId,
                                                     &oldGroupCount, isReUseFlag))) {
        return DSTORE_SUCC;
    }
    do {
        ErrorCode errCode = StorageGetErrorCode();
        switch (errCode) {
            case TBS_ERROR_FILE_BITMAP_GROUP_USE_UP: {
                /* current bitmapGroup has no enough free bits */
                PageId metaPageId = {m_fileId, TBS_BITMAP_META_PAGE};
                LockTag tag;
                tag.SetTbsExtensionLockTag(m_pdbId, metaPageId);
                LockErrorInfo errorInfo = {0};
                LockMgr *lockMgr = g_storageInstance->GetLockMgr();
                if (STORAGE_FUNC_FAIL(lockMgr->Lock(&tag, DSTORE_EXCLUSIVE_LOCK, false, &errorInfo))) {
                    return DSTORE_FAIL;
                }
                if (STORAGE_FUNC_FAIL(AddBitmapGroup(oldGroupCount))) {
                    errCode = StorageGetErrorCode();
                    ErrLog(DSTORE_WARNING, MODULE_TABLESPACE,
                           ErrMsg("AddBitmapGroup failed, fileId is %hu.", m_fileId));
                    lockMgr->Unlock(&tag, DSTORE_EXCLUSIVE_LOCK);
                    storage_set_error(errCode);
                    return DSTORE_FAIL;
                }
                lockMgr->Unlock(&tag, DSTORE_EXCLUSIVE_LOCK);
                break;
            }
            case TBS_ERROR_FILE_SIZE_EXCEED_LIMIT: {
                if (STORAGE_FUNC_FAIL(ProcessFileSizeExceedLimit(targetBlockCount))) {
                    return DSTORE_FAIL;
                }
                break;
            }
            case TBS_ERROR_FILE_SIZE_EXCEED_MAX_LIMIT: {
                /* the current datafile's space has reached 2^32 * 8K, need to alloc from the next datafile. */
                storage_set_error(errCode);
                return DSTORE_FAIL;
            }
            default: {
                ErrLog(DSTORE_WARNING, MODULE_TABLESPACE, ErrMsg("Default errCode, FileId is %hu.", m_fileId));
                return DSTORE_FAIL;
            }
        }
        targetBlockCount = -1;
    } while (STORAGE_FUNC_FAIL(AllocExtentFromExistGroups(&targetBlockCount, newExtentPageId,
        &oldGroupCount, isReUseFlag)));
    return DSTORE_SUCC;
}

/*******************************************************************************
 * bitmap pages has 63 units (each unit has 128 Bytes),
    |000..0|000..0|000..0|000..0|000..0|000..0|000..0|000..0|
    |2Bytes|2Bytes|2Bytes|2Bytes|2Bytes|2Bytes|2Bytes|2Bytes|
    |***********************16Bytes*************************|****|....|****|
    |***********************128Bytes***************************************|
*******************************************************************************/
PageId TbsDataFile::AllocExtentFromBitmapPage(const PageId &bitmapPageId, int64 *targetBlockCount,
    uint32 bitmapGroup, uint32 freePage, TbsBitmapMetaPage *bitmapMeta)
{
    BufferDesc *bufferDesc = INVALID_BUFFER_DESC;
    TbsBitmapPage *bitmapPage = nullptr;
    if (STORAGE_FUNC_FAIL(GetBitmapPage(bitmapPageId, LW_EXCLUSIVE, &bufferDesc, &bitmapPage))) {  // Critical-Point
        return INVALID_PAGE_ID;
    }

    if (bitmapPage->allocatedExtentCount == static_cast<uint16>(DF_BITMAP_BIT_CNT)) {
        ErrLog(DSTORE_LOG, MODULE_TABLESPACE,
               ErrMsg("BitmapPageId(%hu,%u) used up, allocatedExtentCount(%hu). pdb %u",
                      bitmapPageId.m_fileId, bitmapPageId.m_blockId, bitmapPage->allocatedExtentCount, m_pdbId));
        UnlockAndReleasePage(bufferDesc, BufferPoolUnlockContentFlag::DontCheckCrc());
        return INVALID_PAGE_ID;
    }
    if (bitmapPage->allocatedExtentCount > static_cast<uint16>(DF_BITMAP_BIT_CNT)) {
        UnlockAndReleasePage(bufferDesc, BufferPoolUnlockContentFlag::DontCheckCrc());
        ErrLog(DSTORE_PANIC, MODULE_TABLESPACE,
               ErrMsg("BitmapPageId(%hu,%u) allocatedExtentCount(%hu) invalid! pdb %u",
                      bitmapPageId.m_fileId, bitmapPageId.m_blockId, bitmapPage->allocatedExtentCount, m_pdbId));
        return INVALID_PAGE_ID;
    }

    uint32 mapPageNo = bitmapGroup * BITMAP_PAGES_PER_GROUP + freePage;

    PageId result = INVALID_PAGE_ID;
    result = AllocExtentAsBit(mapPageNo, bitmapMeta, bufferDesc, bitmapPage, targetBlockCount);
    if (result == INVALID_PAGE_ID) {
        UnlockAndReleasePage(bufferDesc, BufferPoolUnlockContentFlag::DontCheckCrc());
        return result;
    }
    UnlockAndReleasePage(bufferDesc);
    return result;
}

void TbsDataFile::DoAddBitmapGroup(BufferDesc *bitmapMetaDesc, TbsBitmapMetaPage *mapMeta,
    const PageId &startMapNo, BlockNumber realBlockCount)
{
    PageId metaPageId = {m_fileId, TBS_BITMAP_META_PAGE};
    AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
    TbsBitMapGroup *bitmapGroup = &(mapMeta->bitmapGroups[mapMeta->groupCount]);
    uint16 groupIndex = mapMeta->groupCount;
    mapMeta->groupCount++;
    bitmapGroup->firstBitmapPageId = startMapNo;
    bitmapGroup->firstFreePageNo = 0;
    mapMeta->validOffset += sizeof(TbsBitMapGroup);
    if (realBlockCount != DSTORE_INVALID_BLOCK_NUMBER) {
        mapMeta->totalBlockCount = realBlockCount;
    }
    ErrLog(DSTORE_LOG, MODULE_TABLESPACE, ErrMsg("File %hu added bitmapGroup %hu, firstBitmapPage(%hu, %u)"
        "(plsn %lu glsn %lu). pdb %u.", m_fileId, mapMeta->groupCount, bitmapGroup->firstBitmapPageId.m_fileId,
        bitmapGroup->firstBitmapPageId.m_blockId, mapMeta->GetPlsn(), mapMeta->GetGlsn(), m_pdbId));
    if (m_isTempFile) {
        return;
    }
    bitmapMetaDesc->SetFileVersion(m_ctrlDatafileItem.reuseVersion);
    (void)GetBufferMgr()->MarkDirty(bitmapMetaDesc);
    bool glsnChangedFlag = (mapMeta->GetWalId() != walWriterContext->GetWalId());
    /* need to write Wal. log content:startMapNo, group_size, groupCount */
    WalRecordTbsAddBitmapPages walDataAddBitmapPages;
    walDataAddBitmapPages.groupCount = mapMeta->groupCount;
    walDataAddBitmapPages.groupIndex = groupIndex;
    walDataAddBitmapPages.groupFirstPage = bitmapGroup->firstBitmapPageId;
    walDataAddBitmapPages.groupFreePage = 0;
    walDataAddBitmapPages.validOffset = mapMeta->validOffset;
    walDataAddBitmapPages.groupPageCount = BITMAP_PAGES_PER_GROUP;
    walDataAddBitmapPages.SetHeader({WAL_TBS_ADD_BITMAP_PAGES, sizeof(WalRecordTbsAddBitmapPages), metaPageId,
                                     mapMeta->GetWalId(), mapMeta->GetPlsn(), mapMeta->GetGlsn(), glsnChangedFlag,
                                     m_ctrlDatafileItem.reuseVersion});
    walWriterContext->RememberPageNeedWal(bitmapMetaDesc);
    walWriterContext->PutNewWalRecord(&walDataAddBitmapPages);
    return;
}

RetStatus TbsDataFile::AddBitmapGroup(uint16 oldGroupCount)
{
    PageId groupFirstPage;
    PageId metaPageId = {m_fileId, TBS_BITMAP_META_PAGE};
    BufferDesc *bitmapMetaDesc = INVALID_BUFFER_DESC;
    TbsBitmapMetaPage *mapMeta = nullptr;
    AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
    if (STORAGE_FUNC_FAIL(GetBitmapMetaPage(LW_EXCLUSIVE, &bitmapMetaDesc, &mapMeta))) {  // Critical-Point
        return DSTORE_FAIL;
    }

    if (oldGroupCount < mapMeta->groupCount) {  /* group has been added */
        ErrLog(DSTORE_LOG, MODULE_TABLESPACE, ErrMsg("BitmapGroup %hu already exist. pdb %u file %hu groupCount %hu",
               oldGroupCount, m_pdbId, m_fileId, mapMeta->groupCount));
        UnlockAndReleaseMetaPage(bitmapMetaDesc);
        return DSTORE_SUCC;
    }

    if (mapMeta->groupCount == 0) {
        groupFirstPage = {m_fileId, TBS_BITMAP_META_PAGE + 1};
    } else {
        if (unlikely(m_bitmapMgr == nullptr)) {
            ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("m_bitmapMgr has not been inited"));
            UnlockAndReleaseMetaPage(bitmapMetaDesc);
            return DSTORE_FAIL;
        }
        groupFirstPage = m_bitmapMgr->GetNewGroupStart(mapMeta);
    }

    PageId startMapNo = groupFirstPage;
    uint64 targetBlockCount = static_cast<uint64>(startMapNo.m_blockId) + BITMAP_PAGES_PER_GROUP;
    if (targetBlockCount >= m_maxBlockCount) {
        ErrLog(DSTORE_LOG, MODULE_TABLESPACE,
               ErrMsg("FileBlockCount exceed TBS_ERROR_FILE_SIZE_EXCEED_MAX_LIMIT in AddBitmapGroup,"
                      "fileId is %hu, fileBlockCount is %u.",
                      startMapNo.m_fileId, startMapNo.m_blockId));
        UnlockAndReleaseMetaPage(bitmapMetaDesc);
        storage_set_error(TBS_ERROR_FILE_SIZE_EXCEED_MAX_LIMIT);
        return DSTORE_FAIL;
    }
    /* If the bitmap group to be added exceeds the actual size of the file, you need to extend the file. */
    uint64 realBlockCount = static_cast<uint64>(DSTORE_INVALID_BLOCK_NUMBER);
    if (targetBlockCount > mapMeta->totalBlockCount) {
        if (STORAGE_FUNC_FAIL(ExtendDataFile(targetBlockCount, mapMeta, &realBlockCount))) {
            ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
                   ErrMsg("ExtendDataFile failed, pdbId %u, fileId %hu, fileBlockCount %u.", m_pdbId, m_fileId,
                          startMapNo.m_blockId));
            UnlockAndReleaseMetaPage(bitmapMetaDesc);
            return DSTORE_FAIL;
        }
        mapMeta->totalBlockCount = realBlockCount;
        if (!m_isTempFile) {
            bitmapMetaDesc->SetFileVersion(m_ctrlDatafileItem.reuseVersion);
            (void) GetBufferMgr()->MarkDirty(bitmapMetaDesc);
            bool glsnChangedFlag = (mapMeta->GetWalId() != walWriterContext->GetWalId());
            walWriterContext->BeginAtomicWal(thrd->GetCurrentXid());
            WalRecordTbsExtendFile walDataTbsExtendFile;
            walDataTbsExtendFile.SetHeader({WAL_TBS_EXTEND_FILE, sizeof(WalRecordTbsExtendFile), metaPageId,
                                            mapMeta->GetWalId(), mapMeta->GetPlsn(), mapMeta->GetGlsn(),
                                            glsnChangedFlag, m_ctrlDatafileItem.reuseVersion});
            walDataTbsExtendFile.totalBlockCount = mapMeta->totalBlockCount;
            walDataTbsExtendFile.fileId = m_fileId;
            walWriterContext->RememberPageNeedWal(bitmapMetaDesc);
            walWriterContext->PutNewWalRecord(&walDataTbsExtendFile);
            (void)walWriterContext->EndAtomicWal();
        }
    }
    if (!m_isTempFile) {
        /* update  bitmapMeta Page */
        walWriterContext->BeginAtomicWal(thrd->GetCurrentXid());
    }

    /* init BITMAP_PAGES_PER_GROUP bitmap pages */
    if (STORAGE_FUNC_FAIL(InitBitmapPages(groupFirstPage))) {
        UnlockAndReleaseMetaPage(bitmapMetaDesc);
        if (!m_isTempFile) {
            walWriterContext->ResetForAbort();
        }
        return DSTORE_FAIL;
    }

    DoAddBitmapGroup(bitmapMetaDesc, mapMeta, startMapNo, realBlockCount);
    UnlockAndReleaseMetaPage(bitmapMetaDesc);
    if (!m_isTempFile) {
        (void)walWriterContext->EndAtomicWal();
    }
    return DSTORE_SUCC;
}

RetStatus TbsDataFile::ExtendDataFile(uint64 size, TbsBitmapMetaPage *bitmapMetaPage, uint64 *realBlockCount)
{
    /* initialize file with a specified size. */
    uint64 targetBlocks = static_cast<uint64>(DSTORE_INVALID_BLOCK_NUMBER);
    bool isTemplatePdb = IsTemplate(m_pdbId);
    if (bitmapMetaPage->totalBlockCount >= m_maxBlockCount) {
        ErrLog(DSTORE_LOG, MODULE_TABLESPACE,
               ErrMsg("FileBlockCount exceed TBS_ERROR_FILE_SIZE_EXCEED_MAX_LIMIT in AddBitmapGroup,"
                      " fileId is %hu, fileBlockCount is %lu, pdb is %u.",
                      m_fileId, bitmapMetaPage->totalBlockCount, m_pdbId));
        storage_set_error(TBS_ERROR_FILE_SIZE_EXCEED_MAX_LIMIT);
        return DSTORE_FAIL;
    }
    if (bitmapMetaPage->totalBlockCount >= FILE_EXTEND_BIG_STEP) {
        targetBlocks = DstoreRoundUp<uint64_t>(size, FILE_EXTEND_BIG_STEP);
    } else {
        targetBlocks =
            DstoreRoundUp<uint64_t>(size, (isTemplatePdb ? TEMPLATE_FILE_EXTEND_SMALL_STEP : FILE_EXTEND_SMALL_STEP));
    }
    if (targetBlocks > m_maxBlockCount) {
        ErrLog(DSTORE_LOG, MODULE_TABLESPACE,
               ErrMsg("FileBlockCount exceed TBS_ERROR_FILE_SIZE_EXCEED_MAX_LIMIT in AddBitmapGroup,"
                      " fileId is %hu, fileBlockCount is %lu, pdb is %u.",
                      m_fileId, targetBlocks, m_pdbId));
        storage_set_error(TBS_ERROR_FILE_SIZE_EXCEED_MAX_LIMIT);
        return DSTORE_FAIL;
    }

    TablespaceMgr *tablespaceMgr = g_storageInstance->GetTablespaceMgrByPdbId(m_pdbId);
    if (STORAGE_VAR_NULL(tablespaceMgr)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
            ErrMsg("Failed to get tablespaceMgr when extending dataFile, pdbId %u.", m_pdbId));
        return DSTORE_FAIL;
    }
    /* The tablespace is already locked. See SegmentInterface::AllocExtent. */
    TablespaceId m_tablespaceId = this->m_ctrlDatafileItem.tablespaceId;
    TableSpace *tablespace = tablespaceMgr->OpenTablespace(m_tablespaceId, DSTORE::DSTORE_NO_LOCK);
    if (STORAGE_VAR_NULL(tablespace)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to get tablespace(%u) when extending dataFile on pdb %u.", m_tablespaceId, m_pdbId));
        return DSTORE_FAIL;
    }

    ControlTablespacePageItemData ctrlTbsItem = tablespace->GetTbsCtrlItem();
    uint64 tablespaceMaxSize = ctrlTbsItem.tbsMaxSize;
    uint64 otherDatafileSize = 0;

    for (int i = 0; i < ctrlTbsItem.hwm; i++) {
        FileId fileId = ctrlTbsItem.fileIds[i];
        if (fileId == INVALID_DATA_FILE_ID) {
            continue;
        }
        if (fileId == m_fileId) {
            continue;
        }
        if (m_tablespaceId != static_cast<TablespaceId>(TBS_ID::TEMP_TABLE_SPACE_ID)) {
            TbsDataFile *datafile = tablespaceMgr->AcquireDatafile(fileId, LW_SHARED);
            if (STORAGE_VAR_NULL(datafile)) {
                ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("Acquire datafile failed on pdb %u, fileId is %hu.",
                    m_pdbId, fileId));
                return DSTORE_FAIL;
            }
            BufferDesc *bitmapMetaDesc = INVALID_BUFFER_DESC;
            TbsBitmapMetaPage *bitmapMeta = nullptr;
            if (STORAGE_FUNC_FAIL(datafile->GetBitmapMetaPage(LW_SHARED, &bitmapMetaDesc, &bitmapMeta))) {
                UnlockAndReleaseMetaPage(bitmapMetaDesc);
                tablespaceMgr->ReleaseDatafileLock(datafile);
                ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("Failed to read bitmapMetaPage on file %hu pdb %u.",
                    fileId, m_pdbId));
                return DSTORE_FAIL;
            }
            otherDatafileSize += bitmapMeta->totalBlockCount * BLCKSZ;
            UnlockAndReleaseMetaPage(bitmapMetaDesc);
            tablespaceMgr->ReleaseDatafileLock(datafile);
        } else {
            int64 currentFileSize = 0;
            currentFileSize = m_vfs->GetSize(fileId);
            if (currentFileSize < 0) {
                ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
                       ErrMsg("Get file size failed, tablespaceId(%u)fileId(%u).", m_tablespaceId,
                              ctrlTbsItem.fileIds[i]));
                return DSTORE_FAIL;
            }
            otherDatafileSize += (uint64)currentFileSize;
        }
    }
    tablespaceMgr->CloseTablespace(tablespace, DSTORE::DSTORE_NO_LOCK);

    uint64 currentTargetDataFileSize = targetBlocks * BLCKSZ;
    if (otherDatafileSize + currentTargetDataFileSize > tablespaceMaxSize) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Extend datafile failed, the requiredSize %lu exceeds tablespaceMaxsize %lu, pdbId %u, "
                      "tablespaceId %hu, fileId %hu, targetBlocks %lu.",
                      otherDatafileSize + currentTargetDataFileSize, tablespaceMaxSize, m_pdbId,
                      m_ctrlDatafileItem.tablespaceId, m_fileId, targetBlocks));
        storage_set_error(TBS_ERROR_TABLESPACE_USE_UP);
        return DSTORE_FAIL;
    }

    g_storageInstance->GetStat()->m_reportWaitEvent(
        static_cast<uint32_t>(GsStatWaitEvent::WAIT_EVENT_TBSDATAFILE_EXTEND_FILE));
    RetStatus rc = m_vfs->Extend(m_fileId, currentTargetDataFileSize);
    g_storageInstance->GetStat()->m_reportWaitEvent(static_cast<uint32_t>(OPTUTIL_GSSTAT_WAIT_EVENT_END));
    if (STORAGE_FUNC_FAIL(rc)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Extend data failed, pdbId %u, fileId %hu, targetBlocks %lu.", m_pdbId, m_fileId, targetBlocks));
        return DSTORE_FAIL;
    }
    *realBlockCount = targetBlocks;
    return DSTORE_SUCC;
}

RetStatus TbsDataFile::DoFreeExtent(TbsBitmapMetaPage *bitmapMetaPage, const PageId curExtentPageId,
    uint16 bitmapGroupNo, uint16 bitmapNo, uint16 bitNo)
{
    PageId bitmapPageFound;

    TbsBitMapGroup bitMapGroup = bitmapMetaPage->bitmapGroups[bitmapGroupNo];
    bitmapPageFound.m_fileId = bitMapGroup.firstBitmapPageId.m_fileId;
    bitmapPageFound.m_blockId = bitMapGroup.firstBitmapPageId.m_blockId + bitmapNo;

    BufferDesc *bitmapBuffer = INVALID_BUFFER_DESC;
    TbsBitmapPage *bitmapPage = nullptr;
    if (STORAGE_FUNC_FAIL(GetBitmapPage(bitmapPageFound, LW_EXCLUSIVE,  // Critical-Point
        &bitmapBuffer, &bitmapPage))) {
        return DSTORE_FAIL;
    }
    uint64 calcBlockId = (uint64)bitNo * (uint64)bitmapMetaPage->extentSize + bitmapPage->firstDataPageId.m_blockId;
    if (calcBlockId > DSTORE_MAX_BLOCK_NUMBER) {
        UnlockAndReleasePage(bitmapBuffer);
        ErrLog(DSTORE_PANIC, MODULE_TABLESPACE,
            ErrMsg("Bit %hu in bitmap page (%hu, %u) correspond invalid block number %lu!",
                bitNo, bitmapPageFound.m_fileId, bitmapPageFound.m_blockId, calcBlockId));
        return DSTORE_FAIL;
    }
    PageId calcExtentPageId;
    calcExtentPageId.m_fileId = bitmapPageFound.m_fileId;
    calcExtentPageId.m_blockId = calcBlockId;
    if (unlikely(calcExtentPageId != curExtentPageId)) {
        UnlockAndReleasePage(bitmapBuffer);
        ErrLog(DSTORE_PANIC, MODULE_TABLESPACE,
            ErrMsg("Bit %hu in bitmap page (%hu, %u) correspond with extent(%hu, %u), not (%hu, %u)!",
                bitNo, bitmapPageFound.m_fileId, bitmapPageFound.m_blockId,
                calcExtentPageId.m_fileId, calcExtentPageId.m_blockId,
                curExtentPageId.m_fileId, curExtentPageId.m_blockId));
        return DSTORE_FAIL;
    }
    if (unlikely(bitmapPage->TestBitZero(bitNo))) {
        UnlockAndReleasePage(bitmapBuffer);
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("Bit %hu in bitmap page (%hu, %u) has already been freed!",
                bitNo, bitmapPageFound.m_fileId, bitmapPageFound.m_blockId));
        return DSTORE_FAIL;
    }
    bitmapPage->UnsetByBit(bitNo);
    if (!m_isTempFile) {
        /* need to write wal */
        bitmapBuffer->SetFileVersion(m_ctrlDatafileItem.reuseVersion);
        (void) GetBufferMgr()->MarkDirty(bitmapBuffer);
        /* need to write wal */
        WriteWalForSetBit(bitmapBuffer, bitmapPage, bitmapPageFound, bitNo);
        ErrLog(DSTORE_LOG, MODULE_TABLESPACE,
            ErrMsg("Free bit %hu in bitmap page in (%hu, %u) success.",
                bitNo, bitmapPageFound.m_fileId, bitmapPageFound.m_blockId));
    }

    m_bitmapMgr->BackwardFreeBitsSearchPos(bitNo, bitmapNo);
    UnlockAndReleasePage(bitmapBuffer);
    return DSTORE_SUCC;
}

RetStatus TbsDataFile::FreeExtent(const PageId &curExtentPageId)
{
    BufferDesc *bitmapMetaDesc = INVALID_BUFFER_DESC;
    TbsBitmapMetaPage *bitmapMetaPage = nullptr;
    if (STORAGE_FUNC_FAIL(GetBitmapMetaPage(LW_SHARED, &bitmapMetaDesc, &bitmapMetaPage))) {  // Critical-Point
        return DSTORE_FAIL;
    }

    uint16 bitmapGroupNo = 0;
    uint8 bitmapNo = 0;
    uint16 bitNo = 0;
    bool needUpdate = false;

    /* find bitMapGroup,bitmapNo,bitNo by page_no of curExtentPageId */
    if (!LocateBitsPosByPageId(bitmapMetaPage, curExtentPageId, &bitmapGroupNo, &bitmapNo, &bitNo)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("LocateBitsPosByPageId failed. extent PageId id (%hu, %u).", curExtentPageId.m_fileId,
                      curExtentPageId.m_blockId));
        UnlockAndReleaseMetaPage(bitmapMetaDesc);
        storage_set_error(TBS_ERROR_PAGEID_INVALID);
        return DSTORE_FAIL;
    }
    needUpdate = (bitmapNo < bitmapMetaPage->bitmapGroups[bitmapGroupNo].firstFreePageNo);

    if (STORAGE_FUNC_FAIL(DoFreeExtent(bitmapMetaPage, curExtentPageId, bitmapGroupNo, bitmapNo, bitNo))) {
        UnlockAndReleaseMetaPage(bitmapMetaDesc);
        return DSTORE_FAIL;
    }
    UnlockAndReleaseMetaPage(bitmapMetaDesc);
    /* after FreeExtent, if bitmapNo < bitmapGroup->firstFreePageNo,
    it need to call UpdateFirstFreePageInGroup to modify */
    if (needUpdate && STORAGE_FUNC_FAIL(UpdateFirstFreePageInGroup(bitmapGroupNo, bitmapNo, false, false))) {
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

bool TbsDataFile::LocateBitsPosByPageId(TbsBitmapMetaPage *bitmapMetaPage, const PageId &extentMetaPageId,
                                        uint16 *mapGroupNo, uint8 *bitmapPageNo, uint16 *bitNo) const
{
    uint16 i;
    uint64 pageStart;
    uint64 pageEnd;

    /* find bitmap group, map page id in group and bit id in bitmap of this extent */
    for (i = 0; i < bitmapMetaPage->groupCount && i < MAX_BITMAP_GROUP_CNT; i++) {
        TbsBitMapGroup mapGroup = bitmapMetaPage->bitmapGroups[i];
        if (unlikely(extentMetaPageId.m_fileId != mapGroup.firstBitmapPageId.m_fileId)) {
            ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
                ErrMsg("extentMetaPageId(%hu, %u) not correspond with bitmapPage(%hu, %u). file %hu pdb %u",
                extentMetaPageId.m_fileId, extentMetaPageId.m_blockId, mapGroup.firstBitmapPageId.m_fileId,
                mapGroup.firstBitmapPageId.m_blockId, m_fileId, m_pdbId));
            return false;
        }
        pageStart = mapGroup.firstBitmapPageId.m_blockId + BITMAP_PAGES_PER_GROUP;
        pageEnd = pageStart +
                  (static_cast<uint64>(DF_BITMAP_BIT_CNT) * static_cast<uint64>(bitmapMetaPage->extentSize) *
                   static_cast<uint64>(BITMAP_PAGES_PER_GROUP));

        if (extentMetaPageId.m_blockId >= pageStart && extentMetaPageId.m_blockId < pageEnd) {
            if ((extentMetaPageId.m_blockId - pageStart) % bitmapMetaPage->extentSize != 0) {
                ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
                    ErrMsg("Extent(%hu, %u) is invalid on file %hu(extentsize %hu). bitmapGroup %hu: "
                    "firtsBitmapPageId(%hu, %u) pageStart %lu pageEnd %lu",
                    extentMetaPageId.m_fileId, extentMetaPageId.m_blockId, bitmapMetaPage->m_header.m_myself.m_fileId,
                    bitmapMetaPage->extentSize, i + 1, mapGroup.firstBitmapPageId.m_fileId,
                    mapGroup.firstBitmapPageId.m_blockId, pageStart, pageEnd));
                return false;
            }
            *mapGroupNo = i;
            *bitmapPageNo = static_cast<uint8>(((extentMetaPageId.m_blockId - pageStart) /
                bitmapMetaPage->extentSize) / DF_BITMAP_BIT_CNT);
            *bitNo = static_cast<uint16>((extentMetaPageId.m_blockId - pageStart) / bitmapMetaPage->extentSize %
                     DF_BITMAP_BIT_CNT);
            return true;
        }
    }
    
    ErrLog(DSTORE_ERROR, MODULE_TABLESPACE, ErrMsg("Extent(%hu, %u) is invalid on file %hu(extentsize %hu) pdb %u. "
        "bitmapMetaPage(%hu, %u)(plsn %lu glsn %lu) has %hu bitmapGroup.", extentMetaPageId.m_fileId,
        extentMetaPageId.m_blockId, m_fileId, bitmapMetaPage->extentSize, m_pdbId,
        bitmapMetaPage->GetSelfPageId().m_fileId, bitmapMetaPage->GetSelfPageId().m_blockId, bitmapMetaPage->GetPlsn(),
        bitmapMetaPage->GetGlsn(), bitmapMetaPage->groupCount));
    return false;
}

PageId TbsDataFile::AllocExtentAsBit(uint32 bitmapPageNo, TbsBitmapMetaPage *bitmapMeta,
    BufferDesc *bitmapBufferDesc, TbsBitmapPage *bitmapPage, int64 *targetBlockCount)
{
    uint32 begin = m_bitmapMgr->FindExtentStartPos(bitmapPageNo);
    if (begin >= DF_BITMAP_BIT_CNT) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("FindExtentStartPos failed on bitmapPageNo %u.", bitmapPageNo));
        return INVALID_PAGE_ID;
    }
    AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
    /* find 1 bit "0" from begin, the 'begin' pos is valid. */
    uint32 startFound = bitmapPage->FindFirstFreeBitByBit(begin);
    if (startFound >= DF_BITMAP_BIT_CNT) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("FindFirstFreeBitByBit failed, begin pos in bitmap is %d.", begin));
        return INVALID_PAGE_ID;
    }
    /* find page_id by startFound in bitmap */
    PageId result = bitmapPage->firstDataPageId;
    uint64 resultBlock = (uint64)startFound * (uint64)m_extentSize + result.m_blockId;
    if (resultBlock > DSTORE_MAX_BLOCK_NUMBER) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
           ErrMsg("Found bit %u in bitmapPage(%hu, %u) corrrespond with an invalid blocknumber %lu!",
           startFound, bitmapPage->m_header.m_myself.m_fileId, bitmapPage->m_header.m_myself.m_fileId, resultBlock));
        return INVALID_PAGE_ID;
    }
    result.m_blockId = resultBlock;

    /* check whether the allocated block exceeds the file size */
    uint64 tmpBlockCount = static_cast<uint64>(result.m_blockId) + m_extentSize;
    if (tmpBlockCount > bitmapMeta->totalBlockCount) {
        *targetBlockCount = tmpBlockCount;
        return INVALID_PAGE_ID;
    }
    if (unlikely(!bitmapPage->TestBitZero(startFound))) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
           ErrMsg("BitmapPage(%hu, %u) find free bit (%d) but check it already set!",
           bitmapPage->m_header.m_myself.m_fileId, bitmapPage->m_header.m_myself.m_fileId, startFound));
        return INVALID_PAGE_ID;
    }
    if (unlikely(m_bitmapMgr->ForwardFreeBitsSearchPos(startFound, bitmapPageNo))) {
        return INVALID_PAGE_ID;
    }
    bitmapPage->SetByBit(startFound);
    if (m_isTempFile) {
        return result;
    }
    /* need to write wal */
    bitmapBufferDesc->SetFileVersion(m_ctrlDatafileItem.reuseVersion);
    (void) GetBufferMgr()->MarkDirty(bitmapBufferDesc);
    bool glsnChangedFlag = (bitmapPage->GetWalId() != walWriterContext->GetWalId());
    /* need to write wal */
    /* WAL_TBS_BITMAP_ALLOC_BIT_END will be recorded later.
     * After a log of the WAL_TBS_BITMAP_ALLOC_BIT_START type is recorded,
     * a log of the WAL_TBS_BITMAP_ALLOC_BIT_END type needs to be recorded to prevent extent leakage.
     */
    walWriterContext->BeginAtomicWal(thrd->GetCurrentXid());
    WalRecordTbsBitmapSetBit walData;
    walData.SetHeader({WAL_TBS_BITMAP_ALLOC_BIT_START, sizeof(WalRecordTbsBitmapSetBit), bitmapBufferDesc->GetPageId(),
                       bitmapPage->GetWalId(), bitmapPage->GetPlsn(), bitmapPage->GetGlsn(), glsnChangedFlag,
                       m_ctrlDatafileItem.reuseVersion});
    walData.startBitPos = startFound;
    walData.value = 1;
    walData.allocatedExtentCount = bitmapPage->allocatedExtentCount;
    walWriterContext->RememberPageNeedWal(bitmapBufferDesc);
    walWriterContext->PutNewWalRecord(&walData);
    (void)walWriterContext->EndAtomicWal();

    ErrLog(DSTORE_LOG, MODULE_TABLESPACE,
        ErrMsg("Alloc bit in bitmap page(%hu, %u)(plsn %lu, glsn %lu) for Extent(%hu, %u) success. pdb %u",
            bitmapBufferDesc->GetPageId().m_fileId, bitmapBufferDesc->GetPageId().m_blockId,
            bitmapPage->GetPlsn(), bitmapPage->GetGlsn(), result.m_fileId, result.m_blockId, m_pdbId));
    return result;
}

RetStatus TbsDataFile::InitBitmapPages(PageId groupFirstPage)
{
    PageId firstPageId = groupFirstPage;
    PageId dataStart = groupFirstPage;
    dataStart.m_blockId += BITMAP_PAGES_PER_GROUP;
    PageId firstDataPageId = dataStart;
    AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
    WalRecordLsnInfo preWalPointer[BITMAP_PAGES_PER_GROUP];
    for (uint32 i = 0; i < BITMAP_PAGES_PER_GROUP; i++) {
        BufferDesc *pageBufDesc = INVALID_BUFFER_DESC;
        TbsBitmapPage *page = nullptr;
        if (STORAGE_FUNC_FAIL(GetBitmapPage(groupFirstPage, LW_EXCLUSIVE, &pageBufDesc, &page))) {
            return DSTORE_FAIL;
        }

        pageBufDesc->SetFileVersion(m_ctrlDatafileItem.reuseVersion);
        if (!m_isTempFile) {
            page->InitBitmapPage(pageBufDesc->GetPageId(), dataStart);
            preWalPointer[i].SetValue(page);
            (void) GetBufferMgr()->MarkDirty(pageBufDesc);
            walWriterContext->RememberPageNeedWal(pageBufDesc);
        } else {
            page->InitBitmapPage(groupFirstPage, dataStart);
        }
        UnlockAndReleasePage(pageBufDesc);

        dataStart.m_blockId += DF_BITMAP_BIT_CNT * static_cast<uint16>(m_extentSize);
        groupFirstPage.m_blockId++;
    }
    if (m_isTempFile) {
        return DSTORE_SUCC;
    }
    uint32 walDataSize = sizeof(WalRecordTbsInitBitmapPages) + BITMAP_PAGES_PER_GROUP * sizeof(WalRecordLsnInfo);
    WalRecordTbsInitBitmapPages *walDataPtr = (WalRecordTbsInitBitmapPages *) DstorePalloc0(walDataSize);
    if (STORAGE_VAR_NULL(walDataPtr)) {
        ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
               ErrMsg("Failed to alloc WalRecordTbsInitBitmapPages for fileId %hu, pdbId %u, firstPageId (%u, %u), "
                      "walDataSize %u.",
                      m_fileId, m_pdbId, firstPageId.m_fileId, firstPageId.m_blockId, walDataSize));
        return DSTORE_FAIL;
    }

    walDataPtr->SetHeader(WAL_TBS_INIT_BITMAP_PAGES, walDataSize, m_ctrlDatafileItem.reuseVersion);
    walDataPtr->SetData(PageType::TBS_BITMAP_PAGE_TYPE, firstPageId, firstDataPageId, BITMAP_PAGES_PER_GROUP,
                        m_extentSize, preWalPointer);

    /* need to write logic Wal for BITMAP_PAGES_PER_GROUP bitmap pages. */
    walWriterContext->PutNewWalRecord(walDataPtr);
    DstorePfreeExt(walDataPtr);
    return DSTORE_SUCC;
}

void TbsDataFile::WriteWalForSetBit(
    BufferDesc *bitmapBuffer, TbsBitmapPage *bitmapPage, const PageId &bitmapPageFound, uint16 bitNo)
{
    WalRecordTbsBitmapSetBit walData;
    AtomicWalWriterContext *walWriterContext = thrd->m_walWriterContext;
    bool glsnChangedFlag = (bitmapPage->GetWalId() != walWriterContext->GetWalId());
    walData.SetHeader({WAL_TBS_BITMAP_FREE_BIT_START, sizeof(WalRecordTbsBitmapSetBit), bitmapPageFound,
                       bitmapPage->GetWalId(), bitmapPage->GetPlsn(), bitmapPage->GetGlsn(), glsnChangedFlag,
                       m_ctrlDatafileItem.reuseVersion});
    walData.startBitPos = bitNo;
    walData.value = 0;
    walData.allocatedExtentCount = bitmapPage->allocatedExtentCount;
    walWriterContext->RememberPageNeedWal(bitmapBuffer);
    walWriterContext->PutNewWalRecord(&walData);
}

TempTbsDataFile::TempTbsDataFile(PdbId pdbId, VFSAdapter *vfs, FileId fileId, uint64 maxBlockCount,
                                 TbsTempBitmapPageHashTable *bitmapPagetable)
    : TbsDataFile(pdbId, vfs, fileId, maxBlockCount, TEMP_TABLE_EXT_SIZE, true),
      m_bitmapPagetable(bitmapPagetable)
{}

TempTbsDataFile::~TempTbsDataFile()
{}

RetStatus TempTbsDataFile::GetFileMetaPage(LWLockMode lock, BufferDesc **pageDesc, TbsFileMetaPage **page)
{
    PageId pageId = {GetFileId(), TBS_FILE_META_PAGE};
    TbsBitmapPage *bitmapPage = nullptr;
    if (STORAGE_FUNC_FAIL(GetBitmapPage(pageId, lock, pageDesc, &bitmapPage))) {  // Critical-Point
        return DSTORE_FAIL;
    }
    *page = static_cast<TbsFileMetaPage *>(static_cast<void *>(bitmapPage));
    return DSTORE_SUCC;
}

RetStatus TempTbsDataFile::GetSpaceMetaPage(LWLockMode lock, BufferDesc **pageDesc, TbsSpaceMetaPage **page)
{
    PageId pageId = {GetFileId(), TBS_SPACE_META_PAGE};
    TbsBitmapPage *bitmapPage = nullptr;
    if (STORAGE_FUNC_FAIL(GetBitmapPage(pageId, lock, pageDesc, &bitmapPage))) {  // Critical-Point
        return DSTORE_FAIL;
    }
    *page = static_cast<TbsSpaceMetaPage *>(static_cast<void *>(bitmapPage));
    return DSTORE_SUCC;
}

void TempTbsDataFile::UnlockAndReleaseFileMetaPage(BufferDesc *pageDesc)
{
    return UnlockAndReleasePage(pageDesc);
}

RetStatus TempTbsDataFile::GetBitmapMetaPage(LWLockMode lock,
    BufferDesc **pageDesc, TbsBitmapMetaPage **page)
{
    PageId pageId = {GetFileId(), TBS_BITMAP_META_PAGE};
    TbsBitmapPage *bitmapPage = nullptr;
    if (STORAGE_FUNC_FAIL(GetBitmapPage(pageId, lock, pageDesc, &bitmapPage))) {
        return DSTORE_FAIL;
    }
    *page = static_cast<TbsBitmapMetaPage *>(static_cast<void *>(bitmapPage));
    return DSTORE_SUCC;
}

void TempTbsDataFile::UnlockAndReleaseMetaPage(BufferDesc *pageDesc)
{
    return UnlockAndReleasePage(pageDesc);
}

RetStatus TempTbsDataFile::GetBitmapPage(const PageId &pageId, LWLockMode lock,
    BufferDesc **pageDesc, TbsBitmapPage **page)
{
    TbsPageItem *existPageItem = m_bitmapPagetable->LookUp(pageId);
    if (STORAGE_VAR_NULL(existPageItem)) {
        ErrLog(DSTORE_WARNING, MODULE_TABLESPACE,
            ErrMsg("Get temp BitmapPage null(%u, %u), alloc new now.", pageId.m_fileId, pageId.m_blockId));
        /* create new page */
        AutoMemCxtSwitch autoSwtich{g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LONGLIVE)};
        TbsPageItem *newPageItem = static_cast<TbsPageItem *>(DstorePalloc0(BLCKSZ + sizeof(TbsPageItem)));
        if (STORAGE_VAR_NULL(newPageItem)) {
            *page = nullptr;
            *pageDesc = nullptr;
            ErrLog(DSTORE_ERROR, MODULE_TABLESPACE,
                ErrMsg("Get new temp BitmapPage null(%u, %u)", pageId.m_fileId, pageId.m_blockId));
            return DSTORE_FAIL;
        }
        LWLockInitialize(&newPageItem->pageLock, LWLOCK_GROUP_TBS_PAGE);
        existPageItem = m_bitmapPagetable->Insert(pageId, newPageItem);
        if (existPageItem != nullptr) {
            /* Already exist, use the exist pageItem. */
            DstorePfreeExt(newPageItem);
        } else {
            /* Not exist, use the new pageItem. */
            existPageItem = newPageItem;
        }
    }

    DstoreLWLockAcquireByMode(&existPageItem->pageLock, lock)
    *page = &existPageItem->bitmapPage;
    *pageDesc = static_cast<BufferDesc *>(static_cast<void *>(existPageItem));
    return DSTORE_SUCC;
}

void TempTbsDataFile::UnlockAndReleasePage(BufferDesc *pageDesc, UNUSE_PARAM BufferPoolUnlockContentFlag flag)
{
    TbsPageItem *pageItem = static_cast<TbsPageItem *>(static_cast<void *>(pageDesc));
    LWLockRelease(&pageItem->pageLock);
}

void TempTbsDataFile::Destroy()
{
    TbsDataFile::Destroy();
}

}
