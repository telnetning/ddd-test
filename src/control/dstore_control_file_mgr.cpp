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
 * dstore_control_file_mgr.cpp
 *
 *
 *
 * IDENTIFICATION
 *        dstore/src/control/dstore_control_file_mgr.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "common/memory/dstore_mctx.h"
#include "control/dstore_control_file_page.h"
#include "control/dstore_control_cache.h"
#include "control/dstore_control_file_mgr.h"

namespace DSTORE {

constexpr uint32 CONTROL_PAGE_BUF_VALID = (1UL << 1); /* data is valid */
constexpr uint32 CONTROL_PAGE_BUF_DIRTY = (1UL << 2); /* data is dirty */

constexpr uint16 CONTROL_PAGE_HANDLE_BASE_SIZE = 10;

constexpr const char *CONTROL_DISK_FILE_1_NAME = "database_control_1";
constexpr const char *CONTROL_DISK_FILE_2_NAME = "database_control_2";

ControlFileMgr::ControlFileMgr(PdbId pdbId, DeployType deployType, DstoreMemoryContext context)
    : m_file1(nullptr),
      m_file2(nullptr),
      m_pdbId(pdbId),
      m_pageBuffer{nullptr},
      m_state{nullptr},
      m_pageCount{0},
      m_deployType(deployType),
      m_memoryContext{context},
      m_enableCachePage{true}

{}

ControlFileMgr::~ControlFileMgr()
{
    FreePageBuffer(m_pageCount, m_pageBuffer);
    DstorePfreeExt(m_state);
    m_pageCount = 0;
    if (m_file1 != nullptr) {
        delete m_file1;
        m_file1 = nullptr;
    }
    if (m_file2 != nullptr) {
        delete m_file2;
        m_file2 = nullptr;
    }
}

void ControlFileMgr::FreePageBuffer(uint32 pageCount, char **pageBuffer) const noexcept
{
    if (pageBuffer == nullptr) {
        return;
    }
    for (uint32 i = 0; i < pageCount; i++) {
        DstorePfreeExt(pageBuffer[i]);
    }
    DstorePfreeExt(pageBuffer);
}

RetStatus ControlFileMgr::Init(VFSAdapter *vfs, char *storeSpaceName, const char *dataDir)
{
    if (unlikely(m_memoryContext == nullptr || vfs == nullptr || storeSpaceName == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Invalid param when init control file mgr."));
        return DSTORE_FAIL;
    }

    m_file1 = DstoreNew(m_memoryContext) ControlDiskFile(vfs, storeSpaceName);
    if (unlikely(m_file1 == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("No memory when construct control disk file1."));
        return DSTORE_FAIL;
    }
    m_file2 = DstoreNew(m_memoryContext) ControlDiskFile(vfs, storeSpaceName);
    if (unlikely(m_file2 == nullptr)) {
        delete m_file1;
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("No memory when construct control disk file2."));
        return DSTORE_FAIL;
    }

    if (STORAGE_FUNC_FAIL(m_file1->Init(dataDir, CONTROL_DISK_FILE_1_NAME))) {
        delete m_file1;
        delete m_file2;
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Init control disk file1 failed."));
        return DSTORE_FAIL;
    }
    if (STORAGE_FUNC_FAIL(m_file2->Init(dataDir, CONTROL_DISK_FILE_2_NAME))) {
        delete m_file1;
        delete m_file2;
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Init control disk file2 failed."));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus ControlFileMgr::AllocPageBuffer(uint32 blockCount)
{
    if (m_pageBuffer != nullptr) {
        return DSTORE_SUCC;
    }
    if (unlikely(m_memoryContext == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Invalid mem ctx when alloc page buffer."));
        return DSTORE_FAIL;
    }
    char **tempPageBuffer =
        static_cast<char **>(DstoreMemoryContextAllocZero(m_memoryContext, (blockCount * sizeof(char *))));
    if (unlikely(tempPageBuffer == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Alloc control page buffer failed."));
        return DSTORE_FAIL;
    }
    uint32 *tempState =
        static_cast<uint32 *>(DstoreMemoryContextAllocZero(m_memoryContext, (blockCount * sizeof(uint32))));
    if (unlikely(tempState == nullptr)) {
        DstorePfreeExt(tempPageBuffer);
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Alloc control page state failed."));
        return DSTORE_FAIL;
    }
    for (uint32 i = m_pageCount; i < blockCount; i++) {
        tempPageBuffer[i] = static_cast<char *>(DstoreMemoryContextAllocZero(m_memoryContext, BLCKSZ));
        if (unlikely(tempPageBuffer[i] == nullptr)) {
            DstorePfreeExt(tempState);
            FreePageBuffer(i, tempPageBuffer); /* m_pageCount is 0 when create control file */
            ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Alloc control page buffer failed."));
            return DSTORE_FAIL;
        }
        tempState[i] &= (~(CONTROL_PAGE_BUF_DIRTY | CONTROL_PAGE_BUF_VALID));
    }
    m_pageBuffer = tempPageBuffer;
    m_state = tempState;
    m_pageCount = blockCount;
    return DSTORE_SUCC;
}

void ControlFileMgr::MarkPageStateValid(uint32 blockNumber)
{
    StorageAssert(blockNumber < m_pageCount);
    if (m_enableCachePage.load(std::memory_order_acquire)) {
        m_state[blockNumber] |= CONTROL_PAGE_BUF_VALID;
    }
}

void ControlFileMgr::MarkPageStateDirty(uint32 blockNumber)
{
    StorageAssert(blockNumber < m_pageCount);
    m_state[blockNumber] |= CONTROL_PAGE_BUF_DIRTY;
}

void ControlFileMgr::CleanPageStateDirty(uint32 blockNumber)
{
    StorageAssert(blockNumber < m_pageCount);
    m_state[blockNumber] &= (~CONTROL_PAGE_BUF_DIRTY);
}

void ControlFileMgr::CleanPageStateValid(uint32 blockNumber)
{
    StorageAssert(blockNumber < m_pageCount);
    m_state[blockNumber] &= (~CONTROL_PAGE_BUF_VALID);
}

bool ControlFileMgr::IsPageStateDirty(uint32 blockNumber) const
{
    StorageAssert(blockNumber < m_pageCount);
    return static_cast<bool>(m_state[blockNumber] & CONTROL_PAGE_BUF_DIRTY);
}

bool ControlFileMgr::IsPageStateValid(uint32 blockNumber) const
{
    StorageAssert(blockNumber < m_pageCount);
    return static_cast<bool>(m_state[blockNumber] & CONTROL_PAGE_BUF_VALID);
}

char *ControlFileMgr::GetPage(BlockNumber blockNumber, ControlDiskFile *file)
{
    if (blockNumber >= m_pageCount) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL,
               ErrMsg("Get control page failed for wrong blockNumber, blockNumber(%u).", blockNumber));
        return nullptr;
    }
    /* if page in the buffer is not valid, read page into the buffer */
    if (!IsPageStateValid(blockNumber) && !IsPageStateDirty(blockNumber)) {
        if (unlikely(file == nullptr)) {
            ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("No valid file to read."));
            return nullptr;
        }
        if (STORAGE_FUNC_FAIL(file->ReadPage(blockNumber, m_pageBuffer[blockNumber]))) {
            ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Read control page failed: blockNumber(%u).", blockNumber));
            return nullptr;
        }
        bool isMatch = false;
        RetStatus ret = CheckPageCrcMatch(file, blockNumber, isMatch);
        if (STORAGE_FUNC_FAIL(ret) || !isMatch) {
            ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Check page crc match failed: blockNumber(%u).", blockNumber));
            return nullptr;
        }
        MarkPageStateValid(blockNumber);
    }
    return m_pageBuffer[blockNumber];
}

ControlFileMetaPage *ControlFileMgr::GetMetaPage(ControlDiskFile *file)
{
    return reinterpret_cast<ControlFileMetaPage *>(GetPage(CONTROLFILE_PAGEMAP_FILEMETA, file));
}

RetStatus ControlFileMgr::CreateOneControlFile(ControlDiskFile *file)
{
    if (unlikely(file == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("File is nullptr when create one control file."));
        return DSTORE_FAIL;
    }
    RetStatus ret = file->Create();
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Create control disk file failed."));
        return DSTORE_FAIL;
    }
    ret = file->Extend(CONTROLFILE_PAGEMAP_MAX * BLCKSZ);
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Extend control disk file failed."));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

void ControlFileMgr::UpdateControlPageCrc(uint32 *checksum, const uint8 *page)
{
    *checksum = 0;
    *checksum = CompChecksum(page, BLCKSZ, CHECKSUM_CRC);
}

RetStatus ControlFileMgr::CreateControlFiles()
{
    /* Step 1. Create disk control file. */
    RetStatus ret = CreateOneControlFile(m_file1);
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Create control disk file1 failed."));
        return DSTORE_FAIL;
    }
    ret = CreateOneControlFile(m_file2);
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Create control disk file2 failed."));
        return DSTORE_FAIL;
    }
    /* Step 2. Alloc page buffer and mark m_pageBuffer vaild. */
    ret = AllocPageBuffer(CONTROLFILE_PAGEMAP_MAX);
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Alloc control file page buffer failed."));
        return DSTORE_FAIL;
    }
    for (uint32 i = CONTROLFILE_PAGEMAP_FILEMETA; i < CONTROLFILE_PAGEMAP_MAX; i++) {
        MarkPageStateValid(i);
    }
    /* Step 3. Initialize control file meta page */
    ControlFileMetaPage *fileMetaPage = GetMetaPage(m_file1);
    if (fileMetaPage == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Get MataPage of control file failed."));
        return DSTORE_FAIL;
    }
    fileMetaPage->InitFileMetaPage();
    MarkPageStateDirty(CONTROLFILE_PAGEMAP_FILEMETA);
    /* Step 4. Initialize control file group meta pages */
    for (BlockNumber i = CONTROLFILE_PAGEMAP_FILEMETA + 1; i <= CONTROLFILE_PAGEMAP_METAPAGE_MAX; i++) {
        ControlMetaPage *metaPage = static_cast<ControlMetaPage *>(static_cast<void *>(GetPage(i, m_file1)));
        if (metaPage == nullptr) {
            ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Get group meta page of control file failed."));
            return DSTORE_FAIL;
        }
        metaPage->InitMetaPage(ControlPageType::CONTROL_PAGE_TYPE_INVALID, 0);
        MarkPageStateDirty(i);
    }
    /* Step 5. Initialize control file data pages. */
    for (BlockNumber i = CONTROLFILE_PAGEMAP_METAPAGE_MAX + 1; i < CONTROLFILE_PAGEMAP_MAX; i++) {
        ControlDataPage *dataPage = static_cast<ControlDataPage *>(static_cast<void *>(GetPage(i, m_file1)));
        if (dataPage == nullptr) {
            ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Get data page of control file failed."));
            return DSTORE_FAIL;
        }
        dataPage->InitDataPage(ControlPageType::CONTROL_PAGE_TYPE_INVALID);
        MarkPageStateDirty(i);
    }
    return DSTORE_SUCC;
}

RetStatus ControlFileMgr::OpenControlFiles()
{
    if (unlikely(m_file1 == nullptr) || unlikely(m_file2 == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Open control file failed because file is nullptr."));
        return DSTORE_FAIL;
    }
    if (STORAGE_FUNC_FAIL(m_file1->Open()) || STORAGE_FUNC_FAIL(m_file2->Open())) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Open control disk file failed."));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus ControlFileMgr::CloseControlFiles()
{
    if (unlikely(m_file1 == nullptr) || unlikely(m_file2 == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Close control file failed because file is nullptr."));
        return DSTORE_FAIL;
    }
    if (STORAGE_FUNC_FAIL(m_file1->Close()) || STORAGE_FUNC_FAIL(m_file2->Close())) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Close control disk file failed."));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus ControlFileMgr::LoadControlFile()
{
    RetStatus ret = AllocPageBuffer(CONTROLFILE_PAGEMAP_MAX);
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Alloc control file page buffer failed."));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus ControlFileMgr::CheckMetaPage(bool *isValid, bool *isFileWriting, uint64 *term, BlockNumber metaBlockNum,
                                        ControlDiskFile *file)
{
    if (unlikely(m_memoryContext == nullptr)) {
         ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Invalid mem ctx when check meta page."));
        return DSTORE_FAIL;
    }
    ControlMetaPage *page = reinterpret_cast<ControlMetaPage *>(DstoreMemoryContextAllocZero(m_memoryContext, BLCKSZ));
    if (unlikely(page == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Alloc control file page failed when check meta page."));
        return DSTORE_FAIL;
    }
    RetStatus ret = file->ReadPage(metaBlockNum, page);
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Read file group meta page failed."));
        DstorePfreeExt(page);
        return DSTORE_FAIL;
    }
    *term = page->GetTerm();
    *isFileWriting = page->CheckIfWriting();
    bool isMatch = false;
    ret = CheckPageCrcMatch(file, metaBlockNum, isMatch);
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Check page crc match failed."));
        DstorePfreeExt(page);
        return DSTORE_FAIL;
    }
    if (unlikely(!isMatch || *isFileWriting)) {
        *isValid = false;
        ErrLog(DSTORE_LOG, MODULE_CONTROL,
               ErrMsg("Check meta page(%u) with invalid result of isMatch(%d) and isWriting(%d).",
                      metaBlockNum, isMatch, *isFileWriting));
        DstorePfreeExt(page);
        return DSTORE_SUCC;
    }
    *isValid = true;
    DstorePfreeExt(page);
    return DSTORE_SUCC;
}

RetStatus ControlFileMgr::GetValidMetaPage(PageHandle *pageHandle, BlockNumber metaBlockNum)
{
    if (unlikely(pageHandle == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("GetValidMetaPage pageHandle is nullptr."));
        return DSTORE_FAIL;
    }
    /* Step 1. Check meta page. */
    bool isFile1MetaPageValid = false;
    bool isFile1Writing = false;
    uint64 file1MetaPageTerm = 0;
    if (STORAGE_FUNC_FAIL(
            CheckMetaPage(&isFile1MetaPageValid, &isFile1Writing, &file1MetaPageTerm, metaBlockNum, m_file1))) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Check meta page1 failed when get page handle."));
        return DSTORE_FAIL;
    }
    bool isFile2MetaPageValid = false;
    bool isFile2Writing = false;
    uint64 file2MetaPageTerm = 0;
    if (STORAGE_FUNC_FAIL(
            CheckMetaPage(&isFile2MetaPageValid, &isFile2Writing, &file2MetaPageTerm, metaBlockNum, m_file2))) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Check meta page2 failed when get page handle."));
        return DSTORE_FAIL;
    }

    if (unlikely(!isFile1MetaPageValid && !isFile2MetaPageValid)) {
        // If both controlfiles are in writing state, return fail instead of panic.
        if (isFile1Writing && isFile2Writing) {
            ErrLog(DSTORE_ERROR, MODULE_CONTROL,
               ErrMsg("No valid page handle to get page(%u), both controlfiles are in writing state", metaBlockNum));
            return DSTORE_FAIL;
        }
        char *pageBuffer1 = static_cast<char *>(DstoreMemoryContextAllocZero(m_memoryContext, BLCKSZ));
        char *pageBuffer2 = static_cast<char *>(DstoreMemoryContextAllocZero(m_memoryContext, BLCKSZ));
        if (unlikely(pageBuffer1 == nullptr || pageBuffer2 == nullptr)) {
            DstorePfreeExt(pageBuffer1);
            DstorePfreeExt(pageBuffer2);
            ErrLog(DSTORE_PANIC, MODULE_CONTROL, ErrMsg("Failed to alloc memory when no valid page handle."));
            return DSTORE_FAIL;
        }
        RetStatus ret1 = m_file1->ReadPage(metaBlockNum, pageBuffer1);
        RetStatus ret2 = m_file2->ReadPage(metaBlockNum, pageBuffer2);
        if (STORAGE_FUNC_FAIL(ret1) || STORAGE_FUNC_FAIL(ret2)) {
            DstorePfreeExt(pageBuffer1);
            DstorePfreeExt(pageBuffer2);
            ErrLog(DSTORE_PANIC, MODULE_CONTROL,
                   ErrMsg("Read page(%u) failed when no valid page handle.", metaBlockNum));
            return DSTORE_FAIL;
        }

        ControlBasePage *page1 = STATIC_CAST_PTR_TYPE(pageBuffer1, ControlBasePage *);
        char *content1 = page1->Dump();
        ControlBasePage *page2 = STATIC_CAST_PTR_TYPE(pageBuffer2, ControlBasePage *);
        char *content2 = page2->Dump();
        ErrLog(DSTORE_PANIC, MODULE_CONTROL,
               ErrMsg("No valid page handle to get page(%u) with content1:\n%s \nand content2:\n%s",
                      metaBlockNum, content1, content2));
        DstorePfreeExt(pageBuffer1);
        DstorePfreeExt(pageBuffer2);
        DstorePfreeExt(content1);
        DstorePfreeExt(content2);
        return DSTORE_FAIL;
    }

    /* Step 2. Choose which file to read or write. */
    bool isChosenFile1 = false;
    bool isBothFilesValid = false;
    if (isFile1MetaPageValid && !isFile2MetaPageValid) {
        /* File1 is valid, but file2 is invalid. */
        isChosenFile1 = true;
    } else if (isFile1MetaPageValid && isFile2MetaPageValid) {
        /* Both files of meta page are valid, we choose the last-write one. */
        if (file1MetaPageTerm >= file2MetaPageTerm) {
            isChosenFile1 = true;
            isBothFilesValid = (file1MetaPageTerm == file2MetaPageTerm);
        }
    }

    /* Step 3. handle the pageHandle */
    if (isChosenFile1) {
        pageHandle->file = m_file1;
        pageHandle->checkResult = isBothFilesValid ? BOTH_META_PAGES_ARE_VALID : FIRST_META_PAGE_IS_VALID;
        pageHandle->metaPageTerm = file1MetaPageTerm;
    } else {
        pageHandle->file = m_file2;
        pageHandle->metaPageTerm = file2MetaPageTerm;
        pageHandle->checkResult = SECOND_META_PAGE_IS_VALID;
    }

    if (unlikely(GetPage(metaBlockNum, pageHandle->file) == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Get group meta page failed when get valid meta page."));
        return DSTORE_FAIL;
    }

    return DSTORE_SUCC;
}

RetStatus ControlFileMgr::MarkPageDirty(PageHandle *pageHandle, BlockNumber blockNumber)
{
    if (unlikely(blockNumber >= m_pageCount)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Wrong block number(%u) when mark dirty.", blockNumber));
        return DSTORE_FAIL;
    }
    if (IsPageStateDirty(blockNumber)) {
        return DSTORE_SUCC;
    }
    /* Invalid pageHandle only occuers when we create control file that must already mark dirty. */
    if (pageHandle == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("No page handle when mark page dirty."));
        return DSTORE_FAIL;
    }
    if (pageHandle->numDirtyBlocks == pageHandle->maxSize) {
        BlockNumber *blocks = static_cast<BlockNumber *>(DstoreMemoryContextAllocZero(
            m_memoryContext, (pageHandle->maxSize + CONTROL_PAGE_HANDLE_BASE_SIZE) * sizeof(BlockNumber)));
        if (unlikely(blocks == nullptr)) {
            ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Realloc pagehandle blocks failed."));
            return DSTORE_FAIL;
        }
        if (pageHandle->dirtyBlocks != nullptr) {
            errno_t rc = memcpy_s(blocks, pageHandle->maxSize * sizeof(BlockNumber), pageHandle->dirtyBlocks,
                                  pageHandle->maxSize * sizeof(BlockNumber));
            storage_securec_check(rc, "\0", "\0");
            DstorePfreeExt(pageHandle->dirtyBlocks);
        }

        pageHandle->dirtyBlocks = blocks;
        pageHandle->maxSize += CONTROL_PAGE_HANDLE_BASE_SIZE;
    }
    uint16 index = pageHandle->numDirtyBlocks;
    pageHandle->dirtyBlocks[index] = blockNumber;
    pageHandle->numDirtyBlocks++;
    MarkPageStateDirty(blockNumber);
    return DSTORE_SUCC;
}

char *ControlFileMgr::ReadOnePage(PageHandle *pageHandle, BlockNumber blockNumber)
{
    if (pageHandle == nullptr) {
        /* Only occurs when create control file. */
        return GetPage(blockNumber, nullptr);
    }
    if (pageHandle->checkResult == NO_VALID_META_PAGE) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("No valid page when read blockNumber(%u).", blockNumber));
        return nullptr;
    }
    char *page = GetPage(blockNumber, pageHandle->file);
    if (unlikely(page == nullptr && pageHandle->checkResult == BOTH_META_PAGES_ARE_VALID)) {
        ControlDiskFile *anotherValidFile = pageHandle->file == m_file1 ? m_file2 : m_file1;
        page = GetPage(blockNumber, anotherValidFile);
        if (unlikely(page == nullptr)) {
            pageHandle->checkResult = NO_VALID_META_PAGE;
            ErrLog(DSTORE_PANIC, MODULE_CONTROL, ErrMsg("No valid page when read blockNumber(%u).", blockNumber));
        } else {
            pageHandle->checkResult =
                pageHandle->file == m_file1 ? SECOND_META_PAGE_IS_VALID : FIRST_META_PAGE_IS_VALID;
            pageHandle->file = anotherValidFile;
        }
    }
    return page;
}

RetStatus ControlFileMgr::WriteAndSyncFiles()
{
    if (m_file1 == nullptr || m_file2 == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("No valid disk file when write and sync control file."));
        return DSTORE_FAIL;
    }
    AutoMemCxtSwitch memCxtSwitch(m_memoryContext);
    int64 bufSize = CONTROL_WRITE_ONCE_BLOCK_COUNT * BLCKSZ;
    char *buffer = static_cast<char *>(DstorePallocAligned(static_cast<Size>(bufSize), BLCKSZ));
    if (STORAGE_VAR_NULL(buffer)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Alloc memory failed."));
        return DSTORE_FAIL;
    }
    uint32_t bufCount = 0;
    int64 offset = 0;
    RetStatus rc = DSTORE_SUCC;
    for (BlockNumber i = CONTROLFILE_PAGEMAP_FILEMETA; i < CONTROLFILE_PAGEMAP_MAX; i++) {
        ControlBasePage *ctrlPage = reinterpret_cast<ControlBasePage *>(m_pageBuffer[i]);
        UpdateControlPageCrc(&ctrlPage->m_pageHeader.m_checksum, reinterpret_cast<const uint8 *>(m_pageBuffer[i]));
        errno_t err = memcpy_s(buffer + (bufCount * BLCKSZ), BLCKSZ, m_pageBuffer[i], BLCKSZ);
        storage_securec_check(err, "\0", "\0");
        bufCount++;
        if (bufCount < CONTROL_WRITE_ONCE_BLOCK_COUNT) {
            continue;
        }
        rc = m_file1->PwriteSync(buffer, bufSize, offset);
        if (STORAGE_FUNC_FAIL(rc)) {
            DstorePfreeAligned(buffer);
            ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Write controlFile1 all pages failed."));
            return DSTORE_FAIL;
        }
        rc = m_file2->PwriteSync(buffer, bufSize, offset);
        if (STORAGE_FUNC_FAIL(rc)) {
            DstorePfreeAligned(buffer);
            ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Write controlFile2 all pages failed."));
            return DSTORE_FAIL;
        }
        offset += static_cast<int64>(bufCount * BLCKSZ);
        bufCount = 0;
    }
    if (bufCount > 0) {
        rc = m_file1->PwriteSync(buffer, bufCount * BLCKSZ, offset);
        if (STORAGE_FUNC_FAIL(rc)) {
            DstorePfreeAligned(buffer);
            ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Write controlFile1 all pages failed."));
            return DSTORE_FAIL;
        }
        rc = m_file2->PwriteSync(buffer, bufCount * BLCKSZ, offset);
        if (STORAGE_FUNC_FAIL(rc)) {
            DstorePfreeAligned(buffer);
            ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Write controlFile2 all pages failed."));
            return DSTORE_FAIL;
        }
    }
    DstorePfreeAligned(buffer);
    for (BlockNumber i = CONTROLFILE_PAGEMAP_FILEMETA; i < CONTROLFILE_PAGEMAP_MAX; i++) {
        CleanPageStateDirty(i);
    }
    if (STORAGE_FUNC_FAIL(m_file1->Fsync())) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Fsync file1 pages failed."));
        return DSTORE_FAIL;
    }
    if (STORAGE_FUNC_FAIL(m_file2->Fsync())) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Fsync file2 pages failed."));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus ControlFileMgr::WritePage(ControlDiskFile *file, BlockNumber blockNumber)
{
    if (unlikely(blockNumber >= m_pageCount || file == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL,
               ErrMsg("Write page failed, blockNumber(%u), m_pageCount(%u).", blockNumber, m_pageCount));
        return DSTORE_FAIL;
    }
    if (unlikely(!IsPageStateDirty(blockNumber))) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Write page failed because page is not set dirty before."));
        return DSTORE_FAIL;
    }
    char *buff = m_pageBuffer[blockNumber];
    if (unlikely(buff == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL,
               ErrMsg("Read control page failed for blockNumber when write page, blockNumber(%u).", blockNumber));
        return DSTORE_FAIL;
    }
    ControlBasePage *ctrlPage = reinterpret_cast<ControlBasePage *>(buff);
    UpdateControlPageCrc(&ctrlPage->m_pageHeader.m_checksum, reinterpret_cast<const uint8 *>(buff));
    RetStatus rc = file->WritePage(blockNumber, buff);
    if (STORAGE_FUNC_FAIL(rc)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Write control file failed."));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus ControlFileMgr::WritePage(ControlDiskFile *file, BlockNumber blockNumber, char* inbuffer)
{
    if (unlikely(file == nullptr || inbuffer == nullptr)) {
        return DSTORE_FAIL;
    }
    ControlBasePage *ctrlPage = reinterpret_cast<ControlBasePage *>(inbuffer);
    UpdateControlPageCrc(&ctrlPage->m_pageHeader.m_checksum, reinterpret_cast<const uint8 *>(inbuffer));
    RetStatus rc = file->WritePage(blockNumber, inbuffer);
    if (STORAGE_FUNC_FAIL(rc)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Write controlFile failed."));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus ControlFileMgr::WriteAllDirtyPage(PageHandle *pageHandle, ControlDiskFile *file)
{
    for (BlockNumber i = 0; i < pageHandle->numDirtyBlocks; i++) {
        if (IsPageStateDirty(pageHandle->dirtyBlocks[i]) &&
            STORAGE_FUNC_FAIL(WritePage(file, pageHandle->dirtyBlocks[i]))) {
            return DSTORE_FAIL;
        }
    }
    return DSTORE_SUCC;
}

void ControlFileMgr::CleanAllPageStateDirty(PageHandle *pageHandle)
{
    for (BlockNumber i = 0; i < pageHandle->numDirtyBlocks; i++) {
        if (IsPageStateDirty(pageHandle->dirtyBlocks[i])) {
            CleanPageStateDirty(pageHandle->dirtyBlocks[i]);
        }
    }
    pageHandle->numDirtyBlocks = 0;
}

RetStatus ControlFileMgr::FlushPageBuffer(ControlDiskFile *file)
{
    if (unlikely(file == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Fsync controlFile failed because file is nullptr."));
        return DSTORE_FAIL;
    }
    RetStatus rc = file->Fsync();
    if (STORAGE_FUNC_FAIL(rc)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Fsync controlFile failed."));
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

RetStatus ControlFileMgr::PostPageHandle(PageHandle *pageHandle, BlockNumber metaPageBlockNum)
{
    /* Step 1. Check param. */
    if (unlikely(pageHandle == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Post page handle failed because pageHandle is nullptr."));
        return DSTORE_FAIL;
    }
    if (unlikely(metaPageBlockNum >= m_pageCount)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Post page handle failed because meta block is wrong."));
        return DSTORE_FAIL;
    }
    if (unlikely(pageHandle->checkResult == NO_VALID_META_PAGE)) {
        ErrLog(DSTORE_PANIC, MODULE_CONTROL, ErrMsg("No valid file when post page handle."));
        return DSTORE_FAIL;
    }

    /* Step 2. Recover another file. */
    ControlDiskFile *anotherFile = nullptr;
    if (pageHandle->file == m_file1) {
        anotherFile = m_file2;
    } else {
        anotherFile = m_file1;
    }
    if (pageHandle->checkResult != BOTH_META_PAGES_ARE_VALID) {
        RetStatus ret = CheckCrcAndRecoveryForGroup(metaPageBlockNum, pageHandle->groupType, pageHandle);
        if (STORAGE_FUNC_FAIL(ret)) {
            CleanCache(pageHandle, metaPageBlockNum);
            ErrLog(DSTORE_ERROR, MODULE_CONTROL,
                   ErrMsg("Recovery file(%s) failed when post page handle.", anotherFile->GetFileName()));
            return DSTORE_FAIL;
        }
        ErrLog(DSTORE_LOG, MODULE_CONTROL,
               ErrMsg("Recovery file(%s) success when post page handle.", anotherFile->GetFileName()));
    }
    pageHandle->file = m_file1;
    anotherFile = m_file2;
    pageHandle->checkResult = BOTH_META_PAGES_ARE_VALID;
    /* Step 3. Check and invalid buffer. */
    if (pageHandle->numDirtyBlocks == 0) {
        ErrLog(DSTORE_WARNING, MODULE_CONTROL, ErrMsg("Dirty block number is 0 when post page handle."));
        return DSTORE_SUCC;
    }
    /* Step 3.1 Mark meta page writing. */
    ControlMetaPage *page = reinterpret_cast<ControlMetaPage *>(GetPage(metaPageBlockNum, pageHandle->file));
    if (unlikely(page == nullptr)) {
        CleanCache(pageHandle, metaPageBlockNum);
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Get meta page failed."));
        return DSTORE_FAIL;
    }
    uint64 term = page->GetTerm();
    ErrLog(DSTORE_LOG, MODULE_CONTROL,
           ErrMsg("Begin to PostPageHandle for group type(%d) and term(%lu)",
                  static_cast<int32>(pageHandle->groupType), term));
    
    if (page->CheckIfWriting()) {
        ErrLog(DSTORE_PANIC, MODULE_CONTROL,
               ErrMsg("The meta page(%u) must be not writing before mark writing.", metaPageBlockNum));
    }
    page->MarkWriting();
    MarkPageDirty(pageHandle, metaPageBlockNum);
    if (STORAGE_FUNC_FAIL(WritePage(pageHandle->file, metaPageBlockNum)) ||
        STORAGE_FUNC_FAIL(FlushPageBuffer(pageHandle->file))) {
        CleanCache(pageHandle, metaPageBlockNum);
        return DSTORE_FAIL;
    }
    CleanPageStateDirty(metaPageBlockNum);
    /* Step 3.2 Write data page. */
    if (STORAGE_FUNC_FAIL(WriteAllDirtyPage(pageHandle, pageHandle->file)) ||
        STORAGE_FUNC_FAIL(FlushPageBuffer(pageHandle->file))) {
        CleanCache(pageHandle, metaPageBlockNum);
        return DSTORE_FAIL;
    }
    /* Step 3.3 Mark meta page write finished. */
    page->MarkWriteFinished();
    page->SetTerm(term + 1);
    MarkPageDirty(pageHandle, metaPageBlockNum);
    if (STORAGE_FUNC_FAIL(WritePage(pageHandle->file, metaPageBlockNum)) ||
        STORAGE_FUNC_FAIL(FlushPageBuffer(pageHandle->file))) {
        CleanCache(pageHandle, metaPageBlockNum);
        return DSTORE_FAIL;
    }

    /* Step 4. Write anoth file. */
    page->MarkWriting();
    page->SetTerm(term);
    MarkPageDirty(pageHandle, metaPageBlockNum);
    if (STORAGE_FUNC_FAIL(WritePage(anotherFile, metaPageBlockNum)) ||
        STORAGE_FUNC_FAIL(FlushPageBuffer(anotherFile))) {
        CleanCache(pageHandle, metaPageBlockNum);
        return DSTORE_FAIL;
    }
    CleanPageStateDirty(metaPageBlockNum);
    if (STORAGE_FUNC_FAIL(WriteAllDirtyPage(pageHandle, anotherFile)) ||
        STORAGE_FUNC_FAIL(FlushPageBuffer(anotherFile))) {
        CleanCache(pageHandle, metaPageBlockNum);
        return DSTORE_FAIL;
    }
    page->MarkWriteFinished();
    page->SetTerm(term + 1);
    MarkPageDirty(pageHandle, metaPageBlockNum);
    if (STORAGE_FUNC_FAIL(WritePage(anotherFile, metaPageBlockNum)) ||
        STORAGE_FUNC_FAIL(FlushPageBuffer(anotherFile))) {
        CleanCache(pageHandle, metaPageBlockNum);
        return DSTORE_FAIL;
    }
    CleanAllPageStateDirty(pageHandle);
    ErrLog(DSTORE_LOG, MODULE_CONTROL,
           ErrMsg("Success to PostPageHandle for group type(%d)", static_cast<int>(pageHandle->groupType)));
    return DSTORE_SUCC;
}

void ControlFileMgr::CleanCache(PageHandle *pageHandle, BlockNumber metaPageBlockNum)
{
    CleanPageStateDirty(metaPageBlockNum);
    CleanPageStateValid(metaPageBlockNum);
    for (BlockNumber i = 0; i < pageHandle->numDirtyBlocks; i++) {
        CleanPageStateDirty(pageHandle->dirtyBlocks[i]);
        CleanPageStateValid(pageHandle->dirtyBlocks[i]);
    }
}

RetStatus ControlFileMgr::CheckPageCrcMatch(ControlDiskFile *file, BlockNumber block, bool &isMatch)
{
    char *pageBuffer = static_cast<char *>(DstoreMemoryContextAllocZero(m_memoryContext, BLCKSZ));
    if (unlikely(pageBuffer == nullptr)) {
        storage_set_error(CONTROL_ERROR_MEMORY_NOT_ENOUGH);
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Fail to palloc to read block(%u) for check page crc.", block));
        isMatch = false;
        return DSTORE_FAIL;
    }

    RetStatus rc = file->ReadPage(block, pageBuffer);
    if (STORAGE_FUNC_FAIL(rc)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Fail to read block(%u) for check page crc.", block));
        DstorePfreeExt(pageBuffer);
        isMatch = false;
        return DSTORE_FAIL;
    }

    /* The checksum must be the first member of the page. */
    uint32 *checksum = STATIC_CAST_PTR_TYPE(pageBuffer, uint32 *);
    isMatch = CheckPageCrc(checksum, STATIC_CAST_PTR_TYPE(pageBuffer, uint8 *));
    DstorePfreeExt(pageBuffer);
    return DSTORE_SUCC;
}

bool CheckPageCrc(uint32 *checksum, const uint8 *page)
{
    uint32 curChecksum = *checksum;
    *checksum = 0;
    uint32 newChecksum = CompChecksum(page, BLCKSZ, CHECKSUM_CRC);
    *checksum = curChecksum;
    return curChecksum == newChecksum;
}

bool ControlFileMgr::CheckPageConsistency(BlockNumber block)
{
    char *pageBuffer1 = static_cast<char *>(DstoreMemoryContextAllocZero(m_memoryContext, BLCKSZ));
    char *pageBuffer2 = static_cast<char *>(DstoreMemoryContextAllocZero(m_memoryContext, BLCKSZ));
    if (unlikely(pageBuffer1 == nullptr || pageBuffer2 == nullptr)) {
        storage_set_error(CONTROL_ERROR_MEMORY_NOT_ENOUGH);
        DstorePfreeExt(pageBuffer1);
        DstorePfreeExt(pageBuffer2);
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Fail to palloc to read block(%u).", block));
        return false;
    }

    /* Read page from disk file, not from m_pageBuffer. */
    RetStatus rc1 = m_file1->ReadPage(block, pageBuffer1);
    RetStatus rc2 = m_file2->ReadPage(block, pageBuffer2);
    if (STORAGE_FUNC_FAIL(rc1) || STORAGE_FUNC_FAIL(rc2)) {
        DstorePfreeExt(pageBuffer1);
        DstorePfreeExt(pageBuffer2);
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Fail to read block(%u).", block));
        return false;
    }

    ControlBasePage *page1 = STATIC_CAST_PTR_TYPE(pageBuffer1, ControlBasePage *);
    ControlBasePage *page2 = STATIC_CAST_PTR_TYPE(pageBuffer2, ControlBasePage *);
    if (unlikely(page1 == nullptr || page2 == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Fail to get page for block(%u).", block));
        DstorePfreeExt(pageBuffer1);
        DstorePfreeExt(pageBuffer2);
        return false;
    }

    if (page1->GetPageType() == CONTROL_PAGE_TYPE_INVALID && page2->GetPageType() == CONTROL_PAGE_TYPE_INVALID) {
        /* The page hasn't been used, no need to check the consistency.  */
        DstorePfreeExt(pageBuffer1);
        DstorePfreeExt(pageBuffer2);
        return true;
    }

    uint32 checkSum1 = page1->GetCheckSum();
    uint32 checkSum2 = page2->GetCheckSum();
    DstorePfreeExt(pageBuffer1);
    DstorePfreeExt(pageBuffer2);
    if (checkSum1 == 0 || checkSum2 == 0) {
        return false;
    }
    return checkSum1 == checkSum2;
}

RetStatus ControlFileMgr::SinglePageRecovery(ControlDiskFile *targetFile, char *sourceBuffer, BlockNumber block)
{
    if (unlikely(targetFile == nullptr || sourceBuffer == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Invalid paras when single page recovery."));
        return DSTORE_FAIL;
    }

    /* The checksum must be the first member of the page. */
    uint32 *checksum = STATIC_CAST_PTR_TYPE(sourceBuffer, uint32 *);
    if (!CheckPageCrc(checksum, STATIC_CAST_PTR_TYPE(sourceBuffer, uint8 *))) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("The checksum(%u) of source buffer is not match.", *checksum));
        return DSTORE_FAIL;
    }

    char *errPageBuffer = static_cast<char *>(DstoreMemoryContextAllocZero(m_memoryContext, BLCKSZ));
    char *validPageBuffer = static_cast<char *>(DstoreMemoryContextAllocZero(m_memoryContext, BLCKSZ));
    if (unlikely(errPageBuffer == nullptr) || unlikely(validPageBuffer == nullptr)) {
        DstorePfreeExt(errPageBuffer);
        DstorePfreeExt(validPageBuffer);
        ErrLog(DSTORE_ERROR, MODULE_CONTROL,
               ErrMsg("Fail to palloc to recovery page of block(%u).", block));
        return DSTORE_FAIL;
    }

    /* Read the target page before recovery. */
    RetStatus ret = targetFile->ReadPage(block, errPageBuffer);
    if (STORAGE_FUNC_FAIL(ret)) {
        DstorePfreeExt(errPageBuffer);
        DstorePfreeExt(validPageBuffer);
        ErrLog(DSTORE_ERROR, MODULE_CONTROL,
               ErrMsg("Fail to read the error page of block(%u) to be recovered.", block));
        return DSTORE_FAIL;
    }

    /* Recover the page */
    ret = targetFile->WritePage(block, sourceBuffer);

    /* Read the target page after recovery. */
    ControlBasePage *errPage = STATIC_CAST_PTR_TYPE(errPageBuffer, ControlBasePage *);
    char *errContent = errPage->Dump();
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL,
               ErrMsg("Failed to recover page(%u) with incorrect page data is:\n%s", block, errContent));
        DstorePfreeExt(errPageBuffer);
        DstorePfreeExt(validPageBuffer);
        DstorePfreeExt(errContent);
        return DSTORE_FAIL;
    }

    ControlBasePage *validPage = nullptr;
    ret = targetFile->ReadPage(block, validPageBuffer);
    if (STORAGE_FUNC_FAIL(ret)) {
        validPage = STATIC_CAST_PTR_TYPE(sourceBuffer, ControlBasePage *);
    } else {
        validPage = STATIC_CAST_PTR_TYPE(validPageBuffer, ControlBasePage *);
    }
    char *validContent = validPage->Dump();
    ErrLog(ret == DSTORE_SUCC ? DSTORE_LOG : DSTORE_ERROR, MODULE_CONTROL,
        ErrMsg("%s to recover page(%u) with page data before recovery is:\n%s"
               "\nAnd page data after recovery is:\n%s",
               ret == DSTORE_SUCC ? "Succeed" : "Failed", block, errContent, validContent));

    DstorePfreeExt(errPageBuffer);
    DstorePfreeExt(validPageBuffer);
    DstorePfreeExt(errContent);
    DstorePfreeExt(validContent);
    return ret;
}

RetStatus ControlFileMgr::GroupPagesRecovery(ControlDiskFile *targetFile, ControlDiskFile *sourceFile,
                                             char *sourceMetaBuffer, BlockNumber metaBlock)
{
    /* Recovery group data pages. Here must read page from disk. */
    ControlMetaPage *metaPage = STATIC_CAST_PTR_TYPE(sourceMetaBuffer, ControlMetaPage *);
    if (unlikely(metaPage == nullptr)) {
        storage_set_error(CONTROL_ERROR_INVALID_PARAMETER);
        return DSTORE_FAIL;
    }
    uint16 type = metaPage->GetPageType();
    if (type != CONTROL_TBS_METAPAGE_TYPE && type != CONTROL_WAL_STREAM_METAPAGE_TYPE &&
        type != CONTROL_CSN_METAPAGE_TYPE && type != CONTROL_RELMAP_METAPAGE_TYPE &&
        type != CONTROL_PDBINFO_METAPAGE_TYPE && type != CONTROL_LOGICALREP_METAPAGE_TYPE) {
        /* Not meta page */
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("The block(%u) must be a meta block.", metaBlock));
        return DSTORE_FAIL;
    }

    ControlMetaHeader *metaHeader = metaPage->GetMetaHeader();
    if (unlikely(metaHeader == nullptr)) {
        storage_set_error(CONTROL_ERROR_INVALID_PARAMETER);
        return DSTORE_FAIL;
    }

    char *srcDataPageBuffer = static_cast<char *>(DstoreMemoryContextAllocZero(m_memoryContext, BLCKSZ));
    char *errDataPageBuffer = static_cast<char *>(DstoreMemoryContextAllocZero(m_memoryContext, BLCKSZ));
    if (unlikely(srcDataPageBuffer == nullptr) || unlikely(errDataPageBuffer == nullptr)) {
        DstorePfreeExt(srcDataPageBuffer);
        DstorePfreeExt(errDataPageBuffer);
        ErrLog(DSTORE_ERROR, MODULE_CONTROL,
               ErrMsg("Fail to palloc to recovery group with meta block(%u).", metaBlock));
        return DSTORE_FAIL;
    }

    RetStatus ret1 = DSTORE_FAIL;
    RetStatus ret2 = DSTORE_FAIL;
    for (uint32 i = 0; i < CONTROL_MAX_PAGERANGE_NUM; i++) {
        ControlPageRange range = metaHeader->m_pageRange[i];
        if (range.m_start == DSTORE_INVALID_BLOCK_NUMBER || range.m_end == DSTORE_INVALID_BLOCK_NUMBER) {
            /* This range is not used, so we skip it. */
            continue;
        }

        for (BlockNumber curBlock = range.m_start; curBlock <= range.m_end; curBlock++) {
            ret1 = sourceFile->ReadPage(curBlock, srcDataPageBuffer);
            ret2 = targetFile->ReadPage(curBlock, errDataPageBuffer);
            if (STORAGE_FUNC_FAIL(ret1) || STORAGE_FUNC_FAIL(ret2)) {
                DstorePfreeExt(srcDataPageBuffer);
                DstorePfreeExt(errDataPageBuffer);
                ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Fail to read block(%u) to recovery.", curBlock));
                return DSTORE_FAIL;
            }

            ControlBasePage *srcDataPage = STATIC_CAST_PTR_TYPE(srcDataPageBuffer, ControlBasePage *);
            ControlBasePage *errDataPage = STATIC_CAST_PTR_TYPE(errDataPageBuffer, ControlBasePage *);
            if (unlikely(srcDataPage == nullptr) || unlikely(errDataPage == nullptr)) {
                DstorePfreeExt(srcDataPageBuffer);
                DstorePfreeExt(errDataPageBuffer);
                ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Fail to get page for block(%u) to recovery.", curBlock));
                return DSTORE_FAIL;
            }

            uint32 *checkSum = STATIC_CAST_PTR_TYPE(srcDataPageBuffer, uint32 *);
            if (!CheckPageCrc(checkSum, STATIC_CAST_PTR_TYPE(srcDataPageBuffer, uint8 *))) {
                ErrLog(DSTORE_ERROR, MODULE_CONTROL,
                       ErrMsg("The checksum(%u) of source file page(%u) is not match.",
                              srcDataPage->GetCheckSum(), curBlock));
                DstorePfreeExt(srcDataPageBuffer);
                DstorePfreeExt(errDataPageBuffer);
                return DSTORE_FAIL;
            }
            if (IsSameDataPages(errDataPage, srcDataPage)) {
                /* There is no difference between the two data pages, so we skip it. */
                continue;
            }

            if (STORAGE_FUNC_FAIL(SinglePageRecovery(targetFile, srcDataPageBuffer, curBlock))) {
                DstorePfreeExt(srcDataPageBuffer);
                DstorePfreeExt(errDataPageBuffer);
                ErrLog(DSTORE_ERROR, MODULE_CONTROL,
                       ErrMsg("Fail to recovery data page(%u) with meta block(%hhu).", curBlock, metaBlock));
                return DSTORE_FAIL;
            }
        }
    }

    /* Recovery group meta page */
    RetStatus ret = SinglePageRecovery(targetFile, sourceMetaBuffer, metaBlock);
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Fail to recovery meta page(%u).", metaBlock));
    }
    DstorePfreeExt(srcDataPageBuffer);
    DstorePfreeExt(errDataPageBuffer);
    return ret;
}

bool ControlFileMgr::IsSameDataPages(ControlBasePage *page1, ControlBasePage *page2)
{
    return page1->GetCheckSum() == page2->GetCheckSum() &&
           page1->m_pageHeader.m_magic == page2->m_pageHeader.m_magic &&
           page1->m_pageHeader.m_magic == CONTROL_DATA_MAGIC_NUMBER;
}

RetStatus ControlFileMgr::CheckCrcAndRecoveryForGroup(BlockNumber metaBlock, ControlGroupType groupType,
    PageHandle *pageHandle)
{
    RetStatus ret = CheckCrcAndRecoveryWithMetaPage(metaBlock, pageHandle);
    if (unlikely(ret == DSTORE_FAIL)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL,
               ErrMsg("Fail to recovery group(%hhu) with metaBlock(%u).", groupType, metaBlock));
    }
    return ret;
}

RetStatus ControlFileMgr::CheckCrcAndRecoveryWithMetaPage(BlockNumber metaBlock, PageHandle *pageHandle)
{
    if (unlikely(m_file1 == nullptr || m_file2 == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL,
               ErrMsg("Control files have not been inited, can not do recovery of meta block(%u).", metaBlock));
        return DSTORE_FAIL;
    }

    char *metaPageBuffer1 = static_cast<char *>(DstoreMemoryContextAllocZero(m_memoryContext, BLCKSZ));
    char *metaPageBuffer2 = static_cast<char *>(DstoreMemoryContextAllocZero(m_memoryContext, BLCKSZ));
    if (unlikely(metaPageBuffer1 == nullptr || metaPageBuffer2 == nullptr)) {
        DstorePfreeExt(metaPageBuffer1);
        DstorePfreeExt(metaPageBuffer2);
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Fail to palloc to check pages for meta block(%u).", metaBlock));
        return DSTORE_FAIL;
    }

    RetStatus ret1 = m_file1->ReadPage(metaBlock, metaPageBuffer1);
    RetStatus ret2 = m_file2->ReadPage(metaBlock, metaPageBuffer2);
    if (STORAGE_FUNC_FAIL(ret1) || STORAGE_FUNC_FAIL(ret2)) {
        DstorePfreeExt(metaPageBuffer1);
        DstorePfreeExt(metaPageBuffer2);
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Fail to read file mate page for check pages."));
        return DSTORE_FAIL;
    }

    RetStatus ret = DSTORE_FAIL;
    MetaPageCheckResult checkResult;
    if (pageHandle && pageHandle->checkResult != NO_VALID_META_PAGE) {
        /* Recovery for the scenerio that the meta page is valid but some data pages are not.
         * The checkResult will be reset if a invalid data page is read. */
        checkResult = pageHandle->checkResult;
    } else {
        /* Recovery for the stage of initializing the control file when pageHandle hasn't inited
         * or it hasn't read any page. */
        checkResult = DSTORE::CheckMetaPage(metaPageBuffer1, metaPageBuffer2);
    }
    switch (checkResult) {
        case BOTH_META_PAGES_ARE_VALID:
            /* Both file are same, and not need to recovery. */
            ret = DSTORE_SUCC;
            break;
        case FIRST_META_PAGE_IS_VALID:
            /* File1 is correct, or file1 is newer than file2. */
            ret = RecoveryWithMetaPage(m_file2, m_file1, metaPageBuffer1, metaBlock);
            ErrLog(DSTORE_LOG, MODULE_CONTROL,
                   ErrMsg("Recovery file2 with file1 %s, meta block(%u).",
                          ret == DSTORE_SUCC ? "success" : "fail", metaBlock));
            break;
        case SECOND_META_PAGE_IS_VALID:
            /* File2 is correct, or file2 is newer than file1. */
            ret = RecoveryWithMetaPage(m_file1, m_file2, metaPageBuffer2, metaBlock);
            ErrLog(DSTORE_LOG, MODULE_CONTROL,
                   ErrMsg("Recovery file1 with file2 %s, meta block(%u).",
                          ret == DSTORE_SUCC ? "success" : "fail", metaBlock));
            break;
        case NO_VALID_META_PAGE:
        default:
            /* Both files are incorrect */
            ErrLog(DSTORE_PANIC, MODULE_CONTROL, ErrMsg("Both files are incorrect for metaBlock(%u).", metaBlock));
            ret = DSTORE_FAIL;
    }

    DstorePfreeExt(metaPageBuffer1);
    DstorePfreeExt(metaPageBuffer2);
    return ret;
}

RetStatus ControlFileMgr::CheckCrcAndRecoveryForFileMeta()
{
    RetStatus ret = CheckCrcAndRecoveryWithMetaPage(CONTROLFILE_PAGEMAP_FILEMETA);
    if (unlikely(ret == DSTORE_FAIL)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Fail to recovery file meta page."));
    }
    return ret;
}

RetStatus ControlFileMgr::RecoveryWithMetaPage(ControlDiskFile *targetFile, ControlDiskFile *sourceFile,
                                               char *sourceMetaBuffer, BlockNumber metaBlock)
{
    if (metaBlock == CONTROLFILE_PAGEMAP_FILEMETA) {
        /* File mate page recovery. */
        return SinglePageRecovery(targetFile, sourceMetaBuffer, metaBlock);
    }

    return GroupPagesRecovery(targetFile, sourceFile, sourceMetaBuffer, metaBlock);
}

void ControlFileMgr::CleanPageValid(uint32 blockNumber)
{
    m_state[blockNumber] &= (~CONTROL_PAGE_BUF_VALID);
}

#ifdef UT
RetStatus ControlFileMgr::UTWriteFileForFaultInjection(BlockNumber block, bool isFile1Fault, bool isUpdateCheckSum)
{
    ControlDiskFile *faultFile = isFile1Fault ? m_file1 : m_file2;
    if (unlikely(faultFile == nullptr)) {
        return DSTORE_FAIL;
    }

    char *pageBuffer = static_cast<char *>(DstoreMemoryContextAllocZero(m_memoryContext, BLCKSZ));
    if (unlikely(pageBuffer == nullptr)) {
        return DSTORE_FAIL;
    }
    RetStatus ret = faultFile->ReadPage(block, pageBuffer);
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Read fault control file failed, block(%u).", block));
        DstorePfreeExt(pageBuffer);
        return DSTORE_FAIL;
    }

    ControlBasePage *page = STATIC_CAST_PTR_TYPE(pageBuffer, ControlBasePage *);
    page->SetCheckSum(0); /* set wrong checksum */
    if (isUpdateCheckSum) {
        page->SetNextPage(page->GetNextPage() - 1); /* set wrong next page for diffrent crc value after update crc */
        UpdateControlPageCrc(&page->m_pageHeader.m_checksum, reinterpret_cast<const uint8 *>(pageBuffer));
    }

    ret = faultFile->WritePage(block, pageBuffer);
    if (STORAGE_FUNC_FAIL(ret)) {
        ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Write fault control file failed, block(%u).", block));
        DstorePfreeExt(pageBuffer);
        return DSTORE_FAIL;
    }

    if (isUpdateCheckSum) {
        ControlDiskFile *correctFile = isFile1Fault ? m_file2 : m_file1;
        if (unlikely(correctFile == nullptr)) {
            DstorePfreeExt(pageBuffer);
            return DSTORE_FAIL;
        }

        ret = correctFile->ReadPage(block, pageBuffer);
        if (STORAGE_FUNC_FAIL(ret)) {
            ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Read correct control file failed, block(%u).", block));
            DstorePfreeExt(pageBuffer);
            return DSTORE_FAIL;
        }

        ControlMetaPage *page = STATIC_CAST_PTR_TYPE(pageBuffer, ControlMetaPage *);
        page->SetTerm(1000); /* set bigger term to make sure the file is the last-write one */
        UpdateControlPageCrc(&page->m_pageHeader.m_checksum, reinterpret_cast<const uint8 *>(pageBuffer));

        ret = correctFile->WritePage(block, pageBuffer);
        if (STORAGE_FUNC_FAIL(ret)) {
            ErrLog(DSTORE_ERROR, MODULE_CONTROL, ErrMsg("Write correct control file failed, block(%u).", block));
            DstorePfreeExt(pageBuffer);
            return DSTORE_FAIL;
        }
    }

    DstorePfreeExt(pageBuffer);
    return DSTORE_SUCC;
}


uint32 ControlFileMgr::UtGetPageCount()
{
    return m_pageCount;
}

ControlPage *ControlFileMgr::UtGetPage(BlockNumber blockNumber)
{
    return (ControlPage *)m_pageBuffer[blockNumber];
}

uint32 ControlFileMgr::UtGetPageState(uint32 blockNumber) const
{
    return m_state[blockNumber];
}

void ControlFileMgr::UtInvalidateAll()
{
    for (uint32 blockNumber = 0; blockNumber < m_pageCount; blockNumber++) {
        m_state[blockNumber] &= (~CONTROL_PAGE_BUF_VALID);
    }
}

#endif

MetaPageCheckResult CheckMetaPage(void *firstMetaPage, void *secondMetaPage)
{
    bool isFirstMetaValid = true;
    bool isSecondMetaValid = true;
    uint64 term1 = 0;
    uint64 term2 = 0;
    if (unlikely(firstMetaPage == nullptr)) {
        isFirstMetaValid = false;
    }
    if (unlikely(secondMetaPage == nullptr)) {
        isSecondMetaValid = false;
    }
    if (unlikely(!isFirstMetaValid && !isSecondMetaValid)) {
        return NO_VALID_META_PAGE;
    }
    if (isFirstMetaValid) {
        uint32 *checkSum = STATIC_CAST_PTR_TYPE(firstMetaPage, uint32 *);
        bool isMatch = CheckPageCrc(checkSum, STATIC_CAST_PTR_TYPE(firstMetaPage, uint8 *));
        ControlMetaPage *page = reinterpret_cast<ControlMetaPage *>(firstMetaPage);
        if (unlikely(!isMatch || page->CheckIfWriting())) {
            isFirstMetaValid = false;
        } else {
            term1 = page->GetTerm();
        }
    }
    if (isSecondMetaValid) {
        uint32 *checkSum = STATIC_CAST_PTR_TYPE(secondMetaPage, uint32 *);
        bool isMatch = CheckPageCrc(checkSum, STATIC_CAST_PTR_TYPE(secondMetaPage, uint8 *));
        ControlMetaPage *page = reinterpret_cast<ControlMetaPage *>(secondMetaPage);
        if (unlikely(!isMatch || page->CheckIfWriting())) {
            isSecondMetaValid = false;
        } else {
            term2 = page->GetTerm();
        }
    }
    if (isFirstMetaValid && isSecondMetaValid) {
        if (term1 == term2) {
            return BOTH_META_PAGES_ARE_VALID;
        }
        return term1 > term2 ? FIRST_META_PAGE_IS_VALID : SECOND_META_PAGE_IS_VALID;
    }
    if (unlikely(!isFirstMetaValid && !isSecondMetaValid)) {
        return NO_VALID_META_PAGE;
    }
    return isFirstMetaValid ? FIRST_META_PAGE_IS_VALID : SECOND_META_PAGE_IS_VALID;
}

}  // namespace DSTORE