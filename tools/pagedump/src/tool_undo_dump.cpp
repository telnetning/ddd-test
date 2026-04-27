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
 * tool_undo_dump.cpp
 *
 * IDENTIFICATION
 *        tools/pagedump/src/tool_undo_dump.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "tool_undo_dump.h"
#include "framework/dstore_instance_interface.h"
#include "page/dstore_page_struct.h"
#include "page/dstore_undo_segment_meta_page.h"
#include "page/dstore_undo_page.h"
#include "undo/dstore_undo_record.h"
#include "tool_page_dump_file_reader.h"
#include "diagnose/dstore_undo_mgr_diagnose.h"
#include "page/dstore_page_diagnose.h"
namespace DSTORE {
constexpr uint32 SEGMENT_ID_PER_PAGE = (BLCKSZ - sizeof(Page)) / sizeof(PageId);
constexpr uint32 SEGMENT_ID_PAGES = (UNDO_ZONE_COUNT + SEGMENT_ID_PER_PAGE - 1) / SEGMENT_ID_PER_PAGE;
#define tool_securec_check_ss(errno) \
    if (unlikely(((errno) == -1))) {     \
        (void)fprintf(PageDiagnose::DumpToolHelper::dumpPrint, "Fatal error\n");        \
        exit(1);                        \
    }

UndoDumpFileReader::UndoDumpFileReader(PageDumpConfig *config) : PageDumpFileReader(config)
{
    for (uint32 i = 0; i < UNDO_FILE_COUNT_MAX_VALUE; i++) {
        m_fileDescArray[i].fileId = 0;
        m_fileDescArray[i].fd = nullptr;
    }
    m_fdCount = 0;
}

UndoDumpFileReader::~UndoDumpFileReader()
{
    for (int i = 0; i < m_fdCount; i++) {
        if (m_fileDescArray[i].fd != nullptr) {
            m_fileReader.Close(m_fileDescArray[i].fd);
            m_fileDescArray[i].fd = nullptr;
        }
    }
    m_fdCount = 0;
}

FileDescriptor *UndoDumpFileReader::OpenUndoFile(FileId fileId)
{
    FileDescriptor *fileDesc = nullptr;
    DSTORE::RetStatus ret = DSTORE::DSTORE_SUCC;
    char fileName[MAXPGPATH];
    if (m_config->vfsType == VFS_TYPE_PAGE_STORE) {
        PageDiagnose::GetFileName(fileId, fileName, MAXPGPATH);
        ret = m_fileReader.Open(fileName, FILE_READ_ONLY_FLAG, &fileDesc);
        if (ret == RetStatus::DSTORE_FAIL) {
            (void)fprintf(PageDiagnose::DumpToolHelper::dumpPrint, "Open file failed. Invalid file name %s\n",
                          fileName);
            exit(-1);
        }
    } else if (m_config->vfsType == VFS_TYPE_LOCAL_FS) {
        GetUndoFileName(fileId, fileName, MAXPGPATH);
        char *realPath = realpath(fileName, nullptr);
        if (realPath == nullptr) {
            (void)fprintf(PageDiagnose::DumpToolHelper::dumpPrint, "Open file failed. Invalid file name %s\n",
                          fileName);
            exit(-1);
        }
        ret = m_fileReader.Open(realPath, FILE_READ_ONLY_FLAG, &fileDesc);
        free(realPath);
    }
    return fileDesc;
}

FileDescriptor *UndoDumpFileReader::GetFileDesc(FileId fileId)
{
    if (m_fdCount >= UNDO_FILE_COUNT_MAX_VALUE) {
        (void)fprintf(PageDiagnose::DumpToolHelper::dumpPrint, "file id %hu is invalid\n", fileId);
        return nullptr;
    }
    for (int i = 0; i < m_fdCount; i++) {
        if (m_fileDescArray[i].fd != nullptr && m_fileDescArray[i].fileId == fileId) {
            return m_fileDescArray[i].fd;
        }
    }
    int idx = m_fdCount;
    m_fileDescArray[idx].fd = OpenUndoFile(fileId);
    m_fileDescArray[idx].fileId = fileId;
    m_fdCount++;
    return m_fileDescArray[idx].fd;
}

void UndoDumpFileReader::LoadUndoPage(PageId &pageId, char *pageBuf, int pageSize)
{
    FileDescriptor *fileDesc = GetFileDesc(pageId.m_fileId);

    off_t tailOffset = m_fileReader.Size(fileDesc);
    assert(tailOffset % BLCKSZ == 0);
    uint32_t maxPageId = static_cast<uint32_t>(tailOffset / BLCKSZ);
    if (unlikely(pageId.m_blockId >= maxPageId)) {
        (void)fprintf(PageDiagnose::DumpToolHelper::dumpPrint,
                      "read failed: page id(%u) shoud less than max page id(%u)\n", pageId.m_blockId, maxPageId);
        exit(-1);
    }
    DSTORE::RetStatus ret = m_fileReader.ReadPage(fileDesc, pageId.m_blockId, pageBuf, pageSize);
    if (unlikely(ret == RetStatus::DSTORE_FAIL)) {
        (void)fprintf(PageDiagnose::DumpToolHelper::dumpPrint, "read failed: page id(%u)\n", pageId.m_blockId);
        exit(-1);
    }
    return;
}

PageId UndoDumpFileReader::GetUndoMapSegmentId(PageId &pageId)
{
    char pageBuf[BLCKSZ];
    LoadUndoPage(pageId, pageBuf, BLCKSZ);
    UndoSegmentMetaPage *undoMetaPage = STATIC_CAST_PTR_TYPE(pageBuf, UndoSegmentMetaPage*);
    return undoMetaPage->segmentHeader.extents.last;
}

void UndoDumpFileReader::LoadCsnMetaPage(char *pageBuf, const char *fileName)
{
    FileDescriptor *fileDesc = nullptr;
    DSTORE::RetStatus ret = RetStatus::DSTORE_FAIL;
    if (m_config->vfsType == VFS_TYPE_PAGE_STORE) {
        (void)printf("pagedump pagestore\n");
        ret = m_fileReader.Open(fileName, FILE_READ_ONLY_FLAG, &fileDesc);
    } else if (m_config->vfsType == VFS_TYPE_LOCAL_FS) {
        (void)printf("pagedump local\n");
        char *realPath = realpath(fileName, nullptr);
        if (realPath == nullptr) {
            (void)printf("Open file failed. Invalid file name %s\n", m_config->file);
            exit(-1);
        }
        ret = m_fileReader.Open(realPath, FILE_READ_ONLY_FLAG, &fileDesc);
        free(realPath);
    }
    if (ret == RetStatus::DSTORE_FAIL) {
        (void)fprintf(PageDiagnose::DumpToolHelper::dumpPrint, "Open file failed, file name %s\n, vfsType:%hhu\n",
            fileName, static_cast<uint8>(m_config->vfsType));
        exit(-1);
    }

    LoadPage(fileDesc, static_cast<uint32_t>(CONTROLFILE_PAGEMAP_CSN_META), pageBuf, BLCKSZ);
    m_fileReader.Close(fileDesc);
}

PageId UndoDumpFileReader::GetUndoMapMetaSegmentId()
{
    if (!m_isInitVfs) {
        if (unlikely(InitVfs() == RetStatus::DSTORE_FAIL)) {
            (void)printf("vfs file reader init failed.\n");
            return INVALID_PAGE_ID;
        }
    }

    char pageBuf1[BLCKSZ];
    char pageBuf2[BLCKSZ];
    LoadCsnMetaPage(pageBuf1, DATABASE_CONTROL_FILE_1_NAME);
    LoadCsnMetaPage(pageBuf2, DATABASE_CONTROL_FILE_2_NAME);
    MetaPageCheckResult result = DSTORE::CheckMetaPage(pageBuf1, pageBuf2);
    ControlMetaPage *csnMetaPage = nullptr;
    switch (result) {
        case MetaPageCheckResult::FIRST_META_PAGE_IS_VALID:
        case MetaPageCheckResult::BOTH_META_PAGES_ARE_VALID:
            csnMetaPage = (ControlMetaPage *)pageBuf1;
            break;
        case MetaPageCheckResult::SECOND_META_PAGE_IS_VALID:
            csnMetaPage = (ControlMetaPage *)pageBuf2;
            break;
        default:
            csnMetaPage = nullptr;
            (void)fprintf(PageDiagnose::DumpToolHelper::dumpPrint, "Both metapage is not valid\n");
            return INVALID_PAGE_ID;
    }

    return (static_cast<ControlCsnPageData *>(static_cast<void *>(csnMetaPage->GetMetaData())))->m_segmentId;
}

void UndoDumpFileReader::DumpUndoMap()
{
    char pageBuf[BLCKSZ];


    /* step1: Get undo map segment id */
    PageId undoMapSegmentId = GetUndoMapMetaSegmentId();
    if (unlikely(undoMapSegmentId == INVALID_PAGE_ID)) {
        (void)fprintf(PageDiagnose::DumpToolHelper::dumpPrint, "Get undo map meata segment id failed\n");
        exit(-1);
    }

    /* step2: Get undo map pageid */
    PageId undoMapPageId = GetUndoMapSegmentId(undoMapSegmentId);

    /* step3: Dump undo map */
    undoMapPageId.m_blockId++;
    for (uint32 i = 0; i < SEGMENT_ID_PAGES; ++i) {
        LoadUndoPage(undoMapPageId, pageBuf, BLCKSZ);
        char *rawPage = &pageBuf[0];
        OffsetNumber offset = static_cast<OffsetNumber>(sizeof(Page));
        for (uint32 j = 0; j < SEGMENT_ID_PER_PAGE; ++j) {
            ZoneId zid = static_cast<ZoneId>(i * SEGMENT_ID_PER_PAGE + j);
            PageId pageId = *static_cast<PageId *>(static_cast<void *>(rawPage + offset));
            offset += sizeof(PageId);
            if ((pageId.m_fileId == 0 && pageId.m_blockId == 0) || !pageId.IsValid()) {
                /* we find one invalid page id, and after it all the page is invalid */
                continue;
            }
            (void)fprintf(PageDiagnose::DumpToolHelper::dumpPrint, "zone id(%d): (%hu, %u)\n", zid, pageId.m_fileId,
                          pageId.m_blockId);
        }
        undoMapPageId.m_blockId++;
    }
    (void)fprintf(PageDiagnose::DumpToolHelper::dumpPrint, "\n");
    return;
}

void UndoDumpFileReader::GetUndoFileName(uint16_t fileId, char *fileName, uint32 fileNameLen)
{
    int rc = snprintf_s(fileName, fileNameLen, MAXPGPATH - 1, "%u", fileId);
    tool_securec_check_ss(rc);
}

void UndoDumpFileReader::DumpUndoRecords(PageId &pageId, int offset)
{
    char pageBuf[BLCKSZ];
    /* step1: Get undo map segment id */
    if (!m_isInitVfs) {
        if (unlikely(InitVfs() == RetStatus::DSTORE_FAIL)) {
            (void)fprintf(PageDiagnose::DumpToolHelper::dumpPrint, "vfs file reader init failed.\n");
            exit(-1);
        }
    }
    ItemPointerData nextUndoPtr(pageId, offset);
    while (nextUndoPtr != INVALID_UNDO_RECORD_PTR) {
        PageId curId = nextUndoPtr.GetPageId();
        OffsetNumber curOffset = nextUndoPtr.GetOffset();
        LoadUndoPage(curId, pageBuf, BLCKSZ);
        Page *page = reinterpret_cast<Page *>(pageBuf);
        if (page->GetType() != PageType::UNDO_PAGE_TYPE) {
            (void)fprintf(PageDiagnose::DumpToolHelper::dumpPrint, "Invalid undo page type: %d\n",
                          (int)page->GetType());
            exit(-1);
        }
        char *str = UndoMgrDiagnose::ReadUndoRecord((char *)page, curOffset, nextUndoPtr,
                                                    UndoDumpFileReader::ReadUndoPage, this);
        if (str != nullptr) {
            (void)fprintf(PageDiagnose::DumpToolHelper::dumpPrint, "%s\n", str);
            DestroyObject((void**)&str);
        }      
    }

    return;
}

void UndoDumpFileReader::Dump()
{
    if (m_config->undoCmdId == CMD_UNDO_MAP) {
        DumpUndoMap();
    } else if (m_config->undoCmdId == CMD_UNDO_RECORD) {
        m_config->vfsFileId = static_cast<uint16_t>(strtol(m_config->file, nullptr, 0));
        PageId pageId = {static_cast<uint16>(m_config->vfsFileId), static_cast<uint32>(m_config->blockNum)};
        DumpUndoRecords(pageId, m_config->offset);
    } else {
        (void)fprintf(PageDiagnose::DumpToolHelper::dumpPrint, "Invalid undo cmd id: %d\n", m_config->undoCmdId);
    }
    return;
}

void UndoDumpFileReader::ReadUndoPage(PageId &pageId, char *pageBuf, int pageSize, void *pThis)
{
    assert(pThis);
    UndoDumpFileReader *reader = static_cast<UndoDumpFileReader *>(pThis);
    reader->LoadUndoPage(pageId, pageBuf, pageSize);
}
} // namespace DSTORE