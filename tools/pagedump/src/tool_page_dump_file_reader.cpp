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
 */
#include "tool_page_dump_file_reader.h"
#include <cassert>
#include <unistd.h>
#include <cstdio>
#include <cstdlib>
#include <fcntl.h>
#include "framework/dstore_instance_interface.h"
#include "page/dstore_page_struct.h"
#include "page/dstore_page_diagnose.h"
#include "logical_replication/dstore_decode_dict_file.h"

namespace DSTORE {
PageDumpFileReader::PageDumpFileReader(PageDumpConfig *config)
    : m_fileReader(config->vfsType == VFS_TYPE_PAGE_STORE ? StorageType::PAGESTORE : StorageType::LOCAL,
    config->vfsConfigFile),
      m_config(config),
      m_fileDesc(nullptr),
      m_isInitVfs(false)
{}

PageDumpFileReader::~PageDumpFileReader()
{
    /* Close file */
    if (m_fileDesc) {
        m_fileReader.Close(m_fileDesc);
        m_fileDesc = nullptr;
    }
    m_fileReader.Destroy();
}

DSTORE::RetStatus PageDumpFileReader::OpenFile()
{
    DSTORE::RetStatus ret = DSTORE::DSTORE_SUCC;
    if (!m_isInitVfs) {
        if (unlikely(InitVfs() == RetStatus::DSTORE_FAIL)) {
            (void)printf("vfs file reader init failed.\n");
            return DSTORE::DSTORE_FAIL;
        }
    }
    if (m_config->vfsType == VFS_TYPE_PAGE_STORE) {
        (void)printf("pagedump pagestore\n");
        char fileName[MAXPGPATH];

        if (m_config->isControlFile) {
            ret = m_fileReader.Open(DATABASE_CONTROL_FILE_1_NAME, FILE_READ_ONLY_FLAG, &m_fileDesc);
        } else if (m_config->isDecodeDict) {
            ret = m_fileReader.Open(DECODEDICT_FILE_1_NAME, FILE_READ_ONLY_FLAG, &m_fileDesc);
        } else {
            PageDiagnose::GetFileName(m_config->vfsFileId, fileName, MAXPGPATH);
            ret = m_fileReader.Open(fileName, FILE_READ_ONLY_FLAG, &m_fileDesc);
        }
        if (ret == RetStatus::DSTORE_FAIL) {
            if (m_config->isControlFile) {
                /* Retry to open DATABASE_CONTROL_FILE_2_NAME. With DATABASE_CONTROL_FILE_1_NAME is opened by default */
                ret = m_fileReader.Open(DATABASE_CONTROL_FILE_2_NAME, FILE_READ_ONLY_FLAG, &m_fileDesc);
            } else if (m_config->isDecodeDict) {
                /* Retry to open DECODEDICT_FILE_2_NAME. With DECODEDICT_FILE_1_NAME is opened by default */
                ret = m_fileReader.Open(DECODEDICT_FILE_2_NAME, FILE_READ_ONLY_FLAG, &m_fileDesc);
            }
        }
    } else if (m_config->vfsType == VFS_TYPE_LOCAL_FS) {
        (void)printf("pagedump local\n");
        char *realPath = realpath(m_config->file, nullptr);
        if (realPath == nullptr) {
            (void)printf("Open file failed. Invalid file name %s\n", m_config->file);
            return DSTORE::DSTORE_FAIL;
        }
        ret = m_fileReader.Open(realPath, FILE_READ_ONLY_FLAG, &m_fileDesc);
        free(realPath);
    } else {
        (void)printf("-t vfsType should be 0 or 1, now is %hhu\n", static_cast<uint8>(m_config->vfsType));
        return DSTORE::DSTORE_FAIL;
    }

    if (unlikely(ret == RetStatus::DSTORE_FAIL)) {
        (void)printf("Open file (%s) failed\n", m_config->file);
        return DSTORE::DSTORE_FAIL;
    }

    return DSTORE::DSTORE_SUCC;
}

void PageDumpFileReader::DumpAllPages() const
{
    off_t tailOffset = m_fileReader.Size(m_fileDesc);
    assert(tailOffset % BLCKSZ == 0);
    uint32_t maxPageId = static_cast<uint32_t>(tailOffset / BLCKSZ);
    for (uint32_t i = 0; i < maxPageId; ++i) {
        DumpPage(i);
    }
}

void PageDumpFileReader::LoadPage(FileDescriptor * fileDesc, uint32_t pageId, char *pageBuf, int pageSize) const
{
    if (unlikely(fileDesc == nullptr || pageBuf == nullptr)) {
        (void)printf("read page failed for fileDesc or pageBuf is null");
        exit(-1);
    }
    off_t tailOffset = m_fileReader.Size(fileDesc);
    assert(tailOffset % BLCKSZ == 0);
    uint32_t maxPageId = static_cast<uint32_t>(tailOffset / BLCKSZ);
    if (unlikely(pageId >= maxPageId)) {
        (void)printf("read failed: page id(%u) shoud less than max page id(%u)\n", pageId, maxPageId);
        exit(-1);
    }
    DSTORE::RetStatus ret = m_fileReader.ReadPage(fileDesc, pageId, pageBuf, pageSize);
    if (unlikely(ret == RetStatus::DSTORE_FAIL)) {
        (void)printf("read failed: page id(%u)\n", pageId);
        exit(-1);
    }
    return;
}

void PageDumpFileReader::LoadPage(uint32_t pageId, char *pageBuf, __attribute__((unused)) int pageSize) const
{
    LoadPage(m_fileDesc, pageId, pageBuf, pageSize);
}

void PageDumpFileReader::DumpGetIndexMetaPage(Page *page, Page *metaPage) const
{
    unsigned metaPageBlockId = PageDiagnose::GetIndexMetaPageBlockId(page);
    unsigned short metaPageFileId = PageDiagnose::GetIndexMetaFileId(page);

    PageDumpConfig localConfig;
    errno_t rc = memcpy_s(&localConfig, sizeof(PageDumpConfig), m_config,
        sizeof(PageDumpConfig));
    if (rc != 0) {
        (void)printf("copy memory failed\n");
        exit(-1);
    }

    bool needNewReader = false;

    if (metaPageFileId != INVALID_VFS_FILE_ID && metaPageFileId != m_config->vfsFileId) {
        localConfig.vfsFileId = metaPageFileId;
        needNewReader = true;
    }

    if (needNewReader) {
        PageDumpFileReader *pageDumpIndex = new PageDumpFileReader(&localConfig);
        if (unlikely(pageDumpIndex->OpenFile() == DSTORE::DSTORE_FAIL)) {
            (void)printf("open btree meta page file failed\n");
            delete pageDumpIndex;
            exit(-1);
        }

        if (metaPageBlockId != DSTORE_INVALID_BLOCK_NUMBER) {
            DSTORE::RetStatus ret = pageDumpIndex->m_fileReader.ReadPage(pageDumpIndex->m_fileDesc,
                metaPageBlockId, metaPage, BLCKSZ);
            if (unlikely(ret == RetStatus::DSTORE_FAIL)) {
                (void)printf("read btree meta page failed\n");
                delete pageDumpIndex;
                exit(-1);
            }
        }

        delete pageDumpIndex;
    } else {
        if (metaPageBlockId != DSTORE_INVALID_BLOCK_NUMBER) {
            DSTORE::RetStatus ret = m_fileReader.ReadPage(m_fileDesc, metaPageBlockId, metaPage, BLCKSZ);
            if (unlikely(ret == RetStatus::DSTORE_FAIL)) {
                (void)printf("read btree meta page failed\n");
                exit(-1);
            }
        }
    }
}

char* PageDumpFileReader::DumpDataPage(uint32_t pageId) const
{
    char pageBuf[BLCKSZ];

    LoadPage(pageId, pageBuf, BLCKSZ);
    Page *page = STATIC_CAST_PTR_TYPE(pageBuf, Page*);
    char *str = nullptr;
    if (PageDiagnose::GetPageType(page) == PageType::INDEX_PAGE_TYPE) {
        char metaBuf[BLCKSZ];
        Page *metaPage = static_cast<Page *>(static_cast<void *>(metaBuf));
        DumpGetIndexMetaPage(page, metaPage);
        str = PageDiagnose::PageDump(page, m_config->isShowTupleData, metaPage);
    } else {
        str = PageDiagnose::PageDump(page, m_config->isShowTupleData);
    }
    return str;
}

char* PageDumpFileReader::DumpControlFileMetaPage(uint32_t pageId) const
{
    char pageBuf[BLCKSZ];

    LoadPage(pageId, pageBuf, BLCKSZ);
    return PageDiagnose::PageDump(STATIC_CAST_PTR_TYPE(pageBuf, ControlFileMetaPage*));
}

char* PageDumpFileReader::DumpControlFilePage(uint32_t pageId) const
{
    char pageBuf[BLCKSZ];

    LoadPage(pageId, pageBuf, BLCKSZ);
    return PageDiagnose::PageDump(STATIC_CAST_PTR_TYPE(pageBuf, ControlPage*));
}

char* PageDumpFileReader::DumpDecodeDictPage(uint32_t pageId) const
{
    char pageBuf[BLCKSZ];

    LoadPage(pageId, pageBuf, BLCKSZ);
    return pageId == DECODE_DICT_META_BLOCK ? PageDiagnose::PageDump(STATIC_CAST_PTR_TYPE(pageBuf, DecodeDictMetaPage*))
                                            : PageDiagnose::PageDump(STATIC_CAST_PTR_TYPE(pageBuf, DecodeDictPage*));
}

void PageDumpFileReader::DumpPage(uint32_t pageId) const
{
    char *str = nullptr;

    /* Step 2: Dump page data */
    if (m_config->isControlFile) {
        str = (pageId == 0) ? DumpControlFileMetaPage(pageId) :
            DumpControlFilePage(pageId);
    } else if (m_config->isDecodeDict) {
        str = DumpDecodeDictPage(pageId);
    } else {
        str = DumpDataPage(pageId);
    }

    if (str != nullptr) {
        (void)printf("Start page id %u\n-------------------\n", pageId);
        (void)printf("%s\n", str);
        (void)printf("\n-------------------\n");
        DestroyObject((void**)&str);
        (void)printf("End page id %u\n-------------------\n", pageId);
    }
}
RetStatus PageDumpFileReader::InitVfs()
{
    char pdbVfsName[DSTORE_VFS_NAME_MAX_LEN] = {0};
    PageDiagnose::DumpToolHelperInitParam param = {
        .reuseVfs = false,
        .vfs = nullptr,
        .pdbVfsName = pdbVfsName,
        .commConfig = &m_config->commConfig
    };
    DSTORE::RetStatus ret = m_fileReader.Init(&param);
    if (ret == DSTORE::DSTORE_SUCC) {
        m_isInitVfs = true;
    }
    return ret;
}
}
