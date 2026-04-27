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
 * dstore_page_diagnose.h
 *
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/dstore/include/page/dstore_page_dump_file_reader.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_PAGE_DUMP_FILE_READER_H
#define DSTORE_PAGE_DUMP_FILE_READER_H
#include "control/dstore_control_file.h"
#include "page/dstore_page_diagnose.h"
#include "tablespace/dstore_tablespace.h"

namespace DSTORE {

constexpr uint32_t CMD_UNDO_MAP = 1;
constexpr uint32_t CMD_UNDO_RECORD = 2;

struct PageDumpConfig {
    char *file;
    uint32_t blockNum;
    bool isShowTupleData;
    bool isControlFile;
    bool isDecodeDict;
    VFSType vfsType;
    uint32_t undoCmdId;
    uint32_t offset;
    char *vfsConfigFile;
    uint16_t vfsFileId;
    PageDiagnose::DumpCommConfig commConfig;
};

class PageDumpFileReader {
public:
    explicit PageDumpFileReader(PageDumpConfig *config);
    ~PageDumpFileReader();

    DSTORE::RetStatus OpenFile();
    void DumpAllPages() const;
    void DumpPage(uint32_t pageId) const;
    void DumpGetIndexMetaPage(Page *page, Page *metaPage) const;
protected:
    RetStatus InitVfs();
    void LoadPage(uint32_t pageId, char *pageBuf, int pageSize) const;
    void LoadPage(FileDescriptor * fileDesc, uint32_t pageId, char *pageBuf, int pageSize) const;
    char* DumpDataPage(uint32_t pageId) const;
    char* DumpControlFileMetaPage(uint32_t pageId) const;
    char* DumpControlFilePage(uint32_t pageId) const;
    char* DumpDecodeDictPage(uint32_t pageId) const;
    PageDiagnose::DumpToolHelper m_fileReader;
    PageDumpConfig *m_config;
    FileDescriptor *m_fileDesc;
    bool m_isInitVfs;
};
}  // namespace DSTORE

#endif