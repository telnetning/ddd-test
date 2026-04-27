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
 * tool_undo_dump.h
 *
 *
 *
 * IDENTIFICATION
 *        tools/pagedump/include/tool_undo_dump.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef TOOL_UNDO_DUMP_READER_H
#define TOOL_UNDO_DUMP_READER_H
#include "tool_page_dump_file_reader.h"
#include "undo/dstore_undo_zone.h"

namespace DSTORE {
constexpr int UNDO_FILE_COUNT_MAX_VALUE = 256;
struct UndoFileDesc {
    FileId fileId;
    FileDescriptor *fd;
};
class UndoDumpFileReader : public PageDumpFileReader {
public:
    explicit UndoDumpFileReader(PageDumpConfig *config);
    ~UndoDumpFileReader();

    void Dump();
    static void ReadUndoPage(PageId &pageId, char *pageBuf, int pageSize, void *pThis);
private:
    void DumpUndoMap();
    void ReadUndoRecord(char *page, UndoRecord &record, int startingByte);
    void DumpUndoRecords(PageId &pageId, int offset);
    PageId GetUndoMapSegmentId(PageId &pageId);

    /**
     * Load CSN metadata page, undo map seg ement id is stored in the page.
     * @param pageBuf Temp buffer to store metadata page
     * @param fileName Name of the control file to be loaded
     * @note This function is used to load CSN metadata page from control file.
     */
    void LoadCsnMetaPage(char *pageBuf, const char *fileName);
    PageId GetUndoMapMetaSegmentId();

    void LoadUndoPage(PageId &pageId, char *pageBuf, int pageSize);
    void GetUndoFileName(uint16_t fileId, char *fileName, uint32 fileNameLen);
    FileDescriptor *GetFileDesc(FileId fileId);
    FileDescriptor *OpenUndoFile(FileId fileId);
    UndoFileDesc m_fileDescArray[UNDO_FILE_COUNT_MAX_VALUE];
    int m_fdCount;
};
}  // namespace DSTORE

#endif