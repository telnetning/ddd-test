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
 * dstore_wal_dump_file_reader.h
 *
 * Description:
 * Wal private header file, mainly focus on reading wal files for tool WalDumper.
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_DSTORE_WAL_DUMP_FILE_READER_H
#define DSTORE_DSTORE_WAL_DUMP_FILE_READER_H

#include "common/dstore_datatype.h"
#include "wal/dstore_wal_struct.h"
#include "vfs/vfs_interface.h"
#include "config/dstore_vfs_config.h"
#include "framework/dstore_vfs_adapter.h"
#include "page/dstore_page_diagnose.h"
#include "wal/dstore_wal_file_reader.h"


namespace DSTORE {

constexpr uint32 WAL_FILE_NAME_LEN = 34;

enum GetPageInfoType : uint8 {
    GET_WAL_ID_ARRAY = 0,
    GET_WAL_CHECKPOINT,
    GET_VFS_NAME
};

struct WalDumpWalFileInfo {
    WalId walId;
    uint64 fileSize;
};

struct WalInfoArgs {
    WalId **walIdArray;
    uint32 **walFileSizeArray;
    uint32 *walIdCount;
};

struct WalCheckPointInfoArgs {
    WalId walId;
    WalCheckPoint *checkpoint;
};

struct VfsInfoArgs {
    PdbId pdbId;
    char *vfsName;
    uint32 vfsNameSize;
};

typedef RetStatus (*PreGetInfoFunction)(void *args);
typedef RetStatus (*GetInfoFunction)(ControlPage *controlPage, uint32 curPageId, void *args, bool *finishGetInfo);
typedef RetStatus (*PostGetInfoFunction)(RetStatus res, void *args);

struct GetInfoProcessFunctionTableForWaldump {
    GetPageInfoType getType;
    ControlPageType pageType;
    PreGetInfoFunction preGetInfoFunc;
    GetInfoFunction getInfoFunc;
    PostGetInfoFunction postGetInfoFunc;
};

enum class WalDumpCommandType {
    DUMP_WAL_RECORD,
    HELP,
    VERSION,
    LIST_MODULE
};

class WalDumpFileReader : virtual public BaseObject, virtual public WalStreamBytesReader  {
public:
    static RetStatus GetPageInfoFromControlFile(PageDiagnose::DumpToolHelperInitParam *param, char *vfsConfigPath,
        GetPageInfoType getType, void *args);

    static bool IsTargetTypePage(ControlPage *pageBuf, ControlPageType pageType, uint32_t curPageId);

    WalDumpFileReader(char *path, StorageType vfsType, uint64 checkpointPlsn);

    ~WalDumpFileReader() final;

    void CloseAllFiles();

    void Destroy();

    RetStatus Init(bool reuseVfs, VFSAdapter *vfs, char *pdbVfsName, PageDiagnose::DumpCommConfig *commConfig);

    void SetWalDumpWalFileInfo(WalId walId, uint32 walFileSize);

    RetStatus SetCheckpointInfo();

    RetStatus Read(uint64 plsn, uint8 *data, uint64 readLen, uint64 *resultLen) override;

    RetStatus GetWalFileHeader(uint64 plsn, WalFileHeaderData *walFileHeaderData);

    WalDumpWalFileInfo GetWalFileInfo() const;

    uint64 GetPrevReadStartPoint(uint64 plsn) override;

    void GetWalFilePlsnRange(uint64 *startPlsn, uint64 *endPlsn);

    bool WalStreamIsEmpty();

private:
    RetStatus GetFileInfoFromControlFile(WalId walId);

    WalFileHeaderData *GetFileHeader(uint64 fileStartPlsn);

    RetStatus ParseFileHeader(uint64 fileStartPlsn);

    inline RetStatus MakeWalFileName(char *name, uint32 maxLen, uint64 startPlsn) const;

    RetStatus OpenFile(uint64 startPlsn, const char *fileName, int flags, FileDescriptor **fd, int32_t *index);
    FileDescriptor *GetFileDescriptor(uint64 fileStartPlsn);

    int32_t GetFileIndex(uint64 fileStartPlsn);

    RetStatus InitWalFileInfo();
    RetStatus InitLocalWalFileInfo();
    RetStatus InitPageStoreWalFileInfo();

    RetStatus TryExpandWalFilesArray();
    void UpdateStartEndPlsn(uint64_t fileStartPlsn, uint64_t fileSize);

    static RetStatus PreGetPageInfo(GetPageInfoType getType, void *args);

    static RetStatus GetPageInfo(ControlPage *controlPage, uint32 curPageId, GetPageInfoType getType, void *args,
        bool *finishGetInfo);

    static RetStatus PostGetPageInfo(GetPageInfoType getType, RetStatus res, void *args);

    DSTORE::StorageType m_vfsType;
    PageDiagnose::DumpToolHelper m_fileReader;
    char *m_walDir;
    VFSPageStoreConfig m_pageStoreConfig;
    WalDumpWalFileInfo m_walFileInfo;
    uint32 m_fileNum;
    uint32 m_fileArrayLen;
    uint64 *m_fileStartPlsnArray;
    WalFileHeaderData *m_walFileHeaders;
    FileDescriptor **m_fileDescArrays;
    TenantConfig m_tenantConfig;
    uint64 m_startPlsn;
    uint64 m_endPlsn;

    uint64 m_checkpointPlsn;
    bool m_walStreamEmpty;
};

}
#endif
