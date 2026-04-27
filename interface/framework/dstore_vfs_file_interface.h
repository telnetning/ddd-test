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
 * dstore_vfs_file_interface.h
 *
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/dstore/interface/framework/dstore_vfs_file_interface.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_VFS_FILE_INTERFACE_H
#define DSTORE_VFS_FILE_INTERFACE_H
#include "vfs/vfs_interface.h"
#include "common/dstore_common_utils.h"

namespace VfsInterface {
#pragma GCC visibility push(default)
struct DbConfigMap {
    char const *filename;
    uint16_t fileId;
    uint16_t fileLockId;
};

enum DbConfigFileId : uint16_t {
    GS_HBA_FILEID = 101,
    GS_HBA_LOCK_FILEID = 102,
    GS_HBA_BK_FILEID = 103,
    GAUSSDB_CONF_FILEID = 104,
    GAUSSDB_CONF_LOCK_FILEID = 105,
    GAUSSDB_CONF_BK_FILEID = 106,
    MOT_CONF_FILEID = 107,
    MOT_CONF_LOCK_FILEID = 108,
    GS_GAZELLE_FILEID = 109,
    GS_GAZELLE_LOCK_FILEID = 110,
    GS_IDENT_FILEID = 111,
    GS_IDENT_LOCK_FILEID = 112,
    MEMNODE_CONF_FILEID = 113,
    MEMNODE_CONF_LOCK_FILEID = 114,
    PG_VERSION_FILEID = 115,
    PG_VERSION_LOCK_FILEID = 116,
};

constexpr DbConfigMap DB_CONFIG_MAP[] = {
    {"gs_hba.conf", DbConfigFileId::GS_HBA_FILEID, DbConfigFileId::GS_HBA_LOCK_FILEID},
    {"gs_hba.conf", DbConfigFileId::GS_HBA_BK_FILEID, DbConfigFileId::GS_HBA_LOCK_FILEID},
    {"gaussdb.conf", DbConfigFileId::GAUSSDB_CONF_FILEID, DbConfigFileId::GAUSSDB_CONF_LOCK_FILEID},
    {"gaussdb.conf", DbConfigFileId::GAUSSDB_CONF_BK_FILEID, DbConfigFileId::GAUSSDB_CONF_LOCK_FILEID},
    {"gs_gazelle.conf", DbConfigFileId::GS_GAZELLE_FILEID, DbConfigFileId::GS_GAZELLE_LOCK_FILEID},
    {"gs_ident.conf", DbConfigFileId::GS_IDENT_FILEID, DbConfigFileId::GS_IDENT_LOCK_FILEID},
    {"PG_VERSION", DbConfigFileId::PG_VERSION_FILEID, DbConfigFileId::PG_VERSION_LOCK_FILEID},
};

constexpr unsigned long MAX_CONF_NAME_LEN = 50;

constexpr FileId BASE_FILE_ID = 200; /* user file id starts from 200, since file ids 101-107 used by config files. */
constexpr int MAX_TRY_LOCK_TIMES = 2000; /* max try lock time */
constexpr int TRY_LOCK_DELAY = 1000000;  /* 1s */

DSTORE::RetStatus CopyAllPssConfigFileToLocal(VirtualFileSystem *remoteVfs, const char *pgData, char *storeSpaceName);
DSTORE::RetStatus CopyAllLocalConfigFileToPss(VirtualFileSystem *remoteVfs, const char *pgData, char *storeSpaceName);
ErrorCode GetVfsFileLockAndCopy(VirtualFileSystem *remoteVfs, VirtualFileSystem *localVfs, uint16_t fileId,
                                const char *path, char *storeSpaceName, bool needRemoveExistFile);

#pragma GCC visibility pop

}  // namespace VfsInterface
#endif