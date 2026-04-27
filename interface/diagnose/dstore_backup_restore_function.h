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
 * dstore_backup_restore_function.h
 *
 * Description:
 * Provide function for backup and restore process.
 * ---------------------------------------------------------------------------------------
 *
 */
#ifndef DSTORE_DSTORE_BACKUP_RESTORE_FUNCTION_H
#define DSTORE_DSTORE_BACKUP_RESTORE_FUNCTION_H
#include <cstdint>
#include <thread>
#include "common/dstore_common_utils.h"
#include "vfs/vfs_interface.h"
#include "diagnose/dstore_backup_restore_function.h"
namespace DSTORE {
#pragma GCC visibility push(default)

struct ResultInfo {
    ErrorCode errCode;
    char *errMsg;
};

struct BackupOneWalStreamInfo {
    uint64_t walId;
    uint64_t plsn;
};

struct BackupWalInfo {
    uint32_t walStreamCnt;
    BackupOneWalStreamInfo *walStreamInfoList;
};

struct BackupConsistencyPoint {
    uint64_t csn;
    BackupWalInfo walInfo;
};

struct RestoreStartAndEndPlsn {
    uint64_t restoreStartPlsn;
    uint64_t restoreEndPlsn;
};

struct Level1RestorePara {
    char *pdbName;
    BackupWalInfo *checkPoint;
    BackupConsistencyPoint *consistencyPoint;
    void *allPdbInfo;
    char *inplaceSnapshotName;
    char *appendSnapshotName;
};

struct Level1RestorePdbPara {
    char *restorePdbUuid;
    char *inplaceSnapshotName;
    char *appendSnapshotName;
    DSTORE::BackupConsistencyPoint consistencyPoint;
    DSTORE::BackupWalInfo checkPoint;
    bool needRestore;
};

class BackupRestoreFunction {
public:
    static ResultInfo DoLevel1Backup(uint32_t pdbCount, PdbId pdbIdArray[],
        const char *inplaceSnapshotBaseName, const char *appendSnapshotBaseName);

    static ErrorCode Level1RestorePdb(Level1RestorePara *para);

    static RetStatus GetAllPdbIdForBackupRestore(PdbId *pdbIdArray, uint32_t &count);

    static ErrorCode CleanBackupResult(uint32_t pdbCount, PdbId pdbIdArray[],
        const char *inplaceSnapshotBaseName, const char *appendSnapshotBaseName);

    static ErrorCode DeleteBackupResult(PdbId pdbId, const char *inplaceSnapshotName, const char *appendSnapshotName);

    static ErrorCode Level1ParallRestorePdbs(Level1RestorePdbPara *pdbParas, uint32_t pdbCount);
    
    static bool CheckPdbOpenMode(PdbId pdbId);

    static RetStatus LockBackupRestoreOperation();

    static void UnlockBackupRestoreOperation();
};
#pragma GCC visibility pop
}  // namespace DSTORE

#endif /* DSTORE_STORAGE_BACKUP_RESTORE_FUNCTION_H */