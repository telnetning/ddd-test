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

#ifndef DSTORE_BACKUP_RESTORE_ERROR_CODE_MAP_H
#define DSTORE_BACKUP_RESTORE_ERROR_CODE_MAP_H

#include "errorcode/dstore_backup_restore_error_code.h"

namespace DSTORE {

static ErrorDetails g_backup_restore_error_code_map[] = {
    [STORAGE_OK] = {"STORAGE_OK", ""},
    [ERROR_GET_CODE(BACKUPRESTORE_ERROR_OUT_OF_MEMORY)] =
        {"BACKUPRESTORE_ERROR_OUT_OF_MEMORY", "Backup restore module out of memory"},
    [ERROR_GET_CODE(BACKUPRESTORE_ERROR_PDB_BEING_USED)] =
        {"BACKUPRESTORE_ERROR_PDB_BEING_USED", "Backup restore module pdb is being used"},
    [ERROR_GET_CODE(BACKUPRESTORE_ERROR_PDB_NOT_EXIST)] =
        {"BACKUPRESTORE_ERROR_PDB_NOT_EXIST", "Backup restore module pdb not exist"},
    [ERROR_GET_CODE(BACKUPRESTORE_ERROR_BACKUP_FAILED)] =
        {"BACKUPRESTORE_ERROR_BACKUP_FAILED", "Backup restore module execute backup failed"},
    [ERROR_GET_CODE(BACKUPRESTORE_ERROR_SNAPSHOT_ALREADY_EXIST)] =
        {"BACKUPRESTORE_ERROR_SNAPSHOT_ALREADY_EXIST", "Backup restore module snapshot is already exist"},
    [ERROR_GET_CODE(BACKUPRESTORE_ERROR_SNAPSHOT_COUNT_EXCEED_LIMIT)] =
        {"BACKUPRESTORE_ERROR_SNAPSHOT_COUNT_EXCEED_LIMIT", "Backup restore module snapshot count exceed limit"},
    [ERROR_GET_CODE(BACKUPRESTORE_ERROR_RESTORE_FAILED)] =
        {"BACKUPRESTORE_ERROR_RESTORE_FAILED", "Backup restore module execute restore failed"},
    [ERROR_GET_CODE(BACKUPRESTORE_ERROR_SNAPSHOT_NOT_EXIST)] =
        {"BACKUPRESTORE_ERROR_SNAPSHOT_NOT_EXIST", "Backup restore module snapshot not exist"},
    [ERROR_GET_CODE(BACKUPRESTORE_ERROR_DELETE_BACKUP_RESULT_FAILED)] =
        {"BACKUPRESTORE_ERROR_DELETE_BACKUP_RESULT_FAILED",
         "Backup restore module execute delete backup result failed"},
    [ERROR_GET_CODE(BACKUPRESTORE_ERROR_CLEAN_BACKUP_RESULT_FAILED)] =
        {"BACKUPRESTORE_ERROR_CLEAN_BACKUP_RESULT_FAILED", "Backup restore module execute clean backup result failed"},
    [ERROR_GET_CODE(BACKUPRESTORE_ERROR_BACKUP_NEED_RETRY)] =
        {"BACKUPRESTORE_ERROR_BACKUP_NEED_RETRY", "Backup restore module need retry backup"},
    [ERROR_GET_CODE(BACKUPRESTORE_ERROR_PDB_CLOSED)] =
        {"BACKUPRESTORE_ERROR_PDB_CLOSED", "Backup restore module pdb is closed"},
    [ERROR_GET_CODE(BACKUPRESTORE_ERROR_PREBACKUP_FAILED)] =
        {"BACKUPRESTORE_ERROR_PREBACKUP_FAILED", "Backup preparation failed."},
    [ERROR_GET_CODE(BACKUPRESTORE_ERROR_CLEAN_PREBACKUP_FAILED)] =
        {"BACKUPRESTORE_ERROR_CLEAN_PREBACKUP_FAILED", "Backup preparation cleanup failed."},
    [ERROR_GET_CODE(BACKUPRESTORE_ERROR_GET_CONSISTENCY_POINT_FAILED)] =
        {"BACKUPRESTORE_ERROR_GET_CONSISTENCY_POINT_FAILED", "Backup restore module get consistency point failed"},
    [ERROR_GET_CODE(BACKUPRESTORE_ERROR_PDB_STATUS_NOT_SUPPORT)] =
        {"BACKUPRESTORE_ERROR_PDB_STATUS_NOT_SUPPORT", "Backup pdb status is not support"},
    [ERROR_GET_CODE(BACKUPRESTORE_ERROR_RERUN_WAL_RECYCLE_FAILED)] =
        {"BACKUPRESTORE_ERROR_RERUN_WAL_RECYCLE_FAILED", "Backup clean rerun wal recycle failed"},
    [ERROR_GET_CODE(BACKUPRESTORE_ERROR_WAL_RECYCLE_FLAG_IS_NOT_RIGHT)] =
        {"BACKUPRESTORE_ERROR_WAL_RECYCLE_FLAG_IS_NOT_RIGHT", "Backup wal recycle flag is not right"},
};

static_assert(sizeof(g_backup_restore_error_code_map) != BACKUPRESTORE_ERROR_MAX, "Error code map size doesn't match");

} /* namespace DSTORE */

#endif
