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

#ifndef DSTORE_PDB_ERROR_CODE_MAP_H
#define DSTORE_PDB_ERROR_CODE_MAP_H

#include "errorcode/dstore_pdb_error_code.h"

namespace DSTORE {

static ErrorDetails g_pdb_error_code_map[] = {
    [STORAGE_OK] = {"STORAGE_OK", ""},
    [ERROR_GET_CODE(PDB_ERROR_STOP_SQL_THREAD_FAIL)] =
        {"PDB_ERROR_STOP_SQL_THREAD_FAIL", "node failure occurred or there are still active sessions on this pdb"},
    [ERROR_GET_CODE(PDB_ERROR_STOP_BACKGROUND_THREADS_FAIL)] =
        {"PDB_ERROR_STOP_BACKGROUND_THREADS_FAIL", "failed to stop pdb background threads"},
    [ERROR_GET_CODE(PDB_ERROR_RESET_FAIL)] =
        {"PDB_ERROR_RESET_FAIL", "failed to reset pdb"},
    [ERROR_GET_CODE(PDB_ERROR_UPDATE_STATUS_FAIL)] =
        {"PDB_ERROR_UPDATE_STATUS_FAIL", "failed to update pdb status"},
    [ERROR_GET_CODE(PDB_ERROR_IN_DR_RELATION)] =
        {"PDB_ERROR_IN_DR_RELATION", "pdb in primary-standby relation"},
    [ERROR_GET_CODE(PDB_ERROR_INVALIDATION_BUFFER_FAIL)] =
        {"PDB_ERROR_INVALIDATION_BUFFER_FAIL", "failed to invalidate pdb buffer"},
    [ERROR_GET_CODE(PDB_ERROR_RESET_TIME_FAIL)] =
        {"PDB_ERROR_RESET_TIME_FAIL", "failed to reset time"},
    [ERROR_GET_CODE(PDB_ERROR_NOT_INITIALIZED)] =
        {"PDB_ERROR_NOT_INITIALIZED", "the instance has not been initialized"},
    [ERROR_GET_CODE(PDB_ERROR_ACQUIRE_DIST_LOCK_FAIL)] =
        {"PDB_ERROR_ACQUIRE_DIST_LOCK_FAIL", "fail to acquire distributed pdb id lock"},
    [ERROR_GET_CODE(PDB_ERROR_INVALID_MESSAGE_ORDER)] =
        {"PDB_ERROR_INVALID_MESSAGE_ORDER", "The received messages arrived in an invalid order"},
    [ERROR_GET_CODE(PDB_ERROR_SWITCH_CONTEXT_FAIL)] =
        {"PDB_ERROR_SWITCH_CONTEXT_FAIL", "failed to switch context to target pdb"},
};

static_assert(sizeof(g_pdb_error_code_map) != PDB_ERROR_MAX, "Error code map size doesn't match");

} /* namespace DSTORE */

#endif
