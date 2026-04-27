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

#ifndef DSTORE_TRANSACTION_ERROR_CODE_MAP_H
#define DSTORE_TRANSACTION_ERROR_CODE_MAP_H

#include "errorcode/dstore_transaction_error_code.h"

namespace DSTORE {

static ErrorDetails g_transaction_error_code_map[] = {
    [STORAGE_OK] = {"STORAGE_OK", ""},
    [ERROR_GET_CODE(TRANSACTION_WARNING_NO_ACTIVE_TRANSACTION)] =
        {"TRANSACTION_WARNING_NO_ACTIVE_TRANSACTION", "There is no transaction in progress."},
    [ERROR_GET_CODE(TRANSACTION_WARNING_ACTIVE_SQL_TRANSACTION)] =
        {"TRANSACTION_WARNING_ACTIVE_SQL_TRANSACTION", "There is already a transaction in progress."},
    [ERROR_GET_CODE(TRANSACTION_ERROR_INVALID_TBLOCK_STATE)] =
        {"TRANSACTION_ERROR_INVALID_TBLOCK_STATE", "Invalid transaction block state %s."},
    [ERROR_GET_CODE(TRANSACTION_ERROR_INVALID_STATE)] =
        {"TRANSACTION_ERROR_INVALID_STATE", "Invalid transaction state: %s."},
    [ERROR_GET_CODE(TRANSACTION_ERROR_INVALID_CSN)] =
        {"TRANSACTION_ERROR_INVALID_CSN", "Invalid csn."},
    [ERROR_GET_CODE(TRANSACTION_ERROR_INVALID_SAVEPOINT)] =
        {"TRANSACTION_ERROR_INVALID_SAVEPOINT", "Can't create savepoint, no data change yet in this transaction."},
    [ERROR_GET_CODE(TRANSACTION_ERROR_SAVEPOINT_NOT_FOUND)] =
        {"TRANSACTION_ERROR_SAVEPOINT_NOT_FOUND", "Savepoint:%s cannot be found."},
    [ERROR_GET_CODE(TRANSACTION_INFO_SAME_THREAD_DEADLOCK)] =
        {"TRANSACTION_INFO_SAME_THREAD_DEADLOCK",
         "Deadlock detected between primary transaction and autonomous transaction or between "
         "two autonomous transactions."},
    [ERROR_GET_CODE(TRANSACTION_ERROR_OUT_OF_MEMORY)] =
        {"TRANSACTION_ERROR_OUT_OF_MEMORY", "Transaction runs out of memory."},
    [ERROR_GET_CODE(TRANSACTION_ERROR_DISABLE_SQL_TRANSACTION)] =
        {"TRANSACTION_ERROR_DISABLE_SQL_TRANSACTION", "Transaction is prohibited when pdb is demoting."},
    [ERROR_GET_CODE(TRANSACTION_ERROR_PDB_FENCE_WRITE)] =
        {"TRANSACTION_ERROR_PDB_FENCE_WRITE", "Only primary pdb can execute write transaction."},
    [ERROR_GET_CODE(TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED)] =
        {"TRANSACTION_ERROR_RUNTIME_NOT_INITIALIZED", "Transacton runtime is not initialized."},
    [ERROR_GET_CODE(TRANSACTION_ERROR_INVALID_PARAM)] =
        {"TRANSACTION_ERROR_INVALID_PARAM", "Invalid parameter for transaction runtime."},
    [ERROR_GET_CODE(TRANSACTION_ERROR_CSN_OWNER_ONLY_CALL)] =
        {"TRANSACTION_ERROR_CSN_OWNER_ONLY_CALL", "%s should not be called by non csn owner"},
    [ERROR_GET_CODE(TRANSACTION_ERROR_NODE_RPC_TIMEOUT)] =
        {"TRANSACTION_ERROR_NODE_RPC_TIMEOUT", "RPC Timed out"},
    [ERROR_GET_CODE(TRANSACTION_ERROR_FAILED_TO_GET_BLOCK_COUNT)] =
        {"TRANSACTION_ERROR_FAILED_TO_GET_BLOCK_COUNT", "Failed to get block count"},
    [ERROR_GET_CODE(TRANSACTION_ERROR_FAILED_TO_EXTEND_BLOCK)] =
        {"TRANSACTION_ERROR_FAILED_TO_EXTEND_BLOCK", "Failed to extend block"},
    [ERROR_GET_CODE(TRANSACTION_ERROR_FILE_ACCESS)] =
        {"TRANSACTION_ERROR_FILE_ACCESS", "Failed to access file: %s"},
    [ERROR_GET_CODE(TRANSACTION_ERROR_INCOMPLETE_RPC_MESSAGE)] =
        {"TRANSACTION_ERROR_INCOMPLETE_RPC_MESSAGE", "Incomplete RPC message"},
    [ERROR_GET_CODE(TRANSACTION_ERROR_RPC_SEND_RECEIVE)] =
        {"TRANSACTION_ERROR_RPC_SEND_RECEIVE", "RPC Send/Receive Error"},
    [ERROR_GET_CODE(TRANSACTION_ERROR_NODE_RPC_ERROR)] =
        {"TRANSACTION_ERROR_NODE_RPC_ERROR", "RPC Error Code Received from Node"},
    [ERROR_GET_CODE(TRANSACTION_ERROR_GLOBAL_CLOCK_NOT_SYNCED)] =
        {"TRANSACTION_ERROR_GLOBAL_CLOCK_NOT_SYNCED", "Global clock has not been synchronized"},
    [ERROR_GET_CODE(TRANSACTION_WARNING_GCLOCK_OVER_WAIT_TIME)] =
        {"TRANSACTION_WARNING_GCLOCK_OVER_WAIT_TIME",
         "Syncing %lu (ms) takes longer than max wait time %d (ms), skip sync data for this round."},
    [ERROR_GET_CODE(TRANSACTION_WARNING_GCLOCK_BIGGER_ERROR_BOUND)] =
        {"TRANSACTION_WARNING_GCLOCK_BIGGER_ERROR_BOUND",
         "This newly collected sync data will NOT make error bound tighter"},
    [ERROR_GET_CODE(TRANSACTION_WARNING_EXCEED_MAX_ERRBOUND)] =
        {"TRANSACTION_WARNING_EXCEED_MAX_ERRBOUND", "Get timestamp with maximum allowed errbound : %ld"},
    [ERROR_GET_CODE(TRANSACTION_WARNING_GCLOCK_CANNOT_SYNC_WITH_CSN_OWNER)] =
        {"TRANSACTION_WARNING_GCLOCK_CANNOT_SYNC_WITH_CSN_OWNER",
         "Cannot sync with csn-owner with csn, assigning new CSN got blocked"},
    [ERROR_GET_CODE(TRANSACTION_ERROR_FAILED_TO_GET_CSN)] =
        {"TRANSACTION_ERROR_FAILED_TO_GET_CSN", "Failed to get csn: result = %lu"},
    [ERROR_GET_CODE(TRANSACTION_ERROR_WAIT_TIMEOUT)] =
        {"TRANSACTION_ERROR_WAIT_TIMEOUT", "wait transaction timeout, retried %lu times."},
    [ERROR_GET_CODE(TRANSACTION_ERROR_REMOTE_FAILED)] =
        {"TRANSACTION_ERROR_REMOTE_FAILED", "Remote node[%u] failed."},
};

static_assert(sizeof(g_transaction_error_code_map) != TRANSACTION_ERROR_MAX, "Error code map size doesn't match");

} /* namespace DSTORE */

#endif
