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

#ifndef DSTORE_RECOVERY_ERROR_CODE_MAP_H
#define DSTORE_RECOVERY_ERROR_CODE_MAP_H

#include "errorcode/dstore_recovery_error_code.h"

namespace DSTORE {

static ErrorDetails g_recovery_error_code_map[] = {
    [STORAGE_OK] = {"STORAGE_OK", ""},
    [ERROR_GET_CODE(RPC_ERROR_COMMON_OUT_OF_MEMORY)] =
        {"RPC_ERROR_COMMON_OUT_OF_MEMORY", "Out of memoery."},
    [ERROR_GET_CODE(RPC_ERROR_COMMON_SEND_SANITY_CHECK)] =
        {"RPC_ERROR_COMMON_SEND_SANITY_CHECK", "Sanity check error before sending."},
    [ERROR_GET_CODE(RPC_ERROR_COMMON_RECEIVE_SANITY_CHECK)] =
        {"RPC_ERROR_COMMON_RECEIVE_SANITY_CHECK", "Sanity check error after receiving."},
    [ERROR_GET_CODE(RPC_ERROR_FAIL)] =
        {"RPC_ERROR_FAIL", "Something wrong on remote node."},
    [ERROR_GET_CODE(RPC_ERROR_ASYNC_BROADCAST)] =
        {"RPC_ERROR_ASYNC_BROADCAST", "Async broadcast failed."},
    [ERROR_GET_CODE(RPC_ERROR_CKPT_NOT_INITED)] =
        {"RPC_ERROR_CKPT_NOT_INITED", "Checkpointer has not inited."},
    [ERROR_GET_CODE(RPC_ERROR_CKPT_FAIL_TO_ALLOC_STAT_INFO_MEM)] =
        {"RPC_ERROR_CKPT_FAIL_TO_ALLOC_STAT_INFO_MEM", "Failed to alloc memory for wal ckpt stat info."},
};

static_assert(sizeof(g_recovery_error_code_map) != RECOVERY_ERROR_MAX, "Error code map size doesn't match");

} /* namespace DSTORE */

#endif
