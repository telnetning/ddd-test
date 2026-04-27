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

#ifndef DSTORE_RECOVERY_ERROR_CODE_H
#define DSTORE_RECOVERY_ERROR_CODE_H

#include "common/dstore_common_utils.h"
#include "framework/dstore_modules.h"
#include "errorcode/dstore_error_struct.h"

namespace DSTORE {

enum RecoveryErrorReason : uint32_t {
    RECOVERY_OUT_OF_MEMORY = 0x1,
    RECOVERY_SEND_SANITY_CHECK = 0x2,
    RECOVERY_RECEIVE_SANITY_CHECK = 0x3,
    RECOVERY_FAIL = 0x4,
    RECOVERY_ASYNC_BROADCAST = 0x5,
    RECOVERY_CKPT_NOT_INITED = 0x6,
    RECOVERY_CKPT_FAIL_TO_ALLOC_STAT_INFO_MEM = 0x7,
    RECOVERY_ERROR_MAX
};

const ErrorCode RPC_ERROR_COMMON_OUT_OF_MEMORY =
    (ERROR_MODULE_RECOVERY | ERROR_SEVERITY_ERROR | RECOVERY_OUT_OF_MEMORY);
const ErrorCode RPC_ERROR_COMMON_SEND_SANITY_CHECK =
    (ERROR_MODULE_RECOVERY | ERROR_SEVERITY_ERROR | RECOVERY_SEND_SANITY_CHECK);
const ErrorCode RPC_ERROR_COMMON_RECEIVE_SANITY_CHECK =
    (ERROR_MODULE_RECOVERY | ERROR_SEVERITY_ERROR | RECOVERY_RECEIVE_SANITY_CHECK);
const ErrorCode RPC_ERROR_FAIL =
    (ERROR_MODULE_RECOVERY | ERROR_SEVERITY_ERROR | RECOVERY_FAIL);
const ErrorCode RPC_ERROR_ASYNC_BROADCAST =
    (ERROR_MODULE_RECOVERY | ERROR_SEVERITY_ERROR | RECOVERY_ASYNC_BROADCAST);
const ErrorCode RPC_ERROR_CKPT_NOT_INITED =
    (ERROR_MODULE_RECOVERY | ERROR_SEVERITY_ERROR | RECOVERY_CKPT_NOT_INITED);
const ErrorCode RPC_ERROR_CKPT_FAIL_TO_ALLOC_STAT_INFO_MEM =
    (ERROR_MODULE_RECOVERY | ERROR_SEVERITY_ERROR | RECOVERY_CKPT_FAIL_TO_ALLOC_STAT_INFO_MEM);

} /* namespace DSTORE */

#endif
