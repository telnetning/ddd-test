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

#ifndef DSTORE_HANG_MGR_ERROR_CODE_H
#define DSTORE_HANG_MGR_ERROR_CODE_H

#include "common/dstore_common_utils.h"
#include "framework/dstore_modules.h"
#include "errorcode/dstore_error_struct.h"

namespace DSTORE {

enum HangMgrErrorReason : uint32_t {
    FAILED_TO_REPORT_HANG_IN_SQL_CALLBACK = 0x1,
    HANG_MGR_OUT_OF_MEMORY = 0x2,
    HANG_MGR_ERROR_MAX
};

const ErrorCode HANG_MGR_ERROR_FAILED_TO_REPORT_HANG_IN_SQL_CALLBACK =
    (ERROR_MODULE_HANGMGR | ERROR_SEVERITY_ERROR | FAILED_TO_REPORT_HANG_IN_SQL_CALLBACK);
const ErrorCode HANG_MGR_ERROR_OUT_OF_MEMORY =
    (ERROR_MODULE_HANGMGR | ERROR_SEVERITY_ERROR | HANG_MGR_OUT_OF_MEMORY);

} /* namespace DSTORE */

#endif