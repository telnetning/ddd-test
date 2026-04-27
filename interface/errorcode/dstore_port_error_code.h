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

#ifndef DSTORE_PORT_ERROR_CODE_H
#define DSTORE_PORT_ERROR_CODE_H

#include "common/dstore_common_utils.h"
#include "framework/dstore_modules.h"
#include "errorcode/dstore_error_struct.h"

namespace DSTORE {

enum PortErrorReason : uint32_t {
    PORT_ERROR_REASON_1 = 0x1,
    PORT_ERROR_REASON_2 = 0x2,
    PORT_ERROR_REASON_3 = 0x3,
    PORT_ERROR_MAX
};

const ErrorCode PORT_ERROR_1 = (ERROR_MODULE_PORT | ERROR_SEVERITY_ERROR | PORT_ERROR_REASON_1);
const ErrorCode PORT_ERROR_2 = (ERROR_MODULE_PORT | ERROR_SEVERITY_ERROR | PORT_ERROR_REASON_2);
const ErrorCode PORT_ERROR_3 = (ERROR_MODULE_PORT | ERROR_SEVERITY_ERROR | PORT_ERROR_REASON_3);

} /* namespace DSTORE */

#endif
