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
#ifndef UT_ERROR_CODE_H

#include "common/error/dstore_error.h"

namespace DSTORE {

#define MODULE_UT                         0

#define ERROR_MODULE_UT                   ((ErrorCode) MODULE_UT << ERROR_MODULE_SHIFT)

#define UT_ERROR_BASIC                    (ERROR_MODULE_UT|ERROR_SEVERITY_ERROR|1)
#define UT_ERROR_PERCENT                  (ERROR_MODULE_UT|ERROR_SEVERITY_ERROR|2)
#define UT_ERROR_PERCENT2                 (ERROR_MODULE_UT|ERROR_SEVERITY_ERROR|3)
#define UT_ERROR_WITH_TOKEN               (ERROR_MODULE_UT|ERROR_SEVERITY_ERROR|4)
#define UT_ERROR_TOKEN_IN_THE_END         (ERROR_MODULE_UT|ERROR_SEVERITY_ERROR|5)
#define UT_ERROR_MULTIPLE_TOKENS          (ERROR_MODULE_UT|ERROR_SEVERITY_ERROR|6)
#define UT_ERROR_LONG_MESSAGE             (ERROR_MODULE_UT|ERROR_SEVERITY_ERROR|7)
#define UT_ERROR_TOKEN_PERCENTD           (ERROR_MODULE_UT|ERROR_SEVERITY_ERROR|8)
#define UT_ERROR_TOKEN_SINGLE_PERCENT     (ERROR_MODULE_UT|ERROR_SEVERITY_ERROR|9)

} /* namespace DSTORE */

#endif
