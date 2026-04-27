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
 * dstore_output_utils.h
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/dstore/include/common/datatype/dstore_output_utils.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_DSTORE_OUTPUT_UTILS_H
#define DSTORE_DSTORE_OUTPUT_UTILS_H

#include <cmath>
#include "common/dstore_common_utils.h"
#include "catalog/dstore_function_struct.h"
#include "catalog/dstore_function.h"
#include "common/dstore_datatype.h"

namespace DSTORE {
/*
 *  output functions
 */
Datum BoolOut(FunctionCallInfo fcinfo);
Datum Int2Out(FunctionCallInfo fcinfo);
Datum Int4Out(FunctionCallInfo fcinfo);
Datum Int8Out(FunctionCallInfo fcinfo);
Datum Float4Out(FunctionCallInfo fcinfo);
Datum Float8Out(FunctionCallInfo fcinfo);
Datum VarcharOut(FunctionCallInfo fcinfo);

extern void GsItoa(int32 value, char *a);

}  // namespace DSTORE

#endif
