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
 * Description:
 * declare funcitons that can convert data in string form to various type during inserting core systable preset data.
 */

#ifndef DSTORE_FMGR_H
#define DSTORE_FMGR_H

#include "common/dstore_datatype.h"
#include "catalog/dstore_function_struct.h"

namespace DSTORE {
Datum OidIn(FunctionCallInfo fcinfo);
Datum NameIn(FunctionCallInfo fcinfo);
Datum Int2In(FunctionCallInfo fcinfo);
Datum BoolIn(FunctionCallInfo fcinfo);
Datum CharIn(FunctionCallInfo fcinfo);
Datum Int4In(FunctionCallInfo fcinfo);
Datum ByteaIn(FunctionCallInfo fcinfo);
Datum Int1In(FunctionCallInfo fcinfo);
Datum XidIn4(FunctionCallInfo fcinfo);
Datum Float8In(FunctionCallInfo fcinfo);
Datum XidIn(FunctionCallInfo fcinfo);
Datum PointerIn(FunctionCallInfo fcinfo);
Datum Int8In(FunctionCallInfo fcinfo);
Datum Float4In(FunctionCallInfo fcinfo);
Datum VarcharIn(FunctionCallInfo fcinfo);
}
#endif