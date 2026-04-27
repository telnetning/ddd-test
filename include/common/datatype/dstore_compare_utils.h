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
 * dstore_compare_util.h
 *
 * IDENTIFICATION
 *        dstore/include/common/datatype/dstore_compare_util.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_DSTORE_COMPARE_UTILS_H
#define DSTORE_DSTORE_COMPARE_UTILS_H

#include <cmath>

#include "common/algorithm/dstore_tuplesort.h"
#include "common/datatype/dstore_array_utils.h"
#include "common/datatype/dstore_varlena_utils.h"

namespace DSTORE {

/*
 *  comparison functions
 */
Datum BtreeInt2Cmp(FunctionCallInfo fcinfo);
Datum BtreeInt4Cmp(FunctionCallInfo fcinfo);
Datum BtreeInt8Cmp(FunctionCallInfo fcinfo);
Datum BtreeFloat4Cmp(FunctionCallInfo fcinfo);
Datum BtreeFloat8Cmp(FunctionCallInfo fcinfo);
Datum BtreeOidCmp(FunctionCallInfo fcinfo);
Datum BtreeOidvectorCmp(FunctionCallInfo fcinfo);
Datum BtreeInt2vectorCmp(FunctionCallInfo fcinfo);
Datum BtreeCharCmp(FunctionCallInfo fcinfo);
Datum BtreeNameCmp(FunctionCallInfo fcinfo);
Datum BtreeTextCmp(FunctionCallInfo fcinfo);
Datum BtreeCashCmp(FunctionCallInfo fcinfo);
Datum BtreeDateCmp(FunctionCallInfo fcinfo);
Datum BtreeTimeCmp(FunctionCallInfo fcinfo);
Datum BtreeTimestamptzCmp(FunctionCallInfo fcinfo);
Datum BtreeIntervalCmp(FunctionCallInfo fcinfo);
Datum BtreeTimetzCmp(FunctionCallInfo fcinfo);
Datum BtreeMacaddrCmp(FunctionCallInfo fcinfo);
Datum BtreeInetCmp(FunctionCallInfo fcinfo);
Datum BtreeBoolCmp(FunctionCallInfo fcinfo);
Datum BtreeTimeStampCmp(FunctionCallInfo fcinfo);
Datum BtreeTextPatternCmp(FunctionCallInfo fcinfo);
Datum BtreeDateaCmp(FunctionCallInfo fcinfo);

Datum BtreeInt48Cmp(FunctionCallInfo fcinfo);
Datum BtreeInt84Cmp(FunctionCallInfo fcinfo);
Datum BtreeInt24Cmp(FunctionCallInfo fcinfo);
Datum BtreeInt42Cmp(FunctionCallInfo fcinfo);
Datum BtreeInt28Cmp(FunctionCallInfo fcinfo);
Datum BtreeInt82Cmp(FunctionCallInfo fcinfo);
Datum BtreeFloat48Cmp(FunctionCallInfo fcinfo);
Datum BtreeFloat84Cmp(FunctionCallInfo fcinfo);
Datum BtreeDateTimestampCmp(FunctionCallInfo fcinfo);
Datum BtreeTimestampDateCmp(FunctionCallInfo fcinfo);

Datum BtreeUUIDCmp(FunctionCallInfo fcinfo);

/* fast cmp */
int Int2FastCmp(Datum x, Datum y, __attribute__((unused)) SortSupport ssup);
int Int4FastCmp(Datum x, Datum y, __attribute__((unused)) SortSupport ssup);
int Int8FastCmp(Datum x, Datum y, __attribute__((unused)) SortSupport ssup);
int Float4FastCmp(Datum x, Datum y, __attribute__((unused)) SortSupport ssup);
int Float8FastCmp(Datum x, Datum y, __attribute__((unused)) SortSupport ssup);
int OidFastCmp(Datum x, Datum y, __attribute__((unused)) SortSupport ssup);
int CharFastCmp(Datum x, Datum y, __attribute__((unused)) SortSupport ssup);
int TextFastCmp(Datum x, Datum y, __attribute__((unused)) SortSupport ssup);

/* sortsupport comparison func (for NAME C locale case) */
int NameFastCmp(Datum x, Datum y, __attribute__((unused)) SortSupport ssup);
int DateFastCmp(Datum x, Datum y, __attribute__((unused)) SortSupport ssup);
int TimestampFastCmp(Datum x, Datum y, __attribute__((unused)) SortSupport ssup);

/* sort support */
Datum Int2SortSupport(FunctionCallInfo info);
Datum Int4SortSupport(FunctionCallInfo info);
Datum Int8SortSupport(FunctionCallInfo info);
Datum Float4SortSupport(FunctionCallInfo info);
Datum Float8SortSupport(FunctionCallInfo info);
Datum OidSortSupport(FunctionCallInfo info);
Datum NameSortSupport(FunctionCallInfo info);
Datum DateSortSupport(FunctionCallInfo info);
Datum TimestampSortSupport(FunctionCallInfo info);

/* comparator for btree search */
Datum Int2Eq(FunctionCallInfo fcinfo);
Datum Int2Lt(FunctionCallInfo fcinfo);
Datum Int2Le(FunctionCallInfo fcinfo);
Datum Int2Gt(FunctionCallInfo fcinfo);
Datum Int2Ge(FunctionCallInfo fcinfo);
Datum Int4Eq(FunctionCallInfo fcinfo);
Datum Int4Lt(FunctionCallInfo fcinfo);
Datum Int4Le(FunctionCallInfo fcinfo);
Datum Int4Gt(FunctionCallInfo fcinfo);
Datum Int4Ge(FunctionCallInfo fcinfo);
Datum Int8Eq(FunctionCallInfo fcinfo);
Datum Int8Lt(FunctionCallInfo fcinfo);
Datum Int8Le(FunctionCallInfo fcinfo);
Datum Int8Gt(FunctionCallInfo fcinfo);
Datum Int8Ge(FunctionCallInfo fcinfo);
Datum Float4Eq(FunctionCallInfo fcinfo);
Datum Float4Lt(FunctionCallInfo fcinfo);
Datum Float4Le(FunctionCallInfo fcinfo);
Datum Float4Gt(FunctionCallInfo fcinfo);
Datum Float4Ge(FunctionCallInfo fcinfo);
Datum Float8Eq(FunctionCallInfo fcinfo);
Datum Float8Lt(FunctionCallInfo fcinfo);
Datum Float8Le(FunctionCallInfo fcinfo);
Datum Float8Gt(FunctionCallInfo fcinfo);
Datum Float8Ge(FunctionCallInfo fcinfo);
Datum TextEq(FunctionCallInfo fcinfo);
Datum TextLt(FunctionCallInfo fcinfo);
Datum TextLe(FunctionCallInfo fcinfo);
Datum TextGt(FunctionCallInfo fcinfo);
Datum TextGe(FunctionCallInfo fcinfo);
Datum BoolEq(FunctionCallInfo fcinfo);
Datum CharEq(FunctionCallInfo fcinfo);
Datum NameEq(FunctionCallInfo fcinfo);
Datum OidEq(FunctionCallInfo fcinfo);

}  // namespace DSTORE

#endif  // DSTORE_STORAGE_COMPARE_UTILS_H
