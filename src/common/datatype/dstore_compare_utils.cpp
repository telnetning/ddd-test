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
 * dstore_compare_utils.cpp
 *
 * IDENTIFICATION
 *        dstore/src/common/datatype/dstore_compare_utils.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include <cfloat>
#include <cmath>

#include "common/datatype/dstore_compare_utils.h"
#include "catalog/dstore_function.h"
#include "common/datatype/dstore_array_utils.h"
#include "common/datatype/dstore_inet_utils.h"
#include "common/datatype/dstore_varlena_utils.h"

namespace DSTORE {

/*
 * float32/float64{eq,ne,lt,le,gt,ge}		- float32/float64 comparison operations
 */
template<typename floatType>
static int FloatCmpInternal(floatType a, floatType b)
{
    /*
     * We consider all NANs to be equal and larger than any non-NAN. This is
     * somewhat arbitrary; the important thing is to have a consistent sort
     * order.
     */
    if (std::isnan(a)) {
        if (std::isnan(b)) {
            return 0; /* NAN = NAN */
        } else {
            return 1; /* NAN > non-NAN */
        }
    } else if (std::isnan(b)) {
        return -1;    /* non-NAN < NAN */
    } else {
        if (a > b) {
            return 1;
        } else if (a < b) {
            return -1;
        } else {
            return 0;
        }
    }
}

template<typename lhsType, typename rhsType>
static int IntCmpInternal(lhsType a, rhsType b)
{
    if (a > b) {
        return 1;
    } else if (a < b) {
        return -1;
    } else {
        return 0;
    }
}

Datum BtreeBoolCmp(FunctionCallInfo fcinfo)
{
    int a = DatumGetBool(fcinfo->arg[0]) ? 1 : 0;
    int b = DatumGetBool(fcinfo->arg[1]) ? 1 : 0;
    return Int32GetDatum((IntCmpInternal<int, int>(a, b)));
}

Datum BtreeInt2Cmp(FunctionCallInfo fcinfo)
{
    int16 a = DatumGetInt16(fcinfo->arg[0]);
    int16 b = DatumGetInt16(fcinfo->arg[1]);
    return Int32GetDatum((IntCmpInternal<int16, int16>(a, b)));
}

Datum BtreeInt4Cmp(FunctionCallInfo fcinfo)
{
    int32 a = DatumGetInt32(fcinfo->arg[0]);
    int32 b = DatumGetInt32(fcinfo->arg[1]);
    return Int32GetDatum((IntCmpInternal<int32, int32>(a, b)));
}

Datum BtreeInt8Cmp(FunctionCallInfo fcinfo)
{
    int64 a = DatumGetInt64(fcinfo->arg[0]);
    int64 b = DatumGetInt64(fcinfo->arg[1]);
    return Int32GetDatum((IntCmpInternal<int64, int64>(a, b)));
}

Datum BtreeInt24Cmp(FunctionCallInfo fcinfo)
{
    int16 a = DatumGetInt16(fcinfo->arg[0]);
    int32 b = DatumGetInt32(fcinfo->arg[1]);
    return Int32GetDatum((IntCmpInternal<int16, int32>(a, b)));
}

Datum BtreeInt42Cmp(FunctionCallInfo fcinfo)
{
    int32 a = DatumGetInt32(fcinfo->arg[0]);
    int16 b = DatumGetInt16(fcinfo->arg[1]);
    return Int32GetDatum((IntCmpInternal<int32, int16>(a, b)));
}

Datum BtreeInt48Cmp(FunctionCallInfo fcinfo)
{
    int32 a = DatumGetInt32(fcinfo->arg[0]);
    int64 b = DatumGetInt64(fcinfo->arg[1]);
    return Int32GetDatum((IntCmpInternal<int32, int64>(a, b)));
}

Datum BtreeInt84Cmp(FunctionCallInfo fcinfo)
{
    int64 a = DatumGetInt64(fcinfo->arg[0]);
    int32 b = DatumGetInt32(fcinfo->arg[1]);
    return Int32GetDatum((IntCmpInternal<int64, int64>(a, b)));
}

Datum BtreeInt28Cmp(FunctionCallInfo fcinfo)
{
    int16 a = DatumGetInt16(fcinfo->arg[0]);
    int64 b = DatumGetInt64(fcinfo->arg[1]);
    return Int32GetDatum((IntCmpInternal<int16, int64>(a, b)));
}

Datum BtreeInt82Cmp(FunctionCallInfo fcinfo)
{
    int64 a = DatumGetInt64(fcinfo->arg[0]);
    int16 b = DatumGetInt16(fcinfo->arg[1]);
    return Int32GetDatum((IntCmpInternal<int64, int16>(a, b)));
}

Datum BtreeCharCmp(FunctionCallInfo fcinfo)
{
    char a = DatumGetChar(fcinfo->arg[0]);
    char b = DatumGetChar(fcinfo->arg[1]);
    /* Be careful to compare chars as unsigned */
    return Int32GetDatum((IntCmpInternal<char, char>(a, b)));
}

Datum BtreeOidCmp(FunctionCallInfo fcinfo)
{
    Oid a = DatumGetObjectId(fcinfo->arg[0]);
    Oid b = DatumGetObjectId(fcinfo->arg[1]);
    return Int32GetDatum((IntCmpInternal<Oid, Oid>(a, b)));
}

Datum BtreeOidvectorCmp(FunctionCallInfo fcinfo)
{
    OidVector *a = static_cast<OidVector*>(static_cast<void*>(DatumGetPointer(fcinfo->arg[0])));
    OidVector *b = static_cast<OidVector*>(static_cast<void*>(DatumGetPointer(fcinfo->arg[1])));
    if (STORAGE_VAR_NULL(a) || STORAGE_VAR_NULL(b)) {
        fcinfo->isnull = true;
        return Int32GetDatum(-1);
    }

    /* We arbitrarily choose to sort first by vector length */
    if (a->dim1 != b->dim1) {
        return Int32GetDatum(a->dim1 - b->dim1);
    }

    for (int i = 0; i < a->dim1; i++) {
        if (a->values[i] != b->values[i]) {
            if (a->values[i] > b->values[i]) {
                return Int32GetDatum(1);
            } else {
                return Int32GetDatum(-1);
            }
        }
    }
    return Int32GetDatum((IntCmpInternal<int, int>(0, 0)));
}

Datum BtreeInt2vectorCmp(FunctionCallInfo fcinfo)
{
    int2vector *a = static_cast<int2vector*>(static_cast<void*>(DatumGetPointer(fcinfo->arg[0])));
    int2vector *b = static_cast<int2vector*>(static_cast<void*>(DatumGetPointer(fcinfo->arg[1])));
    if (STORAGE_VAR_NULL(a) || STORAGE_VAR_NULL(b)) {
        fcinfo->isnull = true;
        return Int32GetDatum(-1);
    }

    /* We arbitrarily choose to sort first by vector length */
    if (a->dim1 != b->dim1) {
        return Int32GetDatum(a->dim1 - b->dim1);
    }

    for (int i = 0; i < a->dim1; i++) {
        if (a->values[i] != b->values[i]) {
            if (a->values[i] < b->values[i]) {
                return Int32GetDatum(-1);
            } else {
                return Int32GetDatum(1);
            }
        }
    }
    return Int32GetDatum((IntCmpInternal<int, int>(0, 0)));
}

Datum BtreeNameCmp(FunctionCallInfo fcinfo)
{
    DstoreName a = static_cast<DstoreName>(static_cast<void*>(DatumGetPointer(fcinfo->arg[0])));
    DstoreName b = static_cast<DstoreName>(static_cast<void*>(DatumGetPointer(fcinfo->arg[1])));
    if (STORAGE_VAR_NULL(a) || STORAGE_VAR_NULL(b)) {
        fcinfo->isnull = true;
        return Int32GetDatum(-1);
    }
    return Int32GetDatum(strncmp(a->data, b->data, NAME_DATA_LEN));
}

Datum BtreeFloat4Cmp(FunctionCallInfo fcinfo)
{
    float32 a = DatumGetFloat32(fcinfo->arg[0]);
    float32 b = DatumGetFloat32(fcinfo->arg[1]);
    return Int32GetDatum(FloatCmpInternal<float32>(a, b));
}

Datum BtreeFloat8Cmp(FunctionCallInfo fcinfo)
{
    float64 a = DatumGetFloat64(fcinfo->arg[0]);
    float64 b = DatumGetFloat64(fcinfo->arg[1]);
    return Int32GetDatum(FloatCmpInternal<float64>(a, b));
}

Datum BtreeTextCmp(FunctionCallInfo fcinfo)
{
    text *arg1 = DatumGetText(fcinfo->arg[0]);
    text *arg2 = DatumGetText(fcinfo->arg[1]);
    if (STORAGE_VAR_NULL(arg1) || STORAGE_VAR_NULL(arg2)) {
        fcinfo->isnull = true;
        return Int32GetDatum(-1);
    }
    int32 result = TextCmp(arg1, arg2, fcinfo->fncollation);
    return Int32GetDatum(result);
}

Datum BtreeCashCmp(FunctionCallInfo fcinfo)
{
    Cash a = DatumGetCash(fcinfo->arg[0]);
    Cash b = DatumGetCash(fcinfo->arg[1]);
    return Int32GetDatum((IntCmpInternal<Cash, Cash>(a, b)));
}

Datum BtreeDateCmp(FunctionCallInfo fcinfo)
{
    DateADT a = DstoreDatumGetDateADT(fcinfo->arg[0]);
    DateADT b = DstoreDatumGetDateADT(fcinfo->arg[1]);
    return Int32GetDatum((IntCmpInternal<DateADT, DateADT>(a, b)));
}

Datum BtreeTimeCmp(FunctionCallInfo fcinfo)
{
    TimeADT a = DstoreDatumGetTimeADT(fcinfo->arg[0]);
    TimeADT b = DstoreDatumGetTimeADT(fcinfo->arg[1]);
    return Int32GetDatum((IntCmpInternal<TimeADT, TimeADT>(a, b)));
}

Datum BtreeTimestamptzCmp(FunctionCallInfo fcinfo)
{
    TimestampTz a = DstoreDatumGetTimestampTz(fcinfo->arg[0]);
    TimestampTz b = DstoreDatumGetTimestampTz(fcinfo->arg[1]);
    return Int32GetDatum((IntCmpInternal<TimestampTz, TimestampTz>(a, b)));
}

static inline TimeOffset IntervalCmpValue(const Interval* interval)
{
    TimeOffset span = interval->time;

#ifdef HAVE_INT64_TIMESTAMP
    span += interval->month * static_cast<int64>(STORAGE_DAYS_PER_MONTH) * STORAGE_USECS_PER_DAY;
    span += interval->day * static_cast<int64>(STORAGE_HOURS_PER_DAY) * STORAGE_USECS_PER_HOUR;
#else
    span += interval->month * (static_cast<int64>(STORAGE_DAYS_PER_MONTH) * STORAGE_SECS_PER_DAY);
    span += interval->day * (static_cast<int64>(STORAGE_HOURS_PER_DAY) * STORAGE_SECS_PER_HOUR);
#endif

    return span;
}

Datum BtreeIntervalCmp(FunctionCallInfo fcinfo)
{
    Interval *a = DatumGetIntervalP(fcinfo->arg[0]);
    Interval *b = DatumGetIntervalP(fcinfo->arg[1]);
    if (STORAGE_VAR_NULL(a) || STORAGE_VAR_NULL(b)) {
        fcinfo->isnull = true;
        return Int32GetDatum(-1);
    }

    TimeOffset span1 = IntervalCmpValue(a);
    TimeOffset span2 = IntervalCmpValue(b);

    return Int32GetDatum((IntCmpInternal<TimeOffset, TimeOffset>(span1, span2)));
}

Datum BtreeTimetzCmp(__attribute__((unused)) FunctionCallInfo fcinfo)
{
    TimeTzADT *time1 = DatumGetTimeTzADTP(fcinfo->arg[0]);
    TimeTzADT *time2 = DatumGetTimeTzADTP(fcinfo->arg[1]);
    if (STORAGE_VAR_NULL(time1) || STORAGE_VAR_NULL(time2)) {
        fcinfo->isnull = true;
        return Int32GetDatum(-1);
    }

    TimeOffset t1;
    TimeOffset t2;
/* Primary sort is by true (GMT-equivalent) time */
#ifdef HAVE_INT64_TIMESTAMP
    t1 = time1->time + (static_cast<int64>(time1->zone) * static_cast<int64>(STORAGE_USECS_PER_SEC));
    t2 = time2->time + (static_cast<int64>(time2->zone) * static_cast<int64>(STORAGE_USECS_PER_SEC));
#else
    t1 = time1->time + time1->zone;
    t2 = time2->time + time2->zone;
#endif

    if (t1 > t2) {
        return 1;
    }
    if (t1 < t2) {
        return -1;
    }

    /*
     * If same GMT time, sort by timezone; we only want to say that two
     * timetz's are equal if both the time and zone parts are equal.
     */
    if (time1->zone > time2->zone) {
        return 1;
    }
    if (time1->zone < time2->zone) {
        return -1;
    }

    return 0;
}

Datum BtreeMacaddrCmp(FunctionCallInfo fcinfo)
{
    MacAddr *a = DstoreDatumGetMacaddr(fcinfo->arg[0]);
    MacAddr *b = DstoreDatumGetMacaddr(fcinfo->arg[1]);
    if (STORAGE_VAR_NULL(a) || STORAGE_VAR_NULL(b)) {
        fcinfo->isnull = true;
        return Int32GetDatum(-1);
    }
    return Int32GetDatum(MacAddrCmp(a, b));
}

Datum BtreeInetCmp(FunctionCallInfo fcinfo)
{
    Inet *a = DstoreDatumGetInet(fcinfo->arg[0]);
    Inet *b = DstoreDatumGetInet(fcinfo->arg[1]);
    if (STORAGE_VAR_NULL(a) || STORAGE_VAR_NULL(b)) {
        fcinfo->isnull = true;
        return Int32GetDatum(-1);
    }
    return Int32GetDatum(NetworkCmp(a, b));
}

Datum BtreeTimeStampCmp(FunctionCallInfo fcinfo)
{
    Timestamp a = DstoreDatumGetTimestamp(fcinfo->arg[0]);
    Timestamp b = DstoreDatumGetTimestamp(fcinfo->arg[1]);
    return Int32GetDatum((IntCmpInternal<Timestamp, Timestamp>(a, b)));
}

Datum BtreeDateaCmp(FunctionCallInfo fcinfo)
{
    Datea a = DstoreDatumGetDatea(fcinfo->arg[0]);
    Datea b = DstoreDatumGetDatea(fcinfo->arg[1]);
    return Int32GetDatum((IntCmpInternal<Datea, Datea>(a, b)));
}

Datum BtreeTextPatternCmp(FunctionCallInfo fcinfo)
{
    return BtreeTextCmp(fcinfo);
}

Datum BtreeFloat48Cmp(FunctionCallInfo fcinfo)
{
    float32 a = DatumGetFloat32(fcinfo->arg[0]);
    float64 b = DatumGetFloat64(fcinfo->arg[1]);

    /* widen float4 to float8 and then compare */
    return Int32GetDatum(FloatCmpInternal<float64>(a, b));
}

Datum BtreeFloat84Cmp(FunctionCallInfo fcinfo)
{
    float64 a = DatumGetFloat64(fcinfo->arg[0]);
    float32 b = DatumGetFloat32(fcinfo->arg[1]);

    /* widen float4 to float8 and then compare */
    return Int32GetDatum(FloatCmpInternal<float64>(a, b));
}

/*
 * Infinity and minus infinity must be the max and min values of DateADT.
 * We could use INT_MIN and INT_MAX here, but seems better to not assume that
 * int32 == int.
 */
static const DateADT DATEVAL_NOBEGIN = static_cast<DateADT>(-0x7fffffff - 1);
static const DateADT DATEVAL_NOEND = static_cast<DateADT>(0x7fffffff);
static const Timestamp TIMESTAMPVAL_NOBEGIN = static_cast<Timestamp>(-HUGE_VAL);
static const Timestamp TIMESTAMPVAL_NOEND = static_cast<Timestamp>(HUGE_VAL);
static inline Timestamp Date2Timestamp(DateADT dateVal)
{
    Timestamp result;

    if (dateVal == DATEVAL_NOBEGIN) {
        result = TIMESTAMPVAL_NOBEGIN;
    } else if (dateVal == DATEVAL_NOEND) {
        result = TIMESTAMPVAL_NOEND;
    } else {
#ifdef HAVE_INT64_TIMESTAMP
        /* date is days since 2000, timestamp is microseconds since same... */
        result = dateVal * STORAGE_USECS_PER_DAY;
        /* Date's range is wider than timestamp's, so check for overflow */
        if (result / STORAGE_USECS_PER_DAY != dateVal) {
            ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("date out of range for timestamp"));
        }
#else
        /* date is days since 2000, timestamp is seconds since same... */
        result = static_cast<Timestamp>(dateVal * static_cast<int64>(STORAGE_SECS_PER_DAY));
#endif
    }

    return result;
}

Datum BtreeDateTimestampCmp(FunctionCallInfo fcinfo)
{
    DateADT dateVal = DstoreDatumGetDateADT(fcinfo->arg[0]);
    Timestamp dt2 = DstoreDatumGetTimestamp(fcinfo->arg[1]);
    Timestamp dt1 = Date2Timestamp(dateVal);

#ifdef HAVE_INT64_TIMESTAMP
    return Int32GetDatum(IntCmpInternal<Timestamp, Timestamp>(dt1, dt2));
#else
    return Int32GetDatum(FloatCmpInternal<Timestamp>(dt1, dt2));
#endif
}

Datum BtreeTimestampDateCmp(FunctionCallInfo fcinfo)
{
    Timestamp dt1 = DstoreDatumGetTimestamp(fcinfo->arg[0]);
    DateADT dateVal = DstoreDatumGetDateADT(fcinfo->arg[1]);
    Timestamp dt2 = Date2Timestamp(dateVal);

#ifdef HAVE_INT64_TIMESTAMP
    return Int32GetDatum(IntCmpInternal<Timestamp, Timestamp>(dt1, dt2));
#else
    return Int32GetDatum(FloatCmpInternal<Timestamp>(dt1, dt2));
#endif
}


Datum BtreeUUIDCmp(FunctionCallInfo fcinfo)
{
    static const int UUID_LEN = 16;
    char *a = DatumGetPointer(fcinfo->arg[0]);
    char *b = DatumGetPointer(fcinfo->arg[1]);
    if (STORAGE_VAR_NULL(a) || STORAGE_VAR_NULL(b)) {
        fcinfo->isnull = true;
        return Int32GetDatum(-1);
    }

    return Int32GetDatum(memcmp(a, b, UUID_LEN));
}

/*
 * fast comparator
 */
int Int2FastCmp(Datum x, Datum y, __attribute__((unused)) SortSupport ssup)
{
    int16 a = DatumGetInt16(x);
    int16 b = DatumGetInt16(y);
    return a > b ? 1 : a == b ? 0 : -1;
}

int Int4FastCmp(Datum x, Datum y, __attribute__((unused)) SortSupport ssup)
{
    int32 a = DatumGetInt32(x);
    int32 b = DatumGetInt32(y);
    return a > b ? 1 : a == b ? 0 : -1;
}

int Int8FastCmp(Datum x, Datum y, __attribute__((unused)) SortSupport ssup)
{
    int64 a = DatumGetInt64(x);
    int64 b = DatumGetInt64(y);
    return a > b ? 1 : a == b ? 0 : -1;
}

int Float4FastCmp(Datum x, Datum y, __attribute__((unused)) SortSupport ssup)
{
    float32 a = DatumGetFloat32(x);
    float32 b = DatumGetFloat32(y);

    return FloatCmpInternal<float32>(a, b);
}

int Float8FastCmp(Datum x, Datum y, __attribute__((unused)) SortSupport ssup)
{
    float64 a = DatumGetFloat64(x);
    float64 b = DatumGetFloat64(y);

    return FloatCmpInternal<float64>(a, b);
}

int OidFastCmp(Datum x, Datum y, __attribute__((unused)) SortSupport ssup)
{
    Oid a = DatumGetObjectId(x);
    Oid b = DatumGetObjectId(y);
    return a > b ? 1 : a == b ? 0 : -1;
}

int CharFastCmp(Datum x, Datum y, __attribute__((unused)) SortSupport ssup)
{
    char a = DatumGetChar(x);
    char b = DatumGetChar(y);
    return a > b ? 1 : a == b ? 0 : -1;
}

int TextFastCmp(Datum x, Datum y, __attribute__((unused)) SortSupport ssup)
{
    text* a = DatumGetText(x);
    text* b = DatumGetText(y);
    return TextCmp(a, b, ssup->ssupCollation);
}

/*
* sortsupport comparison func (for NAME C locale case)
*/
int NameFastCmp(Datum x, Datum y, __attribute__((unused)) SortSupport ssup)
{
    DstoreName arg1 = static_cast<DstoreName>(static_cast<void*>(DatumGetPointer(x)));
    DstoreName arg2 = static_cast<DstoreName>(static_cast<void*>(DatumGetPointer(y)));
    int ret = strncmp((*arg1).data, (*arg2).data, NAME_DATA_LEN);
    return ret > 0 ? 1 : ret == 0 ? 0 : -1;
}

int DateFastCmp(Datum x, Datum y, __attribute__((unused)) SortSupport ssup)
{
    DateADT a = DstoreDatumGetDateADT(x);
    DateADT b = DstoreDatumGetDateADT(y);
    return a > b ? 1 : a == b ? 0 : -1;
}

int TimestampFastCmp(Datum x, Datum y, __attribute__((unused)) SortSupport ssup)
{
    Timestamp a = DstoreDatumGetTimestamp(x);
    Timestamp b = DstoreDatumGetTimestamp(y);
    return a > b ? 1 : a == b ? 0 : -1;
}

/*
 * sortsupport func
 */
Datum Int2SortSupport(FunctionCallInfo info)
{
    SortSupport ssup = static_cast<SortSupport>(static_cast<void*>(DatumGetPointer(info->arg[0])));
    ssup->comparator = Int2FastCmp;
    return static_cast<Datum>(0);
}

Datum Int4SortSupport(FunctionCallInfo info)
{
    SortSupport ssup = static_cast<SortSupport>(static_cast<void*>(DatumGetPointer(info->arg[0])));
    ssup->comparator = Int4FastCmp;
    return static_cast<Datum>(0);
}

Datum Int8SortSupport(FunctionCallInfo info)
{
    SortSupport ssup = static_cast<SortSupport>(static_cast<void*>(DatumGetPointer(info->arg[0])));
    ssup->comparator = Int8FastCmp;
    return static_cast<Datum>(0);
}

Datum Float4SortSupport(FunctionCallInfo info)
{
    SortSupport ssup = static_cast<SortSupport>(static_cast<void*>(DatumGetPointer(info->arg[0])));
    ssup->comparator = Float4FastCmp;
    return static_cast<Datum>(0);
}

Datum Float8SortSupport(FunctionCallInfo info)
{
    SortSupport ssup = static_cast<SortSupport>(static_cast<void*>(DatumGetPointer(info->arg[0])));
    ssup->comparator = Float8FastCmp;
    return static_cast<Datum>(0);
}

Datum OidSortSupport(FunctionCallInfo info)
{
    SortSupport ssup = static_cast<SortSupport>(static_cast<void*>(DatumGetPointer(info->arg[0])));
    ssup->comparator = OidFastCmp;
    return static_cast<Datum>(0);
}

Datum NameSortSupport(FunctionCallInfo info)
{
    SortSupport ssup = static_cast<SortSupport>(static_cast<void*>(DatumGetPointer(info->arg[0])));
    ssup->comparator = NameFastCmp;
    return static_cast<Datum>(0);
}

Datum DateSortSupport(FunctionCallInfo info)
{
    SortSupport ssup = static_cast<SortSupport>(static_cast<void*>(DatumGetPointer(info->arg[0])));
    ssup->comparator = DateFastCmp;
    return static_cast<Datum>(0);
}

Datum TimestampSortSupport(FunctionCallInfo info)
{
    SortSupport ssup = static_cast<SortSupport>(static_cast<void*>(DatumGetPointer(info->arg[0])));
    ssup->comparator = TimestampFastCmp;
    return static_cast<Datum>(0);
}

/* comparator for btree search */
Datum Int2Eq(FunctionCallInfo fcinfo)
{
    int16 arg1 = DatumGetInt16(fcinfo->arg[0]);
    int16 arg2 = DatumGetInt16(fcinfo->arg[1]);
    return BoolGetDatum(arg1 == arg2);
}

Datum Int2Lt(FunctionCallInfo fcinfo)
{
    int16 arg1 = DatumGetInt16(fcinfo->arg[0]);
    int16 arg2 = DatumGetInt16(fcinfo->arg[1]);
    return BoolGetDatum(arg1 < arg2);
}

Datum Int2Le(FunctionCallInfo fcinfo)
{
    int16 arg1 = DatumGetInt16(fcinfo->arg[0]);
    int16 arg2 = DatumGetInt16(fcinfo->arg[1]);
    return BoolGetDatum(arg1 <= arg2);
}

Datum Int2Gt(FunctionCallInfo fcinfo)
{
    int16 arg1 = DatumGetInt16(fcinfo->arg[0]);
    int16 arg2 = DatumGetInt16(fcinfo->arg[1]);
    return BoolGetDatum(arg1 > arg2);
}

Datum Int2Ge(FunctionCallInfo fcinfo)
{
    int16 arg1 = DatumGetInt16(fcinfo->arg[0]);
    int16 arg2 = DatumGetInt16(fcinfo->arg[1]);
    return BoolGetDatum(arg1 >= arg2);
}

Datum Int4Eq(FunctionCallInfo fcinfo)
{
    int32 arg1 = DatumGetInt32(fcinfo->arg[0]);
    int32 arg2 = DatumGetInt32(fcinfo->arg[1]);
    return BoolGetDatum(arg1 == arg2);
}

Datum Int4Lt(FunctionCallInfo fcinfo)
{
    int32 arg1 = DatumGetInt32(fcinfo->arg[0]);
    int32 arg2 = DatumGetInt32(fcinfo->arg[1]);
    return BoolGetDatum(arg1 < arg2);
}

Datum Int4Le(FunctionCallInfo fcinfo)
{
    int32 arg1 = DatumGetInt32(fcinfo->arg[0]);
    int32 arg2 = DatumGetInt32(fcinfo->arg[1]);
    return BoolGetDatum(arg1 <= arg2);
}

Datum Int4Gt(FunctionCallInfo fcinfo)
{
    int32 arg1 = DatumGetInt32(fcinfo->arg[0]);
    int32 arg2 = DatumGetInt32(fcinfo->arg[1]);
    return BoolGetDatum(arg1 > arg2);
}

Datum Int4Ge(FunctionCallInfo fcinfo)
{
    int32 arg1 = DatumGetInt32(fcinfo->arg[0]);
    int32 arg2 = DatumGetInt32(fcinfo->arg[1]);
    return BoolGetDatum(arg1 >= arg2);
}

Datum Int8Eq(FunctionCallInfo fcinfo)
{
    int64 arg1 = DatumGetInt64(fcinfo->arg[0]);
    int64 arg2 = DatumGetInt64(fcinfo->arg[1]);
    return BoolGetDatum(arg1 == arg2);
}

Datum Int8Lt(FunctionCallInfo fcinfo)
{
    int64 arg1 = DatumGetInt64(fcinfo->arg[0]);
    int64 arg2 = DatumGetInt64(fcinfo->arg[1]);
    return BoolGetDatum(arg1 < arg2);
}

Datum Int8Le(FunctionCallInfo fcinfo)
{
    int64 arg1 = DatumGetInt64(fcinfo->arg[0]);
    int64 arg2 = DatumGetInt64(fcinfo->arg[1]);
    return BoolGetDatum(arg1 <= arg2);
}

Datum Int8Gt(FunctionCallInfo fcinfo)
{
    int64 arg1 = DatumGetInt64(fcinfo->arg[0]);
    int64 arg2 = DatumGetInt64(fcinfo->arg[1]);
    return BoolGetDatum(arg1 > arg2);
}

Datum Int8Ge(FunctionCallInfo fcinfo)
{
    int64 arg1 = DatumGetInt64(fcinfo->arg[0]);
    int64 arg2 = DatumGetInt64(fcinfo->arg[1]);
    return BoolGetDatum(arg1 >= arg2);
}

Datum Float4Eq(FunctionCallInfo fcinfo)
{
    float32 arg1 = DatumGetFloat32(fcinfo->arg[0]);
    float32 arg2 = DatumGetFloat32(fcinfo->arg[1]);
    return BoolGetDatum(fabs(arg1 - arg2) < FLT_EPSILON);
}

Datum Float4Lt(FunctionCallInfo fcinfo)
{
    float32 arg1 = DatumGetFloat32(fcinfo->arg[0]);
    float32 arg2 = DatumGetFloat32(fcinfo->arg[1]);
    return BoolGetDatum(arg1 < arg2);
}

Datum Float4Le(FunctionCallInfo fcinfo)
{
    float32 arg1 = DatumGetFloat32(fcinfo->arg[0]);
    float32 arg2 = DatumGetFloat32(fcinfo->arg[1]);
    return BoolGetDatum(arg1 <= arg2);
}

Datum Float4Gt(FunctionCallInfo fcinfo)
{
    float32 arg1 = DatumGetFloat32(fcinfo->arg[0]);
    float32 arg2 = DatumGetFloat32(fcinfo->arg[1]);
    return BoolGetDatum(arg1 > arg2);
}

Datum Float4Ge(FunctionCallInfo fcinfo)
{
    float32 arg1 = DatumGetFloat32(fcinfo->arg[0]);
    float32 arg2 = DatumGetFloat32(fcinfo->arg[1]);
    return BoolGetDatum(arg1 >= arg2);
}

Datum Float8Eq(FunctionCallInfo fcinfo)
{
    float64 arg1 = DatumGetFloat64(fcinfo->arg[0]);
    float64 arg2 = DatumGetFloat64(fcinfo->arg[1]);
    return BoolGetDatum(fabs(arg1 - arg2) < DBL_EPSILON);
}

Datum Float8Lt(FunctionCallInfo fcinfo)
{
    float64 arg1 = DatumGetFloat64(fcinfo->arg[0]);
    float64 arg2 = DatumGetFloat64(fcinfo->arg[1]);
    return BoolGetDatum(arg1 < arg2);
}

Datum Float8Le(FunctionCallInfo fcinfo)
{
    float64 arg1 = DatumGetFloat64(fcinfo->arg[0]);
    float64 arg2 = DatumGetFloat64(fcinfo->arg[1]);
    return BoolGetDatum(arg1 <= arg2);
}

Datum Float8Gt(FunctionCallInfo fcinfo)
{
    float64 arg1 = DatumGetFloat64(fcinfo->arg[0]);
    float64 arg2 = DatumGetFloat64(fcinfo->arg[1]);
    return BoolGetDatum(arg1 > arg2);
}

Datum Float8Ge(FunctionCallInfo fcinfo)
{
    float64 arg1 = DatumGetFloat64(fcinfo->arg[0]);
    float64 arg2 = DatumGetFloat64(fcinfo->arg[1]);
    return BoolGetDatum(arg1 >= arg2);
}

Datum TextEq(FunctionCallInfo fcinfo)
{
    text *arg1 = static_cast<text*>(static_cast<void*>(DatumGetPointer(fcinfo->arg[0])));
    text *arg2 = static_cast<text*>(static_cast<void*>(DatumGetPointer(fcinfo->arg[1])));
    return BoolGetDatum(TextCmp(arg1, arg2, fcinfo->fncollation) == 0);
}

Datum TextLt(FunctionCallInfo fcinfo)
{
    text *arg1 = static_cast<text*>(static_cast<void*>(DatumGetPointer(fcinfo->arg[0])));
    text *arg2 = static_cast<text*>(static_cast<void*>(DatumGetPointer(fcinfo->arg[1])));
    return BoolGetDatum(TextCmp(arg1, arg2, fcinfo->fncollation) < 0);
}

Datum TextLe(FunctionCallInfo fcinfo)
{
    text *arg1 = static_cast<text*>(static_cast<void*>(DatumGetPointer(fcinfo->arg[0])));
    text *arg2 = static_cast<text*>(static_cast<void*>(DatumGetPointer(fcinfo->arg[1])));
    return BoolGetDatum(TextCmp(arg1, arg2, fcinfo->fncollation) <= 0);
}

Datum TextGt(FunctionCallInfo fcinfo)
{
    text *arg1 = static_cast<text*>(static_cast<void*>(DatumGetPointer(fcinfo->arg[0])));
    text *arg2 = static_cast<text*>(static_cast<void*>(DatumGetPointer(fcinfo->arg[1])));
    return BoolGetDatum(TextCmp(arg1, arg2, fcinfo->fncollation) > 0);
}

Datum TextGe(FunctionCallInfo fcinfo)
{
    text *arg1 = static_cast<text*>(static_cast<void*>(DatumGetPointer(fcinfo->arg[0])));
    text *arg2 = static_cast<text*>(static_cast<void*>(DatumGetPointer(fcinfo->arg[1])));
    return BoolGetDatum(TextCmp(arg1, arg2, fcinfo->fncollation) >= 0);
}

Datum BoolEq(FunctionCallInfo fcinfo)
{
    bool arg1 = DatumGetBool(fcinfo->arg[0]);
    bool arg2 = DatumGetBool(fcinfo->arg[1]);
    return BoolGetDatum(arg1 == arg2);
}

Datum CharEq(FunctionCallInfo fcinfo)
{
    char arg1 = DatumGetChar(fcinfo->arg[0]);
    char arg2 = DatumGetChar(fcinfo->arg[1]);

    return BoolGetDatum(arg1 == arg2);
}

static int NameCmp(DstoreName arg1, DstoreName arg2, Oid collid)
{
	/* Fast path for common case used in system catalogs */
    if (collid == C_COLLATION_OID) {
        return strncmp((*arg1).data, (*arg2).data, NAME_DATA_LEN);
    }

    /* Else rely on the varstr infrastructure */
    return VarstrCmp((*arg1).data, (int)strlen((*arg1).data),
					  (*arg2).data, (int)strlen((*arg2).data),
                      collid);
}

Datum NameEq(FunctionCallInfo fcinfo)
{
    DstoreName arg1 = static_cast<DstoreName>(static_cast<void *>(DatumGetPointer(fcinfo->arg[0])));
    DstoreName arg2 = static_cast<DstoreName>(static_cast<void *>(DatumGetPointer(fcinfo->arg[1])));
    return BoolGetDatum(NameCmp(arg1, arg2, fcinfo->fncollation) == 0);
}

Datum OidEq(FunctionCallInfo fcinfo)
{
    Oid arg1 = DatumGetObjectId(fcinfo->arg[0]);
    Oid arg2 = DatumGetObjectId(fcinfo->arg[1]);
    return BoolGetDatum(arg1 == arg2);
}

}
