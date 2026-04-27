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
 * dstore_output_utils.cpp
 *
 * ---------------------------------------------------------------------------------------
 *
 */
#include <cfloat>
#include <cmath>
#include "common/datatype/dstore_varlena_utils.h"
#include "common/datatype/dstore_output_utils.h"

namespace DSTORE {

/* all function here will be removed after inject callback in sql-engine */
Datum BoolOut(FunctionCallInfo fcinfo)
{
    bool arg = DatumGetBool(fcinfo->arg[0]);
    char* result = static_cast<char*>(DstorePalloc0(2));
    if (STORAGE_VAR_NULL(result)) {
        return PointerGetDatum(nullptr);
    }
    result[0] = (arg) ? 't' : 'f';
    result[1] = '\0';
    return CStringGetDatum(result);
}

Datum Int2Out(FunctionCallInfo fcinfo)
{
    int16 arg = DatumGetInt16(fcinfo->arg[0]);
    char* result = static_cast<char*>(DstorePalloc0(MAX_INT_LEN + 1)); /* sign, 5 digits, '\0' */
    if (STORAGE_VAR_NULL(result)) {
        return PointerGetDatum(nullptr);
    }
    GsItoa(static_cast<int32>(arg), result);
    return CStringGetDatum(result);
}

Datum Int4Out(FunctionCallInfo fcinfo)
{
    int32 arg = DatumGetInt32(fcinfo->arg[0]);
    char* result = static_cast<char*>(DstorePalloc0(MAX_INT_LEN + 1)); /* sign, 10 digits, '\0' */
    if (STORAGE_VAR_NULL(result)) {
        return PointerGetDatum(nullptr);
    }
    GsItoa(arg, result);
    return CStringGetDatum(result);
}

Datum Int8Out(FunctionCallInfo fcinfo)
{
    int64 arg = DatumGetInt64(fcinfo->arg[0]);
    char* result = static_cast<char*>(DstorePalloc0(MAX_INT8_LEN + 1));
    if (STORAGE_VAR_NULL(result)) {
        return PointerGetDatum(nullptr);
    }
    errno_t rc = snprintf_s(result, MAX_INT8_LEN + 1, MAX_INT8_LEN, "%lld", arg);
    storage_securec_check_ss(rc);
    return CStringGetDatum(result);
}

static int IsInfinite(double val)
{
    int inf = std::isinf(val);
    if (inf == 0) {
        return 0;
    } else if (val > 0) {
        return 1;
    } else {
        return -1;
    }
}

Datum Float4Out(FunctionCallInfo fcinfo)
{
    float32 arg = DatumGetFloat32(fcinfo->arg[0]);
    char* result = static_cast<char*>(DstorePalloc0(MAX_FLOAT_WIDTH + 1));
    if (STORAGE_VAR_NULL(result)) {
        return PointerGetDatum(nullptr);
    }
    errno_t rc = EOK;
    if (std::isnan(arg)) {
        rc = strcpy_s(result, MAX_FLOAT_WIDTH + 1, "NaN");
        storage_securec_check(rc, "\0", "\0");
        return CStringGetDatum(result);
    }
    switch (IsInfinite(arg)) {
        case 1:
            rc = strcpy_s(result, MAX_FLOAT_WIDTH + 1, "Infinity");
            storage_securec_check(rc, "\0", "\0");
            break;
        case -1:
            rc = strcpy_s(result, MAX_FLOAT_WIDTH + 1, "-Infinity");
            storage_securec_check(rc, "\0", "\0");
            break;
        default: {
            int ndig = FLT_DIG + 1;
            if (ndig < 1) {
                ndig = 1;
            }
            rc = snprintf_s(result, MAX_FLOAT_WIDTH + 1, MAX_FLOAT_WIDTH, "%.*g", ndig, arg);
            storage_securec_check_ss(rc);
            break;
        }
    }

    return CStringGetDatum(result);
}

Datum Float8Out(FunctionCallInfo fcinfo)
{
    float64 arg = DatumGetFloat64(fcinfo->arg[0]);
    char* result = static_cast<char*>(DstorePalloc0(MAX_DOUBLE_WIDTH + 1));
    if (STORAGE_VAR_NULL(result)) {
        return PointerGetDatum(nullptr);
    }
    errno_t rc = EOK;
    if (std::isnan(arg)) {
        rc = strcpy_s(result, MAX_DOUBLE_WIDTH + 1, "NaN");
        storage_securec_check(rc, "\0", "\0");
        return CStringGetDatum(result);
    }
    switch (IsInfinite(arg)) {
        case 1:
            rc = strcpy_s(result, MAX_DOUBLE_WIDTH + 1, "Infinity");
            storage_securec_check(rc, "\0", "\0");
            break;
        case -1:
            rc = strcpy_s(result, MAX_DOUBLE_WIDTH + 1, "-Infinity");
            storage_securec_check(rc, "\0", "\0");
            break;
        default: {
            int ndig = DBL_DIG + 1;
            if (ndig < 1) {
                ndig = 1;
            }
            rc = snprintf_s(result, MAX_DOUBLE_WIDTH + 1, MAX_DOUBLE_WIDTH, "%.*g", ndig, arg);
            storage_securec_check_ss(rc);
            break;
        }
    }
    return CStringGetDatum(result);
}

Datum VarcharOut(FunctionCallInfo fcinfo)
{
    Datum arg = fcinfo->arg[0];
    Size varSize = DSTORE_VARSIZE_ANY_EXHDR(DatumGetPointer(arg));
    char *result = static_cast<char*>(DstorePalloc0(varSize + 1));
    if (STORAGE_VAR_NULL(result)) {
        return PointerGetDatum(nullptr);
    }
    errno_t rt = memcpy_s(result, varSize, VarDataAny(DatumGetPointer(arg)), varSize);
    storage_securec_check(rt, "\0", "\0");
    result[varSize] = '\0';
    return CStringGetDatum(result);
}

/* tmp work, will remove after func cache moved to dstore */
void GsItoa(int32 value, char *a)
{
    if (a == nullptr) {
        return;
    }
    char* start = a;
    bool neg = false;

    if (value < 0) {
        value = -value;
        neg = true;
    }

    do {
        int32 remainder;
        int32 oldval = value;
        value /= 10;
        remainder = oldval - value * 10;
        *a++ = remainder + '0';
    } while (value != 0);

    if (neg) {
        *a++ = '-';
    }
    *a-- = '\0';
    while (start < a) {
        char swap = *start;

        *start++ = *a;
        *a-- = swap;
    }
}

}