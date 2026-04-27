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
 * implement funcitons that can convert data in string form to various type during inserting core systable preset data.
 */

#include <cctype>
#include "securec.h"
#include "catalog/dstore_function.h"
#include "common/log/dstore_log.h"
#include "catalog/dstore_fmgr.h"

namespace DSTORE {
static constexpr int BASE = 10;

static unsigned long UtStrtol(const char *s)
{
    char *endptr = nullptr;
    return strtoul(s, &endptr, 10);  // base=10
}

Datum OidIn(FunctionCallInfo fcinfo)
{
    const char *s = STATIC_CAST_PTR_TYPE(DatumGetPointer(fcinfo->arg[0]), const char *);
    Oid cvt = static_cast<Oid>(UtStrtol(s));
    return ObjectIdGetDatum(cvt);
}

Datum NameIn(FunctionCallInfo fcinfo)
{
    DstoreName result;
    const char *s = STATIC_CAST_PTR_TYPE(DatumGetPointer(fcinfo->arg[0]), const char *);

    /* Truncate oversize input */
    size_t len = strlen(s);
    if (len >= NAME_DATA_LEN) {
        len = NAME_DATA_LEN;
    }

    /* We use palloc0 here to ensure result is zero-padded */
    result = static_cast<DstoreName>(DstorePalloc0(NAME_DATA_LEN));
    if (STORAGE_VAR_NULL(result)) {
        ErrLog(DSTORE_ERROR, MODULE_CATALOG, ErrMsg("Alloc memory when NameIn failed %d.", NAME_DATA_LEN));
        return PointerGetDatum(nullptr);
    }
    errno_t ssRc = strncpy_s(result->data, NAME_DATA_LEN, s, len);
    storage_securec_check(ssRc, "\0", "\0");

    return PointerGetDatum(result);
}

Datum PointerIn(FunctionCallInfo fcinfo)
{
    const char *s = STATIC_CAST_PTR_TYPE(DatumGetPointer(fcinfo->arg[0]), const char *);

    size_t len = strlen(s);
    /* We use palloc0 here to ensure result is zero-padded */
    char *result = static_cast<char*>(DstorePalloc0(len + 1));
    if (STORAGE_VAR_NULL(result)) {
        ErrLog(DSTORE_ERROR, MODULE_CATALOG, ErrMsg("Alloc memory when PointerIn failed %lu.", len + 1));
        return PointerGetDatum(nullptr);
    }
    errno_t ssRc = strncpy_s(result, len + 1, s, len);
    storage_securec_check(ssRc, "\0", "\0");

    return PointerGetDatum(result);
}

Datum VarcharIn(FunctionCallInfo fcinfo)
{
    char *s = STATIC_CAST_PTR_TYPE(DatumGetPointer(fcinfo->arg[0]), char *);
    uint len = *STATIC_CAST_PTR_TYPE(s, uint*);
    varlena *result = static_cast<varlena*>(DstorePalloc0(VARHDRSZ + len));
    if (STORAGE_VAR_NULL(result)) {
        ErrLog(DSTORE_ERROR, MODULE_CATALOG, ErrMsg("Alloc memory when VarcharIn failed %u.", VARHDRSZ + len));
        return PointerGetDatum(nullptr);
    }
    DstoreSetVarSize(result, VARHDRSZ + len);
    errno_t ssRc = memcpy_s(result->vl_dat, len, s + VARHDRSZ, len);
    storage_securec_check(ssRc, "\0", "\0");

    return PointerGetDatum(result);
}

Datum Int2In(FunctionCallInfo fcinfo)
{
    const char *s = STATIC_CAST_PTR_TYPE(DatumGetPointer(fcinfo->arg[0]), const char *);
    int16_t r = static_cast<int16_t>(UtStrtol(s));
    return Int16GetDatum(r);
}

static bool ParseBoolWithLen(const char *value, size_t len, bool *result)
{
    bool tmpRes = false;
    bool parseRes = false;
    switch (tolower(*value)) {
        case 't':
            if (strncmp(value, "true", len) == 0) {
                tmpRes = true;
                parseRes = true;
            }
            break;
        case 'f':
            if (strncmp(value, "false", len) == 0) {
                tmpRes = false;
                parseRes = true;
            }
            break;
        case 'y':
            if (strncmp(value, "yes", len) == 0) {
                tmpRes = true;
                parseRes = true;
            }
            break;
        case 'n':
            if (strncmp(value, "no", len) == 0) {
                tmpRes = false;
                parseRes = true;
            }
            break;
        case 'o':
            /* 'o' is not unique enough */
            if (strncmp(value, "on", (len > sizeof("on") ? len : sizeof("on"))) == 0) {
                tmpRes = true;
                parseRes = true;
            } else if (strncmp(value, "off", (len > sizeof("off") ? len : sizeof("off"))) == 0) {
                tmpRes = false;
                parseRes = true;
            }
            break;
        case '1':
            if (len == 1) {
                tmpRes = true;
                parseRes = true;
            }
            break;
        case '0':
            if (len == 1) {
                tmpRes = false;
                parseRes = true;
            }
            break;
        default:
            break;
    }

    if (result != nullptr) {
        *result = tmpRes;
    }
    return parseRes;
}

Datum BoolIn(FunctionCallInfo fcinfo)
{
    const char *str = STATIC_CAST_PTR_TYPE(DatumGetPointer(fcinfo->arg[0]), const char *);
    size_t len;
    bool result = false;

    /*
     * Skip leading and trailing whitespace
     */
    unsigned char c;

    c = static_cast<unsigned char>(*str);
    while (isspace(c)) {
        str++;
        c = static_cast<unsigned char>(*str);
    }

    len = strlen(str);
    c = static_cast<unsigned char>(str[len - 1]);
    while (len > 0 && isspace(c)) {
        len--;
        c = static_cast<unsigned char>(str[len - 1]);
    }

    StorageReleasePanic(!ParseBoolWithLen(str, len, &result), MODULE_SYSTABLE, ErrMsg("invalid bool: %s", str));
    return BoolGetDatum(result);
}

Datum CharIn(FunctionCallInfo fcinfo)
{
    const char *s = STATIC_CAST_PTR_TYPE(DatumGetPointer(fcinfo->arg[0]), const char *);
    return CharGetDatum(s[0]);
}

Datum Int4In(FunctionCallInfo fcinfo)
{
    const char *s = STATIC_CAST_PTR_TYPE(DatumGetPointer(fcinfo->arg[0]), const char *);
    int32_t i = static_cast<int32_t>(UtStrtol(s));
    return Int32GetDatum(i);
}

Datum Int8In(FunctionCallInfo fcinfo)
{
    const char *s = STATIC_CAST_PTR_TYPE(DatumGetPointer(fcinfo->arg[0]), const char *);
    int64_t i = strtoll(s, nullptr, BASE);
    return Int64GetDatum(i);
}

Datum Int1In(FunctionCallInfo fcinfo)
{
    char *s = STATIC_CAST_PTR_TYPE(DatumGetPointer(fcinfo->arg[0]), char *);
    return UINT8_GET_DATUM(static_cast<uint8>(strtol(s, nullptr, BASE)));
}

Datum XidIn4(FunctionCallInfo fcinfo)
{
    char *s = STATIC_CAST_PTR_TYPE(DatumGetPointer(fcinfo->arg[0]), char *);
    return SET_4_BYTES((strtoul(s, nullptr, 0)));
}

Datum Float8In(FunctionCallInfo fcinfo)
{
    char *s = STATIC_CAST_PTR_TYPE(DatumGetPointer(fcinfo->arg[0]), char *);
    double val = strtod(s, nullptr);
    return Float64GetDatum(val);
}

Datum Float4In(FunctionCallInfo fcinfo)
{
    char *s = STATIC_CAST_PTR_TYPE(DatumGetPointer(fcinfo->arg[0]), char *);
    float32 val = static_cast<float32>(strtod(s, nullptr));
    return Float32GetDatum(val);
}

Datum XidIn(FunctionCallInfo fcinfo)
{
    char *s = STATIC_CAST_PTR_TYPE(DatumGetPointer(fcinfo->arg[0]), char *);
    return Int64GetDatum(static_cast<int64_t>(strtoll(s, nullptr, BASE)));
}
} /* namespace DSTORE */