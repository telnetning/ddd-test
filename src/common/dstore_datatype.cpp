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
 * IDENTIFICATION
 *        src/common/dstore_datatype.cpp
 *
 * ---------------------------------------------------------------------------------------
 */


#include "common/dstore_datatype.h"
#include "securec.h"
#include "common/log/dstore_log.h"
#include "common/memory/dstore_mctx.h"
#include "common/error/dstore_error.h"
#include "errorcode/dstore_common_error_code.h"

namespace DSTORE {


/* -------------------------------------------------------------------------
 * datumCopyInternal
 *
 * make a copy of a datum
 *
 * If the datatype is pass-by-reference, use parloc() to obtain the memory.
 * -------------------------------------------------------------------------
 */
Datum datumCopyInternal(Datum value, bool typByVal, int typLen, Size* copySize)
{
    Datum res;

    /* *copySize is 0 when the datum is assigned by value, or NULL pointer. */
    /* otherwise compute the real size and remember it. */
    *copySize = 0;

    if (typByVal) {
        res = value;
    } else {
        Size realSize;
        char* s = nullptr;
        errno_t rc;

        if (DatumGetPointer(value) == nullptr) {
            return PointerGetDatum(nullptr);
        }
        realSize = datumGetSize(value, typByVal, typLen);
        s = static_cast<char *>(DstorePalloc(realSize));
        if (STORAGE_VAR_NULL(s)) {
            return PointerGetDatum(nullptr);
        }
        rc = memcpy_s(s, realSize, DatumGetPointer(value), realSize);
        storage_securec_check(rc, "", "");
        res = PointerGetDatum(s);
        *copySize = realSize;
    }
    return res;
}

/*-------------------------------------------------------------------------
 * datumCopy
 *
 * Make a copy of a non-NULL datum.
 *
 * If the datatype is pass-by-reference, memory is obtained with palloc().
 *
 * If the value is a reference to an expanded object, we flatten into memory
 * obtained with palloc().  We need to copy because one of the main uses of
 * this function is to copy a datum out of a transient memory context that's
 * about to be destroyed, and the expanded object is probably in a child
 * context that will also go away.  Moreover, many callers assume that the
 * result is a single pfree-able chunk.
 *-------------------------------------------------------------------------
 */
Datum datumCopy(Datum value, bool typByVal, int typLen)
{
    Size copySize = 0;

    return datumCopyInternal(value, typByVal, typLen, &copySize);
}

/* -------------------------------------------------------------------------
 * datumGetSize
 *
 * Find the "real" size of a datum, given the datum value,
 * whether it is a "by value", and the length of the declared type.
 *
 * This is essentially an out-of-line version of the att_addlength_datum()
 * macro in access/tupmacs.h.  We did a tad more error checking though.
 * -------------------------------------------------------------------------
 */
Size datumGetSize(Datum value, bool typByVal, int typLen)
{
    Size size;

    if (typByVal) {
        /* Pass-by-value types are always fixed-length */
        StorageAssert(typLen > 0 && static_cast<unsigned int>(typLen) <= sizeof(Datum));
        size = static_cast<Size>(static_cast<unsigned int>(typLen));
    } else {
        if (typLen > 0) {
            /* Fixed-length pass-by-ref type */
            size = static_cast<Size>(static_cast<unsigned int>(typLen));
        } else if (typLen == VARLEAN_DATATYPE_LENGTH) {
            /* It is a varlena datatype */
            struct varlena* s = STATIC_CAST_PTR_TYPE(DatumGetPointer(value), struct varlena*);

            if (!DstorePointerIsValid(s)) {
                storage_set_error(DATATYPE_ERROR_INVALID_DATUM_POINTER);
            }

            size = static_cast<Size>(DstoreVarSizeAny(s));
        } else if (typLen == CSTRING_DATATYPE_LENGTH) {
            /* It is a cstring datatype */
            char* s = static_cast<char*>(DatumGetPointer(value));

            if (!DstorePointerIsValid(s)) {
                storage_set_error(DATATYPE_ERROR_INVALID_DATUM_POINTER);
            }

            size = static_cast<Size>(strlen(s) + 1);
        } else {
            storage_set_error(DATATYPE_ERROR_INVALID_TYPLEN, typLen);
            size = 0; /* keep compiler quiet */
        }
    }

    return size;
}

/*-------------------------------------------------------------------------
 * datumImageEq
 *
 * Compares two datums for identical contents, based on byte images.  Return
 * true if the two datums are equal, false otherwise.
 *-------------------------------------------------------------------------
 */
bool datumImageEq(Datum value1, Datum value2, bool typByVal, int typLen)
{
    Size len1;
    Size len2;
    Size dataLength;
    bool result = true;

    if (typByVal) {
        result = (value1 == value2);
    } else if (typLen > 0) {
        result = (memcmp(DatumGetPointer(value1),
                         DatumGetPointer(value2),
                         static_cast<size_t>(static_cast<long>(typLen))) == 0);
    } else if (typLen == VARLEAN_DATATYPE_LENGTH) {
        len1 = DstoreVarSizeAny(DatumGetPointer(value1));
        len2 = DstoreVarSizeAny(DatumGetPointer(value2));
        /* No need to de-toast if lengths don't match. */
        if (len2 != len1) {
            result = false;
        } else {
            struct varlena *arg1val;
            struct varlena *arg2val;

            /* no detoast, so simply convert datum to varlena */
            arg1val = STATIC_CAST_PTR_TYPE(DatumGetPointer(value1), struct varlena *);
            arg2val = STATIC_CAST_PTR_TYPE(DatumGetPointer(value2), struct varlena *);
            dataLength = DSTORE_VARSIZE_ANY_EXHDR(DatumGetPointer(value1));
            result = (memcmp(VarDataAny(arg1val),
                             VarDataAny(arg2val),
                             dataLength) == 0);
        }
    } else if (typLen == CSTRING_DATATYPE_LENGTH) {
        char *str1, *str2;

        /* Compare cstring datums */
        str1 = DatumGetCString(value1);
        str2 = DatumGetCString(value2);
        len1 = strlen(str1) + 1;
        len2 = strlen(str2) + 1;
        if (len1 != len2) {
            return false;
        }
        result = (memcmp(str1, str2, len1) == 0);
    } else {
        /* unexpected typLen */
        StorageAssert(0);
    }

    return result;
}


/*
 * Int128GetDatum
 *		Returns datum representation for a 128-bit integer.
 */
Datum Int128GetDatum(int128 x)
{
    int128 *retval = (int128*)DstorePalloc(sizeof(int128));
    if (retval == nullptr) {
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        ErrLog(DSTORE_PANIC, MODULE_COMMON, ErrMsg("Out of memory"));
    }
    *retval = x;
    return PointerGetDatum(retval);
}

/* copied from timestamp.c */
time_t timestamptzTotime_t(TimestampTz t)
{
    time_t result;

#ifdef HAVE_INT64_TIMESTAMP
    result = static_cast<time_t>(t / STORAGE_USECS_PER_SEC +
        ((DSTORE_EPOCH_JDATE - STORAGE_UNIX_EPOCH_JDATE) * STORAGE_SECS_PER_DAY));
#else
    result = static_cast<time_t>(t + ((DSTORE_EPOCH_JDATE - STORAGE_UNIX_EPOCH_JDATE) * STORAGE_SECS_PER_DAY));
#endif
    return result;
}

/* copied from timestamp.c */
const char* TimestamptzTostr(TimestampTz dt)
{
    constexpr uint32_t MAXDATELEN = 128;
    constexpr uint32_t MAXOUTPUTLEN = 2 * MAXDATELEN + 14;
    static char buf[MAXOUTPUTLEN];
    char ts[MAXDATELEN + 1];
    char zone[MAXDATELEN + 1];
    time_t result = static_cast<time_t>(timestamptzTotime_t(dt));
    struct tm* ltime = localtime(&result);
    errno_t rc = EOK;

    StorageAssert(ltime != NULL);
    strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", ltime);
    strftime(zone, sizeof(zone), "%Z", ltime);

#ifdef HAVE_INT64_TIMESTAMP
    rc = snprintf_s(buf, sizeof(buf), sizeof(buf) - 1, "%s.%06d %s", ts, (int)(dt % STORAGE_USECS_PER_SEC), zone);
    storage_securec_check_ss(rc);
#else
    rc = snprintf_s(buf, sizeof(buf), sizeof(buf) - 1, "%s.%.6f %s", ts, fabs(dt - floor(dt)), zone);
    storage_securec_check_ss(rc);
#endif

    return buf;
}

}

