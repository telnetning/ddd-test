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
 * dstore_array_utils.cpp
 *
 * IDENTIFICATION
 *        dstore/src/common/datatype/dstore_array_utils.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "catalog/dstore_typecache.h"
#include "common/memory/dstore_memory_allocator.h"
#include "errorcode/dstore_common_error_code.h"

namespace DSTORE {

/*
 * Convert array dimensions into number of elements
 *
 * This must do overflow checking, since it is used to validate that a user
 * dimensionality request doesn't overflow what we can handle.
 *
 * We limit array sizes to at most about a quarter billion elements,
 * so that it's not necessary to check for overflow in quite so many
 * places --- for instance when palloc'ing Datum arrays.
 *
 * The multiplication overflow check only works on machines that have int64
 * arithmetic, but that is nearly all platforms these days, and doing check
 * divides for those that don't seems way too expensive.
 */
int ArrayGetNItems(int ndim, const int *dims)
{
    int32 ret;
    int i;

#define MAX_ARRAY_SIZE (static_cast<Size>(MaxAllocSize / sizeof(Datum)))

    if (ndim <= 0) {
        return 0;
    }
    ret = 1;
    for (i = 0; i < ndim; i++) {
        int64 prod;

        /* A negative dimension implies that UB-LB overflowed ... */
        if (dims[i] < 0) {
            storage_set_error(ARRAY_ERROR_ARRAY_SIZE_EXCEED_LIMIT, static_cast<int>(MAX_ARRAY_SIZE));
        }

        prod = static_cast<int64>(ret) * static_cast<int64>(dims[i]);

        ret = static_cast<int32>(prod);
        if (static_cast<int64>(ret) != prod) {
            storage_set_error(ARRAY_ERROR_ARRAY_SIZE_EXCEED_LIMIT, static_cast<int>(MAX_ARRAY_SIZE));
        }
    }
    StorageAssert(ret >= 0);
    if (static_cast<uint32>(ret) > MAX_ARRAY_SIZE) {
        storage_set_error(ARRAY_ERROR_ARRAY_SIZE_EXCEED_LIMIT, static_cast<int>(MAX_ARRAY_SIZE));
    }

    return static_cast<int>(ret);
}

ArrayType *DatumGetArray(Datum datum)
{
    void *datumPtr = static_cast<void *>(DatumGetPointer(datum));
    void *newDatumPtr = datumPtr;
    if (VarAttIs1B(datumPtr)) {
        uint32 dataSize = static_cast<uint32>(DstoreVarSize1B(datumPtr) - VARHDRSZ_SHORT);
        uint32 newVarSize = dataSize + VARHDRSZ;
        newDatumPtr = DstorePalloc(newVarSize);
        if (STORAGE_VAR_NULL(newDatumPtr)) {
            storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
            return nullptr;
        }
        int rc = memcpy_s(VarData4B(newDatumPtr), dataSize, VarData1B(datumPtr), dataSize);
        storage_securec_check(rc, "\0", "\0");
        DstoreSetVarSize4B(newDatumPtr, newVarSize);
    } else {
        /* To be supported: get array from compressed datum */
        StorageReleasePanic(VarAttIs4BC(datumPtr), MODULE_INDEX,  ErrMsg("Do not support compressed datum"));
    }
    return static_cast<ArrayType *>(newDatumPtr);
}

/*
 * DeconstructArray  --- simple method for extracting data from an array
 */
RetStatus DeconstructArray(ArrayType *array, Datum **elemValues, bool **elemIsNulls, int *elemNum)
{
    TypeCache typeCache = g_storageInstance->GetCacheHashMgr()->GetTypeCacheFromTypeOid(ARR_ELEMTYPE(array));
    StorageAssert(ARR_ELEMTYPE(array) == typeCache.type);
    const uint32 bitstep = 0X100;

    int nElems = ArrayGetNItems(ARR_NDIM(array), ARR_DIMS(array));
    Datum *values = static_cast<Datum *>(DstorePalloc(static_cast<uint32>(nElems) * sizeof(Datum)));
    if (STORAGE_VAR_NULL(values)) {
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        return DSTORE_FAIL;
    }
    bool *isNulls = static_cast<bool *>(DstorePalloc0(static_cast<uint32>(nElems) * sizeof(bool)));
    if (STORAGE_VAR_NULL(isNulls)) {
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        DstorePfree(values);
        return DSTORE_FAIL;
    }

    char *dataPtr = ARR_DATA_PTR(array);
    bits8* bitmap = ARR_NULLBITMAP(array);
    uint32 bitmask = 1;
    for (int i = 0; i < nElems; i++) {
        /* Get source element, checking for NULL */
        if (bitmap != nullptr && (*bitmap & bitmask) == 0) {
            values[i] = static_cast<Datum>(0);
            isNulls[i] = true;
        } else {
            values[i] = FetchAtt(dataPtr, typeCache.attbyval, typeCache.attlen);
            dataPtr = AttAddLength(dataPtr, typeCache.attlen, dataPtr);
            dataPtr = AttAlignNominal(dataPtr, typeCache.attalign);
        }

        /* advance bitmap pointer if any */
        if (bitmap != nullptr) {
            bitmask <<= 1;
            if (bitmask == bitstep) {
                bitmap++;
                bitmask = 1;
            }
        }
    }
    *elemValues = values;
    *elemIsNulls = isNulls;
    *elemNum = nElems;
    return DSTORE_SUCC;
}

/*
 * Support for cleaning up detoasted copies of inputs.  This must only
 * be used for pass-by-ref datatypes, and normally would only be used
 * for toastable types.  If the given pointer is different from the
 * original argument, assume it's a palloc'd detoasted copy, and DstorePfree it.
 * NOTE: most functions on toastable types do not have to worry about this,
 * but we currently require that support functions for indexes not leak
 * memory.
 */
void DstoreFreeIfCopy(ArrayType* ptr, int n, FunctionCallInfo fcinfo)
{
    if (static_cast<Pointer>(static_cast<void*>(ptr)) != DatumGetPointer(fcinfo->arg[n])) {
        DstorePfree(ptr);
    }
}
}
