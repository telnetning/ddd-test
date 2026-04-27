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
 * dstore_array_utils.h
 *
 * IDENTIFICATION
 *        dstore/include/common/datatype/dstore_array_utils.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_DSTORE_ARRAY_UTILS_H
#define DSTORE_DSTORE_ARRAY_UTILS_H

namespace DSTORE {

#include "catalog/dstore_fake_type.h"

/*
 * Arrays are varlena objects, so must meet the varlena convention that
 * the first int32 of the object contains the total object size in bytes.
 * Be sure to use VARSIZE() and SET_VARSIZE() to access it, though!
 *
 * CAUTION: if you change the header for ordinary arrays you will also
 * need to change the headers for oidvector and int2vector!
 */
struct ArrayType {
    int32  vlLen;       /* varlena header (do not touch directly!) */
    int    ndim;        /* # of dimensions */
    int32  dataoffset;  /* offset to data, or 0 if no bitmap */
    Oid    elemtype;    /* element type OID */
};

/*
 * Access macros for varlena array header fields.
 *
 * ARR_DIMS returns a pointer to an array of array dimensions (number of
 * elements along the various array axes).
 *
 * ARR_LBOUND returns a pointer to an array of array lower bounds.
 *
 * That is: if the third axis of an array has elements 5 through 8, then
 * ARR_DIMS(a)[2] == 4 and ARR_LBOUND(a)[2] == 5.
 *
 * Unlike C, the default lower bound is 1.
 */
#define ARR_SIZE(a)                DstoreVarSize(a)
#define ARR_NDIM(a)                ((a)->ndim)
#define ARR_HASNULL(a)            ((a)->dataoffset != 0)
#define ARR_ELEMTYPE(a)            ((a)->elemtype)

#define ARR_DIMS(a) \
        (static_cast<int *>(static_cast<void *>((static_cast<char *>( \
        static_cast<void *>(a))) + sizeof(ArrayType))))
#define ARR_LBOUND(a) \
        (static_cast<int *>(static_cast<void *>((static_cast<char *>(static_cast<void *>(a))) + \
        sizeof(ArrayType) + sizeof(int) * ARR_NDIM(a))))

#define ARR_NULLBITMAP(a) \
        (ARR_HASNULL(a) ? \
         static_cast<bits8 *>(static_cast<void *>((static_cast<char *>(static_cast<void *>(a))) + \
         sizeof(ArrayType) + 2 * sizeof(int) * static_cast<uint32>(ARR_NDIM(a)))) : static_cast<bits8 *>(nullptr))

/*
 * The total array header size (in bytes) for an array with the specified
 * number of dimensions and total number of items.
 */
#define ARR_OVERHEAD_NONULLS(ndims) \
        MAXALIGN(sizeof(ArrayType) + 2 * sizeof(int) * static_cast<uint32>(ndims))
#define ARR_OVERHEAD_WITHNULLS(ndims, nitems) \
        MAXALIGN(sizeof(ArrayType) + 2 * sizeof(int) * static_cast<uint32>(ndims) + \
                 ((nitems) + 7) / 8)

#define ARR_DATA_OFFSET(a) \
        (ARR_HASNULL(a) ? static_cast<uint32>((a)->dataoffset) : ARR_OVERHEAD_NONULLS(ARR_NDIM(a)))
/*
 * Returns a pointer to the actual array data.
 */
#define ARR_DATA_PTR(a) \
        ((static_cast<char *>(static_cast<void *>(a))) + static_cast<uint32>(ARR_DATA_OFFSET(a)))

ArrayType *DatumGetArray(Datum datum);
int ArrayGetNItems(int ndim, const int *dims);
RetStatus DeconstructArray(ArrayType *array, Datum **elemValues, bool **elemIsNulls, int *elemNum);

}

#endif // DSTORE_STORAGE_ARRAY_UTILS_H
