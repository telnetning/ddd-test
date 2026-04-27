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
 * -------------------------------------------------------------------------
 *
 * dstore_expanded_datum.h
 * 	  Declarations for access to "expanded" value representations.
 *
 *
 * IDENTIFICATION
 * 	  include/common/dstore_expanded_datum.h
 *
 * -------------------------------------------------------------------------
 */
#ifndef DSTORE_EXPANDED_DATUM_H
#define DSTORE_EXPANDED_DATUM_H

#include "types/data_types.h"
#include "common/dstore_datatype.h"

namespace DSTORE {
/* in order to distinguish heaptuple header, use 4 bytes 0 */
#define EXPANDED_PLACEHOLDER sizeof(uint32)

/* Size of an EXTERNAL datum that contains an uint32 and a pointer to an expanded object */
#define EXPANDED_POINTER_SIZE (VARHDRSZ_EXTERNAL + EXPANDED_PLACEHOLDER + sizeof(varatt_expanded))

/*
 * "Methods" that must be provided for any expanded object.
 *
 * get_flat_size: calculate total space for flattened representation.
 *
 * flatten_into: convert to flattened representation in *result, of size allocated_size.
 */
typedef Size (*EOM_get_flat_size_method)(ExpandedObjectHeader *eohptr);
typedef void (*EOM_flatten_into_method)(ExpandedObjectHeader *eohptr, void *result, Size allocated_size);

/* Struct of function pointers for an expanded object's methods */
typedef struct ExpandedObjectMethods {
    EOM_get_flat_size_method get_flat_size;
    EOM_flatten_into_method flatten_into;
} ExpandedObjectMethods;

/* Exists in  ExpandedObjectHeader to distinguish different expanded object types */
typedef enum ExpandedType {
    EXPANDED_NONE,
    EXPANDED_RECORD,
    EXPANDED_ARRAY, /* only used in array type's expanded struct ExpandedArrayHeader */
    /* when nesttable nest an int[] type, the element type is EXPANDED_BRACKET_ARRAY */
    /* used in child element type, and it means child element is like int[] type */
    EXPANDED_BRACKET_ARRAY,
    /* when nesttable nest an varray type and VARRAY_COMPAT is true, the element type is EXPANDED_VARING_ARRAY */
    /* used in child element type, and it means child element is a plsql varray type */
    EXPANDED_VARING_ARRAY,
    EXPANDED_NESTTABLE,
    EXPANDED_INDEXBYTABLE
} ExpandedType;

/*
 * Each expanded object must hold this header;
 */
struct ExpandedObjectHeader {
    /* Phony varlena header */
    int32 vl_len_; /* always EOH_HEADER_MAGIC, see below */

    ExpandedType obj_type; /* expanded object type, see ExpandedType */

    /* Pointer to methods required for object type */
    const ExpandedObjectMethods *eoh_methods;

    /* Memory context containing this header and subsidiary data */
    DstoreMemoryContext eoh_context;

    /* Standard R/W TOAST pointer for this object is kept here */
    char eoh_rw_ptr[EXPANDED_POINTER_SIZE];

    /* Standard R/O TOAST pointer for this object is kept here */
    char eoh_ro_ptr[EXPANDED_POINTER_SIZE];
};

uint32 DstoreExpandedVarSize(void* ptr);
void DstoreformExpandedVar(void* ptr, void *result, Size allocatedSize);

}  // namespace DSTORE

#endif  // DSTORE_EXPANDED_DATUM_H
