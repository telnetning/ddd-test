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
 * dstore_tuple_struct.h
 *     This file defines the building blocks that bg page writer needs.
 *
 * IDENTIFICATION
 *        interface/tuple/dstore_tuple_struct.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_TUPLE_UTILS_H
#define DSTORE_TUPLE_UTILS_H

#include "common/dstore_common_utils.h"
#include "systable/systable_attribute.h"

namespace DSTORE {

struct HeapTuple;
struct HeapDiskTuple;
struct IndexTuple;

constexpr uint32_t DSTORE_TUPLE_TYPE = 3;

/* Max dstore heap tuple size, must be greater than sizeof(HeapTuple). */
constexpr uint32_t DSTORE_TUPLE_HEADER_MAX_SIZE = 64;

/*
 * MAX_TUPLE_ATTR limits the number of (user) columns in a tuple.
 * The key limit on this value is that the size of the fixed overhead for
 * a tuple, plus the size of the null-values bitmap (at 1 bit per column),
 * plus MAXALIGN alignment, must fit into t_hoff which is uint8.  On most
 * machines the upper limit without making t_hoff wider would be a little
 * over 1700.  We use round numbers here and for MAX_TUPLE_ATTR
 * so that alterations in HeapTupleHeader layout won't change the
 * supported max number of columns.
 */
const int16_t MAX_TUPLE_ATTR = 1664; /* 8 * 208 */

/*
 * This struct is passed around within the backend to describe the structure
 * of tuples.  For tuples coming from on-disk relations, the information is
 * collected from the pg_attribute, pg_attrdef, and pg_constraint catalogs.
 * Transient row types (such as the result of a join query) have anonymous
 * TupleDesc structs that generally omit any constraint info; therefore the
 * structure is designed to let the constraints be omitted efficiently.
 *
 * Note that only user attributes, not system attributes, are mentioned in
 * TupleDesc; with the exception that tdhasoid indicates if OID is present.
 *
 * If the tupdesc is known to correspond to a named rowtype (such as a table's
 * rowtype) then tdtypeid identifies that type and tdtypmod is -1.  Otherwise
 * tdtypeid is RECORDOID, and tdtypmod can be either -1 for a fully anonymous
 * row type, or a value >= 0 to allow the rowtype to be looked up in the
 * typcache.c type cache.
 *
 * Tuple descriptors that live in caches (relcache or typcache, at present)
 * are reference-counted: they can be deleted when their reference count goes
 * to zero.  Tuple descriptors created by the executor need no reference
 * counting, however: they are simply created in the appropriate memory
 * context and go away when the context is freed.  We set the tdrefcount
 * field of such a descriptor to -1, while reference-counted descriptors
 * always have tdrefcount >= 0.
 */
struct TupleDescData {
    int natts;                     /* number of attributes in the tuple */
    bool tdisredistable;           /* temp table created for data redistribution by the redis tool */
    Oid tdtypeid;                  /* composite type ID for tuple type */
    int32_t tdtypmod;              /* typmod for tuple type */
    bool tdhasoid;                 /* tuple has oid attribute in its header */
    int tdrefcount;                /* reference count, or -1 if not counting */
    bool tdhasuids;                /* tuple has uid attribute in its header */
    bool tdhaslob;                 /* tuple has lob column(s) */
    FormData_pg_attribute **attrs; /* attrs[N] is a pointer to the description of Attribute Number N+1 */
    /* design for ADD COLUMN and constraints */
    struct DstoreTupInitDefVal *initdefvals; /* init default value due to ADD COLUMN */

    /*
     * TupleDescCopy
     *      Copy a tuple descriptor into caller-supplied memory.
     *      The memory may be shared memory mapped at any address, and must
     *      be sufficient to hold TupleDescSize(src) bytes.
     *
     * !!! Constraints and defaults are not copied !!!
     */
    TupleDescData *Copy();
};

using TupleDesc = TupleDescData *;

/* Attribute numbers for the system-defined attributes */
enum class HeapTupleSystemAttr {
    DSTORE_SELF_ITEM_POINTER_ATTRIBUTE_NUMBER = -1,
    DSTORE_OBJECT_ID_ATTRIBUTE_NUMBER = -2,
    DSTORE_TRX_INSERT_XID_ATTRIBUTE_NUMBER = -3,
    DSTORE_TRX_DELETE_XID_ATTRIBUTE_NUMBER = -5,
    DSTORE_TABLE_OID_ATTRIBUTE_NUMBER = -7,
    DSTORE_FIRST_LOW_INVALID_HEAP_ATTRIBUTE_NUMBER = -12
};

struct TupleAttrContext {
    TupleDescData *const tupleDesc;
    Datum *const values;
    bool *const isNulls;
    long offset;
    bool slow;
};

struct ModifyLobLocatorRelidContext {
    HeapTuple *tuple;
    TupleDescData *const tupleDesc;
    Oid targetTableOid;         /* target table oid after swap */
    bool &needUpdateTuple;
};

using AllocMemFunc = void* (*)(uint32_t);
using FreeMemFunc = void (*)(void *ptr);

} /* namespace DSTORE */

#endif
