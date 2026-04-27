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
 * dstore_tupledesc.h
 *     This file defines the building blocks that bg page writer needs.
 *
 * IDENTIFICATION
 *        include/tuple/dstore_tupledesc.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef SRC_GAUSSKERNEL_INCLUDE_TUPLE_DSTORE_TUPLEDESC_H
#define SRC_GAUSSKERNEL_INCLUDE_TUPLE_DSTORE_TUPLEDESC_H

#include "common/dstore_datatype.h"
#include "common/memory/dstore_mctx.h"
#include "catalog/dstore_fake_attribute.h"
#include "errorcode/dstore_common_error_code.h"
#include "securec.h"
#include "tuple/dstore_tuple_interface.h"

namespace DSTORE {

/* This structure contains init\defval of a tuple */
struct DstoreTupInitDefVal {
    Datum *datum;
    bool isNull;
    uint16 dataLen;
};

/*
 * get init default value from tupleDesc.
 * attrinitdefvals of tupleDesc come from the attrinitdefval of pg_attribute
 */
inline Datum GetTupInitDefVal(int attNum, TupleDesc tupleDesc, bool *isNull)
{
    *isNull = true;

    if (tupleDesc->initdefvals != nullptr) {
        *isNull = tupleDesc->initdefvals[attNum - 1].isNull;
        if (!(*isNull)) {
            return FetchAtt(tupleDesc->attrs[attNum - 1], tupleDesc->initdefvals[attNum - 1].datum);
        }
    }

    return static_cast<Datum>(0);
}

/*
 * TupleDescCopy
 *      Copy a tuple descriptor into caller-supplied memory.
 *      The memory may be shared memory mapped at any address, and must
 *      be sufficient to hold TupleDescSize(src) bytes.
 *
 * !!! Constraints and defaults are not copied !!!
 */
inline TupleDescData *CopyTupleDesc(TupleDesc tupleDesc)
{
    /*
     * Allocate enough memory for the tuple descriptor, including the
     * attribute rows, and set up the attribute row pointers.
     *
     * Note: we assume that sizeof(struct tupleDesc) is a multiple of the
     * struct pointer alignment requirement, and hence we don't need to insert
     * alignment padding between the struct and the array of attribute row
     * pointers.
     *
     * Note: Only the fixed part of pg_attribute rows is included in tuple
     * descriptors, so we only need ATTRIBUTE_FIXED_PART_SIZE space per attr.
     * That might need alignment padding, however.
     */
    char *stg = nullptr;
    uint32 attroffset =
        static_cast<uint32>(sizeof(TupleDescData) + static_cast<uint32>(tupleDesc->natts) * sizeof(Form_pg_attribute));
    attroffset = MAXALIGN(attroffset);
    stg = static_cast<char *>(DstorePalloc(attroffset +
        static_cast<uint32>(tupleDesc->natts) * MAXALIGN(ATTRIBUTE_FIXED_PART_SIZE)));
    if (STORAGE_VAR_NULL(stg)) {
        storage_set_error(COMMON_ERROR_MEMORY_ALLOCATION);
        ErrLog(DSTORE_ERROR, MODULE_COMMON,
               ErrMsg("[%s] Failed to palloc for tupleDesc", __FUNCTION__));
        return nullptr;
    }
    TupleDescData *copyDesc = static_cast<TupleDescData *>(static_cast<void *>(stg));
    *copyDesc = *tupleDesc;

    if (tupleDesc->natts > 0) {
        if (unlikely(tupleDesc->attrs == nullptr)) {
            DstorePfreeExt(copyDesc);
            return nullptr;
        }
        copyDesc->attrs = static_cast<Form_pg_attribute *>(static_cast<void *>(stg + sizeof(TupleDescData)));
        stg += attroffset;
        for (int i = 0; i < tupleDesc->natts; i++) {
            if (unlikely(tupleDesc->attrs[i] == nullptr)) {
                DstorePfreeExt(copyDesc);
                return nullptr;
            }
            copyDesc->attrs[i] = static_cast<Form_pg_attribute>(static_cast<void *>(stg));
            errno_t rc =
                memcpy_s(copyDesc->attrs[i], ATTRIBUTE_FIXED_PART_SIZE, tupleDesc->attrs[i], ATTRIBUTE_FIXED_PART_SIZE);
            storage_securec_check(rc, "\0", "\0");
            stg += MAXALIGN(ATTRIBUTE_FIXED_PART_SIZE);
        }
    } else {
        copyDesc->attrs = nullptr;
    }

    copyDesc->initdefvals = nullptr;

    /*
     * Also, assume the destination is not to be ref-counted.  (Copying the
     * source's refcount would be wrong in any case.)
     */
    copyDesc->tdtypeid = tupleDesc->tdtypeid;
    copyDesc->tdtypmod = tupleDesc->tdtypmod;
    copyDesc->tdisredistable = tupleDesc->tdisredistable;

    return copyDesc;
}

inline Form_pg_attribute DstoreTupleDescAttr(TupleDesc tupdesc, int i)
{
    return tupdesc->attrs[i];
}

}  // namespace DSTORE

#endif /* SRC_GAUSSKERNEL_INCLUDE_TUPLE_STORAGE_TUPLEDESC_H */
