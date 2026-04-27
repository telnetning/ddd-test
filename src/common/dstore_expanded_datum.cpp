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
 * dstore_expanded_datum.cpp
 * 	  Support functions for "expanded" value representations.
 *
 * IDENTIFICATION
 * 	  src/common/expandeddatum.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "common/dstore_expanded_datum.h"

namespace DSTORE {
inline char *VarDataExternalExpanded(varattrib_1b_e *ptr)
{
    return VarData1BE(ptr) + EXPANDED_PLACEHOLDER;
}

/*
 * DatumGetEOHP
 *
 * Given a Datum that is an expanded-object reference, extract the pointer.
 *
 * This is a bit tedious since the pointer may not be properly aligned;
 * compare VARATT_EXTERNAL_GET_POINTER().
 */
ExpandedObjectHeader *DatumGetEOHP(void *ptr)
{
    varattrib_1b_e *var_1b_e_ptr = (varattrib_1b_e *)ptr;
    varatt_expanded expanded_ptr;
    errno_t rc =
        memcpy_s(&expanded_ptr, sizeof(expanded_ptr), VarDataExternalExpanded(var_1b_e_ptr), sizeof(expanded_ptr));
    storage_securec_check(rc, "\0", "\0");
    return expanded_ptr.eohptr;
}

uint32 DstoreExpandedVarSize(void *ptr)
{
    ExpandedObjectHeader *eohptr = DatumGetEOHP(ptr);
    return (*eohptr->eoh_methods->get_flat_size)(eohptr);
}

void DstoreformExpandedVar(void *ptr, void *result, Size allocatedSize)
{
    ExpandedObjectHeader *eohptr = DatumGetEOHP(ptr);
    (*eohptr->eoh_methods->flatten_into)(eohptr, result, allocatedSize);
}

}  // namespace DSTORE
