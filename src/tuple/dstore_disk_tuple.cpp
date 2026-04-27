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
 * dstore_disk_tuple.cpp
 *
 * IDENTIFICATION
 *        src/tuple/dstore_disk_tuple.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "tuple/dstore_heap_tuple.h"
#include "tuple/dstore_disk_tuple_interface.h"

namespace DiskTupleInterface {
using namespace DSTORE;

uint32 GetCompFieldLength(HeapDiskTuple *diskTup)
{
    return DstoreVarSizeAny(diskTup);
}

void SetCompFieldLen(HeapDiskTuple *diskTup, uint32 len)
{
    DstoreSetVarSize(diskTup, len);
}

Oid GetDatumTypeId(HeapDiskTuple *diskTup)
{
    return diskTup->GetDatumTypeId();
}

void SetDatumTypeId(HeapDiskTuple *diskTup, Oid typeId)
{
    diskTup->SetDatumTypeId(typeId);
}

int32 GetDatumTypeMod(HeapDiskTuple *diskTup)
{
    return diskTup->GetDatumTypeMod();
}

void SetDatumTypeMod(HeapDiskTuple *diskTup, int32 typeMod)
{
    diskTup->SetDatumTypeMod(typeMod);
}

Oid GetOid(HeapDiskTuple *diskTup)
{
    return diskTup->GetOid();
}
void SetOid(HeapDiskTuple *diskTup, Oid oid)
{
    diskTup->SetOid(oid);
}

bool HasOid(HeapDiskTuple *diskTup)
{
    return diskTup->HasOid();
}

bool HasNull(HeapDiskTuple *diskTup)
{
    return diskTup->HasNull();
}

bool GetReservedForSql(HeapDiskTuple *diskTup)
{
    return diskTup->GetReservedForSql();
}

void SetReservedForSql(HeapDiskTuple *diskTup, bool val)
{
    return diskTup->SetReservedForSql(val);
}

void SetNatts(HeapDiskTuple *diskTup, uint32 natts)
{
    diskTup->SetNumColumn(natts);
}

uint32 GetNatts(HeapDiskTuple *diskTup)
{
    return diskTup->GetNumColumn();
}

bool HasExternal(DSTORE::HeapDiskTuple *diskTup)
{
    return diskTup->HasExternal();
}

Size GetValuesOffset(uint32 natts, bool hasNull, bool hasOid, bool isLinked)
{
    return HeapDiskTuple::GetValuesOffset(natts, hasNull, hasOid, isLinked);
}

void FillDiskTuple(DSTORE::HeapDiskTuple *diskTup, DSTORE::TupleDesc tupleDesc, DSTORE::Datum *values, bool *isNulls,
                   bool hasNull, Size diskTupSize)
{
    int attributeNum = tupleDesc->natts;

    /* Set information of diskTuple correctly */
    diskTup->SetNumColumn(static_cast<uint32>(attributeNum));
    if (hasNull) {
        diskTup->SetHasNull();
    }

    if (tupleDesc->tdhasoid) {
        diskTup->SetHasOid();
    }

    /* Now, fill this tuple by given data */
    if (hasNull) {
        DataTuple::AssembleData<true, HeapDiskTuple>(tupleDesc, values, isNulls,
                                                     static_cast<char *>(static_cast<void *>(diskTup)), diskTupSize);
    } else {
        DataTuple::AssembleData<false, HeapDiskTuple>(tupleDesc, values, isNulls,
                                                      static_cast<char *>(static_cast<void *>(diskTup)), diskTupSize);
    }

    DstoreSetVarSize(diskTup, static_cast<uint32>(diskTupSize));
    diskTup->SetDatumTypeId(tupleDesc->tdtypeid);
    diskTup->SetDatumTypeMod(tupleDesc->tdtypmod);
}

}