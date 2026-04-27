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
 * dstore_disk_tuple_interface.h
 *
 * IDENTIFICATION
 *        interface/tuple/dstore_disk_tuple_interface.h
 *
 * These interfaces are added for V5 SQL engine's direct access of DiskTuple structure.
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_DISK_TUPLE_INTERFACE_H
#define DSTORE_DISK_TUPLE_INTERFACE_H
#include "tuple/dstore_tuple_struct.h"

namespace DiskTupleInterface {
#pragma GCC visibility push(default)
uint32 GetCompFieldLength(DSTORE::HeapDiskTuple *diskTup);          /* HEAP_TUPLE_HEADER_GET_DATUM_LENGTH */
void SetCompFieldLen(DSTORE::HeapDiskTuple *diskTup, uint32 len);   /* HEAP_TUPLE_HEADER_SET_DATUM_LENGTH */
DSTORE::Oid GetDatumTypeId(DSTORE::HeapDiskTuple *diskTup);              /* HEAP_TUPLE_HEADER_GET_TYPE_ID */
void SetDatumTypeId(DSTORE::HeapDiskTuple *diskTup, DSTORE::Oid typeId); /* HEAP_TUPLE_HEADER_SET_TYPE_ID */
int32 GetDatumTypeMod(DSTORE::HeapDiskTuple *diskTup);                   /* HEAP_TUPLE_HEADER_GET_TYP_MOD */
void SetDatumTypeMod(DSTORE::HeapDiskTuple *diskTup, int32 typeMod);     /* HEAP_TUPLE_HEADER_SET_TYP_MOD */
DSTORE::Oid GetOid(DSTORE::HeapDiskTuple *diskTup);                 /* HEAP_TUPLE_HEADER_GET_OID */
void SetOid(DSTORE::HeapDiskTuple *diskTup, DSTORE::Oid oid);       /* HEAP_TUPLE_HEADER_SET_OID */
bool HasOid(DSTORE::HeapDiskTuple *diskTup);                        /* HEAP_TUPLE_HEADER_HAS_OID */
bool HasNull(DSTORE::HeapDiskTuple *diskTup);                       /* HEAP_TUPLE_HEADER_HAS_NULLS */
bool GetReservedForSql(DSTORE::HeapDiskTuple *diskTup);                  /* HEAP_TUPLE_HEADER_HAS_MATCH */
void SetReservedForSql(DSTORE::HeapDiskTuple *diskTup, bool val);        /* HEAP_TUPLE_HEADER_SET_MATCH */
                                                                    /* HEAP_TUPLE_HEADER_CLEAR_MATCH */
void SetNatts(DSTORE::HeapDiskTuple *diskTup, uint32 natts);        /* HEAP_TUPLE_HEADER_SET_NATTS */
uint32 GetNatts(DSTORE::HeapDiskTuple *diskTup);                    /* HEAP_TUPLE_HEADER_GET_NATTS */
bool HasExternal(DSTORE::HeapDiskTuple *diskTup);                   /* HEAP_TUPLE_HEADER_HAS_EXTERNAL */
/* Hack interfaces to expanded data type */
Size GetValuesOffset(uint32 natts, bool hasNull, bool hasOid, bool isLinked);
void FillDiskTuple(DSTORE::HeapDiskTuple *diskTup, DSTORE::TupleDesc tupleDesc, DSTORE::Datum *values,
                   bool *isNulls, bool hasNull, Size diskTupSize);
#pragma GCC visibility pop
} /* namespace DiskTupleInterface */

#endif /* DSTORE_DISK_TUPLE_INTERFACE_H */