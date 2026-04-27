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
 * dstore_tuple_interface.h
 *
 * IDENTIFICATION
 *        interface/tuple/dstore_tuple_interface.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_TUPLE_INTERFACE_H
#define DSTORE_TUPLE_INTERFACE_H

#include "page/dstore_itemptr.h"
#include "tuple/dstore_tuple_struct.h"

namespace TupleInterface {
#pragma GCC visibility push(default)
constexpr uint32_t MAX_TUPLE_NUM_PER_PAGE = 1986;
DSTORE::HeapTuple *CopyHeapTuple(DSTORE::HeapTuple *tuple, DSTORE::AllocMemFunc allocMem);
void CopyHeapTupleToDest(DSTORE::HeapTuple *destTup, DSTORE::HeapTuple *srcTup, bool isExternalMem);

DSTORE::HeapTuple *FormHeapTuple(DSTORE::TupleDesc tupleDescriptor, DSTORE::Datum *values, bool *isnull,
                                 DSTORE::AllocMemFunc allocMem);

void DeformHeapTuple(DSTORE::HeapTuple *tuple, DSTORE::TupleDesc tupleDesc, DSTORE::Datum *values, bool *isNulls);

DSTORE::Datum DeformHeapColumnData(DSTORE::HeapTuple *tuple, DSTORE::TupleDesc tuple_desc,
    DSTORE::Form_pg_attribute att, char *tupleValues, char *&lobValues);

void DeformHeapTuplePart(DSTORE::HeapTuple *tuple, DSTORE::TupleAttrContext &context, int start, int end);

uint64_t GetXid(DSTORE::HeapTuple *tuple);

DSTORE::IndexTuple *FormIndexTuple(DSTORE::TupleDesc tupleDescriptor, DSTORE::Datum *values, bool *isNulls);

DSTORE::Datum GetHeapSysAttr(DSTORE::HeapTuple *tuple, int attnum, bool *isNull);

DSTORE::Datum GetHeapAttr(DSTORE::HeapTuple *tuple, int attnum, DSTORE::TupleDesc desc, bool *isNull,
                          bool forceReturnLobLocator = false);

DSTORE::Datum GetIndexAttr(DSTORE::IndexTuple *tuple, int attNum, DSTORE::TupleDesc desc, bool *isNull);

void DeformIndexTuple(DSTORE::IndexTuple *tuple, DSTORE::TupleDesc tupleDesc, DSTORE::Datum *values, bool *isNulls);

DSTORE::RetStatus IsIndexTupleValueEqual(DSTORE::IndexTuple *tuple1, DSTORE::IndexTuple *tuple2);

DSTORE::HeapTuple *ModifyTuple(DSTORE::HeapTuple *tuple, const DSTORE::TupleAttrContext &context, const bool *doReplace,
                               DSTORE::AllocMemFunc allocMem);

void ModifyLobLocatorRelid(DSTORE::ModifyLobLocatorRelidContext &context);

void DestroyTuple(DSTORE::HeapTuple *tuple, DSTORE::FreeMemFunc freeMem);
bool HasNull(DSTORE::HeapTuple *tuple);
bool HasExternal(DSTORE::HeapTuple *tuple);
bool HasInlineLob(DSTORE::HeapTuple *tuple);

char *GetValues(DSTORE::HeapTuple *tuple);
uint32_t GetValuesOffset(DSTORE::HeapTuple *tuple);

DSTORE::Oid GetOid(DSTORE::HeapTuple *tuple);
void SetOid(DSTORE::HeapTuple *tuple, DSTORE::Oid oid);
bool HasOid(DSTORE::HeapTuple *tuple);
void TupleStructInit(DSTORE::HeapTuple *tuple);

size_t GetTupleStructSize();

DSTORE::Oid GetTableOid(DSTORE::HeapTuple *tuple);
void SetTableOid(DSTORE::HeapTuple *tuple, DSTORE::Oid oid);
void SetLobTargetOid(DSTORE::HeapTuple *tuple, DSTORE::Oid oid);

DSTORE::ItemPointer GetCtidPtr(DSTORE::HeapTuple *tuple);
DSTORE::ItemPointerData GetCtid(DSTORE::HeapTuple *tuple);
void SetCtid(DSTORE::HeapTuple *tuple, DSTORE::ItemPointerData ctid);

uint16_t GetNumAttrs(DSTORE::HeapTuple *tuple);
bool AttrIsNull(DSTORE::HeapTuple *tuple, int attrIdx);

char *GetStruct(DSTORE::HeapTuple *tuple);

uint32_t GetTupleTotalSize(DSTORE::HeapTuple *tuple);
uint32_t GetDiskTupleSize(DSTORE::HeapTuple *tuple);
void SetDiskTupleSize(DSTORE::HeapTuple *tuple, uint32_t size);
void *GetDiskTuple(DSTORE::HeapTuple *tuple);
void SetDiskTuple(DSTORE::HeapTuple *tuple, void *diskTuple);
uint8_t GetDiskTupleHeaderSize(void);
char *GetDiskTupleData(DSTORE::HeapTuple *tuple);
void DiskTupleSetTupleSize(DSTORE::HeapTuple *tuple, uint16_t size);
void ResetInfo(DSTORE::HeapTuple *tuple);
void SetHasOid(DSTORE::HeapTuple *tuple);
void SetHasExternal(DSTORE::HeapTuple *tuple);
void SetHasVariable(DSTORE::HeapTuple *tuple);
void SetNumColumn(DSTORE::HeapTuple *tuple, uint32_t nattrs);
void SetHasNull(DSTORE::HeapTuple *tuple);
void SetExternalMem(DSTORE::HeapTuple *tuple);
uint32_t GetSizeOfHeapTuple(void);
DSTORE::Oid GetDatumTypeId(DSTORE::HeapTuple *tuple);
void SetDatumTypeId(DSTORE::HeapTuple *tuple, DSTORE::Oid typeId);

int32_t GetDatumTypeMod(DSTORE::HeapTuple *tuple);
void SetDatumTypeMod(DSTORE::HeapTuple *tuple, int32_t typeMod);

DSTORE::Form_pg_attribute GetHeapTupleDescAttr(DSTORE::TupleDesc tupdesc, int i);
template<typename OffsetType>
OffsetType HeapAttAlignNominal(OffsetType currOffset, char attAlign);
template<typename OffsetType>
OffsetType HeapAttAlignPointer(OffsetType currOffset, char attAlign, int16_t attLen, const char *attPtr);
template<typename OffsetType>
OffsetType HeapAttAddLength(OffsetType currOffset, int16_t attLen, char *attPtr);
DSTORE::Datum GetHeapTupInitDefVal(int attNum, DSTORE::TupleDesc tupleDesc, bool *isNull);

/* warning: the caller must free the tuple header. */
void CreateHeapTuple(uint32_t diskTupSize, void *diskTup, DSTORE::HeapTuple *tuple);

bool IsTupleEqual(DSTORE::HeapTuple *newTuple, DSTORE::HeapTuple *oldTuple);

#pragma GCC visibility pop
}  // namespace TupleInterface
#endif
