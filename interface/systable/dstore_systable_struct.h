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
 * Description:
 * This header file defines the external structure of the system table.
 */
#ifndef DSTORE_SYSTABLE_STRUCT_H
#define DSTORE_SYSTABLE_STRUCT_H

#include <cstdint>
#include "common/dstore_common_utils.h"
#include "systable/systable_attribute.h"
#include "securec.h"

namespace DSTORE {
struct TableColumn {
    const char *name;
    Oid typeOid;
};
struct SysTablePageIdMap {
    Oid relOid;
    FileId relfileid;
    BlockNumber relblknum;
};
struct SysTableDef {
    const char *name;
    Oid oid;
    Oid rowType;
    const TableColumn *cols;
    int colsCnt;
};
struct SysTableCreateStmt {
    const char *name;
    Oid presetOid;
    Oid rowType;
    const SysAttributeTupDef *cols;
    uint colsCnt;
    SysTableCreateStmt(const char tableName[], Oid tableOid, Oid type, const SysAttributeTupDef *tableCols, int count)
        : name(tableName), presetOid(tableOid), rowType(type), cols(tableCols), colsCnt(count)
    {
    }
};

struct SysTypeCreateStmt {
    Oid newTypeOid;
    const char *typname;
    Oid typeNamespace;
    Oid relationOid; /* only for relation rowtypes */
    char relationKind;
    Oid ownerId;
    int16_t internalSize;
    char typeType;
    char typeCategory;
    bool typePreferred;
    char typDelim;
    /*
     * I/O conversion procedures for the datatype.
     */
    Oid typeInput;
    Oid typeOutput;
    Oid typeReceive;
    Oid typeSend;
    /*
     * I/O functions for optional type modifiers.
     */
    Oid typeModIn;
    Oid typeModOut;
    Oid typeAnalyze;
    Oid elementType;
    bool isImplicitArray;
    Oid arrayType;
        /*
     * Domains use typbasetype to show the base (or domain) type that the
     * domain is based on.	Zero if the type is not a domain.
     */
    Oid baseType;
    const char *defaultTypeValue; /* human readable rep */
    char *defaultTypeBin;         /* cooked rep */
    bool passedByValue;
    /* ----------------
	 * alignment is the alignment required when storing a value of this
	 * type.  It applies to storage on disk as well as most
	 * representations of the value inside openGauss.  When multiple values
	 * are stored consecutively, such as in the representation of a
	 * complete row on disk, padding is inserted before a datum of this
	 * type so that it begins on the specified boundary.  The alignment
	 * reference is the beginning of the first datum in the sequence.
	 *
	 * 'c' = CHAR alignment, ie no alignment needed.
	 * 's' = SHORT alignment (2 bytes on most machines).
	 * 'i' = INT alignment (4 bytes on most machines).
	 * 'd' = DOUBLE alignment (8 bytes on many machines, but by no means all).
	 *
	 * See include/access/tupmacs.h for the macros that compute these
	 * alignment requirements.	Note also that we allow the nominal alignment
	 * to be violated when storing "packed" varlenas; the TOAST mechanism
	 * takes care of hiding that from most code.
	 *
	 * NOTE: for types used in system tables, it is critical that the
	 * size and alignment defined in systable_type agree with the way that the
	 * compiler will lay out the field in a struct representing a table row.
	 * ----------------
	 */
    char alignment;
        /*
     * Domains use typeTypeMod to record the typeMod to be applied to their base
     * type (-1 if base type does not use a typeMod).  -1 if this type is not a
     * domain.
     */
    int32_t typeMod;
    int32_t typNDims; /* Array dimensions for baseType */
    bool typeNotNull;
    Oid typeCollation;
};
struct SysTableInsertStmt {};
struct SysTableDeclareIndexStmt {};

struct SysScanDescData;
using SysScanDesc = struct SysScanDescData *;

}  // namespace DSTORE
#endif