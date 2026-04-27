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
 * dstore_scankey.h
 *
 * IDENTIFICATION
 *        dstore/interface/index/dstore_scankey.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_SKEY_H
#define DSTORE_SKEY_H

#include "catalog/dstore_function_struct.h"

namespace DSTORE {

struct ScanKeyData {
    uint32_t skFlags;    /* flags, see below */
    int16_t skAttno;     /* table or index column number */
    uint16_t skStrategy; /* operator strategy number */
    Oid skSubtype;       /* strategy subtype */
    Oid skCollation;     /* collation to use, if needed */
    FmgrInfo skFunc;
    Datum skArgument; /* data to compare */
};

using ScanKey = ScanKeyData*;

/*
 * ScanKeyData skFlags
 *
 * bits 0-15 of variable skFlags are reserved for system-wide use. Bits 16-31 are reserved for use within
 * individual index access methods.
 */
constexpr uint32_t SCAN_KEY_ISNULL = 1;             /* skArgument is NULL */
constexpr uint32_t SCAN_KEY_UNARY = 1 << 1;
constexpr uint32_t SCAN_KEY_ROW_HEADER = 1 << 2;    /* row comparison header */
constexpr uint32_t SCAN_KEY_ROW_MEMBER = 1 << 3;    /* row comparison member */
constexpr uint32_t SCAN_KEY_ROW_END = 1 << 4;       /* last row comparison member */
constexpr uint32_t SCAN_KEY_SEARCHARRAY = 1 << 5;   /* scankey represents ScalarArrayOp */
constexpr uint32_t SCAN_KEY_SEARCHNULL = 1 << 6;    /* scankey represents "col IS NULL" */
constexpr uint32_t SCAN_KEY_SEARCHNOTNULL = 1 << 7; /* scankey represents "col IS NOT NULL" */
constexpr uint32_t SCAN_KEY_ORDER_BY = 1 << 8;      /* scankey is for ORDER BY op */

}  // namespace DSTORE

#endif /* STORAGE_SKEY_H */
