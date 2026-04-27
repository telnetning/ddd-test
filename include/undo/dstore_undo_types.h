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
 * dstore_undo_types.h
 *
 * IDENTIFICATION
 *        include/undo/dstore_undo_types.h
 *
 * ---------------------------------------------------------------------------------------
 */
 
#ifndef DSTORE_DSTORE_UNDO_TYPES_H
#define DSTORE_DSTORE_UNDO_TYPES_H

#include "common/dstore_datatype.h"
#include "page/dstore_itemptr.h"

namespace DSTORE {

enum UndoType : uint8 {
    UNDO_HEAP_INSERT = 0,
    UNDO_HEAP_BATCH_INSERT,
    UNDO_HEAP_DELETE,
    UNDO_HEAP_INPLACE_UPDATE,
    UNDO_HEAP_SAME_PAGE_APPEND_UPDATE,
    UNDO_HEAP_ANOTHER_PAGE_APPEND_UPDATE_OLD_PAGE,
    UNDO_HEAP_ANOTHER_PAGE_APPEND_UPDATE_NEW_PAGE,
    UNDO_HEAP_BOUND,
    UNDO_BTREE_INSERT,
    UNDO_BTREE_DELETE,
    UNDO_BTREE_BOUND,
    UNDO_HEAP_INSERT_TMP,
    UNDO_HEAP_BATCH_INSERT_TMP,
    UNDO_HEAP_DELETE_TMP,
    UNDO_HEAP_INPLACE_UPDATE_TMP,
    UNDO_HEAP_SAME_PAGE_APPEND_UPDATE_TMP,
    UNDO_HEAP_ANOTHER_PAGE_APPEND_UPDATE_OLD_PAGE_TMP,
    UNDO_HEAP_ANOTHER_PAGE_APPEND_UPDATE_NEW_PAGE_TMP,
    UNDO_HEAP_BOUND_TMP,
    UNDO_BTREE_INSERT_TMP,
    UNDO_BTREE_DELETE_TMP,
    UNDO_BTREE_BOUND_TMP,
    UNDO_UNKNOWN
};

/* undo record address; physical address
 * 16 bits: fileID
 * 32 bits: pageID
 * 16 bits: offset
 */
using UndoRecPtr = ItemPointerData;
using ZoneId = int32;

/* Special value for undo record pointer which indicates that it is invalid. */
const UndoRecPtr INVALID_UNDO_RECORD_PTR = INVALID_ITEM_POINTER;

#ifndef UT
constexpr ZoneId UNDO_ZONE_COUNT = 1024 * 1024;
#else
constexpr ZoneId UNDO_ZONE_COUNT = 512;
#endif

constexpr ZoneId INVALID_ZONE_ID = -1;

constexpr int ALLOCSET_UNDO_MAXSIZE = 300 * UNDO_ZONE_COUNT;

inline bool IS_VALID_ZONE_ID(ZoneId zid)
{
    return zid >= 0 && zid < UNDO_ZONE_COUNT;
}
}  // namespace DSTORE

#endif  // DSTORE_STORAGE_UNDO_TYPES_H