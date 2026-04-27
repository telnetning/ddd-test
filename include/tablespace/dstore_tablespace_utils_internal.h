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
 * dstore_tablespace_utils_internal.h
 *
 * IDENTIFICATION
 *        include/tablespace/dstore_tablespace_utils_internal.h
 *
 * ---------------------------------------------------------------------------------------
 */
 
#ifndef DSTORE_DSTORE_TABLESPACE_UTILS_H
#define DSTORE_DSTORE_TABLESPACE_UTILS_H

#include "common/dstore_datatype.h"
namespace DSTORE {

enum ExtentSize : uint16 {
    INVALID_EXT_SIZE = 0,
    EXT_SIZE_8 = 8,
    EXT_SIZE_128 = 128,
    EXT_SIZE_1024 = 1024,
    EXT_SIZE_8192 = 8192,
};
constexpr uint16 EXTENT_TYPE_COUNT = 4;
constexpr ExtentSize EXTENT_SIZE_ARRAY[EXTENT_TYPE_COUNT] = {EXT_SIZE_8, EXT_SIZE_128, EXT_SIZE_1024, EXT_SIZE_8192};

/* Record in tablespace wal structure */
enum ExtentUseType : uint8 {
    EXT_DATA_PAGE_TYPE = 1,
    EXT_FSM_PAGE_TYPE = 2,
    EXT_UNDO_PAGE_TYPE = 3
};

/* First extent in segment is always EXT_SIZE_8 (8 * BLCKSZ) */
constexpr ExtentSize FIRST_EXT_SIZE = EXT_SIZE_8;
constexpr ExtentSize FSM_EXT_SIZE = EXT_SIZE_8;
constexpr uint16 FSM_FREE_LIST_COUNT = 9;
constexpr uint16 FULL_SPACE_LIST_ID = FSM_FREE_LIST_COUNT - 1;
constexpr uint16 HEAP_MAX_MAP_LEVEL = 5;
constexpr uint16 INVALID_FSM_SLOT_NUM = 0xFFFFU;
/* Max page count add to fsm per time */
constexpr uint16 PAGES_ADD_TO_FSM_PER_TIME = 1024;
/* Size of fsm slot is 12 Bytes, refer to struct FsmNodePtr, sizeof(FsmPage)=8120 */
constexpr uint16 FSM_MAX_SLOT = 670;
constexpr uint16 FSM_MAX_HWM = FSM_MAX_SLOT;
constexpr ExtentSize TEMP_TABLE_EXT_SIZE = EXT_SIZE_8;
constexpr int16 INVALID_LAST_ALLOCED_INDEX = -1;

const uint16 FSM_SPACE_LINE[FSM_FREE_LIST_COUNT] = {0, (BLCKSZ >> 7), (BLCKSZ >> 6), (BLCKSZ >> 5), (BLCKSZ >> 4),
                                                    (BLCKSZ >> 3), (BLCKSZ >> 2), (BLCKSZ >> 1), BLCKSZ};

} /* namespace DSTORE */

#endif  // DSTORE_STORAGE_TABLESPACE_UTILS_H
