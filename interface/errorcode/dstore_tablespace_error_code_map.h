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
 */

#ifndef DSTORE_TABLESPACE_ERROR_CODE_MAP_H
#define DSTORE_TABLESPACE_ERROR_CODE_MAP_H

#include "errorcode/dstore_tablespace_error_code.h"

namespace DSTORE {

static ErrorDetails g_tablespace_error_code_map[] = {
    [STORAGE_OK] = {"STORAGE_OK", ""},
    [ERROR_GET_CODE(TBS_ERROR_SEGMENT_USE_AFTER_DROP)] =
        {"TBS_ERROR_SEGMENT_USE_AFTER_DROP", "Segment used after segment is dropped"},
    [ERROR_GET_CODE(TBS_ERROR_DATA_SEGMENT_TYPE_INVALID)] =
        {"TBS_ERROR_DATA_SEGMENT_TYPE_INVALID", "DataSegment init type invalid"},
    [ERROR_GET_CODE(TBS_ERROR_SEGMENT_PARAMETER_INVALID)] =
        {"TBS_ERROR_SEGMENT_PARAMETER_INVALID", "Segment init parameters invalid"},
    [ERROR_GET_CODE(TBS_ERROR_PAGE_READ_FAILED)] =
        {"TBS_ERROR_PAGE_READ_FAILED", "Page read failed from buffer manager"},
    [ERROR_GET_CODE(TBS_ERROR_SEGMENT_IS_NOT_INIT)] =
        {"TBS_ERROR_SEGMENT_IS_NOT_INIT", "Segment is not init"},
    [ERROR_GET_CODE(TBS_ERROR_DATA_PAGE_INIT_CALLBACK_INVALID)] =
        {"TBS_ERROR_DATA_PAGE_INIT_CALLBACK_INVALID", "DataPage init callback function is nullptr"},
    [ERROR_GET_CODE(TBS_ERROR_SEGMENT_EXTENT_COUNT_REACH_MAX)] =
        {"TBS_ERROR_SEGMENT_EXTENT_COUNT_REACH_MAX", "Segment Extent count has reached maximum"},
    [ERROR_GET_CODE(TBS_ERROR_TABLESPACE_ALLOC_EXTENT_FAILED)] =
        {"TBS_ERROR_TABLESPACE_ALLOC_EXTENT_FAILED", "Tablespace alloc extent failed"},
    [ERROR_GET_CODE(TBS_ERROR_PAGE_REQUEST_SIZE_INVALID)] =
        {"TBS_ERROR_PAGE_REQUEST_SIZE_INVALID", "Invalid page request size"},
    [ERROR_GET_CODE(TBS_ERROR_FSM_TREE_LEVEL_REACH_MAX)] =
        {"TBS_ERROR_FSM_TREE_LEVEL_REACH_MAX", "FSM tree reaches maximum level"},
    [ERROR_GET_CODE(TBS_ERROR_SEGMENT_HAS_NO_SPACE)] =
        {"TBS_ERROR_SEGMENT_HAS_NO_SPACE", "Segment has no enough space to get new page"},
    [ERROR_GET_CODE(TBS_ERROR_INIT_FAILED)] =
        {"TBS_ERROR_INIT_FAILED", "Init tablespace failed"},
    [ERROR_GET_CODE(TBS_ERROR_ALLOC_EXTENT_FAILED)] =
        {"TBS_ERROR_ALLOC_EXTENT_FAILED", "Alloc extent from tablespace failed"},
    [ERROR_GET_CODE(TBS_ERROR_PAGEID_INVALID)] =
        {"TBS_ERROR_PAGEID_INVALID", "PageId of extent is invalid"},
    [ERROR_GET_CODE(TBS_ERROR_EXTENT_SIZE_INVALID)] =
        {"TBS_ERROR_EXTENT_SIZE_INVALID", "Size of extent is invalid"},
    [ERROR_GET_CODE(TBS_ERROR_FILE_SIZE_EXCEED_LIMIT)] =
        {"TBS_ERROR_FILE_SIZE_EXCEED_LIMIT", "File size has exceeded the limit"},
    [ERROR_GET_CODE(TBS_ERROR_FILE_BITMAP_GROUP_USE_UP)] =
        {"TBS_ERROR_FILE_BITMAP_GROUP_USE_UP", "The bitmap group in the file has been used up"},
    [ERROR_GET_CODE(TBS_ERROR_FILE_SIZE_EXCEED_MAX_LIMIT)] =
        {"TBS_ERROR_FILE_SIZE_EXCEED_MAX_LIMIT", "File size has exceeded the max limit"},
    [ERROR_GET_CODE(TBS_ERROR_FILE_COUNT_EXCEED_LIMIT)] =
        {"TBS_ERROR_FILE_COUNT_EXCEED_LIMIT", "File count has exceeded the limit of tablespace"},
    [ERROR_GET_CODE(TBS_ERROR_TABLESPACE_USE_UP)] =
        {"TBS_ERROR_TABLESPACE_USE_UP", "The space of the tablespace has been used up"},
    [ERROR_GET_CODE(TBS_ERROR_PARAMETER_INVALID)] =
        {"TBS_ERROR_PARAMETER_INVALID", "The input parameter is invalid"},
    [ERROR_GET_CODE(TBS_ERROR_FILE_EXIST)] =
        {"TBS_ERROR_FILE_EXIST", "File has existed in tablespace"},
    [ERROR_GET_CODE(TBS_ERROR_SPACE_EXIST)] =
        {"TBS_ERROR_SPACE_EXIST", "Tablespace has been created"},
    [ERROR_GET_CODE(TBS_ERROR_SPACE_NOT_EXIST)] =
        {"TBS_ERROR_SPACE_NOT_EXIST", "Tablespace not exists"},
    [ERROR_GET_CODE(TBS_ERROR_SPACE_HAS_DROPPED)] =
        {"TBS_ERROR_SPACE_HAS_DROPPED", "Tablespace has been dropped"},
    [ERROR_GET_CODE(TBS_ERROR_FILE_NOT_OPEN)] =
        {"TBS_ERROR_FILE_NOT_OPEN", "The file is not open"},
    [ERROR_GET_CODE(TBS_ERROR_FILE_NOT_FOUND)] =
        {"TBS_ERROR_FILE_NOT_FOUND", "The file is not found in tablespace"},
    [ERROR_GET_CODE(TBS_ERROR_ACQUIRE_LOCK_FAILED)] =
        {"TBS_ERROR_ACQUIRE_LOCK_FAILED", "Acquire lock failed"},
    [ERROR_GET_CODE(TBS_ERROR_TABLESPACE_DELETE_PART_FILES)] =
        {"TBS_ERROR_TABLESPACE_DELETE_PART_FILES", "The part of files in tablespace has been deleted"},
    [ERROR_GET_CODE(TBS_ERROR_TABLESPACE_DAMAGED)] =
        {"TBS_ERROR_TABLESPACE_DAMAGED", "The tablespace has been damaged"},
    [ERROR_GET_CODE(TBS_ERROR_TABLESPACE_NOT_EXIST)] =
        {"TBS_ERROR_TABLESPACE_NOT_EXIST", "The tablespace does not exist"},
    [ERROR_GET_CODE(TBS_ERROR_FREESPACEMAP_ASSIGNMENT_FAILED)] =
        {"TBS_ERROR_FREESPACEMAP_ASSIGNMENT_FAILED", "The PartitionFreeSpaceMap assignment failed"},
    [ERROR_GET_CODE(TBS_ERROR_PDB_FENCE_WRITE)] =
        {"TBS_ERROR_PDB_FENCE_WRITE", "Only primary pdb can allocate segment"},
    [ERROR_GET_CODE(TBS_ERROR_STORESPACE_IS_INVALID)] =
        {"TBS_ERROR_STORESPACE_IS_INVALID", "The name of storespace is invalid"},
};

static_assert(sizeof(g_tablespace_error_code_map) != TBS_ERROR_MAX, "Error code map size doesn't match");

} /* namespace DSTORE */

#endif
