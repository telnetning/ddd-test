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
 * dstore_tablespace_struct.h
 *
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/dstore/interface/table/dstore_tablespace_struct.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_TABLE_SPACE_UTILS_H
#define DSTORE_TABLE_SPACE_UTILS_H
#include "page/dstore_page_struct.h"
#include "common/dstore_common_utils.h"

namespace DSTORE {
/* fileId
   [1, 1024] is for temp table tablespace,
   [1025, 2048] is reserved for unlogged table tablespace,
   [2049, 5120] is reserved */
constexpr FileId TMP_TBS_START_FILE_ID = 1;
constexpr FileId TMP_TBS_MAX_FILE_ID = 1024;
constexpr FileId MAX_RESERVED_FILE_ID = 5120;
constexpr FileId START_FILE_ID = MAX_RESERVED_FILE_ID + 1;
constexpr FileId INVALID_DATA_FILE_ID = 0;
constexpr FileId MAX_DATA_FILE_ID = 0xFFFF;
const uint64_t MAX_FILE_SIZE = (32UL << 40); // 32TB
#ifdef UT
constexpr uint32_t MAX_SPACE_FILE_COUNT = 128;
#else
constexpr uint32_t MAX_SPACE_FILE_COUNT = 1024;
#endif

/* tablespaceId
   [1, 7] is for system tablespace,
   [8, 24] is reserved for built-in tablespace,
   [25, 1024] is for customised tablespace */
constexpr TablespaceId SYS_TBS_MAX_ID = 7;
constexpr TablespaceId CUSTOMISED_TABLESPACE_START_ID = 25;
constexpr TablespaceId INVALID_TABLESPACE_ID = 0;
constexpr TablespaceId MAX_TABLESPACE_ID = 1024;
constexpr TablespaceId TABLESPACE_ID_COUNT = 1025;
constexpr uint64_t MAX_TABLESPACE_SIZE = MAX_FILE_SIZE * MAX_SPACE_FILE_COUNT;
constexpr TablespaceId TEMP_TABLESPACE_ID = 5;
constexpr TablespaceId UNLOGGED_TABLESPACE_ID = 7;
/*
 * Used only for locking tablespaces.
 * If the tablespace ID is set to 0xFFFF, all tablespaces in the PDB, that is, the entire tablespaceMgr, are locked.
 */
constexpr TablespaceId LOCK_TAG_TABLESPACE_MGR_ID = 0xFFFF;

constexpr uint16_t MAX_FSM_TREE_PER_RELATION = 660;
constexpr uint16_t INVALID_FSM_ID = MAX_FSM_TREE_PER_RELATION;
constexpr uint8_t DSTORE_TABLESPACE_IS_USED = 1;
constexpr uint8_t DSTORE_TABLESPACE_NOT_USED = 0;
constexpr uint8_t DSTORE_DATAFILE_IS_USED = 1;
constexpr uint8_t DSTORE_DATAFILE_NOT_USED = 0;
constexpr uint8_t NOT_REMAIN_TABLESPACE = 0;
constexpr uint8_t REMAIN_TABLESPACE = 1;
constexpr uint8_t INVALID_REMAIN_TABLESPACE = 2;

constexpr uint8_t NOT_REMAIN_DATAFILE = 0;
constexpr uint8_t REMAIN_DATAFILE = 1;
constexpr uint8_t INVALID_REMAIN_DATAFILE = 2;
#define DSTORE_FILE_NAME_LEN 6

enum class SegmentType : uint8_t {
    HEAP_SEGMENT_TYPE = 0,
    INDEX_SEGMENT_TYPE = 1,
    UNDO_SEGMENT_TYPE = 2,
    HEAP_TEMP_SEGMENT_TYPE = 3,
    INDEX_TEMP_SEGMENT_TYPE = 4
};

typedef struct {
    uint32_t magic;
    uint32_t version;    /* struct version */
    TablespaceId tablespaceId;
    uint16_t hwm;    /* indicates the high watermark of the fileIds array. */
    uint16_t fileNum;    /* current file num. */
    FileId fileIds[MAX_SPACE_FILE_COUNT];
    char tablespaceName[64];
    uint64_t tbsMaxSize;    /* maximum size of a tablespace */
    uint64_t reuseVersion;   /* indicates the reused version of the fileId */
    uint64_t ddlXid;    /* the xid of the transaction that created the tablespace */
    unsigned char used;    /* indicates whether fileId is used */
} DstoreTablespaceAttr;

typedef struct {
    FileId fileId;
    uint16_t extentSize;
    unsigned char used;
    uint32_t magic;
    uint32_t version;
    uint64_t reuseVersion;
    uint64_t ddlXid;
    uint64_t maxSize;
    TablespaceId tablespaceId;
    char fileName[DSTORE_FILE_NAME_LEN];
} DstoreDatafileAttr;

}  // namespace DSTORE
#endif

