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
 * dstore_control_struct.h
 *  the basic data structure of the control file.
 *
 *
 * IDENTIFICATION
 *        dstore/include/control/dstore_control_struct.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_CONTROL_STRUCT_H
#define DSTORE_CONTROL_STRUCT_H

#include "common/dstore_datatype.h"
#include "common/algorithm/dstore_string_info.h"

namespace DSTORE {
constexpr uint16 INVALID_OFFSET = USHRT_MAX;
enum ControlGroupType : uint32 {
    CONTROL_GROUP_TYPE_TABLESPACE = 0,
    CONTROL_GROUP_TYPE_WALSTREAM,
    CONTROL_GROUP_TYPE_CSN,
    CONTROL_GROUP_TYPE_RELMAP,
    CONTROL_GROUP_TYPE_PDBINFO,
    CONTROL_GROUP_TYPE_LOGICALREP,
    CONTROL_GROUP_TYPE_MAX
};
/* control file page map
 * 0 : filemetapage
 * 1- 63: metapage
 * 64 - CONTROLFILE_PAGEMAP_MAX-1: data page */
enum ControlFilePageMap {
    CONTROLFILE_PAGEMAP_FILEMETA = 0,
    CONTROLFILE_PAGEMAP_TABLESPACE_META,
    CONTROLFILE_PAGEMAP_WALSTREAM_META,
    CONTROLFILE_PAGEMAP_CSN_META,
    CONTROLFILE_PAGEMAP_RELMAP_META,
    CONTROLFILE_PAGEMAP_PDBINFO_META,
    CONTROLFILE_PAGEMAP_LOGICALREP_META,
    CONTROLFILE_PAGEMAP_METAPAGE_MAX = 63,
    CONTROLFILE_PAGEMAP_TABLESPACE_START = 64,
    CONTROLFILE_PAGEMAP_TABLESPACE_MAX = CONTROLFILE_PAGEMAP_TABLESPACE_START + 1240,
    CONTROLFILE_PAGEMAP_WALSTREAM_START,
    CONTROLFILE_PAGEMAP_WALSTREAM_MAX = CONTROLFILE_PAGEMAP_WALSTREAM_START + 1920,
    CONTROLFILE_PAGEMAP_CSN_START,
    CONTROLFILE_PAGEMAP_CSN_MAX,
    CONTROLFILE_PAGEMAP_RELMAP_START,
    CONTROLFILE_PAGEMAP_RELMAP_MAX = CONTROLFILE_PAGEMAP_RELMAP_START + 4,
    CONTROLFILE_PAGEMAP_PDBINFO_START,
    CONTROLFILE_PAGEMAP_PDBINFO_MAX = CONTROLFILE_PAGEMAP_PDBINFO_START + 32,
    CONTROLFILE_PAGEMAP_LOGICALREP_START,
    CONTROLFILE_PAGEMAP_LOGICALREP_MAX = CONTROLFILE_PAGEMAP_LOGICALREP_START + 512,
    CONTROLFILE_PAGEMAP_MAX = 3840    /* 3840 * 8K = 30M  Align to 1MB boundary */
};

enum ControlPageType : uint16 {
    CONTROL_PAGE_TYPE_INVALID = 0,
    CONTROL_FILE_MATAPAGE_TYPE,
    CONTROL_TBS_METAPAGE_TYPE,
    CONTROL_TBS_TABLESPACE_DATAPAGE_TYPE,
    CONTROL_TBS_DATAFILE_DATAPAGE_TYPE,
    CONTROL_WAL_STREAM_METAPAGE_TYPE,
    CONTROL_WAL_STREAM_DATAPAGE_TYPE,
    CONTROL_CSN_METAPAGE_TYPE,
    CONTROL_CSN_DATAPAGE_TYPE,
    CONTROL_RELMAP_METAPAGE_TYPE,
    CONTROL_RELMAP_SHARED_DATAPAGE_TYPE,
    CONTROL_RELMAP_LOCAL_DATAPAGE_TYPE,
    CONTROL_PDBINFO_METAPAGE_TYPE,
    CONTROL_PDBINFO_DATAPAGE_TYPE,
    CONTROL_LOGICALREP_METAPAGE_TYPE,
    CONTROL_LOGICALREP_DATAPAGE_TYPE,
    /* make sure this one is the last item */
    CONTROL_MAX_PAGE_TYPE
};

enum DeployType {
    CONTROL_FILE_SINGLE_NODE = 0,
    CONTROL_FILE_DISTRIBUTE,
    CONTROL_FILE_DEPLOYTYPE_INVALID = 100,
};

enum MetaPageCheckResult {
    FIRST_META_PAGE_IS_VALID,
    SECOND_META_PAGE_IS_VALID,
    BOTH_META_PAGES_ARE_VALID,
    NO_VALID_META_PAGE
};

struct ControlPageRange {
    BlockNumber m_start;
    BlockNumber m_end;
} PACKED;

const uint64 MAGIC_NUMBER = 0x436F6E74726F6C65;   /* hex of string "CtrlFile" */
constexpr uint8 CONTROL_FILE_GROUP_WRITING = 1;
constexpr uint8 CONTROL_FILE_GROUP_WRITE_DONE = 0;
constexpr uint32 CONTROL_MAX_PAGERANGE_NUM = 8;
/* GaussStor server supports reading/writing at most 64 blocks at a time 64 * BLCKSZ = 512KB */
constexpr uint32 CONTROL_WRITE_ONCE_BLOCK_COUNT = 64;
struct ControlMetaHeader {
    uint64 m_term;        /* term of the group */
    uint32 m_lastPageId;  /* the last page id of the pagerange[0],  */
    uint32 m_version;
    uint8 m_flag;         /* 0: not writing, 1: writing */
    uint8 m_reserved[7];
    ControlPageRange m_pageRange[CONTROL_MAX_PAGERANGE_NUM];
    static void Dump(void *item, StringInfoData &dumpInfo)
    {
        ControlMetaHeader *data = static_cast<ControlMetaHeader *>(item);
        dumpInfo.append("term %lu, flag %hhu lastPageId %u\n", data->m_term, data->m_flag, data->m_lastPageId);
        for (uint32 i = 0; i < CONTROL_MAX_PAGERANGE_NUM; i++) {
            if (data->m_pageRange[i].m_start != DSTORE_INVALID_BLOCK_NUMBER) {
                dumpInfo.append("pageRange[%d] start %u end %u\n", i, data->m_pageRange[i].m_start,
                                data->m_pageRange[i].m_end);
            }
        }
    }
} PACKED;

struct ControlFileMetaData {
    uint32 m_version;
    uint8 maxPageType; /* The max page type of control file */
    uint8 usedGroupType; /* The actual used group of control file */
    uint8 m_pad[2]; /* Padding to 32bit */
    BlockNumber totalPageCount; /* The actual page count of control file */
    BlockNumber usedPageCount;  /* Where has been used in the control file */
    uint64 magic;
} PACKED;

struct ControlPageHeader {
    uint32 m_checksum;
    uint32 m_magic;
    uint16 m_pageType;
    uint16 m_dataOffset;  /* offset of the data area */
    BlockNumber m_nextPage;
    uint32 m_version;
    uint16 m_writeOffset;
    uint16 m_reserved;
};

struct ControlMetaPageHeader {
    ControlPageHeader pageHeader;
    ControlMetaHeader metaHeader;
};

struct ControlPageBuffer {
    BlockNumber pageId;
    char *page;
} PACKED;

struct ControlPageTypeInfo {
    ControlPageType type;
    uint32_t size;
    void (*metadump)(void *, StringInfoData&);
    void (*dump)(void *, StringInfoData&);
    BlockNumber initialBlockNo;
};
extern const ControlPageTypeInfo CONTROL_PAGE_TYPE_INFOS[CONTROL_MAX_PAGE_TYPE];
} // namespace DSTORE
#endif // DSTORE_CONTROL_STRUCT_H