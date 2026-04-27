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
 * dstore_page_struct.h
 *
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/dstore/interface/page/dstore_page_struct.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_PAGE_UTILS_H
#define DSTORE_PAGE_UTILS_H

#include "common/dstore_common_utils.h"
namespace DSTORE {

enum class PageType : uint8_t {
    INVALID_PAGE_TYPE = 0,
    HEAP_PAGE_TYPE,
    INDEX_PAGE_TYPE,
    TRANSACTION_SLOT_PAGE,
    UNDO_PAGE_TYPE,
    FSM_PAGE_TYPE,
    FSM_META_PAGE_TYPE,
    DATA_SEGMENT_META_PAGE_TYPE,
    HEAP_SEGMENT_META_PAGE_TYPE,
    UNDO_SEGMENT_META_PAGE_TYPE,
    TBS_EXTENT_META_PAGE_TYPE,
    TBS_BITMAP_PAGE_TYPE,
    TBS_BITMAP_META_PAGE_TYPE,
    TBS_FILE_META_PAGE_TYPE,
    BTR_QUEUE_PAGE_TYPE,
    BTR_RECYCLE_PARTITION_META_PAGE_TYPE,
    BTR_RECYCLE_ROOT_META_PAGE_TYPE,
    TBS_SPACE_META_PAGE_TYPE,
    MAX_PAGE_TYPE
};

struct PageId {
    FileId m_fileId;
    BlockNumber m_blockId;

    bool operator==(const PageId &pageId) const
    {
        return this->m_blockId == pageId.m_blockId && this->m_fileId == pageId.m_fileId;
    }

    bool operator!=(const PageId &pageId) const
    {
        return !(*this == pageId);
    }

    bool operator<(const PageId &pageId) const
    {
        return this->m_fileId < pageId.m_fileId ||
               (this->m_fileId == pageId.m_fileId && this->m_blockId < pageId.m_blockId);
    }

    inline bool IsValid() const
    {
        return m_fileId != INVALID_VFS_FILE_ID && m_blockId != DSTORE_INVALID_BLOCK_NUMBER;
    }

    inline bool IsInvalid() const
    {
        return m_fileId == INVALID_VFS_FILE_ID || m_blockId == DSTORE_INVALID_BLOCK_NUMBER;
    }
} __attribute__((packed));


constexpr const PageId INVALID_PAGE_ID = {INVALID_VFS_FILE_ID, DSTORE_INVALID_BLOCK_NUMBER};

}  // namespace DSTORE

#endif
