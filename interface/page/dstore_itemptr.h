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
 * dstore_itemptr.h
 *     This file defines the building blocks that bg page writer needs.
 *
 * IDENTIFICATION
 *        interface/page/heap/dstore_itemptr.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_DSTORE_ITEMPTR_H
#define DSTORE_DSTORE_ITEMPTR_H

#include "common/dstore_common_utils.h"
#include "page/dstore_page_struct.h"

namespace DSTORE {

using OffsetNumber = uint16_t;
constexpr int INVALID_ITEM_OFFSET_NUMBER = 0;
constexpr OffsetNumber FIRST_ITEM_OFFSET_NUMBER = 1;
constexpr OffsetNumber MAX_ITEM_OFFSET_NUMBER = (static_cast<OffsetNumber>(BLCKSZ / sizeof(ItemType)));

/*
 * ItemPointerData:
 * 16 bits: fileID
 * 32 bits: pageID
 * 16 bits: offset
 *
 * The type size is 64 bits, equal to an uint64 variable. Sometimes we need to update the three variables atomically.
 * Thus, we use a union to define the ItemPointerData type so that we can use the uint64 variable if necessary.
 */
union ItemPointerData {
    uint64_t m_placeHolder;
    struct Value {
        PageId m_pageid;
        OffsetNumber m_offset;
    } val;
    static_assert((sizeof(Value)) == 8, "Value size needs equal 8 Bytes.");

    ItemPointerData() = default;

    explicit ItemPointerData(uint64_t rawData) noexcept : m_placeHolder(rawData)
    {}

    ItemPointerData(const PageId pageId, OffsetNumber offset)
    {
        val.m_pageid = pageId;
        val.m_offset = offset;
    }
    inline PageId GetPageId() const
    {
        return val.m_pageid;
    }

    inline void SetPageId(const PageId id)
    {
        val.m_pageid = id;
    }

    inline BlockNumber GetBlockNum() const
    {
        /* m_blockId */
        return val.m_pageid.m_blockId;
    }

    inline OffsetNumber GetOffset() const
    {
        return val.m_offset;
    }

    inline FileId GetFileId() const
    {
        return val.m_pageid.m_fileId;
    }

    inline void SetFileId(FileId fileId)
    {
        val.m_pageid.m_fileId = fileId;
    }

    inline void SetBlockNumber(BlockNumber blockNumber)
    {
        val.m_pageid.m_blockId = blockNumber;
    }

    inline void SetOffset(OffsetNumber offset)
    {
        val.m_offset = offset;
    }

    bool operator==(const ItemPointerData &pointerData) const
    {
        return this->m_placeHolder == pointerData.m_placeHolder;
    }

    bool operator!=(const ItemPointerData &pointerData) const
    {
        return this->m_placeHolder != pointerData.m_placeHolder;
    }

    inline static int32_t Compare(ItemPointerData *arg1, ItemPointerData *arg2)
    {
        if (arg1 == nullptr || arg2 == nullptr) {
            return 0;
        }
    
        FileId fileId1 = arg1->GetFileId();
        FileId fileId2 = arg2->GetFileId();
        if (fileId1 < fileId2) {
            return -1;
        } else if (fileId1 > fileId2) {
            return 1;
        } else {
            BlockNumber b1 = arg1->GetBlockNum();
            BlockNumber b2 = arg2->GetBlockNum();
            OffsetNumber o1 = arg1->GetOffset();
            OffsetNumber o2 = arg2->GetOffset();

            if (b1 < b2) {
                return -1;
            } else if (b1 > b2) {
                return 1;
            } else if (o1 < o2) {
                return -1;
            } else if (o1 > o2) {
                return 1;
            } else {
                return 0;
            }
        }
    }
    uint8_t GetCompressedSize() const;
    void Serialize(char* &data) const;
    void Deserialize(const char* &data);
    DSTORE_EXPORT void Dump(char *buf, uint32_t len) const;
};

using ItemPointer = ItemPointerData *;

const ItemPointerData INVALID_ITEM_POINTER(-1);

}  // namespace DSTORE
#endif  // DSTORE_STORAGE_ITEMPTR_H
