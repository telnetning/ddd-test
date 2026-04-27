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
 * dstore_itemid.h
 *
 * IDENTIFICATION
 *        include/page/dstore_itemid.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_DSTORE_ITEMID_H
#define DSTORE_DSTORE_ITEMID_H

#include "common/dstore_datatype.h"
#include "page/dstore_itemptr.h"
#include "tuple/dstore_data_tuple.h"

namespace DSTORE {

/*
 * ItemId "flags" has these possible states.  An UNUSED item id is available
 * for immediate re-use, the other states are not.
 */
enum ItemIdState : uint8 {
    ITEM_ID_UNUSED = 0,    /* unused (should always have len = 0) */
    ITEM_ID_NORMAL,        /* used (should always have len > 0) */
    ITEM_ID_UNREADABLE_RANGE_HOLDER,   /* Rollbacked tuple. Keep it on page only to show the key range of the page.
                                        * The tuple itself is no longer accessable at all. Should skip it when scan
                                        * because it would always be invisible. */
    ITEM_ID_NO_STORAGE     /* The space of tuple data has been recycled */
};

/* just log the difference between the old itemId and the new one for wal */
struct ItemIdDiff {
    OffsetNumber offNum;
    ItemIdState newState;
};

struct ItemId {
    union {
        /* Default ItemId structure. */
        ItemType m_placeHolder;
        struct {
            uint32 m_flags : 2;
            uint32 m_offset : 15;
            uint32 m_len : 15;
        } direct;

        /*
         * If flags == ITEM_ID_NO_STORAGE, then ItemId structure is defined as "redirect" below.
         * Redirect ItemId points to a pruned row, so has no actual storage but td id instead,
         * this helps in fetching the tuple from undo when required.
         */
        struct {
            uint32 m_flags : 2;
            uint32 m_tdId : 8;
            uint32 m_tdStatus : 2;
            uint32 m_tupLiveMode : 3;
            uint32 m_unused : 17;
        } redirect;
    };

    inline bool IsUnused() const
    {
        return direct.m_flags == static_cast<uint32>(ITEM_ID_UNUSED);
    }

    inline void SetUnused()
    {
        direct.m_flags = static_cast<uint32>(ITEM_ID_UNUSED);
        direct.m_offset = 0;
        direct.m_len = 0;
    }

    inline void SetNormal(uint16 off, uint16 size)
    {
        direct.m_offset = off;
        direct.m_len = size;
        direct.m_flags = static_cast<uint32>(ITEM_ID_NORMAL);
    }

    inline bool IsNormal() const
    {
        return direct.m_flags == static_cast<uint32>(ITEM_ID_NORMAL);
    }

    inline void SetNoStorage()
    {
        direct.m_flags = static_cast<uint32>(ITEM_ID_NO_STORAGE);
        direct.m_len = 0;
    }

    inline bool IsNoStorage() const
    {
        return direct.m_flags == static_cast<uint32>(ITEM_ID_NO_STORAGE);
    }

    inline uint32 GetFlags() const
    {
        return direct.m_flags;
    }

    inline void MarkUnreadableAndRangeholder()
    {
        direct.m_flags = ITEM_ID_UNREADABLE_RANGE_HOLDER;
    }

    inline bool IsRangePlaceholder() const
    {
        return direct.m_flags == static_cast<uint32>(ITEM_ID_UNREADABLE_RANGE_HOLDER);
    }

    inline void SetLen(uint32 size)
    {
        direct.m_len = size;
    }

    inline uint16 GetLen() const
    {
        return static_cast<uint16>(direct.m_len);
    }

    inline void SetOffset(OffsetNumber off)
    {
        direct.m_offset = off;
    }

    inline OffsetNumber GetOffset() const
    {
        return static_cast<OffsetNumber>(direct.m_offset);
    }

    inline bool HasStorage() const
    {
        return direct.m_len > 0;
    }

    inline void SetTdId(uint8 tdId)
    {
        redirect.m_tdId = tdId;
    }

    inline uint8 GetTdId() const
    {
        return static_cast<uint8>(redirect.m_tdId);
    }

    inline TupleTdStatus GetTdStatus() const
    {
        return static_cast<TupleTdStatus>(redirect.m_tdStatus);
    }

    inline void SetTdStatus(TupleTdStatus stat)
    {
        redirect.m_tdStatus = static_cast<uint32>(stat);
    }

    inline bool TestTdStatus(TupleTdStatus stat) const
    {
        return redirect.m_tdStatus == static_cast<uint32>(stat);
    }

    inline uint32 GetTupLiveMode() const
    {
        return redirect.m_tupLiveMode;
    }

    inline void SetTupLiveMode(uint32 mode)
    {
        redirect.m_tupLiveMode = mode;
    }

    static int DescendingSortByOffsetCompare(const void *a, const void *b)
    {
        const ItemId *itemA = *static_cast<const ItemId *const *>((a));
        const ItemId *itemB = *static_cast<const ItemId *const *>((b));
        return (itemA->GetOffset() > itemB->GetOffset()) ? (-1) : 1;
    }
};

}  // namespace DSTORE

#endif  // DSTORE_STORAGE_ITEMID_H
