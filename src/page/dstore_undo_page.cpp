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
 * dstore_undo_page.h
 *
 * IDENTIFICATION
 *        src/page/dstore_undo_page.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "page/dstore_undo_page.h"
#include "page/dstore_undo_segment_meta_page.h"
#include "transaction/dstore_transaction_types.h"

namespace DSTORE {

char* TransactionSlotPage::Dump()
{
    StringInfoData str;
    bool res = str.init();
    if (unlikely(!res)) {
        return nullptr;
    }
    /* Step 1: Dump header */
    Page::DumpHeader(&str);
    str.append(", version %u\n", GetVersion());
    str.append(", next free logic slot id: %lu", GetNextFreeLogicSlotId());
    str.append("\n");

    /* Step 2: Dump transaction slot information */
    for (int32 i = 0; i < TRX_PAGE_SLOTS_NUM; ++i) {
        m_slots[i].Dump(&str);
    }
    return str.data;
}

char* UndoRecordPage::Dump()
{
    StringInfoData str;
    str.init();
    /* Dump header */
    Page::DumpHeader(&str);
    str.append("version %u\n", GetVersion());
    str.append("current page id is (%hu, %u)\n", m_undoRecPageHeader.cur.m_fileId, m_undoRecPageHeader.cur.m_blockId);
    str.append("prev page id is (%hu, %u)\n", m_undoRecPageHeader.prev.m_fileId, m_undoRecPageHeader.prev.m_blockId);
    str.append("next page id is (%hu, %u)\n", m_undoRecPageHeader.next.m_fileId, m_undoRecPageHeader.next.m_blockId);
    return str.data;
}

} /* The end of namespace DSTORE */
