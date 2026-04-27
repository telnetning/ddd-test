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
 *        include/page/dstore_undo_page.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_UNDO_PAGE_H
#define DSTORE_UNDO_PAGE_H

#include "page/dstore_page.h"
#include "undo/dstore_transaction_slot.h"

namespace DSTORE {

struct TransactionSlotPageHeader {
    uint32 m_version;
    uint32  m_pad;
    uint64 m_nextFreeLogicSlotId; /* Only the maximum value on all txn pages is meaningful. */
} PACKED;

const int32 TRX_PAGE_HEADER_SIZE = (sizeof(Page) + sizeof(TransactionSlotPageHeader));
const int32 TRX_PAGE_SLOTS_NUM = (BLCKSZ - TRX_PAGE_HEADER_SIZE) / TRX_SLOT_SIZE;
const int32 TRX_PAGE_PAD = ((BLCKSZ - TRX_PAGE_HEADER_SIZE) - TRX_PAGE_SLOTS_NUM * sizeof(TransactionSlot));

struct TransactionSlotPage : public Page {
    TransactionSlotPageHeader m_undoTrxPageHeader;
    TransactionSlot m_slots[TRX_PAGE_SLOTS_NUM];
    char m_pad[TRX_PAGE_PAD];

    void InitTxnSlotPage(const PageId &selfPageId)
    {
        Page::Init(0, PageType::TRANSACTION_SLOT_PAGE, selfPageId);
        m_undoTrxPageHeader.m_nextFreeLogicSlotId = 0;
        m_undoTrxPageHeader.m_version = 0;
        m_undoTrxPageHeader.m_pad = 0;
    }

    static inline uint16 GetSlotId(uint64 logicSlotId)
    {
        return static_cast<uint16>(logicSlotId % TRX_PAGE_SLOTS_NUM);
    }

    inline TransactionSlot *GetTransactionSlot(uint32 id)
    {
        return &m_slots[id];
    }

    inline uint64 GetNextFreeLogicSlotId() const
    {
        return m_undoTrxPageHeader.m_nextFreeLogicSlotId;
    }

    inline uint32 GetVersion() const
    {
        return m_undoTrxPageHeader.m_version;
    }

    inline void SetNextFreeLogicSlotId(uint64 id)
    {
        m_undoTrxPageHeader.m_nextFreeLogicSlotId = id;
    }

    char* Dump();

    static void PrevDumpPage(char *page)
    {
        if (unlikely(page == nullptr)) {
            ErrLog(DSTORE_WARNING, MODULE_UNDO, ErrMsg("Failed to dump transaction slot page because page is null."));
            return;
        }
        ErrLog(DSTORE_LOG, MODULE_UNDO, ErrMsg("Prev dump slot page: %s.", page));
    }
};

STATIC_ASSERT_TRIVIAL(TransactionSlotPage);
static_assert(sizeof(TransactionSlotPage) == BLCKSZ, "Page size must be equal to BLCKSZ");

struct UndoRecordPageHeader {
    uint32 version;
    PageId cur;
    PageId prev;
    PageId next;
} PACKED;

constexpr uint16 UNDO_RECORD_PAGE_HEADER_SIZE = (sizeof(Page) + sizeof(UndoRecordPageHeader));
/* The size of free space that each empty undo record page has */
constexpr uint16 UNDO_RECORD_PAGE_MAX_FREE_SPACE = BLCKSZ - UNDO_RECORD_PAGE_HEADER_SIZE;

struct UndoRecordPage : public Page {
    UndoRecordPageHeader m_undoRecPageHeader;
    char m_data[UNDO_RECORD_PAGE_MAX_FREE_SPACE];

    void InitUndoRecPage(const PageId &selfPageId)
    {
        UndoRecordPageHeader pre = m_undoRecPageHeader;
        Page::Init(0, PageType::UNDO_PAGE_TYPE, selfPageId);
        m_undoRecPageHeader = pre;
    }

    PageId GetNextPageId() const
    {
        return m_undoRecPageHeader.next;
    }
    inline uint32 GetVersion() const
    {
        return m_undoRecPageHeader.version;
    }

    char* Dump();
} PACKED;

STATIC_ASSERT_TRIVIAL(UndoRecordPage);
static_assert(sizeof(UndoRecordPage) == BLCKSZ, "Page size must be equal to BLCKSZ");
}

#endif /* STORAGE_UNDO_PAGE_H */
