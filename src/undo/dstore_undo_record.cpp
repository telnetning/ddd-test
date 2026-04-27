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
 * dstore_undo_record.cpp
 *
 * IDENTIFICATION
 *        src/undo/dstore_undo_record.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "undo/dstore_undo_record.h"
#include "common/memory/dstore_mctx.h"
#include "heap/dstore_heap_undo_struct.h"

namespace DSTORE {

UndoRecord::UndoRecord() : m_pallocDataSize(0)
{
    m_header.m_undoType = UNDO_UNKNOWN;
    m_header.Init();
    m_dataInfo.len = 0;
    m_dataInfo.data = nullptr;
    m_serializeData = nullptr;
    m_serializeSize = 0;
    m_currentFetchUndoPageBuf = nullptr;
}

UndoRecord::UndoRecord(UndoType type, uint8 tdId, TD *td, ItemPointerData ctid, CommandId cid) : m_pallocDataSize(0)
{
    m_header.m_undoType = type;
    SetPreTdInfo(tdId, td);
    m_header.m_txnPreUndoPtr = INVALID_UNDO_RECORD_PTR.m_placeHolder;
    m_header.m_ctid = ctid.m_placeHolder;
    m_header.m_cid = cid;
    m_header.m_fileVersion = INVALID_FILE_VERSION;
    m_dataInfo.len = 0;
    m_dataInfo.data = nullptr;
    m_serializeData = nullptr;
    m_serializeSize = 0;
    m_currentFetchUndoPageBuf = nullptr;
}

UndoRecord::~UndoRecord() noexcept
{
    if (m_pallocDataSize == 0) {
        DstorePfreeExt(m_dataInfo.data);
    }
    m_dataInfo.data = nullptr;
    m_serializeData = nullptr;
}

RetStatus UndoRecord::CheckValidity() const
{
    switch (GetUndoType()) {
        case UNDO_HEAP_INSERT:
        case UNDO_HEAP_INSERT_TMP:
        case UNDO_HEAP_ANOTHER_PAGE_APPEND_UPDATE_NEW_PAGE:
        case UNDO_HEAP_ANOTHER_PAGE_APPEND_UPDATE_NEW_PAGE_TMP:
            return m_dataInfo.data == nullptr ? DSTORE_SUCC : DSTORE_FAIL;
        default:
            return m_dataInfo.data == nullptr ? DSTORE_FAIL : DSTORE_SUCC;
    }
}

bool UndoRecord::IsMatchedCtid(ItemPointerData ctid) const
{
    if (m_header.GetUndoType() != UNDO_HEAP_BATCH_INSERT && m_header.GetUndoType() != UNDO_HEAP_BATCH_INSERT_TMP) {
        return m_header.GetCtid() == ctid;
    }

    if (ctid.GetPageId() != GetPageId()) {
        return false;
    }

    UndoDataHeapBatchInsert *undoData = static_cast<UndoDataHeapBatchInsert *>(GetUndoData());
    OffsetNumber *offsetData = static_cast<OffsetNumber *>(undoData->GetRawData());
    constexpr uint16 step = 2;
    uint16 rangeNum = (undoData->GetRawDataSize() / sizeof(uint16)) / step;

    for (uint16 i = 0; i < rangeNum; ++i) {
        OffsetNumber startOffset = offsetData[i * step];
        OffsetNumber endOffset = offsetData[i * step + 1];
        if (startOffset <= ctid.GetOffset() && ctid.GetOffset() <= endOffset) {
            return true;
        }
    }

    return false;
}

}
