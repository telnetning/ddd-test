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
 * dstore_page.cpp
 *
 * IDENTIFICATION
 *        src/page/dstore_page.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "page/dstore_page.h"
#include <securec.h>
#include "errorcode/dstore_undo_error_code.h"
#include "page/dstore_heap_page.h"
#include "page/dstore_index_page.h"
#include "page/dstore_td.h"
#include "page/dstore_undo_segment_meta_page.h"
#include "undo/dstore_undo_record.h"
#include "transaction/dstore_transaction.h"
#include "transaction/dstore_transaction_mgr.h"
#include "common/algorithm/dstore_checksum_impl.h"
namespace DSTORE {

/* The checksum of all zero new page get from CompChecksum. */
constexpr uint16 ALL_ZERO_PAGE_CHECKSUM = 25258;

const char *Page::PageTypeToStr(PageType type)
{
    static const char *pageTypeName[] = {
        "Invalid page type",
        "Heap page type",
        "Index page type",
        "Transaction slot page",
        "Undo page type",
        "Fsm page type",
        "Fsm meta page type",
        "Data segment meta page type",
        "Heap segment meta page type",
        "Undo segment meta page type",
        "Extent meta page type",
        "Bitmap page type",
        "Bitmap meta page type",
        "Queue page type",
        "Local btree recycle meta page type",
        "Root btree recycle meta page type",
        "Tbs space meta page type",
        "Max page type"
    };
    StorageAssert(type < PageType::MAX_PAGE_TYPE);

    constexpr uint8 pageTypeNameSize = sizeof(pageTypeName) / sizeof(pageTypeName[0]);
    static_assert(pageTypeNameSize == static_cast<uint8>(PageType::MAX_PAGE_TYPE),
        "pageTypeName must be same as PageType");

    return pageTypeName[static_cast<uint16>(type)];
}
void Page::Init(uint16 specialSize, PageType type, const PageId &selfPageId)
{
    /* For new page: the caller must make sure the header is all-zero */
    /* For reused page: should retain the old glsn plsn and walId */
    uint64 glsn = m_header.m_glsn;
    uint64 plsn = m_header.m_plsn;
    uint16 walId = m_header.m_walId;
    errno_t ret = memset_sp(PageHeaderPtr(), BLCKSZ, 0, BLCKSZ);
    storage_securec_check(ret, "", "");

    m_header.m_special.m_offset = static_cast<uint16>(static_cast<uint16>(BLCKSZ) - specialSize);
    m_header.m_special.m_needCrc = (specialSize != 0) ? 0 : 1;
    m_header.m_type = static_cast<uint16>(type);
    m_header.m_lower = sizeof(Page);
    m_header.m_upper = static_cast<uint16>(static_cast<uint16>(BLCKSZ) - specialSize);
    m_header.m_myself = selfPageId;
    m_header.m_glsn = glsn;
    m_header.m_plsn = plsn;
    m_header.m_walId = walId;
}

void Page::DumpHeader(StringInfo str) const
{
    str->append("m_glsn = %lu \n", m_header.m_glsn);
    str->append("m_plsn = %lu \n", m_header.m_plsn);
    str->append("walId = %lu \n", m_header.m_walId);
    str->append("m_checksum = %hu \n", m_header.m_checksum);
    str->append("m_lower = %hu \n", m_header.m_lower);
    str->append("m_upper = %hu \n", m_header.m_upper);
    str->append("m_special.m_needCrc = %hu \n", m_header.m_special.m_needCrc);
    str->append("m_special.m_offset = %hu \n", m_header.m_special.m_offset);
    str->append("m_type = %hu \n", m_header.m_type);
    str->append("m_myself = (%hu, %u)\n", m_header.m_myself.m_fileId, m_header.m_myself.m_blockId);
}

void Page::SetChecksum(bool isCrExtend)
{
    const void *startPtr = static_cast<const void *>(reinterpret_cast<const char *>(this) + sizeof(uint32));
    uint32 pageSize = isCrExtend ? EXTEND_PAGE_SIZE : PAGE_SIZE;
    uint32 checkSize = pageSize - sizeof(uint32);
    if (!m_header.m_special.m_needCrc && m_header.m_special.m_offset > 0 &&
        m_header.m_special.m_offset < pageSize) {
        checkSize = m_header.m_special.m_offset - sizeof(uint32);
    }
    m_header.m_checksum =
        static_cast<uint16>(CompChecksum(startPtr, checkSize, CHECKSUM_FNV));
}

bool Page::CheckPageCrcMatch() const
{
    uint16 checksum = GetChecksum();
    const void *startPtr = static_cast<const void *>(reinterpret_cast<const char *>(this) + sizeof(uint32));
    uint32 checkSize = BLCKSZ - sizeof(uint32);
    if (!m_header.m_special.m_needCrc && m_header.m_special.m_offset > 0 && m_header.m_special.m_offset < BLCKSZ) {
        checkSize = m_header.m_special.m_offset - sizeof(uint32);
    }
    uint16 newCheckSum =
        static_cast<uint16>(CompChecksum(startPtr, checkSize, CHECKSUM_FNV));
    /* All pages need checksum including all-zero page */
    /* The checksum of the all-zero page is ALL_ZERO_PAGE_CHECKSUM */
    return ((checksum == newCheckSum) || (checksum == 0 && newCheckSum == ALL_ZERO_PAGE_CHECKSUM));
}

CommitSeqNo TD::FillCsn(Transaction *transaction, XidStatus *inXidStatus)
{
    StorageAssert(transaction != nullptr);

    if (TestCsnStatus(IS_CUR_XID_CSN)) {
        return m_csn;
    }

    if (GetXid() == INVALID_XID) {
        return INVALID_CSN;
    }

    XidStatus xs(GetXid(), transaction);
    if (inXidStatus == nullptr) {
        inXidStatus = &xs;
    }

    if (inXidStatus->IsFrozen()) {
        m_csn = COMMITSEQNO_FIRST_NORMAL;
        m_csnStatus = static_cast<uint16>(IS_CUR_XID_CSN);
        if (TestStatus(TDStatus::OCCUPY_TRX_IN_PROGRESS) && GetLockerXid() == INVALID_XID) {
            SetStatus(TDStatus::OCCUPY_TRX_END);
        }
    }

    if (inXidStatus->IsCommitted()) {
        m_csn = inXidStatus->GetCsn();
        StorageAssert(m_csn != INVALID_CSN);
        m_csnStatus = static_cast<uint16>(IS_CUR_XID_CSN);
        if (TestStatus(TDStatus::OCCUPY_TRX_IN_PROGRESS) && GetLockerXid() == INVALID_XID) {
            SetStatus(TDStatus::OCCUPY_TRX_END);
        }
    }

    return m_csn;
}

/*
 * For this td, rollback to previous undo record.
 */
void TD::RollbackTdInfo(UndoRecord *undo)
{
    Xid currentXid = GetXid();
    Xid preXid = undo->GetTdPreXid();
    SetUndoRecPtr(undo->GetTdPreUndoPtr());
    SetXid(preXid);
    SetCsn(undo->GetTdPreCsn());
    SetCsnStatus(undo->GetTdPreCsnStatus());
    if (preXid == INVALID_XID) {
        SetStatus(TDStatus::UNOCCUPY_AND_PRUNEABLE);
    } else if (preXid != currentXid) {
        SetStatus(TDStatus::OCCUPY_TRX_END);
    }

    /* just roll back cid to INVALID_CID, because tdCid == undoCid. */
    SetCommandId(INVALID_CID);
}

/*
 * For this td, rollback to previous xid.
 * Due to btree page split, maybe xid status is aborted in some tds,
 * should rollback to previous xid.
 */
void TD::RollbackTdToPreTxn(PdbId pdbId)
{
    StorageAssert(g_storageInstance->GetPdb(pdbId) != nullptr);
    TransactionMgr *transactionMgr = g_storageInstance->GetPdb(pdbId)->GetTransactionMgr();
    UndoRecord undoRecord;
    Xid xid = GetXid();
    while (xid == GetXid()) {
        if (STORAGE_FUNC_FAIL(transactionMgr->FetchUndoRecord(xid, &undoRecord, GetUndoRecPtr()))) {
            StorageAssert(StorageGetErrorCode() == UNDO_ERROR_RECORD_RECYCLED);
            StorageClearError();
            /* The xid has been recycled */
            Reset();
            break;
        }
        /* Roll back td info */
        RollbackTdInfo(&undoRecord);
    }
}

void ItemPointerData::Dump(char *buf, uint32_t len) const
{
    errno_t rc =
        snprintf_s(buf, len, len - 1, "(%u, %u, %u)", val.m_pageid.m_fileId, val.m_pageid.m_blockId, val.m_offset);
    storage_securec_check_ss(rc);
}

uint8 ItemPointerData::GetCompressedSize() const
{
    return VarintCompress::GetUnsigned32CompressedSize(GetFileId()) +
           VarintCompress::GetUnsigned32CompressedSize(GetBlockNum()) +
           VarintCompress::GetUnsigned32CompressedSize(GetOffset());
}

void ItemPointerData::Serialize(char *&data) const
{
    data += VarintCompress::CompressUnsigned32(GetFileId(), data);
    data += VarintCompress::CompressUnsigned32(GetBlockNum(), data);
    data += VarintCompress::CompressUnsigned32(GetOffset(), data);
}

void ItemPointerData::Deserialize(const char *&data)
{
    uint8 size = 0;
    val.m_pageid.m_fileId = static_cast<uint16>(VarintCompress::DecompressUnsigned32(data, size));
    data += size;
    val.m_pageid.m_blockId = VarintCompress::DecompressUnsigned32(data, size);
    data += size;
    val.m_offset = static_cast<uint16>(VarintCompress::DecompressUnsigned32(data, size));
    data += size;
}

} /* The end of namespace DSTOER */
