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
 * dstore_undo_wal.h
 *
 * IDENTIFICATION
 *        include/undo/dstore_undo_wal.h
 *
 * ---------------------------------------------------------------------------------------
 */
 
#ifndef DSTORE_UNDO_WAL_H
#define DSTORE_UNDO_WAL_H

#include "page/dstore_heap_page.h"
#include "page/dstore_index_page.h"
#include "page/dstore_undo_page.h"
#include "page/dstore_undo_segment_meta_page.h"
#include "framework/dstore_instance.h"
#include "transaction/dstore_csn_mgr.h"
#include "wal/dstore_wal_struct.h"

namespace DSTORE {

struct WalRecordUndo : public WalRecordForPage {
    static void RedoUndoRecord(WalRecordRedoContext *redoCtx, const WalRecordUndo *undoRecord,
        BufferDesc *bufferDesc);

    static const WalRecordCompressAndDecompressItem *GetWalRecordItem(WalType walType);
    static uint16 GetUncompressedHeaderSize(const WalRecord *uncompressRecord);

    static void DumpUndoRecord(const WalRecordUndo *undoRecord, FILE *fp);

    inline void SetHeader(const WalPageHeaderContext &hdrCtx)
    {
        SetWalPageHeader(hdrCtx);
    }
    inline PageId GetPageId() const
    {
        return m_pageId;
    }
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordUndo);

struct WalRecordUndoZidSegmentInit : public WalRecordUndo {
    void Redo(Page *page) const
    {
        page->Init(0, PageType::UNDO_PAGE_TYPE, m_pageId);
    }
    void DumpUndo(FILE *fp) const
    {
        WalRecordForPage::Dump(fp);
    }
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordUndoZidSegmentInit);

struct WalRecordUndoZoneSegmentId : public WalRecordUndo {
    uint16 m_offset;
    PageId m_segmentId;

    inline void SetOffset(const OffsetNumber &offset)
    {
        m_offset = offset;
    }
    inline void SetSegmentId(const PageId &id)
    {
        m_segmentId = id;
    }
    void Redo(Page *page) const
    {
        char *rawPage = static_cast<char *>(static_cast<void *>(page));
        PageId *pageId = static_cast<PageId *>(static_cast<void *>(rawPage + m_offset));
        *pageId = m_segmentId;
    }
    void DumpUndo(FILE *fp) const
    {
        WalRecordForPage::Dump(fp);
        (void)fprintf(fp, "m_offset(%hu), m_segmentId(%hu, %u).",
            m_offset, m_segmentId.m_fileId, m_segmentId.m_blockId);
    }
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordUndoZoneSegmentId);

struct WalRecordUndoRecord : public WalRecordUndo {
    OffsetNumber m_offset;
    char m_data[];

    inline OffsetNumber GetOffset() const
    {
        return m_offset;
    }
    inline void SetOffset(const OffsetNumber &offset)
    {
        m_offset = offset;
    }

    void Redo(Page *page) const
    {
        CopyData(static_cast<char *>(static_cast<void *>(page)) + m_offset, static_cast<uint32>(BLCKSZ - m_offset),
                 m_data, GetSize() - sizeof(WalRecordUndoRecord));
    }

    void DumpUndo(FILE *fp) const
    {
        WalRecordForPage::Dump(fp);
        (void)fprintf(fp, "m_offset(%hu), datasize(%hu).",
            m_offset, static_cast<uint16>(GetSize() - sizeof(WalRecordUndoRecord)));
    }
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordUndoRecord);

struct WalRecordUndoRingOldPage : public WalRecordUndo {
    PageId adjacentPageId;

    inline void SetPageId(const PageId &pageId)
    {
        adjacentPageId = pageId;
    }

    void Redo(Page *page) const;
    void DumpUndo(FILE *fp) const
    {
        WalRecordForPage::Dump(fp);
        (void)fprintf(fp, "adjacentPageId(%hu, %u).", adjacentPageId.m_fileId, adjacentPageId.m_blockId);
    }
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordUndoRingOldPage);

struct WalRecordUndoRingNewPage : public WalRecordUndo {
    PageId m_prev;
    PageId m_next;

    inline void SetUndoRecPageHdr(const UndoRecordPageHeader &hdr)
    {
        m_prev = hdr.prev;
        m_next = hdr.next;
    }
    void Redo(Page *page) const
    {
        static_cast<UndoRecordPage *>(page)->m_undoRecPageHeader = {0, GetPageId(), m_prev, m_next};
        static_cast<UndoRecordPage *>(page)->InitUndoRecPage(GetPageId());
    }
    void DumpUndo(FILE *fp) const
    {
        WalRecordForPage::Dump(fp);
        (void)fprintf(fp, "prev(%hu, %u), next(%hu, %u).",
            m_prev.m_fileId, m_prev.m_blockId, m_next.m_fileId, m_next.m_blockId);
    }
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordUndoRingNewPage);

struct WalRecordUndoInitRecSpace : public WalRecordUndo {
    PageId m_firstUndoPageId;

    inline void SetFirstUndoPageId(const PageId &id)
    {
        m_firstUndoPageId = id;
    }
    void Redo(Page *page) const
    {
        static_cast<UndoSegmentMetaPage *>(page)->firstUndoPageId = m_firstUndoPageId;
    }
    void DumpUndo(FILE *fp) const
    {
        WalRecordForPage::Dump(fp);
        (void)fprintf(fp, "firstUndoPageId(%hu, %u).", m_firstUndoPageId.m_fileId, m_firstUndoPageId.m_blockId);
    }
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordUndoInitRecSpace);

struct WalRecordUndoInitTxnPage : public WalRecordUndo {
    void Redo(Page *page) const
    {
        static_cast<TransactionSlotPage *>(page)->InitTxnSlotPage(m_pageId);
    }
    void DumpUndo(FILE *fp) const
    {
        WalRecordForPage::Dump(fp);
    }
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordUndoInitTxnPage);

struct WalRecordUndoTxnSlotPageInited : public WalRecordUndo {
    void Redo(Page *page) const
    {
        static_cast<UndoSegmentMetaPage *>(page)->alreadyInitTxnSlotPages = true;
    }
    void DumpUndo(FILE *fp) const
    {
        WalRecordForPage::Dump(fp);
    }
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordUndoTxnSlotPageInited);

struct WalRecordUndoUpdateSlotUndoPtr : public WalRecordUndo {
    uint16 m_slotId;
    /* Indicates whether to insert undo record
     * if true, we need refresh spaceTailUndoPtr
     * if not, no need to refresh spaceTailUndoPtr
     */
    bool insertUndoFlag;
    uint64 m_tailPtr;

    inline void SetUpdateSlotUndoInfo(uint16 slotId, const UndoRecPtr &endPtr, bool insertFlag)
    {
        m_slotId = slotId;
        m_tailPtr = endPtr.m_placeHolder;
        insertUndoFlag = insertFlag;
    }

    uint16 GetMaxCompressedSize() const noexcept
    {
        const uint8 uint32Count = 4;
        return WalRecordUndo::GetMaxCompressedSize() + (COMPRESSED32_MAX_BYTE - sizeof(uint32)) * uint32Count;
    }
    uint16 Compress(char *walRecordForPageOnDisk) const noexcept
    {
        char *tempdiskData = walRecordForPageOnDisk;
        uint16 thisSize = WalRecordUndo::Compress(tempdiskData);
        StorageReleasePanic(m_size < sizeof(WalRecordUndoUpdateSlotUndoPtr), MODULE_UNDO,
                            ErrMsg("Wal serialize m_size too small."));
        tempdiskData += thisSize;
        tempdiskData += VarintCompress::CompressUnsigned32(static_cast<uint32>(m_slotId), tempdiskData);
        errno_t rc =
            memcpy_s(tempdiskData, sizeof(insertUndoFlag),
                     static_cast<const char *>(static_cast<const void *>(&(insertUndoFlag))), sizeof(insertUndoFlag));
        storage_securec_check(rc, "\0", "\0");
        tempdiskData += sizeof(insertUndoFlag);

        (static_cast<UndoRecPtr>(m_tailPtr)).Serialize(tempdiskData);
        return static_cast<uint16>(tempdiskData - walRecordForPageOnDisk);
    }

    uint16 Decompress(const WalRecord *origRecord) noexcept
    {
        const char *tempdiskData = static_cast<const char *>(static_cast<const void *>(origRecord));
        uint16 compressedSize1 = WalRecordUndo::Decompress(origRecord);
        tempdiskData += compressedSize1;
        const char *start = tempdiskData;

        uint8 thisSize;
        m_slotId = static_cast<uint16>(VarintCompress::DecompressUnsigned32(tempdiskData, thisSize));
        tempdiskData += thisSize;
        insertUndoFlag = *(static_cast<const bool *>(static_cast<const void *>(tempdiskData)));
        tempdiskData += sizeof(insertUndoFlag);
        UndoRecPtr undoRecPtr;
        undoRecPtr.Deserialize(tempdiskData);
        m_tailPtr = undoRecPtr.m_placeHolder;

        uint16 compressedSize2 = static_cast<uint16>(tempdiskData - start);
        StorageAssert(m_size >= compressedSize2);
        m_size =
            static_cast<uint16>((m_size - compressedSize2) +
                                static_cast<uint16>(sizeof(WalRecordUndoUpdateSlotUndoPtr) - sizeof(WalRecordUndo)));
        return compressedSize1 + compressedSize2;
    }

    void Redo(Page *page) const
    {
        TransactionSlot *slot = static_cast<TransactionSlotPage *>(page)->GetTransactionSlot(m_slotId);
        UndoRecPtr tailPtr(m_tailPtr);
        slot->SetCurTailUndoPtr(tailPtr);
        if (insertUndoFlag) {
            slot->SetSpaceTailUndoPtr(tailPtr);
        }
    }

    void DumpUndo(FILE *fp) const
    {
        WalRecordForPage::Dump(fp);
        UndoRecPtr undoRecPtr(m_tailPtr);
        (void)fprintf(fp, "m_slotId(%hu), insertUndoFlag(%d), m_tailPtr(%hu, %u, %hu).", m_slotId,
                    static_cast<int>(insertUndoFlag), undoRecPtr.GetFileId(), undoRecPtr.GetBlockNum(),
                    undoRecPtr.GetOffset());
    }
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordUndoUpdateSlotUndoPtr);

struct WalRecordUndoTxnSlotAllocate : public WalRecordUndo {
    void Redo(Page *page, Xid xid) const
    {
        static_cast<TransactionSlotPage *>(page)->SetNextFreeLogicSlotId(xid.m_logicSlotId + 1);
        uint32 slotId = TransactionSlotPage::GetSlotId(xid.m_logicSlotId);
        TransactionSlot *slot = static_cast<TransactionSlotPage *>(page)->GetTransactionSlot(slotId);
        slot->Init(xid.m_logicSlotId);
    }
    void DumpUndo(FILE *fp) const
    {
        WalRecordForPage::Dump(fp);
    }
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordUndoTxnSlotAllocate);

struct WalRecordTransactionCommit : public WalRecordUndo {
    uint16 m_slotId;
    CommitSeqNo m_csn;
    TrxSlotStatus m_status;
    TimestampTz m_commitTime;

    inline CommitSeqNo GetCsn() const
    {
        return m_csn;
    }
    inline void SetCsn(CommitSeqNo csn)
    {
        m_csn = csn;
    }

    inline uint16 GetSlotId() const
    {
        return m_slotId;
    }
    inline void SetSlotId(uint16 slotId)
    {
        m_slotId = slotId;
    }

    inline void SetTrxSlotStatus(TrxSlotStatus status)
    {
        m_status = status;
    }

    inline TrxSlotStatus GetTrxSlotStatus() const
    {
        return m_status;
    }

    inline void SetCurrentCommitTime()
    {
        /* only record commit timestamp in TXN_STATUS_COMMITTED */
        if (m_status != TXN_STATUS_COMMITTED) {
            return;
        }
        m_commitTime = GetCurrentTimestamp();
    }

    void Redo(Page *page) const
    {
        TransactionSlot *slot = static_cast<TransactionSlotPage *>(page)->GetTransactionSlot(m_slotId);
        slot->SetCsn(m_csn);
        slot->SetTrxSlotStatus(m_status);
        (void)g_storageInstance->GetCsnMgr()->UpdateMaxReservedCsnIfNecessary(m_csn);
    }

    void DumpUndo(FILE *fp) const
    {
        WalRecordForPage::Dump(fp);
        if (m_status == TXN_STATUS_COMMITTED) {
            (void)fprintf(fp, "m_commitTime(%s), ", TimestamptzTostr(m_commitTime));
        }
        (void)fprintf(fp, "m_slotId(%hu), m_csn(%lu), m_status(%d).", m_slotId, m_csn, static_cast<int>(m_status));
    }
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordTransactionCommit);

struct WalRecordTransactionAbort : public WalRecordUndo {
    uint16 m_slotId;
    CommitSeqNo m_csn;
    TimestampTz m_abortTime;

    inline uint16 GetSlotId() const
    {
        return m_slotId;
    }
    inline void SetSlotId(uint16 slotId)
    {
        m_slotId = slotId;
    }

    inline CommitSeqNo GetCsn() const
    {
        return m_csn;
    }
    inline void SetCsn(CommitSeqNo csn)
    {
        m_csn = csn;
    }
    inline void SetCurrentAbortTime()
    {
        m_abortTime = GetCurrentTimestamp();
    }

    void Redo(Page *page) const
    {
        TransactionSlot *slot = static_cast<TransactionSlotPage *>(page)->GetTransactionSlot(m_slotId);
        slot->SetCsn(m_csn);
        slot->SetTrxSlotStatus(TXN_STATUS_ABORTED);
    }

    void DumpUndo(FILE *fp) const
    {
        WalRecordForPage::Dump(fp);
        (void)fprintf(fp, "m_abortTime(%s), m_slotId(%hu), m_csn(%lu).", TimestamptzTostr(m_abortTime), m_slotId,
                      m_csn);
    }
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordTransactionAbort);

struct WalRecordRollbackForData : public WalRecordUndo {
    UndoType m_undoType;
    uint8 m_tdId;
    TdCsnStatus m_preCsnStatus;
    OffsetNumber m_tupOff;
    uint64 m_preXid;
    uint64 m_prePtr;
    CommitSeqNo m_preCsn;
    char m_data[];

    void RecordUndoRecHdr(const UndoRecord &rec)
    {
        m_undoType = rec.GetUndoType();
        m_tdId = rec.GetTdId();
        m_preCsnStatus = rec.GetTdPreCsnStatus();
        m_tupOff = rec.GetCtid().GetOffset();
        m_preXid = rec.GetTdPreXid().m_placeHolder;
        m_prePtr = rec.GetTdPreUndoPtr().m_placeHolder;
        m_preCsn = rec.GetTdPreCsn();
    }

    void RecordUndoRecHdr(UndoType undoType, uint8 tdId, TdCsnStatus preCsnStatus, OffsetNumber tupOff,
                          uint64 preXid, uint64 prePtr, CommitSeqNo preCsn)
    {
        m_undoType = undoType;
        m_tdId = tdId;
        m_preCsnStatus = preCsnStatus;
        m_tupOff = tupOff;
        m_preXid = preXid;
        m_prePtr = prePtr;
        m_preCsn = preCsn;
    }

    void Redo(Page *page) const
    {
        UndoRecord rec;
        rec.SetUndoType(m_undoType);
        rec.SetCtid({page->GetSelfPageId(), m_tupOff});
        rec.SetPreTdInfo(m_tdId, UndoRecPtr(m_prePtr), Xid(m_preXid), m_preCsn, m_preCsnStatus);
        RetStatus ret = DSTORE_SUCC;
        if (m_size - sizeof(WalRecordRollbackForData) > 0) {
            ret = rec.Append(m_data, m_size - sizeof(WalRecordRollbackForData));
            StorageReleasePanic(STORAGE_FUNC_FAIL(ret), MODULE_WAL, ErrMsg("Redo failed because of oom!"));
        }

        PageType pageType = page->GetType();
        if (pageType == PageType::HEAP_PAGE_TYPE) {
            (void)static_cast<HeapPage *>(page)->UndoHeap(&rec);
        } else if (pageType == PageType::INDEX_PAGE_TYPE) {
            if (m_tupOff == INVALID_ITEM_OFFSET_NUMBER) {
                TD* td = static_cast<DataPage*>(page)->GetTd(m_tdId);
                td->RollbackTdInfo(&rec);
            } else {
                ret = static_cast<BtrPage *>(page)->RollbackBtrForRecovery(&rec);
                StorageReleasePanic(STORAGE_FUNC_FAIL(ret), MODULE_WAL,  ErrMsg("Redo fail"));
            }
        }
    }

    void DumpUndo(FILE *fp) const
    {
        WalRecordForPage::Dump(fp);
        UndoRecPtr prevPtr(m_prePtr);
        Xid xid(m_preXid);
        (void)fprintf(fp,
                      "m_undoType(%d), tdId(%hhu), m_preCsnStatus(%d), offset(%hu), m_preXid(%d, %lu), "
                      "prevUndoRecPtr(%hu, %u, %hu), preCsn(%lu)}. ",
                      static_cast<int>(m_undoType), m_tdId, static_cast<int>(m_preCsnStatus), m_tupOff,
                      static_cast<int>(xid.m_zoneId), xid.m_logicSlotId, prevPtr.GetFileId(), prevPtr.GetBlockNum(),
                      prevPtr.GetOffset(), m_preCsn);

        if (m_type == WAL_UNDO_BTREE_PAGE_ROLL_BACK || m_type == WAL_UNDO_BTREE) {
            UndoRecord rec;
            rec.SetUndoType(m_undoType);
            rec.SetCtid({m_pageId, m_tupOff});
            rec.SetPreTdInfo(m_tdId, UndoRecPtr(m_prePtr), Xid(m_preXid), m_preCsn, m_preCsnStatus);
            RetStatus ret = rec.Append(m_data, m_size - sizeof(WalRecordRollbackForData));
            StorageReleasePanic(STORAGE_FUNC_FAIL(ret), MODULE_WAL,  ErrMsg("Dump undo failed because of oom!"));
            ItemPointerData heapCtid;
            if (m_undoType == UNDO_BTREE_DELETE || m_undoType == UNDO_BTREE_DELETE_TMP) {
                IndexTuple *itup = static_cast<IndexTuple *>(static_cast<UndoDataBtreeDelete*>(rec.GetUndoData())->GetData());
                heapCtid = itup->GetHeapCtid();
            } else {
                heapCtid = static_cast<UndoDataBtreeInsert*>(rec.GetUndoData())->GetHeapCtid();
            }
            (void)fprintf(fp, "heapCtid(%hu, %u, %u)", heapCtid.GetFileId(), heapCtid.GetBlockNum(),
                          heapCtid.GetOffset());
        }
    }
    uint16 GetMaxCompressedSize() const noexcept
    {
        const uint8 uint64Count = 2;
        const uint8 uint32Count = 4;
        return WalRecordUndo::GetMaxCompressedSize() + (COMPRESSED32_MAX_BYTE - sizeof(uint32)) * uint32Count +
               (COMPRESSED64_MAX_BYTE - sizeof(uint64)) * uint64Count;
    }
    uint16 Compress(char *walRecordForPageOnDisk) const noexcept
    {
        char *tempdiskData = walRecordForPageOnDisk;
        uint16 thisSize = WalRecordUndo::Compress(tempdiskData);
        StorageReleasePanic(m_size < sizeof(WalRecordRollbackForData), MODULE_UNDO,
                            ErrMsg("Wal serialize m_size too small."));
        tempdiskData += thisSize;
        errno_t rc = memcpy_s(tempdiskData, sizeof(m_undoType),
                              static_cast<const char *>(static_cast<const void *>(&(m_undoType))), sizeof(m_undoType));
        storage_securec_check(rc, "\0", "\0");
        tempdiskData += sizeof(m_undoType);

        rc = memcpy_s(tempdiskData, sizeof(m_tdId), static_cast<const char *>(static_cast<const void *>(&(m_tdId))),
                      sizeof(m_tdId));
        storage_securec_check(rc, "\0", "\0");
        tempdiskData += sizeof(m_tdId);

        rc = memcpy_s(tempdiskData, sizeof(m_preCsnStatus),
                      static_cast<const char *>(static_cast<const void *>(&(m_preCsnStatus))), sizeof(m_preCsnStatus));
        storage_securec_check(rc, "\0", "\0");
        tempdiskData += sizeof(m_preCsnStatus);

        tempdiskData += VarintCompress::CompressUnsigned32(static_cast<uint32>(m_tupOff), tempdiskData);
        tempdiskData += VarintCompress::CompressUnsigned64(m_preXid, tempdiskData);
        (static_cast<UndoRecPtr>(m_prePtr)).Serialize(tempdiskData);
        tempdiskData += VarintCompress::CompressUnsigned64(m_preCsn, tempdiskData);
        return static_cast<uint16>(tempdiskData - walRecordForPageOnDisk);
    }

    uint16 Decompress(const WalRecord *origRecord) noexcept
    {
        const char *tempdiskData = static_cast<const char *>(static_cast<const void *>(origRecord));
        uint16 compressedSize1 = WalRecordUndo::Decompress(origRecord);
        tempdiskData += compressedSize1;
        const char *start = tempdiskData;

        m_undoType = *(static_cast<const UndoType *>(static_cast<const void *>(tempdiskData)));
        tempdiskData += sizeof(m_undoType);
        m_tdId = *(static_cast<const uint8 *>(static_cast<const void *>(tempdiskData)));
        tempdiskData += sizeof(m_tdId);
        m_preCsnStatus = *(static_cast<const TdCsnStatus *>(static_cast<const void *>(tempdiskData)));
        tempdiskData += sizeof(m_preCsnStatus);

        uint8 thisSize;
        m_tupOff = static_cast<OffsetNumber>(VarintCompress::DecompressUnsigned32(tempdiskData, thisSize));
        tempdiskData += thisSize;

        m_preXid = VarintCompress::DecompressUnsigned64(tempdiskData, thisSize);
        tempdiskData += thisSize;
        UndoRecPtr undoRecPtr;
        undoRecPtr.Deserialize(tempdiskData);
        m_prePtr = undoRecPtr.m_placeHolder;

        m_preCsn = VarintCompress::DecompressUnsigned64(tempdiskData, thisSize);
        tempdiskData += thisSize;

        uint16 compressedSize2 = static_cast<uint16>(tempdiskData - start);
        StorageAssert(m_size >= compressedSize2);
        m_size = static_cast<uint16>((m_size - compressedSize2) +
                                     static_cast<uint16>(sizeof(WalRecordRollbackForData) - sizeof(WalRecordUndo)));
        
        return compressedSize1 + compressedSize2;
    }
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordRollbackForData);

struct WalRecordRecycleSlot : public WalRecordUndo {
    ZoneId m_zoneId;
    CommitSeqNo m_recycleCsn;
    uint64 m_lastRecycleLogicSlotId;
    uint64 m_newRecycleLogicSlotId;

    inline void SetZid(const ZoneId &zid)
    {
        m_zoneId = zid;
    }

    inline void SetRecycleCsn(const CommitSeqNo &csn)
    {
        m_recycleCsn = csn;
    }

    inline void SetLastRecycleLogicSlotId(const uint64 &lastSlotId)
    {
        m_lastRecycleLogicSlotId = lastSlotId;
    }

    inline void SetNewRecycleLogicSlotId(const uint64 &newSlotId)
    {
        m_newRecycleLogicSlotId = newSlotId;
    }

    void Redo(Page *page) const
    {
        uint16 startSlotId = TransactionSlotPage::GetSlotId(m_lastRecycleLogicSlotId);
        uint16 endSlotId = TransactionSlotPage::GetSlotId(m_newRecycleLogicSlotId);
        TransactionSlot *slot = nullptr;
        for (uint16 tempSlotId = startSlotId; tempSlotId < endSlotId; tempSlotId++) {
            slot = static_cast<TransactionSlotPage *>(page)->GetTransactionSlot(tempSlotId);
            slot->SetTrxSlotStatus(TXN_STATUS_FROZEN);
        }
    }

    void DumpUndo(FILE *fp) const
    {
        WalRecordForPage::Dump(fp);
        (void)fprintf(fp,
                      "m_zoneId(%d), m_recyclecsn(%lu), m_lastRecycleLogicSlotId(%lu), m_newRecycleLogicSlotId(%lu).",
                      m_zoneId, m_recycleCsn, m_lastRecycleLogicSlotId, m_newRecycleLogicSlotId);
    }
}PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordRecycleSlot);

}  // namespace DSTORE

#endif