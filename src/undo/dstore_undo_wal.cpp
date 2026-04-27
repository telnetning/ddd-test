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
 * dstore_undo_wal.cpp
 *
 *
 *
 * IDENTIFICATION
 *        storage/src/undo/dstore_undo_wal.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include <functional>
#include <utility>
#include "undo/dstore_undo_wal.h"

namespace DSTORE {

struct UndoWalRedoItem {
    WalType type;
    std::function<void(const WalRecordUndo*, Page *)> redo;

    UndoWalRedoItem(WalType walType, std::function<void(const WalRecordUndo *, Page *)> redoFunc) noexcept
        : type(walType), redo(std::move(redoFunc))
    {}
};

static const UndoWalRedoItem UNDO_WAL_REDO_TABLE[] {
    {WAL_UNDO_INIT_MAP_SEGMENT, [](const WalRecordUndo *self, Page *page)
        { (static_cast<const WalRecordUndoZidSegmentInit *>(self))->Redo(page); }},
    {WAL_UNDO_SET_ZONE_SEGMENT_ID, [](const WalRecordUndo *self, Page *page)
        { (static_cast<const WalRecordUndoZoneSegmentId *>(self))->Redo(page); }},
    {WAL_UNDO_INSERT_RECORD, [](const WalRecordUndo *self, Page *page)
        { (static_cast<const WalRecordUndoRecord *>(self))->Redo(page); }},
    {WAL_UNDO_INIT_RECORD_SPACE, [](const WalRecordUndo *self, Page *page)
        { (static_cast<const WalRecordUndoInitRecSpace *>(self))->Redo(page); }},
    {WAL_UNDO_EXTEND_PAGE_RING_PREV_PAGE, [](const WalRecordUndo *self, Page *page)
        { (static_cast<const WalRecordUndoRingOldPage *>(self))->Redo(page); }},
    {WAL_UNDO_EXTEND_PAGE_RING_NEXT_PAGE, [](const WalRecordUndo *self, Page *page)
        { (static_cast<const WalRecordUndoRingOldPage *>(self))->Redo(page); }},
    {WAL_UNDO_EXTEND_PAGE_RING_NEW_PAGE, [](const WalRecordUndo *self, Page *page)
        { (static_cast<const WalRecordUndoRingNewPage *>(self))->Redo(page); }},
    {WAL_UNDO_INIT_TXN_PAGE, [](const WalRecordUndo *self, Page *page)
        { (static_cast<const WalRecordUndoInitTxnPage *>(self))->Redo(page); }},
    {WAL_UNDO_UPDATE_TXN_SLOT_PTR, [](const WalRecordUndo *self, Page *page)
        { (static_cast<const WalRecordUndoUpdateSlotUndoPtr *>(self))->Redo(page); }},
    {WAL_UNDO_SET_TXN_PAGE_INITED, [](const WalRecordUndo *self, Page *page)
        { (static_cast<const WalRecordUndoTxnSlotPageInited *>(self))->Redo(page); }},
    {WAL_UNDO_HEAP, [](const WalRecordUndo *self, Page *page)
        { (static_cast<const WalRecordRollbackForData *>(self))->Redo(page); }},
    {WAL_UNDO_BTREE, [](const WalRecordUndo *self, Page *page)
        { (static_cast<const WalRecordRollbackForData *>(self))->Redo(page); }},
    {WAL_TXN_COMMIT, [](const WalRecordUndo *self, Page *page)
        { (static_cast<const WalRecordTransactionCommit *>(self))->Redo(page); }},
    {WAL_TXN_ABORT, [](const WalRecordUndo *self, Page *page)
        { (static_cast<const WalRecordTransactionAbort *>(self))->Redo(page); }},
    {WAL_UNDO_HEAP_PAGE_ROLL_BACK, [](const WalRecordUndo *self, Page *page)
        { (static_cast<const WalRecordRollbackForData *>(self))->Redo(page); }},
    {WAL_UNDO_BTREE_PAGE_ROLL_BACK, [](const WalRecordUndo *self, Page *page)
        { (static_cast<const WalRecordRollbackForData *>(self))->Redo(page); }},
    {WAL_UNDO_RECYCLE_TXN_SLOT, [](const WalRecordUndo *self, Page *page)
        { (static_cast<const WalRecordRecycleSlot *>(self))->Redo(page); }}
};

void WalRecordUndo::RedoUndoRecord(WalRecordRedoContext *redoCtx, const WalRecordUndo *undoRecord,
                                   BufferDesc *bufferDesc)
{
    StorageReleasePanic(undoRecord == nullptr, MODULE_UNDO, ErrMsg("Redo undo record failed because record is null."));
    PageId pageId = undoRecord->m_pageId;
    StorageReleasePanic(bufferDesc == nullptr, MODULE_UNDO,
                        ErrMsg("Read page (%hu, %u) failed", pageId.m_fileId, pageId.m_blockId));

    Page *page = bufferDesc->GetPage();
    WalType recordType = undoRecord->m_type;
    if (recordType == WAL_UNDO_ALLOCATE_TXN_SLOT) {
        static_cast<const WalRecordUndoTxnSlotAllocate *>(undoRecord)->Redo(page, redoCtx->xid);
    } else {
        for (uint32 i = 0; i < sizeof(UNDO_WAL_REDO_TABLE) / sizeof(UNDO_WAL_REDO_TABLE[0]); ++i) {
            if (UNDO_WAL_REDO_TABLE[i].type == recordType) {
                UNDO_WAL_REDO_TABLE[i].redo(undoRecord, page);
            }
        }
    }
    const uint64 glsn = undoRecord->m_pagePreWalId != redoCtx->walId ? undoRecord->m_pagePreGlsn + 1
                                                                     : undoRecord->m_pagePreGlsn;
    page->SetLsn(redoCtx->walId, redoCtx->recordEndPlsn, glsn);
}

void WalRecordUndoRingOldPage::Redo(Page *page) const
{
    if (m_type == WAL_UNDO_EXTEND_PAGE_RING_PREV_PAGE) {
        static_cast<UndoRecordPage *>(page)->m_undoRecPageHeader.next = adjacentPageId;
    } else if (m_type == WAL_UNDO_EXTEND_PAGE_RING_NEXT_PAGE) {
        static_cast<UndoRecordPage *>(page)->m_undoRecPageHeader.prev = adjacentPageId;
    }
}

struct UndoWalDumpItem {
    WalType type;
    std::function<void(const WalRecordUndo*, FILE *fp)> dump;

    UndoWalDumpItem(WalType walType, std::function<void(const WalRecordUndo *, FILE *fp)> dumpFunc) noexcept
        : type(walType), dump(std::move(dumpFunc))
    {}
};

static const UndoWalDumpItem UNDO_WAL_DUMP_TABLE[] {
    {WAL_UNDO_INIT_MAP_SEGMENT, [](const WalRecordUndo *self, FILE *fp)
        { (static_cast<const WalRecordUndoZidSegmentInit *>(self))->DumpUndo(fp); }},
    {WAL_UNDO_SET_ZONE_SEGMENT_ID, [](const WalRecordUndo *self, FILE *fp)
        { (static_cast<const WalRecordUndoZoneSegmentId *>(self))->DumpUndo(fp); }},
    {WAL_UNDO_INSERT_RECORD, [](const WalRecordUndo *self, FILE *fp)
        { (static_cast<const WalRecordUndoRecord *>(self))->DumpUndo(fp); }},
    {WAL_UNDO_INIT_RECORD_SPACE, [](const WalRecordUndo *self, FILE *fp)
        { (static_cast<const WalRecordUndoInitRecSpace *>(self))->DumpUndo(fp); }},
    {WAL_UNDO_EXTEND_PAGE_RING_PREV_PAGE, [](const WalRecordUndo *self, FILE *fp)
        { (static_cast<const WalRecordUndoRingOldPage *>(self))->DumpUndo(fp); }},
    {WAL_UNDO_EXTEND_PAGE_RING_NEXT_PAGE, [](const WalRecordUndo *self, FILE *fp)
        { (static_cast<const WalRecordUndoRingOldPage *>(self))->DumpUndo(fp); }},
    {WAL_UNDO_EXTEND_PAGE_RING_NEW_PAGE, [](const WalRecordUndo *self, FILE *fp)
        { (static_cast<const WalRecordUndoRingNewPage *>(self))->DumpUndo(fp); }},
    {WAL_UNDO_INIT_TXN_PAGE, [](const WalRecordUndo *self, FILE *fp)
        { (static_cast<const WalRecordUndoInitTxnPage *>(self))->DumpUndo(fp); }},
    {WAL_UNDO_UPDATE_TXN_SLOT_PTR, [](const WalRecordUndo *self, FILE *fp)
        { (static_cast<const WalRecordUndoUpdateSlotUndoPtr *>(self))->DumpUndo(fp); }},
    {WAL_UNDO_ALLOCATE_TXN_SLOT, [](const WalRecordUndo *self, FILE *fp)
        { (static_cast<const WalRecordUndoTxnSlotAllocate *>(self))->DumpUndo(fp); }},
    {WAL_UNDO_SET_TXN_PAGE_INITED, [](const WalRecordUndo *self, FILE *fp)
        { (static_cast<const WalRecordUndoTxnSlotPageInited *>(self))->DumpUndo(fp); }},
    {WAL_UNDO_HEAP, [](const WalRecordUndo *self, FILE *fp)
        { (static_cast<const WalRecordRollbackForData *>(self))->DumpUndo(fp); }},
    {WAL_UNDO_BTREE, [](const WalRecordUndo *self, FILE *fp)
        { (static_cast<const WalRecordRollbackForData *>(self))->DumpUndo(fp); }},
    {WAL_TXN_COMMIT, [](const WalRecordUndo *self, FILE *fp)
        { (static_cast<const WalRecordTransactionCommit *>(self))->DumpUndo(fp); }},
    {WAL_TXN_ABORT, [](const WalRecordUndo *self, FILE *fp)
        { (static_cast<const WalRecordTransactionAbort *>(self))->DumpUndo(fp); }},
    {WAL_UNDO_HEAP_PAGE_ROLL_BACK, [](const WalRecordUndo *self, FILE *fp)
        { (static_cast<const WalRecordRollbackForData *>(self))->DumpUndo(fp); }},
    {WAL_UNDO_BTREE_PAGE_ROLL_BACK, [](const WalRecordUndo *self, FILE *fp)
        { (static_cast<const WalRecordRollbackForData *>(self))->DumpUndo(fp); }},
    {WAL_UNDO_RECYCLE_TXN_SLOT, [](const WalRecordUndo *self, FILE *fp)
        { (static_cast<const WalRecordRecycleSlot *>(self))->DumpUndo(fp); }}
};

void WalRecordUndo::DumpUndoRecord(const WalRecordUndo *undoRecord, FILE *fp)
{
    StorageReleasePanic(undoRecord == nullptr, MODULE_UNDO, ErrMsg("Undo record is null when dump."));
    WalType recordType = undoRecord->m_type;
    for (uint32 i = 0; i < sizeof(UNDO_WAL_DUMP_TABLE) / sizeof(UNDO_WAL_DUMP_TABLE[0]); ++i) {
        if (UNDO_WAL_DUMP_TABLE[i].type == recordType) {
            UNDO_WAL_DUMP_TABLE[i].dump(undoRecord, fp);
        }
    }
}

static const WalRecordCompressAndDecompressItem COMPRESS_AND_DECOMPRESS_TABLE[MAX_UNDO_WAL_TYPE_SIZE] {
    {WAL_UNDO_INIT_MAP_SEGMENT, sizeof(WalRecordUndo),
     [](WalRecord *self) { return (static_cast<WalRecordUndoZidSegmentInit *>(self))->GetMaxCompressedSize(); },
     [](const WalRecord *self, char *walRecordForPageOnDisk) {
         return (static_cast<const WalRecordUndoZidSegmentInit *>(self))->Compress(walRecordForPageOnDisk);
     },
     [](WalRecord *self, const WalRecord *compressedRecord) {
         return (static_cast<WalRecordUndoZidSegmentInit *>(self))->Decompress(compressedRecord);
     }},
    {WAL_UNDO_SET_ZONE_SEGMENT_ID, sizeof(WalRecordUndo),
     [](WalRecord *self) { return (static_cast<WalRecordUndoZoneSegmentId *>(self))->GetMaxCompressedSize(); },
     [](const WalRecord *self, char *walRecordForPageOnDisk) {
         return (static_cast<const WalRecordUndoZoneSegmentId *>(self))->Compress(walRecordForPageOnDisk);
     },
     [](WalRecord *self, const WalRecord *compressedRecord) {
         return (static_cast<WalRecordUndoZoneSegmentId *>(self))->Decompress(compressedRecord);
     }},
    {WAL_UNDO_INSERT_RECORD, sizeof(WalRecordUndo),
     [](WalRecord *self) { return (static_cast<WalRecordUndoRecord *>(self))->GetMaxCompressedSize(); },
     [](const WalRecord *self, char *walRecordForPageOnDisk) {
         return (static_cast<const WalRecordUndoRecord *>(self))->Compress(walRecordForPageOnDisk);
     },
     [](WalRecord *self, const WalRecord *compressedRecord) {
         return (static_cast<WalRecordUndoRecord *>(self))->Decompress(compressedRecord);
     }},
    {WAL_UNDO_INIT_RECORD_SPACE, sizeof(WalRecordUndo),
     [](WalRecord *self) { return (static_cast<WalRecordUndoInitRecSpace *>(self))->GetMaxCompressedSize(); },
     [](const WalRecord *self, char *walRecordForPageOnDisk) {
         return (static_cast<const WalRecordUndoInitRecSpace *>(self))->Compress(walRecordForPageOnDisk);
     },
     [](WalRecord *self, const WalRecord *compressedRecord) {
         return (static_cast<WalRecordUndoInitRecSpace *>(self))->Decompress(compressedRecord);
     }},
    {WAL_UNDO_EXTEND_PAGE_RING_PREV_PAGE, sizeof(WalRecordUndo),
     [](WalRecord *self) { return (static_cast<WalRecordUndoRingOldPage *>(self))->GetMaxCompressedSize(); },
     [](const WalRecord *self, char *walRecordForPageOnDisk) {
         return (static_cast<const WalRecordUndoRingOldPage *>(self))->Compress(walRecordForPageOnDisk);
     },
     [](WalRecord *self, const WalRecord *compressedRecord) {
         return (static_cast<WalRecordUndoRingOldPage *>(self))->Decompress(compressedRecord);
     }},
    {WAL_UNDO_EXTEND_PAGE_RING_NEXT_PAGE, sizeof(WalRecordUndo),
     [](WalRecord *self) { return (static_cast<WalRecordUndoRingOldPage *>(self))->GetMaxCompressedSize(); },
     [](const WalRecord *self, char *walRecordForPageOnDisk) {
         return (static_cast<const WalRecordUndoRingOldPage *>(self))->Compress(walRecordForPageOnDisk);
     },
     [](WalRecord *self, const WalRecord *compressedRecord) {
         return (static_cast<WalRecordUndoRingOldPage *>(self))->Decompress(compressedRecord);
     }},
    {WAL_UNDO_EXTEND_PAGE_RING_NEW_PAGE, sizeof(WalRecordUndo),
     [](WalRecord *self) { return (static_cast<WalRecordUndoRingNewPage *>(self))->GetMaxCompressedSize(); },
     [](const WalRecord *self, char *walRecordForPageOnDisk) {
         return (static_cast<const WalRecordUndoRingNewPage *>(self))->Compress(walRecordForPageOnDisk);
     },
     [](WalRecord *self, const WalRecord *compressedRecord) {
         return (static_cast<WalRecordUndoRingNewPage *>(self))->Decompress(compressedRecord);
     }},
    {WAL_UNDO_INIT_TXN_PAGE, sizeof(WalRecordUndo),
     [](WalRecord *self) { return (static_cast<WalRecordUndoInitTxnPage *>(self))->GetMaxCompressedSize(); },
     [](const WalRecord *self, char *walRecordForPageOnDisk) {
         return (static_cast<const WalRecordUndoInitTxnPage *>(self))->Compress(walRecordForPageOnDisk);
     },
     [](WalRecord *self, const WalRecord *compressedRecord) {
         return (static_cast<WalRecordUndoInitTxnPage *>(self))->Decompress(compressedRecord);
     }},
    {WAL_UNDO_UPDATE_TXN_SLOT_PTR, sizeof(WalRecordUndoUpdateSlotUndoPtr),
     [](WalRecord *self) { return (static_cast<WalRecordUndoUpdateSlotUndoPtr *>(self))->GetMaxCompressedSize(); },
     [](const WalRecord *self, char *walRecordForPageOnDisk) {
         return (static_cast<const WalRecordUndoUpdateSlotUndoPtr *>(self))->Compress(walRecordForPageOnDisk);
     },
     [](WalRecord *self, const WalRecord *compressedRecord) {
         return (static_cast<WalRecordUndoUpdateSlotUndoPtr *>(self))->Decompress(compressedRecord);
     }},
    {WAL_UNDO_ALLOCATE_TXN_SLOT, sizeof(WalRecordUndo),
     [](WalRecord *self) { return (static_cast<WalRecordUndoTxnSlotAllocate *>(self))->GetMaxCompressedSize(); },
     [](const WalRecord *self, char *walRecordForPageOnDisk) {
         return (static_cast<const WalRecordUndoTxnSlotAllocate *>(self))->Compress(walRecordForPageOnDisk);
     },
     [](WalRecord *self, const WalRecord *compressedRecord) {
         return (static_cast<WalRecordUndoTxnSlotAllocate *>(self))->Decompress(compressedRecord);
     }},
    {WAL_UNDO_SET_TXN_PAGE_INITED, sizeof(WalRecordUndo),
     [](WalRecord *self) { return (static_cast<WalRecordUndoTxnSlotPageInited *>(self))->GetMaxCompressedSize(); },
     [](const WalRecord *self, char *walRecordForPageOnDisk) {
         return (static_cast<const WalRecordUndoTxnSlotPageInited *>(self))->Compress(walRecordForPageOnDisk);
     },
     [](WalRecord *self, const WalRecord *compressedRecord) {
         return (static_cast<WalRecordUndoTxnSlotPageInited *>(self))->Decompress(compressedRecord);
     }},
    {WAL_UNDO_HEAP, sizeof(WalRecordRollbackForData),
     [](WalRecord *self) { return (static_cast<WalRecordRollbackForData *>(self))->GetMaxCompressedSize(); },
     [](const WalRecord *self, char *walRecordForPageOnDisk) {
         return (static_cast<const WalRecordRollbackForData *>(self))->Compress(walRecordForPageOnDisk);
     },
     [](WalRecord *self, const WalRecord *compressedRecord) {
         return (static_cast<WalRecordRollbackForData *>(self))->Decompress(compressedRecord);
     }},
    {WAL_UNDO_BTREE, sizeof(WalRecordRollbackForData),
     [](WalRecord *self) { return (static_cast<WalRecordRollbackForData *>(self))->GetMaxCompressedSize(); },
     [](const WalRecord *self, char *walRecordForPageOnDisk) {
         return (static_cast<const WalRecordRollbackForData *>(self))->Compress(walRecordForPageOnDisk);
     },
     [](WalRecord *self, const WalRecord *compressedRecord) {
         return (static_cast<WalRecordRollbackForData *>(self))->Decompress(compressedRecord);
     }},
    {WAL_UNDO_HEAP_PAGE_ROLL_BACK, sizeof(WalRecordRollbackForData),
     [](WalRecord *self) { return (static_cast<WalRecordRollbackForData *>(self))->GetMaxCompressedSize(); },
     [](const WalRecord *self, char *walRecordForPageOnDisk) {
         return (static_cast<const WalRecordRollbackForData *>(self))->Compress(walRecordForPageOnDisk);
     },
     [](WalRecord *self, const WalRecord *compressedRecord) {
         return (static_cast<WalRecordRollbackForData *>(self))->Decompress(compressedRecord);
     }},
    {WAL_UNDO_BTREE_PAGE_ROLL_BACK, sizeof(WalRecordRollbackForData),
     [](WalRecord *self) { return (static_cast<WalRecordRollbackForData *>(self))->GetMaxCompressedSize(); },
     [](const WalRecord *self, char *walRecordForPageOnDisk) {
         return (static_cast<const WalRecordRollbackForData *>(self))->Compress(walRecordForPageOnDisk);
     },
     [](WalRecord *self, const WalRecord *compressedRecord) {
         return (static_cast<WalRecordRollbackForData *>(self))->Decompress(compressedRecord);
     }},
    {WAL_UNDO_RECYCLE_TXN_SLOT, sizeof(WalRecordUndo),
     [](WalRecord *self) { return (static_cast<WalRecordRecycleSlot *>(self))->GetMaxCompressedSize(); },
     [](const WalRecord *self, char *walRecordForPageOnDisk) {
         return (static_cast<const WalRecordRecycleSlot *>(self))->Compress(walRecordForPageOnDisk);
     },
     [](WalRecord *self, const WalRecord *compressedRecord) {
         return (static_cast<WalRecordRecycleSlot *>(self))->Decompress(compressedRecord);
     }},
    {WAL_TXN_COMMIT, sizeof(WalRecordUndo),
     [](WalRecord *self) { return (static_cast<WalRecordTransactionCommit *>(self))->GetMaxCompressedSize(); },
     [](const WalRecord *self, char *walRecordForPageOnDisk) {
         return (static_cast<const WalRecordTransactionCommit *>(self))->Compress(walRecordForPageOnDisk);
     },
     [](WalRecord *self, const WalRecord *compressedRecord) {
         return (static_cast<WalRecordTransactionCommit *>(self))->Decompress(compressedRecord);
     }},

    {WAL_TXN_ABORT, sizeof(WalRecordUndo),
     [](WalRecord *self) { return (static_cast<WalRecordTransactionAbort *>(self))->GetMaxCompressedSize(); },
     [](const WalRecord *self, char *walRecordForPageOnDisk) {
         return (static_cast<const WalRecordTransactionAbort *>(self))->Compress(walRecordForPageOnDisk);
     },
     [](WalRecord *self, const WalRecord *compressedRecord) {
         return (static_cast<WalRecordTransactionAbort *>(self))->Decompress(compressedRecord);
    }}
};

const WalRecordCompressAndDecompressItem *WalRecordUndo::GetWalRecordItem(WalType walType)
{
    StorageAssert(walType >= WAL_UNDO_INIT_MAP_SEGMENT && walType <= WAL_TXN_ABORT);
    for (auto &item : DSTORE::COMPRESS_AND_DECOMPRESS_TABLE) {
        if (walType == item.type) {
            return &item;
        }
    }
    ErrLog(DSTORE_WARNING, MODULE_UNDO, ErrMsg("walType:%s isnot in compress table", g_walTypeForPrint[walType]));
    return nullptr;
}
uint16 WalRecordUndo::GetUncompressedHeaderSize(const WalRecord *uncompressRecord)
{
    uint16 headerSize = 0;
    WalType walType = uncompressRecord->GetType();
    StorageAssert(walType >= WAL_UNDO_INIT_MAP_SEGMENT && walType <= WAL_TXN_ABORT);
    for (auto &item : DSTORE::COMPRESS_AND_DECOMPRESS_TABLE) {
        if (walType == item.type) {
            headerSize = item.headerSize;
            return headerSize;
        }
    }
    ErrLog(DSTORE_WARNING, MODULE_UNDO, ErrMsg("walType:%s isnot in compress table", g_walTypeForPrint[walType]));
    return sizeof(WalRecordUndo);
}

} /* namespace DSTORE */