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
 * dstore_heap_wal_struct.h
 *
 * IDENTIFICATION
 *        include/heap/dstore_heap_wal_struct.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_HEAP_WAL_STRUCT_H
#define DSTORE_HEAP_WAL_STRUCT_H

#include "page/dstore_td.h"
#include "wal/dstore_wal_struct.h"
#include "page/dstore_heap_page.h"
#include "heap/dstore_heap_update.h"

namespace DSTORE {

struct WalRecordHeap : public WalRecordForDataPage {
    static void RedoHeapRecord(WalRecordRedoContext *redoCtx, const WalRecordHeap *heapRecord,
        BufferDesc *bufferDesc);

    static void DumpHeapRecord(const WalRecordHeap *heapRecord, FILE *fp);
    inline void SetContainLoigcalInfoFlag(bool hasLogicalInfo)
    {
        if (hasLogicalInfo) {
            m_flags.m_flag.containLogicalInfoFlag = 1;
        } else {
            m_flags.m_flag.containLogicalInfoFlag = 0;
        }
    }
    inline bool IsContainLoigcalInfo() const
    {
        return m_flags.m_flag.containLogicalInfoFlag == 1;
    }
    inline void SetDecodeDictChangeFlag(bool decodeDictChangeFlag)
    {
        if (decodeDictChangeFlag) {
            m_flags.m_flag.decodeDictChangeFlag = 1;
        } else {
            m_flags.m_flag.decodeDictChangeFlag = 0;
        }
    }
    inline bool IsDecodeDictChange() const
    {
        return m_flags.m_flag.decodeDictChangeFlag == 1;
    }
    inline void SetDeleteContainsReplicaKeyFlag(bool containsReplicaKey)
    {
        if (containsReplicaKey) {
            m_flags.m_flag.heapDeleteContainsReplicaKeyFlag = 1;
        } else {
            m_flags.m_flag.heapDeleteContainsReplicaKeyFlag = 0;
        }
    }
    inline bool IsDeleteContainsReplicaKey() const
    {
        return m_flags.m_flag.heapDeleteContainsReplicaKeyFlag == 1;
    }
    inline void SetUpdateContainsReplicaKeyFlag(bool containsReplicaKey)
    {
        if (containsReplicaKey) {
            m_flags.m_flag.heapUpdateContainsReplicaKeyFlag = 1;
        } else {
            m_flags.m_flag.heapUpdateContainsReplicaKeyFlag = 0;
        }
    }
    inline bool IsUpdateContainsReplicaKey() const
    {
        return m_flags.m_flag.heapUpdateContainsReplicaKeyFlag == 1;
    }
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordHeap);

struct WalRecordHeapInsert : public WalRecordHeap {
    OffsetNumber offset;
    uint64 undoRecPtr;
    /* if wal level < WAL_LEVEL_LOGICAL: HeapDiskTuple + AllocTd
     * if wal level >= WAL_LEVEL_LOGICAL:
     * HeapDiskTuple + AllocTd + tableOid + snapshotCsn + commandId (if it changed decode dict) */
    char rawData[];

    void SetHeader(const WalPageHeaderContext &heapHeader, OffsetNumber offsetN, UndoRecPtr ptr)
    {
        SetWalPageHeader(heapHeader);
        offset = offsetN;
        undoRecPtr = ptr.m_placeHolder;
    }
    inline void SetAllocTd(TDAllocContext &context)
    {
        HeapDiskTuple *tuple = static_cast<HeapDiskTuple *>(static_cast<void*>(rawData));
        SetAllocTdWal(context, rawData + tuple->GetTupleSize(),
            sizeof(AllocTdRecord) + sizeof(TrxSlotStatus) * (context.allocTd.tdNum));
    }
    void SetLogicalDecodeInfo(uint32 fromPos, Oid *tableOid, CommitSeqNo *snapshotCsn, CommandId *tupleCid)
    {
        SetContainLoigcalInfoFlag(true);
        CopyData(rawData + fromPos, sizeof(Oid), static_cast<char *>(static_cast<void *>(tableOid)), sizeof(Oid));
        fromPos += sizeof(Oid);
        CopyData(rawData + fromPos, sizeof(CommitSeqNo),
                 static_cast<char *>(static_cast<void *>(snapshotCsn)), sizeof(CommitSeqNo));
        if (unlikely(*tupleCid != INVALID_CID)) {
            SetDecodeDictChangeFlag(true);
            fromPos += sizeof(CommitSeqNo);
            CopyData(rawData + fromPos, sizeof(CommandId),
                     static_cast<char *>(static_cast<void *>(tupleCid)), sizeof(CommandId));
        }
    }
    OffsetNumber GetOffset() const
    {
        return offset;
    }
    const HeapDiskTuple* GetInsertTuple() const
    {
        const HeapDiskTuple* intertTuple = static_cast<const HeapDiskTuple *>(static_cast<const void*>(rawData));
        return intertTuple;
    }
    void GetLogicalDecodeInfo(const Oid *&tableOid, const CommitSeqNo *&snapshotCsn, const CommandId *&tupleCid) const
    {
        StorageAssert(IsContainLoigcalInfo());
        uint16 backOffset = 0;
        uint16 rawDataSize = m_size - sizeof(WalRecordHeapInsert);
        if (unlikely(IsDecodeDictChange())) {
            backOffset += sizeof(CommandId);
            tupleCid = static_cast<const CommandId*>(static_cast<const void *>(rawData + rawDataSize - backOffset));
        } else {
            tupleCid = nullptr;
        }
        backOffset += sizeof(CommitSeqNo);
        snapshotCsn = static_cast<const CommitSeqNo*>(static_cast<const void *>(rawData + rawDataSize - backOffset));
        backOffset += sizeof(Oid);
        tableOid = static_cast<const Oid*>(static_cast<const void *>(rawData + rawDataSize - backOffset));
    }
    void SetData(char *data, uint32 size);
    void DescLogicalInfo(FILE *fp) const
    {
        const Oid *tableOid;
        const CommitSeqNo *snapshotCsn;
        const CommandId *tupleCid;
        GetLogicalDecodeInfo(tableOid, snapshotCsn, tupleCid);
        (void)fprintf(fp, "tableOid: %u snapshotCsn: %lu tupleCid: %u", *tableOid, *snapshotCsn, *tupleCid);
    }
    void Dump(FILE *fp) const
    {
        WalRecordForPage::Dump(fp);
        UndoRecPtr undoPtr(undoRecPtr);
        const HeapDiskTuple *tuple = static_cast<const HeapDiskTuple*>(static_cast<const void*>(rawData));
        (void)fprintf(fp, "offset(%hu), undoPtr(%hu, %u, %hu), HeapDiskTuple{tdId(%hhu), lockerTdId(%hhu), size(%hu), "
            "info(%x).", offset, undoPtr.GetFileId(), undoPtr.GetBlockNum(), undoPtr.GetOffset(), tuple->GetTdId(),
            tuple->GetLockerTdId(), tuple->GetTupleSize(), tuple->m_info.m_info);

        if (m_size == sizeof(WalRecordHeapInsert) + tuple->GetTupleSize()) {
            return;
        }
        uint32 tdSize = 0;
        if (!IsContainLoigcalInfo()) {
            tdSize = m_size - (sizeof(WalRecordHeapInsert) + tuple->GetTupleSize());
        } else {
            tdSize = m_size - (sizeof(WalRecordHeapInsert) + tuple->GetTupleSize() + sizeof(Oid) + sizeof(CommitSeqNo));
            if (IsDecodeDictChange()) {
                tdSize -= sizeof(CommandId);
            }
        }
        DescAllocTd(fp, rawData + tuple->GetTupleSize(), tdSize);
        if (IsContainLoigcalInfo()) {
            DescLogicalInfo(fp);
        }
    }
    void Redo(HeapPage *page, Xid xid) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordHeapInsert);

struct WalRecordHeapBatchInsert : public WalRecordHeap {
    uint64 undoRecPtr;
    uint16 pos;
    char rawData[]; /* OffsetNumber + HeapDiskTuple + AllocTd */

    inline void SetHeader(const WalPageHeaderContext &heapHeader, UndoRecPtr ptr)
    {
        SetWalPageHeader(heapHeader);
        undoRecPtr = ptr.m_placeHolder;
        pos = 0;
    }
    inline void SetAllocTd(TDAllocContext &context)
    {
        SetAllocTdWal(context, rawData + pos, m_size - (sizeof(WalRecordHeapBatchInsert) + pos));
    }
    void AppendData(OffsetNumber offset, char *data, uint32 size);
    void Redo(HeapPage *page, Xid xid) const;
    void Dump(FILE *fp) const
    {
        WalRecordForPage::Dump(fp);
        UndoRecPtr undoPtr(undoRecPtr);
        (void)fprintf(fp, "undoPtr(%hu, %u, %hu), pos(%hu), BatchTupleInfo: ",
            undoPtr.GetFileId(), undoPtr.GetBlockNum(), undoPtr.GetOffset(), pos);
        uint32 len = 0;
        while (len < pos) {
            OffsetNumber offset = *static_cast<const OffsetNumber*>(static_cast<const void*>(rawData + len));
            len += sizeof(OffsetNumber);
            const HeapDiskTuple* tuple = static_cast<const HeapDiskTuple*>(static_cast<const void*>(rawData + len));
            len += tuple->GetTupleSize();
            (void)fprintf(fp, "Tuple{offset(%hu), tdId(%hhu), lockerTdId(%hhu), size(%hu), info(%x)}.",
                offset, tuple->GetTdId(), tuple->GetLockerTdId(), tuple->GetTupleSize(), tuple->m_info.m_info);
        }
        if (m_size == sizeof(WalRecordHeapBatchInsert) + pos) {
            return;
        }
        DescAllocTd(fp, rawData + pos, m_size - (sizeof(WalRecordHeapBatchInsert) + pos));
    }
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordHeapBatchInsert);

struct WalRecordHeapDelete : public WalRecordHeap {
    uint8 tdId;
    OffsetNumber offset;
    uint64 undoRecPtr;
    /* if wal level < WAL_LEVEL_LOGICAL: AllocTd
     * if wal level >= WAL_LEVEL_LOGICAL:
     * delete replicaKey + tableOid + snapshotCsn + commandId (if it changed decode dict) + AllocTd */
    char rawData[];

    inline void SetHeader(const WalPageHeaderContext &heapHeader, OffsetNumber offsetN, UndoRecPtr ptr, uint8 tdIdN)
    {
        SetWalPageHeader(heapHeader);
        offset = offsetN;
        undoRecPtr = ptr.m_placeHolder;
        tdId = tdIdN;
    }
    void SetLogicalDecodeInfo(HeapDiskTuple *oldTupleKey, Oid *tableOid, CommitSeqNo *snapshotCsn, CommandId *tupleCid)
    {
        SetContainLoigcalInfoFlag(true);
        SetDeleteContainsReplicaKeyFlag(true);
        uint32 fromPos = 0;
        CopyData(rawData + fromPos, oldTupleKey->GetTupleSize(),
                 static_cast<char *>(static_cast<void *>(oldTupleKey)), oldTupleKey->GetTupleSize());
        fromPos += oldTupleKey->GetTupleSize();
        CopyData(rawData + fromPos, sizeof(Oid), static_cast<char *>(static_cast<void *>(tableOid)), sizeof(Oid));
        fromPos += sizeof(Oid);
        CopyData(rawData + fromPos, sizeof(CommitSeqNo),
                 static_cast<char *>(static_cast<void *>(snapshotCsn)), sizeof(CommitSeqNo));
        if (unlikely(*tupleCid != INVALID_CID)) {
            SetDecodeDictChangeFlag(true);
            fromPos += sizeof(CommitSeqNo);
            CopyData(rawData + fromPos, sizeof(CommandId),
                     static_cast<char *>(static_cast<void *>(tupleCid)), sizeof(CommandId));
        }
    }
    inline void SetAllocTd(TDAllocContext &context)
    {
        uint32 fromPos = 0;
        if (IsContainLoigcalInfo()) {
            StorageAssert(IsDeleteContainsReplicaKey());
            HeapDiskTuple *tuple = static_cast<HeapDiskTuple *>(static_cast<void *>(rawData));
            fromPos += tuple->GetTupleSize();
            fromPos = fromPos + sizeof(Oid) + sizeof(CommitSeqNo);
            if (unlikely(IsDecodeDictChange())) {
                fromPos += sizeof(CommandId);
            }
        }
        SetAllocTdWal(context, rawData + fromPos, m_size - sizeof(WalRecordHeapDelete) - fromPos);
    }
    OffsetNumber GetOffset() const
    {
        return offset;
    }
    void GetLogicalDecodeInfo(const HeapDiskTuple *&oldTupleKey, const Oid *&tableOid,
                              const CommitSeqNo *&snapshotCsn, const CommandId *&tupleCid) const
    {
        StorageAssert(IsContainLoigcalInfo());
        StorageAssert(IsDeleteContainsReplicaKey());
        uint32 fromPos = 0;
        oldTupleKey = static_cast<const HeapDiskTuple*>(static_cast<const void *>(rawData + fromPos));
        fromPos += oldTupleKey->GetTupleSize();
        tableOid = static_cast<const Oid*>(static_cast<const void *>(rawData + fromPos));
        fromPos += sizeof(Oid);
        snapshotCsn = static_cast<const CommitSeqNo*>(static_cast<const void *>(rawData + fromPos));
        if (unlikely(IsDecodeDictChange())) {
            fromPos += sizeof(CommitSeqNo);
            tupleCid = static_cast<const CommandId*>(static_cast<const void *>(rawData + fromPos));
        } else {
            tupleCid = nullptr;
        }
    }
    void DescLogicalInfo(FILE *fp) const
    {
        const HeapDiskTuple *oldTupleKey;
        const Oid *tableOid;
        const CommitSeqNo *snapshotCsn;
        const CommandId *tupleCid;
        GetLogicalDecodeInfo(oldTupleKey, tableOid, snapshotCsn, tupleCid);
        StorageReleasePanic(tableOid == nullptr || snapshotCsn == nullptr || tupleCid == nullptr, MODULE_WAL,
                            ErrMsg("DescLogicalInfo failed because of dereferencing nullptr."));
        (void)fprintf(fp, "tableOid: %u snapshotCsn: %lu tupleCid: %u", *tableOid, *snapshotCsn, *tupleCid);
    }
    void Dump(FILE *fp) const
    {
        WalRecordForPage::Dump(fp);
        UndoRecPtr undoPtr(undoRecPtr);
        (void)fprintf(fp, "offset(%hu), tdId(%hhu), undoPtr(%hu, %u, %hu).", offset, tdId,
            undoPtr.GetFileId(), undoPtr.GetBlockNum(), undoPtr.GetOffset());
        if (m_size == sizeof(WalRecordHeapDelete)) {
            return;
        }
        uint32 fromPos = 0;
        if (IsContainLoigcalInfo()) {
            StorageAssert(IsDeleteContainsReplicaKey());
            const HeapDiskTuple *tuple = static_cast<const HeapDiskTuple *>(static_cast<const void *>(rawData));
            fromPos += tuple->GetTupleSize();
            fromPos = fromPos + sizeof(Oid) + sizeof(CommitSeqNo);
            if (unlikely(IsDecodeDictChange())) {
                fromPos += sizeof(CommandId);
            }
        }
        DescAllocTd(fp, rawData + fromPos, m_size - sizeof(WalRecordHeapDelete) - fromPos);

        if (IsContainLoigcalInfo()) {
            DescLogicalInfo(fp);
        }
    }
    void Redo(HeapPage *page, Xid xid) const;
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordHeapDelete);

struct WalRecordHeapInplaceUpdateHeaderContext {
    OffsetNumber offsetN;
    UndoRecPtr ptr;
    uint8 tdIdN;
    uint16 size;
    uint32 tupleHead;
};
struct WalRecordHeapInplaceUpdate : public WalRecordHeap {
    OffsetNumber offset;
    uint64 undoRecPtr;
    uint8 tdId;
    uint16 diskTupleSize;
    uint16 numDiff;
    uint16 pos;
    uint32 tupleHeadInfo;
    /* if wal level < WAL_LEVEL_LOGICAL : startPos + endPos + diff + AllocTd */
    /* if wal level >= WAL_LEVEL_LOGICAL:
     * startPos + endPos + diff + oldTuple + tableOid + snapshotCsn + commandId (if it changed decode dict) + AllocTd */
    char rawData[];

    inline void SetHeader(const WalPageHeaderContext &heapHeader,
                          const WalRecordHeapInplaceUpdateHeaderContext &inplaceUpdateHeader)
    {
        SetWalPageHeader(heapHeader);
        offset = inplaceUpdateHeader.offsetN;
        undoRecPtr = inplaceUpdateHeader.ptr.m_placeHolder;
        tdId = inplaceUpdateHeader.tdIdN;
        diskTupleSize = inplaceUpdateHeader.size;
        tupleHeadInfo = inplaceUpdateHeader.tupleHead;
    }
    void SetLogicalDecodeInfo(HeapDiskTuple *oldTupleKey, Oid *tableOid, CommitSeqNo *snapshotCsn, CommandId *tupleCid)
    {
        SetContainLoigcalInfoFlag(true);
        SetUpdateContainsReplicaKeyFlag(true);
        uint32 fromPos = pos;
        CopyData(rawData + fromPos, oldTupleKey->GetTupleSize(),
                 static_cast<char *>(static_cast<void *>(oldTupleKey)), oldTupleKey->GetTupleSize());
        fromPos += oldTupleKey->GetTupleSize();
        CopyData(rawData + fromPos, sizeof(Oid), static_cast<char *>(static_cast<void *>(tableOid)), sizeof(Oid));
        fromPos += sizeof(Oid);
        CopyData(rawData + fromPos, sizeof(CommitSeqNo),
                 static_cast<char *>(static_cast<void *>(snapshotCsn)), sizeof(CommitSeqNo));
        if (unlikely(*tupleCid != INVALID_CID)) {
            SetDecodeDictChangeFlag(true);
            fromPos += sizeof(CommitSeqNo);
            CopyData(rawData + fromPos, sizeof(CommandId),
                     static_cast<char *>(static_cast<void *>(tupleCid)), sizeof(CommandId));
        }
    }
    inline void SetAllocTd(TDAllocContext &context)
    {
        uint32 fromPos = pos;
#ifdef ENABLE_LOGICAL_REPL
        if (IsContainLoigcalInfo()) {
            StorageAssert(IsUpdateContainsReplicaKey());
            HeapDiskTuple *tuple = static_cast<HeapDiskTuple *>(static_cast<void *>(rawData + pos));
            fromPos += tuple->GetTupleSize();
            fromPos = fromPos + sizeof(Oid) + sizeof(CommitSeqNo);
            if (unlikely(IsDecodeDictChange())) {
                fromPos += sizeof(CommandId);
            }
        }
#endif
        SetAllocTdWal(context, rawData + fromPos, m_size - sizeof(WalRecordHeapInplaceUpdate) - fromPos);
    }
    OffsetNumber GetOffset() const
    {
        return offset;
    }
    void GetUpdateTupleLen(uint32 &oldTupLen, uint32 &newTupLen) const
    {
        StorageAssert(IsUpdateContainsReplicaKey());
        const HeapDiskTuple *oldTupleKey = static_cast<const HeapDiskTuple *>(static_cast<const void *>(rawData + pos));
        oldTupLen = oldTupleKey->GetTupleSize();
        newTupLen = diskTupleSize;
    }
    void GetUpdateTuple(HeapDiskTuple *oldTup, HeapDiskTuple *newTup) const
    {
        /* memory should be allocated outside */
        StorageAssert(IsUpdateContainsReplicaKey());
        const HeapDiskTuple *oldTuple = static_cast<const HeapDiskTuple *>(static_cast<const void *>(rawData + pos));
        Size oldTupSize = oldTuple->GetTupleSize();
        errno_t rc = memcpy_s(oldTup, oldTupSize, rawData + pos, oldTupSize);
        storage_securec_check(rc, "\0", "\0");
        /* now, form new tuple */
        Size newTupDataSize = diskTupleSize - HEAP_DISK_TUP_HEADER_SIZE;
        char *newTupData = newTup->GetData();
        Size oldTupDataSize = oldTupSize - HEAP_DISK_TUP_HEADER_SIZE;
        char *oldTupData = oldTup->GetData();
        if (diskTupleSize > oldTupSize) {
            Size dataOffset = diskTupleSize - oldTupSize;
            rc = memcpy_s(newTupData + dataOffset, oldTupDataSize, oldTupData, oldTupDataSize);
            storage_securec_check(rc, "\0", "\0");
        } else if (diskTupleSize < oldTupSize) {
            Size dataOffset = oldTupSize - diskTupleSize;
            rc = memcpy_s(newTupData, newTupDataSize, oldTupData + dataOffset, newTupDataSize);
            storage_securec_check(rc, "\0", "\0");
        } else {
            rc = memcpy_s(newTupData, newTupDataSize, oldTupData, newTupDataSize);
            storage_securec_check(rc, "\0", "\0");
        }
        uint16 start, end;
        const char *ptr = rawData;
        uint16 dataEndOffset = static_cast<uint16>(newTupDataSize - 1);
        char *dataEnd = newTupData + dataEndOffset;
        for (uint16 i = 0; i < numDiff; i += NUM_DIFF_STEP) {
            start = *static_cast<const uint16 *>(static_cast<const void *>(ptr));
            end = *static_cast<const uint16 *>(static_cast<const void *>(ptr + sizeof(start)));
            uint16 len = static_cast<uint16>(end - start);
            uint16 destOffset = static_cast<uint16>(end - 1);
            rc = memcpy_s(dataEnd - destOffset, len, ptr + sizeof(start) + sizeof(end), len);
            storage_securec_check(rc, "\0", "\0");
            ptr += sizeof(start) + sizeof(end) + end - start;
        }
        newTup->SetTupleSize(diskTupleSize);
        newTup->m_info.m_info = tupleHeadInfo;
    }
    void GetLogicalDecodeInfo(const Oid *&tableOid, const CommitSeqNo *&snapshotCsn, const CommandId *&tupleCid) const
    {
        StorageAssert(IsContainLoigcalInfo());
        uint32 fromPos = pos;
        const HeapDiskTuple *tuple = static_cast<const HeapDiskTuple*>(static_cast<const void *>(rawData + fromPos));
        fromPos += tuple->GetTupleSize();
        tableOid = static_cast<const Oid*>(static_cast<const void *>(rawData + fromPos));
        fromPos += sizeof(Oid);
        snapshotCsn = static_cast<const CommitSeqNo*>(static_cast<const void *>(rawData + fromPos));
        if (unlikely(IsDecodeDictChange())) {
            fromPos += sizeof(CommitSeqNo);
            tupleCid = static_cast<const CommandId*>(static_cast<const void *>(rawData + fromPos));
        } else {
            tupleCid = nullptr;
        }
    }
    void DescLogicalInfo(FILE *fp) const
    {
        const Oid *tableOid;
        const CommitSeqNo *snapshotCsn;
        const CommandId *tupleCid;
        GetLogicalDecodeInfo(tableOid, snapshotCsn, tupleCid);
        StorageAssert(tupleCid != nullptr);
        (void)fprintf(fp, "tableOid: %u snapshotCsn: %lu tupleCid: %u", *tableOid, *snapshotCsn, *tupleCid);
    }
    void SetData(uint16 numDiffPos, uint16 *diffPos, char *newTupleData);
    uint16 AppendData(uint16 fromPos, char *data, uint16 size);
    void Redo(HeapPage *page, Xid xid) const;
    void Dump(FILE *fp) const
    {
        WalRecordForPage::Dump(fp);
        UndoRecPtr undoPtr(undoRecPtr);
        (void)fprintf(fp, "offset(%hu), undoPtr(%hu, %u, %hu), tdId(%hhu), diskTupleSize(%hu), numDiff(%hu), pos(%hu), "
            "tupleHeadInfo(%x).", offset, undoPtr.GetFileId(), undoPtr.GetBlockNum(), undoPtr.GetOffset(),
            tdId, diskTupleSize, numDiff, pos, tupleHeadInfo);
        const char *ptr = rawData;
        for (uint16 i = 0; i < numDiff; i += NUM_DIFF_STEP) {
            uint16 start = *static_cast<const uint16 *>(static_cast<const void *>(ptr));
            uint16 end = *(static_cast<const uint16 *>(static_cast<const void *>(ptr)) + 1);
            (void)fprintf(fp, "[start]:%hu,[end]:%hu.", start, end);
            ptr += (end - start + (sizeof(start) * NUM_DIFF_STEP));
        }
        if (m_size == sizeof(WalRecordHeapInplaceUpdate) + pos) {
            return;
        }
        uint32 fromPos = pos;
        if (IsContainLoigcalInfo()) {
            StorageAssert(IsUpdateContainsReplicaKey());
            const HeapDiskTuple *tuple = static_cast<const HeapDiskTuple *>(static_cast<const void *>(rawData + pos));
            fromPos += tuple->GetTupleSize();
            fromPos = fromPos + sizeof(Oid) + sizeof(CommitSeqNo);
            if (unlikely(IsDecodeDictChange())) {
                fromPos += sizeof(CommandId);
            }
        }
        DescAllocTd(fp, rawData + fromPos, m_size - (sizeof(WalRecordHeapInplaceUpdate) + fromPos));
        if (IsContainLoigcalInfo()) {
            DescLogicalInfo(fp);
        }
    }
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordHeapInplaceUpdate);

struct WalRecordHeapSamePageAppendUpdate : public WalRecordHeap {
    OffsetNumber offset;
    uint64 undoRecPtr;
    /* if wal level < WAL_LEVEL_LOGICAL: newHeapDiskTuple + AllocTd
     * if wal level >= WAL_LEVEL_LOGICAL:
     * newHeapDiskTuple + oldTupleKey + tableOid + snapshotCsn + commandId (if it changed decode dict) + AllocTd */
    char rawData[];

    inline void SetHeader(const WalPageHeaderContext &heapHeader, OffsetNumber offsetN, UndoRecPtr ptr)
    {
        SetWalPageHeader(heapHeader);
        offset = offsetN;
        undoRecPtr = ptr.m_placeHolder;
    }
    void SetLogicalDecodeInfo(HeapDiskTuple *oldTupleKey, Oid *tableOid, CommitSeqNo *snapshotCsn, CommandId *tupleCid)
    {
        SetContainLoigcalInfoFlag(true);
        SetUpdateContainsReplicaKeyFlag(true);
        HeapDiskTuple *newTuple = static_cast<HeapDiskTuple*>(static_cast<void*>(rawData));
        uint32 fromPos = newTuple->GetTupleSize();
        CopyData(rawData + fromPos, oldTupleKey->GetTupleSize(),
                 static_cast<char *>(static_cast<void *>(oldTupleKey)), oldTupleKey->GetTupleSize());
        fromPos += oldTupleKey->GetTupleSize();
        CopyData(rawData + fromPos, sizeof(Oid), static_cast<char *>(static_cast<void *>(tableOid)), sizeof(Oid));
        fromPos += sizeof(Oid);
        CopyData(rawData + fromPos, sizeof(CommitSeqNo),
                 static_cast<char *>(static_cast<void *>(snapshotCsn)), sizeof(CommitSeqNo));
        if (unlikely(*tupleCid != INVALID_CID)) {
            SetDecodeDictChangeFlag(true);
            fromPos += sizeof(CommitSeqNo);
            CopyData(rawData + fromPos, sizeof(CommandId),
                     static_cast<char *>(static_cast<void *>(tupleCid)), sizeof(CommandId));
        }
    }
    inline void SetAllocTd(TDAllocContext &context)
    {
        HeapDiskTuple *tuple = static_cast<HeapDiskTuple*>(static_cast<void*>(rawData));
        uint32 fromPos = tuple->GetTupleSize();
        if (IsContainLoigcalInfo()) {
            StorageAssert(IsUpdateContainsReplicaKey());
            HeapDiskTuple *oldTuple = static_cast<HeapDiskTuple *>(static_cast<void *>(rawData + fromPos));
            fromPos += oldTuple->GetTupleSize();
            fromPos = fromPos + sizeof(Oid) + sizeof(CommitSeqNo);
            if (unlikely(IsDecodeDictChange())) {
                fromPos += sizeof(CommandId);
            }
        }
        SetAllocTdWal(context, rawData + fromPos, m_size - (sizeof(WalRecordHeapSamePageAppendUpdate) + fromPos));
    }
    void GetUpdateTupleLen(uint32 &oldTupLen, uint32 &newTupLen) const
    {
        StorageAssert(IsUpdateContainsReplicaKey());
        const HeapDiskTuple *newTuple = static_cast<const HeapDiskTuple *>(static_cast<const void *>(rawData));
        newTupLen = newTuple->GetTupleSize();
        const HeapDiskTuple *oldTuple = static_cast<const HeapDiskTuple *>(
            static_cast<const void *>(rawData + newTuple->GetTupleSize()));
        oldTupLen = oldTuple->GetTupleSize();
    }
    void GetUpdateTuple(HeapDiskTuple *oldTup, HeapDiskTuple *newTup) const
    {
        StorageAssert(IsUpdateContainsReplicaKey());
        const HeapDiskTuple *newTuple = static_cast<const HeapDiskTuple *>(static_cast<const void *>(rawData));
        errno_t rc = memcpy_s(static_cast<void *>(newTup), newTuple->GetTupleSize(), rawData, newTuple->GetTupleSize());
        storage_securec_check(rc, "\0", "\0");
        const HeapDiskTuple *oldTupleKey =
            static_cast<const HeapDiskTuple *>(static_cast<const void *>(rawData + newTuple->GetTupleSize()));
        rc = memcpy_s(static_cast<void *>(oldTup), oldTupleKey->GetTupleSize(),
                      rawData + newTuple->GetTupleSize(), oldTupleKey->GetTupleSize());
        storage_securec_check(rc, "\0", "\0");
    }
    void GetLogicalDecodeInfo(const Oid *&tableOid, const CommitSeqNo *&snapshotCsn, const CommandId *&tupleCid) const
    {
        StorageAssert(IsContainLoigcalInfo());
        const HeapDiskTuple *newTuple = static_cast<const HeapDiskTuple *>(static_cast<const void *>(rawData));
        uint32 fromPos = newTuple->GetTupleSize();
        const HeapDiskTuple *oldKey = static_cast<const HeapDiskTuple*>(static_cast<const void *>(rawData + fromPos));
        fromPos += oldKey->GetTupleSize();
        tableOid = static_cast<const Oid*>(static_cast<const void *>(rawData + fromPos));
        fromPos += sizeof(Oid);
        snapshotCsn = static_cast<const CommitSeqNo*>(static_cast<const void *>(rawData + fromPos));
        if (unlikely(IsDecodeDictChange())) {
            fromPos += sizeof(CommitSeqNo);
            tupleCid = static_cast<const CommandId*>(static_cast<const void *>(rawData + fromPos));
        } else {
            tupleCid = nullptr;
        }
    }
    void DescLogicalInfo(FILE *fp) const
    {
        const Oid *tableOid;
        const CommitSeqNo *snapshotCsn;
        const CommandId *tupleCid;
        GetLogicalDecodeInfo(tableOid, snapshotCsn, tupleCid);
        (void)fprintf(fp, "tableOid: %u snapshotCsn: %lu tupleCid: %u", *tableOid, *snapshotCsn, *tupleCid);
    }
    void Redo(HeapPage *page, Xid xid) const;
    void Dump(FILE *fp) const
    {
        WalRecordForPage::Dump(fp);
        const HeapDiskTuple* tuple = static_cast<const HeapDiskTuple*>(static_cast<const void*>(rawData));
        UndoRecPtr undoPtr(undoRecPtr);
        (void)fprintf(fp, "offset(%hu), undoPtr(%hu, %u, %hu), HeapDiskTuple{tdId(%hhu), lockerTdId(%hhu), size(%hu), "
            "info(%x)}.", offset, undoPtr.GetFileId(), undoPtr.GetBlockNum(), undoPtr.GetOffset(), tuple->GetTdId(),
            tuple->GetLockerTdId(), tuple->GetTupleSize(), tuple->m_info.m_info);

        if (m_size == sizeof(WalRecordHeapSamePageAppendUpdate) + tuple->GetTupleSize()) {
            return;
        }
        uint32 fromPos = tuple->GetTupleSize();
        if (IsContainLoigcalInfo()) {
            StorageAssert(IsUpdateContainsReplicaKey());
            const HeapDiskTuple *oldTupleKey =
                static_cast<const HeapDiskTuple *>(static_cast<const void *>(rawData + fromPos));
            fromPos += oldTupleKey->GetTupleSize();
            fromPos = fromPos + sizeof(Oid) + sizeof(CommitSeqNo);
            if (unlikely(IsDecodeDictChange())) {
                fromPos += sizeof(CommandId);
            }
        }
        DescAllocTd(fp, rawData + fromPos, m_size - (sizeof(WalRecordHeapSamePageAppendUpdate) + fromPos));
        if (IsContainLoigcalInfo()) {
            DescLogicalInfo(fp);
        }
    }
    OffsetNumber GetOffset() const
    {
        return offset;
    }
    void SetData(char *data, uint32 size);
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordHeapSamePageAppendUpdate);

struct WalRecordHeapAnotherPageAppendUpdateOldPage : public WalRecordHeap {
    OffsetNumber offset;
    uint64 undoRecPtr;
    uint8 tdId;
    char rawData[]; /* AllocTd */

    inline void SetHeader(const WalPageHeaderContext &heapHeader, OffsetNumber offsetN, UndoRecPtr ptr, uint8 tdIdN)
    {
        SetWalPageHeader(heapHeader);
        offset = offsetN;
        undoRecPtr = ptr.m_placeHolder;
        tdId = tdIdN;
    }
    inline void SetAllocTd(TDAllocContext &context)
    {
        SetAllocTdWal(context, rawData, m_size - sizeof(WalRecordHeapAnotherPageAppendUpdateOldPage));
    }
    OffsetNumber GetOffset() const
    {
        return offset;
    }
    void Redo(HeapPage *page, Xid xid) const;
    void Dump(FILE *fp) const
    {
        WalRecordForPage::Dump(fp);
        UndoRecPtr undoPtr(undoRecPtr);
        (void)fprintf(fp, "offset(%hu), undoPtr(%hu, %u, %hu), tdId(%hhu).", offset, undoPtr.GetFileId(),
            undoPtr.GetBlockNum(), undoPtr.GetOffset(), tdId);
        if (m_size == sizeof(WalRecordHeapAnotherPageAppendUpdateOldPage)) {
            return;
        }
        DescAllocTd(fp, rawData, m_size - sizeof(WalRecordHeapAnotherPageAppendUpdateOldPage));
    }
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordHeapAnotherPageAppendUpdateOldPage);

struct WalRecordHeapAnotherPageAppendUpdateNewPage : public WalRecordHeap {
    OffsetNumber offset;
    uint64 undoRecPtr;
    /* if wal level < WAL_LEVEL_LOGICAL: newHeapDiskTuple + AllocTd
     * if wal level >= WAL_LEVEL_LOGICAL:
     * newHeapDiskTuple + oldTupleKey + tableOid + snapshotCsn + commandId (if it changed decode dict) + AllocTd */
    char rawData[];

    inline void SetHeader(const WalPageHeaderContext &heapHeader, OffsetNumber offsetN, UndoRecPtr ptr)
    {
        SetWalPageHeader(heapHeader);
        offset = offsetN;
        undoRecPtr = ptr.m_placeHolder;
    }
    void SetLogicalDecodeInfo(HeapDiskTuple *oldTupleKey, Oid *tableOid, CommitSeqNo *snapshotCsn, CommandId *tupleCid)
    {
        SetContainLoigcalInfoFlag(true);
        SetUpdateContainsReplicaKeyFlag(true);
        HeapDiskTuple *newTuple = static_cast<HeapDiskTuple*>(static_cast<void*>(rawData));
        uint32 fromPos = newTuple->GetTupleSize();
        CopyData(rawData + fromPos, oldTupleKey->GetTupleSize(),
                 static_cast<char *>(static_cast<void *>(oldTupleKey)), oldTupleKey->GetTupleSize());
        fromPos += oldTupleKey->GetTupleSize();
        CopyData(rawData + fromPos, sizeof(Oid), static_cast<char *>(static_cast<void *>(tableOid)), sizeof(Oid));
        fromPos += sizeof(Oid);
        CopyData(rawData + fromPos, sizeof(CommitSeqNo),
                 static_cast<char *>(static_cast<void *>(snapshotCsn)), sizeof(CommitSeqNo));
        if (unlikely(*tupleCid != INVALID_CID)) {
            SetDecodeDictChangeFlag(true);
            fromPos += sizeof(CommitSeqNo);
            CopyData(rawData + fromPos, sizeof(CommandId),
                     static_cast<char *>(static_cast<void *>(tupleCid)), sizeof(CommandId));
        }
    }
    inline void SetAllocTd(TDAllocContext &context)
    {
        HeapDiskTuple* tuple = static_cast<HeapDiskTuple*>(static_cast<void*>(rawData));
        uint32 fromPos = tuple->GetTupleSize();
        if (IsContainLoigcalInfo()) {
            StorageAssert(IsUpdateContainsReplicaKey());
            HeapDiskTuple *oldTuple = static_cast<HeapDiskTuple *>(static_cast<void *>(rawData + fromPos));
            fromPos += oldTuple->GetTupleSize();
            fromPos = fromPos + sizeof(Oid) + sizeof(CommitSeqNo);
            if (unlikely(IsDecodeDictChange())) {
                fromPos += sizeof(CommandId);
            }
        }
        SetAllocTdWal(context, rawData + fromPos,
            m_size - (sizeof(WalRecordHeapAnotherPageAppendUpdateNewPage) + fromPos));
    }
    void GetUpdateTupleLen(uint32 &oldTupLen, uint32 &newTupLen) const
    {
        StorageAssert(IsUpdateContainsReplicaKey());
        const HeapDiskTuple *newTuple = static_cast<const HeapDiskTuple *>(static_cast<const void *>(rawData));
        newTupLen = newTuple->GetTupleSize();
        const HeapDiskTuple *oldTuple = static_cast<const HeapDiskTuple *>(
            static_cast<const void *>(rawData + newTuple->GetTupleSize()));
        oldTupLen = oldTuple->GetTupleSize();
    }
    void GetUpdateTuple(HeapDiskTuple *oldTup, HeapDiskTuple *newTup) const
    {
        StorageAssert(IsUpdateContainsReplicaKey());
        const HeapDiskTuple *newTuple = static_cast<const HeapDiskTuple *>(static_cast<const void *>(rawData));
        errno_t rc = memcpy_s(static_cast<void *>(newTup), newTuple->GetTupleSize(), rawData, newTuple->GetTupleSize());
        storage_securec_check(rc, "\0", "\0");
        const HeapDiskTuple *oldTupleKey =
            static_cast<const HeapDiskTuple *>(static_cast<const void *>(rawData + newTuple->GetTupleSize()));
        rc = memcpy_s(static_cast<void *>(oldTup), oldTupleKey->GetTupleSize(),
                      rawData + newTuple->GetTupleSize(), oldTupleKey->GetTupleSize());
        storage_securec_check(rc, "\0", "\0");
    }
    void GetLogicalDecodeInfo(const Oid *&tableOid, const CommitSeqNo *&snapshotCsn, const CommandId *&tupleCid) const
    {
        StorageAssert(IsContainLoigcalInfo());
        const HeapDiskTuple *newTuple = static_cast<const HeapDiskTuple *>(static_cast<const void *>(rawData));
        uint32 fromPos = newTuple->GetTupleSize();
        const HeapDiskTuple *oldKey = static_cast<const HeapDiskTuple*>(static_cast<const void *>(rawData + fromPos));
        fromPos += oldKey->GetTupleSize();
        tableOid = static_cast<const Oid*>(static_cast<const void *>(rawData + fromPos));
        fromPos += sizeof(Oid);
        snapshotCsn = static_cast<const CommitSeqNo*>(static_cast<const void *>(rawData + fromPos));
        if (unlikely(IsDecodeDictChange())) {
            fromPos += sizeof(CommitSeqNo);
            tupleCid = static_cast<const CommandId*>(static_cast<const void *>(rawData + fromPos));
        } else {
            tupleCid = nullptr;
        }
    }
    void DescLogicalInfo(FILE *fp) const
    {
        const Oid *tableOid;
        const CommitSeqNo *snapshotCsn;
        const CommandId *tupleCid;
        GetLogicalDecodeInfo(tableOid, snapshotCsn, tupleCid);
        (void)fprintf(fp, "tableOid: %u snapshotCsn: %lu tupleCid: %u", *tableOid, *snapshotCsn, *tupleCid);
    }
    void SetData(char *data, uint32 size);
    void Redo(HeapPage *page, Xid xid) const;
    OffsetNumber GetOffset() const
    {
        return offset;
    }
    void Dump(FILE *fp) const
    {
        WalRecordForPage::Dump(fp);
        const HeapDiskTuple* tuple = static_cast<const HeapDiskTuple*>(static_cast<const void*>(rawData));
        UndoRecPtr undoPtr(undoRecPtr);
        (void)fprintf(fp, "offset(%hu), undoPtr(%hu, %u, %hu), HeapDiskTuple{tdId(%hhu), lockerTdId(%hhu), size(%hu), "
            "info(%x)}.", offset, undoPtr.GetFileId(), undoPtr.GetBlockNum(), undoPtr.GetOffset(), tuple->GetTdId(),
            tuple->GetLockerTdId(), tuple->GetTupleSize(), tuple->m_info.m_info);

        if (m_size == sizeof(WalRecordHeapAnotherPageAppendUpdateNewPage) + tuple->GetTupleSize()) {
            return;
        }
        uint32 fromPos = tuple->GetTupleSize();
        if (IsContainLoigcalInfo()) {
            StorageAssert(IsUpdateContainsReplicaKey());
            const HeapDiskTuple *oldTupleKey =
                static_cast<const HeapDiskTuple *>(static_cast<const void *>(rawData + fromPos));
            fromPos += oldTupleKey->GetTupleSize();
            fromPos = fromPos + sizeof(Oid) + sizeof(CommitSeqNo);
            if (unlikely(IsDecodeDictChange())) {
                fromPos += sizeof(CommandId);
            }
        }
        DescAllocTd(fp, rawData + fromPos, m_size - (sizeof(WalRecordHeapSamePageAppendUpdate) + fromPos));
        if (IsContainLoigcalInfo()) {
            DescLogicalInfo(fp);
        }
    }
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordHeapAnotherPageAppendUpdateNewPage);

struct WalRecordHeapForceUpdateTupleDataNoTrx : public WalRecordHeap {
    OffsetNumber offset;
    uint32 dataLen;
    /* Not involved logic decoding. */
    char rawData[];

    inline void SetHeader(const WalPageHeaderContext &heapHeader, OffsetNumber offsetN, uint32 newLen)
    {
        SetWalPageHeader(heapHeader);
        offset = offsetN;
        dataLen = newLen;
    }
    void SetData(char *data, uint32 size);
    void Redo(HeapPage *page, Xid xid) const;
    OffsetNumber GetOffset() const
    {
        return offset;
    }
    uint32 GetDataLen() const
    {
        return dataLen;
    }
    void Dump(FILE *fp) const
    {
        WalRecordForPage::Dump(fp);
        (void)fprintf(fp, "offset(%hu), dataLen(%u).", offset, dataLen);
    }
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordHeapForceUpdateTupleDataNoTrx);

struct WalRecordHeapPrune : public WalRecordHeap {
    CommitSeqNo recentDeadMinCsn;
    uint16 diffNum;
    char rawData[];

    inline void SetHeader(const WalPageHeaderContext &heapPruneHeader, uint16 diffN, CommitSeqNo minCsn)
    {
        SetWalPageHeader(heapPruneHeader);
        diffNum = diffN;
        recentDeadMinCsn = minCsn;
    }

    void SetData(char *data, uint32 size);
    void Redo(HeapPage *page, Xid xid) const;
    void Dump(FILE *fp) const
    {
        WalRecordForPage::Dump(fp);
        (void)fprintf(fp, "csnmin(%lu), diffNum(%hu).", recentDeadMinCsn, diffNum);
    }
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordHeapPrune);

struct WalRecordHeapAllocTd : public WalRecordHeap {
    char rawData[]; /* AllocTd */

    inline void SetHeader(const WalPageHeaderContext &heapHeader)
    {
        SetWalPageHeader(heapHeader);
    }
    inline void SetAllocTd(TDAllocContext &context)
    {
        SetAllocTdWal(context, rawData, m_size - sizeof(WalRecordHeapAllocTd));
    }

    void Redo(HeapPage *page, Xid xid) const;
    void Dump(FILE *fp) const
    {
        WalRecordForPage::Dump(fp);
        if (m_size == sizeof(WalRecordHeapAllocTd)) {
            return;
        }
        DescAllocTd(fp, rawData, m_size - sizeof(WalRecordHeapAllocTd));
    }
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordHeapAllocTd);

struct WalRecordHeapUpdateNextCtid : public WalRecordHeap {
    OffsetNumber offset;
    uint64 nextCtid;

    inline void SetHeader(const WalPageHeaderContext &heapHeader, OffsetNumber offsetNum, ItemPointerData ctid)
    {
        SetWalPageHeader(heapHeader);
        offset = offsetNum;
        nextCtid = ctid.m_placeHolder;
    }

    void Redo(HeapPage *page, Xid xid) const;
    void Dump(FILE *fp) const
    {
        WalRecordForPage::Dump(fp);
        ItemPointerData ctid(nextCtid);
        (void)fprintf(fp, "offset(%hu), nextctid(%hu, %u, %hu),.", offset, ctid.GetFileId(), ctid.GetBlockNum(),
            ctid.GetOffset());
    }
} PACKED;
STATIC_ASSERT_TRIVIAL(WalRecordHeapUpdateNextCtid);
}

#endif /* STORAGE_HEAP_WAL_STRUCT_H */
