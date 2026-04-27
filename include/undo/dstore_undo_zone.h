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
 * dstore_transaction_mgr.cpp
 *
 *
 * IDENTIFICATION
 *        include/undo/dstore_undo_zone.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_UNDO_ZONE_H
#define DSTORE_UNDO_ZONE_H

#include <atomic>

#include "buffer/dstore_buf_mgr.h"
#include "wal/dstore_wal_struct.h"
#include "page/dstore_undo_page.h"
#include "dstore_undo_types.h"
#include "undo/dstore_undo_zone_txn_mgr.h"
#include "undo/dstore_undo_wal.h"

namespace DSTORE {

struct StrParam {
    char *ptr;
    Size size;
};

struct UndoZoneStatus {
    bool isAsyncRollbacking;
    uint32 pageNum;
    uint64 recycleLogicSlotId;
    uint64 nextFreeLogicSlotId;
};

struct ReadUndoContext {
    PdbId pdbId;
    BufMgrInterface *bufferMgr;
    BufferDesc *bufferDesc;
    int* startingByte;
    StrParam dest;
};

#ifdef UT
#define private public
#endif

class UndoZone : public BaseObject {
public:
    UndoZone(Segment *segment, BufMgrInterface *bufMgr, ZoneId zoneId, PdbId pdbId);
    ~UndoZone();

    DISALLOW_COPY_AND_MOVE(UndoZone);

    RetStatus Init(DstoreMemoryContext context);

    /* Recycle transactions whose CSNs are older than the oldest snapshot and their undo pages */
    void Recycle(CommitSeqNo recycleMinCsn);
    bool IsRecyclingComplete() const;

    RetStatus RollbackUndoZone(Xid xid, bool isRecovery);
    RetStatus RollbackUndoRecords(Xid xid, UndoRecPtr startUndoPtr, UndoRecPtr endUndoPtr, bool isRecovery);

    RetStatus ExtendSpaceIfNeeded(uint32 needSize);
    UndoRecPtr InsertUndoRecord(UndoRecord* record);
    static RetStatus FetchUndoRecord(PdbId pdbId, UndoRecord *record, UndoRecPtr undoRecPtr, Xid xid,
                                     BufMgrInterface *bufferMgr, CommitSeqNo *commitCsn = nullptr);

    static bool IsXidRecycled(PdbId pdbId, Xid xid)
    {
        return UndoZoneTrxManager::IsXidRecycled(pdbId, xid);
    }

    /* Warning: the caller must ensure that the record has not been recycled. */
    static void FetchUndoRecordInternal(PdbId pdbId, UndoRecord *record, UndoRecPtr undoRecPtr,
                                        BufMgrInterface *bufferMgr, bool needStrictCheckUndo = true,
                                        bool *isVaild = nullptr);

    RetStatus RestoreUndoZoneFromTxnSlots();

    inline Xid AllocSlot()
    {
        return m_txnSlotManager->AllocSlot();
    }
    inline void CopySlot(Xid xid, TransactionSlot &txnSlot)
    {
        m_txnSlotManager->CopySlot(xid, txnSlot);
    }
    template<TrxSlotStatus status>
    inline void Commit(Xid xid, CommitSeqNo& csn, bool isSpecialAyncCommitAutoTrx = false)
    {
        m_txnSlotManager->Commit<status>(xid, csn, isSpecialAyncCommitAutoTrx);
    }
    inline RetStatus SetSlotUndoPtr(Xid xid, UndoRecPtr undoRecPtr, bool insertUndoFlag)
    {
        return m_txnSlotManager->SetSlotUndoPtr(xid, undoRecPtr, insertUndoFlag);
    }
    inline void GetSlotCurTailUndoPtr(Xid xid, UndoRecPtr &undoRecPtr)
    {
        m_txnSlotManager->GetSlotCurTailUndoPtr(xid, undoRecPtr);
    }

    inline void SetAsyncRollbackState(bool isAsyncRollbacking)
    {
        m_isAsyncRollbacking.store(isAsyncRollbacking, std::memory_order_release);
    }

    inline bool IsAsyncRollbacking() const
    {
        return m_isAsyncRollbacking.load(std::memory_order_acquire);
    }

    inline uint64 GetRecycleLogicSlotId() const
    {
        return m_txnSlotManager->GetRecycleLogicSlotId();
    }

    inline uint64 GetNextFreeLogicSlotId() const
    {
        return m_txnSlotManager->GetNextFreeLogicSlotId();
    }

    inline PageId GetUndoRecyclePageId() const
    {
        return m_undoRecyclePageId;
    }
    inline Xid GetLastActiveXid()
    {
        return m_txnSlotManager->GetLastActiveXid();
    }
    inline PageId GetSegmentId()
    {
        return m_segment->GetSegmentMetaPageId();
    }
    inline PdbId GetPdbId() const
    {
        return m_pdbId;
    }
    inline ZoneId GetZoneId() const
    {
        return m_zoneId;
    }

    static void GenerateWalForRollback(BufferDesc *bufferDesc, const UndoRecord &undoRec, WalType walType);
#ifndef UT
private:
#endif
    PdbId m_pdbId;
    ZoneId m_zoneId;
    UndoZoneTrxManager *m_txnSlotManager;

    Segment *m_segment;
    BufMgrInterface *m_bufferMgr;

    UndoRecPtr m_nextAppendUndoPtr; /* location where next undo record is appended. */
    PageId m_needCheckPageId;   /* The next page of m_nextAppendUndoPtr that is necessary to check whether undo needs to
                                   be extended before insert undo record. We expect that there is one fully free
                                   undo page before each insertion, If the above condition is not met, expansion is
                                   required. */
    PageId m_undoRecyclePageId; /* The first undo page to be recycled */

    std::atomic<bool> m_isAsyncRollbacking;
    BufferDesc* m_currentInsertUndoPageBuf;

    BufferDesc *ReadSegMetaPageBuf(LWLockMode mode);
    void UnlockAndReleaseSegMetaPageBuf(BufferDesc *&segMetaPageBuf);
    UndoSegmentMetaPage* GetUndoSegmentMetaPage(BufferDesc *segMetaPageBuf);
    void InitCurUndoPage(const PageId &cur);
    UndoRecPtr GetNextUndoRecPtr(UndoRecPtr curPtr, uint32 recordSize);

    bool WriteUndoRecord(UndoRecord &record, char *page, int startingByte, int &alreadyWritten) const;
    static void ReadUndoRecord(PdbId pdbId, UndoRecord &record, BufferDesc *&bufferDesc, int startingByte,
                               BufMgrInterface *bufferMgr);
    static void ReadUndoBytesStream(ReadUndoContext &context);
    static bool InsertUndoBytes(const StrParam &src, char **writePtr, const char *endPtr, int &myBytesWritten,
                                int &alreadyWritten);
    static bool ReadUndoBytes(const StrParam &dest, char **readPtr, const char *endPtr, int &alreadyRead);
    static bool IsPageTypeMatchUndoRecordType(DataPage* page, const UndoRecord &undoRec);

    void RecycleUndoPage(UndoRecPtr end, CommitSeqNo recycleMinCsn);

    RetStatus AllocNewUndoPages(PageId &firstFreePageId, PageId &lastFreePageId);
    RetStatus PutNewPagesIntoRing(const PageId &prev, const PageId &firstFreePageId, const PageId &next);
    RetStatus ExtendUndoPageRing(const PageId &firstFreePageId, const PageId &lastFreePageId);
    RetStatus InitUndoRecordSpace();
    RetStatus InitTransactionSlotSpace(PageId startPageId);

    void RestoreUndoWritePtr(int64 firstAppendUndoSlotId, TailUndoPtrStatus tailPtrStatus);

    RetStatus GenerateWalForUndoRec(BufferDesc *bufferDesc, OffsetNumber offset, uint32 dataSize);
    void GenerateWalForInitUndoRecSpace(const PageId &recyclePageId, BufferDesc *segMetaPageBuf);
    template<WalType type> void GenerateWalForUndoRingOldPage(BufferDesc *bufferDesc, const PageId &pageId);
    void GenerateWalForUndoRingNewPage(BufferDesc *bufferDesc, const UndoRecordPageHeader &hdr);

    /* Rollback committed transactions on index page. Only during backup restore process. */
    void RollbackTdForBackupRestore(BtreeUndoContext &btrUndoCxt);
};

#ifdef UT
#undef private
#endif

}  // namespace DSTORE
#endif  // STORAGE_UNDO_ZONE_H
