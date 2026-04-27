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
 * dstore_undo_zone_txn_mgr.h
 *
 * IDENTIFICATION
 *        include/undo/dstore_undo_zone_txn_mgr.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_UNDO_ZONE_TXN_MGR_H
#define DSTORE_UNDO_ZONE_TXN_MGR_H

#include "buffer/dstore_buf_mgr.h"
#include "common/memory/dstore_mctx.h"
#include "lock/dstore_lock_datatype.h"
#include "page/dstore_page_struct.h"
#include "page/dstore_undo_page.h"
#include "tablespace/dstore_segment.h"
#include "tablespace/dstore_tablespace_utils_internal.h"
#include "transaction/dstore_transaction_types.h"


namespace DSTORE {

#if defined(UT) && !defined(EMBEDDED)
constexpr ExtentSize UNDO_ZONE_EXTENT_SIZE = EXT_SIZE_8;
#else
constexpr ExtentSize UNDO_ZONE_EXTENT_SIZE = EXT_SIZE_128;
#endif
constexpr uint32 TRX_PAGES_PER_ZONE = static_cast<uint32>(UNDO_ZONE_EXTENT_SIZE) - 1;

class UndoZoneTrxManager : public BaseObject {
public:
    explicit UndoZoneTrxManager(ZoneId zid, Segment *segment, BufMgrInterface *bufferMgr, PdbId pdbId)
        : m_pdbId(pdbId),
          m_zoneId(zid),
          m_segment(segment),
          m_bufferMgr(bufferMgr),
          m_startPage(INVALID_PAGE_ID),
          m_recycleLogicSlotId(0),
          m_nextFreeLogicSlotId(0),
          m_currentTxnSlotBuf(INVALID_BUFFER_DESC)
    {}

    ~UndoZoneTrxManager() noexcept;

    RetStatus Init(const PageId &startPageId, bool initPage);

    UndoRecPtr Restore(TailUndoPtrStatus &tailPtrStatus);

    inline PageId GetStartPageId()
    {
        return m_startPage;
    }

    inline void SetStartPageId(const PageId &startPageId)
    {
        m_startPage = startPageId;
    }

    inline bool HasFreeSlot() const
    {
        return m_nextFreeLogicSlotId.load(std::memory_order_acquire) -
            m_recycleLogicSlotId.load(std::memory_order_acquire) < TRX_PAGES_PER_ZONE * TRX_PAGE_SLOTS_NUM;
    }

    /* move slot page forward in ring manner. */
    inline void AdvanceTrxSlotPageId(PageId &trxSlotPageId) const
    {
        trxSlotPageId.m_blockId++;
        if (trxSlotPageId.m_blockId - m_startPage.m_blockId == TRX_PAGES_PER_ZONE) {
            trxSlotPageId.m_blockId = m_startPage.m_blockId;
        }
    }

    inline PageId GetTxnSlotPageId(uint64 logicSlotId) const
    {
        PageId res;
        res.m_fileId = m_startPage.m_fileId;
        res.m_blockId = m_startPage.m_blockId + (logicSlotId / TRX_PAGE_SLOTS_NUM) % TRX_PAGES_PER_ZONE;
        return res;
    }

    inline uint64 GetNextFreeLogicSlotId() const
    {
        return m_nextFreeLogicSlotId.load(std::memory_order_acquire);
    }

    inline uint64 GetRecycleLogicSlotId() const
    {
        return m_recycleLogicSlotId.load(std::memory_order_acquire);
    }

    inline TransactionSlotPage *GetTxnSlotPage(BufferDesc *bufDesc)
    {
        return static_cast<TransactionSlotPage*>(bufDesc->GetPage());
    }

    /* Allocate an empty transaction slot */
    Xid AllocSlot();

    /* If return false, will bring earliest non-recyclable transaction slot spaceTailUndoPtr out
     * by parameter tailUndoPtr. If return true, mean this transaction slot can recycle.
     */
    bool IsSlotRecyclable(uint64 logicSlotId, CommitSeqNo recycleMinCsn, UndoRecPtr &tailUndoPtr,
        TransactionSlotPage *txnPage, bool &isCommitedSlot);

    static bool IsXidRecycled(PdbId pdbId, Xid xid, CommitSeqNo* commitCsn = nullptr);

    bool IsUndoZoneNeedRollback(Xid &rollbackXid);

    void CopySlot(Xid xid, TransactionSlot &trxSlot);

    template<TrxSlotStatus status>
    void Commit(Xid xid, CommitSeqNo& csn, bool isSpecialAyncCommitAutoTrx = false);

    RetStatus RollbackTxnSlot(Xid xid);

    RetStatus SetSlotUndoPtr(Xid xid, UndoRecPtr undoRecPtr, bool insertUndoFlag);

    void GetSlotCurTailUndoPtr(Xid xid, UndoRecPtr &undoRecPtr);

    BufferDesc *ReadTxnSlotPageBuf(const PageId &id, LWLockMode mode);

    RetStatus PinAndLockTxnSlotBuf(PageId pageId);

    void UnlockAndReleaseTxnSlotPageBuf(BufferDesc *&bufDesc);

    UndoRecPtr GetLastSpaceUndoRecPtr(int64 firstAppendSlotId = INVALID_TXN_SLOT_ID);

    void GetFirstSpaceUndoRecPtr(UndoRecPtr &firstTailPtr, int64 &firstAppendUndoSlot);

    void GenerateWalForRecycle(BufferDesc *bufferDesc, uint64 lastRecycleSlotId, uint64 nextRecycleSlotId,
                               CommitSeqNo recycleMinCsn);

    /* Parameter restoreUndoPtr used for restore process.
     * If restoreUndoPtr is INVALID_UNDO_RECORD_PTR, mean has recycle all transaction slots,
     * you can restore m_undoRecyclePageId as start page id.
     * If not, restoreUndoPtr is earliest non-recyclable transaction slot spaceTailUndoPtr.
     *
     * Parameter recycleUndoPtr used for recycle process.
     * If recycleUndoPtr is INVALID_UNDO_RECORD_PTR, mean no need to recycle undo page.
     * If not, recycleUndoPtr is last recyclable transaction slot spaceTailUndoPtr.
     */
    void RecycleTxnSlots(CommitSeqNo recycleMinCsn, UndoRecPtr &restoreUndoPtr,
                                         UndoRecPtr &recycleUndoPtr, bool isInRestore,
                                         TailUndoPtrStatus &tailPtrStatus);
    Xid GetLastActiveXid();

private:
    PdbId m_pdbId;
    ZoneId m_zoneId;
    Segment *m_segment;
    BufMgrInterface *m_bufferMgr;

    PageId m_startPage;
    std::atomic<uint64> m_recycleLogicSlotId;        /* The first transaction slot id to be recycled */
    std::atomic<uint64> m_nextFreeLogicSlotId;       /* The next transaction slot id to be used */
    BufferDesc* m_currentTxnSlotBuf;
    RetStatus InitSlotPage(const PageId &slotBlk);
};

}  // namespace DSTORE
#endif  // STORAGE_UNDO_ZONE_TXN_MGR_H
