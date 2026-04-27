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
 * dstore_backup_restore.cpp
 *
 *
 * IDENTIFICATION
 *        include/undo/dstore_undo_mgr.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_UNDO_MGR_H
#define DSTORE_UNDO_MGR_H

#include <atomic>
#include "common/algorithm/dstore_bitmapset.h"
#include "common/memory/dstore_mctx.h"
#include "undo/dstore_undo_zone.h"
#include "undo/dstore_undo_txn_info_cache.h"

namespace DSTORE {

#ifdef UT
#define private public
#endif

constexpr uint32 MAX_THREAD_NUM = 1024;
constexpr uint32 SEGMENT_ID_PER_PAGE = (BLCKSZ - sizeof(Page)) / sizeof(PageId);
constexpr uint32 SEGMENT_ID_PAGES = (UNDO_ZONE_COUNT + SEGMENT_ID_PER_PAGE - 1) / SEGMENT_ID_PER_PAGE;
constexpr ExtentSize SEGMENT_ID_EXTENT_SIZE = EXT_SIZE_8192;
static_assert(SEGMENT_ID_PAGES < static_cast<uint32>(SEGMENT_ID_EXTENT_SIZE), "");
using GetCurrentActiveXidsCallback = void (*)(Xid xid, void *arg);

class UndoMgr : public BaseObject {
public:
    explicit UndoMgr(BufMgrInterface *bufferMgr = nullptr, PdbId pdbId = INVALID_PDB_ID);
    virtual ~UndoMgr() noexcept;

    void DestroyAllUndoZone();
    virtual RetStatus Init(DstoreMemoryContext parentContext);
    void CreateUndoMapSegment();
    RetStatus LoadUndoMapSegment();

    virtual RetStatus AllocateZoneId(ZoneId &retZid);
    virtual RetStatus ReleaseZoneId(ZoneId &zid);

    RetStatus SwitchZone(UndoZone *&retZone, ZoneId &zid);
    RetStatus GetUndoZone(ZoneId zid, UndoZone **outUzone, bool canCreate = false);
    RetStatus GetUndoStatusForDiagnose(ZoneId zid, UndoZoneStatus *outStatus);

    virtual bool IsZoneOwned(ZoneId zid);
    virtual bool IsZoneOccupied(ZoneId zid);
    virtual void AddZoneOwned(UNUSE_PARAM ZoneId zid)
    {}

    virtual void Recycle(CommitSeqNo recycleMinCsn);
    virtual void RecoverUndoZone();
    void GetLocalCurrentAllActiveTrxXid(GetCurrentActiveXidsCallback callBack, void *arg);
    void StopRecoverUndoZone();
    void ResetStopRecoverUndoZone();

    PageId GetUndoZoneSegmentId(ZoneId zoneId, bool needPanic = false);

    inline RetStatus ReadTxnInfoFromCache(Xid xid, TransactionSlot &outTrxSlot, CommitSeqNo recycleCsnMin)
    {
        return m_txnInfoCache->ReadTxnInfoFromCache(xid, outTrxSlot, recycleCsnMin);
    }

    inline void WriteTxnInfoToCache(Xid xid, TransactionSlot &outTrxSlot, CommitSeqNo recycleCsnMin,
                                    bool cacheInprogress = false)
    {
        m_txnInfoCache->WriteTxnInfoToCache(xid, outTrxSlot, recycleCsnMin, cacheInprogress);
    }

    inline uint64 GetRecycleLogicSlotIdFromCache(ZoneId zid)
    {
        return m_txnInfoCache->GetRecycleLogicSlotId(zid);
    }

    void DestroyUndoMapSegment()
    {
        delete m_mapSegment;
        m_mapSegment = nullptr;
    }
    void DestroyUndoTxnInfoCache()
    {
        m_txnInfoCache->DestroyTxnInfoCache();
        DstorePfree(m_txnInfoCache);
        m_txnInfoCache = nullptr;
    }

    BufMgrInterface *GetBufferMgr()
    {
        return m_bufferMgr;
    }

    void ResetUndoTxnInfoCache()
    {
        m_txnInfoCache->DestroyTxnInfoCache();
        m_txnInfoCache->InitTxnInfoCache();
    }

protected:
    RetStatus CreateUndoZone(ZoneId zid);
    RetStatus LoadUndoZone(const ZoneId &zid, const PageId &segmentId);

    inline bool IsZoneIdFree(ZoneId zid) const
    {
        m_bmsLock->Acquire();
        bool ret = BmsIsMember(zid, m_freeZids);
        m_bmsLock->Release();
        return ret;
    }

    inline PageId GetZidToSegmentMapStartPage()
    {
        return m_mapStartPage;
    }
    PdbId m_pdbId;
    AllUndoZoneTxnInfoCache *m_txnInfoCache;
    UndoZone **m_undoZones;
    Bitmapset *m_freeZids;
    DstoreMemoryContext m_undoMemoryContext;
    RwLock m_zoneLocks[MAX_THREAD_NUM];

    DstoreSpinLock *m_bmsLock; /* warning: analyze all bitmapset lock */

    BufMgrInterface *m_bufferMgr;
    std::atomic<bool> m_needStopRecover;

private:
    RetStatus AllocateZoneMemory(ZoneId zid, Segment *segment);
    RetStatus SetUndoZoneSegmentId(ZoneId zoneId, const PageId &segmentId);

    inline ItemPointerData GetZoneIdLocation(ZoneId zoneId)
    {
#ifndef UT
        StorageReleasePanic(!IS_VALID_ZONE_ID(zoneId), MODULE_UNDO,
            ErrMsg("GetZoneIdLocation zoneId invalid zondId (%d).", zoneId));
#else
        StorageAssert(IS_VALID_ZONE_ID(zoneId));
#endif
        ItemPointerData res;
        res.SetFileId(m_mapStartPage.m_fileId);
        res.SetBlockNumber(m_mapStartPage.m_blockId + static_cast<uint32>(zoneId) / SEGMENT_ID_PER_PAGE);
        res.SetOffset((static_cast<uint32>(zoneId) % SEGMENT_ID_PER_PAGE) * sizeof(PageId) + sizeof(Page));
        StorageAssert(res.GetOffset() + sizeof(PageId) <= BLCKSZ);
        return res;
    }

    /* zoneId to segmentId map related var and func. */
    LWLock m_mapLock;
    Segment *m_mapSegment;
    PageId m_mapStartPage;
    std::atomic_bool m_fullyInited;
};

#ifdef UT
#undef private
#endif

}  // namespace DSTORE

#endif  // STORAGE_UNDO_MGR_H
