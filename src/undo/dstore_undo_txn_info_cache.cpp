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
 * dstore_undo_txn_info_cache.cpp
 *
 * IDENTIFICATION
 *        src/undo/dstore_undo_txn_info_cache.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "framework/dstore_instance.h"
#include "securec.h"
#include "undo/dstore_undo_txn_info_cache.h"

namespace DSTORE {


void AllUndoZoneTxnInfoCache::InitTxnInfoCache()
{
    for (uint32 i = 0; i < UNDO_ZONE_COUNT; i++) {
        m_cachedEntry[i] = nullptr;
    }
    errno_t rc = memset_s(m_recycleLogicSlotId, sizeof(m_recycleLogicSlotId), 0, sizeof(m_recycleLogicSlotId));
    storage_securec_check(rc, "", "");
}

void AllUndoZoneTxnInfoCache::DestroyTxnInfoCache()
{
    for (uint32 i = 0; i < UNDO_ZONE_COUNT; i++) {
        DstorePfreeAlignedImpl(m_cachedEntry[i]);
    }
}


void AllUndoZoneTxnInfoCache::RefreshRecycleLogicSlotId(Xid xid, CommitSeqNo csn, CommitSeqNo recycleCsnMin)
{
    ZoneId zid = static_cast<ZoneId>(xid.m_zoneId);
    uint64 logicSlotId = xid.m_logicSlotId;
    /* if csn smaller than recycleCsnMin, means this slot can recycle, and status is frozen */
    if (csn < recycleCsnMin) {
        uint64 recycleSlotId = GsAtomicReadU64(&m_recycleLogicSlotId[zid]);
        /* must use this bigest logicSlotId + 1 as m_recycleLogicSlotId for quickly judge */
        while (logicSlotId + 1 > recycleSlotId) {
            (void)GsAtomicCompareExchangeU64(&m_recycleLogicSlotId[zid], &recycleSlotId, logicSlotId + 1);
        }
    }
}

RetStatus AllUndoZoneTxnInfoCache::ReadTxnInfoFromCache(Xid xid, TransactionSlot &outTxnSlot, CommitSeqNo recycleCsnMin)
{
    /* step 1: get txn info quickly by m_recycleLogicSlotId if can */
    ZoneId zid = static_cast<ZoneId>(xid.m_zoneId);
    uint64 logicSlotId = xid.m_logicSlotId;
    outTxnSlot.walId = INVALID_WAL_ID;
    outTxnSlot.commitEndPlsn = INVALID_PLSN;
    if (logicSlotId < GsAtomicReadU64(&(m_recycleLogicSlotId[zid]))) {
        outTxnSlot.status = TXN_STATUS_FROZEN;
        outTxnSlot.csn = INVALID_CSN;
        ErrLog(DSTORE_DEBUG1, MODULE_UNDO, ErrMsg("Xid(%d, %lu) read from cache success, status frozen.",
            static_cast<int>(xid.m_zoneId), xid.m_logicSlotId));
        return DSTORE_SUCC;
    }

    /* step 2: get txn info from cache */
    if (likely(GsAtomicReadU64(static_cast<uint64*>(static_cast<void*>(&m_cachedEntry[zid]))) != 0)) {
        CachedTransactionSlot slotInfo;
        uint32 slotId = logicSlotId % CACHED_SLOT_NUM_PER_ZONE;
        slotInfo.placeHolder = atomic_compare_and_swap_u128(
            static_cast<volatile uint128_u *>(&m_cachedEntry[zid][slotId].placeHolder));
        /*
         * if csn invalid, means this slot has not been cached, if logicSlotId is not equal to txnInfo.logicSlotId,
         * means this cache not belong to this transaction(xid).
         */
        if (slotInfo.txnInfo.csn == INVALID_CSN || logicSlotId != slotInfo.txnInfo.logicSlotId) {
            return DSTORE_FAIL;
        }

        outTxnSlot.status = slotInfo.txnInfo.status;
        outTxnSlot.logicSlotId = slotInfo.txnInfo.logicSlotId;
        outTxnSlot.csn = slotInfo.txnInfo.csn;

        /* refresh recycle logic slot id */
        RefreshRecycleLogicSlotId(xid, outTxnSlot.csn, recycleCsnMin);
#ifdef DSTORE_USE_ASSERT_CHECKING
        ErrLog(DSTORE_DEBUG1, MODULE_UNDO,
               ErrMsg("Xid(%d, %lu) read from cache success, status %d, csn %lu.", static_cast<int>(xid.m_zoneId),
                      xid.m_logicSlotId, outTxnSlot.status, outTxnSlot.csn));
#endif
        return DSTORE_SUCC;
    }
    return DSTORE_FAIL;
}

void AllUndoZoneTxnInfoCache::WriteTxnInfoToCache(Xid xid, TransactionSlot &inTxnSlot, CommitSeqNo recycleCsnMin,
                                                  bool cacheInprogress)
{
    ZoneId zid = static_cast<ZoneId>(xid.m_zoneId);
    uint64 logicSlotId = xid.m_logicSlotId;
    /* step 1: if cache is null, create cache */
    if (unlikely(GsAtomicReadUintptr(static_cast<uintptr_t *>(static_cast<void *>(&m_cachedEntry[zid]))) == 0)) {
        /* ensure thread safe, use instance level mem context */
        AutoMemCxtSwitch autoSwitch(g_storageInstance->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));
        uint32 cacheSize = sizeof(CachedTransactionSlot) * CACHED_SLOT_NUM_PER_ZONE;
        CachedTransactionSlot *cachedSlots =
            static_cast<CachedTransactionSlot *>(DstorePallocAligned(cacheSize, DSTORE_CACHELINE_SIZE));
        if (cachedSlots == nullptr) {
            ErrLog(DSTORE_ERROR, MODULE_UNDO, ErrMsg("alloc undoZoneTxnInfo fail, size(%u).", cacheSize));
            return;
        }
        errno_t rc = memset_s(cachedSlots, cacheSize, 0, cacheSize);
        storage_securec_check(rc, "", "");

        /* try compare and exchange */
        CachedTransactionSlot *expect = nullptr;
        bool casSucc = GsAtomicCompareExchangeUintptr(
            static_cast<uintptr_t *>(static_cast<void *>(&(m_cachedEntry[zid]))),
            static_cast<uintptr_t *>(static_cast<void *>(&expect)), reinterpret_cast<uintptr_t>(cachedSlots));
        if (!casSucc) {
            DstorePfreeAlignedImpl(cachedSlots);
        }
        StorageAssert(GsAtomicReadUintptr(static_cast<uintptr_t *>(static_cast<void *>(&m_cachedEntry[zid]))) != 0);
    }

    /* step 2: only refresh recycle logic slot id if txn status frozen, commited, aborted */
    if (inTxnSlot.status == TXN_STATUS_FROZEN || inTxnSlot.status == TXN_STATUS_COMMITTED ||
        inTxnSlot.status == TXN_STATUS_ABORTED) {
        RefreshRecycleLogicSlotId(xid, inTxnSlot.csn, recycleCsnMin);
#ifdef DSTORE_USE_ASSERT_CHECKING
        ErrLog(
            DSTORE_DEBUG1, MODULE_UNDO,
            ErrMsg("Xid(%d, %lu) refresh logic slot id, status %d, csn %lu, "
                   "recycle csn min %lu.",
                   static_cast<int>(xid.m_zoneId), xid.m_logicSlotId, inTxnSlot.status, inTxnSlot.csn, recycleCsnMin));
#endif
    }

    /*
     * Step 3: if csn < recycleCsnMin, means we has refresh recycleLogicSlotId.
     * next time we can get txnInfo by recycleLogicSlotId quickly, no need write cache.
     * we can only write cache if txn status is frozen, commited, aborted.
     */
    if (cacheInprogress || (inTxnSlot.csn >= recycleCsnMin &&
                            (inTxnSlot.status == TXN_STATUS_FROZEN || inTxnSlot.status == TXN_STATUS_COMMITTED ||
                             inTxnSlot.status == TXN_STATUS_ABORTED))) {
        /* record to slot */
        CachedTransactionSlot slotInfo;
        slotInfo.txnInfo.csn = inTxnSlot.csn;
        slotInfo.txnInfo.status = inTxnSlot.status;
        slotInfo.txnInfo.logicSlotId = inTxnSlot.logicSlotId;
        slotInfo.txnInfo.reserve = 0;
        /* write to cache */
        uint32 slotId = logicSlotId % CACHED_SLOT_NUM_PER_ZONE;
        uint128_u compare;
        uint128_u preVal;
        compare =
            atomic_compare_and_swap_u128(static_cast<volatile uint128_u *>(&m_cachedEntry[zid][slotId].placeHolder));
        bool casSucc = false;
        while (!casSucc) {
            preVal =
                atomic_compare_and_swap_u128(static_cast<volatile uint128_u *>(&m_cachedEntry[zid][slotId].placeHolder),
                                             compare, slotInfo.placeHolder);
            if (compare.u128 == preVal.u128) {
                casSucc = true;
            } else {
                compare.u128 = preVal.u128;
            }
        }
#ifdef DSTORE_USE_ASSERT_CHECKING
        ErrLog(
            DSTORE_DEBUG1, MODULE_UNDO,
            ErrMsg("Xid(%d, %lu) write cache, status %d, csn %lu, recycle csn min %lu.", static_cast<int>(xid.m_zoneId),
                   xid.m_logicSlotId, inTxnSlot.status, inTxnSlot.csn, recycleCsnMin));
#endif
    }
}

uint64 AllUndoZoneTxnInfoCache::GetRecycleLogicSlotId(ZoneId zid)
{
    return GsAtomicReadU64(&(m_recycleLogicSlotId[zid]));
}

}  // namespace DSTORE
