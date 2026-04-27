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
 * dstore_undo_txn_info_cache.h
 *     This file defines the building blocks that bg page writer needs.
 *
 * IDENTIFICATION
 *        include/undo/dstore_undo_txn_info_cache.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_UNDO_TXN_INFO_CACHE_H
#define DSTORE_UNDO_TXN_INFO_CACHE_H

#include "common/concurrent/dstore_atomic.h"
#include "transaction/dstore_csn_mgr.h"
#include "undo/dstore_undo_zone.h"

namespace DSTORE {

#ifdef UT
#define private public
#endif

const uint32 CACHED_SLOT_NUM_PER_ZONE = 8000;

struct CachedTransactionSlot {
    union {
        uint128_u placeHolder;
        struct {
            CommitSeqNo csn;
            TrxSlotStatus status : 8;
            uint64 reserve : 8;
            uint64 logicSlotId : 48;
        } txnInfo;
    };
};

class AllUndoZoneTxnInfoCache : public BaseObject {
public:
    AllUndoZoneTxnInfoCache() = default;
    ~AllUndoZoneTxnInfoCache() = default;
    void InitTxnInfoCache();
    void DestroyTxnInfoCache();

    RetStatus ReadTxnInfoFromCache(Xid xid, TransactionSlot &outTxnSlot, CommitSeqNo recycleCsnMin);
    void WriteTxnInfoToCache(Xid xid, TransactionSlot &inTxnSlot, CommitSeqNo recycleCsnMin, bool cacheInprogress);
    uint64 GetRecycleLogicSlotId(ZoneId zid);
private:
    void RefreshRecycleLogicSlotId(Xid xid, CommitSeqNo csn, CommitSeqNo recycleCsnMin);

    CachedTransactionSlot* m_cachedEntry[UNDO_ZONE_COUNT];
    uint64 m_recycleLogicSlotId[UNDO_ZONE_COUNT];
};

#ifdef UT
#undef private
#endif

}  // namespace DSTORE

#endif  // STORAGE_UNDO_TXN_INFO_CACHE_H
