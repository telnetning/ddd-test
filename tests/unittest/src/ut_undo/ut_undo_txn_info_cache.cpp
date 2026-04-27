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
 */
#include "ut_undo/ut_undo_txn_info_cache.h"
#include "common/error/dstore_error.h"

using namespace DSTORE;

/* if logicSlotId < m_recycleLogicSlotId, get txn info from cache quickly */
TEST_F(UndoTxnInfoCacheTest, UndoTxnInfoCacheQuickJudgeTest_level0)
{
    /* alloc zid and logicSlotId */
    ZoneId zid = INVALID_ZONE_ID;
    ASSERT_EQ(m_undoMgr->AllocateZoneId(zid), STORAGE_OK);
    UndoZone *outUzone = nullptr;
    ASSERT_EQ(m_undoMgr->GetUndoZone(zid, &outUzone, true), DSTORE_SUCC);
    ASSERT_TRUE(m_undoMgr->m_undoZones[zid] != nullptr);
    Xid xid;
    m_tranMgr->AllocTransactionSlot(xid);
    ASSERT_EQ(xid.m_zoneId, 0);
    ASSERT_EQ(xid.m_logicSlotId, 0);
    m_tranMgr->CommitTransactionSlot(xid);

    /* get txn info from cache quickly */
    m_undoMgr->m_txnInfoCache->m_recycleLogicSlotId[xid.m_zoneId] = 10;
    TransactionSlot outTrxSlot;
    CommitSeqNo recycleCsnMin = g_storageInstance->GetCsnMgr()->GetRecycleCsnMin(INVALID_PDB_ID);
    RetStatus ret = m_undoMgr->ReadTxnInfoFromCache(xid, outTrxSlot, recycleCsnMin);
    ASSERT_EQ(STORAGE_FUNC_SUCC(ret), true);
    ASSERT_EQ(outTrxSlot.status, TXN_STATUS_FROZEN);
    ASSERT_EQ(outTrxSlot.csn, INVALID_CSN);
    //ASSERT_EQ(m_undoMgr->m_txnInfoCache->m_cachedEntry[xid.m_zoneId], nullptr);
}

/* when read cache, if logicSlotId + 1 > m_recycleLogicSlotId and txn status is frozen, refresh m_recycleLogicSlotId */
TEST_F(UndoTxnInfoCacheTest, UndoReadCacheRecycleLogicSlotIdTest_level0)
{
    /* alloc zid and logicSlotId */
    ZoneId zid = INVALID_ZONE_ID;
    ASSERT_EQ(m_undoMgr->AllocateZoneId(zid), STORAGE_OK);
    UndoZone *outUzone = nullptr;
    ASSERT_EQ(m_undoMgr->GetUndoZone(zid, &outUzone, true), DSTORE_SUCC);
    ASSERT_TRUE(m_undoMgr->m_undoZones[zid] != nullptr);
    Xid xid;
    m_tranMgr->AllocTransactionSlot(xid);
    ASSERT_EQ(xid.m_zoneId, 0);
    ASSERT_EQ(xid.m_logicSlotId, 0);
    m_tranMgr->CommitTransactionSlot(xid);

    /* create cache */
    TransactionSlot inTxnSlot;
    errno_t rc = memset_s(&inTxnSlot, sizeof(inTxnSlot), 0, sizeof(inTxnSlot));
    storage_securec_check(rc, "\0", "\0");
    CommitSeqNo recycleCsnMin = g_storageInstance->GetCsnMgr()->GetRecycleCsnMin(INVALID_PDB_ID);
    m_undoMgr->WriteTxnInfoToCache(xid, inTxnSlot, recycleCsnMin);
    ASSERT_NE(m_undoMgr->m_txnInfoCache->m_cachedEntry[xid.m_zoneId], nullptr);

    /* write cache */
    inTxnSlot.status = TXN_STATUS_COMMITTED;
    inTxnSlot.csn = 1;
    inTxnSlot.logicSlotId = xid.m_logicSlotId;
    m_undoMgr->WriteTxnInfoToCache(xid, inTxnSlot, recycleCsnMin);
    /* read cache and refresh m_recycleLogicSlotId */
    ASSERT_EQ(g_storageInstance->GetCsnMgr()->GetRecycleCsnMin(INVALID_PDB_ID), 1);
    g_storageInstance->GetCsnMgr()->SetLocalCsnMin(2);
    recycleCsnMin = g_storageInstance->GetCsnMgr()->GetRecycleCsnMin(INVALID_PDB_ID);
    TransactionSlot outTxnSlot;
    ASSERT_EQ(m_undoMgr->m_txnInfoCache->m_recycleLogicSlotId[xid.m_zoneId], 0);
    RetStatus ret = m_undoMgr->ReadTxnInfoFromCache(xid, outTxnSlot, recycleCsnMin);
    ASSERT_EQ(STORAGE_FUNC_SUCC(ret), true);
    ASSERT_EQ(m_undoMgr->m_txnInfoCache->m_recycleLogicSlotId[xid.m_zoneId], xid.m_logicSlotId + 1);
}

/* when write cache, if logicSlotId + 1 > m_recycleLogicSlotId and txn status is frozen, refresh m_recycleLogicSlotId */
TEST_F(UndoTxnInfoCacheTest, UndoWriteCacheRecycleLogicSlotIdTest_level0)
{
    /* alloc zid and logicSlotId */
    ZoneId zid = INVALID_ZONE_ID;
    ASSERT_EQ(m_undoMgr->AllocateZoneId(zid), STORAGE_OK);
    UndoZone *outUzone = nullptr;
    ASSERT_EQ(m_undoMgr->GetUndoZone(zid, &outUzone, true), DSTORE_SUCC);
    ASSERT_TRUE(m_undoMgr->m_undoZones[zid] != nullptr);
    Xid xid;
    m_tranMgr->AllocTransactionSlot(xid);
    ASSERT_EQ(xid.m_zoneId, 0);
    ASSERT_EQ(xid.m_logicSlotId, 0);
    m_tranMgr->CommitTransactionSlot(xid);

    /* create cache */
    TransactionSlot inTxnSlot;
    errno_t rc = memset_s(&inTxnSlot, sizeof(inTxnSlot), 0, sizeof(inTxnSlot));
    storage_securec_check(rc, "\0", "\0");
    CommitSeqNo recycleCsnMin = g_storageInstance->GetCsnMgr()->GetRecycleCsnMin(INVALID_PDB_ID);
    m_undoMgr->WriteTxnInfoToCache(xid, inTxnSlot, recycleCsnMin);
    ASSERT_NE(m_undoMgr->m_txnInfoCache->m_cachedEntry[xid.m_zoneId], nullptr);

    /* write cache and refresh m_recycleLogicSlotId */
    inTxnSlot.status = TXN_STATUS_FROZEN;
    inTxnSlot.csn = 1;
    inTxnSlot.logicSlotId = xid.m_logicSlotId;
    ASSERT_EQ(g_storageInstance->GetCsnMgr()->GetRecycleCsnMin(INVALID_PDB_ID), 1);
    g_storageInstance->GetCsnMgr()->SetLocalCsnMin(2);
    recycleCsnMin = g_storageInstance->GetCsnMgr()->GetRecycleCsnMin(INVALID_PDB_ID);
    ASSERT_EQ(m_undoMgr->m_txnInfoCache->m_recycleLogicSlotId[xid.m_zoneId], 0);
    m_undoMgr->WriteTxnInfoToCache(xid, inTxnSlot, recycleCsnMin);
    ASSERT_EQ(m_undoMgr->m_txnInfoCache->m_recycleLogicSlotId[xid.m_zoneId], xid.m_logicSlotId + 1);
}

TEST_F(UndoTxnInfoCacheTest, UndoWriteCacheTest_level0)
{
    /* alloc zid and logicSlotId */
    ZoneId zid = INVALID_ZONE_ID;
    ASSERT_EQ(m_undoMgr->AllocateZoneId(zid), STORAGE_OK);
    UndoZone *outUzone = nullptr;
    ASSERT_EQ(m_undoMgr->GetUndoZone(zid, &outUzone, true), DSTORE_SUCC);
    ASSERT_TRUE(m_undoMgr->m_undoZones[zid] != nullptr);
    Xid xid;
    m_tranMgr->AllocTransactionSlot(xid);
    ASSERT_EQ(xid.m_zoneId, 0);
    ASSERT_EQ(xid.m_logicSlotId, 0);
    m_tranMgr->CommitTransactionSlot(xid);

    /* only write cache when txn status commited, aborted, frozen and csn >= recycleCsnMin */
    CachedTransactionSlot* cachedSlots = m_undoMgr->m_txnInfoCache->m_cachedEntry[xid.m_zoneId];
    TransactionSlot inTxnSlot;
    inTxnSlot.status = TXN_STATUS_FROZEN;
    inTxnSlot.csn = 1;
    inTxnSlot.logicSlotId = xid.m_logicSlotId;
    uint32 slotId = xid.m_logicSlotId % CACHED_SLOT_NUM_PER_ZONE;
    CommitSeqNo recycleCsnMin = g_storageInstance->GetCsnMgr()->GetRecycleCsnMin(INVALID_PDB_ID);
    m_undoMgr->WriteTxnInfoToCache(xid, inTxnSlot, recycleCsnMin);
    ASSERT_EQ(TXN_STATUS_FROZEN, cachedSlots[slotId].txnInfo.status);

    cachedSlots[slotId].txnInfo.status = TXN_STATUS_UNKNOWN;
    inTxnSlot.status = TXN_STATUS_COMMITTED;
    m_undoMgr->WriteTxnInfoToCache(xid, inTxnSlot, recycleCsnMin);
    ASSERT_EQ(TXN_STATUS_COMMITTED, cachedSlots[slotId].txnInfo.status);

    cachedSlots[slotId].txnInfo.status = TXN_STATUS_UNKNOWN;
    inTxnSlot.status = TXN_STATUS_ABORTED;
    m_undoMgr->WriteTxnInfoToCache(xid, inTxnSlot, recycleCsnMin);
    ASSERT_EQ(TXN_STATUS_ABORTED, cachedSlots[slotId].txnInfo.status);

    /* won't write cache because csn < recycleCsnMin, refresh m_recycleLogicSlotId */
    ASSERT_EQ(g_storageInstance->GetCsnMgr()->GetRecycleCsnMin(INVALID_PDB_ID), 1);
    g_storageInstance->GetCsnMgr()->SetLocalCsnMin(2);
    recycleCsnMin = g_storageInstance->GetCsnMgr()->GetRecycleCsnMin(INVALID_PDB_ID);
    cachedSlots[slotId].txnInfo.status = TXN_STATUS_UNKNOWN;
    inTxnSlot.status = TXN_STATUS_FROZEN;
    m_undoMgr->WriteTxnInfoToCache(xid, inTxnSlot, recycleCsnMin);
    ASSERT_EQ(TXN_STATUS_UNKNOWN, cachedSlots[slotId].txnInfo.status);
    ASSERT_EQ(m_undoMgr->m_txnInfoCache->m_recycleLogicSlotId[xid.m_zoneId], xid.m_logicSlotId + 1);
}

TEST_F(UndoTxnInfoCacheTest, UndoReadCacheTest_level0)
{
    /* alloc zid and logicSlotId */
    ZoneId zid = INVALID_ZONE_ID;
    ASSERT_EQ(m_undoMgr->AllocateZoneId(zid), STORAGE_OK);
    UndoZone *outUzone = nullptr;
    ASSERT_EQ(m_undoMgr->GetUndoZone(zid, &outUzone, true), DSTORE_SUCC);
    ASSERT_TRUE(m_undoMgr->m_undoZones[zid] != nullptr);
    Xid xid;
    m_tranMgr->AllocTransactionSlot(xid);
    ASSERT_EQ(xid.m_zoneId, 0);
    ASSERT_EQ(xid.m_logicSlotId, 0);
    m_tranMgr->CommitTransactionSlot(xid);

    /* write cache */
    CachedTransactionSlot* cachedSlots = m_undoMgr->m_txnInfoCache->m_cachedEntry[xid.m_zoneId];
    TransactionSlot inTxnSlot;
    inTxnSlot.status = TXN_STATUS_COMMITTED;
    inTxnSlot.csn = 1;
    inTxnSlot.logicSlotId = xid.m_logicSlotId;
    uint32 slotId = xid.m_logicSlotId % CACHED_SLOT_NUM_PER_ZONE;
    CommitSeqNo recycleCsnMin = g_storageInstance->GetCsnMgr()->GetRecycleCsnMin(INVALID_PDB_ID);
    m_undoMgr->WriteTxnInfoToCache(xid, inTxnSlot, recycleCsnMin);
    ASSERT_EQ(TXN_STATUS_COMMITTED, cachedSlots[slotId].txnInfo.status);

    /* read txn info from cache next time */
    TransactionSlot outTrxSlot;
    RetStatus ret = m_undoMgr->ReadTxnInfoFromCache(xid, outTrxSlot, recycleCsnMin);
    ASSERT_EQ(STORAGE_FUNC_SUCC(ret), true);
    ASSERT_EQ(inTxnSlot.status, outTrxSlot.status);
    ASSERT_EQ(inTxnSlot.csn, outTrxSlot.csn);
    ASSERT_EQ(inTxnSlot.logicSlotId, outTrxSlot.logicSlotId);
}

void UndoTxnInfoCacheTest::UndoWriteCacheTest(uint64 index)
{
    Xid xid = {0, index};
    TransactionSlot inTxnSlot;

    inTxnSlot.logicSlotId = index;
    inTxnSlot.status = static_cast<TrxSlotStatus>(index % TXN_STATUS_FAILED);
    if (inTxnSlot.status == TXN_STATUS_FROZEN || inTxnSlot.status == TXN_STATUS_COMMITTED ||
        inTxnSlot.status == TXN_STATUS_ABORTED) {
        inTxnSlot.csn = index + 1;
    } else {
        inTxnSlot.csn = 0;
    }

    m_undoMgr->WriteTxnInfoToCache(xid, inTxnSlot, 1);
}

void UndoTxnInfoCacheTest::UndoReadCacheTest(uint64 index)
{
    Xid xid = {0, index};
    TransactionSlot inTxnSlot;
    errno_t rc = memset_s(&inTxnSlot, sizeof(inTxnSlot), 0, sizeof(inTxnSlot));
    storage_securec_check(rc, "\0", "\0");
    RetStatus ret = m_undoMgr->ReadTxnInfoFromCache(xid, inTxnSlot, 1);
    if (STORAGE_FUNC_SUCC(ret)) {
        StorageAssert(inTxnSlot.csn == index + 1);
        StorageAssert((inTxnSlot.status == TXN_STATUS_FROZEN || inTxnSlot.status == TXN_STATUS_COMMITTED ||
            inTxnSlot.status == TXN_STATUS_ABORTED));
    }
}

TEST_F(UndoTxnInfoCacheTest, UndoMultiThreadTest_level0)
{
    for (uint64 i = 0; i < 1000; i++) {
        m_pool.AddTask(UndoWriteCacheTask, this, i);
    }

    for (uint64 i = 0; i < 1000; i++) {
        m_pool.AddTask(UndoReadCacheTask, this, i);
    }

    m_pool.WaitAllTaskFinish();
}
