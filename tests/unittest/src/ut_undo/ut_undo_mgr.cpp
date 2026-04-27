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
#include "ut_undo/ut_undo_mgr.h"
#include "common/error/dstore_error.h"

using namespace DSTORE;

/* undo manager */
TEST_F(UndoMgrTest, UndoMgrZoneAccessTest_level0)
{
    ZoneId savedZid;
    UndoMgr undoMgr(m_bufferMgr, m_pdbId);
    undoMgr.Init(g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));
    undoMgr.LoadUndoMapSegment();
    ZoneId zid = INVALID_ZONE_ID;

    /* Test 1: Allocate and release. */
    ASSERT_EQ(undoMgr.AllocateZoneId(zid), STORAGE_OK);
    ASSERT_NE(zid, INVALID_ZONE_ID);
    savedZid = zid;
    undoMgr.ReleaseZoneId(zid);
    ASSERT_EQ(zid, INVALID_ZONE_ID);

    ASSERT_EQ(undoMgr.AllocateZoneId(zid), STORAGE_OK);
    ASSERT_EQ(zid, savedZid);

    /* Test 2: Get the allocated undo zone. */
    UndoZone *outUzone = nullptr;
    ASSERT_EQ(undoMgr.GetUndoZone(zid, &outUzone, true), DSTORE_SUCC);
    ASSERT_NE(outUzone, nullptr);

    /* Test 3: Switch undo zone. */
    ASSERT_EQ(undoMgr.SwitchZone(outUzone, zid), DSTORE_SUCC);
    ASSERT_NE(zid, savedZid);
    ASSERT_NE(outUzone, nullptr);
    undoMgr.ReleaseZoneId(zid);
    ASSERT_EQ(zid, INVALID_ZONE_ID);
}

TEST_F(UndoMgrTest, UndoZoneIdSegmentIdMapTest_level0)
{
    UndoMgr *undoMgr = DstoreNew(m_ut_memory_context) UndoMgr(m_bufferMgr, m_pdbId);
    undoMgr->Init(g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));
    
    undoMgr->LoadUndoMapSegment();
    constexpr uint32 UNDO_ZONE_TEST_COUNT = 10;

    ZoneId *zidArr = (ZoneId *)DstorePalloc0(sizeof(ZoneId) * UNDO_ZONE_TEST_COUNT);
    for (uint32 i = 0; i < UNDO_ZONE_TEST_COUNT; i++) {
        zidArr[i] = INVALID_ZONE_ID;
    }
    std::vector<PageId> pageIds;
    for (uint32 i = 0; i < UNDO_ZONE_TEST_COUNT; i++) {
        undoMgr->AllocateZoneId(*(zidArr + i));
        ASSERT_EQ(zidArr[i], ZoneId(i));
    }
    for (uint32 i = 0; i < UNDO_ZONE_TEST_COUNT; i++) {
        ASSERT_NE(zidArr[i], INVALID_ZONE_ID);
        UndoZone *outUzone = nullptr;
        ASSERT_EQ(undoMgr->GetUndoZone(zidArr[i], &outUzone, true), DSTORE_SUCC);
        pageIds.push_back(outUzone->m_segment->GetSegmentMetaPageId());
    }

    /* reboot undo mgr to check map data correct. */
    if (undoMgr != nullptr) {
        delete undoMgr;
        undoMgr = nullptr;
    }
    undoMgr = DstoreNew(m_ut_memory_context) UndoMgr(m_bufferMgr, m_pdbId);
    undoMgr->Init(g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));
    
    undoMgr->LoadUndoMapSegment();
    for (uint32 i = 0; i < UNDO_ZONE_TEST_COUNT; i++) {
        ASSERT_NE(zidArr[i], INVALID_ZONE_ID);
        UndoZone *outUzone = nullptr;
        ASSERT_EQ(undoMgr->GetUndoZone(zidArr[i], &outUzone), DSTORE_SUCC);
        ASSERT_EQ(pageIds[i], outUzone->m_segment->GetSegmentMetaPageId());
    }
    if (undoMgr != nullptr) {
        delete undoMgr;
        undoMgr = nullptr;
    }
    DstorePfreeExt(zidArr);
}

static void GenerageNeedRollbackUndoZone(UndoMgr *&undoMgr, ZoneId *&zidArr, uint32 undoZoneCount)
{
    zidArr = (ZoneId *)DstorePalloc0(sizeof(ZoneId) * undoZoneCount);
    for (uint32 i = 0; i < undoZoneCount; i++) {
        zidArr[i] = INVALID_ZONE_ID;
    }

    for (uint32 i = 0; i < undoZoneCount; i++) {
        undoMgr->AllocateZoneId(*(zidArr + i));
        ASSERT_EQ(zidArr[i], ZoneId(i));
    }

    Xid xid(0);
    for (uint32 i = 0; i < undoZoneCount; i++) {
        ASSERT_NE(zidArr[i], INVALID_ZONE_ID);
        UndoZone *outUzone = nullptr;
        ASSERT_EQ(undoMgr->GetUndoZone(zidArr[i], &outUzone, true), DSTORE_SUCC);
        ASSERT_EQ(outUzone->IsAsyncRollbacking(), false);

        xid = outUzone->AllocSlot();

        Xid rollbackXid;
        ASSERT_EQ(outUzone->m_txnSlotManager->IsUndoZoneNeedRollback(rollbackXid), true);
        ASSERT_EQ(rollbackXid, xid);
    }
}

static uint32 GetTaskSize(RollbackTrxTaskMgr *taskMgr)
{
    uint32 cnt = 0;
    dlist_mutable_iter iter;
    taskMgr->m_queueSpinlock.Acquire();
    dlist_foreach_modify(iter, &(taskMgr->m_rollbackTrxTaskQueue)) {
        ++cnt;
    }
    taskMgr->m_queueSpinlock.Release();
    return cnt;
}

TEST_F(UndoMgrTest, UndoZoneReInit_level0)
{
    UndoMgr *undoMgr = DstoreNew(m_ut_memory_context) UndoMgr(m_bufferMgr, m_pdbId);
    undoMgr->Init(g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));
    
    undoMgr->LoadUndoMapSegment();
    constexpr uint32 UNDO_ZONE_TEST_COUNT = 10;

    RollbackTrxTaskMgr *taskMgr =
            g_storageInstance->GetPdb(g_defaultPdbId)->GetTransactionMgr()->m_rollbackTrxTaskMgr;
    ASSERT_EQ(taskMgr->m_dispatchThread, nullptr);

    ZoneId *zidArr = nullptr;
    GenerageNeedRollbackUndoZone(undoMgr, zidArr, UNDO_ZONE_TEST_COUNT);

    Size currSize = GetTaskSize(taskMgr);
    ASSERT_EQ(currSize, 0);

    /* reboot undo mgr to check rollback data correct. */
    if (undoMgr != nullptr) {
        delete undoMgr;
        undoMgr = nullptr;
    }
    thrd->GetCore()->bufTagArray->Initialize();

    undoMgr = DstoreNew(m_ut_memory_context) UndoMgr(m_bufferMgr, m_pdbId);
    undoMgr->Init(g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));
    
    undoMgr->LoadUndoMapSegment();

    for (uint32 i = 0; i < UNDO_ZONE_TEST_COUNT; i++) {
        UndoZone *outUzone = nullptr;
        ASSERT_EQ(undoMgr->GetUndoZone(i, &outUzone, true), DSTORE_SUCC);
        ASSERT_EQ(outUzone->IsAsyncRollbacking(), true);
    }

    currSize = GetTaskSize(taskMgr);
    ASSERT_EQ(currSize, UNDO_ZONE_TEST_COUNT);

    taskMgr->StartDispatch();
    taskMgr->StopDispatch();
    while (taskMgr->IsDispatching()) {
        sleep(1);
    }
    currSize = GetTaskSize(taskMgr);
    ASSERT_EQ(currSize, 0);

    for (uint32 i = 0; i < UNDO_ZONE_TEST_COUNT; i++) {
        ASSERT_NE(zidArr[i], INVALID_ZONE_ID);
        UndoZone *outUzone = nullptr;
        ASSERT_EQ(undoMgr->GetUndoZone(zidArr[i], &outUzone), DSTORE_SUCC);
        ASSERT_EQ(outUzone->IsAsyncRollbacking(), false);

        Xid rollbackXid;
        ASSERT_EQ(outUzone->m_txnSlotManager->IsUndoZoneNeedRollback(rollbackXid), false);
    }

    if (undoMgr != nullptr) {
        delete undoMgr;
        undoMgr = nullptr;
    }
    DstorePfreeExt(zidArr);
}

TEST_F(UndoMgrTest, UndoZoneAllocLock_level0)
{
    UndoMgr *undoMgr = DstoreNew(m_ut_memory_context) UndoMgr(m_bufferMgr, m_pdbId);
    undoMgr->Init(g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));
    
    undoMgr->LoadUndoMapSegment();
    constexpr uint32 UNDO_ZONE_TEST_COUNT = 10;

    RollbackTrxTaskMgr *taskMgr =
            g_storageInstance->GetPdb(g_defaultPdbId)->GetTransactionMgr()->m_rollbackTrxTaskMgr;
    ASSERT_EQ(taskMgr->m_dispatchThread, nullptr);

    ZoneId *zidArr = nullptr;
    GenerageNeedRollbackUndoZone(undoMgr, zidArr, UNDO_ZONE_TEST_COUNT);

    /* reboot undo mgr to check rollback data correct. */
    if (undoMgr != nullptr) {
        delete undoMgr;
        undoMgr = nullptr;
    }

    thrd->GetCore()->bufTagArray->Initialize();
    undoMgr = DstoreNew(m_ut_memory_context) UndoMgr(m_bufferMgr, m_pdbId);
    undoMgr->Init(g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));
    
    undoMgr->LoadUndoMapSegment();

    for (uint32 i = 0; i < UNDO_ZONE_TEST_COUNT; i++) {
        UndoZone *outUzone = nullptr;
        ASSERT_EQ(undoMgr->GetUndoZone(i, &outUzone, true), DSTORE_SUCC);
        ASSERT_EQ(outUzone->IsAsyncRollbacking(), true);
    }

    Size currSize = GetTaskSize(taskMgr);
    ASSERT_EQ(currSize, UNDO_ZONE_TEST_COUNT);

    for (uint32 i = 0; i < UNDO_ZONE_TEST_COUNT; i++) {
        UndoZone *outUzone = nullptr;
        ASSERT_EQ(undoMgr->GetUndoZone(i, &outUzone), DSTORE_SUCC);
        ASSERT_EQ(outUzone->IsAsyncRollbacking(), true);
    }

    currSize = GetTaskSize(taskMgr);
    ASSERT_EQ(currSize, UNDO_ZONE_TEST_COUNT);

    taskMgr->StartDispatch();
    taskMgr->StopDispatch();
    while (taskMgr->IsDispatching()) {
        sleep(1);
    }

    for (uint32 i = 0; i < UNDO_ZONE_TEST_COUNT; i++) {
        UndoZone *outUzone = nullptr;
        ASSERT_EQ(undoMgr->GetUndoZone(i, &outUzone), DSTORE_SUCC);
        ASSERT_EQ(outUzone->IsAsyncRollbacking(), false);
    }

    if (undoMgr != nullptr) {
        delete undoMgr;
        undoMgr = nullptr;
    }
    DstorePfreeExt(zidArr);
}

TEST_F(UndoMgrTest, UndoZoneSwitchLock_level0)
{
    UndoMgr *undoMgr = DstoreNew(m_ut_memory_context) UndoMgr(m_bufferMgr, m_pdbId);
    undoMgr->Init(g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));
    
    undoMgr->LoadUndoMapSegment();
    constexpr uint32 UNDO_ZONE_TEST_COUNT = 10;

    RollbackTrxTaskMgr *taskMgr =
            g_storageInstance->GetPdb(g_defaultPdbId)->GetTransactionMgr()->m_rollbackTrxTaskMgr;
    ASSERT_EQ(taskMgr->m_dispatchThread, nullptr);

    ZoneId *zidArr = nullptr;
    GenerageNeedRollbackUndoZone(undoMgr, zidArr, UNDO_ZONE_TEST_COUNT);

    /* reboot undo mgr to check rollback data correct. */
    if (undoMgr != nullptr) {
        delete undoMgr;
        undoMgr = nullptr;
    }

    thrd->GetCore()->bufTagArray->Initialize();
    undoMgr = DstoreNew(m_ut_memory_context) UndoMgr(m_bufferMgr, m_pdbId);
    undoMgr->Init(g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));
    
    undoMgr->LoadUndoMapSegment();

    UndoZone *outUzone = nullptr;
    ZoneId zid = 0;
    ASSERT_EQ(undoMgr->GetUndoZone(zid, &outUzone, true), DSTORE_SUCC);

    for (uint32 i = 0; i < UNDO_ZONE_TEST_COUNT; i++) {
        UndoZone *outUzone = nullptr;
        ZoneId zid = -1;
        ASSERT_EQ(undoMgr->SwitchZone(outUzone, zid), DSTORE_SUCC);
        ASSERT_EQ(zid, i);
        ASSERT_EQ(outUzone->IsAsyncRollbacking(), true);
    }

    ASSERT_EQ(undoMgr->SwitchZone(outUzone, zid), DSTORE_SUCC);
    ASSERT_EQ(zid, UNDO_ZONE_TEST_COUNT);

    Size currSize = GetTaskSize(taskMgr);
    ASSERT_EQ(currSize, UNDO_ZONE_TEST_COUNT);

    taskMgr->StartDispatch();
    taskMgr->StopDispatch();
    while (taskMgr->IsDispatching()) {
        sleep(1);
    }

    /* reboot undo mgr to check rollback data correct. */
    if (undoMgr != nullptr) {
        delete undoMgr;
        undoMgr = nullptr;
    }

    thrd->GetCore()->bufTagArray->Initialize();
    undoMgr = DstoreNew(m_ut_memory_context) UndoMgr(m_bufferMgr, m_pdbId);
    undoMgr->Init(g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));
    
    undoMgr->LoadUndoMapSegment();

    ASSERT_EQ(undoMgr->GetUndoZone(zid, &outUzone), DSTORE_SUCC);
    ASSERT_EQ(outUzone->IsAsyncRollbacking(), false);

    for (uint32 i = 0; i < UNDO_ZONE_TEST_COUNT; i++) {
        UndoZone *outUzone = nullptr;
        ZoneId zid = -1;
        ASSERT_EQ(undoMgr->SwitchZone(outUzone, zid), DSTORE_SUCC);
        ASSERT_EQ(zid, i);
        ASSERT_EQ(outUzone->IsAsyncRollbacking(), false);
    }
}

TEST_F(UndoMgrTest, DispatchTransactionSlot_level0)
{
    UndoMgr *undoMgr = DstoreNew(m_ut_memory_context) UndoMgr(m_bufferMgr, m_pdbId);
    undoMgr->Init(g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));
    
    undoMgr->LoadUndoMapSegment();
    constexpr uint32 UNDO_ZONE_TEST_COUNT = 10;

    RollbackTrxTaskMgr *taskMgr =
            g_storageInstance->GetPdb(g_defaultPdbId)->GetTransactionMgr()->m_rollbackTrxTaskMgr;
    ASSERT_EQ(taskMgr->m_dispatchThread, nullptr);

    ZoneId *zidArr = nullptr;
    GenerageNeedRollbackUndoZone(undoMgr, zidArr, UNDO_ZONE_TEST_COUNT);

    /* reboot undo mgr to check rollback data correct. */
    if (undoMgr != nullptr) {
        delete undoMgr;
        undoMgr = nullptr;
    }

    thrd->GetCore()->bufTagArray->Initialize();
    undoMgr = DstoreNew(m_ut_memory_context) UndoMgr(m_bufferMgr, m_pdbId);
    undoMgr->Init(g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));
    
    undoMgr->LoadUndoMapSegment();

    UndoZone *outUzone = nullptr;
    ZoneId zid = 0;
    ASSERT_EQ(undoMgr->GetUndoZone(zid, &outUzone, true), DSTORE_SUCC);

    CsnMgr *tCsnMgr = g_storageInstance->GetCsnMgr();
    TransactionMgr *tranMgr = DstoreNew(m_ut_memory_context) TransactionMgr(undoMgr, tCsnMgr, g_defaultPdbId);

    Xid resXid;
    tranMgr->AllocTransactionSlot(resXid);
    ASSERT_EQ(resXid.m_zoneId, UNDO_ZONE_TEST_COUNT);

    Size currSize = GetTaskSize(taskMgr);
    ASSERT_EQ(currSize, UNDO_ZONE_TEST_COUNT);

    ASSERT_EQ(undoMgr->SwitchZone(outUzone, zid), DSTORE_SUCC);
    ASSERT_EQ(zid, UNDO_ZONE_TEST_COUNT + 1);

    taskMgr->StartDispatch();
    taskMgr->StopDispatch();
    while (taskMgr->IsDispatching()) {
        sleep(1);
    }

    /* reboot undo mgr to check rollback data correct. */
    if (undoMgr != nullptr) {
        delete undoMgr;
        undoMgr = nullptr;
    }

    thrd->GetCore()->bufTagArray->Initialize();
    undoMgr = DstoreNew(m_ut_memory_context) UndoMgr(m_bufferMgr, m_pdbId);
    undoMgr->Init(g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));
    
    undoMgr->LoadUndoMapSegment();

    ASSERT_EQ(undoMgr->GetUndoZone(zid, &outUzone), DSTORE_SUCC);
    for (uint32 i = 0; i < UNDO_ZONE_TEST_COUNT; i++) {
        UndoZone *outUzone = nullptr;
        ZoneId zid = -1;
        ASSERT_EQ(undoMgr->SwitchZone(outUzone, zid), DSTORE_SUCC);
        ASSERT_EQ(outUzone->IsAsyncRollbacking(), false);
        ASSERT_EQ(zid, i);
    }
}

TEST_F(UndoMgrTest, RecoverUndoZoneAllocSlotTest_level0)
{
    UndoMgr *undoMgr = DstoreNew(m_ut_memory_context) UndoMgr(m_bufferMgr, m_pdbId);
    undoMgr->Init(g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));
    
    undoMgr->LoadUndoMapSegment();
    constexpr uint32 UNDO_ZONE_TEST_COUNT = 10;

    RollbackTrxTaskMgr *taskMgr =
            g_storageInstance->GetPdb(g_defaultPdbId)->GetTransactionMgr()->m_rollbackTrxTaskMgr;
    ASSERT_EQ(taskMgr->m_dispatchThread, nullptr);

    ZoneId *zidArr = nullptr;
    GenerageNeedRollbackUndoZone(undoMgr, zidArr, UNDO_ZONE_TEST_COUNT);

    /* reboot undo mgr to check rollback data correct. */
    if (undoMgr != nullptr) {
        delete undoMgr;
        undoMgr = nullptr;
    }

    thrd->GetCore()->bufTagArray->Initialize();
    undoMgr = DstoreNew(m_ut_memory_context) UndoMgr(m_bufferMgr, m_pdbId);
    undoMgr->Init(g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));
    
    undoMgr->LoadUndoMapSegment();

    CsnMgr *tCsnMgr = g_storageInstance->GetCsnMgr();
    TransactionMgr *tranMgr = DstoreNew(m_ut_memory_context) TransactionMgr(undoMgr, tCsnMgr, g_defaultPdbId);
    Xid resXid;
    tranMgr->AllocTransactionSlot(resXid);
    ASSERT_EQ(resXid.m_zoneId, UNDO_ZONE_TEST_COUNT);

    undoMgr->RecoverUndoZone();

    Size currSize = GetTaskSize(taskMgr);
    ASSERT_EQ(currSize, UNDO_ZONE_TEST_COUNT);

    taskMgr->StartDispatch();
    taskMgr->StopDispatch();
    while (taskMgr->IsDispatching()) {
        sleep(1);
    }

    tranMgr->AllocTransactionSlot(resXid);
    ASSERT_EQ(resXid.m_zoneId, UNDO_ZONE_TEST_COUNT);

    /* reboot undo mgr to check rollback data correct. */
    if (undoMgr != nullptr) {
        delete undoMgr;
        undoMgr = nullptr;
    }

    thrd->GetCore()->bufTagArray->Initialize();
    undoMgr = DstoreNew(m_ut_memory_context) UndoMgr(m_bufferMgr, m_pdbId);
    undoMgr->Init(g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));
    
    undoMgr->LoadUndoMapSegment();

    UndoZone *outUzone = nullptr;
    ASSERT_EQ(undoMgr->GetUndoZone(0, &outUzone), DSTORE_SUCC);
    for (uint32 i = 0; i < UNDO_ZONE_TEST_COUNT; i++) {
        UndoZone *outUzone = nullptr;
        ZoneId zid = -1;
        ASSERT_EQ(undoMgr->SwitchZone(outUzone, zid), DSTORE_SUCC);
        ASSERT_EQ(zid, i);
    }
}

TEST_F(UndoMgrTest, RecoverUndoZoneTest_level0)
{
    UndoMgr *undoMgr = DstoreNew(m_ut_memory_context) UndoMgr(m_bufferMgr, m_pdbId);
    undoMgr->Init(g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));
    
    undoMgr->LoadUndoMapSegment();
    constexpr uint32 UNDO_ZONE_TEST_COUNT = 20;

    RollbackTrxTaskMgr *taskMgr =
            g_storageInstance->GetPdb(g_defaultPdbId)->GetTransactionMgr()->m_rollbackTrxTaskMgr;
    ASSERT_EQ(taskMgr->m_dispatchThread, nullptr);

    ZoneId *zidArr = nullptr;
    GenerageNeedRollbackUndoZone(undoMgr, zidArr, UNDO_ZONE_TEST_COUNT);

    /* reboot undo mgr to check rollback data correct. */
    if (undoMgr != nullptr) {
        delete undoMgr;
        undoMgr = nullptr;
    }

    thrd->GetCore()->bufTagArray->Initialize();
    undoMgr = DstoreNew(m_ut_memory_context) UndoMgr(m_bufferMgr, m_pdbId);
    undoMgr->Init(g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));
    
    undoMgr->LoadUndoMapSegment();

    taskMgr->StartDispatch();
    undoMgr->RecoverUndoZone();

    while (!taskMgr->IsAllTaskFinished()) {
        sleep(1);
    }

    for (uint32 i = 0; i < UNDO_ZONE_TEST_COUNT; i++) {
        UndoZone *outUzone = nullptr;
        ASSERT_EQ(undoMgr->GetUndoZone(i, &outUzone), DSTORE_SUCC);
        ASSERT_EQ(outUzone->IsAsyncRollbacking(), false);
    }
    taskMgr->StopDispatch();
    while (taskMgr->IsDispatching()) {
        taskMgr->WakeupDispatch();
        sleep(1);
    }
    taskMgr->Destroy();
}

