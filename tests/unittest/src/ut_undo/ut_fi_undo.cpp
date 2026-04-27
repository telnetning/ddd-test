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
#include "ut_undo/ut_fi_undo.h"

using namespace DSTORE;

/* this case ensure no matter restore first or service fisrt, both can restore undo zone info correctly after reboot. */
TEST_F(UndoFiTest, UndoRestoreAndServiceTest_level0)
{
    /* Step 1: create UndoMgr, TransactionMgr */
    StartUp();

    /* Step 2: alloc zone */
    ZoneId zid = INVALID_ZONE_ID;
    ASSERT_EQ(m_undoMgr->AllocateZoneId(zid), STORAGE_OK);
    ASSERT_NE(zid, INVALID_ZONE_ID);

    /* Step 3: alloc xid and insert undo record */
    UndoZoneRecordInfo recordInfo;
    StartService(m_undoMgr, m_tranMgr, &recordInfo, 10, 100);

    /* Step 4: record UndoZone info */
    ASSERT_TRUE(m_undoMgr->m_undoZones[zid] != nullptr);
    UndoZoneTestInfo expect;
    RecordUndoZoneInfo(m_undoMgr->m_undoZones[zid], &expect);

    /* Step 5: reboot */
    Reboot();

    /* Step 6: restore first, then service */
    ASSERT_TRUE(m_undoMgr->m_undoZones[zid] == nullptr);
    m_undoMgr->RecoverUndoZone();
    ASSERT_TRUE(m_undoMgr->m_undoZones[zid] != nullptr);
    UndoZoneTestInfo real1;
    RecordUndoZoneInfo(m_undoMgr->m_undoZones[zid], &real1);
    ASSERT_EQ(memcmp(&expect, &real1, sizeof(UndoZoneTestInfo)), 0);

    UndoZone* outUzone = nullptr;
    ASSERT_EQ(m_undoMgr->GetUndoZone(zid, &outUzone, true), DSTORE_SUCC);
    UndoZoneTestInfo real2;
    RecordUndoZoneInfo(m_undoMgr->m_undoZones[zid], &real2);
    ASSERT_EQ(memcmp(&expect, &real2, sizeof(UndoZoneTestInfo)), 0);

    /* Step 7: reboot */
    Reboot();

    /* Step 8: service first, then restore */
    ASSERT_TRUE(m_undoMgr->m_undoZones[zid] == nullptr);
    ASSERT_EQ(m_undoMgr->GetUndoZone(zid, &outUzone, true), DSTORE_SUCC);
    ASSERT_TRUE(m_undoMgr->m_undoZones[zid] != nullptr);
    UndoZoneTestInfo real3;
    RecordUndoZoneInfo(m_undoMgr->m_undoZones[zid], &real3);
    ASSERT_EQ(memcmp(&expect, &real3, sizeof(UndoZoneTestInfo)), 0);

    m_undoMgr->RecoverUndoZone();
    UndoZoneTestInfo real4;
    RecordUndoZoneInfo(m_undoMgr->m_undoZones[zid], &real4);
    ASSERT_EQ(memcmp(&expect, &real4, sizeof(UndoZoneTestInfo)), 0);

    /* Step 9: shut down */
    ShutDown();
}

/* this case ensure no matter restore first or recycle fisrt, must finish restore then recycle after reboot. */
TEST_F(UndoFiTest, UndoRestoreAndRecycleTest_level0)
{
    /* Step 1: create UndoMgr, TransactionMgr */
    StartUp();

    /* Step 2: alloc zone */
    ZoneId zid = INVALID_ZONE_ID;
    ASSERT_EQ(m_undoMgr->AllocateZoneId(zid), STORAGE_OK);
    ASSERT_NE(zid, INVALID_ZONE_ID);

    /* Step 3: alloc slot and insert undo record */
    UndoZoneRecordInfo recordInfo;
    StartService(m_undoMgr, m_tranMgr, &recordInfo, 10, 100);

    /* Step 4: record UndoZone info */
    ASSERT_TRUE(m_undoMgr->m_undoZones[zid] != nullptr);
    UndoZoneTestInfo expect;
    RecordUndoZoneInfo(m_undoMgr->m_undoZones[zid], &expect);

    /* Step 5: reboot */
    Reboot();

    /* Step 6: restore first, then recycle */
    ASSERT_TRUE(m_undoMgr->m_undoZones[zid] == nullptr);
    m_undoMgr->RecoverUndoZone();
    ASSERT_TRUE(m_undoMgr->m_undoZones[zid] != nullptr);
    UndoZoneTestInfo real1;
    RecordUndoZoneInfo(m_undoMgr->m_undoZones[zid], &real1);
    ASSERT_EQ(memcmp(&expect, &real1, sizeof(UndoZoneTestInfo)), 0);

    /* no one slot and undo page could recycle */
    m_undoMgr->Recycle(1);
    UndoZoneTestInfo real2;
    RecordUndoZoneInfo(m_undoMgr->m_undoZones[zid], &real2);
    ASSERT_EQ(memcmp(&expect, &real2, sizeof(UndoZoneTestInfo)), 0);

    /* Step 7: reboot */
    Reboot();

    /* Step 8: recycle first, recycle will continue because has not restore, then restore */
    ASSERT_TRUE(m_undoMgr->m_undoZones[zid] == nullptr);
    m_undoMgr->Recycle(1);
    ASSERT_TRUE(m_undoMgr->m_undoZones[zid] == nullptr);

    m_undoMgr->RecoverUndoZone();
    ASSERT_TRUE(m_undoMgr->m_undoZones[zid] != nullptr);
    UndoZoneTestInfo real3;
    RecordUndoZoneInfo(m_undoMgr->m_undoZones[zid], &real3);
    ASSERT_EQ(memcmp(&expect, &real3, sizeof(UndoZoneTestInfo)), 0);

    /* Step 9: shut down */
    ShutDown();
}

/* this case ensure no matter service or recycle who first, undo zone key fields are correctly maintained.  */
TEST_F(UndoFiTest, UndoServiceAndRecycleTest_level0)
{
    /* Step 1: create UndoMgr, TransactionMgr */
    StartUp();

    /* Step 2: alloc zone */
    ZoneId zid = INVALID_ZONE_ID;
    ASSERT_EQ(m_undoMgr->AllocateZoneId(zid), STORAGE_OK);
    ASSERT_NE(zid, INVALID_ZONE_ID);

    /* Step 3: alloc slot and insert undo record */
    UndoZoneRecordInfo recordInfo;
    StartService(m_undoMgr, m_tranMgr, &recordInfo, 10, 100);

    /* Step 4: record UndoZone info */
    UndoZoneTestInfo expect;
    RecordUndoZoneInfo(m_undoMgr->m_undoZones[zid], &expect);
    ASSERT_EQ(expect.m_recycleLogicSlotId, 0);
    ASSERT_EQ(expect.m_nextFreeLogicSlotId, 10);
    ASSERT_EQ(expect.m_undoRecyclePageId, recordInfo.startUndoPtr[0].GetPageId());
    ASSERT_EQ(expect.m_nextAppendUndoPtr, recordInfo.nextAppendUndoPtr);

    /* Step 6: recycle 5 slots */
    m_undoMgr->Recycle(recordInfo.csn[4] + 1);
    UndoZoneTestInfo real1;
    RecordUndoZoneInfo(m_undoMgr->m_undoZones[zid], &real1);
    /* m_nextAppendUndoPtr, m_nextFreeLogicSlotId won't update when recycle */
    ASSERT_EQ(real1.m_recycleLogicSlotId, 5);
    ASSERT_EQ(real1.m_nextFreeLogicSlotId, 10);
    ASSERT_EQ(real1.m_undoRecyclePageId, recordInfo.startUndoPtr[5].GetPageId());
    ASSERT_EQ(real1.m_nextAppendUndoPtr, recordInfo.nextAppendUndoPtr);

    /* Step 7: start 6 txn */
    StartService(m_undoMgr, m_tranMgr, &recordInfo, 6, 33);
    UndoZoneTestInfo real2;
    RecordUndoZoneInfo(m_undoMgr->m_undoZones[zid], &real2);
    /* m_recycleLogicSlotId, m_undoRecyclePageId won't update when service */
    ASSERT_EQ(real2.m_recycleLogicSlotId, 5);
    ASSERT_EQ(real2.m_nextFreeLogicSlotId, 16);
    ASSERT_EQ(real2.m_undoRecyclePageId, recordInfo.startUndoPtr[5].GetPageId());
    ASSERT_EQ(real2.m_nextAppendUndoPtr, recordInfo.nextAppendUndoPtr);

    /* Step 8: recycle 5 slots */
    m_undoMgr->Recycle(recordInfo.csn[9] + 1);
    UndoZoneTestInfo real3;
    RecordUndoZoneInfo(m_undoMgr->m_undoZones[zid], &real3);
    /* m_nextAppendUndoPtr, m_nextFreeLogicSlotId won't update when recycle */
    ASSERT_EQ(real3.m_recycleLogicSlotId, 10);
    ASSERT_EQ(real3.m_nextFreeLogicSlotId, 16);
    ASSERT_EQ(real3.m_undoRecyclePageId, recordInfo.startUndoPtr[10].GetPageId());
    ASSERT_EQ(real3.m_nextAppendUndoPtr, recordInfo.nextAppendUndoPtr);

    /* Step 9: shut down */
    ShutDown();
}