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
#include "ut_undo/ut_undo_mgr_diagnose.h"
#include "common/error/dstore_error.h"

using namespace DSTORE;

TEST_F(UndoMgrDiagnoseTest, UndoMgrDiagnoseGetUndoZoneInfoTest_level0)
{
    UndoMgr undoMgr(m_bufferMgr, m_pdbId);
    undoMgr.Init(g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));
    undoMgr.LoadUndoMapSegment();
    ZoneId zid = INVALID_ZONE_ID;
    undoMgr.AllocateZoneId(zid);
    char *ret_str;

    /* Test 1: invalid zoneid */
    ret_str = UndoMgrDiagnose::GetUndoZoneInfoByZid(m_pdbId, INVALID_ZONE_ID);
    EXPECT_STREQ(ret_str, "Invalid Zone ID -1, it should be [0 - 512).\n");


    /* Test 2: invalid page */
    ret_str = UndoMgrDiagnose::GetUndoZoneInfoByZid(m_pdbId, zid);
    EXPECT_NE(nullptr, strstr(ret_str, "Undo zone has invalid start page id"));

    /* Test 3: normal test */
    std::vector<PageId> pageIds;
    UndoZone *outUzone = nullptr;
    undoMgr.GetUndoZone(zid, &outUzone, true);
    pageIds.push_back(outUzone->m_segment->GetSegmentMetaPageId());

    ret_str = UndoMgrDiagnose::GetUndoZoneInfoByZid(m_pdbId, zid);
    EXPECT_NE(nullptr, strstr(ret_str, "Undo zone has valid start page"));
}

TEST_F(UndoMgrDiagnoseTest, UndoMgrDiagnoseGetUndoZoneStatusTest_level0)
{
    StoragePdb *pdb = g_storageInstance->GetPdb(m_pdbId);
    UndoMgr *undoMgr = pdb->GetUndoMgr();
    undoMgr->Init(g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));
    undoMgr->LoadUndoMapSegment();
    ZoneId zid = INVALID_ZONE_ID;
    undoMgr->AllocateZoneId(zid);
    char *ret_str;

    /* Test 1: invalid zoneid */
    ret_str = UndoMgrDiagnose::GetUndoZoneStatusByZid(m_pdbId, INVALID_ZONE_ID);
    EXPECT_STREQ(ret_str, "Invalid Zone ID -1, it should be [0 - 512).\n");


    /* Test 2: invalid page */
    ret_str = UndoMgrDiagnose::GetUndoZoneStatusByZid(m_pdbId, zid);
    EXPECT_STREQ(ret_str, "Get invalid undo zone.\n");

    /* Test 3: normal test */
    std::vector<PageId> pageIds;
    UndoZone *outUzone = nullptr;
    undoMgr->GetUndoZone(zid, &outUzone, true);
    pageIds.push_back(outUzone->m_segment->GetSegmentMetaPageId());
    undoMgr->AllocateZoneMemory(zid, outUzone->m_segment);
    ret_str = UndoMgrDiagnose::GetUndoZoneStatusByZid(m_pdbId, zid);
    EXPECT_NE(nullptr, strstr(ret_str, "Undo zone has undo page num"));
}

TEST_F(UndoMgrDiagnoseTest, UndoMgrDiagnoseGetUndoSlotStatusTest_level0)
{
    StoragePdb *pdb = g_storageInstance->GetPdb(m_pdbId);
    UndoMgr *undoMgr = pdb->GetUndoMgr();
    undoMgr->Init(g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));
    undoMgr->LoadUndoMapSegment();
    ZoneId zid = INVALID_ZONE_ID;
    undoMgr->AllocateZoneId(zid);
    char *ret_str;

    /* test1 invalid trasactionid */
    ret_str = UndoMgrDiagnose::GetUndoSlotStatusByXid(m_pdbId, -1);
    EXPECT_STREQ(ret_str, "Invalid Xid.\n");

    /* test2 normal test */
    Transaction *activeTransaction = thrd->GetActiveTransaction();
    ASSERT_TRUE(STORAGE_FUNC_SUCC(activeTransaction->Start()));
    ASSERT_TRUE(STORAGE_FUNC_SUCC(activeTransaction->AllocTransactionSlot()));
    uint64 xid = activeTransaction->GetCurrentXid().m_placeHolder;
    ret_str = UndoMgrDiagnose::GetUndoSlotStatusByXid(m_pdbId, xid);
    EXPECT_NE(nullptr, strstr(ret_str, "Recycle csn is"));
}