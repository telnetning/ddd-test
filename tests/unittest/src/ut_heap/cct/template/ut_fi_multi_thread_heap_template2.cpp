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
#include <gtest/gtest.h>
#include "lock/dstore_xact_lock_mgr.h"
#include "tuple/dstore_data_tuple.h"
#include "undo/dstore_undo_zone.h"
#include "ut_mock/ut_mock.h"
#include "common/fault_injection/undo_fault_injection.h"
#include "heap/dstore_heap_wal_struct.h"
#include "page/dstore_heap_page.h"
#include "ut_wal/ut_wal_basic.h"
#include "ut_heap/ut_heap.h"
#include "ut_heap/ut_heap_wal.h"
#include "ut_heap/ut_heap_multi_thread.h"
 

/**
 * This case is same with UTHeapMultiThread.UndoZoneAsynRollbackTest (Page rollback case)
* CCT::thread-1: {t1:f1, t2:f2, t3:f3, t4:f4, t5:ni}
* CCT::thread-2: {t1:f1, t2:f2, t3:f3, t4:ni, t5:f4}
* CCT::thread-3: {t1:f1, t2:f2, t3:ni, t4:f3, t5:f4}
* CCT::thread-4: {t1:f1, t2:ni, t3:f2, t4:f3, t5:f4}
* CCT::thread-5: {t1:ni, t2:f1, t3:f2, t4:f3, t5:f4}
*/

const ZoneId zoneId = 2;

TEST_F(UTHeapMultiThread, UndoZoneAsynRollbackTest2_level0)
{
    FAULT_INJECTION_ACTIVE_MODE_LEVEL(DstoreUndoFI::GET_UNDO_ZONE, 0, FI_GLOBAL, 0, 1);
    FAULT_INJECTION_ACTIVE_MODE_LEVEL(DstoreUndoFI::RECOVER_UNDO_ZONE, 0, FI_GLOBAL, 0, 1);

    
    UndoMgr *undoMgr = g_storageInstance->GetPdb(g_defaultPdbId)->GetUndoMgr();
    UndoZone *undoZone = nullptr;
    PageId pageId;
    
    // CCT::b_f1
    undoMgr->GetUndoZone(zoneId, &undoZone, true);
    // CCT::e_f1

    // CCT::b_f2
    m_undozones.push(undoZone);
    // CCT::e_f2

    // CCT::b_f3
    undoMgr->LoadUndoZone(zoneId, pageId);
    UndoZone* newUndoZone = nullptr;
    undoMgr->GetUndoZone(zoneId, &newUndoZone);
    UndoZone* undoZone1 = m_undozones.front(); 
    ASSERT_EQ(undoZone1, newUndoZone);    
    // CCT::e_f3

    // CCT::b_f4
    undoMgr->LoadUndoZone(zoneId, pageId);
    UndoZone* newUndoZone2 = nullptr;
    undoMgr->GetUndoZone(zoneId, &newUndoZone2);
    UndoZone* undoZone2 = m_undozones.front(); 
    ASSERT_EQ(undoZone2, newUndoZone2);    
    // CCT::e_f4

    FAULT_INJECTION_INACTIVE(DstoreUndoFI::GET_UNDO_ZONE, FI_GLOBAL);
    FAULT_INJECTION_INACTIVE(DstoreUndoFI::RECOVER_UNDO_ZONE, FI_GLOBAL);
}
