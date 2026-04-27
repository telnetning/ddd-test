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
#ifndef UT_UNDO_MGR_DIAGNOSE_H
#define UT_UNDO_MGR_DIAGNOSE_H

#include <gtest/gtest.h>

#include "ut_utilities/ut_dstore_framework.h"
#include "ut_mock/ut_tablespace_mock.h"
#include "ut_mock/ut_buf_mgr_mock.h"
#include "ut_mock/ut_instance_mock.h"
#include "diagnose/dstore_undo_mgr_diagnose.h"

class UndoMgrDiagnoseTest : public DSTORETEST {
protected:
    void SetUp() override
    {
        DSTORETEST::SetUp();
        MockStorageInstance *instance = DstoreNew(m_ut_memory_context) MockStorageInstance();
        instance->Install(&DSTORETEST::m_guc, m_ut_memory_context);
        instance->Startup(&DSTORETEST::m_guc);

        m_bufferMgr = g_storageInstance->GetBufferMgr();
        m_tablespace = UtMockModule::UtGetTableSpace(UNDO_TABLE_SPACE_ID_UT, m_bufferMgr);
    }
    void TearDown() override
    {
        MockStorageInstance *instance = (MockStorageInstance *)g_storageInstance;
        instance->Shutdown();
        delete instance;
        DSTORETEST::TearDown();
    }

    TableSpaceInterface *m_tablespace;
    BufMgrInterface     *m_bufferMgr;
};


#endif //UT_UNDO_MGR_DIAGNOSE_H
