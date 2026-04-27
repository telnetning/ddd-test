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
#ifndef DSTORE_UT_OBJSPACE_MGR_H
#define DSTORE_UT_OBJSPACE_MGR_H

#include "ut_tablespace/ut_data_segment.h"
#include "ut_btree/ut_btree.h"

using namespace DSTORE;

class ObjSpaceMgrTest : public DataSegmentTest, public UTBtree {
protected:
    void SetUp() override
    {
        DSTORETEST::SetUp();
        UtInstance *instance = DstoreNew(m_ut_memory_context)UtInstance();
        instance->Install(&DSTORETEST::m_guc, m_ut_memory_context);
        instance->Startup(&DSTORETEST::m_guc);
        m_instance = g_storageInstance;
        m_testBufMgr = m_instance->GetBufferMgr();
        m_testTbsId = 2;
        TablespaceMgr *tablespaceMgr = g_storageInstance->GetPdb(g_defaultPdbId)->GetTablespaceMgr();
        m_testTbs = (TableSpaceInterface *)tablespaceMgr->OpenTablespace(m_testTbsId, DSTORE::DSTORE_NO_LOCK);
        m_objSpaceMgr = g_storageInstance->GetPdb(g_defaultPdbId)->GetObjSpaceMgr();
        UTBtree::m_utTableHandler = UTTableHandler::CreateTableHandler(g_defaultPdbId, m_ut_memory_context, false);
    }
    void TearDown() override
    {
        UTTableHandler::Destroy(UTBtree::m_utTableHandler);
        UTBtree::m_utTableHandler = nullptr;
        TablespaceMgr *tablespaceMgr = g_storageInstance->GetPdb(g_defaultPdbId)->GetTablespaceMgr();
        tablespaceMgr->CloseTablespace((TableSpace *)m_testTbs, DSTORE::DSTORE_NO_LOCK);
        UtMockModule::UtDropTableSpace(m_testTbs);
        UtInstance *instance = (UtInstance *)g_storageInstance;
        instance->Shutdown();
        delete instance;
        DSTORETEST::TearDown();
    }

    ObjSpaceMgr *m_objSpaceMgr;
    TablespaceId m_testTbsId;
};

#endif
