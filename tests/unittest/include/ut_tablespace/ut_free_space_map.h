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
#ifndef DSTORE_UT_FREE_SPACE_MAP_H
#define DSTORE_UT_FREE_SPACE_MAP_H

#include "ut_tablespace/ut_data_segment.h"

using namespace DSTORE;

class FreeSpaceMapTest : public DataSegmentTest  {
protected:
    void SetUp() override
    {
        int buffer = DSTORETEST::m_guc.buffer;
        DSTORETEST::m_guc.buffer = 81920;
        DSTORETEST::SetUp();
        UtInstance *instance = DstoreNew(m_ut_memory_context)UtInstance();
        /**
        * update the timestamp of FreeSpaceMapMetaPage everytime accessing it by setting
        * probOfUpdateFsmTimestamp = 100
        */
        DSTORETEST::m_guc.probOfUpdateFsmTimestamp = 100;
        instance->Install(&DSTORETEST::m_guc, m_ut_memory_context);
        instance->Startup(&DSTORETEST::m_guc);
        DSTORETEST::m_guc.buffer = buffer;
        m_instance = g_storageInstance;
        m_testBufMgr = m_instance->GetBufferMgr();
        TablespaceMgr *tablespaceMgr = g_storageInstance->GetPdb(g_defaultPdbId)->GetTablespaceMgr();
        m_testTbs = (TableSpaceInterface *)tablespaceMgr->OpenTablespace(2, DSTORE::DSTORE_NO_LOCK);
    }
    void TearDown() override
    {
        TablespaceMgr *tablespaceMgr = g_storageInstance->GetPdb(g_defaultPdbId)->GetTablespaceMgr();
        tablespaceMgr->CloseTablespace((TableSpace *)m_testTbs, DSTORE::DSTORE_NO_LOCK);
        UtInstance *instance = (UtInstance *)g_storageInstance;
        instance->Shutdown();
        delete instance;
        DSTORETEST::TearDown();
    }
};
#endif //DSTORE_UT_FREE_SPACE_MAP_H
