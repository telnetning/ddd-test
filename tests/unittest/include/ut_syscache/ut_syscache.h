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
#ifndef DSTORE_UT_SYSCACHE_H
#define DSTORE_UT_SYSCACHE_H

#include "ut_tablehandler/ut_table_handler.h"
#include "syscache/dstore_syscache_interface.h"

using namespace DSTORE;

class SysCacheTest : virtual public DSTORETEST {
public:

protected:
   void SetUp() override
    {
        DSTORETEST::SetUp();
        MockStorageInstance *instance = DstoreNew(m_ut_memory_context) MockStorageInstance();
        instance->Install(&DSTORETEST::m_guc, m_ut_memory_context);
        instance->Startup(&DSTORETEST::m_guc);
        m_instance = instance;
        m_utTableHandler = UTTableHandler::CreateTableHandler(g_defaultPdbId, m_ut_memory_context);
    }

    void TearDown() override
    {
        UTTableHandler::Destroy(m_utTableHandler);
        m_utTableHandler = nullptr;
        MockStorageInstance *instance = (MockStorageInstance *)g_storageInstance;
        instance->Shutdown();
        delete instance;
        m_instance = nullptr;
        DSTORETEST::TearDown();
    }

    const char tableName[20] = "SyscacheTestTable_1";
    const char indexName[20] = "SysCacheTestIndex_1";
    MockStorageInstance *m_instance;
    UTTableHandler *m_utTableHandler;

};

#endif
