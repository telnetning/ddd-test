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
#ifndef UT_FI_BTREE_H
#define UT_FI_BTREE_H
#include <gtest/gtest.h>
#include "ut_btree/ut_btree.h"
#include "common/fault_injection/dstore_index_fault_injection.h"

using namespace DSTORE;

class UTFiBtree : virtual public UTBtree
{
public:
   void SetUp() override
    {
        DSTORETEST::SetUp();
        MockStorageInstance *instance = DstoreNew(m_ut_memory_context) MockStorageInstance();
        instance->Install(&DSTORETEST::m_guc, m_ut_memory_context);
        instance->Startup(&DSTORETEST::m_guc);
        m_utTableHandler = UTTableHandler::CreateTableHandler(g_defaultPdbId, m_ut_memory_context, false);
#ifdef ENABLE_FAULT_INJECTION
    FaultInjectionEntry entries[] = {
        FAULT_INJECTION_ENTRY(DstoreIndexFI::FORCE_SPLIT, false, nullptr),
        FAULT_INJECTION_ENTRY(DstoreIndexFI::SET_SPLIT_POINT, false, SetUtSplitContext),
        FAULT_INJECTION_ENTRY(DstoreIndexFI::STOP_RETRY_SEARCH_BTREE, false, nullptr),
    };
    ASSERT_EQ(RegisterFaultInjection(entries, sizeof(entries) / sizeof(entries[0]), FI_GLOBAL), ERROR_SYS_OK);

#endif
    }

    void TearDown() override
    {
        UTTableHandler::Destroy(m_utTableHandler);
        m_utTableHandler = nullptr;
        MockStorageInstance *instance = (MockStorageInstance *)g_storageInstance;
        instance->Shutdown();
        delete instance;
        DSTORETEST::TearDown();
    }
};

#endif