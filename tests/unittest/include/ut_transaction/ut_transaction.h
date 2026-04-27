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
#ifndef DSTORE_UT_TRANSACTION_H
#define DSTORE_UT_TRANSACTION_H

#include <gtest/gtest.h>
#include "ut_utilities/ut_dstore_framework.h"
#include "ut_tablehandler/ut_table_handler.h"
#include "framework/dstore_instance.h"

class UTTransactionTest : public DSTORETEST {
protected:
    void SetUp() override
    {
        using namespace DSTORE;
        DSTORETEST::SetUp();
        UtInstance *instance = DstoreNew(m_ut_memory_context)UtInstance();
        instance->Install(&DSTORETEST::m_guc, m_ut_memory_context);
        instance->Startup(&DSTORETEST::m_guc);
    }

    void TearDown() override
    {
        UTTableHandler::Destroy(m_utTableHandler);
        m_utTableHandler = nullptr;
        using namespace DSTORE;
        UtInstance *instance = (UtInstance *)g_storageInstance;
        instance->Shutdown();
        delete instance;
        DSTORETEST::TearDown();
    }
    
    void SetupTableHandler()
    {
        m_utTableHandler = UTTableHandler::CreateTableHandler(g_defaultPdbId, m_ut_memory_context);
    }

    HeapTuple *InsertHeapTuple(const std::string &rawData)
    {
        HeapTuple *tuple = m_utTableHandler->GenerateSpecificHeapTuple(rawData);
        ItemPointerData ctid = m_utTableHandler->InsertHeapTupAndCheckResult(
                                                    tuple,
                                                    true,
                                                    INVALID_SNAPSHOT);
        EXPECT_NE(ctid, INVALID_ITEM_POINTER);
        return tuple;
    }

    UTTableHandler *m_utTableHandler = nullptr;
};

#endif
