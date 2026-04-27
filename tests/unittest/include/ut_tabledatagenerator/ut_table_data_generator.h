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
#ifndef DSTORE_UT_TABLE_DATA_GENERATOR_H
#define DSTORE_UT_TABLE_DATA_GENERATOR_H

#include "ut_utilities/ut_dstore_framework.h"
#include "table_data_generator.h"
#include "ut_mock/ut_instance_mock.h"

using namespace DSTORE;

class UTTableDataGenerator : public DSTORETEST {
protected:
    void SetUp() override
    {
        DSTORETEST::SetUp();
        MockStorageInstance *instance = DstoreNew(m_ut_memory_context) MockStorageInstance();
        instance->Install(&DSTORETEST::m_guc, m_ut_memory_context);
        instance->Startup(&DSTORETEST::m_guc);
        m_instance = g_storageInstance;
        m_thrd = thrd;
    }

    void TearDown() override
    {
        MockStorageInstance *instance = (MockStorageInstance *)g_storageInstance;
        instance->Shutdown();
        delete instance;
        m_instance = nullptr;
        DSTORETEST::TearDown();
    }

    StorageInstance *m_instance;
    ThreadContext *m_thrd;
};
#endif
