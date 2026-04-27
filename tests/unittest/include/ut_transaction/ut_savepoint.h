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
#ifndef UT_SAVEPOINT_H
#define UT_SAVEPOINT_H

#include <gtest/gtest.h>

#include "ut_utilities/ut_dstore_framework.h"
#include "framework/dstore_instance.h"
#include "ut_tablehandler/ut_table_handler.h"

class SavepointTest : public DSTORETEST {
protected:
    void SetUp() override
    {
        using namespace DSTORE;
        DSTORETEST::SetUp();
        UtInstance *instance = DstoreNew(m_ut_memory_context)UtInstance();
        instance->Install(&DSTORETEST::m_guc, m_ut_memory_context);
        instance->Startup(&DSTORETEST::m_guc);
        InitData();
    }

    void TearDown() override
    {
        DestroyData();
        using namespace DSTORE;
        UtInstance *instance = (UtInstance *)g_storageInstance;
        instance->Shutdown();
        delete instance;
        DSTORETEST::TearDown();
    }

protected:
    void InitData();
    void DestroyData();

    void CopyTuplesToMap(std::map<uint64, std::string> *map);
    void CheckTuplesAreTheSameWith(std::map<uint64, std::string> *compare);

    UTTableHandler *m_utTableHandler;
};

#endif
