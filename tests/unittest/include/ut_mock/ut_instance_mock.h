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
#ifndef DSTORE_UT_MOCK_INSTANCE_H
#define DSTORE_UT_MOCK_INSTANCE_H

#include "ut_utilities/ut_dstore_framework.h"
#include "framework/dstore_instance.h"
#include "framework/dstore_thread.h"
#include "ut_mock/ut_mock.h"

using namespace DSTORE;

class UTStorageInstance : public DSTORE::StorageInstance {
public:
    UTStorageInstance(ModuleId moduleId) : StorageInstance(), m_moudleId(moduleId){}

    ~UTStorageInstance() override = default;

    void Initialize(StorageGUC *guc, bool bootStrap = false) override;

private:
    RetStatus BufMgrInit() override;

private:
    ModuleId m_moudleId;
};

class MockStorageInstance : public UtInstance {
public:
    MockStorageInstance() : UtInstance(), m_tablespace(nullptr) {}

    ~MockStorageInstance() = default;

    TableSpaceInterface *get_ut_tablespace()
    {
        return m_tablespace;
    }

    void Startup(StorageGUC *guc) override
    {
        UtInstance::Startup(guc);
        ut_tablespace_init();
    }

    void Shutdown() override
    {
        UtInstance::Shutdown();
    }

protected:

    void ut_tablespace_init()
    {
        AutoMemCxtSwitch autoSwitch(m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LONGLIVE));
        m_tablespace = UtMockModule::UtGetTableSpace(DEFAULT_TABLE_SPACE_ID, GetBufferMgr());
    }

    void ut_tablespace_destroy()
    {
        UtMockModule::UtDropTableSpace(m_tablespace);
    }

    TableSpaceInterface *m_tablespace;
    static const int DEFAULT_TABLE_SPACE_ID = 2;
};

#endif //DSTORE_UT_MOCK_INSTANCE_H
