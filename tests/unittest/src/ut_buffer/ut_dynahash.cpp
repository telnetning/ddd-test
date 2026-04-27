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
#include "ut_utilities/ut_dstore_framework.h"
#include "common/algorithm/dstore_dynahash.h"
#include "ut_mock/ut_instance_mock.h"

namespace DSTORE {
class DynamicHashTest : public DSTORETEST {
protected:
    void SetUp() override
    {
        DSTORETEST::SetUp();
        InstallDatabase(&DSTORETEST::m_guc, m_ut_memory_context);
        UtInstance *instance = DstoreNew(m_ut_memory_context)UtInstance();
        instance->Startup(&DSTORETEST::m_guc);
    }
    void TearDown() override
    {
        UtInstance *instance = (UtInstance *)g_storageInstance;
        instance->Shutdown();
        delete instance;
        DSTORETEST::TearDown();
    }
};

/* Extendible hash */
TEST_F(DynamicHashTest, BasicTest)
{
    HASHCTL hashCtl;
    errno_t rc = memset_s(&hashCtl, sizeof(hashCtl), 0, sizeof(hashCtl));
    storage_securec_check(rc, "\0", "\0");
    hashCtl.keysize = sizeof(Oid);
    hashCtl.entrysize = sizeof(Oid);
    hashCtl.hash = oid_hash;
    static const int nElements = 512;
    HTAB *hTab = hash_create("test_tab", nElements, &hashCtl, HASH_ELEM | HASH_FUNCTION);

    /* insert key/value pairs more than initial elements number to test extensible hash. */
    for (int i = 0; i < nElements * 4; i++) {
        Oid key = i;
        Oid *value = (Oid *)hash_search(hTab, &key, HASH_ENTER, nullptr);
        *value = i;
    }

    for (int i = 0; i < nElements * 4; i++) {
        Oid key = i;
        bool found;
        Oid *value = (Oid *)hash_search(hTab, &key, HASH_FIND, &found);
        ASSERT_EQ(found, true);
        ASSERT_EQ(*value, i);
    }

    hash_destroy(hTab);
}
}
