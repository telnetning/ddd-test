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
/*
 * Created by c00428156 on 2022/4/20.
 */

#include "ut_buffer/ut_buffer_fake_instance.h"
#include "ut_buffer/ut_buffer_util.h"
#include "ut_utilities/ut_dstore_framework.h"

class BufferRefcountTest : public DSTORETEST {
protected:
    void SetUp() override
    {
        DSTORETEST::SetUp();
        InstallDatabase(&DSTORETEST::m_guc, m_ut_memory_context);
        BufferMgrFakeStorageInstance *bufMgrInstance = DstoreNew(m_ut_memory_context)BufferMgrFakeStorageInstance();
        bufMgrInstance->Startup(&DSTORETEST::m_guc);
    }
    void TearDown() override
    {
        BufferMgrFakeStorageInstance *bufMgrInstance = (BufferMgrFakeStorageInstance *)g_storageInstance;
        bufMgrInstance->Shutdown();
        delete bufMgrInstance;
        DSTORETEST::TearDown();
    }
};

TEST_F(BufferRefcountTest, RefCountAddNotCreateTest)
{
    BufPrivateRefCount *private_ref_count = thrd->GetBufferPrivateRefCount();

    BufferDesc **buffers = UtBufferUtils::prepare_buffer(12);
    PrivateRefCountEntry *entry = private_ref_count->GetPrivateRefcount(buffers[0], false, true);
    ASSERT_EQ(entry, nullptr);

    UtBufferUtils::free_buffer(buffers, 12);
}

TEST_F(BufferRefcountTest, RefCountMoveAddTest_TIER1)
{
    BufPrivateRefCount *private_ref_count = thrd->GetBufferPrivateRefCount();

    BufferDesc **buffers = UtBufferUtils::prepare_buffer(24);
    PrivateRefCountEntry *entry = nullptr;

    for (uint32 c = 0; c < 10; c++) {
        for (uint32 i = 0; i < 24; i++) {
            entry = private_ref_count->GetPrivateRefcount(buffers[i], true, true);
            ASSERT_NE(entry, nullptr);
            ASSERT_EQ(entry->refcount, c);
            entry->refcount++;
        }
    }

    for (uint32 c = 0; c < 24; c++) {
        entry = private_ref_count->GetPrivateRefcount(buffers[c], true, true);
        entry->refcount = 0;
        private_ref_count->ForgetPrivateRefcountEntry(entry);
    }
    UtBufferUtils::free_buffer(buffers, 24);
}

TEST_F(BufferRefcountTest, RefCountNotMoveAddTest)
{
    BufPrivateRefCount *private_ref_count = thrd->GetBufferPrivateRefCount();

    BufferDesc **buffers = UtBufferUtils::prepare_buffer(24);
    PrivateRefCountEntry *entry = nullptr;

    for (uint32 c = 0; c < 10; c++) {
        for (uint32 i = 0; i < 24; i++) {
            entry = private_ref_count->GetPrivateRefcount(buffers[i], true, true);
            ASSERT_NE(entry, nullptr);
            ASSERT_EQ(entry->refcount, c);
            entry->refcount++;
        }
    }

    for (uint32 i = 0; i < 24; i++) {
        entry = private_ref_count->GetPrivateRefcount(buffers[i], false, false);
        ASSERT_NE(entry, nullptr);
        ASSERT_EQ(entry->refcount, 10);
    }

    for (uint32 c = 0; c < 24; c++) {
        entry = private_ref_count->GetPrivateRefcount(buffers[c], true, true);
        entry->refcount = 0;
        private_ref_count->ForgetPrivateRefcountEntry(entry);
    }

    UtBufferUtils::free_buffer(buffers, 24);
}

TEST_F(BufferRefcountTest, RefCountForgetTest_TIER1)
{
    BufPrivateRefCount *private_ref_count = thrd->GetBufferPrivateRefCount();

    BufferDesc **buffers = UtBufferUtils::prepare_buffer(24);
    PrivateRefCountEntry *entry = nullptr;

    for (uint32 c = 0; c < 10; c++) {
        for (uint32 i = 0; i < 24; i++) {
            entry = private_ref_count->GetPrivateRefcount(buffers[i]);
            ASSERT_NE(entry, nullptr);
            ASSERT_EQ(entry->refcount, c);
            entry->refcount++;
        }
    }

    for (uint32 i = 0; i < 24; i++) {
        entry = private_ref_count->GetPrivateRefcount(buffers[i], false, false);
        ASSERT_NE(entry, nullptr);
        ASSERT_EQ(entry->refcount, 10);
        entry->refcount = 0;
        private_ref_count->ForgetPrivateRefcountEntry(entry);
    }

    for (uint32 i = 0; i < 24; i++) {
        entry = private_ref_count->GetPrivateRefcount(buffers[i], false, false);
        ASSERT_EQ(entry, nullptr);
    }

    UtBufferUtils::free_buffer(buffers, 24);
}
