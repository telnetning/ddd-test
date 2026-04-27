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
#include <gtest/gtest.h>
#include "securec.h"
#include "ut_buffer/ut_buffer_util.h"
#include "ut_buffer/ut_buffer_fake_instance.h"
#include "buffer/dstore_buf_mgr.h"
#include "ut_mock/ut_buf_mgr_mock.h"
#include <random>
#include <chrono>
namespace DSTORE {

class MockBufferTest : public DSTORETEST {
protected:
    MockBufMgr *buf_mgr;
    void SetUp() override
    {
        DSTORETEST::SetUp();
        InstallDatabase(&DSTORETEST::m_guc, m_ut_memory_context);
        BufferMgrFakeStorageInstance *bufMgrInstance = DstoreNew(m_ut_memory_context)BufferMgrFakeStorageInstance();
        bufMgrInstance->Startup(&DSTORETEST::m_guc);
        buf_mgr = create_mock_buf_mgr(10);
    }
    void TearDown() override
    {
        delete buf_mgr;
        BufferMgrFakeStorageInstance *bufMgrInstance = (BufferMgrFakeStorageInstance *)g_storageInstance;
        bufMgrInstance->Shutdown();
        delete bufMgrInstance;
        DSTORETEST::TearDown();
    }

public:
    static constexpr CommitSeqNo MAX_CSN = 1000;
    MockBufMgr *create_mock_buf_mgr(int size)
    {
        DstoreMemoryContext buffer_context = g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_BUFFER);
        MockBufMgr *buf = DstoreNew(buffer_context) MockBufMgr{size};
        return buf;
    }
};

void prepare_base_page(MockBufMgr *buf_mgr, PageId &page_id, const char *content)
{
    BufferDesc *buffer = buf_mgr->Read(g_defaultPdbId, page_id, LW_EXCLUSIVE);

    ASSERT_NE(buffer, INVALID_BUFFER_DESC);
    ASSERT_TRUE(LWLockHeldByMeInMode(&buffer->contentLwLock, LW_EXCLUSIVE));
    ASSERT_EQ(buffer->GetRefcount(), 1U);

    Page *block = buffer->GetPage();
    errno_t rc = memcpy_s(block, BLCKSZ, content, strlen(content));
    storage_securec_check(rc, "\0", "\0");
    buf_mgr->MarkDirty(buffer);
    buf_mgr->UnlockContent(buffer);

    ASSERT_TRUE(!LWLockHeldByMeInMode(&buffer->contentLwLock, LW_EXCLUSIVE));

    buf_mgr->Release(buffer);

    ASSERT_EQ(buffer->GetRefcount(), 0U);
}

TEST_F(MockBufferTest, ReadExclusive)
{
    PageId page_id = {1, 1};
    BufferDesc *buffer = buf_mgr->Read(g_defaultPdbId, page_id, LW_EXCLUSIVE);

    ASSERT_NE(buffer, INVALID_BUFFER_DESC);
    ASSERT_TRUE(LWLockHeldByMeInMode(&buffer->contentLwLock, LW_EXCLUSIVE));
    ASSERT_EQ(buffer->GetRefcount(), 1U);

    Page *block = buffer->GetPage();
    errno_t rc = memcpy_s(block, BLCKSZ, "HelloWorld!", strlen("HelloWorld!"));
    storage_securec_check(rc, "\0", "\0");

    buf_mgr->MarkDirty(buffer);
    buf_mgr->UnlockContent(buffer);

    ASSERT_TRUE(!LWLockHeldByMeInMode(&buffer->contentLwLock, LW_EXCLUSIVE));

    buf_mgr->Release(buffer);

    ASSERT_EQ(buffer->GetRefcount(), 0U);

    buffer = buf_mgr->Read(g_defaultPdbId, page_id, LW_EXCLUSIVE);
    ASSERT_NE(buffer, INVALID_BUFFER_DESC);

    block = buffer->GetPage();
    ASSERT_EQ(0, memcmp(block, "HelloWorld!", strlen("HelloWorld!")));
    buf_mgr->MarkDirty(buffer);
    buf_mgr->UnlockAndRelease(buffer);
}

TEST_F(MockBufferTest, ReadShared)
{
    PageId page_id = {1, 1};
    BufferDesc *buffer = buf_mgr->Read(g_defaultPdbId, page_id, LW_EXCLUSIVE);

    ASSERT_NE(buffer, INVALID_BUFFER_DESC);
    ASSERT_TRUE(LWLockHeldByMeInMode(&buffer->contentLwLock, LW_EXCLUSIVE));
    ASSERT_EQ(buffer->GetRefcount(), 1U);

    Page *block = buffer->GetPage();
    errno_t rc = memcpy_s(block, BLCKSZ, "HelloWorld!", strlen("HelloWorld!"));
    storage_securec_check(rc, "\0", "\0");
    buf_mgr->MarkDirty(buffer);
    buf_mgr->UnlockAndRelease(buffer);

    ASSERT_TRUE(!LWLockHeldByMeInMode(&buffer->contentLwLock, LW_EXCLUSIVE));
    ASSERT_EQ(buffer->GetRefcount(), 0U);

    BufferDesc *buffer1 = buf_mgr->Read(g_defaultPdbId, page_id, LW_SHARED);
    ASSERT_NE(buffer1, INVALID_BUFFER_DESC);
    ASSERT_TRUE(LWLockHeldByMeInMode(&buffer1->contentLwLock, LW_SHARED));
    ASSERT_EQ(buffer1->GetRefcount(), 1U);

    block = buffer1->GetPage();
    ASSERT_EQ(0, memcmp(block, "HelloWorld!", strlen("HelloWorld!")));

    BufferDesc *buffer2 = buf_mgr->Read(g_defaultPdbId, page_id, LW_SHARED);
    ASSERT_NE(buffer2, INVALID_BUFFER_DESC);
    ASSERT_TRUE(LWLockHeldByMeInMode(&buffer1->contentLwLock, LW_SHARED));
    ASSERT_EQ(buffer1->GetRefcount(), 1U);
    PrivateRefCountEntry *ref_entry = thrd->GetBufferPrivateRefCount()->GetPrivateRefcount(buffer1);
    ASSERT_EQ(ref_entry->buffer, buffer1);
    ASSERT_EQ(ref_entry->refcount, 2U);

    block = buffer2->GetPage();
    ASSERT_EQ(0, memcmp(block, "HelloWorld!", strlen("HelloWorld!")));

    buf_mgr->UnlockContent(buffer1);
    buf_mgr->UnlockContent(buffer2);

    ASSERT_TRUE(!LWLockHeldByMeInMode(&buffer->contentLwLock, LW_SHARED));

    buf_mgr->Release(buffer1);
    ASSERT_EQ(buffer1->GetRefcount(), 1U);

    buf_mgr->Release(buffer2);
    ASSERT_EQ(buffer2->GetRefcount(), 0U);
}

}  // namespace DSTORE
