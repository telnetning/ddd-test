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
#include <random>
#include "securec.h"
#include "ut_buffer/ut_buffer.h"
#include "ut_buffer/ut_buffer_util.h"

TEST_F(BufferTest, LRUTest)
{
    BufMgr *buffer_pool = DstoreNew (BufferTest::m_ut_memory_context)BufMgr(10, 1);
    buffer_pool->Init();

    int i = 0;
    PageId page_id;
    for (i = 0; i < 15; i++) {
        page_id = {FAKE_FILES[0].file_id, (BlockNumber)i};
        BufferDesc* buffer = buffer_pool->Read(g_defaultPdbId, page_id, LW_EXCLUSIVE);

        ASSERT_TRUE(check_buffer(buffer, page_id));
        ASSERT_TRUE(check_buffer_state(buffer, 1U,
                                    (Buffer::BUF_VALID | Buffer::BUF_TAG_VALID),
                                    UNSET_FLAGS | (Buffer::BUF_CONTENT_DIRTY | Buffer::BUF_HINT_DIRTY | Buffer::BUF_CR_PAGE)));
        ASSERT_TRUE(LWLockHeldByMeInMode(&buffer->contentLwLock, LW_EXCLUSIVE));
        ASSERT_FALSE(LWLockHeldByMe(buffer->controller->GetIoInProgressLwLock()));
        ASSERT_FALSE(LWLockHeldByMe(buffer->controller->GetCrAssignLwLock()));
        ASSERT_TRUE(check_buffer_lru(buffer, DSTORE::LN_LRU, 1U));
        buffer->GetPage()->Init(0, PageType::HEAP_PAGE_TYPE, page_id);
        errno_t rc = memcpy_s(reinterpret_cast<char *>(buffer->GetPage()) + sizeof(Page::m_header),
            BLCKSZ - sizeof(Page::m_header), "Hello", strlen("Hello"));
        storage_securec_check(rc, "\0", "\0");
        buffer_pool->MarkDirty(buffer);
        buffer_pool->UnlockContent(buffer);
        buffer_pool->Release(buffer);
        if (i == 0) {
            buffer->bufTag.pageId = INVALID_PAGE_ID;
        }

        uint64 set_flags = (Buffer::BUF_VALID | Buffer::BUF_TAG_VALID | Buffer::BUF_CONTENT_DIRTY | Buffer::BUF_HINT_DIRTY);
        ASSERT_TRUE(check_buffer_state(buffer, 0U, set_flags, UNSET_FLAGS | Buffer::BUF_CR_PAGE));
        ASSERT_FALSE(LWLockHeldByMe(&buffer->contentLwLock));
        ASSERT_FALSE(LWLockHeldByMe(buffer->controller->GetIoInProgressLwLock()));
        ASSERT_FALSE(LWLockHeldByMe(buffer->controller->GetCrAssignLwLock()));
    }
    buffer_pool->Destroy();
    delete buffer_pool;
}

TEST_F(BufferTest, ReadBufferInExclusiveModeTest)
{
    BufMgr *buffer_pool = DstoreNew (BufferTest::m_ut_memory_context)BufMgr(1000, 4);
    buffer_pool->Init();

    PageId page_id = {FAKE_FILES[0].file_id, 0};
    BufferDesc* buffer = buffer_pool->Read(g_defaultPdbId, page_id, LW_EXCLUSIVE);

    ASSERT_TRUE(check_buffer(buffer, page_id));
    ASSERT_TRUE(check_buffer_state(buffer, 1U,
                                   (Buffer::BUF_VALID | Buffer::BUF_TAG_VALID),
                                   UNSET_FLAGS | (Buffer::BUF_CONTENT_DIRTY | Buffer::BUF_HINT_DIRTY | Buffer::BUF_CR_PAGE)));
    ASSERT_TRUE(LWLockHeldByMeInMode(&buffer->contentLwLock, LW_EXCLUSIVE));
    ASSERT_FALSE(LWLockHeldByMe(buffer->controller->GetIoInProgressLwLock()));
    ASSERT_FALSE(LWLockHeldByMe(buffer->controller->GetCrAssignLwLock()));
    ASSERT_TRUE(check_buffer_lru(buffer, DSTORE::LN_LRU, 1U));
    buffer->GetPage()->Init(0, PageType::HEAP_PAGE_TYPE, page_id);
    errno_t rc = memcpy_s(reinterpret_cast<char *>(buffer->GetPage()) + sizeof(Page::m_header),
        BLCKSZ - sizeof(Page::m_header), "Hello", strlen("Hello"));
    storage_securec_check(rc, "\0", "\0");

    buffer_pool->MarkDirty(buffer);
    buffer_pool->UnlockContent(buffer);
    buffer_pool->Release(buffer);

    uint64 set_flags = (Buffer::BUF_VALID | Buffer::BUF_TAG_VALID | Buffer::BUF_CONTENT_DIRTY | Buffer::BUF_HINT_DIRTY);
    ASSERT_TRUE(check_buffer_state(buffer, 0U, set_flags, UNSET_FLAGS | Buffer::BUF_CR_PAGE));
    ASSERT_FALSE(LWLockHeldByMe(&buffer->contentLwLock));
    ASSERT_FALSE(LWLockHeldByMe(buffer->controller->GetIoInProgressLwLock()));
    ASSERT_FALSE(LWLockHeldByMe(buffer->controller->GetCrAssignLwLock()));
    buffer_pool->Destroy();
    delete buffer_pool;
}

TEST_F(BufferTest, ReadBufferInSharedModeTest)
{
    BufMgr *buffer_pool = DstoreNew (BufferTest::m_ut_memory_context)BufMgr(1000, 4);
    buffer_pool->Init();

    PageId page_id = {FAKE_FILES[0].file_id, 0};
    BufferDesc* buffer = buffer_pool->Read(g_defaultPdbId, page_id, LW_EXCLUSIVE);

    ASSERT_TRUE(check_buffer(buffer, page_id));
    ASSERT_TRUE(check_buffer_state(buffer, 1U,
                                   (Buffer::BUF_VALID | Buffer::BUF_TAG_VALID),
                                   UNSET_FLAGS | (Buffer::BUF_CONTENT_DIRTY | Buffer::BUF_HINT_DIRTY | Buffer::BUF_CR_PAGE)));
    ASSERT_TRUE(LWLockHeldByMeInMode(&buffer->contentLwLock, LW_EXCLUSIVE));
    ASSERT_FALSE(LWLockHeldByMe(buffer->controller->GetIoInProgressLwLock()));
    ASSERT_FALSE(LWLockHeldByMe(buffer->controller->GetCrAssignLwLock()));
    ASSERT_TRUE(check_buffer_lru(buffer, DSTORE::LN_LRU, 1U));
    buffer->GetPage()->Init(0, DSTORE::PageType::HEAP_PAGE_TYPE, page_id);
    errno_t rc = memcpy_s(reinterpret_cast<char *>(buffer->GetPage()) + sizeof(Page::m_header),
        BLCKSZ - sizeof(Page::m_header), "Hello", strlen("Hello"));
    storage_securec_check(rc, "\0", "\0");

    buffer_pool->MarkDirty(buffer);
    buffer_pool->UnlockContent(buffer);
    buffer_pool->Release(buffer);

    uint64 set_flags = (Buffer::BUF_VALID | Buffer::BUF_TAG_VALID | Buffer::BUF_CONTENT_DIRTY | Buffer::BUF_HINT_DIRTY);
    ASSERT_TRUE(check_buffer_state(buffer, 0U, set_flags, UNSET_FLAGS | Buffer::BUF_CR_PAGE));
    ASSERT_FALSE(LWLockHeldByMe(&buffer->contentLwLock));
    ASSERT_FALSE(LWLockHeldByMe(buffer->controller->GetIoInProgressLwLock()));
    ASSERT_FALSE(LWLockHeldByMe(buffer->controller->GetCrAssignLwLock()));

    buffer = buffer_pool->Read(g_defaultPdbId, page_id, LW_SHARED);

    ASSERT_TRUE(check_buffer(buffer, page_id));
    ASSERT_TRUE(check_buffer_state(buffer, 1U, set_flags, UNSET_FLAGS | Buffer::BUF_CR_PAGE));
    ASSERT_TRUE(LWLockHeldByMeInMode(&buffer->contentLwLock, LW_SHARED));
    ASSERT_FALSE(LWLockHeldByMe(buffer->controller->GetIoInProgressLwLock()));
    ASSERT_FALSE(LWLockHeldByMe(buffer->controller->GetCrAssignLwLock()));
    ASSERT_TRUE(check_buffer_lru(buffer, DSTORE::LN_LRU, 2U));

    ASSERT_EQ(memcmp(reinterpret_cast<char *>(buffer->GetPage()) + sizeof(Page::m_header), "Hello", strlen("Hello")), 0);

    buffer_pool->UnlockContent(buffer);
    buffer_pool->Release(buffer);

    ASSERT_TRUE(check_buffer_state(buffer, 0U, set_flags, UNSET_FLAGS | Buffer::BUF_CR_PAGE));
    ASSERT_FALSE(LWLockHeldByMe(&buffer->contentLwLock));
    ASSERT_FALSE(LWLockHeldByMe(buffer->controller->GetIoInProgressLwLock()));
    ASSERT_FALSE(LWLockHeldByMe(buffer->controller->GetCrAssignLwLock()));

    buffer_pool->Destroy();
    delete buffer_pool;
}

TEST_F(BufferTest, ReadBufferInLruListTest_TIER1)
{
    BufMgr *buffer_pool = DstoreNew (BufferTest::m_ut_memory_context)BufMgr(1000, 4);
    buffer_pool->Init();

    PageId page_id = {FAKE_FILES[0].file_id, 0};
    load_page_into_buffer(buffer_pool, page_id, (char*)("HelloWorld"));

    for (uint32 i = 1; i < 4; i++) {
        BufferDesc *buffer = buffer_pool->Read(g_defaultPdbId, page_id, LW_SHARED);

        ASSERT_TRUE(check_buffer(buffer, page_id));
        ASSERT_TRUE(check_buffer_state(buffer, 1U,
                                       (Buffer::BUF_VALID | Buffer::BUF_TAG_VALID),
                                       UNSET_FLAGS | Buffer::BUF_CR_PAGE));

        ASSERT_TRUE(LWLockHeldByMeInMode(&buffer->contentLwLock, LW_SHARED));
        ASSERT_FALSE(LWLockHeldByMe(buffer->controller->GetIoInProgressLwLock()));
        ASSERT_FALSE(LWLockHeldByMe(buffer->controller->GetCrAssignLwLock()));
        ASSERT_TRUE(check_buffer_lru(buffer, DSTORE::LN_LRU, 1 + i));

        ASSERT_EQ(memcmp(reinterpret_cast<char *>(buffer->GetPage()) + sizeof(Page::m_header), "HelloWorld",
                         strlen("HelloWorld")), 0);

        buffer_pool->UnlockContent(buffer);
        buffer_pool->Release(buffer);
        ASSERT_TRUE(check_buffer_state(buffer, 0U,
                                       (Buffer::BUF_VALID | Buffer::BUF_TAG_VALID),
                                       UNSET_FLAGS | Buffer::BUF_CR_PAGE));
        ASSERT_FALSE(LWLockHeldByMe(&buffer->contentLwLock));
        ASSERT_FALSE(LWLockHeldByMe(buffer->controller->GetIoInProgressLwLock()));
        ASSERT_FALSE(LWLockHeldByMe(buffer->controller->GetCrAssignLwLock()));
    }
    buffer_pool->Destroy();
    delete buffer_pool;
}

TEST_F(BufferTest, ReadBufferInHotListTest_TIER1)
{
    BufMgr *buffer_pool = DstoreNew (BufferTest::m_ut_memory_context)BufMgr(1000, 4);
    buffer_pool->Init();

    PageId page_id = {FAKE_FILES[0].file_id, 0};
    load_page_into_buffer(buffer_pool, page_id, (char*)("HelloWorld"));

    for (uint32 i = 1; i < 4; i++) {
        BufferDesc *buffer = buffer_pool->Read(g_defaultPdbId, page_id, LW_SHARED);

        ASSERT_TRUE(check_buffer(buffer, page_id));
        ASSERT_TRUE(check_buffer_state(buffer, 1U,
                                       (Buffer::BUF_VALID | Buffer::BUF_TAG_VALID),
                                       UNSET_FLAGS | Buffer::BUF_CR_PAGE));

        ASSERT_TRUE(LWLockHeldByMeInMode(&buffer->contentLwLock, LW_SHARED));
        ASSERT_FALSE(LWLockHeldByMe(buffer->controller->GetIoInProgressLwLock()));
        ASSERT_TRUE(check_buffer_lru(buffer, DSTORE::LN_LRU, 1 + i));

        ASSERT_EQ(memcmp(reinterpret_cast<char *>(buffer->GetPage()) + sizeof(Page::m_header), "HelloWorld",
                         strlen("HelloWorld")), 0);

        buffer_pool->UnlockContent(buffer);
        buffer_pool->Release(buffer);
        ASSERT_TRUE(check_buffer_state(buffer, 0U,
                                       (Buffer::BUF_VALID | Buffer::BUF_TAG_VALID),
                                       UNSET_FLAGS | Buffer::BUF_CR_PAGE));
        ASSERT_FALSE(LWLockHeldByMe(&buffer->contentLwLock));
        ASSERT_FALSE(LWLockHeldByMe(buffer->controller->GetIoInProgressLwLock()));
    }

    for (uint32 i = 1; i < 4; i++) {
        BufferDesc *buffer = buffer_pool->Read(g_defaultPdbId, page_id, LW_SHARED);

        ASSERT_TRUE(check_buffer(buffer, page_id));
        ASSERT_TRUE(check_buffer_state(buffer, 1U,
                                       (Buffer::BUF_VALID | Buffer::BUF_TAG_VALID),
                                       UNSET_FLAGS | Buffer::BUF_CR_PAGE));

        ASSERT_TRUE(LWLockHeldByMeInMode(&buffer->contentLwLock, LW_SHARED));
        ASSERT_FALSE(LWLockHeldByMe(buffer->controller->GetIoInProgressLwLock()));
        ASSERT_TRUE(check_buffer_lru(buffer, DSTORE::LN_HOT, 0));

        ASSERT_EQ(memcmp(reinterpret_cast<char *>(buffer->GetPage()) + sizeof(Page::m_header), "HelloWorld",
                         strlen("HelloWorld")), 0);

        buffer_pool->UnlockContent(buffer);
        buffer_pool->Release(buffer);
        ASSERT_TRUE(check_buffer_state(buffer, 0U,
                                       (Buffer::BUF_VALID | Buffer::BUF_TAG_VALID),
                                       UNSET_FLAGS | Buffer::BUF_CR_PAGE));
        ASSERT_FALSE(LWLockHeldByMe(&buffer->contentLwLock));
        ASSERT_FALSE(LWLockHeldByMe(buffer->controller->GetIoInProgressLwLock()));
    }

    buffer_pool->Destroy();
    delete buffer_pool;
}

TEST_F(BufferTest, ReadBufferEvictHotBufferBackToLruListTest_TIER1)
{
    Size buf_size = 1000;
    Size hot_buf_size = BUFLRU_DEFAULT_HOT_RATIO * 1000;

    BufMgr *buffer_pool = DstoreNew (BufferTest::m_ut_memory_context)BufMgr(buf_size, 1);
    buffer_pool->Init();

    for (BlockNumber block_num = 0; block_num < FAKE_FILES[0].max_block; block_num++) {
        PageId page_id = {FAKE_FILES[0].file_id, block_num};
        load_page_into_buffer(buffer_pool, page_id, (char*)("HelloWorld"));
    }


    for (BlockNumber block_num = 0; block_num < hot_buf_size; block_num++) {
        PageId page_id = {FAKE_FILES[0].file_id, block_num};
        access_buffer_to_make_it_hot(buffer_pool, page_id);
    }

    PageId page_id = {FAKE_FILES[0].file_id, (BlockNumber)hot_buf_size};
    access_buffer_to_make_it_hot(buffer_pool, page_id);

    /* the first buffer is back to lru */
    page_id = {FAKE_FILES[0].file_id, 0};
    BufferDesc* buffer = buffer_pool->Read(g_defaultPdbId, page_id, LW_SHARED);
    ASSERT_TRUE(check_buffer(buffer, page_id));
    ASSERT_TRUE(check_buffer_state(buffer, 1U,
                                   (Buffer::BUF_VALID | Buffer::BUF_TAG_VALID),
                                   UNSET_FLAGS | Buffer::BUF_CR_PAGE));

    ASSERT_TRUE(LWLockHeldByMeInMode(&buffer->contentLwLock, LW_SHARED));
    ASSERT_FALSE(LWLockHeldByMe(buffer->controller->GetIoInProgressLwLock()));
    ASSERT_TRUE(check_buffer_lru(buffer, DSTORE::LN_LRU, 1));

    ASSERT_EQ(memcmp(reinterpret_cast<char *>(buffer->GetPage()) + sizeof(Page::m_header), "HelloWorld",
                     strlen("HelloWorld")), 0);

    buffer_pool->UnlockContent(buffer);
    buffer_pool->Release(buffer);
    ASSERT_TRUE(check_buffer_state(buffer, 0U,
                                   (Buffer::BUF_VALID | Buffer::BUF_TAG_VALID),
                                   UNSET_FLAGS | Buffer::BUF_CR_PAGE));
    ASSERT_FALSE(LWLockHeldByMe(&buffer->contentLwLock));
    ASSERT_FALSE(LWLockHeldByMe(buffer->controller->GetIoInProgressLwLock()));

    buffer_pool->Destroy();
    delete buffer_pool;
}

TEST_F(BufferTest, ReuseBufferTest_TIER1)
{
    BufMgr *buffer_pool = DstoreNew(BufferTest::m_ut_memory_context) BufMgr(1000, 1);
    buffer_pool->Init();

    /* read buffer in exclusive mode */
    for (Size i = 0; i < FAKE_SIZE; i++) {
        for (BlockNumber block_num = 0; block_num < FAKE_FILES[i].max_block; block_num++) {
            PageId page_id = {FAKE_FILES[i].file_id, block_num};
            load_page_into_buffer(buffer_pool, page_id, (char*)"Hello");
        }
    }

    for (BlockNumber block_num = 0; block_num < FAKE_FILES[FAKE_SIZE-1].max_block; block_num++) {
        PageId pageId = {FAKE_FILES[FAKE_SIZE-1].file_id, block_num};
        BufferTag bufTag = {g_defaultPdbId, pageId};
        ASSERT_TRUE(STORAGE_FUNC_SUCC(buffer_pool->Flush(bufTag)));
    }

    for (Size i = 0; i < FAKE_SIZE; i++) {
        for (BlockNumber block_num = 0; block_num < FAKE_FILES[i].max_block; block_num++) {
            PageId page_id = {FAKE_FILES[i].file_id, block_num};
            BufferDesc *buffer = buffer_pool->Read(g_defaultPdbId, page_id, LW_SHARED);
            ASSERT_TRUE(check_buffer(buffer, page_id));
            ASSERT_TRUE(check_buffer_state(buffer, 1U,
                                           (Buffer::BUF_VALID | Buffer::BUF_TAG_VALID),
                                           UNSET_FLAGS | (Buffer::BUF_CONTENT_DIRTY | Buffer::BUF_HINT_DIRTY | Buffer::BUF_CR_PAGE)));


            ASSERT_TRUE(buffer->IsContentLocked(LW_SHARED));
            ASSERT_FALSE(LWLockHeldByMe(buffer->controller->GetIoInProgressLwLock()));
            ASSERT_TRUE(check_buffer_lru(buffer, DSTORE::LN_LRU, 1));

            ASSERT_EQ(memcmp(reinterpret_cast<char *>(buffer->GetPage()) + sizeof(Page::m_header), "Hello", strlen("Hello")), 0);

            buffer_pool->UnlockContent(buffer);
            buffer_pool->Release(buffer);
            ASSERT_TRUE(check_buffer_state(buffer, 0U,
                                           (Buffer::BUF_VALID | Buffer::BUF_TAG_VALID),
                                           UNSET_FLAGS | (Buffer::BUF_CONTENT_DIRTY | Buffer::BUF_HINT_DIRTY | Buffer::BUF_CR_PAGE)));
            ASSERT_FALSE(LWLockHeldByMe(&buffer->contentLwLock));
            ASSERT_FALSE(LWLockHeldByMe(buffer->controller->GetIoInProgressLwLock()));
        }
    }

    buffer_pool->Destroy();
    delete buffer_pool;
}

TEST_F(BufferTest, ReuseFirstBufferInLruListTest)
{
    BufMgr *buffer_pool = DstoreNew(BufferTest::m_ut_memory_context) BufMgr(1000, 1);
    buffer_pool->Init();
    BufferDesc *buffers[1000];

    /* read buffer in exclusive mode */
    for (BlockNumber block_num = 0; block_num < FAKE_FILES[0].max_block; block_num++) {
        PageId page_id = {FAKE_FILES[0].file_id, block_num};
        load_page_into_buffer(buffer_pool, page_id, (char*)"Hello");
    }

    /* read all buffer and don't release them */
    for (BlockNumber block_num = 0; block_num < FAKE_FILES[0].max_block; block_num++) {
        PageId page_id = {FAKE_FILES[0].file_id, block_num};
        buffers[block_num] = buffer_pool->Read(g_defaultPdbId, page_id, DSTORE::LW_SHARED);
        ASSERT_TRUE(buffers[block_num] != INVALID_BUFFER_DESC);
    }

    /* just release the first buffer in the lru list to test if the buffer can be reuse */
    BufferDesc *first_buffer_in_lru_list = buffers[FAKE_FILES[0].max_block - 1];
    buffer_pool->UnlockContent(first_buffer_in_lru_list);
    buffer_pool->Release(first_buffer_in_lru_list);
    ASSERT_TRUE(check_buffer_state(first_buffer_in_lru_list, 0U,
                                   (Buffer::BUF_VALID | Buffer::BUF_TAG_VALID),
                                   UNSET_FLAGS | Buffer::BUF_CR_PAGE));
    ASSERT_FALSE(LWLockHeldByMe(&first_buffer_in_lru_list->contentLwLock));
    ASSERT_FALSE(LWLockHeldByMe(first_buffer_in_lru_list->controller->GetIoInProgressLwLock()));

    /* read another page and try to reuse the first buffer in the lru list */
    PageId page_id = {FAKE_FILES[1].file_id, 0};
    BufferDesc *other_buffer = buffer_pool->Read(g_defaultPdbId, page_id, LW_SHARED);
    ASSERT_TRUE(other_buffer != INVALID_BUFFER_DESC);
    ASSERT_TRUE(check_buffer(other_buffer, page_id));
    ASSERT_TRUE(check_buffer_state(other_buffer, 1U,
                                   (Buffer::BUF_VALID | Buffer::BUF_TAG_VALID),
                                   UNSET_FLAGS | Buffer::BUF_CR_PAGE));

    ASSERT_TRUE(LWLockHeldByMeInMode(&other_buffer->contentLwLock, LW_SHARED));
    ASSERT_FALSE(LWLockHeldByMe(other_buffer->controller->GetIoInProgressLwLock()));
    ASSERT_TRUE(check_buffer_lru(other_buffer, DSTORE::LN_LRU, 1));

    buffer_pool->UnlockContent(other_buffer);
    buffer_pool->Release(other_buffer);
    ASSERT_TRUE(check_buffer_state(other_buffer, 0U,
                                   (Buffer::BUF_VALID | Buffer::BUF_TAG_VALID),
                                   UNSET_FLAGS | Buffer::BUF_CR_PAGE));
    ASSERT_FALSE(LWLockHeldByMe(&other_buffer->contentLwLock));
    ASSERT_FALSE(LWLockHeldByMe(other_buffer->controller->GetIoInProgressLwLock()));

    /* release all buffer */
    for (BlockNumber block_num = 0; block_num < FAKE_FILES[0].max_block - 1; block_num++) {
        BufferDesc* buffer = buffers[block_num];
        buffer_pool->UnlockContent(buffer);
        buffer_pool->Release(buffer);

        ASSERT_TRUE(check_buffer_state(buffer, 0U,
                                       (Buffer::BUF_VALID | Buffer::BUF_TAG_VALID),
                                       UNSET_FLAGS | Buffer::BUF_CR_PAGE));
        ASSERT_FALSE(LWLockHeldByMe(&buffer->contentLwLock));
        ASSERT_FALSE(LWLockHeldByMe(buffer->controller->GetIoInProgressLwLock()));
    }

    buffer_pool->Destroy();
    delete buffer_pool;
}

TEST_F(BufferTest, ReuseBufferSkipWalIsWritingTest)
{
    BufMgr *buffer_pool = DstoreNew(BufferTest::m_ut_memory_context) BufMgr(1000, 1);
    buffer_pool->Init();
    BufferDesc *buffers[1000];

    /* read buffer in exclusive mode */
    for (BlockNumber block_num = 0; block_num < FAKE_FILES[0].max_block; block_num++) {
        PageId page_id = {FAKE_FILES[0].file_id, block_num};
        load_page_into_buffer(buffer_pool, page_id, (char*)"Hello");
    }

    /* read all buffer and don't release them */
    for (BlockNumber block_num = 0; block_num < FAKE_FILES[0].max_block; block_num++) {
        PageId page_id = {FAKE_FILES[0].file_id, block_num};
        buffers[block_num] = buffer_pool->Read(g_defaultPdbId, page_id, DSTORE::LW_EXCLUSIVE);
        buffers[block_num]->SetPageIsWritingWal();
        buffer_pool->UnlockAndRelease(buffers[block_num]);
        ASSERT_TRUE(buffers[block_num] != INVALID_BUFFER_DESC);
    }

    /* just release the first buffer in the lru list to test if the buffer can be reuse */
    BufferDesc *first_buffer_in_lru_list = buffers[FAKE_FILES[0].max_block - 1];
    first_buffer_in_lru_list->SetPageEndWriteWal();
    ASSERT_TRUE(check_buffer_state(first_buffer_in_lru_list, 0U,
                                   (Buffer::BUF_VALID | Buffer::BUF_TAG_VALID),
                                   UNSET_FLAGS | Buffer::BUF_CR_PAGE));

    /* read another page and try to reuse the first buffer in the lru list */
    PageId page_id = {FAKE_FILES[1].file_id, 0};
    BufferDesc *other_buffer = buffer_pool->Read(g_defaultPdbId, page_id, LW_SHARED);
    ASSERT_TRUE(other_buffer != INVALID_BUFFER_DESC);
    ASSERT_TRUE(first_buffer_in_lru_list == other_buffer);
    ASSERT_TRUE(check_buffer(other_buffer, page_id));
    ASSERT_TRUE(check_buffer_state(other_buffer, 1U,
                                   (Buffer::BUF_VALID | Buffer::BUF_TAG_VALID),
                                   UNSET_FLAGS | Buffer::BUF_CR_PAGE));

    ASSERT_TRUE(LWLockHeldByMeInMode(&other_buffer->contentLwLock, LW_SHARED));
    ASSERT_FALSE(LWLockHeldByMe(other_buffer->controller->GetIoInProgressLwLock()));
    ASSERT_TRUE(check_buffer_lru(other_buffer, DSTORE::LN_LRU, 1));

    buffer_pool->UnlockContent(other_buffer);
    buffer_pool->Release(other_buffer);
    ASSERT_TRUE(check_buffer_state(other_buffer, 0U,
                                   (Buffer::BUF_VALID | Buffer::BUF_TAG_VALID),
                                   UNSET_FLAGS | Buffer::BUF_CR_PAGE));
    ASSERT_FALSE(LWLockHeldByMe(&other_buffer->contentLwLock));
    ASSERT_FALSE(LWLockHeldByMe(other_buffer->controller->GetIoInProgressLwLock()));

    buffer_pool->Destroy();
    delete buffer_pool;
}

TEST_F(BufferTest, FlushBufferTest)
{
    BufMgr *buffer_pool = DstoreNew(m_ut_memory_context) BufMgr(100, 4);
    buffer_pool->Init();

    /* read buffer in exclusive mode */
    for (Size i = 0; i < FAKE_SIZE; i++) {
        for (BlockNumber block_num = 0; block_num < FAKE_FILES[i].max_block; block_num++) {
            PageId page_id = {FAKE_FILES[i].file_id, block_num};
            load_page_into_buffer(buffer_pool, page_id, (char*)("Hello"));
        }
    }

    /* flush buffer */
    for (Size i = 0; i < FAKE_SIZE; i++) {
        for (BlockNumber block_num = 0; block_num < FAKE_FILES[i].max_block; block_num++) {
            PageId page_id = {FAKE_FILES[i].file_id, block_num};
            BufferDesc* buffer = buffer_pool->Read(g_defaultPdbId, page_id, LW_SHARED);

            ASSERT_TRUE(check_buffer(buffer, page_id));
            ASSERT_TRUE(check_buffer_state(buffer, 1U,
                                           (Buffer::BUF_VALID | Buffer::BUF_TAG_VALID),
                                           UNSET_FLAGS | Buffer::BUF_CR_PAGE));
            ASSERT_TRUE(LWLockHeldByMeInMode(&buffer->contentLwLock, LW_SHARED));
            ASSERT_FALSE(LWLockHeldByMe(buffer->controller->GetIoInProgressLwLock()));
            ASSERT_EQ(memcmp(reinterpret_cast<char *>(buffer->GetPage()) + sizeof(Page::m_header), "Hello",
                             strlen("Hello")), 0);
            buffer_pool->UnlockAndRelease(buffer);

            BufferTag bufTag = {g_defaultPdbId, page_id};
            ASSERT_TRUE(STORAGE_FUNC_SUCC(buffer_pool->Flush(bufTag)));
            ASSERT_TRUE(check_buffer_state(buffer, 0U,
                                           (Buffer::BUF_VALID | Buffer::BUF_TAG_VALID),
                                           UNSET_FLAGS | Buffer::BUF_CR_PAGE));
            ASSERT_FALSE(LWLockHeldByMe(&buffer->contentLwLock));
            ASSERT_FALSE(LWLockHeldByMe(buffer->controller->GetIoInProgressLwLock()));
        }
    }

    VFSAdapter *vfs = g_storageInstance->GetPdb(g_defaultPdbId)->GetVFS();
     for (Size i = 0; i < FAKE_SIZE; i++) {
        for (BlockNumber block_num = 0; block_num < FAKE_FILES[i].max_block; block_num++) {
            PageId page_id = {FAKE_FILES[i].file_id, block_num};
            char page[BLCKSZ];
            vfs->ReadPageSync(page_id, page);
            ASSERT_EQ(strncmp(page + sizeof(Page::m_header), "Hello", strlen("Hello")), 0);
        }
    }

    buffer_pool->Destroy();
    delete buffer_pool;
}

TEST_F(BufferTest, AsyncFlushBufferTest_TIER1)
{
    reopen_fake_file_with_ADIO();
    BufMgr *buffer_pool = DstoreNew(m_ut_memory_context) BufMgr(4500, 4);
    buffer_pool->Init();
    VFSAdapter *vfs = g_storageInstance->GetPdb(g_defaultPdbId)->GetVFS();
    /* read buffer in exclusive mode */
    for (Size i = 0; i < FAKE_SIZE; i++) {
        for (BlockNumber block_num = 0; block_num < FAKE_FILES[i].max_block; block_num++) {
            PageId page_id = {FAKE_FILES[i].file_id, block_num};
            load_page_into_buffer(buffer_pool, page_id, (char*)("Hello"));
        }
    }

    BatchBufferAioContextMgr *batchCtxMgr = DstoreNew(m_ut_memory_context) BatchBufferAioContextMgr();
    RetStatus ret = batchCtxMgr->InitBatch(false, (BufMgrInterface *)buffer_pool);
    ASSERT_TRUE(STORAGE_FUNC_SUCC(ret));

    /* flush buffer */
    for (Size i = 0; i < FAKE_SIZE; i++) {
        for (BlockNumber block_num = 0; block_num < FAKE_FILES[i].max_block; block_num++) {
            PageId pageId = {FAKE_FILES[i].file_id, block_num};
            BufferTag bufTag = {g_defaultPdbId, pageId};
            ASSERT_TRUE(STORAGE_FUNC_SUCC(batchCtxMgr->AsyncFlushPage(bufTag)));
        }
    }

    batchCtxMgr->FsyncBatch();
    batchCtxMgr->DestoryBatch();
    delete batchCtxMgr;

    char *page = nullptr;
    posix_memalign((void**)&page, ALIGNOF_BUFFER, BLCKSZ);
    for (Size i = 0; i < FAKE_SIZE; i++) {
        for (BlockNumber block_num = 0; block_num < FAKE_FILES[i].max_block; block_num++) {
            PageId page_id = {FAKE_FILES[i].file_id, block_num};
            BufferDesc* buffer = buffer_pool->Read(g_defaultPdbId, page_id, LW_SHARED);
            ASSERT_TRUE(check_buffer_state(buffer, 1U,
                                           (Buffer::BUF_VALID | Buffer::BUF_TAG_VALID),
                                           UNSET_FLAGS | Buffer::BUF_CR_PAGE));
            buffer_pool->UnlockContent(buffer);
            buffer_pool->Release(buffer);

            memset_s(page, BLCKSZ, 0, BLCKSZ);
            vfs->ReadPageSync(page_id, page);
            ASSERT_EQ(strncmp(page + sizeof(Page::m_header), "Hello", strlen("Hello")), 0);
        }
    }
    free(page);
    buffer_pool->Destroy();
    delete buffer_pool;
}

struct TestContext {
    BufMgr* buffer_pool;
    const FakeFile* file;
    BlockNumber startBlockNum;
    uint32 iterator;

    /* use for CR test */
    int read_hit_count;
    int read_miss_count;
    int read_not_match_count;

    void reset_cr_test_count()
    {
        read_hit_count = 0;
        read_miss_count = 0;
        read_not_match_count = 0;
    }
};

void* write_page_thread(void* param)
{
    TestContext* context = static_cast<TestContext*>(param);
    BufMgr* buffer_pool = context->buffer_pool;

    BufferMgrFakeStorageInstance *instance = (BufferMgrFakeStorageInstance *)g_storageInstance;
    instance->ThreadSetupAndRegister();
    for (BlockNumber block_number = 0; block_number < context->file->max_block; block_number++) {
        for (uint32 i = 0; i < context->iterator; i++) {
            PageId page_id = {context->file->file_id, block_number};
            BufferDesc* buffer = buffer_pool->
                Read(g_defaultPdbId, page_id, DSTORE::LW_EXCLUSIVE);

            uint32* page = (uint32*)(reinterpret_cast<char *>(buffer->GetPage()) + sizeof(Page::m_header));
            *page = *page + 1;

            buffer_pool->MarkDirty(buffer);
            buffer_pool->UnlockContent(buffer);
            buffer_pool->Release(buffer);
        }
    }

    instance->ThreadUnregisterAndExit();
    return nullptr;
}

void* read_page_thread(void* param)
{
    TestContext* context = static_cast<TestContext*>(param);
    BufMgr* buffer_pool = context->buffer_pool;
    std::default_random_engine rand_engine;
    std::uniform_int_distribution<Size> distribution(0, context->file->max_block - 1);

    BufferMgrFakeStorageInstance *instance = (BufferMgrFakeStorageInstance *)g_storageInstance;
    instance->ThreadSetupAndRegister();
    for (uint32 i = 0; i < context->iterator; i++) {
        BlockNumber block_number = distribution(rand_engine);
        PageId page_id = {context->file->file_id, block_number};
        BufferDesc *buffer = buffer_pool->Read(g_defaultPdbId, page_id, DSTORE::LW_SHARED);

        buffer_pool->UnlockContent(buffer);
        buffer_pool->Release(buffer);
    }

    instance->ThreadUnregisterAndExit();
    return nullptr;
}

std::string construct_cr_content(CommitSeqNo cstart, CommitSeqNo cend)
{
    std::string content = "CR PAGE";
    content += cstart;
    content += "-";
    content += cend;
    return content;
}

TEST_F(BufferTest, ReadBufferInSharedModeMultiThreadTest_TIER1)
{
    uint32 read_page_thread_number = 10;
    uint32 write_page_thread_number = 0;
    uint32 thread_number = read_page_thread_number + write_page_thread_number;
    uint32 iterator = 1000;
    std::thread threads[thread_number];

    BufMgr *buffer_pool = DstoreNew(BufferTest::m_ut_memory_context) BufMgr(1000, 4);
    buffer_pool->Init();

    TestContext* context = (TestContext*) DstoreMemoryContextAlloc(BufferTest::m_ut_memory_context, thread_number * sizeof(TestContext));

    for (uint32 i = 0; i < write_page_thread_number; i++) {
        TestContext* ctx = &context[i];
        ctx->buffer_pool = buffer_pool;
        ctx->file = &FAKE_FILES[i];
        ctx->iterator = iterator;
        threads[i] = std::thread(write_page_thread, (void*)ctx);
        pthread_setname_np(threads[i].native_handle(), "write_page");
    }

    for (uint32 i = write_page_thread_number; i < thread_number; i++) {
        TestContext* ctx = &context[i];
        ctx->buffer_pool = buffer_pool;
        ctx->file = &FAKE_FILES[0];
        ctx->iterator = iterator;
        threads[i] = std::thread(read_page_thread, (void*)ctx);
        pthread_setname_np(threads[i].native_handle(), "read_page");
    }

    wait_for_thread_all_finish(threads, thread_number);

    for (Size i = 0; i < 1; i++) {
        for (BlockNumber block_num = 0; block_num < FAKE_FILES[i].max_block; block_num++) {
            PageId page_id = {FAKE_FILES[i].file_id, block_num};
            BufferDesc *buffer = buffer_pool->Read(g_defaultPdbId, page_id, LW_SHARED);
            ASSERT_TRUE(check_buffer(buffer, page_id));
            ASSERT_TRUE(check_buffer_state(buffer, 1U,
                                           (Buffer::BUF_VALID | Buffer::BUF_TAG_VALID),
                                           UNSET_FLAGS | Buffer::BUF_CR_PAGE));

            ASSERT_TRUE(LWLockHeldByMeInMode(&buffer->contentLwLock, LW_SHARED));
            ASSERT_FALSE(LWLockHeldByMe(buffer->controller->GetIoInProgressLwLock()));

            uint32* page = (uint32*)buffer->GetPage();
            ASSERT_EQ(*page, 0);

            buffer_pool->UnlockContent(buffer);
            buffer_pool->Release(buffer);
            ASSERT_TRUE(check_buffer_state(buffer, 0U,
                                           (Buffer::BUF_VALID | Buffer::BUF_TAG_VALID),
                                           UNSET_FLAGS | Buffer::BUF_CR_PAGE));
            ASSERT_FALSE(LWLockHeldByMe(&buffer->contentLwLock));
            ASSERT_FALSE(LWLockHeldByMe(buffer->controller->GetIoInProgressLwLock()));
        }
    }

    DstorePfree(context);
    buffer_pool->Destroy();
    delete buffer_pool;
}

TEST_F(BufferTest, ReadBufferInExclusiveModeMultiThreadTest_TIER1)
{
    uint32 read_page_thread_number = 0;
    uint32 write_page_thread_number = 4;
    uint32 thread_number = read_page_thread_number + write_page_thread_number;
#ifdef ENABLE_THREAD_CHECK
    uint32 iterator = 10;
#else
    uint32 iterator = 1000;
#endif
    std::thread threads[thread_number];

    BufMgr *buffer_pool = DstoreNew(BufferTest::m_ut_memory_context) BufMgr(500, 4);
    buffer_pool->Init();

    TestContext* context = (TestContext*) DstoreMemoryContextAlloc(BufferTest::m_ut_memory_context, thread_number * sizeof(TestContext));

    for (uint32 i = 0; i < write_page_thread_number; i++) {
        TestContext* ctx = &context[i];
        ctx->buffer_pool = buffer_pool;
        ctx->file = &FAKE_FILES[0];
        ctx->iterator = iterator;
        threads[i] = std::thread(write_page_thread, (void*)ctx);
        pthread_setname_np(threads[i].native_handle(), "write_page");
    }

    for (uint32 i = write_page_thread_number; i < thread_number; i++) {
        TestContext* ctx = &context[i];
        ctx->buffer_pool = buffer_pool;
        ctx->file = &FAKE_FILES[0];
        ctx->iterator = iterator;
        threads[i] = std::thread(read_page_thread, (void*)ctx);
        pthread_setname_np(threads[i].native_handle(), "read_page");
    }

    wait_for_thread_all_finish(threads, thread_number);

    for (Size i = 0; i < 1; i++) {
        for (BlockNumber block_num = 0; block_num < FAKE_FILES[i].max_block; block_num++) {
            PageId page_id = {FAKE_FILES[i].file_id, block_num};
            BufferDesc *buffer = buffer_pool->Read(g_defaultPdbId, page_id, LW_SHARED);
            ASSERT_TRUE(check_buffer(buffer, page_id));
            ASSERT_TRUE(check_buffer_state(buffer, 1U,
                                           (Buffer::BUF_VALID | Buffer::BUF_TAG_VALID),
                                           UNSET_FLAGS | Buffer::BUF_CR_PAGE));

            ASSERT_TRUE(LWLockHeldByMeInMode(&buffer->contentLwLock, LW_SHARED));
            ASSERT_FALSE(LWLockHeldByMe(buffer->controller->GetIoInProgressLwLock()));

            Page *pagePtr = buffer->GetPage();
            uint32* page = (uint32*)(reinterpret_cast<char *>(pagePtr) + sizeof(Page::m_header));
            ASSERT_EQ(*page, write_page_thread_number * iterator);

            buffer_pool->UnlockContent(buffer);
            buffer_pool->Release(buffer);
            ASSERT_TRUE(check_buffer_state(buffer, 0U,
                                           (Buffer::BUF_VALID | Buffer::BUF_TAG_VALID),
                                           UNSET_FLAGS | Buffer::BUF_CR_PAGE));
            ASSERT_FALSE(LWLockHeldByMe(&buffer->contentLwLock));
            ASSERT_FALSE(LWLockHeldByMe(buffer->controller->GetIoInProgressLwLock()));
        }
    }

    DstorePfree(context);
    buffer_pool->Destroy();
    delete buffer_pool;
}

TEST_F(BufferTest, ReadBufferMultiThreadTest01)
{
    uint32 read_page_thread_number = 12;
    uint32 write_page_thread_number = 4;
    uint32 thread_number = read_page_thread_number + write_page_thread_number;
    uint32 iterator = 1000;
    std::thread threads[thread_number];

    BufMgr *buffer_pool = DstoreNew(BufferTest::m_ut_memory_context) BufMgr(500, 4);
    buffer_pool->Init();

    TestContext* context = (TestContext*) DstoreMemoryContextAlloc(BufferTest::m_ut_memory_context, thread_number * sizeof(TestContext));

    for (uint32 i = 0; i < write_page_thread_number; i++) {
        TestContext* ctx = &context[i];
        ctx->buffer_pool = buffer_pool;
        ctx->file = &FAKE_FILES[i];
        ctx->iterator = iterator;
        threads[i] = std::thread(write_page_thread, (void*)ctx);
        pthread_setname_np(threads[i].native_handle(), "write_page");
    }

    for (uint32 i = write_page_thread_number; i < thread_number; i++) {
        TestContext* ctx = &context[i];
        ctx->buffer_pool = buffer_pool;
        ctx->file = &FAKE_FILES[i % FAKE_SIZE];
        ctx->iterator = iterator;
        threads[i] = std::thread(read_page_thread, (void*)ctx);
        pthread_setname_np(threads[i].native_handle(), "read_page");
    }

    wait_for_thread_all_finish(threads, thread_number);

    for (Size i = 0; i < FAKE_SIZE; i++) {
        for (BlockNumber block_num = 0; block_num < FAKE_FILES[i].max_block; block_num++) {
            PageId page_id = {FAKE_FILES[i].file_id, block_num};
            BufferDesc *buffer = buffer_pool->Read(g_defaultPdbId, page_id, LW_SHARED);
            ASSERT_TRUE(check_buffer(buffer, page_id));
            ASSERT_TRUE(check_buffer_state(buffer, 1U,
                                           (Buffer::BUF_VALID | Buffer::BUF_TAG_VALID),
                                           UNSET_FLAGS | Buffer::BUF_CR_PAGE));

            ASSERT_TRUE(LWLockHeldByMeInMode(&buffer->contentLwLock, LW_SHARED));
            ASSERT_FALSE(LWLockHeldByMe(buffer->controller->GetIoInProgressLwLock()));

            Page *pagePtr = buffer->GetPage();
            uint32* page = (uint32*)(reinterpret_cast<char *>(pagePtr) + sizeof(Page::m_header));
            ASSERT_EQ(*page, iterator);

            buffer_pool->UnlockContent(buffer);
            buffer_pool->Release(buffer);
            ASSERT_TRUE(check_buffer_state(buffer, 0U,
                                           (Buffer::BUF_VALID | Buffer::BUF_TAG_VALID),
                                           UNSET_FLAGS | Buffer::BUF_CR_PAGE));
            ASSERT_FALSE(LWLockHeldByMe(&buffer->contentLwLock));
            ASSERT_FALSE(LWLockHeldByMe(buffer->controller->GetIoInProgressLwLock()));
        }
    }

    DstorePfree(context);
    buffer_pool->Destroy();
    delete buffer_pool;
}

TEST_F(BufferTest, ReadBufferMultiThreadTest02)
{
    uint32 read_page_thread_number = 12;
    uint32 write_page_thread_number = 4;
    uint32 thread_number = read_page_thread_number + write_page_thread_number;
    uint32 iterator = 1000;
    std::thread threads[thread_number];

    BufMgr *buffer_pool = DstoreNew(BufferTest::m_ut_memory_context) BufMgr(500, 4);
    buffer_pool->Init();

    TestContext* context = (TestContext*) DstoreMemoryContextAlloc(BufferTest::m_ut_memory_context, thread_number * sizeof(TestContext));

    for (uint32 i = 0; i < write_page_thread_number; i++) {
        TestContext* ctx = &context[i];
        ctx->buffer_pool = buffer_pool;
        ctx->file = &FAKE_FILES[0];
        ctx->iterator = iterator;
        threads[i] = std::thread(write_page_thread, (void*)ctx);
        pthread_setname_np(threads[i].native_handle(), "write_page");
    }

    for (uint32 i = write_page_thread_number; i < thread_number; i++) {
        TestContext* ctx = &context[i];
        ctx->buffer_pool = buffer_pool;
        ctx->file = &FAKE_FILES[i % 2];
        ctx->iterator = iterator;
        threads[i] = std::thread(read_page_thread, (void*)ctx);
        pthread_setname_np(threads[i].native_handle(), "read_page");
    }

    wait_for_thread_all_finish(threads, thread_number);

    for (Size i = 0; i < FAKE_SIZE; i++) {
        for (BlockNumber block_num = 0; block_num < FAKE_FILES[i].max_block; block_num++) {
            PageId page_id = {FAKE_FILES[i].file_id, block_num};
            BufferDesc *buffer = buffer_pool->Read(g_defaultPdbId, page_id, LW_SHARED);
            ASSERT_TRUE(check_buffer(buffer, page_id));
            ASSERT_TRUE(check_buffer_state(buffer, 1U,
                                           (Buffer::BUF_VALID | Buffer::BUF_TAG_VALID),
                                           UNSET_FLAGS | Buffer::BUF_CR_PAGE));

            ASSERT_TRUE(LWLockHeldByMeInMode(&buffer->contentLwLock, LW_SHARED));
            ASSERT_FALSE(LWLockHeldByMe(buffer->controller->GetIoInProgressLwLock()));

            Page *pagePtr = buffer->GetPage();
            uint32* page = (uint32*)(reinterpret_cast<char *>(pagePtr) + sizeof(Page::m_header));
            if (i == 0) {
                ASSERT_EQ(*page, iterator * write_page_thread_number);
            } else {
                ASSERT_EQ(*page, 0);
            }

            buffer_pool->UnlockContent(buffer);
            buffer_pool->Release(buffer);
            ASSERT_TRUE(check_buffer_state(buffer, 0U,
                                           (Buffer::BUF_VALID | Buffer::BUF_TAG_VALID),
                                           UNSET_FLAGS | Buffer::BUF_CR_PAGE));
            ASSERT_FALSE(LWLockHeldByMe(&buffer->contentLwLock));
            ASSERT_FALSE(LWLockHeldByMe(buffer->controller->GetIoInProgressLwLock()));
        }
    }

    DstorePfree(context);
    buffer_pool->Destroy();
    delete buffer_pool;
}

void* KeepBufThread(void* param)
{
    TestContext *context = static_cast<TestContext *>(param);
    BufMgr *bufferPool = context->buffer_pool;
    unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
    std::mt19937 randEngine(seed);
    std::uniform_int_distribution<Size> distribution(0, context->file->max_block / 2 - 1);

    BufferMgrFakeStorageInstance *instance = (BufferMgrFakeStorageInstance *)g_storageInstance;
    instance->ThreadSetupAndRegister();
    for (uint32 i = 0; i < context->iterator; i++) {
        BlockNumber blockNum = distribution(randEngine);
        PageId page_id = {context->file->file_id, blockNum};
        BufferDesc *buffer = bufferPool->Read(g_defaultPdbId, page_id, DSTORE::LW_SHARED);
        uint64 state = buffer->GetState();
        bufferPool->UnlockAndRelease(buffer);
    }

    instance->ThreadUnregisterAndExit();
    return nullptr;
}

void* NotKeepBufThread(void* param)
{
    TestContext *context = static_cast<TestContext *>(param);
    BufMgr *bufferPool = context->buffer_pool;
    unsigned seed = std::chrono::system_clock::now().time_since_epoch().count();
    std::mt19937 randEngine(seed);
    std::uniform_int_distribution<Size> distribution(0, context->file->max_block / 2 - 1);

    BufferMgrFakeStorageInstance *instance = (BufferMgrFakeStorageInstance *)g_storageInstance;
    instance->ThreadSetupAndRegister();
    for (uint32 i = 0; i < context->iterator; i++) {
        BlockNumber blockNum = distribution(randEngine);
        PageId pageId = {context->file->file_id, blockNum};
        BufferDesc *buffer = bufferPool->Read(g_defaultPdbId, pageId, DSTORE::LW_EXCLUSIVE);
        uint64 state = buffer->GetState();
        bufferPool->UnlockAndRelease(buffer);
    }

    instance->ThreadUnregisterAndExit();
    return nullptr;
}

TEST_F(BufferTest, SyncFlushAllTest_TIER1)
{
    BufMgr *bufferPool = DstoreNew(BufferTest::m_ut_memory_context) BufMgr(FAKE_FILES[0].max_block, 4);
    bufferPool->Init();

    for (BlockNumber blockNum = 0; blockNum < FAKE_FILES[0].max_block; ++blockNum) {
        PageId pageId = {FAKE_FILES[0].file_id, blockNum};
        BufferDesc *buffer = bufferPool->Read(g_defaultPdbId, pageId, LW_EXCLUSIVE);
        buffer->GetPage()->Init(0, PageType::HEAP_PAGE_TYPE, pageId);
        errno_t rc = memcpy_s(reinterpret_cast<char *>(buffer->GetPage()) + sizeof(Page::m_header),
            BLCKSZ - sizeof(Page::m_header), "Flush all test", strlen("Flush all test"));
        storage_securec_check(rc, "\0", "\0");
        bufferPool->MarkDirty(buffer);
        bufferPool->UnlockAndRelease(buffer);
    }
    RetStatus ret = bufferPool->FlushAll(false);
    ASSERT_TRUE(STORAGE_FUNC_SUCC(ret));

    VFSAdapter *vfs = g_storageInstance->GetPdb(g_defaultPdbId)->GetVFS();
    for (BlockNumber blockNum = 0; blockNum < FAKE_FILES[0].max_block; ++blockNum) {
        PageId page_id = {FAKE_FILES[0].file_id, blockNum};
        char page[BLCKSZ] = {0};
        vfs->ReadPageSync(page_id, page);
        ASSERT_EQ(strncmp(page + sizeof(Page::m_header), "Flush all test", strlen("Flush all test")), 0);
    }

    bufferPool->Destroy();
    delete bufferPool;
}

TEST_F(BufferTest, AsyncFlushAllTest_TIER1)
{
    reopen_fake_file_with_ADIO();
    BufMgr *bufferPool = DstoreNew(BufferTest::m_ut_memory_context) BufMgr(FAKE_FILES[0].max_block, 4);
    bufferPool->Init();

    for (BlockNumber blockNum = 0; blockNum < FAKE_FILES[0].max_block; ++blockNum) {
        PageId pageId = {FAKE_FILES[0].file_id, blockNum};
        BufferDesc *buffer = bufferPool->Read(g_defaultPdbId, pageId, LW_EXCLUSIVE);
        buffer->GetPage()->Init(0, PageType::HEAP_PAGE_TYPE, pageId);
        errno_t rc = memcpy_s(reinterpret_cast<char *>(buffer->GetPage()) + sizeof(Page::m_header),
            BLCKSZ - sizeof(Page::m_header), "Async Flush all test", strlen("Async Flush all test"));
        storage_securec_check(rc, "\0", "\0");
        bufferPool->MarkDirty(buffer);
        bufferPool->UnlockAndRelease(buffer);
    }
    RetStatus ret = bufferPool->FlushAll(false);
    ASSERT_TRUE(STORAGE_FUNC_SUCC(ret));

    VFSAdapter *vfs = g_storageInstance->GetPdb(g_defaultPdbId)->GetVFS();
    char *page = nullptr;
    posix_memalign((void**)&page, ALIGNOF_BUFFER, BLCKSZ);

    for (BlockNumber blockNum = 0; blockNum < FAKE_FILES[0].max_block; ++blockNum) {
        PageId page_id = {FAKE_FILES[0].file_id, blockNum};
        memset_s(page, BLCKSZ, 0, BLCKSZ);
        vfs->ReadPageSync(page_id, page);
        ASSERT_EQ(strncmp(page + sizeof(Page::m_header), "Async Flush all test", strlen("Async Flush all test")), 0);
    }
    free(page);
    bufferPool->Destroy();
    delete bufferPool;
}
