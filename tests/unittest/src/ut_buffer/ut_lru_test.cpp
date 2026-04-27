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
#include "ut_buffer/ut_buffer_fake_instance.h"
#include "ut_buffer/ut_buffer_util.h"
#include "ut_utilities/ut_dstore_framework.h"

#include "buffer/dstore_buf_lru.h"
#include "common/dstore_datatype.h"

#include <vector>

using namespace DSTORE;

class BufLruTest : public DSTORETEST {
protected:
    Size buf_size = 20;
    Size hot_buf_size = 16;
    BufLruList *lru = nullptr;
    BufferDesc **buffers = nullptr;

    void SetUp() override
    {
        DSTORETEST::SetUp();
        InstallDatabase(&DSTORETEST::m_guc, m_ut_memory_context);
        BufferMgrFakeStorageInstance *bufMgrInstance = DstoreNew(m_ut_memory_context)BufferMgrFakeStorageInstance();
        bufMgrInstance->Startup(&DSTORETEST::m_guc);

        lru = DstoreNew(m_ut_memory_context) BufLruList(0, hot_buf_size);
        lru->Initialize();
        make_buf_lru(buf_size, lru);
    }

    void TearDown() override
    {
        UtBufferUtils::free_buffer(buffers, buf_size);
        delete lru;

        BufferMgrFakeStorageInstance *bufMgrInstance = (BufferMgrFakeStorageInstance *)g_storageInstance;
        bufMgrInstance->Shutdown();
        delete bufMgrInstance;
        DSTORETEST::TearDown();
    }

    BufferDesc **make_buf_lru(Size buf_size, BufLruList *lru)
    {
        buffers = UtBufferUtils::prepare_buffer(buf_size);

        for (Size i = 0; i < buf_size; i++) {
            lru->AddNewBuffer(buffers[i]);
        }

        return buffers;
    }

    void move_buffer_to_lru_list(BufLruList *buf_lru, Size buf_size)
    {
        for (Size i = 0; i < buf_size; i++) {
            BufferTag bufTag{g_defaultPdbId, {0, uint32(i)}};
            BufferDesc *buffer = buf_lru->GetCandidateBuffer(bufTag, false);
            buffer->bufTag = bufTag;

            buf_lru->PushBackToLru(buffer, true);

            buffer->UnlockHdr(GsAtomicReadU64(&buffer->state));
            buffer->Unpin();
        }
    }

public:
    static testing::AssertionResult check_buffer_pop_from_candidate_list(BufferDesc *buffer)
    {
        if (buffer == INVALID_BUFFER_DESC) {
            return testing::AssertionFailure() << "buffer is INVALID_BUFFER_DESC";
        }

        if (!buffer->lruNode.IsInPendingState()) {
            return testing::AssertionFailure() << "buffer is NOT in pending state";
        }

        uint64 buf_state = GsAtomicReadU64(&buffer->state);
        if (buffer->GetRefcount() != 1U) {
            return testing::AssertionFailure() << "buffer refcount is NOT 1";
        }

        if ((buf_state & Buffer::BUF_LOCKED) == 0) {
            return testing::AssertionFailure() << "do NOT held buffer header lock";
        }
        return testing::AssertionSuccess();
    }
};

TEST_F(BufLruTest, AppendBufferTest)
{
    for (Size i = 0; i < buf_size; i++) {
        ASSERT_TRUE(buffers[i]->lruNode.IsInCandidateList());
    }
}

TEST_F(BufLruTest, GetFreeBufferTest)
{
    for (Size i = 0; i < buf_size; i++) {
        BufferTag bufTag{g_defaultPdbId, {0, uint32(i)}};
        BufferDesc *buffer = lru->GetCandidateBuffer(bufTag, false);

        ASSERT_NE(buffer, nullptr);
        ASSERT_TRUE(buffers[i]->lruNode.IsInPendingState());

        uint64 buf_state = GsAtomicReadU64(&buffer->state);
        ASSERT_EQ(buffer->GetRefcount(), 1U);
        ASSERT_NE((buf_state & Buffer::BUF_LOCKED), 0);

        buffer->bufTag = bufTag;

        lru->PushBackToLru(buffer, true);

        ASSERT_TRUE(buffers[i]->lruNode.IsInLruList());
        ASSERT_EQ(buffer->lruNode.GetUsage(), 1U);

        buffers[i]->UnlockHdr(GsAtomicReadU64(&buffers[i]->state));
        buffers[i]->Unpin();

        buf_state = GsAtomicReadU64(&buffer->state);
        ASSERT_EQ(buffer->GetRefcount(), 0U);
        ASSERT_EQ((buf_state & Buffer::BUF_LOCKED), 0);
    }
}

TEST_F(BufLruTest, MoveBufferFromLruToCandidateListTest_TIER1)
{
    uint64 buf_state;

    for (Size i = 0; i < buf_size; i++) {
        BufferTag bufTag{g_defaultPdbId, {0, uint32(i)}};
        BufferDesc *buffer = lru->GetCandidateBuffer(bufTag, false);
        ASSERT_TRUE(check_buffer_pop_from_candidate_list(buffer));

        buffer->bufTag = bufTag;

        lru->PushBackToLru(buffer, true);

        ASSERT_TRUE(buffers[i]->lruNode.IsInLruList());
        ASSERT_EQ(buffer->lruNode.GetUsage(), 1U);

        buffers[i]->UnlockHdr(GsAtomicReadU64(&buffers[i]->state));
        buffers[i]->Unpin();

        buf_state = GsAtomicReadU64(&buffer->state);
        ASSERT_EQ(buffer->GetRefcount(), 0U);
        ASSERT_EQ((buf_state & Buffer::BUF_LOCKED), 0);
    }

    BufferTag bufTag{g_defaultPdbId, {1, (BlockNumber)buf_size}};
    /* get one more buffer from candidate list */
    BufferDesc *buffer = lru->GetCandidateBuffer(bufTag, false);
    ASSERT_TRUE(check_buffer_pop_from_candidate_list(buffer));
    /* check if the last one in the lru list is moved to candidate list */
    ASSERT_EQ(buffer->bufTag, BufferTag({g_defaultPdbId, {0, 0}}));

    lru->PushBackToLru(buffer, true);
    ASSERT_TRUE(buffer->lruNode.IsInLruList());
    ASSERT_EQ(buffer->lruNode.GetUsage(), 1U);

    buffer->UnlockHdr(GsAtomicReadU64(&buffer->state));
    buffer->Unpin();
    buf_state = GsAtomicReadU64(&buffer->state);
    ASSERT_EQ(buffer->GetRefcount(), 0U);
    ASSERT_EQ((buf_state & Buffer::BUF_LOCKED), 0);
}

TEST_F(BufLruTest, MoveLastAvailableBufferToCandidateListTest_TIER1)
{
    uint64 buf_state;

    /* move all free buffer to lru list */
    for (Size i = 0; i < buf_size; i++) {
        BufferTag bufTag{g_defaultPdbId, {0, uint32(i)}};
        BufferDesc *buffer = lru->GetCandidateBuffer(bufTag, false);
        ASSERT_TRUE(check_buffer_pop_from_candidate_list(buffer));

        buffer->bufTag = bufTag;

        lru->PushBackToLru(buffer, true);

        ASSERT_TRUE(buffers[i]->lruNode.IsInLruList());
        ASSERT_EQ(buffer->lruNode.GetUsage(), 1U);

        buffers[i]->UnlockHdr(GsAtomicReadU64(&buffers[i]->state));
        buffers[i]->Unpin();

        buf_state = GsAtomicReadU64(&buffer->state);
        ASSERT_EQ(buffer->GetRefcount(), 0U);
        ASSERT_EQ((buf_state & Buffer::BUF_LOCKED), 0);
    }

    /* pin last buffer in lru list */
    buffers[0]->Pin();
    buf_state = GsAtomicReadU64(&buffers[0]->state);
    ASSERT_EQ(buffers[0]->GetRefcount(), 1U);
    BufferTag bufTag{g_defaultPdbId, {1, (BlockNumber)buf_size}};
    /* buffer[1] will be moved to candidate list */
    BufferDesc *buffer = lru->GetCandidateBuffer(bufTag, false);
    ASSERT_TRUE(check_buffer_pop_from_candidate_list(buffer));

    /* make sure we got the last available one, not the last one */
    ASSERT_EQ(buffer->bufTag, BufferTag({g_defaultPdbId, {0, 1}}));

    lru->PushBackToLru(buffer, true);
    ASSERT_TRUE(buffer->lruNode.IsInLruList());
    ASSERT_EQ(buffer->lruNode.GetUsage(), 1U);

    buffer->UnlockHdr(GsAtomicReadU64(&buffer->state));
    buffer->Unpin();
    buf_state = GsAtomicReadU64(&buffer->state);
    ASSERT_EQ(buffer->GetRefcount(), 0U);
    ASSERT_EQ((buf_state & Buffer::BUF_LOCKED), 0);

    /* buffer[0] still in lru list */
    ASSERT_TRUE(buffers[0]->lruNode.IsInLruList());
    buffers[0]->Unpin();
}

TEST_F(BufLruTest, DoNotFindAvailableBufferInCandidateListTest_TIER1)
{
    uint64 buf_state;

    /* move all free buffer to lru list */
    for (Size i = 0; i < buf_size; i++) {
        BufferTag bufTag{g_defaultPdbId, {0, uint32(i)}};
        BufferDesc *buffer = lru->GetCandidateBuffer(bufTag, false);
        ASSERT_TRUE(check_buffer_pop_from_candidate_list(buffer));

        buffer->bufTag = bufTag;

        lru->PushBackToLru(buffer, true);

        ASSERT_TRUE(buffers[i]->lruNode.IsInLruList());
        ASSERT_EQ(buffer->lruNode.GetUsage(), 1U);

        buffers[i]->UnlockHdr(GsAtomicReadU64(&buffers[i]->state));
        buffers[i]->Unpin();

        buf_state = GsAtomicReadU64(&buffer->state);
        ASSERT_EQ(buffer->GetRefcount(), 0U);
        ASSERT_EQ((buf_state & Buffer::BUF_LOCKED), 0);
    }

    /* pin all buffers in lru list */
    for (Size i = 0; i < buf_size; i++) {
        buffers[i]->Pin();
        ASSERT_EQ(buffers[i]->GetRefcount(), 1U);
    }

    /* return INVALID_BUFFER_DESC */
    BufferTag bufTag{g_defaultPdbId, {1, (BlockNumber)buf_size}};
    BufferDesc *buffer = lru->GetCandidateBuffer(bufTag, false);
    ASSERT_EQ(buffer, INVALID_BUFFER_DESC);

    for (Size i = 0; i < buf_size; i++) {
        buffers[i]->Unpin();
    }
}

TEST_F(BufLruTest, AccessBufferInCandidateList)
{
    lru->BufferAccessStat(buffers[0]);

    /* make sure the buffer is moved from candidate list to lru list */
    ASSERT_TRUE(buffers[0]->lruNode.IsInLruList());
    ASSERT_EQ(buffers[0]->lruNode.GetUsage(), 1U);
}

TEST_F(BufLruTest, AccessBufferInLruList)
{
    move_buffer_to_lru_list(lru, buf_size);

    /* check the buffer state */
    for (Size i = 0; i < buf_size; i++) {
        ASSERT_TRUE(buffers[i]->lruNode.IsInLruList());
        ASSERT_EQ(buffers[i]->lruNode.GetUsage(), 1U);
    }

    /* access buffer */
    for (Size i = 0; i < 3; i++) {
        lru->BufferAccessStat(buffers[0]);
        ASSERT_TRUE(buffers[0]->lruNode.IsInLruList());
        ASSERT_EQ(buffers[0]->lruNode.GetUsage(), 1U + (i + 1));
    }

    /* buffer move to hot list */
    lru->BufferAccessStat(buffers[0]);
    ASSERT_TRUE(buffers[0]->lruNode.IsInHotList());
    ASSERT_EQ(buffers[0]->lruNode.GetUsage(), 0U);
}

TEST_F(BufLruTest, AccessBufferInHotList)
{
    move_buffer_to_lru_list(lru, buf_size);

    /* check the buffer state */
    for (Size i = 0; i < buf_size; i++) {
        ASSERT_TRUE(buffers[i]->lruNode.IsInLruList());
        ASSERT_EQ(buffers[i]->lruNode.GetUsage(), 1U);
    }

    /* access buffer */
    for (Size j = 0; j < 3; j++) {
        for (Size i = 0; i < hot_buf_size; i++) {
            lru->BufferAccessStat(buffers[i]);
            ASSERT_TRUE(buffers[i]->lruNode.IsInLruList());
            ASSERT_EQ(buffers[i]->lruNode.GetUsage(), 1U + (j + 1));
        }
    }

    for (Size i = 0; i < hot_buf_size; i++) {
        lru->BufferAccessStat(buffers[i]);
        ASSERT_TRUE(buffers[i]->lruNode.IsInHotList());
        ASSERT_EQ(buffers[i]->lruNode.GetUsage(), 0U);
    }

    /* 前 80% 的 Buffer 已经移到 Hot List */
    for (Size i = 0; i < hot_buf_size; i++) {
        ASSERT_TRUE(buffers[i]->lruNode.IsInHotList());
    }

    for (Size i = hot_buf_size; i < buf_size; i++) {
        ASSERT_TRUE(buffers[i]->lruNode.IsInLruList());
    }
}

TEST_F(BufLruTest, AccessBufferThenMoveBufferToHotAndEvictBackToLru_TIER1)
{
    move_buffer_to_lru_list(lru, buf_size);

    /* check the buffer state */
    for (Size i = 0; i < buf_size; i++) {
        ASSERT_TRUE(buffers[i]->lruNode.IsInLruList());
        ASSERT_EQ(buffers[i]->lruNode.GetUsage(), 1U);
    }

    /* access buffer */
    for (Size j = 0; j < 3; j++) {
        for (Size i = 0; i < hot_buf_size; i++) {
            lru->BufferAccessStat(buffers[i]);
            ASSERT_TRUE(buffers[i]->lruNode.IsInLruList());
            ASSERT_EQ(buffers[i]->lruNode.GetUsage(), 1U + (j + 1));
        }
    }

    /* move buffer to hot list */
    for (Size i = 0; i < hot_buf_size; i++) {
        lru->BufferAccessStat(buffers[i]);
        ASSERT_TRUE(buffers[i]->lruNode.IsInHotList());
        ASSERT_EQ(buffers[i]->lruNode.GetUsage(), 0U);
    }

    /* 前 80% 的 Buffer 已经移到 Hot list */
    for (Size i = 0; i < hot_buf_size; i++) {
        ASSERT_TRUE(buffers[i]->lruNode.IsInHotList());
    }

    for (Size i = hot_buf_size; i < buf_size; i++) {
        ASSERT_TRUE(buffers[i]->lruNode.IsInLruList());
    }

    BufferDesc *lru_buf = buffers[hot_buf_size];
    for (Size i = 0; i < 4; i++) {
        lru->BufferAccessStat(lru_buf);
    }

    /* lru buf move to hot list, and first buffer move back to lru */
    ASSERT_TRUE(lru_buf->lruNode.IsInHotList());
    ASSERT_EQ(lru_buf->lruNode.GetUsage(), 0U);
    ASSERT_TRUE(buffers[0]->lruNode.IsInLruList());
    ASSERT_EQ(buffers[0]->lruNode.GetUsage(), 0U);
}

TEST_F(BufLruTest, MoveCandidateBufferToCandidateListTest)
{
    lru->MoveToCandidateList(buffers[0]);

    /* buffer is still in candidate list */
    ASSERT_TRUE(buffers[0]->lruNode.IsInCandidateList());
}

TEST_F(BufLruTest, MoveLruBufferToCandidateListTest)
{
    move_buffer_to_lru_list(lru, buf_size);

    /* check the buffer state */
    for (Size i = 0; i < buf_size; i++) {
        ASSERT_TRUE(buffers[i]->lruNode.IsInLruList());
        ASSERT_EQ(buffers[i]->lruNode.GetUsage(), 1U);
    }

    lru->MoveToCandidateList(buffers[0]);
    ASSERT_TRUE(buffers[0]->lruNode.IsInCandidateList());
    ASSERT_EQ(buffers[0]->lruNode.GetUsage(), 0U);
}

TEST_F(BufLruTest, MoveHotBufferToCandidateListTest_TIER1)
{
    move_buffer_to_lru_list(lru, buf_size);

    /* check the buffer state */
    for (Size i = 0; i < buf_size; i++) {
        ASSERT_TRUE(buffers[i]->lruNode.IsInLruList());
        ASSERT_EQ(buffers[i]->lruNode.GetUsage(), 1U);
    }

    /* access buffer */
    for (Size j = 0; j < 3; j++) {
        for (Size i = 0; i < hot_buf_size; i++) {
            lru->BufferAccessStat(buffers[i]);
            ASSERT_TRUE(buffers[i]->lruNode.IsInLruList());
            ASSERT_EQ(buffers[i]->lruNode.GetUsage(), 1U + (j + 1));
        }
    }

    for (Size i = 0; i < hot_buf_size; i++) {
        lru->BufferAccessStat(buffers[i]);
        ASSERT_TRUE(buffers[i]->lruNode.IsInHotList());
        ASSERT_EQ(buffers[i]->lruNode.GetUsage(), 0U);
    }

    /* 前 80% 的 Buffer 已经移到 Hot List */
    for (Size i = 0; i < hot_buf_size; i++) {
        ASSERT_TRUE(buffers[i]->lruNode.IsInHotList());
    }

    for (Size i = hot_buf_size; i < buf_size; i++) {
        ASSERT_TRUE(buffers[i]->lruNode.IsInLruList());
    }

    lru->MoveToCandidateList(buffers[0]);
    ASSERT_TRUE(buffers[0]->lruNode.IsInCandidateList());
    ASSERT_EQ(buffers[0]->lruNode.GetUsage(), 0U);
}

TEST_F(BufLruTest, RemoveBufferInCandidateListTest_TIER1)
{
    for (uint32 i = 0; i < buf_size; i++) {
        lru->Remove(buffers[i]);
    }

    for (uint32 i = 0; i < buf_size; i++) {
        ASSERT_TRUE(buffers[i]->lruNode.IsInPendingState());
        ASSERT_EQ(buffers[i]->lruNode.GetUsage(), 0U);
    }
}

TEST_F(BufLruTest, RemoveBufferInLruListTest)
{
    move_buffer_to_lru_list(lru, buf_size);

    for (uint32 i = 0; i < buf_size; i++) {
        ASSERT_TRUE(buffers[i]->lruNode.IsInLruList());
        ASSERT_EQ(buffers[i]->lruNode.GetUsage(), 1U);
    }

    for (uint32 i = 0; i < buf_size; i++) {
        lru->Remove(buffers[i]);
    }

    for (uint32 i = 0; i < buf_size; i++) {
        ASSERT_TRUE(buffers[i]->lruNode.IsInPendingState());
        ASSERT_EQ(buffers[i]->lruNode.GetUsage(), 0U);
    }
}

TEST_F(BufLruTest, RemoveBufferInHotListTest)
{
    move_buffer_to_lru_list(lru, buf_size);

    for (uint32 i = 0; i < buf_size; i++) {
        ASSERT_TRUE(buffers[i]->lruNode.IsInLruList());
        ASSERT_EQ(buffers[i]->lruNode.GetUsage(), 1U);
    }

    /* access buffer */
    for (Size j = 0; j < 3; j++) {
        for (Size i = 0; i < hot_buf_size; i++) {
            lru->BufferAccessStat(buffers[i]);
            ASSERT_TRUE(buffers[i]->lruNode.IsInLruList());
            ASSERT_EQ(buffers[i]->lruNode.GetUsage(), 1U + (j + 1));
        }
    }

    /* move buffer to hot list */
    for (Size i = 0; i < hot_buf_size; i++) {
        lru->BufferAccessStat(buffers[i]);
        ASSERT_TRUE(buffers[i]->lruNode.IsInHotList());
        ASSERT_EQ(buffers[i]->lruNode.GetUsage(), 0U);
    }

    /* 前 80% 的 Buffer 已经移到 Hot list */
    for (Size i = 0; i < hot_buf_size; i++) {
        ASSERT_TRUE(buffers[i]->lruNode.IsInHotList());
    }

    for (uint32 i = 0; i < hot_buf_size; i++) {
        lru->Remove(buffers[i]);
    }

    for (uint32 i = 0; i < hot_buf_size; i++) {
        ASSERT_TRUE(buffers[i]->lruNode.IsInPendingState());
        ASSERT_EQ(buffers[i]->lruNode.GetUsage(), 0U);
    }

    for (uint32 i = hot_buf_size; i < buf_size; i++) {
        ASSERT_TRUE(buffers[i]->lruNode.IsInLruList());
        ASSERT_EQ(buffers[i]->lruNode.GetUsage(), 1U);
    }
}
