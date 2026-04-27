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
#include "securec.h"
#include "ut_buffer/ut_buffer.h"
#include "ut_buffer/ut_buffer_util.h"
#include <random>
#include <algorithm>

TEST_F(BufferTest, ConstructCrAndReadCr_TIER1)
{
    CommitSeqNo pageMaxCsn = 100;
    BufMgr *bufferPool = DstoreNew (BufferTest::m_ut_memory_context)BufMgr(5, 1);
    bufferPool->Init();

    PageId pageId = {FAKE_FILES[0].file_id, 1};
    load_page_into_buffer(bufferPool, pageId, page_contents[range_0].c_str());
    BufferDesc *baseBuffer = bufferPool->Read(g_defaultPdbId, pageId, LW_SHARED);
    ((DataPage *)baseBuffer->GetPage())->m_header.m_type = static_cast<uint16>(PageType::INDEX_PAGE_TYPE);
    baseBuffer->controller->lastPageModifyTime.store(0, std::memory_order_release);
    uint64 latestWriteTime = baseBuffer->GetLastModifyTime();
    bufferPool->UnlockContent(baseBuffer);

    /* Step 1. Alloc cr buffer in the first time. */
    BufferDesc* crBuffer = bufferPool->ReadOrAllocCr(baseBuffer, latestWriteTime);
    ASSERT_NE(crBuffer, INVALID_BUFFER_DESC);
    bufferPool->FinishCrBuild(crBuffer, pageMaxCsn);
    bufferPool->Release(crBuffer);

    /* Step 2. Can't alloc cr buffer because cr buffer is usable. */
    crBuffer = bufferPool->ReadOrAllocCr(baseBuffer, latestWriteTime);
    ASSERT_EQ(crBuffer, INVALID_BUFFER_DESC);

    /* Step 3. Add write lock to update latestWriteTime and set cr buffer is unusable. */
    bufferPool->LockContent(baseBuffer, LW_EXCLUSIVE);
    latestWriteTime = baseBuffer->GetLastModifyTime();
    bufferPool->UnlockContent(baseBuffer);

    /* Step 4. Can't alloc cr buffer due to TIMESTAMP_THRESHOLD_IN_CR */
    crBuffer = bufferPool->ReadOrAllocCr(baseBuffer, latestWriteTime);
    ASSERT_EQ(crBuffer, INVALID_BUFFER_DESC);

    /* Step 5. Reuse cr buffer allocated in step 1 and construct cr. */
    sleep(TIMESTAMP_THRESHOLD_IN_CR);
    crBuffer = bufferPool->ReadOrAllocCr(baseBuffer, latestWriteTime);
    ASSERT_NE(crBuffer, INVALID_BUFFER_DESC);
    bufferPool->FinishCrBuild(crBuffer, pageMaxCsn);
    bufferPool->Release(crBuffer);

    /* Step 6. Read matched cr */
    std::unique_ptr<SnapshotData> snapshot = UtBufferUtils::prepare_fake_snapshot(pageMaxCsn + 1);
    bufferPool->LockContent(baseBuffer, LW_SHARED);
    crBuffer = bufferPool->ReadCr(baseBuffer, snapshot.get());
    ASSERT_NE(crBuffer, INVALID_BUFFER_DESC);
    ASSERT_TRUE(crBuffer->IsCrPage());
    bufferPool->Release(crBuffer);
    bufferPool->UnlockAndRelease(baseBuffer);

    bufferPool->Destroy();
    delete bufferPool;
}
