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

struct ThreadTestContext {
    BufMgr *bufferPool;
    int iterateNum;
};

TEST_F(BufferTest, BufferMemChunkList_BufferMemChunkTest_TIER1)
{
    Size bufferSize = 80; /* block number in buffer pool */
    Size memChunkSize = 8; /* block number of each mem chunk */
    Size memChunkNumber = bufferSize / memChunkSize; /* mem chunk number */
    BufferMemChunkWrapper<BufferMemChunk> *firstMemChunk = nullptr;
    /* 80 blocks in total, each mem chunk has 8 blocks and 10 mem chunks in total */

    BufferMemChunkList *bufferMemChunkList =
        DstoreNew(g_dstoreCurrentMemoryContext) BufferMemChunkList(bufferSize, memChunkSize);
    bufferMemChunkList->AppendBufferMemChunkList<BufferMemChunk>(memChunkNumber, &firstMemChunk);

    BufferMemChunkWrapper<BufferMemChunk> *chunkWrapper = bufferMemChunkList->GetMemChunkIterator<BufferMemChunk>();
    Size numChunks = 0;
    uint64 chunkId = 0;
    while (chunkWrapper != nullptr) {
        ASSERT_EQ(chunkWrapper, firstMemChunk);
        ASSERT_NE(chunkWrapper->memChunk, nullptr);
        BufferMemChunk *chunk = chunkWrapper->memChunk;
        ASSERT_EQ(chunk->GetSize(), memChunkSize);
        ASSERT_EQ(chunk->GetMemChunkId(), chunkId);
        ASSERT_EQ(chunk->GetBufferControllerSize(), (memChunkSize) * sizeof(BufferDescController));
        ASSERT_EQ(chunk->GetTotalSize(),
            sizeof(BufferMemChunk) +
            chunk->GetBufferDescSize() +
            chunk->GetBufferBlockSize() +
            chunk->GetBufferControllerSize());
        for (Size i = 0; i < memChunkSize; ++i) {
            BufferDesc *bufferDesc = chunk->GetBufferDesc(i);
            ASSERT_TRUE(chunk->IsBelongTo(bufferDesc));
            ASSERT_TRUE(chunk->GetBufferBlock(i) == bufferDesc->bufBlock);
            ASSERT_TRUE(chunk->GetBufferController<BufferDescController>(i) == bufferDesc->controller);
            ASSERT_EQ(bufferMemChunkList->GetMemChunkId(bufferDesc), chunkId);
        }
        ASSERT_EQ(bufferMemChunkList->GetMemChunk(chunkId), chunk);
        ++numChunks;
        ++ chunkId;
        chunkWrapper = chunkWrapper->GetNext();
        firstMemChunk = firstMemChunk->GetNext();
    }

    bufferMemChunkList->DestroyBufferMemChunkList<BufferMemChunk>();
}

TEST_F(BufferTest, BufferMemChunkList_TemperatureSortTest_TIER1)
{
    Size bufferSize = 100; /* block number in buffer pool */
    Size memChunkSize = 10; /* block number of each mem chunk */
    Size memChunkNumber = bufferSize / memChunkSize; /* mem chunk number */
    BufferMemChunkWrapper<BufferMemChunk> *firstMemChunk = nullptr;
    /* 100 blocks in total, each mem chunk has 10 blocks and 10 mem chunks in total */

    BufferMemChunkList *bufferMemChunkList =
        DstoreNew(g_dstoreCurrentMemoryContext) BufferMemChunkList(bufferSize, memChunkSize);
    bufferMemChunkList->AppendBufferMemChunkList<BufferMemChunk>(memChunkNumber, &firstMemChunk);

    BufferMemChunkWrapper<BufferMemChunk> *chunkWrapper =
        bufferMemChunkList->GetMemChunkIterator<BufferMemChunk>();
    Size numChunks = 0;
    uint64 chunkId = 0;
    while (chunkWrapper != nullptr) {
        BufferMemChunk *chunk = chunkWrapper->memChunk;
        Size hotBlockNumber = memChunkSize - chunkId;
        for (Size i = 0; i < hotBlockNumber; ++i) {
            BufferDesc *bufferDesc = chunk->GetBufferDesc(i);
            /* mark the buffer desc as hot */
            bufferDesc->lruNode.m_type = LN_HOT;
        }
        chunk->UpdateStatistics();
        ASSERT_EQ(chunk->GetHotPageCount(), hotBlockNumber);
        ++numChunks;
        ++chunkId;
        chunkWrapper = chunkWrapper->GetNext();
    }

    bufferMemChunkList->LockBufferMemChunkList(LW_EXCLUSIVE);
    BufferMemChunkWrapper<BufferMemChunk> **sortedMemChunk = bufferMemChunkList->SortByTemperature<BufferMemChunk>();
    BufferMemChunkWrapper<BufferMemChunk> *firstSortedChunk = *sortedMemChunk;
    chunkId = 9;
    while (firstSortedChunk != nullptr) {
        Size hotBlockNumber = memChunkSize - chunkId;
        BufferMemChunk *chunk = firstSortedChunk->memChunk;
        ASSERT_EQ(chunk->GetMemChunkId(), chunkId);
        ASSERT_EQ(chunk->GetHotPageCount(), hotBlockNumber);
        --chunkId;
        firstSortedChunk = firstSortedChunk->GetNext();
    }

    DstorePfreeExt(sortedMemChunk);

    bufferMemChunkList->UnlockBufferMemChunkList();
}