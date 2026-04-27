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
 *
 * dstore_wal_blocking_spsc_queue.cpp
 *	  Implementation of a Blocking single-producer, single-consumer queue.
 *
 * IDENTIFICATION
 *    src/gausskernel/dstore/include/wal/dstore_wal_blocking_spsc_queue.cpp
 *
 * -------------------------------------------------------------------------
 */
#include "common/concurrent/dstore_barrier.h"
#include "common/memory/dstore_mctx.h"
#include "wal/dstore_wal_block_spsc_queue.h"
namespace DSTORE {

const int QUEUE_CAPACITY_MIN_LIMIT = 2;
const int QUEUE_PUT_WAIT_LIMIT = 3;

inline bool IsPowOfTwo(uint32 x)
{
    return (x & (x - 1)) == 0;
}

RetStatus BlockSpscQueue::Init(uint32 cap, uint32 size, SleepNotify func)
{
    /*
     * We require the capacity to be a power of 2, so index wrap can be
     * handled by a bit-wise and.  The actual capacity is one less than
     * the specified, so the minimum capacity is 2.
     */
    StorageAssert(cap >= QUEUE_CAPACITY_MIN_LIMIT && IsPowOfTwo(cap));

    notifyFunc = func;
    capacity = cap;
    mask = capacity - 1;
    size_t allocSize = size * capacity;
    buffer = static_cast<void *>(DstorePalloc0(allocSize));
    if (buffer == nullptr) {
        return DSTORE_FAIL;
    }
    nodeSize = size;
    info.readTail.store(0);
    info.writeHead.store(0);
    return DSTORE_SUCC;
}

void BlockSpscQueue::Destroy() noexcept
{
    DstorePfreeExt(buffer);
    notifyFunc = nullptr;
}

bool BlockSpscQueue::IsEmpty() const
{
    uint32 head = info.writeHead.load(std::memory_order_consume);
    uint32 tail = info.readTail.load(std::memory_order_consume);
    return (Count(head, tail) == 0);
}

void BlockSpscQueue::Dump() const
{
}

uint32 BlockSpscQueue::GetCurUsage() const
{
    uint32 head = info.writeHead.load(std::memory_order_consume);
    uint32 tail = info.readTail.load(std::memory_order_consume);
    return Count(head, tail);
}

uint32 BlockSpscQueue::GetMaxUsage() const
{
    return maxUsage;
}

uint64 BlockSpscQueue::GetTotalPopCnt() const
{
    return totalPopCnt;
}

uint32 BlockSpscQueue::GetFreeSpace() const
{
    uint32 head = info.writeHead.load(std::memory_order_consume);
    uint32 tail = info.readTail.load(std::memory_order_consume);
    uint32 empty = tail > head ? tail - head : capacity + tail - head;
    return empty > 0 ? empty - 1 : empty;
}

}
