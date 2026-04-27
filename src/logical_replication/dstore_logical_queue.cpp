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
 * ---------------------------------------------------------------------------------------
 *
 * dstore_logical_queue.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "logical_replication/dstore_logical_queue.h"
#include "common/concurrent/dstore_atomic.h"
#include "common/concurrent/dstore_barrier.h"
#include "common/memory/dstore_mctx.h"
#include "port/dstore_port.h"

namespace DSTORE {

#ifdef ENABLE_LOGICAL_REPL
RetStatus LogicalQueue::Init(uint32 cap)
{
    StorageAssert(cap >= MIN_LOGICAL_QUEUE_SIZE && (cap & (cap - 1)) == 0);
    capacity = cap;
    mask = cap - 1;
    size_t allocSize = sizeof(void *) * capacity;
    buffer = static_cast<void **>(DstorePalloc0(allocSize));
    if (buffer == nullptr) {
        return DSTORE_FAIL;
    }
    readTail.store(0);
    writeHead.store(0);
    return DSTORE_SUCC;
}

void LogicalQueue::Destroy() noexcept
{
    DstorePfreeExt(buffer);
}

bool LogicalQueue::Put(void *element)
{
    uint32 head = writeHead.load(std::memory_order_consume);
    uint32 tail = readTail.load(std::memory_order_consume);
    if (CalSpace(head, tail) == 0) {
        return false;
    }
    uint32 tmpCnt = Count(head, tail);
    if (tmpCnt > maxUsage) {
        GsAtomicWriteU32(&maxUsage, tmpCnt);
    }
    *(buffer + head) = element;
    GS_WRITE_BARRIER();
    writeHead.store((head + 1) & mask, std::memory_order_release);
    return true;
}

void* LogicalQueue::Pop()
{
    uint32 head = writeHead.load(std::memory_order_consume);
    uint32 tail = readTail.load(std::memory_order_consume);
    if (Count(head, tail) == 0) {
        return nullptr;
    }
    GS_READ_BARRIER();
    void *element = *(buffer + tail);
    readTail.store((tail + 1) & mask, std::memory_order_release);
    return element;
}

void* LogicalQueue::Peek()
{
    uint32 head = writeHead.load(std::memory_order_consume);
    uint32 tail = readTail.load(std::memory_order_consume);
    if (Count(head, tail) == 0) {
        return nullptr;
    } else {
        GS_READ_BARRIER();
        return *(buffer + tail);
    }
}

bool LogicalQueue::IsEmpty()
{
    uint32 head = writeHead.load(std::memory_order_consume);
    uint32 tail = readTail.load(std::memory_order_consume);
    return (Count(head, tail) == 0);
}

bool LogicalQueue::IsFull()
{
    uint32 head = writeHead.load(std::memory_order_consume);
    uint32 tail = readTail.load(std::memory_order_consume);
    return (CalSpace(head, tail) == 0);
}

uint32 LogicalQueue::GetCurUsage()
{
    uint32 head = writeHead.load(std::memory_order_consume);
    uint32 tail = readTail.load(std::memory_order_consume);
    return Count(head, tail);
}

uint32 LogicalQueue::GetMaxUsage() const
{
    return maxUsage;
}
#endif

}