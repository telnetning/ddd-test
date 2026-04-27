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
 * dstore_logical_queue.h
 *    Definition of a queue to store logical logs, which is put by decode worker and get by logical log sender.
 *
 * IDENTIFICATION
 *    src/gausskernel/dstore/include/logical_replication/dstore_logical_queue.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef DSTORE_LOGICAL_QUEUE_H
#define DSTORE_LOGICAL_QUEUE_H
#include <atomic>
#include "common/concurrent/dstore_atomic.h"
#include "common/dstore_datatype.h"

#ifdef UT
#ifndef ENABLE_LOGICAL_REPL
/* LOGICAL REPL is not supported for now. */
#define ENABLE_LOGICAL_REPL
#endif
#endif

namespace DSTORE {

#ifdef ENABLE_LOGICAL_REPL
constexpr uint32 DEFAULT_LOGICAL_QUEUE_SIZE = 128;
constexpr uint32 MAX_LOGICAL_QUEUE_SIZE = 1024;
constexpr uint32 MIN_LOGICAL_QUEUE_SIZE = 2;
constexpr int MAX_TRY_OP_QUEUE = 10;
constexpr long QUEUE_WAIT_TIME = 10000L;

struct LogicalQueue {
public:
    RetStatus Init(uint32 cap = DEFAULT_LOGICAL_QUEUE_SIZE);
    void Destroy() noexcept;

    /* enqueue operation */
    bool Put(void *element);

    /* dequeue operation */
    void* Pop();

    /* get but not dequeue */
    void* Peek();

    /* get queue usage */
    uint32 GetCurUsage();

    /* get max usage */
    uint32 GetMaxUsage() const;

    /* Check whether the queue is empty. */
    bool IsEmpty();

    /* Check whether the queue is full. */
    bool IsFull();

private:
    inline uint32 Count(uint32 head, uint32 tail) const
    {
        return (head - tail) & mask;
    }

    inline uint32 CalSpace(uint32 head, uint32 tail) const
    {
        return (tail - (head + 1)) & mask;
    }

    std::atomic<uint32> writeHead;
    std::atomic<uint32> readTail;
    uint32 capacity;
    uint32 mask;
    gs_atomic_uint32 maxUsage;
    gs_atomic_uint64 totalCnt;
    void **buffer;
};
#endif

}

#endif