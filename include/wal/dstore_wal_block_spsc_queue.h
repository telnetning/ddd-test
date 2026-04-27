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
 * dstore_wal_blocking_spsc_queue.h
 *    Definition of a Blocking single-producer, single-consumer queue.
 *
 * IDENTIFICATION
 *    src/gausskernel/dstore/include/wal/dstore_wal_blocking_spsc_queue.h
 *
 * -------------------------------------------------------------------------
 */

#ifndef DSTORE_WAL_BLOCKING_SPSC_QUEUE_H
#define DSTORE_WAL_BLOCKING_SPSC_QUEUE_H
#include <atomic>
#include "common/dstore_datatype.h"
#include "port/dstore_port.h"

namespace DSTORE {

using SleepNotify = void (*)();
const uint32 SLEEP_COUNT_QUE_TAKE = 5000;
const uint32 SLEEP_TIME = 1;
struct QueueInfo {
    std::atomic<uint32> readTail;
    std::atomic<uint32> writeHead;
    char padding[DSTORE_CACHELINE_SIZE - sizeof(uint64)];
} ALIGNED(DSTORE_CACHELINE_SIZE);

/* TO DO: Subsequently replaced with a lock-free queue that uses a common library */
struct BlockSpscQueue {
public:
    /*
     * Initialize the queue.
     *
     * @return: return DSTORE_SUCC if init success.
     */
    RetStatus Init(uint32 cap, uint32 size, SleepNotify func = nullptr);

    /*
     * Destroy the queue.
     *
     */
    void Destroy() noexcept;

    /*
     * Put an element into the queue.
     *
     */
    template<typename T>
    void Put(T element)
    {
        uint32 head = info.writeHead.load(std::memory_order_consume);
        uint32 tail = info.readTail.load(std::memory_order_consume);
        uint32 count = 0;
        while (((tail - (head + 1)) & mask) == 0) {
            if ((count++ % SLEEP_COUNT_QUE_TAKE) == 0) {
                ErrLog(DSTORE_DEBUG1, MODULE_WAL,
                    ErrMsg("BlockSpscQueue Put failed due to no enough space(head:%u, tail:%u), retry it", head, tail));
            }
            GaussUsleep(SLEEP_TIME);
            tail = info.readTail.load(std::memory_order_consume);
        };
        /*
        * Make sure the following write to the buffer happens after the read
        * of the tail.  Combining this with the corresponding barrier in Pop()
        * which guarantees that the tail is updated after reading the buffer,
        * we can be sure that we cannot update a slot's value before it has
        * been read.
        */
        uint32 tmpCnt = Count(head, tail);
        if (tmpCnt > maxUsage) {
            maxUsage = tmpCnt;
        }

        *((static_cast<T>(buffer)) + head) = *element;

        /* Make sure the index is updated after the buffer has been written. */
        info.writeHead.store((head + 1) & mask, std::memory_order_release);
        ErrLog(DSTORE_DEBUG1, MODULE_WAL, ErrMsg("BlockSpscQueue Put success(head:%u, tail:%u)", head, tail));
    }
    
    /*
     * Put n elements into the queue.
     *
     */
    template<typename T>
    void PutN(T eleArry, uint32 n)
    {
        uint32 head = info.writeHead.load(std::memory_order_consume);
        uint32 tail = info.readTail.load(std::memory_order_consume);
        uint32 count = 0;
        uint32 empty = tail > head ? tail - head : capacity + tail - head;
        uint32 tmpCnt = Count(head, tail);
        if (tmpCnt > maxUsage) {
            maxUsage = tmpCnt;
        }
        while (empty <= n) {
            if ((count++ % SLEEP_COUNT_QUE_TAKE) == 0) {
                ErrLog(DSTORE_DEBUG1, MODULE_WAL, ErrMsg(
                    "BlockSpscQueue PutN failed due to no enough space(head:%u, tail:%u, empty:%u, n:%u), retry it",
                    head, tail, empty, n));
            }
            GaussUsleep(SLEEP_TIME);
            tail = info.readTail.load(std::memory_order_consume);
            empty = tail > head ? tail - head : capacity + tail - head;
        };

        /*
        * Make sure the following write to the buffer happens after the read
        * of the tail.  Combining this with the corresponding barrier in Pop()
        * which guarantees that the tail is updated after reading the buffer,
        * we can be sure that we cannot update a slot's value before it has
        * been read.
        */

        for (uint32 i = 0; i < n; i++) {
            *((static_cast<T>(buffer)) + ((head + i) & mask)) = eleArry[i];
        }
        /* Make sure the index is updated after the buffer has been written. */
        info.writeHead.store((head + n) & mask, std::memory_order_release);
        ErrLog(DSTORE_DEBUG1, MODULE_WAL, ErrMsg("BlockSpscQueue PutN success(head:%u, tail:%u, n:%d)", head, tail, n));
    }

    /*
     * Put n elements into the queue if could.
     *
     */
    template <typename T>
    RetStatus PutNIfCould(T eleArry, uint32 n)
    {
        uint32 head = info.writeHead.load(std::memory_order_consume);
        uint32 tail = info.readTail.load(std::memory_order_consume);
        uint32 empty = tail > head ? tail - head : capacity + tail - head;
        uint32 tmpCnt = Count(head, tail);
        if (tmpCnt > maxUsage) {
            maxUsage = tmpCnt;
        }
        if (empty <= n) {
            return DSTORE_FAIL;
        }
        /*
         * Make sure the following write to the buffer happens after the read
         * of the tail.  Combining this with the corresponding barrier in Pop()
         * which guarantees that the tail is updated after reading the buffer,
         * we can be sure that we cannot update a slot's value before it has
         * been read.
         */

        for (uint32 i = 0; i < n; i++) {
            *((static_cast<T>(buffer)) + ((head + i) & mask)) = eleArry[i];
        }
        /* Make sure the index is updated after the buffer has been written. */
        info.writeHead.store((head + n) & mask, std::memory_order_release);
        ErrLog(DSTORE_DEBUG1, MODULE_WAL,
               ErrMsg("BlockSpscQueue PutNIfCould success(head:%u, tail:%u, n:%d)", head, tail, n));
        return DSTORE_SUCC;
    }

    /*
     * Pop an element from the queue.
     *
     * @return: return DSTORE_SUCC if pop operation success.
     */
    template<typename T>
    RetStatus Pop(T element)
    {
        uint32 head = info.writeHead.load(std::memory_order_consume);
        uint32 tail = info.readTail.load(std::memory_order_consume);
        uint32 count = 0;
        while (Count(head, tail) == 0) {
            GaussUsleep(SLEEP_TIME);
            ++count;
            if ((count & SLEEP_COUNT_QUE_TAKE) == SLEEP_COUNT_QUE_TAKE) {
                if (notifyFunc != nullptr) {
                    notifyFunc();
                }
                ErrLog(DSTORE_DEBUG1, MODULE_WAL, ErrMsg(
                    "BlockSpscQueue Pop failed due to no enough elements(head:%u, tail:%u), retry it", head, tail));
                return DSTORE_FAIL;
            }
            head = info.writeHead.load(std::memory_order_consume);
        }

        totalPopCnt++;

        /* Make sure the buffer is read after the index. */
        *element = *((static_cast<T>(buffer)) + tail);
        /* Make sure the read of the buffer finishes before updating the tail. */
        info.readTail.store((tail + 1) & mask, std::memory_order_release);
        ErrLog(DSTORE_DEBUG1, MODULE_WAL, ErrMsg("BlockSpscQueue Pop success(head:%u, tail:%u)", head, tail));
        return DSTORE_SUCC;
    }

    /*
     * Get n elements from the queue.
     *
     * @return: return DSTORE_SUCC if get elements success (maybe not enough).
     */
    template<typename T>
    RetStatus GetN(uint32 n, T *eleArray, uint32 *eleNum)
    {
        uint32 head = info.writeHead.load(std::memory_order_acquire);
        uint32 tail = info.readTail.load(std::memory_order_acquire);
        if (Count(head, tail) == 0) {
            ErrLog(DSTORE_DEBUG1, MODULE_WAL,
                   ErrMsg("BlockSpscQueue GetN failed due to no element(head:%u, tail:%u, n:%u)", head, tail, n));
            return DSTORE_FAIL;
        }

        head = head & mask;
        tail = tail & mask;
        if (head >= tail) {
            *eleNum = head - tail;
        } else {
            *eleNum = capacity - tail;
        }
        *eleNum = Min(n, *eleNum);
        *eleArray = ((static_cast<T>(buffer)) + tail);
        ErrLog(DSTORE_DEBUG1, MODULE_WAL,
            ErrMsg("BlockSpscQueue GetN success(head:%u, tail:%u, eleNum:%d, n:%d)", head, tail, *eleNum, n));
        return DSTORE_SUCC;
    }

    /*
     * Get all elements from the queue.
     *
     * @return: return DSTORE_SUCC if get elements success.
     */
    template<typename T>
    RetStatus GetAll(T *eleArray, uint32 *eleNum)
    {
        uint32 head = info.writeHead.load(std::memory_order_acquire);
        uint32 tail = info.readTail.load(std::memory_order_acquire);
        uint32 count = 0;
        while (Count(head, tail) == 0) {
            ++count;
            if ((count & SLEEP_COUNT_QUE_TAKE) == SLEEP_COUNT_QUE_TAKE) {
                if (notifyFunc != nullptr) {
                    notifyFunc();
                }
                return DSTORE_FAIL;
            }
            head = info.writeHead.load(std::memory_order_acquire);
        }

        head = head & mask;
        tail = tail & mask;
        if (head >= tail) {
            *eleNum = head - tail;
        } else {
            *eleNum = capacity - tail;
        }
        *eleArray = ((static_cast<T>(buffer)) + tail);
        return DSTORE_SUCC;
    }

    /*
     * Pop n elements from the queue.
     *
     * @return: return DSTORE_SUCC if pop operation success.
     */
    RetStatus PopN(uint32 n)
    {
        uint32 head = info.writeHead.load(std::memory_order_acquire);
        uint32 tail = info.readTail.load(std::memory_order_acquire);
        if (Count(head, tail) < n || tail + n > capacity) {
            return DSTORE_FAIL;
        }
        totalPopCnt += n;

        /* Make sure the read of the buffer finishes before updating the tail. */
        info.readTail.store((tail + n) & mask, std::memory_order_release);
        ErrLog(DSTORE_DEBUG1, MODULE_WAL, ErrMsg("BlockSpscQueue PopN success(head:%u, tail:%u, n:%d)", head, tail, n));
        return DSTORE_SUCC;
    }
    /*
     * Check whether the queue is empty.
     *
     * @return: return true if queue is empty.
     */
    bool IsEmpty() const;

    /*
     * Print queue information for debugging.
     *
     */
    void Dump() const;

    /*
     * Gets the current number of queue elements.
     *
     */
    uint32 GetCurUsage() const;

    /*
     * Gets the maximum number of elements in the queue history.
     *
     */
    uint32 GetMaxUsage() const;

    /*
     * Gets the cumulative number of pop elements in the queue.
     *
     */
    uint64 GetTotalPopCnt() const;

    uint32 GetFreeSpace() const;

private:
    /*
     * Count the number between head and tail
     *
     */
    uint32 Count(uint32 head, uint32 tail) const
    {
        return (head - tail) & mask;
    }

    QueueInfo info;
    uint32 capacity;
    uint32 mask;
    uint32 maxUsage;
    uint64 totalPopCnt;
    SleepNotify notifyFunc;
    uint32 nodeSize;
    void *buffer; // An array of pointers with a capacity of m_capacity
};

}

#endif
