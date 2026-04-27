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
 * Description: multiple producer multiple consumer queue
 */

#ifndef UTILS_MPMC_QUEUE_H
#define UTILS_MPMC_QUEUE_H

#include "defines/common.h"
#include "types/data_types.h"

GSDB_BEGIN_C_CODE_DECLS

#define MPMC_QUEUE_NAME_SIZE 64

#define MPMC_QUEUE_MIN_CAPACITY 1

// max capacity is 1024*1024
#define MPMC_QUEUE_MAX_CAPACITY (1 << 20)

typedef void *MQElementType;
typedef struct MpmcQueue MpmcQueue;

/*
 * Create queue
 * @param name[in], the name of queue, use 'anonymous' if input NULL
 * @param size[in], the size of queue, we will limit the queue capacity
 *  between MPMC_QUEUE_MIN_CAPACITY and MPMC_QUEUE_MAX_CAPACITY. The
 *  internal implementation adjusts the power of 2 that is greater than
 *  or equal to size, which make it to locate the position of pop or push
 *  quickly.
 * @param elementSize[in], the element size of queue.
 * @return, a valid queue pointer if success, otherwise NULL.
 */
UTILS_EXPORT MpmcQueue *MpmcQueueCreate(const char *name, uint32_t size);
UTILS_EXPORT MpmcQueue *MpmcQueueCreateEx(const char *name, uint32_t size, uint32_t elementSize);

/*
 * Destroy queue
 * @param queue[in], the pointer of queue
 */
UTILS_EXPORT void MpmcQueueDestroy(MpmcQueue *queue);

/*
 * Push an element to queue
 * @param queue[in], the pointer of queue
 * @param data[in], the element to push
 * @return, true if success, otherwise false.
 */
UTILS_EXPORT bool MpmcQueuePush(MpmcQueue *queue, MQElementType data);

/*
 * Pop an element from queue
 * @param queue[in], the pointer of queue
 * @param data[out], the element to pop
 * @return, true if success, otherwise false.
 */
UTILS_EXPORT bool MpmcQueuePop(MpmcQueue *queue, MQElementType *data);

/*
 * Pop N elements from queue
 * @param queue[in], the pointer of queue
 * @param datas[out], the element to pop
 * @param popCount[in,out], means expected number of dequeues when use
 *  as input, indicates actual number when output.
 * @return, true if success, otherwise false.
 */
UTILS_EXPORT bool MpmcQueuePopN(MpmcQueue *queue, MQElementType *datas, uint32_t *popCount);

/*
 * Check if the queue is empty
 * @param queue[in], the pointer of queue
 * @return, true if empty, otherwise false.
 */
UTILS_EXPORT bool MpmcQueueIsEmpty(const MpmcQueue *queue);

/*
 * Check if the queue is full
 * @param queue[in], the pointer of queue
 * @return, true if full, otherwise false.
 */
UTILS_EXPORT bool MpmcQueueIsFull(const MpmcQueue *queue);

/*
 * Get the capacity of queue
 * @param queue[in], the pointer of queue
 * @return the capacity of queue
 */
UTILS_EXPORT uint64_t MpmcQueueGetCapacity(const MpmcQueue *queue);

// The status of queue
typedef struct MpmcQueueStat MpmcQueueStat;
struct MpmcQueueStat {
    char name[MPMC_QUEUE_NAME_SIZE]; // the name of queue
    uint32_t capacity;               // the capacity of queue
    uint32_t elementSize;            // the size of element of queue
    uint64_t pushCount;              // total count of push
    uint64_t popCount;               // total count of pop
};

/*
 * Get the status of queue
 * @param queue[in], the pointer of queue
 * @param stat[out], the handle of MpmcQueueStat
 * @return, true if success, otherwise false.
 */
UTILS_EXPORT bool MpmcQueueGetStatus(const MpmcQueue *queue, MpmcQueueStat *stat);

/*
 * Get element from queue without pop
 * @param queue[in], the pointer of queue
 * @param index[in], the index of the element
 * @param data[out], the address to store element
 * @return, true if success, otherwise false.
 */
UTILS_EXPORT bool MpmcQueueGetElement(const MpmcQueue *queue, uint32_t index, MQElementType *data);

GSDB_END_C_CODE_DECLS

#endif
