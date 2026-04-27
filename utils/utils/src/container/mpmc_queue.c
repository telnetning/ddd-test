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
 * Description: implement of multiple producer consumer bounded queue(MPMC).
 */

#include <stdint.h>
#include <stdbool.h>
#include <stdlib.h>

#include "securec.h"
#include "defines/common.h"
#include "syslog/err_log.h"
#include "container/mpmc_queue.h"

// we use a circular array to implement a bounded lock-less MPMC queue.

typedef struct QueueData QueueData;
struct QueueData {
    // the sequence which is used to identify the status of a queue element
    uint64_t seq;
    union {
        MQElementType data;
        uint8_t cdata[0];
    };
};

struct MpmcQueue {
    char name[MPMC_QUEUE_NAME_SIZE];
    QueueData *slots __attribute__((aligned(GS_CACHE_LINE_SIZE)));
    uint32_t capacity;
    uint32_t elementSize;
    bool isSmallElement;
    uint32_t slotSize;
    uint64_t head __attribute__((aligned(GS_CACHE_LINE_SIZE))); // the position to push
    uint64_t tail __attribute__((aligned(GS_CACHE_LINE_SIZE))); // the position to pop
};

// PLEASE DO NOT REMOVE this code, it is only used when debugging
#undef MPMC_QUEUE_DEBUG
#ifdef MPMC_QUEUE_DEBUG

#define PRINT_BYTES_PER_LINE 16
static void dumpData(const char *title, uint8_t *data, uint32_t len)
{
    printf("--------------------------------------\n");
    printf("dump data for %s, %u bytes:\n", title, len);
    printf("%p:", data);
    for (uint32_t i = 0; i < len; i++) {
        (void)printf("0x%02x ", data[i]);
        if ((i + 1) % PRINT_BYTES_PER_LINE == 0) {
            printf("\n");
            printf("%p:", data + i + 1);
        }
    }
    printf("\n");
    printf("dump data finish.\n");
}

static void dumpQueue(MpmcQueue *queue, const char *title)
{
    printf("--------------------------------------\n");
    printf("dump of queue(%s, %p) for %s\n", queue->name, queue, title);
    printf(" capacity:%u\n", queue->capacity + 1);
    printf(" push count:%lu\n", queue->head);
    printf(" pop  count:%lu\n", queue->tail);
    printf(" element size:%u\n", queue->elementSize);

    size_t slotSize = sizeof(QueueData) - sizeof(MQElementType) + queue->elementSize;
    for (uint32_t i = 0; i < queue->capacity + 1; i++) {
        QueueData *slot = (QueueData *)(((uint8_t *)queue->slots) + slotSize * i);
        printf("  slot[%u]: addr:%p, seq=%lu, data addr:%p, data=", i, slot, slot->seq, slot->cdata);
        for (uint32_t j = 0; j < queue->elementSize; j++) {
            printf("0x%02x,", slot->cdata[j]);
        }
        printf("\n");
    }
    printf("dump of queue finish.\n");
}
#endif

static inline uint32_t GetSlotSize(uint32_t elementSize)
{
    return (uint32_t)(sizeof(QueueData) - sizeof(MQElementType)) + elementSize;
}

static inline QueueData *GetSlotAddr(const MpmcQueue *queue, uint64_t index)
{
    size_t offset = queue->slotSize * (index & queue->capacity);
    uint8_t *slots = (uint8_t *)(void *)queue->slots;
    return (QueueData *)(void *)(slots + offset);
}

static uint32_t UpToPowerOfTwo(uint32_t size)
{
    // We want the queue size to be in a suitable range
    if (size < MPMC_QUEUE_MIN_CAPACITY) {
        return MPMC_QUEUE_MIN_CAPACITY;
    }

    if (size > MPMC_QUEUE_MAX_CAPACITY) {
        return MPMC_QUEUE_MAX_CAPACITY;
    }

    uint64_t newSize = size;
    // Minus one to avoid 'size' is power of two
    newSize--;
    // The following operation makes all bits to be 1 starting from the most significant bit
    newSize |= newSize >> 1;  // Set the 1 x 2 bit of the most significant bit to 1.
    newSize |= newSize >> 2;  // Set the 2 x 2 bit of the most significant bit to 1.
    newSize |= newSize >> 4;  // Set the 4 x 2 bit of the most significant bit to 1.
    newSize |= newSize >> 8;  // Set the 8 x 2 bit of the most significant bit to 1.
    newSize |= newSize >> 16; // Set the 16 x 2 bit of the most significant bit to 1.
    // OK, make it to be a value which is power of two
    newSize++;

    return (uint32_t)newSize;
}

MpmcQueue *MpmcQueueCreateWithDataSize(const char *name, uint32_t size, uint32_t elementSize)
{
    uint32_t slotCount = UpToPowerOfTwo(size);
    ASSERT((slotCount & (slotCount - 1)) == 0);

    size_t slotSize = GetSlotSize(elementSize);

    size_t allocSize = sizeof(MpmcQueue) + slotSize * slotCount;
    MpmcQueue *queue = (MpmcQueue *)malloc(allocSize);
    if (queue == NULL) {
        return NULL;
    }
    errno_t rc = memset_s(queue, allocSize, 0, allocSize);
    if (unlikely(rc != EOK)) {
        ErrLog(ERROR, ErrMsg("Failed to memset_s MpmcQueue, errno is %d.", rc));
        free(queue);
        return NULL;
    }

    int ret = strcpy_s(queue->name, sizeof(queue->name), (name != NULL) ? name : "anonymous");
    if (ret != EOK) {
        free(queue);
        return NULL;
    }

    queue->slots = (QueueData *)(queue + 1);
    queue->head = 0;
    queue->tail = 0;
    queue->capacity = slotCount - 1;
    queue->elementSize = elementSize;
    queue->isSmallElement = (elementSize <= sizeof(MQElementType));
    queue->slotSize = (uint32_t)slotSize;

    for (uint32_t i = 0; i < slotCount; i++) {
        QueueData *slot = (QueueData *)(void *)(((uint8_t *)queue->slots) + slotSize * i);
        slot->seq = i;
    }

    return queue;
}

MpmcQueue *MpmcQueueCreate(const char *name, uint32_t size)
{
    return MpmcQueueCreateWithDataSize(name, size, sizeof(MQElementType));
}

MpmcQueue *MpmcQueueCreateEx(const char *name, uint32_t size, uint32_t elementSize)
{
    if (elementSize <= sizeof(MQElementType)) {
        return NULL;
    }

    return MpmcQueueCreateWithDataSize(name, size, elementSize);
}

void MpmcQueueDestroy(MpmcQueue *queue)
{
    if (queue != NULL) {
        free(queue);
    }
}

bool MpmcQueuePush(MpmcQueue *queue, MQElementType data)
{
    if (queue == NULL) {
        return false;
    }

    uint64_t pushPos = 0;
    QueueData *slot;

    for (;;) {
        __atomic_load(&queue->head, &pushPos, __ATOMIC_RELAXED);

        if (queue->isSmallElement) {
            slot = &queue->slots[pushPos & queue->capacity];
        } else {
            slot = GetSlotAddr(queue, pushPos);
        }

        uint64_t seq = 0;
        __atomic_load(&slot->seq, &seq, __ATOMIC_ACQUIRE);

        if (seq == pushPos) {
            // The current slot is available to push, we use atomic operations to preempt it.
            uint64_t newPushPos = pushPos + 1;
            // clang-format off
            if (__atomic_compare_exchange(&queue->head, &pushPos, &newPushPos, true,
                __ATOMIC_RELAXED, __ATOMIC_RELAXED)) {
                break;
            }
            // clang-format on
        } else if (seq < pushPos) {
            // Queue is full
            // NOTICE: We don't do retries here, because retries may increase the overhead,
            // it can be implemented at the upper layer if user needs.
            return false;
        } else {
            // Someone(other thread) has push data to this slot, so we
            // need to try again (need call sched_yield() to relinquish CPU?)
        }
    }
    if (queue->isSmallElement) {
        slot->data = data;
    } else {
        errno_t ret = memcpy_s(slot->cdata, queue->elementSize, (uint8_t *)data, queue->elementSize);
        if (ret != EOK) {
            return false;
        }
    }
    uint64_t newSeq = pushPos + 1;
    __atomic_store(&slot->seq, &newSeq, __ATOMIC_RELEASE);

    return true;
}

bool MpmcQueuePop(MpmcQueue *queue, MQElementType *data)
{
    if (queue == NULL || data == NULL) {
        return false;
    }

    uint64_t popPos = 0;
    QueueData *slot;

    for (;;) {
        __atomic_load(&queue->tail, &popPos, __ATOMIC_RELAXED);

        if (queue->isSmallElement) {
            slot = &queue->slots[popPos & queue->capacity];
        } else {
            slot = GetSlotAddr(queue, popPos);
        }

        uint64_t seq = 0;
        __atomic_load(&slot->seq, &seq, __ATOMIC_ACQUIRE);

        if (seq == popPos + 1) {
            // The current slot is available to pop, we use atomic operations to preempt it.
            uint64_t newPopPos = popPos + 1;
            // clang-format off
            if (__atomic_compare_exchange(&queue->tail, &popPos, &newPopPos, true,
                __ATOMIC_RELAXED, __ATOMIC_RELAXED)) {
                break;
            }
            // clang-format on
        } else if (seq < popPos + 1) {
            // Queue is empty, return immediately
            return false;
        } else {
            // Someone has pop this slot, so we try again
        }
    }

    if (queue->isSmallElement) {
        *data = slot->data;
    } else {
        errno_t ret = memcpy_s(*data, queue->elementSize, (uint8_t *)slot->cdata, queue->elementSize);
        if (ret != EOK) {
            return false;
        }
    }
    uint64_t newSeq = popPos + queue->capacity + 1;
    __atomic_store(&slot->seq, &newSeq, __ATOMIC_RELEASE);

    return true;
}

static bool CopyData(MpmcQueue *queue, uint64_t popPos, uint64_t popNum, MQElementType *datas)
{
    ASSERT(queue != NULL);
    ASSERT(datas != NULL);

    QueueData *slot;

    for (uint64_t i = 0; i < popNum; i++) {
        if (queue->isSmallElement) {
            slot = &queue->slots[(popPos + i) & queue->capacity];
        } else {
            slot = GetSlotAddr(queue, popPos + i);
        }

        // Wait until the data is ready
        // What should we do if the producer is abnormal and the status is inconsistent?
        for (;;) {
            uint64_t seq = 0;
            __atomic_load(&slot->seq, &seq, __ATOMIC_ACQUIRE);
            if (seq == (popPos + i + 1)) {
                break;
            }
        }

        if (queue->isSmallElement) {
            datas[i] = slot->data;
        } else {
            errno_t ret = memcpy_s(((uint8_t *)datas) + (i * queue->elementSize), queue->elementSize,
                                   (uint8_t *)slot->cdata, queue->elementSize);
            if (ret != EOK) {
                return false;
            }
        }

        uint64_t newPos = popPos + queue->capacity + 1 + i;
        __atomic_store(&slot->seq, &newPos, __ATOMIC_RELEASE);
    }

    return true;
}

bool MpmcQueuePopN(MpmcQueue *queue, MQElementType *datas, uint32_t *popCount)
{
    if (queue == NULL || datas == NULL || popCount == NULL) {
        return false;
    }

    if (*popCount == 0) {
        return false;
    }

    uint64_t popPos = 0;
    uint64_t pushPos = 0;
    QueueData *slot;
    uint64_t actualPopNum = 0;

    for (;;) {
        // Obtain the actual number of pops
        __atomic_load(&queue->head, &pushPos, __ATOMIC_RELAXED);
        __atomic_load(&queue->tail, &popPos, __ATOMIC_RELAXED);
        // NOTICE: the tail of queue maybe larger than head, when other threads have
        // enqueued new elements and dequeued elements at the same time. So we try again.
        if (popPos > pushPos) {
            continue;
        }
        actualPopNum = Min(pushPos - popPos, *popCount);
        // If queue is empty just return
        if (actualPopNum == 0) {
            return false;
        }

        if (queue->isSmallElement) {
            slot = &queue->slots[popPos & queue->capacity];
        } else {
            slot = GetSlotAddr(queue, popPos);
        }

        uint64_t seq = 0;
        __atomic_load(&slot->seq, &seq, __ATOMIC_ACQUIRE);

        if (seq == (popPos + 1)) {
            // Add actualPopNum to 'tail', let the current consumer exclusively occupies these slots
            uint64_t nextTail = popPos + actualPopNum;
            if (__atomic_compare_exchange(&queue->tail, &popPos, &nextTail, true, __ATOMIC_RELAXED, __ATOMIC_RELAXED)) {
                break;
            }
        } else if (seq < (popPos + 1)) {
            // Queue is empty, return immediately
            return false;
        } else {
            // Someone has pop this slot, so we try again
        }
    }

    bool result = CopyData(queue, popPos, actualPopNum, datas);
    if (result) {
        *popCount = (uint32_t)actualPopNum;
    }
    return result;
}

bool MpmcQueueIsEmpty(const MpmcQueue *queue)
{
    if (queue == NULL) {
        return false;
    }

    for (;;) {
        uint64_t pos;
        __atomic_load(&queue->tail, &pos, __ATOMIC_RELAXED);
        QueueData *slot = &queue->slots[pos & queue->capacity];
        uint64_t seq = 0;
        __atomic_load(&slot->seq, &seq, __ATOMIC_ACQUIRE);

        if (seq == (pos + 1)) {
            return false;
        } else if (seq < (pos + 1)) {
            return true;
        }
    }
}

bool MpmcQueueIsFull(const MpmcQueue *queue)
{
    if (queue == NULL) {
        return false;
    }

    for (;;) {
        uint64_t pos;
        __atomic_load(&queue->head, &pos, __ATOMIC_RELAXED);
        QueueData *slot = &queue->slots[pos & queue->capacity];
        uint64_t seq = 0;
        __atomic_load(&slot->seq, &seq, __ATOMIC_ACQUIRE);

        if (seq == pos) {
            return false;
        } else if (seq < pos) {
            return true;
        }
    }
}

uint64_t MpmcQueueGetCapacity(const MpmcQueue *queue)
{
    return (queue == NULL) ? 0 : (queue->capacity + 1);
}

bool MpmcQueueGetStatus(const MpmcQueue *queue, MpmcQueueStat *stat)
{
    if (queue == NULL || stat == NULL) {
        return false;
    }

    errno_t rc = strcpy_s(stat->name, sizeof(stat->name), queue->name);
    if (unlikely(rc != EOK)) {
        ErrLog(ERROR, ErrMsg("Failed to strcpy_s name, errno is %d.", rc));
        return false;
    }

    stat->capacity = queue->capacity + 1;
    stat->elementSize = queue->elementSize;
    stat->pushCount = queue->head;
    stat->popCount = queue->tail;

    return true;
}

bool MpmcQueueGetElement(const MpmcQueue *queue, uint32_t index, MQElementType *data)
{
    if (queue == NULL || data == NULL || index > queue->capacity) {
        return false;
    }

    QueueData *slot = GetSlotAddr(queue, index);

    if (queue->isSmallElement) {
        *data = slot->data;
    } else {
        errno_t ret = memcpy_s(*data, queue->elementSize, (uint8_t *)slot->cdata, queue->elementSize);
        if (ret != EOK) {
            return false;
        }
    }
    return true;
}
