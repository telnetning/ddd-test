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
 * Description: Implement for memory context generic alloctor
 */

#include <stdint.h>
#include <stdio.h>
#include <unistd.h>
#include "lock/spinlock.h"
#include "types/data_types.h"
#include "memory/memory_context_internal.h"

#ifdef LOCAL_MODULE_NAME
#undef LOCAL_MODULE_NAME
#endif
#define LOCAL_MODULE_NAME "memory_context"

#define SHARE_ALLOC_MAGIC          0xEAEA7777
#define ALLOC_CHUNK_HEADER_SIZE    MAXALIGN(sizeof(ShareChunk))
#define ALLOC_ALLOCTOR_HEADER_SIZE MAXALIGN(sizeof(ShareAllocator))

#define ALLOC_CHUNK_TAIL_SIZE MAXALIGN(sizeof(SentinelMagicType))
#define ALLOC_CHUNK_OVERHEAD  (ALLOC_CHUNK_HEADER_SIZE + ALLOC_CHUNK_TAIL_SIZE) /* memory management overhead */

typedef struct ShareChunk ShareChunk;
typedef struct ShareAllocator ShareAllocator;
struct ShareChunk {
    uint32_t lmagic; /* left magic */
    int line;        /* arm64 struct aligned to 8 bytes default, so line put after lmagic */
    const char *file;
    /* the below struct data only used in share memory context, int simple share */
    ShareChunk *next; /* next chunk */
    ShareChunk *prev; /* prev chunk */
    void *mctx;       /* owner memory context */
    size_t allocSize;
    MemCtxChunkMagic magic; /* must be last member, will set to 0x7A7BEE | 8 bit chunk header mask */
};

struct ShareAllocator {
    uint32_t magicNum; /* ALLOC_ALLOCTOR_MAGIC */
    MemCtxHdr header;
    ShareChunk *head;
    SpinLock lock;
    /* initialize argument */
    size_t maxSpaceSize;
    /* statistics data */
    size_t totalChunks;
    size_t totalSpace;
};

static inline ShareAllocator *MemCtxHdr2ShareAllocator(const MemCtxHdr *mctx)
{
    return (ShareAllocator *)((uintptr_t)mctx - offsetof(ShareAllocator, header));
}

static MemCtxHdr *ShareAllocatorCreate(size_t maxUsageLimit, SYMBOL_UNUSED size_t initBlockSize,
                                       SYMBOL_UNUSED size_t maxBlockSize, SYMBOL_UNUSED size_t chunkSize)
{
    /* verify parameters, at least can allocate 1 byte size memory, alloc(1) that is alloc(MAXALIGN(1)) */
    size_t minSize = ALLOC_CHUNK_OVERHEAD + ALLOC_ALLOCTOR_HEADER_SIZE + MAXALIGN(1);
    if (unlikely(maxUsageLimit < minSize)) {
        (void)fprintf(stderr, "share alloctor create fail, maxUsageLimit(%zu) need >%zu.\n", maxUsageLimit, minSize);
        return NULL;
    }
    /* Allocate the initial block. */
    ShareAllocator *allocator = (ShareAllocator *)MctxMalloc(ALLOC_ALLOCTOR_HEADER_SIZE, NULL);
    if (unlikely(allocator == NULL)) {
        return NULL;
    }

    SpinLockInit(&allocator->lock);
    allocator->head = NULL;
    allocator->magicNum = ALLOC_ALLOCTOR_MAGIC;
    allocator->maxSpaceSize = maxUsageLimit;
    allocator->totalChunks = 0;
    allocator->totalSpace = 0;

    return &allocator->header;
}

static inline void *ChunkToPtr(const ShareChunk *chunk)
{
    return (void *)((uintptr_t)chunk + ALLOC_CHUNK_HEADER_SIZE);
}

static inline ShareChunk *PtrToChunk(const void *pointer)
{
    return (ShareChunk *)((uintptr_t)pointer - ALLOC_CHUNK_HEADER_SIZE);
}

static void DoDeleteUserArea(ShareAllocator *allocator)
{
    ASSERT(allocator != NULL);
    ShareChunk *last = NULL;
    size_t size = 0;
    const char *name = MemCtxIsSilent(&allocator->header) ? NULL : GetMemoryContextName(&allocator->header);
    for (ShareChunk *chunk = allocator->head; chunk != NULL; chunk = chunk->next) {
        UnPoisonedSentinel(ChunkToPtr(chunk), chunk->allocSize - ALLOC_CHUNK_OVERHEAD);
        CheckOutOfBoundWrite(ChunkToPtr(chunk), chunk->allocSize - ALLOC_CHUNK_OVERHEAD, name, chunk->file,
                             chunk->line);
        MctxFree(last, size);
        last = chunk;
        size = chunk->allocSize;
        allocator->totalChunks--;
    }
    MctxFree(last, size);
}

static inline void ReportNotFreed(ShareAllocator *allocator, const char *mctxName)
{
    ASSERT(allocator != NULL);
    ASSERT(mctxName != NULL);
    SpinLockAcquire(&allocator->lock);
    for (ShareChunk *chunk = allocator->head; chunk != NULL; chunk = chunk->next) {
        ErrLog(ERROR, ErrMsg("share memory context %s not freed %s:%d", mctxName, chunk->file, chunk->line));
    }
    SpinLockRelease(&allocator->lock);
}

static void ShareAllocatorDelete(MemCtxHdr *context)
{
    ASSERT(context != NULL);
    ShareAllocator *allocator = MemCtxHdr2ShareAllocator(context);
    SpinLockAcquire(&allocator->lock);
    DoDeleteUserArea(allocator);
    SpinLockRelease(&allocator->lock);
    /* MemoryContext will to be deleting, can't operate it, avoid someone collapse when free memory in this context */
    /* this check is in order to prevent the scenario of concurrent memory allocate */
    if (unlikely(allocator->totalChunks != 0)) {
        if (!MemCtxIsSilent(context)) {
            ErrLog(ERROR, ErrMsg("share memory context %s leave %zu chunks not free after delete context!!!",
                                 GetMemoryContextName(context), allocator->totalChunks));

            ReportNotFreed(allocator, GetMemoryContextName(context));
        }
    }

    ASSERT(allocator->totalChunks == 0);

    SpinLockDestroy(&allocator->lock);
    MctxFree(allocator, ALLOC_ALLOCTOR_HEADER_SIZE);
}

static void QueueChunk(ShareAllocator *alloc, ShareChunk *chunk, size_t size)
{
    ASSERT(alloc != NULL);
    ASSERT(chunk != NULL);
    SpinLockAcquire(&alloc->lock);
    chunk->next = alloc->head;
    if (likely(alloc->head != NULL)) {
        alloc->head->prev = chunk;
    }
    ASSERT(alloc->totalChunks < alloc->totalChunks + 1);
    ASSERT(alloc->totalSpace < alloc->totalSpace + size);
    alloc->head = chunk;
    alloc->totalChunks += 1;
    alloc->totalSpace += size;
    SpinLockRelease(&alloc->lock);
}

static void DequeueChunk(ShareAllocator *alloc, ShareChunk *chunk, size_t size)
{
    ASSERT(alloc != NULL);
    ASSERT(chunk != NULL);
    SpinLockAcquire(&alloc->lock);
    if (likely(chunk->prev != NULL)) {
        chunk->prev->next = chunk->next;
    } else {
        /* head node */
        alloc->head = chunk->next;
    }
    if (likely(chunk->next != NULL)) {
        chunk->next->prev = chunk->prev;
    }
    ASSERT(alloc->totalChunks > alloc->totalChunks - 1);
    ASSERT(alloc->totalSpace > alloc->totalSpace - size);
    alloc->totalChunks -= 1;
    alloc->totalSpace -= size;
    SpinLockRelease(&alloc->lock);
}

static inline void InitChunk(ShareAllocator *alloc, ShareChunk *chunk, size_t size, uint8_t id)
{
    ASSERT(alloc != NULL);
    ASSERT(chunk != NULL);
    chunk->lmagic = SHARE_ALLOC_MAGIC;
    chunk->mctx = &alloc->header;
    chunk->allocSize = size;
    chunk->prev = NULL;

    MctxSetChunkMagic(&chunk->magic, ChunkToPtr(chunk), id);
}

static inline void SetChunkCallSite(ShareChunk *chunk, const char *file, int line)
{
    ASSERT(chunk != NULL);
    chunk->file = file;
    chunk->line = line;
}

static size_t WalkWithMemleakSummary(MemCtxHdr *context, const char *file, int line, size_t *chunkCount)
{
    size_t summarySize = 0;
    size_t count = 0;
    ShareAllocator *allocator = MemCtxHdr2ShareAllocator(context);
    SpinLockAcquire(&allocator->lock);
    for (ShareChunk *chunk = allocator->head; chunk != NULL; chunk = chunk->next) {
        if ((chunk->file == file) && (chunk->line == line)) {
            /* find it */
            summarySize += (chunk->allocSize - ALLOC_CHUNK_OVERHEAD);
            count++;
        }
    }
    SpinLockRelease(&allocator->lock);
    if (chunkCount != NULL) {
        *chunkCount = count;
    }
    return summarySize;
}

static void *DoShareAlloc(MemCtxHdr *context, size_t size, bool simple, const char *file, int line)
{
    size_t allocSize = ALLOC_CHUNK_OVERHEAD + size;
    ShareAllocator *allocator = MemCtxHdr2ShareAllocator(context);
    size_t needUsageSize = allocator->totalSpace + allocSize + ALLOC_ALLOCTOR_HEADER_SIZE;
    if (unlikely((needUsageSize > allocator->maxSpaceSize) || (allocSize < size))) {
        // exceed maxSpaceSize limit or overflow
        if (MemCtxIsSilent(context)) {
            return NULL;
        }
        ErrLog(ERROR, ErrMsg("memory context %s, %s:%d alloc %zu(%zu), exceed %zu, statistics: totalChunks=%zu, "
                             "totalSpace=%zu",
                             GetMemoryContextName(context), file, line, size, allocSize, allocator->maxSpaceSize,
                             allocator->totalChunks, allocator->totalSpace));
        /* simple memory context can't traverse */
        if (!simple && MemCtxIsPrintSummary(context)) {
            /* only print the current allocate location summary, ofcourse the most
             * best result is to print all the allocate location */
            ErrLog(ERROR, ErrMsg("memory context %s, %s:%d total alloc %zu", GetMemoryContextName(context), file, line,
                                 WalkWithMemleakSummary(context, file, line, NULL)));
        }
        return NULL;
    }

    ShareChunk *chunk = (ShareChunk *)MctxMalloc(allocSize, NULL);
    if (unlikely(chunk == NULL)) {
        return NULL;
    }

    if (simple) {
        InitChunk(allocator, chunk, allocSize, MCTX_SIMPLE_SHARE_ID);
        /* simple share no spinlock, update statistic data using atomic API, __ATOMIC_RELAXED is ok */
        /* simple share only update totalChunks data */
        SYMBOL_UNUSED size_t oldTotalChunks = __atomic_fetch_add(&allocator->totalChunks, 1, __ATOMIC_RELAXED);
        SYMBOL_UNUSED size_t oldTotalSpace = __atomic_fetch_add(&allocator->totalSpace, allocSize, __ATOMIC_RELAXED);
        ASSERT(oldTotalSpace < oldTotalSpace + allocSize); // overflow assert
        ASSERT(oldTotalChunks < oldTotalChunks + 1);       // overflow assert
    } else {
        InitChunk(allocator, chunk, allocSize, MCTX_SHARE_ID);
        QueueChunk(allocator, chunk, allocSize);
    }
    SetChunkCallSite(chunk, file, line);
    void *pointer = ChunkToPtr(chunk);
    SetOutOfBoundSentinel(pointer, size);
    return pointer;
}

static void *ShareAlloc(MemCtxHdr *context, size_t size, const char *file, int line)
{
    return DoShareAlloc(context, size, false, file, line);
}

static void *SimpleShareAlloc(MemCtxHdr *context, size_t size, const char *file, int line)
{
    return DoShareAlloc(context, size, true, file, line);
}

static void DoShareFree(void *pointer, bool simple)
{
    ASSERT(MctxCheckChunkMagic(NULL, pointer));

    ShareChunk *chunk = PtrToChunk(pointer);
    ASSERT(chunk != NULL);
    ShareAllocator *allocator = MemCtxHdr2ShareAllocator(chunk->mctx);

    if (simple) {
        ASSERT(GetMemCtxID(chunk->mctx) == MCTX_SIMPLE_SHARE_ID);
        /* simple share no spinlock, update statistic data using atomic API, __ATOMIC_RELAXED is ok */
        /* simple share only update totalChunks data */
        SYMBOL_UNUSED size_t oldTotalChunks = __atomic_fetch_sub(&allocator->totalChunks, 1, __ATOMIC_RELAXED);
        SYMBOL_UNUSED size_t oldTotalSpace =
            __atomic_fetch_sub(&allocator->totalSpace, chunk->allocSize, __ATOMIC_RELAXED);
        ASSERT(chunk->allocSize > 0);
        ASSERT(oldTotalChunks > oldTotalChunks - 1);              // wrap assert
        ASSERT(oldTotalSpace > oldTotalSpace - chunk->allocSize); // wrap assert
    } else {
        ASSERT(GetMemCtxID(chunk->mctx) == MCTX_SHARE_ID);
        DequeueChunk(allocator, chunk, chunk->allocSize);
    }
    UnPoisonedSentinel(ChunkToPtr(chunk), chunk->allocSize - ALLOC_CHUNK_OVERHEAD);
    const char *name = MemCtxIsSilent(&allocator->header) ? NULL : GetMemoryContextName(&allocator->header);
    CheckOutOfBoundWrite(pointer, chunk->allocSize - ALLOC_CHUNK_OVERHEAD, name, chunk->file, chunk->line);
    MctxFree(chunk, chunk->allocSize);
}

static void ShareFree(void *pointer)
{
    DoShareFree(pointer, false);
}

static void SimpleShareFree(void *pointer)
{
    DoShareFree(pointer, true);
}

static void *DoShareRealloc(void *pointer, size_t size, bool simple, const char *file, int line)
{
    ASSERT(MctxCheckChunkMagic(NULL, pointer));

    ShareChunk *old = PtrToChunk(pointer);
    ASSERT(old != NULL);

    MemCtxHdr *mctx = old->mctx;
    ShareAllocator *allocator = MemCtxHdr2ShareAllocator(mctx);

    if (simple) {
        /* same as free */
        SYMBOL_UNUSED size_t oldTotalChunks = __atomic_fetch_sub(&allocator->totalChunks, 1, __ATOMIC_RELAXED);
        SYMBOL_UNUSED size_t oldTotalSpace =
            __atomic_fetch_sub(&allocator->totalSpace, old->allocSize, __ATOMIC_RELAXED);
        ASSERT(old->allocSize > 0);
        ASSERT(oldTotalChunks > oldTotalChunks - 1);            // wrap
        ASSERT(oldTotalSpace > oldTotalSpace - old->allocSize); // wrap
    } else {
        DequeueChunk(allocator, old, old->allocSize);
    }

    UnPoisonedSentinel(ChunkToPtr(old), old->allocSize - ALLOC_CHUNK_OVERHEAD);
    const char *name = MemCtxIsSilent(&allocator->header) ? NULL : GetMemoryContextName(&allocator->header);
    CheckOutOfBoundWrite(ChunkToPtr(old), old->allocSize - ALLOC_CHUNK_OVERHEAD, name, old->file, old->line);

    size_t allocSize = ALLOC_CHUNK_OVERHEAD + size;
    size_t needUsageSize = allocator->totalSpace + allocSize + ALLOC_ALLOCTOR_HEADER_SIZE - old->allocSize;
    if (unlikely((needUsageSize > allocator->maxSpaceSize) || (allocSize < size))) {
        // exceed maxSpaceSize limit or overflow
        return NULL;
    }
    ShareChunk *new = (ShareChunk *)MctxRealloc(old, old->allocSize, allocSize);
    if (unlikely(new == NULL)) {
        return NULL;
    }

    if (simple) {
        InitChunk(allocator, new, allocSize, MCTX_SIMPLE_SHARE_ID);
        /* same as alloc */
        SYMBOL_UNUSED size_t oldTotalChunks = __atomic_fetch_add(&allocator->totalChunks, 1, __ATOMIC_RELAXED);
        SYMBOL_UNUSED size_t oldTotalSpace = __atomic_add_fetch(&allocator->totalSpace, allocSize, __ATOMIC_RELAXED);
        ASSERT(oldTotalChunks < oldTotalChunks + 1);       // overflow
        ASSERT(oldTotalSpace < oldTotalSpace + allocSize); // overflow
    } else {
        InitChunk(allocator, new, allocSize, MCTX_SHARE_ID);
        QueueChunk(allocator, new, allocSize);
    }
    SetChunkCallSite(new, file, line);
    void *newPointer = ChunkToPtr(new);
    SetOutOfBoundSentinel(newPointer, size);
    return newPointer;
}

static void *ShareRealloc(void *pointer, size_t size, const char *file, int line)
{
    return DoShareRealloc(pointer, size, false, file, line);
}

static void *SimpleShareRealloc(void *pointer, size_t size, const char *file, int line)
{
    return DoShareRealloc(pointer, size, true, file, line);
}

static void ShareReset(MemCtxHdr *context)
{
    ASSERT(context != NULL);
    ShareAllocator *allocator = MemCtxHdr2ShareAllocator(context);
    SpinLockAcquire(&allocator->lock);
    DoDeleteUserArea(allocator);
    /* DoDeleteUserArea will free all the chunks, but here we still check the functional correctness */
    if (unlikely(allocator->totalChunks != 0)) {
        if (!MemCtxIsSilent(context)) {
            ErrLog(ERROR, ErrMsg("share memory context %s leave %zu chunks not free after reset context!!!",
                                 GetMemoryContextName(context), allocator->totalChunks));
        }
    }
    ASSERT(allocator->totalChunks == 0);

    allocator->head = NULL;
    allocator->totalSpace = 0;
    allocator->totalChunks = 0;
    SpinLockRelease(&allocator->lock);
}

static void ShareCheck(MemCtxHdr *context)
{
    ASSERT(context != NULL);
    ShareAllocator *allocator = MemCtxHdr2ShareAllocator(context);
    SpinLockAcquire(&allocator->lock);
    if (unlikely(allocator->totalChunks == 0)) {
        SpinLockRelease(&allocator->lock);
        return;
    }
    for (ShareChunk *chunk = allocator->head; chunk != NULL; chunk = chunk->next) {
        UnPoisonedSentinel(ChunkToPtr(chunk), chunk->allocSize - ALLOC_CHUNK_OVERHEAD);
        const char *name = MemCtxIsSilent(context) ? NULL : GetMemoryContextName(context);
        CheckOutOfBoundWrite(ChunkToPtr(chunk), chunk->allocSize - ALLOC_CHUNK_OVERHEAD, name, chunk->file,
                             chunk->line);
        PoisonedSentinel(ChunkToPtr(chunk), chunk->allocSize - ALLOC_CHUNK_OVERHEAD);
    }
    SpinLockRelease(&allocator->lock);
}

static void SimpleShareReset(MemCtxHdr *context)
{
    ASSERT(context != NULL);
    ShareAllocator *allocator = MemCtxHdr2ShareAllocator(context);
    size_t totalChunks = allocator->totalChunks;
    if (unlikely(totalChunks != 0)) {
        if (!MemCtxIsSilent(context)) {
            ErrLog(ERROR, ErrMsg("share memory context %s leave %zu chunks not free after delete context!!!",
                                 GetMemoryContextName(context), allocator->totalChunks));
        }
    }
}

static size_t ShareGetChunkSpace(const void *pointer)
{
    ASSERT(MctxCheckChunkMagic(NULL, pointer));

    ShareChunk *chunk = PtrToChunk(pointer);
    ASSERT(chunk != NULL);
    return chunk->allocSize;
}

static bool SimpleShareIsEmpty(const MemoryContext context)
{
    ASSERT(context != NULL);
    return MemCtxHdr2ShareAllocator(context)->totalChunks == 0;
}

static void ShareGetStats(const MemoryContext context, bool isPrintToLog, MemStat *stats)
{
    ASSERT(context != NULL);
    ASSERT(stats != NULL);
    stats->totalSpace = MemCtxHdr2ShareAllocator(context)->totalSpace;
    stats->totalChunks = MemCtxHdr2ShareAllocator(context)->totalChunks;
    if (isPrintToLog) {
        ErrLog(LOG, ErrMsg("memory context %s: allocated totalSpace=%zu, allocated totalChunks=%zu",
                           GetMemoryContextName(context), stats->totalSpace, stats->totalChunks));
    }
}

static void ShareGetStatsByPos(const MemoryContext context, bool isPrintToLog, const char *file, int line,
                               MemStat *stats)
{
    ASSERT(context != NULL);
    ASSERT(stats != NULL);
    if (unlikely((file == NULL) || (GetMemCtxID(context) == MCTX_SIMPLE_SHARE_ID))) {
        /* simple share memory context can't traverse user area, it can't get statistics by file position */
        return;
    }
    stats->totalSpace = WalkWithMemleakSummary(context, file, line, &stats->totalChunks);
    if (isPrintToLog) {
        ErrLog(LOG, ErrMsg("memory context %s: [%s:%d]allocated totalSpace=%zu, allocated totalChunks=%zu",
                           GetMemoryContextName(context), file, line, stats->totalSpace, stats->totalChunks));
    }
}

UTILS_INIT_FUNC void InitShareAllocatorMethod(void)
{
    MemoryContextMethods method = {0};
    method.create = ShareAllocatorCreate;
    method.alloc = ShareAlloc;
    method.mfree = ShareFree;
    method.realloc = ShareRealloc;
    method.reset = ShareReset;
    method.delete = ShareAllocatorDelete;
    method.getChunkSpace = ShareGetChunkSpace;
    method.getStats = ShareGetStats;
    method.getStatsByPos = ShareGetStatsByPos;
    method.check = ShareCheck;
    RegisterMemoryContextMethod(MCTX_SHARE_ID, &method);
}

UTILS_INIT_FUNC void InitSimpleShareAllocatorMethod(void)
{
    MemoryContextMethods method = {0};
    /* create,delete have extra spinlock can accept */
    method.create = ShareAllocatorCreate;
    method.delete = ShareAllocatorDelete;

    method.getChunkSpace = ShareGetChunkSpace;
    method.getStats = ShareGetStats;
    method.getStatsByPos = ShareGetStatsByPos;
    /* SimpleXXX API is no extra lock, only have lock in malloc/free of system library */
    method.alloc = SimpleShareAlloc;
    method.mfree = SimpleShareFree;
    method.realloc = SimpleShareRealloc;
    method.reset = SimpleShareReset;
    method.isEmpty = SimpleShareIsEmpty;
    RegisterMemoryContextMethod(MCTX_SIMPLE_SHARE_ID, &method);
}
