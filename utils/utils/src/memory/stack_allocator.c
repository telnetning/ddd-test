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
 * Description: Implement for memory context stack alloctor, stack alloctor must is not shared between thread.
 */

#include <stdint.h>
#include <stdio.h>
#include <assert.h>
#include "types/data_types.h"
#include "memory/memory_context_internal.h"

#ifdef LOCAL_MODULE_NAME
#undef LOCAL_MODULE_NAME
#endif
#define LOCAL_MODULE_NAME "memory_context"

#define ALLOC_CHUNK_HEADER_SIZE    MAXALIGN(sizeof(StackChunk))
#define ALLOC_ALLOCTOR_HEADER_SIZE MAXALIGN(sizeof(StackAllocator))
/* independent encode, if save stack top pointer, will produce a StackStat data push to stack by sync */
#define ALLOCTOR_STAT_SIZE MAXALIGN(sizeof(StackStat))

/* Allocate the initial memory, ALLOCTOR_STAT_SIZE is reserved, so StackSavePoint will not fail, but if have called
 * StackSavePoint, then first alloc memory will max size also is maxUsageLimit, because another ALLOCTOR_STAT_SIZE
 * is reserve, thus reserve double ALLOCTOR_STAT_SZ. there are also reserve a ALLOCTOR_SAVE_POINT_TAG_SZ size to
 * decide wether have a savepoint before top pointer */
#define ALLOC_ALLOCTOR_EXTRA_SIZE \
    (ALLOC_ALLOCTOR_HEADER_SIZE + (ALLOCTOR_STAT_SIZE << 1) + ALLOCTOR_SAVE_POINT_TAG_SIZE)
#define STACK_STAT_TAG_MAGIC         (uint32_t)0xABCD1928
#define STACK_CHUNK_TAG_MAGIC        (uint32_t)0x8531FEFE
#define ALLOCTOR_SAVE_POINT_TAG_SIZE sizeof(uint32_t)
#define ALLOC_CHUNK_TAIL_SIZE        MAXALIGN(sizeof(SentinelMagicType))
/* a chunk will fixed include a header and a tail */
#define ALLOC_CHUNK_OVERHEAD (ALLOC_CHUNK_HEADER_SIZE + ALLOC_CHUNK_TAIL_SIZE)
/* alloc(1) that is alloc(MAXALIGN(1)) */
#define ALLOC_1_BYTE_NEED_CHUNK_SIZE (ALLOC_CHUNK_OVERHEAD + MAXALIGN(1))
/* will force transfer tag to 4bytes, because the implement have some hard code, if need change, be for careful */
static_assert(ALLOCTOR_SAVE_POINT_TAG_SIZE == sizeof(uint32_t), "ALLOCTOR_SAVE_POINT_TAG_SZ must be sizeof(uint32_t)");

typedef struct StackChunk StackChunk;
typedef struct StackAllocator StackAllocator;
typedef struct StackStat StackStat;

struct StackStat {
    uint32_t totalSpace;      /* total alloc space, include user allocate space and chunk alloc overhead, if
                                 memory size sensitive, this field can also not save */
    uint32_t freeCount;       /* the chunks count by called free */
    uint32_t usedChunks;      /* the in use chunks quantity */
    uint32_t lastChunkOffset; /* fast index of the last chunk, easy reverse traversal the whole chunks */
    uint32_t magic;           /* magic 0xABCD1928 */
};

/* for memory management overhead, the chunk header size is aligned to 8bytes, so every a StackChunk is wasted
 * aligned(8, sizeof(StackChunk)) memory space to record chunk data. current is 32 bytes */
struct StackChunk {
    uint32_t ownerOffset; /* the offset of this chunk to it belongs memory context */
    uint32_t prevOffset;  /* the offset of prev chunk */
    uint32_t requestSize; /* request size, actually alloc size is MAXALIGN(requestSize) */
    const char *file;
    int line;               /* when chunk freed, line is -1 */
    MemCtxChunkMagic magic; /* must be last member, will set to 0x7A7BEE | 8 bit chunk header mask */
};

struct StackAllocator {
    uint32_t magicNum; /* 0xEAEA1234 */
    MemCtxHdr header;
    void *top;             /* stack allocator top pointer */
    uint32_t maxSpaceSize; /* initialize argument, aligned to 8 bytes
                              the context max space size alloc from OS, include the memory manage data */
    StackStat stat;        /* statistics data */
};

static inline MemCtxHdr *GetMemCtxFromAllocator(StackAllocator *allocator)
{
    return &allocator->header;
}

static inline StackAllocator *GetAllocatorFromMemCtx(MemCtxHdr *mctx)
{
    return (StackAllocator *)((uintptr_t)mctx - offsetof(StackAllocator, header));
}

static inline void *GetUserPtrFromChunk(StackChunk *chunk)
{
    return (void *)((char *)chunk + ALLOC_CHUNK_HEADER_SIZE);
}

static inline StackChunk *GetChunkFromUserPtr(const void *pointer)
{
    return (StackChunk *)((uintptr_t)pointer - ALLOC_CHUNK_HEADER_SIZE);
}

static inline StackAllocator *GetAllocatorFromChunk(StackChunk *chunk)
{
    return (StackAllocator *)((uintptr_t)chunk - chunk->ownerOffset);
}

static inline StackChunk *GetLastChunkFromAllocator(StackAllocator *allocator)
{
    return (StackChunk *)((uintptr_t)allocator + allocator->stat.lastChunkOffset);
}

static inline size_t GetAllocSize(size_t size)
{
    return MAXALIGN(size);
}

static inline bool IsChunkFreed(const StackChunk *chunk)
{
    return chunk->line == -1;
}

static inline void MarkTop(StackAllocator *allocator, uint32_t topTypeBefore)
{
    *(uint32_t *)allocator->top = topTypeBefore;
}

static inline bool IsMarkedSavePoint(StackAllocator *allocator)
{
    return *(uint32_t *)allocator->top == STACK_STAT_TAG_MAGIC;
}

/* traverse forward for check sentinel
   @param  frontEnd     - must be before the address of chunk that you want to check. for example, chunk address list
                          0x8, 0x20, 0x32, 0x80, if want check the 0x20 and 0x32 two chunk, your frontEnd parameter
                          need less than 0x20, if frontEnd = 0x20, ForwardWalkWithCheck will only check 0x32 chunk
   @param  backChunk    - the last chunk you want to check. in the above example, backChunk is 0x32
 */
static inline void ForwardWalkWithCheck(void *frontEnd, StackChunk *backChunk, const char *mctxName, bool rePoisone)
{
    StackChunk *chunk = backChunk;
    /* chunk == frontEnd will not to check */
    while ((uintptr_t)chunk > (uintptr_t)frontEnd) {
        void *userPtr = GetUserPtrFromChunk(chunk);
        UnPoisonedSentinel(userPtr, chunk->requestSize);
        CheckOutOfBoundWrite(userPtr, chunk->requestSize, mctxName, chunk->file, chunk->line);
        if (rePoisone) {
            PoisonedSentinel(userPtr, chunk->requestSize);
        }
        chunk = (StackChunk *)((uintptr_t)chunk - chunk->prevOffset);
    }
}

static MemCtxHdr *StackAllocatorCreate(size_t maxUsageLimit, SYMBOL_UNUSED size_t initBlockSize,
                                       SYMBOL_UNUSED size_t maxBlockSize, SYMBOL_UNUSED size_t chunkSize)
{
    /* verify parameters, at least can allocate 1 byte size memory to user */
    size_t minSize = ALLOC_ALLOCTOR_EXTRA_SIZE + ALLOC_1_BYTE_NEED_CHUNK_SIZE;
    /* limit the size to 32bit max, because totalSpace is only 32bit, so it max is UINT32_MAX. and need
       ALLOC_ALLOCTOR_EXTRA_SIZE to allocator manage, see the illustrate at those macro */
    size_t maxSize = UINT32_MAX - ALLOC_ALLOCTOR_EXTRA_SIZE;
    if (unlikely((MAXALIGN(maxUsageLimit) < minSize) || (MAXALIGN(maxUsageLimit) > maxSize))) {
        (void)fprintf(stderr, "stack alloctor fail, maxUsageLimit(%zu) out of scope [%zu, %zu]\n",
                      MAXALIGN(maxUsageLimit), minSize, maxSize);
        return NULL;
    }

    size_t allocSize = MAXALIGN(maxUsageLimit) + ALLOC_ALLOCTOR_EXTRA_SIZE;
    /* the whole size of stack allocator initialize memory is maxUsageLimit, no chunk can alloc to maxUsageLimit size */
    StackAllocator *allocator = (StackAllocator *)MctxMalloc(allocSize, NULL);
    if (allocator == NULL) {
        (void)fprintf(stderr, "stack alloctor create fail, maybe no free memory in system, %s\n", strerror(errno));
        return NULL;
    }

    /* init allocator data */
    allocator->magicNum = ALLOC_ALLOCTOR_MAGIC;
    /* top point init position */
    allocator->top = (void *)((uintptr_t)allocator + ALLOC_ALLOCTOR_HEADER_SIZE);
    /* save initial parameter */
    allocator->maxSpaceSize = (uint32_t)MAXALIGN(maxUsageLimit);

    allocator->stat.totalSpace = 0;
    allocator->stat.freeCount = 0;
    allocator->stat.usedChunks = 0;
    allocator->stat.lastChunkOffset = 0;
    allocator->stat.magic = STACK_STAT_TAG_MAGIC;
    /* push ALLOCTOR_STAT_SIZE and record stat */
    StackStat *stat = (StackStat *)allocator->top;
    *stat = allocator->stat;
    allocator->top = (void *)((uintptr_t)allocator->top + ALLOCTOR_STAT_SIZE);
    MarkTop(allocator, STACK_STAT_TAG_MAGIC); /* created a save point in above, mark it */

    return GetMemCtxFromAllocator(allocator);
}

static void StackAllocatorDelete(MemCtxHdr *mctx)
{
    ASSERT(mctx != NULL);
    StackAllocator *allocator = GetAllocatorFromMemCtx(mctx);

    if (unlikely(allocator->stat.usedChunks != 0)) {
        if (!MemCtxIsSilent(mctx)) {
            ErrLog(ERROR, ErrMsg("delete stack memory context:%s with %u chunks in use", GetMemoryContextName(mctx),
                                 allocator->stat.usedChunks));
        }
    }
    /* before free the memory, walk all chunks to check out of bound */
    const char *name = MemCtxIsSilent(mctx) ? NULL : GetMemoryContextName(mctx);
    /* check memory and reset sentinel */
    ForwardWalkWithCheck(allocator, GetLastChunkFromAllocator(allocator), name, false);

    /* free the initialize memory directly */
    MctxFree(allocator, allocator->maxSpaceSize + ALLOC_ALLOCTOR_EXTRA_SIZE);
}

/* traverse forward for check sentinel */
static size_t ForwardWalkWithSummary(uintptr_t frontEnd, StackChunk *backChunk, const char *file, int line,
                                     size_t *chunkCount)
{
    ASSERT(line >= 0);
    size_t result = 0;
    size_t count = 0;
    StackChunk *chunk = backChunk;
    while ((uintptr_t)chunk > frontEnd) {
        if ((chunk->file == file) && (chunk->line == line)) {
            result += chunk->requestSize;
            count++;
        }
        chunk = (StackChunk *)((uintptr_t)chunk - chunk->prevOffset);
    }
    if (chunkCount != NULL) {
        *chunkCount = count;
    }
    return result;
}

/* after alloc a chunk, you should consider to update allocator statistics, before call this, need check size scope */
static StackChunk *AllocChunk(StackAllocator *allocator, size_t size, const char *file, int line)
{
    ASSERT(allocator != NULL);

    size_t allocSize = GetAllocSize(size) + ALLOC_CHUNK_OVERHEAD;
    if (((size_t)allocator->stat.totalSpace + allocSize > allocator->maxSpaceSize) || (allocSize < size)) {
        // no enough space or overflow
        MemoryContext ctx = GetMemCtxFromAllocator(allocator);
        if (MemCtxIsSilent(ctx)) {
            return NULL;
        }
        /* traverse may spend too much time, so only print total statistics */
        ErrLog(ERROR, ErrMsg("memory context %s, %s:%d alloc %zu(%zu), exceed %u, statistics: totalSpace=%u, "
                             "freeCount=%u, usedChunks=%u",
                             GetMemoryContextName(ctx), file, line, size, allocSize, allocator->maxSpaceSize,
                             allocator->stat.totalSpace, allocator->stat.freeCount, allocator->stat.usedChunks));
        if (MemCtxIsPrintSummary(ctx)) {
            /* only print the current allocate location summary, ofcourse the most
             * best result is to print all the allocate location */
            ErrLog(ERROR, ErrMsg("memory context %s, %s:%d total alloc %zu", GetMemoryContextName(ctx), file, line,
                                 ForwardWalkWithSummary((uintptr_t)allocator, GetLastChunkFromAllocator(allocator),
                                                        file, line, NULL)));
        }
        return NULL;
    }

    StackChunk *chunk = (StackChunk *)allocator->top;
    /* set the offset between chunk with allocator to ownerOffset */
    chunk->ownerOffset = (uint32_t)((uintptr_t)chunk - (uintptr_t)allocator);
    chunk->prevOffset = chunk->ownerOffset - allocator->stat.lastChunkOffset;
    chunk->requestSize = (uint32_t)size;
    MctxSetChunkMagic(&chunk->magic, GetUserPtrFromChunk(chunk), MCTX_STACK_ID);
    SetOutOfBoundSentinel(GetUserPtrFromChunk(chunk), chunk->requestSize);

    allocator->top = (void *)((uintptr_t)allocator->top + allocSize);
    allocator->stat.lastChunkOffset = chunk->ownerOffset;
    /* update statistics */
    allocator->stat.totalSpace += (uint32_t)allocSize;
    allocator->stat.usedChunks += 1;

    MarkTop(allocator, STACK_CHUNK_TAG_MAGIC); /* created a chunk, mark it */
    return chunk;
}

static inline void SetChunkCallSite(StackChunk *chunk, const char *file, int line)
{
    ASSERT(chunk != NULL);
    chunk->file = file;
    chunk->line = line;
}

static inline void ClearChunkCallSite(StackChunk *chunk)
{
    ASSERT(chunk != NULL);
    chunk->line = -1;
}

static void *StackAlloc(MemCtxHdr *mctx, size_t size, const char *file, int line)
{
    ASSERT(mctx != NULL);
    if (unlikely((size == 0) || (size > UINT32_MAX))) { // max space not supper over UINT32_MAX
        return NULL;
    }

    StackAllocator *allocator = GetAllocatorFromMemCtx(mctx);
    /* alloc memory MAXALIGN */
    StackChunk *chunk = AllocChunk(allocator, size, file, line);
    if (chunk == NULL) {
        return NULL;
    }
    SetChunkCallSite(chunk, file, line);
    return GetUserPtrFromChunk(chunk);
}

static void StackFree(void *pointer)
{
    ASSERT(MctxCheckChunkMagic(NULL, pointer));

    StackChunk *chunk = GetChunkFromUserPtr(pointer);
    ASSERT(chunk != NULL);
    StackAllocator *allocator = GetAllocatorFromChunk(chunk);
    ASSERT(allocator != NULL);

    MemoryContext ctx = GetMemCtxFromAllocator(allocator);
    if (unlikely(IsChunkFreed(chunk) && !MemCtxIsSilent(ctx))) {
        ErrLog(ERROR, ErrMsg("%s: double free!!!", __func__));
    }

    ASSERT(!IsChunkFreed(chunk));
    ASSERT(GetMemCtxID(GetMemCtxFromAllocator(allocator)) == MCTX_STACK_ID);

    uintptr_t stackBottom = (uintptr_t)allocator + ALLOC_ALLOCTOR_HEADER_SIZE + ALLOCTOR_STAT_SIZE;
    uintptr_t stackTop = (uintptr_t)allocator->top;
    ASSERT((uintptr_t)pointer >= stackBottom);
    ASSERT((uintptr_t)pointer <= stackTop);
    if (unlikely((((uintptr_t)pointer < stackBottom) || ((uintptr_t)pointer > stackTop)) && !MemCtxIsSilent(ctx))) {
        ErrLog(ERROR, ErrMsg("restore pointer is exceed scope of this memory context, arguments invaild."));
    }

    allocator->stat.freeCount += 1;
    allocator->stat.usedChunks -= 1;
    ClearChunkCallSite(chunk);

    const char *name = MemCtxIsSilent(GetMemCtxFromAllocator(allocator)) ?
                           NULL :
                           GetMemoryContextName(GetMemCtxFromAllocator(allocator));
    UnPoisonedSentinel(pointer, chunk->requestSize);
    CheckOutOfBoundWrite(pointer, chunk->requestSize, name, chunk->file, chunk->line);
    PoisonedSentinel(pointer, chunk->requestSize); /* not release the memory, poison again */
}

static void *StackRealloc(void *pointer, size_t size, const char *file, int line)
{
    /* check pointer magic */
    ASSERT(MctxCheckChunkMagic(NULL, pointer));

    StackChunk *chunk = GetChunkFromUserPtr(pointer);
    ASSERT(chunk != NULL);
    StackAllocator *allocator = GetAllocatorFromChunk(chunk);
    ASSERT(allocator != NULL);

    if (GetAllocSize(chunk->requestSize) >= size) {
        const char *name = MemCtxIsSilent(GetMemCtxFromAllocator(allocator)) ?
                               NULL :
                               GetMemoryContextName(GetMemCtxFromAllocator(allocator));
        /* clear the poison firstly */
        ResetOutOfBoundSentinel(pointer, chunk->requestSize);
        CheckOutOfBoundWrite(pointer, chunk->requestSize, name, chunk->file, chunk->line);
        /* set the new sentinel, secondly */
        SetOutOfBoundSentinel(pointer, size);
        /* ok, this realloc is scale in, return pointer directly */
        return pointer;
    }

    StackChunk *newChunk = AllocChunk(allocator, size, file, line);
    if (newChunk == NULL) {
        return NULL;
    }
    void *newPtr = GetUserPtrFromChunk(newChunk);
    if (unlikely(memmove_s(newPtr, size, pointer, chunk->requestSize) != EOK)) {
        StackFree(newPtr);
        newPtr = NULL;
        if (!MemCtxIsSilent(GetMemCtxFromAllocator(allocator))) {
            ErrLog(ERROR, ErrMsg("stack context realloc memmove_s fail!!!"));
        }
    } else {
        SetChunkCallSite(chunk, file, line);
    }
    StackFree(pointer);
    return newPtr;
}

static void StackReset(MemCtxHdr *mctx)
{
    ASSERT(mctx != NULL);
    StackAllocator *allocator = GetAllocatorFromMemCtx(mctx);
    ASSERT(allocator != NULL);
    if (unlikely(allocator->stat.usedChunks != 0)) {
        if (!MemCtxIsSilent(mctx)) {
            ErrLog(ERROR, ErrMsg("reset stack memory context:%s with %u chunks in use", GetMemoryContextName(mctx),
                                 allocator->stat.usedChunks));
        }
    }
    /* firstly, walk all chunks to check out of bound */
    const char *name = MemCtxIsSilent(mctx) ? NULL : GetMemoryContextName(mctx);
    /* check memory and reset sentinel */
    ForwardWalkWithCheck(allocator, GetLastChunkFromAllocator(allocator), name, false);

    /* secondly, reset all manage data to init value like StackAllocatorCreate */
    allocator->stat.totalSpace = 0;
    allocator->stat.freeCount = 0;
    allocator->stat.usedChunks = 0;
    allocator->stat.lastChunkOffset = 0;
    allocator->top = (void *)((uintptr_t)allocator + ALLOC_ALLOCTOR_HEADER_SIZE + ALLOCTOR_STAT_SIZE);
}

static size_t StackGetChunkSpace(const void *pointer)
{
    ASSERT(MctxCheckChunkMagic(NULL, pointer));
    StackChunk *chunk = GetChunkFromUserPtr(pointer);
    return GetAllocSize(chunk->requestSize) + ALLOC_CHUNK_OVERHEAD;
}

static void StackCheck(MemCtxHdr *context)
{
    ASSERT(context != NULL);
    StackAllocator *allocator = GetAllocatorFromMemCtx(context);
    if (unlikely(allocator->stat.usedChunks == 0)) {
        ASSERT(allocator->stat.lastChunkOffset == 0);
        /* the memory context is empty, do nothing */
        return;
    }
    const char *name = MemCtxIsSilent(context) ? NULL : GetMemoryContextName(context);
    /* only check memory, need re-poisone the sentinel again */
    ForwardWalkWithCheck(allocator, GetLastChunkFromAllocator(allocator), name, true);
}

static void StackGetStats(const MemoryContext context, bool isPrintToLog, MemStat *stats)
{
    ASSERT(context != NULL);
    ASSERT(stats != NULL);
    StackAllocator *allocator = GetAllocatorFromMemCtx(context);
    stats->totalSpace = allocator->stat.totalSpace;
    stats->totalChunks = allocator->stat.usedChunks + allocator->stat.freeCount;
    /* freeChunks in MemStat is the count of call MemFree when is stack memory context */
    stats->freeChunks = allocator->stat.freeCount;
    if (isPrintToLog) {
        ErrLog(LOG, ErrMsg("memory context %s: allocated totalSpace=%zu, allocated totalChunks=%zu, freeCount=%zu",
                           GetMemoryContextName(context), stats->totalSpace, stats->totalChunks, stats->freeChunks));
    }
}

static void StackGetStatsByPos(const MemoryContext context, bool isPrintToLog, const char *file, int line,
                               MemStat *stats)
{
    ASSERT(context != NULL);
    ASSERT(stats != NULL);
    if (unlikely(file == NULL)) {
        return;
    }
    StackAllocator *allocator = GetAllocatorFromMemCtx(context);
    stats->totalSpace = ForwardWalkWithSummary((uintptr_t)allocator, GetLastChunkFromAllocator(allocator), file, line,
                                               &stats->totalChunks);
    if (isPrintToLog) {
        ErrLog(LOG, ErrMsg("memory context %s: [%s:%d]allocated totalSpace=%zu, allocated totalChunks=%zu",
                           GetMemoryContextName(context), file, line, stats->totalSpace, stats->totalChunks));
    }
}

static void *StackSavePoint(MemCtxHdr *mctx)
{
    StackAllocator *allocator = GetAllocatorFromMemCtx(mctx);
    ASSERT(allocator != NULL);
    /* continuous call StackSavePoint with not call Alloc will return the same top point */
    if (unlikely(IsMarkedSavePoint(allocator))) {
        SYMBOL_UNUSED StackStat *oldstat = (StackStat *)((uintptr_t)allocator->top - ALLOCTOR_STAT_SIZE);
        ASSERT(oldstat != NULL);
        ASSERT(oldstat->magic == STACK_STAT_TAG_MAGIC);
        ASSERT(oldstat->usedChunks == allocator->stat.usedChunks);
        return allocator->top; // ok, same point
    }
    /* update statistics */
    ASSERT(allocator->stat.totalSpace <= UINT32_MAX - (uint32_t)ALLOCTOR_STAT_SIZE); /* overflow */
    allocator->stat.totalSpace += (uint32_t)ALLOCTOR_STAT_SIZE;

    /* push ALLOCTOR_STAT_SIZE and record stat */
    StackStat *stat = (StackStat *)allocator->top;
    *stat = allocator->stat;
    allocator->top = (void *)((uintptr_t)allocator->top + ALLOCTOR_STAT_SIZE);
    MarkTop(allocator, STACK_STAT_TAG_MAGIC); /* set tag to mark here have a save point at before */

    return allocator->top;
}

/**
 * MemCtxHdr is inside Allocator, every user area have a StackChunk as header, and a sentinel (0x7E) as tail. if have
 * a save point, there will be a StackStat at front of save point. layout like the follow figure. the Allocator header
 * have a single link list to traverse the last chunk, from the last chunk can forward walk the whole stack chunks.
 *
 *      ______lastChunkOffset______________________________________________/---++
 *     |                                                                       ||
 *     |                         |<--- user pointer          save point --->|  ||           |<------ user pointer
 *     |                         |                                          |  \/           |
 * +---+---------+---------------+--------------+-------------+-------------+---------------+-------------+------------+
 * |  Allocator  | StackChunk 1  | user area 1  | Sentinel 1  |  StackStat  | StackChunk 2  | user area 2 | Sentinel 2 |
 * +-------------+---------_-----+--------------+-------------+-------------+--+------------+-------------+------------+
 * |--- stack grow -->>>  / \                                                  |                                       |
 *                        | |                                                  |        ---  stack top  -------------->|
 *                        |_|_________________________prevOffset_______________|
 */
static void StackRestorePoint(MemCtxHdr *mctx, void *point)
{
    StackAllocator *allocator = GetAllocatorFromMemCtx(mctx);
    ASSERT(allocator != NULL);
    /* the starting point is the same as the top initial position */
    uintptr_t topStart = (uintptr_t)allocator + (uint32_t)ALLOC_ALLOCTOR_HEADER_SIZE + (uint32_t)ALLOCTOR_STAT_SIZE;
    uintptr_t topEnd = (uintptr_t)allocator + (uint32_t)ALLOC_ALLOCTOR_EXTRA_SIZE + allocator->stat.totalSpace;
    ASSERT((uintptr_t)point >= topStart);
    ASSERT((uintptr_t)point <= topEnd);
    if (unlikely((((uintptr_t)point < topStart) || ((uintptr_t)point > topEnd)) && !MemCtxIsSilent(mctx))) {
        ErrLog(ERROR, ErrMsg("restore pointer is exceed scope of this memory context, arguments invaild."));
    }

    StackStat *stat = (StackStat *)((uintptr_t)point - ALLOCTOR_STAT_SIZE);
    ASSERT(stat != NULL);
    ASSERT(stat->magic == STACK_STAT_TAG_MAGIC);

    /* before update statistics, need do forward walk chunk for check, if updated statistics, will can't walk chunk */
    const char *name = MemCtxIsSilent(mctx) ? NULL : GetMemoryContextName(mctx);
    /* point is the first chunk we need check, so let frontEnd < point, then choose the 'stat' as frontEnd */
    /* real release the memory, don't poisoned the sentinel again */
    ForwardWalkWithCheck(stat, GetLastChunkFromAllocator(allocator), name, false);

    /* update statistics */
    allocator->stat = *stat;
    allocator->top = point;
}

UTILS_INIT_FUNC void InitStackAllocatorMethod(void)
{
    MemoryContextMethods method = {0};
    method.create = StackAllocatorCreate;
    method.alloc = StackAlloc;
    method.mfree = StackFree;
    method.realloc = StackRealloc;
    method.reset = StackReset;
    method.delete = StackAllocatorDelete;
    method.getChunkSpace = StackGetChunkSpace;
    method.getStats = StackGetStats;
    method.getStatsByPos = StackGetStatsByPos;
    method.check = StackCheck;
    method.save = StackSavePoint;
    method.restore = StackRestorePoint;
    RegisterMemoryContextMethod(MCTX_STACK_ID, &method);
}
