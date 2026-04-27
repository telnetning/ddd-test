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
#include "types/data_types.h"
#include "memory/memory_context_internal.h"

#ifdef LOCAL_MODULE_NAME
#undef LOCAL_MODULE_NAME
#endif
#define LOCAL_MODULE_NAME "memory_context"

#define ALLOCSET_NUM_FREELISTS      11
#define ALLOC_CHUNK_FRACTION        4
#define ALLOC_MINBITS               3 /* smallest chunk size is 2^3(=8) bytes */
#define ALLOC_CHUNK_LIMIT           (1 << (ALLOCSET_NUM_FREELISTS + ALLOC_MINBITS - 1))
#define GEN_BLOCK_MAGIC             0x12345678AAAABBBB
#define DEFAULT_MIN_INIT_BLOCK_SIZE 8

#define ALLOC_BLOCK_HEADER_SIZE    MAXALIGN(sizeof(GenericBlock))
#define ALLOC_CHUNK_HEADER_SIZE    MAXALIGN(sizeof(GenericChunk))
#define ALLOC_CHUNK_TAIL_SIZE      MAXALIGN(sizeof(SentinelMagicType))
#define ALLOC_ALLOCTOR_HEADER_SIZE MAXALIGN(sizeof(GenericAllocator))
#define ALLOC_CHUNK_OVERHEAD       (ALLOC_CHUNK_HEADER_SIZE + ALLOC_CHUNK_TAIL_SIZE)
/* Determine size of initial block, first block alloc minimum size at least 1 byte */
#define ALLOC_FIRST_BLOCK_SIZE \
    (ALLOC_ALLOCTOR_HEADER_SIZE + ALLOC_BLOCK_HEADER_SIZE + ALLOC_CHUNK_OVERHEAD + MAXALIGN(1))

typedef struct GenericBlock GenericBlock;
typedef struct GenericChunk GenericChunk;
typedef struct GenericAllocator GenericAllocator;

typedef union {
    GenericChunk *next;
    size_t requestedSize;
} AllocSet;

struct GenericBlock {
    MemCtxHdr *mctx;    /* owner memory context */
    GenericBlock *prev; /* prev block in GenericAllocator's blocks list, if any */
    GenericBlock *next; /* next block in GenericAllocator's blocks list */
    char *freeptr;      /* start of free_pointer space in this block,
                           when this block is not current alloc block, freeptr is noly a mark
                           to calculate free size */
    char *endptr;       /* end of space in this block */
    size_t allocSize;   /* the block alloced size, use to free */
    uint64_t magicNum;  /* use to memory check */
};

struct GenericChunk {
    GenericBlock *owner; /* owner block */
    AllocSet aset;       /* chunk in use, aset is used as actual request size; when chunk is free,
                            aset is used as next free chunk */
    size_t size;         /* chunk actual manage size */
    const char *file;
    int line;
    MemCtxChunkMagic magic; /* must be last member, will set to 0x7A7BEE | 8 bit chunk header mask */
};

struct GenericAllocator {
    uint32_t magicNum; /* 0xEAEA1234 */
    MemCtxHdr header;
    GenericBlock *blocks;
    GenericBlock *keeper;
    GenericChunk *freelist[ALLOCSET_NUM_FREELISTS];
    /* initialize argument */
    size_t initBlockSize;
    size_t maxBlockSize;
    size_t nextBlockSize;
    size_t allocChunkLimit;
    size_t maxSpaceSize; /* the context max space size alloc from OS, include the memory manage data */
    /* statistics data */
    size_t totalBlocks;
    size_t totalChunks;
    size_t freeChunks;
    size_t totalSpace;
    size_t freeSpace;
};

#define LTAB16(n) (n), (n), (n), (n), (n), (n), (n), (n), (n), (n), (n), (n), (n), (n), (n), (n)

/* Table for GetAllocSetFreeIndexBySize */
static const unsigned char LOG_TABLE256[256] = {
    0,         1,         2,         2,         3,         3,         3,         3,
    4,         4,         4,         4,         4,         4,         4,         4,
    LTAB16(5), LTAB16(6), LTAB16(6), LTAB16(7), LTAB16(7), LTAB16(7), LTAB16(7), LTAB16(8),
    LTAB16(8), LTAB16(8), LTAB16(8), LTAB16(8), LTAB16(8), LTAB16(8), LTAB16(8)};

/* look for an index of freeList by chunk size */
static inline uint32_t GetAllocSetFreeIndexBySize(size_t size)
{
    if (size > (1 << ALLOC_MINBITS)) {
        size_t tsize = (size - 1) >> ALLOC_MINBITS;

        /*
         * at this point we need to obtain log2(tsize)+1, ie, the number of not-all-zero
         * bits at the right. we used to do this with a shift-and-count loop, but this
         * function is enough of a hotspot to justify micro-optimization effort. the best
         * approach seems to be to use a lookup table. note that this code assumes that
         * ALLOCSET_NUM_FREELISTS<=11, since we only cope with two bytes of the tsize value.
         */
        size_t t = tsize >> 8;
        uint32_t idx = t ? (LOG_TABLE256[t] + (uint32_t)8) : LOG_TABLE256[tsize];

        ASSERT(idx < ALLOCSET_NUM_FREELISTS);
        return idx;
    } else {
        return 0;
    }
}

static inline MemCtxHdr *GetMemCtxFromAllocator(GenericAllocator *allocator)
{
    return &allocator->header;
}

static inline GenericAllocator *GetAllocatorFromMemCtx(MemCtxHdr *mctx)
{
    return (GenericAllocator *)((uintptr_t)mctx - offsetof(GenericAllocator, header));
}

static inline void *GetUserPtrFromChunk(GenericChunk *chunk)
{
    return (void *)((char *)chunk + ALLOC_CHUNK_HEADER_SIZE);
}

static inline GenericChunk *GetChunkFromUserPtr(const void *pointer)
{
    return (GenericChunk *)((uintptr_t)pointer - ALLOC_CHUNK_HEADER_SIZE);
}

static inline void SetChunkCallSite(GenericChunk *chunk, const char *file, int line)
{
    if (chunk == NULL) {
        ErrLog(ERROR, ErrMsg("The generic chunk in  set chunk is null!!!"));
        return;
    }
    chunk->file = file;
    chunk->line = line;
}

static inline void ClearChunkCallSite(GenericChunk *chunk)
{
    ASSERT(chunk != NULL);
    chunk->line = -1;
}

static inline bool IsChunkFreed(const GenericChunk *chunk)
{
    ASSERT(chunk != NULL);
    return chunk->line == -1;
}

static inline void PutBlockAfterHead(GenericBlock *block, GenericBlock *head)
{
    ASSERT(block != NULL);
    ASSERT(head != NULL);
    GenericBlock *after = head->next;
    head->next = block;
    block->prev = head;
    block->next = after;
    if (after != NULL) {
        after->prev = block;
    }
}

static inline void PutBlockHead(GenericBlock *block, GenericBlock **oldHead)
{
    ASSERT(block != NULL);
    ASSERT(oldHead != NULL);
    if (*oldHead == NULL) {
        ErrLog(ERROR, ErrMsg("The generic block of oldHead is null!!!"));
        return;
    }
    block->next = *oldHead;
    (*oldHead)->prev = block;
    *oldHead = block;
}

static void GenericInitBlock(GenericAllocator *allocator, GenericBlock *block, GenericBlock *head, size_t blockSize)
{
    ASSERT(block != NULL);
    /* Fill in the initial block's block header */
    block->mctx = GetMemCtxFromAllocator(allocator);
    block->prev = NULL;
    if (unlikely(head != NULL)) {
        PutBlockAfterHead(block, head);
    } else {
        block->next = NULL;
    }
    block->freeptr = ((char *)block) + ALLOC_BLOCK_HEADER_SIZE;
    block->endptr = ((char *)block) + blockSize;
    block->allocSize = blockSize;
    block->magicNum = GEN_BLOCK_MAGIC;

    /* update statistics */
    allocator->totalSpace += blockSize;
    allocator->freeSpace += (blockSize - ALLOC_BLOCK_HEADER_SIZE);
    allocator->totalBlocks += 1;
}

/* after alloc a chunk from block, you should consider to update allocator statistics */
static GenericChunk *AllocChunkFromBlock(GenericAllocator *allocator, GenericBlock *block, size_t size,
                                         bool outChunkLimit)
{
    ASSERT(block != NULL);

    size_t chunkSize;
    if (!outChunkLimit) {
        uint32_t idx = GetAllocSetFreeIndexBySize(size);
        chunkSize = (size_t)1 << (idx + ALLOC_MINBITS);
        ASSERT(idx == GetAllocSetFreeIndexBySize(chunkSize));
    } else {
        chunkSize = MAXALIGN(size);
    }

    size_t allocSize = chunkSize + ALLOC_CHUNK_OVERHEAD;
    /* aligned to freelists[x] size */
    if ((size_t)(block->endptr - block->freeptr) < allocSize) {
        return NULL;
    }
    GenericChunk *chunk = (GenericChunk *)(uintptr_t)block->freeptr;
    chunk->owner = block;
    chunk->size = chunkSize;
    chunk->aset.requestedSize = size;
    MctxSetChunkMagic(&chunk->magic, GetUserPtrFromChunk(chunk), MCTX_GENERIC_ID);

    block->freeptr += allocSize;

    /* update statistics */
    allocator->totalChunks += 1;
    allocator->freeSpace -= allocSize;

    return chunk;
}

static MemCtxHdr *GenericAllocatorCreate(size_t maxUsageLimit, size_t initBlockSize, size_t maxBlockSize,
                                         SYMBOL_UNUSED size_t chunkSize)
{
    /* verify parameters, at least allocate MAXALIGN(1) size memory. no machine have SIZE_MAX memory,
       so, no need check upper limit */
    if (unlikely((initBlockSize > maxBlockSize) || (maxUsageLimit < initBlockSize) ||
                 /* avoid (maxBlockSize - ALLOC_BLOCK_HEADER_SIZE) / ALLOC_CHUNK_FRACTION == 0 */
                 (maxBlockSize < (ALLOC_CHUNK_FRACTION + ALLOC_BLOCK_HEADER_SIZE)) ||
                 (maxUsageLimit <
                  ALLOC_ALLOCTOR_HEADER_SIZE + ALLOC_BLOCK_HEADER_SIZE + ALLOC_CHUNK_OVERHEAD + MAXALIGN(1)))) {
        (void)fprintf(stderr, "generic alloctor create fail, arguments invaild.\n");
        return NULL;
    }

    size_t initBlkSize = Max(MAXALIGN(initBlockSize), DEFAULT_MIN_INIT_BLOCK_SIZE);
    size_t firstBlockSize = Max(ALLOC_FIRST_BLOCK_SIZE, initBlkSize);

    /* Allocate the initial block. */
    GenericAllocator *allocator = (GenericAllocator *)MctxMalloc(firstBlockSize, NULL);
    if (allocator == NULL) {
        (void)fprintf(stderr, "generic alloctor create fail, %s\n", strerror(errno));
        return NULL;
    }
    /* it starts with the context header and its block header follows that. */
    /* Remember block as part of block list */
    allocator->blocks = (GenericBlock *)((uintptr_t)allocator + ALLOC_ALLOCTOR_HEADER_SIZE);
    allocator->keeper = allocator->blocks;
    (void)memset_s(allocator->freelist, sizeof(allocator->freelist), 0, sizeof(allocator->freelist));

    allocator->magicNum = ALLOC_ALLOCTOR_MAGIC;
    allocator->initBlockSize = initBlkSize;
    allocator->maxBlockSize = maxBlockSize;
    allocator->nextBlockSize = initBlkSize;
    allocator->maxSpaceSize = maxUsageLimit;
    /*
     * Compute the allocation chunk size limit for context. It can't be more than ALLOC_CHUNK_LIMIT,
     * because of the max fixed number of freelists can only express a range of sizes, such as
     * 2^1, 2^2, ..., ALLOC_CHUNK_LIMIT.
     * Only one block at least have ALLOC_CHUNK_FRACTION quantity chunks have worth to manage as a block.
     * Otherwise, it is better to regard the size of chunk as a large to engross one block, too. */
    allocator->allocChunkLimit = ALLOC_CHUNK_LIMIT;
    while ((allocator->allocChunkLimit + ALLOC_CHUNK_OVERHEAD) >
           ((maxBlockSize - ALLOC_BLOCK_HEADER_SIZE) / ALLOC_CHUNK_FRACTION)) {
        allocator->allocChunkLimit >>= 1;
    }
    ASSERT(allocator->allocChunkLimit > 0);

    allocator->totalBlocks = 0;
    allocator->totalChunks = 0;
    allocator->freeChunks = 0;
    allocator->totalSpace = 0;
    allocator->freeSpace = 0;

    GenericInitBlock(allocator, allocator->blocks, NULL, firstBlockSize - ALLOC_ALLOCTOR_HEADER_SIZE);

    return GetMemCtxFromAllocator(allocator);
}

static inline void MemoryCheckChunk(GenericChunk *chunk, const char *mctxName, bool rePoisone)
{
    ASSERT(!IsChunkFreed(chunk));
    void *userPtr = GetUserPtrFromChunk(chunk);
    UnPoisonedSentinel(userPtr, chunk->aset.requestedSize);
    CheckOutOfBoundWrite(userPtr, chunk->aset.requestedSize, mctxName, chunk->file, chunk->line);
    if (rePoisone) {
        PoisonedSentinel(userPtr, chunk->aset.requestedSize);
    }
}

static void CheckChunksInBlock(GenericBlock *block, size_t chunkLimit, const char *mctxName, bool rePoisone)
{
    GenericChunk *chunk = (GenericChunk *)((uintptr_t)block + ALLOC_BLOCK_HEADER_SIZE);
    if ((uintptr_t)chunk >= (uintptr_t)block->freeptr) {
        /* this area is not allocated to user, skip */
        return;
    }
    if (chunk->size > chunkLimit) {
        /* it's a single-chunk block, single-chunk block will not in freelist, so if not freed, it must be vaild */
        MemoryCheckChunk(chunk, mctxName, rePoisone);
        return;
    }
    while ((uintptr_t)chunk < (uintptr_t)block->freeptr) {
        if (IsChunkFreed(chunk)) {
            chunk = (GenericChunk *)((uintptr_t)chunk + chunk->size + ALLOC_CHUNK_OVERHEAD);
            continue;
        }
        MemoryCheckChunk(chunk, mctxName, rePoisone);

        chunk = (GenericChunk *)((uintptr_t)chunk + chunk->size + ALLOC_CHUNK_OVERHEAD);
    }
}

static void DoDeleteUserArea(GenericAllocator *allocator, const char *name)
{
    ASSERT(allocator != NULL);
    GenericBlock *last = allocator->blocks;
    ASSERT(last != NULL);
    ASSERT(allocator->keeper != NULL);

    for (GenericBlock *block = last->next; block != NULL; block = block->next) {
        last = block;
        if (unlikely(block->prev == allocator->keeper)) {
            /* the block is first init block, do free after */
            continue;
        }
        CheckChunksInBlock(block->prev, allocator->allocChunkLimit, name, false);
        MctxFree(block->prev, block->prev->allocSize);
    }
    if (last != allocator->keeper) {
        CheckChunksInBlock(last, allocator->allocChunkLimit, name, false);
        /* last is regular block, free it directly */
        MctxFree(last, last->allocSize);
    }
}

static void GenericAllocatorDelete(MemCtxHdr *context)
{
    ASSERT(context != NULL);
    const char *name = MemCtxIsSilent(context) ? NULL : GetMemoryContextName(context);
    GenericAllocator *allocator = GetAllocatorFromMemCtx(context);
    DoDeleteUserArea(allocator, name);
    /* free the first init block */
    /* check initial block chunk out of bound write and reset sentinel */
    CheckChunksInBlock(allocator->keeper, allocator->allocChunkLimit, name, false);
    MctxFree((char *)allocator->keeper - ALLOC_ALLOCTOR_HEADER_SIZE,
             allocator->keeper->allocSize + ALLOC_ALLOCTOR_HEADER_SIZE);
}

static inline GenericChunk *GetChunkFromFreeList(GenericAllocator *allocator, size_t size, uint32_t fidx)
{
    GenericChunk *chunk = allocator->freelist[fidx];
    if (chunk != NULL) {
        ASSERT(chunk->size >= size);
        ASSERT(fidx == GetAllocSetFreeIndexBySize(chunk->size));
        /* next and requestedSize union, before set requestedSize saving next */
        allocator->freelist[fidx] = chunk->aset.next;
        /* ovlap aset.next */
        chunk->aset.requestedSize = size;
        allocator->freeChunks -= 1;
        allocator->freeSpace -= (chunk->size + ALLOC_CHUNK_OVERHEAD);
    }
    return chunk;
}

static inline void PutChunkToFreeList(GenericAllocator *allocator, GenericChunk *chunk, uint32_t fidx)
{
    ASSERT(allocator != NULL);
    if (chunk == NULL) {
        ErrLog(ERROR, ErrMsg("The generic chunk in put chunk is null!!!"));
        return;
    }

    chunk->aset.next = allocator->freelist[fidx];
    ClearChunkCallSite(chunk);
    allocator->freelist[fidx] = chunk;
    allocator->freeChunks += 1;
    allocator->freeSpace += (chunk->size + ALLOC_CHUNK_OVERHEAD);
}

static void PutBlockFreeSpaceIntoFreelist(GenericAllocator *allocator, GenericBlock *block)
{
    ASSERT(block != NULL);
    /* split the remaining available space into different free chunks, and put into freelist */
    size_t availSpace = (size_t)(block->endptr - block->freeptr);

    size_t smallestChunk = ((1 << ALLOC_MINBITS) + ALLOC_CHUNK_OVERHEAD);
    while (availSpace >= smallestChunk) {
        size_t availChunk = availSpace - ALLOC_CHUNK_OVERHEAD;
        uint32_t fidx = GetAllocSetFreeIndexBySize(availChunk);
        /*
         * in most cases, we'll get back the index of the next larger freelist than the one we need
         * to put this chunk on. the special case is when availChunk is exactly a power of 2.
         */
        if ((fidx > 0) && (availChunk != (1UL << (fidx + ALLOC_MINBITS)))) {
            fidx--;
            availChunk = (size_t)1 << (fidx + ALLOC_MINBITS);
        }
        /* create a chunk from the block than put it into the freelist. */
        GenericChunk *chunk = AllocChunkFromBlock(allocator, block, availChunk, false);
        ASSERT(chunk != NULL);

        PutChunkToFreeList(allocator, chunk, fidx);

        availSpace -= (availChunk + ALLOC_CHUNK_OVERHEAD);
    }
}

static size_t WalkWithMemleakSummary(MemCtxHdr *context, const char *file, int line, size_t *chunkCount)
{
    ASSERT(line > 0);
    size_t summarySize = 0;
    size_t count = 0;
    GenericAllocator *allocator = GetAllocatorFromMemCtx(context);
    for (GenericBlock *block = allocator->blocks; block != NULL; block = block->next) {
        GenericChunk *chunk = (GenericChunk *)((uintptr_t)block + ALLOC_BLOCK_HEADER_SIZE);
        if ((uintptr_t)chunk >= (uintptr_t)block->freeptr) {
            /* this area is not allocated to user, skip */
            continue;
        }
        if (chunk->size > allocator->allocChunkLimit) {
            /* it's a single-chunk block, single-chunk block will not in freelist, so if not freed, it must be vaild */
            if ((chunk->file == file) && (chunk->line == line)) {
                /* find it */
                summarySize += chunk->aset.requestedSize;
                count++;
            }
            continue;
        }
        while ((uintptr_t)chunk < (uintptr_t)block->freeptr) {
            if (IsChunkFreed(chunk)) {
                /* this area had freed by user, skip */
                chunk = (GenericChunk *)((uintptr_t)chunk + chunk->size + ALLOC_CHUNK_OVERHEAD);
                continue;
            }
            if ((chunk->file == file) && (chunk->line == line)) {
                /* find it */
                summarySize += chunk->aset.requestedSize;
                count++;
            }
            chunk = (GenericChunk *)((uintptr_t)chunk + chunk->size + ALLOC_CHUNK_OVERHEAD);
        }
    }
    if (chunkCount != NULL) {
        *chunkCount = count;
    }
    return summarySize;
}

static GenericBlock *GetAllocBlock(GenericAllocator *allocator, SYMBOL_UNUSED size_t size, uint32_t fidx,
                                   SYMBOL_UNUSED const char *file, SYMBOL_UNUSED int line)
{
    /* choose the actual chunk size to allocate. */
    size_t chunkSize = (1UL << ALLOC_MINBITS) << fidx;
    ASSERT(chunkSize >= size);

    /*
     * if there is enough room in the current active allocation block, we will put the
     * chunk into that block. else must start a new one.
     */
    GenericBlock *current = allocator->blocks;
    if (current != NULL) {
        size_t availSpace = (size_t)(current->endptr - current->freeptr);
        if (availSpace < (chunkSize + ALLOC_CHUNK_OVERHEAD)) {
            PutBlockFreeSpaceIntoFreelist(allocator, current);
            /* mark that we need to create a new block */
            current = NULL;
        }
    }
    /* time to create a new regular (multi-chunk) block? */
    if (current == NULL) {
        /*
         * The first such block has size initBlockSize, and we double the
         * space in each succeeding block, but not more than maxBlockSize.
         */
        size_t blockSize = allocator->nextBlockSize;
        allocator->nextBlockSize <<= 1;
        if (allocator->nextBlockSize > allocator->maxBlockSize) {
            allocator->nextBlockSize = allocator->maxBlockSize;
        }
        size_t requiredSize = chunkSize + ALLOC_BLOCK_HEADER_SIZE + ALLOC_CHUNK_OVERHEAD;
        ASSERT(blockSize > 0);
        /* when the block size is smaller than the size of this chunk request, always expanded by pow of 2 */
        while (blockSize < requiredSize) {
            blockSize <<= 1;
        }

        if (unlikely(allocator->totalSpace + blockSize + ALLOC_ALLOCTOR_HEADER_SIZE > allocator->maxSpaceSize)) {
            /* excees maxSpaceSize */
            MemoryContext ctx = GetMemCtxFromAllocator(allocator);
            if (MemCtxIsSilent(ctx)) {
                return NULL;
            }
            /* traverse may spend too much time, so only print total statistics */
            ErrLog(ERROR, ErrMsg("memory context %s, %s:%d alloc %zu(%zu), exceed %zu, statistics: totalBlocks="
                                 "%zu, totalChunks=%zu, freeChunks=%zu, totalSpace=%zu, freeSpace=%zu",
                                 GetMemoryContextName(ctx), file, line, size, blockSize, allocator->maxSpaceSize,
                                 allocator->totalBlocks, allocator->totalChunks, allocator->freeChunks,
                                 allocator->totalSpace, allocator->freeSpace));
            if (MemCtxIsPrintSummary(ctx)) {
                /* only print the current allocate location summary, ofcourse the most
                 * best result is to print all the allocate location */
                ErrLog(ERROR, ErrMsg("memory context %s, %s:%d total alloc %zu", GetMemoryContextName(ctx), file, line,
                                     WalkWithMemleakSummary(ctx, file, line, NULL)));
            }
            return NULL;
        }
        /* Try to allocate it */
        current = (GenericBlock *)MctxMalloc(blockSize, NULL);
        if (current == NULL) {
            return NULL;
        }
        GenericInitBlock(allocator, current, NULL, blockSize);
        PutBlockHead(current, &allocator->blocks);
    }
    return current;
}

static void *GenericAlloc(MemCtxHdr *mctx, size_t size, const char *file, int line)
{
    ASSERT(mctx != NULL);
    void *userPtr;
    GenericChunk *chunk;
    GenericBlock *block;

    if (size == 0) {
        return NULL;
    }
    GenericAllocator *allocator = GetAllocatorFromMemCtx(mctx);
    /* If required size is greater than m_alloc_chunk_limit, create a new block to put the chunk in */
    /* this new block will be a single-chunk block */
    if (size > allocator->allocChunkLimit) {
        size_t blockSize = MAXALIGN(size) + ALLOC_BLOCK_HEADER_SIZE + ALLOC_CHUNK_OVERHEAD;
        if (unlikely((allocator->totalSpace + blockSize + ALLOC_ALLOCTOR_HEADER_SIZE > allocator->maxSpaceSize) ||
                     (blockSize < size))) { /* excees maxSpaceSize or overflow */
            if (MemCtxIsSilent(mctx)) {
                return NULL;
            }
            /* traverse may spend too much time, so only print total statistics */
            ErrLog(ERROR, ErrMsg("memory context %s, %s:%d alloc %zu(%zu), exceed %zu, statistics: totalBlocks="
                                 "%zu, totalChunks=%zu, freeChunks=%zu, totalSpace=%zu, freeSpace=%zu",
                                 GetMemoryContextName(mctx), file, line, size, blockSize, allocator->maxSpaceSize,
                                 allocator->totalBlocks, allocator->totalChunks, allocator->freeChunks,
                                 allocator->totalSpace, allocator->freeSpace));
            if (MemCtxIsPrintSummary(mctx)) {
                /* only print the current allocate location summary, ofcourse the most
                 * best result is to print all the allocate location */
                ErrLog(ERROR, ErrMsg("memory context %s, %s:%d total alloc %zu", GetMemoryContextName(mctx), file, line,
                                     WalkWithMemleakSummary(mctx, file, line, NULL)));
            }
            return NULL;
        }

        block = (GenericBlock *)MctxMalloc(blockSize, NULL);
        if (block == NULL) {
            return NULL;
        }
        GenericInitBlock(allocator, block, allocator->blocks, blockSize);
        chunk = AllocChunkFromBlock(allocator, block, size, true);
        ASSERT(chunk != NULL);
        SetChunkCallSite(chunk, file, line);

        /* block not null, chunk not null surely */
        userPtr = GetUserPtrFromChunk(chunk);
        SetOutOfBoundSentinel(userPtr, size);
        return userPtr;
    }

    uint32_t fidx = GetAllocSetFreeIndexBySize(size);
    /* Try to find a free chunk, free chunk all in allocator's freelist */
    chunk = GetChunkFromFreeList(allocator, size, fidx);
    if (chunk != NULL) {
        SetChunkCallSite(chunk, file, line);
        userPtr = GetUserPtrFromChunk(chunk);
        SetOutOfBoundSentinel(userPtr, size);
        return userPtr;
    }

    /* Failed to find a chunk in freelist, now try to alloc a new chunk from block */
    block = GetAllocBlock(allocator, size, fidx, file, line);
    if (block == NULL) {
        return NULL;
    }
    /* OK, do the allocation */
    chunk = AllocChunkFromBlock(allocator, block, size, false);
    ASSERT(chunk != NULL);
    SetChunkCallSite(chunk, file, line);

    /* block not null, chunk not null surely */
    userPtr = GetUserPtrFromChunk(chunk);
    SetOutOfBoundSentinel(userPtr, size);
    return userPtr;
}

static void GenericFree(void *pointer)
{
    ASSERT(MctxCheckChunkMagic(NULL, pointer));

    GenericChunk *chunk = GetChunkFromUserPtr(pointer);
    ASSERT(chunk != NULL);

    GenericBlock *block = chunk->owner;
    ASSERT(block != NULL);
    ASSERT(block->mctx != NULL);
    ASSERT(GetMemCtxID(block->mctx) == MCTX_GENERIC_ID);

    GenericAllocator *allocator = GetAllocatorFromMemCtx(block->mctx);
    ASSERT(allocator != NULL);

    UnPoisonedSentinel(GetUserPtrFromChunk(chunk), chunk->aset.requestedSize);
    const char *name = MemCtxIsSilent(block->mctx) ? NULL : GetMemoryContextName(block->mctx);
    CheckOutOfBoundWrite(GetUserPtrFromChunk(chunk), chunk->aset.requestedSize, name, chunk->file, chunk->line);

    if (chunk->size > allocator->allocChunkLimit) {
        /* free to OS, update statistics */
        allocator->totalBlocks -= 1;
        allocator->totalChunks -= 1;
        allocator->totalSpace -= block->allocSize;

        GenericBlock *last = NULL;
        GenericBlock *blk = allocator->blocks;
        for (; blk != NULL; blk = blk->next) {
            if (likely(blk != block)) {
                last = blk;
                continue;
            }
            if (last == NULL) {
                ErrLog(ERROR, ErrMsg("The generic block of last is null!!!"));
                return;
            }
            /* remove block from blocks list */
            last->next = blk->next;
            if (blk->next != NULL) {
                blk->next->prev = last;
            }
            break;
        }

        ASSERT(blk != NULL);
        MctxFree(block, block->allocSize);
    } else {
        PutChunkToFreeList(allocator, chunk, GetAllocSetFreeIndexBySize(chunk->size));
    }
}

static void *GenericRealloc(void *pointer, size_t size, const char *file, int line)
{
    ASSERT(MctxCheckChunkMagic(NULL, pointer));

    GenericChunk *chunk = GetChunkFromUserPtr(pointer);
    ASSERT(chunk != NULL);
    GenericBlock *block = chunk->owner;
    ASSERT(block != NULL);
    if (chunk->size >= size) {
        ASSERT(block->mctx != NULL);
        ResetOutOfBoundSentinel(pointer, chunk->aset.requestedSize);
        const char *name = MemCtxIsSilent(block->mctx) ? NULL : GetMemoryContextName(block->mctx);
        CheckOutOfBoundWrite(pointer, chunk->aset.requestedSize, name, chunk->file, chunk->line);
        SetOutOfBoundSentinel(pointer, size);

        chunk->aset.requestedSize = size;
        /* ok, the chunk have enough space */
        return pointer;
    }

    MemCtxHdr *mctx = block->mctx;
    void *newPtr = GenericAlloc(mctx, size, file, line);
    if (newPtr != NULL) {
        if (unlikely(memmove_s(newPtr, size, pointer, chunk->aset.requestedSize) != EOK)) {
            GenericFree(newPtr);
            newPtr = NULL;
            ErrLog(ERROR, ErrMsg("generic realloc memmove_s fail!!!"));
        }
        GenericFree(pointer);
    }
    return newPtr;
}

static void GenericReset(MemCtxHdr *context)
{
    ASSERT(context != NULL);
    const char *name = MemCtxIsSilent(context) ? NULL : GetMemoryContextName(context);
    GenericAllocator *allocator = GetAllocatorFromMemCtx(context);
    DoDeleteUserArea(allocator, name);
    /* check initial block chunk out of bound write and reset sentinel */
    CheckChunksInBlock(allocator->keeper, allocator->allocChunkLimit, name, false);
    /* reset allocator data like GenericAllocatorCreate */
    allocator->totalBlocks = 0;
    allocator->totalChunks = 0;
    allocator->freeChunks = 0;
    allocator->totalSpace = 0;
    allocator->freeSpace = 0;
    allocator->blocks = allocator->keeper;

    /* Determine size of initial block */
    size_t firstBlockSize = Max(ALLOC_FIRST_BLOCK_SIZE, allocator->initBlockSize);
    GenericInitBlock(allocator, allocator->blocks, NULL, firstBlockSize - ALLOC_ALLOCTOR_HEADER_SIZE);
}

static void GenericCheck(MemCtxHdr *context)
{
    ASSERT(context != NULL);
    const char *name = MemCtxIsSilent(context) ? NULL : GetMemoryContextName(context);
    GenericAllocator *allocator = GetAllocatorFromMemCtx(context);
    /* if there are no chunks */
    if (unlikely(allocator->totalChunks == 0)) {
        return;
    }
    for (GenericBlock *block = allocator->blocks; block != NULL; block = block->next) {
        ASSERT(context == block->mctx);
        CheckChunksInBlock(block, allocator->allocChunkLimit, name, true);
    }
}

static size_t GenericGetChunkSpace(const void *pointer)
{
    ASSERT(MctxCheckChunkMagic(NULL, pointer));

    GenericChunk *chunk = GetChunkFromUserPtr(pointer);
    ASSERT(chunk != NULL);
    return chunk->size + ALLOC_CHUNK_OVERHEAD;
}

static void GenericGetStats(const MemoryContext context, bool isPrintToLog, MemStat *stats)
{
    ASSERT(context != NULL);
    ASSERT(stats != NULL);
    GenericAllocator *allocator = GetAllocatorFromMemCtx(context);
    stats->totalBlocks = allocator->totalBlocks;
    stats->totalChunks = allocator->totalChunks;
    stats->freeChunks = allocator->freeChunks;
    stats->totalSpace = allocator->totalSpace;
    stats->freeSpace = allocator->freeSpace;
    if (isPrintToLog) {
        ErrLog(LOG, ErrMsg("memory context %s: allocated totalSpace=%zu, allocated totalChunks=%zu, allocated "
                           "totalBlocks=%zu, freeSpace=%zu, freeChunks=%zu",
                           GetMemoryContextName(context), stats->totalSpace, stats->totalChunks, stats->totalBlocks,
                           stats->freeSpace, stats->freeChunks));
    }
}

static void GenericGetStatsByPos(const MemoryContext context, bool isPrintToLog, const char *file, int line,
                                 MemStat *stats)
{
    ASSERT(context != NULL);
    ASSERT(stats != NULL);
    if (unlikely(file == NULL)) {
        return;
    }

    stats->totalSpace = WalkWithMemleakSummary(context, file, line, &stats->totalChunks);
    if (isPrintToLog) {
        ErrLog(LOG, ErrMsg("memory context %s: [%s:%d]allocated totalSpace=%zu, allocated totalChunks=%zu",
                           GetMemoryContextName(context), file, line, stats->totalSpace, stats->totalChunks));
    }
}

UTILS_INIT_FUNC void InitGenericAllocatorMethod(void)
{
    MemoryContextMethods method = {0};
    method.create = GenericAllocatorCreate;
    method.alloc = GenericAlloc;
    method.mfree = GenericFree;
    method.realloc = GenericRealloc;
    method.reset = GenericReset;
    method.delete = GenericAllocatorDelete;
    method.getChunkSpace = GenericGetChunkSpace;
    method.getStats = GenericGetStats;
    method.getStatsByPos = GenericGetStatsByPos;
    method.check = GenericCheck;
    RegisterMemoryContextMethod(MCTX_GENERIC_ID, &method);
}
