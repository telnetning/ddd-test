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

#ifndef UTILS_MEMORY_CONTEXT_INTERNAL_H
#define UTILS_MEMORY_CONTEXT_INTERNAL_H

#include <stdbool.h>
#include <unistd.h>
#include <sys/mman.h>
#include <malloc.h>
#include <pthread.h>
#include "securec.h"
#include "defines/common.h"
#include "syslog/err_log.h"
#include "lock/spinlock.h"
#include "memory/memory_context_asan.h"
#include "memory/memory_ctx.h"

/* only vaild in shared library. two ways resoved constructor in static library,
 * 1. add -Wl,--whole-archive at libxxx.a that include constructor, and add -Wl,--no-whole-archive after it
 * 2. use compile flags -Wl,-u,constructor1,-u,constructor2 to specified constructor function explicitly
 * utils library not provide static library, and try two ways above if someone static build utils in private.
 */
#define UTILS_INIT_FUNC      __attribute__((constructor))
#define CHUNK_MAGIC_NUM      0x7A7BEE
#define MCTX_NAME_LEN        0x18
#define ALLOC_ALLOCTOR_MAGIC 0xEAEA1234

#define MCTX_GENERIC_ID      0
#define MCTX_SHARE_ID        1
#define MCTX_STACK_ID        2
#define MCTX_SIMPLE_SHARE_ID 3
#define MCTX_METHOD_MAX_ID   4

/* MemCtxChunkMagic must include to specific allocator's chunk and in the last structure member */
typedef struct {
    uint32_t magicNum : 24;
    uint32_t mctxMethodID : 8;
} MemCtxChunkMagic;

/* struct MemCtxHdr* == MemoryContext */
typedef struct MemCtxHdr MemCtxHdr;

typedef struct {
    MemCtxHdr *(*create)(size_t maxUsageLimit, size_t initBlockSize, size_t maxBlockSize, size_t chunkSize);
    void *(*alloc)(MemCtxHdr *context, size_t size, const char *file, int line);
    /* call this mfree(mem free) in case someone #define's free() */
    void (*mfree)(void *pointer);
    void *(*realloc)(void *pointer, size_t size, const char *file, int line);
    void (*reset)(MemCtxHdr *context);
    void (*delete)(MemCtxHdr *context);
    void *(*save)(MemCtxHdr *context);
    void (*restore)(MemCtxHdr *context, void *point);
    MemCtxHdr *(*getChunkContext)(const void *pointer);
    size_t (*getChunkSpace)(const void *pointer);
    bool (*isEmpty)(const MemoryContext context);
    void (*getStats)(const MemoryContext context, bool isPrintToLog, MemStat *stats);
    void (*getStatsByPos)(const MemoryContext context, bool isPrintToLog, const char *file, int line, MemStat *stats);
    void (*check)(const MemoryContext context);
} MemoryContextMethods;

/* memory context header */
struct MemCtxHdr {
    MemoryContextType type;              /* identifies exact kind of context */
    const MemoryContextMethods *methods; /* virtual function table */
    MemCtxHdr *parent;                   /* NULL if no parent (toplevel context) */
    MemCtxHdr *firstChild;               /* head of linked list of children */
    MemCtxHdr *prevChild;                /* previous child of same parent, firstchild's prevchild is NULL */
    MemCtxHdr *nextChild;                /* next child of same parent */
    SpinLock childListLock;   /* spinlock for child list operation regardless of share not not share memory context */
    char name[MCTX_NAME_LEN]; /* context name */
    int32_t level;            /* context level */
    bool isReset;             /* T = no space alloced since last reset */
    bool isSilent;            /* true, not print log to errlog */
    bool isPrintSummary;
    pthread_t threadId; /* context owner thread id if memory context is not thread share */
};

static inline MemCtxChunkMagic *CheckArgs(SYMBOL_UNUSED MemCtxChunkMagic *magic, void *ptr, SYMBOL_UNUSED uint32_t id)
{
    ASSERT((uintptr_t)ptr > (uintptr_t)magic);
    ASSERT(id < MCTX_METHOD_MAX_ID);

    MemCtxChunkMagic *temp = (MemCtxChunkMagic *)((uintptr_t)ptr - sizeof(MemCtxChunkMagic));
    /* ptr - sizeof(MemCtxChunkMagic) and magic either fully overlap or not overlap at all, cannot partially overlap */
    ASSERT(((uintptr_t)temp - (uintptr_t)magic >= sizeof(MemCtxChunkMagic)) || (magic == temp));
    return temp;
}

static inline bool MemCtxIsSilent(const MemoryContext context)
{
    return context->isSilent;
}

static inline bool MemCtxIsPrintSummary(const MemoryContext context)
{
    return context->isPrintSummary;
}

static inline uint32_t GetMemCtxID(const MemoryContext context)
{
    /* the first type is MEM_CXT_TYPE_NULL, mapping to MCTX_METHOD_MAX_ID, indicates invaild ID */
    static uint32_t mctxMapTypeToId[] = {MCTX_METHOD_MAX_ID, MCTX_GENERIC_ID,      MCTX_STACK_ID,
                                         MCTX_SHARE_ID,      MCTX_SIMPLE_SHARE_ID, MCTX_METHOD_MAX_ID};
    return mctxMapTypeToId[context->type];
}

/**
 * check a chunk magic member
 *
 * @magic - the address of chunk save MemCtxChunkMagic type member
 * @ptr   - the pointer give to user
 * @return - true, check success and false, check fail
 */
static inline bool MctxCheckChunkMagic(MemCtxChunkMagic *magic, const void *ptr)
{
    bool res = false;
    if (magic != NULL) {
        res = (magic->magicNum == CHUNK_MAGIC_NUM ? true : false);
    }
    if (ptr != NULL) {
        MemCtxChunkMagic *temp = (MemCtxChunkMagic *)((uintptr_t)ptr - sizeof(MemCtxChunkMagic));
        res = (temp->magicNum == CHUNK_MAGIC_NUM ? true : false);
    }
    return res;
}

/**
 * the function must be called by allocator when success alloc/realloc a memory to set a maigc for chunk
 *
 * @magic - the address of chunk save MemCtxChunkMagic type member
 * @ptr   - the pointer give to user
 * @id    - the allocator hard code method id
 */
static inline void MctxSetChunkMagic(MemCtxChunkMagic *magic, void *ptr, uint8_t id)
{
    ASSERT(magic != NULL);
    ASSERT(ptr != NULL);

    MemCtxChunkMagic *temp = CheckArgs(magic, ptr, id);

    magic->magicNum = CHUNK_MAGIC_NUM;
    magic->mctxMethodID = id;
    if (temp != magic) {
        /* if magic not aligned, there are some pading in the MemCtxChunkMagic struct */
        temp->magicNum = CHUNK_MAGIC_NUM;
        temp->mctxMethodID = id;
    }
}

/**
 * get method id from user pointer
 *
 * @ptr   - the pointer give to user
 * @return - method id
 */
static inline uint8_t MctxChunkGetMethodID(const void *ptr)
{
    /* ptr = null or magic not CHUNK_MAGIC_NUM will abort */
    ASSERT(MctxCheckChunkMagic(NULL, ptr));

    MemCtxChunkMagic *temp = (MemCtxChunkMagic *)((uintptr_t)ptr - sizeof(MemCtxChunkMagic));
    ASSERT(temp != NULL);

    uint8_t id = temp->mctxMethodID;
    ASSERT(id < MCTX_METHOD_MAX_ID);

    return id;
}

#if defined(__SANITIZE_ADDRESS__) && defined(SANITIZE_MEMORY_CONTEXT)
/* sentinel is 8 bytes, because of asan detect grouped by 8 bytes */
typedef struct __attribute__((packed)) {
    uint64_t data;
} SentinelMagicType;

#else
/* sentinel is 2 bytes, when not in asan mode */
typedef struct __attribute__((packed)) {
    uint16_t data;
} SentinelMagicType;

#endif /* __SANITIZE_ADDRESS__ && SANITIZE_MEMORY_CONTEXT */

/* defined a kinds of sentinel magic for protect different data types are enough for current scenes to be used */
#define SENTINEL_MAGIC (uint32_t)0x7E
#define SENTINEL_LEN   sizeof(SentinelMagicType)

/**
 * set user area tail sentinel, using to judge out of bound write
 *
 * @pointer    - user pointer
 * @userSize   - user memory size
 */
static inline void SetOutOfBoundSentinel(void *pointer, size_t userSize)
{
    uint8_t *magic = (uint8_t *)((uintptr_t)pointer + userSize);
    /* the magic address may not aligned, using for loop to check the maigc, avoid system alignment exception */
    for (uint32_t i = 0; i < SENTINEL_LEN; i++) {
        magic[i] = SENTINEL_MAGIC;
    }
    /* suppose in sanitize case, user area is start at zero position, take user area less than 16 bytes as an example
     *  |0|1|2|3|4|5|6|7|8|9|10|11|12|13|14|15|
     * (1)user area finish at byte3, leave byte4 ~ byte11 as Sentinel, for 8 bytes aligned, first 0~7byte leave 4bytes
     * to set magic, PoisonedAddress will mark address:0~7 shadow to 0x4, as this 8 bytes only have 4 bytes can access.
     * the sentinel bytes address:8~11, not mark
     * (2)user area finish at byte4, PoisonedAddress will mark 0~7byte shadow to 0x5, and so on
     * (3)user area finish at byte7, leave byte8 ~ byte15 as Sentinel. The sentinel and user area in different 8bytes,
     * this case PoisonedAddress will mark address:8~15 shadow to 0xf7, state that all the 8 bytes can't access
     */
    PoisonedAddress(magic);
}

/* reset user area Sentinel poisone, when this user area pointer is freed but hope user not access the memory area */
static inline void ResetOutOfBoundSentinel(void *pointer, size_t userSize)
{
    UnPoisonedSentinel(pointer, userSize);
}

/**
 * check user area tail sentinel, judge out of bound write. But before check, need UnPoisonedSentinel if it had
 * poisoned or will report an assn error

 * @pointer    - user pointer
 * @userSize   - user memory size
 * @mctxName   - memory context name, if mctxName is NULL, deny print to errlog. callar can remove dependence on
                 error log module to prevent infinite loop at errlog memory context by set mctxName to NULL
 */
static inline void CheckOutOfBoundWrite(const void *pointer, size_t userSize, const char *mctxName,
                                        SYMBOL_UNUSED const char *file, SYMBOL_UNUSED int line)
{
    int32_t outOfBoundWriteBytes = (int32_t)SENTINEL_LEN;
    uint8_t *magic = (uint8_t *)((uintptr_t)pointer + userSize);
    /* check all the value of Sentinel's bytes is SENTINEL_MAGIC */
    /* the magic address may not aligned, using for loop to check the maigc, avoid system alignment exception */
    for (int32_t i = (int32_t)SENTINEL_LEN - 1; (i >= 0) && (magic[i] == SENTINEL_MAGIC); i--) {
        outOfBoundWriteBytes = i;
    }
    if (unlikely((outOfBoundWriteBytes != 0) && (mctxName != NULL))) {
        /* report out of bound error.
         * read magic from memory, it maybe not aligned to SENTINEL_LEN, but in this case,
         * only have once alignement exception on cpu hardware, so not too bad */
        ErrLog(ERROR,
               ErrMsg("[memory context]%s out of bound write, alloc at %s:%d. [magic:0x%lx, expect:0x%x]\n", mctxName,
                      file, line, (uint64_t)(((const SentinelMagicType *)(uintptr_t)magic)->data), SENTINEL_MAGIC));
    }
    /* generate a coredump when debug */
    ASSERT(outOfBoundWriteBytes == 0);
}

size_t GetMmapThreshold(void);

/* more than 256MB use mmap for memory context */
#define DEFAULT_MMAP_THRESHOLD ((size_t)256 * 1024 * 1024)
static inline void MctxFree(void *ptr, size_t size)
{
    if (unlikely(ptr == NULL)) {
        return;
    }
    if (size < GetMmapThreshold()) {
        free(ptr);
    } else {
        (void)munmap(ptr, size);
    }
}

static inline void *MmapWithCheck(size_t length, int prot, int flags)
{
    if (unlikely(length > ((size_t)128 * 1024 * 1024 * 1024))) { // need less than 128(*1024^3) bytes
        return NULL;
    }
    return mmap(NULL, length, prot, flags, -1, 0);
}

#define MMAP_FLAG           (MAP_PRIVATE | MAP_ANONYMOUS)
#define MMAP_HUGE_PAGE_FLAG (MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB)
static inline void *MctxMmap(size_t size, bool *useHugePage)
{
    void *ptr;
    if ((useHugePage != NULL) && *useHugePage) {
        ptr = MmapWithCheck(size, PROT_READ | PROT_WRITE, MMAP_HUGE_PAGE_FLAG);
        if (ptr != (void *)-1) {
            return ptr;
        }
        *useHugePage = (errno == ENOMEM ? false : true);
    }
    ptr = MmapWithCheck(size, PROT_READ | PROT_WRITE, MMAP_FLAG);
    return ptr == (void *)-1 ? NULL : ptr;
}

static inline void *MctxMalloc(size_t size, bool *useHugePage)
{
    if (unlikely(size == 0)) {
        return NULL;
    }
    if (size < GetMmapThreshold()) {
        return malloc(size);
    } else {
        return MctxMmap(size, useHugePage);
    }
}

static inline void *MctxRealloc(void *oldPtr, size_t oldSize, size_t newSize)
{
    void *newPtr = MctxMalloc(newSize, NULL);
    if (likely(newPtr != NULL)) {
        if (unlikely(memmove_s(newPtr, newSize, oldPtr, Min(newSize, oldSize)) != EOK)) {
            MctxFree(newPtr, newSize);
            newPtr = NULL;
        }
        MctxFree(oldPtr, oldSize);
    }
    return newPtr;
}

void RegisterMemoryContextMethod(uint32_t id, MemoryContextMethods *methods);

#endif /* UTILS_MEMORY_CONTEXT_INTERNAL_H */
