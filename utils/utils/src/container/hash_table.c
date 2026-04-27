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
 * ---------------------------------------------------------------------------------
 *
 * hash_table.c
 *
 * Description:
 * 1. Hash table support different key type and dynamic expend table size
 *
 * ---------------------------------------------------------------------------------
 */

#include <limits.h>
#include "securec.h"
#include "syslog/err_log.h"
#include "defines/utils_errorcode.h"
#include "memory/memory_ctx.h"
#include "container/hash_table_internal.h"

#ifdef LOCAL_MODULE_NAME
#undef LOCAL_MODULE_NAME
#endif
#define LOCAL_MODULE_NAME "hash_table"

/*
 * Constants
 *
 * A hash table has a top-level "directory", each of whose entries points
 * to a "segment" of ssize bucket headers.	The maximum number of hash
 * buckets is thus dsize * ssize (but dsize may be expansible).  Of course,
 * the number of records in the table can be larger, but we don't want a
 * whole lot of records per bucket or performance goes down.
 *
 * In a hash table allocated in shared memory, the directory cannot be
 * expanded because it must stay at a fixed address.  The directory size
 * should be selected using HashSelectDirSize (and you'd better have
 * a good idea of the maximum number of entries!).	For non-shared hash
 * tables, the initial directory size can be left at the default.
 */
#define DEF_SEGSIZE       256U
#define DEF_SEGSIZE_SHIFT 8U /* must be log2(DEF_SEGSIZE) */
#define DEF_DIRSIZE       256U
#define DEF_FFACTOR       1U /* default fill factor */

static inline bool IsPartitioned(const HashHeader *hctl)
{
    return (hctl)->numPartitions != 0;
}

static inline uint32_t FreeListIdx(const HashHeader *hctl, uint32_t hashcode)
{
    return (IsPartitioned(hctl) ? ((hashcode) % NUM_FREELISTS) : 0);
}

/*
 * Key (also entry) part of a HashElement
 */
static inline void *ElementKey(HashBucket helem)
{
    return (void *)(((char *)(helem)) + MAXALIGN(sizeof(HashElement)));
}

/*
 * Fast MOD arithmetic, assuming that y is a power of 2 !
 */
// clang-format off
#define MOD(x, y) ((x) & ((y) - 1))
// clang-format on

#ifdef HASH_STATISTICS
static long g_hashAccesses, g_hashCollisions, g_hashExpansions;
#endif

#define ADD_SIZE(n, delta)                                                          \
    do {                                                                            \
        Size result = (n) + (delta);                                                \
        if (result < (n) || result < (delta)) {                                     \
            ErrLog(ERROR, ErrMsg("requested shared memory size overflows size_t")); \
            return 0;                                                               \
        }                                                                           \
        (n) = result;                                                               \
    } while (0)

#define MUL_SIZE(n, delta)                                                          \
    do {                                                                            \
        Size result = (n) * (delta);                                                \
        if (((delta) != 0) && (result / (delta) != (n))) {                          \
            ErrLog(ERROR, ErrMsg("requested shared memory size overflows size_t")); \
            return 0;                                                               \
        }                                                                           \
        (n) = result;                                                               \
    } while (0)

/*
 * Private function prototypes
 */
static void *DynaHashAlloc(Size size);
static HashSegment SegAlloc(HashTab *hashp);
static bool ElementAlloc(HashTab *hashp, Size nelem, uint32_t freeListIdx);
static bool DirRealloc(HashTab *hashp);
static bool ExpandTable(HashTab *hashp);
static HashBucket GetHashEntry(HashTab *hashp, uint32_t freeListIdx);
static ErrorCode InitHashDirStructure(HashTab *hashp, Size nelem);
static ErrorCode InitHashHeader(HashTab *hashp, HashCTL *info, uint32 flags);
static void InitDefaultHashHeader(HashTab *hashp);
static unsigned int ChooseNElemAlloc(Size entrySize);
static bool InitHtab(HashTab *hashp, Size nelem);
static unsigned long NextPow2ULong(unsigned long num);
static unsigned int NextPow2UInt(unsigned long num);
static void RegisterSeqScan(HashTab *hashp);
static void DeRegisterSeqScan(HashTab *hashp);
static bool HasSeqScans(HashTab *hashp);
static void PrintHashTableInfo(HashTab *hashp);

/**
 * Current hash memory context will free all memory in destroy,
 * so we cannot free any memory during hash table live period
 */
static void HashFree(SYMBOL_UNUSED void *ptr)
{}

#ifdef USE_MEMORY_CONTEXT
static THR_LOCAL MemoryContext g_currentDynaHashCxt;

static void *DynaHashAlloc(Size size)
{
    ASSERT(MemoryContextIsValid(g_currentDynaHashCxt));
    return MemoryContextAllocEx(g_currentDynaHashCxt, size, MCTX_ALLOC_FLAG_OOM);
}

static void *DynaHashAllocNoExcept(Size size)
{
    ASSERT(MemoryContextIsValid(g_currentDynaHashCxt));

    return MemoryContextAlloc(g_currentDynaHashCxt, size);
}

static inline void DeleteHashMemoryContext(MemoryContext ctx)
{
    MemoryContextDelete(ctx);
}
#else
/* Begin: Temporary solution for fix memory leak */
static THR_LOCAL HashMemoryContext g_currentDynaHashCxt;

static HashMemoryContext CreateHashMemoryContext(void)
{
    /* It's not necessary to check return value, because if failed, system exit and restart. */
    HashMemoryContext ctx = malloc(sizeof(struct HashMemoryContextData));
    if (ctx == NULL) {
        return NULL;
    }
    ctx->blockCount = 0;
    return ctx;
}

static void *DynaHashAlloc(Size size)
{
    ASSERT(g_currentDynaHashCxt != NULL);
    ASSERT(g_currentDynaHashCxt->blockCount + 1 <= HASH_BUFFER_BLOCKS_MAX);

    void *ptr = malloc(size);
    if (ptr == NULL) {
        return NULL;
    }
    g_currentDynaHashCxt->bufferBlock[g_currentDynaHashCxt->blockCount++] = ptr;
    return ptr;
}

static void *DynaHashAllocNoExcept(Size size)
{
    return DynaHashAlloc(size);
}

static void DeleteHashMemoryContext(HashMemoryContext ctx)
{
    for (uint32_t i = 0; i < ctx->blockCount; ++i) {
        free(ctx->bufferBlock[i]);
        ctx->bufferBlock[i] = NULL;
    }
    free(ctx);
    ctx = NULL;
}
/* End: Temporary solution for fix memory leak */
#endif

/*
 * HashCompareFunc for string keys
 *
 * Because we copy keys with Strlcpy(), they will be truncated at keySize-1
 * bytes, so we can only compare that many ... hence strncmp is almost but
 * not quite the right thing.
 */
static int StringCompare(const char *key1, const char *key2, Size keySize)
{
    return strncmp(key1, key2, keySize - 1);
}

static size_t StrlcpyProc(void *dst, const void *src, size_t siz)
{
    char *d = (char *)dst;
    const char *s = (const char *)src;
    size_t n = siz;

    /* Copy as many bytes as will fit */
    if (n != 0) {
        while (--n != 0) {
            if ((*d++ = *s++) == '\0') {
                break;
            }
        }
    }

    /* Not enough room in dst, add NUL and traverse rest of src */
    if (n == 0) {
        if (siz != 0) {
            *d = '\0'; /* NUL-terminate dst */
        }
        while ((*s++) != 0) {
            ;
        }
    }
    size_t res = (size_t)(s - (const char *)src);
    return (res - 1); /* count does not include NUL */
}

static int Strlcpy(void *dst, SYMBOL_UNUSED size_t desSize, const void *src, size_t srcSize)
{
    return (int)StrlcpyProc(dst, src, srcSize);
}

#ifdef USE_MEMORY_CONTEXT
static MemoryContext SelectMemoryContext(SYMBOL_UNUSED HashCTL *info, SYMBOL_UNUSED uint32 flags, bool *newctx)
{
    ASSERT(newctx != NULL);
    MemoryContext currentDynaHashCxt;
    *newctx = false;
    if ((flags & HASH_CONTEXT) && (flags & HASH_EXTERN_CONTEXT)) {
        /*
         * HASH_CONTEXT shows info->hcxt provides extern memory context, and
         * HASH_EXTERN_CONTEXT means hash tables use info->hcxt directly, and
         * not create private memory context any more.
         *
         * HASH_EXTERN_CONTEXT is just valid with HASH_CONTEXT, and just for
         * new pooler(poolmgr.cpp)
         *
         * NOTE: hashp->hcxt must be set to NULL before HashDestroy() is called
         *       when HASH_EXTERN_CONTEXT is enabled!!!
         */
        currentDynaHashCxt = info->hcxt;
    } else {
        /* Create the hash table's private memory context */
        MemoryContext parent;
        *newctx = true;
        if (flags & HASH_CONTEXT) {
            parent = info->hcxt;
        } else {
            parent = NULL;
        }
        if (flags & HASH_SHRCTX) {
            currentDynaHashCxt = MemoryContextCreate(parent, MEM_CXT_TYPE_SHARE, tabName, MCTX_UNUSED, MCTX_UNUSED,
                                                     DEFAULT_UNLIMITED_SIZE);
        } else {
            currentDynaHashCxt = MemoryContextCreate(parent, MEM_CXT_TYPE_GENERIC, tabName, 0, DEFAULT_UNLIMITED_SIZE,
                                                     DEFAULT_UNLIMITED_SIZE);
        }
    }
    return currentDynaHashCxt;
}
#else
static HashMemoryContext SelectMemoryContext(SYMBOL_UNUSED HashCTL *info, SYMBOL_UNUSED uint32 flags, bool *newctx)
{
    ASSERT(newctx != NULL);
    *newctx = true;
    /* Begin: Temporary solution for fix memory leak */
    HashMemoryContext currentDynaHashCxt = CreateHashMemoryContext();
    /* End: Temporary solution for fix memory leak */
    return currentDynaHashCxt;
}
#endif

static HashValueFunc SelectHashFunc(HashCTL *info, uint32 flags)
{
    HashValueFunc func;
    /*
     * Select the appropriate hash function (see comments at head of file).
     */
    if ((flags & HASH_FUNCTION) != 0) {
        func = info->hash;
    } else if ((flags & HASH_BLOBS) != 0) {
        /* We can optimize hashing for common key sizes */
        ASSERT((flags & HASH_ELEM) != 0);
        if (info->keySize == sizeof(uint32)) {
            func = Uint32Hash;
        } else {
            func = TagHash;
        }
    } else {
        func = StringHash; /* default hash function */
    }
    return func;
}

static inline HashCompareFunc SelectCompFunc(HashCTL *info, uint32 flags, bool stringHash)
{
    HashCompareFunc func;
    /*
     * If you don't specify a match function, it defaults to StringCompare if
     * you used StringHash (either explicitly or by default) and to memcmp
     * otherwise.  (Prior to PostgreSQL 7.4, memcmp was always used.)
     */
    if ((flags & HASH_COMPARE) != 0) {
        func = info->match;
    } else if (stringHash) {
        func = (HashCompareFunc)StringCompare;
    } else {
        func = memcmp;
    }
    return func;
}

static inline HashCopyFunc SelectKeycopyFunc(HashCTL *info, uint32 flags, bool stringHash)
{
    HashCopyFunc func;
    /*
     * Similarly, the key-copying function defaults to Strlcpy or memcpy.
     */
    if ((flags & HASH_KEYCOPY) != 0) {
        func = info->keyCopy;
    } else if (stringHash) {
        func = (HashCopyFunc)Strlcpy;
    } else {
        func = (HashCopyFunc)memcpy_s;
    }
    return func;
}

static inline HashAllocFunc SelectAllocFunc(HashCTL *info, uint32 flags)
{
    HashAllocFunc func;
    if ((flags & HASH_ALLOC) != 0) {
        func = info->alloc;
    } else if ((flags & HASH_NOEXCEPT) != 0) {
        func = DynaHashAllocNoExcept;
    } else {
        func = DynaHashAlloc;
    }
    return func;
}

static inline HashDeallocFunc SelectDeallocFunc(HashCTL *info, uint32 flags)
{
    HashDeallocFunc func;
    if ((flags & HASH_DEALLOC) != 0) {
        func = info->dealloc;
    } else {
        func = HashFree;
    }
    return func;
}

/************************** CREATE ROUTINES **********************/

/*
 * HashCreate -- create a new dynamic hash table
 *
 *	tabName: a name for the table (for debugging purposes)
 *	nelem: maximum number of elements expected
 *	*info: additional table parameters, as indicated by flags
 *	flags: bitmask indicating which parameters to take from *info
 *
 * Note: for a shared-memory hashtable, nelem needs to be a pretty good
 * estimate, since we can't expand the table on the fly.  But an unshared
 * hashtable can be expanded on-the-fly, so it's better for nelem to be
 * on the small side and let the table grow if it's exceeded.  An overly
 * large nelem will penalize HashSeqSearch speed without buying much.
 */
UTILS_EXPORT HashTab *HashCreate(const char *tabName, Size nelem, HashCTL *info, uint32 flags)
{
    bool newctx;
    HashTab *hashp = NULL;

    g_currentDynaHashCxt = SelectMemoryContext(info, flags, &newctx);
    if (unlikely(g_currentDynaHashCxt == NULL)) {
        ErrLog(ERROR, ErrMsg("g_currentDynaHashCxt is NULL."));
        return NULL;
    }

    /* Initialize the hash header, plus a copy of the table name */
    hashp = (HashTab *)DynaHashAlloc(sizeof(HashTab) + strlen(tabName) + 1);
    if (hashp == NULL) {
        return NULL;
    }
    errno_t rc = memset_s(hashp, sizeof(HashTab), 0, sizeof(HashTab));
    if (unlikely(rc != EOK)) {
        ErrLog(ERROR, ErrMsg("Failed to memset_s HashTab, errno is %d.", rc));
        goto EXIT_HASH_CREATE;
    }

    hashp->tabName = (char *)(hashp + 1);
    rc = strcpy_s(hashp->tabName, strlen(tabName) + 1, tabName);
    if (unlikely(rc != EOK)) {
        ErrLog(ERROR, ErrMsg("Failed to strcpy_s tabName, errno is %d.", rc));
        goto EXIT_HASH_CREATE;
    }

    /*
     * Select the appropriate hash function (see comments at head of file).
     */
    hashp->hash = SelectHashFunc(info, flags);
    hashp->match = SelectCompFunc(info, flags, hashp->hash == StringHash);
    hashp->keyCopy = SelectKeycopyFunc(info, flags, hashp->hash == StringHash);
    hashp->alloc = SelectAllocFunc(info, flags);
    hashp->dealloc = SelectDeallocFunc(info, flags);

    /* setup hash table defaults */
    hashp->hctl = NULL;
    hashp->dir = NULL;
    hashp->hcxt = g_currentDynaHashCxt;
    hashp->isShared = false;

    if (!hashp->hctl && InitHashHeader(hashp, info, flags) != ERROR_SYS_OK) {
        ErrLog(ERROR, ErrMsg("Failed to init hash header."));
        goto EXIT_HASH_CREATE;
    }

    hashp->frozen = false;
    /* Make local copies of heavily-used constant fields */
    hashp->keySize = hashp->hctl->keySize;
    hashp->ssize = hashp->hctl->ssize;
    hashp->sshift = hashp->hctl->sshift;
    if ((flags & HASH_FIXED_SIZE) != 0) {
        hashp->isFixed = true;
    }
    /* Build the hash directory structure */
    if (InitHashDirStructure(hashp, nelem) != ERROR_SYS_OK) {
        ErrLog(ERROR, ErrMsg("Failed to init hash dir structure."));
        goto EXIT_HASH_CREATE;
    }

    return hashp;

EXIT_HASH_CREATE:
    if (newctx) {
        /* if is new create context, delete context once time to free all the memory alloced by it */
        DeleteHashMemoryContext(g_currentDynaHashCxt);
    }
    return NULL;
}

static ErrorCode InitHashHeader(HashTab *hashp, HashCTL *info, uint32 flags)
{
    HashHeader *hctl = NULL;
    hashp->hctl = (HashHeader *)hashp->alloc(sizeof(HashHeader));
    if (!hashp->hctl) {
        return ERROR_UTILS_COMMON_NOENOUGH_MEMORY;
    }

    InitDefaultHashHeader(hashp);

    hctl = hashp->hctl;

    if ((flags & HASH_PARTITION) != 0) {
        /* We are using treaded backends so this hash table can be
         * in Global Memory "shared" by the backends, or
         * in system Shared Memory.
         * The number of partitions had better be a power of 2. Also, it must
         * be less than INT_MAX (see InitHtab()), so call the int version of
         * next_pow2.
         */
        ASSERT(info->numPartitions == NextPow2UInt(info->numPartitions));

        hctl->numPartitions = info->numPartitions;
    }

    if ((flags & HASH_SEGMENT) != 0) {
        hctl->ssize = info->ssize;
        hctl->sshift = MyLog2(info->ssize);
        /* ssize had better be a power of 2 */
        ASSERT(hctl->ssize == (1UL << hctl->sshift));
    }
    if ((flags & HASH_FFACTOR) != 0) {
        hctl->ffactor = info->ffactor;
    }
    /*
     * SHM hash tables have fixed directory size passed by the caller.
     */
    if ((flags & HASH_DIRSIZE) != 0) {
        hctl->maxDSize = info->maxDSize;
        hctl->dsize = info->dsize;
    }

    /*
     * hash table now allocates space for key and data but you have to say how
     * much space to allocate
     */
    if ((flags & HASH_ELEM) != 0) {
        ASSERT(info->entrySize >= info->keySize);
        hctl->keySize = info->keySize;
        hctl->entrySize = info->entrySize;
    }

    return ERROR_SYS_OK;
}

/*
 * Set default HashHeader parameters.
 */
static void InitDefaultHashHeader(HashTab *hashp)
{
    HashHeader *hctl = hashp->hctl;

    (void)memset_s(hctl, sizeof(HashHeader), 0, sizeof(HashHeader));

    hctl->dsize = DEF_DIRSIZE;
    hctl->nSegs = 0;

    /* rather pointless defaults for key & entry size */
    hctl->keySize = sizeof(char *);
    hctl->entrySize = 2 * sizeof(char *);

    hctl->numPartitions = 0; /* not partitioned */

    hctl->ffactor = DEF_FFACTOR;

    /* table has no fixed maximum size */
    hctl->maxDSize = NO_MAX_DSIZE;

    hctl->ssize = DEF_SEGSIZE;
    hctl->sshift = DEF_SEGSIZE_SHIFT;

#ifdef HASH_STATISTICS
    hctl->accesses = hctl->collisions = 0;
#endif
}

static ErrorCode InitHashDirStructure(HashTab *hashp, Size nelem)
{
    if (!InitHtab(hashp, nelem)) {
        ErrLog(ERROR, ErrMsg("Failed to initialize hash table \"%s\"", hashp->tabName));
        return ERROR_UTILS_COMMON_NOENOUGH_MEMORY;
    }
    /*
     * For a shared hash table, preallocate the requested number of elements.
     * This reduces problems with run-time out-of-shared-memory conditions.
     *
     * For a non-shared hash table, preallocate the requested number of
     * elements if it's less than our chosen nElemAlloc.  This avoids wasting
     * space if the caller correctly estimates a small table size.
     */
    if (nelem < hashp->hctl->nElemAlloc) {
        unsigned int i, freeListPartitions, nElemAlloc, nElemAllocFirst;

        /*
         * If hash table is partitioned all freeLists have equal number of
         * elements. Otherwise only freeList[0] is used.
         */
        if (IsPartitioned(hashp->hctl)) {
            freeListPartitions = NUM_FREELISTS;
        } else {
            freeListPartitions = 1;
        }
        nElemAlloc = (unsigned int)(nelem / freeListPartitions);
        if (nElemAlloc == 0) {
            nElemAlloc = 1;
        }
        /* Make sure all memory will be used */
        if (nElemAlloc * freeListPartitions < nelem) {
            nElemAllocFirst = (unsigned int)(nelem - nElemAlloc * (freeListPartitions - 1));
        } else {
            nElemAllocFirst = nElemAlloc;
        }
        for (i = 0; i < freeListPartitions; i++) {
            Size temp = (i == 0) ? nElemAllocFirst : nElemAlloc;

            if (!ElementAlloc(hashp, temp, i)) {
                return ERROR_UTILS_COMMON_NOENOUGH_MEMORY;
            }
        }
    }
    return ERROR_SYS_OK;
}
/*
 * Given the user-specified entry size, choose nElemAlloc, ie, how many
 * elements to add to the hash table when we need more.
 */
static unsigned int ChooseNElemAlloc(Size entrySize)
{
    unsigned int nElemAlloc;
    Size elementSize;
    Size allocSize;

    /* Each element has a HashElement header plus user data. */
    /* NB: this had better match ElementAlloc() */
    elementSize = MAXALIGN(sizeof(HashElement)) + MAXALIGN(entrySize);

    /*
     * The idea here is to choose nElemAlloc at least 32, but round up so
     * that the allocation request will be a power of 2 or just less. This
     * makes little difference for hash tables in shared memory, but for hash
     * tables managed by palloc, the allocation request will be rounded up to
     * a power of 2 anyway.  If we fail to take this into account, we'll waste
     * as much as half the allocated space.
     */
    allocSize = 32 * 4; /* assume elementSize at least 8 */
    do {
        allocSize <<= 1;
        nElemAlloc = (unsigned int)(allocSize / elementSize);
    } while (nElemAlloc < 32);

    return nElemAlloc;
}

/*
 * Compute derived fields of hctl and build the initial directory/segment
 * arrays
 */
static bool InitHtab(HashTab *hashp, Size nelem)
{
    HashHeader *hctl = hashp->hctl;
    HashSegment *segp = NULL;
    unsigned int nBuckets;
    Size nSegs;
    int i;

    /*
     * initialize mutex if it's a partitioned table
     */
    if (IsPartitioned(hctl)) {
        for (i = 0; i < NUM_FREELISTS; i++) {
            SpinLockInit(&(hctl->freeList[i].mutex));
        }
    }
    /*
     * Divide number of elements by the fill factor to determine a desired
     * number of buckets.  Allocate space for the next greater power of two
     * number of buckets
     */
    nBuckets = NextPow2UInt((nelem - 1) / hctl->ffactor + 1);

    /*
     * In a partitioned table, nBuckets must be at least equal to
     * numPartitions; were it less, keys with apparently different partition
     * numbers would map to the same bucket, breaking partition independence.
     * (Normally nBuckets will be much bigger; this is just a safety check.)
     */
    while (nBuckets < hctl->numPartitions) {
        nBuckets <<= 1;
    }
    hctl->maxBucket = hctl->lowMask = nBuckets - 1;
    hctl->highMask = (nBuckets << 1) - 1;

    /*
     * Figure number of directory segments needed, round up to a power of 2
     */
    nSegs = (nBuckets - 1) / hctl->ssize + 1;
    nSegs = NextPow2UInt(nSegs);
    /*
     * Make sure directory is big enough. If pre-allocated directory is too
     * small, choke (caller screwed up).
     */
    if (nSegs > hctl->dsize) {
        if (!(hashp->dir)) {
            hctl->dsize = nSegs;
        } else {
            return false;
        }
    }

    /* Allocate a directory */
    if (!(hashp->dir)) {
        g_currentDynaHashCxt = hashp->hcxt;
        hashp->dir = (HashSegment *)hashp->alloc(hctl->dsize * sizeof(HashSegment));
        if (!hashp->dir) {
            return false;
        }
    }

    /* Allocate initial segments */
    for (segp = hashp->dir; hctl->nSegs < nSegs; hctl->nSegs++, segp++) {
        *segp = SegAlloc(hashp);
        if (*segp == NULL) {
            return false;
        }
    }

    /* Choose number of entries to allocate at a time */
    hctl->nElemAlloc = ChooseNElemAlloc(hctl->entrySize);
    /* For debug */
    PrintHashTableInfo(hashp);

    return true;
}

static void PrintHashTableInfo(SYMBOL_UNUSED HashTab *hashp)
{
#ifdef HASH_DEBUG
    HashHeader *hctl = hashp->hctl;
    // clang-format off
    (void)fprintf(stderr,
                  "InitHtab:\n%s%lx\n%s%lu\n%s%lu\n%s%u\n%s%lu\n%s%u\n%s%x\n%s%x\n%s%lu\n%s%lu\n",
                  "TABLE POINTER   ",
                  hashp,
                  "DIRECTORY SIZE  ",
                  hctl->dsize,
                  "SEGMENT SIZE    ",
                  hctl->ssize,
                  "SEGMENT SHIFT   ",
                  hctl->sshift,
                  "FILL FACTOR     ",
                  hctl->ffactor,
                  "MAX BUCKET      ",
                  hctl->maxBucket,
                  "HIGH MASK       ",
                  hctl->highMask,
                  "LOW  MASK       ",
                  hctl->lowMask,
                  "NSEGS           ",
                  hctl->nSegs,
                  "NENTRIES        ",
                  HashGetNumEntries(hctl));
    // clang-format on
#endif
}

/*
 * Estimate the space needed for a hashtable containing the given number
 * of entries of given size.
 * NOTE: this is used to estimate the footprint of hashtables in shared
 * memory; therefore it does not count HashTab which is in local memory.
 * NB: assumes that all hash structure parameters have default values!
 */
UTILS_EXPORT Size HashEstimateSize(Size numEntries, Size entrySize)
{
    size_t size;
    size_t nBuckets, nSegments, nElementAllocs, elementSize, nDirEntries, elementAllocCnt;

    /* estimate number of buckets wanted */
    nBuckets = NextPow2ULong((numEntries - 1) / DEF_FFACTOR + 1);
    /* # of segments needed for nBuckets */
    nSegments = NextPow2ULong((nBuckets - 1) / DEF_SEGSIZE + 1);
    /* directory entries */
    nDirEntries = DEF_DIRSIZE;
    while (nDirEntries < nSegments) {
        nDirEntries <<= 1; /* dir_alloc doubles dsize at each call */
    }
    /* fixed control info */
    size = MAXALIGN(sizeof(HashHeader)); /* but not HashTab, per above */
    /* directory */
    size_t dirSize = sizeof(HashSegment);
    MUL_SIZE(dirSize, nDirEntries);
    ADD_SIZE(size, dirSize);
    /* segments */
    size_t segSize = MAXALIGN(DEF_SEGSIZE * sizeof(HashBucket));
    MUL_SIZE(segSize, nSegments);
    ADD_SIZE(size, segSize);
    /* elements --- allocated in groups of ChooseNElemAlloc() entries */
    elementAllocCnt = ChooseNElemAlloc(entrySize);
    if (elementAllocCnt == 0) {
        ErrLog(ERROR, ErrMsg("Division by zero when calculate element numbers!"));
        return 0;
    }
    nElementAllocs = (numEntries - 1) / elementAllocCnt + 1;
    elementSize = MAXALIGN(sizeof(HashElement)) + MAXALIGN(entrySize);
    MUL_SIZE(elementSize, elementAllocCnt);
    MUL_SIZE(elementSize, nElementAllocs);
    ADD_SIZE(size, elementSize);

    return size;
}

/*
 * Select an appropriate directory size for a hashtable with the given
 * maximum number of entries.
 * This is only needed for hashtables in shared memory, whose directories
 * cannot be expanded dynamically.
 * NB: assumes that all hash structure parameters have default values!
 *
 * XXX this had better agree with the behavior of InitHtab()...
 */
UTILS_EXPORT Size HashSelectDirSize(Size numEntries)
{
    Size nBuckets, nSegments, nDirEntries;

    /* estimate number of buckets wanted */
    nBuckets = NextPow2ULong((numEntries - 1) / DEF_FFACTOR + 1);
    /* # of segments needed for nBuckets */
    nSegments = NextPow2ULong((nBuckets - 1) / DEF_SEGSIZE + 1);
    /* directory entries */
    nDirEntries = DEF_DIRSIZE;
    while (nDirEntries < nSegments) {
        nDirEntries <<= 1; /* dir_alloc doubles dsize at each call */
    }
    return nDirEntries;
}

/*
 * Compute the required initial memory allocation for a shared-memory
 * hashtable with the given parameters.  We need space for the HashHeader
 * and for the (non expansible) directory.
 */
UTILS_EXPORT Size HashGetSharedSize(const HashCTL *info, SYMBOL_UNUSED uint32 flags)
{
    ASSERT((flags & HASH_DIRSIZE) != 0);
    ASSERT(info->dsize == info->maxDSize);
    return sizeof(HashHeader) + info->dsize * sizeof(HashSegment);
}

/********************** DESTROY ROUTINES ************************/
UTILS_EXPORT void HashDestroy(HashTab *hashp)
{
    if (hashp != NULL) {
        /* allocation method must be one we know how to free, too */
        ASSERT(hashp->alloc == DynaHashAlloc || hashp->alloc == DynaHashAllocNoExcept);
        /* so this hashtable must have it's own context */
        ASSERT(hashp->hcxt != NULL);

        HashStats("destroy", hashp);
#ifdef USE_MEMORY_CONTEXT
        /*
         * Free everything by destroying the hash table's memory context.
         */
        MemoryContextDelete(hashp->hcxt);
#else
        DeleteHashMemoryContext(hashp->hcxt);
#endif
    }
}

UTILS_EXPORT void HashRemove(HashTab *hashp)
{
    if (hashp == NULL || hashp->hcxt == NULL) {
        return;
    }
    HashStats("destroy", hashp);
#ifdef USE_MEMORY_CONTEXT
    /*
     * Free everything by destroying the hash table's memory context.
     */
    MemoryContextDelete(hashp->hcxt);
#else
    DeleteHashMemoryContext(hashp->hcxt);
#endif
}

UTILS_EXPORT void HashStats(SYMBOL_UNUSED const char *where, SYMBOL_UNUSED HashTab *hashp)
{
#ifdef HASH_STATISTICS
    (void)fprintf(stderr, "%s: this HashTab -- accesses %ld collisions %ld\n", where, hashp->hctl->accesses,
                  hashp->hctl->collisions);

    (void)fprintf(stderr, "HashStats: entries %ld keySize %ld maxp %u segmentcount %ld\n", HashGetNumEntries(hashp),
                  (long)hashp->hctl->keySize, hashp->hctl->maxBucket, hashp->hctl->nSegs);
    (void)fprintf(stderr, "%s: total accesses %ld total collisions %ld\n", where, g_hashAccesses, g_hashCollisions);
    (void)fprintf(stderr, "HashStats: total expansions %ld\n", g_hashExpansions);
#endif
}

/*
 * GetHashValue -- exported routine to calculate a key's hash value
 *
 * We export this because for partitioned tables, callers need to compute
 * the partition number (from the low-order bits of the hash value) before
 * searching.
 */
UTILS_EXPORT uint32 GetHashValue(HashTab *hashp, const void *keyPtr)
{
    return hashp->hash(keyPtr, hashp->keySize);
}

/* Convert a hash value to a bucket number */
static inline uint32 CalcBucket(const HashHeader *hctl, uint32 hashVal)
{
    uint32 bucket;

    bucket = hashVal & hctl->highMask;
    if (bucket > hctl->maxBucket) {
        bucket = bucket & hctl->lowMask;
    }
    return bucket;
}

/*
 * HashSearch -- look up key in table and perform action
 * HashSearchWithHashValue -- same, with key's hash value already computed
 *
 * action is one of:
 *		HASH_FIND: look up key in table
 *		HASH_ENTER: look up key in table, creating entry if not present
 *		HASH_ENTER_NULL: same, but return NULL if out of memory
 *		HASH_REMOVE: look up key in table, remove entry if present
 *
 * Return value is a pointer to the element found/entered/removed if any,
 * or NULL if no match was found.  (NB: in the case of the REMOVE action,
 * the result is a dangling pointer that shouldn't be dereferenced!)
 *
 * HASH_ENTER will normally ereport a generic "out of memory" error if
 * it is unable to create a new entry.	The HASH_ENTER_NULL operation is
 * the same except it will return NULL if out of memory.  Note that
 * HASH_ENTER_NULL cannot be used with the default palloc-based allocator,
 * since palloc internally ereports on out-of-memory.
 *
 * If foundPtr isn't NULL, then *foundPtr is set TRUE if we found an
 * existing entry in the table, FALSE otherwise.  This is needed in the
 * HASH_ENTER case, but is redundant with the return value otherwise.
 *
 * For HashSearchWithHashValue, the hashvalue parameter must have been
 * calculated with GetHashValue().
 */
UTILS_EXPORT void *HashSearch(HashTab *hashp, const void *keyPtr, HashAction action, bool *foundPtr)
{
    if (hashp == NULL) {
        ErrLog(PANIC, ErrMsg("Hash table hashp is NULL"));
        return NULL;
    }
    return HashSearchWithHashValue(hashp, keyPtr, hashp->hash(keyPtr, hashp->keySize), action, foundPtr);
}

static void *HashRemoveElement(HashTab *hashp, HashElementPosition *position)
{
    HashHeader *hctl = hashp->hctl;
    uint32_t freeListIdx = position->freeListIdx;
    if (position->currBucket != NULL) {
        /* if partitioned, must lock to touch nEntries and freeList */
        if (IsPartitioned(hctl)) {
            SpinLockAcquire(&(hctl->freeList[freeListIdx].mutex));
        }
        ASSERT(hctl->freeList[freeListIdx].nEntries > 0);
        hctl->freeList[freeListIdx].nEntries--;

        /* remove record from hash bucket's chain. */
        *(position->prevBucketPtr) = position->currBucket->link;

        /* add the record to the freelist for this table.  */
        position->currBucket->link = hctl->freeList[freeListIdx].freeList;
        hctl->freeList[freeListIdx].freeList = position->currBucket;

        if (IsPartitioned(hctl)) {
            SpinLockRelease(&(hctl->freeList[freeListIdx].mutex));
        }

        /*
         * better hope the caller is synchronizing access to this
         * element, because someone else is going to reuse it the next
         * time something is added to the table
         */
        return (void *)ElementKey(position->currBucket);
    }
    return NULL;
}

static void *HashInsertElement(HashTab *hashp, HashElementPosition *position, const void *keyPtr, uint32 hashvalue,
                               HashAction action)
{
    HashBucket currBucket = position->currBucket;
    /* Return existing element if found, else create one */
    if (currBucket != NULL) {
        return (void *)ElementKey(currBucket);
    }

    /* disallow inserts if frozen */
    if (hashp->frozen) {
        if (hashp->alloc == DynaHashAllocNoExcept) {
            return NULL;
        }
        return NULL;
    }

    currBucket = GetHashEntry(hashp, position->freeListIdx);
    if (currBucket == NULL) {
        /* out of memory */
        if (action == HASH_ENTER_NULL) {
            return NULL;
        }

        /* libcomm permanent thread must not use elog */
        return NULL;
    }

    /* link into HashBucket chain */
    *(position->prevBucketPtr) = currBucket;
    currBucket->link = NULL;

    /* copy key into record */
    currBucket->hashValue = hashvalue;
    if (hashp->keyCopy == (HashCopyFunc)memcpy_s) {
        errno_t errorno;
        errorno = memcpy_s(ElementKey(currBucket), hashp->keySize, keyPtr, hashp->keySize);
        SecurecCheckReturn(errorno, false, NULL);
    } else {
        hashp->keyCopy(ElementKey(currBucket), hashp->keySize, keyPtr, hashp->keySize);
    }

    /*
     * Caller is expected to fill the data field on return.  DO NOT
     * insert any code that could possibly throw error here, as doing
     * so would leave the table entry incomplete and hence corrupt the
     * caller's data structure.
     */
    return (void *)ElementKey(currBucket);
}

UTILS_EXPORT void *HashSearchWithHashValue(HashTab *hashp, const void *keyPtr, uint32 hashvalue, HashAction action,
                                           bool *foundPtr)
{
    HashHeader *hctl = hashp->hctl;
    size_t keySize;
    uint32_t bucket;
    size_t segmentNum;
    size_t segmentNdx;
    HashSegment segp;
    HashBucket currBucket;
    HashBucket *prevBucketPtr = NULL;
    HashCompareFunc match = NULL;
    uint32_t freeListIdx = FreeListIdx(hctl, hashvalue);

#ifdef HASH_STATISTICS
    g_hashAccesses++;
    hctl->accesses++;
#endif

    /*
     * If inserting, check if it is time to split a bucket.
     *
     * NOTE: failure to expand table is not a fatal error, it just means we
     * have to run at higher fill factor than we wanted.  However, if we're
     * using the palloc allocator then it will throw error anyway on
     * out-of-memory, so we must do this before modifying the table.
     */
    if (action == HASH_ENTER || action == HASH_ENTER_NULL) {
        /*
         * Can't split if running in partitioned mode, nor if frozen, nor if
         * table is the subject of any active HashSeqSearch scans.  Strange
         * order of these tests is to try to check cheaper conditions first.
         */
        if (!IsPartitioned(hctl) && !hashp->frozen &&
            (hctl->freeList[0].nEntries / (hctl->maxBucket + 1) >= hctl->ffactor) && !HasSeqScans(hashp)) {
            (void)ExpandTable(hashp);
        }
    }

    /*
     * Do the initial lookup
     */
    bucket = CalcBucket(hctl, hashvalue);

    segmentNum = (size_t)(bucket >> hashp->sshift);
    segmentNdx = MOD(bucket, hashp->ssize);

    segp = hashp->dir[segmentNum];

    if (segp == NULL) {
        ErrLog(PANIC, ErrMsg("Hash table HashSegment is NULL"));
    }
    prevBucketPtr = &segp[segmentNdx];
    currBucket = *prevBucketPtr;

    /*
     * Follow collision chain looking for matching key
     */
    match = hashp->match;     /* save one fetch in inner loop */
    keySize = hashp->keySize; /* ditto */

    while (currBucket != NULL) {
        if ((currBucket->hashValue == hashvalue) && (match(ElementKey(currBucket), keyPtr, keySize) == 0)) {
            break;
        }
        prevBucketPtr = &(currBucket->link);
        currBucket = *prevBucketPtr;
#ifdef HASH_STATISTICS
        g_hashCollisions++;
        hctl->collisions++;
#endif
    }

    if (foundPtr != NULL) {
        *foundPtr = (bool)(currBucket != NULL);
    }
    switch (action) {
        case HASH_FIND: {
            if (currBucket != NULL) {
                return (void *)ElementKey(currBucket);
            }
            return NULL;
        }
        case HASH_REMOVE: {
            HashElementPosition position = {freeListIdx, currBucket, prevBucketPtr};
            return HashRemoveElement(hashp, &position);
        }
        case HASH_ENTER_NULL: {
            /* ENTER_NULL does not work with palloc-based allocator */
            ASSERT(hashp->alloc != DynaHashAlloc || hashp->alloc != DynaHashAllocNoExcept);
            /* fall through */
            __attribute__((fallthrough));
        }
        case HASH_ENTER: {
            HashElementPosition position = {freeListIdx, currBucket, prevBucketPtr};
            return HashInsertElement(hashp, &position, keyPtr, hashvalue, action);
        }
        default:
            break;
    }

    if (hashp->alloc == DynaHashAllocNoExcept) {
        ErrLog(WARNING, ErrMsg("unrecognized hash action code: %d", (int)action));
    } else {
        ErrLog(ERROR, ErrMsg("unrecognized hash action code: %d", (int)action));
    }

    return NULL; /* keep compiler quiet */
}

/*
 * create a new entry if possible
 */
static HashBucket GetHashEntry(HashTab *hashp, uint32_t freeListIdx)
{
    HashHeader *hctl = hashp->hctl;
    HashBucket newElement;
    unsigned int borrowFromIdx;

    for (;;) {
        /* if partitioned, must lock to touch nEntries and freeList */
        if (IsPartitioned(hctl)) {
            SpinLockAcquire(&hctl->freeList[freeListIdx].mutex);
        }
        /* try to get an entry from the freelist */
        newElement = hctl->freeList[freeListIdx].freeList;

        if (newElement != NULL) {
            break;
        }
        if (IsPartitioned(hctl)) {
            SpinLockRelease(&hctl->freeList[freeListIdx].mutex);
        }
        if (ElementAlloc(hashp, hctl->nElemAlloc, freeListIdx)) {
            continue;
        }
        /* no free elements.  allocate another chunk of buckets */
        if (!IsPartitioned(hctl)) {
            return NULL; /* out of memory */
        }
        /* try to borrow element from another partition */
        borrowFromIdx = freeListIdx;
        for (;;) {
            borrowFromIdx = (borrowFromIdx + 1) % NUM_FREELISTS;
            if (borrowFromIdx == freeListIdx) {
                break;
            }
            SpinLockAcquire(&(hctl->freeList[borrowFromIdx].mutex));
            newElement = hctl->freeList[borrowFromIdx].freeList;

            if (newElement != NULL) {
                hctl->freeList[borrowFromIdx].freeList = newElement->link;
                SpinLockRelease(&(hctl->freeList[borrowFromIdx].mutex));

                SpinLockAcquire(&hctl->freeList[freeListIdx].mutex);
                hctl->freeList[freeListIdx].nEntries++;
                SpinLockRelease(&hctl->freeList[freeListIdx].mutex);

                break;
            }

            SpinLockRelease(&(hctl->freeList[borrowFromIdx].mutex));
        }

        return newElement;
    }

    /* remove entry from freelist, bump nEntries */
    hctl->freeList[freeListIdx].freeList = newElement->link;
    hctl->freeList[freeListIdx].nEntries++;

    if (IsPartitioned(hctl)) {
        SpinLockRelease(&hctl->freeList[freeListIdx].mutex);
    }
    return newElement;
}

/*
 * HashGetNumEntries -- get the number of entries in a hashtable
 */
UTILS_EXPORT Size HashGetNumEntries(HashTab *hashp)
{
    int i;
    Size sum = hashp->hctl->freeList[0].nEntries;

    /*
     * We currently don't bother with the mutex; it's only sensible to call
     * this function if you've got lock on all partitions of the table.
     */
    if (!IsPartitioned(hashp->hctl)) {
        return sum;
    }
    for (i = 1; i < NUM_FREELISTS; i++) {
        sum += hashp->hctl->freeList[i].nEntries;
    }
    return sum;
}

/*
 * HashSeqInit/_search/_term
 *			Sequentially search through hash table and return
 *			all the elements one by one, return NULL when no more.
 *
 * HashSeqTerm should be called if and only if the scan is abandoned before
 * completion; if HashSeqSearch returns NULL then it has already done the
 * end-of-scan cleanup.
 *
 * NOTE: caller may delete the returned element before continuing the scan.
 * However, deleting any other element while the scan is in progress is
 * UNDEFINED (it might be the one that curIndex is pointing at!).  Also,
 * if elements are added to the table while the scan is in progress, it is
 * unspecified whether they will be visited by the scan or not.
 *
 * NOTE: it is possible to use HashSeqInit/HashSeqSearch without any
 * worry about HashSeqTerm cleanup, if the hashtable is first locked against
 * further insertions by calling HashFreeze.  This is used by nodeAgg.c,
 * wherein it is inconvenient to track whether a scan is still open, and
 * there's no possibility of further insertions after readout has begun.
 *
 * NOTE: to use this with a partitioned hashtable, caller had better hold
 * at least shared lock on all partitions of the table throughout the scan!
 * We can cope with insertions or deletions by our own backend, but *not*
 * with concurrent insertions or deletions by another.
 */
UTILS_EXPORT void HashSeqInit(HashSeqStatus *status, HashTab *hashp)
{
    status->hashp = hashp;
    status->curBucket = 0;
    status->curEntry = NULL;
    if (!hashp->frozen) {
        RegisterSeqScan(hashp);
    }
}

UTILS_EXPORT void *HashSeqSearch(HashSeqStatus *status)
{
    HashTab *hashp = NULL;
    HashHeader *hctl = NULL;
    uint32 maxBucket;
    size_t ssize;
    size_t segmentNum;
    size_t segmentNdx;
    HashSegment segp;
    uint32 curBucket;
    HashElement *curElem = NULL;

    if ((curElem = status->curEntry) != NULL) {
        /* Continuing scan of curBucket... */
        status->curEntry = curElem->link;
        if (status->curEntry == NULL) { /* end of this bucket */
            ++status->curBucket;
        }
        return (void *)ElementKey(curElem);
    }

    /*
     * Search for next nonempty bucket starting at curBucket.
     */
    curBucket = status->curBucket;
    hashp = status->hashp;
    hctl = hashp->hctl;
    ssize = hashp->ssize;
    maxBucket = hctl->maxBucket;

    if (curBucket > maxBucket) {
        HashSeqTerm(status);
        return NULL; /* search is done */
    }

    /*
     * first find the right segment in the table directory.
     */
    segmentNum = (size_t)curBucket >> hashp->sshift;
    segmentNdx = MOD(curBucket, ssize);

    segp = hashp->dir[segmentNum];

    /*
     * Pick up the first item in this bucket's chain.  If chain is not empty
     * we can begin searching it.  Otherwise we have to advance to find the
     * next nonempty bucket.  We try to optimize that case since searching a
     * near-empty hashtable has to iterate this loop a lot.
     */
    while ((curElem = segp[segmentNdx]) == NULL) {
        /* empty bucket, advance to next */
        if (++curBucket > maxBucket) {
            status->curBucket = curBucket;
            HashSeqTerm(status);
            return NULL; /* search is done */
        }
        if (++segmentNdx >= ssize) {
            segmentNum++;
            segmentNdx = 0;
            segp = hashp->dir[segmentNum];
        }
    }

    /* Begin scan of curBucket... */
    status->curEntry = curElem->link;
    if (status->curEntry == NULL) { /* end of this bucket */
        ++curBucket;
    }
    status->curBucket = curBucket;
    return (void *)ElementKey(curElem);
}

UTILS_EXPORT void HashSeqTerm(HashSeqStatus *status)
{
    if (!status->hashp->frozen) {
        DeRegisterSeqScan(status->hashp);
    }
}

/*
 * HashFreeze
 *			Freeze a hashtable against future insertions (deletions are
 *			still allowed)
 *
 * The reason for doing this is that by preventing any more bucket splits,
 * we no longer need to worry about registering HashSeqSearch scans,
 * and thus caller need not be careful about ensuring HashSeqTerm gets
 * called at the right times.
 *
 * Multiple calls to HashFreeze() are allowed, but you can't freeze a table
 * with active scans (since HashSeqTerm would then do the wrong thing).
 */
UTILS_EXPORT void HashFreeze(HashTab *hashp)
{
    hashp->frozen = true;
}

/*
 * Expand the table by adding one more hash bucket.
 */
static bool ExpandTable(HashTab *hashp)
{
    HashHeader *hctl = hashp->hctl;
    HashSegment oldSeg, newSeg;
    unsigned long oldBucket, newBucket;
    unsigned long newSegNum, newSegNdx;
    unsigned long oldSegNum, oldSegNdx;
    HashBucket *oldLink = NULL;
    HashBucket *newLink = NULL;
    HashBucket currElement, nextElement;

    ASSERT(!IsPartitioned(hctl));

#ifdef HASH_STATISTICS
    g_hashExpansions++;
#endif

    newBucket = hctl->maxBucket + 1;
    newSegNum = newBucket >> hashp->sshift;
    newSegNdx = MOD(newBucket, hashp->ssize);

    if (newSegNum >= hctl->nSegs) {
        /* Allocate new segment if necessary -- could fail if dir full */
        if (newSegNum >= hctl->dsize) {
            if (!DirRealloc(hashp)) {
                return false;
            }
        }
        if (!(hashp->dir[newSegNum] = SegAlloc(hashp))) {
            return false;
        }
        hctl->nSegs++;
    }

    /* OK, we created a new bucket */
    hctl->maxBucket++;

    /*
     * *Before* changing masks, find old bucket corresponding to same hash
     * values; values in that bucket may need to be relocated to new bucket.
     * Note that newBucket is certainly larger than low_mask at this point,
     * so we can skip the first step of the regular hash mask calc.
     */
    oldBucket = (newBucket & hctl->lowMask);

    /*
     * If we crossed a power of 2, readjust masks.
     */
    if ((uint32)newBucket > hctl->highMask) {
        hctl->lowMask = hctl->highMask;
        hctl->highMask = (uint32)newBucket | hctl->lowMask;
    }

    /*
     * Relocate records to the new bucket.	NOTE: because of the way the hash
     * masking is done in CalcBucket, only one old bucket can need to be
     * split at this point.  With a different way of reducing the hash value,
     * that might not be true!
     */
    oldSegNum = oldBucket >> hashp->sshift;
    oldSegNdx = MOD(oldBucket, hashp->ssize);

    oldSeg = hashp->dir[oldSegNum];
    newSeg = hashp->dir[newSegNum];

    oldLink = &oldSeg[oldSegNdx];
    newLink = &newSeg[newSegNdx];

    for (currElement = *oldLink; currElement != NULL; currElement = nextElement) {
        nextElement = currElement->link;
        if (CalcBucket(hctl, currElement->hashValue) == oldBucket) {
            *oldLink = currElement;
            oldLink = &currElement->link;
        } else {
            *newLink = currElement;
            newLink = &currElement->link;
        }
    }
    /* don't forget to terminate the rebuilt hash chains... */
    *oldLink = NULL;
    *newLink = NULL;

    return true;
}

static bool DirRealloc(HashTab *hashp)
{
    HashSegment *p = NULL;
    HashSegment *oldSegp = NULL;
    size_t newDSize;
    size_t oldDirSize;
    size_t newDirSize;

    if (hashp->hctl->maxDSize != NO_MAX_DSIZE) {
        return false;
    }
    /* Reallocate directory */
    newDSize = hashp->hctl->dsize << 1;
    oldDirSize = hashp->hctl->dsize * sizeof(HashSegment);
    newDirSize = newDSize * sizeof(HashSegment);

    oldSegp = hashp->dir;
    g_currentDynaHashCxt = hashp->hcxt;
    p = (HashSegment *)hashp->alloc((Size)newDirSize);
    if (p != NULL) {
        errno_t rc;

        rc = memcpy_s(p, newDirSize, oldSegp, oldDirSize);
        SecurecCheckReturn(rc, false, false, hashp->dealloc(p));
        rc = memset_s(((char *)p) + oldDirSize, newDirSize - oldDirSize, 0, newDirSize - oldDirSize);
        SecurecCheckReturn(rc, false, false, hashp->dealloc(p));
        hashp->dir = p;
        hashp->hctl->dsize = newDSize;

        hashp->dealloc(oldSegp);

        return true;
    }

    return false;
}

static HashSegment SegAlloc(HashTab *hashp)
{
    HashSegment segp;

    g_currentDynaHashCxt = hashp->hcxt;
    segp = (HashSegment)hashp->alloc(sizeof(HashBucket) * hashp->ssize);
    if (!segp) {
        return NULL;
    }
    errno_t rc = memset_s(segp, sizeof(HashBucket) * hashp->ssize, 0, sizeof(HashBucket) * hashp->ssize);
    if (rc != EOK) {
        hashp->dealloc(segp);
        return NULL;
    }
    return segp;
}

/*
 * allocate some new elements and link them into the indicated free list
 */
static bool ElementAlloc(HashTab *hashp, Size nelem, uint32_t freeListIdx)
{
    HashHeader *hctl = hashp->hctl;
    Size elementSize;
    HashElement *firstElement = NULL;
    HashElement *tmpElement = NULL;
    HashElement *prevElement = NULL;
    unsigned int i;

    if (hashp->isFixed) {
        return false;
    }
    /* Each element has a HashElement header plus user data. */
    elementSize = MAXALIGN(sizeof(HashElement)) + MAXALIGN(hctl->entrySize);

    g_currentDynaHashCxt = hashp->hcxt;
    firstElement = (HashElement *)hashp->alloc(nelem * elementSize);
    if (firstElement == NULL) {
        return false;
    }
    /* prepare to link all the new entries into the freelist */
    prevElement = NULL;
    tmpElement = firstElement;
    for (i = 0; i < nelem; i++) {
        tmpElement->link = prevElement;
        prevElement = tmpElement;
        tmpElement = (HashElement *)(((void *)tmpElement) + elementSize);
    }

    /* if partitioned, must lock to touch freeList */
    if (IsPartitioned(hctl)) {
        SpinLockAcquire(&hctl->freeList[freeListIdx].mutex);
    }
    /* freelist could be nonempty if two backends did this concurrently */
    firstElement->link = hctl->freeList[freeListIdx].freeList;
    hctl->freeList[freeListIdx].freeList = prevElement;

    if (IsPartitioned(hctl)) {
        SpinLockRelease(&hctl->freeList[freeListIdx].mutex);
    }
    return true;
}

/* calculate ceil(log base 2) of num */
unsigned int MyLog2(unsigned long num)
{
    unsigned int i;
    unsigned long limit;
    register unsigned long newNum = num;

    /* guard against too-large input, which would put us into infinite loop */
    if (num > LONG_MAX / 2) {  /* 2: LOG_BASE */
        newNum = LONG_MAX / 2; /* avoid to out of bound */
    }
    for (i = 0, limit = 1; limit < newNum; i++, limit <<= 1) {
    }
    return i;
}

/* calculate first power of 2 >= num, bounded to what will fit in a unsigned long */
static unsigned long NextPow2ULong(unsigned long num)
{
    /* MyLog2's internal range check is sufficient */
    return 1UL << MyLog2(num);
}

/* calculate first power of 2 >= num, bounded to what will fit in an unsigned int */
static unsigned int NextPow2UInt(unsigned long num)
{
    register unsigned long newNum = num;
    if (num > INT_MAX / 2) {  /* 2: LOG_BASE */
        newNum = INT_MAX / 2; /* avoid to out of bound */
    }
    return 1U << MyLog2(newNum);
}

/* Register a table as having an active HashSeqSearch scan */
static void RegisterSeqScan(HashTab *hashp)
{
    hashp->duringScan = true;
}

/* Deregister an active scan */
static void DeRegisterSeqScan(HashTab *hashp)
{
    hashp->duringScan = false;
}

/* Release the all active scan */
UTILS_EXPORT void ReleaseAllSeqScan(void)
{
    return;
}

/* Check if a table has any active scan */
static bool HasSeqScans(HashTab *hashp)
{
    return hashp->duringScan;
}

/* get current seq scan */
UTILS_EXPORT uint32_t HashGetSeqNum(void)
{
    return 0;
}

/* get current hash dynacxt */
UTILS_EXPORT MemoryContext HashGetCurrentDynacxt(void)
{
    return NULL;
}
