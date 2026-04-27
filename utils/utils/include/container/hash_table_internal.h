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
 * hash_table_internal.h
 *
 * Description:
 * 1. Hash table support different key type and dynamic expend table size
 * 2. Define key data type of Hash table
 *
 * ---------------------------------------------------------------------------------
 */

#ifndef UTILS_HASH_TABLE_INTERNAL_H
#define UTILS_HASH_TABLE_INTERNAL_H

#include "port/platform_port.h"
#include "container/hash_table.h"
#include "defines/common.h"

GSDB_BEGIN_C_CODE_DECLS

/* Number of freelists to be used for a partitioned hash table. */
#define NUM_FREELISTS 32

/* A hash bucket is a linked list of HashElements */
typedef HashElement *HashBucket;

/* A hash segment is an array of bucket headers */
typedef HashBucket *HashSegment;

/*
 * Using array of FreeListData instead of separate arrays of mutexes, nEntries
 * and freeLists prevents, at least partially, sharing one cache line between
 * different mutexes (see below).
 */
typedef struct FreeListData FreeListData;
struct FreeListData {
    SpinLock mutex;
    Size nEntries;         /* number of entries */
    HashElement *freeList; /* list of free elements */
};

typedef struct HashElementPosition HashElementPosition;
struct HashElementPosition {
    uint32_t freeListIdx;
    HashBucket currBucket;
    HashBucket *prevBucketPtr;
};

/*
 * Header structure for a hash table --- contains all changeable info
 *
 * In a shared-memory hash table, the HashHeader is in shared memory, while
 * each backend has a local HashTab struct.  For a non-shared table, there isn't
 * any functional difference between HashHeader and HashTab, but we separate them
 * anyway to share code between shared and non-shared tables.
 */

struct HashHeader {
    /*
     * The freelist can become a point of contention on high-concurrency hash
     * tables, so we use an array of freelist, each with its own mutex and
     * nEntries count, instead of just a single one.
     *
     * If hash table is not partitioned only freeList[0] is used and spinlocks
     * are not used at all.
     */
    FreeListData freeList[NUM_FREELISTS];

    /* These fields can change, but not in a partitioned table */
    /* Also, dsize can't change in a shared table, even if unpartitioned */
    Size dsize;       /* directory size */
    Size nSegs;       /* number of allocated segments (<= dsize) */
    uint32 maxBucket; /* ID of maximum bucket in use */
    uint32 highMask;  /* mask to modulo into entire table */
    uint32 lowMask;   /* mask to modulo into lower half of table */

    /* These fields are fixed at hashtable creation */
    size_t keySize;          /* hash key length in bytes */
    size_t entrySize;        /* total user element size in bytes */
    Size numPartitions;      /* # partitions (must be power of 2), or 0 */
    unsigned long ffactor;   /* target fill factor */
    Size maxDSize;           /* 'dsize' limit if directory is fixed size */
    Size ssize;              /* segment size --- must be power of 2 */
    unsigned int sshift;     /* segment shift = log2(ssize) */
    unsigned int nElemAlloc; /* number of entries to allocate at once */

#ifdef HASH_STATISTICS

    /*
     * Count statistics here.  NB: stats code doesn't bother with mutex, so
     * counts could be corrupted a bit in a partitioned table.
     */
    long accesses;
    long collisions;
#endif
};

/* the offset of the last padding if exists */
#define HASHTAB_PAD_OFFSET 104

/* Begin: Temporary solution for fix memory leak */
#define HASH_BUFFER_BLOCKS_MAX (64 * 1024)
typedef struct HashMemoryContextData *HashMemoryContext;
struct HashMemoryContextData {
    uint32_t blockCount;
    void *bufferBlock[HASH_BUFFER_BLOCKS_MAX];
};

/* End: Temporary solution for fix memory leak */

/*  */
/*
 * Top control structure for a hashtable --- in a shared table, each backend
 * has its own copy (OK since no fields change at runtime)
 */
struct HashTab {
    HashHeader *hctl;        /* => shared control information */
    HashSegment *dir;        /* directory of segment starts */
    HashValueFunc hash;      /* hash function */
    HashCompareFunc match;   /* key comparison function */
    HashCopyFunc keyCopy;    /* key copying function */
    HashAllocFunc alloc;     /* memory allocator */
    HashDeallocFunc dealloc; /* memory deallocator */
#ifdef USE_MEMORY_CONTEXT
    MemoryContext hcxt; /* memory context if default allocator used */
#else
    HashMemoryContext hcxt; /* Temporary fix bug */
#endif
    char *tabName; /* table name (for error messages) */
    bool isShared; /* true if table is in shared memory */
    bool isFixed;  /* if true, don't enlarge */

    /* freezing a shared table isn't allowed, so we can keep state here */
    bool frozen; /* true = no more inserts allowed */
    bool duringScan;

    /* We keep local copies of these fixed values to reduce contention */
    Size keySize;        /* hash key length in bytes */
    Size ssize;          /* segment size --- must be power of 2 */
    unsigned int sshift; /* segment shift = log2(ssize) */
#ifdef __aarch64__
    char pad[GS_CACHE_LINE_SIZE - HASHTAB_PAD_OFFSET];
#endif
};

extern unsigned int MyLog2(unsigned long num);

GSDB_END_C_CODE_DECLS
#endif /* UTILS_HASH_TABLE_INTERNAL_H */
