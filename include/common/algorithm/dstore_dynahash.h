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
 * ---------------------------------------------------------------------------------------
 *
 * IDENTIFICATION
 *        include/common/algorithm/dstore_dynahash.h
 *
 * ---------------------------------------------------------------------------------------
 */

/* copy from dynahash.h */
#ifndef DSTORE_DYNAHASH_H
#define DSTORE_DYNAHASH_H

#include <mutex>
#include <atomic>
#include "dstore_hsearch.h"
#include "errorcode/dstore_common_error_code.h"

namespace DSTORE {
/* Number of freelists to be used for a partitioned hash table. */
#define NUM_FREELISTS 1024
#define FREELIST_CTX_MAX_NAME_LEN 256
/* A hash bucket is a linked list of HASHELEMENTs */
typedef HASHELEMENT* HASHBUCKET;

/* A hash segment is an array of bucket headers */
typedef HASHBUCKET* HASHSEGMENT;

/*
 * Using array of FreeListData instead of separate arrays of mutexes, nentries and freeLists prevents,
 * at least partially, sharing one cache line between different mutexes (see below).
 */
typedef struct {
    DstoreMemoryContext freeListCxt; /* memory context for each freeList if default allocator used */
    std::mutex mutex;      /* spinlock */
    long nentries;         /* number of entries */
    HASHELEMENT* freeList; /* list of free elements */
} FreeListData;

/*
 * Header structure for a hash table --- contains all changeable info
 *
 * In a shared-memory hash table, the HASHHDR is in shared memory, while each backend has a local HTAB struct.
 * For a non-shared table, there isn't any functional difference between HASHHDR and HTAB, but we separate them
 * anyway to share code between shared and non-shared tables.
 */
struct HASHHDR {
    /*
     * The freelist can become a point of contention on high-concurrency hash tables, so we use an array of freelist,
     * each with its own mutex and nentries count, instead of just a single one.
     *
     * If hash table is not partitioned only freeList[0] is used and spinlocks are not used at all.
     */
    FreeListData freeList[NUM_FREELISTS];

    /* These fields can change, but not in a partitioned table. Also, dsize can't change in a shared table,
     * even if unpartitioned. */
    /* directory size. */
    long dsize;
    /* number of allocated segments (<= dsize). */
    long nsegs;
    /* ID of maximum bucket in use. */
    uint32 max_bucket;
    /* mask to modulo into entire table. */
    uint32 high_mask;
    /* mask to modulo into lower half of table. */
    uint32 low_mask;

    /* These fields are fixed at hashtable creation. */
    /* hash key length in bytes. */
    Size keysize;
    /* total user element size in bytes. */
    Size entrysize;
    /* # partitions (must be power of 2), or 0. */
    long num_partitions;
    /* target fill factor. */
    long ffactor;
    /* 'dsize' limit if directory is fixed size. */
    long max_dsize;
    /* segment size --- must be power of 2. */
    long ssize;
    /* segment shift = log2(ssize). */
    int sshift;
    /* number of entries to allocate at once. */
    int nelem_alloc;
    /* number of entries to allocate at once when use HASH_ENTER_PRE_ALLOC action. */
    int nelem_batch_alloc;

#ifdef HASH_STATISTICS

    /*
     * Count statistics here.
     * NB: stats code doesn't bother with mutex, so counts could be corrupted a bit in a partitioned table.
     */
    long accesses;
    long collisions;
#endif
};

/* the offset of the last padding if exists */
#define HTAB_PAD_OFFSET 101

/*
 * Top control structure for a hashtable --- in a shared table, each backend has its own copy
 * (OK since no fields change at runtime)
 */
struct HTAB {
    /* => shared control information. */
    HASHHDR* hctl;
    /* directory of segment starts. */
    HASHSEGMENT* dir;
    /* hash function. */
    HashValueFunc hash;
    /* key comparison function. */
    HashCompareFunc match;
    /* key copying function. */
    HashCopyFunc keycopy;
    /* memory allocator. */
    HashAllocFunc alloc;
    /* memory deallocator. */
    HashDeallocFunc dealloc;
    /* memory context if default allocator used. */
    DstoreMemoryContext hcxt;
    /* table name (for error messages). */
    char* tabname;

    /* 1 byte in total */
    struct {
        /* true if table is in shared memory. */
        bool isshared : 1;
        /* if true, don't enlarge. */
        bool isfixed : 1;
        /* freezing a shared table isn't allowed, so we can keep state here, true = no more inserts allowed. */
        bool frozen : 1;
        bool during_scan : 1;
        /* if hctl and dir are allocated during hash_create, used to free memory when destory hash table */
        bool is_hctl_allocated : 1;
        bool is_dir_allocated : 1;
        bool unused : 2;
    } flags;

    /* We keep local copies of these fixed values to reduce contention hash key length in bytes. */
    Size keysize;
    long ssize;
    /* segment shift = log2(ssize). */
    int sshift;
    std::atomic<uint64> cnt;

#ifdef __aarch64__
    char pad[DSTORE_CACHELINE_SIZE - HTAB_PAD_OFFSET];
#endif
};

extern int my_log2(long num);
}
#endif
