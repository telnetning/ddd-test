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
 *        include/common/algorithm/dstore_hsearch.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_HSEARCH_H
#define DSTORE_HSEARCH_H

#include "common/memory/dstore_mctx.h"
#include "common/dstore_datatype.h"
namespace DSTORE {
/*
 * Hash functions must have this signature.
 */
typedef uint32 (*HashValueFunc)(const void* key, Size keysize);

/*
 * Key comparison functions must have this signature.  Comparison functions return zero for match, nonzero for no match.
 * (The comparison function definition is designed to allow memcmp() and strncmp() to be used directly as key comparison
 * functions.)
 */
typedef int (*HashCompareFunc)(const void* key1, const void* key2, Size keysize);

/*
 * Key copying functions must have this signature.	The return value is not used.  (The definition is set up to allow
 * memcpy() and strncpy() to be used directly.)
 */
typedef void* (*HashCopyFunc)(void* dest, const void* src, Size keysize);

/*
 * Space allocation function for a hashtable --- designed to match malloc().
 * Note: there is no free function API; can't destroy a hashtable unless you use the default allocator.
 */
typedef void* (*HashAllocFunc)(Size request);

/*
 * Space deallocation function for a hashtable --- designed to match allocator().
 * Note: it's a free function API; you must use it with allocator you defined.
 */
typedef void (*HashDeallocFunc)(void* pointer);

/*
 * HASHELEMENT is the private part of a hashtable entry.  The caller's data follows the HASHELEMENT structure
 * (on a MAXALIGN'd boundary).  The hash key is expected to be at the start of the caller's hash entry data structure.
 */
typedef struct HASHELEMENT {
    struct HASHELEMENT* link; /* link to next entry in same bucket */
    uint32 hashvalue;         /* hash function result for this entry */
} HASHELEMENT;

/* Hash table control struct is an opaque type known only within dynahash.c */
typedef struct HTAB HTAB;

/* Hash table header struct is an opaque type known only within dynahash.c */
typedef struct HASHHDR HASHHDR;

/* Parameter data structure for hash_create */
/* Only those fields indicated by hash_flags need be set */
typedef struct HASHCTL {
    /* alloc fixed number of elements at a time when use HASH_ENTER_PRE_ALLOC action */
    long batch_alloc_num;
    /* # partitions (must be power of 2) */
    long num_partitions;
    /* segment size */
    long ssize;
    /* (initial) directory size */
    long dsize;
    /* limit to dsize if dir size is limited */
    long max_dsize;
    /* fill factor */
    long ffactor;
    /* hash key length in bytes */
    Size keysize;
    /* total user element size in bytes */
    Size entrysize;
    /* hash function */
    HashValueFunc hash;
    /* key comparison function */
    HashCompareFunc match;
    /* key copying function */
    HashCopyFunc keycopy;
    /* memory allocator */
    HashAllocFunc alloc;
    /* memory deallocator */
    HashDeallocFunc dealloc;
    /* memory context to use for allocations */
    DstoreMemoryContext hcxt;
    /* location of header in shared mem */
    HASHHDR* hctl;
} HASHCTL;

/* Flags to indicate which parameters are supplied */
/* Hashtable is used w/partitioned locking */
#define HASH_PARTITION 0x001
/* Set segment size */
#define HASH_SEGMENT 0x002
/* Set directory size (initial and max) */
#define HASH_DIRSIZE 0x004
/* Set fill factor */
#define HASH_FFACTOR 0x008
/* Set user defined hash function */
#define HASH_FUNCTION 0x010
/* Set keysize and entrysize */
#define HASH_ELEM 0x020
/* Hashtable is in shared memory */
#define HASH_SHARED_MEM 0x040
/* Do not initialize hctl */
#define HASH_ATTACH 0x080
/* Set memory allocator */
#define HASH_ALLOC 0x100
/* Set memory allocation context */
#define HASH_CONTEXT 0x200
/* Set user defined comparison function */
#define HASH_COMPARE 0x400
/* Set user defined key-copying function */
#define HASH_KEYCOPY 0x800
/* Initial size is a hard limit */
#define HASH_FIXED_SIZE 0x1000
/* Hashtable is in heap memory */
#define HASH_HEAP_MEM 0x2000
/* use extern context directly */
#define HASH_EXTERN_CONTEXT 0x4000
/* Set shared memory allocation context */
#define HASH_SHRCTX 0x8000
/* Set memory deallocator */
#define HASH_DEALLOC 0x10000
/* Select support functions for binary keys */
#define HASH_BLOBS 0x20000
/* Do not throw exception when malloc memory */
#define HASH_NOEXCEPT 0x40000
/* Set user defined hash package */
#define HASH_PACKAGE 0x80000
/* Set batch alloc number of elements */
#define HASH_BATCH_ALLOC_NUM 0x100000

/* max_dsize value to indicate expansible directory */
#define NO_MAX_DSIZE (-1)

/* hash_search operations */
enum HASHACTION : uint8 { HASH_FIND, HASH_ENTER, HASH_REMOVE, HASH_ENTER_NULL, HASH_ENTER_PRE_ALLOC };

/* hash_seq status (should be considered an opaque type by callers). */
typedef struct {
    HTAB* hashp;           /* hashp */
    uint32 curBucket;      /* index of current bucket */
    HASHELEMENT* curEntry; /* current entry in bucket */
} HASH_SEQ_STATUS;

/* prototypes for functions in dynahash.c */
extern uint32 get_hash_value(HTAB* hashp, const void* keyPtr);
extern HTAB* hash_create(const char* tabname, long nelem, HASHCTL* info, int flags);
extern void hash_stats(const char* where, HTAB* hashp);
extern void* hash_search(HTAB* hashp, const void* keyPtr, HASHACTION action, bool* foundPtr);
extern void hash_remove(HTAB* hashp);
extern void hash_destroy(HTAB* hashp) noexcept;
extern void* hash_search_with_hash_value(HTAB* hashp, const void* keyPtr, uint32 hashvalue, HASHACTION action,
    bool* foundPtr);
extern long hash_get_num_entries(HTAB* hashp);
extern void hash_seq_init(HASH_SEQ_STATUS* status, HTAB* hashp);
extern void* hash_seq_search(HASH_SEQ_STATUS* status);
extern void hash_seq_term(HASH_SEQ_STATUS* status);
extern int hash_get_seq_num();
extern void release_all_seq_scan();
extern DstoreMemoryContext hash_get_current_dynacxt(void);
extern long hash_select_dirsize(long num_entries);
extern Size hash_estimate_size(long num_entries, Size entrysize);
extern Size hash_get_shared_size(HASHCTL* info, int flags);
extern void hash_freeze(HTAB* hashp);
extern void AtEOXact_HashTables(bool isCommit);
extern void AtEOSubXact_HashTables(bool isCommit, int nestDepth);

/*
 * prototypes for functions in hashfn.c
 */
extern uint32 string_hash(const void* key, Size keysize);
extern uint32 tag_hash(const void* key, Size keysize);
extern uint32 buf_hash(const void* key, Size keysize);
extern uint32 uint32_hash(const void *key, Size keysize);
extern uint32 oid_hash(const void* key, Size keysize);
extern uint32 bitmap_hash(const void* key, Size keysize);
extern int bitmap_match(const void* key1, const void* key2, Size keysize);

extern Datum hash_any(const unsigned char* k, int keylen);
extern Datum hash_uint32(uint32 k);
extern uint32 hashquickany(uint32 seed, const unsigned char* data, int len);
template<HASHACTION action>
void* BufLookUp(HTAB* hashp,  const struct BufferTag* keyPtr, uint32 hashvalue, bool* foundPtr);
}
#endif /* STORAGE_HSEARCH_H */
