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
 * hash_table.h
 *
 * Description:
 * 1. The header file include data type and interface statement related to hash table.
 *    Using hash table , just include this header file.
 *
 * ---------------------------------------------------------------------------------
 */

#ifndef UTILS_HASH_TABLE_H
#define UTILS_HASH_TABLE_H

#include "defines/common.h"
#include "memory/memory_ctx.h"

GSDB_BEGIN_C_CODE_DECLS

/*
 * Hash functions must have this signature.
 */
typedef uint32 (*HashValueFunc)(const void *key, Size keySize);

/*
 * Key comparison functions must have this signature.  Comparison functions
 * return zero for match, nonzero for no match.  (The comparison function
 * definition is designed to allow memcmp() and strncmp() to be used directly
 * as key comparison functions.)
 */
typedef int (*HashCompareFunc)(const void *key1, const void *key2, Size keySize);

/*
 * Key copying functions must have this signature.	The return value is not
 * used.  (The definition is set up to allow memcpy_s() and strncpy() to be
 * used directly.)
 */
typedef int (*HashCopyFunc)(void *dest, size_t destSize, const void *src, Size keySize);

/*
 * Space allocation function for a hashtable --- designed to match malloc().
 * Note: there is no free function API; can't destroy a hashtable unless you
 * use the default allocator.
 */
typedef void *(*HashAllocFunc)(Size request);

/*
 * Space deallocation function for a hashtable --- designed to match allocator().
 * Note: it's a free function API; you must use it with allocator you defined.
 */
typedef void (*HashDeallocFunc)(void *pointer);

/*
 * HashElement is the private part of a hashtable entry.  The caller's data
 * follows the HashElement structure (on a MAXALIGN'd boundary).  The hash key
 * is expected to be at the start of the caller's hash entry data structure.
 */
typedef struct HashElement HashElement;
struct HashElement {
    struct HashElement *link; /* link to next entry in same bucket */
    uint32 hashValue;         /* hash function result for this entry */
};

/* Hash table header struct is an opaque type known only within dynahash.c */
typedef struct HashHeader HashHeader;

/* Hash table control struct is an opaque type known only within dynahash.c */
typedef struct HashTab HashTab;

/* Parameter data structure for HashCreate */
/* Only those fields indicated by hash_flags need be set */
typedef struct HashCTL HashCTL;
struct HashCTL {
    size_t numPartitions;    /* # partitions (must be power of 2) */
    size_t ssize;            /* segment size */
    size_t dsize;            /* (initial) directory size */
    size_t maxDSize;         /* limit to dsize if dir size is limited */
    unsigned long ffactor;   /* fill factor */
    size_t keySize;          /* hash key length in bytes */
    size_t entrySize;        /* total user element size in bytes */
    HashValueFunc hash;      /* hash function */
    HashCompareFunc match;   /* key comparison function */
    HashCopyFunc keyCopy;    /* key copying function */
    HashAllocFunc alloc;     /* memory allocator */
    HashDeallocFunc dealloc; /* memory deallocator */
    MemoryContext hcxt;      /* memory context to use for allocations */
    HashHeader *hctl;        /* location of header in shared mem */
};

/* Flags to indicate which parameters are supplied */
// clang-format off
#define HASH_PARTITION  0x001U       /* Hashtable is used w/partitioned locking */
#define HASH_SEGMENT    0x002U       /* Set segment size */
#define HASH_DIRSIZE    0x004U       /* Set directory size (initial and max) */
#define HASH_FFACTOR    0x008U       /* Set fill factor */
#define HASH_FUNCTION   0x010U       /* Set user defined hash function */
#define HASH_ELEM       0x020U       /* Set keySize and entrySize */
#define HASH_SHARED_MEM 0x040U       /* Hashtable is in shared memory */
#define HASH_ATTACH     0x080U       /* Do not initialize hctl */
#define HASH_ALLOC      0x100U       /* Set memory allocator */
#define HASH_CONTEXT    0x200U       /* Set memory allocation context */
#define HASH_COMPARE    0x400U       /* Set user defined comparison function */
#define HASH_KEYCOPY    0x800U       /* Set user defined key-copying function */
#define HASH_FIXED_SIZE 0x1000U      /* Initial size is a hard limit */
#define HASH_HEAP_MEM   0x2000U      /* Hashtable is in heap memory */
#define HASH_EXTERN_CONTEXT 0x4000U  /* use extern context directly */
#define HASH_SHRCTX     0x8000U      /* Set shared memory allocation context */
#define HASH_DEALLOC    0x10000U     /* Set memory deallocator */
#define HASH_BLOBS      0x20000U     /* Select support functions for binary keys */
#define HASH_NOEXCEPT   0x40000U     /* Do not throw exception when malloc memory */
#define HASH_PACKAGE    0x80000U     /* Set user defined hash package */
// clang-format on

/* maxDSize value to indicate expansible directory */
#define NO_MAX_DSIZE ((Size)(-1))

/* HashSearch operations */
// clang-format off
typedef enum HashAction {
    HASH_FIND,
    HASH_ENTER,
    HASH_REMOVE,
    HASH_ENTER_NULL
} HashAction;
// clang-format on

/* hash_seq status (should be considered an opaque type by callers) */
typedef struct HashSeqStatus HashSeqStatus;
struct HashSeqStatus {
    HashTab *hashp;
    uint32 curBucket;      /* index of current bucket */
    HashElement *curEntry; /* current entry in bucket */
};

/*
 * prototypes for functions in dynahash.c
 */
extern HashTab *HashCreate(const char *tabName, Size nelem, HashCTL *info, uint32 flags);
extern void HashDestroy(HashTab *hashp);
extern void HashRemove(HashTab *hashp);
extern void HashStats(const char *where, HashTab *hashp);
extern void *HashSearch(HashTab *hashp, const void *keyPtr, HashAction action, bool *foundPtr);
extern uint32 GetHashValue(HashTab *hashp, const void *keyPtr);
extern void *HashSearchWithHashValue(HashTab *hashp, const void *keyPtr, uint32 hashvalue, HashAction action,
                                     bool *foundPtr);
extern Size HashGetNumEntries(HashTab *hashp);
extern void HashSeqInit(HashSeqStatus *status, HashTab *hashp);
extern void *HashSeqSearch(HashSeqStatus *status);
extern void HashSeqTerm(HashSeqStatus *status);
extern void HashFreeze(HashTab *hashp);
extern Size HashEstimateSize(Size numEntries, Size entrySize);
extern Size HashSelectDirSize(Size numEntries);
extern Size HashGetSharedSize(const HashCTL *info, uint32 flags);
extern uint32_t HashGetSeqNum(void);
extern void ReleaseAllSeqScan(void);
extern MemoryContext HashGetCurrentDynacxt(void);
extern uint32_t HashAny(register const unsigned char *key, register int length);

/*
 * prototypes for functions in hashfn.c
 */
extern uint32_t StringHash(const void *key, size_t keySize);
extern uint32_t TagHash(const void *key, size_t keySize);
extern uint32_t Uint32Hash(const void *key, size_t keySize);
extern uint32_t OidHash(const void *key, size_t keySize);
extern int BitmapMatch(const void *key1, const void *key2, size_t keysize);
extern uint32_t BitmapHash(const void *key, size_t keysize);

GSDB_END_C_CODE_DECLS
#endif /* UTILS_HASH_TABLE_H */
