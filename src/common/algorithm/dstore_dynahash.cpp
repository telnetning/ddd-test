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
 *        src/common/algorithm/dstore_dynahash.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

/* copy from dynamic_hash.cpp */

#include "common/algorithm/dstore_dynahash.h"
#include "framework/dstore_thread.h"
#include "buffer/dstore_buf.h"
#include "fault_injection/fault_injection.h"
#include "securec.h"
namespace DSTORE {

/*
 * Constants
 *
 * A hash table has a top-level "directory", each of whose entries points to a "segment" of ssize bucket headers.
 * The maximum number of hash buckets is thus dsize * ssize (but dsize may be expansible).
 * Of course, the number of records in the table can be larger, but we don't want a whole lot of records per bucket
 * or performance goes down.
 *
 * In a hash table allocated in shared memory, the directory cannot be expanded because it must stay at a fixed address.
 * The directory size should be selected using hash_select_dirsize (and you'd better have a good idea of the maximum
 * number of entries!). For non-shared hash tables, the initial directory size can be left at the default.
 */
#define DEF_FFACTOR 1 /* default fill factor */
#define DEF_SEGSIZE_SHIFT 8 /* must be log2(DEF_SEGSIZE) */
#define DEF_SEGSIZE 256
#define DEF_DIRSIZE 256

#define IS_PARTITIONED(hctl) ((hctl)->num_partitions != 0)
#define FREELIST_IDX(hctl, hashcode) (IS_PARTITIONED(hctl) ? ((hashcode) % NUM_FREELISTS) : 0)

/*
 * Fast MOD arithmetic, assuming that y is a power of 2 !
 */
#define MOD(x, y) ((x) & ((y) - 1))

/*
 * Key (also entry) part of a HASHELEMENT
 */
#define ELEMENTKEY(helem) (((char*)(helem)) + MAXALIGN(sizeof(HASHELEMENT)))

#ifdef HASH_STATISTICS
#if HASH_STATISTICS
static long hash_accesses, hash_collisions, hash_expansions;
#endif
#endif  // ifdef HASH_STATISTICS

Size add_size(Size s1, Size s2)
{
    Size result = s1 + s2;

    /* We are assuming Size is an unsigned type here... */
    if (result < s1 || result < s2) {
        storage_set_error(HASH_ERROR_MEMORY_SIZE_OVERFLOW);
        StorageAssert(0);
    }
    return result;
}

/*
 * Multiply two Size values, checking for overflow
 */
Size mul_size(Size s1, Size s2)
{
    if (s1 == 0 || s2 == 0) {
        return 0;
    }

    Size result = s1 * s2;

    /* We are assuming Size is an unsigned type here... */
    if (result / s2 != s1) {
        storage_set_error(HASH_ERROR_MEMORY_SIZE_OVERFLOW);
        StorageAssert(0);
    }
    return result;
}
/*
 * Private function prototypes
 */
static void* DynaHashAlloc(Size size);
static HASHSEGMENT seg_alloc(HTAB* hashp);
static bool dir_realloc(HTAB* hashp);
static bool element_alloc(HTAB* hashp, int nelem, int freelist_idx);
static int choose_nelem_alloc(Size entrysize);
static bool expand_table(HTAB* hashp);
static HASHBUCKET get_hash_entry(HTAB* hashp, int freelist_idx, bool needPreAlloc);
static void hdefault(HTAB* hashp);
static bool init_htab(HTAB* hashp, long nelem);
static void hash_corrupted(HTAB* hashp);
static bool has_seq_scans(HTAB* hashp);
static int next_pow2_int(long num);
static long next_pow2_long(long num);
static void register_seq_scan(HTAB* hashp);
static void deregister_seq_scan(HTAB* hashp);

static thread_local DstoreMemoryContext CurrentDynaHashCxt;

static void* DynaHashAlloc(Size size)
{
    StorageAssert(DstoreMemoryContextIsValid(CurrentDynaHashCxt));
    return DstoreMemoryContextAlloc(CurrentDynaHashCxt, size);
}

static void* DynaHashAllocNoExcept(Size size)
{
    void* p = nullptr;
    DstoreMemoryContext oldContext;
    StorageAssert(DstoreMemoryContextIsValid(CurrentDynaHashCxt));

    oldContext = DstoreMemoryContextSwitchTo(CurrentDynaHashCxt);
    p = DstorePallocExtended(size, MCXT_ALLOC_NO_OOM);
    DstoreMemoryContextSwitchTo(oldContext);
    return p;
}

/*
 * HashCompareFunc for string keys
 *
 * Because we copy keys with strlcpy(), they will be truncated at keysize-1 bytes, so we can only compare that many ...
 * hence strncmp is almost but not quite the right thing.
 */
static int string_compare(const char* key1, const char* key2, Size keysize)
{
    return strncmp(key1, key2, keysize - 1);
}

static const char* strlcpy(char* dst, const char* src, size_t siz)
{
    char* d = dst;
    const char* s = src;
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
        while (*s++) {
            ;
        }
    }

    return s; /* count does not include NUL */
}

/************************** CREATE ROUTINES **********************/
RetStatus create_memory_context_for_freelist(const char *tabname, HASHHDR *hctl, DstoreMemoryContext currentDynaHashCxt)
{
    for (uint32 i = 0; i < NUM_FREELISTS; i++) {
        char freeListCtxName[FREELIST_CTX_MAX_NAME_LEN];
        int rc = sprintf_s(freeListCtxName, FREELIST_CTX_MAX_NAME_LEN, "%s_FreeList_%u", tabname, i);
        storage_securec_check_ss(rc);
        hctl->freeList[i].freeListCxt = DstoreAllocSetContextCreate(
            currentDynaHashCxt, freeListCtxName, ALLOCSET_DEFAULT_MINSIZE, ALLOCSET_DEFAULT_INITSIZE,
            ALLOCSET_DEFAULT_MAXSIZE, MemoryContextType::SHARED_CONTEXT);
        if (unlikely(hctl->freeList[i].freeListCxt == nullptr)) {
            return DSTORE_FAIL;
        }
    }
    return DSTORE_SUCC;
}
void delete_memory_context_if_necessary(DstoreMemoryContext TmpDynaHashCxt, DstoreMemoryContext OldDynaHashCxt)
{
    if (unlikely(TmpDynaHashCxt != nullptr)) {
        DstoreMemoryContextDelete(TmpDynaHashCxt);
        CurrentDynaHashCxt = OldDynaHashCxt;
    }
}
/*
 * hash_create -- create a new dynamic hash table
 *
 * tabname: a name for the table (for debugging purposes)
 * nelem: maximum number of elements expected
 * *info: additional table parameters, as indicated by flags
 * flags: bitmask indicating which parameters to take from *info
 *
 * Note: for a shared-memory hashtable, nelem needs to be a pretty good estimate, since we can't expand the table on the
 * fly.  But an unshared hashtable can be expanded on-the-fly, so it's better for nelem to be on the small side and
 * let the table grow if it's exceeded.  An overly large nelem will penalize hash_seq_search speed without buying much.
 */
HTAB* hash_create(const char* tabname, long nelem, HASHCTL* hashCtl, int flags)
{
    HASHHDR* hctl = nullptr;
    DstoreMemoryContext TmpDynaHashCxt = nullptr;
    DstoreMemoryContext OldDynaHashCxt = nullptr;

    /*
     * For shared hash tables, we have a local hash header (HTAB struct) that we allocate in t_thrd.top_mem_cxt;
     * all else is in shared memory.
     *
     * For non-shared hash tables, everything including the hash header is in a memory context created specially for
     * the hash table --- this makes hash_destroy very simple.  The memory context is made a child of either
     * a context specified by the caller, or t_thrd.top_mem_cxt if nothing is specified.
     */
    if ((flags & HASH_SHARED_MEM) || (flags & HASH_HEAP_MEM)) {
        /* Set up to allocate the hash header */
        CurrentDynaHashCxt = thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LONGLIVE);
    } else if ((flags & HASH_CONTEXT) && (flags & HASH_EXTERN_CONTEXT)) {
        /*
         * HASH_CONTEXT shows info->hcxt provides extern memory context, and HASH_EXTERN_CONTEXT means hash tables use
         * info->hcxt directly, and not create private memory context any more.
         *
         * HASH_EXTERN_CONTEXT is just valid with HASH_CONTEXT, and just for new pooler(poolmgr.cpp)
         *
         * NOTE: hashp->hcxt must be set to NULL before hash_destroy() is called when HASH_EXTERN_CONTEXT is enabled!!!
         */
        CurrentDynaHashCxt = hashCtl->hcxt;
    } else {
        /* Create the hash table's private memory context */
        if ((flags & HASH_CONTEXT) || (flags & HASH_SHRCTX)) {
            CurrentDynaHashCxt = hashCtl->hcxt;
        } else {
            CurrentDynaHashCxt = thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_LONGLIVE);
        }
        if (flags & HASH_SHRCTX) {
            TmpDynaHashCxt = DstoreAllocSetContextCreate(CurrentDynaHashCxt, tabname, ALLOCSET_DEFAULT_MINSIZE,
                                                         ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE,
                                                         MemoryContextType::SHARED_CONTEXT);
        } else {
            TmpDynaHashCxt = DstoreAllocSetContextCreate(CurrentDynaHashCxt, tabname, ALLOCSET_DEFAULT_MINSIZE,
                                                         ALLOCSET_DEFAULT_INITSIZE, ALLOCSET_DEFAULT_MAXSIZE,
                                                         CurrentDynaHashCxt->type);
        }
        if (unlikely(CurrentDynaHashCxt == nullptr || TmpDynaHashCxt == nullptr)) {
            return nullptr;
        }
        OldDynaHashCxt = CurrentDynaHashCxt;
        CurrentDynaHashCxt = TmpDynaHashCxt;
    }

    DstoreMemoryContext currentDynaHashCxt = CurrentDynaHashCxt;

    /* Initialize the hash header, plus a copy of the table name */
    HTAB* htab = (HTAB*)DynaHashAlloc(sizeof(HTAB) + strlen(tabname) + 1);
    if (unlikely(htab == nullptr)) {
        delete_memory_context_if_necessary(TmpDynaHashCxt, OldDynaHashCxt);
        return nullptr;
    }
    errno_t rc = memset_s(htab, sizeof(HTAB), 0, sizeof(HTAB));
    storage_securec_check(rc, "\0", "\0");

    htab->tabname = (char*)(htab + 1);
    rc = strcpy_s(htab->tabname, strlen(tabname) + 1, tabname);
    storage_securec_check(rc, "\0", "\0");
    if (rc != 0) {
        delete_memory_context_if_necessary(TmpDynaHashCxt, OldDynaHashCxt);
        return nullptr;
    }

    /*
     * Select the appropriate hash function (see comments at head of file).
     */
    if (flags & HASH_FUNCTION) {
        htab->hash = hashCtl->hash;
    } else if (flags & HASH_BLOBS) {
        /* We can optimize hashing for common key sizes */
        StorageAssert(flags & HASH_ELEM);
        if (hashCtl->keysize != sizeof(uint32)) {
            htab->hash = tag_hash;
        } else {
            htab->hash = uint32_hash;
        }
    } else {
        htab->hash = string_hash;  /* default hash function */
    }
    /*
     * If you don't specify a match function, it defaults to string_compare if you used string_hash
     * (either explicitly or by default) and to memcmp otherwise.  (Prior to PostgreSQL 7.4, memcmp was always used.)
     */
    if (flags & HASH_COMPARE) {
        htab->match = hashCtl->match;
    } else if (string_hash == htab->hash) {
        htab->match = (HashCompareFunc)string_compare;
    } else {
        htab->match = memcmp;
    }
    /* Similarly, the key-copying function defaults to strlcpy or memcpy. */
    if (flags & HASH_KEYCOPY) {
        htab->keycopy = hashCtl->keycopy;
    } else if (string_hash == htab->hash) {
        htab->keycopy = (HashCopyFunc)strlcpy;
    } else {
        htab->keycopy = memcpy;
    }
    if (flags & HASH_ALLOC) {
        htab->alloc = hashCtl->alloc;
    } else if (flags & HASH_NOEXCEPT) {
        htab->alloc = DynaHashAllocNoExcept;
    } else {
        htab->alloc = DynaHashAlloc;
    }
    if (flags & HASH_DEALLOC) {
        htab->dealloc = hashCtl->dealloc;
    } else {
        htab->dealloc = DstorePfree;
    }
    if ((flags & HASH_SHARED_MEM) || (flags & HASH_HEAP_MEM)) {
        /*
         * ctl structure and directory are preallocated for shared memory
         * tables. Note that HASH_DIRSIZE and HASH_ALLOC had better be set as
         * well.
         */
        htab->hctl = hashCtl->hctl;
        htab->dir = static_cast<HASHSEGMENT *>(static_cast<void *>(
            static_cast<char *>(static_cast<void*>(hashCtl->hctl)) + sizeof(HASHHDR)));
        htab->hcxt = nullptr;
        htab->flags.isshared = true;
        /* hash table already exists, we're just attaching to it */
        if (flags & HASH_ATTACH) {
            /* make local copies of some heavily-used values */
            htab->keysize = htab->hctl->keysize;
            htab->ssize = htab->hctl->ssize;
            htab->sshift = htab->hctl->sshift;
            return htab;
        }
    } else {
        /* setup hash table defaults */
        htab->hctl = nullptr;
        htab->dir = nullptr;
        htab->hcxt = currentDynaHashCxt;
        htab->flags.isshared = false;
    }

    if (!htab->hctl) {
        htab->hctl = (HASHHDR *)htab->alloc(sizeof(HASHHDR));
        if (!htab->hctl) {
            storage_set_error(HASH_ERROR_OUT_OF_MEMORY);
            delete_memory_context_if_necessary(TmpDynaHashCxt, OldDynaHashCxt);
            return nullptr;
        }
        htab->flags.is_hctl_allocated = true;
    }

    htab->flags.frozen = false;
    hdefault(htab);
    hctl = htab->hctl;
    if (flags & HASH_PARTITION) {
        /* We are using treaded backends so this hash table can be in Global Memory "shared" by the backends, or
         * in system Shared Memory.
         * The number of partitions had better be a power of 2. Also, it must be less than INT_MAX (see init_htab()),
         * so call the int version of next_pow2.
         */
        StorageAssert(hashCtl->num_partitions == next_pow2_int(hashCtl->num_partitions));

        hctl->num_partitions = hashCtl->num_partitions;
        if (create_memory_context_for_freelist(tabname, hctl, currentDynaHashCxt) == DSTORE_FAIL) {
            delete_memory_context_if_necessary(TmpDynaHashCxt, OldDynaHashCxt);
            return nullptr;
        }
    }
    if (flags & HASH_SEGMENT) {
        hctl->ssize = hashCtl->ssize;
        hctl->sshift = my_log2(hashCtl->ssize);
        /* ssize had better be a power of 2 */
        StorageAssert(hctl->ssize == (1L << hctl->sshift));
    }
    if (flags & HASH_FFACTOR) {
        hctl->ffactor = hashCtl->ffactor;
    }
    /*
     * SHM hash tables have fixed directory size passed by the caller.
     */
    if (flags & HASH_DIRSIZE) {
        hctl->max_dsize = hashCtl->max_dsize;
        hctl->dsize = hashCtl->dsize;
    }
    /*
     * hash table now allocates space for key and data but you have to say how much space to allocate
     */
    if (flags & HASH_ELEM) {
        StorageAssert(hashCtl->entrysize >= hashCtl->keysize);
        hctl->entrysize = hashCtl->entrysize;
        hctl->keysize = hashCtl->keysize;
    }

    /* make local copies of heavily-used constant fields */
    htab->ssize = hctl->ssize;
    htab->sshift = hctl->sshift;
    htab->keysize = hctl->keysize;

    /* Build the hash directory structure */
    if (!init_htab(htab, nelem)) {
        storage_set_error(HASH_ERROR_FAIL_TO_INITIALIZE_TABLE, htab->tabname);
        delete_memory_context_if_necessary(TmpDynaHashCxt, OldDynaHashCxt);
        return nullptr;
    }
    /*
     * For a shared hash table, preallocate the requested number of elements.
     * This reduces problems with run-time out-of-shared-memory conditions.
     *
     * For a non-shared hash table, preallocate the requested number of elements if it's less than our chosen
     * nelem_alloc. This avoids wasting space if the caller correctly estimates a small table size.
     */
    if ((flags & HASH_SHARED_MEM) || (flags & HASH_HEAP_MEM) || nelem < hctl->nelem_alloc) {
        int freelist_partitions, nelem_alloc_first;

        /*
         * If hash table is partitioned all freeLists have equal number of elements. Otherwise only freeList[0] is used.
         */
        if (IS_PARTITIONED(htab->hctl)) {
            freelist_partitions = NUM_FREELISTS;
        } else {
            freelist_partitions = 1;
        }
        int nelem_alloc = nelem / freelist_partitions;
        if (nelem_alloc == 0) {
            nelem_alloc = 1;
        }
        /* Make sure all memory will be used. */
        if (nelem_alloc * freelist_partitions < nelem) {
            nelem_alloc_first = nelem - nelem_alloc * (freelist_partitions - 1);
        } else {
            nelem_alloc_first = nelem_alloc;
        }
        for (int i = 0; i < freelist_partitions; i++) {
            int temp = (i == 0) ? nelem_alloc_first : nelem_alloc;
            if (!element_alloc(htab, temp, i)) {
                storage_set_error(HASH_ERROR_OUT_OF_MEMORY);
                delete_memory_context_if_necessary(TmpDynaHashCxt, OldDynaHashCxt);
                return nullptr;
            }
        }
    }
    if (flags & HASH_BATCH_ALLOC_NUM) {
        hctl->nelem_batch_alloc = hashCtl->batch_alloc_num;
    } else {
        hctl->nelem_batch_alloc = hctl->nelem_alloc;
    }
    if (flags & HASH_FIXED_SIZE) {
        htab->flags.isfixed = true;
    }
    return htab;
}

/* Set default HASHHDR parameters. */
static void hdefault(HTAB* hashp)
{
    HASHHDR* hctl = hashp->hctl;

    errno_t rc = memset_s(hctl, sizeof(HASHHDR), 0, sizeof(HASHHDR));
    storage_securec_check(rc, "\0", "\0");

    hctl->dsize = DEF_DIRSIZE;
    hctl->nsegs = 0;

    /* rather pointless defaults for key & entry size */
    hctl->keysize = sizeof(char*);
    hctl->entrysize = 2 * sizeof(char*);

    /* table has no fixed maximum size */
    hctl->max_dsize = NO_MAX_DSIZE;

    hctl->num_partitions = 0; /* not partitioned */

    hctl->ffactor = DEF_FFACTOR;

    hctl->ssize = DEF_SEGSIZE;
    hctl->sshift = DEF_SEGSIZE_SHIFT;

#ifdef HASH_STATISTICS
    hctl->accesses = hctl->collisions = 0;
#endif
}

/*
 * Given the user-specified entry size, choose nelem_alloc, ie, how many elements to add to the hash table
 * when we need more.
 */
static int choose_nelem_alloc(Size entrysize)
{
    Size elementSize;
    Size allocSize;
    int nelem_alloc;

    /* Each element has a HASHELEMENT header plus user data. */
    /* NB: this had better match element_alloc() */
    elementSize = MAXALIGN(sizeof(HASHELEMENT)) + MAXALIGN(entrysize);

    /*
     * The idea here is to choose nelem_alloc at least 32, but round up so that the allocation request will be a power
     * of 2 or just less. This makes little difference for hash tables in shared memory, but for hash tables managed by
     * palloc, the allocation request will be rounded up to a power of 2 anyway. If we fail to take this into account,
     * we'll waste as much as half the allocated space.
     */
    allocSize = 32 * 4; /* assume elementSize at least 8 */
    do {
        allocSize <<= 1;
        nelem_alloc = allocSize / elementSize;
    } while (nelem_alloc < 32);

    return nelem_alloc;
}

/*
 * Compute derived fields of hctl and build the initial directory/segment arrays
 */
static bool init_htab(HTAB* hashp, long nelem)
{
    if (unlikely(hashp == nullptr || hashp->hctl == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("invalid hashp when init_htab."));
        return false;
    }
    HASHHDR* hctl = hashp->hctl;
    HASHSEGMENT* segp = nullptr;
    hashp->cnt.store(0, std::memory_order_release);

    /*
     * Divide number of elements by the fill factor to determine a desired number of buckets.
     * Allocate space for the next greater power of two number of buckets
     */
    uint32 nbuckets = next_pow2_int((nelem - 1) / hctl->ffactor + 1);

    /*
     * In a partitioned table, nbuckets must be at least equal to num_partitions; were it less,
     * keys with apparently different partition numbers would map to the same bucket, breaking partition independence.
     * (Normally nbuckets will be much bigger; this is just a safety check)
     */
    while (nbuckets < hctl->num_partitions) {
        nbuckets <<= 1;
    }
    hctl->low_mask = nbuckets - 1;
    hctl->max_bucket = nbuckets - 1;
    hctl->high_mask = (nbuckets << 1) - 1;

    /*
     * figure number of directory segments needed, round up to a power of 2.
     */
    int nsegs = (nbuckets - 1) / hctl->ssize + 1;
    nsegs = next_pow2_int(nsegs);
    /*
     * make sure directory is big enough. If pre-allocated directory is too small, choke (caller screwed up).
     */
    if (hctl->dsize < nsegs) {
        if (hashp->dir) {
            return false;
        } else {
            hctl->dsize = nsegs;
        }
    }

    /* allocate a directory */
    if (!(hashp->dir)) {
        CurrentDynaHashCxt = hashp->hcxt;
        hashp->dir = (HASHSEGMENT *)hashp->alloc(hctl->dsize * sizeof(HASHSEGMENT));
        if (!hashp->dir) {
            /* alloc hash failed */
            return false;
        }
        hashp->flags.is_dir_allocated = true;
    }

    /* allocate initial segments */
    for (segp = hashp->dir; hctl->nsegs < nsegs; hctl->nsegs++, segp++) {
        *segp = seg_alloc(hashp);
        if ((*segp) == nullptr) {
            return false;
        }
    }

    /* choose number of entries to allocate at a time */
    hctl->nelem_alloc = choose_nelem_alloc(hctl->entrysize);

#ifdef HASH_DEBUG
#if HASH_DEBUG
    fprintf(stderr,
        "init_htab:\n%s%p\n%s%ld\n%s%ld\n%s%d\n%s%ld\n%s%u\n%s%x\n%s%x\n%s%ld\n%s%ld\n",
        "TABLE POINTER   ", hashp, "DIRECTORY SIZE  ", hctl->dsize, "SEGMENT SIZE    ", hctl->ssize, "SEGMENT SHIFT   ",
        hctl->sshift, "FILL FACTOR     ", hctl->ffactor, "MAX BUCKET      ", hctl->max_bucket, "HIGH MASK       ",
        hctl->high_mask, "LOW  MASK       ", hctl->low_mask, "NSEGS           ", hctl->nsegs, "NENTRIES        ",
        hash_get_num_entries(hashp));
#endif
#endif  // #ifdef HASH_DEBUG
    return true;
}

/*
 * Estimate the space needed for a hashtable containing the given number of entries of given size.
 * NOTE: this is used to estimate the footprint of hashtables in shared memory; therefore it does not count HTAB
 * which is in local memory.
 * NB: assumes that all hash structure parameters have default values!
 */
Size hash_estimate_size(long num_entries, Size entrysize)
{
    /* estimate number of buckets wanted */
    long nBuckets = next_pow2_long((num_entries - 1) / DEF_FFACTOR + 1);

    /* # of segments needed for nBuckets */
    long nSegments = next_pow2_long((nBuckets - 1) / DEF_SEGSIZE + 1);

    /* directory entries */
    unsigned long nDirEntries = DEF_DIRSIZE;
    while (nDirEntries < (unsigned long)nSegments) {
        nDirEntries <<= 1; /* dir_alloc doubles dsize at each call */
    }

    /* fixed control info */
    Size size = MAXALIGN(sizeof(HASHHDR)); /* but not HTAB, per above */

    /* directory */
    size = add_size(size, mul_size(nDirEntries, sizeof(HASHSEGMENT)));

    /* segments */
    size = add_size(size, mul_size(nSegments, MAXALIGN(DEF_SEGSIZE * sizeof(HASHBUCKET))));

    /* elements --- allocated in groups of choose_nelem_alloc() entries */
    long elementAllocCnt = choose_nelem_alloc(entrysize);
    if (elementAllocCnt == 0) {
        storage_set_error(HASH_ERROR_DIVISION_BY_ZERO);
        return 0;
    }
    long nElementAllocs = (num_entries - 1) / elementAllocCnt + 1;
    long elementSize = static_cast<long>(MAXALIGN(sizeof(HASHELEMENT)) + MAXALIGN(entrysize));
    size = add_size(size, mul_size(nElementAllocs, mul_size(elementAllocCnt, elementSize)));

    return size;
}

/*
 * Select an appropriate directory size for a hashtable with the given maximum number of entries.
 * This is only needed for hashtables in shared memory, whose directories cannot be expanded dynamically.
 * NB: assumes that all hash structure parameters have default values!
 *
 * XXX this had better agree with the behavior of init_htab()......
 */
long hash_select_dirsize(long num_entries)
{
    /* estimate number of buckets wanted */
    long nBuckets = next_pow2_long((num_entries - 1) / DEF_FFACTOR + 1);

    /* # of segments needed for nBuckets */
    long nSegments = next_pow2_long((nBuckets - 1) / DEF_SEGSIZE + 1);

    /* directory entries */
    unsigned long nDirEntries = DEF_DIRSIZE;
    while (nDirEntries < (unsigned long)nSegments) {
        nDirEntries <<= 1; /* dir_alloc doubles dsize at each call */
    }
    return static_cast<long>(nDirEntries);
}

/*
 * Compute the required initial memory allocation for a shared-memory
 * hashtable with the given parameters.  We need space for the HASHHDR
 * and for the (non expansible) directory.
 */
Size hash_get_shared_size(HASHCTL* info, UNUSE_PARAM int flags)
{
    StorageAssert(flags & HASH_DIRSIZE);
    StorageAssert(info->dsize == info->max_dsize);
    return sizeof(HASHHDR) + info->dsize * sizeof(HASHSEGMENT);
}

/********************** DESTROY ROUTINES ************************/
void hash_destroy(HTAB* hashp) noexcept
{
    if (hashp != nullptr) {
        /* allocation method must be one we know how to free, too */
        StorageAssert(hashp->alloc == DynaHashAlloc || hashp->alloc == DynaHashAllocNoExcept || hashp->alloc == malloc);
        /* so this hashtable must have it's own context */
        StorageAssert(hashp->hcxt != nullptr);

        hash_stats("destroy", hashp);

        /*
         * Free everything by destroying the hash table's memory context.
         */
        if (IS_PARTITIONED(hashp->hctl)) {
            for (uint32 i = 0; i < NUM_FREELISTS; i++) {
                DstoreMemoryContextDelete(hashp->hctl->freeList[i].freeListCxt);
            }
        }
        if (hashp->flags.is_hctl_allocated) {
            hashp->dealloc(hashp->hctl);
            hashp->flags.is_hctl_allocated = false;
        }
        if (hashp->flags.is_dir_allocated) {
            hashp->dealloc(hashp->dir);
            hashp->flags.is_dir_allocated = false;
        }
        DstoreMemoryContextDelete(hashp->hcxt);
    }
}

void hash_remove(HTAB* hashp)
{
    if (hashp == nullptr || hashp->hcxt == nullptr) {
        return;
    }
    hash_stats("destroy", hashp);

    /*
     * Free everything by destroying the hash table's memory context.
     */
    if (IS_PARTITIONED(hashp->hctl)) {
        for (uint32 i = 0; i < NUM_FREELISTS; i++) {
            DstoreMemoryContextDelete(hashp->hctl->freeList[i].freeListCxt);
        }
    }
    DstoreMemoryContextDelete(hashp->hcxt);
}

void hash_stats(const char* where, HTAB* hashp)
{
    (void(where));
    (void(hashp));
#ifdef HASH_STATISTICS
#if HASH_STATISTICS
    fprintf(stderr, "%s: this HTAB -- accesses %ld collisions %ld\n", where, hashp->hctl->accesses,
        hashp->hctl->collisions);

    fprintf(stderr, "hash_stats: entries %ld keysize %ld maxp %u segmentcount %ld\n", hash_get_num_entries(hashp),
        (long)hashp->hctl->keysize, hashp->hctl->max_bucket, hashp->hctl->nsegs);
    fprintf(stderr, "%s: total accesses %ld total collisions %ld\n", where, hash_accesses, hash_collisions);
    fprintf(stderr, "hash_stats: total expansions %ld\n", hash_expansions);
#endif
#endif  // #ifdef HASH_STATISTICS
}

/*
 * get_hash_value -- exported routine to calculate a key's hash value
 *
 * We export this because for partitioned tables, callers need to compute
 * the partition number (from the low-order bits of the hash value) before
 * searching.
 */
HOTFUNCTION uint32 get_hash_value(HTAB* hashp, const void* keyPtr)
{
    if (unlikely(hashp == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("invalid hashp when get_hash_value."));
        return 0;
    }
    return hashp->hash(keyPtr, hashp->keysize);
}

/* Convert a hash value to a bucket number */
static HOTFUNCTION inline uint32 calc_bucket(HASHHDR* hctl, uint32 hash_val)
{
    if (unlikely(hctl == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("invalid hctl when calc_bucket."));
        return 0;
    }
    uint32 bucket = hash_val & hctl->high_mask;
    if (bucket > hctl->max_bucket) {
        bucket = bucket & hctl->low_mask;
    }
    return bucket;
}

/*
 * hash_search -- look up key in table and perform action
 * HashSearchWithHashValue -- same, with key's hash value already computed
 *
 * action is one of:
 *  HASH_FIND: look up key in table
 *  HASH_ENTER: look up key in table, creating entry if not present
 *  HASH_ENTER_NULL: same, but return NULL if out of memory
 *  HASH_ENTER_PRE_ALLOC: same, but allocate nelem_batch_alloc enties at a time when freelist is empty
 *  HASH_REMOVE: look up key in table, remove entry if present
 *
 * Return value is a pointer to the element found/entered/removed if any, or NULL if no match was found.
 * (NB: in the case of the REMOVE action, the result is a dangling pointer that shouldn't be dereferenced!)
 *
 * HASH_ENTER will normally ereport a generic "out of memory" error if it is unable to create a new entry.
 * The HASH_ENTER_NULL operation is the same except it will return NULL if out of memory.
 * Note that HASH_ENTER_NULL cannot be used with the default palloc-based allocator, since palloc internally ereports
 * on out-of-memory.
 *
 * If foundPtr isn't NULL, then *foundPtr is set TRUE if we found an existing entry in the table, FALSE otherwise.
 * This is needed in the HASH_ENTER case, but is redundant with the return value otherwise.
 *
 * For HashSearchWithHashValue, the hashvalue parameter must have been calculated with get_hash_value().
 */
HOTFUNCTION void* hash_search(HTAB* hashp, const void* keyPtr, HASHACTION action, bool* foundPtr)
{
    if (unlikely(hashp == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("invalid hashp when hash_search."));
        return nullptr;
    }
    return hash_search_with_hash_value(hashp, keyPtr, hashp->hash(keyPtr, hashp->keysize), action, foundPtr);
}

HOTFUNCTION void* hash_search_with_hash_value(HTAB* hashp, const void* keyPtr, uint32 hashvalue, HASHACTION action, bool* foundPtr)
{
    if (unlikely(hashp == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("invalid hashp when hash_search_with_hash_value."));
        return nullptr;
    }
    HASHHDR* hctl = hashp->hctl;
    if (unlikely(hctl == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("invalid hashp when hash_search_with_hash_value."));
        return nullptr;
    }
    int freelistIdx = static_cast<int>(FREELIST_IDX(hctl, hashvalue));

#ifdef HASH_STATISTICS
#if HASH_STATISTICS
    hash_accesses++;
    hctl->accesses++;
#endif
#endif  // #ifdef HASH_STATISTICS

    /*
     * If inserting, check if it is time to split a bucket.
     *
     * NOTE: failure to expand table is not a fatal error, it just means we have to run at higher fill factor
     * than we wanted. However, if we're using the palloc allocator then it will throw error anyway on out-of-memory,
     * so we must do this before modifying the table.
     */
    if (action == HASH_ENTER || action == HASH_ENTER_NULL) {
        /*
         * Can't split if running in partitioned mode, nor if frozen, nor if table is the subject of any active
         * hash_seq_search scans.  Strange order of these tests is to try to check cheaper conditions first.
         */
        if (!IS_PARTITIONED(hctl) && !hashp->flags.frozen &&
            hctl->freeList[0].nentries / (long)(hctl->max_bucket + 1) >= hctl->ffactor && !has_seq_scans(hashp)) {
            (void)expand_table(hashp);
        }
    }

    /*
     * Do the initial lookup
     */
    uint32 bucket = calc_bucket(hctl, hashvalue);
    long segment_num = bucket >> hashp->sshift;
    long segment_ndx = MOD(bucket, hashp->ssize);

    HASHSEGMENT segp = hashp->dir[segment_num];

    if (segp == nullptr) {
        hash_corrupted(hashp);
    }
    HASHBUCKET* prevBucketPtr = &segp[segment_ndx];
    HASHBUCKET currBucket = *prevBucketPtr;

    /*
     * Follow collision chain looking for matching key
     */
    HashCompareFunc match = hashp->match;     /* save one fetch in inner loop */
    Size keysize = hashp->keysize; /* ditto */

    while (currBucket != nullptr) {
        if (currBucket->hashvalue == hashvalue && match(ELEMENTKEY(currBucket), keyPtr, keysize) == 0) {
            break;
        }
        prevBucketPtr = &(currBucket->link);
        currBucket = *prevBucketPtr;
#ifdef HASH_STATISTICS
#if HASH_STATISTICS
        hash_collisions++;
        hctl->collisions++;
#endif
#endif  // #ifdef HASH_STATISTICS
    }

    if (foundPtr != nullptr) {
        *foundPtr = (bool)(currBucket != nullptr);
    }
    switch (action) {
        case HASH_FIND: {
            if (currBucket != nullptr) {
                return (void*)ELEMENTKEY(currBucket);
            }
            return nullptr;
        }

        case HASH_REMOVE: {
            if (currBucket != nullptr) {
                /* if partitioned, must lock to touch nentries and freeList */
                if (IS_PARTITIONED(hctl)) {
                    hctl->freeList[freelistIdx].mutex.lock();
                    // SpinLockAcquire(&(hctl->freeList[freelist_idx].mutex));
                }
                StorageAssert(hctl->freeList[freelistIdx].nentries > 0);
                hctl->freeList[freelistIdx].nentries--;
                hashp->cnt.fetch_sub(1, std::memory_order_acq_rel);

                /* remove record from hash bucket's chain. */
                *prevBucketPtr = currBucket->link;

                /* add the record to the freelist for this table.  */
                currBucket->link = hctl->freeList[freelistIdx].freeList;
                hctl->freeList[freelistIdx].freeList = currBucket;

                if (IS_PARTITIONED(hctl)) {
                    hctl->freeList[freelistIdx].mutex.unlock();
                }

                /*
                 * better hope the caller is synchronizing access to this
                 * element, because someone else is going to reuse it the next
                 * time something is added to the table
                 */
                return (void*)ELEMENTKEY(currBucket);
            }
            return nullptr;
        }

        case HASH_ENTER_NULL: {
            /* ENTER_NULL does not work with palloc-based allocator */
            StorageAssert(hashp->alloc != DynaHashAlloc || hashp->alloc != DynaHashAllocNoExcept);
            /* FALL THRU */
            __attribute__((fallthrough));
        }
        case HASH_ENTER_PRE_ALLOC:
        case HASH_ENTER: {
            /* Return existing element if found, else create one */
            if (currBucket != nullptr) {
                return (void*)ELEMENTKEY(currBucket);
            }

            /* disallow inserts if frozen */
            if (hashp->flags.frozen) {
                if (hashp->alloc == DynaHashAllocNoExcept) {
                    return nullptr;
                }
                storage_set_error(HASH_ERROR_CANNOT_INSERT_INTO_FROZEN, hashp->tabname);
                return nullptr;
            }

            currBucket = get_hash_entry(hashp, freelistIdx, (action == HASH_ENTER_PRE_ALLOC));
            if (currBucket == nullptr) {
                /* out of memory */
                if (action == HASH_ENTER_NULL) {
                    return nullptr;
                }

                /* report a generic message */
                if (hashp->flags.isshared) {
                    storage_set_error(HASH_ERROR_OUT_OF_SHARED_MEMORY);
                    StorageAssert(0);
                } else {
                    storage_set_error(HASH_ERROR_OUT_OF_MEMORY);
                    StorageAssert(0);
                }
                return nullptr;
            }

            /* link into hashbucket chain */
            *prevBucketPtr = currBucket;
            currBucket->link = nullptr;

            /* copy key into record */
            currBucket->hashvalue = hashvalue;
            if (hashp->keycopy == memcpy) {
                errno_t errorno = memcpy_s(ELEMENTKEY(currBucket), keysize, keyPtr, keysize);
                storage_securec_check(errorno, "\0", "\0");
            } else {
                hashp->keycopy(ELEMENTKEY(currBucket), keyPtr, keysize);
            }

            /*
             * Caller is expected to fill the data field on return.  DO NOT insert any code that could possibly throw
             * error here, as doing so would leave the table entry incomplete and hence corrupt the caller's
             * data structure.
             */
            return (void*)ELEMENTKEY(currBucket);
        }
        default:
            break;
    }

    if (hashp->alloc == DynaHashAllocNoExcept) {
    } else {
        storage_set_error(HASH_ERROR_UNRECOGNIZED_HASH_ACTION_CODE, (int)action);
        StorageAssert(0);
    }

    return nullptr; /* keep compiler quiet */
}

/*
 * create a new entry if possible
 */
static HASHBUCKET get_hash_entry(HTAB* hashp, int freelist_idx, bool needPreAlloc)
{
    if (unlikely(hashp == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("invalid hashp when get_hash_entry."));
        return nullptr;
    }
    HASHHDR* hctl = hashp->hctl;
    if (unlikely(hctl == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("invalid hashp when get_hash_entry."));
        return nullptr;
    }
    HASHBUCKET newElement = nullptr;
    int borrow_from_idx;

    while (true) {
        /* if partitioned, must lock to touch nentries and freeList */
        if (IS_PARTITIONED(hctl)) {
            hctl->freeList[freelist_idx].mutex.lock();
        }
        /* try to get an entry from the freelist */
        newElement = hctl->freeList[freelist_idx].freeList;

        if (newElement != nullptr) {
            break;
        }
        if (IS_PARTITIONED(hctl)) {
            hctl->freeList[freelist_idx].mutex.unlock();
        }
        /* no free elements.  allocate another chunk of buckets */
        if (!element_alloc(hashp, (needPreAlloc ? hctl->nelem_batch_alloc : hctl->nelem_alloc), freelist_idx)) {
            if (!IS_PARTITIONED(hctl)) {
                return nullptr; /* out of memory */
            }
            /* try to borrow element from another partition */
            borrow_from_idx = freelist_idx;
            while (true) {
                borrow_from_idx = (borrow_from_idx + 1) % NUM_FREELISTS;
                if (borrow_from_idx == freelist_idx) {
                    break;
                }
                hctl->freeList[borrow_from_idx].mutex.lock();
                newElement = hctl->freeList[borrow_from_idx].freeList;

                if (newElement != nullptr) {
                    hctl->freeList[borrow_from_idx].freeList = newElement->link;
                    hctl->freeList[borrow_from_idx].mutex.unlock();

                    hctl->freeList[freelist_idx].mutex.lock();
                    hctl->freeList[freelist_idx].nentries++;
                    hashp->cnt.fetch_add(1, std::memory_order_acq_rel);
                    hctl->freeList[freelist_idx].mutex.unlock();

                    break;
                }

                hctl->freeList[borrow_from_idx].mutex.unlock();
            }

            return newElement;
        }
    }

    /* remove entry from freelist, bump nentries */
    hctl->freeList[freelist_idx].freeList = newElement->link;
    hctl->freeList[freelist_idx].nentries++;
    hashp->cnt.fetch_add(1, std::memory_order_acq_rel);

    if (IS_PARTITIONED(hctl)) {
        hctl->freeList[freelist_idx].mutex.unlock();
    }
    return newElement;
}

/*
 * hash_get_num_entries -- get the number of entries in a hashtable
 */
long hash_get_num_entries(HTAB* hashp)
{
    StorageReleasePanic((hashp == nullptr), MODULE_FRAMEWORK, ErrMsg("Get num entries failed, hashpointer is null."));
    return hashp->cnt.load(std::memory_order_acquire);
}

/*
 * hash_seq_init/_search/_term
 *   Sequentially search through hash table and return all the elements one by one, return NULL when no more.
 *
 * hash_seq_term should be called if and only if the scan is abandoned before completion; if hash_seq_search
 * returns NULL then it has already done the end-of-scan cleanup.
 *
 * NOTE: caller may delete the returned element before continuing the scan.
 * However, deleting any other element while the scan is in progress is UNDEFINED (it might be the one that curIndex
 * is pointing at!). Also, if elements are added to the table while the scan is in progress, it is unspecified whether
 * they will be visited by the scan or not.
 *
 * NOTE: it is possible to use hash_seq_init/hash_seq_search without any worry about hash_seq_term cleanup,
 * if the hashtable is first locked against further insertions by calling hash_freeze. This is used by nodeAgg.c,
 * wherein it is inconvenient to track whether a scan is still open, and there's no possibility of further insertions
 * after readout has begun.
 *
 * NOTE: to use this with a partitioned hashtable, caller had better hold at least shared lock on all partitions of
 * the table throughout the scan!
 * We can cope with insertions or deletions by our own backend, but *not* with concurrent insertions or deletions
 * by another.
 */
void hash_seq_init(HASH_SEQ_STATUS* status, HTAB* hashp)
{
    status->curBucket = 0;
    status->curEntry = nullptr;
    status->hashp = hashp;
    if (!hashp->flags.frozen) {
        register_seq_scan(hashp);
    }
}

void* hash_seq_search(HASH_SEQ_STATUS* status)
{
    HTAB* hashp = nullptr;
    HASHHDR* hctl = nullptr;
    HASHELEMENT* curElem = nullptr;

    if ((curElem = status->curEntry) != nullptr) {
        /* Continuing scan of curBucket... */
        status->curEntry = curElem->link;
        if (status->curEntry == nullptr) { /* end of this bucket */
            ++status->curBucket;
        }
        return (void *)ELEMENTKEY(curElem);
    }

    /*
     * Search for next nonempty bucket starting at curBucket.
     */
    uint32 curBucket = status->curBucket;
    hashp = status->hashp;
    hctl = hashp->hctl;
    long ssize = hashp->ssize;
    uint32 max_bucket = hctl->max_bucket;
    if (curBucket > max_bucket) {
        hash_seq_term(status);
        return nullptr; /* search is done */
    }

    /*
     * first find the right segment in the table directory.
     */
    long segment_num = curBucket >> hashp->sshift;
    long segment_ndx = MOD(curBucket, ssize);
    HASHSEGMENT segp = hashp->dir[segment_num];

    /*
     * Pick up the first item in this bucket's chain.  If chain is not empty we can begin searching it.
     * Otherwise we have to advance to find the next nonempty bucket.  We try to optimize that case since searching a
     * near-empty hashtable has to iterate this loop a lot.
     */
    while ((curElem = segp[segment_ndx]) == nullptr) {
        /* empty bucket, advance to next */
        if (++curBucket > max_bucket) {
            status->curBucket = curBucket;
            hash_seq_term(status);
            return nullptr; /* search is done */
        }
        if (++segment_ndx >= ssize) {
            segment_ndx = 0;
            segment_num++;
            segp = hashp->dir[segment_num];
        }
    }
    /* Begin scan of curBucket... */
    status->curEntry = curElem->link;
    if (status->curEntry == nullptr) { /* end of this bucket */
        curBucket++;
    }
    status->curBucket = curBucket;
    return (void *)ELEMENTKEY(curElem);
}

void hash_seq_term(HASH_SEQ_STATUS* hashSeqStatus)
{
    if (!hashSeqStatus->hashp->flags.frozen) {
        deregister_seq_scan(hashSeqStatus->hashp);
    }
}

/*
 * hash_freeze
 *   Freeze a hashtable against future insertions (deletions are still allowed)
 *
 * The reason for doing this is that by preventing any more bucket splits, we no longer need to worry about registering
 * hash_seq_search scans, and thus caller need not be careful about ensuring hash_seq_term gets called at the right
 * times.
 *
 * Multiple calls to hash_freeze() are allowed, but you can't freeze a table with active scans
 * (since hash_seq_term would then do the wrong thing).
 */
void hash_freeze(HTAB* hashp)
{
    if (unlikely(hashp == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("invalid hashp when hash_freeze."));
        return;
    }
    if (hashp->flags.isshared) {
        storage_set_error(HASH_ERROR_CANNOT_FREEZE_SHARED_HASHTABLE, hashp->tabname);
        StorageAssert(0);
    }
    if (!hashp->flags.frozen && has_seq_scans(hashp)) {
        storage_set_error(HASH_ERROR_CANNOT_FREEZE_ACTIVE_HASHTABLE, hashp->tabname);
        StorageAssert(0);
    }
    hashp->flags.frozen = true;
}

/*
 * Expand the table by adding one more hash bucket.
 */
static bool expand_table(HTAB* hashp)
{
    if (unlikely(hashp == nullptr || hashp->hctl == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("invalid hashp when expand_table."));
        return false;
    }
    HASHHDR* hctl = hashp->hctl;
    HASHBUCKET* oldlink = nullptr;
    HASHBUCKET* newlink = nullptr;
    HASHBUCKET currElem, nextElem;

    StorageAssert(!IS_PARTITIONED(hctl));

#ifdef HASH_STATISTICS
    hash_expansions++;
#endif

    long new_bucket = hctl->max_bucket + 1;
    long new_segnum = new_bucket >> hashp->sshift;
    long new_segndx = MOD(new_bucket, hashp->ssize);

    if (hctl->nsegs <= new_segnum) {
        /* Allocate new segment if necessary -- could fail if dir full */
        if (hctl->dsize <= new_segnum) {
            if (!dir_realloc(hashp)) {
                return false;
            }
        }
        hashp->dir[new_segnum] = seg_alloc(hashp);
        if (!(hashp->dir[new_segnum])) {
            return false;
        }
        hctl->nsegs++;
    }

    /* OK, we created a new bucket. */
    hctl->max_bucket++;

    /*
     * *Before* changing masks, find old bucket corresponding to same hash values; values in that bucket may need
     * to be relocated to new bucket.
     * Note that new_bucket is certainly larger than low_mask at this point, so we can skip the first step of
     * the regular hash mask calc.
     */
    long old_bucket = (new_bucket & hctl->low_mask);

    /*
     * If we crossed a power of 2, readjust masks.
     */
    if ((uint32)new_bucket > hctl->high_mask) {
        hctl->low_mask = hctl->high_mask;
        hctl->high_mask = (uint32)new_bucket | hctl->low_mask;
    }

    /*
     * Relocate records to the new bucket. NOTE: because of the way the hash masking is done in calc_bucket,
     * only one old bucket can need to be split at this point.  With a different way of reducing the hash value,
     * that might not be true!
     */
    long old_segnum = old_bucket >> hashp->sshift;
    long old_segndx = MOD(old_bucket, hashp->ssize);

    HASHSEGMENT old_seg = hashp->dir[old_segnum];
    HASHSEGMENT new_seg = hashp->dir[new_segnum];

    oldlink = &old_seg[old_segndx];
    newlink = &new_seg[new_segndx];

    for (currElem = *oldlink; currElem != nullptr; currElem = nextElem) {
        nextElem = currElem->link;
        if ((long)calc_bucket(hctl, currElem->hashvalue) == old_bucket) {
            *oldlink = currElem;
            oldlink = &currElem->link;
        } else {
            *newlink = currElem;
            newlink = &currElem->link;
        }
    }
    /* don't forget to terminate the rebuilt hash chains... */
    *oldlink = nullptr;
    *newlink = nullptr;

    return true;
}

static bool dir_realloc(HTAB* hashp)
{
    if (unlikely(hashp == nullptr || hashp->hctl == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("invalid hashp when dir_realloc."));
        return false;
    }
    HASHSEGMENT* p = nullptr;
    HASHSEGMENT* old_p = nullptr;

    if (hashp->hctl->max_dsize != NO_MAX_DSIZE) {
        return false;
    }
    /* Reallocate directory */
    long new_dsize = hashp->hctl->dsize << 1;
    long old_dirsize = hashp->hctl->dsize * static_cast<long>(sizeof(HASHSEGMENT));
    long new_dirsize = new_dsize * static_cast<long>(sizeof(HASHSEGMENT));

    old_p = hashp->dir;
    CurrentDynaHashCxt = hashp->hcxt;
    p = (HASHSEGMENT*)hashp->alloc((Size)new_dirsize);
    if (p != nullptr) {
        errno_t rc;

        rc = memcpy_s(p, new_dirsize, old_p, old_dirsize);
        storage_securec_check(rc, "\0", "\0");
        rc = memset_s(((char*)p) + old_dirsize, new_dirsize - old_dirsize, 0, new_dirsize - old_dirsize);
        storage_securec_check(rc, "\0", "\0");
        hashp->dir = p;
        hashp->hctl->dsize = new_dsize;

        /* XXX assume the allocator is palloc, so we know how to free */
        if (hashp->alloc == DynaHashAlloc || hashp->alloc == DynaHashAllocNoExcept) {
            DstorePfreeExt(old_p);
        } else {
            hashp->dealloc(old_p);
        }
        return true;
    }
    return false;
}

static HASHSEGMENT seg_alloc(HTAB* hashp)
{
    if (unlikely(hashp == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("invalid hashp when seg_alloc."));
        return nullptr;
    }
    HASHSEGMENT segp = nullptr;

    CurrentDynaHashCxt = hashp->hcxt;
    segp = (HASHSEGMENT)hashp->alloc(sizeof(HASHBUCKET) * hashp->ssize);
    if (!segp) {
        return nullptr;
    }
    errno_t rc = memset_s(segp, sizeof(HASHBUCKET) * hashp->ssize, 0, sizeof(HASHBUCKET) * hashp->ssize);
    storage_securec_check(rc, "\0", "\0");

    return segp;
}

/*
 * allocate some new elements and link them into the indicated free list
 */
static bool element_alloc(HTAB* hashp, int nelem, int freelist_idx)
{
    if (unlikely(hashp == nullptr || hashp->hctl == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("invalid hashp when element_alloc."));
        return false;
    }
    HASHHDR* hctl = hashp->hctl;
    Size elementSize;
    HASHELEMENT* firstElement = nullptr;
    HASHELEMENT* tmpElement = nullptr;
    HASHELEMENT* prevElement = nullptr;
    int i;

    if (hashp->flags.isfixed) {
        return false;
    }
    /* Each element has a HASHELEMENT header plus user data. */
    elementSize = MAXALIGN(sizeof(HASHELEMENT)) + MAXALIGN(hctl->entrysize);

    if (IS_PARTITIONED(hctl)) {
        CurrentDynaHashCxt = hctl->freeList[freelist_idx].freeListCxt;
    } else {
        CurrentDynaHashCxt = hashp->hcxt;
    }

    firstElement = (HASHELEMENT*)hashp->alloc(nelem * elementSize);
    if (firstElement == nullptr) {
        return false;
    }
    CurrentDynaHashCxt = hashp->hcxt;
    errno_t rc = memset_s(firstElement, nelem * elementSize, 0, nelem * elementSize);
    storage_securec_check(rc, "\0", "\0");
    /* prepare to link all the new entries into the freelist */
    prevElement = nullptr;
    tmpElement = firstElement;
    for (i = 0; i < nelem; i++) {
        tmpElement->link = prevElement;
        prevElement = tmpElement;
        tmpElement = static_cast<HASHELEMENT*>(static_cast<void*>(static_cast<char*>(static_cast<void*>(tmpElement)) +
            elementSize));
    }

    /* if partitioned, must lock to touch freeList */
    if (IS_PARTITIONED(hctl)) {
        hctl->freeList[freelist_idx].mutex.lock();
    }
    /* freelist could be nonempty if two backends did this concurrently */
    firstElement->link = hctl->freeList[freelist_idx].freeList;
    hctl->freeList[freelist_idx].freeList = prevElement;

    if (IS_PARTITIONED(hctl)) {
        hctl->freeList[freelist_idx].mutex.unlock();
    }
    return true;
}

/* complain when we have detected a corrupted hashtable */
static void hash_corrupted(HTAB* hashp)
{
    (void(hashp));
    /*
     * If the corruption is in a shared hashtable, we'd better force a
     * systemwide restart. Otherwise, just shut down this one backend.
     */
    StorageAssert(0);
}

/* calculate ceil(log base 2) of num */
int my_log2(long number)
{
    unsigned long limit;
    int i;

    /* guard against too-large input, which would put us into infinite loop */
    if (number > LONG_MAX / 2) {
        number = LONG_MAX / 2;
    }
    for (i = 0, limit = 1; limit < (unsigned long)number; i++, limit <<= 1) {
    }
    return i;
}

/* calculate first power of 2 >= num, bounded to what will fit in a long. */
static long next_pow2_long(long num)
{
    /* my_log2's internal range check is sufficient */
    return 1L << (unsigned int)my_log2(num);
}

/* calculate first power of 2 >= num, bounded to what will fit in an int. */
static int next_pow2_int(long num)
{
    if (num > INT_MAX / 2) {
        num = INT_MAX / 2;
    }
    return 1 << (unsigned int)my_log2(num);
}

/* Register a table as having an active hash_seq_search scan. */
static void register_seq_scan(HTAB* hashp)
{
    hashp->flags.during_scan = true;
}

/* Deregister an active scan */
static void deregister_seq_scan(HTAB* hashp)
{
    hashp->flags.during_scan = false;
    // int i;

    // /* Search backward since it's most likely at the stack top */
    // for (i = t_thrd.dyhash_cxt.num_seq_scans - 1; i >= 0; i--) {
    //     if (t_thrd.dyhash_cxt.seq_scan_tables[i] == hashp) {
    //         t_thrd.dyhash_cxt.seq_scan_tables[i] =
    //             t_thrd.dyhash_cxt.seq_scan_tables[t_thrd.dyhash_cxt.num_seq_scans - 1];
    //         t_thrd.dyhash_cxt.seq_scan_level[i] =
    //             t_thrd.dyhash_cxt.seq_scan_level[t_thrd.dyhash_cxt.num_seq_scans -1];
    //         t_thrd.dyhash_cxt.num_seq_scans--; return;
    //     }
    // }
    // storage_set_error(HASH_ERROR_NO_HASH_SEQ_SEARCH_SCAN, hashp->tabname);
}

/* Release the all active scan */
void release_all_seq_scan()
{
    // t_thrd.dyhash_cxt.num_seq_scans = 0;
    return;
}

/* Check if a table has any active scan */
static bool has_seq_scans(HTAB* hashp)
{
    return hashp->flags.during_scan;
    // int i;

    // for (i = 0; i < t_thrd.dyhash_cxt.num_seq_scans; i++) {
    //     if (t_thrd.dyhash_cxt.seq_scan_tables[i] == hashp)
    //         return true;
    // }
    // return false;
}

/* get current seq scan */
int hash_get_seq_num()
{
    return 0;
    // return t_thrd.dyhash_cxt.num_seq_scans;
}

/* get current hash dynacxt */
DstoreMemoryContext hash_get_current_dynacxt(void)
{
    return CurrentDynaHashCxt;
}

/* Clean up any open scans at end of transaction */
void AtEOXact_HashTables(bool isCommit)
{
    (void(isCommit));
    /*
     * During abort cleanup, open scans are expected; just silently clean 'em out.  An open scan at commit means someone
     * forgot a hash_seq_term() call, so complain.
     *
     * Note: it's tempting to try to print the tabname here, but refrain for fear of touching deallocated memory.
     * This isn't a user-facing message anyway, so it needn't be pretty.
     */
    // if (isCommit) {
    //     int i;

    //     for (i = 0; i < t_thrd.dyhash_cxt.num_seq_scans; i++) {
    //         HTAB* htab = t_thrd.dyhash_cxt.seq_scan_tables[i];
    //         storage_set_error(HASH_WARNING_LEAKED_HASH_SEQ_SEARCH_SCAN,
    //              ((htab == NULL) ? NULL : htab->tabname));
    //     }
    // }
    // t_thrd.dyhash_cxt.num_seq_scans = 0;
}

/* Clean up any open scans at end of subtransaction */
void AtEOSubXact_HashTables(bool isCommit, int nestDepth)
{
    (void(isCommit));
    (void(nestDepth));
    /*
     * Search backward to make cleanup easy.  Note we must check all entries,
     * not only those at the end of the array, because deletion technique
     * doesn't keep them in order.
     */
    // for (i = t_thrd.dyhash_cxt.num_seq_scans - 1; i >= 0; i--) {
    //     if (t_thrd.dyhash_cxt.seq_scan_level[i] >= nestDepth) {
    //         if (isCommit) {
    //             HTAB* htab = t_thrd.dyhash_cxt.seq_scan_tables[i];
    //             storage_set_error(HASH_WARNING_LEAKED_HASH_SEQ_SEARCH_SCAN,
    //                  ((htab == NULL) ? NULL : htab->tabname));
    //         }
    //         t_thrd.dyhash_cxt.seq_scan_tables[i] =
    //             t_thrd.dyhash_cxt.seq_scan_tables[t_thrd.dyhash_cxt.num_seq_scans - 1];
    //         t_thrd.dyhash_cxt.seq_scan_level[i] =
    //             t_thrd.dyhash_cxt.seq_scan_level[t_thrd.dyhash_cxt.num_seq_scans - 1];
    //         t_thrd.dyhash_cxt.num_seq_scans--;
    //     }
    // }
}

template<HASHACTION action>
void* BufLookUp(HTAB* hashp,  const BufferTag* keyPtr, uint32 hashvalue, bool* foundPtr)
{
    if (unlikely(hashp == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("invalid hashp when BufLookUp."));
        return nullptr;
    }
    HASHHDR* hctl = hashp->hctl;
    if (unlikely(hctl == nullptr)) {
        ErrLog(DSTORE_ERROR, MODULE_COMMON, ErrMsg("invalid hashp when BufLookUp."));
        return nullptr;
    }
    int freelistIdx = static_cast<int>(hashvalue % NUM_FREELISTS);

#ifdef HASH_STATISTICS
#if HASH_STATISTICS
    hash_accesses++;
    hctl->accesses++;
#endif
#endif  // #ifdef HASH_STATISTICS
    /*
     * Do the initial lookup
     */
    uint32 bucket = calc_bucket(hctl, hashvalue);
    long segment_num = bucket >> hashp->sshift;
    long segment_ndx = MOD(bucket, hashp->ssize);
    HASHSEGMENT segp = hashp->dir[segment_num];
    HASHBUCKET* prevBucketPtr = &segp[segment_ndx];
    HASHBUCKET currBucket = *prevBucketPtr;

    while (currBucket != nullptr) {
        if (currBucket->hashvalue == hashvalue &&
            BufferTag::Match(static_cast<const BufferTag *>(static_cast<const void*>(ELEMENTKEY(currBucket))),
                keyPtr)) {
            break;
        }
        prevBucketPtr = &(currBucket->link);
        currBucket = *prevBucketPtr;
#ifdef HASH_STATISTICS
#if HASH_STATISTICS
        hash_collisions++;
        hctl->collisions++;
#endif
#endif  // #ifdef HASH_STATISTICS
    }

    if (action == HASH_ENTER) {
        *foundPtr = (bool)(currBucket != nullptr);
    }

    switch (action) {
        case HASH_FIND: {
            if (currBucket != nullptr) {
                return (void*)ELEMENTKEY(currBucket);
            }
            return nullptr;
        }

        case HASH_REMOVE: {
            if (currBucket != nullptr) {
                *static_cast<BufferTag*>(static_cast<void*>(ELEMENTKEY(currBucket))) = INVALID_BUFFER_TAG;
                GS_MEMORY_BARRIER();
                /* remove record from hash bucket's chain. */
                *prevBucketPtr = currBucket->link;

                /* if partitioned, must lock to touch nentries and freeList */
                if (IS_PARTITIONED(hctl)) {
                    hctl->freeList[freelistIdx].mutex.lock();
                    // SpinLockAcquire(&(hctl->freeList[freelist_idx].mutex));
                }
                StorageAssert(hctl->freeList[freelistIdx].nentries > 0);
                hctl->freeList[freelistIdx].nentries--;

                /* add the record to the freelist for this table.  */
                currBucket->link = hctl->freeList[freelistIdx].freeList;
                hctl->freeList[freelistIdx].freeList = currBucket;

                if (IS_PARTITIONED(hctl)) {
                    hctl->freeList[freelistIdx].mutex.unlock();
                    // SpinLockRelease(&hctl->freeList[freelist_idx].mutex);
                }

                /*
                 * better hope the caller is synchronizing access to this
                 * element, because someone else is going to reuse it the next
                 * time something is added to the table
                 */
                return (void*)ELEMENTKEY(currBucket);
            }
            return nullptr;
        }

        case HASH_ENTER: {
            /* Return existing element if found, else create one */
            if (currBucket != nullptr) {
                return (void*)ELEMENTKEY(currBucket);
            }

            currBucket = get_hash_entry(hashp, freelistIdx, false);
            if (unlikely(currBucket == nullptr)) {
                return nullptr;
            }
            currBucket->link = nullptr;

            /* copy key into record */
            currBucket->hashvalue = hashvalue;
            *static_cast<BufferTag*>(static_cast<void*>(ELEMENTKEY(currBucket))) = *keyPtr;
            GS_MEMORY_BARRIER();
            /* link into hashbucket chain */
            *prevBucketPtr = currBucket;

            /*
             * Caller is expected to fill the data field on return.
             * DO NOT insert any code that could possibly throw error here, as doing so would leave the table entry
             * incomplete and hence corrupt the caller's data structure.
             */
            return (void*)ELEMENTKEY(currBucket);
        }
        default:
            break;
    }

    storage_set_error(HASH_ERROR_UNRECOGNIZED_HASH_ACTION_CODE, (int)action);
    StorageAssert(0);

    return nullptr; /* keep compiler quiet */
}

template void* BufLookUp<HASH_FIND>(HTAB*, const BufferTag*, uint32, bool*);
template void* BufLookUp<HASH_ENTER>(HTAB*, const BufferTag*, uint32, bool*);
template void* BufLookUp<HASH_REMOVE>(HTAB*, const BufferTag*, uint32, bool*);
}
