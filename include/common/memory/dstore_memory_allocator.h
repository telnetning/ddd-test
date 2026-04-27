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
 * dstore_memory_allocator.h
 *
 *
 * IDENTIFICATION
 *        include/common/memory/dstore_memory_allocator.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_MEMORY_ALLOCATOR_H
#define DSTORE_MEMORY_ALLOCATOR_H

#include <sys/mman.h>
#include <securec.h>
#include "common/memory/dstore_mctx.h"
#include "common/dstore_datatype.h"
namespace DSTORE {

#define AllocSizeIsValid(size) ((Size)(size) <= MaxAllocSize)
#define AllocHugeSizeIsValid(size) ((Size)(size) <= MaxAllocHugeSize)

class DstoreAllocSetContext;

#define ALLOCSET_NUM_FREELISTS 11
#define ALLOC_MINBITS 3 /* smallest chunk size is 8 bytes */
#ifdef MEMCHECK
#define ALLOC_CHUNK_LIMIT (1)
#else
#define ALLOC_CHUNK_LIMIT (1 << (ALLOCSET_NUM_FREELISTS - 1 + ALLOC_MINBITS))
#endif
#define ALLOC_CHUNK_FRACTION 4
#define MEMORY_CONTEXT_CELL_POS (sizeof(DstoreMemoryContextData) - sizeof(slist_node))

/*
 * Table for get_alloc_set_free_index_by_size
 */
#define LT16(n) (n), (n), (n), (n), (n), (n), (n), (n), (n), (n), (n), (n), (n), (n), (n), (n)
const unsigned char LOG_TABLE_256[256] = {
    0,
    1,
    2, 2,
    3, 3, 3, 3,
    4, 4, 4, 4, 4, 4, 4, 4,
    LT16(5),
    LT16(6), LT16(6),
    LT16(7), LT16(7), LT16(7), LT16(7),
    LT16(8), LT16(8), LT16(8), LT16(8), LT16(8), LT16(8), LT16(8), LT16(8)
};

/* Look for an index of freeList by chunk size */
inline int get_alloc_set_free_index_by_size(Size size)
{
    int idx;
    unsigned int t, tsize;

    if (size > (1 << ALLOC_MINBITS)) {
        tsize = (size - 1) >> ALLOC_MINBITS;

        /*
         * At this point we need to obtain log2(tsize)+1, ie, the number of
         * not-all-zero bits at the right.	We used to do this with a
         * shift-and-count loop, but this function is enough of a hotspot to
         * justify micro-optimization effort.  The best approach seems to be
         * to use a lookup table.  Note that this code assumes that
         * ALLOCSET_NUM_FREELISTS <= 17, since we only cope with two bytes of
         * the tsize value.
         */
        t = tsize >> 8;
        idx = t ? (LOG_TABLE_256[t] + 8) : LOG_TABLE_256[tsize];

        StorageAssert(idx < ALLOCSET_NUM_FREELISTS);
    } else {
        idx = 0;
    }
    return idx;
}

#ifdef MEMORY_CONTEXT_CHECKING
static inline void set_sentinel(void *base, Size offset)
{
    char *ptr = (char *)base + offset;
    *ptr = 0x7E;
}

static inline bool sentinel_ok(const void *base, Size offset)
{
    const char *ptr = (const char *)base + offset;
    return *ptr == 0x7E;
}
#endif

static constexpr int MMAP_HUGE_PAGE_FLAG = MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB;
static constexpr int MMAP_FLAG = MAP_PRIVATE | MAP_ANONYMOUS;

typedef void (*ReleaseMemCallBack)(size_t size, MemoryContextType type, int level);
typedef bool (*ReserveMemCallBack)(size_t size, MemoryContextType type, int level);

/* these memory-apply-macros are used only for memory context */
extern Size mmap_threshold;
/* TODO: set mmap_threshold during system initialization, (size_t)atol(gs_getenv_r("GAUSS_MMAP_THRESHOLD")) */
extern thread_local bool USE_HUGE_PAGE;
static inline void set_use_huge_page(bool flag)
{
    USE_HUGE_PAGE = flag;
}

#define mctx_free(ptr, size)           \
    do {                               \
        if ((size) < mmap_threshold) { \
            free((void *)(ptr));       \
        } else {                       \
            (void)munmap(ptr, (size)); \
        }                              \
    } while (0)

#define mctx_malloc(size, ptr, type)                                                                     \
    do {                                                                                                 \
        if ((size) < mmap_threshold) {                                                                   \
            (ptr) = (type)malloc((Size)(size));                                                          \
        } else {                                                                                         \
        RETRY:                                                                                           \
            if (USE_HUGE_PAGE) {                                                                         \
                (ptr) = (type)mmap(nullptr, (size), PROT_READ | PROT_WRITE, MMAP_HUGE_PAGE_FLAG, -1, 0); \
                if (errno == ENOMEM) {                                                                   \
                    USE_HUGE_PAGE = false;                                                               \
                    goto RETRY;                                                                          \
                }                                                                                        \
            } else {                                                                                     \
                (ptr) = (type)mmap(nullptr, (size), PROT_READ | PROT_WRITE, MMAP_FLAG, -1, 0);           \
            }                                                                                            \
        }                                                                                                \
    } while (0)

#define mctx_realloc(oldPtr, oldSize, newPtr, newSize, type)                                           \
    do {                                                                                               \
        if ((newSize) < mmap_threshold) {                                                              \
            (newPtr) = (type)realloc((oldPtr), (Size)(newSize));                                       \
        } else {                                                                                       \
            (newPtr) = (type)mmap(nullptr, (Size)(newSize), PROT_READ | PROT_WRITE, MMAP_FLAG, -1, 0); \
            if (nullptr == (newPtr)) {                                                                 \
                break;                                                                                 \
            }                                                                                          \
            errno_t rc = EOK;                                                                          \
            rc = memmove_s((newPtr), (newSize), (oldPtr), (oldSize));                                  \
            storage_securec_check(rc, "\0", "\0");                                                     \
            mctx_free((oldPtr), (oldSize));                                                            \
        }                                                                                              \
    } while (0)

typedef void *AllocSet;
typedef struct AllocBlockData {
    AllocSet alloc_set;
} AllocBlockData;
typedef AllocBlockData *AllocBlock;

typedef struct AllocChunkData {
    AllocSet alloc_set;
} AllocChunkData;
typedef AllocChunkData *AllocChunk;

const uint32 PRE_MAGIC_NUM = 0xBABABABA;
#ifdef MEMORY_CONTEXT_CHECKING
const uint64 BLK_MAGIC_NUM = 0xDADADADADADADADA;
const uint32 POS_MAGIC_NUM = 0xDCDCDCDC;
typedef struct AllocMagicData {
    AllocSet magic_alloc_set;
    Size size;                /* chunk size */
    uint32 pos_num;
} AllocMagicData;
#endif

struct MemContextCallBack {
public:
    MemContextCallBack();
    void InitMemCallBack(void *reserve, void *release);

    ReserveMemCallBack reserve_mem_callback_func;
    ReleaseMemCallBack release_mem_callback_func;
};

class DstoreAllocSetContext : public BaseObject {
public:
    DstoreAllocSetContext() = delete; /* no default ctor */
    virtual ~DstoreAllocSetContext() = default;
    DISALLOW_COPY_AND_MOVE(DstoreAllocSetContext);

    inline DstoreMemoryContext GetMemoryContext()
    {
        return &m_memory_context;
    }
    inline Size get_init_block_size()
    {
        return m_init_block_size;
    }
    inline Size get_total_size()
    {
        return m_total_space;
    }
    inline Size get_free_size()
    {
        return m_free_space;
    }
    inline AllocBlock get_block()
    {
        return m_blocks;
    }
    inline Size get_used_size()
    {
        StorageAssert(m_total_space >= m_free_space);
        return static_cast<Size>(m_total_space - m_free_space);
    }
    inline bool is_root_context()
    {
        return m_root_context;
    }
    inline void set_root_context(bool flag)
    {
        m_root_context = flag;
    }
    static MemContextCallBack &get_MemContextCallBack()
    {
        return m_memcallbackfunc;
    }

    static void LockMemoryContextIfShared(DstoreMemoryContext context);
    static void UnlockMemoryContextIfShared(DstoreMemoryContext context);
    static DstoreMemoryContext ChooseRootContext(MemoryContextType type, DstoreMemoryContext parent);

    virtual void *allocate(Size align, Size size, const char *file, int line) = 0;
    virtual void *allocate0(Size align, Size size, const char *file, int line) = 0;
    virtual void free_pointer(void *pointer) = 0;
    virtual void *reallocate(void *pointer, Size align, Size size, const char *file, int line) = 0;
    /* The constructor only handles assignment, memory allocation operations are managed by the init function. */
    virtual bool init(Size allocSize) = 0;
    virtual void reset() = 0;
    virtual void clear() = 0;
    virtual Size get_chunk_space(void *pointer) = 0;
#ifdef MEMORY_CONTEXT_CHECKING
    virtual void check() = 0;
    virtual void check_data_pointer(void *pointer) = 0;
#endif
    virtual void check_memory_valid(AllocBlock block, AllocChunk chunk) = 0;
    DstoreMemoryContextData m_memory_context; /* Standard memory-context fields */
protected:
    /* Info about storage allocated in this context: */
    AllocBlock m_blocks;                      /* head of list of blocks in this set */
    void *m_freelist[ALLOCSET_NUM_FREELISTS]; /* free_pointer chunk lists */
    /* Allocation parameters for this context: */
    Size m_init_block_size;   /* initial block size */
    Size m_max_block_size;    /* maximum block size */
    Size m_next_block_size;   /* next block size to allocate */
    Size m_alloc_chunk_limit; /* effective chunk size limit */
    AllocBlock m_keeper;      /* if not NULL, keep this block over resets */
    Size m_total_space;       /* all bytes allocated by this context */
    Size m_free_space;        /* all bytes freed by this context */
    /* maximum memory allocation of DstoreMemoryContext.For more information,we could see @StackSetContext too. */
    Size m_max_space_size;
    bool m_root_context;
    static MemContextCallBack m_memcallbackfunc;

    DstoreAllocSetContext(DstoreMemoryContext parent, const char *name, Size initBlockSize, Size maxBlockSize,
                          MemoryContextType type, Size maxSize, Size setSize);
};

}  // namespace DSTORE

#endif /* STORAGE_MEMORY_ALLOCATOR_H */
