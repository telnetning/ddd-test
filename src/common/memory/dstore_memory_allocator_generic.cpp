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
 * dstore_memory_allocator_generic.cpp
 *
 *
 * IDENTIFICATION
 *        src/common/memory/dstore_memory_allocator_generic.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "common/memory/dstore_memory_allocator_generic.h"
#include "common/error/dstore_error.h"
#include "common/memory/dstore_mctx.h"
#include "errorcode/dstore_common_error_code.h"
#include "framework/dstore_thread.h"

namespace DSTORE {

MemContextCallBack DstoreAllocSetContext::m_memcallbackfunc = MemContextCallBack();
static thread_local bool enableMemoryControl = true;

/* remove block from AllocSetContext's list and free it */
void GenericAllocSetContext::p_remove_block(GenericBlock block)
{
    Size block_size = block->alloc_size;
    m_total_space -= block_size;

    if (block->prev) {
        ((GenericBlock)(block->prev))->next = block->next;
    } else {
        m_blocks = block->next;
    }
    if (block->next) {
        ((GenericBlock)(block->next))->prev = block->prev;
    }

    /* clean the structure of block */
    block->alloc_set = nullptr;
    block->prev = nullptr;
    block->next = nullptr;
    block->freeptr = nullptr;
    block->endptr = nullptr;
    block->alloc_size = 0;

    mctx_free(block, block_size);
    if (unlikely(DstoreAllocSetContext::get_MemContextCallBack().release_mem_callback_func != nullptr)
        && enableMemoryControl) {
        ReleaseMemCallBack dstore_release_mem =
            DstoreAllocSetContext::get_MemContextCallBack().release_mem_callback_func;
        dstore_release_mem((int64_t)block_size, m_memory_context.type,
                           (m_memory_context.type == MemoryContextType::THREAD_CONTEXT && thrd != nullptr)
                               ? static_cast<int>(thrd->GetLevel())
                               : -1);
    }
}

/*
 * block exclusive occupy means the size we need exceed chunk limit , then we create a whole block for that.
 * we don't lose the use of the space remaining therein.
 */
template<bool block_exclusive_occupy>
void *GenericAllocSetContext::p_create_block(Size block_size)
{
    GenericBlock block;
    if (unlikely(DstoreAllocSetContext::get_MemContextCallBack().reserve_mem_callback_func != nullptr)
        && enableMemoryControl) {
        ReserveMemCallBack dstore_reserve_mem =
            DstoreAllocSetContext::get_MemContextCallBack().reserve_mem_callback_func;
        bool result = dstore_reserve_mem(block_size, m_memory_context.type,
                                         (m_memory_context.type == MemoryContextType::THREAD_CONTEXT && thrd != nullptr)
                                             ? static_cast<int>(thrd->GetLevel())
                                             : -1);
        if (unlikely(!result)) {
            return nullptr;
        }
    }
    mctx_malloc(block_size, block, GenericBlock);
    if (block == nullptr || block == MAP_FAILED) {
        return nullptr;
    }

    block->alloc_set = (AllocSet)this;

    if (block_exclusive_occupy) {
        block->freeptr = STATIC_CAST_PTR_TYPE(block, char *) + block_size;
        block->endptr = STATIC_CAST_PTR_TYPE(block, char *) + block_size;
    } else {
        block->freeptr = STATIC_CAST_PTR_TYPE(block, char *) + ALLOC_BLOCKHDRSZ;
        block->endptr = STATIC_CAST_PTR_TYPE(block, char *) + block_size;
    }

    block->alloc_size = block_size;
#ifdef MEMORY_CONTEXT_CHECKING
    block->magic_num = BLK_MAGIC_NUM;
#endif

    m_total_space += block_size;

    if (!block_exclusive_occupy) {
        m_free_space += block_size - ALLOC_BLOCKHDRSZ;
    }
    /*
     * If this is the first block of the set, make it the "keeper" block.
     * Formerly, a keeper block could only be created during context
     * creation, but allowing it to happen here lets us have fast reset
     * cycling even for contexts created with minContextSize = 0; that way
     * we don't have to force space to be allocated in contexts that might
     * never need any space.  Don't mark an oversize block as a keeper,
     * however.
     */
    if (!block_exclusive_occupy && (m_keeper == nullptr && block_size == m_init_block_size)) {
        m_keeper = (AllocBlock)block;
    }

    if (m_blocks != nullptr) {
        if (block_exclusive_occupy) {
            /*
            * Stick the new block underneath the active allocation block, so that
            * we don't lose the use of the space remaining therein.
            */
            block->prev = m_blocks;
            block->next = ((GenericBlock)m_blocks)->next;
            if (block->next) {
                ((GenericBlock)(block->next))->prev = block;
            }
            ((GenericBlock)m_blocks)->next = block;
        } else {
            block->prev = nullptr;
            block->next = (AllocBlock)(((GenericBlock)m_blocks));
            if (block->next) {
                ((GenericBlock)(block->next))->prev = (AllocBlock)block;
            }
            m_blocks = (AllocBlock)block;
        }
    } else {
        block->prev = nullptr;
        block->next = nullptr;
        m_blocks = (AllocBlock)block;
    }
    return (void *)block;
}

void GenericAllocSetContext::p_init_chunk(GenericChunk chunk, AllocSet alloc_set,
                                          Size requested_size, Size chunk_size, DstoreMemoryAllocDebugInfo *info)
{
    StorageAssert(DstorePointerIsValid(chunk));
    UNUSED_VARIABLE(requested_size);
    chunk->alloc_set = (AllocSet)alloc_set;
    chunk->size = chunk_size;
#ifdef MEMORY_CONTEXT_CHECKING
    if (requested_size > 0) {
        StorageAssert(requested_size >= ALLOC_MAGICHDRSZ);
        chunk->requested_size = requested_size;
        /* set mark to catch clobber of "unused" space */
        if (requested_size < chunk_size) {
            set_sentinel(ChunkGetAllocPtr(chunk), requested_size - ALLOC_MAGICHDRSZ);
        }

        AllocMagicData *magic = ChunkGetMagicPointer(chunk);
        magic->magic_alloc_set = (AllocSet)this;
        magic->size = chunk->size;
        magic->pos_num = POS_MAGIC_NUM;
    }
#endif
    chunk->file = info->file;
    chunk->line = info->line;
    chunk->magicNum = info->magicNum;
}

void GenericAllocSetContext::p_put_chunk_into_freelist(GenericChunk chunk)
{
    int fidx = get_alloc_set_free_index_by_size(chunk->size);
    chunk->alloc_set =  (AllocSet *)m_freelist[fidx];
    m_free_space += chunk->size + ALLOC_CHUNKHDRSZ;

#ifdef MEMORY_CONTEXT_CHECKING
    AllocMagicData *magic = ChunkGetMagicPointer(chunk);
    magic->magic_alloc_set = nullptr;
    magic->size = 0;
    magic->pos_num = 0;
    /* reset requested_size to 0 in chunks that are on freelist */
    chunk->requested_size = 0;
#endif
    /* Set magic num to 0, that means the chunk is in freelist. */
    chunk->magicNum = 0;
    m_freelist[fidx] = (AllocSet)chunk;
    StorageAssert(chunk->alloc_set != this);
}

template<bool block_exclusive_occupy>
void *GenericAllocSetContext::p_create_chunk_from_block(GenericBlock block, AllocSet alloc_set,
    Size requested_size, Size chunk_size, DstoreMemoryAllocDebugInfo *info)
{
    StorageAssert(DstorePointerIsValid(block));
    GenericChunk chunk;
    if (!block_exclusive_occupy) {
        chunk = STATIC_CAST_PTR_TYPE(block->freeptr, GenericChunk);
        block->freeptr += (chunk_size + ALLOC_CHUNKHDRSZ);
        m_free_space -= (chunk_size + ALLOC_CHUNKHDRSZ);
    } else {
        chunk = STATIC_CAST_PTR_TYPE(STATIC_CAST_PTR_TYPE(block, char*) + ALLOC_BLOCKHDRSZ, GenericChunk);
    }
    StorageAssert(block->freeptr <= block->endptr);

    p_init_chunk(chunk, alloc_set, requested_size, chunk_size, info);

    return (void *)chunk;
}

void GenericAllocSetContext::p_put_block_freespace_into_freelist(GenericBlock block, const char *file, int line)
{
    StorageAssert(DstorePointerIsValid(block));
    /*
     * The existing active (top) block does not have enough room for
     * the requested allocation, but it might still have a useful
     * amount of space in it.  Once we push it down in the block list,
     * we'll never try to allocate more space from it. So, before we
     * do that, carve up its free space into chunks that we can put on
     * the set's freelists.
     *
     * Because we can only get here when there's less than
     * ALLOC_CHUNK_LIMIT left in the block, this loop cannot iterate
     * more than ALLOCSET_NUM_FREELISTS-1 times.
     */
    Size avail_space = block->endptr - block->freeptr;
    GenericChunk chunk;

    Size smallest_chunk = ((1 << ALLOC_MINBITS) + ALLOC_CHUNKHDRSZ);
    while (avail_space >= smallest_chunk) {
        Size avail_chunk = avail_space - ALLOC_CHUNKHDRSZ;
        int a_fidx = get_alloc_set_free_index_by_size(avail_chunk);
        /*
         * In most cases, we'll get back the index of the next larger
         * freelist than the one we need to put this chunk on. The
         * exception is when avail_chunk is exactly a power of 2.
         */
        if (a_fidx > 0 && avail_chunk != ((Size)1 << ((unsigned int)a_fidx + ALLOC_MINBITS))) {
            a_fidx--;
            StorageAssert(a_fidx >= 0);
            avail_chunk = ((Size)1 << ((unsigned int)a_fidx + ALLOC_MINBITS));
        }
        /* create a chunk from the block than put it into the freelist. */
        DstoreMemoryAllocDebugInfo info = {
            .file = file,
            .line = line,
            .magicNum = 0
        };
        chunk =
            (GenericChunk)p_create_chunk_from_block<false>(block, (AllocSet)m_freelist[a_fidx], 0, avail_chunk, &info);
        avail_space -= (avail_chunk + ALLOC_CHUNKHDRSZ);
#ifdef MEMORY_CONTEXT_CHECKING
        chunk->requested_size = 0;
#endif
        m_free_space += chunk->size + ALLOC_CHUNKHDRSZ;
        m_freelist[a_fidx] = (AllocSet)chunk;
    }
}

/* public interfaces */
GenericAllocSetContext::GenericAllocSetContext(DstoreMemoryContext parent, const char *name,
                                               UNUSE_PARAM Size minContextSize, Size initBlockSize, Size maxBlockSize,
                                               MemoryContextType contextType, Size maxSize, Size setSize)
    : DstoreAllocSetContext::DstoreAllocSetContext(parent, name, initBlockSize, maxBlockSize, contextType, maxSize,
                                                   setSize)
{
    m_alloc_chunk_limit = ALLOC_CHUNK_LIMIT;
    while (static_cast<Size>(m_alloc_chunk_limit + ALLOC_CHUNKHDRSZ) >
           static_cast<Size>((m_max_block_size - ALLOC_BLOCKHDRSZ) / ALLOC_CHUNK_FRACTION)) {
        m_alloc_chunk_limit >>= 1;
    }
}

DstoreMemoryContext GenericAllocSetContext::Create(DstoreMemoryContext parent, const char *name,
                                                   Size minContextSize, Size initBlockSize, Size maxBlockSize,
                                                   MemoryContextType contextType, Size maxSize)
{
    StorageAssert(contextType == MemoryContextType::THREAD_CONTEXT ||
                  contextType == MemoryContextType::SESSION_CONTEXT ||
                  contextType == MemoryContextType::SHARED_CONTEXT);

    GenericAllocSet allocSetContext;
    DstoreMemoryContext root = ChooseRootContext(contextType, parent);

    Size setSize = sizeof(GenericAllocSetContext);
    if (!AllocSizeIsValid(setSize)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("invalid memory allocate request size %lu",
            static_cast<unsigned long>(setSize)));
        return nullptr;
    }

    if (likely(root != nullptr)) {
        allocSetContext = DstoreNew(root) GenericAllocSetContext(
            parent, name, minContextSize, initBlockSize, maxBlockSize, contextType, maxSize, setSize);
    } else {
        /* Special case for startup */
        allocSetContext = new (nullptr, __FILE__, __LINE__, std::nothrow, true) GenericAllocSetContext(
            parent, name, minContextSize, initBlockSize, maxBlockSize, contextType, maxSize, setSize);
        if (allocSetContext == nullptr) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed on request of size %lu bytes in %s:%d.",
                static_cast<unsigned long>(setSize), __FILE__, __LINE__));
            return nullptr;
        }
        allocSetContext->set_root_context(true);
    }

    if (allocSetContext == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed on request of size %lu bytes in %s:%d.",
            static_cast<unsigned long>(setSize), __FILE__, __LINE__));
        return nullptr;
    }

    if (!allocSetContext->init(minContextSize)) {
        /* If context alloc init failed, we just keep context alive to use, do not free and return nullptr. */
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
               ErrMsg("Failed on request of size %lu bytes in %s:%d.", static_cast<unsigned long>(minContextSize),
                      __FILE__, __LINE__));
    }

    return &(allocSetContext->m_memory_context);
}

void *GenericAllocSetContext::allocate(__attribute__((__unused__)) Size align, Size size, const char *file, int line)
{
    StorageAssert(align == 0);

    GenericBlock block;
    GenericChunk chunk;
    Size chunk_size;
    Size block_size;
    DstoreMemoryAllocDebugInfo info = {
        .file = file,
        .line = line,
        .magicNum = PRE_MAGIC_NUM
    };

    /* If this is a shared context, make it thread safe by acquiring appropriate lock */
    LockMemoryContextIfShared(&m_memory_context);

#ifdef MEMORY_CONTEXT_CHECKING
    StorageAssert(m_memory_context.type != MemoryContextType::THREAD_CONTEXT ||
                  m_memory_context.thread_id == pthread_self());
#endif

    m_memory_context.is_reset = false;

    size += ALLOC_MAGICHDRSZ;
    /* If required size is greater than m_alloc_chunk_limit, create a new block to put the chunk in */
    /* this new block will be a single-chunk block */
    if (size > m_alloc_chunk_limit) {
        chunk_size = MAXALIGN(size);
        block_size = chunk_size + ALLOC_BLOCKHDRSZ + ALLOC_CHUNKHDRSZ;

        block = (GenericBlockData *)p_create_block<true>(block_size);
        if (block == nullptr) {
            UnlockMemoryContextIfShared(&m_memory_context);
            return nullptr;
        }
        chunk = (GenericChunk)p_create_chunk_from_block<true>(block, (AllocSet)this, size, chunk_size, &info);
        UnlockMemoryContextIfShared(&m_memory_context);
        return ChunkGetAllocPtr(chunk);
    }

    /* Try to find a chunk in freelist */
    unsigned int fidx = get_alloc_set_free_index_by_size(size);
    chunk = (GenericChunk)m_freelist[fidx];
    if (chunk != nullptr) {
        StorageAssert(chunk->size >= size);
        StorageAssert(chunk->alloc_set != this);
        StorageAssert((int)fidx == get_alloc_set_free_index_by_size(chunk->size));

        m_freelist[fidx] = ((AllocChunk)chunk)->alloc_set;
        p_init_chunk(chunk, (AllocSet)this, size, chunk->size, &info);
        m_free_space -= (chunk->size + ALLOC_CHUNKHDRSZ);

        UnlockMemoryContextIfShared(&m_memory_context);
        return ChunkGetAllocPtr(chunk);
    }

    /* Failed to find a chunk in freelist, now try to put it into a block */
    /* Choose the actual chunk size to allocate. */
    chunk_size = ((unsigned long)1 << ALLOC_MINBITS) << fidx;
    StorageAssert(chunk_size >= size);

    /*
     * If there is enough room in the active allocation block, we will put the
     * chunk into that block.  Else must start a new one.
     */
    block = (GenericBlock)m_blocks;
    if (block != nullptr) {
        Size avail_space = block->endptr - block->freeptr;
        if (avail_space < (chunk_size + ALLOC_CHUNKHDRSZ)) {
            p_put_block_freespace_into_freelist(block, file, line);
            /* Mark that we need to create a new block */
            block = nullptr;
        }
    }
    /*
     * Time to create a new regular (multi-chunk) block?
     */
    if (block == nullptr) {
        /*
         * The first such block has size initBlockSize, and we double the
         * space in each succeeding block, but not more than maxBlockSize.
         */
        block_size = m_next_block_size;
        m_next_block_size <<= 1;
        m_next_block_size = (m_next_block_size > m_max_block_size) ? m_max_block_size : m_next_block_size;

        /*
         * If initBlockSize is less than ALLOC_CHUNK_LIMIT, we could need more
         * space... but try to keep it a power of 2.
         */
        Size required_size = chunk_size + ALLOC_BLOCKHDRSZ + ALLOC_CHUNKHDRSZ;
        while (block_size < required_size) {
            block_size <<= 1;
        }

        /* Try to allocate it */
        block = (GenericBlock)p_create_block<false>(block_size);
        if (block == nullptr) {
            UnlockMemoryContextIfShared(&m_memory_context);
            return nullptr;
        }
    }
    /*
     * OK, do the allocation
     */
    chunk = (GenericChunk)p_create_chunk_from_block<false>(block, (AllocSet)this, size, chunk_size, &info);
    UnlockMemoryContextIfShared(&m_memory_context);
    return ChunkGetAllocPtr(chunk);
}

void *GenericAllocSetContext::allocate0(Size align, Size size, const char *file, int line)
{
    void *data = allocate(align, size, file, line);

    if (unlikely(data == nullptr)) {
        return nullptr;
    }
    errno_t rc = memset_s(data, size, 0, size);
    storage_securec_check(rc, "\0", "\0");

    return data;
}

void GenericAllocSetContext::free_pointer(void *pointer)
{
    /*
    * If this is a shared context, make it thread safe by acquiring
    * appropriate lock
    */
    LockMemoryContextIfShared(&m_memory_context);

    GenericChunk chunk = static_cast<GenericChunk>(AllocPtrGetChunk(pointer));
#ifdef MEMORY_CONTEXT_CHECKING
    /* Test for someone scribbling on unused space in chunk */
    if (chunk->requested_size != static_cast<Size>(MAXALIGN(chunk->requested_size)) &&
        chunk->requested_size < chunk->size) {
        StorageAssert(chunk->requested_size > ALLOC_MAGICHDRSZ);
        if (!sentinel_ok(pointer, chunk->requested_size - ALLOC_MAGICHDRSZ)) {
            UnlockMemoryContextIfShared(&m_memory_context);
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("detected write past chunk end in %s",
                m_memory_context.name));
            StorageAssert(0);
        }
    }
    check_data_pointer(pointer);
#endif

    if (chunk->size > m_alloc_chunk_limit) {
        /*
         * Big chunks are certain to have been allocated as single-chunk
         * blocks. Find the containing block and return it to malloc().
         */
        GenericBlock block = static_cast<GenericBlock>(SingleChunkGetBlock(static_cast<void*>(chunk)));
        /*
         * Try to verify that we have a sane block pointer: it should
         * reference the correct alloc_set, and freeptr and endptr should point
         * just past the chunk.
         */
        check_memory_valid((AllocBlock)block, (AllocChunk)chunk);

        /* OK, remove block from alloc_set's list and free it */
        p_remove_block(block);
    } else {
        /* Normal case, put the chunk into appropriate freelist */
        p_put_chunk_into_freelist(chunk);
    }
    UnlockMemoryContextIfShared(&m_memory_context);
}

void *GenericAllocSetContext::reallocate(void *pointer, __attribute__((__unused__)) Size align, Size size,
    const char *file, int line)
{
    StorageAssert(align == 0);

    GenericChunk chunk = static_cast<GenericChunk>(AllocPtrGetChunk(pointer));
    Size old_size = chunk->size;

    LockMemoryContextIfShared(&m_memory_context);

#ifdef MEMORY_CONTEXT_CHECKING
    /* Test for someone scribbling on unused space in chunk */
    if (chunk->requested_size != static_cast<Size>(MAXALIGN(chunk->requested_size)) &&
        chunk->requested_size < old_size) {
        if (!sentinel_ok(pointer, chunk->requested_size - ALLOC_MAGICHDRSZ)) {
            UnlockMemoryContextIfShared(&m_memory_context);
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("detected write past chunk end in %s",
                m_memory_context.name));
            return nullptr;
        }
    }
    check_data_pointer(pointer);
#endif

    /*
     * Chunk sizes are aligned to power of 2 in AllocSetAlloc(). Maybe the
     * allocated area already is >= the new size.  (In particular, we always
     * fall out here if the requested size is a decrease.)
     */
    if (old_size >= (size + ALLOC_MAGICHDRSZ)) {
        size += ALLOC_MAGICHDRSZ;

#ifdef MEMORY_CONTEXT_CHECKING
        DstoreMemoryAllocDebugInfo info = {
            .file = file,
            .line = line,
            .magicNum = PRE_MAGIC_NUM
        };
        p_init_chunk(chunk, (AllocSet)this, size, chunk->size, &info);
#endif
        UnlockMemoryContextIfShared(&m_memory_context);
        return pointer;
    }

    if (old_size > m_alloc_chunk_limit) {
        /*
         * The chunk must have been allocated as a single-chunk block. Find
         * the containing block and use reallocate() to make it bigger with
         * minimum space wastage.
         */
        GenericBlock block = static_cast<GenericBlock>(SingleChunkGetBlock(static_cast<void*>(chunk)));
        /*
         * Try to verify that we have a sane block pointer: it should
         * reference the correct alloc_set, and freeptr and endptr should point
         * just past the chunk.
         */
        check_memory_valid((AllocBlock)block, (AllocChunk)chunk);

        /* Do the reallocate */
        GenericBlock old_block = block;
        old_size = old_block->alloc_size;
        size += ALLOC_MAGICHDRSZ;
        Size chunk_size = MAXALIGN(size);
        Size block_size = chunk_size + ALLOC_BLOCKHDRSZ + ALLOC_CHUNKHDRSZ;

        if (old_size > block_size) {
            Size change_size = old_size - block_size;
            if (unlikely(DstoreAllocSetContext::get_MemContextCallBack().release_mem_callback_func != nullptr) &&
                enableMemoryControl) {
                ReleaseMemCallBack dstore_release_mem =
                    DstoreAllocSetContext::get_MemContextCallBack().release_mem_callback_func;
                dstore_release_mem((int64_t)change_size, m_memory_context.type,
                                   (m_memory_context.type == MemoryContextType::THREAD_CONTEXT && thrd != nullptr)
                                       ? static_cast<int>(thrd->GetLevel())
                                       : -1);
            }
        } else {
            Size change_size = block_size - old_size;
            if (unlikely(DstoreAllocSetContext::get_MemContextCallBack().reserve_mem_callback_func != nullptr) &&
                enableMemoryControl) {
                ReserveMemCallBack dstore_reserve_mem =
                    DstoreAllocSetContext::get_MemContextCallBack().reserve_mem_callback_func;
                bool result =
                    dstore_reserve_mem(change_size, m_memory_context.type,
                                       (m_memory_context.type == MemoryContextType::THREAD_CONTEXT && thrd != nullptr)
                                           ? static_cast<int>(thrd->GetLevel())
                                           : -1);
                if (unlikely(!result)) {
                    UnlockMemoryContextIfShared(&m_memory_context);
                    return nullptr;
                }
            }
        }
        mctx_realloc(old_block, old_size, block, block_size, GenericBlock);
        if (block == nullptr) {
            UnlockMemoryContextIfShared(&m_memory_context);
            return nullptr;
        }

        block->freeptr = block->endptr = ((char *)block) + block_size;
        block->alloc_size = block_size;
#ifdef MEMORY_CONTEXT_CHECKING
        block->magic_num = BLK_MAGIC_NUM;
#endif
        /* Update pointers since block has likely been moved */
        if (block->prev) {
            (static_cast<GenericBlock>(block->prev))->next = static_cast<AllocBlock>(block);
        } else {
            m_blocks = block;
        }
        if (block->next) {
            (static_cast<GenericBlock>(block->next))->prev = static_cast<AllocBlock>(block);
        }

        m_total_space += block_size - old_size;

        chunk = STATIC_CAST_PTR_TYPE((STATIC_CAST_PTR_TYPE(block, char *) + ALLOC_BLOCKHDRSZ), GenericChunk);
        chunk->size = chunk_size;
#ifdef MEMORY_CONTEXT_CHECKING
        DstoreMemoryAllocDebugInfo info = {
            .file = file,
            .line = line,
            .magicNum = PRE_MAGIC_NUM
        };
        p_init_chunk(chunk, (AllocSet)this, size, chunk_size, &info);
#endif
        UnlockMemoryContextIfShared(&m_memory_context);
        return ChunkGetAllocPtr(chunk);
    } else {
        /*
         * Small-chunk case.  We just do this by brute force, ie, allocate a
         * new chunk and copy the data.  Since we know the existing data isn't
         * huge, this won't involve any great memcpy expense, so it's not
         * worth being smarter.  (At one time we tried to avoid memcpy when it
         * was possible to enlarge the chunk in-place, but that turns out to
         * misbehave unpleasantly for repeated cycles of
         * palloc/repalloc/pfree: the eventually freed chunks go into the
         * wrong freelist for the next initial palloc request, and so we leak
         * memory indefinitely.  See pgsql-hackers archives for 2007-08-11.)
         */

        UnlockMemoryContextIfShared(&m_memory_context);
        /* allocate new chunk */
        void *new_pointer = allocate(align, size, file, line);
        /* leave immediately if request was not completed */
        if (new_pointer == nullptr) {
            return nullptr;
        }

        /* transfer existing data (certain to fit) */
        errno_t rc = 0;
#ifdef MEMORY_CONTEXT_CHECKING
        Size mem_size = MAXALIGN(chunk->requested_size) - ALLOC_MAGICHDRSZ;
        if (mem_size != 0) {
            rc = memcpy_s(new_pointer, mem_size, pointer, mem_size);
            storage_securec_check(rc, "\0", "\0");
        }
#else
        rc = memcpy_s(new_pointer, old_size, pointer, old_size);
        storage_securec_check(rc, "\0", "\0");
#endif

        /* free old chunk */
        free_pointer(pointer);

        return new_pointer;
    }
}

bool GenericAllocSetContext::init(Size allocSize)
{
    if (allocSize > ALLOC_BLOCKHDRSZ + ALLOC_CHUNKHDRSZ) {
        Size blockSize = MAXALIGN(allocSize);
        GenericBlock block = static_cast<GenericBlock>(p_create_block<false>(blockSize));
        if (block == nullptr) {
            return false;
        }
    }
    return true;
}

void GenericAllocSetContext::reset()
{
    LockMemoryContextIfShared(&m_memory_context);

#ifdef MEMORY_CONTEXT_CHECKING
    /* check for corruption and leaks before freeing */
    check();
#endif

    /* Clear chunk freelists */
    errno_t rc = memset_s(m_freelist, sizeof(m_freelist), 0, sizeof(m_freelist));
    storage_securec_check(rc, "\0", "\0");

    /* empty blocks except the keepeer */
    GenericBlock block = static_cast<GenericBlock>(m_blocks);
    while (block != nullptr) {
        GenericBlock next = (GenericBlock)block->next;
        Size blockSize = block->alloc_size;

        if (block == static_cast<GenericBlock>(m_keeper)) {
            /* reset the keeper block, but do not return it to malloc */
            block->freeptr = (STATIC_CAST_PTR_TYPE(block, char *) + ALLOC_BLOCKHDRSZ);
            block->next = nullptr;
            block->prev = nullptr;
        } else {
            mctx_free(block, blockSize);
            if (unlikely(DstoreAllocSetContext::get_MemContextCallBack().release_mem_callback_func != nullptr)
                && enableMemoryControl) {
                ReleaseMemCallBack dstore_release_mem =
                    DstoreAllocSetContext::get_MemContextCallBack().release_mem_callback_func;
                dstore_release_mem((int64_t)blockSize, m_memory_context.type,
                                   (m_memory_context.type == MemoryContextType::THREAD_CONTEXT && thrd != nullptr)
                                       ? static_cast<int>(thrd->GetLevel())
                                       : -1);
            }
        }
        block = next;
    }

    /* reset block size allocation sequence, too */
    if (m_keeper != nullptr) {
        /* calculate memory statisic after reset. */
        block = (GenericBlock)m_keeper;
        m_free_space = block->endptr - block->freeptr;
        m_total_space = m_free_space + ALLOC_BLOCKHDRSZ;
    } else {
        m_free_space = 0;
        m_total_space = 0;
    }
    m_blocks = m_keeper;
    m_next_block_size = m_init_block_size;

    UnlockMemoryContextIfShared(&m_memory_context);
}

void GenericAllocSetContext::clear()
{
    if (m_blocks == nullptr) {
        return;
    }

    LockMemoryContextIfShared(&m_memory_context);

#ifdef MEMORY_CONTEXT_CHECKING
    /* check for corruption and leaks before freeing */
    check();
#endif

    GenericBlock block = (GenericBlock)m_blocks;
    /* Make it look empty, just in case... */
    errno_t rc = memset_s(m_freelist, sizeof(m_freelist), 0, sizeof(m_freelist));
    storage_securec_check(rc, "\0", "\0");
    m_blocks = nullptr;
    m_keeper = nullptr;

    while (block != nullptr) {
        GenericBlock next = (GenericBlock)block->next;
        Size blockSize = block->alloc_size;
        mctx_free(block, blockSize);
        block = next;
        if (unlikely(DstoreAllocSetContext::get_MemContextCallBack().release_mem_callback_func != nullptr)
            && enableMemoryControl) {
            ReleaseMemCallBack dstore_release_mem =
                DstoreAllocSetContext::get_MemContextCallBack().release_mem_callback_func;
            dstore_release_mem((int64_t)blockSize, m_memory_context.type,
                               (m_memory_context.type == MemoryContextType::THREAD_CONTEXT && thrd != nullptr)
                                   ? static_cast<int>(thrd->GetLevel())
                                   : -1);
        }
    }

    /* reset to 0 after deletion. */
    m_free_space = 0;
    m_total_space = 0;

    UnlockMemoryContextIfShared(&m_memory_context);
}

Size GenericAllocSetContext::get_chunk_space(void *pointer)
{
    GenericChunk chunk = static_cast<GenericChunk>(AllocPtrGetChunk(pointer));
    return chunk->size + ALLOC_CHUNKHDRSZ;
}
#ifdef MEMORY_CONTEXT_CHECKING
void GenericAllocSetContext::check()
{
    GenericBlock prevBlock;
    GenericBlock block;

    for (prevBlock = nullptr, block = static_cast<GenericBlock>(m_blocks);
        block != nullptr;
        prevBlock = block, block = static_cast<GenericBlock>(block->next)) {
        char *blockBody = (STATIC_CAST_PTR_TYPE(block, char *) + ALLOC_BLOCKHDRSZ);
        long blockUsed = block->freeptr - blockBody;
        unsigned long blockData = 0;
        long numChunks = 0;
        long numFreedChunks = 0;

        /* Empty block - empty can be keeper-block only */
        if (blockUsed == 0 && block != static_cast<GenericBlock>(m_keeper)) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("problem in allocate set %s: empty block",
                m_memory_context.name));
            StorageAssert(0);
        }

        /* check block header fields */
        if (block->alloc_set != STATIC_CAST_PTR_TYPE(this, AllocSet*) || block->prev != prevBlock ||
            block->freeptr < blockBody || block->freeptr > block->endptr) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("problem in allocate set %s: corrupt header in block",
                m_memory_context.name));
            StorageAssert(0);
        }

        /* Chunk walker */
        while (blockBody < block->freeptr) {
            GenericChunk chunk = STATIC_CAST_PTR_TYPE(blockBody, GenericChunk);

            Size chunkSize = chunk->size;          /* aligned chunk size */
            Size dataSize = chunk->requested_size; /* real data */

            if (chunk->alloc_set == nullptr) {
                /* this chunk is already freed and put into freelist */
                blockData += chunkSize;
                blockBody += (ALLOC_CHUNKHDRSZ + chunkSize);
                numFreedChunks++;
                continue;
            }

            /* check chunk size */
            if (dataSize > chunkSize) {
                ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("problem in alloc set %s: req size > allocate size",
                    m_memory_context.name));
                StorageAssert(0);
            }
            if (chunkSize < (1 << ALLOC_MINBITS)) {
                ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("problem in allocate set %s: bad size %lu",
                    m_memory_context.name, static_cast<unsigned long>(chunkSize)));
                StorageAssert(0);
            }

            /* single-chunk block? */
            if (chunkSize > m_alloc_chunk_limit && static_cast<long>((chunkSize + ALLOC_CHUNKHDRSZ)) != blockUsed) {
                ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("problem in allocate set %s: bad single-chunk in block",
                    m_memory_context.name));
                StorageAssert(0);
            }

            /*
             * If chunk is allocated, check for correct alloc_set pointer. (If it's
             * free, the alloc_set is the freelist pointer, which we can't check as
             * easily...)
             */
            if (dataSize > 0 && chunk->alloc_set != static_cast<AllocSet>(this)) {
                ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("problem in allocate set %s: bogus alloc_set link",
                    m_memory_context.name));
                StorageAssert(0);
            }

            /* check for overwrite of "unallocated" space in chunk */
            if (dataSize > 0 && dataSize < chunkSize && dataSize != static_cast<Size>(MAXALIGN(dataSize)) &&
                !sentinel_ok(chunk, ALLOC_CHUNKHDRSZ + dataSize - ALLOC_MAGICHDRSZ)) {
                ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("problem in allocate set %s: detected write past "
                    "chunk end", m_memory_context.name));
                StorageAssert(0);
            }

            blockData += chunkSize;
            blockBody += (ALLOC_CHUNKHDRSZ + chunkSize);
            numChunks++;
        }

        if ((blockData + (static_cast<unsigned long>(numChunks + numFreedChunks) * ALLOC_CHUNKHDRSZ)) !=
            static_cast<unsigned long>(blockUsed)) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("problem in allocate set %s: found inconsistent memory block",
                m_memory_context.name));
            StorageAssert(0);
        }
    }
}
#endif

void GenericAllocSetContext::check_memory_valid(AllocBlock block, AllocChunk chunk)
{
    GenericBlock generic_block = (GenericBlock)block;
    GenericChunk generic_chunk = (GenericChunk)chunk;

    if (generic_block->alloc_set != this) {
        UnlockMemoryContextIfShared(&((static_cast<GenericAllocSet>(generic_block->alloc_set))->m_memory_context));
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("The block was freed before this time."));
        StorageAssert(0);
    }

    if (generic_block->freeptr != generic_block->endptr || generic_block->freeptr !=
        STATIC_CAST_PTR_TYPE(generic_block, char *) + generic_chunk->size + ALLOC_BLOCKHDRSZ + ALLOC_CHUNKHDRSZ) {
        UnlockMemoryContextIfShared(&((static_cast<GenericAllocSet>(generic_block->alloc_set))->m_memory_context));
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("The memory use was overflow."));
        StorageAssert(0);
    }
}

#ifdef MEMORY_CONTEXT_CHECKING
void GenericAllocSetContext::check_data_pointer(void *pointer)
{
    GenericChunk chunk = static_cast<GenericChunk>(AllocPtrGetChunk(pointer));
    AllocMagicData *magic = ChunkGetMagicPointer(chunk);
    StorageAssert(magic->magic_alloc_set == static_cast<AllocSet>(this) && magic->size == chunk->size &&
                  magic->pos_num == POS_MAGIC_NUM);
}
#endif

void DstoreSetMemoryOutOfControl()
{
    enableMemoryControl = false;
}

void DstoreSetMemoryInControl()
{
    enableMemoryControl = true;
}

} /* DSTORE */
