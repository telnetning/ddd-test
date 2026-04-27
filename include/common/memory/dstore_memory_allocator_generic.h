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
 * dstore_memory_allocator_generic.h
 *
 *
 * IDENTIFICATION
 *        include/common/memory/dstore_memory_allocator_generic.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_MEMORY_ALLOCATOR_GENERIC_H
#define DSTORE_MEMORY_ALLOCATOR_GENERIC_H

#include "dstore_memory_allocator.h"
#include "errorcode/dstore_common_error_code.h"

namespace DSTORE {

#define ALLOC_BLOCKHDRSZ MAXALIGN(sizeof(GenericBlockData))
#define ALLOC_CHUNKHDRSZ MAXALIGN(sizeof(GenericChunkData))

typedef struct GenericBlockData : AllocBlockData {
    AllocBlock prev;            /* prev block in alloc_set's blocks list, if any */
    AllocBlock next;            /* next block in alloc_set's blocks list */
    char *freeptr;              /* start of free_pointer space in this block */
    char *endptr;               /* end of space in this block */
    Size alloc_size;            /* allocated size */
#ifdef MEMORY_CONTEXT_CHECKING
    uint64 magic_num;           /* DADA */
#endif
} GenericBlockData;
typedef GenericBlockData *GenericBlock;

typedef struct GenericChunkData : AllocChunkData {
    Size size;                  /* size is always the size of the usable space in the chunk */
#ifdef MEMORY_CONTEXT_CHECKING
    Size requested_size;        /* when debugging memory usage, also store actual requested size */
                                /* this is zero in a free_pointer chunk */
#endif
    const char* file; /* __FILE__ of malloc function call */
    int line;         /* __LINE__ of malloc function call */
    uint32 magicNum;                 /* prefix magic number */
} GenericChunkData;
typedef GenericChunkData *GenericChunk;

class GenericAllocSetContext : public DstoreAllocSetContext {
public:
    GenericAllocSetContext() = delete; /* no default ctor */
    ~GenericAllocSetContext() = default;
    DISALLOW_COPY_AND_MOVE(GenericAllocSetContext);

    static DstoreMemoryContext Create(DstoreMemoryContext parent, const char *name, Size minContextSize,
        Size initBlockSize, Size maxBlockSize, MemoryContextType contextType, Size maxSize);

    void *allocate(Size align, Size size, const char *file, int line) final;
    void *allocate0(Size align, Size size, const char *file, int line) final;
    void free_pointer(void *pointer) final;
    void *reallocate(void *pointer, Size align, Size size, const char *file, int line) final;
    bool init(Size allocSize) final;
    void reset() final;
    void clear() final;
    Size get_chunk_space(void *pointer) final;
#ifdef MEMORY_CONTEXT_CHECKING
    void check() final;
    void check_data_pointer(void *pointer) final;
#endif
    void check_memory_valid(AllocBlock block, AllocChunk chunk) final;

private:
    template <bool block_exclusive_occupy>
    void *p_create_block(Size block_size);
    void p_remove_block(GenericBlock block);
    void p_init_chunk(GenericChunk chunk, AllocSet alloc_set, Size requested_size, Size chunk_size,
        DstoreMemoryAllocDebugInfo *info);

    template <bool block_exclusive_occupy>
    void *p_create_chunk_from_block(GenericBlock block, AllocSet alloc_set, Size requested_size, Size chunk_size,
        DstoreMemoryAllocDebugInfo *info);
    void p_put_chunk_into_freelist(GenericChunk chunk);
    void p_put_block_freespace_into_freelist(GenericBlock block, const char *file, int line);

    GenericAllocSetContext(DstoreMemoryContext parent, const char *name, Size minContextSize, Size initBlockSize,
                           Size maxBlockSize, MemoryContextType contextType, Size maxSize, Size setSize);
};

typedef GenericAllocSetContext *GenericAllocSet;

inline AllocChunk AllocPtrGetChunk(void *ptr)
{
    return STATIC_CAST_PTR_TYPE((STATIC_CAST_PTR_TYPE((ptr), char *) - ALLOC_CHUNKHDRSZ), AllocChunk);
}
inline void *ChunkGetAllocPtr(GenericChunk chunk)
{
    return (static_cast<void *>((STATIC_CAST_PTR_TYPE(chunk, char *)) + ALLOC_CHUNKHDRSZ));
}
inline AllocBlock SingleChunkGetBlock(void *chunk)
{
    return (STATIC_CAST_PTR_TYPE(((STATIC_CAST_PTR_TYPE(chunk, char *)) - ALLOC_BLOCKHDRSZ), AllocBlock));
}

#ifdef MEMORY_CONTEXT_CHECKING
#define ALLOC_MAGICHDRSZ MAXALIGN(sizeof(AllocMagicData))
inline AllocMagicData *ChunkGetMagicPointer(GenericChunk chunk)
{
    return (STATIC_CAST_PTR_TYPE((STATIC_CAST_PTR_TYPE((chunk), char *)) +
        ALLOC_CHUNKHDRSZ + MAXALIGN((chunk)->requested_size) - ALLOC_MAGICHDRSZ, AllocMagicData *));
}
#else
#define ALLOC_MAGICHDRSZ 0
#endif

}  // namespace DSTORE
#endif /* STORAGE_MEMORY_ALLOCATOR_GENERIC_H */
