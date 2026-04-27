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
 * dstore_memory_allocator_stack.h
 *
 *
 * IDENTIFICATION
 *        include/common/memory/dstore_memory_allocator_stack.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_MEMORY_ALLOCATOR_STACK_H
#define DSTORE_MEMORY_ALLOCATOR_STACK_H

#include "common/memory/dstore_memory_allocator_generic.h"
#include "errorcode/dstore_common_error_code.h"

namespace DSTORE {

class StackAllocSetContext : public DstoreAllocSetContext {
public:
    StackAllocSetContext() = delete; /* no default ctor */
    ~StackAllocSetContext() = default;
    DISALLOW_COPY_AND_MOVE(StackAllocSetContext);

    static DstoreMemoryContext Create(DstoreMemoryContext parent, const char *name, Size minContextSize,
        Size initBlockSize, Size maxBlockSize, MemoryContextType contextType, Size maxSize);

    void *allocate(Size align, Size size, const char *file, int line) final;
    void *allocate0(Size align, Size size, const char *file, int line) final;
    void free_pointer(void *pointer) final;
    void *reallocate(void *pointer, Size align, Size size, const char *file, int line) final;
    bool init(Size allocSize) final;
    void reset() final;
    void clear() final;
    void p_init_chunk(GenericChunk chunk, AllocSet alloc_set, Size requested_size, Size chunk_size,
        DstoreMemoryAllocDebugInfo *info);
    Size get_chunk_space(void *pointer) final;
#ifdef MEMORY_CONTEXT_CHECKING
    void check() final;
    void check_data_pointer(void *pointer) final;
#endif
    void check_memory_valid(AllocBlock block, AllocChunk chunk) final;

    StackAllocSetContext(DstoreMemoryContext parent, const char *name, Size minContextSize, Size initBlockSize,
                         Size maxBlockSize, MemoryContextType type, Size maxSize, Size setSize);

    void *Push(uint32 size);

    uint32 GetStackTop();

    void Restore(uint32 oldTop);

private:
    uint8 *m_buf;
    uint32 m_size;
    uint32 m_top;
};

typedef StackAllocSetContext *StackAllocSet;

}
#endif /* STORAGE_MEMORY_ALLOCATOR_GENERIC_H */
