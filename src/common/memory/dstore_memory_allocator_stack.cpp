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
 * dstore_memory_allocator_stack.cpp
 *
 *
 * IDENTIFICATION
 *        src/common/memory/dstore_memory_allocator_stack.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "common/error/dstore_error.h"
#include "common/log/dstore_log.h"
#include "common/memory/dstore_memory_allocator_generic.h"
#include "common/memory/dstore_mctx.h"
#include "common/dstore_datatype.h"
#include "errorcode/dstore_common_error_code.h"
#include "common/memory/dstore_memory_allocator_stack.h"

namespace DSTORE {

/* public interfaces */
StackAllocSetContext::StackAllocSetContext(DstoreMemoryContext parent, const char *name,
    UNUSE_PARAM Size minContextSize, Size initBlockSize, Size maxBlockSize, MemoryContextType type, Size maxSize,
    Size setSize) :
    DstoreAllocSetContext::DstoreAllocSetContext(parent, name, initBlockSize, maxBlockSize,
                                                 type, maxSize, setSize), m_top(0)
{
    StorageAssert(type == MemoryContextType::STACK_CONTEXT);
}

DstoreMemoryContext StackAllocSetContext::Create(UNUSE_PARAM DstoreMemoryContext parent, UNUSE_PARAM const char *name,
    UNUSE_PARAM Size minContextSize, UNUSE_PARAM Size initBlockSize, UNUSE_PARAM Size maxBlockSize,
    MemoryContextType UNUSE_PARAM contextType, UNUSE_PARAM Size maxSize)
{
    StorageAssert(contextType == MemoryContextType::STACK_CONTEXT);

    StackAllocSet allocSetContext;
    DstoreMemoryContext root = ChooseRootContext(contextType, parent);

    Size setSize = sizeof(StackAllocSetContext);
    if (!AllocSizeIsValid(setSize)) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("invalid memory allocate request size %lu",
            static_cast<unsigned long>(setSize)));
        return nullptr;
    }

    StorageAssert(root != nullptr);
    allocSetContext = DstoreNew(root) StackAllocSetContext(parent, name, minContextSize, initBlockSize,
                                                           maxBlockSize, contextType, maxSize, setSize);
    if (allocSetContext == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("Failed on request of size %lu bytes in %s:%d.",
            static_cast<unsigned long>(setSize), __FILE__, __LINE__));
        return nullptr;
    }

    if (!allocSetContext->init(SizeM(1))) {
        /* If context alloc init failed, we just keep context alive to use, do not free and return nullptr. */
        ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK,
               ErrMsg("Failed on request of size %lu bytes in %s:%d.", static_cast<unsigned long>(SizeM(1)), __FILE__,
                      __LINE__));
    }

    return &(allocSetContext->m_memory_context);
}

void StackAllocSetContext::p_init_chunk(GenericChunk chunk, AllocSet alloc_set, Size requested_size, Size chunk_size,
    DstoreMemoryAllocDebugInfo *info)
{
    StorageAssert(DstorePointerIsValid(chunk));
    UNUSED_VARIABLE(requested_size);
    chunk->alloc_set = (AllocSet)alloc_set;
    chunk->size = chunk_size;
    
    chunk->file = info->file;
    chunk->line = info->line;
    chunk->magicNum = info->magicNum;
}

void *StackAllocSetContext::allocate(UNUSE_PARAM Size align, Size size, const char *file, int line)
{
    StorageAssert(align == 0);
    if (unlikely(m_buf == nullptr)) {
        return nullptr;
    }
    GenericChunk chunk = (GenericChunk)Push(size + ALLOC_CHUNKHDRSZ);
    if (unlikely(chunk == nullptr)) {
        return nullptr;
    }
    DstoreMemoryAllocDebugInfo info = {
        .file = file,
        .line = line,
        .magicNum = PRE_MAGIC_NUM
    };
    p_init_chunk(chunk, (AllocSet)this, 0, 0, &info);
    return ChunkGetAllocPtr(chunk);
}

void *StackAllocSetContext::allocate0(UNUSE_PARAM Size align, Size size, const char *file, int line)
{
    if (unlikely(m_buf == nullptr)) {
        return nullptr;
    }
    void *data = allocate(0, size, file, line);
    if (unlikely(data == nullptr)) {
        return nullptr;
    }

    errno_t rc = memset_s(data, size, 0, size);
    storage_securec_check(rc, "\0", "\0");

    return data;
}

void StackAllocSetContext::free_pointer(UNUSE_PARAM void *pointer)
{
    /* unsupport to free memory under stack memory allocator */
}

void *StackAllocSetContext::reallocate(UNUSE_PARAM void *pointer, UNUSE_PARAM Size align, UNUSE_PARAM Size size,
    UNUSE_PARAM const char *file, UNUSE_PARAM int line)
{
    /* unsupport to reallocate memory under stack memory allocator */
    return nullptr;
}

bool StackAllocSetContext::init(Size allocSize)
{
    uint32 stackSize = (uint32)allocSize;
    m_buf = static_cast<uint8 *>(DstorePalloc(stackSize * sizeof(uint8)));
    if (m_buf == nullptr) {
        return false;
    }
    m_size = stackSize * sizeof(uint8);
    return true;
}

void StackAllocSetContext::reset()
{}

void StackAllocSetContext::clear()
{}

Size StackAllocSetContext::get_chunk_space(UNUSE_PARAM void *pointer)
{
    return 0;
}

#ifdef MEMORY_CONTEXT_CHECKING
void StackAllocSetContext::check()
{}
#endif

void StackAllocSetContext::check_memory_valid(UNUSE_PARAM AllocBlock block, UNUSE_PARAM AllocChunk chunk)
{}

#ifdef MEMORY_CONTEXT_CHECKING
void StackAllocSetContext::check_data_pointer(UNUSE_PARAM void *pointer)
{}
#endif

void *StackAllocSetContext::Push(uint32 size)
{
    StorageAssert(m_memory_context.type == MemoryContextType::STACK_CONTEXT);
    uint32 allocSize = MAXALIGN(size);
    if (m_top + allocSize > m_size) {
        return nullptr;
    }
    uint8 *ptr = m_buf + m_top;
    m_top += allocSize;
    return ptr;
}

void StackAllocSetContext::Restore(uint32 oldTop)
{
    StorageAssert(m_memory_context.type == MemoryContextType::STACK_CONTEXT);
    m_top = oldTop;
}

uint32 StackAllocSetContext::GetStackTop()
{
    return m_top;
}

} /* DSTORE */
