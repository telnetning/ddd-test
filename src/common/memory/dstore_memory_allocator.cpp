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
 * dstore_memory_allocator.cpp
 *
 *
 * IDENTIFICATION
 *        src/common/memory/dstore_memory_allocator.cpp
 *
 * ---------------------------------------------------------------------------------------
 */
#include "common/memory/dstore_memory_allocator.h"
#include <cstdlib>
#include "framework/dstore_instance.h"
#include "errorcode/dstore_common_error_code.h"

namespace DSTORE {

/* TODO: mmap_threshold is a environment variable. get the value from system during initialization */
Size mmap_threshold = (Size)0x40000000;
thread_local bool USE_HUGE_PAGE = true;

/*
 * Mostly context name will be constant string "XXXMemoryContext" which is process lifecycle and is safe
 * or should set a name same lifecycle as memory context.
 * */
DstoreAllocSetContext::DstoreAllocSetContext(DstoreMemoryContext parent, const char *name, Size initBlockSize,
    Size maxBlockSize, MemoryContextType type, Size maxSize, Size setSize)
{
    void *selfPtr = static_cast<void *>(this);
    errno_t rc = memset_s(selfPtr, setSize, 0, setSize);
    storage_securec_check(rc, "\0", "\0");
    m_memory_context.type = type;
    m_memory_context.allocSet = this;
    m_memory_context.parent = parent;
    m_memory_context.firstChild = nullptr;
    m_memory_context.prevChild = nullptr;
    m_memory_context.nextChild = nullptr;
    size_t copy_size =
        (strlen(name) < MaxContextNameLen ? strlen(name) : MaxContextNameLen - 1);
    rc = strncpy_s(m_memory_context.name, MaxContextNameLen, name, copy_size);
    storage_securec_check(rc, "\0", "\0");
    m_memory_context.name[copy_size] = '\0';
    m_memory_context.is_reset = true;
    m_memory_context.thread_id = pthread_self();
    m_root_context = false;
    if (parent == nullptr) {
        m_memory_context.topnode = &m_memory_context;
    } else {
        m_memory_context.topnode = parent->topnode;
    }
    /* todo: get thread_id, thrd->GetCore()->pid */
    if (parent != nullptr) {
        m_memory_context.session_id = parent->session_id;
    } else {
        m_memory_context.session_id = 0;
    }

    rc = memset_s(m_freelist, sizeof(m_freelist), 0, sizeof(m_freelist));
    storage_securec_check(rc, "\0", "\0");
    const Size minInitBlockSize = 1024;
    m_init_block_size = DstoreMax(MAXALIGN(initBlockSize), minInitBlockSize);
    m_max_block_size = DstoreMax(MAXALIGN(maxBlockSize), m_init_block_size);
    m_next_block_size = m_init_block_size;
    m_total_space = 0;
    m_free_space = 0;
    m_max_space_size = maxSize + SELF_GENRIC_MEMCTX_LIMITATION;

    if (type == MemoryContextType::SHARED_CONTEXT || type == MemoryContextType::MEMALIGN_SHRCTX) {
        if (maxSize == DEFAULT_MEMORY_CONTEXT_MAX_SIZE) {
            /* default maxSize of shared memory context. */
            m_max_space_size = SHARED_MEMORY_CONTEXT_MAX_SIZE;
        }
        (void)pthread_rwlock_init(&(m_memory_context.lock), nullptr);
        (void)pthread_rwlock_init(&(m_memory_context.tree_lock), nullptr);
    }

    if (parent != nullptr) {
        /* Lock the parent context if shared and must be made thread-safe */
        LockMemoryContextIfShared(parent);

        /* link m_memory_context to parent */
        m_memory_context.parent = parent;
        m_memory_context.nextChild = parent->firstChild;
        if (parent->firstChild != nullptr) {
            parent->firstChild->prevChild = &m_memory_context;
        }
        parent->firstChild = &m_memory_context;

        m_memory_context.level = parent->level + 1;

        /* Unlock the parent context if shared and must be made thread-safe */
        UnlockMemoryContextIfShared(parent);
    }
}


void DstoreAllocSetContext::LockMemoryContextIfShared(DstoreMemoryContext context)
{
    if (context->type == MemoryContextType::SHARED_CONTEXT) {
        int err = pthread_rwlock_wrlock(&context->lock);
        if (err != 0) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("system call failed when lock, errno:%d.", err));
            StorageAssert(0);
        }
    }
}

void DstoreAllocSetContext::UnlockMemoryContextIfShared(DstoreMemoryContext context)
{
    if (context->type == MemoryContextType::SHARED_CONTEXT) {
        int unlockErr = pthread_rwlock_unlock(&context->lock);
        if (unlockErr != 0) {
            ErrLog(DSTORE_ERROR, MODULE_FRAMEWORK, ErrMsg("system call failed when lock, errno:%d.", unlockErr));
            StorageAssert(0);
        }
    }
}


DstoreMemoryContext DstoreAllocSetContext::ChooseRootContext(MemoryContextType type, DstoreMemoryContext parent)
{
    DstoreMemoryContext root = nullptr;
    if (type == MemoryContextType::SESSION_CONTEXT) {
        if (parent != nullptr) {
            StorageAssert(parent->type == MemoryContextType::SESSION_CONTEXT);
            if (parent->parent == nullptr) {
                /* during session initialization */
                root = parent;
            } else {
                /* during session running */
                StorageAssert(thrd->GetSession()->topMemCtx != nullptr);
                root = thrd->GetSession()->topMemCtx;
            }
        }
    } else if (type == MemoryContextType::SHARED_CONTEXT) {
        /* for shared type memory context, we always set it under instance level shared memory context. */
        if (g_storageInstance) {
            if (g_storageInstance->GetMemoryMgr()) {
                root = g_storageInstance->GetMemoryMgr()->GetRoot();
            } else if (parent) {
                /* the parent is the root of the memory mgr,
                 * but not set the value yet, this case only in the memory
                 * mgr init phase */
                StorageAssert(parent->type == MemoryContextType::SHARED_CONTEXT);
                StorageAssert(parent->parent == nullptr); // parent self is the instance top memory context;
                root = parent;
            }
        }
    } else {
        /* for thread and stack type memory context, we use thread top memory context. */
        if (thrd && thrd->m_memoryMgr) {
            root = thrd->m_memoryMgr->GetRoot();
        } else if (parent && parent->parent == nullptr) {
            /* this case only in the thread memory mgr init phase */
            root = parent;
        }
    }
    return root;
}

MemContextCallBack::MemContextCallBack() : reserve_mem_callback_func(nullptr), release_mem_callback_func(nullptr)
{}

void MemContextCallBack::InitMemCallBack(void *reserve, void *release)
{
    reserve_mem_callback_func = reinterpret_cast<ReserveMemCallBack>(reserve);
    release_mem_callback_func = reinterpret_cast<ReleaseMemCallBack>(release);
}

} /* DSTORE */

