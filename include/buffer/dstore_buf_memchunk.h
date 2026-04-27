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
 * dstore_buf_memchunk.h
 *      This file declares the class BufferMemChunk and its container BufferMemChunkList.
 *
 * IDENTIFICATION
 *      include/buffer/dstore_buf_memchunk.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_BUF_MEMCHUNK_H
#define DSTORE_BUF_MEMCHUNK_H

#include <mutex>
#include <algorithm>
#include "common/algorithm/dstore_ilist.h"
#include "buffer/dstore_buf.h"

namespace DSTORE {

/*
 * This is the maximum and default number of buffer blocks in each BufferMemChunk.
 * page size        = 8 * 1024                  (8192 bytes)
 * memchunk size    = 10 * 1024 * 1024 * 1024   (10GB)
 * So, each memchunk can store (memchunk size / page size) blocks.
 */
#ifdef UT
constexpr Size BUF_MEMCHUNK_MAX_NBLOCKS = 10;
#else
constexpr Size BUF_MEMCHUNK_MAX_NBLOCKS = 1310720;
#endif

/*
 * This is the minimum number of memchunks a bufferpool should have during startup.
 * Also, the bufferpool size must be divisible by the memchunk size.
 */
#ifdef UT
constexpr Size BUF_MEMCHUNK_MIN_NCHUNKS = 1;
#else
constexpr Size BUF_MEMCHUNK_MIN_NCHUNKS = 8;
#endif

/*
 * These are the minimal number of evictable memchunks when initializing the bufferpool.
 */
constexpr Size BUF_MEMCHUNK_MIN_EVICTABLE_NCHUNKS = 1;

/*
 * ALIGNOF_BUFFER is used to pad in front of the memory allocated for buffer blocks inside each memchunk.
 * It is also defined in src/include/pg_config_manual.h.
 * To avoid compiler errors, we must also define it with #define.
 */
constexpr uint32 ALIGNOF_BUFFER = 4096;

struct BufferMemChunkState {
    uint32 toBeEvicted : 1;
    uint32 unused : 30;

    /* Constructor. */
    BufferMemChunkState() : toBeEvicted{0}, unused{0} {}
};
static_assert(sizeof(BufferMemChunkState) == sizeof(uint32),
    "BufferMemChunkState size must be 4 bytes.");

struct BufferMemChunkStatistics {
    std::mutex lock;
    uint32 hotPageCount;
    uint32 idlePageCount;
    uint32 dirtyPageCount;
    uint32 crPageCount;

    /* Constructor. */
    BufferMemChunkStatistics()
    {
        Reset();
    }

    void Reset()
    {
        hotPageCount = 0;
        idlePageCount = 0;
        dirtyPageCount = 0;
        crPageCount = 0;
    }
};

constexpr uint64 INVALID_MEMCHUNK_ID            = static_cast<uint64>(-1L);
constexpr uint64 BUF_MEMCHUNK_INVALID_SEQ       = 0UL;
constexpr uint64 BUF_MEMCHUNK_FIRST_SEQ         = 1UL;

/*
 * BufferMemChunk is a container for buffers inside the buffer pool.
 * While the BufferMemChunk contains an array of buffers using continuous memory, the buffer pool contains
 * a linked-list of BufferMemChunk's which allows the online expansion and contraction of the buffer pool size.
 * Each BufferMemChunk has 4 components (header, buffer descriptors, blocks, controllers) and is allocated
 * and deallocated as one chunk.
 *
 * It also inherits BaseObject in order to use DstoreNew() allocator.
 */
class BufferMemChunk : public BaseObject {
public:
    /* Constructor and destructor. */
    BufferMemChunk(uint64 memChunkId, Size memChunkSize);
    virtual ~BufferMemChunk();

    virtual RetStatus InitBufferMemChunk(Size memAllocSize);
    virtual RetStatus InitBufferDescriptors();
    void DestroyBufferMemChunk();

    template <class BufferDescController_T>
    RetStatus InitBufferDescriptorsTemplate()
    {
        /* Initialize each buffer descriptor and assign a controller to it. */
        LockMemChunk(LW_EXCLUSIVE);
        for (Size i = 0; i < GetSize(); i++) {
            BufferDesc *bufferDesc = GetBufferDesc(i);
            BufferDescController_T *controller = GetBufferControllerTemplate<BufferDescController_T>(i);
            controller->InitController();
            bufferDesc->InitBufferDesc(GetBufferBlock(i), controller);
        }
        UnlockMemChunk();

        return DSTORE_SUCC;
    }

    char* GetRawBufferBlock()
    {
        return m_rawBufferBlock;
    }

    /* Static methods: Get sizes for different regions of memchunk. */
    Size GetBufferDescSize() const;
    Size GetBufferBlockSize() const;
    virtual Size GetBufferControllerSize();
    virtual Size GetTotalSize();

    /* The following methods are in the order of member variables being declared. */

    /* Getters. */
    Size GetSize() const;
    uint64 GetMemChunkId() const;
    bool IsBelongTo(BufferDesc *desc) const;

    /* Concurrency support. */
    void LockMemChunk(LWLockMode mode);
    void UnlockMemChunk();

    /* Access the X-th element. */
    BufferDesc *GetBufferDesc(Size index);
    BufBlock GetBufferBlock(Size index);

    template <class BufferDescController_T>
    BufferDescController_T *GetBufferController(Size index)
    {
        return GetBufferControllerTemplate<BufferDescController_T>(index);
    }

    void UpdateStatistics();
    uint32 GetHotPageCount() const;
    uint32 GetIdlePageCount() const;
    uint32 GetDirtyPageCount() const;
    uint32 GetCrPageCount() const;

protected:
    /* The number of blocks per memchunk. It supports up to 2^64 blocks per memchunk. */
    Size m_numOfBuf;

    /* The memchunk ID is unique per bufferpool. It supports up to 2^64 memchunks in the bufferpool. */
    uint64 m_memChunkId;

    LWLock m_memChunkLock;
    BufferMemChunkState m_state;
    BufferMemChunkStatistics m_statistics;

    /* Buffer pool descriptor array (memory start address aligned to cache line). */
    char *m_rawBufferDesc;
    BufferDesc *mBufferDesc;

    /* Buffer pool block pointer array (memory start address aligned to cache line). */
    char *m_rawBufferBlock;
    BufBlock m_bufferBlock;

    /* Buffer pool descriptor controller array. */
    BufferDescController *m_controllers;

    /* This is the data buffer allocated for m_rawBufferDesc, m_rawBufferBlock and m_controllers. */
    char *m_data;

    template <class BufferDescController_T>
    BufferDescController_T *GetBufferControllerTemplate(Size index)
    {
        return (static_cast<BufferDescController_T *>(m_controllers)) + index;
    }
};

/*
 * BufferMemChunkWrapper is used as linked-list node to link all the mem chunks together.
 */
template<class BufferMemChunk_T>
struct BufferMemChunkWrapper : public BaseObject {
    dlist_node node;
    dlist_head *head;
    BufferMemChunk_T *memChunk;

    BufferMemChunkWrapper(dlist_head *listHead, BufferMemChunk_T *chunk)
        : node{nullptr, nullptr}, head{listHead}, memChunk{chunk}
    {}

    BufferMemChunkWrapper<BufferMemChunk_T> *GetNext()
    {
        if (!DListHasNext(head, &node)) {
            return nullptr;
        } else {
            dlist_node *next = DListNextNode(head, &node);
            return dlist_container(BufferMemChunkWrapper<BufferMemChunk_T>, node, next);
        }
    }

    void RemoveFromList()
    {
        DListDelete(&node);
    }

    void DestroyBufferMemChunk()
    {
        memChunk->DestroyBufferMemChunk();
        delete memChunk;
        memChunk = nullptr;
    }

    RetStatus InitBufferMemChunk()
    {
        RetStatus rc = memChunk->BufferMemChunk_T::InitBufferMemChunk(memChunk->GetTotalSize());
        if (STORAGE_FUNC_FAIL(rc)) {
            return DSTORE_FAIL;
        }

        rc = memChunk->BufferMemChunk_T::InitBufferDescriptors();
        if (STORAGE_FUNC_FAIL(rc)) {
            return DSTORE_FAIL;
        }

        return DSTORE_SUCC;
    }
};

/*
 * BufferMemChunkList is a container for buffer memchunks.
 * It allocates and initializes each memchunk, and appends it to the linked-list.
 * It also encapsulates the functionalities of elastic bufferpool. During the bufferpool expansion, new memchunks
 * will be allocated and appended to the list. During the bufferpool contraction, existing memchunks
 * will be evaluated and ranked by the usage of buffers. The memchunk with the least usage will be chosen
 * to be evicted.
 *
 * The BufferMemChunkList IS A ringed-doubly-linked list.
 * It also inherits BaseObject in order to use DstoreNew() allocator.
 */
class BufferMemChunkList : public BaseObject {
public:
    /* Constructor and destructor. */
    BufferMemChunkList(Size bufferPoolSize, Size numOfBufInMemChunk);
    ~BufferMemChunkList();

    /* Allocate memchunk linked-list. */
    template<class BufferMemChunk_T>
    RetStatus AppendBufferMemChunkList(Size numMemChunksToAdd,
        BufferMemChunkWrapper<BufferMemChunk_T> **firstMemChunkToAdd)
    {
        StorageAssert(numMemChunksToAdd > 0);
        RetStatus rc = DSTORE_SUCC;
        dlist_head newList;
        DListInit(&newList);

        /*
         * We use separate loops to allocate memory and append to linked list,
         * because memory allocation is a slow operation, for each call, we're potentially allocating 10GB of memory;
         * whereas appending to linked list is a fast operation, however, it requires exclusive lock protection
         * over the list. We should minimize the time that the exclusive lock is held.
         */
        for (Size i = 0; i < numMemChunksToAdd; i++) {
            BufferMemChunk_T *memChunk = static_cast<BufferMemChunk_T *>(DstoreNew(g_dstoreCurrentMemoryContext)
                BufferMemChunk_T(i, m_numOfBufInMemChunk));
            if (memChunk == nullptr) {
                rc = DSTORE_FAIL;
                goto EXIT;
            }
            BufferMemChunkWrapper<BufferMemChunk_T> *chunkWrapper =
                static_cast<BufferMemChunkWrapper<BufferMemChunk_T> *>(DstoreNew(g_dstoreCurrentMemoryContext)
                    BufferMemChunkWrapper<BufferMemChunk_T>(&m_head, memChunk));
            if (chunkWrapper == nullptr) {
                delete memChunk;
                rc = DSTORE_FAIL;
                goto EXIT;
            }
            DListPushTail(&newList, &chunkWrapper->node);

            rc = chunkWrapper->InitBufferMemChunk();
            if (STORAGE_FUNC_FAIL(rc)) {
                goto EXIT;
            }
        }

        /*
         * Link the old list's tail to the new list's head, and the new list's tail to the old list's head.
         * This is similar to DListPushTail(), but instead of adding one node, merging two lists into one.
         */
        LockBufferMemChunkList(LW_EXCLUSIVE);
        DListConcatenateTail(&m_head, &newList);
        m_numOfMemChunk += numMemChunksToAdd;
        UnlockBufferMemChunkList();

EXIT:
        if (STORAGE_FUNC_FAIL(rc)) {
            /*
             * Memory allocation has failed, we have to rollback what was done previously and return failure.
             * Free the memory that was previously allocated.
             */
            while (!DListIsEmpty(&newList)) {
                dlist_node *dnode = DListPopHeadNode(&newList);
                BufferMemChunkWrapper<BufferMemChunk_T> *memChunkWrapper =
                    dlist_container(BufferMemChunkWrapper<BufferMemChunk_T>, node, dnode);
                memChunkWrapper->DestroyBufferMemChunk();
                delete memChunkWrapper;
            }
            *firstMemChunkToAdd = nullptr;
        } else {
            dlist_node *firstNode = DListHeadNode(&newList);
            *firstMemChunkToAdd = dlist_container(BufferMemChunkWrapper<BufferMemChunk_T>, node, firstNode);
        }

        /*
         * newList is declared on stack which doesn't involve memory allocation, so we don't need to
         * deallocate memory here, we only need to detach the first and last elements from it.
         */
        newList.head.prev = nullptr;
        newList.head.next = nullptr;

        return rc;
    }

    /* Deallocate memchunk linked-list. */
    template <class BufferMemChunk_T>
    void DestroyBufferMemChunkList()
    {
        LockBufferMemChunkList(LW_EXCLUSIVE);
        while (!DListIsEmpty(&m_head)) {
            dlist_node *dnode = DListPopHeadNode(&m_head);
            BufferMemChunkWrapper<BufferMemChunk_T> *chunkWrapper =
                dlist_container(BufferMemChunkWrapper<BufferMemChunk_T>, node, dnode);
            chunkWrapper->DestroyBufferMemChunk();
            delete chunkWrapper;
            m_numOfMemChunk--;
        }
        UnlockBufferMemChunkList();
        StorageAssert(m_numOfMemChunk == 0);
    }

    template <class BufferMemChunk_T>
    void DeallocateOneMemChunk(BufferMemChunkWrapper<BufferMemChunk_T> *chunkWrapper)
    {
        if (STORAGE_VAR_NULL(chunkWrapper)) {
            ErrLog(DSTORE_PANIC, MODULE_BUFMGR, ErrMsg("ChunkWrapper is nullptr."));
        }
        StorageAssert(LWLockHeldByMe(&m_memChunkListLock));

        ErrLog(DSTORE_WARNING, MODULE_BUFMGR,
            ErrMsg("Deallocating memchunk #%lu.", chunkWrapper->memChunk->GetMemChunkId()));
        chunkWrapper->RemoveFromList();
        chunkWrapper->DestroyBufferMemChunk();
        DstorePfree(chunkWrapper);
        m_numOfMemChunk--;
    }

    /* Iterator methods. */
    template <class BufferMemChunk_T>
    BufferMemChunkWrapper<BufferMemChunk_T> *GetMemChunkIterator()
    {
        if (DListIsEmpty(&m_head)) {
            return nullptr;
        } else {
            /* Note that this->head is a dummy element, this->head.next is actually the first element. */
            dlist_node *nextNode = this->m_head.head.next;
            return dlist_container(BufferMemChunkWrapper<BufferMemChunk_T>, node, nextNode);
        }
    }

    /* Getters. */
    Size GetSize() const;
    Size GetNumOfBufInMemChunk() const;
    Size GetMinBufferPoolSize() const;
    Size GetMinEvictableSize() const;

    /* Concurrency support. */
    void LockBufferMemChunkList(LWLockMode mode);
    void UnlockBufferMemChunkList();

    /* Elastic bufferpool methods. */
    /*
     * Sort the array of memchunks pointers by their temperature in increasing order.
     * The result of this sorting is used for printing statistics as well as
     * for deciding the order of memchunk eviction during bufferpool shrink.
     */
    template <class BufferMemChunk_T>
    BufferMemChunkWrapper<BufferMemChunk_T> **SortByTemperature()
    {
        /*
        * memChunkArray is an array of memchunks pointers for sorting. It should be deallocated by the caller after use.
        * We use array of pointers (BufferMemChunk*) instead of array of memchunks (BufferMemChunk) to avoid
        * making a value copy of each memchunk which can be very large (10GB).
        */
        StorageAssert(LWLockHeldByMe(&m_memChunkListLock));
        BufferMemChunkWrapper<BufferMemChunk_T> **memChunkWrapperArray =
            static_cast<BufferMemChunkWrapper<BufferMemChunk_T> **>(
                DstorePalloc0(GetSize() * sizeof(BufferMemChunkWrapper<BufferMemChunk_T> *)));
        if (STORAGE_VAR_NULL(memChunkWrapperArray)) {
            ErrLog(DSTORE_ERROR, MODULE_BUFMGR, ErrMsg("MemChunkWrapperArray is nullptr %lu.", GetSize()));
            return nullptr;
        }

        Size index = 0;
        BufferMemChunkWrapper<BufferMemChunk_T> *memChunkWrapper = GetMemChunkIterator<BufferMemChunk_T>();
        while (memChunkWrapper != nullptr) {
            StorageAssert(index < GetSize());
            memChunkWrapper->memChunk->UpdateStatistics();
            memChunkWrapperArray[index++] = memChunkWrapper;
            memChunkWrapper = memChunkWrapper->GetNext();
        }

        std::sort(memChunkWrapperArray, memChunkWrapperArray + GetSize(), &IsLessHot<BufferMemChunk_T>);
        return memChunkWrapperArray;
    }

    /* Compare two memchunks by temperature. */
    template <class BufferMemChunk_T>
    static bool IsLessHot(BufferMemChunkWrapper<BufferMemChunk_T> *memChunk1,
                BufferMemChunkWrapper<BufferMemChunk_T> *memChunk2)
    {
        return (memChunk1->memChunk->GetHotPageCount() < memChunk2->memChunk->GetHotPageCount());
    }

    void IncreaseSizeChangeSequence();
    uint64 GetSizeChangeSequence();

    uint64 GetMemChunkId(BufferDesc *bufferDesc);
    BufferMemChunk *GetMemChunk(uint64 memChunkId);

    uint64 GetRemovingMemChunkId();
    void SetRemovingMemChunkId(uint64 memChunkId);

protected:
    dlist_head m_head;
    Size m_numOfMemChunk;
    Size m_numOfBufInMemChunk;
    LWLock m_memChunkListLock;

    /*
     * (1) Shrink sequence indicates how many times shrink happened.
     * This is used as a hint for re-read buffer
     * from buffer hash table when buffer pool shrinking.
     * (2) Removing memchunk id is a optimization to make the old buffer desc
     * still usable before it is actually invalidated.
     */
    gs_atomic_uint64 m_shrinkSeq;
    gs_atomic_uint64 m_removingMemChunkId;
};

} /* namespace DSTORE { */

#endif /* #define STORAGE_BUF_MEMCHUNK_H */
