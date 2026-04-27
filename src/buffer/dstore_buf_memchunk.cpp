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
 * dstore_buf_memchunk.cpp
 *      This file implements the classes BufferMemChunk and BufferMemChunkList.
 *
 * IDENTIFICATION
 *      src/buffer/dstore_buf_memchunk.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "common/log/dstore_log.h"
#include "buffer/dstore_buf_memchunk.h"

namespace DSTORE {

/***
 *** Implementation of BufferMemChunk
 ***/

BufferMemChunk::BufferMemChunk(uint64 memChunkId, Size memChunkSize)
    : m_numOfBuf{memChunkSize}, m_memChunkId{memChunkId}, m_rawBufferDesc{nullptr}, mBufferDesc{nullptr},
      m_rawBufferBlock{nullptr}, m_bufferBlock{nullptr}, m_controllers{nullptr}, m_data{nullptr}
{
    LWLockInitialize(&(this->m_memChunkLock), LWLOCK_GROUP_BUF_MEMCHUNK);
    /* The constructors for m_state and m_statistics will be automatically invoked. */
}

BufferMemChunk::~BufferMemChunk()
{
    this->m_controllers = nullptr;
    this->m_bufferBlock = nullptr;
    this->m_rawBufferBlock = nullptr;
    this->mBufferDesc = nullptr;
    this->m_rawBufferDesc = nullptr;
    this->m_data = nullptr;
}

/*
 * Initialize all components (header, buffer descriptors, blocks, controllers) of a memchunk.
 * The memory is allocated as one chunk and assigned to each component.
 */
RetStatus BufferMemChunk::InitBufferMemChunk(Size memAllocSize)
{
    this->m_data = static_cast<char *>(DstoreMemoryContextAllocHugeSize(g_dstoreCurrentMemoryContext, memAllocSize));
    if (this->m_data == nullptr) {
        return DSTORE_FAIL;
    }

    /* Assign memory to buffer descriptors. */
    this->m_rawBufferDesc = static_cast<char *>(this->m_data);
    this->mBufferDesc = reinterpret_cast<BufferDesc *>(TYPEALIGN(DSTORE_CACHELINE_SIZE, this->m_rawBufferDesc));

    /* Assign memory to buffer blocks. We clean the buffer block memory the first time when we use it. */
    this->m_rawBufferBlock = static_cast<char *>(this->m_data +
        GetBufferDescSize());
    this->m_bufferBlock = reinterpret_cast<char *>(TYPEALIGN(ALIGNOF_BUFFER, this->m_rawBufferBlock));

    /* Assign memory to buffer controllers. */
    this->m_controllers = static_cast<BufferDescController *>(static_cast<void *>(this->m_data +
        GetBufferDescSize() + GetBufferBlockSize()));

    return DSTORE_SUCC;
}

RetStatus BufferMemChunk::InitBufferDescriptors()
{
    /* Initialize each buffer descriptor and assign a controller to it. */
    LockMemChunk(LW_EXCLUSIVE);
    for (Size i = 0; i < GetSize(); i++) {
        BufferDesc *bufferDesc = GetBufferDesc(i);
        ASSERT_POINTER_ALIGNMENT(bufferDesc, DSTORE_CACHELINE_SIZE);
        BufferDescController *controller = GetBufferControllerTemplate<BufferDescController>(i);
        controller->InitController();
        bufferDesc->InitBufferDesc(GetBufferBlock(i), controller);
        ASSERT_POINTER_ALIGNMENT(GetBufferBlock(i), DSTORE_CACHELINE_SIZE);
    }
    UnlockMemChunk();

    return DSTORE_SUCC;
}

/*
 * Destroy all components of a memchunk and recursively call destructors if needed.
 * The memory will be deallocated as one chunk by the caller.
 */
void BufferMemChunk::DestroyBufferMemChunk()
{
    /*
     * If there is any memory allocated for each buffer descriptor, we will need to
     * loop through the buffer descriptors to call the destructor as well.
     */
    DstorePfreeExt(this->m_data);
    this->m_data = nullptr;
}

/* Static methods: Get sizes for different regions of memchunk. */
Size BufferMemChunk::GetBufferDescSize() const
{
    return (m_numOfBuf) * sizeof(BufferDesc) + DSTORE_CACHELINE_SIZE;
}

Size BufferMemChunk::GetBufferBlockSize() const
{
    return (m_numOfBuf) * BLCKSZ + ALIGNOF_BUFFER;
}

Size BufferMemChunk::GetBufferControllerSize()
{
    return (m_numOfBuf) * sizeof(BufferDescController);
}

Size BufferMemChunk::GetTotalSize()
{
    return sizeof(BufferMemChunk) + GetBufferDescSize() + GetBufferBlockSize() + GetBufferControllerSize();
}

/* Getters. */
Size BufferMemChunk::GetSize() const
{
    return this->m_numOfBuf;
}

uint64 BufferMemChunk::GetMemChunkId() const
{
    return this->m_memChunkId;
}

bool BufferMemChunk::IsBelongTo(BufferDesc *desc) const
{
    PointerToAddress buffer;
    buffer.pointer = &this->mBufferDesc[0];
    uint64 lowAddr = buffer.address;
    buffer.pointer = &this->mBufferDesc[m_numOfBuf];
    uint64 highAddr = buffer.address;

    buffer.pointer = desc;
    if ((buffer.address >= lowAddr) &&
        (buffer.address < highAddr)) {
        return true;
    } else {
        return false;
    }
}

/* Concurrency support. */
void BufferMemChunk::LockMemChunk(LWLockMode mode)
{
    DstoreLWLockAcquireByMode(&(this->m_memChunkLock), mode)
}

void BufferMemChunk::UnlockMemChunk()
{
    LWLockRelease(&(this->m_memChunkLock));
}

/* Access the X-th element. */
BufferDesc *BufferMemChunk::GetBufferDesc(Size index)
{
    return &(this->mBufferDesc[index]);
}

BufBlock BufferMemChunk::GetBufferBlock(Size index)
{
    return &(this->m_bufferBlock[index * BLCKSZ]);
}


/*
 * Count the number of hot pages owned by this memchunk.
 * This statistics will be used as heuristics to evict memchunks during bufferpool shrink.
 */
void BufferMemChunk::UpdateStatistics()
{
    /* Only one thread can collect the statistics at one time. */
    std::lock_guard<std::mutex> lock{m_statistics.lock};
    /*
     * Lock the memchunk here with shared mode so that it can't be evicted while we're collecting the statistics.
     * To avoid affecting the performance, the buffers can still be accessed during this time,
     * which means the buffer usages of this memchunk can change while we're counting it,
     * so this statistics is only an estimate.
     */
    LockMemChunk(LW_SHARED);
    m_statistics.Reset();
    for (Size i = 0; i < GetSize(); i++) {
        BufferDesc *bufferDesc = GetBufferDesc(i);
        /*
         * We don't lock the bufferDesc header here, because again we don't want to affect the performance
         * for collecting statistics.
         * Instead, we will only pin the buffer to make sure it's not getting evicted during this time.
         */
        bufferDesc->Pin();
        if (bufferDesc->lruNode.IsInHotList()) {
            m_statistics.hotPageCount++;
        }
        if (bufferDesc->GetRefcount() == 1) {
            m_statistics.idlePageCount++;
        }
        if (bufferDesc->IsPageDirty()) {
            m_statistics.dirtyPageCount++;
        }
        if (bufferDesc->IsCrPage()) {
            m_statistics.crPageCount++;
        }
        bufferDesc->Unpin();
    }
    ErrLog(DSTORE_DEBUG1, MODULE_BUFMGR,
        ErrMsg("Statistics for memchunk #%lu: hotPageCount = %u", GetMemChunkId(), m_statistics.hotPageCount));
    UnlockMemChunk();
}

uint32 BufferMemChunk::GetHotPageCount() const
{
    return m_statistics.hotPageCount;
}

uint32 BufferMemChunk::GetIdlePageCount() const
{
    return m_statistics.idlePageCount;
}

uint32 BufferMemChunk::GetDirtyPageCount() const
{
    return m_statistics.dirtyPageCount;
}
uint32 BufferMemChunk::GetCrPageCount() const
{
    return m_statistics.crPageCount;
}

/***
 *** Implementation of BufferMemChunkList
 ***/

BufferMemChunkList::BufferMemChunkList(Size bufferPoolSize, Size numOfBufInMemChunk)
    : m_head{nullptr, nullptr},
      m_numOfMemChunk{0},
      m_numOfBufInMemChunk{numOfBufInMemChunk},
      m_memChunkListLock{},
      m_shrinkSeq{0},
      m_removingMemChunkId{0}
{
    StorageAssert(numOfBufInMemChunk > 0);
    StorageAssert(numOfBufInMemChunk <= BUF_MEMCHUNK_MAX_NBLOCKS);
    StorageAssert(bufferPoolSize % numOfBufInMemChunk == 0);
    UNUSED_VARIABLE(bufferPoolSize);

    DListInit(&m_head);
    LWLockInitialize(&m_memChunkListLock, LWLOCK_GROUP_BUF_MEMCHUNK_LIST);
    GsAtomicWriteU64(&m_shrinkSeq, BUF_MEMCHUNK_FIRST_SEQ);
    GsAtomicWriteU64(&m_removingMemChunkId, INVALID_MEMCHUNK_ID);
}

BufferMemChunkList::~BufferMemChunkList()
{
    /* The linked list is emptied and deallocated by DestroyBufferMemChunkList(). */
}

/* Getters. */
Size BufferMemChunkList::GetSize() const
{
    return this->m_numOfMemChunk;
}

Size BufferMemChunkList::GetNumOfBufInMemChunk() const
{
    return this->m_numOfBufInMemChunk;
}

/*
 * This is the minimal amount of buffers a bufferpool should have.
 * It is calculated from minimal number of evictable buffers
 *
 * Please update this to actual fixed bufferpool size going forward.
 */
Size BufferMemChunkList::GetMinBufferPoolSize() const
{
    return GetNumOfBufInMemChunk() * (GetMinEvictableSize());
}

Size BufferMemChunkList::GetMinEvictableSize() const
{
    return BUF_MEMCHUNK_MIN_EVICTABLE_NCHUNKS;
}

/* Concurrency support. */
void BufferMemChunkList::LockBufferMemChunkList(LWLockMode mode)
{
    /*
     * We only acquire the lock here if the thread local variable "thrd" is not yet deallocated,
     * as LWLockAcquire() checks the waiters of this lock through thrd before acquiring it.
     * During a thread cleanup, such scenario may happen that thrd is deallocated before
     * DestroyBufferMemChunkList() is called.
     */
    if (thrd != nullptr && thrd->lwlockContext.held_lwlocks != nullptr) {
        if (unlikely(mode == LW_EXCLUSIVE)) {
            DstoreLWLockAcquire(&(this->m_memChunkListLock), LW_EXCLUSIVE);
        } else {
            DstoreLWLockAcquire(&(this->m_memChunkListLock), LW_SHARED);
        }
    }
}

void BufferMemChunkList::UnlockBufferMemChunkList()
{
    if (thrd != nullptr && thrd->lwlockContext.held_lwlocks != nullptr) {
        LWLockRelease(&(this->m_memChunkListLock));
    }
}

void BufferMemChunkList::IncreaseSizeChangeSequence()
{
    static_cast<void>(GsAtomicFetchAddU64(&m_shrinkSeq, 1UL));
}

uint64 BufferMemChunkList::GetSizeChangeSequence()
{
    return GsAtomicReadU64(&m_shrinkSeq);
}

uint64 BufferMemChunkList::GetMemChunkId(UNUSE_PARAM BufferDesc *bufferDesc)
{
    uint64 memChunkId = INVALID_MEMCHUNK_ID;
    LockBufferMemChunkList(LW_SHARED);
    BufferMemChunkWrapper<BufferMemChunk> *memChunkWrapper = GetMemChunkIterator<BufferMemChunk>();
    while (memChunkWrapper != nullptr) {
        if (memChunkWrapper->memChunk->IsBelongTo(bufferDesc)) {
            memChunkId = memChunkWrapper->memChunk->GetMemChunkId();
            break;
        }
        memChunkWrapper = memChunkWrapper->GetNext();
    }
    UnlockBufferMemChunkList();
    return memChunkId;
}

BufferMemChunk *BufferMemChunkList::GetMemChunk(uint64 memChunkId)
{
    BufferMemChunk *memChunkRet = nullptr;
    LockBufferMemChunkList(LW_SHARED);
    BufferMemChunkWrapper<BufferMemChunk> *memChunkWrapper = GetMemChunkIterator<BufferMemChunk>();
    while (memChunkWrapper != nullptr) {
        if (memChunkWrapper->memChunk->GetMemChunkId() == memChunkId) {
            memChunkRet = memChunkWrapper->memChunk;
            break;
        }
        memChunkWrapper = memChunkWrapper->GetNext();
    }
    UnlockBufferMemChunkList();
    return memChunkRet;
}

uint64 BufferMemChunkList::GetRemovingMemChunkId()
{
    return GsAtomicReadU64(&m_removingMemChunkId);
}

void BufferMemChunkList::SetRemovingMemChunkId(uint64 memChunkId)
{
    GsAtomicWriteU64(&m_removingMemChunkId, memChunkId);
}

} /* namespace DSTORE */
