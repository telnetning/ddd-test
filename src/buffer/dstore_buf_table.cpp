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
 */

#include "buffer/dstore_buf_table.h"
#include "framework/dstore_instance.h"
#include "common/dstore_datatype.h"

namespace DSTORE {

/* entry for buffer lookup hashtable */
struct BufferLookupEnt {
    BufferTag key;      /* Id of a disk page */
    BufferDesc *buffer; /* Pointer to BufferDesc */
};

BufTable::BufTable(Size size) : m_size(size),
                                m_bufHash(nullptr),
                                m_bufMappingLwlock(nullptr),
                                m_sharedHashMemoryContext(nullptr)
{
}

void BufTable::Initialize()
{
    /* Create a shared DstoreMemoryContext for hash table */
    m_sharedHashMemoryContext = DstoreAllocSetContextCreate(
        g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_BUFFER),
        "BufTableSharedMemory",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
        MemoryContextType::SHARED_CONTEXT);
    StorageReleasePanic(m_sharedHashMemoryContext == nullptr, MODULE_BUFFER,
        ErrMsg("failed to alloc sharedHashMemoryContext."));

    /* Initialize buf_mapping_lwlock */
    Size bufMappingLwlockSize = NUM_BUFFER_PARTITIONS * sizeof(LWLockPadded);
    m_bufMappingLwlock = (LWLockPadded *)DstorePallocAligned(bufMappingLwlockSize, DSTORE_CACHELINE_SIZE);
    StorageReleasePanic(m_bufMappingLwlock == nullptr, MODULE_BUFFER,
        ErrMsg("alloc memory for bufMappingLwlock fail!"));
    for (int i = 0; i < NUM_BUFFER_PARTITIONS; i++) {
        LWLockInitialize(&m_bufMappingLwlock[i].lock, LWLOCK_GROUP_BUF_MAPPING);
    }

    /* Create shared hash table */
    HASHCTL info;
    info.keysize = sizeof(BufferTag);
    info.entrysize = sizeof(BufferLookupEnt);
    info.hash = tag_hash;
    info.num_partitions = NUM_BUFFER_PARTITIONS;
    info.dsize = info.max_dsize = hash_select_dirsize(m_size);
    info.hcxt = m_sharedHashMemoryContext;

    m_bufHash = hash_create("Shared Buffer Lookup Table", m_size, &info,
                            HASH_ELEM | HASH_FUNCTION | HASH_PARTITION | HASH_DIRSIZE | HASH_CONTEXT | HASH_SHRCTX);
}

void BufTable::Destroy()
{
    StorageAssert(m_bufHash != nullptr);
    StorageAssert(m_bufMappingLwlock != nullptr);
    StorageAssert(m_sharedHashMemoryContext != nullptr);

    hash_destroy(m_bufHash);
    DstorePfreeAligned(m_bufMappingLwlock);
    DstoreMemoryContextDelete(m_sharedHashMemoryContext);
}

HOTFUNCTION uint32 BufTable::GetHashCode(const BufferTag *bufTag)
{
    return get_hash_value(m_bufHash, static_cast<const void *>(bufTag));
}

/*
 * Lookup
 *		Lookup the given PageId; return Buffer, or NULL if not found
 *
 * Caller no need hold LWlock on BufMappingLock for page_id's partition
 */
HOTFUNCTION BufferDesc* BufTable::LookUp(const BufferTag *bufTag, uint32 hashCode)
{
    BufferLookupEnt *result;

RETRY:
    result = static_cast<BufferLookupEnt *>(BufLookUp<HASH_FIND>(m_bufHash, bufTag, hashCode, nullptr));

    if (result == nullptr) {
        return INVALID_BUFFER_DESC;
    }

    BufferDesc *bufferDesc = result->buffer;
    if (STORAGE_VAR_NULL(bufferDesc)) {
        goto RETRY;
    }

    bufferDesc->Pin();

    if (bufferDesc->bufTag != *bufTag) {
        bufferDesc->Unpin();
        goto RETRY;
    }

    return bufferDesc;
}

/*
 * Insert
 *		Insert a hashtable entry for given page_id and Buffer,
 *		unless an entry already exists for that page_id
 *
 * Returns Null on successful insertion.	If a conflicting entry exists
 * already, returns the Buffer in that entry.
 *
 * Caller must hold exclusive LWlock on BufMappingLock for page_id's partition
 */
BufferDesc* BufTable::Insert(const BufferTag *bufTag, uint32 hashCode, BufferDesc* bufferDesc)
{
    BufferLookupEnt *result;
    bool found = false;

    result = static_cast<BufferLookupEnt *>(BufLookUp<HASH_ENTER>(m_bufHash, bufTag, hashCode, &found));
    if (STORAGE_VAR_NULL(result)) {
        ErrLog(DSTORE_PANIC, MODULE_BUFMGR, ErrMsg("Result is nullptr."));
    }
    if (found) { /* found something already in the table */
        StorageAssert(result->buffer != INVALID_BUFFER_DESC);
        return result->buffer;
    }
    result->buffer = bufferDesc;

    return INVALID_BUFFER_DESC;
}

/*
 * drop
 *		Delete the hashtable entry for given page_id (which must exist)
 *
 * Caller must hold exclusive LWLock on BufMappingLWLock for page_id's partition
 */
void BufTable::Remove(const BufferTag *bufTag, uint32 hashCode)
{
    BufferLookupEnt *result;

    result = static_cast<BufferLookupEnt *>(BufLookUp<HASH_REMOVE>(m_bufHash, bufTag, hashCode, nullptr));

    if (result == nullptr) { /* shouldn't happen */
        StorageAssert(false);
    }
}

void BufTable::LockBufMapping(uint32 hashCode, LWLockMode mode) const
{
    LWLock *lwlock = GetBufMappingLwlock(hashCode);
    DstoreLWLockAcquireByMode(lwlock, mode)
}

bool BufTable::TryLockBufMapping(uint32 hashCode, LWLockMode mode) const
{
    LWLock *lwlock = GetBufMappingLwlock(hashCode);
    return DstoreLWLockConditionalAcquire(lwlock, mode);
}

void BufTable::LockBufMapping(uint32 hashCode1, uint32 hashCode2, LWLockMode mode) const
{
    LWLock *lwlock1 = GetBufMappingLwlock(hashCode1);
    LWLock *lwlock2 = GetBufMappingLwlock(hashCode2);

    if (lwlock1 < lwlock2) {
        if (mode == LW_EXCLUSIVE) {
            DstoreLWLockAcquire(lwlock1, LW_EXCLUSIVE);
            DstoreLWLockAcquire(lwlock2, LW_EXCLUSIVE);
        } else {
            DstoreLWLockAcquire(lwlock1, LW_SHARED);
            DstoreLWLockAcquire(lwlock2, LW_SHARED);
        }
    } else if (lwlock1 > lwlock2) {
        if (mode == LW_EXCLUSIVE) {
            DstoreLWLockAcquire(lwlock2, LW_EXCLUSIVE);
            DstoreLWLockAcquire(lwlock1, LW_EXCLUSIVE);
        } else {
            DstoreLWLockAcquire(lwlock2, LW_SHARED);
            DstoreLWLockAcquire(lwlock1, LW_SHARED);
        }
    } else {
        if (mode == LW_EXCLUSIVE) {
            DstoreLWLockAcquire(lwlock1, LW_EXCLUSIVE);
        } else {
            DstoreLWLockAcquire(lwlock1, LW_SHARED);
        }
    }
}

void BufTable::UnlockBufMapping(uint32 hashCode) const
{
    LWLock *lwlock = GetBufMappingLwlock(hashCode);
    return LWLockRelease(lwlock);
}

bool BufTable::IsBufMappingLockedByMe(const BufferDesc *bufferDesc)
{
    if (STORAGE_VAR_NULL(bufferDesc)) {
        ErrLog(DSTORE_PANIC, MODULE_BUFMGR, ErrMsg("BufferDesc is nullptr."));
    }
    uint32 bufferHash = GetHashCode(&(bufferDesc->bufTag));
    LWLock *lwlock = GetBufMappingLwlock(bufferHash);
    return LWLockHeldByMe(lwlock);
}

void BufTable::LockAllBufMapping(LWLockMode mode)
{
    for (int i = 0; i < NUM_BUFFER_PARTITIONS; i++) {
        LWLock *lwlock = &m_bufMappingLwlock[i].lock;
        if (mode == LW_EXCLUSIVE) {
            DstoreLWLockAcquire(lwlock, LW_EXCLUSIVE);
        } else {
            DstoreLWLockAcquire(lwlock, LW_SHARED);
        }
    }
}

void BufTable::UnlockAllBufMapping()
{
    for (int i = 0; i < NUM_BUFFER_PARTITIONS; i++) {
        LWLock *lwlock = &m_bufMappingLwlock[i].lock;
        LWLockRelease(lwlock);
    }
}

Size BufTable::PrintAllBufEntry(char **items)
{
    Size itemSize = 0;
    HASH_SEQ_STATUS status;
    BufferLookupEnt *entry = nullptr;

    LockAllBufMapping(LW_SHARED);
    hash_seq_init(&status, m_bufHash);
    while ((entry = (BufferLookupEnt *)hash_seq_search(&status)) != nullptr) {
        BufferDesc *bufferDesc = entry->buffer;
        items[itemSize] = bufferDesc->PrintBufferDesc();
        itemSize++;
    }
    UnlockAllBufMapping();
    return itemSize;
}

LWLock* BufTable::GetBufMappingLwlock(uint32 hashCode) const
{
    return &(m_bufMappingLwlock + (hashCode % NUM_BUFFER_PARTITIONS))->lock;
}

bool BufTable::IsSameBufMapping(uint32 hashCode1, uint32 hashCode2) const
{
    return (hashCode1 % NUM_BUFFER_PARTITIONS) == (hashCode2 % NUM_BUFFER_PARTITIONS);
}
}  // namespace DSTORE
