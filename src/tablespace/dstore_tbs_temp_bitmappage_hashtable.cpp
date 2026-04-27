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
 * dstore_tbs_temp_bitmappage_hashtable.cpp
 *
 *
 *
 * IDENTIFICATION
 *        src/tablespace/dstore_tbs_temp_bitmappage_hashtable.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "tablespace/dstore_tbs_temp_bitmappage_hashtable.h"
#include "framework/dstore_instance.h"
#include "common/dstore_datatype.h"

namespace DSTORE {

TbsTempBitmapPageHashTable::TbsTempBitmapPageHashTable() : m_bitmapPageHash(nullptr),
    m_pageMappingLwlock(nullptr), m_sharedHashMemoryContext(nullptr)
{}

RetStatus TbsTempBitmapPageHashTable::Initialize()
{
    AutoMemCxtSwitch autoSwtich{g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LONGLIVE)};

    /* Create a shared DstoreMemoryContext for hash table */
    m_sharedHashMemoryContext = DstoreAllocSetContextCreate(
        g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_LONGLIVE),
        "TbsTempBitmapPageHashTableMemory",
        ALLOCSET_DEFAULT_MINSIZE,
        ALLOCSET_DEFAULT_INITSIZE,
        ALLOCSET_DEFAULT_MAXSIZE,
        MemoryContextType::SHARED_CONTEXT);
    if (m_sharedHashMemoryContext == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("Failed to alloc sharedbitmapHashMemoryContext."));
        return DSTORE_FAIL;
    }

    /* Initialize page_mapping_lwlock */
    Size pageMappingLwlockSize = m_hashTablePartitionNum * sizeof(LWLockPadded);
    m_pageMappingLwlock = (LWLockPadded *)DstorePallocAligned(pageMappingLwlockSize, DSTORE_CACHELINE_SIZE);
    if (m_pageMappingLwlock == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("Failed to alloc memory for bitmapMappingLwlock!"));
        DstoreMemoryContextDelete(m_sharedHashMemoryContext);
        m_sharedHashMemoryContext = nullptr;
        return DSTORE_FAIL;
    }

    for (int i = 0; i < m_hashTablePartitionNum; i++) {
        LWLockInitialize(&m_pageMappingLwlock[i].lock, LWLOCK_GROUP_TBS_PAGE_MAPPING);
    }

    /* Create shared hash table */
    HASHCTL info;
    info.keysize = sizeof(PageId);
    info.entrysize = sizeof(PageLookupEnt);
    info.hash = tag_hash;
    info.num_partitions = m_hashTablePartitionNum;
    info.hcxt = m_sharedHashMemoryContext;

    m_bitmapPageHash = hash_create("Shared BitmapPage Lookup Table", m_hashTablePartitionNum, &info,
        HASH_ELEM | HASH_FUNCTION | HASH_PARTITION | HASH_CONTEXT | HASH_SHRCTX);
    if (m_bitmapPageHash == nullptr) {
        ErrLog(DSTORE_ERROR, MODULE_SEGMENT, ErrMsg("Failed to create shared bitmapPage lookup table."));
        DstorePfreeAligned(m_pageMappingLwlock);
        m_pageMappingLwlock = nullptr;
        DstoreMemoryContextDelete(m_sharedHashMemoryContext);
        m_sharedHashMemoryContext = nullptr;
        return DSTORE_FAIL;
    }
    return DSTORE_SUCC;
}

void TbsTempBitmapPageHashTable::Destroy()
{
    StorageAssert(m_bitmapPageHash != nullptr);
    StorageAssert(m_pageMappingLwlock != nullptr);
    StorageAssert(m_sharedHashMemoryContext != nullptr);

    FreeAllEnt();
    hash_destroy(m_bitmapPageHash);
    DstorePfreeAligned(m_pageMappingLwlock);
    DstoreMemoryContextDelete(m_sharedHashMemoryContext);
}

TbsPageItem* TbsTempBitmapPageHashTable::LookUp(const PageId pageId)
{
    const void *keyPtr = static_cast<const void *>(&pageId);
    uint32 hashcode = get_hash_value(m_bitmapPageHash, keyPtr);
    LockPageMapping(hashcode, LW_SHARED);
    bool isFound;
    PageLookupEnt *entry = static_cast<PageLookupEnt *>(hash_search(m_bitmapPageHash, keyPtr, HASH_FIND, &isFound));
    UnlockPageMapping(hashcode);
    if (isFound) {
        return entry->bitmapPage;
    } else {
        return nullptr;
    }
}

TbsPageItem *TbsTempBitmapPageHashTable::Insert(const PageId pageId, TbsPageItem *page)
{
    const void *keyPtr = static_cast<const void *>(&pageId);
    uint32 hashcode = get_hash_value(m_bitmapPageHash, keyPtr);
    LockPageMapping(hashcode, LW_EXCLUSIVE);
    bool isFound;
    PageLookupEnt *entry = static_cast<PageLookupEnt *>(hash_search(m_bitmapPageHash, keyPtr, HASH_ENTER, &isFound));
    if (STORAGE_VAR_NULL(entry)) {
        ErrLog(DSTORE_PANIC, MODULE_BUFMGR, ErrMsg("Temp page(%hu, %u)alloc null.", pageId.m_fileId, pageId.m_blockId));
    }

    if (isFound) {
        StorageAssert(entry->bitmapPage != nullptr);
        UnlockPageMapping(hashcode);
        return entry->bitmapPage;
    }

    entry->bitmapPage = page;
    entry->key = pageId;
    UnlockPageMapping(hashcode);
    return nullptr;
}

LWLock* TbsTempBitmapPageHashTable::GetPageMappingLwlock(uint32 hashCode) const
{
    return &(m_pageMappingLwlock + (hashCode % m_hashTablePartitionNum))->lock;
}

void TbsTempBitmapPageHashTable::LockPageMapping(uint32 hashCode, LWLockMode mode) const
{
    LWLock *lwlock = GetPageMappingLwlock(hashCode);
    DstoreLWLockAcquireByMode(lwlock, mode)
}

void TbsTempBitmapPageHashTable::UnlockPageMapping(uint32 hashCode) const
{
    LWLock *lwlock = GetPageMappingLwlock(hashCode);
    return LWLockRelease(lwlock);
}

void TbsTempBitmapPageHashTable::FreeAllEnt()
{
    HASH_SEQ_STATUS status;
    PageLookupEnt *entry = nullptr;

    hash_seq_init(&status, m_bitmapPageHash);
    while ((entry = (PageLookupEnt *)hash_seq_search(&status)) != nullptr) {
        TbsPageItem *page = entry->bitmapPage;
        DstorePfreeExt(page);
        page = nullptr;
    }
}

} /* namespace DSTORE */
