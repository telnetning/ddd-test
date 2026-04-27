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
 * dstore_tbs_temp_bitmappage_hashtable.h
 *
 * IDENTIFICATION
 *        include/tablespace/dstore_tbs_temp_bitmappage_hashtable.h
 *
 * Description: The bitmap pages local memory manager of temporary tablespace.
 * For global temporary table, the bitmapMeta page and bitmap pages of the temporary file are
 * managed in memory.
 * The bitmap pages in memory are managed by hash table with key(pageId) and value(bitmap page).
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_TBS_TEMP_BITMAPPAGE_TABLE_H
#define DSTORE_TBS_TEMP_BITMAPPAGE_TABLE_H

#include "common/algorithm/dstore_hsearch.h"
#include "lock/dstore_lwlock.h"
#include "page/dstore_bitmap_page.h"

namespace DSTORE {

struct TbsPageItem {
    LWLock pageLock;          /* page lock */
    TbsBitmapPage bitmapPage; /* bitmapPage */
};

struct PageLookupEnt {
    PageId key;              /* Id of a disk page */
    TbsPageItem *bitmapPage; /* Pointer to bitmapPage and pageLock */
};

/*
 * 1, For temporay tablespace, bitmapMeta page and bitmap pages are managed in memory by this
 *    TbsTempBitmapPageHashTable.
 * 2, The max bitmap page number is 2^9(consider the max file size is 32TB and 1 bit for 1MB).
*/
class TbsTempBitmapPageHashTable : public BaseObject {
public:
    TbsTempBitmapPageHashTable(const TbsTempBitmapPageHashTable &) = delete;
    TbsTempBitmapPageHashTable& operator=(const TbsTempBitmapPageHashTable&) = delete;
    TbsTempBitmapPageHashTable(const TbsTempBitmapPageHashTable&&) = delete;
    TbsTempBitmapPageHashTable& operator=(const TbsTempBitmapPageHashTable&&) = delete;

    TbsTempBitmapPageHashTable();
    ~TbsTempBitmapPageHashTable() = default;

    /*
     * Create the DstoreMemoryContext that Hash Table needed,
     * and alloc nesserary memory space.
     * alloc all of memory and create hash table.
     */
    RetStatus Initialize();

    void Destroy();

    /*
     * GetHashCode
     *  Compute the hash code associated with a pageId
     */
    uint32 GetHashCode(const PageId pageId);
    /*
     * Lookup
     *  Lookup the given pageId; return BitmapPage, or NULL if not found
     */
    TbsPageItem* LookUp(const PageId pageId);
    /*
     * Insert
     *  Insert a hashtable entry for given pageId and bitmapPage,
     *  unless an entry already exists for that pageId
     */
    TbsPageItem* Insert(const PageId pageId, TbsPageItem *bitmapPage);
    /*
     * LockPageMapping
     *
     */
    void LockPageMapping(uint32 hashCode, LWLockMode mode) const;
    /*
     * UnlockPageMapping
     */
    void UnlockPageMapping(uint32 hashCode) const;
    void FreeAllEnt();
private:
    HTAB *m_bitmapPageHash;
    LWLockPadded *m_pageMappingLwlock;
    DstoreMemoryContext m_sharedHashMemoryContext;
    const long m_hashTablePartitionNum = 1024;
    LWLock *GetPageMappingLwlock(uint32 hashCode) const;
};

}  // namespace DSTORE

#endif  // DSTORE_TBS_TEMP_BITMAPPAGE_TABLE_H
