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

#ifndef DSTORE_BUF_TABLE_H
#define DSTORE_BUF_TABLE_H

#include "common/memory/dstore_mctx.h"
#include "common/algorithm/dstore_hsearch.h"
#include "lock/dstore_lwlock.h"
#include "buffer/dstore_buf.h"

namespace DSTORE {

const long NUM_BUFFER_PARTITIONS = 4096;

class BufTable : public BaseObject {
public:
    BufTable() = delete;
    BufTable(const BufTable &) = delete;
    BufTable& operator=(const BufTable&) = delete;
    BufTable(const BufTable&&) = delete;
    BufTable& operator=(const BufTable&&) = delete;

    explicit BufTable(Size size);
    ~BufTable() = default;

    /*
     * Finish buffer table initialization. Create the DstoreMemoryContext that Hash Table needed,
     * and alloc nesserary memory space.
     *
     * If alloc all of memory success and finish initialization, return true.
     * If out of memory, it return false.
     */
    void Initialize();

    void Destroy();

    /*
     * GetHashCode
     *  Compute the hash code associated with a bufTag
     */
    uint32 GetHashCode(const BufferTag *bufTag);
    /*
     * Lookup
     *  Lookup the given bufTag; return Buffer, or NULL if not found
     *
     * Caller must hold at least share LWLock on BufMappingLWLock for bufTag's partition
     */
    BufferDesc *LookUp(const BufferTag *bufTag, uint32 hashCode);
    /*
     * Insert
     *  Insert a hashtable entry for given bufTag and Buffer,
     *  unless an entry already exists for that bufTag
     *
     * Returns Null on successful insertion.	If a conflicting entry exists
     * already, returns the Buffer in that entry.
     *
     * Caller must hold exclusive LWLock on BufMappingLWLock for bufTag's partition
     */
    BufferDesc* Insert(const BufferTag *bufTag, uint32 hashCode, BufferDesc *bufferDesc);
    /*
     * Delete
     *  Delete the hashtable entry for given bufTag (which must exist)
     *
     * Caller must hold exclusive LWLock on BufMappingLWock for bufTag's partition
     */
    void Remove(const BufferTag *bufTag, uint32 hashCode);
    /*
     * LockBufMapping
     *
     */
    void LockBufMapping(uint32 hashCode, LWLockMode mode) const;
    bool TryLockBufMapping(uint32 hashCode, LWLockMode mode) const;

    void LockBufMapping(uint32 hashCode1, uint32 hashCode2, LWLockMode mode) const;

    inline LWLock *GetBufLock(uint32 hashCode) const
    {
        return &(m_bufMappingLwlock + (hashCode % NUM_BUFFER_PARTITIONS))->lock;
    }

    /*
     * UnlockBufMapping
     *
     */
    void UnlockBufMapping(uint32 hashCode) const;

    bool IsBufMappingLockedByMe(const BufferDesc *bufferDesc);

    bool IsSameBufMapping(uint32 hashCode1, uint32 hashCode2) const;

    void LockAllBufMapping(LWLockMode mode);
    void UnlockAllBufMapping();
    Size PrintAllBufEntry(char **items);
private:
    long m_size;
    HTAB *m_bufHash;
    LWLockPadded *m_bufMappingLwlock;
    DstoreMemoryContext m_sharedHashMemoryContext;

    LWLock *GetBufMappingLwlock(uint32 hashCode) const;
};

};
#endif
