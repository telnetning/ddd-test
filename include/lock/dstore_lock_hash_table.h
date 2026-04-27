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

#ifndef DSTORE_LOCK_HASH_TABLE_H
#define DSTORE_LOCK_HASH_TABLE_H

#include "securec.h"
#include "common/algorithm/dstore_hsearch.h"
#include "lock/dstore_lwlock.h"
#include "lock/dstore_lock_datatype.h"
#include "lock/dstore_lock_entry.h"

namespace DSTORE {

/*
 * LockHashTable describes a shared lock table, supporting different LockRequest type.
 */
class LockHashTable : public BaseObject {
public:
    LockHashTable();
    ~LockHashTable();

    RetStatus Initialize(uint32 hashTableSize, uint32 partitionNum, DstoreMemoryContext ctx);
    void Destroy();

    RetStatus LockRequestEnqueue(const LockTagCache &tag, LockRequestInterface *request,
                                 LockErrorInfo *info);
    bool LockRequestDequeue(const LockTagCache &tag, LockRequestInterface *request);
    void UpdateAllWaitersAndHolders();

    bool IsHeldByRequester(const LockTag *tag, LockRequestInterface *request);

    RetStatus TransferSingleLockHolder(const LockTagCache &tagCache, uint32 *oldThrdCoreIdx);

#ifdef UT
    LockEntry *UTGetLockEntry(const LockTag *tag);

    /* UT function to get partition lock based on lock tag. */
    LWLock *UTGetPartitionLock(const LockTagCache *tag)
    {
        return &(m_partitionLocks[tag->GetHashCode() % m_partitionNum].lock);
    }
#endif

    RetStatus DumpByLockTag(const LockTagCache &tagCache, StringInfo str);

    RetStatus DescribeState(bool dumpAllLocks, StringInfo str);

protected:
    void AcquireAllPartitionLocks(LWLockMode mode);
    void ReleaseAllPartitionLocks();

    struct HTAB          *m_lockTable;
    LWLockPadded         *m_partitionLocks;
    uint32                m_partitionNum;
    LockRequestFreeLists  m_freeLists;
    DstoreMemoryContext   m_ctx;
};

} /* namespace DSTORE */
#endif
