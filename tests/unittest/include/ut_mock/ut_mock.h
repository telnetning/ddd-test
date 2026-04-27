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
#ifndef DSTORE_UT_MOCK_H
#define DSTORE_UT_MOCK_H

#include "config.h"
#include "framework/dstore_instance.h"
#include "tablespace/dstore_tablespace.h"
#include "tablespace/dstore_heap_normal_segment.h"
#include "tablespace/dstore_index_normal_segment.h"
#include "transaction/dstore_transaction.h"
#include "transaction/dstore_transaction_mgr.h"
#include "common/dstore_datatype.h"
#include "ut_buf_mgr_mock.h"
#include "ut_tablespace_mock.h"
#include "ut_segment_mock.h"
#include "ut_tablespace/ut_tablespace_util.h"
#include "buffer/dstore_buf_refcount.h"
#include "wal/dstore_wal_struct.h"
#include "wal/dstore_wal_write_context.h"

namespace DSTORE {

class UtMockModule {
public:
    /* UT Tablespace interface */
    static TableSpaceInterface *UtGetTableSpace(TablespaceId tablespaceId, BufMgrInterface *bufMgr = nullptr,
                                                LockMode lockMode = DSTORE::DSTORE_NO_LOCK)
    {
        return UtTbsUtil::UtGetTablespace(bufMgr, tablespaceId, lockMode);
    }

    static void UtDropTableSpace(TableSpaceInterface *tablespace, LockMode lockMode = DSTORE::DSTORE_NO_LOCK)
    {
        UtTbsUtil::UtDropTablespace(tablespace, lockMode);
        tablespace = nullptr;
    }

    /* UT Segment interface */
    static SegmentInterface *UtGetSegment(PdbId pdbId, SegmentType type, TableSpaceInterface *tablespace, BufMgrInterface *bufMgr)
    {
        return SegmentInterface::AllocUndoSegment(pdbId, tablespace->GetTablespaceId(), type, bufMgr);
    }

    /* UT BufMgr interface */
    static BufMgrInterface *UtInitBufMgr(Size buf_size, Size lru_partition)
    {
        BufMgrInterface *buf_mgr;
        DstoreMemoryContext buffer_context = g_storageInstance->GetMemoryMgr()->GetGroupContext(MEMORY_CONTEXT_BUFFER);
        buf_mgr = DstoreNew(buffer_context) BufMgr(buf_size, lru_partition);
        buf_mgr->Init();

        return buf_mgr;
    }

    static void UtDestroyBufMgr(BufMgrInterface *buf_mgr)
    {
        buf_mgr->Destroy();
        delete buf_mgr;
    }
};

/* TBD remove MockAtomicWalWriterContext after AtomicWalWriterContext implemented */
class MockAtomicWalWriterContext : public AtomicWalWriterContext {
public:
    MockAtomicWalWriterContext(DstoreMemoryContext memoryContext, PdbId pdbId, WalManager *manager)
        : AtomicWalWriterContext(memoryContext, pdbId, manager) {
    }

    int Init()
    {
        return 0;
    }
    int Destroy()
    {
        return 0;
    }

    void BeginAtomicWal(Xid xid)
    {
        (void)xid;
    }

    WalGroupLsnInfo EndAtomicWal()
    {
        WalGroupLsnInfo insertResult = {INVALID_WAL_ID, INVALID_PLSN, INVALID_PLSN};

        return insertResult;
    }

    void PutNewWalRecord(WalRecord *record)
    {
    }

    void Append(__attribute__((__unused__)) void *buf, __attribute__((__unused__)) uint32 size)
    {
    }

    WalGroupLsnInfo AtomicInsertOneWal(WalRecord *record, Xid xid)
    {
        WalGroupLsnInfo insertResult = {INVALID_WAL_ID, INVALID_PLSN, INVALID_PLSN};

        (void)record;
        (void)xid;

        return insertResult;
    }

    void WaitTargetPlsnPersist(WalId walId, uint64 plsn)
    {
        (void)walId;
        (void)plsn;
    }
};
}  // namespace DSTORE
#endif  // DSTORE_UT_MOCK_H
