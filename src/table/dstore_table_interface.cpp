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
 * dstore_table_interface.cpp
 *
 * IDENTIFICATION
 *        src/table/dstore_table_interface.cpp
 *
 * ---------------------------------------------------------------------------------------
 */

#include "table/dstore_table_interface.h"
#include "tablespace/dstore_tablespace.h"
#include "heap/dstore_heap_handler.h"
#include "index/dstore_btree.h"
#include "framework/dstore_instance.h"
#include "lock/dstore_lock_interface.h"

namespace StorageTableInterface {
using namespace DSTORE;

TableStorageMgr *CreateTableSmgr(DSTORE::PdbId pdbId, const TablespaceId tablespaceId, const PageId segmentId,
                                 const int fillfactor, DSTORE::TupleDesc tupDesc, const char persistenceMethod,
                                 bool enableLsc)
{
    DstoreMemoryContext context = ((!enableLsc) ?
        thrd->GetSessionMemoryCtx() : thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_SMGR));
    AutoMemCxtSwitch autoSwitch(context);
    TableStorageMgr *tableSmgr = DstoreNew(context)
        TableStorageMgr(pdbId, fillfactor, tupDesc, static_cast<RelationPersistence>(persistenceMethod));
    if (STORAGE_VAR_NULL(tableSmgr)) {
        return nullptr;
    }
    if (unlikely(tupDesc != nullptr) && STORAGE_VAR_NULL(tableSmgr->m_tupDesc)) {
        delete tableSmgr;
        return nullptr;
    }
    if (STORAGE_FUNC_FAIL(tableSmgr->Init(segmentId, tablespaceId, context))) {
        delete tableSmgr;
        return nullptr;
    }
    return tableSmgr;
}

SegmentInterface *GetTableSegment(TableStorageMgr *tableSmgr)
{
    return tableSmgr->GetSegment();
}

DSTORE::TupleDesc GetTableTupleDesc(TableStorageMgr *tableSmgr)
{
    return tableSmgr->GetTupleDesc();
}

uint64 GetTableBlockCount(TableStorageMgr *tableSmgr)
{
    StorageAssert(tableSmgr != nullptr);
    return tableSmgr->GetTableBlockCount();
}

void DestroyTableSmgr(TableStorageMgr *tableSmgr)
{
    delete tableSmgr;
}

BtreeStorageMgr *CreateBtreeSmgr(PdbId pdbId, const TablespaceId tablespaceId, const PageId segmentId,
                                 const int fillfactor, const char persistenceMethod, bool enableLsc)
{
    DstoreMemoryContext context = ((!enableLsc) ?
        thrd->GetSessionMemoryCtx() : thrd->m_memoryMgr->GetGroupContext(MEMORY_CONTEXT_SMGR));
    BtreeStorageMgr *btreeSmgr = DstoreNew(context)
        BtreeStorageMgr(pdbId, fillfactor, static_cast<RelationPersistence>(persistenceMethod));
    if (STORAGE_VAR_NULL(btreeSmgr)) {
        return nullptr;
    }
    if (STORAGE_FUNC_FAIL(btreeSmgr->Init(segmentId, tablespaceId, context))) {
        delete btreeSmgr;
        return nullptr;
    }
    return btreeSmgr;
}

uint64 GetIndexBlockCount(BtreeStorageMgr *btreeSmgr)
{
    StorageAssert(btreeSmgr != nullptr);
    return btreeSmgr->GetIndexBlockCount();
}

void DestroyBtreeSmgr(BtreeStorageMgr *btreeSmgr)
{
    delete btreeSmgr;
}
}  // namespace StorageTableInterface
