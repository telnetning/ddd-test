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
 * dstore_table_interface.h
 *
 * IDENTIFICATION
 *        interface/table/dstore_table_interface.h
 *
 * ---------------------------------------------------------------------------------------
 */
 
#ifndef DSTORE_TABLE_INTERFACE_H
#define DSTORE_TABLE_INTERFACE_H

#include "common/dstore_common_utils.h"
#include "page/dstore_page_struct.h"
#include "tuple/dstore_tuple_struct.h"

namespace DSTORE {
class TableStorageMgr;
class BtreeStorageMgr;
class SegmentInterface;
struct HeapTuple;
}  // namespace DSTORE

namespace StorageTableInterface {
#pragma GCC visibility push(default)

DSTORE::TableStorageMgr *CreateTableSmgr(DSTORE::PdbId pdbId, const DSTORE::TablespaceId tablespaceId,
    const DSTORE::PageId segmentId, const int fillfactor, DSTORE::TupleDesc tupDesc,
    const char persistenceMethod = 'p', bool enableLsc = false);
uint64_t GetTableBlockCount(DSTORE::TableStorageMgr *tableSmgr);
void DestroyTableSmgr(DSTORE::TableStorageMgr *tableSmgr);

DSTORE::SegmentInterface *GetTableSegment(DSTORE::TableStorageMgr *tableSmgr);
DSTORE::TupleDesc GetTableTupleDesc(DSTORE::TableStorageMgr *tableSmgr);

DSTORE::BtreeStorageMgr *CreateBtreeSmgr(DSTORE::PdbId pdbId, const DSTORE::TablespaceId tablespaceId,
    const DSTORE::PageId segmentId, const int fillfactor, const char persistenceMethod = 'p', bool enableLsc = false);
uint64_t GetIndexBlockCount(DSTORE::BtreeStorageMgr *btreeSmgr);
void DestroyBtreeSmgr(DSTORE::BtreeStorageMgr *btreeSmgr);

#pragma GCC visibility pop
} /* namespace StorageTableInterface */
#endif
