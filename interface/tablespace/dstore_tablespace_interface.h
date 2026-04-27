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
 * dstore_tablespace_interface.h
 *
 * IDENTIFICATION
 *        interface/tablespace/dstore_tablespace_interface.h
 *
 * ---------------------------------------------------------------------------------------
 */
 
#ifndef DSTORE_TABLE_SPACE_INTERFACE_H
#define DSTORE_TABLE_SPACE_INTERFACE_H

#include "common/dstore_common_utils.h"
#include "tablespace/dstore_tablespace_struct.h"
#include "transaction/dstore_transaction_interface.h"
#include "systable/dstore_relation.h"

namespace DSTORE {
class TableStorageMgr;
}  // namespace DSTORE

namespace TableSpace_Interface {
#pragma GCC visibility push(default)

DSTORE::PageId AllocSegment(DSTORE::PdbId pdbId, DSTORE::TablespaceId, DSTORE::SegmentType,
                            DSTORE::Oid = DSTORE::DSTORE_INVALID_OID);

DSTORE::RetStatus DropSegment(DSTORE::PdbId pdbId, DSTORE::TablespaceId, DSTORE::SegmentType, DSTORE::PageId);
DSTORE::RetStatus DstoreGetTablespaceInfo(const DSTORE::PdbId, const DSTORE::TablespaceId,
    DSTORE::DstoreTablespaceAttr*);
DSTORE::RetStatus DstoreGetDatafileInfo(const DSTORE::PdbId, const DSTORE::FileId, DSTORE::DstoreDatafileAttr*);
DSTORE::RetStatus DstoreGetTablespaceSize(const DSTORE::PdbId, const DSTORE::TablespaceId, uint64_t& size);

DSTORE::RetStatus GetFileIdsByTablespaceId(DSTORE::PdbId pdbId, DSTORE::Oid tablespaceId, DSTORE::FileId **fileIds,
                                           uint32_t &fileIdCount);

DSTORE::RetStatus AllocTablespaceId(DSTORE::PdbId pdbId, uint64_t tbsMaxSize, DSTORE::TablespaceId *tablespaceId);
DSTORE::RetStatus FreeTablespaceId(DSTORE::PdbId pdbId, DSTORE::TablespaceId tablespaceId, uint64_t ddlXid,
                                   bool needWal = true);

DSTORE::RetStatus BatchAllocAndAddDataFile(DSTORE::PdbId pdbId, DSTORE::TablespaceId tablespaceId,
    DSTORE::FileId *fileIds, uint64_t *fileIdCnt, bool needWal);
DSTORE::RetStatus BatchFreeAndRemoveDataFile(DSTORE::PdbId pdbId, DSTORE::TablespaceId tablespaceId,
    DSTORE::FileId *fileIds, uint64_t fileIdCnt, uint64_t ddlXid);

DSTORE::RetStatus AlterMaxSize(DSTORE::TablespaceId tablespaceId, uint64_t maxSize, DSTORE::PdbId pdbId);

uint8_t IsRemainTablespace(DSTORE::PdbId pdbId, DSTORE::TablespaceId tablespaceId, bool isExistSysTableRecord);
void FreeRemainTablespace(DSTORE::PdbId pdbId, DSTORE::TablespaceId tablespaceId, bool isExistSysTableRecord,
                          uint64_t ddlXid);

uint8_t IsRemainDatafile(DSTORE::PdbId pdbId, DSTORE::FileId fileId);
void FreeRemainDatafile(DSTORE::PdbId pdbId, DSTORE::FileId fileId);

#pragma GCC visibility pop
}  // namespace TableSpace_Interface
#endif
