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
 * dstore_systable_interface.h
 *     This file defines the building blocks that bg page writer needs.
 *
 * IDENTIFICATION
 *        interface/systable/dstore_systable_interface.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_SYSTABLE_INTERFACE_H
#define DSTORE_SYSTABLE_INTERFACE_H
#include "page/dstore_page_struct.h"
#include "systable/dstore_systable_struct.h"
#include "common/dstore_common_utils.h"
#include "index/dstore_scankey.h"
#include "systable/dstore_relation.h"
#include "page/dstore_itemptr.h"

namespace SystableInterface {
#pragma GCC visibility push(default)
DSTORE::RetStatus GetCoreSystableSegmentId(DSTORE::PdbId pdbId, DSTORE::Oid sysTableOid, DSTORE::PageId &segmentId);
DSTORE::RetStatus CreateCoreSystable(DSTORE::PdbId pdbId, DSTORE::Oid relOid, DSTORE::TablespaceId tableSpaceId,
                                     DSTORE::PageId &segmentId);
DSTORE::RetStatus AddRelationMap(DSTORE::PdbId pdbId, DSTORE::Oid relOid, DSTORE::PageId &segmentId);
DSTORE::Oid GetNewObjectId(DSTORE::PdbId pdbId, bool isInitDb, bool isInplaceUpgrade);
/*
 * Scan sys_relation.
 * Note:
 *    If successed, the return value needs to be free..
 */
DSTORE::HeapTuple *ScanSysRelation(DSTORE::StorageRelation sysRel, DSTORE::Oid targetRelOid);
#pragma GCC visibility pop
}  // namespace SystableInterface
#endif