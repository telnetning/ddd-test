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
 * dstore_flashback_interface.h
 *
 *
 *
 * IDENTIFICATION
 *        src/gausskernel/dstore/interface/flashback/dstore_flashback_interface.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_FLASHBACK_INTERFACE_H
#define DSTORE_FLASHBACK_INTERFACE_H

#include "heap/dstore_heap_struct.h"
#include "common/dstore_common_utils.h"

namespace DSTORE {

class TableStorageMgr;
class FlashbackTableHandler;
}  // namespace DSTORE

namespace FlashbackInterface {
#pragma GCC visibility push(default)

DSTORE::FlashbackTableHandler *CreateFlashbackTableHandler(DSTORE::StorageRelation heapRel,
                                                           DSTORE::CommitSeqNo snapshotCsn, DSTORE::Snapshot snapshot,
                                                           bool isLob = false);
DSTORE::HeapTuple *GetDeltaTuple(DSTORE::FlashbackTableHandler *flashbackTableHandler);
DSTORE::HeapTuple *GetLostTuple(DSTORE::FlashbackTableHandler *flashbackTableHandler);
void DestroyFlashbackHandler(DSTORE::FlashbackTableHandler *flashbackTableHandler);

#pragma GCC visibility pop
}  // namespace FlashbackInterface
#endif
