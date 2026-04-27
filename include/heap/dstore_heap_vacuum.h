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
 * dstore_heap_vacuum.h
 *
 * IDENTIFICATION
 *        include/heap/dstore_heap_vacuum.h
 *
 * ---------------------------------------------------------------------------------------
 */

#ifndef DSTORE_HEAP_VACUUM_H
#define DSTORE_HEAP_VACUUM_H

#include "heap/dstore_heap_handler.h"
#include "framework/dstore_instance.h"
#include "tablespace/dstore_data_segment_context.h"

namespace DSTORE {

class HeapVacuumHandler : virtual public HeapHandler {
public:
    HeapVacuumHandler(StorageInstance *instance, ThreadContext *thread, StorageRelation heapRel,
                        bool isLobOperation = false);
    HeapVacuumHandler(const HeapVacuumHandler &) = delete;
    HeapVacuumHandler &operator = (const HeapVacuumHandler &) = delete;
    ~HeapVacuumHandler() final;

    RetStatus LazyVacuum();

private:
    DataSegmentScanContext* m_segScanContext;
};

} /* namespace DSTORE */

#endif
