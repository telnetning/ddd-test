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
 * dstore_index_fault_injection.h
 *
 * IDENTIFICATION
 *        dstore.include/common/fault_injection/dstore_index_fault_injection.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_INDEX_FAULT_INJECTION_H
#define DSTORE_INDEX_FAULT_INJECTION_H

#include "fault_injection/fault_injection.h"
#include "index/dstore_btree_split.h"

namespace DSTORE {

enum class DstoreIndexFI {
    FORCE_SPLIT,
    SET_SPLIT_POINT,
    STOP_RETRY_SEARCH_BTREE,
};

extern SplitContext g_utSplitContext;

void SetUtSplitContext(UNUSE_PARAM const FaultInjectionEntry *entry, SplitContext *splitContext);

}

#endif  // DSTORE_INDEX_FAULT_INJECTION_H