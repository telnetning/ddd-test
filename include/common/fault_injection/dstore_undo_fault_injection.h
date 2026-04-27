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
 * ---------------------------------------------------------------------------------
 *
 * dstore_undo_fault_injection.h
 *
 * ---------------------------------------------------------------------------------------
 */
#ifndef DSTORE_UNDO_FAULT_INJECTION_H
#define DSTORE_UNDO_FAULT_INJECTION_H

#include <atomic>
#include "page/dstore_page_struct.h"

struct FaultInjectionEntry;

namespace DSTORE {

enum class DstoreUndoFI {
    GET_UNDO_ZONE,
    RECOVER_UNDO_ZONE,
    SET_FREE_PAGE_NUM_ZERO,
    PERSIST_INIT_TRANSACTION_SLOT_FAIL,
    PERSIST_INIT_UNDO_RECORD_FAIL,
};

void SetUndoZoneFreePageNumZero(UNUSE_PARAM const FaultInjectionEntry *entry, PageId &needCheckPageId,
                                const PageId recyclePageId);

}

#endif  // STORAGE_UNDO_FAULT_INJECTION_H